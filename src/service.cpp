// The coordinator service: the distributed form of the authority model.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Structure, in the order a request travels:
//
//   accept -> session -> frame -> replay suppression -> handshake check ->
//   payload decode -> identity check -> ONE mutex -> coordinator -> local
//   answer buffer -> release -> frame -> send.
//
// Invariants this file is responsible for:
//
//   *  Exactly one mutex (Impl::coordinator_mutex_) serialises every call into
//      the Coordinator. It is held across the coordinator call and nothing else:
//      no send, no log, no sleep and no allocation of an unbounded size happens
//      while it is held. Every answer is copied into a local buffer before the
//      lock is released. The coordinator detects re-entrant entry, so a missed
//      release would surface as InvalidState rather than as a deadlock.
//   *  A connection begins with HandshakeRequest. Any other first message is
//      refused with HandshakeRequired. The handshake binds a SessionIdentity to
//      the connection and the server assigns the SessionId that later frames
//      must carry; a frame whose session id or payload identity contradicts the
//      envelope is refused with IdentityMismatch.
//   *  Every reply is exactly one frame carrying the Response flag and the
//      request's sequence number. A duplicated sequence is answered from a
//      bounded cache of the last 32 answers instead of being applied twice.
//   *  Determinism: no wall-clock time is read, no sleep exists in this
//      translation unit, and the bounded socket waits below only decide when a
//      shutdown flag is re-checked. No protocol decision depends on them.
//   *  Bounded allocation: the frame bound is enforced by the frame layer and by
//      the explicit max_message_payload_bytes check below, every collection is
//      read through ByteReader::collection_count(), and the per-session answer
//      cache is a fixed 32 entries.
//
// Payload layouts written by this file (canonical little-endian, ByteWriter and
// ByteReader; every reply payload starts with the uniform envelope
// {u16 ErrorCode, u32 detail_length, detail bytes}):
//
//   HandshakeRequest    {u8 kind, u64 participant_id, u64 incarnation, u64 boot_nonce}
//   OpenShuffleRequest  {u64 shuffle, u64 generation, u32 partition_count, policy}
//   RegisterParticipant {u8 kind, u64 id, u64 incarnation, string endpoint,
//                        u8 selection_kind, u64 begin, u64 end, u32 list_count, u64[]}
//   PublishManifest     {byte string: encoded manifest}
//   NextWave            {}
//   CommitTransfer      {CommitRequest encoding, field order below}
//   ReportFailure       {u64 attempt, u16 error_code}
//   CongestionIntent    {u8 reporter_kind, u64 reporter_id, u64 reporter_incarnation,
//                        u64 producer, u64 consumer, u32 level, u64 policy_generation,
//                        u64 observed_at}
//   CancelShuffle       {u16 reason}
//   ManifestRequest     {u64 partition}
//   ProgressRequest / StatusRequest / CloseRequest   {}
//   ExplainRequest      {u32 max_samples}
//
//   CommitRequest encoding, in order: partition, partition_generation, consumer,
//   attempt, wave, shuffle_generation, topology_generation, policy_generation,
//   producer, producer_incarnation, consumer, consumer_incarnation,
//   manifest_digest, observed_partition_digest, integrity_verified, bytes,
//   u32 chunk_count, digest[] -- the same order the durable CommitRecord uses,
//   so the wire form and the durable form cannot drift apart.
//
//   RegisterParticipantResponse {u64 topology_generation, u64 incarnation, u8 superseded_previous}
//   PublishManifestResponse     {digest manifest_digest, u64 partition_generation}
//   NextWaveResponse            {u64 wave, u32 grant_count, grants..., counters}
//                               grant: {u64 attempt, u64 wave, u64 shuffle, u64 shuffle_generation,
//                                       u64 partition, u64 partition_generation, u64 producer,
//                                       u64 producer_incarnation, string producer_endpoint,
//                                       u64 consumer, u64 consumer_incarnation, u64 topology_generation,
//                                       u64 policy_generation, digest manifest_digest, u64 total_bytes,
//                                       u32 attempt_ordinal, u64 issued_at}
//                               counters: examined, skipped_completed, skipped_failed, skipped_in_flight,
//                                         skipped_unproduced, deferred_limits, deferred_source_pressure,
//                                         deferred_destination_pressure, deferred_unknown_pressure,
//                                         deferred_retry_wait (all u32), cursor_wrapped (u8),
//                                         all_resolved (u8)
//   CommitTransferResponse      {u8 newly_committed, u8 edge_newly_completed, u8 duplicate,
//                                u64 sequence, digest manifest_digest, u64 accounted_bytes,
//                                u8 shuffle_completed}
//   ReportFailureResponse       {u8 permanent, u32 attempts, u64 ready_at}
//   ManifestResponse            {byte string: encoded manifest}
//   ProgressResponse            {progress snapshot: 20 u64 fields in ProgressSnapshot order}
//   ExplainResponse             {u32 entry_count, {u16 code, string subject, string detail, u64 count}[]}
//   StatusResponse              {u8 state, u16 reason, u64 epoch, u64 shuffle, u64 shuffle_generation,
//                                u64 topology_generation, u64 policy_generation, u64 tick,
//                                u32 active_producers, u32 active_consumers, u32 in_flight,
//                                u64 persisted_records, u8 durable, u8 revalidation_required,
//                                u8 history_incomplete, progress snapshot}
//
// The data plane (ChunkFetch*) is deliberately not served here: the coordinator
// never carries bulk data, and those message types are answered with
// FrameTypeUnsupported rather than being partially implemented.

#include "shuffle/fabric/service.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "shuffle/fabric/codec.hpp"
#include "shuffle/fabric/ledger.hpp"
#include "shuffle/fabric/records.hpp"
#include "shuffle/fabric/schedule.hpp"
#include "shuffle/fabric/topology.hpp"

namespace shuffle::fabric {
namespace {

// ---------------------------------------------------------------------------
// Bounds and bounded waits
// ---------------------------------------------------------------------------

// Bounded cache of answers already produced for a sequence number. It is a
// memory bound, not a policy: a duplicate whose answer has fallen out of the
// window is refused with DuplicateFrame instead of being applied a second time.
constexpr std::size_t kAnswerCacheEntries = 32;

// Sequence window of the per-session ReplayGuard.
constexpr std::uint64_t kReplayWindow = 1024;

// Bytes read from a socket in one call, and the waits that bound every socket
// operation in this file. The waits are transport responsiveness only: they
// decide when a shutdown flag is re-checked, never whether an operation
// succeeded. Correctness comes from the frame layer and the coordinator.
constexpr std::size_t kSessionReadChunkBytes = 1u << 16;
constexpr int kAcceptWaitMs = 50;
constexpr int kSessionReadWaitMs = 100;
constexpr int kServerSendWaitMs = 5000;
constexpr int kClientWaitMs = 5000;

// A worker thread serves one connection at a time, so the pool size is a real
// concurrency bound; it is capped so that a bogus option cannot ask the process
// for an unbounded number of threads.
constexpr std::uint32_t kMaxWorkerThreads = 1024;

[[nodiscard]] constexpr bool is_producer_kind(std::uint8_t raw) noexcept {
  return raw == static_cast<std::uint8_t>(ParticipantKind::Producer);
}

[[nodiscard]] constexpr bool is_consumer_kind(std::uint8_t raw) noexcept {
  return raw == static_cast<std::uint8_t>(ParticipantKind::Consumer);
}

[[nodiscard]] bool is_known_error_code(std::uint16_t raw) noexcept {
  return raw <= static_cast<std::uint16_t>(ErrorCode::TooManyTrackedEdges) &&
         (raw == 0 || raw >= static_cast<std::uint16_t>(ErrorCode::InvalidArgument));
}

// The handshake binds a participant identity to the connection. A payload that
// carries a different id, incarnation or kind is refused rather than believed.
Status check_participant_identity(const SessionIdentity& identity, ParticipantKind kind, std::uint64_t id,
                                  IncarnationId incarnation) {
  if (identity.kind != kind || identity.participant_id != id || identity.incarnation != incarnation) {
    return Status{make_error(ErrorCode::IdentityMismatch,
                             "request identity contradicts the session established by the handshake")};
  }
  return Status{};
}

// ---------------------------------------------------------------------------
// Uniform reply envelope
// ---------------------------------------------------------------------------

void write_ok_envelope(ByteWriter& writer) {
  writer.put_u16(0);
  writer.put_string(std::string_view{});
}

void write_error_envelope(ByteWriter& writer, const Limits& limits, ErrorCode code, std::string_view detail) {
  const std::size_t bound = static_cast<std::size_t>(limits.max_string_bytes);
  if (detail.size() > bound) {
    detail = detail.substr(0, bound);
  }
  writer.put_u16(static_cast<std::uint16_t>(code));
  writer.put_string(detail);
}

Status read_envelope(ByteReader& reader, const Limits& limits, ErrorCode& code, std::string& detail) {
  const std::uint16_t raw = reader.u16();
  detail = reader.string(limits.max_string_bytes);
  if (!reader.ok()) {
    return reader.status();
  }
  if (!is_known_error_code(raw)) {
    return Status{make_error(ErrorCode::ProtocolViolation, "reply carries an error code outside protocol version 1")};
  }
  code = static_cast<ErrorCode>(raw);
  return Status{};
}

void write_text(ByteWriter& writer, std::string_view text, std::uint32_t max_bytes) {
  if (text.size() > max_bytes) {
    text = text.substr(0, max_bytes);
  }
  writer.put_string(text);
}

// ---------------------------------------------------------------------------
// Payload codecs shared by the server (read) and the client (write)
// ---------------------------------------------------------------------------

Status write_selection(ByteWriter& writer, const PartitionSelection& selection, const Limits& limits) {
  if (static_cast<std::uint64_t>(selection.list.size()) > limits.max_selection_items) {
    return Status{make_error(ErrorCode::OversizedInput, "selection list exceeds max_selection_items")};
  }
  writer.put_u8(static_cast<std::uint8_t>(selection.kind));
  writer.put_u64(selection.begin.value());
  writer.put_u64(selection.end.value());
  writer.put_u32(static_cast<std::uint32_t>(selection.list.size()));
  for (const PartitionId partition : selection.list) {
    writer.put_u64(partition.value());
  }
  return Status{};
}

Result<PartitionSelection> read_selection(ByteReader& reader, const Limits& limits) {
  PartitionSelection selection;
  const std::uint8_t raw_kind = reader.u8();
  selection.begin = PartitionId{reader.u64()};
  selection.end = PartitionId{reader.u64()};
  if (!reader.ok()) {
    return Result<PartitionSelection>{reader.error()};
  }
  if (raw_kind > static_cast<std::uint8_t>(SelectionKind::List)) {
    return make_failure<PartitionSelection>(ErrorCode::MalformedInput,
                                            "selection kind is not part of protocol version 1");
  }
  selection.kind = static_cast<SelectionKind>(raw_kind);
  const std::uint32_t count = reader.collection_count(limits.max_selection_items);
  if (!reader.ok()) {
    return Result<PartitionSelection>{reader.error()};
  }
  selection.list.reserve(count);
  for (std::uint32_t index = 0; index < count; ++index) {
    selection.list.push_back(PartitionId{reader.u64()});
  }
  if (!reader.ok()) {
    return Result<PartitionSelection>{reader.error()};
  }
  return selection;
}

Result<ShuffleOpenRequest> read_open_shuffle(ByteReader& reader) {
  ShuffleOpenRequest request;
  request.shuffle = ShuffleId{reader.u64()};
  request.generation = ShuffleGeneration{reader.u64()};
  request.partition_count = reader.u32();
  if (!reader.ok()) {
    return Result<ShuffleOpenRequest>{reader.error()};
  }
  const Result<PolicyEnvelope> policy = decode_policy(reader);
  if (!policy.ok()) {
    return Result<ShuffleOpenRequest>{policy.error()};
  }
  request.policy = policy.value();
  if (!reader.ok()) {
    return Result<ShuffleOpenRequest>{reader.error()};
  }
  return request;
}

void write_commit_request(ByteWriter& writer, const CommitRequest& request) {
  writer.put_u64(request.edge.partition.value());
  writer.put_u64(request.edge.partition_generation.value());
  writer.put_u64(request.edge.consumer.value());
  writer.put_u64(request.attempt.value());
  writer.put_u64(request.wave.value());
  writer.put_u64(request.shuffle_generation.value());
  writer.put_u64(request.topology_generation.value());
  writer.put_u64(request.policy_generation.value());
  writer.put_u64(request.producer.value());
  writer.put_u64(request.producer_incarnation.value());
  writer.put_u64(request.consumer.value());
  writer.put_u64(request.consumer_incarnation.value());
  writer.put_digest(request.manifest_digest);
  writer.put_digest(request.observed_partition_digest);
  writer.put_bool(request.integrity_verified);
  writer.put_u64(request.bytes);
  writer.put_u32(static_cast<std::uint32_t>(request.observed_chunk_digests.size()));
  for (const Digest& digest : request.observed_chunk_digests) {
    writer.put_digest(digest);
  }
}

Result<CommitRequest> read_commit_request(ByteReader& reader, const Limits& limits) {
  CommitRequest request;
  request.edge.partition = PartitionId{reader.u64()};
  request.edge.partition_generation = PartitionGeneration{reader.u64()};
  request.edge.consumer = ConsumerId{reader.u64()};
  request.attempt = TransferAttemptId{reader.u64()};
  request.wave = WaveId{reader.u64()};
  request.shuffle_generation = ShuffleGeneration{reader.u64()};
  request.topology_generation = TopologyGeneration{reader.u64()};
  request.policy_generation = PolicyGeneration{reader.u64()};
  request.producer = ProducerId{reader.u64()};
  request.producer_incarnation = IncarnationId{reader.u64()};
  request.consumer = ConsumerId{reader.u64()};
  request.consumer_incarnation = IncarnationId{reader.u64()};
  request.manifest_digest = reader.digest();
  request.observed_partition_digest = reader.digest();
  request.integrity_verified = reader.boolean();
  request.bytes = reader.u64();
  if (!reader.ok()) {
    return Result<CommitRequest>{reader.error()};
  }
  const std::uint32_t chunks = reader.collection_count(limits.max_chunks_per_partition);
  if (!reader.ok()) {
    return Result<CommitRequest>{reader.error()};
  }
  request.observed_chunk_digests.reserve(chunks);
  for (std::uint32_t index = 0; index < chunks; ++index) {
    request.observed_chunk_digests.push_back(reader.digest());
  }
  if (!reader.ok()) {
    return Result<CommitRequest>{reader.error()};
  }
  return request;
}

void write_progress_snapshot(ByteWriter& writer, const ProgressSnapshot& snapshot) {
  writer.put_u64(snapshot.shuffle.value());
  writer.put_u64(snapshot.shuffle_generation.value());
  writer.put_u64(snapshot.partitions_total);
  writer.put_u64(snapshot.partitions_committed);
  writer.put_u64(snapshot.partitions_failed);
  writer.put_u64(snapshot.partitions_incomplete);
  writer.put_u64(snapshot.edges_required);
  writer.put_u64(snapshot.edges_completed);
  writer.put_u64(snapshot.edges_failed);
  writer.put_u64(snapshot.edges_incomplete);
  writer.put_u64(snapshot.edges_over_counted);
  writer.put_u64(snapshot.bytes_committed);
  writer.put_u64(snapshot.bytes_attempted);
  writer.put_u64(snapshot.duplicate_commits_suppressed);
  writer.put_u64(snapshot.retriable_failures);
  writer.put_u64(snapshot.permanent_failures);
  writer.put_u64(snapshot.authority_refusals);
  writer.put_u64(snapshot.tracked_partitions);
  writer.put_u64(snapshot.tracked_edges);
  writer.put_u64(snapshot.tick.value());
}

Result<ProgressSnapshot> read_progress_snapshot(ByteReader& reader) {
  ProgressSnapshot snapshot;
  snapshot.shuffle = ShuffleId{reader.u64()};
  snapshot.shuffle_generation = ShuffleGeneration{reader.u64()};
  snapshot.partitions_total = reader.u64();
  snapshot.partitions_committed = reader.u64();
  snapshot.partitions_failed = reader.u64();
  snapshot.partitions_incomplete = reader.u64();
  snapshot.edges_required = reader.u64();
  snapshot.edges_completed = reader.u64();
  snapshot.edges_failed = reader.u64();
  snapshot.edges_incomplete = reader.u64();
  snapshot.edges_over_counted = reader.u64();
  snapshot.bytes_committed = reader.u64();
  snapshot.bytes_attempted = reader.u64();
  snapshot.duplicate_commits_suppressed = reader.u64();
  snapshot.retriable_failures = reader.u64();
  snapshot.permanent_failures = reader.u64();
  snapshot.authority_refusals = reader.u64();
  snapshot.tracked_partitions = reader.u64();
  snapshot.tracked_edges = reader.u64();
  snapshot.tick = TickId{reader.u64()};
  if (!reader.ok()) {
    return Result<ProgressSnapshot>{reader.error()};
  }
  return snapshot;
}

void write_status(ByteWriter& writer, const CoordinatorStatus& status) {
  writer.put_u8(static_cast<std::uint8_t>(status.state));
  writer.put_u16(static_cast<std::uint16_t>(status.reason));
  writer.put_u64(status.epoch);
  writer.put_u64(status.shuffle.value());
  writer.put_u64(status.shuffle_generation.value());
  writer.put_u64(status.topology_generation.value());
  writer.put_u64(status.policy_generation.value());
  writer.put_u64(status.tick.value());
  writer.put_u32(status.active_producers);
  writer.put_u32(status.active_consumers);
  writer.put_u32(status.in_flight);
  writer.put_u64(status.persisted_records);
  writer.put_bool(status.durable);
  writer.put_bool(status.revalidation_required);
  writer.put_bool(status.history_incomplete);
  write_progress_snapshot(writer, status.progress);
}

Result<CoordinatorStatus> read_status(ByteReader& reader) {
  CoordinatorStatus status;
  const std::uint8_t raw_state = reader.u8();
  const std::uint16_t raw_reason = reader.u16();
  status.epoch = reader.u64();
  status.shuffle = ShuffleId{reader.u64()};
  status.shuffle_generation = ShuffleGeneration{reader.u64()};
  status.topology_generation = TopologyGeneration{reader.u64()};
  status.policy_generation = PolicyGeneration{reader.u64()};
  status.tick = TickId{reader.u64()};
  status.active_producers = reader.u32();
  status.active_consumers = reader.u32();
  status.in_flight = reader.u32();
  status.persisted_records = reader.u64();
  status.durable = reader.boolean();
  status.revalidation_required = reader.boolean();
  status.history_incomplete = reader.boolean();
  if (!reader.ok()) {
    return Result<CoordinatorStatus>{reader.error()};
  }
  if (raw_state > static_cast<std::uint8_t>(ShuffleState::Failed)) {
    return make_failure<CoordinatorStatus>(ErrorCode::ProtocolViolation, "shuffle state is outside protocol version 1");
  }
  if (!is_known_error_code(raw_reason)) {
    return make_failure<CoordinatorStatus>(ErrorCode::ProtocolViolation, "shuffle reason is outside protocol version 1");
  }
  status.state = static_cast<ShuffleState>(raw_state);
  status.reason = static_cast<ErrorCode>(raw_reason);
  const auto progress = read_progress_snapshot(reader);
  if (!progress.ok()) {
    return Result<CoordinatorStatus>{progress.error()};
  }
  status.progress = progress.value();
  if (!reader.ok()) {
    return Result<CoordinatorStatus>{reader.error()};
  }
  return status;
}

void write_grant(ByteWriter& writer, const DispatchGrant& grant, const Limits& limits) {
  writer.put_u64(grant.attempt.value());
  writer.put_u64(grant.wave.value());
  writer.put_u64(grant.shuffle.value());
  writer.put_u64(grant.shuffle_generation.value());
  writer.put_u64(grant.partition.value());
  writer.put_u64(grant.partition_generation.value());
  writer.put_u64(grant.producer.value());
  writer.put_u64(grant.producer_incarnation.value());
  write_text(writer, grant.producer_endpoint, limits.max_endpoint_bytes);
  writer.put_u64(grant.consumer.value());
  writer.put_u64(grant.consumer_incarnation.value());
  writer.put_u64(grant.topology_generation.value());
  writer.put_u64(grant.policy_generation.value());
  writer.put_digest(grant.manifest_digest);
  writer.put_u64(grant.total_bytes);
  writer.put_u32(grant.attempt_ordinal);
  writer.put_u64(grant.issued_at.value());
}

Result<DispatchGrant> read_grant(ByteReader& reader, const Limits& limits) {
  DispatchGrant grant;
  grant.attempt = TransferAttemptId{reader.u64()};
  grant.wave = WaveId{reader.u64()};
  grant.shuffle = ShuffleId{reader.u64()};
  grant.shuffle_generation = ShuffleGeneration{reader.u64()};
  grant.partition = PartitionId{reader.u64()};
  grant.partition_generation = PartitionGeneration{reader.u64()};
  grant.producer = ProducerId{reader.u64()};
  grant.producer_incarnation = IncarnationId{reader.u64()};
  grant.producer_endpoint = reader.string(limits.max_endpoint_bytes);
  grant.consumer = ConsumerId{reader.u64()};
  grant.consumer_incarnation = IncarnationId{reader.u64()};
  grant.topology_generation = TopologyGeneration{reader.u64()};
  grant.policy_generation = PolicyGeneration{reader.u64()};
  grant.manifest_digest = reader.digest();
  grant.total_bytes = reader.u64();
  grant.attempt_ordinal = reader.u32();
  grant.issued_at = TickId{reader.u64()};
  if (!reader.ok()) {
    return Result<DispatchGrant>{reader.error()};
  }
  return grant;
}

void write_wave_plan(ByteWriter& writer, const WavePlan& plan, const Limits& limits) {
  writer.put_u64(plan.id.value());
  writer.put_u32(static_cast<std::uint32_t>(plan.grants.size()));
  for (const DispatchGrant& grant : plan.grants) {
    write_grant(writer, grant, limits);
  }
  writer.put_u32(plan.examined);
  writer.put_u32(plan.skipped_completed);
  writer.put_u32(plan.skipped_failed);
  writer.put_u32(plan.skipped_in_flight);
  writer.put_u32(plan.skipped_unproduced);
  writer.put_u32(plan.deferred_limits);
  writer.put_u32(plan.deferred_source_pressure);
  writer.put_u32(plan.deferred_destination_pressure);
  writer.put_u32(plan.deferred_unknown_pressure);
  writer.put_u32(plan.deferred_retry_wait);
  writer.put_bool(plan.cursor_wrapped);
  writer.put_bool(plan.all_resolved);
}

Result<WavePlan> read_wave_plan(ByteReader& reader, const Limits& limits) {
  WavePlan plan;
  plan.id = WaveId{reader.u64()};
  const std::uint32_t grants = reader.collection_count(limits.max_wave_grants);
  if (!reader.ok()) {
    return Result<WavePlan>{reader.error()};
  }
  plan.grants.reserve(grants);
  for (std::uint32_t index = 0; index < grants; ++index) {
    const auto grant = read_grant(reader, limits);
    if (!grant.ok()) {
      return Result<WavePlan>{grant.error()};
    }
    plan.grants.push_back(grant.value());
  }
  plan.examined = reader.u32();
  plan.skipped_completed = reader.u32();
  plan.skipped_failed = reader.u32();
  plan.skipped_in_flight = reader.u32();
  plan.skipped_unproduced = reader.u32();
  plan.deferred_limits = reader.u32();
  plan.deferred_source_pressure = reader.u32();
  plan.deferred_destination_pressure = reader.u32();
  plan.deferred_unknown_pressure = reader.u32();
  plan.deferred_retry_wait = reader.u32();
  plan.cursor_wrapped = reader.boolean();
  plan.all_resolved = reader.boolean();
  if (!reader.ok()) {
    return Result<WavePlan>{reader.error()};
  }
  return plan;
}

void write_explanation(ByteWriter& writer, const Explanation& explanation, const Limits& limits) {
  writer.put_u32(static_cast<std::uint32_t>(explanation.entries.size()));
  for (const ExplainEntry& entry : explanation.entries) {
    writer.put_u16(static_cast<std::uint16_t>(entry.code));
    write_text(writer, entry.subject, limits.max_string_bytes);
    write_text(writer, entry.detail, limits.max_string_bytes);
    writer.put_u64(entry.count);
  }
}

Result<Explanation> read_explanation(ByteReader& reader, const Limits& limits) {
  Explanation explanation;
  const std::uint32_t count = reader.collection_count(limits.max_collection_items);
  if (!reader.ok()) {
    return Result<Explanation>{reader.error()};
  }
  explanation.entries.reserve(count);
  for (std::uint32_t index = 0; index < count; ++index) {
    ExplainEntry entry;
    const std::uint16_t raw_code = reader.u16();
    entry.subject = reader.string(limits.max_string_bytes);
    entry.detail = reader.string(limits.max_string_bytes);
    entry.count = reader.u64();
    if (!reader.ok()) {
      return Result<Explanation>{reader.error()};
    }
    if (!is_known_error_code(raw_code)) {
      return make_failure<Explanation>(ErrorCode::ProtocolViolation, "explanation carries an unknown error code");
    }
    entry.code = static_cast<ErrorCode>(raw_code);
    explanation.entries.push_back(std::move(entry));
  }
  return explanation;
}

// ---------------------------------------------------------------------------
// One connection
// ---------------------------------------------------------------------------

struct AnswerCacheEntry {
  std::uint64_t sequence{0};
  std::vector<std::byte> frame{};
};

struct Session {
  Session(Socket accepted, std::uint64_t key, const Limits& bounds)
      : socket(std::move(accepted)),
        id(key),
        guard(kReplayWindow),
        stream(bounds),
        limits(bounds),
        read_buffer(kSessionReadChunkBytes) {}

  Socket socket;
  std::uint64_t id{0};      // server-side registry key; never travels on the wire
  SessionId wire_id{};      // session id assigned by the handshake, zero until then
  bool bound{false};
  SessionIdentity identity{};
  ReplayGuard guard;
  FrameStream stream;
  Limits limits;
  std::vector<std::byte> read_buffer;
  std::deque<AnswerCacheEntry> answers;

  [[nodiscard]] const std::vector<std::byte>* cached_answer(std::uint64_t sequence) const noexcept {
    for (const AnswerCacheEntry& entry : answers) {
      if (entry.sequence == sequence) {
        return &entry.frame;
      }
    }
    return nullptr;
  }

  void remember_answer(std::uint64_t sequence, const std::vector<std::byte>& frame) {
    answers.push_back(AnswerCacheEntry{sequence, frame});
    while (answers.size() > kAnswerCacheEntries) {
      answers.pop_front();
    }
  }
};

}  // namespace

// ---------------------------------------------------------------------------
// CoordinatorServer
// ---------------------------------------------------------------------------

struct CoordinatorServer::Impl {
  Impl(Coordinator& coordinator, ServerOptions options)
      : coordinator_(&coordinator), options_(std::move(options)) {}
  ~Impl() { static_cast<void>(stop()); }

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;

  [[nodiscard]] Status start();
  [[nodiscard]] Status stop();
  [[nodiscard]] bool running() const noexcept { return running_.load(std::memory_order_acquire); }
  [[nodiscard]] std::uint16_t port() const noexcept { return bound_port_.load(std::memory_order_acquire); }
  [[nodiscard]] ServerStats stats() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return stats_;
  }

  void accept_loop();
  void worker_loop();
  void serve(const std::shared_ptr<Session>& session);
  void handle(Session& session, const DecodedFrame& frame, MessageType& answer_type, std::vector<std::byte>& answer,
              bool& served);
  void finish_session(const std::shared_ptr<Session>& session);
  void note_frame(bool served);
  void note_duplicate(bool replayed);
  void refuse_socket(Socket& socket, ErrorCode code, std::string_view detail);

  Coordinator* coordinator_{nullptr};
  ServerOptions options_{};

  // Declared before the listener and before every session so that the socket
  // subsystem outlives every handle created against it.
  SocketRuntime runtime_{};
  TcpListener listener_{};

  // Guards the registry, the pending queue, the statistics and the lifecycle
  // flags. It is never held while the coordinator is entered and never while a
  // frame is sent.
  mutable std::mutex state_mutex_{};
  std::condition_variable work_ready_{};
  std::unordered_map<std::uint64_t, std::shared_ptr<Session>> sessions_{};
  std::deque<std::shared_ptr<Session>> pending_{};
  ServerStats stats_{};

  // Serialises start()/stop() and owns threads_.
  std::mutex lifecycle_mutex_{};
  std::vector<std::thread> threads_{};

  // THE mutex: every Coordinator call in this translation unit passes through
  // it, and it is held for nothing else.
  std::mutex coordinator_mutex_{};

  std::atomic<bool> running_{false};
  std::atomic<std::uint16_t> bound_port_{0};
  std::atomic<std::uint64_t> next_session_key_{1};
  std::atomic<std::uint64_t> next_wire_session_{1};

  std::uint32_t max_sessions_{1};
  std::uint32_t workers_{1};
};

Status CoordinatorServer::Impl::start() {
  std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
  if (running_.load(std::memory_order_acquire)) {
    return Status{make_error(ErrorCode::InvalidState, "the server is already running")};
  }
  const Status limits_valid = options_.limits.validate();
  if (!limits_valid.ok()) {
    return limits_valid;
  }
  if (options_.accept_backlog == 0 || options_.accept_backlog > static_cast<std::uint32_t>(kMaxListenBacklog)) {
    return Status{make_error(ErrorCode::InvalidArgument, "accept_backlog must be in [1, kMaxListenBacklog]")};
  }
  if (!runtime_.active()) {
    return Status{make_error(runtime_.code(), "the socket runtime is not active")};
  }

  auto bound = TcpListener::bind(options_.bind_host, options_.port, static_cast<int>(options_.accept_backlog), runtime_);
  if (!bound.ok()) {
    return bound.status();
  }
  listener_ = bound.take();
  const auto local = listener_.local_port();
  if (!local.ok() || local.value() == 0) {
    listener_.close();
    return local.ok() ? Status{make_error(ErrorCode::InvalidState, "the listener reported port 0")}
                      : local.status();
  }

  // A session worker serves one connection for that connection's lifetime, so
  // the pool must be large enough to serve every session the server admits.
  // Sizing it below max_sessions would leave admitted sessions queued and their
  // clients waiting on answers that never come, which is why the requested
  // worker count is raised to the session bound.
  max_sessions_ = std::clamp(options_.max_sessions, 1u, kMaxWorkerThreads);
  workers_ = std::clamp(std::max(options_.worker_threads, max_sessions_), 1u, kMaxWorkerThreads);
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    stats_ = ServerStats{};
    sessions_.clear();
    pending_.clear();
  }
  bound_port_.store(local.value(), std::memory_order_release);
  running_.store(true, std::memory_order_release);

  const auto spawn = [this]() -> Status {
    try {
      threads_.reserve(static_cast<std::size_t>(workers_) + 1);
      threads_.emplace_back([this] { accept_loop(); });
      for (std::uint32_t index = 0; index < workers_; ++index) {
        threads_.emplace_back([this] { worker_loop(); });
      }
    } catch (const std::system_error& error) {
      return Status{make_error(ErrorCode::ResourceExhausted, error.what())};
    }
    return Status{};
  };
  const Status spawned = spawn();
  if (!spawned.ok()) {
    running_.store(false, std::memory_order_release);
    listener_.close();
    work_ready_.notify_all();
    for (std::thread& thread : threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
    threads_.clear();
    bound_port_.store(0, std::memory_order_release);
    return spawned;
  }
  return Status{};
}

Status CoordinatorServer::Impl::stop() {
  std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
  const bool was_running = running_.exchange(false, std::memory_order_acq_rel);
  std::vector<std::shared_ptr<Session>> closing;
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    closing.reserve(sessions_.size());
    for (auto& entry : sessions_) {
      closing.push_back(entry.second);
    }
    sessions_.clear();
    pending_.clear();
    stats_.active_sessions = 0;
  }

  // Closing the listening handle makes a blocked accept() return immediately,
  // and closing a session socket wakes a reader blocked on it. Both are
  // documented operations of the socket layer.
  listener_.close();
  for (const std::shared_ptr<Session>& session : closing) {
    static_cast<void>(session->socket.shutdown());
    session->socket.close();
  }
  work_ready_.notify_all();

  // The join happens without holding state_mutex_: a worker unregisters itself
  // through the same mutex on its way out.
  const std::thread::id self = std::this_thread::get_id();
  for (std::thread& thread : threads_) {
    if (!thread.joinable()) {
      continue;
    }
    if (thread.get_id() == self) {
      // stop() called from one of this server's own threads: that thread is the
      // caller and cannot join itself, so it is detached and exits on its own
      // once running_ is false. The service never re-enters user code, so this
      // path is unreachable through the public API.
      thread.detach();
      continue;
    }
    thread.join();
  }
  threads_.clear();
  bound_port_.store(0, std::memory_order_release);
  static_cast<void>(was_running);
  return Status{};
}

void CoordinatorServer::Impl::note_frame(bool served) {
  std::lock_guard<std::mutex> lock(state_mutex_);
  if (served) {
    ++stats_.served_requests;
  } else {
    ++stats_.refused_requests;
  }
}

void CoordinatorServer::Impl::note_duplicate(bool replayed) {
  std::lock_guard<std::mutex> lock(state_mutex_);
  ++stats_.duplicate_requests;
  if (replayed) {
    ++stats_.replayed_answers;
  }
}

void CoordinatorServer::Impl::refuse_socket(Socket& socket, ErrorCode code, std::string_view detail) {
  // A refused session has no sequence and no session id yet, so the refusal
  // frame carries session 0 and sequence 1 -- the sequence a peer's first
  // request uses. The connection is closed immediately afterwards.
  ByteWriter writer;
  write_error_envelope(writer, options_.limits, code, detail);
  FrameHeader header;
  header.type = MessageType::ErrorResponse;
  header.flags = kFlagResponse;
  header.session = SessionId{};
  header.sequence = 1;
  std::vector<std::byte> frame;
  const Status encoded = encode_frame(header, writer.data(), frame, options_.limits);
  if (encoded.ok()) {
    static_cast<void>(socket.send_all(frame, kServerSendWaitMs));
  }
  // Drain what the peer has already sent before the socket is closed. Closing a
  // socket that still holds unread data aborts the connection (RST), and an
  // abort can discard the refusal frame before the peer ever reads it, which
  // would turn a named refusal into an unnamed transport error. The wait is
  // bounded and the loop ends as soon as the peer closes or stops sending.
  std::array<std::byte, 256> drained{};
  for (;;) {
    const auto arrived = socket.recv_some(drained, kSessionReadWaitMs);
    if (!arrived.ok()) {
      break;
    }
  }
}

void CoordinatorServer::Impl::accept_loop() {
  for (;;) {
    if (!running_.load(std::memory_order_acquire)) {
      return;
    }
    auto accepted = listener_.accept(kAcceptWaitMs);
    if (!accepted.ok()) {
      if (accepted.code() == ErrorCode::PeerUnavailable) {
        return;  // the listener was closed by stop()
      }
      if (!running_.load(std::memory_order_acquire)) {
        return;
      }
      continue;  // NoWorkAvailable (the bounded wait expired) or a transient failure
    }
    Socket socket = accepted.take();

    std::shared_ptr<Session> session;
    bool refused = false;
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      if (!running_.load(std::memory_order_acquire)) {
        socket.close();
        return;
      }
      if (static_cast<std::uint64_t>(sessions_.size()) >= static_cast<std::uint64_t>(max_sessions_)) {
        ++stats_.refused_sessions;
        refused = true;
      } else {
        const std::uint64_t key = next_session_key_.fetch_add(1, std::memory_order_relaxed);
        session = std::make_shared<Session>(std::move(socket), key, options_.limits);
        sessions_.emplace(key, session);
        pending_.push_back(session);
        ++stats_.accepted_sessions;
        ++stats_.active_sessions;
        stats_.peak_sessions = std::max(stats_.peak_sessions, stats_.active_sessions);
      }
    }

    if (refused) {
      refuse_socket(socket, ErrorCode::QueueFull, "the server session bound is reached");
      socket.close();
      continue;
    }
    work_ready_.notify_one();
  }
}

void CoordinatorServer::Impl::worker_loop() {
  for (;;) {
    std::shared_ptr<Session> session;
    {
      std::unique_lock<std::mutex> lock(state_mutex_);
      work_ready_.wait(lock, [this] {
        return !pending_.empty() || !running_.load(std::memory_order_acquire);
      });
      if (!running_.load(std::memory_order_acquire)) {
        return;
      }
      session = pending_.front();
      pending_.pop_front();
    }
    serve(session);
    finish_session(session);
  }
}

void CoordinatorServer::Impl::finish_session(const std::shared_ptr<Session>& session) {
  bool removed = false;
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    removed = sessions_.erase(session->id) > 0;
    if (removed && stats_.active_sessions > 0) {
      --stats_.active_sessions;
    }
  }
  static_cast<void>(session->socket.shutdown());
  session->socket.close();
}

void CoordinatorServer::Impl::serve(const std::shared_ptr<Session>& session) {
  for (;;) {
    if (!running_.load(std::memory_order_acquire)) {
      return;
    }
    auto decoded = session->stream.next();
    if (decoded.ok()) {
      const DecodedFrame frame = decoded.value();  // payload is a view into the stream
      const std::uint64_t sequence = frame.header.sequence;
      MessageType answer_type = MessageType::ErrorResponse;
      std::vector<std::byte> answer;

      const Result<bool> accepted = session->guard.accept(sequence);
      if (!accepted.ok()) {
        ByteWriter writer;
        write_error_envelope(writer, session->limits, accepted.code(), accepted.detail());
        answer = writer.take();
        note_frame(false);
      } else if (!accepted.value()) {
        // A duplicated sequence is answered from the bounded cache and never
        // applied a second time.
        const std::vector<std::byte>* cached = session->cached_answer(sequence);
        if (cached == nullptr) {
          ByteWriter writer;
          write_error_envelope(writer, session->limits, ErrorCode::DuplicateFrame,
                               "duplicate sequence whose answer has left the retained window");
          answer = writer.take();
          note_duplicate(false);
          note_frame(false);
        } else {
          note_duplicate(true);
          const Status replayed = session->socket.send_all(*cached, kServerSendWaitMs);
          if (!replayed.ok()) {
            return;
          }
          continue;
        }
      } else {
        bool served = false;
        handle(*session, frame, answer_type, answer, served);
        note_frame(served);
      }

      FrameHeader header;
      header.type = answer_type;
      header.flags = kFlagResponse;
      header.session = session->wire_id;
      header.sequence = sequence;
      std::vector<std::byte> reply;
      Status encoded = encode_frame(header, answer, reply, session->limits);
      if (!encoded.ok()) {
        // The answer could not be framed (an oversized wave plan, for example).
        // The refusal is sent instead of a partial frame.
        ByteWriter writer;
        write_error_envelope(writer, session->limits, encoded.code(), encoded.detail());
        header.type = MessageType::ErrorResponse;
        encoded = encode_frame(header, writer.data(), reply, session->limits);
        if (!encoded.ok()) {
          return;
        }
      }
      session->remember_answer(sequence, reply);
      const Status sent = session->socket.send_all(reply, kServerSendWaitMs);
      if (!sent.ok()) {
        return;
      }
      if (frame.header.type == MessageType::CloseRequest) {
        return;  // the graceful close is answered; the session ends here
      }
      continue;
    }

    if (decoded.code() != ErrorCode::NoWorkAvailable) {
      // A frame-level refusal (magic, version, CRC, flags, message type or a
      // declared length beyond the bound) leaves the stream unusable: there is
      // no sequence to answer and no way to resynchronise, so the session ends.
      return;
    }
    const auto arrived = session->socket.recv_some(session->read_buffer, kSessionReadWaitMs);
    if (!arrived.ok()) {
      if (arrived.code() == ErrorCode::NoWorkAvailable) {
        continue;  // the bounded wait expired: re-check the shutdown flag
      }
      return;
    }
    const Status fed =
        session->stream.feed(std::span<const std::byte>(session->read_buffer.data(), arrived.value()));
    if (!fed.ok()) {
      return;  // the peer exceeded the frame bound: the session is not answerable
    }
  }
}

void CoordinatorServer::Impl::handle(Session& session, const DecodedFrame& frame, MessageType& answer_type,
                                     std::vector<std::byte>& answer, bool& served) {
  const Limits& limits = session.limits;
  const MessageType type = frame.header.type;
  served = false;
  answer.clear();
  answer_type = MessageType::ErrorResponse;

  const auto refuse = [&answer, &answer_type, &limits](ErrorCode code, std::string_view detail) {
    answer_type = MessageType::ErrorResponse;
    ByteWriter writer;
    write_error_envelope(writer, limits, code, detail);
    answer = writer.take();
  };

  if (frame.payload.size() > limits.max_message_payload_bytes) {
    refuse(ErrorCode::OversizedInput, "payload exceeds max_message_payload_bytes");
    return;
  }

  // ---- handshake stage -----------------------------------------------------
  if (!session.bound) {
    if (type != MessageType::HandshakeRequest) {
      refuse(ErrorCode::HandshakeRequired, "the first request on a connection must be HandshakeRequest");
      return;
    }
    ByteReader reader{frame.payload, limits, "handshake request"};
    const std::uint8_t raw_kind = reader.u8();
    const std::uint64_t participant = reader.u64();
    const std::uint64_t incarnation = reader.u64();
    const std::uint64_t boot_nonce = reader.u64();
    const Status end = reader.require_end();
    if (!end.ok()) {
      refuse(end.code(), end.detail());
      return;
    }
    if (!is_producer_kind(raw_kind) && !is_consumer_kind(raw_kind)) {
      refuse(ErrorCode::MalformedInput, "handshake carries a participant kind outside protocol version 1");
      return;
    }
    SessionIdentity identity;
    identity.kind = static_cast<ParticipantKind>(raw_kind);
    identity.participant_id = participant;
    identity.incarnation = IncarnationId{incarnation};
    identity.boot_nonce = boot_nonce;
    if (!identity.valid()) {
      refuse(ErrorCode::InvalidArgument, "handshake identity is incomplete");
      return;
    }
    // The server assigns the session id; the peer learns it from the reply
    // header and must carry it on every later frame.
    session.identity = identity;
    session.wire_id = SessionId{next_wire_session_.fetch_add(1, std::memory_order_relaxed)};
    session.bound = true;
    served = true;
    answer_type = MessageType::HandshakeResponse;
    ByteWriter writer;
    write_ok_envelope(writer);
    answer = writer.take();
    return;
  }

  // ---- bound stage ---------------------------------------------------------
  if (type == MessageType::HandshakeRequest) {
    refuse(ErrorCode::ProtocolViolation, "the session is already bound to an identity");
    return;
  }
  if (frame.header.session != session.wire_id) {
    refuse(ErrorCode::IdentityMismatch, "frame carries a session id that is not this session's");
    return;
  }

  switch (type) {
    case MessageType::OpenShuffleRequest: {
      ByteReader reader{frame.payload, limits, "open shuffle request"};
      const auto request = read_open_shuffle(reader);
      if (!request.ok()) {
        refuse(request.code(), request.detail());
        return;
      }
      const Status end = reader.require_end();
      if (!end.ok()) {
        refuse(end.code(), end.detail());
        return;
      }
      const Status outcome = [&] {
        // The one lock. Nothing but the coordinator call happens inside it: the
        // answer below is written after the guard has been destroyed.
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        return coordinator_->open_shuffle(request.value());
      }();
      served = true;
      if (!outcome.ok()) {
        refuse(outcome.code(), outcome.detail());
        return;
      }
      answer_type = MessageType::OpenShuffleResponse;
      ByteWriter writer;
      write_ok_envelope(writer);
      answer = writer.take();
      return;
    }

    case MessageType::RegisterParticipantRequest: {
      ByteReader reader{frame.payload, limits, "register participant request"};
      const std::uint8_t raw_kind = reader.u8();
      const std::uint64_t id = reader.u64();
      const std::uint64_t incarnation = reader.u64();
      const std::string endpoint = reader.string(limits.max_endpoint_bytes);
      const auto selection = read_selection(reader, limits);
      if (!selection.ok()) {
        refuse(selection.code(), selection.detail());
        return;
      }
      const Status end = reader.require_end();
      if (!end.ok()) {
        refuse(end.code(), end.detail());
        return;
      }
      if (!is_producer_kind(raw_kind) && !is_consumer_kind(raw_kind)) {
        refuse(ErrorCode::MalformedInput, "registration carries a participant kind outside protocol version 1");
        return;
      }
      const auto kind = static_cast<ParticipantKind>(raw_kind);
      const Status identity =
          check_participant_identity(session.identity, kind, id, IncarnationId{incarnation});
      if (!identity.ok()) {
        refuse(identity.code(), identity.detail());
        return;
      }
      const Result<RegistrationOutcome> outcome = [&]() -> Result<RegistrationOutcome> {
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        if (kind == ParticipantKind::Producer) {
          return coordinator_->register_producer(ProducerId{id}, IncarnationId{incarnation}, endpoint);
        }
        return coordinator_->register_consumer(ConsumerId{id}, IncarnationId{incarnation}, endpoint,
                                               selection.value());
      }();
      served = true;
      if (!outcome.ok()) {
        refuse(outcome.code(), outcome.detail());
        return;
      }
      answer_type = MessageType::RegisterParticipantResponse;
      ByteWriter writer;
      write_ok_envelope(writer);
      writer.put_u64(outcome.value().topology_generation.value());
      writer.put_u64(outcome.value().incarnation.value());
      writer.put_bool(outcome.value().superseded_previous);
      answer = writer.take();
      return;
    }

    case MessageType::PublishManifestRequest: {
      ByteReader reader{frame.payload, limits, "publish manifest request"};
      const std::span<const std::byte> encoded = reader.bytes(limits.max_message_payload_bytes);
      if (!reader.ok()) {
        refuse(reader.error().code, reader.error().detail);
        return;
      }
      const Status end = reader.require_end();
      if (!end.ok()) {
        refuse(end.code(), end.detail());
        return;
      }
      const auto manifest = decode_manifest(encoded, limits);
      if (!manifest.ok()) {
        refuse(manifest.code(), manifest.detail());
        return;
      }
      if (session.identity.kind != ParticipantKind::Producer ||
          manifest.value().producer.value() != session.identity.participant_id ||
          manifest.value().producer_incarnation != session.identity.incarnation) {
        refuse(ErrorCode::IdentityMismatch, "manifest contradicts the identity bound by the handshake");
        return;
      }
      const Result<ManifestAcceptance> acceptance = [&]() -> Result<ManifestAcceptance> {
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        return coordinator_->publish_manifest(manifest.value());
      }();
      served = true;
      if (!acceptance.ok()) {
        refuse(acceptance.code(), acceptance.detail());
        return;
      }
      answer_type = MessageType::PublishManifestResponse;
      ByteWriter writer;
      write_ok_envelope(writer);
      writer.put_digest(acceptance.value().manifest_digest);
      writer.put_u64(acceptance.value().partition_generation.value());
      answer = writer.take();
      return;
    }

    case MessageType::NextWaveRequest: {
      ByteReader reader{frame.payload, limits, "next wave request"};
      const Status end = reader.require_end();
      if (!end.ok()) {
        refuse(end.code(), end.detail());
        return;
      }
      const Result<WavePlan> plan = [&]() -> Result<WavePlan> {
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        return coordinator_->next_wave();
      }();
      served = true;
      if (!plan.ok()) {
        refuse(plan.code(), plan.detail());
        return;
      }
      answer_type = MessageType::NextWaveResponse;
      ByteWriter writer;
      write_ok_envelope(writer);
      write_wave_plan(writer, plan.value(), limits);
      answer = writer.take();
      return;
    }

    case MessageType::CommitTransferRequest: {
      ByteReader reader{frame.payload, limits, "commit transfer request"};
      const auto request = read_commit_request(reader, limits);
      if (!request.ok()) {
        refuse(request.code(), request.detail());
        return;
      }
      const Status end = reader.require_end();
      if (!end.ok()) {
        refuse(end.code(), end.detail());
        return;
      }
      const CommitRequest& commit = request.value();
      const bool identity_matches =
          session.identity.kind == ParticipantKind::Consumer
              ? (commit.consumer.value() == session.identity.participant_id &&
                 commit.consumer_incarnation == session.identity.incarnation)
              : (commit.producer.value() == session.identity.participant_id &&
                 commit.producer_incarnation == session.identity.incarnation);
      if (!identity_matches) {
        refuse(ErrorCode::IdentityMismatch, "completion contradicts the identity bound by the handshake");
        return;
      }
      const Result<CommitOutcome> outcome = [&]() -> Result<CommitOutcome> {
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        return coordinator_->commit_transfer(commit);
      }();
      served = true;
      if (!outcome.ok()) {
        refuse(outcome.code(), outcome.detail());
        return;
      }
      const CommitReceipt& receipt = outcome.value().receipt;
      answer_type = MessageType::CommitTransferResponse;
      ByteWriter writer;
      write_ok_envelope(writer);
      writer.put_bool(receipt.newly_committed);
      writer.put_bool(receipt.edge_newly_completed);
      writer.put_bool(receipt.duplicate);
      writer.put_u64(receipt.sequence.value());
      writer.put_digest(receipt.manifest_digest);
      writer.put_u64(receipt.accounted_bytes);
      writer.put_bool(outcome.value().shuffle_completed);
      answer = writer.take();
      return;
    }

    case MessageType::ReportFailureRequest: {
      ByteReader reader{frame.payload, limits, "report failure request"};
      const std::uint64_t attempt = reader.u64();
      const std::uint16_t raw_code = reader.u16();
      const Status end = reader.require_end();
      if (!end.ok()) {
        refuse(end.code(), end.detail());
        return;
      }
      if (raw_code == 0 || !is_known_error_code(raw_code)) {
        refuse(ErrorCode::MalformedInput, "failure code is outside protocol version 1");
        return;
      }
      const Result<FailureOutcome> outcome = [&]() -> Result<FailureOutcome> {
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        return coordinator_->report_failure(TransferAttemptId{attempt}, static_cast<ErrorCode>(raw_code));
      }();
      served = true;
      if (!outcome.ok()) {
        refuse(outcome.code(), outcome.detail());
        return;
      }
      answer_type = MessageType::ReportFailureResponse;
      ByteWriter writer;
      write_ok_envelope(writer);
      writer.put_bool(outcome.value().permanent);
      writer.put_u32(outcome.value().attempts);
      writer.put_u64(outcome.value().ready_at.value());
      answer = writer.take();
      return;
    }

    case MessageType::CongestionIntentRequest: {
      ByteReader reader{frame.payload, limits, "congestion intent request"};
      const std::uint8_t raw_kind = reader.u8();
      const std::uint64_t reporter = reader.u64();
      const std::uint64_t incarnation = reader.u64();
      CongestionIntent intent;
      intent.producer = ProducerId{reader.u64()};
      intent.consumer = ConsumerId{reader.u64()};
      intent.level = reader.u32();
      intent.policy_generation = PolicyGeneration{reader.u64()};
      intent.observed_at = TickId{reader.u64()};
      const Status end = reader.require_end();
      if (!end.ok()) {
        refuse(end.code(), end.detail());
        return;
      }
      if (!is_producer_kind(raw_kind) && !is_consumer_kind(raw_kind)) {
        refuse(ErrorCode::MalformedInput, "congestion reporter kind is outside protocol version 1");
        return;
      }
      intent.reporter_kind = static_cast<ParticipantKind>(raw_kind);
      intent.reporter_id = reporter;
      intent.reporter_incarnation = IncarnationId{incarnation};
      const Status identity =
          check_participant_identity(session.identity, intent.reporter_kind, reporter, intent.reporter_incarnation);
      if (!identity.ok()) {
        refuse(identity.code(), identity.detail());
        return;
      }
      const Status outcome = [&] {
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        return coordinator_->update_congestion(intent);
      }();
      served = true;
      if (!outcome.ok()) {
        refuse(outcome.code(), outcome.detail());
        return;
      }
      answer_type = MessageType::CongestionIntentResponse;
      ByteWriter writer;
      write_ok_envelope(writer);
      answer = writer.take();
      return;
    }

    case MessageType::CancelShuffleRequest: {
      ByteReader reader{frame.payload, limits, "cancel shuffle request"};
      const std::uint16_t raw_reason = reader.u16();
      const Status end = reader.require_end();
      if (!end.ok()) {
        refuse(end.code(), end.detail());
        return;
      }
      if (raw_reason == 0 || !is_known_error_code(raw_reason)) {
        refuse(ErrorCode::MalformedInput, "cancellation reason is outside protocol version 1");
        return;
      }
      const Status outcome = [&] {
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        return coordinator_->cancel_shuffle(static_cast<ErrorCode>(raw_reason));
      }();
      served = true;
      if (!outcome.ok()) {
        refuse(outcome.code(), outcome.detail());
        return;
      }
      answer_type = MessageType::CancelShuffleResponse;
      ByteWriter writer;
      write_ok_envelope(writer);
      answer = writer.take();
      return;
    }

    case MessageType::ManifestRequest: {
      ByteReader reader{frame.payload, limits, "manifest request"};
      const std::uint64_t partition = reader.u64();
      const Status end = reader.require_end();
      if (!end.ok()) {
        refuse(end.code(), end.detail());
        return;
      }
      const Result<PartitionManifest> manifest = [&]() -> Result<PartitionManifest> {
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        return coordinator_->manifest_of(PartitionId{partition});
      }();
      served = true;
      if (!manifest.ok()) {
        refuse(manifest.code(), manifest.detail());
        return;
      }
      const auto encoded = encode_manifest(manifest.value(), limits);
      if (!encoded.ok()) {
        refuse(encoded.code(), encoded.detail());
        return;
      }
      answer_type = MessageType::ManifestResponse;
      ByteWriter writer;
      write_ok_envelope(writer);
      writer.put_byte_string(encoded.value());
      answer = writer.take();
      return;
    }

    case MessageType::ProgressRequest: {
      ByteReader reader{frame.payload, limits, "progress request"};
      const Status end = reader.require_end();
      if (!end.ok()) {
        refuse(end.code(), end.detail());
        return;
      }
      const Result<ProgressSnapshot> snapshot = [&]() -> Result<ProgressSnapshot> {
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        return coordinator_->progress();
      }();
      served = true;
      if (!snapshot.ok()) {
        refuse(snapshot.code(), snapshot.detail());
        return;
      }
      answer_type = MessageType::ProgressResponse;
      ByteWriter writer;
      write_ok_envelope(writer);
      write_progress_snapshot(writer, snapshot.value());
      answer = writer.take();
      return;
    }

    case MessageType::ExplainRequest: {
      ByteReader reader{frame.payload, limits, "explain request"};
      const std::uint32_t max_samples = reader.u32();
      const Status end = reader.require_end();
      if (!end.ok()) {
        refuse(end.code(), end.detail());
        return;
      }
      const Result<Explanation> explanation = [&]() -> Result<Explanation> {
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        return coordinator_->explain(max_samples);
      }();
      served = true;
      if (!explanation.ok()) {
        refuse(explanation.code(), explanation.detail());
        return;
      }
      answer_type = MessageType::ExplainResponse;
      ByteWriter writer;
      write_ok_envelope(writer);
      write_explanation(writer, explanation.value(), limits);
      answer = writer.take();
      return;
    }

    case MessageType::StatusRequest: {
      ByteReader reader{frame.payload, limits, "status request"};
      const Status end = reader.require_end();
      if (!end.ok()) {
        refuse(end.code(), end.detail());
        return;
      }
      const Result<CoordinatorStatus> snapshot = [&]() -> Result<CoordinatorStatus> {
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        return coordinator_->status();
      }();
      served = true;
      if (!snapshot.ok()) {
        refuse(snapshot.code(), snapshot.detail());
        return;
      }
      answer_type = MessageType::StatusResponse;
      ByteWriter writer;
      write_ok_envelope(writer);
      write_status(writer, snapshot.value());
      answer = writer.take();
      return;
    }

    case MessageType::CloseRequest: {
      ByteReader reader{frame.payload, limits, "close request"};
      const Status end = reader.require_end();
      if (!end.ok()) {
        refuse(end.code(), end.detail());
        return;
      }
      served = true;
      answer_type = MessageType::CloseResponse;
      ByteWriter writer;
      write_ok_envelope(writer);
      answer = writer.take();
      return;
    }

    default:
      // ChunkFetch* (the data plane), every response type sent as a request,
      // and any type this service does not serve.
      refuse(ErrorCode::FrameTypeUnsupported,
             std::string{"message type "} + to_string(type) + " is not served by the coordinator service");
      return;
  }
}

// ---------------------------------------------------------------------------
// CoordinatorClient
// ---------------------------------------------------------------------------

struct CoordinatorClient::Impl {
  Impl() : stream(Limits{}), read_buffer(kSessionReadChunkBytes) {}

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;

  [[nodiscard]] Status connect(const ClientOptions& options);
  [[nodiscard]] Status close();
  [[nodiscard]] bool connected() const noexcept { return connected_.load(std::memory_order_acquire); }
  [[nodiscard]] const Limits& limits() const noexcept { return options_.limits; }

  // One request frame out, exactly one reply frame in, with the reply sequence
  // checked against the request sequence and against a client-side ReplayGuard.
  // mutex_ must be held by the caller.
  [[nodiscard]] Status transact(MessageType request_type, MessageType response_type,
                                std::span<const std::byte> payload, std::vector<std::byte>& body);
  // Locks mutex_, performs the exchange and requires an empty response body.
  [[nodiscard]] Status call(MessageType request_type, MessageType response_type,
                            std::span<const std::byte> payload);
  // Locks mutex_, performs the exchange and returns the response body.
  [[nodiscard]] Status round_trip(MessageType request_type, MessageType response_type,
                                  std::span<const std::byte> payload, std::vector<std::byte>& body);

 private:
  [[nodiscard]] Status handshake(std::span<const std::byte> payload);
  [[nodiscard]] Status receive_frame(DecodedFrame& out, int wait_ms);
  void drop_connection() noexcept;

  ClientOptions options_{};
  SocketRuntime runtime_{};
  Socket socket_{};
  SessionId session_{};
  std::uint64_t next_sequence_{1};
  ReplayGuard guard_{kReplayWindow};
  FrameStream stream;
  std::vector<std::byte> read_buffer;
  std::atomic<bool> connected_{false};
  std::mutex mutex_{};
};

void CoordinatorClient::Impl::drop_connection() noexcept {
  connected_.store(false, std::memory_order_release);
  static_cast<void>(socket_.shutdown());
  socket_.close();
}

Status CoordinatorClient::Impl::receive_frame(DecodedFrame& out, int wait_ms) {
  for (;;) {
    auto decoded = stream.next();
    if (decoded.ok()) {
      out = decoded.value();
      return Status{};
    }
    if (decoded.code() != ErrorCode::NoWorkAvailable) {
      return decoded.status();
    }
    const auto arrived = socket_.recv_some(read_buffer, wait_ms);
    if (!arrived.ok()) {
      if (arrived.code() == ErrorCode::NoWorkAvailable) {
        // The peer produced no reply within the bounded wait. That is a
        // transport conclusion, never a protocol one.
        return Status{make_error(ErrorCode::ConnectionFailure, "no reply arrived within the bounded wait")};
      }
      return arrived.status();
    }
    const Status fed = stream.feed(std::span<const std::byte>(read_buffer.data(), arrived.value()));
    if (!fed.ok()) {
      return fed;
    }
  }
}

Status CoordinatorClient::Impl::handshake(std::span<const std::byte> payload) {
  FrameHeader header;
  header.type = MessageType::HandshakeRequest;
  header.flags = 0;
  header.session = SessionId{};  // not yet bound: the server assigns the id
  header.sequence = 1;
  std::vector<std::byte> frame;
  Status status = encode_frame(header, payload, frame, options_.limits);
  if (!status.ok()) {
    return status;
  }
  const int budget = static_cast<int>(options_.connect_timeout_ms);
  status = socket_.send_all(frame, budget);
  if (!status.ok()) {
    return status;
  }
  DecodedFrame reply{};
  status = receive_frame(reply, budget);
  if (!status.ok()) {
    return status;
  }
  if (!reply.header.carries_response_flag() || reply.header.sequence != header.sequence) {
    return Status{make_error(ErrorCode::ProtocolViolation, "handshake reply does not carry the request sequence")};
  }
  const Result<bool> fresh = guard_.accept(reply.header.sequence);
  if (!fresh.ok()) {
    return fresh.status();
  }
  if (!fresh.value()) {
    return Status{make_error(ErrorCode::ProtocolViolation, "handshake reply reuses a sequence already seen")};
  }
  ByteReader reader{reply.payload, options_.limits, "handshake response"};
  ErrorCode code = ErrorCode::Ok;
  std::string detail;
  status = read_envelope(reader, options_.limits, code, detail);
  if (!status.ok()) {
    return status;
  }
  if (code != ErrorCode::Ok) {
    return Status{Error{code, detail}};
  }
  if (reply.header.type == MessageType::ErrorResponse) {
    return Status{make_error(ErrorCode::ProtocolViolation, "an error response carries an Ok code")};
  }
  if (reply.header.type != MessageType::HandshakeResponse) {
    return Status{make_error(ErrorCode::ProtocolViolation, "the first reply must be HandshakeResponse")};
  }
  const Status end = reader.require_end();
  if (!end.ok()) {
    return end;
  }
  if (reply.header.session.is_zero()) {
    return Status{make_error(ErrorCode::ProtocolViolation, "the server did not assign a session id")};
  }
  session_ = reply.header.session;
  next_sequence_ = header.sequence + 1;
  return Status{};
}

Status CoordinatorClient::Impl::connect(const ClientOptions& options) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (connected_.load(std::memory_order_acquire)) {
    return Status{make_error(ErrorCode::InvalidState, "the client is already connected")};
  }
  const Status limits_valid = options.limits.validate();
  if (!limits_valid.ok()) {
    return limits_valid;
  }
  if (!options.identity.valid()) {
    return Status{make_error(ErrorCode::InvalidArgument, "a client identity must be bound before connecting")};
  }
  if (options.port == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "port 0 is not a connectable endpoint")};
  }
  if (options.connect_timeout_ms > static_cast<std::uint32_t>(kMaxWaitMs)) {
    return Status{make_error(ErrorCode::InvalidArgument, "connect_timeout_ms exceeds kMaxWaitMs")};
  }

  auto candidate = Socket::connect(options.host, options.port, static_cast<int>(options.connect_timeout_ms), runtime_);
  if (!candidate.ok()) {
    return candidate.status();
  }
  socket_ = candidate.take();
  options_ = options;
  stream = FrameStream{options.limits};
  session_ = SessionId{};
  next_sequence_ = 1;
  guard_ = ReplayGuard{kReplayWindow};

  ByteWriter writer;
  writer.put_u8(static_cast<std::uint8_t>(options.identity.kind));
  writer.put_u64(options.identity.participant_id);
  writer.put_u64(options.identity.incarnation.value());
  writer.put_u64(options.identity.boot_nonce);
  const std::vector<std::byte> payload = writer.take();
  const Status bound = handshake(payload);
  if (!bound.ok()) {
    drop_connection();
    return bound;
  }
  connected_.store(true, std::memory_order_release);
  return Status{};
}

Status CoordinatorClient::Impl::close() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!socket_.valid()) {
    connected_.store(false, std::memory_order_release);
    return Status{};  // idempotent: nothing is open
  }
  if (connected_.load(std::memory_order_acquire)) {
    // A graceful close is attempted once and its outcome is not an error: the
    // contract of close() is "the connection is gone", which holds either way.
    std::vector<std::byte> body;
    static_cast<void>(transact(MessageType::CloseRequest, MessageType::CloseResponse, std::span<const std::byte>{}, body));
  }
  drop_connection();
  return Status{};
}

Status CoordinatorClient::Impl::transact(MessageType request_type, MessageType response_type,
                                         std::span<const std::byte> payload, std::vector<std::byte>& body) {
  body.clear();
  if (!connected_.load(std::memory_order_acquire)) {
    return Status{make_error(ErrorCode::SessionClosed, "the client is not connected")};
  }
  FrameHeader header;
  header.type = request_type;
  header.flags = 0;
  header.session = session_;
  header.sequence = next_sequence_;
  std::vector<std::byte> frame;
  Status status = encode_frame(header, payload, frame, options_.limits);
  if (!status.ok()) {
    return status;
  }
  status = socket_.send_all(frame, kClientWaitMs);
  if (!status.ok()) {
    drop_connection();
    return status;
  }
  DecodedFrame reply{};
  status = receive_frame(reply, kClientWaitMs);
  if (!status.ok()) {
    drop_connection();
    return status;
  }

  // A peer that is not speaking protocol version 1 as this file defines it
  // cannot be trusted to keep the stream in sync, so every violation below ends
  // the connection instead of leaving it half usable.
  const auto violation = [this](std::string_view detail) {
    drop_connection();
    return Status{make_error(ErrorCode::ProtocolViolation, detail)};
  };
  if (!reply.header.carries_response_flag()) {
    return violation("reply does not carry the Response flag");
  }
  if (reply.header.sequence != header.sequence) {
    return violation("reply does not carry the request sequence");
  }
  const Result<bool> fresh = guard_.accept(reply.header.sequence);
  if (!fresh.ok()) {
    return violation(fresh.detail());
  }
  if (!fresh.value()) {
    return violation("reply sequence was already used");
  }
  if (reply.header.session != session_) {
    return violation("reply carries another session id");
  }
  if (reply.header.type != response_type && reply.header.type != MessageType::ErrorResponse) {
    return violation("reply type is not the expected response");
  }
  ByteReader reader{reply.payload, options_.limits, "reply payload"};
  ErrorCode code = ErrorCode::Ok;
  std::string detail;
  status = read_envelope(reader, options_.limits, code, detail);
  if (!status.ok()) {
    return violation(status.detail());
  }
  if (code != ErrorCode::Ok) {
    // A refusal carries the envelope and nothing else.
    const Status end = reader.require_end();
    if (!end.ok()) {
      return violation(end.detail());
    }
    next_sequence_ = header.sequence + 1;
    return Status{Error{code, detail}};
  }
  if (reply.header.type == MessageType::ErrorResponse) {
    return violation("an error response carries an Ok code");
  }
  // The envelope is {u16 code, u32 detail_length, detail bytes}; everything
  // after it is the type-specific body.
  const std::size_t envelope_bytes = 2 + 4 + detail.size();
  if (reply.payload.size() < envelope_bytes) {
    return violation("reply payload is shorter than its envelope");
  }
  body.assign(reply.payload.begin() + static_cast<std::ptrdiff_t>(envelope_bytes), reply.payload.end());
  next_sequence_ = header.sequence + 1;
  return Status{};
}

Status CoordinatorClient::Impl::call(MessageType request_type, MessageType response_type,
                                     std::span<const std::byte> payload) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::byte> body;
  const Status status = transact(request_type, response_type, payload, body);
  if (!status.ok()) {
    return status;
  }
  if (!body.empty()) {
    drop_connection();
    return Status{make_error(ErrorCode::ProtocolViolation, "the reply carries a body it must not carry")};
  }
  return Status{};
}

Status CoordinatorClient::Impl::round_trip(MessageType request_type, MessageType response_type,
                                           std::span<const std::byte> payload, std::vector<std::byte>& body) {
  std::lock_guard<std::mutex> lock(mutex_);
  return transact(request_type, response_type, payload, body);
}

CoordinatorClient::CoordinatorClient() : impl_(std::make_unique<Impl>()) {}

CoordinatorClient::~CoordinatorClient() { static_cast<void>(close()); }

Status CoordinatorClient::connect(const ClientOptions& options) { return impl_->connect(options); }

Status CoordinatorClient::close() { return impl_->close(); }

bool CoordinatorClient::connected() const noexcept { return impl_->connected(); }

Status CoordinatorClient::open_shuffle(const ShuffleOpenRequest& request) {
  ByteWriter writer;
  writer.put_u64(request.shuffle.value());
  writer.put_u64(request.generation.value());
  writer.put_u32(request.partition_count);
  const Status encoded = encode_policy(request.policy, writer);
  if (!encoded.ok()) {
    return encoded;
  }
  return impl_->call(MessageType::OpenShuffleRequest, MessageType::OpenShuffleResponse, writer.data());
}

Result<RegistrationOutcome> CoordinatorClient::register_participant(ParticipantKind kind, std::uint64_t id,
                                                                   IncarnationId incarnation, std::string endpoint,
                                                                   const PartitionSelection& selection) {
  const Limits bounds = impl_->limits();
  ByteWriter writer;
  writer.put_u8(static_cast<std::uint8_t>(kind));
  writer.put_u64(id);
  writer.put_u64(incarnation.value());
  write_text(writer, endpoint, bounds.max_endpoint_bytes);
  const Status encoded = write_selection(writer, selection, bounds);
  if (!encoded.ok()) {
    return encoded.error();
  }
  std::vector<std::byte> body;
  const Status status = impl_->round_trip(MessageType::RegisterParticipantRequest,
                                          MessageType::RegisterParticipantResponse, writer.data(), body);
  if (!status.ok()) {
    return status.error();
  }
  ByteReader reader{body, bounds, "registration response"};
  RegistrationOutcome outcome;
  outcome.topology_generation = TopologyGeneration{reader.u64()};
  outcome.incarnation = IncarnationId{reader.u64()};
  outcome.superseded_previous = reader.boolean();
  if (!reader.ok()) {
    return reader.error();
  }
  const Status end = reader.require_end();
  if (!end.ok()) {
    return end.error();
  }
  return outcome;
}

Result<ManifestAcceptance> CoordinatorClient::publish_manifest(const PartitionManifest& manifest) {
  const Limits bounds = impl_->limits();
  const auto encoded = encode_manifest(manifest, bounds);
  if (!encoded.ok()) {
    return encoded.error();
  }
  ByteWriter writer;
  writer.put_byte_string(encoded.value());
  std::vector<std::byte> body;
  const Status status = impl_->round_trip(MessageType::PublishManifestRequest, MessageType::PublishManifestResponse,
                                          writer.data(), body);
  if (!status.ok()) {
    return status.error();
  }
  ByteReader reader{body, bounds, "publish manifest response"};
  ManifestAcceptance acceptance;
  acceptance.manifest_digest = reader.digest();
  acceptance.partition_generation = PartitionGeneration{reader.u64()};
  if (!reader.ok()) {
    return reader.error();
  }
  const Status end = reader.require_end();
  if (!end.ok()) {
    return end.error();
  }
  return acceptance;
}

Result<WavePlan> CoordinatorClient::next_wave() {
  const Limits bounds = impl_->limits();
  std::vector<std::byte> body;
  const Status status =
      impl_->round_trip(MessageType::NextWaveRequest, MessageType::NextWaveResponse, std::span<const std::byte>{}, body);
  if (!status.ok()) {
    return status.error();
  }
  ByteReader reader{body, bounds, "next wave response"};
  auto plan = read_wave_plan(reader, bounds);
  if (!plan.ok()) {
    return plan.error();
  }
  const Status end = reader.require_end();
  if (!end.ok()) {
    return end.error();
  }
  return plan.take();
}

Result<CommitOutcome> CoordinatorClient::commit_transfer(const CommitRequest& request) {
  const Limits bounds = impl_->limits();
  ByteWriter writer;
  write_commit_request(writer, request);
  std::vector<std::byte> body;
  const Status status = impl_->round_trip(MessageType::CommitTransferRequest, MessageType::CommitTransferResponse,
                                          writer.data(), body);
  if (!status.ok()) {
    return status.error();
  }
  ByteReader reader{body, bounds, "commit transfer response"};
  CommitOutcome outcome;
  outcome.receipt.newly_committed = reader.boolean();
  outcome.receipt.edge_newly_completed = reader.boolean();
  outcome.receipt.duplicate = reader.boolean();
  outcome.receipt.sequence = CommitSequence{reader.u64()};
  outcome.receipt.manifest_digest = reader.digest();
  outcome.receipt.accounted_bytes = reader.u64();
  outcome.shuffle_completed = reader.boolean();
  if (!reader.ok()) {
    return reader.error();
  }
  const Status end = reader.require_end();
  if (!end.ok()) {
    return end.error();
  }
  return outcome;
}

Result<FailureOutcome> CoordinatorClient::report_failure(TransferAttemptId attempt, ErrorCode code) {
  const Limits bounds = impl_->limits();
  ByteWriter writer;
  writer.put_u64(attempt.value());
  writer.put_u16(static_cast<std::uint16_t>(code));
  std::vector<std::byte> body;
  const Status status = impl_->round_trip(MessageType::ReportFailureRequest, MessageType::ReportFailureResponse,
                                          writer.data(), body);
  if (!status.ok()) {
    return status.error();
  }
  ByteReader reader{body, bounds, "report failure response"};
  FailureOutcome outcome;
  outcome.permanent = reader.boolean();
  outcome.attempts = reader.u32();
  outcome.ready_at = TickId{reader.u64()};
  if (!reader.ok()) {
    return reader.error();
  }
  const Status end = reader.require_end();
  if (!end.ok()) {
    return end.error();
  }
  return outcome;
}

Status CoordinatorClient::report_congestion(const CongestionIntent& intent) {
  ByteWriter writer;
  writer.put_u8(static_cast<std::uint8_t>(intent.reporter_kind));
  writer.put_u64(intent.reporter_id);
  writer.put_u64(intent.reporter_incarnation.value());
  writer.put_u64(intent.producer.value());
  writer.put_u64(intent.consumer.value());
  writer.put_u32(intent.level);
  writer.put_u64(intent.policy_generation.value());
  writer.put_u64(intent.observed_at.value());
  return impl_->call(MessageType::CongestionIntentRequest, MessageType::CongestionIntentResponse, writer.data());
}

Status CoordinatorClient::cancel_shuffle(ErrorCode reason) {
  ByteWriter writer;
  writer.put_u16(static_cast<std::uint16_t>(reason));
  return impl_->call(MessageType::CancelShuffleRequest, MessageType::CancelShuffleResponse, writer.data());
}

Result<PartitionManifest> CoordinatorClient::manifest(PartitionId partition) {
  const Limits bounds = impl_->limits();
  ByteWriter writer;
  writer.put_u64(partition.value());
  std::vector<std::byte> body;
  const Status status = impl_->round_trip(MessageType::ManifestRequest, MessageType::ManifestResponse, writer.data(), body);
  if (!status.ok()) {
    return status.error();
  }
  ByteReader reader{body, bounds, "manifest response"};
  const std::span<const std::byte> encoded = reader.bytes(bounds.max_message_payload_bytes);
  if (!reader.ok()) {
    return reader.error();
  }
  const Status end = reader.require_end();
  if (!end.ok()) {
    return end.error();
  }
  return decode_manifest(encoded, bounds);
}

Result<ProgressSnapshot> CoordinatorClient::progress() {
  const Limits bounds = impl_->limits();
  std::vector<std::byte> body;
  const Status status = impl_->round_trip(MessageType::ProgressRequest, MessageType::ProgressResponse,
                                          std::span<const std::byte>{}, body);
  if (!status.ok()) {
    return status.error();
  }
  ByteReader reader{body, bounds, "progress response"};
  auto snapshot = read_progress_snapshot(reader);
  if (!snapshot.ok()) {
    return snapshot.error();
  }
  const Status end = reader.require_end();
  if (!end.ok()) {
    return end.error();
  }
  return snapshot.take();
}

Result<Explanation> CoordinatorClient::explain(std::uint32_t max_samples_per_code) {
  const Limits bounds = impl_->limits();
  ByteWriter writer;
  writer.put_u32(max_samples_per_code);
  std::vector<std::byte> body;
  const Status status = impl_->round_trip(MessageType::ExplainRequest, MessageType::ExplainResponse, writer.data(), body);
  if (!status.ok()) {
    return status.error();
  }
  ByteReader reader{body, bounds, "explain response"};
  auto explanation = read_explanation(reader, bounds);
  if (!explanation.ok()) {
    return explanation.error();
  }
  const Status end = reader.require_end();
  if (!end.ok()) {
    return end.error();
  }
  return explanation.take();
}

Result<CoordinatorStatus> CoordinatorClient::status() {
  const Limits bounds = impl_->limits();
  std::vector<std::byte> body;
  const Status status = impl_->round_trip(MessageType::StatusRequest, MessageType::StatusResponse,
                                          std::span<const std::byte>{}, body);
  if (!status.ok()) {
    return status.error();
  }
  ByteReader reader{body, bounds, "status response"};
  auto snapshot = read_status(reader);
  if (!snapshot.ok()) {
    return snapshot.error();
  }
  const Status end = reader.require_end();
  if (!end.ok()) {
    return end.error();
  }
  return snapshot.take();
}

// ---------------------------------------------------------------------------
// CoordinatorServer public surface
// ---------------------------------------------------------------------------

CoordinatorServer::CoordinatorServer(Coordinator& coordinator, ServerOptions options)
    : impl_(std::make_unique<Impl>(coordinator, std::move(options))) {}

CoordinatorServer::~CoordinatorServer() { static_cast<void>(impl_->stop()); }

Status CoordinatorServer::start() { return impl_->start(); }

Status CoordinatorServer::stop() { return impl_->stop(); }

bool CoordinatorServer::running() const noexcept { return impl_->running(); }

std::uint16_t CoordinatorServer::port() const noexcept { return impl_->port(); }

ServerStats CoordinatorServer::stats() const { return impl_->stats(); }

}  // namespace shuffle::fabric
