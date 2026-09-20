// The coordinator service: the distributed form of the authority model.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// The service is a thin, strict envelope around one Coordinator. It adds no
// authority of its own: it binds a session identity to a connection, decodes a
// request, calls the coordinator under a single serialising mutex, and answers
// with exactly one response frame. Event emission never happens while that
// mutex is held, and the coordinator itself refuses re-entrant entry, so a
// callback-under-lock defect is detected rather than tolerated.
//
// Wire contract (protocol version 1, see frame.hpp for the framing itself):
//   * A connection begins with HandshakeRequest, whose payload is
//     {u8 ParticipantKind, u64 participant_id, u64 incarnation, u64 boot_nonce}.
//     The server answers HandshakeResponse carrying the uniform reply envelope.
//   * Every reply payload starts with the uniform envelope
//     {u16 ErrorCode, u32 detail_length, detail bytes}; when the code is not Ok
//     the reply carries no further bytes.
//   * Requests are attributed to the session identity established by the
//     handshake. A request that carries a contradicting identity is refused
//     with IdentityMismatch rather than believed.
//   * A duplicated sequence number on one connection re-sends the previous
//     answer instead of applying the request twice.
//
// Payload layouts (canonical little-endian, ByteWriter/ByteReader):
//   OpenShuffleRequest  {u64 shuffle, u64 generation, u32 partition_count, policy}
//   RegisterParticipant {u8 kind, u64 id, u64 incarnation, string endpoint,
//                        u8 selection_kind, u64 begin, u64 end, u32 list_count, u64[]}
//   PublishManifest     {encoded manifest (manifest.hpp)}
//   NextWave            {} -- request carries no payload
//   CommitTransfer      {CommitRequest encoding, see service.cpp}
//   ReportFailure       {u64 attempt, u16 error_code}
//   CongestionIntent    {u8 reporter_kind, u64 reporter_id, u64 reporter_incarnation,
//                        u64 producer, u64 consumer, u32 level, u64 policy_generation,
//                        u64 observed_at}
//   CancelShuffle       {u16 reason}
//   ManifestRequest     {u64 partition}
//   ProgressRequest     {} / ExplainRequest {u32 max_samples} / StatusRequest {} / CloseRequest {}
//   ProgressResponse    {progress snapshot}
//   ExplainResponse     {u32 entry_count, {u16 code, string subject, string detail, u64 count}[]}
//   StatusResponse      {CoordinatorStatus encoding}
//   ManifestResponse    {encoded manifest}
//   NextWaveResponse    {u64 wave, u32 grant_count, grants...} where a grant is
//                        {u64 attempt, u64 wave, u64 shuffle, u64 shuffle_generation,
//                         u64 partition, u64 partition_generation, u64 producer,
//                         u64 producer_incarnation, string producer_endpoint,
//                         u64 consumer, u64 consumer_incarnation,
//                         u64 topology_generation, u64 policy_generation,
//                         digest manifest_digest, u64 total_bytes,
//                         u32 attempt_ordinal, u64 issued_at}
//                        followed by the plan counters (examined, skipped_*,
//                        deferred_*, cursor_wrapped, all_resolved).
//   CommitTransferResponse {u8 newly_committed, u8 edge_newly_completed, u8 duplicate,
//                           u64 sequence, digest manifest_digest, u64 accounted_bytes,
//                           u8 shuffle_completed}
//   ChunkFetchRequest   {u64 shuffle, u64 shuffle_generation, u64 partition,
//                        u64 partition_generation, u64 chunk}
//   ChunkFetchResponse  {u64 chunk, u64 offset, u32 length, digest, u32 payload_length, payload}
//   ChunkFetchFailure   {u16 error_code, string detail}
//
// The data plane (ChunkFetch*) is deliberately separate from the management
// plane: participants exchange chunks directly, and the coordinator never
// carries bulk data.

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "shuffle/fabric/coordinator.hpp"
#include "shuffle/fabric/frame.hpp"
#include "shuffle/fabric/manifest.hpp"
#include "shuffle/fabric/socket.hpp"

namespace shuffle::fabric {

struct SessionIdentity {
  ParticipantKind kind{ParticipantKind::Producer};
  std::uint64_t participant_id{0};
  IncarnationId incarnation{};
  std::uint64_t boot_nonce{0};

  [[nodiscard]] bool valid() const noexcept { return participant_id != 0 && !incarnation.is_zero(); }
};

struct ServerOptions {
  Limits limits{};
  std::string bind_host{"127.0.0.1"};
  std::uint16_t port{0};
  std::uint32_t max_sessions{64};
  // A session worker serves one connection for that connection's lifetime, so
  // the pool is always sized to serve every admitted session concurrently: a
  // smaller value is raised to max_sessions rather than leaving admitted
  // clients waiting for a worker that never frees up.
  std::uint32_t worker_threads{4};
  std::uint32_t accept_backlog{16};
};

struct ClientOptions {
  Limits limits{};
  std::string host{"127.0.0.1"};
  std::uint16_t port{0};
  SessionIdentity identity{};
  // A bounded wait for the connect handshake only. It never decides whether an
  // operation succeeded: correctness comes from the protocol, not from timing.
  std::uint32_t connect_timeout_ms{2000};
};

struct ServerStats {
  std::uint64_t accepted_sessions{0};
  std::uint64_t refused_sessions{0};
  std::uint64_t served_requests{0};
  std::uint64_t refused_requests{0};
  std::uint64_t duplicate_requests{0};
  std::uint64_t replayed_answers{0};
  std::uint32_t active_sessions{0};
  std::uint32_t peak_sessions{0};
};

class CoordinatorServer {
 public:
  CoordinatorServer(Coordinator& coordinator, ServerOptions options);
  ~CoordinatorServer();
  CoordinatorServer(const CoordinatorServer&) = delete;
  CoordinatorServer& operator=(const CoordinatorServer&) = delete;
  CoordinatorServer(CoordinatorServer&&) = delete;
  CoordinatorServer& operator=(CoordinatorServer&&) = delete;

  // Binds a real listener and starts the worker pool. Refuses a second start.
  [[nodiscard]] Status start();
  // Stops accepting, wakes and closes sessions, joins every worker. Idempotent,
  // and safe to call from a session worker.
  [[nodiscard]] Status stop();
  [[nodiscard]] bool running() const noexcept;
  [[nodiscard]] std::uint16_t port() const noexcept;
  [[nodiscard]] ServerStats stats() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

class CoordinatorClient {
 public:
  CoordinatorClient();
  ~CoordinatorClient();
  CoordinatorClient(const CoordinatorClient&) = delete;
  CoordinatorClient& operator=(const CoordinatorClient&) = delete;
  CoordinatorClient(CoordinatorClient&&) = delete;
  CoordinatorClient& operator=(CoordinatorClient&&) = delete;

  [[nodiscard]] Status connect(const ClientOptions& options);
  [[nodiscard]] Status close();
  [[nodiscard]] bool connected() const noexcept;

  [[nodiscard]] Status open_shuffle(const ShuffleOpenRequest& request);
  [[nodiscard]] Result<RegistrationOutcome> register_participant(ParticipantKind kind, std::uint64_t id,
                                                                 IncarnationId incarnation, std::string endpoint,
                                                                 const PartitionSelection& selection);
  [[nodiscard]] Result<ManifestAcceptance> publish_manifest(const PartitionManifest& manifest);
  [[nodiscard]] Result<WavePlan> next_wave();
  [[nodiscard]] Result<CommitOutcome> commit_transfer(const CommitRequest& request);
  [[nodiscard]] Result<FailureOutcome> report_failure(TransferAttemptId attempt, ErrorCode code);
  [[nodiscard]] Status report_congestion(const CongestionIntent& intent);
  [[nodiscard]] Status cancel_shuffle(ErrorCode reason);
  [[nodiscard]] Result<PartitionManifest> manifest(PartitionId partition);
  [[nodiscard]] Result<ProgressSnapshot> progress();
  [[nodiscard]] Result<Explanation> explain(std::uint32_t max_samples_per_code);
  [[nodiscard]] Result<CoordinatorStatus> status();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace shuffle::fabric
