// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/records.hpp"

#include <algorithm>

namespace shuffle::fabric {
namespace {

void write_selection(ByteWriter& writer, const PartitionSelection& selection) {
  writer.put_u8(static_cast<std::uint8_t>(selection.kind));
  writer.put_u64(selection.begin.value());
  writer.put_u64(selection.end.value());
  writer.put_u32(static_cast<std::uint32_t>(selection.list.size()));
  for (const PartitionId partition : selection.list) {
    writer.put_u64(partition.value());
  }
}

Result<PartitionSelection> read_selection(ByteReader& reader, const Limits& limits) {
  PartitionSelection selection;
  const std::uint8_t kind = reader.u8();
  if (kind > static_cast<std::uint8_t>(SelectionKind::List)) {
    return make_failure<PartitionSelection>(ErrorCode::StateImpossible, "durable record holds an unknown selection kind");
  }
  selection.kind = static_cast<SelectionKind>(kind);
  selection.begin = PartitionId{reader.u64()};
  selection.end = PartitionId{reader.u64()};
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
  if (selection.kind == SelectionKind::List) {
    if (selection.list.empty() || !std::is_sorted(selection.list.begin(), selection.list.end()) ||
        std::adjacent_find(selection.list.begin(), selection.list.end()) != selection.list.end()) {
      return make_failure<PartitionSelection>(ErrorCode::StateCorrupt, "durable selection list is not canonical");
    }
  } else if (!selection.list.empty()) {
    return make_failure<PartitionSelection>(ErrorCode::StateCorrupt, "durable selection carries a list it cannot use");
  }
  return selection;
}

void write_kind(ByteWriter& writer, RecordKind kind) {
  writer.put_u16(static_cast<std::uint16_t>(kind));
}

[[nodiscard]] Result<RecordKind> read_kind(ByteReader& reader) {
  const std::uint16_t raw = reader.u16();
  if (!reader.ok()) {
    return Result<RecordKind>{reader.error()};
  }
  const auto kind = static_cast<RecordKind>(raw);
  switch (kind) {
    case RecordKind::EpochAdvanced:
    case RecordKind::ShuffleState:
    case RecordKind::Participant:
    case RecordKind::Manifest:
    case RecordKind::Commit:
    case RecordKind::Failure:
      return kind;
  }
  return make_failure<RecordKind>(ErrorCode::StateCorrupt, "durable record holds an unknown kind tag");
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
  if (request.edge.consumer != request.consumer) {
    return make_failure<CommitRequest>(ErrorCode::StateCorrupt, "durable commit disagrees about its consumer identity");
  }
  return request;
}

}  // namespace

const char* to_string(RecordKind kind) noexcept {
  switch (kind) {
    case RecordKind::EpochAdvanced: return "EpochAdvanced";
    case RecordKind::ShuffleState: return "ShuffleState";
    case RecordKind::Participant: return "Participant";
    case RecordKind::Manifest: return "Manifest";
    case RecordKind::Commit: return "Commit";
    case RecordKind::Failure: return "Failure";
  }
  return "Unknown";
}

const char* to_string(ShuffleState state) noexcept {
  switch (state) {
    case ShuffleState::Closed: return "Closed";
    case ShuffleState::Open: return "Open";
    case ShuffleState::Cancelling: return "Cancelling";
    case ShuffleState::Cancelled: return "Cancelled";
    case ShuffleState::Completed: return "Completed";
    case ShuffleState::Failed: return "Failed";
  }
  return "Unknown";
}

void EpochRecord::encode(ByteWriter& writer) const {
  write_kind(writer, RecordKind::EpochAdvanced);
  writer.put_u64(epoch);
  writer.put_u64(opened_at.value());
}

Result<EpochRecord> EpochRecord::decode(ByteReader& reader) {
  EpochRecord record;
  record.epoch = reader.u64();
  record.opened_at = TickId{reader.u64()};
  if (!reader.ok()) {
    return Result<EpochRecord>{reader.error()};
  }
  return record;
}

void ShuffleStateRecord::encode(ByteWriter& writer) const {
  write_kind(writer, RecordKind::ShuffleState);
  writer.put_u64(shuffle.value());
  writer.put_u64(generation.value());
  writer.put_u32(partition_count);
  writer.put_u16(static_cast<std::uint16_t>(state));
  writer.put_u16(static_cast<std::uint16_t>(reason));
  writer.put_u64(at.value());
  // Encoding an already validated envelope has no failure mode.
  static_cast<void>(encode_policy(policy, writer));
}

Result<ShuffleStateRecord> ShuffleStateRecord::decode(ByteReader& reader) {
  ShuffleStateRecord record;
  record.shuffle = ShuffleId{reader.u64()};
  record.generation = ShuffleGeneration{reader.u64()};
  record.partition_count = reader.u32();
  const std::uint16_t state = reader.u16();
  const std::uint16_t reason = reader.u16();
  record.at = TickId{reader.u64()};
  if (!reader.ok()) {
    return Result<ShuffleStateRecord>{reader.error()};
  }
  if (state > static_cast<std::uint16_t>(ShuffleState::Failed)) {
    return make_failure<ShuffleStateRecord>(ErrorCode::StateImpossible, "durable shuffle state is unknown");
  }
  record.state = static_cast<ShuffleState>(state);
  record.reason = static_cast<ErrorCode>(reason);
  const auto policy = decode_policy(reader);
  if (!policy.ok()) {
    return Result<ShuffleStateRecord>{policy.error()};
  }
  record.policy = policy.value();
  if (record.state == ShuffleState::Open && record.reason != ErrorCode::Ok) {
    return make_failure<ShuffleStateRecord>(ErrorCode::StateImpossible, "open shuffle carries a failure reason");
  }
  return record;
}

void ParticipantRecord::encode(ByteWriter& writer) const {
  write_kind(writer, RecordKind::Participant);
  writer.put_u8(static_cast<std::uint8_t>(kind));
  writer.put_u64(id);
  writer.put_u64(incarnation.value());
  writer.put_u8(static_cast<std::uint8_t>(state));
  writer.put_string(endpoint);
  write_selection(writer, selection);
  writer.put_u64(at.value());
}

Result<ParticipantRecord> ParticipantRecord::decode(ByteReader& reader, const Limits& limits) {
  ParticipantRecord record;
  const std::uint8_t kind = reader.u8();
  record.id = reader.u64();
  record.incarnation = IncarnationId{reader.u64()};
  const std::uint8_t state = reader.u8();
  record.endpoint = reader.string(limits.max_endpoint_bytes);
  if (!reader.ok()) {
    return Result<ParticipantRecord>{reader.error()};
  }
  if (kind != static_cast<std::uint8_t>(ParticipantKind::Producer) &&
      kind != static_cast<std::uint8_t>(ParticipantKind::Consumer)) {
    return make_failure<ParticipantRecord>(ErrorCode::StateImpossible, "durable participant kind is unknown");
  }
  if (state > static_cast<std::uint8_t>(ParticipantState::Withdrawn)) {
    return make_failure<ParticipantRecord>(ErrorCode::StateImpossible, "durable participant state is unknown");
  }
  record.kind = static_cast<ParticipantKind>(kind);
  record.state = static_cast<ParticipantState>(state);
  const auto selection = read_selection(reader, limits);
  if (!selection.ok()) {
    return Result<ParticipantRecord>{selection.error()};
  }
  record.selection = selection.value();
  record.at = TickId{reader.u64()};
  if (!reader.ok()) {
    return Result<ParticipantRecord>{reader.error()};
  }
  return record;
}

Status ManifestRecord::encode(ByteWriter& writer, const Limits& limits) const {
  write_kind(writer, RecordKind::Manifest);
  const auto encoded = encode_manifest(manifest, limits);
  if (!encoded.ok()) {
    return encoded.status();
  }
  writer.put_byte_string(encoded.value());
  return Status{};
}

Result<ManifestRecord> ManifestRecord::decode(ByteReader& reader, const Limits& limits) {
  ManifestRecord record;
  const auto bytes = reader.bytes(limits.max_message_payload_bytes);
  if (!reader.ok()) {
    return Result<ManifestRecord>{reader.error()};
  }
  const auto manifest = decode_manifest(bytes, limits);
  if (!manifest.ok()) {
    return Result<ManifestRecord>{manifest.error()};
  }
  record.manifest = manifest.value();
  return record;
}

void CommitRecord::encode(ByteWriter& writer) const {
  write_kind(writer, RecordKind::Commit);
  write_commit_request(writer, request);
  writer.put_u64(at.value());
}

Result<CommitRecord> CommitRecord::decode(ByteReader& reader, const Limits& limits) {
  const auto request = read_commit_request(reader, limits);
  if (!request.ok()) {
    return Result<CommitRecord>{request.error()};
  }
  CommitRecord record;
  record.request = request.value();
  record.at = TickId{reader.u64()};
  if (!reader.ok()) {
    return Result<CommitRecord>{reader.error()};
  }
  return record;
}

void FailureRecordDurable::encode(ByteWriter& writer) const {
  write_kind(writer, RecordKind::Failure);
  writer.put_u64(key.partition.value());
  writer.put_u64(key.partition_generation.value());
  writer.put_u64(key.consumer.value());
  writer.put_u16(static_cast<std::uint16_t>(code));
  writer.put_bool(permanent);
  writer.put_u32(attempts);
  writer.put_u64(recorded_at.value());
  writer.put_u64(ready_at.value());
}

Result<FailureRecordDurable> FailureRecordDurable::decode(ByteReader& reader) {
  FailureRecordDurable record;
  record.key.partition = PartitionId{reader.u64()};
  record.key.partition_generation = PartitionGeneration{reader.u64()};
  record.key.consumer = ConsumerId{reader.u64()};
  const std::uint16_t code = reader.u16();
  record.permanent = reader.boolean();
  record.attempts = reader.u32();
  record.recorded_at = TickId{reader.u64()};
  record.ready_at = TickId{reader.u64()};
  if (!reader.ok()) {
    return Result<FailureRecordDurable>{reader.error()};
  }
  if (code == 0 || code > static_cast<std::uint16_t>(ErrorCode::TooManyTrackedEdges)) {
    return make_failure<FailureRecordDurable>(ErrorCode::StateImpossible, "durable failure carries an unknown error code");
  }
  record.code = static_cast<ErrorCode>(code);
  return record;
}

Result<RecordKind> peek_record_kind(ByteReader& reader) { return read_kind(reader); }

}  // namespace shuffle::fabric
