// Durable record vocabulary of the coordinator.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Every durable mutation is one of these records. The same decode -> apply
// path is used during normal operation and during recovery, so replay cannot
// drift from live behaviour. Congestion evidence is deliberately absent: a
// pressure reading is dynamic evidence and must never be resurrected from
// durable storage as if it were current.

#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "shuffle/fabric/codec.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/identity.hpp"
#include "shuffle/fabric/ledger.hpp"
#include "shuffle/fabric/manifest.hpp"
#include "shuffle/fabric/policy.hpp"
#include "shuffle/fabric/topology.hpp"

namespace shuffle::fabric {

enum class RecordKind : std::uint16_t {
  EpochAdvanced = 1,
  ShuffleState = 2,
  Participant = 3,
  Manifest = 4,
  Commit = 5,
  Failure = 6,
};
[[nodiscard]] const char* to_string(RecordKind kind) noexcept;

enum class ShuffleState : std::uint8_t {
  Closed = 0,
  Open = 1,
  Cancelling = 2,
  Cancelled = 3,
  Completed = 4,
  Failed = 5,
};
[[nodiscard]] const char* to_string(ShuffleState state) noexcept;

struct EpochRecord {
  std::uint64_t epoch{0};
  TickId opened_at{};

  void encode(ByteWriter& writer) const;
  [[nodiscard]] static Result<EpochRecord> decode(ByteReader& reader);
};

// One record describes the shuffle's identity and its current lifecycle state,
// so a restart replays open, cancel and completion from the same vocabulary.
struct ShuffleStateRecord {
  ShuffleId shuffle{};
  ShuffleGeneration generation{};
  std::uint32_t partition_count{0};
  PolicyEnvelope policy{};
  ShuffleState state{ShuffleState::Closed};
  ErrorCode reason{ErrorCode::Ok};
  TickId at{};

  void encode(ByteWriter& writer) const;
  [[nodiscard]] static Result<ShuffleStateRecord> decode(ByteReader& reader);
};

struct ParticipantRecord {
  ParticipantKind kind{ParticipantKind::Producer};
  std::uint64_t id{0};
  IncarnationId incarnation{};
  ParticipantState state{ParticipantState::Pending};
  std::string endpoint{};
  PartitionSelection selection{};
  TickId at{};

  void encode(ByteWriter& writer) const;
  [[nodiscard]] static Result<ParticipantRecord> decode(ByteReader& reader, const Limits& limits = Limits{});
};

struct ManifestRecord {
  PartitionManifest manifest{};

  [[nodiscard]] Status encode(ByteWriter& writer, const Limits& limits) const;
  [[nodiscard]] static Result<ManifestRecord> decode(ByteReader& reader, const Limits& limits = Limits{});
};

struct CommitRecord {
  CommitRequest request{};
  TickId at{};

  void encode(ByteWriter& writer) const;
  [[nodiscard]] static Result<CommitRecord> decode(ByteReader& reader, const Limits& limits = Limits{});
};

struct FailureRecordDurable {
  EdgeKey key{};
  ErrorCode code{ErrorCode::InternalError};
  bool permanent{false};
  std::uint32_t attempts{0};
  TickId recorded_at{};
  TickId ready_at{};

  void encode(ByteWriter& writer) const;
  [[nodiscard]] static Result<FailureRecordDurable> decode(ByteReader& reader);
};

// Reads the leading kind tag. Fails with StateCorrupt for an unknown tag.
[[nodiscard]] Result<RecordKind> peek_record_kind(ByteReader& reader);

}  // namespace shuffle::fabric
