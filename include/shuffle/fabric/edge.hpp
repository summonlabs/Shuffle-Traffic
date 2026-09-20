// Edge identity and the read-only views the scheduler depends on.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// An edge is one required (partition generation, consumer) delivery. The
// scheduler never mutates the ledger or the manifest store: it reads through
// these interfaces and reports decisions, which keeps authority and planning
// strictly separate.

#pragma once

#include <compare>
#include <cstdint>
#include <functional>
#include <string>

#include "shuffle/fabric/digest.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/identity.hpp"

namespace shuffle::fabric {

struct EdgeKey {
  PartitionId partition{};
  PartitionGeneration partition_generation{};
  ConsumerId consumer{};

  friend bool operator==(const EdgeKey&, const EdgeKey&) noexcept = default;
  friend auto operator<=>(const EdgeKey&, const EdgeKey&) noexcept = default;
  [[nodiscard]] std::string to_string() const;
};

enum class EdgePhase : std::uint8_t {
  Unknown = 0,     // not part of the required edge set, or history was purged
  Pending = 1,     // required, not yet authoritatively completed
  Dispatched = 2,  // an attempt is in flight
  Completed = 3,   // authoritatively committed
  Failed = 4,      // permanently failed; explicitly recorded, never silent
  Purged = 5,      // the partition generation is older than retained history
};
[[nodiscard]] const char* to_string(EdgePhase phase) noexcept;

struct EdgeStatus {
  EdgePhase phase{EdgePhase::Unknown};
  ErrorCode reason{ErrorCode::UnknownPartition};
  std::uint32_t attempts{0};
  TickId ready_at{};
  IncarnationId producer_incarnation{};
  Digest manifest_digest{};
  std::uint64_t bytes{0};
  bool authoritative{false};  // true only for Completed: a durable commit exists

  [[nodiscard]] bool resolved() const noexcept {
    return phase == EdgePhase::Completed || phase == EdgePhase::Failed || phase == EdgePhase::Purged;
  }
};

// Pressure is evidence, never authority: a reading can defer dispatch, it can
// never authorise or complete one.
struct PressureReading {
  std::uint32_t level{0};
  bool observed{false};
  bool fresh{false};
  TickId observed_at{};
  IncarnationId reporter{};
  PolicyGeneration policy_generation{};

  [[nodiscard]] static PressureReading unknown() noexcept { return PressureReading{}; }
};

// Facts about the currently produced generation of a partition.
struct PartitionFacts {
  bool produced{false};
  PartitionGeneration partition_generation{};
  Digest manifest_digest{};
  std::uint64_t total_bytes{0};
  IncarnationId producer_incarnation{};
  ProducerId producer{};
};

// Terminal classification of a dispatched attempt, as decided by the
// coordinator. The scheduler uses it to update counters and pressure only.
enum class EdgeOutcome : std::uint8_t {
  Completed = 0,
  RetriableFailure = 1,
  PermanentFailure = 2,
  AuthorityRefused = 3,
  Cancelled = 4,
};
[[nodiscard]] const char* to_string(EdgeOutcome outcome) noexcept;

class CompletionView {
 public:
  CompletionView() = default;
  virtual ~CompletionView() = default;

  [[nodiscard]] virtual EdgeStatus edge_status(const EdgeKey& key) const = 0;
};

class CongestionView {
 public:
  CongestionView() = default;
  virtual ~CongestionView() = default;

  [[nodiscard]] virtual PressureReading producer_pressure(ProducerId id) const = 0;
  [[nodiscard]] virtual PressureReading consumer_pressure(ConsumerId id) const = 0;
};

class PartitionView {
 public:
  PartitionView() = default;
  virtual ~PartitionView() = default;

  [[nodiscard]] virtual PartitionFacts partition_facts(PartitionId id) const = 0;
};

}  // namespace shuffle::fabric

namespace std {

template <>
struct hash<shuffle::fabric::EdgeKey> {
  [[nodiscard]] size_t operator()(const shuffle::fabric::EdgeKey& key) const noexcept {
    std::uint64_t hash = 1469598103934665603ull;
    const std::uint64_t values[3] = {key.partition.value(), key.partition_generation.value(), key.consumer.value()};
    for (const std::uint64_t value : values) {
      hash ^= value;
      hash *= 1099511628211ull;
    }
    return static_cast<size_t>(hash);
  }
};

}  // namespace std
