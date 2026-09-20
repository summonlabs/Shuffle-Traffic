// Deterministic wave planning under bounded concurrency.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Planning is not authority. A wave plan is a proposal: the coordinator decides
// which grants actually become dispatchable authority. The scheduler never
// sleeps, never consults a wall clock and never performs I/O, so the same
// topology, policy, ledger state and tick produce the same wave.

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "shuffle/fabric/bytes.hpp"
#include "shuffle/fabric/edge.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/identity.hpp"
#include "shuffle/fabric/policy.hpp"
#include "shuffle/fabric/topology.hpp"

namespace shuffle::fabric {

// A grant is the evidence that authority existed at planning time. It carries
// every generation it was issued under, so a stale holder cannot present it
// later and be believed.
struct DispatchGrant {
  TransferAttemptId attempt{};
  WaveId wave{};
  ShuffleId shuffle{};
  ShuffleGeneration shuffle_generation{};
  PartitionId partition{};
  PartitionGeneration partition_generation{};
  ProducerId producer{};
  IncarnationId producer_incarnation{};
  // Where the producing incarnation serves chunk data. Carried in the grant so
  // a consumer never has to guess or trust an out-of-band directory.
  std::string producer_endpoint{};
  ConsumerId consumer{};
  IncarnationId consumer_incarnation{};
  TopologyGeneration topology_generation{};
  PolicyGeneration policy_generation{};
  Digest manifest_digest{};
  std::uint64_t total_bytes{0};
  std::uint32_t attempt_ordinal{1};
  TickId issued_at{};
};

struct WavePlan {
  WaveId id{};
  std::vector<DispatchGrant> grants{};

  std::uint32_t examined{0};
  std::uint32_t skipped_completed{0};
  std::uint32_t skipped_failed{0};
  std::uint32_t skipped_in_flight{0};
  std::uint32_t skipped_unproduced{0};
  std::uint32_t deferred_limits{0};
  std::uint32_t deferred_source_pressure{0};
  std::uint32_t deferred_destination_pressure{0};
  std::uint32_t deferred_unknown_pressure{0};
  std::uint32_t deferred_retry_wait{0};

  bool cursor_wrapped{false};
  // True when the cursor completed a full pass with no grant because every
  // required edge is already resolved. Only then is the shuffle finished.
  bool all_resolved{false};
};

struct SchedulingEnvironment {
  const Topology* topology{nullptr};
  const CompletionView* completion{nullptr};
  const CongestionView* congestion{nullptr};
  const PartitionView* partitions{nullptr};
  TickId now{};
};

struct SchedulerStats {
  std::uint64_t waves_planned{0};
  std::uint64_t grants_issued{0};
  std::uint64_t edges_examined{0};
  std::uint64_t edges_skipped_resolved{0};
  std::uint64_t attempts_resolved{0};
  std::uint64_t grants_abandoned{0};
};

class WaveScheduler {
 public:
  explicit WaveScheduler(const Limits& limits);

  // Binds the scheduler to one shuffle generation, policy generation and
  // topology generation. Planning under any other generation is refused.
  [[nodiscard]] Status configure(const PolicyEnvelope& policy, const Topology& topology);

  [[nodiscard]] bool configured() const noexcept { return configured_; }
  [[nodiscard]] TopologyGeneration topology_generation() const noexcept { return topology_generation_; }
  [[nodiscard]] PolicyGeneration policy_generation() const noexcept { return policy_generation_; }
  [[nodiscard]] ShuffleGeneration shuffle_generation() const noexcept { return shuffle_generation_; }
  [[nodiscard]] const PolicyEnvelope& policy() const noexcept { return policy_; }
  [[nodiscard]] std::uint32_t in_flight() const noexcept { return global_in_flight_; }
  [[nodiscard]] bool has_in_flight() const noexcept { return global_in_flight_ != 0; }
  [[nodiscard]] const SchedulerStats& stats() const noexcept { return stats_; }

  // Produces the next bounded batch of grants. Work per call is bounded by
  // max_edges_examined_per_wave regardless of the size of the edge space.
  [[nodiscard]] Result<WavePlan> next_wave(const SchedulingEnvironment& environment);

  // Records the outcome of a grant. Unknown attempts are refused: a late or
  // forged completion cannot perturb counters it never owned.
  [[nodiscard]] Status resolve(TransferAttemptId attempt, EdgeOutcome outcome, ErrorCode code);

  // Cancellation: every in-flight grant loses authority immediately and is
  // reported as Cancelled. No late authority is possible afterwards.
  [[nodiscard]] Status abandon_all(ErrorCode code);

  // Number of attempts currently in flight for a producer or consumer.
  [[nodiscard]] std::uint32_t in_flight_for_producer(ProducerId id) const;
  [[nodiscard]] std::uint32_t in_flight_for_consumer(ConsumerId id) const;

 private:
  struct AttemptRecord {
    EdgeKey key{};
    WaveId wave{};
    std::uint32_t ordinal{1};
    ProducerId producer{};
    ConsumerId consumer{};
  };

  Limits limits_{};
  PolicyEnvelope policy_{};
  bool configured_{false};
  ShuffleId shuffle_{};
  ShuffleGeneration shuffle_generation_{};
  TopologyGeneration topology_generation_{};
  PolicyGeneration policy_generation_{};

  std::uint32_t partition_count_{0};
  std::uint32_t partition_cursor_{0};
  std::uint32_t pattern_cursor_{0};
  std::uint32_t consumer_cursor_{0};

  WaveId next_wave_id_{1};
  TransferAttemptId next_attempt_id_{1};

  std::uint32_t global_in_flight_{0};
  std::unordered_map<std::uint64_t, std::uint32_t> producer_in_flight_{};
  std::unordered_map<std::uint64_t, std::uint32_t> consumer_in_flight_{};
  std::unordered_map<std::uint64_t, AttemptRecord> attempts_{};
  std::unordered_map<EdgeKey, TransferAttemptId> edge_attempts_{};
  std::unordered_map<std::uint64_t, bool> producer_paused_{};
  std::unordered_map<std::uint64_t, bool> consumer_paused_{};
  SchedulerStats stats_{};
};

}  // namespace shuffle::fabric
