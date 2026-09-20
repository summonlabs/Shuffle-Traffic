// Topology: who participates, on which incarnation, and which partitions each
// consumer requires.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Two properties matter here and are enforced by construction:
//
//   * deterministic ownership -- partition p belongs to the p-th active
//     producer in ascending identifier order, so independent processes derive
//     the same assignment without coordination;
//
//   * bounded edge expansion -- consumer interest is interned into a small set
//     of distinct selection patterns. Coverage of a partition is answered from
//     the pattern set, so the runtime never materialises the N x M edge matrix
//     and the planned edge count is computed in closed form.

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "shuffle/fabric/bytes.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/identity.hpp"
#include "shuffle/fabric/policy.hpp"

namespace shuffle::fabric {

enum class ParticipantKind : std::uint8_t { Producer = 1, Consumer = 2 };
[[nodiscard]] const char* to_string(ParticipantKind kind) noexcept;

// Lifecycle of a participant incarnation. A participant that is not Active
// holds no authority: dispatch to it and completion from it are refused.
enum class ParticipantState : std::uint8_t {
  Pending = 0,
  Active = 1,
  Suspect = 2,
  Failed = 3,
  Withdrawn = 4,
};
[[nodiscard]] const char* to_string(ParticipantState state) noexcept;
[[nodiscard]] bool holds_authority(ParticipantState state) noexcept;

enum class SelectionKind : std::uint8_t { All = 0, Range = 1, List = 2 };
[[nodiscard]] const char* to_string(SelectionKind kind) noexcept;

// Which partitions a consumer requires. Canonical form: Range is half-open and
// List is sorted and unique.
struct PartitionSelection {
  SelectionKind kind{SelectionKind::All};
  PartitionId begin{};
  PartitionId end{};
  std::vector<PartitionId> list{};

  [[nodiscard]] static PartitionSelection all() { return PartitionSelection{}; }
  [[nodiscard]] static PartitionSelection range(PartitionId first, PartitionId limit);
  [[nodiscard]] static Result<PartitionSelection> from_list(std::vector<PartitionId> partitions, const Limits& limits);

  [[nodiscard]] bool covers(PartitionId partition) const noexcept;
  // Number of partitions selected out of partition_count, in closed form.
  [[nodiscard]] Result<std::uint64_t> selected_count(std::uint32_t partition_count) const;
  [[nodiscard]] std::string canonical_key() const;
};

struct ProducerRecord {
  ProducerId id{};
  IncarnationId incarnation{};
  ParticipantState state{ParticipantState::Pending};
  std::string endpoint{};
};

struct ConsumerRecord {
  ConsumerId id{};
  IncarnationId incarnation{};
  ParticipantState state{ParticipantState::Pending};
  std::string endpoint{};
  std::uint32_t pattern{0};  // index into the interned pattern table
};

class Topology {
 public:
  Topology(ShuffleId shuffle, ShuffleGeneration shuffle_generation, std::uint32_t partition_count, const Limits& limits);

  [[nodiscard]] ShuffleId shuffle() const noexcept { return shuffle_; }
  [[nodiscard]] ShuffleGeneration shuffle_generation() const noexcept { return shuffle_generation_; }
  [[nodiscard]] std::uint32_t partition_count() const noexcept { return partition_count_; }
  [[nodiscard]] const Limits& limits() const noexcept { return limits_; }

  // Every mutation that changes membership, interest or lifecycle advances the
  // topology generation. Plans and attempts bound to an older generation are
  // refused rather than reinterpreted.
  [[nodiscard]] TopologyGeneration generation() const noexcept { return generation_; }

  // Registration is idempotent for an identical (id, incarnation, interest,
  // endpoint) and supersedes a previous incarnation otherwise. The check_*
  // helpers apply exactly the same rules without mutating, so a caller can
  // validate before making a change durable and know the application cannot
  // fail afterwards.
  [[nodiscard]] Status check_producer_registration(ProducerId id, IncarnationId incarnation,
                                                   std::string_view endpoint) const;
  [[nodiscard]] Status check_consumer_registration(ConsumerId id, IncarnationId incarnation, std::string_view endpoint,
                                                   const PartitionSelection& selection) const;
  [[nodiscard]] Status check_participant_state(ParticipantKind kind, std::uint64_t id, IncarnationId incarnation,
                                               ParticipantState state) const;

  [[nodiscard]] Status register_producer(ProducerId id, IncarnationId incarnation, std::string endpoint);
  [[nodiscard]] Status register_consumer(ConsumerId id, IncarnationId incarnation, std::string endpoint,
                                         const PartitionSelection& selection);
  [[nodiscard]] Status set_producer_state(ProducerId id, IncarnationId incarnation, ParticipantState state);
  [[nodiscard]] Status set_consumer_state(ConsumerId id, IncarnationId incarnation, ParticipantState state);
  // Removes the participant while retaining its identifier as withdrawn, so a
  // later re-registration is a fresh incarnation rather than a resurrection.
  [[nodiscard]] Status withdraw_producer(ProducerId id, IncarnationId incarnation);
  [[nodiscard]] Status withdraw_consumer(ConsumerId id, IncarnationId incarnation);

  [[nodiscard]] std::size_t producer_count() const noexcept { return producers_.size(); }
  [[nodiscard]] std::size_t consumer_count() const noexcept { return consumers_.size(); }
  [[nodiscard]] std::uint32_t active_producer_count() const noexcept;
  [[nodiscard]] std::uint32_t active_consumer_count() const noexcept;
  [[nodiscard]] std::uint32_t pattern_count() const noexcept { return static_cast<std::uint32_t>(patterns_.size()); }

  [[nodiscard]] const std::vector<ProducerRecord>& producers() const noexcept { return producers_; }
  [[nodiscard]] const std::vector<ConsumerRecord>& consumers() const noexcept { return consumers_; }
  [[nodiscard]] const PartitionSelection& pattern(std::uint32_t index) const;
  [[nodiscard]] const std::vector<ConsumerId>& consumers_of_pattern(std::uint32_t index) const;
  // Pattern indices sorted by canonical selection key: the canonical order in
  // which edges are enumerated, independent of registration order.
  [[nodiscard]] const std::vector<std::uint32_t>& pattern_order() const noexcept { return pattern_order_; }

  // Returned pointers stay valid until the topology is mutated. A successful
  // Result always carries a non-null pointer.
  [[nodiscard]] Result<const ProducerRecord*> producer(ProducerId id) const;
  [[nodiscard]] Result<const ConsumerRecord*> consumer(ConsumerId id) const;

  // Deterministic ownership: the p-th active producer in identifier order.
  [[nodiscard]] Result<ProducerId> owner_of(PartitionId partition) const;
  [[nodiscard]] Result<std::uint32_t> owner_index_of(PartitionId partition) const;

  // Consumers requiring the partition, in canonical order: patterns sorted by
  // canonical key, consumers ascending inside each pattern. Only consumers
  // holding authority are reported.
  [[nodiscard]] std::vector<ConsumerId> covering_consumers(PartitionId partition) const;
  [[nodiscard]] std::size_t covering_consumer_count(PartitionId partition) const;

  // Distinct producers that serve a consumer, in closed form.
  [[nodiscard]] Result<std::uint32_t> fan_in(ConsumerId id) const;
  [[nodiscard]] Result<std::uint32_t> fan_out(PartitionId partition) const;

  // Total required producer-consumer edges, computed in closed form.
  [[nodiscard]] Result<std::uint64_t> planned_edge_count() const;

  [[nodiscard]] Status validate() const;

 private:
  struct PatternEntry {
    PartitionSelection selection{};
    std::string key{};
    std::vector<ConsumerId> consumers{};  // ascending; only consumers holding authority
  };

  [[nodiscard]] Result<std::uint32_t> intern_pattern(const PartitionSelection& selection);
  void add_consumer_to_pattern(std::uint32_t pattern, ConsumerId id);
  void remove_consumer_from_pattern(std::uint32_t pattern, ConsumerId id);
  void rebuild_pattern_order();
  void rebuild_active_producers();
  void bump_generation();

  ShuffleId shuffle_{};
  ShuffleGeneration shuffle_generation_{};
  std::uint32_t partition_count_{0};
  Limits limits_{};
  TopologyGeneration generation_{};
  std::vector<ProducerRecord> producers_{};  // ascending by id
  std::vector<ConsumerRecord> consumers_{};  // ascending by id
  std::vector<ProducerId> active_producers_{};  // ascending, rebuilt on mutation
  std::vector<PatternEntry> patterns_{};        // stable storage, appended
  std::vector<std::uint32_t> pattern_order_{};  // pattern indices sorted by canonical key
  std::unordered_map<std::string, std::uint32_t> pattern_index_{};
  std::uint64_t interned_selection_items_{0};
};

[[nodiscard]] std::string describe_topology(const Topology& topology);

}  // namespace shuffle::fabric
