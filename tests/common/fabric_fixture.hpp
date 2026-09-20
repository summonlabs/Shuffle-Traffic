// Shared deterministic fixture for scheduling, ledger and integration proofs.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// The fixture is deliberately explicit: produced partitions, pressure readings
// and ledger state are set by the test, so every plan is a pure function of
// visible inputs.

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "shuffle/fabric/codec.hpp"
#include "shuffle/fabric/hash.hpp"
#include "shuffle/fabric/ledger.hpp"
#include "shuffle/fabric/manifest.hpp"
#include "shuffle/fabric/schedule.hpp"
#include "shuffle/fabric/topology.hpp"

namespace shuffle::test {

[[nodiscard]] inline std::string render_wave_plan(const shuffle::fabric::WavePlan& plan) {
  std::string text = "wave=" + plan.id.to_string() + " grants=" + std::to_string(plan.grants.size()) + "\n";
  for (const shuffle::fabric::DispatchGrant& grant : plan.grants) {
    text += "  p" + grant.partition.to_string() + "/g" + grant.partition_generation.to_string() + " -> c" +
            grant.consumer.to_string() + " from p" + grant.producer.to_string() + " attempt=" +
            grant.attempt.to_string() + " ordinal=" + std::to_string(grant.attempt_ordinal) + "\n";
  }
  text += "  examined=" + std::to_string(plan.examined) + " completed=" + std::to_string(plan.skipped_completed) +
          " failed=" + std::to_string(plan.skipped_failed) + " inflight=" + std::to_string(plan.skipped_in_flight) +
          " unproduced=" + std::to_string(plan.skipped_unproduced) + " limits=" + std::to_string(plan.deferred_limits) +
          " pressure=" + std::to_string(plan.deferred_source_pressure + plan.deferred_destination_pressure) +
          " unknown=" + std::to_string(plan.deferred_unknown_pressure) +
          " retry_wait=" + std::to_string(plan.deferred_retry_wait) + "\n";
  return text;
}

class FabricFixture {
 public:
  explicit FabricFixture(std::uint32_t partitions = 16, const shuffle::fabric::Limits& limits = {})
      : limits_(limits),
        topology_(shuffle::fabric::ShuffleId{1}, shuffle::fabric::ShuffleGeneration{1}, partitions, limits_),
        ledger_(limits_),
        partition_view_(*this),
        congestion_view_(*this) {
    ledger_.bind(shuffle::fabric::ShuffleId{1}, shuffle::fabric::ShuffleGeneration{1}, partitions);
    policy_.shuffle = shuffle::fabric::ShuffleId{1};
    policy_.shuffle_generation = shuffle::fabric::ShuffleGeneration{1};
    policy_.generation = shuffle::fabric::PolicyGeneration{1};
    policy_.limits = limits_;
  }

  [[nodiscard]] shuffle::fabric::Topology& topology() noexcept { return topology_; }
  [[nodiscard]] const shuffle::fabric::Topology& topology() const noexcept { return topology_; }
  [[nodiscard]] shuffle::fabric::CompletionLedger& ledger() noexcept { return ledger_; }
  [[nodiscard]] const shuffle::fabric::CompletionLedger& ledger() const noexcept { return ledger_; }
  [[nodiscard]] shuffle::fabric::PolicyEnvelope& policy() noexcept { return policy_; }
  [[nodiscard]] const shuffle::fabric::PolicyEnvelope& policy() const noexcept { return policy_; }
  [[nodiscard]] const shuffle::fabric::Limits& limits() const noexcept { return limits_; }
  [[nodiscard]] std::uint32_t partitions() const noexcept { return topology_.partition_count(); }

  void produce(shuffle::fabric::PartitionId partition, shuffle::fabric::PartitionGeneration generation,
               shuffle::fabric::ProducerId producer, shuffle::fabric::IncarnationId incarnation,
               std::uint64_t bytes) {
    shuffle::fabric::PartitionFacts facts;
    facts.produced = true;
    facts.partition_generation = generation;
    facts.producer = producer;
    facts.producer_incarnation = incarnation;
    facts.total_bytes = bytes;
    shuffle::fabric::ByteWriter writer;
    writer.put_u64(partition.value());
    writer.put_u64(generation.value());
    writer.put_u64(bytes);
    facts.manifest_digest = shuffle::fabric::sha256(writer.data());
    facts_[partition.value()] = facts;
  }

  void withdraw_partition(shuffle::fabric::PartitionId partition) { facts_.erase(partition.value()); }

  void set_producer_pressure(shuffle::fabric::ProducerId producer, std::uint32_t level, bool observed, bool fresh) {
    shuffle::fabric::PressureReading reading;
    reading.level = level;
    reading.observed = observed;
    reading.fresh = fresh;
    reading.observed_at = shuffle::fabric::TickId{1};
    reading.reporter = shuffle::fabric::IncarnationId{1};
    reading.policy_generation = policy_.generation;
    producer_pressure_[producer.value()] = reading;
  }

  void set_consumer_pressure(shuffle::fabric::ConsumerId consumer, std::uint32_t level, bool observed, bool fresh) {
    shuffle::fabric::PressureReading reading;
    reading.level = level;
    reading.observed = observed;
    reading.fresh = fresh;
    reading.observed_at = shuffle::fabric::TickId{1};
    reading.reporter = shuffle::fabric::IncarnationId{1};
    reading.policy_generation = policy_.generation;
    consumer_pressure_[consumer.value()] = reading;
  }

  [[nodiscard]] shuffle::fabric::SchedulingEnvironment environment(shuffle::fabric::TickId now) {
    shuffle::fabric::SchedulingEnvironment environment;
    environment.topology = &topology_;
    environment.completion = &ledger_;
    environment.congestion = &congestion_view_;
    environment.partitions = &partition_view_;
    environment.now = now;
    return environment;
  }

  // Commits a partition generation for one consumer through the real ledger.
  [[nodiscard]] shuffle::fabric::Result<shuffle::fabric::CommitReceipt> commit(
      shuffle::fabric::PartitionId partition, shuffle::fabric::PartitionGeneration generation,
      shuffle::fabric::ConsumerId consumer, shuffle::fabric::TickId now) {
    const auto found = facts_.find(partition.value());
    if (found == facts_.end()) {
      return shuffle::fabric::make_failure<shuffle::fabric::CommitReceipt>(shuffle::fabric::ErrorCode::NotCommitted,
                                                                          "partition is not produced");
    }
    shuffle::fabric::CommitRequest request;
    request.edge = shuffle::fabric::EdgeKey{partition, generation, consumer};
    request.shuffle_generation = shuffle::fabric::ShuffleGeneration{1};
    request.topology_generation = topology_.generation();
    request.policy_generation = policy_.generation;
    request.producer = found->second.producer;
    request.producer_incarnation = found->second.producer_incarnation;
    request.consumer = consumer;
    request.consumer_incarnation = shuffle::fabric::IncarnationId{1};
    request.manifest_digest = found->second.manifest_digest;
    request.observed_partition_digest = found->second.manifest_digest;
    request.bytes = found->second.total_bytes;
    request.integrity_verified = true;
    return ledger_.commit(request, now);
  }

 private:
  class PartitionViewImpl final : public shuffle::fabric::PartitionView {
   public:
    explicit PartitionViewImpl(const FabricFixture& owner) : owner_(&owner) {}
    [[nodiscard]] shuffle::fabric::PartitionFacts partition_facts(shuffle::fabric::PartitionId id) const override {
      const auto found = owner_->facts_.find(id.value());
      if (found == owner_->facts_.end()) {
        return shuffle::fabric::PartitionFacts{};
      }
      return found->second;
    }

   private:
    const FabricFixture* owner_;
  };

  class CongestionViewImpl final : public shuffle::fabric::CongestionView {
   public:
    explicit CongestionViewImpl(const FabricFixture& owner) : owner_(&owner) {}
    [[nodiscard]] shuffle::fabric::PressureReading producer_pressure(shuffle::fabric::ProducerId id) const override {
      const auto found = owner_->producer_pressure_.find(id.value());
      return found == owner_->producer_pressure_.end() ? shuffle::fabric::PressureReading::unknown() : found->second;
    }
    [[nodiscard]] shuffle::fabric::PressureReading consumer_pressure(shuffle::fabric::ConsumerId id) const override {
      const auto found = owner_->consumer_pressure_.find(id.value());
      return found == owner_->consumer_pressure_.end() ? shuffle::fabric::PressureReading::unknown() : found->second;
    }

   private:
    const FabricFixture* owner_;
  };

  shuffle::fabric::Limits limits_{};
  shuffle::fabric::Topology topology_;
  shuffle::fabric::CompletionLedger ledger_;
  shuffle::fabric::PolicyEnvelope policy_{};
  std::unordered_map<std::uint64_t, shuffle::fabric::PartitionFacts> facts_{};
  std::unordered_map<std::uint64_t, shuffle::fabric::PressureReading> producer_pressure_{};
  std::unordered_map<std::uint64_t, shuffle::fabric::PressureReading> consumer_pressure_{};
  PartitionViewImpl partition_view_;
  CongestionViewImpl congestion_view_;
};

// Produces every partition of the fixture with round-robin ownership taken
// from the topology, so facts and owners agree unless a test says otherwise.
inline void produce_all(FabricFixture& fixture, std::uint64_t bytes = 256,
                        shuffle::fabric::PartitionGeneration generation = shuffle::fabric::PartitionGeneration{1}) {
  for (std::uint32_t index = 0; index < fixture.partitions(); ++index) {
    const auto owner = fixture.topology().owner_of(shuffle::fabric::PartitionId{index});
    if (!owner.ok()) {
      continue;
    }
    auto incarnation = shuffle::fabric::IncarnationId{1};
    const auto record = fixture.topology().producer(owner.value());
    if (record.ok()) {
      incarnation = record.value()->incarnation;
    }
    fixture.produce(shuffle::fabric::PartitionId{index}, generation, owner.value(), incarnation, bytes);
  }
}

}  // namespace shuffle::test
