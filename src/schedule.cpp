// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/schedule.hpp"

#include <algorithm>

namespace shuffle::fabric {
namespace {

// Pressure decisions are tri-state and deterministic. Absent evidence is not
// positive evidence: it never counts as "no congestion".
enum class PressureDecision : std::uint8_t { Allow = 0, Paused = 1, Unknown = 2 };

}  // namespace

WaveScheduler::WaveScheduler(const Limits& limits) : limits_(limits) {}

Status WaveScheduler::configure(const PolicyEnvelope& policy, const Topology& topology) {
  const Status structural = validate_policy(policy);
  if (!structural.ok()) {
    return structural;
  }
  if (policy.shuffle != topology.shuffle() || policy.shuffle_generation != topology.shuffle_generation()) {
    return Status{make_error(ErrorCode::PolicyMismatch, "policy envelope does not belong to this shuffle generation")};
  }

  policy_ = policy;
  limits_ = policy.limits;
  shuffle_ = topology.shuffle();
  shuffle_generation_ = topology.shuffle_generation();
  topology_generation_ = topology.generation();
  policy_generation_ = policy.generation;
  partition_count_ = topology.partition_count();

  // The cursor restarts so that every required edge is re-enumerated under the
  // new topology or policy. In-flight attempts are deliberately preserved:
  // a topology change must not strand work that already holds authority.
  partition_cursor_ = 0;
  pattern_cursor_ = 0;
  consumer_cursor_ = 0;
  configured_ = true;
  return Status{};
}

Result<WavePlan> WaveScheduler::next_wave(const SchedulingEnvironment& environment) {
  if (!configured_) {
    return make_failure<WavePlan>(ErrorCode::InvalidState, "scheduler has no shuffle configured");
  }
  if (environment.topology == nullptr || environment.completion == nullptr || environment.partitions == nullptr) {
    return make_failure<WavePlan>(ErrorCode::InvalidArgument, "scheduling environment is incomplete");
  }
  const Topology& topology = *environment.topology;
  if (topology.shuffle() != shuffle_ || topology.shuffle_generation() != shuffle_generation_) {
    return make_failure<WavePlan>(ErrorCode::PolicyMismatch, "environment belongs to another shuffle generation");
  }
  if (topology.generation() != topology_generation_) {
    return make_failure<WavePlan>(ErrorCode::StaleTopology, "topology advanced since this plan was configured");
  }

  WavePlan plan;
  plan.id = next_wave_id_;

  const std::vector<std::uint32_t>& order = topology.pattern_order();
  if (partition_count_ == 0 || order.empty()) {
    plan.all_resolved = true;
    return plan;
  }

  const std::uint32_t budget = policy_.waves.max_edges_examined_per_wave;
  const std::uint32_t grant_target = policy_.waves.max_grants_per_wave;
  std::uint32_t examined = 0;
  bool wrapped = false;
  bool stop = false;
  std::uint32_t grants_at_last_wrap = 0;

  // A full pass over the edge space that adds no grant proves the wave cannot
  // grow under the current limits, pressure and completions. Stopping there
  // keeps a limited wave from burning the whole examination budget on edges it
  // already decided not to grant.
  const auto note_wrap = [&]() {
    if (plan.grants.size() == grants_at_last_wrap) {
      stop = true;
    }
    grants_at_last_wrap = static_cast<std::uint32_t>(plan.grants.size());
    wrapped = true;
  };

  const auto decide = [this](std::uint64_t id, const PressureReading& reading,
                             std::unordered_map<std::uint64_t, bool>& paused_map) {
    bool& paused = paused_map[id];
    if (!reading.observed || !reading.fresh) {
      if (policy_.congestion.require_fresh_evidence) {
        paused = true;
        return PressureDecision::Unknown;
      }
      // Without a freshness requirement, absent evidence leaves the previous
      // decision in force and never counts as a positive "no congestion".
      return paused ? PressureDecision::Paused : PressureDecision::Allow;
    }
    if (reading.level >= policy_.congestion.pause_threshold) {
      paused = true;
      return PressureDecision::Paused;
    }
    if (reading.level <= policy_.congestion.resume_threshold) {
      paused = false;
      return PressureDecision::Allow;
    }
    return paused ? PressureDecision::Paused : PressureDecision::Allow;
  };

  while (examined < budget && plan.grants.size() < grant_target && !stop) {
    ++examined;  // one cursor step; the budget bounds every step, not just the productive ones

    if (partition_cursor_ >= partition_count_) {
      partition_cursor_ = 0;
      note_wrap();
    }
    const PartitionId partition{partition_cursor_};

    if (pattern_cursor_ >= order.size()) {
      pattern_cursor_ = 0;
      consumer_cursor_ = 0;
      ++partition_cursor_;
      if (partition_cursor_ >= partition_count_) {
        partition_cursor_ = 0;
        note_wrap();
      }
      continue;
    }

    const std::uint32_t pattern_index = order[pattern_cursor_];
    // A pattern only contributes edges for partitions it actually selects.
    // Skipping this check would dispatch transfers for partitions a consumer
    // never asked for.
    if (!topology.pattern(pattern_index).covers(partition)) {
      consumer_cursor_ = 0;
      ++pattern_cursor_;
      continue;
    }
    const std::vector<ConsumerId>& consumers = topology.consumers_of_pattern(pattern_index);
    if (consumer_cursor_ >= consumers.size()) {
      consumer_cursor_ = 0;
      ++pattern_cursor_;
      continue;
    }
    const ConsumerId consumer = consumers[consumer_cursor_];
    ++consumer_cursor_;

    const PartitionFacts facts = environment.partitions->partition_facts(partition);
    if (!facts.produced) {
      ++plan.skipped_unproduced;
      continue;
    }
    const auto owner = topology.owner_of(partition);
    if (!owner.ok() || owner.value() != facts.producer) {
      // Content produced by an incarnation that no longer owns the partition
      // is not dispatchable: the current owner must produce it again.
      ++plan.skipped_unproduced;
      continue;
    }
    const auto producer_record = topology.producer(owner.value());
    if (!producer_record.ok() || !holds_authority(producer_record.value()->state) ||
        producer_record.value()->incarnation != facts.producer_incarnation) {
      // The producing incarnation was superseded. Dispatching now would only
      // produce an attempt that cannot commit, so the partition is reported as
      // needing (re)production instead.
      ++plan.skipped_unproduced;
      continue;
    }

    const EdgeKey key{partition, facts.partition_generation, consumer};
    const EdgeStatus status = environment.completion->edge_status(key);
    if (status.phase == EdgePhase::Completed) {
      ++plan.skipped_completed;
      continue;
    }
    if (status.phase == EdgePhase::Failed || status.phase == EdgePhase::Purged) {
      ++plan.skipped_failed;
      continue;
    }
    if (status.attempts >= policy_.retry.max_attempts) {
      // The retry budget is spent; the coordinator records this as a permanent
      // failure rather than dispatching a further attempt.
      ++plan.skipped_failed;
      continue;
    }
    if (edge_attempts_.find(key) != edge_attempts_.end()) {
      ++plan.skipped_in_flight;
      continue;
    }
    if (status.ready_at.value() > environment.now.value()) {
      ++plan.deferred_retry_wait;
      continue;
    }

    if (global_in_flight_ >= policy_.concurrency.global) {
      ++plan.deferred_limits;
      stop = true;  // the wave is full; nothing else can be granted this call
      continue;
    }
    auto& source_count = producer_in_flight_[owner.value().value()];
    auto& destination_count = consumer_in_flight_[consumer.value()];
    if (source_count >= policy_.concurrency.per_source ||
        destination_count >= policy_.concurrency.per_destination) {
      ++plan.deferred_limits;
      continue;
    }

    if (environment.congestion != nullptr) {
      const PressureReading source_pressure = environment.congestion->producer_pressure(owner.value());
      switch (decide(owner.value().value(), source_pressure, producer_paused_)) {
        case PressureDecision::Paused:
          ++plan.deferred_source_pressure;
          continue;
        case PressureDecision::Unknown:
          ++plan.deferred_unknown_pressure;
          continue;
        case PressureDecision::Allow:
          break;
      }
      const PressureReading destination_pressure = environment.congestion->consumer_pressure(consumer);
      switch (decide(consumer.value(), destination_pressure, consumer_paused_)) {
        case PressureDecision::Paused:
          ++plan.deferred_destination_pressure;
          continue;
        case PressureDecision::Unknown:
          ++plan.deferred_unknown_pressure;
          continue;
        case PressureDecision::Allow:
          break;
      }
    }

    const auto consumer_record = topology.consumer(consumer);
    if (!consumer_record.ok() || !holds_authority(consumer_record.value()->state)) {
      ++plan.skipped_unproduced;
      continue;
    }

    DispatchGrant grant;
    grant.attempt = next_attempt_id_;
    next_attempt_id_ = next_attempt_id_.next();
    grant.wave = plan.id;
    grant.shuffle = shuffle_;
    grant.shuffle_generation = shuffle_generation_;
    grant.partition = partition;
    grant.partition_generation = facts.partition_generation;
    grant.producer = owner.value();
    grant.producer_incarnation = facts.producer_incarnation;
    grant.producer_endpoint = producer_record.value()->endpoint;
    grant.consumer = consumer;
    grant.consumer_incarnation = consumer_record.value()->incarnation;
    grant.topology_generation = topology_generation_;
    grant.policy_generation = policy_generation_;
    grant.manifest_digest = facts.manifest_digest;
    grant.total_bytes = facts.total_bytes;
    grant.attempt_ordinal = status.attempts + 1;
    grant.issued_at = environment.now;

    attempts_.emplace(grant.attempt.value(),
                      AttemptRecord{key, plan.id, grant.attempt_ordinal, owner.value(), consumer});
    edge_attempts_.emplace(key, grant.attempt);
    ++source_count;
    ++destination_count;
    ++global_in_flight_;
    ++stats_.grants_issued;
    plan.grants.push_back(grant);
  }

  plan.examined = examined;
  plan.cursor_wrapped = wrapped;
  plan.all_resolved = plan.grants.empty() && wrapped && plan.deferred_limits == 0 &&
                      plan.deferred_source_pressure == 0 && plan.deferred_destination_pressure == 0 &&
                      plan.deferred_unknown_pressure == 0 && plan.deferred_retry_wait == 0 &&
                      plan.skipped_in_flight == 0 && plan.skipped_unproduced == 0;

  if (!plan.grants.empty()) {
    next_wave_id_ = next_wave_id_.next();
  }
  ++stats_.waves_planned;
  stats_.edges_examined += examined;
  stats_.edges_skipped_resolved += plan.skipped_completed + plan.skipped_failed;
  return plan;
}

Status WaveScheduler::resolve(TransferAttemptId attempt, EdgeOutcome outcome, ErrorCode) {
  const auto found = attempts_.find(attempt.value());
  if (found == attempts_.end()) {
    return Status{make_error(ErrorCode::StaleAttempt,
                             "attempt " + attempt.to_string() + " is not in flight and carries no authority")};
  }
  const AttemptRecord record = found->second;
  attempts_.erase(found);
  edge_attempts_.erase(record.key);

  const auto source = producer_in_flight_.find(record.producer.value());
  if (source != producer_in_flight_.end()) {
    if (source->second > 0) {
      --source->second;
    }
    if (source->second == 0) {
      producer_in_flight_.erase(source);
    }
  }
  const auto destination = consumer_in_flight_.find(record.consumer.value());
  if (destination != consumer_in_flight_.end()) {
    if (destination->second > 0) {
      --destination->second;
    }
    if (destination->second == 0) {
      consumer_in_flight_.erase(destination);
    }
  }
  if (global_in_flight_ > 0) {
    --global_in_flight_;
  }
  ++stats_.attempts_resolved;
  if (outcome == EdgeOutcome::Cancelled) {
    ++stats_.grants_abandoned;
  }
  return Status{};
}

Status WaveScheduler::abandon_all(ErrorCode) {
  const std::size_t abandoned = attempts_.size();
  attempts_.clear();
  edge_attempts_.clear();
  producer_in_flight_.clear();
  consumer_in_flight_.clear();
  global_in_flight_ = 0;
  stats_.grants_abandoned += abandoned;
  return Status{};
}

std::uint32_t WaveScheduler::in_flight_for_producer(ProducerId id) const {
  const auto found = producer_in_flight_.find(id.value());
  return found == producer_in_flight_.end() ? 0u : found->second;
}

std::uint32_t WaveScheduler::in_flight_for_consumer(ConsumerId id) const {
  const auto found = consumer_in_flight_.find(id.value());
  return found == consumer_in_flight_.end() ? 0u : found->second;
}

}  // namespace shuffle::fabric
