// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/policy.hpp"

#include <algorithm>
#include <string>

#include "shuffle/fabric/codec.hpp"
#include "shuffle/fabric/topology.hpp"

namespace shuffle::fabric {
namespace {

// A bounded sample of violations is reported per category: the first
// offending subjects in canonical order, so the explanation is deterministic
// and the evaluation cannot be turned into an unbounded scan.
constexpr std::size_t kMaxViolationsPerCategory = 8;

void encode_limits(ByteWriter& writer, const Limits& limits) {
  writer.put_u64(limits.max_frame_payload_bytes);
  writer.put_u64(limits.max_message_payload_bytes);
  writer.put_u64(limits.max_allocation_bytes);
  writer.put_u64(limits.max_string_bytes);
  writer.put_u64(limits.max_endpoint_bytes);
  writer.put_u64(limits.max_collection_items);
  writer.put_u64(limits.max_chunks_per_partition);
  writer.put_u64(limits.max_partitions);
  writer.put_u64(limits.max_participants);
  writer.put_u64(limits.max_selection_patterns);
  writer.put_u64(limits.max_selection_items);
  writer.put_u64(limits.max_planned_edges);
  writer.put_u64(limits.max_tracked_edges);
  writer.put_u64(limits.max_tracked_partitions);
  writer.put_u64(limits.max_state_bytes);
  writer.put_u64(limits.max_journal_record_bytes);
  writer.put_u64(limits.max_retained_generations);
  writer.put_u64(limits.max_attempt_records_per_shuffle);
  writer.put_u64(limits.max_pending_retries);
  writer.put_u64(limits.max_wave_grants);
}

Limits decode_limits(ByteReader& reader) {
  Limits limits;
  limits.max_frame_payload_bytes = reader.u64();
  limits.max_message_payload_bytes = reader.u64();
  limits.max_allocation_bytes = reader.u64();
  limits.max_string_bytes = static_cast<std::uint32_t>(reader.u64());
  limits.max_endpoint_bytes = static_cast<std::uint32_t>(reader.u64());
  limits.max_collection_items = static_cast<std::uint32_t>(reader.u64());
  limits.max_chunks_per_partition = reader.u64();
  limits.max_partitions = static_cast<std::uint32_t>(reader.u64());
  limits.max_participants = static_cast<std::uint32_t>(reader.u64());
  limits.max_selection_patterns = static_cast<std::uint32_t>(reader.u64());
  limits.max_selection_items = reader.u64();
  limits.max_planned_edges = reader.u64();
  limits.max_tracked_edges = reader.u64();
  limits.max_tracked_partitions = reader.u64();
  limits.max_state_bytes = reader.u64();
  limits.max_journal_record_bytes = reader.u64();
  limits.max_retained_generations = static_cast<std::uint32_t>(reader.u64());
  limits.max_attempt_records_per_shuffle = static_cast<std::uint32_t>(reader.u64());
  limits.max_pending_retries = static_cast<std::uint32_t>(reader.u64());
  limits.max_wave_grants = static_cast<std::uint32_t>(reader.u64());
  return limits;
}

}  // namespace

Status validate_policy(const PolicyEnvelope& policy) {
  if (policy.shuffle.is_zero()) {
    return Status{make_error(ErrorCode::InvalidArgument, "policy envelope has no shuffle identity")};
  }
  if (policy.fan.max_fan_out == 0 || policy.fan.max_fan_in == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "fan bounds must be positive")};
  }
  if (policy.concurrency.global == 0 || policy.concurrency.per_source == 0 || policy.concurrency.per_destination == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "concurrency limits must be positive")};
  }
  if (policy.concurrency.per_source > policy.concurrency.global ||
      policy.concurrency.per_destination > policy.concurrency.global) {
    return Status{make_error(ErrorCode::InvalidArgument, "per-source/per-destination limits exceed the global limit")};
  }
  if (policy.waves.max_grants_per_wave == 0 ||
      policy.waves.max_grants_per_wave > policy.limits.max_wave_grants) {
    return Status{make_error(ErrorCode::InvalidArgument, "max_grants_per_wave is zero or exceeds max_wave_grants")};
  }
  if (policy.waves.max_edges_examined_per_wave < policy.waves.max_grants_per_wave) {
    return Status{make_error(ErrorCode::InvalidArgument, "max_edges_examined_per_wave is below max_grants_per_wave")};
  }
  if (policy.retry.max_attempts == 0 || policy.retry.max_attempts > 64) {
    return Status{make_error(ErrorCode::InvalidArgument, "retry.max_attempts must be in [1, 64]")};
  }
  if (policy.congestion.pause_threshold > 100) {
    return Status{make_error(ErrorCode::InvalidArgument, "pause_threshold exceeds 100")};
  }
  if (policy.congestion.resume_threshold > policy.congestion.pause_threshold) {
    return Status{make_error(ErrorCode::InvalidArgument, "resume_threshold exceeds pause_threshold")};
  }
  if (policy.congestion.require_fresh_evidence && policy.congestion.evidence_validity_ticks == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "fresh evidence required but validity window is zero")};
  }
  return policy.limits.validate();
}

Result<PolicyEvaluation> evaluate_policy(const Topology& topology, const PolicyEnvelope& policy) {
  const Status structural = validate_policy(policy);
  if (!structural.ok()) {
    return Result<PolicyEvaluation>{structural.error()};
  }
  if (policy.shuffle != topology.shuffle() || policy.shuffle_generation != topology.shuffle_generation()) {
    return make_failure<PolicyEvaluation>(ErrorCode::PolicyMismatch, "policy envelope belongs to another shuffle generation");
  }

  PolicyEvaluation evaluation;

  const auto planned = topology.planned_edge_count();
  if (!planned.ok()) {
    return Result<PolicyEvaluation>{planned.error()};
  }
  evaluation.planned_edges = planned.value();
  if (evaluation.planned_edges > policy.limits.max_planned_edges) {
    evaluation.violations.push_back(PolicyViolation{
        ErrorCode::LimitExceeded, "shuffle",
        "planned edge count " + std::to_string(evaluation.planned_edges) + " exceeds max_planned_edges"});
  }

  // Fan-out: the number of consumers one partition is delivered to. Evaluated
  // from the pattern table, never by materialising the consumer list.
  std::size_t fan_out_violations = 0;
  for (std::uint32_t index = 0; index < topology.partition_count(); ++index) {
    const auto fan_out = topology.fan_out(PartitionId{index});
    if (!fan_out.ok()) {
      return Result<PolicyEvaluation>{fan_out.error()};
    }
    evaluation.max_fan_out = std::max(evaluation.max_fan_out, fan_out.value());
    if (fan_out.value() > policy.fan.max_fan_out) {
      if (fan_out_violations < kMaxViolationsPerCategory) {
        evaluation.violations.push_back(PolicyViolation{
            ErrorCode::FanOutCeilingExceeded, "partition " + std::to_string(index),
            "fan-out " + std::to_string(fan_out.value()) + " exceeds max_fan_out " +
                std::to_string(policy.fan.max_fan_out)});
      }
      ++fan_out_violations;
    }
  }

  std::size_t fan_in_violations = 0;
  for (const ConsumerRecord& record : topology.consumers()) {
    const auto fan_in = topology.fan_in(record.id);
    if (!fan_in.ok()) {
      return Result<PolicyEvaluation>{fan_in.error()};
    }
    evaluation.max_fan_in = std::max(evaluation.max_fan_in, fan_in.value());
    if (fan_in.value() > policy.fan.max_fan_in) {
      if (fan_in_violations < kMaxViolationsPerCategory) {
        evaluation.violations.push_back(PolicyViolation{
            ErrorCode::FanInCeilingExceeded, "consumer " + record.id.to_string(),
            "fan-in " + std::to_string(fan_in.value()) + " exceeds max_fan_in " +
                std::to_string(policy.fan.max_fan_in)});
      }
      ++fan_in_violations;
    }
  }

  // Deterministic order regardless of discovery order.
  std::sort(evaluation.violations.begin(), evaluation.violations.end(),
            [](const PolicyViolation& lhs, const PolicyViolation& rhs) {
              if (lhs.code != rhs.code) {
                return static_cast<std::uint16_t>(lhs.code) < static_cast<std::uint16_t>(rhs.code);
              }
              return lhs.subject < rhs.subject;
            });
  return evaluation;
}

Status encode_policy(const PolicyEnvelope& policy, ByteWriter& writer) {
  writer.put_u64(policy.shuffle.value());
  writer.put_u64(policy.shuffle_generation.value());
  writer.put_u64(policy.generation.value());
  writer.put_u32(policy.fan.max_fan_out);
  writer.put_u32(policy.fan.max_fan_in);
  writer.put_u32(policy.concurrency.global);
  writer.put_u32(policy.concurrency.per_source);
  writer.put_u32(policy.concurrency.per_destination);
  writer.put_u32(policy.waves.max_grants_per_wave);
  writer.put_u32(policy.waves.max_edges_examined_per_wave);
  writer.put_u32(policy.retry.max_attempts);
  writer.put_u32(policy.retry.min_ticks_between_attempts);
  writer.put_u32(policy.congestion.pause_threshold);
  writer.put_u32(policy.congestion.resume_threshold);
  writer.put_bool(policy.congestion.require_fresh_evidence);
  writer.put_u32(policy.congestion.evidence_validity_ticks);
  encode_limits(writer, policy.limits);
  return Status{};
}

Result<PolicyEnvelope> decode_policy(ByteReader& reader) {
  PolicyEnvelope policy;
  policy.shuffle = ShuffleId{reader.u64()};
  policy.shuffle_generation = ShuffleGeneration{reader.u64()};
  policy.generation = PolicyGeneration{reader.u64()};
  policy.fan.max_fan_out = reader.u32();
  policy.fan.max_fan_in = reader.u32();
  policy.concurrency.global = reader.u32();
  policy.concurrency.per_source = reader.u32();
  policy.concurrency.per_destination = reader.u32();
  policy.waves.max_grants_per_wave = reader.u32();
  policy.waves.max_edges_examined_per_wave = reader.u32();
  policy.retry.max_attempts = reader.u32();
  policy.retry.min_ticks_between_attempts = reader.u32();
  policy.congestion.pause_threshold = reader.u32();
  policy.congestion.resume_threshold = reader.u32();
  policy.congestion.require_fresh_evidence = reader.boolean();
  policy.congestion.evidence_validity_ticks = reader.u32();
  policy.limits = decode_limits(reader);
  if (!reader.ok()) {
    return Result<PolicyEnvelope>{reader.error()};
  }
  const Status valid = validate_policy(policy);
  if (!valid.ok()) {
    return Result<PolicyEnvelope>{valid.error()};
  }
  return policy;
}

std::string describe_policy(const PolicyEnvelope& policy) {
  std::string text;
  text += "shuffle=" + policy.shuffle.to_string() + " generation=" + policy.shuffle_generation.to_string() +
          " policy_generation=" + policy.generation.to_string() + "\n";
  text += "  fan_out<=" + std::to_string(policy.fan.max_fan_out) + " fan_in<=" + std::to_string(policy.fan.max_fan_in) +
          "\n";
  text += "  concurrency global=" + std::to_string(policy.concurrency.global) +
          " per_source=" + std::to_string(policy.concurrency.per_source) +
          " per_destination=" + std::to_string(policy.concurrency.per_destination) + "\n";
  text += "  waves grants<=" + std::to_string(policy.waves.max_grants_per_wave) +
          " examined<=" + std::to_string(policy.waves.max_edges_examined_per_wave) + "\n";
  text += "  retry attempts<=" + std::to_string(policy.retry.max_attempts) +
          " min_ticks=" + std::to_string(policy.retry.min_ticks_between_attempts) + "\n";
  text += "  congestion pause>=" + std::to_string(policy.congestion.pause_threshold) +
          " resume<=" + std::to_string(policy.congestion.resume_threshold) +
          " fresh_required=" + (policy.congestion.require_fresh_evidence ? "yes" : "no") +
          " validity_ticks=" + std::to_string(policy.congestion.evidence_validity_ticks) + "\n";
  return text;
}

}  // namespace shuffle::fabric
