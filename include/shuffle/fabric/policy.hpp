// The policy envelope: the bounded authority under which transfers may move.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// A policy never grants authority by itself. It restricts what authority may
// be exercised: when the envelope is violated the coordinator refuses to open
// or to continue the shuffle instead of silently exceeding the bound.

#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "shuffle/fabric/bytes.hpp"
#include "shuffle/fabric/codec.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/identity.hpp"

namespace shuffle::fabric {

struct FanBounds {
  // Maximum number of consumers one partition may be delivered to.
  std::uint32_t max_fan_out{64};
  // Maximum number of distinct producers serving one consumer.
  std::uint32_t max_fan_in{64};

  friend bool operator==(const FanBounds&, const FanBounds&) noexcept = default;
};

struct ConcurrencyLimits {
  std::uint32_t global{64};
  std::uint32_t per_source{8};
  std::uint32_t per_destination{8};

  friend bool operator==(const ConcurrencyLimits&, const ConcurrencyLimits&) noexcept = default;
};

struct WaveLimits {
  std::uint32_t max_grants_per_wave{256};
  // Upper bound on edges examined per wave request. This is what keeps a fully
  // backpressured or fully completed fabric from turning one call into a full
  // N x M scan: each call does bounded work and the cursor resumes.
  std::uint32_t max_edges_examined_per_wave{8192};

  friend bool operator==(const WaveLimits&, const WaveLimits&) noexcept = default;
};

struct RetryPolicy {
  std::uint32_t max_attempts{3};
  // Logical ticks that must pass before a retriable edge may be dispatched
  // again. Ticks are advanced explicitly, never by wall-clock time.
  std::uint32_t min_ticks_between_attempts{0};

  friend bool operator==(const RetryPolicy&, const RetryPolicy&) noexcept = default;
};

struct CongestionPolicy {
  // Pressure at or above this level pauses new dispatch to the target.
  std::uint32_t pause_threshold{80};
  // Pressure at or below this level resumes dispatch (hysteresis).
  std::uint32_t resume_threshold{40};
  // When true, absent or stale pressure evidence pauses the target instead of
  // being treated as "no congestion".
  bool require_fresh_evidence{false};
  // Ticks after which a pressure reading is stale.
  std::uint32_t evidence_validity_ticks{20};

  friend bool operator==(const CongestionPolicy&, const CongestionPolicy&) noexcept = default;
};

struct PolicyEnvelope {
  ShuffleId shuffle{};
  ShuffleGeneration shuffle_generation{};
  PolicyGeneration generation{};

  FanBounds fan{};
  ConcurrencyLimits concurrency{};
  WaveLimits waves{};
  RetryPolicy retry{};
  CongestionPolicy congestion{};
  Limits limits{};

  friend bool operator==(const PolicyEnvelope&, const PolicyEnvelope&) noexcept = default;
};

// Structural validation of the envelope itself, independent of any topology.
[[nodiscard]] Status validate_policy(const PolicyEnvelope& policy);

struct PolicyViolation {
  ErrorCode code{ErrorCode::Ok};
  std::string subject{};
  std::string detail{};
};

struct PolicyEvaluation {
  std::uint64_t planned_edges{0};
  std::uint32_t max_fan_out{0};
  std::uint32_t max_fan_in{0};
  std::vector<PolicyViolation> violations{};

  [[nodiscard]] bool acceptable() const noexcept { return violations.empty(); }
};

// Evaluates the envelope against a concrete topology. The planned edge count
// is computed in closed form from the interned selection patterns, so a large
// partition matrix is never materialised to answer the question.
[[nodiscard]] Result<PolicyEvaluation> evaluate_policy(const class Topology& topology, const PolicyEnvelope& policy);

// Canonical encoding of the envelope, used by the wire protocol and durable
// state. Decoding is strict: unknown discriminants, trailing bytes and
// inconsistent values are refused.
[[nodiscard]] Status encode_policy(const PolicyEnvelope& policy, ByteWriter& writer);
[[nodiscard]] Result<PolicyEnvelope> decode_policy(ByteReader& reader);

[[nodiscard]] std::string describe_policy(const PolicyEnvelope& policy);

}  // namespace shuffle::fabric
