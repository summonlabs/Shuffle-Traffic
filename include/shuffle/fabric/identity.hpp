// Strongly typed identities and generations.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Identities are distinct types on purpose: a partition identifier is not a
// chunk identifier, and a generation is not a sequence number. Nothing in the
// runtime compares raw integers across kinds, so a stale or mismatched value
// cannot be silently substituted for the one authority requires.

#pragma once

#include <compare>
#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <string_view>

#include "shuffle/fabric/error.hpp"

namespace shuffle::fabric {

struct ShuffleTag;
struct ShuffleGenerationTag;
struct TopologyGenerationTag;
struct PolicyGenerationTag;
struct PartitionTag;
struct PartitionGenerationTag;
struct ChunkTag;
struct ProducerTag;
struct ConsumerTag;
struct WaveTag;
struct TransferAttemptTag;
struct IncarnationTag;
struct SessionTag;
struct CommitSequenceTag;
struct TickTag;

template <class Tag, class Rep = std::uint64_t>
class Id {
 public:
  using rep_type = Rep;
  using tag_type = Tag;

  constexpr Id() noexcept = default;
  constexpr explicit Id(Rep value) noexcept : value_(value) {}

  [[nodiscard]] static constexpr Id from_value(Rep value) noexcept { return Id{value}; }

  [[nodiscard]] constexpr Rep value() const noexcept { return value_; }
  [[nodiscard]] constexpr bool is_zero() const noexcept { return value_ == 0; }
  [[nodiscard]] constexpr Id next() const noexcept { return Id{static_cast<Rep>(value_ + 1)}; }

  friend constexpr bool operator==(const Id&, const Id&) noexcept = default;
  friend constexpr auto operator<=>(const Id&, const Id&) noexcept = default;

  [[nodiscard]] std::string to_string() const { return std::to_string(value_); }

  // Canonical text form: decimal, no leading zeros, no sign, no whitespace.
  [[nodiscard]] static Result<Id> parse(std::string_view text) {
    if (text.empty()) {
      return make_failure<Id>(ErrorCode::EmptyValue, "identity text is empty");
    }
    if (text.size() > 1 && text.front() == '0') {
      return make_failure<Id>(ErrorCode::InvalidArgument, "identity text has a leading zero");
    }
    Rep value = 0;
    for (const char ch : text) {
      if (ch < '0' || ch > '9') {
        return make_failure<Id>(ErrorCode::InvalidArgument, "identity text is not decimal");
      }
      const Rep digit = static_cast<Rep>(ch - '0');
      if (value > (std::numeric_limits<Rep>::max() - digit) / 10) {
        return make_failure<Id>(ErrorCode::IntegerOverflow, "identity text overflows");
      }
      value = static_cast<Rep>(value * 10 + digit);
    }
    return Id{value};
  }

 private:
  Rep value_{0};
};

template <class Tag>
using Generation = Id<Tag>;

using ShuffleId = Id<ShuffleTag>;
using ShuffleGeneration = Generation<ShuffleGenerationTag>;
using TopologyGeneration = Generation<TopologyGenerationTag>;
using PolicyGeneration = Generation<PolicyGenerationTag>;
using PartitionId = Id<PartitionTag>;
using PartitionGeneration = Generation<PartitionGenerationTag>;
using ChunkId = Id<ChunkTag>;
using ProducerId = Id<ProducerTag>;
using ConsumerId = Id<ConsumerTag>;
using WaveId = Id<WaveTag>;
using TransferAttemptId = Id<TransferAttemptTag>;
using IncarnationId = Id<IncarnationTag>;
using SessionId = Id<SessionTag>;
using CommitSequence = Id<CommitSequenceTag>;
using TickId = Id<TickTag>;

// A monotonic logical clock owned by the coordinator. Scheduling decisions use
// ticks rather than wall-clock time so that replays and tests are exact.
class LogicalClock {
 public:
  [[nodiscard]] TickId now() const noexcept { return tick_; }
  [[nodiscard]] TickId advance() noexcept {
    tick_ = tick_.next();
    return tick_;
  }
  void reset(TickId tick) noexcept { tick_ = tick; }

 private:
  TickId tick_{};
};

}  // namespace shuffle::fabric

namespace std {

template <class Tag, class Rep>
struct hash<shuffle::fabric::Id<Tag, Rep>> {
  [[nodiscard]] size_t operator()(const shuffle::fabric::Id<Tag, Rep>& id) const noexcept {
    return std::hash<Rep>{}(id.value());
  }
};

}  // namespace std
