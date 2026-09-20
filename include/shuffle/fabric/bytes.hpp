// Checked arithmetic, byte helpers and the explicit resource bounds that every
// externally influenced size must pass through.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string_view>

#include "shuffle/fabric/error.hpp"

namespace shuffle::fabric {

[[nodiscard]] constexpr bool add_overflows(std::uint64_t a, std::uint64_t b) noexcept {
  return a > (std::numeric_limits<std::uint64_t>::max() - b);
}

[[nodiscard]] constexpr bool mul_overflows(std::uint64_t a, std::uint64_t b) noexcept {
  return b != 0 && a > (std::numeric_limits<std::uint64_t>::max() / b);
}

// Checked 64-bit arithmetic. Externally supplied sizes never reach an
// allocation without passing through one of these.
[[nodiscard]] inline Result<std::uint64_t> checked_add(std::uint64_t a, std::uint64_t b) {
  if (add_overflows(a, b)) {
    return make_failure<std::uint64_t>(ErrorCode::IntegerOverflow, "checked_add overflow");
  }
  return a + b;
}

[[nodiscard]] inline Result<std::uint64_t> checked_mul(std::uint64_t a, std::uint64_t b) {
  if (mul_overflows(a, b)) {
    return make_failure<std::uint64_t>(ErrorCode::IntegerOverflow, "checked_mul overflow");
  }
  return a * b;
}

// Rejects values that cannot be represented as std::size_t on this platform.
[[nodiscard]] inline Result<std::size_t> to_size(std::uint64_t value) {
  if (value > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return make_failure<std::size_t>(ErrorCode::IntegerOverflow, "size does not fit std::size_t");
  }
  return static_cast<std::size_t>(value);
}

// Bounds applied to everything that arrives from outside the library: frames,
// durable files, manifests, collections, strings and retained history. A value
// of 0 means "nothing of this kind is accepted".
struct Limits {
  std::uint64_t max_frame_payload_bytes = 1ull << 20;      // 1 MiB per wire frame
  std::uint64_t max_message_payload_bytes = 1ull << 23;    // 8 MiB per decoded message
  // Bounds one wire-message allocation. Durable payloads are bounded separately
  // by max_state_bytes, which is deliberately larger than a single message.
  std::uint64_t max_allocation_bytes = 1ull << 23;         // 8 MiB single allocation
  std::uint32_t max_string_bytes = 4096;
  std::uint32_t max_endpoint_bytes = 256;
  std::uint32_t max_collection_items = 1u << 18;           // 262144 elements
  std::uint64_t max_chunks_per_partition = 1ull << 20;
  std::uint32_t max_partitions = 1u << 22;
  std::uint32_t max_participants = 1u << 20;
  std::uint32_t max_selection_patterns = 1u << 16;
  std::uint64_t max_selection_items = 1ull << 22;
  std::uint64_t max_planned_edges = 1ull << 32;
  std::uint64_t max_tracked_edges = 1ull << 26;            // 67M tracked edge records
  std::uint64_t max_tracked_partitions = 1ull << 22;
  std::uint64_t max_state_bytes = 1ull << 28;              // 256 MiB durable payload
  std::uint64_t max_journal_record_bytes = 1ull << 20;     // 1 MiB journal record
  std::uint32_t max_retained_generations = 8;
  std::uint32_t max_attempt_records_per_shuffle = 1u << 20;
  std::uint32_t max_pending_retries = 1u << 16;
  std::uint32_t max_wave_grants = 1u << 14;                // 16384 grants per wave

  // Structural consistency of the bound set itself.
  [[nodiscard]] Status validate() const;

  friend bool operator==(const Limits&, const Limits&) noexcept = default;
};

[[nodiscard]] inline std::span<const std::byte> as_bytes(std::string_view text) noexcept {
  return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

[[nodiscard]] inline std::string_view as_chars(std::span<const std::byte> data) noexcept {
  return {reinterpret_cast<const char*>(data.data()), data.size()};
}

}  // namespace shuffle::fabric
