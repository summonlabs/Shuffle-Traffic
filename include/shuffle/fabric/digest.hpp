// Content digests (SHA-256) used for chunk, partition and state integrity.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#pragma once

#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include "shuffle/fabric/error.hpp"

namespace shuffle::fabric {

inline constexpr std::size_t kDigestBytes = 32;
inline constexpr std::size_t kHexDigestChars = kDigestBytes * 2;

struct Digest {
  std::array<std::uint8_t, kDigestBytes> bytes{};

  friend constexpr bool operator==(const Digest&, const Digest&) noexcept = default;
  friend constexpr auto operator<=>(const Digest&, const Digest&) noexcept = default;

  [[nodiscard]] constexpr bool is_zero() const noexcept {
    for (const std::uint8_t byte : bytes) {
      if (byte != 0) {
        return false;
      }
    }
    return true;
  }

  // Lower-case hex, exactly 64 characters.
  [[nodiscard]] std::string to_hex() const;

  // Canonical hex form: exactly 64 lower-case hex characters.
  [[nodiscard]] static Result<Digest> from_hex(std::string_view text);
};

}  // namespace shuffle::fabric

namespace std {

template <>
struct hash<shuffle::fabric::Digest> {
  [[nodiscard]] size_t operator()(const shuffle::fabric::Digest& digest) const noexcept {
    // FNV-1a over the digest bytes: cheap, stable and independent of
    // std::hash<std::string> implementations.
    std::uint64_t hash = 1469598103934665603ull;
    for (const std::uint8_t byte : digest.bytes) {
      hash ^= byte;
      hash *= 1099511628211ull;
    }
    return static_cast<size_t>(hash);
  }
};

}  // namespace std
