// Seeded xoshiro256** generator used by property and adversarial tests.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace shuffle::test {

class Random {
 public:
  explicit Random(std::uint64_t seed) noexcept : seed_(seed) {
    std::uint64_t state = seed;
    for (std::size_t index = 0; index < state_.size(); ++index) {
      state_[index] = splitmix64(state);
    }
  }

  [[nodiscard]] std::uint64_t seed() const noexcept { return seed_; }

  [[nodiscard]] std::uint64_t next_u64() noexcept {
    const std::uint64_t result = rotl(state_[1] * 5ull, 7) * 9ull;
    const std::uint64_t temp = state_[1] << 17;

    state_[2] ^= state_[0];
    state_[3] ^= state_[1];
    state_[1] ^= state_[2];
    state_[0] ^= state_[3];
    state_[2] ^= temp;
    state_[3] = rotl(state_[3], 45);
    return result;
  }

  // Uniform in [0, bound); returns 0 when bound == 0.
  [[nodiscard]] std::uint64_t bounded(std::uint64_t bound) noexcept {
    if (bound == 0) {
      return 0;
    }
    const std::uint64_t threshold = (0ull - bound) % bound;  // rejection sampling: no modulo bias
    for (;;) {
      const std::uint64_t value = next_u64();
      if (value >= threshold) {
        return value % bound;
      }
    }
  }

  [[nodiscard]] bool chance(std::uint32_t percent) noexcept { return bounded(100) < percent; }

  [[nodiscard]] std::uint64_t in_range(std::uint64_t low, std::uint64_t high) noexcept {
    if (high <= low) {
      return low;
    }
    return low + bounded(high - low + 1);
  }

  [[nodiscard]] std::string reproduction(std::string_view label) const {
    return std::string{"--seed="} + std::to_string(seed_) + " --filter=" + std::string{label};
  }

 private:
  [[nodiscard]] static constexpr std::uint64_t rotl(std::uint64_t value, int shift) noexcept {
    return (value << shift) | (value >> (64 - shift));
  }

  [[nodiscard]] static constexpr std::uint64_t splitmix64(std::uint64_t& state) noexcept {
    state += 0x9e3779b97f4a7c15ull;
    std::uint64_t value = state;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ull;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebull;
    return value ^ (value >> 31);
  }

  std::uint64_t seed_;
  std::array<std::uint64_t, 4> state_{};
};

}  // namespace shuffle::test
