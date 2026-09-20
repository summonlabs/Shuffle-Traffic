// Streaming SHA-256 and CRC-32C. No third-party dependency is used: the
// integrity primitives of the fabric are part of the fabric.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

#include "shuffle/fabric/digest.hpp"

namespace shuffle::fabric {

class Sha256 {
 public:
  Sha256() noexcept;

  void update(std::span<const std::byte> data) noexcept;
  void update(std::string_view text) noexcept;
  void update(const void* data, std::size_t size) noexcept;

  // Finalizes and returns the digest. The hasher is left in a finished state;
  // reset() must be called before reuse.
  [[nodiscard]] Digest finish() noexcept;
  void reset() noexcept;

 private:
  void compress(const std::uint8_t* block) noexcept;

  std::uint32_t state_[8];
  std::uint64_t bit_length_;
  std::array<std::uint8_t, 64> buffer_;
  std::size_t buffered_;
};

[[nodiscard]] Digest sha256(std::span<const std::byte> data) noexcept;
[[nodiscard]] Digest sha256(std::string_view text) noexcept;

// CRC-32C (Castagnoli, polynomial 0x1EDC6F41 reflected, init/xor 0xFFFFFFFF).
[[nodiscard]] std::uint32_t crc32c(std::span<const std::byte> data) noexcept;
[[nodiscard]] std::uint32_t crc32c(std::string_view text) noexcept;

}  // namespace shuffle::fabric
