// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/hash.hpp"

#include <array>
#include <cstring>

#include "shuffle/fabric/bytes.hpp"

namespace shuffle::fabric {
namespace {

constexpr std::array<std::uint32_t, 64> kSha256RoundConstants = {
    0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u, 0x3956c25bu, 0x59f111f1u, 0x923f82a4u, 0xab1c5ed5u,
    0xd807aa98u, 0x12835b01u, 0x243185beu, 0x550c7dc3u, 0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u, 0xc19bf174u,
    0xe49b69c1u, 0xefbe4786u, 0x0fc19dc6u, 0x240ca1ccu, 0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau,
    0x983e5152u, 0xa831c66du, 0xb00327c8u, 0xbf597fc7u, 0xc6e00bf3u, 0xd5a79147u, 0x06ca6351u, 0x14292967u,
    0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu, 0x53380d13u, 0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u,
    0xa2bfe8a1u, 0xa81a664bu, 0xc24b8b70u, 0xc76c51a3u, 0xd192e819u, 0xd6990624u, 0xf40e3585u, 0x106aa070u,
    0x19a4c116u, 0x1e376c08u, 0x2748774cu, 0x34b0bcb5u, 0x391c0cb3u, 0x4ed8aa4au, 0x5b9cca4fu, 0x682e6ff3u,
    0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u, 0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u};

constexpr std::array<std::uint32_t, 8> kSha256InitialState = {
    0x6a09e667u, 0xbb67ae85u, 0x3c6ef372u, 0xa54ff53au, 0x510e527fu, 0x9b05688cu, 0x1f83d9abu, 0x5be0cd19u};

constexpr std::uint32_t rotr(std::uint32_t value, unsigned count) noexcept {
  return (value >> count) | (value << (32u - count));
}

constexpr std::uint32_t big_endian_load(const std::uint8_t* data) noexcept {
  return (static_cast<std::uint32_t>(data[0]) << 24) | (static_cast<std::uint32_t>(data[1]) << 16) |
         (static_cast<std::uint32_t>(data[2]) << 8) | static_cast<std::uint32_t>(data[3]);
}

// CRC-32C (Castagnoli) reflected lookup table, generated at compile time.
struct Crc32cTable {
  std::array<std::uint32_t, 256> entries{};

  constexpr Crc32cTable() noexcept {
    for (std::uint32_t index = 0; index < 256; ++index) {
      std::uint32_t value = index;
      for (int bit = 0; bit < 8; ++bit) {
        value = (value & 1u) != 0u ? (0x82f63b78u ^ (value >> 1)) : (value >> 1);
      }
      entries[index] = value;
    }
  }
};

constexpr Crc32cTable kCrc32cTable{};

}  // namespace

Sha256::Sha256() noexcept : state_{}, bit_length_(0), buffer_{}, buffered_(0) {
  reset();
}

void Sha256::reset() noexcept {
  for (std::size_t index = 0; index < kSha256InitialState.size(); ++index) {
    state_[index] = kSha256InitialState[index];
  }
  bit_length_ = 0;
  buffered_ = 0;
  buffer_.fill(0);
}

void Sha256::compress(const std::uint8_t* block) noexcept {
  std::uint32_t schedule[64];
  for (std::size_t index = 0; index < 16; ++index) {
    schedule[index] = big_endian_load(block + (index * 4));
  }
  for (std::size_t index = 16; index < 64; ++index) {
    const std::uint32_t s0 = rotr(schedule[index - 15], 7) ^ rotr(schedule[index - 15], 18) ^ (schedule[index - 15] >> 3);
    const std::uint32_t s1 = rotr(schedule[index - 2], 17) ^ rotr(schedule[index - 2], 19) ^ (schedule[index - 2] >> 10);
    schedule[index] = schedule[index - 16] + s0 + schedule[index - 7] + s1;
  }

  std::uint32_t a = state_[0];
  std::uint32_t b = state_[1];
  std::uint32_t c = state_[2];
  std::uint32_t d = state_[3];
  std::uint32_t e = state_[4];
  std::uint32_t f = state_[5];
  std::uint32_t g = state_[6];
  std::uint32_t h = state_[7];

  for (std::size_t index = 0; index < 64; ++index) {
    const std::uint32_t s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
    const std::uint32_t choose = (e & f) ^ ((~e) & g);
    const std::uint32_t temp1 = h + s1 + choose + kSha256RoundConstants[index] + schedule[index];
    const std::uint32_t s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
    const std::uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
    const std::uint32_t temp2 = s0 + majority;

    h = g;
    g = f;
    f = e;
    e = d + temp1;
    d = c;
    c = b;
    b = a;
    a = temp1 + temp2;
  }

  state_[0] += a;
  state_[1] += b;
  state_[2] += c;
  state_[3] += d;
  state_[4] += e;
  state_[5] += f;
  state_[6] += g;
  state_[7] += h;
}

void Sha256::update(const void* data, std::size_t size) noexcept {
  const auto* cursor = static_cast<const std::uint8_t*>(data);
  bit_length_ += static_cast<std::uint64_t>(size) * 8u;

  if (buffered_ > 0) {
    const std::size_t needed = 64 - buffered_;
    const std::size_t taken = size < needed ? size : needed;
    std::memcpy(buffer_.data() + buffered_, cursor, taken);
    buffered_ += taken;
    cursor += taken;
    size -= taken;
    if (buffered_ == 64) {
      compress(buffer_.data());
      buffered_ = 0;
    }
  }

  while (size >= 64) {
    compress(cursor);
    cursor += 64;
    size -= 64;
  }

  if (size > 0) {
    std::memcpy(buffer_.data() + buffered_, cursor, size);
    buffered_ += size;
  }
}

void Sha256::update(std::span<const std::byte> data) noexcept {
  update(data.data(), data.size());
}

void Sha256::update(std::string_view text) noexcept {
  update(text.data(), text.size());
}

Digest Sha256::finish() noexcept {
  const std::uint64_t length_bits = bit_length_;
  const std::uint8_t padding = 0x80;
  update(&padding, 1);

  const std::uint8_t zero = 0x00;
  while (buffered_ != 56) {
    update(&zero, 1);
  }

  std::uint8_t length_bytes[8];
  for (std::size_t index = 0; index < 8; ++index) {
    length_bytes[index] = static_cast<std::uint8_t>((length_bits >> ((7 - index) * 8)) & 0xffu);
  }
  update(length_bytes, 8);

  Digest digest{};
  for (std::size_t index = 0; index < 8; ++index) {
    const std::uint32_t word = state_[index];
    digest.bytes[(index * 4) + 0] = static_cast<std::uint8_t>((word >> 24) & 0xffu);
    digest.bytes[(index * 4) + 1] = static_cast<std::uint8_t>((word >> 16) & 0xffu);
    digest.bytes[(index * 4) + 2] = static_cast<std::uint8_t>((word >> 8) & 0xffu);
    digest.bytes[(index * 4) + 3] = static_cast<std::uint8_t>(word & 0xffu);
  }
  return digest;
}

Digest sha256(std::span<const std::byte> data) noexcept {
  Sha256 hasher;
  hasher.update(data);
  return hasher.finish();
}

Digest sha256(std::string_view text) noexcept {
  return sha256(as_bytes(text));
}

std::uint32_t crc32c(std::span<const std::byte> data) noexcept {
  std::uint32_t crc = 0xffffffffu;
  for (const std::byte raw : data) {
    const std::uint32_t byte = static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(raw));
    crc = kCrc32cTable.entries[(crc ^ byte) & 0xffu] ^ (crc >> 8);
  }
  return ~crc;
}

std::uint32_t crc32c(std::string_view text) noexcept {
  return crc32c(as_bytes(text));
}

}  // namespace shuffle::fabric
