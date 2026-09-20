// Canonical binary encoding for wire messages and durable records.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Rules (version 1):
//   * fixed-width little-endian integers, no varints, no padding;
//   * every length-prefixed value carries a 32-bit byte count;
//   * collections carry a 32-bit element count that is checked against the
//     configured Limits *and* against the bytes actually remaining;
//   * strings must be valid UTF-8;
//   * a decoder that consumed fewer bytes than the payload contains reports
//     TrailingGarbage -- canonical form is exact.
//
// ByteReader uses a sticky error: the first failure is recorded and every
// later read returns a zero value without touching memory. Callers must check
// status()/require_end() before using what they decoded.

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "shuffle/fabric/bytes.hpp"
#include "shuffle/fabric/digest.hpp"
#include "shuffle/fabric/error.hpp"

namespace shuffle::fabric {

[[nodiscard]] bool is_valid_utf8(std::string_view text) noexcept;

class ByteWriter {
 public:
  explicit ByteWriter(std::size_t reserve = 0);

  void put_u8(std::uint8_t value);
  void put_u16(std::uint16_t value);
  void put_u32(std::uint32_t value);
  void put_u64(std::uint64_t value);
  void put_bool(bool value);
  void put_digest(const Digest& digest);
  void put_string(std::string_view text);
  void put_byte_string(std::span<const std::byte> data);
  void put_raw(std::span<const std::byte> data);

  [[nodiscard]] const std::vector<std::byte>& data() const noexcept { return buffer_; }
  [[nodiscard]] std::size_t size() const noexcept { return buffer_.size(); }
  [[nodiscard]] std::vector<std::byte> take();

 private:
  std::vector<std::byte> buffer_;
};

class ByteReader {
 public:
  ByteReader(std::span<const std::byte> data, const Limits& limits, std::string_view context);

  [[nodiscard]] bool ok() const noexcept { return error_.ok(); }
  [[nodiscard]] const Error& error() const noexcept { return error_; }
  [[nodiscard]] Status status() const { return error_.ok() ? Status{} : Status{error_}; }
  [[nodiscard]] std::size_t remaining() const noexcept { return data_.size() - offset_; }
  [[nodiscard]] std::string_view context() const noexcept { return context_; }

  std::uint8_t u8();
  std::uint16_t u16();
  std::uint32_t u32();
  std::uint64_t u64();
  bool boolean();
  Digest digest();

  // Length-prefixed (32-bit) byte string bounded by max_bytes.
  std::span<const std::byte> bytes(std::uint64_t max_bytes);

  // Length-prefixed (32-bit) UTF-8 string bounded by max_bytes.
  std::string string(std::uint32_t max_bytes);

  // Element count of a collection, bounded by max_items, by
  // Limits::max_collection_items and by the bytes that remain.
  std::uint32_t collection_count(std::uint64_t max_items);

  // Succeeds only when no error was recorded and no byte is left over.
  Status require_end();

 private:
  void fail(ErrorCode code, std::string_view detail);
  std::span<const std::byte> raw(std::uint64_t count);

  std::span<const std::byte> data_;
  // The bounds are owned by value: a decoder must never depend on the lifetime
  // of a caller-supplied object it was handed by reference.
  Limits limits_;
  std::string_view context_;
  std::size_t offset_{0};
  Error error_{};
};

}  // namespace shuffle::fabric
