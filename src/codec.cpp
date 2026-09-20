// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/codec.hpp"

#include <cstring>

namespace shuffle::fabric {

bool is_valid_utf8(std::string_view text) noexcept {
  const auto* bytes = reinterpret_cast<const std::uint8_t*>(text.data());
  const std::size_t size = text.size();
  std::size_t index = 0;
  while (index < size) {
    const std::uint8_t lead = bytes[index];
    if (lead < 0x80u) {
      ++index;
      continue;
    }

    std::size_t continuation = 0;
    std::uint32_t code_point = 0;
    std::uint32_t minimum = 0;
    if ((lead & 0xe0u) == 0xc0u) {
      continuation = 1;
      code_point = lead & 0x1fu;
      minimum = 0x80u;
    } else if ((lead & 0xf0u) == 0xe0u) {
      continuation = 2;
      code_point = lead & 0x0fu;
      minimum = 0x800u;
    } else if ((lead & 0xf8u) == 0xf0u) {
      continuation = 3;
      code_point = lead & 0x07u;
      minimum = 0x10000u;
    } else {
      return false;
    }

    if (index + continuation >= size) {
      return false;
    }
    for (std::size_t offset = 1; offset <= continuation; ++offset) {
      const std::uint8_t next = bytes[index + offset];
      if ((next & 0xc0u) != 0x80u) {
        return false;
      }
      code_point = (code_point << 6) | (next & 0x3fu);
    }
    if (code_point < minimum || code_point > 0x10ffffu) {
      return false;  // overlong encoding or beyond the Unicode range
    }
    if (code_point >= 0xd800u && code_point <= 0xdfffu) {
      return false;  // UTF-16 surrogate half
    }
    index += continuation + 1;
  }
  return true;
}

ByteWriter::ByteWriter(std::size_t reserve) {
  buffer_.reserve(reserve);
}

void ByteWriter::put_u8(std::uint8_t value) {
  buffer_.push_back(static_cast<std::byte>(value));
}

void ByteWriter::put_u16(std::uint16_t value) {
  put_u8(static_cast<std::uint8_t>(value & 0xffu));
  put_u8(static_cast<std::uint8_t>((value >> 8) & 0xffu));
}

void ByteWriter::put_u32(std::uint32_t value) {
  put_u16(static_cast<std::uint16_t>(value & 0xffffu));
  put_u16(static_cast<std::uint16_t>((value >> 16) & 0xffffu));
}

void ByteWriter::put_u64(std::uint64_t value) {
  put_u32(static_cast<std::uint32_t>(value & 0xffffffffull));
  put_u32(static_cast<std::uint32_t>((value >> 32) & 0xffffffffull));
}

void ByteWriter::put_bool(bool value) {
  put_u8(value ? 1u : 0u);
}

void ByteWriter::put_digest(const Digest& digest) {
  put_raw(std::span<const std::byte>{reinterpret_cast<const std::byte*>(digest.bytes.data()), digest.bytes.size()});
}

void ByteWriter::put_string(std::string_view text) {
  put_u32(static_cast<std::uint32_t>(text.size()));
  put_raw(as_bytes(text));
}

void ByteWriter::put_byte_string(std::span<const std::byte> data) {
  put_u32(static_cast<std::uint32_t>(data.size()));
  put_raw(data);
}

void ByteWriter::put_raw(std::span<const std::byte> data) {
  buffer_.insert(buffer_.end(), data.begin(), data.end());
}

std::vector<std::byte> ByteWriter::take() {
  std::vector<std::byte> out = std::move(buffer_);
  buffer_.clear();
  return out;
}

ByteReader::ByteReader(std::span<const std::byte> data, const Limits& limits, std::string_view context)
    : data_(data), limits_(limits), context_(context) {}

void ByteReader::fail(ErrorCode code, std::string_view detail) {
  if (error_.ok()) {
    error_ = Error{code, std::string(detail)};
  }
}

std::span<const std::byte> ByteReader::raw(std::uint64_t count) {
  if (!error_.ok()) {
    return {};
  }
  if (count > remaining()) {
    fail(ErrorCode::TruncatedInput, "payload ended before the declared length");
    return {};
  }
  if (count > limits_.max_allocation_bytes) {
    fail(ErrorCode::OversizedInput, "declared length exceeds max_allocation_bytes");
    return {};
  }
  const auto size = static_cast<std::size_t>(count);
  const std::span<const std::byte> slice = data_.subspan(offset_, size);
  offset_ += size;
  return slice;
}

std::uint8_t ByteReader::u8() {
  const auto slice = raw(1);
  if (slice.empty()) {
    return 0;
  }
  return std::to_integer<std::uint8_t>(slice[0]);
}

std::uint16_t ByteReader::u16() {
  const auto slice = raw(2);
  if (slice.size() != 2) {
    return 0;
  }
  return static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(slice[0]) |
                                    (static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(slice[1])) << 8));
}

std::uint32_t ByteReader::u32() {
  const auto slice = raw(4);
  if (slice.size() != 4) {
    return 0;
  }
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(slice[index])) << (index * 8);
  }
  return value;
}

std::uint64_t ByteReader::u64() {
  const auto slice = raw(8);
  if (slice.size() != 8) {
    return 0;
  }
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(slice[index])) << (index * 8);
  }
  return value;
}

bool ByteReader::boolean() {
  const std::uint8_t value = u8();
  if (!error_.ok()) {
    return false;
  }
  if (value > 1u) {
    fail(ErrorCode::MalformedInput, "boolean field must be 0 or 1");
    return false;
  }
  return value == 1u;
}

Digest ByteReader::digest() {
  Digest digest{};
  const auto slice = raw(kDigestBytes);
  if (slice.size() != kDigestBytes) {
    return digest;
  }
  std::memcpy(digest.bytes.data(), slice.data(), kDigestBytes);
  return digest;
}

std::span<const std::byte> ByteReader::bytes(std::uint64_t max_bytes) {
  const std::uint32_t declared = u32();
  if (!error_.ok()) {
    return {};
  }
  if (declared > max_bytes) {
    fail(ErrorCode::OversizedInput, "declared byte string exceeds its bound");
    return {};
  }
  return raw(declared);
}

std::string ByteReader::string(std::uint32_t max_bytes) {
  const std::uint32_t declared = u32();
  if (!error_.ok()) {
    return {};
  }
  if (declared > max_bytes) {
    fail(ErrorCode::OversizedInput, "declared string exceeds its bound");
    return {};
  }
  const auto slice = raw(declared);
  if (!error_.ok()) {
    return {};
  }
  const std::string_view text = as_chars(slice);
  if (!is_valid_utf8(text)) {
    fail(ErrorCode::InvalidUnicode, "string is not valid UTF-8");
    return {};
  }
  return std::string{text};
}

std::uint32_t ByteReader::collection_count(std::uint64_t max_items) {
  const std::uint32_t declared = u32();
  if (!error_.ok()) {
    return 0;
  }
  if (static_cast<std::uint64_t>(declared) > max_items) {
    fail(ErrorCode::LimitExceeded, "collection count exceeds the caller bound");
    return 0;
  }
  if (declared > limits_.max_collection_items) {
    fail(ErrorCode::LimitExceeded, "collection count exceeds max_collection_items");
    return 0;
  }
  // Every element costs at least one byte, so a count larger than the bytes
  // that remain is impossible regardless of the caller's bound.
  if (static_cast<std::uint64_t>(declared) > remaining()) {
    fail(ErrorCode::MalformedInput, "collection count exceeds the remaining payload");
    return 0;
  }
  return declared;
}

Status ByteReader::require_end() {
  if (!error_.ok()) {
    return Status{error_};
  }
  if (offset_ != data_.size()) {
    return Status{make_error(ErrorCode::TrailingGarbage, "payload contains trailing bytes after the canonical form")};
  }
  return Status{};
}

}  // namespace shuffle::fabric
