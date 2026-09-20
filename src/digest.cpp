// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/digest.hpp"

namespace shuffle::fabric {
namespace {

constexpr char kHexDigits[] = "0123456789abcdef";

[[nodiscard]] constexpr int hex_value(char ch) noexcept {
  if (ch >= '0' && ch <= '9') {
    return ch - '0';
  }
  if (ch >= 'a' && ch <= 'f') {
    return (ch - 'a') + 10;
  }
  return -1;
}

}  // namespace

std::string Digest::to_hex() const {
  std::string text;
  text.resize(kHexDigestChars);
  for (std::size_t index = 0; index < kDigestBytes; ++index) {
    const std::uint8_t byte = bytes[index];
    text[index * 2] = kHexDigits[(byte >> 4) & 0x0fu];
    text[(index * 2) + 1] = kHexDigits[byte & 0x0fu];
  }
  return text;
}

Result<Digest> Digest::from_hex(std::string_view text) {
  if (text.size() != kHexDigestChars) {
    return make_failure<Digest>(ErrorCode::InvalidArgument, "digest text must be exactly 64 hex characters");
  }
  Digest digest{};
  for (std::size_t index = 0; index < kDigestBytes; ++index) {
    const int high = hex_value(text[index * 2]);
    const int low = hex_value(text[(index * 2) + 1]);
    if (high < 0 || low < 0) {
      return make_failure<Digest>(ErrorCode::InvalidArgument, "digest text contains a non-hex character");
    }
    digest.bytes[index] = static_cast<std::uint8_t>((high << 4) | low);
  }
  return digest;
}

}  // namespace shuffle::fabric
