// Protocol proof surface for the wire transport: frame layout, round trips,
// every refusal the decoder owes a peer, bounded stream reading and replay
// suppression.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Every expectation in this file is built from explicit byte offsets rather
// than from the library's codec, so a layout mistake cannot prove itself
// correct. Where a field must be refused the header CRC is recomputed first, so
// the decoder is forced to judge that field itself instead of stopping at the
// integrity gate in front of it.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "shuffle/fabric/bytes.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/frame.hpp"
#include "shuffle/fabric/hash.hpp"
#include "shuffle/fabric/identity.hpp"
#include "test_support.hpp"

namespace shuffle::fabric {

// Test-only rendering so a failure names the message type and the error code.
inline std::ostream& operator<<(std::ostream& stream, MessageType type) {
  return stream << to_string(type);
}

inline std::ostream& operator<<(std::ostream& stream, ErrorCode code) {
  return stream << to_string(code);
}

}  // namespace shuffle::fabric

namespace {

using namespace shuffle::fabric;

// ---------------------------------------------------------------------------
// Independent little-endian helpers. The library's own codec is never used to
// construct an expectation.
// ---------------------------------------------------------------------------

void put_u16_at(std::vector<std::byte>& bytes, std::size_t offset, std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffu);
  bytes[offset + 1] = static_cast<std::byte>((value >> 8) & 0xffu);
}

void put_u32_at(std::vector<std::byte>& bytes, std::size_t offset, std::uint32_t value) {
  for (std::size_t index = 0; index < 4; ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8)) & 0xffu);
  }
}

void put_u64_at(std::vector<std::byte>& bytes, std::size_t offset, std::uint64_t value) {
  for (std::size_t index = 0; index < 8; ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8)) & 0xffu);
  }
}

[[nodiscard]] std::uint8_t byte_at(std::span<const std::byte> bytes, std::size_t offset) {
  return std::to_integer<std::uint8_t>(bytes[offset]);
}

[[nodiscard]] unsigned unsigned_at(std::span<const std::byte> bytes, std::size_t offset) {
  return static_cast<unsigned>(byte_at(bytes, offset));
}

[[nodiscard]] std::uint32_t u32_at(std::span<const std::byte> bytes, std::size_t offset) {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value |= static_cast<std::uint32_t>(byte_at(bytes, offset + index)) << (index * 8);
  }
  return value;
}

[[nodiscard]] std::span<const std::byte> header_prefix(const std::vector<std::byte>& bytes) {
  return std::span<const std::byte>(bytes.data(), kFrameHeaderCrcOffset);
}

[[nodiscard]] std::vector<std::byte> bytes_of(std::string_view text) {
  std::vector<std::byte> out;
  out.reserve(text.size());
  for (const char ch : text) {
    out.push_back(static_cast<std::byte>(static_cast<unsigned char>(ch)));
  }
  return out;
}

[[nodiscard]] bool same_bytes(std::span<const std::byte> lhs, std::span<const std::byte> rhs) {
  return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

[[nodiscard]] Limits frame_limits(std::uint64_t max_payload) {
  Limits limits{};
  limits.max_frame_payload_bytes = max_payload;
  return limits;
}

// A frame header described field by field, as the wire sees it.
struct HeaderFields {
  std::uint16_t version{kProtocolVersion};
  std::uint16_t type{static_cast<std::uint16_t>(MessageType::HandshakeRequest)};
  std::uint32_t flags{0};
  std::uint64_t session{0};
  std::uint64_t sequence{1};
  std::uint32_t payload_length{0};
  std::uint32_t payload_crc32c{0};
};

// Builds the 40 header bytes at their documented offsets. fix_crc recomputes the
// header CRC, which is what lets a test express "well formed except this field".
[[nodiscard]] std::vector<std::byte> make_header(const HeaderFields& fields, bool fix_crc) {
  std::vector<std::byte> header(kFrameHeaderBytes, std::byte{0});
  header[0] = static_cast<std::byte>('S');
  header[1] = static_cast<std::byte>('F');
  header[2] = static_cast<std::byte>('B');
  header[3] = static_cast<std::byte>('1');
  put_u16_at(header, 4, fields.version);
  put_u16_at(header, 6, fields.type);
  put_u32_at(header, 8, fields.flags);
  put_u64_at(header, 12, fields.session);
  put_u64_at(header, 20, fields.sequence);
  put_u32_at(header, 28, fields.payload_length);
  put_u32_at(header, 32, fields.payload_crc32c);
  if (fix_crc) {
    put_u32_at(header, 36, crc32c(header_prefix(header)));
  }
  return header;
}

// Builds a full frame: header plus payload, with either CRC under the test's
// control so that an integrity failure can be aimed at exactly one of them.
[[nodiscard]] std::vector<std::byte> make_frame(HeaderFields fields, std::span<const std::byte> payload,
                                                bool fix_header_crc = true, bool fix_payload_crc = true) {
  if (fields.payload_length == 0 && !payload.empty()) {
    fields.payload_length = static_cast<std::uint32_t>(payload.size());
  }
  if (fix_payload_crc) {
    fields.payload_crc32c = crc32c(payload);
  }
  std::vector<std::byte> frame = make_header(fields, fix_header_crc);
  frame.insert(frame.end(), payload.begin(), payload.end());
  return frame;
}

[[nodiscard]] std::vector<std::byte> valid_frame(MessageType type = MessageType::ProgressRequest,
                                                 std::string_view payload = "payload-under-proof") {
  HeaderFields fields;
  fields.type = static_cast<std::uint16_t>(type);
  fields.session = 0x1122334455667788ull;
  fields.sequence = 7;
  return make_frame(fields, bytes_of(payload));
}

// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------

SHUFFLE_TEST(protocol_frames, header_layout_is_exactly_the_documented_offsets) {
  const std::vector<std::byte> payload = bytes_of("abc");

  FrameHeader header;
  header.type = MessageType::CommitTransferResponse;  // 19
  header.flags = kFlagResponse;
  header.session = SessionId{0x0102030405060708ull};
  header.sequence = 0x1112131415161718ull;

  std::vector<std::byte> encoded;
  REQUIRE_OK(encode_frame(header, payload, encoded, Limits{}));
  REQUIRE_EQ(encoded.size(), kFrameHeaderBytes + payload.size());

  // Magic: the ASCII bytes 'S','F','B','1' in that wire order, which is the
  // little-endian value 0x31424653.
  REQUIRE_EQ(kFrameMagic, 0x31424653u);
  REQUIRE_EQ(unsigned_at(encoded, 0), 0x53u);
  REQUIRE_EQ(unsigned_at(encoded, 1), 0x46u);
  REQUIRE_EQ(unsigned_at(encoded, 2), 0x42u);
  REQUIRE_EQ(unsigned_at(encoded, 3), 0x31u);

  // Version and message type, little-endian, at their fixed offsets.
  REQUIRE_EQ(unsigned_at(encoded, 4), 0x01u);
  REQUIRE_EQ(unsigned_at(encoded, 5), 0x00u);
  REQUIRE_EQ(unsigned_at(encoded, 6), 19u);
  REQUIRE_EQ(unsigned_at(encoded, 7), 0x00u);

  // Flags: bit 0 is Response, everything else is zero.
  REQUIRE_EQ(unsigned_at(encoded, 8), 0x01u);
  REQUIRE_EQ(unsigned_at(encoded, 9), 0x00u);
  REQUIRE_EQ(unsigned_at(encoded, 10), 0x00u);
  REQUIRE_EQ(unsigned_at(encoded, 11), 0x00u);

  const std::uint8_t expected_session[8] = {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01};
  const std::uint8_t expected_sequence[8] = {0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11};
  for (std::size_t index = 0; index < 8; ++index) {
    REQUIRE_EQ(unsigned_at(encoded, 12 + index), static_cast<unsigned>(expected_session[index]));
    REQUIRE_EQ(unsigned_at(encoded, 20 + index), static_cast<unsigned>(expected_sequence[index]));
  }

  REQUIRE_EQ(u32_at(encoded, 28), 3u);               // payload length
  REQUIRE_EQ(u32_at(encoded, 32), crc32c(payload));  // payload CRC-32C
  REQUIRE_EQ(u32_at(encoded, 36), crc32c(std::span<const std::byte>(encoded.data(), kFrameHeaderCrcOffset)));

  // The hand-built bytes and the encoded bytes must be identical.
  HeaderFields fields;
  fields.type = static_cast<std::uint16_t>(MessageType::CommitTransferResponse);
  fields.flags = kFlagResponse;
  fields.session = 0x0102030405060708ull;
  fields.sequence = 0x1112131415161718ull;
  const std::vector<std::byte> golden = make_frame(fields, payload);
  REQUIRE_EQ(golden.size(), encoded.size());
  REQUIRE(same_bytes(encoded, golden));

  // And the hand-built bytes decode back to exactly those field values.
  const auto decoded = decode_frame(golden, Limits{});
  REQUIRE_OK(decoded);
  REQUIRE_EQ(decoded.value().header.version, kProtocolVersion);
  REQUIRE_EQ(decoded.value().header.type, MessageType::CommitTransferResponse);
  REQUIRE_EQ(decoded.value().header.flags, kFlagResponse);
  REQUIRE(decoded.value().header.carries_response_flag());
  REQUIRE_EQ(decoded.value().header.session, SessionId{0x0102030405060708ull});
  REQUIRE_EQ(decoded.value().header.sequence, 0x1112131415161718ull);
  REQUIRE_EQ(decoded.value().header.payload_length, 3u);
  REQUIRE_EQ(decoded.value().header.payload_crc32c, crc32c(payload));
  REQUIRE_EQ(decoded.value().consumed, golden.size());
  REQUIRE(same_bytes(decoded.value().payload, payload));
}

SHUFFLE_TEST(protocol_frames, header_span_must_be_exactly_one_header) {
  const std::vector<std::byte> frame = valid_frame();
  const std::span<const std::byte> bytes{frame};

  REQUIRE_ERROR(decode_header(bytes.first(kFrameHeaderBytes - 1), Limits{}), ErrorCode::TruncatedInput);
  REQUIRE_ERROR(decode_header(bytes.first(kFrameHeaderBytes + 1), Limits{}), ErrorCode::TrailingGarbage);
  const auto header = decode_header(bytes.first(kFrameHeaderBytes), Limits{});
  REQUIRE_OK(header);
  REQUIRE_EQ(header.value().type, MessageType::ProgressRequest);
  REQUIRE_EQ(header.value().sequence, 7u);
  REQUIRE_EQ(header.value().session, SessionId{0x1122334455667788ull});
}

// ---------------------------------------------------------------------------
// Round trips
// ---------------------------------------------------------------------------

SHUFFLE_TEST(protocol_frames, every_message_type_round_trips_with_its_payload) {
  REQUIRE_EQ(kMessageTypes.size(), 32u);

  const std::vector<std::byte> payload = bytes_of("typed payload 0123456789");
  std::size_t requests = 0;
  std::size_t responses = 0;

  for (std::size_t index = 0; index < kMessageTypes.size(); ++index) {
    const MessageType type = kMessageTypes[index];
    REQUIRE(is_known_message_type(type));
    REQUIRE(is_request(type) != is_response(type));
    requests += is_request(type) ? 1u : 0u;
    responses += is_response(type) ? 1u : 0u;

    FrameHeader header;
    header.type = type;
    header.flags = is_response(type) ? kFlagResponse : 0u;
    header.session = SessionId{index + 1};
    header.sequence = index + 1;

    std::vector<std::byte> encoded;
    REQUIRE_OK(encode_frame(header, payload, encoded, Limits{}));
    REQUIRE_EQ(encoded.size(), kFrameHeaderBytes + payload.size());

    const auto decoded = decode_frame(encoded, Limits{});
    REQUIRE_OK(decoded);
    REQUIRE_EQ(decoded.value().header.type, type);
    REQUIRE_EQ(decoded.value().header.session, header.session);
    REQUIRE_EQ(decoded.value().header.sequence, header.sequence);
    REQUIRE_EQ(decoded.value().header.payload_length, static_cast<std::uint32_t>(payload.size()));
    REQUIRE(same_bytes(decoded.value().payload, payload));

    // Decoding and re-encoding is the identity on the wire bytes.
    std::vector<std::byte> re_encoded;
    REQUIRE_OK(encode_frame(decoded.value().header, decoded.value().payload, re_encoded, Limits{}));
    REQUIRE(same_bytes(re_encoded, encoded));
  }
  REQUIRE_EQ(requests, 15u);
  REQUIRE_EQ(responses, 17u);
  REQUIRE_EQ(requests + responses, kMessageTypes.size());
}

SHUFFLE_TEST(protocol_frames, message_type_names_and_classification_are_stable) {
  REQUIRE_EQ(std::string{to_string(MessageType::HandshakeRequest)}, std::string{"HandshakeRequest"});
  REQUIRE_EQ(std::string{to_string(MessageType::ErrorResponse)}, std::string{"ErrorResponse"});
  REQUIRE_EQ(std::string{to_string(MessageType::ExplainResponse)}, std::string{"ExplainResponse"});
  REQUIRE_EQ(std::string{to_string(MessageType::CloseResponse)}, std::string{"CloseResponse"});
  REQUIRE_EQ(std::string{to_string(MessageType::ChunkFetchFailure)}, std::string{"ChunkFetchFailure"});
  REQUIRE_EQ(std::string{to_string(static_cast<MessageType>(4))}, std::string{"Unknown"});
  REQUIRE_EQ(std::string{to_string(static_cast<MessageType>(36))}, std::string{"Unknown"});
  REQUIRE_EQ(std::string{to_string(static_cast<MessageType>(0xffff))}, std::string{"Unknown"});

  // The assigned numbers are exactly 1..3, 10..35 and 40..42; 4..9, 36..39 and
  // everything from 43 up are unassigned and must never be accepted.
  for (std::uint16_t raw = 0; raw <= 45; ++raw) {
    const bool known = raw == 1 || raw == 2 || raw == 3 || (raw >= 10 && raw <= 35) || (raw >= 40 && raw <= 42);
    REQUIRE_EQ(is_known_message_type(static_cast<MessageType>(raw)), known);
  }
  REQUIRE_FALSE(is_known_message_type(static_cast<MessageType>(0xffff)));
  REQUIRE_FALSE(is_request(MessageType::HandshakeResponse));
  REQUIRE_FALSE(is_response(MessageType::HandshakeRequest));
  REQUIRE(is_response(MessageType::ErrorResponse));
  REQUIRE(is_request(MessageType::CloseRequest));
  REQUIRE_FALSE(is_request(static_cast<MessageType>(9)));
  REQUIRE_FALSE(is_response(static_cast<MessageType>(9)));
}

SHUFFLE_TEST(protocol_frames, out_of_range_message_type_is_refused) {
  const std::vector<std::byte> payload = bytes_of("x");
  const std::uint16_t refused[] = {0, 4, 5, 9, 36, 37, 39, 43, 44, 1000, 0xffff};
  for (const std::uint16_t raw : refused) {
    HeaderFields fields;
    fields.type = raw;
    REQUIRE_ERROR(decode_frame(make_frame(fields, payload), Limits{}), ErrorCode::FrameTypeUnsupported);
  }

  const std::uint16_t accepted[] = {1, 2, 3, 10, 20, 30, 31, 32, 33, 35, 40, 42};
  for (const std::uint16_t raw : accepted) {
    HeaderFields fields;
    fields.type = raw;
    const auto decoded = decode_frame(make_frame(fields, payload), Limits{});
    REQUIRE_OK(decoded);
    REQUIRE_EQ(static_cast<std::uint16_t>(decoded.value().header.type), raw);
  }
}

SHUFFLE_TEST(protocol_frames, added_message_types_are_stable_and_round_trip) {
  struct Expected {
    MessageType type;
    std::uint16_t number;
    const char* name;
    bool response;
  };
  const Expected added[] = {
      {MessageType::ManifestRequest, 32, "ManifestRequest", false},
      {MessageType::ManifestResponse, 33, "ManifestResponse", true},
      {MessageType::StatusRequest, 34, "StatusRequest", false},
      {MessageType::StatusResponse, 35, "StatusResponse", true},
      {MessageType::ChunkFetchRequest, 40, "ChunkFetchRequest", false},
      {MessageType::ChunkFetchResponse, 41, "ChunkFetchResponse", true},
      {MessageType::ChunkFetchFailure, 42, "ChunkFetchFailure", true},
  };

  // The canonical list is strictly increasing and has no repeats, so a message
  // number identifies exactly one entry.
  for (std::size_t slot = 1; slot < kMessageTypes.size(); ++slot) {
    REQUIRE(static_cast<std::uint16_t>(kMessageTypes[slot - 1]) <
            static_cast<std::uint16_t>(kMessageTypes[slot]));
    REQUIRE(is_known_message_type(kMessageTypes[slot]));
  }
  REQUIRE_EQ(kMessageTypes.size(), 32u);

  std::size_t index = 0;
  for (const Expected& expected : added) {
    // The wire number is the contract: it is asserted literally and never moves.
    REQUIRE_EQ(static_cast<std::uint16_t>(expected.type), expected.number);
    REQUIRE_EQ(std::string{to_string(expected.type)}, std::string{expected.name});
    REQUIRE(is_known_message_type(expected.type));
    REQUIRE_EQ(is_response(expected.type), expected.response);
    REQUIRE_EQ(is_request(expected.type), !expected.response);

    std::size_t occurrences = 0;
    for (const MessageType listed : kMessageTypes) {
      if (listed == expected.type) {
        ++occurrences;
      }
    }
    REQUIRE_EQ(occurrences, 1u);

    const std::vector<std::byte> payload = bytes_of(std::string(3 + index, 'm'));
    FrameHeader header;
    header.type = expected.type;
    header.flags = expected.response ? kFlagResponse : 0u;
    header.session = SessionId{0x2000 + index};
    header.sequence = index + 1;

    std::vector<std::byte> encoded;
    REQUIRE_OK(encode_frame(header, payload, encoded, Limits{}));
    const auto decoded = decode_frame(encoded, Limits{});
    REQUIRE_OK(decoded);
    REQUIRE_EQ(decoded.value().header.type, expected.type);
    REQUIRE_EQ(decoded.value().header.session, header.session);
    REQUIRE_EQ(decoded.value().header.sequence, index + 1);
    REQUIRE(same_bytes(decoded.value().payload, payload));

    // The same value written by hand decodes too, so acceptance comes from the
    // number on the wire rather than from the encoder agreeing with itself.
    HeaderFields raw;
    raw.type = expected.number;
    raw.sequence = 1;
    const auto from_raw = decode_frame(make_frame(raw, payload), Limits{});
    REQUIRE_OK(from_raw);
    REQUIRE_EQ(from_raw.value().header.type, expected.type);

    // The unassigned number just past this one stays refused.
    HeaderFields neighbour;
    neighbour.type = static_cast<std::uint16_t>(expected.number + 1);
    if (!is_known_message_type(static_cast<MessageType>(neighbour.type))) {
      REQUIRE_ERROR(decode_frame(make_frame(neighbour, payload), Limits{}), ErrorCode::FrameTypeUnsupported);
    }
    ++index;
  }
  REQUIRE_EQ(index, 7u);
}

// ---------------------------------------------------------------------------
// Structure, version and endianness
// ---------------------------------------------------------------------------

SHUFFLE_TEST(protocol_frames, wrong_magic_is_a_protocol_violation) {
  const std::vector<std::byte> frame = valid_frame();

  std::vector<std::byte> first_byte_wrong = frame;
  first_byte_wrong[0] = static_cast<std::byte>('X');
  REQUIRE_ERROR(decode_frame(first_byte_wrong, Limits{}), ErrorCode::ProtocolViolation);

  // A repaired header CRC must not help: the magic is judged before integrity.
  std::vector<std::byte> repaired = first_byte_wrong;
  put_u32_at(repaired, 36, crc32c(header_prefix(repaired)));
  REQUIRE_ERROR(decode_frame(repaired, Limits{}), ErrorCode::ProtocolViolation);

  // Byte-reversed magic, with a correct CRC over it.
  std::vector<std::byte> reversed = frame;
  reversed[0] = static_cast<std::byte>('1');
  reversed[1] = static_cast<std::byte>('B');
  reversed[2] = static_cast<std::byte>('F');
  reversed[3] = static_cast<std::byte>('S');
  put_u32_at(reversed, 36, crc32c(header_prefix(reversed)));
  REQUIRE_ERROR(decode_frame(reversed, Limits{}), ErrorCode::ProtocolViolation);
}

SHUFFLE_TEST(protocol_frames, wrong_version_is_refused) {
  const std::vector<std::byte> payload = bytes_of("v");
  const std::uint16_t versions[] = {0, 2, 3, 0x0100, 0xffff};
  for (const std::uint16_t version : versions) {
    HeaderFields fields;
    fields.version = version;
    REQUIRE_ERROR(decode_frame(make_frame(fields, payload), Limits{}), ErrorCode::UnsupportedVersion);
  }

  HeaderFields supported;
  supported.version = kProtocolVersion;
  REQUIRE_OK(decode_frame(make_frame(supported, payload), Limits{}));

  // The version is judged before the integrity gate, so a big-endian version
  // field is refused as an unsupported version and never silently accepted.
  std::vector<std::byte> big_endian_version = make_frame(supported, payload);
  big_endian_version[4] = std::byte{0x00};
  big_endian_version[5] = std::byte{0x01};
  REQUIRE_ERROR(decode_frame(big_endian_version, Limits{}), ErrorCode::UnsupportedVersion);
}

SHUFFLE_TEST(protocol_frames, a_big_endian_frame_is_refused) {
  const std::vector<std::byte> payload = bytes_of("be");
  HeaderFields fields;
  fields.type = static_cast<std::uint16_t>(MessageType::ProgressRequest);
  fields.sequence = 1;

  // The whole header written most-significant byte first: the magic alone
  // already fails, because version 1 is little-endian and nothing else.
  std::vector<std::byte> big_endian = make_frame(fields, payload);
  std::reverse(big_endian.begin(), big_endian.end() - static_cast<std::ptrdiff_t>(payload.size()));
  REQUIRE_ERROR(decode_frame(big_endian, Limits{}), ErrorCode::ProtocolViolation);

  // With the magic intact but the length field big-endian, the declared length
  // becomes huge and is refused by the bound instead of being misread as small.
  std::vector<std::byte> big_endian_length = make_frame(fields, payload);
  big_endian_length[28] = std::byte{0x00};
  big_endian_length[29] = std::byte{0x00};
  big_endian_length[30] = std::byte{0x00};
  big_endian_length[31] = std::byte{0x02};
  put_u32_at(big_endian_length, 36, crc32c(header_prefix(big_endian_length)));
  REQUIRE_EQ(u32_at(big_endian_length, 28), 0x02000000u);
  REQUIRE_ERROR(decode_frame(big_endian_length, Limits{}), ErrorCode::OversizedInput);

  // The little-endian reading of the same two-byte length is accepted.
  std::vector<std::byte> little_endian = make_frame(fields, payload);
  REQUIRE_EQ(u32_at(little_endian, 28), 2u);
  REQUIRE_OK(decode_frame(little_endian, Limits{}));
}

SHUFFLE_TEST(protocol_frames, reserved_flag_bits_are_refused) {
  const std::vector<std::byte> payload = bytes_of("flags");
  const std::uint32_t reserved[] = {2u, 4u, 8u, 0x80000000u, 0xffffffffu, 0xfffffffeu};
  for (const std::uint32_t flags : reserved) {
    HeaderFields fields;
    fields.flags = flags;
    REQUIRE_ERROR(decode_frame(make_frame(fields, payload), Limits{}), ErrorCode::MalformedInput);
  }

  HeaderFields response;
  response.flags = kFlagResponse;
  const auto decoded = decode_frame(make_frame(response, payload), Limits{});
  REQUIRE_OK(decoded);
  REQUIRE(decoded.value().header.carries_response_flag());
  REQUIRE_EQ(decoded.value().header.flags, kFlagResponse);

  // The reserved mask keeps bit 0 and rejects every other bit.
  REQUIRE_EQ(kFlagReservedMask & kFlagResponse, 0u);
  REQUIRE_EQ(kFlagReservedMask | kFlagResponse, 0xffffffffu);
}

SHUFFLE_TEST(protocol_frames, sequence_zero_is_refused) {
  const std::vector<std::byte> payload = bytes_of("seq");
  HeaderFields fields;
  fields.sequence = 0;
  REQUIRE_ERROR(decode_frame(make_frame(fields, payload), Limits{}), ErrorCode::SequenceViolation);

  FrameHeader header;
  header.type = MessageType::ProgressRequest;
  header.sequence = 0;
  std::vector<std::byte> encoded;
  REQUIRE_ERROR(encode_frame(header, payload, encoded, Limits{}), ErrorCode::SequenceViolation);
  REQUIRE(encoded.empty());

  fields.sequence = 1;
  REQUIRE_OK(decode_frame(make_frame(fields, payload), Limits{}));
}

// ---------------------------------------------------------------------------
// Integrity
// ---------------------------------------------------------------------------

SHUFFLE_TEST(protocol_frames, payload_crc_mismatch_is_detected) {
  const std::vector<std::byte> frame = valid_frame();
  std::vector<std::byte> damaged = frame;
  damaged[kFrameHeaderBytes] = static_cast<std::byte>(byte_at(damaged, kFrameHeaderBytes) ^ 0xffu);
  REQUIRE_ERROR(decode_frame(damaged, Limits{}), ErrorCode::ChecksumMismatch);

  // A payload CRC field that simply disagrees with the payload is refused too.
  HeaderFields wrong_crc;
  wrong_crc.payload_length = 4;
  wrong_crc.payload_crc32c = 0x12345678u;
  REQUIRE_ERROR(decode_frame(make_frame(wrong_crc, bytes_of("body"), true, false), Limits{}),
                ErrorCode::ChecksumMismatch);

  // The honest CRC of the very same payload is accepted, so the check above is
  // not simply refusing every frame.
  HeaderFields honest;
  honest.payload_length = 4;
  honest.payload_crc32c = crc32c(bytes_of("body"));
  REQUIRE_OK(decode_frame(make_frame(honest, bytes_of("body"), true, false), Limits{}));
}

SHUFFLE_TEST(protocol_frames, header_crc_mismatch_is_detected) {
  const std::vector<std::byte> frame = valid_frame();

  std::vector<std::byte> sequence_flipped = frame;
  sequence_flipped[20] = static_cast<std::byte>(byte_at(sequence_flipped, 20) ^ 0x01u);
  REQUIRE_ERROR(decode_frame(sequence_flipped, Limits{}), ErrorCode::ChecksumMismatch);

  std::vector<std::byte> crc_flipped = frame;
  crc_flipped[36] = static_cast<std::byte>(byte_at(crc_flipped, 36) ^ 0x80u);
  REQUIRE_ERROR(decode_frame(crc_flipped, Limits{}), ErrorCode::ChecksumMismatch);

  // A reserved flag bit with a stale CRC is an integrity failure, not a
  // semantic one: unverified fields are never interpreted.
  std::vector<std::byte> flags_flipped = frame;
  flags_flipped[8] = std::byte{0x02};
  REQUIRE_ERROR(decode_frame(flags_flipped, Limits{}), ErrorCode::ChecksumMismatch);
}

// ---------------------------------------------------------------------------
// Bounds and truncation
// ---------------------------------------------------------------------------

SHUFFLE_TEST(protocol_frames, declared_length_is_bounded_before_any_allocation) {
  const Limits limits = frame_limits(64);

  // Exactly at the bound is accepted.
  const std::vector<std::byte> body(64, std::byte{0x5a});
  HeaderFields at_bound;
  at_bound.payload_length = 64;
  at_bound.payload_crc32c = crc32c(body);
  const std::vector<std::byte> at_bound_frame = make_frame(at_bound, body, true, false);
  const auto decoded = decode_frame(at_bound_frame, limits);
  REQUIRE_OK(decoded);
  REQUIRE_EQ(decoded.value().payload.size(), 64u);
  REQUIRE(same_bytes(decoded.value().payload, body));

  // One byte past the bound is refused from the header alone.
  HeaderFields over_bound;
  over_bound.payload_length = 65;
  const std::vector<std::byte> over_bound_header = make_header(over_bound, true);
  REQUIRE_EQ(over_bound_header.size(), kFrameHeaderBytes);
  REQUIRE_ERROR(decode_frame_prefix(over_bound_header, limits), ErrorCode::OversizedInput);

  // The extreme declared length is refused while only 40 bytes are held:
  // nothing was reserved on the strength of the field.
  HeaderFields absurd;
  absurd.payload_length = 0xffffffffu;
  const std::vector<std::byte> absurd_header = make_header(absurd, true);
  REQUIRE_EQ(absurd_header.size(), kFrameHeaderBytes);
  REQUIRE_ERROR(decode_frame_prefix(absurd_header, limits), ErrorCode::OversizedInput);

  // Encoding refuses the same way and leaves the caller's buffer untouched.
  FrameHeader header;
  header.type = MessageType::ProgressRequest;
  header.sequence = 1;
  std::vector<std::byte> out = bytes_of("untouched");
  REQUIRE_ERROR(encode_frame(header, std::vector<std::byte>(65, std::byte{0x78}), out, limits),
                ErrorCode::OversizedInput);
  REQUIRE_EQ(out.size(), 9u);
  REQUIRE(same_bytes(out, bytes_of("untouched")));
}

SHUFFLE_TEST(protocol_frames, every_truncated_prefix_is_refused) {
  const std::vector<std::byte> frame = valid_frame(MessageType::NextWaveResponse, "truncate me please");
  const std::span<const std::byte> bytes{frame};
  REQUIRE(frame.size() > kFrameHeaderBytes);

  for (std::size_t length = 0; length < frame.size(); ++length) {
    REQUIRE_ERROR(decode_frame(bytes.first(length), Limits{}), ErrorCode::TruncatedInput);
    REQUIRE_ERROR(decode_frame_prefix(bytes.first(length), Limits{}), ErrorCode::TruncatedInput);
  }

  const auto decoded = decode_frame(bytes, Limits{});
  REQUIRE_OK(decoded);
  REQUIRE_EQ(decoded.value().consumed, frame.size());
  REQUIRE(same_bytes(decoded.value().payload, bytes_of("truncate me please")));
}

SHUFFLE_TEST(protocol_frames, trailing_garbage_is_refused_by_exact_decoding) {
  const std::vector<std::byte> frame = valid_frame();
  const std::vector<std::byte> second_frame = valid_frame(MessageType::CloseRequest, "second");

  std::vector<std::byte> with_tail = frame;
  with_tail.push_back(std::byte{0x00});
  REQUIRE_ERROR(decode_frame(with_tail, Limits{}), ErrorCode::TrailingGarbage);

  std::vector<std::byte> doubled = frame;
  doubled.insert(doubled.end(), second_frame.begin(), second_frame.end());
  REQUIRE_ERROR(decode_frame(doubled, Limits{}), ErrorCode::TrailingGarbage);

  // The prefix decoder reports exactly one frame and how long it was.
  const auto first = decode_frame_prefix(doubled, Limits{});
  REQUIRE_OK(first);
  REQUIRE_EQ(first.value().consumed, frame.size());
  REQUIRE_EQ(first.value().header.type, MessageType::ProgressRequest);
  REQUIRE(same_bytes(first.value().payload, std::span<const std::byte>(frame).subspan(kFrameHeaderBytes)));

  const auto second =
      decode_frame_prefix(std::span<const std::byte>(doubled).subspan(first.value().consumed), Limits{});
  REQUIRE_OK(second);
  REQUIRE_EQ(second.value().header.type, MessageType::CloseRequest);
  REQUIRE_EQ(second.value().consumed, second_frame.size());
  REQUIRE(same_bytes(second.value().payload, bytes_of("second")));
}

SHUFFLE_TEST(protocol_frames, encoder_refusals_leave_the_output_untouched) {
  const std::vector<std::byte> payload = bytes_of("payload");
  const std::vector<std::byte> sentinel = bytes_of("sentinel-bytes");
  const Limits limits = frame_limits(4);  // the payload is longer than the bound

  FrameHeader header;
  header.type = MessageType::ProgressRequest;
  header.sequence = 3;

  std::vector<std::byte> out = sentinel;
  REQUIRE_ERROR(encode_frame(header, payload, out, limits), ErrorCode::OversizedInput);
  REQUIRE(same_bytes(out, sentinel));

  FrameHeader bad_version = header;
  bad_version.version = 2;
  REQUIRE_ERROR(encode_frame(bad_version, payload, out, Limits{}), ErrorCode::UnsupportedVersion);
  REQUIRE(same_bytes(out, sentinel));

  FrameHeader bad_type = header;
  bad_type.type = static_cast<MessageType>(9);
  REQUIRE_ERROR(encode_frame(bad_type, payload, out, Limits{}), ErrorCode::FrameTypeUnsupported);
  REQUIRE(same_bytes(out, sentinel));

  FrameHeader bad_flags = header;
  bad_flags.flags = 0x00000002u;
  REQUIRE_ERROR(encode_frame(bad_flags, payload, out, Limits{}), ErrorCode::MalformedInput);
  REQUIRE(same_bytes(out, sentinel));

  FrameHeader lying_length = header;
  lying_length.payload_length = 99;
  REQUIRE_ERROR(encode_frame(lying_length, payload, out, Limits{}), ErrorCode::InvalidArgument);
  REQUIRE(same_bytes(out, sentinel));

  FrameHeader lying_crc = header;
  lying_crc.payload_crc32c = 0xdeadbeefu;
  REQUIRE_ERROR(encode_frame(lying_crc, payload, out, Limits{}), ErrorCode::InvalidArgument);
  REQUIRE(same_bytes(out, sentinel));

  // A truthful length and CRC are accepted, so the refusals above are not
  // simply refusing everything.
  FrameHeader truthful = header;
  truthful.payload_length = static_cast<std::uint32_t>(payload.size());
  truthful.payload_crc32c = crc32c(payload);
  REQUIRE_OK(encode_frame(truthful, payload, out, Limits{}));
  REQUIRE_EQ(out.size(), kFrameHeaderBytes + payload.size());
  REQUIRE(same_bytes(out, make_frame([] {
            HeaderFields fields;
            fields.type = static_cast<std::uint16_t>(MessageType::ProgressRequest);
            fields.sequence = 3;
            return fields;
          }(),
          payload)));
}

SHUFFLE_TEST(protocol_frames, empty_payload_is_a_complete_frame) {
  FrameHeader header;
  header.type = MessageType::CloseRequest;
  header.sequence = 1;

  std::vector<std::byte> encoded;
  REQUIRE_OK(encode_frame(header, {}, encoded, Limits{}));
  REQUIRE_EQ(encoded.size(), kFrameHeaderBytes);
  REQUIRE_EQ(u32_at(encoded, 28), 0u);
  REQUIRE_EQ(u32_at(encoded, 32), crc32c(std::span<const std::byte>{}));

  const auto decoded = decode_frame(encoded, Limits{});
  REQUIRE_OK(decoded);
  REQUIRE(decoded.value().payload.empty());
  REQUIRE_EQ(decoded.value().consumed, kFrameHeaderBytes);
  REQUIRE_ERROR(decode_frame(std::span<const std::byte>(encoded).first(kFrameHeaderBytes - 1), Limits{}),
                ErrorCode::TruncatedInput);
}

// ---------------------------------------------------------------------------
// FrameStream
// ---------------------------------------------------------------------------

SHUFFLE_TEST(protocol_frames, frame_stream_accepts_one_byte_at_a_time) {
  const std::vector<std::byte> frame = valid_frame(MessageType::PublishManifestRequest, "byte at a time");
  const Limits limits{};

  FrameStream stream{limits};
  REQUIRE_EQ(stream.buffered(), 0u);

  for (std::size_t index = 0; index < frame.size(); ++index) {
    REQUIRE_OK(stream.feed(std::span<const std::byte>(frame).subspan(index, 1)));
    REQUIRE_EQ(stream.buffered(), index + 1);
    const auto pending = stream.next();
    if (index + 1 < frame.size()) {
      REQUIRE_ERROR(pending, ErrorCode::NoWorkAvailable);
    } else {
      REQUIRE_OK(pending);
      REQUIRE_EQ(pending.value().consumed, frame.size());
      REQUIRE_EQ(pending.value().header.type, MessageType::PublishManifestRequest);
      REQUIRE(same_bytes(pending.value().payload, std::span<const std::byte>(frame).subspan(kFrameHeaderBytes)));
    }
  }
  REQUIRE_EQ(stream.buffered(), 0u);
  REQUIRE_ERROR(stream.next(), ErrorCode::NoWorkAvailable);

  // The same frame delivered whole produces the same frame.
  FrameStream whole{limits};
  REQUIRE_OK(whole.feed(frame));
  const auto decoded = whole.next();
  REQUIRE_OK(decoded);
  REQUIRE_EQ(decoded.value().consumed, frame.size());
  REQUIRE_EQ(decoded.value().header.type, MessageType::PublishManifestRequest);
  REQUIRE(same_bytes(decoded.value().payload, bytes_of("byte at a time")));
  REQUIRE_EQ(whole.buffered(), 0u);
}

SHUFFLE_TEST(protocol_frames, frame_stream_refuses_a_prefix_that_cannot_become_a_frame) {
  const Limits limits{};

  FrameStream stream{limits};
  REQUIRE_OK(stream.feed(bytes_of("X")));
  REQUIRE_EQ(stream.buffered(), 1u);
  REQUIRE_ERROR(stream.next(), ErrorCode::ProtocolViolation);

  // A correct prefix stays acceptable until it is complete.
  FrameStream partial{limits};
  REQUIRE_OK(partial.feed(bytes_of("SFB")));
  REQUIRE_ERROR(partial.next(), ErrorCode::NoWorkAvailable);
  REQUIRE_EQ(partial.buffered(), 3u);
  REQUIRE_OK(partial.feed(bytes_of("1")));
  REQUIRE_ERROR(partial.next(), ErrorCode::NoWorkAvailable);
  REQUIRE_EQ(partial.buffered(), 4u);

  // Feeding nothing changes nothing.
  REQUIRE_OK(partial.feed(std::span<const std::byte>{}));
  REQUIRE_EQ(partial.buffered(), 4u);
}

SHUFFLE_TEST(protocol_frames, frame_stream_decodes_frames_back_to_back) {
  const std::vector<std::byte> first = valid_frame(MessageType::NextWaveRequest, "first frame");
  const std::vector<std::byte> second = valid_frame(MessageType::ProgressResponse, "second frame");

  std::vector<std::byte> both = first;
  both.insert(both.end(), second.begin(), second.end());

  FrameStream stream{Limits{}};
  REQUIRE_OK(stream.feed(both));
  REQUIRE_EQ(stream.buffered(), both.size());

  const auto one = stream.next();
  REQUIRE_OK(one);
  REQUIRE_EQ(one.value().header.type, MessageType::NextWaveRequest);
  REQUIRE(same_bytes(one.value().payload, bytes_of("first frame")));
  REQUIRE_EQ(stream.buffered(), second.size());

  const auto two = stream.next();
  REQUIRE_OK(two);
  REQUIRE_EQ(two.value().header.type, MessageType::ProgressResponse);
  REQUIRE(same_bytes(two.value().payload, bytes_of("second frame")));
  REQUIRE_EQ(stream.buffered(), 0u);
  REQUIRE_ERROR(stream.next(), ErrorCode::NoWorkAvailable);

  // Draining the first frame while the second is only half delivered must not
  // lose the partial frame.
  FrameStream split{Limits{}};
  REQUIRE_OK(split.feed(first));
  const std::size_t head = second.size() / 2;
  REQUIRE_OK(split.feed(std::span<const std::byte>(second).first(head)));
  const auto split_first = split.next();
  REQUIRE_OK(split_first);
  REQUIRE_EQ(split_first.value().header.type, MessageType::NextWaveRequest);
  REQUIRE_ERROR(split.next(), ErrorCode::NoWorkAvailable);
  REQUIRE_EQ(split.buffered(), head);
  REQUIRE_OK(split.feed(std::span<const std::byte>(second).subspan(head)));
  const auto split_second = split.next();
  REQUIRE_OK(split_second);
  REQUIRE_EQ(split_second.value().header.type, MessageType::ProgressResponse);
  REQUIRE(same_bytes(split_second.value().payload, bytes_of("second frame")));
  REQUIRE_EQ(split.buffered(), 0u);
}

SHUFFLE_TEST(protocol_frames, frame_stream_refuses_an_oversized_declared_length_without_the_payload) {
  const Limits limits = frame_limits(64);
  HeaderFields fields;
  fields.type = static_cast<std::uint16_t>(MessageType::ProgressRequest);
  fields.sequence = 1;
  fields.payload_length = 65;
  const std::vector<std::byte> header = make_header(fields, true);
  REQUIRE_EQ(header.size(), kFrameHeaderBytes);

  FrameStream stream{limits};
  REQUIRE_OK(stream.feed(header));
  REQUIRE_EQ(stream.buffered(), kFrameHeaderBytes);
  REQUIRE_ERROR(stream.next(), ErrorCode::OversizedInput);

  // The queue itself is bounded by max_frame_payload_bytes + kFrameHeaderBytes,
  // so a peer cannot make the reader grow past it.
  FrameStream bounded{limits};
  REQUIRE_OK(bounded.feed(header));
  REQUIRE_ERROR(bounded.feed(std::vector<std::byte>(65, std::byte{0x7f})), ErrorCode::ResourceExhausted);
  REQUIRE_EQ(bounded.buffered(), kFrameHeaderBytes);

  // A frame that fits the bound is read normally, so the bound is not refusing
  // frames it should accept.
  const std::vector<std::byte> body(64, std::byte{0x11});
  HeaderFields exact;
  exact.type = static_cast<std::uint16_t>(MessageType::ProgressRequest);
  exact.sequence = 1;
  exact.payload_length = 64;
  exact.payload_crc32c = crc32c(body);
  const std::vector<std::byte> frame = make_frame(exact, body, true, false);

  FrameStream fitting{limits};
  REQUIRE_OK(fitting.feed(frame));
  const auto decoded = fitting.next();
  REQUIRE_OK(decoded);
  REQUIRE_EQ(decoded.value().payload.size(), 64u);
  REQUIRE(same_bytes(decoded.value().payload, body));
  REQUIRE_EQ(fitting.buffered(), 0u);
}

// ---------------------------------------------------------------------------
// ReplayGuard
// ---------------------------------------------------------------------------

SHUFFLE_TEST(protocol_frames, replay_guard_accepts_in_order_and_reports_duplicates) {
  ReplayGuard guard{8};
  REQUIRE_EQ(guard.highest(), 0u);

  for (std::uint64_t sequence = 1; sequence <= 8; ++sequence) {
    const auto accepted = guard.accept(sequence);
    REQUIRE_OK(accepted);
    REQUIRE(accepted.value());
    REQUIRE_EQ(guard.highest(), sequence);
  }

  for (std::uint64_t sequence = 1; sequence <= 8; ++sequence) {
    const auto duplicate = guard.accept(sequence);
    REQUIRE_OK(duplicate);
    REQUIRE_FALSE(duplicate.value());
  }
  REQUIRE_EQ(guard.highest(), 8u);
  REQUIRE_EQ(guard.retained(), 8u);
  REQUIRE_EQ(guard.window(), 8u);
}

SHUFFLE_TEST(protocol_frames, replay_guard_handles_out_of_order_within_the_window) {
  ReplayGuard guard{4};
  REQUIRE(guard.accept(1).value());
  REQUIRE(guard.accept(3).value());
  REQUIRE(guard.accept(5).value());
  REQUIRE_EQ(guard.highest(), 5u);

  // 2 and 4 were never seen but are still inside the window: they are new.
  const auto two = guard.accept(2);
  REQUIRE_OK(two);
  REQUIRE(two.value());
  const auto four = guard.accept(4);
  REQUIRE_OK(four);
  REQUIRE(four.value());
  REQUIRE_EQ(guard.highest(), 5u);

  // Having been accepted once, they are duplicates now.
  const auto two_again = guard.accept(2);
  REQUIRE_OK(two_again);
  REQUIRE_FALSE(two_again.value());
  const auto four_again = guard.accept(4);
  REQUIRE_OK(four_again);
  REQUIRE_FALSE(four_again.value());
  REQUIRE_EQ(guard.highest(), 5u);

  // A later high-water mark keeps working.
  const auto six = guard.accept(6);
  REQUIRE_OK(six);
  REQUIRE(six.value());
  REQUIRE_EQ(guard.highest(), 6u);
}

SHUFFLE_TEST(protocol_frames, replay_guard_refuses_sequences_older_than_the_window) {
  ReplayGuard guard{4};
  for (std::uint64_t sequence = 1; sequence <= 16; ++sequence) {
    const auto accepted = guard.accept(sequence);
    REQUIRE_OK(accepted);
    REQUIRE(accepted.value());
  }
  REQUIRE_EQ(guard.highest(), 16u);
  REQUIRE_EQ(guard.retained(), 4u);

  // 13..16 are retained; anything older has fallen out of the window and can no
  // longer be told apart from a replay.
  REQUIRE_FALSE(guard.accept(16).value());
  REQUIRE_FALSE(guard.accept(13).value());
  REQUIRE_ERROR(guard.accept(12), ErrorCode::SequenceViolation);
  REQUIRE_ERROR(guard.accept(1), ErrorCode::SequenceViolation);
  REQUIRE_ERROR(guard.accept(0), ErrorCode::SequenceViolation);
  REQUIRE_EQ(guard.highest(), 16u);
}

SHUFFLE_TEST(protocol_frames, replay_guard_refuses_absurd_jumps) {
  ReplayGuard guard{16};
  REQUIRE(guard.accept(1).value());

  const auto absurd = guard.accept(1 + ReplayGuard::kMaxForwardJump + 1);
  REQUIRE_ERROR(absurd, ErrorCode::MalformedInput);
  REQUIRE_EQ(guard.highest(), 1u);  // a refused sequence never moves the mark

  const auto exact = guard.accept(1 + ReplayGuard::kMaxForwardJump);
  REQUIRE_OK(exact);
  REQUIRE(exact.value());
  REQUIRE_EQ(guard.highest(), 1u + ReplayGuard::kMaxForwardJump);

  // A guard that has accepted nothing measures the jump from zero.
  ReplayGuard fresh{16};
  REQUIRE_ERROR(fresh.accept(ReplayGuard::kMaxForwardJump + 1), ErrorCode::MalformedInput);
  REQUIRE_EQ(fresh.highest(), 0u);
  const auto first = fresh.accept(ReplayGuard::kMaxForwardJump);
  REQUIRE_OK(first);
  REQUIRE(first.value());
}

SHUFFLE_TEST(protocol_frames, replay_guard_window_is_a_bounded_resource) {
  ReplayGuard clamped{1u << 20};
  REQUIRE_EQ(clamped.window(), ReplayGuard::kMaxReplayWindow);

  ReplayGuard zero{0};
  REQUIRE_EQ(zero.window(), 0u);
  REQUIRE(zero.accept(4).value());
  REQUIRE_FALSE(zero.accept(4).value());  // the high-water mark is always known
  REQUIRE_ERROR(zero.accept(3), ErrorCode::SequenceViolation);
  REQUIRE_EQ(zero.retained(), 0u);

  ReplayGuard small{2};
  REQUIRE(small.accept(10).value());
  REQUIRE(small.accept(20).value());
  REQUIRE(small.accept(30).value());
  REQUIRE_EQ(small.retained(), 2u);
  REQUIRE_FALSE(small.accept(30).value());
  REQUIRE_FALSE(small.accept(20).value());
  REQUIRE_ERROR(small.accept(10), ErrorCode::SequenceViolation);
}

}  // namespace
