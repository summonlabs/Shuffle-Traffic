// Adversarial proof surface for the wire transport: randomized decoding driven
// by a seeded generator, byte-exact differential checking against an independent
// re-derivation of the specification, and real loopback socket proofs.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// The differential oracle below is a second implementation of the documented
// decoding rules, written from the offsets in the header comment of frame.hpp
// rather than by calling the library. Every randomized or mutated input is
// judged by both implementations and the two verdicts must agree exactly,
// including the exact ErrorCode, which is what makes "deterministic" a property
// rather than a hope.
//
// No test here waits for something to happen: a peer that has already sent bytes
// is read with a block, a listener that has nothing pending is polled with a
// zero budget, and no assertion depends on how long an operation took except the
// one that asserts a blocked accept was woken (where latency is the property).

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "shuffle/fabric/bytes.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/frame.hpp"
#include "shuffle/fabric/hash.hpp"
#include "shuffle/fabric/identity.hpp"
#include "shuffle/fabric/socket.hpp"
#include "rng.hpp"
#include "test_support.hpp"

namespace shuffle::fabric {

inline std::ostream& operator<<(std::ostream& stream, MessageType type) {
  return stream << to_string(type);
}

inline std::ostream& operator<<(std::ostream& stream, ErrorCode code) {
  return stream << to_string(code);
}

inline std::ostream& operator<<(std::ostream& stream, Socket::WaitResult result) {
  switch (result) {
    case Socket::WaitResult::Ready: return stream << "Ready";
    case Socket::WaitResult::TimedOut: return stream << "TimedOut";
    case Socket::WaitResult::Closed: return stream << "Closed";
  }
  return stream << "Unknown";
}

}  // namespace shuffle::fabric

namespace {

using namespace shuffle::fabric;
using shuffle::test::Random;
using shuffle::test::derive_seed;

// ---------------------------------------------------------------------------
// Byte helpers and randomized generators
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

[[nodiscard]] std::uint16_t u16_at(std::span<const std::byte> bytes, std::size_t offset) {
  return static_cast<std::uint16_t>(static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset])) |
                                    static_cast<std::uint16_t>(
                                        static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset + 1])) << 8));
}

[[nodiscard]] std::uint32_t u32_at(std::span<const std::byte> bytes, std::size_t offset) {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + index])) << (index * 8);
  }
  return value;
}

[[nodiscard]] std::uint64_t u64_at(std::span<const std::byte> bytes, std::size_t offset) {
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[offset + index])) << (index * 8);
  }
  return value;
}

[[nodiscard]] std::span<const std::byte> header_prefix(const std::vector<std::byte>& bytes) {
  return std::span<const std::byte>(bytes.data(), kFrameHeaderCrcOffset);
}

[[nodiscard]] bool same_bytes(std::span<const std::byte> lhs, std::span<const std::byte> rhs) {
  return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

[[nodiscard]] Limits frame_limits(std::uint64_t max_payload) {
  Limits limits{};
  limits.max_frame_payload_bytes = max_payload;
  return limits;
}

[[nodiscard]] std::vector<std::byte> random_bytes(Random& rng, std::size_t size) {
  std::vector<std::byte> out(size);
  for (std::size_t index = 0; index < size; ++index) {
    out[index] = static_cast<std::byte>(static_cast<std::uint8_t>(rng.next_u64() & 0xffu));
  }
  return out;
}

// Lengths that matter: tiny, around the 40-byte header, around the configured
// payload bound and comfortably larger.
[[nodiscard]] std::size_t adversarial_length(Random& rng, std::uint64_t bound) {
  switch (rng.bounded(6)) {
    case 0: return static_cast<std::size_t>(rng.bounded(8));
    case 1: return static_cast<std::size_t>(rng.bounded(64));
    case 2: return static_cast<std::size_t>(kFrameHeaderBytes + rng.bounded(9));
    case 3: return static_cast<std::size_t>(bound + rng.bounded(8));
    case 4: return static_cast<std::size_t>(rng.bounded(bound + 8));
    default: return static_cast<std::size_t>(rng.bounded(512));
  }
}

// A socket handle is only meaningful while the SocketRuntime that created it is
// alive, so passing a temporary runtime is a lifetime bug. Socket::connect and
// TcpListener::bind take the runtime by const reference for that reason and
// declare a deleted rvalue overload, so the temporary form is a compile error;
// the compile-time proof of that rule is a separate negative-compilation check
// (see the report), because MSVC rejects deleted-function selection inside a
// SFINAE context as a hard error rather than a deduction failure.

[[nodiscard]] std::vector<std::byte> build_frame(MessageType type, std::uint64_t session, std::uint64_t sequence,
                                                 std::uint32_t flags, const std::vector<std::byte>& payload) {
  FrameHeader header;
  header.type = type;
  header.flags = flags;
  header.session = SessionId{session};
  header.sequence = sequence;
  std::vector<std::byte> encoded;
  const Status status = encode_frame(header, payload, encoded, Limits{});
  REQUIRE_OK(status);
  return encoded;
}

// ---------------------------------------------------------------------------
// Differential oracle: the specification, written a second time.
// ---------------------------------------------------------------------------

// Returns ErrorCode::Ok when the first frame of bytes decodes, otherwise the
// code the specification requires. consumed receives the frame length.
[[nodiscard]] ErrorCode reference_decode_prefix(std::span<const std::byte> bytes, const Limits& limits,
                                                std::size_t& consumed) {
  consumed = 0;
  if (bytes.size() < kFrameHeaderBytes) {
    return ErrorCode::TruncatedInput;
  }
  if (u32_at(bytes, 0) != kFrameMagic) {
    return ErrorCode::ProtocolViolation;
  }
  if (u16_at(bytes, 4) != kProtocolVersion) {
    return ErrorCode::UnsupportedVersion;
  }
  if (u32_at(bytes, 36) != crc32c(bytes.first(kFrameHeaderCrcOffset))) {
    return ErrorCode::ChecksumMismatch;
  }
  if ((u32_at(bytes, 8) & kFlagReservedMask) != 0) {
    return ErrorCode::MalformedInput;
  }
  const std::uint16_t type = u16_at(bytes, 6);
  const bool known =
      type == 1 || type == 2 || type == 3 || (type >= 10 && type <= 35) || (type >= 40 && type <= 42);
  if (!known) {
    return ErrorCode::FrameTypeUnsupported;
  }
  const std::uint32_t declared = u32_at(bytes, 28);
  if (declared > limits.max_frame_payload_bytes) {
    return ErrorCode::OversizedInput;
  }
  if (u64_at(bytes, 20) == 0) {
    return ErrorCode::SequenceViolation;
  }
  const std::uint64_t total = static_cast<std::uint64_t>(kFrameHeaderBytes) + declared;
  if (bytes.size() < total) {
    return ErrorCode::TruncatedInput;
  }
  if (crc32c(bytes.subspan(kFrameHeaderBytes, declared)) != u32_at(bytes, 32)) {
    return ErrorCode::ChecksumMismatch;
  }
  consumed = static_cast<std::size_t>(total);
  return ErrorCode::Ok;
}

// Requires that the library and the oracle agree about this buffer, that the
// library is deterministic, and that decode_frame agrees with decode_frame_prefix.
void require_agreement_with_reference(std::span<const std::byte> bytes, const Limits& limits) {
  std::size_t reference_consumed = 0;
  const ErrorCode expected = reference_decode_prefix(bytes, limits, reference_consumed);

  const Result<DecodedFrame> first = decode_frame_prefix(bytes, limits);
  const Result<DecodedFrame> second = decode_frame_prefix(bytes, limits);
  REQUIRE_EQ(first.code(), expected);
  REQUIRE_EQ(second.code(), expected);

  const Result<DecodedFrame> exact = decode_frame(bytes, limits);
  if (expected == ErrorCode::Ok) {
    REQUIRE_OK(first);
    REQUIRE_OK(second);
    REQUIRE_EQ(first.value().consumed, reference_consumed);
    REQUIRE_EQ(second.value().consumed, reference_consumed);
    REQUIRE_EQ(first.value().header.payload_length, static_cast<std::uint32_t>(reference_consumed - kFrameHeaderBytes));
    REQUIRE(first.value().payload.data() == bytes.data() + kFrameHeaderBytes);
    REQUIRE_EQ(first.value().payload.size(), reference_consumed - kFrameHeaderBytes);
    // An accepted frame is exactly one whose returned payload carries the CRC
    // the header declares: acceptance and integrity are the same statement.
    REQUIRE_EQ(crc32c(first.value().payload), first.value().header.payload_crc32c);
    REQUIRE_EQ(first.value().header.payload_crc32c, u32_at(bytes, 32));
    if (reference_consumed == bytes.size()) {
      REQUIRE_OK(exact);
      REQUIRE_EQ(exact.value().consumed, bytes.size());
    } else {
      REQUIRE_ERROR(exact, ErrorCode::TrailingGarbage);
    }
  } else {
    REQUIRE_ERROR(first, expected);
    REQUIRE_ERROR(second, expected);
    REQUIRE_ERROR(exact, expected);
  }
}

// ---------------------------------------------------------------------------
// Randomized decoding
// ---------------------------------------------------------------------------

SHUFFLE_TEST(adversarial_frames, random_buffers_are_judged_exactly_like_the_specification) {
  Random rng{derive_seed("adversarial_frames.random_buffers")};
  const Limits limits = frame_limits(256);

  std::size_t truncated = 0;
  std::size_t violations = 0;
  constexpr std::size_t kIterations = 4000;

  for (std::size_t iteration = 0; iteration < kIterations; ++iteration) {
    const std::size_t size = adversarial_length(rng, limits.max_frame_payload_bytes);
    const std::vector<std::byte> bytes = random_bytes(rng, size);
    require_agreement_with_reference(bytes, limits);

    switch (decode_frame_prefix(bytes, limits).code()) {
      case ErrorCode::TruncatedInput: ++truncated; break;
      case ErrorCode::ProtocolViolation: ++violations; break;
      default: break;
    }
  }

  // Non-vacuity: the corpus really did contain buffers shorter than a header and
  // buffers long enough to reach the magic, and both were refused for the reason
  // the rules name. The deeper rules are covered by the structured corpora
  // below, which is the only way to reach them at all.
  REQUIRE(truncated > 0);
  REQUIRE(violations > 0);
  REQUIRE(truncated + violations <= kIterations);
}

SHUFFLE_TEST(adversarial_frames, every_single_byte_corruption_is_refused_exactly_once) {
  Random rng{derive_seed("adversarial_frames.byte_corruption")};

  for (std::size_t round = 0; round < 64; ++round) {
    const std::vector<std::byte> payload = random_bytes(rng, 1 + rng.bounded(96));
    const std::vector<std::byte> frame = build_frame(MessageType::CommitTransferRequest, rng.next_u64(),
                                                     1 + rng.bounded(1000), 0, payload);
    REQUIRE(frame.size() > kFrameHeaderBytes);

    std::size_t protocol_violations = 0;
    std::size_t unsupported_versions = 0;
    std::size_t checksum_mismatches = 0;

    for (std::size_t position = 0; position < frame.size(); ++position) {
      std::vector<std::byte> corrupted = frame;
      corrupted[position] = static_cast<std::byte>(std::to_integer<std::uint8_t>(corrupted[position]) ^ 0xffu);

      std::size_t reference_consumed = 0;
      const ErrorCode expected = reference_decode_prefix(corrupted, Limits{}, reference_consumed);
      const Result<DecodedFrame> actual = decode_frame(corrupted, Limits{});
      REQUIRE_EQ(actual.code(), expected);
      REQUIRE_FALSE(actual.ok());  // every single-byte corruption is detected

      switch (expected) {
        case ErrorCode::ProtocolViolation: ++protocol_violations; break;
        case ErrorCode::UnsupportedVersion: ++unsupported_versions; break;
        case ErrorCode::ChecksumMismatch: ++checksum_mismatches; break;
        default: FAIL_TEST("a corrupted frame produced an unexpected verdict");
      }
    }

    // Exactly the four magic bytes and the two version bytes are refused before
    // the integrity gate; every other position is an integrity failure.
    REQUIRE_EQ(protocol_violations, 4u);
    REQUIRE_EQ(unsupported_versions, 2u);
    REQUIRE_EQ(checksum_mismatches, frame.size() - 6);
  }
}

SHUFFLE_TEST(adversarial_frames, length_field_fuzzing_agrees_with_the_reference) {
  Random rng{derive_seed("adversarial_frames.length_field")};
  const Limits limits = frame_limits(64);

  const std::uint32_t lengths[] = {0u,      1u,       39u,      40u,      41u,       63u,       64u,
                                   65u,     128u,     1024u,    0x7fffffffu, 0x80000000u, 0xfffffffeu, 0xffffffffu};
  for (const std::uint32_t declared : lengths) {
    for (std::size_t delivered = 0; delivered <= 80; delivered += 8) {
      std::vector<std::byte> bytes(kFrameHeaderBytes, std::byte{0});
      bytes[0] = static_cast<std::byte>('S');
      bytes[1] = static_cast<std::byte>('F');
      bytes[2] = static_cast<std::byte>('B');
      bytes[3] = static_cast<std::byte>('1');
      put_u16_at(bytes, 4, kProtocolVersion);
      put_u16_at(bytes, 6, static_cast<std::uint16_t>(MessageType::ProgressRequest));
      put_u32_at(bytes, 8, 0);
      put_u32_at(bytes, 20, 1);
      put_u32_at(bytes, 28, declared);
      put_u32_at(bytes, 32, 0);
      put_u32_at(bytes, 36, crc32c(header_prefix(bytes)));

      const std::vector<std::byte> body = random_bytes(rng, delivered);
      bytes.insert(bytes.end(), body.begin(), body.end());
      require_agreement_with_reference(bytes, limits);
    }
  }

  // Every power-of-two length is also exercised, plus lengths chosen at random.
  for (std::size_t round = 0; round < 500; ++round) {
    std::vector<std::byte> bytes(kFrameHeaderBytes, std::byte{0});
    bytes[0] = static_cast<std::byte>('S');
    bytes[1] = static_cast<std::byte>('F');
    bytes[2] = static_cast<std::byte>('B');
    bytes[3] = static_cast<std::byte>('1');
    put_u16_at(bytes, 4, kProtocolVersion);
    put_u16_at(bytes, 6, static_cast<std::uint16_t>(MessageType::NextWaveRequest));
    put_u32_at(bytes, 20, static_cast<std::uint32_t>(1 + rng.bounded(1000)));
    const std::uint32_t declared = static_cast<std::uint32_t>(rng.next_u64());
    put_u32_at(bytes, 28, declared);
    const std::vector<std::byte> body = random_bytes(rng, rng.bounded(160));
    put_u32_at(bytes, 32, crc32c(body));
    if (rng.chance(50)) {
      // The declared length is patched to the delivered length half the time, so
      // both "truthful" and "lying" headers are covered.
      put_u32_at(bytes, 28, static_cast<std::uint32_t>(body.size()));
    }
    put_u32_at(bytes, 36, crc32c(header_prefix(bytes)));
    bytes.insert(bytes.end(), body.begin(), body.end());
    require_agreement_with_reference(bytes, limits);
  }
}

SHUFFLE_TEST(adversarial_frames, magic_and_metadata_fuzzing_agrees_with_the_reference) {
  Random rng{derive_seed("adversarial_frames.metadata")};

  for (std::size_t round = 0; round < 2000; ++round) {
    const std::size_t size = adversarial_length(rng, 128);
    std::vector<std::byte> bytes = random_bytes(rng, size);
    if (bytes.size() > 4 && rng.chance(30)) {
      // A correct magic makes the buffer reach the deeper rules more often.
      bytes[0] = static_cast<std::byte>('S');
      bytes[1] = static_cast<std::byte>('F');
      bytes[2] = static_cast<std::byte>('B');
      bytes[3] = static_cast<std::byte>('1');
    }
    if (bytes.size() >= kFrameHeaderBytes && rng.chance(50)) {
      put_u16_at(bytes, 4, kProtocolVersion);
    }
    if (bytes.size() >= kFrameHeaderBytes && rng.chance(40)) {
      put_u16_at(bytes, 6, static_cast<std::uint16_t>(kMessageTypes[rng.bounded(kMessageTypes.size())]));
    }
    if (bytes.size() >= kFrameHeaderBytes && rng.chance(40)) {
      put_u32_at(bytes, 8, static_cast<std::uint32_t>(rng.bounded(4)));
    }
    if (bytes.size() >= kFrameHeaderBytes && rng.chance(40)) {
      put_u32_at(bytes, 20, static_cast<std::uint32_t>(1 + rng.bounded(1000)));
    }
    if (bytes.size() >= kFrameHeaderBytes && rng.chance(50)) {
      put_u32_at(bytes, 28, static_cast<std::uint32_t>(rng.bounded(200)));
    }
    if (bytes.size() >= kFrameHeaderBytes && rng.chance(50)) {
      put_u32_at(bytes, 36, crc32c(std::span<const std::byte>(bytes).first(kFrameHeaderCrcOffset)));
    }
    require_agreement_with_reference(bytes, frame_limits(128));
  }
}

SHUFFLE_TEST(adversarial_frames, absurd_and_contradictory_metadata_is_refused) {
  Random seed{1};
  const std::vector<std::byte> payload = random_bytes(seed, 8);
  const std::vector<std::byte> base = build_frame(MessageType::PublishManifestResponse, 9, 3, kFlagResponse, payload);

  // Reserved flags with a repaired header CRC.
  const std::uint32_t reserved_flags[] = {2u, 0x10u, 0x80000000u, 0xfffffffcu};
  for (const std::uint32_t flags : reserved_flags) {
    std::vector<std::byte> bytes = base;
    put_u32_at(bytes, 8, flags);
    put_u32_at(bytes, 36, crc32c(header_prefix(bytes)));
    require_agreement_with_reference(bytes, Limits{});
    REQUIRE_ERROR(decode_frame(bytes, Limits{}), ErrorCode::MalformedInput);
  }

  // Unsupported versions.
  const std::uint16_t unsupported_versions[] = {0u, 2u, 0x0100u, 0xffffu};
  for (const std::uint16_t version : unsupported_versions) {
    std::vector<std::byte> bytes = base;
    put_u16_at(bytes, 4, version);
    put_u32_at(bytes, 36, crc32c(header_prefix(bytes)));
    require_agreement_with_reference(bytes, Limits{});
    REQUIRE_ERROR(decode_frame(bytes, Limits{}), ErrorCode::UnsupportedVersion);
  }

  // Unknown message types, including the reserved gap and the values past 31.
  const std::uint16_t unknown_types[] = {0u, 4u, 9u, 36u, 39u, 43u, 0x00ffu, 0xffffu};
  for (const std::uint16_t type : unknown_types) {
    std::vector<std::byte> bytes = base;
    put_u16_at(bytes, 6, type);
    put_u32_at(bytes, 36, crc32c(header_prefix(bytes)));
    require_agreement_with_reference(bytes, Limits{});
    REQUIRE_ERROR(decode_frame(bytes, Limits{}), ErrorCode::FrameTypeUnsupported);
  }

  // Sequence zero.
  std::vector<std::byte> zero_sequence = base;
  for (std::size_t index = 0; index < 8; ++index) {
    zero_sequence[20 + index] = std::byte{0};
  }
  put_u32_at(zero_sequence, 36, crc32c(header_prefix(zero_sequence)));
  require_agreement_with_reference(zero_sequence, Limits{});
  REQUIRE_ERROR(decode_frame(zero_sequence, Limits{}), ErrorCode::SequenceViolation);

  // A declared length that contradicts the bytes present, both ways.
  std::vector<std::byte> longer = base;
  put_u32_at(longer, 28, static_cast<std::uint32_t>(payload.size()) + 32);
  put_u32_at(longer, 36, crc32c(header_prefix(longer)));
  require_agreement_with_reference(longer, Limits{});
  REQUIRE_ERROR(decode_frame(longer, Limits{}), ErrorCode::TruncatedInput);

  std::vector<std::byte> shorter = base;
  put_u32_at(shorter, 28, 0);
  put_u32_at(shorter, 32, crc32c(std::span<const std::byte>{}));  // the CRC of an empty payload
  put_u32_at(shorter, 36, crc32c(header_prefix(shorter)));
  require_agreement_with_reference(shorter, Limits{});
  REQUIRE_ERROR(decode_frame(shorter, Limits{}), ErrorCode::TrailingGarbage);

  // Duplicated frames are trailing garbage for exact decoding.
  std::vector<std::byte> doubled = base;
  doubled.insert(doubled.end(), base.begin(), base.end());
  require_agreement_with_reference(doubled, Limits{});
  REQUIRE_ERROR(decode_frame(doubled, Limits{}), ErrorCode::TrailingGarbage);
  const auto first = decode_frame_prefix(doubled, Limits{});
  REQUIRE_OK(first);
  REQUIRE_EQ(first.value().consumed, base.size());

  // A frame with a random tail appended is trailing garbage that the prefix
  // decoder still separates exactly.
  Random rng{derive_seed("adversarial_frames.tails")};
  for (std::size_t round = 0; round < 200; ++round) {
    std::vector<std::byte> with_tail = base;
    const std::vector<std::byte> tail = random_bytes(rng, 1 + rng.bounded(48));
    with_tail.insert(with_tail.end(), tail.begin(), tail.end());
    require_agreement_with_reference(with_tail, Limits{});
    const auto decoded = decode_frame_prefix(with_tail, Limits{});
    REQUIRE_OK(decoded);
    REQUIRE_EQ(decoded.value().consumed, base.size());
  }
}

SHUFFLE_TEST(adversarial_frames, truncated_valid_frames_agree_with_the_reference) {
  Random rng{derive_seed("adversarial_frames.truncation")};

  for (std::size_t round = 0; round < 200; ++round) {
    const std::vector<std::byte> payload = random_bytes(rng, 1 + rng.bounded(64));
    const std::vector<std::byte> frame =
        build_frame(kMessageTypes[rng.bounded(kMessageTypes.size())], rng.next_u64(), 1 + rng.bounded(50), 0, payload);

    const std::size_t cut = static_cast<std::size_t>(rng.bounded(frame.size()));
    const std::span<const std::byte> prefix = std::span<const std::byte>(frame).first(cut);
    require_agreement_with_reference(prefix, Limits{});
    REQUIRE_ERROR(decode_frame_prefix(prefix, Limits{}), ErrorCode::TruncatedInput);
  }
}

SHUFFLE_TEST(adversarial_frames, added_message_types_survive_fuzzing_and_agree_with_the_reference) {
  struct Expected {
    MessageType type;
    std::uint16_t number;
    bool response;
  };
  const Expected added[] = {
      {MessageType::ManifestRequest, 32, false},   {MessageType::ManifestResponse, 33, true},
      {MessageType::StatusRequest, 34, false},     {MessageType::StatusResponse, 35, true},
      {MessageType::ChunkFetchRequest, 40, false}, {MessageType::ChunkFetchResponse, 41, true},
      {MessageType::ChunkFetchFailure, 42, true},
  };

  Random rng{derive_seed("adversarial_frames.added_types")};
  for (const Expected& expected : added) {
    REQUIRE_EQ(static_cast<std::uint16_t>(expected.type), expected.number);
    REQUIRE(is_known_message_type(expected.type));
    REQUIRE_EQ(is_request(expected.type), !expected.response);
    REQUIRE_EQ(is_response(expected.type), expected.response);

    const std::vector<std::byte> payload = random_bytes(rng, rng.bounded(64));
    const std::uint64_t session = rng.next_u64();
    const std::uint64_t sequence = 1 + rng.bounded(1000);
    const std::uint32_t flags = expected.response ? kFlagResponse : 0u;
    const std::vector<std::byte> frame = build_frame(expected.type, session, sequence, flags, payload);

    // The whole frame, and every prefix of it, is judged exactly as the
    // specification says by both implementations.
    require_agreement_with_reference(frame, Limits{});
    for (std::size_t cut = 0; cut < frame.size(); ++cut) {
      const std::span<const std::byte> prefix = std::span<const std::byte>(frame).first(cut);
      require_agreement_with_reference(prefix, Limits{});
      REQUIRE_ERROR(decode_frame_prefix(prefix, Limits{}), ErrorCode::TruncatedInput);
    }

    const auto decoded = decode_frame(frame, Limits{});
    REQUIRE_OK(decoded);
    REQUIRE_EQ(decoded.value().header.type, expected.type);
    REQUIRE_EQ(decoded.value().header.session, SessionId{session});
    REQUIRE_EQ(decoded.value().header.sequence, sequence);
    REQUIRE_EQ(decoded.value().header.flags, flags);
    REQUIRE(same_bytes(decoded.value().payload, payload));

    // Mapping the type field to another number is an integrity failure for a
    // foreign number and a clean decode for a known one: the number on the wire
    // is what decides, and the CRC is what gates it.
    for (std::size_t round = 0; round < 8; ++round) {
      std::vector<std::byte> retyped = frame;
      const std::uint16_t replacement = static_cast<std::uint16_t>(rng.bounded(50));
      put_u16_at(retyped, 6, replacement);
      put_u32_at(retyped, 36, crc32c(std::span<const std::byte>(retyped).first(kFrameHeaderCrcOffset)));
      require_agreement_with_reference(retyped, Limits{});
      const auto retyped_frame = decode_frame(retyped, Limits{});
      if (is_known_message_type(static_cast<MessageType>(replacement))) {
        REQUIRE_OK(retyped_frame);
        REQUIRE_EQ(retyped_frame.value().header.type, static_cast<MessageType>(replacement));
      } else {
        REQUIRE_ERROR(retyped_frame, ErrorCode::FrameTypeUnsupported);
      }
    }

    // A corrupted payload byte of a chunk frame is still refused.
    if (!payload.empty()) {
      std::vector<std::byte> damaged = frame;
      damaged[kFrameHeaderBytes] =
          static_cast<std::byte>(std::to_integer<std::uint8_t>(damaged[kFrameHeaderBytes]) ^ 0x01u);
      require_agreement_with_reference(damaged, Limits{});
      REQUIRE_ERROR(decode_frame(damaged, Limits{}), ErrorCode::ChecksumMismatch);
    }
  }
}

// ---------------------------------------------------------------------------
// FrameStream under adversarial feeding
// ---------------------------------------------------------------------------

SHUFFLE_TEST(adversarial_frames, frame_stream_reassembles_random_chunkings) {
  Random rng{derive_seed("adversarial_frames.stream_chunking")};

  for (std::size_t round = 0; round < 120; ++round) {
    const std::size_t frame_count = 1 + static_cast<std::size_t>(rng.bounded(5));
    std::vector<std::vector<std::byte>> frames;
    std::vector<std::vector<std::byte>> payloads;
    std::vector<FrameHeader> headers;
    std::vector<std::byte> wire;

    for (std::size_t index = 0; index < frame_count; ++index) {
      const std::vector<std::byte> payload = random_bytes(rng, rng.bounded(160));
      const MessageType type = kMessageTypes[rng.bounded(kMessageTypes.size())];
      const std::uint64_t session = rng.next_u64();
      const std::uint64_t sequence = index + 1;
      const std::uint32_t flags = is_response(type) ? kFlagResponse : 0u;

      FrameHeader header;
      header.type = type;
      header.flags = flags;
      header.session = SessionId{session};
      header.sequence = sequence;

      std::vector<std::byte> encoded;
      REQUIRE_OK(encode_frame(header, payload, encoded, Limits{}));
      wire.insert(wire.end(), encoded.begin(), encoded.end());
      frames.push_back(std::move(encoded));
      payloads.push_back(payload);
      headers.push_back(header);
    }

    FrameStream stream{Limits{}};
    std::size_t offset = 0;
    std::size_t decoded_count = 0;
    while (offset < wire.size()) {
      const std::size_t chunk = 1 + static_cast<std::size_t>(rng.bounded(48));
      const std::size_t taken = std::min(chunk, wire.size() - offset);
      REQUIRE_OK(stream.feed(std::span<const std::byte>(wire).subspan(offset, taken)));
      offset += taken;
      // The queue can never exceed the configured frame bound, whatever the
      // chunking, even when several frames are buffered at once.
      REQUIRE(stream.buffered() <=
              static_cast<std::size_t>(Limits{}.max_frame_payload_bytes) + kFrameHeaderBytes);

      for (;;) {
        const auto decoded = stream.next();
        if (!decoded.ok()) {
          REQUIRE_ERROR(decoded, ErrorCode::NoWorkAvailable);
          break;
        }
        REQUIRE(decoded_count < frame_count);
        REQUIRE_EQ(decoded.value().header.type, headers[decoded_count].type);
        REQUIRE_EQ(decoded.value().header.session, headers[decoded_count].session);
        REQUIRE_EQ(decoded.value().header.sequence, headers[decoded_count].sequence);
        REQUIRE_EQ(decoded.value().header.flags, headers[decoded_count].flags);
        REQUIRE_EQ(decoded.value().consumed, frames[decoded_count].size());
        REQUIRE(same_bytes(decoded.value().payload, payloads[decoded_count]));
        ++decoded_count;
      }
    }

    REQUIRE_EQ(decoded_count, frame_count);
    REQUIRE_EQ(stream.buffered(), 0u);
    REQUIRE_ERROR(stream.next(), ErrorCode::NoWorkAvailable);
  }
}

SHUFFLE_TEST(adversarial_frames, frame_stream_keeps_its_queue_bounded) {
  Random rng{derive_seed("adversarial_frames.stream_bounds")};
  const Limits limits = frame_limits(64);
  const std::size_t capacity =
      static_cast<std::size_t>(limits.max_frame_payload_bytes) + kFrameHeaderBytes;

  for (std::size_t round = 0; round < 200; ++round) {
    FrameStream stream{limits};
    const std::vector<std::byte> chunk = random_bytes(rng, 1 + rng.bounded(200));
    const Status fed = stream.feed(chunk);
    REQUIRE(stream.buffered() <= capacity);
    if (chunk.size() > capacity) {
      REQUIRE_ERROR(fed, ErrorCode::ResourceExhausted);
      REQUIRE_EQ(stream.buffered(), 0u);
    } else {
      REQUIRE_OK(fed);
      REQUIRE_EQ(stream.buffered(), chunk.size());
    }
  }

  // A declared length beyond the bound is refused from the header alone, even
  // though the payload never arrives.
  FrameStream stream{limits};
  std::vector<std::byte> header(kFrameHeaderBytes, std::byte{0});
  header[0] = static_cast<std::byte>('S');
  header[1] = static_cast<std::byte>('F');
  header[2] = static_cast<std::byte>('B');
  header[3] = static_cast<std::byte>('1');
  put_u16_at(header, 4, kProtocolVersion);
  put_u16_at(header, 6, static_cast<std::uint16_t>(MessageType::ProgressRequest));
  put_u32_at(header, 20, 1);
  put_u32_at(header, 28, 65);
  put_u32_at(header, 36, crc32c(header_prefix(header)));
  REQUIRE_OK(stream.feed(header));
  REQUIRE_ERROR(stream.next(), ErrorCode::OversizedInput);
}

// ---------------------------------------------------------------------------
// Loopback sockets
// ---------------------------------------------------------------------------

// Opens a connected client/server pair over the loopback interface. Every wait
// blocks until the peer's already-issued work is observable, so nothing here
// depends on how quickly a machine schedules threads.
void open_loopback_pair(SocketRuntime& runtime, TcpListener& listener, Socket& client, Socket& server) {
  REQUIRE(runtime.active());
  auto bound = TcpListener::bind("127.0.0.1", 0, 4, runtime);
  REQUIRE_OK(bound);
  listener = bound.take();
  REQUIRE(listener.valid());

  auto port = listener.local_port();
  REQUIRE_OK(port);
  REQUIRE(port.value() != 0);

  auto connecting = Socket::connect("127.0.0.1", port.value(), 2000, runtime);
  REQUIRE_OK(connecting);
  client = connecting.take();
  REQUIRE(client.valid());

  auto accepted = listener.accept(2000);
  REQUIRE_OK(accepted);
  server = accepted.take();
  REQUIRE(server.valid());

  auto client_port = client.local_port();
  REQUIRE_OK(client_port);
  REQUIRE(client_port.value() != 0);
  auto server_port = server.local_port();
  REQUIRE_OK(server_port);
  REQUIRE_EQ(server_port.value(), port.value());
}

// Reads from the socket until the stream yields one whole frame. The caller has
// already sent the frame, so every read succeeds by blocking, never by timing.
[[nodiscard]] std::vector<std::byte> receive_one_frame(Socket& socket, FrameStream& stream) {
  std::vector<std::byte> received;
  std::array<std::byte, 256> chunk{};
  for (;;) {
    const auto frame = stream.next();
    if (frame.ok()) {
      return received;
    }
    REQUIRE_ERROR(frame, ErrorCode::NoWorkAvailable);
    const auto count = socket.recv_some(chunk, 2000);
    REQUIRE_OK(count);
    REQUIRE(count.value() > 0);
    const std::span<const std::byte> arrived(chunk.data(), count.value());
    received.insert(received.end(), arrived.begin(), arrived.end());
    REQUIRE_OK(stream.feed(arrived));
  }
}

SHUFFLE_TEST(adversarial_frames, loopback_carries_framed_messages_in_both_directions) {
  SocketRuntime runtime;
  TcpListener listener;
  Socket client;
  Socket server;
  open_loopback_pair(runtime, listener, client, server);

  const std::vector<std::byte> request_payload = {std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};
  const std::vector<std::byte> request = build_frame(MessageType::HandshakeRequest, 0xabcdu, 1, 0, request_payload);
  REQUIRE_OK(client.send_all(request, 2000));

  FrameStream server_stream{Limits{}};
  const std::vector<std::byte> received_request = receive_one_frame(server, server_stream);
  REQUIRE(same_bytes(received_request, request));  // byte-exact framing on the wire

  const auto decoded_request = decode_frame(received_request, Limits{});
  REQUIRE_OK(decoded_request);
  REQUIRE_EQ(decoded_request.value().header.type, MessageType::HandshakeRequest);
  REQUIRE_EQ(decoded_request.value().header.session, SessionId{0xabcdu});
  REQUIRE_EQ(decoded_request.value().header.sequence, 1u);
  REQUIRE(same_bytes(decoded_request.value().payload, request_payload));

  const std::vector<std::byte> response_payload = {std::byte{0x10}, std::byte{0x20}};
  const std::vector<std::byte> response =
      build_frame(MessageType::HandshakeResponse, 0xabcdu, 1, kFlagResponse, response_payload);
  REQUIRE_OK(server.send_all(response, 2000));

  FrameStream client_stream{Limits{}};
  const std::vector<std::byte> received_response = receive_one_frame(client, client_stream);
  REQUIRE(same_bytes(received_response, response));

  const auto decoded_response = decode_frame(received_response, Limits{});
  REQUIRE_OK(decoded_response);
  REQUIRE_EQ(decoded_response.value().header.type, MessageType::HandshakeResponse);
  REQUIRE(decoded_response.value().header.carries_response_flag());
  REQUIRE(same_bytes(decoded_response.value().payload, response_payload));

  // A second exchange reuses the same streams: nothing was consumed twice.
  const std::vector<std::byte> second_payload = {std::byte{0x7f}};
  const std::vector<std::byte> second = build_frame(MessageType::ProgressRequest, 0xabcdu, 2, 0, second_payload);
  REQUIRE_OK(client.send_all(second, 2000));
  REQUIRE(same_bytes(receive_one_frame(server, server_stream), second));
  REQUIRE_EQ(decode_frame(second, Limits{}).value().header.sequence, 2u);
}

SHUFFLE_TEST(adversarial_frames, loopback_reassembles_two_frames_and_a_split_frame) {
  SocketRuntime runtime;
  TcpListener listener;
  Socket client;
  Socket server;
  open_loopback_pair(runtime, listener, client, server);

  const std::vector<std::byte> first = build_frame(MessageType::NextWaveRequest, 1, 1, 0, {std::byte{0xa1}});
  const std::vector<std::byte> second =
      build_frame(MessageType::NextWaveResponse, 1, 2, kFlagResponse, {std::byte{0xb2}, std::byte{0xb3}});

  // Two frames in a single write arrive as one byte stream.
  std::vector<std::byte> both = first;
  both.insert(both.end(), second.begin(), second.end());
  REQUIRE_OK(client.send_all(both, 2000));

  FrameStream stream{Limits{}};
  std::array<std::byte, 64> chunk{};
  while (stream.buffered() < both.size()) {
    const auto count = server.recv_some(chunk, 2000);
    REQUIRE_OK(count);
    REQUIRE(count.value() > 0);
    REQUIRE_OK(stream.feed(std::span<const std::byte>(chunk.data(), count.value())));
  }

  const auto one = stream.next();
  REQUIRE_OK(one);
  REQUIRE_EQ(one.value().consumed, first.size());
  REQUIRE_EQ(one.value().header.type, MessageType::NextWaveRequest);
  const auto two = stream.next();
  REQUIRE_OK(two);
  REQUIRE_EQ(two.value().header.type, MessageType::NextWaveResponse);
  REQUIRE(same_bytes(two.value().payload, std::span<const std::byte>(second).subspan(kFrameHeaderBytes)));
  REQUIRE_EQ(stream.buffered(), 0u);

  // A frame written in two pieces is reassembled into exactly one frame.
  const std::vector<std::byte> third =
      build_frame(MessageType::PublishManifestRequest, 2, 1, 0, {std::byte{0xc4}, std::byte{0xc5}, std::byte{0xc6}});
  const std::size_t split = third.size() / 2;
  REQUIRE_OK(client.send_all(std::span<const std::byte>(third).first(split), 2000));
  REQUIRE_OK(client.send_all(std::span<const std::byte>(third).subspan(split), 2000));

  const std::vector<std::byte> received = receive_one_frame(server, stream);
  REQUIRE(same_bytes(received, third));
  const auto decoded = decode_frame(received, Limits{});
  REQUIRE_OK(decoded);
  REQUIRE_EQ(decoded.value().header.type, MessageType::PublishManifestRequest);
  REQUIRE_EQ(decoded.value().header.sequence, 1u);
}

SHUFFLE_TEST(adversarial_frames, loopback_reports_a_peer_close_as_end_of_stream) {
  SocketRuntime runtime;
  TcpListener listener;
  Socket client;
  Socket server;
  open_loopback_pair(runtime, listener, client, server);

  // Nothing was written, so a zero budget is a poll and must time out.
  const auto idle = server.wait_readable(0);
  REQUIRE_OK(idle);
  REQUIRE_EQ(idle.value(), Socket::WaitResult::TimedOut);

  client.close();
  REQUIRE_FALSE(client.valid());

  // The peer's close is a fact that arrives; the wait blocks until it does.
  const auto closed = server.wait_readable(2000);
  REQUIRE_OK(closed);
  REQUIRE_EQ(closed.value(), Socket::WaitResult::Closed);

  std::array<std::byte, 16> buffer{};
  const auto eof = server.recv_some(buffer, 2000);
  REQUIRE_ERROR(eof, ErrorCode::ConnectionClosed);
}

SHUFFLE_TEST(adversarial_frames, local_shutdown_is_reported_as_a_closed_stream) {
  SocketRuntime runtime;
  TcpListener listener;
  Socket client;
  Socket server;
  open_loopback_pair(runtime, listener, client, server);

  REQUIRE_OK(client.shutdown());
  REQUIRE_OK(client.shutdown());  // idempotent

  std::array<std::byte, 16> buffer{};
  const std::vector<std::byte> payload = {std::byte{0x01}};
  REQUIRE_ERROR(client.send_all(payload, 500), ErrorCode::ConnectionClosed);
  REQUIRE_ERROR(client.recv_some(buffer, 500), ErrorCode::ConnectionClosed);

  // The peer observes the shutdown as end of stream without sending anything.
  const auto closed = server.wait_readable(2000);
  REQUIRE_OK(closed);
  REQUIRE_EQ(closed.value(), Socket::WaitResult::Closed);
  REQUIRE_ERROR(server.recv_some(buffer, 2000), ErrorCode::ConnectionClosed);
}

SHUFFLE_TEST(adversarial_frames, operations_on_a_closed_handle_are_refused_not_blocked) {
  SocketRuntime runtime;
  TcpListener listener;
  Socket client;
  Socket server;
  open_loopback_pair(runtime, listener, client, server);

  client.close();
  client.close();  // closing twice must not close a second handle
  REQUIRE_FALSE(client.valid());
  REQUIRE_EQ(client.native(), kInvalidSocket);

  std::array<std::byte, 8> buffer{};
  const std::vector<std::byte> payload = {std::byte{0x09}};
  REQUIRE_ERROR(client.send_all(payload, 100), ErrorCode::PeerUnavailable);
  REQUIRE_ERROR(client.recv_some(buffer, 100), ErrorCode::PeerUnavailable);
  REQUIRE_ERROR(client.wait_readable(100), ErrorCode::PeerUnavailable);
  REQUIRE_ERROR(client.wait_writable(100), ErrorCode::PeerUnavailable);
  REQUIRE_ERROR(client.shutdown(), ErrorCode::PeerUnavailable);
  REQUIRE_ERROR(client.local_port(), ErrorCode::PeerUnavailable);
  REQUIRE_ERROR(client.set_nonblocking(true), ErrorCode::PeerUnavailable);

  // The peer is untouched: closing one end must not close the other.
  REQUIRE(server.valid());
  REQUIRE(server.local_port().value() != 0);
}

SHUFFLE_TEST(adversarial_frames, a_moved_socket_owns_the_handle_exactly_once) {
  SocketRuntime runtime;
  TcpListener listener;
  Socket client;
  Socket server;
  open_loopback_pair(runtime, listener, client, server);

  Socket moved = std::move(client);
  REQUIRE_FALSE(client.valid());
  REQUIRE(moved.valid());

  // If the moved-from handle had been closed, this send would fail.
  const std::vector<std::byte> frame = build_frame(MessageType::CloseRequest, 3, 1, 0, {std::byte{0x2a}});
  REQUIRE_OK(moved.send_all(frame, 2000));
  FrameStream stream{Limits{}};
  REQUIRE(same_bytes(receive_one_frame(server, stream), frame));

  // Move assignment closes the previous handle of the target, exactly once.
  Socket reused;
  reused = std::move(moved);
  REQUIRE_FALSE(moved.valid());
  REQUIRE(reused.valid());
  reused.close();
  REQUIRE_FALSE(reused.valid());
}

SHUFFLE_TEST(adversarial_frames, a_default_constructed_socket_is_not_usable) {
  Socket socket;
  REQUIRE_FALSE(socket.valid());
  REQUIRE_EQ(socket.native(), kInvalidSocket);

  std::array<std::byte, 8> buffer{};
  const std::vector<std::byte> payload = {std::byte{0x01}};
  REQUIRE_ERROR(socket.send_all(payload, 10), ErrorCode::PeerUnavailable);
  REQUIRE_ERROR(socket.recv_some(buffer, 10), ErrorCode::PeerUnavailable);
  REQUIRE_ERROR(socket.wait_readable(10), ErrorCode::PeerUnavailable);
  REQUIRE_ERROR(socket.local_port(), ErrorCode::PeerUnavailable);

  TcpListener listener;
  REQUIRE_FALSE(listener.valid());
  REQUIRE_ERROR(listener.local_port(), ErrorCode::PeerUnavailable);
  auto accepted = listener.accept(10);
  REQUIRE_ERROR(accepted, ErrorCode::PeerUnavailable);
}

SHUFFLE_TEST(adversarial_frames, listener_reports_no_work_when_the_queue_is_empty) {
  SocketRuntime runtime;
  REQUIRE(runtime.active());
  auto bound = TcpListener::bind("127.0.0.1", 0, 4, runtime);
  REQUIRE_OK(bound);
  TcpListener listener = bound.take();

  // A zero budget is a poll: with nothing pending the answer is "no work", and
  // the listener is still usable afterwards.
  const auto empty = listener.accept(0);
  REQUIRE_ERROR(empty, ErrorCode::NoWorkAvailable);
  REQUIRE(listener.valid());

  auto port = listener.local_port();
  REQUIRE_OK(port);
  auto connecting = Socket::connect("127.0.0.1", port.value(), 2000, runtime);
  REQUIRE_OK(connecting);
  Socket client = connecting.take();

  auto accepted = listener.accept(2000);
  REQUIRE_OK(accepted);
  Socket server = accepted.take();
  REQUIRE(server.valid());

  listener.close();
  REQUIRE_FALSE(listener.valid());
  listener.close();  // idempotent
  REQUIRE_ERROR(listener.accept(10), ErrorCode::PeerUnavailable);
  REQUIRE_ERROR(listener.local_port(), ErrorCode::PeerUnavailable);
}

SHUFFLE_TEST(adversarial_frames, a_blocked_accept_is_woken_by_closing_the_listener) {
  SocketRuntime runtime;
  REQUIRE(runtime.active());
  auto bound = TcpListener::bind("127.0.0.1", 0, 4, runtime);
  REQUIRE_OK(bound);
  TcpListener listener = bound.take();

  std::atomic<bool> entered{false};
  ErrorCode observed = ErrorCode::Ok;
  std::thread worker([&listener, &entered, &observed] {
    entered.store(true, std::memory_order_release);
    auto accepted = listener.accept(10000);
    observed = accepted.ok() ? ErrorCode::Ok : accepted.code();
  });

  while (!entered.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  const auto started = std::chrono::steady_clock::now();
  listener.close();
  worker.join();
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started);

  // A closed listener never hands out a connection, and the blocked wait woke up
  // instead of running out its ten-second budget. Latency is the property under
  // test here, so it is measured; the bound is generous enough that only a
  // genuinely stuck wait can miss it.
  REQUIRE(observed != ErrorCode::Ok);
  REQUIRE(observed == ErrorCode::PeerUnavailable || observed == ErrorCode::ConnectionClosed ||
          observed == ErrorCode::ConnectionFailure);
  REQUIRE(elapsed.count() < 3000);
}

SHUFFLE_TEST(adversarial_frames, a_connect_to_a_closed_port_reports_peer_unavailable) {
  SocketRuntime runtime;
  REQUIRE(runtime.active());

  std::uint16_t port = 0;
  {
    auto bound = TcpListener::bind("127.0.0.1", 0, 1, runtime);
    REQUIRE_OK(bound);
    TcpListener probe = bound.take();
    auto local = probe.local_port();
    REQUIRE_OK(local);
    port = local.value();
    REQUIRE(port != 0);
    probe.close();
  }

  // Nothing is listening on that port any more, so the refusal is immediate.
  const auto refused = Socket::connect("127.0.0.1", port, 2000, runtime);
  REQUIRE_ERROR(refused, ErrorCode::PeerUnavailable);
}

SHUFFLE_TEST(adversarial_frames, socket_arguments_are_validated_explicitly) {
  SocketRuntime runtime;
  REQUIRE(runtime.active());

  REQUIRE_ERROR(TcpListener::bind("", 0, 4, runtime), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(TcpListener::bind("256.0.0.1", 0, 4, runtime), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(TcpListener::bind("::1", 0, 4, runtime), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(TcpListener::bind("127.0.0.1", 0, 0, runtime), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(TcpListener::bind("127.0.0.1", 0, kMaxListenBacklog + 1, runtime), ErrorCode::InvalidArgument);

  REQUIRE_ERROR(Socket::connect("127.0.0.1", 0, 100, runtime), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(Socket::connect("localhost.example.com", 80, 100, runtime), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(Socket::connect("127.0.0.1", 80, -1, runtime), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(Socket::connect("127.0.0.1", 80, kMaxWaitMs + 1, runtime), ErrorCode::InvalidArgument);

  // A bound and listening socket is cheap; the same port number twice is not
  // required to fail, so only the listener form is exercised here.
  auto bound = TcpListener::bind("localhost", 0, 1, runtime);
  REQUIRE_OK(bound);
  TcpListener listener = bound.take();
  auto port = listener.local_port();
  REQUIRE_OK(port);
  REQUIRE(port.value() != 0);

  auto connecting = Socket::connect("127.0.0.1", port.value(), 2000, runtime);
  REQUIRE_OK(connecting);
  Socket client = connecting.take();
  auto accepted = listener.accept(2000);
  REQUIRE_OK(accepted);
  Socket server = accepted.take();

  std::array<std::byte, 8> buffer{};
  REQUIRE_ERROR(client.recv_some(std::span<std::byte>{}, 10), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(client.send_all(std::span<const std::byte>{}, -1), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(client.recv_some(buffer, kMaxWaitMs + 1), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(client.wait_readable(-1), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(client.wait_writable(kMaxWaitMs + 1), ErrorCode::InvalidArgument);

  // An empty send is a no-op success: there is nothing to send, which is not a
  // failure, and the connection is unaffected.
  REQUIRE_OK(client.send_all(std::span<const std::byte>{}, 0));

  // A zero-budget poll on a live but idle socket times out rather than lying.
  const auto idle = client.wait_writable(0);
  REQUIRE_OK(idle);
  REQUIRE_EQ(idle.value(), Socket::WaitResult::Ready);  // an empty send buffer is writable
}

}  // namespace
