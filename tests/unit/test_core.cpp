// Deterministic unit tests for identities, digests, hashing and the canonical
// codec. These are the foundations every other proof surface rests on.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include <array>
#include <cstdint>
#include <ostream>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

#include "shuffle/fabric/bytes.hpp"
#include "shuffle/fabric/codec.hpp"
#include "shuffle/fabric/digest.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/hash.hpp"
#include "shuffle/fabric/identity.hpp"
#include "shuffle/fabric/version.hpp"
#include "test_support.hpp"

namespace {

using namespace shuffle::fabric;

constexpr std::uint64_t kMaxU64 = 0xffffffffffffffffull;

SHUFFLE_TEST(identity, distinct_types_do_not_interchange) {
  static_assert(!std::is_same_v<PartitionId, ChunkId>);
  static_assert(!std::is_same_v<ProducerId, ConsumerId>);
  static_assert(!std::is_same_v<ShuffleId, ShuffleGeneration>);
  static_assert(sizeof(PartitionId) == sizeof(std::uint64_t));

  const PartitionId partition{7};
  const ChunkId chunk{7};
  REQUIRE_EQ(partition.value(), chunk.value());
  REQUIRE_EQ(partition.to_string(), std::string{"7"});
}

SHUFFLE_TEST(identity, text_round_trip) {
  const PartitionId original{123456789};
  const auto parsed = PartitionId::parse(original.to_string());
  REQUIRE_OK(parsed);
  REQUIRE_EQ(parsed.value(), original);

  const auto zero = PartitionId::parse("0");
  REQUIRE_OK(zero);
  REQUIRE_EQ(zero.value().value(), 0u);

  const auto maximum = ShuffleId::parse("18446744073709551615");
  REQUIRE_OK(maximum);
  REQUIRE_EQ(maximum.value().value(), kMaxU64);
}

SHUFFLE_TEST(identity, canonical_text_is_enforced) {
  REQUIRE_ERROR(ProducerId::parse(""), ErrorCode::EmptyValue);
  REQUIRE_ERROR(ProducerId::parse("007"), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(ProducerId::parse("+7"), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(ProducerId::parse(" 7"), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(ProducerId::parse("7 "), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(ProducerId::parse("0x7"), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(ProducerId::parse("18446744073709551616"), ErrorCode::IntegerOverflow);
}

SHUFFLE_TEST(identity, generations_advance_monotonically) {
  ShuffleGeneration generation{};
  REQUIRE(generation.is_zero());
  generation = generation.next();
  REQUIRE_EQ(generation.value(), 1u);
  generation = generation.next();
  REQUIRE(generation > ShuffleGeneration{1});
  REQUIRE(ShuffleGeneration{2} == generation);

  LogicalClock clock;
  REQUIRE_EQ(clock.now().value(), 0u);
  const TickId first = clock.advance();
  const TickId second = clock.advance();
  REQUIRE(first < second);
  clock.reset(TickId{10});
  REQUIRE_EQ(clock.now().value(), 10u);
}

SHUFFLE_TEST(identity, usable_in_ordered_and_hashed_containers) {
  std::set<PartitionId> ordered;
  ordered.insert(PartitionId{3});
  ordered.insert(PartitionId{1});
  ordered.insert(PartitionId{2});
  REQUIRE_EQ(ordered.size(), std::size_t{3});
  REQUIRE_EQ(ordered.begin()->value(), 1u);
}

SHUFFLE_TEST(digest, hex_round_trip) {
  const Digest digest = sha256("shuffle-fabric");
  const std::string hex = digest.to_hex();
  REQUIRE_EQ(hex.size(), kHexDigestChars);
  const auto parsed = Digest::from_hex(hex);
  REQUIRE_OK(parsed);
  REQUIRE_EQ(parsed.value(), digest);

  REQUIRE_ERROR(Digest::from_hex(""), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(Digest::from_hex(std::string(64, 'z')), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(Digest::from_hex(std::string(63, 'a')), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(Digest::from_hex(std::string(65, 'a')), ErrorCode::InvalidArgument);
  REQUIRE(Digest{}.is_zero());
  REQUIRE_FALSE(digest.is_zero());
}

SHUFFLE_TEST(digest, sha256_known_vectors) {
  // FIPS 180-4 / NIST published digests.
  REQUIRE_EQ(sha256("").to_hex(),
             std::string{"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"});
  REQUIRE_EQ(sha256("abc").to_hex(),
             std::string{"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"});
  REQUIRE_EQ(sha256("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq").to_hex(),
             std::string{"248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"});
  // Streaming across buffer boundaries must agree with a single-shot update.
  Sha256 streaming;
  const std::string chunk(64, 'a');
  for (int index = 0; index < 1000; ++index) {
    streaming.update(chunk);
  }
  const Digest streamed = streaming.finish();
  REQUIRE_EQ(streamed, sha256(std::string(64000, 'a')));
}

SHUFFLE_TEST(digest, crc32c_known_vectors) {
  // CRC-32C check value from the Castagnoli specification.
  REQUIRE_EQ(crc32c("123456789"), 0xe3069283u);
  REQUIRE_EQ(crc32c(""), 0u);
}

SHUFFLE_TEST(codec, utf8_validation) {
  REQUIRE(is_valid_utf8("plain ascii"));
  REQUIRE(is_valid_utf8("accents: caf\xc3\xa9 na\xc3\xafve"));
  REQUIRE(is_valid_utf8("\xf0\x9f\x9a\x80"));
  REQUIRE_FALSE(is_valid_utf8("\xc0\xaf"));
  REQUIRE_FALSE(is_valid_utf8("\xed\xa0\x80"));
  REQUIRE_FALSE(is_valid_utf8("\xf5\x80\x80\x80"));
  REQUIRE_FALSE(is_valid_utf8("\xe2\x82"));
  REQUIRE_FALSE(is_valid_utf8("\x80"));
}

SHUFFLE_TEST(codec, round_trip_all_primitives) {
  const Limits limits{};
  REQUIRE_OK(limits.validate());

  ByteWriter writer;
  writer.put_u8(0x12);
  writer.put_u16(0x3456);
  writer.put_u32(0x789abcde);
  writer.put_u64(0x0123456789abcdefull);
  writer.put_bool(true);
  writer.put_digest(sha256("payload"));
  writer.put_string("hello \xe2\x82\xac");
  writer.put_byte_string(as_bytes("raw-bytes"));

  const std::vector<std::byte> encoded = writer.take();
  REQUIRE_EQ(encoded.size(), std::size_t{1 + 2 + 4 + 8 + 1 + 32 + 4 + 9 + 4 + 9});

  ByteReader reader{encoded, limits, "round-trip"};
  REQUIRE_EQ(reader.u8(), std::uint8_t{0x12});
  REQUIRE_EQ(reader.u16(), std::uint16_t{0x3456});
  REQUIRE_EQ(reader.u32(), std::uint32_t{0x789abcdeu});
  REQUIRE_EQ(reader.u64(), 0x0123456789abcdefull);
  REQUIRE(reader.boolean());
  REQUIRE_EQ(reader.digest(), sha256("payload"));
  REQUIRE_EQ(reader.string(limits.max_string_bytes), std::string{"hello \xe2\x82\xac"});
  REQUIRE_EQ(as_chars(reader.bytes(limits.max_allocation_bytes)), std::string_view{"raw-bytes"});
  REQUIRE_OK(reader.require_end());
}

SHUFFLE_TEST(codec, rejects_truncation_and_trailing_bytes) {
  const Limits limits{};

  ByteWriter writer;
  writer.put_u32(0x11223344);
  writer.put_u8(0xee);  // one byte the decoder must refuse to ignore
  const std::vector<std::byte> encoded = writer.take();

  ByteReader trailing{encoded, limits, "trailing"};
  REQUIRE_EQ(trailing.u32(), 0x11223344u);
  REQUIRE_ERROR(trailing.require_end(), ErrorCode::TrailingGarbage);

  const std::span<const std::byte> truncated{encoded.data(), 3};
  ByteReader short_reader{truncated, limits, "truncated"};
  static_cast<void>(short_reader.u32());
  REQUIRE_ERROR(short_reader.status(), ErrorCode::TruncatedInput);
  // Sticky error: later reads stay failed, return zero and consume nothing.
  REQUIRE_FALSE(short_reader.ok());
  REQUIRE_EQ(short_reader.u8(), std::uint8_t{0});
  REQUIRE_EQ(short_reader.remaining(), std::size_t{3});
}

SHUFFLE_TEST(codec, enforces_declared_bounds) {
  const Limits limits{};

  ByteWriter writer;
  writer.put_string(std::string(600, 'x'));
  const std::vector<std::byte> encoded = writer.take();

  ByteReader reader{encoded, limits, "string bound"};
  static_cast<void>(reader.string(100));
  REQUIRE_ERROR(reader.status(), ErrorCode::OversizedInput);

  ByteWriter collection;
  collection.put_u32(1000);
  const std::vector<std::byte> collection_bytes = collection.take();
  ByteReader collection_reader{collection_bytes, limits, "collection bound"};
  static_cast<void>(collection_reader.collection_count(10));
  REQUIRE_ERROR(collection_reader.status(), ErrorCode::LimitExceeded);

  ByteReader impossible{collection_bytes, limits, "impossible collection"};
  static_cast<void>(impossible.collection_count(4096));
  REQUIRE_ERROR(impossible.status(), ErrorCode::MalformedInput);

  ByteWriter invalid_utf8;
  const std::string bad = "\xff\xfe";
  invalid_utf8.put_u32(static_cast<std::uint32_t>(bad.size()));
  invalid_utf8.put_raw(as_bytes(bad));
  const std::vector<std::byte> invalid_bytes = invalid_utf8.take();
  ByteReader invalid_reader{invalid_bytes, limits, "invalid utf8"};
  static_cast<void>(invalid_reader.string(limits.max_string_bytes));
  REQUIRE_ERROR(invalid_reader.status(), ErrorCode::InvalidUnicode);

  ByteWriter bad_boolean;
  bad_boolean.put_u8(2);
  const std::vector<std::byte> boolean_bytes = bad_boolean.take();
  ByteReader boolean_reader{boolean_bytes, limits, "boolean"};
  REQUIRE_FALSE(boolean_reader.boolean());
  REQUIRE_ERROR(boolean_reader.status(), ErrorCode::MalformedInput);
}

SHUFFLE_TEST(codec, checked_arithmetic_never_wraps) {
  REQUIRE_EQ(checked_add(1, 2).value(), 3u);
  REQUIRE_ERROR(checked_add(kMaxU64, 1), ErrorCode::IntegerOverflow);
  REQUIRE_EQ(checked_mul(3, 4).value(), 12u);
  REQUIRE_ERROR(checked_mul(kMaxU64, 2), ErrorCode::IntegerOverflow);
  REQUIRE_EQ(checked_mul(0, kMaxU64).value(), 0u);
  REQUIRE_OK(to_size(1024));
  REQUIRE_FALSE(add_overflows(1, 2));
  REQUIRE_FALSE(mul_overflows(kMaxU64, 0));
}

SHUFFLE_TEST(limits, validate_rejects_inconsistent_bounds) {
  Limits limits{};
  REQUIRE_OK(limits.validate());

  Limits bad_frame{};
  bad_frame.max_frame_payload_bytes = bad_frame.max_message_payload_bytes + 1;
  REQUIRE_ERROR(bad_frame.validate(), ErrorCode::InvalidArgument);

  Limits bad_tracking{};
  bad_tracking.max_tracked_partitions = static_cast<std::uint64_t>(bad_tracking.max_partitions) + 1ull;
  REQUIRE_ERROR(bad_tracking.validate(), ErrorCode::InvalidArgument);

  Limits bad_wave{};
  bad_wave.max_wave_grants = 0;
  REQUIRE_ERROR(bad_wave.validate(), ErrorCode::InvalidArgument);
}

SHUFFLE_TEST(errors, classification_is_stable_and_total) {
  REQUIRE_EQ(classify(ErrorCode::Ok), RetryClass::NotApplicable);
  REQUIRE_EQ(classify(ErrorCode::ConnectionFailure), RetryClass::Retriable);
  REQUIRE_EQ(classify(ErrorCode::IntegrityFailure), RetryClass::Retriable);
  REQUIRE_EQ(classify(ErrorCode::PayloadRejected), RetryClass::Retriable);
  REQUIRE_EQ(classify(ErrorCode::BackpressureActive), RetryClass::Deferred);
  REQUIRE_EQ(classify(ErrorCode::BackpressureUnknown), RetryClass::Deferred);
  REQUIRE_EQ(classify(ErrorCode::StaleIncarnation), RetryClass::Authority);
  REQUIRE_EQ(classify(ErrorCode::Cancelled), RetryClass::Authority);
  REQUIRE_EQ(classify(ErrorCode::LateAuthority), RetryClass::Authority);
  REQUIRE_EQ(classify(ErrorCode::MalformedInput), RetryClass::Permanent);
  REQUIRE_EQ(classify(ErrorCode::StateCorrupt), RetryClass::Permanent);

  REQUIRE_EQ(std::string{to_string(ErrorCode::StaleIncarnation)}, std::string{"StaleIncarnation"});
  REQUIRE_EQ(std::string{to_string(RetryClass::Deferred)}, std::string{"Deferred"});
  REQUIRE_EQ(format_error(Error{ErrorCode::DigestMismatch, "chunk 4"}), std::string{"DigestMismatch: chunk 4"});
  REQUIRE_EQ(format_error(Error{ErrorCode::Cancelled, ""}), std::string{"Cancelled"});
}

SHUFFLE_TEST(version, reports_one_zero_zero) {
  REQUIRE_EQ(version_string(), std::string{"1.0.0"});
  REQUIRE_EQ(version_number(), 10000u);
}

}  // namespace
