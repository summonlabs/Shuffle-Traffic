// Proof obligations for the durable snapshot+journal store.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// These cases are written against bytes, not against the implementation: every
// file they feed the store is built here from the frozen format, so a change
// that keeps the API but breaks the format fails the suite.

#include "shuffle/fabric/durable_store.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "shuffle/fabric/hash.hpp"
#include "temp_dir.hpp"
#include "test_support.hpp"

namespace {

using shuffle::fabric::DurableStore;
using shuffle::fabric::ErrorCode;
using shuffle::fabric::JournalTailStatus;
using shuffle::fabric::Limits;
using shuffle::fabric::RecoveryReport;
using shuffle::fabric::Result;
using shuffle::fabric::StoreConfig;
using Bytes = std::vector<std::byte>;

constexpr std::size_t kSnapshotHeaderBytes = 72;
constexpr std::size_t kJournalHeaderBytes = 24;
constexpr std::uint32_t kJournalRecordMagic = 0x4E524A31u;

void write_u32(std::span<std::byte> bytes, std::size_t offset, std::uint32_t value) {
  for (std::size_t index = 0; index < 4; ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (8u * static_cast<unsigned>(index))) & 0xFFu);
  }
}

void write_u64(std::span<std::byte> bytes, std::size_t offset, std::uint64_t value) {
  for (std::size_t index = 0; index < 8; ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (8u * static_cast<unsigned>(index))) & 0xFFu);
  }
}

// Distinct, length-varying payloads: a store that mixed two records up, or that
// lost a length, would still have to reproduce these bytes exactly.
[[nodiscard]] Bytes payload_for(std::uint64_t index) {
  const std::string text =
      "payload-" + std::to_string(index) + "-" + std::string(static_cast<std::size_t>(index % 7), 'x');
  Bytes bytes(text.size());
  for (std::size_t offset = 0; offset < text.size(); ++offset) {
    bytes[offset] = static_cast<std::byte>(static_cast<unsigned char>(text[offset]));
  }
  return bytes;
}

[[nodiscard]] Bytes state_for(std::uint64_t cycle) {
  const std::string text = "snapshot-state-cycle-" + std::to_string(cycle);
  Bytes bytes(text.size());
  for (std::size_t offset = 0; offset < text.size(); ++offset) {
    bytes[offset] = static_cast<std::byte>(static_cast<unsigned char>(text[offset]));
  }
  return bytes;
}

[[nodiscard]] Bytes snapshot_image(std::uint64_t sequence, std::span<const std::byte> payload) {
  const std::array<std::byte, 8> magic{std::byte{0x53}, std::byte{0x46}, std::byte{0x53}, std::byte{0x4E},
                                       std::byte{0x41}, std::byte{0x50}, std::byte{0x30}, std::byte{0x31}};
  Bytes image(kSnapshotHeaderBytes + payload.size());
  std::copy(magic.begin(), magic.end(), image.begin());
  write_u32(image, 8, 1);
  write_u32(image, 12, 0);
  write_u64(image, 16, sequence);
  write_u64(image, 24, payload.size());
  write_u32(image, 32, shuffle::fabric::crc32c(std::span<const std::byte>(image).first(32)));
  write_u32(image, 36, shuffle::fabric::crc32c(payload));
  const auto digest = shuffle::fabric::sha256(payload);
  for (std::size_t index = 0; index < digest.bytes.size(); ++index) {
    image[40 + index] = static_cast<std::byte>(digest.bytes[index]);
  }
  std::copy(payload.begin(), payload.end(), image.begin() + static_cast<std::ptrdiff_t>(kSnapshotHeaderBytes));
  return image;
}

[[nodiscard]] Bytes record_image(std::uint64_t sequence, std::span<const std::byte> payload) {
  Bytes record(kJournalHeaderBytes + payload.size());
  write_u32(record, 0, kJournalRecordMagic);
  write_u32(record, 4, static_cast<std::uint32_t>(payload.size()));
  write_u64(record, 8, sequence);
  write_u32(record, 16, shuffle::fabric::crc32c(std::span<const std::byte>(record).first(16)));
  write_u32(record, 20, shuffle::fabric::crc32c(payload));
  std::copy(payload.begin(), payload.end(), record.begin() + static_cast<std::ptrdiff_t>(kJournalHeaderBytes));
  return record;
}

void write_file(const std::filesystem::path& path, std::span<const std::byte> data) {
  std::ofstream stream(path, std::ios::binary | std::ios::trunc);
  stream.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
  stream.close();
  if (!stream) {
    FAIL_TEST("could not write " + path.string());
  }
}

void append_file(const std::filesystem::path& path, std::span<const std::byte> data) {
  std::ofstream stream(path, std::ios::binary | std::ios::app);
  stream.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
  stream.close();
  if (!stream) {
    FAIL_TEST("could not append to " + path.string());
  }
}

[[nodiscard]] Bytes read_file(const std::filesystem::path& path) {
  std::ifstream stream(path, std::ios::binary);
  if (!stream) {
    FAIL_TEST("could not read " + path.string());
  }
  Bytes data;
  char byte = 0;
  while (stream.get(byte)) {
    data.push_back(static_cast<std::byte>(static_cast<unsigned char>(byte)));
  }
  return data;
}

[[nodiscard]] std::uint64_t file_size_of(const std::filesystem::path& path) {
  std::error_code error;
  const std::uintmax_t size = std::filesystem::file_size(path, error);
  if (error) {
    FAIL_TEST("could not measure " + path.string());
  }
  return static_cast<std::uint64_t>(size);
}

[[nodiscard]] StoreConfig store_config(const std::filesystem::path& directory) {
  StoreConfig config;
  config.directory = directory;
  return config;
}

// Enums do not stream, and a failure message that says "1" instead of
// "TornTail" costs more than this helper.
[[nodiscard]] std::string tail_text(JournalTailStatus status) {
  switch (status) {
    case JournalTailStatus::Clean:
      return "Clean";
    case JournalTailStatus::TornTail:
      return "TornTail";
    case JournalTailStatus::Corrupt:
      return "Corrupt";
    case JournalTailStatus::Missing:
      return "Missing";
  }
  return "Unknown";
}

}  // namespace

SHUFFLE_TEST(durable_store, open_on_an_empty_directory_reports_missing) {
  shuffle::test::TempDir root("durable-store-empty");
  const std::filesystem::path directory = root.path() / "nested" / "store";
  DurableStore store(store_config(directory));
  REQUIRE_OK(store.open());
  const RecoveryReport& report = store.recovery();
  REQUIRE_FALSE(report.snapshot_loaded);
  REQUIRE_FALSE(report.snapshot_from_previous);
  REQUIRE(report.journal_missing);
  REQUIRE_EQ(tail_text(report.tail), std::string{"Missing"});
  REQUIRE(report.revalidation_required);
  REQUIRE_EQ(report.snapshot_sequence, std::uint64_t{0});
  REQUIRE_EQ(report.records_replayed, std::uint64_t{0});
  REQUIRE_EQ(report.records_skipped, std::uint64_t{0});
  REQUIRE_EQ(report.bytes_discarded, std::uint64_t{0});
  REQUIRE_EQ(report.last_sequence, std::uint64_t{0});
  REQUIRE(store.records().empty());
  REQUIRE_EQ(store.journal_bytes(), std::uint64_t{0});
  REQUIRE_EQ(store.appended_records(), std::uint64_t{0});
  REQUIRE(std::filesystem::is_directory(directory));
  // An open that wrote nothing must not leave state behind: "no journal" has to
  // keep meaning "no journal" on the next open.
  REQUIRE_FALSE(std::filesystem::exists(directory / "state.snapshot"));
  REQUIRE_FALSE(std::filesystem::exists(directory / "state.journal"));
}

SHUFFLE_TEST(durable_store, append_close_and_reopen_replays_identical_payloads_in_order) {
  shuffle::test::TempDir root("durable-store-replay");
  const std::vector<Bytes> expected{payload_for(1), payload_for(2), payload_for(3)};
  {
    DurableStore store(store_config(root.path()));
    REQUIRE_OK(store.open());
    for (std::size_t index = 0; index < expected.size(); ++index) {
      const Result<std::uint64_t> sequence = store.append(expected[index]);
      REQUIRE_OK(sequence);
      REQUIRE_EQ(sequence.value(), static_cast<std::uint64_t>(index + 1));
    }
    REQUIRE_EQ(store.last_sequence(), std::uint64_t{3});
    REQUIRE_EQ(store.appended_records(), std::uint64_t{3});
  }

  DurableStore reopened(store_config(root.path()));
  REQUIRE_OK(reopened.open());
  REQUIRE_EQ(reopened.recovery().records_replayed, std::uint64_t{3});
  REQUIRE_EQ(tail_text(reopened.recovery().tail), std::string{"Clean"});
  REQUIRE_FALSE(reopened.recovery().journal_missing);
  REQUIRE_EQ(reopened.records().size(), expected.size());
  for (std::size_t index = 0; index < expected.size(); ++index) {
    REQUIRE(reopened.records()[index] == expected[index]);
  }
  REQUIRE_EQ(reopened.last_sequence(), std::uint64_t{3});
}

SHUFFLE_TEST(durable_store, sequences_stay_monotonic_across_reopens) {
  shuffle::test::TempDir root("durable-store-sequence");
  const StoreConfig config = store_config(root.path());
  {
    DurableStore store(config);
    REQUIRE_OK(store.open());
    REQUIRE_OK(store.append(payload_for(1)));
    REQUIRE_OK(store.append(payload_for(2)));
  }
  {
    DurableStore store(config);
    REQUIRE_OK(store.open());
    REQUIRE_EQ(store.last_sequence(), std::uint64_t{2});
    REQUIRE_OK(store.append(payload_for(3)));
    REQUIRE_EQ(store.last_sequence(), std::uint64_t{3});
  }
  {
    DurableStore store(config);
    REQUIRE_OK(store.open());
    REQUIRE_EQ(store.last_sequence(), std::uint64_t{3});
    REQUIRE_EQ(store.records().size(), std::size_t{3});
    const Result<std::uint64_t> sequence = store.append(payload_for(4));
    REQUIRE_OK(sequence);
    REQUIRE_EQ(sequence.value(), std::uint64_t{4});
  }
}

SHUFFLE_TEST(durable_store, oversized_append_is_refused_without_consuming_a_sequence) {
  shuffle::test::TempDir root("durable-store-oversized");
  StoreConfig config = store_config(root.path());
  config.limits.max_journal_record_bytes = 64;
  DurableStore store(config);
  REQUIRE_OK(store.open());
  REQUIRE_OK(store.append(payload_for(1)));

  const Bytes oversized(65, std::byte{0xAB});
  REQUIRE_ERROR(store.append(oversized), ErrorCode::OversizedInput);
  REQUIRE_EQ(store.last_sequence(), std::uint64_t{1});
  REQUIRE_EQ(store.appended_records(), std::uint64_t{1});

  // The boundary itself is still accepted: the bound is inclusive.
  const Bytes at_bound(64, std::byte{0xCD});
  REQUIRE_OK(store.append(at_bound));
  REQUIRE_EQ(store.last_sequence(), std::uint64_t{2});
}

SHUFFLE_TEST(durable_store, append_and_compact_before_open_are_refused) {
  shuffle::test::TempDir root("durable-store-closed");
  DurableStore store(store_config(root.path()));
  REQUIRE_ERROR(store.append(payload_for(1)), ErrorCode::InvalidState);
  REQUIRE_ERROR(store.compact(state_for(0), 0), ErrorCode::InvalidState);

  // A failed open leaves the store unopened rather than half-open.
  const Bytes broken = snapshot_image(1, payload_for(1));
  Bytes corrupted = broken;
  corrupted[kSnapshotHeaderBytes + 1] ^= std::byte{0xFF};
  write_file(root.file("state.snapshot"), corrupted);
  REQUIRE_ERROR(store.open(), ErrorCode::ChecksumMismatch);
  REQUIRE_ERROR(store.append(payload_for(2)), ErrorCode::InvalidState);
  REQUIRE_EQ(store.last_sequence(), std::uint64_t{0});
  REQUIRE(store.records().empty());
}

SHUFFLE_TEST(durable_store, compact_then_reopen_folds_records) {
  shuffle::test::TempDir root("durable-store-compact");
  const Bytes state = state_for(1);
  const std::vector<Bytes> appended{payload_for(1), payload_for(2), payload_for(3), payload_for(4)};
  Bytes journal_image;
  {
    DurableStore store(store_config(root.path()));
    REQUIRE_OK(store.open());
    for (const Bytes& payload : appended) {
      REQUIRE_OK(store.append(payload));
    }
    REQUIRE_OK(store.compact(state, 4));
    REQUIRE_EQ(store.last_sequence(), std::uint64_t{4});
    REQUIRE_EQ(store.journal_bytes(), std::uint64_t{0});
    REQUIRE(store.records().empty());
  }
  REQUIRE_EQ(file_size_of(root.file("state.journal")), std::uint64_t{0});

  {
    DurableStore reopened(store_config(root.path()));
    REQUIRE_OK(reopened.open());
    REQUIRE(reopened.recovery().snapshot_loaded);
    REQUIRE_EQ(reopened.recovery().snapshot_sequence, std::uint64_t{4});
    REQUIRE_EQ(tail_text(reopened.recovery().tail), std::string{"Clean"});
    REQUIRE(reopened.records().empty());
    REQUIRE(!reopened.recovery().revalidation_required);
    const Result<Bytes> decoded = DurableStore::read_snapshot(root.file("state.snapshot"), Limits{});
    REQUIRE_OK(decoded);
    REQUIRE(decoded.value() == state);
  }

  // A stale writer that re-appends records the snapshot already folded in must
  // be absorbed as duplicates, not replayed and not reported as corruption.
  for (std::size_t index = 0; index < appended.size(); ++index) {
    const Bytes record = record_image(index + 1, appended[index]);
    journal_image.insert(journal_image.end(), record.begin(), record.end());
  }
  write_file(root.file("state.journal"), journal_image);

  DurableStore overlapping(store_config(root.path()));
  REQUIRE_OK(overlapping.open());
  REQUIRE(overlapping.recovery().snapshot_loaded);
  REQUIRE_EQ(overlapping.recovery().snapshot_sequence, std::uint64_t{4});
  REQUIRE_EQ(overlapping.recovery().records_replayed, std::uint64_t{0});
  REQUIRE_EQ(overlapping.recovery().records_skipped, std::uint64_t{4});
  REQUIRE_EQ(tail_text(overlapping.recovery().tail), std::string{"Clean"});
  REQUIRE_EQ(overlapping.recovery().bytes_discarded, std::uint64_t{0});
  REQUIRE_EQ(overlapping.last_sequence(), std::uint64_t{4});
  REQUIRE(overlapping.records().empty());
}

SHUFFLE_TEST(durable_store, compact_refuses_sequences_outside_the_open_range) {
  shuffle::test::TempDir root("durable-store-compact-range");
  DurableStore store(store_config(root.path()));
  REQUIRE_OK(store.open());
  for (std::uint64_t index = 1; index <= 3; ++index) {
    REQUIRE_OK(store.append(payload_for(index)));
  }
  REQUIRE_ERROR(store.compact(state_for(0), 4), ErrorCode::InvalidArgument);
  REQUIRE_OK(store.compact(state_for(1), 3));
  REQUIRE_EQ(store.journal_bytes(), std::uint64_t{0});
  // One step below the sequence the snapshot now carries is refused.
  REQUIRE_ERROR(store.compact(state_for(2), 2), ErrorCode::InvalidArgument);
  REQUIRE_OK(store.compact(state_for(2), 3));
}

SHUFFLE_TEST(durable_store, two_compactions_keep_the_previous_snapshot_readable) {
  shuffle::test::TempDir root("durable-store-previous");
  const Bytes first = state_for(1);
  const Bytes second = state_for(2);
  {
    DurableStore store(store_config(root.path()));
    REQUIRE_OK(store.open());
    REQUIRE_OK(store.append(payload_for(1)));
    REQUIRE_OK(store.compact(first, 1));
    REQUIRE_FALSE(std::filesystem::exists(root.file("state.snapshot.prev")));
    REQUIRE_OK(store.append(payload_for(2)));
    REQUIRE_OK(store.compact(second, 2));
    REQUIRE_EQ(store.journal_bytes(), std::uint64_t{0});
  }

  const Result<Bytes> primary = DurableStore::read_snapshot(root.file("state.snapshot"), Limits{});
  REQUIRE_OK(primary);
  REQUIRE(primary.value() == second);
  const Result<Bytes> previous = DurableStore::read_snapshot(root.file("state.snapshot.prev"), Limits{});
  REQUIRE_OK(previous);
  REQUIRE(previous.value() == first);

  {
    DurableStore reopened(store_config(root.path()));
    REQUIRE_OK(reopened.open());
    REQUIRE(reopened.recovery().snapshot_loaded);
    REQUIRE_FALSE(reopened.recovery().snapshot_from_previous);
    REQUIRE_EQ(reopened.recovery().snapshot_sequence, std::uint64_t{2});
  }
}

SHUFFLE_TEST(durable_store, snapshot_header_corruption_at_every_offset_is_refused) {
  shuffle::test::TempDir root("durable-store-header-sweep");
  const std::filesystem::path path = root.file("state.snapshot");
  const Bytes payload = payload_for(9);
  const Bytes image = snapshot_image(5, payload);
  REQUIRE_EQ(image.size(), kSnapshotHeaderBytes + payload.size());
  for (std::size_t offset = 0; offset < kSnapshotHeaderBytes; ++offset) {
    Bytes corrupted = image;
    corrupted[offset] ^= std::byte{0xFF};
    write_file(path, corrupted);
    const Result<Bytes> decoded = DurableStore::read_snapshot(path, Limits{});
    if (decoded.ok()) {
      FAIL_TEST("snapshot corruption at header offset " + std::to_string(offset) + " was accepted");
    }
  }
  // The same refusal must reach open(): a store whose snapshot was tampered with
  // must not start believing a mixture of old and new bytes.
  Bytes corrupted = image;
  corrupted[0] ^= std::byte{0xFF};
  write_file(path, corrupted);
  DurableStore store(store_config(root.path()));
  REQUIRE_ERROR(store.open(), ErrorCode::MalformedInput);
  REQUIRE_ERROR(store.append(payload_for(1)), ErrorCode::InvalidState);
}

SHUFFLE_TEST(durable_store, snapshot_truncated_at_every_length_below_the_header_is_refused) {
  shuffle::test::TempDir root("durable-store-truncated");
  const std::filesystem::path path = root.file("state.snapshot");
  const Bytes image = snapshot_image(3, payload_for(3));
  for (std::size_t length = 0; length < kSnapshotHeaderBytes; ++length) {
    write_file(path, std::span<const std::byte>(image).first(length));
    const Result<Bytes> decoded = DurableStore::read_snapshot(path, Limits{});
    REQUIRE_ERROR(decoded, ErrorCode::TruncatedInput);
  }
  // Cutting the payload short is just as unusable as cutting the header short.
  write_file(path, std::span<const std::byte>(image).first(image.size() - 1));
  REQUIRE_ERROR(DurableStore::read_snapshot(path, Limits{}), ErrorCode::TruncatedInput);
}

SHUFFLE_TEST(durable_store, snapshot_trailing_garbage_is_refused) {
  shuffle::test::TempDir root("durable-store-garbage");
  const std::filesystem::path path = root.file("state.snapshot");
  Bytes image = snapshot_image(2, payload_for(2));
  image.push_back(std::byte{0x5A});
  write_file(path, image);
  REQUIRE_ERROR(DurableStore::read_snapshot(path, Limits{}), ErrorCode::TrailingGarbage);
}

SHUFFLE_TEST(durable_store, snapshot_version_two_is_refused) {
  shuffle::test::TempDir root("durable-store-version");
  const std::filesystem::path path = root.file("state.snapshot");
  Bytes image = snapshot_image(2, payload_for(2));
  write_u32(image, 8, 2);
  // Recomputed so that the version check, not the checksum, is what refuses.
  write_u32(image, 32, shuffle::fabric::crc32c(std::span<const std::byte>(image).first(32)));
  write_file(path, image);
  REQUIRE_ERROR(DurableStore::read_snapshot(path, Limits{}), ErrorCode::UnsupportedVersion);
}

SHUFFLE_TEST(durable_store, snapshot_nonzero_flags_are_refused) {
  shuffle::test::TempDir root("durable-store-flags");
  const std::filesystem::path path = root.file("state.snapshot");
  Bytes image = snapshot_image(2, payload_for(2));
  write_u32(image, 12, 1);
  write_u32(image, 32, shuffle::fabric::crc32c(std::span<const std::byte>(image).first(32)));
  write_file(path, image);
  REQUIRE_ERROR(DurableStore::read_snapshot(path, Limits{}), ErrorCode::MalformedInput);
}

SHUFFLE_TEST(durable_store, absurd_payload_length_is_refused_without_a_huge_allocation) {
  shuffle::test::TempDir root("durable-store-absurd");
  const std::filesystem::path path = root.file("state.snapshot");
  const std::array<std::byte, 8> magic{std::byte{0x53}, std::byte{0x46}, std::byte{0x53}, std::byte{0x4E},
                                       std::byte{0x41}, std::byte{0x50}, std::byte{0x30}, std::byte{0x31}};
  Bytes header(kSnapshotHeaderBytes, std::byte{0});
  std::copy(magic.begin(), magic.end(), header.begin());
  write_u32(header, 8, 1);
  write_u32(header, 12, 0);
  write_u64(header, 16, 7);
  write_u64(header, 24, 0xFFFFFFFFFFFFF000ull);
  // A correct header checksum isolates the length bound: the only thing that can
  // refuse this file is the bound itself, and a decoder that allocated first
  // would die here instead of returning.
  write_u32(header, 32, shuffle::fabric::crc32c(std::span<const std::byte>(header).first(32)));
  write_file(path, header);
  REQUIRE_EQ(file_size_of(path), std::uint64_t{kSnapshotHeaderBytes});
  REQUIRE_ERROR(DurableStore::read_snapshot(path, Limits{}), ErrorCode::StateTooLarge);

  DurableStore store(store_config(root.path()));
  REQUIRE_ERROR(store.open(), ErrorCode::StateTooLarge);
  REQUIRE_ERROR(store.append(payload_for(1)), ErrorCode::InvalidState);
}

SHUFFLE_TEST(durable_store, missing_snapshot_is_an_error_not_a_crash) {
  shuffle::test::TempDir root("durable-store-missing");
  REQUIRE_ERROR(DurableStore::read_snapshot(root.file("absent.snapshot"), Limits{}), ErrorCode::InvalidArgument);
}

SHUFFLE_TEST(durable_store, journal_truncated_at_every_byte_position_keeps_the_intact_prefix) {
  shuffle::test::TempDir root("durable-store-journal-sweep");
  const std::filesystem::path path = root.file("state.journal");
  const std::vector<Bytes> payloads{payload_for(1), payload_for(2), payload_for(3), payload_for(4)};
  Bytes journal;
  std::vector<std::size_t> record_end;
  for (std::size_t index = 0; index < payloads.size(); ++index) {
    const Bytes record = record_image(index + 1, payloads[index]);
    journal.insert(journal.end(), record.begin(), record.end());
    record_end.push_back(journal.size());
  }

  for (std::size_t cut = 0; cut <= journal.size(); ++cut) {
    write_file(path, std::span<const std::byte>(journal).first(cut));
    std::size_t intact = 0;
    for (std::size_t index = 0; index < record_end.size(); ++index) {
      if (record_end[index] <= cut) {
        intact = index + 1;
      }
    }
    const std::size_t intact_bytes = intact == 0 ? 0 : record_end[intact - 1];
    const std::size_t discarded = cut - intact_bytes;

    DurableStore store(store_config(root.path()));
    REQUIRE_OK(store.open());
    const RecoveryReport& report = store.recovery();
    REQUIRE_EQ(report.records_replayed, static_cast<std::uint64_t>(intact));
    REQUIRE_EQ(store.records().size(), intact);
    for (std::size_t index = 0; index < intact; ++index) {
      REQUIRE(store.records()[index] == payloads[index]);
    }
    REQUIRE_EQ(report.bytes_discarded, static_cast<std::uint64_t>(discarded));
    REQUIRE_EQ(report.last_sequence, static_cast<std::uint64_t>(intact));
    if (discarded == 0) {
      // A cut that lands exactly on a record boundary is indistinguishable from
      // a clean end of file, and must be reported as one.
      REQUIRE_EQ(tail_text(report.tail), std::string{"Clean"});
    } else {
      REQUIRE_EQ(tail_text(report.tail), std::string{"TornTail"});
      REQUIRE(report.revalidation_required);
    }
    // Recovery is idempotent: opening again sees the same prefix, because the
    // refused tail was cut away rather than left behind.
    REQUIRE_OK(store.open());
    REQUIRE_EQ(store.recovery().records_replayed, static_cast<std::uint64_t>(intact));
    REQUIRE_EQ(tail_text(store.recovery().tail), std::string{"Clean"});
    REQUIRE_EQ(store.journal_bytes(), static_cast<std::uint64_t>(intact_bytes));
    REQUIRE_EQ(file_size_of(path), static_cast<std::uint64_t>(intact_bytes));
  }
}

SHUFFLE_TEST(durable_store, flipped_payload_byte_in_record_three_reports_corrupt) {
  shuffle::test::TempDir root("durable-store-corrupt-payload");
  const std::filesystem::path path = root.file("state.journal");
  const std::vector<Bytes> payloads{payload_for(1), payload_for(2), payload_for(3)};
  Bytes journal;
  std::size_t third_offset = 0;
  for (std::size_t index = 0; index < payloads.size(); ++index) {
    if (index == 2) {
      third_offset = journal.size();
    }
    const Bytes record = record_image(index + 1, payloads[index]);
    journal.insert(journal.end(), record.begin(), record.end());
  }
  journal[third_offset + kJournalHeaderBytes + 1] ^= std::byte{0xFF};
  write_file(path, journal);

  {
    DurableStore store(store_config(root.path()));
    REQUIRE_OK(store.open());
    const RecoveryReport& report = store.recovery();
    REQUIRE_EQ(report.records_replayed, std::uint64_t{2});
    REQUIRE_EQ(tail_text(report.tail), std::string{"Corrupt"});
    REQUIRE_EQ(report.bytes_discarded, static_cast<std::uint64_t>(journal.size() - third_offset));
    REQUIRE(report.revalidation_required);
    REQUIRE_EQ(store.records().size(), std::size_t{2});
    REQUIRE(store.records()[0] == payloads[0]);
    REQUIRE(store.records()[1] == payloads[1]);
    REQUIRE_EQ(report.last_sequence, std::uint64_t{2});
    // The refused record is cut away, so the next append continues from the last
    // complete record instead of landing behind debris.
    const Result<std::uint64_t> appended = store.append(payload_for(9));
    REQUIRE_OK(appended);
    REQUIRE_EQ(appended.value(), std::uint64_t{3});
    REQUIRE_EQ(file_size_of(path), store.journal_bytes());
  }

  DurableStore reopened(store_config(root.path()));
  REQUIRE_OK(reopened.open());
  REQUIRE_EQ(tail_text(reopened.recovery().tail), std::string{"Clean"});
  REQUIRE_EQ(reopened.records().size(), std::size_t{3});
  REQUIRE(reopened.records()[2] == payload_for(9));
}

SHUFFLE_TEST(durable_store, decreased_sequence_mid_file_is_corrupt) {
  shuffle::test::TempDir root("durable-store-sequence-regression");
  const std::filesystem::path path = root.file("state.journal");
  const std::vector<std::uint64_t> sequences{1, 2, 1};
  Bytes journal;
  std::size_t third_offset = 0;
  for (std::size_t index = 0; index < sequences.size(); ++index) {
    if (index == 2) {
      third_offset = journal.size();
    }
    const Bytes record = record_image(sequences[index], payload_for(sequences[index]));
    journal.insert(journal.end(), record.begin(), record.end());
  }
  write_file(path, journal);

  DurableStore store(store_config(root.path()));
  REQUIRE_OK(store.open());
  REQUIRE_EQ(store.recovery().records_replayed, std::uint64_t{2});
  REQUIRE_EQ(tail_text(store.recovery().tail), std::string{"Corrupt"});
  REQUIRE_EQ(store.recovery().bytes_discarded, static_cast<std::uint64_t>(journal.size() - third_offset));
  REQUIRE_EQ(store.last_sequence(), std::uint64_t{2});
}

SHUFFLE_TEST(durable_store, duplicated_sequence_at_the_tail_is_skipped_not_corrupt) {
  shuffle::test::TempDir root("durable-store-duplicate");
  const std::filesystem::path path = root.file("state.journal");
  const std::vector<std::uint64_t> sequences{1, 2, 2};
  Bytes journal;
  for (std::size_t index = 0; index < sequences.size(); ++index) {
    const Bytes record = record_image(sequences[index], payload_for(sequences[index]));
    journal.insert(journal.end(), record.begin(), record.end());
  }
  write_file(path, journal);

  DurableStore store(store_config(root.path()));
  REQUIRE_OK(store.open());
  const RecoveryReport& report = store.recovery();
  REQUIRE_EQ(report.records_replayed, std::uint64_t{2});
  REQUIRE_EQ(report.records_skipped, std::uint64_t{1});
  REQUIRE_EQ(tail_text(report.tail), std::string{"Clean"});
  REQUIRE_EQ(report.bytes_discarded, std::uint64_t{0});
  REQUIRE_EQ(report.last_sequence, std::uint64_t{2});
  REQUIRE_EQ(store.records().size(), std::size_t{2});
  REQUIRE(store.records()[1] == payload_for(2));
  // The duplicate is still a real record: the next sequence follows it.
  const Result<std::uint64_t> appended = store.append(payload_for(3));
  REQUIRE_OK(appended);
  REQUIRE_EQ(appended.value(), std::uint64_t{3});
}

SHUFFLE_TEST(durable_store, impossible_record_length_is_corrupt) {
  shuffle::test::TempDir root("durable-store-impossible-length");
  const std::filesystem::path path = root.file("state.journal");
  Bytes header(kJournalHeaderBytes, std::byte{0});
  write_u32(header, 0, kJournalRecordMagic);
  write_u32(header, 4, (1u << 20) + 1u);  // one byte above the default bound
  write_u64(header, 8, 1);
  write_u32(header, 16, shuffle::fabric::crc32c(std::span<const std::byte>(header).first(16)));
  write_u32(header, 20, 0);
  write_file(path, header);

  DurableStore store(store_config(root.path()));
  REQUIRE_OK(store.open());
  REQUIRE_EQ(tail_text(store.recovery().tail), std::string{"Corrupt"});
  REQUIRE_EQ(store.recovery().bytes_discarded, std::uint64_t{kJournalHeaderBytes});
  REQUIRE_EQ(store.recovery().records_replayed, std::uint64_t{0});
}

SHUFFLE_TEST(durable_store, journal_bytes_and_appended_records_accounting) {
  shuffle::test::TempDir root("durable-store-accounting");
  const std::filesystem::path path = root.file("state.journal");
  const Bytes first = payload_for(1);
  const Bytes second = payload_for(2);
  const std::uint64_t expected_bytes =
      kJournalHeaderBytes * 2 + static_cast<std::uint64_t>(first.size() + second.size());
  {
    DurableStore store(store_config(root.path()));
    REQUIRE_OK(store.open());
    REQUIRE_EQ(store.journal_bytes(), std::uint64_t{0});
    REQUIRE_EQ(store.appended_records(), std::uint64_t{0});
    REQUIRE_OK(store.append(first));
    REQUIRE_EQ(store.journal_bytes(), kJournalHeaderBytes + static_cast<std::uint64_t>(first.size()));
    REQUIRE_EQ(store.appended_records(), std::uint64_t{1});
    REQUIRE_OK(store.append(second));
    REQUIRE_EQ(store.journal_bytes(), expected_bytes);
    REQUIRE_EQ(store.appended_records(), std::uint64_t{2});
    REQUIRE_EQ(file_size_of(path), expected_bytes);
  }
  {
    DurableStore store(store_config(root.path()));
    REQUIRE_OK(store.open());
    // Accounting is per open(): the durable bytes are the journal's, the
    // appended count is this session's.
    REQUIRE_EQ(store.journal_bytes(), expected_bytes);
    REQUIRE_EQ(store.appended_records(), std::uint64_t{0});
    REQUIRE_EQ(store.recovery().records_replayed, std::uint64_t{2});
    REQUIRE_EQ(file_size_of(path), expected_bytes);
    REQUIRE_OK(store.compact(state_for(1), 2));
    REQUIRE_EQ(store.journal_bytes(), std::uint64_t{0});
    REQUIRE_EQ(file_size_of(path), std::uint64_t{0});
    REQUIRE_EQ(store.last_sequence(), std::uint64_t{2});
  }
}

SHUFFLE_TEST(durable_store, journal_larger_than_four_snapshots_is_refused) {
  shuffle::test::TempDir root("durable-store-journal-bound");
  StoreConfig config = store_config(root.path());
  config.limits.max_state_bytes = 16;
  DurableStore store(config);
  REQUIRE_OK(store.open());
  // Sixteen bytes times four is the whole budget; the file below is larger.
  const Bytes record = record_image(1, payload_for(1));
  Bytes journal;
  while (journal.size() <= 64) {
    journal.insert(journal.end(), record.begin(), record.end());
  }
  write_file(root.file("state.journal"), journal);
  DurableStore bounded(config);
  REQUIRE_ERROR(bounded.open(), ErrorCode::StateTooLarge);
  REQUIRE_ERROR(bounded.append(payload_for(1)), ErrorCode::InvalidState);
}
