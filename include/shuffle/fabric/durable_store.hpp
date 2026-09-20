// Durable snapshot + journal store with crash-tolerant recovery.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// The store keeps one snapshot file and one append-only journal inside a single
// directory. Both formats are self-describing and checksummed end to end, so a
// half-written image is never believed: a snapshot that does not decode exactly
// is refused rather than partially applied, and a journal record is only
// materialised after its header has been bound-checked and its CRC verified.
//
// Every statement this store makes about "what is durable" comes from bytes
// that are already on disk. Nothing here consults wall-clock time, sleeps,
// guesses at a timeout or keeps mutable state outside the instance, because a
// recovery decision that depends on anything but the bytes themselves cannot be
// reproduced from a crash dump.
//
// Recovery is idempotent: opening the same directory twice reports the same
// record prefix. open() additionally repairs a torn journal tail by truncating
// the file to the last complete record -- bytes that were never acknowledged
// can never become valid again, and leaving them in place would make the next
// append land behind garbage.

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "shuffle/fabric/bytes.hpp"
#include "shuffle/fabric/error.hpp"

namespace shuffle::fabric {

// Why the journal scan stopped. The distinction matters to a caller: a torn
// tail is normal after a crash and costs only unacknowledged bytes, while a
// corrupt record means bytes that were already acknowledged disagree with
// their own checksums and the folded state must be revalidated from outside.
enum class JournalTailStatus : std::uint8_t {
  Clean = 0,     // the file ended exactly on a record boundary
  TornTail = 1,  // an incomplete trailing header or payload
  Corrupt = 2,   // a complete-looking record was rejected
  Missing = 3,   // no journal file exists
};

// What the most recent successful open() observed. A report is a description of
// decoded bytes, never a promise about the future.
struct RecoveryReport {
  bool snapshot_loaded{false};        // a snapshot decoded and was adopted
  bool snapshot_from_previous{false}; // it came from "<name>.prev", not the primary
  bool journal_missing{false};        // there was no journal file at all
  std::uint64_t snapshot_sequence{0}; // highest journal sequence folded in
  std::uint64_t records_replayed{0};  // journal records adopted into records()
  std::uint64_t records_skipped{0};   // duplicates of an already-applied sequence
  std::uint64_t bytes_discarded{0};   // file size minus the first unusable record
  JournalTailStatus tail{JournalTailStatus::Missing};
  std::uint64_t last_sequence{0};     // sequence the next append will follow
  bool revalidation_required{false};  // callers must re-derive state from a peer
};

struct StoreConfig {
  std::filesystem::path directory;
  std::string snapshot_name{"state.snapshot"};
  std::string journal_name{"state.journal"};
  Limits limits{};
};

// Single-writer durable store. Concurrent use of one instance is not supported;
// two instances on one directory are not arbitrated (the journal format detects
// the damage, it does not prevent it).
class DurableStore {
 public:
  explicit DurableStore(StoreConfig config);
  ~DurableStore();
  DurableStore(const DurableStore&) = delete;
  DurableStore& operator=(const DurableStore&) = delete;
  DurableStore(DurableStore&&) noexcept;
  DurableStore& operator=(DurableStore&&) noexcept;

  // Scans the directory and opens the store for appending. Any failure leaves
  // the store unopened: later append() calls report InvalidState rather than
  // writing into a directory whose contents were never understood.
  [[nodiscard]] Status open();

  // The report produced by the most recent successful open(). compact() does
  // not rewrite history: it changes the files, not what was recovered from
  // them. A moved-from store may only be destroyed or assigned to.
  [[nodiscard]] const RecoveryReport& recovery() const;

  // Journal records replayed by open(), in file order. Records skipped as
  // duplicates are not returned; their payloads were already folded in.
  [[nodiscard]] const std::vector<std::vector<std::byte>>& records() const;

  // Appends one record and returns its sequence only after the bytes and the
  // flush that makes them durable have both succeeded. A failed append consumes
  // no sequence and leaves the journal at its previous length.
  [[nodiscard]] Result<std::uint64_t> append(std::span<const std::byte> payload);

  // Installs "state" as the snapshot for every sequence up to upto_sequence and
  // empties the journal. The previous snapshot survives as "<name>.prev".
  // upto_sequence must lie in [snapshot_sequence, last_sequence]; callers that
  // pass less than last_sequence deliberately discard the records above it.
  [[nodiscard]] Status compact(std::span<const std::byte> state, std::uint64_t upto_sequence);

  [[nodiscard]] std::uint64_t last_sequence() const;
  // Durable length of the journal on disk, in bytes.
  [[nodiscard]] std::uint64_t journal_bytes() const;
  // Records appended successfully since the last open().
  [[nodiscard]] std::uint64_t appended_records() const;
  [[nodiscard]] const StoreConfig& config() const { return config_; }

  // Decodes one snapshot file with exactly the rules open() applies to the
  // primary snapshot. A path that does not exist is InvalidArgument, never a
  // crash and never an empty success.
  [[nodiscard]] static Result<std::vector<std::byte>> read_snapshot(const std::filesystem::path& path,
                                                                   const Limits& limits);

 private:
  struct Impl;

  StoreConfig config_;
  std::unique_ptr<Impl> impl_;
};

}  // namespace shuffle::fabric
