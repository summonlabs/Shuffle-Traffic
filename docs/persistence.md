# Shuffle Fabric persistence

Verified against the working tree at 2026-09-20 11:25. Source hashes (SHA-256,
first 12 hex digits): `durable_store.hpp` 2C2746F6DF06, `durable_store.cpp`
EF487FF85B60, `records.hpp` B7AEF23A7CA0, `records.cpp` 2345A091AA8C,
`coordinator.cpp` 227FB20CB2C6, `ledger.cpp` B38A7B0ABD8C, `codec.cpp`
7190C86992EE, `bytes.cpp` F033EFCA0789.

## 1. Files

| Path | Role |
| --- | --- |
| `<dir>/state.snapshot` | the primary snapshot (`StoreConfig::snapshot_name`) |
| `<dir>/state.snapshot.prev` | the previous snapshot, kept by compaction |
| `<dir>/state.snapshot.tmp` | the staging file compaction writes before an atomic replace |
| `<dir>/state.journal` | the append-only journal (`StoreConfig::journal_name`) |

Both formats are self-describing and checksummed end to end. Every statement the
store makes about "what is durable" comes from bytes already on disk: nothing
consults wall-clock time, sleeps, guesses at a timeout or keeps mutable state
outside the instance, because a recovery decision that depends on anything but
the bytes cannot be reproduced from a crash dump.

Geometry is frozen contract, not tuning (`src/durable_store.cpp`):

```cpp
constexpr std::size_t kSnapshotHeaderBytes = 72;
constexpr std::size_t kJournalHeaderBytes = 24;
constexpr std::uint32_t kSnapshotVersion = 1;
constexpr std::uint32_t kJournalRecordMagic = 0x4E524A31u;
```

## 2. Journal record layout

Header, 24 bytes, little-endian:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 4 | record magic `0x4E524A31` (on disk the bytes are `31 4A 52 4E`, i.e. "1JRN") |
| 4 | 4 | payload length |
| 8 | 8 | sequence, starting at 1 |
| 16 | 4 | header CRC-32C over bytes [0, 16) |
| 20 | 4 | payload CRC-32C over the payload |
| 24 | `payload_length` | payload: one encoded durable record (section 6) |

Independent confirmation: `tests/persistence/test_durable_store.cpp` builds the
same image with its own helpers (`record_image`), so a format change that keeps
the API fails the suite.

## 3. Snapshot layout

Header, 72 bytes, little-endian:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | magic bytes `53 46 53 4E 41 50 30 31` ("SFSNAP01") |
| 8 | 4 | version, 1 |
| 12 | 4 | flags, must be 0 |
| 16 | 8 | sequence: the highest journal sequence folded into this snapshot |
| 24 | 8 | payload length |
| 32 | 4 | header CRC-32C over bytes [0, 32) |
| 36 | 4 | payload CRC-32C |
| 40 | 32 | SHA-256 of the payload |
| 72 | `payload_length` | payload: the coordinator's snapshot payload (section 5) |

Decode order and refusals (`decode_snapshot_file`):

| Step | Refusal |
| --- | --- |
| open for reading | missing path -> `InvalidArgument`; other failure -> `PersistenceFailure` |
| size >= 72 | `TruncatedInput` |
| read header | `TruncatedInput` if the read is short |
| magic | `MalformedInput` |
| version == 1 | `UnsupportedVersion` |
| flags == 0 | `MalformedInput` |
| `payload_length <= max_state_bytes` — checked **before** anything is allocated or read | `StateTooLarge` |
| header CRC over [0, 32) | `ChecksumMismatch` |
| file size == 72 + payload_length exactly | `TruncatedInput` when shorter, `TrailingGarbage` when longer |
| read payload | `TruncatedInput` |
| payload CRC-32C | `ChecksumMismatch` |
| payload SHA-256 | `ChecksumMismatch` |

`DurableStore::read_snapshot(path, limits)` applies exactly these rules to one
file; a path that does not exist is `InvalidArgument`, never a crash and never an
empty success.

## 4. Append durability, rollback and compaction

**`append(payload)`** returns the record's sequence only after the header, the
payload and a flush (`FlushFileBuffers` on Windows, `fsync` elsewhere) have all
succeeded.

| Condition | Result |
| --- | --- |
| store not open | `InvalidState` |
| an earlier failed append could not be rolled back | `PersistenceFailure` ("journal writer is unusable after an earlier failure") |
| payload above `max_journal_record_bytes` | `OversizedInput` |
| payload above 0xFFFFFFFF | `OversizedInput` |
| sequence arithmetic overflows | `IntegerOverflow` |
| write or flush fails | the file is truncated back to the pre-append length and flushed; the call reports `PersistenceFailure`, **consumes no sequence** and leaves the journal at its previous length |

The journal writer is opened lazily, so a store that is opened and never appended
to creates no journal file — "no journal" keeps meaning "no journal" across
restarts. On Windows the file is opened with maximal sharing
(`FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE`) so inspection tools and
a second reader can look at a live journal; the format, not the filesystem, is
what detects two writers.

**`compact(state, upto_sequence)`** installs a new snapshot and empties the
journal:

1. require open (`InvalidState`), `state.size() <= max_state_bytes`
   (`StateTooLarge`), and `snapshot_sequence <= upto_sequence <= last_sequence`
   (`InvalidArgument`). Passing less than `last_sequence` deliberately discards
   the records above it.
2. write header + state to `<snapshot>.tmp`, flush, close. Any failure removes
   the temporary and leaves the installed snapshot exactly where it was.
3. atomically replace the primary snapshot, keeping the replaced bytes as
   `.prev`:
   * Windows with an existing primary: a stale `.prev` is removed first, then
     `ReplaceFileW(destination, source, backup, REPLACEFILE_IGNORE_MERGE_ERRORS)`;
   * Windows without an existing primary: `MoveFileExW(...,
     MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)`;
   * POSIX: two `rename()` calls. The source marks this branch
     `// UNVALIDATED: no POSIX host has executed this branch in this repository.`
   Failure is `AtomicReplaceFailed` (604) and the temporary is removed.
4. reopen the journal for append and truncate it to 0. If that fails the call
   reports `PersistenceFailure` ("journal could not be emptied after the snapshot
   was installed") — the snapshot is already installed at that point.
5. `snapshot_sequence = upto_sequence`, `journal_bytes = 0`.

`compact()` does not rewrite history: the `RecoveryReport` keeps describing what
the last `open()` recovered.

## 5. The coordinator's snapshot payload

`Coordinator::snapshot_payload()` encodes:

| Field | Encoding |
| --- | --- |
| payload version | `u32` = 1 (`kSnapshotPayloadVersion`) |
| record count | `u32` |
| records | that many byte strings, each an encoded durable record |
| ledger state | one byte string, the `CompletionLedger` encoding |

The record list is, in order: one `EpochAdvanced` record carrying the current
epoch and the current tick; one `ShuffleState` record when the state is not
`Closed`; one `Participant` record per producer then per consumer (endpoint
included, and for consumers the selection taken from its interned pattern); one
`Manifest` record per accepted manifest in ascending partition id order.
Commit and failure history is not written as separate records here: it is inside
the ledger encoding.

`CompletionLedger::encode` field order:

```
u64 shuffle, u64 shuffle_generation, u32 partition_count, u64 next_sequence,
u64 bytes_committed, u64 duplicate_commits, u64 attempted_bytes,
u64 retriable_failures, u64 permanent_failures, u64 authority_refusals,
u32 partition_record_count,
  per record: u64 partition, u64 partition_generation, u64 shuffle,
              u64 shuffle_generation, u64 producer, u64 producer_incarnation,
              digest manifest_digest, u64 total_bytes, u64 topology_generation,
              u64 sequence, u64 committed_at, u32 duplicate_commits,
              u32 consumer_count, u64 consumer[consumer_count]
u32 failure_record_count,
  per record: u64 partition, u64 partition_generation, u64 consumer, u16 code,
              u32 attempts, u64 recorded_at, u64 ready_at, u8 permanent
u32 generation_record_count,
  per record: u64 partition, u64 latest_partition_generation
```

Decoding is strict and never partially applies: records are validated (partition
inside the shuffle, consumer lists sorted and unique, no duplicate keys, no
unknown error code, counts inside `max_tracked_partitions`/`max_tracked_edges`/
`max_collection_items`), and the decoded value replaces the live one only after
every field validated. Derived counters — `completed_edges_`,
`committed_partitions_`, `permanent_failures_`, `current_permanent_failures_`,
`failed_partitions_` — are **recomputed** from the records and the
latest-generation table rather than trusted from the bytes, so the current edge
set has exactly one definition.

## 6. Durable record vocabulary

Every mutation is one of six records (`records.hpp`), and the live path and the
replay path use the same decode-then-apply function. Congestion evidence is
deliberately absent from this vocabulary.

| Kind | Tag | Payload after the tag |
| --- | --- | --- |
| `EpochAdvanced` | 1 | `u64 epoch, u64 opened_at` |
| `ShuffleState` | 2 | `u64 shuffle, u64 generation, u32 partition_count, u16 state, u16 reason, u64 at`, then the policy envelope |
| `Participant` | 3 | `u8 kind, u64 id, u64 incarnation, u8 state, string endpoint, selection, u64 at`; selection = `u8 kind, u64 begin, u64 end, u32 list_count, u64[]` |
| `Manifest` | 4 | byte string: the encoded manifest |
| `Commit` | 5 | `CommitRequest` (identical field order to the wire form), then `u64 at` |
| `Failure` | 6 | `u64 partition, u64 partition_generation, u64 consumer, u16 code, u8 permanent, u32 attempts, u64 recorded_at, u64 ready_at` |

Decoders refuse impossible content with `StateImpossible` (603): unknown record
kind, unknown selection kind, non-canonical selection list, unknown participant
kind or state, unknown error code, an `Open` state that carries a failure reason,
a commit whose `edge.consumer` disagrees with its `consumer` field. Corruption
that breaks canonical form is `StateCorrupt` (601).

## 7. The recovery algorithm

`DurableStore::open()`:

1. Reset every field, require a non-empty directory, create it if needed and
   require that it is a directory (`PersistenceFailure` otherwise).
2. Decode `state.snapshot`. On success: `snapshot_loaded = true`,
   `snapshot_sequence` = its sequence.
3. Otherwise decode `state.snapshot.prev`. On success: adopt it and report
   `snapshot_from_previous = true`, because the state may be behind what was
   durable.
4. If neither file decoded and at least one of the two errors is **not**
   `InvalidArgument` (that is, a file exists but is unusable), return the primary
   error and leave the store unopened. Only "neither file exists" is a legitimate
   fresh start. Leaving the store unopened is what stops a caller from appending
   onto state nobody understood: later `append()` calls report `InvalidState`.
5. Scan the journal (`scan_journal`), streaming through a fixed 64 KiB buffer so
   resident cost does not grow with the file. A payload is allocated only after
   its header passed every bound check.

| Journal scan outcome | Classification |
| --- | --- |
| no journal file | `Missing`; `journal_missing = true`; not an error |
| file size above `4 * max_state_bytes` | refused outright with `StateTooLarge` |
| file ends exactly on a record boundary | `Clean` |
| header starts but the file ends inside it | `TornTail` |
| `payload_length` read but the payload is not all there | `TornTail` |
| magic mismatch | `Corrupt` |
| `payload_length > max_journal_record_bytes` | `Corrupt` |
| header CRC mismatch | `Corrupt` |
| payload CRC mismatch | `Corrupt` |
| sequence lower than the previous record's | `Corrupt` |
| a repeat of the last applied sequence, or a sequence the snapshot already folded in | skipped, not an error |

The scan stops at the first record it cannot believe; `bytes_discarded` is the
file size minus the offset where that record starts, and no record is ever
partially applied. Ordering is checked before duplication on purpose: a sequence
moving backwards means the file was rewritten or two writers interleaved, and
neither can be repaired by skipping, while a repeated sequence is exactly what an
interrupted writer leaves behind.

6. `last_sequence = max(snapshot_sequence, last sequence seen)`;
   `revalidation_required = !snapshot_loaded || snapshot_from_previous || tail is
   TornTail or Corrupt`; `journal_bytes` = end of the last complete valid record.
7. If a journal exists, reopen it for append at `journal_bytes` (which truncates
   anything beyond it). A torn tail is therefore **repaired on open**, because
   bytes that were never acknowledged can never become valid again and leaving
   them would make the next append land behind garbage. `records()` returns only
   the non-duplicate payloads, in file order.

Recovery is idempotent: opening the same directory twice reports the same record
prefix. The report is a description of decoded bytes, never a promise about the
future.

**Coordinator-side recovery** (`Coordinator::recover`): a fresh coordinator
(state must be `Closed`, else `InvalidState`) replays the snapshot payload and
then the journal through `apply_record` — the same decode-and-apply path used
live — into a scratch coordinator. Snapshot payload version != 1 is
`UnsupportedVersion`; a truncated payload is `TruncatedInput`; trailing bytes
are `TrailingGarbage`; impossible content is `StateImpossible`. If every step
succeeded, every participant that held authority is demoted to `Suspect`,
`pending_revalidation_` is set, a new `EpochAdvanced` record is made durable
through the real sink, and only then is the scratch state adopted. Any failure
returns the error with the coordinator still `Closed` and no partially recovered
authority behind it (`test_unit_coordinator.snapshot_payload_rejects_corruption`
asserts the state after a refused recovery).

## 8. What recovery refuses outright

* A snapshot that exists but does not decode exactly (magic, version, flags,
  length, header CRC, payload CRC, payload SHA-256, exact file size) — refused,
  unless a usable `.prev` exists, in which case the previous snapshot is adopted
  and `snapshot_from_previous` is reported.
* A journal larger than four maximal snapshots.
* Any journal record whose header, CRC, length bound or sequence ordering is
  inconsistent — the region from that record onwards is discarded and reported.
* Any record whose decoded content is impossible or corrupt — the whole replay
  fails; nothing is adopted.
* A compaction sequence outside `[snapshot_sequence, last_sequence]`.

## 9. Bounds

| Bound | Default | Applies to |
| --- | --- | --- |
| `max_state_bytes` | 256 MiB (1<<28) | snapshot payload; compaction state |
| `max_journal_record_bytes` | 1 MiB (1<<20) | one journal payload; also enforced during the scan |
| `4 * max_state_bytes` | 1 GiB | total journal file size before the scan refuses |
| `max_collection_items` | 262144 | decoded collections, including a commit's consumer list |
| `max_tracked_partitions` | 1<<22 | partition records in a decoded ledger |
| `max_tracked_edges` | 1<<26 | failure records in a decoded ledger |
| `max_partitions` | 1<<22 | latest-generation records in a decoded ledger |
| `max_message_payload_bytes` | 8 MiB | a manifest byte string inside a `Manifest` record |
| read buffer / transfer chunk | 64 KiB / 1 MiB | recovery scan and file I/O granularity |

`Limits::validate()` enforces the internal consistency of the set, including
`max_journal_record_bytes <= max_state_bytes`, `max_tracked_partitions <=
max_partitions` and the frame/message/allocation ordering. Bounds are used
literally: a bound of 0 means "nothing of this kind is accepted".

## 10. The single-writer contract, stated honestly

* `DurableStore` is single-writer by contract. Concurrent use of one instance is
  not supported, and nothing serialises it: there is no lock, no lock file and no
  ownership marker.
* Two live stores on one directory are **not arbitrated**. No lock prevents it;
  the journal format detects the damage afterwards — a sequence that moves
  backwards is classified `Corrupt` and the tail from that record on is
  discarded, with `revalidation_required` reported.
* No write is acknowledged before its flush succeeded, so a torn tail costs only
  bytes that were never acknowledged. That is the whole claim: acknowledged
  records survive, unacknowledged bytes may be lost.
* `compact()` while another writer is live can lose that writer's records: the
  journal is truncated to zero after the snapshot is installed.
* There is no read-only open. `open()` repairs a torn tail by truncating, which is
  why `shuffle-fabric-cli inspect-state` prints `bytes_discarded` and says so
  explicitly instead of claiming a read-only pass.
* Nothing in the store consults wall-clock time, sleeps or retries: a failure is
  reported as an `ErrorCode`, and the caller decides what to do next.
