# Shuffle Fabric architecture

Verified against the working tree at 2026-09-20 11:36. Source hashes (SHA-256,
first 12 hex digits) of the files this document leans on: `coordinator.hpp`
D41C50FB21A1, `coordinator.cpp` 227FB20CB2C6, `service.hpp` C9A6799D2DDA,
`service.cpp` 9AF7783F0AD5, `schedule.cpp` 115F3D3BE6B0, `ledger.hpp`
93D725727833, `topology.cpp` 69FD299E3D9B, `durable_store.cpp`
EF487FF85B60, `frame.cpp` 9B5BF7F5EA4D, `socket.cpp` 537385A771D4. `.hpp`
declarations are the contract; `.cpp` behaviour is what is described here, and
any line number cited is a line of that revision. The tree was being edited
while this document was written; re-check a hash before trusting a line number.

## 1. What the runtime is

A library (`shuffle_fabric`, alias `ShuffleFabric::fabric`), one service layer,
three process tools and two examples. The coordinator decides which shuffle
transfers may move under which authority and records what happened; it is not a
scheduler for arbitrary jobs, not a storage engine and not a data mover. The
only bulk bytes it knows about are digests and byte counts.

## 2. Module map

| Module | Public header | Implementation | Owns |
| --- | --- | --- | --- |
| Foundations | `bytes.hpp` | `bytes.cpp` | `Limits` (every externally influenced bound) and `Limits::validate()`; `checked_add`/`checked_mul`/`to_size`; byte/char conversion helpers |
| Errors | `error.hpp` | `error.cpp` | `ErrorCode` (stable numbers, grouped 1xx..8xx), `classify()` -> `RetryClass`, `Error`, `Status`, `Result<T>` |
| Identity | `identity.hpp` | header only | the tagged `Id<Tag, Rep>` types (`ShuffleId`, `PartitionId`, `ChunkId`, `ProducerId`, `ConsumerId`, `IncarnationId`, `WaveId`, `TransferAttemptId`, `TopologyGeneration`, `PolicyGeneration`, `PartitionGeneration`, `ShuffleGeneration`, `SessionId`, `CommitSequence`, `TickId`) and `LogicalClock` |
| Digests | `digest.hpp` | `digest.cpp` | `Digest` (32 bytes), hex round trip |
| Hashing | `hash.hpp` | `hash.cpp` | `sha256`, `crc32c` |
| Codec | `codec.hpp` | `codec.cpp` | canonical little-endian `ByteWriter`/`ByteReader`; `is_valid_utf8`; every decode refusal (`TruncatedInput`, `OversizedInput`, `LimitExceeded`, `MalformedInput`, `InvalidUnicode`, `TrailingGarbage`) |
| Edges | `edge.hpp` | `edge.cpp` | `EdgeKey`, `EdgePhase`, `EdgeStatus`, `EdgeOutcome`, `PressureReading`, `PartitionFacts`, the read-only views (`CompletionView`, `CongestionView`, `PartitionView`), `holds_authority()` and the vocabulary `to_string` functions |
| Policy | `policy.hpp` | `policy.cpp` | `PolicyEnvelope` (fan bounds, concurrency, wave limits, retry, congestion), `validate_policy()`, `evaluate_policy()`, canonical policy encoding |
| Manifests | `manifest.hpp` | `manifest.cpp` | `ChunkDescriptor`, `PartitionManifest`, `compute_partition_digest`, `compute_manifest_digest`, `validate_manifest`, `encode/decode_manifest`, `verify_chunk`, `verify_partition_content`, `build_manifest`, `synthetic_partition_payload` |
| Topology | `topology.hpp` | `topology.cpp` | participants, incarnations, `ParticipantState`, interned `PartitionSelection` patterns, deterministic ownership, fan-in/fan-out and closed-form edge counts, topology generations |
| Ledger | `ledger.hpp` | `ledger.cpp` | `CommitRequest`/`CommitReceipt`/`FailureRecord`, `PartitionCommitRecord`, `ProgressSnapshot`, `Explanation`; exactly-once accounting, divergence refusal, generation retirement, retention, the ledger's own durable encoding |
| Scheduler | `schedule.hpp` | `schedule.cpp` | `DispatchGrant`, `WavePlan`, `SchedulingEnvironment`, `WaveScheduler` (cursors, in-flight counters, pressure hysteresis, retry gating) |
| Durable vocabulary | `records.hpp` | `records.cpp` | `RecordKind`, `ShuffleState`, and the six durable records with their encoders/decoders |
| Durable store | `durable_store.hpp` | `durable_store.cpp` | the snapshot+journal files, `RecoveryReport`, `JournalTailStatus`, `DurableStore`; the only file-format code in the tree |
| Framing | `frame.hpp` | `frame.cpp` | the 40-byte header, `MessageType` table, `kMessageTypes`, `encode_frame`, `decode_header`, `decode_frame(_prefix)`, `FrameStream`, `ReplayGuard` |
| Sockets | `socket.hpp` | `socket.cpp` | `SocketRuntime`, `Socket`, `TcpListener`; every wait is an explicit millisecond budget |
| Coordinator | `coordinator.hpp` | `coordinator.cpp` | `DurableSink`, `VolatileSink`, `Coordinator`; all authority decisions and the durable ordering rule |
| Service | `service.hpp` | `service.cpp` | `SessionIdentity`, `ServerOptions`, `ClientOptions`, `ServerStats`, `CoordinatorServer`, `CoordinatorClient`; the wire payload codecs and the single serialising mutex |

Outside the library: `tools/` (three processes), `examples/` (two programs),
`tests/` (`support/` harness, `common/fabric_fixture.hpp`, seven test
directories), `downstream/shuffle-consumer-demo` (independent `find_package`
consumer), `scripts/` (`msvc.ps1`, `validate.ps1`, `validate-install.ps1`).

## 3. Dependency direction

Every header includes only headers below it; the graph is a DAG with no cycles.
Verified from the `#include "shuffle/fabric/..."` lines:

| Layer | Header | Includes (first-party only) |
| --- | --- | --- |
| 0 | `error.hpp`, `identity.hpp`, `digest.hpp`, `socket.hpp` | `error.hpp` only (or nothing) |
| 1 | `bytes.hpp` | `error` |
| 2 | `hash.hpp` | `digest` |
| 3 | `codec.hpp`; `edge.hpp` | `bytes, digest, error`; `digest, error, identity` |
| 4 | `policy.hpp`; `manifest.hpp` | `bytes, codec, error, identity`; `bytes, codec, digest, error, identity` |
| 5 | `topology.hpp` | `bytes, error, identity, policy` |
| 6 | `ledger.hpp`; `schedule.hpp` | `bytes, codec, edge, error, identity`; `bytes, edge, error, identity, policy, topology` |
| 7 | `records.hpp`; `durable_store.hpp`; `frame.hpp` | `codec, error, identity, ledger, manifest, policy, topology`; `bytes, error`; `bytes, error, identity` |
| 8 | `coordinator.hpp` | `codec, edge, ledger, manifest, policy, records, schedule, topology` |
| 9 | `service.hpp` | `coordinator, frame, manifest, socket` |

Two absences carry the architecture:

* `coordinator.hpp` includes neither `durable_store.hpp` nor `frame.hpp` nor
  `socket.hpp`. The authority core cannot name a file, a socket or a journal,
  so it cannot perform I/O even by accident: it can only hand a record to a
  `DurableSink*.`
* `service.hpp` includes no `durable_store.hpp`. The service serialises calls
  and frames bytes; the tool (`tools/shuffle-fabric-coordinator.cpp`) is what
  composes `DurableStore` + `DurableSink` + `Coordinator`.

Translation-unit dependencies mirror this: `policy.cpp` and `service.cpp` are
the only TUs that pull in `topology`/`ledger`/`schedule`/records beyond their
own header, and neither `service.cpp` nor `coordinator.cpp` includes a platform
header. Across `src/`, only `durable_store.cpp` (`windows.h`, `fcntl.h`,
`unistd.h`, `sys/stat.h`) and `socket.cpp` (`winsock2.h`, `sys/socket.h`,
`sys/select.h`) include OS headers.

## 4. Threading model

**The coordinator is single-threaded and not internally synchronised.** It has no
mutex, no atomic and no thread. Instead a private `Coordinator::Guard` sets
`inside_` for the duration of every public call; a nested call sees
`engaged() == false` and returns `InvalidState` ("re-entrant coordinator entry
refused") instead of recursing. `tests/unit/test_coordinator.cpp` drives this
through a sink that calls back during `persist()`.

**The service is the serialising wrapper.** `CoordinatorServer::Impl` owns
exactly one `std::mutex coordinator_mutex_`. Every coordinator call in
`service.cpp` happens inside a `std::lock_guard` on that mutex and nothing else
happens under it: per `src/service.cpp` the answer is written into a local
`std::vector<std::byte>` after the guard is destroyed, then framed and sent.

The remaining locks are separate and never held across the coordinator mutex:

| Lock | Protects |
| --- | --- |
| `coordinator_mutex_` | every `Coordinator` call |
| `state_mutex_` | the session registry, the pending queue, counters and lifecycle flags |
| `lifecycle_mutex_` | `start()`/`stop()` and the thread vector |
| `CoordinatorClient::Impl::mutex_` | one client's request/response exchange |

Shape of a served connection: one accept thread pushes sessions into a queue; a
worker pool takes one session per worker and serves it for that connection's
lifetime; a session owns its `FrameStream`, its `ReplayGuard`, a fixed 32-entry
answer cache and one 64 KiB read buffer. Because a worker serves one connection
at a time, the pool size is a real concurrency bound, and the sizing rule makes
admitted sessions servable: `max_sessions_ = clamp(max_sessions, 1, 1024)` and
`workers_ = clamp(max(worker_threads, max_sessions_), 1, 1024)`, with the source
comment stating why — "Sizing it below max_sessions would leave admitted sessions
queued and their clients waiting on answers that never come". `CoordinatorServer::stop()` closes the listener, shuts down and closes
every session socket, notifies the queue condition variable and joins the
workers; it is idempotent and safe to call from a session worker (that thread
detaches itself rather than joining itself).

`DurableStore` is single-writer by contract and has no internal lock: one live
store per directory, and concurrent use of one instance is not supported
(`durable_store.hpp`). Two live writers are detected by the format, not
prevented by a lock.

## 5. Where determinism comes from

| Source | Mechanism |
| --- | --- |
| Identity | typed `Id<Tag, Rep>` values; a partition id cannot be passed where a chunk id is required; canonical decimal text (`Id::parse`) |
| Time | `LogicalClock` in `identity.hpp`: `now()`, `advance()`, `reset()`. Ticks move only through `Coordinator::advance_tick()` or through replay of a recorded `at`/`ready_at`/`opened_at` tick, which resets the clock to at-or-beyond that value. No library decision reads a wall clock: `<chrono>` appears only in `src/socket.cpp`, for wait deadlines (`SteadyClock::now()`), which decide when a shutdown flag is re-checked and never whether an operation succeeded |
| Randomness | none: no `<random>`, no `rand()`, no time-derived seed anywhere in `src/` |
| Canonical order | `Topology` keeps producers and consumers as ascending vectors, sorts pattern indices by canonical selection key (`pattern_order_`), keeps each pattern's consumer list ascending, and keeps completed-consumer lists ascending and unique; `snapshot_payload()` sorts manifest keys; `CompletionLedger::encode` sorts partition/failure/generation keys; `write_progress_snapshot` is a fixed field order; `Explanation` groups by code in a `std::map` and `build_explanation` sorts entries by (code, subject); `evaluate_policy` sorts violations the same way; `std::unordered_map` iteration order is never observable |
| Closed form | `PartitionSelection::selected_count`, `Topology::planned_edge_count` (checked multiply-add over interned patterns), `Topology::fan_in`, `Topology::fan_out`, `Topology::covering_consumer_count`. The N x M edge matrix is never materialised |
| Bounded work | `WaveLimits::max_edges_examined_per_wave` bounds every `next_wave()` call step by step (`plan.examined`), `max_grants_per_wave` bounds a wave, and a full pass that adds no grant stops the wave early. `Limits` bounds frames, collections, strings, manifests, tracked edges, retained generations, journal records and state size |
| Explicit refusal | every refusal is an `ErrorCode`; no bare booleans, no exceptions in control flow (`Result::value()` throws `ResultAccessError` only on a programming error) |

Two runs over the same state render the same text because every ordering above is
total and every counter is derived, not sampled.

## 6. Why the decision core performs no I/O

`coordinator.hpp` states it directly:

> The coordinator performs no I/O of its own: durable records are handed to a
> DurableSink, which the service layer backs with the journal store for a real
> deployment and with VolatileSink only where durability is explicitly not
> claimed (reported through durable()).

The reasons are visible in the code and are structural, not stylistic:

1. **One decode-and-apply path for live operation and recovery.** `persist()`
   hands an encoded record to the sink, and `apply_record()` is the only thing
   that turns record bytes into state. Recovery replays durable bytes through
   `apply_record()` into a scratch coordinator, so replay cannot drift from live
   behaviour. If the coordinator opened the journal itself, replay would be a
   second code path.
2. **Durability failure is a value, not a crash.** `DurableSink::persist` is
   documented as "Must return only once the record is durable, or report why it
   is not"; a failure becomes `PersistenceFailure` returned to the caller with
   nothing applied. `commit_record()` is exactly `persist(writer.data())`
   followed by `apply_record(writer.data())`.
3. **Substitutability is testable.** `VolatileSink` (accepts, claims no
   durability; the coordinator then reports `durable() == false`),
   `RecordingSink`, a failing sink and a callback sink are all ordinary
   subclasses, which is how the durability-ordering and re-entrancy proofs are
   written without touching a filesystem.
4. **The composition point is the process, not the library.** The tool wires
   `DurableStore` to a sink and passes `&sink` to the coordinator; the store
   stays single-writer by contract, and the coordinator stays free of handles,
   paths and flush semantics.

This is the reason the library's I/O is confined to two translation units:
`durable_store.cpp` (files) and `socket.cpp` (sockets). Everything between them
is a pure function of its inputs.

## 7. Public entry points

### 7.1 Coordinator (`coordinator.hpp`)

| Entry point | What it does |
| --- | --- |
| `Coordinator(limits, DurableSink*)` | binds bounds and the durability point; `durable()` is true only when the sink claims durability |
| `open_shuffle(ShuffleOpenRequest)` | validates identity, partition count against `max_partitions` and the policy envelope (including that the envelope names this shuffle generation); persists an epoch advance then an `Open` state record |
| `recover(snapshot, journal, history_incomplete)` | replays snapshot records then journal records into a scratch coordinator, demotes every authoritative participant to `Suspect`, persists a new epoch and adopts the scratch state only if every step succeeded |
| `register_producer/register_consumer` | idempotent for an identical (id, incarnation, endpoint[, selection]) and otherwise supersedes the previous incarnation; returns the topology generation, the incarnation and whether a previous one was superseded |
| `set_participant_state` | moves a participant incarnation between `Pending/Active/Suspect/Failed/Withdrawn`; a stale incarnation is refused |
| `publish_manifest(PartitionManifest)` | validates structure, ownership by the current active producer set, shuffle generation, incarnation and divergence; stamps the current topology generation and records it durably |
| `next_wave()` | configures the scheduler when the topology or policy generation moved, plans a bounded wave and registers a `DispatchGrant` per grant in `grants_` |
| `commit_transfer(CommitRequest)` | validates the attempt and every binding, verifies the content evidence against the accepted manifest, makes the record durable, applies it to the ledger and answers |
| `report_failure(attempt, ErrorCode)` | classifies the code, computes permanence from the retry budget and authority class, records the failure durably and resolves the attempt |
| `update_congestion(CongestionIntent)` | validates the reporter's current incarnation, the policy generation and the observation tick, then stores the reading in memory only |
| `cancel_shuffle(ErrorCode)` | durable state transition; strips in-flight authority and refuses further dispatch. Idempotent |
| `advance_tick()` | the only way time moves |
| `manifest_of(partition)` | the accepted manifest, or an explicit `PartitionNotProduced` |
| `progress()`, `explain(max_samples)`, `status()` | derived inspection: closure, deterministic refusal groups, coordinator status |
| `snapshot_payload()` | encodes the record list plus the ledger's own encoding for compaction |
| accessors | `durable()`, `state()`, `epoch()`, `revalidation_required()`, `history_incomplete()`, `pending_revalidation()`, `limits()` |

### 7.2 Store, transport, service

| Entry point | What it does |
| --- | --- |
| `DurableStore::open/append/compact/recovery/records/last_sequence/journal_bytes/appended_records/read_snapshot` | snapshot+journal persistence with strict decode, torn-tail repair on open and atomic compaction |
| `encode_frame/decode_header/decode_frame(_prefix)` | the framed wire format, one frame per call |
| `FrameStream::feed/next` | bounded incremental stream reader |
| `ReplayGuard::accept` | per-direction duplicate suppression |
| `CoordinatorServer::start/stop/running/port/stats` | binds a listener, runs the worker pool, reports counters |
| `CoordinatorClient::connect/close` and the typed request methods | one request frame out, one reply frame in, with the reply checked against the request sequence, session id and expected type |
| `Topology`, `CompletionLedger`, `WaveScheduler`, `validate_policy`/`evaluate_policy`, manifesto helpers | the model and planning layer, usable without a coordinator (this is what `shuffle-fabric-cli plan` and the scheduler/ledger/model tests do) |

### 7.3 Processes

| Entry point | What it does |
| --- | --- |
| `tools/shuffle-fabric-coordinator` | opens or recovers one shuffle, wires the `DurableStore` sink (with periodic compaction, `--compact-every`), serves the management protocol and prints `READY ...` / `STOP ...` |
| `tools/shuffle-fabric-node` | one participant: management client plus its own `ChunkFetch` data-plane listener, with documented fault-injection flags |
| `tools/shuffle-fabric-cli` | `plan` (real `WaveScheduler` over a real `Topology`), `inspect-state` (real store open), `progress`/`explain`/`status` (real client sessions) |
| `examples/in_process_shuffle` | the authority path without a socket, run volatile and then through a real store |
| `examples/loopback_service` | the same model over loopback TCP with one server and two client threads |
