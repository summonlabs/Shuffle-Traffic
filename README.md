# Shuffle Fabric

Shuffle Fabric is an open-source, vendor-neutral C++20 runtime for governing
many-to-many shuffle traffic. It answers one question for a shuffle it owns:

> For this many-to-many data exchange, which partitions and transfers may move
> now, through what bounded concurrency and policy envelope, without turning
> fan-out/fan-in into uncontrolled fabric collapse?

It is a library, a coordinator service and three tools. It is not a
scheduler for arbitrary jobs, not a storage engine, not a routing layer and not
a congestion-control implementation; it decides *which* shuffle transfers may
move under *which* authority, and it proves what happened afterwards.

Everything here is deterministic where determinism is claimed: no wall-clock
time in library decisions, no randomness, no sleeps, no hidden retries.

## Boundary

Shuffle Fabric owns shuffle semantics:

- shuffle identity and generations, participant membership and incarnations;
- partition and chunk identity, manifests and content digests;
- deterministic many-to-many edge expansion (producer x partition x consumer);
- transfer waves under fan-out/fan-in ceilings and per-source,
  per-destination and global concurrency limits;
- transfer-attempt identity, retry classification and duplicate suppression;
- exactly-once authoritative completion per partition generation, plus
  explicit accounting of everything that did not complete;
- congestion/backpressure *intents* as advisory evidence;
- cancellation and its authority consequences;
- durable, versioned, integrity-checked state for all of the above;
- framed, versioned, integrity-checked transport for the management plane.

It deliberately does not own:

- the application that performs the shuffle, or what the bytes mean;
- generic routing, discovery, load balancing or service meshes;
- storage engines, filesystems or object stores (the durable store is a
  journal-and-snapshot for coordinator state, not a data store);
- generic congestion control (pressure readings are consumed as intents);
- arbitrary object movement outside shuffle semantics;
- cryptographic peer authentication (see Limitations).

## Repository layout

| Path | Contents |
| --- | --- |
| `include/shuffle/fabric/` | the public API: identities, codec, manifests, topology, policy, scheduler, ledger, coordinator, records, durable store, framing, sockets, service |
| `src/` | the implementation, one translation unit per module |
| `tools/` | `shuffle-fabric-coordinator` (authority service), `shuffle-fabric-node` (participant), `shuffle-fabric-cli` (inspection) |
| `examples/` | runnable examples that execute real supported paths |
| `tests/` | unit, property, concurrency, adversarial, protocol, persistence, scale and multiprocess proof surfaces |
| `downstream/` | an independent CMake project that consumes the installed package through `find_package` |
| `scripts/` | MSVC environment helper and the validation entry points |

## Core model and authority semantics

**Identities are types, not integers.** `ShuffleId`, `ShuffleGeneration`,
`PartitionId`, `PartitionGeneration`, `ChunkId`, `ProducerId`, `ConsumerId`,
`IncarnationId`, `WaveId`, `TransferAttemptId`, `TopologyGeneration`,
`PolicyGeneration` are distinct types, so a value of one kind cannot be
substituted for another by accident.

**Required edges are registration-scoped; authority is liveness-scoped.** A
required edge is one (partition, consumer) pair implied by a registered
consumer's selection. It stays required while the consumer is registered, even
if it is currently down, so a dead participant leaves *explicitly incomplete*
work rather than silently shrinking the requirement. Only withdrawal removes
the requirement. Authority — the ability to dispatch a transfer or to complete
one — requires an `Active` incarnation.

**Planning is not authority.** A wave plan is a proposal. The coordinator
registers authority for each grant it issues, and every grant carries the
shuffle generation, partition generation, topology generation, policy
generation, attempt identity and manifest digest it was issued under.

**Dispatch is not completion.** Only a verified completion, recorded durably,
satisfies an edge.

**Exactly-once per partition generation.** Completion is recorded once per
(partition, partition generation): a repeated completion is suppressed and can
never account bytes twice, and a divergent second claim about the same
generation (different manifest digest or different producing incarnation) is
refused with `DivergentCommit` rather than merged. Re-producing a partition
under a new generation retires the previous generation's completions from the
current edge set, so a stale completion cannot keep satisfying an edge that now
requires fresh content.

**Stale anything is refused, deterministically.** A superseded incarnation
cannot register work, publish a manifest, report pressure, or complete an
attempt. A coordinator restart removes all in-flight authority: attempts live
in memory only, and a restarted coordinator advances its durable epoch. A
pre-restart attempt is answered with `StaleAttempt`, never replayed.

**Established evidence can be re-derived, never resurrected.** After recovery,
participants return as `Suspect` and hold no authority until they re-register
with a fresh incarnation; congestion evidence is not persisted at all, because
a pressure reading is dynamic evidence and would be a lie after a restart.
Until every participant has revalidated, `revalidation_required()` is true and
the explanation says so.

### Completion path

```
plan a wave            -> grants carrying full generation identity
dispatch               -> an attempt is in flight (in-memory authority)
fetch + verify         -> chunk digests verified by the consumer
commit                 -> coordinator validates every binding, then:
                          validate -> make the record durable -> apply -> answer
```

An answer is never sent before the durability point it claims. If the durable
append fails, nothing is applied and the caller sees `PersistenceFailure`.

## Deterministic decisions and error codes

Every refusal is an `ErrorCode` with a stable numeric value; there are no bare
booleans. Codes are grouped: 1xx input/decoding, 2xx identity and authority,
3xx policy and lifecycle, 4xx accounting, 5xx retry, 6xx durable state, 7xx
transport and protocol, 8xx resources.

The vocabulary is stable and complete for the wire protocol. Every code is
classified and can be raised by some surface; a handful are raised only by
peers or by a caller of the public API rather than by the core itself, which
`docs/authority-model.md` records code by code.

Each code has one retry classification (`classify()`), which is what the retry
path acts on:

- **Retriable** — transport or payload damage: `ConnectionFailure`,
  `PeerUnavailable`, `ConnectionClosed`, `SessionClosed`, `IntegrityFailure`,
  `PayloadRejected`, `ChecksumMismatch`, `AmbiguousOutcome`. The edge stays
  pending and may be dispatched again when its retry budget allows.
- **Deferred** — scheduling pressure, not failure: `BackpressureActive`,
  `BackpressureUnknown`, `RetryDeferred`, `NoWorkAvailable`,
  `WaveLimitReached`, `ConcurrencyLimitExceeded`, `QueueFull`,
  `DuplicateFrame`.
- **Authority** — another attempt cannot repair it; it needs new authority:
  `AuthorityDenied`, `StaleGeneration`, `StaleIncarnation`, `StaleTopology`,
  `StalePolicy`, `StaleAttempt`, `IncarnationMismatch`,
  `GenerationMismatch`, `LateAuthority`, `AttemptSuperseded`, `Cancelled`,
  `ShuttingDown`, `PolicyDenied`, `FanOutCeilingExceeded`,
  `FanInCeilingExceeded`, `DivergentCommit` and friends.
- **Permanent** — everything else, including malformed input and corrupt state.

Deterministic explanations come from `Coordinator::explain()`: grouped counts
per code with a bounded, canonically ordered sample, plus the reasons for
partitions that were never produced, participants without authority, pending
post-restart revalidations and any accounting that fails to close. Two runs
over the same state render the same text.

## Persistence semantics

The durable store keeps a checksummed **snapshot** plus an append-only
**journal** in one directory:

- journal record = 24-byte header (magic, length, sequence, header CRC-32C,
  payload CRC-32C) followed by the payload;
- snapshot = 72-byte header (magic, version, flags, sequence, length, header
  CRC-32C, payload CRC-32C, payload SHA-256) followed by the payload;
- `append()` returns only after the bytes and the flush are on disk; a failed
  append rolls the file back and consumes no sequence;
- `compact()` writes a new snapshot through a temporary file and an atomic
  replace, keeps the previous snapshot as `.prev`, and empties the journal;
- recovery is strict: a snapshot that does not decode exactly is refused, and
  the primary snapshot falls back to `.prev` when it is usable; a journal scan
  stops at the first record it cannot believe, reports the discarded region as
  a torn tail or corruption, and never partially applies a record;
- a torn tail costs only bytes that were never acknowledged.

The coordinator replays durable records through the *same* decode-and-apply
path it uses live, into a scratch state that is adopted only when every step
succeeded, so a malformed file can never leave a partially recovered authority
behind.

## Process, epoch and generation behaviour

- A coordinator instance owns exactly one shuffle and one logical clock; ticks
  are advanced explicitly, never by wall-clock time, so replays are exact.
- Every boot advances the durable epoch. In-flight attempts from a previous
  epoch do not exist in the new one.
- Participants that were active before a restart come back as `Suspect`;
  they regain authority only by re-registering with a fresh incarnation, which
  supersedes the previous one immediately.
- Topology, policy and partition generations fence plans and manifests:
  publishing under a stale generation, completing with a superseded
  incarnation, or planning against an advanced topology are all refused.
- Cancellation is a durable state transition: it stops new dispatch, strips
  authority from work in flight, and refuses late completions with
  `Cancelled`. It is idempotent.

## Proof surfaces

Labels are exact: **REAL** means executed on this host and part of the green
suite; **SYNTHETIC** means modelled inputs stand in for hardware or networks
that are not present; **UNSUPPORTED** means not implemented or not validated
here.

| Surface | Label | What it covers |
| --- | --- | --- |
| `test_unit_core` | REAL | identities, canonical text, SHA-256 and CRC-32C against published vectors, codec round trips, truncation, trailing garbage, declared bounds, checked arithmetic |
| `test_unit_model` | REAL | manifest build/verify/encode/decode, deterministic ownership, interest interning, closed-form edge counts and fan bounds, policy validation and evaluation |
| `test_unit_ledger` | REAL | exactly-once accounting, duplicate suppression, divergence refusal, generation retirement, retention, tracked-edge bounds, deterministic encoding |
| `test_unit_schedule` | REAL | deterministic wave plans, global/per-source/per-destination concurrency, pressure deferral and hysteresis, retry eligibility and exhaustion, cancellation, bounded work per call |
| `test_unit_coordinator` | REAL | attempt-bound completion, chunk-evidence refusal, ownership and incarnation fencing, durability ordering (a refused append leaves no residue), re-entrancy refusal, congestion fencing, atomic recovery |
| `test_property_shuffle` | REAL | seeded randomized execution with a model: closure and byte accounting checked after every step, reproduction seed on failure |
| `test_concurrency_accounting` | REAL | many threads through the documented serialising wrapper, concurrent readers never observing torn accounting, repeated lifecycles, interleaved cancellation |
| `test_integration_shuffle` | REAL | service + durable store + coordinator together in one process: a full shuffle over loopback TCP, a snapshot-and-restart, revalidation, and completion under fresh incarnations |
| `test_protocol_frames` | REAL | byte-exact 40-byte header layout, every message type, all corruption and truncation cases, stream reassembly, replay guard |
| `test_service_loopback` | REAL | the coordinator service over loopback TCP: bounded sessions and worker pool, handshake identity binding, duplicate-sequence replay, refusal propagation, two servers at once, durable coordinator through the wire |
| `test_adversarial_frames` | REAL | randomized and mutated buffers judged against an independently written reference decoder, every single-byte corruption, real loopback socket behaviour |
| `test_durable_store` | REAL | journal and snapshot formats, atomic compaction, every header offset corrupted and every truncation length, torn tails, fallback snapshots |
| `test_durable_recovery` | REAL | real abrupt child-process death mid-append, deterministic torn tails, snapshot/journal overlap, repeated cycles |
| `test_coordinator_recovery` | REAL | coordinator + real durable store restart, epoch advance, stale-authority refusal, closure after re-registration |
| `test_scale_matrix` | REAL | 65k x 256 closed-form edge counts, cursor work independent of the edge space, exact accounting on large matrices, bounded pattern interning |
| multiprocess proof (scripts/validate-multiprocess.ps1) | REAL | one real coordinator service process plus independent producer and consumer processes over loopback TCP: exactly-once completion of every required edge, a producer that dies between publishing and delivery (explicit failures, no silent loss), and a corrupt payload that is never committed |
| ASan build | REAL | AddressSanitizer build of the suite on this toolchain |
| POSIX branches (sockets, durable store, process spawning) | UNSUPPORTED | written to the same contract, never compiled or executed on this host |
| Multi-host, RDMA, SmartNIC/DPU, NVLink, InfiniBand, RoCE, programmable switches | UNSUPPORTED | no such hardware here; nothing in this repository claims physical validation of it |

## Build

Requirements: CMake 3.25+, a C++20 compiler (MSVC 19.3x validated), Ninja
recommended. No third-party dependencies.

```powershell
# From a Visual Studio developer environment (or use scripts/msvc.ps1):
cmake -S . -B build/release -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build/release
```

Options: `SHUFFLE_FABRIC_BUILD_TOOLS` (default ON),
`SHUFFLE_FABRIC_BUILD_EXAMPLES` (ON), `SHUFFLE_FABRIC_BUILD_TESTS` (ON),
`SHUFFLE_FABRIC_WARNINGS_AS_ERRORS` (ON),
`SHUFFLE_FABRIC_SANITIZE_ADDRESS` (OFF). First-party code builds warning-free
under `/W4 /WX` (MSVC) or `-Wall -Wextra -Wpedantic -Werror` (GCC/Clang).

## Test

```powershell
ctest --test-dir build/release --output-on-failure
```

No test declares a timeout and none is passed on the command line: a hanging
test is treated as a defect. Tests synchronise on protocol responses, process
handles and durability points, never on sleeps.

The suite is re-run in three configurations by scripts/validate.ps1 (Release,
Debug, AddressSanitizer), and the distributed path has its own proof:
scripts/validate-multiprocess.ps1 starts a real coordinator service process plus
independent producer and consumer processes over loopback TCP and asserts
exactly-once completion, explicit failure when a producer dies between publishing
and delivery, and refusal of corrupt payloads. The released artifact is proved
again from a fresh clone by scripts/validate-fresh-clone.ps1.

## Install and consume

```powershell
cmake --install build/release --prefix build/install-prefix
```

The installed package exports `ShuffleFabric::fabric`. An independent
consumer lives in `downstream/shuffle-consumer-demo`:

```powershell
cmake -S downstream/shuffle-consumer-demo -B build/downstream \
      -DCMAKE_PREFIX_PATH=<absolute path to the install prefix>
cmake --build build/downstream
build/downstream/shuffle-consumer-demo
```

`scripts/validate-install.ps1` performs build, clean install, file
assertions, downstream configure/build/run and reports PASS/FAIL.
`scripts/validate.ps1` runs the whole matrix: Release, Debug, the
AddressSanitizer build (each configured, built and tested) and the install
validation.

## Tools

- `shuffle-fabric-coordinator` — opens or recovers one shuffle, serves the
  management protocol, prints `READY port=... epoch=... durable=...` and a
  final `STOP ...` summary. `--state-dir` makes it durable; `--volatile`
  states plainly that durability is not claimed.
- `shuffle-fabric-node` — a participant process that moves real payload bytes
  over loopback TCP, with documented fault-injection flags used by the
  multiprocess proof.
- `shuffle-fabric-cli` — inspection: `plan` (deterministic wave plan and
  closed-form edge count), `inspect-state` (recovery report and durable record
  summary), `progress`/`explain`/`status` against a running coordinator.

Examples in `examples/` execute real supported paths; see
`examples/README.md` for exact commands.

## Public API sketch

```cpp
#include "shuffle/fabric/coordinator.hpp"

// Authority: validate -> durable record -> apply -> answer.
shuffle::fabric::VolatileSink sink;              // or a journal-backed sink
shuffle::fabric::Coordinator coordinator{limits, &sink};
coordinator.open_shuffle(request);               // identity, policy, partitions
coordinator.register_producer(id, incarnation, endpoint);
coordinator.register_consumer(id, incarnation, endpoint, selection);
coordinator.publish_manifest(manifest);          // ownership + generation fenced

auto plan = coordinator.next_wave();             // bounded, deterministic
for (const auto& grant : plan.value().grants) {
  // fetch, verify chunk digests, assemble, then:
  coordinator.commit_transfer(request);          // exactly once per generation
}
auto progress = coordinator.progress();          // closes: completed+failed+incomplete == required
auto why      = coordinator.explain(8);          // deterministic reasons
```

## Limitations actually observed

- **No peer authentication.** Sessions bind an identity from the handshake
  envelope, and a completion must present the manifest and per-chunk evidence,
  but a hostile client that already knows the digests could claim a completion
  it did not perform. There is no key exchange, no signing and no transport
  encryption in 1.0.0.
- **One shuffle per coordinator instance**, and one coordinator per state
  directory. There is no replication, no leader election and no multi-raft.
- **The durable store is single-writer by contract.** Two live stores on one
  directory are not arbitrated by a lock; the format detects the damage rather
  than preventing it. There is no read-only open either: inspecting a store
  opens it for appending and may truncate a torn tail, so inspection belongs to
  a process that owns the directory.
- **Ownership changes require re-production.** If the active producer set
  changes, partitions map to new owners; content published by a superseded
  incarnation is not dispatchable, so the new owner must produce those
  partitions again.
- **POSIX branches are unvalidated** on this host (sockets, durable store
  file handling, process spawning). They are written to the same contract and
  marked in the code.
- **No physical network-fabric validation.** All network proofs run over
  loopback TCP on one host with synthetic payloads; nothing here was exercised
  on RDMA, InfiniBand, RoCE, NVLink, SmartNIC/DPU or programmable switches.
- **Bounded by policy, by design.** Edge counts, tracked edges, retained
  generations, patterns, participants, journal and snapshot sizes and session
  counts are all bounded; exceeding a bound is refused with a deterministic
  error instead of growing without limit.

## License

Apache License 2.0. Copyright 2026 Summon Software Labs. No telemetry transmission.
