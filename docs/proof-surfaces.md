# Shuffle Fabric proof surfaces

Verified against the working tree at 2026-09-20 11:38. Source hashes (SHA-256,
first 12 hex digits): `tests/CMakeLists.txt` F4104BB501AE,
`tests/support/test_support.cpp` 44A5990665A0, `tests/support/rng.hpp`
633E2AF25636, the root `CMakeLists.txt` 97E1882A2C28, `scripts/validate.ps1`
234035D8520F, `src/service.cpp` 9AF7783F0AD5. The tree was being edited while
this document was written — `tests/integration/test_integration_shuffle.cpp`
was observed as F9394C601589, then 37E1F3BFF44E, then 109EB0D285B8 — so section
1 stamps every observation with the revision it was run against, and the row for
that target states what was last observed rather than a standing verdict.

Labels follow README.md exactly: **REAL** = executed on this host and part of the
green suite; **SYNTHETIC** = modelled inputs stand in for hardware or networks
that are not present; **UNSUPPORTED** = not implemented or not validated here.

## 1. How to run the suite

```powershell
# the whole registered suite, no timeout anywhere
ctest --test-dir build/release --output-on-failure

# through the MSVC environment, exactly as documented in README.md
.\scripts\msvc.ps1 -Command 'ctest --test-dir build/release --output-on-failure'

# the full matrix: Release, Debug, AddressSanitizer, install + downstream consumer
powershell -File scripts\validate.ps1
```

Each target is a single CTest case that runs one binary; the binary also takes
its own flags:

```powershell
build/release/tests/test_unit_core.exe --list                 # case names
build/release/tests/test_unit_core.exe --seed=12345 --filter=codec
build/release/tests/test_unit_core.exe --seed=12345 --filter=codec.checked_arithmetic_never_wraps
```

Observed on this host, in order. The tree was being edited concurrently, so
each run is stamped with the revision it used.

| Run | Revision | Verdict |
| --- | --- | --- |
| A | `tests/CMakeLists.txt` EE52D164A80D (13 targets) | `100% tests passed, 0 tests failed out of 13`, total test time 5.30 sec |
| B | `tests/CMakeLists.txt` F4104BB501AE, which adds a 14th target (`test_integration_shuffle`) | `93% tests passed, 1 tests failed out of 14`, total test time 18.48 sec |
| C | `test_integration_shuffle.cpp` rebuilt after `src/service.cpp` changed to 9AF7783F0AD5 (the server's worker-pool sizing) | that target still red, at a later assertion |
| D | `test_integration_shuffle.cpp` 37E1F3BFF44E, rebuilt (the file was edited again to 109EB0D285B8 after this run and was not re-run) | that target still red, at a third assertion |

The three distinct failures observed for `test_integration_shuffle`, verbatim:

```
test_integration_shuffle.cpp:187: expected success from second_producer_client.connect(
    make_client_options(server.port(), ParticipantKind::Producer, 2, 1))
    but got ConnectionFailure: no reply arrived within the bounded wait
reproduce: --seed=484907840942700 --filter=integration.service_durable_store_and_restart_complete_a_shuffle

test_integration_shuffle.cpp:224: expected status.value().active_producers == 1u but 2 != 1
reproduce: --seed=484975457969900 --filter=integration.service_durable_store_and_restart_complete_a_shuffle

test_integration_shuffle.cpp:313: requirement failed: finished
reproduce: --seed=485094906522000 --filter=integration.service_durable_store_and_restart_complete_a_shuffle
```

In every one of those runs, all 13 targets that README.md's proof-surface table
lists passed, and the second case of the new target
(`integration.service_refuses_wrong_identity_and_keeps_serving`) passed as well.
`build/validate-logs/asan-ctest.log` records `100% tests passed, 0 tests failed
out of 13` for the AddressSanitizer configuration and
`build/validate-logs/release-ctest.log` the same for the release configuration;
both logs predate the 14th target.

## 2. The CTest surface

`tests/CMakeLists.txt` registers exactly the 14 targets below through
`shuffle_fabric_add_test(name source)`, each linking `ShuffleFabric::fabric` and
the shared support objects (`support/test_main.cpp`, `support/test_support.cpp`,
`support/temp_dir.cpp`). Case counts are what each built binary reports for
`--list`; the 14 targets hold 158 cases in total.

| CTest name | Implemented by | Cases | What it proves | Label |
| --- | --- | --- | --- | --- |
| `test_unit_core` | `tests/unit/test_core.cpp` | 16 | Tagged identities are distinct types with canonical decimal text and monotone generations; digest hex round-trips; SHA-256 and CRC-32C match published vectors; the codec round-trips every primitive, rejects truncation and trailing bytes, enforces declared bounds, validates UTF-8 and never wraps checked arithmetic; `Limits::validate()` rejects inconsistent bound sets; `classify()` is total and stable; `version_string()` reports 1.0.0 | REAL |
| `test_unit_model` | `tests/unit/test_model.cpp` | 14 | Manifest build/split/verify, digests binding identity as well as content, encode/decode round trip and rejection; deterministic round-robin ownership; a topology generation bump on every mutation; interest interning with canonical keys; planned edge counts equal a brute-force expansion; fan-in/fan-out in closed form; selection validation and clamping; policy structural validation, evaluation and encoding | REAL |
| `test_unit_ledger` | `tests/unit/test_ledger.cpp` | 12 | Bytes accounted exactly once; duplicate completions suppressed without double accounting; a second consumer completes its edge without re-accounting bytes; divergent commits refused and never merged; integrity evidence required; a superseded generation cannot commit late; failures recorded and classified; closure over mixed outcomes; retention purges old generations explicitly; the tracked-edge bound is enforced; encoding is deterministic and round-trips; decode refuses corruption without partial application | REAL |
| `test_unit_schedule` | `tests/unit/test_schedule.cpp` | 13 | Identical inputs produce identical plans (compared as rendered text); global, per-source and per-destination concurrency bounds; every required edge granted exactly once until resolved; `resolve` refuses attempts it never issued; pressure defers dispatch until released; absent evidence is not treated as "no congestion"; retry readiness and budget; unproduced and stale-owned partitions skipped explicitly; work per call bounded regardless of the edge space; a topology change requires reconfiguration; cancellation abandons in-flight authority; completed edges are never re-granted | REAL |
| `test_unit_coordinator` | `tests/unit/test_coordinator.cpp` | 12 | Opening advances the epoch, refuses a second open and refuses a mismatched policy; an injected durability failure leaves no partial application and no residue; a callback from inside `persist()` is refused as re-entrant instead of deadlocking; registration supersedes the previous incarnation; manifest acceptance requires current ownership and refuses divergence; completion requires attempt-bound evidence, chunk evidence, byte agreement and current incarnations; a full shuffle completes with exactly-once accounting and a durable `Completed` state record; failure classification and the retry budget; cancellation refuses late authority and is idempotent; congestion evidence is fenced by generation, incarnation and tick; recovery fences stale authority and requires revalidation; a corrupted snapshot payload is refused with the coordinator still `Closed` | REAL |
| `test_scale_matrix` | `tests/scale/test_scale_matrix.cpp` | 5 | 65k x 256 closed-form edge counts; cursor work does not grow with the edge space (counted cursor steps, never wall-clock); progress is exact without materialising edges; many distinct selections stay bounded and exact; registration cost stays bounded for ascending identifiers | REAL |
| `test_property_shuffle` | `tests/property/test_property_shuffle.cpp` | 2 | Seeded randomized execution checked after every step against an independent model: closure, byte accounting, no double accounting, and duplicate completions never double-account. A failure prints its reproduction command | REAL |
| `test_concurrency_accounting` | `tests/concurrency/test_concurrency_accounting.cpp` | 4 | Many threads through the documented serialising wrapper keep accounting exact; concurrent readers never observe torn accounting; repeated lifecycles leave no residue; interleaved cancellation stops accounting cleanly | REAL |
| `test_integration_shuffle` | `tests/integration/test_integration_shuffle.cpp` | 2 | The real pieces wired as a deployment: a `CoordinatorServer` over loopback TCP serving a producer and a consumer client through a journal-backed `DurableSink`, one committed wave, a snapshot compaction, `DurableStore::open` + `Coordinator::recover` with the epoch advancing to 2 and revalidation required, re-registration with fresh incarnations, completion under the recovered state, and a second case proving a session cannot register another participant id (`IdentityMismatch`) while the connection stays usable. **Not green in any observed run** (section 1): the first case failed at three different assertions while its source was being edited | UNSUPPORTED |
| `test_protocol_frames` | `tests/protocol/test_protocol_frames.cpp` | 27 | Byte-exact 40-byte header offsets built from explicit constants rather than the library codec; a header span must be exactly one header; every message type round-trips with its payload; names and request/response classification are stable; refusals for magic, version, big-endian framing, reserved flags, sequence 0, both CRCs, declared length bounds, every truncated prefix and trailing garbage; a refused encode leaves the output untouched; empty payload; stream reassembly one byte at a time, back to back, bounded, and refusing an impossible prefix; the `ReplayGuard` window, out-of-order acceptance, out-of-window refusal and absurd jumps | REAL |
| `test_adversarial_frames` | `tests/adversarial/test_adversarial_frames.cpp` | 20 | Randomized and mutated buffers judged against an independently written reference decoder, with the exact `ErrorCode` required to agree: random buffers, every single-byte corruption, length-field fuzzing, magic/metadata fuzzing, absurd metadata, truncation, the added message types, random stream chunkings, and the stream queue bound. Real loopback socket proofs: two-way framed traffic, reassembly across writes, peer close as end of stream, local shutdown, operations on a closed handle, move ownership, a default-constructed socket, an empty accept queue, a blocked accept woken by closing the listener, connect to a closed port, argument validation | REAL |
| `test_durable_store` | `tests/persistence/test_durable_store.cpp` | 22 | Journal and snapshot formats built by the test from the frozen layout: open on an empty directory; append/close/reopen replaying identical payloads in order; monotone sequences across reopens; an oversized append refused without consuming a sequence; append/compact before open refused; compact then reopen folding records; compaction sequence range; two compactions keeping `.prev` readable; every snapshot header offset corrupted and every truncation length below the header; trailing garbage; version 2; non-zero flags; an absurd payload length refused without a huge allocation; a missing snapshot as an error, not a crash; the journal truncated at every byte position keeping the intact prefix; a flipped payload byte reported as corrupt; a decreased sequence as corrupt; a duplicated sequence at the tail skipped, not corrupt; an impossible record length as corrupt; byte accounting; a journal larger than four snapshots refused | REAL |
| `test_durable_recovery` | `tests/persistence/test_durable_recovery.cpp` | 5 | Real abrupt process death: a child re-executes the binary, appends records, prints each acknowledged sequence and kills itself without running a destructor; the parent trusts only the printed sequences and the bytes it finds. Deterministic torn tails keep the intact prefix; snapshot/journal overlap skips folded sequences; a corrupt primary snapshot falls back to the previous one; repeated open/append/compact cycles keep accounting sane | REAL |
| `test_coordinator_recovery` | `tests/persistence/test_coordinator_recovery.cpp` | 4 | A real coordinator writing through the real durable store, restarted from disk: snapshot+journal restore authoritative completions; journal-only recovery preserves acknowledged records; a torn journal tail costs only unacknowledged bytes; a cancelled shuffle stays cancelled across a restart | REAL |

## 3. Not registered with CTest in this revision

| File | Status |
| --- | --- |
| `tests/protocol/test_service_loopback.cpp` | `cmake --build build/release --target test_service_loopback` fails with `ninja: error: unknown target 'test_service_loopback'`, which is direct evidence that the file is not a CMake target. Implemented (7 cases: start/stop cycles, a mini shuffle over real TCP, handshake required first and identity binding, a duplicate sequence replaying an identical answer once, refusals propagating through the client, stopping the server failing a connected client deterministically, two servers routing to the right coordinator). It is **not** named by any `shuffle_fabric_add_test(...)` call in `tests/CMakeLists.txt`, so CTest does not build or run it, and no recorded verdict exists for it in this revision. Label: UNSUPPORTED as a registered CTest surface. |
| Multiprocess coordinator/producer/consumer scenario | `tools/shuffle-fabric-node.cpp` implements the participant processes (management client, own `ChunkFetch` listener, fault flags `--fault-corrupt-chunk`, `--fault-truncate-chunk`, `--fault-duplicate-frame`, `--fault-stale-incarnation`, `--print-result`). No CTest target in this revision drives it, so the scenario is not part of the green suite here. Label: UNSUPPORTED as a CTest surface. |

## 4. Why no test uses a timeout

* `tests/CMakeLists.txt` states it: "Every test is registered with CTest and runs
  without a timeout property: a hanging test is a defect to diagnose, not
  something to paper over."
* No `TIMEOUT` property and no `set_tests_properties(...)` call exists anywhere
  in the project's CMake files; `shuffle_fabric_add_test` does exactly
  `add_executable`, `target_link_libraries`, `target_include_directories`,
  the first-party warning options and `add_test(NAME ... COMMAND ...)`.
* No test passes a timeout on the command line, and `scripts/validate.ps1`
  applies none to configure, build or ctest.
* No test sleeps: there is no `sleep`, `sleep_for` or `Sleep` call anywhere
  under `tests/`. Synchronisation is on protocol responses, process handles,
  durability points and condition variables:
  * the abrupt-death test uses the child's exit status as its only synchronisation
    and trusts only the sequences the child printed before it died;
  * socket tests read with a bounded budget only after the peer has already been
    asked for the frame, and a listener with nothing pending is polled with a zero
    budget;
  * the one test where latency *is* the property asserts that a blocked `accept`
    is woken by closing the listener.
* A hang therefore stays a hang and is diagnosed, rather than being converted
  into a passing "timeout" verdict.

## 5. Seeds and reproduction

* The harness is `tests/support/test_support.cpp` with `tests/support/rng.hpp`
  (seeded xoshiro256**, splitmix64 seeding, rejection sampling in `bounded`).
* `run_seed()` returns the value given by `--seed=<u64>`; with no flag it takes
  the value once from `std::chrono::steady_clock::now()` and reports it. Every
  binary prints `shuffle-fabric test binary: <n> case(s), seed=<seed>` first, so
  an unseeded run still says exactly what to re-run.
* `derive_seed(label)` mixes the run seed with a constant and an FNV-style hash
  of the label, so each randomized case has a stable stream that does not depend
  on which other cases ran or in what order.
* Randomized cases and their labels:
  `property.randomized_execution_preserves_closure`,
  `property.duplicate_completions_never_double_account` (2), and
  `adversarial_frames.random_buffers`, `.byte_corruption`, `.length_field`,
  `.metadata`, `.tails`, `.truncation`, `.added_types`, `.stream_chunking`,
  `.stream_bounds` (9).
* On failure the harness prints the file and line of the requirement, then
  `reproduce: --seed=<seed> --filter=<suite.name>`. Because the seed is per run
  and per label, copying that line reproduces the exact input stream; a different
  `--seed` explores different inputs while the invariants stay fixed.
* `--filter` is a substring match against `<suite>.<name>`, and `--list` prints
  the matching case names without running anything. Exit codes: 0 all selected
  cases passed, 1 at least one failed (or no case matched), 2 usage error.
* CTest passes no seed, so a plain `ctest` run is a fresh randomized run whose
  seed appears in the failure output. Re-running `ctest` with the same seed
  requires invoking the binary directly.

## 6. What the suite guards (it is not a bug log)

The tests carry no defect-history commentary: no case names a defect it caught,
no file records a regression number, and there is no changelog to read. What can
be stated is what each case *guards*, which is what the table in section 2 gives.
The regression-shaped guards that are visible in the code and comments are:

| Guard | Where | What it prevents |
| --- | --- | --- |
| Duplicated sequence at the journal tail is skipped, while a decreased sequence mid-file is corrupt | `test_durable_store`, temp dir named `durable-store-sequence-regression` | an interrupted writer's repeated last record being mistaken for corruption; a rewound or interleaved journal being silently replayed |
| A refused append leaves no residue (topology generation and participant count unchanged, the same call succeeds afterwards) | `test_unit_coordinator.durability_failure_leaves_no_partial_application` | a mutation applied before its durability point |
| A callback from inside `persist()` is refused as re-entrant and the coordinator is usable afterwards | `test_unit_coordinator.re_entrant_callbacks_are_refused_not_deadlocked` | a callback-under-lock defect being tolerated as a deadlock or a silent recursion |
| `edges_over_counted` and `accounting_closes()` | `test_unit_ledger`, `test_unit_coordinator` | saturating subtraction hiding resolved edges that exceed the current requirement |
| Every field refusal in `test_protocol_frames` recomputes the header CRC before asserting | `tests/protocol/test_protocol_frames.cpp` header comment | a decoder that appears correct because it stopped at the integrity gate instead of judging the field |
| The adversarial differential oracle is a second implementation written from the offsets in `frame.hpp` | `tests/adversarial/test_adversarial_frames.cpp` header comment | a layout mistake proving itself correct by being tested against the library's own codec |
| Passing a temporary `SocketRuntime` is a compile error (deleted overload) | `include/shuffle/fabric/socket.hpp`, noted in the adversarial test | a handle outliving the socket subsystem it belongs to |

## 7. Unvalidated capabilities on this host

| Capability | Why unvalidated |
| --- | --- |
| POSIX socket branch (`src/socket.cpp`) | The `#else` branches are written to the same contract and are never compiled or executed here; `socket.hpp` states the POSIX branch "is UNVALIDATED until a POSIX build and run has proven it" |
| POSIX durable-store branch (`src/durable_store.cpp`) | File I/O and the two-`rename()` replace path carry the source comment `// UNVALIDATED: no POSIX host has executed this branch in this repository.` |
| POSIX process spawning | The abrupt-death proof uses `CreateProcessW`/`WaitForSingleObject` on Windows and a `fork`/`waitpid` branch elsewhere; only the Windows branch runs here |
| Physical network fabric | Every network proof is loopback TCP on one host with synthetic payloads: no RDMA, InfiniBand, RoCE, NVLink, SmartNIC/DPU or programmable switch has been exercised |
| Peer authentication | There is no key exchange, no signing and no transport encryption. No test asserts authentication, because there is nothing to assert: a peer that knows the digests can claim a completion it did not perform |
| Multi-host deployment | One coordinator per state directory, one shuffle per coordinator, no replication and no leader election; nothing here tests more than one host |
| Registered loopback-service and multiprocess CTest coverage | See section 3: the service loopback test file is unregistered, and no CTest case drives the participant processes |
| `test_integration_shuffle` | Registered and executed here, but red in every observed run (section 1), so it is not part of a green suite |

## 8. What a green run does not claim

A pass means the 13 registered targets agreed with their own assertions on this
host with this toolchain. It does not claim POSIX behaviour, physical-fabric
behaviour, authenticated peers, or durability against a power failure beyond the
flush boundary described in `docs/persistence.md`.
