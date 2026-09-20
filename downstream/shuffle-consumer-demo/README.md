# ShuffleFabric consumer demo (downstream)

An independent CMake project that consumes the **installed** ShuffleFabric
package. It is deliberately not part of the ShuffleFabric build: nothing adds it
with `add_subdirectory`, it never sees the source tree's include
directories and it never links the in-tree target. Everything it uses arrives
through one `find_package(ShuffleFabric 1.0.0 CONFIG REQUIRED)` call against an
installed prefix.

## What it proves

* `cmake --install <build> --prefix <prefix>` produces a package usable from
  outside the source tree: the public headers, the static library and
  `ShuffleFabricConfig.cmake` / `ShuffleFabricConfigVersion.cmake` /
  `ShuffleFabricTargets.cmake` under `<prefix>/lib/cmake/ShuffleFabric`.
* The installed package carries its own usage requirements: the imported target
  supplies the include directory, the C++20 requirement and the `ws2_32`
  dependency, so this project sets none of them itself.
* A **real** shuffle runs end to end through the public API only: identity,
  topology, policy, manifests, planning, dispatch, per-chunk integrity evidence
  and exactly-once completion accounting.
* The accounting invariant holds on the final state:
  `edges_completed + edges_failed + edges_incomplete == edges_required`.
  The demo exits non-zero when it does not.

## Building and running it

The prefix is produced by the parent project and validated by
`scripts/validate-install.ps1`, which performs exactly these steps from a
clean prefix:

```powershell
# 1. build and install the library (from the repository root)
.\scripts\msvc.ps1 -Command 'cmake -S . -B build/install-check -G Ninja -DCMAKE_BUILD_TYPE=Release -DSHUFFLE_FABRIC_BUILD_TESTS=OFF -DSHUFFLE_FABRIC_BUILD_TOOLS=OFF -DSHUFFLE_FABRIC_BUILD_EXAMPLES=OFF'
.\scripts\msvc.ps1 -Command 'cmake --build build/install-check'
.\scripts\msvc.ps1 -Command 'cmake --install build/install-check --prefix build/install-prefix'

# 2. build this project against that prefix only
.\scripts\msvc.ps1 -Command 'cmake -S downstream/shuffle-consumer-demo -B build/downstream-demo -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=build/install-prefix'
.\scripts\msvc.ps1 -Command 'cmake --build build/downstream-demo'

# 3. run it
.\build\downstream-demo\shuffle_consumer_demo.exe
```

Any other prefix works the same way: point `CMAKE_PREFIX_PATH` at it.

## What the demo actually does

The scenario is fixed: 2 producers, 3 consumers with three different
`PartitionSelection` patterns, 6 partitions of 192 bytes split into 3 chunks
of 64 bytes each.

| consumer | selection | partitions |
| --- | --- | --- |
| 1 | `PartitionSelection::all()` | 0,1,2,3,4,5 |
| 2 | `PartitionSelection::range(PartitionId{0}, PartitionId{3})` | 0,1,2 |
| 3 | `PartitionSelection::from_list({1,4,5}, Limits{})` | 1,4,5 |

That is 12 required edges over 6 partitions and 3 interned selection patterns.

The call sequence mirrors `tests/unit/test_coordinator.cpp` exactly, because
that is the sequence the library actually supports:

1. `Coordinator coordinator{Limits{}, &sink}` with a `VolatileSink`, so
   `durable()` must stay `false` and the demo asserts it.
2. `open_shuffle(ShuffleOpenRequest{shuffle, generation, partition_count, policy})`.
3. `register_producer` twice and `register_consumer` three times, once per
   selection pattern. Registration is what makes a participant `Active` and
   therefore able to hold authority.
4. One `build_manifest` per partition over
   `synthetic_partition_payload`, stamped with the topology generation read
   from `status()`, then `publish_manifest` for each. Ownership is
   deterministic: partition `p` belongs to the `p`-th active producer in
   ascending identifier order.
5. `next_wave()` in a loop. The policy allows 4 grants per wave, so the
   budget forces several waves and exercises the scheduler cursor; a tick is
   advanced between waves. A wave with no grants must report
   `all_resolved`, which is the only honest completion signal.
6. For every `DispatchGrant`, the consumer side is simulated honestly: it
   derives the partition payload independently, verifies it with
   `verify_partition_content`, hashes each chunk out of the bytes it holds
   and submits those digests as `observed_chunk_digests`. Nothing is copied
   from the manifest's own digest list.
7. `commit_transfer` with attempt identity, generations, incarnations,
   manifest digest, byte count and chunk evidence copied from the grant.
8. `status()` once at the end; the closure invariant is checked against
   `progress` before the summary is printed.

## Observed output

```
ShuffleFabric consumer demo
library_version=1.0.0
package_source=find_package(ShuffleFabric 1.0.0 CONFIG REQUIRED), installed prefix
package_target=ShuffleFabric::fabric
package_target_native=1
topology_producers=2
topology_consumers=3
selection_patterns=3
topology_generation=5
partitions_total=6
partitions_committed=6
partitions_incomplete=0
edges_required=12
edges_completed=12
edges_failed=0
edges_incomplete=0
bytes_declared=1152
chunks_declared=18
bytes_committed=1152
bytes_attempted=2304
bytes_verified=2304
chunks_verified=36
waves_planned=4
grants_committed=12
duplicate_commits_suppressed=0
permanent_failures=0
authority_refusals=0
shuffle_state=Completed
durable=0
accounting_closes=1
demo_result=PASS
```

Every number is a function of the topology, the policy and the synthetic
payload. There is no timestamp, no address and no machine-specific value in the
output, so two runs print the same bytes.

Reading the totals:

* `edges_required=12` are the producer-consumer deliveries implied by the
  three selection patterns; all 12 complete, none fail and none stay incomplete,
  so `accounting_closes=1`.
* `bytes_attempted=2304` is 12 deliveries of 192 bytes.
  `bytes_committed=1152` is 6 partitions of 192 bytes: a partition generation
  is accounted once no matter how many consumers received it, which is exactly
  the exactly-once accounting the ledger promises.
* `chunks_verified=36` is 12 deliveries times 3 chunks, each verified against
  the manifest before its digest is submitted as evidence.
* `waves_planned=4` is three productive waves plus the wave that reports
  `all_resolved`: the grant budget of 4 really did bound each wave.
* `shuffle_state=Completed` and `durable=0`: the shuffle ran to completion
  against a sink that claims no durability, and the coordinator says so instead
  of pretending otherwise. This demo proves the transfer and accounting path,
  not crash recovery -- durability is the `DurableSink` implementation's job.

## `package_target_native`

This value is reported by the demo and fixed at `1` by this project's
`CMakeLists.txt` after it has checked the installed package:

```cmake
if(NOT TARGET ShuffleFabric::fabric)
  message(FATAL_ERROR "the installed ShuffleFabric package does not provide the imported target ShuffleFabric::fabric; ...")
endif()
```

There is no fallback and no consumer-side alias: if
`ShuffleFabricTargets.cmake` exported the library under any other name, this
project would fail to configure instead of quietly patching the name up.
`scripts/validate-install.ps1` asserts the same thing independently by
grepping the installed targets file for `add_library(ShuffleFabric::fabric `,
so the exported name is checked in two places rather than inferred from a
successful link.
