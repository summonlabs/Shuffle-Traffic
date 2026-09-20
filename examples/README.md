# Shuffle Fabric examples

Two runnable programs and one inspection tool. Both examples execute a real,
supported path of the runtime and print a deterministic report: no timestamps,
no wall clock, no random ordering, and a non-zero exit code whenever anything
was refused or the ledger failed to close.

| file | what it is |
| --- | --- |
| `examples/in_process_shuffle.cpp` | a complete shuffle driven in one process, no network |
| `examples/loopback_service.cpp` | a complete shuffle over real loopback TCP, one server and two clients |
| `tools/shuffle-fabric-cli.cpp` | inspection tooling: plan, inspect-state, progress, explain, status |

## What each example proves

### `in_process_shuffle.cpp`

Proves the whole authority path end to end without a socket:

* a topology is built from three producers and four consumers with four
  distinct partition selections (all, two disjoint ranges, an explicit list);
* the shuffle is opened through `Coordinator::open_shuffle`, whose policy
  envelope is validated before anything else happens;
* one manifest per partition is built with `build_manifest` over
  `synthetic_partition_payload` and published through the coordinator;
* the wave loop calls `next_wave` until it returns an empty plan that reports
  `all_resolved`, committing every granted edge through `commit_transfer`
  with the per-chunk digests the consumer recomputed from the payload it holds;
* the report prints the policy, the closed-form planned edge count, the number
  of waves and the grants in each one, the final progress snapshot, and the
  closure check `completed + failed + incomplete == required`.

It then repeats the identical shuffle twice more, changing only the durability
point: run 2 writes every durable record into a real `DurableStore` in a
temporary directory and prints how many records the store reports, and run 3
reopens that directory and checks that the same number of records replays.

The store directory is deliberately left behind, named
`shuffle-fabric-example-store` inside the system temporary directory, so the
inspection tool can be pointed at it.

### `loopback_service.cpp`

Proves the distributed form of the same model over real TCP:

* one `CoordinatorServer` owns the only `Coordinator` and binds
  `127.0.0.1` port 0, so the kernel chooses the port and no report ever
  contains one;
* two `CoordinatorClient` instances run in separate `std::thread`s -- one
  producer role, one consumer role -- each with its own handshake identity;
* the producer client opens the shuffle, registers, and publishes every
  manifest; the consumer client registers, plans waves, fetches each accepted
  manifest from the coordinator, verifies every chunk, and commits;
* the choreography is a condition variable and an event counter, never a sleep
  or a timeout, so the run is reproducible;
* the report prints the waves, the committed edges, the final progress
  snapshot, the closure check, and the server's own counters.

DATA PLANE BOUNDARY. `include/shuffle/fabric/service.hpp` states that the
`ChunkFetch` data plane is deliberately not served by the coordinator, and
`CoordinatorClient` exposes no chunk fetch. No bulk byte therefore crosses
this socket. The consumer takes the accepted manifest over the wire and
re-derives the partition bytes from the deterministic recipe in `manifest.hpp`
-- the mechanism the library documents for multiprocess proofs -- then verifies
every chunk against the descriptor the coordinator accepted. The digests
reported in the completion are the ones computed locally, never ones echoed
back by a peer.

## Build and run

From the repository root, with the library current:

    scripts\msvc.ps1 -Command 'cmake --build build/release --target shuffle_fabric'

Nothing else is needed: there are no third-party dependencies, and `ws2_32`
is the only system library, because `shuffle_fabric.lib` already carries the
service layer. If you link against a library built before `src/service.cpp`
joined `SHUFFLE_FABRIC_CORE_SOURCES`, add
`src/frame.cpp src/socket.cpp src/service.cpp` to the commands below and the
result is identical.

### Direct compiler route (verified from the repository root)

    scripts\msvc.ps1 -Command 'mkdir build\examples-scratch 2>nul & cl /nologo /std:c++20 /W4 /WX /EHsc /permissive- /utf-8 /Zc:__cplusplus /MD /Iinclude /Fo:build\examples-scratch\ /Fd:build\examples-scratch\vc.pdb examples\in_process_shuffle.cpp build\release\shuffle_fabric.lib /Fe:build\examples-scratch\in_process_shuffle.exe'

    scripts\msvc.ps1 -Command 'cl /nologo /std:c++20 /W4 /WX /EHsc /permissive- /utf-8 /Zc:__cplusplus /MD /Iinclude /Fo:build\examples-scratch\ /Fd:build\examples-scratch\vc-loopback.pdb examples\loopback_service.cpp build\release\shuffle_fabric.lib ws2_32.lib /Fe:build\examples-scratch\loopback_service.exe'

    scripts\msvc.ps1 -Command 'cl /nologo /std:c++20 /W4 /WX /EHsc /permissive- /utf-8 /Zc:__cplusplus /MD /Iinclude /Fo:build\examples-scratch\ /Fd:build\examples-scratch\vc-cli.pdb tools\shuffle-fabric-cli.cpp build\release\shuffle_fabric.lib ws2_32.lib /Fe:build\examples-scratch\shuffle-fabric-cli.exe'

Run them from the repository root:

    build\examples-scratch\in_process_shuffle.exe
    build\examples-scratch\loopback_service.exe
    build\examples-scratch\shuffle-fabric-cli.exe --help

`/MD` matches the runtime library the CMake build uses; omitting it links the
static CRT against a dynamic-CRT library and fails with `LNK2038`.

### CMake route (from a build directory)

The CMake targets are `example-in-process-shuffle`, `example-loopback-service`
and `shuffle-fabric-cli`; each links `ShuffleFabric::fabric`, which carries
the service layer and inherits `ws2_32` on Windows. From a configured build
directory, verified here:

    cmake --build . --target example-in-process-shuffle example-loopback-service shuffle-fabric-cli
    .\examples\example-in-process-shuffle.exe
    .\examples\example-loopback-service.exe
    .\tools\shuffle-fabric-cli.exe plan --partitions 12 --producers 3 --consumers 4 --waves 5

## REAL versus UNSUPPORTED in this build

REAL, executed and verified here:

* the full in-process authority path: open, register, publish, plan, commit,
  exactly-once accounting, closure, and the durable journal behind all of it;
* `DurableStore` recovery on reopen, including the recovery report;
* real loopback TCP: listener, handshake, session binding, and every management
  request answered by the coordinator behind the server's single mutex;
* the CLI's `plan` surface, which runs the real `WaveScheduler` against a real
  `Topology` and prints grants, examined counts and deferral counters;
* the CLI's `progress`, `explain` and `status`, which are real
  `CoordinatorClient` sessions against a running server;
* the CLI's `inspect-state`, which reads a real store directory.

UNSUPPORTED, and stated rather than faked:

* the `ChunkFetch` bulk data plane: the coordinator service does not serve it
  and `CoordinatorClient` has no chunk fetch, so a consumer cannot pull chunk
  bytes from this service. The loopback example says so in its own comments and
  uses the deterministic recipe instead;
* a read-only open of a `DurableStore`: the class has one entry point,
  `DurableStore::open`, which repairs a torn journal tail by truncating it.
  `inspect-state` therefore reports `bytes_discarded` and says so explicitly
  instead of claiming a read-only pass.

## Inspecting what the examples produced

    build\examples-scratch\shuffle-fabric-cli.exe inspect-state --state-dir %TEMP%\shuffle-fabric-example-store

prints the recovery report (snapshot sequence, records replayed and skipped,
tail status, bytes discarded, revalidation required) and a summary of the
durable records by kind. The same tool explains a plan before anything moves:

    build\examples-scratch\shuffle-fabric-cli.exe plan --partitions 12 --producers 3 --consumers 4 --selection range:0:6 --max-grants 4 --global 4 --max-per-source 2 --max-per-destination 2 --waves 3

and answers questions about a running coordinator:

    build\examples-scratch\shuffle-fabric-cli.exe status   --coordinator 127.0.0.1:9100
    build\examples-scratch\shuffle-fabric-cli.exe progress --coordinator 127.0.0.1:9100
    build\examples-scratch\shuffle-fabric-cli.exe explain  --coordinator 127.0.0.1:9100
