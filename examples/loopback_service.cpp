// A real TCP shuffle over loopback: one server, two clients, two threads.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// The producer client opens the shuffle, registers as a producer and publishes
// every manifest. The consumer client registers, plans a wave, verifies every
// chunk of every granted partition and commits the transfer. Both are real
// CoordinatorClient instances on real loopback sockets, driven from separate
// std::threads, against one CoordinatorServer that owns the only Coordinator
// and serialises every call.
//
// DATA PLANE BOUNDARY. include/shuffle/fabric/service.hpp states that the
// ChunkFetch data plane is deliberately not served by the coordinator, and
// CoordinatorClient exposes no chunk fetch, so no bulk byte ever crosses this
// socket. The consumer therefore takes the accepted manifest over the wire and
// re-derives the partition bytes from the deterministic recipe in
// manifest.hpp -- the mechanism the library documents for multiprocess proofs.
// Every chunk is still verified against the descriptor the coordinator
// accepted, and the digests reported in the completion are the ones computed
// here, never ones echoed back by the peer.
//
// Build (from the repository root, once the library is current):
//
//   scripts\msvc.ps1 -Command 'cmake --build build/release --target shuffle_fabric'
//   scripts\msvc.ps1 -Command 'cl /nologo /std:c++20 /W4 /WX /EHsc /permissive- /utf-8 /Zc:__cplusplus /MD /Iinclude /Fo:build\examples-scratch\ /Fd:build\examples-scratch\vc-loopback.pdb examples\loopback_service.cpp build\release\shuffle_fabric.lib ws2_32.lib /Fe:build\examples-scratch\loopback_service.exe'
//   build\examples-scratch\loopback_service.exe
//
// The listener binds port 0, so the kernel chooses the port and no report ever
// contains one. Nothing here prints a timestamp or consults a wall clock.

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "shuffle/fabric/hash.hpp"
#include "shuffle/fabric/manifest.hpp"
#include "shuffle/fabric/service.hpp"

namespace {

using namespace shuffle::fabric;

constexpr ShuffleId kShuffle{9};
constexpr ShuffleGeneration kShuffleGeneration{2};
constexpr std::uint32_t kPartitions = 4;
constexpr std::uint64_t kPartitionBytes = 96;
constexpr std::uint32_t kChunkBytes = 32;
constexpr std::uint32_t kMaxWaveRounds = 32;

// The choreography is explicit rather than timing-based: one event is published
// by the producer and waited for by the consumer, so no sleep and no timeout
// decides anything.
class Gate {
 public:
  void advance_to(std::uint32_t stage) {
    const std::lock_guard<std::mutex> guard{mutex_};
    if (stage > stage_) {
      stage_ = stage;
    }
    condition_.notify_all();
  }

  void wait_for(std::uint32_t stage) {
    std::unique_lock<std::mutex> lock{mutex_};
    condition_.wait(lock, [this, stage] { return stage_ >= stage; });
  }

 private:
  std::mutex mutex_;
  std::condition_variable condition_;
  std::uint32_t stage_{0};
};

// Every fallible call goes through here: a refusal prints its deterministic
// ErrorCode name and the run is reported as failed.
struct Run {
  std::uint32_t failures{0};
  [[nodiscard]] bool fail(const char* what, ErrorCode code, const std::string& detail) {
    ++failures;
    std::cout << "FAIL " << what << ": " << to_string(code) << ": " << detail << "\n";
    return false;
  }
  [[nodiscard]] bool check(const char* what, const Status& status) {
    return status.ok() ? true : fail(what, status.code(), status.detail());
  }
  template <class T>
  [[nodiscard]] bool check(const char* what, const Result<T>& result) {
    return result.ok() ? true : fail(what, result.code(), result.detail());
  }
};

[[nodiscard]] PolicyEnvelope make_policy() {
  PolicyEnvelope policy;
  policy.shuffle = kShuffle;
  policy.shuffle_generation = kShuffleGeneration;
  policy.generation = PolicyGeneration{1};
  policy.fan = FanBounds{4, 4};
  policy.concurrency = ConcurrencyLimits{4, 2, 2};
  policy.waves = WaveLimits{4, 64};
  policy.retry = RetryPolicy{3, 0};
  policy.congestion = CongestionPolicy{80, 40, false, 10};
  return policy;
}

[[nodiscard]] ClientOptions client_options(std::uint16_t port, ParticipantKind kind, std::uint64_t id,
                                           std::uint64_t nonce) {
  ClientOptions options;
  options.host = "127.0.0.1";
  options.port = port;
  options.identity = SessionIdentity{kind, id, IncarnationId{1}, nonce};
  return options;
}

// The producer side: one immutable claim per partition, built from the same
// recipe the consumer will use.
[[nodiscard]] Result<PartitionManifest> build_partition(std::uint32_t partition, const Limits& limits) {
  const std::vector<std::byte> payload = synthetic_partition_payload(
      kShuffle, kShuffleGeneration, PartitionId{partition}, PartitionGeneration{1}, kPartitionBytes);
  return build_manifest(kShuffle, kShuffleGeneration, PartitionId{partition}, PartitionGeneration{1}, ProducerId{1},
                        IncarnationId{1}, TopologyGeneration{0}, payload, kChunkBytes, limits);
}

// The consumer side: check every chunk the manifest describes against the bytes
// the consumer holds, and carry exactly the digests it observed.
[[nodiscard]] Result<CommitRequest> make_commit(const DispatchGrant& grant, const PartitionManifest& manifest) {
  const std::vector<std::byte> payload = synthetic_partition_payload(
      kShuffle, kShuffleGeneration, manifest.partition, manifest.partition_generation, manifest.total_bytes);
  CommitRequest request;
  request.edge = EdgeKey{grant.partition, grant.partition_generation, grant.consumer};
  request.attempt = grant.attempt;
  request.wave = grant.wave;
  request.shuffle_generation = grant.shuffle_generation;
  request.topology_generation = grant.topology_generation;
  request.policy_generation = grant.policy_generation;
  request.producer = grant.producer;
  request.producer_incarnation = grant.producer_incarnation;
  request.consumer = grant.consumer;
  request.consumer_incarnation = grant.consumer_incarnation;
  request.manifest_digest = compute_manifest_digest(manifest);
  request.observed_partition_digest = request.manifest_digest;
  request.bytes = manifest.total_bytes;
  request.integrity_verified = true;
  for (const ChunkDescriptor& chunk : manifest.chunks) {
    const std::size_t offset = static_cast<std::size_t>(chunk.offset);
    if (offset > payload.size() || chunk.length > payload.size() - offset) {
      return make_failure<CommitRequest>(ErrorCode::TruncatedInput, "chunk lies outside the partition payload");
    }
    const std::span<const std::byte> slice{payload.data() + offset, chunk.length};
    const Status verified = verify_chunk(chunk, slice);
    if (!verified.ok()) return Result<CommitRequest>{verified.error()};
    request.observed_chunk_digests.push_back(sha256(slice));
  }
  return request;
}

// What the consumer learned, read by main only after the thread has joined.
struct ConsumerReport {
  std::uint32_t waves{0};
  std::uint64_t committed{0};
  ProgressSnapshot progress{};
  ShuffleState state{ShuffleState::Closed};
  bool durable{false};
};

}  // namespace

int main() {
  const Limits limits;
  VolatileSink sink;
  Coordinator coordinator{limits, &sink};

  ServerOptions server_options;
  server_options.limits = limits;
  server_options.bind_host = "127.0.0.1";
  server_options.port = 0;  // the kernel chooses; the report stays deterministic
  CoordinatorServer server{coordinator, server_options};
  const Status started = server.start();
  if (!started.ok()) {
    std::cout << "FAIL server.start: " << to_string(started.code()) << ": " << started.detail() << "\n";
    std::cout << "result: refused failures=1\n";
    return 1;
  }
  const std::uint16_t port = server.port();

  Gate gate;
  std::atomic<std::uint32_t> failures{0};
  ConsumerReport report;

  // Producer role: open, register, publish, then release the consumer.
  std::thread producer([&] {
    Run run;
    CoordinatorClient client;
    const auto work = [&]() {
      if (!run.check("producer.connect", client.connect(client_options(port, ParticipantKind::Producer, 1, 1)))) {
        return;
      }
      ShuffleOpenRequest open;
      open.shuffle = kShuffle;
      open.generation = kShuffleGeneration;
      open.partition_count = kPartitions;
      open.policy = make_policy();
      if (!run.check("producer.open_shuffle", client.open_shuffle(open))) return;
      if (!run.check("producer.register", client.register_participant(ParticipantKind::Producer, 1, IncarnationId{1},
                                                                      "127.0.0.1:1", PartitionSelection::all()))) {
        return;
      }
      for (std::uint32_t partition = 0; partition < kPartitions; ++partition) {
        const auto built = build_partition(partition, limits);
        if (!run.check("producer.build_manifest", built)) return;
        if (!run.check("producer.publish_manifest", client.publish_manifest(built.value()))) return;
      }
    };
    work();
    gate.advance_to(1);
    const bool closed = run.check("producer.close", client.close());
    static_cast<void>(closed);
    failures.fetch_add(run.failures, std::memory_order_relaxed);
  });

  // Consumer role: register, plan, verify, commit, read the result back.
  std::thread consumer([&] {
    Run run;
    gate.wait_for(1);
    CoordinatorClient client;
    const auto work = [&]() {
      if (!run.check("consumer.connect", client.connect(client_options(port, ParticipantKind::Consumer, 1, 2)))) {
        return;
      }
      if (!run.check("consumer.register", client.register_participant(ParticipantKind::Consumer, 1, IncarnationId{1},
                                                                      "127.0.0.1:2", PartitionSelection::all()))) {
        return;
      }
      bool complete = false;
      for (std::uint32_t round = 0; round < kMaxWaveRounds && !complete; ++round) {
        const auto plan = client.next_wave();
        if (!run.check("consumer.next_wave", plan)) return;
        if (plan.value().grants.empty()) {
          complete = plan.value().all_resolved;
          break;
        }
        ++report.waves;
        for (const DispatchGrant& grant : plan.value().grants) {
          const auto manifest = client.manifest(grant.partition);
          if (!run.check("consumer.manifest", manifest)) return;
          const auto commit = make_commit(grant, manifest.value());
          if (!run.check("consumer.make_commit", commit)) return;
          if (!run.check("consumer.commit_transfer", client.commit_transfer(commit.value()))) return;
          ++report.committed;
        }
      }
      const auto progress = client.progress();
      if (!run.check("consumer.progress", progress)) return;
      report.progress = progress.value();
      const auto status = client.status();
      if (!run.check("consumer.status", status)) return;
      report.state = status.value().state;
      report.durable = status.value().durable;
    };
    work();
    const bool closed = run.check("consumer.close", client.close());
    static_cast<void>(closed);
    failures.fetch_add(run.failures, std::memory_order_relaxed);
  });

  producer.join();
  consumer.join();
  const ServerStats stats = server.stats();
  const Status stopped = server.stop();

  const ProgressSnapshot& progress = report.progress;
  std::cout << "consumer: waves=" << report.waves << " committed_edges=" << report.committed
            << " required_edges=" << progress.edges_required << " completed=" << progress.edges_completed
            << " failed=" << progress.edges_failed << " incomplete=" << progress.edges_incomplete << "\n";
  std::cout << "consumer: bytes_committed=" << progress.bytes_committed
            << " partitions_committed=" << progress.partitions_committed
            << " duplicate_commits_suppressed=" << progress.duplicate_commits_suppressed << "\n";
  std::cout << "shuffle: state=" << to_string(report.state) << " durable=" << (report.durable ? "yes" : "no") << "\n";
  std::cout << "closure: " << progress.edges_completed << " + " << progress.edges_failed << " + "
            << progress.edges_incomplete << " == " << progress.edges_required << " -> "
            << (progress.accounting_closes() ? "yes" : "no") << "\n";
  if (!stopped.ok()) {
    std::cout << "FAIL server.stop: " << to_string(stopped.code()) << ": " << stopped.detail() << "\n";
    failures.fetch_add(1, std::memory_order_relaxed);
  }
  std::cout << "server: bind_host=" << server_options.bind_host << " port=ephemeral"
            << " accepted_sessions=" << stats.accepted_sessions << " refused_sessions=" << stats.refused_sessions
            << " served_requests=" << stats.served_requests << " refused_requests=" << stats.refused_requests
            << " duplicate_requests=" << stats.duplicate_requests
            << " replayed_answers=" << stats.replayed_answers << "\n";
  const std::uint32_t counted = failures.load(std::memory_order_relaxed);
  const bool accepted = counted == 0 && report.state == ShuffleState::Completed && progress.accounting_closes() &&
                        progress.edges_completed == progress.edges_required && progress.edges_incomplete == 0;
  std::cout << "result: " << (accepted ? "ok" : "refused") << " failures=" << counted << "\n";
  return accepted ? 0 : 1;
}
