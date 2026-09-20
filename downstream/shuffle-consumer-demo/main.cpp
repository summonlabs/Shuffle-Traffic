// An independent consumer of an INSTALLED ShuffleFabric package.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// This file is deliberately not part of the ShuffleFabric build tree. It is the
// downstream proof that the installed package alone -- installed headers, the
// static library and the CMake package files under lib/cmake/ShuffleFabric --
// is sufficient to drive a real shuffle end to end through the public API.
//
// The scenario is fixed and deterministic: every count and total below is a
// function of the topology, the policy and the synthetic payload, never of a
// clock. Nothing here touches wall-clock time, so two runs of the same build
// print byte-identical summaries.
//
// Real call sequence (mirrors tests/unit/test_coordinator.cpp):
//   open_shuffle -> register_producer/register_consumer -> publish_manifest
//   -> next_wave -> commit_transfer (per-chunk digest evidence) -> advance_tick
//   -> status().progress, checked against the closure invariant.

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#ifndef SHUFFLE_FABRIC_TARGET_NATIVE
#error "SHUFFLE_FABRIC_TARGET_NATIVE must be defined by the project's CMakeLists.txt"
#endif

#include "shuffle/fabric/coordinator.hpp"
#include "shuffle/fabric/hash.hpp"
#include "shuffle/fabric/manifest.hpp"
#include "shuffle/fabric/policy.hpp"
#include "shuffle/fabric/version.hpp"

namespace {

using namespace shuffle::fabric;

// ---------------------------------------------------------------------------
// Scenario: 2 producers, 3 consumers with three different PartitionSelections,
// 6 partitions of 192 bytes each, split into 3 chunks of 64 bytes.
// ---------------------------------------------------------------------------
constexpr std::uint64_t kShuffleValue = 42;
constexpr std::uint64_t kShuffleGenerationValue = 1;
constexpr std::uint64_t kPolicyGenerationValue = 1;
constexpr std::uint32_t kPartitionCount = 6;
constexpr std::uint64_t kPartitionBytes = 192;
constexpr std::uint32_t kChunkBytes = 64;
constexpr std::uint64_t kIncarnation = 1;
constexpr std::uint32_t kProducers = 2;
constexpr std::uint32_t kConsumers = 3;
constexpr std::uint32_t kGrantsPerWave = 4;
constexpr std::uint32_t kEdgesExaminedPerWave = 256;
constexpr std::uint32_t kMaxWaves = 64;

constexpr ShuffleId kShuffle{kShuffleValue};
constexpr ShuffleGeneration kShuffleGeneration{kShuffleGenerationValue};
constexpr PartitionGeneration kPartitionGeneration{1};

// Expected shape of the scenario. These are not derived from the library; they
// are the numbers the topology below implies, and the demo fails when the
// library disagrees with them.
constexpr std::uint64_t kExpectedEdges = 12;      // partition x covering consumer
constexpr std::uint64_t kExpectedPartitions = 6;
constexpr std::uint64_t kExpectedPatterns = 3;    // all / range / list
constexpr std::uint64_t kExpectedBytesCommitted = kExpectedPartitions * kPartitionBytes;
constexpr std::uint64_t kExpectedChunksPerPartition = kPartitionBytes / kChunkBytes;

// Collects every failed expectation so one run reports all of them at once.
class Report {
 public:
  void fail(std::string what) { failures_.push_back(std::move(what)); }

  bool expect(bool condition, std::string what) {
    if (!condition) {
      fail(std::move(what));
    }
    return condition;
  }

  bool expect_ok(const Status& status, const std::string& what) {
    if (status.ok()) {
      return true;
    }
    fail(what + ": " + std::string{to_string(status.code())} + ": " + status.detail());
    return false;
  }

  template <class T>
  bool expect_ok(const Result<T>& result, const std::string& what) {
    if (result.ok()) {
      return true;
    }
    fail(what + ": " + std::string{to_string(result.code())} + ": " + result.detail());
    return false;
  }

  [[nodiscard]] bool clean() const { return failures_.empty(); }
  [[nodiscard]] const std::vector<std::string>& failures() const { return failures_; }

 private:
  std::vector<std::string> failures_{};
};

// Prints the accumulated failures and returns the process exit code.
int bail(const Report& report) {
  for (const std::string& failure : report.failures()) {
    std::cerr << "FAIL " << failure << "\n";
  }
  std::cerr << "demo_result=FAIL failures=" << report.failures().size() << "\n";
  return 1;
}

[[nodiscard]] PolicyEnvelope make_policy() {
  PolicyEnvelope policy;
  policy.shuffle = kShuffle;
  policy.shuffle_generation = kShuffleGeneration;
  policy.generation = PolicyGeneration{kPolicyGenerationValue};
  // Three consumers may cover one partition; two producers may serve one
  // consumer. Anything above that is outside this envelope.
  policy.fan = FanBounds{kConsumers, kProducers};
  // Bounded concurrency: four transfers in flight at most, two per producer and
  // two per consumer, so a wave is genuinely partial and the cursor must resume.
  policy.concurrency = ConcurrencyLimits{kGrantsPerWave, 2, 2};
  policy.waves = WaveLimits{kGrantsPerWave, kEdgesExaminedPerWave};
  policy.retry = RetryPolicy{3, 0};
  policy.congestion = CongestionPolicy{80, 40, false, 20};
  return policy;
}

// Builds the completion request exactly as the protocol requires it: attempt
// identity copied from the grant, the manifest digest the grant was issued for,
// and one observed digest per chunk, hashed from the bytes the consumer holds.
[[nodiscard]] CommitRequest make_commit_request(const DispatchGrant& grant, const PartitionManifest& manifest,
                                                std::span<const std::byte> received) {
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
  // The coordinator binds this field to the accepted manifest digest.
  request.observed_partition_digest = request.manifest_digest;
  request.bytes = manifest.total_bytes;
  request.integrity_verified = true;
  request.observed_chunk_digests.reserve(manifest.chunks.size());
  for (const ChunkDescriptor& chunk : manifest.chunks) {
    const auto offset = static_cast<std::size_t>(chunk.offset);
    const auto length = static_cast<std::size_t>(chunk.length);
    request.observed_chunk_digests.push_back(sha256(received.subspan(offset, length)));
  }
  return request;
}

}  // namespace

int main() {
  std::cout << "ShuffleFabric consumer demo\n";
  std::cout << "library_version=" << version_string() << "\n";
  std::cout << "package_source=find_package(ShuffleFabric 1.0.0 CONFIG REQUIRED), installed prefix\n";
  std::cout << "package_target=ShuffleFabric::fabric\n";
  // 1 means the installed package exported ShuffleFabric::fabric itself; the
  // project asserts that at configure time instead of compensating for it.
  std::cout << "package_target_native=" << SHUFFLE_FABRIC_TARGET_NATIVE << "\n";

  Report report;

  // A VolatileSink accepts every record and claims no durability. The
  // coordinator must therefore report durable() == false, and the demo asserts
  // exactly that instead of pretending the commits are crash-safe.
  VolatileSink sink;
  Coordinator coordinator{Limits{}, &sink};

  // -- open the shuffle ------------------------------------------------------
  ShuffleOpenRequest open_request;
  open_request.shuffle = kShuffle;
  open_request.generation = kShuffleGeneration;
  open_request.partition_count = kPartitionCount;
  open_request.policy = make_policy();
  if (!report.expect_ok(coordinator.open_shuffle(open_request), "open_shuffle")) {
    return bail(report);
  }

  // -- register 2 producers --------------------------------------------------
  for (std::uint64_t id = 1; id <= kProducers; ++id) {
    const auto outcome = coordinator.register_producer(ProducerId{id}, IncarnationId{kIncarnation},
                                                       "127.0.0.1:" + std::to_string(9000 + id));
    if (!report.expect_ok(outcome, "register_producer " + std::to_string(id))) {
      return bail(report);
    }
  }

  // -- register 3 consumers with three different PartitionSelections ---------
  const auto listed = PartitionSelection::from_list({PartitionId{1}, PartitionId{4}, PartitionId{5}}, Limits{});
  if (!report.expect_ok(listed, "PartitionSelection::from_list")) {
    return bail(report);
  }
  const std::vector<PartitionSelection> selections = {
      PartitionSelection::all(),                                 // 0,1,2,3,4,5
      PartitionSelection::range(PartitionId{0}, PartitionId{3}),  // 0,1,2
      listed.value(),                                            // 1,4,5
  };
  for (std::size_t index = 0; index < selections.size(); ++index) {
    const std::uint64_t id = static_cast<std::uint64_t>(index) + 1;
    const auto outcome = coordinator.register_consumer(ConsumerId{id}, IncarnationId{kIncarnation},
                                                       "127.0.0.1:" + std::to_string(9500 + id), selections[index]);
    if (!report.expect_ok(outcome, "register_consumer " + std::to_string(id))) {
      return bail(report);
    }
  }

  const auto registered = coordinator.status();
  if (!report.expect_ok(registered, "status after registration")) {
    return bail(report);
  }
  const TopologyGeneration topology_generation = registered.value().topology_generation;

  // -- publish one manifest per partition ------------------------------------
  // Deterministic ownership: partition p belongs to the p-th active producer in
  // ascending identifier order, so 1 + (p % 2) is the only legal producer.
  std::unordered_map<std::uint64_t, PartitionManifest> accepted;
  std::uint64_t declared_chunks = 0;
  std::uint64_t declared_bytes = 0;
  for (std::uint32_t partition = 0; partition < kPartitionCount; ++partition) {
    const ProducerId producer{1 + (partition % kProducers)};
    const std::vector<std::byte> payload = synthetic_partition_payload(
        kShuffle, kShuffleGeneration, PartitionId{partition}, kPartitionGeneration, kPartitionBytes);
    const auto built = build_manifest(kShuffle, kShuffleGeneration, PartitionId{partition}, kPartitionGeneration,
                                      producer, IncarnationId{kIncarnation}, topology_generation, payload, kChunkBytes,
                                      Limits{});
    if (!report.expect_ok(built, "build_manifest partition " + std::to_string(partition))) {
      return bail(report);
    }
    PartitionManifest manifest = built.value();
    const auto acceptance = coordinator.publish_manifest(manifest);
    if (!report.expect_ok(acceptance, "publish_manifest partition " + std::to_string(partition))) {
      return bail(report);
    }
    if (!report.expect(acceptance.value().manifest_digest == compute_manifest_digest(manifest),
                       "published digest disagrees with the local manifest digest")) {
      return bail(report);
    }
    declared_chunks += manifest.chunks.size();
    declared_bytes += manifest.total_bytes;
    accepted.emplace(partition, std::move(manifest));
  }

  // -- plan waves and commit every granted edge ------------------------------
  std::uint64_t waves_planned = 0;
  std::uint64_t grants_committed = 0;
  std::uint64_t chunks_verified = 0;
  std::uint64_t bytes_verified = 0;
  bool resolved = false;

  for (std::uint32_t iteration = 0; iteration < kMaxWaves && !resolved; ++iteration) {
    const auto plan = coordinator.next_wave();
    if (!report.expect_ok(plan, "next_wave")) {
      return bail(report);
    }
    ++waves_planned;

    if (plan.value().grants.empty()) {
      if (!report.expect(plan.value().all_resolved, "an empty wave must report all_resolved")) {
        return bail(report);
      }
      resolved = true;
      break;
    }

    for (const DispatchGrant& grant : plan.value().grants) {
      const auto found = accepted.find(grant.partition.value());
      if (!report.expect(found != accepted.end(), "grant names a partition with no accepted manifest")) {
        return bail(report);
      }
      const PartitionManifest& manifest = found->second;

      // The consumer side of a real transfer: derive the partition content
      // independently (a second process would derive the same bytes) and verify
      // every chunk against the manifest before submitting any evidence.
      const std::vector<std::byte> received = synthetic_partition_payload(
          kShuffle, kShuffleGeneration, grant.partition, grant.partition_generation, manifest.total_bytes);
      if (!report.expect_ok(verify_partition_content(manifest, received), "verify_partition_content")) {
        return bail(report);
      }

      const CommitRequest request = make_commit_request(grant, manifest, received);
      if (!report.expect(request.observed_chunk_digests.size() == manifest.chunks.size(),
                         "chunk evidence does not cover the manifest")) {
        return bail(report);
      }
      const auto outcome = coordinator.commit_transfer(request);
      if (!report.expect_ok(outcome, "commit_transfer")) {
        return bail(report);
      }
      if (!report.expect(outcome.value().receipt.edge_newly_completed,
                         "a granted edge must complete exactly once")) {
        return bail(report);
      }
      ++grants_committed;
      chunks_verified += request.observed_chunk_digests.size();
      bytes_verified += request.bytes;
    }

    if (!report.expect_ok(coordinator.advance_tick(), "advance_tick")) {
      return bail(report);
    }
  }

  if (!report.expect(resolved, "the shuffle did not resolve within the wave budget")) {
    return bail(report);
  }

  // -- deterministic summary -------------------------------------------------
  const auto final_status = coordinator.status();
  if (!report.expect_ok(final_status, "final status")) {
    return bail(report);
  }
  const ProgressSnapshot& progress = final_status.value().progress;

  // The invariant this demo exists to prove: every required edge is accounted
  // for as completed, failed or explicitly incomplete -- never silently lost
  // and never counted twice.
  const std::uint64_t closure =
      progress.edges_completed + progress.edges_failed + progress.edges_incomplete;
  const bool accounting_closes = closure == progress.edges_required;

  std::cout << "topology_producers=" << kProducers << "\n";
  std::cout << "topology_consumers=" << kConsumers << "\n";
  std::cout << "selection_patterns=" << kExpectedPatterns << "\n";
  std::cout << "topology_generation=" << topology_generation.value() << "\n";
  std::cout << "partitions_total=" << progress.partitions_total << "\n";
  std::cout << "partitions_committed=" << progress.partitions_committed << "\n";
  std::cout << "partitions_incomplete=" << progress.partitions_incomplete << "\n";
  std::cout << "edges_required=" << progress.edges_required << "\n";
  std::cout << "edges_completed=" << progress.edges_completed << "\n";
  std::cout << "edges_failed=" << progress.edges_failed << "\n";
  std::cout << "edges_incomplete=" << progress.edges_incomplete << "\n";
  std::cout << "bytes_declared=" << declared_bytes << "\n";
  std::cout << "chunks_declared=" << declared_chunks << "\n";
  std::cout << "bytes_committed=" << progress.bytes_committed << "\n";
  std::cout << "bytes_attempted=" << progress.bytes_attempted << "\n";
  std::cout << "bytes_verified=" << bytes_verified << "\n";
  std::cout << "chunks_verified=" << chunks_verified << "\n";
  std::cout << "waves_planned=" << waves_planned << "\n";
  std::cout << "grants_committed=" << grants_committed << "\n";
  std::cout << "duplicate_commits_suppressed=" << progress.duplicate_commits_suppressed << "\n";
  std::cout << "permanent_failures=" << progress.permanent_failures << "\n";
  std::cout << "authority_refusals=" << progress.authority_refusals << "\n";
  std::cout << "shuffle_state=" << to_string(final_status.value().state) << "\n";
  std::cout << "durable=" << (coordinator.durable() ? 1 : 0) << "\n";
  std::cout << "accounting_closes=" << (accounting_closes ? 1 : 0) << "\n";

  // -- the invariant, plus the shape this scenario must produce --------------
  report.expect(accounting_closes,
                "edges_completed + edges_failed + edges_incomplete != edges_required");
  report.expect(progress.edges_required == kExpectedEdges, "unexpected required edge count");
  report.expect(progress.edges_completed == kExpectedEdges, "not every required edge completed");
  report.expect(progress.edges_failed == 0, "no edge in this scenario may fail");
  report.expect(progress.edges_incomplete == 0, "no edge in this scenario may stay incomplete");
  report.expect(progress.partitions_committed == kExpectedPartitions, "not every partition committed");
  report.expect(progress.partitions_incomplete == 0, "every partition must be accounted for");
  report.expect(progress.bytes_committed == kExpectedBytesCommitted, "committed byte total is wrong");
  report.expect(progress.bytes_attempted == kExpectedEdges * kPartitionBytes, "an edge was dispatched more than once");
  report.expect(progress.duplicate_commits_suppressed == 0, "no duplicate completion may be suppressed here");
  report.expect(declared_chunks == kExpectedPartitions * kExpectedChunksPerPartition, "chunk count is wrong");
  report.expect(chunks_verified == kExpectedEdges * kExpectedChunksPerPartition, "not every chunk was verified");
  report.expect(bytes_verified == kExpectedEdges * kPartitionBytes, "not every delivered byte was verified");
  report.expect(grants_committed == kExpectedEdges, "grant count does not match the required edge set");
  report.expect(waves_planned >= 4, "a 12-edge shuffle with 4 grants per wave needs several waves");
  report.expect(final_status.value().state == ShuffleState::Completed, "shuffle did not reach Completed");
  report.expect(!coordinator.durable(), "a VolatileSink must never make the coordinator claim durability");

  if (!report.clean()) {
    return bail(report);
  }
  std::cout << "demo_result=PASS\n";
  return 0;
}
