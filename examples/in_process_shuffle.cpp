// A complete in-process shuffle: topology, manifests, waves, closure -- twice.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Run 1 drives the coordinator through a VolatileSink, which claims no
// durability. Run 2 repeats the identical shuffle through a real DurableStore
// in a temporary directory, so the records are shown to be on disk. The
// directory is left behind and can be inspected with
//
//   shuffle-fabric-cli inspect-state --state-dir <the directory named below>
//
// Nothing here opens a socket or reads a clock, so the report is reproducible:
//
//   scripts\msvc.ps1 -Command 'cmake --build build/release --target shuffle_fabric'
//   scripts\msvc.ps1 -Command 'mkdir build\examples-scratch 2>nul & cl /nologo /std:c++20 /W4 /WX /EHsc /permissive- /utf-8 /Zc:__cplusplus /MD /Iinclude /Fo:build\examples-scratch\ /Fd:build\examples-scratch\vc.pdb examples\in_process_shuffle.cpp build\release\shuffle_fabric.lib /Fe:build\examples-scratch\in_process_shuffle.exe'
//   build\examples-scratch\in_process_shuffle.exe

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <span>
#include <string>
#include <system_error>
#include <vector>

#include "shuffle/fabric/coordinator.hpp"
#include "shuffle/fabric/durable_store.hpp"
#include "shuffle/fabric/hash.hpp"
#include "shuffle/fabric/manifest.hpp"
#include "shuffle/fabric/policy.hpp"

namespace {

using namespace shuffle::fabric;

constexpr ShuffleId kShuffle{42};
constexpr ShuffleGeneration kShuffleGeneration{7};
constexpr std::uint32_t kPartitions = 12;
constexpr std::uint32_t kProducers = 3;
constexpr std::uint32_t kConsumers = 4;
constexpr std::uint64_t kPartitionBytes = 128;
constexpr std::uint32_t kChunkBytes = 32;  // four chunks per partition
constexpr std::uint32_t kMaxWaves = 64;
constexpr const char* kStoreDirectory = "shuffle-fabric-example-store";

// Every fallible call goes through here: a refusal prints its deterministic
// ErrorCode name and the caller stops rather than continuing on a value that
// does not exist.
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
  policy.fan = FanBounds{8, 8};
  policy.concurrency = ConcurrencyLimits{8, 4, 4};
  policy.waves = WaveLimits{6, 64};
  policy.retry = RetryPolicy{3, 0};
  policy.congestion = CongestionPolicy{80, 40, false, 10};
  return policy;
}

// The consumer side of a grant: the payload is re-derived from the producer's
// recipe, every chunk is checked against its descriptor, and the digests the
// consumer actually observed travel back with the completion. The completion
// also echoes every generation and identity the grant carried, so a stale
// holder cannot present the grant under a newer one.
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
  request.observed_partition_digest = request.manifest_digest;  // the digest content is bound to
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

// One complete shuffle: open, register, publish, wave until closure, report.
// The answer is the number of refusals, so a caller can exit non-zero.
[[nodiscard]] std::uint32_t execute(const char* title, DurableSink* sink) {
  Run run;
  const Limits limits;
  Coordinator coordinator{limits, sink};
  const ShuffleOpenRequest open{.shuffle = kShuffle,
                                .generation = kShuffleGeneration,
                                .partition_count = kPartitions,
                                .policy = make_policy()};
  if (!run.check("open_shuffle", coordinator.open_shuffle(open))) return 1;

  // Three producers and four consumers with four distinct interests: a full
  // shuffle, two disjoint ranges and an explicit list. Distinct selections are
  // interned into distinct patterns, so the planned edge count stays closed
  // form instead of materialising the N x M matrix.
  for (std::uint32_t index = 1; index <= kProducers; ++index) {
    const auto registered = coordinator.register_producer(ProducerId{index}, IncarnationId{1},
                                                          "127.0.0.1:" + std::to_string(9000 + index));
    if (!run.check("register_producer", registered)) return 1;
  }
  const auto listed =
      PartitionSelection::from_list({PartitionId{1}, PartitionId{3}, PartitionId{5}, PartitionId{7}}, Limits{});
  if (!run.check("from_list", listed)) return 1;
  const PartitionSelection selections[kConsumers] = {PartitionSelection::all(),
                                                     PartitionSelection::range(PartitionId{0}, PartitionId{6}),
                                                     listed.value(),
                                                     PartitionSelection::range(PartitionId{6}, PartitionId{12})};
  for (std::uint32_t index = 1; index <= kConsumers; ++index) {
    const auto registered = coordinator.register_consumer(ConsumerId{index}, IncarnationId{1},
                                                          "127.0.0.1:" + std::to_string(9500 + index),
                                                          selections[index - 1]);
    if (!run.check("register_consumer", registered)) return 1;
  }
  const auto opened = coordinator.status();
  if (!run.check("status", opened)) return 1;
  const std::uint64_t planned_edges = opened.value().progress.edges_required;

  // One immutable claim per partition, from the payload recipe a real producer
  // would have written. Publishing stamps the accepted topology generation, and
  // that accepted manifest is what the consumer verifies its chunks against.
  for (std::uint32_t partition = 0; partition < kPartitions; ++partition) {
    const std::vector<std::byte> payload = synthetic_partition_payload(
        kShuffle, kShuffleGeneration, PartitionId{partition}, PartitionGeneration{1}, kPartitionBytes);
    const auto built = build_manifest(kShuffle, kShuffleGeneration, PartitionId{partition}, PartitionGeneration{1},
                                      ProducerId{1 + (partition % kProducers)}, IncarnationId{1},
                                      TopologyGeneration{0}, payload, kChunkBytes, limits);
    if (!run.check("build_manifest", built)) return 1;
    if (!run.check("publish_manifest", coordinator.publish_manifest(built.value()))) return 1;
  }

  // Waves until closure. An empty wave that reports all_resolved is the only
  // honest end of the loop; anything else is a refusal, not a completion.
  std::vector<std::uint32_t> grants_per_wave;
  bool finished = false;
  for (std::uint32_t wave = 0; wave < kMaxWaves && !finished; ++wave) {
    const auto plan = coordinator.next_wave();
    if (!run.check("next_wave", plan)) return 1;
    if (plan.value().grants.empty()) {
      finished = plan.value().all_resolved;
      break;
    }
    grants_per_wave.push_back(static_cast<std::uint32_t>(plan.value().grants.size()));
    for (const DispatchGrant& grant : plan.value().grants) {
      const auto manifest = coordinator.manifest_of(grant.partition);
      if (!run.check("manifest_of", manifest)) return 1;
      const auto commit = make_commit(grant, manifest.value());
      if (!run.check("make_commit", commit)) return 1;
      if (!run.check("commit_transfer", coordinator.commit_transfer(commit.value()))) return 1;
    }
    if (!run.check("advance_tick", coordinator.advance_tick())) return 1;
  }

  const auto closed = coordinator.status();
  if (!run.check("status", closed)) return 1;
  const ProgressSnapshot& progress = closed.value().progress;

  // The report: policy, plan, waves, the final snapshot, and the closure check.
  std::cout << "\n== " << title << " ==\n" << describe_policy(make_policy());
  std::cout << "planned_edges=" << planned_edges << " waves=" << grants_per_wave.size() << "\n";
  for (std::size_t index = 0; index < grants_per_wave.size(); ++index) {
    std::cout << "  wave " << (index + 1) << " grants=" << grants_per_wave[index] << "\n";
  }
  std::cout << "progress: required=" << progress.edges_required << " completed=" << progress.edges_completed
            << " failed=" << progress.edges_failed << " incomplete=" << progress.edges_incomplete << "\n";
  std::cout << "committed: partitions=" << progress.partitions_committed << " bytes=" << progress.bytes_committed
            << " duplicate_commits_suppressed=" << progress.duplicate_commits_suppressed << "\n";
  std::cout << "state=" << to_string(closed.value().state)
            << " durable=" << (closed.value().durable ? "yes" : "no")
            << " all_resolved=" << (finished ? "yes" : "no") << "\n";
  std::cout << "closure: " << progress.edges_completed << " + " << progress.edges_failed << " + "
            << progress.edges_incomplete << " == " << progress.edges_required << " -> "
            << (progress.accounting_closes() ? "yes" : "no") << "\n";

  // A run is accepted only when every call succeeded, the ledger closed and the
  // shutdown state is the one completion implies.
  const bool accepted = run.failures == 0 && finished && closed.value().state == ShuffleState::Completed &&
                        progress.accounting_closes() && progress.edges_completed == planned_edges &&
                        progress.edges_failed == 0 && progress.edges_incomplete == 0;
  return accepted ? 0u : 1u;
}

// The production wiring: every durable record the coordinator commits becomes a
// journal record, and persist() returns only once the store reports it durable.
class StoreSink final : public DurableSink {
 public:
  explicit StoreSink(DurableStore& store) : store_(&store) {}
  [[nodiscard]] Status persist(std::span<const std::byte> record) override {
    const auto appended = store_->append(record);
    return appended.ok() ? Status{} : appended.status();
  }

 private:
  DurableStore* store_;
};

}  // namespace

int main() {
  std::uint32_t refusals = 0;

  // Run 1: no durability is claimed, and none is reported.
  VolatileSink volatile_sink;
  refusals += execute("run 1: volatile sink (no durability claimed)", &volatile_sink);

  // Run 2: the same shuffle with every durable record written to a real store.
  const std::filesystem::path directory = std::filesystem::temp_directory_path() / kStoreDirectory;
  std::error_code ec;
  std::filesystem::remove_all(directory, ec);
  std::filesystem::create_directories(directory, ec);
  if (ec) {
    std::cout << "FAIL store_directory: " << to_string(ErrorCode::PersistenceFailure) << ": " << ec.message() << "\n";
    return 2;
  }
  StoreConfig config;
  config.directory = directory;
  DurableStore store{config};
  const Status store_opened = store.open();
  if (!store_opened.ok()) {
    std::cout << "FAIL store.open: " << to_string(store_opened.code()) << ": " << store_opened.detail() << "\n";
    ++refusals;
  } else {
    StoreSink sink{store};
    refusals += execute("run 2: durable store", &sink);
    std::cout << "durable store (" << kStoreDirectory << "): records_reported=" << store.appended_records()
              << " last_sequence=" << store.last_sequence() << " journal_bytes=" << store.journal_bytes() << "\n";
  }

  std::cout << "\nresult: " << (refusals == 0 ? "ok" : "refused") << " refusals=" << refusals << "\n";
  return refusals == 0 ? 0 : 1;
}
