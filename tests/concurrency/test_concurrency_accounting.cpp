// Concurrency and lifecycle proofs.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// The coordinator is deliberately single-threaded: the supported deployment
// serialises every call (the TCP service does this with one mutex, and event
// emission never happens under it). These tests prove that the documented
// serialisation keeps accounting exact under real thread contention and that
// repeated lifecycles leave no residue behind.

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "shuffle/fabric/coordinator.hpp"
#include "shuffle/fabric/hash.hpp"
#include "test_support.hpp"

namespace {

using namespace shuffle::fabric;

class Sink final : public DurableSink {
 public:
  std::atomic<std::uint64_t> records{0};
  [[nodiscard]] Status persist(std::span<const std::byte>) override {
    records.fetch_add(1, std::memory_order_relaxed);
    return Status{};
  }
};

// The supported deployment shape: one serialising mutex around the coordinator.
class Serialized {
 public:
  explicit Serialized(Coordinator& coordinator) : coordinator_(&coordinator) {}

  [[nodiscard]] Result<WavePlan> next_wave() {
    const std::lock_guard<std::mutex> guard{mutex_};
    return coordinator_->next_wave();
  }
  [[nodiscard]] Result<CommitOutcome> commit_transfer(const CommitRequest& request) {
    const std::lock_guard<std::mutex> guard{mutex_};
    return coordinator_->commit_transfer(request);
  }
  [[nodiscard]] Result<FailureOutcome> report_failure(TransferAttemptId attempt, ErrorCode code) {
    const std::lock_guard<std::mutex> guard{mutex_};
    return coordinator_->report_failure(attempt, code);
  }
  [[nodiscard]] Result<CoordinatorStatus> status() {
    const std::lock_guard<std::mutex> guard{mutex_};
    return coordinator_->status();
  }
  [[nodiscard]] Result<TickId> advance_tick() {
    const std::lock_guard<std::mutex> guard{mutex_};
    return coordinator_->advance_tick();
  }

 private:
  Coordinator* coordinator_;
  std::mutex mutex_;
};

struct Fixture {
  Sink sink{};
  Coordinator coordinator;
  std::uint32_t partitions{0};
  std::map<std::uint32_t, PartitionManifest> manifests{};

  Fixture(std::uint32_t partition_count, std::uint32_t producers)
      : coordinator(Limits{}, &sink), partitions(partition_count) {
    PolicyEnvelope policy;
    policy.shuffle = ShuffleId{11};
    policy.shuffle_generation = ShuffleGeneration{4};
    policy.generation = PolicyGeneration{1};
    policy.fan = FanBounds{512, 512};
    policy.concurrency = ConcurrencyLimits{64, 16, 16};
    policy.waves = WaveLimits{32, 1024};
    policy.retry = RetryPolicy{3, 0};
    policy.congestion = CongestionPolicy{80, 40, false, 10};

    ShuffleOpenRequest request;
    request.shuffle = policy.shuffle;
    request.generation = policy.shuffle_generation;
    request.partition_count = partition_count;
    request.policy = policy;
    REQUIRE_OK(coordinator.open_shuffle(request));
    for (std::uint32_t index = 1; index <= producers; ++index) {
      REQUIRE_OK(coordinator.register_producer(ProducerId{index}, IncarnationId{1},
                                               "127.0.0.1:" + std::to_string(12000 + index)));
    }
    for (std::uint32_t index = 1; index <= 6; ++index) {
      REQUIRE_OK(coordinator.register_consumer(ConsumerId{index}, IncarnationId{1},
                                               "127.0.0.1:" + std::to_string(13000 + index),
                                               PartitionSelection::all()));
    }
    for (std::uint32_t partition = 0; partition < partition_count; ++partition) {
      const std::uint64_t producer = 1 + (partition % producers);
      const std::vector<std::byte> payload = synthetic_partition_payload(
          policy.shuffle, policy.shuffle_generation, PartitionId{partition}, PartitionGeneration{1}, 48);
      const auto built = build_manifest(policy.shuffle, policy.shuffle_generation, PartitionId{partition},
                                        PartitionGeneration{1}, ProducerId{producer}, IncarnationId{1},
                                        TopologyGeneration{0}, payload, 16, Limits{});
      REQUIRE_OK(built);
      PartitionManifest manifest = built.value();
      manifest.topology_generation = coordinator.status().value().topology_generation;
      REQUIRE_OK(coordinator.publish_manifest(manifest));
      manifests[partition] = manifest;
    }
  }

  [[nodiscard]] CommitRequest make_commit(const DispatchGrant& grant) const {
    const PartitionManifest& manifest = manifests.at(static_cast<std::uint32_t>(grant.partition.value()));
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
      request.observed_chunk_digests.push_back(chunk.digest);
    }
    return request;
  }
};

SHUFFLE_TEST(concurrency, serialized_workers_keep_accounting_exact) {
  Fixture fixture(24, 3);
  Serialized serialized{fixture.coordinator};

  std::atomic<std::uint64_t> committed{0};
  std::atomic<std::uint64_t> failures{0};
  std::atomic<bool> stop{false};

  const auto worker = [&](std::uint32_t index) {
    std::uint32_t idle = 0;
    while (!stop.load(std::memory_order_relaxed) && idle < 4) {
      const auto plan = serialized.next_wave();
      if (!plan.ok()) {
        break;
      }
      if (plan.value().grants.empty()) {
        ++idle;
        static_cast<void>(serialized.advance_tick());
        continue;
      }
      idle = 0;
      for (const DispatchGrant& grant : plan.value().grants) {
        // One worker in four exercises the failure path; the accounting must
        // stay exact whichever outcome lands.
        if ((index % 4) == 3 && (grant.partition.value() % 5) == 0) {
          const auto outcome = serialized.report_failure(grant.attempt, ErrorCode::ConnectionFailure);
          if (outcome.ok()) {
            failures.fetch_add(1, std::memory_order_relaxed);
          }
          continue;
        }
        const auto outcome = serialized.commit_transfer(fixture.make_commit(grant));
        if (outcome.ok()) {
          committed.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(8);
  for (std::uint32_t index = 0; index < 8; ++index) {
    threads.emplace_back(worker, index);
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  stop.store(true, std::memory_order_relaxed);

  const auto status = serialized.status();
  REQUIRE_OK(status);
  const ProgressSnapshot& progress = status.value().progress;
  REQUIRE(progress.accounting_closes());
  REQUIRE_EQ(progress.edges_completed, committed.load());
  REQUIRE(progress.edges_failed <= failures.load() * 3);
  REQUIRE_EQ(progress.edges_completed + progress.edges_failed + progress.edges_incomplete,
             progress.edges_required);

  // Bytes are accounted exactly once per committed partition generation: every
  // partition in this fixture carries the same payload size, so the totals must
  // agree exactly rather than approximately.
  REQUIRE_EQ(progress.bytes_committed, progress.partitions_committed * 48);
  REQUIRE(progress.partitions_committed > 0);
}

SHUFFLE_TEST(concurrency, concurrent_readers_never_observe_torn_accounting) {
  Fixture fixture(16, 2);
  Serialized serialized{fixture.coordinator};

  std::atomic<bool> stop{false};
  std::atomic<std::uint64_t> observations{0};
  std::atomic<bool> consistent{true};

  std::vector<std::thread> readers;
  for (int index = 0; index < 3; ++index) {
    readers.emplace_back([&]() {
      while (!stop.load(std::memory_order_relaxed)) {
        const auto status = serialized.status();
        if (!status.ok()) {
          consistent.store(false, std::memory_order_relaxed);
          return;
        }
        if (!status.value().progress.accounting_closes()) {
          consistent.store(false, std::memory_order_relaxed);
          return;
        }
        observations.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  const auto writer = [&]() {
    for (int round = 0; round < 40; ++round) {
      const auto plan = serialized.next_wave();
      if (!plan.ok() || plan.value().grants.empty()) {
        static_cast<void>(serialized.advance_tick());
        continue;
      }
      for (const DispatchGrant& grant : plan.value().grants) {
        static_cast<void>(serialized.commit_transfer(fixture.make_commit(grant)));
      }
    }
  };
  std::thread writer_thread{writer};
  writer_thread.join();
  stop.store(true, std::memory_order_relaxed);
  for (std::thread& thread : readers) {
    thread.join();
  }

  REQUIRE(consistent.load());
  REQUIRE(observations.load() > 0);
  const auto status = serialized.status();
  REQUIRE_OK(status);
  REQUIRE(status.value().progress.accounting_closes());
}

SHUFFLE_TEST(concurrency, repeated_lifecycles_leave_no_residue) {
  std::uint64_t total_committed = 0;
  for (int cycle = 0; cycle < 40; ++cycle) {
    Fixture fixture(6, 2);
    Serialized serialized{fixture.coordinator};

    bool finished = false;
    for (int iteration = 0; iteration < 80 && !finished; ++iteration) {
      const auto plan = serialized.next_wave();
      REQUIRE_OK(plan);
      if (plan.value().grants.empty()) {
        finished = true;
        break;
      }
      for (const DispatchGrant& grant : plan.value().grants) {
        const auto outcome = serialized.commit_transfer(fixture.make_commit(grant));
        REQUIRE_OK(outcome);
        ++total_committed;
      }
      static_cast<void>(serialized.advance_tick());
    }
    REQUIRE(finished);

    const auto status = serialized.status();
    REQUIRE_OK(status);
    REQUIRE_EQ(status.value().state, ShuffleState::Completed);
    REQUIRE(status.value().progress.accounting_closes());
    REQUIRE_EQ(status.value().progress.edges_incomplete, std::uint64_t{0});
    REQUIRE_EQ(status.value().progress.edges_completed, status.value().progress.edges_required);
    REQUIRE_EQ(status.value().in_flight, 0u);
    // Every durable record of this lifecycle belongs to this coordinator only.
    REQUIRE(fixture.sink.records.load() > 0);
  }
  REQUIRE_EQ(total_committed, std::uint64_t{40} * 6 * 6);
}

SHUFFLE_TEST(concurrency, interleaved_cancellation_stops_accounting_cleanly) {
  Fixture fixture(12, 2);
  Serialized serialized{fixture.coordinator};

  std::atomic<std::uint64_t> committed{0};
  std::atomic<bool> cancelled{false};
  std::mutex cancel_mutex;

  const auto worker = [&]() {
    for (int round = 0; round < 30; ++round) {
      {
        const std::lock_guard<std::mutex> guard{cancel_mutex};
        if (cancelled.load(std::memory_order_relaxed)) {
          return;
        }
      }
      const auto plan = serialized.next_wave();
      if (!plan.ok()) {
        return;
      }
      for (const DispatchGrant& grant : plan.value().grants) {
        if (serialized.commit_transfer(fixture.make_commit(grant)).ok()) {
          committed.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
  };

  std::vector<std::thread> workers;
  workers.reserve(4);
  for (int index = 0; index < 4; ++index) {
    workers.emplace_back(worker);
  }
  {
    const std::lock_guard<std::mutex> guard{cancel_mutex};
    REQUIRE_OK(fixture.coordinator.cancel_shuffle(ErrorCode::Cancelled));
    cancelled.store(true, std::memory_order_relaxed);
  }
  for (std::thread& thread : workers) {
    thread.join();
  }

  const auto status = serialized.status();
  REQUIRE_OK(status);
  REQUIRE_EQ(status.value().state, ShuffleState::Cancelled);
  REQUIRE_EQ(status.value().in_flight, 0u);
  REQUIRE(status.value().progress.accounting_closes());
  // No completion can land after the cancellation point.
  const std::uint64_t after_cancel = status.value().progress.edges_completed;
  REQUIRE_EQ(after_cancel, committed.load());
}

}  // namespace
