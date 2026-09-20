// Seeded randomized proof of the closure property: whatever mix of commits,
// failures, retries, pressure and cancellation a run produces, every required
// producer-consumer edge ends either exactly once authoritatively completed or
// explicitly failed/incomplete, and bytes are never accounted twice.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include <map>
#include <set>
#include <string>
#include <vector>

#include "rng.hpp"
#include "shuffle/fabric/coordinator.hpp"
#include "shuffle/fabric/hash.hpp"
#include "test_support.hpp"

namespace {

using namespace shuffle::fabric;
using shuffle::test::derive_seed;
using shuffle::test::Random;

class Sink final : public DurableSink {
 public:
  std::uint64_t records{0};
  [[nodiscard]] Status persist(std::span<const std::byte>) override {
    ++records;
    return Status{};
  }
};

struct Model {
  std::map<std::string, std::uint64_t> committed_partitions{};  // "p:g" -> bytes
  std::set<std::string> completed_edges{};                      // "p:g:c"
  std::set<std::string> failed_edges{};
  std::uint64_t duplicate_commits{0};

  [[nodiscard]] static std::string partition_key(PartitionId partition, PartitionGeneration generation) {
    return partition.to_string() + ":" + generation.to_string();
  }
  [[nodiscard]] static std::string edge_key(const EdgeKey& key) { return key.to_string(); }
};

struct Run {
  Sink sink{};
  Coordinator coordinator;
  std::uint32_t partitions{0};
  PolicyEnvelope policy{};
  std::map<std::uint32_t, PartitionManifest> manifests{};
  std::map<std::uint64_t, DispatchGrant> grants{};

  Run(std::uint32_t partition_count, const PolicyEnvelope& envelope)
      : coordinator(Limits{}, &sink), partitions(partition_count), policy(envelope) {}

  void start() {
    ShuffleOpenRequest request;
    request.shuffle = policy.shuffle;
    request.generation = policy.shuffle_generation;
    request.partition_count = partitions;
    request.policy = policy;
    REQUIRE_OK(coordinator.open_shuffle(request));
  }

  void publish(std::uint32_t partition, std::uint64_t producer, std::uint64_t incarnation) {
    const std::vector<std::byte> payload =
        synthetic_partition_payload(policy.shuffle, policy.shuffle_generation, PartitionId{partition},
                                    PartitionGeneration{1}, 64 + (partition % 7));
    const auto built = build_manifest(policy.shuffle, policy.shuffle_generation, PartitionId{partition},
                                      PartitionGeneration{1}, ProducerId{producer}, IncarnationId{incarnation},
                                      TopologyGeneration{0}, payload, 32, Limits{});
    REQUIRE_OK(built);
    PartitionManifest manifest = built.value();
    manifest.topology_generation = coordinator.status().value().topology_generation;
    const auto accepted = coordinator.publish_manifest(manifest);
    if (!accepted.ok()) {
      FAIL_TEST(std::string{"publishing partition "} + std::to_string(partition) + " failed: " +
                format_error(accepted.error()));
    }
    manifests[partition] = manifest;
  }

  [[nodiscard]] CommitRequest make_commit(const DispatchGrant& grant) {
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

// The invariants that must hold after every randomized step.
void check_invariants(const Run& run, const Model& model, const std::string& context) {
  const auto status = run.coordinator.status();
  if (!status.ok()) {
    FAIL_TEST(context + ": coordinator status failed: " + format_error(status.error()));
  }
  const ProgressSnapshot& progress = status.value().progress;

  if (!progress.accounting_closes()) {
    FAIL_TEST(context + ": edge accounting does not close (completed=" + std::to_string(progress.edges_completed) +
              " failed=" + std::to_string(progress.edges_failed) +
              " incomplete=" + std::to_string(progress.edges_incomplete) +
              " required=" + std::to_string(progress.edges_required) + ")");
  }
  if (progress.edges_completed != model.completed_edges.size()) {
    FAIL_TEST(context + ": completed edge count " + std::to_string(progress.edges_completed) +
              " does not match the model " + std::to_string(model.completed_edges.size()));
  }
  if (progress.edges_failed != model.failed_edges.size()) {
    FAIL_TEST(context + ": failed edge count " + std::to_string(progress.edges_failed) + " does not match the model " +
              std::to_string(model.failed_edges.size()));
  }
  std::uint64_t expected_bytes = 0;
  for (const auto& entry : model.committed_partitions) {
    expected_bytes += entry.second;
  }
  if (progress.bytes_committed != expected_bytes) {
    FAIL_TEST(context + ": committed bytes " + std::to_string(progress.bytes_committed) +
              " do not match the model " + std::to_string(expected_bytes));
  }
  if (progress.duplicate_commits_suppressed != model.duplicate_commits) {
    FAIL_TEST(context + ": duplicate suppression count does not match the model");
  }
}

SHUFFLE_TEST(property, randomized_execution_preserves_closure) {
  Random rng(derive_seed("property.randomized_execution_preserves_closure"));
  const std::string label = "property.randomized_execution_preserves_closure";

  for (int scenario = 0; scenario < 24; ++scenario) {
    const std::uint32_t partitions = static_cast<std::uint32_t>(rng.in_range(3, 24));
    const std::uint32_t producers = static_cast<std::uint32_t>(rng.in_range(1, 4));
    const std::uint32_t consumers = static_cast<std::uint32_t>(rng.in_range(1, 6));

    PolicyEnvelope policy;
    policy.shuffle = ShuffleId{5};
    policy.shuffle_generation = ShuffleGeneration{2};
    policy.generation = PolicyGeneration{1};
    policy.fan = FanBounds{64, 64};
    policy.concurrency = ConcurrencyLimits{static_cast<std::uint32_t>(rng.in_range(1, 6)),
                                           static_cast<std::uint32_t>(rng.in_range(1, 3)),
                                           static_cast<std::uint32_t>(rng.in_range(1, 3))};
    if (policy.concurrency.per_source > policy.concurrency.global) {
      policy.concurrency.per_source = policy.concurrency.global;
    }
    if (policy.concurrency.per_destination > policy.concurrency.global) {
      policy.concurrency.per_destination = policy.concurrency.global;
    }
    policy.waves = WaveLimits{static_cast<std::uint32_t>(rng.in_range(1, 8)),
                              static_cast<std::uint32_t>(rng.in_range(8, 128))};
    policy.retry = RetryPolicy{static_cast<std::uint32_t>(rng.in_range(1, 4)), 0};
    policy.congestion = CongestionPolicy{80, 40, false, 5};

    Run run(partitions, policy);
    run.start();
    for (std::uint32_t index = 1; index <= producers; ++index) {
      const auto registered = run.coordinator.register_producer(
          ProducerId{index}, IncarnationId{1}, "127.0.0.1:" + std::to_string(10000 + index));
      REQUIRE_OK(registered);
    }
    for (std::uint32_t index = 1; index <= consumers; ++index) {
      PartitionSelection selection;
      switch (rng.bounded(3)) {
        case 0:
          selection = PartitionSelection::all();
          break;
        case 1: {
          const auto begin = static_cast<std::uint32_t>(rng.bounded(partitions));
          const auto end = static_cast<std::uint32_t>(rng.in_range(begin + 1, partitions));
          selection = PartitionSelection::range(PartitionId{begin}, PartitionId{end});
          break;
        }
        default: {
          std::vector<PartitionId> list;
          const std::uint32_t count = static_cast<std::uint32_t>(rng.in_range(1, partitions));
          for (std::uint32_t pick = 0; pick < count; ++pick) {
            list.push_back(PartitionId{rng.bounded(partitions)});
          }
          const auto listed = PartitionSelection::from_list(list, Limits{});
          if (!listed.ok()) {
            selection = PartitionSelection::all();
          } else {
            selection = listed.value();
          }
          break;
        }
      }
      const auto registered = run.coordinator.register_consumer(
          ConsumerId{index}, IncarnationId{1}, "127.0.0.1:" + std::to_string(11000 + index), selection);
      REQUIRE_OK(registered);
    }
    for (std::uint32_t partition = 0; partition < partitions; ++partition) {
      run.publish(partition, 1 + (partition % producers), 1);
    }

    Model model;
    // Every required edge must be accounted for by the end of the scenario.
    const auto required = run.coordinator.progress().value().edges_required;

    for (int step = 0; step < 120; ++step) {
      const std::uint64_t action = rng.bounded(100);
      const std::string context = "scenario " + std::to_string(scenario) + " step " + std::to_string(step) +
                                  " seed " + rng.reproduction(label);

      if (action < 55) {
        const auto plan = run.coordinator.next_wave();
        if (!plan.ok()) {
          if (plan.code() == ErrorCode::Cancelled) {
            break;
          }
          FAIL_TEST(context + ": next_wave failed: " + format_error(plan.error()));
        }
        if (plan.value().grants.empty() && plan.value().all_resolved) {
          break;
        }
        for (const DispatchGrant& grant : plan.value().grants) {
          run.grants[grant.attempt.value()] = grant;
        }
      } else if (action < 75 && !run.grants.empty()) {
        // Resolve an in-flight grant: commit, transient failure or refusal.
        const std::uint64_t pick = rng.bounded(run.grants.size());
        auto it = run.grants.begin();
        std::advance(it, static_cast<std::ptrdiff_t>(pick));
        const DispatchGrant grant = it->second;
        const std::string edge = Model::edge_key(EdgeKey{grant.partition, grant.partition_generation, grant.consumer});
        const std::uint64_t choice = rng.bounded(100);
        if (choice < 60) {
          const auto outcome = run.coordinator.commit_transfer(run.make_commit(grant));
          if (outcome.ok()) {
            model.committed_partitions.emplace(
                Model::partition_key(grant.partition, grant.partition_generation),
                run.manifests.at(static_cast<std::uint32_t>(grant.partition.value())).total_bytes);
            model.completed_edges.insert(edge);
          } else {
            FAIL_TEST(context + ": valid completion was refused: " + format_error(outcome.error()));
          }
        } else if (choice < 90) {
          const auto outcome = run.coordinator.report_failure(grant.attempt, ErrorCode::ConnectionFailure);
          if (outcome.ok()) {
            if (outcome.value().permanent) {
              model.failed_edges.insert(edge);
            }
          } else {
            FAIL_TEST(context + ": transient failure was refused: " + format_error(outcome.error()));
          }
        } else {
          const auto outcome = run.coordinator.report_failure(grant.attempt, ErrorCode::AuthorityDenied);
          if (outcome.ok()) {
            model.failed_edges.insert(edge);
          } else {
            FAIL_TEST(context + ": authority refusal was refused: " + format_error(outcome.error()));
          }
        }
        run.grants.erase(it);
      } else if (action < 85) {
        const auto tick = run.coordinator.advance_tick();
        REQUIRE_OK(tick);
      } else if (action < 92) {
        CongestionIntent intent;
        intent.reporter_kind = ParticipantKind::Consumer;
        intent.reporter_id = 1;
        intent.reporter_incarnation = IncarnationId{1};
        intent.consumer = ConsumerId{1};
        intent.level = static_cast<std::uint32_t>(rng.bounded(101));
        intent.policy_generation = policy.generation;
        intent.observed_at = run.coordinator.status().value().tick;
        const Status accepted = run.coordinator.update_congestion(intent);
        if (!accepted.ok()) {
          // Pressure evidence is only accepted while the shuffle holds
          // authority; a finished shuffle refuses it, which ends the scenario.
          if (accepted.code() == ErrorCode::InvalidState || accepted.code() == ErrorCode::Cancelled) {
            break;
          }
          FAIL_TEST(context + ": congestion evidence was refused: " + format_error(accepted.error()));
        }
      } else if (action < 96 && !model.completed_edges.empty()) {
        // Replay a completion that is already authoritative: it must be
        // suppressed without touching the accounting.
        for (const auto& entry : run.grants) {
          const DispatchGrant grant = entry.second;
          const std::string edge =
              Model::edge_key(EdgeKey{grant.partition, grant.partition_generation, grant.consumer});
          if (model.completed_edges.count(edge) == 0) {
            continue;
          }
          const auto replay = run.coordinator.commit_transfer(run.make_commit(grant));
          if (replay.ok()) {
            ++model.duplicate_commits;
          }
          break;
        }
      } else {
        const auto tick = run.coordinator.advance_tick();
        REQUIRE_OK(tick);
      }

      check_invariants(run, model, context);
    }

    // Closure: resolved edges plus explicit incompleteness must equal the
    // required set, and nothing may be counted twice.
    const ProgressSnapshot final_progress = run.coordinator.status().value().progress;
    REQUIRE(final_progress.accounting_closes());
    REQUIRE_EQ(final_progress.edges_completed + final_progress.edges_failed + final_progress.edges_incomplete,
               required);
    REQUIRE(final_progress.edges_completed <= required);
    for (const auto& entry : model.committed_partitions) {
      REQUIRE(entry.second > 0);
    }
  }
}

SHUFFLE_TEST(property, duplicate_completions_never_double_account) {
  Random rng(derive_seed("property.duplicate_completions_never_double_account"));
  const std::string label = "property.duplicate_completions_never_double_account";

  for (int scenario = 0; scenario < 12; ++scenario) {
    PolicyEnvelope policy;
    policy.shuffle = ShuffleId{9};
    policy.shuffle_generation = ShuffleGeneration{1};
    policy.generation = PolicyGeneration{1};
    policy.fan = FanBounds{64, 64};
    policy.concurrency = ConcurrencyLimits{4, 4, 4};
    policy.waves = WaveLimits{8, 64};
    policy.retry = RetryPolicy{2, 0};
    policy.congestion = CongestionPolicy{80, 40, false, 5};

    Run run(static_cast<std::uint32_t>(rng.in_range(2, 8)), policy);
    run.start();
    REQUIRE_OK(run.coordinator.register_producer(ProducerId{1}, IncarnationId{1}, "127.0.0.1:10001"));
    REQUIRE_OK(run.coordinator.register_consumer(ConsumerId{1}, IncarnationId{1}, "127.0.0.1:11001",
                                                 PartitionSelection::all()));
    for (std::uint32_t partition = 0; partition < run.partitions; ++partition) {
      run.publish(partition, 1, 1);
    }

    const auto plan = run.coordinator.next_wave();
    REQUIRE_OK(plan);
    std::uint64_t expected_bytes = 0;
    for (const DispatchGrant& grant : plan.value().grants) {
      expected_bytes += run.manifests.at(static_cast<std::uint32_t>(grant.partition.value())).total_bytes;
      const auto first = run.coordinator.commit_transfer(run.make_commit(grant));
      REQUIRE_OK(first);
      REQUIRE_EQ(run.coordinator.status().value().progress.bytes_committed, expected_bytes);
      // A replay of the same attempt is a duplicate: no extra authority, no
      // extra bytes, and the answer stays truthful about what changed.
      const auto replay = run.coordinator.commit_transfer(run.make_commit(grant));
      if (replay.ok()) {
        REQUIRE(replay.value().receipt.duplicate);
        REQUIRE_FALSE(replay.value().receipt.newly_committed);
      } else {
        REQUIRE_EQ(replay.code(), ErrorCode::StaleAttempt);
      }
      REQUIRE_EQ(run.coordinator.status().value().progress.bytes_committed, expected_bytes);
    }
    const ProgressSnapshot progress = run.coordinator.status().value().progress;
    REQUIRE(progress.accounting_closes());
  }
}

}  // namespace
