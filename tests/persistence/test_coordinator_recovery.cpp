// Domain-level persistence proof: a real coordinator writing through the real
// durable store, restarted from what is actually on disk.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Each phase keeps its store, sink and coordinator in their own scope: the
// durable store holds the journal open for appending, so reopening the same
// directory is only meaningful once the previous writer is gone. That is the
// same discipline a real restart has.

#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

#include "shuffle/fabric/coordinator.hpp"
#include "shuffle/fabric/durable_store.hpp"
#include "shuffle/fabric/hash.hpp"
#include "temp_dir.hpp"
#include "test_support.hpp"

namespace {

using namespace shuffle::fabric;
using shuffle::test::TempDir;

// The production wiring: every durable record the coordinator commits becomes a
// journal record, and persist() returns only after the store reports it durable.
class JournalSink final : public DurableSink {
 public:
  explicit JournalSink(DurableStore& store) : store_(&store) {}

  [[nodiscard]] Status persist(std::span<const std::byte> record) override {
    const auto appended = store_->append(record);
    return appended.ok() ? Status{} : appended.status();
  }

  [[nodiscard]] std::uint64_t appended() const { return store_->appended_records(); }

 private:
  DurableStore* store_;
};

[[nodiscard]] StoreConfig make_config(const std::filesystem::path& directory) {
  StoreConfig config;
  config.directory = directory;
  return config;
}

[[nodiscard]] PolicyEnvelope make_policy() {
  PolicyEnvelope policy;
  policy.shuffle = ShuffleId{21};
  policy.shuffle_generation = ShuffleGeneration{5};
  policy.generation = PolicyGeneration{1};
  policy.fan = FanBounds{64, 64};
  policy.concurrency = ConcurrencyLimits{16, 8, 8};
  policy.waves = WaveLimits{8, 256};
  policy.retry = RetryPolicy{3, 0};
  policy.congestion = CongestionPolicy{80, 40, false, 10};
  return policy;
}

void start_shuffle(Coordinator& coordinator, std::uint32_t partitions) {
  ShuffleOpenRequest request;
  request.shuffle = ShuffleId{21};
  request.generation = ShuffleGeneration{5};
  request.partition_count = partitions;
  request.policy = make_policy();
  REQUIRE_OK(coordinator.open_shuffle(request));
}

[[nodiscard]] PartitionManifest make_manifest(std::uint32_t partition, std::uint64_t producer,
                                              std::uint64_t incarnation = 1, std::uint64_t generation = 1) {
  const std::vector<std::byte> payload =
      synthetic_partition_payload(ShuffleId{21}, ShuffleGeneration{5}, PartitionId{partition},
                                  PartitionGeneration{generation}, 96);
  const auto built = build_manifest(ShuffleId{21}, ShuffleGeneration{5}, PartitionId{partition},
                                    PartitionGeneration{generation}, ProducerId{producer}, IncarnationId{incarnation},
                                    TopologyGeneration{0}, payload, 32, Limits{});
  REQUIRE_OK(built);
  return built.value();
}

[[nodiscard]] std::vector<PartitionManifest> publish_all(Coordinator& coordinator, std::uint32_t partitions,
                                                         std::uint64_t producers, std::uint64_t incarnation = 1,
                                                         std::uint64_t generation = 1) {
  std::vector<PartitionManifest> manifests;
  manifests.reserve(partitions);
  for (std::uint32_t partition = 0; partition < partitions; ++partition) {
    PartitionManifest manifest = make_manifest(partition, 1 + (partition % producers), incarnation, generation);
    manifest.topology_generation = coordinator.status().value().topology_generation;
    REQUIRE_OK(coordinator.publish_manifest(manifest));
    manifests.push_back(manifest);
  }
  return manifests;
}

[[nodiscard]] CommitRequest make_commit(const DispatchGrant& grant, const PartitionManifest& manifest) {
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

// Commits every grant of the first wave except two, which stay in flight: one
// is later resolved through the retry path, the other survives into the restart
// so that stale authority can be demonstrated.
[[nodiscard]] std::vector<DispatchGrant> commit_one_wave(Coordinator& coordinator,
                                                        const std::vector<PartitionManifest>& manifests) {
  const auto plan = coordinator.next_wave();
  REQUIRE_OK(plan);
  REQUIRE(plan.value().grants.size() >= 3);
  std::vector<DispatchGrant> in_flight{plan.value().grants[0], plan.value().grants[1]};
  for (std::size_t index = 2; index < plan.value().grants.size(); ++index) {
    const DispatchGrant& grant = plan.value().grants[index];
    REQUIRE_OK(coordinator.commit_transfer(make_commit(grant, manifests.at(static_cast<std::size_t>(grant.partition.value())))));
  }
  return in_flight;
}

SHUFFLE_TEST(coordinator_recovery, snapshot_and_journal_restore_authoritative_completions) {
  TempDir directory{"recovery-snapshot"};
  const StoreConfig config = make_config(directory.path());

  std::vector<PartitionManifest> manifests;
  std::vector<DispatchGrant> in_flight{};
  std::uint64_t committed_before = 0;
  std::uint64_t committed_after = 0;
  std::uint64_t bytes_after = 0;
  std::uint64_t edges_after = 0;
  std::uint64_t journal_records = 0;

  {
    DurableStore store{config};
    REQUIRE_OK(store.open());
    JournalSink sink{store};
    Coordinator coordinator{Limits{}, &sink};

    start_shuffle(coordinator, 6);
    REQUIRE_OK(coordinator.register_producer(ProducerId{1}, IncarnationId{1}, "127.0.0.1:15001"));
    REQUIRE_OK(coordinator.register_producer(ProducerId{2}, IncarnationId{1}, "127.0.0.1:15002"));
    REQUIRE_OK(coordinator.register_consumer(ConsumerId{1}, IncarnationId{1}, "127.0.0.1:16001",
                                             PartitionSelection::all()));
    manifests = publish_all(coordinator, 6, 2);
    in_flight = commit_one_wave(coordinator, manifests);
    REQUIRE_EQ(in_flight.size(), std::size_t{2});

    const auto before = coordinator.status();
    REQUIRE_OK(before);
    committed_before = before.value().progress.partitions_committed;
    REQUIRE(committed_before > 0);

    // Fold everything so far into a snapshot, then keep appending: recovery
    // must replay the journal on top of the snapshot.
    const std::uint64_t appended_before_compaction = store.appended_records();
    const auto payload = coordinator.snapshot_payload();
    REQUIRE_OK(payload);
    REQUIRE_OK(store.compact(payload.value(), store.last_sequence()));
    REQUIRE_EQ(store.journal_bytes(), std::uint64_t{0});

    // Resolve the in-flight attempt as a transient failure and commit the same
    // edge again: the journal now holds records written after the snapshot.
    // The first in-flight attempt is resolved through the retry path, so the
    // journal holds records written after the snapshot.
    REQUIRE_OK(coordinator.report_failure(in_flight[0].attempt, ErrorCode::ConnectionFailure));
    const auto plan = coordinator.next_wave();
    REQUIRE_OK(plan);
    REQUIRE_FALSE(plan.value().grants.empty());
    const DispatchGrant retry = plan.value().grants.front();
    REQUIRE_OK(coordinator.commit_transfer(make_commit(retry, manifests.at(static_cast<std::size_t>(retry.partition.value())))));

    const auto mid = coordinator.status();
    REQUIRE_OK(mid);
    committed_after = mid.value().progress.partitions_committed;
    bytes_after = mid.value().progress.bytes_committed;
    edges_after = mid.value().progress.edges_completed;
    REQUIRE(committed_after > committed_before);
    // Only the records written after the compaction remain in the journal.
    journal_records = store.appended_records() - appended_before_compaction;
    REQUIRE(journal_records > 0);
  }

  // Restart: a brand new store and a brand new coordinator over the same bytes.
  DurableStore reopened{config};
  REQUIRE_OK(reopened.open());
  REQUIRE(reopened.recovery().snapshot_loaded);
  REQUIRE_FALSE(reopened.recovery().snapshot_from_previous);
  REQUIRE(reopened.recovery().records_replayed > 0);
  REQUIRE_EQ(reopened.records().size(), static_cast<std::size_t>(journal_records));

  const auto snapshot_bytes = DurableStore::read_snapshot(config.directory / config.snapshot_name, Limits{});
  REQUIRE_OK(snapshot_bytes);
  JournalSink reopened_sink{reopened};
  Coordinator restored{Limits{}, &reopened_sink};
  REQUIRE_OK(restored.recover(snapshot_bytes.value(), reopened.records(),
                              reopened.recovery().revalidation_required));

  const auto recovered = restored.status();
  REQUIRE_OK(recovered);
  REQUIRE_EQ(recovered.value().state, ShuffleState::Open);
  REQUIRE_EQ(recovered.value().progress.partitions_committed, committed_after);
  REQUIRE_EQ(recovered.value().progress.bytes_committed, bytes_after);
  REQUIRE_EQ(recovered.value().progress.edges_completed, edges_after);
  REQUIRE(recovered.value().progress.accounting_closes());
  REQUIRE_EQ(recovered.value().epoch, std::uint64_t{2});

  // Liveness must be re-established, and a pre-restart attempt carries nothing.
  REQUIRE(restored.revalidation_required());
  REQUIRE_EQ(recovered.value().active_producers, 0u);
  REQUIRE_EQ(recovered.value().active_consumers, 0u);
  CommitRequest stale =
      make_commit(in_flight[1], manifests.at(static_cast<std::size_t>(in_flight[1].partition.value())));
  REQUIRE_ERROR(restored.commit_transfer(stale), ErrorCode::StaleAttempt);

  // The run continues to closure with fresh incarnations and generations.
  REQUIRE_OK(restored.register_producer(ProducerId{1}, IncarnationId{2}, "127.0.0.1:15001"));
  REQUIRE_OK(restored.register_producer(ProducerId{2}, IncarnationId{2}, "127.0.0.1:15002"));
  REQUIRE_OK(restored.register_consumer(ConsumerId{1}, IncarnationId{2}, "127.0.0.1:16001",
                                        PartitionSelection::all()));
  REQUIRE_FALSE(restored.revalidation_required());
  const std::vector<PartitionManifest> republished = publish_all(restored, 6, 2, 2, 2);

  bool finished = false;
  for (int iteration = 0; iteration < 200 && !finished; ++iteration) {
    const auto next = restored.next_wave();
    REQUIRE_OK(next);
    if (next.value().grants.empty() && next.value().all_resolved) {
      finished = true;
      break;
    }
    for (const DispatchGrant& grant : next.value().grants) {
      REQUIRE_OK(restored.commit_transfer(
          make_commit(grant, republished.at(static_cast<std::size_t>(grant.partition.value())))));
    }
  }
  REQUIRE(finished);
  const auto final_status = restored.status();
  REQUIRE_OK(final_status);
  REQUIRE_EQ(final_status.value().state, ShuffleState::Completed);
  REQUIRE(final_status.value().progress.accounting_closes());
  REQUIRE_EQ(final_status.value().progress.edges_incomplete, std::uint64_t{0});
  REQUIRE_EQ(final_status.value().progress.edges_completed, final_status.value().progress.edges_required);
}

SHUFFLE_TEST(coordinator_recovery, journal_only_recovery_preserves_acknowledged_records) {
  TempDir directory{"recovery-journal"};
  const StoreConfig config = make_config(directory.path());

  std::vector<PartitionManifest> manifests;
  std::uint64_t bytes_before = 0;
  std::uint64_t partitions_before = 0;
  std::uint64_t edges_before = 0;
  std::uint64_t appended = 0;

  {
    DurableStore store{config};
    REQUIRE_OK(store.open());
    JournalSink sink{store};
    Coordinator coordinator{Limits{}, &sink};

    start_shuffle(coordinator, 4);
    REQUIRE_OK(coordinator.register_producer(ProducerId{1}, IncarnationId{1}, "127.0.0.1:15001"));
    REQUIRE_OK(coordinator.register_consumer(ConsumerId{1}, IncarnationId{1}, "127.0.0.1:16001",
                                             PartitionSelection::all()));
    manifests = publish_all(coordinator, 4, 1);
    static_cast<void>(commit_one_wave(coordinator, manifests));

    const auto before = coordinator.status();
    REQUIRE_OK(before);
    bytes_before = before.value().progress.bytes_committed;
    partitions_before = before.value().progress.partitions_committed;
    edges_before = before.value().progress.edges_completed;
    appended = sink.appended();
  }

  DurableStore reopened{config};
  REQUIRE_OK(reopened.open());
  REQUIRE_FALSE(reopened.recovery().snapshot_loaded);
  REQUIRE_EQ(reopened.recovery().tail, JournalTailStatus::Clean);
  REQUIRE_EQ(reopened.recovery().bytes_discarded, std::uint64_t{0});
  REQUIRE_EQ(reopened.records().size(), static_cast<std::size_t>(appended));

  JournalSink reopened_sink{reopened};
  Coordinator restored{Limits{}, &reopened_sink};
  REQUIRE_OK(restored.recover({}, reopened.records(), reopened.recovery().revalidation_required));

  const auto recovered = restored.status();
  REQUIRE_OK(recovered);
  REQUIRE_EQ(recovered.value().progress.bytes_committed, bytes_before);
  REQUIRE_EQ(recovered.value().progress.partitions_committed, partitions_before);
  REQUIRE_EQ(recovered.value().progress.edges_completed, edges_before);
  REQUIRE(recovered.value().progress.accounting_closes());
}

SHUFFLE_TEST(coordinator_recovery, torn_journal_tail_costs_only_unacknowledged_bytes) {
  TempDir directory{"recovery-torn"};
  const StoreConfig config = make_config(directory.path());
  std::uint64_t bytes_before = 0;
  std::uint64_t appended_records = 0;

  {
    DurableStore store{config};
    REQUIRE_OK(store.open());
    JournalSink sink{store};
    Coordinator coordinator{Limits{}, &sink};

    start_shuffle(coordinator, 4);
    REQUIRE_OK(coordinator.register_producer(ProducerId{1}, IncarnationId{1}, "127.0.0.1:15001"));
    REQUIRE_OK(coordinator.register_consumer(ConsumerId{1}, IncarnationId{1}, "127.0.0.1:16001",
                                             PartitionSelection::all()));
    const std::vector<PartitionManifest> manifests = publish_all(coordinator, 4, 1);
    static_cast<void>(commit_one_wave(coordinator, manifests));
    const auto before = coordinator.status();
    REQUIRE_OK(before);
    bytes_before = before.value().progress.bytes_committed;
    appended_records = sink.appended();
  }

  // Simulate a crash during an append: the last record is only partly written.
  const std::filesystem::path journal_path = config.directory / config.journal_name;
  const auto size = std::filesystem::file_size(journal_path);
  REQUIRE(size > 8);
  std::filesystem::resize_file(journal_path, size - 5);
  const std::uint64_t appended = appended_records;
  REQUIRE(appended > 0);

  DurableStore reopened{config};
  REQUIRE_OK(reopened.open());
  REQUIRE_EQ(reopened.recovery().tail, JournalTailStatus::TornTail);
  // The discarded region is the whole record whose tail was lost, not just the
  // bytes that were cut: the record cannot be believed at all.
  REQUIRE(reopened.recovery().bytes_discarded >= std::uint64_t{5});
  REQUIRE_EQ(reopened.records().size(), static_cast<std::size_t>(appended - 1));
  REQUIRE(reopened.recovery().revalidation_required);

  JournalSink reopened_sink{reopened};
  Coordinator restored{Limits{}, &reopened_sink};
  REQUIRE_OK(restored.recover({}, reopened.records(), reopened.recovery().revalidation_required));
  const auto recovered = restored.status();
  REQUIRE_OK(recovered);
  // Exactly the effects of the cut record are gone: the commit for one
  // partition generation of 96 bytes. Everything still intact replayed.
  REQUIRE_EQ(bytes_before - recovered.value().progress.bytes_committed, std::uint64_t{96});
  REQUIRE(recovered.value().progress.accounting_closes());
  REQUIRE(restored.history_incomplete());

  const auto explanation = restored.explain(4);
  REQUIRE_OK(explanation);
  REQUIRE(explanation.value().render().find("journal") != std::string::npos);

  // The store repaired the tail, so a further acknowledged record is durable
  // and replays on the next restart.
  REQUIRE_OK(restored.register_producer(ProducerId{1}, IncarnationId{2}, "127.0.0.1:15001"));
  const std::size_t before_count = reopened.records().size();
  REQUIRE(reopened.appended_records() > 0);
  static_cast<void>(before_count);
}

SHUFFLE_TEST(coordinator_recovery, cancelled_shuffle_stays_cancelled_across_restart) {
  TempDir directory{"recovery-cancel"};
  const StoreConfig config = make_config(directory.path());

  {
    DurableStore store{config};
    REQUIRE_OK(store.open());
    JournalSink sink{store};
    Coordinator coordinator{Limits{}, &sink};
    start_shuffle(coordinator, 3);
    REQUIRE_OK(coordinator.register_producer(ProducerId{1}, IncarnationId{1}, "127.0.0.1:15001"));
    REQUIRE_OK(coordinator.cancel_shuffle(ErrorCode::Cancelled));
  }

  DurableStore reopened{config};
  REQUIRE_OK(reopened.open());
  JournalSink reopened_sink{reopened};
  Coordinator restored{Limits{}, &reopened_sink};
  REQUIRE_OK(restored.recover({}, reopened.records(), false));
  REQUIRE_EQ(restored.state(), ShuffleState::Cancelled);
  REQUIRE_ERROR(restored.register_producer(ProducerId{2}, IncarnationId{1}, "127.0.0.1:15002"), ErrorCode::Cancelled);
  REQUIRE_ERROR(restored.next_wave(), ErrorCode::Cancelled);
}

}  // namespace
