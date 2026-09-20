// Integration proof: the coordinator service, the durable store and the
// authority model running together in one process, including a restart.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// The other suites isolate one module each. This one wires the real pieces the
// way a deployment does: a CoordinatorServer over loopback TCP, CoordinatorClient
// sessions for a producer and a consumer, a journal-backed durable sink, real
// manifests with verified chunk evidence, then a restart from disk and a
// completion run under fresh incarnations.

#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

#include "shuffle/fabric/coordinator.hpp"
#include "shuffle/fabric/durable_store.hpp"
#include "shuffle/fabric/hash.hpp"
#include "shuffle/fabric/service.hpp"
#include "temp_dir.hpp"
#include "test_support.hpp"

namespace {

using namespace shuffle::fabric;
using shuffle::test::TempDir;

class JournalSink final : public DurableSink {
 public:
  explicit JournalSink(DurableStore& store) : store_(&store) {}
  [[nodiscard]] Status persist(std::span<const std::byte> record) override {
    const auto appended = store_->append(record);
    return appended.ok() ? Status{} : appended.status();
  }

 private:
  DurableStore* store_;
};

struct Settings {
  ShuffleId shuffle{31};
  ShuffleGeneration generation{4};
  std::uint32_t partitions{8};
  std::uint32_t producers{2};
  std::uint64_t payload_bytes{128};
  std::uint32_t chunk_bytes{48};
};

[[nodiscard]] Digest compute_stored_digest(const PartitionManifest& manifest) {
  return compute_manifest_digest(manifest);
}

[[nodiscard]] PolicyEnvelope make_policy(const Settings& settings) {
  PolicyEnvelope policy;
  policy.shuffle = settings.shuffle;
  policy.shuffle_generation = settings.generation;
  policy.generation = PolicyGeneration{1};
  policy.fan = FanBounds{64, 64};
  policy.concurrency = ConcurrencyLimits{8, 4, 4};
  policy.waves = WaveLimits{4, 64};
  policy.retry = RetryPolicy{3, 0};
  policy.congestion = CongestionPolicy{80, 40, false, 10};
  return policy;
}

[[nodiscard]] ClientOptions make_client_options(std::uint16_t port, ParticipantKind kind, std::uint64_t id,
                                                std::uint64_t incarnation) {
  ClientOptions options;
  options.host = "127.0.0.1";
  options.port = port;
  options.identity.kind = kind;
  options.identity.participant_id = id;
  options.identity.incarnation = IncarnationId{incarnation};
  options.identity.boot_nonce = id * 1000 + incarnation;
  return options;
}

// Publishes every partition the given producer owns and returns the accepted
// manifests in partition order.
[[nodiscard]] std::vector<PartitionManifest> publish_owned(CoordinatorClient& producer_client, const Settings& settings,
                                                           std::uint64_t producer, std::uint64_t incarnation) {
  std::vector<PartitionManifest> manifests(settings.partitions);
  for (std::uint32_t partition = 0; partition < settings.partitions; ++partition) {
    if ((1 + (partition % settings.producers)) != producer) {
      continue;
    }
    const std::vector<std::byte> payload =
        synthetic_partition_payload(settings.shuffle, settings.generation, PartitionId{partition},
                                    PartitionGeneration{1}, settings.payload_bytes);
    const auto built = build_manifest(settings.shuffle, settings.generation, PartitionId{partition},
                                      PartitionGeneration{1}, ProducerId{producer}, IncarnationId{incarnation},
                                      TopologyGeneration{0}, payload, settings.chunk_bytes, Limits{});
    REQUIRE_OK(built);
    const auto accepted = producer_client.publish_manifest(built.value());
    REQUIRE_OK(accepted);
    // The coordinator is the authority on the accepted manifest: it stamps the
    // topology generation it validated ownership under, so the accepted form is
    // read back rather than assumed.
    const auto stored = producer_client.manifest(PartitionId{partition});
    REQUIRE_OK(stored);
    REQUIRE_EQ(compute_manifest_digest(stored.value()), accepted.value().manifest_digest);
    manifests[partition] = stored.value();
  }
  return manifests;
}

// Commits every grant of one wave, verifying the chunk evidence the way a real
// consumer does.
// Commits every grant of an already-fetched plan. A plan must be committed (or
// reported as failed) before another one is requested, otherwise its grants stay
// in flight and consume the concurrency budget.
[[nodiscard]] std::uint64_t commit_grants(CoordinatorClient& consumer_client, const WavePlan& plan,
                                          const std::vector<PartitionManifest>& manifests) {
  std::uint64_t committed = 0;
  for (const DispatchGrant& grant : plan.grants) {
    const auto partition_index = static_cast<std::size_t>(grant.partition.value());
    const PartitionManifest& manifest = manifests.at(partition_index);
    const auto fetched = consumer_client.manifest(grant.partition);
    REQUIRE_OK(fetched);
    REQUIRE(fetched.value() == manifest);
    REQUIRE_EQ(compute_manifest_digest(fetched.value()), grant.manifest_digest);

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
    request.observed_partition_digest = compute_partition_digest(manifest);
    request.bytes = manifest.total_bytes;
    request.integrity_verified = true;
    for (const ChunkDescriptor& chunk : manifest.chunks) {
      request.observed_chunk_digests.push_back(chunk.digest);
    }
    const auto outcome = consumer_client.commit_transfer(request);
    REQUIRE_OK(outcome);
    ++committed;
  }
  return committed;
}

[[nodiscard]] std::uint64_t commit_one_wave(CoordinatorClient& consumer_client,
                                            const std::vector<PartitionManifest>& manifests) {
  const auto plan = consumer_client.next_wave();
  REQUIRE_OK(plan);
  return commit_grants(consumer_client, plan.value(), manifests);
}

SHUFFLE_TEST(integration, service_durable_store_and_restart_complete_a_shuffle) {
  const Settings settings;
  TempDir directory{"integration-shuffle"};
  StoreConfig config;
  config.directory = directory.path();

  std::uint64_t committed_first_phase = 0;
  std::uint64_t partitions_after_restart = 0;
  std::uint64_t edges_required = 0;
  std::uint64_t partitions_first_phase = 0;

  {
    DurableStore store{config};
    REQUIRE_OK(store.open());
    JournalSink sink{store};
    Coordinator coordinator{Limits{}, &sink};

    ServerOptions server_options;
    server_options.bind_host = "127.0.0.1";
    server_options.port = 0;
    server_options.max_sessions = 8;
    server_options.worker_threads = 2;
    CoordinatorServer server{coordinator, server_options};
    REQUIRE_OK(server.start());
    REQUIRE(server.port() > 0);

    CoordinatorClient producer_client;
    REQUIRE_OK(producer_client.connect(make_client_options(server.port(), ParticipantKind::Producer, 1, 1)));
    CoordinatorClient consumer_client;
    REQUIRE_OK(consumer_client.connect(make_client_options(server.port(), ParticipantKind::Consumer, 1, 1)));

    ShuffleOpenRequest open;
    open.shuffle = settings.shuffle;
    open.generation = settings.generation;
    open.partition_count = settings.partitions;
    open.policy = make_policy(settings);
    REQUIRE_OK(producer_client.open_shuffle(open));

    const auto producer = producer_client.register_participant(ParticipantKind::Producer, 1, IncarnationId{1},
                                                              "127.0.0.1:18001", PartitionSelection::all());
    REQUIRE_OK(producer);
    const auto consumer = consumer_client.register_participant(ParticipantKind::Consumer, 1, IncarnationId{1},
                                                              "127.0.0.1:18002", PartitionSelection::all());
    REQUIRE_OK(consumer);

    // A session publishes only its own partitions: the service binds provenance
    // to the handshake identity, so each producer process publishes its own.
    CoordinatorClient second_producer_client;
    REQUIRE_OK(second_producer_client.connect(make_client_options(server.port(), ParticipantKind::Producer, 2, 1)));
    REQUIRE_OK(second_producer_client.register_participant(ParticipantKind::Producer, 2, IncarnationId{1},
                                                           "127.0.0.1:18003", PartitionSelection::all()));
    std::vector<PartitionManifest> manifests = publish_owned(producer_client, settings, 1, 1);
    const std::vector<PartitionManifest> second = publish_owned(second_producer_client, settings, 2, 1);
    for (std::size_t index = 0; index < manifests.size(); ++index) {
      if (second[index].total_bytes > 0) {
        manifests[index] = second[index];
      }
      REQUIRE_EQ(manifests[index].total_bytes, settings.payload_bytes);
    }

    // One wave only, so the restart has unfinished work to reason about.
    committed_first_phase = commit_one_wave(consumer_client, manifests);

    const auto progress = consumer_client.progress();
    REQUIRE_OK(progress);
    edges_required = progress.value().edges_required;
    REQUIRE_EQ(edges_required, std::uint64_t{settings.partitions});
    REQUIRE(progress.value().accounting_closes());
    REQUIRE_EQ(progress.value().edges_completed, committed_first_phase);
    partitions_first_phase = progress.value().partitions_committed;
    partitions_after_restart = partitions_first_phase;
    REQUIRE(partitions_after_restart > 0);

    const auto status = consumer_client.status();
    REQUIRE_OK(status);
    REQUIRE(status.value().durable);
    REQUIRE_EQ(status.value().active_producers, settings.producers);

    REQUIRE_OK(server.stop());
    REQUIRE_FALSE(server.running());
    REQUIRE_OK(producer_client.close());
    REQUIRE_OK(second_producer_client.close());
    REQUIRE_OK(consumer_client.close());

    // Fold the run into a snapshot so the restart exercises both files.
    const auto payload = coordinator.snapshot_payload();
    REQUIRE_OK(payload);
    REQUIRE_OK(store.compact(payload.value(), store.last_sequence()));
  }

  {
    DurableStore store{config};
    REQUIRE_OK(store.open());
    REQUIRE(store.recovery().snapshot_loaded);
    JournalSink sink{store};
    Coordinator coordinator{Limits{}, &sink};

    const auto snapshot = DurableStore::read_snapshot(config.directory / config.snapshot_name, Limits{});
    REQUIRE_OK(snapshot);
    REQUIRE_OK(coordinator.recover(snapshot.value(), store.records(), store.recovery().revalidation_required));
    REQUIRE_EQ(coordinator.epoch(), std::uint64_t{2});
    REQUIRE(coordinator.revalidation_required());

    const auto recovered = coordinator.status();
    REQUIRE_OK(recovered);
    REQUIRE_EQ(recovered.value().progress.partitions_committed, partitions_after_restart);
    REQUIRE(recovered.value().progress.accounting_closes());

    ServerOptions server_options;
    server_options.bind_host = "127.0.0.1";
    server_options.port = 0;
    server_options.max_sessions = 8;
    server_options.worker_threads = 2;
    CoordinatorServer server{coordinator, server_options};
    REQUIRE_OK(server.start());

    CoordinatorClient producer_client;
    REQUIRE_OK(producer_client.connect(make_client_options(server.port(), ParticipantKind::Producer, 1, 2)));
    CoordinatorClient second_producer_client;
    REQUIRE_OK(second_producer_client.connect(make_client_options(server.port(), ParticipantKind::Producer, 2, 2)));
    CoordinatorClient consumer_client;
    REQUIRE_OK(consumer_client.connect(make_client_options(server.port(), ParticipantKind::Consumer, 1, 2)));

    // Re-registration with fresh incarnations is what re-establishes authority.
    REQUIRE_OK(producer_client.register_participant(ParticipantKind::Producer, 1, IncarnationId{2},
                                                    "127.0.0.1:18001", PartitionSelection::all()));
    REQUIRE_OK(second_producer_client.register_participant(ParticipantKind::Producer, 2, IncarnationId{2},
                                                          "127.0.0.1:18003", PartitionSelection::all()));
    REQUIRE_OK(consumer_client.register_participant(ParticipantKind::Consumer, 1, IncarnationId{2},
                                                    "127.0.0.1:18002", PartitionSelection::all()));
    const auto after_revalidation = consumer_client.status();
    REQUIRE_OK(after_revalidation);
    REQUIRE_FALSE(after_revalidation.value().revalidation_required);

    // Re-produce every partition under the new incarnation and finish.
    std::vector<PartitionManifest> manifests(settings.partitions);
    for (std::uint32_t partition = 0; partition < settings.partitions; ++partition) {
      const std::uint64_t owner = 1 + (partition % settings.producers);
      const std::vector<std::byte> payload =
          synthetic_partition_payload(settings.shuffle, settings.generation, PartitionId{partition},
                                      PartitionGeneration{2}, settings.payload_bytes);
      const auto built = build_manifest(settings.shuffle, settings.generation, PartitionId{partition},
                                        PartitionGeneration{2}, ProducerId{owner}, IncarnationId{2},
                                        TopologyGeneration{0}, payload, settings.chunk_bytes, Limits{});
      REQUIRE_OK(built);
      CoordinatorClient& owner_client = owner == 1 ? producer_client : second_producer_client;
      const auto accepted = owner_client.publish_manifest(built.value());
      REQUIRE_OK(accepted);
      const auto stored = owner_client.manifest(PartitionId{partition});
      REQUIRE_OK(stored);
      REQUIRE_EQ(compute_stored_digest(stored.value()), accepted.value().manifest_digest);
      manifests[partition] = stored.value();
    }

    std::uint64_t committed_second_phase = 0;
    bool finished = false;
    for (int iteration = 0; iteration < 64 && !finished; ++iteration) {
      const auto plan = consumer_client.next_wave();
      REQUIRE_OK(plan);
      if (plan.value().grants.empty()) {
        // Nothing dispatchable left: the plan says whether every required edge
        // is resolved or the fabric is merely idle for now.
        finished = plan.value().all_resolved;
        break;
      }
      committed_second_phase += commit_grants(consumer_client, plan.value(), manifests);
    }
    REQUIRE(finished);

    const auto final_progress = consumer_client.progress();
    REQUIRE_OK(final_progress);
    REQUIRE(final_progress.value().accounting_closes());
    REQUIRE_EQ(final_progress.value().edges_completed, edges_required);
    REQUIRE_EQ(final_progress.value().edges_incomplete, std::uint64_t{0});
    REQUIRE_EQ(final_progress.value().partitions_committed, std::uint64_t{settings.partitions});
    // Bytes are cumulative over partition generations: the first phase committed
    // one generation for some partitions, the second phase committed generation 2
    // for all of them, and nothing was counted twice.
    REQUIRE_EQ(final_progress.value().bytes_committed,
               (partitions_first_phase + settings.partitions) * settings.payload_bytes);
    REQUIRE(committed_first_phase + committed_second_phase >= edges_required);

    const auto final_status = consumer_client.status();
    REQUIRE_OK(final_status);
    REQUIRE_EQ(final_status.value().state, ShuffleState::Completed);
    REQUIRE_EQ(final_status.value().epoch, std::uint64_t{2});
    REQUIRE_EQ(final_status.value().in_flight, 0u);

    REQUIRE_OK(server.stop());
  }
}

SHUFFLE_TEST(integration, service_refuses_wrong_identity_and_keeps_serving) {
  TempDir directory{"integration-refusals"};
  DurableStore store{StoreConfig{directory.path(), "state.snapshot", "state.journal", Limits{}}};
  REQUIRE_OK(store.open());
  JournalSink sink{store};
  Coordinator coordinator{Limits{}, &sink};

  ServerOptions server_options;
  server_options.port = 0;
  server_options.max_sessions = 4;
  server_options.worker_threads = 2;
  CoordinatorServer server{coordinator, server_options};
  REQUIRE_OK(server.start());

  ShuffleOpenRequest open;
  open.shuffle = ShuffleId{77};
  open.generation = ShuffleGeneration{1};
  open.partition_count = 2;
  open.policy.shuffle = open.shuffle;
  open.policy.shuffle_generation = open.generation;
  open.policy.generation = PolicyGeneration{1};
  open.policy.fan = FanBounds{8, 8};
  open.policy.concurrency = ConcurrencyLimits{4, 2, 2};
  open.policy.waves = WaveLimits{4, 32};
  open.policy.retry = RetryPolicy{2, 0};
  open.policy.congestion = CongestionPolicy{80, 40, false, 10};

  CoordinatorClient client;
  REQUIRE_OK(client.connect(make_client_options(server.port(), ParticipantKind::Producer, 5, 9)));
  REQUIRE_OK(client.open_shuffle(open));

  // The same session cannot register a different participant id: the session
  // envelope is the identity, payload provenance is not trusted.
  const auto mismatched = client.register_participant(ParticipantKind::Producer, 6, IncarnationId{9},
                                                     "127.0.0.1:19001", PartitionSelection::all());
  REQUIRE_FALSE(mismatched.ok());
  REQUIRE_EQ(mismatched.code(), ErrorCode::IdentityMismatch);

  // The connection stays usable and a correct request succeeds.
  REQUIRE_OK(client.register_participant(ParticipantKind::Producer, 5, IncarnationId{9}, "127.0.0.1:19001",
                                         PartitionSelection::all()));
  const auto status = client.status();
  REQUIRE_OK(status);
  REQUIRE_EQ(status.value().active_producers, 1u);
  REQUIRE_OK(client.close());
  REQUIRE_OK(server.stop());
}

}  // namespace
