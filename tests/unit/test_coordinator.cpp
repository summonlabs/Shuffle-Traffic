// Deterministic unit tests for coordinator authority: registration, manifest
// acceptance, attempt-bound completion, refusal and durability ordering.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include <functional>
#include <unordered_map>
#include <string>
#include <vector>

#include "shuffle/fabric/coordinator.hpp"
#include "shuffle/fabric/hash.hpp"
#include "test_support.hpp"

namespace {

using namespace shuffle::fabric;

// Records every durable payload in order and can inject a durability failure.
class RecordingSink final : public DurableSink {
 public:
  std::vector<std::vector<std::byte>> records{};
  bool fail_next{false};
  std::uint64_t failures{0};

  [[nodiscard]] Status persist(std::span<const std::byte> record) override {
    if (fail_next) {
      fail_next = false;
      ++failures;
      return Status{make_error(ErrorCode::PersistenceFailure, "injected durability failure")};
    }
    records.emplace_back(record.begin(), record.end());
    return Status{};
  }
};

// Calls back into the coordinator from inside the durability point: exactly the
// shape a callback-under-lock defect would take.
class CallbackSink final : public DurableSink {
 public:
  std::function<Status()> callback{};
  std::uint32_t calls{0};

  [[nodiscard]] Status persist(std::span<const std::byte>) override {
    ++calls;
    return callback ? callback() : Status{};
  }
};

[[nodiscard]] PolicyEnvelope make_policy(std::uint32_t partitions) {
  PolicyEnvelope policy;
  policy.shuffle = ShuffleId{7};
  policy.shuffle_generation = ShuffleGeneration{3};
  policy.generation = PolicyGeneration{1};
  policy.fan = FanBounds{partitions + 1, partitions + 1};
  policy.concurrency = ConcurrencyLimits{32, 32, 32};
  policy.waves = WaveLimits{16, 512};
  policy.retry = RetryPolicy{3, 0};
  policy.congestion = CongestionPolicy{80, 40, false, 10};
  return policy;
}

// Defined after the harness; declared here because the harness publishes.
[[nodiscard]] PartitionManifest make_manifest(std::uint32_t partition, std::uint64_t producer, std::uint64_t incarnation,
                                              std::uint64_t bytes = 128, std::uint64_t generation = 1);

struct Harness {
  RecordingSink sink{};
  Coordinator coordinator;
  std::uint32_t partitions{8};
  std::unordered_map<std::uint32_t, PartitionManifest> accepted{};

  explicit Harness(std::uint32_t partition_count = 8, DurableSink* custom = nullptr)
      : coordinator(Limits{}, custom != nullptr ? custom : &sink), partitions(partition_count) {}

  void open() {
    ShuffleOpenRequest request;
    request.shuffle = ShuffleId{7};
    request.generation = ShuffleGeneration{3};
    request.partition_count = partitions;
    request.policy = make_policy(partitions);
    REQUIRE_OK(coordinator.open_shuffle(request));
  }

  void add_producer(std::uint64_t id, std::uint64_t incarnation) {
    const auto outcome = coordinator.register_producer(ProducerId{id}, IncarnationId{incarnation},
                                                       "127.0.0.1:" + std::to_string(9000 + id));
    REQUIRE_OK(outcome);
  }

  void add_consumer(std::uint64_t id, std::uint64_t incarnation, const PartitionSelection& selection) {
    const auto outcome = coordinator.register_consumer(ConsumerId{id}, IncarnationId{incarnation},
                                                       "127.0.0.1:" + std::to_string(9500 + id), selection);
    REQUIRE_OK(outcome);
  }

  // Publishes a manifest for every partition and keeps the accepted form: the
  // coordinator is the authority on the topology generation it stamps.
  void publish_all(std::uint32_t producers, std::uint64_t bytes = 128, std::uint64_t incarnation = 1,
                   std::uint64_t generation = 1) {
    const TopologyGeneration topology_generation = coordinator.status().value().topology_generation;
    for (std::uint32_t partition = 0; partition < partitions; ++partition) {
      const std::uint64_t producer = 1 + (partition % producers);
      PartitionManifest manifest = make_manifest(partition, producer, incarnation, bytes, generation);
      manifest.topology_generation = topology_generation;
      const auto accepted_result = coordinator.publish_manifest(manifest);
      REQUIRE_OK(accepted_result);
      accepted[partition] = manifest;
    }
  }

  [[nodiscard]] const PartitionManifest& manifest_of(std::uint32_t partition) const { return accepted.at(partition); }
};

[[nodiscard]] PartitionManifest make_manifest(std::uint32_t partition, std::uint64_t producer, std::uint64_t incarnation,
                                              std::uint64_t bytes, std::uint64_t generation) {
  const std::vector<std::byte> payload =
      synthetic_partition_payload(ShuffleId{7}, ShuffleGeneration{3}, PartitionId{partition}, PartitionGeneration{generation}, bytes);
  const auto built = build_manifest(ShuffleId{7}, ShuffleGeneration{3}, PartitionId{partition}, PartitionGeneration{generation},
                                    ProducerId{producer}, IncarnationId{incarnation}, TopologyGeneration{0}, payload, 64,
                                    Limits{});
  REQUIRE_OK(built);
  return built.value();
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


SHUFFLE_TEST(coordinator, open_advances_epoch_and_refuses_second_open) {
  Harness harness(4);
  REQUIRE_EQ(harness.coordinator.state(), ShuffleState::Closed);
  REQUIRE_EQ(harness.coordinator.epoch(), std::uint64_t{0});

  harness.open();
  REQUIRE_EQ(harness.coordinator.state(), ShuffleState::Open);
  REQUIRE_EQ(harness.coordinator.epoch(), std::uint64_t{1});
  REQUIRE(harness.coordinator.durable());

  ShuffleOpenRequest again;
  again.shuffle = ShuffleId{7};
  again.generation = ShuffleGeneration{3};
  again.partition_count = 4;
  again.policy = make_policy(4);
  REQUIRE_ERROR(harness.coordinator.open_shuffle(again), ErrorCode::InvalidState);

  ShuffleOpenRequest mismatched = again;
  mismatched.policy.shuffle = ShuffleId{99};
  Harness other(4);
  REQUIRE_ERROR(other.coordinator.open_shuffle(mismatched), ErrorCode::PolicyMismatch);

  Harness zero(4);
  ShuffleOpenRequest empty = again;
  empty.partition_count = 0;
  REQUIRE_ERROR(zero.coordinator.open_shuffle(empty), ErrorCode::InvalidArgument);
}

SHUFFLE_TEST(coordinator, durability_failure_leaves_no_partial_application) {
  Harness harness(4);
  harness.open();
  const auto before = harness.coordinator.status();
  REQUIRE_OK(before);

  harness.sink.fail_next = true;
  const auto rejected = harness.coordinator.register_producer(ProducerId{1}, IncarnationId{1}, "127.0.0.1:9001");
  REQUIRE_ERROR(rejected, ErrorCode::PersistenceFailure);

  const auto after = harness.coordinator.status();
  REQUIRE_OK(after);
  REQUIRE_EQ(after.value().active_producers, 0u);
  REQUIRE_EQ(after.value().topology_generation.value(), before.value().topology_generation.value());

  // The refused mutation left no residue: the same call succeeds afterwards.
  harness.add_producer(1, 1);
  REQUIRE_EQ(harness.coordinator.status().value().active_producers, 1u);
}

SHUFFLE_TEST(coordinator, re_entrant_callbacks_are_refused_not_deadlocked) {
  CallbackSink sink;
  Harness harness(4, &sink);
  harness.open();
  sink.callback = [&harness]() { return harness.coordinator.progress().status(); };
  const auto outcome = harness.coordinator.register_producer(ProducerId{1}, IncarnationId{1}, "127.0.0.1:9001");
  REQUIRE_ERROR(outcome, ErrorCode::InvalidState);
  REQUIRE(sink.calls > 0);
  // The guard unwound: the coordinator is usable again.
  sink.callback = nullptr;
  harness.add_producer(1, 1);
}

SHUFFLE_TEST(coordinator, registration_supersedes_previous_incarnation) {
  Harness harness(4);
  harness.open();
  harness.add_producer(1, 1);
  harness.add_consumer(1, 1, PartitionSelection::all());

  const auto first = harness.coordinator.register_producer(ProducerId{1}, IncarnationId{2}, "127.0.0.1:9001");
  REQUIRE_OK(first);
  REQUIRE(first.value().superseded_previous);
  REQUIRE(first.value().topology_generation.value() > 0);

  // The superseded incarnation can no longer change state or publish.
  REQUIRE_ERROR(harness.coordinator.set_participant_state(ParticipantKind::Producer, 1, IncarnationId{1},
                                                          ParticipantState::Failed),
                ErrorCode::StaleIncarnation);
  REQUIRE_ERROR(harness.coordinator.publish_manifest(make_manifest(0, 1, 1)), ErrorCode::StaleIncarnation);
}

SHUFFLE_TEST(coordinator, manifest_requires_current_ownership) {
  Harness harness(4);
  harness.open();
  harness.add_producer(1, 1);
  harness.add_producer(2, 1);

  // Partition 1 is owned by producer 2 under round-robin ownership.
  REQUIRE_ERROR(harness.coordinator.publish_manifest(make_manifest(1, 1, 1)), ErrorCode::PartitionNotOwned);
  REQUIRE_OK(harness.coordinator.publish_manifest(make_manifest(1, 2, 1)));

  // Publishing the same generation with different content is a contradiction.
  PartitionManifest divergent = make_manifest(1, 2, 1, 256);
  REQUIRE_ERROR(harness.coordinator.publish_manifest(divergent), ErrorCode::DivergentCommit);

  // Idempotent republication of the identical manifest is accepted.
  REQUIRE_OK(harness.coordinator.publish_manifest(make_manifest(1, 2, 1)));

  // A manifest outside the partition space is refused.
  PartitionManifest outside = make_manifest(0, 1, 1);
  outside.partition = PartitionId{99};
  REQUIRE_ERROR(harness.coordinator.publish_manifest(outside), ErrorCode::UnknownPartition);

  // Joining producers shift ownership: partition 1 belongs to producer 2, so a
  // manifest signed by producer 1 for it is refused as not owned.
  REQUIRE_OK(harness.coordinator.register_producer(ProducerId{3}, IncarnationId{1}, "127.0.0.1:9003"));
  REQUIRE_ERROR(harness.coordinator.publish_manifest(make_manifest(1, 1, 1)), ErrorCode::PartitionNotOwned);
}

SHUFFLE_TEST(coordinator, completion_requires_attempt_bound_evidence) {
  Harness harness(4);
  harness.open();
  harness.add_producer(1, 1);
  harness.add_consumer(1, 1, PartitionSelection::all());
  harness.publish_all(1);

  const auto plan = harness.coordinator.next_wave();
  REQUIRE_OK(plan);
  REQUIRE_FALSE(plan.value().grants.empty());
  const DispatchGrant grant = plan.value().grants.front();
  const PartitionManifest manifest = harness.manifest_of(static_cast<std::uint32_t>(grant.partition.value()));

  CommitRequest forged;
  forged.edge = EdgeKey{grant.partition, grant.partition_generation, grant.consumer};
  forged.attempt = TransferAttemptId{grant.attempt.value() + 1000};
  forged.shuffle_generation = grant.shuffle_generation;
  forged.producer = grant.producer;
  forged.producer_incarnation = grant.producer_incarnation;
  forged.consumer = grant.consumer;
  forged.consumer_incarnation = grant.consumer_incarnation;
  forged.manifest_digest = compute_manifest_digest(manifest);
  forged.observed_partition_digest = forged.manifest_digest;
  forged.bytes = manifest.total_bytes;
  forged.integrity_verified = true;
  for (const ChunkDescriptor& chunk : manifest.chunks) {
    forged.observed_chunk_digests.push_back(chunk.digest);
  }
  REQUIRE_ERROR(harness.coordinator.commit_transfer(forged), ErrorCode::StaleAttempt);

  CommitRequest unverified = make_commit(grant, manifest);
  unverified.integrity_verified = false;
  REQUIRE_ERROR(harness.coordinator.commit_transfer(unverified), ErrorCode::IntegrityFailure);

  CommitRequest partial_evidence = make_commit(grant, manifest);
  partial_evidence.observed_chunk_digests.pop_back();
  REQUIRE_ERROR(harness.coordinator.commit_transfer(partial_evidence), ErrorCode::IntegrityFailure);

  CommitRequest corrupt_evidence = make_commit(grant, manifest);
  corrupt_evidence.observed_chunk_digests[0] = sha256("not the payload");
  REQUIRE_ERROR(harness.coordinator.commit_transfer(corrupt_evidence), ErrorCode::DigestMismatch);

  CommitRequest wrong_bytes = make_commit(grant, manifest);
  wrong_bytes.bytes += 1;
  REQUIRE_ERROR(harness.coordinator.commit_transfer(wrong_bytes), ErrorCode::AccountingMismatch);

  CommitRequest wrong_incarnation = make_commit(grant, manifest);
  wrong_incarnation.producer_incarnation = IncarnationId{99};
  REQUIRE_ERROR(harness.coordinator.commit_transfer(wrong_incarnation), ErrorCode::StaleIncarnation);

  const auto accepted = harness.coordinator.commit_transfer(make_commit(grant, manifest));
  REQUIRE_OK(accepted);
  REQUIRE(accepted.value().receipt.newly_committed);
  REQUIRE(accepted.value().receipt.edge_newly_completed);
}

SHUFFLE_TEST(coordinator, full_shuffle_completes_with_exactly_once_accounting) {
  Harness harness(8);
  harness.open();
  harness.add_producer(1, 1);
  harness.add_producer(2, 1);
  harness.add_consumer(1, 1, PartitionSelection::all());
  harness.add_consumer(2, 1, PartitionSelection::range(PartitionId{0}, PartitionId{4}));
  harness.publish_all(2);

  const auto status = harness.coordinator.status();
  REQUIRE_OK(status);
  REQUIRE_EQ(status.value().progress.edges_required, std::uint64_t{12});

  std::uint64_t committed = 0;
  bool finished = false;
  for (int iteration = 0; iteration < 200 && !finished; ++iteration) {
    const auto plan = harness.coordinator.next_wave();
    REQUIRE_OK(plan);
    if (plan.value().grants.empty()) {
      REQUIRE(plan.value().all_resolved);
      finished = true;
      break;
    }
    for (const DispatchGrant& grant : plan.value().grants) {
      const PartitionManifest manifest = harness.manifest_of(static_cast<std::uint32_t>(grant.partition.value()));

      const auto outcome = harness.coordinator.commit_transfer(make_commit(grant, manifest));
      REQUIRE_OK(outcome);
      ++committed;
    }
    REQUIRE_OK(harness.coordinator.advance_tick());
  }
  REQUIRE(finished);
  REQUIRE_EQ(committed, std::uint64_t{12});

  const auto final_status = harness.coordinator.status();
  REQUIRE_OK(final_status);
  REQUIRE_EQ(final_status.value().state, ShuffleState::Completed);
  REQUIRE(final_status.value().progress.accounting_closes());
  REQUIRE_EQ(final_status.value().progress.edges_completed, std::uint64_t{12});
  REQUIRE_EQ(final_status.value().progress.edges_incomplete, std::uint64_t{0});
  REQUIRE_EQ(final_status.value().progress.partitions_committed, std::uint64_t{8});
  REQUIRE_EQ(final_status.value().progress.bytes_committed, std::uint64_t{8 * 128});

  // Completion is durable: the last record written describes the completed state.
  bool saw_completion = false;
  for (const std::vector<std::byte>& record : harness.sink.records) {
    ByteReader reader{record, Limits{}, "durable record"};
    const auto kind = peek_record_kind(reader);
    REQUIRE_OK(kind);
    if (kind.value() != RecordKind::ShuffleState) {
      continue;
    }
    const auto decoded = ShuffleStateRecord::decode(reader);
    REQUIRE_OK(decoded);
    REQUIRE_OK(reader.require_end());
    if (decoded.value().state == ShuffleState::Completed) {
      saw_completion = true;
    }
  }
  REQUIRE(saw_completion);
}

SHUFFLE_TEST(coordinator, failure_classification_and_retry_budget) {
  Harness harness(4);
  harness.open();
  harness.add_producer(1, 1);
  harness.add_consumer(1, 1, PartitionSelection::all());
  harness.publish_all(1);

  // Deferred conditions are scheduling pressure, not transfer failures.
  const auto plan = harness.coordinator.next_wave();
  REQUIRE_OK(plan);
  const DispatchGrant grant = plan.value().grants.front();
  REQUIRE_ERROR(harness.coordinator.report_failure(grant.attempt, ErrorCode::BackpressureActive),
                ErrorCode::InvalidArgument);

  const auto transient = harness.coordinator.report_failure(grant.attempt, ErrorCode::ConnectionFailure);
  REQUIRE_OK(transient);
  REQUIRE_FALSE(transient.value().permanent);
  REQUIRE_EQ(transient.value().attempts, 1u);

  // A fresh incarnation supersedes the producer. Content produced by the
  // superseded incarnation is not dispatchable: a new attempt could never
  // commit, so the partition must be produced again instead.
  REQUIRE_OK(harness.coordinator.register_producer(ProducerId{1}, IncarnationId{2}, "127.0.0.1:9001"));
  const auto stalled = harness.coordinator.next_wave();
  REQUIRE_OK(stalled);
  REQUIRE(stalled.value().grants.empty());
  REQUIRE(stalled.value().skipped_unproduced > 0);

  harness.accepted.clear();
  // A new incarnation re-produces the partitions as a new generation.
  harness.publish_all(1, 128, 2, 2);
  const auto replan = harness.coordinator.next_wave();
  REQUIRE_OK(replan);
  REQUIRE_FALSE(replan.value().grants.empty());
  const DispatchGrant second = replan.value().grants.front();
  REQUIRE_EQ(second.producer_incarnation, IncarnationId{2});

  const auto authority_failure = harness.coordinator.report_failure(second.attempt, ErrorCode::AuthorityDenied);
  REQUIRE_OK(authority_failure);
  REQUIRE(authority_failure.value().permanent);

  const auto explanation = harness.coordinator.explain(4);
  REQUIRE_OK(explanation);
  REQUIRE(explanation.value().render().find("ConnectionFailure") != std::string::npos);
}

SHUFFLE_TEST(coordinator, cancellation_refuses_late_authority) {
  Harness harness(4);
  harness.open();
  harness.add_producer(1, 1);
  harness.add_consumer(1, 1, PartitionSelection::all());
  harness.publish_all(1);

  const auto plan = harness.coordinator.next_wave();
  REQUIRE_OK(plan);
  REQUIRE_FALSE(plan.value().grants.empty());
  const DispatchGrant grant = plan.value().grants.front();
  const PartitionManifest manifest = harness.manifest_of(static_cast<std::uint32_t>(grant.partition.value()));

  REQUIRE_OK(harness.coordinator.cancel_shuffle(ErrorCode::Cancelled));
  REQUIRE_EQ(harness.coordinator.state(), ShuffleState::Cancelled);

  // Authority that was in flight before the cancellation cannot commit after it.
  REQUIRE_ERROR(harness.coordinator.commit_transfer(make_commit(grant, manifest)), ErrorCode::Cancelled);
  REQUIRE_ERROR(harness.coordinator.next_wave(), ErrorCode::Cancelled);
  REQUIRE_ERROR(harness.coordinator.report_failure(grant.attempt, ErrorCode::ConnectionFailure), ErrorCode::Cancelled);
  REQUIRE_ERROR(harness.coordinator.register_producer(ProducerId{5}, IncarnationId{1}, "127.0.0.1:9005"),
                ErrorCode::Cancelled);

  // Cancellation is idempotent.
  REQUIRE_OK(harness.coordinator.cancel_shuffle(ErrorCode::Cancelled));
}

SHUFFLE_TEST(coordinator, congestion_evidence_is_fenced_by_generation_and_incarnation) {
  Harness harness(4);
  harness.open();
  harness.add_producer(1, 1);
  harness.add_consumer(1, 1, PartitionSelection::all());
  harness.publish_all(1);

  CongestionIntent intent;
  intent.reporter_kind = ParticipantKind::Consumer;
  intent.reporter_id = 1;
  intent.reporter_incarnation = IncarnationId{1};
  intent.consumer = ConsumerId{1};
  intent.level = 90;
  intent.policy_generation = PolicyGeneration{1};
  intent.observed_at = TickId{0};
  REQUIRE_OK(harness.coordinator.update_congestion(intent));

  const auto paused = harness.coordinator.next_wave();
  REQUIRE_OK(paused);
  REQUIRE(paused.value().grants.empty());
  REQUIRE(paused.value().deferred_destination_pressure > 0);

  // A stale reporter incarnation cannot push evidence.
  CongestionIntent stale = intent;
  stale.reporter_incarnation = IncarnationId{42};
  REQUIRE_ERROR(harness.coordinator.update_congestion(stale), ErrorCode::StaleIncarnation);

  // Evidence from another policy generation is refused.
  CongestionIntent wrong_policy = intent;
  wrong_policy.policy_generation = PolicyGeneration{9};
  REQUIRE_ERROR(harness.coordinator.update_congestion(wrong_policy), ErrorCode::StalePolicy);

  // Evidence observed in the future is refused.
  CongestionIntent future = intent;
  future.observed_at = TickId{1000};
  REQUIRE_ERROR(harness.coordinator.update_congestion(future), ErrorCode::InvalidArgument);

  // Releasing the pressure restores dispatch.
  CongestionIntent released = intent;
  released.level = 0;
  REQUIRE_OK(harness.coordinator.update_congestion(released));
  REQUIRE_OK(harness.coordinator.advance_tick());
  const auto resumed = harness.coordinator.next_wave();
  REQUIRE_OK(resumed);
  REQUIRE_FALSE(resumed.value().grants.empty());
}

SHUFFLE_TEST(coordinator, recovery_fences_stale_authority_and_requires_revalidation) {
  Harness first(4);
  first.open();
  first.add_producer(1, 1);
  first.add_consumer(1, 1, PartitionSelection::all());
  first.publish_all(1);

  const auto plan = first.coordinator.next_wave();
  REQUIRE_OK(plan);
  REQUIRE_FALSE(plan.value().grants.empty());
  const DispatchGrant stale_grant = plan.value().grants.front();
  const PartitionManifest manifest = first.manifest_of(static_cast<std::uint32_t>(stale_grant.partition.value()));
  REQUIRE_OK(first.coordinator.commit_transfer(make_commit(stale_grant, manifest)));

  const auto snapshot = first.coordinator.snapshot_payload();
  REQUIRE_OK(snapshot);
  const std::vector<std::byte> snapshot_bytes = snapshot.value();
  const std::vector<std::vector<std::byte>> journal;

  Harness second(4);
  REQUIRE_OK(second.coordinator.recover(snapshot_bytes, journal, false));
  REQUIRE_EQ(second.coordinator.state(), ShuffleState::Open);
  REQUIRE_EQ(second.coordinator.epoch(), std::uint64_t{2});
  REQUIRE(second.coordinator.revalidation_required());
  REQUIRE_EQ(second.coordinator.pending_revalidation(), 2u);
  const auto explanation = second.coordinator.explain(4);
  REQUIRE_OK(explanation);
  REQUIRE(explanation.value().render().find("RevalidationRequired") != std::string::npos);
  REQUIRE(explanation.value().render().find("Suspect") == std::string::npos);

  const auto recovered = second.coordinator.status();
  REQUIRE_OK(recovered);
  REQUIRE_EQ(recovered.value().active_producers, 0u);
  REQUIRE_EQ(recovered.value().active_consumers, 0u);
  REQUIRE_EQ(recovered.value().progress.edges_completed, std::uint64_t{1});
  REQUIRE_EQ(recovered.value().progress.partitions_committed, std::uint64_t{1});

  // The attempt that was in flight before the restart carries no authority now.
  REQUIRE_ERROR(second.coordinator.commit_transfer(make_commit(stale_grant, manifest)), ErrorCode::StaleAttempt);
  // An unrelated attempt identifier is refused for the same reason.
  CommitRequest forged = make_commit(stale_grant, manifest);
  forged.attempt = TransferAttemptId{9999};
  REQUIRE_ERROR(second.coordinator.commit_transfer(forged), ErrorCode::StaleAttempt);

  // Re-registration with fresh incarnations clears the revalidation requirement.
  second.add_producer(1, 2);
  second.add_consumer(1, 2, PartitionSelection::all());
  REQUIRE_EQ(second.coordinator.pending_revalidation(), 0u);
  REQUIRE_FALSE(second.coordinator.revalidation_required());

  // Every required edge is either committed or explicitly incomplete: nothing
  // is silently lost, and the accounting closes.
  const auto recovered_progress = second.coordinator.progress();
  REQUIRE_OK(recovered_progress);
  REQUIRE(recovered_progress.value().accounting_closes());
  REQUIRE_EQ(recovered_progress.value().edges_completed, std::uint64_t{1});
  REQUIRE_EQ(recovered_progress.value().edges_incomplete, recovered_progress.value().edges_required - 1);
}

SHUFFLE_TEST(coordinator, snapshot_payload_rejects_corruption) {
  Harness harness(4);
  harness.open();
  harness.add_producer(1, 1);
  const auto snapshot = harness.coordinator.snapshot_payload();
  REQUIRE_OK(snapshot);

  std::vector<std::byte> corrupt = snapshot.value();
  corrupt[0] = std::byte{9};  // unsupported payload version
  Harness other(4);
  REQUIRE_ERROR(other.coordinator.recover(corrupt, {}, false), ErrorCode::UnsupportedVersion);

  std::vector<std::byte> truncated = snapshot.value();
  truncated.resize(truncated.size() - 1);
  Harness third(4);
  REQUIRE_FALSE(third.coordinator.recover(truncated, {}, false).ok());
  REQUIRE_EQ(third.coordinator.state(), ShuffleState::Closed);

  std::vector<std::byte> extended = snapshot.value();
  extended.push_back(std::byte{0x11});
  Harness fourth(4);
  REQUIRE_ERROR(fourth.coordinator.recover(extended, {}, false), ErrorCode::TrailingGarbage);
}

}  // namespace
