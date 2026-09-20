// Deterministic unit tests for the completion ledger: exactly-once accounting,
// duplicate suppression, divergence refusal and explicit failure records.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include <string>
#include <vector>

#include "fabric_fixture.hpp"
#include "test_support.hpp"

namespace {

using namespace shuffle::fabric;
using shuffle::test::FabricFixture;
using shuffle::test::produce_all;

[[nodiscard]] CommitRequest make_request(const Digest& digest, ConsumerId consumer, std::uint64_t bytes,
                                         PartitionGeneration generation = PartitionGeneration{1},
                                         PartitionId partition = PartitionId{0}) {
  CommitRequest request;
  request.edge = EdgeKey{partition, generation, consumer};
  request.attempt = TransferAttemptId{1};
  request.wave = WaveId{1};
  request.shuffle_generation = ShuffleGeneration{1};
  request.topology_generation = TopologyGeneration{1};
  request.policy_generation = PolicyGeneration{1};
  request.producer = ProducerId{1};
  request.producer_incarnation = IncarnationId{7};
  request.consumer = consumer;
  request.consumer_incarnation = IncarnationId{9};
  request.manifest_digest = digest;
  request.observed_partition_digest = digest;
  request.bytes = bytes;
  request.integrity_verified = true;
  return request;
}

[[nodiscard]] Digest sample_digest(std::uint64_t salt) {
  ByteWriter writer;
  writer.put_u64(salt);
  return sha256(writer.data());
}

SHUFFLE_TEST(ledger, commit_accounts_bytes_exactly_once) {
  CompletionLedger ledger{Limits{}};
  ledger.bind(ShuffleId{1}, ShuffleGeneration{1}, 4);
  const Digest digest = sample_digest(1);

  const auto first = ledger.commit(make_request(digest, ConsumerId{1}, 1024), TickId{1});
  REQUIRE_OK(first);
  REQUIRE(first.value().newly_committed);
  REQUIRE(first.value().edge_newly_completed);
  REQUIRE_FALSE(first.value().duplicate);
  REQUIRE_EQ(first.value().accounted_bytes, std::uint64_t{1024});
  REQUIRE_EQ(first.value().sequence.value(), 1u);

  const auto progress = ledger.progress(4, TickId{1});
  REQUIRE_EQ(progress.bytes_committed, std::uint64_t{1024});
  REQUIRE_EQ(progress.edges_completed, std::uint64_t{1});
  REQUIRE_EQ(progress.partitions_committed, std::uint64_t{1});
}

SHUFFLE_TEST(ledger, duplicate_completion_never_double_accounts) {
  CompletionLedger ledger{Limits{}};
  ledger.bind(ShuffleId{1}, ShuffleGeneration{1}, 4);
  const Digest digest = sample_digest(2);
  REQUIRE_OK(ledger.commit(make_request(digest, ConsumerId{1}, 512), TickId{1}));

  for (int repeat = 0; repeat < 5; ++repeat) {
    const auto duplicate = ledger.commit(make_request(digest, ConsumerId{1}, 512), TickId{2});
    REQUIRE_OK(duplicate);
    REQUIRE(duplicate.value().duplicate);
    REQUIRE_FALSE(duplicate.value().newly_committed);
    REQUIRE_FALSE(duplicate.value().edge_newly_completed);
    REQUIRE_EQ(duplicate.value().accounted_bytes, std::uint64_t{0});
  }

  const auto progress = ledger.progress(4, TickId{2});
  REQUIRE_EQ(progress.bytes_committed, std::uint64_t{512});
  REQUIRE_EQ(progress.edges_completed, std::uint64_t{1});
  REQUIRE_EQ(progress.duplicate_commits_suppressed, std::uint64_t{5});
}

SHUFFLE_TEST(ledger, additional_consumer_completes_edge_without_reaccounting_bytes) {
  CompletionLedger ledger{Limits{}};
  ledger.bind(ShuffleId{1}, ShuffleGeneration{1}, 4);
  const Digest digest = sample_digest(3);
  REQUIRE_OK(ledger.commit(make_request(digest, ConsumerId{5}, 2048), TickId{1}));

  const auto second = ledger.commit(make_request(digest, ConsumerId{2}, 2048), TickId{2});
  REQUIRE_OK(second);
  REQUIRE_FALSE(second.value().newly_committed);
  REQUIRE(second.value().edge_newly_completed);
  REQUIRE_FALSE(second.value().duplicate);

  const auto progress = ledger.progress(4, TickId{2});
  REQUIRE_EQ(progress.bytes_committed, std::uint64_t{2048});
  REQUIRE_EQ(progress.edges_completed, std::uint64_t{2});
  REQUIRE_EQ(progress.partitions_committed, std::uint64_t{1});

  const EdgeStatus first_status = ledger.edge_status(EdgeKey{PartitionId{0}, PartitionGeneration{1}, ConsumerId{5}});
  REQUIRE_EQ(first_status.phase, EdgePhase::Completed);
  REQUIRE(first_status.authoritative);
  REQUIRE_EQ(first_status.bytes, std::uint64_t{2048});
}

SHUFFLE_TEST(ledger, divergent_commit_is_refused_never_merged) {
  CompletionLedger ledger{Limits{}};
  ledger.bind(ShuffleId{1}, ShuffleGeneration{1}, 4);
  REQUIRE_OK(ledger.commit(make_request(sample_digest(4), ConsumerId{1}, 100), TickId{1}));

  CommitRequest divergent = make_request(sample_digest(99), ConsumerId{2}, 100);
  REQUIRE_ERROR(ledger.commit(divergent, TickId{2}), ErrorCode::DivergentCommit);

  CommitRequest other_incarnation = make_request(sample_digest(4), ConsumerId{2}, 100);
  other_incarnation.producer_incarnation = IncarnationId{8};
  REQUIRE_ERROR(ledger.commit(other_incarnation, TickId{2}), ErrorCode::DivergentCommit);

  CommitRequest contradictory_bytes = make_request(sample_digest(4), ConsumerId{2}, 101);
  REQUIRE_ERROR(ledger.commit(contradictory_bytes, TickId{2}), ErrorCode::AccountingMismatch);

  const auto progress = ledger.progress(4, TickId{2});
  REQUIRE_EQ(progress.bytes_committed, std::uint64_t{100});
  REQUIRE_EQ(progress.edges_completed, std::uint64_t{1});
}

SHUFFLE_TEST(ledger, completion_requires_verified_integrity) {
  CompletionLedger ledger{Limits{}};
  ledger.bind(ShuffleId{1}, ShuffleGeneration{1}, 4);

  CommitRequest unverified = make_request(sample_digest(5), ConsumerId{1}, 64);
  unverified.integrity_verified = false;
  REQUIRE_ERROR(ledger.commit(unverified, TickId{1}), ErrorCode::IntegrityFailure);

  CommitRequest unsigned_claim = make_request(Digest{}, ConsumerId{1}, 64);
  REQUIRE_ERROR(ledger.commit(unsigned_claim, TickId{1}), ErrorCode::ManifestInconsistent);

  CommitRequest zero_bytes = make_request(sample_digest(5), ConsumerId{1}, 0);
  REQUIRE_ERROR(ledger.commit(zero_bytes, TickId{1}), ErrorCode::InvalidArgument);

  CommitRequest no_generation = make_request(sample_digest(5), ConsumerId{1}, 64, PartitionGeneration{0});
  REQUIRE_ERROR(ledger.commit(no_generation, TickId{1}), ErrorCode::InvalidArgument);

  CommitRequest outside = make_request(sample_digest(5), ConsumerId{1}, 64);
  outside.edge.partition = PartitionId{9};
  REQUIRE_ERROR(ledger.commit(outside, TickId{1}), ErrorCode::UnknownPartition);

  REQUIRE_EQ(ledger.progress(4, TickId{1}).bytes_committed, std::uint64_t{0});
}

SHUFFLE_TEST(ledger, stale_generation_cannot_commit_late) {
  CompletionLedger ledger{Limits{}};
  ledger.bind(ShuffleId{1}, ShuffleGeneration{1}, 4);
  const Digest first = sample_digest(6);
  const Digest second = sample_digest(7);

  REQUIRE_OK(ledger.commit(make_request(first, ConsumerId{1}, 100, PartitionGeneration{1}), TickId{1}));
  REQUIRE_OK(ledger.commit(make_request(second, ConsumerId{1}, 200, PartitionGeneration{2}), TickId{2}));

  // A late completion of the superseded generation is refused, not merged.
  REQUIRE_ERROR(ledger.commit(make_request(first, ConsumerId{2}, 100, PartitionGeneration{1}), TickId{3}),
                ErrorCode::StaleGeneration);

  const EdgeStatus superseded = ledger.edge_status(EdgeKey{PartitionId{0}, PartitionGeneration{1}, ConsumerId{2}});
  REQUIRE_EQ(superseded.phase, EdgePhase::Purged);
  REQUIRE_EQ(superseded.reason, ErrorCode::PurgedHistory);

  const EdgeStatus current = ledger.edge_status(EdgeKey{PartitionId{0}, PartitionGeneration{2}, ConsumerId{1}});
  REQUIRE_EQ(current.phase, EdgePhase::Completed);
  REQUIRE_EQ(current.bytes, std::uint64_t{200});

  // Shuffle generations are fenced too.
  CommitRequest wrong_shuffle = make_request(second, ConsumerId{3}, 200, PartitionGeneration{2});
  wrong_shuffle.shuffle_generation = ShuffleGeneration{2};
  REQUIRE_ERROR(ledger.commit(wrong_shuffle, TickId{4}), ErrorCode::StaleGeneration);
}

SHUFFLE_TEST(ledger, failures_are_recorded_and_classified) {
  CompletionLedger ledger{Limits{}};
  ledger.bind(ShuffleId{1}, ShuffleGeneration{1}, 8);
  const EdgeKey transient{PartitionId{1}, PartitionGeneration{1}, ConsumerId{1}};
  const EdgeKey permanent{PartitionId{2}, PartitionGeneration{1}, ConsumerId{1}};

  REQUIRE_OK(ledger.record_failure(transient, ErrorCode::ConnectionFailure, false, 1, TickId{1}, TickId{3}));
  REQUIRE_OK(ledger.record_failure(permanent, ErrorCode::MalformedInput, true, 1, TickId{1}, TickId{1}));

  const EdgeStatus transient_status = ledger.edge_status(transient);
  REQUIRE_EQ(transient_status.phase, EdgePhase::Pending);
  REQUIRE_EQ(transient_status.reason, ErrorCode::ConnectionFailure);
  REQUIRE_EQ(transient_status.attempts, 1u);
  REQUIRE_EQ(transient_status.ready_at.value(), 3u);
  REQUIRE_FALSE(transient_status.resolved());

  const EdgeStatus permanent_status = ledger.edge_status(permanent);
  REQUIRE_EQ(permanent_status.phase, EdgePhase::Failed);
  REQUIRE(permanent_status.resolved());

  const EdgeStatus untouched = ledger.edge_status(EdgeKey{PartitionId{3}, PartitionGeneration{1}, ConsumerId{1}});
  REQUIRE_EQ(untouched.phase, EdgePhase::Pending);
  REQUIRE_EQ(untouched.reason, ErrorCode::NotCommitted);

  REQUIRE_ERROR(ledger.record_failure(transient, ErrorCode::Ok, false, 1, TickId{1}, TickId{1}),
                ErrorCode::InvalidArgument);

  const auto progress = ledger.progress(24, TickId{1});
  REQUIRE_EQ(progress.retriable_failures, std::uint64_t{1});
  REQUIRE_EQ(progress.permanent_failures, std::uint64_t{1});
  REQUIRE_EQ(progress.edges_failed, std::uint64_t{1});
  REQUIRE_EQ(progress.edges_completed, std::uint64_t{0});
  REQUIRE_EQ(progress.edges_incomplete, std::uint64_t{23});
  REQUIRE(progress.accounting_closes());

  const Explanation explanation = ledger.explain(4);
  REQUIRE_EQ(explanation.entries.size(), std::size_t{2});
  REQUIRE(explanation.render().find("ConnectionFailure") != std::string::npos);
}

SHUFFLE_TEST(ledger, accounting_closes_over_mixed_outcomes) {
  FabricFixture fixture(6);
  REQUIRE_OK(fixture.topology().register_producer(ProducerId{1}, IncarnationId{1}, "p1"));
  REQUIRE_OK(fixture.topology().register_consumer(ConsumerId{1}, IncarnationId{1}, "c1", PartitionSelection::all()));
  REQUIRE_OK(fixture.topology().register_consumer(ConsumerId{2}, IncarnationId{1}, "c2", PartitionSelection::all()));
  produce_all(fixture);

  const auto required = fixture.topology().planned_edge_count();
  REQUIRE_OK(required);
  REQUIRE_EQ(required.value(), std::uint64_t{12});

  // Partition 0: both consumers complete. Partition 1: one completes, one
  // fails permanently. Partitions 2..5: untouched.
  REQUIRE_OK(fixture.commit(PartitionId{0}, PartitionGeneration{1}, ConsumerId{1}, TickId{1}));
  REQUIRE_OK(fixture.commit(PartitionId{0}, PartitionGeneration{1}, ConsumerId{2}, TickId{1}));
  REQUIRE_OK(fixture.commit(PartitionId{1}, PartitionGeneration{1}, ConsumerId{1}, TickId{1}));
  REQUIRE_OK(fixture.ledger().record_failure(EdgeKey{PartitionId{1}, PartitionGeneration{1}, ConsumerId{2}},
                                             ErrorCode::DigestMismatch, true, 3, TickId{1}, TickId{1}));

  const auto progress = fixture.ledger().progress(required.value(), TickId{1});
  REQUIRE(progress.accounting_closes());
  REQUIRE_EQ(progress.edges_completed, std::uint64_t{3});
  REQUIRE_EQ(progress.edges_failed, std::uint64_t{1});
  REQUIRE_EQ(progress.edges_incomplete, std::uint64_t{8});
  REQUIRE_EQ(progress.partitions_committed, std::uint64_t{2});
  REQUIRE_EQ(progress.partitions_failed, std::uint64_t{0});  // partition 1 did commit for one consumer

  // A partition with no commit and a permanent failure is counted as failed.
  REQUIRE_OK(fixture.ledger().record_failure(EdgeKey{PartitionId{4}, PartitionGeneration{1}, ConsumerId{1}},
                                             ErrorCode::AuthorityDenied, true, 3, TickId{1}, TickId{1}));
  const auto after = fixture.ledger().progress(required.value(), TickId{1});
  REQUIRE_EQ(after.partitions_failed, std::uint64_t{1});
  REQUIRE_EQ(after.authority_refusals, std::uint64_t{1});
  REQUIRE(after.accounting_closes());
}

SHUFFLE_TEST(ledger, retention_purges_old_generations_explicitly) {
  Limits limits{};
  limits.max_retained_generations = 2;
  CompletionLedger ledger{limits};
  ledger.bind(ShuffleId{1}, ShuffleGeneration{1}, 4);

  for (std::uint64_t generation = 1; generation <= 4; ++generation) {
    REQUIRE_OK(ledger.commit(make_request(sample_digest(generation), ConsumerId{1}, 10 * generation,
                                          PartitionGeneration{generation}),
                             TickId{generation}));
  }

  // Only the two most recent generations remain; the older ones are reported
  // as purged history rather than silently missing.
  REQUIRE_EQ(ledger.tracked_partitions(), std::uint64_t{2});
  REQUIRE_EQ(ledger.edge_status(EdgeKey{PartitionId{0}, PartitionGeneration{1}, ConsumerId{1}}).phase, EdgePhase::Purged);
  REQUIRE_EQ(ledger.edge_status(EdgeKey{PartitionId{0}, PartitionGeneration{3}, ConsumerId{1}}).phase,
             EdgePhase::Completed);
  REQUIRE_EQ(ledger.edge_status(EdgeKey{PartitionId{0}, PartitionGeneration{4}, ConsumerId{1}}).phase,
             EdgePhase::Completed);
}

SHUFFLE_TEST(ledger, tracked_edge_bound_is_enforced) {
  Limits limits{};
  limits.max_tracked_edges = 3;
  CompletionLedger ledger{limits};
  ledger.bind(ShuffleId{1}, ShuffleGeneration{1}, 8);

  for (std::uint64_t consumer = 1; consumer <= 3; ++consumer) {
    REQUIRE_OK(ledger.commit(make_request(sample_digest(1), ConsumerId{consumer}, 16), TickId{1}));
  }
  REQUIRE_ERROR(ledger.commit(make_request(sample_digest(1), ConsumerId{4}, 16), TickId{1}),
                ErrorCode::TooManyTrackedEdges);
  REQUIRE_ERROR(ledger.record_failure(EdgeKey{PartitionId{5}, PartitionGeneration{1}, ConsumerId{1}},
                                      ErrorCode::ConnectionFailure, false, 1, TickId{1}, TickId{1}),
                ErrorCode::TooManyTrackedEdges);
}

SHUFFLE_TEST(ledger, encoding_is_deterministic_and_round_trips) {
  CompletionLedger ledger{Limits{}};
  ledger.bind(ShuffleId{1}, ShuffleGeneration{1}, 8);
  for (std::uint64_t consumer = 1; consumer <= 4; ++consumer) {
    REQUIRE_OK(ledger.commit(
        make_request(sample_digest(consumer), ConsumerId{consumer}, 32, PartitionGeneration{1}, PartitionId{consumer}),
        TickId{2}));
  }
  REQUIRE_OK(ledger.record_failure(EdgeKey{PartitionId{6}, PartitionGeneration{1}, ConsumerId{9}},
                                   ErrorCode::PeerUnavailable, false, 2, TickId{2}, TickId{5}));

  ByteWriter first;
  REQUIRE_OK(ledger.encode(first));
  ByteWriter second;
  REQUIRE_OK(ledger.encode(second));
  REQUIRE(first.data() == second.data());

  ByteReader reader{first.data(), Limits{}, "ledger"};
  CompletionLedger restored{Limits{}};
  REQUIRE_OK(restored.decode(reader, Limits{}));
  REQUIRE_OK(reader.require_end());

  const EdgeStatus before = ledger.edge_status(EdgeKey{PartitionId{0}, PartitionGeneration{1}, ConsumerId{3}});
  const EdgeStatus after = restored.edge_status(EdgeKey{PartitionId{0}, PartitionGeneration{1}, ConsumerId{3}});
  REQUIRE_EQ(before.phase, after.phase);
  REQUIRE_EQ(before.bytes, after.bytes);
  REQUIRE_EQ(restored.progress(8, TickId{2}).bytes_committed, ledger.progress(8, TickId{2}).bytes_committed);
  REQUIRE_EQ(restored.progress(8, TickId{2}).duplicate_commits_suppressed,
             ledger.progress(8, TickId{2}).duplicate_commits_suppressed);

  ByteWriter reencoded;
  REQUIRE_OK(restored.encode(reencoded));
  REQUIRE(reencoded.data() == first.data());
}

SHUFFLE_TEST(ledger, decode_refuses_corruption_without_partial_application) {
  CompletionLedger ledger{Limits{}};
  ledger.bind(ShuffleId{1}, ShuffleGeneration{1}, 8);
  REQUIRE_OK(ledger.commit(make_request(sample_digest(11), ConsumerId{1}, 64), TickId{1}));
  ByteWriter writer;
  REQUIRE_OK(ledger.encode(writer));
  const std::vector<std::byte> encoded = writer.data();

  const std::span<const std::byte> truncated{encoded.data(), encoded.size() - 3};
  ByteReader short_reader{truncated, Limits{}, "truncated ledger"};
  CompletionLedger restored{Limits{}};
  REQUIRE_FALSE(restored.decode(short_reader, Limits{}).ok());
  REQUIRE_EQ(restored.tracked_partitions(), std::uint64_t{0});

  // The ledger's own encoding carries structure, not a checksum: the durable
  // store above it adds integrity. A structurally impossible value is still
  // refused here rather than partially applied.
  std::vector<std::byte> corrupt = encoded;
  for (std::size_t index = 16; index < 20; ++index) {
    corrupt[index] = std::byte{0};  // partition_count := 0 while records exist
  }
  ByteReader corrupt_reader{corrupt, Limits{}, "corrupt ledger"};
  CompletionLedger other{Limits{}};
  REQUIRE_ERROR(other.decode(corrupt_reader, Limits{}), ErrorCode::StateImpossible);
  REQUIRE_EQ(other.tracked_partitions(), std::uint64_t{0});
}

}  // namespace
