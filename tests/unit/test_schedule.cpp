// Deterministic unit tests for wave planning: bounded concurrency, fan bounds,
// pressure deferral, retry eligibility and cancellation.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include "fabric_fixture.hpp"
#include "test_support.hpp"

namespace {

using namespace shuffle::fabric;
using shuffle::test::FabricFixture;
using shuffle::test::produce_all;
using shuffle::test::render_wave_plan;

[[nodiscard]] std::uint64_t key_value(const EdgeKey& key) {
  return (key.partition.value() * 1000003ull) ^ (key.partition_generation.value() * 10007ull) ^ key.consumer.value();
}

struct Prepared {
  FabricFixture fixture;
  WaveScheduler scheduler;

  explicit Prepared(std::uint32_t partitions = 8) : fixture(partitions), scheduler(fixture.limits()) {}

  void add_producers(std::uint32_t count) {
    for (std::uint32_t index = 1; index <= count; ++index) {
      REQUIRE_OK(fixture.topology().register_producer(ProducerId{index}, IncarnationId{1},
                                                      "127.0.0.1:" + std::to_string(7000 + index)));
    }
  }

  void add_consumer(ConsumerId id, const PartitionSelection& selection) {
    REQUIRE_OK(fixture.topology().register_consumer(id, IncarnationId{1},
                                                    "127.0.0.1:" + std::to_string(8000 + id.value()), selection));
  }

  void start() { REQUIRE_OK(scheduler.configure(fixture.policy(), fixture.topology())); }
};

SHUFFLE_TEST(schedule, identical_inputs_produce_identical_plans) {
  Prepared first(16);
  Prepared second(16);
  for (Prepared* prepared : {&first, &second}) {
    prepared->add_producers(2);
    prepared->add_consumer(ConsumerId{1}, PartitionSelection::all());
    prepared->add_consumer(ConsumerId{2}, PartitionSelection::range(PartitionId{2}, PartitionId{10}));
    const auto listed = PartitionSelection::from_list({PartitionId{0}, PartitionId{5}, PartitionId{15}},
                                                      prepared->fixture.limits());
    REQUIRE_OK(listed);
    prepared->add_consumer(ConsumerId{3}, listed.value());
    produce_all(prepared->fixture);
    prepared->start();
  }

  TickId now{1};
  for (int wave = 0; wave < 4; ++wave) {
    const auto left = first.scheduler.next_wave(first.fixture.environment(now));
    const auto right = second.scheduler.next_wave(second.fixture.environment(now));
    REQUIRE_OK(left);
    REQUIRE_OK(right);
    REQUIRE_EQ(render_wave_plan(left.value()), render_wave_plan(right.value()));
    REQUIRE_FALSE(left.value().grants.empty());
    for (const DispatchGrant& grant : left.value().grants) {
      REQUIRE_OK(first.scheduler.resolve(grant.attempt, EdgeOutcome::Completed, ErrorCode::Ok));
    }
    for (const DispatchGrant& grant : right.value().grants) {
      REQUIRE_OK(second.scheduler.resolve(grant.attempt, EdgeOutcome::Completed, ErrorCode::Ok));
    }
    now = now.next();
  }
}

SHUFFLE_TEST(schedule, global_concurrency_bound_is_enforced) {
  Prepared prepared(32);
  prepared.add_producers(4);
  for (std::uint32_t index = 1; index <= 4; ++index) {
    prepared.add_consumer(ConsumerId{index}, PartitionSelection::all());
  }
  prepared.fixture.policy().concurrency = ConcurrencyLimits{3, 3, 3};
  prepared.fixture.policy().waves = WaveLimits{64, 512};
  produce_all(prepared.fixture);
  prepared.start();

  const auto plan = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{1}));
  REQUIRE_OK(plan);
  REQUIRE_EQ(plan.value().grants.size(), std::size_t{3});
  REQUIRE_EQ(prepared.scheduler.in_flight(), 3u);
  REQUIRE(plan.value().deferred_limits > 0);
  REQUIRE(plan.value().grants.size() <= prepared.fixture.policy().concurrency.global);
}

SHUFFLE_TEST(schedule, per_source_and_per_destination_bounds_are_enforced) {
  Prepared prepared(64);
  prepared.add_producers(2);
  prepared.add_consumer(ConsumerId{1}, PartitionSelection::all());
  prepared.add_consumer(ConsumerId{2}, PartitionSelection::all());
  prepared.fixture.policy().concurrency = ConcurrencyLimits{64, 2, 1};
  prepared.fixture.policy().waves = WaveLimits{64, 4096};
  produce_all(prepared.fixture);
  prepared.start();

  const auto plan = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{1}));
  REQUIRE_OK(plan);
  REQUIRE_FALSE(plan.value().grants.empty());

  std::unordered_map<std::uint64_t, std::uint32_t> by_source;
  std::unordered_map<std::uint64_t, std::uint32_t> by_destination;
  for (const DispatchGrant& grant : plan.value().grants) {
    ++by_source[grant.producer.value()];
    ++by_destination[grant.consumer.value()];
  }
  for (const auto& entry : by_source) {
    REQUIRE(entry.second <= 2u);
  }
  for (const auto& entry : by_destination) {
    REQUIRE(entry.second <= 1u);
  }
}

SHUFFLE_TEST(schedule, every_required_edge_is_granted_exactly_once_until_resolved) {
  Prepared prepared(12);
  prepared.add_producers(3);
  prepared.add_consumer(ConsumerId{1}, PartitionSelection::all());
  prepared.add_consumer(ConsumerId{2}, PartitionSelection::range(PartitionId{1}, PartitionId{7}));
  const auto listed = PartitionSelection::from_list({PartitionId{0}, PartitionId{4}, PartitionId{11}},
                                                    prepared.fixture.limits());
  REQUIRE_OK(listed);
  prepared.add_consumer(ConsumerId{3}, listed.value());
  produce_all(prepared.fixture, 128);
  prepared.fixture.policy().waves = WaveLimits{4, 64};
  prepared.start();

  const auto required = prepared.fixture.topology().planned_edge_count();
  REQUIRE_OK(required);

  std::unordered_set<std::uint64_t> granted_edges;
  TickId now{1};
  bool finished = false;
  for (int iteration = 0; iteration < 500 && !finished; ++iteration) {
    const auto plan = prepared.scheduler.next_wave(prepared.fixture.environment(now));
    REQUIRE_OK(plan);
    if (plan.value().all_resolved) {
      finished = true;
      break;
    }
    if (plan.value().grants.empty()) {
      FAIL_TEST("plan produced no grants and did not report completion");
    }
    for (const DispatchGrant& grant : plan.value().grants) {
      const EdgeKey key{grant.partition, grant.partition_generation, grant.consumer};
      REQUIRE(granted_edges.insert(key_value(key)).second);  // never dispatched twice
      const auto receipt = prepared.fixture.commit(grant.partition, grant.partition_generation, grant.consumer, now);
      REQUIRE_OK(receipt);
      REQUIRE_OK(prepared.scheduler.resolve(grant.attempt, EdgeOutcome::Completed, ErrorCode::Ok));
    }
    now = now.next();
  }
  REQUIRE(finished);

  const auto progress = prepared.fixture.ledger().progress(required.value(), now);
  REQUIRE_EQ(progress.edges_completed, required.value());
  REQUIRE_EQ(progress.edges_failed, std::uint64_t{0});
  REQUIRE_EQ(progress.edges_incomplete, std::uint64_t{0});
  REQUIRE(progress.accounting_closes());
  REQUIRE_EQ(prepared.scheduler.in_flight(), 0u);
  REQUIRE_EQ(granted_edges.size(), static_cast<std::size_t>(required.value()));
}

SHUFFLE_TEST(schedule, resolve_refuses_attempts_it_never_issued) {
  Prepared prepared(4);
  prepared.add_producers(1);
  prepared.add_consumer(ConsumerId{1}, PartitionSelection::all());
  produce_all(prepared.fixture);
  prepared.start();

  REQUIRE_ERROR(prepared.scheduler.resolve(TransferAttemptId{99}, EdgeOutcome::Completed, ErrorCode::Ok),
                ErrorCode::StaleAttempt);

  const auto plan = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{1}));
  REQUIRE_OK(plan);
  REQUIRE_FALSE(plan.value().grants.empty());
  const TransferAttemptId issued = plan.value().grants.front().attempt;
  REQUIRE_OK(prepared.scheduler.resolve(issued, EdgeOutcome::Completed, ErrorCode::Ok));
  // Resolving twice is a stale-attempt refusal, not a silent counter drift.
  REQUIRE_ERROR(prepared.scheduler.resolve(issued, EdgeOutcome::Completed, ErrorCode::Ok), ErrorCode::StaleAttempt);
  REQUIRE_EQ(prepared.scheduler.in_flight(), static_cast<std::uint32_t>(plan.value().grants.size() - 1));
}

SHUFFLE_TEST(schedule, backpressure_defers_dispatch_until_released) {
  Prepared prepared(8);
  prepared.add_producers(1);
  for (std::uint32_t index = 1; index <= 3; ++index) {
    prepared.add_consumer(ConsumerId{index}, PartitionSelection::all());
  }
  prepared.fixture.policy().congestion = CongestionPolicy{80, 40, false, 20};
  produce_all(prepared.fixture);
  prepared.start();

  for (std::uint32_t index = 1; index <= 3; ++index) {
    prepared.fixture.set_consumer_pressure(ConsumerId{index}, 95, true, true);
  }

  const auto paused = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{1}));
  REQUIRE_OK(paused);
  REQUIRE(paused.value().grants.empty());
  REQUIRE(paused.value().deferred_destination_pressure > 0);
  REQUIRE_EQ(paused.value().deferred_unknown_pressure, 0u);

  // In the hysteresis band the previous decision stands, so every target is
  // still paused: a reading between resume and pause thresholds changes nothing.
  prepared.fixture.set_consumer_pressure(ConsumerId{1}, 60, true, true);
  const auto hysteresis = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{2}));
  REQUIRE_OK(hysteresis);
  REQUIRE(hysteresis.value().grants.empty());
  REQUIRE(hysteresis.value().deferred_destination_pressure > 0);

  // Below the resume threshold the target is released.
  prepared.fixture.set_consumer_pressure(ConsumerId{1}, 10, true, true);
  const auto released = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{3}));
  REQUIRE_OK(released);
  bool saw_first = false;
  for (const DispatchGrant& grant : released.value().grants) {
    if (grant.consumer == ConsumerId{1}) {
      saw_first = true;
    }
  }
  REQUIRE(saw_first);
}

SHUFFLE_TEST(schedule, absent_evidence_is_not_treated_as_no_congestion) {
  Prepared prepared(8);
  prepared.add_producers(1);
  prepared.add_consumer(ConsumerId{1}, PartitionSelection::all());
  prepared.fixture.policy().congestion = CongestionPolicy{80, 40, true, 5};
  produce_all(prepared.fixture);
  prepared.start();

  const auto unknown = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{1}));
  REQUIRE_OK(unknown);
  REQUIRE(unknown.value().grants.empty());
  REQUIRE(unknown.value().deferred_unknown_pressure > 0);

  // A stale reading is refused as evidence rather than believed: both the
  // source and the destination need current evidence under this policy.
  prepared.fixture.set_consumer_pressure(ConsumerId{1}, 0, true, false);
  prepared.fixture.set_producer_pressure(ProducerId{1}, 0, true, false);
  const auto stale = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{2}));
  REQUIRE_OK(stale);
  REQUIRE(stale.value().grants.empty());
  REQUIRE(stale.value().deferred_unknown_pressure > 0);

  // Fresh evidence with no pressure releases the target.
  prepared.fixture.set_consumer_pressure(ConsumerId{1}, 0, true, true);
  prepared.fixture.set_producer_pressure(ProducerId{1}, 0, true, true);
  const auto fresh = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{3}));
  REQUIRE_OK(fresh);
  REQUIRE_FALSE(fresh.value().grants.empty());
}

SHUFFLE_TEST(schedule, retry_readiness_and_budget_are_respected) {
  Prepared prepared(4);
  prepared.add_producers(1);
  prepared.add_consumer(ConsumerId{1}, PartitionSelection::all());
  prepared.fixture.policy().retry = RetryPolicy{2, 0};
  produce_all(prepared.fixture);
  prepared.start();

  const auto plan = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{10}));
  REQUIRE_OK(plan);
  const DispatchGrant first = plan.value().grants.front();
  const EdgeKey key{first.partition, first.partition_generation, first.consumer};
  REQUIRE_OK(prepared.scheduler.resolve(first.attempt, EdgeOutcome::RetriableFailure, ErrorCode::ConnectionFailure));
  REQUIRE_OK(prepared.fixture.ledger().record_failure(key, ErrorCode::ConnectionFailure, false, 1, TickId{10},
                                                      TickId{25}));

  const auto waiting = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{11}));
  REQUIRE_OK(waiting);
  REQUIRE(waiting.value().deferred_retry_wait > 0);

  const auto eligible = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{25}));
  REQUIRE_OK(eligible);
  bool regranted = false;
  for (const DispatchGrant& grant : eligible.value().grants) {
    if (grant.partition == key.partition && grant.consumer == key.consumer) {
      regranted = true;
      REQUIRE_EQ(grant.attempt_ordinal, 2u);
    }
  }
  REQUIRE(regranted);

  // Exhausting the attempt budget stops dispatch for that edge.
  REQUIRE_OK(prepared.fixture.ledger().record_failure(key, ErrorCode::ConnectionFailure, false, 2, TickId{25},
                                                      TickId{25}));
  const auto exhausted = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{30}));
  REQUIRE_OK(exhausted);
  for (const DispatchGrant& grant : exhausted.value().grants) {
    REQUIRE_FALSE(grant.partition == key.partition && grant.consumer == key.consumer);
  }
}

SHUFFLE_TEST(schedule, unproduced_and_stale_owned_partitions_are_skipped_explicitly) {
  Prepared prepared(6);
  prepared.add_producers(2);
  prepared.add_consumer(ConsumerId{1}, PartitionSelection::all());
  produce_all(prepared.fixture);
  // Partition 3 was never produced for this incarnation.
  prepared.fixture.withdraw_partition(PartitionId{3});
  // Partition 4 claims a producer that does not own it.
  prepared.fixture.produce(PartitionId{4}, PartitionGeneration{1}, ProducerId{9}, IncarnationId{1}, 64);
  prepared.fixture.policy().waves = WaveLimits{16, 256};
  prepared.start();

  const auto plan = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{1}));
  REQUIRE_OK(plan);
  REQUIRE(plan.value().skipped_unproduced >= 2u);
  for (const DispatchGrant& grant : plan.value().grants) {
    REQUIRE_NE(grant.partition, PartitionId{3});
    REQUIRE_NE(grant.partition, PartitionId{4});
  }
  REQUIRE_EQ(prepared.fixture.ledger().edge_status(EdgeKey{PartitionId{3}, PartitionGeneration{1}, ConsumerId{1}}).phase,
             EdgePhase::Pending);
}

SHUFFLE_TEST(schedule, work_per_call_is_bounded_regardless_of_edge_space) {
  Prepared prepared(4096);
  prepared.add_producers(4);
  for (std::uint32_t index = 1; index <= 64; ++index) {
    prepared.add_consumer(ConsumerId{index}, PartitionSelection::all());
  }
  prepared.fixture.policy().waves = WaveLimits{8, 100};
  prepared.fixture.policy().concurrency = ConcurrencyLimits{1024, 1024, 1024};
  produce_all(prepared.fixture, 64);
  prepared.start();

  const auto required = prepared.fixture.topology().planned_edge_count();
  REQUIRE_OK(required);
  REQUIRE_EQ(required.value(), std::uint64_t{4096} * 64);

  // One call examines at most the configured budget even though the irreducible
  // edge set is more than a quarter of a million edges.
  const auto plan = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{1}));
  REQUIRE_OK(plan);
  REQUIRE(plan.value().examined <= 100u);
  REQUIRE(plan.value().grants.size() <= 8u);
}

SHUFFLE_TEST(schedule, topology_change_requires_reconfiguration) {
  Prepared prepared(8);
  prepared.add_producers(1);
  prepared.add_consumer(ConsumerId{1}, PartitionSelection::all());
  produce_all(prepared.fixture);
  prepared.start();

  REQUIRE_OK(prepared.fixture.topology().register_producer(ProducerId{2}, IncarnationId{1}, "127.0.0.1:7002"));
  REQUIRE_ERROR(prepared.scheduler.next_wave(prepared.fixture.environment(TickId{1})), ErrorCode::StaleTopology);

  prepared.start();  // reconfigure under the new topology generation
  const auto plan = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{1}));
  REQUIRE_OK(plan);
  REQUIRE_FALSE(plan.value().grants.empty());
}

SHUFFLE_TEST(schedule, cancellation_abandons_in_flight_authority) {
  Prepared prepared(8);
  prepared.add_producers(1);
  prepared.add_consumer(ConsumerId{1}, PartitionSelection::all());
  produce_all(prepared.fixture);
  prepared.start();

  const auto plan = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{1}));
  REQUIRE_OK(plan);
  REQUIRE_FALSE(plan.value().grants.empty());
  const TransferAttemptId abandoned = plan.value().grants.front().attempt;

  REQUIRE_OK(prepared.scheduler.abandon_all(ErrorCode::Cancelled));
  REQUIRE_EQ(prepared.scheduler.in_flight(), 0u);
  REQUIRE_ERROR(prepared.scheduler.resolve(abandoned, EdgeOutcome::Completed, ErrorCode::Ok), ErrorCode::StaleAttempt);

  // The work is not lost: the edges are re-granted under fresh attempts.
  const auto replanned = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{2}));
  REQUIRE_OK(replanned);
  REQUIRE_FALSE(replanned.value().grants.empty());
  REQUIRE_NE(replanned.value().grants.front().attempt, abandoned);
  REQUIRE_EQ(prepared.scheduler.stats().grants_abandoned, std::uint64_t{plan.value().grants.size()});
}

SHUFFLE_TEST(schedule, completed_edges_are_never_regranted) {
  Prepared prepared(4);
  prepared.add_producers(1);
  prepared.add_consumer(ConsumerId{1}, PartitionSelection::range(PartitionId{0}, PartitionId{2}));
  produce_all(prepared.fixture);
  prepared.start();

  const auto first = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{1}));
  REQUIRE_OK(first);
  REQUIRE_EQ(first.value().grants.size(), std::size_t{2});
  for (const DispatchGrant& grant : first.value().grants) {
    REQUIRE_OK(prepared.fixture.commit(grant.partition, grant.partition_generation, grant.consumer, TickId{1}));
    REQUIRE_OK(prepared.scheduler.resolve(grant.attempt, EdgeOutcome::Completed, ErrorCode::Ok));
  }

  const auto second = prepared.scheduler.next_wave(prepared.fixture.environment(TickId{2}));
  REQUIRE_OK(second);
  REQUIRE(second.value().grants.empty());
  REQUIRE(second.value().skipped_completed >= 2u);
  REQUIRE(second.value().all_resolved);
}

}  // namespace
