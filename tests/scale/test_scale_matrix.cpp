// Scale proofs: the control plane must not grow with the size of the edge
// matrix beyond the irreducible edge set, and planning work must be bounded per
// call rather than proportional to the fabric.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// The assertions are deliberately deterministic (counted cursor steps and
// closed-form edge counts), never wall-clock measurements, so they cannot pass
// or fail by luck on a loaded machine.

#include <cstdint>
#include <string>
#include <vector>

#include "fabric_fixture.hpp"
#include "rng.hpp"
#include "test_support.hpp"

namespace {

using namespace shuffle::fabric;
using shuffle::test::FabricFixture;
using shuffle::test::produce_all;

struct Matrix {
  FabricFixture fixture;
  WaveScheduler scheduler;

  Matrix(std::uint32_t partitions, std::uint32_t producers, std::uint32_t consumers)
      : fixture(partitions), scheduler(fixture.limits()) {
    for (std::uint32_t index = 1; index <= producers; ++index) {
      REQUIRE_OK(fixture.topology().register_producer(ProducerId{index}, IncarnationId{1}, "p"));
    }
    for (std::uint32_t index = 1; index <= consumers; ++index) {
      REQUIRE_OK(fixture.topology().register_consumer(ConsumerId{index}, IncarnationId{1}, "c",
                                                      PartitionSelection::all()));
    }
    produce_all(fixture, 64);
    REQUIRE_OK(scheduler.configure(fixture.policy(), fixture.topology()));
  }
};

// Runs the same amount of dispatch work against two fabrics whose edge spaces
// differ by a factor of 64 and returns the total cursor steps taken.
[[nodiscard]] std::uint64_t cursor_steps_for(std::uint32_t partitions, std::uint32_t waves, std::uint32_t budget) {
  Matrix matrix(partitions, 4, 8);
  matrix.fixture.policy().waves = WaveLimits{4, budget};
  matrix.fixture.policy().concurrency = ConcurrencyLimits{4, 4, 4};
  REQUIRE_OK(matrix.scheduler.configure(matrix.fixture.policy(), matrix.fixture.topology()));

  std::uint64_t steps = 0;
  TickId now{1};
  for (std::uint32_t wave = 0; wave < waves; ++wave) {
    const auto plan = matrix.scheduler.next_wave(matrix.fixture.environment(now));
    REQUIRE_OK(plan);
    steps += plan.value().examined;
    REQUIRE(plan.value().examined <= budget);  // every call is bounded by policy
    for (const DispatchGrant& grant : plan.value().grants) {
      REQUIRE_OK(matrix.scheduler.resolve(grant.attempt, EdgeOutcome::Completed, ErrorCode::Ok));
    }
    now = now.next();
  }
  return steps;
}

SHUFFLE_TEST(scale, planned_edge_count_is_closed_form_for_large_matrices) {
  // 65536 partitions x 256 consumers all-selecting is 16.7M required edges.
  // The count is computed from interned patterns, so no edge matrix exists.
  FabricFixture fixture(65536);
  REQUIRE_OK(fixture.topology().register_producer(ProducerId{1}, IncarnationId{1}, "p"));
  for (std::uint32_t index = 1; index <= 256; ++index) {
    REQUIRE_OK(fixture.topology().register_consumer(ConsumerId{index}, IncarnationId{1}, "c",
                                                    PartitionSelection::all()));
  }
  const auto planned = fixture.topology().planned_edge_count();
  REQUIRE_OK(planned);
  REQUIRE_EQ(planned.value(), std::uint64_t{65536} * 256);
  REQUIRE_EQ(fixture.topology().pattern_count(), 1u);

  // Coverage queries stay exact and cheap for sampled partitions.
  REQUIRE_EQ(fixture.topology().covering_consumer_count(PartitionId{0}), std::size_t{256});
  REQUIRE_EQ(fixture.topology().covering_consumer_count(PartitionId{65535}), std::size_t{256});
  const auto fan_out = fixture.topology().fan_out(PartitionId{12345});
  REQUIRE_OK(fan_out);
  REQUIRE_EQ(fan_out.value(), 256u);
  REQUIRE_OK(fixture.topology().validate());
}

SHUFFLE_TEST(scale, cursor_work_does_not_grow_with_the_edge_space) {
  constexpr std::uint32_t kWaves = 24;
  constexpr std::uint32_t kBudget = 64;
  const std::uint64_t small = cursor_steps_for(1024, kWaves, kBudget);
  const std::uint64_t large = cursor_steps_for(65536, kWaves, kBudget);

  // The same dispatch work costs the same number of cursor steps: the control
  // plane is proportional to the work granted, not to the fabric size.
  REQUIRE_EQ(small, large);
  REQUIRE(small <= static_cast<std::uint64_t>(kWaves) * kBudget);
}

SHUFFLE_TEST(scale, progress_is_exact_without_materialising_edges) {
  FabricFixture fixture(8192);
  REQUIRE_OK(fixture.topology().register_producer(ProducerId{1}, IncarnationId{1}, "p"));
  for (std::uint32_t index = 1; index <= 32; ++index) {
    REQUIRE_OK(fixture.topology().register_consumer(ConsumerId{index}, IncarnationId{1}, "c",
                                                    PartitionSelection::range(PartitionId{0}, PartitionId{4096})));
  }
  const auto required = fixture.topology().planned_edge_count();
  REQUIRE_OK(required);
  REQUIRE_EQ(required.value(), std::uint64_t{32} * 4096);

  // Three committed edges against a required set of 131072: the accounting
  // closes exactly, which is what proves no edge was invented or dropped.
  produce_all(fixture, 32);
  for (std::uint32_t consumer = 1; consumer <= 3; ++consumer) {
    REQUIRE_OK(fixture.commit(PartitionId{0}, PartitionGeneration{1}, ConsumerId{consumer}, TickId{1}));
  }
  const ProgressSnapshot progress = fixture.ledger().progress(required.value(), TickId{1});
  REQUIRE(progress.accounting_closes());
  REQUIRE_EQ(progress.edges_completed, std::uint64_t{3});
  REQUIRE_EQ(progress.edges_incomplete, required.value() - 3);
  REQUIRE_EQ(progress.bytes_committed, std::uint64_t{32});
}

SHUFFLE_TEST(scale, many_distinct_selections_stay_bounded_and_exact) {
  // 4096 consumers with distinct four-partition lists: 4096 interned patterns
  // and 16384 required edges, each answered exactly.
  Limits limits{};
  FabricFixture fixture(4096);
  REQUIRE_OK(fixture.topology().register_producer(ProducerId{1}, IncarnationId{1}, "p"));
  for (std::uint32_t index = 0; index < 4096; ++index) {
    // Four distinct partitions per consumer, shifted so that every consumer
    // interns its own pattern.
    std::vector<PartitionId> selection{PartitionId{index % 4096u}, PartitionId{(index + 1u) % 4096u},
                                       PartitionId{(index + 1024u) % 4096u}, PartitionId{(index + 2048u) % 4096u}};
    const auto listed = PartitionSelection::from_list(selection, limits);
    REQUIRE_OK(listed);
    REQUIRE_OK(fixture.topology().register_consumer(ConsumerId{index + 1}, IncarnationId{1}, "c", listed.value()));
  }
  REQUIRE_EQ(fixture.topology().pattern_count(), 4096u);
  const auto required = fixture.topology().planned_edge_count();
  REQUIRE_OK(required);
  REQUIRE_EQ(required.value(), std::uint64_t{4096} * 4);

  for (std::uint32_t index = 0; index < 4096; index += 997) {
    REQUIRE_EQ(fixture.topology().covering_consumer_count(PartitionId{index}), std::size_t{4});
  }
  REQUIRE_OK(fixture.topology().validate());
}

SHUFFLE_TEST(scale, registration_cost_stays_bounded_for_ascending_identifiers) {
  // Ascending registration is the supported bulk path: it appends, and the
  // interned pattern budget refuses runaway distinct selections instead of
  // growing without bound.
  Limits limits{};
  limits.max_selection_patterns = 64;
  FabricFixture fixture(1024, limits);
  REQUIRE_OK(fixture.topology().register_producer(ProducerId{1}, IncarnationId{1}, "p"));
  std::uint32_t admitted = 0;
  Status first_refusal{};
  for (std::uint32_t index = 0; index < 256; ++index) {
    std::vector<PartitionId> selection{PartitionId{index}, PartitionId{index + 1}};
    const auto listed = PartitionSelection::from_list(selection, limits);
    REQUIRE_OK(listed);
    const Status registered =
        fixture.topology().register_consumer(ConsumerId{index + 1}, IncarnationId{1}, "c", listed.value());
    if (registered.ok()) {
      ++admitted;
      first_refusal = registered;
    } else if (first_refusal.ok()) {
      first_refusal = registered;
    }
  }
  REQUIRE_EQ(admitted, 64u);
  REQUIRE_ERROR(first_refusal, ErrorCode::TooManyPatterns);
  REQUIRE_EQ(fixture.topology().pattern_count(), 64u);
  REQUIRE_OK(fixture.topology().validate());
}

}  // namespace
