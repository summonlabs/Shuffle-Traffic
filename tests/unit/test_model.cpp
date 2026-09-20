// Deterministic unit tests for the model layer: manifests, topology and the
// policy envelope.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "shuffle/fabric/manifest.hpp"
#include "shuffle/fabric/policy.hpp"
#include "shuffle/fabric/topology.hpp"
#include "test_support.hpp"

namespace {

using namespace shuffle::fabric;

constexpr std::uint32_t kPartitions = 12;

struct Fixture {
  Limits limits{};
  ShuffleId shuffle{1};
  ShuffleGeneration shuffle_generation{1};

  [[nodiscard]] Topology make_topology(std::uint32_t partitions = kPartitions) const {
    return Topology{shuffle, shuffle_generation, partitions, limits};
  }
};

[[nodiscard]] PolicyEnvelope make_policy(std::uint32_t partitions = kPartitions) {
  PolicyEnvelope policy;
  policy.shuffle = ShuffleId{1};
  policy.shuffle_generation = ShuffleGeneration{1};
  policy.generation = PolicyGeneration{1};
  policy.fan.max_fan_out = 64;
  policy.fan.max_fan_in = 64;
  policy.concurrency = ConcurrencyLimits{8, 4, 4};
  policy.waves = WaveLimits{16, 256};
  policy.retry = RetryPolicy{3, 0};
  policy.congestion = CongestionPolicy{80, 40, false, 20};
  static_cast<void>(partitions);
  return policy;
}

[[nodiscard]] std::uint64_t brute_force_edges(const Topology& topology) {
  std::uint64_t count = 0;
  for (std::uint32_t partition = 0; partition < topology.partition_count(); ++partition) {
    count += topology.covering_consumers(PartitionId{partition}).size();
  }
  return count;
}

SHUFFLE_TEST(manifest, build_split_and_verify) {
  const Fixture fixture;
  const std::vector<std::byte> payload = synthetic_partition_payload(ShuffleId{1}, ShuffleGeneration{1}, PartitionId{3},
                                                                    PartitionGeneration{1}, 1000);
  REQUIRE_EQ(payload.size(), std::size_t{1000});

  const auto built = build_manifest(ShuffleId{1}, ShuffleGeneration{1}, PartitionId{3}, PartitionGeneration{1},
                                    ProducerId{1}, IncarnationId{5}, TopologyGeneration{2}, payload, 256, fixture.limits);
  REQUIRE_OK(built);
  const PartitionManifest& manifest = built.value();
  REQUIRE_EQ(manifest.chunks.size(), std::size_t{4});
  REQUIRE_EQ(manifest.chunks[3].length, 1000u - (3u * 256u));
  REQUIRE_EQ(manifest.total_bytes, 1000u);
  REQUIRE_OK(validate_manifest(manifest, fixture.limits));
  REQUIRE_OK(verify_partition_content(manifest, payload));

  // Every chunk descriptor must match its slice of the payload.
  for (const ChunkDescriptor& chunk : manifest.chunks) {
    const auto offset = static_cast<std::size_t>(chunk.offset);
    REQUIRE_OK(verify_chunk(chunk, std::span<const std::byte>{payload}.subspan(offset, chunk.length)));
  }

  // Corruption is detected, and so is a length mismatch.
  std::vector<std::byte> corrupted = payload;
  corrupted[10] = static_cast<std::byte>(std::to_integer<std::uint8_t>(corrupted[10]) ^ 0xffu);
  REQUIRE_ERROR(verify_partition_content(manifest, corrupted), ErrorCode::DigestMismatch);
  REQUIRE_ERROR(verify_chunk(manifest.chunks[0], std::span<const std::byte>{payload}.first(10)),
                ErrorCode::IntegrityFailure);
}

SHUFFLE_TEST(manifest, digest_binds_identity_and_content) {
  const Fixture fixture;
  const std::vector<std::byte> payload = synthetic_partition_payload(ShuffleId{1}, ShuffleGeneration{1}, PartitionId{0},
                                                                    PartitionGeneration{1}, 512);
  const auto built = build_manifest(ShuffleId{1}, ShuffleGeneration{1}, PartitionId{0}, PartitionGeneration{1},
                                    ProducerId{1}, IncarnationId{1}, TopologyGeneration{1}, payload, 128, fixture.limits);
  REQUIRE_OK(built);
  const Digest digest = compute_manifest_digest(built.value());
  REQUIRE_FALSE(digest.is_zero());
  REQUIRE_EQ(compute_partition_digest(built.value()), compute_partition_digest(built.value()));

  PartitionManifest other = built.value();
  other.partition = PartitionId{1};
  REQUIRE_NE(compute_manifest_digest(other), digest);
  // The partition digest covers content only, so it is unchanged by identity.
  REQUIRE_EQ(compute_partition_digest(other), compute_partition_digest(built.value()));

  PartitionManifest regenerated = built.value();
  regenerated.partition_generation = PartitionGeneration{2};
  REQUIRE_NE(compute_manifest_digest(regenerated), digest);
}

SHUFFLE_TEST(manifest, encode_decode_round_trip_and_rejection) {
  const Fixture fixture;
  const std::vector<std::byte> payload = synthetic_partition_payload(ShuffleId{1}, ShuffleGeneration{1}, PartitionId{2},
                                                                    PartitionGeneration{1}, 300);
  const auto built = build_manifest(ShuffleId{1}, ShuffleGeneration{1}, PartitionId{2}, PartitionGeneration{1},
                                    ProducerId{9}, IncarnationId{2}, TopologyGeneration{3}, payload, 100, fixture.limits);
  REQUIRE_OK(built);
  const auto encoded = encode_manifest(built.value(), fixture.limits);
  REQUIRE_OK(encoded);

  const auto decoded = decode_manifest(encoded.value(), fixture.limits);
  REQUIRE_OK(decoded);
  REQUIRE(decoded.value() == built.value());
  REQUIRE_EQ(compute_manifest_digest(decoded.value()), compute_manifest_digest(built.value()));

  // Truncation, trailing bytes and absurd collection counts are refused.
  const std::span<const std::byte> truncated{encoded.value().data(), encoded.value().size() - 1};
  REQUIRE_FALSE(decode_manifest(truncated, fixture.limits).ok());

  std::vector<std::byte> extended = encoded.value();
  extended.push_back(std::byte{0x7f});
  REQUIRE_ERROR(decode_manifest(extended, fixture.limits), ErrorCode::TrailingGarbage);

  std::vector<std::byte> absurd = encoded.value();
  absurd[57] = std::byte{0xff};  // chunk count high byte
  REQUIRE_FALSE(decode_manifest(absurd, fixture.limits).ok());

  // A manifest whose chunk coverage does not sum to total_bytes is refused.
  PartitionManifest inconsistent = built.value();
  inconsistent.total_bytes += 1;
  REQUIRE_ERROR(encode_manifest(inconsistent, fixture.limits), ErrorCode::ManifestInconsistent);
  REQUIRE_ERROR(validate_manifest(inconsistent, fixture.limits), ErrorCode::ManifestInconsistent);
}

SHUFFLE_TEST(manifest, synthetic_payload_is_deterministic_and_distinct) {
  const auto first = synthetic_partition_payload(ShuffleId{1}, ShuffleGeneration{1}, PartitionId{4},
                                                 PartitionGeneration{1}, 777);
  const auto again = synthetic_partition_payload(ShuffleId{1}, ShuffleGeneration{1}, PartitionId{4},
                                                 PartitionGeneration{1}, 777);
  const auto other_partition = synthetic_partition_payload(ShuffleId{1}, ShuffleGeneration{1}, PartitionId{5},
                                                           PartitionGeneration{1}, 777);
  const auto other_generation = synthetic_partition_payload(ShuffleId{1}, ShuffleGeneration{1}, PartitionId{4},
                                                           PartitionGeneration{2}, 777);
  REQUIRE(first == again);
  REQUIRE(first != other_partition);
  REQUIRE(first != other_generation);
  REQUIRE_EQ(first.size(), std::size_t{777});
}

SHUFFLE_TEST(topology, ownership_is_deterministic_round_robin) {
  Fixture fixture;
  Topology topology = fixture.make_topology();

  REQUIRE_OK(topology.register_producer(ProducerId{2}, IncarnationId{1}, "p2"));
  REQUIRE_OK(topology.register_producer(ProducerId{1}, IncarnationId{1}, "p1"));
  REQUIRE_OK(topology.register_producer(ProducerId{3}, IncarnationId{1}, "p3"));
  REQUIRE_EQ(topology.active_producer_count(), 3u);

  // Sorted producer order decides ownership, independent of registration order.
  for (std::uint32_t partition = 0; partition < kPartitions; ++partition) {
    const auto owner = topology.owner_of(PartitionId{partition});
    REQUIRE_OK(owner);
    REQUIRE_EQ(owner.value(), ProducerId{1 + (partition % 3)});
  }

  // A fourth producer shifts ownership: partition 1 now belongs to producer 1
  // only if it is the second active producer in ascending order.
  REQUIRE_OK(topology.register_producer(ProducerId{4}, IncarnationId{1}, "p4"));
  const auto shifted = topology.owner_of(PartitionId{3});
  REQUIRE_OK(shifted);
  REQUIRE_EQ(shifted.value(), ProducerId{4});
  // Zero is not a valid identifier: identities are explicit, never implicit.
  REQUIRE_ERROR(topology.register_producer(ProducerId{0}, IncarnationId{1}, "p0"), ErrorCode::InvalidArgument);

  REQUIRE(PartitionId{kPartitions}.value() == kPartitions);
  REQUIRE_ERROR(topology.owner_of(PartitionId{kPartitions}), ErrorCode::UnknownPartition);
}

SHUFFLE_TEST(topology, generations_advance_on_every_mutation) {
  Fixture fixture;
  Topology topology = fixture.make_topology();
  REQUIRE_EQ(topology.generation().value(), 0u);

  REQUIRE_OK(topology.register_producer(ProducerId{1}, IncarnationId{1}, "p1"));
  const TopologyGeneration after_register = topology.generation();
  REQUIRE(after_register.value() > 0);

  // Idempotent registration does not advance the generation.
  REQUIRE_OK(topology.register_producer(ProducerId{1}, IncarnationId{1}, "p1"));
  REQUIRE_EQ(topology.generation(), after_register);

  REQUIRE_ERROR(topology.register_producer(ProducerId{1}, IncarnationId{1}, "elsewhere"), ErrorCode::IdentityMismatch);
  REQUIRE_ERROR(topology.set_producer_state(ProducerId{1}, IncarnationId{99}, ParticipantState::Failed),
                ErrorCode::StaleIncarnation);

  REQUIRE_OK(topology.set_producer_state(ProducerId{1}, IncarnationId{1}, ParticipantState::Failed));
  REQUIRE(topology.generation() > after_register);
  REQUIRE_EQ(topology.active_producer_count(), 0u);
  REQUIRE_ERROR(topology.owner_of(PartitionId{0}), ErrorCode::PartitionNotOwned);

  // A fresh incarnation supersedes the failed one and regains ownership.
  REQUIRE_OK(topology.register_producer(ProducerId{1}, IncarnationId{2}, "p1"));
  REQUIRE_EQ(topology.active_producer_count(), 1u);
  REQUIRE_OK(topology.owner_of(PartitionId{0}));
}

SHUFFLE_TEST(topology, interest_patterns_are_interned_and_canonical) {
  Fixture fixture;
  Topology topology = fixture.make_topology();

  REQUIRE_OK(topology.register_producer(ProducerId{1}, IncarnationId{1}, "p1"));
  // Registration order deliberately reversed to prove canonical ordering.
  REQUIRE_OK(topology.register_consumer(ConsumerId{5}, IncarnationId{1}, "c5",
                                        PartitionSelection::range(PartitionId{2}, PartitionId{6})));
  REQUIRE_OK(topology.register_consumer(ConsumerId{2}, IncarnationId{1}, "c2", PartitionSelection::all()));
  REQUIRE_OK(topology.register_consumer(ConsumerId{4}, IncarnationId{1}, "c4",
                                        PartitionSelection::range(PartitionId{2}, PartitionId{6})));
  const auto listed = PartitionSelection::from_list({PartitionId{1}, PartitionId{9}}, fixture.limits);
  REQUIRE_OK(listed);
  REQUIRE_OK(topology.register_consumer(ConsumerId{1}, IncarnationId{1}, "c1", listed.value()));

  REQUIRE_EQ(topology.pattern_count(), 3u);

  const std::vector<ConsumerId> covering_two = topology.covering_consumers(PartitionId{2});
  REQUIRE_EQ(covering_two.size(), std::size_t{3});
  // "all" sorts before "list:..." before "range:...", and consumers ascend
  // inside each pattern. Partition 2 is covered by "all" (c2) and "range:2:6"
  // (c4, c5); the "list:1,9" pattern does not cover it.
  REQUIRE_EQ(covering_two[0], ConsumerId{2});
  REQUIRE_EQ(covering_two[1], ConsumerId{4});
  REQUIRE_EQ(covering_two[2], ConsumerId{5});

  const std::vector<ConsumerId> covering_nine = topology.covering_consumers(PartitionId{9});
  REQUIRE_EQ(covering_nine.size(), std::size_t{2});
  REQUIRE_EQ(covering_nine[0], ConsumerId{2});
  REQUIRE_EQ(covering_nine[1], ConsumerId{1});

  REQUIRE_EQ(topology.covering_consumer_count(PartitionId{2}), std::size_t{3});
  REQUIRE_EQ(topology.covering_consumer_count(PartitionId{0}), std::size_t{1});
  REQUIRE_OK(topology.validate());
}

SHUFFLE_TEST(topology, planned_edges_match_brute_force_expansion) {
  Fixture fixture;
  Topology topology = fixture.make_topology();
  REQUIRE_OK(topology.register_producer(ProducerId{1}, IncarnationId{1}, "p1"));
  REQUIRE_OK(topology.register_producer(ProducerId{2}, IncarnationId{1}, "p2"));
  REQUIRE_OK(topology.register_consumer(ConsumerId{1}, IncarnationId{1}, "c1", PartitionSelection::all()));
  REQUIRE_OK(topology.register_consumer(ConsumerId{2}, IncarnationId{1}, "c2",
                                        PartitionSelection::range(PartitionId{0}, PartitionId{5})));
  const auto listed = PartitionSelection::from_list({PartitionId{3}, PartitionId{7}, PartitionId{11}}, fixture.limits);
  REQUIRE_OK(listed);
  REQUIRE_OK(topology.register_consumer(ConsumerId{3}, IncarnationId{1}, "c3", listed.value()));

  const auto planned = topology.planned_edge_count();
  REQUIRE_OK(planned);
  REQUIRE_EQ(planned.value(), brute_force_edges(topology));
  REQUIRE_EQ(planned.value(), std::uint64_t{kPartitions + 5 + 3});

  // Withdrawing a consumer removes its edges without materialising anything.
  REQUIRE_OK(topology.withdraw_consumer(ConsumerId{1}, IncarnationId{1}));
  const auto after = topology.planned_edge_count();
  REQUIRE_OK(after);
  REQUIRE_EQ(after.value(), brute_force_edges(topology));
  REQUIRE_EQ(after.value(), std::uint64_t{5 + 3});
}

SHUFFLE_TEST(topology, fan_bounds_are_computed_in_closed_form) {
  Fixture fixture;
  Topology topology = fixture.make_topology(16);
  REQUIRE_OK(topology.register_producer(ProducerId{1}, IncarnationId{1}, "p1"));
  REQUIRE_OK(topology.register_producer(ProducerId{2}, IncarnationId{1}, "p2"));
  REQUIRE_OK(topology.register_consumer(ConsumerId{1}, IncarnationId{1}, "c1", PartitionSelection::all()));
  const auto range = PartitionSelection::range(PartitionId{0}, PartitionId{3});
  REQUIRE_OK(topology.register_consumer(ConsumerId{2}, IncarnationId{1}, "c2", range));
  const auto listed = PartitionSelection::from_list({PartitionId{0}, PartitionId{2}, PartitionId{4}}, fixture.limits);
  REQUIRE_OK(listed);
  REQUIRE_OK(topology.register_consumer(ConsumerId{3}, IncarnationId{1}, "c3", listed.value()));

  const auto fan_out_two = topology.fan_out(PartitionId{2});
  REQUIRE_OK(fan_out_two);
  REQUIRE_EQ(fan_out_two.value(), 3u);
  const auto fan_out_three = topology.fan_out(PartitionId{3});
  REQUIRE_OK(fan_out_three);
  REQUIRE_EQ(fan_out_three.value(), 1u);

  const auto fan_in_all = topology.fan_in(ConsumerId{1});
  REQUIRE_OK(fan_in_all);
  REQUIRE_EQ(fan_in_all.value(), 2u);  // two active producers, both own some partition
  // Partitions 0, 2 and 4 all map onto ownership residue 0 with two
  // producers, so this consumer is served by exactly one producer.
  const auto fan_in_list = topology.fan_in(ConsumerId{3});
  REQUIRE_OK(fan_in_list);
  REQUIRE_EQ(fan_in_list.value(), 1u);
  const auto fan_in_range = topology.fan_in(ConsumerId{2});
  REQUIRE_OK(fan_in_range);
  REQUIRE_EQ(fan_in_range.value(), 2u);  // partitions 0..2 alternate owners
  REQUIRE_ERROR(topology.fan_in(ConsumerId{42}), ErrorCode::UnknownParticipant);
}

SHUFFLE_TEST(topology, selections_are_validated) {
  Fixture fixture;
  Topology topology = fixture.make_topology();
  REQUIRE_OK(topology.register_producer(ProducerId{1}, IncarnationId{1}, "p1"));

  REQUIRE_ERROR(topology.register_consumer(ConsumerId{1}, IncarnationId{1}, "c",
                                           PartitionSelection::range(PartitionId{5}, PartitionId{5})),
                ErrorCode::InvalidArgument);
  REQUIRE_ERROR(topology.register_consumer(ConsumerId{1}, IncarnationId{1}, "c",
                                           PartitionSelection::range(PartitionId{0}, PartitionId{kPartitions + 1})),
                ErrorCode::UnknownPartition);
  REQUIRE_ERROR(PartitionSelection::from_list({}, fixture.limits), ErrorCode::InvalidArgument);
  REQUIRE_ERROR(PartitionSelection::from_list({PartitionId{1}, PartitionId{1}}, fixture.limits),
                ErrorCode::InvalidArgument);

  const auto canonical = PartitionSelection::from_list({PartitionId{5}, PartitionId{1}, PartitionId{3}},
                                                       fixture.limits);
  REQUIRE_OK(canonical);
  REQUIRE_EQ(canonical.value().canonical_key(), std::string{"list:1,3,5"});
  REQUIRE(canonical.value().covers(PartitionId{3}));
  REQUIRE_FALSE(canonical.value().covers(PartitionId{4}));
  REQUIRE_EQ(PartitionSelection::all().canonical_key(), std::string{"all"});
  REQUIRE_EQ(PartitionSelection::range(PartitionId{1}, PartitionId{4}).canonical_key(), std::string{"range:1:4"});

  REQUIRE_OK(topology.register_consumer(ConsumerId{1}, IncarnationId{1}, "c1", PartitionSelection::all()));
  // The same selection content interns to a single pattern.
  REQUIRE_OK(topology.register_consumer(ConsumerId{2}, IncarnationId{1}, "c2", PartitionSelection::all()));
  REQUIRE_EQ(topology.pattern_count(), 1u);
  REQUIRE_OK(topology.validate());
}

SHUFFLE_TEST(topology, selection_counts_are_clamped_to_the_partition_space) {
  const auto wide = PartitionSelection::range(PartitionId{4}, PartitionId{1000});
  const auto count = wide.selected_count(16);
  REQUIRE_OK(count);
  REQUIRE_EQ(count.value(), std::uint64_t{12});
  const auto beyond = PartitionSelection::range(PartitionId{100}, PartitionId{200}).selected_count(16);
  REQUIRE_OK(beyond);
  REQUIRE_EQ(beyond.value(), std::uint64_t{0});
  const auto all = PartitionSelection::all().selected_count(16);
  REQUIRE_OK(all);
  REQUIRE_EQ(all.value(), std::uint64_t{16});
}

SHUFFLE_TEST(policy, structural_validation_rejects_inconsistent_envelopes) {
  PolicyEnvelope policy = make_policy();
  REQUIRE_OK(validate_policy(policy));

  PolicyEnvelope zero_fan = policy;
  zero_fan.fan.max_fan_out = 0;
  REQUIRE_ERROR(validate_policy(zero_fan), ErrorCode::InvalidArgument);

  PolicyEnvelope over_global = policy;
  over_global.concurrency.per_source = over_global.concurrency.global + 1;
  REQUIRE_ERROR(validate_policy(over_global), ErrorCode::InvalidArgument);

  PolicyEnvelope hysteresis = policy;
  hysteresis.congestion.pause_threshold = 10;
  hysteresis.congestion.resume_threshold = 20;
  REQUIRE_ERROR(validate_policy(hysteresis), ErrorCode::InvalidArgument);

  PolicyEnvelope no_retry = policy;
  no_retry.retry.max_attempts = 0;
  REQUIRE_ERROR(validate_policy(no_retry), ErrorCode::InvalidArgument);

  PolicyEnvelope too_many_grants = policy;
  too_many_grants.waves.max_grants_per_wave = too_many_grants.limits.max_wave_grants + 1;
  REQUIRE_ERROR(validate_policy(too_many_grants), ErrorCode::InvalidArgument);

  PolicyEnvelope anonymous = policy;
  anonymous.shuffle = ShuffleId{};
  REQUIRE_ERROR(validate_policy(anonymous), ErrorCode::InvalidArgument);
}

SHUFFLE_TEST(policy, evaluation_reports_fan_bounds_and_edge_counts) {
  Fixture fixture;
  Topology topology = fixture.make_topology();
  REQUIRE_OK(topology.register_producer(ProducerId{1}, IncarnationId{1}, "p1"));
  for (std::uint32_t index = 1; index <= 6; ++index) {
    REQUIRE_OK(topology.register_consumer(ConsumerId{index}, IncarnationId{1}, "c",
                                          PartitionSelection::range(PartitionId{0}, PartitionId{4})));
  }

  PolicyEnvelope policy = make_policy();
  policy.fan.max_fan_out = 2;
  const auto evaluated = evaluate_policy(topology, policy);
  REQUIRE_OK(evaluated);
  REQUIRE_FALSE(evaluated.value().acceptable());
  REQUIRE_EQ(evaluated.value().max_fan_out, 6u);
  REQUIRE_EQ(evaluated.value().planned_edges, std::uint64_t{6 * 4});
  REQUIRE_EQ(evaluated.value().violations.front().code, ErrorCode::FanOutCeilingExceeded);

  PolicyEnvelope generous = make_policy();
  const auto accepted = evaluate_policy(topology, generous);
  REQUIRE_OK(accepted);
  REQUIRE(accepted.value().acceptable());
  REQUIRE_EQ(accepted.value().max_fan_in, 1u);

  PolicyEnvelope wrong_shuffle = generous;
  wrong_shuffle.shuffle = ShuffleId{99};
  REQUIRE_ERROR(evaluate_policy(topology, wrong_shuffle), ErrorCode::PolicyMismatch);
}

SHUFFLE_TEST(policy, encode_decode_round_trip) {
  const PolicyEnvelope policy = make_policy();
  ByteWriter writer;
  REQUIRE_OK(encode_policy(policy, writer));
  const std::vector<std::byte> encoded = writer.take();

  ByteReader reader{encoded, policy.limits, "policy"};
  const auto decoded = decode_policy(reader);
  REQUIRE_OK(decoded);
  REQUIRE_OK(reader.require_end());
  REQUIRE(decoded.value() == policy);
  REQUIRE(decoded.value().limits.max_frame_payload_bytes == policy.limits.max_frame_payload_bytes);

  std::vector<std::byte> extended = encoded;
  extended.push_back(std::byte{0x01});
  ByteReader trailing{extended, policy.limits, "policy trailing"};
  REQUIRE_OK(decode_policy(trailing));
  REQUIRE_ERROR(trailing.require_end(), ErrorCode::TrailingGarbage);

  const std::span<const std::byte> truncated{encoded.data(), encoded.size() - 4};
  ByteReader short_reader{truncated, policy.limits, "policy truncated"};
  REQUIRE_FALSE(decode_policy(short_reader).ok());

  REQUIRE(describe_policy(policy).find("max_fan_out") == std::string::npos);
  REQUIRE(describe_policy(policy).find("fan_out<=64") != std::string::npos);
}

}  // namespace
