// Partition and chunk manifests: what a producer claims it produced, and how a
// consumer proves it received exactly that.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "shuffle/fabric/bytes.hpp"
#include "shuffle/fabric/codec.hpp"
#include "shuffle/fabric/digest.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/identity.hpp"

namespace shuffle::fabric {

// A byte range of a partition with its own content digest. Chunk digests are
// what make corruption detectable per transfer instead of per partition.
struct ChunkDescriptor {
  ChunkId id{};
  std::uint64_t offset{0};
  std::uint32_t length{0};
  Digest digest{};

  friend bool operator==(const ChunkDescriptor&, const ChunkDescriptor&) noexcept = default;
};

// The immutable claim about one partition generation. A manifest binds the
// shuffle generation, the partition generation, the producing incarnation and
// the topology generation it was planned against, so a stale incarnation can
// never have its work accepted under a newer generation.
struct PartitionManifest {
  ShuffleId shuffle{};
  ShuffleGeneration shuffle_generation{};
  PartitionId partition{};
  PartitionGeneration partition_generation{};
  ProducerId producer{};
  IncarnationId producer_incarnation{};
  TopologyGeneration topology_generation{};
  std::uint64_t total_bytes{0};
  std::vector<ChunkDescriptor> chunks{};

  friend bool operator==(const PartitionManifest&, const PartitionManifest&) noexcept = default;
};

// Digest over the ordered chunk digests and the declared size. Two manifests
// with the same partition digest describe byte-identical partition content.
[[nodiscard]] Digest compute_partition_digest(const PartitionManifest& manifest);

// Digest over the canonical encoding of the whole manifest, including identity
// and generation fields. This is the value completion authority is bound to.
[[nodiscard]] Digest compute_manifest_digest(const PartitionManifest& manifest);

// Structural validation: contiguous chunk coverage, no overflow, bounded
// counts, non-zero digests, canonical chunk ordering.
[[nodiscard]] Status validate_manifest(const PartitionManifest& manifest, const Limits& limits);

[[nodiscard]] Result<std::vector<std::byte>> encode_manifest(const PartitionManifest& manifest, const Limits& limits);
[[nodiscard]] Result<PartitionManifest> decode_manifest(std::span<const std::byte> encoded, const Limits& limits);

// Verifies one chunk payload against its descriptor. A payload that does not
// match never becomes evidence of anything.
[[nodiscard]] Status verify_chunk(const ChunkDescriptor& chunk, std::span<const std::byte> payload);

// Verifies the concatenation of all chunk payloads against the manifest.
[[nodiscard]] Status verify_partition_content(const PartitionManifest& manifest, std::span<const std::byte> content);

// Builds a manifest from an in-memory payload, splitting it into chunks of at
// most max_chunk_bytes. The caller supplies the identity fields; the digests
// are computed here.
[[nodiscard]] Result<PartitionManifest> build_manifest(ShuffleId shuffle, ShuffleGeneration shuffle_generation,
                                                       PartitionId partition, PartitionGeneration partition_generation,
                                                       ProducerId producer, IncarnationId producer_incarnation,
                                                       TopologyGeneration topology_generation,
                                                       std::span<const std::byte> content, std::uint32_t max_chunk_bytes,
                                                       const Limits& limits);

// Deterministic synthetic payload used by examples and multiprocess proofs.
// Content depends only on the shuffle, generation, partition and size, so two
// independent processes derive identical bytes and digests.
[[nodiscard]] std::vector<std::byte> synthetic_partition_payload(ShuffleId shuffle, ShuffleGeneration shuffle_generation,
                                                                PartitionId partition, PartitionGeneration generation,
                                                                std::uint64_t size);

}  // namespace shuffle::fabric
