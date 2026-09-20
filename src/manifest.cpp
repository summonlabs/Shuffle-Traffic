// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/manifest.hpp"

#include <algorithm>
#include <cstring>

#include "shuffle/fabric/hash.hpp"

namespace shuffle::fabric {
namespace {

// Sanity bound on a single chunk: a chunk is transferred and verified whole, so
// it must fit comfortably inside one message payload.
constexpr std::uint32_t kMaxChunkBytes = 1u << 24;  // 16 MiB
constexpr std::uint64_t kMaxPartitionBytes = 1ull << 46;

void write_identity(ByteWriter& writer, const PartitionManifest& manifest) {
  writer.put_u64(manifest.shuffle.value());
  writer.put_u64(manifest.shuffle_generation.value());
  writer.put_u64(manifest.partition.value());
  writer.put_u64(manifest.partition_generation.value());
  writer.put_u64(manifest.producer.value());
  writer.put_u64(manifest.producer_incarnation.value());
  writer.put_u64(manifest.topology_generation.value());
  writer.put_u64(manifest.total_bytes);
  writer.put_u32(static_cast<std::uint32_t>(manifest.chunks.size()));
}

void write_chunks(ByteWriter& writer, const PartitionManifest& manifest) {
  for (const ChunkDescriptor& chunk : manifest.chunks) {
    writer.put_u64(chunk.id.value());
    writer.put_u64(chunk.offset);
    writer.put_u32(chunk.length);
    writer.put_digest(chunk.digest);
  }
}

}  // namespace

Digest compute_partition_digest(const PartitionManifest& manifest) {
  ByteWriter writer;
  writer.put_u64(manifest.total_bytes);
  writer.put_u32(static_cast<std::uint32_t>(manifest.chunks.size()));
  write_chunks(writer, manifest);
  return sha256(writer.data());
}

Digest compute_manifest_digest(const PartitionManifest& manifest) {
  ByteWriter writer;
  write_identity(writer, manifest);
  write_chunks(writer, manifest);
  return sha256(writer.data());
}

Status validate_manifest(const PartitionManifest& manifest, const Limits& limits) {
  if (manifest.chunks.empty()) {
    return Status{make_error(ErrorCode::ManifestInconsistent, "manifest declares no chunks")};
  }
  if (manifest.chunks.size() > limits.max_chunks_per_partition) {
    return Status{make_error(ErrorCode::TooManyChunks, "manifest exceeds max_chunks_per_partition")};
  }
  if (manifest.total_bytes == 0) {
    return Status{make_error(ErrorCode::ManifestInconsistent, "manifest declares zero bytes")};
  }
  if (manifest.total_bytes > kMaxPartitionBytes) {
    return Status{make_error(ErrorCode::OversizedInput, "manifest exceeds the partition size bound")};
  }
  if (manifest.partition.value() >= limits.max_partitions) {
    return Status{make_error(ErrorCode::TooManyPartitions, "partition identifier exceeds max_partitions")};
  }
  if (manifest.shuffle.is_zero()) {
    return Status{make_error(ErrorCode::ManifestInconsistent, "manifest has no shuffle identity")};
  }
  if (manifest.producer.is_zero()) {
    return Status{make_error(ErrorCode::ManifestInconsistent, "manifest has no producer identity")};
  }

  std::uint64_t expected_offset = 0;
  for (std::size_t index = 0; index < manifest.chunks.size(); ++index) {
    const ChunkDescriptor& chunk = manifest.chunks[index];
    if (chunk.id.value() != index) {
      return Status{make_error(ErrorCode::ManifestInconsistent, "chunk identifiers must be the canonical 0..n-1")};
    }
    if (chunk.length == 0) {
      return Status{make_error(ErrorCode::ManifestInconsistent, "chunk declares zero length")};
    }
    if (chunk.length > kMaxChunkBytes) {
      return Status{make_error(ErrorCode::OversizedInput, "chunk exceeds the chunk size bound")};
    }
    if (chunk.offset != expected_offset) {
      return Status{make_error(ErrorCode::ManifestInconsistent, "chunk coverage is not contiguous")};
    }
    if (chunk.digest.is_zero()) {
      return Status{make_error(ErrorCode::ManifestInconsistent, "chunk carries a zero digest")};
    }
    if (add_overflows(chunk.offset, chunk.length)) {
      return Status{make_error(ErrorCode::IntegerOverflow, "chunk coverage overflows")};
    }
    expected_offset = chunk.offset + chunk.length;
  }
  if (expected_offset != manifest.total_bytes) {
    return Status{make_error(ErrorCode::ManifestInconsistent, "chunk coverage does not sum to total_bytes")};
  }
  return Status{};
}

Result<std::vector<std::byte>> encode_manifest(const PartitionManifest& manifest, const Limits& limits) {
  const Status valid = validate_manifest(manifest, limits);
  if (!valid.ok()) {
    return Result<std::vector<std::byte>>{valid.error()};
  }
  ByteWriter writer;
  write_identity(writer, manifest);
  write_chunks(writer, manifest);
  return writer.take();
}

Result<PartitionManifest> decode_manifest(std::span<const std::byte> encoded, const Limits& limits) {
  if (encoded.size() > limits.max_message_payload_bytes) {
    return make_failure<PartitionManifest>(ErrorCode::OversizedInput, "encoded manifest exceeds max_message_payload_bytes");
  }
  ByteReader reader{encoded, limits, "partition manifest"};
  PartitionManifest manifest;
  manifest.shuffle = ShuffleId{reader.u64()};
  manifest.shuffle_generation = ShuffleGeneration{reader.u64()};
  manifest.partition = PartitionId{reader.u64()};
  manifest.partition_generation = PartitionGeneration{reader.u64()};
  manifest.producer = ProducerId{reader.u64()};
  manifest.producer_incarnation = IncarnationId{reader.u64()};
  manifest.topology_generation = TopologyGeneration{reader.u64()};
  manifest.total_bytes = reader.u64();
  const std::uint32_t chunk_count = reader.collection_count(limits.max_chunks_per_partition);
  if (!reader.ok()) {
    return Result<PartitionManifest>{reader.error()};
  }
  manifest.chunks.reserve(chunk_count);
  for (std::uint32_t index = 0; index < chunk_count; ++index) {
    ChunkDescriptor chunk;
    chunk.id = ChunkId{reader.u64()};
    chunk.offset = reader.u64();
    chunk.length = reader.u32();
    chunk.digest = reader.digest();
    if (!reader.ok()) {
      return Result<PartitionManifest>{reader.error()};
    }
    manifest.chunks.push_back(chunk);
  }
  const Status end = reader.require_end();
  if (!end.ok()) {
    return Result<PartitionManifest>{end.error()};
  }
  const Status valid = validate_manifest(manifest, limits);
  if (!valid.ok()) {
    return Result<PartitionManifest>{valid.error()};
  }
  return manifest;
}

Status verify_chunk(const ChunkDescriptor& chunk, std::span<const std::byte> payload) {
  if (payload.size() != chunk.length) {
    return Status{make_error(ErrorCode::IntegrityFailure, "chunk payload length does not match the descriptor")};
  }
  const Digest observed = sha256(payload);
  if (observed != chunk.digest) {
    return Status{make_error(ErrorCode::DigestMismatch, "chunk payload digest does not match the manifest")};
  }
  return Status{};
}

Status verify_partition_content(const PartitionManifest& manifest, std::span<const std::byte> content) {
  if (content.size() != manifest.total_bytes) {
    return Status{make_error(ErrorCode::IntegrityFailure, "partition payload length does not match the manifest")};
  }
  for (const ChunkDescriptor& chunk : manifest.chunks) {
    const auto begin = static_cast<std::size_t>(chunk.offset);
    const auto length = static_cast<std::size_t>(chunk.length);
    const Status verified = verify_chunk(chunk, content.subspan(begin, length));
    if (!verified.ok()) {
      return verified;
    }
  }
  return Status{};
}

Result<PartitionManifest> build_manifest(ShuffleId shuffle, ShuffleGeneration shuffle_generation,
                                         PartitionId partition, PartitionGeneration partition_generation,
                                         ProducerId producer, IncarnationId producer_incarnation,
                                         TopologyGeneration topology_generation, std::span<const std::byte> content,
                                         std::uint32_t max_chunk_bytes, const Limits& limits) {
  if (max_chunk_bytes == 0 || max_chunk_bytes > kMaxChunkBytes) {
    return make_failure<PartitionManifest>(ErrorCode::InvalidArgument, "max_chunk_bytes is outside the supported range");
  }
  if (content.empty()) {
    return make_failure<PartitionManifest>(ErrorCode::InvalidArgument, "cannot build a manifest for empty content");
  }
  const std::uint64_t chunk_count = (content.size() + max_chunk_bytes - 1) / max_chunk_bytes;
  if (chunk_count > limits.max_chunks_per_partition) {
    return make_failure<PartitionManifest>(ErrorCode::TooManyChunks, "content would exceed max_chunks_per_partition");
  }

  PartitionManifest manifest;
  manifest.shuffle = shuffle;
  manifest.shuffle_generation = shuffle_generation;
  manifest.partition = partition;
  manifest.partition_generation = partition_generation;
  manifest.producer = producer;
  manifest.producer_incarnation = producer_incarnation;
  manifest.topology_generation = topology_generation;
  manifest.total_bytes = content.size();
  manifest.chunks.reserve(static_cast<std::size_t>(chunk_count));

  std::uint64_t offset = 0;
  std::uint64_t index = 0;
  while (offset < content.size()) {
    const std::size_t remaining = content.size() - static_cast<std::size_t>(offset);
    const std::size_t take = std::min<std::size_t>(remaining, max_chunk_bytes);
    ChunkDescriptor chunk;
    chunk.id = ChunkId{index};
    chunk.offset = offset;
    chunk.length = static_cast<std::uint32_t>(take);
    chunk.digest = sha256(content.subspan(static_cast<std::size_t>(offset), take));
    manifest.chunks.push_back(chunk);
    offset += take;
    ++index;
  }

  const Status valid = validate_manifest(manifest, limits);
  if (!valid.ok()) {
    return Result<PartitionManifest>{valid.error()};
  }
  return manifest;
}

std::vector<std::byte> synthetic_partition_payload(ShuffleId shuffle, ShuffleGeneration shuffle_generation,
                                                  PartitionId partition, PartitionGeneration generation,
                                                  std::uint64_t size) {
  std::vector<std::byte> payload(static_cast<std::size_t>(size));
  ByteWriter seed_writer;
  seed_writer.put_u64(shuffle.value());
  seed_writer.put_u64(shuffle_generation.value());
  seed_writer.put_u64(partition.value());
  seed_writer.put_u64(generation.value());
  const std::vector<std::byte> seed = seed_writer.take();

  std::size_t offset = 0;
  std::uint64_t counter = 0;
  while (offset < payload.size()) {
    ByteWriter block;
    block.put_raw(seed);
    block.put_u64(counter);
    const Digest digest = sha256(block.data());
    const std::size_t take = std::min<std::size_t>(kDigestBytes, payload.size() - offset);
    std::memcpy(payload.data() + offset, digest.bytes.data(), take);
    offset += take;
    ++counter;
  }
  return payload;
}

}  // namespace shuffle::fabric
