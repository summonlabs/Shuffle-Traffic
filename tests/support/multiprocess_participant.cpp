// A real participant process for the multiprocess proof surface.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// The proof obligation is about independent OS processes exchanging real bytes
// over loopback TCP. This program is one such process: it either produces
// partitions (publishing manifests and serving chunk data from its own listener)
// or consumes them (planning waves through the coordinator, fetching every chunk
// from the producing peer, verifying digests and committing the result). It
// prints deterministic single lines so the test can synchronise on protocol
// state instead of guessing from timing.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "shuffle/fabric/codec.hpp"
#include "shuffle/fabric/frame.hpp"
#include "shuffle/fabric/hash.hpp"
#include "shuffle/fabric/manifest.hpp"
#include "shuffle/fabric/service.hpp"
#include "shuffle/fabric/socket.hpp"

namespace {

using namespace shuffle::fabric;

struct Options {
  std::string coordinator{"127.0.0.1:0"};
  std::string role{"consumer"};
  std::uint64_t id{1};
  std::uint64_t incarnation{0};
  std::string selection{"all"};
  std::uint64_t payload_bytes{256};
  std::uint32_t chunk_bytes{64};
  std::uint16_t listen_port{0};
  std::string data_host{"127.0.0.1"};
  std::uint32_t max_idle_waves{3};
  ShuffleId shuffle{1};
  ShuffleGeneration generation{1};
  std::uint32_t corrupt_chunk{0xffffffffu};
};

[[nodiscard]] bool parse_u64(const char* text, std::uint64_t& out) {
  if (text == nullptr || *text == '\0') {
    return false;
  }
  std::uint64_t value = 0;
  for (const char* cursor = text; *cursor != '\0'; ++cursor) {
    if (*cursor < '0' || *cursor > '9') {
      return false;
    }
    value = value * 10 + static_cast<std::uint64_t>(*cursor - '0');
  }
  out = value;
  return true;
}

[[nodiscard]] bool split_endpoint(const std::string& text, std::string& host, std::uint16_t& port) {
  const std::size_t colon = text.rfind(':');
  if (colon == std::string::npos) {
    return false;
  }
  host = text.substr(0, colon);
  std::uint64_t value = 0;
  if (!parse_u64(text.substr(colon + 1).c_str(), value) || value == 0 || value > 65535) {
    return false;
  }
  port = static_cast<std::uint16_t>(value);
  return true;
}

[[nodiscard]] bool parse_options(int argc, char** argv, Options& options) {
  for (int index = 1; index < argc; ++index) {
    const std::string flag = argv[index];
    const auto take = [&](std::string& value) {
      if (index + 1 >= argc) {
        return false;
      }
      value = argv[++index];
      return true;
    };
    std::string text;
    std::uint64_t number = 0;

    if (flag == "--coordinator") {
      if (!take(options.coordinator)) {
        return false;
      }
    } else if (flag == "--role") {
      if (!take(options.role)) {
        return false;
      }
    } else if (flag == "--selection") {
      if (!take(options.selection)) {
        return false;
      }
    } else if (flag == "--data-host") {
      if (!take(options.data_host)) {
        return false;
      }
    } else if (flag == "--id" || flag == "--incarnation" || flag == "--payload-bytes" || flag == "--chunk-bytes" ||
               flag == "--listen-port" || flag == "--max-idle-waves" || flag == "--shuffle" ||
               flag == "--generation" || flag == "--corrupt-chunk") {
      if (!take(text) || !parse_u64(text.c_str(), number)) {
        return false;
      }
      if (flag == "--id") {
        options.id = number;
      } else if (flag == "--incarnation") {
        options.incarnation = number;
      } else if (flag == "--payload-bytes") {
        options.payload_bytes = number;
      } else if (flag == "--chunk-bytes") {
        options.chunk_bytes = static_cast<std::uint32_t>(number);
      } else if (flag == "--listen-port") {
        options.listen_port = static_cast<std::uint16_t>(number);
      } else if (flag == "--max-idle-waves") {
        options.max_idle_waves = static_cast<std::uint32_t>(number);
      } else if (flag == "--shuffle") {
        options.shuffle = ShuffleId{number};
      } else if (flag == "--generation") {
        options.generation = ShuffleGeneration{number};
      } else {
        options.corrupt_chunk = static_cast<std::uint32_t>(number);
      }
    } else {
      std::fprintf(stderr, "unknown option: %s\n", flag.c_str());
      return false;
    }
  }
  if (options.role != "producer" && options.role != "consumer") {
    std::fprintf(stderr, "role must be producer or consumer\n");
    return false;
  }
  if (options.id == 0) {
    std::fprintf(stderr, "id must be non-zero\n");
    return false;
  }
  return true;
}

[[nodiscard]] PartitionSelection parse_selection(const std::string& text, std::uint32_t partitions) {
  if (text == "all") {
    return PartitionSelection::all();
  }
  const std::string range_prefix = "range:";
  if (text.rfind(range_prefix, 0) == 0) {
    const std::size_t colon = text.find(':', range_prefix.size());
    if (colon != std::string::npos) {
      std::uint64_t begin = 0;
      std::uint64_t end = 0;
      if (parse_u64(text.substr(range_prefix.size(), colon - range_prefix.size()).c_str(), begin) &&
          parse_u64(text.substr(colon + 1).c_str(), end) && begin < end && end <= partitions) {
        return PartitionSelection::range(PartitionId{begin}, PartitionId{end});
      }
    }
    return PartitionSelection::all();
  }
  const std::string list_prefix = "list:";
  if (text.rfind(list_prefix, 0) == 0) {
    std::vector<PartitionId> list;
    std::size_t start = list_prefix.size();
    while (start <= text.size()) {
      const std::size_t comma = text.find(',', start);
      const std::string piece = text.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
      std::uint64_t value = 0;
      if (!piece.empty() && parse_u64(piece.c_str(), value) && value < partitions) {
        list.push_back(PartitionId{value});
      }
      if (comma == std::string::npos) {
        break;
      }
      start = comma + 1;
    }
    const auto selection = PartitionSelection::from_list(list, Limits{});
    return selection.ok() ? selection.value() : PartitionSelection::all();
  }
  return PartitionSelection::all();
}

[[nodiscard]] std::uint64_t boot_nonce() {
  const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  return static_cast<std::uint64_t>(now);
}

// One chunk request per connection keeps the data plane trivially bounded.
[[nodiscard]] Status send_frame(Socket& socket, MessageType type, SessionId session, std::uint64_t sequence,
                                std::span<const std::byte> payload, const Limits& limits) {
  FrameHeader header;
  header.type = type;
  header.session = session;
  header.sequence = sequence;
  std::vector<std::byte> frame;
  const Status encoded = encode_frame(header, payload, frame, limits);
  if (!encoded.ok()) {
    return encoded;
  }
  return socket.send_all(frame, 5000);
}

struct Fetched {
  Status status{};
  std::vector<std::byte> payload{};
  std::vector<Digest> chunk_digests{};
  Digest partition_digest{};
};

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!parse_options(argc, argv, options)) {
    std::fprintf(stderr, "usage: multiprocess_participant --coordinator HOST:PORT --role producer|consumer --id N "
                         "[--incarnation N] [--selection all|range:A:B|list:a,b] [--payload-bytes N] [--chunk-bytes N] "
                         "[--corrupt-chunk K]\n");
    return 2;
  }

  const Limits limits{};
  SocketRuntime runtime;
  if (!runtime.active()) {
    std::fprintf(stderr, "PARTICIPANT-ERROR socket runtime unavailable\n");
    return 3;
  }

  std::string host;
  std::uint16_t port = 0;
  if (!split_endpoint(options.coordinator, host, port)) {
    std::fprintf(stderr, "PARTICIPANT-ERROR coordinator endpoint is malformed\n");
    return 2;
  }

  // A participant without an explicit incarnation derives a fresh one, which is
  // what a restarted process does.
  if (options.incarnation == 0) {
    options.incarnation = boot_nonce() | 1ull;
  }

  // The producer needs its data-plane listener before it registers, because the
  // endpoint it advertises has to be the one it will serve on.
  TcpListener listener{};
  std::uint16_t data_port = 0;
  if (options.role == "producer") {
    auto bound = TcpListener::bind(options.data_host, options.listen_port, 16, runtime);
    if (!bound.ok()) {
      std::fprintf(stderr, "PARTICIPANT-ERROR data listener failed: %s\n", format_error(bound.error()).c_str());
      return 3;
    }
    listener = bound.take();
    const auto local = listener.local_port();
    if (!local.ok()) {
      std::fprintf(stderr, "PARTICIPANT-ERROR data port unknown\n");
      return 3;
    }
    data_port = local.value();
  }

  const std::string endpoint = options.data_host + ":" + std::to_string(data_port);

  CoordinatorClient client;
  ClientOptions client_options;
  client_options.host = host;
  client_options.port = port;
  client_options.identity.kind = options.role == "producer" ? ParticipantKind::Producer : ParticipantKind::Consumer;
  client_options.identity.participant_id = options.id;
  client_options.identity.incarnation = IncarnationId{options.incarnation};
  client_options.identity.boot_nonce = boot_nonce();
  const Status connected = client.connect(client_options);
  if (!connected.ok()) {
    std::fprintf(stderr, "PARTICIPANT-ERROR connect failed: %s\n", format_error(connected.error()).c_str());
    return 3;
  }

  const auto status = client.status();
  if (!status.ok()) {
    std::fprintf(stderr, "PARTICIPANT-ERROR status failed: %s\n", format_error(status.error()).c_str());
    return 3;
  }
  const std::uint32_t partitions = static_cast<std::uint32_t>(status.value().progress.partitions_total);
  const PartitionSelection selection = parse_selection(options.selection, partitions);

  const auto registered = client.register_participant(
      options.role == "producer" ? ParticipantKind::Producer : ParticipantKind::Consumer, options.id,
      IncarnationId{options.incarnation}, endpoint, selection);
  if (!registered.ok()) {
    std::fprintf(stderr, "PARTICIPANT-ERROR register failed: %s\n", format_error(registered.error()).c_str());
    return 3;
  }

  std::printf("PARTICIPANT-READY role=%s id=%llu incarnation=%llu endpoint=%s\n", options.role.c_str(),
              static_cast<unsigned long long>(options.id),
              static_cast<unsigned long long>(options.incarnation), endpoint.c_str());
  std::fflush(stdout);

  if (options.role == "producer") {
    // Publish every selected partition, bumping the generation when a previous
    // incarnation already described it.
    std::vector<PartitionManifest> published;
    for (std::uint32_t partition = 0; partition < partitions; ++partition) {
      if (!selection.covers(PartitionId{partition})) {
        continue;
      }
      PartitionGeneration generation{1};
      const auto existing = client.manifest(PartitionId{partition});
      if (existing.ok()) {
        if (existing.value().producer == ProducerId{options.id} &&
            existing.value().producer_incarnation == IncarnationId{options.incarnation}) {
          generation = existing.value().partition_generation;
        } else {
          generation = PartitionGeneration{existing.value().partition_generation.value() + 1};
        }
      }
      const std::vector<std::byte> payload = synthetic_partition_payload(
          options.shuffle, options.generation, PartitionId{partition}, generation, options.payload_bytes);
      const auto built = build_manifest(options.shuffle, options.generation, PartitionId{partition}, generation,
                                        ProducerId{options.id}, IncarnationId{options.incarnation},
                                        TopologyGeneration{0}, payload, options.chunk_bytes, limits);
      if (!built.ok()) {
        std::fprintf(stderr, "PARTICIPANT-ERROR manifest failed: %s\n", format_error(built.error()).c_str());
        return 3;
      }
      PartitionManifest manifest = built.value();
      manifest.topology_generation = status.value().topology_generation;
      const auto accepted = client.publish_manifest(manifest);
      if (!accepted.ok()) {
        std::fprintf(stderr, "PARTICIPANT-ERROR publish failed: %s\n", format_error(accepted.error()).c_str());
        return 3;
      }
      const auto stored = client.manifest(PartitionId{partition});
      if (!stored.ok()) {
        std::fprintf(stderr, "PARTICIPANT-ERROR manifest read back failed\n");
        return 3;
      }
      published.push_back(stored.value());
    }

    std::printf("PARTICIPANT-PUBLISHED role=producer id=%llu partitions=%zu\n",
                static_cast<unsigned long long>(options.id), published.size());
    std::fflush(stdout);

    // Serve chunks until the parent terminates this process.
    std::uint64_t served = 0;
    while (true) {
      auto accepted = listener.accept(200);
      if (!accepted.ok()) {
        if (accepted.code() == ErrorCode::PeerUnavailable || accepted.code() == ErrorCode::ConnectionClosed) {
          break;
        }
        continue;
      }
      Socket socket = accepted.take();
      FrameStream stream{limits};
      std::vector<std::byte> buffer(64 * 1024);
      bool answered = false;
      for (int attempt = 0; attempt < 64 && !answered; ++attempt) {
        const auto received = socket.recv_some(buffer, 200);
        if (!received.ok() || received.value() == 0) {
          break;
        }
        const Status fed = stream.feed(std::span<const std::byte>{buffer.data(), received.value()});
        if (!fed.ok()) {
          break;
        }
        const auto decoded = stream.next();
        if (!decoded.ok()) {
          continue;
        }
        ByteReader reader{decoded.value().payload, limits, "chunk fetch request"};
        const std::uint64_t partition = reader.u64();
        const std::uint64_t generation = reader.u64();
        const std::uint64_t chunk = reader.u64();
        if (!reader.ok()) {
          break;
        }
        const PartitionManifest* manifest = nullptr;
        for (const PartitionManifest& candidate : published) {
          if (candidate.partition.value() == partition &&
              candidate.partition_generation.value() == generation) {
            manifest = &candidate;
            break;
          }
        }
        if (manifest == nullptr) {
          ByteWriter writer;
          writer.put_u16(static_cast<std::uint16_t>(ErrorCode::UnknownPartition));
          writer.put_string("no such partition generation on this producer");
          static_cast<void>(
              send_frame(socket, MessageType::ChunkFetchFailure, decoded.value().header.session,
                         decoded.value().header.sequence, writer.data(), limits));
          answered = true;
          break;
        }
        const std::vector<std::byte> payload = synthetic_partition_payload(
            options.shuffle, options.generation, PartitionId{partition}, PartitionGeneration{generation},
            options.payload_bytes);
        if (chunk >= manifest->chunks.size()) {
          ByteWriter writer;
          writer.put_u16(static_cast<std::uint16_t>(ErrorCode::UnknownChunk));
          writer.put_string("no such chunk");
          static_cast<void>(
              send_frame(socket, MessageType::ChunkFetchFailure, decoded.value().header.session,
                         decoded.value().header.sequence, writer.data(), limits));
          answered = true;
          break;
        }
        const ChunkDescriptor& descriptor = manifest->chunks[static_cast<std::size_t>(chunk)];
        std::vector<std::byte> slice(payload.begin() + static_cast<std::ptrdiff_t>(descriptor.offset),
                                     payload.begin() + static_cast<std::ptrdiff_t>(descriptor.offset + descriptor.length));
        if (options.corrupt_chunk == chunk && !slice.empty()) {
          slice[0] = static_cast<std::byte>(std::to_integer<std::uint8_t>(slice[0]) ^ 0x5au);
        }
        ByteWriter writer;
        writer.put_u64(chunk);
        writer.put_u64(descriptor.offset);
        writer.put_u32(descriptor.length);
        writer.put_digest(descriptor.digest);
        writer.put_byte_string(slice);
        static_cast<void>(send_frame(socket, MessageType::ChunkFetchResponse, decoded.value().header.session,
                                     decoded.value().header.sequence, writer.data(), limits));
        ++served;
        answered = true;
      }
      socket.close();
    }
    std::printf("PARTICIPANT-DONE role=producer id=%llu served=%llu\n",
                static_cast<unsigned long long>(options.id), static_cast<unsigned long long>(served));
    std::fflush(stdout);
    return 0;
  }

  // Consumer: plan waves, fetch and verify every chunk, commit what verifies.
  std::uint64_t committed = 0;
  std::uint64_t failed = 0;
  std::uint64_t waves = 0;
  std::uint32_t idle = 0;
  std::uint64_t sequence = 1;

  while (idle < options.max_idle_waves) {
    const auto plan = client.next_wave();
    if (!plan.ok()) {
      std::printf("PARTICIPANT-DONE role=consumer id=%llu committed=%llu failed=%llu waves=%llu terminal=%s\n",
                  static_cast<unsigned long long>(options.id), static_cast<unsigned long long>(committed),
                  static_cast<unsigned long long>(failed), static_cast<unsigned long long>(waves),
                  to_string(plan.code()));
      std::fflush(stdout);
      return 0;
    }
    if (plan.value().grants.empty()) {
      ++idle;
      continue;
    }
    idle = 0;
    ++waves;
    for (const DispatchGrant& grant : plan.value().grants) {
      const auto manifest = client.manifest(grant.partition);
      if (!manifest.ok()) {
        static_cast<void>(client.report_failure(grant.attempt, manifest.code()));
        ++failed;
        continue;
      }
      const PartitionManifest& expected = manifest.value();
      std::vector<std::byte> assembled;
      std::vector<Digest> chunk_digests;
      bool verified = true;
      ErrorCode failure_code = ErrorCode::IntegrityFailure;

      std::string peer_host;
      std::uint16_t peer_port = 0;
      if (!split_endpoint(grant.producer_endpoint, peer_host, peer_port)) {
        static_cast<void>(client.report_failure(grant.attempt, ErrorCode::PeerUnavailable));
        ++failed;
        continue;
      }

      for (const ChunkDescriptor& descriptor : expected.chunks) {
        auto socket = Socket::connect(peer_host, peer_port, 2000, runtime);
        if (!socket.ok()) {
          failure_code = socket.code();
          verified = false;
          break;
        }
        ByteWriter writer;
        writer.put_u64(grant.partition.value());
        writer.put_u64(grant.partition_generation.value());
        writer.put_u64(descriptor.id.value());
        const Status sent = send_frame(socket.value(), MessageType::ChunkFetchRequest, SessionId{options.id},
                                       sequence++, writer.data(), limits);
        if (!sent.ok()) {
          failure_code = sent.code();
          verified = false;
          break;
        }
        FrameStream stream{limits};
        std::vector<std::byte> buffer(64 * 1024);
        bool received = false;
        for (int attempt = 0; attempt < 64 && !received; ++attempt) {
          const auto bytes = socket.value().recv_some(buffer, 2000);
          if (!bytes.ok() || bytes.value() == 0) {
            failure_code = bytes.ok() ? ErrorCode::ConnectionClosed : bytes.code();
            break;
          }
          const Status fed = stream.feed(std::span<const std::byte>{buffer.data(), bytes.value()});
          if (!fed.ok()) {
            failure_code = fed.code();
            break;
          }
          const auto decoded = stream.next();
          if (!decoded.ok()) {
            continue;
          }
          received = true;
          if (decoded.value().header.type == MessageType::ChunkFetchFailure) {
            failure_code = ErrorCode::PayloadRejected;
            break;
          }
          ByteReader reader{decoded.value().payload, limits, "chunk fetch response"};
          const std::uint64_t chunk_id = reader.u64();
          const std::uint64_t offset = reader.u64();
          const std::uint32_t length = reader.u32();
          const Digest digest = reader.digest();
          const auto payload = reader.bytes(limits.max_frame_payload_bytes);
          if (!reader.ok() || chunk_id != descriptor.id.value() || offset != descriptor.offset ||
              length != descriptor.length || digest != descriptor.digest) {
            failure_code = ErrorCode::MalformedInput;
            break;
          }
          const Digest observed = sha256(payload);
          if (observed != descriptor.digest) {
            failure_code = ErrorCode::DigestMismatch;
            break;
          }
          chunk_digests.push_back(observed);
          assembled.insert(assembled.end(), payload.begin(), payload.end());
        }
        if (!received) {
          verified = false;
          break;
        }
        static_cast<void>(socket.value().shutdown());
        socket.value().close();
      }

      if (!verified) {
        static_cast<void>(client.report_failure(grant.attempt, failure_code));
        ++failed;
        continue;
      }

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
      request.manifest_digest = compute_manifest_digest(expected);
      request.observed_partition_digest = compute_partition_digest(expected);
      request.bytes = expected.total_bytes;
      request.integrity_verified = true;
      request.observed_chunk_digests = chunk_digests;
      const auto outcome = client.commit_transfer(request);
      if (outcome.ok()) {
        ++committed;
      } else {
        static_cast<void>(client.report_failure(grant.attempt, outcome.code()));
        ++failed;
      }
    }
  }

  std::printf("PARTICIPANT-DONE role=consumer id=%llu committed=%llu failed=%llu waves=%llu\n",
              static_cast<unsigned long long>(options.id), static_cast<unsigned long long>(committed),
              static_cast<unsigned long long>(failed), static_cast<unsigned long long>(waves));
  std::fflush(stdout);
  return 0;
}
