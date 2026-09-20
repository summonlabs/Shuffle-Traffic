// Loopback proof surface for the coordinator service: real TCP, one mutex, one
// response frame per request, deterministic refusals.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Every expectation here is a protocol fact, never a timing fact. A peer that
// has already been asked for something is read with a bounded wait (the socket
// layer requires one for liveness); no assertion depends on how long an
// operation took, and nothing sleeps.
//
// The raw-message proofs build their payloads from the documented little-endian
// layout with their own helpers instead of the library's codec, so a layout
// mistake cannot prove itself correct.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "shuffle/fabric/bytes.hpp"
#include "shuffle/fabric/codec.hpp"
#include "shuffle/fabric/coordinator.hpp"
#include "shuffle/fabric/durable_store.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/frame.hpp"
#include "shuffle/fabric/hash.hpp"
#include "shuffle/fabric/identity.hpp"
#include "shuffle/fabric/ledger.hpp"
#include "shuffle/fabric/manifest.hpp"
#include "shuffle/fabric/policy.hpp"
#include "shuffle/fabric/records.hpp"
#include "shuffle/fabric/schedule.hpp"
#include "shuffle/fabric/service.hpp"
#include "shuffle/fabric/socket.hpp"
#include "shuffle/fabric/topology.hpp"
#include "temp_dir.hpp"
#include "test_support.hpp"

namespace shuffle::fabric {

// Test-only rendering so a failure names the message type, the error code and
// the shuffle state instead of a number.
inline std::ostream& operator<<(std::ostream& stream, MessageType type) { return stream << to_string(type); }
inline std::ostream& operator<<(std::ostream& stream, ErrorCode code) { return stream << to_string(code); }
inline std::ostream& operator<<(std::ostream& stream, ShuffleState state) { return stream << to_string(state); }
inline std::ostream& operator<<(std::ostream& stream, ParticipantKind kind) { return stream << to_string(kind); }

}  // namespace shuffle::fabric

namespace {

using namespace shuffle::fabric;

// The wait every raw read uses. It is transport responsiveness only: the peer
// has always already been asked for the frame being read, so an expiry is a
// defect and never a scheduling accident the test tolerates.
constexpr int kPeerWaitMs = 2000;

// ---------------------------------------------------------------------------
// Independent little-endian helpers. The library's codec is never used to build
// an expectation of the documented layout.
// ---------------------------------------------------------------------------

void put_u8(std::vector<std::byte>& out, std::uint8_t value) { out.push_back(static_cast<std::byte>(value)); }

void put_u16(std::vector<std::byte>& out, std::uint16_t value) {
  put_u8(out, static_cast<std::uint8_t>(value & 0xffu));
  put_u8(out, static_cast<std::uint8_t>((value >> 8) & 0xffu));
}

void put_u32(std::vector<std::byte>& out, std::uint32_t value) {
  for (std::size_t index = 0; index < 4; ++index) {
    put_u8(out, static_cast<std::uint8_t>((value >> (index * 8)) & 0xffu));
  }
}

void put_u64(std::vector<std::byte>& out, std::uint64_t value) {
  for (std::size_t index = 0; index < 8; ++index) {
    put_u8(out, static_cast<std::uint8_t>((value >> (index * 8)) & 0xffu));
  }
}

void put_str(std::vector<std::byte>& out, std::string_view text) {
  put_u32(out, static_cast<std::uint32_t>(text.size()));
  for (const char character : text) {
    out.push_back(static_cast<std::byte>(static_cast<unsigned char>(character)));
  }
}

[[nodiscard]] std::uint8_t byte_at(std::span<const std::byte> bytes, std::size_t offset) {
  return std::to_integer<std::uint8_t>(bytes[offset]);
}

[[nodiscard]] std::uint32_t u32_at(std::span<const std::byte> bytes, std::size_t offset) {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value |= static_cast<std::uint32_t>(byte_at(bytes, offset + index)) << (index * 8);
  }
  return value;
}

[[nodiscard]] std::uint64_t u64_at(std::span<const std::byte> bytes, std::size_t offset) {
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    value |= static_cast<std::uint64_t>(byte_at(bytes, offset + index)) << (index * 8);
  }
  return value;
}

// {u16 ErrorCode, u32 detail_length, detail bytes}
struct Envelope {
  ErrorCode code{ErrorCode::Ok};
  std::string detail{};
};

[[nodiscard]] Envelope parse_envelope(std::span<const std::byte> payload) {
  Envelope envelope;
  REQUIRE(payload.size() >= 6u);
  const std::uint16_t raw_code =
      static_cast<std::uint16_t>(byte_at(payload, 0) | (static_cast<std::uint16_t>(byte_at(payload, 1)) << 8));
  const std::uint32_t length = u32_at(payload, 2);
  REQUIRE(payload.size() >= 6u + static_cast<std::size_t>(length));
  envelope.code = static_cast<ErrorCode>(raw_code);
  envelope.detail.assign(reinterpret_cast<const char*>(payload.data()) + 6, length);
  return envelope;
}

// The type-specific part of a reply payload: everything after the envelope.
[[nodiscard]] std::span<const std::byte> reply_body(std::span<const std::byte> payload) {
  return payload.subspan(6u + static_cast<std::size_t>(u32_at(payload, 2)));
}

[[nodiscard]] bool same_bytes(std::span<const std::byte> lhs, std::span<const std::byte> rhs) {
  return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

// ---------------------------------------------------------------------------
// Raw peer: a Socket plus the frame layer, with no client on top of it
// ---------------------------------------------------------------------------

struct RawPeer {
  explicit RawPeer(const Limits& bounds) : stream(bounds) {}

  RawPeer(const RawPeer&) = delete;
  RawPeer& operator=(const RawPeer&) = delete;

  void connect(std::uint16_t port) {
    auto connecting = Socket::connect("127.0.0.1", port, kPeerWaitMs, runtime);
    REQUIRE_OK(connecting);
    socket = connecting.take();
  }

  // The socket runtime is declared first so that it outlives the socket.
  SocketRuntime runtime;
  Socket socket;
  FrameStream stream;
  std::uint64_t sequence{1};
  SessionId session{};
};

[[nodiscard]] std::vector<std::byte> raw_frame(MessageType type, SessionId session, std::uint64_t sequence,
                                               std::uint32_t flags, std::span<const std::byte> payload) {
  FrameHeader header;
  header.type = type;
  header.flags = flags;
  header.session = session;
  header.sequence = sequence;
  std::vector<std::byte> frame;
  REQUIRE_OK(encode_frame(header, payload, frame, Limits{}));
  REQUIRE_EQ(frame.size(), kFrameHeaderBytes + payload.size());
  return frame;
}

// Reads exactly one frame. Bytes beyond it are left in the stream for the next
// call, so a peer that pipelines two replies is still read frame by frame.
[[nodiscard]] std::vector<std::byte> receive_one(RawPeer& peer) {
  std::vector<std::byte> received;
  std::array<std::byte, 256> chunk{};
  for (;;) {
    const auto frame = peer.stream.next();
    if (frame.ok()) {
      received.resize(frame.value().consumed);
      return received;
    }
    REQUIRE_ERROR(frame, ErrorCode::NoWorkAvailable);
    const auto count = peer.socket.recv_some(chunk, kPeerWaitMs);
    REQUIRE_OK(count);
    REQUIRE(count.value() > 0);
    const std::span<const std::byte> arrived(chunk.data(), count.value());
    received.insert(received.end(), arrived.begin(), arrived.end());
    REQUIRE_OK(peer.stream.feed(arrived));
  }
}

[[nodiscard]] std::vector<std::byte> send_and_receive(RawPeer& peer, const std::vector<std::byte>& frame) {
  REQUIRE_OK(peer.socket.send_all(frame, kPeerWaitMs));
  return receive_one(peer);
}

// One reply kept together with the bytes it points into. A DecodedFrame holds
// views into the buffer it was decoded from, so that buffer must outlive every
// use of the frame: decoding from a temporary vector would be a use-after-free.
struct RawReply {
  RawReply(std::vector<std::byte> received, Result<DecodedFrame> decoded)
      : bytes(std::move(received)), frame(std::move(decoded)) {}

  std::vector<std::byte> bytes;
  Result<DecodedFrame> frame;
};

[[nodiscard]] RawReply round_trip(RawPeer& peer, const std::vector<std::byte>& frame) {
  std::vector<std::byte> received = send_and_receive(peer, frame);
  auto decoded = decode_frame(received, Limits{});
  REQUIRE_OK(decoded);
  return RawReply{std::move(received), std::move(decoded)};
}

[[nodiscard]] std::vector<std::byte> handshake_payload(ParticipantKind kind, std::uint64_t id,
                                                      std::uint64_t incarnation, std::uint64_t boot_nonce) {
  std::vector<std::byte> payload;
  put_u8(payload, static_cast<std::uint8_t>(kind));
  put_u64(payload, id);
  put_u64(payload, incarnation);
  put_u64(payload, boot_nonce);
  return payload;
}

[[nodiscard]] SessionId raw_handshake(RawPeer& peer, ParticipantKind kind, std::uint64_t id, std::uint64_t incarnation,
                                      std::uint64_t boot_nonce) {
  const std::vector<std::byte> payload = handshake_payload(kind, id, incarnation, boot_nonce);
  const std::vector<std::byte> frame =
      raw_frame(MessageType::HandshakeRequest, SessionId{}, peer.sequence, 0, payload);
  const std::vector<std::byte> reply = send_and_receive(peer, frame);
  const auto decoded = decode_frame(reply, Limits{});
  REQUIRE_OK(decoded);
  REQUIRE_EQ(decoded.value().header.type, MessageType::HandshakeResponse);
  REQUIRE(decoded.value().header.carries_response_flag());
  REQUIRE_EQ(decoded.value().header.sequence, peer.sequence);
  REQUIRE_FALSE(decoded.value().header.session.is_zero());
  REQUIRE_EQ(parse_envelope(decoded.value().payload).code, ErrorCode::Ok);
  peer.session = decoded.value().header.session;
  ++peer.sequence;
  return peer.session;
}

// RegisterParticipant {u8 kind, u64 id, u64 incarnation, string endpoint,
//                      u8 selection_kind, u64 begin, u64 end, u32 count, u64[]}
[[nodiscard]] std::vector<std::byte> register_payload(ParticipantKind kind, std::uint64_t id, std::uint64_t incarnation,
                                                     std::string_view endpoint, SelectionKind selection_kind,
                                                     std::uint64_t begin, std::uint64_t end,
                                                     const std::vector<std::uint64_t>& list) {
  std::vector<std::byte> payload;
  put_u8(payload, static_cast<std::uint8_t>(kind));
  put_u64(payload, id);
  put_u64(payload, incarnation);
  put_str(payload, endpoint);
  put_u8(payload, static_cast<std::uint8_t>(selection_kind));
  put_u64(payload, begin);
  put_u64(payload, end);
  put_u32(payload, static_cast<std::uint32_t>(list.size()));
  for (const std::uint64_t partition : list) {
    put_u64(payload, partition);
  }
  return payload;
}

// ---------------------------------------------------------------------------
// Server and client construction
// ---------------------------------------------------------------------------

[[nodiscard]] ServerOptions server_options(const Limits& bounds, std::uint32_t workers = 4,
                                           std::uint32_t sessions = 8) {
  ServerOptions options;
  options.limits = bounds;
  options.bind_host = "127.0.0.1";
  options.port = 0;
  options.worker_threads = workers;
  options.max_sessions = sessions;
  return options;
}

[[nodiscard]] ClientOptions client_options(std::uint16_t port, ParticipantKind kind, std::uint64_t id,
                                           std::uint64_t incarnation, std::uint64_t boot_nonce,
                                           const Limits& bounds = Limits{}) {
  ClientOptions options;
  options.limits = bounds;
  options.host = "127.0.0.1";
  options.port = port;
  options.identity = SessionIdentity{kind, id, IncarnationId{incarnation}, boot_nonce};
  return options;
}

[[nodiscard]] ShuffleOpenRequest open_request(ShuffleId shuffle, ShuffleGeneration generation,
                                              std::uint32_t partitions, const Limits& bounds) {
  ShuffleOpenRequest request;
  request.shuffle = shuffle;
  request.generation = generation;
  request.partition_count = partitions;
  request.policy.shuffle = shuffle;
  request.policy.shuffle_generation = generation;
  request.policy.generation = PolicyGeneration{1};
  request.policy.limits = bounds;
  return request;
}

// ===========================================================================
// 1. start/stop, the bound port, repeated cycles
// ===========================================================================

SHUFFLE_TEST(service_loopback, start_stop_cycles_report_the_bound_port_and_leak_no_session) {
  const Limits limits;
  VolatileSink sink;
  Coordinator coordinator{limits, &sink};
  CoordinatorServer server{coordinator, server_options(limits, 2, 4)};

  REQUIRE_FALSE(server.running());
  REQUIRE_EQ(server.port(), 0u);
  REQUIRE_OK(server.stop());  // stop before start is a no-op
  REQUIRE_FALSE(server.running());

  REQUIRE_OK(server.start());
  REQUIRE(server.running());
  const std::uint16_t bound_port = server.port();
  REQUIRE(bound_port > 0);

  // A real client proves that the reported port is the bound one.
  CoordinatorClient probe;
  REQUIRE_OK(probe.connect(client_options(bound_port, ParticipantKind::Producer, 1, 1, 0x11)));
  REQUIRE(probe.connected());
  REQUIRE_OK(probe.status());  // served, so the session exists on the server
  REQUIRE_EQ(server.stats().accepted_sessions, 1u);
  REQUIRE_EQ(server.stats().peak_sessions, 1u);
  REQUIRE_EQ(server.stats().active_sessions, 1u);
  REQUIRE_OK(probe.close());
  REQUIRE_OK(probe.close());  // idempotent
  REQUIRE_FALSE(probe.connected());
  REQUIRE_ERROR(probe.status(), ErrorCode::SessionClosed);

  for (int cycle = 0; cycle < 25; ++cycle) {
    REQUIRE_OK(server.stop());
    REQUIRE_FALSE(server.running());
    REQUIRE_EQ(server.port(), 0u);
    REQUIRE_EQ(server.stats().active_sessions, 0u);
    REQUIRE_OK(server.start());
    REQUIRE(server.running());
    REQUIRE(server.port() > 0);
  }

  REQUIRE_OK(server.stop());
  REQUIRE_OK(server.stop());  // idempotent, including after a real run
  REQUIRE_FALSE(server.running());
  REQUIRE_EQ(server.port(), 0u);
  REQUIRE_EQ(server.stats().active_sessions, 0u);
}

// ===========================================================================
// 2. a full mini shuffle over real TCP
// ===========================================================================

SHUFFLE_TEST(service_loopback, a_mini_shuffle_runs_over_real_tcp) {
  const Limits limits;
  VolatileSink sink;
  Coordinator coordinator{limits, &sink};
  CoordinatorServer server{coordinator, server_options(limits)};
  REQUIRE_OK(server.start());

  const std::uint16_t port = server.port();
  CoordinatorClient producer;
  REQUIRE_OK(producer.connect(client_options(port, ParticipantKind::Producer, 1, 11, 0x1001)));
  CoordinatorClient consumer;
  REQUIRE_OK(consumer.connect(client_options(port, ParticipantKind::Consumer, 1, 21, 0x2002)));

  const ShuffleId shuffle{7};
  const ShuffleGeneration generation{3};
  const ShuffleOpenRequest open = open_request(shuffle, generation, 2, limits);
  REQUIRE_OK(producer.open_shuffle(open));
  REQUIRE_ERROR(producer.open_shuffle(open), ErrorCode::InvalidState);  // one shuffle at a time

  const auto producer_registration =
      producer.register_participant(ParticipantKind::Producer, 1, IncarnationId{11}, "127.0.0.1:41001",
                                    PartitionSelection::all());
  REQUIRE_OK(producer_registration);
  const auto consumer_registration =
      consumer.register_participant(ParticipantKind::Consumer, 1, IncarnationId{21}, "127.0.0.1:41002",
                                    PartitionSelection::all());
  REQUIRE_OK(consumer_registration);
  REQUIRE_EQ(producer_registration.value().topology_generation, TopologyGeneration{1});
  REQUIRE_EQ(producer_registration.value().incarnation, IncarnationId{11});
  REQUIRE_FALSE(producer_registration.value().superseded_previous);
  REQUIRE_EQ(consumer_registration.value().topology_generation, TopologyGeneration{2});

  // Real content: 512 bytes in two chunks of 256.
  const std::vector<std::byte> payload =
      synthetic_partition_payload(shuffle, generation, PartitionId{0}, PartitionGeneration{1}, 512);
  REQUIRE_EQ(payload.size(), 512u);
  const auto manifest =
      build_manifest(shuffle, generation, PartitionId{0}, PartitionGeneration{1}, ProducerId{1}, IncarnationId{11},
                     TopologyGeneration{7}, payload, 256, limits);
  REQUIRE_OK(manifest);
  REQUIRE_EQ(manifest.value().chunks.size(), 2u);
  REQUIRE_EQ(manifest.value().total_bytes, 512u);

  const auto acceptance = producer.publish_manifest(manifest.value());
  REQUIRE_OK(acceptance);
  REQUIRE_EQ(acceptance.value().partition_generation, PartitionGeneration{1});

  // The accepted manifest is normalized to the current topology generation, and
  // that normalized manifest is what the digest binds.
  const auto fetched = consumer.manifest(PartitionId{0});
  REQUIRE_OK(fetched);
  REQUIRE_EQ(fetched.value().topology_generation, consumer_registration.value().topology_generation);
  REQUIRE_NE(fetched.value().topology_generation, TopologyGeneration{7});
  REQUIRE_EQ(compute_manifest_digest(fetched.value()), acceptance.value().manifest_digest);
  REQUIRE_EQ(fetched.value().partition, PartitionId{0});
  REQUIRE_EQ(fetched.value().producer, ProducerId{1});
  REQUIRE_EQ(fetched.value().producer_incarnation, IncarnationId{11});
  REQUIRE_EQ(fetched.value().total_bytes, 512u);
  REQUIRE_EQ(fetched.value().chunks.size(), 2u);
  REQUIRE_EQ(fetched.value().chunks[0].offset, 0u);
  REQUIRE_EQ(fetched.value().chunks[0].length, 256u);
  REQUIRE_EQ(fetched.value().chunks[1].offset, 256u);
  REQUIRE_EQ(fetched.value().chunks[1].length, 256u);
  REQUIRE_EQ(fetched.value().chunks[0].digest, sha256(std::span<const std::byte>(payload.data(), 256)));
  REQUIRE_EQ(fetched.value().chunks[1].digest, sha256(std::span<const std::byte>(payload.data() + 256, 256)));
  REQUIRE_OK(verify_partition_content(fetched.value(), payload));

  // Planning: partition 1 has no accepted manifest, so exactly one grant.
  const auto wave = consumer.next_wave();
  REQUIRE_OK(wave);
  REQUIRE_EQ(wave.value().id, WaveId{1});
  REQUIRE_EQ(wave.value().grants.size(), 1u);
  REQUIRE_FALSE(wave.value().all_resolved);
  REQUIRE_EQ(wave.value().skipped_completed, 0u);
  REQUIRE_EQ(wave.value().deferred_limits, 0u);

  const DispatchGrant grant = wave.value().grants.front();
  REQUIRE_EQ(grant.attempt, TransferAttemptId{1});
  REQUIRE_EQ(grant.wave, WaveId{1});
  REQUIRE_EQ(grant.shuffle, shuffle);
  REQUIRE_EQ(grant.shuffle_generation, generation);
  REQUIRE_EQ(grant.partition, PartitionId{0});
  REQUIRE_EQ(grant.partition_generation, PartitionGeneration{1});
  REQUIRE_EQ(grant.producer, ProducerId{1});
  REQUIRE_EQ(grant.producer_incarnation, IncarnationId{11});
  REQUIRE_EQ(grant.producer_endpoint, std::string{"127.0.0.1:41001"});
  REQUIRE_EQ(grant.consumer, ConsumerId{1});
  REQUIRE_EQ(grant.consumer_incarnation, IncarnationId{21});
  REQUIRE_EQ(grant.topology_generation, consumer_registration.value().topology_generation);
  REQUIRE_EQ(grant.policy_generation, PolicyGeneration{1});
  REQUIRE_EQ(grant.manifest_digest, acceptance.value().manifest_digest);
  REQUIRE_EQ(grant.total_bytes, 512u);
  REQUIRE_EQ(grant.attempt_ordinal, 1u);

  // Completion evidence: the manifest digest, the per-chunk digests and the
  // byte count the consumer actually observed.
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
  request.manifest_digest = grant.manifest_digest;
  request.observed_partition_digest = grant.manifest_digest;
  request.bytes = grant.total_bytes;
  request.integrity_verified = true;
  for (const ChunkDescriptor& chunk : fetched.value().chunks) {
    request.observed_chunk_digests.push_back(chunk.digest);
  }

  const auto committed = consumer.commit_transfer(request);
  REQUIRE_OK(committed);
  REQUIRE(committed.value().receipt.newly_committed);
  REQUIRE(committed.value().receipt.edge_newly_completed);
  REQUIRE_FALSE(committed.value().receipt.duplicate);
  REQUIRE_EQ(committed.value().receipt.sequence, CommitSequence{1});
  REQUIRE_EQ(committed.value().receipt.manifest_digest, acceptance.value().manifest_digest);
  REQUIRE_EQ(committed.value().receipt.accounted_bytes, 512u);
  REQUIRE_FALSE(committed.value().shuffle_completed);

  const auto progress = consumer.progress();
  REQUIRE_OK(progress);
  REQUIRE_EQ(progress.value().shuffle, shuffle);
  REQUIRE_EQ(progress.value().shuffle_generation, generation);
  REQUIRE_EQ(progress.value().partitions_total, 2u);
  REQUIRE_EQ(progress.value().partitions_committed, 1u);
  REQUIRE_EQ(progress.value().partitions_failed, 0u);
  REQUIRE_EQ(progress.value().partitions_incomplete, 1u);
  REQUIRE_EQ(progress.value().edges_required, 2u);
  REQUIRE_EQ(progress.value().edges_completed, 1u);
  REQUIRE_EQ(progress.value().edges_failed, 0u);
  REQUIRE_EQ(progress.value().edges_incomplete, 1u);
  REQUIRE_EQ(progress.value().bytes_committed, 512u);
  REQUIRE_EQ(progress.value().bytes_attempted, 512u);
  REQUIRE_EQ(progress.value().duplicate_commits_suppressed, 0u);
  REQUIRE_EQ(progress.value().tracked_partitions, 1u);
  REQUIRE_EQ(progress.value().tracked_edges, 1u);
  REQUIRE(progress.value().accounting_closes());

  const auto status = producer.status();
  REQUIRE_OK(status);
  REQUIRE_EQ(status.value().state, ShuffleState::Open);
  REQUIRE_EQ(status.value().reason, ErrorCode::Ok);
  REQUIRE_EQ(status.value().shuffle, shuffle);
  REQUIRE_EQ(status.value().shuffle_generation, generation);
  REQUIRE_EQ(status.value().policy_generation, PolicyGeneration{1});
  REQUIRE_EQ(status.value().epoch, 1u);
  REQUIRE_EQ(status.value().active_producers, 1u);
  REQUIRE_EQ(status.value().active_consumers, 1u);
  REQUIRE_EQ(status.value().in_flight, 0u);
  REQUIRE_FALSE(status.value().durable);
  REQUIRE_FALSE(status.value().revalidation_required);
  REQUIRE_FALSE(status.value().history_incomplete);
  REQUIRE_EQ(status.value().progress.edges_completed, 1u);
  REQUIRE_EQ(status.value().progress.partitions_committed, 1u);

  const auto explanation = consumer.explain(4);
  REQUIRE_OK(explanation);
  bool missing_manifest_reported = false;
  bool durability_reported = false;
  for (const ExplainEntry& entry : explanation.value().entries) {
    if (entry.code == ErrorCode::PartitionNotProduced) {
      missing_manifest_reported = true;
      REQUIRE_EQ(entry.count, 1u);
      REQUIRE_EQ(entry.subject, std::string{"partition 1"});
    }
    if (entry.code == ErrorCode::CapabilityUnsupported) {
      durability_reported = true;
      REQUIRE_EQ(entry.count, 1u);
    }
  }
  REQUIRE(missing_manifest_reported);
  REQUIRE(durability_reported);

  REQUIRE_EQ(server.stats().accepted_sessions, 2u);
  REQUIRE_EQ(server.stats().served_requests, 13u);
  REQUIRE_EQ(server.stats().refused_requests, 0u);
  REQUIRE_EQ(server.stats().duplicate_requests, 0u);
  REQUIRE_EQ(server.stats().replayed_answers, 0u);
  REQUIRE_EQ(server.stats().refused_sessions, 0u);

  REQUIRE_OK(consumer.close());
  REQUIRE_OK(producer.close());
  REQUIRE_OK(server.stop());
  REQUIRE_EQ(server.stats().active_sessions, 0u);
}

// ===========================================================================
// 3. handshake enforcement and identity binding
// ===========================================================================

SHUFFLE_TEST(service_loopback, handshake_is_required_first_and_binds_the_identity) {
  const Limits limits;
  VolatileSink sink;
  Coordinator coordinator{limits, &sink};
  CoordinatorServer server{coordinator, server_options(limits)};
  REQUIRE_OK(server.start());

  // A client opens the shuffle the raw peer registers into: opening authority
  // is a separate step and not part of what this case proves.
  CoordinatorClient opener;
  REQUIRE_OK(opener.connect(client_options(server.port(), ParticipantKind::Producer, 6, 66, 0x6060)));
  REQUIRE_OK(opener.open_shuffle(open_request(ShuffleId{5}, ShuffleGeneration{1}, 1, limits)));

  RawPeer peer{limits};
  peer.connect(server.port());

  // A first message that is not a handshake is refused, and the connection is
  // left open so its peer can recover by handshaking.
  const std::vector<std::byte> early =
      raw_frame(MessageType::ProgressRequest, SessionId{}, peer.sequence, 0, std::span<const std::byte>{});
  const RawReply early_reply = round_trip(peer, early);
  REQUIRE_EQ(early_reply.frame.value().header.type, MessageType::ErrorResponse);
  REQUIRE(early_reply.frame.value().header.carries_response_flag());
  REQUIRE_EQ(early_reply.frame.value().header.sequence, 1u);
  REQUIRE_EQ(early_reply.frame.value().header.session, SessionId{});
  REQUIRE_EQ(parse_envelope(early_reply.frame.value().payload).code, ErrorCode::HandshakeRequired);
  ++peer.sequence;

  // A malformed handshake payload is answered with the decoder's code and the
  // connection stays usable.
  const std::vector<std::byte> truncated = handshake_payload(ParticipantKind::Producer, 7, 11, 0x5150);
  const std::vector<std::byte> short_frame = raw_frame(MessageType::HandshakeRequest, SessionId{}, peer.sequence, 0,
                                                       std::span<const std::byte>(truncated).first(9));
  const RawReply short_reply = round_trip(peer, short_frame);
  REQUIRE_EQ(short_reply.frame.value().header.type, MessageType::ErrorResponse);
  REQUIRE_EQ(parse_envelope(short_reply.frame.value().payload).code, ErrorCode::TruncatedInput);
  ++peer.sequence;

  // The real handshake binds the identity and assigns the session id.
  const SessionId assigned = raw_handshake(peer, ParticipantKind::Producer, 7, 11, 0x5150);
  REQUIRE(assigned.value() != 0);

  // A frame carrying another session id is refused as an identity mismatch.
  const std::vector<std::byte> foreign =
      raw_frame(MessageType::StatusRequest, SessionId{assigned.value() + 1}, peer.sequence, 0,
                std::span<const std::byte>{});
  const RawReply foreign_reply = round_trip(peer, foreign);
  REQUIRE_EQ(foreign_reply.frame.value().header.type, MessageType::ErrorResponse);
  REQUIRE_EQ(foreign_reply.frame.value().header.session, assigned);
  REQUIRE_EQ(foreign_reply.frame.value().header.sequence, peer.sequence);
  REQUIRE_EQ(parse_envelope(foreign_reply.frame.value().payload).code, ErrorCode::IdentityMismatch);
  ++peer.sequence;

  // A handshake repeated on a bound connection is a protocol violation.
  const std::vector<std::byte> repeated =
      raw_frame(MessageType::HandshakeRequest, assigned, peer.sequence, 0,
                handshake_payload(ParticipantKind::Producer, 7, 11, 0x5150));
  const RawReply repeated_reply = round_trip(peer, repeated);
  REQUIRE_EQ(repeated_reply.frame.value().header.type, MessageType::ErrorResponse);
  REQUIRE_EQ(parse_envelope(repeated_reply.frame.value().payload).code, ErrorCode::ProtocolViolation);
  ++peer.sequence;

  // A request payload that contradicts the envelope is refused: another id,
  // and the same id under another incarnation.
  const std::vector<std::byte> other_id = register_payload(ParticipantKind::Producer, 8, 11, "127.0.0.1:43001",
                                                           SelectionKind::All, 0, 0, {});
  const std::vector<std::byte> other_id_request =
      raw_frame(MessageType::RegisterParticipantRequest, assigned, peer.sequence, 0, other_id);
  const RawReply other_id_reply = round_trip(peer, other_id_request);
  REQUIRE_EQ(other_id_reply.frame.value().header.type, MessageType::ErrorResponse);
  REQUIRE_EQ(parse_envelope(other_id_reply.frame.value().payload).code, ErrorCode::IdentityMismatch);
  ++peer.sequence;

  const std::vector<std::byte> other_incarnation = register_payload(ParticipantKind::Producer, 7, 99, "127.0.0.1:43001",
                                                                   SelectionKind::All, 0, 0, {});
  const std::vector<std::byte> other_incarnation_request =
      raw_frame(MessageType::RegisterParticipantRequest, assigned, peer.sequence, 0, other_incarnation);
  const RawReply other_incarnation_reply = round_trip(peer, other_incarnation_request);
  REQUIRE_EQ(other_incarnation_reply.frame.value().header.type, MessageType::ErrorResponse);
  REQUIRE_EQ(parse_envelope(other_incarnation_reply.frame.value().payload).code, ErrorCode::IdentityMismatch);
  ++peer.sequence;

  // The connection is still usable: the matching registration is served, and
  // its reply body carries the documented {u64 topology_generation, u64
  // incarnation, u8 superseded_previous}.
  const std::vector<std::byte> matching = register_payload(ParticipantKind::Producer, 7, 11, "127.0.0.1:43001",
                                                          SelectionKind::All, 0, 0, {});
  const std::vector<std::byte> matching_request =
      raw_frame(MessageType::RegisterParticipantRequest, assigned, peer.sequence, 0, matching);
  const RawReply matching_reply = round_trip(peer, matching_request);
  REQUIRE_EQ(matching_reply.frame.value().header.type, MessageType::RegisterParticipantResponse);
  REQUIRE(matching_reply.frame.value().header.carries_response_flag());
  REQUIRE_EQ(matching_reply.frame.value().header.session, assigned);
  REQUIRE_EQ(matching_reply.frame.value().header.sequence, peer.sequence);
  const std::span<const std::byte> body = reply_body(matching_reply.frame.value().payload);
  REQUIRE_EQ(parse_envelope(matching_reply.frame.value().payload).code, ErrorCode::Ok);
  REQUIRE_EQ(body.size(), 17u);
  REQUIRE_EQ(u64_at(body, 0), 1u);
  REQUIRE_EQ(u64_at(body, 8), 11u);
  REQUIRE_EQ(static_cast<unsigned>(byte_at(body, 16)), 0u);
  ++peer.sequence;

  // A message type this service does not serve is answered, not ignored: the
  // data plane is refused with FrameTypeUnsupported and the session survives.
  const std::vector<std::byte> data_plane =
      raw_frame(MessageType::ChunkFetchRequest, assigned, peer.sequence, 0, std::span<const std::byte>{});
  const RawReply data_plane_reply = round_trip(peer, data_plane);
  REQUIRE_EQ(data_plane_reply.frame.value().header.type, MessageType::ErrorResponse);
  REQUIRE_EQ(parse_envelope(data_plane_reply.frame.value().payload).code, ErrorCode::FrameTypeUnsupported);
  ++peer.sequence;

  const std::vector<std::byte> final_request =
      raw_frame(MessageType::StatusRequest, assigned, peer.sequence, 0, std::span<const std::byte>{});
  const RawReply final_reply = round_trip(peer, final_request);
  REQUIRE_EQ(final_reply.frame.value().header.type, MessageType::StatusResponse);
  REQUIRE_EQ(parse_envelope(final_reply.frame.value().payload).code, ErrorCode::Ok);

  peer.socket.close();
  REQUIRE_EQ(server.stats().refused_requests, 7u);
  REQUIRE_OK(opener.close());
  REQUIRE_OK(server.stop());
}

// ===========================================================================
// 4. duplicate sequence numbers replay the identical answer exactly once
// ===========================================================================

SHUFFLE_TEST(service_loopback, a_duplicate_sequence_replays_the_identical_answer_once) {
  const Limits limits;
  VolatileSink sink;
  Coordinator coordinator{limits, &sink};
  CoordinatorServer server{coordinator, server_options(limits)};
  REQUIRE_OK(server.start());

  const std::uint16_t port = server.port();
  CoordinatorClient admin;
  REQUIRE_OK(admin.connect(client_options(port, ParticipantKind::Producer, 6, 66, 0x6060)));
  const ShuffleId shuffle{21};
  REQUIRE_OK(admin.open_shuffle(open_request(shuffle, ShuffleGeneration{1}, 2, limits)));

  const auto before = admin.status();
  REQUIRE_OK(before);

  RawPeer peer{limits};
  peer.connect(port);
  const SessionId assigned = raw_handshake(peer, ParticipantKind::Producer, 5, 55, 0x5050);
  REQUIRE(assigned.value() != 0);

  const std::vector<std::byte> registration = register_payload(ParticipantKind::Producer, 5, 55, "127.0.0.1:44001",
                                                              SelectionKind::All, 0, 0, {});
  const std::vector<std::byte> request =
      raw_frame(MessageType::RegisterParticipantRequest, assigned, peer.sequence, 0, registration);

  const std::vector<std::byte> first = send_and_receive(peer, request);
  const auto first_frame = decode_frame(first, limits);
  REQUIRE_OK(first_frame);
  REQUIRE_EQ(first_frame.value().header.type, MessageType::RegisterParticipantResponse);
  REQUIRE_EQ(parse_envelope(first_frame.value().payload).code, ErrorCode::Ok);

  // The identical request frame again: the answer must be byte-identical and
  // the request must not be applied twice.
  const std::vector<std::byte> second = send_and_receive(peer, request);
  REQUIRE(same_bytes(first, second));

  const auto after = admin.status();
  REQUIRE_OK(after);
  REQUIRE_EQ(after.value().persisted_records, before.value().persisted_records + 1u);
  REQUIRE_EQ(after.value().active_producers, 1u);
  REQUIRE_EQ(server.stats().duplicate_requests, 1u);
  REQUIRE_EQ(server.stats().replayed_answers, 1u);
  REQUIRE_EQ(server.stats().accepted_sessions, 2u);

  // A fresh sequence on the same connection is still served.
  ++peer.sequence;
  const std::vector<std::byte> fresh_request =
      raw_frame(MessageType::StatusRequest, assigned, peer.sequence, 0, std::span<const std::byte>{});
  const RawReply fresh = round_trip(peer, fresh_request);
  REQUIRE_EQ(fresh.frame.value().header.type, MessageType::StatusResponse);
  REQUIRE_EQ(parse_envelope(fresh.frame.value().payload).code, ErrorCode::Ok);

  peer.socket.close();
  REQUIRE_OK(admin.close());
  REQUIRE_OK(server.stop());
}

// ===========================================================================
// 5. coordinator refusals propagate through the client
// ===========================================================================

SHUFFLE_TEST(service_loopback, coordinator_refusals_propagate_through_the_client) {
  const Limits limits;
  VolatileSink sink;
  Coordinator coordinator{limits, &sink};
  CoordinatorServer server{coordinator, server_options(limits)};
  REQUIRE_OK(server.start());

  const std::uint16_t port = server.port();
  CoordinatorClient producer_one;
  CoordinatorClient producer_two;
  CoordinatorClient consumer;
  REQUIRE_OK(producer_one.connect(client_options(port, ParticipantKind::Producer, 1, 11, 0x11)));
  REQUIRE_OK(producer_two.connect(client_options(port, ParticipantKind::Producer, 2, 12, 0x22)));
  REQUIRE_OK(consumer.connect(client_options(port, ParticipantKind::Consumer, 1, 21, 0x33)));

  const ShuffleId shuffle{9};
  const ShuffleGeneration generation{2};
  REQUIRE_OK(producer_one.open_shuffle(open_request(shuffle, generation, 2, limits)));
  REQUIRE_OK(producer_one.register_participant(ParticipantKind::Producer, 1, IncarnationId{11}, "127.0.0.1:45001",
                                               PartitionSelection::all()));
  REQUIRE_OK(producer_two.register_participant(ParticipantKind::Producer, 2, IncarnationId{12}, "127.0.0.1:45002",
                                               PartitionSelection::all()));
  REQUIRE_OK(consumer.register_participant(ParticipantKind::Consumer, 1, IncarnationId{21}, "127.0.0.1:45003",
                                           PartitionSelection::all()));

  // An attempt this coordinator epoch never issued cannot be completed.
  CommitRequest ghost;
  ghost.edge = EdgeKey{PartitionId{0}, PartitionGeneration{1}, ConsumerId{1}};
  ghost.attempt = TransferAttemptId{999};
  ghost.shuffle_generation = generation;
  ghost.topology_generation = TopologyGeneration{1};
  ghost.policy_generation = PolicyGeneration{1};
  ghost.producer = ProducerId{1};
  ghost.producer_incarnation = IncarnationId{11};
  ghost.consumer = ConsumerId{1};
  ghost.consumer_incarnation = IncarnationId{21};
  ghost.manifest_digest = sha256(std::string_view{"ghost"});
  ghost.observed_partition_digest = ghost.manifest_digest;
  ghost.bytes = 1;
  ghost.integrity_verified = true;
  REQUIRE_ERROR(consumer.commit_transfer(ghost), ErrorCode::StaleAttempt);
  REQUIRE_ERROR(consumer.report_failure(TransferAttemptId{404}, ErrorCode::InternalError), ErrorCode::StaleAttempt);

  // Partition 1 belongs to the second active producer in identifier order, so
  // the first producer's manifest for it is refused as not owned.
  const std::vector<std::byte> payload =
      synthetic_partition_payload(shuffle, generation, PartitionId{1}, PartitionGeneration{1}, 128);
  const auto intruding =
      build_manifest(shuffle, generation, PartitionId{1}, PartitionGeneration{1}, ProducerId{1}, IncarnationId{11},
                     TopologyGeneration{1}, payload, 128, limits);
  REQUIRE_OK(intruding);
  REQUIRE_ERROR(producer_one.publish_manifest(intruding.value()), ErrorCode::PartitionNotOwned);

  // The owner's manifest for the same content is accepted, so the refusal was
  // about ownership and not about the manifest itself.
  const auto owned =
      build_manifest(shuffle, generation, PartitionId{1}, PartitionGeneration{1}, ProducerId{2}, IncarnationId{12},
                     TopologyGeneration{1}, payload, 128, limits);
  REQUIRE_OK(owned);
  const auto acceptance = producer_two.publish_manifest(owned.value());
  REQUIRE_OK(acceptance);
  REQUIRE_EQ(acceptance.value().partition_generation, PartitionGeneration{1});
  const auto accepted_partition_one = consumer.manifest(PartitionId{1});
  REQUIRE_OK(accepted_partition_one);
  REQUIRE_EQ(acceptance.value().manifest_digest, compute_manifest_digest(accepted_partition_one.value()));
  REQUIRE_EQ(accepted_partition_one.value().producer, ProducerId{2});
  REQUIRE_EQ(accepted_partition_one.value().producer_incarnation, IncarnationId{12});

  // Asking for a partition nobody produced is refused explicitly.
  REQUIRE_ERROR(consumer.manifest(PartitionId{0}), ErrorCode::PartitionNotProduced);

  // An identity contradiction is refused by the service, so the coordinator is
  // never asked to believe it.
  REQUIRE_ERROR(consumer.register_participant(ParticipantKind::Producer, 1, IncarnationId{11}, "127.0.0.1:45004",
                                              PartitionSelection::all()),
                ErrorCode::IdentityMismatch);
  REQUIRE_ERROR(producer_one.register_participant(ParticipantKind::Producer, 2, IncarnationId{12}, "127.0.0.1:45005",
                                                  PartitionSelection::all()),
                ErrorCode::IdentityMismatch);

  // The connection survives every refusal above.
  const auto progress = consumer.progress();
  REQUIRE_OK(progress);
  REQUIRE_EQ(progress.value().partitions_total, 2u);
  REQUIRE_EQ(progress.value().edges_required, 2u);

  REQUIRE_OK(server.stop());
}

// ===========================================================================
// 6. stop() with a connected client
// ===========================================================================

SHUFFLE_TEST(service_loopback, stopping_the_server_fails_a_connected_client_deterministically) {
  const Limits limits;
  VolatileSink sink;
  Coordinator coordinator{limits, &sink};
  CoordinatorServer server{coordinator, server_options(limits)};
  REQUIRE_OK(server.start());

  CoordinatorClient client;
  REQUIRE_OK(client.connect(client_options(server.port(), ParticipantKind::Consumer, 1, 31, 0x77)));
  REQUIRE_OK(client.status());
  REQUIRE_EQ(server.stats().active_sessions, 1u);

  REQUIRE_OK(server.stop());
  REQUIRE_FALSE(server.running());
  REQUIRE_EQ(server.stats().active_sessions, 0u);

  const auto after = client.status();
  REQUIRE_FALSE(after.ok());
  const ErrorCode code = after.code();
  REQUIRE(code == ErrorCode::ConnectionClosed || code == ErrorCode::SessionClosed ||
          code == ErrorCode::ConnectionFailure || code == ErrorCode::PeerUnavailable);
  REQUIRE_FALSE(client.connected());

  // close() is idempotent and must not fail on a connection that already went
  // away, and a second call is still a no-op.
  REQUIRE_OK(client.close());
  REQUIRE_OK(client.close());
  REQUIRE_FALSE(client.connected());
  REQUIRE_ERROR(client.progress(), ErrorCode::SessionClosed);
}

// ===========================================================================
// 7. two servers on one machine
// ===========================================================================

SHUFFLE_TEST(service_loopback, two_servers_route_requests_to_the_right_coordinator) {
  const Limits limits;
  VolatileSink sink_a;
  VolatileSink sink_b;
  Coordinator coordinator_a{limits, &sink_a};
  Coordinator coordinator_b{limits, &sink_b};
  CoordinatorServer server_a{coordinator_a, server_options(limits)};
  CoordinatorServer server_b{coordinator_b, server_options(limits)};
  REQUIRE_OK(server_a.start());
  REQUIRE_OK(server_b.start());
  REQUIRE_NE(server_a.port(), server_b.port());

  CoordinatorClient producer_a;
  CoordinatorClient consumer_a;
  CoordinatorClient producer_b;
  REQUIRE_OK(producer_a.connect(client_options(server_a.port(), ParticipantKind::Producer, 1, 11, 0xa1)));
  REQUIRE_OK(consumer_a.connect(client_options(server_a.port(), ParticipantKind::Consumer, 1, 21, 0xa2)));
  REQUIRE_OK(producer_b.connect(client_options(server_b.port(), ParticipantKind::Producer, 2, 12, 0xb1)));

  const ShuffleId shuffle_a{11};
  const ShuffleId shuffle_b{22};
  REQUIRE_OK(producer_a.open_shuffle(open_request(shuffle_a, ShuffleGeneration{1}, 2, limits)));
  REQUIRE_OK(producer_b.open_shuffle(open_request(shuffle_b, ShuffleGeneration{4}, 5, limits)));
  REQUIRE_OK(producer_a.register_participant(ParticipantKind::Producer, 1, IncarnationId{11}, "127.0.0.1:46001",
                                             PartitionSelection::all()));
  REQUIRE_OK(consumer_a.register_participant(ParticipantKind::Consumer, 1, IncarnationId{21}, "127.0.0.1:46002",
                                             PartitionSelection::all()));

  const std::vector<std::byte> payload =
      synthetic_partition_payload(shuffle_a, ShuffleGeneration{1}, PartitionId{0}, PartitionGeneration{1}, 64);
  const auto manifest =
      build_manifest(shuffle_a, ShuffleGeneration{1}, PartitionId{0}, PartitionGeneration{1}, ProducerId{1},
                     IncarnationId{11}, TopologyGeneration{1}, payload, 64, limits);
  REQUIRE_OK(manifest);
  REQUIRE_OK(producer_a.publish_manifest(manifest.value()));

  // Each coordinator answers about itself.
  const auto status_a = consumer_a.status();
  REQUIRE_OK(status_a);
  REQUIRE_EQ(status_a.value().shuffle, shuffle_a);
  REQUIRE_EQ(status_a.value().shuffle_generation, ShuffleGeneration{1});
  REQUIRE_EQ(status_a.value().active_producers, 1u);
  REQUIRE_EQ(status_a.value().active_consumers, 1u);
  REQUIRE_EQ(status_a.value().progress.partitions_total, 2u);

  const auto status_b = producer_b.status();
  REQUIRE_OK(status_b);
  REQUIRE_EQ(status_b.value().shuffle, shuffle_b);
  REQUIRE_EQ(status_b.value().shuffle_generation, ShuffleGeneration{4});
  REQUIRE_EQ(status_b.value().active_producers, 0u);
  REQUIRE_EQ(status_b.value().progress.partitions_total, 5u);

  const auto progress_b = producer_b.progress();
  REQUIRE_OK(progress_b);
  REQUIRE_EQ(progress_b.value().shuffle, shuffle_b);
  REQUIRE_EQ(progress_b.value().edges_required, 0u);

  // The manifest published on A is invisible on B.
  const auto manifest_a = consumer_a.manifest(PartitionId{0});
  REQUIRE_OK(manifest_a);
  REQUIRE_EQ(manifest_a.value().total_bytes, 64u);
  REQUIRE_ERROR(producer_b.manifest(PartitionId{0}), ErrorCode::PartitionNotProduced);

  REQUIRE_EQ(server_a.stats().accepted_sessions, 2u);
  REQUIRE_EQ(server_b.stats().accepted_sessions, 1u);
  REQUIRE_EQ(server_a.stats().served_requests, 8u);
  REQUIRE_EQ(server_b.stats().served_requests, 5u);

  REQUIRE_OK(server_a.stop());
  REQUIRE_OK(server_b.stop());
  REQUIRE_FALSE(server_a.running());
  REQUIRE_FALSE(server_b.running());
}

// ===========================================================================
// 8. the session bound refuses beyond max_sessions and counts the refusal
// ===========================================================================

SHUFFLE_TEST(service_loopback, the_session_bound_refuses_beyond_max_sessions) {
  const Limits limits;
  VolatileSink sink;
  Coordinator coordinator{limits, &sink};
  CoordinatorServer server{coordinator, server_options(limits, 1, 1)};
  REQUIRE_OK(server.start());

  CoordinatorClient live;
  REQUIRE_OK(live.connect(client_options(server.port(), ParticipantKind::Producer, 1, 11, 0x11)));
  REQUIRE_OK(live.status());
  REQUIRE_EQ(server.stats().active_sessions, 1u);

  // The bound is reached: the next connection is refused with QueueFull, as a
  // protocol answer rather than a silent close, and the refusal is counted.
  CoordinatorClient refused;
  REQUIRE_ERROR(refused.connect(client_options(server.port(), ParticipantKind::Producer, 2, 12, 0x22)),
                ErrorCode::QueueFull);
  REQUIRE_FALSE(refused.connected());
  REQUIRE_EQ(server.stats().refused_sessions, 1u);
  REQUIRE_EQ(server.stats().active_sessions, 1u);
  REQUIRE_EQ(server.stats().peak_sessions, 1u);
  REQUIRE_EQ(server.stats().accepted_sessions, 1u);

  // The live session is untouched by the refusal.
  REQUIRE_OK(live.status());
  REQUIRE_OK(server.stop());
  REQUIRE_EQ(server.stats().active_sessions, 0u);

  // With the bound released the same server accepts a fresh session again.
  REQUIRE_OK(server.start());
  CoordinatorClient second_wave;
  REQUIRE_OK(second_wave.connect(client_options(server.port(), ParticipantKind::Consumer, 3, 33, 0x33)));
  REQUIRE_OK(second_wave.status());
  REQUIRE_EQ(server.stats().refused_sessions, 0u);  // start() begins a fresh accounting
  REQUIRE_EQ(server.stats().accepted_sessions, 1u);
  REQUIRE_OK(second_wave.close());
  REQUIRE_OK(server.stop());
}

// ===========================================================================
// 9. the service over a durable coordinator: records reach the journal
// ===========================================================================

SHUFFLE_TEST(service_loopback, a_durable_coordinator_persists_through_the_service) {
  // The production wiring: every durable record the coordinator commits becomes
  // a journal record inside this test's scratch directory.
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

  const Limits limits;
  shuffle::test::TempDir directory{"service-loopback-durable"};
  StoreConfig config;
  config.directory = directory.path();
  config.limits = limits;

  DurableStore store{config};
  REQUIRE_OK(store.open());
  JournalSink sink{store};
  Coordinator coordinator{limits, &sink};
  CoordinatorServer server{coordinator, server_options(limits)};
  REQUIRE_OK(server.start());

  CoordinatorClient producer;
  REQUIRE_OK(producer.connect(client_options(server.port(), ParticipantKind::Producer, 1, 11, 0x11)));

  const auto before = producer.status();
  REQUIRE_OK(before);
  REQUIRE(before.value().durable);  // the sink claims durability, and the service reports it
  REQUIRE_EQ(before.value().state, ShuffleState::Closed);
  REQUIRE_EQ(before.value().epoch, 0u);
  REQUIRE_EQ(before.value().persisted_records, 0u);
  REQUIRE_EQ(before.value().progress.partitions_total, 0u);
  REQUIRE_EQ(store.journal_bytes(), 0u);

  const ShuffleId shuffle{31};
  REQUIRE_OK(producer.open_shuffle(open_request(shuffle, ShuffleGeneration{1}, 1, limits)));
  const auto opened = producer.status();
  REQUIRE_OK(opened);
  REQUIRE_EQ(opened.value().state, ShuffleState::Open);
  REQUIRE_EQ(opened.value().epoch, 1u);
  REQUIRE_EQ(opened.value().persisted_records, 2u);  // the epoch record and the shuffle state
  REQUIRE(store.journal_bytes() > 0u);               // and both are bytes on disk
  REQUIRE(std::filesystem::exists(config.directory / config.journal_name));

  REQUIRE_OK(producer.register_participant(ParticipantKind::Producer, 1, IncarnationId{11}, "127.0.0.1:47001",
                                           PartitionSelection::all()));
  const auto registered = producer.status();
  REQUIRE_OK(registered);
  REQUIRE_EQ(registered.value().persisted_records, 3u);
  REQUIRE_EQ(registered.value().active_producers, 1u);

  const std::uint64_t bytes_before_publish = store.journal_bytes();
  const std::vector<std::byte> payload =
      synthetic_partition_payload(shuffle, ShuffleGeneration{1}, PartitionId{0}, PartitionGeneration{1}, 96);
  const auto built = build_manifest(shuffle, ShuffleGeneration{1}, PartitionId{0}, PartitionGeneration{1},
                                    ProducerId{1}, IncarnationId{11}, TopologyGeneration{1}, payload, 32, limits);
  REQUIRE_OK(built);
  REQUIRE_EQ(built.value().chunks.size(), 3u);
  const auto acceptance = producer.publish_manifest(built.value());
  REQUIRE_OK(acceptance);
  REQUIRE(store.journal_bytes() > bytes_before_publish);

  const auto fetched = producer.manifest(PartitionId{0});
  REQUIRE_OK(fetched);
  REQUIRE_EQ(fetched.value().total_bytes, 96u);
  REQUIRE_EQ(fetched.value().chunks.size(), 3u);
  REQUIRE_EQ(fetched.value().producer, ProducerId{1});
  REQUIRE_EQ(producer.status().value().persisted_records, 4u);

  REQUIRE_OK(producer.close());
  REQUIRE_OK(server.stop());
  REQUIRE_OK(store.open());  // the journal is readable and the records are still there
  REQUIRE_EQ(store.recovery().tail, JournalTailStatus::Clean);
}

}  // namespace
