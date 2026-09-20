// shuffle-fabric-node: one real Shuffle Fabric participant process.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// The node is a participant, not an authority. It keeps no completion state of
// its own: it asks the coordinator for a wave, moves real payload bytes over
// loopback TCP, verifies every chunk against the manifest the coordinator
// accepted, and reports either the evidence or the deterministic ErrorCode
// that says why it could not.
//
// Management plane: CoordinatorClient over TCP (handshake, register, publish,
// next_wave, manifest, commit_transfer, report_failure). The coordinator stays
// the only place where authority is granted or accounted for; this process
// never decides that an edge is complete.
//
// Data plane: ChunkFetchRequest/ChunkFetchResponse frames over a real TCP
// listener the producer owns. Chunks are exchanged participant-to-participant;
// the coordinator never carries bulk data. A producer answers a repeated
// request sequence by re-sending the previous answer byte for byte, which is
// the replay path --fault-duplicate-frame proves.
//
// Everything is bounded: a fixed number of publication passes, a bounded wave
// loop, a bounded wait per socket operation. No decision depends on how long an
// operation took and no line carries a timestamp.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#endif

#include "shuffle/fabric/codec.hpp"
#include "shuffle/fabric/coordinator.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/frame.hpp"
#include "shuffle/fabric/hash.hpp"
#include "shuffle/fabric/identity.hpp"
#include "shuffle/fabric/manifest.hpp"
#include "shuffle/fabric/service.hpp"
#include "shuffle/fabric/socket.hpp"
#include "shuffle/fabric/topology.hpp"

namespace {

using namespace shuffle::fabric;

// Transport budgets. A budget is responsiveness, never authority: expiry is
// reported as a transport failure and never as a completion or a refusal.
constexpr int kConnectTimeoutMs = 2000;
constexpr int kAcceptBudgetMs = 200;
constexpr int kReadBudgetMs = 500;
constexpr std::size_t kReceiveBytes = 64 * 1024;

// How many empty bounded reads a producer tolerates on one connection before
// re-checking the run's state. Four read budgets, never a wall-clock decision.
constexpr std::uint64_t kIdleReadsBeforeStateCheck = 4;

// Bounds on the node's own loops. Both are deliberately small: a participant
// that cannot settle its work in this many steps reports that fact instead of
// spinning.
constexpr std::uint32_t kMaxPublishPasses = 8;
constexpr std::uint64_t kMaxWaves = 100000;
constexpr std::uint32_t kMaxSettleChecks = 8;

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

struct Options {
  Limits limits{};
  std::string coordinator_host{"127.0.0.1"};
  std::uint16_t coordinator_port{0};
  std::string role{"both"};
  std::uint64_t id{0};
  std::uint64_t incarnation{0};  // 0 asks for a fresh one derived from boot entropy
  PartitionSelection selection{};
  bool selection_given{false};
  std::uint64_t payload_bytes{256};
  std::uint32_t chunk_bytes{64};
  std::uint16_t listen_port{0};
  std::string data_host{"127.0.0.1"};
  std::uint32_t max_idle_waves{3};
  bool has_corrupt_chunk{false};
  std::uint64_t corrupt_chunk{0};
  bool has_truncate_chunk{false};
  std::uint64_t truncate_chunk{0};
  bool fault_duplicate_frame{false};
  bool fault_stale_incarnation{false};
  ShuffleId shuffle{1};
  ShuffleGeneration generation{1};
  bool print_result{false};
};

[[nodiscard]] void print_usage() {
  std::printf(
      "shuffle-fabric-node: one real participant process\n"
      "usage: shuffle-fabric-node --coordinator HOST:PORT --id N [options]\n"
      "  --coordinator HOST:PORT  coordinator service to join (required)\n"
      "  --role producer|consumer|both  which side of the fabric to play (default both)\n"
      "  --id N                   participant identifier (required, non-zero)\n"
      "  --incarnation N          incarnation to claim; 0 derives a fresh one (default 0)\n"
      "  --partitions SELECTION   all | range:A:B | list:a,b,c (default all)\n"
      "                           producers publish the selected partitions, consumers require them\n"
      "  --payload-bytes N        synthetic payload size per partition (default 256)\n"
      "  --chunk-bytes N          maximum chunk size (default 64)\n"
      "  --listen-port N          data-plane listen port; 0 selects a free one (default 0)\n"
      "  --data-host HOST         data-plane bind/advertised host (default 127.0.0.1)\n"
      "  --max-idle-waves K       consumer stops after K idle waves (default 3)\n"
      "  --fault-corrupt-chunk K  serve chunk K with corrupted payload bytes\n"
      "  --fault-truncate-chunk K serve chunk K as a truncated frame, then close\n"
      "  --fault-duplicate-frame  send one request frame twice and require an identical answer\n"
      "  --fault-stale-incarnation re-register mid-run and require a superseded completion to be refused\n"
      "  --shuffle ID             shuffle identity to join (default 1)\n"
      "  --generation N           shuffle generation to join (default 1)\n"
      "  --print-result           print one extra RESULT line naming the terminal reason\n"
      "  --help                   print this help and exit\n");
  std::fflush(stdout);
}

[[nodiscard]] bool parse_u64(std::string_view text, std::uint64_t& out) {
  if (text.empty()) {
    return false;
  }
  std::uint64_t value = 0;
  for (const char character : text) {
    if (character < '0' || character > '9') {
      return false;
    }
    const std::uint64_t digit = static_cast<std::uint64_t>(character - '0');
    if (value > (UINT64_MAX - digit) / 10) {
      return false;
    }
    value = value * 10 + digit;
  }
  out = value;
  return true;
}

[[nodiscard]] bool parse_selection(std::string_view text, PartitionSelection& selection, std::string& error) {
  if (text == "all") {
    selection = PartitionSelection::all();
    return true;
  }
  if (text.rfind("range:", 0) == 0) {
    const std::string_view rest = text.substr(6);
    const std::size_t colon = rest.find(':');
    if (colon == std::string_view::npos) {
      error = "range selection needs range:A:B";
      return false;
    }
    std::uint64_t begin = 0;
    std::uint64_t end = 0;
    if (!parse_u64(rest.substr(0, colon), begin) || !parse_u64(rest.substr(colon + 1), end)) {
      error = "range selection needs two decimal bounds";
      return false;
    }
    selection = PartitionSelection::range(PartitionId{begin}, PartitionId{end});
    return true;
  }
  if (text.rfind("list:", 0) == 0) {
    const std::string_view rest = text.substr(5);
    std::vector<PartitionId> items;
    std::size_t cursor = 0;
    while (cursor < rest.size()) {
      const std::size_t comma = rest.find(',', cursor);
      const std::string_view token = rest.substr(cursor, comma == std::string_view::npos ? std::string_view::npos
                                                                                        : comma - cursor);
      std::uint64_t value = 0;
      if (!parse_u64(token, value)) {
        error = "list selection needs decimal identifiers separated by commas";
        return false;
      }
      items.push_back(PartitionId{value});
      if (comma == std::string_view::npos) {
        break;
      }
      cursor = comma + 1;
    }
    const auto built = PartitionSelection::from_list(std::move(items), Limits{});
    if (!built.ok()) {
      error = format_error(built.error());
      return false;
    }
    selection = built.value();
    return true;
  }
  error = "selection must be all, range:A:B or list:a,b,c";
  return false;
}

[[nodiscard]] bool parse_host_port(std::string_view text, std::string& host, std::uint16_t& port) {
  const std::size_t colon = text.rfind(':');
  if (colon == std::string_view::npos || colon == 0 || colon + 1 >= text.size()) {
    return false;
  }
  std::uint64_t value = 0;
  if (!parse_u64(text.substr(colon + 1), value) || value == 0 || value > 65535) {
    return false;
  }
  host.assign(text.substr(0, colon));
  port = static_cast<std::uint16_t>(value);
  return true;
}

[[nodiscard]] bool parse_options(int argc, char** argv, Options& options) {
  for (int index = 1; index < argc; ++index) {
    const std::string_view flag = argv[index];
    const auto next = [&](std::string_view& value) {
      if (index + 1 >= argc) {
        std::fprintf(stderr, "missing value for %.*s\n", static_cast<int>(flag.size()), flag.data());
        return false;
      }
      value = argv[++index];
      return true;
    };
    std::string_view value;

    if (flag == "--help") {
      print_usage();
      std::exit(0);
    }
    if (flag == "--fault-duplicate-frame") {
      options.fault_duplicate_frame = true;
      continue;
    }
    if (flag == "--fault-stale-incarnation") {
      options.fault_stale_incarnation = true;
      continue;
    }
    if (flag == "--print-result") {
      options.print_result = true;
      continue;
    }

    if (flag == "--coordinator") {
      if (!next(value)) {
        return false;
      }
      if (!parse_host_port(value, options.coordinator_host, options.coordinator_port)) {
        std::fprintf(stderr, "--coordinator needs HOST:PORT\n");
        return false;
      }
      continue;
    }
    if (flag == "--role") {
      if (!next(value)) {
        return false;
      }
      if (value != "producer" && value != "consumer" && value != "both") {
        std::fprintf(stderr, "--role must be producer, consumer or both\n");
        return false;
      }
      options.role.assign(value);
      continue;
    }
    if (flag == "--partitions") {
      if (!next(value)) {
        return false;
      }
      std::string error;
      if (!parse_selection(value, options.selection, error)) {
        std::fprintf(stderr, "--partitions: %s\n", error.c_str());
        return false;
      }
      options.selection_given = true;
      continue;
    }
    if (flag == "--data-host") {
      if (!next(value)) {
        return false;
      }
      options.data_host.assign(value);
      continue;
    }

    std::uint64_t number = 0;
    if (flag == "--id" || flag == "--incarnation" || flag == "--payload-bytes" || flag == "--chunk-bytes" ||
        flag == "--listen-port" || flag == "--max-idle-waves" || flag == "--shuffle" || flag == "--generation" ||
        flag == "--fault-corrupt-chunk" || flag == "--fault-truncate-chunk") {
      if (!next(value)) {
        return false;
      }
      if (!parse_u64(value, number)) {
        std::fprintf(stderr, "invalid numeric value for %.*s\n", static_cast<int>(flag.size()), flag.data());
        return false;
      }
    } else {
      std::fprintf(stderr, "unknown option: %.*s\n", static_cast<int>(flag.size()), flag.data());
      return false;
    }

    if (flag == "--id") {
      options.id = number;
    } else if (flag == "--incarnation") {
      options.incarnation = number;
    } else if (flag == "--payload-bytes") {
      options.payload_bytes = number;
    } else if (flag == "--chunk-bytes") {
      if (number == 0 || number > UINT32_MAX) {
        std::fprintf(stderr, "--chunk-bytes is out of range\n");
        return false;
      }
      options.chunk_bytes = static_cast<std::uint32_t>(number);
    } else if (flag == "--listen-port") {
      if (number > 65535) {
        std::fprintf(stderr, "--listen-port is out of range\n");
        return false;
      }
      options.listen_port = static_cast<std::uint16_t>(number);
    } else if (flag == "--max-idle-waves") {
      if (number > UINT32_MAX) {
        std::fprintf(stderr, "--max-idle-waves is out of range\n");
        return false;
      }
      options.max_idle_waves = static_cast<std::uint32_t>(number);
    } else if (flag == "--shuffle") {
      options.shuffle = ShuffleId{number};
    } else if (flag == "--generation") {
      options.generation = ShuffleGeneration{number};
    } else if (flag == "--fault-corrupt-chunk") {
      options.has_corrupt_chunk = true;
      options.corrupt_chunk = number;
    } else if (flag == "--fault-truncate-chunk") {
      options.has_truncate_chunk = true;
      options.truncate_chunk = number;
    }
  }

  if (options.coordinator_port == 0) {
    std::fprintf(stderr, "--coordinator HOST:PORT is required\n");
    return false;
  }
  if (options.id == 0) {
    std::fprintf(stderr, "--id must be a non-zero participant identifier\n");
    return false;
  }
  if (!options.selection_given) {
    options.selection = PartitionSelection::all();
  }
  return true;
}

// ---------------------------------------------------------------------------
// Boot entropy
// ---------------------------------------------------------------------------

[[nodiscard]] std::uint64_t mix64(std::uint64_t value) noexcept {
  value ^= value >> 33;
  value *= 0xff51afd7ed558ccdull;
  value ^= value >> 33;
  value *= 0xc4ceb9fe1a85ec53ull;
  value ^= value >> 33;
  return value;
}

// A fresh identity value for this boot. It is mixed from the monotonic clock,
// the process identifier and a process-local counter, and is never zero: two
// nodes started in the same tick of the same machine still disagree.
[[nodiscard]] std::uint64_t fresh_identity_value() noexcept {
  static std::atomic<std::uint64_t> counter{0};
  const auto ticks = static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
#if defined(_WIN32)
  const auto process = static_cast<std::uint64_t>(::_getpid());
#else
  const auto process = static_cast<std::uint64_t>(::getpid());
#endif
  const std::uint64_t ordinal = counter.fetch_add(1, std::memory_order_relaxed) + 1;
  const std::uint64_t value = mix64(ticks ^ (process * 0x9e3779b97f4a7c15ull) ^ mix64(ordinal));
  return value == 0 ? 1u : value;
}

// ---------------------------------------------------------------------------
// Deterministic output
// ---------------------------------------------------------------------------

void emit(const std::string& line) {
  std::fputs(line.c_str(), stdout);
  std::fputc('\n', stdout);
  std::fflush(stdout);
}

[[nodiscard]] std::string u64_text(std::uint64_t value) { return std::to_string(value); }

// ---------------------------------------------------------------------------
// Framing helpers
// ---------------------------------------------------------------------------

[[nodiscard]] Status encode_message(MessageType type, SessionId session, std::uint64_t sequence,
                                    std::span<const std::byte> payload, std::vector<std::byte>& out,
                                    const Limits& limits) {
  FrameHeader header;
  header.type = type;
  header.flags = is_response(type) ? kFlagResponse : 0u;
  header.session = session;
  header.sequence = sequence;
  header.payload_length = 0;  // derived from the payload by encode_frame
  header.payload_crc32c = 0;
  return encode_frame(header, payload, out, limits);
}

// Reads exactly one frame with a bounded wait. RetryDeferred means the budget
// expired with a frame still incomplete; ConnectionClosed means the peer closed
// cleanly between frames and TruncatedInput means it closed inside one.
[[nodiscard]] Result<DecodedFrame> read_frame(Socket& socket, FrameStream& stream, std::vector<std::byte>& scratch,
                                              int budget_ms) {
  for (;;) {
    const auto frame = stream.next();
    if (frame.ok()) {
      return frame;
    }
    if (frame.code() != ErrorCode::NoWorkAvailable) {
      return Result<DecodedFrame>{frame.error()};
    }
    const auto received = socket.recv_some(scratch, budget_ms);
    if (received.ok()) {
      const Status fed = stream.feed(std::span<const std::byte>(scratch.data(), received.value()));
      if (!fed.ok()) {
        return Result<DecodedFrame>{fed.error()};
      }
      continue;
    }
    if (received.code() == ErrorCode::RetryDeferred) {
      return make_failure<DecodedFrame>(ErrorCode::RetryDeferred, "no complete frame within the wait budget");
    }
    if (received.code() == ErrorCode::ConnectionClosed || received.code() == ErrorCode::PeerUnavailable) {
      return stream.buffered() == 0 ? make_failure<DecodedFrame>(ErrorCode::ConnectionClosed, "peer closed the stream")
                                    : make_failure<DecodedFrame>(ErrorCode::TruncatedInput,
                                                                 "peer closed inside a frame");
    }
    return make_failure<DecodedFrame>(received.code(), received.detail());
  }
}

// A received frame re-encoded to its exact wire bytes, so two answers can be
// compared as the protocol defines them.
[[nodiscard]] Status raw_frame_bytes(const DecodedFrame& frame, std::vector<std::byte>& out, const Limits& limits) {
  return encode_frame(frame.header, frame.payload, out, limits);
}

// {u64 shuffle, u64 shuffle_generation, u64 partition, u64 partition_generation, u64 chunk}
struct ChunkRequest {
  std::uint64_t shuffle{0};
  std::uint64_t shuffle_generation{0};
  std::uint64_t partition{0};
  std::uint64_t partition_generation{0};
  std::uint64_t chunk{0};
};

[[nodiscard]] Result<ChunkRequest> decode_chunk_request(std::span<const std::byte> payload, const Limits& limits) {
  ByteReader reader{payload, limits, "chunk fetch request"};
  ChunkRequest request;
  request.shuffle = reader.u64();
  request.shuffle_generation = reader.u64();
  request.partition = reader.u64();
  request.partition_generation = reader.u64();
  request.chunk = reader.u64();
  const Status end = reader.require_end();
  if (!end.ok()) {
    return Result<ChunkRequest>{end.error()};
  }
  return request;
}

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

struct PublishedPartition {
  PartitionManifest manifest{};
  std::vector<std::byte> payload{};
};

struct ProducerCounters {
  std::uint64_t published{0};
  std::uint64_t chunks{0};
  std::uint64_t served{0};
  std::uint64_t failed{0};
  std::uint64_t incomplete{0};
  std::uint64_t replayed{0};
  std::uint64_t truncated{0};
  std::uint64_t corrupt{0};
  bool impossible{false};
  std::string terminal{"not_started"};
};

class ProducerSession {
 public:
  ProducerSession(const Options& options, const SocketRuntime& runtime)
      : options_(options), runtime_(runtime), limits_(options.limits) {}

  // Binds the data listener, joins the coordinator, registers and publishes
  // every partition it owns. Returns false when the producer holds no work.
  [[nodiscard]] bool setup(std::uint64_t incarnation, std::uint64_t boot_nonce, std::string& failure) {
    auto bound = TcpListener::bind(options_.data_host, options_.listen_port, 8, runtime_);
    if (!bound.ok()) {
      failure = format_error(bound.error());
      return false;
    }
    listener_ = bound.take();
    const auto port = listener_.local_port();
    if (!port.ok()) {
      failure = format_error(port.error());
      return false;
    }
    endpoint_ = options_.data_host + ":" + std::to_string(static_cast<unsigned>(port.value()));
    incarnation_ = IncarnationId{incarnation};

    ClientOptions client_options;
    client_options.limits = limits_;
    client_options.host = options_.coordinator_host;
    client_options.port = options_.coordinator_port;
    client_options.identity = SessionIdentity{ParticipantKind::Producer, options_.id, incarnation_, boot_nonce};
    const Status connected = client_.connect(client_options);
    if (!connected.ok()) {
      failure = "connect: " + format_error(connected.error());
      return false;
    }
    const auto registration = client_.register_participant(ParticipantKind::Producer, options_.id, incarnation_,
                                                           endpoint_, options_.selection);
    if (!registration.ok()) {
      failure = "register: " + format_error(registration.error());
      return false;
    }
    const auto progress = client_.progress();
    if (!progress.ok()) {
      failure = "progress: " + format_error(progress.error());
      return false;
    }
    partition_count_ = progress.value().partitions_total;
    publish_phase();
    return true;
  }

  // Serves chunk requests until the shuffle leaves the Open state, the
  // coordinator goes away, or the caller asks the process to stop.
  [[nodiscard]] ProducerCounters serve(const std::atomic<bool>& external_stop) {
    external_stop_ = &external_stop;
    while (!external_stop.load(std::memory_order_relaxed)) {
      if (published_.empty()) {
        counters_.terminal = "nothing_published";
        break;
      }
      const auto status = client_.status();
      if (!status.ok()) {
        counters_.terminal = "coordinator_lost";
        break;
      }
      if (status.value().state != ShuffleState::Open) {
        counters_.terminal = "shuffle_closed";
        break;
      }
      auto accepted = listener_.accept(kAcceptBudgetMs);
      if (!accepted.ok()) {
        if (accepted.code() == ErrorCode::NoWorkAvailable || accepted.code() == ErrorCode::RetryDeferred) {
          continue;
        }
        counters_.terminal = "listener_failed";
        emit("NODE-FAIL role=producer id=" + u64_text(options_.id) + " code=" +
             to_string(accepted.code()) + " detail=listener");
        break;
      }
      serve_connection(accepted.value());
    }
    close_counters();
    return counters_;
  }

  [[nodiscard]] const std::string& endpoint() const noexcept { return endpoint_; }

 private:
  void close_counters() {
    std::uint64_t served_distinct = 0;
    for (const auto& entry : served_chunks_) {
      static_cast<void>(entry);
      ++served_distinct;
    }
    counters_.served = served_distinct;
    counters_.incomplete = counters_.chunks > served_distinct ? counters_.chunks - served_distinct : 0;
  }

  void publish_phase() {
    std::vector<std::uint64_t> wanted;
    if (options_.selection.kind == SelectionKind::All) {
      for (std::uint64_t partition = 0; partition < partition_count_; ++partition) {
        wanted.push_back(partition);
      }
    } else {
      for (std::uint64_t partition = 0; partition < partition_count_; ++partition) {
        if (options_.selection.covers(PartitionId{partition})) {
          wanted.push_back(partition);
        }
      }
    }

    std::set<std::uint64_t> previous;
    bool settled = false;
    for (std::uint32_t pass = 0; pass < kMaxPublishPasses && !settled; ++pass) {
      std::set<std::uint64_t> current;
      for (const std::uint64_t partition : wanted) {
        if (publish_one(partition)) {
          current.insert(partition);
        }
      }
      settled = pass > 0 && current == previous;
      previous = std::move(current);
    }
    if (!settled) {
      counters_.impossible = true;
      emit("NODE-FAIL role=producer id=" + u64_text(options_.id) + " code=InvalidState detail=publication_did_not_settle");
    }
    counters_.published = 0;
    counters_.chunks = 0;
    for (const auto& entry : published_) {
      static_cast<void>(entry);
      ++counters_.published;
      counters_.chunks += entry.second.manifest.chunks.size();
    }
    emit("NODE-READY role=producer id=" + u64_text(options_.id) + " incarnation=" + u64_text(incarnation_.value()) +
         " endpoint=" + endpoint_ + " published=" + u64_text(counters_.published));
  }

  // Publishes one partition. Returns true when an accepted manifest for this
  // partition exists afterwards. A partition owned by another producer is not
  // a failure: it is simply not this producer's work.
  [[nodiscard]] bool publish_one(std::uint64_t partition) {
    PartitionGeneration generation{1};
    const auto existing = client_.manifest(PartitionId{partition});
    if (existing.ok() && existing.value().partition != PartitionId{partition}) {
      return false;
    }
    if (existing.ok()) {
      const PartitionManifest& accepted = existing.value();
      if (accepted.producer == ProducerId{options_.id} && accepted.producer_incarnation == incarnation_) {
        generation = accepted.partition_generation;
      } else {
        generation = PartitionGeneration{accepted.partition_generation.value() + 1};
      }
    }

    for (std::uint32_t attempt = 0; attempt < 2; ++attempt) {
      const std::vector<std::byte> payload = synthetic_partition_payload(
          options_.shuffle, options_.generation, PartitionId{partition}, generation, options_.payload_bytes);
      const auto built = build_manifest(options_.shuffle, options_.generation, PartitionId{partition}, generation,
                                        ProducerId{options_.id}, incarnation_, TopologyGeneration{0}, payload,
                                        options_.chunk_bytes, limits_);
      if (!built.ok()) {
        emit("NODE-FAIL role=producer id=" + u64_text(options_.id) + " partition=" + u64_text(partition) +
             " code=" + to_string(built.code()) + " detail=manifest");
        return false;
      }
      const auto accepted = client_.publish_manifest(built.value());
      if (accepted.ok()) {
        PublishedPartition published;
        published.manifest = built.value();
        published.payload = payload;
        published_.emplace(std::make_pair(partition, generation.value()), std::move(published));
        return true;
      }
      if (accepted.code() == ErrorCode::PartitionNotOwned) {
        return false;
      }
      if (accepted.code() == ErrorCode::DivergentCommit) {
        // Another incarnation already described this partition generation: the
        // only honest move is a newer generation, which supersedes it.
        generation = PartitionGeneration{generation.value() + 1};
        continue;
      }
      emit("NODE-FAIL role=producer id=" + u64_text(options_.id) + " partition=" + u64_text(partition) +
           " code=" + to_string(accepted.code()) + " detail=publish");
      return false;
    }
    emit("NODE-FAIL role=producer id=" + u64_text(options_.id) + " partition=" + u64_text(partition) +
         " code=DivergentCommit detail=reproduction_exhausted");
    return false;
  }

  [[nodiscard]] bool serving_should_end() {
    if (external_stop_ != nullptr && external_stop_->load(std::memory_order_relaxed)) {
      return true;
    }
    const auto status = client_.status();
    if (!status.ok()) {
      return true;
    }
    if (status.value().state != ShuffleState::Open) {
      counters_.terminal = "shuffle_closed";
      return true;
    }
    return false;
  }

  void serve_connection(Socket& socket) {
    FrameStream stream{limits_};
    std::vector<std::byte> scratch(kReceiveBytes);
    std::vector<std::byte> answer;
    std::uint64_t last_sequence = 0;
    std::uint64_t idle_reads = 0;
    bool have_last = false;

    for (;;) {
      const auto frame = read_frame(socket, stream, scratch, kReadBudgetMs);
      if (!frame.ok()) {
        if (frame.code() == ErrorCode::RetryDeferred) {
          // No request yet. The budget bounded this wait, not a decision, so
          // the connection is kept and the run's state is re-checked at a
          // bounded interval: a peer that connects and then says nothing can
          // never hold the producer past the end of the run.
          if (++idle_reads >= kIdleReadsBeforeStateCheck && serving_should_end()) {
            break;
          }
          continue;
        }
        if (frame.code() != ErrorCode::ConnectionClosed) {
          emit("NODE-FAIL role=producer id=" + u64_text(options_.id) + " code=" + to_string(frame.code()) +
               " detail=chunk_request_frame");
        }
        break;
      }
      const DecodedFrame decoded = frame.value();
      if (decoded.header.type != MessageType::ChunkFetchRequest) {
        ++counters_.failed;
        emit("NODE-FAIL role=producer id=" + u64_text(options_.id) + " code=" + to_string(ErrorCode::FrameTypeUnsupported) +
             " detail=" + std::string{to_string(decoded.header.type)});
        break;
      }
      if (have_last && decoded.header.sequence == last_sequence) {
        // The replay path: the identical answer, byte for byte, and no second
        // read of the chunk.
        const Status sent = socket.send_all(answer, kReadBudgetMs);
        if (!sent.ok()) {
          break;
        }
        ++counters_.replayed;
        continue;
      }

      const auto request = decode_chunk_request(decoded.payload, limits_);
      std::vector<std::byte> outgoing;
      if (!request.ok()) {
        ++counters_.failed;
        emit("NODE-FAIL role=producer id=" + u64_text(options_.id) + " code=" + to_string(request.code()) +
             " detail=chunk_request");
        if (!encode_failure(ErrorCode::MalformedInput, "chunk request is not decodable", decoded.header.sequence,
                            outgoing)) {
          break;
        }
      } else if (!serve_chunk(request.value(), decoded.header.sequence, outgoing)) {
        if (outgoing.empty()) {
          break;
        }
      }
      // A truncated fault writes a frame prefix and then ends the stream: the
      // declared length is honest, the bytes are not all there.
      const bool truncate_after = truncate_next_;
      truncate_next_ = false;
      std::span<const std::byte> wire{outgoing};
      if (truncate_after && outgoing.size() > kFrameHeaderBytes) {
        const std::size_t keep = kFrameHeaderBytes + (outgoing.size() - kFrameHeaderBytes) / 2;
        wire = std::span<const std::byte>(outgoing.data(), keep);
      }
      const Status sent = socket.send_all(wire, kReadBudgetMs);
      if (!sent.ok()) {
        break;
      }
      if (truncate_after) {
        static_cast<void>(socket.shutdown());
        break;
      }
      idle_reads = 0;
      last_sequence = decoded.header.sequence;
      answer = outgoing;
      have_last = true;
    }
  }

  [[nodiscard]] bool encode_failure(ErrorCode code, std::string_view detail, std::uint64_t sequence,
                                   std::vector<std::byte>& out) {
    ByteWriter writer;
    writer.put_u16(static_cast<std::uint16_t>(code));
    writer.put_string(detail);
    return encode_message(MessageType::ChunkFetchFailure, SessionId{}, sequence, writer.data(), out, limits_).ok();
  }

  // Refusals are answers, not silence: the peer learns the deterministic code.
  [[nodiscard]] bool refuse(ErrorCode code, std::string_view detail, std::uint64_t sequence,
                            std::vector<std::byte>& out) {
    ++counters_.failed;
    emit("NODE-FAIL role=producer id=" + u64_text(options_.id) + " code=" + to_string(code) + " detail=" +
         std::string{detail});
    return encode_failure(code, detail, sequence, out);
  }

  // Fills out with the answer for one request. Returns false when out is a
  // refusal; out is empty only when the connection must be dropped.
  [[nodiscard]] bool serve_chunk(const ChunkRequest& request, std::uint64_t sequence, std::vector<std::byte>& out) {
    if (request.shuffle != options_.shuffle.value() || request.shuffle_generation != options_.generation.value()) {
      return refuse(ErrorCode::StaleGeneration, "request belongs to another shuffle generation", sequence, out);
    }
    const auto found = published_.find(std::make_pair(request.partition, request.partition_generation));
    if (found == published_.end()) {
      return refuse(ErrorCode::UnknownPartition, "no accepted manifest for this partition generation", sequence, out);
    }
    const PartitionManifest& manifest = found->second.manifest;
    const auto chunk = std::find_if(manifest.chunks.begin(), manifest.chunks.end(),
                                    [&](const ChunkDescriptor& descriptor) {
                                      return descriptor.id.value() == request.chunk;
                                    });
    if (chunk == manifest.chunks.end()) {
      return refuse(ErrorCode::UnknownChunk, "chunk is outside the manifest", sequence, out);
    }

    const std::size_t offset = static_cast<std::size_t>(chunk->offset);
    const std::size_t length = static_cast<std::size_t>(chunk->length);
    if (offset + length > found->second.payload.size()) {
      counters_.impossible = true;
      return refuse(ErrorCode::InternalError, "manifest describes bytes outside the produced payload", sequence, out);
    }

    std::vector<std::byte> bytes(found->second.payload.begin() + static_cast<std::ptrdiff_t>(offset),
                                 found->second.payload.begin() + static_cast<std::ptrdiff_t>(offset + length));
    if (options_.has_corrupt_chunk && request.chunk == options_.corrupt_chunk && !bytes.empty()) {
      // The descriptor is never touched: the bytes are wrong and the answer
      // still claims the digest the manifest declares.
      bytes[0] ^= std::byte{0x01};
      ++counters_.corrupt;
    }

    ByteWriter writer;
    writer.put_u64(chunk->id.value());
    writer.put_u64(chunk->offset);
    writer.put_u32(chunk->length);
    writer.put_digest(chunk->digest);
    writer.put_byte_string(bytes);
    if (!encode_message(MessageType::ChunkFetchResponse, SessionId{}, sequence, writer.data(), out, limits_).ok()) {
      counters_.impossible = true;
      ++counters_.failed;
      return false;
    }
    if (options_.has_truncate_chunk && request.chunk == options_.truncate_chunk) {
      ++counters_.failed;
      ++counters_.truncated;
      truncate_next_ = true;
      return true;
    }
    ++counters_.served;  // provisional: recomputed from the distinct set at the end
    served_chunks_.insert(std::make_pair(request.partition, request.chunk));
    return true;
  }

  const Options& options_;
  const SocketRuntime& runtime_;
  Limits limits_;
  TcpListener listener_{};
  std::string endpoint_{};
  IncarnationId incarnation_{};
  std::uint64_t partition_count_{0};
  CoordinatorClient client_{};
  std::map<std::pair<std::uint64_t, std::uint64_t>, PublishedPartition> published_{};
  std::set<std::pair<std::uint64_t, std::uint64_t>> served_chunks_{};
  ProducerCounters counters_{};
  bool truncate_next_{false};
  const std::atomic<bool>* external_stop_{nullptr};
};

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

enum class Terminal { AllResolved, WorkDone, Cancelled, Idle, CoordinatorLost, WaveLimit };

[[nodiscard]] const char* terminal_name(Terminal terminal) noexcept {
  switch (terminal) {
    case Terminal::AllResolved: return "all_resolved";
    case Terminal::WorkDone: return "work_done";
    case Terminal::Cancelled: return "cancelled";
    case Terminal::Idle: return "idle";
    case Terminal::CoordinatorLost: return "coordinator_lost";
    case Terminal::WaveLimit: return "wave_limit";
  }
  return "unknown";
}

// A refusal another attempt cannot repair resolves the edge for good; anything
// else leaves it pending, so the node keeps asking for work.
[[nodiscard]] bool terminal_refusal(ErrorCode code) noexcept {
  const RetryClass kind = classify(code);
  return kind == RetryClass::Permanent || kind == RetryClass::Authority || kind == RetryClass::NotApplicable;
}

struct ConsumerCounters {
  std::uint64_t observed{0};
  std::uint64_t committed{0};
  std::uint64_t failed{0};
  std::uint64_t incomplete{0};
  std::uint64_t waves{0};
  std::uint64_t idle{0};
  std::uint64_t foreign{0};
  bool impossible{false};
  Terminal terminal{Terminal::AllResolved};
  std::string duplicate{"not_run"};
  std::string stale_probe{"not_run"};
};

struct FetchResult {
  bool ok{false};
  ErrorCode code{ErrorCode::Ok};
  std::string detail{};
  std::vector<std::byte> content{};
  std::vector<Digest> digests{};
};

class ConsumerSession {
 public:
  ConsumerSession(const Options& options, const SocketRuntime& runtime)
      : options_(options), runtime_(runtime), limits_(options.limits) {}

  [[nodiscard]] bool setup(std::uint64_t incarnation, std::uint64_t boot_nonce, std::string& failure) {
    incarnation_ = IncarnationId{incarnation};
    boot_nonce_ = boot_nonce;
    ClientOptions client_options;
    client_options.limits = limits_;
    client_options.host = options_.coordinator_host;
    client_options.port = options_.coordinator_port;
    client_options.identity = SessionIdentity{ParticipantKind::Consumer, options_.id, incarnation_, boot_nonce_};
    const Status connected = client_.connect(client_options);
    if (!connected.ok()) {
      failure = format_error(connected.error());
      return false;
    }
    const auto registration = client_.register_participant(ParticipantKind::Consumer, options_.id, incarnation_,
                                                           "127.0.0.1:0", options_.selection);
    if (!registration.ok()) {
      failure = format_error(registration.error());
      return false;
    }
    const auto progress = client_.progress();
    if (!progress.ok()) {
      failure = "progress: " + format_error(progress.error());
      return false;
    }
    for (std::uint64_t partition = 0; partition < progress.value().partitions_total; ++partition) {
      if (options_.selection.covers(PartitionId{partition})) {
        ++expected_edges_;
      }
    }
    active_ = &client_;
    emit("NODE-READY role=consumer id=" + u64_text(options_.id) + " incarnation=" + u64_text(incarnation_.value()));
    return true;
  }

  [[nodiscard]] ConsumerCounters run() {
    for (std::uint64_t wave_index = 0; wave_index < kMaxWaves; ++wave_index) {
      if (expected_edges_ > 0 && terminal_edges_ >= expected_edges_) {
        // Every edge this node was required to move has reached a terminal
        // outcome: asking for more work could only steal another session's
        // authority, so the node stops here.
        counters_.terminal = Terminal::WorkDone;
        break;
      }
      const auto plan = active_->next_wave();
      if (!plan.ok()) {
        if (plan.code() == ErrorCode::Cancelled) {
          counters_.terminal = Terminal::Cancelled;
        } else {
          counters_.terminal = Terminal::CoordinatorLost;
          emit("NODE-FAIL role=consumer id=" + u64_text(options_.id) + " code=" + to_string(plan.code()) +
               " detail=next_wave");
        }
        break;
      }
      ++counters_.waves;
      if (plan.value().all_resolved) {
        counters_.terminal = Terminal::AllResolved;
        break;
      }

      std::uint64_t own = 0;
      for (const DispatchGrant& grant : plan.value().grants) {
        // A grant is this node's own when it names this consumer and either the
        // incarnation this session holds or the one the supersession probe just
        // replaced (whose attempts still have to be resolved).
        const bool mine = grant.consumer == ConsumerId{options_.id} &&
                          (grant.consumer_incarnation == incarnation_ ||
                           (superseding_ && !superseded_incarnation_.is_zero() &&
                            grant.consumer_incarnation == superseded_incarnation_));
        if (!mine) {
          // The wave plan is produced for the fabric, not for this session: a
          // grant addressed to another consumer carries authority this session
          // cannot exercise. It is handed straight back so its owner can pick
          // it up, and it is never counted as this node's own work.
          ++counters_.foreign;
          hand_back(grant);
          continue;
        }
        ++own;
        if (superseding_) {
          resolve_superseded(grant);
          continue;
        }
        process(grant);
        if (options_.fault_stale_incarnation && counters_.stale_probe == "not_run" && counters_.committed > 0) {
          superseding_ = probe_stale_incarnation();
        }
      }

      if (own == 0) {
        ++counters_.idle;
        if (counters_.idle >= options_.max_idle_waves) {
          counters_.terminal = Terminal::Idle;
          break;
        }
      } else {
        counters_.idle = 0;
      }
      if (wave_index + 1 == kMaxWaves) {
        counters_.terminal = Terminal::WaveLimit;
      }
    }
    return counters_;
  }

 private:
  [[nodiscard]] ErrorCode transport_code(ErrorCode code) const noexcept {
    switch (code) {
      case ErrorCode::ConnectionFailure:
      case ErrorCode::ConnectionClosed:
      case ErrorCode::PeerUnavailable:
      case ErrorCode::ChecksumMismatch:
      case ErrorCode::TruncatedInput:
      case ErrorCode::ProtocolViolation:
      case ErrorCode::MalformedInput:
      case ErrorCode::RetryDeferred:
        return code == ErrorCode::RetryDeferred ? ErrorCode::ConnectionFailure : code;
      default:
        return ErrorCode::ConnectionFailure;
    }
  }

  // A grant addressed to another consumer session is not this node's work. It
  // is returned to the fabric with a retriable code -- the outcome of an
  // attempt this session cannot exercise is ambiguous *to this session* -- so
  // its owner receives it again and no edge is left in flight forever.
  void hand_back(const DispatchGrant& grant) {
    const auto returned = active_->report_failure(grant.attempt, ErrorCode::AmbiguousOutcome);
    emit("NODE-FAIL role=consumer id=" + u64_text(options_.id) + " attempt=" + u64_text(grant.attempt.value()) +
         " partition=" + u64_text(grant.partition.value()) + " consumer=" + u64_text(grant.consumer.value()) +
         " code=AmbiguousOutcome returned=" + (returned.ok() ? "yes" : "no"));
  }

  void report_failure(const DispatchGrant& grant, ErrorCode code, std::string_view detail) {
    const auto reported = active_->report_failure(grant.attempt, code);
    if (reported.ok()) {
      ++counters_.failed;
      if (reported.value().permanent || terminal_refusal(code)) {
        ++terminal_edges_;
      }
      emit("NODE-FAIL role=consumer id=" + u64_text(options_.id) + " attempt=" + u64_text(grant.attempt.value()) +
           " partition=" + u64_text(grant.partition.value()) + " code=" + to_string(code) + " reported=yes");
      return;
    }
    counters_.impossible = counters_.impossible || reported.code() == ErrorCode::IdentityMismatch;
    ++counters_.incomplete;
    emit("NODE-FAIL role=consumer id=" + u64_text(options_.id) + " attempt=" + u64_text(grant.attempt.value()) +
         " partition=" + u64_text(grant.partition.value()) + " code=" + to_string(code) + " reported=no refusal=" +
         to_string(reported.code()) + " detail=" + std::string{detail});
  }

  void process(const DispatchGrant& grant) {
    ++counters_.observed;
    const auto fetched = active_->manifest(grant.partition);
    if (!fetched.ok()) {
      report_failure(grant, fetched.code(), fetched.detail());
      return;
    }
    const PartitionManifest& manifest = fetched.value();
    if (compute_manifest_digest(manifest) != grant.manifest_digest) {
      report_failure(grant, ErrorCode::StaleGeneration, "accepted manifest advanced past the grant");
      return;
    }

    FetchResult result = fetch(grant, manifest);
    if (!result.ok) {
      report_failure(grant, result.code, result.detail);
      return;
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
    request.manifest_digest = grant.manifest_digest;
    request.observed_partition_digest = compute_partition_digest(manifest);
    request.observed_chunk_digests = result.digests;
    request.bytes = manifest.total_bytes;
    request.integrity_verified = true;

    const auto committed = active_->commit_transfer(request);
    if (!committed.ok()) {
      ++counters_.failed;
      if (terminal_refusal(committed.code())) {
        ++terminal_edges_;
      }
      emit("NODE-FAIL role=consumer id=" + u64_text(options_.id) + " attempt=" + u64_text(grant.attempt.value()) +
           " partition=" + u64_text(grant.partition.value()) + " code=" + to_string(committed.code()) +
           " reported=refused");
      return;
    }
    ++counters_.committed;
    ++terminal_edges_;
    if (counters_.committed == 1) {
      first_commit_ = request;
    } else {
      last_commit_ = request;
    }
  }

  [[nodiscard]] FetchResult fetch(const DispatchGrant& grant, const PartitionManifest& manifest) {
    FetchResult result;
    std::string host;
    std::uint16_t port = 0;
    if (!parse_host_port(grant.producer_endpoint, host, port)) {
      result.code = ErrorCode::InvalidArgument;
      result.detail = "producer endpoint is not HOST:PORT";
      return result;
    }
    auto socket = Socket::connect(host, port, kConnectTimeoutMs, runtime_);
    if (!socket.ok()) {
      result.code = transport_code(socket.code());
      result.detail = "data connection refused";
      return result;
    }

    FrameStream stream{limits_};
    std::vector<std::byte> scratch(kReceiveBytes);
    std::vector<std::byte> content;
    std::uint64_t sequence = 0;

    for (const ChunkDescriptor& chunk : manifest.chunks) {
      ++sequence;
      ByteWriter writer;
      writer.put_u64(options_.shuffle.value());
      writer.put_u64(options_.generation.value());
      writer.put_u64(grant.partition.value());
      writer.put_u64(grant.partition_generation.value());
      writer.put_u64(chunk.id.value());
      std::vector<std::byte> request_frame;
      const Status encoded = encode_message(MessageType::ChunkFetchRequest, SessionId{}, sequence, writer.data(),
                                            request_frame, limits_);
      if (!encoded.ok()) {
        result.code = encoded.code();
        result.detail = "chunk request could not be encoded";
        return result;
      }
      const Status sent = socket.value().send_all(request_frame, kReadBudgetMs);
      if (!sent.ok()) {
        result.code = transport_code(sent.code());
        result.detail = "chunk request could not be sent";
        return result;
      }
      const auto answer = read_frame(socket.value(), stream, scratch, kReadBudgetMs);
      if (!answer.ok()) {
        result.code = transport_code(answer.code());
        result.detail = "chunk answer was not received";
        return result;
      }

      if (options_.fault_duplicate_frame && counters_.duplicate == "not_run") {
        counters_.duplicate = "mismatch";
        const Status resent = socket.value().send_all(request_frame, kReadBudgetMs);
        if (!resent.ok()) {
          result.code = transport_code(resent.code());
          result.detail = "duplicate request could not be sent";
          return result;
        }
        const auto replay = read_frame(socket.value(), stream, scratch, kReadBudgetMs);
        if (!replay.ok()) {
          result.code = transport_code(replay.code());
          result.detail = "duplicate request was not answered";
          return result;
        }
        std::vector<std::byte> first_bytes;
        std::vector<std::byte> second_bytes;
        const Status first_encoded = raw_frame_bytes(answer.value(), first_bytes, limits_);
        const Status second_encoded = raw_frame_bytes(replay.value(), second_bytes, limits_);
        if (!first_encoded.ok() || !second_encoded.ok() || first_bytes != second_bytes) {
          counters_.impossible = true;
          result.code = ErrorCode::DuplicateFrame;
          result.detail = "a repeated request frame did not produce the identical answer";
          return result;
        }
        counters_.duplicate = "verified";
      }

      const DecodedFrame decoded = answer.value();
      if (decoded.header.type == MessageType::ChunkFetchFailure) {
        ByteReader reader{decoded.payload, limits_, "chunk fetch failure"};
        const std::uint16_t raw_code = reader.u16();
        const std::string detail = reader.string(limits_.max_string_bytes);
        const Status end = reader.require_end();
        result.code = end.ok() ? static_cast<ErrorCode>(raw_code) : end.code();
        result.detail = detail;
        return result;
      }
      if (decoded.header.type != MessageType::ChunkFetchResponse) {
        result.code = ErrorCode::ProtocolViolation;
        result.detail = "data plane answered with an unexpected message type";
        return result;
      }
      ByteReader reader{decoded.payload, limits_, "chunk fetch response"};
      const std::uint64_t chunk_id = reader.u64();
      const std::uint64_t offset = reader.u64();
      const std::uint32_t length = reader.u32();
      const Digest digest = reader.digest();
      const std::span<const std::byte> bytes = reader.bytes(limits_.max_frame_payload_bytes);
      const Status end = reader.require_end();
      if (!end.ok()) {
        result.code = end.code();
        result.detail = "chunk answer is not decodable";
        return result;
      }
      if (chunk_id != chunk.id.value() || offset != chunk.offset || length != chunk.length ||
          bytes.size() != chunk.length) {
        result.code = ErrorCode::PayloadRejected;
        result.detail = "chunk answer does not describe the requested range";
        return result;
      }
      const Digest observed = sha256(bytes);
      if (observed != chunk.digest || digest != chunk.digest) {
        result.code = ErrorCode::DigestMismatch;
        result.detail = "chunk payload does not match the manifest";
        return result;
      }
      result.digests.push_back(observed);
      content.insert(content.end(), bytes.begin(), bytes.end());
    }

    const Status verified = verify_partition_content(manifest, content);
    if (!verified.ok()) {
      result.code = verified.code();
      result.detail = "assembled partition does not match the manifest";
      return result;
    }
    result.content = std::move(content);
    result.ok = true;
    return result;
  }

  // Mid-run re-registration with a fresh incarnation, then one completion
  // offered under the superseded incarnation: it must be refused. Attempts that
  // were already in flight under the superseded incarnation are resolved
  // explicitly, so no edge is silently lost. The run then continues on the new
  // session, which is the incarnation that holds authority.
  [[nodiscard]] bool probe_stale_incarnation() {
    if (counters_.committed == 0) {
      return false;
    }
    const auto state = active_->status();
    if (!state.ok() || state.value().state != ShuffleState::Open) {
      counters_.stale_probe = "shuffle_closed";
      return false;
    }
    counters_.stale_probe = "accepted";
    const std::uint64_t next_incarnation = fresh_identity_value();
    auto replacement = std::make_unique<CoordinatorClient>();
    ClientOptions client_options;
    client_options.limits = limits_;
    client_options.host = options_.coordinator_host;
    client_options.port = options_.coordinator_port;
    client_options.identity = SessionIdentity{ParticipantKind::Consumer, options_.id, IncarnationId{next_incarnation},
                                              fresh_identity_value()};
    if (!replacement->connect(client_options).ok()) {
      emit("NODE-FAIL role=consumer id=" + u64_text(options_.id) + " code=ConnectionFailure detail=superseding_session");
      counters_.stale_probe = "session_failed";
      return false;
    }
    const auto registration = replacement->register_participant(ParticipantKind::Consumer, options_.id,
                                                               IncarnationId{next_incarnation}, "127.0.0.1:0",
                                                               options_.selection);
    if (!registration.ok()) {
      emit("NODE-FAIL role=consumer id=" + u64_text(options_.id) + " code=" + to_string(registration.code()) +
           " detail=superseding_registration:" + format_error(registration.error()));
      counters_.stale_probe = "registration_failed";
      return false;
    }

    const CommitRequest& duplicate = counters_.committed > 1 ? last_commit_ : first_commit_;
    const auto refused = active_->commit_transfer(duplicate);
    if (refused.ok()) {
      counters_.impossible = true;
      emit("NODE-FAIL role=consumer id=" + u64_text(options_.id) + " code=InvalidState detail=stale_incarnation_completed");
    } else {
      counters_.stale_probe = "refused";
      emit("NODE-PROBE stale_incarnation refused code=" + std::string{to_string(refused.code())});
    }

    // Continue on the incarnation that holds authority.
    static_cast<void>(active_->close());
    replacement_ = std::move(replacement);
    active_ = replacement_.get();
    superseded_incarnation_ = incarnation_;
    incarnation_ = IncarnationId{next_incarnation};
    return true;
  }

  // An attempt granted to the incarnation that no longer holds authority is
  // resolved explicitly as superseded: the ledger records it, the scheduler
  // releases it, and nothing is left in flight forever.
  void resolve_superseded(const DispatchGrant& grant) {
    ++counters_.observed;
    const auto reported = active_->report_failure(grant.attempt, ErrorCode::AttemptSuperseded);
    if (reported.ok()) {
      ++counters_.failed;
      ++terminal_edges_;
      emit("NODE-FAIL role=consumer id=" + u64_text(options_.id) + " attempt=" + u64_text(grant.attempt.value()) +
           " partition=" + u64_text(grant.partition.value()) + " code=AttemptSuperseded reported=yes");
      return;
    }
    ++counters_.incomplete;
    emit("NODE-FAIL role=consumer id=" + u64_text(options_.id) + " attempt=" + u64_text(grant.attempt.value()) +
         " partition=" + u64_text(grant.partition.value()) + " code=AttemptSuperseded reported=no refusal=" +
         to_string(reported.code()));
  }

  const Options& options_;
  const SocketRuntime& runtime_;
  Limits limits_;
  CoordinatorClient client_{};
  CoordinatorClient* active_{nullptr};
  std::unique_ptr<CoordinatorClient> replacement_{};
  IncarnationId incarnation_{};
  std::uint64_t boot_nonce_{0};
  CommitRequest first_commit_{};
  CommitRequest last_commit_{};
  bool superseding_{false};
  IncarnationId superseded_incarnation_{};
  std::uint64_t expected_edges_{0};
  std::uint64_t terminal_edges_{0};
  ConsumerCounters counters_{};
};

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!parse_options(argc, argv, options)) {
    print_usage();
    return 2;
  }

  SocketRuntime runtime;
  if (!runtime.active()) {
    emit("NODE-FAIL role=" + options.role + " id=" + u64_text(options.id) + " code=" + to_string(runtime.code()) +
         " detail=socket_runtime");
    return 3;
  }

  const std::uint64_t incarnation = options.incarnation != 0 ? options.incarnation : fresh_identity_value();
  const std::uint64_t boot_nonce = fresh_identity_value();
  const bool wants_producer = options.role == "producer" || options.role == "both";
  const bool wants_consumer = options.role == "consumer" || options.role == "both";

  ProducerCounters producer_counters;
  ConsumerCounters consumer_counters;
  bool setup_failed = false;

  if (wants_producer && wants_consumer) {
    ProducerSession producer{options, runtime};
    std::string failure;
    if (!producer.setup(incarnation, boot_nonce, failure)) {
      emit("NODE-FAIL role=both id=" + u64_text(options.id) + " code=InvalidState detail=" + failure);
      return 3;
    }
    std::atomic<bool> stop{false};
    std::thread serving{[&] { producer_counters = producer.serve(stop); }};
    ConsumerSession consumer{options, runtime};
    if (consumer.setup(incarnation, boot_nonce, failure)) {
      consumer_counters = consumer.run();
    } else {
      setup_failed = true;
      emit("NODE-FAIL role=both id=" + u64_text(options.id) + " code=InvalidState detail=" + failure);
    }
    stop.store(true, std::memory_order_relaxed);
    serving.join();
  } else if (wants_producer) {
    ProducerSession producer{options, runtime};
    std::string failure;
    if (!producer.setup(incarnation, boot_nonce, failure)) {
      emit("NODE-FAIL role=producer id=" + u64_text(options.id) + " code=InvalidState detail=" + failure);
      return 3;
    }
    const std::atomic<bool> never_stop{false};
    producer_counters = producer.serve(never_stop);
  } else {
    ConsumerSession consumer{options, runtime};
    std::string failure;
    if (!consumer.setup(incarnation, boot_nonce, failure)) {
      emit("NODE-FAIL role=consumer id=" + u64_text(options.id) + " code=InvalidState detail=" + failure);
      return 3;
    }
    consumer_counters = consumer.run();
  }

  if (setup_failed) {
    return 3;
  }

  std::string summary;
  int exit_code = 0;
  if (wants_consumer) {
    summary = "NODE role=" + options.role + " id=" + u64_text(options.id) + " incarnation=" + u64_text(incarnation) +
              " committed=" + u64_text(consumer_counters.committed) + " failed=" + u64_text(consumer_counters.failed) +
              " incomplete=" + u64_text(consumer_counters.incomplete) + " waves=" + u64_text(consumer_counters.waves) +
              " idle=" + u64_text(consumer_counters.idle) + " foreign=" + u64_text(consumer_counters.foreign);
    const bool closes = consumer_counters.committed + consumer_counters.failed + consumer_counters.incomplete ==
                        consumer_counters.observed;
    if (!closes || consumer_counters.impossible) {
      exit_code = 1;
    } else if (consumer_counters.terminal == Terminal::CoordinatorLost ||
               consumer_counters.terminal == Terminal::WaveLimit) {
      exit_code = 3;
    }
    if (options.print_result) {
      emit("RESULT role=" + options.role + " id=" + u64_text(options.id) + " terminal=" +
           terminal_name(consumer_counters.terminal) + " impossible=" + (consumer_counters.impossible ? "1" : "0") +
           " observed=" + u64_text(consumer_counters.observed) + " committed=" + u64_text(consumer_counters.committed) +
           " failed=" + u64_text(consumer_counters.failed) + " incomplete=" + u64_text(consumer_counters.incomplete) +
           " foreign=" + u64_text(consumer_counters.foreign) + " duplicate=" + consumer_counters.duplicate +
           " stale_probe=" + consumer_counters.stale_probe);
    }
  } else {
    summary = "NODE role=producer id=" + u64_text(options.id) + " incarnation=" + u64_text(incarnation) +
              " committed=" + u64_text(producer_counters.served) + " failed=" + u64_text(producer_counters.failed) +
              " incomplete=" + u64_text(producer_counters.incomplete) + " waves=0 idle=0 published=" +
              u64_text(producer_counters.published);
    const bool closes = producer_counters.served + producer_counters.incomplete == producer_counters.chunks;
    if (!closes || producer_counters.impossible) {
      exit_code = 1;
    } else if (producer_counters.terminal == "coordinator_lost" || producer_counters.terminal == "listener_failed") {
      exit_code = 3;
    }
    if (options.print_result) {
      emit("RESULT role=" + options.role + " id=" + u64_text(options.id) + " terminal=" + producer_counters.terminal +
           " impossible=" + (producer_counters.impossible ? "1" : "0") + " published=" +
           u64_text(producer_counters.published) + " chunks=" + u64_text(producer_counters.chunks) + " served=" +
           u64_text(producer_counters.served) + " failed=" + u64_text(producer_counters.failed) + " incomplete=" +
           u64_text(producer_counters.incomplete) + " replayed=" + u64_text(producer_counters.replayed) +
           " corrupt=" + u64_text(producer_counters.corrupt) + " truncated=" + u64_text(producer_counters.truncated));
    }
  }

  emit(summary);
  return exit_code;
}
