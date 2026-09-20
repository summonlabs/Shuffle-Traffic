// shuffle-fabric-cli -- deterministic inspection tooling for ShuffleFabric.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Build (from the repository root, once the library is current):
//
//   scripts\msvc.ps1 -Command 'cmake --build build/release --target shuffle_fabric'
//   scripts\msvc.ps1 -Command 'cl /nologo /std:c++20 /W4 /WX /EHsc /permissive- /utf-8 /Zc:__cplusplus /MD /Iinclude /Fo:build\examples-scratch\ /Fd:build\examples-scratch\vc-cli.pdb tools\shuffle-fabric-cli.cpp build\release\shuffle_fabric.lib ws2_32.lib /Fe:build\examples-scratch\shuffle-fabric-cli.exe'
//
// The tool answers questions about a shuffle without moving anything:
//
//   plan           what would move, planned through the real WaveScheduler
//   inspect-state  what a durable store on disk actually reports
//   progress       remote progress, when this build has a service client
//   explain        remote refusal explanation, when this build has a service client
//   status         remote status, when this build has a service client
//
// Every value is parsed and bounded before use, every refusal names a
// deterministic ErrorCode, and no output contains a timestamp or a wall clock.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <vector>

#include "shuffle/fabric/coordinator.hpp"
#include "shuffle/fabric/durable_store.hpp"
#include "shuffle/fabric/hash.hpp"
#include "shuffle/fabric/policy.hpp"
#include "shuffle/fabric/records.hpp"
#include "shuffle/fabric/schedule.hpp"
#include "shuffle/fabric/service.hpp"
#include "shuffle/fabric/topology.hpp"
#include "shuffle/fabric/version.hpp"

namespace {

using namespace shuffle::fabric;

constexpr ShuffleId kShuffle{1};
constexpr ShuffleGeneration kShuffleGeneration{1};
constexpr PolicyGeneration kPolicyGeneration{1};
constexpr IncarnationId kIncarnation{1};
constexpr PartitionGeneration kPartitionGeneration{1};
constexpr std::uint64_t kModelledPartitionBytes = 65536;

// Argument bounds. Nothing reaches the library without passing one of these.
constexpr std::uint32_t kMaxPartitions = 65536;
constexpr std::uint32_t kMaxParticipants = 4096;
constexpr std::uint32_t kMaxWavesToPrint = 64;
constexpr std::uint32_t kMaxGrantsPerWave = 16384;
constexpr std::uint32_t kMaxConcurrency = 65536;
constexpr std::uint32_t kMaxConnectTimeoutMs = 600000;
constexpr std::uint32_t kMaxIdentityValue = 1000000000;
constexpr std::uint32_t kExplainSamples = 8;
constexpr std::size_t kMaxSelectionItems = 256;
constexpr std::size_t kPairsPerLine = 8;

// Bounded decimal parsing. No atoi, no strtol: leading zeros, signs, whitespace
// and any value outside [lowest, highest] are refused rather than truncated.
[[nodiscard]] bool parse_u32(std::string_view text, std::uint32_t lowest, std::uint32_t highest,
                             std::uint32_t& out) {
  if (text.empty() || text.size() > 10) return false;
  if (text.size() > 1 && text.front() == '0') return false;
  std::uint64_t value = 0;
  for (const char ch : text) {
    if (ch < '0' || ch > '9') return false;
    value = value * 10 + static_cast<std::uint64_t>(ch - '0');
    if (value > highest) return false;
  }
  if (value < lowest) return false;
  out = static_cast<std::uint32_t>(value);
  return true;
}

void print_usage() {
  std::cout << "shuffle-fabric-cli " << version_string() << " -- deterministic inspection tooling\n"
            << "\nusage:\n"
            << "  shuffle-fabric-cli plan --partitions N --producers P --consumers C [options]\n"
            << "      --selection all|range:A:B|list:a,b,c  what every consumer requires (default all)\n"
            << "      --max-grants G           grants per wave, 1..16384 (default 4)\n"
            << "      --global G               concurrent transfers overall, 1..65536 (default 8)\n"
            << "      --max-per-source S       concurrent transfers per producer (default 2)\n"
            << "      --max-per-destination D  concurrent transfers per consumer (default 2)\n"
            << "      --waves K                waves to print, 1..64 (default 4)\n"
            << "  shuffle-fabric-cli inspect-state --state-dir <dir>\n"
            << "  shuffle-fabric-cli progress --coordinator <host:port> [options]\n"
            << "  shuffle-fabric-cli explain  --coordinator <host:port> [options]\n"
            << "  shuffle-fabric-cli status   --coordinator <host:port> [options]\n"
            << "      --connect-timeout-ms N   connect wait in ms, 1..600000 (default 5000)\n"
            << "      --participant-id N       session identity, 1..1000000000 (default 1)\n"
            << "      --incarnation N          session incarnation, 1..1000000000 (default 1)\n"
            << "  shuffle-fabric-cli --version\n"
            << "  shuffle-fabric-cli --help\n"
            << "\nEvery option value is validated and bounded; unknown options are refused.\n";
}

[[nodiscard]] int usage_refusal(const std::string_view subcommand, const std::string& detail) {
  std::cout << "refused: " << subcommand << ": " << detail << "\n";
  return 2;
}

[[nodiscard]] int library_refusal(const std::string_view subcommand, ErrorCode code, const std::string& detail) {
  std::cout << "refused: " << subcommand << ": " << to_string(code) << ": " << detail << "\n";
  return 1;
}

[[nodiscard]] std::string number(std::uint32_t value) { return std::to_string(value); }

// ---------------------------------------------------------------------------
// plan: what would move
// ---------------------------------------------------------------------------
//
// The plan is produced by the real WaveScheduler over a real Topology. Nothing
// is transferred: after each wave the CLI resolves its own grants as modelled
// completions so the cursor can move on, and says so in the output.

class ModelledCompletion final : public CompletionView {
 public:
  void complete(const EdgeKey& key) { completed_.insert(key); }

  [[nodiscard]] EdgeStatus edge_status(const EdgeKey& key) const override {
    EdgeStatus status;
    status.phase = completed_.count(key) == 0 ? EdgePhase::Pending : EdgePhase::Completed;
    status.reason = ErrorCode::Ok;
    status.authoritative = status.phase == EdgePhase::Completed;
    return status;
  }

 private:
  std::unordered_set<EdgeKey> completed_{};
};

// Every partition is modelled as already produced at generation 1 by its
// deterministic owner. The question being answered is "what would move", not
// "what has been produced so far".
class ModelledPartitions final : public PartitionView {
 public:
  explicit ModelledPartitions(const Topology& topology) : topology_(&topology), digest_(sha256("shuffle-fabric-cli")) {}

  [[nodiscard]] PartitionFacts partition_facts(PartitionId id) const override {
    PartitionFacts facts;
    const auto owner = topology_->owner_of(id);
    facts.produced = owner.ok();
    facts.partition_generation = kPartitionGeneration;
    facts.manifest_digest = digest_;
    facts.total_bytes = kModelledPartitionBytes;
    facts.producer = owner.ok() ? owner.value() : ProducerId{};
    facts.producer_incarnation = kIncarnation;
    return facts;
  }

 private:
  const Topology* topology_;
  Digest digest_;
};

[[nodiscard]] Result<PartitionSelection> parse_selection(std::string_view text, std::uint32_t partitions) {
  if (text == "all") return PartitionSelection::all();
  if (text.substr(0, 6) == "range:") {
    const std::string_view body = text.substr(6);
    const std::size_t colon = body.find(':');
    if (colon == std::string_view::npos) {
      return make_failure<PartitionSelection>(ErrorCode::MalformedInput, "range needs two bounds: range:A:B");
    }
    std::uint32_t first = 0;
    std::uint32_t limit = 0;
    if (!parse_u32(body.substr(0, colon), 0, partitions, first) ||
        !parse_u32(body.substr(colon + 1), 0, partitions, limit)) {
      return make_failure<PartitionSelection>(ErrorCode::MalformedInput,
                                              "range bounds must be integers in [0, " + number(partitions) + "]");
    }
    if (first >= limit) {
      return make_failure<PartitionSelection>(ErrorCode::InvalidArgument, "range is empty: first must be below limit");
    }
    return PartitionSelection::range(PartitionId{first}, PartitionId{limit});
  }
  if (text.substr(0, 5) == "list:") {
    const std::string_view body = text.substr(5);
    std::vector<PartitionId> items;
    std::size_t start = 0;
    while (start <= body.size()) {
      const std::size_t comma = body.find(',', start);
      const std::string_view item = body.substr(start, comma == std::string_view::npos ? comma : comma - start);
      if (item.empty()) {
        return make_failure<PartitionSelection>(ErrorCode::MalformedInput, "list contains an empty item");
      }
      std::uint32_t value = 0;
      if (!parse_u32(item, 0, partitions - 1, value)) {
        return make_failure<PartitionSelection>(
            ErrorCode::MalformedInput, "list items must be integers in [0, " + number(partitions - 1) + "]");
      }
      items.push_back(PartitionId{value});
      if (items.size() > kMaxSelectionItems) {
        return make_failure<PartitionSelection>(ErrorCode::LimitExceeded,
                                                "list holds more than " + number(kMaxSelectionItems) + " partitions");
      }
      if (comma == std::string_view::npos) break;
      start = comma + 1;
    }
    return PartitionSelection::from_list(std::move(items), Limits{});
  }
  return make_failure<PartitionSelection>(ErrorCode::MalformedInput,
                                          "selection must be all, range:A:B or list:a,b,c");
}

void print_pairs(const std::vector<DispatchGrant>& grants) {
  for (std::size_t index = 0; index < grants.size(); ++index) {
    if (index % kPairsPerLine == 0) {
      std::cout << (index == 0 ? "  pairs:" : "\n        ");
    }
    const DispatchGrant& grant = grants[index];
    std::cout << " " << EdgeKey{grant.partition, grant.partition_generation, grant.consumer}.to_string();
  }
  std::cout << "\n";
}

struct PlanOptions {
  std::uint32_t partitions{8};
  std::uint32_t producers{3};
  std::uint32_t consumers{4};
  std::uint32_t max_grants{4};
  std::uint32_t global{8};
  std::uint32_t per_source{2};
  std::uint32_t per_destination{2};
  std::uint32_t waves{4};
  std::string selection{"all"};
};

[[nodiscard]] int command_plan(const std::vector<std::string_view>& arguments) {
  constexpr std::string_view kSub{"plan"};
  PlanOptions options;
  std::vector<std::string_view> used;
  for (std::size_t index = 0; index < arguments.size(); ++index) {
    const std::string_view flag = arguments[index];
    if (flag == "--help" || flag == "-h") {
      print_usage();
      return 0;
    }
    if (flag.size() < 3 || flag.substr(0, 2) != "--") {
      return usage_refusal(kSub, "unexpected argument: " + std::string(flag));
    }
    if (index + 1 >= arguments.size()) {
      return usage_refusal(kSub, "option " + std::string(flag) + " requires a value");
    }
    if (std::find(used.begin(), used.end(), flag) != used.end()) {
      return usage_refusal(kSub, "option " + std::string(flag) + " was given more than once");
    }
    used.push_back(flag);
    const std::string_view value = arguments[++index];
    if (flag == "--selection") {
      options.selection = std::string(value);
    } else {
      std::uint32_t parsed = 0;
      const std::uint32_t highest = flag == "--max-grants"      ? kMaxGrantsPerWave
                                    : flag == "--waves"         ? kMaxWavesToPrint
                                    : flag == "--partitions"    ? kMaxPartitions
                                    : flag == "--producers" || flag == "--consumers" ? kMaxParticipants
                                                                                     : kMaxConcurrency;
      if (flag != "--max-grants" && flag != "--waves" && flag != "--partitions" && flag != "--producers" &&
          flag != "--consumers" && flag != "--global" && flag != "--max-per-source" &&
          flag != "--max-per-destination") {
        return usage_refusal(kSub, "unknown option: " + std::string(flag));
      }
      if (!parse_u32(value, 1, highest, parsed)) {
        return usage_refusal(kSub, "option " + std::string(flag) + " must be an integer in [1, " + number(highest) + "]");
      }
      if (flag == "--partitions") options.partitions = parsed;
      if (flag == "--producers") options.producers = parsed;
      if (flag == "--consumers") options.consumers = parsed;
      if (flag == "--max-grants") options.max_grants = parsed;
      if (flag == "--global") options.global = parsed;
      if (flag == "--max-per-source") options.per_source = parsed;
      if (flag == "--max-per-destination") options.per_destination = parsed;
      if (flag == "--waves") options.waves = parsed;
    }
  }
  if (options.per_source > options.global) {
    return library_refusal(kSub, ErrorCode::InvalidArgument, "max-per-source exceeds global concurrency");
  }
  if (options.per_destination > options.global) {
    return library_refusal(kSub, ErrorCode::InvalidArgument, "max-per-destination exceeds global concurrency");
  }

  const auto selection = parse_selection(options.selection, options.partitions);
  if (!selection.ok()) {
    return library_refusal(kSub, selection.code(), selection.detail());
  }

  const Limits limits;
  PolicyEnvelope policy;
  policy.shuffle = kShuffle;
  policy.shuffle_generation = kShuffleGeneration;
  policy.generation = kPolicyGeneration;
  policy.fan = FanBounds{options.consumers, options.producers};
  policy.concurrency = ConcurrencyLimits{options.global, options.per_source, options.per_destination};
  policy.waves = WaveLimits{options.max_grants, std::max(options.max_grants * 16u, 1024u)};
  policy.retry = RetryPolicy{3, 0};
  policy.congestion = CongestionPolicy{80, 40, false, 10};

  Topology topology{kShuffle, kShuffleGeneration, options.partitions, limits};
  for (std::uint32_t index = 1; index <= options.producers; ++index) {
    const Status registered =
        topology.register_producer(ProducerId{index}, kIncarnation, "producer-" + std::to_string(index));
    if (!registered.ok()) return library_refusal(kSub, registered.code(), registered.detail());
  }
  for (std::uint32_t index = 1; index <= options.consumers; ++index) {
    const Status registered = topology.register_consumer(ConsumerId{index}, kIncarnation,
                                                         "consumer-" + std::to_string(index), selection.value());
    if (!registered.ok()) return library_refusal(kSub, registered.code(), registered.detail());
  }

  const auto evaluation = evaluate_policy(topology, policy);
  if (!evaluation.ok()) {
    return library_refusal(kSub, evaluation.code(), evaluation.detail());
  }
  const auto planned = topology.planned_edge_count();
  if (!planned.ok()) {
    return library_refusal(kSub, planned.code(), planned.detail());
  }

  std::cout << "subcommand=plan selection=" << selection.value().canonical_key() << "\n";
  std::cout << "topology: shuffle=" << kShuffle.value() << " generation=" << kShuffleGeneration.value()
            << " partitions=" << options.partitions << " producers=" << options.producers
            << " consumers=" << options.consumers << " patterns=" << topology.pattern_count() << "\n";
  std::cout << describe_policy(policy);
  std::cout << "planned_edges=" << planned.value() << " (closed form over interned patterns)\n";
  std::cout << "fan: max_fan_out=" << evaluation.value().max_fan_out << " max_fan_in=" << evaluation.value().max_fan_in
            << " acceptable=" << (evaluation.value().acceptable() ? "yes" : "no") << "\n";
  for (const PolicyViolation& violation : evaluation.value().violations) {
    std::cout << "violation: " << to_string(violation.code) << " " << violation.subject << ": " << violation.detail
              << "\n";
  }
  if (!evaluation.value().acceptable()) {
    return library_refusal(kSub, ErrorCode::PolicyDenied, "the envelope does not cover this topology");
  }
  std::cout << "note: grants are modelled as completing so the plan can continue; no transfer is performed\n";

  ModelledCompletion completion;
  ModelledPartitions partitions_view{topology};
  WaveScheduler scheduler{limits};
  const Status configured = scheduler.configure(policy, topology);
  if (!configured.ok()) return library_refusal(kSub, configured.code(), configured.detail());
  const SchedulingEnvironment environment{&topology, &completion, nullptr, &partitions_view, TickId{}};

  std::uint64_t edges_planned = 0;
  bool complete = false;
  for (std::uint32_t index = 0; index < options.waves; ++index) {
    const auto plan = scheduler.next_wave(environment);
    if (!plan.ok()) return library_refusal(kSub, plan.code(), plan.detail());
    const WavePlan& wave = plan.value();
    std::cout << "wave " << wave.id.value() << ": grants=" << wave.grants.size() << " examined=" << wave.examined
              << " skipped_completed=" << wave.skipped_completed << " skipped_failed=" << wave.skipped_failed
              << " skipped_in_flight=" << wave.skipped_in_flight << " skipped_unproduced=" << wave.skipped_unproduced
              << "\n";
    std::cout << "  deferred_limits=" << wave.deferred_limits
              << " deferred_source_pressure=" << wave.deferred_source_pressure
              << " deferred_destination_pressure=" << wave.deferred_destination_pressure
              << " deferred_unknown_pressure=" << wave.deferred_unknown_pressure
              << " deferred_retry_wait=" << wave.deferred_retry_wait
              << " cursor_wrapped=" << (wave.cursor_wrapped ? "yes" : "no")
              << " all_resolved=" << (wave.all_resolved ? "yes" : "no") << "\n";
    if (wave.grants.empty()) {
      complete = wave.all_resolved;
      break;
    }
    print_pairs(wave.grants);
    for (const DispatchGrant& grant : wave.grants) {
      const EdgeKey key{grant.partition, grant.partition_generation, grant.consumer};
      const Status resolved = scheduler.resolve(grant.attempt, EdgeOutcome::Completed, ErrorCode::Ok);
      if (!resolved.ok()) return library_refusal(kSub, resolved.code(), resolved.detail());
      completion.complete(key);
      ++edges_planned;
    }
  }
  std::cout << "summary: waves_printed=" << options.waves << " edges_planned=" << edges_planned
            << " planned_edges=" << planned.value() << " plan_complete=" << (complete ? "yes" : "no") << "\n";
  return 0;
}

// ---------------------------------------------------------------------------
// inspect-state: what a durable store actually reports
// ---------------------------------------------------------------------------

[[nodiscard]] const char* tail_name(JournalTailStatus tail) {
  switch (tail) {
    case JournalTailStatus::Clean: return "Clean";
    case JournalTailStatus::TornTail: return "TornTail";
    case JournalTailStatus::Corrupt: return "Corrupt";
    case JournalTailStatus::Missing: return "Missing";
  }
  return "Unknown";
}

[[nodiscard]] int command_inspect_state(const std::vector<std::string_view>& arguments) {
  constexpr std::string_view kSub{"inspect-state"};
  std::string state_directory;
  bool have_directory = false;
  std::vector<std::string_view> used;
  for (std::size_t index = 0; index < arguments.size(); ++index) {
    const std::string_view flag = arguments[index];
    if (flag == "--help" || flag == "-h") {
      print_usage();
      return 0;
    }
    if (flag != "--state-dir") {
      return usage_refusal(kSub, "unknown option: " + std::string(flag));
    }
    if (index + 1 >= arguments.size()) {
      return usage_refusal(kSub, "option " + std::string(flag) + " requires a value");
    }
    if (have_directory) {
      return usage_refusal(kSub, "option --state-dir was given more than once");
    }
    state_directory = std::string(arguments[++index]);
    have_directory = true;
  }
  if (!have_directory || state_directory.empty()) {
    return usage_refusal(kSub, "option --state-dir <dir> is required");
  }

  const std::filesystem::path directory{state_directory};
  std::error_code error;
  if (!std::filesystem::is_directory(directory, error)) {
    return library_refusal(kSub, ErrorCode::InvalidArgument, "no durable state directory at " + state_directory);
  }
  StoreConfig config;
  config.directory = directory;
  const bool has_snapshot = std::filesystem::exists(directory / config.snapshot_name, error) ||
                            std::filesystem::exists(directory / (config.snapshot_name + ".prev"), error);
  const bool has_journal = std::filesystem::exists(directory / config.journal_name, error);
  if (!has_snapshot && !has_journal) {
    return library_refusal(kSub, ErrorCode::InvalidArgument,
                           "the directory holds no " + config.snapshot_name + " and no " + config.journal_name);
  }

  DurableStore store{config};
  const Status opened = store.open();
  if (!opened.ok()) return library_refusal(kSub, opened.code(), opened.detail());
  const RecoveryReport& recovery = store.recovery();

  std::cout << "subcommand=inspect-state directory=" << state_directory << "\n";
  std::cout << "snapshot_loaded=" << (recovery.snapshot_loaded ? "yes" : "no")
            << " snapshot_from_previous=" << (recovery.snapshot_from_previous ? "yes" : "no")
            << " snapshot_sequence=" << recovery.snapshot_sequence << "\n";
  std::cout << "journal_missing=" << (recovery.journal_missing ? "yes" : "no")
            << " records_replayed=" << recovery.records_replayed << " records_skipped=" << recovery.records_skipped
            << "\n";
  std::cout << "tail=" << tail_name(recovery.tail) << " bytes_discarded=" << recovery.bytes_discarded
            << " revalidation_required=" << (recovery.revalidation_required ? "yes" : "no") << "\n";
  std::cout << "last_sequence=" << store.last_sequence() << " journal_bytes=" << store.journal_bytes()
            << " appended_records=" << store.appended_records() << "\n";
  std::cout << "note: DurableStore has no read-only open; recovery repairs a torn tail by truncating it, which is\n"
            << "      what bytes_discarded reports here\n";

  const RecordKind kinds[] = {RecordKind::EpochAdvanced, RecordKind::ShuffleState, RecordKind::Participant,
                              RecordKind::Manifest,      RecordKind::Commit,       RecordKind::Failure};
  const auto count_kind = [&store](RecordKind kind) {
    std::uint64_t count = 0;
    for (const std::vector<std::byte>& record : store.records()) {
      ByteReader reader{record, Limits{}, "journal record"};
      const auto found = peek_record_kind(reader);
      if (found.ok() && found.value() == kind) {
        ++count;
      }
    }
    return count;
  };
  std::cout << "records by kind:\n";
  std::uint64_t total = 0;
  for (const RecordKind kind : kinds) {
    const std::uint64_t count = count_kind(kind);
    total += count;
    std::cout << "  " << to_string(kind) << "=" << count << "\n";
  }
  std::uint64_t undecodable = 0;
  for (const std::vector<std::byte>& record : store.records()) {
    ByteReader reader{record, Limits{}, "journal record"};
    if (!peek_record_kind(reader).ok()) {
      ++undecodable;
    }
  }
  std::cout << "  undecodable=" << undecodable << "\n";
  std::cout << "records_total=" << store.records().size() << " records_classified=" << total + undecodable << "\n";
  return 0;
}

// ---------------------------------------------------------------------------
// progress / explain / status: real loopback queries through CoordinatorClient
// ---------------------------------------------------------------------------
//
// The inspection session is a real protocol session: one handshake binds a
// session identity, and the three read-only requests below are answered by the
// coordinator behind the server's serialising mutex. A connection that is
// merely accepted proves nothing, so nothing is reported before a reply that
// carries an answer has actually been decoded.

struct RemoteOptions {
  std::string host{"127.0.0.1"};
  std::uint16_t port{0};
  std::uint32_t connect_timeout_ms{5000};
  std::uint32_t participant_id{1};
  std::uint32_t incarnation{1};
};

// Splits host:port with the same bounded decimal rule as every other value.
[[nodiscard]] bool split_endpoint(const std::string& text, std::string& host, std::uint16_t& port) {
  const std::size_t colon = text.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 >= text.size()) return false;
  std::uint32_t parsed = 0;
  if (!parse_u32(text.substr(colon + 1), 1, 65535, parsed)) return false;
  host = text.substr(0, colon);
  port = static_cast<std::uint16_t>(parsed);
  return true;
}

void print_progress(const ProgressSnapshot& progress) {
  std::cout << "progress: required=" << progress.edges_required << " completed=" << progress.edges_completed
            << " failed=" << progress.edges_failed << " incomplete=" << progress.edges_incomplete << "\n";
  std::cout << "progress: partitions_total=" << progress.partitions_total
            << " partitions_committed=" << progress.partitions_committed
            << " partitions_failed=" << progress.partitions_failed
            << " partitions_incomplete=" << progress.partitions_incomplete << "\n";
  std::cout << "progress: bytes_committed=" << progress.bytes_committed
            << " bytes_attempted=" << progress.bytes_attempted
            << " duplicate_commits_suppressed=" << progress.duplicate_commits_suppressed
            << " authority_refusals=" << progress.authority_refusals << "\n";
  std::cout << "progress: retriable_failures=" << progress.retriable_failures
            << " permanent_failures=" << progress.permanent_failures << " tick=" << progress.tick.value()
            << " closure=" << (progress.accounting_closes() ? "yes" : "no") << "\n";
}

void print_status(const CoordinatorStatus& status) {
  std::cout << "status: state=" << to_string(status.state) << " reason=" << to_string(status.reason)
            << " epoch=" << status.epoch << "\n";
  std::cout << "status: shuffle=" << status.shuffle.value() << " generation=" << status.shuffle_generation.value()
            << " topology_generation=" << status.topology_generation.value()
            << " policy_generation=" << status.policy_generation.value() << " tick=" << status.tick.value() << "\n";
  std::cout << "status: active_producers=" << status.active_producers
            << " active_consumers=" << status.active_consumers << " in_flight=" << status.in_flight
            << " persisted_records=" << status.persisted_records << "\n";
  std::cout << "status: durable=" << (status.durable ? "yes" : "no")
            << " revalidation_required=" << (status.revalidation_required ? "yes" : "no")
            << " history_incomplete=" << (status.history_incomplete ? "yes" : "no") << "\n";
  print_progress(status.progress);
}

[[nodiscard]] int command_remote(const std::string_view subcommand, const std::vector<std::string_view>& arguments) {
  RemoteOptions options;
  bool have_coordinator = false;
  std::vector<std::string_view> used;
  for (std::size_t index = 0; index < arguments.size(); ++index) {
    const std::string_view flag = arguments[index];
    if (flag == "--help" || flag == "-h") {
      print_usage();
      return 0;
    }
    if (flag != "--coordinator" && flag != "--connect-timeout-ms" && flag != "--participant-id" &&
        flag != "--incarnation") {
      return usage_refusal(subcommand, "unknown option: " + std::string(flag));
    }
    if (index + 1 >= arguments.size()) {
      return usage_refusal(subcommand, "option " + std::string(flag) + " requires a value");
    }
    if (std::find(used.begin(), used.end(), flag) != used.end()) {
      return usage_refusal(subcommand, "option " + std::string(flag) + " was given more than once");
    }
    used.push_back(flag);
    const std::string_view value = arguments[++index];
    if (flag == "--coordinator") {
      have_coordinator = true;
      if (!split_endpoint(std::string(value), options.host, options.port)) {
        return library_refusal(subcommand, ErrorCode::InvalidArgument,
                               "--coordinator must be host:port with a port in [1, 65535]");
      }
      continue;
    }
    const std::uint32_t highest = flag == "--connect-timeout-ms" ? kMaxConnectTimeoutMs : kMaxIdentityValue;
    std::uint32_t parsed = 0;
    if (!parse_u32(value, 1, highest, parsed)) {
      return usage_refusal(subcommand,
                           "option " + std::string(flag) + " must be an integer in [1, " + number(highest) + "]");
    }
    if (flag == "--connect-timeout-ms") options.connect_timeout_ms = parsed;
    if (flag == "--participant-id") options.participant_id = parsed;
    if (flag == "--incarnation") options.incarnation = parsed;
  }
  if (!have_coordinator) {
    return usage_refusal(subcommand, "option --coordinator <host:port> is required");
  }

  ClientOptions client_options;
  client_options.host = options.host;
  client_options.port = options.port;
  client_options.connect_timeout_ms = options.connect_timeout_ms;
  // A read-only inspection session: the handshake still needs a complete
  // identity, and the identity is what the server binds every later frame to.
  client_options.identity =
      SessionIdentity{ParticipantKind::Producer, options.participant_id, IncarnationId{options.incarnation}, 1};

  CoordinatorClient client;
  const Status connected = client.connect(client_options);
  if (!connected.ok()) return library_refusal(subcommand, connected.code(), connected.detail());
  std::cout << "subcommand=" << subcommand << " coordinator=" << options.host << ":" << options.port
            << " participant_id=" << options.participant_id << " incarnation=" << options.incarnation
            << " connect_timeout_ms=" << options.connect_timeout_ms << "\n";

  int answer = 0;
  if (subcommand == "progress") {
    const auto snapshot = client.progress();
    if (!snapshot.ok()) {
      answer = library_refusal(subcommand, snapshot.code(), snapshot.detail());
    } else {
      print_progress(snapshot.value());
    }
  } else if (subcommand == "explain") {
    const auto explanation = client.explain(kExplainSamples);
    if (!explanation.ok()) {
      answer = library_refusal(subcommand, explanation.code(), explanation.detail());
    } else {
      std::cout << explanation.value().render();
    }
  } else {
    const auto snapshot = client.status();
    if (!snapshot.ok()) {
      answer = library_refusal(subcommand, snapshot.code(), snapshot.detail());
    } else {
      print_status(snapshot.value());
    }
  }
  const Status closed = client.close();
  if (answer == 0 && !closed.ok()) {
    return library_refusal(subcommand, closed.code(), closed.detail());
  }
  return answer;
}

}  // namespace

int main(int argc, char** argv) {
  const std::vector<std::string_view> arguments(argv + 1, argv + argc);
  if (arguments.empty()) {
    print_usage();
    return 2;
  }
  const std::string_view command = arguments.front();
  const std::vector<std::string_view> rest(arguments.begin() + 1, arguments.end());
  if (command == "--help" || command == "-h") {
    print_usage();
    return 0;
  }
  if (command == "--version") {
    std::cout << "shuffle-fabric-cli " << version_string() << " version_number=" << version_number() << "\n";
    return 0;
  }
  if (command == "plan") return command_plan(rest);
  if (command == "inspect-state") return command_inspect_state(rest);
  if (command == "progress" || command == "explain" || command == "status") return command_remote(command, rest);
  std::cout << "refused: unknown subcommand: " << command << "\n";
  print_usage();
  return 2;
}
