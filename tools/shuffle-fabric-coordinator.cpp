// shuffle-fabric-coordinator: the coordinator service process.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Owns no semantics of its own: it opens (or recovers) one shuffle, wires the
// durable store as the coordinator's durability point, serves the management
// protocol on real TCP and reports what it did in deterministic single lines.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <span>
#include <string>
#include <memory>
#include <thread>
#include <vector>

#include "shuffle/fabric/coordinator.hpp"
#include "shuffle/fabric/durable_store.hpp"
#include "shuffle/fabric/hash.hpp"
#include "shuffle/fabric/service.hpp"
#include "shuffle/fabric/version.hpp"

#if defined(_WIN32)
// WIN32_LEAN_AND_MEAN is already defined for every first-party target.
#include <windows.h>
#endif

namespace {

using namespace shuffle::fabric;

struct Options {
  std::string bind_host{"127.0.0.1"};
  std::uint16_t port{0};
  std::filesystem::path state_directory{};
  bool volatile_state{false};
  std::uint32_t max_sessions{64};
  std::uint32_t worker_threads{4};
  std::filesystem::path port_file{};
  ShuffleId shuffle{1};
  ShuffleGeneration generation{1};
  std::uint32_t partitions{8};
  std::uint32_t max_fan_out{4096};
  std::uint32_t max_fan_in{4096};
  std::uint32_t global_concurrency{64};
  std::uint32_t per_source{16};
  std::uint32_t per_destination{16};
  std::uint32_t max_grants{256};
  std::uint32_t max_attempts{3};
  std::uint32_t compact_every{512};
};

[[nodiscard]] void print_usage() {
  std::printf(
      "shuffle-fabric-coordinator %s\n"
      "usage: shuffle-fabric-coordinator [options]\n"
      "  --bind HOST              listen host (default 127.0.0.1)\n"
      "  --port N                 listen port, 0 selects a free port (default 0)\n"
      "  --state-dir DIR          durable state directory; omitting it runs volatile\n"
      "  --volatile               run without durable state and say so\n"
      "  --max-sessions N         bound on concurrent sessions (default 64)\n"
      "  --worker-threads N       session worker threads (default 4)\n"
      "  --print-port-file PATH   write the bound port to PATH once listening\n"
      "  --shuffle ID             shuffle identity to open (default 1)\n"
      "  --generation N           shuffle generation (default 1)\n"
      "  --partitions N           partition count (default 8)\n"
      "  --max-fan-out N          fan-out ceiling (default 4096)\n"
      "  --max-fan-in N           fan-in ceiling (default 4096)\n"
      "  --global N               global concurrency limit (default 64)\n"
      "  --per-source N           per-producer concurrency limit (default 16)\n"
      "  --per-destination N      per-consumer concurrency limit (default 16)\n"
      "  --max-grants N           grants per wave (default 256)\n"
      "  --max-attempts N         retry budget per edge (default 3)\n"
      "  --compact-every N        snapshot after N durable records (default 512)\n"
      "  --version                print the version and exit\n"
      "  --help                   print this help and exit\n",
      version_string().c_str());
}

[[nodiscard]] bool parse_u64(const char* text, std::uint64_t& out) {
  if (text == nullptr || *text == '\0') {
    return false;
  }
  std::uint64_t value = 0;
  for (const char* cursor = text; *cursor != '\0'; ++cursor) {
    if (*cursor < '0' || *cursor > '9') {
      return false;
    }
    const std::uint64_t digit = static_cast<std::uint64_t>(*cursor - '0');
    if (value > (UINT64_MAX - digit) / 10) {
      return false;
    }
    value = value * 10 + digit;
  }
  out = value;
  return true;
}

[[nodiscard]] bool parse_options(int argc, char** argv, Options& options, bool& exit_requested) {
  exit_requested = false;
  for (int index = 1; index < argc; ++index) {
    const std::string flag = argv[index];
    const auto next = [&](const char** value) {
      if (index + 1 >= argc) {
        std::fprintf(stderr, "missing value for %s\n", flag.c_str());
        return false;
      }
      *value = argv[++index];
      return true;
    };
    const char* value = nullptr;

    if (flag == "--help") {
      print_usage();
      exit_requested = true;
      return true;
    }
    if (flag == "--version") {
      std::printf("shuffle-fabric-coordinator %s\n", version_string().c_str());
      exit_requested = true;
      return true;
    }
    if (flag == "--volatile") {
      options.volatile_state = true;
      continue;
    }
    if (flag == "--bind") {
      if (!next(&value)) {
        return false;
      }
      options.bind_host = value;
      continue;
    }
    if (flag == "--state-dir") {
      if (!next(&value)) {
        return false;
      }
      options.state_directory = value;
      continue;
    }
    if (flag == "--print-port-file") {
      if (!next(&value)) {
        return false;
      }
      options.port_file = value;
      continue;
    }

    std::uint64_t number = 0;
    if (flag == "--port" || flag == "--max-sessions" || flag == "--worker-threads" || flag == "--shuffle" ||
        flag == "--generation" || flag == "--partitions" || flag == "--max-fan-out" || flag == "--max-fan-in" ||
        flag == "--global" || flag == "--per-source" || flag == "--per-destination" || flag == "--max-grants" ||
        flag == "--max-attempts" || flag == "--compact-every") {
      if (!next(&value)) {
        return false;
      }
      if (!parse_u64(value, number)) {
        std::fprintf(stderr, "invalid numeric value for %s\n", flag.c_str());
        return false;
      }
    } else {
      std::fprintf(stderr, "unknown option: %s\n", flag.c_str());
      return false;
    }

    if (flag == "--port") {
      if (number > 65535) {
        std::fprintf(stderr, "port is out of range\n");
        return false;
      }
      options.port = static_cast<std::uint16_t>(number);
    } else if (flag == "--max-sessions") {
      options.max_sessions = static_cast<std::uint32_t>(number);
    } else if (flag == "--worker-threads") {
      options.worker_threads = static_cast<std::uint32_t>(number);
    } else if (flag == "--shuffle") {
      options.shuffle = ShuffleId{number};
    } else if (flag == "--generation") {
      options.generation = ShuffleGeneration{number};
    } else if (flag == "--partitions") {
      options.partitions = static_cast<std::uint32_t>(number);
    } else if (flag == "--max-fan-out") {
      options.max_fan_out = static_cast<std::uint32_t>(number);
    } else if (flag == "--max-fan-in") {
      options.max_fan_in = static_cast<std::uint32_t>(number);
    } else if (flag == "--global") {
      options.global_concurrency = static_cast<std::uint32_t>(number);
    } else if (flag == "--per-source") {
      options.per_source = static_cast<std::uint32_t>(number);
    } else if (flag == "--per-destination") {
      options.per_destination = static_cast<std::uint32_t>(number);
    } else if (flag == "--max-grants") {
      options.max_grants = static_cast<std::uint32_t>(number);
    } else if (flag == "--max-attempts") {
      options.max_attempts = static_cast<std::uint32_t>(number);
    } else if (flag == "--compact-every") {
      options.compact_every = static_cast<std::uint32_t>(number);
    }
  }
  return true;
}

[[nodiscard]] PolicyEnvelope make_policy(const Options& options) {
  PolicyEnvelope policy;
  policy.shuffle = options.shuffle;
  policy.shuffle_generation = options.generation;
  policy.generation = PolicyGeneration{1};
  policy.fan = FanBounds{options.max_fan_out, options.max_fan_in};
  policy.concurrency = ConcurrencyLimits{options.global_concurrency, options.per_source, options.per_destination};
  policy.waves = WaveLimits{options.max_grants, options.max_grants * 8};
  policy.retry = RetryPolicy{options.max_attempts, 0};
  policy.congestion = CongestionPolicy{80, 40, false, 20};
  return policy;
}

// The durable sink: one journal append per committed record, and a periodic
// snapshot so recovery never has to replay an unbounded journal. A failed
// snapshot is not fatal: the journal is still durable, the caller simply keeps
// replaying more of it next time.
class StoreSink final : public DurableSink {
 public:
  StoreSink(DurableStore& store, std::uint32_t compact_every)
      : store_(&store), compact_every_(compact_every) {}

  // Attached after the coordinator exists; the sink never calls back into it
  // while a mutation is being made durable, only between appends.
  void attach(Coordinator& coordinator) noexcept { coordinator_ = &coordinator; }

  [[nodiscard]] Status persist(std::span<const std::byte> record) override {
    const auto appended = store_->append(record);
    if (!appended.ok()) {
      return appended.status();
    }
    ++since_compact_;
    if (coordinator_ != nullptr && compact_every_ != 0 && since_compact_ >= compact_every_) {
      const auto snapshot = coordinator_->snapshot_payload();
      if (snapshot.ok() && !snapshot.value().empty() &&
          store_->compact(snapshot.value(), store_->last_sequence()).ok()) {
        since_compact_ = 0;
      }
    }
    return Status{};
  }

 private:
  DurableStore* store_;
  Coordinator* coordinator_{nullptr};
  std::uint32_t compact_every_{0};
  std::uint32_t since_compact_{0};
};

[[nodiscard]] const char* tail_name(JournalTailStatus tail) noexcept {
  switch (tail) {
    case JournalTailStatus::Clean: return "clean";
    case JournalTailStatus::TornTail: return "torn";
    case JournalTailStatus::Corrupt: return "corrupt";
    case JournalTailStatus::Missing: return "missing";
  }
  return "unknown";
}

std::atomic<bool> g_stop_requested{false};

#if defined(_WIN32)
BOOL WINAPI console_handler(DWORD signal) {
  if (signal == CTRL_C_EVENT || signal == CTRL_BREAK_EVENT || signal == CTRL_CLOSE_EVENT) {
    g_stop_requested.store(true);
    return TRUE;
  }
  return FALSE;
}
#endif

}  // namespace

int main(int argc, char** argv) {
  Options options;
  bool exit_requested = false;
  if (!parse_options(argc, argv, options, exit_requested)) {
    print_usage();
    return 2;
  }
  if (exit_requested) {
    return 0;
  }
  if (!options.state_directory.empty() && options.volatile_state) {
    std::fprintf(stderr, "--state-dir and --volatile are mutually exclusive\n");
    return 2;
  }

  const Limits limits{};
  const bool durable = !options.state_directory.empty();
  std::unique_ptr<DurableStore> store;
  std::unique_ptr<StoreSink> sink;
  std::unique_ptr<Coordinator> coordinator_ptr;

  if (durable) {
    StoreConfig config;
    config.directory = options.state_directory;
    config.limits = limits;
    store = std::make_unique<DurableStore>(config);
    const Status opened = store->open();
    if (!opened.ok()) {
      std::fprintf(stderr, "FAILED state-dir=%s error=%s\n", options.state_directory.string().c_str(),
                   format_error(opened.error()).c_str());
      return 3;
    }
    sink = std::make_unique<StoreSink>(*store, options.compact_every);
    coordinator_ptr = std::make_unique<Coordinator>(limits, sink.get());
    sink->attach(*coordinator_ptr);
  } else {
    coordinator_ptr = std::make_unique<Coordinator>(limits, nullptr);
  }
  Coordinator& coordinator = *coordinator_ptr;

  if (store != nullptr && (store->recovery().snapshot_loaded || !store->records().empty())) {
    std::vector<std::byte> snapshot;
    if (store->recovery().snapshot_loaded) {
      const auto read = DurableStore::read_snapshot(store->config().directory / store->config().snapshot_name, limits);
      if (!read.ok()) {
        std::fprintf(stderr, "FAILED snapshot error=%s\n", format_error(read.error()).c_str());
        return 3;
      }
      snapshot = read.value();
    }
    const Status recovered =
        coordinator.recover(snapshot, store->records(), store->recovery().revalidation_required);
    if (!recovered.ok()) {
      std::fprintf(stderr, "FAILED recovery error=%s\n", format_error(recovered.error()).c_str());
      return 3;
    }
    std::printf("RECOVERED snapshot=%llu replayed=%llu skipped=%llu discarded=%llu tail=%s revalidation=%s\n",
                static_cast<unsigned long long>(store->recovery().snapshot_sequence),
                static_cast<unsigned long long>(store->recovery().records_replayed),
                static_cast<unsigned long long>(store->recovery().records_skipped),
                static_cast<unsigned long long>(store->recovery().bytes_discarded),
                tail_name(store->recovery().tail), coordinator.revalidation_required() ? "yes" : "no");
    std::fflush(stdout);
  }

  const auto initial = coordinator.status();
  if (!initial.ok()) {
    std::fprintf(stderr, "FAILED status error=%s\n", format_error(initial.error()).c_str());
    return 3;
  }
  if (initial.value().state == ShuffleState::Closed) {
    ShuffleOpenRequest request;
    request.shuffle = options.shuffle;
    request.generation = options.generation;
    request.partition_count = options.partitions;
    request.policy = make_policy(options);
    const Status opened = coordinator.open_shuffle(request);
    if (!opened.ok()) {
      std::fprintf(stderr, "FAILED open error=%s\n", format_error(opened.error()).c_str());
      return 3;
    }
  }

  ServerOptions server_options;
  server_options.limits = limits;
  server_options.bind_host = options.bind_host;
  server_options.port = options.port;
  server_options.max_sessions = options.max_sessions;
  server_options.worker_threads = options.worker_threads;
  CoordinatorServer server{coordinator, server_options};
  const Status started = server.start();
  if (!started.ok()) {
    std::fprintf(stderr, "FAILED listen error=%s\n", format_error(started.error()).c_str());
    return 3;
  }

  if (!options.port_file.empty()) {
    std::FILE* file = nullptr;
#if defined(_MSC_VER)
    if (fopen_s(&file, options.port_file.string().c_str(), "wb") != 0) {
      file = nullptr;
    }
#else
    file = std::fopen(options.port_file.string().c_str(), "wb");
#endif
    if (file == nullptr) {
      std::fprintf(stderr, "FAILED port-file path=%s\n", options.port_file.string().c_str());
      static_cast<void>(server.stop());
      return 3;
    }
    std::fprintf(file, "%u\n", static_cast<unsigned>(server.port()));
    std::fclose(file);
  }

  const auto ready_status = coordinator.status();
  std::printf("READY port=%u epoch=%llu durable=%s state=%s revalidation=%llu in_flight=%u\n",
              static_cast<unsigned>(server.port()), static_cast<unsigned long long>(coordinator.epoch()),
              durable ? "yes" : "no", ready_status.ok() ? to_string(ready_status.value().state) : "unknown",
              static_cast<unsigned long long>(coordinator.revalidation_required() ? 1 : 0),
              static_cast<unsigned>(ready_status.ok() ? ready_status.value().in_flight : 0));
  std::fflush(stdout);

#if defined(_WIN32)
  static_cast<void>(SetConsoleCtrlHandler(console_handler, TRUE));
#endif

  // A bounded wait per iteration keeps the process responsive to a stop request
  // without busy-waiting; the service threads do the actual work.
  while (!g_stop_requested.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  const Status stopped = server.stop();
  const ServerStats stats = server.stats();
  const auto final_status = coordinator.status();
  std::printf("STOP ok=%s sessions=%llu peak=%u served=%llu refused=%llu replayed=%llu epoch=%llu committed=%llu "
              "incomplete=%llu\n",
              stopped.ok() ? "yes" : "no", static_cast<unsigned long long>(stats.accepted_sessions),
              static_cast<unsigned>(stats.peak_sessions), static_cast<unsigned long long>(stats.served_requests),
              static_cast<unsigned long long>(stats.refused_requests),
              static_cast<unsigned long long>(stats.replayed_answers),
              static_cast<unsigned long long>(coordinator.epoch()),
              static_cast<unsigned long long>(final_status.ok() ? final_status.value().progress.edges_completed : 0),
              static_cast<unsigned long long>(final_status.ok() ? final_status.value().progress.edges_incomplete : 0));
  std::fflush(stdout);
  if (!stopped.ok()) {
    return 4;
  }
  return 0;
}
