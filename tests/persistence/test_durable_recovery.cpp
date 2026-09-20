// Recovery proof obligations: what survives a process that never unwound.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// The first case below is the only one that can prove anything about abrupt
// death, and it does so by re-executing this binary: the child appends records,
// prints each acknowledged sequence, and then kills itself without running a
// single destructor. The parent trusts nothing but the sequences it read and
// the bytes it finds afterwards. No timeout, sleep or clock is used anywhere:
// the child's exit status is the only synchronisation.

#include "shuffle/fabric/durable_store.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/hash.hpp"
#include "temp_dir.hpp"
#include "test_support.hpp"

#if defined(_WIN32)
#if !defined(WIN32_LEAN_AND_MEAN)
#define WIN32_LEAN_AND_MEAN
#endif
#if !defined(NOMINMAX)
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

using shuffle::fabric::DurableStore;
using shuffle::fabric::ErrorCode;
using shuffle::fabric::JournalTailStatus;
using shuffle::fabric::Limits;
using shuffle::fabric::RecoveryReport;
using shuffle::fabric::Result;
using shuffle::fabric::Status;
using shuffle::fabric::StoreConfig;
using Bytes = std::vector<std::byte>;

constexpr std::size_t kSnapshotHeaderBytes = 72;
constexpr std::size_t kJournalHeaderBytes = 24;
constexpr std::uint32_t kJournalRecordMagic = 0x4E524A31u;
// The code the writer child kills itself with: a normal return would be 0 or 1,
// so a distinct value proves the process died without unwinding.
constexpr std::uint64_t kAbruptExitCode = 3;

void write_u32(std::span<std::byte> bytes, std::size_t offset, std::uint32_t value) {
  for (std::size_t index = 0; index < 4; ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (8u * static_cast<unsigned>(index))) & 0xFFu);
  }
}

void write_u64(std::span<std::byte> bytes, std::size_t offset, std::uint64_t value) {
  for (std::size_t index = 0; index < 8; ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (8u * static_cast<unsigned>(index))) & 0xFFu);
  }
}

[[nodiscard]] Bytes payload_for(std::uint64_t index) {
  const std::string text =
      "payload-" + std::to_string(index) + "-" + std::string(static_cast<std::size_t>(index % 7), 'x');
  Bytes bytes(text.size());
  for (std::size_t offset = 0; offset < text.size(); ++offset) {
    bytes[offset] = static_cast<std::byte>(static_cast<unsigned char>(text[offset]));
  }
  return bytes;
}

[[nodiscard]] Bytes state_for(std::uint64_t cycle) {
  const std::string text = "snapshot-state-cycle-" + std::to_string(cycle);
  Bytes bytes(text.size());
  for (std::size_t offset = 0; offset < text.size(); ++offset) {
    bytes[offset] = static_cast<std::byte>(static_cast<unsigned char>(text[offset]));
  }
  return bytes;
}

[[nodiscard]] Bytes snapshot_image(std::uint64_t sequence, std::span<const std::byte> payload) {
  const std::array<std::byte, 8> magic{std::byte{0x53}, std::byte{0x46}, std::byte{0x53}, std::byte{0x4E},
                                       std::byte{0x41}, std::byte{0x50}, std::byte{0x30}, std::byte{0x31}};
  Bytes image(kSnapshotHeaderBytes + payload.size());
  std::copy(magic.begin(), magic.end(), image.begin());
  write_u32(image, 8, 1);
  write_u32(image, 12, 0);
  write_u64(image, 16, sequence);
  write_u64(image, 24, payload.size());
  write_u32(image, 32, shuffle::fabric::crc32c(std::span<const std::byte>(image).first(32)));
  write_u32(image, 36, shuffle::fabric::crc32c(payload));
  const auto digest = shuffle::fabric::sha256(payload);
  for (std::size_t index = 0; index < digest.bytes.size(); ++index) {
    image[40 + index] = static_cast<std::byte>(digest.bytes[index]);
  }
  std::copy(payload.begin(), payload.end(), image.begin() + static_cast<std::ptrdiff_t>(kSnapshotHeaderBytes));
  return image;
}

[[nodiscard]] Bytes record_image(std::uint64_t sequence, std::span<const std::byte> payload) {
  Bytes record(kJournalHeaderBytes + payload.size());
  write_u32(record, 0, kJournalRecordMagic);
  write_u32(record, 4, static_cast<std::uint32_t>(payload.size()));
  write_u64(record, 8, sequence);
  write_u32(record, 16, shuffle::fabric::crc32c(std::span<const std::byte>(record).first(16)));
  write_u32(record, 20, shuffle::fabric::crc32c(payload));
  std::copy(payload.begin(), payload.end(), record.begin() + static_cast<std::ptrdiff_t>(kJournalHeaderBytes));
  return record;
}

[[nodiscard]] Bytes concatenate(const Bytes& first, const Bytes& second) {
  Bytes joined = first;
  joined.insert(joined.end(), second.begin(), second.end());
  return joined;
}

void write_file(const std::filesystem::path& path, std::span<const std::byte> data) {
  std::ofstream stream(path, std::ios::binary | std::ios::trunc);
  stream.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
  stream.close();
  if (!stream) {
    FAIL_TEST("could not write " + path.string());
  }
}

void append_file(const std::filesystem::path& path, std::span<const std::byte> data) {
  std::ofstream stream(path, std::ios::binary | std::ios::app);
  stream.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
  stream.close();
  if (!stream) {
    FAIL_TEST("could not append to " + path.string());
  }
}

[[nodiscard]] Bytes read_file(const std::filesystem::path& path) {
  std::ifstream stream(path, std::ios::binary);
  if (!stream) {
    FAIL_TEST("could not read " + path.string());
  }
  Bytes data;
  char byte = 0;
  while (stream.get(byte)) {
    data.push_back(static_cast<std::byte>(static_cast<unsigned char>(byte)));
  }
  return data;
}

[[nodiscard]] std::uint64_t file_size_of(const std::filesystem::path& path) {
  std::error_code error;
  const std::uintmax_t size = std::filesystem::file_size(path, error);
  if (error) {
    FAIL_TEST("could not measure " + path.string());
  }
  return static_cast<std::uint64_t>(size);
}

[[nodiscard]] StoreConfig store_config(const std::filesystem::path& directory) {
  StoreConfig config;
  config.directory = directory;
  return config;
}

[[nodiscard]] std::string tail_text(JournalTailStatus status) {
  switch (status) {
    case JournalTailStatus::Clean:
      return "Clean";
    case JournalTailStatus::TornTail:
      return "TornTail";
    case JournalTailStatus::Corrupt:
      return "Corrupt";
    case JournalTailStatus::Missing:
      return "Missing";
  }
  return "Unknown";
}

// ------------------------------------------------------- abrupt-death harness

[[noreturn]] void abrupt_terminate(int code) {
#if defined(_WIN32)
  // TerminateProcess on the process' own pseudo-handle skips destructors,
  // atexit handlers and every buffered stream, which is precisely the failure
  // this test needs to reproduce.
  ::TerminateProcess(::GetCurrentProcess(), static_cast<UINT>(code));
  ::_exit(code);  // reached only if TerminateProcess itself fails
#else
  // UNVALIDATED: no POSIX host has executed this branch in this repository.
  static_cast<void>(code);
  ::abort();
#endif
}

[[nodiscard]] std::uint64_t parse_count(const std::wstring& text) {
  std::uint64_t value = 0;
  for (const wchar_t character : text) {
    if (character < L'0' || character > L'9') {
      break;
    }
    value = (value * 10) + static_cast<std::uint64_t>(character - L'0');
  }
  return value;
}

// The child never reaches main(): the shared test harness owns main() and
// rejects arguments it does not know, so child mode is entered from a
// file-scope initializer, which is the only hook this translation unit owns.
[[noreturn]] void writer_child_main(const std::filesystem::path& directory, std::uint64_t count) {
  StoreConfig config;
  config.directory = directory;
  DurableStore store(config);
  const Status opened = store.open();
  if (!opened.ok()) {
    std::fprintf(stderr, "writer: open failed: %s\n", shuffle::fabric::format_error(opened.error()).c_str());
    abrupt_terminate(70);
  }
  for (std::uint64_t index = 1; index <= count; ++index) {
    const Result<std::uint64_t> sequence = store.append(payload_for(index));
    if (!sequence.ok()) {
      std::fprintf(stderr, "writer: append %llu failed: %s\n", static_cast<unsigned long long>(index),
                   shuffle::fabric::format_error(sequence.error()).c_str());
      abrupt_terminate(71);
    }
    // An acknowledgement is only an acknowledgement once it has left every
    // buffer this process owns.
    std::printf("%llu\n", static_cast<unsigned long long>(sequence.value()));
    std::fflush(stdout);
  }
  // No destructor, no snapshot, no orderly close: the store is left exactly as
  // a killed process leaves it.
  abrupt_terminate(static_cast<int>(kAbruptExitCode));
}

[[nodiscard]] std::vector<std::wstring> process_arguments() {
  std::vector<std::wstring> arguments;
#if defined(_WIN32)
  const std::wstring command_line = ::GetCommandLineW();
  std::wstring current;
  bool quoted = false;
  for (const wchar_t character : command_line) {
    if (character == L'"') {
      quoted = !quoted;
      continue;
    }
    if (character == L' ' && !quoted) {
      if (!current.empty()) {
        arguments.push_back(current);
        current.clear();
      }
      continue;
    }
    current.push_back(character);
  }
  if (!current.empty()) {
    arguments.push_back(current);
  }
#else
  // UNVALIDATED: Linux-only introspection of the process command line.
  std::ifstream stream("/proc/self/cmdline", std::ios::binary);
  std::wstring current;
  char character = 0;
  while (stream.get(character)) {
    if (character == '\0') {
      if (!current.empty()) {
        arguments.push_back(current);
        current.clear();
      }
      continue;
    }
    current.push_back(static_cast<wchar_t>(static_cast<unsigned char>(character)));
  }
  if (!current.empty()) {
    arguments.push_back(current);
  }
#endif
  return arguments;
}

struct WriterChildHook {
  WriterChildHook() {
    const std::vector<std::wstring> arguments = process_arguments();
    if (arguments.size() >= 4 && arguments[1] == L"--writer") {
      writer_child_main(std::filesystem::path{arguments[2]}, parse_count(arguments[3]));
    }
  }
};

const WriterChildHook g_writer_child_hook{};

struct ChildRun {
  std::uint64_t exit_code{0};
  std::vector<std::uint64_t> acknowledged;
  std::string log;
};

[[nodiscard]] std::filesystem::path self_executable_path() {
#if defined(_WIN32)
  std::wstring buffer(512, L'\0');
  for (;;) {
    const DWORD length = ::GetModuleFileNameW(nullptr, buffer.data(), static_cast<DWORD>(buffer.size()));
    if (length == 0) {
      return {};
    }
    if (static_cast<std::size_t>(length) < buffer.size()) {
      buffer.resize(length);
      return std::filesystem::path{buffer};
    }
    buffer.resize(buffer.size() * 2);
  }
#else
  // UNVALIDATED: Linux-only.
  std::array<char, 4096> buffer{};
  const ssize_t length = ::readlink("/proc/self/exe", buffer.data(), buffer.size() - 1);
  if (length <= 0) {
    return {};
  }
  buffer[static_cast<std::size_t>(length)] = '\0';
  return std::filesystem::path{std::string{buffer.data()}};
#endif
}

[[nodiscard]] Result<ChildRun> parse_child_log(const std::filesystem::path& log_path) {
  std::string text;
  {
    std::ifstream stream(log_path, std::ios::binary);
    if (!stream) {
      return make_failure<ChildRun>(ErrorCode::PersistenceFailure, "the writer child log could not be read");
    }
    text.assign(std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>());
  }
  ChildRun run;
  run.log = text;
  std::size_t position = 0;
  while (position < text.size()) {
    const std::size_t end = text.find('\n', position);
    std::string line = text.substr(position, end == std::string::npos ? std::string::npos : end - position);
    position = end == std::string::npos ? text.size() : end + 1;
    // The child's stdout is a text-mode stream, so a line may end with CRLF.
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    bool digits = !line.empty();
    for (const char character : line) {
      if (character < '0' || character > '9') {
        digits = false;
        break;
      }
    }
    if (!digits) {
      continue;  // diagnostics are not acknowledgements
    }
    std::uint64_t value = 0;
    for (const char character : line) {
      value = (value * 10) + static_cast<std::uint64_t>(character - '0');
    }
    run.acknowledged.push_back(value);
  }
  return run;
}

// Runs "<self> --writer <directory> <count>" and waits for the handle. The only
// synchronisation is the process handle and the exit code; nothing here waits
// for a duration.
[[nodiscard]] Result<ChildRun> run_writer_child(const std::filesystem::path& executable,
                                                const std::filesystem::path& directory, std::uint64_t count,
                                                const std::filesystem::path& log_path) {
#if defined(_WIN32)
  SECURITY_ATTRIBUTES attributes{};
  attributes.nLength = static_cast<DWORD>(sizeof(SECURITY_ATTRIBUTES));
  attributes.bInheritHandle = TRUE;
  const HANDLE log = ::CreateFileW(log_path.c_str(), GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
                                   &attributes, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
  if (log == INVALID_HANDLE_VALUE) {
    return make_failure<ChildRun>(ErrorCode::PersistenceFailure, "the writer child log could not be created");
  }
  STARTUPINFOW startup{};
  startup.cb = static_cast<DWORD>(sizeof(STARTUPINFOW));
  startup.dwFlags = STARTF_USESTDHANDLES;
  startup.hStdInput = ::GetStdHandle(STD_INPUT_HANDLE);
  startup.hStdOutput = log;
  startup.hStdError = log;
  std::wstring command = L"\"" + executable.wstring() + L"\" --writer \"" + directory.wstring() + L"\" " +
                         std::to_wstring(count);
  std::vector<wchar_t> command_buffer(command.begin(), command.end());
  command_buffer.push_back(L'\0');
  PROCESS_INFORMATION process{};
  const BOOL created = ::CreateProcessW(nullptr, command_buffer.data(), nullptr, nullptr, TRUE, 0, nullptr, nullptr,
                                        &startup, &process);
  ::CloseHandle(log);
  if (created == FALSE) {
    return make_failure<ChildRun>(ErrorCode::PersistenceFailure, "the writer child could not be started");
  }
  ::CloseHandle(process.hThread);
  const DWORD waited = ::WaitForSingleObject(process.hProcess, INFINITE);
  DWORD exit_code = 0;
  const BOOL read_exit = ::GetExitCodeProcess(process.hProcess, &exit_code);
  ::CloseHandle(process.hProcess);
  if (waited != WAIT_OBJECT_0 || read_exit == FALSE) {
    return make_failure<ChildRun>(ErrorCode::PersistenceFailure, "the writer child did not terminate cleanly");
  }
  Result<ChildRun> run = parse_child_log(log_path);
  if (!run.ok()) {
    return run;
  }
  run.value().exit_code = static_cast<std::uint64_t>(exit_code);
  return run;
#else
  // UNVALIDATED: no POSIX host has executed this branch in this repository.
  const int log_fd = ::open(log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (log_fd < 0) {
    return make_failure<ChildRun>(ErrorCode::PersistenceFailure, "the writer child log could not be created");
  }
  const pid_t child = ::fork();
  if (child < 0) {
    ::close(log_fd);
    return make_failure<ChildRun>(ErrorCode::PersistenceFailure, "the writer child could not be started");
  }
  if (child == 0) {
    ::dup2(log_fd, STDOUT_FILENO);
    ::dup2(log_fd, STDERR_FILENO);
    ::close(log_fd);
    const std::string directory_text = directory.string();
    const std::string count_text = std::to_string(count);
    const std::string executable_text = executable.string();
    ::execl(executable_text.c_str(), executable_text.c_str(), "--writer", directory_text.c_str(),
            count_text.c_str(), static_cast<char*>(nullptr));
    ::_exit(72);
  }
  ::close(log_fd);
  int status = 0;
  if (::waitpid(child, &status, 0) != child) {
    return make_failure<ChildRun>(ErrorCode::PersistenceFailure, "the writer child did not terminate cleanly");
  }
  Result<ChildRun> run = parse_child_log(log_path);
  if (!run.ok()) {
    return run;
  }
  run.value().exit_code = WIFEXITED(status) ? static_cast<std::uint64_t>(WEXITSTATUS(status)) : 128ull;
  return run;
#endif
}

}  // namespace

SHUFFLE_TEST(durable_recovery, abrupt_process_death_preserves_acknowledged_records) {
  shuffle::test::TempDir root("durable-recovery-abrupt");
  const std::uint64_t count = 12;
  const std::filesystem::path executable = self_executable_path();
  REQUIRE(!executable.empty());
  REQUIRE(std::filesystem::exists(executable));
  const Result<ChildRun> child = run_writer_child(executable, root.path(), count, root.file("writer.log"));
  REQUIRE_OK(child);
  if (child.value().exit_code != kAbruptExitCode) {
    FAIL_TEST("the writer child exited with " + std::to_string(child.value().exit_code) +
              " instead of dying abruptly; child log:\n" + child.value().log);
  }
  if (child.value().acknowledged.size() != static_cast<std::size_t>(count)) {
    FAIL_TEST("the writer child acknowledged " + std::to_string(child.value().acknowledged.size()) + " of " +
              std::to_string(count) + " records; exit=" + std::to_string(child.value().exit_code) + ", log (" +
              std::to_string(child.value().log.size()) + " bytes):\n" + child.value().log);
  }
  for (std::size_t index = 0; index < child.value().acknowledged.size(); ++index) {
    REQUIRE_EQ(child.value().acknowledged[index], static_cast<std::uint64_t>(index + 1));
  }

  DurableStore store(store_config(root.path()));
  REQUIRE_OK(store.open());
  const RecoveryReport& report = store.recovery();
  // A killed process can leave a torn tail, but it can never leave a corrupt
  // record: every byte it acknowledged was flushed before the acknowledgement
  // was printed, and nothing else was ever written.
  REQUIRE(report.tail == JournalTailStatus::Clean || report.tail == JournalTailStatus::TornTail);
  REQUIRE_NE(tail_text(report.tail), std::string{"Corrupt"});
  REQUIRE_EQ(report.records_replayed, count);
  REQUIRE_EQ(store.records().size(), child.value().acknowledged.size());
  for (std::size_t index = 0; index < child.value().acknowledged.size(); ++index) {
    const std::uint64_t sequence = child.value().acknowledged[index];
    if (!(store.records()[index] == payload_for(sequence))) {
      FAIL_TEST("record " + std::to_string(index) + " (sequence " + std::to_string(sequence) +
                ") did not survive the abrupt death byte-identically");
    }
  }
  REQUIRE_EQ(store.last_sequence(), count);
  // Recovery leaves a usable store, not just a readable one.
  const Result<std::uint64_t> appended = store.append(payload_for(count + 1));
  REQUIRE_OK(appended);
  REQUIRE_EQ(appended.value(), count + 1);
}

SHUFFLE_TEST(durable_recovery, deterministic_torn_tail_keeps_the_intact_prefix) {
  shuffle::test::TempDir root("durable-recovery-torn");
  const std::filesystem::path path = root.file("state.journal");
  const Bytes intact = concatenate(record_image(1, payload_for(1)), record_image(2, payload_for(2)));
  write_file(path, intact);

  // Half a payload: the header is complete and believable, the record is not.
  Bytes partial = record_image(3, payload_for(3));
  partial.resize(kJournalHeaderBytes + (partial.size() - kJournalHeaderBytes) / 2);
  append_file(path, partial);
  {
    DurableStore store(store_config(root.path()));
    REQUIRE_OK(store.open());
    const RecoveryReport& report = store.recovery();
    REQUIRE_EQ(report.records_replayed, std::uint64_t{2});
    REQUIRE_EQ(tail_text(report.tail), std::string{"TornTail"});
    REQUIRE_EQ(report.bytes_discarded, static_cast<std::uint64_t>(partial.size()));
    REQUIRE_EQ(report.last_sequence, std::uint64_t{2});
    REQUIRE(report.revalidation_required);
    REQUIRE_EQ(store.records().size(), std::size_t{2});
    REQUIRE(store.records()[0] == payload_for(1));
    REQUIRE(store.records()[1] == payload_for(2));
    REQUIRE_EQ(store.journal_bytes(), static_cast<std::uint64_t>(intact.size()));
  }
  // The refused bytes are cut away, so the next recovery is clean instead of
  // reporting the same torn tail forever.
  REQUIRE_EQ(file_size_of(path), static_cast<std::uint64_t>(intact.size()));

  // Half a header is torn for the same reason.
  const Bytes whole = record_image(4, payload_for(4));
  append_file(path, std::span<const std::byte>(whole).first(9));
  {
    DurableStore store(store_config(root.path()));
    REQUIRE_OK(store.open());
    REQUIRE_EQ(store.recovery().records_replayed, std::uint64_t{2});
    REQUIRE_EQ(tail_text(store.recovery().tail), std::string{"TornTail"});
    REQUIRE_EQ(store.recovery().bytes_discarded, std::uint64_t{9});
    REQUIRE_EQ(store.recovery().last_sequence, std::uint64_t{2});
  }
}

SHUFFLE_TEST(durable_recovery, snapshot_and_journal_overlap_skips_folded_sequences) {
  shuffle::test::TempDir root("durable-recovery-overlap");
  write_file(root.file("state.snapshot"), snapshot_image(3, state_for(3)));
  Bytes journal;
  for (std::uint64_t sequence = 1; sequence <= 5; ++sequence) {
    journal = concatenate(journal, record_image(sequence, payload_for(sequence)));
  }
  write_file(root.file("state.journal"), journal);

  DurableStore store(store_config(root.path()));
  REQUIRE_OK(store.open());
  const RecoveryReport& report = store.recovery();
  REQUIRE(report.snapshot_loaded);
  REQUIRE_FALSE(report.snapshot_from_previous);
  REQUIRE_EQ(report.snapshot_sequence, std::uint64_t{3});
  REQUIRE_EQ(report.records_replayed, std::uint64_t{2});
  REQUIRE_EQ(report.records_skipped, std::uint64_t{3});
  REQUIRE_EQ(report.bytes_discarded, std::uint64_t{0});
  REQUIRE_EQ(tail_text(report.tail), std::string{"Clean"});
  REQUIRE_EQ(report.last_sequence, std::uint64_t{5});
  REQUIRE_EQ(store.records().size(), std::size_t{2});
  REQUIRE(store.records()[0] == payload_for(4));
  REQUIRE(store.records()[1] == payload_for(5));
  const Result<std::uint64_t> appended = store.append(payload_for(6));
  REQUIRE_OK(appended);
  REQUIRE_EQ(appended.value(), std::uint64_t{6});
}

SHUFFLE_TEST(durable_recovery, corrupt_primary_snapshot_falls_back_to_the_previous) {
  shuffle::test::TempDir root("durable-recovery-previous");
  const StoreConfig config = store_config(root.path());
  const Bytes first = state_for(11);
  const Bytes second = state_for(22);
  {
    DurableStore store(config);
    REQUIRE_OK(store.open());
    REQUIRE_OK(store.append(payload_for(1)));
    REQUIRE_OK(store.compact(first, 1));
    REQUIRE_OK(store.append(payload_for(2)));
    REQUIRE_OK(store.compact(second, 2));
  }
  REQUIRE(std::filesystem::exists(root.file("state.snapshot.prev")));

  Bytes primary = read_file(root.file("state.snapshot"));
  primary[primary.size() - 1] ^= std::byte{0xFF};
  write_file(root.file("state.snapshot"), primary);

  {
    DurableStore store(config);
    REQUIRE_OK(store.open());
    const RecoveryReport& report = store.recovery();
    REQUIRE(report.snapshot_loaded);
    REQUIRE(report.snapshot_from_previous);
    REQUIRE(report.revalidation_required);
    REQUIRE_EQ(report.snapshot_sequence, std::uint64_t{1});
    REQUIRE_EQ(report.last_sequence, std::uint64_t{1});
    REQUIRE_EQ(report.records_replayed, std::uint64_t{0});
    REQUIRE(store.records().empty());
  }
  const Result<Bytes> previous = DurableStore::read_snapshot(root.file("state.snapshot.prev"), Limits{});
  REQUIRE_OK(previous);
  REQUIRE(previous.value() == first);

  // With no usable snapshot left, the open fails and says why: quietly starting
  // from an empty state would discard state the caller still needs.
  Bytes damaged = previous.value();
  damaged[0] ^= std::byte{0xFF};
  write_file(root.file("state.snapshot.prev"), damaged);
  DurableStore hopeless(config);
  REQUIRE_ERROR(hopeless.open(), ErrorCode::ChecksumMismatch);
  REQUIRE_ERROR(hopeless.append(payload_for(1)), ErrorCode::InvalidState);
}

SHUFFLE_TEST(durable_recovery, repeated_open_append_compact_cycles_keep_accounting_sane) {
  shuffle::test::TempDir root("durable-recovery-cycles");
  const StoreConfig config = store_config(root.path());
  std::uint64_t expected = 0;
  for (std::uint64_t cycle = 0; cycle < 25; ++cycle) {
    DurableStore store(config);
    REQUIRE_OK(store.open());
    const RecoveryReport& report = store.recovery();
    if (cycle == 0) {
      REQUIRE_FALSE(report.snapshot_loaded);
      REQUIRE_EQ(tail_text(report.tail), std::string{"Missing"});
      REQUIRE(report.revalidation_required);
    } else {
      REQUIRE(report.snapshot_loaded);
      REQUIRE_FALSE(report.snapshot_from_previous);
      REQUIRE_EQ(report.snapshot_sequence, expected);
      REQUIRE_EQ(report.records_replayed, std::uint64_t{0});
      REQUIRE_EQ(report.records_skipped, std::uint64_t{0});
      REQUIRE_EQ(report.bytes_discarded, std::uint64_t{0});
      REQUIRE_EQ(tail_text(report.tail), std::string{"Clean"});
      REQUIRE_FALSE(report.revalidation_required);
      REQUIRE_EQ(report.last_sequence, expected);
    }

    const Result<std::uint64_t> first = store.append(payload_for(expected + 1));
    REQUIRE_OK(first);
    REQUIRE_EQ(first.value(), expected + 1);
    const Result<std::uint64_t> second = store.append(payload_for(expected + 2));
    REQUIRE_OK(second);
    REQUIRE_EQ(second.value(), expected + 2);
    expected += 2;

    REQUIRE_OK(store.compact(state_for(cycle), expected));
    REQUIRE_EQ(store.journal_bytes(), std::uint64_t{0});
    REQUIRE_EQ(store.appended_records(), std::uint64_t{2});
    REQUIRE_EQ(store.last_sequence(), expected);
    REQUIRE_EQ(file_size_of(root.file("state.journal")), std::uint64_t{0});
    const Result<Bytes> folded = DurableStore::read_snapshot(root.file("state.snapshot"), Limits{});
    REQUIRE_OK(folded);
    REQUIRE(folded.value() == state_for(cycle));
  }

  DurableStore final_store(config);
  REQUIRE_OK(final_store.open());
  REQUIRE_EQ(final_store.recovery().snapshot_sequence, std::uint64_t{50});
  REQUIRE_EQ(final_store.recovery().last_sequence, std::uint64_t{50});
  REQUIRE_EQ(final_store.last_sequence(), std::uint64_t{50});
  REQUIRE(final_store.records().empty());
  REQUIRE_FALSE(final_store.recovery().revalidation_required);
}
