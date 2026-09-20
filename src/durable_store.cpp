// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/durable_store.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <system_error>
#include <utility>

#include "shuffle/fabric/hash.hpp"

#if defined(_WIN32)
#if !defined(WIN32_LEAN_AND_MEAN)
#define WIN32_LEAN_AND_MEAN
#endif
#if !defined(NOMINMAX)
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace shuffle::fabric {
namespace {

// Frozen on-disk geometry. These are contract, not tuning: changing any of them
// changes the meaning of every file already written.
constexpr std::size_t kSnapshotHeaderBytes = 72;
constexpr std::size_t kJournalHeaderBytes = 24;
constexpr std::uint32_t kSnapshotVersion = 1;
constexpr std::uint32_t kJournalRecordMagic = 0x4E524A31u;
constexpr std::array<std::byte, 8> kSnapshotMagic{std::byte{0x53}, std::byte{0x46}, std::byte{0x53},
                                                  std::byte{0x4E}, std::byte{0x41}, std::byte{0x50},
                                                  std::byte{0x30}, std::byte{0x31}};

// The journal is scanned through this buffer, so the resident cost of recovery
// does not grow with the file. A record payload is allocated only after its
// header has passed every bound check.
constexpr std::size_t kReadBufferBytes = 64u * 1024u;

// One read/write chunk. Keeps a single read from asking the OS for an
// unbounded transfer.
constexpr std::size_t kTransferBytes = 1u << 20;

// ---------------------------------------------------------------- byte access

[[nodiscard]] std::uint32_t load_u32(std::span<const std::byte> bytes, std::size_t offset) noexcept {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    const auto byte = std::to_integer<std::uint32_t>(bytes[offset + index]);
    value |= byte << (8u * static_cast<unsigned>(index));
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64(std::span<const std::byte> bytes, std::size_t offset) noexcept {
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    const auto byte = std::to_integer<std::uint64_t>(bytes[offset + index]);
    value |= byte << (8u * static_cast<unsigned>(index));
  }
  return value;
}

void store_u32(std::span<std::byte> bytes, std::size_t offset, std::uint32_t value) noexcept {
  for (std::size_t index = 0; index < 4; ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (8u * static_cast<unsigned>(index))) & 0xFFu);
  }
}

void store_u64(std::span<std::byte> bytes, std::size_t offset, std::uint64_t value) noexcept {
  for (std::size_t index = 0; index < 8; ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (8u * static_cast<unsigned>(index))) & 0xFFu);
  }
}

// ------------------------------------------------------------------ OS handles

// INVALID_HANDLE_VALUE is a cast to a pointer and therefore not a constant
// expression, so the sentinel is a const object rather than a constexpr one.
#if defined(_WIN32)
using NativeFile = HANDLE;
const NativeFile kInvalidFile = INVALID_HANDLE_VALUE;
#else
using NativeFile = int;
const NativeFile kInvalidFile = -1;
#endif

// RAII owner of one OS handle. Closing is idempotent because close() observes
// the same invalid sentinel the destructor does, so a handle is never closed
// twice and never leaked, whichever path leaves the scope.
class FileHandle {
 public:
  FileHandle() noexcept = default;
  explicit FileHandle(NativeFile native) noexcept : native_(native) {}
  ~FileHandle() { close(); }

  FileHandle(const FileHandle&) = delete;
  FileHandle& operator=(const FileHandle&) = delete;
  FileHandle(FileHandle&& other) noexcept : native_(other.native_) { other.native_ = kInvalidFile; }
  FileHandle& operator=(FileHandle&& other) noexcept {
    if (this != &other) {
      close();
      native_ = other.native_;
      other.native_ = kInvalidFile;
    }
    return *this;
  }

  [[nodiscard]] bool valid() const noexcept { return native_ != kInvalidFile; }
  [[nodiscard]] NativeFile native() const noexcept { return native_; }

  void close() noexcept {
    if (native_ == kInvalidFile) {
      return;
    }
#if defined(_WIN32)
    ::CloseHandle(native_);
#else
    ::close(native_);
#endif
    native_ = kInvalidFile;
  }

 private:
  NativeFile native_{kInvalidFile};
};

[[nodiscard]] Result<FileHandle> open_for_read(const std::filesystem::path& path, std::string_view what) {
#if defined(_WIN32)
  const HANDLE native = ::CreateFileW(path.c_str(), GENERIC_READ,
                                      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
                                      FILE_ATTRIBUTE_NORMAL, nullptr);
  if (native == INVALID_HANDLE_VALUE) {
    const DWORD error = ::GetLastError();
    const bool missing = error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND || error == ERROR_INVALID_NAME;
    return make_failure<FileHandle>(missing ? ErrorCode::InvalidArgument : ErrorCode::PersistenceFailure,
                                    std::string{what} + " could not be opened for reading");
  }
  return FileHandle{native};
#else
  const int native = ::open(path.c_str(), O_RDONLY);
  if (native < 0) {
    return make_failure<FileHandle>(errno == ENOENT ? ErrorCode::InvalidArgument : ErrorCode::PersistenceFailure,
                                    std::string{what} + " could not be opened for reading");
  }
  return FileHandle{native};
#endif
}

[[nodiscard]] Result<FileHandle> create_for_write(const std::filesystem::path& path, std::string_view what) {
#if defined(_WIN32)
  const HANDLE native = ::CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr, CREATE_ALWAYS,
                                      FILE_ATTRIBUTE_NORMAL, nullptr);
  if (native == INVALID_HANDLE_VALUE) {
    return make_failure<FileHandle>(ErrorCode::PersistenceFailure,
                                    std::string{what} + " could not be created for writing");
  }
  return FileHandle{native};
#else
  const int native = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (native < 0) {
    return make_failure<FileHandle>(ErrorCode::PersistenceFailure,
                                    std::string{what} + " could not be created for writing");
  }
  return FileHandle{native};
#endif
}

[[nodiscard]] Result<std::uint64_t> file_size(const FileHandle& file);

// Opens (creating if absent) the journal for appending at exactly 'offset', and
// truncates anything beyond it: the caller has just decided that those bytes
// can never become part of a valid record.
[[nodiscard]] Result<FileHandle> open_for_append(const std::filesystem::path& path, std::uint64_t offset,
                                                 std::string_view what) {
#if defined(_WIN32)
  // Sharing is deliberately maximally permissive: recovery tools, inspection
  // code and a second reader must be able to look at a live journal, and the
  // format -- not the filesystem -- is what detects two writers. Refusing to
  // share would turn an ordinary read into a hard failure on Windows.
  const HANDLE native = ::CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE,
                                      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_ALWAYS,
                                      FILE_ATTRIBUTE_NORMAL, nullptr);
  if (native == INVALID_HANDLE_VALUE) {
    return make_failure<FileHandle>(ErrorCode::PersistenceFailure,
                                    std::string{what} + " could not be opened for appending");
  }
  FileHandle file{native};
  LARGE_INTEGER position{};
  position.QuadPart = static_cast<LONGLONG>(offset);
  if (::SetFilePointerEx(file.native(), position, nullptr, FILE_BEGIN) == FALSE) {
    return make_failure<FileHandle>(ErrorCode::PersistenceFailure, std::string{what} + " could not be positioned");
  }
  const Result<std::uint64_t> size = file_size(file);
  if (!size.ok()) {
    return make_failure<FileHandle>(size.code(), size.detail());
  }
  if (size.value() > offset) {
    if (::SetEndOfFile(file.native()) == FALSE) {
      return make_failure<FileHandle>(ErrorCode::PersistenceFailure, std::string{what} + " could not be truncated");
    }
  }
  return file;
#else
  const int native = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
  if (native < 0) {
    return make_failure<FileHandle>(ErrorCode::PersistenceFailure,
                                    std::string{what} + " could not be opened for appending");
  }
  FileHandle file{native};
  if (::ftruncate(file.native(), static_cast<off_t>(offset)) != 0 ||
      ::lseek(file.native(), static_cast<off_t>(offset), SEEK_SET) < 0) {
    return make_failure<FileHandle>(ErrorCode::PersistenceFailure, std::string{what} + " could not be positioned");
  }
  return file;
#endif
}

[[nodiscard]] Result<std::uint64_t> file_size(const FileHandle& file) {
#if defined(_WIN32)
  LARGE_INTEGER size{};
  if (::GetFileSizeEx(file.native(), &size) == FALSE || size.QuadPart < 0) {
    return make_failure<std::uint64_t>(ErrorCode::PersistenceFailure, "durable file size is unavailable");
  }
  return static_cast<std::uint64_t>(size.QuadPart);
#else
  struct stat info {};
  if (::fstat(file.native(), &info) != 0 || info.st_size < 0) {
    return make_failure<std::uint64_t>(ErrorCode::PersistenceFailure, "durable file size is unavailable");
  }
  return static_cast<std::uint64_t>(info.st_size);
#endif
}

// Reads until 'destination' is full or the file ends, and reports how many
// bytes were actually produced. A short count is the torn-tail signal, not an
// error: the caller decides whether a short read is legitimate.
[[nodiscard]] Result<std::uint64_t> read_upto(FileHandle& file, std::span<std::byte> destination) {
  std::uint64_t total = 0;
  while (total < destination.size()) {
    const std::size_t remaining = destination.size() - static_cast<std::size_t>(total);
    const std::size_t chunk = std::min(remaining, kTransferBytes);
    std::size_t produced = 0;
#if defined(_WIN32)
    DWORD read = 0;
    if (::ReadFile(file.native(), destination.data() + total, static_cast<DWORD>(chunk), &read, nullptr) == FALSE) {
      return make_failure<std::uint64_t>(ErrorCode::PersistenceFailure, "durable file read failed");
    }
    produced = static_cast<std::size_t>(read);
#else
    const ssize_t received = ::read(file.native(), destination.data() + total, chunk);
    if (received < 0) {
      if (errno == EINTR) {
        continue;
      }
      return make_failure<std::uint64_t>(ErrorCode::PersistenceFailure, "durable file read failed");
    }
    produced = static_cast<std::size_t>(received);
#endif
    if (produced == 0) {
      break;
    }
    total += static_cast<std::uint64_t>(produced);
  }
  return total;
}

[[nodiscard]] Status read_exact(FileHandle& file, std::span<std::byte> destination, std::string_view what) {
  const Result<std::uint64_t> read = read_upto(file, destination);
  if (!read.ok()) {
    return read.status();
  }
  if (read.value() != static_cast<std::uint64_t>(destination.size())) {
    return Status{make_error(ErrorCode::TruncatedInput, std::string{what} + " ends in the middle of a value")};
  }
  return Status{};
}

[[nodiscard]] Status write_all(FileHandle& file, std::span<const std::byte> data, std::string_view what) {
  std::size_t written = 0;
  while (written < data.size()) {
    const std::size_t chunk = std::min(data.size() - written, kTransferBytes);
#if defined(_WIN32)
    DWORD produced = 0;
    if (::WriteFile(file.native(), data.data() + written, static_cast<DWORD>(chunk), &produced, nullptr) == FALSE) {
      return Status{make_error(ErrorCode::PersistenceFailure, std::string{what} + " could not be written")};
    }
    if (produced == 0) {
      return Status{make_error(ErrorCode::PersistenceFailure, std::string{what} + " accepted no bytes")};
    }
    written += static_cast<std::size_t>(produced);
#else
    const ssize_t produced = ::write(file.native(), data.data() + written, chunk);
    if (produced < 0) {
      if (errno == EINTR) {
        continue;
      }
      return Status{make_error(ErrorCode::PersistenceFailure, std::string{what} + " could not be written")};
    }
    written += static_cast<std::size_t>(produced);
#endif
  }
  return Status{};
}

// The durability barrier. append() and compact() report success only after this
// has reported success, which is what makes "acknowledged" mean "survives
// process death".
[[nodiscard]] Status flush_file(FileHandle& file, std::string_view what) {
#if defined(_WIN32)
  if (::FlushFileBuffers(file.native()) == FALSE) {
    return Status{make_error(ErrorCode::PersistenceFailure, std::string{what} + " could not be flushed")};
  }
#else
  if (::fsync(file.native()) != 0) {
    return Status{make_error(ErrorCode::PersistenceFailure, std::string{what} + " could not be flushed")};
  }
#endif
  return Status{};
}

// Cuts the file to 'length' and parks the write position there, so a rolled
// back append is followed by the next append and not by its own debris.
[[nodiscard]] Status truncate_file(FileHandle& file, std::uint64_t length) {
#if defined(_WIN32)
  LARGE_INTEGER position{};
  position.QuadPart = static_cast<LONGLONG>(length);
  if (::SetFilePointerEx(file.native(), position, nullptr, FILE_BEGIN) == FALSE) {
    return Status{make_error(ErrorCode::PersistenceFailure, "durable file could not be positioned")};
  }
  if (::SetEndOfFile(file.native()) == FALSE) {
    return Status{make_error(ErrorCode::PersistenceFailure, "durable file could not be truncated")};
  }
#else
  if (::ftruncate(file.native(), static_cast<off_t>(length)) != 0) {
    return Status{make_error(ErrorCode::PersistenceFailure, "durable file could not be truncated")};
  }
  if (::lseek(file.native(), static_cast<off_t>(length), SEEK_SET) < 0) {
    return Status{make_error(ErrorCode::PersistenceFailure, "durable file could not be positioned")};
  }
#endif
  return flush_file(file, "truncated file");
}

[[nodiscard]] bool path_exists(const std::filesystem::path& path) {
  std::error_code error;
  return std::filesystem::exists(path, error) && !error;
}

void remove_quietly(const std::filesystem::path& path) {
  std::error_code error;
  std::filesystem::remove(path, error);
}

// Installs 'source' as 'destination' in one step, keeping the bytes that were
// already there under 'backup'. A reader therefore sees either the old snapshot
// or the new one, never a mixture, and "<name>.prev" stays decodable.
[[nodiscard]] Status replace_file(const std::filesystem::path& source, const std::filesystem::path& destination,
                                  const std::filesystem::path& backup) {
#if defined(_WIN32)
  if (path_exists(destination)) {
    // ReplaceFileW is the only Windows primitive that swaps a file and hands
    // the replaced bytes to a backup name. A stale backup makes the call fail,
    // so it is dropped first; if the swap then fails the destination still
    // holds the previous snapshot, which is what the caller must fall back to.
    remove_quietly(backup);
    if (::ReplaceFileW(destination.c_str(), source.c_str(), backup.c_str(), REPLACEFILE_IGNORE_MERGE_ERRORS, nullptr,
                       nullptr) == FALSE) {
      return Status{make_error(ErrorCode::AtomicReplaceFailed, "ReplaceFileW could not install the new snapshot")};
    }
    return Status{};
  }
  if (::MoveFileExW(source.c_str(), destination.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH) == FALSE) {
    return Status{make_error(ErrorCode::AtomicReplaceFailed, "MoveFileExW could not install the new snapshot")};
  }
  return Status{};
#else
  // UNVALIDATED: no POSIX host has executed this branch in this repository.
  if (path_exists(destination) && ::rename(destination.c_str(), backup.c_str()) != 0) {
    return Status{make_error(ErrorCode::AtomicReplaceFailed, "the previous snapshot could not be preserved")};
  }
  if (::rename(source.c_str(), destination.c_str()) != 0) {
    return Status{make_error(ErrorCode::AtomicReplaceFailed, "the new snapshot could not be installed")};
  }
  return Status{};
#endif
}

// ------------------------------------------------------------- decoded images

struct SnapshotImage {
  std::uint64_t sequence{0};
  std::vector<std::byte> payload;
};

// Decodes a snapshot file with the frozen rules: exact size, magic, version,
// flags, declared length bounded before allocation, header CRC, payload CRC and
// payload SHA-256. Every refusal is named, because the caller has to choose
// between falling back to "<name>.prev" and failing the open.
[[nodiscard]] Result<SnapshotImage> decode_snapshot_file(const std::filesystem::path& path, const Limits& limits) {
  Result<FileHandle> handle = open_for_read(path, "snapshot");
  if (!handle.ok()) {
    return make_failure<SnapshotImage>(handle.code(), handle.detail());
  }
  const Result<std::uint64_t> size = file_size(handle.value());
  if (!size.ok()) {
    return make_failure<SnapshotImage>(size.code(), size.detail());
  }
  if (size.value() < kSnapshotHeaderBytes) {
    return make_failure<SnapshotImage>(ErrorCode::TruncatedInput, "snapshot is shorter than its header");
  }

  std::array<std::byte, kSnapshotHeaderBytes> header{};
  const Status header_read = read_exact(handle.value(), header, "snapshot header");
  if (!header_read.ok()) {
    return make_failure<SnapshotImage>(header_read.code(), header_read.detail());
  }
  if (!std::equal(kSnapshotMagic.begin(), kSnapshotMagic.end(), header.begin())) {
    return make_failure<SnapshotImage>(ErrorCode::MalformedInput, "snapshot magic does not match");
  }
  const std::uint32_t version = load_u32(header, 8);
  if (version != kSnapshotVersion) {
    return make_failure<SnapshotImage>(ErrorCode::UnsupportedVersion, "snapshot version is not supported");
  }
  const std::uint32_t flags = load_u32(header, 12);
  if (flags != 0) {
    return make_failure<SnapshotImage>(ErrorCode::MalformedInput, "snapshot flags must be zero");
  }
  const std::uint64_t sequence = load_u64(header, 16);
  const std::uint64_t payload_length = load_u64(header, 24);
  // Bounded before anything is allocated or read: a hostile length field must
  // cost the decoder nothing.
  if (payload_length > limits.max_state_bytes) {
    return make_failure<SnapshotImage>(ErrorCode::StateTooLarge, "snapshot payload exceeds max_state_bytes");
  }
  const std::uint32_t header_crc = load_u32(header, 32);
  const std::uint32_t payload_crc = load_u32(header, 36);
  if (crc32c(std::span<const std::byte>(header).first(32)) != header_crc) {
    return make_failure<SnapshotImage>(ErrorCode::ChecksumMismatch, "snapshot header checksum does not match");
  }
  const Result<std::uint64_t> expected = checked_add(kSnapshotHeaderBytes, payload_length);
  if (!expected.ok()) {
    return make_failure<SnapshotImage>(expected.code(), expected.detail());
  }
  if (size.value() < expected.value()) {
    return make_failure<SnapshotImage>(ErrorCode::TruncatedInput, "snapshot payload is shorter than declared");
  }
  if (size.value() > expected.value()) {
    return make_failure<SnapshotImage>(ErrorCode::TrailingGarbage, "snapshot has bytes after its payload");
  }
  const Result<std::size_t> length = to_size(payload_length);
  if (!length.ok()) {
    return make_failure<SnapshotImage>(length.code(), length.detail());
  }

  SnapshotImage image;
  image.sequence = sequence;
  image.payload.resize(length.value());
  const Status payload_read = read_exact(handle.value(), image.payload, "snapshot payload");
  if (!payload_read.ok()) {
    return make_failure<SnapshotImage>(payload_read.code(), payload_read.detail());
  }
  if (crc32c(image.payload) != payload_crc) {
    return make_failure<SnapshotImage>(ErrorCode::ChecksumMismatch, "snapshot payload checksum does not match");
  }
  const Digest digest = sha256(image.payload);
  for (std::size_t index = 0; index < kDigestBytes; ++index) {
    if (static_cast<std::uint8_t>(header[40 + index]) != digest.bytes[index]) {
      return make_failure<SnapshotImage>(ErrorCode::ChecksumMismatch, "snapshot payload digest does not match");
    }
  }
  return image;
}

struct JournalScan {
  std::vector<std::vector<std::byte>> records;
  std::uint64_t last_sequence{0};
  std::uint64_t replayable_bytes{0};  // end of the last complete, valid record
  std::uint64_t records_replayed{0};
  std::uint64_t records_skipped{0};
  std::uint64_t bytes_discarded{0};
  JournalTailStatus tail{JournalTailStatus::Clean};
};

// Sequential reader over a fixed buffer. Nothing larger than one record payload
// is ever resident, and a payload is allocated only after its header has been
// validated.
class JournalScanner {
 public:
  explicit JournalScanner(FileHandle file) : file_(std::move(file)) {}

  [[nodiscard]] std::uint64_t position() const noexcept { return position_; }

  // Returns false -- with 'destination' partially filled -- when the file ends
  // before the requested bytes exist. That is the torn tail, and it is data,
  // not an error.
  [[nodiscard]] Result<bool> read_full(std::span<std::byte> destination) {
    std::size_t filled = 0;
    while (filled < destination.size()) {
      if (buffer_offset_ == buffer_length_) {
        const Result<std::uint64_t> read = read_upto(file_, buffer_);
        if (!read.ok()) {
          return make_failure<bool>(read.code(), read.detail());
        }
        buffer_length_ = static_cast<std::size_t>(read.value());
        buffer_offset_ = 0;
        if (buffer_length_ == 0) {
          break;
        }
      }
      const std::size_t available = buffer_length_ - buffer_offset_;
      const std::size_t take = std::min(available, destination.size() - filled);
      std::memcpy(destination.data() + filled, buffer_.data() + buffer_offset_, take);
      buffer_offset_ += take;
      filled += take;
      position_ += static_cast<std::uint64_t>(take);
    }
    return filled == destination.size();
  }

 private:
  FileHandle file_;
  std::array<std::byte, kReadBufferBytes> buffer_{};
  std::size_t buffer_offset_{0};
  std::size_t buffer_length_{0};
  std::uint64_t position_{0};
};

// Streams the journal, adopting every record that is not a duplicate of an
// already-applied sequence. The scan stops at the first record that cannot be
// believed and reports how many bytes from that point on were refused.
[[nodiscard]] Result<JournalScan> scan_journal(const std::filesystem::path& path, const Limits& limits,
                                               std::uint64_t snapshot_sequence, bool& exists) {
  exists = false;
  Result<FileHandle> handle = open_for_read(path, "journal");
  if (!handle.ok()) {
    if (handle.code() == ErrorCode::InvalidArgument) {
      JournalScan missing;
      missing.tail = JournalTailStatus::Missing;
      return missing;
    }
    return make_failure<JournalScan>(handle.code(), handle.detail());
  }
  exists = true;
  const Result<std::uint64_t> size = file_size(handle.value());
  if (!size.ok()) {
    return make_failure<JournalScan>(size.code(), size.detail());
  }
  // A journal larger than four maximal snapshots cannot be a journal this store
  // wrote; refusing early keeps a corrupt length from becoming a memory plan.
  const Result<std::uint64_t> bound = checked_mul(limits.max_state_bytes, 4);
  if (bound.ok() && size.value() > bound.value()) {
    return make_failure<JournalScan>(ErrorCode::StateTooLarge, "journal exceeds four times max_state_bytes");
  }

  JournalScanner scanner{handle.take()};
  std::array<std::byte, kJournalHeaderBytes> header{};
  JournalScan scan;
  std::uint64_t last_seen = 0;
  bool discard = false;
  std::uint64_t discard_from = 0;

  for (;;) {
    const std::uint64_t record_offset = scanner.position();
    const Result<bool> header_read = scanner.read_full(header);
    if (!header_read.ok()) {
      return make_failure<JournalScan>(header_read.code(), header_read.detail());
    }
    if (!header_read.value()) {
      // A file that ends exactly on a boundary is clean; a header that started
      // and stopped is torn.
      if (scanner.position() != record_offset) {
        scan.tail = JournalTailStatus::TornTail;
        discard = true;
        discard_from = record_offset;
      }
      break;
    }

    const std::uint32_t magic = load_u32(header, 0);
    const std::uint32_t payload_length = load_u32(header, 4);
    const std::uint64_t sequence = load_u64(header, 8);
    const std::uint32_t header_crc = load_u32(header, 16);
    const std::uint32_t payload_crc = load_u32(header, 20);
    if (magic != kJournalRecordMagic) {
      scan.tail = JournalTailStatus::Corrupt;
      discard = true;
      discard_from = record_offset;
      break;
    }
    if (static_cast<std::uint64_t>(payload_length) > limits.max_journal_record_bytes) {
      scan.tail = JournalTailStatus::Corrupt;
      discard = true;
      discard_from = record_offset;
      break;
    }
    if (crc32c(std::span<const std::byte>(header).first(16)) != header_crc) {
      scan.tail = JournalTailStatus::Corrupt;
      discard = true;
      discard_from = record_offset;
      break;
    }

    std::vector<std::byte> payload(static_cast<std::size_t>(payload_length));
    const Result<bool> payload_read = scanner.read_full(payload);
    if (!payload_read.ok()) {
      return make_failure<JournalScan>(payload_read.code(), payload_read.detail());
    }
    if (!payload_read.value()) {
      scan.tail = JournalTailStatus::TornTail;
      discard = true;
      discard_from = record_offset;
      break;
    }
    if (crc32c(payload) != payload_crc) {
      scan.tail = JournalTailStatus::Corrupt;
      discard = true;
      discard_from = record_offset;
      break;
    }
    // Ordering is checked before duplication: a sequence that moves backwards
    // means the file was rewritten or two writers interleaved, and neither can
    // be repaired by skipping. A repeat of the last applied sequence (or of a
    // sequence the snapshot already folded in) is exactly what an interrupted
    // writer leaves behind, and is skipped.
    if (sequence < last_seen) {
      scan.tail = JournalTailStatus::Corrupt;
      discard = true;
      discard_from = record_offset;
      break;
    }
    const bool duplicate = sequence == last_seen || sequence <= snapshot_sequence;
    last_seen = sequence;
    if (duplicate) {
      ++scan.records_skipped;
    } else {
      scan.records.push_back(std::move(payload));
      ++scan.records_replayed;
    }
    scan.replayable_bytes = scanner.position();
  }

  scan.last_sequence = last_seen;
  if (discard) {
    scan.bytes_discarded = size.value() - discard_from;
  }
  return scan;
}

}  // namespace

struct DurableStore::Impl {
  explicit Impl(StoreConfig in) : config(std::move(in)) {}

  [[nodiscard]] Status open();
  [[nodiscard]] Result<std::uint64_t> append(std::span<const std::byte> payload);
  [[nodiscard]] Status compact(std::span<const std::byte> state, std::uint64_t upto_sequence);

  [[nodiscard]] std::filesystem::path primary_path() const { return config.directory / config.snapshot_name; }
  [[nodiscard]] std::filesystem::path previous_path() const {
    return config.directory / (config.snapshot_name + ".prev");
  }
  [[nodiscard]] std::filesystem::path temporary_path() const {
    return config.directory / (config.snapshot_name + ".tmp");
  }
  [[nodiscard]] std::filesystem::path journal_path() const { return config.directory / config.journal_name; }

  // The journal writer is opened lazily: a store that is opened and never
  // appended to must not create files, so that "no journal" keeps meaning "no
  // journal" across restarts.
  [[nodiscard]] Status ensure_journal() {
    if (journal.valid()) {
      return Status{};
    }
    Result<FileHandle> writer = open_for_append(journal_path(), journal_bytes, "journal");
    if (!writer.ok()) {
      return writer.status();
    }
    journal = writer.take();
    return Status{};
  }

  StoreConfig config;
  RecoveryReport report;
  std::vector<std::vector<std::byte>> records;
  FileHandle journal;
  bool opened{false};
  bool journal_degraded{false};
  std::uint64_t snapshot_sequence{0};
  std::uint64_t last_sequence{0};
  std::uint64_t journal_bytes{0};
  std::uint64_t appended_records{0};
};

Status DurableStore::Impl::open() {
  opened = false;
  journal.close();
  journal_degraded = false;
  records.clear();
  snapshot_sequence = 0;
  last_sequence = 0;
  journal_bytes = 0;
  appended_records = 0;
  report = RecoveryReport{};

  if (config.directory.empty()) {
    return Status{make_error(ErrorCode::InvalidArgument, "store directory is empty")};
  }
  std::error_code error;
  std::filesystem::create_directories(config.directory, error);
  if (!std::filesystem::is_directory(config.directory, error)) {
    return Status{make_error(ErrorCode::PersistenceFailure, "store directory could not be created")};
  }

  Result<SnapshotImage> primary = decode_snapshot_file(primary_path(), config.limits);
  if (primary.ok()) {
    snapshot_sequence = primary.value().sequence;
    report.snapshot_loaded = true;
  } else {
    Result<SnapshotImage> previous = decode_snapshot_file(previous_path(), config.limits);
    if (previous.ok()) {
      // The primary is unusable but a complete earlier snapshot exists: adopt
      // it and tell the caller that the state may be behind what was durable.
      snapshot_sequence = previous.value().sequence;
      report.snapshot_loaded = true;
      report.snapshot_from_previous = true;
    } else if (primary.code() != ErrorCode::InvalidArgument || previous.code() != ErrorCode::InvalidArgument) {
      // The primary error is the one the caller must act on; only "neither file
      // exists" is a legitimate fresh start, and leaving the store unopened is
      // what stops a caller from appending onto state nobody understood.
      return Status{primary.error()};
    }
  }
  report.snapshot_sequence = snapshot_sequence;

  bool journal_exists = false;
  Result<JournalScan> scanned = scan_journal(journal_path(), config.limits, snapshot_sequence, journal_exists);
  if (!scanned.ok()) {
    return scanned.status();
  }
  report.journal_missing = !journal_exists;
  report.tail = scanned.value().tail;
  report.records_replayed = scanned.value().records_replayed;
  report.records_skipped = scanned.value().records_skipped;
  report.bytes_discarded = scanned.value().bytes_discarded;
  records = std::move(scanned.value().records);
  last_sequence = std::max(snapshot_sequence, scanned.value().last_sequence);
  report.last_sequence = last_sequence;
  report.revalidation_required = !report.snapshot_loaded || report.snapshot_from_previous ||
                                 report.tail == JournalTailStatus::TornTail ||
                                 report.tail == JournalTailStatus::Corrupt;
  journal_bytes = scanned.value().replayable_bytes;
  if (journal_exists) {
    // Position the writer at the last complete record and drop the refused
    // tail: those bytes can never become valid again, and leaving them in place
    // would make the next append land behind garbage.
    Result<FileHandle> writer = open_for_append(journal_path(), journal_bytes, "journal");
    if (!writer.ok()) {
      return writer.status();
    }
    journal = writer.take();
  }
  opened = true;
  return Status{};
}

Result<std::uint64_t> DurableStore::Impl::append(std::span<const std::byte> payload) {
  if (!opened) {
    return make_failure<std::uint64_t>(ErrorCode::InvalidState, "store is not open");
  }
  if (journal_degraded) {
    return make_failure<std::uint64_t>(ErrorCode::PersistenceFailure,
                                       "journal writer is unusable after an earlier failure");
  }
  if (payload.size() > config.limits.max_journal_record_bytes) {
    return make_failure<std::uint64_t>(ErrorCode::OversizedInput, "record exceeds max_journal_record_bytes");
  }
  if (payload.size() > 0xFFFFFFFFull) {
    return make_failure<std::uint64_t>(ErrorCode::OversizedInput, "record does not fit the 32-bit length field");
  }
  const Result<std::uint64_t> sequence = checked_add(last_sequence, 1);
  if (!sequence.ok()) {
    return make_failure<std::uint64_t>(sequence.code(), sequence.detail());
  }

  std::array<std::byte, kJournalHeaderBytes> header{};
  store_u32(header, 0, kJournalRecordMagic);
  store_u32(header, 4, static_cast<std::uint32_t>(payload.size()));
  store_u64(header, 8, sequence.value());
  store_u32(header, 16, crc32c(std::span<const std::byte>(header).first(16)));
  store_u32(header, 20, crc32c(payload));

  const Status ready = ensure_journal();
  if (!ready.ok()) {
    return make_failure<std::uint64_t>(ready.code(), ready.detail());
  }
  const std::uint64_t offset = journal_bytes;
  Status written = write_all(journal, header, "journal record header");
  if (written.ok()) {
    written = write_all(journal, payload, "journal record payload");
  }
  if (written.ok()) {
    written = flush_file(journal, "journal record");
  }
  if (!written.ok()) {
    // The caller was told the append failed, so no sequence is consumed and the
    // file is cut back to the last acknowledged length. If even that fails the
    // writer can no longer be trusted to place the next record correctly.
    if (!truncate_file(journal, offset).ok()) {
      journal_degraded = true;
    }
    return make_failure<std::uint64_t>(ErrorCode::PersistenceFailure, written.detail());
  }
  last_sequence = sequence.value();
  journal_bytes = offset + kJournalHeaderBytes + payload.size();
  ++appended_records;
  return sequence.value();
}

Status DurableStore::Impl::compact(std::span<const std::byte> state, std::uint64_t upto_sequence) {
  if (!opened) {
    return Status{make_error(ErrorCode::InvalidState, "store is not open")};
  }
  if (state.size() > config.limits.max_state_bytes) {
    return Status{make_error(ErrorCode::StateTooLarge, "state exceeds max_state_bytes")};
  }
  if (upto_sequence < snapshot_sequence || upto_sequence > last_sequence) {
    return Status{make_error(ErrorCode::InvalidArgument,
                             "compaction sequence must lie between the snapshot and the last sequence")};
  }

  std::array<std::byte, kSnapshotHeaderBytes> header{};
  std::copy(kSnapshotMagic.begin(), kSnapshotMagic.end(), header.begin());
  store_u32(header, 8, kSnapshotVersion);
  store_u32(header, 12, 0);
  store_u64(header, 16, upto_sequence);
  store_u64(header, 24, state.size());
  store_u32(header, 32, crc32c(std::span<const std::byte>(header).first(32)));
  store_u32(header, 36, crc32c(state));
  const Digest digest = sha256(state);
  for (std::size_t index = 0; index < kDigestBytes; ++index) {
    header[40 + index] = static_cast<std::byte>(digest.bytes[index]);
  }

  const std::filesystem::path temporary = temporary_path();
  const std::filesystem::path primary = primary_path();
  const std::filesystem::path previous = previous_path();
  Result<FileHandle> created = create_for_write(temporary, "snapshot temporary file");
  if (!created.ok()) {
    return Status{make_error(created.code(), created.detail())};
  }
  FileHandle file = created.take();
  Status written = write_all(file, header, "snapshot header");
  if (written.ok()) {
    written = write_all(file, state, "snapshot payload");
  }
  if (written.ok()) {
    written = flush_file(file, "snapshot");
  }
  file.close();
  if (!written.ok()) {
    // The temporary is discarded and the previously installed snapshot is left
    // exactly where it was, so a failed compaction is a no-op for readers.
    remove_quietly(temporary);
    return Status{make_error(ErrorCode::PersistenceFailure, written.detail())};
  }

  const Status replaced = replace_file(temporary, primary, previous);
  if (!replaced.ok()) {
    remove_quietly(temporary);
    return replaced;
  }

  const Status ready = ensure_journal();
  if (!ready.ok()) {
    return Status{make_error(ErrorCode::PersistenceFailure, ready.detail())};
  }
  const Status truncated = truncate_file(journal, 0);
  if (!truncated.ok()) {
    return Status{make_error(ErrorCode::PersistenceFailure,
                             "journal could not be emptied after the snapshot was installed")};
  }
  snapshot_sequence = upto_sequence;
  journal_bytes = 0;
  return Status{};
}

DurableStore::DurableStore(StoreConfig config) : config_(std::move(config)), impl_(std::make_unique<Impl>(config_)) {}

DurableStore::~DurableStore() = default;

DurableStore::DurableStore(DurableStore&& other) noexcept
    : config_(std::move(other.config_)), impl_(std::move(other.impl_)) {}

DurableStore& DurableStore::operator=(DurableStore&& other) noexcept {
  if (this != &other) {
    impl_ = std::move(other.impl_);
    config_ = std::move(other.config_);
  }
  return *this;
}

Status DurableStore::open() {
  if (!impl_) {
    return Status{make_error(ErrorCode::InvalidState, "store has been moved from")};
  }
  return impl_->open();
}

const RecoveryReport& DurableStore::recovery() const {
  return impl_->report;
}

const std::vector<std::vector<std::byte>>& DurableStore::records() const {
  return impl_->records;
}

Result<std::uint64_t> DurableStore::append(std::span<const std::byte> payload) {
  if (!impl_) {
    return make_failure<std::uint64_t>(ErrorCode::InvalidState, "store has been moved from");
  }
  return impl_->append(payload);
}

Status DurableStore::compact(std::span<const std::byte> state, std::uint64_t upto_sequence) {
  if (!impl_) {
    return Status{make_error(ErrorCode::InvalidState, "store has been moved from")};
  }
  return impl_->compact(state, upto_sequence);
}

std::uint64_t DurableStore::last_sequence() const {
  return impl_ ? impl_->last_sequence : 0;
}

std::uint64_t DurableStore::journal_bytes() const {
  return impl_ ? impl_->journal_bytes : 0;
}

std::uint64_t DurableStore::appended_records() const {
  return impl_ ? impl_->appended_records : 0;
}

Result<std::vector<std::byte>> DurableStore::read_snapshot(const std::filesystem::path& path, const Limits& limits) {
  Result<SnapshotImage> image = decode_snapshot_file(path, limits);
  if (!image.ok()) {
    return make_failure<std::vector<std::byte>>(image.code(), image.detail());
  }
  return image.take().payload;
}

}  // namespace shuffle::fabric
