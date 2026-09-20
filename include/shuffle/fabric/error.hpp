// Deterministic error codes, retry classification and Result/Status carriers.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

namespace shuffle::fabric {

// Every deterministic refusal in Shuffle Fabric is named by one of these
// codes. Numeric values are part of the wire/durable contract and must not be
// renumbered: diagnostics, explanations and inspection output all rely on them.
enum class ErrorCode : std::uint16_t {
  Ok = 0,

  // 1xx -- input, decoding, formats.
  InvalidArgument = 100,
  MalformedInput = 101,
  TruncatedInput = 102,
  OversizedInput = 103,
  TrailingGarbage = 104,
  UnsupportedVersion = 105,
  ChecksumMismatch = 106,
  IntegerOverflow = 107,
  InvalidUnicode = 108,
  EmptyValue = 109,
  LimitExceeded = 110,
  InvalidState = 111,
  UnsupportedOperation = 112,
  CapabilityUnsupported = 113,
  InternalError = 114,

  // 2xx -- identity, generations, authority.
  UnknownShuffle = 200,
  UnknownPartition = 201,
  UnknownChunk = 202,
  UnknownParticipant = 203,
  StaleGeneration = 204,
  StaleIncarnation = 205,
  StaleTopology = 206,
  StalePolicy = 207,
  StaleAttempt = 208,
  IncarnationMismatch = 209,
  GenerationMismatch = 210,
  DigestMismatch = 211,
  AuthorityDenied = 212,
  EligibilityDenied = 213,
  DivergentCommit = 214,
  LateAuthority = 215,
  TopologyMismatch = 216,
  PolicyMismatch = 217,
  ManifestInconsistent = 218,
  ParticipantNotActive = 219,
  DuplicateRegistration = 220,
  PartitionNotOwned = 221,
  InterestNotFound = 222,
  PartitionNotProduced = 223,

  // 3xx -- policy envelope, scheduling and lifecycle.
  FanOutCeilingExceeded = 300,
  FanInCeilingExceeded = 301,
  ConcurrencyLimitExceeded = 302,
  PolicyDenied = 303,
  BackpressureActive = 304,
  BackpressureUnknown = 305,
  NoWorkAvailable = 306,
  WaveLimitReached = 307,
  Cancelled = 308,
  ShuttingDown = 309,
  AttemptSuperseded = 310,
  EdgeAlreadyDispatched = 311,
  RetryDeferred = 312,

  // 4xx -- accounting and completion.
  DuplicateCompletion = 400,
  AlreadyCompleted = 401,
  PartitionIncomplete = 402,
  IntegrityFailure = 403,
  PayloadRejected = 404,
  AccountingMismatch = 405,
  PurgedHistory = 406,
  RevalidationRequired = 407,
  NotCommitted = 408,

  // 5xx -- retry state machine.
  RetryExhausted = 500,
  RetryNotAllowed = 501,
  AmbiguousOutcome = 502,

  // 6xx -- durable state.
  PersistenceFailure = 600,
  StateCorrupt = 601,
  StateIncompatible = 602,
  StateImpossible = 603,
  AtomicReplaceFailed = 604,
  StateTooLarge = 605,
  JournalSequenceViolation = 606,

  // 7xx -- transport, sessions, protocol.
  SessionNotFound = 700,
  SessionClosed = 701,
  ProtocolViolation = 702,
  DuplicateFrame = 703,
  SequenceViolation = 704,
  FrameTypeUnsupported = 705,
  HandshakeRequired = 706,
  IdentityMismatch = 707,
  ConnectionFailure = 708,
  PeerUnavailable = 709,
  ConnectionClosed = 710,

  // 8xx -- resource discipline.
  ResourceExhausted = 800,
  QueueFull = 801,
  TooManyParticipants = 802,
  TooManyPartitions = 803,
  TooManyChunks = 804,
  TooManyPatterns = 805,
  TooManyTrackedEdges = 806,
};

// Stable, human-readable name of a code. Never returns nullptr.
[[nodiscard]] const char* to_string(ErrorCode code) noexcept;

// Retry classification is a property of the code, not of the caller. The
// transfer retry state machine only re-attempts Retriable codes, reports
// Deferred codes as scheduling pressure, refuses to retry Authority codes
// (they require fresh authority, not another attempt) and stops on Permanent.
enum class RetryClass : std::uint8_t {
  NotApplicable = 0,
  Retriable = 1,
  Deferred = 2,
  Authority = 3,
  Permanent = 4,
};

[[nodiscard]] const char* to_string(RetryClass value) noexcept;
[[nodiscard]] RetryClass classify(ErrorCode code) noexcept;
[[nodiscard]] constexpr bool is_ok(ErrorCode code) noexcept { return code == ErrorCode::Ok; }

struct Error {
  ErrorCode code{ErrorCode::Ok};
  std::string detail{};

  Error() noexcept = default;
  Error(ErrorCode c, std::string d) : code(c), detail(std::move(d)) {}

  [[nodiscard]] bool ok() const noexcept { return code == ErrorCode::Ok; }
  [[nodiscard]] RetryClass retry_class() const noexcept { return classify(code); }
};

[[nodiscard]] inline Error make_error(ErrorCode code, std::string_view detail) {
  return Error{code, std::string(detail)};
}

// Rendered as "<CodeName>: <detail>" -- deterministic for a given code/detail.
[[nodiscard]] std::string format_error(const Error& error);

// Thrown only when a caller dereferences a failed Result (a programming error),
// never as part of normal control flow.
class ResultAccessError : public std::logic_error {
 public:
  explicit ResultAccessError(const char* what) : std::logic_error(what) {}
};

class Status {
 public:
  Status() noexcept = default;
  Status(Error error) : error_(std::move(error)) {}

  [[nodiscard]] bool ok() const noexcept { return error_.ok(); }
  explicit operator bool() const noexcept { return ok(); }
  [[nodiscard]] ErrorCode code() const noexcept { return error_.code; }
  [[nodiscard]] const std::string& detail() const noexcept { return error_.detail; }
  [[nodiscard]] const Error& error() const noexcept { return error_; }
  [[nodiscard]] RetryClass retry_class() const noexcept { return classify(error_.code); }

 private:
  Error error_{};
};

template <class T>
class Result {
 public:
  Result(T value) : storage_(std::move(value)) {}          // NOLINT(google-explicit-constructor)
  Result(Error error) : storage_(std::move(error)) {}      // NOLINT(google-explicit-constructor)

  [[nodiscard]] bool ok() const noexcept { return std::holds_alternative<T>(storage_); }
  explicit operator bool() const noexcept { return ok(); }
  [[nodiscard]] ErrorCode code() const noexcept { return ok() ? ErrorCode::Ok : std::get<Error>(storage_).code; }
  [[nodiscard]] const std::string& detail() const noexcept {
    static const std::string empty{};
    return ok() ? empty : std::get<Error>(storage_).detail;
  }
  [[nodiscard]] const Error& error() const noexcept {
    static const Error ok_error{};
    return ok() ? ok_error : std::get<Error>(storage_);
  }
  [[nodiscard]] const T& value() const {
    if (!ok()) {
      throw ResultAccessError("Result::value() called on an error result");
    }
    return std::get<T>(storage_);
  }
  [[nodiscard]] T& value() {
    if (!ok()) {
      throw ResultAccessError("Result::value() called on an error result");
    }
    return std::get<T>(storage_);
  }
  [[nodiscard]] T take() {
    if (!ok()) {
      throw ResultAccessError("Result::take() called on an error result");
    }
    return std::move(std::get<T>(storage_));
  }
  [[nodiscard]] T value_or(T fallback) const {
    return ok() ? std::get<T>(storage_) : std::move(fallback);
  }
  [[nodiscard]] Status status() const { return ok() ? Status{} : Status{error()}; }

  template <class F>
  [[nodiscard]] auto map(F&& fn) const -> Result<std::invoke_result_t<F, const T&>> {
    using U = std::invoke_result_t<F, const T&>;
    if (!ok()) {
      return Result<U>{error()};
    }
    return Result<U>{fn(std::get<T>(storage_))};
  }

 private:
  std::variant<T, Error> storage_;
};

template <class T>
[[nodiscard]] inline Result<std::decay_t<T>> make_result(T&& value) {
  return Result<std::decay_t<T>>{std::forward<T>(value)};
}

template <class T>
[[nodiscard]] inline Result<T> make_failure(ErrorCode code, std::string_view detail) {
  return Result<T>{make_error(code, detail)};
}

}  // namespace shuffle::fabric
