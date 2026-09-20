// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/socket.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <string>

#if defined(_WIN32)
#if !defined(WIN32_LEAN_AND_MEAN)
#define WIN32_LEAN_AND_MEAN
#endif
#if !defined(NOMINMAX)
#define NOMINMAX
#endif
#if defined(_MSC_VER)
// Platform headers are not first-party code: warnings from inside them are not
// actionable here, and this file is compiled with warnings-as-errors.
#pragma warning(push, 0)
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#if defined(_MSC_VER)
#pragma warning(pop)
#endif
#else
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace shuffle::fabric {
namespace {

using SteadyClock = std::chrono::steady_clock;

// One syscall per MiB keeps the Winsock int length parameter in range and
// matches the frame bound; a larger buffer simply takes more iterations.
constexpr std::size_t kMaxIoChunkBytes = 1u << 20;

#if defined(_WIN32)
using PlatformSocket = SOCKET;
using SockLen = int;
constexpr PlatformSocket kPlatformInvalidSocket = INVALID_SOCKET;
#else
using PlatformSocket = int;
using SockLen = socklen_t;
constexpr PlatformSocket kPlatformInvalidSocket = -1;
#endif

[[nodiscard]] constexpr SockLen sock_len(std::size_t size) noexcept {
  return static_cast<SockLen>(size);
}

[[nodiscard]] PlatformSocket to_platform(NativeSocket handle) noexcept {
  return static_cast<PlatformSocket>(handle);
}

[[nodiscard]] NativeSocket from_platform(PlatformSocket handle) noexcept {
  return static_cast<NativeSocket>(handle);
}

[[nodiscard]] int last_socket_error() noexcept {
#if defined(_WIN32)
  return WSAGetLastError();
#else
  return errno;
#endif
}

// Maps a platform error to the fabric code that names the same condition. The
// numeric platform value travels in the detail, so a diagnosis never depends on
// a localized message string.
[[nodiscard]] ErrorCode classify_io_error(int platform_error) noexcept {
#if defined(_WIN32)
  switch (platform_error) {
    case WSAECONNRESET:
    case WSAECONNABORTED:
    case WSAENOTCONN:
    case WSAESHUTDOWN:
      return ErrorCode::ConnectionClosed;
    case WSAECONNREFUSED:
    case WSAEHOSTUNREACH:
    case WSAENETUNREACH:
    case WSAENETDOWN:
    case WSAETIMEDOUT:
    case WSAENOTSOCK:
      return ErrorCode::PeerUnavailable;
    case WSAEMFILE:
    case WSAENOBUFS:
      return ErrorCode::ResourceExhausted;
    case WSAEWOULDBLOCK:
      return ErrorCode::NoWorkAvailable;
    case WSAEINTR:
      return ErrorCode::RetryDeferred;
    case WSAEINVAL:
      return ErrorCode::InvalidState;
    default:
      return ErrorCode::ConnectionFailure;
  }
#else
  switch (platform_error) {
    case ECONNRESET:
    case ECONNABORTED:
    case ENOTCONN:
    case EPIPE:
#if defined(ESHUTDOWN)
    case ESHUTDOWN:
#endif
      return ErrorCode::ConnectionClosed;
    case ECONNREFUSED:
    case EHOSTUNREACH:
    case ENETUNREACH:
    case ENETDOWN:
    case ETIMEDOUT:
    case EBADF:
      return ErrorCode::PeerUnavailable;
    case EMFILE:
    case ENFILE:
    case ENOBUFS:
    case ENOMEM:
      return ErrorCode::ResourceExhausted;
    case EWOULDBLOCK:
#if defined(EAGAIN) && (EAGAIN != EWOULDBLOCK)
    case EAGAIN:
#endif
      return ErrorCode::NoWorkAvailable;
    case EINTR:
      return ErrorCode::RetryDeferred;
    case EINVAL:
      return ErrorCode::InvalidState;
    default:
      return ErrorCode::ConnectionFailure;
  }
#endif
}

[[nodiscard]] Error socket_error(ErrorCode code, int platform_error, std::string_view operation) {
  std::string detail{operation};
  detail += " failed (platform error ";
  detail += std::to_string(platform_error);
  detail += ")";
  return Error{code, std::move(detail)};
}

[[nodiscard]] bool is_would_block(int platform_error) noexcept {
#if defined(_WIN32)
  return platform_error == WSAEWOULDBLOCK;
#else
  return platform_error == EWOULDBLOCK || platform_error == EAGAIN;
#endif
}

[[nodiscard]] bool is_interrupted(int platform_error) noexcept {
#if defined(_WIN32)
  return platform_error == WSAEINTR;
#else
  return platform_error == EINTR;
#endif
}

// SIGPIPE would turn a closed peer into process death instead of an error code.
// Windows has no SIGPIPE at all and Linux and the BSDs honour MSG_NOSIGNAL. On a
// platform without the flag (macOS) the process must ignore SIGPIPE itself: that
// is a process-wide decision this layer refuses to make on the caller's behalf.
[[nodiscard]] constexpr int send_flags() noexcept {
#if defined(MSG_NOSIGNAL)
  return MSG_NOSIGNAL;
#else
  return 0;
#endif
}

[[nodiscard]] Status validate_wait_budget(int timeout_ms) {
  if (timeout_ms < 0 || timeout_ms > kMaxWaitMs) {
    return Status{make_error(ErrorCode::InvalidArgument, "wait budget must be in [0, kMaxWaitMs]")};
  }
  return Status{};
}

[[nodiscard]] SteadyClock::time_point deadline_after(int timeout_ms) noexcept {
  return SteadyClock::now() + std::chrono::milliseconds{timeout_ms};
}

// Milliseconds left in the budget, floored at zero. Every retry inside one call
// is driven from a single deadline, so a loop can never extend the caller's
// budget by retrying.
[[nodiscard]] int remaining_ms(SteadyClock::time_point deadline) noexcept {
  const SteadyClock::time_point now = SteadyClock::now();
  if (now >= deadline) {
    return 0;
  }
  const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
  const auto capped = left > static_cast<decltype(left)>(kMaxWaitMs) ? static_cast<decltype(left)>(kMaxWaitMs) : left;
  return static_cast<int>(capped);
}

[[nodiscard]] timeval to_timeval(int milliseconds) noexcept {
  timeval budget{};
  budget.tv_sec = static_cast<decltype(budget.tv_sec)>(milliseconds / 1000);
  budget.tv_usec = static_cast<decltype(budget.tv_usec)>((milliseconds % 1000) * 1000);
  return budget;
}

// Waits until the handle is ready in the requested direction, retrying only
// while the deadline allows. A zero remaining budget becomes a poll, which is
// what "wait 0 ms" means and keeps a non-blocking caller non-blocking.
[[nodiscard]] Result<Socket::WaitResult> wait_until_ready(PlatformSocket handle, bool for_read,
                                                          SteadyClock::time_point deadline) {
  for (;;) {
    fd_set ready;
    FD_ZERO(&ready);
    FD_SET(handle, &ready);
    timeval budget = to_timeval(remaining_ms(deadline));
#if defined(_WIN32)
    const int rc = ::select(0, for_read ? &ready : nullptr, for_read ? nullptr : &ready, nullptr, &budget);
#else
    const int rc = ::select(static_cast<int>(handle) + 1, for_read ? &ready : nullptr, for_read ? nullptr : &ready,
                            nullptr, &budget);
#endif
    if (rc > 0) {
      return Socket::WaitResult::Ready;
    }
    if (rc == 0) {
      return Socket::WaitResult::TimedOut;
    }
    const int error = last_socket_error();
    if (is_interrupted(error) && SteadyClock::now() < deadline) {
      continue;
    }
    return socket_error(classify_io_error(error), error, "select");
  }
}

[[nodiscard]] Status set_nonblocking_handle(PlatformSocket handle, bool enabled) {
#if defined(_WIN32)
  u_long mode = enabled ? 1ul : 0ul;
  if (ioctlsocket(handle, FIONBIO, &mode) != 0) {
    const int error = last_socket_error();
    return Status{socket_error(classify_io_error(error), error, "ioctlsocket(FIONBIO)")};
  }
#else
  const int flags = ::fcntl(handle, F_GETFL, 0);
  if (flags < 0) {
    const int error = last_socket_error();
    return Status{socket_error(classify_io_error(error), error, "fcntl(F_GETFL)")};
  }
  const int updated = enabled ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
  if (::fcntl(handle, F_SETFL, updated) < 0) {
    const int error = last_socket_error();
    return Status{socket_error(classify_io_error(error), error, "fcntl(F_SETFL)")};
  }
#endif
  return Status{};
}

// Whole frames are written by a single send_all, so Nagle's algorithm can only
// add latency here: it would hold a small control frame back waiting for more
// data that this layer never produces in the same write.
[[nodiscard]] Status enable_tcp_nodelay(PlatformSocket handle) {
  const int enabled = 1;
#if defined(_WIN32)
  const char* option = reinterpret_cast<const char*>(&enabled);
#else
  const void* option = &enabled;
#endif
  if (::setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, option, sock_len(sizeof(enabled))) != 0) {
    const int error = last_socket_error();
    return Status{socket_error(classify_io_error(error), error, "setsockopt(TCP_NODELAY)")};
  }
  return Status{};
}

void close_platform_handle(PlatformSocket handle) noexcept {
#if defined(_WIN32)
  (void)::closesocket(handle);
#else
  (void)::close(handle);
#endif
}

// Parses the host forms this layer accepts. Anything else -- including IPv6 and
// every DNS name -- is refused instead of being handed to a resolver that could
// block for an unbounded time.
[[nodiscard]] bool parse_host(std::string_view host, in_addr& out) noexcept {
  if (host == "localhost") {
    return inet_pton(AF_INET, "127.0.0.1", &out) == 1;
  }
  if (host.empty() || host.size() >= static_cast<std::size_t>(INET_ADDRSTRLEN)) {
    return false;
  }
  char text[INET_ADDRSTRLEN] = {};
  std::memcpy(text, host.data(), host.size());
  return inet_pton(AF_INET, text, &out) == 1;
}

[[nodiscard]] Result<std::uint16_t> local_port_of(PlatformSocket handle) {
  sockaddr_in address{};
  SockLen length = sock_len(sizeof(address));
  if (::getsockname(handle, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
    const int error = last_socket_error();
    return socket_error(classify_io_error(error), error, "getsockname");
  }
  return static_cast<std::uint16_t>(ntohs(address.sin_port));
}

}  // namespace

SocketRuntime::SocketRuntime() noexcept {
#if defined(_WIN32)
  WSADATA data{};
  const int rc = WSAStartup(MAKEWORD(2, 2), &data);
  if (rc != 0) {
    code_ = ErrorCode::ConnectionFailure;
    return;
  }
#endif
  active_ = true;
}

SocketRuntime::~SocketRuntime() {
#if defined(_WIN32)
  if (active_) {
    (void)WSACleanup();
  }
#endif
  active_ = false;
}

Socket::~Socket() {
  close();
}

Socket::Socket(Socket&& other) noexcept
    : handle_(other.handle_.exchange(kInvalidSocket, std::memory_order_acq_rel)), runtime_(other.runtime_) {}

Socket& Socket::operator=(Socket&& other) noexcept {
  if (this != &other) {
    close();
    handle_.store(other.handle_.exchange(kInvalidSocket, std::memory_order_acq_rel), std::memory_order_release);
    runtime_ = other.runtime_;
  }
  return *this;
}

bool Socket::valid() const noexcept {
  return handle_.load(std::memory_order_acquire) != kInvalidSocket && runtime_ != nullptr && runtime_->active();
}

Status Socket::require_usable() const {
  if (handle_.load(std::memory_order_acquire) == kInvalidSocket) {
    return Status{make_error(ErrorCode::PeerUnavailable, "socket handle is closed")};
  }
  if (runtime_ == nullptr || !runtime_->active()) {
    return Status{make_error(ErrorCode::InvalidState, "socket runtime is not active")};
  }
  return Status{};
}

void Socket::close() noexcept {
  const NativeSocket handle = handle_.exchange(kInvalidSocket, std::memory_order_acq_rel);
  if (handle != kInvalidSocket) {
    close_platform_handle(to_platform(handle));
  }
}

// The deleted rvalue overload in the header makes "a temporary runtime" a
// compile error; this definition is the only one that exists.
Result<Socket> Socket::connect(std::string_view host, std::uint16_t port, int timeout_ms,
                               const SocketRuntime& runtime) {
  if (!runtime.active()) {
    return make_failure<Socket>(ErrorCode::InvalidState, "socket runtime is not active");
  }
  const Status budget = validate_wait_budget(timeout_ms);
  if (!budget.ok()) {
    return budget.error();
  }
  if (port == 0) {
    return make_failure<Socket>(ErrorCode::InvalidArgument, "port 0 is not a connectable endpoint");
  }
  in_addr address{};
  if (!parse_host(host, address)) {
    return make_failure<Socket>(ErrorCode::InvalidArgument, "host must be an IPv4 literal or 'localhost'");
  }

  const PlatformSocket handle = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (handle == kPlatformInvalidSocket) {
    const int error = last_socket_error();
    return socket_error(classify_io_error(error), error, "socket");
  }
  // The handle is owned from here on, so every early return below closes it.
  Socket socket{from_platform(handle), &runtime};

  Status status = set_nonblocking_handle(handle, true);
  if (!status.ok()) {
    return status.error();
  }
  status = enable_tcp_nodelay(handle);
  if (!status.ok()) {
    return status.error();
  }

  sockaddr_in endpoint{};
  endpoint.sin_family = AF_INET;
  endpoint.sin_port = htons(port);
  endpoint.sin_addr = address;

  const SteadyClock::time_point deadline = deadline_after(timeout_ms);
  if (::connect(handle, reinterpret_cast<const sockaddr*>(&endpoint), sock_len(sizeof(endpoint))) != 0) {
    const int error = last_socket_error();
    if (!is_would_block(error)) {
      return socket_error(classify_io_error(error), error, "connect");
    }
    const Result<Socket::WaitResult> ready = wait_until_ready(handle, false, deadline);
    if (!ready.ok()) {
      return ready.error();
    }
    if (ready.value() != Socket::WaitResult::Ready) {
      return make_failure<Socket>(ErrorCode::PeerUnavailable, "connect deadline expired");
    }
    int pending = 0;
    SockLen length = sock_len(sizeof(pending));
#if defined(_WIN32)
    if (::getsockopt(handle, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&pending), &length) != 0) {
#else
    if (::getsockopt(handle, SOL_SOCKET, SO_ERROR, &pending, &length) != 0) {
#endif
      const int query_error = last_socket_error();
      return socket_error(classify_io_error(query_error), query_error, "getsockopt(SO_ERROR)");
    }
    if (pending != 0) {
      return socket_error(classify_io_error(pending), pending, "connect");
    }
  }
  return std::move(socket);
}

Status Socket::set_nonblocking(bool enabled) {
  const Status usable = require_usable();
  if (!usable.ok()) {
    return usable;
  }
  return set_nonblocking_handle(to_platform(handle_.load(std::memory_order_acquire)), enabled);
}

Result<Socket::WaitResult> Socket::wait_readable(int timeout_ms) {
  const Status budget = validate_wait_budget(timeout_ms);
  if (!budget.ok()) {
    return budget.error();
  }
  const SteadyClock::time_point deadline = deadline_after(timeout_ms);
  for (;;) {
    const Status usable = require_usable();
    if (!usable.ok()) {
      return usable.error();
    }
    const PlatformSocket handle = to_platform(handle_.load(std::memory_order_acquire));
    const Result<WaitResult> ready = wait_until_ready(handle, true, deadline);
    if (!ready.ok()) {
      return ready.error();
    }
    if (ready.value() != WaitResult::Ready) {
      return ready.value();
    }
    // Readable means either data or end of stream. A non-consuming peek tells
    // them apart without taking a byte away from the reader that follows.
    char probe = 0;
    const int received = ::recv(handle, &probe, 1, MSG_PEEK);
    if (received > 0) {
      return WaitResult::Ready;
    }
    if (received == 0) {
      return WaitResult::Closed;
    }
    const int error = last_socket_error();
    if (is_would_block(error)) {
      continue;  // spurious readiness: wait again inside the same budget
    }
    const ErrorCode code = classify_io_error(error);
    if (code == ErrorCode::ConnectionClosed) {
      return WaitResult::Closed;
    }
    return socket_error(code, error, "recv(MSG_PEEK)");
  }
}

Result<Socket::WaitResult> Socket::wait_writable(int timeout_ms) {
  const Status budget = validate_wait_budget(timeout_ms);
  if (!budget.ok()) {
    return budget.error();
  }
  const Status usable = require_usable();
  if (!usable.ok()) {
    return usable.error();
  }
  const PlatformSocket handle = to_platform(handle_.load(std::memory_order_acquire));
  return wait_until_ready(handle, false, deadline_after(timeout_ms));
}

Status Socket::send_all(std::span<const std::byte> bytes, int timeout_ms) {
  const Status budget = validate_wait_budget(timeout_ms);
  if (!budget.ok()) {
    return budget;
  }
  if (bytes.empty()) {
    return Status{};  // nothing to send is not a failure
  }
  const SteadyClock::time_point deadline = deadline_after(timeout_ms);
  std::size_t sent = 0;
  for (;;) {
    const Status usable = require_usable();
    if (!usable.ok()) {
      return usable;
    }
    const PlatformSocket handle = to_platform(handle_.load(std::memory_order_acquire));
    const std::size_t chunk = std::min(bytes.size() - sent, kMaxIoChunkBytes);
    const int written =
        ::send(handle, reinterpret_cast<const char*>(bytes.data() + sent), static_cast<int>(chunk), send_flags());
    if (written > 0) {
      sent += static_cast<std::size_t>(written);
      if (sent == bytes.size()) {
        return Status{};
      }
      continue;
    }
    if (written == 0) {
      return Status{make_error(ErrorCode::ConnectionFailure, "send accepted no byte of a non-empty buffer")};
    }
    const int error = last_socket_error();
    if (is_interrupted(error)) {
      if (SteadyClock::now() < deadline) {
        continue;
      }
      return Status{socket_error(ErrorCode::RetryDeferred, error, "send")};
    }
    if (!is_would_block(error)) {
      // A transport failure is reported as itself even when part of the buffer
      // was already accepted: the caller tears the session down either way, and
      // "the peer is gone" is more useful than "the outcome is ambiguous".
      return Status{socket_error(classify_io_error(error), error, "send")};
    }
    const Result<WaitResult> ready = wait_until_ready(handle, false, deadline);
    if (!ready.ok()) {
      return ready.status();
    }
    if (ready.value() != WaitResult::Ready) {
      return sent == 0
                 ? Status{make_error(ErrorCode::RetryDeferred, "send deadline expired before any byte was accepted")}
                 : Status{make_error(ErrorCode::AmbiguousOutcome, "send deadline expired mid-frame")};
    }
  }
}

Result<std::size_t> Socket::recv_some(std::span<std::byte> out, int timeout_ms) {
  if (out.empty()) {
    return make_failure<std::size_t>(ErrorCode::InvalidArgument,
                                     "a zero-length receive cannot be told apart from end of stream");
  }
  const Status budget = validate_wait_budget(timeout_ms);
  if (!budget.ok()) {
    return budget.error();
  }
  const SteadyClock::time_point deadline = deadline_after(timeout_ms);
  for (;;) {
    const Status usable = require_usable();
    if (!usable.ok()) {
      return usable.error();
    }
    const PlatformSocket handle = to_platform(handle_.load(std::memory_order_acquire));
    const std::size_t chunk = std::min(out.size(), kMaxIoChunkBytes);
    const int received = ::recv(handle, reinterpret_cast<char*>(out.data()), static_cast<int>(chunk), 0);
    if (received > 0) {
      return static_cast<std::size_t>(received);
    }
    if (received == 0) {
      return make_failure<std::size_t>(ErrorCode::ConnectionClosed, "peer closed the stream");
    }
    const int error = last_socket_error();
    if (is_interrupted(error)) {
      if (SteadyClock::now() < deadline) {
        continue;
      }
      return make_failure<std::size_t>(ErrorCode::RetryDeferred, "receive was interrupted");
    }
    if (!is_would_block(error)) {
      return socket_error(classify_io_error(error), error, "recv");
    }
    const Result<WaitResult> ready = wait_until_ready(handle, true, deadline);
    if (!ready.ok()) {
      return ready.error();
    }
    if (ready.value() != WaitResult::Ready) {
      return make_failure<std::size_t>(ErrorCode::NoWorkAvailable, "no byte arrived within the receive budget");
    }
  }
}

Status Socket::shutdown() {
  const Status usable = require_usable();
  if (!usable.ok()) {
    return usable;
  }
  const PlatformSocket handle = to_platform(handle_.load(std::memory_order_acquire));
#if defined(_WIN32)
  const int rc = ::shutdown(handle, SD_BOTH);
#else
  const int rc = ::shutdown(handle, SHUT_RDWR);
#endif
  if (rc != 0) {
    const int error = last_socket_error();
    const ErrorCode code = classify_io_error(error);
    if (code == ErrorCode::ConnectionClosed) {
      return Status{};  // already shut down or already gone: the requested state holds
    }
    return Status{socket_error(code, error, "shutdown")};
  }
  return Status{};
}

Result<std::uint16_t> Socket::local_port() const {
  const Status usable = require_usable();
  if (!usable.ok()) {
    return usable.error();
  }
  return local_port_of(to_platform(handle_.load(std::memory_order_acquire)));
}

TcpListener::~TcpListener() {
  close();
}

TcpListener::TcpListener(TcpListener&& other) noexcept
    : handle_(other.handle_.exchange(kInvalidSocket, std::memory_order_acq_rel)), runtime_(other.runtime_) {}

TcpListener& TcpListener::operator=(TcpListener&& other) noexcept {
  if (this != &other) {
    close();
    handle_.store(other.handle_.exchange(kInvalidSocket, std::memory_order_acq_rel), std::memory_order_release);
    runtime_ = other.runtime_;
  }
  return *this;
}

bool TcpListener::valid() const noexcept {
  return handle_.load(std::memory_order_acquire) != kInvalidSocket && runtime_ != nullptr && runtime_->active();
}

void TcpListener::close() noexcept {
  const NativeSocket handle = handle_.exchange(kInvalidSocket, std::memory_order_acq_rel);
  if (handle != kInvalidSocket) {
    close_platform_handle(to_platform(handle));
  }
}

Result<TcpListener> TcpListener::bind(std::string_view host, std::uint16_t port, int backlog,
                                      const SocketRuntime& runtime) {
  if (!runtime.active()) {
    return make_failure<TcpListener>(ErrorCode::InvalidState, "socket runtime is not active");
  }
  if (backlog < 1 || backlog > kMaxListenBacklog) {
    return make_failure<TcpListener>(ErrorCode::InvalidArgument, "listen backlog must be in [1, kMaxListenBacklog]");
  }
  in_addr address{};
  if (!parse_host(host, address)) {
    return make_failure<TcpListener>(ErrorCode::InvalidArgument, "host must be an IPv4 literal or 'localhost'");
  }

  const PlatformSocket handle = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (handle == kPlatformInvalidSocket) {
    const int error = last_socket_error();
    return socket_error(classify_io_error(error), error, "socket");
  }
  // The handle is owned from here on, so every early return below closes it.
  TcpListener listener{from_platform(handle), &runtime};

#if !defined(_WIN32)
  // A listener that restarts must be able to rebind a port that is still in
  // TIME_WAIT. Windows is deliberately left alone: SO_REUSEADDR there lets an
  // unrelated process steal the port, which is worse than a bind failure.
  const int reuse = 1;
  if (::setsockopt(handle, SOL_SOCKET, SO_REUSEADDR, &reuse, sock_len(sizeof(reuse))) != 0) {
    const int error = last_socket_error();
    return socket_error(classify_io_error(error), error, "setsockopt(SO_REUSEADDR)");
  }
#endif

  sockaddr_in endpoint{};
  endpoint.sin_family = AF_INET;
  endpoint.sin_port = htons(port);
  endpoint.sin_addr = address;
  if (::bind(handle, reinterpret_cast<const sockaddr*>(&endpoint), sock_len(sizeof(endpoint))) != 0) {
    const int error = last_socket_error();
    return socket_error(classify_io_error(error), error, "bind");
  }
  if (::listen(handle, backlog) != 0) {
    const int error = last_socket_error();
    return socket_error(classify_io_error(error), error, "listen");
  }
  // A non-blocking listening socket is what makes accept() bounded: select says
  // whether a connection is pending, and the accept itself can never park.
  const Status nonblocking = set_nonblocking_handle(handle, true);
  if (!nonblocking.ok()) {
    return nonblocking.error();
  }
  return std::move(listener);
}

Status TcpListener::require_usable() const {
  if (handle_.load(std::memory_order_acquire) == kInvalidSocket) {
    return Status{make_error(ErrorCode::PeerUnavailable, "listener is closed")};
  }
  if (runtime_ == nullptr || !runtime_->active()) {
    return Status{make_error(ErrorCode::InvalidState, "socket runtime is not active")};
  }
  return Status{};
}

Result<Socket> TcpListener::accept(int timeout_ms) {
  const Status budget = validate_wait_budget(timeout_ms);
  if (!budget.ok()) {
    return budget.error();
  }
  const Status usable = require_usable();
  if (!usable.ok()) {
    return usable.error();
  }
  const SteadyClock::time_point deadline = deadline_after(timeout_ms);
  for (;;) {
    const NativeSocket raw = handle_.load(std::memory_order_acquire);
    if (raw == kInvalidSocket) {
      return make_failure<Socket>(ErrorCode::PeerUnavailable, "listener is closed");
    }
    const PlatformSocket handle = to_platform(raw);
    const Result<Socket::WaitResult> ready = wait_until_ready(handle, true, deadline);
    if (!ready.ok()) {
      // Closing the handle makes a blocked select fail; the closure is the real
      // answer, the platform symptom is not.
      if (handle_.load(std::memory_order_acquire) == kInvalidSocket) {
        return make_failure<Socket>(ErrorCode::PeerUnavailable, "listener was closed while accepting");
      }
      return ready.error();
    }
    if (ready.value() != Socket::WaitResult::Ready) {
      return make_failure<Socket>(ErrorCode::NoWorkAvailable, "no connection arrived within the accept budget");
    }

    sockaddr_in peer{};
    SockLen length = sock_len(sizeof(peer));
    const PlatformSocket connection = ::accept(handle, reinterpret_cast<sockaddr*>(&peer), &length);
    if (connection == kPlatformInvalidSocket) {
      const int error = last_socket_error();
      if (is_would_block(error) || is_interrupted(error)) {
        if (SteadyClock::now() >= deadline) {
          return make_failure<Socket>(ErrorCode::NoWorkAvailable, "no connection arrived within the accept budget");
        }
        continue;
      }
      if (handle_.load(std::memory_order_acquire) == kInvalidSocket) {
        return make_failure<Socket>(ErrorCode::PeerUnavailable, "listener was closed while accepting");
      }
      return socket_error(classify_io_error(error), error, "accept");
    }

    Socket socket{from_platform(connection), runtime_};
    // The accepted socket is made explicitly non-blocking instead of relying on
    // whether the platform inherits the listening socket's mode.
    Status status = set_nonblocking_handle(connection, true);
    if (!status.ok()) {
      return status.error();
    }
    status = enable_tcp_nodelay(connection);
    if (!status.ok()) {
      return status.error();
    }
    return std::move(socket);
  }
}

Result<std::uint16_t> TcpListener::local_port() const {
  const Status usable = require_usable();
  if (!usable.ok()) {
    return usable.error();
  }
  return local_port_of(to_platform(handle_.load(std::memory_order_acquire)));
}

}  // namespace shuffle::fabric
