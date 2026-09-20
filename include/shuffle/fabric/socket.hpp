// TCP sockets for the Shuffle Fabric transport: a portable shape with a
// Windows-validated implementation.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Scope, deliberately narrow:
//   * IPv4 numeric literals ("127.0.0.1", "0.0.0.0") and the literal name
//     "localhost". Name resolution is absent on purpose: getaddrinfo can block
//     for an unbounded time and would put a resolver between the runtime and a
//     peer it was already told how to reach.
//   * Every wait carries an explicit millisecond budget, capped at kMaxWaitMs.
//     A wait budget is transport responsiveness and never protocol authority:
//     no decision in the fabric depends on how long a socket wait lasted, and no
//     liveness conclusion is drawn from one.
//   * Nothing allocates: buffers belong to the caller, and the listener backlog
//     is a bounded queue.
//
// Failures are ErrorCodes, never bare booleans: PeerUnavailable when there is no
// live peer or handle, ConnectionClosed when the peer went away, ConnectionFailure
// for a platform refusal that is neither, ResourceExhausted when the process is
// out of handles, InvalidArgument for a caller mistake (bad host, bad port, bad
// budget) and InvalidState when the socket outlived its SocketRuntime.
//
// Platform status: the _WIN32 branch is validated by the socket tests on this
// host. The POSIX branch implements the same contract and was written with the
// same care, but no POSIX host was available to run it here -- it is
// UNVALIDATED until a POSIX build and run has proven it.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

#include "shuffle/fabric/error.hpp"

namespace shuffle::fabric {

// A native handle. A Winsock SOCKET is an unsigned pointer-sized integer and a
// POSIX descriptor is an int, so uintptr_t holds either without dragging a
// platform header into every translation unit that includes this one.
using NativeSocket = std::uintptr_t;
inline constexpr NativeSocket kInvalidSocket = static_cast<NativeSocket>(~static_cast<NativeSocket>(0));

// Upper bound on a single wait. A caller that wants to wait longer re-enters
// with a fresh budget; refusing rather than silently clamping keeps the real
// bound visible at the call site.
inline constexpr int kMaxWaitMs = 600000;  // 10 minutes

// Largest listen backlog a caller may request. The accept queue is a kernel
// resource, so it is bounded here instead of being left to the platform's idea
// of "a lot" (SOMAXCONN is 0x7fffffff on Windows).
inline constexpr int kMaxListenBacklog = 512;

// Owns the platform socket subsystem: WSAStartup/WSACleanup on Windows, nothing
// on POSIX. One instance must outlive every Socket and TcpListener created
// against it, which is exactly the lifetime bug a socket handle would otherwise
// hide: after cleanup the handles are dead and a socket used past that point
// reports InvalidState instead of touching freed kernel state.
class SocketRuntime {
 public:
  SocketRuntime() noexcept;
  ~SocketRuntime();

  SocketRuntime(const SocketRuntime&) = delete;
  SocketRuntime& operator=(const SocketRuntime&) = delete;
  SocketRuntime(SocketRuntime&&) = delete;
  SocketRuntime& operator=(SocketRuntime&&) = delete;

  [[nodiscard]] bool active() const noexcept { return active_; }

  // Ok while active; otherwise the reason the subsystem is unusable.
  [[nodiscard]] ErrorCode code() const noexcept { return code_; }

 private:
  bool active_{false};
  ErrorCode code_{ErrorCode::Ok};
};

// A connected (or connecting) stream socket. Move-only: the handle is closed
// exactly once, by the move target or by the destructor, never twice.
//
// Thread safety: close() is the one operation that may be called from another
// thread, and its purpose is to wake an operation blocked in this object. Every
// other call is expected to come from the thread that owns the socket.
class Socket {
 public:
  Socket() noexcept = default;
  ~Socket();
  Socket(Socket&& other) noexcept;
  Socket& operator=(Socket&& other) noexcept;
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;

  // Connects to host:port and leaves the socket in non-blocking mode, because
  // every wait in this layer is an explicit select() with a bounded budget. The
  // budget covers the whole connect sequence.
  //
  // The runtime must outlive the socket it creates (a handle is only meaningful
  // while the subsystem is initialized), so passing a temporary is a compile
  // error rather than a dangling pointer.
  [[nodiscard]] static Result<Socket> connect(std::string_view host, std::uint16_t port, int timeout_ms,
                                              const SocketRuntime& runtime);
  [[nodiscard]] static Result<Socket> connect(std::string_view host, std::uint16_t port, int timeout_ms,
                                              const SocketRuntime&& runtime) = delete;

  [[nodiscard]] bool valid() const noexcept;
  [[nodiscard]] NativeSocket native() const noexcept { return handle_.load(std::memory_order_acquire); }

  [[nodiscard]] Status set_nonblocking(bool enabled);

  // Sends every byte or reports why it could not. A short send is retried, and
  // a would-block send waits for writability inside the same budget. Sending an
  // empty span is a no-op success (no bytes to send is not a failure).
  //
  // On deadline expiry the outcome depends on what already reached the peer:
  // nothing sent is RetryDeferred (the transport is simply not ready), a partial
  // frame is AmbiguousOutcome (the peer's framing state is unknown).
  [[nodiscard]] Status send_all(std::span<const std::byte> bytes, int timeout_ms);

  // Receives at least one byte, at most out.size(). A zero-length buffer is
  // InvalidArgument because a successful zero-byte receive is indistinguishable
  // from end of stream.
  [[nodiscard]] Result<std::size_t> recv_some(std::span<std::byte> out, int timeout_ms);

  // Tri-state readiness. Ready means the operation can proceed; TimedOut means
  // the budget expired first; Closed means the peer has closed or reset the
  // stream. Readability is proven by a non-consuming peek, so Closed is the
  // peer's end of stream and not a guess. Errors carry the transport failure.
  enum class WaitResult : std::uint8_t { Ready, TimedOut, Closed };

  [[nodiscard]] Result<WaitResult> wait_readable(int timeout_ms);
  [[nodiscard]] Result<WaitResult> wait_writable(int timeout_ms);

  // Refuses further sends and reports end of stream to the peer. Idempotent.
  [[nodiscard]] Status shutdown();

  // Closes the handle at most once and is safe to call from another thread to
  // wake a blocked operation on this socket.
  void close() noexcept;

  [[nodiscard]] Result<std::uint16_t> local_port() const;

 private:
  friend class TcpListener;

  Socket(NativeSocket handle, const SocketRuntime* runtime) noexcept : handle_(handle), runtime_(runtime) {}

  // PeerUnavailable when the handle is gone, InvalidState when the runtime is.
  [[nodiscard]] Status require_usable() const;

  std::atomic<NativeSocket> handle_{kInvalidSocket};
  const SocketRuntime* runtime_{nullptr};
};

// A bound, listening TCP socket. Move-only for the same reason Socket is.
class TcpListener {
 public:
  TcpListener() noexcept = default;
  ~TcpListener();
  TcpListener(TcpListener&& other) noexcept;
  TcpListener& operator=(TcpListener&& other) noexcept;
  TcpListener(const TcpListener&) = delete;
  TcpListener& operator=(const TcpListener&) = delete;

  // Binds host:port and listens with the given backlog. Port 0 asks the kernel
  // for a free port; local_port() reports which one it got. As with
  // Socket::connect, the runtime must outlive the listener and a temporary is a
  // compile error.
  [[nodiscard]] static Result<TcpListener> bind(std::string_view host, std::uint16_t port, int backlog,
                                                const SocketRuntime& runtime);
  [[nodiscard]] static Result<TcpListener> bind(std::string_view host, std::uint16_t port, int backlog,
                                                const SocketRuntime&& runtime) = delete;

  // Accepts one connection within the budget, returning a non-blocking Socket.
  // NoWorkAvailable when the deadline expires with no connection pending.
  [[nodiscard]] Result<Socket> accept(int timeout_ms);

  [[nodiscard]] Result<std::uint16_t> local_port() const;

  [[nodiscard]] bool valid() const noexcept;

  // Closes the listening handle at most once. Safe to call from another thread:
  // a blocked accept() observes the closure and returns an error rather than
  // waiting out the rest of its budget.
  void close() noexcept;

 private:
  TcpListener(NativeSocket handle, const SocketRuntime* runtime) noexcept : handle_(handle), runtime_(runtime) {}

  // PeerUnavailable when the handle is gone, InvalidState when the runtime is.
  // The handle is judged first so a listener that was never opened answers
  // "there is no peer", not "the runtime that never made it has gone away".
  [[nodiscard]] Status require_usable() const;

  std::atomic<NativeSocket> handle_{kInvalidSocket};
  const SocketRuntime* runtime_{nullptr};
};

}  // namespace shuffle::fabric
