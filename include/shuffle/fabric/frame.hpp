// Fixed framing, integrity checking, bounded stream decoding, session identity
// binding and replay suppression for the Shuffle Fabric wire protocol,
// version 1.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// A frame is exactly 40 header bytes followed by exactly payload_length payload
// bytes: no padding, no trailer and no extension space, so a reader can find the
// next frame from the header alone instead of scanning for a delimiter.
//
// Protocol version 1 layout (fixed-width little-endian fields only):
//
//   offset size field
//   0      4    magic: 'S','F','B','1' (little-endian 0x31424653)
//   4      2    protocol version, 1
//   6      2    message type
//   8      4    flags: bit 0 = Response, every other bit reserved and zero
//   12     8    session id
//   20     8    sequence, per direction, starts at 1, strictly increasing
//   28     4    payload length
//   32     4    payload CRC-32C
//   36     4    header CRC-32C over bytes [0, 36)
//   40     n    payload
//
// Field interpretation follows one order, always the same one: structure
// (length, magic), then version, then integrity (header CRC), then semantics
// (reserved flags, message type, declared length, sequence). Only the magic and
// the version are read before the header CRC passes, because until then every
// other field -- including payload_length -- is attacker-controlled input and
// must not steer a bound, an allocation or an error message.
//
// The session id travels in the header so that a frame carries its own identity
// binding: a session that receives a frame whose session id is not its own
// refuses it without decoding the payload (see ErrorCode::IdentityMismatch,
// which the session layer raises). A zero session id means "not yet bound" and
// is what a handshake request carries before the peer assigns an identity.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <span>
#include <unordered_set>
#include <vector>

#include "shuffle/fabric/bytes.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/identity.hpp"

namespace shuffle::fabric {

// 'S','F','B','1' in wire order; the low byte of the little-endian value is 'S'.
inline constexpr std::uint32_t kFrameMagic = 0x31424653u;
inline constexpr std::uint16_t kProtocolVersion = 1;
inline constexpr std::size_t kFrameHeaderBytes = 40;

// The header CRC covers the 36 bytes that precede the CRC field itself.
inline constexpr std::size_t kFrameHeaderCrcOffset = 36;

// Stable message numbers. These values are part of the wire contract: they are
// never reused and never renumbered. The unassigned numbers (4..9, 36..39 and
// 43..65535) are reserved so that a later revision can add messages without
// renumbering anything that is already deployed.
enum class MessageType : std::uint16_t {
  HandshakeRequest = 1,
  HandshakeResponse = 2,
  ErrorResponse = 3,

  OpenShuffleRequest = 10,
  OpenShuffleResponse = 11,
  RegisterParticipantRequest = 12,
  RegisterParticipantResponse = 13,
  PublishManifestRequest = 14,
  PublishManifestResponse = 15,
  NextWaveRequest = 16,
  NextWaveResponse = 17,
  CommitTransferRequest = 18,
  CommitTransferResponse = 19,
  ReportFailureRequest = 20,
  ReportFailureResponse = 21,
  CongestionIntentRequest = 22,
  CongestionIntentResponse = 23,
  CancelShuffleRequest = 24,
  CancelShuffleResponse = 25,
  ProgressRequest = 26,
  ProgressResponse = 27,
  ExplainRequest = 28,
  ExplainResponse = 29,
  CloseRequest = 30,
  CloseResponse = 31,

  // Manifest and status traffic, then bulk chunk transfer. The chunk messages
  // are numbered away from the control block on purpose: a reader can classify a
  // frame into control or bulk from its number alone, before any payload work.
  ManifestRequest = 32,
  ManifestResponse = 33,
  StatusRequest = 34,
  StatusResponse = 35,

  ChunkFetchRequest = 40,
  ChunkFetchResponse = 41,
  ChunkFetchFailure = 42,
};

// Every message type of protocol version 1 in numeric order. Session dispatch
// and validation tables are indexed by these values.
inline constexpr std::array<MessageType, 32> kMessageTypes{
    MessageType::HandshakeRequest,            MessageType::HandshakeResponse,
    MessageType::ErrorResponse,               MessageType::OpenShuffleRequest,
    MessageType::OpenShuffleResponse,         MessageType::RegisterParticipantRequest,
    MessageType::RegisterParticipantResponse, MessageType::PublishManifestRequest,
    MessageType::PublishManifestResponse,     MessageType::NextWaveRequest,
    MessageType::NextWaveResponse,            MessageType::CommitTransferRequest,
    MessageType::CommitTransferResponse,      MessageType::ReportFailureRequest,
    MessageType::ReportFailureResponse,       MessageType::CongestionIntentRequest,
    MessageType::CongestionIntentResponse,    MessageType::CancelShuffleRequest,
    MessageType::CancelShuffleResponse,       MessageType::ProgressRequest,
    MessageType::ProgressResponse,            MessageType::ExplainRequest,
    MessageType::ExplainResponse,             MessageType::CloseRequest,
    MessageType::CloseResponse,               MessageType::ManifestRequest,
    MessageType::ManifestResponse,            MessageType::StatusRequest,
    MessageType::StatusResponse,              MessageType::ChunkFetchRequest,
    MessageType::ChunkFetchResponse,          MessageType::ChunkFetchFailure,
};

// Stable name for diagnostics; never returns nullptr. A value outside the enum
// renders as "Unknown".
[[nodiscard]] const char* to_string(MessageType type) noexcept;

// True only for the values listed above. A decoder must ask this before it uses
// a message type to index anything.
[[nodiscard]] bool is_known_message_type(MessageType type) noexcept;

// Request/response classification is a property of the message number, not of
// the frame: a peer needs it before it has a frame to look at, for example to
// decide whether an incoming message may be answered. Both helpers return false
// for an unknown value.
[[nodiscard]] bool is_response(MessageType type) noexcept;
[[nodiscard]] bool is_request(MessageType type) noexcept;

// Bit 0 marks a response. Every other bit is reserved: a decoder that ignored
// them would silently accept a peer that believes it is speaking a later
// revision of this protocol.
inline constexpr std::uint32_t kFlagResponse = 0x00000001u;
inline constexpr std::uint32_t kFlagReservedMask = 0xfffffffeu;

// The header as the protocol means it, independent of byte order. payload_length
// and payload_crc32c are outputs of encode_frame: it derives both from the
// payload it is given. A caller-supplied non-zero value that disagrees is
// refused rather than silently corrected, so a stale length can never be
// transmitted as if it described the payload.
struct FrameHeader {
  std::uint16_t version{kProtocolVersion};
  MessageType type{MessageType::HandshakeRequest};
  std::uint32_t flags{0};
  SessionId session{};
  std::uint64_t sequence{0};
  std::uint32_t payload_length{0};
  std::uint32_t payload_crc32c{0};

  [[nodiscard]] bool carries_response_flag() const noexcept { return (flags & kFlagResponse) != 0; }

  friend bool operator==(const FrameHeader&, const FrameHeader&) noexcept = default;
};

// Encodes one complete frame into out (header, CRCs and payload). On failure out
// is left exactly as it was: a half-written frame is never observable.
//
// Refusals: UnsupportedVersion for a version other than 1, FrameTypeUnsupported
// for an unknown message type, MalformedInput for reserved flag bits,
// SequenceViolation for sequence 0, OversizedInput when the payload exceeds
// max_frame_payload_bytes or does not fit the 32-bit length field, LimitExceeded
// when the whole frame would exceed max_allocation_bytes, InvalidArgument when a
// caller-supplied length or CRC contradicts the payload.
[[nodiscard]] Status encode_frame(const FrameHeader& header, std::span<const std::byte> payload,
                                  std::vector<std::byte>& out, const Limits& limits);

// Decodes exactly one header: the span must be exactly kFrameHeaderBytes long.
// TruncatedInput when it is shorter, TrailingGarbage when it is longer.
[[nodiscard]] Result<FrameHeader> decode_header(std::span<const std::byte> header_bytes, const Limits& limits);

// One decoded frame. The payload span is a view into the buffer that was
// decoded and stays valid exactly as long as that buffer does (for FrameStream:
// until the next call to feed() or next()).
struct DecodedFrame {
  FrameHeader header{};
  std::span<const std::byte> payload{};
  std::size_t consumed{0};
};

// Decodes the first frame of a buffer and reports how many bytes it occupied, so
// that a stream reader can advance past exactly one frame. Bytes after the frame
// are not an error here: the caller decides whether they are another frame or
// garbage. Fails with TruncatedInput when the buffer is shorter than the frame
// the header declares, ChecksumMismatch when either CRC disagrees, and with the
// decode_header refusals above.
[[nodiscard]] Result<DecodedFrame> decode_frame_prefix(std::span<const std::byte> bytes, const Limits& limits);

// Decodes a buffer that must contain exactly one frame: a complete frame with
// bytes left over fails with TrailingGarbage.
[[nodiscard]] Result<DecodedFrame> decode_frame(std::span<const std::byte> bytes, const Limits& limits);

// Incremental reader for a byte stream (a socket, a file, a test vector).
//
// The queue never exceeds max_frame_payload_bytes + kFrameHeaderBytes. The
// declared payload length is checked as soon as the header is buffered, so a
// peer cannot make the reader reserve memory by promising a payload it never
// sends; and feed() refuses to append bytes that would push the queue past the
// same bound, so the reader stays bounded even against a peer that streams
// garbage as fast as the socket accepts it.
//
// The stream owns a copy of the bounds it was constructed with. Holding a
// reference to the caller's Limits would be the same thing with a dangling
// pointer waiting for the first caller who passes a temporary, and a bound set
// is small enough that copying it is free next to a socket read. Limits are used
// literally: validate them with Limits::validate() where they are configured.
class FrameStream {
 public:
  explicit FrameStream(const Limits& limits) noexcept : limits_(limits) {}

  // Appends received bytes. The span must not alias the stream's own buffer
  // (payloads returned by next() are views into it). Refuses with
  // ResourceExhausted, appending nothing, when the queue plus these bytes would
  // exceed the frame bound; drain with next() and feed again.
  [[nodiscard]] Status feed(std::span<const std::byte> bytes);

  // Returns the next complete frame and advances past it. NoWorkAvailable while
  // a whole frame is not buffered yet. A buffered prefix that already
  // contradicts the magic is refused as soon as that byte arrives rather than
  // after 40 bytes, and every other refusal is the decode_header refusal of the
  // buffered header. After a refusal the stream is unusable: the caller closes
  // the session instead of trying to resynchronise.
  [[nodiscard]] Result<DecodedFrame> next();

  [[nodiscard]] std::size_t buffered() const noexcept { return buffer_.size() - consumed_; }

 private:
  // Bytes the queue may hold: one maximal frame. Returns UINT64_MAX when the
  // caller's bound is so large that the sum cannot be represented, which keeps
  // the comparison in feed() total instead of wrapping.
  [[nodiscard]] std::uint64_t capacity() const noexcept;

  // Drops the consumed prefix. Called before the buffer is read or appended to,
  // never while a decoded payload span is being handed out.
  void compact();

  std::vector<std::byte> buffer_;
  std::size_t consumed_{0};
  Limits limits_;
};

// Replay and duplicate suppression for one direction of one session.
//
// The guard answers one question: has this sequence number already been used?
// It is deliberately not the ordering authority -- a session enforces
// "sequence == previous + 1" itself -- so an unseen sequence that is still
// inside the retained window is accepted (an out-of-order arrival), while an
// unseen sequence that has fallen out of the window is refused because the
// guard can no longer tell it from a replay.
//
//   sequence            result
//   0                   SequenceViolation (wire sequences start at 1)
//   already accepted    0, duplicate
//   new high-water mark 1, accepted
//   above the window,    1, accepted, unless it jumps more than 2^40 past the
//   never accepted       highest seen -> MalformedInput
//   below the window,    SequenceViolation
//   never accepted
//
// The window is a memory bound, not a policy: the guard retains at most
// min(window, kMaxReplayWindow) accepted sequences and reports the effective
// value through window().
class ReplayGuard {
 public:
  static constexpr std::uint64_t kMaxReplayWindow = 1u << 16;
  static constexpr std::uint64_t kMaxForwardJump = 1ull << 40;

  explicit ReplayGuard(std::uint64_t window = 1024) noexcept
      : window_(window < kMaxReplayWindow ? window : kMaxReplayWindow) {}

  [[nodiscard]] Result<bool> accept(std::uint64_t sequence);

  // Highest accepted sequence, 0 when nothing has been accepted yet.
  [[nodiscard]] std::uint64_t highest() const noexcept { return highest_; }
  [[nodiscard]] std::uint64_t window() const noexcept { return window_; }
  [[nodiscard]] std::size_t retained() const noexcept { return recent_.size(); }

 private:
  void remember(std::uint64_t sequence);

  std::deque<std::uint64_t> recent_;
  std::unordered_set<std::uint64_t> recent_set_;
  std::uint64_t highest_{0};
  std::uint64_t window_{0};
};

}  // namespace shuffle::fabric
