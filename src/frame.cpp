// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/frame.hpp"

#include <array>
#include <cstring>
#include <limits>

#include "shuffle/fabric/hash.hpp"

namespace shuffle::fabric {
namespace {

// The magic is carried little-endian, so its low byte is the first byte on the
// wire. Proving that here keeps a renumbered or byte-swapped constant from ever
// reaching a peer.
static_assert((kFrameMagic & 0xffu) == static_cast<std::uint32_t>('S'), "frame magic must start with 'S'");
static_assert(((kFrameMagic >> 8) & 0xffu) == static_cast<std::uint32_t>('F'), "frame magic must continue with 'F'");
static_assert(((kFrameMagic >> 16) & 0xffu) == static_cast<std::uint32_t>('B'), "frame magic must continue with 'B'");
static_assert(((kFrameMagic >> 24) & 0xffu) == static_cast<std::uint32_t>('1'), "frame magic must end with '1'");

constexpr std::array<std::byte, 4> kMagicBytes{static_cast<std::byte>('S'), static_cast<std::byte>('F'),
                                               static_cast<std::byte>('B'), static_cast<std::byte>('1')};

// Compacting a consumed prefix costs a memmove of everything that is left, so it
// is only worth doing once a meaningful amount has been drained. Without this
// threshold, draining N frames out of one feed would be quadratic in the frame
// size.
constexpr std::size_t kCompactThresholdBytes = 4096;

[[nodiscard]] std::uint16_t read_u16(const std::byte* bytes) noexcept {
  return static_cast<std::uint16_t>(static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[0])) |
                                    static_cast<std::uint16_t>(
                                        static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[1])) << 8));
}

[[nodiscard]] std::uint32_t read_u32(const std::byte* bytes) noexcept {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[index])) << (index * 8);
  }
  return value;
}

[[nodiscard]] std::uint64_t read_u64(const std::byte* bytes) noexcept {
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[index])) << (index * 8);
  }
  return value;
}

void write_u16(std::byte* bytes, std::uint16_t value) noexcept {
  bytes[0] = static_cast<std::byte>(value & 0xffu);
  bytes[1] = static_cast<std::byte>((value >> 8) & 0xffu);
}

void write_u32(std::byte* bytes, std::uint32_t value) noexcept {
  for (std::size_t index = 0; index < 4; ++index) {
    bytes[index] = static_cast<std::byte>((value >> (index * 8)) & 0xffu);
  }
}

void write_u64(std::byte* bytes, std::uint64_t value) noexcept {
  for (std::size_t index = 0; index < 8; ++index) {
    bytes[index] = static_cast<std::byte>((value >> (index * 8)) & 0xffu);
  }
}

}  // namespace

const char* to_string(MessageType type) noexcept {
  switch (type) {
    case MessageType::HandshakeRequest: return "HandshakeRequest";
    case MessageType::HandshakeResponse: return "HandshakeResponse";
    case MessageType::ErrorResponse: return "ErrorResponse";
    case MessageType::OpenShuffleRequest: return "OpenShuffleRequest";
    case MessageType::OpenShuffleResponse: return "OpenShuffleResponse";
    case MessageType::RegisterParticipantRequest: return "RegisterParticipantRequest";
    case MessageType::RegisterParticipantResponse: return "RegisterParticipantResponse";
    case MessageType::PublishManifestRequest: return "PublishManifestRequest";
    case MessageType::PublishManifestResponse: return "PublishManifestResponse";
    case MessageType::NextWaveRequest: return "NextWaveRequest";
    case MessageType::NextWaveResponse: return "NextWaveResponse";
    case MessageType::CommitTransferRequest: return "CommitTransferRequest";
    case MessageType::CommitTransferResponse: return "CommitTransferResponse";
    case MessageType::ReportFailureRequest: return "ReportFailureRequest";
    case MessageType::ReportFailureResponse: return "ReportFailureResponse";
    case MessageType::CongestionIntentRequest: return "CongestionIntentRequest";
    case MessageType::CongestionIntentResponse: return "CongestionIntentResponse";
    case MessageType::CancelShuffleRequest: return "CancelShuffleRequest";
    case MessageType::CancelShuffleResponse: return "CancelShuffleResponse";
    case MessageType::ProgressRequest: return "ProgressRequest";
    case MessageType::ProgressResponse: return "ProgressResponse";
    case MessageType::ExplainRequest: return "ExplainRequest";
    case MessageType::ExplainResponse: return "ExplainResponse";
    case MessageType::CloseRequest: return "CloseRequest";
    case MessageType::CloseResponse: return "CloseResponse";
    case MessageType::ManifestRequest: return "ManifestRequest";
    case MessageType::ManifestResponse: return "ManifestResponse";
    case MessageType::StatusRequest: return "StatusRequest";
    case MessageType::StatusResponse: return "StatusResponse";
    case MessageType::ChunkFetchRequest: return "ChunkFetchRequest";
    case MessageType::ChunkFetchResponse: return "ChunkFetchResponse";
    case MessageType::ChunkFetchFailure: return "ChunkFetchFailure";
  }
  return "Unknown";
}

bool is_known_message_type(MessageType type) noexcept {
  switch (type) {
    case MessageType::HandshakeRequest:
    case MessageType::HandshakeResponse:
    case MessageType::ErrorResponse:
    case MessageType::OpenShuffleRequest:
    case MessageType::OpenShuffleResponse:
    case MessageType::RegisterParticipantRequest:
    case MessageType::RegisterParticipantResponse:
    case MessageType::PublishManifestRequest:
    case MessageType::PublishManifestResponse:
    case MessageType::NextWaveRequest:
    case MessageType::NextWaveResponse:
    case MessageType::CommitTransferRequest:
    case MessageType::CommitTransferResponse:
    case MessageType::ReportFailureRequest:
    case MessageType::ReportFailureResponse:
    case MessageType::CongestionIntentRequest:
    case MessageType::CongestionIntentResponse:
    case MessageType::CancelShuffleRequest:
    case MessageType::CancelShuffleResponse:
    case MessageType::ProgressRequest:
    case MessageType::ProgressResponse:
    case MessageType::ExplainRequest:
    case MessageType::ExplainResponse:
    case MessageType::CloseRequest:
    case MessageType::CloseResponse:
    case MessageType::ManifestRequest:
    case MessageType::ManifestResponse:
    case MessageType::StatusRequest:
    case MessageType::StatusResponse:
    case MessageType::ChunkFetchRequest:
    case MessageType::ChunkFetchResponse:
    case MessageType::ChunkFetchFailure:
      return true;
  }
  return false;
}

bool is_response(MessageType type) noexcept {
  switch (type) {
    case MessageType::HandshakeResponse:
    case MessageType::ErrorResponse:
    case MessageType::OpenShuffleResponse:
    case MessageType::RegisterParticipantResponse:
    case MessageType::PublishManifestResponse:
    case MessageType::NextWaveResponse:
    case MessageType::CommitTransferResponse:
    case MessageType::ReportFailureResponse:
    case MessageType::CongestionIntentResponse:
    case MessageType::CancelShuffleResponse:
    case MessageType::ProgressResponse:
    case MessageType::ExplainResponse:
    case MessageType::CloseResponse:
    case MessageType::ManifestResponse:
    case MessageType::StatusResponse:
    case MessageType::ChunkFetchResponse:
    case MessageType::ChunkFetchFailure:
      return true;
    default:
      return false;
  }
}

bool is_request(MessageType type) noexcept {
  switch (type) {
    case MessageType::HandshakeRequest:
    case MessageType::OpenShuffleRequest:
    case MessageType::RegisterParticipantRequest:
    case MessageType::PublishManifestRequest:
    case MessageType::NextWaveRequest:
    case MessageType::CommitTransferRequest:
    case MessageType::ReportFailureRequest:
    case MessageType::CongestionIntentRequest:
    case MessageType::CancelShuffleRequest:
    case MessageType::ProgressRequest:
    case MessageType::ExplainRequest:
    case MessageType::CloseRequest:
    case MessageType::ManifestRequest:
    case MessageType::StatusRequest:
    case MessageType::ChunkFetchRequest:
      return true;
    default:
      return false;
  }
}

Status encode_frame(const FrameHeader& header, std::span<const std::byte> payload, std::vector<std::byte>& out,
                    const Limits& limits) {
  // Everything is validated before a single byte of out is touched, so a refused
  // encode leaves the caller's buffer byte-identical.
  if (header.version != kProtocolVersion) {
    return Status{make_error(ErrorCode::UnsupportedVersion, "frame version is not supported")};
  }
  if (!is_known_message_type(header.type)) {
    return Status{make_error(ErrorCode::FrameTypeUnsupported, "message type is not part of protocol version 1")};
  }
  if ((header.flags & kFlagReservedMask) != 0) {
    return Status{make_error(ErrorCode::MalformedInput, "reserved frame flag bits must be zero")};
  }
  if (header.sequence == 0) {
    return Status{make_error(ErrorCode::SequenceViolation, "wire sequences start at 1")};
  }
  if (payload.size() > limits.max_frame_payload_bytes) {
    return Status{make_error(ErrorCode::OversizedInput, "payload exceeds max_frame_payload_bytes")};
  }
  if (payload.size() > std::numeric_limits<std::uint32_t>::max()) {
    return Status{make_error(ErrorCode::OversizedInput, "payload does not fit the 32-bit length field")};
  }

  const std::uint32_t payload_crc = crc32c(payload);
  if (header.payload_length != 0 && header.payload_length != payload.size()) {
    return Status{make_error(ErrorCode::InvalidArgument, "declared payload_length contradicts the payload")};
  }
  if (header.payload_crc32c != 0 && header.payload_crc32c != payload_crc) {
    return Status{make_error(ErrorCode::InvalidArgument, "declared payload_crc32c contradicts the payload")};
  }

  const Result<std::uint64_t> total = checked_add(kFrameHeaderBytes, payload.size());
  if (!total.ok()) {
    return total.status();
  }
  const Result<std::size_t> frame_size = to_size(total.value());
  if (!frame_size.ok()) {
    return frame_size.status();
  }
  if (total.value() > limits.max_allocation_bytes) {
    return Status{make_error(ErrorCode::LimitExceeded, "frame exceeds max_allocation_bytes")};
  }

  out.resize(frame_size.value());
  std::byte* base = out.data();
  write_u32(base, kFrameMagic);
  write_u16(base + 4, header.version);
  write_u16(base + 6, static_cast<std::uint16_t>(header.type));
  write_u32(base + 8, header.flags);
  write_u64(base + 12, header.session.value());
  write_u64(base + 20, header.sequence);
  write_u32(base + 28, static_cast<std::uint32_t>(payload.size()));
  write_u32(base + 32, payload_crc);
  if (!payload.empty()) {
    std::memcpy(base + kFrameHeaderBytes, payload.data(), payload.size());
  }
  // The header CRC is written last: it covers every other header field, so it
  // must never be computed against a half-populated header.
  write_u32(base + kFrameHeaderCrcOffset, crc32c(std::span<const std::byte>(base, kFrameHeaderCrcOffset)));
  return Status{};
}

Result<FrameHeader> decode_header(std::span<const std::byte> header_bytes, const Limits& limits) {
  if (header_bytes.size() < kFrameHeaderBytes) {
    return make_failure<FrameHeader>(ErrorCode::TruncatedInput, "fewer than 40 bytes of frame header");
  }
  if (header_bytes.size() > kFrameHeaderBytes) {
    return make_failure<FrameHeader>(ErrorCode::TrailingGarbage, "header span is longer than one frame header");
  }

  const std::byte* base = header_bytes.data();
  if (read_u32(base) != kFrameMagic) {
    return make_failure<FrameHeader>(ErrorCode::ProtocolViolation, "frame magic does not match 'SFB1'");
  }

  FrameHeader header;
  header.version = read_u16(base + 4);
  if (header.version != kProtocolVersion) {
    return make_failure<FrameHeader>(ErrorCode::UnsupportedVersion, "frame version is not supported");
  }

  // Integrity before semantics: every field below this line, including the
  // declared payload length, is unverified input until the header CRC agrees.
  if (read_u32(base + kFrameHeaderCrcOffset) != crc32c(header_bytes.first(kFrameHeaderCrcOffset))) {
    return make_failure<FrameHeader>(ErrorCode::ChecksumMismatch, "header CRC-32C does not match");
  }

  header.flags = read_u32(base + 8);
  if ((header.flags & kFlagReservedMask) != 0) {
    return make_failure<FrameHeader>(ErrorCode::MalformedInput, "reserved frame flag bits must be zero");
  }

  header.type = static_cast<MessageType>(read_u16(base + 6));
  if (!is_known_message_type(header.type)) {
    return make_failure<FrameHeader>(ErrorCode::FrameTypeUnsupported, "message type is not part of protocol version 1");
  }

  header.payload_length = read_u32(base + 28);
  if (header.payload_length > limits.max_frame_payload_bytes) {
    return make_failure<FrameHeader>(ErrorCode::OversizedInput, "declared payload exceeds max_frame_payload_bytes");
  }

  header.session = SessionId{read_u64(base + 12)};
  header.sequence = read_u64(base + 20);
  if (header.sequence == 0) {
    return make_failure<FrameHeader>(ErrorCode::SequenceViolation, "wire sequences start at 1");
  }
  header.payload_crc32c = read_u32(base + 32);
  return header;
}

Result<DecodedFrame> decode_frame_prefix(std::span<const std::byte> bytes, const Limits& limits) {
  if (bytes.size() < kFrameHeaderBytes) {
    return make_failure<DecodedFrame>(ErrorCode::TruncatedInput, "fewer than 40 bytes of frame header");
  }
  const Result<FrameHeader> header = decode_header(bytes.first(kFrameHeaderBytes), limits);
  if (!header.ok()) {
    return header.error();
  }

  const Result<std::uint64_t> total = checked_add(kFrameHeaderBytes, header.value().payload_length);
  if (!total.ok()) {
    return total.error();
  }
  const Result<std::size_t> frame_size = to_size(total.value());
  if (!frame_size.ok()) {
    return frame_size.error();
  }
  if (bytes.size() < frame_size.value()) {
    return make_failure<DecodedFrame>(ErrorCode::TruncatedInput, "buffer ends before the declared frame length");
  }

  const std::span<const std::byte> payload = bytes.subspan(kFrameHeaderBytes, header.value().payload_length);
  if (crc32c(payload) != header.value().payload_crc32c) {
    return make_failure<DecodedFrame>(ErrorCode::ChecksumMismatch, "payload CRC-32C does not match");
  }
  return DecodedFrame{header.value(), payload, frame_size.value()};
}

Result<DecodedFrame> decode_frame(std::span<const std::byte> bytes, const Limits& limits) {
  const Result<DecodedFrame> frame = decode_frame_prefix(bytes, limits);
  if (!frame.ok()) {
    return frame.error();
  }
  if (frame.value().consumed != bytes.size()) {
    return make_failure<DecodedFrame>(ErrorCode::TrailingGarbage, "bytes follow the complete frame");
  }
  return frame;
}

std::uint64_t FrameStream::capacity() const noexcept {
  const Result<std::uint64_t> total = checked_add(kFrameHeaderBytes, limits_.max_frame_payload_bytes);
  if (!total.ok()) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  return total.value();
}

void FrameStream::compact() {
  if (consumed_ == 0) {
    return;
  }
  if (consumed_ == buffer_.size()) {
    buffer_.clear();
    consumed_ = 0;
    return;
  }
  if (consumed_ < kCompactThresholdBytes) {
    return;
  }
  buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(consumed_));
  consumed_ = 0;
}

Status FrameStream::feed(std::span<const std::byte> bytes) {
  if (bytes.empty()) {
    return Status{};
  }
  compact();
  const Result<std::uint64_t> queued =
      checked_add(static_cast<std::uint64_t>(buffered()), static_cast<std::uint64_t>(bytes.size()));
  if (!queued.ok()) {
    return Status{make_error(ErrorCode::ResourceExhausted, "frame stream queue bound overflowed")};
  }
  if (queued.value() > capacity()) {
    return Status{make_error(ErrorCode::ResourceExhausted,
                             "frame stream queue would exceed max_frame_payload_bytes + kFrameHeaderBytes")};
  }
  buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());
  return Status{};
}

Result<DecodedFrame> FrameStream::next() {
  compact();
  const std::size_t available = buffered();
  if (available == 0) {
    return make_failure<DecodedFrame>(ErrorCode::NoWorkAvailable, "no bytes are buffered");
  }

  const std::span<const std::byte> queued{buffer_.data() + consumed_, available};
  // A prefix that already contradicts the magic can never become a frame, so it
  // is refused as soon as it arrives instead of after 40 bytes of waiting. This
  // is the same refusal decode_header would produce for a complete header.
  const std::size_t prefixed = available < kMagicBytes.size() ? available : kMagicBytes.size();
  for (std::size_t index = 0; index < prefixed; ++index) {
    if (queued[index] != kMagicBytes[index]) {
      return make_failure<DecodedFrame>(ErrorCode::ProtocolViolation, "frame magic does not match 'SFB1'");
    }
  }
  if (available < kFrameHeaderBytes) {
    return make_failure<DecodedFrame>(ErrorCode::NoWorkAvailable, "frame header is not complete");
  }

  // The header is complete, so its declared length is the authority now: the
  // bound is enforced even though the payload has not arrived, and a frame that
  // is merely incomplete is "no work yet" rather than a truncation failure.
  const Result<FrameHeader> header = decode_header(queued.first(kFrameHeaderBytes), limits_);
  if (!header.ok()) {
    return header.error();
  }
  const Result<std::uint64_t> total = checked_add(kFrameHeaderBytes, header.value().payload_length);
  if (!total.ok()) {
    return total.error();
  }
  const Result<std::size_t> frame_size = to_size(total.value());
  if (!frame_size.ok()) {
    return frame_size.error();
  }
  if (available < frame_size.value()) {
    return make_failure<DecodedFrame>(ErrorCode::NoWorkAvailable, "frame payload is not complete");
  }

  const Result<DecodedFrame> decoded = decode_frame_prefix(queued, limits_);
  if (!decoded.ok()) {
    return decoded.error();
  }
  consumed_ += decoded.value().consumed;
  return decoded;
}

void ReplayGuard::remember(std::uint64_t sequence) {
  recent_.push_back(sequence);
  recent_set_.insert(sequence);
  // Retained entries are appended in acceptance order and every accepted
  // sequence is at least the front of the deque, so the front is always the
  // oldest retained value: evicting it advances the window's lower bound.
  while (recent_.size() > window_) {
    recent_set_.erase(recent_.front());
    recent_.pop_front();
  }
}

Result<bool> ReplayGuard::accept(std::uint64_t sequence) {
  if (sequence == 0) {
    return make_failure<bool>(ErrorCode::SequenceViolation, "wire sequences start at 1");
  }

  if (highest_ != 0 && sequence <= highest_) {
    if (sequence == highest_ || recent_set_.find(sequence) != recent_set_.end()) {
      return false;  // already accepted: a replay or a retransmission
    }
    if (recent_.empty() || sequence < recent_.front()) {
      return make_failure<bool>(ErrorCode::SequenceViolation,
                                "sequence fell out of the retained replay window and was never accepted");
    }
    remember(sequence);  // out of order but still inside the window
    return true;
  }

  if (sequence - highest_ > kMaxForwardJump) {
    return make_failure<bool>(ErrorCode::MalformedInput,
                              "sequence jumps more than 2^40 past the highest accepted sequence");
  }
  highest_ = sequence;
  remember(sequence);
  return true;
}

}  // namespace shuffle::fabric
