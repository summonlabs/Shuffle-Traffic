// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/bytes.hpp"

namespace shuffle::fabric {

Status Limits::validate() const {
  if (max_frame_payload_bytes == 0 || max_frame_payload_bytes > max_message_payload_bytes) {
    return Status{make_error(ErrorCode::InvalidArgument, "max_frame_payload_bytes must be in (0, max_message_payload_bytes]")};
  }
  if (max_allocation_bytes == 0 || max_allocation_bytes > max_message_payload_bytes) {
    return Status{make_error(ErrorCode::InvalidArgument, "max_allocation_bytes must be in (0, max_message_payload_bytes]")};
  }
  if (max_string_bytes == 0 || max_string_bytes > max_allocation_bytes) {
    return Status{make_error(ErrorCode::InvalidArgument, "max_string_bytes must be in (0, max_allocation_bytes]")};
  }
  if (max_endpoint_bytes == 0 || max_endpoint_bytes > max_string_bytes) {
    return Status{make_error(ErrorCode::InvalidArgument, "max_endpoint_bytes must be in (0, max_string_bytes]")};
  }
  if (max_collection_items == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "max_collection_items must be positive")};
  }
  if (max_chunks_per_partition == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "max_chunks_per_partition must be positive")};
  }
  if (max_partitions == 0 || max_participants == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "max_partitions and max_participants must be positive")};
  }
  if (max_selection_patterns == 0 || max_selection_items == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "selection bounds must be positive")};
  }
  if (max_planned_edges == 0 || max_tracked_edges == 0 || max_tracked_partitions == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "edge and partition tracking bounds must be positive")};
  }
  if (max_tracked_partitions > max_partitions) {
    return Status{make_error(ErrorCode::InvalidArgument, "max_tracked_partitions exceeds max_partitions")};
  }
  if (max_state_bytes == 0 || max_journal_record_bytes == 0 || max_journal_record_bytes > max_state_bytes) {
    return Status{make_error(ErrorCode::InvalidArgument, "durable state bounds are inconsistent")};
  }
  if (max_retained_generations == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "max_retained_generations must be positive")};
  }
  if (max_wave_grants == 0 || static_cast<std::uint64_t>(max_wave_grants) > max_tracked_edges) {
    return Status{make_error(ErrorCode::InvalidArgument, "max_wave_grants is inconsistent with tracked edges")};
  }
  return Status{};
}

}  // namespace shuffle::fabric
