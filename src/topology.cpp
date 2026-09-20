// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/topology.hpp"

#include <algorithm>
#include <string>

namespace shuffle::fabric {
namespace {

[[nodiscard]] const ProducerRecord* find_producer(const std::vector<ProducerRecord>& records, ProducerId id) {
  const auto found = std::lower_bound(records.begin(), records.end(), id,
                                      [](const ProducerRecord& record, ProducerId value) { return record.id < value; });
  if (found == records.end() || found->id != id) {
    return nullptr;
  }
  return &*found;
}

// A consumer counts toward the shuffle's requirement from registration until it
// is explicitly withdrawn. Liveness decides whether an edge may be dispatched,
// never whether it is required: an edge whose consumer is down stays explicitly
// incomplete instead of silently disappearing from the accounting.
[[nodiscard]] constexpr bool counts_toward_requirement(ParticipantState state) noexcept {
  return state != ParticipantState::Withdrawn;
}

[[nodiscard]] const ConsumerRecord* find_consumer(const std::vector<ConsumerRecord>& records, ConsumerId id) {
  const auto found = std::lower_bound(records.begin(), records.end(), id,
                                      [](const ConsumerRecord& record, ConsumerId value) { return record.id < value; });
  if (found == records.end() || found->id != id) {
    return nullptr;
  }
  return &*found;
}

}  // namespace

PartitionSelection PartitionSelection::range(PartitionId first, PartitionId limit) {
  PartitionSelection selection;
  selection.kind = SelectionKind::Range;
  selection.begin = first;
  selection.end = limit;
  return selection;
}

Result<PartitionSelection> PartitionSelection::from_list(std::vector<PartitionId> partitions, const Limits& limits) {
  if (partitions.empty()) {
    return make_failure<PartitionSelection>(ErrorCode::InvalidArgument, "partition selection list is empty");
  }
  if (partitions.size() > limits.max_selection_items) {
    return make_failure<PartitionSelection>(ErrorCode::LimitExceeded,
                                            "partition selection list exceeds max_selection_items");
  }
  std::sort(partitions.begin(), partitions.end());
  for (std::size_t index = 1; index < partitions.size(); ++index) {
    if (partitions[index] == partitions[index - 1]) {
      return make_failure<PartitionSelection>(ErrorCode::InvalidArgument,
                                              "partition selection list contains a duplicate identifier");
    }
  }
  PartitionSelection selection;
  selection.kind = SelectionKind::List;
  selection.list = std::move(partitions);
  return selection;
}

bool PartitionSelection::covers(PartitionId partition) const noexcept {
  switch (kind) {
    case SelectionKind::All:
      return true;
    case SelectionKind::Range:
      return partition >= begin && partition < end;
    case SelectionKind::List:
      return std::binary_search(list.begin(), list.end(), partition);
  }
  return false;
}

Result<std::uint64_t> PartitionSelection::selected_count(std::uint32_t partition_count) const {
  switch (kind) {
    case SelectionKind::All:
      return static_cast<std::uint64_t>(partition_count);
    case SelectionKind::Range: {
      const std::uint64_t first = begin.value();
      const std::uint64_t limit = end.value();
      if (first >= partition_count || limit <= first) {
        return std::uint64_t{0};
      }
      const std::uint64_t capped = std::min<std::uint64_t>(limit, partition_count);
      return capped - first;
    }
    case SelectionKind::List: {
      const auto found = std::lower_bound(list.begin(), list.end(), PartitionId{partition_count});
      return static_cast<std::uint64_t>(std::distance(list.begin(), found));
    }
  }
  return make_failure<std::uint64_t>(ErrorCode::InvalidState, "unreachable partition selection kind");
}

std::string PartitionSelection::canonical_key() const {
  switch (kind) {
    case SelectionKind::All:
      return "all";
    case SelectionKind::Range:
      return "range:" + begin.to_string() + ":" + end.to_string();
    case SelectionKind::List: {
      std::string key = "list:";
      for (std::size_t index = 0; index < list.size(); ++index) {
        if (index != 0) {
          key += ",";
        }
        key += list[index].to_string();
      }
      return key;
    }
  }
  return "invalid";
}

Topology::Topology(ShuffleId shuffle, ShuffleGeneration shuffle_generation, std::uint32_t partition_count,
                   const Limits& limits)
    : shuffle_(shuffle), shuffle_generation_(shuffle_generation), partition_count_(partition_count), limits_(limits) {}

std::uint32_t Topology::active_producer_count() const noexcept {
  return static_cast<std::uint32_t>(active_producers_.size());
}

std::uint32_t Topology::active_consumer_count() const noexcept {
  std::uint32_t count = 0;
  for (const ConsumerRecord& record : consumers_) {
    if (holds_authority(record.state)) {
      ++count;
    }
  }
  return count;
}

const PartitionSelection& Topology::pattern(std::uint32_t index) const {
  static const PartitionSelection empty{};
  if (index >= patterns_.size()) {
    return empty;
  }
  return patterns_[index].selection;
}

const std::vector<ConsumerId>& Topology::consumers_of_pattern(std::uint32_t index) const {
  static const std::vector<ConsumerId> empty{};
  if (index >= patterns_.size()) {
    return empty;
  }
  return patterns_[index].consumers;
}

Result<const ProducerRecord*> Topology::producer(ProducerId id) const {
  const ProducerRecord* record = find_producer(producers_, id);
  if (record == nullptr) {
    return make_failure<const ProducerRecord*>(ErrorCode::UnknownParticipant, "producer " + id.to_string() + " is not registered");
  }
  return record;
}

Result<const ConsumerRecord*> Topology::consumer(ConsumerId id) const {
  const ConsumerRecord* record = find_consumer(consumers_, id);
  if (record == nullptr) {
    return make_failure<const ConsumerRecord*>(ErrorCode::UnknownParticipant, "consumer " + id.to_string() + " is not registered");
  }
  return record;
}

void Topology::bump_generation() {
  generation_ = generation_.next();
}

void Topology::rebuild_active_producers() {
  active_producers_.clear();
  for (const ProducerRecord& record : producers_) {
    if (holds_authority(record.state)) {
      active_producers_.push_back(record.id);
    }
  }
}

void Topology::rebuild_pattern_order() {
  pattern_order_.resize(patterns_.size());
  for (std::size_t index = 0; index < pattern_order_.size(); ++index) {
    pattern_order_[index] = static_cast<std::uint32_t>(index);
  }
  std::sort(pattern_order_.begin(), pattern_order_.end(), [this](std::uint32_t lhs, std::uint32_t rhs) {
    return patterns_[lhs].key < patterns_[rhs].key;
  });
}

void Topology::add_consumer_to_pattern(std::uint32_t pattern_index, ConsumerId id) {
  if (pattern_index >= patterns_.size()) {
    return;
  }
  std::vector<ConsumerId>& consumers = patterns_[pattern_index].consumers;
  const auto position = std::lower_bound(consumers.begin(), consumers.end(), id);
  if (position == consumers.end() || *position != id) {
    consumers.insert(position, id);
  }
}

void Topology::remove_consumer_from_pattern(std::uint32_t pattern_index, ConsumerId id) {
  if (pattern_index >= patterns_.size()) {
    return;
  }
  std::vector<ConsumerId>& consumers = patterns_[pattern_index].consumers;
  const auto position = std::lower_bound(consumers.begin(), consumers.end(), id);
  if (position != consumers.end() && *position == id) {
    consumers.erase(position);
  }
}

Result<std::uint32_t> Topology::intern_pattern(const PartitionSelection& selection) {
  const std::string key = selection.canonical_key();
  const auto existing = pattern_index_.find(key);
  if (existing != pattern_index_.end()) {
    return existing->second;
  }
  if (patterns_.size() >= limits_.max_selection_patterns) {
    return make_failure<std::uint32_t>(ErrorCode::TooManyPatterns,
                                       "distinct selection patterns exceed max_selection_patterns");
  }
  const std::uint64_t items =
      selection.kind == SelectionKind::List ? static_cast<std::uint64_t>(selection.list.size()) : 1ull;
  if (interned_selection_items_ + items > limits_.max_selection_items) {
    return make_failure<std::uint32_t>(ErrorCode::LimitExceeded,
                                       "interned selection items exceed max_selection_items");
  }
  const auto index = static_cast<std::uint32_t>(patterns_.size());
  PatternEntry entry;
  entry.selection = selection;
  entry.key = key;
  patterns_.push_back(std::move(entry));
  pattern_index_.emplace(key, index);
  interned_selection_items_ += items;
  rebuild_pattern_order();
  return index;
}

Status Topology::check_producer_registration(ProducerId id, IncarnationId incarnation,
                                                 std::string_view endpoint) const {
  if (id.is_zero()) {
    return Status{make_error(ErrorCode::InvalidArgument, "producer identifier must be non-zero")};
  }
  if (incarnation.is_zero()) {
    return Status{make_error(ErrorCode::InvalidArgument, "producer incarnation must be non-zero")};
  }
  if (endpoint.size() > limits_.max_endpoint_bytes) {
    return Status{make_error(ErrorCode::OversizedInput, "producer endpoint exceeds max_endpoint_bytes")};
  }
  const ProducerRecord* existing = find_producer(producers_, id);
  if (existing == nullptr && producers_.size() >= limits_.max_participants) {
    return Status{make_error(ErrorCode::TooManyParticipants, "producer count would exceed max_participants")};
  }
  if (existing != nullptr && existing->incarnation == incarnation && existing->endpoint != endpoint) {
    return Status{make_error(ErrorCode::IdentityMismatch,
                             "producer re-registered with a different endpoint under the same incarnation")};
  }
  return Status{};
}

Status Topology::check_consumer_registration(ConsumerId id, IncarnationId incarnation, std::string_view endpoint,
                                             const PartitionSelection& selection) const {
  if (id.is_zero()) {
    return Status{make_error(ErrorCode::InvalidArgument, "consumer identifier must be non-zero")};
  }
  if (incarnation.is_zero()) {
    return Status{make_error(ErrorCode::InvalidArgument, "consumer incarnation must be non-zero")};
  }
  if (endpoint.size() > limits_.max_endpoint_bytes) {
    return Status{make_error(ErrorCode::OversizedInput, "consumer endpoint exceeds max_endpoint_bytes")};
  }
  if (partition_count_ == 0) {
    return Status{make_error(ErrorCode::InvalidState, "topology has no partitions")};
  }
  switch (selection.kind) {
    case SelectionKind::All:
      break;
    case SelectionKind::Range:
      if (selection.begin >= selection.end) {
        return Status{make_error(ErrorCode::InvalidArgument, "range selection is empty")};
      }
      if (selection.end.value() > partition_count_) {
        return Status{make_error(ErrorCode::UnknownPartition, "range selection exceeds the partition count")};
      }
      break;
    case SelectionKind::List:
      if (selection.list.empty()) {
        return Status{make_error(ErrorCode::InvalidArgument, "list selection is empty")};
      }
      for (const PartitionId partition : selection.list) {
        if (partition.value() >= partition_count_) {
          return Status{make_error(ErrorCode::UnknownPartition, "list selection references an unknown partition")};
        }
      }
      break;
  }

  const ConsumerRecord* existing = find_consumer(consumers_, id);
  if (existing == nullptr && consumers_.size() >= limits_.max_participants) {
    return Status{make_error(ErrorCode::TooManyParticipants, "consumer count would exceed max_participants")};
  }

  // Interning a new pattern must succeed before the registration is durable.
  const std::string key = selection.canonical_key();
  if (pattern_index_.find(key) == pattern_index_.end()) {
    if (patterns_.size() >= limits_.max_selection_patterns) {
      return Status{make_error(ErrorCode::TooManyPatterns,
                               "distinct selection patterns exceed max_selection_patterns")};
    }
    const std::uint64_t items =
        selection.kind == SelectionKind::List ? static_cast<std::uint64_t>(selection.list.size()) : 1ull;
    if (interned_selection_items_ + items > limits_.max_selection_items) {
      return Status{make_error(ErrorCode::LimitExceeded, "interned selection items exceed max_selection_items")};
    }
  }
  return Status{};
}

Status Topology::check_participant_state(ParticipantKind kind, std::uint64_t id, IncarnationId incarnation,
                                         ParticipantState state) const {
  if (state > ParticipantState::Withdrawn) {
    return Status{make_error(ErrorCode::InvalidArgument, "unknown participant state")};
  }
  if (id == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "participant identifier must be non-zero")};
  }
  if (kind == ParticipantKind::Producer) {
    const ProducerRecord* record = find_producer(producers_, ProducerId{id});
    if (record == nullptr) {
      return Status{make_error(ErrorCode::UnknownParticipant, "producer is not registered")};
    }
    if (record->incarnation != incarnation) {
      return Status{make_error(ErrorCode::StaleIncarnation, "producer state change carries a superseded incarnation")};
    }
    return Status{};
  }
  const ConsumerRecord* record = find_consumer(consumers_, ConsumerId{id});
  if (record == nullptr) {
    return Status{make_error(ErrorCode::UnknownParticipant, "consumer is not registered")};
  }
  if (record->incarnation != incarnation) {
    return Status{make_error(ErrorCode::StaleIncarnation, "consumer state change carries a superseded incarnation")};
  }
  return Status{};
}

Status Topology::register_producer(ProducerId id, IncarnationId incarnation, std::string endpoint) {
  const Status checked = check_producer_registration(id, incarnation, endpoint);
  if (!checked.ok()) {
    return checked;
  }

  const auto position = std::lower_bound(producers_.begin(), producers_.end(), id,
                                         [](const ProducerRecord& record, ProducerId value) { return record.id < value; });
  if (position != producers_.end() && position->id == id) {
    if (position->incarnation == incarnation) {
      if (position->endpoint != endpoint) {
        return Status{make_error(ErrorCode::IdentityMismatch,
                                 "producer re-registered with a different endpoint under the same incarnation")};
      }
      return Status{};  // idempotent
    }
    // A new incarnation supersedes the previous one. The superseded
    // incarnation loses authority at this instant: attempts carrying it are
    // refused from here on.
    position->incarnation = incarnation;
    position->endpoint = std::move(endpoint);
    position->state = ParticipantState::Active;
    rebuild_active_producers();
    bump_generation();
    return Status{};
  }

  if (producers_.size() >= limits_.max_participants) {
    return Status{make_error(ErrorCode::TooManyParticipants, "producer count would exceed max_participants")};
  }
  producers_.insert(position, ProducerRecord{id, incarnation, ParticipantState::Active, std::move(endpoint)});
  rebuild_active_producers();
  bump_generation();
  return Status{};
}

Status Topology::register_consumer(ConsumerId id, IncarnationId incarnation, std::string endpoint,
                                   const PartitionSelection& selection) {
  const Status checked = check_consumer_registration(id, incarnation, endpoint, selection);
  if (!checked.ok()) {
    return checked;
  }

  const auto interned = intern_pattern(selection);
  if (!interned.ok()) {
    return Status{interned.error()};
  }
  const std::uint32_t new_pattern = interned.value();

  const auto position = std::lower_bound(consumers_.begin(), consumers_.end(), id,
                                         [](const ConsumerRecord& record, ConsumerId value) { return record.id < value; });
  if (position != consumers_.end() && position->id == id) {
    const bool identical = position->incarnation == incarnation && position->pattern == new_pattern &&
                           position->endpoint == endpoint;
    if (identical) {
      return Status{};  // idempotent
    }
    if (position->incarnation != incarnation && holds_authority(position->state)) {
      // Superseding an active incarnation is allowed: the newest registration
      // wins and the previous one loses authority immediately.
    }
    remove_consumer_from_pattern(position->pattern, id);
    position->incarnation = incarnation;
    position->endpoint = std::move(endpoint);
    position->pattern = new_pattern;
    position->state = ParticipantState::Active;
    add_consumer_to_pattern(new_pattern, id);
    bump_generation();
    return Status{};
  }

  consumers_.insert(position, ConsumerRecord{id, incarnation, ParticipantState::Active, std::move(endpoint), new_pattern});
  add_consumer_to_pattern(new_pattern, id);
  bump_generation();
  return Status{};
}

Status Topology::set_producer_state(ProducerId id, IncarnationId incarnation, ParticipantState state) {
  const Status checked = check_participant_state(ParticipantKind::Producer, id.value(), incarnation, state);
  if (!checked.ok()) {
    return checked;
  }
  // The check above established that the record exists and carries this
  // incarnation, so the lookup below cannot fail.
  const auto position = std::lower_bound(producers_.begin(), producers_.end(), id,
                                         [](const ProducerRecord& record, ProducerId value) { return record.id < value; });
  if (position->state == state) {
    return Status{};
  }
  position->state = state;
  rebuild_active_producers();
  bump_generation();
  return Status{};
}

Status Topology::set_consumer_state(ConsumerId id, IncarnationId incarnation, ParticipantState state) {
  const Status checked = check_participant_state(ParticipantKind::Consumer, id.value(), incarnation, state);
  if (!checked.ok()) {
    return checked;
  }
  // The check above established that the record exists and carries this
  // incarnation, so the lookup below cannot fail.
  const auto position = std::lower_bound(consumers_.begin(), consumers_.end(), id,
                                         [](const ConsumerRecord& record, ConsumerId value) { return record.id < value; });
  if (position->state == state) {
    return Status{};
  }
  const bool was_counted = counts_toward_requirement(position->state);
  const bool now_counted = counts_toward_requirement(state);
  position->state = state;
  if (was_counted && !now_counted) {
    // Withdrawal is the only transition that removes the requirement.
    remove_consumer_from_pattern(position->pattern, id);
  } else if (!was_counted && now_counted) {
    add_consumer_to_pattern(position->pattern, id);
  }
  bump_generation();
  return Status{};
}

Status Topology::withdraw_producer(ProducerId id, IncarnationId incarnation) {
  return set_producer_state(id, incarnation, ParticipantState::Withdrawn);
}

Status Topology::withdraw_consumer(ConsumerId id, IncarnationId incarnation) {
  return set_consumer_state(id, incarnation, ParticipantState::Withdrawn);
}

Result<std::uint32_t> Topology::owner_index_of(PartitionId partition) const {
  if (partition.value() >= partition_count_) {
    return make_failure<std::uint32_t>(ErrorCode::UnknownPartition, "partition " + partition.to_string() + " is outside the shuffle");
  }
  if (active_producers_.empty()) {
    return make_failure<std::uint32_t>(ErrorCode::PartitionNotOwned, "shuffle has no active producer");
  }
  const auto index = static_cast<std::uint32_t>(partition.value() % active_producers_.size());
  return index;
}

Result<ProducerId> Topology::owner_of(PartitionId partition) const {
  const auto index = owner_index_of(partition);
  if (!index.ok()) {
    return Result<ProducerId>{index.error()};
  }
  return active_producers_[index.value()];
}

std::vector<ConsumerId> Topology::covering_consumers(PartitionId partition) const {
  std::vector<ConsumerId> covering;
  if (partition.value() >= partition_count_) {
    return covering;
  }
  for (const std::uint32_t pattern_index : pattern_order_) {
    const PatternEntry& entry = patterns_[pattern_index];
    if (entry.consumers.empty() || !entry.selection.covers(partition)) {
      continue;
    }
    covering.insert(covering.end(), entry.consumers.begin(), entry.consumers.end());
  }
  return covering;
}

std::size_t Topology::covering_consumer_count(PartitionId partition) const {
  if (partition.value() >= partition_count_) {
    return 0;
  }
  std::size_t count = 0;
  for (const std::uint32_t pattern_index : pattern_order_) {
    const PatternEntry& entry = patterns_[pattern_index];
    if (entry.selection.covers(partition)) {
      count += entry.consumers.size();
    }
  }
  return count;
}

Result<std::uint32_t> Topology::fan_out(PartitionId partition) const {
  if (partition.value() >= partition_count_) {
    return make_failure<std::uint32_t>(ErrorCode::UnknownPartition, "partition is outside the shuffle");
  }
  std::uint64_t count = 0;
  for (const PatternEntry& entry : patterns_) {
    if (entry.selection.covers(partition)) {
      count += entry.consumers.size();
    }
  }
  if (count > 0xffffffffull) {
    return make_failure<std::uint32_t>(ErrorCode::IntegerOverflow, "fan-out exceeds the representable range");
  }
  return static_cast<std::uint32_t>(count);
}

Result<std::uint32_t> Topology::fan_in(ConsumerId id) const {
  const ConsumerRecord* record = find_consumer(consumers_, id);
  if (record == nullptr) {
    return make_failure<std::uint32_t>(ErrorCode::UnknownParticipant, "consumer is not registered");
  }
  const std::uint32_t producer_count = active_producer_count();
  if (producer_count == 0) {
    return std::uint32_t{0};
  }
  const PartitionSelection& selection = patterns_[record->pattern].selection;
  switch (selection.kind) {
    case SelectionKind::All:
      return std::min(producer_count, partition_count_);
    case SelectionKind::Range: {
      const auto selected = selection.selected_count(partition_count_);
      if (!selected.ok()) {
        return Result<std::uint32_t>{selected.error()};
      }
      const std::uint64_t capped = std::min<std::uint64_t>(selected.value(), producer_count);
      return static_cast<std::uint32_t>(capped);
    }
    case SelectionKind::List: {
      // Distinct ownership residues among the listed partitions.
      std::vector<std::uint32_t> residues;
      residues.reserve(selection.list.size());
      for (const PartitionId partition : selection.list) {
        residues.push_back(static_cast<std::uint32_t>(partition.value() % producer_count));
      }
      std::sort(residues.begin(), residues.end());
      residues.erase(std::unique(residues.begin(), residues.end()), residues.end());
      if (residues.size() > 0xffffffffull) {
        return make_failure<std::uint32_t>(ErrorCode::IntegerOverflow, "fan-in exceeds the representable range");
      }
      return static_cast<std::uint32_t>(residues.size());
    }
  }
  return make_failure<std::uint32_t>(ErrorCode::InvalidState, "unreachable selection kind");
}

Result<std::uint64_t> Topology::planned_edge_count() const {
  std::uint64_t total = 0;
  for (const PatternEntry& entry : patterns_) {
    if (entry.consumers.empty()) {
      continue;
    }
    const auto selected = entry.selection.selected_count(partition_count_);
    if (!selected.ok()) {
      return Result<std::uint64_t>{selected.error()};
    }
    const auto contribution = checked_mul(static_cast<std::uint64_t>(entry.consumers.size()), selected.value());
    if (!contribution.ok()) {
      return Result<std::uint64_t>{contribution.error()};
    }
    const auto sum = checked_add(total, contribution.value());
    if (!sum.ok()) {
      return Result<std::uint64_t>{sum.error()};
    }
    total = sum.value();
  }
  return total;
}

Status Topology::validate() const {
  if (partition_count_ == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "topology has no partitions")};
  }
  const Status limits_valid = limits_.validate();
  if (!limits_valid.ok()) {
    return limits_valid;
  }
  for (std::size_t index = 1; index < producers_.size(); ++index) {
    if (!(producers_[index - 1].id < producers_[index].id)) {
      return Status{make_error(ErrorCode::InvalidState, "producer records are not strictly ascending")};
    }
  }
  for (std::size_t index = 1; index < consumers_.size(); ++index) {
    if (!(consumers_[index - 1].id < consumers_[index].id)) {
      return Status{make_error(ErrorCode::InvalidState, "consumer records are not strictly ascending")};
    }
  }
  for (const ConsumerRecord& record : consumers_) {
    if (record.pattern >= patterns_.size()) {
      return Status{make_error(ErrorCode::InvalidState, "consumer references an unknown selection pattern")};
    }
  }
  for (const PatternEntry& entry : patterns_) {
    for (std::size_t index = 1; index < entry.consumers.size(); ++index) {
      if (!(entry.consumers[index - 1] < entry.consumers[index])) {
        return Status{make_error(ErrorCode::InvalidState, "pattern consumer list is not strictly ascending")};
      }
    }
    for (const ConsumerId id : entry.consumers) {
      const ConsumerRecord* record = find_consumer(consumers_, id);
      if (record == nullptr || record->pattern != static_cast<std::uint32_t>(&entry - patterns_.data())) {
        return Status{make_error(ErrorCode::InvalidState, "pattern consumer list disagrees with the consumer record")};
      }
      if (!counts_toward_requirement(record->state)) {
        return Status{make_error(ErrorCode::InvalidState, "pattern contains a withdrawn consumer")};
      }
    }
  }
  if (pattern_order_.size() != patterns_.size()) {
    return Status{make_error(ErrorCode::InvalidState, "pattern order is not a permutation of the pattern table")};
  }
  for (std::size_t index = 1; index < pattern_order_.size(); ++index) {
    if (!(patterns_[pattern_order_[index - 1]].key < patterns_[pattern_order_[index]].key)) {
      return Status{make_error(ErrorCode::InvalidState, "pattern order is not sorted by canonical key")};
    }
  }
  return Status{};
}

std::string describe_topology(const Topology& topology) {
  std::string text;
  text += "shuffle=" + topology.shuffle().to_string() + " generation=" + topology.shuffle_generation().to_string() +
          " topology_generation=" + topology.generation().to_string() + "\n";
  text += "  partitions=" + std::to_string(topology.partition_count()) +
          " producers=" + std::to_string(topology.producer_count()) +
          " (active " + std::to_string(topology.active_producer_count()) + ")" +
          " consumers=" + std::to_string(topology.consumer_count()) +
          " (active " + std::to_string(topology.active_consumer_count()) + ")" +
          " patterns=" + std::to_string(topology.pattern_count()) + "\n";
  const auto edges = topology.planned_edge_count();
  text += "  planned_edges=" + (edges.ok() ? std::to_string(edges.value()) : std::string{"<error>"}) + "\n";
  return text;
}

}  // namespace shuffle::fabric
