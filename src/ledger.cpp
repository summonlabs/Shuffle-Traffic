// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/ledger.hpp"

#include <algorithm>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

namespace shuffle::fabric {
namespace {

[[nodiscard]] bool is_authority_code(ErrorCode code) noexcept {
  return classify(code) == RetryClass::Authority;
}

}  // namespace

CompletionLedger::CompletionLedger(const Limits& limits) : limits_(limits) {}

void CompletionLedger::bind(ShuffleId shuffle, ShuffleGeneration generation, std::uint32_t partition_count) {
  shuffle_ = shuffle;
  shuffle_generation_ = generation;
  partition_count_ = partition_count;
}

std::uint64_t CompletionLedger::partition_key(PartitionId partition, PartitionGeneration generation) noexcept {
  // Partition and generation are combined into one key only for indexing; the
  // typed values remain the identity that every caller reasons about.
  return (partition.value() << 16) ^ generation.value();
}

bool CompletionLedger::retain_generation(PartitionGeneration generation) const {
  return generation.value() > 0;
}

Status CompletionLedger::validate_commit(const CommitRequest& request) const {
  if (request.edge.partition.value() >= partition_count_) {
    return Status{make_error(ErrorCode::UnknownPartition, "completion references a partition outside the shuffle")};
  }
  if (request.edge.partition_generation.is_zero()) {
    return Status{make_error(ErrorCode::InvalidArgument, "completion carries no partition generation")};
  }
  if (!retain_generation(request.edge.partition_generation)) {
    return Status{make_error(ErrorCode::InvalidArgument, "completion carries an impossible partition generation")};
  }
  if (!request.integrity_verified) {
    return Status{make_error(ErrorCode::IntegrityFailure, "completion requires verified integrity evidence")};
  }
  if (request.manifest_digest.is_zero()) {
    return Status{make_error(ErrorCode::ManifestInconsistent, "completion carries no manifest digest")};
  }
  if (request.bytes == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "completion declares zero bytes")};
  }
  if (!shuffle_generation_.is_zero() && request.shuffle_generation != shuffle_generation_) {
    return Status{make_error(ErrorCode::StaleGeneration, "completion belongs to another shuffle generation")};
  }

  const auto latest = latest_generation_.find(request.edge.partition.value());
  if (latest != latest_generation_.end() && request.edge.partition_generation < latest->second) {
    return Status{make_error(ErrorCode::StaleGeneration,
                             "partition generation has been superseded; late authority is refused")};
  }

  const std::uint64_t key = partition_key(request.edge.partition, request.edge.partition_generation);
  const auto existing = partitions_.find(key);
  if (existing == partitions_.end()) {
    if (partitions_.size() >= limits_.max_tracked_partitions) {
      return Status{make_error(ErrorCode::TooManyTrackedEdges,
                               "tracked partition records exceed max_tracked_partitions")};
    }
    if (completed_edges_ + failure_records_ + 1 > limits_.max_tracked_edges) {
      return Status{make_error(ErrorCode::TooManyTrackedEdges, "tracked edges exceed max_tracked_edges")};
    }
    return Status{};
  }

  const PartitionCommitRecord& record = existing->second;
  if (record.manifest_digest != request.manifest_digest || record.producer != request.producer ||
      record.producer_incarnation != request.producer_incarnation) {
    // A different claim about the same partition generation is a contradiction,
    // never a merge: merging would let one of the two claims be believed.
    return Status{make_error(ErrorCode::DivergentCommit,
                             "a different manifest or incarnation already committed this partition generation")};
  }
  if (record.total_bytes != request.bytes) {
    return Status{make_error(ErrorCode::AccountingMismatch,
                             "completion declares a byte count that contradicts the committed record")};
  }
  const auto position =
      std::lower_bound(record.completed_consumers.begin(), record.completed_consumers.end(), request.consumer);
  const bool already_completed = position != record.completed_consumers.end() && *position == request.consumer;
  if (!already_completed && completed_edges_ + failure_records_ + 1 > limits_.max_tracked_edges) {
    return Status{make_error(ErrorCode::TooManyTrackedEdges, "tracked edges exceed max_tracked_edges")};
  }
  return Status{};
}

Result<CommitReceipt> CompletionLedger::commit(const CommitRequest& request, TickId now) {
  const Status valid = validate_commit(request);
  if (!valid.ok()) {
    return Result<CommitReceipt>{valid.error()};
  }

  const std::uint64_t partition_key_value = request.edge.partition.value();
  const auto latest = latest_generation_.find(partition_key_value);
  const std::uint64_t key = partition_key(request.edge.partition, request.edge.partition_generation);
  CommitReceipt receipt;
  receipt.manifest_digest = request.manifest_digest;

  const auto existing = partitions_.find(key);
  if (existing == partitions_.end()) {
    PartitionCommitRecord record;
    record.anchor = EdgeKey{request.edge.partition, request.edge.partition_generation, ConsumerId{}};
    record.shuffle = shuffle_;
    record.shuffle_generation = request.shuffle_generation;
    record.producer = request.producer;
    record.producer_incarnation = request.producer_incarnation;
    record.manifest_digest = request.manifest_digest;
    record.total_bytes = request.bytes;
    record.topology_generation = request.topology_generation;
    record.sequence = next_sequence_;
    record.committed_at = now;
    record.completed_consumers.push_back(request.consumer);
    next_sequence_ = next_sequence_.next();

    receipt.newly_committed = true;
    receipt.edge_newly_completed = true;
    receipt.accounted_bytes = request.bytes;
    receipt.sequence = record.sequence;

    // A new partition generation retires the previous generation's completions
    // and permanent failures from the *current* edge set: the required edge is
    // "this consumer holds the current content of this partition".
    if (latest == latest_generation_.end() || request.edge.partition_generation > latest->second) {
      if (latest != latest_generation_.end()) {
        const auto previous = partitions_.find(partition_key(request.edge.partition, latest->second));
        if (previous != partitions_.end()) {
          completed_edges_ -= previous->second.completed_consumers.size();
          if (committed_partitions_ > 0) {
            --committed_partitions_;
          }
        }
        const auto retired = current_permanent_failures_.find(partition_key_value);
        if (retired != current_permanent_failures_.end()) {
          permanent_failures_ =
              permanent_failures_ >= retired->second ? permanent_failures_ - retired->second : 0;
          retired->second = 0;
        }
      }
      failed_partitions_.erase(partition_key_value);
      latest_generation_[partition_key_value] = request.edge.partition_generation;
    }

    bytes_committed_ += request.bytes;
    ++completed_edges_;
    ++committed_partitions_;
    failed_partitions_.erase(partition_key_value);
    partitions_.emplace(key, std::move(record));
    std::vector<std::uint64_t>& retained = retained_keys_[partition_key_value];
    retained.push_back(key);
    while (retained.size() > limits_.max_retained_generations) {
      const std::uint64_t oldest = retained.front();
      retained.erase(retained.begin());
      const auto purged = partitions_.find(oldest);
      if (purged != partitions_.end()) {
        completed_edges_ -= purged->second.completed_consumers.size();
        partitions_.erase(purged);
      }
    }
    return receipt;
  }

  PartitionCommitRecord& record = existing->second;
  receipt.sequence = record.sequence;
  const auto position =
      std::lower_bound(record.completed_consumers.begin(), record.completed_consumers.end(), request.consumer);
  if (position != record.completed_consumers.end() && *position == request.consumer) {
    // Duplicate completion: suppressed, and bytes are never accounted twice.
    ++record.duplicate_commits;
    ++duplicate_commits_;
    receipt.duplicate = true;
    return receipt;
  }
  record.completed_consumers.insert(position, request.consumer);
  ++completed_edges_;
  receipt.edge_newly_completed = true;
  return receipt;
}

Status CompletionLedger::validate_failure(const EdgeKey& key, ErrorCode code) const {
  if (key.partition.value() >= partition_count_) {
    return Status{make_error(ErrorCode::UnknownPartition, "failure references a partition outside the shuffle")};
  }
  if (code == ErrorCode::Ok) {
    return Status{make_error(ErrorCode::InvalidArgument, "a failure record requires a failure code")};
  }
  const bool known = failures_.find(key) != failures_.end();
  if (!known && completed_edges_ + failure_records_ + 1 > limits_.max_tracked_edges) {
    return Status{make_error(ErrorCode::TooManyTrackedEdges, "tracked edges exceed max_tracked_edges")};
  }
  return Status{};
}

Status CompletionLedger::retire_generation(PartitionId partition, PartitionGeneration generation) {
  if (partition.value() >= partition_count_) {
    return Status{make_error(ErrorCode::UnknownPartition, "retirement references a partition outside the shuffle")};
  }
  const std::uint64_t partition_key_value = partition.value();
  const auto latest = latest_generation_.find(partition_key_value);
  if (latest != latest_generation_.end() && !(latest->second < generation)) {
    return Status{};  // already at or beyond this generation
  }

  if (latest != latest_generation_.end()) {
    const auto previous = partitions_.find(partition_key(partition, latest->second));
    if (previous != partitions_.end()) {
      completed_edges_ -= previous->second.completed_consumers.size();
      if (committed_partitions_ > 0) {
        --committed_partitions_;
      }
    }
    const auto retired = current_permanent_failures_.find(partition_key_value);
    if (retired != current_permanent_failures_.end()) {
      permanent_failures_ = permanent_failures_ >= retired->second ? permanent_failures_ - retired->second : 0;
      retired->second = 0;
    }
  }
  failed_partitions_.erase(partition_key_value);
  latest_generation_[partition_key_value] = generation;
  return Status{};
}

Status CompletionLedger::record_failure(const EdgeKey& key, ErrorCode code, bool permanent, std::uint32_t attempts,
                                        TickId now, TickId ready_at) {
  if (key.partition.value() >= partition_count_) {
    return Status{make_error(ErrorCode::UnknownPartition, "failure references a partition outside the shuffle")};
  }
  if (code == ErrorCode::Ok) {
    return Status{make_error(ErrorCode::InvalidArgument, "a failure record requires a failure code")};
  }
  const auto latest = latest_generation_.find(key.partition.value());
  // A failure of a superseded generation is history: it is stored and readable,
  // but it is not part of the current edge set.
  const bool current = latest == latest_generation_.end() || !(key.partition_generation < latest->second);
  const auto existing = failures_.find(key);
  if (existing == failures_.end()) {
    if (completed_edges_ + failure_records_ + 1 > limits_.max_tracked_edges) {
      return Status{make_error(ErrorCode::TooManyTrackedEdges, "tracked edges exceed max_tracked_edges")};
    }
    FailureRecord record;
    record.key = key;
    record.code = code;
    record.attempts = attempts;
    record.recorded_at = now;
    record.ready_at = ready_at;
    record.permanent = permanent;
    failures_.emplace(key, record);
    ++failure_records_;
    if (permanent && current) {
      ++permanent_failures_;
      ++current_permanent_failures_[key.partition.value()];
      if (partitions_.find(partition_key(key.partition, key.partition_generation)) == partitions_.end()) {
        failed_partitions_.insert(key.partition.value());
      }
    } else if (permanent) {
      // counted as history only
    } else {
      ++retriable_failures_;
    }
    if (is_authority_code(code)) {
      ++authority_refusals_;
    }
    return Status{};
  }

  FailureRecord& record = existing->second;
  const bool was_permanent = record.permanent;
  const bool was_authority = is_authority_code(record.code);
  record.code = code;
  record.attempts = attempts;
  record.recorded_at = now;
  record.ready_at = ready_at;
  record.permanent = permanent;

  if (current && permanent && !was_permanent) {
    ++permanent_failures_;
    ++current_permanent_failures_[key.partition.value()];
    if (retriable_failures_ > 0) {
      --retriable_failures_;
    }
  } else if (current && !permanent && was_permanent) {
    ++retriable_failures_;
    std::uint32_t& counted = current_permanent_failures_[key.partition.value()];
    if (counted > 0) {
      --counted;
    }
    if (permanent_failures_ > 0) {
      --permanent_failures_;
    }
  }
  if (current && permanent &&
      partitions_.find(partition_key(key.partition, key.partition_generation)) == partitions_.end()) {
    failed_partitions_.insert(key.partition.value());
  }
  const bool now_authority = is_authority_code(code);
  if (now_authority && !was_authority) {
    ++authority_refusals_;
  } else if (!now_authority && was_authority && authority_refusals_ > 0) {
    --authority_refusals_;
  }
  return Status{};
}

EdgeStatus CompletionLedger::edge_status(const EdgeKey& key) const {
  EdgeStatus status;
  status.reason = ErrorCode::NotCommitted;

  if (key.partition.value() >= partition_count_) {
    status.phase = EdgePhase::Unknown;
    status.reason = ErrorCode::UnknownPartition;
    return status;
  }

  // Recorded evidence is consulted before the generation comparison: a
  // generation that is older than the latest but still retained is not
  // "purged", it is answered from its own record.
  const auto commit = partitions_.find(partition_key(key.partition, key.partition_generation));
  if (commit != partitions_.end()) {
    const PartitionCommitRecord& record = commit->second;
    status.producer_incarnation = record.producer_incarnation;
    status.manifest_digest = record.manifest_digest;
    status.bytes = record.total_bytes;
    const auto& consumers = record.completed_consumers;
    const auto position = std::lower_bound(consumers.begin(), consumers.end(), key.consumer);
    if (position != consumers.end() && *position == key.consumer) {
      status.phase = EdgePhase::Completed;
      status.reason = ErrorCode::Ok;
      status.authoritative = true;
      const auto failure = failures_.find(key);
      if (failure != failures_.end()) {
        status.attempts = failure->second.attempts;
        status.ready_at = failure->second.ready_at;
      }
      return status;
    }
  }

  const auto failure = failures_.find(key);
  if (failure != failures_.end() && failure->second.permanent) {
    status.attempts = failure->second.attempts;
    status.ready_at = failure->second.ready_at;
    status.reason = failure->second.code;
    status.phase = EdgePhase::Failed;
    return status;
  }

  const auto latest = latest_generation_.find(key.partition.value());
  if (latest != latest_generation_.end() && key.partition_generation < latest->second) {
    // Superseded history that is no longer retained: explicitly purged, never
    // silently reported as pending.
    status.phase = EdgePhase::Purged;
    status.reason = ErrorCode::PurgedHistory;
    return status;
  }

  if (failure != failures_.end()) {
    status.attempts = failure->second.attempts;
    status.ready_at = failure->second.ready_at;
    status.reason = failure->second.code;
  }
  status.phase = EdgePhase::Pending;
  return status;
}

Result<const PartitionCommitRecord*> CompletionLedger::commit_record(PartitionId partition,
                                                                     PartitionGeneration generation) const {
  const auto found = partitions_.find(partition_key(partition, generation));
  if (found == partitions_.end()) {
    return make_failure<const PartitionCommitRecord*>(ErrorCode::NotCommitted, "partition generation is not committed");
  }
  return &found->second;
}

Result<const FailureRecord*> CompletionLedger::failure_record(const EdgeKey& key) const {
  const auto found = failures_.find(key);
  if (found == failures_.end()) {
    return make_failure<const FailureRecord*>(ErrorCode::NotCommitted, "edge has no failure record");
  }
  return &found->second;
}

ProgressSnapshot CompletionLedger::progress(std::uint64_t required_edges, TickId now) const {
  ProgressSnapshot snapshot;
  snapshot.shuffle = shuffle_;
  snapshot.shuffle_generation = shuffle_generation_;
  snapshot.partitions_total = partition_count_;
  snapshot.edges_required = required_edges;
  snapshot.edges_completed = completed_edges_;
  snapshot.edges_failed = permanent_failures_;
  snapshot.bytes_committed = bytes_committed_;
  snapshot.bytes_attempted = attempted_bytes_;
  snapshot.duplicate_commits_suppressed = duplicate_commits_;
  snapshot.retriable_failures = retriable_failures_;
  snapshot.permanent_failures = permanent_failures_;
  snapshot.authority_refusals = authority_refusals_;
  snapshot.tracked_partitions = partitions_.size();
  snapshot.tracked_edges = completed_edges_ + failure_records_;
  snapshot.tick = now;

  snapshot.partitions_committed = committed_partitions_;
  snapshot.partitions_failed = static_cast<std::uint64_t>(failed_partitions_.size());
  snapshot.partitions_incomplete =
      snapshot.partitions_total > snapshot.partitions_committed + snapshot.partitions_failed
          ? snapshot.partitions_total - snapshot.partitions_committed - snapshot.partitions_failed
          : 0;

  const std::uint64_t resolved = snapshot.edges_completed + snapshot.edges_failed;
  snapshot.edges_incomplete = snapshot.edges_required > resolved ? snapshot.edges_required - resolved : 0;
  snapshot.edges_over_counted = resolved > snapshot.edges_required ? resolved - snapshot.edges_required : 0;
  return snapshot;
}

Explanation CompletionLedger::explain(std::uint32_t max_samples_per_code) const {
  Explanation explanation;
  std::map<std::uint16_t, ExplainEntry> grouped;
  std::map<std::uint16_t, std::uint64_t> counts;

  for (const auto& entry : failures_) {
    const FailureRecord& record = entry.second;
    const auto code = static_cast<std::uint16_t>(record.code);
    counts[code] += 1;
    auto found = grouped.find(code);
    if (found == grouped.end()) {
      ExplainEntry sample;
      sample.code = record.code;
      sample.subject = record.key.to_string();
      sample.detail = std::string{"permanent="} + (record.permanent ? "yes" : "no") + " attempts=" +
                      std::to_string(record.attempts) + " ready_at=" + record.ready_at.to_string();
      grouped.emplace(code, std::move(sample));
    } else if (record.key.to_string() < found->second.subject) {
      found->second.subject = record.key.to_string();
    }
  }

  static_cast<void>(max_samples_per_code);
  for (auto& entry : grouped) {
    entry.second.count = counts[entry.first];
    explanation.entries.push_back(std::move(entry.second));
  }

  if (duplicate_commits_ > 0) {
    ExplainEntry entry;
    entry.code = ErrorCode::DuplicateCompletion;
    entry.subject = "ledger";
    entry.detail = "duplicate completions suppressed without double accounting";
    entry.count = duplicate_commits_;
    explanation.entries.push_back(std::move(entry));
  }
  return explanation;
}

std::string Explanation::render() const {
  if (entries.empty()) {
    return "no recorded refusals or failures\n";
  }
  std::string text;
  for (const ExplainEntry& entry : entries) {
    text += std::to_string(entry.count) + "x " + to_string(entry.code) + " [" + entry.subject + "] " + entry.detail +
            "\n";
  }
  return text;
}

Status CompletionLedger::encode(ByteWriter& writer) const {
  writer.put_u64(shuffle_.value());
  writer.put_u64(shuffle_generation_.value());
  writer.put_u32(partition_count_);
  writer.put_u64(next_sequence_.value());
  writer.put_u64(bytes_committed_);
  writer.put_u64(duplicate_commits_);
  writer.put_u64(attempted_bytes_);
  writer.put_u64(retriable_failures_);
  writer.put_u64(permanent_failures_);
  writer.put_u64(authority_refusals_);

  std::vector<std::uint64_t> keys;
  keys.reserve(partitions_.size());
  for (const auto& entry : partitions_) {
    keys.push_back(entry.first);
  }
  std::sort(keys.begin(), keys.end());
  writer.put_u32(static_cast<std::uint32_t>(keys.size()));
  for (const std::uint64_t key : keys) {
    const PartitionCommitRecord& record = partitions_.at(key);
    writer.put_u64(record.anchor.partition.value());
    writer.put_u64(record.anchor.partition_generation.value());
    writer.put_u64(record.shuffle.value());
    writer.put_u64(record.shuffle_generation.value());
    writer.put_u64(record.producer.value());
    writer.put_u64(record.producer_incarnation.value());
    writer.put_digest(record.manifest_digest);
    writer.put_u64(record.total_bytes);
    writer.put_u64(record.topology_generation.value());
    writer.put_u64(record.sequence.value());
    writer.put_u64(record.committed_at.value());
    writer.put_u32(record.duplicate_commits);
    writer.put_u32(static_cast<std::uint32_t>(record.completed_consumers.size()));
    for (const ConsumerId consumer : record.completed_consumers) {
      writer.put_u64(consumer.value());
    }
  }

  std::vector<EdgeKey> failure_keys;
  failure_keys.reserve(failures_.size());
  for (const auto& entry : failures_) {
    failure_keys.push_back(entry.first);
  }
  std::sort(failure_keys.begin(), failure_keys.end());
  writer.put_u32(static_cast<std::uint32_t>(failure_keys.size()));
  for (const EdgeKey& key : failure_keys) {
    const FailureRecord& record = failures_.at(key);
    writer.put_u64(record.key.partition.value());
    writer.put_u64(record.key.partition_generation.value());
    writer.put_u64(record.key.consumer.value());
    writer.put_u16(static_cast<std::uint16_t>(record.code));
    writer.put_u32(record.attempts);
    writer.put_u64(record.recorded_at.value());
    writer.put_u64(record.ready_at.value());
    writer.put_bool(record.permanent);
  }

  std::vector<std::uint64_t> generation_keys;
  generation_keys.reserve(latest_generation_.size());
  for (const auto& entry : latest_generation_) {
    generation_keys.push_back(entry.first);
  }
  std::sort(generation_keys.begin(), generation_keys.end());
  writer.put_u32(static_cast<std::uint32_t>(generation_keys.size()));
  for (const std::uint64_t key : generation_keys) {
    writer.put_u64(key);
    writer.put_u64(latest_generation_.at(key).value());
  }
  return Status{};
}

Status CompletionLedger::decode(ByteReader& reader, const Limits& limits) {
  CompletionLedger decoded{limits};
  decoded.shuffle_ = ShuffleId{reader.u64()};
  decoded.shuffle_generation_ = ShuffleGeneration{reader.u64()};
  decoded.partition_count_ = reader.u32();
  decoded.next_sequence_ = CommitSequence{reader.u64()};
  decoded.bytes_committed_ = reader.u64();
  decoded.duplicate_commits_ = reader.u64();
  decoded.attempted_bytes_ = reader.u64();
  decoded.retriable_failures_ = reader.u64();
  decoded.permanent_failures_ = reader.u64();
  decoded.authority_refusals_ = reader.u64();
  if (!reader.ok()) {
    return reader.status();
  }

  const std::uint32_t partition_records = reader.collection_count(limits.max_tracked_partitions);
  for (std::uint32_t index = 0; index < partition_records; ++index) {
    PartitionCommitRecord record;
    record.anchor.partition = PartitionId{reader.u64()};
    record.anchor.partition_generation = PartitionGeneration{reader.u64()};
    record.anchor.consumer = ConsumerId{};
    record.shuffle = ShuffleId{reader.u64()};
    record.shuffle_generation = ShuffleGeneration{reader.u64()};
    record.producer = ProducerId{reader.u64()};
    record.producer_incarnation = IncarnationId{reader.u64()};
    record.manifest_digest = reader.digest();
    record.total_bytes = reader.u64();
    record.topology_generation = TopologyGeneration{reader.u64()};
    record.sequence = CommitSequence{reader.u64()};
    record.committed_at = TickId{reader.u64()};
    record.duplicate_commits = reader.u32();
    const std::uint32_t consumer_count = reader.collection_count(limits.max_collection_items);
    record.completed_consumers.reserve(consumer_count);
    for (std::uint32_t consumer = 0; consumer < consumer_count; ++consumer) {
      record.completed_consumers.push_back(ConsumerId{reader.u64()});
    }
    if (!reader.ok()) {
      return reader.status();
    }
    if (record.anchor.partition.value() >= decoded.partition_count_) {
      return Status{make_error(ErrorCode::StateImpossible, "durable ledger references a partition outside the shuffle")};
    }
    if (!std::is_sorted(record.completed_consumers.begin(), record.completed_consumers.end()) ||
        std::adjacent_find(record.completed_consumers.begin(), record.completed_consumers.end()) !=
            record.completed_consumers.end()) {
      return Status{make_error(ErrorCode::StateCorrupt, "durable ledger holds a non-canonical consumer set")};
    }
    const std::uint64_t key = partition_key(record.anchor.partition, record.anchor.partition_generation);
    if (!decoded.partitions_.emplace(key, std::move(record)).second) {
      return Status{make_error(ErrorCode::StateCorrupt, "durable ledger holds a duplicate partition record")};
    }
  }

  const std::uint32_t failure_count = reader.collection_count(limits.max_tracked_edges);
  for (std::uint32_t index = 0; index < failure_count; ++index) {
    FailureRecord record;
    record.key.partition = PartitionId{reader.u64()};
    record.key.partition_generation = PartitionGeneration{reader.u64()};
    record.key.consumer = ConsumerId{reader.u64()};
    const std::uint16_t code = reader.u16();
    record.attempts = reader.u32();
    record.recorded_at = TickId{reader.u64()};
    record.ready_at = TickId{reader.u64()};
    record.permanent = reader.boolean();
    if (!reader.ok()) {
      return reader.status();
    }
    if (code == 0 || code > static_cast<std::uint16_t>(ErrorCode::TooManyTrackedEdges)) {
      return Status{make_error(ErrorCode::StateImpossible, "durable ledger holds an unknown error code")};
    }
    record.code = static_cast<ErrorCode>(code);
    if (!decoded.failures_.emplace(record.key, record).second) {
      return Status{make_error(ErrorCode::StateCorrupt, "durable ledger holds a duplicate failure record")};
    }
  }

  const std::uint32_t generation_records = reader.collection_count(limits.max_partitions);
  for (std::uint32_t index = 0; index < generation_records; ++index) {
    const std::uint64_t key = reader.u64();
    const PartitionGeneration generation{reader.u64()};
    if (!reader.ok()) {
      return reader.status();
    }
    decoded.latest_generation_[key] = generation;
  }

  if (!reader.ok()) {
    return reader.status();
  }

  // Derived counters are recomputed from the records rather than trusted from
  // the bytes: the current edge set has exactly one definition, and a durable
  // file that disagrees with it is not silently believed.
  decoded.completed_edges_ = 0;
  decoded.committed_partitions_ = 0;
  decoded.permanent_failures_ = 0;
  decoded.current_permanent_failures_.clear();
  decoded.failed_partitions_.clear();
  for (const auto& entry : decoded.partitions_) {
    const PartitionCommitRecord& record = entry.second;
    const auto latest = decoded.latest_generation_.find(record.anchor.partition.value());
    if (latest == decoded.latest_generation_.end() || record.anchor.partition_generation != latest->second) {
      continue;  // superseded generation: readable history, not a current edge
    }
    decoded.completed_edges_ += record.completed_consumers.size();
    ++decoded.committed_partitions_;
  }
  for (const auto& entry : decoded.failures_) {
    const FailureRecord& record = entry.second;
    if (!record.permanent) {
      continue;
    }
    const auto latest = decoded.latest_generation_.find(record.key.partition.value());
    const bool current =
        latest == decoded.latest_generation_.end() || !(record.key.partition_generation < latest->second);
    if (!current) {
      continue;
    }
    ++decoded.permanent_failures_;
    ++decoded.current_permanent_failures_[record.key.partition.value()];
    if (decoded.partitions_.find(partition_key(record.key.partition, record.key.partition_generation)) ==
        decoded.partitions_.end()) {
      decoded.failed_partitions_.insert(record.key.partition.value());
    }
  }
  decoded.failure_records_ = decoded.failures_.size();

  // The decoded value is swapped in only after every field validated, so a
  // malformed record can never partially apply.
  shuffle_ = decoded.shuffle_;
  shuffle_generation_ = decoded.shuffle_generation_;
  partition_count_ = decoded.partition_count_;
  next_sequence_ = decoded.next_sequence_;
  bytes_committed_ = decoded.bytes_committed_;
  duplicate_commits_ = decoded.duplicate_commits_;
  attempted_bytes_ = decoded.attempted_bytes_;
  retriable_failures_ = decoded.retriable_failures_;
  permanent_failures_ = decoded.permanent_failures_;
  authority_refusals_ = decoded.authority_refusals_;
  completed_edges_ = decoded.completed_edges_;
  committed_partitions_ = decoded.committed_partitions_;
  failure_records_ = decoded.failure_records_;
  current_permanent_failures_ = std::move(decoded.current_permanent_failures_);
  failed_partitions_ = std::move(decoded.failed_partitions_);
  partitions_ = std::move(decoded.partitions_);
  failures_ = std::move(decoded.failures_);
  latest_generation_ = std::move(decoded.latest_generation_);
  retained_keys_ = std::move(decoded.retained_keys_);
  return Status{};
}

}  // namespace shuffle::fabric
