// The completion ledger: authoritative, exactly-once accounting per partition
// generation, plus the explicit record of everything that did not complete.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Authority rules enforced here:
//
//   * completion is recorded once per (partition, partition generation);
//     repeating it is suppressed and never accounts bytes twice;
//   * a second, divergent commit for the same partition generation (different
//     manifest digest or producing incarnation) is refused, never merged;
//   * a refused or failed edge is recorded explicitly. An edge is therefore
//     always completed, failed, or pending -- never silently absent.

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "shuffle/fabric/bytes.hpp"
#include "shuffle/fabric/codec.hpp"
#include "shuffle/fabric/edge.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/identity.hpp"

namespace shuffle::fabric {

struct PartitionCommitRecord {
  EdgeKey anchor{};  // partition + generation; consumer is not part of the key
  ShuffleId shuffle{};
  ShuffleGeneration shuffle_generation{};
  ProducerId producer{};
  IncarnationId producer_incarnation{};
  Digest manifest_digest{};
  std::uint64_t total_bytes{0};
  TopologyGeneration topology_generation{};
  CommitSequence sequence{};
  TickId committed_at{};
  std::vector<ConsumerId> completed_consumers{};  // ascending, unique
  std::uint32_t duplicate_commits{0};
};

struct CommitRequest {
  EdgeKey edge{};
  TransferAttemptId attempt{};
  WaveId wave{};
  ShuffleGeneration shuffle_generation{};
  TopologyGeneration topology_generation{};
  PolicyGeneration policy_generation{};
  ProducerId producer{};
  IncarnationId producer_incarnation{};
  ConsumerId consumer{};
  IncarnationId consumer_incarnation{};
  Digest manifest_digest{};
  Digest observed_partition_digest{};
  // Per-chunk digests the consumer observed while assembling the partition, in
  // manifest order. The coordinator compares them with the manifest, which is
  // the strongest content evidence a non-authenticated protocol can carry.
  std::vector<Digest> observed_chunk_digests{};
  std::uint64_t bytes{0};
  bool integrity_verified{false};
};

struct CommitReceipt {
  bool newly_committed{false};       // the partition generation was committed by this call
  bool edge_newly_completed{false};  // this consumer was added to the completed set
  bool duplicate{false};             // nothing changed: a full duplicate was suppressed
  CommitSequence sequence{};
  Digest manifest_digest{};
  std::uint64_t accounted_bytes{0};
};

struct FailureRecord {
  EdgeKey key{};
  ErrorCode code{ErrorCode::InternalError};
  std::uint32_t attempts{0};
  TickId recorded_at{};
  TickId ready_at{};
  bool permanent{false};
};

struct ProgressSnapshot {
  ShuffleId shuffle{};
  ShuffleGeneration shuffle_generation{};
  std::uint64_t partitions_total{0};
  std::uint64_t partitions_committed{0};
  std::uint64_t partitions_failed{0};
  std::uint64_t partitions_incomplete{0};
  std::uint64_t edges_required{0};
  std::uint64_t edges_completed{0};
  std::uint64_t edges_failed{0};
  std::uint64_t edges_incomplete{0};
  // Non-zero when the resolved edge count exceeds the required set, which is
  // what a saturating subtraction would otherwise hide. Withdrawal of a
  // consumer after its edges completed is the legitimate case; a defect looks
  // the same until this counter says so.
  std::uint64_t edges_over_counted{0};
  // Cumulative bytes moved: every committed partition generation counts once,
  // including generations that were later superseded.
  std::uint64_t bytes_committed{0};
  std::uint64_t bytes_attempted{0};
  std::uint64_t duplicate_commits_suppressed{0};
  std::uint64_t retriable_failures{0};
  std::uint64_t permanent_failures{0};
  std::uint64_t authority_refusals{0};
  std::uint64_t tracked_partitions{0};
  std::uint64_t tracked_edges{0};
  TickId tick{};

  // The closure invariant, expressed as a predicate instead of a hope.
  [[nodiscard]] bool accounting_closes() const noexcept {
    return edges_over_counted == 0 && edges_completed + edges_failed + edges_incomplete == edges_required;
  }
};

struct ExplainEntry {
  ErrorCode code{ErrorCode::Ok};
  std::string subject{};
  std::string detail{};
  std::uint64_t count{0};
};

struct Explanation {
  std::vector<ExplainEntry> entries{};
  [[nodiscard]] std::string render() const;
};

class CompletionLedger : public CompletionView {
 public:
  explicit CompletionLedger(const Limits& limits);

  void bind(ShuffleId shuffle, ShuffleGeneration generation, std::uint32_t partition_count);

  [[nodiscard]] ShuffleId shuffle() const noexcept { return shuffle_; }
  [[nodiscard]] ShuffleGeneration shuffle_generation() const noexcept { return shuffle_generation_; }

  // Records an authoritative completion. The caller must already have verified
  // authority (attempt identity, incarnations, generations) and integrity.
  // validate_commit performs every check commit() performs without mutating
  // state, so a caller can make the mutation durable in between and know the
  // application cannot fail afterwards.
  [[nodiscard]] Status validate_commit(const CommitRequest& request) const;
  [[nodiscard]] Result<CommitReceipt> commit(const CommitRequest& request, TickId now);

  // Advances the partition's current generation without a completion: the
  // completions and permanent failures of every older generation stop counting
  // toward the required edge set. The coordinator calls this when a producer
  // re-produces a partition, so a stale-generation completion cannot keep
  // satisfying an edge that now requires fresh content.
  [[nodiscard]] Status retire_generation(PartitionId partition, PartitionGeneration generation);

  // Checks everything record_failure would check, without mutating state, so a
  // caller can make the record durable knowing the application cannot fail.
  [[nodiscard]] Status validate_failure(const EdgeKey& key, ErrorCode code) const;

  // Records a non-completion. Retriable failures keep the edge pending with a
  // readiness tick; permanent and authority failures resolve it explicitly.
  [[nodiscard]] Status record_failure(const EdgeKey& key, ErrorCode code, bool permanent, std::uint32_t attempts,
                                      TickId now, TickId ready_at);

  [[nodiscard]] EdgeStatus edge_status(const EdgeKey& key) const override;

  [[nodiscard]] Result<const PartitionCommitRecord*> commit_record(PartitionId partition,
                                                                  PartitionGeneration generation) const;
  [[nodiscard]] Result<const FailureRecord*> failure_record(const EdgeKey& key) const;

  // Bytes carried by a dispatched attempt. Attempted bytes are a pressure
  // signal; they are never completion accounting.
  void note_attempt_bytes(std::uint64_t bytes) noexcept { attempted_bytes_ += bytes; }

  [[nodiscard]] ProgressSnapshot progress(std::uint64_t required_edges, TickId now) const;
  [[nodiscard]] Explanation explain(std::uint32_t max_samples_per_code) const;

  [[nodiscard]] std::uint64_t tracked_partitions() const noexcept { return partitions_.size(); }
  [[nodiscard]] std::uint64_t tracked_edges() const noexcept { return completed_edges_ + failure_records_; }
  [[nodiscard]] const Limits& limits() const noexcept { return limits_; }
  [[nodiscard]] const std::unordered_map<std::uint64_t, PartitionCommitRecord>& commit_records() const noexcept {
    return partitions_;
  }
  [[nodiscard]] const std::unordered_map<EdgeKey, FailureRecord>& failure_records() const noexcept {
    return failures_;
  }

  // Durable round trip of the ledger's own state. Decoding is strict and never
  // partially applies: the caller swaps in the decoded value on success only.
  [[nodiscard]] Status encode(ByteWriter& writer) const;
  [[nodiscard]] Status decode(ByteReader& reader, const Limits& limits);

 private:
  [[nodiscard]] static std::uint64_t partition_key(PartitionId partition, PartitionGeneration generation) noexcept;
  [[nodiscard]] bool retain_generation(PartitionGeneration generation) const;

  Limits limits_{};
  ShuffleId shuffle_{};
  ShuffleGeneration shuffle_generation_{};
  std::uint32_t partition_count_{0};
  CommitSequence next_sequence_{1};
  std::unordered_map<std::uint64_t, PartitionCommitRecord> partitions_{};
  std::unordered_map<EdgeKey, FailureRecord> failures_{};
  std::unordered_map<std::uint64_t, PartitionGeneration> latest_generation_{};
  std::unordered_map<std::uint64_t, std::vector<std::uint64_t>> retained_keys_{};
  // Completions and permanent failures are counted for the *current* generation
  // of each partition only: re-producing a partition retires the completions of
  // the superseded generation from the required edge set (they stay readable as
  // history). This is what keeps completed + failed + incomplete == required
  // across a re-production instead of double counting.
  std::unordered_map<std::uint64_t, std::uint32_t> current_permanent_failures_{};
  std::unordered_set<std::uint64_t> failed_partitions_{};
  std::uint64_t completed_edges_{0};
  std::uint64_t committed_partitions_{0};
  std::uint64_t failure_records_{0};
  std::uint64_t bytes_committed_{0};
  std::uint64_t duplicate_commits_{0};
  std::uint64_t attempted_bytes_{0};
  std::uint64_t retriable_failures_{0};
  std::uint64_t permanent_failures_{0};
  std::uint64_t authority_refusals_{0};
  std::uint64_t first_retained_sequence_{0};
};

}  // namespace shuffle::fabric

namespace std {

template <>
struct hash<shuffle::fabric::EdgeKey>;

}  // namespace std
