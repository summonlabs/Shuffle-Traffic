// The coordinator: the single place where authority is granted, bound,
// refused and accounted for.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// Ordering rule for every durable mutation:
//   validate authority -> encode record -> make the record durable ->
//   apply it in memory -> answer.
// A caller is never told "accepted" before the durability point the answer
// claims. Planning is not authority, dispatch is not completion, and a
// recovered peer is never treated as a current one.
//
// The coordinator performs no I/O of its own: durable records are handed to a
// DurableSink, which the service layer backs with the journal store for a real
// deployment and with VolatileSink only where durability is explicitly not
// claimed (reported through durable()).

#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "shuffle/fabric/codec.hpp"
#include "shuffle/fabric/edge.hpp"
#include "shuffle/fabric/ledger.hpp"
#include "shuffle/fabric/manifest.hpp"
#include "shuffle/fabric/policy.hpp"
#include "shuffle/fabric/records.hpp"
#include "shuffle/fabric/schedule.hpp"
#include "shuffle/fabric/topology.hpp"

namespace shuffle::fabric {

class DurableSink {
 public:
  DurableSink() = default;
  virtual ~DurableSink() = default;
  DurableSink(const DurableSink&) = delete;
  DurableSink& operator=(const DurableSink&) = delete;
  DurableSink(DurableSink&&) = delete;
  DurableSink& operator=(DurableSink&&) = delete;

  // Must return only once the record is durable, or report why it is not.
  [[nodiscard]] virtual Status persist(std::span<const std::byte> record) = 0;
  // False for sinks that accept records without making them durable; the
  // coordinator then never reports durable() == true.
  [[nodiscard]] virtual bool claims_durability() const noexcept { return true; }
};

// Backs the coordinator without claiming durability. The coordinator reports
// durable() == false and no surface claims a durable commit.
class VolatileSink final : public DurableSink {
 public:
  [[nodiscard]] Status persist(std::span<const std::byte>) override { return Status{}; }
  [[nodiscard]] bool claims_durability() const noexcept override { return false; }
};

struct ShuffleOpenRequest {
  ShuffleId shuffle{};
  ShuffleGeneration generation{};
  std::uint32_t partition_count{0};
  PolicyEnvelope policy{};
};

struct RegistrationOutcome {
  TopologyGeneration topology_generation{};
  IncarnationId incarnation{};
  bool superseded_previous{false};
};

struct ManifestAcceptance {
  Digest manifest_digest{};
  PartitionGeneration partition_generation{};
};

struct CommitOutcome {
  CommitReceipt receipt{};
  bool shuffle_completed{false};
};

struct FailureOutcome {
  bool permanent{false};
  std::uint32_t attempts{0};
  TickId ready_at{};
};

// Congestion evidence is advisory and must come from a current participant.
struct CongestionIntent {
  ParticipantKind reporter_kind{ParticipantKind::Consumer};
  std::uint64_t reporter_id{0};
  IncarnationId reporter_incarnation{};
  ProducerId producer{};
  ConsumerId consumer{};
  std::uint32_t level{0};
  PolicyGeneration policy_generation{};
  TickId observed_at{};

  [[nodiscard]] bool is_global() const noexcept { return producer.is_zero() && consumer.is_zero(); }
};

struct CoordinatorStatus {
  ShuffleState state{ShuffleState::Closed};
  ErrorCode reason{ErrorCode::Ok};
  std::uint64_t epoch{0};
  ShuffleId shuffle{};
  ShuffleGeneration shuffle_generation{};
  TopologyGeneration topology_generation{};
  PolicyGeneration policy_generation{};
  TickId tick{};
  std::uint32_t active_producers{0};
  std::uint32_t active_consumers{0};
  std::uint32_t in_flight{0};
  std::uint64_t persisted_records{0};
  bool durable{false};
  bool revalidation_required{true};
  // True when recovery discarded bytes at the journal tail. The discarded
  // record was never acknowledged, so it is reported rather than repaired.
  bool history_incomplete{false};
  ProgressSnapshot progress{};
};

class Coordinator final : private PartitionView, private CongestionView {
 public:
  Coordinator(const Limits& limits, DurableSink* sink);
  ~Coordinator() override = default;
  Coordinator(const Coordinator&) = delete;
  Coordinator& operator=(const Coordinator&) = delete;
  Coordinator(Coordinator&&) = delete;
  Coordinator& operator=(Coordinator&&) = delete;

  // Opens a fresh shuffle. Refuses when a shuffle is already open.
  [[nodiscard]] Status open_shuffle(const ShuffleOpenRequest& request);

  // Rebuilds authority from durable bytes: snapshot records first, then the
  // journal in order. Participants come back as Suspect, congestion evidence
  // is dropped, and no pre-restart attempt keeps authority.
  [[nodiscard]] Status recover(std::span<const std::byte> snapshot,
                               const std::vector<std::vector<std::byte>>& journal, bool history_incomplete);

  [[nodiscard]] Result<RegistrationOutcome> register_producer(ProducerId id, IncarnationId incarnation,
                                                              std::string endpoint);
  [[nodiscard]] Result<RegistrationOutcome> register_consumer(ConsumerId id, IncarnationId incarnation,
                                                              std::string endpoint, const PartitionSelection& selection);
  [[nodiscard]] Status set_participant_state(ParticipantKind kind, std::uint64_t id, IncarnationId incarnation,
                                             ParticipantState state);

  [[nodiscard]] Result<ManifestAcceptance> publish_manifest(const PartitionManifest& manifest);

  // Produces the next bounded wave and registers authority for every grant.
  [[nodiscard]] Result<WavePlan> next_wave();

  // Validates attempt-bound authority and commits exactly once.
  [[nodiscard]] Result<CommitOutcome> commit_transfer(const CommitRequest& request);
  [[nodiscard]] Result<FailureOutcome> report_failure(TransferAttemptId attempt, ErrorCode code);

  [[nodiscard]] Status update_congestion(const CongestionIntent& intent);
  [[nodiscard]] Status cancel_shuffle(ErrorCode reason);

  [[nodiscard]] Result<TickId> advance_tick();

  // The accepted manifest for a partition, which is what a consumer verifies
  // its chunks against. Absence is reported explicitly, never as an empty
  // manifest.
  [[nodiscard]] Result<PartitionManifest> manifest_of(PartitionId partition) const;

  [[nodiscard]] Result<ProgressSnapshot> progress() const;
  [[nodiscard]] Result<Explanation> explain(std::uint32_t max_samples_per_code) const;
  [[nodiscard]] Result<CoordinatorStatus> status() const;
  [[nodiscard]] Result<std::vector<std::byte>> snapshot_payload() const;

  [[nodiscard]] bool durable() const noexcept { return durable_; }
  [[nodiscard]] ShuffleState state() const noexcept { return state_; }
  [[nodiscard]] std::uint64_t epoch() const noexcept { return epoch_; }
  [[nodiscard]] bool revalidation_required() const noexcept { return pending_revalidation_ > 0; }
  [[nodiscard]] bool history_incomplete() const noexcept { return history_incomplete_; }
  [[nodiscard]] std::uint32_t pending_revalidation() const noexcept { return pending_revalidation_; }
  [[nodiscard]] const Limits& limits() const noexcept { return limits_; }

 private:
  // A guard that detects re-entrant entry into the coordinator, which is the
  // shape a callback-under-lock or self-recursive authority path would take.
  class Guard {
   public:
    explicit Guard(const Coordinator& owner);
    ~Guard();
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
    [[nodiscard]] bool engaged() const noexcept { return engaged_; }

   private:
    const Coordinator* owner_;
    bool engaged_;
  };

  [[nodiscard]] PartitionFacts partition_facts(PartitionId id) const override;
  [[nodiscard]] PressureReading producer_pressure(ProducerId id) const override;
  [[nodiscard]] PressureReading consumer_pressure(ConsumerId id) const override;

  void adopt(Coordinator&& other);
  [[nodiscard]] Status persist(std::span<const std::byte> record);
  [[nodiscard]] Status commit_record(ByteWriter& writer);
  [[nodiscard]] Status apply_record(std::span<const std::byte> record);
  [[nodiscard]] Status apply_shuffle_state(const ShuffleStateRecord& record);
  [[nodiscard]] Status apply_participant(const ParticipantRecord& record);
  [[nodiscard]] Status apply_manifest(const ManifestRecord& record);
  [[nodiscard]] Status apply_commit(const CommitRecord& record);
  [[nodiscard]] Status apply_failure(const FailureRecordDurable& record);
  [[nodiscard]] Status persist_epoch();
  [[nodiscard]] Status ensure_scheduler();
  [[nodiscard]] Status ensure_open() const;
  [[nodiscard]] PressureReading freshen(const PressureReading& reading) const;
  [[nodiscard]] ProgressSnapshot compute_progress() const;
  [[nodiscard]] Explanation build_explanation(std::uint32_t max_samples_per_code) const;
  void note_completion();

  Limits limits_{};
  DurableSink* sink_{nullptr};
  bool durable_{false};
  mutable bool inside_{false};

  ShuffleState state_{ShuffleState::Closed};
  ErrorCode reason_{ErrorCode::Ok};
  ShuffleOpenRequest request_{};
  std::uint64_t epoch_{0};
  LogicalClock clock_{};
  std::optional<Topology> topology_{};
  CompletionLedger ledger_;
  WaveScheduler scheduler_;
  std::unordered_map<std::uint64_t, PartitionManifest> manifests_{};
  std::unordered_map<std::uint64_t, PartitionFacts> facts_{};
  std::unordered_map<std::uint64_t, DispatchGrant> grants_{};
  std::unordered_map<std::uint64_t, PressureReading> producer_pressure_{};
  std::unordered_map<std::uint64_t, PressureReading> consumer_pressure_{};
  std::optional<PressureReading> global_pressure_{};
  std::unordered_set<std::uint64_t> unrevalidated_producers_{};
  std::unordered_set<std::uint64_t> unrevalidated_consumers_{};
  std::uint32_t pending_revalidation_{0};
  bool history_incomplete_{false};
  std::uint64_t persisted_records_{0};
  std::uint64_t commits_applied_{0};
  std::uint64_t failures_applied_{0};
};

}  // namespace shuffle::fabric
