// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/coordinator.hpp"

#include <algorithm>
#include <string>
#include <utility>

namespace shuffle::fabric {
namespace {

// Version of the snapshot payload (a record list followed by ledger state).
constexpr std::uint32_t kSnapshotPayloadVersion = 1;


}  // namespace

Coordinator::Guard::Guard(const Coordinator& owner) : owner_(&owner), engaged_(!owner.inside_) {
  if (engaged_) {
    owner_->inside_ = true;
  }
}

Coordinator::Guard::~Guard() {
  if (engaged_) {
    owner_->inside_ = false;
  }
}

Coordinator::Coordinator(const Limits& limits, DurableSink* sink)
    : limits_(limits), sink_(sink), durable_(sink != nullptr && sink->claims_durability()), ledger_(limits),
      scheduler_(limits) {}

// ---------------------------------------------------------------------------
// Durability plumbing
// ---------------------------------------------------------------------------

void Coordinator::adopt(Coordinator&& other) {
  // Every authority-bearing member is transferred together; nothing of the
  // previous state survives the adoption.
  state_ = other.state_;
  reason_ = other.reason_;
  request_ = std::move(other.request_);
  epoch_ = other.epoch_;
  clock_ = other.clock_;
  topology_ = std::move(other.topology_);
  ledger_ = std::move(other.ledger_);
  scheduler_ = std::move(other.scheduler_);
  manifests_ = std::move(other.manifests_);
  facts_ = std::move(other.facts_);
  grants_.clear();
  producer_pressure_ = std::move(other.producer_pressure_);
  consumer_pressure_ = std::move(other.consumer_pressure_);
  global_pressure_ = other.global_pressure_;
  unrevalidated_producers_ = std::move(other.unrevalidated_producers_);
  unrevalidated_consumers_ = std::move(other.unrevalidated_consumers_);
  pending_revalidation_ = other.pending_revalidation_;
  history_incomplete_ = other.history_incomplete_;
  persisted_records_ = other.persisted_records_;
  commits_applied_ = other.commits_applied_;
  failures_applied_ = other.failures_applied_;
}

Status Coordinator::persist(std::span<const std::byte> record) {
  if (sink_ == nullptr) {
    return Status{};
  }
  const Status status = sink_->persist(record);
  if (status.ok()) {
    ++persisted_records_;
  }
  return status;
}

Status Coordinator::commit_record(ByteWriter& writer) {
  const Status persisted = persist(writer.data());
  if (!persisted.ok()) {
    return persisted;
  }
  return apply_record(writer.data());
}

Status Coordinator::persist_epoch() {
  EpochRecord record;
  record.epoch = epoch_ + 1;
  record.opened_at = clock_.now();
  ByteWriter writer;
  record.encode(writer);
  return commit_record(writer);
}

Status Coordinator::apply_record(std::span<const std::byte> record) {
  ByteReader reader{record, limits_, "durable record"};
  const auto kind = peek_record_kind(reader);
  if (!kind.ok()) {
    return kind.status();
  }

  switch (kind.value()) {
    case RecordKind::EpochAdvanced: {
      const auto decoded = EpochRecord::decode(reader);
      if (!decoded.ok()) {
        return Status{decoded.error()};
      }
      const Status end = reader.require_end();
      if (!end.ok()) {
        return end;
      }
      if (decoded.value().epoch > epoch_) {
        epoch_ = decoded.value().epoch;
      }
      if (decoded.value().opened_at > clock_.now()) {
        clock_.reset(decoded.value().opened_at);
      }
      return Status{};
    }
    case RecordKind::ShuffleState: {
      const auto decoded = ShuffleStateRecord::decode(reader);
      if (!decoded.ok()) {
        return Status{decoded.error()};
      }
      const Status end = reader.require_end();
      if (!end.ok()) {
        return end;
      }
      return apply_shuffle_state(decoded.value());
    }
    case RecordKind::Participant: {
      const auto decoded = ParticipantRecord::decode(reader, limits_);
      if (!decoded.ok()) {
        return Status{decoded.error()};
      }
      const Status end = reader.require_end();
      if (!end.ok()) {
        return end;
      }
      return apply_participant(decoded.value());
    }
    case RecordKind::Manifest: {
      const auto decoded = ManifestRecord::decode(reader, limits_);
      if (!decoded.ok()) {
        return Status{decoded.error()};
      }
      const Status end = reader.require_end();
      if (!end.ok()) {
        return end;
      }
      return apply_manifest(decoded.value());
    }
    case RecordKind::Commit: {
      const auto decoded = CommitRecord::decode(reader, limits_);
      if (!decoded.ok()) {
        return Status{decoded.error()};
      }
      const Status end = reader.require_end();
      if (!end.ok()) {
        return end;
      }
      return apply_commit(decoded.value());
    }
    case RecordKind::Failure: {
      const auto decoded = FailureRecordDurable::decode(reader);
      if (!decoded.ok()) {
        return Status{decoded.error()};
      }
      const Status end = reader.require_end();
      if (!end.ok()) {
        return end;
      }
      return apply_failure(decoded.value());
    }
  }
  return Status{make_error(ErrorCode::StateCorrupt, "durable record kind is not handled")};
}

Status Coordinator::apply_shuffle_state(const ShuffleStateRecord& record) {
  if (state_ != ShuffleState::Closed &&
      (record.shuffle != request_.shuffle || record.generation != request_.generation)) {
    return Status{make_error(ErrorCode::StateImpossible, "durable state describes another shuffle generation")};
  }
  if (record.state == ShuffleState::Open) {
    if (state_ == ShuffleState::Cancelled || state_ == ShuffleState::Completed || state_ == ShuffleState::Failed) {
      return Status{make_error(ErrorCode::StateImpossible, "durable state reopens a finished shuffle")};
    }
    request_.shuffle = record.shuffle;
    request_.generation = record.generation;
    request_.partition_count = record.partition_count;
    request_.policy = record.policy;
    if (!topology_) {
      topology_.emplace(record.shuffle, record.generation, record.partition_count, limits_);
    }
    ledger_.bind(record.shuffle, record.generation, record.partition_count);
  }

  state_ = record.state;
  reason_ = record.reason;
  if (state_ == ShuffleState::Cancelled || state_ == ShuffleState::Cancelling) {
    // Cancellation stops new dispatch and removes authority from work already
    // in flight: nothing dispatched before the cancellation can commit after it.
    static_cast<void>(scheduler_.abandon_all(reason_));
    grants_.clear();
  }
  if (record.at > clock_.now()) {
    clock_.reset(record.at);
  }
  return Status{};
}

Status Coordinator::apply_participant(const ParticipantRecord& record) {
  if (!topology_) {
    return Status{make_error(ErrorCode::InvalidState, "participant record arrived before the shuffle was opened")};
  }

  IncarnationId previous_incarnation{};
  bool had_previous = false;
  if (record.kind == ParticipantKind::Producer) {
    const auto existing = topology_->producer(ProducerId{record.id});
    if (existing.ok()) {
      had_previous = true;
      previous_incarnation = existing.value()->incarnation;
    }
    const Status registered = topology_->register_producer(ProducerId{record.id}, record.incarnation, record.endpoint);
    if (!registered.ok()) {
      return registered;
    }
    if (record.state != ParticipantState::Active) {
      const Status applied =
          topology_->set_producer_state(ProducerId{record.id}, record.incarnation, record.state);
      if (!applied.ok()) {
        return applied;
      }
    }
  } else {
    const auto existing = topology_->consumer(ConsumerId{record.id});
    if (existing.ok()) {
      had_previous = true;
      previous_incarnation = existing.value()->incarnation;
    }
    const Status registered =
        topology_->register_consumer(ConsumerId{record.id}, record.incarnation, record.endpoint, record.selection);
    if (!registered.ok()) {
      return registered;
    }
    if (record.state != ParticipantState::Active) {
      const Status applied =
          topology_->set_consumer_state(ConsumerId{record.id}, record.incarnation, record.state);
      if (!applied.ok()) {
        return applied;
      }
    }
  }

  // A participant that re-registers with a fresh incarnation has re-established
  // the liveness that the restart invalidated.
  if (had_previous && previous_incarnation != record.incarnation) {
    // Producers and consumers are tracked in separate sets, so the participant
    // identifier alone identifies the pending revalidation.
    std::unordered_set<std::uint64_t>& pending = record.kind == ParticipantKind::Producer ? unrevalidated_producers_
                                                                                          : unrevalidated_consumers_;
    if (pending.erase(record.id) > 0 && pending_revalidation_ > 0) {
      --pending_revalidation_;
    }
  }
  if (record.at > clock_.now()) {
    clock_.reset(record.at);
  }
  return Status{};
}

Status Coordinator::apply_manifest(const ManifestRecord& record) {
  if (!topology_) {
    return Status{make_error(ErrorCode::InvalidState, "manifest record arrived before the shuffle was opened")};
  }
  const PartitionManifest& manifest = record.manifest;
  const auto existing = manifests_.find(manifest.partition.value());
  if (existing != manifests_.end()) {
    if (existing->second.partition_generation > manifest.partition_generation) {
      return Status{make_error(ErrorCode::StaleGeneration, "manifest record is older than the accepted generation")};
    }
    if (existing->second.partition_generation == manifest.partition_generation && !(existing->second == manifest)) {
      return Status{make_error(ErrorCode::DivergentCommit,
                               "a different manifest already describes this partition generation")};
    }
  }

  manifests_[manifest.partition.value()] = manifest;
  PartitionFacts facts;
  facts.produced = true;
  facts.partition_generation = manifest.partition_generation;
  facts.manifest_digest = compute_manifest_digest(manifest);
  facts.total_bytes = manifest.total_bytes;
  facts.producer_incarnation = manifest.producer_incarnation;
  facts.producer = manifest.producer;
  facts_[manifest.partition.value()] = facts;

  // A re-produced partition immediately invalidates the completions of its
  // previous generation: the required edge now means "holds the current
  // content", so a stale completion must not keep the edge satisfied.
  const Status retired = ledger_.retire_generation(manifest.partition, manifest.partition_generation);
  if (!retired.ok()) {
    return retired;
  }

  return Status{};
}

Status Coordinator::apply_commit(const CommitRecord& record) {
  const auto receipt = ledger_.commit(record.request, record.at);
  if (!receipt.ok()) {
    // Replay must reproduce live behaviour exactly; a divergence here means the
    // durable history contradicts itself.
    return Status{receipt.error()};
  }
  ++commits_applied_;
  if (record.at > clock_.now()) {
    clock_.reset(record.at);
  }
  return Status{};
}

Status Coordinator::apply_failure(const FailureRecordDurable& record) {
  const Status status = ledger_.record_failure(record.key, record.code, record.permanent, record.attempts,
                                               record.recorded_at, record.ready_at);
  if (!status.ok()) {
    return status;
  }
  ++failures_applied_;
  // The retry readiness tick is part of the durable record, so the logical
  // clock resumes at or beyond it instead of restarting below it.
  if (record.ready_at > clock_.now()) {
    clock_.reset(record.ready_at);
  }
  return Status{};
}

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

PartitionFacts Coordinator::partition_facts(PartitionId id) const {
  const auto found = facts_.find(id.value());
  return found == facts_.end() ? PartitionFacts{} : found->second;
}

PressureReading Coordinator::freshen(const PressureReading& reading) const {
  PressureReading result = reading;
  const std::uint64_t now = clock_.now().value();
  const std::uint64_t observed = reading.observed_at.value();
  result.fresh = reading.observed && reading.policy_generation == request_.policy.generation && observed <= now &&
                 (now - observed) <= request_.policy.congestion.evidence_validity_ticks;
  return result;
}

PressureReading Coordinator::producer_pressure(ProducerId id) const {
  const auto found = producer_pressure_.find(id.value());
  if (found != producer_pressure_.end()) {
    return freshen(found->second);
  }
  return global_pressure_.has_value() ? freshen(*global_pressure_) : PressureReading::unknown();
}

PressureReading Coordinator::consumer_pressure(ConsumerId id) const {
  const auto found = consumer_pressure_.find(id.value());
  if (found != consumer_pressure_.end()) {
    return freshen(found->second);
  }
  return global_pressure_.has_value() ? freshen(*global_pressure_) : PressureReading::unknown();
}

// ---------------------------------------------------------------------------
// Authority
// ---------------------------------------------------------------------------

Status Coordinator::ensure_open() const {
  if (state_ == ShuffleState::Cancelled || state_ == ShuffleState::Cancelling) {
    return Status{make_error(ErrorCode::Cancelled, "shuffle is cancelled; no further authority is granted")};
  }
  if (state_ != ShuffleState::Open || !topology_) {
    return Status{make_error(ErrorCode::InvalidState, "no shuffle is open")};
  }
  return Status{};
}

Status Coordinator::open_shuffle(const ShuffleOpenRequest& request) {
  Guard guard{*this};
  if (!guard.engaged()) {
    return Status{make_error(ErrorCode::InvalidState, "re-entrant coordinator entry refused")};
  }
  if (state_ != ShuffleState::Closed) {
    return Status{make_error(ErrorCode::InvalidState, "a shuffle is already open in this coordinator")};
  }
  if (request.shuffle.is_zero() || request.generation.is_zero()) {
    return Status{make_error(ErrorCode::InvalidArgument, "shuffle identity and generation must be non-zero")};
  }
  if (request.partition_count == 0) {
    return Status{make_error(ErrorCode::InvalidArgument, "shuffle must declare at least one partition")};
  }
  if (request.partition_count > limits_.max_partitions) {
    return Status{make_error(ErrorCode::TooManyPartitions, "partition count exceeds max_partitions")};
  }
  const Status valid = validate_policy(request.policy);
  if (!valid.ok()) {
    return valid;
  }
  if (request.policy.shuffle != request.shuffle || request.policy.shuffle_generation != request.generation) {
    return Status{make_error(ErrorCode::PolicyMismatch, "policy envelope does not belong to this shuffle generation")};
  }

  const Status epoch_status = persist_epoch();
  if (!epoch_status.ok()) {
    return epoch_status;
  }

  ShuffleStateRecord record;
  record.shuffle = request.shuffle;
  record.generation = request.generation;
  record.partition_count = request.partition_count;
  record.policy = request.policy;
  record.state = ShuffleState::Open;
  record.reason = ErrorCode::Ok;
  record.at = clock_.now();
  ByteWriter writer;
  record.encode(writer);
  return commit_record(writer);
}

Status Coordinator::recover(std::span<const std::byte> snapshot, const std::vector<std::vector<std::byte>>& journal,
                            bool history_incomplete) {
  Guard guard{*this};
  if (!guard.engaged()) {
    return Status{make_error(ErrorCode::InvalidState, "re-entrant coordinator entry refused")};
  }
  if (state_ != ShuffleState::Closed) {
    return Status{make_error(ErrorCode::InvalidState, "recovery requires a fresh coordinator")};
  }
  // Recovery is all-or-nothing: the records are replayed into a scratch
  // coordinator and adopted only when every step succeeded, so a malformed
  // durable file can never leave a partially recovered authority behind.
  Coordinator scratch{limits_, nullptr};
  scratch.history_incomplete_ = history_incomplete;

  if (!snapshot.empty()) {
    ByteReader reader{snapshot, limits_, "snapshot payload"};
    const std::uint32_t version = reader.u32();
    if (!reader.ok()) {
      return reader.status();
    }
    if (version != kSnapshotPayloadVersion) {
      return Status{make_error(ErrorCode::UnsupportedVersion, "snapshot payload version is not supported")};
    }
    const std::uint32_t record_count = reader.collection_count(limits_.max_collection_items);
    for (std::uint32_t index = 0; index < record_count; ++index) {
      const auto bytes = reader.bytes(limits_.max_journal_record_bytes);
      if (!reader.ok()) {
        return reader.status();
      }
      const Status applied = scratch.apply_record(bytes);
      if (!applied.ok()) {
        return applied;
      }
    }
    const auto ledger_bytes = reader.bytes(limits_.max_state_bytes);
    if (!reader.ok()) {
      return reader.status();
    }
    ByteReader ledger_reader{ledger_bytes, limits_, "ledger state"};
    const Status decoded = scratch.ledger_.decode(ledger_reader, limits_);
    if (!decoded.ok()) {
      return decoded;
    }
    const Status ledger_end = ledger_reader.require_end();
    if (!ledger_end.ok()) {
      return ledger_end;
    }
    const Status payload_end = reader.require_end();
    if (!payload_end.ok()) {
      return payload_end;
    }
  }

  for (const std::vector<std::byte>& record : journal) {
    const Status applied = scratch.apply_record(record);
    if (!applied.ok()) {
      return applied;
    }
  }

  // Conservative restart: a participant that was active before the restart is
  // not assumed to be alive now. It holds no authority until it re-registers
  // with a fresh incarnation, and the fabric reports the pending
  // revalidations until that happens.
  std::uint32_t suspect = 0;
  if (scratch.topology_) {
    const std::vector<ProducerRecord> producers = scratch.topology_->producers();
    for (const ProducerRecord& record : producers) {
      if (!holds_authority(record.state)) {
        continue;
      }
      const Status demoted = scratch.topology_->set_producer_state(record.id, record.incarnation, ParticipantState::Suspect);
      if (!demoted.ok()) {
        return demoted;
      }
      scratch.unrevalidated_producers_.insert(record.id.value());
      ++suspect;
    }
    const std::vector<ConsumerRecord> consumers = scratch.topology_->consumers();
    for (const ConsumerRecord& record : consumers) {
      if (!holds_authority(record.state)) {
        continue;
      }
      const Status demoted = scratch.topology_->set_consumer_state(record.id, record.incarnation, ParticipantState::Suspect);
      if (!demoted.ok()) {
        return demoted;
      }
      scratch.unrevalidated_consumers_.insert(record.id.value());
      ++suspect;
    }
  }
  scratch.pending_revalidation_ = suspect;

  // A restart is a new epoch, and the epoch record must be durable before the
  // recovered authority is exposed.
  scratch.sink_ = sink_;
  scratch.durable_ = durable_;
  const Status epoch_status = scratch.persist_epoch();
  if (!epoch_status.ok()) {
    return epoch_status;
  }
  adopt(std::move(scratch));
  return Status{};
}

Result<RegistrationOutcome> Coordinator::register_producer(ProducerId id, IncarnationId incarnation,
                                                           std::string endpoint) {
  Guard guard{*this};
  if (!guard.engaged()) {
    return make_failure<RegistrationOutcome>(ErrorCode::InvalidState, "re-entrant coordinator entry refused");
  }
  const Status open = ensure_open();
  if (!open.ok()) {
    return Result<RegistrationOutcome>{open.error()};
  }
  const Status checked = topology_->check_producer_registration(id, incarnation, endpoint);
  if (!checked.ok()) {
    return Result<RegistrationOutcome>{checked.error()};
  }

  const auto existing = topology_->producer(id);
  RegistrationOutcome outcome;
  outcome.incarnation = incarnation;
  outcome.superseded_previous = existing.ok() && existing.value()->incarnation != incarnation;

  ParticipantRecord record;
  record.kind = ParticipantKind::Producer;
  record.id = id.value();
  record.incarnation = incarnation;
  record.state = ParticipantState::Active;
  record.endpoint = std::move(endpoint);
  record.at = clock_.now();
  ByteWriter writer;
  record.encode(writer);
  const Status committed = commit_record(writer);
  if (!committed.ok()) {
    return Result<RegistrationOutcome>{committed.error()};
  }
  outcome.topology_generation = topology_->generation();
  return outcome;
}

Result<RegistrationOutcome> Coordinator::register_consumer(ConsumerId id, IncarnationId incarnation,
                                                           std::string endpoint,
                                                           const PartitionSelection& selection) {
  Guard guard{*this};
  if (!guard.engaged()) {
    return make_failure<RegistrationOutcome>(ErrorCode::InvalidState, "re-entrant coordinator entry refused");
  }
  const Status open = ensure_open();
  if (!open.ok()) {
    return Result<RegistrationOutcome>{open.error()};
  }
  const Status checked = topology_->check_consumer_registration(id, incarnation, endpoint, selection);
  if (!checked.ok()) {
    return Result<RegistrationOutcome>{checked.error()};
  }

  const auto existing = topology_->consumer(id);
  RegistrationOutcome outcome;
  outcome.incarnation = incarnation;
  outcome.superseded_previous = existing.ok() && existing.value()->incarnation != incarnation;

  ParticipantRecord record;
  record.kind = ParticipantKind::Consumer;
  record.id = id.value();
  record.incarnation = incarnation;
  record.state = ParticipantState::Active;
  record.endpoint = std::move(endpoint);
  record.selection = selection;
  record.at = clock_.now();
  ByteWriter writer;
  record.encode(writer);
  const Status committed = commit_record(writer);
  if (!committed.ok()) {
    return Result<RegistrationOutcome>{committed.error()};
  }
  outcome.topology_generation = topology_->generation();
  return outcome;
}

Status Coordinator::set_participant_state(ParticipantKind kind, std::uint64_t id, IncarnationId incarnation,
                                          ParticipantState state) {
  Guard guard{*this};
  if (!guard.engaged()) {
    return Status{make_error(ErrorCode::InvalidState, "re-entrant coordinator entry refused")};
  }
  const Status open = ensure_open();
  if (!open.ok()) {
    return open;
  }
  const Status checked = topology_->check_participant_state(kind, id, incarnation, state);
  if (!checked.ok()) {
    return checked;
  }

  std::string endpoint;
  PartitionSelection selection;
  if (kind == ParticipantKind::Producer) {
    endpoint = topology_->producer(ProducerId{id}).value()->endpoint;
  } else {
    const ConsumerRecord* record = topology_->consumer(ConsumerId{id}).value();
    endpoint = record->endpoint;
    selection = topology_->pattern(record->pattern);
  }

  ParticipantRecord record;
  record.kind = kind;
  record.id = id;
  record.incarnation = incarnation;
  record.state = state;
  record.endpoint = std::move(endpoint);
  record.selection = std::move(selection);
  record.at = clock_.now();
  ByteWriter writer;
  record.encode(writer);
  return commit_record(writer);
}

Result<ManifestAcceptance> Coordinator::publish_manifest(const PartitionManifest& manifest) {
  Guard guard{*this};
  if (!guard.engaged()) {
    return make_failure<ManifestAcceptance>(ErrorCode::InvalidState, "re-entrant coordinator entry refused");
  }
  const Status open = ensure_open();
  if (!open.ok()) {
    return Result<ManifestAcceptance>{open.error()};
  }
  const Status valid = validate_manifest(manifest, limits_);
  if (!valid.ok()) {
    return Result<ManifestAcceptance>{valid.error()};
  }
  if (manifest.shuffle != request_.shuffle || manifest.shuffle_generation != request_.generation) {
    return make_failure<ManifestAcceptance>(ErrorCode::StaleGeneration, "manifest belongs to another shuffle generation");
  }
  if (manifest.partition.value() >= request_.partition_count) {
    return make_failure<ManifestAcceptance>(ErrorCode::UnknownPartition, "manifest references a partition outside the shuffle");
  }
  const auto owner = topology_->owner_of(manifest.partition);
  if (!owner.ok() || owner.value() != manifest.producer) {
    return make_failure<ManifestAcceptance>(ErrorCode::PartitionNotOwned,
                                            "manifest was published by a producer that does not own the partition");
  }
  const auto producer_record = topology_->producer(manifest.producer);
  if (!producer_record.ok()) {
    return make_failure<ManifestAcceptance>(ErrorCode::UnknownParticipant, "manifest producer is not registered");
  }
  if (producer_record.value()->incarnation != manifest.producer_incarnation) {
    return make_failure<ManifestAcceptance>(ErrorCode::StaleIncarnation,
                                            "manifest carries a superseded producer incarnation");
  }
  if (!holds_authority(producer_record.value()->state)) {
    return make_failure<ManifestAcceptance>(ErrorCode::ParticipantNotActive, "manifest producer holds no authority");
  }

  PartitionManifest normalized = manifest;
  normalized.topology_generation = topology_->generation();
  const auto existing = manifests_.find(normalized.partition.value());
  if (existing != manifests_.end()) {
    if (existing->second.partition_generation > normalized.partition_generation) {
      return make_failure<ManifestAcceptance>(ErrorCode::StaleGeneration,
                                              "a newer partition generation is already accepted");
    }
    if (existing->second.partition_generation == normalized.partition_generation && !(existing->second == normalized)) {
      return make_failure<ManifestAcceptance>(ErrorCode::DivergentCommit,
                                              "a different manifest already describes this partition generation");
    }
  }

  ManifestRecord record;
  record.manifest = normalized;
  ByteWriter writer;
  const Status encoded = record.encode(writer, limits_);
  if (!encoded.ok()) {
    return Result<ManifestAcceptance>{encoded.error()};
  }
  const Status committed = commit_record(writer);
  if (!committed.ok()) {
    return Result<ManifestAcceptance>{committed.error()};
  }

  ManifestAcceptance acceptance;
  acceptance.manifest_digest = compute_manifest_digest(normalized);
  acceptance.partition_generation = normalized.partition_generation;
  return acceptance;
}

Result<WavePlan> Coordinator::next_wave() {
  Guard guard{*this};
  if (!guard.engaged()) {
    return make_failure<WavePlan>(ErrorCode::InvalidState, "re-entrant coordinator entry refused");
  }
  if (state_ == ShuffleState::Completed) {
    // Nothing is left to plan. An empty plan grants no authority, which is the
    // honest answer: the caller learns completion from the plan and the ledger.
    // No wave identity is consumed: an empty plan grants nothing, so it is not
    // a wave the scheduler ever issued.
    WavePlan plan;
    plan.all_resolved = true;
    return plan;
  }
  const Status open = ensure_open();
  if (!open.ok()) {
    return Result<WavePlan>{open.error()};
  }
  const Status configured = ensure_scheduler();
  if (!configured.ok()) {
    return Result<WavePlan>{configured.error()};
  }

  SchedulingEnvironment environment;
  environment.topology = &topology_.value();
  environment.completion = &ledger_;
  environment.congestion = this;
  environment.partitions = this;
  environment.now = clock_.now();

  auto plan = scheduler_.next_wave(environment);
  if (!plan.ok()) {
    return plan;
  }
  for (const DispatchGrant& grant : plan.value().grants) {
    grants_.emplace(grant.attempt.value(), grant);
    ledger_.note_attempt_bytes(grant.total_bytes);
  }
  return plan;
}

Result<CommitOutcome> Coordinator::commit_transfer(const CommitRequest& request) {
  Guard guard{*this};
  if (!guard.engaged()) {
    return make_failure<CommitOutcome>(ErrorCode::InvalidState, "re-entrant coordinator entry refused");
  }
  if (state_ == ShuffleState::Cancelled || state_ == ShuffleState::Cancelling) {
    return make_failure<CommitOutcome>(ErrorCode::Cancelled,
                                       "shuffle is cancelled: late authority is refused");
  }
  // Cancellation is answered first, because a cancelled shuffle refuses every
  // attempt whatever its identity. After that the attempt registry is consulted
  // before the remaining lifecycle checks, so an attempt that no longer carries
  // authority is always reported as a stale attempt.
  const auto grant_found = grants_.find(request.attempt.value());
  if (grant_found == grants_.end()) {
    return make_failure<CommitOutcome>(ErrorCode::StaleAttempt,
                                       "attempt was not issued in this coordinator epoch");
  }
  const Status open = ensure_open();
  if (!open.ok()) {
    return Result<CommitOutcome>{open.error()};
  }
  const DispatchGrant& grant = grant_found->second;

  if (request.shuffle_generation != request_.generation) {
    return make_failure<CommitOutcome>(ErrorCode::StaleGeneration, "completion belongs to another shuffle generation");
  }
  if (request.edge.partition != grant.partition || request.edge.partition_generation != grant.partition_generation ||
      request.consumer != grant.consumer || request.edge.consumer != grant.consumer) {
    return make_failure<CommitOutcome>(ErrorCode::AttemptSuperseded,
                                       "completion does not match the attempt it claims");
  }
  if (request.producer != grant.producer || request.producer_incarnation != grant.producer_incarnation ||
      request.consumer_incarnation != grant.consumer_incarnation) {
    return make_failure<CommitOutcome>(ErrorCode::StaleIncarnation, "completion carries a superseded incarnation");
  }
  if (request.manifest_digest != grant.manifest_digest) {
    return make_failure<CommitOutcome>(ErrorCode::ManifestInconsistent,
                                       "completion disagrees with the grant about the manifest");
  }

  const auto consumer_record = topology_->consumer(request.consumer);
  if (!consumer_record.ok()) {
    return make_failure<CommitOutcome>(ErrorCode::UnknownParticipant, "completing consumer is not registered");
  }
  if (consumer_record.value()->incarnation != request.consumer_incarnation) {
    return make_failure<CommitOutcome>(ErrorCode::StaleIncarnation, "completing consumer incarnation is superseded");
  }
  if (!holds_authority(consumer_record.value()->state)) {
    return make_failure<CommitOutcome>(ErrorCode::ParticipantNotActive, "completing consumer holds no authority");
  }
  const auto owner = topology_->owner_of(request.edge.partition);
  if (!owner.ok() || owner.value() != request.producer) {
    return make_failure<CommitOutcome>(ErrorCode::PartitionNotOwned, "partition is no longer owned by that producer");
  }
  const auto producer_record = topology_->producer(request.producer);
  if (!producer_record.ok() || producer_record.value()->incarnation != request.producer_incarnation) {
    return make_failure<CommitOutcome>(ErrorCode::StaleIncarnation, "producing incarnation is superseded");
  }
  if (!holds_authority(producer_record.value()->state)) {
    return make_failure<CommitOutcome>(ErrorCode::ParticipantNotActive, "producing participant holds no authority");
  }

  const auto manifest_found = manifests_.find(request.edge.partition.value());
  if (manifest_found == manifests_.end() ||
      manifest_found->second.partition_generation != request.edge.partition_generation) {
    return make_failure<CommitOutcome>(ErrorCode::StaleGeneration, "no accepted manifest for this partition generation");
  }
  const PartitionManifest& manifest = manifest_found->second;
  const Digest expected = compute_manifest_digest(manifest);
  if (expected != request.manifest_digest) {
    return make_failure<CommitOutcome>(ErrorCode::DigestMismatch, "accepted manifest digest does not match the attempt");
  }
  if (!request.integrity_verified) {
    return make_failure<CommitOutcome>(ErrorCode::IntegrityFailure, "completion requires verified integrity evidence");
  }
  // The consumer may present either digest: the manifest digest binds content to
  // identity and is what the attempt was granted for, while the partition digest
  // is what a consumer naturally computes over the assembled bytes. Both are
  // derived from the same chunk set, so accepting either cannot admit different
  // content.
  const Digest expected_content = compute_partition_digest(manifest);
  if (request.observed_partition_digest != expected && request.observed_partition_digest != expected_content) {
    return make_failure<CommitOutcome>(ErrorCode::DigestMismatch, "consumer observed different partition content");
  }
  if (request.bytes != manifest.total_bytes) {
    return make_failure<CommitOutcome>(ErrorCode::AccountingMismatch, "completion byte count contradicts the manifest");
  }
  if (request.observed_chunk_digests.size() != manifest.chunks.size()) {
    return make_failure<CommitOutcome>(ErrorCode::IntegrityFailure,
                                       "chunk evidence does not cover the manifest chunk set");
  }
  for (std::size_t index = 0; index < manifest.chunks.size(); ++index) {
    if (request.observed_chunk_digests[index] != manifest.chunks[index].digest) {
      return make_failure<CommitOutcome>(ErrorCode::DigestMismatch,
                                         "chunk " + manifest.chunks[index].id.to_string() +
                                             " evidence does not match the manifest");
    }
  }

  // Everything the ledger will check is checked first: a record that became
  // durable and was then refused would be a contradiction replayed on every
  // restart, which is exactly what the ordering rule exists to prevent.
  const Status ledger_valid = ledger_.validate_commit(request);
  if (!ledger_valid.ok()) {
    return Result<CommitOutcome>{ledger_valid.error()};
  }

  // Durable before acknowledged: the record is written and flushed first, and
  // only then is the completion applied and answered.
  CommitRecord record;
  record.request = request;
  record.at = clock_.now();
  ByteWriter writer;
  record.encode(writer);
  const Status persisted = persist(writer.data());
  if (!persisted.ok()) {
    return Result<CommitOutcome>{persisted.error()};
  }

  const auto receipt = ledger_.commit(request, clock_.now());
  if (!receipt.ok()) {
    return Result<CommitOutcome>{receipt.error()};
  }
  ++commits_applied_;

  grants_.erase(grant_found);
  static_cast<void>(scheduler_.resolve(request.attempt, EdgeOutcome::Completed, ErrorCode::Ok));

  CommitOutcome outcome;
  outcome.receipt = receipt.value();
  note_completion();
  outcome.shuffle_completed = state_ == ShuffleState::Completed;
  return outcome;
}

Result<FailureOutcome> Coordinator::report_failure(TransferAttemptId attempt, ErrorCode code) {
  Guard guard{*this};
  if (!guard.engaged()) {
    return make_failure<FailureOutcome>(ErrorCode::InvalidState, "re-entrant coordinator entry refused");
  }
  if (classify(code) == RetryClass::Deferred) {
    return make_failure<FailureOutcome>(ErrorCode::InvalidArgument,
                                        "a deferred condition is scheduling pressure, not a transfer failure");
  }
  if (state_ == ShuffleState::Cancelled || state_ == ShuffleState::Cancelling) {
    return make_failure<FailureOutcome>(ErrorCode::Cancelled, "shuffle is cancelled; the attempt has no authority");
  }
  const auto grant_found = grants_.find(attempt.value());
  if (grant_found == grants_.end()) {
    return make_failure<FailureOutcome>(ErrorCode::StaleAttempt,
                                        "attempt was not issued in this coordinator epoch");
  }
  const Status open = ensure_open();
  if (!open.ok()) {
    return Result<FailureOutcome>{open.error()};
  }
  const DispatchGrant& grant = grant_found->second;
  const EdgeKey key{grant.partition, grant.partition_generation, grant.consumer};
  // Copied before the grant registry entry is erased below: the reference into
  // the map is invalidated by that erase, and the answer must not read through
  // it afterwards.
  const std::uint32_t attempt_ordinal = grant.attempt_ordinal;

  const bool authority_refused = classify(code) == RetryClass::Authority;
  const bool exhausted = attempt_ordinal >= request_.policy.retry.max_attempts;
  const bool permanent = authority_refused || classify(code) == RetryClass::Permanent || exhausted;
  const TickId ready_at = permanent
                              ? clock_.now()
                              : TickId{clock_.now().value() + request_.policy.retry.min_ticks_between_attempts};

  // Bounds and codes are checked before the record becomes durable so that a
  // durable record can never fail to apply.
  const Status failure_valid = ledger_.validate_failure(key, code);
  if (!failure_valid.ok()) {
    return Result<FailureOutcome>{failure_valid.error()};
  }

  FailureRecordDurable record;
  record.key = key;
  record.code = code;
  record.permanent = permanent;
  record.attempts = attempt_ordinal;
  record.recorded_at = clock_.now();
  record.ready_at = ready_at;
  ByteWriter writer;
  record.encode(writer);
  const Status persisted = persist(writer.data());
  if (!persisted.ok()) {
    return Result<FailureOutcome>{persisted.error()};
  }
  const Status applied = apply_failure(record);
  if (!applied.ok()) {
    return Result<FailureOutcome>{applied.error()};
  }

  const EdgeOutcome outcome = permanent ? (authority_refused ? EdgeOutcome::AuthorityRefused : EdgeOutcome::PermanentFailure)
                                        : EdgeOutcome::RetriableFailure;
  grants_.erase(grant_found);
  static_cast<void>(scheduler_.resolve(attempt, outcome, code));

  note_completion();

  FailureOutcome result;
  result.permanent = permanent;
  result.attempts = attempt_ordinal;
  result.ready_at = ready_at;
  return result;
}

Status Coordinator::update_congestion(const CongestionIntent& intent) {
  Guard guard{*this};
  if (!guard.engaged()) {
    return Status{make_error(ErrorCode::InvalidState, "re-entrant coordinator entry refused")};
  }
  const Status open = ensure_open();
  if (!open.ok()) {
    return open;
  }
  if (intent.level > 100) {
    return Status{make_error(ErrorCode::InvalidArgument, "congestion level is outside [0, 100]")};
  }
  if (intent.policy_generation != request_.policy.generation) {
    return Status{make_error(ErrorCode::StalePolicy,
                             "congestion evidence was produced under another policy generation")};
  }
  if (intent.observed_at > clock_.now()) {
    return Status{make_error(ErrorCode::InvalidArgument, "congestion evidence is observed in the future")};
  }

  if (intent.reporter_kind == ParticipantKind::Producer) {
    const auto record = topology_->producer(ProducerId{intent.reporter_id});
    if (!record.ok()) {
      return Status{make_error(ErrorCode::UnknownParticipant, "congestion reporter is not registered")};
    }
    if (record.value()->incarnation != intent.reporter_incarnation) {
      return Status{make_error(ErrorCode::StaleIncarnation, "congestion reporter incarnation is superseded")};
    }
    if (!holds_authority(record.value()->state)) {
      return Status{make_error(ErrorCode::ParticipantNotActive, "congestion reporter holds no authority")};
    }
  } else {
    const auto record = topology_->consumer(ConsumerId{intent.reporter_id});
    if (!record.ok()) {
      return Status{make_error(ErrorCode::UnknownParticipant, "congestion reporter is not registered")};
    }
    if (record.value()->incarnation != intent.reporter_incarnation) {
      return Status{make_error(ErrorCode::StaleIncarnation, "congestion reporter incarnation is superseded")};
    }
    if (!holds_authority(record.value()->state)) {
      return Status{make_error(ErrorCode::ParticipantNotActive, "congestion reporter holds no authority")};
    }
  }

  PressureReading reading;
  reading.level = intent.level;
  reading.observed = true;
  reading.fresh = true;
  reading.observed_at = intent.observed_at;
  reading.reporter = intent.reporter_incarnation;
  reading.policy_generation = intent.policy_generation;

  if (intent.is_global()) {
    global_pressure_ = reading;
    return Status{};
  }
  if (!intent.producer.is_zero()) {
    producer_pressure_[intent.producer.value()] = reading;
  }
  if (!intent.consumer.is_zero()) {
    consumer_pressure_[intent.consumer.value()] = reading;
  }
  return Status{};
}

Status Coordinator::cancel_shuffle(ErrorCode reason) {
  Guard guard{*this};
  if (!guard.engaged()) {
    return Status{make_error(ErrorCode::InvalidState, "re-entrant coordinator entry refused")};
  }
  if (state_ == ShuffleState::Cancelled) {
    return Status{};  // idempotent
  }
  if (state_ != ShuffleState::Open) {
    return Status{make_error(ErrorCode::InvalidState, "no shuffle is open to cancel")};
  }
  if (reason == ErrorCode::Ok) {
    return Status{make_error(ErrorCode::InvalidArgument, "cancellation requires a reason code")};
  }
  ShuffleStateRecord record;
  record.shuffle = request_.shuffle;
  record.generation = request_.generation;
  record.partition_count = request_.partition_count;
  record.policy = request_.policy;
  record.state = ShuffleState::Cancelled;
  record.reason = reason;
  record.at = clock_.now();
  ByteWriter writer;
  record.encode(writer);
  return commit_record(writer);
}

Result<TickId> Coordinator::advance_tick() {
  Guard guard{*this};
  if (!guard.engaged()) {
    return make_failure<TickId>(ErrorCode::InvalidState, "re-entrant coordinator entry refused");
  }
  return clock_.advance();
}

// ---------------------------------------------------------------------------
// Inspection
// ---------------------------------------------------------------------------

Status Coordinator::ensure_scheduler() {
  if (!topology_) {
    return Status{make_error(ErrorCode::InvalidState, "no topology is available")};
  }
  if (!scheduler_.configured() || scheduler_.topology_generation() != topology_->generation() ||
      scheduler_.policy_generation() != request_.policy.generation) {
    return scheduler_.configure(request_.policy, *topology_);
  }
  return Status{};
}

ProgressSnapshot Coordinator::compute_progress() const {
  if (!topology_) {
    return ProgressSnapshot{};
  }
  const auto required = topology_->planned_edge_count();
  return ledger_.progress(required.ok() ? required.value() : 0, clock_.now());
}

void Coordinator::note_completion() {
  if (state_ != ShuffleState::Open || !topology_) {
    return;
  }
  const auto required = topology_->planned_edge_count();
  if (!required.ok()) {
    return;
  }
  const ProgressSnapshot snapshot = ledger_.progress(required.value(), clock_.now());
  if (snapshot.edges_completed + snapshot.edges_failed < required.value()) {
    return;
  }
  ShuffleStateRecord record;
  record.shuffle = request_.shuffle;
  record.generation = request_.generation;
  record.partition_count = request_.partition_count;
  record.policy = request_.policy;
  record.state = ShuffleState::Completed;
  record.reason = ErrorCode::Ok;
  record.at = clock_.now();
  ByteWriter writer;
  record.encode(writer);
  static_cast<void>(commit_record(writer));
}

Result<PartitionManifest> Coordinator::manifest_of(PartitionId partition) const {
  Guard guard{*this};
  if (!guard.engaged()) {
    return make_failure<PartitionManifest>(ErrorCode::InvalidState, "re-entrant coordinator entry refused");
  }
  const auto found = manifests_.find(partition.value());
  if (found == manifests_.end()) {
    return make_failure<PartitionManifest>(ErrorCode::PartitionNotProduced,
                                           "no manifest has been accepted for this partition");
  }
  return found->second;
}

Result<ProgressSnapshot> Coordinator::progress() const {
  Guard guard{*this};
  if (!guard.engaged()) {
    return make_failure<ProgressSnapshot>(ErrorCode::InvalidState, "re-entrant coordinator entry refused");
  }
  if (!topology_) {
    return make_failure<ProgressSnapshot>(ErrorCode::InvalidState, "no shuffle is open");
  }
  return compute_progress();
}

Explanation Coordinator::build_explanation(std::uint32_t max_samples_per_code) const {
  Explanation explanation = ledger_.explain(max_samples_per_code);

  if (topology_) {
    std::uint64_t missing = 0;
    std::string first_missing;
    for (std::uint32_t index = 0; index < topology_->partition_count(); ++index) {
      if (manifests_.find(index) == manifests_.end()) {
        if (missing == 0) {
          first_missing = std::to_string(index);
        }
        ++missing;
      }
    }
    if (missing > 0) {
      ExplainEntry entry;
      entry.code = ErrorCode::PartitionNotProduced;
      entry.subject = "partition " + first_missing;
      entry.detail = "partitions without an accepted manifest are explicitly incomplete";
      entry.count = missing;
      explanation.entries.push_back(std::move(entry));
    }

    std::uint64_t inactive = 0;
    for (const ProducerRecord& record : topology_->producers()) {
      if (!holds_authority(record.state)) {
        ++inactive;
      }
    }
    for (const ConsumerRecord& record : topology_->consumers()) {
      if (!holds_authority(record.state)) {
        ++inactive;
      }
    }
    if (inactive > 0) {
      ExplainEntry entry;
      entry.code = ErrorCode::ParticipantNotActive;
      entry.subject = "topology";
      entry.detail = "registered participants that currently hold no authority";
      entry.count = inactive;
      explanation.entries.push_back(std::move(entry));
    }
  }

  if (pending_revalidation_ > 0) {
    ExplainEntry entry;
    entry.code = ErrorCode::RevalidationRequired;
    entry.subject = "epoch " + std::to_string(epoch_);
    entry.detail = "participants must re-register with a fresh incarnation after the restart";
    entry.count = pending_revalidation_;
    explanation.entries.push_back(std::move(entry));
  }
  if (history_incomplete_) {
    ExplainEntry entry;
    entry.code = ErrorCode::RevalidationRequired;
    entry.subject = "journal";
    entry.detail = "recovery discarded a torn journal tail; the discarded record was never acknowledged";
    entry.count = 1;
    explanation.entries.push_back(std::move(entry));
  }
  if (state_ != ShuffleState::Open) {
    ExplainEntry entry;
    entry.code = state_ == ShuffleState::Cancelled ? reason_ : ErrorCode::InvalidState;
    entry.subject = "shuffle";
    entry.detail = std::string{"shuffle state is "} + to_string(state_);
    entry.count = 1;
    explanation.entries.push_back(std::move(entry));
  }
  if (!durable_) {
    ExplainEntry entry;
    entry.code = ErrorCode::CapabilityUnsupported;
    entry.subject = "durability";
    entry.detail = "this coordinator runs without a durable sink; commits are not durable";
    entry.count = 1;
    explanation.entries.push_back(std::move(entry));
  }

  const ProgressSnapshot snapshot = compute_progress();
  if (snapshot.edges_over_counted > 0) {
    ExplainEntry entry;
    entry.code = ErrorCode::AccountingMismatch;
    entry.subject = "progress";
    entry.detail = "more edges are resolved than the current requirement implies";
    entry.count = snapshot.edges_over_counted;
    explanation.entries.push_back(std::move(entry));
  } else if (!snapshot.accounting_closes()) {
    ExplainEntry entry;
    entry.code = ErrorCode::AccountingMismatch;
    entry.subject = "progress";
    entry.detail = "required edge accounting does not close against completed and failed edges";
    entry.count = 1;
    explanation.entries.push_back(std::move(entry));
  }

  std::sort(explanation.entries.begin(), explanation.entries.end(),
            [](const ExplainEntry& lhs, const ExplainEntry& rhs) {
              if (lhs.code != rhs.code) {
                return static_cast<std::uint16_t>(lhs.code) < static_cast<std::uint16_t>(rhs.code);
              }
              return lhs.subject < rhs.subject;
            });
  return explanation;
}

Result<Explanation> Coordinator::explain(std::uint32_t max_samples_per_code) const {
  Guard guard{*this};
  if (!guard.engaged()) {
    return make_failure<Explanation>(ErrorCode::InvalidState, "re-entrant coordinator entry refused");
  }
  return build_explanation(max_samples_per_code);
}

Result<CoordinatorStatus> Coordinator::status() const {
  Guard guard{*this};
  if (!guard.engaged()) {
    return make_failure<CoordinatorStatus>(ErrorCode::InvalidState, "re-entrant coordinator entry refused");
  }
  CoordinatorStatus snapshot;
  snapshot.state = state_;
  snapshot.reason = reason_;
  snapshot.epoch = epoch_;
  snapshot.tick = clock_.now();
  snapshot.shuffle = request_.shuffle;
  snapshot.shuffle_generation = request_.generation;
  snapshot.policy_generation = request_.policy.generation;
  if (topology_) {
    snapshot.topology_generation = topology_->generation();
    snapshot.active_producers = topology_->active_producer_count();
    snapshot.active_consumers = topology_->active_consumer_count();
  }
  snapshot.in_flight = scheduler_.in_flight();
  snapshot.persisted_records = persisted_records_;
  snapshot.durable = durable_;
  snapshot.revalidation_required = revalidation_required();
  snapshot.history_incomplete = history_incomplete_;
  snapshot.progress = compute_progress();
  return snapshot;
}

Result<std::vector<std::byte>> Coordinator::snapshot_payload() const {
  Guard guard{*this};
  if (!guard.engaged()) {
    return make_failure<std::vector<std::byte>>(ErrorCode::InvalidState, "re-entrant coordinator entry refused");
  }
  std::vector<std::vector<std::byte>> records;
  {
    EpochRecord record;
    record.epoch = epoch_;
    record.opened_at = clock_.now();
    ByteWriter writer;
    record.encode(writer);
    records.push_back(writer.take());
  }
  if (state_ != ShuffleState::Closed) {
    ShuffleStateRecord record;
    record.shuffle = request_.shuffle;
    record.generation = request_.generation;
    record.partition_count = request_.partition_count;
    record.policy = request_.policy;
    record.state = state_;
    record.reason = reason_;
    record.at = clock_.now();
    ByteWriter writer;
    record.encode(writer);
    records.push_back(writer.take());
  }
  if (topology_) {
    for (const ProducerRecord& entry : topology_->producers()) {
      ParticipantRecord record;
      record.kind = ParticipantKind::Producer;
      record.id = entry.id.value();
      record.incarnation = entry.incarnation;
      record.state = entry.state;
      record.endpoint = entry.endpoint;
      record.at = clock_.now();
      ByteWriter writer;
      record.encode(writer);
      records.push_back(writer.take());
    }
    for (const ConsumerRecord& entry : topology_->consumers()) {
      ParticipantRecord record;
      record.kind = ParticipantKind::Consumer;
      record.id = entry.id.value();
      record.incarnation = entry.incarnation;
      record.state = entry.state;
      record.endpoint = entry.endpoint;
      record.selection = topology_->pattern(entry.pattern);
      record.at = clock_.now();
      ByteWriter writer;
      record.encode(writer);
      records.push_back(writer.take());
    }
  }
  {
    std::vector<std::uint64_t> keys;
    keys.reserve(manifests_.size());
    for (const auto& entry : manifests_) {
      keys.push_back(entry.first);
    }
    std::sort(keys.begin(), keys.end());
    for (const std::uint64_t key : keys) {
      ManifestRecord record;
      record.manifest = manifests_.at(key);
      ByteWriter writer;
      const Status encoded = record.encode(writer, limits_);
      if (!encoded.ok()) {
        return Result<std::vector<std::byte>>{encoded.error()};
      }
      records.push_back(writer.take());
    }
  }

  ByteWriter writer;
  writer.put_u32(kSnapshotPayloadVersion);
  writer.put_u32(static_cast<std::uint32_t>(records.size()));
  for (const std::vector<std::byte>& record : records) {
    writer.put_byte_string(record);
  }
  ByteWriter ledger_writer;
  const Status ledger_encoded = ledger_.encode(ledger_writer);
  if (!ledger_encoded.ok()) {
    return Result<std::vector<std::byte>>{ledger_encoded.error()};
  }
  writer.put_byte_string(ledger_writer.data());
  return writer.take();
}

}  // namespace shuffle::fabric
