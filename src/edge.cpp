// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/edge.hpp"

#include "shuffle/fabric/topology.hpp"

namespace shuffle::fabric {

std::string EdgeKey::to_string() const {
  return "p" + partition.to_string() + "/g" + partition_generation.to_string() + "/c" + consumer.to_string();
}

const char* to_string(EdgePhase phase) noexcept {
  switch (phase) {
    case EdgePhase::Unknown: return "Unknown";
    case EdgePhase::Pending: return "Pending";
    case EdgePhase::Dispatched: return "Dispatched";
    case EdgePhase::Completed: return "Completed";
    case EdgePhase::Failed: return "Failed";
    case EdgePhase::Purged: return "Purged";
  }
  return "Unknown";
}

const char* to_string(EdgeOutcome outcome) noexcept {
  switch (outcome) {
    case EdgeOutcome::Completed: return "Completed";
    case EdgeOutcome::RetriableFailure: return "RetriableFailure";
    case EdgeOutcome::PermanentFailure: return "PermanentFailure";
    case EdgeOutcome::AuthorityRefused: return "AuthorityRefused";
    case EdgeOutcome::Cancelled: return "Cancelled";
  }
  return "Unknown";
}

const char* to_string(ParticipantKind kind) noexcept {
  switch (kind) {
    case ParticipantKind::Producer: return "Producer";
    case ParticipantKind::Consumer: return "Consumer";
  }
  return "Unknown";
}

const char* to_string(ParticipantState state) noexcept {
  switch (state) {
    case ParticipantState::Pending: return "Pending";
    case ParticipantState::Active: return "Active";
    case ParticipantState::Suspect: return "Suspect";
    case ParticipantState::Failed: return "Failed";
    case ParticipantState::Withdrawn: return "Withdrawn";
  }
  return "Unknown";
}

bool holds_authority(ParticipantState state) noexcept {
  return state == ParticipantState::Active;
}

const char* to_string(SelectionKind kind) noexcept {
  switch (kind) {
    case SelectionKind::All: return "All";
    case SelectionKind::Range: return "Range";
    case SelectionKind::List: return "List";
  }
  return "Unknown";
}

}  // namespace shuffle::fabric
