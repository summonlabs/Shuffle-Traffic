// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "shuffle/fabric/error.hpp"

namespace shuffle::fabric {

const char* to_string(ErrorCode code) noexcept {
  switch (code) {
    case ErrorCode::Ok: return "Ok";
    case ErrorCode::InvalidArgument: return "InvalidArgument";
    case ErrorCode::MalformedInput: return "MalformedInput";
    case ErrorCode::TruncatedInput: return "TruncatedInput";
    case ErrorCode::OversizedInput: return "OversizedInput";
    case ErrorCode::TrailingGarbage: return "TrailingGarbage";
    case ErrorCode::UnsupportedVersion: return "UnsupportedVersion";
    case ErrorCode::ChecksumMismatch: return "ChecksumMismatch";
    case ErrorCode::IntegerOverflow: return "IntegerOverflow";
    case ErrorCode::InvalidUnicode: return "InvalidUnicode";
    case ErrorCode::EmptyValue: return "EmptyValue";
    case ErrorCode::LimitExceeded: return "LimitExceeded";
    case ErrorCode::InvalidState: return "InvalidState";
    case ErrorCode::UnsupportedOperation: return "UnsupportedOperation";
    case ErrorCode::CapabilityUnsupported: return "CapabilityUnsupported";
    case ErrorCode::InternalError: return "InternalError";
    case ErrorCode::UnknownShuffle: return "UnknownShuffle";
    case ErrorCode::UnknownPartition: return "UnknownPartition";
    case ErrorCode::UnknownChunk: return "UnknownChunk";
    case ErrorCode::UnknownParticipant: return "UnknownParticipant";
    case ErrorCode::StaleGeneration: return "StaleGeneration";
    case ErrorCode::StaleIncarnation: return "StaleIncarnation";
    case ErrorCode::StaleTopology: return "StaleTopology";
    case ErrorCode::StalePolicy: return "StalePolicy";
    case ErrorCode::StaleAttempt: return "StaleAttempt";
    case ErrorCode::IncarnationMismatch: return "IncarnationMismatch";
    case ErrorCode::GenerationMismatch: return "GenerationMismatch";
    case ErrorCode::DigestMismatch: return "DigestMismatch";
    case ErrorCode::AuthorityDenied: return "AuthorityDenied";
    case ErrorCode::EligibilityDenied: return "EligibilityDenied";
    case ErrorCode::DivergentCommit: return "DivergentCommit";
    case ErrorCode::LateAuthority: return "LateAuthority";
    case ErrorCode::TopologyMismatch: return "TopologyMismatch";
    case ErrorCode::PolicyMismatch: return "PolicyMismatch";
    case ErrorCode::ManifestInconsistent: return "ManifestInconsistent";
    case ErrorCode::ParticipantNotActive: return "ParticipantNotActive";
    case ErrorCode::DuplicateRegistration: return "DuplicateRegistration";
    case ErrorCode::PartitionNotOwned: return "PartitionNotOwned";
    case ErrorCode::InterestNotFound: return "InterestNotFound";
    case ErrorCode::PartitionNotProduced: return "PartitionNotProduced";
    case ErrorCode::FanOutCeilingExceeded: return "FanOutCeilingExceeded";
    case ErrorCode::FanInCeilingExceeded: return "FanInCeilingExceeded";
    case ErrorCode::ConcurrencyLimitExceeded: return "ConcurrencyLimitExceeded";
    case ErrorCode::PolicyDenied: return "PolicyDenied";
    case ErrorCode::BackpressureActive: return "BackpressureActive";
    case ErrorCode::BackpressureUnknown: return "BackpressureUnknown";
    case ErrorCode::NoWorkAvailable: return "NoWorkAvailable";
    case ErrorCode::WaveLimitReached: return "WaveLimitReached";
    case ErrorCode::Cancelled: return "Cancelled";
    case ErrorCode::ShuttingDown: return "ShuttingDown";
    case ErrorCode::AttemptSuperseded: return "AttemptSuperseded";
    case ErrorCode::EdgeAlreadyDispatched: return "EdgeAlreadyDispatched";
    case ErrorCode::RetryDeferred: return "RetryDeferred";
    case ErrorCode::DuplicateCompletion: return "DuplicateCompletion";
    case ErrorCode::AlreadyCompleted: return "AlreadyCompleted";
    case ErrorCode::PartitionIncomplete: return "PartitionIncomplete";
    case ErrorCode::IntegrityFailure: return "IntegrityFailure";
    case ErrorCode::PayloadRejected: return "PayloadRejected";
    case ErrorCode::AccountingMismatch: return "AccountingMismatch";
    case ErrorCode::PurgedHistory: return "PurgedHistory";
    case ErrorCode::RevalidationRequired: return "RevalidationRequired";
    case ErrorCode::NotCommitted: return "NotCommitted";
    case ErrorCode::RetryExhausted: return "RetryExhausted";
    case ErrorCode::RetryNotAllowed: return "RetryNotAllowed";
    case ErrorCode::AmbiguousOutcome: return "AmbiguousOutcome";
    case ErrorCode::PersistenceFailure: return "PersistenceFailure";
    case ErrorCode::StateCorrupt: return "StateCorrupt";
    case ErrorCode::StateIncompatible: return "StateIncompatible";
    case ErrorCode::StateImpossible: return "StateImpossible";
    case ErrorCode::AtomicReplaceFailed: return "AtomicReplaceFailed";
    case ErrorCode::StateTooLarge: return "StateTooLarge";
    case ErrorCode::JournalSequenceViolation: return "JournalSequenceViolation";
    case ErrorCode::SessionNotFound: return "SessionNotFound";
    case ErrorCode::SessionClosed: return "SessionClosed";
    case ErrorCode::ProtocolViolation: return "ProtocolViolation";
    case ErrorCode::DuplicateFrame: return "DuplicateFrame";
    case ErrorCode::SequenceViolation: return "SequenceViolation";
    case ErrorCode::FrameTypeUnsupported: return "FrameTypeUnsupported";
    case ErrorCode::HandshakeRequired: return "HandshakeRequired";
    case ErrorCode::IdentityMismatch: return "IdentityMismatch";
    case ErrorCode::ConnectionFailure: return "ConnectionFailure";
    case ErrorCode::PeerUnavailable: return "PeerUnavailable";
    case ErrorCode::ConnectionClosed: return "ConnectionClosed";
    case ErrorCode::ResourceExhausted: return "ResourceExhausted";
    case ErrorCode::QueueFull: return "QueueFull";
    case ErrorCode::TooManyParticipants: return "TooManyParticipants";
    case ErrorCode::TooManyPartitions: return "TooManyPartitions";
    case ErrorCode::TooManyChunks: return "TooManyChunks";
    case ErrorCode::TooManyPatterns: return "TooManyPatterns";
    case ErrorCode::TooManyTrackedEdges: return "TooManyTrackedEdges";
  }
  return "UnknownErrorCode";
}

const char* to_string(RetryClass value) noexcept {
  switch (value) {
    case RetryClass::NotApplicable: return "NotApplicable";
    case RetryClass::Retriable: return "Retriable";
    case RetryClass::Deferred: return "Deferred";
    case RetryClass::Authority: return "Authority";
    case RetryClass::Permanent: return "Permanent";
  }
  return "UnknownRetryClass";
}

RetryClass classify(ErrorCode code) noexcept {
  switch (code) {
    case ErrorCode::Ok:
      return RetryClass::NotApplicable;

    // Transient transport or payload damage: another attempt with fresh
    // authority can legitimately succeed.
    case ErrorCode::ConnectionFailure:
    case ErrorCode::PeerUnavailable:
    case ErrorCode::ConnectionClosed:
    case ErrorCode::SessionClosed:
    case ErrorCode::IntegrityFailure:
    case ErrorCode::PayloadRejected:
    case ErrorCode::ChecksumMismatch:
    case ErrorCode::AmbiguousOutcome:
      return RetryClass::Retriable;

    // Not a failure of the transfer: the fabric is applying pressure. The edge
    // stays pending and is revisited on a later wave.
    case ErrorCode::BackpressureActive:
    case ErrorCode::BackpressureUnknown:
    case ErrorCode::RetryDeferred:
    case ErrorCode::NoWorkAvailable:
    case ErrorCode::WaveLimitReached:
    case ErrorCode::ConcurrencyLimitExceeded:
    case ErrorCode::QueueFull:
    case ErrorCode::DuplicateFrame:
      return RetryClass::Deferred;

    // Refusals that another attempt cannot repair: they need new authority,
    // a new generation or a new incarnation.
    case ErrorCode::AuthorityDenied:
    case ErrorCode::EligibilityDenied:
    case ErrorCode::StaleGeneration:
    case ErrorCode::StaleIncarnation:
    case ErrorCode::StaleTopology:
    case ErrorCode::StalePolicy:
    case ErrorCode::StaleAttempt:
    case ErrorCode::IncarnationMismatch:
    case ErrorCode::GenerationMismatch:
    case ErrorCode::LateAuthority:
    case ErrorCode::AttemptSuperseded:
    case ErrorCode::TopologyMismatch:
    case ErrorCode::PolicyMismatch:
    case ErrorCode::ParticipantNotActive:
    case ErrorCode::PartitionNotOwned:
    case ErrorCode::PolicyDenied:
    case ErrorCode::FanOutCeilingExceeded:
    case ErrorCode::FanInCeilingExceeded:
    case ErrorCode::Cancelled:
    case ErrorCode::ShuttingDown:
    case ErrorCode::DivergentCommit:
      return RetryClass::Authority;

    // Everything else is a permanent refusal for this attempt.
    default:
      return RetryClass::Permanent;
  }
}

std::string format_error(const Error& error) {
  std::string rendered = to_string(error.code);
  if (!error.detail.empty()) {
    rendered += ": ";
    rendered += error.detail;
  }
  return rendered;
}

}  // namespace shuffle::fabric
