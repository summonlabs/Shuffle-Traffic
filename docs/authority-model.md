# Shuffle Fabric authority model

Verified against the working tree at 2026-09-20 11:25. Source hashes (SHA-256,
first 12 hex digits): `coordinator.hpp` D41C50FB21A1, `coordinator.cpp`
227FB20CB2C6, `ledger.hpp` 93D725727833, `ledger.cpp` B38A7B0ABD8C,
`schedule.hpp` 33F18E1EA81A, `schedule.cpp` 115F3D3BE6B0, `topology.hpp`
2CFB9AFD40B6, `topology.cpp` 69FD299E3D9B, `records.hpp` B7AEF23A7CA0,
`edge.hpp` B98AC2C6A108, `error.cpp` D0C6D8FDA871.

This document is the invariant reference. Each rule states what is claimed, where
it is enforced, and the deterministic refusal that reports a violation.

## 1. The invariant list

| # | Invariant | Enforced in | Violation |
| --- | --- | --- | --- |
| 1 | **Observation is not authority.** A pressure reading can defer dispatch; it can never authorise or complete one. | `edge.hpp` (`PressureReading`), `Coordinator::update_congestion`, `Coordinator::freshen`, `WaveScheduler::decide` | no refusal is needed: the reading has no code path into grants or commits |
| 2 | **Eligibility is not authority.** An edge is required from registration until withdrawal, even while its consumer is down; the ability to move or complete it additionally requires a current `Active` incarnation. | `topology.cpp` `counts_toward_requirement()` vs `holds_authority()`; `Topology::covering_consumers` | `ParticipantNotActive` (219) for dispatch/commit/state change; the edge stays explicitly incomplete |
| 3 | **Planning is not authority.** A `WavePlan` is a proposal; authority exists only once the coordinator registers a `DispatchGrant`. | `schedule.hpp`, `Coordinator::next_wave` | planning a wave grants nothing by itself; `grants_` is the registry |
| 4 | **Dispatch is not completion.** An edge with an attempt in flight is not resolved; only a verified completion satisfies it. | `CompletionLedger::edge_status`; `EdgeStatus::authoritative` is true only for `Completed` | an edge stays `Pending` (or `Failed`/`Purged`) until a durable commit exists |
| 5 | **Completion is not authoritative until it is durable.** The record is on disk before the ledger is mutated and before the caller is answered. | `Coordinator::commit_transfer`, `Coordinator::commit_record` | `PersistenceFailure` (600) with nothing applied |
| 6 | **Persistence is not currentness.** A recovered record describes what was durable at the previous epoch; it does not testify that a participant, an attempt or a pressure reading is current. | `Coordinator::recover`, `RecoveryReport` | `StaleAttempt` (208), `RevalidationRequired` (407) |
| 7 | **Reachable is not current.** A peer that answers a socket is not thereby a current incarnation, and no transport outcome is a protocol decision. | `socket.hpp`, `Coordinator` incarnation checks | `StaleIncarnation` (205), `IdentityMismatch` (707) |
| 8 | **A matching identifier is not a current generation.** Identity equality is necessary and never sufficient: the shuffle, topology, policy and partition generations are checked separately. | `coordinator.cpp`, `schedule.cpp`, `ledger.cpp` | `StaleGeneration` (204), `StaleTopology` (206), `StalePolicy` (207), `GenerationMismatch` (210) |
| 9 | **Exactly-once per partition generation.** A completion is recorded once per (partition, partition generation); repeats are suppressed and never account bytes twice. | `CompletionLedger::commit` | `DuplicateCompletion` (400) is reported in explanations; the call itself succeeds with `duplicate == true` |
| 10 | **Divergence is refused, never merged.** A second claim about the same partition generation with a different manifest digest or producing incarnation is a contradiction. | `CompletionLedger::validate_commit`, `Coordinator::publish_manifest`, `Coordinator::apply_manifest` | `DivergentCommit` (214) |
| 11 | **Re-production retires the old generation from the current edge set.** A stale completion cannot keep satisfying an edge that now requires fresh content. | `Coordinator::apply_manifest` -> `CompletionLedger::retire_generation`; `CompletionLedger::commit` | no refusal: the superseded record stays readable as history |
| 12 | **A new incarnation supersedes the previous one immediately.** | `Topology::register_producer`, `Topology::register_consumer` | `StaleIncarnation` (205) for every later use of the superseded incarnation |
| 13 | **In-flight authority does not survive a restart.** Attempts live in memory; every boot advances the durable epoch. | `Coordinator::adopt` (`grants_.clear()`), `Coordinator::persist_epoch`, `Coordinator::recover` | `StaleAttempt` (208), "attempt was not issued in this coordinator epoch" |
| 14 | **Recovery is conservative.** Every participant that held authority comes back `Suspect` and holds none until it re-registers with a fresh incarnation. Congestion evidence is never persisted. | `Coordinator::recover`, `records.hpp` (congestion deliberately absent) | `RevalidationRequired` (407); `revalidation_required()` stays true until every demoted participant returns |
| 15 | **Cancellation ends authority and is idempotent.** It stops new dispatch, strips authority from work in flight, and refuses late completions. | `Coordinator::cancel_shuffle`, `Coordinator::apply_shuffle_state`, `WaveScheduler::abandon_all` | `Cancelled` (308) |
| 16 | **Bounds are refusals, not growth.** Every externally influenced size passes through `Limits`. | `Limits::validate`, `ByteReader`, `Topology`, `CompletionLedger`, `DurableStore` | `LimitExceeded` (110), `TooMany*` (8xx), `StateTooLarge` (605) |
| 17 | **The durable store is single-writer by contract.** Two live writers on one directory are not arbitrated. | `durable_store.hpp` | the format detects the damage (sequence going backwards is `Corrupt`) rather than preventing it |

## 2. Observation vs authority

`PressureReading` carries `level`, `observed`, `fresh`, `observed_at`,
`reporter` and `policy_generation`. `edge.hpp` states the rule: "Pressure is
evidence, never authority: a reading can defer dispatch, it can never authorise or
complete one."

* `Coordinator::update_congestion` is the only writer. Before storing a reading
  it requires: a current `Active` reporter incarnation, a matching policy
  generation, a level in [0, 100] and an observation tick that is not in the
  future. Anything else is `StaleIncarnation`, `StalePolicy`,
  `ParticipantNotActive`, `InvalidArgument`.
* Readings are stored in memory only (`global_pressure_`,
  `producer_pressure_`, `consumer_pressure_`) and are absent from
  `records.hpp`: no durable record carries congestion evidence.
* `Coordinator::freshen` defines freshness structurally: a reading is fresh only
  when it was observed, its policy generation is the current one, its tick is not
  in the future and `now - observed_at <= policy.congestion.evidence_validity_ticks`.
* The scheduler's decision is tri-state (`Allow`, `Paused`, `Unknown`).
  Absent or stale evidence is never "no congestion": with
  `require_fresh_evidence` it pauses the target (`deferred_unknown_pressure`),
  and without it the previous decision stays in force.

## 3. Required edges vs authority

| Question | Answer | Code |
| --- | --- | --- |
| Is this (partition, consumer) edge required? | Yes from registration until an explicit withdrawal. `counts_toward_requirement(state)` is `state != Withdrawn`, so `Pending`, `Active`, `Suspect` and `Failed` all count | `withdraw_consumer` is the only transition that removes the requirement |
| May this edge be dispatched now? | Only if the consumer holds authority (`Active`) and the producing incarnation is the partition's current owner and holds authority | `skipped_unproduced` in the plan; commit refuses with `ParticipantNotActive` |
| May this completion be accepted? | Only if the completing consumer and the producing incarnation are both `Active`, current, and the partition is still owned by that producer | `ParticipantNotActive`, `PartitionNotOwned` (221) |

A consumer that is down therefore leaves work explicitly incomplete rather than
shrinking the requirement, and `ProgressSnapshot::accounting_closes()` keeps
`edges_completed + edges_failed + edges_incomplete == edges_required`.
`edges_over_counted` is non-zero exactly when resolved edges exceed the current
requirement (a withdrawal after completion is the legitimate case), and
`accounting_closes()` requires it to be zero.

## 4. Planning vs dispatch

`WaveScheduler` reads through `SchedulingEnvironment` (topology, completion
view, congestion view, partition view) and never mutates the ledger, the
topology or the manifest store. A grant is a value object carrying the attempt,
wave, shuffle, shuffle generation, partition, partition generation, producer,
producer incarnation and endpoint, consumer, consumer incarnation, topology
generation, policy generation, manifest digest, total bytes, attempt ordinal and
issue tick.

`Coordinator::next_wave` is where a plan becomes authority: for each grant it
inserts into `grants_` and calls `CompletionLedger::note_attempt_bytes`.
Attempted bytes are a pressure signal, never completion accounting. A plan that
returns no grants grants nothing; `WaveScheduler::next_wave` consumes a wave
identity only when it produced at least one grant.

## 5. Dispatch vs completion, completion vs commit

* In-flight is **scheduler state**, not ledger state: `edge_attempts_`,
  `attempts_`, `producer_in_flight_`, `consumer_in_flight_`,
  `global_in_flight_`. `CompletionLedger::edge_status` never returns
  `EdgePhase::Dispatched`; the `Dispatched` phase exists in the vocabulary and
  is not produced by the ledger in this revision.
* `EdgeStatus::authoritative` is documented as "true only for Completed: a
  durable commit exists". A `Pending` edge with three failed attempts is still
  pending, and a `Failed` edge is an explicit terminal record, not an absence.
* `commit_transfer` answers with a `CommitReceipt` whose three booleans are
  distinct claims: `newly_committed` (this partition generation was committed by
  this call), `edge_newly_completed` (this consumer joined the completed set) and
  `duplicate` (nothing changed). Only the first accounts bytes, and only once.

## 6. Attempt-bound completion

Every completion is bound to the attempt the coordinator issued:

| Check | Refusal |
| --- | --- |
| the shuffle is not cancelled/cancelling (checked first) | `Cancelled` |
| the attempt id is present in `grants_` | `StaleAttempt` (208) |
| the shuffle is open and a topology exists | `InvalidState` / `Cancelled` |
| shuffle generation equals the open generation | `StaleGeneration` |
| partition, partition generation and consumer equal the grant | `AttemptSuperseded` (310) |
| producer, producer incarnation and consumer incarnation equal the grant | `StaleIncarnation` |
| the manifest digest equals the grant's | `ManifestInconsistent` (218) |
| the consumer is registered, current and `Active` | `UnknownParticipant` (203) / `StaleIncarnation` / `ParticipantNotActive` |
| the partition is still owned by the claiming producer, whose incarnation is current and `Active` | `PartitionNotOwned` / `StaleIncarnation` / `ParticipantNotActive` |
| an accepted manifest exists for exactly this partition generation | `StaleGeneration` |
| the accepted manifest's digest equals the claim's | `DigestMismatch` (211) |
| `integrity_verified` is set | `IntegrityFailure` (403) |
| the observed partition digest equals the manifest digest or the partition digest | `DigestMismatch` |
| the byte count equals the manifest's | `AccountingMismatch` (405) |
| the chunk-evidence count equals the manifest chunk count and every digest matches | `IntegrityFailure` / `DigestMismatch` |

`report_failure` is the mirror image: a code classified `Deferred` is refused as
`InvalidArgument` ("a deferred condition is scheduling pressure, not a transfer
failure"), a cancelled shuffle refuses with `Cancelled`, an unknown attempt with
`StaleAttempt`, and permanence is decided by
`authority_refused || classify(code) == Permanent || attempt_ordinal >= retry.max_attempts`.
A durable record is written for the failure, after which the attempt is resolved
and removed from the registry.

Note on ordering: the comment above the registry lookup in `commit_transfer`
says the attempt registry is consulted before the shuffle's lifecycle state, but
the cancellation check is a lifecycle test and precedes the lookup. Both orders
are deterministic; the observable difference is that a completion offered after a
cancellation is reported as `Cancelled` even when the attempt also left the
registry (for example because the coordinator restarted).

## 7. Exactly-once, divergence and retirement

`CompletionLedger` keys a completion by `partition_key(partition, generation)`
and stores the ascending, unique set of consumers that completed it.

| Situation | Behaviour |
| --- | --- |
| First commit for a (partition, generation) | `newly_committed = true`, `edge_newly_completed = true`, `bytes_committed += request.bytes`, one `CommitSequence` consumed |
| Another consumer, same generation, same digest/incarnation/bytes | `edge_newly_completed = true`, no byte re-accounting |
| The same consumer again | `duplicate = true`, `duplicate_commits` incremented, nothing accounted |
| Same generation, different manifest digest, producer or producer incarnation | `DivergentCommit` |
| Same generation, different byte count | `AccountingMismatch` |
| Generation older than `latest_generation_` | `StaleGeneration` ("partition generation has been superseded; late authority is refused") |
| More tracked partitions/edges than the bounds allow | `TooManyTrackedEdges` (806) |

Re-production is what moves `latest_generation_`:
`Coordinator::apply_manifest` calls `CompletionLedger::retire_generation` for
every accepted manifest, which drops the previous generation's completions and
permanent failures out of the *current* edge set while keeping them readable as
history (`retained_keys_`, `max_retained_generations`, after which the oldest
generation's records are purged and reported as `EdgePhase::Purged` /
`PurgedHistory` (406)). A commit that itself carries a newer generation retires
the previous one inside `commit()` by the same rule.

## 8. Incarnation supersession

`Topology::register_producer`/`register_consumer` are idempotent when
(id, incarnation, endpoint[, selection]) is unchanged, and otherwise replace the
record with the new incarnation in `Active` state and bump the topology
generation. The superseded incarnation keeps its identifier but loses authority
at that instant:

* `set_participant_state` -> `StaleIncarnation`;
* `publish_manifest` -> `StaleIncarnation`;
* `commit_transfer` -> `StaleIncarnation`;
* `update_congestion` -> `StaleIncarnation`;
* the scheduler skips a partition whose content was produced by a superseded
  incarnation and counts it as `skipped_unproduced`, so the current owner must
  produce it again.

Re-registering the *same* incarnation with a *different* endpoint is refused with
`IdentityMismatch` ("producer re-registered with a different endpoint under the
same incarnation"), so an incarnation cannot be re-pointed at another address.

Withdrawal is the one state that removes the requirement: `set_consumer_state`
to `Withdrawn` removes the consumer from its pattern's consumer list, and the
identifier stays registered so a later re-registration is a fresh incarnation
rather than a resurrection.

## 9. Epoch, recovery and currentness

* `Coordinator::open_shuffle` persists an `EpochAdvanced` record with
  `epoch_ + 1` before the `Open` state record; `recover()` does the same for
  the recovered coordinator. `apply_record` only ever raises `epoch_`.
* `adopt()` transfers every authority-bearing member and explicitly clears
  `grants_`: an attempt issued before the restart cannot be found afterwards.
  `test_unit_coordinator` asserts both the epoch advance (0 -> 1 -> 2) and
  `StaleAttempt` for a pre-restart grant.
* `recover()` replays the snapshot payload (record list, then the ledger's own
  encoding) and then the journal records through the same `apply_record` path
  used live, into a scratch coordinator that is adopted only when every step
  succeeded.
* After replay, every producer and consumer that `holds_authority` is set to
  `Suspect` and recorded in `unrevalidated_producers_`/`unrevalidated_consumers_`;
  `pending_revalidation_` counts them. This demotion is an in-memory recovery
  decision, not a durable transition: it is re-derived on every recovery, and the
  journal still holds the pre-restart state.
* `revalidation_required()` is `pending_revalidation_ > 0`. A participant clears
  its own entry only by re-registering with a *fresh* incarnation
  (`apply_participant` compares the previous incarnation with the record's).
* Congestion evidence is dropped: nothing in `records.hpp` carries it, so a
  recovered coordinator starts with no pressure readings at all.
* `history_incomplete` is propagated from `RecoveryReport::revalidation_required`
  by the tool and explained as "recovery discarded a torn journal tail; the
  discarded record was never acknowledged".

## 10. Cancellation and late authority

`cancel_shuffle(reason)` refuses `reason == Ok` with `InvalidArgument`, is
idempotent once cancelled, and otherwise writes a durable `ShuffleStateRecord`
with state `Cancelled` and the reason. It is the apply path that removes
authority: `apply_shuffle_state` calls `scheduler_.abandon_all(reason_)` and
clears `grants_` whenever the state is `Cancelling` or `Cancelled`.

Afterwards: `commit_transfer` -> `Cancelled` ("shuffle is cancelled: late
authority is refused"), `report_failure` -> `Cancelled`, `next_wave` ->
`Cancelled`, and `register_*`/`set_participant_state`/`publish_manifest` ->
`Cancelled` via `ensure_open()`. Because cancellation is durable, a coordinator
recovered from a cancelled directory comes back cancelled
(`test_coordinator_recovery` asserts this).

## 11. The durable ordering rule

`coordinator.hpp`:

> Ordering rule for every durable mutation:
>   validate authority -> encode record -> make the record durable ->
>   apply it in memory -> answer.

| Mutation | Validated before the append | Record | Applied after the append |
| --- | --- | --- | --- |
| `open_shuffle` | guard, state == Closed, non-zero identity, partition count and `max_partitions`, `validate_policy`, policy/shuffle agreement | `EpochAdvanced`, `ShuffleState(Open)` | epoch, state, request, topology construction, ledger bind |
| `register_producer/consumer` | guard, open state, `check_*_registration` (identity, endpoint size, participant bound, selection shape, pattern bound) | `Participant` | incarnation replacement, pattern interning, topology generation bump |
| `set_participant_state` | guard, open state, `check_participant_state` | `Participant` | state transition, requirement bookkeeping |
| `publish_manifest` | guard, open state, `validate_manifest`, shuffle generation, partition range, ownership, incarnation, authority, generation ordering and divergence | `Manifest` | manifest store, partition facts, `retire_generation` |
| `commit_transfer` | guard, not cancelled, attempt registered, open, every binding in §6 including the content evidence | `Commit` | ledger commit, grant removal, scheduler resolve, completion check |
| `report_failure` | guard, code not `Deferred`, not cancelled, attempt registered, open, permanence, and the tracked-edge bound ("Bounds are checked before the record becomes durable so that a durable record can never fail to apply") | `Failure` | ledger failure record, grant removal, scheduler resolve |
| `cancel_shuffle` | guard, state, reason non-zero | `ShuffleState(Cancelled)` | state, `abandon_all`, registry clear |
| `note_completion` (implicit) | closure computation | `ShuffleState(Completed)` when completed + failed >= required | state |

**When durability fails.** `persist()` is called before any mutation of the
in-memory state; a refused append returns `PersistenceFailure` to the caller and
nothing is applied. `test_unit_coordinator` proves the stronger property: after a
refused append the topology generation is unchanged, the participant count is
unchanged and the identical call succeeds afterwards. With a `VolatileSink` the
append cannot fail, but `durable()` is false and `explain()` always carries a
`CapabilityUnsupported` entry saying so.

**Where the pre-append validation stops (observed behaviour).** The bindings of a
completion are validated before the append, but the ledger's own structural
preconditions are not: `CompletionLedger::validate_commit` — the function
`ledger.hpp` documents as existing "so a caller can make the mutation durable in
between and know the application cannot fail afterwards" — is called only from
`CompletionLedger::commit`, and the coordinator never calls it directly. A
`completion` that passes the coordinator's checks can therefore be appended and
then refused by the ledger with `StaleGeneration`, `DivergentCommit`,
`AccountingMismatch`, `TooManyTrackedEdges`, `TooManyTrackedPartitions`,
`UnknownPartition` or `InvalidArgument`. The same is true of a failure record
whose code is `Ok` (`report_failure` does not reject `Ok`; the ledger does).
Replay uses the same `apply_commit`/`apply_failure` calls, so such a record
replays into the same refusal and `recover()` reports it instead of adopting a
partially replayed state.

## 12. Error-code vocabulary actually produced

`ErrorCode` values are stable (1xx input/decoding, 2xx identity/authority, 3xx
policy/lifecycle, 4xx accounting, 5xx retry, 6xx durable state, 7xx
transport/protocol, 8xx resources) and every code has exactly one
`classify()` result. Not every code is produced by this library in this
revision: `EligibilityDenied`, `LateAuthority`, `AuthorityDenied`,
`DuplicateRegistration`, `InterestNotFound`, `EdgeAlreadyDispatched`,
`AlreadyCompleted` and `RetryExhausted` appear in `to_string`/`classify`
but at no construction site under `src/`. They remain part of the wire
vocabulary: a peer may report one through `ReportFailureRequest`, and a refusal
envelope may carry one.
