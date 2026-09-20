# Shuffle Fabric wire protocol, version 1

Verified against the working tree at 2026-09-20 11:36. Source hashes (SHA-256,
first 12 hex digits): `frame.hpp` A1C00D4083BD, `frame.cpp` 9B5BF7F5EA4D,
`service.hpp` C9A6799D2DDA, `service.cpp` 9AF7783F0AD5, `codec.hpp`
4B306767AB54, `codec.cpp` 7190C86992EE, `socket.hpp` 69321BC3E6EE,
`tools/shuffle-fabric-node.cpp` 233FFDEC73ED. Line numbers cited below are lines
of that revision; the tree was being edited while this document was written.

Everything below is little-endian, fixed width, no padding, no varints, no
alignment. Strings are a 32-bit byte count followed by UTF-8 bytes; byte strings
are a 32-bit byte count followed by raw bytes; a `digest` is 32 raw bytes; a
bool is one byte, 0 or 1 (any other value is `MalformedInput`). A decoder that
does not consume the whole payload reports `TrailingGarbage` (104): canonical
form is exact.

## 1. Transport assumptions

* TCP over IPv4 numeric literals ("127.0.0.1", "0.0.0.0") and the literal name
  "localhost". There is no name resolution, by design (`socket.hpp`).
* Sockets are non-blocking and every operation carries an explicit millisecond
  budget, capped at `kMaxWaitMs` = 600000. A budget is transport
  responsiveness: no protocol decision depends on how long a wait lasted.
* No TLS, no signing, no peer authentication. A session binds the identity a
  peer claims in its handshake envelope. See the "unvalidated" section of
  `docs/proof-surfaces.md`.
* Management traffic is what this document defines. Bulk chunk bytes travel on a
  separate connection between participants (section 9).

## 2. The frame header

A frame is exactly 40 header bytes followed by exactly `payload_length` payload
bytes: no trailer, no extension space, no delimiter to scan for.

| Offset | Size | Field | Meaning |
| --- | --- | --- | --- |
| 0 | 4 | magic | `'S','F','B','1'` in wire order; little-endian value `0x31424653` |
| 4 | 2 | version | `kProtocolVersion` = 1 |
| 6 | 2 | message type | one of the 32 values in section 3 |
| 8 | 4 | flags | bit 0 (`kFlagResponse` = 0x1) marks a response; every other bit is reserved and must be zero |
| 12 | 8 | session id | the session this frame belongs to; 0 means "not yet bound" |
| 20 | 8 | sequence | per direction, starts at 1, strictly increasing |
| 28 | 4 | payload length | bytes that follow the header |
| 32 | 4 | payload CRC-32C | over the payload bytes |
| 36 | 4 | header CRC-32C | over bytes [0, 36) |
| 40 | `payload_length` | payload | message-specific, see section 6 |

The header CRC is therefore computed last by the encoder and verified first by
the decoder (`src/frame.cpp` `encode_frame`@197, `decode_header`@259).

**Field interpretation order** (documented in `frame.hpp`, implemented in
`decode_header`): structure (length, magic) -> version -> integrity (header CRC)
-> semantics (reserved flags, message type, declared length, sequence). Only the
magic and the version are read before the header CRC passes, because until then
every other field, including `payload_length`, is attacker-controlled input and
must not steer an allocation, a bound or an error message.

## 3. Message types

`MessageType` values are part of the wire contract: never reused, never
renumbered. 4..9, 36..39 and 43..65535 are reserved so a later revision can add
messages without renumbering anything deployed. `kMessageTypes` lists all 32
values of version 1 in numeric order.

| # | Name | Direction | Payload (section 6) |
| --- | --- | --- | --- |
| 1 | `HandshakeRequest` | client -> server | `H` |
| 2 | `HandshakeResponse` | server -> client | envelope only |
| 3 | `ErrorResponse` | either -> either | envelope only |
| 10 | `OpenShuffleRequest` | client -> server | `O` |
| 11 | `OpenShuffleResponse` | server -> client | envelope only |
| 12 | `RegisterParticipantRequest` | client -> server | `R` |
| 13 | `RegisterParticipantResponse` | server -> client | `R2` |
| 14 | `PublishManifestRequest` | client -> server | `M` |
| 15 | `PublishManifestResponse` | server -> client | `M2` |
| 16 | `NextWaveRequest` | client -> server | empty |
| 17 | `NextWaveResponse` | server -> client | `W` |
| 18 | `CommitTransferRequest` | client -> server | `C` |
| 19 | `CommitTransferResponse` | server -> client | `C2` |
| 20 | `ReportFailureRequest` | client -> server | `F` |
| 21 | `ReportFailureResponse` | server -> client | `F2` |
| 22 | `CongestionIntentRequest` | client -> server | `G` |
| 23 | `CongestionIntentResponse` | server -> client | envelope only |
| 24 | `CancelShuffleRequest` | client -> server | `X` |
| 25 | `CancelShuffleResponse` | server -> client | envelope only |
| 26 | `ProgressRequest` | client -> server | empty |
| 27 | `ProgressResponse` | server -> client | `P` |
| 28 | `ExplainRequest` | client -> server | `E` |
| 29 | `ExplainResponse` | server -> client | `E2` |
| 30 | `CloseRequest` | client -> server | empty |
| 31 | `CloseResponse` | server -> client | envelope only |
| 32 | `ManifestRequest` | client -> server | `Q` |
| 33 | `ManifestResponse` | server -> client | `Q2` |
| 34 | `StatusRequest` | client -> server | empty |
| 35 | `StatusResponse` | server -> client | `S` |
| 40 | `ChunkFetchRequest` | consumer -> producer (data plane) | section 9 |
| 41 | `ChunkFetchResponse` | producer -> consumer (data plane) | section 9 |
| 42 | `ChunkFetchFailure` | producer -> consumer (data plane) | section 9 |

Request/response classification is a property of the number, not of the frame:
`is_request`/`is_response` answer from the table alone, so a peer can decide
whether an incoming message may be answered before it has a frame to look at.
`is_response` includes `ErrorResponse` and `ChunkFetchFailure`;
`is_request` excludes them.

## 4. The uniform reply envelope

Every reply payload begins with:

| Field | Size |
| --- | --- |
| error code (`ErrorCode`) | 2 bytes |
| detail length | 4 bytes |
| detail | `detail_length` UTF-8 bytes (ASCII in practice) |

When the code is not `Ok` the reply carries **no further bytes** and the reply
type is `ErrorResponse` (server refusals) or the type-specific response with a
non-Ok code, which the client accepts only if the body is exactly the envelope.
A code outside the version-1 range is `ProtocolViolation` on decode
(`read_envelope`, `src/service.cpp`@191; `is_known_error_code` accepts 0 and
`InvalidArgument`..`TooManyTrackedEdges`).

The reply frame header always carries the `Response` flag, the session id the
server assigned and the request's sequence number. A refusal the server cannot
answer in a session — the initial session-bound refusal — is an `ErrorResponse`
with session 0 and sequence 1 (`refuse_socket`, `src/service.cpp`@826);
the connection is closed immediately afterwards.

## 5. Sessions, identity binding and provenance

* A connection begins with `HandshakeRequest`. Any other first message is
  refused with `HandshakeRequired` (706) and the session ends.
* The handshake payload is checked for a known participant kind and a complete
  identity (`participant_id != 0` and a non-zero incarnation); the server then
  assigns the session id (`next_wire_session_`) and answers
  `HandshakeResponse` with an empty body. The client learns its id from the
  reply header and must carry it on every later frame.
* Every later frame whose header session id disagrees with the bound session is
  refused with `IdentityMismatch` (707) **before the payload is decoded**
  (`src/service.cpp`@1096). A second `HandshakeRequest` on a bound session is
  `ProtocolViolation`.
* The payload's own identity fields are never trusted as provenance. They are
  compared with the session identity and a contradiction is `IdentityMismatch`:

| Message | What must agree with the handshake envelope |
| --- | --- |
| `RegisterParticipantRequest` | kind, id and incarnation (`check_participant_identity`, `src/service.cpp`@164) |
| `PublishManifestRequest` | the session must be a Producer, and `manifest.producer`/`manifest.producer_incarnation` must equal it |
| `CommitTransferRequest` | a Consumer session must match `consumer`/`consumer_incarnation`; a Producer session must match `producer`/`producer_incarnation` |
| `CongestionIntentRequest` | reporter kind, id and incarnation |

Because the protocol is not authenticated, this binding is a consistency check,
not proof of origin: a peer that knows the digests can claim a completion it did
not perform (README, "Limitations actually observed").

## 6. Payload layouts

```
H   HandshakeRequest    {u8 kind, u64 participant_id, u64 incarnation, u64 boot_nonce}
O   OpenShuffleRequest  {u64 shuffle, u64 generation, u32 partition_count, policy}
R   RegisterParticipant {u8 kind, u64 id, u64 incarnation, string endpoint,
                         u8 selection_kind, u64 begin, u64 end, u32 list_count, u64[]}
M   PublishManifest     {byte string: encoded manifest}
W   NextWaveResponse    {u64 wave, u32 grant_count, grants..., counters}
C   CommitTransfer      {CommitRequest}
F   ReportFailure       {u64 attempt, u16 error_code}
G   CongestionIntent    {u8 reporter_kind, u64 reporter_id, u64 reporter_incarnation,
                         u64 producer, u64 consumer, u32 level, u64 policy_generation,
                         u64 observed_at}
X   CancelShuffle       {u16 reason}
Q   ManifestRequest     {u64 partition}
E   ExplainRequest      {u32 max_samples}
```

Responses (all preceded by the uniform envelope):

```
R2  RegisterParticipantResponse {u64 topology_generation, u64 incarnation, u8 superseded_previous}
M2  PublishManifestResponse     {digest manifest_digest, u64 partition_generation}
W   grant (repeated grant_count times):
        {u64 attempt, u64 wave, u64 shuffle, u64 shuffle_generation, u64 partition,
         u64 partition_generation, u64 producer, u64 producer_incarnation,
         string producer_endpoint, u64 consumer, u64 consumer_incarnation,
         u64 topology_generation, u64 policy_generation, digest manifest_digest,
         u64 total_bytes, u32 attempt_ordinal, u64 issued_at}
    counters (all u32): examined, skipped_completed, skipped_failed, skipped_in_flight,
        skipped_unproduced, deferred_limits, deferred_source_pressure,
        deferred_destination_pressure, deferred_unknown_pressure, deferred_retry_wait
    then u8 cursor_wrapped, u8 all_resolved
C2  CommitTransferResponse {u8 newly_committed, u8 edge_newly_completed, u8 duplicate,
                            u64 sequence, digest manifest_digest, u64 accounted_bytes,
                            u8 shuffle_completed}
F2  ReportFailureResponse  {u8 permanent, u32 attempts, u64 ready_at}
Q2  ManifestResponse       {byte string: encoded manifest}
P   ProgressResponse       {progress snapshot: 20 u64 fields}
E2  ExplainResponse        {u32 entry_count, {u16 code, string subject, string detail, u64 count}[]}
S   StatusResponse         {u8 state, u16 reason, u64 epoch, u64 shuffle, u64 shuffle_generation,
                            u64 topology_generation, u64 policy_generation, u64 tick,
                            u32 active_producers, u32 active_consumers, u32 in_flight,
                            u64 persisted_records, u8 durable, u8 revalidation_required,
                            u8 history_incomplete, progress snapshot}
```

The progress snapshot is 20 `u64` fields in `ProgressSnapshot` declaration
order: shuffle, shuffle_generation, partitions_total, partitions_committed,
partitions_failed, partitions_incomplete, edges_required, edges_completed,
edges_failed, edges_incomplete, **edges_over_counted**, bytes_committed,
bytes_attempted, duplicate_commits_suppressed, retriable_failures,
permanent_failures, authority_refusals, tracked_partitions, tracked_edges, tick.

`CommitRequest` (wire form, identical to the durable `CommitRecord` payload —
the two are written by `write_commit_request` in `src/service.cpp` and
`src/records.cpp`, which list the same fields in the same order):

```
u64 edge.partition, u64 edge.partition_generation, u64 edge.consumer,
u64 attempt, u64 wave, u64 shuffle_generation, u64 topology_generation,
u64 policy_generation, u64 producer, u64 producer_incarnation, u64 consumer,
u64 consumer_incarnation, digest manifest_digest, digest observed_partition_digest,
u8 integrity_verified, u64 bytes, u32 chunk_count, digest[chunk_count]
```

The embedded policy envelope is:

```
u64 shuffle, u64 shuffle_generation, u64 policy_generation,
u32 fan.max_fan_out, u32 fan.max_fan_in,
u32 concurrency.global, u32 concurrency.per_source, u32 concurrency.per_destination,
u32 waves.max_grants_per_wave, u32 waves.max_edges_examined_per_wave,
u32 retry.max_attempts, u32 retry.min_ticks_between_attempts,
u32 congestion.pause_threshold, u32 congestion.resume_threshold,
u8 congestion.require_fresh_evidence, u32 congestion.evidence_validity_ticks,
then Limits: 20 u64 fields in Limits declaration order
```

A decoded policy is re-validated with `validate_policy`, so an envelope that
decodes but is structurally inconsistent (per-source above global, zero fan
bounds, `retry.max_attempts` outside [1, 64], resume above pause, a zero
validity window while fresh evidence is required, ...) is refused by the same
rules as one constructed locally.

## 7. Replay, duplicates and ordering

`ReplayGuard` (`frame.hpp`, implemented at `src/frame.cpp`@449) answers one
question — has this sequence number already been used? — for one direction of one
session:

| Sequence | Result |
| --- | --- |
| 0 | `SequenceViolation` (704); wire sequences start at 1 |
| already accepted | `false` (duplicate), no error |
| new high-water mark | `true` |
| above the window, never accepted | `true`, unless it jumps more than `kMaxForwardJump` = 2^40 past the highest seen, which is `MalformedInput` |
| below the window, never accepted | `SequenceViolation` |

The window is a memory bound: at most `min(window, kMaxReplayWindow)` accepted
sequences are retained, and `kMaxReplayWindow` is 65536. It is deliberately not
the ordering authority — the session enforces ordering itself.

On the server:

* each session has a `ReplayGuard` with window `kReplayWindow` = 1024;
* a duplicate sequence is **not** applied twice: the server looks it up in a
  fixed 32-entry cache of the answers it already sent
  (`kAnswerCacheEntries`, `src/service.cpp`@129) and re-sends the identical
  frame; a duplicate whose answer has left that window is refused with
  `DuplicateFrame` (703);
* a sequence refusal or a frame-level refusal has no sequence to answer with, so
  the session ends instead of resynchronising.

On the client, every reply is checked against the request: the `Response` flag,
the request's sequence number (also fed through the client's own `ReplayGuard`),
the session id and the expected response type. Any violation drops the
connection and reports `ProtocolViolation` — a peer that is not speaking
version 1 as this code defines it cannot be trusted to keep the stream in sync.

## 8. Every rejection a malformed frame or payload produces

Frame layer (`src/frame.cpp`; decoder order is the order in the table):

| Condition | Code |
| --- | --- |
| fewer than 40 bytes where a header is required | `TruncatedInput` (102) |
| more than 40 bytes where exactly one header is required | `TrailingGarbage` (104) |
| magic != 'SFB1' | `ProtocolViolation` (702) |
| version != 1 | `UnsupportedVersion` (105) |
| header CRC-32C mismatch | `ChecksumMismatch` (106) |
| reserved flag bits set | `MalformedInput` (101) |
| unknown message type | `FrameTypeUnsupported` (705) |
| declared payload length above `max_frame_payload_bytes` | `OversizedInput` (103) |
| declared sequence 0 | `SequenceViolation` (704) |
| buffer shorter than the declared frame | `TruncatedInput` |
| payload CRC-32C mismatch | `ChecksumMismatch` |
| bytes after a complete frame (`decode_frame`) | `TrailingGarbage` |
| frame size arithmetic overflow | `IntegerOverflow` (107) |

Encoder refusals (`encode_frame`): `UnsupportedVersion`,
`FrameTypeUnsupported`, `MalformedInput` (reserved flags),
`SequenceViolation` (sequence 0), `OversizedInput` (payload above
`max_frame_payload_bytes`, or above the 32-bit length field), `InvalidArgument`
(a caller-supplied non-zero `payload_length` or `payload_crc32c` that
contradicts the payload), `IntegerOverflow`, `LimitExceeded` (110) when the
whole frame would exceed `max_allocation_bytes`. On any refusal the output
buffer is left byte-identical.

Stream reader (`FrameStream`): `NoWorkAvailable` (306) while a whole frame is
not buffered; `ProtocolViolation` as soon as a buffered prefix contradicts the
magic; `ResourceExhausted` (800) when `feed()` would push the queue past
`max_frame_payload_bytes + 40`; every other refusal is a `decode_header`
refusal of the buffered header.

Payload layer (`src/service.cpp`, `codec.cpp`, decode helpers):

| Condition | Code |
| --- | --- |
| payload longer than `max_message_payload_bytes` | `OversizedInput` |
| first message on a connection is not `HandshakeRequest` | `HandshakeRequired` |
| handshake payload truncated, or with trailing bytes | `TruncatedInput` / `TrailingGarbage` |
| handshake kind is not Producer/Consumer | `MalformedInput` |
| handshake identity incomplete (zero id or incarnation) | `InvalidArgument` (100) |
| `HandshakeRequest` on a bound session | `ProtocolViolation` |
| header session id differs from the bound session | `IdentityMismatch` |
| a declared byte-string or string longer than its bound | `OversizedInput` |
| a collection count above its bound | `LimitExceeded` |
| a collection count above the bytes remaining | `MalformedInput` |
| a string that is not valid UTF-8 | `InvalidUnicode` (108) |
| a bool byte other than 0/1 | `MalformedInput` |
| an unknown selection kind, participant kind or reporter kind | `MalformedInput` |
| bytes left after the canonical form | `TrailingGarbage` |
| `ReportFailureRequest` with code 0 or an unknown code | `MalformedInput` |
| `CancelShuffleRequest` with reason 0 or an unknown code | `MalformedInput` |
| a reply whose envelope carries an unknown code | `ProtocolViolation` |
| a response type (or any `ChunkFetch*`) sent as a request | `FrameTypeUnsupported` |
| session count above `max_sessions` | `QueueFull` (801), sent as a bare `ErrorResponse` |

Everything the coordinator then refuses is carried verbatim in the envelope: the
`ErrorCode` and the detail string produced by `Coordinator` (truncated to
`max_string_bytes`, 4096 by default).

## 9. The data plane: `ChunkFetch*`

The data plane is deliberately separate from the management plane: participants
exchange chunks directly and the coordinator never carries bulk data.
`CoordinatorServer` has no case for message types 40..42; they fall into the
default branch of `Impl::handle` (`src/service.cpp`@1517) and are answered with
`FrameTypeUnsupported`, "message type ... is not served by the coordinator
service". `CoordinatorClient` exposes no chunk fetch either.

The wire types are defined and used by `tools/shuffle-fabric-node.cpp`, which is
a producer-side listener, not the coordinator:

```
ChunkFetchRequest  {u64 shuffle, u64 shuffle_generation, u64 partition,
                    u64 partition_generation, u64 chunk}
ChunkFetchResponse {u64 chunk, u64 offset, u32 length, digest, u32 payload_length, payload}
ChunkFetchFailure  {u16 error_code, string detail}
```

Observed behaviour of that implementation, which is the only one in the tree:

| Aspect | Behaviour |
| --- | --- |
| Session | no handshake: the producer answers frames that carry session id 0, echoing the request's sequence number |
| Sequence | the consumer numbers its requests 1, 2, 3... within one connection; the producer does not enforce gap-free ordering |
| Replay | a request whose sequence equals the previous request's sequence is answered by re-sending the previous answer **byte for byte** and the chunk is not read again; the `--fault-duplicate-frame` flag makes the consumer verify that property |
| Verification | the consumer checks the returned chunk id, offset, length and payload size against the requested descriptor, hashes the payload itself and compares with the manifest digest; a mismatch is `DigestMismatch`, a descriptor mismatch is `PayloadRejected` (404) |
| Refusals | `StaleGeneration` when the shuffle/generation differ, `UnknownPartition` (201) when no accepted manifest exists for that partition generation, `UnknownChunk` (202) when the chunk is outside the manifest, `MalformedInput` when the request does not decode, `InternalError` (114) when the manifest describes bytes outside the produced payload |
| Faults | `--fault-corrupt-chunk K` flips one bit of the served bytes while still claiming the manifest digest; `--fault-truncate-chunk K` writes a frame prefix and then closes the stream |

## 10. Management and data plane split

| | Management plane | Data plane |
| --- | --- | --- |
| Frames | types 1..35 | types 40..42 |
| Served by | `CoordinatorServer` (one process, one coordinator, one mutex) | a participant (the producer of the chunk) |
| Carries | identities, generations, digests, counters, manifests, status | chunk payload bytes |
| Authority | granted, bound and refused by the coordinator | none: a chunk answer is evidence, and the consumer's commit is what creates authority |
| Bound | `max_frame_payload_bytes` = 1 MiB per frame, `max_message_payload_bytes` = 8 MiB per decoded message | same framing layer, so the same bounds |
