# Acquisition source reclamation v2 apply authority

Status: **inert and unwired**. This boundary does not alter scheduler,
execution, raw-artifact, capacity, lifecycle, service, HA, or MQTT state.
Existing reclamation-v1 receipts remain non-positive and are not upgraded by
this module.

## Purpose

`acquisition_source_reclamation_protocol.py` defines the immutable v2 request,
candidate, decision, local apply/abort, and raw-unlink receipt contracts.
`acquisition_source_reclamation_apply_authority.py` implements two separate
durable authorities:

1. `SqliteSourceReclamationCandidateAuthority` is an append-only catalogue of
   sealed **metadata references**. It never stores provider raw bytes. A
   reference binds the candidate, archive segment and record, content-addressed
   archive-object count/root/total bytes, and all five closed role-local view
   hashes.
2. `AcquisitionSourceReclamationApplyCoordinator` is a FULL/DELETE SQLite
   journal for the exact state machine and participant receipts. It does not
   mutate a source store itself.

Both authorities bind a persistent random instance, canonical path, limits,
schema, and the initial `(st_dev, st_ino, st_mode, st_nlink)` witness into their
identity. The witness is rechecked before every physical operation. A copied or
replaced database at the same path fails closed, including after reopen against
the old expected identity.

## Canonical candidate roots

Candidates are globally sorted by `candidate_sha256`. The candidate inventory
root is a bounded ordered SHA-chain over every exact
`SourceReclamationCandidateBindingV1.binding_sha256`. The archive root is a
separate ordered SHA-chain over candidate, archive-segment, archive-record, and
metadata-reference hashes. Neither algorithm builds a candidate-count-sized
canonical JSON document, so all 4096 candidates remain representable under the
512 KiB single-document bound.

A page contains at most 64 records; each metadata reference is at most 64 KiB
and a page at most 4 MiB. A 101 MiB raw object is represented only by its
content-addressed reference root and integer byte count. Inline payload aliases
or unknown fields are rejected by the closed reference schema.

The five fixed reference fields are:

- `downstream_lifecycle_view_sha256`
- `execution_receipts_view_sha256`
- `scheduler_view_sha256`
- `raw_artifacts_view_sha256`
- `capacity_ledger_view_sha256`

Retention sealing and native participants must use the shared pure builders in
`acquisition_source_reclamation_views.py`. A participant compares its current
role-local projection with the archived role hash before persisting its local
fence.

## Coordinator state machine

Only these transitions are legal:

```text
PREPARING -> PREPARED -> COMMITTED -> APPLIED
     |           |
     +-----------+-> ABORTING -> ABORTED
```

`COMMITTED` is written only after all five participant PREPARED receipts have
been physically reloaded from their constructor-bound participants and match
the coordinator journal exactly. The irreversible apply order is closed:

```text
downstream_lifecycle
-> execution_receipts
-> scheduler
-> raw_artifacts
-> capacity_ledger
```

The frozen `SourceReclamationCandidateAuthority` also exposes
`hold_current_request_authority_v1(request)`. Its backwards-compatible default
holds and validates the request's exact first candidate page with cursor `0`
and page size `1`. Candidate authorities with stronger live currentness may
override that context; retention-v2 additionally binds the current consumer
registry epoch and source-archive witnesses while preserving the same yielded
page contract.

`prepare_v2` holds current request authority continuously across
`begin_preparing_v2`, all five source-local prepare calls in the fixed role
order, their coordinator records, and the transition to PREPARED. `commit_v1`
reacquires current authority and holds it across physical participant-prepare
reproof and the complete coordinator `BEGIN IMMEDIATE` transaction through the
COMMITTED commit. Consequently, current-authority drift before PREPARED or
between PREPARED and COMMITTED cannot create a positive decision. Once the
durable state is COMMITTED or APPLIED, idempotent `prepare_v2`, `commit_v1`, and
`resume_v2` replay the durable decision/progress without requiring historical
requests to remain live-current.

Before a source-local apply, `SqliteHeldCommittedDecisionAuthority` opens
`BEGIN IMMEDIATE` on the coordinator and proves the exact COMMITTED receipt,
participant prepare binding, and complete predecessor-apply prefix. The same
transaction and file witness remain held across the source-local commit. The
authority reproves all evidence before releasing the context.

After the coordinator is durably APPLIED, that same hold has one deliberately
narrow replay mode. It accepts any of the five fixed roles only when all five
apply receipts are durable, their predecessor chain is complete, every receipt
still binds its exact prepare and committed decision, the requested role binds
the caller's exact predecessor, and the COMMITTED and APPLIED event witnesses
both reproof. The complete coordinator metadata, request, prepare, apply,
abort, and request-event rows are captured without normalizing their SQLite
values; their BLOB bytes and scalar encodings must be byte-identical after the
yield. This terminal hold is read-only and appends no event. ABORTING and
ABORTED never satisfy it. COMMITTED retains its next-role-only prefix
semantics, and `record_source_local_apply_v1` remains strictly COMMITTED; the
APPLIED exception authorizes only physical participant replay validation.

The coordinator's own APPLIED fast path is not a journal-only success path.
`apply_v1` (and APPLIED `resume_v2` through it) opens a read-only terminal
reproof transaction, loads all five constructor-bound participants in the
fixed apply order, and requires each physical local apply receipt to equal the
coordinator BLOB exactly. That equality includes the role, source-store and
prepare bindings, committed decision and decision-receipt hashes, predecessor,
effect and post-apply view, release counters, and local applied timestamp. The
coordinator snapshot, full durable decision proof, and exact SQLite row/BLOB
bytes are then re-read before the original snapshot is returned. A missing,
malformed, forged, or changed local receipt fails closed. No participant apply
method is called and no coordinator event is appended during terminal replay.

ABORTING uses the same physical hold. In addition to the abort-decision hash,
it proves the aborting role's exact prepare receipt, source-store identity, and
source-fence hash before that participant may release its fence. Only roles
that durably reached PREPARED need an abort receipt.

The coordinator reloads each returned local apply/abort receipt through that
same participant before accepting it into progress. A caller-built receipt is
not sufficient for APPLIED or ABORTED.

## Public API boundary

Candidate catalogue:

- `SqliteSourceReclamationCandidateAuthority.create(...)`
- reopen with explicit `expected_store_identity_sha256`
- `seal_request_v2(request, bindings=..., record_payloads=...)`
- `hold_candidate_page_v1(request, after_cursor=..., max_candidates=...)`
- `hold_current_request_authority_v1(request)`

Coordinator:

- `AcquisitionSourceReclamationApplyCoordinator.create(...)`
- reopen with explicit `expected_store_identity_sha256`
- PREPARED: `begin_preparing_v2`, `record_source_local_prepare_v2`,
  `finish_prepared_v2`, or `prepare_v2`
- COMMITTED/APPLIED: `commit_v1`, `record_source_local_apply_v1`,
  `finish_applied_v1`, or `apply_v1`
- ABORTED: `begin_aborting_v1`, `record_source_local_abort_v1`,
  `finish_aborted_v1`, or `abort_v1`
- restart: `load_state_v2` and
  `resume_v2(..., allow_commit=<explicit bool>)`
- read-only integrity check: `audit_v2`

The low-level record methods require the exact participant and physically load
its durable local receipt. `allow_commit=False` stops restart handling at
PREPARED; no implicit positive decision is made.

## Durability and recovery matrix

| Fault boundary | Durable state after restart | Required action |
|---|---|---|
| Before candidate seal commit | no request row | retry exact seal |
| After candidate seal commit | exact immutable seal | idempotent load |
| During participant prepare | PREPARING plus a role prefix | resume prepare or abort prepared fences |
| Current authority drifts before PREPARED | no new COMMITTED decision | restore/reseal current authority, or abort durable prepare fences |
| Before COMMITTED commit | PREPARED | physically reproof all participants and retry |
| Current authority drifts after PREPARED | PREPARED | do not commit the stale request; restore/reseal authority or abort |
| After COMMITTED commit | COMMITTED | continue the ordered apply prefix |
| After local apply, before progress record | COMMITTED with old prefix | participant returns its same durable receipt; record it once |
| During ordered apply | COMMITTED with exact prefix | resume at the first missing role |
| After final APPLIED commit | APPLIED, inactive | return the same terminal snapshot |
| During abort | ABORTING with exact abort prefix | resume remaining prepared roles in reverse order |
| After ABORTED commit | ABORTED, inactive | return the same terminal snapshot |

All receipt/event tables are no-update/no-delete. Schema and trigger inventory,
canonical bytes, receipt checksums, event hash-chain continuity, path witness,
row caps, database byte cap, and monotone wall-clock high-water are verified
fail-closed. Directory fsync must be explicitly supplied on platforms where a
native directory barrier is unavailable.

## Offline five-role integration evidence

The final local code freeze exercises the real retention-backed candidate
authority, the real coordinator and all five native participants together in
the fixed order:

```text
downstream_lifecycle
-> execution_receipts
-> scheduler
-> raw_artifacts
-> capacity_ledger
```

`tests/test_worker_acquisition_source_reclamation_native_five_role_integration.py`
proves both positive and refusal/recovery paths without service wiring. One
request reaches PREPARED, COMMITTED and APPLIED, removes the exact archived
lifecycle, execution, scheduler, raw and capacity source facts, writes all
role-local tombstones/effects and keeps the sealed historical resolver
readable. Exact replay and full reopen return the same durable receipts without
refetch, a second unlink or a second capacity decrement. A consumer-registry
epoch advance between PREPARED and COMMITTED refuses the stale commit; abort
releases the five prepared fences, after which an exact higher-epoch successor
can prepare, commit and apply.

This is offline correctness evidence, not a live-filesystem admission claim.
Both filesystem participants expose the same remaining prewiring blocker:

```json
{
  "live_wiring": false,
  "prewiring_blockers": [
    "exclusive_filesystem_owner_dirfd_policy_required_for_crash_atomic_hardlink_exclusion"
  ]
}
```

Raw and lifecycle file deletion verify regular-file type, inode and link count
around the byte hash and fail closed when an alias already exists. Portable
Python cannot exclude a privileged or separately authorised writer creating a
same-filesystem hardlink after the last witness at every instruction-level
crash boundary. Production therefore still requires an exclusive owner/parent
directory policy, a verified directory durability barrier on the target OS,
real power-loss recovery, disk-full/media-fault tests and resource soak.

The complete final offline test inventory is recorded in
`ROADMAP_EXECUTION_STATUS_2026-08-11.md`: 4,934 passed, 15 explicitly explained
platform/live-data skips, and zero failures, errors or warnings across 4,949
collected nodes. That matrix does not activate this authority.

## Activation boundary

The generic coordinator accepts any constructor-bound
`SourceReclamationCandidateAuthority`, including stronger retention-backed
authorities, and the five native source stores implement the frozen participant
contract in their owning modules. This authority remains deliberately absent
from worker-service composition. No hot prune, HA/MQTT mutation, live source
reclamation, or production activation is claimed by this document.
