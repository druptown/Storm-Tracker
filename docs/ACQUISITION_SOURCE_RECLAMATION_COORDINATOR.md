# Acquisition source reclamation coordinator

> **Historical v1 document — sealed evidence.** This file preserves the
> prepare-only coordinator and its hard-disabled apply conclusion. It is not
> the current v2 apply contract. The normative, still unwired v2 boundary is
> `ACQUISITION_SOURCE_RECLAMATION_APPLY_AUTHORITY.md`; v1 receipts remain
> non-positive and are never promoted into v2 permission.

Status: historical v1; worker-side, offline, inert and off by default. This
component is not imported by the worker service, Home Assistant, MQTT or any
provider handler. It does not delete, unlink, release or rewrite source
authority.

## Safety conclusion

The current source schemas cannot safely support positive reclamation.

- The raw artifact authority combines SQLite catalog rows with filesystem
  objects but has no durable, restart-resumable unlink journal.
- The capacity ledger is append-only and has explicit triggers that reject
  reservation deletion; it has no source-local release transaction.
- Scheduler and execution stores expose immutable terminal evidence but no
  source-local prune CAS or archive resolver binding.
- The evidence lifecycle can seal and prune its own hot mirror, but it is not a
  complete registry for every present and future downstream consumer.
- No authoritative consumer-registry root is bound to an authority epoch.

Consequently `commit()` and `apply()` are unconditionally hard disabled. They
raise `AcquisitionSourceReclamationApplyDisabled` with the fixed blockers:

1. `complete_consumer_registry_epoch_root_authority_unavailable`
2. `participant_source_local_apply_transactions_unavailable`
3. `raw_filesystem_unlink_restart_journal_unavailable`

Supplying claimed registry or epoch hashes does not remove these blockers.
The coordinator records those claims in the immutable request only; it cannot
promote them into authority.

## What is implemented

`acquisition_source_reclamation_authority.py` now contains an offline
`AcquisitionSourceReclamationCoordinator` and five participant prepare APIs:

| Participant | Prepare API | Exact sealed resolver |
| --- | --- | --- |
| raw artifacts | `prepare_reference_counted_capture_prune_cas_v1` | `resolve_archived_capture_receipt_object_v1` |
| scheduler | `prepare_terminal_scheduler_prune_cas_v1` | `resolve_archived_scheduler_terminal_v1` |
| execution history | `prepare_execution_history_prune_cas_v1` | `resolve_archived_execution_history_v1` |
| capacity ledger | `prepare_committed_capacity_release_cas_v1` | `resolve_archived_capacity_commit_v1` |
| evidence lifecycle | `prepare_complete_source_reference_closure_cas_v1` | `resolve_archived_source_reference_closure_v1` |

Each prepare API performs two exact current-source reads around one local
`BEGIN IMMEDIATE` sidecar transaction. It writes a content-addressed prepare
receipt only when the current view equals the fully verified sealed archive
view. Current source stores are append-only at these identities, so the
sidecar is a durable evidence receipt. It is deliberately not an apply token.

The raw prepare additionally inventories capture-to-receipt and
receipt-to-object references. Its child order is fixed as:

1. capture membership
2. capture
3. receipt only at zero remaining capture references
4. object only at zero remaining receipt references

The final decision fixes capacity after `raw_artifacts_child_first`. Because
apply is disabled, capacity is never released in the current implementation.

## Durable journal and recovery

The coordinator journal schema supports the ordered state vocabulary
`PREPARING -> PREPARED -> COMMITTED -> APPLIED`. With the current blockers,
only `PREPARING -> PREPARED` is reachable.

- One process lock plus one operating-system file lock fences coordinators.
- Every lock owner obtains a monotonic persisted owner epoch.
- A decreasing wall clock is rejected instead of being silently normalized.
- Requests, participant receipts, the participant root and the final decision
  are immutable SHA-256 identities over canonical JSON.
- A globally contiguous, hash-chained event journal records `BEGIN_PREPARING`,
  each participant boundary and `FINISH_PREPARED`.
- A crash after any participant or coordinator commit is resumed by exact
  idempotent replay. A conflicting replay fails closed.
- Unknown tables/columns, failed SQLite integrity checks, event gaps, event
  hash tampering, partial archives and anchor drift are rejected.
- SQLite full errors and configured file/row bounds become explicit capacity
  errors. No source operation follows such an error.

The coordinator and every participant sidecar use SQLite DELETE journaling,
`synchronous=FULL`, explicit file synchronization and caller-provided or native
directory synchronization. On Windows, production construction must provide a
verified directory synchronization implementation; otherwise initialization
fails closed.

## Bounded retention and resource behavior

Coordinator decisions, events, participant prepares, candidate count and
database bytes all have hard limits. Reaching a limit rejects a new prepare;
no old refusal evidence is silently discarded. An idempotent replay reuses the
same decision, five participant receipts and seven events, so its journal
footprint plateaus. Tests cover a 1,000-candidate aggregate prepare and 2,000
disabled-apply replays without journal growth.

The component never performs database compaction and never issues SQLite
`VACUUM`. Future source apply work
must not hide unbounded growth behind compaction; it needs a separately proven
retention/archive protocol for completed decision evidence.

## Offline runbook

There is intentionally no command-line or service entry point yet.

1. Build a fresh `AcquisitionSourceRetentionPlan` from the five exact source
   authorities.
2. Export every eligible record into `AcquisitionSourceRetentionArchive`.
3. Verify `archive.audit()` is healthy and record its anchor SHA-256.
4. Construct the coordinator in a dedicated absolute directory outside all
   source stores, with a verified directory-sync callback where required.
5. Call `prepare(plan)`. The only successful state today is `PREPARED` with
   `reclaim_authorized=false` and `source_release_performed=false`.
6. Run `coordinator.audit()` and retain its hashes as test evidence.
7. Use `coordinator.resolver(request_sha256)` only for exact sealed reads.
8. Do not call this a cleanup, release or reclamation success. No capacity or
   source bytes were released.

Crash recovery repeats steps 3 through 6 with the same archive, plan and
coordinator directory. A changed archive anchor, source view or participant
view requires a new plan; it must never be forced through the old request.

## Migration path to a future positive implementation

Positive reclamation remains blocked until all of these are delivered and
independently tested:

1. A versioned, complete downstream consumer registry with an immutable root
   bound to one authority epoch and explicit registration/de-registration
   semantics.
2. Source-local prepare, apply and resolver transactions in all five source
   schemas—not sidecars—including immutable decision and expected-view roots.
3. A raw filesystem journal that durably records child-first unlink intent,
   directory synchronization and exact crash recovery before catalog removal.
4. Source-local tombstones/resolvers that prevent scheduler reconcile or any
   reader from rematerializing a committed archived identity.
5. Capacity release CAS that requires a verified raw `APPLIED` receipt and
   decrements provider/global counters exactly once.
6. A cross-store recovery proof for every boundary through `COMMITTED` and
   `APPLIED`, including concurrent owners, full disks and host crashes.
7. A retention protocol for completed coordinator and participant decisions.
8. A separate red-team review and explicit activation change. Merely changing
   `APPLY_ENABLED` is insufficient and must remain rejected by tests.

No source schema migration is performed by the current implementation. The
sidecar files are backward compatible and removable only as offline test
artifacts; deleting them in a future deployment would lose prepare evidence
but cannot expose a source mutation because apply is unavailable.
