# RAW capture reclamation controller

## Status

`raw_capture_reclamation_controller.py` is an offline-only, default-disabled
operator boundary over the durable acquisition-source reclamation protocol. It
does not make reclamation operationally ready and is not wired into the worker
service, Home Assistant, MQTT, meteorological output, dashboards, or alerts.

The controller contains no candidate discovery and no direct deletion
primitive. The existing five-role coordinator remains the only component that
may ask its constructor-bound participants to release an exact source.

## Closed authority boundary

Every operation requires all of the following exact, content-addressed inputs:

- one existing `SourceReclamationRequestV2` made by the candidate authority;
- one phase-specific `RawCaptureReclamationPhaseCommandV1` held by
  `worker_authority.raw_capture_reclamation_operator_v1`;
- one blocker proof held and physically re-proved by
  `worker_authority.raw_capture_reclamation_blockers_v1`;
- the exact coordinator authority and five participants in the fixed order
  `downstream_lifecycle`, `execution_receipts`, `scheduler`, `raw_artifacts`,
  `capacity_ledger`;
- unique source-store identities for those five roles.

The caller supplies the request and every command. The controller never lists,
ranks, ages, or selects candidates. A boolean enable is necessary but not
sufficient: both external authority holds must remain valid for the complete
phase.

The blocker proof binds the request, candidate root, sealed archive anchor,
archive binding root, archive audit, complete hold inventory, complete golden
reference inventory, and the overlap counts. PREPARE, COMMIT, and APPLY require
the proof to be clear. A new hold, golden-reference overlap, incomplete
inventory, or failed archive verification stops release.

ABORT is deliberately different. It still requires an exact operator command
and exact blocker-proof identity under both held authorities, but a non-clear
proof may not trap already prepared source fences. ABORT may release fences;
it may never release source data.

## Explicit phase sequence

1. **PREPARE** installs only the five durable source-local fences. The returned
   prepared review binds the exact five prepare receipts and their release
   envelope. It contains no mutable coordinator snapshot identity, so the same
   review remains verifiable after a later durable state transition.
2. An operator reviews that prepared evidence and issues a distinct **COMMIT**
   command binding its hash and maximum releasable bytes/entries per role.
   COMMIT records the durable decision but performs no source release.
3. An operator reviews the committed receipt and issues a distinct **APPLY**
   command binding both the prepared-review hash and committed-receipt hash.
   APPLY refuses PREPARED state and cannot silently perform COMMIT. The durable
   coordinator applies the exact request in the fixed five-role order.
4. Before COMMIT, an explicit **ABORT** command may remove prepared fences. It
   has a zero release envelope and reports `source_release_performed=false`.

Each command is content addressed, time bounded, explicitly operator initiated,
and states `automatic_candidate_selection=false`. A phase command cannot stand
in for a different phase.

## Capacity and failure behavior

Controller limits bound candidate count, per-role releasable bytes and entries,
and diagnostic size. A prepared release envelope larger than the operator's
command fails closed. That failure may leave a durable PREPARED fence; the
operator must inspect it and issue ABORT or a new sufficiently bounded command.
The controller never auto-aborts and never auto-advances.

Durable coordinator transitions are replayed, not repeated. After a crash at a
PREPARED, COMMITTED, or APPLIED boundary, a new controller instance can replay
the same still-valid command against the same authorities and five stores. A
fresh command can be issued when the original authorization expires. APPLY
replay requires the exact committed receipt and re-proofs the five participant
receipts through the coordinator.

Exceptions from authority implementations are translated to fixed error codes.
Diagnostics contain only bounded counters, contract identifiers, booleans, and
SHA-256 identities. They never include request bodies, candidate names, paths,
provider payloads, credentials, or external exception text.

## Deliberately open production gates

This contract does not supply concrete physical operator or blocker-authority
implementations. It also does not resolve the separately documented exclusive-
owner, same-filesystem, hardlink/directory-authority admission requirements of
the acquisition-source reclamation stores. Therefore:

- `PRODUCTION_ENABLE_READY` remains false;
- `LIVE_SERVICE_WIRING` remains false;
- `AUTOMATIC_RECLAMATION` remains false;
- meteorological and dashboard output remain false.

No live filesystem data was deleted or altered while implementing or testing
this controller contract.
