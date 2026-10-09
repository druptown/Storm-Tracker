# Continuous RAW retention guard

`storm_tracker_worker/raw_capture_retention_guard.py` is the service-wired,
read-only capacity boundary for continuous RAW capture. It is not a retention
planner and not a reclamation executor.

## Runtime contract

Before service start, before and after every cadence tick, and around a desired
state change that can activate event ingress, the worker must prove:

- the existing worker-first process ownership lease is held before the first
  supplier read and is re-proved after the complete inspection window;
- the cadence RAW root and capacity-ledger file exist with their exact regular
  kinds, contain no symlink/reparse traversal, retain stable identities across
  the inspection, and remain on the configured filesystem;
- active Blitzortung storage paths satisfy the same proof (inactive,
  uninitialized event paths make no storage-existence claim);
- durable ledger counters, an exact-count full startup audit, RAW object,
  receipt and capture-table inventory, and all event headroom counters remain
  within their exact immutable limits;
- enough live filesystem space remains for the unfilled configured RAW
  envelope **plus** the pinned host operational free-space reserve;
- at least one complete closed cadence cycle can still be reserved in the
  ledger, object store, entry inventory, reservation inventory, and catalog
  envelopes.

The guard runs a full ledger/RAW audit at every process start and preserves that
fact across later bounded fast inspections. A failed inspection revokes the
full-audit fact, so only another successful full audit can reopen admission. A
restart therefore cannot inherit an in-memory admission decision: it
reconstructs the decision from the durable stores and current filesystem
capacity.

## Fail-closed response

Capacity exhaustion, unmanaged files, crash staging residue, store-integrity
failure, path drift, ownership loss, or an unverifiable capacity supplier stops
new capture admission. The event sidecar is stopped before the service exposes
the fixed `raw_capture_retention_guard_blocked` failure.

The guard has no unlink, remove, replace, pruning, archive, or reclamation-apply
capability. Diagnostics bind these facts explicitly:

```text
automatic_reclaim=false
source_reclaim_authorized=false
live_apply_wired=false
live_apply_authorized=false
live_delete_performed=false
unmanaged_data_action=alarm_and_block_never_delete
```

Positive source reclamation remains the separately reviewed offline five-role
protocol. The guard never imports that protocol.

## Ownership boundary

The worker-first lease proves one exclusive cooperating process owner. It does
not prove that an administrator, another container, or another host cannot
write to the filesystem. Diagnostics therefore always keep:

```text
exclusive_filesystem_owner_proven=false
external_writer_exclusion_proven=false
```

Path and same-filesystem reproof protect the current inspection from silent
identity drift; they are not presented as an external-writer authority.

## Capacity alarms

Alarm codes are a closed, fixed-size set. Exhaustion alarms block immediately.
Low-capacity alarms preserve admission only while one complete cycle, the full
remaining configured envelope, and the host reserve are still provably
available. No alarm text, path, source payload, credential, or exception detail
enters diagnostics.

The current service v3 profile reserves the full cadence and event envelope.
An event-disabled cadence-only profile requires a new, explicitly materialized
configuration contract; it must not be inferred from a missing environment
authority. Until that new contract exists, the guard correctly keeps the full
event capacity in the physical-space proof.

## Crash/restart evidence

`tests/test_worker_raw_capture_retention_guard.py` hard-exits a child process at
every guard boundary: after ownership, ledger read, RAW-store read, filesystem
capacity probe, final identity reproof, and final ownership reproof. The test
proves the complete case tree is byte-identical after each exit and that a fresh
guard recomputes a green full audit. Separate tests cover missing final nodes,
active-event path absence, identity swap, forged event maxima, unmanaged/staging
residue, object/receipt/capture table exhaustion, full-audit count drift,
full-volume exhaustion, and preservation of the host reserve after the RAW
envelope itself is full.

These local tests do not replace target-host power-loss, media-error, external
filesystem ownership, long soak, or offline reclamation-operator evidence.
