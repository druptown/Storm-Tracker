# NOAA ERR terminal-completion consumer (inert shadow boundary)

## Status and scope

`storm_tracker_worker.noaa_err_completion_consumer` is an inert, unwired
reference consumer for exactly one closed terminal-router route:

- workload/stream: `satellite.noaa.err`
- consumer: `noaa-err-quantitative-decoder-reference-v1`
- input payload: `stv3-terminal-raw-manifest-reference-v1`
- input proof: `stv3-terminal-raw-reference-proof-v1`
- decoder: exactly
  `noaa_err_quantitative.decode_noaa_err_region`

It is not imported by worker service composition, `main`, Home Assistant,
MQTT, snapshot publication, fact staging or operational admission. Its output
is always a factless shadow candidate with admission withheld.

## Closed lifecycle

1. The existing acquisition scheduler, execution store, capacity ledger and
   raw-capture store reach a successful terminal.
2. `TerminalRawCompletionRouter` publishes one content-addressed manifest
   reference into `AcquisitionEvidenceLifecycleStore`. It copies no raw bytes
   and performs no refetch.
3. `NoaaErrCompletionConsumer` leases only the exact consumer/stream above,
   reloads the lease, and obtains the desired state from its durable
   `DesiredStateStore`. A missing or superseded desired state is durably
   rejected once, so it cannot poison and repeatedly reclaim the stream head.
   The consumer then re-proves the canonical router payload and proof, closed
   catalog fingerprint, desired authority, reconstructed acquisition job,
   capture manifest and every raw role receipt.
4. `issue_noaa_err_region_authority` derives one non-wrapping spherical-cap
   union from the acquisition job's exact desired-state regions and targets.
   It records the resulting native GLB row/column window and decoder grid and
   coverage hashes. Region count, target count, radius and pixel count are
   bounded.
5. One fresh spawn-only child opens only the explicitly bound stores, rechecks
   the capture and region/window authority, and invokes the exact NOAA ERR
   decoder. CPU time, RSS, wall time, request bytes and result bytes are hard
   capped by `HardDecoderExecutor` (Windows Job Object or Linux rlimits/process
   accounting).
6. The decode child commits the derived content-addressed artifact and
   canonical decoder evidence, then atomically commits a bounded, append-only
   output receipt. Before acknowledgement, the parent rechecks the exact lease
   and active desired identity and starts a second hard child. That child does
   not decode: it reopens and re-proves the raw capture, canonical evidence,
   candidate artifact and output receipt from their bound stores.
7. The parent repeats the lease, desired and exact-output barriers at the
   acknowledgement boundary. It accepts only the exact lifecycle
   `ConsumptionReceipt`, including its schema, event/producer/stream identity,
   output hash and self-checksum.

An interruption before the output receipt leaves the lifecycle event
unacknowledged and retryable. An interruption after output commit but before
ack is reconciled from the exact receipt on restart and does not decode again;
it still performs the fresh hard-process authority reproof before ack. A
conflicting receipt, manifest, authority, region, event, store role or content
identity fails closed.

## Public API

The bounded public surface is exported from
`storm_tracker_worker.noaa_err_completion_consumer`:

- `issue_noaa_err_region_authority(*, desired_state, lease, policy=...)`
  returns the exact bounded geographic authority or raises before decode.
- `NoaaErrCompletionOutputStore(path, *, limits=None, clock=...)` owns the
  checksum-verified output-before-ack receipts.
- `NoaaErrCompletionIsolatedRunner(raw_store=..., artifact_store=...,
  evidence_store=..., output_store=..., limits=..., executor=None)` owns the
  spawn authority and exact durable reconciliation.
- `NoaaErrCompletionConsumer(lifecycle_store=..., desired_state_store=...,
  ...)` requires disjoint durable lifecycle, desired, canonical-evidence and
  output databases and disjoint raw/candidate object stores.
- `run_once(owner_id, lease_seconds=..., cancel_event=None)` leases at most one
  event, reads the active desired state itself, decodes/reconciles it and acks
  only a verified durable output.
- `process_leased(lease, desired_state, cancel_event)` is the bounded
  coordinator-facing form and also verifies that the supplied desired state
  is still active. It returns a
  `PipelineOutcome` whose disposition is `withheld`, with
  `fact_revision_staged=False`.

Default process caps are 75 seconds wall, 60 seconds CPU, 1536 MiB RSS, 1 MiB
request JSON and 16 KiB result JSON. A standalone lease must contain both hard
process boundaries plus coordination margin; the default lease is 180 seconds.
Output-store defaults are 4096 receipts, 128 KiB per receipt and a hard 64 MiB
SQLite page cap. Its schema and immutable update/delete triggers are rechecked
on access. All limit objects are included in content-addressed authority
fingerprints.

## Output claims and privacy

The durable output contains only receipt identities, hashes, bounded region
metadata, an opaque content-addressed artifact handle and explicit negative
claims. It contains no raw bytes, raw path, endpoint URL, lease token,
credential, secret, fact revision or snapshot claim. The required claims are:

```json
{
  "decoded_shadow_candidate": true,
  "fact_staged": false,
  "meteorological_admission_granted": false,
  "operational_snapshot_published": false,
  "raw_refetch_performed": false
}
```

Diagnostics are bounded and path-free. Process requests necessarily carry the
closed local store paths into the spawned child, but those paths are neither
returned nor persisted in the output/lifecycle receipt.

## Geographic fail-closed boundary

There is deliberately no implicit global or default target region. If the
reconstructed `satellite.noaa.err` acquisition job lacks a non-empty and exact
`region_ids`/`target_ids` binding, if a referenced region is absent from the
desired acquisition plan, or if the union crosses a pole/dateline or exceeds a
configured cap, the consumer refuses the event before spawning the decoder.
Such a missing target-region binding remains a hard admission blocker; it is
never inferred from the raw file's global coverage.

## Verification coverage

`tests/test_worker_noaa_err_completion_consumer.py` constructs the real native
NOAA NetCDF4 capture and full scheduler/execution/capacity/router lifecycle.
Its 26 focused cases cover exact decoder invocation, a real OS spawn,
output-before-ack, crash/restart/idempotence, concurrent duplicate claims,
stale-head rejection, desired and lease changes at both pre-ack barriers,
router/raw/canonical-artifact/output tamper, forged same-event authority and
consumption receipts, missing or mismatched target-region authority, disjoint
stores, hard database page capacity, cancellation, deadline, CPU/RSS/result
caps, privacy and static proof that the module remains unwired. The
terminal-router suite separately preserves DWD compatibility and covers the
other closed provider routes.

## Activation blockers

This boundary intentionally provides no service factory or live registration.
Activation requires a separate reviewed decision about scheduling/ownership,
resource budgets on the target host, retention and downstream scientific
admission. None of those decisions may be inferred from a successful shadow
completion receipt.
