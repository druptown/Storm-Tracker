# Met Office UK RATE completion consumer (inert shadow boundary)

## Status and scope

`storm_tracker_worker.met_office_rate_completion_consumer` is a local, deliberately
unwired completion consumer for `radar.metoffice.uk_rate`. It is not imported by the
worker service or service composition and has no Home Assistant, MQTT,
operational snapshot, fact-store, or meteorological-admission connection.

The only accepted queue input is a leased completion event produced by
`terminal_raw_completion_router` for the closed catalog binding:

- workload and stream: `radar.metoffice.uk_rate`;
- consumer: `metoffice-rate-quantitative-decoder-reference-v1`;
- transfer: `verified_capture_manifest_reference_v1`;
- implementation: `met_office_rate_quantitative_v1`.

No provider request, refetch, raw copy, weather decision, cell extraction, or
fallback is performed at this boundary.

## Exact authority chain

Before a process may be spawned, the parent re-proves all of the following:

1. canonical, duplicate-key-free reference bytes and lifecycle event identity;
2. the current desired-state SHA and complete desired-authority proof;
3. the exact inert consumer-catalog binding and fingerprint;
4. scheduler terminal proof, reconstructed historical acquisition slot, plan,
   job, workload, provider, and product hashes;
5. the ACTIVE_SHADOW_RAW activation and recovery-binding fingerprint;
6. the terminal execution receipt and committed capacity receipt;
7. the deterministic capture ID and exact three-role manifest:
   `listing_current`, `listing_previous`, and `odim_hdf5`;
8. every receipt/content hash, content type, job suffix, raw byte, listing
   prefix/object selection, ETag, size, timestamp, and existing recovery
   provenance rule that binds both listings to the selected ODIM object.

The child receives no lifecycle lease token. It independently parses the
desired state and repeats the complete reference/job/raw verification after it
opens only the checksummed store paths and immutable limits in the process
authority. Source-raw, derived-artifact, canonical-evidence, and completion-
output roles must be disjoint: aliased databases, overlapping object roots,
or a database placed inside either object root are rejected even if an attacker
recomputes the outer authority checksum.

## Hard process and decoder

Every non-duplicate decode uses `HardDecoderExecutor` with the mandatory
`spawn` start method. The default envelope is:

- wall time: 105 seconds;
- CPU time: 90 seconds;
- RSS/process-memory boundary: 1536 MiB;
- canonical request: 512 KiB;
- child result: 16 KiB.

The child invokes exactly
`met_office_rate_quantitative.decode_met_office_rate_capture` with one
`MetOfficeRateRawAuthority` shared by the decoder and canonical evidence store.
Decoder job and attempt IDs are deterministic hashes of the completion event,
capture, catalog consumer, and decoder configuration. The existing Met Office
artifact store and canonical evidence store remain the scientific authorities;
the completion layer never reimplements decoding or invents values.

The hard-kill effect contract is `idempotent`. A kill can leave:

- no artifact/evidence/output, after which exact retry is safe;
- a content-addressed artifact but no evidence, after which exact retry
  verifies/reuses it;
- canonical evidence but no completion output, after which exact retry reuses
  the deterministic evidence commit;
- a committed completion output/process receipt but lost child IPC, after
  which the parent reloads and verifies the durable row before interpreting
  the technical child outcome.

Reloading is not limited to the completion database's self-consistency. The
parent rehashes all three raw roles, reapplies listing/object/capture provenance,
reopens the canonical decoder run, re-verifies its raw-capture snapshot,
matches every decoder/evidence identity and outcome, and, for a complete
candidate, verifies the exact opaque Met Office handle, checksum, size, and physical
derived object. An internally rehashed completion row cannot replace a decoder
rejection with a fabricated successful candidate or suppress the real decode.

## Durable output before acknowledgement

The output store is a bounded SQLite append-only authority with FULL
synchronous durability, DELETE journaling, immutable profile metadata, and
update/delete denial triggers. Receipt-count, canonical-output-byte, and
SQLite-page limits are independently enforced; a pre-inflated database above
its configured page cap is refused on open. One transaction commits both:

- the canonical payload-free shadow output receipt; and
- the hard-process receipt binding the event, output, authority, effect
  contract, and process limits.

Output-store schema v2 adds a random persistent `store_instance_sha256`
sentinel and exact metadata no-update/no-delete triggers. `identity_sha256`
binds that sentinel, contract/schema, limits fingerprint, canonical path hash,
and the initial `(st_dev, st_ino, st_mode)` regular-file witness. Every open
rechecks that the visible path is still the same single-link file. The signed
child request carries the expected output-store identity and rechecks it before
decode/commit and after durable reload, so a valid same-path database
replacement cannot redirect the child. The former exact v1 schema migrates
inside one `BEGIN IMMEDIATE` transaction after all existing rows are rehashed;
faults before commit leave exact v1 and faults after commit leave valid v2.

The coordinator then reloads the lifecycle lease and active desired state,
re-proves the completion plus canonical evidence/artifact authorities, checks
cancellation and expiry, executes the explicit pre-ack fault boundary, and
repeats those checks immediately before lifecycle acknowledgement. A lease
reclaim at that boundary cannot be acknowledged with the old lease. A crash
after output commit and before acknowledgement therefore causes no second
decode and no conflicting output on restart.

The final output-row reproof now runs inside
`completion_reproof_lease`. It holds the output database's `BEGIN IMMEDIATE`
writer fence across the existing pre-ACK fault boundary, the final raw,
canonical-evidence and physical-artifact reproof, the last
cancel/deadline/lease/desired barriers, and lifecycle acknowledgement. A
concurrent trigger-drop/delete/recreate writer is blocked before ACK; exact
retry reuses the one durable output without another decode. Same-inode sentinel
tamper and same-path replacement fail closed. A crash after ACK is also
replay-safe because both the terminal lifecycle receipt and immutable output
are already durable.

An unavailable or superseded desired state is not left as a perpetually
reclaimable stream head. The coordinator uses the lifecycle store's durable,
allowlisted rejection receipt before advancing the cursor. If desired state
changes after a factless output was committed, that output remains immutable
for audit/reconciliation but the lifecycle event is terminalized with the
separate rejection receipt, never with the candidate output SHA.

The output contract is intentionally factless:

- `fact_revision` is always `null`;
- `fact_staged`, `meteorological_admission`, and
  `operational_snapshot_published` are always `false`;
- a successful quantitative decode is only a `shadow_candidate`;
- a decoder rejection is only `shadow_rejected`;
- neither disposition is an operational weather assertion.

## Fail-closed behavior

Tampered JSON, proof hashes, desired authority, reconstructed job facts, raw
bytes, manifest members, canonical decoder evidence, physical derived
artifacts, output rows, process authority/store-role paths, or immutable store
profiles stop processing without acknowledgement. Cancellation or lease expiry
after a durable output also leaves the event unacknowledged so a later exact
lease can reconcile it. Capacity exhaustion never evicts evidence and never
turns a partial output into success. Absolute filesystem paths and
credential/capability-shaped text are rejected before an output can be stored.

Diagnostics contain only bounded status/count/hash metadata. Store paths, raw
payloads, upstream identifiers, provenance documents, credentials, and lease
tokens are absent from output receipts, process results, and diagnostics.

## Verification evidence

`tests/test_worker_met_office_rate_completion_consumer.py` covers:

- real terminal-router reference publication and lifecycle consumption;
- both a decoder rejection and a valid quantitative ODIM HDF5 decode;
- a real spawned hard process;
- output-before-ack crash/restart reconciliation and duplicate idempotence;
- v1-to-v2 migration, injected old-or-new migration crashes, persistent store
  identity, metadata tamper, same-path replacement, and child identity binding;
- a real trigger-drop/delete/recreate race at the final reproof lease, exact
  retry without another decode, and crash immediately after lifecycle ACK;
- desired supersession before processing and desired/lease races at the final
  acknowledgement boundary, including durable lifecycle rejection;
- job/reference/proof/raw/output/canonical-evidence/artifact and rehashed
  store-role-path tamper refusal, including forged same-job completion poison;
- wall/CPU/RSS/request/result/output/database caps, cancellation before and
  after output, private-path refusal, and bounded diagnostics;
- factless/admission-false output and absence from live wiring.

Broad regression should include the terminal raw router, acquisition evidence
lifecycle, hard process isolation, and Met Office quantitative decoder suites.

## Remaining release gates

This module alone does not make Met Office operational. Live activation remains
blocked until the broader roadmap explicitly supplies and validates a
coordinator/consumer loop, lifecycle retention and source-reclamation epochs,
scientific comparison/admission policy, operational fact publication,
end-to-end resource soak evidence, and rollback/deployment gates. Those gates
must not be bypassed by importing this module from the current service.
The v2 identity/fence does not authorize retention, pruning, reclamation, or
runtime wiring.
