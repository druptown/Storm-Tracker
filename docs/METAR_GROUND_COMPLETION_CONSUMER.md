# METAR canonical-ground completion consumer (inert shadow boundary)

## Status and scope

`storm_tracker_worker.metar_ground_completion_consumer` is a deliberately
unwired completion consumer for `ground.metar.global`. It is absent from the
worker service, service composition, Home Assistant, MQTT, ground fusion,
ground confirmation, operational fact/snapshot publication, and
meteorological admission.

Its only queue input is a leased terminal-reference event from
`terminal_raw_completion_router` for the closed binding:

- workload and stream: `ground.metar.global`;
- consumer: `metar-canonical-ground-decoder-reference-v1`;
- implementation: `metar_canonical_ground_decoder_v1`;
- transfer: `verified_capture_manifest_reference_v1`.

It performs no provider request, refetch, raw-byte copy, fallback, station
confirmation, rain/dry decision, or operational publication.

## Exact authority chain

Before a child process may be spawned, the parent re-proves:

1. canonical, duplicate-key-free reference bytes and lifecycle event identity;
2. the active desired-state SHA and complete desired-authority proof;
3. the exact inert terminal-consumer catalog binding and fingerprint;
4. the scheduler terminal proof and reconstructed acquisition plan, slot, job,
   workload, provider, and product hashes;
5. the `ACTIVE_SHADOW_RAW` activation rule and exact recovery binding;
6. the terminal execution receipt and committed capacity receipt;
7. the deterministic schema-3 capture ID and sole `csv_gzip` role;
8. the receipt/content hashes, content type, acquisition-job suffix, exact raw
   bytes, AviationWeather native-v2 provenance, and capture provenance.

The child receives no usable lifecycle lease token. It parses the desired state
again and repeats the same closed job/reference/raw proof after opening only
the checksummed source-raw, canonical-ground, and completion-output authorities.
The two SQLite roles must be distinct and neither database may live inside the
raw object tree, including when an attacker recomputes the outer authority
checksum.

## Exact decoder and durable factset

Every non-duplicate run invokes exactly
`metar_ground_evidence.decode_metar_ground_capture` with its closed default
`MetarGroundDecodeLimits`, a shorter absolute deadline when the hard process
envelope requires one, the exact `RawArtifactStore`, and a concrete
`CanonicalGroundEvidenceStore`.

The decoder parses and validates the complete bounded CSV before its one
atomic `commit_batch`. Therefore no partial canonical-ground fact prefix can
survive a malformed input, capacity failure, or deadline. A process kill after
that batch but before completion output is safe: the content-addressed
ground-fact identities make an exact retry idempotent.

The completion layer creates one compact factset receipt. It contains only
capture/decoder identities, count, and hashes; it contains no METAR rows,
station values, raw text, path, upstream identifier, or provenance document.
The factset SHA is a length-framed digest over every complete
`GroundObservationReceipt.to_metadata()`, sorted by fact ID.

Before output commit, after output commit, after any lost child IPC, and again
around lifecycle acknowledgement, the relevant process physically:

- opens the bounded canonical-ground SQLite authority read-only;
- validates its exact schema, triggers, profile, page/catalog envelope, and
  integrity;
- selects the complete closed set for capture + decoder identity/version/config;
- reconstructs every materialized fact, member link, QC row, fact checksum, and
  individual decoder receipt;
- re-verifies one complete raw-capture snapshot for the whole factset;
- recomputes and compares the exact factset ID, count, SHA, and receipt SHA;
- independently rehashes the raw `csv_gzip` bytes and reapplies recovery
  provenance.

A self-consistent forged completion row, factset receipt, or process receipt
cannot suppress the real decode because the physical canonical-ground and raw
authorities remain decisive.

## Hard process and bounded persistence

`MetarGroundCompletionIsolatedRunner` uses `HardDecoderExecutor` with the
mandatory fresh `spawn` method. Default hard limits are:

- wall: 90 seconds;
- CPU: 60 seconds;
- RSS/process memory: 1024 MiB;
- canonical request: 512 KiB;
- child result: 16 KiB;
- each full factset reproof: 45 seconds.

The child result contains only the event ID and output/process receipt hashes.
The output authority is append-only SQLite v2 with synchronous FULL, DELETE
journaling, a persistent random store-instance SHA, exact schema/metadata
validation, checksum-pinned update/delete-denial triggers for metadata and
output rows, receipt count/byte bounds, and a physical page cap. Canonical
path, regular-file, single-link and `(st_dev, st_ino, st_mode)` witnesses are
checked before every SQLite open. The persistent instance is re-read, so an
in-place valid database reset that preserves the inode also fails closed. The
derived identity binds contract, schema, limits, path hash, witness and
instance into the hard-child process authority and inert dispatcher adapter.

An exact v1 store migrates to v2 in one `BEGIN IMMEDIATE` transaction after
full verification of its legacy metadata, columns, limits, output rows and
exact legacy triggers. A fault before commit leaves valid v1; a fault after
commit leaves valid v2 and restart does not repeat the transition.

One output transaction stores both the canonical output receipt and the
process receipt. Finalization then opens a `completion_reproof_lease`: its
`BEGIN IMMEDIATE` starts at the exact final output-row reproof and holds the
SQLite writer fence while the raw authority and complete physical ground
factset are freshly re-proved, through lifecycle acknowledgement. An
out-of-band SQLite writer cannot drop a trigger, delete/recreate the row and
race the ACK. A crash after output commit reconciles the existing receipt
without a second decode.

On POSIX, a same-UID process can still rename a database over an open SQLite
path. The immediate pre-ACK path/instance check catches deterministic
replacement, but Python cannot eliminate the nanorace after its last userspace
check. Production enablement therefore retains the shared systemd/container
directory-authority gate that prevents same-UID path replacement.

Stale desired events are terminalized with the lifecycle store's explicit,
allowlisted rejection receipt. Desired changes, lease reclaim, cancellation,
or deadline expiry at either final acknowledgement boundary never acknowledge
the old proof.

## Shadow-only claims

The durable output always states:

- `canonical_ground_evidence_committed=true`;
- `ground_confirmation=false`;
- `meteorological_admission=false`;
- `operational_fact_published=false`;
- `operational_snapshot_published=false`;
- `fact_revision=null`;
- disposition `shadow_evidence_committed`;
- runtime wiring `inert`.

“Canonical ground evidence committed” describes only durable decoder evidence.
It is not a meteorological fact assertion, ground confirmation, admission
decision, or operational snapshot.

## Public API

The inert composition surface is:

- `validate_metar_ground_completion_lease(...)`;
- `MetarGroundCompletionOutputStore`;
- `MetarGroundCompletionIsolatedRunner`;
- `MetarGroundCompletionConsumer.process_leased(...)`;
- `MetarGroundCompletionConsumer.run_once(...)`.

No live module imports or constructs these objects.

## Verification evidence

`tests/test_worker_metar_ground_completion_consumer.py` covers terminal-router
publication, exact METAR decode, full physical factset reproof, atomic capacity
failure, raw/ground/output/process tamper, forged same-event poison,
crash/restart idempotence, v1-to-v2 before/after-commit migration recovery,
immutable metadata/output definitions, instance/same-inode/path-replacement
reproof, a real DDL/delete/recreate race fenced through exact ACK, concurrent
duplicate calls, stale/final desired races, lease reclaim,
cancellation/deadline, CPU/RSS/wall/request/result/output and SQLite page caps,
path-role aliasing, privacy, exact consumption receipts, a real spawned child,
and absence from live/HA/MQTT/ground-fusion wiring.

Broad regression must additionally include terminal raw routing, acquisition
lifecycle, native METAR capture, canonical METAR ground decoding, canonical
ground evidence persistence, and hard decoder isolation.

## Remaining release gates

This module does not activate the decoder. Live use remains blocked until the
roadmap supplies an explicit coordinator/consumer loop, retention and source
reclamation epochs, resource-soak evidence, scientific comparison/admission
policy, operational publication policy, rollback, and deployment gates.
