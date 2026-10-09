# Raw-manifest audit completion consumer

Status: locally implemented and tested, deliberately inert and unwired.

This consumer closes the current `raw_manifest_audit` subset of
`TERMINAL_RAW_CONSUMER_CATALOG`. It consumes only the durable completion references emitted
through the terminal raw completion router and stored by
`AcquisitionEvidenceLifecycleStore`. It does not fetch provider data, decode meteorology, stage
facts, request admission, publish snapshots, or copy raw bytes.

## Closed catalogue

| Workload | Required raw roles | Maximum capture bytes |
| --- | --- | ---: |
| `radar.aemet.composite` | `opendata_data_response`, `opendata_envelope_json`, `opendata_metadata_response` | 105,906,176 |
| `radar.dpc.sri` | `catalog_json`, `geotiff` | 33,554,432 |
| `radar.meteofrance.dpradar` | `catalog_json`, `hdf5` | 50,331,648 |
| `satellite.goes18.rrqpe` | `listing_current`, `listing_previous`, `netcdf4` | 20,971,520 |
| `satellite.goes19.rrqpe` | `listing_current`, `listing_previous`, `netcdf4` | 20,971,520 |
| `satellite.hsaf.h40b` | `listing_nlst`, `netcdf_gzip` | 69,206,016 |

Import-time catalogue validation derives this set from the router catalogue. The parametrised
test also pins the six-entry current inventory, so an intentional router catalogue change must
arrive with an explicit consumer/test review.

## Authority and durable order

For a leased event, the parent performs bounded metadata validation of the exact event,
consumer/stream binding, desired-state authority, reconstructed scheduler slot and acquisition
job, activation rule, recovery binding, execution/capacity proof, capture manifest, and role
set. It does not open the raw payloads.

The isolated child repeats that validation and then reopens the authoritative raw store. It
checks every receipt and capture manifest and computes SHA-256 over every raw role. Only after
that full reproof may it transactionally append both the audit output and its process-receipt
hash. The parent reloads and verifies that durable record before acknowledging the lifecycle
lease. A restart after output commit but before acknowledgement reconciles the same immutable
record rather than writing a second output.

The output authority is schema v2. Its metadata contains a random persistent
`store_instance_sha256` sentinel protected by exact no-update/no-delete triggers. Its public
`identity_sha256` binds that sentinel, the limits fingerprint, schema/contract, canonical path
hash, and the initial `(st_dev, st_ino, st_mode)` file witness. Every open rechecks the visible
path against that witness and requires a single regular-file link. The signed child request
also carries the expected identity, so a same-path valid-database replacement cannot redirect
the isolated commit. The exact former v1 schema is migrated inside one `BEGIN IMMEDIATE`
transaction; injected failures before commit leave exact v1, while failures after commit leave
valid v2. Existing receipt rows are fully rehashed before migration commits.

Immediately before acknowledgement, a second hard-isolated child repeats the complete raw and
manifest SHA reproof and idempotently reopens the exact output/process receipt. The coordinator
then reloads the lease, active desired state, and output one final time and validates every field
and checksum of the returned lifecycle `ConsumptionReceipt`. This repeat is intentional: the
raw source is mutable across process boundaries, so an output committed earlier in the lease is
not by itself current proof that the source still matches at the ack boundary.

After that final child returns, `finalize_leased` acquires an output-store
`completion_reproof_lease`. It validates the exact row, receipt, process receipt, schema,
triggers, persistent instance, and live file witness under `BEGIN IMMEDIATE`. The reserved
writer lock remains held across the final lease/desired/cancel/deadline barrier and durable
lifecycle acknowledgement. A concurrent trigger-drop/delete/recreate writer therefore fails
before ACK and the same pending continuation can retry; path replacement or same-inode sentinel
tamper fails closed. A crash after the lifecycle ACK remains replay-safe: the terminal lifecycle
receipt and single immutable output already exist, so restart observes no second item.

An absent or superseded desired state is terminalised through the lifecycle store's durable
rejection receipt. It cannot remain the stream head and be reclaimed forever. A desired-state
change after output commit is handled the same way, leaving the inert audit output as orphaned
evidence rather than mislabelling it as accepted work.

Every output is explicitly `fact_revision=null`, `meteorological_admission=false`, and has all
decode, fact, snapshot, raw-copy, and refetch claims set to false.

## Why this work uses a hard process

The parent path is bounded canonical metadata/proof work. The child performs a meaningful full
SHA pass over one to three raw files, with a catalogue-enforced aggregate ceiling of about
101 MiB. That byte-linear work is isolated behind fresh-process CPU, RSS, wall-time, request,
result, and global-concurrency caps. The child has a transactional, hard-kill-safe effect
contract and may only write the append-only audit output store.

The raw store, lifecycle store, desired-state database, and audit-output database are required
to be filesystem-disjoint. Store-role aliasing is rejected both when constructing the parent
coordinator and when the child reconstructs its signed process authority.

## Explicit non-goals and release blockers

- No service, activation, worker main loop, Home Assistant, or MQTT code imports this module.
- No provider has a meteorological decoder or science/admission path here; this is evidence
  auditing only.
- There is no live consumer-loop supervision, operational health surface, or deployment
  configuration yet.
- Audit-store retention is fail-closed at its configured capacity. A reviewed epoch/retention
  protocol is required before any reclamation.
- Store v2 identity and the ACK fence do not authorize output pruning, source reclamation, or
  live wiring; they only harden the inert append-only authority.
- Raw-source reclamation remains blocked until the wider completion-consumer epoch and durable
  unlink-journal design exists.

Consequently, passing these tests is not permission to activate the consumer or publish its
outputs as meteorological truth.
