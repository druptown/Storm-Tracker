# Stable completion-output archive adapters

## Boundary

`storm_tracker_worker.completion_output_archive_adapters` is an inert,
unwired adapter authority with two closed constructors. The compatibility
constructor remains exactly the original four completion-output stores:

| Workload | Store contract | Immutability authority |
| --- | --- | --- |
| `radar.dwd.radolan_rv` | `stv3-dwd-rv-completion-output-store-v3` | immutable SQLite triggers plus checksum reproof |
| `radar.knmi.rtcor` | `stv3-knmi-rtcor-completion-output-store-v2` | immutable output/metadata triggers, persistent instance identity and checksum reproof |
| `radar.metoffice.uk_rate` | `stv3-met-office-rate-completion-output-store-v2` | immutable output/metadata triggers, persistent instance identity and checksum reproof |
| `ground.metar.global` | `stv3-metar-ground-completion-output-store-v2` | immutable output/metadata triggers, persistent instance identity and checksum reproof |

The exact-eleven constructor adds:

| Workload(s) | Physical store | Route authority |
| --- | --- | --- |
| `satellite.noaa.err` | NOAA ERR v2 completion output store | persistent instance plus NOAA region, artifact, process and receipt reproof |
| AEMET, DPC, Météo-France, GOES-18, GOES-19 and HSAF | one shared raw-manifest audit v2 output store | one persistent physical instance with six separate workload, consumer, stream, recovery and exact role-set authorities |

The module is not imported by the service, lifecycle, terminal router, HA or
MQTT code. It does not acknowledge a lifecycle event, fetch raw data, decode
provider data, publish a fact or mutate a provider store. Hot-store pruning,
deletion and reclaim authority remain absent.

## Public API

Create the closed store-bound authority with:

```python
adapters = build_stable_completion_output_archive_adapters(
    dwd_store=dwd_output_store,
    knmi_store=knmi_output_store,
    met_office_store=met_office_output_store,
    metar_store=metar_output_store,
)
```

That call shape and its exact four-profile inventory remain compatible. The
v2 adapter authority deliberately does not pretend that a legacy v1 source
lineage is the same archive lineage. Bind every
current dispatcher workload with:

```python
adapters = build_all_completion_output_archive_adapters(
    dwd_store=dwd_output_store,
    knmi_store=knmi_output_store,
    met_office_store=met_office_output_store,
    metar_store=metar_output_store,
    noaa_store=noaa_output_store,
    raw_manifest_store=shared_raw_manifest_audit_output_store,
)
```

Both return `StableCompletionOutputArchiveAdapters`, which exposes:

- `profiles` and `profile_for(workload_id)` for the exact closed source
  profiles;
- `build_record(workload_id, event_id)` for a read-only planned archive
  record;
- `open_archive_store(...)` for a sealed append-only archive whose source
  catalog and resolver authority are constructor-bound;
- `seal(archive, workload_id, event_id)` for the second source reproof and
  archive commit;
- `load_archived(...)` and `verify_archived_for_ack(...)` for archive-only
  reads. These two operations do not reopen a provider store.

There is no prune, delete or reclaim method.

## Source profiles

Each `CompletionOutputArchiveSourceProfile` binds the real store contract and
schema version, output document/receipt/process-receipt schemas, exact output
limits fingerprint, hard-process effect-contract fingerprint, global
acquisition-catalog fingerprint, per-workload terminal consumer binding,
catalog cadence and source-row reproof contract.

DWD v3 validates immutable output and metadata triggers, its public
limits fingerprint, persistent store-instance fingerprint and native physical
store identity. Its profile uses
`dwd-completion-output-trigger-immutable-plus-checksum-reproof-v3`. KNMI,
Met Office and METAR bind their current v2 contract/schema, validate the exact
output plus metadata trigger inventory and persistent instance sentinel, and
then checksum the row again under
`completion-output-trigger-immutable-plus-checksum-reproof-v2`.

NOAA binds its v2 schema, native identity and persistent instance sentinel,
then revalidates its exact trigger inventory, metadata singleton, limits,
page/row bounds, complete SQLite quick check and native row receipt. Its adapter
additionally binds the desired-authority field, region-authority fingerprint,
decoder run, candidate artifact and hard-process receipt.

The shared raw-manifest v2 database is one native physical source authority but
has six logical profiles and six workload-specific resolvers. Each profile
binds its terminal consumer plus the exact
activation and recovery catalogs, required role ordering, raw provider,
per-role product/content-type/byte limits, aggregate transport limit and
canonical role-set content/receipt digests. A row query includes both event ID
and workload ID, so one route cannot claim another route's event. All six
profiles must be present together in the exact-eleven constructor.

A limits, catalog or consumer-binding change changes the source profile and
therefore the archive catalog authority. A source path change leaves the
portable source profile unchanged but changes the constructor-bound source
store identity and reproof authority.

## Store and path identity

Every physical resolver is bound to a hash of:

- the canonical source path;
- an explicit path-case policy;
- the initial regular-file instance witness (`st_dev`, `st_ino`, `st_mode`);
- exact Python store type;
- store contract and schema version;
- source-limits fingerprint; and
- the source store's own public `identity_sha256`;
- the source store's persistent `store_instance_sha256`; and
- workload ID, except that the six raw-manifest profiles bind the same explicit
  physical-store scope ID.

On Windows, the identity path uses `normpath` followed by `normcase`, matching
the platform's case-insensitive alias behavior. The bound policy identifier is
`windows-normcase-v1`. On case-sensitive platforms the exact canonical case is
retained under `platform-case-sensitive-v1`. Plaintext paths are never placed
in archive records, authority documents, diagnostics or representations.

The initial file witness and native identity/instance hashes are included in
the hashed adapter-store identity. At construction the store's own
`assert_instance_held()` is called twice around the instance read. Before and
after opening the database, the external file witness is checked. Every row
snapshot first calls `assert_instance_held(expected_identity_sha256)` while
holding the real store lock. Under the same `BEGIN IMMEDIATE` transaction used
to read the source row, the store's full native schema verifier returns the
live instance sentinel; both identity and instance must equal their
constructor-bound values exactly. Replacing a source database with
another valid same-schema database at the same pathname therefore fails in the
existing adapter authority. Reconstructing the adapters observes a new witness
and native identity and produces a different reproof authority, which cannot
reopen an existing archive lineage. Silent restore/replacement is intentionally
fail-closed.

Relative, non-normalized and symlinked source paths fail closed. The archive
core additionally rejects the same path, aliases and either direction of a
parent/child relationship between an archive and every source store.

## Two-stage exact reproof

`build_record` obtains the real store's Python lock, opens the store's own
configured SQLite connection, starts `BEGIN IMMEDIATE`, validates the complete
store schema and its live persistent instance, and calls the store's native
row verifier (`_from_row` or `_row`). It then hashes
a canonical typed representation of every physical SQLite column. BLOBs are
represented by their exact byte count and SHA-256 rather than copied into the
source-row proof.

The planned archive record preserves:

- exact output JSON and output SHA-256;
- output-receipt and hard-process-receipt SHA-256;
- event ID and event SHA-256;
- producer-receipt SHA-256;
- desired-state SHA-256 inside the exact output authority;
- workload, consumer and stream route;
- stream sequence and source creation time; and
- canonical physical source-row SHA-256 and byte count.

`seal` reopens the same constructor-bound resolver while the archive
transaction is active. The source `BEGIN IMMEDIATE` transaction and store lock
remain held until the archive commit returns. Event, producer, output,
output-receipt, process-receipt, sequence and complete output bytes are
rechecked. A DWD row that is validly rehashed after planning is therefore still
detected as a different source row and cannot create an archive head.

Exceptions raised by the archive commit after the resolver yields are not
reclassified as source failures. Before-commit failure leaves no head;
after-commit failure is recovered by the sealed archive row.

## Archive-only replay boundary

`load_archived` and `verify_archived_for_ack` validate the archive's bound
catalog and resolver-authority fingerprints and then use only
`CompletionOutputArchiveStore.load/verify_for_ack`. Provider output databases
may be unavailable at this point. This is a replay/reference read boundary,
not authorization to delete its hot source row.

## Verification

`tests/test_worker_completion_output_archive_adapters.py` keeps the original
four-store compatibility matrix. In addition,
`tests/test_worker_completion_output_archive_all_adapters.py` commits real
small rows through NOAA and all six routes in one raw-manifest store. Together
they cover:

- exact profile, schema, limits, effect, catalog and consumer bindings;
- all four native receipt shapes and exact archive load/ack verification;
- archive-only reads with every provider connection forced unavailable;
- duplicate/idempotent sealing and same-event conflicts;
- individually forged event, producer, output, output-receipt and
  process-receipt bindings;
- a blocked direct-SQL DWD update, followed by an explicit test-only trigger
  drop, valid row rehash, exact trigger restoration and checksum mismatch
  between build and seal, with zero resulting archive heads;
- immutable output/metadata trigger enforcement, exact v2 schema/profile
  binding and direct sentinel tamper for KNMI, NOAA, raw audit, Met Office and
  METAR;
- same, aliased and parent/child source/archive paths;
- path, limits, acquisition-catalog, consumer and resolver-authority drift;
- valid same-schema database replacement at the same source path, both in an
  existing authority and after reconstructing adapters against an archive;
- commit faults, two-store-instance concurrent CAS and archive tamper; and
- byte-identical provider rows before and after sealing, proving that this
  layer performs no provider update, delete or prune.
- all eleven exact terminal routes and six-distinct/one-physical raw authority,
  including one shared native identity and instance hash;
- NOAA desired, region, decoder artifact/run and process projections;
- raw role ordering, provider/product/content-type limits and recomputed
  role-set hashes;
- byte-exact physical SQLite projections for event, producer, output,
  outputreceipt, processreceipt, route, desired, capture and isolation fields;
- cross-route event poisoning, source metadata/trigger tamper and same-path
  valid-store replacement;
- two-store-instance CAS and archive crash boundaries for NOAA/raw routes; and
- archive-only replay with source connections unavailable.

The archive privacy scanner admits only the two exact boolean audit assertions
required by the immutable raw-manifest output: `raw_bytes_verified=True` and
`raw_bytes_copied=False`. Opposite values, strings, nested values, aliases and
every other raw-bytes/raw-payload field remain rejected.

This adapter layer does not claim integrated retention, replay scheduling or
hot-window reclamation.
