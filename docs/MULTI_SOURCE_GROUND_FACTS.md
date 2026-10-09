# Multi-source ground facts

`storm_tracker_worker.multi_source_ground_facts` is the pure ground-input
boundary for the closed multi-source operational cycle. It covers all four
ground cadence workloads in the 28-source catalog. A caller supplies already
committed canonical ground receipts and target scopes; the module performs no
network, filesystem, database, secret, scheduler, admission, alert, or
production action.

The result is intentionally honest. Every ground workload and every requested
target has exactly one of `current`, `stale`, `error`, `blocked`, or
`not_applicable`. A supported decoder that produced no usable target fact is an
explicit error. A workload without a canonical decoder is explicitly blocked.
No source disappears from the slice.

## Closed source and decoder matrix

| Workload | Existing authority used by this slice | Slice maturity |
| --- | --- | --- |
| `ground.kmi.aws_10min` | Exact KMI WFS raw capture exists; there is no canonical ground-fact decoder | Applicable Belgian targets are `blocked`; targets outside catalog scope are `not_applicable` |
| `ground.metar.global` | `metar_ground_evidence` canonical `GroundObservationReceipt` | Receipt-derived target candidates are allowed |
| `ground.netatmo.region` | `netatmo_ground_decoder` canonical `GroundObservationReceipt` | Receipt-derived target candidates are allowed; the native unit/field semantics remain explicit and unverified |
| `ground.synoptic.region` | `synoptic_ground_decoder` canonical `GroundObservationReceipt` | Receipt-derived target candidates are allowed; interval and accumulated semantics remain distinct |

“Allowed” does not mean operationally admitted. It only means the existing
decoder identity, version, provider, product, receipt, fact, and capture hashes
can be re-proved by this contract. Runtime acquisition, credentials, raw
retention authority, and decoder invocation remain separate concerns.

## Inputs

`build_all_ground_slice` requires:

- one desired-state SHA-256 and positive authority epoch;
- one evaluation timestamp;
- between 1 and 64 immutable `GroundTargetScope` values;
- an exact mapping containing all four ground source IDs, including empty
  sequences for sources without receipts;
- optionally, a reason code for a supported decoder that failed before
  producing data.

Each target scope binds a canonical `CycleTarget` and a station radius. Catalog
applicability is evaluated first with the cycle's country-first, bbox-fallback
rule. Only then are canonical stations matched by haversine distance. An
unavailable target, country mismatch, or bbox mismatch remains explicitly
`not_applicable`; it is never converted into a provider failure.

## Canonical receipt proof

METAR, Netatmo, and Synoptic inputs must be actual
`GroundObservationReceipt` objects. Every receipt is reparsed and rehashed.
The slice verifies:

- canonical fact ID, observation key, fact SHA-256, and receipt SHA-256;
- capture ID, capture manifest, capture receipt, source member, and source
  record hashes;
- the exact source-specific provider, product, decoder identity, and decoder
  version;
- decoder configuration SHA-256 and receipt commit ordering;
- a single capture/config authority per source batch;
- unique fact IDs and canonical time/fact ordering.

The workload carries sorted `GroundReceiptReference` values plus a receipt-set
hash. Every target candidate must resolve back to one of those references with
the same fact, receipt, capture, decoder, provider, product, and observation
time authority. Candidate, target, workload, workload-receipt, target-set, and
slice hashes form the content-addressed envelope.

## Candidate meaning

A candidate preserves the canonical value without meteorological
reinterpretation:

- variable, value state, representation, exact value, unit, and censor limit;
- station identity, exact decimal coordinates, and target distance;
- observation interval, receive, ingest, commit, and freshness timestamps;
- provider QC flags and a hash of the complete preserved provider-QC rows;
- source-specific semantics profile;
- all capture, member, record, fact, and receipt identities.

`observed`, `trace`, `censored_below`, and `censored_above` are usable canonical
value states. Missing or unknown values, unknown station positions, stations
outside the target radius, and observations beyond the allowed future skew are
not silently discarded: every target contribution records bounded exclusion
counts. For each applicable decoded target, candidates plus exclusions exactly
cover its input receipt count.

Freshness is based only on canonical `observed_at`. METAR uses the existing
`MAX_STALE_SECONDS` authority. Netatmo and Synoptic currently use an explicit
ten-minute slice policy. Stale candidates remain available as evidence but can
never make a target or workload current.

## Status selection

Target status is deterministic:

1. Outside catalog scope: `not_applicable`.
2. Applicable source without canonical decoder: `blocked`.
3. Explicit supported-decoder failure: `error`.
4. At least one matching current candidate: `current`.
5. Matching candidates exist but all are stale: `stale`.
6. Otherwise: `error`, with either
   `decoder_returned_no_ground_facts` or
   `no_usable_ground_fact_for_target`.

The cycle adapter emits exactly four `SourceContribution` envelopes. Its
workload status uses `current`, then `stale`, then `error`, then `blocked`, then
`not_applicable` precedence across targets. A data status carries a `GroundInput`
whose evidence reference points to the complete workload hash and its
workload-receipt hash. Non-data statuses carry the cycle's exact empty ground
payload.

## No cross-provider consensus

METAR accumulations, Netatmo native fields, and Synoptic interval or accumulated
fields are not assumed interchangeable. The slice therefore fixes
`consensus.status` to `withheld`, fixes the reason to
`cross_provider_semantics_certificate_unavailable`, and requires an empty
consensus candidate list. Each candidate also has
`consensus_eligible = false`.

Consensus can only be added by a later, separately reviewed contract that
binds compatible quantity, unit conversion, representative interval, sensor
meaning, QC treatment, and freshness authority. It cannot be inferred from
similar variable names.

## Bounds and fail-closed behavior

| Bound | Limit |
| --- | ---: |
| Targets | 64 |
| Canonical receipts per source | 2,048 |
| Canonical receipts per slice | 4,096 |
| Candidates per source/target | 512 |
| Target candidates per slice | 8,192 |
| Provider-QC flags per candidate | 64 |
| Target radius | 250 km |
| Future observation skew | 5 minutes |
| Canonical slice document | 16 MiB |

Target iterables and receipt sequences are bounded before full materialization.
Unknown sources, omitted sources, duplicate facts, mixed decode batches,
error-plus-data contradictions, raw-only canonical receipts, non-canonical JSON,
duplicate JSON keys, count omissions, and nested hash tampering fail closed.

## Integration boundary

This module is deliberately not wired to a live loop. A later integration may:

1. obtain committed receipts from the existing provider-specific decoder
   paths;
2. construct the complete four-entry receipt mapping and exact target scopes;
3. build and persist the canonical ground slice;
4. adapt its four workloads into the full operational cycle.

That integration must not change a blocked source into data merely because raw
bytes exist. KMI AWS requires a real canonical ground decoder and semantic
certification first. Likewise, this slice is evidence for downstream decision
logic; it does not itself generate forecasts, warnings, admissions, or alerts.
