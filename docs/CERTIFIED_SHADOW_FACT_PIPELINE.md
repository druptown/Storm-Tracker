# Certified shadow-to-fact pipeline

## Status

`storm_tracker_worker.certified_shadow_fact_pipeline` is a worker-only,
content-addressed audit and staging composition. It is deliberately not wired
into the worker service, Home Assistant, MQTT, the dashboard or the operational
snapshot publication runtime.

The composition closes a local roadmap gap: the repository already contained
the individual raw-recovery, canonical decoder, native-field, analysis-grid,
cell-detection, CellTrack and fact-staging authorities, but no single durable
boundary proved that all of their identities referred to the same work item.
This module supplies that boundary without inventing a production admission
certificate or a rich meteorological fact.

## What is genuinely executed or reverified

For one `ScienceOperationalFactRequest`, the pipeline performs the following
ordered checks:

1. `RecoveredCaptureEvidence` is bound to the exact closed
   `RECOVERY_BINDING_CATALOG` entry and converted to a path-free,
   content-addressed `CertifiedRawRecoveryReference`.
2. The active `DesiredStateStore` head, authority epoch and exact target
   footprint are reloaded and compared with the request.
3. `CanonicalEvidenceStore.load(run_id)` reloads the durable
   `DecoderRunReceipt`. Its capture ID, capture receipt, member count and, for a
   single-role capture, member content/receipt hashes are crossed with the raw
   recovery reference. A structurally incomplete decoder outcome is terminal
   and inert.
4. Real-shadow mode accepts only an exact concrete
   `ScienceShadowIsolatedRunner` (not a structural protocol look-alike or
   subclass) and calls its `execute()` operation. A real runner executes or
   reconciles the existing hard-child composition:

   `canonical evidence -> native radar field -> certified analysis grid ->`
   `lossless detections -> immutable CellTrack transition`.

5. The returned `ScienceShadowProcessResult` must be successful, committed and
   kernel-isolated. Its checksummed receipt metadata is crossed with the
   request, canonical receipt, raw capture and all native/grid/detection/
tracking output hashes. The radar-semantics and coordinate-transform
certificate hashes must be present in the request certificate inventory.
6. The desired-state head is reloaded after science processing.
7. Only then is `ScienceOperationalFactBridge.stage()` called. The bridge
   independently reloads its desired-state and science/test authority and owns
   the existing fact-store transition.
8. A terminal pipeline receipt is committed to a separate FULL-sync bounded
   SQLite store. It contains IDs, reason codes, booleans and hashes only; no raw
   payload, coordinates, display name, provider response, credential or path is
   retained.

An absolute caller deadline is checked before science invocation and again
before fact staging. The existing science runner supplies the pre-emptive hard
child boundary. Expiry, crash, corrupt metadata, authority drift, missing stage
coverage or a changed desired-state produces a durable inert receipt and no
fact-stage call.

The v1 composition accepts exactly an empty lineage-hypothesis sequence. It
materializes and rejects non-empty lineage before request de-duplication, so a
caller cannot reuse an attempt ID with a different tracking input and receive a
stale receipt. Supporting caller-supplied lineage later requires a bounded,
canonical lineage identity in the pipeline request hash first.

## Why real production admission remains false

The current repository does not expose an honest complete production admission
path:

- `ScienceShadowReceipt` v2 fixes `meteorological_admission_granted` to false;
- that receipt also retains the operational blockers and never generates an
  operational snapshot claim;
- `DurableScienceShadowFactAuthority` always returns a withheld authority for
  current science receipts;
- `ScienceOperationalFactBridge` only admits its visibly prefixed,
  content-addressed `synthetic-test:` file authority;
- there is no external meteorological admission-certificate verifier;
- there is no production builder that derives exact target rain, calibrated
  threat, lineage and rich-snapshot claims from the CellTrack result;
- no dashboard projection, publication call or live service ownership is part
  of this module.

Therefore every real receipt, including a receipt with complete verified stage
coverage, has all of these fields fixed to false:

- `meteorological_admission_granted`;
- `created_ready_fact`;
- `operational_snapshot_claim_generated`;
- `dashboard_projection_generated`;
- `publication_authorized`;
- `live_service_wiring`.

The real bridge is still invoked after complete cross-binding, so its actual
withheld decision and blocker set are durable evidence. The pipeline never
turns a hash reference into an admission certificate.

## Synthetic-test behavior

The existing `SyntheticScienceTestFileAuthority` may stage one exact synthetic
fact revision when the desired-state producer, target, provider and region all
carry the `synthetic-test:` prefix. The pipeline normalizes first-stage and
duplicate bridge responses to the same content-addressed terminal identity and
records:

- `status=synthetic_test_result`;
- `synthetic_test_only=true`;
- `synthetic_test_fact_staged=true`.

It still records every live/publication/admission flag as false. This makes the
fact-store hand-off testable without presenting synthetic pixels as
meteorological truth.

## Durability and idempotency

`CertifiedShadowFactPipelineStore` uses:

- an exact schema/algorithm manifest;
- `PRAGMA synchronous=FULL` and rollback-journal transactions;
- a unique attempt ID, immutable request hash and immutable receipt hash;
- canonical receipt bytes plus a separate byte hash and materialized-row hash;
- bounded receipt bytes, row count and database bytes;
- full-row validation on open, load, record and audit.

The local receipt is written after the external fact bridge. If the process
dies between those stores, retry uses the same science attempt and bridge
request. The science authority and fact store return their durable duplicate,
the normalized pipeline result has the same content hash, and the missing local
receipt can be committed safely. Reusing an attempt ID for different immutable
inputs is a hard conflict.

## Tests and remaining external gates

`tests/test_worker_certified_shadow_fact_pipeline.py` covers a real spawned
`ScienceShadowIsolatedRunner` composition E2E, real withheld
primary/repeat/reopen behavior, rejection of a forged structural executor,
same-attempt/non-empty-lineage rejection, explicit synthetic staging,
cross-store retry, missing stage coverage, pre/post deadlines, process crash,
privacy, receipt/database capacity, an uncatchable SQLite crash, exact raw
reference hashing and absence of service/dashboard/publication wiring. The E2E
authority uses the repository's explicitly synthetic upstream test composer;
it proves the physical process/orchestrator/bridge boundary, not live-provider
meteorological admission.

The following remain external or future-product gates and must not be inferred
from these tests:

- a signed and independently verifiable meteorological admission certificate;
- real provider credentials, rights and live target storage;
- labelled/scientifically accepted target-level fact derivation;
- a rich snapshot revision with complete fact-specific admission receipts;
- operational snapshot publication and dashboard projection;
- live service ownership, soak, canary and rollback approval.
