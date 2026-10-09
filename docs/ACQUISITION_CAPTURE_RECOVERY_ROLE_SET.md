# Acquisition capture recovery role-set contract

Status: worker-local development contract; this document does not enable or
deploy a provider.

`CaptureCapacityReconciler` is the only bridge from immutable schema-3 raw
captures to scalar `RawCapacityLedger` commits. It is network-free,
decoder-free and credential-free. A reservation must exist before any bytes;
recovery never creates one after capture.

## Stable extension boundary

Every candidate is declared in `RECOVERY_BINDING_CATALOG` as one immutable
`RawRecoveryBinding`:

- exact handler, workload, ledger provider and raw-store provider;
- exact capture product and capture fetch suffix;
- exact ordered role set;
- per role: raw product, fetch suffix, byte cap, content types and provenance
  contract;
- aggregate byte cap and capture provenance contract.

`RECOVERY_BINDINGS` contains only catalog entries whose activation rule is
currently executable. `validate_closed_recovery_bindings()` requires an exact
one-to-one match. Consequently an unknown handler, an executable handler
without recovery, an extra active binding, role drift, budget drift, suffix
collision or product ambiguity prevents module/runtime construction.

Shared providers and products are supported. Resolution uses the closed tuple
`(raw_provider, product, exact_suffix)`. This is required for GOES-18 and
GOES-19, which share one provider and capture product but have distinct
`:G18:*` and `:G19:*` identities.

## Aggregate digests

Contract: `stv3-worker-raw-role-set-digest-v1`.

`recovery_binding_sha256()` hashes canonical JSON containing the complete
binding, including `capture_suffix` and role-sorted members.

For two or more roles, `recovery_evidence_digest()` creates two role-sorted
canonical bodies:

- `content_role_set`: `binding_sha256` plus, per role, `content_sha256`,
  `raw_product`, `role`, and `size_bytes`;
- `receipt_role_set`: `binding_sha256` plus, per role, `content_sha256`,
  `receipt_id`, `receipt_sha256`, `role`, and `size_bytes`.

Their SHA-256 values occupy the capacity ledger's scalar content and receipt
hash fields. The independent schema-3 capture receipt SHA remains the capture
hash. Mapping iteration order cannot change any digest.

Existing single-role METAR, DWD and KNMI commits deliberately retain their
original scalar content/receipt hashes, allowing durable restart replay without
a ledger migration.

## Safe provider activation sequence

1. Add and validate the dormant exact catalog binding and native provenance
   parity tests.
2. Ensure the child uses `recovery_evidence_digest()` rather than duplicating
   canonical JSON.
3. Ensure the activation rule roles, caps and wall budget exactly match the
   binding and acquisition catalog.
4. Ensure capacity profiles and raw-store entry/member envelopes admit the
   role set.
5. Keep the executor below the rule/job/scheduler deadline and the serialized
   service lane above the executor only by its fixed parent margin.
6. Only then mark the rule executable. Import-time closed validation must pass
   with no fallback.

The current hard timeout hierarchy is: admitted rule/job and scheduler
remaining time, capped at 105 seconds in the isolated executor; the serialized
service acquisition lane is exactly 120 seconds. Neither value grants a
provider more authority than its own rule.

## Evidence tests

- `test_worker_acquisition_capture_recovery.py`: legacy restart, CAS,
  corruption, capacity and diagnostics.
- `test_worker_acquisition_capture_recovery_multirole.py`: role ordering,
  aggregate digest, restart replay, role tamper and cap drift.
- `test_worker_acquisition_capture_recovery_native_multirole.py`: exact native
  provenance/capture parity for DPC, NOAA ERR, GOES-18/19 and Met Office.
- `test_worker_first_service_composition_timeout.py`: rule/executor/service
  timeout hierarchy.

