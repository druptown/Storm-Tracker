# Terminal raw → completion router

Status: **implemented, test-only and deliberately unwired**.

Contract: `stv3-worker-terminal-raw-completion-router-v1`.

## Boundary

`storm_tracker_worker.terminal_raw_completion_router` starts only after all of
these durable authorities agree:

1. the scheduler slot is terminal `completed`;
2. the execution head is the sole `completed_shadow_raw` receipt for the
   reconstructed acquisition job;
3. the immutable desired-state version reconstructs the same plan, slot and
   job hashes;
4. the workload is exactly `ACTIVE_SHADOW_RAW`;
5. the activation rule has one closed recovery binding and one explicit
   consumer binding;
6. the schema-3 raw capture manifest and every role receipt verify against the
   raw object bytes;
7. the role-set aggregate hashes agree with the committed capacity receipt and
   with all four terminal execution hashes.

The router then publishes at most one missing completion and advances only an
in-memory scan cursor. After a process restart it scans again from cursor zero;
`AcquisitionEvidenceLifecycleStore.contains_acquisition_job()` and the
lifecycle store's unique durable identities make replay idempotent. A crash
after producer/outbox commit therefore recovers without network access.

The module is not imported by `service.py`, `run.py`, Home Assistant code, the
snapshot runtime or the science orchestrator.

## Payload modes

All non-DWD workloads publish
`stv3-worker-terminal-raw-capture-reference-v1`: a canonical, content-addressed
JSON object capped at 64 KiB. It contains only capture/manifest/receipt/content
hashes, role names, provider/product identifiers, media types and sizes. It
contains no raw provider bytes, filesystem path, upstream identity,
provenance, URL or credential capability. The referenced raw store is verified
with a full SHA pass before publication; no network client exists in the
router.

DWD is the one explicit compatibility exception. The existing deployed
consumer `dwd-rv-quantitative-decoder-v1` expects the exact tar in the
lifecycle payload. The router therefore delegates DWD to the already tested
`publish_dwd_completion()` handoff, preserving:

- consumer ID `dwd-rv-quantitative-decoder-v1`;
- stream ID `radar.dwd.radolan_rv`;
- proof schema `stv3-worker-dwd-rv-completion-proof-v1`;
- exact byte payload and its current crash-replay behavior.

It is not valid to silently change DWD to a manifest reference until that
consumer has a separately versioned reference-reading contract and migration.

## Closed consumer catalogue

The hand-written catalogue must equal the complete current
`ACTIVE_SHADOW_RAW` set. Import/construction fails closed on a missing, extra,
duplicate or drifted consumer. A consumer marked `decoder_reference` receives
only an input reference; that label does **not** claim that decoding ran.

| Workload | Consumer | Kind | Transfer |
|---|---|---|---|
| `ground.metar.global` | `metar-canonical-ground-decoder-reference-v1` | decoder reference | manifest reference |
| `radar.aemet.composite` | `aemet-raw-manifest-audit-v1` | raw-manifest audit | manifest reference |
| `radar.dpc.sri` | `dpc-sri-raw-manifest-audit-v1` | raw-manifest audit | manifest reference |
| `radar.dwd.radolan_rv` | `dwd-rv-quantitative-decoder-v1` | deployed decoder | legacy exact raw payload |
| `radar.knmi.rtcor` | `knmi-rtcor-quantitative-decoder-reference-v1` | decoder reference | manifest reference |
| `radar.meteofrance.dpradar` | `meteofrance-dpradar-raw-manifest-audit-v1` | raw-manifest audit | manifest reference |
| `radar.metoffice.uk_rate` | `metoffice-rate-quantitative-decoder-reference-v1` | decoder reference | manifest reference |
| `satellite.goes18.rrqpe` | `goes18-rrqpe-raw-manifest-audit-v1` | raw-manifest audit | manifest reference |
| `satellite.goes19.rrqpe` | `goes19-rrqpe-raw-manifest-audit-v1` | raw-manifest audit | manifest reference |
| `satellite.hsaf.h40b` | `hsaf-h40b-raw-manifest-audit-v1` | raw-manifest audit | manifest reference |
| `satellite.noaa.err` | `noaa-err-quantitative-decoder-reference-v1` | decoder reference | manifest reference |

## Public API

```python
router = TerminalRawCompletionRouter(
    lifecycle_store=lifecycle,
    desired_state_store=desired,
    scheduler=scheduler,
    execution_store=executions,
    capacity_ledger=capacity,
    source_raw_store=raw,
    dwd_handoff_raw_store=dwd_handoff,  # required only when DWD is encountered
)

result = router.publish_next()
diagnostics = router.diagnostics()
```

`publish_next()` returns one of:

- `published`: one exact completion/reference became durable;
- `idle`: the bounded page contained nothing missing;
- `busy`: another call on the same router instance owns the non-blocking lane.

`validate_terminal_raw_consumer_catalog()` is the reviewed extension gate.
`TerminalRawCompletionIntegrityError` and
`TerminalRawCompletionCapacityError` are fail-closed terminals; neither causes
fallback, refetch, truncation or eviction.

## Claims explicitly withheld

Every reference payload and proof records all of these as false:

- decoded;
- meteorological admission;
- operational fact staged;
- operational snapshot published;
- raw bytes copied;
- raw refetch performed.

The router authorizes neither source reclamation nor lifecycle admission.

## Remaining blockers before composition

1. Non-DWD decoder/audit loops still need their own hard process limits,
   durable output receipts, desired-state verification and lifecycle
   acknowledgement. The catalogue is their closed input contract, not live
   wiring.
2. Reference completions depend on the source raw object remaining available.
   Reclamation must not unlink it until the consumer/epoch registry and a
   crash-durable raw-unlink journal prove every required reference terminal.
3. Each quantitative decoder still needs its provider-specific semantic/grid
   certificate and meteorological admission gates. A valid raw reference is
   not a rain-rate or observation claim.
4. No service composition, HA/NAS deployment, MQTT publication or NUC soak has
   been performed by this change.
5. DWD still incurs its legacy handoff byte copy. Removing it requires a
   separately versioned consumer migration, not a router-side optimization.

## Test evidence

`tests/test_worker_terminal_raw_completion_router.py` covers:

- exact catalogue closure for all active raw workloads;
- KNMI, NOAA ERR and Met Office reference publication;
- complete sorted three-role evidence;
- deployed DWD consumer/stream/payload compatibility;
- crash after lifecycle commit and restart replay;
- raw-object tamper;
- refusal of a capture assembled from another acquisition job's role receipts;
- reference byte cap;
- path/capability/provenance privacy;
- two concurrent router instances converging on one content-addressed row;
- missing-consumer refusal and live-wiring absence.

The existing real DWD lifecycle and completion-consumer regression tests remain
the byte-level compatibility authority.
