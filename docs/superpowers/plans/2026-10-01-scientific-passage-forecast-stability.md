# Scientific Passage Forecast Stability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and deploy a scientifically honest, lineage-stable target-passage forecast that advects quantity-correct radar pixels, preserves uncertainty and multiple passages, and can be verified later without future-data leakage.

**Architecture:** Extend the existing `ThinRadarField` and CellTrack/WeatherSystem path rather than adding a tracker. A bounded target-passage registry consumes admitted WeatherSystem motion plus labelled QPE pixels, emits immutable forecast issues and passage intervals, and publishes one versioned contract through PostGIS, MQTT, the strict operational snapshot and the dashboard. Replay and verification remain offline and never join worker startup.

**Tech Stack:** Python 3, NumPy, PyProj, PostgreSQL/PostGIS, MQTT, Home Assistant custom integration, JavaScript dashboard, pytest

**Spec:** `docs/superpowers/specs/2026-10-01-scientific-passage-forecast-stability-design.md`

## Global Constraints

- Only strictly increasing accepted radar valid-times count as temporal evidence; a same-time correction never increments a support or counterevidence counter.
- Reuse the existing CellTrackEngine and WeatherSystem grouping as the only tracking and grouping authorities; do not add a third tracker or global assignment engine.
- The operational baseline is rigid advection of labelled QPE pixels with one admitted WeatherSystem consensus vector; growth/decay and spatial-flow remain unpromoted challengers.
- A candidate policy may use two support scans, two complete corridor counterevidence scans, and a bounded six-field/thirty-minute ring, but every live result must remain labelled `candidate_policy_v1` and `provisional_unvalidated`.
- DBZH and non-QPE products may support echo and motion but must never be published as local `mm/h` without an explicitly validated conversion.
- A two-hour horizon is a computation limit, not a proven skill horizon; horizon exits remain right-censored.
- PostGIS failure must not block current MQTT publication; replay, recovery, verification and calibration must not block worker startup.
- Preserve existing provider acquisition, immediate startup, retention and radar display behavior; add no RAW, manifest, authority, ledger or startup audit.
- Do not add NUC-HA notifications in this change. Deploy only to NAS-HA `192.168.0.15` and follow `docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md`.
- The repository contains unrelated user changes. Never reset or clean them; stage and commit only paths named by the active task.

## Review Focus

1. A changed content fingerprint at the same valid-time must update provenance without becoming another scan — pinned by `test_same_valid_time_revision_never_advances_evidence` in Task 1.
2. A DBZH echo over a target must retain echo/motion utility while local intensity stays unknown and current ground rain is not asserted — pinned by `test_dbzh_echo_cannot_publish_local_rain_rate` in Task 3.
3. A new unrelated motion-ready WeatherSystem that misses the target must not cancel or clear an existing passage — pinned by `test_unrelated_system_cannot_cancel_existing_passage` in Task 4.
4. Split/merge overlap and a dry gap must preserve ambiguous lineage and separate chronological passage intervals — pinned by `test_split_merge_and_reentry_remain_distinct` in Task 4.
5. Restart or PostGIS outage must publish `recovering/indeterminate`, keep MQTT live, and never reuse a future-arriving field in an old issue — pinned by `test_restart_database_outage_and_as_of_replay_fail_closed` in Tasks 5 and 7.

---

### Task 1: Typed radar-field identity and unique temporal evidence

**Files:**
- Modify: `storm_tracker_worker/thin_provider_adapters.py`
- Modify: `storm_tracker_worker/thin_regional_radar_fusion.py`
- Modify: `storm_tracker_worker/thin_radar_tracking.py`
- Test: `tests/test_thin_provider_adapters.py`
- Test: `tests/test_thin_regional_radar_fusion.py`
- Test: `tests/test_thin_radar_tracking.py`

**Interfaces:**
- Consumes: existing decoded `ThinRadarField` arrays and masks.
- Produces: `RadarFieldIdentity`, `ThinRadarField.is_quantitative_precipitation`, `ThinRadarField.motion_compatibility_key`, and `ThinRadarTracker.advance(field)` semantics that accept only a strictly later compatible valid-time as new evidence.

- [ ] **Step 1: Write failing contract tests**

Add literal fixtures for an `mm/h` QPE field and an OPERA `DBZH` field. Add `test_same_valid_time_revision_never_advances_evidence`, asserting that a corrected fingerprint at the same valid-time cannot create a second track history point or advance evidence count. Add a fusion round-trip assertion that quantity, unit, quality semantics, processing version, source composition, valid/received/available times, grid ID and content fingerprint survive unchanged.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_provider_adapters.py tests/test_thin_regional_radar_fusion.py tests/test_thin_radar_tracking.py`

Expected: FAIL because the typed identity/provenance fields and compatibility behavior do not exist.

- [ ] **Step 3: Implement the typed field contract**

Add frozen `RadarFieldIdentity(quantity, unit, quality_kind, processing_version, source_composition, canonical_valid_at, received_at, available_at, grid_id, content_fingerprint)` and carry it on `ThinRadarField`. Derive `grid_id` from CRS/origin/resolution/shape when a decoder does not supply one and use canonical byte/metadata hashing for `content_fingerprint`. Same-time corrections may replace the retained revision/provenance but must return `advanced=False`; incompatible quantity/unit/grid/processing/composition pairs must not silently feed motion.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_provider_adapters.py tests/test_thin_regional_radar_fusion.py tests/test_thin_radar_tracking.py`

Expected: PASS.

- [ ] **Step 5: Commit only Task 1 paths**

Commit message: `feat: type radar field evidence`

### Task 2: WeatherSystem motion admission and auditable lineage

**Files:**
- Create: `storm_tracker_worker/thin_system_motion.py`
- Modify: `storm_tracker_worker/thin_radar_tracking.py`
- Modify: `storm_tracker_worker/tracking_core/engine/cell_track_engine.py`
- Test: `tests/test_thin_radar_tracking.py`
- Test: `tests/test_cell_track_engine_shadow.py`

**Interfaces:**
- Consumes: Task 1 compatible field transitions and existing member-track motion evidence.
- Produces: `SystemMotionAdmission` and `admit_weather_system_motion(system, member_consensus, policy) -> SystemMotionAdmission`; `RadarTrack` gains source field identity, current footprint pixels and auditable lineage fields.

- [ ] **Step 1: Write failing admission tests**

Cover initial `(0, 0)` recursion, correlated optical-flow/object evidence, residual vectors `[0, 0, D]`, coherent member vectors, incoherent member vectors, split, merge and unresolved identity. Assert non-null descriptive motion alone never yields `motion_ready=True`.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_radar_tracking.py tests/test_cell_track_engine_shadow.py`

Expected: FAIL on the present descriptive-motion shortcut and disabled coherence gate.

- [ ] **Step 3: Implement one fail-closed system admission path**

Use existing `MotionConsensusResult` for current membertracks. Add versioned `candidate_policy_v1` WeatherSystem coherence parameters, replace the practical `1_000_000 m/s` difference limit with the policy-bound comparison, and retain raw candidates/reasons. Enable existing bounded identity-alternative evidence only within current engine limits; otherwise mark lineage `ambiguous` rather than guessing.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_radar_tracking.py tests/test_cell_track_engine_shadow.py`

Expected: PASS.

- [ ] **Step 5: Commit only Task 2 paths**

Commit message: `fix: gate weather system motion`

### Task 3: Labelled QPE advection and interval-aware passage candidates

**Files:**
- Create: `storm_tracker_worker/thin_passage_forecast.py`
- Modify: `storm_tracker_worker/thin_footprint_forecast.py`
- Modify: `storm_tracker_worker/thin_nowcast.py`
- Test: `tests/test_thin_passage_forecast.py`
- Test: `tests/test_thin_footprint_forecast.py`
- Test: `tests/test_thin_nowcast.py`

**Interfaces:**
- Consumes: admitted `RadarTrack` plus its Task 1 field identity, full QPE array, validity mask and current system-label pixels.
- Produces: `PassageBracket`, `PassageScenario`, `PassageInterval`, and `advect_labelled_qpe(field, labelled_pixels, motion, target, policy) -> tuple[PassageInterval, ...]`.

- [ ] **Step 1: Write failing scientific-behavior tests**

Cover straight rigid motion with hand-derived entry/exit brackets, target already wet (left-censored), exit beyond horizon (right-censored), two wet intervals separated by a dry gap, miss scenarios retained in denominator, nodata/outside-grid, and `test_dbzh_echo_cannot_publish_local_rain_rate`. Assert local mean/peak come from the advected target pixels, not system maximum or mean.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_passage_forecast.py tests/test_thin_footprint_forecast.py tests/test_thin_nowcast.py`

Expected: FAIL because current rigid polygon forecasting collapses intervals and lacks quantity gating/censoring.

- [ ] **Step 3: Implement the QPE baseline**

Advect the newest labelled quantitative raster values rigidly at deterministic forecast steps using the admitted vector. Sample target support with position accuracy, grid resolution, valid mask and coverage. Return maximal contiguous wet intervals with entry/exit brackets, censoring, scenario hit/miss counts, exact field provenance, and `advected_radar_estimated_rain_rate`; keep the existing centerline path as an explicitly labelled last fallback without precise duration or intensity.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_passage_forecast.py tests/test_thin_footprint_forecast.py tests/test_thin_nowcast.py`

Expected: PASS.

- [ ] **Step 5: Commit only Task 3 paths**

Commit message: `feat: advect labelled qpe passages`

### Task 4: Target-bound passage continuity and evidence state machine

**Files:**
- Create: `storm_tracker_worker/thin_passage_state.py`
- Modify: `storm_tracker_worker/thin_nowcast.py`
- Modify: `storm_tracker_worker/thin_operational_runtime.py`
- Test: `tests/test_thin_passage_state.py`
- Test: `tests/test_thin_nowcast.py`
- Test: `tests/test_thin_operational_runtime.py`

**Interfaces:**
- Consumes: Task 3 passage intervals and Task 2 lineage events for each accepted unique field.
- Produces: `PassageStateRegistry.advance(evidence: PassageEvidence) -> PassageStateSnapshot` with statuses `candidate`, `confirmed`, `coasting`, `current`, `ended`, `cancelled`, `indeterminate` and immutable `passage_system_id` aliases.

- [ ] **Step 1: Write failing registry tests**

Exercise two unique supports to confirm, same-time repeat ignored, stale/dropout/nodata/coast, two complete lineage-corridor counter-scans to cancel, local ground observation changing only current state, restart recovery, and bounded coasting. Include `test_unrelated_system_cannot_cancel_existing_passage` and `test_split_merge_and_reentry_remain_distinct`.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_passage_state.py tests/test_thin_nowcast.py tests/test_thin_operational_runtime.py`

Expected: FAIL because held arrival is presently cleared by unrelated new motion and there is no lineage-aware registry.

- [ ] **Step 3: Implement the bounded registry**

Key state by target and deterministic passage alias, not target alone. Increment radar counters only for strictly later accepted valid-times. Define counterevidence as fresh, quality-suitable coverage of both the same lineage footprint and every still-admitted route to the target; absence at the target alone never cancels. Preserve multiple systems and re-entry intervals chronologically; on restart begin at `recovering/indeterminate`.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_passage_state.py tests/test_thin_nowcast.py tests/test_thin_operational_runtime.py`

Expected: PASS.

- [ ] **Step 5: Commit only Task 4 paths**

Commit message: `feat: preserve target passage continuity`

### Task 5: Immutable PostGIS issue and passage storage

**Files:**
- Create: `storm_tracker_worker/postgis_migrations/0008_scientific_passage_forecast.sql`
- Modify: `storm_tracker_worker/thin_postgis.py`
- Test: `tests/test_thin_postgis.py`

**Interfaces:**
- Consumes: Task 4 state snapshots and Task 3 interval/scenario provenance.
- Produces: deterministic `forecast_issue_id`, deterministic `passage_id`, append-only issue metadata, and geometrically complete passage rows only when entry/exit representation is usable.

- [ ] **Step 1: Write failing migration and outage tests**

Assert two algorithm versions at one issue-time remain distinct, two re-entry intervals retain separate IDs, retry is idempotent, a provisional arrival stores no fabricated `exit_at`, JSON provenance is size-bounded, and the PostGIS exception path still returns a publishable MQTT result. Include the database-outage half of `test_restart_database_outage_and_as_of_replay_fail_closed`.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_postgis.py`

Expected: FAIL because existing target/time and target/time/cell upsert identities overwrite scientific issues and intervals.

- [ ] **Step 3: Implement an additive idempotent migration**

Keep existing result tables for compatibility. Add immutable forecast-issue identity and passage identity without destructive data migration, use conflict-safe inserts keyed by those identities, and store provisional/censoring/lineage/scenario metadata in the versioned forecast metadata contract. Never require PostGIS availability before current publication.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_postgis.py`

Expected: PASS, including applying migration 0008 twice.

- [ ] **Step 5: Commit only Task 5 paths**

Commit message: `feat: store immutable passage forecasts`

### Task 6: Coordinated MQTT, snapshot, integration and dashboard contract

**Files:**
- Modify: `storm_tracker_worker/thin_mqtt_service.py`
- Modify: `storm_tracker_worker/operational_snapshot_builder.py`
- Modify: `custom_components/storm_tracker_v3/worker_bridge.py`
- Modify: `custom_components/storm_tracker_v3/operational_snapshot.py`
- Modify: `dashboard/stv3-forecast-card.js`
- Test: `tests/test_thin_mqtt_service.py`
- Test: `tests/test_worker_operational_snapshot_builder.py`
- Test: `tests/test_worker_bridge.py`
- Test: `tests/test_operational_snapshot.py`
- Test: `tests/test_forecast_dashboard_card.py`

**Interfaces:**
- Consumes: Task 4 states and Task 5 immutable IDs/metadata.
- Produces: passages payload schema version 2; integration accepts schema 1 during rolling deployment but emits the strict version-2 snapshot; dashboard displays status, estimates/brackets, censoring and method without inventing missing values.

- [ ] **Step 1: Write failing end-to-end contract tests**

Use one literal confirmed passage, one candidate with no exit, one right-censored passage and one indeterminate passage. Assert identical enum names, units, brackets, IDs and null semantics at MQTT, bridge, exact-key snapshot and rendered card boundaries.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_mqtt_service.py tests/test_worker_operational_snapshot_builder.py tests/test_worker_bridge.py tests/test_operational_snapshot.py tests/test_forecast_dashboard_card.py`

Expected: FAIL against schema version 1 and the old four-state/mandatory-exit presentation.

- [ ] **Step 3: Implement contract version 2 atomically**

Publish all Task 4 statuses plus policy/validation labels, paired scenario outcomes, entry/exit brackets, left/right censoring, immutable IDs, quantity/unit and provenance. Keep a narrow schema-1 reader only for rollout compatibility. Render unknown exit/duration as unknown and show estimates distinctly from brackets.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_mqtt_service.py tests/test_worker_operational_snapshot_builder.py tests/test_worker_bridge.py tests/test_operational_snapshot.py tests/test_forecast_dashboard_card.py`

Expected: PASS.

- [ ] **Step 5: Commit only Task 6 paths**

Commit message: `feat: publish passage forecast contract v2`

### Task 7: Leakage-safe replay and passage verification

**Files:**
- Create: `storm_tracker_worker/thin_passage_verification.py`
- Modify: `storm_tracker_worker/thin_replay.py`
- Modify: `storm_tracker_worker/tracking_core/engine/forecast_verification.py`
- Test: `tests/test_thin_passage_verification.py`
- Test: `tests/test_thin_replay.py`
- Test: `tests/test_forecast_verification.py`

**Interfaces:**
- Consumes: Task 1 availability/fingerprint provenance and Task 5 immutable issues.
- Produces: `build_temporal_radar_reference(issue, later_fields)`, bracket-aware passage scores, and an external-truth verifier that rejects Storm Tracker fused radar as independent truth.

- [ ] **Step 1: Write failing as-of and scoring tests**

Reject any input whose `available_at` or `received_at` exceeds `issued_at`; choose the exact available same-time revision; use only later complete valid-times for temporal reference; retain misses/censoring in denominators; calculate literal bracket intersection/containment and duration error; reject internal fused radar at the external-truth boundary. Add deterministic synthetic event fixtures for straight motion, acceleration, deformation, growth/decay, dropout, nodata, split, merge, two passages, re-entry, horizon censoring and a true miss; assert unsupported acceleration/deformation/growth/decay degrades or becomes `indeterminate` rather than appearing exact. Include the restart/as-of half of `test_restart_database_outage_and_as_of_replay_fail_closed`.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_passage_verification.py tests/test_thin_replay.py tests/test_forecast_verification.py`

Expected: FAIL because current replay lacks issue-time revision admission and passage-bracket verification.

- [ ] **Step 3: Implement offline-only verification**

Keep temporal radar reference separate from external ground truth, preserve exact provider/grid/quantity/threshold/mask provenance, and aggregate scores by event rather than frame. Add no startup hook, background audit or automatic provider reweighting.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_passage_verification.py tests/test_thin_replay.py tests/test_forecast_verification.py`

Expected: PASS.

- [ ] **Step 5: Commit only Task 7 paths**

Commit message: `feat: verify passage forecasts without leakage`

### Task 8: Release, NAS-HA deployment and live proof

**Files:**
- Modify: `storm_tracker_worker/__init__.py`
- Modify: `storm_tracker_worker/config.yaml`
- Modify: `custom_components/storm_tracker_v3/manifest.json`
- Create: `docs/DEPLOY_2026-10-01_<integration>_<worker>.md`
- Modify only if contract evidence requires it: `docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md`

**Interfaces:**
- Consumes: all Tasks 1–7 and the fixed NAS deployment procedure.
- Produces: one version-matched integration/worker release installed only on NAS-HA, with rollback evidence and a live passage-contract proof.

- [ ] **Step 1: Run all relevant contract suites**

Run the union of Tasks 1–7 test files, then `PYTHONPATH=. ./.venv/bin/pytest -q`.

Expected: all tests PASS; no new startup audit or NUC path appears.

- [ ] **Step 2: Bump the paired release and build candidates**

Bump integration `1.14.153` to `1.14.154` and worker `1.18.88` to `1.18.89`, unless live preflight proves a newer repository version; in that case increment each current version once and record the ruling. Build with `scripts/build_release_candidate.py`, verify archive roots, embedded versions and SHA-256 hashes.

- [ ] **Step 3: Perform NAS-HA preflight and backup**

Read live integration/worker versions, health, free space and current passage topic; verify host `192.168.0.15`, user `hassio`, expected SSH host fingerprint and a recent backup, then create a dedicated deploy backup under `/backup/storm_tracker_deploy_runs`.

- [ ] **Step 4: Install in the fixed safe lifecycle**

Disable only the Storm Tracker integration and stop only `local_storm_tracker_worker`; run `scripts/install_ha_release_pair_ssh.py --evidence <file> --confirm-components-stopped --apply`; reload the HA App Store, update the worker App, validate HA configuration, start the worker first, require a fresh heartbeat plus `job_admission_open=true`, then enable the integration.

- [ ] **Step 5: Verify live behavior and rollback on failure**

Verify exact deployed versions/hashes, worker freshness, provider independence, MQTT schema 2, snapshot schema, PostGIS migration 0008, dashboard rendering, immediate startup, and one controlled live/replay passage showing honest status/brackets/censoring. On any contract failure restore the dedicated backup and record the evidence instead of leaving a mixed pair running.

- [ ] **Step 6: Commit the exact release metadata and deployment report**

Commit message: `release: deploy scientific passage forecast`
