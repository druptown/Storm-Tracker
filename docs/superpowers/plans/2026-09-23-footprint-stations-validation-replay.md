# Footprint, Station Catalog, Ground Validation, and Replay Plan

> **Execution:** Inline in the existing Storm Tracker workspace. Use TDD for
> every production change. Do not import legacy RAW/audit/receipt modules.

**Goal:** Add polygon-footprint arrival/exit prediction, a persistent offline
Netatmo/METAR/Wunderground station catalog, station-aware ground polling,
forecast validation, and an offline PostGIS replay/calibration command to the
live thin operational pipeline.

**Architecture:** Pure meteorological modules operate on thin-domain objects.
PostGIS is the only durable weather and station store. A resumable bootstrap
populates station metadata outside worker startup. The live runtime performs
only indexed station selection and bounded polling. Replay invokes the same
pure footprint and validation functions as live processing.

**Spec:**
`docs/superpowers/specs/2026-09-23-footprint-ground-validation-replay-design.md`

## Global constraints

- Deploy only to NAS-HA `192.168.0.15`; never touch NUC `192.168.0.144`.
- Follow `docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md` for the final deployment.
- Do not run station discovery during worker startup or block heartbeat/MQTT.
- Use only official provider interfaces; never scrape Wunderground pages.
- Netatmo/Wunderground API budgets are separate from live observation polling;
  live observations have priority.
- No secrets in PostGIS, MQTT, logs, exceptions, fixtures, or documentation.
- CPU-heavy radar calculations remain limited to two threads.
- A provider/catalog/PostGIS/replay failure never stops other providers.
- Preserve all existing PostGIS rows and rollback data.

## Review focus

- Polygon intersection must reject a near-centerline target outside the cell.
- Entry and exit times must be deterministic and ordered.
- Station selection must deduplicate physical stations across shared regions.
- A missing Wunderground key must produce `ready/blocked_api_key`.
- Full bootstrap must resume from checkpoints without repeating completed
  tiles and must stop at configured provider budgets.
- Replay and live calculation must return identical output for identical data.
- Fresh-process import tests must prove that legacy infrastructure is absent.

---

### Task 1: Add the PostGIS station and forecast-validation schema

**Files:**
- Create: `storm_tracker_worker/postgis_migrations/0003_station_catalog_validation.sql`
- Create: `storm_tracker_worker/thin_station_catalog.py`
- Test: `tests/test_thin_station_catalog.py`
- Modify: `tests/test_thin_postgis.py`

**Produces:** Immutable station/capability models, catalog upserts, indexed
radius selection, discovery-tile checkpoints, passage/validation/replay tables.

- [ ] Write failing tests for station identity, capability normalization,
  idempotent upsert, nearest-capable selection, shared-region deduplication,
  stale-tile detection, and checkpoint resumption.
- [ ] Run `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_station_catalog.py tests/test_thin_postgis.py`.
  **Expected:** failures because the catalog and migration do not exist.
- [ ] Add migration `0003` with `stv3_ground_station`,
  `stv3_station_capability`, `stv3_station_discovery_tile`,
  `stv3_target_passage`, `stv3_forecast_validation`, `stv3_replay_run`, and
  `stv3_replay_metric`; add GiST/time/provider indexes and least-privilege
  grants to `storm_ingest`.
- [ ] Implement focused catalog models and SQL repository operations. Keep API
  clients out of this module.
- [ ] Rerun the task tests and verify they pass.

### Task 2: Build the resumable one-time station bootstrap

**Files:**
- Create: `storm_tracker_worker/thin_station_bootstrap.py`
- Create: `scripts/bootstrap_thin_station_catalog.py`
- Create: `storm_tracker_worker/wunderground_seed_stations.json`
- Test: `tests/test_thin_station_bootstrap.py`
- Modify: `storm_tracker_worker/config.yaml`
- Modify: `storm_tracker_worker/run.sh`

**Produces:** A manual, resumable bootstrap command and provider importers.

- [ ] Verify current official Netatmo, AviationWeather/METAR, and
  Wunderground API documentation. Record endpoint, pagination/tile behavior,
  rate-limit assumptions, and discovery availability in test fixtures/docs.
- [ ] Write failing fixture tests proving:
  - the complete official METAR station feed normalizes globally;
  - Netatmo tile traversal checkpoints each completed tile;
  - a restart skips completed tiles;
  - provider call and byte budgets stop cleanly and remain resumable;
  - Wunderground imports the known seed list without a key;
  - Wunderground directory discovery runs only when officially supported;
  - credentials are absent from every persisted record and diagnostic.
- [ ] Run `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_station_bootstrap.py`.
  **Expected:** failures because the bootstrap module is absent.
- [ ] Implement injected provider sources and a CLI with `--provider`,
  `--resume`, `--max-calls`, `--max-bytes`, and `--dry-run`. Make global
  Netatmo traversal explicitly rate-limited and checkpointed.
- [ ] Seed the approved Wunderground stations: `IWILLE87`, `IMECHE70`,
  `IMECHE17`, `IRUMST20`, `IKAPEL108`, `IMECHE71`, `IMECHE79`, `IDUFFE19`,
  and `IBOOM10`. Do not restore removed `IMECHE89`.
- [ ] Rerun tests and verify green. Do not execute the live global bootstrap
  until its dry-run, official limits, and PostGIS migration are verified.

### Task 3: Select and poll only useful catalog stations

**Files:**
- Create: `storm_tracker_worker/thin_ground_station_runtime.py`
- Modify: `storm_tracker_worker/thin_provider_catalog.py`
- Modify: `storm_tracker_worker/thin_provider_adapters.py`
- Modify: `storm_tracker_worker/thin_provider_runtime.py`
- Modify: `storm_tracker_worker/thin_mqtt_service.py`
- Modify: `storm_tracker_worker/config.yaml`
- Modify: `storm_tracker_worker/run.sh`
- Test: `tests/test_thin_ground_station_runtime.py`
- Modify: `tests/test_thin_provider_adapters.py`
- Modify: `tests/test_thin_provider_catalog.py`

**Produces:** Per-region station plans and a Wunderground adapter using the
same `NormalizedObservation` contract as Netatmo and METAR.

- [ ] Write failing tests for capability/distance/freshness scoring, directional
  coverage, shared-region deduplication, one request per physical station,
  METAR station-ID queries, Netatmo region query filtering, Wunderground
  current-observation normalization, missing-key blocking, and secret safety.
- [ ] Run the task tests.
  **Expected:** failures for missing station planner/Wunderground adapter.
- [ ] Implement local PostGIS radius queries and choose the smallest station
  set that covers rain plus available pressure/wind/temperature. Keep
  provider-specific call budgets and one in-flight request per station.
- [ ] Add Wunderground configuration and direct current-observation adapter.
  Do not invent a directory API if official documentation provides none.
- [ ] Verify the task tests and all existing thin provider tests pass.

### Task 4: Forecast complete translated cell footprints

**Files:**
- Create: `storm_tracker_worker/thin_footprint_forecast.py`
- Modify: `storm_tracker_worker/thin_radar_tracking.py`
- Modify: `storm_tracker_worker/thin_nowcast.py`
- Modify: `storm_tracker_worker/thin_operational_runtime.py`
- Modify: `storm_tracker_worker/thin_postgis.py`
- Test: `tests/test_thin_footprint_forecast.py`
- Modify: `tests/test_thin_radar_tracking.py`
- Modify: `tests/test_thin_nowcast.py`
- Modify: `tests/test_thin_operational_runtime.py`
- Modify: `tests/test_thin_postgis.py`

**Produces:** `TargetPassage` with exact footprint entry/exit, method, cell,
provider, intensity, and fallback reason.

- [ ] Write failing tests for current intersection, future entry/exit, no
  intersection outside an irregular polygon, east/west/north/south motion,
  stationary cells, horizon clipping, target accuracy buffer, and older-frame
  rejection.
- [ ] Run footprint/tracking/nowcast tests.
  **Expected:** failures because the thin tracker exposes only centroids.
- [ ] Extract only the deterministic interval/translated-footprint mathematics
  from the old worker implementation into the new pure module. Add polygon or
  footprint pixels to `RadarTrack`; do not import `worker_only_*`.
- [ ] Make polygon prediction primary and retain the centerline radius only as
  an explicit `centerline_fallback` method.
- [ ] Persist passage entry/exit and publish the fields through the existing
  compact snapshot/MQTT contract.
- [ ] Rerun all named tests and verify green.

### Task 5: Validate radar passages with ground measurements

**Files:**
- Create: `storm_tracker_worker/thin_ground_validation.py`
- Modify: `storm_tracker_worker/thin_operational_runtime.py`
- Modify: `storm_tracker_worker/thin_postgis.py`
- Test: `tests/test_thin_ground_validation.py`
- Modify: `tests/test_thin_operational_runtime.py`
- Modify: `tests/test_thin_postgis.py`

**Produces:** Immutable validation outcomes: hit, miss, false alarm, early,
late, entry error, exit error, intensity comparison, contributing stations.

- [ ] Write failing tests covering each outcome, clock tolerance, stale ground
  data rejection, duplicate physical stations, dry confirmations, and mixed
  Netatmo/METAR/Wunderground input.
- [ ] Run task tests.
  **Expected:** failures because validation does not exist.
- [ ] Implement one pure temporal/spatial validator. Persist results
  idempotently; expose only compact validation counts/status in MQTT.
- [ ] Keep validation observer-only: it must not change live forecast weights.
- [ ] Rerun task tests and the full thin suite.

### Task 6: Add offline PostGIS replay and calibration metrics

**Files:**
- Create: `storm_tracker_worker/thin_replay.py`
- Create: `scripts/run_thin_replay.py`
- Test: `tests/test_thin_replay.py`

**Produces:** A manual date-range replay using the exact live footprint and
validation functions, plus PostGIS replay metrics.

- [ ] Write failing tests proving live/replay parity, deterministic ordering,
  date-range bounds, no provider/network access, hit/miss/false-alarm metrics,
  entry/exit error percentiles, and failure isolation.
- [ ] Run `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_replay.py`.
  **Expected:** failure because replay is absent.
- [ ] Implement repository reads, pure replay orchestration and CLI options
  `--from`, `--until`, `--target`, `--provider`, and `--dry-run`.
- [ ] Persist algorithm version, parameters and aggregate metrics without
  receipts or hashes. Ensure replay is not imported by worker startup.
- [ ] Rerun task tests and verify green.

### Task 7: Verify, bootstrap, deploy and live-accept

**Files:**
- Modify: `CHANGELOG.md`
- Modify: integration and worker version files
- Modify: `docs/THIN_OPERATIONAL_LIVE_ACCEPTANCE.md`
- Create: a versioned deploy evidence document

- [ ] Run the entire targeted thin suite plus release, packaging, runtime-mode,
  and fresh-process forbidden-import tests.
- [ ] Build the verified release pair and record SHA-256 hashes.
- [ ] Create the required NAS-HA rollback backup.
- [ ] Apply PostGIS migration `0003` without deleting existing data.
- [ ] Run bootstrap dry-runs for METAR, Netatmo, and Wunderground and inspect
  bounded call/byte estimates.
- [ ] Execute the full METAR import. Execute Netatmo in resumable rate-limited
  batches. Execute Wunderground directory bootstrap only if the official API
  supports it; otherwise import seeds and report catalog coverage honestly.
- [ ] Deploy through `scripts/install_ha_release_pair_ssh.py`, start worker
  first, wait for fresh heartbeat/admission, then enable integration.
- [ ] Verify heartbeat under 15 seconds, first Belgian snapshot under 60
  seconds, footprint geometry plus entry/exit, station selection, PostGIS row
  growth, MQTT/HA consumption, and provider-local failure isolation.
- [ ] Move only the fictive tracker through representative catalog-covered and
  uncovered regions; prove local selection and bounded discovery without
  changing real Life360 targets.

## Completion

The feature is complete only when the thin live runtime imports no legacy
infrastructure, all named tests and the wider thin suite pass, PostGIS retains
existing data, the NAS deployment passes the fixed runbook, and catalog
coverage is reported per provider without claiming unavailable discovery.
