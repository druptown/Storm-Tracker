# Thin Operational Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the operational worker path with the direct pipeline locations → geographically applicable providers → normalized observations → quality/fusion → storm tracking/forecasting → PostGIS → MQTT/HA.

**Architecture:** A new `thin_operational` runtime runs beside the existing worker until live parity is proven. It reuses pure provider parsing and proven V3 tracking logic, but does not import or call RAW stores, ledgers, receipts, authority gates, completion routers, reclamation, or corpus audits. After live cutover, those old runtime paths are deleted in a separate final task.

**Tech Stack:** Python 3.12, asyncio, paho-mqtt, aiohttp/stdlib HTTP adapters, psycopg/PostgreSQL/PostGIS, pytest, Home Assistant MQTT entities.

**Spec:** `docs/superpowers/specs/2026-09-20-thin-operational-pipeline-design.md`

## Global Constraints

- NAS-Home Assistant `192.168.0.15` is the only deploy target; never touch NUC `192.168.0.144`.
- Heartbeat must publish within 15 seconds and the first Belgian snapshot within 60 seconds unless an external provider is unavailable.
- HA owns locations/configuration/dashboard/alerts; the worker owns provider I/O and meteorological computation.
- CPU-heavy decode/fusion uses at most two threads; independent provider I/O may run concurrently; one request per provider maximum.
- PostGIS is the only durable meteorological store in the new path.
- No RAW archive, manifest, receipt, capacity ledger, authority/admission proof, completion router, reclamation role, or corpus audit may be imported by the new runtime.
- A provider failure is isolated and never stops MQTT or another provider.
- Existing data and legacy runtime stay intact until live parity is proven; removal requires a rollback backup.

## Review Focus

- A target crossing a provider coverage boundary selects the provider only while inside; Task 2 tests both sides of the boundary.
- A malformed provider response becomes a provider-local error while other observations still fuse; Tasks 3 and 4 test this.
- Out-of-order observations never move a track backward; Task 5 tests the time reversal case.
- PostGIS downtime does not suppress MQTT output and memory buffering stays bounded; Task 6 tests this.
- A retained/stale MQTT desired state cannot overwrite a newer revision; Task 7 tests monotonic configuration revisions.

---

### Task 1: Define the one internal observation contract

**Files:**
- Create: `storm_tracker_worker/thin_observation.py`
- Test: `tests/test_thin_observation.py`

**Interfaces:**
- Consumes: provider-native values, timestamps, geometry and quality metadata.
- Produces: `ObservationKind`, `ObservationQuality`, `NormalizedObservation`, and `normalize_unit(kind, value, unit)`.

- [ ] **Step 1: Write the failing contract tests**

```python
def test_normalized_rain_rate_converts_inches_to_mm_per_hour():
    observation = NormalizedObservation.create(
        provider_id="test", product_id="rain", target_id="home",
        kind=ObservationKind.RAIN_RATE, value=1.0, unit="in/h",
        observed_at=100.0, received_at=101.0,
        latitude=51.1, longitude=4.4, quality=ObservationQuality.GOOD,
    )
    assert observation.value == 25.4
    assert observation.unit == "mm/h"

def test_observation_rejects_future_or_non_finite_values():
    with pytest.raises(ThinObservationError, match="observed_at_invalid"):
        NormalizedObservation.create(
            provider_id="test", product_id="rain", target_id="home",
            kind=ObservationKind.RAIN_RATE, value=float("nan"), unit="mm/h",
            observed_at=200.0, received_at=100.0,
            latitude=51.1, longitude=4.4,
            quality=ObservationQuality.GOOD,
        )
```

- [ ] **Step 2: Run `PYTHONPATH=. ./.venv/bin/pytest -q tests/test_thin_observation.py` and verify both tests fail because the module is absent.**

- [ ] **Step 3: Implement immutable enums/dataclass, canonical SI-unit conversion, finite/range checks, maximum 15-minute source age, and a compact `as_dict()` containing no credentials.**

- [ ] **Step 4: Run the Task 1 test file and verify it passes.**

- [ ] **Step 5: Commit only Task 1 files with `feat: add thin normalized observation contract`.**

### Task 2: Select only geographically applicable providers

**Files:**
- Create: `storm_tracker_worker/thin_provider_catalog.py`
- Test: `tests/test_thin_provider_catalog.py`

**Interfaces:**
- Consumes: `DesiredState` from `storm_tracker_worker.desired_state`.
- Produces: `ThinProviderSpec` and `build_due_requests(state, now, last_attempts)` returning immutable `ThinProviderRequest` objects.

- [ ] **Step 1: Write failing catalog tests with literal Belgian and outside-coverage coordinates.**

```python
def test_belgian_target_selects_belgian_operational_sources_only():
    requests = build_due_requests(belgian_state(), now=600.0, last_attempts={})
    assert {item.provider_id for item in requests} >= {
        "opera_radar", "open_meteo", "metar"
    }
    assert "aemet_radar" not in {item.provider_id for item in requests}

def test_provider_boundary_excludes_target_outside_coverage():
    requests = build_due_requests(spain_state(), now=600.0, last_attempts={})
    assert "kmi_radar" not in {item.provider_id for item in requests}
```

- [ ] **Step 2: Run the Task 2 test file and verify catalog selection fails before implementation.**

- [ ] **Step 3: Implement a static catalog for Belgium-first providers with explicit bounding polygons/radius rules and simple cadence timestamps; do not import `acquisition_*`, `raw_*`, scheduler, ledger, receipt, or authority modules.**

- [ ] **Step 4: Add and pass a test proving a provider gets at most one due request even when several targets share its query footprint.**

- [ ] **Step 5: Commit with `feat: add geographic thin provider planner`.**

### Task 3: Add direct Belgian provider adapters

**Files:**
- Create: `storm_tracker_worker/thin_provider_adapters.py`
- Create: `storm_tracker_worker/thin_provider_runtime.py`
- Test: `tests/test_thin_provider_adapters.py`
- Test: `tests/test_thin_provider_runtime.py`
- Reuse parsing math from: `custom_components/storm_tracker_v3/providers/kmi.py`, `dwd_radolan.py`, `knmi.py`, `opera.py`, `open_meteo.py`, `blitzortung.py`

**Interfaces:**
- Consumes: `ThinProviderRequest`, injected async HTTP fetcher, desired-state targets.
- Produces: `Mapping[str, ProviderResult]`, keyed by provider id; each result contains observations, status, reason code, attempt time and completion time.

- [ ] **Step 1: Write fixture-driven failing tests for KMI/OPERA radar and Open-Meteo that assert exact `NormalizedObservation` output from existing small recorded fixtures.**

```python
async def test_runtime_isolates_one_malformed_provider():
    results = await ThinProviderRuntime(adapters={
        "broken": BrokenAdapter(), "healthy": FixtureAdapter(RAIN_OBSERVATION)
    }, max_parallel=8).fetch((BROKEN_REQUEST, HEALTHY_REQUEST))
    assert results["broken"].status == "error"
    assert results["healthy"].observations == (RAIN_OBSERVATION,)
```

- [ ] **Step 2: Run the adapter/runtime tests and verify failure because the direct interfaces do not exist.**

- [ ] **Step 3: Extract only pure request construction and parsing code into adapters; adapters return observations directly and never write files, RAW objects, manifests or receipts. Implement one `asyncio.Lock` per provider, request timeouts, two bounded retries, and provider-local back-off.**

- [ ] **Step 4: Add DWD/KNMI edge coverage, METAR ground observations, available Netatmo data, and direct Blitzortung event ingestion using the same result contract. Keep the KMI adapter `ready` but return `blocked/external_access_required` until API access exists; missing credentials are never a process error.**

- [ ] **Step 5: Run both Task 3 test files and the existing provider parser tests used by the extracted code.**

- [ ] **Step 6: Commit with `feat: fetch Belgian observations without raw infrastructure`.**

### Task 4: Quality-control and fuse normalized observations

**Files:**
- Create: `storm_tracker_worker/thin_fusion.py`
- Test: `tests/test_thin_fusion.py`
- Reuse algorithms from: `custom_components/storm_tracker_v3/providers/radar_fusion.py`

**Interfaces:**
- Consumes: `Mapping[str, ProviderResult]`, target IDs and evaluation time.
- Produces: `FusedTargetObservation` and `FusionCycle` with accepted/rejected provider reasons.

- [ ] **Step 1: Write failing tests for unit-compatible median/quality fusion, stale-source rejection, disagreement handling, and one malformed result beside two healthy providers.**

```python
def test_one_bad_provider_does_not_erase_two_consistent_sources():
    cycle = fuse_observations({
        "kmi_radar": KMI_2_MM_H,
        "opera_radar": OPERA_2_2_MM_H,
        "broken": BROKEN_RESULT,
    }, evaluated_at=120.0)
    home = cycle.targets["home"]
    assert home.precipitation_rate_mm_h == pytest.approx(2.1)
    assert home.contributors == ("kmi_radar", "opera_radar")
    assert cycle.providers["broken"].status == "error"
```

- [ ] **Step 2: Run Task 4 tests and verify failure before implementation.**

- [ ] **Step 3: Implement deterministic freshness/quality filtering, weighted fusion with literal thresholds, contribution provenance and `onvoldoende_data` only when no usable input remains.**

- [ ] **Step 4: Run Task 4 and relevant existing `test_radar_fusion.py` tests.**

- [ ] **Step 5: Commit with `feat: fuse normalized provider observations`.**

### Task 5: Reuse V3 cell tracking and produce map/forecast results

**Files:**
- Create: `storm_tracker_worker/thin_nowcast.py`
- Test: `tests/test_thin_nowcast.py`
- Reuse: `custom_components/storm_tracker_v3/engine/cell_track.py`, `motion_prediction.py`, `trajectory.py`, and pure geometry helpers.

**Interfaces:**
- Consumes: successive `FusionCycle` objects and targets.
- Produces: `ThinNowcast(target_results, storm_cells, evaluated_at)` where map cells include current precipitation geometry even without a future target arrival.

- [ ] **Step 1: Write failing two-frame tests proving eastward motion, ETA calculation, current-rain map visibility, and rejection of an older frame after a newer frame.**

```python
def test_current_rain_is_on_map_without_future_arrival():
    nowcast = runtime.advance(current_rain_cycle())
    assert nowcast.target_results["home"].current_state == "regen"
    assert len(nowcast.storm_cells) >= 1
```

- [ ] **Step 2: Run Task 5 tests and verify failure before implementation.**

- [ ] **Step 3: Adapt proven V3 tracking primitives to `FusionCycle`; keep only bounded in-memory previous frames and no SQLite tracking database. Use the two-thread executor only around CPU-heavy grid/cell work.**

- [ ] **Step 4: Run Task 5 plus existing V3 tracking/trajectory tests.**

- [ ] **Step 5: Commit with `feat: add thin storm tracking and forecast`.**

### Task 6: Persist useful observations and results directly in PostGIS

**Files:**
- Create: `storm_tracker_worker/thin_postgis.py`
- Test: `tests/test_thin_postgis.py`

**Interfaces:**
- Consumes: `FusionCycle` and `ThinNowcast`.
- Produces: async `write_cycle(cycle, nowcast) -> PostGISWriteResult`; diagnostics expose status, written row counts and last fixed error code.

- [ ] **Step 1: Write failing tests against the existing fake psycopg connection for idempotent inserts, geometry parameters, reconnect after outage, and bounded queue overflow dropping the oldest uncommitted cycle.**

- [ ] **Step 2: Run Task 6 tests and verify failure before implementation.**

- [ ] **Step 3: Create `stv3_observation`, `stv3_fusion_cycle`, `stv3_storm_cell`, `stv3_track_point` and `stv3_target_forecast` tables with unique natural keys and PostGIS geometry columns. Implement `ON CONFLICT DO UPDATE` and a maximum eight-cycle in-memory retry deque.**

- [ ] **Step 4: Verify a connection failure returns `degraded/postgis_unavailable` without raising into the runtime or suppressing MQTT.**

- [ ] **Step 5: Run Task 6 tests and commit with `feat: persist thin weather results in PostGIS`.**

### Task 7: Publish heartbeat and operational snapshot directly over MQTT

**Files:**
- Create: `storm_tracker_worker/thin_operational_runtime.py`
- Create: `storm_tracker_worker/thin_mqtt_service.py`
- Test: `tests/test_thin_operational_runtime.py`
- Test: `tests/test_thin_mqtt_service.py`
- Modify: `storm_tracker_worker/run.sh`
- Modify: `storm_tracker_worker/config.yaml`

**Interfaces:**
- Consumes: desired-state MQTT messages and Tasks 2–6 services.
- Produces: existing `storm_tracker/v1/worker/status` and `storm_tracker/v1/operational/snapshot` payload shapes consumed by HA.

- [ ] **Step 1: Write failing lifecycle tests proving MQTT loop and heartbeat start before provider construction, desired-state revisions are monotonic, first cycle is scheduled immediately, and PostGIS failure does not suppress the snapshot.**

```python
async def test_heartbeat_precedes_provider_work():
    events = []
    service = ThinMqttService(client=RecordingClient(events),
                              runtime=SlowRuntime(events))
    await service.start()
    assert events[0] == "mqtt_loop_started"
    assert events[1] == "heartbeat_online"
```

- [ ] **Step 2: Run Task 7 tests and verify failure before implementation.**

- [ ] **Step 3: Implement a small async loop: connect MQTT, publish online heartbeat, accept latest desired state, execute due provider cycle, fuse, nowcast, enqueue PostGIS write, publish snapshot, sleep until next due time. Publish offline through MQTT last-will.**

- [ ] **Step 4: Add `thin_operational` to the runtime-mode schema and route it directly to `python -m storm_tracker_worker.thin_mqtt_service`; do not construct `WorkerService`, `RawCaptureServiceRuntime` or `WorkerOnlyServiceRuntime`.**

- [ ] **Step 5: Run Task 7, packaging tests and a local fake-MQTT end-to-end test that must publish heartbeat in under one second and a fixture snapshot in under five seconds.**

- [ ] **Step 6: Commit with `feat: add direct thin MQTT runtime`.**

### Task 8: Deploy shadow mode, prove Belgium, cut over, then remove ballast

**Files:**
- Modify: `tests/test_runtime_payload_bounds.py`
- Modify: `tests/test_worker_runtime_mode_packaging_contract.py`
- Create: `docs/THIN_OPERATIONAL_LIVE_ACCEPTANCE.md`
- Delete after the Step 4 parity approval: old `raw_*`, `acquisition_*`, ledger/receipt/authority/completion/reclamation runtime modules and their exclusive tests.

**Interfaces:**
- Consumes: the complete thin runtime and the fixed NAS deployment procedure.
- Produces: live HA snapshot/map/forecast plus a deletion manifest listing every legacy file and retained shared decoder.

- [ ] **Step 1: Add an import-boundary test that loads the thin runtime and fails if any forbidden module prefix (`raw_`, `acquisition_`, `completion_`, `terminal_`) appears in `sys.modules`.**

- [ ] **Step 2: Build the verified release pair, create a rollback backup, deploy only to NAS-HA via `scripts/install_ha_release_pair_ssh.py`, select `thin_operational`, and start the app once.**

- [ ] **Step 3: Record live acceptance: heartbeat ≤15 seconds; first snapshot ≤60 seconds; OPERA/Open-Meteo and available Belgian ground sources current; KMI reports `ready` plus `blocked/external_access_required` while its API access is absent; rain visible as target measurement and map geometry; PostGIS rows increasing; MQTT snapshot consumed by HA.**

- [ ] **Step 4: Move the fictive tracker to one applicable location per remaining provider and record `ready` plus actual status without allowing those checks to block Belgian output.**

- [ ] **Step 5: After user-visible parity approval, generate a literal deletion manifest with `rg` import evidence, remove only modules exclusive to the legacy path, rebuild, rerun the full suite and redeploy. Preserve provider decoders, V3 algorithms, PostGIS schema data and rollback backup.**

- [ ] **Step 6: Commit with `refactor: remove legacy worker infrastructure after thin cutover`.**
