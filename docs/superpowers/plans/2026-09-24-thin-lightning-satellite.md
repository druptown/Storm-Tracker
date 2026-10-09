# Thin Lightning and Satellite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Connect all supported lightning and satellite products to the thin worker, with geographic selection, normalized observations, truthful dual status, PostGIS persistence, and live NAS-HA validation.

**Architecture:** Add focused thin adapters around the already tested native provider primitives, register them in the existing catalogue/service/runtime maps, and let the current provider runtime, fusion, snapshot, and PostGIS writer carry the normalized observations. The legacy scheduler, RAW receipts, and startup audit remain disconnected from the live service.

**Tech Stack:** Python 3.13, asyncio, h5py/netCDF4-compatible readers already packaged by the worker, pytest, Home Assistant Supervisor app deployment, MQTT, PostgreSQL/PostGIS.

**Spec:** `docs/superpowers/specs/2026-09-24-thin-lightning-satellite.md`

## Global Constraints

- Work and deploy only against NAS-HA `192.168.0.15`; never touch the NUC.
- Preserve `dedicated_nas_fast`, CPU 200%, nice 0, and two compute threads.
- Do not expose credentials or secrets in logs, tests, evidence, or chat.
- Do not add jobs, receipts, RAW-state audits, startup audits, or a legacy scheduler dependency.
- Worker is deployed before integration through `scripts/install_ha_release_pair_ssh.py`.
- Every live location test restores the fictive tracker to exact home coordinates when finished.

## Review Focus

- A current but empty lightning scene must be operational with zero observations, not unavailable.
- Dateline-spanning GOES-18 coverage must accept both western longitudes and the western Pacific without a false global match.
- Stale or future-dated products must not enter fusion or PostGIS.
- Missing EUMETSAT/H SAF authority must produce a precise blocked state without leaking credentials.
- A malformed or unit-incompatible precipitation grid must fail closed and retain the previous healthy snapshot.

---

### Task 1: Thin lightning sources

**Files:**
- Create: `storm_tracker_worker/thin_satellite_adapters.py`
- Modify: `storm_tracker_worker/thin_provider_catalog.py`
- Modify: `storm_tracker_worker/thin_mqtt_service.py`
- Modify: `storm_tracker_worker/thin_operational_runtime.py`
- Test: `tests/test_thin_satellite_adapters.py`
- Test: `tests/test_thin_provider_catalog.py`
- Test: `tests/test_thin_operational_runtime.py`

**Interfaces:**
- Consumes: `ThinProviderRequest`, `NormalizedObservation.create`, existing EUMETSAT/GOES native download and netCDF inspection helpers, and the current provider runtime adapter contract `async fetch(request, fetcher, *, now)`.
- Produces: `EumetsatLightningAdapter`, `GoesGlmAdapter(satellite: int)`, catalogue IDs `eumetsat_li`, `goes18_glm`, `goes19_glm`, and snapshot IDs matching the existing workload identities.

- [x] **Step 1: Write failing coverage and adapter tests**

```python
def test_lightning_catalogue_selects_only_geographically_applicable_sources():
    europe = provider_ids_for_target(51.05, 4.42)
    america = provider_ids_for_target(40.0, -75.0)
    assert "eumetsat_li" in europe
    assert "goes19_glm" in america
    assert "goes18_glm" not in europe

@pytest.mark.asyncio
async def test_empty_current_lightning_scene_is_operational():
    result = await adapter.fetch(request, fetcher, now=NOW)
    assert tuple(result) == ()
    assert result.status_reason == "no_observations"
```

- [x] **Step 2: Run the focused tests and observe RED**

Run: `pytest -q tests/test_thin_satellite_adapters.py tests/test_thin_provider_catalog.py tests/test_thin_operational_runtime.py`

Expected: FAIL because the three thin lightning providers and adapters are absent.

- [x] **Step 3: Implement minimal lightning adapters and registrations**

Create focused decoding helpers that emit one `LIGHTNING_STRIKE` observation per valid in-range event, reject stale/future events, and return `ThinAdapterResult((), status_reason="no_observations")` for a current quiet scene. Register catalogue coverage/cadence, construct adapters in `_build_provider_adapters`, and map thin IDs to `lightning.eumetsat.li`, `lightning.goes18.glm`, and `lightning.goes19.glm`.

- [x] **Step 4: Run focused and thin regression tests**

Run: `pytest -q tests/test_thin_satellite_adapters.py tests/test_thin_blitzortung.py tests/test_thin_provider_catalog.py tests/test_thin_provider_runtime.py tests/test_thin_operational_runtime.py tests/test_thin_mqtt_service.py`

Expected: PASS.

- [x] **Step 5: Deploy and live-test lightning on NAS-HA**

Build the next worker/integration release pair, deploy worker first, test Europe and both GOES domains with the fictive tracker, verify real timestamps/statuses/PostGIS, and restore exact home coordinates.

Expected: Blitzortung plus every in-scope thin lightning provider is ready; its effective status is truthful for the current scene.

### Task 2: Thin satellite precipitation sources

**Files:**
- Modify: `storm_tracker_worker/thin_satellite_adapters.py`
- Modify: `storm_tracker_worker/thin_provider_catalog.py`
- Modify: `storm_tracker_worker/thin_mqtt_service.py`
- Modify: `storm_tracker_worker/thin_operational_runtime.py`
- Test: `tests/test_thin_satellite_adapters.py`
- Test: `tests/test_thin_fusion.py`
- Test: `tests/test_thin_postgis.py`

**Interfaces:**
- Consumes: the same thin adapter contract, existing GOES RRQPE/NOAA ERR/H SAF native bounded fetch and quantitative decode primitives, and `ObservationKind.RAIN_RATE`.
- Produces: `GoesRrqpeAdapter(satellite: int)`, `NoaaErrAdapter`, `HsafH40bAdapter`, catalogue IDs `goes18_rrqpe`, `goes19_rrqpe`, `noaa_err`, `hsaf_h40b`, normalized precipitation observations, and existing snapshot workload IDs.

- [x] **Step 1: Write failing quantitative, age, coverage, fusion and persistence tests**

```python
@pytest.mark.asyncio
async def test_goes_rrqpe_emits_current_rain_rate_inside_target_region():
    observations = await adapter.fetch(request, fetcher, now=NOW)
    assert observations
    assert {item.kind for item in observations} == {ObservationKind.RAIN_RATE}
    assert all(item.unit == "mm/h" for item in observations)

@pytest.mark.asyncio
async def test_satellite_adapter_rejects_stale_grid():
    with pytest.raises(ValueError, match="stale"):
        await adapter.fetch(request, stale_fetcher, now=NOW)
```

- [x] **Step 2: Run the focused tests and observe RED**

Run: `pytest -q tests/test_thin_satellite_adapters.py tests/test_thin_provider_catalog.py tests/test_thin_fusion.py tests/test_thin_postgis.py`

Expected: FAIL because the four thin satellite providers and adapters are absent.

- [x] **Step 3: Implement minimal satellite adapters and registrations**

Decode only current quantitative rain-rate pixels intersecting active acquisition regions; normalize accepted values to `mm/h` with geometry, time, quality, and source metadata; reject invalid units, coordinates, fill values, stale/future scenes, and malformed grids. Register provider coverage/cadence, service construction, and existing snapshot workload IDs.

- [x] **Step 4: Run focused and complete relevant regression suites**

Run: `pytest -q tests/test_thin_satellite_adapters.py tests/test_thin_provider_catalog.py tests/test_thin_provider_runtime.py tests/test_thin_fusion.py tests/test_thin_postgis.py tests/test_thin_operational_runtime.py tests/test_thin_mqtt_service.py`

Expected: PASS.

- [x] **Step 5: Deploy and live-test satellites on NAS-HA**

Build the next release pair, deploy worker before integration, move the fictive tracker through each product's existing catalogue coverage, and verify actual current artifacts, normalized rain data when present, truthful quiet/empty state, fusion visibility, and PostGIS writes. Restore exact home coordinates.

Expected: all four satellite providers are ready, geography is respected, effective state is truthful, PostGIS remains current, and the worker remains online.
