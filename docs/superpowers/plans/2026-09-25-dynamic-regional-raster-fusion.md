# Dynamic Regional Raster Fusion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build one geographically correct, quality-weighted precipitation field per active warning region and feed it through the existing cell tracking, forecast, PostGIS, snapshot, and MQTT path.

**Architecture:** Keep provider acquisition and scalar observation fusion unchanged. A new focused thin module builds a stable local azimuthal-equidistant `AnalysisGrid`, conservatively projects applicable quantitative `ThinRadarField` values, and fuses at most the three best contributors per pixel. A two-thread non-blocking coordinator publishes completed regional fields to the existing `ThinRadarTracker`; the current per-provider path remains an explicitly reported fallback.

**Tech Stack:** Python 3.13, NumPy 2.5, pyproj 3.7, asyncio, `concurrent.futures.ThreadPoolExecutor`, psycopg/PostGIS, MQTT, pytest.

**Spec:** `docs/superpowers/specs/2026-09-25-dynamic-regional-raster-fusion-design.md`

## Global Constraints

- Work only in `/home/ikke/storm-tracker` and deploy only to NAS-Home Assistant `192.168.0.15`; never access or modify NUC `192.168.0.144`.
- Do not add RAW-, receipt-, audit-, authority-, manifest-, or persistent raster infrastructure.
- Keep `dedicated_nas_fast`, `cpu_limit_percent: 200`, `worker_nice: 0`, and `cpu_threads: 2` unchanged.
- Keep provider I/O, scalar `fuse_observations`, lightning, satellite, ground station, and model roles unchanged.
- Fuse only quantitative rain-rate rasters; do not average lightning, satellite imagery, ground observations, models, reflectivity, or visual-only products into rain rate.
- Select applicable providers dynamically from current region coverage and runtime state; do not encode a country/provider allowlist.
- Use a local azimuthal-equidistant CRS, minimum 500 m pixels, and at most 250,000 cells per active region.
- Distinguish valid zero, source nodata, and outside source support.
- Use at most three numerical contributors per pixel; report other valid candidates as `lower_ranked_standby`.
- Keep the existing per-provider tracker available as a marked fallback until live acceptance succeeds.
- Do not store or publish complete raster matrices in PostGIS, Home Assistant, or MQTT.
- Follow `AGENTS.md` and `docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md`; start the worker before the integration and require a fresh heartbeat with `job_admission_open=true`.

## Review Focus

- A field whose CRS declares kilometres while its arrays are indexed in metres must be normalized once at adapter output; projection code must contain no KNMI-specific branch.
- A valid zero-rain source pixel must remain a contributing zero, while nodata and pixels outside the source extent remain non-contributors.
- Regions crossing longitude ±180° or centered at high latitude must produce finite, stable local grids and bounded projected shapes.
- Four or more applicable radars must use only the best three per pixel, with deterministic ordering and explicit standby provenance.
- A slow, failed, or empty fusion job must never block heartbeats or prevent the old per-provider fallback from producing a cycle.

---

## File Map

- `storm_tracker_worker/thin_provider_adapters.py`: enforce metre-based `ThinRadarField` georeferencing at the adapter boundary.
- `storm_tracker_worker/thin_radar_tracking.py`: remove the historical KNMI unit exception and accept fused fields through the existing contract.
- `storm_tracker_worker/thin_regional_radar_fusion.py`: new pure grid, reprojection, weighting, provenance, and fusion implementation.
- `storm_tracker_worker/thin_regional_radar_runtime.py`: new two-thread non-blocking coordinator and completed-result cache.
- `storm_tracker_worker/thin_operational_runtime.py`: dynamic regional fusion wiring, fused tracking, and marked fallback.
- `storm_tracker_worker/thin_mqtt_service.py`: close the coordinator cleanly and publish only compact snapshot/GeoJSON metadata.
- `storm_tracker_worker/tracking_core/operational_snapshot.py`: validate the optional compact `radar_fusion` collection.
- `custom_components/storm_tracker_v3/operational_snapshot.py`: mirror the snapshot contract used by Home Assistant.
- `storm_tracker_worker/thin_postgis.py`: persist compact regional fusion summaries with each cycle.
- `storm_tracker_worker/postgis_migrations/0004_regional_radar_fusion.sql`: add one JSONB summary column, not raster storage.
- `tests/test_postgis_migrations.py`: verify the migration is idempotent, registered, and contains no raster/blob column.
- Tests named below pin every new contract and failure path.

### Task 1: Normalize Radar Field Georeferencing at the Adapter Boundary

**Files:**
- Modify: `storm_tracker_worker/thin_provider_adapters.py:133-170,3470-3555`
- Modify: `storm_tracker_worker/thin_radar_tracking.py:224-285,291-365`
- Test: `tests/test_thin_provider_adapters.py`
- Test: `tests/test_thin_radar_tracking.py`

**Interfaces:**
- Consumes: pyproj `CRS` and the current immutable `ThinRadarField` constructor.
- Produces: every `ThinRadarField.crs` is projected with metre axes and every origin/resolution value is expressed in those CRS metres; `ThinRadarTracker` has no provider-specific coordinate scaling.

- [ ] **Step 1: Add a failing KNMI metric-contract test**

Extend `test_knmi_rtcor_latest_frame_yields_rain_without_leaking_auth` after
its existing `field` assertions:

```python
crs = CRS.from_user_input(field.crs)
assert crs.is_projected
assert crs.axis_info[0].unit_conversion_factor == pytest.approx(1.0)
assert field.x_resolution_m == pytest.approx(1_000.0)
assert field.y_resolution_m == pytest.approx(1_000.0)
```

- [ ] **Step 2: Run the metric-contract test and verify the current kilometre CRS fails**

Run: `pytest -q tests/test_thin_provider_adapters.py -k knmi_rtcor_latest_frame`

Expected: FAIL because the decoded KNMI CRS axis converts kilometres to 1,000 metres.

- [ ] **Step 3: Normalize the KNMI projected CRS to metre units**

Add this helper near the radar decode helpers and use its returned CRS for the KNMI forward/inverse transformers and `ThinRadarField.crs`:

```python
def _projected_crs_in_metres(value: Any) -> CRS:
    from pyproj import CRS

    source = CRS.from_user_input(value)
    if not source.is_projected:
        raise ValueError("radar_crs_must_be_projected")
    parameters = source.to_dict()
    parameters["units"] = "m"
    metric = CRS.from_dict(parameters)
    if not metric.axis_info or not math.isclose(
        metric.axis_info[0].unit_conversion_factor, 1.0,
    ):
        raise ValueError("radar_crs_metre_normalization_failed")
    return metric
```

For KNMI, convert `pixel_x_km`, `pixel_y_km`, `column_offset_km`, and `row_offset_km` to metres before all pixel transforms. Remove the `knmi_radar` multiply/divide branches from `build_target_radar_frame()` and `ThinRadarTracker.tracks()`.

- [ ] **Step 4: Add a provider-neutral tracking regression**

```python
def test_tracker_needs_no_provider_specific_coordinate_scale():
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 20.0, "BE")
    field = ThinRadarField(
        "knmi_radar", "rtcor", 100.0, "EPSG:3857",
        -2_000.0, 2_000.0, 1_000.0, 1_000.0,
        np.ones((4, 4), dtype=float), np.ones((4, 4), dtype=bool),
    )
    frame = build_target_radar_frame(field, target, maximum_pixels=16)
    assert frame.target_x_m == pytest.approx(0.0)
    assert frame.frame.grid.x_resolution_m == pytest.approx(1_000.0)
```

- [ ] **Step 5: Run focused adapter and tracking tests**

Run: `pytest -q tests/test_thin_provider_adapters.py -k 'knmi or radar_field' tests/test_thin_radar_tracking.py`

Expected: PASS.

- [ ] **Step 6: Commit the metric georeferencing seam**

```bash
git add storm_tracker_worker/thin_provider_adapters.py storm_tracker_worker/thin_radar_tracking.py tests/test_thin_provider_adapters.py tests/test_thin_radar_tracking.py
git commit -m "fix: normalize thin radar fields to metric CRS"
```

### Task 2: Build the Stable Regional Grid and Conservative Projection

**Files:**
- Create: `storm_tracker_worker/thin_regional_radar_fusion.py`
- Create: `tests/test_thin_regional_radar_fusion.py`
- Reuse: `storm_tracker_worker/tracking_core/engine/analysis_grid.py`

**Interfaces:**
- Consumes: `ThinAcquisitionRegion`, metre-based `ThinRadarField`, and `AnalysisGrid`.
- Produces:
  - `RegionalRadarLayer(provider_id: str, product_id: str, observed_at: float, grid: AnalysisGrid, rain_rate_mm_h: np.ndarray, valid_mask: np.ndarray, support_mask: np.ndarray, coverage_fraction: np.ndarray, native_resolution_m: float)`
  - `build_regional_analysis_grid(region, fields, *, minimum_resolution_m=500.0, maximum_cells=250_000) -> AnalysisGrid`
  - `project_radar_field(field, grid) -> RegionalRadarLayer`

- [ ] **Step 1: Write failing deterministic-grid tests**

```python
def test_region_grid_is_metric_stable_and_capacity_bounded():
    region = _region(latitude=50.85, longitude=4.35, radius_km=80.0)
    fine = _field(crs="EPSG:3857", resolution=250.0)
    first = build_regional_analysis_grid(region, (fine,))
    second = build_regional_analysis_grid(region, (fine,))
    assert first == second
    assert first.crs.startswith("+proj=aeqd")
    assert first.x_resolution_m >= 500.0
    assert first.width * first.height <= 250_000
    assert first.extent_edges_m[0] <= -80_000.0
    assert first.extent_edges_m[2] >= 80_000.0

@pytest.mark.parametrize("longitude,latitude", [(179.95, 10.0), (-179.95, 10.0), (20.0, 75.0)])
def test_region_grid_handles_dateline_and_high_latitude(longitude, latitude):
    grid = build_regional_analysis_grid(
        _region(latitude=latitude, longitude=longitude, radius_km=50.0),
        (_field(),),
    )
    assert all(math.isfinite(value) for value in grid.extent_edges_m)
    assert grid.width * grid.height <= 250_000
```

- [ ] **Step 2: Run the new grid tests and verify imports fail**

Run: `pytest -q tests/test_thin_regional_radar_fusion.py -k region_grid`

Expected: FAIL with missing module or missing function.

- [ ] **Step 3: Implement deterministic AEQD grid construction**

Create the local CRS as:

```python
crs = (
    f"+proj=aeqd +lat_0={region.center_latitude:.8f} "
    f"+lon_0={region.center_longitude:.8f} +datum=WGS84 +units=m +no_defs"
)
```

Choose the finest finite native resolution, clamp to 500 m, compute square coverage for `observation_radius_km`, and repeatedly increase resolution with `ceil(sqrt(cell_count / 250_000))` until the complete extent fits. Derive `grid_id` from canonical JSON containing region id, stable centre, radius, CRS, resolution, width, and height.

- [ ] **Step 4: Write failing projection-state and conservation tests**

```python
def test_projection_keeps_zero_nodata_and_outside_distinct():
    field = _field(
        values=np.array([[0.0, 4.0], [8.0, 0.0]]),
        valid=np.array([[True, False], [True, True]]),
        origin=(-1_000.0, 1_000.0),
        resolution=1_000.0,
    )
    grid = _grid(width=3, height=2, west=-1_000.0, north=1_000.0, resolution=1_000.0)
    layer = project_radar_field(field, grid)
    assert layer.support_mask[0, 0]
    assert layer.valid_mask[0, 0]
    assert layer.rain_rate_mm_h[0, 0] == 0.0
    assert layer.support_mask[0, 1]
    assert not layer.valid_mask[0, 1]
    assert not layer.support_mask[0, 2]

def test_projection_conserves_constant_rate_across_resolution_change():
    layer = project_radar_field(
        _field(values=np.full((4, 4), 6.0), valid=np.ones((4, 4), bool), resolution=500.0),
        _grid(width=2, height=2, west=0.0, north=2_000.0, resolution=1_000.0),
    )
    assert np.allclose(layer.rain_rate_mm_h[layer.valid_mask], 6.0)
    assert np.all(layer.coverage_fraction[layer.valid_mask] == pytest.approx(1.0))
```

- [ ] **Step 5: Implement bounded conservative area overlap**

For each source pixel in the inverse-projected target bounding box, transform its four edge corners into the target CRS, clip the resulting convex polygon against each intersected target pixel rectangle with Sutherland-Hodgman clipping, and accumulate `overlap_area * rain_rate`. Maintain separate support area and valid area arrays. Set `support_mask = support_area > epsilon`, `valid_mask = nearest_source_valid & (valid_area > epsilon)`, and divide the integral by valid area only where valid. Reject non-projected or non-metre source CRS with `RegionalRadarProjectionError("unsupported_metric_crs")`.

- [ ] **Step 6: Add the half-pixel physical-position regression**

```python
def test_reprojected_object_stays_within_one_analysis_pixel():
    source = _offset_projected_blob()
    grid = build_regional_analysis_grid(_region(), (source,))
    layer = project_radar_field(source, grid)
    row, column = np.unravel_index(np.nanargmax(layer.rain_rate_mm_h), layer.rain_rate_mm_h.shape)
    x, y = grid.pixel_center(row, column)
    assert math.hypot(x - EXPECTED_X_M, y - EXPECTED_Y_M) <= grid.x_resolution_m
```

- [ ] **Step 7: Run the full pure raster module tests**

Run: `pytest -q tests/test_thin_regional_radar_fusion.py -k 'grid or projection or reprojected'`

Expected: PASS with no network access.

- [ ] **Step 8: Commit grid and projection**

```bash
git add storm_tracker_worker/thin_regional_radar_fusion.py tests/test_thin_regional_radar_fusion.py
git commit -m "feat: project radar fields onto regional grids"
```

### Task 3: Fuse the Best Three Quantitative Radars Per Pixel

**Files:**
- Modify: `storm_tracker_worker/thin_regional_radar_fusion.py`
- Modify: `tests/test_thin_regional_radar_fusion.py`

**Interfaces:**
- Consumes: `ThinAcquisitionRegion`, `Mapping[str, ProviderResult]`, `evaluated_at`, `PROVIDER_FUSION_AGE_SECONDS`, and the Task 2 projection functions.
- Produces:
  - `RegionalFusionSummary(region_id, grid_id, status, mode, evaluated_at, field_observed_at, contributors, excluded, standby, reason_codes, coverage_fraction, mean_confidence, disagreement_fraction)` with `as_dict()`; status is `current`, `degraded`, or `onvoldoende_data`, and mode is `fused`, `single_source`, or `fallback`.
  - `FusedRegionalRadarField(region_id: str, field: ThinRadarField | None, confidence: np.ndarray, contributor_mask: np.ndarray, summary: RegionalFusionSummary)`.
  - `fuse_regional_radar_fields(region, provider_results, *, evaluated_at) -> FusedRegionalRadarField`.

- [ ] **Step 1: Write failing weighted-fusion tests**

```python
def test_two_sources_fuse_valid_overlap_with_provenance():
    result = fuse_regional_radar_fields(
        _region(),
        {
            "local": _provider(_field(provider="local", values=_constant(4.0), observed_at=990.0)),
            "wide": _provider(_field(provider="wide", values=_constant(8.0), observed_at=970.0)),
        },
        evaluated_at=1_000.0,
    )
    assert result.field is not None
    assert 4.0 < result.field.rain_rate_mm_h[result.field.valid_mask].mean() < 8.0
    assert result.summary.contributors == ("local", "wide")
    assert result.summary.mode == "fused"
    assert result.contributor_mask.dtype == np.uint64
```

- [ ] **Step 2: Run the test and verify the fusion API is absent**

Run: `pytest -q tests/test_thin_regional_radar_fusion.py -k two_sources`

Expected: FAIL with missing fusion symbols.

- [ ] **Step 3: Implement explicit weight factors and fused output**

Use these deterministic factors for every valid candidate pixel:

```python
age_factor = max(0.10, 1.0 - age_seconds / maximum_age_seconds)
resolution_factor = min(1.0, grid.x_resolution_m / layer.native_resolution_m)
coverage_factor = np.clip(layer.coverage_fraction, 0.0, 1.0)
weight = age_factor * resolution_factor * coverage_factor
```

Being present as a `ThinRadarField` is the quantitative-product gate; runtime must be `ready=True` and `status == "operational"`. Generate `fusion:<region_id>` as provider id and `regional-rain-rate` as product id. Set `observed_at` to the newest contributing field time. Preserve zero values through the boolean validity mask.

- [ ] **Step 4: Write the top-three and disagreement regressions**

```python
def test_only_best_three_sources_contribute_per_pixel():
    providers = {
        name: _provider(_field(provider=name, values=_constant(value), observed_at=stamp))
        for name, value, stamp in (
            ("a", 1.0, 999.0), ("b", 2.0, 998.0),
            ("c", 3.0, 997.0), ("d", 100.0, 900.0),
        )
    }
    result = fuse_regional_radar_fields(_region(), providers, evaluated_at=1_000.0)
    assert result.summary.contributors == ("a", "b", "c")
    assert result.summary.standby == {"d": "lower_ranked_standby"}
    assert np.max(result.field.rain_rate_mm_h) < 4.0

def test_disagreement_is_reported_without_silent_source_removal():
    result = fuse_regional_radar_fields(
        _region(), _three_disagreeing_providers(), evaluated_at=1_000.0,
    )
    assert result.summary.disagreement_fraction > 0.0
    assert "provider_disagreement" in result.summary.reason_codes
    assert len(result.summary.contributors) == 3
```

- [ ] **Step 5: Implement deterministic per-pixel ranking and bounded disagreement**

Sort candidates per pixel by descending weight, descending `observed_at`, then ascending provider id; retain three. Compute a weighted median, set the winsorization distance to `max(5.0, 3.0 * max(1.0, median))`, clip only for the weighted mean, and mark every clipped pixel as disagreement. Keep each selected source in the contributor mask and provenance.

- [ ] **Step 6: Add exclusion tests for age, runtime state, coverage, and empty data**

```python
@pytest.mark.parametrize(
    "provider,reason",
    [
        (_stale_provider(), "field_stale"),
        (_error_provider(), "runtime_not_operational"),
        (_outside_provider(), "outside_region_coverage"),
    ],
)
def test_unusable_provider_is_excluded_with_fixed_reason(provider, reason):
    result = fuse_regional_radar_fields(
        _region(), {provider.provider_id: provider}, evaluated_at=1_000.0,
    )
    assert result.field is None
    assert result.summary.excluded[provider.provider_id] == reason
    assert result.summary.status == "onvoldoende_data"
```

- [ ] **Step 7: Run all regional fusion tests**

Run: `pytest -q tests/test_thin_regional_radar_fusion.py`

Expected: PASS.

- [ ] **Step 8: Commit quality-weighted fusion**

```bash
git add storm_tracker_worker/thin_regional_radar_fusion.py tests/test_thin_regional_radar_fusion.py
git commit -m "feat: fuse best regional radar contributors"
```

### Task 4: Wire Non-Blocking Regional Fusion into Tracking and Fallback

**Files:**
- Create: `storm_tracker_worker/thin_regional_radar_runtime.py`
- Create: `tests/test_thin_regional_radar_runtime.py`
- Modify: `storm_tracker_worker/thin_operational_runtime.py:164-210,345-454`
- Modify: `storm_tracker_worker/thin_mqtt_service.py:170-215`
- Modify: `tests/test_thin_operational_runtime.py`

**Interfaces:**
- Consumes: Task 3 `fuse_regional_radar_fields`, `ThinRegionPlan`, current `ProviderResult` cache, and the existing `ThinRadarTracker`.
- Produces:
  - `ThinRegionalRadarFusionCoordinator(max_workers: int = 2)`.
  - `poll() -> Mapping[str, FusedRegionalRadarField]` returns completed work without waiting.
  - `submit(region_plan, provider_results, *, evaluated_at) -> None` schedules only changed region/input fingerprints.
  - `close() -> None` cancels pending work and shuts down both threads.
  - `ThinCycleResult.regional_radar: tuple[RegionalFusionSummary, ...]`.

- [ ] **Step 1: Write failing non-blocking coordinator tests**

```python
def test_poll_never_waits_for_pending_fusion():
    gate = threading.Event()
    coordinator = ThinRegionalRadarFusionCoordinator(
        max_workers=2,
        fuse=lambda *args, **kwargs: gate.wait(5.0),
    )
    coordinator.submit(_plan(), _providers(), evaluated_at=1_000.0)
    started = time.monotonic()
    assert coordinator.poll() == {}
    assert time.monotonic() - started < 0.05
    gate.set()
    coordinator.close()

def test_coordinator_uses_at_most_two_compute_threads():
    coordinator = ThinRegionalRadarFusionCoordinator(max_workers=2)
    assert coordinator.max_workers == 2
    coordinator.close()
```

- [ ] **Step 2: Run coordinator tests and verify the module is absent**

Run: `pytest -q tests/test_thin_regional_radar_runtime.py`

Expected: FAIL with missing module.

- [ ] **Step 3: Implement fingerprinted two-thread scheduling**

The fingerprint must hash region id/centre/radius plus sorted `(provider_id, product_id, observed_at, shape)` tuples. `submit()` skips identical in-flight or completed fingerprints. `poll()` calls `future.done()` only, converts exceptions into a `RegionalFusionSummary` with `mode="fallback"` and reason `fusion_error`, and never calls blocking `future.result()` unless `done()` is true.

- [ ] **Step 4: Write failing runtime primary/fallback tests**

```python
@pytest.mark.asyncio
async def test_completed_fusion_is_primary_tracking_source():
    runtime = _runtime_with_completed_regional_field()
    runtime.apply_desired_state(_state())
    result = await runtime.run_cycle()
    assert result.nowcast.storm_cells
    assert {cell.provider_id for cell in result.nowcast.storm_cells} == {"fusion:region:home"}
    assert result.regional_radar[0].mode == "fused"

@pytest.mark.asyncio
async def test_pending_fusion_uses_marked_per_provider_fallback():
    runtime = _runtime_with_pending_fusion_and_native_field()
    runtime.apply_desired_state(_state())
    result = await runtime.run_cycle()
    assert any(cell.provider_id == "dwd_radolan" for cell in result.nowcast.storm_cells)
    assert result.regional_radar[0].mode == "fallback"
    assert result.regional_radar[0].reason_codes == ("fusion_pending",)
```

- [ ] **Step 5: Replace the inline provider tracking loop with regional-first selection**

At the start of the tracking stage, call `completed = coordinator.poll()`, then `coordinator.submit(...)`. For every region:

1. use a completed, current field whose fingerprint matches the active region/input set;
2. advance it once per target in that region through `ThinRadarTracker`;
3. otherwise run the existing provider-field loop only for that region's targets;
4. create a fallback summary with `fusion_pending`, `fusion_error`, or `onvoldoende_data`;
5. deduplicate cells by `cell_id` as today.

No fused result may suppress scalar observations or providers for another region.

- [ ] **Step 6: Close the coordinator during MQTT service shutdown**

Add `ThinOperationalRuntime.close()` and call it from `ThinMqttService.stop()` after cancelling the cycle task and before disconnecting MQTT.

- [ ] **Step 7: Run coordinator, runtime, nowcast, and MQTT regressions**

Run: `pytest -q tests/test_thin_regional_radar_runtime.py tests/test_thin_operational_runtime.py tests/test_thin_nowcast.py tests/test_thin_mqtt_service.py`

Expected: PASS; the heartbeat thread tests continue to pass while fusion is pending.

- [ ] **Step 8: Commit runtime integration**

```bash
git add storm_tracker_worker/thin_regional_radar_runtime.py storm_tracker_worker/thin_operational_runtime.py storm_tracker_worker/thin_mqtt_service.py tests/test_thin_regional_radar_runtime.py tests/test_thin_operational_runtime.py tests/test_thin_mqtt_service.py
git commit -m "feat: track nonblocking regional radar fusion"
```

### Task 5: Publish and Persist Compact Fusion Provenance

**Files:**
- Modify: `storm_tracker_worker/tracking_core/operational_snapshot.py:150-180,360-430,866-930`
- Modify: `custom_components/storm_tracker_v3/operational_snapshot.py:150-180,360-430,866-930`
- Modify: `storm_tracker_worker/thin_operational_runtime.py:455-905`
- Modify: `storm_tracker_worker/thin_postgis.py:80-115,219-435`
- Create: `storm_tracker_worker/postgis_migrations/0004_regional_radar_fusion.sql`
- Create: `tests/test_postgis_migrations.py`
- Modify: `tests/test_operational_snapshot.py`
- Modify: `tests/test_thin_operational_runtime.py`
- Modify: `tests/test_thin_postgis.py`

**Interfaces:**
- Consumes: `RegionalFusionSummary.as_dict()` and `ThinCycleResult.regional_radar`.
- Produces: an optional backward-compatible root snapshot collection `radar_fusion`, GeoJSON `metadata.radar_fusion`, and `stv3_fusion_cycle.regional_radar jsonb`.

- [ ] **Step 1: Write failing snapshot contract tests**

```python
def test_snapshot_accepts_compact_regional_radar_fusion_collection():
    snapshot = _valid_snapshot()
    snapshot["radar_fusion"] = {
        "complete": True,
        "overflow_count": 0,
        "items": [{
            "region_id": "region:home",
            "grid_id": "grid:abc",
            "status": "current",
            "mode": "fused",
            "contributors": ["dwd_radolan", "knmi_radar"],
            "excluded": {},
            "standby": {},
            "reason_codes": [],
            "coverage_fraction": 0.92,
            "mean_confidence": 0.84,
            "evaluated_at": 1_000.0,
            "field_observed_at": 990.0,
        }],
    }
    assert validate_snapshot(snapshot)["radar_fusion"]["items"][0]["mode"] == "fused"
```

- [ ] **Step 2: Run the snapshot test and verify the unknown key fails**

Run: `pytest -q tests/test_operational_snapshot.py -k regional_radar_fusion_collection`

Expected: FAIL because `radar_fusion` is not accepted by the exact root contract.

- [ ] **Step 3: Add the bounded backward-compatible snapshot extension in both copies**

Permit `radar_fusion` as one optional root key so historical snapshots still decode. Validate at most `MAX_REGIONS` items, finite fractions in `[0, 1]`, sorted unique provider/reason lists, bounded provider-to-reason mappings for `excluded` and `standby`, safe ids, statuses `current`, `degraded`, or `onvoldoende_data`, and modes `fused`, `single_source`, or `fallback`. Keep both operational snapshot files byte-equivalent after the edit.

- [ ] **Step 4: Add compact snapshot and GeoJSON metadata in the thin runtime**

Populate the collection from sorted summaries. In GeoJSON, add only this compact form under `metadata["radar_fusion"]`; do not add arrays, masks, or pixel values. Assert encoded GeoJSON remains under `MAX_GEOJSON_PAYLOAD_BYTES` in the existing payload-bound test.

- [ ] **Step 5: Write the PostGIS migration**

```sql
BEGIN;

ALTER TABLE public.stv3_fusion_cycle
    ADD COLUMN IF NOT EXISTS regional_radar jsonb NOT NULL DEFAULT '[]'::jsonb;

INSERT INTO stv3_meta.schema_migrations(migration_id, contract)
VALUES('0004_regional_radar_fusion', 'stv3-regional-radar-fusion-summary-v1')
ON CONFLICT(migration_id) DO NOTHING;

COMMIT;
```

- [ ] **Step 6: Write a failing PostGIS summary test**

```python
@pytest.mark.asyncio
async def test_writer_persists_compact_regional_radar_summary_without_raster():
    writer, factory = _writer_and_factory()
    cycle, nowcast = _cycle_and_nowcast(100.0)
    summary = _regional_summary()
    await writer.write_cycle(cycle, nowcast, regional_radar=(summary,))
    fusion_row = next(row for sql, row in factory.statements if sql.startswith("INSERT INTO stv3_fusion_cycle"))
    payload = json.loads(fusion_row[-1])
    assert payload[0]["region_id"] == "region:home"
    assert "rain_rate_mm_h" not in json.dumps(payload)
    assert "valid_mask" not in json.dumps(payload)
```

- [ ] **Step 7: Add a migration-content regression**

```python
from pathlib import Path


def test_regional_radar_migration_is_compact_and_registered():
    sql = Path(
        "storm_tracker_worker/postgis_migrations/0004_regional_radar_fusion.sql"
    ).read_text(encoding="utf-8")
    normalized = " ".join(sql.split()).lower()
    assert "add column if not exists regional_radar jsonb" in normalized
    assert "0004_regional_radar_fusion" in sql
    assert "raster" not in normalized
    assert "bytea" not in normalized
```

- [ ] **Step 8: Extend the writer queue and fusion upsert**

Add `regional_radar jsonb` to `_INSERT_FUSION`, add a keyword-only `regional_radar: tuple[RegionalFusionSummary, ...] = ()` argument to `write_cycle`, include it in queued retry tuples, and serialize only `summary.as_dict()` values. Preserve the queue length, retry, rollback, and no-runtime-DDL behavior.

- [ ] **Step 9: Run snapshot, runtime, PostGIS, and migration regressions**

Run: `pytest -q tests/test_operational_snapshot.py tests/test_thin_operational_runtime.py tests/test_thin_postgis.py tests/test_postgis_migrations.py`

Expected: PASS.

- [ ] **Step 10: Commit compact publication and persistence**

```bash
git add storm_tracker_worker/tracking_core/operational_snapshot.py custom_components/storm_tracker_v3/operational_snapshot.py storm_tracker_worker/thin_operational_runtime.py storm_tracker_worker/thin_postgis.py storm_tracker_worker/postgis_migrations/0004_regional_radar_fusion.sql tests/test_operational_snapshot.py tests/test_thin_operational_runtime.py tests/test_thin_postgis.py tests/test_postgis_migrations.py
git commit -m "feat: persist regional radar fusion summaries"
```

### Task 6: Release, NAS Deployment, and Live Acceptance

**Files:**
- Modify: `storm_tracker_worker/config.yaml`
- Modify: `storm_tracker_worker/__init__.py`
- Modify: `custom_components/storm_tracker_v3/const.py`
- Modify: `custom_components/storm_tracker_v3/manifest.json`
- Modify: `custom_components/storm_tracker_v3/__init__.py`
- Modify: `CHANGELOG.md`
- Create: `docs/DEPLOY_2026-09-25_1.14.140_1.18.48.md`

**Interfaces:**
- Consumes: all prior tasks, release builder, fixed NAS SSH installer, NAS Home Assistant MCP, and migration `0004_regional_radar_fusion.sql`.
- Produces: integration `1.14.140`, worker `1.18.48`, applied PostGIS summary migration, live regional fused or explicitly fallback results, and rollback evidence.

- [ ] **Step 1: Run the focused pre-release suite**

Run:

```bash
pytest -q \
  tests/test_thin_provider_adapters.py \
  tests/test_thin_radar_tracking.py \
  tests/test_thin_regional_radar_fusion.py \
  tests/test_thin_regional_radar_runtime.py \
  tests/test_thin_operational_runtime.py \
  tests/test_thin_nowcast.py \
  tests/test_thin_postgis.py \
  tests/test_operational_snapshot.py \
  tests/test_thin_mqtt_service.py
```

Expected: PASS.

- [ ] **Step 2: Run the worker/integration regression gate**

Run:

```bash
python3 scripts/build_release_candidate.py \
  --integration-root custom_components/storm_tracker_v3 \
  --worker-root storm_tracker_worker \
  --integration-version 1.14.140 \
  --worker-version 1.18.48 \
  --output-directory .analysis/release-1.14.140-1.18.48-20260925
```

Expected: all mandatory tests pass; only documented platform skips are accepted.

- [ ] **Step 3: Bump paired versions and build id**

Set worker `1.18.48` in `storm_tracker_worker/config.yaml` and `storm_tracker_worker/__init__.py`; set integration `1.14.140` in `const.py` and `manifest.json`; set build id exactly `integration-1.14.140-worker-1.18.48`; add one changelog entry describing regional top-three raster fusion, marked fallback, and compact PostGIS provenance.

- [ ] **Step 4: Build and verify deploy archives**

Build exact archive names:

```text
storm_tracker_v3_v1.14.140_deploy.zip
storm_tracker_worker_v1.18.48_deploy.tar.gz
```

Verify filenames, internal roots, embedded versions, evidence, and SHA-256 values before contacting the NAS.

- [ ] **Step 5: Apply the PostGIS migration before starting the new worker**

Execute `0004_regional_radar_fusion.sql` once against `storm-tracker-postgis` with the existing administrative migration route. Verify read-only that `stv3_meta.schema_migrations` contains `0004_regional_radar_fusion` and `stv3_fusion_cycle.regional_radar` exists. Do not change other tables or database retention.

- [ ] **Step 6: Deploy only through the fixed NAS route**

Use `scripts/install_ha_release_pair_ssh.py` with host `192.168.0.15`, user `hassio`, and `/home/ikke/.ssh/codex_nas_ha_deploy`. Create the prescribed rollback backup, stop only Storm Tracker, install the worker and integration archives, reload the Store through NAS MCP `supervisor/api /store/reload`, start worker first, and wait for a fresh heartbeat with `job_admission_open=true` before enabling/restarting the integration.

- [ ] **Step 7: Prove live regional behavior**

Read-only verify:

1. worker `1.18.48`, integration `1.14.140`, PostGIS `current`, queue zero, and no failed cycle;
2. current Belgian warning region lists the actually applicable quantitative fields rather than a fixed provider list;
3. `radar_fusion` reports `fused` or `single_source`, or an explicit fallback reason while the old cells remain available;
4. at least two consecutive new radar frames produce one `fusion:region:home` cell set and retain direction/speed/footprint passage;
5. `regional_radar` JSONB contains summaries but no raster arrays;
6. heartbeat remains current during an intentionally delayed synthetic fusion test or a naturally long live fusion;
7. Home Assistant map and result sensors remain bounded and responsive.

- [ ] **Step 8: Record live evidence and commit the release**

Write versions, archive hashes, backup id, migration proof, heartbeat times, regional source selection, fusion/fallback mode, PostGIS row proof, and live map result to `docs/DEPLOY_2026-09-25_1.14.140_1.18.48.md`.

```bash
git add storm_tracker_worker/config.yaml storm_tracker_worker/__init__.py custom_components/storm_tracker_v3/const.py custom_components/storm_tracker_v3/manifest.json custom_components/storm_tracker_v3/__init__.py CHANGELOG.md docs/DEPLOY_2026-09-25_1.14.140_1.18.48.md
git commit -m "release: deploy regional radar fusion"
```

## Final Verification

- [ ] Run `git diff --check` and confirm no whitespace errors.
- [ ] Run `pytest -q tests/test_thin_regional_radar_fusion.py tests/test_thin_regional_radar_runtime.py tests/test_thin_operational_runtime.py tests/test_thin_postgis.py tests/test_operational_snapshot.py` after all integration edits.
- [ ] Confirm `rg -n 'T[B]D|T[O]DO|implement l[a]ter|fill in d[e]tails|Similar to T[a]sk' docs/superpowers/plans/2026-09-25-dynamic-regional-raster-fusion.md` returns no matches.
- [ ] Compare the live worker options with the preserved `dedicated_nas_fast` values and record any deviation as release failure.
- [ ] Keep the per-provider fallback in place; removing it requires a separate live-acceptance decision after this release.
