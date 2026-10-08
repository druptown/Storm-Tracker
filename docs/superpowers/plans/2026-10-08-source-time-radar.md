# Source time radar implementation plan

> For agentic workers: use superpowers:executing-plans inline. The user explicitly
> authorizes immediate implementation and NAS deployment after the design.

**Goal:** Stop estimating motion on asynchronous composites and keep a stable,
traceable full-field primary prediction with explicit source conflicts.

**Architecture:** Native source histories feed bounded local flow and reverse
target tracing, on common future valid times. Observation fusion remains for
the map only. Source selection retains supported prior primary sources, then
uses documented resolution/recency proxy ordering, never averaging timings.

**Tech Stack:** Existing Python, NumPy, pyproj, two-thread coordinator, MQTT/PostGIS.

**Spec:** ../specs/2026-10-08-source-time-aware-radar-fusion-design.md

## Global constraints

Only NAS-HA 192.168.0.15. Keep worker 105 base ecd2d49 and integration 1.14.156.
No broad/historical suites, new dependencies, source polling, credentials,
importer changes, trackers or Core/VM restarts. Keep two CPU threads and month
retention. Preserve running Oldenburg experiment and dirty main checkout.
Only focused tests. User overrides additional review/permission pauses and
forbids subagents; perform the final review separately inline.

## Review focus

- A corrected same-time field must not create a new motion interval.
- A regional revision change must not retain another region's source flow.
- Unknown early support must not become an exact first-rain arrival.
- A source conflict must retain a primary result without implying calibration.
- Repeated ticks must not refresh the original age of a held prediction.

## Task 1 Source field motion and target samples

Files: create `storm_tracker_worker/thin_source_radar_forecast.py` and
`tests/test_thin_source_radar_forecast.py`.

Interfaces: `SourceRadarForecastEngine.advance(field, layer, received_at)`
returns immutable `SourceRadarMotion`; `trace_target(motion, latitude,
longitude, issued_at)` returns 121 nullable quantitative samples plus native
pair provenance; `select_primary(candidates, previous_provider)` preserves
stable source choice and conflicts.

- [ ] Write failing tests for real own-clock pairs, future/stale/incompatible
  input, local support, elapsed age, weak rain, gaps/censoring, conflict and
  stable primary selection. Use hand-derived metric-grid examples.
- [ ] Run `python -m pytest tests/test_thin_source_radar_forecast.py -q` and
  confirm missing-feature failures before implementation.
- [ ] Implement existing sparse LK on 128-pixel tiles with stride 96 and
  24 features, nearest eight IDW within 60 km, minimum three vectors,
  maximum RMS 8 m/s, floor 1500 m, RK2 steps <=30 s and quarter-pixel travel.
  Keep max pair gap 1200 s, source freshness unchanged, horizon 7200 s,
  step 60 s, at most three sources per region and bounded history/cache.
- [ ] Run the same focused tests, verify red to green, commit these files.

## Task 2 Regional integration without fused motion

Files: modify `thin_regional_radar_fusion.py`, `thin_regional_radar_runtime.py`,
`thin_operational_runtime.py`; test source clock routing and coordinator.

Interfaces: fusion exposes its projected native layers without extra projection;
the existing two-thread coordinator owns per-region-revision source engines.
`FusedRegionalRadarField.source_motions` is immutable completed source output.
Operational object tracking consumes only corresponding native sources.

- [ ] Add failing tests proving composite input never reaches motion tracking,
  duplicate products reuse flow, changed revision invalidates flow, and source
  identity is independent of layer bits. Run those tests to observe failures.
- [ ] Add native layer references and explicit contributor-index/time mapping;
  calculate source flow inside the existing executor, not the MQTT loop.
- [ ] Route source object tracking and field trajectories using their own
  clocks. Preserve observation-map raster and normal capture bindings.
- [ ] Run directly affected fusion/runtime tests; commit only touched files.

## Task 3 Prediction continuity and immutable provenance

Files: modify `thin_nowcast.py`, `thin_postgis.py`, `thin_mqtt_service.py` and
bounded existing forecast publication/consumer code as needed.

Interfaces: `TargetNowcast.field_forecast` carries primary status, original
issue/product time, source alternatives/conflict, nullable local samples and
input fingerprints; main arrival/end uses primary full-field intervals while
existing component passages remain separately available. Existing forecast
metadata persists this field; raw cycle context persists motion/policy support.

- [ ] Add failing tests for weak field overriding a short component duration,
  conflict keeping primary, coasting age not refreshed, expired estimates only
  historical, target moves invalidating held estimates, source fingerprints
  and MQTT/PostGIS readback. Run to observe failure.
- [ ] Integrate full-field outcome and primary continuity. Unknown support
  keeps previous estimate visibly labelled not current; absent earlier input
  is honestly unknown. No silent finite exit or invented dry interval.
- [ ] Publish compact field forecast metadata and retain native references,
  code/policy and actual vector support in existing raw cycle context.
- [ ] Run focused forecast/storage/publication tests and commit touched files.

## Task 4 Review build and NAS deployment

- [ ] Perform a separate inline whole-change review against the spec and
  focused failures, including capacity and causal storage. Fix actual findings
  test-first, without broad suite or source parameter search.
- [ ] Build worker next available version and unchanged integration via the
  existing release builder; verify names, internals and hashes.
- [ ] Follow HA_DEPLOY_EN_HERSTELPROCEDURE.md: read live state, confirmed recent
  full backup plus scoped rollback, official integration disable and worker
  stop, fixed installer, Store reload, official App update/start, fresh worker
  heartbeat/admission, integration load, no Core/VM restart.
- [ ] Verify ordinary unique source refreshes, primary/conflict metadata,
  separate Home/fictitious regions, current queue/PostGIS and independent
  field/issue readback. Preserve mqtt_dump baseline after any bounded dump.
  Report qualification limits, not 90% skill or all-provider success.

The existing Oldenburg run is scored once by its already scheduled continuation;
do not start a duplicate replay or overwrite experiment artifacts.
