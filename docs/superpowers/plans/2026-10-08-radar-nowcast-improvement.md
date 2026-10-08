# Radar Nowcast Improvement Implementation Plan

> **For agentic workers:** Implement the stages in order in an isolated checkout, with focused failing regressions before each behavior change. Use the approved source-clock design and this plan together. A stage is complete only after its evidence gate passes.

**Goal:** Give each target a traceable arrival and end estimate for the whole rain zone, with separate intensity peaks and honest uncertainty, while increasing supported forecasts without increasing misleading wet forecasts.

**Architecture:** Keep quantitative native radar sources on their own product clocks. Estimate source-specific motion, advect the entire valid rain field to common future times, then choose a primary source while retaining alternatives and conflicts. Derive target rain intervals from the field; keep tracked cores for intensity. Add motion continuity and probabilistic output only after the deterministic route is measured.

**Tech Stack:** Existing Python/NumPy/pyproj worker, Home Assistant integration, MQTT, PostGIS native and forecast archive. No new runtime dependency is needed for the first four stages.

**Spec:** `docs/superpowers/specs/2026-10-08-source-time-aware-radar-fusion-design.md`; later uncertainty work follows the explicit decisions in stages 5–6 below. The original Oldenburg products and issue records remain immutable comparators, not training labels or restrictions on new forecasts.

## Global Constraints

- NAS Home Assistant `192.168.0.15` only; never develop or deploy to the NUC.
- Preserve `dedicated_nas_fast`, CPU limit 200%, nice 0, and two compute threads; do not change provider polling cadence.
- Keep each target and region generation isolated. Reuse region-level motion computation; trace each target separately.
- Retain a maximum of one calendar month of meteorological history. Store the source IDs, valid and available times, policy version, support and selection reason of every issued prediction.
- Preserve the original issues and source arrays. Future products may verify a prediction but must never enter its calculation.
- Use focused regressions, then the deployment and recovery runbook. Do not equate successful installation with meteorological accuracy.
- No invented QPE from display colors, no replacement of nodata by zero, no claim of a calibrated probability before out-of-sample calibration.

## Review Focus

The owner of each stage adds a focused test for these cases: a same-time source revision must not count as new motion; a source gap or target relocation must not reuse another history; nodata adjacent to zero must stay unknown; a four-hour zone must not acquire a fabricated two-hour end; correlated ensemble members must not be counted as independent evidence.

## Stage 0: Freeze comparison and causes

**Files:** Read the existing `source-clock-corrected` and `legacy-0498-comparison` artifacts under `.analysis/oldenburg-replay-20261008/`. Add a small scoring/diagnostic report beside them without changing their manifests.

- [ ] Fix a comparison universe: the same 68 historical issue times, actual input availability at each issue, later source-specific target radar observations, and leads 5–30, 30–60, 60–90 and 90–120 minutes. Score `>0` and `>=0.1 mm/u` separately.
- [ ] For every unknown sample, distinguish missing source pair, insufficient local vectors, excessive vector spread, source gap, stale product, nodata, outside coverage, and horizon censoring. Count them by lead and by genuinely distinct issue; inspect the `8 m/s` rejection before changing it.
- [ ] Report supported fraction, hits, misses, false-wet, onset error to the observation bracket, exit error/censoring, issue-to-issue onset jumps, and runtime. Nearby gauges supply separate point evidence, not exact Oldenburg target onset.

**Gate:** The causal baseline, immutable hashes, and the number of independent events are recorded. One Oldenburg event cannot establish a 90% success rate.

## Stage 1: Complete source-clock field route

**Files:** Continue the already isolated `codex/source-time-radar` branch, especially `storm_tracker_worker/thin_source_radar_forecast.py`, the worker's existing regional cycle and publication adapters, and focused `tests/test_thin_source_radar_forecast.py` tests. Do not rebuild the already committed primitives.

- [ ] Write failing integration tests: KNMI valid at 01:20 and another source at 01:25 both forecast for 01:35 using their own elapsed time; a provider switch cannot form an optical-flow pair; equal product times do not create new evidence; distinct targets share region motion yet keep separate trajectories.
- [ ] Wire the existing `SourceRadarForecastEngine`, `trace_target`, `rain_intervals`, and `select_primary` into the actual worker cycle. Cache motion by source, compatible grid, product pair and region generation. Keep received time separate from product time.
- [ ] Preserve each source's candidate and conflict reason. Select one supported primary with the approved policy; do not average conflicting arrival times or compute motion again on the combined forecast. Keep the existing map composite as a time-labelled observation view.
- [ ] Store each actual issued target forecast, source pair, algorithm version and support in the existing additive PostGIS bindings and publish it through the current MQTT contract. Verify read-back and a duplicate-cycle no-recompute test.

**Gate:** The focused tests and the existing causal Oldenburg control pass. On NAS, two normal unique source products show correct product times, published per-target predictions, source bindings and no cross-target leakage. Measure full-cycle runtime before accepting the stage. Deploy according to `docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md` only after rollback preparation.

## Stage 2: Make the whole rain zone the passage unit

**Files:** Extend the source forecast target-series and passage logic in `thin_source_radar_forecast.py` or a focused adjacent module; use the existing component and intensity records as separate inputs. Extend the targeted passage tests.

- [ ] Test a broad weak field with several embedded cores: the outer valid quantitative rain field gives the total target passage; the cores produce separate intensity windows. A component ending cannot terminate the whole passage while the target remains wet in the field.
- [ ] Test `>0` as a sensitive weak-echo diagnostic and `>=0.1 mm/u` as a distinct forecast threshold. Keep zero, nodata and source coverage boundaries distinct. Do not connect two target intervals across a supported dry sample or an unknown gap.
- [ ] Emit earliest supported arrival with its **predicted** one-minute sampling bracket, and an end only when a supported wet-to-dry transition exists. If the two-hour horizon or motion support ends first, retain a previous end only for the same target position and same identified episode while that end remains in the future; show its original issue time and mark it stale/provisional. Otherwise mark the end unknown/censored. Never present a one-minute core window as the duration of the whole rain zone.
- [ ] Verify collocated targets receive comparable timing while independently located targets may differ; inspect the map and MQTT details against actual source products.

**Gate:** On historical Oldenburg issues, weak onset and late end are reported separately, including missing advance warnings and censored ends. A long rain zone no longer produces a misleading short total duration. Continue only if false-wet on the fixed comparison universe is understood and the new field is visible on normal NAS products.

## Stage 3: Restore motion continuity where evidence permits

**Files:** Extend `SourceRadarForecastEngine` and local velocity evaluation in `thin_source_radar_forecast.py`; focused motion/trace tests and the existing replay scorer.

- [ ] Retain at most six unique, compatible products per source from the previous 30 minutes. Store product and receipt times, feature positions, covariance diagnostics and generation. Never count repeated products or the same tracked feature in adjacent pairs as independent spatial support.
- [ ] Compare a time-aware EMA of source-specific supported motion with the latest-pair estimate on the same spatial locations. Age weight depends on actual product time difference; stale motion expires. On a material direction conflict, expose competing estimates rather than smoothing through a turn.
- [ ] When a trace lacks three local vectors within 60 km, test older compatible motion from that source at the trace location, at most 10 minutes old and within 90 km; require at least three distinct spatial support locations, directional agreement and RMS spread <=8 m/s. Record fallback age and support. This is a proposed starting policy for a shadow comparison, not a calibrated threshold. Reject a whole-domain mean unless a separate test demonstrates coherent movement over the relevant domain.
- [ ] Attribute the `8 m/s` spread cutoff using stage-0 diagnostics; test any alternative in shadow on the fixed issues. Keep the current gate unless the alternative reduces unsupported actually-wet samples without increasing false-wet counts on that same fixed comparison universe or increasing median arrival jumps across successive supported issues. Check a second independent event before rollout.

**Gate:** One bounded, causal Oldenburg shadow comparison reports support gained, false-wet gained or lost, misses, onset stability and per-issue runtime. Do not repeat parameter search on this single event. If the second independent event is not yet archived, keep the motion change in shadow and proceed with other independent stages.

## Stage 4: Test spatial sampling as a separate change

**Files:** Replace only the QPE lookup after inverse tracing in `thin_source_radar_forecast.py`, behind a named policy; add masked four-cell interpolation tests. Keep motion estimation and source selection fixed.

- [ ] Test exact cell centers, a rain/dry edge, a narrow intense core, raster edge, and a mixture of valid zero and nodata. Bilinear interpolation may use only four valid neighbors with an explicit support fraction; it must not fill a nodata boundary as measured rain.
- [ ] Compare floor-cell and masked bilinear sampling on the same saved trajectories, observing both threshold-crossing times and quantitative error. No new SciPy dependency for a four-cell operation.

**Gate:** Select bilinear only if its measured arrival/end/intensity behavior is better without creating material false drizzle or weakening cores. Otherwise keep floor-cell sampling and retain the test result.

## Stage 5: Add an ensemble only after deterministic support improves

**Files:** A focused source-motion uncertainty module adjacent to `thin_source_radar_forecast.py`, plus targeted tests and an additive replay metric. No production percentage in this stage.

- [ ] Estimate two-dimensional motion error and its spatial/time dependence from archived forecast-versus-observation residuals across several independent rain events. Keep source, radar intensity, growth/decay and support errors distinct. Do not equate the local vector RMS with a calibrated forecast error.
- [ ] Generate spatially and temporally coherent member perturbations; keep each member coherent along its trajectory. Unknown members remain unknown. Record supported-member fraction, event definition, and conditional wet intensity distribution.
- [ ] Define two separate outputs: chance of rain at a specified future instant and chance of any rain before a specified horizon. Test that the latter is nondecreasing with horizon. Test onset/end brackets only among supported members and preserve censored exits.
- [ ] Calibrate and verify on held-out events using reliability diagrams, Brier score, interval coverage, false alarms and misses. Keep the deterministic estimate and support reason visible until the probability is independently credible. Treat optical-flow vorticity as a diagnostic, not a physical storm-rotation or automatic confidence score.

**Gate:** Report held-out reliability by lead and precipitation threshold, supported-member counts, and runtime under the NAS worker limit. Publish numerical rain percentages only after this gate passes.

## Stage 6: Present the result without hiding uncertainty

**Files:** The existing worker MQTT publication and Home Assistant forecast-card data paths; targeted schema and presentation tests.

- [ ] Define additive fields for issuance time, product valid time, target and region generation, primary source, conflict, support, arrival/end estimates, brackets, end censoring, weak-rain diagnostic and intensity-core windows. Preserve existing consumers and discovery entities.
- [ ] Make the main card show one understandable time window when supported, plus short wording such as “einde voorlopig” for a censored exit and “laatste verwachting van 19:05” for a retained estimate. Show source conflict and uncertainty without suggesting that a retained estimate is fresh radar evidence. Never carry a prior target position across a move.
- [ ] When stage 5 passes, show calibrated probabilities with a named event and lead time. Do not publish example numbers or an uncalibrated single confidence percentage.

**Gate:** Validate actual Home Assistant payloads and card behavior on normal source cycles. Run the full NAS deploy and rollback checks. Record the real forecast **before** each verifying rain passage; compare against later radar and available ground evidence across multiple independent events.

## Decision rule after each stage

Advance on measured benefit, not on visual smoothness or a single successful arrival. Keep the previous production policy and its rollback package if support, false-wet, onset stability, end handling or runtime regresses. A target may always display its last applicable estimate with an explicit age and uncertainty, but no algorithm can guarantee a fresh, correct clock time without valid evidence.
