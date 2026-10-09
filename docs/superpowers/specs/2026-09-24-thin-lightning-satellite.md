# Thin Lightning and Satellite Design

## Goal

Connect the geographically applicable lightning and satellite products to the
existing thin worker pipeline, normalize useful current data, persist the
results in PostGIS, and expose truthful dual provider status to Home Assistant.

## Scope

- Lightning: existing direct Blitzortung stream, EUMETSAT MTG-LI, GOES-18 GLM,
  and GOES-19 GLM.
- Satellite precipitation: GOES-18 RRQPE, GOES-19 RRQPE, NOAA Enterprise Rain
  Rate, and H SAF H40B.
- Reuse the existing native bounded download and decode primitives where they
  are useful, but do not reintroduce jobs, receipts, RAW-state audits, startup
  audits, or the legacy scheduler into the normal runtime.
- Fetch only products whose catalogue coverage contains at least one current
  target.

## Behaviour

- `readiness=ready` means the implementation is complete and live-tested.
- The effective state independently reports `operational`, truthful
  `no_observations`, `target_outside_catalog_scope`, blocked credentials, stale
  input, or a concrete fetch/decode failure.
- A successfully fetched empty lightning scene is operational and is not a
  provider failure.
- Lightning data is normalized as `LIGHTNING_STRIKE`; quantitative satellite
  precipitation is normalized as `RAIN_RATE` with source time, geometry, units,
  quality, and provenance metadata.
- Only current, valid observations may enter fusion and PostGIS.
- Per-provider concurrency remains one request; CPU-heavy decode remains bounded
  by the worker's existing two-thread computation ceiling.

## Delivery and validation

1. Deliver and deploy the lightning sources as one coherent release.
2. Live-test each source by placing the fictive tracker inside its catalogue
   coverage; distinguish an operational quiet scene from an unavailable source.
3. Deliver and deploy the satellite sources as a second coherent release.
4. Live-test each satellite product within coverage and verify the actual source
   timestamp/artifact, normalized observations, provider status, and PostGIS.
5. Restore the fictive tracker to its exact home location.

