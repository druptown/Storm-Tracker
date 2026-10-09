# Netatmo World Catalog Importer Implementation Plan

**Spec:** `docs/superpowers/specs/2026-09-23-netatmo-world-catalog-importer-design.md`

**Goal:** Build and start a separate resumable NAS container that inventories
all stations returned by Netatmo's public world API into the existing PostGIS
catalog without involving Home Assistant, MQTT, or the live worker runtime.

## Constraints

- Use only the official Netatmo API.
- Base grid is 1 degree; subdivide incomplete/dense tiles deterministically.
- One request at a time and no more than 400 discovery calls per hour.
- Persist every recognized public station and its sensor capabilities.
- Live selection later filters for rain, wind, or pressure; no destructive
  cleanup during import.
- Never log or persist Netatmo/PostGIS secrets.
- Do not modify, restart, or deploy the HA integration or active worker.

## Task 1: Complete adaptive world traversal

**Files:**
- Modify `storm_tracker_worker/thin_station_bootstrap.py`
- Modify `tests/test_thin_station_bootstrap.py`

Implement the 61,200-tile world inventory, deterministic subdivision,
checkpoint reconciliation, quota back-off, renewable access-token callback,
and one world-complete marker. Verify RED then GREEN with the focused tests.

## Task 2: Add the standalone importer runtime

**Files:**
- Create `station_catalog_importer/Dockerfile`
- Create `station_catalog_importer/run.sh`
- Create `station_catalog_importer/compose.yaml`
- Create `storm_tracker_worker/netatmo_world_importer.py`
- Create `tests/test_netatmo_world_importer.py`

The runtime reads secrets only from environment/file inputs, connects directly
to PostGIS, reports payload-free progress, exits on the world-complete marker,
and retries temporary provider/database failures with bounded delays. The
container has no HA or MQTT configuration.

## Task 3: Verify and package

Run focused importer/catalog tests, the entire relevant thin-station/provider
suite, then the full project test command. Build the container image and run a
secret-free dry-run proving exactly 61,200 base tiles and a 10-second minimum
call interval (maximum 360 calls/hour, below the 400/hour design ceiling).

## Task 4: Start on the NAS

Resolve the existing authorized Synology Container Manager route read-only.
Create only the importer container, attach its existing PostGIS and Netatmo
configuration without printing secrets, then start it. Verify the importer is
running, its first new checkpoint appears in PostGIS, and NAS-HA plus the
Storm Tracker worker remain unchanged and online.
