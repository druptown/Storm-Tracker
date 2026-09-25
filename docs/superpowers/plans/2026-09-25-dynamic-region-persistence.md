# Dynamic Region Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store every meteorological fact once, bind its use to an exact dynamic region revision, and retain separate location-specific forecasts for every target.

**Architecture:** `ThinRegionPlanner` emits a logical region, one activation instance, and an immutable geographic revision. PostGIS stores canonical observations and station measurements independently from targets, then links them many-to-many to the region revision in which they were used. Targets and result rows keep revision provenance, while six-month retention removes operational history without touching the station catalog or importer state.

**Tech Stack:** Python 3.12+, pytest/pytest-asyncio, psycopg 3, PostgreSQL 17/PostGIS 3.5, Home Assistant App packaging, MQTT.

**Spec:** `docs/superpowers/specs/2026-09-25-dynamic-region-persistence-design.md`

## Global Constraints

- Deploy only to NAS-Home Assistant `192.168.0.15`; never modify or restart the NUC.
- Preserve the global station catalog, station capabilities, importer progress, migrations, configuration, and the running Netatmo importer.
- Keep canonical source facts independent from both target and region identities.
- Retain operational history for six months.
- Keep target forecasts location-specific even when targets share one region.
- Use `scripts/install_ha_release_pair_ssh.py`, user `hassio`, key `/home/ikke/.ssh/codex_nas_ha_deploy`, and MCP server `home_assistant_NAS` for lifecycle operations.
- Preserve NAS worker settings `dedicated_nas_fast`, `cpu_limit_percent: 200`, `worker_nice: 0`, and `cpu_threads: 2`.
- Migration `0006` has not been applied live and may be replaced before deployment.

## Review Focus

- A target leaving and later re-entering the same home revision must create two membership intervals without duplicating source facts.
- A worker restart while an external region is active may open a new external instance, but must never merge two separated activations.
- Small GPS jitter must not create new revisions; recentering or a material radius-bucket change must create one.
- One source fact used by two overlapping revisions must produce one canonical row and two revision links.
- A cycle with no active external targets must close prior external instances, revisions, memberships, and station polling context without deleting history.

---

### Task 1: Emit explicit region instances and immutable revisions

**Files:**
- Modify: `storm_tracker_worker/thin_region_planner.py`
- Modify: `tests/test_thin_region_planner.py`

**Interfaces:**
- Consumes: `DesiredState`, `Target`, and the existing hysteresis/recenter rules.
- Produces: `ThinAcquisitionRegion.region_instance_id: str`, `region_revision_id: str`, `activated_at: float`, and `revision_valid_from: float`.

- [ ] **Step 1: Write failing lifecycle tests**

Add tests which use literal expected relationships rather than implementation hashes:

```python
def test_external_region_reuses_instance_until_it_disappears():
    planner = _planner()
    first = planner.plan(_state(remote=(48.85, 2.35)), now=100.0)
    moved = planner.plan(_state(remote=(48.86, 2.36), generation=2), now=200.0)
    assert moved.regions[-1].region_instance_id == first.regions[-1].region_instance_id
    assert moved.regions[-1].region_revision_id == first.regions[-1].region_revision_id


def test_external_region_reactivation_gets_new_instance():
    planner = _planner()
    first = planner.plan(_state(remote=(48.85, 2.35)), now=100.0)
    planner.plan(_home_only_state(generation=2), now=200.0)
    returned = planner.plan(_state(remote=(48.85, 2.35), generation=3), now=300.0)
    assert returned.regions[-1].region_instance_id != first.regions[-1].region_instance_id


def test_recenter_creates_revision_inside_same_instance():
    planner = _planner(recenter_distance_km=15.0)
    first = planner.plan(_state(remote=(48.85, 2.35)), now=100.0)
    recentered = planner.plan(_state(remote=(49.05, 2.35), generation=2), now=200.0)
    assert recentered.regions[-1].region_instance_id == first.regions[-1].region_instance_id
    assert recentered.regions[-1].region_revision_id != first.regions[-1].region_revision_id
```

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
PYTHONPATH=. .venv/bin/pytest -q \
  tests/test_thin_region_planner.py::test_external_region_reuses_instance_until_it_disappears \
  tests/test_thin_region_planner.py::test_external_region_reactivation_gets_new_instance \
  tests/test_thin_region_planner.py::test_recenter_creates_revision_inside_same_instance
```

Expected: FAIL because the four lifecycle fields do not yet exist.

- [ ] **Step 3: Implement stable instance and revision identities**

Extend `ThinAcquisitionRegion` with the four produced fields. Maintain:

```python
self._external_instances: dict[str, tuple[str, float]] = {}
self._region_revisions: dict[str, tuple[str, str, float]] = {}
```

Create external instance IDs from logical region ID plus activation timestamp, and remove them when the external region disappears. Use constant `region-instance:home` for home. Round coverage upward in safe 5 km buckets so GPS jitter does not rewrite the footprint:

```python
def _coverage_bucket_km(value: float) -> float:
    return math.ceil(float(value) / 5.0) * 5.0
```

Build the revision fingerprint from instance ID, center rounded to five decimals, warning-radius bucket, and radar-radius bucket. Reuse the prior revision while unchanged; otherwise hash a new revision ID and set `revision_valid_from=now`.

- [ ] **Step 4: Run the planner suite**

Run: `PYTHONPATH=. .venv/bin/pytest -q tests/test_thin_region_planner.py`

Expected: all tests pass, including jitter, recentering, disappearance, and reactivation.

- [ ] **Step 5: Commit the planner contract**

```bash
git add storm_tracker_worker/thin_region_planner.py tests/test_thin_region_planner.py
git commit -m "feat: version dynamic region lifecycles"
```

### Task 2: Replace migration 0006 with revision-aware canonical storage

**Files:**
- Modify: `storm_tracker_worker/postgis_migrations/0006_canonical_observation_retention.sql`
- Modify: `tests/test_postgis_migrations.py`

**Interfaces:**
- Consumes: migration `0005`, existing `stv3_observation`, `stv3_station_measurement`, and `stv3_meta.schema_migrations`.
- Produces: `stv3_region_instance`, `stv3_region_revision`, `stv3_region_target`, `stv3_observation_region`, `stv3_station_measurement_region`, `stv3_observation_source_key(...)`, and `stv3_enforce_retention()`.

- [ ] **Step 1: Write failing schema contract tests**

```python
assert "create table if not exists public.stv3_region_instance" in normalized
assert "create table if not exists public.stv3_region_revision" in normalized
assert "region_revision_id text not null" in normalized
assert "create table if not exists public.stv3_region_target" in normalized
assert "valid_from timestamptz not null" in normalized
assert "valid_until timestamptz" in normalized
assert "create table if not exists public.stv3_observation_region" in normalized
assert "create table if not exists public.stv3_station_measurement_region" in normalized
assert "interval '6 months'" in normalized
assert "delete from public.stv3_ground_station" not in normalized
assert "delete from public.stv3_station_capability" not in normalized
assert "delete from public.stv3_station_discovery_tile" not in normalized
```

- [ ] **Step 2: Run the migration test and verify RED**

Run: `PYTHONPATH=. .venv/bin/pytest -q tests/test_postgis_migrations.py`

Expected: FAIL because the draft migration still stores a bare `region_id`.

- [ ] **Step 3: Define the revision-aware schema**

Use these lifecycle tables:

```sql
CREATE TABLE public.stv3_region_instance(
    region_instance_id text PRIMARY KEY,
    region_id text NOT NULL,
    kind text NOT NULL,
    activated_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    valid_until timestamptz,
    active boolean NOT NULL
);

CREATE TABLE public.stv3_region_revision(
    region_revision_id text PRIMARY KEY,
    region_instance_id text NOT NULL REFERENCES public.stv3_region_instance
        ON DELETE CASCADE,
    center geometry(Point,4326) NOT NULL,
    observation_radius_km double precision NOT NULL,
    radar_analysis_radius_km double precision,
    valid_from timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    valid_until timestamptz,
    active boolean NOT NULL
);
```

Use a surrogate `bigserial` membership key for `stv3_region_target`, a partial unique index on active `target_id`, and `region_revision_id` foreign keys in both source-link tables. Add nullable `region_revision_id` columns and indexes to `stv3_storm_cell`, `stv3_track_point`, `stv3_target_forecast`, `stv3_target_passage`, and `stv3_forecast_validation`.

Keep the target-neutral observation hash over provider, product, kind, observation timestamp, and EWKB geometry. Rekey/deduplicate any existing observations before creating links; never invent region links for legacy `target_id` values.

- [ ] **Step 4: Implement six-month retention in dependency order**

The daily `SECURITY DEFINER` function deletes old validation, passage, forecast, track, cell, fusion, station-measurement, observation, replay, raw-capture, and lightning facts first. It then deletes closed region instances whose `valid_until` is older than the cutoff. Cascades may remove only revision/link context. Grant the worker only required DML and function execution privileges.

- [ ] **Step 5: Run migration and station schema tests**

```bash
PYTHONPATH=. .venv/bin/pytest -q \
  tests/test_postgis_migrations.py \
  tests/test_thin_postgis_station_migration.py
```

Expected: all pass.

- [ ] **Step 6: Commit the migration**

```bash
git add storm_tracker_worker/postgis_migrations/0006_canonical_observation_retention.sql tests/test_postgis_migrations.py
git commit -m "feat: add revision-aware regional PostGIS schema"
```

### Task 3: Persist canonical facts and region-revision context atomically

**Files:**
- Modify: `storm_tracker_worker/thin_postgis.py`
- Modify: `storm_tracker_worker/thin_operational_runtime.py`
- Modify: `tests/test_thin_postgis.py`
- Modify: `tests/test_thin_operational_runtime.py`

**Interfaces:**
- Consumes: `ThinPostGISWriter.write_cycle(..., region_plan: ThinRegionPlan | None)` and lifecycle fields from Task 1.
- Produces: canonical fact rows, revision links, lifecycle intervals, and result provenance in one transaction.

- [ ] **Step 1: Replace draft bare-region tests with revision tests**

Cover these literal outcomes:

```python
assert result.written_rows["observations"] == 1
assert result.written_rows["observation_region_links"] == 1
assert result.written_rows["station_measurements"] == 1
assert result.written_rows["station_measurement_region_links"] == 1
assert region_link_row[5] == "region-revision:home:v1"
assert {row[1] for row in target_rows} == {"region-revision:home:v1"}
```

Add a second test with two region revisions sharing an identical observation. It must assert one observation row and two `stv3_observation_region` rows. Add a disappearance test asserting closure statements for absent external instances, revisions, and memberships.

- [ ] **Step 2: Run writer tests and verify RED**

Run: `PYTHONPATH=. .venv/bin/pytest -q tests/test_thin_postgis.py`

Expected: FAIL because the draft writer links bare `region_id` and has no lifecycle closure.

- [ ] **Step 3: Upsert lifecycle state from the newest plan**

Keep all queued plans available for linking retried facts, but derive active state from the newest cycle only. Upsert instances and revisions first. Close active database rows absent from the newest plan with `valid_until=to_timestamp(evaluated_at)`; never delete them during writes.

For memberships, close any active row whose target/revision pair is absent. Update the current interval when unchanged; insert a new interval when a target moves revision or re-enters after closure.

- [ ] **Step 4: Store facts once and link them to revisions**

Keep the canonical observation key independent from target and region. Deduplicate station measurements by `(provider_id, station_id, observed_at, measurement_type)`. Derive each fact’s revision from `observation.target_id` first and spatial containment second. Deduplicate links by `(source_identity, region_revision_id)`.

Execute within the existing transaction in this order:

```text
instances → revisions → memberships → station catalog/capabilities
→ station measurements → station/revision links
→ general observations → observation/revision links → results → retention
```

- [ ] **Step 5: Pass the exact plan from operational runtime**

```python
await self._postgis.write_cycle(
    fusion,
    nowcast,
    validations,
    regional_radar=tuple(regional_summaries),
    region_plan=region_plan,
)
```

Update only test doubles that model this writer boundary; do not add a production fallback that discards live region context.

- [ ] **Step 6: Run writer and runtime tests**

```bash
PYTHONPATH=. .venv/bin/pytest -q \
  tests/test_thin_postgis.py \
  tests/test_thin_operational_runtime.py
```

Expected: all pass.

- [ ] **Step 7: Commit atomic regional persistence**

```bash
git add storm_tracker_worker/thin_postgis.py storm_tracker_worker/thin_operational_runtime.py tests/test_thin_postgis.py tests/test_thin_operational_runtime.py
git commit -m "feat: persist canonical facts by region revision"
```

### Task 4: Preserve region provenance on cells and per-target forecasts

**Files:**
- Modify: `storm_tracker_worker/thin_postgis.py`
- Modify: `tests/test_thin_postgis.py`
- Test: `tests/test_thin_footprint_forecast.py`

**Interfaces:**
- Consumes: revision lookup by target ID or spatial position.
- Produces: `region_revision_id` on persisted cells, tracks, forecasts, passages, and validations.

- [ ] **Step 1: Write failing result-provenance tests**

Extend the writer fixture with two targets in one revision at different coordinates. Assert both forecast rows carry the same revision but retain different target IDs and independently calculated ETA/intensity fields. Assert a storm cell and track carry the revision selected by cell centroid.

- [ ] **Step 2: Run result tests and verify RED**

```bash
PYTHONPATH=. .venv/bin/pytest -q \
  tests/test_thin_postgis.py \
  tests/test_thin_footprint_forecast.py
```

Expected: FAIL because result inserts do not bind revisions.

- [ ] **Step 3: Add revision parameters to result upserts**

Resolve target results through `region_plan.region_revision_id_for(target_id)`. Resolve cell/track context through `region_revision_id_for_position(latitude, longitude)`. Use `NULL` only when a direct legacy unit test explicitly supplies no plan; live runtime always supplies one.

Do not alter fusion weights, cell detection, footprint advection, or target-specific ETA calculations.

- [ ] **Step 4: Run complete thin/PostGIS suite**

```bash
PYTHONPATH=. .venv/bin/pytest -q \
  tests/test_thin_*.py \
  tests/test_postgis_migrations.py \
  tests/test_worker_postgis_projection.py
```

Expected: all pass.

- [ ] **Step 5: Commit result provenance**

```bash
git add storm_tracker_worker/thin_postgis.py tests/test_thin_postgis.py tests/test_thin_footprint_forecast.py
git commit -m "feat: retain region provenance on forecasts"
```

### Task 5: Build, migrate, deploy, and verify on NAS-HA

**Files:**
- Modify: `storm_tracker_worker/__init__.py`
- Modify: `storm_tracker_worker/config.yaml`
- Modify: `CHANGELOG.md`
- Create: `docs/DEPLOY_2026-09-25_1.14.144_1.18.56.md`

**Interfaces:**
- Consumes: worker `1.18.56`, integration `1.14.144`, migration `0006`, and the fixed NAS deployment procedure.
- Produces: verified release artifacts and live evidence from NAS-HA only.

- [ ] **Step 1: Verify versions and relevant tests**

```bash
PYTHONPATH=. .venv/bin/pytest -q \
  tests/test_thin_*.py \
  tests/test_postgis_migrations.py \
  tests/test_worker_postgis_projection.py \
  tests/test_release_artifacts.py \
  tests/test_install_ha_release_pair_ssh.py
```

Then run bare `PYTHONPATH=. .venv/bin/pytest -q`. Record every unrelated pre-existing failure by exact test name; do not alter pinned historical infrastructure solely to make this release green.

- [ ] **Step 2: Build deterministic release artifacts**

```bash
PYTHONPATH=. .venv/bin/python scripts/build_release_candidate.py \
  --integration-root custom_components/storm_tracker_v3 \
  --worker-root storm_tracker_worker \
  --integration-version 1.14.144 \
  --worker-version 1.18.56 \
  --output-directory .analysis/release-1.14.144-1.18.56-20260925
```

Verify archive names, internal versions, SHA-256 values, and absence of secrets, databases, caches, and test output.

- [ ] **Step 3: Create rollback evidence and stop only Storm Tracker**

Use `home_assistant_NAS` to read live Core/App/integration status and create the prescribed rollback backup. Disable only the Storm Tracker config entry and stop only `local_storm_tracker_worker`. Do not stop PostGIS or the Netatmo importer.

- [ ] **Step 4: Apply migration 0006 transactionally**

Retrieve the PostGIS admin environment through the established local trust chain without printing credentials. Execute the exact migration in one transaction. Verify:

```sql
SELECT migration_id FROM stv3_meta.schema_migrations
WHERE migration_id='0006_canonical_observation_retention';
SELECT count(*) FROM stv3_ground_station;
SELECT count(*) FROM stv3_station_capability;
SELECT count(*) FROM stv3_station_discovery_tile;
```

The three preserved counts must not decrease.

- [ ] **Step 5: Install through the fixed SSH route**

Run `scripts/install_ha_release_pair_ssh.py` with host `192.168.0.15`, user `hassio`, key `/home/ikke/.ssh/codex_nas_ha_deploy`, verified evidence, `--confirm-components-stopped`, and `--apply`. Reload the Store through NAS MCP `supervisor/api` endpoint `/store/reload`; use the official App update action.

- [ ] **Step 6: Start worker first and wait for readiness**

Start `local_storm_tracker_worker`. Wait for a fresh MQTT heartbeat with runtime version `1.18.56` and `job_admission_open=true`. Only then re-enable or reload the Storm Tracker integration.

- [ ] **Step 7: Prove live regional behavior**

After at least one complete cycle, run:

```sql
SELECT count(*) FROM stv3_observation WHERE target_id IS NOT NULL;
SELECT observation_key,count(*) FROM stv3_observation
GROUP BY observation_key HAVING count(*)>1;
SELECT count(*) FROM stv3_observation_region;
SELECT count(*) FROM stv3_station_measurement_region;
SELECT count(*) FROM stv3_region_instance WHERE active;
SELECT count(*) FROM stv3_region_revision WHERE active;
SELECT target_id,count(*) FROM stv3_region_target
WHERE valid_until IS NULL GROUP BY target_id HAVING count(*)>1;
```

Expected: zero target-bound canonical observations, zero duplicate keys, nonzero revision links after relevant data arrives, one active membership interval per target, and active instance/revision counts equal the live plan.

Move the fictive tracker only if required for one bounded external-region test. Confirm creation of a new instance/revision, restore its original position, and confirm closure without history deletion.

- [ ] **Step 8: Record deployment evidence**

Document versions, hashes, migration registration, preserved catalog counts, worker readiness, PostGIS checks, and full-suite pre-existing failures in `docs/DEPLOY_2026-09-25_1.14.144_1.18.56.md`.

- [ ] **Step 9: Commit release evidence**

```bash
git add CHANGELOG.md storm_tracker_worker/__init__.py storm_tracker_worker/config.yaml docs/DEPLOY_2026-09-25_1.14.144_1.18.56.md
git commit -m "release: deploy regional persistence worker 1.18.56"
```

