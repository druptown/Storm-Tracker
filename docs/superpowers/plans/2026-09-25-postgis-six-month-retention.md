# PostGIS Six-Month Retention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep all Storm Tracker time-bound PostGIS measurements and derived results for no more than six calendar months while preserving permanent station catalog data.

**Architecture:** High-volume and operational time-series move to monthly partitioned v2 tables through additive shadow tables, bounded backfill, dual-write verification, and an explicit cutover. A fail-closed retention runner drops fully expired partitions and batch-deletes the exact cutoff boundary. Permanent catalog and migration metadata are excluded by a closed retention-policy registry.

**Tech Stack:** Python 3.13, psycopg 3, PostgreSQL 17 declarative partitioning, PostGIS 3.5, pytest

**Spec:** `docs/superpowers/specs/2026-09-25-station-centric-postgis-retention-design.md`

## Global Constraints

- Execute this plan only after the station-storage plan is live and verified.
- Retention is exactly six calendar months for all time-bound PostGIS facts and results.
- `stv3_ground_station`, `stv3_station_capability`, `stv3_station_discovery_tile`, and `stv3_meta.schema_migrations` are permanent.
- The Netatmo importer must remain running and untouched.
- No destructive apply occurs before a dry-run report and explicit live verification gate.
- Runtime writers execute DML only; DDL and partition maintenance run through the management CLI.
- Legacy tables remain rollback sources until post-cutover counts and live behavior are verified.

## Review Focus

- A cutoff in the middle of a month must remove only older rows, not the complete boundary partition; Task 2 tests this exactly.
- Month arithmetic over February and year boundaries must use calendar months rather than 180 days; Task 1 tests both.
- Catalog and discovery tables must be impossible to add to retention accidentally; Task 1 uses a closed allowlist test.
- A concurrent cleanup attempt must exit without deleting data; Task 4 tests the PostgreSQL advisory lock path.
- A failed dual write must roll back both legacy and v2 writes and retain the retry cycle; Task 3 extends writer transaction tests.

---

### Task 1: Define the closed retention policy and calendar cutoff

**Files:**
- Create: `storm_tracker_worker/thin_postgis_retention.py`
- Create: `tests/test_thin_postgis_retention.py`

**Interfaces:**
- Produces: `retention_cutoff(now: datetime, months: int = 6) -> datetime`, `RetentionTable`, and immutable `RETENTION_TABLES`.

- [ ] **Step 1: Write failing policy tests**

```python
def test_cutoff_uses_calendar_months():
    assert retention_cutoff(datetime(2026, 9, 30, 12, tzinfo=UTC)) == datetime(2026, 3, 30, 12, tzinfo=UTC)
    assert retention_cutoff(datetime(2026, 8, 31, 12, tzinfo=UTC)) == datetime(2026, 2, 28, 12, tzinfo=UTC)


def test_permanent_tables_are_not_retention_candidates():
    names = {item.table_name for item in RETENTION_TABLES}
    assert names.isdisjoint({"stv3_ground_station", "stv3_station_capability", "stv3_station_discovery_tile"})
```

- [ ] **Step 2: Run the tests and verify failure**

Run: `pytest -q tests/test_thin_postgis_retention.py`

- [ ] **Step 3: Implement calendar subtraction and a closed policy registry**

Include station measurements, physical observations, fusion cycles, cells, tracks, target positions, forecasts, passages, validations, and replay runs. Associate each table with its exact time column. Replay metrics expire through `ON DELETE CASCADE` from replay runs.

- [ ] **Step 4: Run focused tests and commit**

Run: `pytest -q tests/test_thin_postgis_retention.py`

```bash
git add storm_tracker_worker/thin_postgis_retention.py tests/test_thin_postgis_retention.py
git commit -m "feat: define six month PostGIS retention policy"
```

### Task 2: Add monthly partitioned v2 operational tables

**Files:**
- Create: `storm_tracker_worker/postgis_migrations/0006_partitioned_operational_results.sql`
- Modify: `tests/test_postgis_migrations.py`

**Interfaces:**
- Produces partitioned shadow tables: `stv3_observation_v2`, `stv3_fusion_cycle_v2`, `stv3_storm_cell_v2`, `stv3_track_point_v2`, `stv3_target_forecast_v2`, `stv3_target_position_v2`, `stv3_target_passage_v2`, `stv3_forecast_validation_v2`, and `stv3_replay_run_v2`; replay metrics reference `stv3_replay_run_v2` with cascade.

- [ ] **Step 1: Write failing migration tests**

For every table, assert `PARTITION BY RANGE(<time column>)`, a primary/unique key containing the partition time, required spatial indexes, and registration as migration `0006_partitioned_operational_results`. Assert catalog table names do not appear after `DROP`, `DELETE`, or `TRUNCATE`.

- [ ] **Step 2: Run and verify failure**

Run: `pytest -q tests/test_postgis_migrations.py`

- [ ] **Step 3: Create additive v2 table definitions and partitions**

Preserve all existing columns. Change formerly single-column identities as follows:

```text
observation:        PRIMARY KEY (observation_key, observed_at)
fusion cycle:       PRIMARY KEY (cycle_key, evaluated_at)
forecast validation:PRIMARY KEY (validation_key, evaluated_at)
replay run:         PRIMARY KEY (replay_id, started_at)
```

Create partitions for the previous six calendar months, current month, and next month. Do not rename, drop, truncate, or copy from legacy tables in this migration.

- [ ] **Step 4: Run migration tests and commit**

Run: `pytest -q tests/test_postgis_migrations.py`

```bash
git add storm_tracker_worker/postgis_migrations/0006_partitioned_operational_results.sql tests/test_postgis_migrations.py
git commit -m "feat: add partitioned operational shadow tables"
```

### Task 3: Add explicit dual-write and partitioned-read modes

**Files:**
- Modify: `storm_tracker_worker/thin_postgis.py`
- Modify: `storm_tracker_worker/thin_replay.py`
- Modify: `storm_tracker_worker/thin_mqtt_service.py`
- Modify: `tests/test_thin_postgis.py`
- Modify: `tests/test_thin_replay.py`
- Modify: `tests/test_thin_mqtt_service.py`

**Interfaces:**
- Adds `STV3_POSTGIS_STORAGE_GENERATION=legacy|dual|partitioned`, defaulting to `legacy` until live schema verification.
- `dual` writes legacy and v2 tables in the same transaction; `partitioned` writes and reads v2 only.

- [ ] **Step 1: Write failing mode tests**

Prove:

```python
assert StorageGeneration.parse({}) is StorageGeneration.LEGACY
assert _tables(StorageGeneration.DUAL).observation == ("stv3_observation", "stv3_observation_v2")
assert _tables(StorageGeneration.PARTITIONED).observation == ("stv3_observation_v2",)
```

Add a transaction test where the v2 insert fails in dual mode: neither legacy nor v2 connection state may commit, and the worker cycle stays queued.

- [ ] **Step 2: Run focused tests and verify failure**

Run: `pytest -q tests/test_thin_postgis.py tests/test_thin_replay.py tests/test_thin_mqtt_service.py -k 'generation or dual or partitioned'`

- [ ] **Step 3: Implement table-name mapping without SQL string injection**

Use an enum-to-static-SQL mapping; never interpolate environment text into SQL. Update composite `ON CONFLICT` targets for v2 tables. Expose the selected generation in diagnostics without credentials.

- [ ] **Step 4: Run writer, replay, and service suites**

Run: `pytest -q tests/test_thin_postgis.py tests/test_thin_replay.py tests/test_thin_mqtt_service.py`

- [ ] **Step 5: Commit**

```bash
git add storm_tracker_worker/thin_postgis.py storm_tracker_worker/thin_replay.py storm_tracker_worker/thin_mqtt_service.py tests/test_thin_postgis.py tests/test_thin_replay.py tests/test_thin_mqtt_service.py
git commit -m "feat: support dual PostGIS storage generations"
```

### Task 4: Implement bounded v2 backfill, verification, and retention runner

**Files:**
- Create: `storm_tracker_worker/thin_postgis_retention_repository.py`
- Create: `scripts/manage_postgis_retention.py`
- Create: `tests/test_thin_postgis_retention_repository.py`

**Interfaces:**
- CLI commands: `partitions`, `backfill`, `verify`, `dry-run`, `apply`.
- Produces `RetentionReport` with cutoff, partitions, boundary rows, deleted rows, oldest remaining timestamp, status, and error code per table.

- [ ] **Step 1: Write failing repository tests**

Test keyset backfill of only rows newer than the six-month cutoff, idempotent resume, advisory-lock contention, dry-run immutability, whole-partition selection, and bounded boundary deletion.

```python
def test_boundary_partition_is_trimmed_not_dropped():
    report = repository.plan(now=datetime(2026, 9, 25, tzinfo=UTC))
    assert report.cutoff == datetime(2026, 3, 25, tzinfo=UTC)
    assert "2026_02" in report.drop_partitions
    assert "2026_03" not in report.drop_partitions
    assert report.boundary_before == datetime(2026, 3, 25, tzinfo=UTC)
```

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest -q tests/test_thin_postgis_retention_repository.py`

- [ ] **Step 3: Implement safe identifiers, advisory lock, and batch limits**

Identifiers may only come from `RETENTION_TABLES`, never CLI input. Use `pg_try_advisory_lock` and exit `retention_already_running` on contention. `apply` drops only partitions whose exclusive upper bound is at or before cutoff, then deletes at most 10,000 boundary rows per transaction until none remain. Create next month's partitions before cleanup.

- [ ] **Step 4: Implement v2 verification**

For the six-month window compare legacy and v2 per table by row count, min/max timestamp, and stable identity count. Require zero rows older than cutoff in v2 after apply. Verify permanent catalog counts before and after are identical.

- [ ] **Step 5: Run tests and commit**

Run: `pytest -q tests/test_thin_postgis_retention.py tests/test_thin_postgis_retention_repository.py tests/test_postgis_migrations.py`

```bash
git add storm_tracker_worker/thin_postgis_retention_repository.py scripts/manage_postgis_retention.py tests/test_thin_postgis_retention_repository.py
git commit -m "feat: add bounded PostGIS retention runner"
```

### Task 5: Deploy, verify, cut over, and enable retention

**Files:**
- Modify: `scripts/build_release_candidate.py`
- Modify: `docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md`
- Create: `docs/DEPLOY_2026-09-25_POSTGIS_RETENTION.md`
- Modify: `tests/test_release_artifacts.py`

**Interfaces:**
- Consumes migrations `0005` and `0006`, both management CLIs, and storage-generation setting.
- Produces a verified partitioned live store and scheduled six-month cleanup.

- [ ] **Step 1: Extend packaging tests and build the release**

Require migration `0006`, retention modules, and CLI in the worker archive.

Run: `pytest -q tests/test_release_artifacts.py tests/test_postgis_migrations.py`

- [ ] **Step 2: Run the complete relevant regression suite**

Run:

```bash
pytest -q \
  tests/test_thin_station_measurement.py \
  tests/test_thin_postgis.py \
  tests/test_thin_postgis_station_migration.py \
  tests/test_thin_postgis_retention.py \
  tests/test_thin_postgis_retention_repository.py \
  tests/test_thin_replay.py \
  tests/test_netatmo_world_importer.py \
  tests/test_postgis_migrations.py
```

- [ ] **Step 3: Deploy additive schema and dual-write mode**

Follow `docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md`, make a rollback backup, apply migrations, create partitions, set `STV3_POSTGIS_STORAGE_GENERATION=dual`, and start the worker. Do not touch the importer.

- [ ] **Step 4: Backfill and verify the six-month window**

```bash
python scripts/manage_postgis_retention.py partitions
python scripts/manage_postgis_retention.py backfill --batch-size 5000
python scripts/manage_postgis_retention.py verify
python scripts/manage_postgis_retention.py dry-run
```

Require matching stable identities, valid ranges, zero v2 orphans, unchanged catalog/checkpoint counts, current heartbeat, `postgis=current`, and no failed-job increase.

- [ ] **Step 5: Switch to partitioned mode and observe**

Set `STV3_POSTGIS_STORAGE_GENERATION=partitioned`, restart only Storm Tracker through `home_assistant_NAS`, and observe at least two complete provider cycles plus one replay query. Roll back to `dual` on any mismatch.

- [ ] **Step 6: Apply six-month retention only after the live gate**

Run `dry-run` again and preserve its report. Then run:

```bash
python scripts/manage_postgis_retention.py apply
python scripts/manage_postgis_retention.py verify
```

Confirm no time-bound v2 row exceeds the cutoff and permanent catalog/checkpoint counts are unchanged. Apply the same bounded cutoff cleanup to legacy tables only after the rollback window is explicitly closed; do not drop entire legacy tables in this task.

- [ ] **Step 7: Record deployment evidence and commit**

Record versions, hashes, cutoff, per-table counts, dropped partitions, boundary deletes, catalog counts, importer status, worker status, and rollback instructions.

```bash
git add scripts/build_release_candidate.py docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md docs/DEPLOY_2026-09-25_POSTGIS_RETENTION.md tests/test_release_artifacts.py
git commit -m "release: enable six month PostGIS retention"
```
