# Station-Centric PostGIS Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store each physical ground-station measurement once and remove `target_id` from its persistence identity without interrupting the live Netatmo catalog importer.

**Architecture:** A provider-neutral conversion layer extracts canonical station measurements from existing `NormalizedObservation` objects. A new partitioned PostGIS table references the permanent station catalog; the writer stores station facts there and keeps non-station observations on their existing route. A resumable CLI backfills and verifies historical station rows without deleting legacy data.

**Tech Stack:** Python 3.13, dataclasses, psycopg 3, PostgreSQL 17, PostGIS 3.5, pytest

**Spec:** `docs/superpowers/specs/2026-09-25-station-centric-postgis-retention-design.md`

## Global Constraints

- Work only on NAS-Home Assistant `192.168.0.15`; never touch the NUC.
- Do not stop, restart, change, or delete the running Netatmo importer, its catalog rows, or its checkpoints.
- Do not delete or rewrite `stv3_observation` during this plan.
- Station identity is exactly `provider_id + station_id`.
- Measurement identity is exactly `provider_id + station_id + observed_at + measurement_type`; `target_id` is forbidden.
- All database changes are additive until live verification and an explicit cleanup gate.
- Runtime writers never execute DDL.
- No secrets or full provider payloads enter logs, diagnostics, or test fixtures.

## Review Focus

- A station seen through two targets must yield one stored measurement; Task 3 pins this with a two-target writer test.
- `wind_speed` and `wind_gust` at the same instant must remain distinct; Task 1 pins this in the canonical identity test.
- An observation without a valid station ID or measurement type must stay on the generic route; Task 1 tests both missing fields.
- A catalog FK failure must roll back the complete database cycle and preserve the retry queue; Task 3 extends the outage/retry test.
- Backfill restart after a partial batch must neither skip nor duplicate rows; Task 5 tests checkpoint resume and idempotence.

---

### Task 1: Canonical station-measurement contract

**Files:**
- Create: `storm_tracker_worker/thin_station_measurement.py`
- Create: `tests/test_thin_station_measurement.py`

**Interfaces:**
- Consumes: `NormalizedObservation` from `storm_tracker_worker.thin_observation`.
- Produces: `StationMeasurement.from_observation(observation) -> StationMeasurement | None` and `deduplicate_station_measurements(observations) -> tuple[StationMeasurement, ...]`.

- [ ] **Step 1: Write failing contract tests**

```python
def test_target_is_not_part_of_station_measurement_identity():
    first = _ground_observation(target_id="home", value=2.0)
    second = _ground_observation(target_id="child", value=2.0)
    rows = deduplicate_station_measurements((first, second))
    assert len(rows) == 1
    assert rows[0].identity == ("netatmo", "station-1", NOW, "rain_rate")


def test_wind_speed_and_gust_are_distinct_measurements():
    rows = deduplicate_station_measurements((
        _ground_observation(measurement="wind_speed", kind=ObservationKind.WIND_SPEED),
        _ground_observation(measurement="wind_gust", kind=ObservationKind.WIND_SPEED),
    ))
    assert {row.measurement_type for row in rows} == {"wind_speed", "wind_gust"}


@pytest.mark.parametrize("metadata", ({"measurement": "rain_rate"}, {"station_id": "s1"}))
def test_incomplete_station_metadata_stays_outside_station_route(metadata):
    assert StationMeasurement.from_observation(_observation(metadata=metadata)) is None
```

- [ ] **Step 2: Run the focused tests and verify failure**

Run: `pytest -q tests/test_thin_station_measurement.py`

Expected: collection fails because `thin_station_measurement` does not exist.

- [ ] **Step 3: Implement the immutable contract and deterministic conflict rule**

```python
@dataclass(frozen=True, slots=True)
class StationMeasurement:
    provider_id: str
    station_id: str
    product_id: str
    measurement_type: str
    value: float
    unit: str
    observed_at: float
    received_at: float
    quality: ObservationQuality
    metadata: Mapping[str, Any]

    @property
    def identity(self) -> tuple[str, str, float, str]:
        return (self.provider_id, self.station_id, self.observed_at, self.measurement_type)

    @classmethod
    def from_observation(cls, observation: NormalizedObservation) -> "StationMeasurement | None":
        station_id = str(observation.metadata.get("station_id", "")).strip()
        measurement_type = str(observation.metadata.get("measurement", "")).strip().lower()
        if not station_id or not measurement_type:
            return None
        return cls(
            provider_id=observation.provider_id,
            station_id=station_id,
            product_id=observation.product_id,
            measurement_type=measurement_type,
            value=observation.value,
            unit=observation.unit,
            observed_at=observation.observed_at,
            received_at=observation.received_at,
            quality=observation.quality,
            metadata=observation.metadata,
        )
```

Use the explicit rank `good > suspect > poor > unknown`; for equal quality retain the greatest `received_at`. Reject non-finite timestamps and measurement types outside the catalog capability vocabulary instead of inventing aliases.

- [ ] **Step 4: Run the focused tests**

Run: `pytest -q tests/test_thin_station_measurement.py`

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add storm_tracker_worker/thin_station_measurement.py tests/test_thin_station_measurement.py
git commit -m "feat: define canonical station measurements"
```

### Task 2: Add the partitioned station-measurement schema

**Files:**
- Create: `storm_tracker_worker/postgis_migrations/0005_station_measurements.sql`
- Modify: `tests/test_postgis_migrations.py`

**Interfaces:**
- Consumes: permanent `stv3_ground_station(provider_id, station_id)` from migration `0003`.
- Produces: `stv3_station_measurement`, monthly partitions, and `stv3_station_measurement_backfill_state`.

- [ ] **Step 1: Write failing SQL contract tests**

```python
def test_station_measurement_migration_is_target_independent_and_partitioned():
    sql = Path("storm_tracker_worker/postgis_migrations/0005_station_measurements.sql").read_text()
    normalized = " ".join(sql.lower().split())
    assert "partition by range (observed_at)" in normalized
    assert "primary key(provider_id,station_id,observed_at,measurement_type)" in normalized
    assert "references public.stv3_ground_station(provider_id,station_id)" in normalized
    station_table = normalized.split("create table public.stv3_station_measurement", 1)[1]
    assert "target_id" not in station_table.split("create table", 1)[0]
```

- [ ] **Step 2: Run the test and verify failure**

Run: `pytest -q tests/test_postgis_migrations.py`

Expected: failure because migration `0005` is absent.

- [ ] **Step 3: Create migration `0005`**

Define the parent with this key structure:

```sql
CREATE TABLE public.stv3_station_measurement(
    provider_id text NOT NULL,
    station_id text NOT NULL,
    observed_at timestamptz NOT NULL,
    measurement_type text NOT NULL,
    product_id text NOT NULL,
    value double precision NOT NULL,
    unit text NOT NULL,
    received_at timestamptz NOT NULL,
    quality text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(provider_id,station_id,observed_at,measurement_type),
    FOREIGN KEY(provider_id,station_id)
      REFERENCES public.stv3_ground_station(provider_id,station_id)
      ON DELETE RESTRICT
) PARTITION BY RANGE(observed_at);
```

The migration creates monthly partitions covering the previous six calendar months, the current month, and next month in an idempotent `DO` block. Add parent indexes on `(provider_id, station_id, observed_at DESC)` and `(measurement_type, observed_at DESC)`. Grant only `SELECT, INSERT, UPDATE` to `storm_ingest`. The backfill-state table stores a singleton source cursor, copied count, rejected count, status, and `updated_at`; it contains no provider payload.

- [ ] **Step 4: Run migration tests**

Run: `pytest -q tests/test_postgis_migrations.py`

Expected: all tests pass and migration `0005_station_measurements` is registered.

- [ ] **Step 5: Commit**

```bash
git add storm_tracker_worker/postgis_migrations/0005_station_measurements.sql tests/test_postgis_migrations.py
git commit -m "feat: add partitioned station measurement schema"
```

### Task 3: Route station facts through the new writer path

**Files:**
- Modify: `storm_tracker_worker/thin_postgis.py`
- Modify: `tests/test_thin_postgis.py`

**Interfaces:**
- Consumes: `deduplicate_station_measurements()` from Task 1 and table from Task 2.
- Produces: `station_measurements` count in `PostGISWriteResult.written_rows`; non-station observations retain the existing `stv3_observation` path.

- [ ] **Step 1: Add failing two-target and mixed-route tests**

```python
@pytest.mark.asyncio
async def test_writer_stores_one_physical_station_measurement_for_two_targets():
    cycle, nowcast = _cycle_with_duplicate_station_observations("home", "child")
    factory = ConnectionFactory()
    result = await ThinPostGISWriter(_settings(), connect_factory=factory).write_cycle(cycle, nowcast)
    batches = [rows for sql, rows in factory.batch_calls if sql.startswith("INSERT INTO stv3_station_measurement")]
    assert len(batches) == 1
    assert len(batches[0]) == 1
    assert "home" not in batches[0][0]
    assert "child" not in batches[0][0]
    assert result.written_rows["station_measurements"] == 1


@pytest.mark.asyncio
async def test_writer_keeps_nonstation_radar_on_generic_observation_route():
    cycle, nowcast = _cycle_and_nowcast(100.0)
    factory = ConnectionFactory()
    await ThinPostGISWriter(_settings(), connect_factory=factory).write_cycle(cycle, nowcast)
    assert any(sql.startswith("INSERT INTO stv3_observation") for sql, _ in factory.statements)
```

- [ ] **Step 2: Run focused writer tests and verify failure**

Run: `pytest -q tests/test_thin_postgis.py -k 'station_measurement or nonstation'`

Expected: no station-measurement insert exists.

- [ ] **Step 3: Add the station upsert and split each batch before SQL execution**

Use this conflict target and rule:

```sql
ON CONFLICT(provider_id,station_id,observed_at,measurement_type) DO UPDATE SET
    product_id=EXCLUDED.product_id,
    value=CASE WHEN stv3_quality_rank(EXCLUDED.quality) >= stv3_quality_rank(stv3_station_measurement.quality)
               THEN EXCLUDED.value ELSE stv3_station_measurement.value END,
    unit=CASE WHEN stv3_quality_rank(EXCLUDED.quality) >= stv3_quality_rank(stv3_station_measurement.quality)
              THEN EXCLUDED.unit ELSE stv3_station_measurement.unit END,
    received_at=GREATEST(stv3_station_measurement.received_at,EXCLUDED.received_at),
    quality=CASE WHEN stv3_quality_rank(EXCLUDED.quality) >= stv3_quality_rank(stv3_station_measurement.quality)
                 THEN EXCLUDED.quality ELSE stv3_station_measurement.quality END,
    metadata=CASE WHEN stv3_quality_rank(EXCLUDED.quality) >= stv3_quality_rank(stv3_station_measurement.quality)
                  THEN EXCLUDED.metadata ELSE stv3_station_measurement.metadata END,
    updated_at=clock_timestamp()
```

Define `stv3_quality_rank(text)` in migration `0005` as an immutable SQL function. Do not create partitions or other DDL in `ThinPostGISWriter`.

- [ ] **Step 4: Prove transaction rollback and retry behavior**

Extend `test_outage_is_degraded_and_next_call_reconnects_and_drains_queue` so a station-FK exception produces `postgis_unavailable`, retains the queued cycle, and the next successful call writes one canonical row.

Run: `pytest -q tests/test_thin_postgis.py tests/test_thin_station_measurement.py`

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add storm_tracker_worker/thin_postgis.py storm_tracker_worker/postgis_migrations/0005_station_measurements.sql tests/test_thin_postgis.py
git commit -m "feat: persist station facts once"
```

### Task 4: Read ground replay data from canonical station measurements

**Files:**
- Modify: `storm_tracker_worker/thin_replay.py`
- Modify: `tests/test_thin_replay.py`

**Interfaces:**
- Consumes: station measurements joined to `stv3_ground_station.position` and target positions.
- Produces: unchanged `ReplayFrame.ground_observations` contract, with the requested target attached only in memory.

- [ ] **Step 1: Write a failing replay-query test**

```python
def test_replay_loads_ground_data_by_space_and_time_without_target_copies():
    repository = ReplayRepository(_factory_with_station_rows(), {})
    frames = repository.load_frames(target_id="home", start_at=100.0, end_at=200.0)
    assert frames[0].ground_observations[0].target_id == "home"
    sql = _last_ground_query()
    assert "FROM stv3_station_measurement" in sql
    assert "JOIN stv3_ground_station" in sql
    assert "measurement.target_id" not in sql
```

- [ ] **Step 2: Run the test and verify it fails on the legacy query**

Run: `pytest -q tests/test_thin_replay.py -k ground`

- [ ] **Step 3: Replace `_SELECT_GROUND`**

Join station measurements to the station catalog, constrain by the replay time window and `ST_DWithin` around the recorded target position, and map `measurement_type='rain_rate'` back to `NormalizedObservation`. Attach the replay's requested `target_id` only while constructing the in-memory object.

- [ ] **Step 4: Run replay and validation tests**

Run: `pytest -q tests/test_thin_replay.py tests/test_thin_ground_validation.py`

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add storm_tracker_worker/thin_replay.py tests/test_thin_replay.py
git commit -m "feat: replay canonical station measurements"
```

### Task 5: Build resumable, non-destructive backfill and verification

**Files:**
- Create: `storm_tracker_worker/thin_postgis_station_migration.py`
- Create: `scripts/manage_postgis_station_storage.py`
- Create: `tests/test_thin_postgis_station_migration.py`

**Interfaces:**
- Produces: `BackfillReport`, `VerificationReport`, `backfill_batch(limit: int)`, `verify(window_start, window_end)`, and CLI commands `dry-run`, `backfill`, `verify`.
- Consumes: legacy `stv3_observation` and canonical `stv3_station_measurement`.

- [ ] **Step 1: Write failing repository tests**

```python
def test_backfill_resumes_after_last_observation_key_and_deduplicates_targets():
    repository = _repository_with_two_target_copies()
    first = repository.backfill_batch(limit=1)
    second = repository.backfill_batch(limit=100)
    assert first.status == "partial"
    assert second.status == "complete"
    assert repository.canonical_count() == 1


def test_dry_run_never_executes_insert_update_delete_or_drop():
    repository = _repository_with_legacy_rows()
    repository.dry_run()
    assert not [sql for sql in repository.sql if sql.startswith(("INSERT", "UPDATE", "DELETE", "DROP"))]
```

- [ ] **Step 2: Run the focused test and verify failure**

Run: `pytest -q tests/test_thin_postgis_station_migration.py`

- [ ] **Step 3: Implement keyset-pagination backfill**

Select only legacy rows containing both `metadata->>'station_id'` and `metadata->>'measurement'`, ordered by `observation_key`, using `WHERE observation_key > %s ORDER BY observation_key LIMIT %s`. Insert with the same canonical upsert as the live writer. Update the checkpoint only in the same committed transaction as the inserted batch. Record unrecognizable counts without copying their payloads.

- [ ] **Step 4: Implement verification**

`verify()` must report, per provider and measurement type:

- distinct legacy physical identities;
- canonical count;
- minimum/maximum time and value;
- duplicate reduction count;
- rows missing station ID or measurement type;
- foreign-key orphan count, which must be zero.

Exit code is `0` only when all recognizable legacy identities in the requested window exist canonically and no orphans exist. It performs no delete.

- [ ] **Step 5: Run tests**

Run: `pytest -q tests/test_thin_postgis_station_migration.py tests/test_thin_postgis.py tests/test_thin_replay.py`

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add storm_tracker_worker/thin_postgis_station_migration.py scripts/manage_postgis_station_storage.py tests/test_thin_postgis_station_migration.py
git commit -m "feat: add resumable station measurement backfill"
```

### Task 6: Package, deploy, and prove the additive cutover

**Files:**
- Modify: `scripts/build_release_candidate.py`
- Modify: `docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md`
- Create: `docs/DEPLOY_2026-09-25_STATION_STORAGE.md`
- Test: `tests/test_release_artifacts.py`

**Interfaces:**
- Consumes: migration `0005`, updated worker, and management CLI.
- Produces: a release artifact containing all three and a rollback record.

- [ ] **Step 1: Add a failing packaging test**

Assert that the worker archive contains migration `0005_station_measurements.sql`, `thin_station_measurement.py`, `thin_postgis_station_migration.py`, and the management CLI.

Run: `pytest -q tests/test_release_artifacts.py -k postgis`

- [ ] **Step 2: Update packaging and run the focused release tests**

Run: `pytest -q tests/test_release_artifacts.py tests/test_postgis_migrations.py`

Expected: all tests pass.

- [ ] **Step 3: Run the complete relevant local suite**

Run:

```bash
pytest -q \
  tests/test_thin_station_measurement.py \
  tests/test_thin_postgis.py \
  tests/test_thin_postgis_station_migration.py \
  tests/test_thin_replay.py \
  tests/test_netatmo_world_importer.py \
  tests/test_thin_station_catalog.py \
  tests/test_postgis_migrations.py
```

Expected: all tests pass; importer tests prove no coupling was added.

- [ ] **Step 4: Follow the fixed NAS deployment procedure**

Use `docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md` and `scripts/install_ha_release_pair_ssh.py`. Apply migration `0005` before starting the new writer. Make a rollback backup. Do not restart or modify the importer container.

- [ ] **Step 5: Run live additive verification**

Run the management CLI in this order:

```bash
python scripts/manage_postgis_station_storage.py dry-run
python scripts/manage_postgis_station_storage.py backfill --batch-size 5000
python scripts/manage_postgis_station_storage.py verify --months 6
```

Confirm one full provider cycle, current worker heartbeat, `postgis=current`, zero failed jobs, a growing canonical station table, and no duplicate identity rows. Compare importer station/checkpoint counts before and after; they must continue growing or remain unchanged without reset.

- [ ] **Step 6: Record evidence and commit**

Do not delete the legacy table or duplicate rows. Record versions, hashes, counts, verification result, and rollback reference in `docs/DEPLOY_2026-09-25_STATION_STORAGE.md`.

```bash
git add scripts/build_release_candidate.py docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md docs/DEPLOY_2026-09-25_STATION_STORAGE.md tests/test_release_artifacts.py
git commit -m "release: deploy additive station storage"
```
