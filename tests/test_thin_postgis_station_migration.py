from __future__ import annotations

from pathlib import Path
import os
import subprocess
import sys

from storm_tracker_worker.thin_postgis_station_migration import (
    StationMeasurementMigrationRepository,
)


NOW = 1_800_000_000.0


def _legacy_row(observation_key: str):
    return (
        observation_key,
        "netatmo",
        "getpublicdata",
        2.5,
        "mm/h",
        NOW,
        NOW + 1.0,
        "good",
        {"station_id": "station-1", "measurement": "rain_rate"},
        True,
    )


class _Database:
    def __init__(self, rows=()):
        self.rows = tuple(rows)
        self.state = (None, 0, 0, "pending")
        self.canonical = {}
        self.sql = []
        self.commits = 0
        self.rollbacks = 0


class _Cursor:
    def __init__(self, database):
        self.database = database
        self.result = ()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, parameters=()):
        normalized = " ".join(sql.split())
        self.database.sql.append(normalized)
        if "FROM stv3_station_measurement_backfill_state" in normalized:
            self.result = (self.database.state,)
        elif normalized.startswith("SELECT observation.observation_key"):
            source_cursor, repeated_cursor, fetch_limit = parameters
            assert repeated_cursor == source_cursor
            rows = [
                row for row in self.database.rows
                if source_cursor is None or row[0] > source_cursor
            ]
            self.result = tuple(rows[:fetch_limit])
        elif normalized.startswith(
            "UPDATE stv3_station_measurement_backfill_state"
        ):
            cursor, copied, rejected, status, _migration_id = parameters
            self.database.state = (cursor, copied, rejected, status)
            self.result = ()
        elif "AS legacy_distinct" in normalized:
            self.result = ((
                "netatmo", "rain_rate", 1, len(self.database.canonical),
                NOW, NOW, 2.5, 2.5, max(0, len(self.database.rows) - 1),
            ),)
        elif "AS unrecognizable_rows" in normalized:
            self.result = ((0,),)
        elif "AS orphan_count" in normalized:
            self.result = ((0,),)
        else:
            self.result = ()

    def executemany(self, sql, parameters):
        normalized = " ".join(sql.split())
        self.database.sql.append(normalized)
        for row in parameters:
            identity = (row[0], row[1], row[2], row[3])
            self.database.canonical[identity] = row

    def fetchone(self):
        return None if not self.result else self.result[0]

    def fetchall(self):
        return self.result


class _Connection:
    def __init__(self, database):
        self.database = database

    def cursor(self):
        return _Cursor(self.database)

    def commit(self):
        self.database.commits += 1

    def rollback(self):
        self.database.rollbacks += 1

    def close(self):
        pass


def _repository(database):
    return StationMeasurementMigrationRepository(
        lambda **kwargs: _Connection(database),
        {},
    )


def test_backfill_resumes_after_last_observation_key_and_deduplicates_targets():
    database = _Database((_legacy_row("a"), _legacy_row("b")))
    repository = _repository(database)

    first = repository.backfill_batch(limit=1)
    second = repository.backfill_batch(limit=100)

    assert first.status == "partial"
    assert first.source_cursor == "a"
    assert second.status == "complete"
    assert second.source_cursor == "b"
    assert len(database.canonical) == 1
    assert database.commits == 2


def test_dry_run_never_executes_insert_update_delete_or_drop():
    database = _Database((_legacy_row("a"),))
    repository = _repository(database)

    report = repository.dry_run(window_start=NOW - 3600.0, window_end=NOW + 1.0)

    assert report.status == "ready"
    assert not [
        sql for sql in database.sql
        if sql.startswith(("INSERT", "UPDATE", "DELETE", "DROP"))
    ]
    assert database.commits == 0


def test_verify_reports_duplicate_reduction_and_zero_orphans():
    database = _Database((_legacy_row("a"), _legacy_row("b")))
    database.canonical[("netatmo", "station-1", NOW, "rain_rate")] = object()
    repository = _repository(database)

    report = repository.verify(
        window_start=NOW - 3600.0,
        window_end=NOW + 1.0,
    )

    assert report.status == "verified"
    assert report.orphan_count == 0
    assert report.unrecognizable_rows == 0
    assert report.buckets[0].legacy_distinct == 1
    assert report.buckets[0].canonical_count == 1
    assert report.buckets[0].duplicate_reduction_count == 1


def test_management_cli_can_start_from_the_repository_root():
    root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "scripts/manage_postgis_station_storage.py", "--help"],
        cwd=root,
        env={key: value for key, value in os.environ.items() if key != "PYTHONPATH"},
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "dry-run" in result.stdout
    assert "backfill" in result.stdout
    assert "verify" in result.stdout
