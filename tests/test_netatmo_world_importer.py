from __future__ import annotations

import json

import pytest

from storm_tracker_worker.netatmo_world_importer import (
    ImporterConfig,
    dry_run_report,
    postgis_connect_kwargs,
    run_importer,
)
from storm_tracker_worker.thin_station_bootstrap import BootstrapReport


def test_importer_default_budget_stays_below_netatmo_user_quota():
    config = ImporterConfig()

    assert config.minimum_call_interval_seconds == 10.0
    assert config.batch_calls == 6
    assert config.maximum_calls_per_hour == 360
    assert config.maximum_calls_per_hour <= 400


@pytest.mark.asyncio
async def test_importer_logs_startup_and_each_completed_batch(capsys):
    budgets = []

    class Runner:
        async def run(self, *, budget):
            budgets.append(budget)
            return BootstrapReport(
                provider_id="netatmo",
                status="complete",
                calls=2,
                response_bytes=1234,
                stations_written=8,
                completed_tiles=2,
                skipped_tiles=0,
                remaining_tiles=0,
            )

    result = await run_importer(
        ImporterConfig(), runner_factory=lambda _config: Runner(),
    )

    assert result == 0
    assert budgets[0].maximum_calls == 6
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines[0] == {
        "batch_calls": 6,
        "minimum_call_interval_seconds": 10.0,
        "status": "started",
    }
    assert lines[1]["status"] == "complete"
    assert lines[1]["stations_written"] == 8


@pytest.mark.asyncio
async def test_expected_call_budget_is_logged_as_batch_complete(capsys):
    reports = iter((
        BootstrapReport(
            provider_id="netatmo", status="budget_exhausted", calls=6,
            response_bytes=300, stations_written=2, completed_tiles=6,
            skipped_tiles=0, remaining_tiles=61_194,
        ),
        BootstrapReport(
            provider_id="netatmo", status="complete", calls=1,
            response_bytes=50, stations_written=0, completed_tiles=1,
            skipped_tiles=0, remaining_tiles=0,
        ),
    ))

    class Runner:
        async def run(self, *, budget):
            del budget
            return next(reports)

    await run_importer(
        ImporterConfig(),
        runner_factory=lambda _config: Runner(),
        sleep=lambda _seconds: __import__("asyncio").sleep(0),
    )

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines[1]["status"] == "batch_complete"
    assert lines[1]["resume"] == "scheduled"
    assert lines[2]["status"] == "complete"


def test_importer_rejects_interval_that_exceeds_400_calls_per_hour():
    with pytest.raises(ValueError, match="netatmo_import_rate_limit_invalid"):
        ImporterConfig(minimum_call_interval_seconds=8.0)


def test_dry_run_reports_complete_world_without_credentials():
    report = dry_run_report(ImporterConfig())

    assert report == {
        "base_tile_count": 61_200,
        "maximum_calls_per_hour": 360,
        "minimum_call_interval_seconds": 10.0,
        "status": "dry_run",
    }
    serialized = json.dumps(report).lower()
    assert "token" not in serialized
    assert "password" not in serialized
    assert "secret" not in serialized


def test_standalone_compose_has_no_home_assistant_or_mqtt_dependency():
    compose = (
        __import__("pathlib").Path(__file__).parents[1]
        / "station_catalog_importer"
        / "compose.yaml"
    ).read_text(encoding="utf-8").lower()

    assert "homeassistant" not in compose
    assert "mqtt" not in compose
    assert "network_mode: host" not in compose


def test_importer_builds_postgis_connection_without_loading_runtime_pipeline():
    kwargs = postgis_connect_kwargs({
        "STV3_POSTGIS_ENABLED": "true",
        "STV3_POSTGIS_HOST": "192.168.0.21",
        "STV3_POSTGIS_PORT": "5433",
        "STV3_POSTGIS_DATABASE": "storm_tracker",
        "STV3_POSTGIS_USER": "storm_ingest",
        "STV3_POSTGIS_PASSWORD": "private",
        "STV3_POSTGIS_SSLMODE": "disable",
    })

    assert kwargs == {
        "host": "192.168.0.21",
        "port": 5433,
        "dbname": "storm_tracker",
        "user": "storm_ingest",
        "password": "private",
        "sslmode": "disable",
        "connect_timeout": 3,
        "application_name": "storm_tracker_station_importer",
    }


def test_importer_rejects_incomplete_postgis_connection():
    with pytest.raises(RuntimeError, match="postgis_config_incomplete"):
        postgis_connect_kwargs({"STV3_POSTGIS_ENABLED": "true"})
