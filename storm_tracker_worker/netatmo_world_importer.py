"""Standalone, resumable Netatmo world station-catalog importer."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
import json
import math
import os
import re
import sys
from typing import Any, Callable, Mapping

from .thin_provider_adapters import AioHttpFetcher, NetatmoOAuthTokenProvider
from .thin_station_bootstrap import (
    BootstrapBudget,
    NetatmoTileSource,
    NetatmoWorldCatalogRunner,
    world_netatmo_tiles,
)
from .thin_station_catalog import StationCatalogRepository


@dataclass(frozen=True, slots=True)
class ImporterConfig:
    minimum_call_interval_seconds: float = 10.0
    batch_calls: int = 6
    batch_maximum_bytes: int = 4 * 1024 * 1024 * 1024
    failure_retry_seconds: float = 300.0

    def __post_init__(self) -> None:
        values = (
            self.minimum_call_interval_seconds,
            self.failure_retry_seconds,
        )
        if not all(math.isfinite(float(value)) and float(value) > 0 for value in values):
            raise ValueError("netatmo_import_config_invalid")
        if self.maximum_calls_per_hour > 400:
            raise ValueError("netatmo_import_rate_limit_invalid")
        if self.batch_calls < 1 or self.batch_maximum_bytes < 1:
            raise ValueError("netatmo_import_config_invalid")

    @property
    def maximum_calls_per_hour(self) -> int:
        return math.floor(3600.0 / float(self.minimum_call_interval_seconds))

    @classmethod
    def from_environment(cls) -> "ImporterConfig":
        return cls(
            minimum_call_interval_seconds=float(os.getenv(
                "STV3_NETATMO_IMPORT_MINIMUM_CALL_INTERVAL_SECONDS", "10"
            )),
            batch_calls=int(os.getenv("STV3_NETATMO_IMPORT_BATCH_CALLS", "6")),
            batch_maximum_bytes=int(os.getenv(
                "STV3_NETATMO_IMPORT_BATCH_MAXIMUM_BYTES",
                str(4 * 1024 * 1024 * 1024),
            )),
            failure_retry_seconds=float(os.getenv(
                "STV3_NETATMO_IMPORT_FAILURE_RETRY_SECONDS", "300"
            )),
        )


def dry_run_report(config: ImporterConfig) -> dict[str, object]:
    return {
        "base_tile_count": len(world_netatmo_tiles()),
        "maximum_calls_per_hour": config.maximum_calls_per_hour,
        "minimum_call_interval_seconds": config.minimum_call_interval_seconds,
        "status": "dry_run",
    }


def _safe_reason(error: BaseException) -> str:
    message = str(error)
    if re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", message):
        return message
    return type(error).__name__


def postgis_connect_kwargs(
    environment: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Build the importer's narrow PostGIS connection without pipeline imports."""
    values = os.environ if environment is None else environment
    if str(values.get("STV3_POSTGIS_ENABLED", "false")).lower() not in {
        "1", "true", "yes", "on",
    }:
        raise RuntimeError("postgis_not_enabled")
    host = str(values.get("STV3_POSTGIS_HOST", "")).strip()
    database = str(values.get("STV3_POSTGIS_DATABASE", "")).strip()
    user = str(values.get("STV3_POSTGIS_USER", "")).strip()
    password = str(values.get("STV3_POSTGIS_PASSWORD", ""))
    if not all((host, database, user, password)):
        raise RuntimeError("postgis_config_incomplete")
    return {
        "host": host,
        "port": int(values.get("STV3_POSTGIS_PORT", "5432")),
        "dbname": database,
        "user": user,
        "password": password,
        "sslmode": str(values.get("STV3_POSTGIS_SSLMODE", "disable")),
        "connect_timeout": int(values.get("STV3_POSTGIS_CONNECT_TIMEOUT", "3")),
        "application_name": "storm_tracker_station_importer",
    }


def build_runner(config: ImporterConfig) -> NetatmoWorldCatalogRunner:
    import psycopg

    fetcher = AioHttpFetcher(timeout_seconds=30.0)
    token_provider = NetatmoOAuthTokenProvider(
        fetcher=fetcher,
        refresh_token_state_path=os.getenv(
            "STV3_NETATMO_TOKEN_STATE", "/state/netatmo-token.json"
        ),
    )
    return NetatmoWorldCatalogRunner(
        repository=StationCatalogRepository(
            psycopg.connect,
            postgis_connect_kwargs(),
        ),
        source=NetatmoTileSource(
            fetch_json=fetcher.get_json,
            access_token_provider=token_provider.get_access_token,
        ),
        minimum_call_interval_seconds=config.minimum_call_interval_seconds,
    )


async def run_importer(
    config: ImporterConfig,
    *,
    runner_factory: Callable[[ImporterConfig], NetatmoWorldCatalogRunner] = build_runner,
    sleep: Callable[[float], Any] = asyncio.sleep,
) -> int:
    runner = runner_factory(config)
    print(json.dumps({
        "batch_calls": config.batch_calls,
        "minimum_call_interval_seconds": config.minimum_call_interval_seconds,
        "status": "started",
    }, sort_keys=True), flush=True)
    while True:
        try:
            report = await runner.run(budget=BootstrapBudget(
                maximum_calls=config.batch_calls,
                maximum_bytes=config.batch_maximum_bytes,
            ))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            print(json.dumps({
                "reason_code": _safe_reason(error),
                "status": "retrying",
            }, sort_keys=True), flush=True)
            await sleep(config.failure_retry_seconds)
            continue
        log_report = asdict(report)
        if report.status == "budget_exhausted":
            log_report["status"] = "batch_complete"
            log_report["resume"] = "scheduled"
        print(json.dumps(log_report, sort_keys=True), flush=True)
        if report.status in {"complete", "already_complete"}:
            return 0
        await sleep(config.minimum_call_interval_seconds)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    config = ImporterConfig.from_environment()
    if _arguments().dry_run:
        print(json.dumps(dry_run_report(config), sort_keys=True))
        return 0
    return asyncio.run(run_importer(config))


if __name__ == "__main__":
    sys.exit(main())
