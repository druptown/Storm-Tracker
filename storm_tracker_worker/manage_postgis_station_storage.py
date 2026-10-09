"""CLI for additive station-measurement migration and verification."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import json
import sys

from .thin_postgis import ThinPostGISSettings
from .thin_postgis_station_migration import (
    StationMeasurementMigrationRepository,
)


def _calendar_months_before(value: datetime, months: int) -> datetime:
    absolute = value.year * 12 + value.month - 1 - months
    year, zero_based_month = divmod(absolute, 12)
    month = zero_based_month + 1
    leap_year = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    month_lengths = (
        31, 29 if leap_year else 28, 31, 30, 31, 30,
        31, 31, 30, 31, 30, 31,
    )
    return value.replace(
        year=year,
        month=month,
        day=min(value.day, month_lengths[month - 1]),
    )


def _repository() -> StationMeasurementMigrationRepository:
    settings = ThinPostGISSettings.from_environment()
    if settings is None:
        raise RuntimeError("postgis_disabled")
    import psycopg
    return StationMeasurementMigrationRepository(
        psycopg.connect,
        settings.connect_kwargs(),
    )


def _window(months: int) -> tuple[float, float]:
    now = datetime.now(timezone.utc)
    return (_calendar_months_before(now, months).timestamp(), now.timestamp())


def _write_report(report) -> None:
    print(json.dumps(asdict(report), sort_keys=True, separators=(",", ":")))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("dry-run", "verify"):
        child = subparsers.add_parser(command)
        child.add_argument("--months", type=int, default=6)
    backfill = subparsers.add_parser("backfill")
    backfill.add_argument("--batch-size", type=int, default=5_000)
    backfill.add_argument("--max-batches", type=int, default=0)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    repository = _repository()
    if arguments.command in {"dry-run", "verify"}:
        if not 1 <= arguments.months <= 120:
            raise ValueError("months_invalid")
        start, end = _window(arguments.months)
        report = (
            repository.dry_run(window_start=start, window_end=end)
            if arguments.command == "dry-run"
            else repository.verify(window_start=start, window_end=end)
        )
        _write_report(report)
        return 0 if report.status in {"ready", "verified"} else 2

    batches = 0
    while True:
        report = repository.backfill_batch(limit=arguments.batch_size)
        _write_report(report)
        batches += 1
        if report.status == "complete":
            return 0
        if arguments.max_batches > 0 and batches >= arguments.max_batches:
            return 3


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["main"]
