#!/usr/bin/env python3
"""Populate the thin PostGIS station catalog outside worker startup."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
import re
import sys
import time

from storm_tracker_worker.thin_postgis import ThinPostGISSettings
from storm_tracker_worker.thin_provider_adapters import (
    AioHttpFetcher,
    NetatmoOAuthTokenProvider,
)
from storm_tracker_worker.thin_station_bootstrap import (
    BootstrapBudget,
    GeographicTile,
    METAR_STATION_CACHE_URL,
    NetatmoTileSource,
    SinglePageSource,
    StationCatalogBootstrap,
    geographic_tiles,
    load_wunderground_seed_stations,
    parse_metar_station_cache,
)
from storm_tracker_worker.thin_station_catalog import StationCatalogRepository


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--provider", required=True,
        choices=("metar", "netatmo", "wunderground"),
    )
    parser.add_argument("--resume", action="store_true", default=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-calls", type=int, default=20)
    parser.add_argument("--max-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--south", type=float, default=-85.0)
    parser.add_argument("--north", type=float, default=85.0)
    parser.add_argument("--west", type=float, default=-180.0)
    parser.add_argument("--east", type=float, default=180.0)
    parser.add_argument("--tile-degrees", type=float, default=1.0)
    parser.add_argument("--delay-seconds", type=float)
    return parser.parse_args()


def _single_tile(provider_id: str) -> tuple[GeographicTile, ...]:
    return (GeographicTile(
        tile_id=f"{provider_id}:global",
        south=-85.0,
        north=85.0,
        west=-180.0,
        east=180.0,
    ),)


async def _run(arguments: argparse.Namespace) -> dict[str, object]:
    if arguments.provider == "netatmo":
        tiles = geographic_tiles(
            south=arguments.south,
            north=arguments.north,
            west=arguments.west,
            east=arguments.east,
            tile_degrees=arguments.tile_degrees,
        )
    else:
        tiles = _single_tile(arguments.provider)
    if arguments.dry_run:
        return {
            "provider_id": arguments.provider,
            "status": "dry_run",
            "tile_count": len(tiles),
            "maximum_calls": arguments.max_calls,
            "maximum_bytes": arguments.max_bytes,
            "delay_seconds": (
                8.0 if arguments.provider == "netatmo"
                and arguments.delay_seconds is None
                else float(arguments.delay_seconds or 0.0)
            ),
        }

    settings = ThinPostGISSettings.from_environment()
    if settings is None:
        raise RuntimeError("postgis_not_enabled")
    import psycopg

    repository = StationCatalogRepository(psycopg.connect, settings.connect_kwargs())
    fetcher = AioHttpFetcher(timeout_seconds=30.0)
    now = time.time()
    if arguments.provider == "metar":
        payload = await fetcher.get_bytes(
            METAR_STATION_CACHE_URL,
            maximum_bytes=64 * 1024 * 1024,
        )
        source = SinglePageSource(
            provider_id="metar",
            stations=parse_metar_station_cache(payload, discovered_at=now),
            response_bytes=len(payload),
        )
    elif arguments.provider == "wunderground":
        seed_path = (
            Path(__file__).parents[1]
            / "storm_tracker_worker"
            / "wunderground_seed_stations.json"
        )
        source = SinglePageSource(
            provider_id="wunderground",
            stations=load_wunderground_seed_stations(seed_path, discovered_at=now),
            response_bytes=seed_path.stat().st_size,
        )
    else:
        token = await NetatmoOAuthTokenProvider(fetcher=fetcher).get_access_token()
        if not token:
            raise RuntimeError("netatmo_access_token_missing")
        source = NetatmoTileSource(
            fetch_json=fetcher.get_json,
            access_token=token,
        )
    report = await StationCatalogBootstrap(
        repository=repository,
        source=source,
        minimum_call_interval_seconds=(
            8.0 if arguments.provider == "netatmo"
            and arguments.delay_seconds is None
            else float(arguments.delay_seconds or 0.0)
        ),
    ).run(
        tiles,
        budget=BootstrapBudget(
            maximum_calls=arguments.max_calls,
            maximum_bytes=arguments.max_bytes,
        ),
    )
    return asdict(report)


def main() -> int:
    try:
        result = asyncio.run(_run(_arguments()))
    except Exception as error:
        message = str(error)
        print(json.dumps({
            "status": "error",
            "reason_code": (
                message
                if re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", message)
                else type(error).__name__
            ),
        }, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
