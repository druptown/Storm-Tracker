#!/usr/bin/env python3
"""Run footprint/ground validation offline against PostGIS facts."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import sys
import time

from storm_tracker_worker.thin_postgis import ThinPostGISSettings
from storm_tracker_worker.thin_replay import ReplayRepository, run_replay


ALGORITHM_VERSION = "thin-footprint-validation-v2"
RADAR_LEADS_SECONDS = (300.0, 900.0, 1800.0, 2700.0, 3600.0)


def _timestamp(value: str) -> float:
    text = value.strip().replace("Z", "+00:00")
    try:
        return float(text)
    except ValueError:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="from_at", required=True, type=_timestamp)
    parser.add_argument("--until", dest="until_at", required=True, type=_timestamp)
    parser.add_argument("--target", help="Required for target/ground replay.")
    parser.add_argument("--provider")
    parser.add_argument(
        "--radar-only",
        action="store_true",
        help="Skip target/ground replay and validate only later radar frames.",
    )
    parser.add_argument("--dry-run", action="store_true")
    arguments = parser.parse_args()
    if not arguments.radar_only and not arguments.target:
        parser.error("--target is required unless --radar-only is used")
    return arguments


def main() -> int:
    arguments = _arguments()
    started_at = time.monotonic()
    try:
        settings = ThinPostGISSettings.from_environment()
        if settings is None:
            raise RuntimeError("postgis_not_enabled")
        import psycopg

        repository = ReplayRepository(psycopg.connect, settings.connect_kwargs())
        frames = () if arguments.radar_only else repository.load_frames(
            from_at=arguments.from_at,
            until_at=arguments.until_at,
            target_id=arguments.target,
            provider_id=arguments.provider,
        )
        radar_cells = repository.load_radar_cells(
            from_at=arguments.from_at,
            until_at=arguments.until_at,
            provider_id=arguments.provider,
        )
        report = run_replay(
            frames,
            algorithm_version=ALGORITHM_VERSION,
            radar_cells=radar_cells,
            radar_lead_seconds=RADAR_LEADS_SECONDS,
        )
        parameters = {
            "from_at": arguments.from_at,
            "until_at": arguments.until_at,
            "target_id": arguments.target,
            "provider_id": arguments.provider,
            "radar_only": arguments.radar_only,
            "radar_lead_seconds": RADAR_LEADS_SECONDS,
            "radar_match_tolerance_seconds": 150.0,
            "radar_maximum_match_distance_km": 75.0,
            "method": "archived_cell_constant_velocity",
            "cell_matching": "greedy_shape_overlap_then_distance",
            "limitations": [
                "archived_cells_not_raw_radar_reprocessing",
                "cell_reception_time_unavailable",
                "no_empty_frame_or_spatial_coverage_mask",
                "cell_matches_do_not_prove_storm_identity",
                "planar_lon_lat_area_approximation",
            ],
        }
        if not arguments.dry_run:
            repository.persist_report(report, parameters=parameters)
        result = {
            "status": "dry_run" if arguments.dry_run else "completed",
            "algorithm_version": ALGORITHM_VERSION,
            "replay_id": report.replay_id,
            "frame_count": len(frames),
            "passage_count": len(report.passages),
            "validation_count": len(report.validations),
            "radar_verification_count": len(report.radar_verifications),
            "radar_cell_count": len(radar_cells),
            "radar_frame_count": len({(c.provider_id, c.observed_at) for c in radar_cells}),
            "duration_seconds": round(time.monotonic() - started_at, 2),
            "parameters": parameters,
            "metrics": [{
                "metric_name": item.metric_name,
                "metric_value": item.metric_value,
                "sample_count": item.sample_count,
                "dimension": dict(item.dimension),
            } for item in report.metrics],
        }
    except Exception as error:
        result = {"status": "error", "reason_code": type(error).__name__}
        print(json.dumps(result, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
