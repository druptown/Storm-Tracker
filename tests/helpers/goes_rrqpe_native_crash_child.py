"""Abrupt-process crash harness for GOES RRQPE native schema-3 recovery."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import runpy
import sys
import time

from storm_tracker_worker import goes_rrqpe_native as native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


root = Path(sys.argv[1])
target_checkpoint = sys.argv[2]
repo_root = Path(__file__).resolve().parents[2]
fixtures = runpy.run_path(
    str(repo_root / "tests" / "test_worker_goes_rrqpe_native_capture.py")
)
previous, current, netcdf = fixtures["_fixture_payloads"](18)
response = fixtures["_response"]
job = fixtures["_job"]()


def fetch_listing(satellite, prefix, *, deadline):
    assert satellite == 18 and deadline > time.monotonic()
    payload = previous if prefix == fixtures["PREFIX_PREVIOUS"] else current
    return response(payload, "application/xml", 1_785_651_001.0)


def fetch_object(selected, *, deadline):
    assert selected.satellite == 18 and deadline > time.monotonic()
    return response(
        netcdf,
        "binary/octet-stream",
        1_785_651_003.0,
        etag=fixtures["ETAG"],
        last_modified=fixtures["LAST_MODIFIED"],
    )


def crash(checkpoint):
    if checkpoint == target_checkpoint:
        os._exit(73)


native._fetch_listing = fetch_listing
native._fetch_object = fetch_object
native.capture_latest_goes_rrqpe(
    job,
    satellite=18,
    raw_store=RawArtifactStore(root),
    now=datetime(2026, 8, 2, 7, 40, tzinfo=timezone.utc),
    _fault_injector=crash,
)
raise RuntimeError(f"checkpoint was not reached: {target_checkpoint}")
