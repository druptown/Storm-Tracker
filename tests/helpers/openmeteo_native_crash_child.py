"""Hard-exit child for Open-Meteo native-capture recovery tests."""
from __future__ import annotations

import base64
import hashlib
import io
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace

from storm_tracker_worker import openmeteo_native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


EXIT_CODE = 73


class _Response:
    def __init__(self, payload: bytes, *, url: str) -> None:
        self._stream = io.BytesIO(payload)
        self._url = url
        self.status = 200
        self.headers = {
            "Content-Length": str(len(payload)),
            "Content-Type": "application/json",
        }

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def close(self) -> None:
        self._stream.close()

    def geturl(self) -> str:
        return self._url

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)


def _crash_at(expected: str):
    def crash(checkpoint: str) -> None:
        if checkpoint == expected:
            os._exit(EXIT_CODE)

    return crash


def _authority_identity(prefix: str, seed: str) -> str:
    token = base64.b32encode(hashlib.sha256(seed.encode("ascii")).digest())
    encoded = token.decode("ascii").lower().rstrip("=")
    chunks = [encoded[offset:offset + 8] for offset in range(0, len(encoded), 8)]
    return prefix + ":" + ":z:".join(chunks)


def _job() -> SimpleNamespace:
    return SimpleNamespace(
        job_id=_authority_identity("acq", "job-a"),
        payload={
            "contract": "stv3-openmeteo-target-query-batch-v1",
            "operational_handler_compatible": False,
            "queries": [
                {
                    "latitude": 50.85,
                    "longitude": 4.35,
                    "query_key": _authority_identity("aqt", "target-a"),
                    "query_kind": "local",
                }
            ],
            "target_resolution_required": True,
        },
    )


def _payload() -> bytes:
    minutely_time = [
        1_800_000_000 + 900 * index
        for index in range(openmeteo_native.FORECAST_MINUTELY_15)
    ]
    hourly_time = [
        1_800_000_000 + 3600 * index
        for index in range(openmeteo_native.FORECAST_HOURS)
    ]
    minutely = {"time": minutely_time}
    minutely.update({
        name: [0.0] * openmeteo_native.FORECAST_MINUTELY_15
        for name in openmeteo_native.MINUTELY_15
    })
    hourly = {"time": hourly_time}
    hourly.update({
        name: [0.0] * openmeteo_native.FORECAST_HOURS
        for name in openmeteo_native.HOURLY
    })
    payload = {
        "latitude": 50.85,
        "longitude": 4.35,
        "elevation": 30.0,
        "generationtime_ms": 1.25,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "current": {
            "time": 1_800_000_000,
            "interval": 900,
            "precipitation": 0.0,
        },
        "current_units": {
            "time": "unixtime",
            "interval": "seconds",
            "precipitation": "mm",
        },
        "minutely_15": minutely,
        "minutely_15_units": dict(openmeteo_native.EXPECTED_MINUTELY_15_UNITS),
        "hourly": hourly,
        "hourly_units": dict(openmeteo_native.EXPECTED_HOURLY_UNITS),
    }
    return json.dumps(payload, separators=(",", ":")).encode()


def _capture(root: Path, checkpoint: str) -> None:
    allowed = {
        "openmeteo.after_response_store",
        "openmeteo.before_capture_commit",
        "openmeteo.after_capture_commit",
    }
    if checkpoint not in allowed:
        raise SystemExit("unsupported Open-Meteo checkpoint")
    payload = _payload()

    def opener(request, *, timeout):
        del timeout
        return _Response(payload, url=request.full_url)

    openmeteo_native.capture_openmeteo_forecast(
        _job(),
        raw_store=RawArtifactStore(root),
        opener=opener,
        _fault_injector=_crash_at(checkpoint),
    )


if len(sys.argv) != 3:
    raise SystemExit("usage: child RAW_ROOT CHECKPOINT")

_capture(Path(sys.argv[1]), sys.argv[2])
raise SystemExit("crash checkpoint was not reached")
