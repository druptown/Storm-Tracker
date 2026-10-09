"""Closed subprocess harness for exact provider-capture crash boundaries.

Only routes with a complete local fixture and an explicit checkpoint allowlist
belong in ``_ACTIVE_SCENARIOS``.  The reserved names document the stable CLI
shape for later provider slices without claiming that those routes work yet.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import Callable, Sequence


_AEMET_CHECKPOINTS = frozenset(
    {
        "aemet.after_envelope_store",
        "aemet.after_data_store",
        "aemet.after_metadata_store",
        "aemet.before_capture_commit",
        "aemet.after_capture_commit",
    }
)

# These identifiers are intentionally not dispatchable until their fixture,
# checkpoint allowlist, and recovery assertions land together.
_RESERVED_SCENARIOS = frozenset(
    {
        "acquisition-recovery",
        "dwd-handoff",
        "hsaf-h40b",
        "meteofrance",
        "openmeteo",
    }
)


def _run_aemet(root: Path, checkpoint: str) -> None:
    if checkpoint not in _AEMET_CHECKPOINTS:
        raise SystemExit("AEMET checkpoint is not allowlisted")

    from storm_tracker_worker import aemet_native as native
    from storm_tracker_worker.raw_artifacts import RawArtifactStore

    key = "aemet-application-key-0123456789"
    data_url = "https://opendata.aemet.es/opendata/sh/dataSelectionToken001"
    metadata_url = "https://opendata.aemet.es/opendata/sh/metaSelectionToken001"
    envelope = json.dumps(
        {
            "descripcion": "exito",
            "estado": 200,
            "datos": data_url,
            "metadatos": metadata_url,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    data = b"II*\x00AEMET-EXACT-GEOTIFF-BYTES"
    metadata = b'{"format":"GeoTIFF","scale":"ESCALA"}'

    def response(payload: bytes, content_type: str, received_at: float):
        return native.AemetHttpResponse(
            payload=payload,
            content_type=content_type,
            received_at=received_at,
            content_length=len(payload),
            etag=None,
            last_modified=None,
        )

    native._api_key_from_environment = lambda: key
    native._fetch_envelope = lambda _key, **_kwargs: response(
        envelope, "application/json", 1_785_700_001.0
    )

    def fetch_download(_url: str, role: str, **_kwargs):
        if role == native.REQUIRED_ROLES[1]:
            return response(data, "image/tiff", 1_785_700_002.0)
        return response(metadata, "application/json", 1_785_700_003.0)

    native._fetch_download = fetch_download

    def crash(stage: str) -> None:
        if stage == checkpoint:
            os._exit(73)

    native.capture_latest_aemet_national_radar(
        SimpleNamespace(job_id="aemet-crash-job", payload={}),
        raw_store=RawArtifactStore(root),
        _fault_injector=crash,
    )
    raise RuntimeError("allowlisted AEMET checkpoint was not reached")


_ACTIVE_SCENARIOS: dict[str, Callable[[Path, str], None]] = {
    "aemet": _run_aemet,
}


def main(arguments: Sequence[str]) -> None:
    if len(arguments) != 3:
        raise SystemExit("usage: worker_capture_crash_child.py SCENARIO ROOT CHECKPOINT")
    scenario, root_value, checkpoint = arguments
    runner = _ACTIVE_SCENARIOS.get(scenario)
    if runner is None:
        if scenario in _RESERVED_SCENARIOS:
            raise SystemExit("capture-crash scenario is reserved but inactive")
        raise SystemExit("capture-crash scenario is not allowlisted")
    runner(Path(root_value), checkpoint)


if __name__ == "__main__":
    main(sys.argv[1:])
