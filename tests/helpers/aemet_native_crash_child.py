"""Abrupt-process harness for AEMET schema-3 partial-capture recovery."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace

from storm_tracker_worker import aemet_native as native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


KEY = "aemet-application-key-0123456789"
DATA_URL = "https://opendata.aemet.es/opendata/sh/dataSelectionToken001"
METADATA_URL = "https://opendata.aemet.es/opendata/sh/metaSelectionToken001"
ENVELOPE = json.dumps(
    {
        "descripcion": "exito",
        "estado": 200,
        "datos": DATA_URL,
        "metadatos": METADATA_URL,
    },
    separators=(",", ":"),
).encode("utf-8")
DATA = b"II*\x00AEMET-EXACT-GEOTIFF-BYTES"
METADATA = b'{"format":"GeoTIFF","scale":"ESCALA"}'


def response(payload: bytes, content_type: str, received_at: float):
    return native.AemetHttpResponse(
        payload=payload,
        content_type=content_type,
        received_at=received_at,
        content_length=len(payload),
        etag=None,
        last_modified=None,
    )


native._api_key_from_environment = lambda: KEY
native._fetch_envelope = lambda _key, **_kwargs: response(
    ENVELOPE, "application/json", 1_785_700_001.0
)


def fetch_download(_url: str, role: str, **_kwargs):
    if role == native.REQUIRED_ROLES[1]:
        return response(DATA, "image/tiff", 1_785_700_002.0)
    return response(METADATA, "application/json", 1_785_700_003.0)


native._fetch_download = fetch_download
checkpoint = sys.argv[2]


def crash(stage: str) -> None:
    if stage == checkpoint:
        os._exit(23)


native.capture_latest_aemet_national_radar(
    SimpleNamespace(job_id="aemet-crash-job", payload={}),
    raw_store=RawArtifactStore(Path(sys.argv[1])),
    _fault_injector=crash,
)
raise SystemExit(0)
