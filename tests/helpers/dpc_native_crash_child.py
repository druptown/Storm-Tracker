"""Abrupt subprocess for DPC exact-native capture boundaries."""
from __future__ import annotations

import json
import os
from pathlib import Path
import struct
import sys
from types import SimpleNamespace
from urllib.parse import urlencode

from storm_tracker_worker import dpc_native as native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


PRODUCT_TIME_MS = 1_785_648_600_000
KEY = "SRI/02-08-2026-05-30.tif"
CATALOG = (
    b'{"total":1,"lastProducts":[{"productType":"SRI",'
    b'"time":1785648600000,"period":"PT5M"}]}'
)


def signed_url() -> str:
    query = urlencode({
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": "temporary/20260802/eu-south-1/s3/aws4_request",
        "X-Amz-Date": "20260802T053901Z",
        "X-Amz-Expires": "300",
        "X-Amz-Security-Token": "temporary-security-token",
        "X-Amz-Signature": "a" * 64,
        "X-Amz-SignedHeaders": "host",
        "response-content-disposition": 'attachment; filename="02-08-2026-05-30.tif"',
    })
    return f"https://{native.DPC_S3_HOST}/{KEY}?{query}"


def geotiff() -> bytes:
    short = lambda value: struct.pack("<H", value) + b"\x00\x00"
    long = lambda value: struct.pack("<I", value)
    strip = b"\x5a" * 64
    ifd_offset = 8 + len(strip)
    entries = [
        (256, 3, 1, short(1200)),
        (257, 3, 1, short(1400)),
        (258, 3, 1, short(32)),
        (259, 3, 1, short(5)),
        (273, 4, 1, long(8)),
        (277, 3, 1, short(1)),
        (278, 4, 1, long(1400)),
        (279, 4, 1, long(len(strip))),
        (339, 3, 1, short(3)),
        (33550, 12, 3, long(0)),
        (33922, 12, 6, long(0)),
        (34735, 3, 4, long(0)),
    ]
    ifd = struct.pack("<H", len(entries)) + b"".join(
        struct.pack("<HHI", tag, kind, count) + value
        for tag, kind, count, value in entries
    )
    return (
        b"II"
        + struct.pack("<HI", 42, ifd_offset)
        + strip
        + ifd
        + struct.pack("<I", 0)
    )


TIFF = geotiff()
URL = signed_url()
ENVELOPE = json.dumps(
    {
        "bucket": native.DPC_BUCKET,
        "key": KEY,
        "url": URL,
        "expiresSeconds": 300,
    },
    separators=(",", ":"),
).encode()


def response(payload: bytes, content_type: str, received_at: float):
    return native.DpcHttpResponse(
        payload=payload,
        content_type=content_type,
        received_at=received_at,
        content_length=len(payload),
        etag='"23300b18b47a941478a64eed513a3998"',
    )


native._fetch_catalog = lambda *, deadline: response(
    CATALOG, "application/json", 1_785_649_141.0
)
native._fetch_download_envelope = lambda product_time_ms, *, deadline: response(
    ENVELOPE, "application/json", 1_785_649_142.0
)
native._fetch_geotiff = lambda url, identity, *, deadline: response(
    TIFF, "binary/octet-stream", 1_785_649_143.0
)

root = Path(sys.argv[1])
checkpoint = sys.argv[2]


def crash(name: str) -> None:
    if name == checkpoint:
        os._exit(73)


native.capture_latest_dpc_sri(
    SimpleNamespace(job_id="dpc-native-slot-001", payload={}),
    raw_store=RawArtifactStore(root),
    _fault_injector=crash,
)
raise SystemExit("checkpoint was not reached")
