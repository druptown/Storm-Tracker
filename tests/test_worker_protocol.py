from __future__ import annotations

import base64
import gzip
import json

import pytest

import storm_tracker_worker.protocol as protocol
from storm_tracker_worker.protocol import (
    CONTRACT_VERSION,
    MAX_PAYLOAD_BYTES,
    decode,
    encode,
    envelope,
    job_payload,
    topic,
)


def test_protocol_round_trip_and_topic_normalization():
    message = envelope("job_status", job_id="abc", state="completed")
    assert decode(encode(message)) == message
    assert topic("storm_tracker/v1/", "/worker/status") == (
        "storm_tracker/v1/worker/status"
    )


def test_protocol_rejects_wrong_version_and_non_object():
    with pytest.raises(ValueError, match="contractversie"):
        decode(json.dumps({"contract_version": CONTRACT_VERSION + 1}))
    with pytest.raises(ValueError, match="object"):
        decode("[]")


def test_protocol_enforces_payload_limit():
    with pytest.raises(ValueError, match="contractlimiet"):
        encode(
            {
                "contract_version": CONTRACT_VERSION,
                "payload": "x" * MAX_PAYLOAD_BYTES,
            }
        )


def test_protocol_decodes_losslessly_compressed_job_payload():
    original = {
        "region_id": "america",
        "observations": [
            {"latitude": 40.7, "longitude": -74.0, "value": index}
            for index in range(20_000)
        ],
    }
    raw = json.dumps(
        original, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    message = envelope(
        "job_request",
        payload_codec="gzip+base64+json",
        payload_data=base64.b64encode(gzip.compress(raw)).decode("ascii"),
        payload_uncompressed_bytes=len(raw),
    )

    assert job_payload(decode(encode(message))) == original


def test_protocol_bounds_decompression_before_json_parse(monkeypatch):
    monkeypatch.setattr(protocol, "MAX_UNCOMPRESSED_JOB_BYTES", 32)
    compressed = gzip.compress(b'{"value":"' + b"x" * 1000 + b'"}', mtime=0)
    message = envelope(
        "job_request",
        payload_codec="gzip+base64+json",
        payload_data=base64.b64encode(compressed).decode("ascii"),
    )

    with pytest.raises(ValueError, match="uitgepakte payload"):
        job_payload(message)


def test_protocol_rejects_concatenated_gzip_members():
    raw = gzip.compress(b'{"value":1}', mtime=0)
    trailing_member = gzip.compress(b'{"hidden":2}', mtime=0)
    message = envelope(
        "job_request",
        payload_codec="gzip+base64+json",
        payload_data=base64.b64encode(raw + trailing_member).decode("ascii"),
    )

    with pytest.raises(ValueError, match="extra leden|trailing"):
        job_payload(message)
