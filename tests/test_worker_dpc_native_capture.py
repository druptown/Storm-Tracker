from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import ast
import hashlib
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlencode

import pytest

from storm_tracker_worker import dpc_native as native
from storm_tracker_worker.raw_artifacts import (
    RawArtifactCapacityError,
    RawArtifactLimits,
    RawArtifactStore,
)


PRODUCT_TIME_MS = 1_785_648_600_000
PRODUCT_TIMESTAMP = datetime(2026, 8, 2, 5, 30, tzinfo=timezone.utc).timestamp()
KEY = "SRI/02-08-2026-05-30.tif"
CATALOG = (
    b'{"total":1,"lastProducts":[{"productType":"SRI",'
    b'"time":1785648600000,"period":"PT5M"}]}'
)


def _job(job_id: str = "dpc-native-slot-001", payload: dict | None = None):
    return SimpleNamespace(job_id=job_id, payload={} if payload is None else payload)


def _signed_url(
    *,
    key: str = KEY,
    expires: int = 300,
    host: str = native.DPC_S3_HOST,
    signature: str = "a" * 64,
) -> str:
    query = urlencode({
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": "temporary/20260802/eu-south-1/s3/aws4_request",
        "X-Amz-Date": "20260802T053901Z",
        "X-Amz-Expires": str(expires),
        "X-Amz-Security-Token": "temporary-security-token",
        "X-Amz-Signature": signature,
        "X-Amz-SignedHeaders": "host",
        "response-content-disposition": (
            f'attachment; filename="{key.rsplit("/", 1)[-1]}"'
        ),
    })
    return f"https://{host}/{key}?{query}"


def _download_envelope(
    *,
    key: str = KEY,
    expires: int = 300,
    url: str | None = None,
    bucket: str = native.DPC_BUCKET,
) -> bytes:
    return json.dumps(
        {
            "bucket": bucket,
            "key": key,
            "url": _signed_url(key=key, expires=expires) if url is None else url,
            "expiresSeconds": expires,
        },
        separators=(",", ":"),
    ).encode()


def _tiff_bytes(
    *,
    width: int = 1200,
    height: int = 1400,
    compression: int = 5,
    sample_format: int = 3,
    pad_to: int = 0,
) -> bytes:
    short = lambda value: struct.pack("<H", value) + b"\x00\x00"
    long = lambda value: struct.pack("<I", value)
    strip = b"\x5a" * (64 if pad_to <= 0 else pad_to)
    ifd_offset = 8 + len(strip)
    entries = [
        (256, 3, 1, short(width)),
        (257, 3, 1, short(height)),
        (258, 3, 1, short(32)),
        (259, 3, 1, short(compression)),
        (273, 4, 1, long(8)),
        (277, 3, 1, short(1)),
        (278, 4, 1, long(height)),
        (279, 4, 1, long(len(strip))),
        (339, 3, 1, short(sample_format)),
        (33550, 12, 3, long(0)),
        (33922, 12, 6, long(0)),
        (34735, 3, 4, long(0)),
    ]
    ifd = struct.pack("<H", len(entries))
    ifd += b"".join(
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


def _response(
    payload: bytes,
    content_type: str,
    received_at: float,
    *,
    etag: str | None = None,
) -> native.DpcHttpResponse:
    return native.DpcHttpResponse(
        payload=payload,
        content_type=content_type,
        received_at=received_at,
        content_length=len(payload),
        etag=etag,
    )


def _install_responses(monkeypatch, tiff: bytes | None = None):
    payload = _tiff_bytes() if tiff is None else tiff
    calls = {"catalog": 0, "download": 0, "geotiff": 0}

    def catalog(*, deadline):
        assert deadline > time.monotonic()
        calls["catalog"] += 1
        return _response(CATALOG, "application/json", 1_785_649_141.0)

    def download(product_time_ms, *, deadline):
        assert product_time_ms == PRODUCT_TIME_MS
        assert deadline > time.monotonic()
        calls["download"] += 1
        return _response(
            _download_envelope(), "application/json", 1_785_649_142.0
        )

    def geotiff(url, identity, *, deadline):
        assert url == _signed_url()
        assert identity.key == KEY
        assert deadline > time.monotonic()
        calls["geotiff"] += 1
        return _response(
            payload,
            "binary/octet-stream",
            1_785_649_143.0,
            etag='"23300b18b47a941478a64eed513a3998"',
        )

    monkeypatch.setattr(native, "_fetch_catalog", catalog)
    monkeypatch.setattr(native, "_fetch_download_envelope", download)
    monkeypatch.setattr(native, "_fetch_geotiff", geotiff)
    return calls, payload


def _capture(monkeypatch, tmp_path, *, tiff: bytes | None = None):
    store = RawArtifactStore(tmp_path / "raw")
    calls, payload = _install_responses(monkeypatch, tiff)
    result = native.capture_latest_dpc_sri(_job(), raw_store=store)
    return store, calls, payload, result


def test_exact_members_are_durable_secret_free_and_complete(monkeypatch, tmp_path):
    store, calls, tiff, result = _capture(monkeypatch, tmp_path)
    provenance = result["raw_provenance"]
    assert calls == {"catalog": 1, "download": 1, "geotiff": 1}
    assert result == {
        **result,
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "decode_status": "not_decoded",
        "pixel_values_read": False,
        "forecast_eligible": False,
        "operational_admission_eligible": False,
    }
    assert provenance["raw_store_schema_version"] == 3
    assert provenance["product_time_ms"] == PRODUCT_TIME_MS
    assert provenance["selected_key"] == KEY
    assert provenance["presigned_url_persisted"] is False
    assert provenance["raw_member_count"] == 2
    assert set(provenance["sources"]) == {
        "catalog_json", "geotiff"
    }
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 1

    sources = provenance["sources"]
    assert store.read_bytes(sources["catalog_json"]["receipt_id"]) == CATALOG
    assert store.read_bytes(sources["geotiff"]["receipt_id"]) == tiff
    assert provenance["selection_identity"]["presigned_query_persisted"] is False
    persisted_and_result = (
        store.read_bytes(sources["catalog_json"]["receipt_id"])
        + store.read_bytes(sources["geotiff"]["receipt_id"])
        + json.dumps(result, sort_keys=True).encode()
    )
    assert b"X-Amz" not in persisted_and_result
    assert b"temporary-security-token" not in persisted_and_result
    assert b"Signature" not in persisted_and_result


def test_originals_are_stored_before_structural_inspection(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    _, tiff = _install_responses(monkeypatch)
    real_catalog = native._inspect_catalog
    real_tiff = native._inspect_geotiff
    events: list[str] = []

    def inspect_catalog(payload):
        assert payload == CATALOG
        assert store.stats()["receipt_count"] == 1
        assert store.stats()["capture_count"] == 0
        events.append("catalog")
        return real_catalog(payload)

    def inspect_tiff(payload):
        assert payload == tiff
        assert store.stats()["receipt_count"] == 2
        assert store.stats()["capture_count"] == 0
        events.append("tiff")
        return real_tiff(payload)

    monkeypatch.setattr(native, "_inspect_catalog", inspect_catalog)
    monkeypatch.setattr(native, "_inspect_geotiff", inspect_tiff)
    native.capture_latest_dpc_sri(_job(), raw_store=store)
    assert events == ["catalog", "tiff"]


def test_complete_retry_is_network_free_and_deterministic(monkeypatch, tmp_path):
    store, calls, _tiff, first = _capture(monkeypatch, tmp_path)

    def forbidden(*_args, **_kwargs):
        pytest.fail("complete retry mag geen netwerk gebruiken")

    monkeypatch.setattr(native, "_fetch_catalog", forbidden)
    monkeypatch.setattr(native, "_fetch_download_envelope", forbidden)
    monkeypatch.setattr(native, "_fetch_geotiff", forbidden)
    reopened = RawArtifactStore(store.root)
    retry = native.capture_latest_dpc_sri(_job(), raw_store=reopened)
    assert retry == first
    assert calls == {"catalog": 1, "download": 1, "geotiff": 1}


class InjectedFailure(RuntimeError):
    pass


@pytest.mark.parametrize(
    ("checkpoint", "receipts", "retry_calls"),
    [
        ("dpc.after_catalog_store", 1, {"catalog": 1, "download": 1, "geotiff": 1}),
        (
            "dpc.after_download_identity_validation",
            1,
            {"catalog": 1, "download": 2, "geotiff": 1},
        ),
        ("dpc.after_geotiff_store", 2, {"catalog": 1, "download": 1, "geotiff": 1}),
        ("dpc.before_capture_commit", 2, {"catalog": 1, "download": 1, "geotiff": 1}),
    ],
)
def test_partial_faults_resume_without_reselecting_durable_members(
    monkeypatch, tmp_path, checkpoint, receipts, retry_calls
):
    store = RawArtifactStore(tmp_path / "raw")
    calls, _ = _install_responses(monkeypatch)

    def fail(name):
        if name == checkpoint:
            raise InjectedFailure(name)

    with pytest.raises(InjectedFailure, match=checkpoint):
        native.capture_latest_dpc_sri(
            _job(), raw_store=store, _fault_injector=fail
        )
    assert store.stats()["receipt_count"] == receipts
    assert store.stats()["capture_count"] == 0
    result = native.capture_latest_dpc_sri(_job(), raw_store=store)
    assert result["capture_state"] == "captured_not_decoded"
    assert calls == retry_calls


def test_after_commit_fault_leaves_complete_offline_retry(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch)

    def fail(name):
        if name == "dpc.after_capture_commit":
            raise InjectedFailure(name)

    with pytest.raises(InjectedFailure):
        native.capture_latest_dpc_sri(
            _job(), raw_store=store, _fault_injector=fail
        )
    assert store.stats()["capture_count"] == 1
    monkeypatch.setattr(native, "_fetch_catalog", lambda **_: pytest.fail("network"))
    result = native.capture_latest_dpc_sri(_job(), raw_store=store)
    assert result["capture_state"] == "captured_not_decoded"


def test_signed_url_and_download_contract_fail_closed():
    catalog = native._inspect_catalog(CATALOG)
    identity, _ = native._inspect_download_envelope(
        _download_envelope(), catalog=catalog
    )
    assert identity.key == KEY
    assert identity.product_time_ms == PRODUCT_TIME_MS
    assert identity.expires_seconds == 300

    bad_urls = [
        _signed_url(host="127.0.0.1"),
        _signed_url(host="example.org"),
        _signed_url(signature="short"),
        _signed_url() + "#fragment",
        _signed_url().replace("https://", "http://", 1),
        _signed_url().replace("X-Amz-Signature", "Unexpected"),
    ]
    for url in bad_urls:
        with pytest.raises(native.DpcNativeContractError):
            native._inspect_download_envelope(
                _download_envelope(url=url), catalog=catalog
            )
    with pytest.raises(native.DpcNativeContractError):
        native._inspect_download_envelope(
            _download_envelope(key="SRI/02-08-2026-05-35.tif"), catalog=catalog
        )


@pytest.mark.parametrize(
    "payload",
    [
        b'{"total":1,"total":1,"lastProducts":[]}',
        b'{"total":1,"lastProducts":[]}',
        b'{"total":1,"lastProducts":[{"productType":"SRI",'
        b'"time":1785648600001,"period":"PT5M"}]}',
        b'{"total":1,"lastProducts":[{"productType":"VMI",'
        b'"time":1785648600000,"period":"PT5M"}]}',
        b'{"total":1,"lastProducts":[{"productType":"SRI",'
        b'"time":1785648600000,"period":"PT5M","extra":1}]}',
    ],
)
def test_catalog_shape_identity_and_duplicate_keys_are_strict(payload):
    with pytest.raises(native.DpcNativeError):
        native._inspect_catalog(payload)


def test_geotiff_container_profile_reads_no_pixels():
    identity = native._inspect_geotiff(_tiff_bytes())
    assert (
        identity.width,
        identity.height,
        identity.bits_per_sample,
        identity.compression,
        identity.samples_per_pixel,
        identity.sample_format,
    ) == (1200, 1400, 32, 5, 1, 3)
    assert identity.georeference_tags == (33550, 33922, 34735)
    assert identity.pixel_values_read is False
    for payload in (
        b"not-a-tiff-container",
        _tiff_bytes(width=1199),
        _tiff_bytes(height=1399),
        _tiff_bytes(compression=1),
        _tiff_bytes(sample_format=1),
    ):
        with pytest.raises(native.DpcNativeError):
            native._inspect_geotiff(payload)


class _FakeHttpResponse(io.BytesIO):
    def __init__(self, payload: bytes, *, url: str, headers=None, status=200):
        super().__init__(payload)
        self.status = status
        self._url = url
        self.headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(payload)),
            **(headers or {}),
        }

    def geturl(self):
        return self._url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def test_http_transport_rejects_redirect_encoding_length_and_type():
    url = native.DPC_API_ORIGIN + native.DPC_CATALOG_PATH + "?type=SRI"

    def run(response):
        return native._request_exact(
            url,
            method="GET",
            request_body=None,
            headers={},
            maximum_bytes=1024,
            allowed_content_types=frozenset({"application/json"}),
            deadline=time.monotonic() + 2,
            opener=lambda *_args, **_kwargs: response,
        )

    assert run(_FakeHttpResponse(CATALOG, url=url)).payload == CATALOG
    failures = [
        _FakeHttpResponse(CATALOG, url=url + "&redirected=1"),
        _FakeHttpResponse(CATALOG, url=url, headers={"Content-Encoding": "gzip"}),
        _FakeHttpResponse(CATALOG, url=url, headers={"Content-Length": "999999"}),
        _FakeHttpResponse(CATALOG, url=url, headers={"Content-Type": "text/html"}),
        _FakeHttpResponse(CATALOG, url=url, status=206),
    ]
    for response in failures:
        with pytest.raises(native.DpcNativeError):
            run(response)


def test_raw_capacity_failure_never_commits_capture(monkeypatch, tmp_path):
    tiff = _tiff_bytes(pad_to=4096)
    limits = RawArtifactLimits(max_object_bytes=1024, max_total_bytes=8192)
    store = RawArtifactStore(tmp_path / "raw", limits=limits)
    _install_responses(monkeypatch, tiff)
    with pytest.raises(RawArtifactCapacityError):
        native.capture_latest_dpc_sri(_job(), raw_store=store)
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_concurrent_same_job_fetches_once(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    calls, _ = _install_responses(monkeypatch)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(
            lambda _index: native.capture_latest_dpc_sri(_job(), raw_store=store),
            range(2),
        ))
    assert results[0] == results[1]
    assert calls == {"catalog": 1, "download": 1, "geotiff": 1}


def test_thread_lock_and_expired_deadline_fail_without_network(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    monkeypatch.setattr(
        native, "_fetch_catalog", lambda **_: pytest.fail("deadline gebruikte netwerk")
    )
    with pytest.raises(native.DpcNativeDeadlineError):
        native.capture_latest_dpc_sri(
            _job(), raw_store=store, deadline_at=time.monotonic() - 1
        )

    entered = threading.Event()
    release = threading.Event()

    def holder():
        with native._CAPTURE_LOCK:
            entered.set()
            release.wait(2)

    thread = threading.Thread(target=holder)
    thread.start()
    assert entered.wait(1)
    try:
        with pytest.raises(native.DpcNativeDeadlineError):
            native.capture_latest_dpc_sri(
                _job("dpc-lock-timeout"),
                raw_store=store,
                deadline_at=time.monotonic() + 0.05,
            )
    finally:
        release.set()
        thread.join(2)


@pytest.mark.parametrize(
    "checkpoint",
    [
        "dpc.after_catalog_store",
        "dpc.after_download_identity_validation",
        "dpc.after_geotiff_store",
        "dpc.before_capture_commit",
        "dpc.after_capture_commit",
    ],
)
def test_abrupt_process_crash_is_recoverable(monkeypatch, tmp_path, checkpoint):
    root = tmp_path / checkpoint.replace(".", "_")
    helper = Path(__file__).parent / "helpers" / "dpc_native_crash_child.py"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    result = subprocess.run(
        [sys.executable, str(helper), str(root), checkpoint],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 73, (result.stdout, result.stderr)

    store = RawArtifactStore(root)
    calls, _ = _install_responses(monkeypatch)
    recovered = native.capture_latest_dpc_sri(_job(), raw_store=store)
    assert recovered["capture_state"] == "captured_not_decoded"
    if checkpoint in {
        "dpc.after_geotiff_store",
        "dpc.before_capture_commit",
        "dpc.after_capture_commit",
    }:
        assert calls == {"catalog": 0, "download": 0, "geotiff": 0}


def test_live_golden_profile_matches_pinned_contract():
    fixture = json.loads(
        (Path(__file__).parent / "fixtures" / "dpc_sri_v2_live_identity_profile.json")
        .read_text(encoding="utf-8")
    )
    exact_catalog = fixture["catalog"]["exact_body"].encode()
    assert hashlib.sha256(exact_catalog).hexdigest() == fixture["catalog"]["sha256"]
    catalog = native._inspect_catalog(exact_catalog)
    assert catalog.product_time_ms == fixture["catalog"]["product_time_ms"]
    assert native._expected_key(catalog.product_time_ms) == fixture["object"]["key"]
    assert fixture["object"]["size_bytes"] <= native.MAX_GEOTIFF_BYTES
    assert fixture["object"]["tiff_profile"] == {
        "width": 1200,
        "height": 1400,
        "bits_per_sample": 32,
        "compression": 5,
        "samples_per_pixel": 1,
        "sample_format": 3,
    }
    assert "signed_url" not in json.dumps(fixture).lower()
    assert "x-amz" not in json.dumps(fixture).lower()


def test_candidate_has_no_pixel_decoder_or_operational_wiring():
    module_path = Path(native.__file__)
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imports = {
        alias.name.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imports.update(
        node.module.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    )
    assert imports.isdisjoint({"numpy", "PIL", "rasterio", "gdal", "osgeo"})
    source = module_path.read_text(encoding="utf-8")
    for forbidden in ("observations", "rain_rate", "forecast_output", "admit("):
        assert forbidden not in source

    from storm_tracker_worker.service import PRODUCTION_JOB_HANDLERS

    reference = PRODUCTION_JOB_HANDLERS["dpc_radar_fetch"]
    assert reference.module == "storm_tracker_worker.provider_radar"
    assert reference.qualname == "dpc_radar_fetch"
    assert "dpc_native" not in (
        Path(__file__).resolve().parents[1]
        / "storm_tracker_worker" / "service.py"
    ).read_text(encoding="utf-8")
    component_root = Path(__file__).resolve().parents[1] / "custom_components"
    assert not any(
        "dpc_native" in path.read_text(encoding="utf-8", errors="ignore")
        for path in component_root.rglob("*.py")
    )


def test_job_payload_is_exactly_empty_and_deadline_is_bounded(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    for payload in ({"url": _signed_url()}, {"token": "secret"}, {"lat": 1.0}):
        with pytest.raises(ValueError, match="exact lege"):
            native.capture_latest_dpc_sri(_job(payload=payload), raw_store=store)
    for deadline in (True, float("nan"), "later"):
        with pytest.raises(ValueError, match="deadline_at"):
            native.capture_latest_dpc_sri(
                _job(), raw_store=store, deadline_at=deadline
            )
