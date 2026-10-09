"""Proof tests for the dedicated DWD tar-only acquisition path."""
from __future__ import annotations

import ast
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import tarfile
from types import SimpleNamespace

import pytest

from storm_tracker_worker import dwd_rv_native as native
from storm_tracker_worker.raw_artifacts import RawArtifactLimits, RawArtifactStore


TIMESTAMP = datetime(2026, 8, 8, 12, 35, tzinfo=timezone.utc)


def _tar(*, member_payload=b"opaque-hdf5-not-opened", member_name=None) -> bytes:
    target = member_name or f"composite_rv_{TIMESTAMP:%Y%m%d}_{TIMESTAMP:%H%M}_000-hd5"
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        member = tarfile.TarInfo(target)
        member.size = len(member_payload)
        archive.addfile(member, io.BytesIO(member_payload))
        forecast_name = f"composite_rv_{TIMESTAMP:%Y%m%d}_{TIMESTAMP:%H%M}_005-hd5"
        forecast = tarfile.TarInfo(forecast_name)
        forecast.size = len(member_payload)
        archive.addfile(forecast, io.BytesIO(member_payload))
    return stream.getvalue()


def _store(tmp_path) -> RawArtifactStore:
    store = RawArtifactStore(
        tmp_path / "raw",
        limits=RawArtifactLimits(
            max_object_bytes=native.MAX_ARCHIVE_BYTES,
            max_total_bytes=32 * 1024 * 1024,
        ),
    )
    store.initialize()
    return store


def _job(value="acquisition:" + "a" * 64):
    return SimpleNamespace(job_id=value, payload={})


def test_transport_limit_rejects_declared_oversize_before_body_read(monkeypatch):
    class Oversize(io.BytesIO):
        def __init__(self):
            super().__init__(b"must-not-be-read")
            self.headers = {
                "Content-Encoding": "identity",
                "Content-Length": str(native.MAX_ARCHIVE_BYTES + 1),
                "Content-Type": "application/x-tar",
            }

        def read(self, *_args, **_kwargs):
            pytest.fail("oversized DWD response body was read")

    monkeypatch.setattr(native, "urlopen", lambda *_args, **_kwargs: Oversize())
    with pytest.raises(ValueError, match="contractlimiet"):
        native._download()


def test_exact_tar_is_durable_before_layout_inspection(monkeypatch, tmp_path):
    payload = _tar()
    received_at = TIMESTAMP.timestamp() + 27.0
    store = _store(tmp_path)
    monkeypatch.setattr(
        native,
        "_download",
        lambda: native.DwdRvHttpResponse(payload, "application/x-tar", received_at),
    )
    real_inspector = native._inspect_tar_layout
    calls = {"count": 0}

    def inspect_after_store(value):
        calls["count"] += 1
        assert store.stats()["receipt_count"] == 1
        objects = list((tmp_path / "raw" / "objects").glob("*.raw"))
        assert len(objects) == 1 and objects[0].read_bytes() == payload
        return real_inspector(value)

    monkeypatch.setattr(native, "_inspect_tar_layout", inspect_after_store)
    result = native.capture_latest_dwd_rv_tar(_job(), raw_store=store)
    source = result["raw_provenance"]["raw_sources"]["rv_tar"]
    assert result["capture_state"] == "captured_not_decoded"
    assert result["decode_status"] == "not_decoded"
    assert result["weather_state"] == "unknown"
    assert result["quantitative_semantics_eligible"] is False
    assert result["operational_admission_eligible"] is False
    assert source["raw_content_sha256"] == hashlib.sha256(payload).hexdigest()
    assert source["received_at"] == received_at
    assert source["declared_product_timestamp"] == TIMESTAMP.timestamp()
    assert store.read_bytes(source["raw_receipt_id"]) == payload
    assert calls["count"] == 1
    encoded = json.dumps(result["raw_provenance"], sort_keys=True).lower()
    assert "precipitation" not in encoded and "rain" not in encoded
    assert "unit" not in encoded and "quantity" not in encoded


def test_retry_reuses_durable_receipt_and_capture_without_network(monkeypatch, tmp_path):
    payload = _tar()
    calls = {"count": 0}

    def download():
        calls["count"] += 1
        return native.DwdRvHttpResponse(
            payload, "application/octet-stream", TIMESTAMP.timestamp() + 30
        )

    monkeypatch.setattr(native, "_download", download)
    store = _store(tmp_path)
    first = native.capture_latest_dwd_rv_tar(_job(), raw_store=store)
    second = native.capture_latest_dwd_rv_tar(_job(), raw_store=store)
    assert first == second
    assert calls["count"] == 1
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 1


def test_invalid_tar_remains_raw_receipt_only_and_retry_does_not_redownload(
    monkeypatch, tmp_path
):
    payload = b"not-a-tar"
    calls = {"count": 0}

    def download():
        calls["count"] += 1
        return native.DwdRvHttpResponse(
            payload, "application/x-tar", TIMESTAMP.timestamp()
        )

    monkeypatch.setattr(native, "_download", download)
    store = _store(tmp_path)
    with pytest.raises(tarfile.ReadError):
        native.capture_latest_dwd_rv_tar(_job(), raw_store=store)
    with pytest.raises(tarfile.ReadError):
        native.capture_latest_dwd_rv_tar(_job(), raw_store=store)
    assert calls["count"] == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_payload_and_ambiguous_member_layout_fail_closed(monkeypatch, tmp_path):
    store = _store(tmp_path)
    with pytest.raises(ValueError, match="exact leeg"):
        native.capture_latest_dwd_rv_tar(
            SimpleNamespace(job_id="job", payload={"areas": []}), raw_store=store
        )
    monkeypatch.setattr(
        native,
        "_download",
        lambda: native.DwdRvHttpResponse(
            _tar(member_name="not-a-production-member.h5"),
            "application/x-tar",
            TIMESTAMP.timestamp(),
        ),
    )
    with pytest.raises(ValueError, match="productie"):
        native.capture_latest_dwd_rv_tar(_job("different-job"), raw_store=store)
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_raw_only_module_has_no_legacy_decoder_or_science_import():
    source_path = Path(native.__file__)
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(source_path))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(item.name for item in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    assert all(not value.endswith(".radar") for value in imports)
    assert imports.isdisjoint({"h5py", "numpy", "pyproj"})
    assert "parse_rv_archive" not in source
    assert "dwd_rv_quantitative" not in source
