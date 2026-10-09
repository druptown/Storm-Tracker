from __future__ import annotations

from dataclasses import replace
import hashlib
import sqlite3

import pytest

from storm_tracker_worker.raw_artifacts import (
    RawArtifactCaptureMember,
    RawArtifactCaptureReceipt,
)
from storm_tracker_worker.raw_calibration_index import (
    RawCalibrationIndex,
    RawCalibrationIndexError,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _capture(label: str, *, provider: str = "dwd") -> RawArtifactCaptureReceipt:
    return RawArtifactCaptureReceipt(
        capture_id=_sha("capture:" + label),
        provider=provider,
        product="radolan-rv",
        fetch_id="fetch-" + label,
        upstream_capture_id="upstream-" + label,
        product_timestamp=1_000.0,
        manifest_sha256=_sha("manifest:" + label),
        capture_receipt_sha256=_sha("receipt:" + label),
        created_at=1_002.0,
        members=(
            RawArtifactCaptureMember(
                role="payload",
                receipt_id="member-" + label,
                content_sha256=_sha("content:" + label),
                receipt_sha256=_sha("member-receipt:" + label),
                received_at=1_001.0,
            ),
        ),
    )


def test_raw_calibration_index_is_persistent_idempotent_and_payload_free(
    tmp_path,
):
    path = tmp_path / "raw-calibration.sqlite3"
    store = RawCalibrationIndex(path)
    assert store.statistics()["status"] == "not_initialized"

    first = store.record(
        "radar.dwd.radolan_rv",
        _capture("one"),
        indexed_at=1_003.0,
    )
    assert first["already_indexed"] is False
    assert first["captures"] == 1
    assert first["sources"] == 1
    assert first["providers"] == 1
    assert first["exact_raw"] is True
    assert first["payload_storage"] == "raw_artifact_store"

    repeated = RawCalibrationIndex(path).record(
        "radar.dwd.radolan_rv",
        _capture("one"),
        indexed_at=2_000.0,
    )
    assert repeated["already_indexed"] is True
    assert repeated["captures"] == 1

    with sqlite3.connect(path) as database:
        columns = {
            row[1]
            for row in database.execute(
                "PRAGMA table_info(raw_calibration_captures)"
            )
        }
        assert "payload" not in columns
        assert "artifact_bytes" not in columns
        assert database.execute(
            "SELECT indexed_at FROM raw_calibration_captures"
        ).fetchone()[0] == 1_003.0


def test_raw_calibration_index_groups_every_source_and_provider(tmp_path):
    store = RawCalibrationIndex(tmp_path / "raw-calibration.sqlite3")
    store.record(
        "radar.dwd.radolan_rv",
        _capture("dwd", provider="dwd"),
        indexed_at=1_003.0,
    )
    store.record(
        "radar.knmi.rtcor",
        _capture("knmi", provider="knmi"),
        indexed_at=1_004.0,
    )

    statistics = store.statistics()
    assert statistics["captures"] == 2
    assert statistics["sources"] == 2
    assert statistics["providers"] == 2
    assert [row["source_id"] for row in store.source_statistics()] == [
        "radar.dwd.radolan_rv",
        "radar.knmi.rtcor",
    ]


def test_raw_calibration_index_rejects_provenance_collision(tmp_path):
    store = RawCalibrationIndex(tmp_path / "raw-calibration.sqlite3")
    capture = _capture("one")
    store.record(
        "radar.dwd.radolan_rv",
        capture,
        indexed_at=1_003.0,
    )

    with pytest.raises(RawCalibrationIndexError, match="botst"):
        store.record(
            "radar.knmi.rtcor",
            replace(capture, provider="knmi"),
            indexed_at=1_004.0,
        )


def test_raw_calibration_index_rejects_invalid_source_and_manifest(tmp_path):
    store = RawCalibrationIndex(tmp_path / "raw-calibration.sqlite3")
    with pytest.raises(RawCalibrationIndexError, match="source_id"):
        store.record("Radar DWD", _capture("one"), indexed_at=1_003.0)
    with pytest.raises(RawCalibrationIndexError, match="controlesommen"):
        store.record(
            "radar.dwd.radolan_rv",
            replace(_capture("two"), manifest_sha256="invalid"),
            indexed_at=1_003.0,
        )


def test_raw_calibration_index_rejects_manifest_drift(tmp_path):
    path = tmp_path / "raw-calibration.sqlite3"
    store = RawCalibrationIndex(path)
    store.initialize()
    with sqlite3.connect(path) as database:
        database.execute(
            "UPDATE raw_calibration_manifest SET value='999' "
            "WHERE key='schema_version'"
        )
        database.commit()
    with pytest.raises(RawCalibrationIndexError, match="manifest"):
        store.initialize()
