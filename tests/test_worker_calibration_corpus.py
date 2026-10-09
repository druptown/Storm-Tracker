from __future__ import annotations

from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from storm_tracker_worker import artifacts, calibration_corpus, calibration_stage


def _payload(source: str, timestamp: float, cells: list[tuple[float, float, int]]):
    observations = []
    runs = []
    for lat, lon, intensity in cells:
        observations.append({
            "source": source,
            "lat": lat,
            "lon": lon,
            "intensity": intensity,
            "quality": 0.8,
        })
        runs.append({
            "intensity": intensity,
            "ring": [
                [lat - 0.04, lon - 0.04],
                [lat - 0.04, lon + 0.04],
                [lat + 0.04, lon + 0.04],
                [lat + 0.04, lon - 0.04],
            ],
        })
    return {
        "provider": source,
        "frame_timestamp": float(timestamp),
        "observations": observations,
        "overlay": {
            "source": source,
            "timestamp": float(timestamp),
            "runs": runs,
        },
    }


def _capture(
    metadata: dict,
    *,
    source: str,
    timestamp: float,
    region_key: str = "region-1@51.0,4.0,100",
) -> dict:
    descriptor = {
        "contract": calibration_stage.CAPTURE_CONTRACT,
        "region_key": region_key,
        "region_id": "region-1",
        "source": source,
        "product_timestamp": float(timestamp),
        "grid_deg": 0.1,
        "minimum_intensity": 1,
        "evaluation": {
            "center_lat": 51.0,
            "center_lon": 4.0,
            "radius_km": 100.0,
        },
        "coverage_bbox": [2.0, 49.0, 6.0, 53.0],
        "coverage_fraction": 1.0,
        "fetch_state": "success",
        "artifact": {**metadata, "frame_timestamp": float(timestamp)},
    }
    encoded = json.dumps(
        descriptor,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    descriptor["capture_id"] = hashlib.sha256(encoded).hexdigest()
    return descriptor


def _artifact(monkeypatch, tmp_path, name: str, payload: dict) -> dict:
    monkeypatch.setattr(artifacts, "ARTIFACT_ROOT", tmp_path)
    return artifacts.write_json_artifact(name, payload)


def test_cross_job_frames_recompute_comparison_bias_and_profiles_atomically(
    monkeypatch, tmp_path,
):
    timestamp = 1_700_000_020.0
    kmi_metadata = _artifact(
        monkeypatch,
        tmp_path,
        "corpus-kmi",
        _payload("kmi", timestamp, [(51.05, 4.05, 1), (51.25, 4.25, 4)]),
    )
    opera_metadata = _artifact(
        monkeypatch,
        tmp_path,
        "corpus-opera",
        _payload("opera", timestamp, [(51.05, 4.05, 2), (51.35, 4.35, 5)]),
    )
    kmi_capture = _capture(kmi_metadata, source="kmi", timestamp=timestamp)
    opera_capture = _capture(opera_metadata, source="opera", timestamp=timestamp)
    path = tmp_path / "corpus.sqlite3"
    store = calibration_corpus.CalibrationCorpusStore(path)

    first = store.ingest([kmi_capture], receipt_id="job-kmi", ingested_at=timestamp + 1)
    assert first["statistics"]["frames"] == 1
    assert first["statistics"]["comparisons"] == 0

    second = store.ingest(
        [opera_capture], receipt_id="job-opera", ingested_at=timestamp + 2
    )

    assert second["already_applied"] is False
    expected_counts = {
        "captures": 2,
        "frames": 2,
        "artifacts": 2,
        "comparisons": 1,
        "bias_samples": 2,
        "bias_profiles": 4,
    }
    assert {
        key: second["statistics"][key] for key in expected_counts
    } == expected_counts
    assert second["receipt"]["comparisons_recomputed"] == 1
    assert second["receipt"]["bias_samples_recomputed"] == 2
    assert len(second["receipt"]["capture_set_sha256"]) == 64
    assert second["statistics"]["artifact_kind"] == "decoded_provider_result"
    assert second["statistics"]["provenance_grade"] == "decoded_legacy"
    assert second["statistics"]["exact_raw_artifacts_complete"] is False
    assert second["statistics"]["parser_id"] is None
    assert second["statistics"]["upstream_product_id"] is None
    assert second["profiles_total"] == 4
    assert {profile["scope_key"] for profile in second["profiles"]} == {
        "*", "51.0,4.0,100"
    }
    assert all(profile["sample_count"] == 1 for profile in second["profiles"])
    assert all(
        profile["algorithm_version"]
        == calibration_corpus.CORPUS_ALGORITHM_VERSION
        for profile in second["profiles"]
    )

    with closing(sqlite3.connect(path)) as db:
        comparison = db.execute(
            "SELECT capture_id_a, capture_id_b, artifact_sha256_a, "
            "artifact_sha256_b, product_timestamp_a, product_timestamp_b, "
            "algorithm_version FROM comparisons"
        ).fetchone()
        assert comparison == (
            kmi_capture["capture_id"], opera_capture["capture_id"],
            kmi_metadata["artifact_sha256"], opera_metadata["artifact_sha256"],
            timestamp, timestamp, calibration_corpus.CORPUS_ALGORITHM_VERSION,
        )
        retained = db.execute(
            "SELECT payload FROM artifacts WHERE artifact_sha256=?",
            (kmi_metadata["artifact_sha256"],),
        ).fetchone()[0]
        provenance = db.execute(
            "SELECT capture_id, artifact_sha256, descriptor_product_timestamp, "
            "algorithm_version FROM captures WHERE capture_id=?",
            (kmi_capture["capture_id"],),
        ).fetchone()
        artifact_grade = db.execute(
            "SELECT artifact_kind, provenance_grade, "
            "exact_raw_artifacts_complete, parser_id, upstream_product_id, "
            "upstream_content_type FROM artifacts WHERE artifact_sha256=?",
            (kmi_metadata["artifact_sha256"],),
        ).fetchone()
    assert retained == Path(kmi_metadata["artifact_path"]).read_bytes()
    assert provenance == (
        kmi_capture["capture_id"], kmi_metadata["artifact_sha256"], timestamp,
        calibration_corpus.CORPUS_ALGORITHM_VERSION,
    )
    assert artifact_grade == (
        "decoded_provider_result", "decoded_legacy", 0, None, None, None
    )

    reopened = calibration_corpus.CalibrationCorpusStore(path)
    assert reopened.statistics()["comparisons"] == 1


def test_ingest_receipt_is_order_independent_idempotent_and_collision_safe(
    monkeypatch, tmp_path,
):
    timestamp = 1_700_000_020.0
    first_metadata = _artifact(
        monkeypatch, tmp_path, "receipt-kmi",
        _payload("kmi", timestamp, [(51.05, 4.05, 2)]),
    )
    second_metadata = _artifact(
        monkeypatch, tmp_path, "receipt-opera",
        _payload("opera", timestamp, [(51.05, 4.05, 2)]),
    )
    first_capture = _capture(first_metadata, source="kmi", timestamp=timestamp)
    second_capture = _capture(second_metadata, source="opera", timestamp=timestamp)
    store = calibration_corpus.CalibrationCorpusStore(tmp_path / "receipt.sqlite3")

    applied = store.ingest(
        [first_capture, second_capture],
        receipt_id="stable-receipt",
        ingested_at=timestamp + 1,
    )
    repeated = store.ingest(
        [second_capture, first_capture],
        receipt_id="stable-receipt",
        ingested_at=timestamp + 99,
    )

    assert applied["already_applied"] is False
    assert repeated["already_applied"] is True
    assert repeated["receipt"]["applied_at"] == timestamp + 1
    assert repeated["statistics"]["receipts"] == 1
    assert repeated["statistics"]["captures"] == 2

    # The corpus, not the short-lived /share artifact, owns known captures.
    # A later receipt can therefore acknowledge the same immutable evidence
    # after the transfer artifacts have expired.
    Path(first_metadata["artifact_path"]).unlink()
    Path(second_metadata["artifact_path"]).unlink()
    later_receipt = store.ingest(
        [first_capture, second_capture],
        receipt_id="later-receipt",
        ingested_at=timestamp + 100,
    )
    assert later_receipt["receipt"]["captures_inserted"] == 0
    assert later_receipt["receipt"]["artifacts_inserted"] == 0
    assert later_receipt["statistics"]["receipts"] == 2

    with pytest.raises(ValueError, match="andere captures"):
        store.ingest(
            [first_capture],
            receipt_id="stable-receipt",
            ingested_at=timestamp + 101,
        )
    assert store.statistics()["receipts"] == 2


def test_newer_same_minute_frame_becomes_active_without_losing_corpus_history(
    monkeypatch, tmp_path,
):
    timestamp = 6_000.0
    old_kmi_metadata = _artifact(
        monkeypatch, tmp_path, "old-kmi",
        _payload("kmi", timestamp, [(51.05, 4.05, 2)]),
    )
    opera_metadata = _artifact(
        monkeypatch, tmp_path, "same-minute-opera",
        _payload("opera", timestamp, [(51.05, 4.05, 2)]),
    )
    new_kmi_metadata = _artifact(
        monkeypatch, tmp_path, "new-kmi",
        _payload("kmi", timestamp + 10, [(51.25, 4.25, 4)]),
    )
    old_kmi = _capture(old_kmi_metadata, source="kmi", timestamp=timestamp)
    opera = _capture(opera_metadata, source="opera", timestamp=timestamp)
    new_kmi = _capture(
        new_kmi_metadata, source="kmi", timestamp=timestamp + 10
    )
    path = tmp_path / "newest.sqlite3"
    store = calibration_corpus.CalibrationCorpusStore(path)
    store.ingest([old_kmi, opera], receipt_id="initial", ingested_at=6_020.0)

    result = store.ingest([new_kmi], receipt_id="newer", ingested_at=6_030.0)

    assert result["statistics"]["captures"] == 3
    assert result["statistics"]["frames"] == 3
    assert result["statistics"]["artifacts"] == 3
    assert result["statistics"]["comparisons"] == 1
    assert result["statistics"]["bias_samples"] == 2
    with closing(sqlite3.connect(path)) as db:
        selected = db.execute(
            "SELECT capture_id_a, product_timestamp_a FROM comparisons "
            "WHERE source_a='kmi' AND source_b='opera'"
        ).fetchone()
        old_still_present = db.execute(
            "SELECT count(*) FROM frames WHERE capture_id=?",
            (old_kmi["capture_id"],),
        ).fetchone()[0]
    assert selected == (new_kmi["capture_id"], timestamp + 10)
    assert old_still_present == 1


def test_invalid_batch_and_corpus_bound_fail_without_partial_receipt(
    monkeypatch, tmp_path,
):
    timestamp = 1_700_000_020.0
    metadata = _artifact(
        monkeypatch, tmp_path, "atomic-valid",
        _payload("kmi", timestamp, [(51.05, 4.05, 2)]),
    )
    valid = _capture(metadata, source="kmi", timestamp=timestamp)
    missing_metadata = {
        "artifact_path": str(tmp_path / "missing.json.gz"),
        "artifact_sha256": "0" * 64,
        "artifact_bytes": 10,
        "content_type": "application/json+gzip",
    }
    missing = _capture(missing_metadata, source="opera", timestamp=timestamp)
    path = tmp_path / "atomic.sqlite3"
    store = calibration_corpus.CalibrationCorpusStore(path)

    with pytest.raises(FileNotFoundError):
        store.ingest([valid, missing], receipt_id="invalid-batch")
    assert store.statistics()["captures"] == 0
    assert store.statistics()["receipts"] == 0

    bounded = calibration_corpus.CalibrationCorpusStore(
        path,
        bounds=calibration_corpus.CorpusBounds(total_artifact_bytes=1),
    )
    with pytest.raises(ValueError, match="maximale artifactomvang"):
        bounded.ingest([valid], receipt_id="bounded-batch")
    assert store.statistics()["captures"] == 0
    assert store.statistics()["artifacts"] == 0
    assert store.statistics()["receipts"] == 0

    opera_metadata = _artifact(
        monkeypatch, tmp_path, "atomic-opera",
        _payload("opera", timestamp, [(51.05, 4.05, 2)]),
    )
    opera = _capture(opera_metadata, source="opera", timestamp=timestamp)
    recompute_path = tmp_path / "atomic-recompute.sqlite3"
    recompute_bounded = calibration_corpus.CalibrationCorpusStore(
        recompute_path,
        bounds=calibration_corpus.CorpusBounds(sources_per_region_minute=1),
    )
    with pytest.raises(ValueError, match="bronbudget"):
        recompute_bounded.ingest(
            [valid, opera], receipt_id="recompute-must-rollback"
        )
    assert recompute_bounded.statistics()["captures"] == 0
    assert recompute_bounded.statistics()["artifacts"] == 0
    assert recompute_bounded.statistics()["comparisons"] == 0
    assert recompute_bounded.statistics()["bias_samples"] == 0
    assert recompute_bounded.statistics()["receipts"] == 0


def test_service_entrypoint_accepts_worker_job(monkeypatch, tmp_path):
    timestamp = 1_700_000_020.0
    metadata = _artifact(
        monkeypatch, tmp_path, "entrypoint-kmi",
        _payload("kmi", timestamp, [(51.05, 4.05, 2)]),
    )
    capture = _capture(metadata, source="kmi", timestamp=timestamp)
    monkeypatch.setattr(
        calibration_corpus,
        "DEFAULT_CORPUS_PATH",
        tmp_path / "entrypoint.sqlite3",
    )

    result = calibration_corpus.calibration_corpus_ingest(SimpleNamespace(
        job_id="entrypoint-job", payload={"captures": [capture]}
    ))

    assert result["receipt"]["receipt_id"] == "entrypoint-job"
    assert result["statistics"]["frames"] == 1


def test_snapshot_entrypoint_is_compact_consistent_and_read_only(
    monkeypatch, tmp_path,
):
    timestamp = 1_700_000_020.0
    kmi_metadata = _artifact(
        monkeypatch, tmp_path, "snapshot-kmi",
        _payload("kmi", timestamp, [(51.05, 4.05, 2)]),
    )
    opera_metadata = _artifact(
        monkeypatch, tmp_path, "snapshot-opera",
        _payload("opera", timestamp, [(51.05, 4.05, 2)]),
    )
    captures = [
        _capture(kmi_metadata, source="kmi", timestamp=timestamp),
        _capture(opera_metadata, source="opera", timestamp=timestamp),
    ]
    path = tmp_path / "snapshot.sqlite3"
    store = calibration_corpus.CalibrationCorpusStore(path)
    ingested = store.ingest(
        captures, receipt_id="snapshot-source", ingested_at=timestamp + 1
    )
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    monkeypatch.setattr(calibration_corpus, "DEFAULT_CORPUS_PATH", path)

    result = calibration_corpus.calibration_corpus_snapshot(SimpleNamespace(
        job_id="snapshot-read", payload={}
    ))

    after = hashlib.sha256(path.read_bytes()).hexdigest()
    assert before == after
    assert set(result) == {
        "method", "statistics", "profiles", "profiles_total",
        "profiles_truncated",
    }
    assert result["method"] == calibration_corpus.SNAPSHOT_METHOD
    assert result["statistics"] == ingested["statistics"]
    assert result["profiles"] == ingested["profiles"]
    assert result["profiles_total"] == 4
    assert result["profiles_truncated"] is False
    with closing(store._connect_read_only()) as db:
        assert db.execute("PRAGMA query_only").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            db.execute("DELETE FROM captures")


def test_snapshot_may_idempotently_initialize_an_empty_corpus(
    monkeypatch, tmp_path,
):
    path = tmp_path / "empty-snapshot.sqlite3"
    monkeypatch.setattr(calibration_corpus, "DEFAULT_CORPUS_PATH", path)
    job = SimpleNamespace(job_id="empty-snapshot", payload={})

    first = calibration_corpus.calibration_corpus_snapshot(job)
    initialized_sha = hashlib.sha256(path.read_bytes()).hexdigest()
    second = calibration_corpus.calibration_corpus_snapshot(job)

    assert first["statistics"]["captures"] == 0
    assert first["statistics"]["comparisons"] == 0
    assert first["profiles"] == []
    assert second == first
    assert hashlib.sha256(path.read_bytes()).hexdigest() == initialized_sha


@pytest.mark.parametrize("payload", [
    None,
    [],
    {"contract": "stv3-calibration-corpus-snapshot-v1"},
    {"unexpected": "x" * 1024},
])
def test_snapshot_rejects_every_nonempty_or_nondict_payload_before_db_init(
    monkeypatch, tmp_path, payload,
):
    path = tmp_path / "must-not-exist.sqlite3"
    monkeypatch.setattr(calibration_corpus, "DEFAULT_CORPUS_PATH", path)

    with pytest.raises(ValueError, match="exact een leeg object"):
        calibration_corpus.calibration_corpus_snapshot(SimpleNamespace(
            job_id="invalid-snapshot", payload=payload
        ))

    assert not path.exists()
