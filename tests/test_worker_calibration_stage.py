from __future__ import annotations

from contextlib import closing
import hashlib
import json
import sqlite3
from types import SimpleNamespace

import pytest

from storm_tracker_worker import artifacts, calibration_stage


def _capture(
    metadata: dict,
    *,
    source: str,
    timestamp: float,
    region_key: str = "region-1@51.0,4.0,100",
    region_id: str = "region-1",
    center_lat: float = 51.0,
    center_lon: float = 4.0,
    radius_km: float = 100.0,
    coverage_bbox=(2.0, 49.0, 6.0, 53.0),
) -> dict:
    descriptor = {
        "contract": calibration_stage.CAPTURE_CONTRACT,
        "region_key": region_key,
        "region_id": region_id,
        "source": source,
        "product_timestamp": float(timestamp),
        "grid_deg": 0.1,
        "minimum_intensity": 1,
        "evaluation": {
            "center_lat": center_lat,
            "center_lon": center_lon,
            "radius_km": radius_km,
        },
        "coverage_bbox": list(coverage_bbox),
        "coverage_fraction": 1.0,
        "fetch_state": "success",
        "artifact": {**metadata, "frame_timestamp": float(timestamp)},
    }
    canonical = json.dumps(
        descriptor,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    descriptor["capture_id"] = hashlib.sha256(canonical).hexdigest()
    return descriptor


def _provider_payload(source: str, timestamp: float, observations: list[dict]):
    runs = []
    for observation in observations:
        lat = float(observation["lat"])
        lon = float(observation["lon"])
        runs.append({
            "intensity": int(observation["intensity"]),
            "ring": [
                [lat - 0.01, lon - 0.01],
                [lat - 0.01, lon + 0.01],
                [lat + 0.01, lon + 0.01],
                [lat + 0.01, lon - 0.01],
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


def _write_provider_artifact(
    monkeypatch, tmp_path, job_id: str, payload: dict
) -> dict:
    monkeypatch.setattr(artifacts, "ARTIFACT_ROOT", tmp_path)
    monkeypatch.setattr(calibration_stage, "ARTIFACT_ROOT", tmp_path)
    return artifacts.write_json_artifact(job_id, payload)


def test_worker_stage_round_trip_and_repeat_merge_are_idempotent(
    monkeypatch, tmp_path, calibration_store_module,
):
    timestamp = 1_700_000_020.0
    kmi_overlay = _provider_payload("kmi", timestamp, [
        {
            "source": "kmi", "lat": 51.05, "lon": 4.05,
            "intensity": 1, "quality": 0.8,
            "footprint_points": [[51.04, 4.04], [51.06, 4.06]],
        },
        {
            "source": "kmi", "lat": 51.25, "lon": 4.25,
            "intensity": 4, "quality": 0.9,
        },
    ])
    opera_overlay = _provider_payload("opera", timestamp, [
        {
            "source": "opera", "lat": 51.05, "lon": 4.05,
            "intensity": 2, "quality": 0.7,
        },
        {
            "source": "opera", "lat": 51.35, "lon": 4.35,
            "intensity": 5, "quality": 0.6,
        },
    ])
    kmi_meta = _write_provider_artifact(
        monkeypatch, tmp_path, "kmi-product", kmi_overlay
    )
    opera_meta = _write_provider_artifact(
        monkeypatch, tmp_path, "opera-product", opera_overlay
    )
    job = SimpleNamespace(
        job_id="calibration-stage-roundtrip",
        payload={"captures": [
            _capture(kmi_meta, source="kmi", timestamp=timestamp),
            _capture(opera_meta, source="opera", timestamp=timestamp),
        ]},
    )

    result = calibration_stage.calibration_prepare_v2(job)

    assert result["method"] == "worker_radar_calibration_v2"
    assert result["source_artifacts_read"] == 2
    assert result["frames_prepared"] == 2
    assert result["comparisons_prepared"] == 1
    with closing(sqlite3.connect(result["stage_path"])) as stage:
        assert stage.execute("SELECT count(*) FROM frames").fetchone()[0] == 2
        assert stage.execute("SELECT count(*) FROM frame_points").fetchone()[0] == 4
        assert stage.execute(
            "SELECT count(*) FROM provider_bias_samples"
        ).fetchone()[0] == 2

    monkeypatch.setattr(
        calibration_store_module,
        "WORKER_CALIBRATION_STAGE_ROOT",
        tmp_path,
    )
    store = calibration_store_module.CalibrationDataStore(
        str(tmp_path / "calibration.sqlite3")
    )
    store.initialize()
    first = store.merge_worker_calibration_stage(result)
    assert first["frames_written"] == 2
    assert first["comparisons_written"] == 1
    assert first["total_frames"] == 2
    assert first["total_comparisons"] == 1
    restored = store.load_original_radar_frames(region_id="region-1")
    assert {item["source"] for item in restored} == {"kmi", "opera"}
    assert next(
        item for item in restored if item["source"] == "kmi"
    )["overlay"] == kmi_overlay["overlay"]

    repeated = store.merge_worker_calibration_stage(result)
    assert repeated["frames_written"] == 0
    assert repeated["comparisons_written"] == 0
    assert repeated["total_frames"] == 2


def test_missing_artifact_is_not_converted_to_a_dry_frame(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(artifacts, "ARTIFACT_ROOT", tmp_path)
    monkeypatch.setattr(calibration_stage, "ARTIFACT_ROOT", tmp_path)
    timestamp = 1_700_000_020.0
    metadata = {
        "artifact_path": str(tmp_path / "missing.json.gz"),
        "artifact_sha256": "0" * 64,
        "artifact_bytes": 10,
        "content_type": "application/json+gzip",
    }
    job = SimpleNamespace(
        job_id="missing-calibration-artifact",
        payload={"captures": [
            _capture(metadata, source="kmi", timestamp=timestamp)
        ]},
    )

    with pytest.raises(FileNotFoundError):
        calibration_stage.calibration_prepare_v2(job)
    assert not list(tmp_path.glob("*.calibration.sqlite3"))


def test_goes_frame_uses_exact_regional_product_time(monkeypatch, tmp_path):
    envelope_timestamp = 1_700_000_020.0
    regional_timestamp = envelope_timestamp + 60.0
    payload = _provider_payload(
        "noaa_goes_rrqpe",
        envelope_timestamp,
        [{
            "source": "noaa_goes_rrqpe", "lat": 40.0, "lon": -75.0,
            "intensity": 3, "quality": 0.9,
        }],
    )
    payload["overlays_by_satellite"] = {
        "19": {
            "source": "noaa_goes_rrqpe",
            "timestamp": regional_timestamp,
            "runs": payload["overlay"]["runs"],
        }
    }
    metadata = _write_provider_artifact(
        monkeypatch, tmp_path, "goes-mixed-product", payload
    )
    capture = _capture(
        metadata,
        source="noaa_goes_rrqpe",
        timestamp=envelope_timestamp,
        region_key="new-york@40.0,-75.0,100",
        region_id="new-york",
        center_lat=40.0,
        center_lon=-75.0,
        coverage_bbox=(-80.0, 35.0, -70.0, 45.0),
    )

    result = calibration_stage.calibration_prepare_v2(SimpleNamespace(
        job_id="goes-regional-time", payload={"captures": [capture]}
    ))

    with closing(sqlite3.connect(result["stage_path"])) as stage:
        stored = stage.execute(
            "SELECT nominal_minute, product_timestamp FROM frames"
        ).fetchone()
    assert stored == (
        int(round(regional_timestamp / 60.0)), regional_timestamp
    )


def test_filled_overlay_polygon_rasterizes_interior_not_only_vertices():
    descriptor = {
        "grid_deg": 0.1,
        "minimum_intensity": 1,
        "coverage_bbox": (3.0, 50.0, 5.0, 52.0),
        "center_lat": 51.0,
        "center_lon": 4.0,
        "radius_km": 150.0,
    }
    points = calibration_stage._rasterize_overlay({
        "runs": [{
            "intensity": 2,
            "ring": [
                [50.8, 3.8], [50.8, 4.2],
                [51.2, 4.2], [51.2, 3.8],
            ],
        }]
    }, descriptor)
    assert len(points) >= 16
    assert (510, 40) in points


def test_comparison_uses_only_shared_provider_coverage():
    common = {
        "region_key": "region",
        "nominal_minute": 100,
        "product_timestamp": 6000.0,
        "collected_at": 6010.0,
        "grid_deg": 0.1,
        "coverage_fraction": 1.0,
        "evaluation_bbox": (0.0, 0.0, 10.0, 10.0),
    }
    first = {
        **common,
        "source": "a",
        "coverage_bbox": (0.0, 0.0, 10.0, 10.0),
        "points": {(10, 10): [1.0, None, 1], (10, 80): [1.0, None, 1]},
    }
    second = {
        **common,
        "source": "b",
        "coverage_bbox": (0.0, 0.0, 6.0, 10.0),
        "points": {(10, 10): [1.0, None, 1]},
    }
    comparison = calibration_stage._comparison(first, second, 6010.0)
    assert comparison is not None
    assert comparison["primary_cells"] == 1
    assert comparison["reference_cells"] == 1
    assert comparison["false_positive_cells"] == 0
    assert comparison["f1_score"] == 1.0


@pytest.mark.parametrize("dbz,expected", [
    (7.9, 0), (8.0, 1), (14.9, 1), (15.0, 2),
    (20.0, 3), (30.0, 5), (40.0, 7), (50.0, 8),
])
def test_opera_intensity_mapping_preserves_light_echoes(dbz, expected):
    assert calibration_stage._opera_intensity(dbz) == expected


def test_stage_checksum_failure_cannot_modify_target_database(
    monkeypatch, tmp_path, calibration_store_module,
):
    timestamp = 1_700_000_020.0
    payload = _provider_payload("kmi", timestamp, [])
    metadata = _write_provider_artifact(
        monkeypatch, tmp_path, "dry-kmi-product", payload
    )
    result = calibration_stage.calibration_prepare_v2(SimpleNamespace(
        job_id="dry-stage",
        payload={"captures": [
            _capture(metadata, source="kmi", timestamp=timestamp)
        ]},
    ))
    monkeypatch.setattr(
        calibration_store_module,
        "WORKER_CALIBRATION_STAGE_ROOT",
        tmp_path,
    )
    store = calibration_store_module.CalibrationDataStore(
        str(tmp_path / "calibration.sqlite3")
    )
    store.initialize()

    with pytest.raises(ValueError, match="controlesom"):
        store.merge_worker_calibration_stage({
            **result, "stage_sha256": "f" * 64,
        })
    assert store.statistics()["total_frames"] == 0
