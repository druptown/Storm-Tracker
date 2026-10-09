from __future__ import annotations

from types import SimpleNamespace
import gzip
import hashlib
import json
from pathlib import Path

import storm_tracker_worker.artifacts as artifacts
import storm_tracker_worker.radar_frame as module


def test_worker_builds_bounded_frame_and_compact_display_layer(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(artifacts, "ARTIFACT_ROOT", tmp_path)
    monkeypatch.setattr(module, "ARTIFACT_ROOT", tmp_path)
    input_meta = artifacts.write_json_artifact(
        "input",
        {
            "artifact_type": "radar_overlays",
            "regions": {"region-1": [4.0, 50.0, 4.2, 50.2]},
            "overlays": {
                "region-1": {
                    "source": "test_radar",
                    "timestamp": 1_785_200_123,
                    "runs": [{
                        "intensity": 120,
                        "ring": [
                            [50.04, 4.04], [50.04, 4.12],
                            [50.10, 4.12], [50.10, 4.04],
                            [50.04, 4.04],
                        ],
                    }],
                },
            },
        },
    )
    result = module.radar_frame_operational(
        SimpleNamespace(job_id="output", payload=input_meta)
    )
    value = json.loads(gzip.decompress(
        Path(result["artifact_path"]).read_bytes()
    ))
    assert value["artifact_type"] == "regional_radar_frames"
    region = value["regions"]["region-1"]
    assert region["frame"]["cells"]
    assert region["frame"]["nominal_timestamp"] % 60 == 0
    assert region["overlay"]["worker_bounded"] is True
    assert len(region["overlay"]["runs"]) < len(region["frame"]["cells"])


def test_worker_uses_source_adaptive_resolution():
    assert module._source_grid_deg("kmi") == 0.005
    assert module._source_grid_deg("dwd_radolan") == 0.005
    assert module._source_grid_deg("opera") == 0.01
    assert module._source_grid_deg("rainviewer") == 0.01
    assert module._source_grid_deg("noaa_enterprise_rainrate") == 0.02
    assert module._source_grid_deg("noaa_goes_rrqpe") == 0.02


def test_large_high_resolution_region_stays_safely_bounded():
    grid_deg, height, width = module._shape(
        (-0.5, 49.5, 5.5, 53.5), "kmi"
    )
    assert grid_deg >= module.HIGH_RES_GRID_DEG
    assert height * width <= module.MAX_GRID_CELLS


def test_worker_rejects_input_artifact_outside_shared_root(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(module, "ARTIFACT_ROOT", tmp_path / "allowed")
    foreign = tmp_path / "foreign.json.gz"
    foreign.write_bytes(b"invalid")
    try:
        module._read_input({
            "artifact_path": str(foreign),
            "artifact_bytes": len(foreign.read_bytes()),
            "artifact_sha256": "x",
        })
    except ValueError as err:
        assert "buiten" in str(err)
    else:
        raise AssertionError("foreign artifact should be rejected")


def test_worker_rejects_concatenated_input_artifact(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(module, "ARTIFACT_ROOT", tmp_path)
    visible = gzip.compress(
        json.dumps({
            "artifact_type": "radar_overlays",
            "regions": {},
            "overlays": {},
        }).encode(),
        mtime=0,
    )
    hidden = gzip.compress(b'{"artifact_type":"hidden"}', mtime=0)
    payload = visible + hidden
    path = tmp_path / "concatenated.json.gz"
    path.write_bytes(payload)

    try:
        module._read_input({
            "artifact_path": str(path),
            "artifact_bytes": len(payload),
            "artifact_sha256": hashlib.sha256(payload).hexdigest(),
        })
    except ValueError as err:
        assert "extra leden" in str(err) or "trailing" in str(err)
    else:
        raise AssertionError("concatenated gzip should be rejected")


def test_goes_provider_artifact_uses_exact_satellite_overlay_and_time(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(artifacts, "ARTIFACT_ROOT", tmp_path)
    monkeypatch.setattr(module, "ARTIFACT_ROOT", tmp_path)

    def overlay(timestamp, west):
        return {
            "source": "noaa_goes_rrqpe",
            "timestamp": timestamp,
            "runs": [{
                "intensity": 3,
                "ring": [
                    [35.00, west], [35.00, west + 0.05],
                    [35.05, west + 0.05], [35.05, west],
                    [35.00, west],
                ],
            }],
        }

    provider = artifacts.write_json_artifact(
        "goes-input",
        {
            "provider": "noaa_goes_rrqpe",
            "frame_timestamp": 1_000.0,
            "observations": [],
            "overlay": overlay(1_000.0, -130.0),
            "overlays_by_satellite": {
                "18": overlay(1_000.0, -130.0),
                "19": overlay(1_060.0, -75.0),
            },
        },
    )
    result = module.radar_frame_from_provider_artifacts(SimpleNamespace(
        job_id="goes-output",
        payload={"selections": {
            "west": {
                "provider": "noaa_goes_rrqpe",
                "artifact": provider,
                "bounds": [-130.1, 34.9, -129.9, 35.1],
            },
            "east": {
                "provider": "noaa_goes_rrqpe",
                "artifact": provider,
                "bounds": [-75.1, 34.9, -74.9, 35.1],
            },
            "unsupported": {
                "provider": "noaa_goes_rrqpe",
                "artifact": provider,
                "bounds": [3.9, 50.9, 4.1, 51.1],
            },
        }},
    ))
    value = json.loads(gzip.decompress(Path(result["artifact_path"]).read_bytes()))
    assert value["regions"]["west"]["overlay"]["timestamp"] == 1_000.0
    assert value["regions"]["east"]["overlay"]["timestamp"] == 1_060.0
    assert "unsupported" not in value["regions"]
