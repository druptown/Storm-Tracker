"""Independent worker tracking from raw observations."""
import ast
import hashlib
import hmac
import json
import math
from pathlib import Path
import time
import uuid

import pytest

from storm_tracker_worker import tracking as tracking_module
from storm_tracker_worker.tracking import track_observation_batch


BASE_TIMESTAMP = int(time.time()) - 600


def _radar(timestamp=BASE_TIMESTAMP):
    return {
        "obs_type": "radar",
        "lat": 51.1,
        "lon": 4.2,
        "timestamp": timestamp,
        "intensity": 4,
        "max_dbz": 42.0,
        "mean_dbz": 35.0,
        "area_km2": 20.0,
        "quality": 0.9,
        "footprint_points": [[51.09, 4.19], [51.11, 4.21]],
        "radar_cell_id": "cell-a",
        "parent_system_id": "system-a",
        "parent_area_km2": 50.0,
        "parent_footprint_points": [[51.08, 4.18], [51.12, 4.22]],
        "rain_mm": None,
        "rain_5min": None,
        "rain_60min": None,
        "station_id": None,
        "rain_capable": False,
        "wind_speed": None,
        "wind_angle": None,
        "gust_speed": None,
        "pressure": None,
        "temperature": None,
        "humidity": None,
        "source": "test_radar",
    }


def _payload(
    observations,
    *,
    start_state=None,
    region_id="region-1",
    start_revision=0,
    batch_id=None,
):
    observations = list(observations)
    start_state = sorted(
        list(start_state or []),
        key=lambda item: str(item.get("storm_id", "")),
    )
    return {
        "tracking_protocol_version": 5,
        "region_id": region_id,
        "batch_id": batch_id or uuid.uuid4().hex,
        "start_revision": start_revision,
        "input_observation_count": len(observations),
        "start_state_count": len(start_state),
        "input_digest": tracking_module._input_digest(observations),
        "start_state_digest": tracking_module._state_digest(start_state),
        "start_state": start_state,
        "observations": observations,
    }


def test_tracking_worker_recalculates_deterministically_from_raw_input():
    payload = _payload([_radar()])

    first = track_observation_batch(payload)
    second = track_observation_batch(payload)

    assert first["method"] == "independent_tracking_batch_v5"
    assert first["tracking_protocol_version"] == 5
    assert first["batch_id"] == payload["batch_id"]
    assert first["start_revision"] == 0
    assert first["result_revision"] == 1
    assert first["snapshot_digest"] == tracking_module._state_digest(
        first["snapshots"]
    )
    assert second["snapshot_digest"] == tracking_module._state_digest(
        second["snapshots"]
    )
    assert (
        first["semantic_snapshot_digest"]
        == second["semantic_snapshot_digest"]
    )
    assert first["active_digest"] == second["active_digest"]
    assert first["input_observation_count"] == 1
    assert first["split_count"] == 0
    assert first["merge_count"] == 0
    assert first["lineage_events"] == []
    assert "trajectory_curved" in first["active_tracks"][0]
    assert "turn_rate_deg_per_hour" in first["active_tracks"][0]
    assert "curvature_radius_km" in first["active_tracks"][0]
    assert "acceleration_kmh2" in first["active_tracks"][0]
    assert "parent_storm_ids" in first["active_tracks"][0]
    assert first["active_tracks"][0]["storm_id"] == second[
        "active_tracks"
    ][0]["storm_id"]


def test_tracking_protocol_has_platform_independent_golden_digests():
    assert tracking_module._input_digest([{
        "lat": 51.1,
        "lon": 4.2,
        "obs_type": "radar",
        "timestamp": 1.0,
    }]) == (
        "3f32f634e1ad3abd42b0485b2dc159f1"
        "9f9a2b20662ca7a3d5bbc2531caa119d"
    )
    assert tracking_module._state_digest([
        {"last_update": 1.0, "storm_id": "a"},
        {"last_update": 2.0, "storm_id": "b"},
    ]) == (
        "f99d5d5d63bb93508d104e4d27e6e592"
        "c750b504737f92cb4e7bd238238267a4"
    )


def test_home_assistant_and_worker_use_identical_tracking_digests():
    root = Path(__file__).resolve().parents[1]
    source_path = root / "custom_components/storm_tracker_v3/__init__.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    names = {
        "_tracking_canonical",
        "_tracking_digest",
        "_normalized_tracking_state",
        "_tracking_input_digest",
        "_tracking_state_digest",
        "_matching_sha256",
    }
    functions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    namespace = {"hashlib": hashlib, "hmac": hmac, "json": json}
    exec(
        compile(
            ast.fix_missing_locations(
                ast.Module(body=functions, type_ignores=[])
            ),
            str(source_path),
            "exec",
        ),
        namespace,
    )
    observations = [{
        "lat": 51.1,
        "lon": 4.2,
        "obs_type": "radar",
        "timestamp": 1.0,
        "received_at": 2.0,
        "parser_version": "parity-parser-v5",
        "artifact_id": "artifact-parity",
        "provenance": {"native_product_time": 1.0},
    }]
    state = [
        {"last_update": 2.0, "storm_id": "b"},
        {"last_update": 1.0, "storm_id": "a"},
    ]

    assert namespace["_tracking_input_digest"](
        observations
    ) == tracking_module._input_digest(observations)
    assert namespace["_tracking_state_digest"](
        state
    ) == tracking_module._state_digest(state)


def test_tracking_v5_digest_binds_provenance_fields():
    original = _radar()
    original.update({
        "artifact_id": "artifact-a",
        "raw_artifact_receipt_id": "receipt-a",
        "upstream_record_id": "upstream-a",
        "contribution_id": "contribution-a",
        "provenance_grade": "exact_raw",
        "provenance": {"native_product_time": BASE_TIMESTAMP},
    })
    changed = json.loads(json.dumps(original))
    changed["provenance"]["native_product_time"] += 1

    assert tracking_module._input_digest([original]) != (
        tracking_module._input_digest([changed])
    )

    snapshot = track_observation_batch(_payload([original]))["snapshots"]
    changed_snapshot = json.loads(json.dumps(snapshot))
    changed_snapshot[0]["radar_cells"][0]["artifact_id"] = "artifact-b"
    assert tracking_module._state_digest(snapshot) != (
        tracking_module._state_digest(changed_snapshot)
    )


def test_home_assistant_v5_validator_rejects_missing_provenance_markers():
    root = Path(__file__).resolve().parents[1]
    source_path = root / "custom_components/storm_tracker_v3/__init__.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    function_names = {
        "_valid_tracking_v5_radar_record",
        "_valid_tracking_v5_snapshot",
    }
    body = [
        node
        for node in tree.body
        if (
            isinstance(node, ast.FunctionDef)
            and node.name in function_names
        )
        or (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name)
                and target.id == "_TRACKING_V5_RADAR_PROVENANCE_KEYS"
                for target in node.targets
            )
        )
    ]
    namespace = {"math": math}
    exec(
        compile(
            ast.fix_missing_locations(ast.Module(body=body, type_ignores=[])),
            str(source_path),
            "exec",
        ),
        namespace,
    )
    snapshot = track_observation_batch(_payload([_radar()]))["snapshots"][0]

    assert namespace["_valid_tracking_v5_snapshot"](snapshot) is True
    disguised_v4 = json.loads(json.dumps(snapshot))
    disguised_v4["radar_cells"][0].pop("source")
    assert namespace["_valid_tracking_v5_snapshot"](disguised_v4) is False


def test_tracking_worker_continues_from_shared_historical_state():
    first = track_observation_batch(_payload([_radar()]))
    second_payload = _payload(
        [_radar(BASE_TIMESTAMP + 300)],
        start_state=first["snapshots"],
        start_revision=first["result_revision"],
    )
    second = track_observation_batch(second_payload)

    assert len(second["active_tracks"]) == 1
    assert second["active_tracks"][0]["storm_id"] == first[
        "active_tracks"
    ][0]["storm_id"]
    assert len(
        second["active_tracks"][0]["radar_frame_timestamps"]
    ) == 2
    assert second["start_state_digest"] == first["snapshot_digest"]


def test_tracking_worker_preserves_structured_footprints():
    observation = _radar()
    polygons = [[[
        [50.9, 3.9], [50.9, 4.5], [51.3, 4.5],
        [51.3, 3.9], [50.9, 3.9],
    ], [
        [51.0, 4.0], [51.0, 4.2], [51.2, 4.2],
        [51.2, 4.0], [51.0, 4.0],
    ]]]
    observation["footprint_polygons"] = polygons
    observation["parent_footprint_polygons"] = polygons

    result = track_observation_batch(_payload([observation]))
    snapshot = result["snapshots"][0]

    assert snapshot["radar_cells"][0]["footprint_polygons"] == polygons
    assert snapshot["frames"][0]["footprint_polygons"] == polygons


def test_tracking_v5_roundtrip_preserves_provenance_and_product_times():
    rainviewer_time = BASE_TIMESTAMP
    noaa_time = BASE_TIMESTAMP + 4 * 60
    rainviewer = _radar(rainviewer_time)
    rainviewer.update({
        "source": "rainviewer",
        "received_at": rainviewer_time + 12.0,
        "parser_version": "rainviewer-parser-v7",
        "artifact_id": "artifact-rainviewer-1200",
        "raw_artifact_receipt_id": "receipt-rainviewer-1200",
        "upstream_record_id": "rainviewer/frame/1200",
        "contribution_id": "fusion-contribution-rainviewer-1200",
        "provenance_grade": "exact_raw",
        "provenance": {
            "artifact_sha256": "a" * 64,
            "native_product_time": rainviewer_time,
            "roles": ["primary", "radar"],
        },
        "radar_cell_id": "rainviewer-cell",
        "parent_system_id": "rainviewer-system",
    })
    first = track_observation_batch(_payload([rainviewer]))

    noaa = _radar(noaa_time)
    noaa.update({
        "source": "noaa_enterprise_rainrate",
        "received_at": noaa_time + 18.0,
        "parser_version": "noaa-enterr-parser-v3",
        "artifact_id": "artifact-noaa-1204",
        "raw_artifact_receipt_id": "receipt-noaa-1204",
        "upstream_record_id": "noaa/enterr/frame/1204",
        "contribution_id": "fusion-contribution-noaa-1204",
        "provenance_grade": "exact_raw",
        "provenance": {
            "artifact_sha256": "b" * 64,
            "native_product_time": noaa_time,
            "roles": ["supplemental", "radar"],
        },
        "radar_cell_id": "noaa-cell",
        "parent_system_id": "noaa-system",
    })
    second = track_observation_batch(_payload(
        [noaa],
        start_state=first["snapshots"],
        start_revision=first["result_revision"],
    ))

    cells = [
        cell
        for snapshot in second["snapshots"]
        for cell in snapshot["radar_cells"]
    ]
    frames = [
        frame
        for snapshot in second["snapshots"]
        for frame in snapshot["frames"]
    ]
    cells_by_source = {item["source"]: item for item in cells}
    frames_by_source = {item["source"]: item for item in frames}
    assert set(cells_by_source) == {
        "rainviewer", "noaa_enterprise_rainrate"
    }
    assert set(frames_by_source) == set(cells_by_source)
    assert {
        item["timestamp"] for item in cells_by_source.values()
    } == {rainviewer_time, noaa_time}
    assert {
        item["timestamp"] for item in frames_by_source.values()
    } == {rainviewer_time, noaa_time}

    expected = {
        "rainviewer": {
            "timestamp": rainviewer_time,
            "received_at": rainviewer_time + 12.0,
            "parser_version": "rainviewer-parser-v7",
            "artifact_id": "artifact-rainviewer-1200",
            "raw_artifact_receipt_id": "receipt-rainviewer-1200",
            "upstream_record_id": "rainviewer/frame/1200",
            "contribution_id": "fusion-contribution-rainviewer-1200",
            "provenance_grade": "exact_raw",
            "provenance": rainviewer["provenance"],
        },
        "noaa_enterprise_rainrate": {
            "timestamp": noaa_time,
            "received_at": noaa_time + 18.0,
            "parser_version": "noaa-enterr-parser-v3",
            "artifact_id": "artifact-noaa-1204",
            "raw_artifact_receipt_id": "receipt-noaa-1204",
            "upstream_record_id": "noaa/enterr/frame/1204",
            "contribution_id": "fusion-contribution-noaa-1204",
            "provenance_grade": "exact_raw",
            "provenance": noaa["provenance"],
        },
    }
    for source, expected_values in expected.items():
        for record in (cells_by_source[source], frames_by_source[source]):
            assert {
                key: record[key] for key in expected_values
            } == expected_values


def test_tracking_worker_rejects_unbounded_observation_batches():
    with pytest.raises(ValueError, match="observatiebatch"):
        track_observation_batch(_payload([_radar()] * 8001))


def test_tracking_worker_preserves_more_than_thirty_independent_tracks():
    observations = []
    for index in range(31):
        item = _radar()
        latitude = -60.0 + index * 4.0
        item.update({
            "lat": latitude,
            "lon": 0.0,
            "footprint_points": [
                [latitude - 0.01, -0.01],
                [latitude + 0.01, 0.01],
            ],
            "radar_cell_id": f"cell-{index}",
            "parent_system_id": f"system-{index}",
            "parent_footprint_points": [
                [latitude - 0.02, -0.02],
                [latitude + 0.02, 0.02],
            ],
        })
        observations.append(item)

    first = track_observation_batch(_payload(
        observations,
        region_id="many-tracks",
    ))
    continued = track_observation_batch(_payload(
        [],
        region_id="many-tracks",
        start_state=first["snapshots"],
        start_revision=first["result_revision"],
    ))

    assert len(first["snapshots"]) == 31
    assert len(continued["snapshots"]) == 31
    assert {
        snapshot["storm_id"] for snapshot in continued["snapshots"]
    } == {
        snapshot["storm_id"] for snapshot in first["snapshots"]
    }


@pytest.mark.parametrize(
    ("field", "replacement", "message"),
    (
        ("input_digest", "0" * 64, "input_digest"),
        ("start_state_digest", "0" * 64, "start_state_digest"),
        ("tracking_protocol_version", 4, "tracking_protocol_version"),
    ),
)
def test_tracking_worker_rejects_tampered_request_binding(
    field, replacement, message,
):
    payload = _payload([_radar()])
    payload[field] = replacement

    with pytest.raises(ValueError, match=message):
        track_observation_batch(payload)


@pytest.mark.parametrize(
    ("batch_id", "start_revision"),
    (
        ("not-a-uuid", 0),
        ("a" * 32, True),
        ("a" * 32, -1),
        ("a" * 32, 1.0),
        ("a" * 32, "1"),
    ),
)
def test_tracking_worker_rejects_invalid_receipt_contract(
    batch_id, start_revision,
):
    payload = _payload(
        [_radar()],
        batch_id=batch_id,
        start_revision=start_revision,
    )

    with pytest.raises(ValueError):
        track_observation_batch(payload)


def test_tracking_worker_rejects_duplicate_start_storm_ids():
    first = track_observation_batch(_payload([_radar()]))
    duplicate_state = [first["snapshots"][0], first["snapshots"][0]]
    payload = _payload([], start_state=duplicate_state, start_revision=1)

    with pytest.raises(ValueError, match="storm-ID"):
        track_observation_batch(payload)


def test_tracking_worker_rejects_non_finite_protocol_numbers():
    observations = [_radar()]
    observations[0]["lat"] = float("nan")

    with pytest.raises(ValueError):
        _payload(observations)


def test_worker_tracking_core_is_exact_mirror_of_integration_core():
    root = Path(__file__).resolve().parents[1]
    pairs = (
        ("engine/association_graph.py", "engine/association_graph.py"),
        ("engine/cell_track.py", "engine/cell_track.py"),
        ("engine/storm_engine.py", "engine/storm_engine.py"),
        ("engine/storm.py", "engine/storm.py"),
        ("engine/trajectory.py", "engine/trajectory.py"),
        ("engine/observation.py", "engine/observation.py"),
        ("geometry/bounding_box.py", "geometry/bounding_box.py"),
        ("geometry/hull.py", "geometry/hull.py"),
        ("geometry/geocode.py", "geometry/geocode.py"),
    )
    for integration_name, worker_name in pairs:
        integration = (
            root / "custom_components/storm_tracker_v3" / integration_name
        ).read_text(encoding="utf-8").replace("\r\n", "\n")
        worker = (
            root / "storm_tracker_worker/tracking_core" / worker_name
        ).read_text(encoding="utf-8").replace("\r\n", "\n")
        assert worker == integration, integration_name
