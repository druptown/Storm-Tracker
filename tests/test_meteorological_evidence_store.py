"""Contract tests for the isolated append-only vNext evidence ledger."""
from __future__ import annotations

from contextlib import closing
from dataclasses import replace
import importlib.util
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.weather_system_grouping import (
    WeatherSystemGroupingPolicy,
    WeatherSystemTrackInput,
    group_weather_systems,
)


_GROUND_REGISTRY_FINGERPRINT = "a" * 64
_GROUND_CATALOG_FINGERPRINT = "b" * 64
_GROUND_METAR_MAPPING_FINGERPRINT = "c" * 64
_GROUND_SYNOPTIC_MAPPING_FINGERPRINT = "d" * 64


@pytest.fixture(scope="session")
def evidence_module():
    path = (
        Path(__file__).resolve().parents[1]
        / "custom_components"
        / "storm_tracker_v3"
        / "engine"
        / "meteorological_evidence_store.py"
    )
    name = "storm_tracker_v3.engine.meteorological_evidence_store"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def evidence_store(tmp_path, evidence_module):
    store = evidence_module.MeteorologicalEvidenceStore(
        tmp_path / "meteorological-evidence.sqlite3"
    )
    store.initialize()
    return store


def _append_radar_chain(store):
    raw_payload = b"\x89PNG\r\n\x1a\n\x00exact-radar-provider-bytes"
    receipt = store.append_raw_receipt(
        provider_id="rainviewer",
        upstream_record_id="radar/20260730T100000Z/frame-01",
        payload=raw_payload,
        observed_start=1_785_406_800.0,
        observed_end=1_785_406_800.0,
        received_at=1_785_406_806.0,
        media_type="image/png",
        parser_version="rainviewer-parser-3",
    )
    observation = store.append_observation(
        receipt_id=receipt,
        source_observation_id="radar-summary-01",
        observed_start=1_785_406_800.0,
        observed_end=1_785_406_800.0,
        received_at=1_785_406_806.0,
        quantity="precipitation_rate",
        unit="mm/h",
        value_numeric=8.5,
        categorical={"coverage": "convective"},
        latitude=40.72,
        longitude=-73.98,
        native_projection="EPSG:3857",
        native_resolution_x=1000.0,
        native_resolution_y=1000.0,
        parser_version="rainviewer-parser-3",
        calibration_id="rainviewer-native-v2",
    )
    native = store.append_native_frame(
        receipt_id=receipt,
        source_frame_id="native-20260730T100000Z",
        observed_start=1_785_406_800.0,
        observed_end=1_785_406_800.0,
        received_at=1_785_406_806.0,
        quantity="precipitation_rate",
        unit="mm/h",
        categorical={"nodata": 255},
        native_projection="EPSG:3857",
        width=4,
        height=3,
        native_resolution_x=1000.0,
        native_resolution_y=1000.0,
        encoding="float32-le",
        frame_payload=b"\x00\x00\x00\x00" * 12,
        parser_version="rainviewer-parser-3",
        calibration_id="rainviewer-native-v2",
    )
    analysis = store.append_analysis_frame(
        native_frame_id=native,
        analysis_grid_id="nyc-analysis-grid-v1",
        observed_start=1_785_406_800.0,
        observed_end=1_785_406_800.0,
        quantity="precipitation_rate",
        unit="mm/h",
        analysis_projection="EPSG:32618",
        width=4,
        height=3,
        resolution_x=1000.0,
        resolution_y=1000.0,
        encoding="float32-le",
        frame_payload=b"\x00\x00\x10A" * 12,
        transform_version="nearest-conservative-v1",
        calibration_id="rainviewer-native-v2",
    )
    first_detection = store.append_detection(
        analysis_frame_id=analysis,
        detection_label="cell-a-at-t0",
        valid_at=1_785_406_800.0,
        geometry={
            "type": "Polygon",
            "coordinates": [[[0, 0], [2, 0], [2, 2], [0, 0]]],
        },
        centroid_x=583_000.0,
        centroid_y=4_508_000.0,
        area_m2=3_000_000.0,
        maximum_value=17.0,
        mean_value=8.5,
        uncertainty={"centroid_sigma_m": 600.0},
        detector_version="hierarchical-detector-v1",
    )

    second_receipt = store.append_raw_receipt(
        provider_id="rainviewer",
        upstream_record_id="radar/20260730T100500Z/frame-02",
        payload=b"\x89PNG\r\n\x1a\n\x00exact-radar-provider-bytes-2",
        observed_start=1_785_407_100.0,
        observed_end=1_785_407_100.0,
        received_at=1_785_407_106.0,
        media_type="image/png",
        parser_version="rainviewer-parser-3",
    )
    second_native = store.append_native_frame(
        receipt_id=second_receipt,
        source_frame_id="native-20260730T100500Z",
        observed_start=1_785_407_100.0,
        observed_end=1_785_407_100.0,
        received_at=1_785_407_106.0,
        quantity="precipitation_rate",
        unit="mm/h",
        native_projection="EPSG:3857",
        width=4,
        height=3,
        native_resolution_x=1000.0,
        native_resolution_y=1000.0,
        encoding="float32-le",
        frame_payload=b"\x00\x00\x00@" * 12,
        parser_version="rainviewer-parser-3",
        calibration_id="rainviewer-native-v2",
    )
    second_analysis = store.append_analysis_frame(
        native_frame_id=second_native,
        analysis_grid_id="nyc-analysis-grid-v1",
        observed_start=1_785_407_100.0,
        observed_end=1_785_407_100.0,
        quantity="precipitation_rate",
        unit="mm/h",
        analysis_projection="EPSG:32618",
        width=4,
        height=3,
        resolution_x=1000.0,
        resolution_y=1000.0,
        encoding="float32-le",
        frame_payload=b"\x00\x00 A" * 12,
        transform_version="nearest-conservative-v1",
        calibration_id="rainviewer-native-v2",
    )
    second_detection = store.append_detection(
        analysis_frame_id=second_analysis,
        detection_label="cell-a-at-t1",
        valid_at=1_785_407_100.0,
        geometry={
            "type": "Polygon",
            "coordinates": [[[1, 0], [3, 0], [3, 2], [1, 0]]],
        },
        centroid_x=585_000.0,
        centroid_y=4_508_500.0,
        area_m2=3_200_000.0,
        maximum_value=18.0,
        mean_value=9.2,
        uncertainty={"centroid_sigma_m": 550.0},
        detector_version="hierarchical-detector-v1",
    )
    flow_run = store.append_flow_run(
        from_analysis_frame_id=analysis,
        to_analysis_frame_id=second_analysis,
        valid_start=1_785_406_800.0,
        valid_end=1_785_407_100.0,
        width=4,
        height=3,
        field_encoding="float32-le-interleaved-vx-vy",
        field_payload=b"\x00\x00\x00@" * 24,
        method_version="pyramidal-lk-v1",
        parameters={"levels": 3, "window": 15},
        quality={"state": "usable", "valid_fraction": 0.75},
    )
    flow_sample = store.append_flow_sample(
        flow_run_id=flow_run,
        sample_label="cell-a-centroid",
        x_m=584_000.0,
        y_m=4_508_250.0,
        displacement_x_m=2000.0,
        displacement_y_m=500.0,
        velocity_x_mps=6.6666666667,
        velocity_y_mps=1.6666666667,
        covariance=[[360000.0, 0.0], [0.0, 360000.0]],
        quality={"residual": 0.08, "aperture": "resolved"},
    )
    motion = store.append_motion_estimate(
        from_detection_id=first_detection,
        to_detection_id=second_detection,
        valid_start=1_785_406_800.0,
        valid_end=1_785_407_100.0,
        displacement_x_m=2000.0,
        displacement_y_m=500.0,
        velocity_x_mps=6.6666666667,
        velocity_y_mps=1.6666666667,
        covariance=[[360000.0, 0.0], [0.0, 360000.0]],
        method_version="two-position-plus-flow-v1",
        flow_run_id=flow_run,
        flow_sample_id=flow_sample,
        evidence_detail={"aggregation": "centroid_sample"},
    )
    candidate = store.append_association_candidate(
        from_detection_id=first_detection,
        to_detection_id=second_detection,
        motion_id=motion,
        cost=0.18,
        features={
            "iou": 0.61,
            "centroid_distance_m": 2061.55,
            "intensity_similarity": 0.92,
        },
        gates={"retained": True, "reason": "candidate_not_rejected"},
        algorithm_version="global-assignment-v1",
    )
    alternative_candidate = store.append_association_candidate(
        from_detection_id=first_detection,
        to_detection_id=second_detection,
        motion_id=motion,
        cost=0.27,
        features={"iou": 0.61, "challenger": True},
        gates={"retained": True},
        algorithm_version="global-assignment-challenger-v1",
    )
    assignment_run = store.append_assignment_run(
        region_id="new-york",
        from_analysis_frame_id=analysis,
        to_analysis_frame_id=second_analysis,
        valid_start=1_785_406_800.0,
        valid_end=1_785_407_100.0,
        candidate_ids=[candidate, alternative_candidate],
        parameters={"birth_cost": 0.8, "death_cost": 0.8},
        algorithm_version="global-assignment-v1",
    )
    assignment_solution = store.append_assignment_solution(
        assignment_run_id=assignment_run,
        solution_rank=0,
        total_cost=0.18,
        members=[
            {
                "role": "selected_match",
                "candidate_id": candidate,
                "cost": 0.18,
                "rationale": {"globally_selected": True},
            },
            {
                "role": "top_alternative",
                "candidate_id": alternative_candidate,
                "alternative_rank": 1,
                "cost": 0.27,
                "rationale": {"cost_delta": 0.09},
            },
        ],
        set_rationale={"objective": "minimum_total_cost"},
        solver_version="min-cost-flow-v1",
    )
    association_decision = store.append_association_decision(
        candidate_id=candidate,
        assignment_solution_id=assignment_solution,
        decision="selected",
        candidate_rank=0,
        rationale={"global_cost": 0.18},
        algorithm_version="global-assignment-v1",
    )
    track = store.append_cell_track(
        track_key="nyc-cell-a",
        region_id="new-york",
        born_from_detection_id=first_detection,
    )
    first_snapshot = store.append_track_snapshot(
        track_id=track,
        detection_id=first_detection,
        valid_at=1_785_406_800.0,
        state={"x_m": 583000.0, "y_m": 4508000.0, "vx_mps": 0.0},
        covariance=[[360000.0, 0.0], [0.0, 360000.0]],
        lifecycle_state="tentative",
        estimator_version="celltrack-kalman-v1",
    )
    second_snapshot = store.append_track_snapshot(
        track_id=track,
        detection_id=second_detection,
        association_decision_id=association_decision,
        previous_snapshot_id=first_snapshot,
        valid_at=1_785_407_100.0,
        state={
            "x_m": 585000.0,
            "y_m": 4508500.0,
            "vx_mps": 6.6666666667,
            "vy_mps": 1.6666666667,
        },
        covariance=[
            [250000.0, 0.0, 0.0, 0.0],
            [0.0, 250000.0, 0.0, 0.0],
            [0.0, 0.0, 4.0, 0.0],
            [0.0, 0.0, 0.0, 4.0],
        ],
        lifecycle_state="confirmed",
        estimator_version="celltrack-kalman-v1",
    )
    child_track = store.append_cell_track(
        track_key="nyc-cell-a-child",
        region_id="new-york",
        born_from_detection_id=second_detection,
    )
    child_snapshot = store.append_track_snapshot(
        track_id=child_track,
        detection_id=second_detection,
        valid_at=1_785_407_100.0,
        state={"x_m": 585000.0, "y_m": 4508500.0},
        covariance=[[250000.0, 0.0], [0.0, 250000.0]],
        lifecycle_state="tentative",
        estimator_version="celltrack-kalman-v1",
    )
    lineage = store.append_track_lineage(
        event_type="split",
        parent_track_id=track,
        child_track_id=child_track,
        parent_snapshot_id=second_snapshot,
        child_snapshot_id=child_snapshot,
        event_at=1_785_407_100.0,
        algorithm_version="lineage-v1",
    )
    nowcast = store.append_nowcast_run(
        source_snapshot_id=second_snapshot,
        analysis_frame_id=second_analysis,
        issued_at=1_785_407_110.0,
        horizon_seconds=3600.0,
        model_version="celltrack-nowcast-v1",
        parameters={"ensemble_members": 20, "speed_cap": None},
    )
    return {
        "raw_payload": raw_payload,
        "receipt": receipt,
        "observation": observation,
        "native": native,
        "analysis": analysis,
        "second_analysis": second_analysis,
        "first_detection": first_detection,
        "second_detection": second_detection,
        "motion": motion,
        "flow_run": flow_run,
        "flow_sample": flow_sample,
        "candidate": candidate,
        "alternative_candidate": alternative_candidate,
        "assignment_run": assignment_run,
        "assignment_solution": assignment_solution,
        "association_decision": association_decision,
        "track": track,
        "first_snapshot": first_snapshot,
        "second_snapshot": second_snapshot,
        "child_track": child_track,
        "child_snapshot": child_snapshot,
        "lineage": lineage,
        "nowcast": nowcast,
    }


def _weather_grid():
    return AnalysisGrid(
        grid_id="nyc-analysis-grid-v1",
        crs="EPSG:32618",
        origin_x_m=582_500.0,
        origin_y_m=4_507_500.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=4,
        height=3,
    )


def _weather_policy():
    return WeatherSystemGroupingPolicy(
        policy_id="evidence-store-explicit-policy",
        pixel_connectivity=8,
        maximum_footprint_gap_m=500.0,
        maximum_motion_difference_m_s=5.0,
        maximum_motion_age_s=600.0,
        maximum_motion_time_difference_s=300.0,
    )


def _weather_track(
    *,
    track_id,
    snapshot_id,
    detection_id,
    valid_at,
    lifecycle_state,
    footprint,
    object_kind,
    shield_ids,
    vx_m_s=None,
    vy_m_s=None,
    child_track_ids=(),
    lineage_event_ids=(),
    identity_alternative_track_ids=(),
):
    row, column = footprint[0]
    x_m, y_m = _weather_grid().pixel_center(row, column)
    return WeatherSystemTrackInput(
        track_snapshot_id=snapshot_id,
        track_id=track_id,
        analysis_grid_id=_weather_grid().grid_id,
        valid_at=valid_at,
        current_detection_id=detection_id,
        object_kind=object_kind,
        current_footprint_pixels=footprint,
        rain_shield_detection_ids=shield_ids,
        x_m=x_m,
        y_m=y_m,
        vx_m_s=vx_m_s,
        vy_m_s=vy_m_s,
        motion_reference_valid_at=(
            valid_at if vx_m_s is not None else None
        ),
        motion_method=(
            "durable-weather-system-test-motion"
            if vx_m_s is not None
            else None
        ),
        lifecycle_state=lifecycle_state,
        child_track_ids=child_track_ids,
        lineage_event_ids=lineage_event_ids,
        identity_alternative_track_ids=identity_alternative_track_ids,
    )


def _append_weather_groupings(store, radar):
    first_valid_at = 1_785_406_800.0
    second_valid_at = 1_785_407_100.0
    peer_first_detection = store.append_detection(
        analysis_frame_id=radar["analysis"],
        detection_label="peer-cell-at-t0",
        valid_at=first_valid_at,
        geometry={"type": "Point", "coordinates": [3, 2]},
        centroid_x=586_000.0,
        centroid_y=4_510_000.0,
        area_m2=1_000_000.0,
        detector_version="hierarchical-detector-v1",
    )
    peer_second_detection = store.append_detection(
        analysis_frame_id=radar["second_analysis"],
        detection_label="peer-cell-at-t1",
        valid_at=second_valid_at,
        geometry={"type": "Point", "coordinates": [2, 1]},
        centroid_x=585_000.0,
        centroid_y=4_509_000.0,
        area_m2=900_000.0,
        detector_version="hierarchical-detector-v1",
    )
    peer_track = store.append_cell_track(
        track_key="nyc-weather-system-peer",
        region_id="new-york",
        born_from_detection_id=peer_first_detection,
    )
    peer_first_snapshot = store.append_track_snapshot(
        track_id=peer_track,
        detection_id=peer_first_detection,
        valid_at=first_valid_at,
        state={"x_m": 586000.0, "y_m": 4510000.0},
        covariance=[[250000.0, 0.0], [0.0, 250000.0]],
        lifecycle_state="tentative",
        estimator_version="celltrack-kalman-v1",
    )
    peer_second_snapshot = store.append_track_snapshot(
        track_id=peer_track,
        detection_id=peer_second_detection,
        previous_snapshot_id=peer_first_snapshot,
        valid_at=second_valid_at,
        state={"x_m": 585000.0, "y_m": 4509000.0},
        covariance=[[250000.0, 0.0], [0.0, 250000.0]],
        lifecycle_state="confirmed",
        estimator_version="celltrack-kalman-v1",
    )

    previous_tracks = (
        _weather_track(
            track_id=radar["track"],
            snapshot_id=radar["first_snapshot"],
            detection_id=radar["first_detection"],
            valid_at=first_valid_at,
            lifecycle_state="tentative",
            footprint=((0, 0),),
            object_kind="rain_shield",
            shield_ids=(radar["first_detection"],),
        ),
        _weather_track(
            track_id=peer_track,
            snapshot_id=peer_first_snapshot,
            detection_id=peer_first_detection,
            valid_at=first_valid_at,
            lifecycle_state="tentative",
            footprint=((2, 3),),
            object_kind="rain_shield",
            shield_ids=(peer_first_detection,),
        ),
    )
    previous = group_weather_systems(
        valid_at=first_valid_at,
        grid=_weather_grid(),
        tracks=previous_tracks,
        policy=_weather_policy(),
    )
    current_tracks = (
        _weather_track(
            track_id=radar["track"],
            snapshot_id=radar["second_snapshot"],
            detection_id=radar["second_detection"],
            valid_at=second_valid_at,
            lifecycle_state="confirmed",
            footprint=((1, 1),),
            object_kind="rain_shield",
            shield_ids=(radar["second_detection"],),
            vx_m_s=6.6666666667,
            vy_m_s=1.6666666667,
            child_track_ids=(radar["child_track"],),
            lineage_event_ids=(radar["lineage"],),
            identity_alternative_track_ids=(radar["child_track"],),
        ),
        _weather_track(
            track_id=peer_track,
            snapshot_id=peer_second_snapshot,
            detection_id=peer_second_detection,
            valid_at=second_valid_at,
            lifecycle_state="confirmed",
            footprint=((1, 2),),
            object_kind="embedded_core",
            shield_ids=(radar["second_detection"],),
            vx_m_s=-30.0,
            vy_m_s=12.0,
        ),
    )
    current = group_weather_systems(
        valid_at=second_valid_at,
        grid=_weather_grid(),
        tracks=current_tracks,
        policy=_weather_policy(),
        previous=previous,
    )
    previous_record_id = store.append_weather_system_grouping(previous)
    current_record_id = store.append_weather_system_grouping(current)
    return {
        "previous": previous,
        "current": current,
        "previous_record_id": previous_record_id,
        "current_record_id": current_record_id,
        "peer_track": peer_track,
        "peer_second_detection": peer_second_detection,
    }


def _create_exact_v2_store(path, evidence_module):
    payload = b"preserved-v2-raw-blob"
    content_hash = hashlib.sha256(payload).hexdigest()
    with closing(sqlite3.connect(path)) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("BEGIN IMMEDIATE")
        for table in evidence_module._V2_IMMUTABLE_TABLES:
            db.execute(evidence_module._TABLE_DEFINITIONS[table])
            for operation in ("update", "delete"):
                db.execute(
                    f"""
                    CREATE TRIGGER immutable_{table}_{operation}
                    BEFORE {operation.upper()} ON {table}
                    BEGIN
                        SELECT RAISE(
                            ABORT,
                            'append-only evidence table'
                        );
                    END
                    """
                )
        db.execute(
            """
            INSERT INTO meteorological_evidence_metadata
            VALUES('store_kind', 'storm-tracker-meteorological-evidence')
            """
        )
        db.execute(
            """
            INSERT INTO meteorological_evidence_metadata
            VALUES('schema_version', '2')
            """
        )
        db.execute(
            """
            INSERT INTO meteorological_evidence_schema_migrations
            VALUES('empty-to-v2', 0, 2, 0)
            """
        )
        db.execute(
            """
            INSERT INTO meteorological_raw_blobs(
                content_sha256, payload, size_bytes, created_at_ns
            ) VALUES(?, ?, ?, 1)
            """,
            (content_hash, payload, len(payload)),
        )
        db.commit()
    return content_hash, payload


def _append_ground_chain(store, radar):
    payload = (
        b"KJFK 301051Z 18008KT 5SM -RA BR BKN012 OVC025 22/21 A2992"
    )
    receipt = store.append_raw_receipt(
        provider_id="aviationweather-metar",
        upstream_record_id="KJFK/20260730T105100Z",
        payload=payload,
        observed_start=1_785_409_860.0,
        observed_end=1_785_409_860.0,
        received_at=1_785_409_900.0,
        media_type="text/plain",
        parser_version="metar-parser-v4",
    )
    site = store.append_physical_site(
        site_key="physical-site-kjfk",
        latitude=40.639447,
        longitude=-73.779317,
        elevation_m=4.0,
        independence_group="kjfk-airfield",
        site_metadata={"kind": "airport_weather_site"},
    )
    alias = store.append_site_alias(
        physical_site_id=site,
        source_id="metar",
        external_station_id="KJFK",
        valid_start=1_600_000_000.0,
    )
    observation = store.append_ground_observation(
        receipt_id=receipt,
        site_alias_id=alias,
        source_observation_id="KJFK-20260730T105100Z",
        observed_start=1_785_409_800.0,
        observed_end=1_785_409_860.0,
        received_at=1_785_409_900.0,
        quantity="present_weather",
        unit="WMO_4677",
        categorical={"codes": ["-RA", "BR"], "raining": True},
        parser_version="metar-parser-v4",
        calibration_id="metar-weather-codes-v1",
        quality={"age_seconds": 40, "usable": True},
    )
    match = store.append_ground_match(
        ground_observation_id=observation,
        snapshot_id=radar["second_snapshot"],
        distance_m=7200.0,
        temporal_overlap_seconds=60.0,
        match_score=0.89,
        match_details={"inside_advected_footprint": True},
        matcher_version="ground-cell-match-v1",
    )
    decision = store.append_ground_decision(
        ground_match_id=match,
        decision="supports_precipitation",
        policy_version="independent-ground-policy-v1",
        registry_fingerprint=_GROUND_REGISTRY_FINGERPRINT,
        catalog_fingerprint=_GROUND_CATALOG_FINGERPRINT,
        mapping_fingerprint=_GROUND_METAR_MAPPING_FINGERPRINT,
        reason={"independence_group": "kjfk-airfield", "wet": True},
    )
    return {
        "raw_payload": payload,
        "receipt": receipt,
        "site": site,
        "alias": alias,
        "observation": observation,
        "match": match,
        "decision": decision,
    }


def _append_complete_forecast(store):
    radar = _append_radar_chain(store)
    ground = _append_ground_chain(store, radar)
    forecast = store.append_tracker_forecast(
        nowcast_run_id=radar["nowcast"],
        ground_decision_id=ground["decision"],
        tracker_id="device_tracker.new_york_test",
        issued_at=1_785_407_110.0,
        valid_at=1_785_407_710.0,
        predicted_latitude=40.7128,
        predicted_longitude=-74.006,
        precipitation_probability=0.84,
        eta_seconds=600.0,
        forecast={
            "status": "rain_expected",
            "uncertainty_seconds": 240,
        },
    )
    return radar, ground, forecast


def test_empty_database_migrates_to_vnext_once(tmp_path, evidence_module):
    path = tmp_path / "already-empty.sqlite3"
    path.touch()
    store = evidence_module.MeteorologicalEvidenceStore(path)

    first = store.initialize()
    second = store.initialize()

    assert first == second
    assert first["schema_version"] == 3
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute(
            """
            SELECT migration_id, from_version, to_version
            FROM meteorological_evidence_schema_migrations
            """
        ).fetchall() == [("empty-to-v3", 0, 3)]


def test_exact_v2_store_migrates_atomically_to_v3_and_preserves_data(
    tmp_path,
    evidence_module,
):
    path = tmp_path / "exact-v2.sqlite3"
    content_hash, payload = _create_exact_v2_store(path, evidence_module)

    store = evidence_module.MeteorologicalEvidenceStore(path)
    first = store.initialize()
    second = store.initialize()

    assert first == second
    assert first["schema_version"] == 3
    assert first["raw_blobs"] == 1
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute(
            """
            SELECT migration_id, from_version, to_version
            FROM meteorological_evidence_schema_migrations
            ORDER BY to_version
            """
        ).fetchall() == [
            ("empty-to-v2", 0, 2),
            ("v2-to-v3", 2, 3),
        ]
        assert db.execute(
            """
            SELECT payload FROM meteorological_raw_blobs
            WHERE content_sha256=?
            """,
            (content_hash,),
        ).fetchone() == (payload,)
        weather_tables = {
            row[0]
            for row in db.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table'
                  AND name LIKE 'meteorological_weather_system_%'
                """
            )
        }
        assert weather_tables == set(evidence_module._WEATHER_SYSTEM_TABLES)
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            db.execute(
                """
                UPDATE meteorological_evidence_metadata
                SET metadata_value='4'
                WHERE metadata_key='schema_version'
                """
            )
    assert store.integrity_check()["healthy"] is True


def test_corrupt_v2_store_is_not_partially_migrated(
    tmp_path,
    evidence_module,
):
    path = tmp_path / "corrupt-v2.sqlite3"
    _create_exact_v2_store(path, evidence_module)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            """
            DROP TRIGGER
                immutable_meteorological_evidence_metadata_update
            """
        )
        db.execute(
            """
            CREATE TRIGGER
                immutable_meteorological_evidence_metadata_update
            BEFORE UPDATE ON meteorological_evidence_metadata
            BEGIN
                SELECT 1;
            END
            """
        )

    store = evidence_module.MeteorologicalEvidenceStore(path)
    with pytest.raises(
        evidence_module.EvidenceSchemaError,
        match="mist immutable triggers",
    ):
        store.initialize()

    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute(
            """
            SELECT metadata_value
            FROM meteorological_evidence_metadata
            WHERE metadata_key='schema_version'
            """
        ).fetchone() == ("2",)
        assert db.execute(
            """
            SELECT count(*) FROM sqlite_master
            WHERE type='table'
              AND name LIKE 'meteorological_weather_system_%'
            """
        ).fetchone() == (0,)


def test_weather_system_bundle_is_lossless_append_only_and_idempotent(
    evidence_store,
    evidence_module,
):
    radar = _append_radar_chain(evidence_store)
    weather = _append_weather_groupings(evidence_store, radar)
    current = weather["current"]
    expected = evidence_module._normalize_weather_system_grouping_snapshot(
        current
    )

    loaded = evidence_store.load_weather_system_grouping(
        current.grouping_snapshot_id
    )

    assert loaded == expected
    assert loaded["policy"]["fingerprint"] == current.policy.fingerprint
    assert loaded["systems"][0]["aggregate_current_footprint"] == [
        [1, 1],
        [1, 2],
    ]
    assert loaded["systems"][0]["descriptive_motion"][
        "supporting_track_ids"
    ] == [radar["track"]]
    assert loaded["connections"][0]["decision"] == "connected"
    assert loaded["connections"][0]["shared_rain_shield_detection_ids"] == [
        radar["second_detection"]
    ]
    assert len(loaded["systems"][0]["predecessor_candidates"]) == 2
    assert sum(
        item["selected"]
        for item in loaded["systems"][0]["predecessor_candidates"]
    ) == 1
    assert loaded["membership_events"][0]["event_type"] == "merge"
    assert len(
        loaded["membership_events"][0][
            "alternative_predecessor_system_ids"
        ]
    ) == 1
    assert loaded["membership_events"][0][
        "member_lineage_event_ids"
    ] == [radar["lineage"]]
    assert loaded["membership_events"][0][
        "member_identity_alternative_track_ids"
    ] == [radar["child_track"]]

    stats_before_replay = evidence_store.stats()
    assert stats_before_replay["weather_system_groupings"] == 2
    assert stats_before_replay["weather_system_track_inputs"] == 4
    assert stats_before_replay["weather_system_snapshots"] == 3
    assert stats_before_replay["weather_system_members"] == 4
    assert stats_before_replay["weather_system_footprint_pixels"] == 4
    assert stats_before_replay["weather_system_motion_members"] == 1
    assert stats_before_replay["weather_system_connections"] == 2
    assert (
        stats_before_replay["weather_system_predecessor_candidates"] == 2
    )
    assert stats_before_replay["weather_system_membership_events"] == 3

    replay_id = evidence_store.append_weather_system_grouping(current)
    assert replay_id == weather["current_record_id"]
    assert evidence_store.stats() == stats_before_replay
    assert evidence_store.integrity_check()["healthy"] is True

    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            db.execute(
                """
                UPDATE meteorological_weather_system_groupings
                SET policy_id='mutated'
                WHERE weather_system_grouping_id=?
                """,
                (weather["current_record_id"],),
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            db.execute(
                """
                DELETE FROM meteorological_weather_system_connections
                WHERE weather_system_grouping_id=?
                """,
                (weather["current_record_id"],),
            )


def test_weather_system_conflict_records_no_partial_child_revision(
    evidence_store,
    evidence_module,
):
    radar = _append_radar_chain(evidence_store)
    weather = _append_weather_groupings(evidence_store, radar)
    current = weather["current"]
    changed_policy = replace(
        current.policy,
        maximum_motion_difference_m_s=(
            current.policy.maximum_motion_difference_m_s + 1.0
        ),
    )
    conflicting = replace(current, policy=changed_policy)
    counts_before = evidence_store.stats()

    with pytest.raises(
        evidence_module.EvidenceConflictError,
        match="andere immutable inhoud",
    ):
        evidence_store.append_weather_system_grouping(conflicting)

    counts_after = evidence_store.stats()
    assert counts_after["evidence_conflict_events"] == (
        counts_before["evidence_conflict_events"] + 1
    )
    for table, count in counts_before.items():
        if table not in {"evidence_conflict_events"}:
            assert counts_after[table] == count
    assert evidence_store.load_weather_system_grouping(
        current.grouping_snapshot_id
    ) == evidence_module._normalize_weather_system_grouping_snapshot(current)


def test_weather_system_rejects_mismatched_base_snapshot_before_append(
    evidence_store,
    evidence_module,
):
    radar = _append_radar_chain(evidence_store)
    invalid_track = _weather_track(
        track_id=radar["track"],
        snapshot_id=radar["child_snapshot"],
        detection_id=radar["second_detection"],
        valid_at=1_785_407_100.0,
        lifecycle_state="tentative",
        footprint=((1, 1),),
        object_kind="rain_shield",
        shield_ids=(radar["second_detection"],),
    )
    invalid_grouping = group_weather_systems(
        valid_at=1_785_407_100.0,
        grid=_weather_grid(),
        tracks=(invalid_track,),
        policy=_weather_policy(),
    )

    with pytest.raises(
        evidence_module.EvidenceConflictError,
        match="niet overeen met zijn snapshot",
    ):
        evidence_store.append_weather_system_grouping(invalid_grouping)

    assert evidence_store.stats()["weather_system_groupings"] == 0
    assert evidence_store.stats()["weather_system_track_inputs"] == 0


def test_weather_system_bundle_corruption_fails_load_and_integrity(
    evidence_store,
    evidence_module,
):
    radar = _append_radar_chain(evidence_store)
    weather = _append_weather_groupings(evidence_store, radar)
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        db.execute(
            """
            DROP TRIGGER
                immutable_meteorological_weather_system_footprint_pixels_delete
            """
        )
        db.execute(
            """
            DELETE FROM meteorological_weather_system_footprint_pixels
            WHERE weather_system_snapshot_record_id=(
                SELECT weather_system_snapshot_record_id
                FROM meteorological_weather_system_snapshots
                WHERE weather_system_grouping_id=?
                ORDER BY ordinal
                LIMIT 1
            )
              AND ordinal=0
            """,
            (weather["current_record_id"],),
        )

    with pytest.raises(
        evidence_module.EvidenceConflictError,
        match="onvolledig of inconsistent",
    ):
        evidence_store.load_weather_system_grouping(
            weather["current"].grouping_snapshot_id
        )
    report = evidence_store.integrity_check()
    assert report["healthy"] is False
    assert report["evidence_set_mismatches"] >= 1
    assert (
        "immutable_meteorological_weather_system_footprint_pixels_delete"
        in report["missing_immutable_triggers"]
    )


def test_complete_forecast_is_transitively_traceable_to_exact_raw_bytes(
    evidence_store,
):
    radar, ground, forecast = _append_complete_forecast(evidence_store)

    trace = evidence_store.trace_forecast(forecast)

    assert trace["forecast_id"] == forecast
    assert trace["nowcast_run_id"] == radar["nowcast"]
    assert trace["snapshot_id"] == radar["second_snapshot"]
    assert trace["detection_id"] == radar["second_detection"]
    assert trace["analysis_frame_id"] != radar["analysis"]
    assert trace["receipt_id"] != radar["receipt"]
    assert trace["ground"]["ground_receipt_id"] == ground["receipt"]
    assert trace["ground"]["physical_site_id"] == ground["site"]
    assert trace["ground"]["independence_group"] == "kjfk-airfield"
    assert (
        evidence_store.load_raw_payload(radar["receipt"])
        == radar["raw_payload"]
    )
    assert (
        evidence_store.load_raw_payload(ground["receipt"])
        == ground["raw_payload"]
    )
    assert evidence_store.integrity_check() == {
        "healthy": True,
        "sqlite": ["ok"],
        "foreign_key_violations": [],
        "orphan_count": 0,
        "raw_content_mismatches": 0,
        "frame_content_mismatches": 0,
        "record_identity_mismatches": 0,
        "broken_forecast_provenance_chains": 0,
        "broken_ground_provenance_chains": 0,
        "provenance_relation_mismatches": 0,
        "evidence_set_mismatches": 0,
        "site_alias_overlap_conflicts": 0,
        "missing_immutable_triggers": [],
    }


def test_all_requested_evidence_families_exist_in_end_to_end_fixture(
    evidence_store,
):
    _append_complete_forecast(evidence_store)
    stats = evidence_store.stats()

    assert stats["raw_blobs"] == 3
    assert stats["raw_receipts"] == 3
    assert stats["observations"] == 1
    assert stats["native_frames"] == 2
    assert stats["analysis_frames"] == 2
    assert stats["detections"] == 2
    assert stats["flow_runs"] == 1
    assert stats["flow_samples"] == 1
    assert stats["motion_estimates"] == 1
    assert stats["association_candidates"] == 2
    assert stats["association_decisions"] == 1
    assert stats["assignment_runs"] == 1
    assert stats["assignment_run_candidates"] == 2
    assert stats["assignment_solutions"] == 1
    assert stats["assignment_solution_members"] == 2
    assert stats["cell_tracks"] == 2
    assert stats["cell_track_snapshots"] == 3
    assert stats["track_lineage_events"] == 1
    assert stats["track_lineage_members"] == 2
    assert stats["nowcast_runs"] == 1
    assert stats["tracker_forecasts"] == 1
    assert stats["physical_sites"] == 1
    assert stats["physical_site_aliases"] == 1
    assert stats["ground_observations"] == 1
    assert stats["ground_matches"] == 1
    assert stats["ground_decisions"] == 1
    assert stats["ground_decision_inputs"] == 1


def test_exact_duplicate_replay_is_idempotent(evidence_store):
    first = _append_radar_chain(evidence_store)
    before = evidence_store.stats()
    second = _append_radar_chain(evidence_store)

    assert second == first
    assert evidence_store.stats() == before
    assert evidence_store.stats()["evidence_conflict_events"] == 0


def test_same_logical_identity_with_mutated_bytes_records_conflict(
    evidence_store,
    evidence_module,
):
    first = _append_radar_chain(evidence_store)
    with pytest.raises(evidence_module.EvidenceConflictError):
        evidence_store.append_native_frame(
            receipt_id=first["receipt"],
            source_frame_id="native-20260730T100000Z",
            observed_start=1_785_406_800.0,
            observed_end=1_785_406_800.0,
            received_at=1_785_406_806.0,
            quantity="precipitation_rate",
            unit="mm/h",
            native_projection="EPSG:3857",
            width=4,
            height=3,
            native_resolution_x=1000.0,
            native_resolution_y=1000.0,
            encoding="float32-le",
            frame_payload=b"mutated-frame-bytes",
            parser_version="rainviewer-parser-3",
            calibration_id="rainviewer-native-v2",
        )

    assert evidence_store.stats()["native_frames"] == 2
    assert evidence_store.stats()["evidence_conflict_events"] == 1
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        attempted = db.execute(
            """
            SELECT attempted_record_json
            FROM meteorological_evidence_conflict_events
            """
        ).fetchone()[0]
    assert "mutated-frame-bytes" not in attempted
    assert "sha256" in attempted


def test_same_upstream_receipt_with_other_payload_preserves_both_revisions(
    evidence_store,
):
    radar = _append_radar_chain(evidence_store)
    revised = evidence_store.append_raw_receipt(
        provider_id="rainviewer",
        upstream_record_id="radar/20260730T100000Z/frame-01",
        payload=b"different-provider-bytes",
        observed_start=1_785_406_800.0,
        observed_end=1_785_406_800.0,
        received_at=1_785_406_807.0,
        media_type="image/png",
        parser_version="rainviewer-parser-3",
    )

    assert evidence_store.load_raw_payload(radar["receipt"]) == (
        radar["raw_payload"]
    )
    assert evidence_store.load_raw_payload(revised) == (
        b"different-provider-bytes"
    )
    assert evidence_store.stats()["raw_receipts"] == 3
    assert evidence_store.stats()["raw_blobs"] == 3
    assert evidence_store.stats()["source_revision_events"] == 1
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        assert db.execute(
            """
            SELECT prior_receipt_id, revised_receipt_id, conflict_kind
            FROM meteorological_source_revision_events
            """
        ).fetchone() == (
            radar["receipt"],
            revised,
            "upstream_id_byte_revision",
        )
    replay = evidence_store.append_raw_receipt(
        provider_id="rainviewer",
        upstream_record_id="radar/20260730T100000Z/frame-01",
        payload=b"different-provider-bytes",
        observed_start=1_785_406_800.0,
        observed_end=1_785_406_800.0,
        received_at=1_785_406_807.0,
        media_type="image/png",
        parser_version="rainviewer-parser-3",
    )
    assert replay == revised
    assert evidence_store.stats()["source_revision_events"] == 1
    repeated_current_revision = evidence_store.append_raw_receipt(
        provider_id="rainviewer",
        upstream_record_id="radar/20260730T100000Z/frame-01",
        payload=b"different-provider-bytes",
        observed_start=1_785_406_800.0,
        observed_end=1_785_406_800.0,
        received_at=1_785_406_808.0,
        media_type="image/png",
        parser_version="rainviewer-parser-3",
    )
    assert repeated_current_revision != revised
    assert evidence_store.stats()["source_revision_events"] == 1


def test_unknown_and_future_nominal_times_are_retained_with_explicit_quality(
    evidence_store,
):
    unknown = evidence_store.append_raw_receipt(
        provider_id="provider",
        upstream_record_id="unknown-time",
        payload=b"unknown-time-bytes",
        observed_start=None,
        observed_end=None,
        received_at=100.0,
        media_type="application/octet-stream",
        parser_version="v1",
        time_quality={"provider_timestamp": "missing"},
    )
    future = evidence_store.append_raw_receipt(
        provider_id="provider",
        upstream_record_id="future-time",
        payload=b"future-time-bytes",
        observed_start=200.0,
        observed_end=210.0,
        received_at=100.0,
        media_type="application/octet-stream",
        parser_version="v1",
        time_quality={"source_clock": "untrusted"},
    )
    inverted = evidence_store.append_raw_receipt(
        provider_id="provider",
        upstream_record_id="inverted-interval",
        payload=b"inverted-time-bytes",
        observed_start=220.0,
        observed_end=210.0,
        received_at=100.0,
        media_type="application/octet-stream",
        parser_version="v1",
        time_quality={"provider_interval": "malformed"},
    )
    observation = evidence_store.append_observation(
        receipt_id=future,
        source_observation_id="future-observation",
        observed_start=200.0,
        observed_end=210.0,
        received_at=100.0,
        quantity="precipitation_rate",
        unit="mm/h",
        value_numeric=1.0,
        native_projection="EPSG:4326",
        parser_version="v1",
        calibration_id="uncalibrated",
        time_quality={"review_state": "clock_skew"},
    )

    assert evidence_store.load_raw_payload(unknown) == b"unknown-time-bytes"
    assert evidence_store.load_raw_payload(future) == b"future-time-bytes"
    assert evidence_store.load_raw_payload(inverted) == b"inverted-time-bytes"
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        db.row_factory = sqlite3.Row
        raw_rows = {
            row["upstream_record_id"]: json.loads(row["time_quality_json"])
            for row in db.execute(
                """
                SELECT upstream_record_id, time_quality_json
                FROM meteorological_raw_receipts
                WHERE provider_id='provider'
                """
            )
        }
        observation_quality = json.loads(
            db.execute(
                """
                SELECT time_quality_json
                FROM meteorological_observations
                WHERE observation_id=?
                """,
                (observation,),
            ).fetchone()[0]
        )
    assert raw_rows["unknown-time"]["nominal_time_state"] == "unknown"
    assert raw_rows["unknown-time"]["received_before_observed_start"] is None
    assert raw_rows["future-time"]["received_before_observed_start"] is True
    assert raw_rows["future-time"]["reported"]["source_clock"] == "untrusted"
    assert (
        raw_rows["inverted-interval"]["interval_order_state"]
        == "inverted"
    )
    assert observation_quality["received_before_observed_end"] is True
    assert observation_quality["reported"]["review_state"] == "clock_skew"
    assert evidence_store.integrity_check()["healthy"] is True


def test_repeated_fetch_of_same_bytes_retains_distinct_receipt_metadata(
    evidence_store,
):
    first = evidence_store.append_raw_receipt(
        provider_id="provider",
        upstream_record_id="same-bytes",
        payload=b"same",
        observed_start=1.0,
        observed_end=1.0,
        received_at=2.0,
        media_type="application/octet-stream",
        parser_version="v1",
    )
    second = evidence_store.append_raw_receipt(
        provider_id="provider",
        upstream_record_id="same-bytes",
        payload=b"same",
        observed_start=1.0,
        observed_end=1.0,
        received_at=3.0,
        media_type="application/octet-stream",
        parser_version="v1",
    )
    assert first != second
    assert evidence_store.stats()["raw_receipts"] == 2
    assert evidence_store.stats()["raw_blobs"] == 1
    assert evidence_store.stats()["source_revision_events"] == 0


def test_ground_decision_traces_every_match_role_group_and_reason(
    evidence_store,
):
    radar, ground, _ = _append_complete_forecast(evidence_store)
    second_receipt = evidence_store.append_raw_receipt(
        provider_id="synoptic",
        upstream_record_id="NYC-SECOND/20260730T105000Z",
        payload=b'{"precipitation":0.0}',
        observed_start=1_785_409_800.0,
        observed_end=1_785_409_860.0,
        received_at=1_785_409_901.0,
        media_type="application/json",
        parser_version="synoptic-v3",
    )
    second_site = evidence_store.append_physical_site(
        site_key="nyc-independent-site",
        latitude=40.74,
        longitude=-73.97,
        independence_group="nyc-independent-site",
    )
    second_alias = evidence_store.append_site_alias(
        physical_site_id=second_site,
        source_id="synoptic",
        external_station_id="NYC-SECOND",
        valid_start=1_700_000_000.0,
    )
    second_observation = evidence_store.append_ground_observation(
        receipt_id=second_receipt,
        site_alias_id=second_alias,
        source_observation_id="NYC-SECOND-20260730T105000Z",
        observed_start=1_785_409_800.0,
        observed_end=1_785_409_860.0,
        received_at=1_785_409_901.0,
        quantity="interval_precipitation",
        unit="mm",
        value_numeric=0.0,
        parser_version="synoptic-v3",
        calibration_id="native",
        quality={"usable": True},
    )
    second_match = evidence_store.append_ground_match(
        ground_observation_id=second_observation,
        snapshot_id=radar["second_snapshot"],
        distance_m=2500.0,
        temporal_overlap_seconds=60.0,
        match_score=0.92,
        match_details={"inside_advected_footprint": True},
        matcher_version="ground-cell-match-v1",
    )
    decision = evidence_store.append_ground_decision(
        decision_key="two-independent-inputs",
        decision="mixed_evidence",
        policy_version="independent-ground-policy-v2",
        registry_fingerprint=_GROUND_REGISTRY_FINGERPRINT,
        catalog_fingerprint=_GROUND_CATALOG_FINGERPRINT,
        reason={"outcome": "indeterminate"},
        inputs=[
            {
                "ground_match_id": ground["match"],
                "role": "wet_support",
                "mapping_fingerprint": (
                    _GROUND_METAR_MAPPING_FINGERPRINT
                ),
                "reason": {"present_weather": "-RA"},
            },
            {
                "ground_match_id": second_match,
                "role": "dry_counterevidence",
                "mapping_fingerprint": (
                    _GROUND_SYNOPTIC_MAPPING_FINGERPRINT
                ),
                "reason": {"complete_interval": True},
            },
        ],
    )
    forecast = evidence_store.append_tracker_forecast(
        nowcast_run_id=radar["nowcast"],
        ground_decision_id=decision,
        tracker_id="device_tracker.multi_ground_test",
        issued_at=1_785_407_110.0,
        valid_at=1_785_407_711.0,
        predicted_latitude=40.7128,
        predicted_longitude=-74.006,
        precipitation_probability=0.5,
        forecast={"status": "indeterminate"},
    )

    trace = evidence_store.trace_forecast(forecast)
    assert [row["input_role"] for row in trace["ground_inputs"]] == [
        "wet_support",
        "dry_counterevidence",
    ]
    assert {
        row["decision_independence_group"]
        for row in trace["ground_inputs"]
    } == {"kjfk-airfield", "nyc-independent-site"}
    assert trace["ground_provenance_contract"] == (
        "stv3-ground-decision-provenance-v1"
    )
    assert trace["ground_registry_fingerprint"] == (
        _GROUND_REGISTRY_FINGERPRINT
    )
    assert trace["ground_catalog_fingerprint"] == (
        _GROUND_CATALOG_FINGERPRINT
    )
    assert trace["ground_decision_reason"] == {
        "outcome": "indeterminate"
    }
    assert trace["ground_inputs"][0]["mapping_fingerprint"] == (
        _GROUND_METAR_MAPPING_FINGERPRINT
    )
    assert trace["ground_inputs"][1]["mapping_fingerprint"] == (
        _GROUND_SYNOPTIC_MAPPING_FINGERPRINT
    )
    assert trace["ground_inputs"][1]["input_reason"] == {
        "complete_interval": True
    }
    assert evidence_store.integrity_check()["healthy"] is True


def test_ground_decision_fails_closed_without_valid_fingerprints(
    evidence_store,
):
    radar = _append_radar_chain(evidence_store)
    ground = _append_ground_chain(evidence_store, radar)

    with pytest.raises(ValueError, match="mapping_fingerprint"):
        evidence_store.append_ground_decision(
            ground_match_id=ground["match"],
            decision_key="missing-mapping-fingerprint",
            decision="indeterminate",
            policy_version="independent-ground-policy-v2",
            registry_fingerprint=_GROUND_REGISTRY_FINGERPRINT,
            catalog_fingerprint=_GROUND_CATALOG_FINGERPRINT,
            reason={},
        )

    with pytest.raises(ValueError, match="lowercase SHA-256"):
        evidence_store.append_ground_decision(
            ground_match_id=ground["match"],
            decision_key="invalid-registry-fingerprint",
            decision="indeterminate",
            policy_version="independent-ground-policy-v2",
            registry_fingerprint="NOT-A-SHA256",
            catalog_fingerprint=_GROUND_CATALOG_FINGERPRINT,
            mapping_fingerprint=_GROUND_METAR_MAPPING_FINGERPRINT,
            reason={},
        )

    with pytest.raises(ValueError, match="lowercase SHA-256"):
        evidence_store.append_ground_decision(
            decision_key="invalid-input-mapping-fingerprint",
            decision="indeterminate",
            policy_version="independent-ground-policy-v2",
            registry_fingerprint=_GROUND_REGISTRY_FINGERPRINT,
            catalog_fingerprint=_GROUND_CATALOG_FINGERPRINT,
            reason={},
            inputs=[
                {
                    "ground_match_id": ground["match"],
                    "role": "primary",
                    "mapping_fingerprint": "E" * 64,
                    "reason": {},
                }
            ],
        )


def test_coasting_snapshot_without_detection_remains_forecast_traceable(
    evidence_store,
):
    radar = _append_radar_chain(evidence_store)
    coasting = evidence_store.append_track_snapshot(
        track_id=radar["track"],
        detection_id=None,
        previous_snapshot_id=radar["second_snapshot"],
        valid_at=1_785_407_400.0,
        state={"x_m": 587000.0, "y_m": 4509000.0},
        covariance=[[500000.0, 0.0], [0.0, 500000.0]],
        lifecycle_state="coasting",
        snapshot_kind="coasting",
        estimator_version="celltrack-kalman-v1",
    )
    nowcast = evidence_store.append_nowcast_run(
        source_snapshot_id=coasting,
        analysis_frame_id=radar["second_analysis"],
        issued_at=1_785_407_410.0,
        horizon_seconds=900.0,
        model_version="celltrack-nowcast-v1",
        parameters={"source_state": "coasting"},
    )
    forecast = evidence_store.append_tracker_forecast(
        nowcast_run_id=nowcast,
        tracker_id="device_tracker.coasting_test",
        issued_at=1_785_407_410.0,
        valid_at=1_785_407_710.0,
        predicted_latitude=40.72,
        predicted_longitude=-73.99,
        precipitation_probability=0.4,
        forecast={"status": "track_coasting"},
    )

    trace = evidence_store.trace_forecast(forecast)
    assert trace["snapshot_id"] == coasting
    assert trace["detection_id"] is None
    assert trace["analysis_frame_id"] == radar["second_analysis"]
    assert evidence_store.integrity_check()["healthy"] is True


def test_assignment_solution_records_birth_death_and_top_alternative(
    evidence_store,
):
    radar = _append_radar_chain(evidence_store)
    solution = evidence_store.append_assignment_solution(
        assignment_run_id=radar["assignment_run"],
        solution_rank=1,
        total_cost=1.87,
        members=[
            {
                "role": "birth",
                "detection_id": radar["second_detection"],
                "cost": 0.8,
                "rationale": {"unmatched_current": True},
            },
            {
                "role": "death",
                "detection_id": radar["first_detection"],
                "cost": 0.8,
                "rationale": {"unmatched_previous": True},
            },
            {
                "role": "top_alternative",
                "candidate_id": radar["alternative_candidate"],
                "alternative_rank": 1,
                "cost": 0.27,
                "rationale": {"retained_for_audit": True},
            },
        ],
        set_rationale={"rank": 1, "chosen": False},
        solver_version="min-cost-flow-v1",
    )
    replay = evidence_store.append_assignment_solution(
        assignment_run_id=radar["assignment_run"],
        solution_rank=1,
        total_cost=1.87,
        members=[
            {
                "role": "birth",
                "detection_id": radar["second_detection"],
                "cost": 0.8,
                "rationale": {"unmatched_current": True},
            },
            {
                "role": "death",
                "detection_id": radar["first_detection"],
                "cost": 0.8,
                "rationale": {"unmatched_previous": True},
            },
            {
                "role": "top_alternative",
                "candidate_id": radar["alternative_candidate"],
                "alternative_rank": 1,
                "cost": 0.27,
                "rationale": {"retained_for_audit": True},
            },
        ],
        set_rationale={"rank": 1, "chosen": False},
        solver_version="min-cost-flow-v1",
    )
    assert replay == solution
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        roles = db.execute(
            """
            SELECT member_role
            FROM meteorological_assignment_solution_members
            WHERE assignment_solution_id=?
            ORDER BY ordinal
            """,
            (solution,),
        ).fetchall()
    assert roles == [("birth",), ("death",), ("top_alternative",)]
    assert evidence_store.integrity_check()["healthy"] is True


def test_lineage_event_groups_multiple_parents_and_children(evidence_store):
    radar = _append_radar_chain(evidence_store)
    parent_two = evidence_store.append_cell_track(
        track_key="second-parent",
        region_id="new-york",
        born_from_detection_id=radar["first_detection"],
    )
    parent_two_snapshot = evidence_store.append_track_snapshot(
        track_id=parent_two,
        detection_id=radar["first_detection"],
        valid_at=1_785_406_800.0,
        state={"x_m": 582000.0, "y_m": 4507000.0},
        covariance=[[1.0, 0.0], [0.0, 1.0]],
        lifecycle_state="confirmed",
        estimator_version="v1",
    )
    child_two = evidence_store.append_cell_track(
        track_key="second-child",
        region_id="new-york",
        born_from_detection_id=radar["second_detection"],
    )
    child_two_snapshot = evidence_store.append_track_snapshot(
        track_id=child_two,
        detection_id=radar["second_detection"],
        valid_at=1_785_407_100.0,
        state={"x_m": 586000.0, "y_m": 4509000.0},
        covariance=[[1.0, 0.0], [0.0, 1.0]],
        lifecycle_state="tentative",
        estimator_version="v1",
    )
    event = evidence_store.append_track_lineage_event(
        event_key="complex-split-merge-1",
        event_type="split_merge",
        event_at=1_785_407_100.0,
        members=[
            {
                "role": "parent",
                "track_id": radar["track"],
                "snapshot_id": radar["first_snapshot"],
            },
            {
                "role": "parent",
                "track_id": parent_two,
                "snapshot_id": parent_two_snapshot,
            },
            {
                "role": "child",
                "track_id": radar["child_track"],
                "snapshot_id": radar["child_snapshot"],
            },
            {
                "role": "child",
                "track_id": child_two,
                "snapshot_id": child_two_snapshot,
            },
        ],
        algorithm_version="lineage-v2",
        rationale={"overlap_graph": "many-to-many"},
    )
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        grouped = db.execute(
            """
            SELECT member_role, count(*)
            FROM meteorological_track_lineage_members
            WHERE lineage_event_id=?
            GROUP BY member_role
            ORDER BY member_role
            """,
            (event,),
        ).fetchall()
    assert grouped == [("child", 2), ("parent", 2)]
    assert evidence_store.integrity_check()["healthy"] is True


def test_site_alias_history_allows_moves_but_rejects_overlap(
    evidence_store,
    evidence_module,
):
    old_site = evidence_store.append_physical_site(
        site_key="station-old-location",
        latitude=40.0,
        longitude=-74.0,
        independence_group="station-x",
    )
    new_site = evidence_store.append_physical_site(
        site_key="station-new-location",
        latitude=40.1,
        longitude=-73.9,
        independence_group="station-x",
    )
    old_alias = evidence_store.append_site_alias(
        physical_site_id=old_site,
        source_id="metar",
        external_station_id="KXYZ",
        valid_start=0.0,
        valid_end=100.0,
    )
    new_alias = evidence_store.append_site_alias(
        physical_site_id=new_site,
        source_id="metar",
        external_station_id="KXYZ",
        valid_start=100.0,
        valid_end=None,
    )
    assert old_alias != new_alias
    with pytest.raises(
        evidence_module.EvidenceConflictError,
        match="overlapt",
    ):
        evidence_store.append_site_alias(
            physical_site_id=new_site,
            source_id="metar",
            external_station_id="KXYZ",
            valid_start=50.0,
            valid_end=150.0,
        )
    assert evidence_store.stats()["physical_site_aliases"] == 2
    assert evidence_store.stats()["evidence_conflict_events"] == 1
    assert evidence_store.integrity_check()["site_alias_overlap_conflicts"] == 0


def test_wal_full_and_foreign_keys_are_required(evidence_store):
    with closing(evidence_store._connect()) as db, db:
        assert str(db.execute("PRAGMA journal_mode").fetchone()[0]).lower() == (
            "wal"
        )
        assert int(db.execute("PRAGMA synchronous").fetchone()[0]) == 2
        assert int(db.execute("PRAGMA foreign_keys").fetchone()[0]) == 1


def test_flow_field_bytes_and_sample_are_losslessly_linked_to_motion(
    evidence_store,
    evidence_module,
):
    radar = _append_radar_chain(evidence_store)
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        field_type, field_hash, field_payload = db.execute(
            """
            SELECT typeof(field_payload), field_sha256, field_payload
            FROM meteorological_flow_runs
            WHERE flow_run_id=?
            """,
            (radar["flow_run"],),
        ).fetchone()
        motion_relation = db.execute(
            """
            SELECT flow_run_id, flow_sample_id
            FROM meteorological_motion_estimates
            WHERE motion_id=?
            """,
            (radar["motion"],),
        ).fetchone()
    assert field_type == "blob"
    assert hashlib.sha256(field_payload).hexdigest() == field_hash
    assert motion_relation == (radar["flow_run"], radar["flow_sample"])

    other_run = evidence_store.append_flow_run(
        from_analysis_frame_id=radar["analysis"],
        to_analysis_frame_id=radar["second_analysis"],
        valid_start=1_785_406_800.0,
        valid_end=1_785_407_100.0,
        width=4,
        height=3,
        field_encoding="float32-le-interleaved-vx-vy",
        field_payload=b"\x00\x00\x80?" * 24,
        method_version="pyramidal-lk-challenger-v1",
        parameters={},
        quality={"state": "challenger"},
    )
    with pytest.raises(
        evidence_module.EvidenceConflictError,
        match="incompatibele",
    ):
        evidence_store.append_motion_estimate(
            from_detection_id=radar["first_detection"],
            to_detection_id=radar["second_detection"],
            valid_start=1_785_406_800.0,
            valid_end=1_785_407_100.0,
            displacement_x_m=1.0,
            displacement_y_m=1.0,
            velocity_x_mps=1.0,
            velocity_y_mps=1.0,
            covariance=[[1.0, 0.0], [0.0, 1.0]],
            method_version="bad-link",
            flow_run_id=other_run,
            flow_sample_id=radar["flow_sample"],
        )
    assert evidence_store.integrity_check()["healthy"] is True


def test_existing_v1_like_database_fails_closed_without_mutation(
    tmp_path,
    evidence_module,
):
    path = tmp_path / "legacy-v1.sqlite3"
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            """
            CREATE TABLE meteorological_evidence_metadata(
                metadata_key TEXT PRIMARY KEY,
                metadata_value TEXT NOT NULL
            ) WITHOUT ROWID
            """
        )
        db.execute(
            """
            INSERT INTO meteorological_evidence_metadata
            VALUES('store_kind', 'storm-tracker-meteorological-evidence')
            """
        )
        db.execute(
            """
            INSERT INTO meteorological_evidence_metadata
            VALUES('schema_version', '1')
            """
        )
    before = path.read_bytes()
    with pytest.raises(evidence_module.EvidenceSchemaError):
        evidence_module.MeteorologicalEvidenceStore(path).initialize()
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute(
            """
            SELECT metadata_value
            FROM meteorological_evidence_metadata
            WHERE metadata_key='schema_version'
            """
        ).fetchone() == ("1",)
    assert path.read_bytes() == before


def test_missing_parent_foreign_key_fails_closed(
    evidence_store,
    evidence_module,
):
    with pytest.raises(evidence_module.EvidenceForeignKeyError):
        evidence_store.append_native_frame(
            receipt_id="receipt-does-not-exist",
            source_frame_id="orphan",
            observed_start=100.0,
            observed_end=100.0,
            received_at=101.0,
            quantity="precipitation_rate",
            unit="mm/h",
            native_projection="EPSG:4326",
            width=1,
            height=1,
            native_resolution_x=0.01,
            native_resolution_y=0.01,
            encoding="uint8",
            frame_payload=b"\x01",
            parser_version="parser-v1",
            calibration_id="uncalibrated",
        )

    assert evidence_store.stats()["native_frames"] == 0


def test_database_triggers_reject_update_and_delete(evidence_store):
    radar = _append_radar_chain(evidence_store)
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            db.execute(
                """
                UPDATE meteorological_raw_receipts
                SET parser_version='mutated'
                WHERE receipt_id=?
                """,
                (radar["receipt"],),
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            db.execute(
                """
                DELETE FROM meteorological_detections
                WHERE detection_id=?
                """,
                (radar["first_detection"],),
            )


def test_reopen_preserves_ids_bytes_and_forecast_trace(
    evidence_store,
    evidence_module,
):
    radar, _, forecast = _append_complete_forecast(evidence_store)

    reopened = evidence_module.MeteorologicalEvidenceStore(
        evidence_store.path
    )
    reopened.initialize()

    assert reopened.load_raw_payload(radar["receipt"]) == radar["raw_payload"]
    assert reopened.trace_forecast(forecast)["forecast_id"] == forecast
    assert reopened.integrity_check()["healthy"] is True


def test_unknown_schema_version_is_rejected_without_repair(
    evidence_store,
    evidence_module,
):
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        db.execute(
            """
            DROP TRIGGER
                immutable_meteorological_evidence_metadata_update
            """
        )
        db.execute(
            """
            UPDATE meteorological_evidence_metadata
            SET metadata_value='999'
            WHERE metadata_key='schema_version'
            """
        )
    before_reopen = evidence_store.path.read_bytes()

    reopened = evidence_module.MeteorologicalEvidenceStore(
        evidence_store.path
    )
    with pytest.raises(
        evidence_module.EvidenceSchemaError,
        match="onbekende.*schemaversie",
    ):
        reopened.initialize()
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        assert db.execute(
            """
            SELECT metadata_value
            FROM meteorological_evidence_metadata
            WHERE metadata_key='schema_version'
            """
        ).fetchone() == ("999",)
    assert evidence_store.path.read_bytes() == before_reopen


def test_unknown_existing_database_is_not_adopted(
    tmp_path,
    evidence_module,
):
    path = tmp_path / "home-assistant.sqlite3"
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("CREATE TABLE recorder_sentinel(value TEXT NOT NULL)")
        db.execute("INSERT INTO recorder_sentinel VALUES('keep')")
    before = path.read_bytes()

    store = evidence_module.MeteorologicalEvidenceStore(path)
    with pytest.raises(
        evidence_module.EvidenceSchemaError,
        match="onbekend of onvolledig",
    ):
        store.initialize()

    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute(
            "SELECT value FROM recorder_sentinel"
        ).fetchone() == ("keep",)
        assert db.execute(
            """
            SELECT count(*) FROM sqlite_master
            WHERE type='table'
              AND name LIKE 'meteorological_%'
            """
        ).fetchone() == (0,)
    assert path.read_bytes() == before


def test_corrupt_database_fails_closed(tmp_path, evidence_module):
    path = tmp_path / "corrupt.sqlite3"
    path.write_bytes(b"this is not a sqlite database")

    with pytest.raises(
        evidence_module.EvidenceSchemaError,
        match="niet leesbaar",
    ):
        evidence_module.MeteorologicalEvidenceStore(path).initialize()

    assert path.read_bytes() == b"this is not a sqlite database"


def test_integrity_audit_reports_foreign_key_orphan(
    evidence_store,
):
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        db.execute("PRAGMA foreign_keys=OFF")
        db.execute(
            """
            INSERT INTO meteorological_observations(
                observation_id, logical_key, record_hash, receipt_id,
                source_observation_id, observed_start, observed_end,
                received_at, quantity, unit, value_numeric,
                time_quality_json, categorical_json, latitude, longitude,
                native_projection,
                native_resolution_x, native_resolution_y, parser_version,
                calibration_id, created_at_ns
            ) VALUES(
                'orphan-id', 'orphan-key',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                'missing-receipt', 'orphan-source', 1, 1, 2, 'rain',
                'mm/h', 1, '{}', '{}', NULL, NULL, 'EPSG:4326', NULL, NULL,
                'parser-v1', 'uncalibrated', 1
            )
            """
        )

    report = evidence_store.integrity_check()
    assert report["healthy"] is False
    assert report["orphan_count"] == 1
    assert report["foreign_key_violations"][0]["table"] == (
        "meteorological_observations"
    )


def test_integrity_audit_reports_exact_byte_corruption(evidence_store):
    radar = _append_radar_chain(evidence_store)
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        db.execute(
            "DROP TRIGGER immutable_meteorological_raw_blobs_update"
        )
        content_hash = db.execute(
            """
            SELECT content_sha256 FROM meteorological_raw_receipts
            WHERE receipt_id=?
            """,
            (radar["receipt"],),
        ).fetchone()[0]
        db.execute(
            """
            UPDATE meteorological_raw_blobs
            SET payload=X'00'
            WHERE content_sha256=?
            """,
            (content_hash,),
        )

    report = evidence_store.integrity_check()
    assert report["healthy"] is False
    assert report["raw_content_mismatches"] == 1
    assert "immutable_meteorological_raw_blobs_update" in (
        report["missing_immutable_triggers"]
    )


def test_integrity_audit_detects_corrupted_source_revision_bytes_and_hash(
    evidence_store,
):
    original = evidence_store.append_raw_receipt(
        provider_id="provider",
        upstream_record_id="mutable-upstream-id",
        payload=b"revision-one",
        observed_start=1.0,
        observed_end=1.0,
        received_at=2.0,
        media_type="application/octet-stream",
        parser_version="v1",
    )
    revised = evidence_store.append_raw_receipt(
        provider_id="provider",
        upstream_record_id="mutable-upstream-id",
        payload=b"revision-two",
        observed_start=1.0,
        observed_end=1.0,
        received_at=3.0,
        media_type="application/octet-stream",
        parser_version="v1",
    )
    unrelated = evidence_store.append_raw_receipt(
        provider_id="provider",
        upstream_record_id="unrelated",
        payload=b"unrelated-bytes",
        observed_start=1.0,
        observed_end=1.0,
        received_at=4.0,
        media_type="application/octet-stream",
        parser_version="v1",
    )
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        revised_hash = db.execute(
            """
            SELECT content_sha256 FROM meteorological_raw_receipts
            WHERE receipt_id=?
            """,
            (revised,),
        ).fetchone()[0]
        unrelated_hash = db.execute(
            """
            SELECT content_sha256 FROM meteorological_raw_receipts
            WHERE receipt_id=?
            """,
            (unrelated,),
        ).fetchone()[0]
        db.execute(
            "DROP TRIGGER immutable_meteorological_raw_blobs_update"
        )
        db.execute(
            """
            UPDATE meteorological_raw_blobs
            SET payload=X'00'
            WHERE content_sha256=?
            """,
            (revised_hash,),
        )
        db.execute(
            """
            DROP TRIGGER
                immutable_meteorological_source_revision_events_update
            """
        )
        db.execute(
            """
            UPDATE meteorological_source_revision_events
            SET revised_content_sha256=?
            WHERE prior_receipt_id=? AND revised_receipt_id=?
            """,
            (unrelated_hash, original, revised),
        )

    report = evidence_store.integrity_check()
    assert report["healthy"] is False
    assert report["raw_content_mismatches"] == 1
    assert report["record_identity_mismatches"] >= 1
    assert report["provenance_relation_mismatches"] >= 1
    assert {
        "immutable_meteorological_raw_blobs_update",
        "immutable_meteorological_source_revision_events_update",
    }.issubset(report["missing_immutable_triggers"])


def test_integrity_audit_detects_missing_source_revision_event(evidence_store):
    evidence_store.append_raw_receipt(
        provider_id="provider",
        upstream_record_id="revision-gap",
        payload=b"first",
        observed_start=1.0,
        observed_end=1.0,
        received_at=2.0,
        media_type="application/octet-stream",
        parser_version="v1",
    )
    evidence_store.append_raw_receipt(
        provider_id="provider",
        upstream_record_id="revision-gap",
        payload=b"second",
        observed_start=1.0,
        observed_end=1.0,
        received_at=3.0,
        media_type="application/octet-stream",
        parser_version="v1",
    )
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        db.execute(
            """
            DROP TRIGGER
                immutable_meteorological_source_revision_events_delete
            """
        )
        db.execute("DELETE FROM meteorological_source_revision_events")

    report = evidence_store.integrity_check()
    assert report["healthy"] is False
    assert report["provenance_relation_mismatches"] >= 1
    assert (
        "immutable_meteorological_source_revision_events_delete"
        in report["missing_immutable_triggers"]
    )


def test_integrity_audit_detects_missing_ground_decision_member(
    evidence_store,
    evidence_module,
):
    _, ground, forecast = _append_complete_forecast(evidence_store)
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        db.execute(
            """
            DROP TRIGGER
                immutable_meteorological_ground_decision_inputs_delete
            """
        )
        db.execute(
            """
            DELETE FROM meteorological_ground_decision_inputs
            WHERE ground_decision_id=?
            """,
            (ground["decision"],),
        )

    report = evidence_store.integrity_check()
    assert report["healthy"] is False
    assert report["evidence_set_mismatches"] == 1
    assert report["broken_ground_provenance_chains"] == 1
    assert (
        "immutable_meteorological_ground_decision_inputs_delete"
        in report["missing_immutable_triggers"]
    )
    with pytest.raises(
        evidence_module.EvidenceStoreError,
        match="ground-provenance",
    ):
        evidence_store.trace_forecast(forecast)


def test_integrity_audit_reports_cross_record_provenance_mismatch(
    evidence_store,
):
    radar = _append_radar_chain(evidence_store)
    evidence_store.append_nowcast_run(
        source_snapshot_id=radar["second_snapshot"],
        analysis_frame_id=radar["analysis"],
        issued_at=1_785_407_111.0,
        horizon_seconds=1800.0,
        model_version="deliberately-inconsistent-test-run",
        parameters={},
    )

    report = evidence_store.integrity_check()
    assert report["healthy"] is False
    assert report["provenance_relation_mismatches"] == 1
    assert report["orphan_count"] == 0


def test_raw_and_frame_payloads_are_real_sqlite_blobs(evidence_store):
    radar = _append_radar_chain(evidence_store)
    with closing(sqlite3.connect(evidence_store.path)) as db, db:
        raw_type, raw_value = db.execute(
            """
            SELECT typeof(blob.payload), blob.payload
            FROM meteorological_raw_receipts AS receipt
            JOIN meteorological_raw_blobs AS blob USING(content_sha256)
            WHERE receipt.receipt_id=?
            """,
            (radar["receipt"],),
        ).fetchone()
        frame_types = db.execute(
            """
            SELECT typeof(frame_payload)
            FROM meteorological_native_frames
            """
        ).fetchall()
    assert raw_type == "blob"
    assert raw_value == radar["raw_payload"]
    assert frame_types == [("blob",), ("blob",)]


@pytest.mark.parametrize(
    ("call", "match"),
    (
        (
            lambda store: store.append_raw_receipt(
                provider_id="rainviewer",
                upstream_record_id="https://example.invalid/frame.png",
                payload=b"secret-free",
                observed_start=1,
                observed_end=1,
                received_at=2,
                media_type="image/png",
                parser_version="v1",
            ),
            "URL",
        ),
        (
            lambda store: store.append_physical_site(
                site_key="site-a",
                latitude=50,
                longitude=4,
                independence_group="group-a",
                site_metadata={"headers": {"Authorization": "secret"}},
            ),
            "gevoelig",
        ),
    ),
)
def test_transport_secrets_are_outside_the_evidence_contract(
    evidence_store,
    call,
    match,
):
    with pytest.raises(ValueError, match=match):
        call(evidence_store)


def test_record_ids_are_stable_across_independent_databases(
    tmp_path,
    evidence_module,
):
    stores = [
        evidence_module.MeteorologicalEvidenceStore(
            tmp_path / f"stable-{index}.sqlite3"
        )
        for index in range(2)
    ]
    ids = []
    for store in stores:
        store.initialize()
        ids.append(_append_radar_chain(store))

    assert ids[0] == ids[1]


def test_store_requires_explicit_initialization(tmp_path, evidence_module):
    store = evidence_module.MeteorologicalEvidenceStore(
        tmp_path / "not-initialized.sqlite3"
    )
    with pytest.raises(
        evidence_module.EvidenceStoreError,
        match="initialize",
    ):
        store.append_raw_receipt(
            provider_id="provider",
            upstream_record_id="record",
            payload=b"bytes",
            observed_start=1,
            observed_end=1,
            received_at=2,
            media_type="application/octet-stream",
            parser_version="v1",
        )


def test_worker_mirror_is_byte_identical():
    root = Path(__file__).resolve().parents[1]
    integration = (
        root
        / "custom_components"
        / "storm_tracker_v3"
        / "engine"
        / "meteorological_evidence_store.py"
    ).read_bytes()
    worker = (
        root
        / "storm_tracker_worker"
        / "tracking_core"
        / "engine"
        / "meteorological_evidence_store.py"
    ).read_bytes()
    assert worker == integration


def test_vnext_evidence_store_remains_unwired_shadow_code():
    root = Path(__file__).resolve().parents[1]
    integration_root = root / "custom_components" / "storm_tracker_v3"
    runtime_files = [integration_root / "__init__.py"]
    runtime_files.extend((integration_root / "providers").glob("*.py"))
    worker_root = root / "storm_tracker_worker"
    runtime_files.extend(worker_root.glob("*.py"))

    references = {
        str(path.relative_to(root))
        for path in runtime_files
        if path.is_file()
        and path.name != "worker_cutover_manifest.py"
        and "meteorological_evidence_store" in path.read_text(
            encoding="utf-8"
        )
    }
    assert references == set()
