from __future__ import annotations

import time
from types import SimpleNamespace

import numpy as np
import pytest
from pyproj import Transformer

from storm_tracker_worker.desired_state import Target
from storm_tracker_worker.thin_provider_adapters import RadarFieldIdentity, ThinRadarField
from storm_tracker_worker.thin_footprint_forecast import forecast_target_passages
from storm_tracker_worker.thin_radar_tracking import ThinRadarTracker, build_target_radar_frame
from storm_tracker_worker.thin_system_motion import (
    SystemMotionAdmissionPolicy,
    admit_weather_system_motion,
)
from storm_tracker_worker.tracking_core.engine.motion_consensus import MotionConsensusResult


def _consensus(
    vx: float | None,
    vy: float | None,
    *,
    status: str = "ready_medium",
    confidence: str = "medium",
) -> MotionConsensusResult:
    speed = None if vx is None or vy is None else float(np.hypot(vx, vy))
    return MotionConsensusResult(
        status=status,
        confidence=confidence,
        vx_m_s=vx,
        vy_m_s=vy,
        speed_m_s=speed,
        heading_deg=None,
        selected_method=None if vx is None else "conservative_mean",
        supporting_methods=() if vx is None else ("flow", "track"),
        agreement_angle_deg=None,
        speed_ratio=None,
        normalized_difference=None,
        reason_codes=(),
    )


def _system(
    track_ids: tuple[str, ...],
    *,
    predecessors: tuple[str, ...] = (),
    uncertainty: tuple[str, ...] = (),
):
    return SimpleNamespace(
        system_id="system:current",
        member_tracks=tuple(
            SimpleNamespace(track_id=value, object_kind="rain_shield")
            for value in track_ids
        ),
        predecessor_candidates=tuple(
            SimpleNamespace(previous_system_id=value, selected=index == 0)
            for index, value in enumerate(predecessors)
        ),
        system_uncertainty=uncertainty,
    )


def test_system_motion_requires_admitted_member_consensus() -> None:
    result = admit_weather_system_motion(
        _system(("track:a",)), {}, SystemMotionAdmissionPolicy(),
    )

    assert result.motion_ready is False
    assert result.vx_m_s is None
    assert "member_consensus_missing" in result.reason_codes


def test_system_motion_admits_coherent_member_vectors() -> None:
    result = admit_weather_system_motion(
        _system(("track:a", "track:b")),
        {
            "track:a": _consensus(10.0, 1.0),
            "track:b": _consensus(12.0, 1.0),
        },
        SystemMotionAdmissionPolicy(maximum_member_difference_m_s=5.0),
    )

    assert result.motion_ready is True
    assert result.vx_m_s == pytest.approx(11.0)
    assert result.vy_m_s == pytest.approx(1.0)
    assert result.supporting_track_ids == ("track:a", "track:b")


def test_system_motion_rejects_zero_zero_outlier_pattern() -> None:
    result = admit_weather_system_motion(
        _system(("track:a", "track:b", "track:c")),
        {
            "track:a": _consensus(0.0, 0.0, status="stationary"),
            "track:b": _consensus(0.0, 0.0, status="stationary"),
            "track:c": _consensus(40.0, 0.0),
        },
        SystemMotionAdmissionPolicy(maximum_member_difference_m_s=15.0),
    )

    assert result.motion_ready is False
    assert "member_motion_incoherent" in result.reason_codes


def test_system_motion_keeps_multiple_predecessors_ambiguous() -> None:
    result = admit_weather_system_motion(
        _system(
            ("track:a",),
            predecessors=("system:old-a", "system:old-b"),
            uncertainty=("multiple_predecessor_system_candidates",),
        ),
        {"track:a": _consensus(8.0, 0.0)},
        SystemMotionAdmissionPolicy(),
    )

    assert result.motion_ready is True
    assert result.lineage_status == "ambiguous"
    assert result.predecessor_system_ids == ("system:old-a", "system:old-b")


def test_tracker_needs_no_provider_specific_coordinate_scale():
    target = Target(
        "home", "Home", "home", True, 0.0, 0.01,
        100.0, 5.0, 20.0, "BE",
    )
    target_x, target_y = Transformer.from_crs(
        "EPSG:4326", "EPSG:3857", always_xy=True,
    ).transform(target.longitude, target.latitude)
    field = ThinRadarField(
        "knmi_radar", "rtcor", 100.0, "EPSG:3857",
        target_x - 2_000.0, target_y + 2_000.0, 1_000.0, 1_000.0,
        np.ones((4, 4), dtype=float), np.ones((4, 4), dtype=bool),
    )

    frame = build_target_radar_frame(field, target, maximum_pixels=16)

    assert frame.target_x_m == pytest.approx(target_x)
    assert frame.target_y_m == pytest.approx(target_y)
    assert frame.frame.grid.x_resolution_m == pytest.approx(1_000.0)


def test_target_radar_frame_crops_a_metric_field_around_the_target():
    field = ThinRadarField(
        provider_id="dwd_radolan",
        product_id="radolan_rw",
        observed_at=100.0,
        crs="EPSG:3857",
        origin_x_m=-3_000.0,
        origin_y_m=3_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        rain_rate_mm_h=np.arange(36, dtype=float).reshape(6, 6),
        valid_mask=np.ones((6, 6), dtype=bool),
    )
    target = Target(
        target_id="home",
        display_name="Home",
        kind="home",
        available=True,
        latitude=0.0,
        longitude=0.0,
        position_observed_at=100.0,
        location_accuracy_m=5.0,
        observation_radius_km=50.0,
        country_code="BE",
    )

    result = build_target_radar_frame(field, target, maximum_pixels=16)

    assert result.target_id == "home"
    assert result.frame.grid.width * result.frame.grid.height <= 16
    assert result.frame.grid.crs == "EPSG:3857"
    assert result.frame.valid_at == 100.0
    assert result.frame.values == (
        (7.0, 8.0, 9.0, 10.0),
        (13.0, 14.0, 15.0, 16.0),
        (19.0, 20.0, 21.0, 22.0),
        (25.0, 26.0, 27.0, 28.0),
    )
    assert result.target_x_m == 0.0
    assert result.target_y_m == 0.0


def test_radar_tracker_advances_the_existing_celltrack_engine():
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 50.0, "BE")
    tracker = ThinRadarTracker()

    def field(valid_at: float) -> ThinRadarField:
        rates = np.zeros((16, 16), dtype=float)
        rates[6:10, 6:10] = 5.0
        return ThinRadarField("dwd_radolan", "radolan_rw", valid_at, "EPSG:3857", -8_000.0, 8_000.0, 1_000.0, 1_000.0, rates, np.ones((16, 16), dtype=bool))

    first = tracker.advance(field(100.0), target)
    second = tracker.advance(field(400.0), target)

    assert first.next_state.revision == 1
    assert second.next_state.revision == 2
    assert second.next_state.ledger.tracks


def test_fragmented_regional_frame_retains_all_199_objects_across_two_scans():
    """The live 199-object regional frame must not exceed the NAS work budget."""
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 50.0, "BE")
    tracker = ThinRadarTracker()

    def field(valid_at: float, offset: int) -> ThinRadarField:
        rates = np.zeros((64, 64), dtype=float)
        positions = [(3 + row * 4, 3 + column * 4) for row in range(14) for column in range(15)]
        for row, column in positions[:199]:
            rates[row, column + offset] = 1.5
        return ThinRadarField(
            "fusion:region:home", "fused", valid_at, "EPSG:3857",
            -32_000.0, 32_000.0, 1_000.0, 1_000.0, rates,
            np.ones(rates.shape, dtype=bool),
        )

    first = tracker.advance(field(100.0, 0), target)
    assert first.next_state.revision == 1
    assert len(first.next_state.ledger.tracks) == 199
    second = tracker.advance(field(400.0, 1), target)
    assert second.next_state.revision == 2
    assert "detected_object_capacity_exceeded" not in second.reason_codes
    assert len(second.next_state.last_detection_frame.detections) == 199


def test_regional_tracking_budget_retains_two_gap_frames_and_complete_candidates():
    policy = ThinRadarTracker()._policy
    assert policy.maximum_active_tracks >= policy.maximum_detected_objects * 3
    assert policy.maximum_candidate_edges >= policy.maximum_active_tracks * policy.maximum_detected_objects


def test_regional_budget_overflow_remains_fail_closed_without_losing_detections():
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 60.0, "BE")
    rates = np.zeros((80, 80), dtype=float)
    positions = [(3 + row * 4, 3 + column * 4) for row in range(16) for column in range(17)]
    for row, column in positions[:257]:
        rates[row, column] = 1.5
    field = ThinRadarField(
        "fusion:region:home", "fused", 100.0, "EPSG:3857",
        -40_000.0, 40_000.0, 1_000.0, 1_000.0, rates,
        np.ones(rates.shape, dtype=bool),
    )

    result = ThinRadarTracker().advance(field, target)
    assert result.next_state.revision == 0
    assert result.reason_codes == ("detected_object_capacity_exceeded",)
    assert len(result.detection_frame.detections) == 257


def test_same_valid_time_revision_never_advances_evidence() -> None:
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 50.0, "BE")
    tracker = ThinRadarTracker()
    rates = np.zeros((16, 16), dtype=float)
    rates[6:10, 6:10] = 5.0

    def field(fingerprint: str, peak: float) -> ThinRadarField:
        revised = rates.copy()
        revised[7, 7] = peak
        return ThinRadarField(
            "dwd_radolan", "radolan_rw", 100.0, "EPSG:3857",
            -8_000.0, 8_000.0, 1_000.0, 1_000.0, revised,
            np.ones((16, 16), dtype=bool),
            identity=RadarFieldIdentity(
                quantity="precipitation_rate",
                unit="mm/h",
                quality_kind="source_valid_mask",
                processing_version="radolan-v1",
                source_composition=("dwd_radolan",),
                canonical_valid_at=100.0,
                received_at=101.0,
                available_at=102.0,
                grid_id="radolan-grid",
                content_fingerprint=fingerprint,
            ),
        )

    first = tracker.advance(field("a" * 64, 6.0), target)
    correction = tracker.advance(field("b" * 64, 7.0), target)

    assert first is not None
    assert first.next_state.revision == 1
    assert correction is None
    assert tracker.latest_field_identity("dwd_radolan", "home").content_fingerprint == "b" * 64
    assert tracker.latest_evidence_revision("dwd_radolan", "home") == 1


def test_radar_tracker_only_marks_current_confirmed_tracks_motion_ready():
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 50.0, "BE")
    tracker = ThinRadarTracker()

    def field(valid_at: float, first_column: int | None) -> ThinRadarField:
        rates = np.zeros((16, 16), dtype=float)
        if first_column is not None:
            rates[6:10, first_column:first_column + 4] = 5.0
        return ThinRadarField(
            "dwd_radolan", "radolan_rw", valid_at, "EPSG:3857",
            -8_000.0, 8_000.0, 1_000.0, 1_000.0, rates,
            np.ones((16, 16), dtype=bool),
        )

    first_field = field(100.0, 4)
    tracker.advance(first_field, target)
    first_tracks = tracker.tracks(first_field, "home")
    assert first_tracks
    assert all(track.motion_ready is False for track in first_tracks)

    second_field = field(400.0, 6)
    tracker.advance(second_field, target)
    second_tracks = tracker.tracks(second_field, "home")
    assert second_tracks
    assert any(track.motion_ready is True for track in second_tracks)

    missing_field = field(700.0, None)
    tracker.advance(missing_field, target)
    coasting_tracks = tracker.tracks(missing_field, "home")
    assert coasting_tracks
    assert all(track.motion_ready is False for track in coasting_tracks)


def test_radar_tracker_exposes_one_moving_weather_system_for_nearby_fragments():
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 50.0, "BE")
    tracker = ThinRadarTracker()

    def field(valid_at: float, shift: int) -> ThinRadarField:
        rates = np.zeros((32, 32), dtype=float)
        rates[10:14, 5 + shift:9 + shift] = 2.0
        rates[10:14, 14 + shift:18 + shift] = 6.0
        return ThinRadarField(
            "fusion:region:home", "regional_fusion", valid_at, "EPSG:3857",
            -16_000.0, 16_000.0, 1_000.0, 1_000.0, rates,
            np.ones((32, 32), dtype=bool),
        )

    tracker.advance(field(100.0, 0), target)
    latest = field(400.0, 2)
    tracker.advance(latest, target)

    systems = tracker.systems(latest, "home")

    assert len(systems) == 1
    assert systems[0].motion_ready is True
    assert systems[0].geometry["type"] == "MultiPolygon"
    assert systems[0].mean_rain_rate_mm_h == pytest.approx(4.0)


def test_forecast_tracks_keep_admitted_shield_when_new_member_blocks_system():
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 50.0, "BE")
    tracker = ThinRadarTracker()

    def field(valid_at: float, shift: int, newborn: bool) -> ThinRadarField:
        rates = np.zeros((32, 32), dtype=float)
        rates[10:14, 5 + shift:9 + shift] = 2.0
        if newborn:
            rates[10:14, 18:22] = 6.0
        return ThinRadarField(
            "fusion:region:home", "regional_fusion", valid_at, "EPSG:3857",
            -16_000.0, 16_000.0, 1_000.0, 1_000.0, rates,
            np.ones((32, 32), dtype=bool),
        )

    tracker.advance(field(100.0, 0, False), target)
    latest = field(400.0, 2, True)
    tracker.advance(latest, target)
    systems = tracker.systems(latest, "home")
    assert len(systems) == 1
    assert not systems[0].motion_ready
    assert "member_consensus_missing" in systems[0].motion_reason_codes

    selected = tracker.forecast_tracks(latest, "home")
    ready = [item for item in selected if item.motion_ready]
    assert len(ready) == 1
    assert ready[0].velocity_east_kmh > 0.0
    assert ready[0].footprint_pixels == tuple(
        (row, column) for row in range(10, 14) for column in range(7, 11)
    )
    assert "partial_system_member_forecast" in ready[0].motion_reason_codes
    assert "member_consensus_missing" in ready[0].lineage_reason_codes
    assert not set(ready[0].footprint_pixels).intersection(
        (row, column) for row in range(10, 14) for column in range(18, 22)
    )


def test_forecast_tracks_do_not_duplicate_admitted_system_members():
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 50.0, "BE")
    tracker = ThinRadarTracker()
    for valid_at, shift in ((100.0, 0), (400.0, 2)):
        rates = np.zeros((32, 32), dtype=float)
        rates[10:14, 5 + shift:9 + shift] = 2.0
        rates[10:14, 14 + shift:18 + shift] = 6.0
        latest = ThinRadarField(
            "fusion:region:home", "regional_fusion", valid_at, "EPSG:3857",
            -16_000.0, 16_000.0, 1_000.0, 1_000.0, rates,
            np.ones((32, 32), dtype=bool),
        )
        tracker.advance(latest, target)
    selected = tracker.forecast_tracks(latest, "home")
    assert len(selected) == 1
    assert selected[0].motion_ready
    assert len(selected[0].footprint_pixels) == 32


def test_forecast_tracks_never_promote_unconfirmed_births():
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 50.0, "BE")
    tracker = ThinRadarTracker()
    rates = np.zeros((32, 32), dtype=float)
    rates[10:14, 5:9] = 2.0
    latest = ThinRadarField(
        "fusion:region:home", "regional_fusion", 100.0, "EPSG:3857",
        -16_000.0, 16_000.0, 1_000.0, 1_000.0, rates,
        np.ones((32, 32), dtype=bool),
    )
    tracker.advance(latest, target)
    assert not any(item.motion_ready for item in tracker.forecast_tracks(latest, "home"))


def test_cropped_tracking_pixels_reference_original_quantitative_field():
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 10.0, "BE")
    tracker = ThinRadarTracker()
    for valid_at, shift in ((100.0, 0), (400.0, 2)):
        rates = np.zeros((32, 32), dtype=float)
        rates[14:18, 10 + shift:14 + shift] = 2.0
        latest = ThinRadarField(
            "fusion:region:home", "regional_fusion", valid_at, "EPSG:3857",
            -16_000.0, 16_000.0, 1_000.0, 1_000.0, rates,
            np.ones((32, 32), dtype=bool),
        )
        tracker.advance(latest, target)
    expected = tuple((r, c) for r in range(14, 18) for c in range(12, 16))
    for items in (tracker.tracks(latest, "home"), tracker.systems(latest, "home")):
        assert items
        for item in items:
            assert item.footprint_pixels == expected
            assert all(item.radar_field.rain_rate_mm_h[r, c] == 2.0 for r, c in item.footprint_pixels)
            if item.motion_ready:
                cell = SimpleNamespace(
                    cell_id=item.track_id, provider_id=item.provider_id,
                    observed_at=item.observed_at, latitude=item.latitude,
                    longitude=item.longitude, geometry=item.geometry,
                    precipitation_rate_mm_h=item.rain_rate_mm_h,
                    velocity_east_kmh=item.velocity_east_kmh,
                    velocity_north_kmh=item.velocity_north_kmh,
                    motion_ready=True, radar_field=item.radar_field,
                    footprint_pixels=item.footprint_pixels,
                )
                passages = forecast_target_passages(
                    cell=cell, target_id="home", target_latitude=0.0,
                    target_longitude=0.0, evaluated_at=400.0,
                )
                assert len(passages) == 1
                assert 400.0 < passages[0].entry_at < 7600.0
                assert passages[0].predicted_intensity_mm_h == pytest.approx(2.0)


def test_radar_tracker_ignores_a_repeated_native_frame_without_erasing_tracks(monkeypatch):
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 50.0, "BE")
    tracker = ThinRadarTracker()

    def field(valid_at: float, first_column: int) -> ThinRadarField:
        rates = np.zeros((16, 16), dtype=float)
        rates[6:10, first_column:first_column + 4] = 5.0
        return ThinRadarField(
            "dwd_radolan", "radolan_rw", valid_at, "EPSG:3857",
            -8_000.0, 8_000.0, 1_000.0, 1_000.0, rates,
            np.ones((16, 16), dtype=bool),
        )

    tracker.advance(field(100.0, 4), target)
    tracker.advance(field(400.0, 6), target)
    before = tracker.tracks(field(400.0, 6), "home")
    def unexpected_crop(*args, **kwargs):
        raise AssertionError("cached frame must not be decoded/cropped again")
    monkeypatch.setattr("storm_tracker_worker.thin_radar_tracking.build_target_radar_frame", unexpected_crop)
    repeated = tracker.advance(field(400.0, 6), target)

    assert before
    assert before[0].velocity_east_kmh > 0.0
    assert before[0].geometry["type"] == "MultiPolygon"
    assert before[0].geometry["coordinates"]
    assert repeated is None
    assert tracker.tracks(field(400.0, 6), "home") == before


def test_radar_tracker_reseeds_after_grid_change_and_recovers_motion_next_frame():
    target = Target(
        "home", "Home", "home", True, 0.0, 0.0,
        100.0, 5.0, 50.0, "BE",
    )
    tracker = ThinRadarTracker()

    def field(valid_at: float, origin_x_m: float, first_column: int) -> ThinRadarField:
        rates = np.zeros((16, 16), dtype=float)
        rates[6:10, first_column:first_column + 4] = 5.0
        return ThinRadarField(
            "fusion:region:external", "regional_fusion", valid_at,
            "EPSG:3857", origin_x_m, 8_000.0, 1_000.0, 1_000.0,
            rates, np.ones((16, 16), dtype=bool),
        )

    tracker.advance(field(100.0, -8_000.0, 4), target)
    tracker.advance(field(400.0, -8_000.0, 6), target)
    assert any(
        track.motion_ready
        for track in tracker.tracks(field(400.0, -8_000.0, 6), "home")
    )

    first_new_grid_field = field(700.0, -7_000.0, 5)
    first_new_grid = tracker.advance(first_new_grid_field, target)
    assert first_new_grid is not None
    assert first_new_grid.status == "advanced"
    assert all(
        track.motion_ready is False
        for track in tracker.tracks(first_new_grid_field, "home")
    )

    second_new_grid_field = field(1_000.0, -7_000.0, 7)
    second_new_grid = tracker.advance(second_new_grid_field, target)
    assert second_new_grid is not None
    assert second_new_grid.status == "advanced"
    assert any(
        track.motion_ready
        for track in tracker.tracks(second_new_grid_field, "home")
    )


def test_large_regional_frame_groups_cells_without_quadratic_reprocessing():
    size = 200
    rates = np.zeros((size, size), dtype=float)
    for row in range(10, size - 20, 20):
        for column in range(10, size - 20, 20):
            rates[row : row + 12, column : column + 16] = (
                1.5 + ((row + column) // 40) % 6
            )
    field = ThinRadarField(
        "fusion:region:home",
        "regional_fusion",
        100.0,
        "EPSG:3857",
        -100_000.0,
        100_000.0,
        1_000.0,
        1_000.0,
        rates,
        np.ones((size, size), dtype=bool),
    )
    target = Target(
        "region:home", "Home", "home", True, 0.0, 0.0,
        100.0, 5.0, 250.0, "BE",
    )

    started = time.monotonic()
    result = ThinRadarTracker().advance(field, target)
    elapsed = time.monotonic() - started

    assert len(result.detection_frame.detections) == 126
    assert elapsed < 5.0
