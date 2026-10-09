from __future__ import annotations

from types import SimpleNamespace
import unittest

import numpy as np
from pyproj import Transformer

from storm_tracker_worker.thin_provider_adapters import ThinRadarField
from storm_tracker_worker.thin_replay import run_motion_admission_replay
from storm_tracker_worker.thin_radar_tracking import ThinRadarTracker
from storm_tracker_worker.tracking_core.engine.motion_consensus import (
    MotionVectorCandidate,
    select_motion_consensus,
)


_COVARIANCE = ((4.0, 0.0), (0.0, 4.0))


def _candidate(method: str, role: str, vx: float, vy: float):
    return MotionVectorCandidate(
        method=method,
        role=role,
        vx_m_s=vx,
        vy_m_s=vy,
        covariance_m2_s2=_COVARIANCE,
        support_count=16,
        support_fraction=0.8,
        history_samples=4,
        hindcast_skill=None,
    )


def _field(index: int) -> ThinRadarField:
    values = np.zeros((20, 20), dtype=float)
    for row in range(7, 13):
        for column in range(5 + index, 11 + index):
            values[row, column] = (
                2.0 + (row - 7) * 0.4 + (column - 5 - index) * 0.3
            )
    return ThinRadarField(
        provider_id="radar.test",
        product_id="regional-rain-rate",
        observed_at=float(index * 300),
        crs="EPSG:3857",
        origin_x_m=0.0,
        origin_y_m=20_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        rain_rate_mm_h=values,
        valid_mask=np.ones_like(values, dtype=bool),
    )


class MotionConsensusTests(unittest.TestCase):
    def test_deterministic_motion_replay_covers_failure_modes(self) -> None:
        report = run_motion_admission_replay()
        self.assertTrue(report["passed"])
        self.assertFalse(report["operational_state_mutated"])
        self.assertEqual(report["scenarios"]["translation"], "ready_high")
        self.assertEqual(
            report["scenarios"]["intensity_change_without_translation"],
            "stationary",
        )
        self.assertEqual(
            report["scenarios"]["rotation_without_bulk_translation"],
            "indeterminate",
        )
        self.assertTrue(report["checks"]["independent_system_directions"])

    def test_wraparound_headings_agree(self) -> None:
        result = select_motion_consensus((
            _candidate("flow", "optical_flow", -0.1, 5.0),
            _candidate("track", "object_track", 0.1, 5.0),
        ))
        self.assertEqual(result.status, "ready_medium")
        self.assertLess(result.agreement_angle_deg or 999.0, 5.0)

    def test_opposite_vectors_are_withheld(self) -> None:
        result = select_motion_consensus((
            _candidate("flow", "optical_flow", 10.0, 0.0),
            _candidate("track", "object_track", -10.0, 0.0),
        ))
        self.assertEqual(result.status, "indeterminate")
        self.assertIsNone(result.vx_m_s)
        self.assertIn("motion_candidates_disagree", result.reason_codes)

    def test_zero_motion_has_no_heading(self) -> None:
        result = select_motion_consensus((
            _candidate("flow", "optical_flow", 0.0, 0.0),
            _candidate("track", "object_track", 0.0, 0.0),
        ))
        self.assertEqual(result.status, "stationary")
        self.assertIsNone(result.heading_deg)

    def test_thin_tracker_uses_admitted_flow_consensus(self) -> None:
        longitude, latitude = Transformer.from_crs(
            "EPSG:3857", "EPSG:4326", always_xy=True
        ).transform(10_000.0, 10_000.0)
        scope = SimpleNamespace(
            target_id="region-test",
            latitude=latitude,
            longitude=longitude,
            observation_radius_km=10.0,
        )
        tracker = ThinRadarTracker()
        tracker.advance(_field(0), scope)
        tracker.advance(_field(1), scope)

        tracks = tracker.tracks(_field(1), scope.target_id)
        rain = next(item for item in tracks if item.motion_ready)
        self.assertEqual(rain.motion_status, "ready_medium")
        self.assertEqual(rain.motion_confidence, "medium")
        self.assertIn("conservative_mean", rain.motion_method or "")
        self.assertGreater(rain.velocity_east_kmh, 0.0)


if __name__ == "__main__":
    unittest.main()
