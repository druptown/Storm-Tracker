from __future__ import annotations

from types import SimpleNamespace
import unittest

from storm_tracker_worker.thin_footprint_forecast import forecast_target_passage
from storm_tracker_worker.thin_postgis import (
    _passage_metadata,
    _track_motion_metadata,
)


def _ready_cell() -> SimpleNamespace:
    return SimpleNamespace(
        cell_id="radar.test:region:test-cell",
        provider_id="radar.test",
        observed_at=1_000.0,
        latitude=51.0,
        longitude=4.0,
        geometry={
            "type": "Polygon",
            "coordinates": [[
                [3.99, 50.99],
                [4.01, 50.99],
                [4.01, 51.01],
                [3.99, 51.01],
                [3.99, 50.99],
            ]],
        },
        precipitation_rate_mm_h=4.2,
        velocity_east_kmh=24.0,
        velocity_north_kmh=6.0,
        speed_kmh=24.7386337537,
        heading_deg=75.9637565321,
        motion_ready=True,
        motion_status="ready_high",
        motion_confidence="high",
        motion_method="optical_flow_primary_object_track_confirmed",
        motion_reason_codes=("motion_candidates_agree",),
        motion_supporting_methods=("optical_flow", "object_track"),
        motion_agreement_angle_deg=4.5,
    )


class MotionPersistenceTests(unittest.TestCase):
    def test_passage_preserves_admitted_motion_evidence(self) -> None:
        cell = _ready_cell()
        passage = forecast_target_passage(
            cell=cell,
            target_id="target.test",
            target_latitude=51.0,
            target_longitude=4.0,
            evaluated_at=1_000.0,
        )

        self.assertIsNotNone(passage)
        assert passage is not None
        self.assertEqual(passage.motion_status, "ready_high")
        self.assertEqual(passage.motion_confidence, "high")
        self.assertEqual(
            passage.motion_method,
            "optical_flow_primary_object_track_confirmed",
        )
        self.assertEqual(
            passage.motion_supporting_methods,
            ("optical_flow", "object_track"),
        )
        self.assertEqual(passage.motion_agreement_angle_deg, 4.5)

    def test_postgis_metadata_retains_ready_and_withheld_evidence(self) -> None:
        cell = _ready_cell()
        track_metadata = _track_motion_metadata(cell)
        self.assertTrue(track_metadata["motion_ready"])
        self.assertEqual(track_metadata["motion_status"], "ready_high")
        self.assertEqual(
            track_metadata["motion_reason_codes"],
            ["motion_candidates_agree"],
        )

        passage = forecast_target_passage(
            cell=cell,
            target_id="target.test",
            target_latitude=51.0,
            target_longitude=4.0,
            evaluated_at=1_000.0,
        )
        assert passage is not None
        passage_metadata = _passage_metadata(passage)
        self.assertEqual(passage_metadata["motion_status"], "ready_high")
        self.assertEqual(
            passage_metadata["motion_supporting_methods"],
            ["optical_flow", "object_track"],
        )


if __name__ == "__main__":
    unittest.main()
