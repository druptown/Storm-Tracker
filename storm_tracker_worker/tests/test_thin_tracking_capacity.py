from __future__ import annotations

import unittest

from storm_tracker_worker.thin_radar_tracking import _thin_tracking_policy


class ThinTrackingCapacityTests(unittest.TestCase):
    def test_capacity_can_retain_tracks_for_the_configured_gap_window(self) -> None:
        policy = _thin_tracking_policy()
        # Live fused regional fields can legitimately contain more than 128
        # hierarchical rain-shield/core objects. Keep bounded headroom instead
        # of rejecting the entire meteorological frame at 129 objects.
        self.assertGreaterEqual(policy.maximum_detected_objects, 192)
        required_active_tracks = (
            policy.maximum_detected_objects
            * (policy.lifecycle.maximum_gap_frames + 1)
        )

        self.assertGreaterEqual(
            policy.maximum_active_tracks,
            required_active_tracks,
        )
        self.assertGreaterEqual(
            policy.maximum_candidate_edges,
            required_active_tracks * policy.maximum_detected_objects,
        )


if __name__ == "__main__":
    unittest.main()
