from __future__ import annotations

from types import SimpleNamespace
import hashlib
import math

import numpy as np
from pyproj import CRS, Transformer

from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker import opera_native as native
from storm_tracker_worker.worker_only_radar_processing import (
    _opera_echo_observations_from_artifact,
)


NOW = 1_800_200_000.0


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("ascii")).hexdigest()


def _desired():
    return parse_desired_state({
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha:test",
        "authority_epoch": 1,
        "generation": 1,
        "issued_at": NOW,
        "previous_state_sha256": None,
        "targets": [{
            "target_id": "home",
            "display_name": "Thuis",
            "kind": "home",
            "available": True,
            "latitude": 50.8,
            "longitude": 4.2,
            "position_observed_at": NOW,
            "location_accuracy_m": 10.0,
            "observation_radius_km": 250.0,
            "country_code": "BE",
        }],
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
    })


def test_opera_echo_sampling_preserves_dbz_without_inventing_rain_rate():
    desired = _desired()
    target = desired.targets[0]
    transformer = Transformer.from_crs(
        CRS.from_epsg(4326),
        CRS.from_user_input(native.OPERA_PROJDEF),
        always_xy=True,
    )
    x_metres, y_metres = transformer.transform(target.longitude, target.latitude)
    global_row = math.floor(-y_metres / native.OPERA_GRID_SCALE_METRES)
    global_column = math.floor(x_metres / native.OPERA_GRID_SCALE_METRES)
    shape = (3, 3)
    detected_dbz = np.zeros(shape, dtype=np.float64)
    detected = np.zeros(shape, dtype=np.uint8)
    coverage = np.ones(shape, dtype=np.uint8)
    qi = np.zeros(shape, dtype=np.float64)
    qi_valid = np.zeros(shape, dtype=np.uint8)
    detected_dbz[1, 1] = 27.5
    detected[1, 1] = 1
    qi[1, 1] = 0.8
    qi_valid[1, 1] = 1
    artifact = SimpleNamespace(
        metadata={
            "grid": {
                "row_start": global_row - 1,
                "column_start": global_column - 1,
            },
            "capture": {"product_timestamp": NOW - 60.0},
            "product": {"nominal_timestamp": NOW - 60.0},
        },
        detected_dbz=detected_dbz,
        dbzh_detected_mask=detected,
        coverage_mask=coverage,
        qi_raw=qi,
        qi_valid_mask=qi_valid,
    )

    observations = _opera_echo_observations_from_artifact(
        desired,
        artifact,
        received_at=NOW - 30.0,
        source_receipt_sha256=_sha("source"),
        certificate_sha256=_sha("certificate"),
        process_receipt_sha256=_sha("process"),
    )

    assert len(observations) == 1
    observation = observations[0]
    assert observation.source_id == "radar.opera.dbzh"
    assert observation.target_id == "home"
    assert observation.reflectivity_dbz == 27.5
    assert observation.quality_index == 0.8
    assert not hasattr(observation, "rain_rate_mm_h")
