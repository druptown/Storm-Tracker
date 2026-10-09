from dataclasses import dataclass

from storm_tracker_worker.geojson import build_geojson, geojson_operational


@dataclass
class Job:
    payload: dict


def _payload():
    return {
        "built_at": 2_000.0,
        "targets": {
            "home": {
                "latitude": 51.0, "longitude": 4.0,
                "name": "Thuis", "primary": True,
            }
        },
        "regions": [{
            "engine_id": "region-1",
            "center_lat": 51.0, "center_lon": 4.0,
            "radius_km": 175.0, "targets": ["home"],
            "radar_source": "opera",
            "storms": [{
                "storm_id": "storm-1",
                "centroid_lat": 51.1, "centroid_lon": 4.1,
                "confidence": "Hoog",
                "heading_deg": 90.0, "speed_kmh": 30.0,
                "motion_reliable": True, "motion_samples": 4,
                "motion_history_minutes": 15.0, "motion_fit": 0.9,
                "cells": [{
                    "cell_id": "opera:cell-1",
                    "lat": 51.1, "lon": 4.1, "timestamp": 1_990.0,
                    "intensity": 4, "max_dbz": 40.0, "area_km2": 20.0,
                    "footprint": [],
                }],
            }],
        }],
        "lightning": [],
        "probes": [],
    }


def test_worker_builds_bounded_operational_geojson():
    result = build_geojson(_payload())
    layers = {
        feature["properties"]["layer"] for feature in result["features"]
    }

    assert result["type"] == "FeatureCollection"
    assert result["metadata"]["builder"] == "worker"
    assert {"target", "region", "storm", "motion", "radar_cell"} <= layers
    assert result["metadata"]["feature_count"] == len(result["features"])


def test_geojson_job_wraps_collection():
    result = geojson_operational(Job(_payload()))
    assert result["collection"]["metadata"]["builder"] == "worker"


def test_worker_draws_curved_accelerating_motion_path():
    payload = _payload()
    storm = payload["regions"][0]["storms"][0]
    storm.update({
        "motion_model": "constant_acceleration",
        "velocity_east_kmh": 30.0,
        "velocity_north_kmh": 0.0,
        "acceleration_east_kmh2": 0.0,
        "acceleration_north_kmh2": 40.0,
        "turn_rate_deg_per_hour": -53.0,
        "trajectory_curved": True,
    })

    result = build_geojson(payload)
    motion = next(
        feature for feature in result["features"]
        if feature["properties"]["layer"] == "motion"
    )

    assert len(motion["geometry"]["coordinates"]) == 7
    assert motion["properties"]["motion_model"] == "constant_acceleration"
    assert motion["properties"]["trajectory_curved"] is True
    assert motion["geometry"]["coordinates"][-1][1] > 51.1


def test_worker_builds_lightning_zone_with_radar_confirmation():
    payload = _payload()
    payload["lightning"] = [{
        "lat": 51.11, "lon": 4.11, "timestamp": 1_990.0,
        "source": "blitzortung", "engine_ids": ["region-1"],
    }]

    result = build_geojson(payload)
    zone = next(
        feature for feature in result["features"]
        if feature["properties"]["layer"] == "lightning_zone"
    )

    assert zone["properties"]["radar_confirmed"] is True
    assert result["metadata"]["lightning_events_included"] == 1
    assert result["metadata"]["lightning_zones_included"] == 1
