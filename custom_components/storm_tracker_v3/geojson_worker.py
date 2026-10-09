"""Serialize a bounded map snapshot and refresh worker-owned GeoJSON."""
from __future__ import annotations

import time

from .const import DOMAIN
from .engine.geojson import _cell_matches_active_source, _storm_has_reliable_motion
from .worker_bridge import async_request_worker_job


def build_worker_geojson_payload(data: dict) -> dict:
    manager = data.get("storm_manager")
    decisions = data.get("radar_sources_by_engine", {})
    regions = []
    for region in manager.get_all_engines() if manager else []:
        decision = decisions.get(region.engine_id) or {}
        source = decision.get("source", data.get("active_radar_source"))
        timestamps = [
            float(cell.timestamp)
            for storm in region.storm_engine.get_storms()
            for cell in storm.radar_cells.values()
            if _cell_matches_active_source(cell, source)
        ]
        latest = max(timestamps, default=None)
        storms = []
        for storm in region.storm_engine.get_storms():
            cells = [
                cell for cell in storm.radar_cells.values()
                if _cell_matches_active_source(cell, source)
                and (
                    latest is None
                    or float(cell.timestamp) >= latest - 60.0
                )
            ]
            storms.append({
                "storm_id": str(storm.storm_id),
                "centroid_lat": float(storm.centroid_lat),
                "centroid_lon": float(storm.centroid_lon),
                "system_type": getattr(storm, "system_type", "unknown"),
                "mcs_status": getattr(storm, "mcs_status", "not_evaluated"),
                "confidence": storm.confidence,
                "heading_deg": storm.heading_deg,
                "speed_kmh": storm.speed_kmh,
                "motion_model": getattr(storm, "motion_model", "linear"),
                "velocity_east_kmh": getattr(
                    storm, "velocity_east_kmh", None
                ),
                "velocity_north_kmh": getattr(
                    storm, "velocity_north_kmh", None
                ),
                "acceleration_east_kmh2": getattr(
                    storm, "acceleration_east_kmh2", 0.0
                ),
                "acceleration_north_kmh2": getattr(
                    storm, "acceleration_north_kmh2", 0.0
                ),
                "turn_rate_deg_per_hour": getattr(
                    storm, "turn_rate_deg_per_hour", 0.0
                ),
                "trajectory_curved": getattr(
                    storm, "trajectory_curved", False
                ),
                "radar_cells": len(storm.radar_cells),
                "motion_reliable": _storm_has_reliable_motion(storm),
                "motion_samples": storm.motion_sample_count,
                "motion_history_minutes": storm.motion_history_minutes,
                "motion_fit": storm.motion_fit_quality,
                "cells": [{
                    "cell_id": str(cell.cell_id),
                    "lat": float(cell.lat), "lon": float(cell.lon),
                    "timestamp": float(cell.timestamp),
                    "intensity": cell.intensity,
                    "max_dbz": cell.max_dbz,
                    "area_km2": cell.area_km2,
                    "footprint": [list(point) for point in (
                        cell.footprint_points or ()
                    )],
                } for cell in cells[:200]],
            })
        regions.append({
            "engine_id": str(region.engine_id),
            "center_lat": float(region.center_lat),
            "center_lon": float(region.center_lon),
            "radius_km": float(region.observation_radius_km),
            "targets": sorted(region.projection_targets),
            "radar_source": source,
            "radar_source_reason": decision.get("reason"),
            "radar_age_seconds": decision.get("age_seconds"),
            "storms": storms[:30],
        })
    cutoff = time.time() - 15 * 60
    return {
        "built_at": time.time(),
        "targets": data.get("targets", {}),
        "regions": regions,
        "lightning": [
            dict(item) for item in data.get("recent_lightning", [])
            if float(item.get("timestamp", 0)) >= cutoff
        ][:300],
        "probes": list(
            (data.get("virtual_probe_diagnostics") or {}).get("probes", [])
        )[:16],
    }


async def async_refresh_worker_geojson(hass) -> None:
    data = hass.data[DOMAIN]
    if data.get("geojson_worker_refreshing"):
        return
    data["geojson_worker_refreshing"] = True
    try:
        response = await async_request_worker_job(
            hass, "geojson_operational",
            build_worker_geojson_payload(data), timeout=20.0,
        )
        collection = response.get("collection")
        if (
            not isinstance(collection, dict)
            or collection.get("type") != "FeatureCollection"
            or not isinstance(collection.get("features"), list)
            or len(collection["features"]) > 1200
        ):
            raise ValueError("ongeldige worker-GeoJSON")
        data["geojson_worker_collection"] = collection
        data["geojson_worker_status"] = {
            "state": "operationeel", "mode": "worker_first",
            "fallback_active": False, "updated_at": time.time(),
            "feature_count": len(collection["features"]),
        }
        hass.bus.async_fire(f"{DOMAIN}_geojson_worker_update")
    except Exception as err:
        data["geojson_worker_status"] = {
            "state": "worker_onbeschikbaar", "mode": "worker_first",
            "source": "bewaarde_workerkaart",
            "fallback_active": False, "error": str(err),
        }
    finally:
        data["geojson_worker_refreshing"] = False
