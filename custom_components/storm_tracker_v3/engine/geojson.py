"""Compact GeoJSON-contract voor kaartclients van Storm Tracker V3."""
from __future__ import annotations

import math

from ..geometry.hull import convex_hull

MAX_HULL_POINTS = 48
MAX_RADAR_CELLS = 150


def _point(lon: float, lat: float) -> list[float]:
    return [round(float(lon), 5), round(float(lat), 5)]


def _feature(feature_id: str, geometry: dict, **properties) -> dict:
    return {
        "type": "Feature",
        "id": feature_id,
        "geometry": geometry,
        "properties": properties,
    }


def _sample_ring(points, *, order_as_hull: bool = False) -> list[list[float]]:
    values = list(points or [])
    if order_as_hull and len(values) >= 3:
        values = convex_hull(values)
    if len(values) > MAX_HULL_POINTS:
        step = math.ceil(len(values) / MAX_HULL_POINTS)
        values = values[::step]
    ring = [_point(lon, lat) for lat, lon in values]
    if len(ring) >= 3 and ring[0] != ring[-1]:
        ring.append(ring[0])
    return ring


def _destination(lat: float, lon: float, heading: float, distance_km: float):
    radius = 6371.0088
    angular = distance_km / radius
    bearing = math.radians(heading)
    lat1 = math.radians(lat)
    lon1 = math.radians(lon)
    lat2 = math.asin(
        math.sin(lat1) * math.cos(angular)
        + math.cos(lat1) * math.sin(angular) * math.cos(bearing)
    )
    lon2 = lon1 + math.atan2(
        math.sin(bearing) * math.sin(angular) * math.cos(lat1),
        math.cos(angular) - math.sin(lat1) * math.sin(lat2),
    )
    return math.degrees(lat2), math.degrees(lon2)


def build_feature_collection(targets: dict, regions: list) -> dict:
    """Publiceer targets, regio's, systemen, hulls, cellen en vectoren compact."""
    features = []
    radar_cells_written = 0
    radar_cells_total = 0

    for target_id, target in sorted(targets.items()):
        lat = target.get("latitude")
        lon = target.get("longitude")
        if lat is None or lon is None:
            continue
        features.append(_feature(
            f"target:{target_id}",
            {"type": "Point", "coordinates": _point(lon, lat)},
            layer="target",
            target_id=target_id,
            name=target.get("name", target_id),
            entity_id=target.get("entity_id"),
            primary=bool(target.get("primary")),
            available=bool(target.get("available")),
            radar_covered=bool(target.get("radar_covered")),
            region_engine=target.get("region_engine_id"),
        ))

    for region in regions:
        features.append(_feature(
            f"region:{region.engine_id}",
            {"type": "Point", "coordinates": _point(region.center_lon, region.center_lat)},
            layer="region",
            engine_id=region.engine_id,
            radius_km=round(region.observation_radius_km, 1),
            targets=sorted(region.projection_targets),
        ))
        for storm in region.storm_engine.get_storms():
            storm_id = f"{region.engine_id}:{storm.storm_id}"
            common = {
                "engine_id": region.engine_id,
                "storm_id": storm.storm_id,
                "system_type": getattr(storm, "system_type", "unknown"),
                "mcs_status": getattr(storm, "mcs_status", "not_evaluated"),
                "confidence": storm.confidence,
                "heading_deg": storm.heading_deg,
                "speed_kmh": storm.speed_kmh,
                "radar_cells": len(storm.radar_cells),
            }
            ring = _sample_ring(storm.hull)
            geometry = (
                {"type": "Polygon", "coordinates": [ring]}
                if len(ring) >= 4
                else {"type": "Point", "coordinates": _point(
                    storm.centroid_lon, storm.centroid_lat
                )}
            )
            features.append(_feature(
                f"storm:{storm_id}", geometry, layer="storm", **common
            ))

            if storm.heading_deg is not None and storm.speed_kmh is not None:
                end_lat, end_lon = _destination(
                    storm.centroid_lat,
                    storm.centroid_lon,
                    storm.heading_deg,
                    max(0.0, storm.speed_kmh),
                )
                features.append(_feature(
                    f"motion:{storm_id}",
                    {"type": "LineString", "coordinates": [
                        _point(storm.centroid_lon, storm.centroid_lat),
                        _point(end_lon, end_lat),
                    ]},
                    layer="motion",
                    engine_id=region.engine_id,
                    storm_id=storm.storm_id,
                    minutes=60,
                    heading_deg=storm.heading_deg,
                    speed_kmh=storm.speed_kmh,
                ))

            cells = sorted(
                storm.radar_cells.values(), key=lambda cell: cell.timestamp, reverse=True
            )
            radar_cells_total += len(cells)
            for cell in cells:
                if radar_cells_written >= MAX_RADAR_CELLS:
                    break
                # OPERA-footprints zijn puntwolken/scanlijnen, geen gegarandeerd
                # geordende polygonring. Eerst hullen voorkomt zigzagdiagonalen.
                cell_ring = _sample_ring(
                    cell.footprint_points, order_as_hull=True
                )
                cell_geometry = (
                    {"type": "Polygon", "coordinates": [cell_ring]}
                    if len(cell_ring) >= 4
                    else {"type": "Point", "coordinates": _point(cell.lon, cell.lat)}
                )
                features.append(_feature(
                    f"cell:{storm_id}:{cell.cell_id}",
                    cell_geometry,
                    layer="radar_cell",
                    engine_id=region.engine_id,
                    storm_id=storm.storm_id,
                    intensity=cell.intensity,
                    max_dbz=cell.max_dbz,
                    area_km2=cell.area_km2,
                ))
                radar_cells_written += 1

    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "schema_version": 1,
            "feature_count": len(features),
            "radar_cells_total": radar_cells_total,
            "radar_cells_included": radar_cells_written,
            "truncated": radar_cells_written < radar_cells_total,
        },
    }
