"""Compact GeoJSON-contract voor kaartclients van Storm Tracker V3."""
from __future__ import annotations

import math
import time

from ..geometry.hull import convex_hull

MAX_HULL_POINTS = 48
MAX_SOURCE_RING_POINTS = 2048
MAX_RADAR_CELLS = 150
MAX_LIGHTNING_EVENTS = 300
LIGHTNING_MAX_AGE_S = 15 * 60
LIGHTNING_ZONE_MAX_AGE_S = 5 * 60
LIGHTNING_CLUSTER_DISTANCE_KM = 25.0
LIGHTNING_ZONE_BUFFER_KM = 12.0
LIGHTNING_RADAR_ASSOCIATION_KM = 25.0
CURRENT_FRAME_TOLERANCE_S = 60.0
MIN_MOTION_SAMPLES = 4
MIN_MOTION_HISTORY_MINUTES = 10.0
MIN_MOTION_FIT = 0.60


def _point(lon: float, lat: float) -> list[float]:
    return [round(float(lon), 5), round(float(lat), 5)]


def _feature(feature_id: str, geometry: dict, **properties) -> dict:
    return {
        "type": "Feature",
        "id": feature_id,
        "geometry": geometry,
        "properties": properties,
    }


def _sample_ring(
    points, *, order_as_hull: bool = False, max_points: int = MAX_HULL_POINTS
) -> list[list[float]]:
    values = list(points or [])
    if order_as_hull and len(values) >= 3:
        values = convex_hull(values)
    if len(values) > max_points:
        step = math.ceil(len(values) / max_points)
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


def _distance_km(a, b) -> float:
    lat1, lon1 = a
    lat2, lon2 = b
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    value = (
        math.sin(dlat / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    )
    return 6371.0088 * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))


def _lightning_clusters(points):
    """Groepeer nabije recente inslagen zonder verre systemen te verbinden."""
    remaining = list(points)
    clusters = []
    while remaining:
        cluster = [remaining.pop()]
        changed = True
        while changed:
            changed = False
            for point in list(remaining):
                if any(
                    _distance_km(point[:2], member[:2])
                    <= LIGHTNING_CLUSTER_DISTANCE_KM
                    for member in cluster
                ):
                    remaining.remove(point)
                    cluster.append(point)
                    changed = True
        clusters.append(cluster)
    return clusters


def _lightning_zone_ring(cluster):
    expanded = []
    for lat, lon, *_ in cluster:
        for heading in range(0, 360, 45):
            expanded.append(_destination(
                lat, lon, heading, LIGHTNING_ZONE_BUFFER_KM
            ))
    return _sample_ring(expanded, order_as_hull=True, max_points=64)


def _cell_source(cell) -> str | None:
    cell_id = str(getattr(cell, "cell_id", ""))
    if ":" not in cell_id:
        return None
    return cell_id.split(":", 1)[0]


def _cell_matches_active_source(cell, active_radar_source: str | None) -> bool:
    """Keep stale cells from an inactive radar provider out of the map feed."""
    if not active_radar_source:
        return True
    source = _cell_source(cell)
    return source is None or source == active_radar_source


def _storm_has_reliable_motion(storm) -> bool:
    """Publiceer alleen een vector die operationeel bruikbaar is."""
    return (
        storm.heading_deg is not None
        and storm.speed_kmh is not None
        and float(storm.speed_kmh) > 0.0
        and getattr(storm, "tracking_status", None) == "bevestigd"
        and getattr(storm, "confidence", "") in {"Matig", "Hoog"}
        and int(getattr(storm, "motion_sample_count", 0)) >= MIN_MOTION_SAMPLES
        and float(getattr(storm, "motion_history_minutes", 0.0))
        >= MIN_MOTION_HISTORY_MINUTES
        and float(getattr(storm, "motion_fit_quality", 0.0)) >= MIN_MOTION_FIT
    )


def build_feature_collection(
    targets: dict, regions: list, active_radar_source: str | None = None,
    radar_sources_by_engine: dict | None = None,
    lightning_events: list | None = None,
) -> dict:
    """Publiceer targets, regio's, systemen, hulls, cellen en vectoren compact."""
    features = []
    radar_cells_written = 0
    radar_cells_total = 0
    historical_radar_cells_excluded = 0

    radar_sources_by_engine = radar_sources_by_engine or {}
    lightning_events = lightning_events or []
    latest_by_region = {}
    radar_points_by_engine = {}
    for region in regions:
        region_source = (radar_sources_by_engine.get(region.engine_id) or {}).get("source", active_radar_source)
        timestamps = [
            float(cell.timestamp)
            for storm in region.storm_engine.get_storms()
            for cell in storm.radar_cells.values()
            if _cell_matches_active_source(cell, region_source)
        ]
        latest_by_region[region.engine_id] = max(timestamps, default=None)

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
            radar_source=(radar_sources_by_engine.get(target.get("region_engine_id")) or {}).get("source"),
            radar_source_reason=(radar_sources_by_engine.get(target.get("region_engine_id")) or {}).get("reason"),
            goes_rrqpe=(radar_sources_by_engine.get(target.get("region_engine_id")) or {}).get("goes_rrqpe"),
        ))

    for region in regions:
        region_decision = radar_sources_by_engine.get(region.engine_id) or {}
        region_source = region_decision.get("source", active_radar_source)
        features.append(_feature(
            f"region:{region.engine_id}",
            {"type": "Point", "coordinates": _point(region.center_lon, region.center_lat)},
            layer="region",
            engine_id=region.engine_id,
            radius_km=round(region.observation_radius_km, 1),
            targets=sorted(region.projection_targets),
            radar_source=region_source,
            radar_source_reason=region_decision.get("reason"),
            radar_age_seconds=region_decision.get("age_seconds"),
            goes_rrqpe=region_decision.get("goes_rrqpe"),
        ))
        for storm in region.storm_engine.get_storms():
            all_cells = [
                cell for cell in storm.radar_cells.values()
                if _cell_matches_active_source(cell, region_source)
            ]
            latest_timestamp = latest_by_region.get(region.engine_id)
            cells = sorted(
                (
                    cell for cell in all_cells
                    if latest_timestamp is None
                    or float(cell.timestamp) >= latest_timestamp - CURRENT_FRAME_TOLERANCE_S
                ),
                key=lambda cell: cell.timestamp,
                reverse=True,
            )
            historical_radar_cells_excluded += len(all_cells) - len(cells)
            radar_cells_total += len(cells)
            radar_points = radar_points_by_engine.setdefault(region.engine_id, [])
            for cell in cells:
                footprint = tuple(cell.footprint_points or ())
                radar_points.extend(footprint or ((cell.lat, cell.lon),))

            # Wanneer een operationele bron gekozen is, mag historische
            # StormEngine-state geen verweesde systeemvlakken of vectoren op
            # de actuele kaart achterlaten.
            if region_source and not cells:
                continue

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
            source_rings = []
            for cell in cells:
                footprint = tuple(cell.footprint_points or ())
                if len(footprint) < 4 or footprint[0] != footprint[-1]:
                    continue
                cell_ring = _sample_ring(
                    footprint, max_points=MAX_SOURCE_RING_POINTS
                )
                if len(cell_ring) >= 4:
                    source_rings.append([cell_ring])
            if source_rings:
                geometry = {
                    "type": "MultiPolygon",
                    "coordinates": source_rings,
                }
            else:
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

            if _storm_has_reliable_motion(storm):
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
                    motion_samples=storm.motion_sample_count,
                    motion_history_minutes=storm.motion_history_minutes,
                    motion_fit=storm.motion_fit_quality,
                ))

            for cell in cells:
                if radar_cells_written >= MAX_RADAR_CELLS:
                    break
                footprint = tuple(cell.footprint_points or ())
                is_closed_source_ring = (
                    len(footprint) >= 4 and footprint[0] == footprint[-1]
                )
                cell_ring = _sample_ring(
                    footprint,
                    order_as_hull=not is_closed_source_ring,
                    max_points=(
                        MAX_SOURCE_RING_POINTS
                        if is_closed_source_ring else MAX_HULL_POINTS
                    ),
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

    lightning_written = 0
    lightning_cutoff = time.time() - LIGHTNING_MAX_AGE_S
    valid_engine_ids = {region.engine_id for region in regions}
    recent_lightning = sorted(
        (
            item for item in lightning_events
            if float(item.get("timestamp", 0)) >= lightning_cutoff
        ),
        key=lambda item: float(item.get("timestamp", 0)),
        reverse=True,
    )[:MAX_LIGHTNING_EVENTS]
    lightning_by_engine = {}
    for index, event in enumerate(recent_lightning):
        engine_ids = [
            engine_id for engine_id in event.get("engine_ids", [])
            if engine_id in valid_engine_ids
        ]
        for engine_id in engine_ids:
            timestamp = float(event["timestamp"])
            lightning_by_engine.setdefault(engine_id, []).append((
                float(event["lat"]), float(event["lon"]), timestamp,
                event.get("source", "unknown"),
            ))
            features.append(_feature(
                f"lightning:{engine_id}:{timestamp:.3f}:{index}",
                {"type": "Point", "coordinates": _point(event["lon"], event["lat"])},
                layer="lightning",
                engine_id=engine_id,
                source=event.get("source", "unknown"),
                timestamp=timestamp,
                age_seconds=max(0, round(time.time() - timestamp)),
            ))
            lightning_written += 1

    lightning_zones_written = 0
    zone_cutoff = time.time() - LIGHTNING_ZONE_MAX_AGE_S
    for engine_id, points in lightning_by_engine.items():
        active = [point for point in points if point[2] >= zone_cutoff]
        for index, cluster in enumerate(_lightning_clusters(active)):
            ring = _lightning_zone_ring(cluster)
            if len(ring) < 4:
                continue
            radar_points = radar_points_by_engine.get(engine_id, [])
            radar_confirmed = any(
                _distance_km(strike[:2], radar) <= LIGHTNING_RADAR_ASSOCIATION_KM
                for strike in cluster for radar in radar_points
            )
            newest = max(point[2] for point in cluster)
            features.append(_feature(
                f"lightning-zone:{engine_id}:{newest:.0f}:{index}",
                {"type": "Polygon", "coordinates": [ring]},
                layer="lightning_zone",
                engine_id=engine_id,
                system_type=(
                    "radar_lightning" if radar_confirmed else "lightning_only"
                ),
                radar_confirmed=radar_confirmed,
                strike_count=len(cluster),
                newest_timestamp=newest,
                age_seconds=max(0, round(time.time() - newest)),
                buffer_km=LIGHTNING_ZONE_BUFFER_KM,
            ))
            lightning_zones_written += 1

    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "schema_version": 1,
            "feature_count": len(features),
            "radar_cells_total": radar_cells_total,
            "radar_cells_included": radar_cells_written,
            "truncated": radar_cells_written < radar_cells_total,
            "historical_radar_cells_excluded": historical_radar_cells_excluded,
            "lightning_events_included": lightning_written,
            "lightning_zones_included": lightning_zones_written,
            "lightning_max_age_minutes": LIGHTNING_MAX_AGE_S // 60,
        },
    }
