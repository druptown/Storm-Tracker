"""Bounded GeoJSON assembly outside Home Assistant Core."""
from __future__ import annotations

import math

MAX_FEATURES = 1200
MAX_RADAR_CELLS = 150
LIGHTNING_ZONE_MAX_AGE_S = 5 * 60
LIGHTNING_CLUSTER_DISTANCE_KM = 25.0
LIGHTNING_ZONE_BUFFER_KM = 12.0
LIGHTNING_RADAR_ASSOCIATION_KM = 25.0


def _point(lon, lat):
    return [round(float(lon), 5), round(float(lat), 5)]


def _feature(feature_id, geometry, **properties):
    return {
        "type": "Feature", "id": feature_id,
        "geometry": geometry, "properties": properties,
    }


def _ring(points, limit=48):
    values = list(points or [])
    if len(values) > limit:
        values = values[::math.ceil(len(values) / limit)]
    result = [_point(point[1], point[0]) for point in values]
    if len(result) >= 3 and result[0] != result[-1]:
        result.append(result[0])
    return result


def _destination(lat, lon, heading, distance_km):
    angular = float(distance_km) / 6371.0088
    bearing = math.radians(float(heading))
    lat1, lon1 = math.radians(float(lat)), math.radians(float(lon))
    lat2 = math.asin(
        math.sin(lat1) * math.cos(angular)
        + math.cos(lat1) * math.sin(angular) * math.cos(bearing)
    )
    lon2 = lon1 + math.atan2(
        math.sin(bearing) * math.sin(angular) * math.cos(lat1),
        math.cos(angular) - math.sin(lat1) * math.sin(lat2),
    )
    return math.degrees(lat2), math.degrees(lon2)


def _distance_km(a, b):
    lat1, lon1 = a
    lat2, lon2 = b
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    value = (
        math.sin(dlat / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    )
    return 6371.0088 * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))


def _motion_path(storm, horizon_minutes=60, step_minutes=10):
    """Build the selected linear or accelerated trajectory for the map."""
    lat = float(storm["centroid_lat"])
    lon = float(storm["centroid_lon"])
    heading = float(storm["heading_deg"])
    speed = float(storm["speed_kmh"])
    velocity_east = storm.get("velocity_east_kmh")
    velocity_north = storm.get("velocity_north_kmh")
    if velocity_east is None:
        velocity_east = speed * math.sin(math.radians(heading))
    if velocity_north is None:
        velocity_north = speed * math.cos(math.radians(heading))
    acceleration_east = float(storm.get("acceleration_east_kmh2") or 0.0)
    acceleration_north = float(storm.get("acceleration_north_kmh2") or 0.0)
    result = []
    for minutes in range(0, horizon_minutes + 1, step_minutes):
        hours = minutes / 60.0
        east = (
            float(velocity_east) * hours
            + 0.5 * acceleration_east * hours * hours
        )
        north = (
            float(velocity_north) * hours
            + 0.5 * acceleration_north * hours * hours
        )
        predicted_lat = lat + north / 110.574
        predicted_lon = lon + east / (
            111.32 * max(0.05, math.cos(math.radians(lat)))
        )
        result.append(_point(predicted_lon, predicted_lat))
    return result


def _lightning_clusters(points):
    values = list(points)
    parent = list(range(len(values)))

    def find(index):
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    for left in range(len(values)):
        for right in range(left + 1, len(values)):
            if (
                _distance_km(values[left][:2], values[right][:2])
                > LIGHTNING_CLUSTER_DISTANCE_KM
            ):
                continue
            left_root, right_root = find(left), find(right)
            if left_root != right_root:
                parent[right_root] = left_root
    grouped = {}
    for index, point in enumerate(values):
        grouped.setdefault(find(index), []).append(point)
    return list(grouped.values())


def _convex_hull(points):
    values = sorted(set(points))
    if len(values) <= 1:
        return values

    def cross(origin, left, right):
        return (
            (left[0] - origin[0]) * (right[1] - origin[1])
            - (left[1] - origin[1]) * (right[0] - origin[0])
        )

    lower = []
    for point in values:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], point) <= 0:
            lower.pop()
        lower.append(point)
    upper = []
    for point in reversed(values):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], point) <= 0:
            upper.pop()
        upper.append(point)
    return lower[:-1] + upper[:-1]


def _lightning_zone_ring(cluster):
    expanded = []
    for lat, lon, *_ in cluster:
        for heading in range(0, 360, 45):
            expanded.append(_destination(
                lat, lon, heading, LIGHTNING_ZONE_BUFFER_KM
            ))
    return _ring(_convex_hull(expanded), limit=64)


def build_geojson(payload: dict) -> dict:
    """Build only from a bounded immutable snapshot supplied by HA."""
    targets = payload.get("targets", {})
    regions = payload.get("regions", [])
    lightning = payload.get("lightning", [])
    probes = payload.get("probes", [])
    if (
        not isinstance(targets, dict) or len(targets) > 32
        or not isinstance(regions, list) or len(regions) > 8
        or not isinstance(lightning, list) or len(lightning) > 300
        or not isinstance(probes, list) or len(probes) > 16
    ):
        raise ValueError("ongeldige of te grote GeoJSON-snapshot")

    features = []
    cell_count = 0
    total_cells = 0
    storm_positions = {}
    radar_points_by_engine = {}
    for target_id, target in sorted(targets.items()):
        if target.get("latitude") is None or target.get("longitude") is None:
            continue
        features.append(_feature(
            f"target:{target_id}",
            {"type": "Point", "coordinates": _point(
                target["longitude"], target["latitude"]
            )},
            layer="target", target_id=target_id,
            name=target.get("name", target_id),
            entity_id=target.get("entity_id"),
            primary=bool(target.get("primary")),
            available=bool(target.get("available")),
            radar_covered=bool(target.get("radar_covered")),
            region_engine=target.get("region_engine_id"),
        ))

    for region in regions:
        engine_id = str(region["engine_id"])
        features.append(_feature(
            f"region:{engine_id}",
            {"type": "Point", "coordinates": _point(
                region["center_lon"], region["center_lat"]
            )},
            layer="region", engine_id=engine_id,
            radius_km=round(float(region["radius_km"]), 1),
            targets=sorted(region.get("targets", [])),
            radar_source=region.get("radar_source"),
            radar_source_reason=region.get("radar_source_reason"),
            radar_age_seconds=region.get("radar_age_seconds"),
        ))
        for storm in region.get("storms", []):
            storm_id = str(storm["storm_id"])
            key = f"{engine_id}:{storm_id}"
            cells = list(storm.get("cells", []))
            total_cells += len(cells)
            if region.get("radar_source") and not cells:
                continue
            storm_positions[(engine_id, storm_id)] = (
                storm["centroid_lat"], storm["centroid_lon"]
            )
            source_rings = []
            for cell in cells:
                ring = _ring(cell.get("footprint"), limit=2048)
                if len(ring) >= 4:
                    source_rings.append([ring])
            geometry = (
                {"type": "MultiPolygon", "coordinates": source_rings}
                if source_rings else
                {"type": "Point", "coordinates": _point(
                    storm["centroid_lon"], storm["centroid_lat"]
                )}
            )
            features.append(_feature(
                f"storm:{key}", geometry, layer="storm",
                engine_id=engine_id, storm_id=storm_id,
                system_type=storm.get("system_type", "unknown"),
                mcs_status=storm.get("mcs_status", "not_evaluated"),
                confidence=storm.get("confidence"),
                heading_deg=storm.get("heading_deg"),
                speed_kmh=storm.get("speed_kmh"),
                radar_cells=storm.get("radar_cells", len(cells)),
            ))
            if storm.get("motion_reliable"):
                path = _motion_path(storm)
                features.append(_feature(
                    f"motion:{key}",
                    {"type": "LineString", "coordinates": path},
                    layer="motion", engine_id=engine_id, storm_id=storm_id,
                    minutes=60, heading_deg=storm.get("heading_deg"),
                    speed_kmh=storm.get("speed_kmh"),
                    motion_model=storm.get("motion_model", "linear"),
                    turn_rate_deg_per_hour=storm.get(
                        "turn_rate_deg_per_hour", 0.0
                    ),
                    trajectory_curved=bool(
                        storm.get("trajectory_curved", False)
                    ),
                    motion_samples=storm.get("motion_samples"),
                    motion_history_minutes=storm.get("motion_history_minutes"),
                    motion_fit=storm.get("motion_fit"),
                ))
            for cell in cells:
                if cell_count >= MAX_RADAR_CELLS:
                    break
                radar_points_by_engine.setdefault(engine_id, []).append((
                    float(cell["lat"]), float(cell["lon"])
                ))
                ring = _ring(cell.get("footprint"), limit=2048)
                geometry = (
                    {"type": "Polygon", "coordinates": [ring]}
                    if len(ring) >= 4 else
                    {"type": "Point", "coordinates": _point(
                        cell["lon"], cell["lat"]
                    )}
                )
                features.append(_feature(
                    f"cell:{key}:{cell['cell_id']}", geometry,
                    layer="radar_cell", engine_id=engine_id,
                    storm_id=storm_id, intensity=cell.get("intensity"),
                    max_dbz=cell.get("max_dbz"),
                    area_km2=cell.get("area_km2"),
                ))
                cell_count += 1

    for index, probe in enumerate(probes):
        if probe.get("latitude") is None or probe.get("longitude") is None:
            continue
        probe_id = str(probe.get("probe_id", index))
        common = {
            "engine_id": probe.get("region_id"), "probe_id": probe_id,
            "storm_id": probe.get("storm_id"),
            "station_id": probe.get("station_id"),
            "calibration_only": True, "status": "actief",
            "status_text": probe.get("status_text"),
            "summary": probe.get("summary"),
            "eta_minutes": probe.get("minutes_until_expected"),
            "expected_at": probe.get("expected_at"),
            "passage": probe.get("passage"),
            "pass_distance_km": probe.get("pass_distance_km"),
            "confidence": probe.get("motion_confidence"),
        }
        features.append(_feature(
            f"virtual-probe:{probe_id}",
            {"type": "Point", "coordinates": _point(
                probe["longitude"], probe["latitude"]
            )},
            layer="virtual_probe", **common,
        ))
        start = storm_positions.get((
            str(probe.get("region_id")), str(probe.get("storm_id"))
        ))
        if start:
            features.append(_feature(
                f"virtual-probe-path:{probe_id}",
                {"type": "LineString", "coordinates": [
                    _point(start[1], start[0]),
                    _point(probe["longitude"], probe["latitude"]),
                ]},
                layer="virtual_probe_path", **common,
            ))

    lightning_written = 0
    lightning_by_engine = {}
    valid_engine_ids = {str(region["engine_id"]) for region in regions}
    for index, event in enumerate(lightning):
        for engine_id in event.get("engine_ids", []):
            engine_id = str(engine_id)
            if engine_id not in valid_engine_ids:
                continue
            timestamp = float(event["timestamp"])
            lightning_by_engine.setdefault(engine_id, []).append((
                float(event["lat"]), float(event["lon"]), timestamp,
                event.get("source", "unknown"),
            ))
            features.append(_feature(
                f"lightning:{engine_id}:{timestamp:.3f}:{index}",
                {"type": "Point", "coordinates": _point(
                    event["lon"], event["lat"]
                )},
                layer="lightning", engine_id=engine_id,
                source=event.get("source", "unknown"),
                timestamp=timestamp,
                age_seconds=max(
                    0, round(float(payload["built_at"]) - timestamp)
                ),
            ))
            lightning_written += 1

    lightning_zones_written = 0
    zone_cutoff = float(payload["built_at"]) - LIGHTNING_ZONE_MAX_AGE_S
    for engine_id, points in lightning_by_engine.items():
        active = [point for point in points if point[2] >= zone_cutoff]
        for index, cluster in enumerate(_lightning_clusters(active)):
            ring = _lightning_zone_ring(cluster)
            if len(ring) < 4:
                continue
            radar_points = radar_points_by_engine.get(engine_id, [])
            radar_confirmed = any(
                _distance_km(strike[:2], radar)
                <= LIGHTNING_RADAR_ASSOCIATION_KM
                for strike in cluster for radar in radar_points
            )
            newest = max(point[2] for point in cluster)
            features.append(_feature(
                f"lightning-zone:{engine_id}:{newest:.0f}:{index}",
                {"type": "Polygon", "coordinates": [ring]},
                layer="lightning_zone", engine_id=engine_id,
                system_type=(
                    "radar_lightning" if radar_confirmed else "lightning_only"
                ),
                radar_confirmed=radar_confirmed,
                strike_count=len(cluster),
                newest_timestamp=newest,
                age_seconds=max(
                    0, round(float(payload["built_at"]) - newest)
                ),
                buffer_km=LIGHTNING_ZONE_BUFFER_KM,
            ))
            lightning_zones_written += 1
    if len(features) > MAX_FEATURES:
        raise ValueError("GeoJSON featurelimiet overschreden")
    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "schema_version": 1,
            "feature_count": len(features),
            "radar_cells_total": total_cells,
            "radar_cells_included": cell_count,
            "truncated": cell_count < total_cells,
            "historical_radar_cells_excluded": 0,
            "lightning_events_included": lightning_written,
            "lightning_zones_included": lightning_zones_written,
            "lightning_max_age_minutes": 15,
            "virtual_probes_included": len(probes),
            "virtual_probe_outcomes_included": 0,
            "builder": "worker",
        },
    }


def geojson_operational(job) -> dict:
    return {"collection": build_geojson(job.payload)}
