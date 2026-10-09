"""Worker-owned radar calibration preparation and atomic SQLite staging.

Home Assistant submits only immutable provider-artifact handles plus compact
region metadata.  All rasterisation, footprint walking, provider comparison,
bias calculation, JSON encoding and zlib compression happen here.
"""
from __future__ import annotations

from itertools import combinations
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import zlib

from .artifacts import ARTIFACT_ROOT, read_json_artifact

CAPTURE_CONTRACT = "stv3-calibration-capture-v2"
STAGE_SCHEMA_VERSION = 1
ALGORITHM_VERSION = "worker-radar-calibration-v2"
MAX_CAPTURES = 256
MAX_OBSERVATIONS_PER_ARTIFACT = 10_000
MAX_POINTS_PER_CAPTURE = 250_000
MAX_OVERLAY_RUNS_PER_ARTIFACT = 25_000
MAX_OVERLAY_VERTICES_PER_RUN = 64
MAX_RASTER_CANDIDATES_PER_CAPTURE = 1_000_000
MAX_UNIQUE_ARTIFACTS = 32
MAX_TOTAL_DECLARED_ARTIFACT_BYTES = 256 * 1024 * 1024
MAX_TOTAL_STAGE_POINTS = 500_000
MAX_COMPARISON_PAIRS = 512
MAX_COMPARISON_POINT_OPERATIONS = 2_000_000
MAX_STAGE_BYTES = 64 * 1024 * 1024
STAGE_RETENTION_SECONDS = 2 * 60 * 60
MIN_SHARED_COVERAGE_FRACTION = 0.60
ARTIFACT_CONTENT_TYPE = "application/json+gzip"


def _finite(value, name: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} is niet eindig")
    return result


def _point(value) -> tuple[float, float]:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ValueError("kalibratieartefact bevat ongeldige geometrie")
    lat, lon = _finite(value[0], "latitude"), _finite(value[1], "longitude")
    if not -90.0 <= lat <= 90.0 or not -180.0 <= lon <= 180.0:
        raise ValueError("kalibratieartefact bevat ongeldige coördinaat")
    return lat, lon


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    value = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2)
        * math.sin(delta_lambda / 2.0) ** 2
    )
    return 6371.0088 * 2.0 * math.atan2(
        math.sqrt(value), math.sqrt(max(0.0, 1.0 - value))
    )


def _evaluation_bbox(center_lat: float, center_lon: float, radius_km: float):
    lat_delta = radius_km / 110.574
    lon_delta = radius_km / (
        111.320 * max(0.1, abs(math.cos(math.radians(center_lat))))
    )
    return (
        center_lon - lon_delta,
        center_lat - lat_delta,
        center_lon + lon_delta,
        center_lat + lat_delta,
    )


def _intersection(first, second):
    if first is None or second is None:
        return first or second
    value = (
        max(first[0], second[0]),
        max(first[1], second[1]),
        min(first[2], second[2]),
        min(first[3], second[3]),
    )
    return value if value[0] < value[2] and value[1] < value[3] else None


def _bbox_fraction(coverage, evaluation) -> float:
    if coverage is None or evaluation is None:
        return 1.0
    overlap = _intersection(coverage, evaluation)
    if overlap is None:
        return 0.0
    denominator = max(
        1e-12,
        (evaluation[2] - evaluation[0]) * (evaluation[3] - evaluation[1]),
    )
    return min(
        1.0,
        max(
            0.0,
            ((overlap[2] - overlap[0]) * (overlap[3] - overlap[1]))
            / denominator,
        ),
    )


def _inside_bbox(lat: float, lon: float, bbox) -> bool:
    return (
        bbox is None
        or bbox[0] <= lon <= bbox[2] and bbox[1] <= lat <= bbox[3]
    )


def _clip_polygon_axis(points, *, axis: int, boundary: float, keep_above: bool):
    """Clip one lat/lon polygon against one axis-aligned half-plane."""
    if not points:
        return []

    def inside(point):
        return point[axis] >= boundary if keep_above else point[axis] <= boundary

    def intersection(first, second):
        delta = second[axis] - first[axis]
        ratio = 0.0 if abs(delta) < 1e-15 else (boundary - first[axis]) / delta
        other = 1 - axis
        value = [0.0, 0.0]
        value[axis] = boundary
        value[other] = first[other] + ratio * (second[other] - first[other])
        return tuple(value)

    output = []
    previous = points[-1]
    previous_inside = inside(previous)
    for current in points:
        current_inside = inside(current)
        if current_inside:
            if not previous_inside:
                output.append(intersection(previous, current))
            output.append(current)
        elif previous_inside:
            output.append(intersection(previous, current))
        previous, previous_inside = current, current_inside
    return output


def _polygon_cell_overlap_area(
    ring, *, lat_min: float, lon_min: float, lat_max: float, lon_max: float
) -> float:
    """Return positive degree-squared overlap with one analysis-grid cell."""
    clipped = list(ring)
    for axis, boundary, keep_above in (
        (0, lat_min, True), (0, lat_max, False),
        (1, lon_min, True), (1, lon_max, False),
    ):
        clipped = _clip_polygon_axis(
            clipped, axis=axis, boundary=boundary, keep_above=keep_above
        )
        if len(clipped) < 3:
            return 0.0
    area = 0.0
    previous = clipped[-1]
    for current in clipped:
        area += previous[1] * current[0] - current[1] * previous[0]
        previous = current
    return abs(area) * 0.5


def _rasterize_overlay(overlay: dict, descriptor: dict) -> dict:
    """Rasterise filled native precipitation runs onto the analysis grid."""
    runs = overlay.get("runs")
    if not isinstance(runs, list) or len(runs) > MAX_OVERLAY_RUNS_PER_ARTIFACT:
        raise ValueError("provideroverlay overschrijdt runbudget")
    grid_deg = descriptor["grid_deg"]
    coverage = descriptor["coverage_bbox"]
    center_lon = descriptor["center_lon"]
    points: dict[tuple[int, int], list] = {}
    candidates = 0
    for run in runs:
        if not isinstance(run, dict):
            raise ValueError("provideroverlay bevat ongeldige run")
        intensity = int(run.get("intensity") or 0)
        if intensity < descriptor["minimum_intensity"]:
            continue
        raw_ring = run.get("ring")
        if (
            not isinstance(raw_ring, (list, tuple))
            or not 3 <= len(raw_ring) <= MAX_OVERLAY_VERTICES_PER_RUN
        ):
            raise ValueError("provideroverlay bevat ongeldige rungeometrie")
        ring = []
        for raw_point in raw_ring:
            lat, lon = _point(raw_point)
            # Unwrap around the target so dateline-adjacent regional products
            # remain one small polygon instead of a nearly global bbox.
            lon = center_lon + ((lon - center_lon + 180.0) % 360.0) - 180.0
            ring.append((lat, lon))
        lat_start = math.floor(min(point[0] for point in ring) / grid_deg)
        lat_stop = math.floor(max(point[0] for point in ring) / grid_deg)
        lon_start = math.floor(min(point[1] for point in ring) / grid_deg)
        lon_stop = math.floor(max(point[1] for point in ring) / grid_deg)
        for grid_lat in range(lat_start, lat_stop + 1):
            lat_min = grid_lat * grid_deg
            lat_max = lat_min + grid_deg
            center_lat = lat_min + grid_deg / 2.0
            for grid_lon in range(lon_start, lon_stop + 1):
                candidates += 1
                if candidates > MAX_RASTER_CANDIDATES_PER_CAPTURE:
                    raise ValueError("kalibratiecapture overschrijdt rasterbudget")
                lon_min = grid_lon * grid_deg
                lon_max = lon_min + grid_deg
                cell_lon = lon_min + grid_deg / 2.0
                if not _inside_bbox(center_lat, cell_lon, coverage):
                    continue
                if _haversine_km(
                    descriptor["center_lat"], center_lon,
                    center_lat, cell_lon,
                ) > descriptor["radius_km"]:
                    continue
                if _polygon_cell_overlap_area(
                    ring,
                    lat_min=lat_min,
                    lon_min=lon_min,
                    lat_max=lat_max,
                    lon_max=lon_max,
                ) <= 1e-12:
                    continue
                current = points.setdefault((grid_lat, grid_lon), [None, None, 0])
                current[0] = max(
                    float(intensity),
                    current[0] if current[0] is not None else float("-inf"),
                )
                current[2] += 1
                if len(points) > MAX_POINTS_PER_CAPTURE:
                    raise ValueError("kalibratiecapture overschrijdt puntbudget")
    return points


def _scope(region_key: str) -> str:
    return region_key.split("@", 1)[1] if "@" in region_key else region_key


def _descriptor(value: dict) -> dict:
    if not isinstance(value, dict) or value.get("contract") != CAPTURE_CONTRACT:
        raise ValueError("ongeldig kalibratiecapturecontract")
    capture_id = str(value.get("capture_id") or "")
    canonical_value = dict(value)
    canonical_value.pop("capture_id", None)
    canonical = json.dumps(
        canonical_value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if (
        len(capture_id) != 64
        or hashlib.sha256(canonical).hexdigest() != capture_id
    ):
        raise ValueError("kalibratiecapture-ID klopt niet")
    source = str(value.get("source") or "")
    region_key = str(value.get("region_key") or "")
    region_id = str(value.get("region_id") or "")
    if not source or len(source) > 128 or not region_key or not region_id:
        raise ValueError("kalibratiecapture mist bron of regio")
    timestamp = _finite(value.get("product_timestamp"), "product_timestamp")
    grid_deg = _finite(value.get("grid_deg"), "grid_deg")
    minimum_intensity = int(value.get("minimum_intensity", 1))
    evaluation = value.get("evaluation")
    if not isinstance(evaluation, dict):
        raise ValueError("kalibratiecapture mist evaluatiegebied")
    center_lat = _finite(evaluation.get("center_lat"), "center_lat")
    center_lon = _finite(evaluation.get("center_lon"), "center_lon")
    radius_km = _finite(evaluation.get("radius_km"), "radius_km")
    coverage = tuple(
        _finite(item, "coverage_bbox")
        for item in value.get("coverage_bbox", ())
    )
    artifact = value.get("artifact")
    if (
        not -90 <= center_lat <= 90
        or not -180 <= center_lon <= 180
        or not 0 < radius_km <= 2_000
        or not 0 < grid_deg <= 1.0
        or not 0 <= minimum_intensity <= 255
        or len(coverage) != 4
        or not -180.0 <= coverage[0] <= 180.0
        or not -90.0 <= coverage[1] <= 90.0
        or not -180.0 <= coverage[2] <= 180.0
        or not -90.0 <= coverage[3] <= 90.0
        or coverage[0] >= coverage[2]
        or coverage[1] >= coverage[3]
        or value.get("fetch_state") != "success"
        or not isinstance(artifact, dict)
    ):
        raise ValueError("kalibratiecapture bevat ongeldige grenzen")
    artifact_timestamp = _finite(
        artifact.get("frame_timestamp"), "artifact.frame_timestamp"
    )
    try:
        artifact_bytes = int(artifact.get("artifact_bytes", -1))
    except (TypeError, ValueError) as err:
        raise ValueError("kalibratiecapture heeft ongeldige artifactgrootte") from err
    artifact_sha = str(artifact.get("artifact_sha256") or "")
    artifact_path = artifact.get("artifact_path")
    if (
        not isinstance(artifact_path, str)
        or not artifact_path
        or len(artifact_sha) != 64
        or any(character not in "0123456789abcdef" for character in artifact_sha)
        or artifact_bytes <= 0
        or artifact.get("content_type") != ARTIFACT_CONTENT_TYPE
        or abs(artifact_timestamp - timestamp) > 1.0
    ):
        raise ValueError("kalibratiecapture heeft ongeldige artifactmetadata")
    return {
        **value,
        "source": source,
        "region_key": region_key,
        "region_id": region_id,
        "product_timestamp": timestamp,
        "grid_deg": grid_deg,
        "minimum_intensity": minimum_intensity,
        "center_lat": center_lat,
        "center_lon": center_lon,
        "radius_km": radius_km,
        "artifact_frame_timestamp": artifact_timestamp,
        "coverage_bbox": coverage,
        "evaluation_bbox": _evaluation_bbox(
            center_lat, center_lon, radius_km
        ),
    }


def _goes_satellite(center_lon: float) -> str | None:
    longitude = ((center_lon + 180.0) % 360.0) - 180.0
    if -105.0 <= longitude <= -15.0:
        return "19"
    if longitude < -105.0 or longitude >= 155.0:
        return "18"
    return None


def _overlay(payload: dict, descriptor: dict) -> dict:
    overlay = payload.get("overlay")
    if descriptor["source"] == "noaa_goes_rrqpe":
        satellite = _goes_satellite(descriptor["center_lon"])
        overlays = payload.get("overlays_by_satellite")
        overlay = overlays.get(satellite) if isinstance(overlays, dict) else None
    if (
        not isinstance(overlay, dict)
        or overlay.get("source") != descriptor["source"]
        or not isinstance(overlay.get("runs"), list)
    ):
        raise ValueError("providerartefact mist de exacte regionale overlay")
    timestamp = _finite(overlay.get("timestamp"), "overlay_timestamp")
    return {**overlay, "timestamp": timestamp}


def _opera_intensity(max_dbz: float) -> int:
    """Use the native OPERA DBZH category boundaries without losing drizzle."""
    if max_dbz < 8.0:
        return 0
    if max_dbz < 15.0:
        return 1
    if max_dbz < 20.0:
        return 2
    if max_dbz < 25.0:
        return 3
    if max_dbz < 30.0:
        return 4
    if max_dbz < 35.0:
        return 5
    if max_dbz < 40.0:
        return 6
    if max_dbz < 50.0:
        return 7
    return 8


def _observations(payload: dict, source: str) -> list[dict]:
    raw = payload.get("observations")
    if isinstance(raw, list):
        if len(raw) > MAX_OBSERVATIONS_PER_ARTIFACT:
            raise ValueError("providerartefact bevat te veel observaties")
        result = []
        for value in raw:
            if not isinstance(value, dict):
                raise ValueError("providerartefact bevat ongeldige observatie")
            value_source = str(value.get("source") or source)
            # Forecast pixels may coexist in KNMI's artifact but are not an
            # observed radar frame and therefore never enter calibration.
            if value_source != source:
                continue
            lat, lon = _point((value.get("lat"), value.get("lon")))
            intensity = int(value.get("intensity") or 0)
            quality = value.get("quality")
            quality = _finite(quality, "quality") if quality is not None else None
            footprint = value.get("footprint_points") or ((lat, lon),)
            result.append({
                "lat": lat,
                "lon": lon,
                "intensity": intensity,
                "quality": quality,
                "footprint_points": tuple(_point(point) for point in footprint),
            })
        return result
    if source == "opera" and isinstance(payload.get("cells"), list):
        if len(payload["cells"]) > MAX_OBSERVATIONS_PER_ARTIFACT:
            raise ValueError("OPERA-artefact bevat te veel cellen")
        result = []
        for cell in payload["cells"]:
            lat, lon = _point((cell.get("centroid_lat"), cell.get("centroid_lon")))
            intensity = _opera_intensity(
                _finite(cell.get("max_dbz"), "max_dbz")
            )
            footprint = cell.get("footprint_points") or ((lat, lon),)
            result.append({
                "lat": lat,
                "lon": lon,
                "intensity": intensity,
                "quality": _finite(cell.get("mean_quality"), "mean_quality"),
                "footprint_points": tuple(_point(point) for point in footprint),
            })
        return result
    raise ValueError("providerartefact bevat geen kalibreerbare observaties")


def _frame(descriptor: dict, payload: dict, collected_at: float) -> dict:
    source = descriptor["source"]
    if payload.get("provider") != source:
        raise ValueError("capturebron en providerartefact verschillen")
    payload_timestamp = _finite(
        payload.get("frame_timestamp"), "payload.frame_timestamp"
    )
    if abs(payload_timestamp - descriptor["artifact_frame_timestamp"]) > 1.0:
        raise ValueError("providerartefact en artifactmetadata verschillen in tijd")
    observations = _observations(payload, source)
    overlay = _overlay(payload, descriptor)
    product_timestamp = float(overlay["timestamp"])
    maximum_overlay_offset = 30.0 * 60.0 if source == "noaa_goes_rrqpe" else 1.0
    if abs(product_timestamp - payload_timestamp) > maximum_overlay_offset:
        raise ValueError("regionale overlaytijd is niet aan het product gekoppeld")
    selected = [
        value for value in observations
        if value["intensity"] >= descriptor["minimum_intensity"]
        and _inside_bbox(value["lat"], value["lon"], descriptor["coverage_bbox"])
        and _haversine_km(
            descriptor["center_lat"], descriptor["center_lon"],
            value["lat"], value["lon"],
        ) <= descriptor["radius_km"]
    ]
    # Filled native overlay runs are the meteorological precipitation field.
    # Observation footprints are component boundaries and would turn a wet
    # area into a sparse contour if only their vertices were sampled.
    points = _rasterize_overlay(overlay, descriptor)
    coverage_fraction = _bbox_fraction(
        descriptor["coverage_bbox"], descriptor["evaluation_bbox"]
    )
    return {
        "region_key": descriptor["region_key"],
        "region_id": descriptor["region_id"],
        "source": source,
        # The exact regional overlay time is authoritative.  This matters for
        # mixed GOES-18/19 jobs, whose envelope timestamp is deliberately the
        # oldest satellite product while each region has its own product time.
        "nominal_minute": int(round(product_timestamp / 60.0)),
        "product_timestamp": product_timestamp,
        "collected_at": collected_at,
        "grid_deg": descriptor["grid_deg"],
        "observation_count": len(selected),
        "coverage_bbox": descriptor["coverage_bbox"],
        "coverage_fraction": round(coverage_fraction, 6),
        "evaluation_bbox": descriptor["evaluation_bbox"],
        "points": points,
        "overlay": overlay,
    }


def _points_in_bbox(frame: dict, bbox) -> dict:
    grid_deg = float(frame["grid_deg"])
    return {
        cell: values
        for cell, values in frame["points"].items()
        if _inside_bbox(
            (cell[0] + 0.5) * grid_deg,
            (cell[1] + 0.5) * grid_deg,
            bbox,
        )
    }


def _comparison(first: dict, second: dict, compared_at: float) -> dict | None:
    if abs(float(first["grid_deg"]) - float(second["grid_deg"])) > 1e-12:
        raise ValueError("kalibratieframes gebruiken onverenigbare rasters")
    shared_bbox = _intersection(first["coverage_bbox"], second["coverage_bbox"])
    shared_fraction = _bbox_fraction(shared_bbox, first["evaluation_bbox"])
    if shared_fraction < MIN_SHARED_COVERAGE_FRACTION:
        return None
    primary_points = _points_in_bbox(first, shared_bbox)
    reference_points = _points_in_bbox(second, shared_bbox)
    primary, reference = set(primary_points), set(reference_points)
    overlap = primary & reference
    precision = len(overlap) / len(primary) if primary else None
    recall = len(overlap) / len(reference) if reference else None
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision is not None and recall is not None and precision + recall
        else None
    )
    return {
        "region_key": first["region_key"],
        "source_a": first["source"],
        "source_b": second["source"],
        "nominal_minute": first["nominal_minute"],
        "compared_at": compared_at,
        "primary_cells": len(primary),
        "reference_cells": len(reference),
        "overlap_cells": len(overlap),
        "false_positive_cells": len(primary - reference),
        "missed_cells": len(reference - primary),
        "precision": round(precision, 3) if precision is not None else None,
        "recall": round(recall, 3) if recall is not None else None,
        "f1_score": round(f1, 3) if f1 is not None else None,
        "shared_coverage_fraction": round(shared_fraction, 3),
        "_shared_bbox": shared_bbox,
    }


def _best_shift(from_points: set, to_points: set):
    if not from_points or not to_points:
        return None
    best, best_score, best_cost = (0, 0), -1, 99
    for shift_lat in range(-2, 3):
        for shift_lon in range(-2, 3):
            score = sum(
                (lat + shift_lat, lon + shift_lon) in from_points
                for lat, lon in to_points
            )
            cost = abs(shift_lat) + abs(shift_lon)
            if score > best_score or score == best_score and cost < best_cost:
                best, best_score, best_cost = (
                    (shift_lat, shift_lon), score, cost
                )
    return best


def _intensity_bias(from_points: dict, to_points: dict, shift):
    if shift is None:
        return None
    differences = []
    for (lat, lon), to_value in to_points.items():
        from_value = from_points.get((lat + shift[0], lon + shift[1]))
        if from_value is not None and to_value is not None:
            differences.append(float(to_value) - float(from_value))
    return round(sum(differences) / len(differences), 4) if differences else None


def _bias_rows(comparison: dict, first: dict, second: dict):
    rows = []
    latency = second["product_timestamp"] - first["product_timestamp"]
    for source_frame, target_frame, from_count, to_count, delta in (
        (
            first, second, comparison["primary_cells"],
            comparison["reference_cells"], latency,
        ),
        (
            second, first, comparison["reference_cells"],
            comparison["primary_cells"], -latency,
        ),
    ):
        if from_count <= 0:
            continue
        from_points = {
            point: values[0]
            for point, values in _points_in_bbox(
                source_frame, comparison["_shared_bbox"]
            ).items()
        }
        to_points = {
            point: values[0]
            for point, values in _points_in_bbox(
                target_frame, comparison["_shared_bbox"]
            ).items()
        }
        shift = _best_shift(set(from_points), set(to_points))
        overlap = comparison["overlap_cells"]
        extra = max(0, to_count - overlap) / to_count if to_count else None
        rows.append((
            comparison["region_key"],
            _scope(comparison["region_key"]),
            source_frame["source"],
            target_frame["source"],
            comparison["nominal_minute"],
            comparison["compared_at"],
            from_count,
            to_count,
            overlap,
            round(overlap / from_count, 6),
            round(extra, 6) if extra is not None else None,
            round(to_count / from_count, 6),
            comparison["f1_score"],
            _intensity_bias(from_points, to_points, shift),
            shift[0] if shift is not None else None,
            shift[1] if shift is not None else None,
            delta,
        ))
    return rows


def _create_schema(db: sqlite3.Connection) -> None:
    db.executescript("""
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=FULL;
        CREATE TABLE manifest(key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
        CREATE TABLE frames(
            region_key TEXT NOT NULL, source TEXT NOT NULL,
            nominal_minute INTEGER NOT NULL, product_timestamp REAL NOT NULL,
            collected_at REAL NOT NULL, grid_deg REAL NOT NULL,
            observation_count INTEGER NOT NULL, grid_point_count INTEGER NOT NULL,
            coverage_lon_min REAL, coverage_lat_min REAL,
            coverage_lon_max REAL, coverage_lat_max REAL,
            coverage_fraction REAL NOT NULL,
            PRIMARY KEY(region_key, source, nominal_minute)
        ) WITHOUT ROWID;
        CREATE TABLE frame_points(
            region_key TEXT NOT NULL, source TEXT NOT NULL,
            nominal_minute INTEGER NOT NULL, grid_lat INTEGER NOT NULL,
            grid_lon INTEGER NOT NULL, max_intensity REAL,
            max_quality REAL, observation_count INTEGER NOT NULL,
            PRIMARY KEY(region_key, source, nominal_minute, grid_lat, grid_lon)
        ) WITHOUT ROWID;
        CREATE TABLE comparisons(
            region_key TEXT NOT NULL, source_a TEXT NOT NULL,
            source_b TEXT NOT NULL, nominal_minute INTEGER NOT NULL,
            compared_at REAL NOT NULL, primary_cells INTEGER NOT NULL,
            reference_cells INTEGER NOT NULL, overlap_cells INTEGER NOT NULL,
            false_positive_cells INTEGER NOT NULL, missed_cells INTEGER NOT NULL,
            precision REAL, recall REAL, f1_score REAL,
            shared_coverage_fraction REAL NOT NULL,
            PRIMARY KEY(region_key, source_a, source_b, nominal_minute)
        ) WITHOUT ROWID;
        CREATE TABLE original_radar_frames(
            region_id TEXT NOT NULL, source TEXT NOT NULL,
            nominal_timestamp INTEGER NOT NULL, saved_at REAL NOT NULL,
            encoding TEXT NOT NULL, payload BLOB NOT NULL,
            PRIMARY KEY(region_id, source, nominal_timestamp)
        ) WITHOUT ROWID;
        CREATE TABLE provider_bias_samples(
            region_key TEXT NOT NULL, scope_key TEXT NOT NULL,
            from_source TEXT NOT NULL, to_source TEXT NOT NULL,
            nominal_minute INTEGER NOT NULL, compared_at REAL NOT NULL,
            from_cells INTEGER NOT NULL, to_cells INTEGER NOT NULL,
            overlap_cells INTEGER NOT NULL, detection_ratio REAL,
            extra_fraction REAL, wet_area_ratio REAL, f1_score REAL,
            intensity_bias REAL, shift_lat_cells REAL, shift_lon_cells REAL,
            latency_seconds REAL,
            PRIMARY KEY(region_key, from_source, to_source, nominal_minute)
        ) WITHOUT ROWID;
    """)


def _cleanup_stages(now: float) -> None:
    if not ARTIFACT_ROOT.is_dir():
        return
    for path in ARTIFACT_ROOT.iterdir():
        is_stage = path.name.endswith(".calibration.sqlite3")
        is_stage_temporary = (
            path.name.startswith(".")
            and (
                path.name.endswith(".sqlite3")
                or path.name.endswith(".sqlite3-journal")
            )
        )
        if not path.is_file() or not (is_stage or is_stage_temporary):
            continue
        try:
            if now - path.stat().st_mtime > STAGE_RETENTION_SECONDS:
                path.unlink()
        except FileNotFoundError:
            pass


def _write_stage(job_id: str, frames: list[dict], comparisons: list[dict], now: float):
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    _cleanup_stages(now)
    safe_id = "".join(
        value for value in str(job_id)
        if value.isalnum() or value in {"-", "_"}
    )
    if not safe_id or safe_id != str(job_id):
        raise ValueError("ongeldig calibration stage job-ID")
    target = ARTIFACT_ROOT / f"{safe_id}.calibration.sqlite3"
    fd, temporary_name = tempfile.mkstemp(
        dir=ARTIFACT_ROOT, prefix=f".{safe_id}.", suffix=".sqlite3"
    )
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        db = sqlite3.connect(temporary)
        try:
            _create_schema(db)
            manifest = {
                "stage_schema_version": str(STAGE_SCHEMA_VERSION),
                "algorithm_version": ALGORITHM_VERSION,
                "job_id": str(job_id),
                "created_at": repr(float(now)),
            }
            db.executemany(
                "INSERT INTO manifest(key, value) VALUES(?, ?)",
                sorted(manifest.items()),
            )
            for frame in frames:
                coverage = frame["coverage_bbox"]
                db.execute(
                    "INSERT INTO frames VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        frame["region_key"], frame["source"],
                        frame["nominal_minute"], frame["product_timestamp"],
                        frame["collected_at"], frame["grid_deg"],
                        frame["observation_count"], len(frame["points"]),
                        *coverage, frame["coverage_fraction"],
                    ),
                )
                if (
                    db.execute("PRAGMA page_count").fetchone()[0]
                    * db.execute("PRAGMA page_size").fetchone()[0]
                    > MAX_STAGE_BYTES
                ):
                    raise ValueError("calibration stage overschrijdt bytecontract")
                db.executemany(
                    "INSERT INTO frame_points VALUES(?,?,?,?,?,?,?,?)",
                    (
                        (
                            frame["region_key"], frame["source"],
                            frame["nominal_minute"], lat, lon,
                            values[0], values[1], values[2],
                        )
                        for (lat, lon), values in frame["points"].items()
                    ),
                )
                overlay_bytes = json.dumps(
                    frame["overlay"],
                    allow_nan=False,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
                db.execute(
                    "INSERT INTO original_radar_frames VALUES(?,?,?,?,?,?)",
                    (
                        frame["region_id"], frame["source"],
                        int(frame["overlay"]["timestamp"] // 60) * 60,
                        now, "zlib-json-v1", zlib.compress(overlay_bytes, 6),
                    ),
                )
                if (
                    db.execute("PRAGMA page_count").fetchone()[0]
                    * db.execute("PRAGMA page_size").fetchone()[0]
                    > MAX_STAGE_BYTES
                ):
                    raise ValueError("calibration stage overschrijdt bytecontract")
            frame_index = {
                (frame["region_key"], frame["source"], frame["nominal_minute"]): frame
                for frame in frames
            }
            for comparison in comparisons:
                db.execute(
                    "INSERT INTO comparisons VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    tuple(comparison[key] for key in (
                        "region_key", "source_a", "source_b", "nominal_minute",
                        "compared_at", "primary_cells", "reference_cells",
                        "overlap_cells", "false_positive_cells", "missed_cells",
                        "precision", "recall", "f1_score",
                        "shared_coverage_fraction",
                    )),
                )
                first = frame_index[(
                    comparison["region_key"], comparison["source_a"],
                    comparison["nominal_minute"],
                )]
                second = frame_index[(
                    comparison["region_key"], comparison["source_b"],
                    comparison["nominal_minute"],
                )]
                db.executemany(
                    "INSERT INTO provider_bias_samples VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    _bias_rows(comparison, first, second),
                )
                if (
                    db.execute("PRAGMA page_count").fetchone()[0]
                    * db.execute("PRAGMA page_size").fetchone()[0]
                    > MAX_STAGE_BYTES
                ):
                    raise ValueError("calibration stage overschrijdt bytecontract")
            db.commit()
        finally:
            db.close()
        # Windows rejects FlushFileBuffers on a read-only handle.  Open the
        # completed SQLite file read/write solely for the durability barrier;
        # no bytes are changed here.
        with temporary.open("rb+") as stream:
            os.fsync(stream.fileno())
        size = temporary.stat().st_size
        if size <= 0 or size > MAX_STAGE_BYTES:
            raise ValueError("calibration stage overschrijdt bytecontract")
        temporary.replace(target)
        try:
            directory_fd = os.open(ARTIFACT_ROOT, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            # Directory fsync is unavailable on some supported platforms;
            # the file itself was already fsynced before atomic replacement.
            pass
        encoded = target.read_bytes()
        return {
            "stage_path": str(target),
            "stage_sha256": hashlib.sha256(encoded).hexdigest(),
            "stage_bytes": len(encoded),
            "stage_content_type": (
                "application/vnd.stv3.calibration-stage+sqlite3"
            ),
        }
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def calibration_prepare_v2(job) -> dict:
    """Prepare one complete provider-cycle calibration transaction."""
    raw_captures = job.payload.get("captures")
    if (
        not isinstance(raw_captures, list)
        or not raw_captures
        or len(raw_captures) > MAX_CAPTURES
    ):
        raise ValueError("ongeldige kalibratiecapturecollectie")
    captures = [_descriptor(value) for value in raw_captures]
    if len({value["capture_id"] for value in captures}) != len(captures):
        raise ValueError("dubbele kalibratiecapture-ID")
    artifact_groups = {}
    total_declared_bytes = 0
    for capture in captures:
        metadata = capture["artifact"]
        cache_key = (
            metadata.get("artifact_path"), metadata.get("artifact_sha256"),
            metadata.get("artifact_bytes"), metadata.get("content_type"),
        )
        artifact_groups.setdefault(cache_key, []).append(capture)
    if len(artifact_groups) > MAX_UNIQUE_ARTIFACTS:
        raise ValueError("kalibratiejob bevat te veel unieke providerartefacten")
    total_declared_bytes = sum(int(key[2]) for key in artifact_groups)
    if total_declared_bytes > MAX_TOTAL_DECLARED_ARTIFACT_BYTES:
        raise ValueError("kalibratiejob overschrijdt totaal artifactbudget")

    frames_by_key = {}
    total_stage_points = 0
    collected_at = time.time()
    for cache_key, artifact_captures in artifact_groups.items():
        payload = read_json_artifact(artifact_captures[0]["artifact"])
        for capture in artifact_captures:
            frame = _frame(capture, payload, collected_at)
            frame_key = (
                frame["region_key"], frame["source"], frame["nominal_minute"]
            )
            existing = frames_by_key.get(frame_key)
            # More than one product can fall in the same rounded minute while
            # a previous low-priority job is queued. Select the newest actual
            # regional product deterministically.
            candidate_order = (
                float(frame["product_timestamp"]), str(capture["capture_id"])
            )
            existing_order = (
                float(existing["product_timestamp"]),
                str(existing.get("capture_id") or ""),
            ) if existing is not None else None
            if existing_order is None or candidate_order > existing_order:
                projected_points = (
                    total_stage_points
                    - (len(existing["points"]) if existing is not None else 0)
                    + len(frame["points"])
                )
                if projected_points > MAX_TOTAL_STAGE_POINTS:
                    raise ValueError("kalibratiejob overschrijdt totaal puntbudget")
                total_stage_points = projected_points
                frame["capture_id"] = str(capture["capture_id"])
                frames_by_key[frame_key] = frame
        del payload
    frames = sorted(
        frames_by_key.values(),
        key=lambda value: (
            value["region_key"], value["nominal_minute"], value["source"]
        ),
    )
    comparisons = []
    grouped = {}
    for frame in frames:
        grouped.setdefault(
            (frame["region_key"], frame["nominal_minute"]), []
        ).append(frame)
    comparison_pairs = sum(
        len(available) * (len(available) - 1) // 2
        for available in grouped.values()
    )
    comparison_point_operations = sum(
        len(first["points"]) + len(second["points"])
        for available in grouped.values()
        for first, second in combinations(available, 2)
    )
    if (
        comparison_pairs > MAX_COMPARISON_PAIRS
        or comparison_point_operations > MAX_COMPARISON_POINT_OPERATIONS
    ):
        raise ValueError("kalibratiejob overschrijdt vergelijkingsbudget")
    for available in grouped.values():
        for first, second in combinations(
            sorted(available, key=lambda value: value["source"]), 2
        ):
            comparison = _comparison(first, second, collected_at)
            if comparison is not None:
                comparisons.append(comparison)
    stage = _write_stage(job.job_id, frames, comparisons, collected_at)
    compact_comparisons = [
        [comparison[key] for key in (
            "region_key", "source_a", "source_b", "nominal_minute",
            "compared_at", "primary_cells", "reference_cells",
            "overlap_cells", "false_positive_cells", "missed_cells",
            "precision", "recall", "f1_score", "shared_coverage_fraction",
        )]
        for comparison in comparisons
    ]
    return {
        "method": "worker_radar_calibration_v2",
        **stage,
        "captures_received": len(captures),
        "source_artifacts_read": len(artifact_groups),
        "frames_prepared": len(frames),
        "comparisons_prepared": len(comparisons),
        "original_overlays_prepared": len(frames),
        "comparisons": compact_comparisons,
    }
