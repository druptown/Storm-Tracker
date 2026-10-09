"""Bronbewuste lokale grondverificatie voor neerslagsystemen."""
from __future__ import annotations

import hashlib
import json
import math
import time

from ..geometry.distance import haversine as _distance_km
from ..geometry.polygon import point_to_polygon_distance_km

RAIN_THRESHOLD_MM_H = 0.1
DEFAULT_LOCAL_RADIUS_KM = 10.0
DEFAULT_FOOTPRINT_TOLERANCE_KM = 8.0
DEFAULT_FOOTPRINT_TIME_TOLERANCE_S = 15 * 60
SAME_RADAR_FRAME_TOLERANCE_S = 1.0
PHYSICAL_SITE_DISTANCE_KM = 0.1
EARTH_MEAN_RADIUS_KM = 6_371.0088
MAX_SERIALIZED_GROUND_ASSESSMENTS = 128
GROUND_FRESHNESS_POLICY_ID = "stv3-ground-current-freshness-v1"
FRESHNESS_SECONDS = {
    "netatmo": 10 * 60,
    "synoptic": 20 * 60,
    "metar": 35 * 60,
}
PROVIDER_WEIGHT = {"netatmo": 0.75, "synoptic": 1.0, "metar": 0.8}


def _confirmation_basis(sources) -> str | None:
    sources = sorted(set(sources or ()))
    if sources == ["synoptic"]:
        return "synoptic_grondmeting"
    if sources == ["metar"]:
        return "metar_grondwaarneming"
    if sources == ["netatmo"]:
        return "netatmo_grondmeting"
    if sources:
        return "meerdere_grondnetwerken"
    return None


def _observation_is_wet(observation) -> bool:
    present = getattr(observation, "precipitation_present", None)
    if isinstance(present, bool):
        return present
    if present is not None:
        return False
    try:
        rain_rate = float(getattr(observation, "rain_mm", None) or 0.0)
    except (TypeError, ValueError):
        return False
    return math.isfinite(rain_rate) and rain_rate >= RAIN_THRESHOLD_MM_H


def _coerce_footprint_ring(value) -> list[tuple[float, float]]:
    """Normaliseer exact één contour zonder losse contouren te verbinden."""
    points = []
    for point in value or ():
        if (
            not isinstance(point, (list, tuple))
            or len(point) < 2
            or isinstance(point[0], (list, tuple))
        ):
            return []
        try:
            points.append((float(point[0]), float(point[1])))
        except (TypeError, ValueError):
            return []
    return points


def _storm_footprints(
    storm,
    observation_timestamp: float | None = None,
) -> list[list[tuple[float, float]]]:
    if isinstance(storm, dict):
        raw_footprints = storm.get("footprints")
        if raw_footprints:
            single_ring = _coerce_footprint_ring(raw_footprints)
            if single_ring:
                footprints = [single_ring]
            else:
                footprints = [
                    ring
                    for raw_ring in raw_footprints
                    if (ring := _coerce_footprint_ring(raw_ring))
                ]
        else:
            ring = _coerce_footprint_ring(
                storm.get("footprint") or storm.get("hull") or ()
            )
            footprints = [ring] if ring else []
        footprint_timestamp = storm.get("footprint_timestamp")
        if (
            footprints
            and footprint_timestamp is not None
            and observation_timestamp is not None
            and abs(
                float(footprint_timestamp) - float(observation_timestamp)
            ) > DEFAULT_FOOTPRINT_TIME_TOLERANCE_S
        ):
            footprints = []
    else:
        footprints = []
        frames = getattr(storm, "radar_system_frames", {}).values()
        frames = list(frames)
        if frames:
            if observation_timestamp is None:
                selected = max(frames, key=lambda frame: frame.timestamp)
            else:
                selected = min(
                    frames,
                    key=lambda frame: abs(
                        float(frame.timestamp)
                        - float(observation_timestamp)
                    ),
                )
                if abs(
                    float(selected.timestamp)
                    - float(observation_timestamp)
                ) > DEFAULT_FOOTPRINT_TIME_TOLERANCE_S:
                    selected = None
            if selected is not None:
                selected_timestamp = float(selected.timestamp)
                footprints = [
                    ring
                    for frame in frames
                    if abs(
                        float(frame.timestamp) - selected_timestamp
                    ) <= SAME_RADAR_FRAME_TOLERANCE_S
                    and (
                        ring := _coerce_footprint_ring(
                            getattr(frame, "footprint_points", ())
                        )
                    )
                ]
    return footprints


def _storm_footprint(
    storm,
    observation_timestamp: float | None = None,
) -> list[tuple[float, float]]:
    """Compatibiliteitshulp: geef de eerste losse contour terug."""
    footprints = _storm_footprints(storm, observation_timestamp)
    return footprints[0] if footprints else []


def _storm_id(storm) -> str | None:
    value = storm.get("storm_id") if isinstance(storm, dict) else getattr(
        storm, "storm_id", None
    )
    return str(value) if value is not None else None


def _footprint_matches(
    lat,
    lon,
    storms,
    tolerance_km,
    observation_timestamp=None,
):
    matches = []
    for storm in storms or []:
        storm_id = _storm_id(storm)
        footprints = _storm_footprints(storm, observation_timestamp)
        if not footprints or storm_id is None:
            continue
        distances = [
            distance
            for footprint in footprints
            if (
                distance := point_to_polygon_distance_km(
                    float(lat), float(lon), footprint
                )
            ) is not None
        ]
        distance = min(distances, default=None)
        if distance is not None and distance <= float(tolerance_km):
            matches.append((distance, storm_id))
    return sorted(matches, key=lambda item: (item[0], item[1]))


def _physical_site_identity(item) -> str | None:
    observation = item["observation"]
    explicit = (
        getattr(observation, "physical_station_id", None)
        or getattr(observation, "canonical_station_id", None)
    )
    if explicit:
        return f"physical:{str(explicit).strip().upper()}"
    station_id = str(
        getattr(observation, "station_id", "") or ""
    ).strip()
    if not station_id:
        return None
    provider = str(item["provider"]).strip().lower()
    explicit_prefix = f"{provider}:"
    normalized = (
        station_id[len(explicit_prefix):]
        if station_id.lower().startswith(explicit_prefix)
        else station_id
    ).strip().upper()
    if (
        provider in {"metar", "synoptic"}
        and len(normalized) == 4
        and normalized.isalnum()
    ):
        return f"aviation:{normalized}"
    return f"{provider}:{normalized}"


def _source_record_identity(item) -> tuple[str, str]:
    """Keep a real provider record ID or label a deterministic derived key."""

    observation = item["observation"]
    raw_payload = getattr(observation, "raw_payload", None)
    if isinstance(raw_payload, dict):
        for key in ("provider_record_id", "observation_id", "record_id"):
            value = raw_payload.get(key)
            if isinstance(value, str) and value.strip():
                return (
                    f"{item['provider']}:{value.strip()}",
                    "provider_record",
                )
    station_id = str(
        getattr(observation, "station_id", "") or ""
    ).strip()
    return (
        f"{item['provider']}:{station_id}:{float(item['observed_at']):.6f}",
        "derived_station_timestamp",
    )


def _site_member_key(item) -> str:
    observation = item["observation"]
    return "|".join((
        str(item["provider"]),
        str(getattr(observation, "station_id", "") or ""),
        f"{float(observation.lat):.8f}",
        f"{float(observation.lon):.8f}",
        f"{float(item['observed_at']):.6f}",
    ))


def _site_identity_member_key(item) -> str:
    """Stable cluster member key without observation-event timestamps."""

    observation = item["observation"]
    return "|".join((
        str(item["provider"]),
        str(getattr(observation, "station_id", "") or ""),
        f"{float(observation.lat):.8f}",
        f"{float(observation.lon):.8f}",
    ))


def _unit_sphere_coordinates(latitude: float, longitude: float):
    latitude_rad = math.radians(float(latitude))
    longitude_rad = math.radians(float(longitude))
    cos_latitude = math.cos(latitude_rad)
    return (
        cos_latitude * math.cos(longitude_rad),
        cos_latitude * math.sin(longitude_rad),
        math.sin(latitude_rad),
    )


def _deduplicate_physical_sites(assessed):
    """Deduplicate deterministically; proximity is not an identity alias."""

    ordered = sorted(assessed, key=_site_member_key)
    identities = [_physical_site_identity(item) for item in ordered]
    parent = list(range(len(ordered)))

    def find(index):
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left, right):
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    identity_roots = {}
    for index, identity in enumerate(identities):
        if identity is None:
            continue
        if identity in identity_roots:
            union(index, identity_roots[identity])
        else:
            identity_roots[identity] = index

    chord_cell_size = 2.0 * math.sin(
        PHYSICAL_SITE_DISTANCE_KM / (2.0 * EARTH_MEAN_RADIUS_KM)
    )
    spatial_buckets = {}
    for index, item in enumerate(ordered):
        observation = item["observation"]
        coordinates = _unit_sphere_coordinates(
            observation.lat,
            observation.lon,
        )
        bucket = tuple(
            math.floor(value / chord_cell_size)
            for value in coordinates
        )
        for x_offset in (-1, 0, 1):
            for y_offset in (-1, 0, 1):
                for z_offset in (-1, 0, 1):
                    nearby_bucket = (
                        bucket[0] + x_offset,
                        bucket[1] + y_offset,
                        bucket[2] + z_offset,
                    )
                    for other_index in spatial_buckets.get(
                        nearby_bucket,
                        (),
                    ):
                        other = ordered[other_index]["observation"]
                        if _distance_km(
                            observation.lat,
                            observation.lon,
                            other.lat,
                            other.lon,
                        ) <= PHYSICAL_SITE_DISTANCE_KM:
                            union(index, other_index)
        spatial_buckets.setdefault(bucket, []).append(index)

    grouped = {}
    for index, item in enumerate(ordered):
        grouped.setdefault(find(index), []).append(item)

    representatives = []
    for root_index in sorted(grouped):
        items = grouped[root_index]
        representative = max(
            items,
            key=lambda item: (
                bool(item["fresh"]),
                float(item["quality"]),
                float(getattr(item["observation"], "timestamp", 0.0)),
                _site_member_key(item),
            ),
        )
        group_identities = sorted({
            identity
            for item in items
            if (identity := _physical_site_identity(item)) is not None
        })
        exact_identity_group = (
            len(group_identities) == 1
            and all(
                _physical_site_identity(item) == group_identities[0]
                for item in items
            )
        )
        if exact_identity_group:
            site_id = group_identities[0]
            site_basis = "provider_identity"
        elif len(items) > 1:
            digest = hashlib.sha256(
                "\n".join(
                    sorted(
                        _site_identity_member_key(item)
                        for item in items
                    )
                ).encode("utf-8")
            ).hexdigest()
            site_id = f"proximity-group:{digest[:24]}"
            site_basis = "proximity_conservative_dedup"
        elif group_identities:
            site_id = group_identities[0]
            site_basis = "provider_identity"
        else:
            observation = representative["observation"]
            site_id = (
                f"unverified-geo:{float(observation.lat):.5f},"
                f"{float(observation.lon):.5f}"
            )
            site_basis = "unverified_coordinate"
        site_sources = sorted({item["provider"] for item in items})
        site_station_ids = sorted({
            str(getattr(item["observation"], "station_id", "") or "")
            for item in items
            if getattr(item["observation"], "station_id", None)
        })
        representative_station_id = str(
            getattr(
                representative["observation"], "station_id", ""
            ) or ""
        )
        for item in items:
            item["physical_site_id"] = site_id
            item["physical_site_basis"] = site_basis
            item["physical_site_member_count"] = len(items)
            item["physical_duplicate"] = item is not representative
            item["counts_as_independent"] = item is representative
        representative["site_sources"] = site_sources
        representative["site_station_ids"] = site_station_ids
        # Alleen de gekozen onafhankelijke waarneming levert evidence.
        # Andere providerregels op dezelfde fysieke site blijven zichtbaar
        # als provenance, maar mogen niet als natte corroboratie meetellen.
        representative["evidence_sources"] = [
            representative["provider"]
        ]
        representative["evidence_station_ids"] = (
            [representative_station_id]
            if representative_station_id
            else []
        )
        representatives.append(representative)
    return representatives


def _assessment_row(item):
    """Serialize one bounded, provenance-preserving assessment row."""

    return {
        "station_id": str(getattr(item["observation"], "station_id", "")),
        "physical_site_id": item["physical_site_id"],
        "physical_duplicate": item["physical_duplicate"],
        "counts_as_independent": item["counts_as_independent"],
        "physical_site_basis": item["physical_site_basis"],
        "physical_site_member_count": item["physical_site_member_count"],
        "provider": item["provider"],
        "source_record_id": item["source_record_id"],
        "source_record_id_kind": item["source_record_id_kind"],
        "observed_at": item["observed_at"],
        "age_seconds": round(item["age"], 1),
        "freshness_limit_seconds": item["freshness_limit_seconds"],
        "freshness_policy_id": item["freshness_policy_id"],
        "freshness_policy_known": item["freshness_policy_known"],
        "distance_target_km": round(item["distance"], 2),
        "distance_storm_km": (
            round(item["storm_distance"], 2)
            if item["storm_distance"] is not None else None
        ),
        "storm_id": item["storm_id"],
        "storm_ids": item["storm_ids"],
        "storm_match_ambiguous": item["storm_match_ambiguous"],
        "wet": item["wet"],
        "fresh": item["fresh"],
        "eligible": item["eligible"],
        "quality_score": round(item["quality"], 3),
    }


def _assessment_collection(assessed, local_radius_km):
    """Bound HA serialization while hashing and evaluating every input row."""

    digest = hashlib.sha256()
    for item in sorted(assessed, key=_site_member_key):
        encoded = json.dumps(
            _assessment_row(item),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)

    def priority(item):
        local = (
            item["fresh"]
            and item["distance"] <= float(local_radius_km)
        )
        if item["counts_as_independent"] and local and item["wet"]:
            category = 0
        elif item["counts_as_independent"] and local:
            category = 1
        elif item["counts_as_independent"]:
            category = 2
        else:
            category = 3
        return (
            category,
            float(item["distance"]),
            _site_member_key(item),
        )

    ordered = sorted(assessed, key=priority)
    selected = ordered[:MAX_SERIALIZED_GROUND_ASSESSMENTS]
    overflow = len(ordered) - len(selected)
    return [_assessment_row(item) for item in selected], {
        "input_total_count": len(ordered),
        "serialized_count": len(selected),
        "overflow_count": overflow,
        "complete": overflow == 0,
        "content_sha256": digest.hexdigest(),
        "reason_codes": (
            ["ground_assessment_serialization_bounded"]
            if overflow else []
        ),
    }


def build_local_netatmo_summary(
    observations,
    target_lat,
    target_lon,
    *,
    local_radius_km: float = DEFAULT_LOCAL_RADIUS_KM,
    storms=None,
    footprint_tolerance_km: float = DEFAULT_FOOTPRINT_TOLERANCE_KM,
    now: float | None = None,
) -> dict:
    """Beoordeel verse grondmetingen lokaal en tegen dezelfde radarvoetafdruk."""
    current_time = time.time() if now is None else float(now)
    numeric_inputs = (
        ("current_time", current_time),
        ("target_lat", target_lat),
        ("target_lon", target_lon),
        ("local_radius_km", local_radius_km),
        ("footprint_tolerance_km", footprint_tolerance_km),
    )
    for label, value in numeric_inputs:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise ValueError(f"{label} must be finite")
    if not -90.0 <= float(target_lat) <= 90.0:
        raise ValueError("target_lat must be in [-90, 90]")
    if not -180.0 <= float(target_lon) <= 180.0:
        raise ValueError("target_lon must be in [-180, 180]")
    if current_time < 0.0:
        raise ValueError("current_time cannot be negative")
    if float(local_radius_km) < 0.0:
        raise ValueError("local_radius_km cannot be negative")
    if float(footprint_tolerance_km) < 0.0:
        raise ValueError("footprint_tolerance_km cannot be negative")
    observations = list(observations or [])
    storm_filter_active = storms is not None
    assessed = []
    rejected_observation_count = 0
    for observation in observations:
        provider = str(
            getattr(observation, "source", None) or "netatmo"
        ).strip().lower()
        try:
            timestamp = float(
                getattr(observation, "timestamp", 0.0) or 0.0
            )
            target_distance = _distance_km(
                target_lat, target_lon, observation.lat, observation.lon
            )
        except (AttributeError, TypeError, ValueError):
            rejected_observation_count += 1
            continue
        if (
            not math.isfinite(timestamp)
            or not math.isfinite(target_distance)
            or not -90.0 <= float(observation.lat) <= 90.0
            or not -180.0 <= float(observation.lon) <= 180.0
        ):
            rejected_observation_count += 1
            continue
        age = current_time - timestamp
        configured_limit = FRESHNESS_SECONDS.get(provider)
        fresh_limit = (
            float(configured_limit)
            if configured_limit is not None else None
        )
        fresh = (
            timestamp > 0.0
            and age >= 0.0
            and fresh_limit is not None
            and age <= fresh_limit
        )
        storm_matches = _footprint_matches(
            observation.lat,
            observation.lon,
            storms,
            footprint_tolerance_km,
            timestamp,
        )
        storm_ids = [storm_id for _distance, storm_id in storm_matches]
        matched = bool(storm_matches)
        storm_distance = storm_matches[0][0] if storm_matches else None
        storm_id = storm_matches[0][1] if storm_matches else None
        storm_match_ambiguous = bool(
            len(storm_matches) > 1
            and abs(storm_matches[0][0] - storm_matches[1][0]) <= 0.05
        )
        wet = _observation_is_wet(observation)
        eligible = (
            fresh
            and target_distance <= float(local_radius_km)
            and (matched if storm_filter_active else True)
            and not storm_match_ambiguous
        )
        quality = (
            PROVIDER_WEIGHT.get(provider, 0.7)
            * max(0.2, 1.0 - age / float(fresh_limit))
            if eligible else 0.0
        )
        item = {
            "distance": target_distance,
            "observation": observation,
            "provider": provider,
            "observed_at": timestamp,
            "age": age,
            "fresh": fresh,
            "freshness_limit_seconds": fresh_limit,
            "freshness_policy_id": GROUND_FRESHNESS_POLICY_ID,
            "freshness_policy_known": fresh_limit is not None,
            "wet": wet,
            "storm_match": matched,
            "storm_id": storm_id,
            "storm_ids": storm_ids,
            "storm_distance": storm_distance,
            "storm_match_ambiguous": storm_match_ambiguous,
            "eligible": eligible,
            "quality": quality,
        }
        (
            item["source_record_id"],
            item["source_record_id_kind"],
        ) = _source_record_identity(item)
        assessed.append(item)

    assessed.sort(key=lambda item: item["distance"])
    independent = _deduplicate_physical_sites(assessed)
    independent.sort(key=lambda item: item["distance"])
    local = [
        item for item in independent
        if item["fresh"] and item["distance"] <= float(local_radius_km)
    ]
    eligible = [item for item in local if item["eligible"]]
    local_raining = [item for item in eligible if item["wet"]]
    observed_raining = [item for item in local if item["wet"]]
    all_raining = [
        item for item in independent if item["fresh"] and item["wet"]
    ]
    confirmation_by_storm = {}
    for item in local_raining:
        storm_id = item["storm_id"]
        if storm_id is not None:
            confirmation_by_storm.setdefault(storm_id, []).append(item)
    confirmation_details = {}
    for storm_id, items in confirmation_by_storm.items():
        score = sum(item["quality"] for item in items)
        # Twee waarnemingen voor exact hetzelfde systeem blijven vereist.
        group_confirmed = len(items) >= 2 and score >= 0.75
        sources = sorted({
            source
            for item in items
            for source in item.get(
                "evidence_sources", [item["provider"]]
            )
        })
        confirmation_details[storm_id] = {
            "raining_station_count": len(items),
            "confirmation_score": round(score, 3),
            "confirmed": group_confirmed,
            "confidence_percent": (
                min(95, round(35 + score * 30))
                if group_confirmed
                else min(70, round(score * 40))
            ),
            "confirmation_basis": _confirmation_basis(sources),
            "sources": sources,
            "station_ids": sorted({
                station_id
                for item in items
                for station_id in item.get(
                    "evidence_station_ids", ()
                )
            }),
            "physical_site_ids": sorted({
                item["physical_site_id"] for item in items
            }),
        }
    confirmed_storm_ids = sorted(
        storm_id
        for storm_id, detail in confirmation_details.items()
        if detail["confirmed"]
    )
    if storm_filter_active:
        raining_score = max(
            (
                detail["confirmation_score"]
                for detail in confirmation_details.values()
            ),
            default=0.0,
        )
        confirmed = bool(confirmed_storm_ids)
    else:
        raining_score = sum(item["quality"] for item in local_raining)
        confirmed = len(local_raining) >= 2 and raining_score >= 0.75
    precipitation_observed = (
        len(observed_raining) >= 2
        or any(item["distance"] <= 3.0 for item in observed_raining)
    )
    status = (
        "regen_bevestigd"
        if confirmed
        else "regen_waargenomen"
        if precipitation_observed
        else "regen_aanwijzing"
        if observed_raining
        else "natte_stations_buiten_systeem"
        if any(item["wet"] for item in local) and storm_filter_active
        else "geen_lokale_regenmeter_nat"
        if local
        else "geen_lokale_stations"
        if assessed
        else "onvoldoende_data"
    )
    ground_sources = sorted({
        source
        for item in local
        for source in item.get("site_sources", [item["provider"]])
    })
    raining_sources = sorted({
        source
        for item in observed_raining
        for source in item.get(
            "evidence_sources", [item["provider"]]
        )
    })
    rain_values = [
        float(item["observation"].rain_mm)
        for item in local_raining
        if getattr(item["observation"], "quantitative_precipitation", True)
        and item["observation"].rain_mm is not None
    ]
    nearest_station = independent[0] if independent else None
    nearest_rain = all_raining[0] if all_raining else None
    matched_storm_ids = sorted({
        storm_id
        for item in local_raining
        for storm_id in item["storm_ids"]
    })
    confirmation_items = [
        item
        for item in local_raining
        if not storm_filter_active
        or any(
            storm_id in confirmed_storm_ids
            for storm_id in item["storm_ids"]
        )
    ]
    confirmation_sources = sorted({
        source
        for item in confirmation_items
        for source in item.get(
            "evidence_sources", [item["provider"]]
        )
    })
    confidence = min(95, round(35 + raining_score * 30)) if confirmed else (
        min(70, round(raining_score * 40)) if local_raining else 0
    )
    serialized_assessments, assessment_collection = _assessment_collection(
        assessed,
        local_radius_km,
    )
    result = {
        "ground_status": status,
        "ground_evaluated_at": current_time,
        "ground_freshness_policy_id": GROUND_FRESHNESS_POLICY_ID,
        "ground_local_radius_km": local_radius_km,
        "ground_station_count": len(local),
        "ground_input_observation_count": len(observations),
        "ground_rejected_observation_count": rejected_observation_count,
        "ground_raw_observation_count": len(assessed),
        "ground_physical_site_count": len(independent),
        "ground_duplicate_observation_count": (
            len(assessed) - len(independent)
        ),
        "ground_eligible_station_count": len(eligible),
        "ground_stale_station_count": sum(
            not item["fresh"] for item in independent
        ),
        "ground_raw_stale_observation_count": sum(
            not item["fresh"] for item in assessed
        ),
        "ground_raining_station_count": len(observed_raining),
        "ground_system_raining_station_count": len(local_raining),
        "ground_observed_raining_station_count": len(observed_raining),
        "ground_precipitation_observed": precipitation_observed,
        "ground_confirmed": confirmed,
        "ground_confirmation_score": round(raining_score, 3),
        "ground_confidence_percent": confidence,
        "ground_sources": ground_sources,
        "ground_raining_sources": raining_sources,
        "ground_matched_storm_ids": matched_storm_ids,
        "ground_confirmed_storm_id": (
            confirmed_storm_ids[0] if len(confirmed_storm_ids) == 1 else None
        ),
        "ground_confirmed_storm_ids": confirmed_storm_ids,
        "ground_confirmation_by_storm": confirmation_details,
        "ground_same_system_required": storm_filter_active,
        "ground_confirmation_basis": _confirmation_basis(
            confirmation_sources
        ),
        "ground_max_rain_mm_h": round(max(rain_values), 2) if rain_values else None,
        "ground_average_rain_mm_h": (
            round(sum(rain_values) / len(rain_values), 2) if rain_values else None
        ),
        "ground_assessment_collection": assessment_collection,
        "ground_data_age_seconds": (
            round(min(item["age"] for item in local), 1) if local else None
        ),
        "ground_assessments": serialized_assessments,
    }
    # Compatibiliteitslaag voor bestaande dashboards en automatiseringen.
    result.update({
        "netatmo_local_status": status,
        "netatmo_local_radius_km": local_radius_km,
        "netatmo_local_stations": len(local),
        "netatmo_local_raining_stations": len(observed_raining),
        "netatmo_local_confirmed": confirmed,
        "netatmo_local_max_rain_mm_h": result["ground_max_rain_mm_h"],
        "netatmo_local_average_rain_mm_h": result["ground_average_rain_mm_h"],
        "netatmo_data_age_seconds": result["ground_data_age_seconds"],
        "netatmo_nearest_station_id": (
            nearest_station["observation"].station_id if nearest_station else None
        ),
        "netatmo_nearest_station_distance_km": (
            round(nearest_station["distance"], 1) if nearest_station else None
        ),
        "netatmo_nearest_rain_station_id": (
            nearest_rain["observation"].station_id if nearest_rain else None
        ),
        "netatmo_nearest_rain_distance_km": (
            round(nearest_rain["distance"], 1) if nearest_rain else None
        ),
        "netatmo_nearest_rain_mm_h": (
            round(float(nearest_rain["observation"].rain_mm), 2)
            if nearest_rain
            and nearest_rain["observation"].rain_mm is not None
            else None
        ),
    })
    return result
