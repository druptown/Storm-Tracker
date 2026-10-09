"""Verliesloze radar-aanvulling voor regio's zonder officiële lokale radar."""
from __future__ import annotations

import math

AMERICAN_COUNTRY_CODES = frozenset({"US", "VS", "CA", "MX"})
MAX_SOURCE_AGE_SECONDS = 20 * 60
OVERLAP_DISTANCE_KM = 6.0


def _distance_km(first, second) -> float:
    lat1 = math.radians(float(first[0]))
    lat2 = math.radians(float(second[0]))
    dlat = lat2 - lat1
    dlon = math.radians(float(second[1]) - float(first[1]))
    value = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 6371.0 * 2.0 * math.asin(min(1.0, math.sqrt(value)))


def _points(observation) -> tuple[tuple[float, float], ...]:
    footprint = tuple(getattr(observation, "footprint_points", ()) or ())
    return ((float(observation.lat), float(observation.lon)), *footprint)


def _overlaps(observation, accepted) -> bool:
    candidate_points = _points(observation)
    for current in accepted:
        current_points = _points(current)
        if any(
            _distance_km(candidate, existing) <= OVERLAP_DISTANCE_KM
            for candidate in candidate_points
            for existing in current_points
        ):
            return True
    return False


def is_american_region(country_codes) -> bool:
    return bool({
        str(value).upper() for value in country_codes if value
    } & AMERICAN_COUNTRY_CODES)


def fuse_radar_observations(
    primary,
    auxiliaries,
    *,
    now: float,
) -> tuple[list, dict]:
    """Behoud bronmetingen met hun eigen tijd en vul onbedekte echo's aan.

    ``analysis_timestamp`` is alleen de referentietijd van deze fusieronde.
    Ze mag nooit teruggeschreven worden naar ``Observation.timestamp``:
    dat veld blijft de autoritatieve producttijd van de afzonderlijke bron.
    """
    sources = [list(primary), *(list(values) for values in auxiliaries)]
    fresh = [
        [
            item for item in values
            if 0 <= now - float(item.timestamp) <= MAX_SOURCE_AGE_SECONDS
        ]
        for values in sources
    ]
    available = [values for values in fresh if values]
    if not available:
        return [], {
            "active": False,
            "sources": [],
            "primary_observations": 0,
            "supplemental_observations": 0,
            "duplicates_suppressed": 0,
            "timestamp_policy": "preserve_source_product_time_v1",
            "analysis_timestamp": None,
            "fusion_evaluated_at": float(now),
            "source_product_timestamps": {},
            "contributing_product_timestamps": {},
        }

    analysis_timestamp = max(
        float(item.timestamp) for values in available for item in values
    )
    source_product_timestamps: dict[str, set[float]] = {}
    for values in available:
        for item in values:
            source_product_timestamps.setdefault(
                str(getattr(item, "source", None) or "unknown"), set()
            ).add(float(item.timestamp))

    # Behoud exact dezelfde objecten. Naast de producttijd blijven zo ook alle
    # toekomstige providerprovenancevelden byte-/waardegetrouw aanwezig.
    accepted = list(fresh[0])
    primary_count = len(accepted)
    suppressed = 0
    source_names = {
        str(item.source) for item in accepted if getattr(item, "source", None)
    }
    for values in fresh[1:]:
        for item in values:
            if _overlaps(item, accepted):
                suppressed += 1
                continue
            accepted.append(item)
            if getattr(item, "source", None):
                source_names.add(str(item.source))

    contributing_product_timestamps: dict[str, set[float]] = {}
    for item in accepted:
        contributing_product_timestamps.setdefault(
            str(getattr(item, "source", None) or "unknown"), set()
        ).add(float(item.timestamp))

    return accepted, {
        "active": len(source_names) > 1,
        "sources": sorted(source_names),
        "primary_observations": primary_count,
        "supplemental_observations": len(accepted) - primary_count,
        "duplicates_suppressed": suppressed,
        "timestamp_policy": "preserve_source_product_time_v1",
        "analysis_timestamp": analysis_timestamp,
        "fusion_evaluated_at": float(now),
        "source_product_timestamps": {
            source: sorted(timestamps)
            for source, timestamps in sorted(
                source_product_timestamps.items()
            )
        },
        "contributing_product_timestamps": {
            source: sorted(timestamps)
            for source, timestamps in sorted(
                contributing_product_timestamps.items()
            )
        },
    }
