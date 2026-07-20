"""Runtime policy for selecting exactly one operational radar source."""
from __future__ import annotations

from dataclasses import dataclass
from dataclasses import is_dataclass, replace
from copy import copy
import math
from typing import Iterable, Sequence


OPERA_MIN_STANDALONE_QUALITY = 0.5
OPERA_CORROBORATION_RADIUS_KM = 12.0
OPERA_CORROBORATION_MAX_AGE_S = 15 * 60
OPERA_MIN_STRUCTURED_MEAN_DBZ = 20.0
OPERA_MIN_STRUCTURED_MAX_DBZ = 30.0
OPERA_MIN_STRUCTURED_AREA_KM2 = 50.0


def _replace_observation(observation, **changes):
    """Replace production dataclasses while keeping test doubles supported."""
    if is_dataclass(observation):
        return replace(observation, **changes)
    cloned = copy(observation)
    for key, value in changes.items():
        setattr(cloned, key, value)
    return cloned


def usable_corroborating_observations(observations: Iterable) -> tuple:
    """Selecteer alleen bronnen/pixels die werkelijk neerslag aantonen.

    KMI's product bevat een ingetekende groene basiskaart die als intensiteit 1
    kan worden gedecodeerd. KNMI's opaak-witte WMS-achtergrond heeft hetzelfde
    probleem. Vanaf intensiteit 2 tonen beide parsers een echte radarkleur en
    mogen ze, net als RainViewer, een nabije OPERA-echo bevestigen.
    """
    usable = []
    for obs in observations:
        source = getattr(obs, "source", "")
        intensity = getattr(obs, "intensity", None)
        minimum = 2 if source in {"kmi", "knmi", "rainviewer"} else 1
        if source in {
            "kmi", "knmi", "rainviewer", "dwd_radolan",
            "met_office_radar", "meteofrance_radar",
            "dpc_radar",
            "aemet_radar",
        } and (
            intensity is not None and intensity >= minimum
        ):
            usable.append(obs)
    return tuple(usable)


def corroboration_source_counts(observations: Iterable) -> dict[str, int]:
    """Tel de werkelijk bruikbare vergelijkingspunten per radarbron."""
    supported = {
        "kmi", "knmi", "rainviewer", "dwd_radolan",
        "met_office_radar", "meteofrance_radar", "dpc_radar", "aemet_radar",
    }
    counts: dict[str, int] = {}
    for observation in observations:
        source = getattr(observation, "source", "")
        if source in supported:
            counts[source] = counts.get(source, 0) + 1
    return counts


@dataclass(frozen=True, slots=True)
class RadarDecision:
    source: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class OperaVerification:
    """Result of validating OPERA cells against quality and local radar."""

    accepted: tuple
    high_quality: int
    structured_echo: int
    corroborated: int
    rejected: int


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 6371.0088 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _within_radius_km(
    lat1: float, lon1: float, lat2: float, lon2: float, radius_km: float
) -> bool:
    """Reject distant points cheaply before evaluating great-circle distance."""
    latitude_margin = radius_km / 110.574
    if abs(lat2 - lat1) > latitude_margin:
        return False
    longitude_margin = radius_km / (
        111.320 * max(0.1, abs(math.cos(math.radians((lat1 + lat2) / 2))))
    )
    longitude_delta = abs((lon2 - lon1 + 180.0) % 360.0 - 180.0)
    if longitude_delta > longitude_margin:
        return False
    return _haversine_km(lat1, lon1, lat2, lon2) <= radius_km


def verify_opera_observations(
    observations: Sequence,
    corroborating: Iterable,
    *,
    min_quality: float = OPERA_MIN_STANDALONE_QUALITY,
    radius_km: float = OPERA_CORROBORATION_RADIUS_KM,
    max_age_s: float = OPERA_CORROBORATION_MAX_AGE_S,
) -> OperaVerification:
    """Accept trustworthy OPERA cells or cells confirmed by another radar.

    OPERA's composite can contain strong but low-quality clutter.  Quality is
    therefore sufficient, but not required: a nearby, recent national-radar
    observation also confirms the cell. Forecast observations are excluded by
    the caller; this function remains provider-agnostic and easy to test.
    """
    references = tuple(corroborating)
    accepted = []
    high_quality = structured_echo = corroborated = rejected = 0

    for obs in observations:
        quality = getattr(obs, "quality", None)
        if quality is not None and quality >= min_quality:
            accepted.append(obs)
            high_quality += 1
            continue

        # qi_total is aanvullende kwaliteitsinformatie, geen regen/geen-regen
        # vlag. Een ruimtelijk substantieel gebied met zowel een hoge
        # gemiddelde als piekreflectiviteit is zelfstandig meteorologisch
        # plausibel. Dit laat echte Franse regenbanden door, maar niet de
        # zwakke Belgische echo (gemiddeld circa 12-14 dBZ) die de bug toonde.
        mean_dbz = getattr(obs, "mean_dbz", None)
        max_dbz = getattr(obs, "max_dbz", None)
        area_km2 = getattr(obs, "area_km2", None)
        structured = (
            mean_dbz is not None
            and max_dbz is not None
            and area_km2 is not None
            and float(mean_dbz) >= OPERA_MIN_STRUCTURED_MEAN_DBZ
            and float(max_dbz) >= OPERA_MIN_STRUCTURED_MAX_DBZ
            and float(area_km2) >= OPERA_MIN_STRUCTURED_AREA_KM2
        )

        # Grote of langgerekte cellen kunnen een centroid hebben dat ver van
        # de werkelijk bevestigde regen ligt. Vergelijk daarom ook met de
        # compacte footprint van werkelijk bezette OPERA-rasterpixels.
        footprint = tuple(getattr(obs, "footprint_points", ()) or ())
        candidate_points = footprint or ((obs.lat, obs.lon),)
        recent_references = tuple(
            ref for ref in references
            if abs(float(obs.timestamp) - float(ref.timestamp)) <= max_age_s
        )
        confirmed_points = tuple(
            (lat, lon) for lat, lon in candidate_points
            if any(
                _within_radius_km(lat, lon, ref.lat, ref.lon, radius_km)
                for ref in recent_references
            )
        )
        if confirmed_points:
            confirmed_obs = obs
            if footprint:
                # Bewaar alleen het deel van een lage-kwaliteit OPERA-cel dat
                # werkelijk door de onafhankelijke radar wordt gedekt. Eén
                # echte bui kan zo geen volledige foutieve megacel bevestigen.
                coverage = len(confirmed_points) / len(footprint)
                confirmed_obs = _replace_observation(
                    obs,
                    lat=sum(point[0] for point in confirmed_points) / len(confirmed_points),
                    lon=sum(point[1] for point in confirmed_points) / len(confirmed_points),
                    area_km2=(
                        float(area_km2) * coverage
                        if area_km2 is not None else None
                    ),
                    footprint_points=confirmed_points,
                    parent_area_km2=(
                        float(getattr(obs, "parent_area_km2")) * coverage
                        if getattr(obs, "parent_area_km2", None) is not None
                        else None
                    ),
                    parent_footprint_points=confirmed_points,
                )
            accepted.append(confirmed_obs)
            if structured:
                structured_echo += 1
            else:
                corroborated += 1
        else:
            rejected += 1

    return OperaVerification(
        tuple(accepted), high_quality, structured_echo, corroborated, rejected
    )


def select_radar_source(*, opera_configured: bool, opera_healthy: bool,
                        rainviewer_configured: bool,
                        rainviewer_healthy: bool) -> RadarDecision:
    """Choose one source. Comparison providers never participate here."""
    if opera_configured and opera_healthy:
        return RadarDecision("opera", "OPERA product is fresh and parsed successfully")
    if rainviewer_configured and rainviewer_healthy:
        reason = "OPERA unavailable or stale" if opera_configured else "outside OPERA coverage"
        return RadarDecision("rainviewer", reason)
    if rainviewer_configured:
        return RadarDecision(None, "RainViewer unavailable or stale")
    return RadarDecision(None, "no healthy radar provider available")
