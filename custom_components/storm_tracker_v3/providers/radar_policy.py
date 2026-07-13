"""Runtime policy for selecting exactly one operational radar source."""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable, Sequence


OPERA_MIN_STANDALONE_QUALITY = 0.5
OPERA_CORROBORATION_RADIUS_KM = 25.0
OPERA_CORROBORATION_MAX_AGE_S = 15 * 60


@dataclass(frozen=True, slots=True)
class RadarDecision:
    source: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class OperaVerification:
    """Result of validating OPERA cells against quality and local radar."""

    accepted: tuple
    high_quality: int
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
    high_quality = corroborated = rejected = 0

    for obs in observations:
        quality = getattr(obs, "quality", None)
        if quality is not None and quality >= min_quality:
            accepted.append(obs)
            high_quality += 1
            continue

        confirmed = any(
            abs(float(obs.timestamp) - float(ref.timestamp)) <= max_age_s
            and _haversine_km(obs.lat, obs.lon, ref.lat, ref.lon) <= radius_km
            for ref in references
        )
        if confirmed:
            accepted.append(obs)
            corroborated += 1
        else:
            rejected += 1

    return OperaVerification(tuple(accepted), high_quality, corroborated, rejected)


def select_radar_source(*, opera_configured: bool, opera_healthy: bool,
                        rainviewer_configured: bool) -> RadarDecision:
    """Choose one source. Comparison providers never participate here."""
    if opera_configured and opera_healthy:
        return RadarDecision("opera", "OPERA product is fresh and parsed successfully")
    if rainviewer_configured:
        reason = "OPERA unavailable or stale" if opera_configured else "outside OPERA coverage"
        return RadarDecision("rainviewer", reason)
    return RadarDecision(None, "no healthy radar provider available")
