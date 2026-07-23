"""Persistente, richtingsgebonden bronprofielen voor veilige radarwissels.

Een profiel van KMI naar OPERA is bewust niet hetzelfde als een profiel van
OPERA naar KMI. De ruwe radarwaarden of kaartpixels worden hier niet vervormd:
de eerste operationele toepassing is een datagedreven onzekerheidsmarge tijdens
de korte overgang na een providerwissel.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


DEFAULT_TRANSITION_PENALTY_PERCENT = 10
DEFAULT_TRANSITION_WINDOW_SECONDS = 10 * 60
MIN_OPERATIONAL_PROFILE_SAMPLES = 12


def scope_from_region_key(region_key: str) -> str:
    """Verwijder het vluchtige engine-ID en behoud de geografische sleutel."""
    value = str(region_key)
    return value.split("@", 1)[1] if "@" in value else value


def profile_confidence(sample_count: int) -> str:
    """Classificeer uitsluitend op onafhankelijke, vergelijkbare natte frames."""
    count = max(0, int(sample_count))
    if count < MIN_OPERATIONAL_PROFILE_SAMPLES:
        return "insufficient"
    if count < 30:
        return "low"
    if count < 100:
        return "medium"
    return "high"


def build_profile_index(
    profiles: Iterable[Mapping[str, Any]],
) -> dict[tuple[str, str, str], dict[str, Any]]:
    """Maak een goedkope runtime-index van serialiseerbare databaserijen."""
    return {
        (
            str(profile["scope_key"]),
            str(profile["from_source"]),
            str(profile["to_source"]),
        ): dict(profile)
        for profile in profiles
    }


def select_transition_profile(
    index: Mapping[tuple[str, str, str], Mapping[str, Any]],
    *,
    region_key: str,
    from_source: str,
    to_source: str,
) -> dict[str, Any] | None:
    """Kies eerst de exacte regio en daarna het globale bronpaarprofiel."""
    scope = scope_from_region_key(region_key)
    exact = index.get((scope, str(from_source), str(to_source)))
    if exact is not None:
        return dict(exact)
    fallback = index.get(("*", str(from_source), str(to_source)))
    return dict(fallback) if fallback is not None else None


def transition_adjustment(
    profile: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Vertaal een historisch profiel naar een conservatieve overgangsmarge.

    Zonder voldoende historie blijft het bestaande veilige gedrag van tien
    procentpunten gedurende tien minuten exact behouden. Alleen aantoonbaar
    goed gevulde profielen mogen de marge verkorten; slechte overeenkomst kan
    de marge juist vergroten.
    """
    if profile is None:
        return {
            "profile_available": False,
            "profile_confidence": "insufficient",
            "profile_samples": 0,
            "confidence_penalty_percent": DEFAULT_TRANSITION_PENALTY_PERCENT,
            "transition_window_seconds": DEFAULT_TRANSITION_WINDOW_SECONDS,
            "application": "confidence_only",
        }

    samples = int(profile.get("sample_count") or 0)
    confidence = str(
        profile.get("confidence") or profile_confidence(samples)
    )
    f1 = profile.get("mean_f1_score")
    detection = profile.get("mean_detection_ratio")
    agreement = f1 if f1 is not None else detection
    if samples < MIN_OPERATIONAL_PROFILE_SAMPLES or agreement is None:
        penalty = DEFAULT_TRANSITION_PENALTY_PERCENT
        window = DEFAULT_TRANSITION_WINDOW_SECONDS
    else:
        agreement = max(0.0, min(1.0, float(agreement)))
        # 3..13 procentpunten: een bekende, goed overeenkomende overgang
        # verdient minder straf; een structureel afwijkende overgang meer.
        penalty = round(max(3.0, min(13.0, 3.0 + (1.0 - agreement) * 10.0)))
        if confidence == "high" and agreement >= 0.75:
            window = 5 * 60
        elif confidence in {"medium", "high"} and agreement >= 0.55:
            window = 7 * 60
        else:
            window = DEFAULT_TRANSITION_WINDOW_SECONDS

    return {
        "profile_available": samples >= MIN_OPERATIONAL_PROFILE_SAMPLES,
        "profile_scope": profile.get("scope_key"),
        "profile_confidence": confidence,
        "profile_samples": samples,
        "profile_mean_f1_score": profile.get("mean_f1_score"),
        "profile_detection_ratio": profile.get("mean_detection_ratio"),
        "profile_extra_fraction": profile.get("mean_extra_fraction"),
        "profile_wet_area_ratio": profile.get("mean_wet_area_ratio"),
        "profile_intensity_bias": profile.get("mean_intensity_bias"),
        "profile_shift_lat_cells": profile.get("mean_shift_lat_cells"),
        "profile_shift_lon_cells": profile.get("mean_shift_lon_cells"),
        "profile_latency_seconds": profile.get("mean_latency_seconds"),
        "confidence_penalty_percent": int(penalty),
        "transition_window_seconds": int(window),
        "application": "confidence_only",
    }
