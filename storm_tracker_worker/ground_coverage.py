"""Low-frequency worldwide ground-network coverage survey."""
from __future__ import annotations

from datetime import datetime, timezone
import time

from .netatmo import _NETATMO
from .synoptic import _query as synoptic_query, _observations as synoptic_observations

SURVEY_RADIUS_KM = 100.0

# Countries already supported by provider_policy.json. Coordinates are the
# national capitals and intentionally static so surveys remain comparable.
CAPITALS = {
    "AT": ("Wenen", 48.2082, 16.3738),
    "AU": ("Canberra", -35.2809, 149.1300),
    "BE": ("Brussel", 50.8503, 4.3517),
    "CA": ("Ottawa", 45.4215, -75.6972),
    "CO": ("Bogotá", 4.7110, -74.0721),
    "DE": ("Berlijn", 52.5200, 13.4050),
    "ES": ("Madrid", 40.4168, -3.7038),
    "FR": ("Parijs", 48.8566, 2.3522),
    "GB": ("Londen", 51.5074, -0.1278),
    "IT": ("Rome", 41.9028, 12.4964),
    "JP": ("Tokio", 35.6762, 139.6503),
    "LU": ("Luxemburg", 49.6116, 6.1319),
    "NL": ("Amsterdam", 52.3676, 4.9041),
    "TW": ("Taipei", 25.0330, 121.5654),
    "US": ("Washington D.C.", 38.9072, -77.0369),
}


def _summary(source: str, observations: list[dict]) -> dict:
    rain_capable = [
        item for item in observations if item.get("rain_capable", True)
    ]
    wet = [
        item for item in rain_capable
        if float(item.get("rain_mm") or 0.0) >= 0.1
    ]
    count = len(rain_capable)
    quality = (
        "goed" if count >= 20
        else "beperkt" if count >= 5
        else "onvoldoende"
    )
    return {
        "source": source,
        "station_count": len(observations),
        "rain_capable_station_count": count,
        "wet_station_count": len(wet),
        "coverage_quality": quality,
    }


def survey(payload: dict) -> dict:
    """Survey every supported capital without returning station identities."""
    _NETATMO._validate_credential_reference(payload)
    forbidden_synoptic_keys = {
        "synoptic_token",
        "synoptic_api_token",
    }.intersection(payload)
    if forbidden_synoptic_keys:
        raise ValueError(
            "Synoptic-credentials mogen niet in een workerjobpayload staan"
        )
    credential_ref = payload.get("credential_ref")
    if credential_ref not in {None, "worker_environment"}:
        raise ValueError("onbekende ground-survey credential reference")
    now = datetime.now(timezone.utc).timestamp()
    netatmo_token = _NETATMO._token(now) if _NETATMO.configured() else None
    import os
    synoptic_token = str(os.getenv("STV3_SYNOPTIC_TOKEN", ""))

    countries = []
    for country_code, (capital, latitude, longitude) in CAPITALS.items():
        sources = []
        errors = {}
        if netatmo_token:
            try:
                sources.append(_summary(
                    "netatmo",
                    _NETATMO.query_radius(
                        netatmo_token,
                        latitude,
                        longitude,
                        SURVEY_RADIUS_KM,
                    ),
                ))
            except Exception as err:
                errors["netatmo"] = f"{type(err).__name__}: {err}"
        if synoptic_token:
            try:
                raw = synoptic_query(
                    latitude,
                    longitude,
                    radius_km=SURVEY_RADIUS_KM,
                    token=synoptic_token,
                    limit=1000,
                )
                sources.append(_summary(
                    "synoptic", synoptic_observations(raw, now)
                ))
            except RuntimeError as err:
                if "Synoptic API-fout 2" in str(err):
                    sources.append(_summary("synoptic", []))
                else:
                    errors["synoptic"] = f"{type(err).__name__}: {err}"
            except Exception as err:
                errors["synoptic"] = f"{type(err).__name__}: {err}"
        usable = [
            item for item in sources
            if item["coverage_quality"] != "onvoldoende"
        ]
        selected = [item["source"] for item in usable]
        countries.append({
            "country_code": country_code,
            "capital": capital,
            "latitude": latitude,
            "longitude": longitude,
            "radius_km": SURVEY_RADIUS_KM,
            "sources": sources,
            "selected_sources": selected,
            "selection": (
                " + ".join(selected) if selected else "niet_verifieerbaar"
            ),
            "errors": errors,
        })
    return {
        "method": "capital_ground_coverage_v1",
        "surveyed_at": time.time(),
        "radius_km": SURVEY_RADIUS_KM,
        "country_count": len(countries),
        "countries": countries,
    }


def ground_coverage_survey(job) -> dict:
    return survey(job.payload)
