"""Deterministische radarbronkeuze per dynamische RegionEngine."""
from __future__ import annotations

from dataclasses import dataclass

LOCAL_RADAR_BY_COUNTRY = {
    "BE": "kmi",
    "NL": "knmi",
    "DE": "dwd_radolan",
    "FR": "meteofrance_radar",
    "GB": "met_office_radar",
    "IT": "dpc_radar",
    "ES": "aemet_radar",
}


@dataclass(frozen=True, slots=True)
class SourceState:
    configured: bool
    healthy: bool
    last_success: float | None = None


@dataclass(frozen=True, slots=True)
class EngineRadarDecision:
    source: str | None
    reason: str
    country_codes: tuple[str, ...]
    age_seconds: float | None


def select_engine_radar_source(
    country_codes, states: dict[str, SourceState], *, now: float
) -> EngineRadarDecision:
    countries = tuple(sorted({str(code).upper() for code in country_codes if code}))
    local_sources = {LOCAL_RADAR_BY_COUNTRY[code] for code in countries if code in LOCAL_RADAR_BY_COUNTRY}
    if len(local_sources) == 1:
        source = next(iter(local_sources))
        state = states.get(source, SourceState(False, False))
        age = now - state.last_success if state.last_success is not None else None
        if state.configured and state.healthy:
            return EngineRadarDecision(source, f"lokale officiële radar voor {','.join(countries)}", countries, age)
        local_reason = f"lokale bron {source} niet gezond of niet geconfigureerd"
    elif len(local_sources) > 1:
        local_reason = "gedeelde engine overspant meerdere nationale radargebieden"
    else:
        local_reason = "geen operationele lokale realtime radar"

    opera = states.get("opera", SourceState(False, False))
    if opera.configured and opera.healthy:
        age = now - opera.last_success if opera.last_success is not None else None
        return EngineRadarDecision("opera", f"{local_reason}; OPERA fallback", countries, age)
    rainviewer = states.get("rainviewer", SourceState(False, False))
    if rainviewer.configured and rainviewer.healthy:
        age = now - rainviewer.last_success if rainviewer.last_success is not None else None
        return EngineRadarDecision("rainviewer", f"{local_reason}; OPERA niet beschikbaar", countries, age)
    return EngineRadarDecision(None, f"{local_reason}; geen gezonde fallback", countries, None)
