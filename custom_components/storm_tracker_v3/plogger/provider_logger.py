"""Storm Tracker V3 — plogger/provider_logger.py v0.2.0

Dedicated CSV logging voor alle providers.
Alle schrijfoperaties via hass.async_add_executor_job — non-blocking.
Roteert automatisch na 7 dagen.

Versiegeschiedenis:
  v0.2.0 — non-blocking via async_add_executor_job
  v0.1.0 — eerste versie (blocking I/O in event loop)
"""
from __future__ import annotations

import csv
import logging
from datetime import datetime
from pathlib import Path

_LOGGER = logging.getLogger(__name__)

LOG_DIR     = Path("/config/storm_tracker_v3_logs")
ROTATE_DAYS = 7


def _ensure_dir() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def _rotate_if_needed(path: Path) -> None:
    if not path.exists():
        return
    age = datetime.now().timestamp() - path.stat().st_mtime
    if age > ROTATE_DAYS * 86400:
        archive = path.with_suffix(f".{datetime.now().strftime('%Y%m%d')}.csv")
        path.rename(archive)
        _LOGGER.info("ProviderLogger: %s → %s", path.name, archive.name)


def _write_row_sync(filename: str, row: dict) -> None:
    """Synchrone schrijfoperatie — altijd via executor aanroepen."""
    _ensure_dir()
    path    = LOG_DIR / filename
    _rotate_if_needed(path)
    is_new  = not path.exists()
    try:
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(row.keys()))
            if is_new:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        _LOGGER.warning("ProviderLogger: schrijffout %s: %s", filename, e)


def _log(hass, filename: str, row: dict) -> None:
    """Schrijf via executor — volledig non-blocking."""
    hass.async_add_executor_job(_write_row_sync, filename, row)


def log_lightning(hass, lat: float, lon: float, timestamp: float) -> None:
    _log(hass, "blitzortung.csv", {
        "timestamp": datetime.fromtimestamp(timestamp).isoformat(),
        "lat":       round(lat, 5),
        "lon":       round(lon, 5),
    })


def log_kmi(hass, obs_list: list, tracker_lat: float, tracker_lon: float) -> None:
    intensities = [o.intensity for o in obs_list if o.intensity]
    _log(hass, "kmi.csv", {
        "timestamp":       datetime.now().isoformat(timespec="seconds"),
        "tracker_lat":     round(tracker_lat, 4),
        "tracker_lon":     round(tracker_lon, 4),
        "observaties":     len(obs_list),
        "max_intensiteit": max(intensities) if intensities else 0,
        "gem_intensiteit": round(sum(intensities) / len(intensities), 2) if intensities else 0,
    })


def log_rainviewer(hass, obs_list: list, tracker_lat: float, tracker_lon: float) -> None:
    intensities = [o.intensity for o in obs_list if o.intensity]
    _log(hass, "rainviewer.csv", {
        "timestamp":       datetime.now().isoformat(timespec="seconds"),
        "tracker_lat":     round(tracker_lat, 4),
        "tracker_lon":     round(tracker_lon, 4),
        "observaties":     len(obs_list),
        "max_intensiteit": max(intensities) if intensities else 0,
        "gem_intensiteit": round(sum(intensities) / len(intensities), 2) if intensities else 0,
    })


def log_knmi(
    hass,
    current: list, forecast: list,
    intensity_now: int, i30: int, i60: int, i120: int,
    tracker_lat: float, tracker_lon: float,
) -> None:
    _log(hass, "knmi.csv", {
        "timestamp":        datetime.now().isoformat(timespec="seconds"),
        "tracker_lat":      round(tracker_lat, 4),
        "tracker_lon":      round(tracker_lon, 4),
        "huidig_pixels":    len(current),
        "nowcast_pixels":   len(forecast),
        "intensiteit_nu":   intensity_now,
        "intensiteit_30m":  i30,
        "intensiteit_60m":  i60,
        "intensiteit_120m": i120,
    })


def log_netatmo(hass, obs_list: list, tracker_lat: float, tracker_lon: float) -> None:
    raining   = [o for o in obs_list if (o.rain_mm or 0) >= 0.1]
    rain_vals = [o.rain_mm for o in raining if o.rain_mm]
    _log(hass, "netatmo.csv", {
        "timestamp":       datetime.now().isoformat(timespec="seconds"),
        "tracker_lat":     round(tracker_lat, 4),
        "tracker_lon":     round(tracker_lon, 4),
        "totaal_stations": len(obs_list),
        "natte_stations":  len(raining),
        "max_regen_mm":    round(max(rain_vals), 2) if rain_vals else 0,
        "gem_regen_mm":    round(sum(rain_vals) / len(rain_vals), 2) if rain_vals else 0,
    })


def log_open_meteo(
    hass,
    result: dict,
    tracker_lat: float,
    tracker_lon: float,
    *,
    target_id: str = "unknown",
) -> None:
    import json as _json
    # Nieuw doelgericht schema in een nieuw bestand; zo blijven historische
    # 324-puntengridregels correct leesbaar in open_meteo.csv.
    _log(hass, "open_meteo_targets.csv", {
        "timestamp":              datetime.now().isoformat(timespec="seconds"),
        "target_id":              target_id,
        "tracker_lat":            round(tracker_lat, 4),
        "tracker_lon":            round(tracker_lon, 4),
        "provider_status":        result.get("provider_status"),
        "fetch_sequence":         result.get("fetch_sequence"),
        "last_success_at":        result.get("last_success_at"),
        "neerslag_nu_mm":         result.get("current_precipitation_mm"),
        "max_neerslag_90min_mm":  result.get("forecast_90m_max_mm"),
        "totaal_90min_mm":        result.get("forecast_90m_total_mm"),
        "natte_stappen_90min":    result.get("forecast_90m_wet_steps"),
        "eerste_regen_minuten":   result.get("forecast_90m_first_wet_minutes"),
        "model_lat":              result.get("model_latitude"),
        "model_lon":              result.get("model_longitude"),
        "kwartierwaarden": _json.dumps(
            result.get("forecast_90m_precipitation_mm", [])
        ),
    })
