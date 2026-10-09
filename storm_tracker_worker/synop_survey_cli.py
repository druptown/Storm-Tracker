"""Isolated international SYNOP coverage survey using DWD open data."""
from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import io
import json
import re
import sys
import urllib.request

from .metar_survey_cli import RADIUS_KM, _distance_km, _download

DWD_INDEX_URL = (
    "https://opendata.dwd.de/weather/weather_reports/synoptic/international/"
)
ISD_HISTORY_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
WINDOW_MINUTES = 150


def _recent_file_names(index_html: str, now: datetime) -> list[str]:
    earliest = now - timedelta(minutes=WINDOW_MINUTES)
    result = []
    for name in set(re.findall(
        r"gda01-synop-(\d{12})\.txt",
        index_html,
    )):
        try:
            timestamp = datetime.strptime(name, "%Y%m%d%H%M").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            continue
        if earliest <= timestamp <= now + timedelta(minutes=10):
            result.append(f"gda01-synop-{name}.txt")
    return sorted(result)


def _land_station_ids(text: str) -> set[str]:
    """Extract five-digit WMO station identifiers from AAXX sections."""
    markers = list(re.finditer(r"(?m)^(AAXX|BBXX|OOXX)\b", text))
    result: set[str] = set()
    for index, marker in enumerate(markers):
        if marker.group(1) != "AAXX":
            continue
        end = markers[index + 1].start() if index + 1 < len(markers) else len(text)
        section = text[marker.end():end]
        result.update(re.findall(
            r"(?m)^(\d{5})\s+[0-9/]{5}(?:\s|$)",
            section,
        ))
    return result


def _station_metadata(history_csv: str, active_ids: set[str]) -> list[dict]:
    by_wmo: dict[str, dict] = {}
    for row in csv.DictReader(io.StringIO(history_csv)):
        usaf = str(row.get("USAF") or "")
        if len(usaf) != 6 or usaf[:5] not in active_ids:
            continue
        try:
            latitude = float(row["LAT"])
            longitude = float(row["LON"])
        except (KeyError, TypeError, ValueError):
            continue
        if latitude == 0.0 and longitude == 0.0:
            continue
        candidate = {
            "station_id": usaf[:5],
            "name": str(row.get("STATION NAME") or ""),
            "latitude": latitude,
            "longitude": longitude,
        }
        previous = by_wmo.get(usaf[:5])
        if previous is None or str(row.get("END") or "") > previous.get("_end", ""):
            candidate["_end"] = str(row.get("END") or "")
            by_wmo[usaf[:5]] = candidate
    for item in by_wmo.values():
        item.pop("_end", None)
    return list(by_wmo.values())


def _probe_country(country: dict, stations: list[dict]) -> dict:
    cities = []
    all_station_ids: set[str] = set()
    for city in country.get("cities", []):
        nearby = []
        for station in stations:
            distance = _distance_km(
                city["latitude"],
                city["longitude"],
                station["latitude"],
                station["longitude"],
            )
            if distance <= RADIUS_KM:
                nearby.append({
                    **station,
                    "distance_km": round(distance, 1),
                })
                all_station_ids.add(station["station_id"])
        nearby.sort(key=lambda item: item["distance_km"])
        cities.append({
            "name": city["name"],
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "station_count": len(nearby),
            "stations": nearby,
        })
    return {
        "country_code": country["country_code"],
        "cities_tested": len(cities),
        "station_count": len(all_station_ids),
        "selection": "synop" if len(all_station_ids) >= 2 else "niet_verifieerbaar",
        "cities": cities,
    }


def run(
    baseline: dict,
    *,
    index_html: str | None = None,
    bulletins: list[str] | None = None,
    history_csv: str | None = None,
    now: datetime | None = None,
) -> dict:
    now = now or datetime.now(timezone.utc)
    if index_html is None:
        index_html = _download(DWD_INDEX_URL).decode("utf-8", "replace")
    file_names = _recent_file_names(index_html, now)
    if bulletins is None:
        bulletins = [
            _download(DWD_INDEX_URL + name).decode("latin-1", "replace")
            for name in file_names
        ]
    active_ids: set[str] = set()
    for bulletin in bulletins:
        active_ids.update(_land_station_ids(bulletin))
    if history_csv is None:
        history_csv = _download(ISD_HISTORY_URL).decode("utf-8-sig", "replace")
    stations = _station_metadata(history_csv, active_ids)
    countries = [
        _probe_country(country, stations)
        for country in baseline.get("countries", [])
        if country.get("selection") == "niet_verifieerbaar"
    ]
    return {
        "method": "international_synop_ground_coverage_v1",
        "source": "DWD international SYNOP open data",
        "surveyed_at": now.timestamp(),
        "window_minutes": WINDOW_MINUTES,
        "radius_km": RADIUS_KM,
        "bulletin_count": len(file_names),
        "active_synop_id_count": len(active_ids),
        "mapped_station_count": len(stations),
        "countries": countries,
    }


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("gebruik: synop_survey_cli METAR_WIDE_JSON")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    with open(sys.argv[1], encoding="utf-8-sig") as source:
        baseline = json.load(source)
    print(json.dumps(
        run(baseline),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
