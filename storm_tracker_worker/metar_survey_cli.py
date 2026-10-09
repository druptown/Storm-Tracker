"""Broad, isolated METAR coverage survey for weak ground-network regions."""
from __future__ import annotations

from collections import defaultdict
import csv
from datetime import datetime, timezone
import io
import json
import math
import os
import sys
import time
import urllib.request
import zipfile

from .city_survey_cli import (
    GEONAMES_URL,
    MAX_GEONAMES_ARCHIVE_BYTES,
    MAX_GEONAMES_MEMBER_BYTES,
)
from .tracking_core.engine.bounded_payload import (
    decompress_limited,
    read_limited_stream,
)

METAR_CACHE_URL = (
    "https://aviationweather.gov/data/cache/metars.cache.csv.gz"
)
RADIUS_KM = float(os.getenv("STV3_METAR_SURVEY_RADIUS_KM", "150"))
CITIES_PER_COUNTRY = int(os.getenv("STV3_METAR_SURVEY_CITY_COUNT", "8"))
MAX_OBSERVATION_AGE_MINUTES = 120
MAX_METAR_CACHE_BYTES = 16 * 1024 * 1024
MAX_METAR_UNCOMPRESSED_BYTES = 64 * 1024 * 1024

# Non-sovereign territories and very remote micro-islands where maintaining a
# dedicated Storm Tracker ground-network policy has little practical value.
# The list is explicit so a later survey can include any entry deliberately.
EXCLUDED_REMOTE_CODES = {
    "AI", "AQ", "AS", "AW", "AX", "BL", "BM", "BQ", "BV", "CC", "CK",
    "CW", "CX", "EH", "FK", "FO", "GF", "GG", "GI", "GL", "GP", "GS",
    "GU", "HK", "HM", "IM", "IO", "JE", "KI", "KY", "MF", "MH", "MO",
    "MP", "MQ", "MS", "NC", "NF", "NR", "NU", "PF", "PM", "PN", "PR",
    "PW", "RE", "SH", "SJ", "SX", "TC", "TF", "TK", "TO", "TV", "UM",
    "VG", "VI", "WF", "WS", "YT",
}

PRECIPITATION_TOKENS = {
    "DZ", "RA", "SN", "SG", "IC", "PL", "GR", "GS", "UP", "SH", "TS",
    "FZRA", "FZDZ",
}


def _download(url: str, *, maximum_bytes: int) -> bytes:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "StormTrackerV3-CoverageSurvey/1.0"},
    )
    with urllib.request.urlopen(request, timeout=90) as response:
        return read_limited_stream(
            response,
            maximum_bytes=maximum_bytes,
            declared_bytes=response.headers.get("Content-Length"),
            require_declared_match=True,
        )


def _largest_cities(
    country_codes: set[str],
    *,
    archive_bytes: bytes | None = None,
) -> dict[str, list[dict]]:
    """Return geographically useful population centres per country."""
    payload = (
        archive_bytes
        if archive_bytes is not None
        else _download(
            GEONAMES_URL,
            maximum_bytes=MAX_GEONAMES_ARCHIVE_BYTES,
        )
    )
    if len(payload) > MAX_GEONAMES_ARCHIVE_BYTES:
        raise ValueError("GeoNames-archief overschrijdt contractlimiet")
    archive = zipfile.ZipFile(io.BytesIO(payload))
    member = archive.getinfo("cities15000.txt")
    if member.file_size > MAX_GEONAMES_MEMBER_BYTES:
        raise ValueError("GeoNames-member overschrijdt contractlimiet")
    grouped: dict[str, list[dict]] = defaultdict(list)
    with archive.open("cities15000.txt") as source:
        for raw in io.TextIOWrapper(source, encoding="utf-8"):
            fields = raw.rstrip("\n").split("\t")
            if len(fields) < 15 or fields[8] not in country_codes:
                continue
            try:
                grouped[fields[8]].append({
                    "name": fields[1],
                    "latitude": float(fields[4]),
                    "longitude": float(fields[5]),
                    "population": int(fields[14] or 0),
                })
            except ValueError:
                continue
    return {
        code: sorted(
            values,
            key=lambda item: item["population"],
            reverse=True,
        )[:CITIES_PER_COUNTRY]
        for code, values in grouped.items()
    }


def _parse_time(value: str) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        ).timestamp()
    except ValueError:
        return None


def _metar_observations(
    *,
    cache_bytes: bytes | None = None,
    now: float | None = None,
) -> list[dict]:
    """Decode current worldwide METAR observations from one cache request."""
    now = now if now is not None else datetime.now(timezone.utc).timestamp()
    compressed = (
        cache_bytes
        if cache_bytes is not None
        else _download(
            METAR_CACHE_URL,
            maximum_bytes=MAX_METAR_CACHE_BYTES,
        )
    )
    text = decompress_limited(
        compressed,
        codec="gzip",
        maximum_compressed_bytes=MAX_METAR_CACHE_BYTES,
        maximum_uncompressed_bytes=MAX_METAR_UNCOMPRESSED_BYTES,
    ).decode("utf-8-sig", "replace")
    result = []
    for row in csv.DictReader(io.StringIO(text)):
        observed_at = _parse_time(row.get("observation_time", ""))
        if observed_at is None:
            continue
        age_minutes = max(0.0, (now - observed_at) / 60.0)
        if age_minutes > MAX_OBSERVATION_AGE_MINUTES:
            continue
        try:
            latitude = float(row["latitude"])
            longitude = float(row["longitude"])
        except (KeyError, TypeError, ValueError):
            continue
        wx_string = str(row.get("wx_string") or "").upper()
        tokens = {
            token.lstrip("+-")
            for group in wx_string.split()
            for token in group.split("/")
        }
        wet = any(
            marker in token
            for token in tokens
            for marker in PRECIPITATION_TOKENS
        )
        result.append({
            "station_id": str(row.get("station_id") or ""),
            "latitude": latitude,
            "longitude": longitude,
            "observed_at": observed_at,
            "age_minutes": round(age_minutes, 1),
            "wet": wet,
            "weather": wx_string,
        })
    return result


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = phi2 - phi1
    dlambda = math.radians(lon2 - lon1)
    value = (
        math.sin(dphi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    )
    return radius * 2.0 * math.atan2(math.sqrt(value), math.sqrt(1.0 - value))


def _probe_city(city: dict, observations: list[dict]) -> dict:
    nearby = []
    for observation in observations:
        distance = _distance_km(
            city["latitude"],
            city["longitude"],
            observation["latitude"],
            observation["longitude"],
        )
        if distance <= RADIUS_KM:
            nearby.append({**observation, "distance_km": round(distance, 1)})
    nearby.sort(key=lambda item: item["distance_km"])
    count = len(nearby)
    quality = "goed" if count >= 5 else "beperkt" if count >= 2 else "onvoldoende"
    return {
        **city,
        "radius_km": RADIUS_KM,
        "station_count": count,
        "wet_station_count": sum(1 for item in nearby if item["wet"]),
        "coverage_quality": quality,
        "stations": nearby,
    }


def run(
    baseline: dict,
    *,
    cities_archive: bytes | None = None,
    metar_cache: bytes | None = None,
    now: float | None = None,
) -> dict:
    """Survey relevant countries that remain weak after the PWS scan."""
    weak = {
        item["country_code"]
        for item in baseline.get("countries", [])
        if item.get("selection") == "niet_verifieerbaar"
    }
    relevant = weak - EXCLUDED_REMOTE_CODES
    excluded = sorted(weak & EXCLUDED_REMOTE_CODES)
    cities = _largest_cities(relevant, archive_bytes=cities_archive)
    observations = _metar_observations(cache_bytes=metar_cache, now=now)
    countries = []
    for code in sorted(relevant):
        results = [
            _probe_city(city, observations)
            for city in cities.get(code, [])
        ]
        usable = [
            city for city in results
            if city["coverage_quality"] != "onvoldoende"
        ]
        countries.append({
            "country_code": code,
            "cities_tested": len(results),
            "usable_city_count": len(usable),
            "selection": "metar" if usable else "niet_verifieerbaar",
            "cities": results,
        })
    return {
        "method": "broad_metar_ground_coverage_v1",
        "source": "NOAA AviationWeather.gov worldwide METAR cache",
        "surveyed_at": time.time(),
        "radius_km": RADIUS_KM,
        "cities_per_country": CITIES_PER_COUNTRY,
        "max_observation_age_minutes": MAX_OBSERVATION_AGE_MINUTES,
        "input_weak_country_count": len(weak),
        "relevant_country_count": len(relevant),
        "excluded_remote_country_count": len(excluded),
        "excluded_remote_country_codes": excluded,
        "metar_observation_count": len(observations),
        "countries": countries,
    }


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("gebruik: metar_survey_cli CITY_SURVEY_JSON")
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
