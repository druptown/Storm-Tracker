"""Isolated follow-up survey for countries with weak capital coverage."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import io
import json
import os
import sys
import time
import urllib.request
import zipfile

from .survey_cli import _inherit_worker_environment
from .tracking_core.engine.bounded_payload import read_limited_stream

GEONAMES_URL = (
    "https://download.geonames.org/export/dump/cities15000.zip"
)
RADIUS_KM = 100.0
CITIES_PER_COUNTRY = 3
MAX_GEONAMES_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_GEONAMES_MEMBER_BYTES = 256 * 1024 * 1024


def _largest_cities(country_codes: set[str]) -> dict[str, list[dict]]:
    """Return the largest GeoNames population centres for each country."""
    with urllib.request.urlopen(GEONAMES_URL, timeout=60) as response:
        payload = read_limited_stream(
            response,
            maximum_bytes=MAX_GEONAMES_ARCHIVE_BYTES,
            declared_bytes=response.headers.get("Content-Length"),
            require_declared_match=True,
        )
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
            cities,
            key=lambda item: item["population"],
            reverse=True,
        )[:CITIES_PER_COUNTRY]
        for code, cities in grouped.items()
    }


def _provider_context():
    from .netatmo import _NETATMO
    from .synoptic import _query, _observations

    now = datetime.now(timezone.utc).timestamp()
    token = _NETATMO._token(now) if _NETATMO.configured() else None
    return _NETATMO, token, _query, _observations, now, str(
        os.getenv("STV3_SYNOPTIC_TOKEN", "")
    )


def _probe_city(city: dict, context) -> dict:
    from .ground_coverage import _summary

    netatmo, netatmo_token, query, observations, now, synoptic_token = context
    sources, errors = [], {}
    latitude, longitude = city["latitude"], city["longitude"]
    if netatmo_token:
        for attempt in range(3):
            try:
                sources.append(_summary(
                    "netatmo",
                    netatmo.query_radius(
                        netatmo_token,
                        latitude,
                        longitude,
                        RADIUS_KM,
                    ),
                ))
                break
            except Exception as err:
                if attempt == 2:
                    errors["netatmo"] = f"{type(err).__name__}: {err}"
                else:
                    time.sleep(2 ** attempt)
    if synoptic_token:
        try:
            raw = query(
                latitude,
                longitude,
                radius_km=RADIUS_KM,
                token=synoptic_token,
                limit=1000,
            )
            sources.append(_summary(
                "synoptic", observations(raw, now)
            ))
        except RuntimeError as err:
            if "Synoptic API-fout 2" in str(err):
                sources.append(_summary("synoptic", []))
            else:
                errors["synoptic"] = f"{type(err).__name__}: {err}"
        except Exception as err:
            errors["synoptic"] = f"{type(err).__name__}: {err}"
    selected = [
        item["source"] for item in sources
        if item["coverage_quality"] != "onvoldoende"
    ]
    return {
        **city,
        "radius_km": RADIUS_KM,
        "sources": sources,
        "selected_sources": selected,
        "selection": " + ".join(selected) if selected else "niet_verifieerbaar",
        "errors": errors,
    }


def run(baseline: dict) -> dict:
    """Probe three large cities for every insufficient baseline country."""
    insufficient = {
        item["country_code"]
        for item in baseline.get("countries", [])
        if item.get("selection") == "niet_verifieerbaar"
    }
    cities = _largest_cities(insufficient)
    context = _provider_context()
    countries = []
    for code in sorted(insufficient):
        results = [
            _probe_city(city, context)
            for city in cities.get(code, [])
        ]
        usable = sorted({
            source
            for result in results
            for source in result["selected_sources"]
        })
        countries.append({
            "country_code": code,
            "cities_tested": len(results),
            "cities": results,
            "selected_sources": usable,
            "selection": (
                " + ".join(usable) if usable else "niet_verifieerbaar"
            ),
        })
    return {
        "method": "multi_city_ground_coverage_v1",
        "source": "GeoNames cities15000 CC BY 4.0",
        "surveyed_at": time.time(),
        "radius_km": RADIUS_KM,
        "cities_per_country": CITIES_PER_COUNTRY,
        "country_count": len(countries),
        "countries": countries,
    }


def main() -> None:
    _inherit_worker_environment()
    if len(sys.argv) != 2:
        raise SystemExit("gebruik: city_survey_cli BASELINE_JSON")
    with open(sys.argv[1], encoding="utf-8") as source:
        baseline = json.load(source)
    print(json.dumps(
        run(baseline),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
