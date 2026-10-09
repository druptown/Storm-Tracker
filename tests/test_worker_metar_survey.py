from __future__ import annotations

import csv
import gzip
import io
import zipfile

import pytest

from storm_tracker_worker import metar_survey_cli
from storm_tracker_worker.metar_survey_cli import (
    _metar_observations,
    _probe_city,
    run,
)


def _metar_cache(rows: list[dict]) -> bytes:
    fields = [
        "raw_text", "station_id", "observation_time", "latitude",
        "longitude", "wx_string",
    ]
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    return gzip.compress(stream.getvalue().encode())


def _cities_archive(lines: list[str]) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("cities15000.txt", "\n".join(lines))
    return stream.getvalue()


def _city_line(name: str, country: str, lat: float, lon: float, pop: int) -> str:
    fields = [""] * 19
    fields[1] = name
    fields[4] = str(lat)
    fields[5] = str(lon)
    fields[8] = country
    fields[14] = str(pop)
    return "\t".join(fields)


def test_metar_parser_keeps_recent_rain_observation():
    cache = _metar_cache([{
        "raw_text": "METAR TEST 281400Z RA",
        "station_id": "TEST",
        "observation_time": "2026-07-28T14:00:00.000Z",
        "latitude": "50.0",
        "longitude": "4.0",
        "wx_string": "-RA",
    }])
    result = _metar_observations(
        cache_bytes=cache,
        now=1785249000.0,
    )
    assert len(result) == 1
    assert result[0]["wet"] is True


def test_metar_parser_refuses_unbounded_decompression(monkeypatch):
    monkeypatch.setattr(
        metar_survey_cli,
        "MAX_METAR_UNCOMPRESSED_BYTES",
        16,
    )
    cache = gzip.compress(b"x" * 17)

    with pytest.raises(ValueError, match="contractlimiet"):
        _metar_observations(cache_bytes=cache, now=1785249000.0)


def test_city_needs_two_recent_metars_for_limited_coverage():
    city = {"name": "Teststad", "latitude": 50.0, "longitude": 4.0}
    observations = [
        {
            "station_id": "ONE",
            "latitude": 50.1,
            "longitude": 4.0,
            "observed_at": 1.0,
            "age_minutes": 1.0,
            "wet": False,
            "weather": "",
        },
        {
            "station_id": "TWO",
            "latitude": 50.2,
            "longitude": 4.0,
            "observed_at": 1.0,
            "age_minutes": 1.0,
            "wet": True,
            "weather": "RA",
        },
    ]
    result = _probe_city(city, observations)
    assert result["coverage_quality"] == "beperkt"
    assert result["wet_station_count"] == 1


def test_run_excludes_remote_and_finds_relevant_country():
    baseline = {
        "countries": [
            {"country_code": "BE", "selection": "niet_verifieerbaar"},
            {"country_code": "TV", "selection": "niet_verifieerbaar"},
        ],
    }
    cities = _cities_archive([
        _city_line("Brussel", "BE", 50.85, 4.35, 1_000_000),
        _city_line("Funafuti", "TV", -8.52, 179.19, 6_000),
    ])
    cache = _metar_cache([
        {
            "station_id": "EBBR",
            "observation_time": "2026-07-28T14:00:00.000Z",
            "latitude": "50.90",
            "longitude": "4.48",
            "wx_string": "",
        },
        {
            "station_id": "EBCI",
            "observation_time": "2026-07-28T14:00:00.000Z",
            "latitude": "50.46",
            "longitude": "4.45",
            "wx_string": "RA",
        },
    ])
    result = run(
        baseline,
        cities_archive=cities,
        metar_cache=cache,
        now=1785249000.0,
    )
    assert result["excluded_remote_country_codes"] == ["TV"]
    assert result["countries"][0]["selection"] == "metar"
