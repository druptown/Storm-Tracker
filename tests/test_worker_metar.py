from datetime import datetime, timezone
import csv
import gzip
import io

from storm_tracker_worker.metar import (
    MetarWorker,
    _decode,
    _decode_with_quarantine,
    _hourly_precipitation_mm,
    _parse_weather_group,
)


def _cache(rows):
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=[
        "station_id", "observation_time", "latitude", "longitude",
        "wx_string", "raw_text",
    ])
    writer.writeheader()
    writer.writerows(rows)
    return gzip.compress(stream.getvalue().encode())


def test_metar_decode_marks_explicit_rain():
    now = datetime.now(timezone.utc).timestamp()
    observations = _decode(_cache([{
        "station_id": "KNYC",
        "observation_time": datetime.fromtimestamp(
            now - 300, timezone.utc
        ).isoformat(),
        "latitude": "40.7789",
        "longitude": "-73.9692",
        "wx_string": "-RA",
        "raw_text": "METAR KNYC -RA",
    }]), now)
    assert observations[0]["source"] == "metar"
    assert observations[0]["rain_mm"] is None
    assert observations[0]["precipitation_present"] is True
    assert observations[0]["quantitative_precipitation"] is False


def test_metar_decode_ignores_ended_rain_in_remarks():
    now = datetime.now(timezone.utc).timestamp()
    observations = _decode(_cache([{
        "station_id": "KJFK",
        "observation_time": datetime.fromtimestamp(
            now - 300, timezone.utc
        ).isoformat(),
        "latitude": "40.6392",
        "longitude": "-73.7639",
        "wx_string": "",
        "raw_text": "SPECI KJFK 10SM BKN025 RMK AO2 RAE1057 P0000",
    }]), now)
    assert observations[0]["rain_mm"] is None
    assert observations[0]["precipitation_present"] is False


def test_metar_decode_ignores_freezing_rain_sensor_status():
    now = datetime.now(timezone.utc).timestamp()
    observations = _decode(_cache([{
        "station_id": "KJRB",
        "observation_time": datetime.fromtimestamp(
            now - 300, timezone.utc
        ).isoformat(),
        "latitude": "40.701",
        "longitude": "-74.009",
        "wx_string": "",
        "raw_text": "SPECI KJRB 10SM OVC015 RMK AO2 FZRANO PNO",
    }]), now)
    assert observations[0]["rain_mm"] is None
    assert observations[0]["precipitation_present"] is False
    assert observations[0]["quality_flags"] == ["PNO", "FZRANO"]


def test_metar_vicinity_rain_is_not_local_ground_confirmation():
    now = datetime.now(timezone.utc).timestamp()
    observation = _decode(_cache([{
        "station_id": "KVC",
        "observation_time": datetime.fromtimestamp(
            now - 300, timezone.utc
        ).isoformat(),
        "latitude": "40.7",
        "longitude": "-74.0",
        "wx_string": "VCSH VCRA",
        "raw_text": "METAR KVC AUTO 10SM VCSH RMK AO2",
    }]), now)[0]

    assert observation["precipitation_present"] is False
    assert observation["precipitation_in_vicinity"] is True
    assert observation["interpretation_reason"] == "vicinity_weather:VCSH VCRA"
    assert observation["rain_capable"] is True
    assert observation["raw_payload"]["weather_groups"][1] == {
        "raw": "VCRA",
        "intensity": None,
        "proximity": "vicinity",
        "descriptor": None,
        "phenomena": ["RA"],
        "precipitation": True,
        "local_precipitation": False,
    }


def test_metar_pwino_makes_missing_weather_indeterminate_not_dry():
    now = datetime.now(timezone.utc).timestamp()
    observation = _decode(_cache([{
        "station_id": "KNO",
        "observation_time": datetime.fromtimestamp(
            now - 300, timezone.utc
        ).isoformat(),
        "latitude": "40.7",
        "longitude": "-74.0",
        "wx_string": "",
        "raw_text": "METAR KNO AUTO 10SM CLR RMK AO2 PWINO $",
    }]), now)[0]

    assert observation["precipitation_present"] is False
    assert observation["rain_capable"] is False
    assert observation["can_observe_dry"] is False
    assert observation["interpretation_reason"] == (
        "present_weather_sensor_unavailable"
    )
    assert observation["quality_flags"] == [
        "PWINO",
        "maintenance_required",
    ]


def test_metar_accumulation_is_kept_as_interval_not_current_rain():
    now = datetime.now(timezone.utc).timestamp()
    observation = _decode(_cache([{
        "station_id": "KACC",
        "observation_time": datetime.fromtimestamp(
            now - 300, timezone.utc
        ).isoformat(),
        "latitude": "40.7",
        "longitude": "-74.0",
        "wx_string": "",
        "raw_text": "METAR KACC AUTO 10SM CLR RMK AO2 P0012",
    }]), now)[0]

    assert observation["precipitation_present"] is False
    assert observation["raw_payload"]["hourly_precipitation_mm"] == 3.048
    assert observation["raw_payload"]["hourly_precipitation_interval"] == (
        "since_previous_metar_report"
    )


def test_metar_weather_group_preserves_intensity_descriptor_and_phenomena():
    assert _parse_weather_group("-SHRASN") == {
        "raw": "-SHRASN",
        "intensity": "-",
        "proximity": "at_station",
        "descriptor": "SH",
        "phenomena": ["RA", "SN"],
        "precipitation": True,
        "local_precipitation": True,
    }
    assert _hourly_precipitation_mm("RMK AO2 P0001") == 0.254


def test_metar_worker_filters_to_requested_radius():
    now = datetime.now(timezone.utc).timestamp()
    payload = _cache([{
        "station_id": "KNYC",
        "observation_time": datetime.fromtimestamp(
            now - 300, timezone.utc
        ).isoformat(),
        "latitude": "40.7789",
        "longitude": "-73.9692",
        "wx_string": "RA",
        "raw_text": "METAR KNYC RA",
    }])

    class Response:
        def __init__(self):
            self.stream = io.BytesIO(payload)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, maximum_bytes=-1):
            return self.stream.read(maximum_bytes)

    result = MetarWorker(opener=lambda *_args, **_kwargs: Response()).fetch({
        "latitude": 40.7128,
        "longitude": -74.0060,
        "radius_km": 20,
    })
    assert result["method"] == "metar_worker_v1"
    assert result["station_count"] == 1
    assert result["wet_station_count"] == 1


def test_future_metar_is_preserved_but_quarantined_from_current_decisions():
    now = datetime.now(timezone.utc).timestamp()
    payload = _cache([{
        "station_id": "KFUT",
        "observation_time": datetime.fromtimestamp(
            now + 300, timezone.utc
        ).isoformat(),
        "latitude": "40.7789",
        "longitude": "-73.9692",
        "wx_string": "RA",
        "raw_text": "METAR KFUT RA",
    }])

    current, quarantined = _decode_with_quarantine(payload, now)

    assert current == []
    assert len(quarantined) == 1
    assert quarantined[0]["precipitation_present"] is True
    assert quarantined[0]["time_quality"] == "future_clock_skew"
    assert quarantined[0]["usable_for_current_decision"] is False
    assert "observation_from_future" in quarantined[0]["quality_flags"]
    assert quarantined[0]["time_quality_details"][
        "observation_age_seconds"
    ] == -300.0
    assert quarantined[0]["time_quality_details"]["policy_id"] == (
        "metar-current-report-v1"
    )


def test_stale_metar_is_preserved_as_quarantine_evidence_not_dropped():
    now = datetime.now(timezone.utc).timestamp()
    payload = _cache([{
        "station_id": "KOLD",
        "observation_time": datetime.fromtimestamp(
            now - 121 * 60, timezone.utc
        ).isoformat(),
        "latitude": "40.7789",
        "longitude": "-73.9692",
        "wx_string": "RA",
        "raw_text": "METAR KOLD RA",
    }])

    current, quarantined = _decode_with_quarantine(payload, now)

    assert current == []
    assert len(quarantined) == 1
    assert quarantined[0]["precipitation_present"] is True
    assert quarantined[0]["time_quality"] == "stale"
    assert quarantined[0]["usable_for_current_decision"] is False
    assert "observation_stale" in quarantined[0]["quality_flags"]
    assert quarantined[0]["time_quality_details"][
        "observation_age_seconds"
    ] == 121 * 60
