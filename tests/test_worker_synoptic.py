from __future__ import annotations

import io
import json
from datetime import datetime, timedelta, timezone

from storm_tracker_worker.synoptic import (
    SYNOPTIC_MAX_RESPONSE_BYTES,
    SynopticWorker,
    _observations,
    probe_radius,
)


class _Response:
    def __init__(self, payload, *, headers=None):
        self._body = json.dumps(payload).encode()
        self._stream = io.BytesIO(self._body)
        if headers is not None:
            self.headers = headers

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, size=-1):
        return self._stream.read(size)


def test_synoptic_probe_requires_token():
    assert probe_radius(40.0, -75.0, token="") == {
        "status": "credentials_missing",
        "station_count": 0,
    }


def test_synoptic_probe_is_bounded_and_redacts_station_details():
    seen = {}
    now = datetime.now(timezone.utc)

    def opener(request, *, timeout):
        seen["url"] = request.full_url
        seen["timeout"] = timeout
        return _Response({
            "SUMMARY": {"RESPONSE_CODE": 1, "RESPONSE_MESSAGE": "OK"},
            "STATION": [{
                "STID": "SECRET",
                "LATITUDE": "40.0",
                "LONGITUDE": "-75.0",
                "OBSERVATIONS": {
                    "date_time": [
                        (now - timedelta(minutes=5)).isoformat(),
                        now.isoformat(),
                    ],
                    "precip_intervals_set_1d": [None, 1.2],
                },
            }],
        })

    result = probe_radius(40.0, -75.0, token="secret-token", opener=opener)

    assert seen["timeout"] == 20
    assert "limit=50" in seen["url"]
    assert "12.43" in seen["url"]
    assert "precip=1" in seen["url"]
    assert "qc=on" in seen["url"]
    assert "qc_remove_data=on" in seen["url"]
    assert "qc_flags=on" in seen["url"]
    assert "qc_checks=synopticlabs" in seen["url"]
    assert "recent=60" in seen["url"]
    assert result["status"] == "ok"
    assert result["station_count"] == 1
    assert result["precipitation_station_count"] == 1
    assert result["wet_station_count"] == 1
    serialized = json.dumps(result)
    assert "secret-token" not in serialized
    assert "SECRET" not in serialized


def test_synoptic_probe_rejects_invalid_or_lying_content_length():
    payload = {
        "SUMMARY": {"RESPONSE_CODE": 1, "RESPONSE_MESSAGE": "OK"},
        "STATION": [],
    }
    exact_length = len(json.dumps(payload).encode())

    for declared in (
        "invalid",
        str(SYNOPTIC_MAX_RESPONSE_BYTES + 1),
        str(exact_length + 1),
    ):
        result = probe_radius(
            40.0,
            -75.0,
            token="secret-token",
            opener=lambda *_args, value=declared, **_kwargs: _Response(
                payload,
                headers={"Content-Length": value},
            ),
        )
        assert result == {"status": "transport_error", "station_count": 0}


def test_synoptic_worker_returns_ground_observations_and_uses_50km_roaming(
    monkeypatch,
):
    now = datetime.now(timezone.utc)
    seen = {}

    def opener(request, *, timeout):
        seen["url"] = request.full_url
        return _Response({
            "SUMMARY": {"RESPONSE_CODE": 1},
            "STATION": [{
                "STID": "KTEST",
                "LATITUDE": "35.18",
                "LONGITUDE": "-102.03",
                "OBSERVATIONS": {
                    "date_time": [
                        (now - timedelta(minutes=5)).isoformat(),
                        now.isoformat(),
                    ],
                    "precip_intervals_set_1": [None, 1.5],
                },
            }],
        })

    monkeypatch.setenv("STV3_SYNOPTIC_TOKEN", "token")
    worker = SynopticWorker(opener=opener)
    payload = {
        "query_key": "roaming:test",
        "query_kind": "roaming",
        "latitude": 35.18,
        "longitude": -102.03,
        "radius_km": 50,
    }

    first = worker.fetch(payload)
    second = worker.fetch(payload)

    assert "31.07" in seen["url"]
    assert first["method"] == "synoptic_worker_v1"
    assert first["station_count"] == 1
    assert first["wet_station_count"] == 1
    assert first["observations"][0]["source"] == "synoptic"
    assert first["observations"][0]["station_id"] == "synoptic:KTEST"
    assert second["cache"] == "rate_limited"


def test_synoptic_matches_netatmo_weather_fields(monkeypatch):
    now = datetime.now(timezone.utc)

    def opener(_request, *, timeout):
        return _Response({
            "SUMMARY": {"RESPONSE_CODE": 1},
            "STATION": [{
                "STID": "KFULL",
                "LATITUDE": "35.18",
                "LONGITUDE": "-102.03",
                "OBSERVATIONS": {
                    "date_time": [
                        (now - timedelta(minutes=5)).isoformat(),
                        now.isoformat(),
                    ],
                    "precip_intervals_set_1": [None, 0.4],
                    "air_temp_set_1": [None, 24.5],
                    "relative_humidity_set_1": [None, 72.0],
                    "wind_speed_set_1": [None, 18.0],
                    "wind_direction_set_1": [None, 245.0],
                    "wind_gust_set_1": [None, 31.0],
                    "sea_level_pressure_set_1d": [None, 1007.4],
                },
            }],
        })

    monkeypatch.setenv("STV3_SYNOPTIC_TOKEN", "token")
    observation = SynopticWorker(opener=opener).fetch({
        "latitude": 35.18, "longitude": -102.03, "radius_km": 20,
    })["observations"][0]

    assert observation["pressure"] == 1007.4
    assert observation["temperature"] == 24.5
    assert observation["humidity"] == 72.0
    assert observation["wind_speed"] == 18.0
    assert observation["wind_angle"] == 245.0
    assert observation["gust_speed"] == 31.0


def test_synoptic_preserves_precipitation_interval_semantics(monkeypatch):
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=12)

    def opener(_request, *, timeout):
        return _Response({
            "SUMMARY": {"RESPONSE_CODE": 1, "NUMBER_OF_OBJECTS": 1},
            "STATION": [{
                "STID": "KINTERVAL",
                "LATITUDE": "35.18",
                "LONGITUDE": "-102.03",
                "QC_FLAGGED": {"precip_intervals_set_1d": [False, False]},
                "QC_SUMMARY": {"QC_CHECKS_APPLIED": ["sl_rate"]},
                "SENSOR_VARIABLES": {
                    "precip_intervals": {
                        "precip_intervals_set_1d": {
                            "position": "main",
                        },
                    },
                },
                "OBSERVATIONS": {
                    "date_time": [start.isoformat(), now.isoformat()],
                    "precip_intervals_set_1d": [None, 1.5],
                },
            }],
        })

    monkeypatch.setenv("STV3_SYNOPTIC_TOKEN", "token")
    observation = SynopticWorker(opener=opener).fetch({
        "latitude": 35.18,
        "longitude": -102.03,
    })["observations"][0]

    assert observation["rain_mm"] is None
    assert observation["quantitative_precipitation"] is False
    assert observation["measurement_kind"] == "synoptic_precip_interval"
    assert observation["measurement_interval_start"] == start.timestamp()
    assert observation["measurement_interval_end"] == now.timestamp()
    assert observation["parser_version"] == "synoptic_interval_v4"
    interval = observation["raw_payload"]["precipitation_intervals"][0]
    assert interval == {
        "field": "precip_intervals_set_1d",
        "sensor_set": "1d",
        "sample_index": 1,
        "value_mm": 1.5,
        "interval_start": start.timestamp(),
        "interval_end": now.timestamp(),
        "interval_seconds": 720.0,
        "interval_complete": True,
    }
    assert observation["raw_payload"]["selected_sample_index"] == 1
    assert observation["raw_payload"]["qc_flagged"] == {
        "precip_intervals_set_1d": [False, False],
    }
    assert observation["raw_payload"]["api_summary"][
        "NUMBER_OF_OBJECTS"
    ] == 1


def test_synoptic_incomplete_interval_cannot_supply_false_dry_evidence(
    monkeypatch,
):
    now = datetime.now(timezone.utc)

    def opener(_request, *, timeout):
        return _Response({
            "SUMMARY": {"RESPONSE_CODE": 1},
            "STATION": [{
                "STID": "KINCOMPLETE",
                "LATITUDE": "35.18",
                "LONGITUDE": "-102.03",
                "OBSERVATIONS": {
                    "date_time": [now.isoformat()],
                    "precip_intervals_set_1d": [0.0],
                    "air_temp_set_1": [20.0],
                },
            }],
        })

    monkeypatch.setenv("STV3_SYNOPTIC_TOKEN", "token")
    observation = SynopticWorker(opener=opener).fetch({
        "latitude": 35.18,
        "longitude": -102.03,
    })["observations"][0]

    assert observation["precipitation_present"] is False
    assert observation["rain_capable"] is False
    assert observation["can_observe_dry"] is False
    assert observation["interpretation_reason"] == (
        "no_complete_fresh_precip_interval"
    )
    assert "incomplete_precip_interval" in observation["quality_flags"]
    interval = observation["raw_payload"]["precipitation_intervals"][0]
    assert interval["interval_start"] is None
    assert interval["interval_complete"] is False


def test_synoptic_overlapping_sensor_sets_are_not_summed(monkeypatch):
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=5)

    def opener(_request, *, timeout):
        return _Response({
            "SUMMARY": {"RESPONSE_CODE": 1},
            "STATION": [{
                "STID": "KMULTI",
                "LATITUDE": "35.18",
                "LONGITUDE": "-102.03",
                "OBSERVATIONS": {
                    "date_time": [start.isoformat(), now.isoformat()],
                    "precip_intervals_set_1": [None, 1.0],
                    "precip_intervals_set_2": [None, 2.0],
                },
            }],
        })

    monkeypatch.setenv("STV3_SYNOPTIC_TOKEN", "token")
    observation = SynopticWorker(opener=opener).fetch({
        "latitude": 35.18,
        "longitude": -102.03,
    })["observations"][0]

    assert observation["precipitation_present"] is True
    assert observation["rain_mm"] is None
    assert [
        item["value_mm"]
        for item in observation["raw_payload"]["precipitation_intervals"]
    ] == [1.0, 2.0]


def test_synoptic_worker_caps_requested_radius_at_50km(monkeypatch):
    seen = {}

    def opener(request, *, timeout):
        seen["url"] = request.full_url
        return _Response({
            "SUMMARY": {"RESPONSE_CODE": 1},
            "STATION": [],
        })

    monkeypatch.setenv("STV3_SYNOPTIC_TOKEN", "token")
    worker = SynopticWorker(opener=opener)
    result = worker.fetch({
        "latitude": 35.0,
        "longitude": -101.0,
        "radius_km": 500,
    })

    assert result["radius_km"] == 50.0
    assert "31.07" in seen["url"]


def test_synoptic_future_precipitation_is_preserved_but_quarantined():
    now = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)
    future = now + timedelta(minutes=2)
    payload = {
        "SUMMARY": {"RESPONSE_CODE": 1},
        "STATION": [{
            "STID": "KFUTURE",
            "LATITUDE": "40.0",
            "LONGITUDE": "-74.0",
            "OBSERVATIONS": {
                "date_time": [
                    (future - timedelta(minutes=5)).isoformat(),
                    future.isoformat(),
                ],
                "precip_intervals_set_1": [None, 1.0],
            },
        }],
    }

    record = _observations(payload, now.timestamp())[0]

    assert record["precipitation_present"] is True
    assert record["usable_for_current_decision"] is False
    assert record["time_quality"] == "future_clock_skew"
    assert "time_quality_future_clock_skew" in record["quality_flags"]


def test_synoptic_stale_precipitation_is_preserved_but_quarantined():
    now = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)
    stale = now - timedelta(hours=2)
    payload = {
        "SUMMARY": {"RESPONSE_CODE": 1},
        "STATION": [{
            "STID": "KSTALE",
            "LATITUDE": "40.0",
            "LONGITUDE": "-74.0",
            "OBSERVATIONS": {
                "date_time": [
                    (stale - timedelta(minutes=5)).isoformat(),
                    stale.isoformat(),
                ],
                "precip_intervals_set_1": [None, 0.0],
            },
        }],
    }

    record = _observations(payload, now.timestamp())[0]

    assert record["can_observe_dry"] is False
    assert record["usable_for_current_decision"] is False
    assert record["time_quality"] == "stale"
    assert record["raw_payload"]["precipitation_intervals"][0][
        "interval_end"
    ] == stale.timestamp()


def test_synoptic_no_stations_is_valid_empty_result():
    class Response:
        def __init__(self):
            self._stream = io.BytesIO(
                b'{"SUMMARY":{"RESPONSE_CODE":2,'
                b'"RESPONSE_MESSAGE":"No stations found for this request"}}'
            )

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, size=-1):
            return self._stream.read(size)

    worker = SynopticWorker(opener=lambda *_args, **_kwargs: Response())
    worker._token = "test-token"
    result = worker.fetch({
        "latitude": 40.7128,
        "longitude": -74.006,
        "radius_km": 50,
    })
    assert result["method"] == "synoptic_worker_v1"
    assert result["station_count"] == 0
    assert result["cache"] == "fresh"
