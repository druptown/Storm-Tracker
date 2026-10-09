from storm_tracker_worker import ground_coverage
import pytest


def _station(source, *, wet=False):
    return {
        "source": source,
        "rain_capable": True,
        "rain_mm": 0.4 if wet else 0.0,
    }


def test_summary_classifies_coverage_and_wet_stations():
    observations = [_station("netatmo") for _ in range(19)]
    observations.append(_station("netatmo", wet=True))

    result = ground_coverage._summary("netatmo", observations)

    assert result["coverage_quality"] == "goed"
    assert result["rain_capable_station_count"] == 20
    assert result["wet_station_count"] == 1


def test_capital_survey_combines_usable_sources_without_station_ids(monkeypatch):
    monkeypatch.setattr(
        ground_coverage,
        "CAPITALS",
        {"US": ("Washington D.C.", 38.9072, -77.0369)},
    )
    monkeypatch.setattr(
        ground_coverage._NETATMO,
        "configured",
        lambda: True,
    )
    monkeypatch.setattr(
        ground_coverage._NETATMO,
        "_token",
        lambda now: "token",
    )
    monkeypatch.setattr(
        ground_coverage._NETATMO,
        "query_radius",
        lambda token, lat, lon, radius: [
            _station("netatmo") for _ in range(8)
        ],
    )
    monkeypatch.setenv("STV3_SYNOPTIC_TOKEN", "synoptic-token")
    monkeypatch.setattr(
        ground_coverage,
        "synoptic_query",
        lambda *args, **kwargs: {"STATION": []},
    )
    monkeypatch.setattr(
        ground_coverage,
        "synoptic_observations",
        lambda raw, now: [_station("synoptic") for _ in range(22)],
    )

    result = ground_coverage.survey({})

    country = result["countries"][0]
    assert country["radius_km"] == 100.0
    assert country["selected_sources"] == ["netatmo", "synoptic"]
    assert country["selection"] == "netatmo + synoptic"
    assert "observations" not in country


@pytest.mark.parametrize(
    "secret_payload",
    (
        {"client_secret": "must-not-travel"},
        {"refresh_token": "must-not-travel"},
        {"synoptic_token": "must-not-travel"},
    ),
)
def test_capital_survey_rejects_secret_bearing_jobs(secret_payload):
    with pytest.raises(ValueError, match="credentials"):
        ground_coverage.survey(secret_payload)
