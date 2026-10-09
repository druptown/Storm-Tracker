from storm_tracker_worker import city_survey_cli


def test_followup_only_retests_insufficient_countries(monkeypatch):
    baseline = {"countries": [
        {"country_code": "AA", "selection": "niet_verifieerbaar"},
        {"country_code": "BB", "selection": "netatmo"},
    ]}
    monkeypatch.setattr(
        city_survey_cli,
        "_largest_cities",
        lambda codes: {
            "AA": [{
                "name": "Grote Stad",
                "latitude": 1.0,
                "longitude": 2.0,
                "population": 100_000,
            }],
        },
    )
    monkeypatch.setattr(city_survey_cli, "_provider_context", lambda: object())
    monkeypatch.setattr(
        city_survey_cli,
        "_probe_city",
        lambda city, context: {
            **city,
            "selected_sources": ["netatmo"],
            "selection": "netatmo",
        },
    )

    result = city_survey_cli.run(baseline)

    assert result["country_count"] == 1
    assert result["countries"][0]["country_code"] == "AA"
    assert result["countries"][0]["selection"] == "netatmo"
