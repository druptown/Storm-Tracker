"""Contracttests voor de slapende nationale buurlandproviders."""


def test_met_office_selects_latest_hdf_key(met_office_radar_module):
    xml = """<ListBucketResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>
      <Contents><Key>radar/2026/07/20/202607201500_test.h5</Key></Contents>
      <Contents><Key>radar/2026/07/20/202607201515_test.h5</Key></Contents>
    </ListBucketResult>"""
    assert met_office_radar_module.latest_key_from_listing(xml).endswith("1515_test.h5")


def test_national_provider_coverage(base_module, met_office_radar_module, meteofrance_radar_module, meteolux_module):
    london = base_module.CoverageArea(51.5, -0.1, 250)
    paris = base_module.CoverageArea(48.86, 2.35, 250)
    luxembourg = base_module.CoverageArea(49.61, 6.13, 250)
    miami = base_module.CoverageArea(25.76, -80.19, 250)
    assert met_office_radar_module.MetOfficeRadarProvider(None).supports(london).supported
    assert meteofrance_radar_module.MeteoFranceRadarProvider(None, "token").supports(paris).supported
    assert meteolux_module.MeteoLuxProvider(None).supports(luxembourg).supported
    assert not met_office_radar_module.MetOfficeRadarProvider(None).supports(miami).supported
    assert not meteofrance_radar_module.MeteoFranceRadarProvider(None, "token").supports(miami).supported
    assert not meteolux_module.MeteoLuxProvider(None).supports(miami).supported


def test_policy_uses_real_neighbor_provider_ids():
    import json
    from pathlib import Path
    policy = json.loads((Path(__file__).parents[1] / "custom_components/storm_tracker_v3/provider_policy.json").read_text())
    assert "meteofrance_radar" in policy["countries"]["FR"]["radar"]
    assert "met_office_radar" in policy["countries"]["GB"]["radar"]
    assert "meteolux" in policy["countries"]["LU"]["ground_validation"]


def test_austria_nowcast_summary(geosphere_at_module):
    payload = {"reference_time": "2026-07-20T17:15+00:00", "features": [{"properties": {"parameters": {"rr": {"data": [0, 0.2, 1.3, None]}}}}]}
    summary = geosphere_at_module.summarize_nowcast(payload)
    assert summary["forecast_steps"] == 3
    assert summary["rain_next_3h_mm"] == 1.5
    assert summary["max_15min_mm"] == 1.3


def test_austria_and_italy_coverage(base_module, geosphere_at_module, italiameteo_module):
    vienna = base_module.CoverageArea(48.21, 16.37, 250)
    rome = base_module.CoverageArea(41.90, 12.50, 250)
    miami = base_module.CoverageArea(25.76, -80.19, 250)
    assert geosphere_at_module.GeoSphereAustriaProvider(None).supports(vienna).supported
    assert italiameteo_module.ItaliaMeteoRadarProvider(None).supports(rome).supported
    assert not geosphere_at_module.GeoSphereAustriaProvider(None).supports(miami).supported
    assert not italiameteo_module.ItaliaMeteoRadarProvider(None).supports(miami).supported


def test_italiameteo_selects_latest_bundle(italiameteo_module):
    latest = italiameteo_module.latest_bundle([
        {"date": "2026-07-18", "filename": "old.grib"},
        {"date": "2026-07-19", "filename": "new.grib"},
    ])
    assert latest["filename"] == "new.grib"


def test_italiameteo_declares_forecast_and_radar(italiameteo_module, base_module):
    provider = italiameteo_module.ItaliaMeteoRadarProvider(None)
    assert base_module.Capability.RADAR in provider.capabilities
    assert base_module.Capability.NOWCAST in provider.capabilities


def test_italiameteo_rejects_non_json_success_body(italiameteo_module):
    import pytest
    with pytest.raises(Exception):
        italiameteo_module.decode_json_response("upstream temporarily unavailable")
