"""Contracttests voor de slapende nationale buurlandproviders."""

import asyncio


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


def test_meteofrance_application_id_refreshes_and_caches_token(
    meteofrance_radar_module,
):
    class Response:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): return False
        def raise_for_status(self): return None
        async def json(self, content_type=None):
            return {"access_token": "fresh", "expires_in": 3600}

    class Session:
        def __init__(self): self.posts = 0
        def post(self, url, data=None, headers=None):
            self.posts += 1
            assert headers["Authorization"] == "Basic application-id"
            return Response()

    session = Session()
    provider = meteofrance_radar_module.MeteoFranceRadarProvider(
        session, application_id="application-id"
    )
    first = asyncio.run(provider._access_token())
    second = asyncio.run(provider._access_token())
    assert first == second == "fresh"
    assert session.posts == 1


def test_policy_uses_real_neighbor_provider_ids():
    import json
    from pathlib import Path
    policy = json.loads((Path(__file__).parents[1] / "custom_components/storm_tracker_v3/provider_policy.json").read_text())
    assert "meteofrance_radar" in policy["countries"]["FR"]["radar"]
    assert "met_office_radar" in policy["countries"]["GB"]["radar"]
    assert "meteolux" in policy["countries"]["LU"]["model_guidance"]


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


def test_austria_point_nowcast_does_not_query_florence(base_module, geosphere_at_module):
    florence = base_module.CoverageArea(43.7696, 11.2558, 250.0)
    assert not geosphere_at_module.GeoSphereAustriaProvider(None).supports(florence).supported


def test_italiameteo_selects_latest_bundle(italiameteo_module):
    latest = italiameteo_module.latest_bundle([
        {"date": "2026-07-18", "filename": "old.grib"},
        {"date": "2026-07-19", "filename": "new.grib"},
    ])
    assert latest["filename"] == "new.grib"


def test_italiameteo_is_validation_not_realtime_radar(italiameteo_module, base_module):
    provider = italiameteo_module.ItaliaMeteoRadarProvider(None)
    assert base_module.Capability.NOWCAST in provider.capabilities
    assert base_module.Capability.RADAR not in provider.capabilities


def test_italiameteo_rejects_non_json_success_body(italiameteo_module):
    import pytest
    with pytest.raises(Exception):
        italiameteo_module.decode_json_response("upstream temporarily unavailable")


def test_italiameteo_does_not_hide_open_meteo_poll_when_disabled(
    italiameteo_module, base_module,
):
    class CatalogResponse:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def raise_for_status(self):
            return None

        async def json(self):
            return [{
                "date": "2026-07-23",
                "filename": "radar_sri_dpc.grib",
            }]

    class Session:
        def __init__(self):
            self.urls = []

        def get(self, url, **kwargs):
            self.urls.append(url)
            if url != italiameteo_module.CATALOG_URL:
                raise AssertionError(f"verborgen netwerkrequest: {url}")
            return CatalogResponse()

    session = Session()
    provider = italiameteo_module.ItaliaMeteoRadarProvider(
        session,
        model_guidance_enabled=False,
    )
    provider._areas = (base_module.CoverageArea(41.9, 12.5, 250),)

    assert asyncio.run(provider.async_fetch()) == []
    assert session.urls == [italiameteo_module.CATALOG_URL]
    assert provider.diagnostics["model_guidance_enabled"] is False
    assert provider.diagnostics["forecast_model"] is None
    assert provider.diagnostics["forecast_status"] == "disabled"
    assert provider.diagnostics["forecast_errors"] == []
