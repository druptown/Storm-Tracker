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
