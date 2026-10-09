def _state(module, configured=True, healthy=True, last_success=900.0):
    return module.SourceState(configured, healthy, last_success)


def test_italy_prefers_healthy_dpc(engine_radar_policy_module):
    module = engine_radar_policy_module
    decision = module.select_engine_radar_source(
        {"IT"},
        {"dpc_radar": _state(module), "opera": _state(module)},
        now=1_000.0,
    )
    assert decision.source == "dpc_radar"
    assert decision.age_seconds == 100.0


def test_italy_falls_back_to_opera_then_rainviewer(engine_radar_policy_module):
    module = engine_radar_policy_module
    decision = module.select_engine_radar_source(
        {"IT"},
        {
            "dpc_radar": _state(module, healthy=False),
            "opera": _state(module),
            "rainviewer": _state(module),
        },
        now=1_000.0,
    )
    assert decision.source == "opera"

    decision = module.select_engine_radar_source(
        {"IT"},
        {
            "dpc_radar": _state(module, healthy=False),
            "opera": _state(module, healthy=False),
            "rainviewer": _state(module),
        },
        now=1_000.0,
    )
    assert decision.source == "rainviewer"


def test_shared_cross_border_engine_uses_composite(engine_radar_policy_module):
    module = engine_radar_policy_module
    decision = module.select_engine_radar_source(
        {"BE", "NL"},
        {"kmi": _state(module), "knmi": _state(module), "opera": _state(module)},
        now=1_000.0,
    )
    assert decision.source == "opera"
    assert "meerdere nationale" in decision.reason


def test_american_region_reports_outside_opera_coverage(engine_radar_policy_module):
    module = engine_radar_policy_module
    decision = module.select_engine_radar_source(
        {"US"},
        {
            "opera": _state(module, configured=False, healthy=False),
            "rainviewer": _state(module),
        },
        now=1_000.0,
    )
    assert decision.source == "rainviewer"
    assert "buiten OPERA-dekking" in decision.reason
    assert "OPERA niet beschikbaar" not in decision.reason


def test_spain_prefers_aemet(engine_radar_policy_module):
    module = engine_radar_policy_module
    decision = module.select_engine_radar_source(
        {"ES"}, {"aemet_radar": _state(module), "opera": _state(module)},
        now=1_000.0,
    )
    assert decision.source == "aemet_radar"


def test_localized_country_name_still_prefers_local_radar(engine_radar_policy_module):
    module = engine_radar_policy_module
    decision = module.select_engine_radar_source(
        {"BELGIE"},
        {"kmi": _state(module), "opera": _state(module)},
        now=1_000.0,
    )
    assert decision.source == "kmi"
    assert decision.country_codes == ("BE",)


def test_dutch_uk_code_prefers_met_office(engine_radar_policy_module):
    module = engine_radar_policy_module
    decision = module.select_engine_radar_source(
        {"VK"},
        {
            "met_office_radar": _state(module),
            "opera": _state(module),
            "rainviewer": _state(module),
        },
        now=1_000.0,
    )
    assert decision.source == "met_office_radar"
    assert decision.country_codes == ("GB",)


def test_luxembourg_uses_opera_while_meteolux_remains_validation(engine_radar_policy_module):
    module = engine_radar_policy_module
    decision = module.select_engine_radar_source(
        {"LUXEMBURG"},
        {"meteolux": _state(module), "opera": _state(module)},
        now=1_000.0,
    )
    assert decision.source == "opera"


def test_empty_local_opera_uses_rainviewer_echo(engine_radar_policy_module):
    module = engine_radar_policy_module
    states = {
        "opera": _state(module),
        "rainviewer": _state(module, last_success=950.0),
    }
    decision = module.select_engine_radar_source({"GR"}, states, now=1_000.0)
    decision = module.apply_echo_availability(
        decision,
        states,
        opera_observations=0,
        rainviewer_observations=12,
        now=1_000.0,
    )
    assert decision.source == "rainviewer"
    assert decision.age_seconds == 50.0
    assert "OPERA zonder lokale echo" in decision.reason


def test_dry_region_keeps_opera_when_both_sources_are_empty(engine_radar_policy_module):
    module = engine_radar_policy_module
    states = {"opera": _state(module), "rainviewer": _state(module)}
    decision = module.select_engine_radar_source({"GR"}, states, now=1_000.0)
    decision = module.apply_echo_availability(
        decision,
        states,
        opera_observations=0,
        rainviewer_observations=0,
        now=1_000.0,
    )
    assert decision.source == "opera"


def test_opera_echo_remains_preferred(engine_radar_policy_module):
    module = engine_radar_policy_module
    states = {"opera": _state(module), "rainviewer": _state(module)}
    decision = module.select_engine_radar_source({"GR"}, states, now=1_000.0)
    decision = module.apply_echo_availability(
        decision,
        states,
        opera_observations=2,
        rainviewer_observations=12,
        now=1_000.0,
    )
    assert decision.source == "opera"


def test_unconfirmed_opera_echo_at_coverage_edge_uses_rainviewer(
    engine_radar_policy_module,
):
    module = engine_radar_policy_module
    states = {"opera": _state(module), "rainviewer": _state(module)}
    decision = module.select_engine_radar_source({"TR"}, states, now=1_000.0)
    decision = module.apply_echo_availability(
        decision,
        states,
        opera_observations=2,
        rainviewer_observations=5,
        opera_coverage_complete=False,
        opera_corroborated_observations=0,
        now=1_000.0,
    )
    assert decision.source == "rainviewer"
    assert "randdekking" in decision.reason


def test_corroborated_opera_echo_at_coverage_edge_remains_preferred(
    engine_radar_policy_module,
):
    module = engine_radar_policy_module
    states = {"opera": _state(module), "rainviewer": _state(module)}
    decision = module.select_engine_radar_source({"TR"}, states, now=1_000.0)
    decision = module.apply_echo_availability(
        decision,
        states,
        opera_observations=2,
        rainviewer_observations=5,
        opera_coverage_complete=False,
        opera_corroborated_observations=1,
        now=1_000.0,
    )
    assert decision.source == "opera"


def test_quality_fallback_rejects_structurally_incomplete_local_radar(
    engine_radar_policy_module,
):
    module = engine_radar_policy_module
    decision = module.EngineRadarDecision(
        "kmi", "lokale officiële radar voor BE", ("BE",), 60
    )
    result = module.apply_quality_fallback(
        decision,
        {"rainviewer": _state(module, last_success=990.0)},
        rainviewer_observations=58,
        completeness_profile={
            "sample_count": 345,
            "mean_detection_ratio": 0.000584,
            "independent_confirmation_count": 12,
        },
        now=1_000.0,
    )
    assert result.source == "rainviewer"
    assert "inhoudelijk onvolledig" in result.reason


def test_quality_fallback_keeps_local_without_strong_evidence(
    engine_radar_policy_module,
):
    module = engine_radar_policy_module
    decision = module.EngineRadarDecision(
        "kmi", "lokale officiële radar voor BE", ("BE",), 60
    )
    result = module.apply_quality_fallback(
        decision,
        {"rainviewer": _state(module, last_success=990.0)},
        rainviewer_observations=58,
        completeness_profile={
            "sample_count": 12,
            "mean_detection_ratio": 0.01,
        },
        now=1_000.0,
    )
    assert result == decision


def test_quality_fallback_does_not_use_rainviewer_as_its_own_truth(
    engine_radar_policy_module,
):
    module = engine_radar_policy_module
    decision = module.EngineRadarDecision(
        "kmi", "lokale officiële radar voor BE", ("BE",), 60
    )
    result = module.apply_quality_fallback(
        decision,
        {"rainviewer": _state(module, last_success=990.0)},
        rainviewer_observations=58,
        completeness_profile={
            "sample_count": 532,
            "mean_detection_ratio": 0.025,
        },
        now=1_000.0,
    )
    assert result == decision


def test_global_enterprise_satellite_is_used_after_radar_sources_fail(
    engine_radar_policy_module,
):
    module = engine_radar_policy_module
    states = {
        "opera": module.SourceState(False, False),
        "rainviewer": module.SourceState(False, False),
        "noaa_enterprise_rainrate": module.SourceState(True, True, 990.0),
    }
    result = module.select_engine_radar_source(
        ("JP",), states, now=1_000.0
    )
    assert result.source == "noaa_enterprise_rainrate"


def test_enterprise_echo_can_replace_empty_rainviewer(
    engine_radar_policy_module,
):
    module = engine_radar_policy_module
    decision = module.EngineRadarDecision(
        "rainviewer", "test", ("JP",), 1.0
    )
    states = {
        "rainviewer": module.SourceState(True, True, 995.0),
        "noaa_enterprise_rainrate": module.SourceState(True, True, 990.0),
    }
    result = module.apply_echo_availability(
        decision,
        states,
        opera_observations=0,
        rainviewer_observations=0,
        enterprise_observations=3,
        now=1_000.0,
    )
    assert result.source == "noaa_enterprise_rainrate"
