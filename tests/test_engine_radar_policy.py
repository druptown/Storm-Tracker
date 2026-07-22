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
