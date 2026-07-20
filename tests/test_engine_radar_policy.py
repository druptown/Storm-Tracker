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


def test_spain_prefers_aemet(engine_radar_policy_module):
    module = engine_radar_policy_module
    decision = module.select_engine_radar_source(
        {"ES"}, {"aemet_radar": _state(module), "opera": _state(module)},
        now=1_000.0,
    )
    assert decision.source == "aemet_radar"
