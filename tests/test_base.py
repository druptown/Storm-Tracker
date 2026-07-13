"""Storm Tracker V3 — tests/test_base.py v0.2.0

Unit tests voor het plugincontract (providers/base.py).

Test coverage:
  - CoverageArea.contains()
  - CoverageResult.score
  - compute_observation_horizon()
  - ProviderRegistry strategie per capability
  - ProviderPlugin Protocol check

Versiegeschiedenis:
  v0.2.0 — portable gemaakt (geen hardcoded pad meer); omgezet naar pytest;
            modules geladen via gedeelde conftest-fixtures
  v0.1.0 — eerste versie (hardcoded pad /home/claude/stv3/...)
"""
from __future__ import annotations

import pytest


def test_compute_observation_horizon(base_module):
    horizon = base_module.compute_observation_horizon(
        forecast_time_h=2.0, max_storm_speed_kmh=100.0
    )
    assert horizon == 200.0

    horizon_fast = base_module.compute_observation_horizon(
        forecast_time_h=2.0, max_storm_speed_kmh=150.0
    )
    assert horizon_fast == 300.0


def test_coverage_area_contains(base_module):
    CoverageArea = base_module.CoverageArea
    heffen = CoverageArea(center_lat=51.026, center_lon=4.478, horizon_km=200.0)

    assert heffen.contains(51.026, 4.478) is True     # zelf
    assert heffen.contains(52.37, 4.89) is True        # Amsterdam ~152km
    assert heffen.contains(48.857, 2.351) is False     # Parijs ~285km > 200km horizon
    assert heffen.contains(25.77, -80.19) is False     # Miami


def test_coverage_result_score(base_module):
    CoverageResult = base_module.CoverageResult

    r1 = CoverageResult(supported=True, coverage_fraction=0.95, quality=0.9)
    r2 = CoverageResult(supported=True, coverage_fraction=1.0, quality=0.6)
    r3 = CoverageResult(supported=False, coverage_fraction=0.0, quality=0.0)

    assert r1.score > r2.score, "KMI (0.95x0.9) moet hoger scoren dan RainViewer (1.0x0.6)"
    assert r3.score == 0.0


def _make_plugin(base_module, plugin_id, capabilities, priority, coverage_result):
    """Bouw een minimale mock-plugin die het ProviderPlugin-protocol volgt."""

    class _MockPlugin:
        pass

    p = _MockPlugin()
    p.plugin_id = plugin_id
    p.capabilities = capabilities
    p.priority = priority
    p.supports = lambda area: coverage_result

    async def _noop(*a, **kw):
        return None

    async def _fetch(*a, **kw):
        return []

    p.async_start = _noop
    p.async_stop = _noop
    p.async_fetch = _fetch
    return p


def test_registry_lightning_always_all(base_module):
    Capability, CoverageArea, CoverageResult, ProviderRegistry = (
        base_module.Capability, base_module.CoverageArea,
        base_module.CoverageResult, base_module.ProviderRegistry,
    )
    blitz = _make_plugin(
        base_module, "blitzortung", frozenset([Capability.LIGHTNING]), 100,
        CoverageResult(True, 1.0, 1.0),
    )
    registry = ProviderRegistry()
    registry.register(blitz)
    area = CoverageArea(51.026, 4.478, 200.0)

    selected = registry.select_for_area(area, Capability.LIGHTNING)
    assert len(selected) == 1
    assert selected[0].plugin_id == "blitzortung"


def test_registry_radar_best_plus_fallback(base_module):
    Capability, CoverageArea, CoverageResult, ProviderRegistry = (
        base_module.Capability, base_module.CoverageArea,
        base_module.CoverageResult, base_module.ProviderRegistry,
    )
    kmi = _make_plugin(base_module, "kmi", frozenset([Capability.RADAR]), 100,
                        CoverageResult(True, 0.95, 0.9))
    rainviewer = _make_plugin(base_module, "rainviewer", frozenset([Capability.RADAR]), 40,
                               CoverageResult(True, 1.0, 0.6))

    registry = ProviderRegistry()
    registry.register(kmi)
    registry.register(rainviewer)
    area = CoverageArea(51.026, 4.478, 200.0)

    selected = registry.select_for_area(area, Capability.RADAR)
    assert len(selected) == 2, "verwacht 2 (primair + fallback)"
    assert selected[0].plugin_id == "kmi"
    assert selected[1].plugin_id == "rainviewer"


def test_registry_rain_gauge_combines_all(base_module):
    Capability, CoverageArea, CoverageResult, ProviderRegistry = (
        base_module.Capability, base_module.CoverageArea,
        base_module.CoverageResult, base_module.ProviderRegistry,
    )
    netatmo = _make_plugin(base_module, "netatmo", frozenset([Capability.RAIN_GAUGE]), 100,
                            CoverageResult(True, 0.8, 0.85))
    nationaal = _make_plugin(base_module, "nationaal_station", frozenset([Capability.RAIN_GAUGE]), 90,
                              CoverageResult(True, 0.5, 0.95))

    registry = ProviderRegistry()
    registry.register(netatmo)
    registry.register(nationaal)
    area = CoverageArea(51.026, 4.478, 200.0)

    selected = registry.select_for_area(area, Capability.RAIN_GAUGE)
    assert len(selected) == 2, "RAIN_GAUGE moet alle bronnen combineren"


def test_registry_excludes_unsupported_area(base_module):
    Capability, CoverageArea, CoverageResult, ProviderRegistry = (
        base_module.Capability, base_module.CoverageArea,
        base_module.CoverageResult, base_module.ProviderRegistry,
    )

    def _kmi_supports(area):
        if area.center_lat < 48 or area.center_lon > 11:
            return CoverageResult(False, 0.0, 0.0, "buiten KMI dekkingsgebied")
        return CoverageResult(True, 0.95, 0.9)

    kmi = _make_plugin(base_module, "kmi", frozenset([Capability.RADAR]), 100,
                        CoverageResult(True, 0.95, 0.9))
    kmi.supports = _kmi_supports
    rainviewer = _make_plugin(base_module, "rainviewer", frozenset([Capability.RADAR]), 40,
                               CoverageResult(True, 1.0, 0.6))

    registry = ProviderRegistry()
    registry.register(kmi)
    registry.register(rainviewer)

    japan = CoverageArea(35.68, 139.69, 200.0)
    selected = registry.select_for_area(japan, Capability.RADAR)
    assert len(selected) == 1
    assert selected[0].plugin_id == "rainviewer"
