"""Tests voor gedeelde, locatiegestuurde provideractivatie."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parent.parent


def _load_lifecycle(base_module):
    name = "storm_tracker_v3.providers.lifecycle"
    sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(
        name, ROOT / "custom_components/storm_tracker_v3/providers/lifecycle.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class FakePlugin:
    plugin_id = "national"
    capabilities = frozenset()
    priority = 100

    def __init__(self):
        self.starts = 0
        self.stops = 0
        self.fetches = 0

    def supports(self, area):
        base = sys.modules["storm_tracker_v3.providers.base"]
        return base.CoverageResult(area.center_lon >= 10, 1.0, 1.0)

    async def async_start(self, context):
        self.starts += 1

    async def async_stop(self):
        self.stops += 1

    async def async_fetch(self):
        self.fetches += 1
        return [object(), object()]


@pytest.mark.asyncio
async def test_provider_sleeps_until_matching_engine_and_is_shared(base_module):
    lifecycle = _load_lifecycle(base_module)
    now = [0.0]
    controller = lifecycle.ProviderLifecycleController(
        cooldown_seconds=60, clock=lambda: now[0]
    )
    plugin = FakePlugin()
    controller.register(plugin, lambda plugin, areas: object())
    area = base_module.CoverageArea(50, 12, 200)
    overlapping = base_module.CoverageArea(51, 13, 200)

    await controller.async_reconcile([])
    assert controller.diagnostics()["national"]["status"] == "sleeping"
    await controller.async_reconcile([area, overlapping])
    assert plugin.starts == 1
    assert controller.diagnostics()["national"]["matching_engines"] == 2

    result = await controller.async_fetch_active()
    assert len(result["national"]) == 2
    assert plugin.fetches == 1


@pytest.mark.asyncio
async def test_provider_stops_only_after_cooldown(base_module):
    lifecycle = _load_lifecycle(base_module)
    now = [0.0]
    controller = lifecycle.ProviderLifecycleController(
        cooldown_seconds=60, clock=lambda: now[0]
    )
    plugin = FakePlugin()
    controller.register(plugin, lambda plugin, areas: object())
    await controller.async_reconcile([base_module.CoverageArea(50, 12, 200)])
    await controller.async_reconcile([])
    assert controller.diagnostics()["national"]["status"] == "cooldown"
    now[0] = 59
    await controller.async_reconcile([])
    assert plugin.stops == 0
    now[0] = 60
    await controller.async_reconcile([])
    assert plugin.stops == 1
    assert controller.diagnostics()["national"]["status"] == "sleeping"


@pytest.mark.asyncio
async def test_engine_return_during_cooldown_reuses_provider(base_module):
    lifecycle = _load_lifecycle(base_module)
    now = [0.0]
    controller = lifecycle.ProviderLifecycleController(clock=lambda: now[0])
    plugin = FakePlugin()
    controller.register(plugin, lambda plugin, areas: object())
    area = base_module.CoverageArea(50, 12, 200)
    await controller.async_reconcile([area])
    await controller.async_reconcile([])
    await controller.async_reconcile([area])
    assert plugin.starts == 1
    assert plugin.stops == 0
    assert controller.diagnostics()["national"]["status"] == "active"

