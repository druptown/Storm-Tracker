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
        self.overlay = {"source": "national", "runs": []}

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


class SlowPlugin(FakePlugin):
    plugin_id = "slow"

    async def async_fetch(self):
        await __import__("asyncio").sleep(1)
        return []


class FailingPlugin(FakePlugin):
    plugin_id = "failing"

    async def async_fetch(self):
        raise RuntimeError("offline")


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
    assert controller.overlay("national") is plugin.overlay
    assert controller.overlay("missing") is None


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


@pytest.mark.asyncio
async def test_slow_provider_is_cancelled_by_hard_timeout(base_module):
    lifecycle = _load_lifecycle(base_module)
    controller = lifecycle.ProviderLifecycleController(
        fetch_timeout_seconds=0.01, failure_threshold=1
    )
    plugin = SlowPlugin()
    controller.register(plugin, lambda plugin, areas: object())
    await controller.async_reconcile([base_module.CoverageArea(50, 12, 200)])

    assert await controller.async_fetch_active() == {}
    diagnostics = controller.diagnostics()["slow"]
    assert diagnostics["status"] == "cooldown"
    assert diagnostics["error"] == "timeout"


@pytest.mark.asyncio
async def test_three_failures_open_circuit_until_controlled_probe(base_module):
    lifecycle = _load_lifecycle(base_module)
    now = [0.0]
    controller = lifecycle.ProviderLifecycleController(
        failure_threshold=3,
        circuit_breaker_seconds=900,
        clock=lambda: now[0],
    )
    plugin = FailingPlugin()
    controller.register(plugin, lambda plugin, areas: object())
    area = base_module.CoverageArea(50, 12, 200)
    await controller.async_reconcile([area])

    for expected in (1, 2, 3):
        assert await controller.async_fetch_active() == {}
        assert controller.diagnostics()["failing"]["consecutive_failures"] == expected

    diagnostics = controller.diagnostics()["failing"]
    assert diagnostics["status"] == "cooldown"
    assert diagnostics["circuit_open_until"] == 900
    await controller.async_reconcile([area])
    assert await controller.async_fetch_active() == {}
    assert diagnostics["consecutive_failures"] == 3

    now[0] = 900
    await controller.async_reconcile([area])
    assert controller.diagnostics()["failing"]["status"] == "active"

