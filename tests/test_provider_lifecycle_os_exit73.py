"""Real-exit local-process crash matrix for the provider lifecycle.

The controller state is process-local. These tests prove deterministic restart
and replay only; they deliberately make no exactly-once claim for external
provider side effects.
"""
from __future__ import annotations

import ast
import asyncio
from collections import Counter
import importlib.util
import os
from pathlib import Path
import runpy
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parent.parent
LIFECYCLE_PATH = (
    ROOT / "custom_components" / "storm_tracker_v3" / "providers" / "lifecycle.py"
)
CHILD = ROOT / "tests" / "helpers" / "provider_lifecycle_os_exit73_child.py"

CHECKPOINTS = (
    "lifecycle.start.before",
    "lifecycle.start.after",
    "lifecycle.update_areas.before",
    "lifecycle.update_areas.after",
    "lifecycle.cooldown_stop.before",
    "lifecycle.cooldown_stop.after",
    "lifecycle.fetch.before",
    "lifecycle.fetch.after",
    "lifecycle.shutdown.admission_closed",
    "lifecycle.shutdown.cancel_requested",
    "lifecycle.shutdown.fetches_drained",
    "lifecycle.shutdown.provider_stop.before",
    "lifecycle.shutdown.provider_stop.after",
    "lifecycle.shutdown.complete",
)

_START = (
    "checkpoint:lifecycle.start.before",
    "plugin.start.enter",
    "plugin.start.done",
    "checkpoint:lifecycle.start.after",
)
_SHUTDOWN_PREFIX = _START + (
    "checkpoint:lifecycle.fetch.before",
    "plugin.fetch.enter",
    "checkpoint:lifecycle.shutdown.admission_closed",
)


def _load_lifecycle(base_module):
    name = "storm_tracker_v3.providers.lifecycle"
    sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(name, LIFECYCLE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load provider lifecycle")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _checkpoint_literals() -> tuple[str, ...]:
    tree = ast.parse(LIFECYCLE_PATH.read_text(encoding="utf-8"))
    points = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "_checkpoint" or not node.args:
            continue
        point = node.args[0]
        if isinstance(point, ast.Constant) and isinstance(point.value, str):
            points.append(point.value)
    return tuple(points)


def _expected_prefix(checkpoint: str) -> tuple[str, ...]:
    if checkpoint == "lifecycle.start.before":
        return ("checkpoint:lifecycle.start.before",)
    if checkpoint == "lifecycle.start.after":
        return _START
    if checkpoint == "lifecycle.update_areas.before":
        return _START + ("checkpoint:lifecycle.update_areas.before",)
    if checkpoint == "lifecycle.update_areas.after":
        return _START + (
            "checkpoint:lifecycle.update_areas.before",
            "plugin.update_areas.enter:2",
            "plugin.update_areas.done:2",
            "checkpoint:lifecycle.update_areas.after",
        )
    if checkpoint == "lifecycle.cooldown_stop.before":
        return _START + ("checkpoint:lifecycle.cooldown_stop.before",)
    if checkpoint == "lifecycle.cooldown_stop.after":
        return _START + (
            "checkpoint:lifecycle.cooldown_stop.before",
            "plugin.stop.enter",
            "plugin.stop.done",
            "checkpoint:lifecycle.cooldown_stop.after",
        )
    if checkpoint == "lifecycle.fetch.before":
        return _START + ("checkpoint:lifecycle.fetch.before",)
    if checkpoint == "lifecycle.fetch.after":
        return _START + (
            "checkpoint:lifecycle.fetch.before",
            "plugin.fetch.enter",
            "plugin.fetch.done",
            "checkpoint:lifecycle.fetch.after",
        )

    prefix = _SHUTDOWN_PREFIX
    if checkpoint == "lifecycle.shutdown.admission_closed":
        return prefix
    prefix += ("checkpoint:lifecycle.shutdown.cancel_requested",)
    if checkpoint == "lifecycle.shutdown.cancel_requested":
        return prefix
    prefix += (
        "plugin.fetch.cancelled",
        "checkpoint:lifecycle.shutdown.fetches_drained",
    )
    if checkpoint == "lifecycle.shutdown.fetches_drained":
        return prefix
    prefix += ("checkpoint:lifecycle.shutdown.provider_stop.before",)
    if checkpoint == "lifecycle.shutdown.provider_stop.before":
        return prefix
    prefix += (
        "plugin.stop.enter",
        "plugin.stop.done",
        "checkpoint:lifecycle.shutdown.provider_stop.after",
    )
    if checkpoint == "lifecycle.shutdown.provider_stop.after":
        return prefix
    return prefix + ("checkpoint:lifecycle.shutdown.complete",)


class _ReplayProvider:
    plugin_id = "closed-provider"
    capabilities = frozenset()
    priority = 1

    def __init__(self, base, *, blocking: bool) -> None:
        self._base = base
        self._blocking = blocking
        self.actions: list[str] = []
        self.fetch_started = asyncio.Event()

    def supports(self, area):
        return self._base.CoverageResult(area.center_lon >= 10.0, 1.0, 1.0)

    async def async_start(self, context) -> None:
        self.actions.extend(("start.enter", "start.done"))

    async def async_update_areas(self, areas) -> None:
        self.actions.extend((f"update.enter:{len(areas)}", f"update.done:{len(areas)}"))

    async def async_stop(self) -> None:
        self.actions.extend(("stop.enter", "stop.done"))

    async def async_fetch(self):
        self.actions.append("fetch.enter")
        self.fetch_started.set()
        if self._blocking:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.actions.append("fetch.cancelled")
                raise
        self.actions.append("fetch.done")
        return ["closed-observation-1", "closed-observation-2"]


async def _replay(checkpoint: str, lifecycle, base) -> None:
    now = [0.0]
    shutdown = checkpoint.startswith("lifecycle.shutdown.")
    provider = _ReplayProvider(base, blocking=shutdown)
    controller = lifecycle.ProviderLifecycleController(
        cooldown_seconds=60.0,
        fetch_timeout_seconds=5.0,
        clock=lambda: now[0],
    )
    controller.register(provider, lambda plugin, areas: object())
    first = base.CoverageArea(50.0, 12.0, 200.0)
    second = base.CoverageArea(51.0, 13.0, 200.0)
    await controller.async_reconcile([first])

    if checkpoint.startswith("lifecycle.start."):
        assert provider.actions == ["start.enter", "start.done"]
        assert controller.diagnostics()[provider.plugin_id]["status"] == "active"
        return
    if checkpoint.startswith("lifecycle.update_areas."):
        await controller.async_reconcile([first, second])
        assert provider.actions == [
            "start.enter", "start.done", "update.enter:2", "update.done:2"
        ]
        assert controller.diagnostics()[provider.plugin_id]["matching_engines"] == 2
        return
    if checkpoint.startswith("lifecycle.cooldown_stop."):
        await controller.async_reconcile([])
        now[0] = 60.0
        await controller.async_reconcile([])
        assert provider.actions == [
            "start.enter", "start.done", "stop.enter", "stop.done"
        ]
        assert controller.diagnostics()[provider.plugin_id]["status"] == "sleeping"
        return
    if checkpoint.startswith("lifecycle.fetch."):
        fetched = await controller.async_fetch_active()
        assert fetched == {
            provider.plugin_id: ["closed-observation-1", "closed-observation-2"]
        }
        assert provider.actions == [
            "start.enter", "start.done", "fetch.enter", "fetch.done"
        ]
        assert controller.diagnostics()[provider.plugin_id]["fetched"] == 2
        return

    fetch = asyncio.create_task(controller.async_fetch_active())
    await asyncio.wait_for(provider.fetch_started.wait(), timeout=2.0)
    await asyncio.wait_for(controller.async_stop_all(), timeout=2.0)
    assert await fetch == {}
    assert provider.actions == [
        "start.enter",
        "start.done",
        "fetch.enter",
        "fetch.cancelled",
        "stop.enter",
        "stop.done",
    ]
    diagnostics = controller.diagnostics()[provider.plugin_id]
    assert diagnostics["status"] == "sleeping"
    assert diagnostics["matching_engines"] == 0
    assert await controller.async_fetch_active() == {}
    await controller.async_reconcile([first])
    assert provider.actions.count("start.enter") == 1


@pytest.mark.parametrize("checkpoint", CHECKPOINTS, ids=CHECKPOINTS)
@pytest.mark.asyncio
async def test_provider_lifecycle_real_exit73_has_exact_prefix_and_fresh_replay(
    tmp_path: Path,
    base_module,
    checkpoint: str,
) -> None:
    assert Counter(_checkpoint_literals()) == Counter(CHECKPOINTS)
    child_globals = runpy.run_path(str(CHILD))
    assert child_globals["_CHECKPOINTS"] == frozenset(CHECKPOINTS)

    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONHASHSEED"] = "0"
    result = subprocess.run(
        [sys.executable, "-I", str(CHILD), str(tmp_path), checkpoint],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=15.0,
        check=False,
    )
    assert result.returncode == 73, (result.stdout, result.stderr)
    assert result.stdout == ""
    assert result.stderr == ""
    assert (tmp_path / "events.log").read_text(encoding="utf-8").splitlines() == list(
        _expected_prefix(checkpoint)
    )

    lifecycle = _load_lifecycle(base_module)
    await _replay(checkpoint, lifecycle, base_module)
