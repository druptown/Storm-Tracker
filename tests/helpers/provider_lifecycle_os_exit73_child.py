"""Closed child process for provider-lifecycle local crash checkpoints."""
from __future__ import annotations

import asyncio
import importlib.util
import os
from pathlib import Path
import sys
import types
from typing import Sequence


REPOSITORY = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPOSITORY / "custom_components" / "storm_tracker_v3"

_CHECKPOINTS = frozenset(
    {
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
    }
)


def _package(name: str) -> None:
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        module.__path__ = []
        sys.modules[name] = module


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load closed lifecycle module: {name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _modules():
    _package("storm_tracker_v3")
    _package("storm_tracker_v3.engine")
    _package("storm_tracker_v3.providers")
    _load(
        "storm_tracker_v3.engine.observation",
        PACKAGE_ROOT / "engine" / "observation.py",
    )
    base = _load(
        "storm_tracker_v3.providers.base",
        PACKAGE_ROOT / "providers" / "base.py",
    )
    lifecycle = _load(
        "storm_tracker_v3.providers.lifecycle",
        PACKAGE_ROOT / "providers" / "lifecycle.py",
    )
    return base, lifecycle


class _DurableMarker:
    def __init__(self, path: Path) -> None:
        self._path = path

    def write(self, event: str) -> None:
        with self._path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(f"{event}\n")
            stream.flush()
            os.fsync(stream.fileno())


class _Provider:
    plugin_id = "closed-provider"
    capabilities = frozenset()
    priority = 1

    def __init__(self, base, marker: _DurableMarker, *, blocking: bool) -> None:
        self._base = base
        self._marker = marker
        self._blocking = blocking
        self.fetch_started = asyncio.Event()

    def supports(self, area):
        return self._base.CoverageResult(
            area.center_lon >= 10.0,
            1.0,
            1.0,
        )

    async def async_start(self, context) -> None:
        self._marker.write("plugin.start.enter")
        self._marker.write("plugin.start.done")

    async def async_update_areas(self, areas) -> None:
        self._marker.write(f"plugin.update_areas.enter:{len(areas)}")
        self._marker.write(f"plugin.update_areas.done:{len(areas)}")

    async def async_stop(self) -> None:
        self._marker.write("plugin.stop.enter")
        self._marker.write("plugin.stop.done")

    async def async_fetch(self):
        self._marker.write("plugin.fetch.enter")
        self.fetch_started.set()
        if self._blocking:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self._marker.write("plugin.fetch.cancelled")
                raise
        self._marker.write("plugin.fetch.done")
        return ["closed-observation-1", "closed-observation-2"]


async def _run(root: Path, target: str) -> None:
    base, lifecycle = _modules()
    marker = _DurableMarker(root / "events.log")

    def crash(point: str) -> None:
        marker.write(f"checkpoint:{point}")
        if point == target:
            os._exit(73)

    now = [0.0]
    shutdown = target.startswith("lifecycle.shutdown.")
    provider = _Provider(base, marker, blocking=shutdown)
    controller = lifecycle.ProviderLifecycleController(
        cooldown_seconds=60.0,
        fetch_timeout_seconds=5.0,
        clock=lambda: now[0],
        fault_injector=crash,
    )
    controller.register(provider, lambda plugin, areas: object())
    first = base.CoverageArea(50.0, 12.0, 200.0)
    second = base.CoverageArea(51.0, 13.0, 200.0)

    await controller.async_reconcile([first])
    if target.startswith("lifecycle.start."):
        return
    if target.startswith("lifecycle.update_areas."):
        await controller.async_reconcile([first, second])
        return
    if target.startswith("lifecycle.cooldown_stop."):
        await controller.async_reconcile([])
        now[0] = 60.0
        await controller.async_reconcile([])
        return
    if target.startswith("lifecycle.fetch."):
        await controller.async_fetch_active()
        return

    fetch = asyncio.create_task(controller.async_fetch_active())
    await asyncio.wait_for(provider.fetch_started.wait(), timeout=2.0)
    await controller.async_stop_all()
    await fetch


def main(arguments: Sequence[str]) -> None:
    if len(arguments) != 2:
        raise SystemExit(
            "usage: provider_lifecycle_os_exit73_child.py ROOT CHECKPOINT"
        )
    root_value, checkpoint = arguments
    if checkpoint not in _CHECKPOINTS:
        raise SystemExit("provider lifecycle checkpoint is not allowlisted")
    root = Path(root_value)
    if not root.is_absolute() or not root.is_dir() or root.is_symlink():
        raise SystemExit(
            "provider lifecycle root must be an existing real directory"
        )
    asyncio.run(_run(root.resolve(), checkpoint))
    raise RuntimeError("allowlisted provider lifecycle checkpoint was not reached")


if __name__ == "__main__":
    main(sys.argv[1:])
