"""Contracttests voor de diagnostische bliksembronmodus."""
from __future__ import annotations

import ast
import logging
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parent.parent
INIT_FILE = ROOT / "custom_components" / "storm_tracker_v3" / "__init__.py"


def _function():
    tree = ast.parse(INIT_FILE.read_text(encoding="utf-8"))
    node = next(
        item for item in tree.body
        if isinstance(item, ast.FunctionDef) and item.name == "_use_satellite_lightning"
    )
    module = ast.Module(body=[node], type_ignores=[])
    namespace = {}
    exec(compile(module, str(INIT_FILE), "exec"), namespace)
    return namespace["_use_satellite_lightning"]


def _options_listener():
    tree = ast.parse(INIT_FILE.read_text(encoding="utf-8"))
    nodes = [
        item for item in tree.body
        if (
            isinstance(item, ast.FunctionDef)
            and item.name == "_explicit_entry_mode"
        ) or (
            isinstance(item, ast.AsyncFunctionDef)
            and item.name == "_async_options_updated"
        )
    ]
    module = ast.Module(body=nodes, type_ignores=[])
    namespace = {
        "HomeAssistant": object,
        "DOMAIN": "storm_tracker_v3",
        "DATA_ENTRY_RUNTIMES": "entry_runtimes",
        "CONF_RUNTIME_MODE": "runtime_mode",
        "RUNTIME_MODE_LEGACY": "legacy",
        "RUNTIME_MODE_WORKER_ONLY": "worker_only",
        "_LIGHTNING_SOURCE_MODES": frozenset({"auto", "satellite_test"}),
        "_LOGGER": logging.getLogger(__name__),
    }
    exec(compile(module, str(INIT_FILE), "exec"), namespace)
    return namespace["_async_options_updated"]


class _Entry:
    def __init__(self, data=None, options=None):
        self.data = dict(data or {})
        self.data.setdefault("runtime_mode", "worker_only")
        self.options = dict(options or {})
        self.entry_id = "lightning-owner"


class _Hass:
    def __init__(self, setter=None):
        runtime = {
            "entry_runtimes": {
                "lightning-owner": {"mode": "worker_only"},
            }
        }
        if setter is not None:
            runtime["set_lightning_source_mode"] = setter
        self.data = {"storm_tracker_v3": runtime}


def test_auto_uses_blitz_when_connected():
    assert _function()(True, "auto") is False


def test_auto_uses_satellite_when_blitz_is_offline():
    assert _function()(False, "auto") is True


def test_satellite_test_overrides_connected_blitz():
    assert _function()(True, "satellite_test") is True


def test_options_updates_are_wired_to_live_runtime_setter():
    source = INIT_FILE.read_text(encoding="utf-8")
    assert "entry.add_update_listener(_async_options_updated)" in source
    assert "entry.async_on_unload(entry.add_update_listener(" in source
    assert 'hass.data[DOMAIN]["set_lightning_source_mode"]' in source
    assert "updated_config = {" in source
    assert "**updated_entry.data" in source
    assert "**updated_entry.options" in source
    assert 'updated_config.get("lightning_source_mode", "auto")' in source


@pytest.mark.asyncio
async def test_options_listener_merges_current_values_and_applies_repeated_updates():
    applied = []

    async def setter(source_mode):
        applied.append(source_mode)

    hass = _Hass(setter)
    entry = _Entry(
        {"lightning_source_mode": "satellite_test"},
        {"lightning_source_mode": "auto"},
    )
    listener = _options_listener()

    await listener(hass, entry)
    entry.options["lightning_source_mode"] = "satellite_test"
    await listener(hass, entry)

    assert applied == ["auto", "satellite_test"]


@pytest.mark.asyncio
async def test_options_listener_defaults_to_auto_and_fails_closed():
    applied = []

    async def setter(source_mode):
        applied.append(source_mode)

    listener = _options_listener()
    hass = _Hass(setter)
    entry = _Entry()
    await listener(hass, entry)
    for invalid_mode in ("invalid", None, ["auto"]):
        entry.options["lightning_source_mode"] = invalid_mode
        await listener(hass, entry)
    assert applied == ["auto"]

    # Een update tijdens setup/unload heeft geen setter om aan te roepen en
    # moet zonder bijwerking worden geweigerd.
    await listener(_Hass(), _Entry(options={"lightning_source_mode": "auto"}))


def test_runtime_mode_is_not_captured_by_provider_callbacks():
    source = INIT_FILE.read_text(encoding="utf-8")
    assert 'hass.data[DOMAIN].get("lightning_source_mode", "auto")' in source
    assert "if satellite_test_mode:" not in source


def test_live_switch_stops_blitz_and_polls_satellite_immediately():
    source = INIT_FILE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    runtime = next(
        item for item in tree.body
        if isinstance(item, ast.AsyncFunctionDef) and item.name == "_async_setup_runtime"
    )
    setter = next(
        item for item in ast.walk(runtime)
        if isinstance(item, ast.AsyncFunctionDef)
        and item.name == "_set_lightning_source_mode"
    )
    setter_source = ast.get_source_segment(source, setter)
    assert "blitz.stop()" in setter_source
    assert "await _poll_eumetsat_li()" in setter_source
    assert "await _poll_goes_glm()" in setter_source
    assert "blitz.start()" in setter_source
