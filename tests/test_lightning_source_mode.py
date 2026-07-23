"""Contracttests voor de diagnostische bliksembronmodus."""
from __future__ import annotations

import ast
from pathlib import Path


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


def test_auto_uses_blitz_when_connected():
    assert _function()(True, "auto") is False


def test_auto_uses_satellite_when_blitz_is_offline():
    assert _function()(False, "auto") is True


def test_satellite_test_overrides_connected_blitz():
    assert _function()(True, "satellite_test") is True


def test_options_updates_are_wired_to_live_runtime_setter():
    source = INIT_FILE.read_text(encoding="utf-8")
    assert "entry.add_update_listener(_async_options_updated)" in source
    assert 'hass.data[DOMAIN]["set_lightning_source_mode"]' in source
    assert "updated_config = {" in source
    assert "**updated_entry.data" in source
    assert "**updated_entry.options" in source
    assert 'updated_config.get("lightning_source_mode", "auto")' in source


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
