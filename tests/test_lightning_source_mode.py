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
