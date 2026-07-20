import ast
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
COMPONENT = ROOT / "custom_components" / "storm_tracker_v3"


def test_config_flow_is_enabled_and_version_matches():
    manifest = json.loads((COMPONENT / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["config_flow"] is True
    assert manifest["version"] == "0.4.59"


def test_config_flow_and_translations_are_valid():
    ast.parse((COMPONENT / "config_flow.py").read_text(encoding="utf-8"))
    strings = json.loads((COMPONENT / "strings.json").read_text(encoding="utf-8"))
    dutch = json.loads(
        (COMPONENT / "translations" / "nl.json").read_text(encoding="utf-8")
    )
    for content in (strings, dutch):
        fields = content["config"]["step"]["user"]["data"]
        assert "persons" in fields
        assert "test_tracker_entity" in fields
        assert "eumetsat_consumer_key" in fields
        assert "eumetsat_consumer_secret" in fields
        assert "lightning_source_mode" in fields


def test_runtime_supports_config_entries_and_yaml():
    tree = ast.parse((COMPONENT / "__init__.py").read_text(encoding="utf-8"))
    functions = {
        node.name for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert {"async_setup", "async_setup_entry", "_async_setup_runtime"} <= functions
