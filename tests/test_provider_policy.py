"""Contracttest voor de land-/datatypebronmatrix."""
from __future__ import annotations

import json
import importlib.util
from pathlib import Path


POLICY = Path(__file__).resolve().parent.parent / "custom_components" / "storm_tracker_v3" / "provider_policy.json"
REGISTRY = POLICY.parent / "providers" / "registry.py"
_spec = importlib.util.spec_from_file_location("stv3_provider_registry", REGISTRY)
_registry = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_registry)
IMPLEMENTED_RADAR_PROVIDERS = _registry.IMPLEMENTED_RADAR_PROVIDERS
IMPLEMENTED_GROUND_PROVIDERS = _registry.IMPLEMENTED_GROUND_PROVIDERS


def test_policy_is_valid_and_germany_separates_radar_from_lightning():
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    germany = policy["countries"]["DE"]
    assert germany["radar"] == [
        "dwd_radolan", "opera", "rainviewer",
        "noaa_enterprise_rainrate", "hsaf_h40b"
    ]
    assert "eumetsat_li" not in germany["radar"]
    assert germany["lightning"] == ["blitzortung", "eumetsat_li"]


def test_every_country_has_core_capability_lists():
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    assert policy["schema_version"] == 2
    for country, config in policy["countries"].items():
        assert len(country) == 2
        for capability in (
            "radar", "lightning", "ground_validation", "model_guidance"
        ):
            assert isinstance(config[capability], list)
        assert config["radar"]
        assert config["lightning"]
        assert config["model_guidance"]
        assert set(config["radar"]) <= IMPLEMENTED_RADAR_PROVIDERS
        assert (
            set(config["ground_validation"])
            <= IMPLEMENTED_GROUND_PROVIDERS
        )
    assert set(policy["default"]["radar"]) <= IMPLEMENTED_RADAR_PROVIDERS
    assert (
        set(policy["default"]["ground_validation"])
        <= IMPLEMENTED_GROUND_PROVIDERS
    )


def test_open_meteo_is_model_guidance_not_ground_truth():
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    assert policy["default"]["model_guidance"] == ["open_meteo"]
    assert "open_meteo" not in policy["default"]["ground_validation"]
    for config in policy["countries"].values():
        assert "open_meteo" in config["model_guidance"]
        assert "open_meteo" not in config["ground_validation"]


def test_italy_uses_dpc_before_composite_fallbacks():
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    assert policy["countries"]["IT"]["radar"] == [
        "dpc_radar", "opera", "rainviewer",
        "noaa_enterprise_rainrate", "hsaf_h40b"
    ]


def test_spain_uses_aemet_before_composite_fallbacks():
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    assert policy["countries"]["ES"]["radar"] == [
        "aemet_radar", "opera", "rainviewer",
        "noaa_enterprise_rainrate", "hsaf_h40b"
    ]


def test_ground_policy_lists_global_metar_and_north_american_synoptic():
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    assert policy["default"]["ground_validation"] == ["netatmo", "metar"]
    assert policy["countries"]["US"]["ground_validation"] == [
        "netatmo",
        "synoptic",
        "metar",
    ]
    assert policy["countries"]["CA"]["ground_validation"] == [
        "netatmo",
        "synoptic",
        "metar",
    ]
    assert policy["countries"]["BE"]["ground_validation"] == [
        "netatmo",
        "metar",
    ]
    assert policy["countries"]["IT"]["ground_validation"] == [
        "netatmo",
        "metar",
    ]
    assert all(
        "metar" in config["ground_validation"]
        for config in policy["countries"].values()
    )
