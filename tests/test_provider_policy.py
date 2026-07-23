"""Contracttest voor de land-/datatypebronmatrix."""
from __future__ import annotations

import json
from pathlib import Path


POLICY = Path(__file__).resolve().parent.parent / "custom_components" / "storm_tracker_v3" / "provider_policy.json"


def test_policy_is_valid_and_germany_separates_radar_from_lightning():
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    germany = policy["countries"]["DE"]
    assert germany["radar"] == [
        "dwd_radolan", "opera", "rainviewer", "hsaf_h40b"
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
        "dpc_radar", "opera", "rainviewer", "hsaf_h40b"
    ]


def test_spain_uses_aemet_before_composite_fallbacks():
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    assert policy["countries"]["ES"]["radar"] == [
        "aemet_radar", "opera", "rainviewer", "hsaf_h40b"
    ]
