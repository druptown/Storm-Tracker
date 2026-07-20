"""Contracttest voor de land-/datatypebronmatrix."""
from __future__ import annotations

import json
from pathlib import Path


POLICY = Path(__file__).resolve().parent.parent / "custom_components" / "storm_tracker_v3" / "provider_policy.json"


def test_policy_is_valid_and_germany_separates_radar_from_lightning():
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    germany = policy["countries"]["DE"]
    assert germany["radar"] == ["opera", "dwd_radolan", "rainviewer"]
    assert "eumetsat_li" not in germany["radar"]
    assert germany["lightning"] == ["blitzortung", "eumetsat_li"]


def test_every_country_has_core_capability_lists():
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    for country, config in policy["countries"].items():
        assert len(country) == 2
        for capability in ("radar", "lightning", "ground_validation"):
            assert isinstance(config[capability], list)
            assert config[capability]
