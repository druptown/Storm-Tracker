"""Contracts for the thin-HA/provider-worker boundary."""
from __future__ import annotations

import ast
from pathlib import Path

import numpy as np

from storm_tracker_worker import provider_radar
from storm_tracker_worker.service import (
    PRODUCTION_JOB_EFFECT_CONTRACTS,
    PRODUCTION_JOB_HANDLERS,
)

ROOT = Path(__file__).parents[1]


def _source(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_every_image_provider_job_is_process_isolated_and_kill_safe():
    expected = {
        "rainviewer_fetch",
        "kmi_fetch",
        "knmi_fetch",
        "aemet_radar_fetch",
        "dpc_radar_fetch",
        "meteofrance_radar_fetch",
    }
    assert expected <= set(PRODUCTION_JOB_HANDLERS)
    assert expected <= set(PRODUCTION_JOB_EFFECT_CONTRACTS)
    assert all(
        PRODUCTION_JOB_EFFECT_CONTRACTS[name].hard_kill_safe
        for name in expected
    )


def test_live_provider_instances_receive_hass_and_use_worker_jobs():
    runtime = _source("custom_components/storm_tracker_v3/__init__.py")
    assert "KmiProvider(\n                kmi_region.center_lat, kmi_region.center_lon, hass=hass" in runtime
    assert "hass=hass,\n            )" in runtime
    assert "RainViewerProvider(\n                    region.center_lat, region.center_lon, hass=hass" in runtime
    for relative, job_type in (
        ("custom_components/storm_tracker_v3/providers/rainviewer.py", "rainviewer_fetch"),
        ("custom_components/storm_tracker_v3/providers/kmi.py", "kmi_fetch"),
        ("custom_components/storm_tracker_v3/providers/knmi.py", "knmi_fetch"),
        ("custom_components/storm_tracker_v3/providers/aemet_radar.py", "aemet_radar_fetch"),
        ("custom_components/storm_tracker_v3/providers/dpc_radar.py", "dpc_radar_fetch"),
        ("custom_components/storm_tracker_v3/providers/meteofrance_radar.py", "meteofrance_radar_fetch"),
    ):
        source = _source(relative)
        assert "if self._hass is not None:" in source
        assert f'"{job_type}"' in source
        assert "lokale verwerking blijft uit" in source or "lokale decode is bewust uitgeschakeld" in source


def test_rainviewer_vectorized_palette_matches_public_decoder_contract():
    rgba = np.asarray([[
        [0, 0, 0, 129],
        [0, 0, 255, 130],
        [0, 0, 255, 189],
        [120, 120, 120, 255],
        [0, 0, 200, 255],
        [255, 255, 255, 255],
    ]], dtype=np.uint8)
    assert provider_radar._rainviewer_grid(rgba).tolist() == [[0, 1, 2, 0, 3, 8]]


def test_integration_has_real_config_entry_unload():
    tree = ast.parse(_source("custom_components/storm_tracker_v3/__init__.py"))
    unload = next(
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "async_unload_entry"
    )
    text = ast.unparse(unload)
    assert "async_unload_worker_only_entry" in text
    assert "RUNTIME_MODE_WORKER_ONLY" in text
    for forbidden in (
        "async_shutdown_runtime",
        "_async_setup_runtime",
        "provider",
        "poll",
        "Store",
    ):
        assert forbidden not in text


def test_tracking_is_worker_only_and_accepts_large_structured_system_sets():
    integration = _source("custom_components/storm_tracker_v3/__init__.py")
    worker = _source("storm_tracker_worker/tracking.py")
    assert "calculate_batch_candidate" not in integration
    assert "lokale_tracking_fallback" not in integration
    assert "MAX_SNAPSHOTS = 2048" in worker
