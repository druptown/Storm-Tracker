"""Repository-wide isolation gates for quantitative decoder candidates.

These checks are intentionally broader than each provider's focused tests.
They prevent a candidate decoder from becoming reachable through a production
import, handler or provider policy before a separate meteorological admission
receipt exists.
"""
from __future__ import annotations

import ast
from pathlib import Path

from storm_tracker_worker.provider_capture_registry import (
    PROVIDER_CAPTURE_POLICIES,
)
from storm_tracker_worker.service import PRODUCTION_JOB_HANDLERS


ROOT = Path(__file__).parents[1]
WORKER_ROOT = ROOT / "storm_tracker_worker"
HA_ROOT = ROOT / "custom_components" / "storm_tracker_v3"

CANDIDATE_MODULES = frozenset(
    "storm_tracker_worker." + path.stem
    for path in WORKER_ROOT.glob("*_quantitative.py")
)
SHADOW_BRIDGE_MODULES = frozenset({
    "storm_tracker_worker.canonical_radar_field_bridge",
})
ISOLATED_MODULES = CANDIDATE_MODULES | SHADOW_BRIDGE_MODULES
CANDIDATE_FILENAMES = frozenset(
    module.rsplit(".", 1)[1] + ".py" for module in ISOLATED_MODULES
)
QUANTITATIVE_PROVIDER_JOBS = frozenset({
    "dwd_radolan_fetch",
    "enterprise_rainrate_fetch",
    "knmi_fetch",
    "meteofrance_radar_fetch",
    "odim_hdf5_fetch",
    "opera_hdf5_fetch",
})

EXPECTED_CANDIDATE_MODULES = frozenset({
    "storm_tracker_worker.dwd_rv_quantitative",
    "storm_tracker_worker.knmi_rtcor_quantitative",
    "storm_tracker_worker.met_office_rate_quantitative",
    "storm_tracker_worker.noaa_err_quantitative",
    "storm_tracker_worker.opera_dbzh_quantitative",
})


def test_quantitative_candidate_inventory_is_closed() -> None:
    """A new decoder must be reviewed here before the suite can pass."""

    assert CANDIDATE_MODULES == EXPECTED_CANDIDATE_MODULES


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module)
    return found


def _handler_module(value: object) -> str | None:
    return getattr(value, "module", getattr(value, "__module__", None))


def test_no_candidate_decoder_is_a_production_handler() -> None:
    handler_modules = {
        module
        for value in PRODUCTION_JOB_HANDLERS.values()
        if (module := _handler_module(value)) is not None
    }
    assert handler_modules.isdisjoint(ISOLATED_MODULES)
    assert all(
        "quantitative" not in str(getattr(value, "qualname", ""))
        for value in PRODUCTION_JOB_HANDLERS.values()
    )


def test_no_other_worker_or_ha_module_imports_a_candidate_decoder() -> None:
    importers: list[tuple[str, str]] = []
    for source_root in (WORKER_ROOT, HA_ROOT):
        for path in source_root.rglob("*.py"):
            if path.name in CANDIDATE_FILENAMES:
                continue
            imported = _imported_modules(path)
            for candidate in sorted(ISOLATED_MODULES & imported):
                importers.append((str(path.relative_to(ROOT)), candidate))
    assert importers == []


def test_quantitative_provider_policies_keep_operational_admission_closed() -> None:
    for job_type in QUANTITATIVE_PROVIDER_JOBS:
        policy = PROVIDER_CAPTURE_POLICIES[job_type]
        assert policy.vnext_operational_admission is False


def test_candidate_modules_do_not_exist_in_home_assistant_package() -> None:
    relative_files = {
        str(path.relative_to(HA_ROOT)).replace("\\", "/")
        for path in HA_ROOT.rglob("*.py")
    }
    assert relative_files.isdisjoint(CANDIDATE_FILENAMES)
