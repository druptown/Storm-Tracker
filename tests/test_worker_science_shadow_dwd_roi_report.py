from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

import pytest

from scripts import run_science_shadow_dwd_roi_replay as replay
from scripts import seal_science_shadow_dwd_roi_resource_failure as failure_seal


ROOT = Path(__file__).resolve().parents[1]


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(item.name for item in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module)
    return result


def test_resource_summary_requires_and_accounts_all_36_children() -> None:
    frames = [
        {
            "child_cpu_seconds_sampled": float(index + 1),
            "child_reported_elapsed_seconds": float(index + 2),
            "parent_execute_wall_seconds": float(index + 3),
            "peak_child_rss_sampled_bytes": 100_000_000 + index,
        }
        for index in range(36)
    ]
    summary = replay._resource_summary(frames)

    assert summary["frame_count"] == 36
    assert summary["child_cpu_seconds"]["count"] == 36
    assert summary["child_peak_rss_bytes"]["maximum"] == 100_000_035.0
    assert summary["parent_execute_wall_seconds"]["nearest_rank_p95"] == 37.0
    assert summary["kernel_limits"]["rss_bytes_each"] == 1024**3


def test_comparison_is_descriptive_and_cannot_promote_or_rebind() -> None:
    identity = {
        "active_tracks": 1,
        "birth_events": 2,
        "death_events": 1,
        "ended_tracks": 1,
        "hypothesis_interval_count": 3,
        "hypothesis_jump_count_over_200_kmh": 0,
        "hypothesis_speed_maximum_kmh": 12.0,
        "hypothesis_speed_median_kmh": 8.0,
        "hypothesis_speed_p95_kmh": 12.0,
        "total_tracks": 2,
        "track_length_maximum_observations": 3,
        "track_length_mean_observations": 2.0,
        "track_length_median_observations": 2.0,
    }
    result = replay._comparison(
        {"audit": identity},
        {"audit": {"multi_scan": {"selected_decision_path": identity}}},
    )

    assert result["accuracy_claimed"] is False
    assert result["independent_identity_truth_available"] is False
    assert result["promotion_or_rebinding_permitted"] is False
    assert result["weather_system_rebinding_performed"] is False
    assert result["baseline_used_for_tracker_tuning"] is False


def test_offline_report_has_no_live_or_network_imports() -> None:
    imports = _imports(ROOT / "scripts" / "run_science_shadow_dwd_roi_replay.py")
    assert not imports.intersection(
        {
            "aiohttp", "custom_components", "http", "paho", "requests",
            "socket", "urllib", "websockets",
        }
    )
    source = (ROOT / "scripts" / "run_science_shadow_dwd_roi_replay.py").read_text(
        encoding="utf-8"
    )
    assert "weather_system_rebinding_performed\": False" in source


def test_sealed_ordered_identity_hashes_are_explicit() -> None:
    assert replay.ORDERED_EVIDENCE_RECORD_CONCAT_SHA256 == (
        "9fac976800e063fb44da4d1344f6daec1aba4ecd53ccf0d908cf55d94eab4777"
    )
    assert replay.ORDERED_RUN_IDS_SHA256 == (
        "16f439ad52481c8086afc206ec2fda9ee9365e0fa4c27cb090632ce5b09e5ebf"
    )


def test_failure_sealer_is_unwired_and_preserves_exact_terminal_status() -> None:
    imports = _imports(
        ROOT / "scripts" / "seal_science_shadow_dwd_roi_resource_failure.py"
    )
    assert not imports.intersection(
        {
            "aiohttp", "custom_components", "http", "paho", "requests",
            "socket", "urllib", "websockets",
        }
    )
    assert failure_seal.V5_FAILURE["status"] == "resource"
    assert failure_seal.V5_FAILURE["execution_outcome"] == "cpu_limit_exceeded"
    assert failure_seal.V5_FAILURE["durable_disposition"] == "not_committed"
    assert failure_seal.V5_FAILURE["receipt_metadata"] is None
    assert failure_seal.V5_FAILURE["meteorological_admission_granted"] is False


def test_generated_failure_evidence_is_canonical_and_fail_closed() -> None:
    path = failure_seal.OUTPUT
    if not path.is_file():
        pytest.skip("generated DWD ROI failure evidence is not present")
    body = path.read_bytes()
    document = json.loads(body)

    assert json.dumps(
        document,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8") == body
    assert hashlib.sha256(body).hexdigest() == (
        "74b0f1678f8a1d1e5270bd1c21ec7ca773e231b0b316929bc74daac2f44332fd"
    )
    assert document["v4_1_complete"]["state"]["revision"] == 36
    assert document["v4_1_complete"]["audit"][
        "candidate_accounting_exact_all_frames"
    ] is True
    assert document["v5_resource_negative"]["durable_partial_head_revision"] == 18
    assert document["v5_resource_negative"]["frame_19_committed"] is False
    assert document["v5_resource_negative"][
        "no_retry_or_limit_change_performed"
    ] is True
    assert document["weather_system_rebinding_performed"] is False
    assert document["worker_processes_after_audit"] == 0
