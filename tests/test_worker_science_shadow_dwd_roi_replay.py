from __future__ import annotations

from dataclasses import replace
import hashlib
import multiprocessing
from pathlib import Path
import time

import pytest

from storm_tracker_worker.science_shadow_dwd_roi_export import (
    dwd_germany_roi_profile,
)
from storm_tracker_worker.science_shadow_dwd_roi_replay import (
    DWD_ROI_SCIENCE_BLOCKERS,
    DwdScienceReplayError,
    EXPECTED_FRAME_COUNT,
    EXPECTED_METRICS_MANIFEST_SHA256,
    EXPECTED_SEQUENCE_ID,
    EXPECTED_SEQUENCE_MANIFEST_SHA256,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    build_dwd_roi_science_replay_orchestrator,
    create_dwd_roi_science_replay_authority,
    dwd_roi_science_tracking_policy,
    load_dwd_roi_science_replay_export,
    recover_dwd_roi_science_replay_state,
)
from storm_tracker_worker.science_shadow_process_isolation import (
    ScienceShadowIsolatedRunner,
)


REPOSITORY = Path(__file__).resolve().parents[1]
EXPORT_ROOT = REPOSITORY / ".analysis" / "dwd-rv-germany-roi-2km-36-export-v1"
SEQUENCE_MANIFEST = (
    EXPORT_ROOT
    / "manifests"
    / f"{EXPECTED_SEQUENCE_MANIFEST_SHA256}.json"
)


def _require_export() -> None:
    if not SEQUENCE_MANIFEST.is_file():
        pytest.skip("immutable DWD Germany ROI export is not present")


def _inventory(root: Path) -> dict[str, tuple[int, str]]:
    return {
        path.relative_to(root).as_posix(): (
            path.stat().st_size,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _authority(tmp_path: Path, mode: str):
    return create_dwd_roi_science_replay_authority(
        export_root=EXPORT_ROOT.resolve(),
        sequence_manifest_path=SEQUENCE_MANIFEST.resolve(),
        receipt_path=tmp_path / f"{mode}.sqlite3",
        authority_path=tmp_path / f"{mode}.authority.json",
        identity_mode=mode,
    )


def test_exact_roi_export_is_read_only_physical_evidence() -> None:
    _require_export()
    before = _inventory(EXPORT_ROOT)
    export = load_dwd_roi_science_replay_export(
        EXPORT_ROOT.resolve(), SEQUENCE_MANIFEST.resolve()
    )
    after = _inventory(EXPORT_ROOT)

    assert before == after
    assert export.sequence_id == EXPECTED_SEQUENCE_ID
    assert export.source_manifest_sha256 == EXPECTED_SOURCE_MANIFEST_SHA256
    assert export.metrics_manifest_sha256 == EXPECTED_METRICS_MANIFEST_SHA256
    assert len(export.entries) == EXPECTED_FRAME_COUNT
    assert export.evidence_aggregate_bytes == 28_769_643
    assert (min(e.finite_pixels for e in export.entries),
            max(e.finite_pixels for e in export.entries),
            sum(e.finite_pixels for e in export.entries)) == (1740, 1939, 66048)
    assert (min(e.detection_objects for e in export.entries),
            max(e.detection_objects for e in export.entries),
            sum(e.detection_objects for e in export.entries)) == (11, 32, 753)
    assert min(e.maximum_rate_mm_h for e in export.entries) == pytest.approx(
        3.2639997773319482
    )
    assert max(e.maximum_rate_mm_h for e in export.entries) == pytest.approx(
        32.7269977673844
    )
    assert all(
        e.finite_pixels + e.nodata_pixels + e.undetect_pixels + e.mixed_pixels
        + e.outside_support_pixels == 148_750
        for e in export.entries
    )


def test_manifest_substitution_and_relative_paths_fail_closed() -> None:
    _require_export()
    with pytest.raises(DwdScienceReplayError):
        load_dwd_roi_science_replay_export(
            EXPORT_ROOT.resolve(),
            (EXPORT_ROOT / "manifests" / f"{EXPECTED_METRICS_MANIFEST_SHA256}.json").resolve(),
        )
    with pytest.raises(DwdScienceReplayError):
        load_dwd_roi_science_replay_export(
            Path(".analysis/dwd-rv-germany-roi-2km-36-export-v1"),
            SEQUENCE_MANIFEST,
        )


def test_identity_modes_change_only_bounded_multi_scan_memory() -> None:
    detector = dwd_germany_roi_profile().detector_config
    baseline = dwd_roi_science_tracking_policy(
        detector, identity_mode="v4_1_per_transition"
    )
    challenger = dwd_roi_science_tracking_policy(
        detector, identity_mode="v5_multi_scan"
    )
    assert baseline.detector == challenger.detector == detector
    assert baseline.maximum_frame_pixels == challenger.maximum_frame_pixels == 148_750
    assert baseline.motion_nll_weight == challenger.motion_nll_weight == 0.0
    assert baseline.multi_hypothesis_identity is not None
    assert challenger.multi_hypothesis_identity is not None
    assert baseline.multi_hypothesis_identity.multi_scan is None
    assert challenger.multi_hypothesis_identity.multi_scan is not None
    assert baseline == replace(
        challenger,
        policy_id=baseline.policy_id,
        lifecycle=replace(
            challenger.lifecycle,
            policy_id=baseline.lifecycle.policy_id,
        ),
        estimator=replace(
            challenger.estimator,
            policy_id=baseline.estimator.policy_id,
        ),
        weather_system=replace(
            challenger.weather_system,
            policy_id=baseline.weather_system.policy_id,
        ),
        multi_hypothesis_identity=replace(
            challenger.multi_hypothesis_identity,
            policy_id=baseline.multi_hypothesis_identity.policy_id,
            multi_scan=None,
        ),
    )
    assert baseline.fingerprint == (
        "5412fa0f60d9dc37e5d3c3867f5c35e7e12b43e9283479062cdc05039ae08807"
    )
    assert challenger.fingerprint == (
        "560a3eac67cdf9510ebd0b2e72a8c69789f84707ae04a6134a090aa8d0534556"
    )
    assert "independent_speed_truth_unavailable" in DWD_ROI_SCIENCE_BLOCKERS
    assert "weather_system_rebinding_forbidden" in DWD_ROI_SCIENCE_BLOCKERS


@pytest.mark.parametrize("mode", ["v4_1_per_transition", "v5_multi_scan"])
def test_direct_restart_duplicate_and_lineage_are_deterministic(
    tmp_path: Path,
    mode: str,
) -> None:
    _require_export()
    authority = _authority(tmp_path, mode)
    run_ids = authority.export.run_ids
    first_runtime = build_dwd_roi_science_replay_orchestrator(
        authority.authority_document
    )
    first = first_runtime.process(
        attempt_id=f"{mode}-first",
        run_id=run_ids[0],
        deadline_at=time.monotonic() + 60.0,
    )
    restarted = build_dwd_roi_science_replay_orchestrator(
        authority.authority_document
    )
    second = restarted.process(
        attempt_id=f"{mode}-second",
        run_id=run_ids[1],
        deadline_at=time.monotonic() + 60.0,
    )
    duplicate = restarted.process(
        attempt_id=f"{mode}-duplicate",
        run_id=run_ids[0],
        deadline_at=time.monotonic() + 60.0,
    )
    state = recover_dwd_roi_science_replay_state(authority)

    assert first.receipt.outcome.value == second.receipt.outcome.value == "advanced"
    assert first.receipt.accounting.tracker_detection_objects == 11
    assert second.receipt.previous_advanced_receipt_sha256 == first.receipt.receipt_sha256
    assert duplicate.duplicate is True
    assert duplicate.evidence is None and duplicate.transition is None
    assert state.revision == 2
    assert len(state.ledger.tracks) > 0


def test_hard_isolated_spawn_and_durable_duplicate(tmp_path: Path) -> None:
    _require_export()
    authority = _authority(tmp_path, "v4_1_per_transition")
    runner = ScienceShadowIsolatedRunner(
        authority_path=authority.authority_path,
        expected_authority_sha256=authority.authority_sha256,
        factory_contract=authority.factory_contract,
        limits=authority.isolation_limits,
        context=multiprocessing.get_context("spawn"),
    )
    run_id = authority.export.run_ids[0]
    first = runner.execute(attempt_id="roi-spawn-first", canonical_run_id=run_id)
    duplicate = runner.execute(
        attempt_id="roi-spawn-duplicate", canonical_run_id=run_id
    )

    assert first.status == "success"
    assert first.isolation_enforced is True
    assert first.receipt_metadata is not None
    assert first.receipt_metadata["accounting"]["tracker_detection_objects"] == 11
    assert duplicate.status == "success"
    assert duplicate.duplicate is True
    assert duplicate.execution_outcome == "durable_duplicate"
    assert multiprocessing.active_children() == []
