from __future__ import annotations

import hashlib
import multiprocessing
from pathlib import Path
import time

import pytest

from storm_tracker_worker.science_shadow_dwd_replay import (
    DWD_SCIENCE_REPLAY_BLOCKERS,
    DwdScienceReplayError,
    EXPECTED_FRAME_COUNT,
    EXPECTED_METRICS_MANIFEST_SHA256,
    EXPECTED_SEQUENCE_ID,
    EXPECTED_SEQUENCE_MANIFEST_SHA256,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    build_dwd_science_replay_orchestrator,
    create_dwd_science_replay_authority,
    dwd_science_tracking_policy,
    load_dwd_science_replay_export,
    recover_dwd_science_replay_state,
)
from storm_tracker_worker.dwd_rv_observation_replay import (
    default_belgium_observation_profile,
)
from storm_tracker_worker.science_shadow_process_isolation import (
    ScienceShadowIsolatedRunner,
)


REPOSITORY = Path(__file__).resolve().parents[1]
EXPORT_ROOT = (
    REPOSITORY
    / ".analysis"
    / "dwd-rv-observation-sequence-36-cap960-export"
)
SEQUENCE_MANIFEST = (
    EXPORT_ROOT
    / "manifests"
    / f"{EXPECTED_SEQUENCE_MANIFEST_SHA256}.json"
)


def _require_export() -> None:
    if not SEQUENCE_MANIFEST.is_file():
        pytest.skip("definitive immutable DWD replay export is not present")


def _file_inventory(root: Path) -> dict[str, tuple[int, str]]:
    return {
        path.relative_to(root).as_posix(): (
            path.stat().st_size,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _authority(tmp_path: Path, mode: str):
    return create_dwd_science_replay_authority(
        export_root=EXPORT_ROOT.resolve(),
        sequence_manifest_path=SEQUENCE_MANIFEST.resolve(),
        receipt_path=tmp_path / f"{mode}.sqlite3",
        authority_path=tmp_path / f"{mode}.authority.json",
        identity_mode=mode,
    )


def test_definitive_export_is_exact_read_only_no_signal_evidence() -> None:
    _require_export()
    before = _file_inventory(EXPORT_ROOT)
    export = load_dwd_science_replay_export(
        EXPORT_ROOT.resolve(),
        SEQUENCE_MANIFEST.resolve(),
    )
    after = _file_inventory(EXPORT_ROOT)

    assert before == after
    assert export.sequence_id == EXPECTED_SEQUENCE_ID
    assert export.source_manifest_sha256 == EXPECTED_SOURCE_MANIFEST_SHA256
    assert export.metrics_manifest_sha256 == EXPECTED_METRICS_MANIFEST_SHA256
    assert len(export.entries) == EXPECTED_FRAME_COUNT
    assert export.evidence_aggregate_bytes == 18_299_803
    assert min(export.finite_pixels) == 11
    assert max(export.finite_pixels) == 534
    assert sum(export.finite_pixels) == 7_728
    assert {item.detection_objects for item in export.entries} == {0}
    maxima = tuple(
        item.maximum_rate_mm_h
        for item in export.entries
        if item.maximum_rate_mm_h is not None
    )
    assert min(maxima) == pytest.approx(0.08399999426957219)
    assert max(maxima) == pytest.approx(0.2999999795341864)
    assert all(
        item.finite_pixels
        + item.nodata_pixels
        + item.undetect_pixels
        + item.mixed_pixels
        + item.outside_support_pixels
        == 316 * 316
        for item in export.entries
    )


def test_manifest_substitution_and_relative_paths_fail_closed() -> None:
    _require_export()
    with pytest.raises(DwdScienceReplayError):
        load_dwd_science_replay_export(
            EXPORT_ROOT.resolve(),
            (
                EXPORT_ROOT
                / "manifests"
                / f"{EXPECTED_METRICS_MANIFEST_SHA256}.json"
            ).resolve(),
        )
    with pytest.raises(DwdScienceReplayError):
        load_dwd_science_replay_export(
            Path(".analysis/dwd-rv-observation-sequence-36-cap960-export"),
            SEQUENCE_MANIFEST,
        )


@pytest.mark.parametrize("mode", ["v4_1_per_transition", "v5_multi_scan"])
def test_empty_sequence_restart_and_duplicate_are_deterministic(
    tmp_path: Path,
    mode: str,
) -> None:
    _require_export()
    authority = _authority(tmp_path, mode)
    run_ids = authority.export.run_ids
    first = build_dwd_science_replay_orchestrator(authority.authority_document)
    results = [
        first.process(
            attempt_id=f"{mode}-attempt-{index}",
            run_id=run_ids[index],
            deadline_at=time.monotonic() + 60.0,
        )
        for index in range(2)
    ]
    restarted = build_dwd_science_replay_orchestrator(authority.authority_document)
    third = restarted.process(
        attempt_id=f"{mode}-attempt-2",
        run_id=run_ids[2],
        deadline_at=time.monotonic() + 60.0,
    )
    duplicate = restarted.process(
        attempt_id=f"{mode}-duplicate",
        run_id=run_ids[1],
        deadline_at=time.monotonic() + 60.0,
    )
    state = recover_dwd_science_replay_state(authority)

    assert all(item.receipt.outcome.value == "advanced" for item in results)
    assert third.receipt.outcome.value == "advanced"
    assert duplicate.duplicate is True
    assert duplicate.evidence is None
    assert duplicate.transition is None
    assert state.revision == 3
    assert state.ledger.tracks == ()
    assert all(
        item.receipt.accounting.tracker_detection_objects == 0
        for item in (*results, third)
    )
    assert tuple(
        item.receipt.previous_advanced_receipt_sha256
        for item in (*results, third)
    ) == (
        None,
        results[0].receipt.receipt_sha256,
        results[1].receipt.receipt_sha256,
    )


def test_identity_modes_change_only_bounded_identity_memory() -> None:
    detector = default_belgium_observation_profile().detector_config
    baseline = dwd_science_tracking_policy(
        detector,
        identity_mode="v4_1_per_transition",
    )
    challenger = dwd_science_tracking_policy(
        detector,
        identity_mode="v5_multi_scan",
    )
    assert baseline.detector == challenger.detector == detector
    assert baseline.maximum_frame_pixels == challenger.maximum_frame_pixels
    assert baseline.motion_nll_weight == challenger.motion_nll_weight == 0.0
    assert baseline.multi_hypothesis_identity is not None
    assert challenger.multi_hypothesis_identity is not None
    assert baseline.multi_hypothesis_identity.multi_scan is None
    assert challenger.multi_hypothesis_identity.multi_scan is not None
    assert "independent_speed_truth_unavailable" in DWD_SCIENCE_REPLAY_BLOCKERS
    assert "weather_system_rebinding_forbidden" in DWD_SCIENCE_REPLAY_BLOCKERS


def test_hard_isolated_empty_frame_and_durable_duplicate(tmp_path: Path) -> None:
    _require_export()
    authority = _authority(tmp_path, "v5_multi_scan")
    runner = ScienceShadowIsolatedRunner(
        authority_path=authority.authority_path,
        expected_authority_sha256=authority.authority_sha256,
        factory_contract=authority.factory_contract,
        limits=authority.isolation_limits,
        context=multiprocessing.get_context("spawn"),
    )
    run_id = authority.export.run_ids[0]
    result = runner.execute(
        attempt_id="dwd-spawn-first",
        canonical_run_id=run_id,
    )
    duplicate = runner.execute(
        attempt_id="dwd-spawn-duplicate",
        canonical_run_id=run_id,
    )

    assert result.status == "success"
    assert result.isolation_enforced is True
    assert result.durable_disposition == "committed"
    assert result.receipt_metadata is not None
    assert result.receipt_metadata["accounting"]["tracker_detection_objects"] == 0
    assert result.receipt_metadata["next_state_revision"] == 1
    assert duplicate.status == "success"
    assert duplicate.duplicate is True
    assert duplicate.execution_outcome == "durable_duplicate"
    assert multiprocessing.active_children() == []


def test_stale_authority_sha_and_authority_tamper_are_rejected(
    tmp_path: Path,
) -> None:
    _require_export()
    authority = _authority(tmp_path, "v4_1_per_transition")
    stale = ScienceShadowIsolatedRunner(
        authority_path=authority.authority_path,
        expected_authority_sha256="0" * 64,
        factory_contract=authority.factory_contract,
        limits=authority.isolation_limits,
        context=multiprocessing.get_context("spawn"),
    ).execute(
        attempt_id="dwd-stale-authority",
        canonical_run_id=authority.export.run_ids[0],
    )
    assert stale.status == "corrupt_contract"
    assert stale.execution_outcome == "authority_rejected"

    authority.authority_path.write_bytes(authority.authority_path.read_bytes() + b"\n")
    tampered = ScienceShadowIsolatedRunner(
        authority_path=authority.authority_path,
        expected_authority_sha256=authority.authority_sha256,
        factory_contract=authority.factory_contract,
        limits=authority.isolation_limits,
        context=multiprocessing.get_context("spawn"),
    ).execute(
        attempt_id="dwd-tampered-authority",
        canonical_run_id=authority.export.run_ids[0],
    )
    assert tampered.status == "corrupt_contract"
    assert tampered.execution_outcome == "authority_rejected"
    assert multiprocessing.active_children() == []
