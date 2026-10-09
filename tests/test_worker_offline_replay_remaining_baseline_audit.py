from __future__ import annotations

from dataclasses import replace
import hashlib
from pathlib import Path

import pytest

import storm_tracker_worker.offline_replay_nowcast_runner as nowcast_runner
from storm_tracker_worker.offline_replay_acceptance import BaselineId
from storm_tracker_worker.offline_replay_remaining_baseline_audit import (
    REMAINING_BASELINE_AUDIT_CONTRACT,
    RemainingBaselineAuditError,
    _audit_source_file,
    _parse_object,
    _requirements_declare_pysteps,
    audit_remaining_baseline_executability,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = (
    REPOSITORY_ROOT
    / "tests"
    / "fixtures"
    / "offline_replay_nowcast_baseline_input.json"
)


def test_real_audit_binds_same_bytes_and_keeps_three_baselines_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = FIXTURE.read_bytes()
    counts = {"motion": 0, "eulerian": 0, "translation": 0}
    originals = {
        "motion": nowcast_runner.derive_two_frame_constant_motion,
        "eulerian": nowcast_runner.eulerian_persistence,
        "translation": nowcast_runner.lagrangian_constant_motion,
    }

    def counted_motion(*args: object, **kwargs: object) -> object:
        counts["motion"] += 1
        return originals["motion"](*args, **kwargs)

    def counted_eulerian(*args: object, **kwargs: object) -> object:
        counts["eulerian"] += 1
        return originals["eulerian"](*args, **kwargs)

    def counted_translation(*args: object, **kwargs: object) -> object:
        counts["translation"] += 1
        return originals["translation"](*args, **kwargs)

    monkeypatch.setattr(
        nowcast_runner,
        "derive_two_frame_constant_motion",
        counted_motion,
    )
    monkeypatch.setattr(nowcast_runner, "eulerian_persistence", counted_eulerian)
    monkeypatch.setattr(
        nowcast_runner,
        "lagrangian_constant_motion",
        counted_translation,
    )

    receipt = audit_remaining_baseline_executability(
        body,
        repository_directory=REPOSITORY_ROOT,
        package_directory=REPOSITORY_ROOT / "storm_tracker_worker",
    )

    assert receipt.contract_version == REMAINING_BASELINE_AUDIT_CONTRACT
    assert receipt.replay_pack_sha256 == hashlib.sha256(body).hexdigest()
    assert receipt.validated_primary_repeat_passed is True
    assert [item.value for item in receipt.validated_executed_baselines] == [
        "eulerian_persistence",
        "two_frame_linear_translation",
    ]
    assert counts == {"motion": 38, "eulerian": 38, "translation": 38}
    assert [item.baseline_id.value for item in receipt.decisions] == [
        "last_released_vnext",
        "legacy_engine",
        "pysteps_deterministic_extrapolation",
    ]
    assert receipt.closed_remaining_baselines == ()
    assert receipt.unclosed_remaining_baselines == tuple(
        item.baseline_id for item in receipt.decisions
    )
    assert receipt.remaining_baseline_execution_call_count == 0
    assert all(item.related_source_code_present for item in receipt.decisions)
    assert all(not item.required_executable_path_present for item in receipt.decisions)
    assert all(not item.existing_code_invoked for item in receipt.decisions)
    assert all(item.execution_call_count == 0 for item in receipt.decisions)
    assert all(not item.completed for item in receipt.decisions)
    assert all(item.primary_output_sha256 is None for item in receipt.decisions)
    assert all(item.reproduction_output_sha256 is None for item in receipt.decisions)
    assert receipt.blocking_reasons == tuple(sorted(receipt.blocking_reasons))
    assert {
        "last_released_vnext.release_authority_not_bound_to_replay",
        "legacy_engine.storm_state_sequence_missing",
        "pysteps_deterministic_extrapolation.adapter_not_exposed",
    }.issubset(receipt.blocking_reasons)
    assert receipt.offline_science_gate_passed is False
    assert receipt.meteorological_admission is False
    assert receipt.live_wiring_allowed is False
    assert receipt.dashboard_publication_allowed is False
    assert receipt.home_assistant_state_mutated is False
    assert receipt.to_mapping()["remaining_baseline_audit_sha256"] == receipt.sha256


def test_related_sources_are_content_addressed_and_semantically_identified() -> None:
    receipt = audit_remaining_baseline_executability(
        FIXTURE.read_bytes(),
        repository_directory=REPOSITORY_ROOT,
        package_directory=REPOSITORY_ROOT / "storm_tracker_worker",
    )
    decisions = {item.baseline_id: item for item in receipt.decisions}

    legacy = decisions[BaselineId.LEGACY_ENGINE]
    assert {item.role for item in legacy.source_files} == {
        "legacy_nowcast_entrypoint",
        "legacy_storm_engine",
        "legacy_storm_model",
        "legacy_trajectory_support",
    }
    assert any(
        item.required_symbols == ("build_precipitation_status",)
        for item in legacy.source_files
    )

    released = decisions[BaselineId.LAST_RELEASED_VNEXT]
    assert released.runtime_facts[0].value == "current_tree_only"
    assert "last_released_vnext_release_authority" in released.missing_capability_ids

    pysteps = decisions[BaselineId.PYSTEPS_DETERMINISTIC]
    facts = {item.fact_id: item.value for item in pysteps.runtime_facts}
    assert facts["worker_shadow_adapter_algorithm_ids"] == (
        "pysteps_linda,pysteps_steps"
    )
    assert "pysteps_deterministic_extrapolation" not in facts[
        "worker_shadow_adapter_algorithm_ids"
    ]
    assert "pysteps_dense_motion_contract" in pysteps.missing_capability_ids


def test_requirements_detection_is_exact_and_comment_safe() -> None:
    assert _requirements_declare_pysteps(b"numpy==2.5.1\n") is False
    assert _requirements_declare_pysteps(b"# pysteps==1.21.1\nnumpy\n") is False
    assert _requirements_declare_pysteps(b"pySTEPS[extra]==1.21.1\n") is True
    assert _requirements_declare_pysteps(b"py_steps==1.21.1\n") is False


def test_source_audit_detects_missing_required_symbol(tmp_path: Path) -> None:
    source = tmp_path / "worker.py"
    source.write_text("def present():\n    return 1\n", encoding="utf-8")

    with pytest.raises(
        RemainingBaselineAuditError,
        match="lacks required existing symbol",
    ):
        _audit_source_file(
            tmp_path,
            "worker.py",
            "test_role",
            ("missing",),
        )


def test_source_audit_hash_changes_with_exact_source_bytes(tmp_path: Path) -> None:
    source = tmp_path / "worker.py"
    source.write_text("def run():\n    return 1\n", encoding="utf-8")
    first = _audit_source_file(tmp_path, "worker.py", "test_role", ("run",))
    source.write_text("def run():\n    return 2\n", encoding="utf-8")
    second = _audit_source_file(tmp_path, "worker.py", "test_role", ("run",))

    assert first.sha256 != second.sha256
    assert first.size_bytes == second.size_bytes


def test_receipt_and_decision_reject_false_execution_claims() -> None:
    receipt = audit_remaining_baseline_executability(
        FIXTURE.read_bytes(),
        repository_directory=REPOSITORY_ROOT,
        package_directory=REPOSITORY_ROOT / "storm_tracker_worker",
    )
    decision = receipt.decisions[0]

    with pytest.raises(ValueError, match="may not claim remaining-baseline execution"):
        replace(
            decision,
            existing_code_invoked=True,
            execution_call_count=1,
        )
    with pytest.raises(ValueError, match="cannot close an unexecuted baseline"):
        replace(
            receipt,
            closed_remaining_baselines=(BaselineId.LAST_RELEASED_VNEXT,),
        )
    with pytest.raises(ValueError, match="never grant live/science authority"):
        replace(receipt, meteorological_admission=True)


def test_duplicate_replay_key_is_rejected_before_any_claim() -> None:
    with pytest.raises(ValueError, match="duplicate JSON key"):
        _parse_object(b'{"contract_version":"a","contract_version":"b"}', "pack")


def test_high_level_audit_rejects_a_different_checkout_or_worker_tree(
    tmp_path: Path,
) -> None:
    body = FIXTURE.read_bytes()
    with pytest.raises(
        RemainingBaselineAuditError,
        match="differs from the imported auditor checkout",
    ):
        audit_remaining_baseline_executability(
            body,
            repository_directory=tmp_path,
        )
    with pytest.raises(
        RemainingBaselineAuditError,
        match="worker package differs",
    ):
        audit_remaining_baseline_executability(
            body,
            repository_directory=REPOSITORY_ROOT,
            package_directory=tmp_path,
        )
