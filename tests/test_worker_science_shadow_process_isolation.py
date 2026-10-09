"""Adversarial proofs for the never-live science process boundary."""
from __future__ import annotations

import ast
from dataclasses import dataclass
import hashlib
import json
import multiprocessing
from pathlib import Path
import threading
import time
from typing import Any

import pytest

from storm_tracker_worker import hard_decoder_isolation as hard_isolation
from storm_tracker_worker.hard_decoder_isolation import (
    DecoderProcessResult,
    _canonical_json_bytes,
)
from storm_tracker_worker.science_shadow_orchestrator import (
    ScienceShadowLimits,
    ScienceShadowReceiptStore,
)
from storm_tracker_worker.science_shadow_process_isolation import (
    SCIENCE_SHADOW_PROCESS_AUTHORITY_CONTRACT,
    ScienceShadowFactoryContract,
    ScienceShadowIsolatedRunner,
    ScienceShadowIsolationLimits,
)
from storm_tracker_worker.tracking_core.engine.cell_track_engine import (
    CellTrackLineageHypothesis,
    LineageChildIdentity,
)
from tests.helpers.science_shadow_process_factory import (
    active_track_ids_for_authority,
    detection_ids_for_run,
    runtime_profile_for_authority,
)


ROOT = Path(__file__).parents[1]
FACTORY_TARGET = (
    "tests.helpers.science_shadow_process_factory:build_test_orchestrator"
)


def _run(number: int) -> str:
    return "run-" + format(number, "064x")


@dataclass(frozen=True)
class AuthorityBundle:
    path: Path
    sha256: str
    document: dict[str, Any]
    receipt_path: Path
    factory: ScienceShadowFactoryContract
    isolation_limits: ScienceShadowIsolationLimits
    science_limits: ScienceShadowLimits

    def runner(self) -> ScienceShadowIsolatedRunner:
        return ScienceShadowIsolatedRunner(
            authority_path=self.path,
            expected_authority_sha256=self.sha256,
            factory_contract=self.factory,
            limits=self.isolation_limits,
        )

    def store(self) -> ScienceShadowReceiptStore:
        return ScienceShadowReceiptStore(
            self.receipt_path,
            engine_id=self.document["engine_id"],
            runtime_profile_sha256=self.document["runtime_profile_sha256"],
            limits=self.science_limits,
        )


def _authority(
    tmp_path: Path,
    *,
    mode: str = "normal",
    receipt_path: Path | None = None,
    isolation_changes: dict[str, Any] | None = None,
    config_changes: dict[str, Any] | None = None,
    name: str | None = None,
) -> AuthorityBundle:
    tmp_path.mkdir(parents=True, exist_ok=True)
    isolation_values: dict[str, Any] = {
        "wall_seconds": 6.0,
        "cpu_seconds": 4.0,
        "rss_bytes": 1024 * 1024**2,
        "max_authority_bytes": 256 * 1024,
        "max_request_bytes": 32 * 1024,
        "max_result_bytes": 64 * 1024,
        "max_authorized_paths": 8,
    }
    isolation_values.update(isolation_changes or {})
    isolation_limits = ScienceShadowIsolationLimits(**isolation_values)
    science_limits = ScienceShadowLimits(
        max_runtime_seconds=30.0,
        checkpoint_interval_frames=4,
        max_receipts=64,
    )
    factory = ScienceShadowFactoryContract(
        factory_id="science-process-test-factory-v1",
        target=FACTORY_TARGET,
        opens_only_authorized_paths=True,
        hard_kill_safe=True,
        transactional_receipt_only=True,
        spawns_descendants=False,
    )
    store_path = (receipt_path or (tmp_path / "science-process.sqlite3")).resolve()
    config = {"mode": mode}
    config.update(config_changes or {})
    document: dict[str, Any] = {
        "authorized_paths": [{
            "access": "transactional_file",
            "content_sha256": None,
            "label": "science_receipts",
            "path": str(store_path),
        }],
        "contract": SCIENCE_SHADOW_PROCESS_AUTHORITY_CONTRACT,
        "engine_id": "science-process-test-engine",
        "factory_config": config,
        "factory_contract_sha256": factory.fingerprint,
        "factory_id": factory.factory_id,
        "isolation_limits": isolation_limits.to_mapping(),
        "receipt_store_label": "science_receipts",
        "runtime_profile_sha256": "0" * 64,
        "science_limits": science_limits.to_mapping(),
    }
    # Profile derivation uses the exact production orchestrator and initializes
    # the SQLite file before it is admitted as a transactional_file.
    document["runtime_profile_sha256"] = runtime_profile_for_authority(document)
    authority_name = name or f"authority-{mode}-{len(tuple(tmp_path.glob('authority-*')))}.json"
    authority_path = (tmp_path / authority_name).resolve()
    body = _canonical_json_bytes(document)
    authority_path.write_bytes(body)
    return AuthorityBundle(
        path=authority_path,
        sha256=hashlib.sha256(body).hexdigest(),
        document=document,
        receipt_path=store_path,
        factory=factory,
        isolation_limits=isolation_limits,
        science_limits=science_limits,
    )


class _CapturingExecutor:
    def __init__(self, delegate) -> None:
        self._delegate = delegate
        self.request = None
        self.target = None

    def execute(self, **kwargs):
        self.request = kwargs["request"]
        self.target = kwargs["target"]
        return self._delegate.execute(**kwargs)


def test_happy_path_uses_windows_spawn_bounded_metadata_and_real_tracking(
    tmp_path: Path,
) -> None:
    bundle = _authority(tmp_path)
    runner = bundle.runner()
    capture = _CapturingExecutor(runner._executor)
    runner._executor = capture

    result = runner.execute(attempt_id="attempt-happy", canonical_run_id=_run(1))

    assert result.status == "success", result
    assert result.execution_outcome == "succeeded"
    assert result.committed and not result.duplicate
    assert result.isolation_enforced
    assert result.receipt_metadata["outcome"] == "advanced"
    assert result.receipt_metadata["next_state_revision"] == 1
    assert result.receipt_metadata["process_isolation_certified"] is False
    assert result.meteorological_admission_granted is False
    assert result.operational_snapshot_claim_generated is False
    assert multiprocessing.get_context("spawn").get_start_method() == "spawn"
    assert capture.target.endswith(":_science_shadow_child_entry")
    wire = _canonical_json_bytes(capture.request)
    assert len(wire) <= bundle.isolation_limits.max_request_bytes
    assert b'"values"' not in wire and b'"raster"' not in wire
    assert bundle.store().stats()["head_revision"] == 1

    diagnostic = json.dumps(result.to_mapping(), sort_keys=True)
    assert str(tmp_path) not in diagnostic
    assert "authorization" not in diagnostic.lower()
    assert "token" not in diagnostic.lower()


def test_deadline_during_tracking_is_preemptive_and_leaves_old_head(
    tmp_path: Path,
) -> None:
    bundle = _authority(
        tmp_path,
        mode="tracking_hang",
        isolation_changes={"wall_seconds": 1.2, "cpu_seconds": 4.0},
    )
    started = time.monotonic()
    result = bundle.runner().execute(
        attempt_id="attempt-before-commit",
        canonical_run_id=_run(2),
    )

    assert result.status == "deadline"
    assert result.execution_outcome == "deadline_exceeded"
    assert result.durable_disposition == "not_committed"
    assert result.receipt_metadata is None
    assert result.isolation_enforced
    assert time.monotonic() - started < 4.0
    assert bundle.store().stats()["head_revision"] == 0

    recovered = _authority(
        tmp_path,
        mode="normal",
        receipt_path=bundle.receipt_path,
        name="authority-recovery.json",
    ).runner().execute(
        attempt_id="attempt-before-commit",
        canonical_run_id=_run(2),
    )
    assert recovered.status == "success" and recovered.committed


def test_kill_after_atomic_commit_reconciles_receipt_and_retry_is_exact(
    tmp_path: Path,
) -> None:
    bundle = _authority(
        tmp_path,
        mode="after_commit_hang",
        isolation_changes={"wall_seconds": 1.4, "cpu_seconds": 4.0},
    )
    runner = bundle.runner()
    killed = runner.execute(
        attempt_id="attempt-after-commit",
        canonical_run_id=_run(3),
    )

    assert killed.status == "deadline"
    assert killed.committed
    assert killed.receipt_metadata["next_state_revision"] == 1
    assert bundle.store().stats()["head_revision"] == 1

    exact = runner.execute(
        attempt_id="attempt-after-commit",
        canonical_run_id=_run(3),
    )
    new_attempt_same_run = bundle.runner().execute(
        attempt_id="attempt-after-commit-new-key",
        canonical_run_id=_run(3),
    )
    assert exact.status == new_attempt_same_run.status == "success"
    assert exact.duplicate is new_attempt_same_run.duplicate is True
    assert exact.receipt_metadata == new_attempt_same_run.receipt_metadata
    assert exact.receipt_metadata["receipt_sha256"] == killed.receipt_metadata["receipt_sha256"]


def test_restart_duplicate_and_new_run_advance_deterministically(tmp_path: Path) -> None:
    bundle = _authority(tmp_path)
    first = bundle.runner().execute(
        attempt_id="attempt-restart",
        canonical_run_id=_run(4),
    )
    retry = bundle.runner().execute(
        attempt_id="attempt-restart",
        canonical_run_id=_run(4),
    )
    next_run = bundle.runner().execute(
        attempt_id="attempt-next",
        canonical_run_id=_run(5),
    )

    assert first.committed and not first.duplicate
    assert retry.committed and retry.duplicate
    assert first.receipt_metadata["receipt_sha256"] == retry.receipt_metadata["receipt_sha256"]
    assert next_run.committed and next_run.receipt_metadata["next_state_revision"] == 2
    assert bundle.store().stats()["head_revision"] == 2


def test_lineage_hypothesis_crosses_json_and_survives_durable_restart(
    tmp_path: Path,
) -> None:
    bundle = _authority(tmp_path, config_changes={"two_cells": True})
    first_run = _run(51)
    lineage_run = _run(52)
    first = bundle.runner().execute(
        attempt_id="attempt-lineage-parent",
        canonical_run_id=first_run,
    )
    assert first.status == "success" and first.committed
    parents = active_track_ids_for_authority(bundle.document)
    detections = detection_ids_for_run(bundle.document, lineage_run)
    assert len(parents) == len(detections) == 2
    hypothesis = CellTrackLineageHypothesis(
        hypothesis_id="unselected-process-lineage",
        event_type="split",
        parent_track_ids=(parents[0],),
        children=tuple(
            LineageChildIdentity(
                detection_id=detection_id,
                track_id=f"unselected-child-{index}",
            )
            for index, detection_id in enumerate(detections, start=1)
        ),
        cost=2.0,
        cost_model_id="unselected-process-test-v1",
        cost_components=(("audit_cost", 2.0),),
        provenance_ids=("audit-provenance-a", "audit-provenance-b"),
        selected=False,
    )
    advanced = bundle.runner().execute(
        attempt_id="attempt-lineage-json",
        canonical_run_id=lineage_run,
        lineage_hypotheses=(hypothesis,),
    )
    restarted = bundle.runner().execute(
        attempt_id="attempt-lineage-json",
        canonical_run_id=lineage_run,
        lineage_hypotheses=(hypothesis,),
    )

    assert advanced.status == "success" and advanced.committed
    assert advanced.receipt_metadata["outcome"] == "advanced"
    assert advanced.receipt_metadata["lineage_hypothesis_ids"] == [
        hypothesis.hypothesis_id
    ]
    assert advanced.receipt_metadata["applied_lineage_hypothesis_ids"] == []
    assert restarted.status == "success" and restarted.duplicate
    assert restarted.receipt_metadata == advanced.receipt_metadata


def test_cpu_rss_and_output_limits_have_distinct_resource_terminals(
    tmp_path: Path,
) -> None:
    support = hard_isolation.hard_boundary_support()
    if not support.supported:
        pytest.skip("hard kernel resource boundary unavailable")

    cpu = _authority(
        tmp_path / "cpu",
        mode="tracking_cpu",
        isolation_changes={"wall_seconds": 6.0, "cpu_seconds": 1.0},
    )
    cpu_result = cpu.runner().execute(
        attempt_id="attempt-cpu",
        canonical_run_id=_run(6),
    )
    assert cpu_result.status == "resource"
    assert cpu_result.execution_outcome == "cpu_limit_exceeded"
    assert not cpu_result.committed

    rss = _authority(
        tmp_path / "rss",
        mode="rss",
        isolation_changes={
            "wall_seconds": 8.0,
            "cpu_seconds": 6.0,
                "rss_bytes": 768 * 1024**2,
        },
        config_changes={"chunk_bytes": 8 * 1024**2},
    )
    rss_result = rss.runner().execute(
        attempt_id="attempt-rss",
        canonical_run_id=_run(7),
    )
    assert rss_result.status == "resource"
    assert rss_result.execution_outcome == "memory_limit_exceeded"
    assert not rss_result.committed

    output = _authority(
        tmp_path / "output",
        isolation_changes={"max_result_bytes": 1024},
    )
    output_result = output.runner().execute(
        attempt_id="attempt-output",
        canonical_run_id=_run(8),
    )
    assert output_result.status == "resource"
    assert output_result.execution_outcome == "result_too_large"
    assert output_result.committed
    assert output_result.receipt_metadata["next_state_revision"] == 1


def test_crash_cancel_and_pre_cancel_are_secret_free_and_cleanup_resources(
    tmp_path: Path,
) -> None:
    crash = _authority(tmp_path / "crash", mode="tracking_crash")
    crashed = crash.runner().execute(
        attempt_id="attempt-crash",
        canonical_run_id=_run(9),
    )
    assert crashed.status == "crash"
    assert crashed.execution_outcome == "child_crashed"
    assert crashed.child_exit_code not in {None, 0}
    assert not crashed.committed
    assert crash.store().stats()["head_revision"] == 0

    cancel_bundle = _authority(
        tmp_path / "cancel",
        mode="tracking_hang",
        isolation_changes={"wall_seconds": 8.0},
    )
    runner = cancel_bundle.runner()
    cancel = threading.Event()
    holder = []
    baseline_children = {child.pid for child in multiprocessing.active_children()}
    thread = threading.Thread(
        target=lambda: holder.append(runner.execute(
            attempt_id="attempt-cancel",
            canonical_run_id=_run(10),
            cancel_event=cancel,
        )),
        name="science-process-cancel-test",
    )
    thread.start()
    deadline = time.monotonic() + 4.0
    while not hard_isolation._GLOBAL_JOB_SLOT.locked() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert hard_isolation._GLOBAL_JOB_SLOT.locked()
    cancel.set()
    thread.join(timeout=4.0)
    assert not thread.is_alive()
    assert [item.status for item in holder] == ["cancelled"]
    assert holder[0].durable_disposition == "not_committed"
    assert cancel_bundle.store().stats()["head_revision"] == 0

    pre_cancel = threading.Event()
    pre_cancel.set()
    before = runner.execute(
        attempt_id="attempt-pre-cancel",
        canonical_run_id=_run(11),
        cancel_event=pre_cancel,
    )
    assert before.status == "cancelled"
    assert before.execution_outcome == "cancelled"
    assert before.isolation_enforced is False

    cleanup_deadline = time.monotonic() + 3.0
    while time.monotonic() < cleanup_deadline:
        children = {child.pid for child in multiprocessing.active_children()}
        if children <= baseline_children:
            break
        time.sleep(0.02)
    assert {child.pid for child in multiprocessing.active_children()} <= baseline_children
    assert not any(
        item.name == "science-process-cancel-test" and item.is_alive()
        for item in threading.enumerate()
    )
    diagnostic = repr((crashed, holder[0], before))
    assert str(tmp_path) not in diagnostic


def test_stale_authority_unauthorized_path_and_corrupt_ipc_fail_closed(
    tmp_path: Path,
) -> None:
    stale = _authority(tmp_path / "stale")
    stale_runner = stale.runner()
    stale.path.write_bytes(stale.path.read_bytes() + b"\n")
    stale_result = stale_runner.execute(
        attempt_id="attempt-stale",
        canonical_run_id=_run(12),
    )
    assert stale_result.status == "corrupt_contract"
    assert stale_result.execution_outcome == "authority_rejected"
    assert stale.store().stats()["head_revision"] == 0
    assert str(stale.path) not in repr(stale_result)

    unauthorized = _authority(tmp_path / "unauthorized")
    unauthorized.document["factory_config"]["data_path"] = str(
        (tmp_path / "not-authorized.data").resolve()
    )
    body = _canonical_json_bytes(unauthorized.document)
    unauthorized.path.write_bytes(body)
    unauthorized_runner = ScienceShadowIsolatedRunner(
        authority_path=unauthorized.path,
        expected_authority_sha256=hashlib.sha256(body).hexdigest(),
        factory_contract=unauthorized.factory,
        limits=unauthorized.isolation_limits,
    )
    rejected = unauthorized_runner.execute(
        attempt_id="attempt-unauthorized",
        canonical_run_id=_run(13),
    )
    assert rejected.status == "corrupt_contract"
    assert rejected.isolation_enforced is False
    assert unauthorized.store().stats()["head_revision"] == 0

    corrupt = _authority(tmp_path / "ipc")
    corrupt_runner = corrupt.runner()

    class CorruptExecutor:
        @staticmethod
        def execute(**_kwargs):
            return DecoderProcessResult(
                outcome="succeeded",
                result={"contract": "not-the-contract"},
                child_exit_code=0,
                elapsed_seconds=0.01,
            )

    corrupt_runner._executor = CorruptExecutor()
    corrupt_result = corrupt_runner.execute(
        attempt_id="attempt-corrupt-ipc",
        canonical_run_id=_run(14),
    )
    assert corrupt_result.status == "corrupt_contract"
    assert corrupt_result.execution_outcome == "protocol_error"
    assert corrupt_result.durable_disposition == "not_committed"
    assert corrupt.store().stats()["head_revision"] == 0


def test_concurrent_call_is_busy_not_parallel_and_store_cas_remains_exact(
    tmp_path: Path,
) -> None:
    bundle = _authority(
        tmp_path,
        mode="tracking_hang",
        isolation_changes={"wall_seconds": 8.0},
    )
    runner = bundle.runner()
    cancel = threading.Event()
    holder = []
    thread = threading.Thread(
        target=lambda: holder.append(runner.execute(
            attempt_id="attempt-concurrent-a",
            canonical_run_id=_run(15),
            cancel_event=cancel,
        ))
    )
    thread.start()
    deadline = time.monotonic() + 4.0
    while not hard_isolation._GLOBAL_JOB_SLOT.locked() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert hard_isolation._GLOBAL_JOB_SLOT.locked()

    competing = bundle.runner().execute(
        attempt_id="attempt-concurrent-b",
        canonical_run_id=_run(16),
    )
    cancel.set()
    thread.join(timeout=4.0)

    assert competing.status == "busy"
    assert competing.durable_disposition == "not_committed"
    assert [item.status for item in holder] == ["cancelled"]
    assert bundle.store().stats()["head_revision"] == 0


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_process_candidate_is_not_wired_into_service_home_assistant_or_mqtt() -> None:
    candidate = "storm_tracker_worker.science_shadow_process_isolation"
    assert candidate not in _imports(ROOT / "storm_tracker_worker" / "service.py")
    for path in (ROOT / "custom_components" / "storm_tracker_v3").rglob("*.py"):
        assert candidate not in _imports(path)
