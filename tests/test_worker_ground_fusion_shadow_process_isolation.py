"""Adversarial proofs for the never-live ground process boundary."""
from __future__ import annotations

import ast
from dataclasses import dataclass, replace
import hashlib
import json
import multiprocessing
from pathlib import Path
import threading
import time
from typing import Any

import pytest

from storm_tracker_worker import hard_decoder_isolation as hard_isolation
from storm_tracker_worker import ground_fusion_shadow_process_isolation as isolation_module
from storm_tracker_worker.ground_fusion_shadow_process_isolation import (
    GROUND_SHADOW_PROCESS_AUTHORITY_CONTRACT,
    GROUND_SHADOW_PROCESS_CHILD_CONTRACT,
    GroundShadowFactoryContract,
    GroundShadowIsolatedRunner,
    GroundShadowIsolationLimits,
    canonical_ground_store_identity_sha256,
    ground_shadow_request_mapping,
)
from storm_tracker_worker.ground_fusion_shadow_runtime import (
    GeographicFootprintProof,
    GroundCellTrackBinding,
    GroundFusionShadowLimits,
    GroundFusionShadowReceiptStore,
    GroundFusionShadowRequest,
    GroundFusionShadowRetentionPolicy,
    GroundTargetSnapshot,
    footprint_geometry_sha256,
)
from storm_tracker_worker.hard_decoder_isolation import (
    DecoderProcessResult,
    _canonical_json_bytes,
)
from storm_tracker_worker.tracking_core.engine.ground_fusion import (
    CellTrackFootprint,
)
from tests.helpers.ground_shadow_process_factory import (
    initialize_canonical_ground,
    runtime_profile_for_authority,
)


ROOT = Path(__file__).parents[1]
FACTORY_TARGET = (
    "tests.helpers.ground_shadow_process_factory:build_test_runtime"
)
NOW = 1_800_000_000.0
PRIVATE_STATION = "PROCESS-STATION-MUST-NOT-LEAK"


@dataclass(frozen=True)
class AuthorityBundle:
    path: Path
    sha256: str
    document: dict[str, Any]
    canonical_path: Path
    receipt_path: Path
    archive_path: Path
    factory: GroundShadowFactoryContract
    isolation_limits: GroundShadowIsolationLimits
    ground_limits: GroundFusionShadowLimits
    retention: GroundFusionShadowRetentionPolicy

    def runner(self) -> GroundShadowIsolatedRunner:
        return GroundShadowIsolatedRunner(
            authority_path=self.path,
            expected_authority_sha256=self.sha256,
            factory_contract=self.factory,
            limits=self.isolation_limits,
        )

    def store(self) -> GroundFusionShadowReceiptStore:
        store = GroundFusionShadowReceiptStore(
            self.receipt_path,
            runtime_profile_sha256=self.document["runtime_profile_sha256"],
            limits=self.ground_limits,
        )
        store.initialize()
        return store


def _authority(
    tmp_path: Path,
    *,
    mode: str = "normal",
    receipt_path: Path | None = None,
    canonical_path: Path | None = None,
    archive_path: Path | None = None,
    isolation_changes: dict[str, Any] | None = None,
    config_changes: dict[str, Any] | None = None,
    name: str | None = None,
) -> AuthorityBundle:
    tmp_path.mkdir(parents=True, exist_ok=True)
    isolation_values: dict[str, Any] = {
        "wall_seconds": 7.0,
        "cpu_seconds": 5.0,
        "rss_bytes": 1024 * 1024**2,
        "max_authority_bytes": 256 * 1024,
        "max_request_bytes": 256 * 1024,
        "max_result_bytes": 64 * 1024,
        "max_authorized_paths": 8,
    }
    isolation_values.update(isolation_changes or {})
    isolation = GroundShadowIsolationLimits(**isolation_values)
    ground_limits = GroundFusionShadowLimits(
        max_receipts=64,
        max_runtime_seconds=30.0,
    )
    retention = GroundFusionShadowRetentionPolicy(
        archive_namespace="ground-process-test-archive",
        hot_receipts_to_keep=1,
        max_segment_receipts=8,
        max_segment_bytes=4 * 1024 * 1024,
        max_archive_segments=32,
        max_archived_receipts=64,
        max_total_archive_bytes=32 * 1024 * 1024,
    )
    factory = GroundShadowFactoryContract(
        factory_id="ground-process-test-factory-v1",
        target=FACTORY_TARGET,
        opens_only_authorized_paths=True,
        canonical_raw_reverification=True,
        hard_kill_safe=True,
        transactional_receipt_only=True,
        spawns_descendants=False,
    )
    canonical = (canonical_path or (tmp_path / "canonical-ground.sqlite3")).resolve()
    receipt = (receipt_path or (tmp_path / "ground-process.sqlite3")).resolve()
    archive = (archive_path or (tmp_path / "ground-archive")).resolve()
    archive.mkdir(parents=True, exist_ok=True)
    initialize_canonical_ground(canonical)
    config = {"mode": mode}
    config.update(config_changes or {})
    document: dict[str, Any] = {
        "archive_directory_label": "ground_archive",
        "authorized_paths": [
            {
                "access": "canonical_ground_database",
                "identity_sha256": canonical_ground_store_identity_sha256(canonical),
                "label": "canonical_ground",
                "path": str(canonical),
            },
            {
                "access": "ground_receipt_database",
                "identity_sha256": None,
                "label": "ground_receipts",
                "path": str(receipt),
            },
            {
                "access": "ground_archive_directory",
                "identity_sha256": None,
                "label": "ground_archive",
                "path": str(archive),
            },
        ],
        "canonical_ground_label": "canonical_ground",
        "contract": GROUND_SHADOW_PROCESS_AUTHORITY_CONTRACT,
        "factory_config": config,
        "factory_contract_sha256": factory.fingerprint,
        "factory_id": factory.factory_id,
        "ground_limits": ground_limits.to_mapping(),
        "isolation_limits": isolation.to_mapping(),
        "receipt_store_label": "ground_receipts",
        "retention_policy": retention.to_mapping(),
        "runtime_profile_sha256": "0" * 64,
    }
    document["runtime_profile_sha256"] = runtime_profile_for_authority(document)
    authority_name = name or f"authority-{mode}.json"
    authority = (tmp_path / authority_name).resolve()
    body = _canonical_json_bytes(document)
    authority.write_bytes(body)
    return AuthorityBundle(
        path=authority,
        sha256=hashlib.sha256(body).hexdigest(),
        document=document,
        canonical_path=canonical,
        receipt_path=receipt,
        archive_path=archive,
        factory=factory,
        isolation_limits=isolation,
        ground_limits=ground_limits,
        retention=retention,
    )


def _request(*, attempt: str = "ground-process-attempt-1", ordinal: int = 1):
    footprint = CellTrackFootprint(
        footprint_id=f"ground-process-footprint-{ordinal}",
        cell_track_id=f"ground-process-track-{ordinal}",
        cell_detection_id=f"ground-process-detection-{ordinal}",
        valid_from=NOW - 60.0,
        valid_until=NOW + 60.0,
        representative_at=NOW,
        rings=((
            (40.70, -74.05),
            (40.80, -74.05),
            (40.80, -73.90),
            (40.70, -73.90),
        ),),
        radar_source="certified-ground-process-radar",
        raw_artifact_receipt_id=f"ground-process-radar-receipt-{ordinal}",
    )
    proof = GeographicFootprintProof(
        proof_id=f"ground-process-proof-{ordinal}",
        proof_version="1",
        source_science_receipt_sha256="1" * 64,
        source_target_grid_sha256="2" * 64,
        source_transform_receipt_sha256="3" * 64,
        geometry_builder_id="certified-grid-cell-contour",
        geometry_certificate_sha256="4" * 64,
        footprint_geometry_sha256=footprint_geometry_sha256(footprint),
    )
    binding = GroundCellTrackBinding(
        footprint=footprint,
        engine_id="ground-process-engine",
        engine_state_revision=ordinal,
        track_revision=ordinal,
        analysis_grid_id="ground-process-grid",
        science_run_id=f"ground-process-science-run-{ordinal}",
        science_transition_id=f"ground-process-transition-{ordinal}",
        science_receipt_sha256="1" * 64,
        source_frame_id=f"ground-process-frame-{ordinal}",
        target_grid_sha256="2" * 64,
        radar_semantics_certificate_sha256="5" * 64,
        transform_certificate_sha256="6" * 64,
        parent_track_ids=(),
        child_track_ids=(),
        lineage_event_ids=(f"ground-process-birth-{ordinal}",),
        applied_lineage_hypothesis_ids=(),
        footprint_proof=proof,
    )
    return GroundFusionShadowRequest(
        attempt_id=attempt,
        decision_key=f"private-target-ground-track-{ordinal}",
        region_key="private-ground-process-region",
        target=GroundTargetSnapshot(
            target_id="device_tracker.private_ground_process_person",
            target_revision=f"private-revision-{ordinal}",
            latitude=40.75,
            longitude=-73.98,
            observed_at=NOW - 10.0,
            source_state_sha256="7" * 64,
        ),
        cell=binding,
        evaluated_at=NOW,
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


def test_real_spawn_success_uses_bounded_command_and_secret_free_metadata(
    tmp_path: Path,
) -> None:
    bundle = _authority(tmp_path)
    runner = bundle.runner()
    capture = _CapturingExecutor(runner._executor)
    runner._executor = capture
    request = _request()

    result = runner.execute(request)

    assert result.status == "success", result
    assert result.execution_outcome == "succeeded"
    assert result.committed and result.duplicate is False
    assert result.isolation_enforced
    assert result.receipt_metadata["accounting_source_fact_count"] == 0
    assert result.receipt_metadata["meteorological_admission_granted"] is False
    assert result.operational_confirmation_claim is False
    assert multiprocessing.get_context("spawn").get_start_method() == "spawn"
    assert capture.target.endswith(":_ground_shadow_child_entry")
    wire = _canonical_json_bytes(capture.request)
    assert len(wire) <= bundle.isolation_limits.max_request_bytes
    assert b'"ground_request"' in wire
    assert b'"ground_facts"' not in wire and b'"observations"' not in wire
    assert ground_shadow_request_mapping(request) == capture.request["ground_request"]

    diagnostics = json.dumps(result.to_mapping(), sort_keys=True)
    assert str(tmp_path) not in diagnostics
    assert PRIVATE_STATION not in diagnostics
    assert "device_tracker.private_ground_process_person" not in diagnostics
    assert "authorization" not in diagnostics.casefold()
    assert "token" not in diagnostics.casefold()


def test_parent_deadline_before_commit_is_preemptive_and_recoverable(
    tmp_path: Path,
) -> None:
    bundle = _authority(
        tmp_path,
        mode="hang",
        isolation_changes={"wall_seconds": 1.4, "cpu_seconds": 4.0},
    )
    started = time.monotonic()
    result = bundle.runner().execute(_request(attempt="ground-deadline"))

    assert result.status == "deadline"
    assert result.execution_outcome == "deadline_exceeded"
    assert result.durable_disposition == "not_committed"
    assert result.receipt_metadata is None
    assert result.isolation_enforced
    assert time.monotonic() - started < 5.0
    assert bundle.store().diagnostics()["receipt_count"] == 0

    recovered = _authority(
        tmp_path,
        mode="normal",
        receipt_path=bundle.receipt_path,
        canonical_path=bundle.canonical_path,
        archive_path=bundle.archive_path,
        name="authority-recovery.json",
    ).runner().execute(_request(attempt="ground-deadline"))
    assert recovered.status == "success" and recovered.committed


def test_kill_after_commit_reconciles_exact_once_and_restart_is_duplicate(
    tmp_path: Path,
) -> None:
    bundle = _authority(
        tmp_path,
        mode="after_commit_hang",
        isolation_changes={"wall_seconds": 1.6, "cpu_seconds": 4.0},
    )
    request = _request(attempt="ground-after-commit")
    killed = bundle.runner().execute(request)

    assert killed.status == "deadline"
    assert killed.committed
    assert killed.receipt_metadata["request_sha256"]
    assert bundle.store().diagnostics()["receipt_count"] == 1

    exact = bundle.runner().execute(request)
    new_attempt = bundle.runner().execute(
        replace(request, attempt_id="ground-after-commit-new-attempt")
    )
    assert exact.status == new_attempt.status == "success"
    assert exact.duplicate is new_attempt.duplicate is True
    assert exact.receipt_metadata == new_attempt.receipt_metadata
    assert exact.receipt_metadata == killed.receipt_metadata


def test_restart_concurrent_retry_and_archived_lookup_are_exact(
    tmp_path: Path,
) -> None:
    bundle = _authority(tmp_path)
    first_request = _request(attempt="ground-archive-first", ordinal=1)
    second_request = _request(attempt="ground-archive-second", ordinal=2)
    first = bundle.runner().execute(first_request)
    second = bundle.runner().execute(second_request)
    assert first.committed and second.committed

    results = []
    threads = [
        threading.Thread(target=lambda: results.append(bundle.runner().execute(first_request)))
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)
    assert len(results) == 2
    assert all(item.status == "success" and item.duplicate for item in results)
    assert len({item.receipt_metadata["receipt_sha256"] for item in results}) == 1

    rotation = bundle.store().rotate_archive(
        bundle.archive_path,
        policy=bundle.retention,
        deadline_at=time.monotonic() + 5.0,
        directory_fsync=lambda _path: None,
    )
    assert rotation.archived_receipts == 1
    archived = bundle.runner().execute(first_request)
    assert archived.status == "success"
    assert archived.execution_outcome == "durable_duplicate"
    assert archived.duplicate is True
    assert archived.receipt_metadata == first.receipt_metadata


def test_cpu_rss_and_output_overflow_are_distinct_resources(tmp_path: Path) -> None:
    support = hard_isolation.hard_boundary_support()
    if not support.supported:
        pytest.skip("hard kernel resource boundary unavailable")

    cpu = _authority(
        tmp_path / "cpu",
        mode="cpu",
        isolation_changes={"wall_seconds": 7.0, "cpu_seconds": 1.0},
    )
    cpu_result = cpu.runner().execute(_request(attempt="ground-cpu"))
    assert cpu_result.status == "resource"
    assert cpu_result.execution_outcome == "cpu_limit_exceeded"
    assert not cpu_result.committed

    rss = _authority(
        tmp_path / "rss",
        mode="rss",
        isolation_changes={
            "wall_seconds": 9.0,
            "cpu_seconds": 7.0,
            "rss_bytes": 256 * 1024**2,
        },
        config_changes={"chunk_bytes": 16 * 1024**2},
    )
    rss_result = rss.runner().execute(_request(attempt="ground-rss"))
    assert rss_result.status == "resource"
    assert rss_result.execution_outcome == "memory_limit_exceeded"
    assert not rss_result.committed

    output = _authority(
        tmp_path / "output",
        mode="output",
        isolation_changes={"max_result_bytes": 1024},
    )
    output_result = output.runner().execute(_request(attempt="ground-output"))
    assert output_result.status == "resource"
    assert output_result.execution_outcome == "result_too_large"
    assert output_result.committed


def test_crash_cancel_and_pre_cancel_terminate_child_and_leave_no_receipt(
    tmp_path: Path,
) -> None:
    crash = _authority(tmp_path / "crash", mode="crash")
    crashed = crash.runner().execute(_request(attempt="ground-crash"))
    assert crashed.status == "crash"
    assert crashed.execution_outcome == "child_crashed"
    assert crashed.child_exit_code not in {None, 0}
    assert not crashed.committed

    cancel_bundle = _authority(
        tmp_path / "cancel",
        mode="hang",
        isolation_changes={"wall_seconds": 8.0},
    )
    runner = cancel_bundle.runner()
    cancel = threading.Event()
    holder = []
    baseline = {child.pid for child in multiprocessing.active_children()}
    thread = threading.Thread(
        target=lambda: holder.append(
            runner.execute(_request(attempt="ground-cancel"), cancel_event=cancel)
        ),
        name="ground-process-cancel-test",
    )
    thread.start()
    deadline = time.monotonic() + 4.0
    while not hard_isolation._GLOBAL_JOB_SLOT.locked() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert hard_isolation._GLOBAL_JOB_SLOT.locked()
    competing_retry = cancel_bundle.runner().execute(
        _request(attempt="ground-cancel")
    )
    assert competing_retry.status == "busy"
    assert competing_retry.durable_disposition == "not_committed"
    cancel.set()
    thread.join(timeout=5.0)
    assert not thread.is_alive()
    assert [item.status for item in holder] == ["cancelled"]
    assert holder[0].durable_disposition == "not_committed"

    pre_cancel = threading.Event()
    pre_cancel.set()
    before = runner.execute(
        _request(attempt="ground-pre-cancel"),
        cancel_event=pre_cancel,
    )
    assert before.status == "cancelled"
    assert before.isolation_enforced is False

    cleanup_deadline = time.monotonic() + 3.0
    while time.monotonic() < cleanup_deadline:
        if {child.pid for child in multiprocessing.active_children()} <= baseline:
            break
        time.sleep(0.02)
    assert {child.pid for child in multiprocessing.active_children()} <= baseline
    assert str(tmp_path) not in repr((crashed, holder[0], before))


def test_authority_path_identity_tamper_and_corrupt_ipc_fail_closed(
    tmp_path: Path,
) -> None:
    stale = _authority(tmp_path / "stale")
    stale.path.write_bytes(stale.path.read_bytes() + b"\n")
    rejected = stale.runner().execute(_request(attempt="ground-stale"))
    assert rejected.status == "corrupt_contract"
    assert rejected.execution_outcome == "authority_rejected"
    assert stale.store().diagnostics()["receipt_count"] == 0
    assert str(stale.path) not in repr(rejected)

    unauthorized = _authority(tmp_path / "unauthorized")
    unauthorized.document["factory_config"]["data_path"] = str(
        (tmp_path / "not-authorized.data").resolve()
    )
    body = _canonical_json_bytes(unauthorized.document)
    unauthorized.path.write_bytes(body)
    unauthorized_runner = GroundShadowIsolatedRunner(
        authority_path=unauthorized.path,
        expected_authority_sha256=hashlib.sha256(body).hexdigest(),
        factory_contract=unauthorized.factory,
        limits=unauthorized.isolation_limits,
    )
    path_rejected = unauthorized_runner.execute(
        _request(attempt="ground-unauthorized")
    )
    assert path_rejected.status == "corrupt_contract"
    assert path_rejected.isolation_enforced is False

    relative = _authority(tmp_path / "relative")
    relative.document["factory_config"]["data_path"] = "../unowned.data"
    body = _canonical_json_bytes(relative.document)
    relative.path.write_bytes(body)
    relative_runner = GroundShadowIsolatedRunner(
        authority_path=relative.path,
        expected_authority_sha256=hashlib.sha256(body).hexdigest(),
        factory_contract=relative.factory,
        limits=relative.isolation_limits,
    )
    relative_rejected = relative_runner.execute(
        _request(attempt="ground-relative-path")
    )
    assert relative_rejected.status == "corrupt_contract"

    identity = _authority(tmp_path / "identity")
    identity.document["authorized_paths"][0]["identity_sha256"] = "f" * 64
    body = _canonical_json_bytes(identity.document)
    identity.path.write_bytes(body)
    identity_runner = GroundShadowIsolatedRunner(
        authority_path=identity.path,
        expected_authority_sha256=hashlib.sha256(body).hexdigest(),
        factory_contract=identity.factory,
        limits=identity.isolation_limits,
    )
    identity_rejected = identity_runner.execute(
        _request(attempt="ground-identity-tamper")
    )
    assert identity_rejected.status == "corrupt_contract"

    profile = _authority(tmp_path / "profile")
    profile.document["runtime_profile_sha256"] = "e" * 64
    body = _canonical_json_bytes(profile.document)
    profile.path.write_bytes(body)
    profile_runner = GroundShadowIsolatedRunner(
        authority_path=profile.path,
        expected_authority_sha256=hashlib.sha256(body).hexdigest(),
        factory_contract=profile.factory,
        limits=profile.isolation_limits,
    )
    profile_rejected = profile_runner.execute(
        _request(attempt="ground-runtime-profile-tamper")
    )
    assert profile_rejected.status == "corrupt_contract"
    assert profile_rejected.execution_outcome == "durable_store_rejected"

    corrupt = _authority(tmp_path / "ipc")
    runner = corrupt.runner()

    class CorruptExecutor:
        @staticmethod
        def execute(**_kwargs):
            return DecoderProcessResult(
                outcome="succeeded",
                result={
                    "authority_sha256": "f" * 64,
                    "code": "ground_child_failed",
                    "contract": GROUND_SHADOW_PROCESS_CHILD_CONTRACT,
                    "status": "crash",
                },
                child_exit_code=0,
                elapsed_seconds=0.01,
            )

    runner._executor = CorruptExecutor()
    corrupt_result = runner.execute(_request(attempt="ground-corrupt-ipc"))
    assert corrupt_result.status == "corrupt_contract"
    assert corrupt_result.execution_outcome == "protocol_error"
    assert corrupt_result.durable_disposition == "not_committed"


def test_command_checksum_tamper_is_explicit_corrupt_contract(tmp_path: Path) -> None:
    bundle = _authority(tmp_path)
    runner = bundle.runner()

    class CaptureCancelled:
        request = None

        def execute(self, **kwargs):
            self.request = kwargs["request"]
            return DecoderProcessResult(
                outcome="cancelled",
                result=None,
                detail_code="cancelled_before_spawn",
                child_exit_code=None,
                elapsed_seconds=0.0,
            )

    capture = CaptureCancelled()
    runner._executor = capture
    cancelled = runner.execute(_request(attempt="ground-command-capture"))
    assert cancelled.status == "cancelled"
    command = json.loads(json.dumps(capture.request))
    command["ground_request"]["target"]["latitude"] = 41.0

    envelope = isolation_module._ground_shadow_child_entry(command)

    assert envelope["status"] == "corrupt_contract"
    assert envelope["code"] == "authority_or_request_invalid"
    assert set(envelope) == {"authority_sha256", "code", "contract", "status"}


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_ground_process_candidate_has_no_pickle_or_live_wiring() -> None:
    module_path = (
        ROOT / "storm_tracker_worker" / "ground_fusion_shadow_process_isolation.py"
    )
    source = module_path.read_text(encoding="utf-8")
    assert "pickle" not in source
    candidate = "storm_tracker_worker.ground_fusion_shadow_process_isolation"
    assert candidate not in _imports(ROOT / "storm_tracker_worker" / "service.py")
    for path in (ROOT / "custom_components" / "storm_tracker_v3").rglob("*.py"):
        assert candidate not in _imports(path)
    assert not any(
        name.startswith("homeassistant")
        or name.startswith("custom_components")
        or name == "storm_tracker_worker.service"
        for name in _imports(module_path)
    )
