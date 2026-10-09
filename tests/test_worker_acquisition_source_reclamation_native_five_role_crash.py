"""Real-process crash matrix for native five-role source reclamation.

The fixture, crashing apply and recovery each run from a fresh set of store
objects.  The injected failure is an actual ``os._exit(73)`` rather than an
exception that gives Python a chance to unwind transactions or file handles.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
from typing import Iterator

import pytest

from storm_tracker_worker import autonomous_scheduler, desired_state
from storm_tracker_worker.acquisition_consumer_registry import (
    AcquisitionConsumerRegistryAuthority,
)
from storm_tracker_worker.acquisition_control_reclamation import (
    CapacityLedgerReclamationParticipantV2,
    ExecutionReceiptReclamationParticipantV2,
    SchedulerReclamationParticipantV2,
)
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
    CompletionWindowExpired,
)
from storm_tracker_worker.acquisition_execution_store import ExecutionReceiptStore
from storm_tracker_worker.acquisition_source_reclamation_apply_authority import (
    AcquisitionSourceReclamationApplyCoordinator,
)
from storm_tracker_worker.acquisition_source_reclamation_protocol import (
    APPLIED,
    COMMITTED,
    PARTICIPANT_APPLY_ORDER,
    SourceReclamationParticipantV2,
    SourceReclamationRequestV2,
    canonical_bytes,
    digest_mapping,
)
from storm_tracker_worker.acquisition_source_retention import (
    AcquisitionSourceCandidate,
    AcquisitionSourceRetentionArchive,
    AcquisitionSourceRetentionArchiveV2,
    AcquisitionSourceRetentionLimits,
    AcquisitionSourceRetentionLimitsV2,
)
from storm_tracker_worker.raw_artifacts import (
    RAW_RECLAMATION_APPLIED,
    RAW_RECLAMATION_FILES_PENDING,
    RAW_RECLAMATION_UNLINKED_DURABLE,
    RawArtifactLimits,
    RawArtifactStore,
)
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityProfile,
)

from test_worker_acquisition_control_reclamation import _Clock
from test_worker_acquisition_source_reclamation_native_five_role_integration import (
    _native_five_role_stack,
)
from test_worker_acquisition_source_retention import NOW, SYNC, Authorities


ROOT = Path(__file__).resolve().parents[1]
CHILD = ROOT / "tests" / "helpers" / "worker_reclamation_crash_child.py"
MANIFEST_NAME = "native-five-role-crash-manifest.json"
MANIFEST_CONTRACT = "stv3-test-native-five-role-crash-manifest-v1"
CRASH_EXIT_CODE = 73
_VOLATILE_LOCK_PATHS = frozenset({
    "closure-source/source-archive/rotation.lock",
    "fixture/sources/source-archive/rotation.lock",
    "registry-authority/registry/registry.sqlite3.owner.lock",
    "retention-v2/retention-v2.lock",
})

_RAW_LIMITS = RawArtifactLimits(
    max_object_bytes=2 * 1024 * 1024,
    max_total_bytes=8 * 1024 * 1024,
    max_object_entries=32,
    max_staging_entries=8,
    max_receipt_entries=32,
    max_capture_entries=32,
    max_capture_members=4,
    max_catalog_bytes=4 * 1024 * 1024,
)
_CAPACITY_PROFILE = RawCapacityProfile(
    global_max_bytes=8 * 1024 * 1024,
    global_max_entries=32,
    providers=(ProviderCapacityLimit("dwd", 8 * 1024 * 1024, 32),),
    max_reservations=32,
    max_catalog_bytes=4 * 1024 * 1024,
    sqlite_busy_timeout_seconds=2.0,
)


# These are the existing COMMITTED -> APPLIED mutation boundaries.  Pure
# reproof callbacks are deliberately outside this first bounded matrix.
CRASH_CASES = (
    ("downstream_lifecycle", "reclamation.apply.before_file_unlink", 1),
    ("downstream_lifecycle", "reclamation.apply.after_file_unlink", 1),
    ("downstream_lifecycle", "reclamation.apply.before_file_unlink", 2),
    ("downstream_lifecycle", "reclamation.apply.after_file_unlink", 2),
    ("downstream_lifecycle", "reclamation.apply.before_logical_commit", 1),
    ("downstream_lifecycle", "reclamation.apply.after_logical_commit", 1),
    ("downstream_lifecycle", "reclamation.apply.before_applied_commit", 1),
    ("downstream_lifecycle", "reclamation.apply.after_applied_commit", 1),
    ("execution_receipts", "execution_receipts.apply.after_applying", 1),
    ("execution_receipts", "execution_receipts.apply.before_commit", 1),
    ("execution_receipts", "execution_receipts.apply.after_commit", 1),
    ("scheduler", "scheduler.apply.after_applying", 1),
    ("scheduler", "scheduler.apply.before_commit", 1),
    ("scheduler", "scheduler.apply.after_commit", 1),
    ("raw_artifacts", "reclamation_catalog_before_commit", 1),
    ("raw_artifacts", "reclamation_catalog_after_commit", 1),
    ("raw_artifacts", "reclamation_unlink_after_preunlink_reproof", 1),
    ("raw_artifacts", "reclamation_unlink_after_unlink", 1),
    ("raw_artifacts", "reclamation_unlink_event_after_commit", 1),
    ("raw_artifacts", "reclamation_unlinked_before_commit", 1),
    ("raw_artifacts", "reclamation_unlinked_after_commit", 1),
    ("raw_artifacts", "reclamation_apply_before_commit", 1),
    ("raw_artifacts", "reclamation_apply_after_commit", 1),
    ("capacity_ledger", "capacity_ledger.apply.after_applying", 1),
    ("capacity_ledger", "capacity_ledger.apply.before_commit", 1),
    ("capacity_ledger", "capacity_ledger.apply.after_commit", 1),
    *(
        ("coordinator", checkpoint, occurrence)
        for occurrence in range(1, len(PARTICIPANT_APPLY_ORDER) + 1)
        for checkpoint in (
            "record_apply.before_commit",
            "record_apply.after_commit",
        )
    ),
    ("coordinator", "finish_applied.before_commit", 1),
    ("coordinator", "finish_applied.after_commit", 1),
)


PINNED_EXPECTED_CRASH_CASE_KEYS = frozenset({
    "downstream_lifecycle|reclamation.apply.before_file_unlink|1",
    "downstream_lifecycle|reclamation.apply.after_file_unlink|1",
    "downstream_lifecycle|reclamation.apply.before_file_unlink|2",
    "downstream_lifecycle|reclamation.apply.after_file_unlink|2",
    "downstream_lifecycle|reclamation.apply.before_logical_commit|1",
    "downstream_lifecycle|reclamation.apply.after_logical_commit|1",
    "downstream_lifecycle|reclamation.apply.before_applied_commit|1",
    "downstream_lifecycle|reclamation.apply.after_applied_commit|1",
    "execution_receipts|execution_receipts.apply.after_applying|1",
    "execution_receipts|execution_receipts.apply.before_commit|1",
    "execution_receipts|execution_receipts.apply.after_commit|1",
    "scheduler|scheduler.apply.after_applying|1",
    "scheduler|scheduler.apply.before_commit|1",
    "scheduler|scheduler.apply.after_commit|1",
    "raw_artifacts|reclamation_catalog_before_commit|1",
    "raw_artifacts|reclamation_catalog_after_commit|1",
    "raw_artifacts|reclamation_unlink_after_preunlink_reproof|1",
    "raw_artifacts|reclamation_unlink_after_unlink|1",
    "raw_artifacts|reclamation_unlink_event_after_commit|1",
    "raw_artifacts|reclamation_unlinked_before_commit|1",
    "raw_artifacts|reclamation_unlinked_after_commit|1",
    "raw_artifacts|reclamation_apply_before_commit|1",
    "raw_artifacts|reclamation_apply_after_commit|1",
    "capacity_ledger|capacity_ledger.apply.after_applying|1",
    "capacity_ledger|capacity_ledger.apply.before_commit|1",
    "capacity_ledger|capacity_ledger.apply.after_commit|1",
    "coordinator|record_apply.before_commit|1",
    "coordinator|record_apply.after_commit|1",
    "coordinator|record_apply.before_commit|2",
    "coordinator|record_apply.after_commit|2",
    "coordinator|record_apply.before_commit|3",
    "coordinator|record_apply.after_commit|3",
    "coordinator|record_apply.before_commit|4",
    "coordinator|record_apply.after_commit|4",
    "coordinator|record_apply.before_commit|5",
    "coordinator|record_apply.after_commit|5",
    "coordinator|finish_applied.before_commit|1",
    "coordinator|finish_applied.after_commit|1",
})


def _case_key(case: tuple[str, str, int]) -> str:
    return "|".join((case[0], case[1], str(case[2])))


def _case_id(case: tuple[str, str, int]) -> str:
    target, checkpoint, occurrence = case
    if target == "coordinator" and checkpoint.startswith("record_apply."):
        role = PARTICIPANT_APPLY_ORDER[occurrence - 1]
        return f"coordinator-{role}-{checkpoint}"
    if occurrence != 1:
        return f"{target}-{checkpoint}-occurrence-{occurrence}"
    return f"{target}-{checkpoint}"


assert len(CRASH_CASES) == 38
assert len(set(CRASH_CASES)) == 38
assert len(PINNED_EXPECTED_CRASH_CASE_KEYS) == 38
assert frozenset(map(_case_key, CRASH_CASES)) == PINNED_EXPECTED_CRASH_CASE_KEYS
assert len({_case_id(case) for case in CRASH_CASES}) == 38


def _manifest_path(root: Path) -> Path:
    return root / MANIFEST_NAME


def _page_bytes(page) -> bytes:
    """Serialize every public page field and exact sealed record byte."""

    return canonical_bytes({
        "after_cursor": page.after_cursor,
        "archive_binding_root_sha256": page.archive_binding_root_sha256,
        "bindings": [item.as_dict() for item in page.bindings],
        "candidate_authority_sha256": page.candidate_authority_sha256,
        "candidate_bindings_sha256": page.candidate_bindings_sha256,
        "consumer_epoch_root_sha256": page.consumer_epoch_root_sha256,
        "consumer_registry_root_sha256": page.consumer_registry_root_sha256,
        "exhausted": page.exhausted,
        "next_cursor": page.next_cursor,
        "page_sha256": page.page_sha256,
        "record_payload_hex": [item.hex() for item in page.record_payloads],
        "request_sha256": page.request_sha256,
        "sealed_archive_anchor_sha256": page.sealed_archive_anchor_sha256,
        "total_candidate_count": page.total_candidate_count,
    })


def _request_from_manifest(value: object) -> SourceReclamationRequestV2:
    if not isinstance(value, dict):
        raise AssertionError("crash manifest request is not an object")
    fields = {
        key: item
        for key, item in value.items()
        if key not in {"protocol_contract", "schema"}
    }
    return SourceReclamationRequestV2(**fields)


def _read_manifest(root: Path) -> dict[str, object]:
    document = json.loads(_manifest_path(root).read_text(encoding="ascii"))
    if not isinstance(document, dict) or document.get("contract") != MANIFEST_CONTRACT:
        raise AssertionError("native five-role crash manifest differs")
    return document


@contextmanager
def _read_only_database(path: Path) -> Iterator[sqlite3.Connection]:
    with closing(sqlite3.connect(
        f"{path.resolve().as_uri()}?mode=ro",
        uri=True,
    )) as database:
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA query_only=ON")
        yield database


def _lifecycle_unlink_paths(
    store: AcquisitionEvidenceLifecycleStore,
    request_sha256: str,
) -> tuple[Path, ...]:
    with _read_only_database(store.path) as database:
        rows = database.execute(
            "SELECT file_kind,filename FROM lifecycle_reclamation_files "
            "WHERE request_sha256=? ORDER BY file_kind,identity_sha256",
            (request_sha256,),
        ).fetchall()
    return tuple(
        (
            store.archive_segment_directory
            if row["file_kind"] == "segment"
            else store.archive_object_directory
        ) / str(row["filename"])
        for row in rows
    )


def _crash_callback(expected: str, occurrence: int = 1):
    if occurrence < 1:
        raise ValueError("crash checkpoint occurrence must be positive")
    seen = 0

    def crash(actual: str) -> None:
        nonlocal seen
        if actual == expected:
            seen += 1
            if seen == occurrence:
                os._exit(CRASH_EXIT_CODE)

    return crash


@dataclass
class _ReopenedCrashStack:
    root: Path
    sources: Authorities
    source_archive: AcquisitionSourceRetentionArchive
    retention: AcquisitionSourceRetentionArchiveV2
    coordinator: AcquisitionSourceReclamationApplyCoordinator
    registry: AcquisitionConsumerRegistryAuthority
    participants: tuple[SourceReclamationParticipantV2, ...]
    candidate: AcquisitionSourceCandidate
    request: SourceReclamationRequestV2
    manifest: dict[str, object]


def _participant_fault(
    target: str,
    expected: str,
    occurrence: int,
    role: str,
):
    return _crash_callback(expected, occurrence) if target == role else None


@contextmanager
def _reopen_stack(
    root: Path,
    *,
    target: str = "none",
    checkpoint: str = "none",
    occurrence: int = 1,
    clock_base: float = NOW + 20_000.0,
) -> Iterator[_ReopenedCrashStack]:
    manifest = _read_manifest(root)
    candidate_value = manifest["candidate"]
    if not isinstance(candidate_value, dict):
        raise AssertionError("crash manifest candidate is not an object")
    candidate = AcquisitionSourceCandidate(**candidate_value)
    request = _request_from_manifest(manifest["request"])
    source_root = root / "fixture" / "sources"

    desired = desired_state.DesiredStateStore(
        (source_root / "desired.sqlite3").resolve()
    )
    scheduler = autonomous_scheduler.AutonomousScheduler(
        (source_root / "scheduler.sqlite3").resolve(),
        desired_state_store=desired,
        plan_builder=lambda _state: (),
        clock=_Clock(clock_base + 10.0),
        lease_seconds=60.0,
    )
    raw = RawArtifactStore(
        (source_root / "raw").resolve(),
        limits=_RAW_LIMITS,
        fault_injector=(
            _crash_callback(checkpoint, occurrence)
            if target == "raw_artifacts"
            else None
        ),
        reclamation_directory_fsync=SYNC,
        reclamation_clock=_Clock(clock_base + 100.0),
    )
    raw.initialize()
    capacity = RawCapacityLedger(
        (source_root / "capacity.sqlite3").resolve(),
        profile=_CAPACITY_PROFILE,
        clock=_Clock(clock_base + 200.0),
    )
    executions = ExecutionReceiptStore(
        (source_root / "execution.sqlite3").resolve(),
        clock=_Clock(clock_base + 300.0),
    )
    evidence = AcquisitionEvidenceLifecycleStore(
        (source_root / "evidence").resolve(),
        clock=_Clock(clock_base + 400.0),
        fault_injector=(
            _crash_callback(checkpoint, occurrence)
            if target == "downstream_lifecycle"
            else None
        ),
        directory_fsync=SYNC,
    )
    object_paths_value = manifest["object_paths"]
    if not isinstance(object_paths_value, list):
        raise AssertionError("crash manifest object paths differ")
    sources = Authorities(
        raw=raw,
        scheduler=scheduler,
        executions=executions,
        capacity=capacity,
        evidence=evidence,
        candidates=(candidate,),
        object_paths=tuple(Path(item) for item in object_paths_value),
    )
    source_archive = AcquisitionSourceRetentionArchive(
        (root / "closure-source" / "source-archive").resolve(),
        raw_store=raw,
        scheduler=scheduler,
        execution_store=executions,
        capacity_ledger=capacity,
        evidence_store=evidence,
        limits=AcquisitionSourceRetentionLimits(
            min_terminal_age_seconds=10.0,
        ),
        clock=_Clock(clock_base + 500.0),
        directory_fsync=SYNC,
    )
    registry = AcquisitionConsumerRegistryAuthority(
        (
            root
            / "registry-authority"
            / "registry"
            / "registry.sqlite3"
        ).resolve(),
        clock=_Clock(clock_base + 600.0),
    )
    try:
        retention = AcquisitionSourceRetentionArchiveV2(
            (root / "retention-v2").resolve(),
            source_archive=source_archive,
            consumer_registry_authority=registry,
            raw_store=raw,
            scheduler=scheduler,
            execution_store=executions,
            capacity_ledger=capacity,
            evidence_store=evidence,
            limits=AcquisitionSourceRetentionLimitsV2(
                min_terminal_age_seconds=10.0,
            ),
            clock=_Clock(clock_base + 700.0),
            directory_fsync=SYNC,
        )
        coordinator_identity = manifest["coordinator_store_identity_sha256"]
        if not isinstance(coordinator_identity, str):
            raise AssertionError("crash manifest coordinator identity differs")
        coordinator = AcquisitionSourceReclamationApplyCoordinator(
            (root / "coordinator" / "reclamation.sqlite3").resolve(),
            expected_store_identity_sha256=coordinator_identity,
            candidate_authority=retention,
            clock=_Clock(clock_base + 800.0),
            directory_fsync=SYNC,
            fault_injector=(
                _crash_callback(checkpoint, occurrence)
                if target == "coordinator"
                else None
            ),
        )
        raw.bind_reclamation_authorities_v1(
            retention,
            coordinator.decision_authority,
        )
        raw.initialize_reclamation_extension_v1()
        decisions = coordinator.decision_authority
        participants: tuple[SourceReclamationParticipantV2, ...] = (
            evidence.reclamation_participant_v2(
                candidate_authority=retention,
                committed_decision_authority=decisions,
            ),
            ExecutionReceiptReclamationParticipantV2(
                executions,
                candidate_authority=retention,
                committed_decision_authority=decisions,
                clock=_Clock(clock_base + 900.0),
                fault_injector=_participant_fault(
                    target,
                    checkpoint,
                    occurrence,
                    "execution_receipts",
                ),
            ),
            SchedulerReclamationParticipantV2(
                scheduler,
                candidate_authority=retention,
                committed_decision_authority=decisions,
                clock=_Clock(clock_base + 1_000.0),
                fault_injector=_participant_fault(
                    target,
                    checkpoint,
                    occurrence,
                    "scheduler",
                ),
            ),
            raw,
            CapacityLedgerReclamationParticipantV2(
                capacity,
                candidate_authority=retention,
                committed_decision_authority=decisions,
                clock=_Clock(clock_base + 1_100.0),
                fault_injector=_participant_fault(
                    target,
                    checkpoint,
                    occurrence,
                    "capacity_ledger",
                ),
            ),
        )
        assert tuple(item.role for item in participants) == PARTICIPANT_APPLY_ORDER
        retention.bind_source_participants_v2(participants)
        yield _ReopenedCrashStack(
            root=root,
            sources=sources,
            source_archive=source_archive,
            retention=retention,
            coordinator=coordinator,
            registry=registry,
            participants=participants,
            candidate=candidate,
            request=request,
            manifest=manifest,
        )
    finally:
        registry.close()


def _setup_committed_fixture(root: Path) -> None:
    with _native_five_role_stack(root) as stack:
        capture = stack.sources.raw.load_capture(stack.candidate.raw_capture_id)
        assert capture is not None
        archive_files = tuple(
            stack.sources.evidence.archive_segment_directory.iterdir()
        ) + tuple(stack.sources.evidence.archive_object_directory.iterdir())
        assert archive_files
        with stack.retention.hold_candidate_page_v1(
            stack.request,
            after_cursor=0,
            max_candidates=64,
        ) as page:
            historical_page_hex = _page_bytes(page).hex()
        prepared = stack.coordinator.prepare_v2(
            stack.request,
            participants=stack.participants,
        )
        assert prepared.prepared_roles == PARTICIPANT_APPLY_ORDER
        stack.coordinator.commit_v1(
            stack.request.request_sha256,
            participants=stack.participants,
        )
        state = stack.coordinator.load_state_v2(stack.request.request_sha256)
        assert state is not None and state.state == COMMITTED
        lifecycle_unlink_paths = _lifecycle_unlink_paths(
            stack.sources.evidence,
            stack.request.request_sha256,
        )
        assert len(lifecycle_unlink_paths) == len(archive_files) == 2
        manifest = {
            "archive_files": [str(item) for item in archive_files],
            "candidate": stack.candidate.as_dict(),
            "capture_receipt_ids": [item.receipt_id for item in capture.members],
            "contract": MANIFEST_CONTRACT,
            "coordinator_store_identity_sha256": (
                stack.coordinator.store_identity_sha256
            ),
            "historical_page_hex": historical_page_hex,
            "lifecycle_unlink_paths": [
                str(item) for item in lifecycle_unlink_paths
            ],
            "object_paths": [str(item) for item in stack.sources.object_paths],
            "request": stack.request.as_dict(),
        }
        _manifest_path(root).write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")),
            encoding="ascii",
        )


def run_crash_child(
    root: Path,
    *,
    mode: str,
    target: str,
    checkpoint: str,
    occurrence: int,
) -> None:
    """Entry called only by the dedicated child-process helper."""

    if mode == "setup":
        if target != "none" or checkpoint != "none" or occurrence != 1:
            raise SystemExit("setup does not accept a crash target")
        _setup_committed_fixture(root)
        return
    if mode != "apply":
        raise SystemExit("unsupported native reclamation child mode")
    with _reopen_stack(
        root,
        target=target,
        checkpoint=checkpoint,
        occurrence=occurrence,
        clock_base=NOW + 10_000.0,
    ) as stack:
        stack.coordinator.apply_v1(
            stack.request.request_sha256,
            participants=stack.participants,
        )
    raise SystemExit(f"crash checkpoint was not reached: {target}:{checkpoint}")


def _run_child(
    root: Path,
    *,
    mode: str,
    target: str,
    checkpoint: str,
    occurrence: int = 1,
) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    python_path = [str(ROOT), str(ROOT / ".testdeps")]
    existing = environment.get("PYTHONPATH")
    if existing:
        python_path.append(existing)
    environment["PYTHONPATH"] = os.pathsep.join(python_path)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [
            sys.executable,
            "-B",
            str(CHILD),
            str(root),
            mode,
            target,
            checkpoint,
            str(occurrence),
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=90.0,
        check=False,
    )


def _file_inventory(root: Path) -> tuple[tuple[str, int, str], ...]:
    rows = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        metadata = path.lstat()
        if relative in _VOLATILE_LOCK_PATHS:
            rows.append((
                relative,
                metadata.st_size,
                f"volatile-lock-type:{stat.S_IFMT(metadata.st_mode)}",
            ))
            continue
        payload = path.read_bytes()
        rows.append((
            relative,
            len(payload),
            hashlib.sha256(payload).hexdigest(),
        ))
    return tuple(rows)


def _terminal_snapshot(stack: _ReopenedCrashStack) -> dict[str, object]:
    state = stack.coordinator.load_state_v2(stack.request.request_sha256)
    assert state is not None
    applies = tuple(
        item.load_source_local_apply_v1(stack.request.request_sha256)
        for item in stack.participants
    )
    assert all(item is not None for item in applies)
    capacity = stack.sources.capacity.audit()
    coordinator = stack.coordinator.audit_v2()
    execution = stack.sources.executions.stats()
    lifecycle = stack.sources.evidence.diagnostics()
    lifecycle_archive = stack.sources.evidence.audit_archive()
    raw = stack.sources.raw.integrity_check()["reclamation"]
    raw_stats = stack.sources.raw.stats()
    retention = stack.retention.audit_v2()
    scheduler = stack.sources.scheduler.diagnostics()
    source_archive = stack.source_archive.audit()
    historical_page = _historical_page_bytes(stack)
    files = _file_inventory(stack.root)
    return {
        "applies": tuple(item.as_dict() for item in applies if item is not None),
        "capacity": capacity,
        "coordinator": coordinator,
        "coordinator_state": state,
        "execution": execution,
        "files": files,
        "historical_page": historical_page,
        "lifecycle": lifecycle,
        "lifecycle_archive": lifecycle_archive,
        "raw": raw,
        "raw_stats": raw_stats,
        "retention": retention,
        "scheduler": scheduler,
        "source_archive": source_archive,
    }


def _historical_page_bytes(stack: _ReopenedCrashStack) -> bytes:
    with stack.retention.hold_candidate_page_v1(
        stack.request,
        after_cursor=0,
        max_candidates=64,
    ) as page:
        return _page_bytes(page)


def _expected_crash_prefix(
    target: str,
    checkpoint: str,
    occurrence: int,
) -> tuple[int, int, str]:
    if target == "coordinator":
        if checkpoint.startswith("record_apply."):
            coordinator_count = (
                occurrence
                if checkpoint == "record_apply.after_commit"
                else occurrence - 1
            )
            return coordinator_count, occurrence, COMMITTED
        if checkpoint == "finish_applied.before_commit":
            return len(PARTICIPANT_APPLY_ORDER), len(PARTICIPANT_APPLY_ORDER), COMMITTED
        if checkpoint == "finish_applied.after_commit":
            return len(PARTICIPANT_APPLY_ORDER), len(PARTICIPANT_APPLY_ORDER), APPLIED
        raise AssertionError("unknown coordinator crash checkpoint")

    role_index = PARTICIPANT_APPLY_ORDER.index(target)
    local_count = role_index
    if target in {"execution_receipts", "scheduler", "capacity_ledger"}:
        if checkpoint.endswith(".apply.after_commit"):
            local_count += 1
    elif target == "downstream_lifecycle":
        if checkpoint == "reclamation.apply.after_applied_commit":
            local_count += 1
    elif target == "raw_artifacts":
        if checkpoint == "reclamation_apply_after_commit":
            local_count += 1
    else:  # pragma: no cover - the static case table owns this vocabulary.
        raise AssertionError("unknown participant crash target")
    return role_index, local_count, COMMITTED


def _assert_crash_prefix(
    stack: _ReopenedCrashStack,
    *,
    target: str,
    checkpoint: str,
    occurrence: int,
) -> None:
    coordinator_count, local_count, expected_state = _expected_crash_prefix(
        target,
        checkpoint,
        occurrence,
    )
    state = stack.coordinator.load_state_v2(stack.request.request_sha256)
    assert state is not None and state.state == expected_state
    assert state.applied_roles == PARTICIPANT_APPLY_ORDER[:coordinator_count]
    local = tuple(
        item.load_source_local_apply_v1(stack.request.request_sha256)
        for item in stack.participants
    )
    local_roles = tuple(item.role for item in local if item is not None)
    assert local_roles == PARTICIPANT_APPLY_ORDER[:local_count]
    assert local_roles[:coordinator_count] == state.applied_roles
    assert local_count in {coordinator_count, coordinator_count + 1}


def _assert_crash_hot_journal(
    root: Path,
    *,
    target: str,
    checkpoint: str,
) -> None:
    expected: bool | None = None
    database_path: Path | None = None
    if target == "downstream_lifecycle":
        database_path = (
            root
            / "fixture"
            / "sources"
            / "evidence"
            / "acquisition_evidence.sqlite3"
        )
        expected = {
            "reclamation.apply.before_logical_commit": True,
            "reclamation.apply.after_logical_commit": False,
            "reclamation.apply.before_applied_commit": True,
            "reclamation.apply.after_applied_commit": False,
        }.get(checkpoint)
    elif target == "raw_artifacts":
        database_path = (
            root
            / "fixture"
            / "sources"
            / "raw"
            / "raw_artifacts.sqlite3"
        )
        expected = {
            "reclamation_catalog_before_commit": True,
            "reclamation_catalog_after_commit": False,
            "reclamation_unlink_event_after_commit": False,
            "reclamation_unlinked_before_commit": True,
            "reclamation_unlinked_after_commit": False,
            "reclamation_apply_before_commit": True,
            "reclamation_apply_after_commit": False,
        }.get(checkpoint)
    if expected is None or database_path is None:
        return
    journal_path = database_path.with_name(database_path.name + "-journal")
    assert journal_path.exists() is expected
    if expected:
        metadata = journal_path.lstat()
        assert stat.S_ISREG(metadata.st_mode)
        assert metadata.st_size > 0


def _lifecycle_boundary_facts(
    stack: _ReopenedCrashStack,
) -> dict[str, object]:
    request = stack.request.request_sha256
    with _read_only_database(stack.sources.evidence.path) as database:
        decision = database.execute(
            "SELECT state,active_slot,journal_sealed,floor_before_sha256,"
            "floor_before_json,expected_floor_after_sha256,"
            "expected_reclaimed_completion_count,"
            "expected_reclaimed_segment_count,expected_reclaimed_file_bytes,"
            "target_floor_ordinal,target_floor_segment_id,"
            "target_floor_file_sha256,target_floor_global_sequence,"
            "committed_decision_sha256,apply_receipt_sha256,"
            "apply_receipt_json FROM lifecycle_reclamation_decisions "
            "WHERE request_sha256=?",
            (request,),
        ).fetchone()
        assert decision is not None
        floor = database.execute(
            "SELECT * FROM lifecycle_archive_floor WHERE singleton=1"
        ).fetchone()
        assert floor is not None
        floor_sha256 = digest_mapping({
            "authority_schema": str(floor["authority_schema"]),
            "last_apply_receipt_sha256": floor["last_apply_receipt_sha256"],
            "pruned_through_file_sha256": floor[
                "pruned_through_file_sha256"
            ],
            "pruned_through_global_sequence": int(
                floor["pruned_through_global_sequence"]
            ),
            "pruned_through_segment_id": floor["pruned_through_segment_id"],
            "pruned_through_segment_ordinal": int(
                floor["pruned_through_segment_ordinal"]
            ),
            "reclaimed_completion_count": int(
                floor["reclaimed_completion_count"]
            ),
            "reclaimed_file_bytes": int(floor["reclaimed_file_bytes"]),
            "reclaimed_segment_count": int(floor["reclaimed_segment_count"]),
            "schema": "stv3-worker-acquisition-evidence-archive-floor-v1",
        })
        floor_before = json.loads(
            bytes(decision["floor_before_json"]).decode("ascii")
        )
        sealed_after = {
            **floor_before,
            "pruned_through_file_sha256": decision[
                "target_floor_file_sha256"
            ],
            "pruned_through_global_sequence": int(
                decision["target_floor_global_sequence"]
            ),
            "pruned_through_segment_id": decision["target_floor_segment_id"],
            "pruned_through_segment_ordinal": int(
                decision["target_floor_ordinal"]
            ),
            "reclaimed_completion_count": int(
                decision["expected_reclaimed_completion_count"]
            ),
            "reclaimed_file_bytes": int(
                decision["expected_reclaimed_file_bytes"]
            ),
            "reclaimed_segment_count": int(
                decision["expected_reclaimed_segment_count"]
            ),
        }
        assert digest_mapping(sealed_after) == decision[
            "expected_floor_after_sha256"
        ]
        apply_receipt_sha256 = decision["apply_receipt_sha256"]
        applied_floor_sha256 = (
            None
            if apply_receipt_sha256 is None
            else digest_mapping({
                **sealed_after,
                "last_apply_receipt_sha256": str(apply_receipt_sha256),
            })
        )
        if floor_sha256 == decision["floor_before_sha256"]:
            floor_position = "before"
        elif floor_sha256 == decision["expected_floor_after_sha256"]:
            floor_position = "after"
        elif floor_sha256 == applied_floor_sha256:
            floor_position = "applied"
        else:
            floor_position = floor_sha256

        def count(statement: str) -> int:
            return int(database.execute(statement, (request,)).fetchone()[0])

        archive_rows = (
            count(
                "SELECT count(*) FROM archive_index WHERE event_id IN ("
                "SELECT event_id FROM lifecycle_reclamation_members "
                "WHERE request_sha256=?)"
            ),
            count(
                "SELECT count(*) FROM archive_segments WHERE segment_id IN ("
                "SELECT lifecycle_archive_segment_id FROM "
                "lifecycle_reclamation_members WHERE request_sha256=?)"
            ),
            count(
                "SELECT count(*) FROM archive_objects WHERE content_sha256 IN ("
                "SELECT content_sha256 FROM "
                "lifecycle_reclamation_object_references "
                "WHERE request_sha256=? AND delete_file=1)"
            ),
        )
        journal_file_rows = count(
            "SELECT count(*) FROM lifecycle_reclamation_files "
            "WHERE request_sha256=?"
        )
    paths = stack.manifest["lifecycle_unlink_paths"]
    assert isinstance(paths, list) and len(paths) == 2
    return {
        "active_slot": decision["active_slot"],
        "archive_rows": archive_rows,
        "committed": decision["committed_decision_sha256"] is not None,
        "decision_state": str(decision["state"]),
        "file_present": tuple(Path(item).exists() for item in paths),
        "floor_position": floor_position,
        "has_apply_receipt": decision["apply_receipt_json"] is not None,
        "journal_file_rows": journal_file_rows,
        "journal_sealed": int(decision["journal_sealed"]),
    }


def _assert_lifecycle_boundary(
    stack: _ReopenedCrashStack,
    checkpoint: str,
    occurrence: int,
) -> None:
    prepared = {
        "active_slot": 1,
        "archive_rows": (1, 1, 1),
        "committed": False,
        "decision_state": "PREPARED",
        "file_present": (True, True),
        "floor_position": "before",
        "has_apply_receipt": False,
        "journal_file_rows": 2,
        "journal_sealed": 1,
    }
    applying_files = {
        **prepared,
        # The immutable segment catalogue is historical authority; only its
        # live index/object rows and physical files are reclaimed.
        "archive_rows": (0, 1, 0),
        "committed": True,
        "decision_state": "APPLYING",
        "floor_position": "after",
    }
    expected_by_boundary = {
        ("reclamation.apply.before_logical_commit", 1): prepared,
        ("reclamation.apply.after_logical_commit", 1): applying_files,
        ("reclamation.apply.before_file_unlink", 1): applying_files,
        ("reclamation.apply.after_file_unlink", 1): {
            **applying_files,
            "file_present": (False, True),
        },
        ("reclamation.apply.before_file_unlink", 2): {
            **applying_files,
            "file_present": (False, True),
        },
        ("reclamation.apply.after_file_unlink", 2): {
            **applying_files,
            "file_present": (False, False),
        },
        ("reclamation.apply.before_applied_commit", 1): {
            **applying_files,
            "file_present": (False, False),
        },
        ("reclamation.apply.after_applied_commit", 1): {
            **applying_files,
            "active_slot": None,
            "decision_state": "APPLIED",
            "file_present": (False, False),
            "floor_position": "applied",
            "has_apply_receipt": True,
        },
    }
    expected = expected_by_boundary.get((checkpoint, occurrence))
    if expected is None:
        raise AssertionError("unknown lifecycle crash boundary occurrence")
    assert _lifecycle_boundary_facts(stack) == expected


def _raw_boundary_facts(stack: _ReopenedCrashStack) -> dict[str, object]:
    request = stack.request.request_sha256
    receipt_ids = stack.manifest["capture_receipt_ids"]
    object_paths = stack.manifest["object_paths"]
    assert isinstance(receipt_ids, list) and receipt_ids
    assert isinstance(object_paths, list) and object_paths
    content_sha256s = [Path(item).stem for item in object_paths]
    with _read_only_database(stack.sources.raw.catalog_path) as database:
        catalog_row = database.execute(
            "SELECT state,unlink_item_count FROM "
            "raw_artifact_reclamation_catalog_applies "
            "WHERE request_sha256=?",
            (request,),
        ).fetchone()
        event_states = tuple(
            str(row[0]) for row in database.execute(
                "SELECT state FROM raw_artifact_reclamation_unlink_durable "
                "WHERE request_sha256=? ORDER BY unlink_ordinal",
                (request,),
            ).fetchall()
        )
        batch_row = database.execute(
            "SELECT state FROM raw_artifact_reclamation_unlinked_batches "
            "WHERE request_sha256=?",
            (request,),
        ).fetchone()
        apply_row = database.execute(
            "SELECT state FROM raw_artifact_reclamation_applies "
            "WHERE request_sha256=?",
            (request,),
        ).fetchone()

        def count(statement: str, values: tuple[object, ...]) -> int:
            return int(database.execute(statement, values).fetchone()[0])

        intent_count = count(
            "SELECT count(*) FROM raw_artifact_reclamation_unlink_intents "
            "WHERE request_sha256=?",
            (request,),
        )
        failure_count = count(
            "SELECT count(*) FROM raw_artifact_reclamation_unlink_failures "
            "WHERE request_sha256=?",
            (request,),
        )
        capture_rows = count(
            "SELECT count(*) FROM raw_artifact_captures WHERE capture_id=?",
            (stack.candidate.raw_capture_id,),
        )
        receipt_rows = count(
            "SELECT count(*) FROM raw_artifact_receipts WHERE receipt_id IN ("
            + ",".join("?" for _item in receipt_ids)
            + ")",
            tuple(receipt_ids),
        )
        object_rows = count(
            "SELECT count(*) FROM raw_artifact_objects "
            "WHERE content_sha256 IN ("
            + ",".join("?" for _item in content_sha256s)
            + ")",
            tuple(content_sha256s),
        )
    return {
        "apply_state": None if apply_row is None else str(apply_row[0]),
        "base_rows": (capture_rows, receipt_rows, object_rows),
        "batch_state": None if batch_row is None else str(batch_row[0]),
        "catalog": (
            None
            if catalog_row is None
            else (str(catalog_row["state"]), int(catalog_row["unlink_item_count"]))
        ),
        "event_states": event_states,
        "failure_count": failure_count,
        "intent_count": intent_count,
        "object_present": tuple(Path(item).exists() for item in object_paths),
    }


def _assert_raw_boundary(
    stack: _ReopenedCrashStack,
    checkpoint: str,
) -> None:
    object_count = len(stack.manifest["object_paths"])
    receipt_count = len(stack.manifest["capture_receipt_ids"])
    old = {
        "apply_state": None,
        "base_rows": (1, receipt_count, object_count),
        "batch_state": None,
        "catalog": None,
        "event_states": (),
        "failure_count": 0,
        "intent_count": 0,
        "object_present": (True,) * object_count,
    }
    pending = {
        **old,
        "base_rows": (0, 0, 0),
        "catalog": (RAW_RECLAMATION_FILES_PENDING, object_count),
        "intent_count": object_count,
    }
    unlinked = {
        **pending,
        "object_present": (False,) * object_count,
    }
    event_durable = {
        **unlinked,
        "event_states": (RAW_RECLAMATION_UNLINKED_DURABLE,) * object_count,
    }
    batch_durable = {
        **event_durable,
        "batch_state": RAW_RECLAMATION_UNLINKED_DURABLE,
    }
    expected = {
        "reclamation_catalog_before_commit": old,
        "reclamation_catalog_after_commit": pending,
        "reclamation_unlink_after_preunlink_reproof": pending,
        "reclamation_unlink_after_unlink": unlinked,
        "reclamation_unlink_event_after_commit": event_durable,
        "reclamation_unlinked_before_commit": event_durable,
        "reclamation_unlinked_after_commit": batch_durable,
        "reclamation_apply_before_commit": batch_durable,
        "reclamation_apply_after_commit": {
            **batch_durable,
            "apply_state": RAW_RECLAMATION_APPLIED,
        },
    }[checkpoint]
    assert _raw_boundary_facts(stack) == expected


def _assert_source_boundary(
    stack: _ReopenedCrashStack,
    *,
    target: str,
    checkpoint: str,
    occurrence: int,
) -> None:
    if target == "downstream_lifecycle":
        _assert_lifecycle_boundary(stack, checkpoint, occurrence)
    elif target == "raw_artifacts":
        _assert_raw_boundary(stack, checkpoint)


def _assert_terminal_effects(stack: _ReopenedCrashStack) -> None:
    candidate = stack.candidate
    state = stack.coordinator.load_state_v2(stack.request.request_sha256)
    assert state is not None
    assert state.state == APPLIED
    assert state.applied_roles == PARTICIPANT_APPLY_ORDER
    assert stack.coordinator.audit_v2()["event_count"] == 4
    assert stack.sources.evidence.lookup_sequence(
        candidate.evidence_stream_id,
        candidate.evidence_stream_sequence,
    ).status == "window_expired"
    with pytest.raises(CompletionWindowExpired, match="reclaimed"):
        stack.sources.evidence.load_terminal_consumption(
            candidate.evidence_stream_id,
            candidate.evidence_stream_sequence,
        )
    assert stack.sources.executions.load_job_receipts(
        candidate.acquisition_job_id
    ) == ()
    assert stack.sources.executions.stats()["receipt_count"] == 0
    assert stack.sources.executions.stats()["terminal_count"] == 0
    assert stack.sources.scheduler.get_record(candidate.scheduler_job_id) is None
    assert stack.sources.scheduler.terminal_proof(candidate.scheduler_job_id) is None
    assert stack.sources.scheduler.diagnostics()["outbox_rows"] == 0
    assert stack.sources.raw.load_capture(candidate.raw_capture_id) is None
    receipt_ids = stack.manifest["capture_receipt_ids"]
    assert isinstance(receipt_ids, list)
    assert all(stack.sources.raw.load_receipt(item) is None for item in receipt_ids)
    assert stack.sources.raw.stats()["object_count"] == 0
    assert stack.sources.raw.stats()["receipt_count"] == 0
    assert stack.sources.raw.stats()["capture_count"] == 0
    assert all(not Path(item).exists() for item in stack.manifest["object_paths"])
    assert stack.sources.capacity.load_for_job(
        provider="dwd",
        job_identity=candidate.acquisition_job_id,
    ) is None
    capacity = stack.sources.capacity.audit()
    assert capacity["reservation_rows"] == 0
    assert capacity["committed_count"] == 0
    assert all(not Path(item).exists() for item in stack.manifest["archive_files"])
    assert stack.sources.evidence.audit_archive()["healthy"] is True
    assert stack.source_archive.audit()["healthy"] is True
    assert stack.retention.audit_v2()["healthy"] is True
    assert _historical_page_bytes(stack).hex() == stack.manifest[
        "historical_page_hex"
    ]


@pytest.mark.parametrize(
    ("target", "checkpoint", "occurrence"),
    CRASH_CASES,
    ids=[_case_id(case) for case in CRASH_CASES],
)
def test_native_five_role_real_process_crash_resumes_exactly_once(
    tmp_path: Path,
    target: str,
    checkpoint: str,
    occurrence: int,
) -> None:
    setup = _run_child(
        tmp_path,
        mode="setup",
        target="none",
        checkpoint="none",
        occurrence=1,
    )
    assert setup.returncode == 0, setup.stderr
    crashed = _run_child(
        tmp_path,
        mode="apply",
        target=target,
        checkpoint=checkpoint,
        occurrence=occurrence,
    )
    assert crashed.returncode == CRASH_EXIT_CODE, (
        crashed.stdout,
        crashed.stderr,
    )
    _assert_crash_hot_journal(
        tmp_path,
        target=target,
        checkpoint=checkpoint,
    )

    with _reopen_stack(tmp_path, clock_base=NOW + 20_000.0) as stack:
        _assert_crash_prefix(
            stack,
            target=target,
            checkpoint=checkpoint,
            occurrence=occurrence,
        )
        _assert_source_boundary(
            stack,
            target=target,
            checkpoint=checkpoint,
            occurrence=occurrence,
        )
        resumed = stack.coordinator.resume_v2(
            stack.request,
            participants=stack.participants,
            allow_commit=False,
        )
        assert resumed.state == APPLIED
        assert resumed.applied_roles == PARTICIPANT_APPLY_ORDER
        _assert_terminal_effects(stack)

        before_replay = _terminal_snapshot(stack)
        predecessor = None
        for participant, expected in zip(
            stack.participants,
            (
                item.load_source_local_apply_v1(stack.request.request_sha256)
                for item in stack.participants
            ),
            strict=True,
        ):
            assert expected is not None
            assert participant.apply_source_local_v1(
                stack.request.request_sha256,
                predecessor_apply_receipt_sha256=predecessor,
            ) == expected
            predecessor = expected.apply_receipt_sha256
        replayed = stack.coordinator.resume_v2(
            stack.request,
            participants=stack.participants,
            allow_commit=False,
        )
        assert replayed == resumed
        assert _terminal_snapshot(stack) == before_replay
