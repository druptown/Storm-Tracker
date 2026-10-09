"""Fresh-process restart consistency for all retention-v2 interruption labels."""
from __future__ import annotations

import ast
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Iterator

import pytest

from storm_tracker_worker import autonomous_scheduler, desired_state
from storm_tracker_worker.acquisition_consumer_registry import (
    AcquisitionConsumerRegistryAuthority,
    parse_consumer_closure_receipt,
)
from storm_tracker_worker.acquisition_control_reclamation import (
    CapacityLedgerReclamationParticipantV2,
    ExecutionReceiptReclamationParticipantV2,
    SchedulerReclamationParticipantV2,
)
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
)
from storm_tracker_worker.acquisition_execution_store import ExecutionReceiptStore
from storm_tracker_worker.acquisition_source_reclamation_protocol import (
    PARTICIPANT_APPLY_ORDER,
    SourceReclamationParticipantV2,
    SourceReclamationRequestV2,
)
from storm_tracker_worker.acquisition_source_retention import (
    AcquisitionSourceArchiveSealResultV2,
    AcquisitionSourceCandidate,
    AcquisitionSourceRetentionArchive,
    AcquisitionSourceRetentionArchiveV2,
    AcquisitionSourceRetentionLimits,
    AcquisitionSourceRetentionLimitsV2,
)
from storm_tracker_worker.raw_artifacts import RawArtifactLimits, RawArtifactStore
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityProfile,
)

from test_worker_acquisition_consumer_registry import _archive_authorities
from test_worker_acquisition_control_reclamation import _Clock, _DecisionAuthority
from test_worker_acquisition_lifecycle_retention_v2 import _v2_context
from test_worker_acquisition_source_retention import NOW, SYNC, Authorities


ROOT = Path(__file__).resolve().parents[1]
CHILD = (
    ROOT
    / "tests"
    / "helpers"
    / "worker_acquisition_source_retention_v2_restart_child.py"
)
MANIFEST_NAME = "retention-v2-restart-manifest.json"
MANIFEST_CONTRACT = "stv3-test-retention-v2-restart-consistency-v1"
EXIT_CODE = 73

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
_V2_LIMITS = AcquisitionSourceRetentionLimitsV2(
    min_terminal_age_seconds=10.0,
)

RESTART_CASES = (
    ("candidate_hold", "retention_v2.candidate_hold.before_yield"),
    ("candidate_hold", "retention_v2.candidate_hold.after_reproof"),
    ("seal", "retention_v2.seal.after_pending"),
    ("seal", "retention_v2.seal.after_target.objects"),
    ("seal", "retention_v2.seal.after_target.metadata"),
    ("seal", "retention_v2.seal.after_target.references"),
    ("seal", "retention_v2.seal.after_target.segments"),
    ("seal", "retention_v2.seal.after_target.manifests"),
    ("seal", "retention_v2.seal.after_target.anchors"),
    ("seal", "retention_v2.seal.after_target.candidate_sets"),
    ("seal", "retention_v2.seal.before_head"),
    ("seal", "retention_v2.seal.after_head"),
    (
        "current_request_hold",
        "retention_v2.current_request_hold.before_yield",
    ),
    (
        "current_request_hold",
        "retention_v2.current_request_hold.after_reproof",
    ),
)
PINNED_RESTART_LABELS = frozenset({
    "retention_v2.candidate_hold.before_yield",
    "retention_v2.candidate_hold.after_reproof",
    "retention_v2.seal.after_pending",
    "retention_v2.seal.after_target.objects",
    "retention_v2.seal.after_target.metadata",
    "retention_v2.seal.after_target.references",
    "retention_v2.seal.after_target.segments",
    "retention_v2.seal.after_target.manifests",
    "retention_v2.seal.after_target.anchors",
    "retention_v2.seal.after_target.candidate_sets",
    "retention_v2.seal.before_head",
    "retention_v2.seal.after_head",
    "retention_v2.current_request_hold.before_yield",
    "retention_v2.current_request_hold.after_reproof",
})


def _manifest_path(root: Path) -> Path:
    return root / MANIFEST_NAME


def _request_from_mapping(value: object) -> SourceReclamationRequestV2 | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise AssertionError("retention-v2 restart request is not a mapping")
    fields = {
        key: item
        for key, item in value.items()
        if key not in {"protocol_contract", "schema"}
    }
    return SourceReclamationRequestV2(**fields)


def _read_manifest(root: Path) -> dict[str, object]:
    value = json.loads(_manifest_path(root).read_text(encoding="ascii"))
    if not isinstance(value, dict) or value.get("contract") != MANIFEST_CONTRACT:
        raise AssertionError("retention-v2 restart manifest differs")
    return value


def _setup_restart_case(root: Path, operation: str) -> None:
    if operation not in {"seal", "candidate_hold", "current_request_hold"}:
        raise SystemExit("unsupported retention-v2 restart operation")
    with _v2_context(root, seal=operation != "seal") as context:
        request = None if context.request is None else context.request.as_dict()
        _manifest_path(root).write_text(
            json.dumps(
                {
                    "candidate": context.sources.candidates[0].as_dict(),
                    "closure": context.closure.as_dict(),
                    "contract": MANIFEST_CONTRACT,
                    "object_paths": [str(item) for item in context.sources.object_paths],
                    "operation": operation,
                    "request": request,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            encoding="ascii",
        )


@dataclass
class _RestartStack:
    sources: Authorities
    source_archive: AcquisitionSourceRetentionArchive
    retention: AcquisitionSourceRetentionArchiveV2
    registry: AcquisitionConsumerRegistryAuthority
    participants: tuple[SourceReclamationParticipantV2, ...]
    candidate: AcquisitionSourceCandidate
    closure: object
    request: SourceReclamationRequestV2 | None


@contextmanager
def _reopen_restart_case(
    root: Path,
    *,
    fault=None,
) -> Iterator[_RestartStack]:
    manifest = _read_manifest(root)
    candidate_value = manifest["candidate"]
    if not isinstance(candidate_value, dict):
        raise AssertionError("retention-v2 restart candidate is not a mapping")
    candidate = AcquisitionSourceCandidate(**candidate_value)
    closure = parse_consumer_closure_receipt(manifest["closure"])
    request = _request_from_mapping(manifest["request"])
    source_root = (root / "authorities" / "sources").resolve()
    desired = desired_state.DesiredStateStore(
        (source_root / "desired.sqlite3").resolve()
    )
    scheduler = autonomous_scheduler.AutonomousScheduler(
        (source_root / "scheduler.sqlite3").resolve(),
        desired_state_store=desired,
        plan_builder=lambda _state: (),
        clock=_Clock(NOW + 1_000.0),
        lease_seconds=60.0,
    )
    raw = RawArtifactStore(
        (source_root / "raw").resolve(),
        limits=_RAW_LIMITS,
    )
    raw.initialize()
    capacity = RawCapacityLedger(
        (source_root / "capacity.sqlite3").resolve(),
        profile=_CAPACITY_PROFILE,
        clock=_Clock(NOW + 1_100.0),
    )
    executions = ExecutionReceiptStore(
        (source_root / "execution.sqlite3").resolve(),
        clock=_Clock(NOW + 1_200.0),
    )
    evidence = AcquisitionEvidenceLifecycleStore(
        (source_root / "evidence").resolve(),
        clock=_Clock(NOW + 1_300.0),
        directory_fsync=SYNC,
    )
    object_paths_value = manifest["object_paths"]
    if not isinstance(object_paths_value, list):
        raise AssertionError("retention-v2 restart object paths differ")
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
        clock=_Clock(NOW + 1_400.0),
        directory_fsync=SYNC,
    )
    registry = AcquisitionConsumerRegistryAuthority(
        (root / "registry-authority" / "registry" / "registry.sqlite3").resolve(),
        clock=_Clock(NOW + 1_500.0),
    )
    try:
        catalog, reproof = _archive_authorities("reclamation-closure")
        current = registry.load_current()
        if current is None:
            raise AssertionError("retention-v2 restart registry epoch is absent")
        registry.verifier(
            source_catalog=catalog,
            source_reproof_authority=reproof,
        ).bind_current_epoch(
            claimed_epoch_root_sha256=current[1].epoch_root_sha256,
        )
        retention = AcquisitionSourceRetentionArchiveV2(
            (root / "retention-v2").resolve(),
            source_archive=source_archive,
            consumer_registry_authority=registry,
            raw_store=raw,
            scheduler=scheduler,
            execution_store=executions,
            capacity_ledger=capacity,
            evidence_store=evidence,
            limits=_V2_LIMITS,
            clock=_Clock(NOW + 1_600.0),
            fault_injector=fault,
            directory_fsync=SYNC,
        )
        decisions = _DecisionAuthority()
        raw.bind_reclamation_authorities_v1(retention, decisions)
        raw.initialize_reclamation_extension_v1()
        participants: tuple[SourceReclamationParticipantV2, ...] = (
            evidence.reclamation_participant_v2(
                candidate_authority=retention,
                committed_decision_authority=decisions,
            ),
            ExecutionReceiptReclamationParticipantV2(
                executions,
                candidate_authority=retention,
                committed_decision_authority=decisions,
                clock=_Clock(NOW + 1_700.0),
            ),
            SchedulerReclamationParticipantV2(
                scheduler,
                candidate_authority=retention,
                committed_decision_authority=decisions,
                clock=_Clock(NOW + 1_800.0),
            ),
            raw,
            CapacityLedgerReclamationParticipantV2(
                capacity,
                candidate_authority=retention,
                committed_decision_authority=decisions,
                clock=_Clock(NOW + 1_900.0),
            ),
        )
        assert tuple(item.role for item in participants) == PARTICIPANT_APPLY_ORDER
        retention.bind_source_participants_v2(participants)
        yield _RestartStack(
            sources=sources,
            source_archive=source_archive,
            retention=retention,
            registry=registry,
            participants=participants,
            candidate=candidate,
            closure=closure,
            request=request,
        )
    finally:
        registry.close()


def _run_operation(stack: _RestartStack, operation: str) -> object:
    if operation == "seal":
        return stack.retention.seal_candidates_v2(
            (stack.candidate,),
            consumer_closures=(stack.closure,),
            now=NOW + 500.0,
        )
    if stack.request is None:
        raise AssertionError("retention-v2 hold operation lacks a request")
    if operation == "candidate_hold":
        with stack.retention.hold_candidate_page_v1(
            stack.request,
            after_cursor=0,
            max_candidates=64,
        ) as page:
            return page
    if operation == "current_request_hold":
        with stack.retention.hold_current_request_authority_v1(
            stack.request
        ) as page:
            return page
    raise AssertionError("unknown retention-v2 restart operation")


def run_retention_v2_restart_child(
    root: Path,
    *,
    mode: str,
    operation: str,
    checkpoint: str,
) -> None:
    if (operation, checkpoint) not in RESTART_CASES:
        raise SystemExit("retention-v2 operation/checkpoint is not allowlisted")
    if mode == "setup":
        _setup_restart_case(root, operation)
        return
    if mode != "interrupt":
        raise SystemExit("unsupported retention-v2 child mode")

    def interrupt(point: str) -> None:
        if point == checkpoint:
            os._exit(EXIT_CODE)

    with _reopen_restart_case(root, fault=interrupt) as stack:
        _run_operation(stack, operation)
    raise SystemExit("allowlisted retention-v2 interruption was not reached")


def _run_child(
    root: Path,
    *,
    mode: str,
    operation: str,
    checkpoint: str,
) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            str(ROOT / ".testdeps"),
            str(ROOT),
            environment.get("PYTHONPATH", ""),
        )
        if value
    )
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONNOUSERSITE"] = "1"
    return subprocess.run(
        [
            sys.executable,
            "-B",
            str(CHILD),
            str(root),
            mode,
            operation,
            checkpoint,
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=90.0,
        check=False,
    )


def _retention_v2_fault_inventory() -> frozenset[str]:
    module_path = Path(
        sys.modules[AcquisitionSourceRetentionArchiveV2.__module__].__file__
    )
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    archive = next(
        node
        for node in tree.body
        if (
            isinstance(node, ast.ClassDef)
            and node.name == "AcquisitionSourceRetentionArchiveV2"
        )
    )
    fixed: set[str] = set()
    target_categories: set[str] = set()
    dynamic_target_found = False
    for call in ast.walk(archive):
        if not (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id == "self"
            and call.func.attr == "_fault_v2"
            and len(call.args) == 1
        ):
            continue
        argument = call.args[0]
        if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
            fixed.add(f"retention_v2.{argument.value}")
        elif isinstance(argument, ast.JoinedStr):
            constants = "".join(
                str(item.value)
                for item in argument.values
                if isinstance(item, ast.Constant)
            )
            dynamic_target_found |= constants == "seal.after_target."
    seal_method = next(
        node
        for node in archive.body
        if isinstance(node, ast.FunctionDef) and node.name == "seal_candidates_v2"
    )
    joined_strings = (
        node for node in ast.walk(seal_method) if isinstance(node, ast.JoinedStr)
    )
    for joined in joined_strings:
        prefix = next(
            (
                str(item.value)
                for item in joined.values
                if isinstance(item, ast.Constant)
                and isinstance(item.value, str)
                and "/" in item.value
            ),
            None,
        )
        if prefix is not None:
            category = prefix.split("/", 1)[0]
            if category in {
                "objects",
                "metadata",
                "references",
                "segments",
                "manifests",
                "anchors",
                "candidate_sets",
            }:
                target_categories.add(category)
    if not dynamic_target_found:
        return frozenset(fixed)
    return frozenset(
        fixed
        | {
            f"retention_v2.seal.after_target.{category}"
            for category in target_categories
        }
    )


def _file_inventory(root: Path) -> tuple[tuple[str, int, str], ...]:
    return tuple(
        (
            path.relative_to(root).as_posix(),
            path.stat().st_size,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in sorted(item for item in root.rglob("*") if item.is_file())
    )


def _page_identity(page: object) -> tuple[object, ...]:
    return (
        page.page_sha256,
        page.request_sha256,
        page.candidate_bindings_sha256,
        tuple(item.candidate_sha256 for item in page.bindings),
        tuple(hashlib.sha256(item).hexdigest() for item in page.record_payloads),
    )


def test_retention_v2_restart_inventory_expands_exact_fourteen_labels():
    discovered = _retention_v2_fault_inventory()
    assert discovered == PINNED_RESTART_LABELS
    assert len(discovered) == len(RESTART_CASES) == 14
    assert len(set(RESTART_CASES)) == 14


@pytest.mark.parametrize(
    ("operation", "checkpoint"),
    RESTART_CASES,
    ids=[
        checkpoint.removeprefix("retention_v2.")
        for _operation, checkpoint in RESTART_CASES
    ],
)
def test_retention_v2_interruption_reopens_and_repeats_without_growth(
    tmp_path: Path,
    operation: str,
    checkpoint: str,
) -> None:
    root = (tmp_path / checkpoint.replace(".", "-")).resolve()
    setup = _run_child(
        root,
        mode="setup",
        operation=operation,
        checkpoint=checkpoint,
    )
    assert setup.returncode == 0, (setup.stdout, setup.stderr)
    interrupted = _run_child(
        root,
        mode="interrupt",
        operation=operation,
        checkpoint=checkpoint,
    )
    assert interrupted.returncode == EXIT_CODE, (
        interrupted.stdout,
        interrupted.stderr,
    )
    assert interrupted.stdout == ""
    assert interrupted.stderr == ""

    with _reopen_restart_case(root) as stack:
        recovered_audit = stack.retention.audit_v2()
        expected_generation = int(
            operation != "seal" or checkpoint == "retention_v2.seal.after_head"
        )
        assert recovered_audit["healthy"] is True
        assert recovered_audit["archive_generation"] == expected_generation
        assert recovered_audit["archived_candidate_count"] == expected_generation
        assert not stack.retention.pending_path.exists()

        seal = stack.retention.seal_candidates_v2(
            (stack.candidate,),
            consumer_closures=(stack.closure,),
            now=NOW + 500.0,
        )
        assert isinstance(seal, AcquisitionSourceArchiveSealResultV2)
        if stack.request is None:
            request = stack.retention.create_reclamation_request_v2(
                seal.candidate_bindings_sha256,
                source_binding_sha256="1" * 64,
                source_view_sha256="2" * 64,
                created_at=NOW + 600.0,
            )
        else:
            request = stack.request
        with stack.retention.hold_candidate_page_v1(
            request,
            after_cursor=0,
            max_candidates=64,
        ) as candidate_page:
            candidate_page_id = _page_identity(candidate_page)
        with stack.retention.hold_current_request_authority_v1(
            request
        ) as current_page:
            current_page_id = _page_identity(current_page)
        assert candidate_page_id == current_page_id

        stable_audit = stack.retention.audit_v2()
        stable_head = stack.retention._read_head_v2()
        stable_set = stack.retention._read_candidate_set_v2(
            seal.candidate_bindings_sha256
        )
        stable_files = _file_inventory(stack.retention.root)
        assert stable_audit["healthy"] is True
        assert stable_audit["archive_generation"] == 1
        assert stable_audit["archived_candidate_count"] == 1
        assert stable_audit["object_count"] == 1

        repeated = stack.retention.seal_candidates_v2(
            (stack.candidate,),
            consumer_closures=(stack.closure,),
            now=NOW + 501.0,
        )
        assert repeated == seal
        with stack.retention.hold_candidate_page_v1(
            request,
            after_cursor=0,
            max_candidates=64,
        ) as repeated_candidate:
            assert _page_identity(repeated_candidate) == candidate_page_id
        with stack.retention.hold_current_request_authority_v1(
            request
        ) as repeated_current:
            assert _page_identity(repeated_current) == current_page_id
        assert stack.retention.audit_v2() == stable_audit
        assert stack.retention._read_head_v2() == stable_head
        assert stack.retention._read_candidate_set_v2(
            seal.candidate_bindings_sha256
        ) == stable_set
        assert _file_inventory(stack.retention.root) == stable_files
        assert not stack.retention.pending_path.exists()
