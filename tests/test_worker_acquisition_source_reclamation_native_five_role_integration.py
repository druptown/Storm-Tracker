from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Iterator

import pytest

from storm_tracker_worker.acquisition_consumer_registry import (
    AcquisitionConsumerRegistryAuthority,
    AcquisitionConsumerRegistryBoundEpochVerifier,
)
from storm_tracker_worker.acquisition_control_reclamation import (
    CapacityLedgerReclamationParticipantV2,
    ExecutionReceiptReclamationParticipantV2,
    SchedulerReclamationParticipantV2,
)
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
    CompletionWindowExpired,
    EvidenceRetentionPolicy,
)
from storm_tracker_worker.acquisition_source_reclamation_apply_authority import (
    AcquisitionSourceReclamationApplyCoordinator,
)
from storm_tracker_worker.acquisition_source_reclamation_protocol import (
    ABORTED,
    APPLIED,
    COMMITTED,
    PARTICIPANT_APPLY_ORDER,
    PREPARED,
    SourceReclamationParticipantV2,
)
from storm_tracker_worker.acquisition_source_retention import (
    AcquisitionSourceArchiveSealResultV2,
    AcquisitionSourceRetentionArchive,
    AcquisitionSourceRetentionArchiveV2,
    AcquisitionSourceRetentionConflict,
    AcquisitionSourceRetentionLimitsV2,
)
from storm_tracker_worker.raw_artifacts import RawArtifactStore

from test_worker_acquisition_consumer_registry import _archive_authorities
from test_worker_acquisition_control_reclamation import _Clock
from test_worker_acquisition_source_reclamation_registry_closure import (
    WORKLOAD_ID,
    _bound_registry,
    _exact_route_context,
)
from test_worker_acquisition_source_retention import (
    NOW,
    SYNC,
    Authorities,
    _archive,
)


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


class _AdjustableClock:
    """One constructor-held clock whose authority can advance explicitly."""

    def __init__(self, value: float) -> None:
        self._value = value

    def __call__(self) -> float:
        result = self._value
        self._value += 1.0
        return result

    def advance_to(self, value: float) -> None:
        if value <= self._value:
            raise ValueError("test clock may only advance")
        self._value = value


@dataclass
class _NativeFiveRoleStack:
    root: Path
    sources: Authorities
    source_archive: AcquisitionSourceRetentionArchive
    retention: AcquisitionSourceRetentionArchiveV2
    coordinator: AcquisitionSourceReclamationApplyCoordinator
    registry: AcquisitionConsumerRegistryAuthority
    registry_verifier: AcquisitionConsumerRegistryBoundEpochVerifier
    participants: tuple[SourceReclamationParticipantV2, ...]
    seal: AcquisitionSourceArchiveSealResultV2
    request: object
    closure: object
    evidence_clock: _AdjustableClock

    @property
    def candidate(self):
        return self.sources.candidates[0]


def _issue_closure(
    stack: _NativeFiveRoleStack,
    bound: AcquisitionConsumerRegistryBoundEpochVerifier,
):
    candidate = stack.candidate
    downstream = stack.sources.evidence.load_terminal_consumption(
        candidate.evidence_stream_id,
        candidate.evidence_stream_sequence,
    )
    identity = stack.sources.evidence.terminal_event_identity(
        candidate.evidence_stream_id,
        candidate.evidence_stream_sequence,
    )
    lookup = stack.source_archive.lookup(candidate.candidate_sha256)
    assert downstream is not None and downstream.status == "archived"
    assert identity is not None and identity.status == "archived"
    assert lookup.record is not None and lookup.segment_sha256 is not None
    return bound.issue_consumer_closure(
        workload_id=WORKLOAD_ID,
        candidate_sha256=candidate.candidate_sha256,
        acquisition_job_id=candidate.acquisition_job_id,
        producer_receipt=downstream.producer_receipt,
        consumption_receipt=downstream.consumption_receipt,
        terminal_identity=identity,
        source_archive_anchor_sha256=stack.source_archive.audit()[
            "archive_anchor_sha256"
        ],
        source_archive_segment_sha256=lookup.segment_sha256,
        source_archive_record_sha256=lookup.record["record_sha256"],
    )


def _native_participants(
    *,
    sources: Authorities,
    retention: AcquisitionSourceRetentionArchiveV2,
    coordinator: AcquisitionSourceReclamationApplyCoordinator,
) -> tuple[SourceReclamationParticipantV2, ...]:
    decisions = coordinator.decision_authority
    participants: tuple[SourceReclamationParticipantV2, ...] = (
        sources.evidence.reclamation_participant_v2(
            candidate_authority=retention,
            committed_decision_authority=decisions,
        ),
        ExecutionReceiptReclamationParticipantV2(
            sources.executions,
            candidate_authority=retention,
            committed_decision_authority=decisions,
            clock=_Clock(NOW + 1_200.0),
        ),
        SchedulerReclamationParticipantV2(
            sources.scheduler,
            candidate_authority=retention,
            committed_decision_authority=decisions,
            clock=_Clock(NOW + 1_300.0),
        ),
        sources.raw,
        CapacityLedgerReclamationParticipantV2(
            sources.capacity,
            candidate_authority=retention,
            committed_decision_authority=decisions,
            clock=_Clock(NOW + 1_400.0),
        ),
    )
    assert tuple(item.role for item in participants) == PARTICIPANT_APPLY_ORDER
    assert len({item.source_store_identity_sha256 for item in participants}) == 5
    return participants


@contextmanager
def _native_five_role_stack(
    tmp_path: Path,
) -> Iterator[_NativeFiveRoleStack]:
    evidence_clock = _AdjustableClock(NOW + 2.0)
    sources, _bootstrap_archive, _bootstrap_plan, candidate = (
        _exact_route_context(
            tmp_path / "fixture",
            raw_store_kwargs={
                "reclamation_directory_fsync": SYNC,
                "reclamation_clock": _Clock(NOW + 1_100.0),
            },
            evidence_store_kwargs={
                "clock": evidence_clock,
                "directory_fsync": SYNC,
            },
        )
    )
    rotated = sources.evidence.rotate_archive(
        policy=EvidenceRetentionPolicy(max_segment_completions=1),
        now=NOW + 120.0,
    )
    assert rotated.archived_completions == 1
    evidence_clock.advance_to(NOW + 1_000.0)

    source_archive = _archive(tmp_path / "closure-source", sources)
    source_plan = source_archive.plan((candidate,), now=NOW + 200.0)
    assert source_plan.eligible_count == 1
    assert source_archive.export(source_plan).archived_jobs == 1

    registry, bound = _bound_registry(tmp_path / "registry-authority")
    try:
        downstream = sources.evidence.load_terminal_consumption(
            candidate.evidence_stream_id,
            candidate.evidence_stream_sequence,
        )
        identity = sources.evidence.terminal_event_identity(
            candidate.evidence_stream_id,
            candidate.evidence_stream_sequence,
        )
        lookup = source_archive.lookup(candidate.candidate_sha256)
        assert downstream is not None and downstream.status == "archived"
        assert identity is not None and identity.status == "archived"
        assert lookup.record is not None and lookup.segment_sha256 is not None
        closure = bound.issue_consumer_closure(
            workload_id=WORKLOAD_ID,
            candidate_sha256=candidate.candidate_sha256,
            acquisition_job_id=candidate.acquisition_job_id,
            producer_receipt=downstream.producer_receipt,
            consumption_receipt=downstream.consumption_receipt,
            terminal_identity=identity,
            source_archive_anchor_sha256=source_archive.audit()[
                "archive_anchor_sha256"
            ],
            source_archive_segment_sha256=lookup.segment_sha256,
            source_archive_record_sha256=lookup.record["record_sha256"],
        )
        retention = AcquisitionSourceRetentionArchiveV2(
            (tmp_path / "retention-v2").resolve(),
            source_archive=source_archive,
            consumer_registry_authority=registry,
            raw_store=sources.raw,
            scheduler=sources.scheduler,
            execution_store=sources.executions,
            capacity_ledger=sources.capacity,
            evidence_store=sources.evidence,
            limits=AcquisitionSourceRetentionLimitsV2(
                min_terminal_age_seconds=10.0,
            ),
            clock=_Clock(NOW + 1_500.0),
            directory_fsync=SYNC,
        )
        coordinator_directory = (tmp_path / "coordinator").resolve()
        coordinator_directory.mkdir(parents=True, exist_ok=False)
        coordinator = AcquisitionSourceReclamationApplyCoordinator.create(
            coordinator_directory / "reclamation.sqlite3",
            candidate_authority=retention,
            clock=_Clock(NOW + 1_600.0),
            directory_fsync=SYNC,
        )
        sources.raw.bind_reclamation_authorities_v1(
            retention,
            coordinator.decision_authority,
        )
        sources.raw.initialize_reclamation_extension_v1()
        participants = _native_participants(
            sources=sources,
            retention=retention,
            coordinator=coordinator,
        )
        retention.bind_source_participants_v2(participants)
        seal = retention.seal_candidates_v2(
            (candidate,),
            consumer_closures=(closure,),
            now=NOW + 1_700.0,
        )
        request = retention.create_reclamation_request_v2(
            seal.candidate_bindings_sha256,
            source_binding_sha256=_sha("native-five-role-source-binding-1"),
            source_view_sha256=_sha("native-five-role-source-view-1"),
            created_at=NOW + 1_800.0,
        )
        yield _NativeFiveRoleStack(
            root=tmp_path,
            sources=sources,
            source_archive=source_archive,
            retention=retention,
            coordinator=coordinator,
            registry=registry,
            registry_verifier=bound,
            participants=participants,
            seal=seal,
            request=request,
            closure=closure,
            evidence_clock=evidence_clock,
        )
    finally:
        registry.close()


def _load_participant_receipts(stack: _NativeFiveRoleStack, request):
    prepares = tuple(
        item.load_source_local_prepare_v2(request.request_sha256)
        for item in stack.participants
    )
    applies = tuple(
        item.load_source_local_apply_v1(request.request_sha256)
        for item in stack.participants
    )
    assert all(item is not None for item in prepares)
    assert all(item is not None for item in applies)
    assert tuple(item.role for item in prepares if item is not None) == (
        PARTICIPANT_APPLY_ORDER
    )
    assert tuple(item.role for item in applies if item is not None) == (
        PARTICIPANT_APPLY_ORDER
    )
    return prepares, applies


def _assert_physical_source_effects(
    stack: _NativeFiveRoleStack,
    *,
    capture,
    archive_files: tuple[Path, ...],
) -> None:
    candidate = stack.candidate
    assert all(not path.exists() for path in archive_files)
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
    for member in capture.members:
        assert stack.sources.raw.load_receipt(member.receipt_id) is None
    assert stack.sources.raw.stats()["object_count"] == 0
    assert stack.sources.raw.stats()["receipt_count"] == 0
    assert stack.sources.raw.stats()["capture_count"] == 0
    assert all(not path.exists() for path in stack.sources.object_paths)
    assert stack.sources.capacity.load_for_job(
        provider="dwd",
        job_identity=candidate.acquisition_job_id,
    ) is None
    assert stack.sources.capacity.audit()["reservation_rows"] == 0
    assert stack.sources.capacity.audit()["committed_count"] == 0

    lifecycle_audit = stack.sources.evidence.audit_archive()
    lifecycle_diagnostics = stack.sources.evidence.diagnostics()
    raw_audit = stack.sources.raw.integrity_check()
    retention_audit = stack.retention.audit_v2()
    assert lifecycle_audit["healthy"] is True
    assert lifecycle_diagnostics["reclamation"]["states"][APPLIED] == 1
    assert raw_audit["healthy"] is True
    assert raw_audit["reclamation"]["checked_applies"] == 1
    assert retention_audit["healthy"] is True


def _assert_replay_and_reopen(
    stack: _NativeFiveRoleStack,
    request,
) -> None:
    _prepares, applies = _load_participant_receipts(stack, request)
    predecessor = None
    for participant, durable in zip(
        stack.participants,
        applies,
        strict=True,
    ):
        assert durable is not None
        assert participant.apply_source_local_v1(
            request.request_sha256,
            predecessor_apply_receipt_sha256=predecessor,
        ) == durable
        predecessor = durable.apply_receipt_sha256

    audit_before_replay = stack.coordinator.audit_v2()
    assert stack.coordinator.apply_v1(
        request.request_sha256,
        participants=stack.participants,
    ).state == APPLIED
    assert stack.coordinator.audit_v2() == audit_before_replay

    reopened = AcquisitionSourceReclamationApplyCoordinator(
        stack.coordinator.path,
        expected_store_identity_sha256=stack.coordinator.store_identity_sha256,
        candidate_authority=stack.retention,
        clock=_Clock(NOW + 3_000.0),
        directory_fsync=SYNC,
    )
    replay_raw = RawArtifactStore(
        stack.sources.raw.root,
        limits=stack.sources.raw.limits,
        candidate_authority=stack.retention,
        committed_decision_authority=reopened.decision_authority,
        reclamation_directory_fsync=SYNC,
        reclamation_clock=_Clock(NOW + 3_100.0),
    )
    replay_raw.initialize()
    reopened_participants: tuple[SourceReclamationParticipantV2, ...] = (
        stack.sources.evidence.reclamation_participant_v2(
            candidate_authority=stack.retention,
            committed_decision_authority=reopened.decision_authority,
        ),
        ExecutionReceiptReclamationParticipantV2(
            stack.sources.executions,
            candidate_authority=stack.retention,
            committed_decision_authority=reopened.decision_authority,
            clock=_Clock(NOW + 3_200.0),
        ),
        SchedulerReclamationParticipantV2(
            stack.sources.scheduler,
            candidate_authority=stack.retention,
            committed_decision_authority=reopened.decision_authority,
            clock=_Clock(NOW + 3_300.0),
        ),
        replay_raw,
        CapacityLedgerReclamationParticipantV2(
            stack.sources.capacity,
            candidate_authority=stack.retention,
            committed_decision_authority=reopened.decision_authority,
            clock=_Clock(NOW + 3_400.0),
        ),
    )
    assert tuple(item.role for item in reopened_participants) == (
        PARTICIPANT_APPLY_ORDER
    )
    predecessor = None
    for participant, expected in zip(
        reopened_participants,
        applies,
        strict=True,
    ):
        assert expected is not None
        assert participant.load_source_local_apply_v1(
            request.request_sha256
        ) == expected
        assert participant.apply_source_local_v1(
            request.request_sha256,
            predecessor_apply_receipt_sha256=predecessor,
        ) == expected
        predecessor = expected.apply_receipt_sha256
    assert reopened.apply_v1(
        request.request_sha256,
        participants=reopened_participants,
    ).state == APPLIED
    assert reopened.audit_v2() == audit_before_replay


def test_native_five_role_prepare_commit_apply_replay_and_reopen(tmp_path):
    with _native_five_role_stack(tmp_path) as stack:
        request = stack.request
        capture = stack.sources.raw.load_capture(stack.candidate.raw_capture_id)
        assert capture is not None
        archive_files = tuple(
            stack.sources.evidence.archive_segment_directory.iterdir()
        ) + tuple(stack.sources.evidence.archive_object_directory.iterdir())
        assert archive_files
        with stack.retention.hold_candidate_page_v1(
            request,
            after_cursor=0,
            max_candidates=64,
        ) as historical_before:
            assert historical_before.exhausted is True

        prepared = stack.coordinator.prepare_v2(
            request,
            participants=stack.participants,
        )
        assert prepared.state == PREPARED
        assert prepared.prepared_roles == PARTICIPANT_APPLY_ORDER
        committed = stack.coordinator.commit_v1(
            request.request_sha256,
            participants=stack.participants,
        )
        assert committed.request_sha256 == request.request_sha256
        assert stack.coordinator.load_state_v2(request.request_sha256).state == (
            COMMITTED
        )
        applied = stack.coordinator.apply_v1(
            request.request_sha256,
            participants=stack.participants,
        )
        assert applied.state == APPLIED
        assert applied.applied_roles == PARTICIPANT_APPLY_ORDER
        coordinator_audit = stack.coordinator.audit_v2()
        assert coordinator_audit["states"][APPLIED] == 1
        assert coordinator_audit["event_count"] == 4

        _assert_physical_source_effects(
            stack,
            capture=capture,
            archive_files=archive_files,
        )
        with stack.retention.hold_candidate_page_v1(
            request,
            after_cursor=0,
            max_candidates=64,
        ) as historical_after:
            assert historical_after == historical_before
        _assert_replay_and_reopen(stack, request)


def test_prepared_epoch_advance_refuses_commit_aborts_then_successor_applies(
    tmp_path,
):
    with _native_five_role_stack(tmp_path) as stack:
        first_request = stack.request
        capture = stack.sources.raw.load_capture(stack.candidate.raw_capture_id)
        assert capture is not None
        archive_files = tuple(
            stack.sources.evidence.archive_segment_directory.iterdir()
        ) + tuple(stack.sources.evidence.archive_object_directory.iterdir())
        assert archive_files
        first_prepared = stack.coordinator.prepare_v2(
            first_request,
            participants=stack.participants,
        )
        assert first_prepared.state == PREPARED

        catalog, reproof = _archive_authorities("native-five-role-epoch-two")
        second_epoch = stack.registry.ensure_epoch(
            source_catalog=catalog,
            source_reproof_authority=reproof,
        )
        second_bound = stack.registry.verifier(
            source_catalog=catalog,
            source_reproof_authority=reproof,
        ).bind_current_epoch(
            claimed_epoch_root_sha256=second_epoch.epoch_root_sha256,
        )
        with pytest.raises(AcquisitionSourceRetentionConflict, match="stale"):
            stack.coordinator.commit_v1(
                first_request.request_sha256,
                participants=stack.participants,
            )
        assert stack.coordinator.load_state_v2(
            first_request.request_sha256
        ).state == PREPARED

        aborted = stack.coordinator.abort_v1(
            first_request.request_sha256,
            participants=stack.participants,
        )
        assert aborted.state == ABORTED
        assert set(aborted.aborted_roles) == set(PARTICIPANT_APPLY_ORDER)
        assert all(
            participant.load_source_local_abort_v1(
                first_request.request_sha256
            ) is not None
            for participant in stack.participants
        )
        assert stack.sources.raw.load_capture(stack.candidate.raw_capture_id) == (
            capture
        )
        assert stack.sources.executions.load_job_receipts(
            stack.candidate.acquisition_job_id
        )
        assert stack.sources.scheduler.get_record(
            stack.candidate.scheduler_job_id
        ) is not None
        assert stack.sources.capacity.load_for_job(
            provider="dwd",
            job_identity=stack.candidate.acquisition_job_id,
        ) is not None

        second_closure = _issue_closure(stack, second_bound)
        successor = stack.retention.seal_candidates_v2(
            (stack.candidate,),
            consumer_closures=(second_closure,),
            now=NOW + 2_200.0,
        )
        assert successor.candidate_bindings_sha256 != (
            stack.seal.candidate_bindings_sha256
        )
        second_request = stack.retention.create_reclamation_request_v2(
            successor.candidate_bindings_sha256,
            source_binding_sha256=_sha("native-five-role-source-binding-2"),
            source_view_sha256=_sha("native-five-role-source-view-2"),
            created_at=NOW + 2_300.0,
        )
        second_prepared = stack.coordinator.prepare_v2(
            second_request,
            participants=stack.participants,
        )
        assert second_prepared.state == PREPARED
        assert second_prepared.owner_epoch == first_prepared.owner_epoch + 1
        stack.coordinator.commit_v1(
            second_request.request_sha256,
            participants=stack.participants,
        )
        successor_applied = stack.coordinator.apply_v1(
            second_request.request_sha256,
            participants=stack.participants,
        )
        assert successor_applied.state == APPLIED
        assert successor_applied.applied_roles == PARTICIPANT_APPLY_ORDER
        coordinator_audit = stack.coordinator.audit_v2()
        assert coordinator_audit["states"][ABORTED] == 1
        assert coordinator_audit["states"][APPLIED] == 1
        assert coordinator_audit["active_requests"] == 0

        _assert_physical_source_effects(
            stack,
            capture=capture,
            archive_files=archive_files,
        )
        with stack.retention.hold_candidate_page_v1(
            first_request,
            after_cursor=0,
            max_candidates=64,
        ) as historical_first:
            assert historical_first.exhausted is True
        with stack.retention.hold_candidate_page_v1(
            second_request,
            after_cursor=0,
            max_candidates=64,
        ) as historical_successor:
            assert historical_successor.exhausted is True
        _assert_replay_and_reopen(stack, second_request)
