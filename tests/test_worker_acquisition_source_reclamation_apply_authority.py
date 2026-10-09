from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
import json
import os
import sqlite3
import threading

import pytest

from storm_tracker_worker.acquisition_source_reclamation_apply_authority import (
    ABORTED,
    ABORTING,
    APPLIED,
    COMMITTED,
    PREPARED,
    AcquisitionSourceReclamationApplyConflict,
    AcquisitionSourceReclamationApplyCapacityError,
    AcquisitionSourceReclamationApplyCoordinator,
    AcquisitionSourceReclamationApplyIntegrityError,
    AcquisitionSourceReclamationApplyClockError,
    AcquisitionSourceReclamationCoordinatorLimits,
    SourceReclamationCandidateCatalogLimits,
    SqliteSourceReclamationCandidateAuthority,
)
from storm_tracker_worker.acquisition_source_reclamation_protocol import (
    AcquisitionSourceReclamationProtocolIntegrityError,
    CANDIDATE_REFERENCE_RECORD_SCHEMA,
    PARTICIPANT_APPLY_ORDER,
    SourceLocalAbortReceiptV1,
    SourceLocalApplyReceiptV1,
    SourceLocalPrepareReceiptV2,
    SourceReclamationCandidateAuthority,
    SourceReclamationCandidateBindingV1,
    SourceReclamationRequestV2,
    archive_binding_root,
    candidate_binding_root,
    canonical_bytes,
    digest_mapping,
)


def _sha(value: str) -> str:
    return value * 64


class _Clock:
    def __init__(self, value: float = 1_000.0) -> None:
        self.value = value
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            result = self.value
            self.value += 1.0
            return result


def _reference(
    binding: SourceReclamationCandidateBindingV1,
    *,
    raw_bytes: int = 105_906_176,
) -> bytes:
    return canonical_bytes({
        "archive_object_count": 3,
        "archive_object_reference_root_sha256": _sha("a"),
        "archive_object_total_bytes": raw_bytes,
        "capacity_ledger_view_sha256": _sha("b"),
        "candidate_sha256": binding.candidate_sha256,
        "downstream_lifecycle_view_sha256": _sha("c"),
        "execution_receipts_view_sha256": _sha("d"),
        "metadata_sha256": _sha("e"),
        "raw_artifacts_view_sha256": _sha("f"),
        "record_sha256": binding.sealed_archive_record_sha256,
        "scheduler_view_sha256": _sha("1"),
        "schema": CANDIDATE_REFERENCE_RECORD_SCHEMA,
        "sealed_archive_segment_sha256": (
            binding.sealed_archive_segment_sha256
        ),
    })


def _binding(index: int = 1) -> SourceReclamationCandidateBindingV1:
    candidate = f"{index:064x}"
    return SourceReclamationCandidateBindingV1.create(
        candidate_sha256=candidate,
        acquisition_job_id=f"acquisition-{index}",
        scheduler_job_id=f"scheduler-{index}",
        raw_capture_id=f"capture-{index}",
        evidence_stream_id="reclamation-test-stream",
        evidence_stream_sequence=index,
        sealed_archive_segment_sha256=hashlib_sha(f"segment-{index}"),
        sealed_archive_record_sha256=hashlib_sha(f"record-{index}"),
        consumer_closure_receipt_sha256=hashlib_sha(f"closure-{index}"),
        consumer_closure_root_sha256=hashlib_sha(f"closure-root-{index}"),
    )


def hashlib_sha(value: str) -> str:
    import hashlib

    return hashlib.sha256(value.encode("ascii")).hexdigest()


@dataclass
class _Harness:
    candidate: SqliteSourceReclamationCandidateAuthority
    coordinator: AcquisitionSourceReclamationApplyCoordinator
    request: SourceReclamationRequestV2
    bindings: tuple[SourceReclamationCandidateBindingV1, ...]
    payloads: tuple[bytes, ...]
    clock: _Clock


def _harness(tmp_path: Path, *, count: int = 1) -> _Harness:
    clock = _Clock()
    candidate = SqliteSourceReclamationCandidateAuthority.create(
        tmp_path / "candidate.sqlite3",
        source_binding_sha256=_sha("2"),
        archive_store_identity_sha256=_sha("3"),
        clock=clock,
        directory_fsync=lambda _path: None,
    )
    bindings = tuple(_binding(index) for index in range(1, count + 1))
    payloads = tuple(_reference(item) for item in bindings)
    request = SourceReclamationRequestV2.create(
        source_binding_sha256=candidate.source_binding_sha256,
        source_view_sha256=_sha("4"),
        sealed_archive_anchor_sha256=_sha("5"),
        archive_binding_root_sha256=archive_binding_root(bindings, payloads),
        candidate_bindings_sha256=candidate_binding_root(bindings),
        candidate_authority_sha256=candidate.authority_sha256,
        consumer_registry_root_sha256=_sha("6"),
        consumer_epoch_root_sha256=_sha("7"),
        candidate_count=count,
        created_at=900.0,
    )
    candidate.seal_request_v2(
        request,
        bindings=bindings,
        record_payloads=payloads,
    )
    coordinator = AcquisitionSourceReclamationApplyCoordinator.create(
        tmp_path / "coordinator.sqlite3",
        candidate_authority=candidate,
        clock=clock,
        directory_fsync=lambda _path: None,
    )
    return _Harness(candidate, coordinator, request, bindings, payloads, clock)


class _ProtocolCandidateAuthority(SourceReclamationCandidateAuthority):
    """Protocol-only wrapper mirroring retention-v2's authority surface."""

    def __init__(self, inner: SqliteSourceReclamationCandidateAuthority) -> None:
        self._inner = inner
        self.path = inner.path
        self.page_calls: list[tuple[int, int]] = []
        self.held_page_count = 0
        self.page_exit_count = 0
        super().__init__(inner.authority_sha256)

    @contextmanager
    def hold_candidate_page_v1(
        self,
        request: SourceReclamationRequestV2,
        *,
        after_cursor: int,
        max_candidates: int,
    ):
        self.page_calls.append((after_cursor, max_candidates))
        try:
            with self._inner.hold_candidate_page_v1(
                request,
                after_cursor=after_cursor,
                max_candidates=max_candidates,
            ) as page:
                self.held_page_count += 1
                try:
                    yield page
                finally:
                    self.held_page_count -= 1
        finally:
            self.page_exit_count += 1


class _TrackingCurrentCandidateAuthority(_ProtocolCandidateAuthority):
    def __init__(
        self,
        inner: SqliteSourceReclamationCandidateAuthority,
        request: SourceReclamationRequestV2,
    ) -> None:
        super().__init__(inner)
        self.current_request_sha256 = request.request_sha256
        self.current_hold_entries = 0
        self.current_hold_exits = 0
        self.current_hold_depth = 0
        self.reject_current_holds = False

    def mark_current_request_stale(self) -> None:
        self.current_request_sha256 = _sha("9")

    @contextmanager
    def hold_current_request_authority_v1(
        self,
        request: SourceReclamationRequestV2,
    ):
        self.current_hold_entries += 1
        if (
            self.reject_current_holds
            or self.current_request_sha256 != request.request_sha256
        ):
            raise AcquisitionSourceReclamationApplyConflict(
                "request is no longer the current candidate authority"
            )
        held_request_sha256 = self.current_request_sha256
        self.current_hold_depth += 1
        try:
            with super().hold_current_request_authority_v1(request) as page:
                yield page
        finally:
            self.current_hold_depth -= 1
            self.current_hold_exits += 1
        if self.current_request_sha256 != held_request_sha256:
            raise AcquisitionSourceReclamationApplyConflict(
                "current candidate authority changed while held"
            )


def test_coordinator_accepts_frozen_protocol_candidate_authority(
    tmp_path: Path,
) -> None:
    root = tmp_path / "protocol-candidate"
    root.mkdir()
    harness = _harness(root)
    candidate = _ProtocolCandidateAuthority(harness.candidate)
    coordinator = AcquisitionSourceReclamationApplyCoordinator.create(
        root / "protocol-coordinator.sqlite3",
        candidate_authority=candidate,
        clock=harness.clock,
        directory_fsync=lambda _path: None,
    )

    state = coordinator.begin_preparing_v2(harness.request)

    assert state.state == "PREPARING"
    assert coordinator.candidate_authority_sha256 == candidate.authority_sha256


def test_protocol_default_current_authority_holds_exact_first_page(
    tmp_path: Path,
) -> None:
    harness = _harness(tmp_path, count=2)
    candidate = _ProtocolCandidateAuthority(harness.candidate)

    with candidate.hold_current_request_authority_v1(harness.request) as page:
        assert candidate.held_page_count == 1
        assert page.after_cursor == 0
        assert page.next_cursor == 1
        assert page.total_candidate_count == 2
        assert page.exhausted is False

    assert candidate.page_calls == [(0, 1)]
    assert candidate.held_page_count == 0
    assert candidate.page_exit_count == 1


def test_coordinator_refuses_a_protocol_authority_owned_root(
    tmp_path: Path,
) -> None:
    harness_root = tmp_path / "catalogue"
    harness_root.mkdir()
    harness = _harness(harness_root)
    candidate = _ProtocolCandidateAuthority(harness.candidate)
    candidate.root = (tmp_path / "retention-root").resolve()
    candidate.root.mkdir()

    with pytest.raises(
        AcquisitionSourceReclamationApplyIntegrityError,
        match="paths overlap",
    ):
        AcquisitionSourceReclamationApplyCoordinator.create(
            candidate.root / "coordinator.sqlite3",
            candidate_authority=candidate,
            clock=harness.clock,
            directory_fsync=lambda _path: None,
        )


_DEFAULT_APPLY_LOAD = object()


class _FakeParticipant:
    def __init__(
        self,
        harness: _Harness,
        role: str,
        ordinal: int,
    ) -> None:
        self.role = role
        self.source_store_identity_sha256 = f"{ordinal + 8:064x}"
        self.coordinator_authority_sha256 = (
            harness.coordinator.authority_sha256
        )
        self.candidate_authority_sha256 = harness.candidate.authority_sha256
        self._candidates = harness.candidate
        self._decision = harness.coordinator.decision_authority
        self._clock = harness.clock
        self._prepare: SourceLocalPrepareReceiptV2 | None = None
        self._apply: SourceLocalApplyReceiptV1 | None = None
        self._abort: SourceLocalAbortReceiptV1 | None = None
        self.prepare_calls = 0
        self.prepare_load_calls = 0
        self.apply_calls = 0
        self.apply_load_calls = 0
        self.abort_calls = 0
        self.prepare_observer: Callable[[str], None] | None = None
        self.prepare_load_observer: Callable[[str], None] | None = None
        self.apply_load_observer: Callable[[str], None] | None = None
        self.apply_load_override: object = _DEFAULT_APPLY_LOAD

    def rebind(self, coordinator: AcquisitionSourceReclamationApplyCoordinator) -> None:
        assert coordinator.authority_sha256 == self.coordinator_authority_sha256
        self._decision = coordinator.decision_authority

    def prepare_source_local_v2(
        self,
        request: SourceReclamationRequestV2,
    ) -> SourceLocalPrepareReceiptV2:
        self.prepare_calls += 1
        if self.prepare_observer is not None:
            self.prepare_observer(self.role)
        with self._candidates.hold_candidate_page_v1(
            request,
            after_cursor=0,
            max_candidates=min(64, request.candidate_count),
        ) as page:
            record = page.decoded_records()[0]
            archived_role_view = record[f"{self.role}_view_sha256"]
        receipt = SourceLocalPrepareReceiptV2.create(
            request_sha256=request.request_sha256,
            role=self.role,
            source_store_identity_sha256=self.source_store_identity_sha256,
            coordinator_authority_sha256=self.coordinator_authority_sha256,
            candidate_authority_sha256=self.candidate_authority_sha256,
            sealed_archive_anchor_sha256=request.sealed_archive_anchor_sha256,
            archive_binding_root_sha256=request.archive_binding_root_sha256,
            candidate_bindings_sha256=request.candidate_bindings_sha256,
            consumer_registry_root_sha256=request.consumer_registry_root_sha256,
            consumer_epoch_root_sha256=request.consumer_epoch_root_sha256,
            source_view_sha256=archived_role_view,
            child_reference_root_sha256=digest_mapping({
                "request_sha256": request.request_sha256,
                "role": self.role,
            }),
            source_fence_sha256=digest_mapping({
                "request_sha256": request.request_sha256,
                "role": self.role,
                "source_store_identity_sha256": self.source_store_identity_sha256,
            }),
            candidate_count=request.candidate_count,
            releasable_bytes=100,
            releasable_entries=request.candidate_count,
            prepared_at=self._clock(),
        )
        if self._prepare is not None:
            return self._prepare
        self._prepare = receipt
        return receipt

    def load_source_local_prepare_v2(
        self,
        request_sha256: str,
    ) -> SourceLocalPrepareReceiptV2 | None:
        self.prepare_load_calls += 1
        if self.prepare_load_observer is not None:
            self.prepare_load_observer(self.role)
        if self._prepare is not None:
            assert self._prepare.request_sha256 == request_sha256
        return self._prepare

    def apply_source_local_v1(
        self,
        request_sha256: str,
        *,
        predecessor_apply_receipt_sha256: str | None,
    ) -> SourceLocalApplyReceiptV1:
        self.apply_calls += 1
        assert self._prepare is not None
        if self._apply is not None:
            return self._apply
        with self._decision.hold_committed_decision_v1(
            request_sha256,
            applying_role=self.role,
            expected_predecessor_apply_receipt_sha256=(
                predecessor_apply_receipt_sha256
            ),
        ) as decision:
            receipt = SourceLocalApplyReceiptV1.create(
                request_sha256=request_sha256,
                role=self.role,
                source_store_identity_sha256=self.source_store_identity_sha256,
                prepare_receipt_sha256=self._prepare.prepare_receipt_sha256,
                committed_decision_sha256=decision.decision_sha256,
                committed_decision_receipt_sha256=decision.receipt_sha256,
                predecessor_apply_receipt_sha256=(
                    predecessor_apply_receipt_sha256
                ),
                effect_sha256=digest_mapping({
                    "request_sha256": request_sha256,
                    "role": self.role,
                    "effect": "source-local-apply",
                }),
                post_apply_view_sha256=digest_mapping({
                    "request_sha256": request_sha256,
                    "role": self.role,
                    "view": "released",
                }),
                released_bytes=100,
                released_entries=1,
                applied_at=self._clock(),
            )
            self._apply = receipt
        return receipt

    def load_source_local_apply_v1(
        self,
        request_sha256: str,
    ) -> SourceLocalApplyReceiptV1 | None:
        self.apply_load_calls += 1
        if self.apply_load_observer is not None:
            self.apply_load_observer(self.role)
        if self.apply_load_override is not _DEFAULT_APPLY_LOAD:
            return self.apply_load_override  # type: ignore[return-value]
        if self._apply is not None:
            assert self._apply.request_sha256 == request_sha256
        return self._apply

    def abort_source_local_v1(
        self,
        request_sha256: str,
        *,
        expected_abort_decision_sha256: str,
    ) -> SourceLocalAbortReceiptV1:
        self.abort_calls += 1
        assert self._prepare is not None
        if self._abort is not None:
            return self._abort
        with self._decision.hold_abort_decision_v1(
            request_sha256,
            expected_abort_decision_sha256=expected_abort_decision_sha256,
            aborting_role=self.role,
            expected_prepare_receipt_sha256=self._prepare.prepare_receipt_sha256,
            expected_source_store_identity_sha256=(
                self.source_store_identity_sha256
            ),
            expected_source_fence_sha256=self._prepare.source_fence_sha256,
        ):
            receipt = SourceLocalAbortReceiptV1.create(
                request_sha256=request_sha256,
                role=self.role,
                source_store_identity_sha256=self.source_store_identity_sha256,
                prepare_receipt_sha256=self._prepare.prepare_receipt_sha256,
                coordinator_abort_decision_sha256=(
                    expected_abort_decision_sha256
                ),
                fence_release_root_sha256=digest_mapping({
                    "request_sha256": request_sha256,
                    "role": self.role,
                    "released": True,
                }),
                aborted_at=self._clock(),
            )
            self._abort = receipt
        return receipt

    def load_source_local_abort_v1(
        self,
        request_sha256: str,
    ) -> SourceLocalAbortReceiptV1 | None:
        if self._abort is not None:
            assert self._abort.request_sha256 == request_sha256
        return self._abort


def _participants(harness: _Harness) -> tuple[_FakeParticipant, ...]:
    return tuple(
        _FakeParticipant(harness, role, ordinal)
        for ordinal, role in enumerate(PARTICIPANT_APPLY_ORDER)
    )


def _tracking_harness(
    tmp_path: Path,
    *,
    count: int = 1,
) -> tuple[_Harness, _TrackingCurrentCandidateAuthority]:
    historical = _harness(tmp_path, count=count)
    candidate = _TrackingCurrentCandidateAuthority(
        historical.candidate,
        historical.request,
    )
    coordinator = AcquisitionSourceReclamationApplyCoordinator.create(
        tmp_path / "current-coordinator.sqlite3",
        candidate_authority=candidate,
        clock=historical.clock,
        directory_fsync=lambda _path: None,
    )
    harness = _Harness(
        candidate=candidate,  # type: ignore[arg-type]
        coordinator=coordinator,
        request=historical.request,
        bindings=historical.bindings,
        payloads=historical.payloads,
        clock=historical.clock,
    )
    return harness, candidate


def test_prepare_and_commit_hold_current_authority_in_exact_role_order(
    tmp_path: Path,
) -> None:
    harness, candidate = _tracking_harness(tmp_path)
    participants = _participants(harness)
    prepare_order: list[tuple[str, int]] = []
    prepare_reproof_order: list[tuple[str, int]] = []
    for participant in participants:
        participant.prepare_observer = lambda role: prepare_order.append(
            (role, candidate.current_hold_depth)
        )
        participant.prepare_load_observer = lambda role: (
            prepare_reproof_order.append((role, candidate.current_hold_depth))
        )

    prepared = harness.coordinator.prepare_v2(
        harness.request,
        participants=participants,
    )

    assert prepared.state == PREPARED
    assert prepare_order == [
        (role, 1) for role in PARTICIPANT_APPLY_ORDER
    ]
    assert [item.prepare_calls for item in participants] == [1] * 5
    assert candidate.current_hold_entries == 1
    assert candidate.current_hold_exits == 1
    assert candidate.current_hold_depth == 0

    committed = harness.coordinator.commit_v1(
        harness.request.request_sha256,
        participants=participants,
    )

    assert committed.state == COMMITTED
    assert prepare_reproof_order == [
        (role, 1) for role in PARTICIPANT_APPLY_ORDER
    ]
    assert [item.prepare_load_calls for item in participants] == [1] * 5
    assert candidate.current_hold_entries == 2
    assert candidate.current_hold_exits == 2
    assert candidate.current_hold_depth == 0


def test_prepare_refuses_current_authority_drift_before_prepared(
    tmp_path: Path,
) -> None:
    harness, candidate = _tracking_harness(tmp_path)
    participants = _participants(harness)
    candidate.mark_current_request_stale()

    with pytest.raises(
        AcquisitionSourceReclamationApplyConflict,
        match="no longer the current",
    ):
        harness.coordinator.prepare_v2(
            harness.request,
            participants=participants,
        )

    assert harness.coordinator.load_state_v2(harness.request.request_sha256) is None
    assert [item.prepare_calls for item in participants] == [0] * 5


def test_commit_refuses_current_authority_drift_after_prepare(
    tmp_path: Path,
) -> None:
    harness, candidate = _tracking_harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)
    candidate.mark_current_request_stale()

    with pytest.raises(
        AcquisitionSourceReclamationApplyConflict,
        match="no longer the current",
    ):
        harness.coordinator.commit_v1(
            harness.request.request_sha256,
            participants=participants,
        )

    state = harness.coordinator.load_state_v2(harness.request.request_sha256)
    assert state is not None and state.state == PREPARED
    assert [item.prepare_load_calls for item in participants] == [0] * 5


def test_current_authority_exit_reproof_blocks_commit_after_in_hold_drift(
    tmp_path: Path,
) -> None:
    harness, candidate = _tracking_harness(tmp_path)
    participants = _participants(harness)
    participants[2].prepare_observer = (
        lambda _role: candidate.mark_current_request_stale()
    )

    with pytest.raises(
        AcquisitionSourceReclamationApplyConflict,
        match="changed while held",
    ):
        harness.coordinator.prepare_v2(
            harness.request,
            participants=participants,
        )

    state = harness.coordinator.load_state_v2(harness.request.request_sha256)
    assert state is not None and state.state == PREPARED
    assert [item.prepare_calls for item in participants] == [1] * 5
    assert candidate.current_hold_entries == 1
    assert candidate.current_hold_exits == 1
    with pytest.raises(AcquisitionSourceReclamationApplyConflict):
        harness.coordinator.commit_v1(
            harness.request.request_sha256,
            participants=participants,
        )
    state = harness.coordinator.load_state_v2(harness.request.request_sha256)
    assert state is not None and state.state == PREPARED


def test_committed_crash_replay_does_not_require_live_current_authority(
    tmp_path: Path,
) -> None:
    harness, candidate = _tracking_harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)

    def fault(point: str) -> None:
        if point == "commit.after_commit":
            raise RuntimeError("commit.after_commit")

    faulty = AcquisitionSourceReclamationApplyCoordinator(
        harness.coordinator.path,
        expected_store_identity_sha256=(
            harness.coordinator.store_identity_sha256
        ),
        candidate_authority=candidate,
        clock=harness.clock,
        directory_fsync=lambda _path: None,
        fault_injector=fault,
    )
    for participant in participants:
        participant.rebind(faulty)
    with pytest.raises(RuntimeError, match=r"commit\.after_commit"):
        faulty.commit_v1(
            harness.request.request_sha256,
            participants=participants,
        )

    durable = faulty.load_state_v2(harness.request.request_sha256)
    assert durable is not None and durable.state == COMMITTED
    candidate.mark_current_request_stale()
    candidate.reject_current_holds = True
    holds_before_replay = candidate.current_hold_entries
    reopened = AcquisitionSourceReclamationApplyCoordinator(
        harness.coordinator.path,
        expected_store_identity_sha256=(
            harness.coordinator.store_identity_sha256
        ),
        candidate_authority=candidate,
        clock=harness.clock,
        directory_fsync=lambda _path: None,
    )
    for participant in participants:
        participant.rebind(reopened)

    committed = reopened.commit_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    prepared_replay = reopened.prepare_v2(
        harness.request,
        participants=participants,
    )
    applied = reopened.resume_v2(
        harness.request,
        participants=participants,
        allow_commit=True,
    )

    assert committed.state == COMMITTED
    assert prepared_replay.state == COMMITTED
    assert applied.state == APPLIED
    assert candidate.current_hold_entries == holds_before_replay
    assert [item.prepare_calls for item in participants] == [1] * 5
    assert [item.apply_calls for item in participants] == [1] * 5


def test_end_to_end_applied_and_idempotent_resume(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)

    prepared = harness.coordinator.prepare_v2(
        harness.request,
        participants=participants,
    )
    assert prepared.state == PREPARED
    decision = harness.coordinator.commit_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    assert decision.state == COMMITTED
    applied = harness.coordinator.apply_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    assert applied.state == APPLIED
    assert applied.applied_roles == PARTICIPANT_APPLY_ORDER
    assert harness.coordinator.audit_v2()["event_count"] == 4

    repeated = harness.coordinator.resume_v2(
        harness.request,
        participants=participants,
        allow_commit=True,
    )
    assert repeated == applied
    assert [item.apply_calls for item in participants] == [1] * 5


def test_reopened_applied_coordinator_replay_reproofs_all_participants(
    tmp_path: Path,
) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)
    harness.coordinator.commit_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    applied = harness.coordinator.apply_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    apply_calls = [item.apply_calls for item in participants]
    load_calls = [item.apply_load_calls for item in participants]
    load_order: list[str] = []
    for participant in participants:
        participant.apply_load_observer = load_order.append
    reopened = AcquisitionSourceReclamationApplyCoordinator(
        harness.coordinator.path,
        expected_store_identity_sha256=(
            harness.coordinator.store_identity_sha256
        ),
        candidate_authority=harness.candidate,
        clock=harness.clock,
        directory_fsync=lambda _path: None,
    )
    for participant in participants:
        participant.rebind(reopened)
    event_count = reopened.audit_v2()["event_count"]

    direct = reopened.apply_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    resumed = reopened.resume_v2(
        harness.request,
        participants=participants,
        allow_commit=True,
    )

    assert direct == applied
    assert resumed == applied
    assert load_order == list(PARTICIPANT_APPLY_ORDER) * 2
    assert [item.apply_calls for item in participants] == apply_calls
    assert [item.apply_load_calls for item in participants] == [
        count + 2 for count in load_calls
    ]
    assert reopened.audit_v2()["event_count"] == event_count


@pytest.mark.parametrize(
    ("failure_mode", "target_index"),
    (("missing", 1), ("corrupt", 2), ("forged", 3)),
)
def test_applied_coordinator_replay_rejects_invalid_participant_load(
    tmp_path: Path,
    failure_mode: str,
    target_index: int,
) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)
    harness.coordinator.commit_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    applied = harness.coordinator.apply_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    target = participants[target_index]
    durable = target._apply
    assert durable is not None
    if failure_mode == "missing":
        target.apply_load_override = None
    elif failure_mode == "corrupt":
        target.apply_load_override = {"corrupt": True}
    else:
        target.apply_load_override = SourceLocalApplyReceiptV1.create(
            request_sha256=durable.request_sha256,
            role=durable.role,
            source_store_identity_sha256=(
                durable.source_store_identity_sha256
            ),
            prepare_receipt_sha256=durable.prepare_receipt_sha256,
            committed_decision_sha256=durable.committed_decision_sha256,
            committed_decision_receipt_sha256=(
                durable.committed_decision_receipt_sha256
            ),
            predecessor_apply_receipt_sha256=(
                durable.predecessor_apply_receipt_sha256
            ),
            effect_sha256=_sha("f"),
            post_apply_view_sha256=durable.post_apply_view_sha256,
            released_bytes=durable.released_bytes,
            released_entries=durable.released_entries,
            applied_at=durable.applied_at,
        )
    apply_calls = [item.apply_calls for item in participants]
    load_calls = [item.apply_load_calls for item in participants]
    load_order: list[str] = []
    for participant in participants:
        participant.apply_load_observer = load_order.append
    event_count = harness.coordinator.audit_v2()["event_count"]

    with pytest.raises(
        AcquisitionSourceReclamationApplyConflict,
        match="participant APPLIED replay receipt differs",
    ):
        harness.coordinator.apply_v1(
            harness.request.request_sha256,
            participants=participants,
        )

    assert load_order == list(PARTICIPANT_APPLY_ORDER[: target_index + 1])
    assert [item.apply_calls for item in participants] == apply_calls
    assert [item.apply_load_calls for item in participants] == [
        count + (1 if index <= target_index else 0)
        for index, count in enumerate(load_calls)
    ]
    assert harness.coordinator.load_state_v2(
        harness.request.request_sha256
    ) == applied
    assert harness.coordinator.audit_v2()["event_count"] == event_count


def test_applied_decision_hold_replays_all_five_roles_without_new_event(
    tmp_path: Path,
) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)
    committed = harness.coordinator.commit_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    applied = harness.coordinator.apply_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    receipts = tuple(
        participant.load_source_local_apply_v1(
            harness.request.request_sha256
        )
        for participant in participants
    )
    assert all(receipt is not None for receipt in receipts)
    event_count = harness.coordinator.audit_v2()["event_count"]

    for index, role in enumerate(PARTICIPANT_APPLY_ORDER):
        predecessor = (
            None
            if index == 0
            else receipts[index - 1].apply_receipt_sha256  # type: ignore[union-attr]
        )
        with harness.coordinator.decision_authority.hold_committed_decision_v1(
            harness.request.request_sha256,
            applying_role=role,
            expected_predecessor_apply_receipt_sha256=predecessor,
        ) as replayed:
            assert replayed == committed

    assert harness.coordinator.load_state_v2(
        harness.request.request_sha256
    ) == applied
    assert harness.coordinator.audit_v2()["event_count"] == event_count


def test_applied_decision_hold_rejects_wrong_role_predecessor(
    tmp_path: Path,
) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)
    harness.coordinator.commit_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    harness.coordinator.apply_v1(
        harness.request.request_sha256,
        participants=participants,
    )

    with pytest.raises(
        AcquisitionSourceReclamationApplyConflict,
        match="APPLIED replay predecessor differs",
    ):
        with harness.coordinator.decision_authority.hold_committed_decision_v1(
            harness.request.request_sha256,
            applying_role="raw_artifacts",
            expected_predecessor_apply_receipt_sha256=_sha("9"),
        ):
            pytest.fail("wrong APPLIED predecessor was yielded")


def test_reopened_applied_decision_hold_reproofs_exact_terminal_bytes(
    tmp_path: Path,
) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)
    committed = harness.coordinator.commit_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    harness.coordinator.apply_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    raw_predecessor = participants[2].load_source_local_apply_v1(
        harness.request.request_sha256
    )
    assert raw_predecessor is not None
    fault_points: list[str] = []
    reopened = AcquisitionSourceReclamationApplyCoordinator(
        harness.coordinator.path,
        expected_store_identity_sha256=(
            harness.coordinator.store_identity_sha256
        ),
        candidate_authority=harness.candidate,
        clock=harness.clock,
        directory_fsync=lambda _path: None,
        fault_injector=fault_points.append,
    )
    event_count = reopened.audit_v2()["event_count"]

    with reopened.decision_authority.hold_committed_decision_v1(
        harness.request.request_sha256,
        applying_role="raw_artifacts",
        expected_predecessor_apply_receipt_sha256=(
            raw_predecessor.apply_receipt_sha256
        ),
    ) as replayed:
        assert replayed == committed

    assert fault_points == [
        "decision_hold.commit.before_yield",
        "decision_hold.commit.after_reproof",
    ]
    assert reopened.audit_v2()["event_count"] == event_count


def test_committed_hold_keeps_exact_prefix_and_record_is_not_applied_replay(
    tmp_path: Path,
) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)
    harness.coordinator.commit_v1(
        harness.request.request_sha256,
        participants=participants,
    )

    with harness.coordinator.decision_authority.hold_committed_decision_v1(
        harness.request.request_sha256,
        applying_role="downstream_lifecycle",
        expected_predecessor_apply_receipt_sha256=None,
    ):
        pass
    with pytest.raises(
        AcquisitionSourceReclamationApplyConflict,
        match="predecessor progress differs",
    ):
        with harness.coordinator.decision_authority.hold_committed_decision_v1(
            harness.request.request_sha256,
            applying_role="execution_receipts",
            expected_predecessor_apply_receipt_sha256=_sha("9"),
        ):
            pytest.fail("non-prefix COMMITTED role was yielded")

    first = participants[0].apply_source_local_v1(
        harness.request.request_sha256,
        predecessor_apply_receipt_sha256=None,
    )
    harness.coordinator.record_source_local_apply_v1(
        first,
        participant=participants[0],
    )
    with harness.coordinator.decision_authority.hold_committed_decision_v1(
        harness.request.request_sha256,
        applying_role="execution_receipts",
        expected_predecessor_apply_receipt_sha256=first.apply_receipt_sha256,
    ):
        pass
    with pytest.raises(
        AcquisitionSourceReclamationApplyConflict,
        match="predecessor progress differs",
    ):
        with harness.coordinator.decision_authority.hold_committed_decision_v1(
            harness.request.request_sha256,
            applying_role="downstream_lifecycle",
            expected_predecessor_apply_receipt_sha256=None,
        ):
            pytest.fail("already-recorded COMMITTED role was replayed")

    applied = harness.coordinator.apply_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    assert applied.state == APPLIED
    final_receipt = participants[-1].load_source_local_apply_v1(
        harness.request.request_sha256
    )
    assert final_receipt is not None
    with pytest.raises(
        AcquisitionSourceReclamationApplyConflict,
        match="no longer COMMITTED",
    ):
        harness.coordinator.record_source_local_apply_v1(
            final_receipt,
            participant=participants[-1],
        )


def test_decision_hold_rejects_aborting_and_aborted_states(
    tmp_path: Path,
) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)
    harness.coordinator.begin_aborting_v1(harness.request.request_sha256)

    for expected_state in (ABORTING, ABORTED):
        state = harness.coordinator.load_state_v2(
            harness.request.request_sha256
        )
        assert state is not None and state.state == expected_state
        with pytest.raises(
            AcquisitionSourceReclamationApplyConflict,
            match="neither COMMITTED nor APPLIED",
        ):
            with harness.coordinator.decision_authority.hold_committed_decision_v1(
                harness.request.request_sha256,
                applying_role="downstream_lifecycle",
                expected_predecessor_apply_receipt_sha256=None,
            ):
                pytest.fail(f"{expected_state} decision was yielded")
        if expected_state == ABORTING:
            harness.coordinator.abort_v1(
                harness.request.request_sha256,
                participants=participants,
            )


def test_partial_prepare_aborts_only_durable_fences(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.begin_preparing_v2(harness.request)
    for participant in participants[:2]:
        harness.coordinator.record_source_local_prepare_v2(
            participant.prepare_source_local_v2(harness.request)
        )

    aborted = harness.coordinator.abort_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    assert aborted.state == ABORTED
    assert aborted.aborted_roles == PARTICIPANT_APPLY_ORDER[:2]
    assert [item.abort_calls for item in participants] == [1, 1, 0, 0, 0]
    assert harness.coordinator.audit_v2()["event_count"] == 3


def test_101_mib_raw_is_a_small_metadata_reference(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    assert len(harness.payloads[0]) < 4096
    with harness.candidate.hold_candidate_page_v1(
        harness.request,
        after_cursor=0,
        max_candidates=1,
    ) as page:
        record = page.decoded_records()[0]
    assert record["archive_object_total_bytes"] == 105_906_176
    assert "payload_base64" not in record


def test_restart_from_committed_reuses_local_receipts(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)
    harness.coordinator.commit_v1(
        harness.request.request_sha256,
        participants=participants,
    )

    reopened = AcquisitionSourceReclamationApplyCoordinator(
        harness.coordinator.path,
        expected_store_identity_sha256=(
            harness.coordinator.store_identity_sha256
        ),
        candidate_authority=harness.candidate,
        clock=harness.clock,
        directory_fsync=lambda _path: None,
    )
    for participant in participants:
        participant.rebind(reopened)
    result = reopened.resume_v2(
        harness.request,
        participants=participants,
        allow_commit=True,
    )
    assert result.state == APPLIED
    assert [item.prepare_calls for item in participants] == [1] * 5
    assert [item.apply_calls for item in participants] == [1] * 5


@pytest.mark.parametrize("fault_point", ["commit.before_commit", "commit.after_commit"])
def test_commit_fault_is_restart_safe(tmp_path: Path, fault_point: str) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)

    def fault(point: str) -> None:
        if point == fault_point:
            raise RuntimeError(fault_point)

    faulty = AcquisitionSourceReclamationApplyCoordinator(
        harness.coordinator.path,
        expected_store_identity_sha256=(
            harness.coordinator.store_identity_sha256
        ),
        candidate_authority=harness.candidate,
        clock=harness.clock,
        directory_fsync=lambda _path: None,
        fault_injector=fault,
    )
    for participant in participants:
        participant.rebind(faulty)
    with pytest.raises(RuntimeError, match=fault_point.replace(".", r"\.")):
        faulty.commit_v1(
            harness.request.request_sha256,
            participants=participants,
        )

    recovered = AcquisitionSourceReclamationApplyCoordinator(
        harness.coordinator.path,
        expected_store_identity_sha256=(
            harness.coordinator.store_identity_sha256
        ),
        candidate_authority=harness.candidate,
        clock=harness.clock,
        directory_fsync=lambda _path: None,
    )
    for participant in participants:
        participant.rebind(recovered)
    state = recovered.load_state_v2(harness.request.request_sha256)
    assert state is not None
    assert state.state == (PREPARED if fault_point.endswith("before_commit") else COMMITTED)
    result = recovered.resume_v2(
        harness.request,
        participants=participants,
        allow_commit=True,
    )
    assert result.state == APPLIED


def test_forged_predecessor_is_rejected_by_held_progress(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)
    harness.coordinator.commit_v1(
        harness.request.request_sha256,
        participants=participants,
    )

    with pytest.raises(AcquisitionSourceReclamationApplyConflict):
        participants[1].apply_source_local_v1(
            harness.request.request_sha256,
            predecessor_apply_receipt_sha256=_sha("9"),
        )
    assert participants[1]._apply is None


def test_caller_built_apply_receipt_is_not_progress_authority(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)
    decision = harness.coordinator.commit_v1(
        harness.request.request_sha256,
        participants=participants,
    )
    prepared = participants[0]._prepare
    assert prepared is not None
    forged = SourceLocalApplyReceiptV1.create(
        request_sha256=harness.request.request_sha256,
        role=participants[0].role,
        source_store_identity_sha256=(
            participants[0].source_store_identity_sha256
        ),
        prepare_receipt_sha256=prepared.prepare_receipt_sha256,
        committed_decision_sha256=decision.decision_sha256,
        committed_decision_receipt_sha256=decision.receipt_sha256,
        predecessor_apply_receipt_sha256=None,
        effect_sha256=_sha("8"),
        post_apply_view_sha256=_sha("9"),
        released_bytes=1,
        released_entries=1,
        applied_at=harness.clock(),
    )
    with pytest.raises(AcquisitionSourceReclamationApplyConflict):
        harness.coordinator.record_source_local_apply_v1(
            forged,
            participant=participants[0],
        )
    state = harness.coordinator.load_state_v2(harness.request.request_sha256)
    assert state is not None and state.applied_roles == ()


def test_forged_abort_binding_is_rejected_while_held(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.begin_preparing_v2(harness.request)
    prepared = participants[0].prepare_source_local_v2(harness.request)
    harness.coordinator.record_source_local_prepare_v2(prepared)
    decision = harness.coordinator.begin_aborting_v1(
        harness.request.request_sha256
    )

    with pytest.raises(AcquisitionSourceReclamationApplyConflict):
        with harness.coordinator.decision_authority.hold_abort_decision_v1(
            harness.request.request_sha256,
            expected_abort_decision_sha256=decision.abort_decision_sha256,
            aborting_role=participants[0].role,
            expected_prepare_receipt_sha256=prepared.prepare_receipt_sha256,
            expected_source_store_identity_sha256=(
                participants[0].source_store_identity_sha256
            ),
            expected_source_fence_sha256=_sha("9"),
        ):
            pytest.fail("forged abort binding was yielded")


def test_candidate_rows_are_immutable_and_ddl_tamper_fails_closed(
    tmp_path: Path,
) -> None:
    harness = _harness(tmp_path)
    with closing(sqlite3.connect(harness.candidate.path)) as database:
        with pytest.raises(sqlite3.IntegrityError):
            database.execute(
                "UPDATE candidate_records SET candidate_sha256=?",
                (_sha("9"),),
            )
        with pytest.raises(sqlite3.IntegrityError):
            database.execute("DELETE FROM candidate_records")
        database.execute("DROP TRIGGER candidate_records_no_update")
        database.commit()

    with pytest.raises(AcquisitionSourceReclamationApplyIntegrityError):
        with harness.candidate.hold_candidate_page_v1(
            harness.request,
            after_cursor=0,
            max_candidates=1,
        ):
            pytest.fail("tampered candidate schema was accepted")


def test_coordinator_receipts_are_immutable_and_ddl_tamper_fails_closed(
    tmp_path: Path,
) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.prepare_v2(harness.request, participants=participants)
    with closing(sqlite3.connect(harness.coordinator.path)) as database:
        with pytest.raises(sqlite3.IntegrityError):
            database.execute(
                "UPDATE coordinator_prepares SET prepare_receipt_sha256=?",
                (_sha("9"),),
            )
        with pytest.raises(sqlite3.IntegrityError):
            database.execute("DELETE FROM coordinator_prepares")
        database.execute("DROP TRIGGER coordinator_prepares_no_update")
        database.commit()

    with pytest.raises(AcquisitionSourceReclamationApplyIntegrityError):
        harness.coordinator.load_state_v2(harness.request.request_sha256)


def test_same_path_valid_store_replacement_is_rejected(tmp_path: Path) -> None:
    original_root = tmp_path / "original"
    original_root.mkdir()
    original = _harness(original_root)
    replacement_root = tmp_path / "replacement"
    replacement_root.mkdir()
    replacement = SqliteSourceReclamationCandidateAuthority.create(
        replacement_root / "candidate.sqlite3",
        source_binding_sha256=_sha("2"),
        archive_store_identity_sha256=_sha("3"),
        directory_fsync=lambda _path: None,
    )
    original_identity = original.candidate.store_identity_sha256
    os.replace(replacement.path, original.candidate.path)

    with pytest.raises(AcquisitionSourceReclamationApplyIntegrityError):
        _ = original.candidate.authority_sha256
    with pytest.raises(AcquisitionSourceReclamationApplyIntegrityError):
        SqliteSourceReclamationCandidateAuthority(
            original.candidate.path,
            expected_store_identity_sha256=original_identity,
            directory_fsync=lambda _path: None,
        )


def test_clock_rollback_blocks_next_coordinator_write(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    participants = _participants(harness)
    harness.coordinator.begin_preparing_v2(harness.request)
    prepared = participants[0].prepare_source_local_v2(harness.request)
    harness.clock.value = 1.0
    with pytest.raises(AcquisitionSourceReclamationApplyClockError):
        harness.coordinator.record_source_local_prepare_v2(prepared)
    state = harness.coordinator.load_state_v2(harness.request.request_sha256)
    assert state is not None and state.prepared_roles == ()


def test_inline_raw_aliases_and_wrong_role_views_fail_closed(tmp_path: Path) -> None:
    binding = _binding()
    record = json.loads(_reference(binding).decode("ascii"))
    record["payload_base64"] = "AA=="
    payload = canonical_bytes(record)
    with pytest.raises(AcquisitionSourceReclamationProtocolIntegrityError):
        archive_binding_root((binding,), (payload,))

    record.pop("payload_base64")
    record["raw_artifacts_view_sha256"] = False
    with pytest.raises(AcquisitionSourceReclamationProtocolIntegrityError):
        archive_binding_root((binding,), (canonical_bytes(record),))


def test_candidate_seal_conflict_and_idempotency(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    first = harness.candidate.seal_request_v2(
        harness.request,
        bindings=harness.bindings,
        record_payloads=harness.payloads,
    )
    second = harness.candidate.seal_request_v2(
        harness.request,
        bindings=harness.bindings,
        record_payloads=harness.payloads,
    )
    assert second == first

    changed = json.loads(harness.payloads[0].decode("ascii"))
    changed["metadata_sha256"] = _sha("9")
    with pytest.raises(AcquisitionSourceReclamationApplyConflict):
        harness.candidate.seal_request_v2(
            harness.request,
            bindings=harness.bindings,
            record_payloads=(canonical_bytes(changed),),
        )


def test_candidate_seal_fault_before_commit_is_retryable(tmp_path: Path) -> None:
    clock = _Clock()

    def fault(point: str) -> None:
        if point == "candidate.seal.before_commit":
            raise RuntimeError("seal crash")

    authority = SqliteSourceReclamationCandidateAuthority.create(
        tmp_path / "candidate.sqlite3",
        source_binding_sha256=_sha("2"),
        archive_store_identity_sha256=_sha("3"),
        clock=clock,
        directory_fsync=lambda _path: None,
        fault_injector=fault,
    )
    binding = _binding()
    payload = _reference(binding)
    request = SourceReclamationRequestV2.create(
        source_binding_sha256=authority.source_binding_sha256,
        source_view_sha256=_sha("4"),
        sealed_archive_anchor_sha256=_sha("5"),
        archive_binding_root_sha256=archive_binding_root((binding,), (payload,)),
        candidate_bindings_sha256=candidate_binding_root((binding,)),
        candidate_authority_sha256=authority.authority_sha256,
        consumer_registry_root_sha256=_sha("6"),
        consumer_epoch_root_sha256=_sha("7"),
        candidate_count=1,
        created_at=900.0,
    )
    with pytest.raises(RuntimeError, match="seal crash"):
        authority.seal_request_v2(
            request,
            bindings=(binding,),
            record_payloads=(payload,),
        )

    reopened = SqliteSourceReclamationCandidateAuthority(
        authority.path,
        expected_store_identity_sha256=authority.store_identity_sha256,
        clock=clock,
        directory_fsync=lambda _path: None,
    )
    with pytest.raises(AcquisitionSourceReclamationApplyConflict):
        with reopened.hold_candidate_page_v1(
            request,
            after_cursor=0,
            max_candidates=1,
        ):
            pytest.fail("rolled-back candidate seal was visible")
    reopened.seal_request_v2(
        request,
        bindings=(binding,),
        record_payloads=(payload,),
    )
    with reopened.hold_candidate_page_v1(
        request,
        after_cursor=0,
        max_candidates=1,
    ) as page:
        assert page.exhausted is True


def test_candidate_catalog_request_cap(tmp_path: Path) -> None:
    clock = _Clock()
    authority = SqliteSourceReclamationCandidateAuthority.create(
        tmp_path / "candidate.sqlite3",
        source_binding_sha256=_sha("2"),
        archive_store_identity_sha256=_sha("3"),
        limits=SourceReclamationCandidateCatalogLimits(
            max_requests=1,
            max_candidates_per_request=2,
            max_total_records=2,
        ),
        clock=clock,
        directory_fsync=lambda _path: None,
    )
    for index in (1, 2):
        binding = _binding(index)
        payload = _reference(binding)
        request = SourceReclamationRequestV2.create(
            source_binding_sha256=authority.source_binding_sha256,
            source_view_sha256=hashlib_sha(f"view-{index}"),
            sealed_archive_anchor_sha256=hashlib_sha(f"anchor-{index}"),
            archive_binding_root_sha256=archive_binding_root((binding,), (payload,)),
            candidate_bindings_sha256=candidate_binding_root((binding,)),
            candidate_authority_sha256=authority.authority_sha256,
            consumer_registry_root_sha256=_sha("6"),
            consumer_epoch_root_sha256=_sha("7"),
            candidate_count=1,
            created_at=900.0 + index,
        )
        if index == 1:
            authority.seal_request_v2(
                request,
                bindings=(binding,),
                record_payloads=(payload,),
            )
        else:
            with pytest.raises(AcquisitionSourceReclamationApplyCapacityError):
                authority.seal_request_v2(
                    request,
                    bindings=(binding,),
                    record_payloads=(payload,),
                )


def test_ordered_roots_scale_to_4096_candidates() -> None:
    bindings = tuple(_binding(index) for index in range(1, 4097))
    payloads = tuple(_reference(item, raw_bytes=101 * 1024**2) for item in bindings)
    candidate_root = candidate_binding_root(bindings)
    archive_root = archive_binding_root(bindings, payloads)
    assert len(candidate_root) == 64
    assert len(archive_root) == 64
    assert sum(map(len, payloads)) < 8 * 1024 * 1024


def test_two_coordinator_instances_cas_same_request(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    peer = AcquisitionSourceReclamationApplyCoordinator(
        harness.coordinator.path,
        expected_store_identity_sha256=(
            harness.coordinator.store_identity_sha256
        ),
        candidate_authority=harness.candidate,
        clock=harness.clock,
        directory_fsync=lambda _path: None,
    )
    barrier = threading.Barrier(2)
    states: list[str] = []
    errors: list[BaseException] = []

    def begin(coordinator: AcquisitionSourceReclamationApplyCoordinator) -> None:
        try:
            barrier.wait()
            states.append(coordinator.begin_preparing_v2(harness.request).state)
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [
        threading.Thread(target=begin, args=(coordinator,))
        for coordinator in (harness.coordinator, peer)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10.0)
    assert not any(thread.is_alive() for thread in threads)
    assert errors == []
    assert states == ["PREPARING", "PREPARING"]
    assert harness.coordinator.audit_v2()["request_count"] == 1
