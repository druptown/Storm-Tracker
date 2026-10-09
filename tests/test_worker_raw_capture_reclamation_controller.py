from __future__ import annotations

import ast
from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
from typing import Iterator

import pytest

from storm_tracker_worker import raw_capture_reclamation_controller as module
from storm_tracker_worker.acquisition_source_reclamation_apply_authority import (
    AcquisitionSourceReclamationApplyConflict,
    AcquisitionSourceReclamationApplyCoordinator,
    CoordinatorStateSnapshotV2,
)
from storm_tracker_worker.acquisition_source_reclamation_protocol import (
    ABORTED,
    APPLIED,
    COMMITTED,
    PARTICIPANT_APPLY_ORDER,
    PREPARED,
    CommittedReclamationDecisionReceiptV1,
    ParticipantPrepareBindingV1,
    SourceLocalPrepareReceiptV2,
    SourceReclamationRequestV2,
)
from storm_tracker_worker.raw_capture_reclamation_controller import (
    ABORT,
    APPLY,
    COMMIT,
    PREPARE,
    RawCaptureReclamationBlockerAuthority,
    RawCaptureReclamationBlockerProofV1,
    RawCaptureReclamationController,
    RawCaptureReclamationControllerCapacityError,
    RawCaptureReclamationControllerConflict,
    RawCaptureReclamationControllerDisabled,
    RawCaptureReclamationControllerIntegrityError,
    RawCaptureReclamationOperatorAuthority,
    RawCaptureReclamationPhaseCommandV1,
)


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


@dataclass
class _DurableState:
    state: str | None = None
    event_sequence: int = 0
    committed: CommittedReclamationDecisionReceiptV1 | None = None
    transitions: list[str] = field(default_factory=list)
    crash_after: str | None = None
    crashed_phases: set[str] = field(default_factory=set)


class _Participant:
    def __init__(
        self,
        *,
        role: str,
        request: SourceReclamationRequestV2,
        coordinator_authority_sha256: str,
        candidate_authority_sha256: str,
        index: int,
    ) -> None:
        self._role = role
        self._store = _sha(f"store:{role}")
        self._coordinator = coordinator_authority_sha256
        self._candidate = candidate_authority_sha256
        self.receipt = self._receipt(request, index=index)

    @property
    def role(self) -> str:
        return self._role

    @property
    def source_store_identity_sha256(self) -> str:
        return self._store

    @property
    def coordinator_authority_sha256(self) -> str:
        return self._coordinator

    @property
    def candidate_authority_sha256(self) -> str:
        return self._candidate

    def _receipt(
        self,
        request: SourceReclamationRequestV2,
        *,
        index: int,
        candidate_bindings_sha256: str | None = None,
    ) -> SourceLocalPrepareReceiptV2:
        return SourceLocalPrepareReceiptV2.create(
            request_sha256=request.request_sha256,
            role=self.role,
            source_store_identity_sha256=self.source_store_identity_sha256,
            coordinator_authority_sha256=self.coordinator_authority_sha256,
            candidate_authority_sha256=self.candidate_authority_sha256,
            sealed_archive_anchor_sha256=request.sealed_archive_anchor_sha256,
            archive_binding_root_sha256=request.archive_binding_root_sha256,
            candidate_bindings_sha256=(
                candidate_bindings_sha256
                if candidate_bindings_sha256 is not None
                else request.candidate_bindings_sha256
            ),
            consumer_registry_root_sha256=(
                request.consumer_registry_root_sha256
            ),
            consumer_epoch_root_sha256=request.consumer_epoch_root_sha256,
            source_view_sha256=_sha(f"source-view:{self.role}"),
            child_reference_root_sha256=_sha(f"children:{self.role}"),
            source_fence_sha256=_sha(f"fence:{self.role}"),
            candidate_count=request.candidate_count,
            releasable_bytes=100 + index,
            releasable_entries=10 + index,
            prepared_at=1_000.0 + index,
        )

    def prepare_source_local_v2(
        self,
        request: SourceReclamationRequestV2,
    ) -> SourceLocalPrepareReceiptV2:
        assert request.request_sha256 == self.receipt.request_sha256
        return self.receipt

    def load_source_local_prepare_v2(
        self,
        request_sha256: str,
    ) -> SourceLocalPrepareReceiptV2 | None:
        if request_sha256 != self.receipt.request_sha256:
            return None
        return self.receipt

    def apply_source_local_v1(self, *args: object, **kwargs: object) -> object:
        raise AssertionError("controller must delegate apply to the coordinator")

    def load_source_local_apply_v1(self, *args: object, **kwargs: object) -> None:
        return None

    def abort_source_local_v1(self, *args: object, **kwargs: object) -> object:
        raise AssertionError("controller must delegate abort to the coordinator")

    def load_source_local_abort_v1(self, *args: object, **kwargs: object) -> None:
        return None


class _Coordinator(AcquisitionSourceReclamationApplyCoordinator):
    def __init__(
        self,
        *,
        request: SourceReclamationRequestV2,
        participants: tuple[_Participant, ...],
        authority_sha256: str,
        candidate_authority_sha256: str,
        durable: _DurableState,
    ) -> None:
        self._request = request
        self._test_participants = participants
        self._test_authority = authority_sha256
        self._test_candidate_authority = candidate_authority_sha256
        self._durable = durable
        self.prepare_calls = 0
        self.commit_calls = 0
        self.apply_calls = 0
        self.abort_calls = 0

    @property
    def authority_sha256(self) -> str:
        return self._test_authority

    @property
    def candidate_authority_sha256(self) -> str:
        return self._test_candidate_authority

    def _check_participants(self, participants: tuple[object, ...]) -> None:
        assert tuple(item.role for item in participants) == PARTICIPANT_APPLY_ORDER
        assert participants == self._test_participants

    def _transition(self, state: str) -> None:
        self._durable.state = state
        self._durable.event_sequence += 1
        self._durable.transitions.append(state)

    def _crash_if_armed(self, phase: str) -> None:
        if (
            self._durable.crash_after == phase
            and phase not in self._durable.crashed_phases
        ):
            self._durable.crashed_phases.add(phase)
            raise SystemExit(f"simulated-{phase.lower()}-crash")

    def _snapshot(self) -> CoordinatorStateSnapshotV2 | None:
        state = self._durable.state
        if state is None:
            return None
        prepared = (
            PARTICIPANT_APPLY_ORDER
            if state in {PREPARED, COMMITTED, APPLIED, ABORTED}
            else ()
        )
        applied = PARTICIPANT_APPLY_ORDER if state == APPLIED else ()
        aborted = PARTICIPANT_APPLY_ORDER if state == ABORTED else ()
        committed_sha = (
            self._durable.committed.receipt_sha256
            if self._durable.committed is not None
            else None
        )
        return CoordinatorStateSnapshotV2.create(
            request_sha256=self._request.request_sha256,
            state=state,
            owner_epoch=1,
            prepared_roles=prepared,
            applied_roles=applied,
            aborted_roles=aborted,
            committed_receipt_sha256=committed_sha,
            abort_receipt_sha256=(
                _sha("abort-receipt") if state == ABORTED else None
            ),
            event_sequence=self._durable.event_sequence,
            head_event_sha256=_sha(
                f"event:{self._durable.event_sequence}:{state}"
            ),
            updated_at=1_000.0 + self._durable.event_sequence,
        )

    def _committed_receipt(
        self,
    ) -> CommittedReclamationDecisionReceiptV1:
        bindings = tuple(
            ParticipantPrepareBindingV1(
                role=item.role,
                prepare_receipt_sha256=item.receipt.prepare_receipt_sha256,
                source_store_identity_sha256=(
                    item.source_store_identity_sha256
                ),
                source_fence_sha256=item.receipt.source_fence_sha256,
            )
            for item in self._test_participants
        )
        return CommittedReclamationDecisionReceiptV1.create(
            request_sha256=self._request.request_sha256,
            coordinator_store_identity_sha256=_sha("coordinator-store"),
            sealed_archive_anchor_sha256=(
                self._request.sealed_archive_anchor_sha256
            ),
            archive_binding_root_sha256=(
                self._request.archive_binding_root_sha256
            ),
            candidate_bindings_sha256=(
                self._request.candidate_bindings_sha256
            ),
            candidate_authority_sha256=(
                self._request.candidate_authority_sha256
            ),
            consumer_registry_root_sha256=(
                self._request.consumer_registry_root_sha256
            ),
            consumer_epoch_root_sha256=(
                self._request.consumer_epoch_root_sha256
            ),
            participant_prepares=bindings,
            candidate_count=self._request.candidate_count,
            owner_epoch=1,
            committed_at=1_100.0,
            commit_event_sequence=max(1, self._durable.event_sequence),
            previous_event_sha256=_sha("previous-event"),
            commit_event_sha256=_sha("commit-event"),
        )

    def load_state_v2(
        self,
        request_sha256: str,
    ) -> CoordinatorStateSnapshotV2 | None:
        if request_sha256 != self._request.request_sha256:
            return None
        return self._snapshot()

    def prepare_v2(
        self,
        request: SourceReclamationRequestV2,
        *,
        participants: tuple[object, ...],
    ) -> CoordinatorStateSnapshotV2:
        self.prepare_calls += 1
        self._check_participants(participants)
        assert request == self._request
        if self._durable.state is None:
            self._transition(PREPARED)
            self._crash_if_armed(PREPARE)
        elif self._durable.state not in {PREPARED, COMMITTED, APPLIED}:
            raise AcquisitionSourceReclamationApplyConflict(
                "fake prepare state differs"
            )
        snapshot = self._snapshot()
        assert snapshot is not None
        return snapshot

    def commit_v1(
        self,
        request_sha256: str,
        *,
        participants: tuple[object, ...],
    ) -> CommittedReclamationDecisionReceiptV1:
        self.commit_calls += 1
        self._check_participants(participants)
        assert request_sha256 == self._request.request_sha256
        if self._durable.state == PREPARED:
            self._transition(COMMITTED)
            self._durable.committed = self._committed_receipt()
            self._crash_if_armed(COMMIT)
        elif self._durable.state not in {COMMITTED, APPLIED}:
            raise AcquisitionSourceReclamationApplyConflict(
                "fake commit state differs"
            )
        assert self._durable.committed is not None
        return self._durable.committed

    def apply_v1(
        self,
        request_sha256: str,
        *,
        participants: tuple[object, ...],
    ) -> CoordinatorStateSnapshotV2:
        self.apply_calls += 1
        self._check_participants(participants)
        assert request_sha256 == self._request.request_sha256
        if self._durable.state == COMMITTED:
            self._transition(APPLIED)
            self._crash_if_armed(APPLY)
        elif self._durable.state != APPLIED:
            raise AcquisitionSourceReclamationApplyConflict(
                "fake apply state differs"
            )
        snapshot = self._snapshot()
        assert snapshot is not None
        return snapshot

    def abort_v1(
        self,
        request_sha256: str,
        *,
        participants: tuple[object, ...],
    ) -> CoordinatorStateSnapshotV2:
        self.abort_calls += 1
        self._check_participants(participants)
        assert request_sha256 == self._request.request_sha256
        if self._durable.state == PREPARED:
            self._transition(ABORTED)
        elif self._durable.state != ABORTED:
            raise AcquisitionSourceReclamationApplyConflict(
                "fake abort state differs"
            )
        snapshot = self._snapshot()
        assert snapshot is not None
        return snapshot


class _OperatorAuthority(RawCaptureReclamationOperatorAuthority):
    def __init__(self, authority_sha256: str) -> None:
        super().__init__(authority_sha256)
        self.entered = 0
        self.exited = 0
        self.failure: Exception | None = None

    @contextmanager
    def hold_phase_authorization_v1(
        self,
        command: RawCaptureReclamationPhaseCommandV1,
    ) -> Iterator[RawCaptureReclamationPhaseCommandV1]:
        self.entered += 1
        try:
            if self.failure is not None:
                raise self.failure
            yield command
        finally:
            self.exited += 1


class _BlockerAuthority(RawCaptureReclamationBlockerAuthority):
    def __init__(
        self,
        authority_sha256: str,
        proof: RawCaptureReclamationBlockerProofV1,
    ) -> None:
        super().__init__(authority_sha256)
        self.proof = proof
        self.entered = 0
        self.exited = 0

    @contextmanager
    def hold_verified_blockers_v1(
        self,
        request: SourceReclamationRequestV2,
        *,
        expected_proof_sha256: str,
    ) -> Iterator[RawCaptureReclamationBlockerProofV1]:
        self.entered += 1
        try:
            assert request.request_sha256 == self.proof.request_sha256
            assert expected_proof_sha256 == self.proof.proof_sha256
            yield self.proof
        finally:
            self.exited += 1


@dataclass
class _Harness:
    request: SourceReclamationRequestV2
    participants: tuple[_Participant, ...]
    durable: _DurableState
    coordinator: _Coordinator
    operator: _OperatorAuthority
    blocker: _BlockerAuthority
    controller: RawCaptureReclamationController

    def reopen(self, *, enabled: bool = True) -> RawCaptureReclamationController:
        reopened = _Coordinator(
            request=self.request,
            participants=self.participants,
            authority_sha256=self.coordinator.authority_sha256,
            candidate_authority_sha256=(
                self.coordinator.candidate_authority_sha256
            ),
            durable=self.durable,
        )
        self.coordinator = reopened
        return RawCaptureReclamationController(
            coordinator=reopened,
            participants=self.participants,
            operator_authority=self.operator,
            blocker_authority=self.blocker,
            enabled=enabled,
            clock=lambda: 1_100.0,
        )


def _proof_values(
    *,
    request: SourceReclamationRequestV2,
    blocker_authority_sha256: str,
) -> dict[str, object]:
    return {
        "blocker_authority_sha256": blocker_authority_sha256,
        "request_sha256": request.request_sha256,
        "candidate_bindings_sha256": request.candidate_bindings_sha256,
        "sealed_archive_anchor_sha256": request.sealed_archive_anchor_sha256,
        "archive_binding_root_sha256": request.archive_binding_root_sha256,
        "archive_audit_sha256": _sha("archive-audit"),
        "hold_inventory_sha256": _sha("hold-inventory"),
        "golden_reference_inventory_sha256": _sha("golden-inventory"),
        "candidate_count": request.candidate_count,
        "active_hold_overlap_count": 0,
        "golden_reference_overlap_count": 0,
        "archive_verified": True,
        "hold_inventory_complete": True,
        "golden_reference_inventory_complete": True,
    }


def _harness(*, enabled: bool = True) -> _Harness:
    candidate_authority = _sha("candidate-authority")
    coordinator_authority = _sha("coordinator-authority")
    request = SourceReclamationRequestV2.create(
        source_binding_sha256=_sha("source-binding"),
        source_view_sha256=_sha("source-view"),
        sealed_archive_anchor_sha256=_sha("archive-anchor"),
        archive_binding_root_sha256=_sha("archive-root"),
        candidate_bindings_sha256=_sha("candidate-root"),
        candidate_authority_sha256=candidate_authority,
        consumer_registry_root_sha256=_sha("consumer-registry"),
        consumer_epoch_root_sha256=_sha("consumer-epoch"),
        candidate_count=2,
        created_at=1_000.0,
    )
    participants = tuple(
        _Participant(
            role=role,
            request=request,
            coordinator_authority_sha256=coordinator_authority,
            candidate_authority_sha256=candidate_authority,
            index=index,
        )
        for index, role in enumerate(PARTICIPANT_APPLY_ORDER)
    )
    durable = _DurableState()
    coordinator = _Coordinator(
        request=request,
        participants=participants,
        authority_sha256=coordinator_authority,
        candidate_authority_sha256=candidate_authority,
        durable=durable,
    )
    operator = _OperatorAuthority(_sha("operator-authority"))
    blocker_authority_sha256 = _sha("blocker-authority")
    proof = RawCaptureReclamationBlockerProofV1.create(
        **_proof_values(
            request=request,
            blocker_authority_sha256=blocker_authority_sha256,
        )
    )
    blocker = _BlockerAuthority(blocker_authority_sha256, proof)
    controller = RawCaptureReclamationController(
        coordinator=coordinator,
        participants=participants,
        operator_authority=operator,
        blocker_authority=blocker,
        enabled=enabled,
        clock=lambda: 1_100.0,
    )
    return _Harness(
        request=request,
        participants=participants,
        durable=durable,
        coordinator=coordinator,
        operator=operator,
        blocker=blocker,
        controller=controller,
    )


def _command(
    harness: _Harness,
    phase: str,
    *,
    review_sha256: str | None = None,
    committed_sha256: str | None = None,
    max_bytes: int = 1_000,
    max_entries: int = 1_000,
    nonce_suffix: str = "1",
) -> RawCaptureReclamationPhaseCommandV1:
    if phase == ABORT:
        max_bytes = 0
        max_entries = 0
    return RawCaptureReclamationPhaseCommandV1.create(
        operator_authority_sha256=harness.operator.authority_sha256,
        phase=phase,
        request_sha256=harness.request.request_sha256,
        candidate_authority_sha256=(
            harness.request.candidate_authority_sha256
        ),
        coordinator_authority_sha256=(
            harness.coordinator.authority_sha256
        ),
        candidate_bindings_sha256=(
            harness.request.candidate_bindings_sha256
        ),
        sealed_archive_anchor_sha256=(
            harness.request.sealed_archive_anchor_sha256
        ),
        archive_binding_root_sha256=(
            harness.request.archive_binding_root_sha256
        ),
        blocker_proof_sha256=harness.blocker.proof.proof_sha256,
        prepared_review_sha256=review_sha256,
        committed_receipt_sha256=committed_sha256,
        candidate_count=harness.request.candidate_count,
        max_releasable_bytes_per_role=max_bytes,
        max_releasable_entries_per_role=max_entries,
        issued_at=1_000.0,
        expires_at=1_200.0,
        nonce=f"{phase.lower()}:{nonce_suffix}",
    )


def _prepare_and_review(
    harness: _Harness,
) -> tuple[RawCaptureReclamationPhaseCommandV1, object]:
    command = _command(harness, PREPARE)
    harness.controller.prepare_v1(harness.request, command)
    review = harness.controller.review_prepared_v1(harness.request, command)
    return command, review


def test_controller_is_default_disabled_before_any_authority_or_coordinator_call() -> None:
    harness = _harness(enabled=False)
    command = _command(harness, PREPARE)

    with pytest.raises(RawCaptureReclamationControllerDisabled):
        harness.controller.prepare_v1(harness.request, command)

    assert harness.operator.entered == 0
    assert harness.blocker.entered == 0
    assert harness.coordinator.prepare_calls == 0
    diagnostics = harness.controller.diagnostics()
    assert diagnostics["enabled"] is False
    assert diagnostics["automatic_candidate_selection"] is False
    assert diagnostics["automatic_reclamation"] is False
    assert diagnostics["live_service_wiring"] is False
    assert diagnostics["meteorological_output"] is False
    assert diagnostics["dashboard_output"] is False
    assert diagnostics["production_enable_ready"] is False


def test_phase_command_builder_rejects_extra_keys_and_boolean_timestamps() -> None:
    harness = _harness()
    command = _command(harness, PREPARE)
    values = command.as_dict()
    for derived in (
        "automatic_candidate_selection",
        "command_sha256",
        "operator_authority_id",
        "operator_initiated",
        "schema",
    ):
        values.pop(derived)

    with pytest.raises(TypeError):
        RawCaptureReclamationPhaseCommandV1.create(
            **values,
            attacker_extra_field="ignored-by-loose-builder",
        )
    values["issued_at"] = True
    with pytest.raises(RawCaptureReclamationControllerIntegrityError):
        RawCaptureReclamationPhaseCommandV1.create(**values)


@pytest.mark.parametrize(
    ("override", "value"),
    (
        ("archive_verified", False),
        ("hold_inventory_complete", False),
        ("golden_reference_inventory_complete", False),
        ("active_hold_overlap_count", 1),
        ("golden_reference_overlap_count", 1),
    ),
)
def test_every_archive_hold_and_golden_blocker_stops_before_prepare(
    override: str,
    value: object,
) -> None:
    harness = _harness()
    values = _proof_values(
        request=harness.request,
        blocker_authority_sha256=harness.blocker.authority_sha256,
    )
    values[override] = value
    harness.blocker.proof = RawCaptureReclamationBlockerProofV1.create(**values)
    command = _command(harness, PREPARE)

    with pytest.raises(RawCaptureReclamationControllerConflict):
        harness.controller.prepare_v1(harness.request, command)

    assert harness.coordinator.prepare_calls == 0
    assert harness.durable.state is None


def test_prepare_commit_apply_are_separate_and_reopen_idempotently() -> None:
    harness = _harness()
    prepare_command, review = _prepare_and_review(harness)
    assert harness.durable.transitions == [PREPARED]
    assert harness.coordinator.commit_calls == 0
    assert harness.coordinator.apply_calls == 0

    commit_command = _command(
        harness,
        COMMIT,
        review_sha256=review.review_sha256,
    )
    committed_phase = harness.controller.commit_v1(
        harness.request,
        commit_command,
    )
    assert committed_phase.state == COMMITTED
    assert committed_phase.source_release_performed is False
    assert committed_phase.committed_receipt_sha256 is not None

    stable_review = harness.controller.review_prepared_v1(
        harness.request,
        prepare_command,
    )
    assert stable_review == review
    apply_command = _command(
        harness,
        APPLY,
        review_sha256=review.review_sha256,
        committed_sha256=committed_phase.committed_receipt_sha256,
    )
    applied_phase = harness.controller.apply_v1(
        harness.request,
        apply_command,
    )
    assert applied_phase.state == APPLIED
    assert applied_phase.source_release_performed is True
    assert harness.durable.transitions == [PREPARED, COMMITTED, APPLIED]

    harness.controller = harness.reopen()
    replayed_commit = harness.controller.commit_v1(
        harness.request,
        commit_command,
    )
    replayed_apply = harness.controller.apply_v1(
        harness.request,
        apply_command,
    )
    assert replayed_commit.committed_receipt_sha256 == (
        committed_phase.committed_receipt_sha256
    )
    assert replayed_apply.state == APPLIED
    assert harness.durable.transitions == [PREPARED, COMMITTED, APPLIED]


def test_apply_command_never_implicitly_commits_a_prepared_request() -> None:
    harness = _harness()
    _, review = _prepare_and_review(harness)
    command = _command(
        harness,
        APPLY,
        review_sha256=review.review_sha256,
        committed_sha256=_sha("not-committed"),
    )

    with pytest.raises(RawCaptureReclamationControllerIntegrityError):
        harness.controller.apply_v1(harness.request, command)

    assert harness.durable.state == PREPARED
    assert harness.coordinator.commit_calls == 0
    assert harness.coordinator.apply_calls == 0


@pytest.mark.parametrize("crash_phase", (PREPARE, COMMIT, APPLY))
def test_post_transition_crash_reopens_and_replays_without_second_transition(
    crash_phase: str,
) -> None:
    harness = _harness()
    harness.durable.crash_after = crash_phase
    prepare_command = _command(harness, PREPARE)

    if crash_phase == PREPARE:
        with pytest.raises(SystemExit):
            harness.controller.prepare_v1(harness.request, prepare_command)
        assert harness.durable.state == PREPARED
        harness.controller = harness.reopen()
        harness.controller.prepare_v1(harness.request, prepare_command)
        review = harness.controller.review_prepared_v1(
            harness.request,
            prepare_command,
        )
    else:
        harness.controller.prepare_v1(harness.request, prepare_command)
        review = harness.controller.review_prepared_v1(
            harness.request,
            prepare_command,
        )

    commit_command = _command(
        harness,
        COMMIT,
        review_sha256=review.review_sha256,
    )
    if crash_phase == COMMIT:
        with pytest.raises(SystemExit):
            harness.controller.commit_v1(harness.request, commit_command)
        assert harness.durable.state == COMMITTED
        harness.controller = harness.reopen()
    committed = harness.controller.commit_v1(harness.request, commit_command)

    apply_command = _command(
        harness,
        APPLY,
        review_sha256=review.review_sha256,
        committed_sha256=committed.committed_receipt_sha256,
    )
    if crash_phase == APPLY:
        with pytest.raises(SystemExit):
            harness.controller.apply_v1(harness.request, apply_command)
        assert harness.durable.state == APPLIED
        harness.controller = harness.reopen()
    harness.controller.apply_v1(harness.request, apply_command)

    assert harness.durable.transitions == [PREPARED, COMMITTED, APPLIED]
    assert harness.operator.entered == harness.operator.exited
    assert harness.blocker.entered == harness.blocker.exited


def test_prepare_capacity_failure_leaves_fence_but_never_commits_or_applies() -> None:
    harness = _harness()
    command = _command(harness, PREPARE, max_bytes=1, max_entries=1)

    with pytest.raises(RawCaptureReclamationControllerCapacityError):
        harness.controller.prepare_v1(harness.request, command)

    assert harness.durable.state == PREPARED
    assert harness.coordinator.commit_calls == 0
    assert harness.coordinator.apply_calls == 0
    assert harness.controller.diagnostics()["last_error_code"] == (
        "controller_capacity_rejected"
    )


def test_prepare_receipt_drift_is_rejected_before_commit() -> None:
    harness = _harness()
    prepare_command, _ = _prepare_and_review(harness)
    participant = harness.participants[2]
    participant.receipt = participant._receipt(
        harness.request,
        index=2,
        candidate_bindings_sha256=_sha("drifted-candidate-root"),
    )

    with pytest.raises(RawCaptureReclamationControllerIntegrityError):
        harness.controller.review_prepared_v1(
            harness.request,
            prepare_command,
        )

    assert harness.coordinator.commit_calls == 0


def test_explicit_abort_releases_no_source_and_cannot_follow_commit() -> None:
    harness = _harness()
    _prepare_and_review(harness)
    abort_command = _command(harness, ABORT)
    receipt = harness.controller.abort_v1(harness.request, abort_command)
    assert receipt.state == ABORTED
    assert receipt.source_release_performed is False
    assert receipt.applied_role_count == 0

    committed_harness = _harness()
    _, review = _prepare_and_review(committed_harness)
    commit_command = _command(
        committed_harness,
        COMMIT,
        review_sha256=review.review_sha256,
    )
    committed_harness.controller.commit_v1(
        committed_harness.request,
        commit_command,
    )
    with pytest.raises(RawCaptureReclamationControllerConflict):
        committed_harness.controller.abort_v1(
            committed_harness.request,
            _command(committed_harness, ABORT),
        )


@pytest.mark.parametrize(
    ("override", "value"),
    (
        ("archive_verified", False),
        ("active_hold_overlap_count", 1),
        ("golden_reference_overlap_count", 1),
    ),
)
def test_new_blocker_after_prepare_stops_release_but_not_safe_abort(
    override: str,
    value: object,
) -> None:
    harness = _harness()
    _, review = _prepare_and_review(harness)
    values = _proof_values(
        request=harness.request,
        blocker_authority_sha256=harness.blocker.authority_sha256,
    )
    values[override] = value
    harness.blocker.proof = RawCaptureReclamationBlockerProofV1.create(**values)

    commit_command = _command(
        harness,
        COMMIT,
        review_sha256=review.review_sha256,
    )
    apply_command = _command(
        harness,
        APPLY,
        review_sha256=review.review_sha256,
        committed_sha256=_sha("not-committed"),
    )
    with pytest.raises(RawCaptureReclamationControllerConflict):
        harness.controller.commit_v1(harness.request, commit_command)
    with pytest.raises(RawCaptureReclamationControllerConflict):
        harness.controller.apply_v1(harness.request, apply_command)
    assert harness.coordinator.commit_calls == 0
    assert harness.coordinator.apply_calls == 0

    abort_command = _command(harness, ABORT)
    aborted = harness.controller.abort_v1(harness.request, abort_command)
    assert aborted.state == ABORTED
    assert aborted.source_release_performed is False
    assert harness.durable.transitions == [PREPARED, ABORTED]

    harness.controller = harness.reopen()
    replayed = harness.controller.abort_v1(harness.request, abort_command)
    assert replayed.state == ABORTED
    assert replayed.source_release_performed is False
    assert harness.durable.transitions == [PREPARED, ABORTED]


@pytest.mark.parametrize(
    ("failure", "expected_error"),
    (
        (RuntimeError("customer/site/private/archive-name"), "authority_reproof_failed"),
        (
            RawCaptureReclamationControllerIntegrityError(
                "customer/site/private/archive-name"
            ),
            "controller_integrity_rejected",
        ),
    ),
)
def test_diagnostics_never_echo_external_exception_text(
    failure: Exception,
    expected_error: str,
) -> None:
    harness = _harness()
    secret = "customer/site/private/archive-name"
    harness.operator.failure = failure

    with pytest.raises(
        RawCaptureReclamationControllerIntegrityError,
        match=expected_error,
    ):
        harness.controller.prepare_v1(
            harness.request,
            _command(harness, PREPARE),
        )

    encoded = json.dumps(harness.controller.diagnostics(), sort_keys=True)
    assert secret not in encoded
    assert "authority_reproof_failed" not in encoded
    assert "controller_integrity_rejected" in encoded
    assert len(encoded.encode("utf-8")) <= module.MAX_DIAGNOSTIC_BYTES


def test_constructor_rejects_nonexclusive_five_role_store_ownership() -> None:
    harness = _harness()
    harness.participants[1]._store = harness.participants[0]._store

    with pytest.raises(
        RawCaptureReclamationControllerIntegrityError,
        match="controller_participant_authority_mismatch",
    ):
        RawCaptureReclamationController(
            coordinator=harness.coordinator,
            participants=harness.participants,
            operator_authority=harness.operator,
            blocker_authority=harness.blocker,
            enabled=False,
        )


def test_module_contains_no_live_wiring_or_direct_deletion_primitive() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    imported.update(
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    )
    assert not any(
        name.startswith(
            (
                "homeassistant",
                "storm_tracker_worker.service",
                "storm_tracker_worker.raw_capture_service_composition",
                "paho",
            )
        )
        for name in imported
    )
    forbidden_calls = {
        "unlink",
        "remove",
        "removedirs",
        "rmdir",
        "rmtree",
    }
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in forbidden_calls
        for node in ast.walk(tree)
    )
    assert module.AUTOMATIC_CANDIDATE_SELECTION is False
    assert module.AUTOMATIC_RECLAMATION is False
    assert module.LIVE_SERVICE_WIRING is False
    assert module.METEOROLOGICAL_OUTPUT is False
    assert module.DASHBOARD_OUTPUT is False
    assert module.PRODUCTION_ENABLE_READY is False
