from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
import hashlib
import os
from pathlib import Path
import shutil
import sqlite3
from typing import Iterator

import pytest

from storm_tracker_worker.acquisition_control_reclamation import (
    AcquisitionControlReclamationConflict,
    AcquisitionControlReclamationIntegrityError,
    CapacityLedgerReclamationParticipantV2,
    ExecutionReceiptReclamationParticipantV2,
    SchedulerReclamationParticipantV2,
)
from storm_tracker_worker import acquisition_control_reclamation as reclamation
from storm_tracker_worker.acquisition_source_reclamation_apply_authority import (
    SqliteSourceReclamationCandidateAuthority,
)
from storm_tracker_worker.acquisition_source_reclamation_protocol import (
    CANDIDATE_REFERENCE_RECORD_SCHEMA,
    PARTICIPANT_APPLY_ORDER,
    CommittedReclamationDecisionReceiptV1,
    CoordinatorAbortDecisionReceiptV1,
    HeldCommittedDecisionAuthority,
    ParticipantPrepareBindingV1,
    SourceReclamationCandidateBindingV1,
    SourceReclamationRequestV2,
    archive_binding_root,
    candidate_binding_root,
    canonical_bytes,
    digest_mapping,
)
from storm_tracker_worker.acquisition_source_reclamation_views import (
    participant_candidate_view_sha256,
)

from test_worker_acquisition_source_retention import NOW, _authorities


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


class _Clock:
    def __init__(self, value: float = NOW + 1_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        result = self.value
        self.value += 1.0
        return result


class _DecisionAuthority(HeldCommittedDecisionAuthority):
    def __init__(self) -> None:
        super().__init__(_sha("control-decision-authority"))
        self.committed: CommittedReclamationDecisionReceiptV1 | None = None
        self.aborting: CoordinatorAbortDecisionReceiptV1 | None = None
        self.prepares: dict[str, ParticipantPrepareBindingV1] = {}

    @contextmanager
    def hold_committed_decision_v1(
        self,
        request_sha256: str,
        *,
        applying_role: str,
        expected_predecessor_apply_receipt_sha256: str | None,
    ) -> Iterator[CommittedReclamationDecisionReceiptV1]:
        assert self.committed is not None
        assert self.committed.request_sha256 == request_sha256
        assert applying_role in PARTICIPANT_APPLY_ORDER
        if applying_role == PARTICIPANT_APPLY_ORDER[0]:
            assert expected_predecessor_apply_receipt_sha256 is None
        else:
            assert expected_predecessor_apply_receipt_sha256 is not None
        yield self.committed

    @contextmanager
    def hold_abort_decision_v1(
        self,
        request_sha256: str,
        *,
        expected_abort_decision_sha256: str,
        aborting_role: str,
        expected_prepare_receipt_sha256: str,
        expected_source_store_identity_sha256: str,
        expected_source_fence_sha256: str,
    ) -> Iterator[CoordinatorAbortDecisionReceiptV1]:
        assert self.aborting is not None
        assert self.aborting.request_sha256 == request_sha256
        assert (
            self.aborting.abort_decision_sha256
            == expected_abort_decision_sha256
        )
        binding = self.prepares[aborting_role]
        assert binding.prepare_receipt_sha256 == expected_prepare_receipt_sha256
        assert (
            binding.source_store_identity_sha256
            == expected_source_store_identity_sha256
        )
        assert binding.source_fence_sha256 == expected_source_fence_sha256
        yield self.aborting


def _candidate_authority_and_request(tmp_path: Path, sources):
    candidate = sources.candidates[0]
    history = sources.executions.load_job_receipts(
        candidate.acquisition_job_id
    )
    scheduler = sources.scheduler.terminal_proof(candidate.scheduler_job_id)
    assert scheduler is not None
    capacity = sources.capacity.load_for_job(
        provider="dwd",
        job_identity=candidate.acquisition_job_id,
    )
    assert capacity is not None
    candidate_sha = _sha("control-candidate")
    views = {
        "downstream_lifecycle": _sha("lifecycle-view"),
        "execution_receipts": participant_candidate_view_sha256(
            role="execution_receipts",
            candidate_sha256=candidate_sha,
            projection={
                "execution_history": [item.as_dict() for item in history],
            },
        ),
        "scheduler": participant_candidate_view_sha256(
            role="scheduler",
            candidate_sha256=candidate_sha,
            projection={"scheduler_terminal": {
                **scheduler.as_dict(),
                "proof_sha256": scheduler.proof_sha256,
            }},
        ),
        "raw_artifacts": _sha("raw-view"),
        "capacity_ledger": participant_candidate_view_sha256(
            role="capacity_ledger",
            candidate_sha256=candidate_sha,
            projection={"capacity_commit": capacity.to_mapping()},
        ),
    }
    binding = SourceReclamationCandidateBindingV1.create(
        candidate_sha256=candidate_sha,
        acquisition_job_id=candidate.acquisition_job_id,
        scheduler_job_id=candidate.scheduler_job_id,
        raw_capture_id=candidate.raw_capture_id,
        evidence_stream_id=candidate.evidence_stream_id,
        evidence_stream_sequence=candidate.evidence_stream_sequence,
        sealed_archive_segment_sha256=_sha("control-segment"),
        sealed_archive_record_sha256=_sha("control-record"),
        consumer_closure_receipt_sha256=_sha("control-closure"),
        consumer_closure_root_sha256=_sha("control-closure-root"),
    )
    payload = canonical_bytes({
        "archive_object_count": 3,
        "archive_object_reference_root_sha256": _sha("object-root"),
        "archive_object_total_bytes": 101 * 1024 * 1024,
        "candidate_sha256": candidate_sha,
        "capacity_ledger_view_sha256": views["capacity_ledger"],
        "downstream_lifecycle_view_sha256": views[
            "downstream_lifecycle"
        ],
        "execution_receipts_view_sha256": views["execution_receipts"],
        "metadata_sha256": _sha("control-metadata"),
        "raw_artifacts_view_sha256": views["raw_artifacts"],
        "record_sha256": binding.sealed_archive_record_sha256,
        "scheduler_view_sha256": views["scheduler"],
        "schema": CANDIDATE_REFERENCE_RECORD_SCHEMA,
        "sealed_archive_segment_sha256": (
            binding.sealed_archive_segment_sha256
        ),
    })
    authority = SqliteSourceReclamationCandidateAuthority.create(
        (tmp_path / "candidate-authority.sqlite3").resolve(),
        source_binding_sha256=_sha("control-source-binding"),
        archive_store_identity_sha256=_sha("control-archive-identity"),
        directory_fsync=lambda _path: None,
        clock=_Clock(),
    )
    bindings = (binding,)
    payloads = (payload,)
    request = SourceReclamationRequestV2.create(
        source_binding_sha256=authority.source_binding_sha256,
        source_view_sha256=_sha("control-all-source-view"),
        sealed_archive_anchor_sha256=_sha("control-anchor"),
        archive_binding_root_sha256=archive_binding_root(bindings, payloads),
        candidate_bindings_sha256=candidate_binding_root(bindings),
        candidate_authority_sha256=authority.authority_sha256,
        consumer_registry_root_sha256=_sha("control-registry"),
        consumer_epoch_root_sha256=_sha("control-epoch"),
        candidate_count=1,
        created_at=NOW + 900.0,
    )
    authority.seal_request_v2(
        request,
        bindings=bindings,
        record_payloads=payloads,
    )
    return authority, request


def _prepare_bindings(real_receipts) -> tuple[ParticipantPrepareBindingV1, ...]:
    actual = {item.role: item for item in real_receipts}
    result = []
    for ordinal, role in enumerate(PARTICIPANT_APPLY_ORDER):
        receipt = actual.get(role)
        result.append(ParticipantPrepareBindingV1(
            role=role,
            prepare_receipt_sha256=(
                receipt.prepare_receipt_sha256
                if receipt is not None
                else _sha(f"dummy-prepare-{role}")
            ),
            source_store_identity_sha256=(
                receipt.source_store_identity_sha256
                if receipt is not None
                else _sha(f"dummy-store-{role}")
            ),
            source_fence_sha256=(
                receipt.source_fence_sha256
                if receipt is not None
                else _sha(f"dummy-fence-{role}")
            ),
        ))
    return tuple(result)


def _committed(request, prepares):
    return CommittedReclamationDecisionReceiptV1.create(
        request_sha256=request.request_sha256,
        coordinator_store_identity_sha256=_sha("control-coordinator-store"),
        sealed_archive_anchor_sha256=request.sealed_archive_anchor_sha256,
        archive_binding_root_sha256=request.archive_binding_root_sha256,
        candidate_bindings_sha256=request.candidate_bindings_sha256,
        candidate_authority_sha256=request.candidate_authority_sha256,
        consumer_registry_root_sha256=request.consumer_registry_root_sha256,
        consumer_epoch_root_sha256=request.consumer_epoch_root_sha256,
        participant_prepares=prepares,
        candidate_count=request.candidate_count,
        owner_epoch=1,
        committed_at=NOW + 2_000.0,
        commit_event_sequence=1,
        previous_event_sha256=_sha("control-previous-event"),
        commit_event_sha256=_sha("control-commit-event"),
    )


def _bind_committed(decisions, request, receipts):
    bindings = _prepare_bindings(tuple(receipts))
    decisions.prepares = {item.role: item for item in bindings}
    decisions.committed = _committed(request, bindings)
    return bindings


def _bind_aborting(decisions, request, receipts):
    bindings = _prepare_bindings(tuple(receipts))
    decisions.prepares = {item.role: item for item in bindings}
    decisions.aborting = CoordinatorAbortDecisionReceiptV1.create(
        request_sha256=request.request_sha256,
        coordinator_store_identity_sha256=_sha("control-coordinator-store"),
        participant_prepare_root_sha256=digest_mapping(
            [item.as_dict() for item in bindings]
        ),
        owner_epoch=1,
        aborting_at=NOW + 2_000.0,
        abort_event_sequence=1,
        previous_event_sha256=_sha("control-abort-previous"),
        abort_event_sha256=_sha("control-abort-event"),
    )
    return bindings


class _FailOnce:
    def __init__(self, point: str) -> None:
        self.point = point
        self.fired = False

    def __call__(self, point: str) -> None:
        if point == self.point and not self.fired:
            self.fired = True
            raise RuntimeError(f"injected control fault: {point}")


def _second_request(authority, request):
    with authority.hold_candidate_page_v1(
        request,
        after_cursor=0,
        max_candidates=request.candidate_count,
    ) as page:
        bindings = page.bindings
        payloads = page.record_payloads
    result = SourceReclamationRequestV2.create(
        source_binding_sha256=request.source_binding_sha256,
        source_view_sha256=_sha("control-second-source-view"),
        sealed_archive_anchor_sha256=request.sealed_archive_anchor_sha256,
        archive_binding_root_sha256=request.archive_binding_root_sha256,
        candidate_bindings_sha256=request.candidate_bindings_sha256,
        candidate_authority_sha256=request.candidate_authority_sha256,
        consumer_registry_root_sha256=request.consumer_registry_root_sha256,
        consumer_epoch_root_sha256=request.consumer_epoch_root_sha256,
        candidate_count=request.candidate_count,
        created_at=request.created_at + 1.0,
    )
    authority.seal_request_v2(
        result,
        bindings=bindings,
        record_payloads=payloads,
    )
    return result


def _rewrite_schema_sql(path: Path, name: str, old: str, new: str) -> None:
    with closing(sqlite3.connect(path)) as database:
        row = database.execute(
            "SELECT sql FROM sqlite_master WHERE name=?",
            (name,),
        ).fetchone()
        assert row is not None
        changed = str(row[0]).replace(old, new)
        assert changed != str(row[0])
        database.execute("PRAGMA writable_schema=ON")
        cursor = database.execute(
            "UPDATE sqlite_master SET sql=? WHERE name=?",
            (changed, name),
        )
        assert cursor.rowcount == 1
        database.execute("PRAGMA writable_schema=OFF")
        database.commit()


def _table_rows(path: Path, table: str) -> tuple[dict, ...]:
    assert table in {
        "execution_receipts",
        "terminal_heads",
        "scheduler_outbox",
        "reservations",
    }
    with closing(sqlite3.connect(path)) as database:
        database.row_factory = sqlite3.Row
        return tuple(dict(row) for row in database.execute(
            f"SELECT * FROM {table}"
        ).fetchall())


def _insert_row(database, table: str, row: dict) -> None:
    assert table in {
        "execution_receipts",
        "terminal_heads",
        "scheduler_outbox",
        "reservations",
    }
    columns = tuple(row)
    database.execute(
        f"INSERT INTO {table}({','.join(columns)}) VALUES("
        + ",".join("?" for _ in columns)
        + ")",
        tuple(row[item] for item in columns),
    )


def test_three_control_participants_apply_and_replay_without_source_rows(
    tmp_path,
):
    sources = _authorities(tmp_path / "sources")
    candidate, request = _candidate_authority_and_request(tmp_path, sources)
    decisions = _DecisionAuthority()
    clock = _Clock()
    execution = ExecutionReceiptReclamationParticipantV2(
        sources.executions,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=clock,
    )
    scheduler = SchedulerReclamationParticipantV2(
        sources.scheduler,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=clock,
    )
    capacity = CapacityLedgerReclamationParticipantV2(
        sources.capacity,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=clock,
    )
    participants = (execution, scheduler, capacity)
    prepares = tuple(
        participant.prepare_source_local_v2(request)
        for participant in participants
    )
    decision_bindings = _prepare_bindings(prepares)
    decisions.prepares = {item.role: item for item in decision_bindings}
    decisions.committed = _committed(request, decision_bindings)

    lifecycle_predecessor = _sha("lifecycle-apply-receipt")
    execution_apply = execution.apply_source_local_v1(
        request.request_sha256,
        predecessor_apply_receipt_sha256=lifecycle_predecessor,
    )
    scheduler_apply = scheduler.apply_source_local_v1(
        request.request_sha256,
        predecessor_apply_receipt_sha256=(
            execution_apply.apply_receipt_sha256
        ),
    )
    raw_predecessor = _sha("raw-apply-receipt")
    capacity_apply = capacity.apply_source_local_v1(
        request.request_sha256,
        predecessor_apply_receipt_sha256=raw_predecessor,
    )

    source = sources.candidates[0]
    assert sources.executions.load_job_receipts(
        source.acquisition_job_id
    ) == ()
    assert sources.scheduler.get_record(source.scheduler_job_id) is None
    assert sources.capacity.load_for_job(
        provider="dwd",
        job_identity=source.acquisition_job_id,
    ) is None
    assert sources.capacity.stats()["committed_count"] == 0
    capacity_audit = sources.capacity.audit()
    assert capacity_audit["reservation_rows"] == 0
    assert capacity_audit["committed_count"] == 0

    assert execution.prepare_source_local_v2(request) == prepares[0]
    assert scheduler.prepare_source_local_v2(request) == prepares[1]
    assert capacity.prepare_source_local_v2(request) == prepares[2]
    assert execution.apply_source_local_v1(
        request.request_sha256,
        predecessor_apply_receipt_sha256=lifecycle_predecessor,
    ) == execution_apply
    assert scheduler.apply_source_local_v1(
        request.request_sha256,
        predecessor_apply_receipt_sha256=(
            execution_apply.apply_receipt_sha256
        ),
    ) == scheduler_apply
    assert capacity.apply_source_local_v1(
        request.request_sha256,
        predecessor_apply_receipt_sha256=raw_predecessor,
    ) == capacity_apply


def test_control_abort_is_physically_bound_and_preserves_source(tmp_path):
    sources = _authorities(tmp_path / "sources")
    candidate, request = _candidate_authority_and_request(tmp_path, sources)
    decisions = _DecisionAuthority()
    execution = ExecutionReceiptReclamationParticipantV2(
        sources.executions,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=_Clock(),
    )
    prepare = execution.prepare_source_local_v2(request)
    bindings = _prepare_bindings((prepare,))
    decisions.prepares = {item.role: item for item in bindings}
    decisions.aborting = CoordinatorAbortDecisionReceiptV1.create(
        request_sha256=request.request_sha256,
        coordinator_store_identity_sha256=_sha("control-coordinator-store"),
        participant_prepare_root_sha256=digest_mapping(
            [item.as_dict() for item in bindings]
        ),
        owner_epoch=1,
        aborting_at=NOW + 2_000.0,
        abort_event_sequence=1,
        previous_event_sha256=_sha("control-abort-previous"),
        abort_event_sha256=_sha("control-abort-event"),
    )
    abort = execution.abort_source_local_v1(
        request.request_sha256,
        expected_abort_decision_sha256=(
            decisions.aborting.abort_decision_sha256
        ),
    )
    source = sources.candidates[0]
    assert len(sources.executions.load_job_receipts(
        source.acquisition_job_id
    )) == 2
    assert execution.prepare_source_local_v2(request) == prepare
    assert execution.abort_source_local_v1(
        request.request_sha256,
        expected_abort_decision_sha256=(
            decisions.aborting.abort_decision_sha256
        ),
    ) == abort
    decisions.committed = _committed(request, bindings)
    with pytest.raises(
        AcquisitionControlReclamationConflict,
        match="aborted control source",
    ):
        execution.apply_source_local_v1(
            request.request_sha256,
            predecessor_apply_receipt_sha256=_sha("lifecycle-apply-receipt"),
        )


@pytest.mark.parametrize(
    ("action", "fault_suffix", "committed"),
    (
        ("initialize", "initialize.before_commit", False),
        ("initialize", "initialize.after_commit", True),
        ("prepare", "prepare.before_commit", False),
        ("prepare", "prepare.after_commit", True),
        ("apply", "apply.after_applying", False),
        ("apply", "apply.before_commit", False),
        ("apply", "apply.after_commit", True),
        ("abort", "abort.before_commit", False),
        ("abort", "abort.after_commit", True),
    ),
)
def test_control_crash_boundaries_restart_to_one_durable_outcome(
    tmp_path,
    action,
    fault_suffix,
    committed,
):
    sources = _authorities(tmp_path / "sources")
    candidate, request = _candidate_authority_and_request(tmp_path, sources)
    decisions = _DecisionAuthority()
    fault = _FailOnce(f"execution_receipts.{fault_suffix}")
    clock = _Clock()
    if action == "initialize":
        with pytest.raises(RuntimeError, match="injected control fault"):
            ExecutionReceiptReclamationParticipantV2(
                sources.executions,
                candidate_authority=candidate,
                committed_decision_authority=decisions,
                clock=clock,
                fault_injector=fault,
            )
        recovered = ExecutionReceiptReclamationParticipantV2(
            sources.executions,
            candidate_authority=candidate,
            committed_decision_authority=decisions,
            clock=clock,
        )
        assert recovered.prepare_source_local_v2(request).role == (
            "execution_receipts"
        )
        return

    participant = ExecutionReceiptReclamationParticipantV2(
        sources.executions,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=clock,
        fault_injector=fault,
    )
    if action == "prepare":
        with pytest.raises(RuntimeError, match="injected control fault"):
            participant.prepare_source_local_v2(request)
        recovered = ExecutionReceiptReclamationParticipantV2(
            sources.executions,
            candidate_authority=candidate,
            committed_decision_authority=decisions,
            clock=clock,
        )
        durable = recovered.load_source_local_prepare_v2(
            request.request_sha256
        )
        assert (durable is not None) is committed
        assert recovered.prepare_source_local_v2(request).role == (
            "execution_receipts"
        )
        return

    prepare = participant.prepare_source_local_v2(request)
    source = sources.candidates[0]
    if action == "apply":
        _bind_committed(decisions, request, (prepare,))
        predecessor = _sha("lifecycle-apply-receipt")
        with pytest.raises(RuntimeError, match="injected control fault"):
            participant.apply_source_local_v1(
                request.request_sha256,
                predecessor_apply_receipt_sha256=predecessor,
            )
        recovered = ExecutionReceiptReclamationParticipantV2(
            sources.executions,
            candidate_authority=candidate,
            committed_decision_authority=decisions,
            clock=clock,
        )
        assert (
            recovered.load_source_local_apply_v1(request.request_sha256)
            is not None
        ) is committed
        assert bool(sources.executions.load_job_receipts(
            source.acquisition_job_id
        )) is (not committed)
        assert recovered.apply_source_local_v1(
            request.request_sha256,
            predecessor_apply_receipt_sha256=predecessor,
        ).state == "APPLIED"
        return

    _bind_aborting(decisions, request, (prepare,))
    assert decisions.aborting is not None
    with pytest.raises(RuntimeError, match="injected control fault"):
        participant.abort_source_local_v1(
            request.request_sha256,
            expected_abort_decision_sha256=(
                decisions.aborting.abort_decision_sha256
            ),
        )
    recovered = ExecutionReceiptReclamationParticipantV2(
        sources.executions,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=clock,
    )
    assert (
        recovered.load_source_local_abort_v1(request.request_sha256)
        is not None
    ) is committed
    assert len(sources.executions.load_job_receipts(
        source.acquisition_job_id
    )) == 2
    assert recovered.abort_source_local_v1(
        request.request_sha256,
        expected_abort_decision_sha256=(
            decisions.aborting.abort_decision_sha256
        ),
    ).state == "ABORTED"


def test_control_schema_trigger_and_base_ddl_tamper_fail_closed(tmp_path):
    sources = _authorities(tmp_path / "sources")
    candidate, _request = _candidate_authority_and_request(tmp_path, sources)
    decisions = _DecisionAuthority()
    execution = ExecutionReceiptReclamationParticipantV2(
        sources.executions,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=_Clock(),
    )
    scheduler = SchedulerReclamationParticipantV2(
        sources.scheduler,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=_Clock(),
    )

    _rewrite_schema_sql(
        execution.path,
        "source_reclamation_decisions",
        "candidate_count>=1",
        "candidate_count>=0",
    )
    with pytest.raises(
        AcquisitionControlReclamationIntegrityError,
        match="table definitions differ",
    ):
        _ = execution.store_instance_sha256
    _rewrite_schema_sql(
        execution.path,
        "source_reclamation_decisions",
        "candidate_count>=0",
        "candidate_count>=1",
    )
    assert len(execution.store_instance_sha256) == 64

    trigger_name = "execution_reclamation_receipt_insert_guard"
    with closing(sqlite3.connect(execution.path)) as database:
        database.execute(f"DROP TRIGGER {trigger_name}")
        database.execute(
            f"CREATE TRIGGER {trigger_name} BEFORE INSERT ON "
            "execution_receipts BEGIN SELECT 1; END"
        )
        database.commit()
    with pytest.raises(
        AcquisitionControlReclamationIntegrityError,
        match="source triggers differ",
    ):
        _ = execution.store_instance_sha256
    with closing(sqlite3.connect(execution.path)) as database:
        database.execute(f"DROP TRIGGER {trigger_name}")
        database.execute(
            reclamation._source_trigger_definitions(
                "execution_receipts"
            )[trigger_name]
        )
        database.commit()
    assert len(execution.store_instance_sha256) == 64

    _rewrite_schema_sql(
        execution.path,
        "execution_receipts",
        "terminal IN (0,1)",
        "terminal IN (0,1,2)",
    )
    with pytest.raises(
        AcquisitionControlReclamationIntegrityError,
        match="base schema fingerprint differs",
    ):
        _ = execution.store_instance_sha256

    _rewrite_schema_sql(
        scheduler.path,
        "scheduler_outbox",
        "cadence_seconds >= 1",
        "cadence_seconds >= 0",
    )
    with pytest.raises(
        AcquisitionControlReclamationIntegrityError,
        match="scheduler base schema fingerprint differs",
    ):
        _ = scheduler.store_instance_sha256


def test_control_tombstones_block_reinsert_and_replay_detects_bypass(tmp_path):
    sources = _authorities(tmp_path / "sources")
    candidate, request = _candidate_authority_and_request(tmp_path, sources)
    decisions = _DecisionAuthority()
    execution = ExecutionReceiptReclamationParticipantV2(
        sources.executions,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=_Clock(),
    )
    scheduler = SchedulerReclamationParticipantV2(
        sources.scheduler,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=_Clock(),
    )
    capacity = CapacityLedgerReclamationParticipantV2(
        sources.capacity,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=_Clock(),
    )
    execution_rows = _table_rows(execution.path, "execution_receipts")
    head_rows = _table_rows(execution.path, "terminal_heads")
    scheduler_rows = _table_rows(scheduler.path, "scheduler_outbox")
    capacity_rows = _table_rows(capacity.path, "reservations")
    prepares = tuple(item.prepare_source_local_v2(request) for item in (
        execution,
        scheduler,
        capacity,
    ))
    _bind_committed(decisions, request, prepares)
    execution_apply = execution.apply_source_local_v1(
        request.request_sha256,
        predecessor_apply_receipt_sha256=_sha("lifecycle-apply-receipt"),
    )
    scheduler.apply_source_local_v1(
        request.request_sha256,
        predecessor_apply_receipt_sha256=(
            execution_apply.apply_receipt_sha256
        ),
    )
    capacity.apply_source_local_v1(
        request.request_sha256,
        predecessor_apply_receipt_sha256=_sha("raw-apply-receipt"),
    )

    for path, table, row in (
        (execution.path, "execution_receipts", execution_rows[0]),
        (scheduler.path, "scheduler_outbox", scheduler_rows[0]),
        (capacity.path, "reservations", capacity_rows[0]),
    ):
        with closing(sqlite3.connect(path)) as database:
            with pytest.raises(sqlite3.IntegrityError, match="fenced"):
                _insert_row(database, table, row)
            database.rollback()

    definitions = reclamation._source_trigger_definitions(
        "execution_receipts"
    )
    with closing(sqlite3.connect(execution.path)) as database:
        for name in (
            "execution_reclamation_receipt_insert_guard",
            "execution_reclamation_head_insert_guard",
        ):
            database.execute(f"DROP TRIGGER {name}")
        for row in execution_rows:
            _insert_row(database, "execution_receipts", row)
        for row in head_rows:
            _insert_row(database, "terminal_heads", row)
        for name in (
            "execution_reclamation_receipt_insert_guard",
            "execution_reclamation_head_insert_guard",
        ):
            database.execute(definitions[name])
        database.commit()
    with pytest.raises(
        AcquisitionControlReclamationIntegrityError,
        match="post-apply closure differs",
    ):
        execution.load_source_local_apply_v1(request.request_sha256)


def test_capacity_prepare_reconciles_counters_with_reservation_facts(tmp_path):
    sources = _authorities(tmp_path / "sources")
    candidate, request = _candidate_authority_and_request(tmp_path, sources)
    decisions = _DecisionAuthority()
    participant = CapacityLedgerReclamationParticipantV2(
        sources.capacity,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=_Clock(),
    )
    with closing(sqlite3.connect(participant.path)) as database:
        database.execute(
            "UPDATE global_capacity SET reservation_rows=reservation_rows+1,"
            "committed_count=committed_count+1,"
            "committed_bytes=committed_bytes+1,"
            "committed_entries=committed_entries+1 WHERE singleton=1"
        )
        database.execute(
            "UPDATE provider_capacity SET reservation_rows=reservation_rows+1,"
            "committed_count=committed_count+1,"
            "committed_bytes=committed_bytes+1,"
            "committed_entries=committed_entries+1 WHERE provider='dwd'"
        )
        database.commit()
    with pytest.raises(
        AcquisitionControlReclamationIntegrityError,
        match="counters differ from reservation facts",
    ):
        participant.prepare_source_local_v2(request)


def test_control_prepare_cas_abort_release_and_stale_abort_are_isolated(
    tmp_path,
):
    sources = _authorities(tmp_path / "sources")
    candidate, request = _candidate_authority_and_request(tmp_path, sources)
    second = _second_request(candidate, request)
    decisions = _DecisionAuthority()
    participant = ExecutionReceiptReclamationParticipantV2(
        sources.executions,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=_Clock(),
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(
            executor.submit(participant.prepare_source_local_v2, request)
            for _ in range(2)
        )
        receipts = tuple(item.result(timeout=10.0) for item in futures)
    assert receipts[0] == receipts[1]
    with pytest.raises(
        AcquisitionControlReclamationConflict,
        match="fence is held",
    ):
        participant.prepare_source_local_v2(second)
    _bind_aborting(decisions, request, (receipts[0],))
    assert decisions.aborting is not None
    old_abort = participant.abort_source_local_v1(
        request.request_sha256,
        expected_abort_decision_sha256=(
            decisions.aborting.abort_decision_sha256
        ),
    )
    second_prepare = participant.prepare_source_local_v2(second)
    assert second_prepare.source_fence_sha256 != receipts[0].source_fence_sha256
    assert participant.abort_source_local_v1(
        request.request_sha256,
        expected_abort_decision_sha256=(
            decisions.aborting.abort_decision_sha256
        ),
    ) == old_abort
    assert participant.load_source_local_prepare_v2(
        second.request_sha256
    ) == second_prepare


def test_control_rejects_forged_candidate_and_committed_binding(tmp_path):
    sources = _authorities(tmp_path / "sources")
    candidate, request = _candidate_authority_and_request(tmp_path, sources)
    decisions = _DecisionAuthority()
    participant = ExecutionReceiptReclamationParticipantV2(
        sources.executions,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=_Clock(),
    )
    other = SqliteSourceReclamationCandidateAuthority.create(
        (tmp_path / "other-candidate.sqlite3").resolve(),
        source_binding_sha256=_sha("other-source-binding"),
        archive_store_identity_sha256=_sha("other-archive-identity"),
        directory_fsync=lambda _path: None,
        clock=_Clock(),
    )
    forged_request = SourceReclamationRequestV2.create(
        source_binding_sha256=request.source_binding_sha256,
        source_view_sha256=request.source_view_sha256,
        sealed_archive_anchor_sha256=request.sealed_archive_anchor_sha256,
        archive_binding_root_sha256=request.archive_binding_root_sha256,
        candidate_bindings_sha256=request.candidate_bindings_sha256,
        candidate_authority_sha256=other.authority_sha256,
        consumer_registry_root_sha256=request.consumer_registry_root_sha256,
        consumer_epoch_root_sha256=request.consumer_epoch_root_sha256,
        candidate_count=request.candidate_count,
        created_at=request.created_at,
    )
    with pytest.raises(
        AcquisitionControlReclamationConflict,
        match="candidate authority differs",
    ):
        participant.prepare_source_local_v2(forged_request)

    prepare = participant.prepare_source_local_v2(request)
    bindings = list(_prepare_bindings((prepare,)))
    index = PARTICIPANT_APPLY_ORDER.index("execution_receipts")
    bindings[index] = ParticipantPrepareBindingV1(
        role="execution_receipts",
        prepare_receipt_sha256=_sha("forged-control-prepare"),
        source_store_identity_sha256=prepare.source_store_identity_sha256,
        source_fence_sha256=prepare.source_fence_sha256,
    )
    decisions.prepares = {item.role: item for item in bindings}
    decisions.committed = _committed(request, tuple(bindings))
    with pytest.raises(
        AcquisitionControlReclamationConflict,
        match="differs from the local prepare",
    ):
        participant.apply_source_local_v1(
            request.request_sha256,
            predecessor_apply_receipt_sha256=_sha("lifecycle-apply-receipt"),
        )
    source = sources.candidates[0]
    assert len(sources.executions.load_job_receipts(
        source.acquisition_job_id
    )) == 2


def test_control_live_path_replacement_is_rejected(tmp_path):
    sources = _authorities(tmp_path / "sources")
    candidate, _request = _candidate_authority_and_request(tmp_path, sources)
    decisions = _DecisionAuthority()
    participant = ExecutionReceiptReclamationParticipantV2(
        sources.executions,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
        clock=_Clock(),
    )
    replacement = (tmp_path / "replacement-execution.sqlite3").resolve()
    shutil.copy2(participant.path, replacement)
    os.replace(replacement, participant.path)
    with pytest.raises(
        AcquisitionControlReclamationIntegrityError,
        match="path authority changed",
    ):
        _ = participant.store_instance_sha256
