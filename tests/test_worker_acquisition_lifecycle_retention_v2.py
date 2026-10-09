from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Iterator

import pytest

from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceIntegrityError,
    AcquisitionEvidenceLifecycleStore,
    CompletionWindowExpired,
    EvidenceRetentionPolicy,
)
from storm_tracker_worker.acquisition_control_reclamation import (
    CapacityLedgerReclamationParticipantV2,
    ExecutionReceiptReclamationParticipantV2,
    SchedulerReclamationParticipantV2,
)
from storm_tracker_worker.acquisition_source_reclamation_protocol import (
    CoordinatorAbortDecisionReceiptV1,
    HeldCommittedDecisionAuthority,
    PARTICIPANT_APPLY_ORDER,
    ParticipantPrepareBindingV1,
    SourceReclamationParticipantV2,
    SourceLocalPrepareReceiptV2,
    digest_mapping,
)
from storm_tracker_worker.acquisition_source_retention import (
    AcquisitionSourceArchiveSealResultV2,
    AcquisitionSourceRetentionArchiveV2,
    AcquisitionSourceRetentionCapacityError,
    AcquisitionSourceRetentionConflict,
    AcquisitionSourceRetentionIntegrityError,
    AcquisitionSourceRetentionLimitsV2,
)
from storm_tracker_worker.terminal_raw_completion_router import (
    TERMINAL_RAW_CONSUMER_CATALOG,
)

from test_worker_acquisition_control_reclamation import (
    _Clock,
    _DecisionAuthority,
    _committed,
    _prepare_bindings,
)
from test_worker_acquisition_consumer_registry import _archive_authorities
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
    _authorities,
)


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


class _FaultOnce:
    def __init__(self, point: str) -> None:
        self.point = point
        self.armed = False
        self.hit = False

    def __call__(self, point: str) -> None:
        if self.armed and not self.hit and point == self.point:
            self.hit = True
            raise RuntimeError(point)


@dataclass
class _V2Context:
    sources: Authorities
    source_archive: object
    archive: AcquisitionSourceRetentionArchiveV2
    seal: AcquisitionSourceArchiveSealResultV2 | None
    request: object | None
    closure: object
    registry: object
    registry_verifier: object
    limits: AcquisitionSourceRetentionLimitsV2
    decisions: _DecisionAuthority
    participants: tuple[SourceReclamationParticipantV2, ...]


@contextmanager
def _v2_context(
    tmp_path: Path,
    *,
    fault: _FaultOnce | None = None,
    limits: AcquisitionSourceRetentionLimitsV2 | None = None,
    seal: bool = True,
    source_candidate_count: int = 1,
) -> Iterator[_V2Context]:
    if source_candidate_count == 1:
        sources, _hot_source_archive, _plan, candidate = _exact_route_context(
            tmp_path / "authorities"
        )
    else:
        member = TERMINAL_RAW_CONSUMER_CATALOG[WORKLOAD_ID]
        sources = _authorities(
            tmp_path / "authorities" / "sources",
            count=source_candidate_count,
            consumer_id=member.consumer_id,
            stream_id=member.stream_id,
        )
        candidate = sources.candidates[0]
    archived_completions = 0
    for ordinal in range(source_candidate_count):
        rotated = sources.evidence.rotate_archive(
            policy=EvidenceRetentionPolicy(max_segment_completions=1),
            now=NOW + 120.0 + ordinal,
        )
        archived_completions += rotated.archived_completions
    assert archived_completions == source_candidate_count
    source_archive = _archive(tmp_path / "closure-source", sources)
    source_plan = source_archive.plan((candidate,), now=NOW + 200.0)
    assert source_plan.eligible_count == 1
    assert source_archive.export(source_plan).archived_jobs == 1
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
    registry, bound = _bound_registry(tmp_path / "registry-authority")
    try:
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
        selected_limits = limits or AcquisitionSourceRetentionLimitsV2(
            min_terminal_age_seconds=10.0,
        )
        archive = AcquisitionSourceRetentionArchiveV2(
            (tmp_path / "retention-v2").resolve(),
            source_archive=source_archive,
            consumer_registry_authority=registry,
            raw_store=sources.raw,
            scheduler=sources.scheduler,
            execution_store=sources.executions,
            capacity_ledger=sources.capacity,
            evidence_store=sources.evidence,
            limits=selected_limits,
            clock=lambda: NOW + 500.0,
            fault_injector=fault,
            directory_fsync=SYNC,
        )
        decisions = _DecisionAuthority()
        sources.raw.bind_reclamation_authorities_v1(
            archive,
            decisions,
        )
        sources.raw.initialize_reclamation_extension_v1()
        participants: tuple[SourceReclamationParticipantV2, ...] = (
            sources.evidence.reclamation_participant_v2(
                candidate_authority=archive,
                committed_decision_authority=decisions,
            ),
            ExecutionReceiptReclamationParticipantV2(
                sources.executions,
                candidate_authority=archive,
                committed_decision_authority=decisions,
                clock=_Clock(NOW + 800.0),
            ),
            SchedulerReclamationParticipantV2(
                sources.scheduler,
                candidate_authority=archive,
                committed_decision_authority=decisions,
                clock=_Clock(NOW + 800.0),
            ),
            sources.raw,
            CapacityLedgerReclamationParticipantV2(
                sources.capacity,
                candidate_authority=archive,
                committed_decision_authority=decisions,
                clock=_Clock(NOW + 800.0),
            ),
        )
        archive.bind_source_participants_v2(participants)
        result = None
        request = None
        if seal:
            result = archive.seal_candidates_v2(
                (candidate,),
                consumer_closures=(closure,),
                now=NOW + 500.0,
            )
            request = archive.create_reclamation_request_v2(
                result.candidate_bindings_sha256,
                source_binding_sha256=_sha("retention-v2-source-binding"),
                source_view_sha256=_sha("retention-v2-source-view"),
                created_at=NOW + 600.0,
            )
        yield _V2Context(
            sources=sources,
            source_archive=source_archive,
            archive=archive,
            seal=result,
            request=request,
            closure=closure,
            registry=registry,
            registry_verifier=bound,
            limits=selected_limits,
            decisions=decisions,
            participants=participants,
        )
    finally:
        registry.close()


def _reopen_archive(context: _V2Context) -> AcquisitionSourceRetentionArchiveV2:
    reopened = AcquisitionSourceRetentionArchiveV2(
        context.archive.root,
        source_archive=context.source_archive,
        consumer_registry_authority=context.registry,
        raw_store=context.sources.raw,
        scheduler=context.sources.scheduler,
        execution_store=context.sources.executions,
        capacity_ledger=context.sources.capacity,
        evidence_store=context.sources.evidence,
        limits=context.limits,
        clock=lambda: NOW + 700.0,
        directory_fsync=SYNC,
    )
    reopened.bind_source_participants_v2(context.participants)
    return reopened


def _participant(context: _V2Context, decisions: HeldCommittedDecisionAuthority):
    assert decisions is context.decisions
    context.sources.evidence._clock = _Clock(NOW + 1_000.0)
    return context.sources.evidence.reclamation_participant_v2(
        candidate_authority=context.archive,
        committed_decision_authority=decisions,
    )


def _successor_request(context: _V2Context):
    candidate = context.sources.candidates[0]
    catalog, reproof = _archive_authorities("lifecycle-abort-successor")
    epoch = context.registry.ensure_epoch(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    )
    bound = context.registry.verifier(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    ).bind_current_epoch(
        claimed_epoch_root_sha256=epoch.epoch_root_sha256,
    )
    downstream = context.sources.evidence.load_terminal_consumption(
        candidate.evidence_stream_id,
        candidate.evidence_stream_sequence,
    )
    identity = context.sources.evidence.terminal_event_identity(
        candidate.evidence_stream_id,
        candidate.evidence_stream_sequence,
    )
    assert downstream is not None and downstream.status == "archived"
    assert identity is not None and identity.status == "archived"
    closure = bound.issue_consumer_closure(
        workload_id=WORKLOAD_ID,
        candidate_sha256=candidate.candidate_sha256,
        acquisition_job_id=candidate.acquisition_job_id,
        producer_receipt=downstream.producer_receipt,
        consumption_receipt=downstream.consumption_receipt,
        terminal_identity=identity,
        source_archive_anchor_sha256=(
            context.closure.source_archive_anchor_sha256
        ),
        source_archive_segment_sha256=(
            context.closure.source_archive_segment_sha256
        ),
        source_archive_record_sha256=(
            context.closure.source_archive_record_sha256
        ),
    )
    seal = context.archive.seal_candidates_v2(
        (candidate,),
        consumer_closures=(closure,),
        now=NOW + 4_000.0,
    )
    return context.archive.create_reclamation_request_v2(
        seal.candidate_bindings_sha256,
        source_binding_sha256=_sha("lifecycle-abort-successor-binding"),
        source_view_sha256=_sha("lifecycle-abort-successor-view"),
        created_at=NOW + 4_001.0,
    )


def test_retention_v2_is_concrete_pages_are_metadata_only_and_restart_audits(
    tmp_path,
):
    with _v2_context(tmp_path) as context:
        assert context.request is not None and context.seal is not None
        audit = context.archive.audit_v2()
        assert audit["healthy"] is True
        assert audit["archive_generation"] == 1
        with context.archive.hold_candidate_page_v1(
            context.request,
            after_cursor=0,
            max_candidates=64,
        ) as page:
            assert page.exhausted is True
            assert page.total_candidate_count == 1
            encoded = json.dumps(page.decoded_records(), sort_keys=True)
            for forbidden in (
                "artifact_path",
                "payload_base64",
                "raw_bytes",
                str(tmp_path),
            ):
                assert forbidden not in encoded
        proof = context.archive.prove_anchor_ancestor_v2(
            context.seal.archive_anchor_sha256
        )
        assert proof.ancestor_anchor_sha256 == proof.descendant_anchor_sha256
        reopened = _reopen_archive(context)
        assert reopened.audit_v2()["archive_anchor_sha256"] == (
            context.seal.archive_anchor_sha256
        )


def test_retention_v2_participant_binding_is_exact_and_create_once(tmp_path):
    with _v2_context(tmp_path) as context:
        expected_root = context.archive.audit_v2()[
            "source_store_set_root_sha256"
        ]
        assert (
            context.archive.bind_source_participants_v2(context.participants)
            == expected_root
        )
        replacement = ExecutionReceiptReclamationParticipantV2(
            context.sources.executions,
            candidate_authority=context.archive,
            committed_decision_authority=context.decisions,
            clock=_Clock(NOW + 900.0),
        )
        rebound = (
            context.participants[0],
            replacement,
            *context.participants[2:],
        )
        with pytest.raises(
            AcquisitionSourceRetentionConflict,
            match="already bound",
        ):
            context.archive.bind_source_participants_v2(rebound)


def test_lifecycle_v4_prepare_apply_and_restart_reclaim_complete_prefix(tmp_path):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        decisions = context.decisions
        participant = _participant(context, decisions)
        prepared = participant.prepare_source_local_v2(context.request)
        bindings = _prepare_bindings((prepared,))
        decisions.prepares = {item.role: item for item in bindings}
        decisions.committed = _committed(context.request, bindings)
        archived_files = tuple(
            context.sources.evidence.archive_segment_directory.iterdir()
        ) + tuple(context.sources.evidence.archive_object_directory.iterdir())
        assert archived_files

        applied = participant.apply_source_local_v1(
            context.request.request_sha256,
            predecessor_apply_receipt_sha256=None,
        )

        assert applied.released_entries == 1
        assert all(not path.exists() for path in archived_files)
        expired = context.sources.evidence.lookup_sequence(WORKLOAD_ID, 1)
        assert expired.status == "window_expired"
        with pytest.raises(CompletionWindowExpired, match="reclaimed"):
            context.sources.evidence.load_terminal_consumption(WORKLOAD_ID, 1)
        assert context.sources.evidence.audit_archive()["healthy"] is True
        reopened_store = AcquisitionEvidenceLifecycleStore(
            context.sources.evidence.root,
            clock=_Clock(NOW + 2_000.0),
            directory_fsync=SYNC,
        )
        replay = reopened_store.reclamation_participant_v2(
            candidate_authority=context.archive,
            committed_decision_authority=decisions,
        )
        assert replay.load_source_local_prepare_v2(
            context.request.request_sha256
        ) == prepared
        assert replay.apply_source_local_v1(
            context.request.request_sha256,
            predecessor_apply_receipt_sha256=None,
        ) == applied


@pytest.mark.parametrize(
    "point",
    [
        "reclamation.apply.after_logical_commit",
        "reclamation.apply.after_file_unlink",
    ],
)
def test_lifecycle_v4_apply_crash_rolls_forward_after_restart(tmp_path, point):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        decisions = context.decisions
        participant = _participant(context, decisions)
        prepared = participant.prepare_source_local_v2(context.request)
        bindings = _prepare_bindings((prepared,))
        decisions.prepares = {item.role: item for item in bindings}
        decisions.committed = _committed(context.request, bindings)
        fault = _FaultOnce(point)
        fault.armed = True
        context.sources.evidence._fault_injector = fault
        with pytest.raises(RuntimeError, match=point):
            participant.apply_source_local_v1(
                context.request.request_sha256,
                predecessor_apply_receipt_sha256=None,
            )
        assert fault.hit
        context.sources.evidence._fault_injector = None
        reopened_store = AcquisitionEvidenceLifecycleStore(
            context.sources.evidence.root,
            clock=_Clock(NOW + 2_000.0),
            directory_fsync=SYNC,
        )
        resumed = reopened_store.reclamation_participant_v2(
            candidate_authority=context.archive,
            committed_decision_authority=decisions,
        ).apply_source_local_v1(
            context.request.request_sha256,
            predecessor_apply_receipt_sha256=None,
        )
        assert resumed.released_entries == 1
        assert reopened_store.audit_archive()["healthy"] is True


def test_lifecycle_v4_abort_is_durable_and_mutation_free(tmp_path):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        decisions = context.decisions
        participant = _participant(context, decisions)
        prepared = participant.prepare_source_local_v2(context.request)
        bindings = _prepare_bindings((prepared,))
        decisions.prepares = {item.role: item for item in bindings}
        decisions.aborting = CoordinatorAbortDecisionReceiptV1.create(
            request_sha256=context.request.request_sha256,
            coordinator_store_identity_sha256=_sha("lifecycle-coordinator"),
            participant_prepare_root_sha256=digest_mapping(
                [item.as_dict() for item in bindings]
            ),
            owner_epoch=1,
            aborting_at=NOW + 2_000.0,
            abort_event_sequence=1,
            previous_event_sha256=_sha("lifecycle-abort-previous"),
            abort_event_sha256=_sha("lifecycle-abort-event"),
        )
        before = context.sources.evidence.audit_archive()
        aborted = participant.abort_source_local_v1(
            context.request.request_sha256,
            expected_abort_decision_sha256=(
                decisions.aborting.abort_decision_sha256
            ),
        )
        assert context.sources.evidence.audit_archive() == before
        reopened = AcquisitionEvidenceLifecycleStore(
            context.sources.evidence.root,
            clock=_Clock(NOW + 3_000.0),
            directory_fsync=SYNC,
        ).reclamation_participant_v2(
            candidate_authority=context.archive,
            committed_decision_authority=decisions,
        )
        assert reopened.load_source_local_abort_v1(
            context.request.request_sha256
        ) == aborted
        assert reopened.prepare_source_local_v2(context.request) == prepared


def test_lifecycle_v4_aborted_request_keeps_journal_and_successor_applies(
    tmp_path,
):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        decisions = context.decisions
        participant = _participant(context, decisions)
        first_prepared = participant.prepare_source_local_v2(context.request)
        first_bindings = _prepare_bindings((first_prepared,))
        decisions.prepares = {
            item.role: item for item in first_bindings
        }
        decisions.aborting = CoordinatorAbortDecisionReceiptV1.create(
            request_sha256=context.request.request_sha256,
            coordinator_store_identity_sha256=_sha(
                "lifecycle-successor-coordinator"
            ),
            participant_prepare_root_sha256=digest_mapping(
                [item.as_dict() for item in first_bindings]
            ),
            owner_epoch=1,
            aborting_at=NOW + 3_000.0,
            abort_event_sequence=1,
            previous_event_sha256=_sha("lifecycle-successor-abort-previous"),
            abort_event_sha256=_sha("lifecycle-successor-abort-event"),
        )
        participant.abort_source_local_v1(
            context.request.request_sha256,
            expected_abort_decision_sha256=(
                decisions.aborting.abort_decision_sha256
            ),
        )

        successor_request = _successor_request(context)
        successor_prepared = participant.prepare_source_local_v2(
            successor_request
        )
        successor_bindings = _prepare_bindings((successor_prepared,))
        decisions.prepares = {
            item.role: item for item in successor_bindings
        }
        decisions.committed = _committed(
            successor_request,
            successor_bindings,
        )
        applied = participant.apply_source_local_v1(
            successor_request.request_sha256,
            predecessor_apply_receipt_sha256=None,
        )
        assert applied.released_entries == 1

        with closing(sqlite3.connect(context.sources.evidence.path)) as database:
            rows = database.execute(
                "SELECT decision.state,reference.request_sha256,"
                "reference.content_sha256,reference.effect_binding_sha256,"
                "reference.reference_sha256 FROM "
                "lifecycle_reclamation_object_references AS reference JOIN "
                "lifecycle_reclamation_decisions AS decision "
                "USING(request_sha256) ORDER BY decision.fence_epoch"
            ).fetchall()
        assert [row[0] for row in rows] == ["ABORTED", "APPLIED"]
        assert rows[0][1] == context.request.request_sha256
        assert rows[1][1] == successor_request.request_sha256
        assert rows[0][2] == rows[1][2]
        assert rows[0][3] != rows[1][3]
        assert rows[0][4] != rows[1][4]


@pytest.mark.parametrize(
    "point,expected_generation",
    [
        ("retention_v2.seal.after_target.objects", 0),
        ("retention_v2.seal.after_head", 1),
    ],
)
def test_retention_v2_crash_recovery_is_old_or_new_and_retryable(
    tmp_path,
    point,
    expected_generation,
):
    fault = _FaultOnce(point)
    limits = AcquisitionSourceRetentionLimitsV2(
        min_terminal_age_seconds=10.0,
        max_archive_segments=1,
    )
    with _v2_context(
        tmp_path,
        fault=fault,
        limits=limits,
        seal=False,
    ) as context:
        fault.armed = True
        with pytest.raises(RuntimeError, match=point):
            context.archive.seal_candidates_v2(
                context.sources.candidates,
                consumer_closures=(context.closure,),
                now=NOW + 500.0,
            )
        reopened = _reopen_archive(context)
        assert reopened.audit_v2()["archive_generation"] == expected_generation
        result = reopened.seal_candidates_v2(
            context.sources.candidates,
            consumer_closures=(context.closure,),
            now=NOW + 501.0,
        )
        assert result.status == "sealed"
        assert reopened.audit_v2()["archive_generation"] == 1


def test_retention_v2_tamper_and_capacity_fail_closed(tmp_path):
    with _v2_context(tmp_path / "tamper") as context:
        reference = next(context.archive.reference_directory.iterdir())
        payload = bytearray(reference.read_bytes())
        payload[len(payload) // 2] ^= 1
        reference.write_bytes(payload)
        with pytest.raises(AcquisitionSourceRetentionIntegrityError):
            context.archive.audit_v2()
        with pytest.raises(AcquisitionSourceRetentionIntegrityError):
            with context.archive.hold_candidate_page_v1(
                context.request,
                after_cursor=0,
                max_candidates=64,
            ):
                pass

    limits = AcquisitionSourceRetentionLimitsV2(
        min_terminal_age_seconds=10.0,
        max_total_archive_bytes=4096,
    )
    with _v2_context(
        tmp_path / "capacity",
        limits=limits,
        seal=False,
    ) as context:
        old_head = context.archive.audit_v2()["archive_anchor_sha256"]
        with pytest.raises(AcquisitionSourceRetentionCapacityError):
            context.archive.seal_candidates_v2(
                context.sources.candidates,
                consumer_closures=(context.closure,),
                now=NOW + 500.0,
            )
        assert context.archive.audit_v2()["archive_anchor_sha256"] == old_head


def test_lifecycle_v3_to_v4_migration_preserves_identity_and_detects_trigger_tamper(
    tmp_path,
):
    sources, _archive, _plan, candidate = _exact_route_context(tmp_path)
    store = sources.evidence
    identity_before = store.identity_sha256
    terminal_before = store.terminal_event_identity(
        candidate.evidence_stream_id,
        candidate.evidence_stream_sequence,
    )
    with closing(sqlite3.connect(store.path)) as database:
        for trigger in store._reclamation_trigger_definitions():
            database.execute(f'DROP TRIGGER IF EXISTS "{trigger}"')
        database.execute("DROP TRIGGER IF EXISTS lifecycle_schema_no_update")
        database.execute("DROP TRIGGER IF EXISTS lifecycle_schema_no_delete")
        database.execute(
            "DROP TABLE lifecycle_reclamation_object_references"
        )
        database.execute("DROP TABLE lifecycle_reclamation_files")
        database.execute("DROP TABLE lifecycle_reclamation_members")
        database.execute("DROP TABLE lifecycle_reclamation_decisions")
        database.execute("DROP TABLE lifecycle_reclamation_meta")
        database.execute("DROP TABLE lifecycle_archive_floor")
        database.execute(
            "UPDATE lifecycle_schema SET schema_version=3 WHERE singleton=1"
        )
        database.commit()
    migrated = AcquisitionEvidenceLifecycleStore(
        store.root,
        clock=_Clock(NOW + 1_000.0),
        directory_fsync=SYNC,
    )
    assert migrated.identity_sha256 == identity_before
    assert migrated.terminal_event_identity(
        candidate.evidence_stream_id,
        candidate.evidence_stream_sequence,
    ) == terminal_before
    with closing(sqlite3.connect(migrated.path)) as database:
        database.execute(
            "DROP TRIGGER lifecycle_reclamation_members_no_update"
        )
        database.execute(
            "CREATE TRIGGER lifecycle_reclamation_members_no_update "
            "BEFORE UPDATE ON lifecycle_reclamation_members BEGIN SELECT 1; END"
        )
        database.commit()
    with pytest.raises(
        AcquisitionEvidenceIntegrityError,
        match="trigger authority differs",
    ):
        AcquisitionEvidenceLifecycleStore(
            store.root,
            clock=_Clock(NOW + 2_000.0),
            directory_fsync=SYNC,
        )
