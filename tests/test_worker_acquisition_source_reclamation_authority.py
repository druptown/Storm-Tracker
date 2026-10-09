from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from storm_tracker_worker import acquisition_source_reclamation_authority as reclamation

from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
)
from storm_tracker_worker.acquisition_execution_store import ExecutionReceiptStore
from storm_tracker_worker.acquisition_source_reclamation_authority import (
    APPLY_BLOCKERS,
    PREPARED,
    AcquisitionSourceReclamationApplyDisabled,
    AcquisitionSourceReclamationCapacityError,
    AcquisitionSourceReclamationConflict,
    AcquisitionSourceReclamationCoordinator,
    AcquisitionSourceReclamationIntegrityError,
    COORDINATOR_CONTRACT,
    CONTRACT,
    CROSS_STORE_REQUIREMENTS,
    REASON_CODE,
    ReclamationCoordinatorLimits,
    ReclamationPrepareRequest,
    REQUIRED_STORE_CAPABILITIES,
    SCHEMA,
    AcquisitionSourceReclamationAuthorityError,
    build_fail_closed_reclamation_authority,
    capability_catalog_mapping,
    capability_catalog_sha256,
    parse_fail_closed_reclamation_authority,
    validate_fail_closed_reclamation_authority,
)
from storm_tracker_worker.autonomous_scheduler import AutonomousScheduler
from storm_tracker_worker.raw_artifacts import RawArtifactStore
from storm_tracker_worker.raw_capacity_ledger import RawCapacityLedger

from test_worker_acquisition_source_retention import (
    NOW,
    _archive,
    _authorities,
)


def _authority(tmp_path, *, consume: bool = True):
    sources = _authorities(tmp_path, consume=consume)
    archive = _archive(tmp_path, sources)
    plan = archive.plan(sources.candidates, now=NOW + 100.0)
    if plan.eligible_count:
        archive.export(plan)
    return (
        build_fail_closed_reclamation_authority(plan, archive),
        sources,
        archive,
        plan,
    )


def test_real_sealed_export_yields_only_content_addressed_refusal(tmp_path):
    authority, sources, archive, plan = _authority(tmp_path)
    assert authority.status == "withheld"
    assert authority.reason_code == REASON_CODE
    assert authority.reclaim_authorized is False
    assert authority.source_release_performed is False
    assert authority.candidate_count == 1
    assert authority.sealed_export_eligible_count == 1
    assert authority.withheld_count == 0
    assert authority.sealed_archive_record_count == 1
    assert authority.retention_plan_sha256 == plan.plan_sha256
    assert authority.sealed_archive_anchor_sha256 == (
        archive.audit()["archive_anchor_sha256"]
    )
    assert authority.capability_catalog_sha256 == capability_catalog_sha256()
    assert parse_fail_closed_reclamation_authority(
        authority.to_json_bytes()
    ) == authority
    assert all(path.exists() for path in sources.object_paths)


def test_unconsumed_candidate_is_explicitly_withheld_and_never_exported(tmp_path):
    authority, _sources, archive, _plan = _authority(tmp_path, consume=False)
    assert authority.candidate_count == 1
    assert authority.sealed_export_eligible_count == 0
    assert authority.withheld_count == 1
    assert authority.sealed_archive_record_count == 0
    assert archive.audit()["reclaim_enabled"] is False


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("reclaim_authorized", True),
        ("source_release_performed", True),
        ("status", "eligible"),
        ("reason_code", "all_authorities_cross_bound"),
        ("candidate_count", 2),
        ("capability_catalog_sha256", "f" * 64),
        ("authority_sha256", "e" * 64),
    ),
)
def test_dataclass_forgery_is_rejected(tmp_path, field, value):
    authority, *_ = _authority(tmp_path)
    with pytest.raises(AcquisitionSourceReclamationAuthorityError):
        validate_fail_closed_reclamation_authority(
            replace(authority, **{field: value})
        )


def test_parser_rejects_noncanonical_or_catalog_tampered_json(tmp_path):
    authority, *_ = _authority(tmp_path)
    payload = authority.to_json_bytes()
    with pytest.raises(
        AcquisitionSourceReclamationAuthorityError,
        match="not canonical",
    ):
        parse_fail_closed_reclamation_authority(payload + b"\n")

    value = json.loads(payload)
    value["capability_catalog"]["positive_authority_implemented"] = True
    tampered = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    with pytest.raises(
        AcquisitionSourceReclamationAuthorityError,
        match="shape or catalogue",
    ):
        parse_fail_closed_reclamation_authority(tampered)


def test_capability_catalog_is_exact_bounded_private_and_apis_are_really_absent():
    catalog = capability_catalog_mapping()
    assert catalog["positive_authority_implemented"] is False
    assert catalog["source_mutation_implemented"] is False
    assert catalog["participant_prepare_sidecars_implemented"] is True
    assert catalog["exact_archive_resolvers_implemented"] is True
    assert catalog["consumer_closure_receipts_implemented"] is True
    assert catalog["consumer_registry_epoch_root_verifier_implemented"] is True
    assert catalog["participant_source_apply_apis_implemented"] is False
    assert catalog["apply_hard_disabled"] is True
    assert tuple(catalog["cross_store_requirements"]) == CROSS_STORE_REQUIREMENTS
    assert [item["role"] for item in catalog["participants"]] == [
        item.role for item in REQUIRED_STORE_CAPABILITIES
    ]
    assert len(json.dumps(catalog, sort_keys=True).encode("utf-8")) < 16 * 1024
    assert len(capability_catalog_sha256()) == 64

    classes = {
        "capacity_ledger": RawCapacityLedger,
        "downstream_lifecycle": AcquisitionEvidenceLifecycleStore,
        "execution_receipts": ExecutionReceiptStore,
        "raw_artifacts": RawArtifactStore,
        "scheduler": AutonomousScheduler,
    }
    for gap in REQUIRED_STORE_CAPABILITIES:
        participant = classes[gap.role]
        assert all(hasattr(participant, name) for name in gap.present_evidence_apis)
        assert not hasattr(participant, gap.required_prepare_api)
        assert not hasattr(participant, gap.required_apply_api)
        assert not hasattr(participant, gap.required_resolver_api)
        assert hasattr(
            reclamation.ReclamationParticipantPreparer,
            gap.required_prepare_api,
        )
        assert hasattr(
            reclamation.ExactArchivedReclamationResolver,
            gap.required_resolver_api,
        )
        assert not hasattr(
            reclamation.ReclamationParticipantPreparer,
            gap.required_apply_api,
        )

    encoded = json.dumps(catalog, sort_keys=True)
    for forbidden in ("zone.home", "acquisition-job", "latitude", "longitude"):
        assert forbidden not in encoded


def test_concurrent_read_only_builds_are_identical_and_leave_anchor_unchanged(
    tmp_path,
):
    authority, _sources, archive, plan = _authority(tmp_path)
    before = archive.audit()["archive_anchor_sha256"]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = tuple(
            pool.map(
                lambda _index: build_fail_closed_reclamation_authority(
                    plan, archive
                ),
                range(32),
            )
        )
    assert {item.authority_sha256 for item in results} == {
        authority.authority_sha256
    }
    assert archive.audit()["archive_anchor_sha256"] == before


def test_foreign_archive_cannot_validate_a_plan_from_other_source_authorities(
    tmp_path,
):
    first = _authorities(tmp_path / "first")
    first_archive = _archive(tmp_path / "first", first)
    plan = first_archive.plan(first.candidates, now=NOW + 100.0)
    second = _authorities(tmp_path / "second")
    second_archive = _archive(tmp_path / "second", second)
    with pytest.raises(Exception, match="source authority"):
        build_fail_closed_reclamation_authority(plan, second_archive)


def test_contract_is_distinct_from_positive_reclamation_or_sealed_export():
    assert CONTRACT == "stv3-worker-acquisition-source-reclamation-gap-authority-v1"
    assert SCHEMA == "stv3-worker-acquisition-source-reclamation-eligibility-v1"
    assert "retention" not in CONTRACT
    assert REASON_CODE == "cross_store_atomic_prune_contract_unavailable"


def test_gap_authority_has_no_mutator_and_is_not_live_wired():
    root = Path(__file__).resolve().parents[1]
    module = (
        root
        / "storm_tracker_worker"
        / "acquisition_source_reclamation_authority.py"
    ).read_text(encoding="utf-8")
    compact = "".join(module.lower().split())
    for forbidden in (
        ".unlink(",
        ".rmdir(",
        "os.remove(",
        "os.replace(",
        "shutil.rmtree(",
        'execute("delete',
        "execute('delete",
        "vacuum",
    ):
        assert forbidden not in compact
    for runtime in (
        "service.py",
        "worker_first_composition.py",
        "worker_first_service_composition.py",
    ):
        source = (
            root / "storm_tracker_worker" / runtime
        ).read_text(encoding="utf-8")
        assert "acquisition_source_reclamation_authority" not in source


def _coordinator_context(
    tmp_path,
    *,
    count=1,
    clock=lambda: NOW + 200.0,
    fault=None,
):
    sources = _authorities(tmp_path / "sources", count=count)
    archive = _archive(tmp_path / "sources", sources)
    plan = archive.plan(sources.candidates, now=NOW + 100.0)
    result = archive.export(plan)
    assert result.archived_jobs == count
    coordinator = AcquisitionSourceReclamationCoordinator(
        (tmp_path / "reclamation").resolve(),
        archive=archive,
        clock=clock,
        fault_injector=fault,
        directory_fsync=lambda _path: None,
    )
    return sources, archive, plan, coordinator


def _source_hashes(sources):
    paths = (
        sources.raw.catalog_path,
        sources.scheduler.path,
        sources.executions.path,
        sources.capacity.path,
        sources.evidence.path,
        *sources.object_paths,
    )
    return {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in paths
    }


def test_real_coordinator_prepares_all_participants_but_cannot_commit_or_apply(
    tmp_path,
):
    sources, _archive_value, plan, coordinator = _coordinator_context(tmp_path)
    before = _source_hashes(sources)
    outcome = coordinator.prepare(plan)
    assert outcome.state == PREPARED
    assert outcome.reclaim_authorized is False
    assert outcome.source_release_performed is False
    assert outcome.blockers == APPLY_BLOCKERS
    audit = coordinator.audit()
    assert audit["prepared_count"] == 1
    assert audit["preparing_count"] == 0
    assert audit["participant_count"] == 5
    assert audit["event_count"] == 7
    assert audit["apply_enabled"] is False
    assert _source_hashes(sources) == before
    for operation in (coordinator.commit, coordinator.apply):
        with pytest.raises(AcquisitionSourceReclamationApplyDisabled) as error:
            operation(outcome.request_sha256)
        assert error.value.as_dict()["blockers"] == list(APPLY_BLOCKERS)
        assert error.value.as_dict()["source_release_performed"] is False
    assert _source_hashes(sources) == before


def test_coordinator_root_cannot_overlap_any_source_or_archive_authority(
    tmp_path,
):
    sources = _authorities(tmp_path / "sources")
    archive = _archive(tmp_path / "sources", sources)
    forbidden = (
        archive.root / "coordinator",
        sources.raw.root / "coordinator",
        sources.evidence.root / "coordinator",
        tmp_path / "sources",
    )
    for root in forbidden:
        marker = root / "reclamation_coordinator.sqlite3"
        with pytest.raises(
            AcquisitionSourceReclamationIntegrityError,
            match="(overlaps|contains)",
        ):
            AcquisitionSourceReclamationCoordinator(
                root.resolve(),
                archive=archive,
                directory_fsync=lambda _path: None,
            )
        assert not marker.exists()

def test_exact_archive_resolvers_return_every_participant_authority(tmp_path):
    sources, archive, plan, coordinator = _coordinator_context(tmp_path)
    outcome = coordinator.prepare(plan)
    candidate = sources.candidates[0].candidate_sha256
    resolver = coordinator.resolver(outcome.request_sha256)
    lookup = archive.lookup(candidate)
    assert lookup.record is not None
    assert resolver.resolve_archived_capacity_commit_v1(candidate) == (
        lookup.record["capacity_commit"]
    )
    assert resolver.resolve_archived_source_reference_closure_v1(candidate) == (
        lookup.record["downstream"]
    )
    assert list(resolver.resolve_archived_execution_history_v1(candidate)) == (
        lookup.record["execution_history"]
    )
    assert resolver.resolve_archived_scheduler_terminal_v1(candidate) == (
        lookup.record["scheduler_terminal"]
    )
    raw = resolver.resolve_archived_capture_receipt_object_v1(candidate)
    assert raw["raw_capture"] == lookup.record["raw_capture"]
    assert raw["raw_receipts"] == lookup.record["raw_receipts"]
    assert raw["raw_objects"] == lookup.record["raw_objects"]


@pytest.mark.parametrize(
    "fault_point",
    (
        "coordinator.after_preparing_commit",
        "participant.capacity_ledger.after_commit",
        "coordinator.after_capacity_ledger_prepare",
        "coordinator.after_capacity_ledger_journal_commit",
        "participant.downstream_lifecycle.after_commit",
        "coordinator.after_downstream_lifecycle_journal_commit",
        "participant.execution_receipts.after_commit",
        "coordinator.after_execution_receipts_journal_commit",
        "participant.raw_artifacts.after_commit",
        "coordinator.after_raw_artifacts_journal_commit",
        "participant.scheduler.after_commit",
        "coordinator.after_scheduler_journal_commit",
        "coordinator.after_prepared_commit",
    ),
)
def test_restart_resumes_idempotently_after_every_durable_prepare_boundary(
    tmp_path, fault_point,
):
    fired = False

    def fault(point):
        nonlocal fired
        if point == fault_point and not fired:
            fired = True
            raise RuntimeError(point)

    _sources, archive, plan, coordinator = _coordinator_context(
        tmp_path, fault=fault
    )
    with pytest.raises(RuntimeError, match=fault_point.replace(".", r"\.")):
        coordinator.prepare(plan)
    assert fired
    restarted = AcquisitionSourceReclamationCoordinator(
        coordinator.root,
        archive=archive,
        clock=lambda: NOW + 201.0,
        directory_fsync=lambda _path: None,
    )
    outcome = restarted.prepare(plan)
    assert outcome.state == PREPARED
    assert restarted.audit()["prepared_count"] == 1


def test_concurrent_coordinators_are_os_fenced_and_converge(tmp_path):
    _sources, archive, plan, first = _coordinator_context(tmp_path)
    second = AcquisitionSourceReclamationCoordinator(
        first.root,
        archive=archive,
        clock=lambda: NOW + 200.0,
        directory_fsync=lambda _path: None,
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = tuple(pool.map(lambda item: item.prepare(plan), (first, second)))
    assert len({item.request_sha256 for item in outcomes}) == 1
    assert len({item.decision_sha256 for item in outcomes}) == 1
    audit = first.audit()
    assert audit["decision_count"] == 1
    assert audit["event_count"] == 7


@pytest.mark.parametrize("mutation", ("hash", "gap"))
def test_event_tamper_and_gap_are_detected(tmp_path, mutation):
    _sources, _archive_value, plan, coordinator = _coordinator_context(tmp_path)
    coordinator.prepare(plan)
    import sqlite3

    database = sqlite3.connect(coordinator.path)
    try:
        if mutation == "hash":
            database.execute(
                "UPDATE reclamation_events SET event_sha256=? "
                "WHERE event_sequence=2",
                ("f" * 64,),
            )
        else:
            database.execute(
                "DELETE FROM reclamation_events WHERE event_sequence=2"
            )
        database.commit()
    finally:
        database.close()
    with pytest.raises(
        AcquisitionSourceReclamationIntegrityError,
        match="event (chain|sequence)",
    ):
        coordinator.audit()


def test_unknown_coordinator_schema_object_is_rejected(tmp_path):
    _sources, archive, _plan, coordinator = _coordinator_context(tmp_path)
    import sqlite3

    database = sqlite3.connect(coordinator.path)
    try:
        database.execute("CREATE TABLE injected_tamper(value TEXT)")
        database.commit()
    finally:
        database.close()
    with pytest.raises(
        AcquisitionSourceReclamationIntegrityError,
        match="schema differs",
    ):
        AcquisitionSourceReclamationCoordinator(
            coordinator.root,
            archive=archive,
            clock=lambda: NOW + 201.0,
            directory_fsync=lambda _path: None,
        )


def test_partial_archive_is_rejected_before_any_participant_prepare(tmp_path):
    _sources, archive, plan, coordinator = _coordinator_context(tmp_path)
    disposition = plan.dispositions[0]
    lookup = archive.lookup(disposition.candidate_sha256)
    assert lookup.segment_sha256 is not None
    segment = archive.segment_directory / (
        f"{lookup.segment_sha256}.sealed.json"
    )
    segment.unlink()
    with pytest.raises(Exception):
        coordinator.prepare(plan)
    audit = coordinator.audit()
    assert audit["decision_count"] == 0
    assert audit["participant_count"] == 0


def test_participant_disk_bound_fails_closed_without_source_changes(
    tmp_path, monkeypatch,
):
    sources, _archive_value, plan, coordinator = _coordinator_context(tmp_path)
    before = _source_hashes(sources)
    journal = coordinator._participants._journals["capacity_ledger"]
    physical_bytes = journal._physical_bytes
    monkeypatch.setattr(
        journal,
        "_physical_bytes",
        lambda: coordinator._limits.max_participant_database_bytes + 1,
    )
    with pytest.raises(AcquisitionSourceReclamationCapacityError):
        coordinator.prepare(plan)
    assert _source_hashes(sources) == before
    monkeypatch.setattr(journal, "_physical_bytes", physical_bytes)
    assert coordinator.audit()["preparing_count"] == 1


def test_clock_rollback_is_rejected_on_restart(tmp_path):
    _sources, archive, plan, coordinator = _coordinator_context(tmp_path)
    coordinator.prepare(plan)
    rollback = AcquisitionSourceReclamationCoordinator(
        coordinator.root,
        archive=archive,
        clock=lambda: NOW + 199.0,
        directory_fsync=lambda _path: None,
    )
    with pytest.raises(
        AcquisitionSourceReclamationConflict,
        match="clock_rollback",
    ):
        rollback.prepare(plan)


def test_shared_raw_children_are_reference_counted_and_not_releasable(
    tmp_path,
):
    sources, _archive_value, plan, coordinator = _coordinator_context(tmp_path)
    candidate_capture = sources.raw.load_capture(
        sources.candidates[0].raw_capture_id
    )
    assert candidate_capture is not None
    receipt_id = candidate_capture.members[0].receipt_id
    sources.raw.commit_capture(
        provider=candidate_capture.provider,
        product="shared-retention-test-capture",
        fetch_id="shared-noncandidate-capture",
        upstream_capture_id="shared-noncandidate-capture",
        product_timestamp=NOW - 600.0,
        members={"rv_tar": receipt_id},
    )
    outcome = coordinator.prepare(plan)
    receipt = coordinator._participants.load(
        role="raw_artifacts",
        request_sha256=outcome.request_sha256,
    )
    assert receipt is not None
    assert receipt.releasable_entries == 0
    assert receipt.releasable_bytes == 0


def test_thousands_of_disabled_apply_replays_have_constant_journal_footprint(
    tmp_path,
):
    _sources, _archive_value, plan, coordinator = _coordinator_context(tmp_path)
    outcome = coordinator.prepare(plan)
    before = coordinator.audit()
    for _index in range(2000):
        with pytest.raises(AcquisitionSourceReclamationApplyDisabled):
            coordinator.apply(outcome.request_sha256)
    after = coordinator.audit()
    assert after["decision_count"] == before["decision_count"]
    assert after["event_count"] == before["event_count"]
    assert after["database_bytes"] == before["database_bytes"]


def test_thousand_candidate_prepare_is_aggregate_bounded_and_idempotent(
    tmp_path,
):
    limits = ReclamationCoordinatorLimits(
        max_candidates=4096,
        max_decisions=4,
        max_events=32,
        max_participant_prepares=1,
    )
    journal = reclamation._ParticipantPrepareJournal(
        (tmp_path / "aggregate.sqlite3").resolve(),
        role="scheduler",
        limits=limits,
        clock=lambda: NOW + 300.0,
        directory_fsync=lambda _path: None,
        fault_injector=None,
    )
    core = {
        "candidate_bindings_sha256": "1" * 64,
        "candidate_count": 1000,
        "claimed_consumer_epoch_root_sha256": None,
        "claimed_consumer_registry_root_sha256": None,
        "contract": COORDINATOR_CONTRACT,
        "retention_plan_sha256": "2" * 64,
        "sealed_archive_anchor_sha256": "3" * 64,
        "source_binding_sha256": "4" * 64,
        "source_view_sha256": "5" * 64,
    }
    request = ReclamationPrepareRequest(
        request_sha256=hashlib.sha256(
            json.dumps(
                core,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
        ).hexdigest(),
        source_binding_sha256="4" * 64,
        source_view_sha256="5" * 64,
        retention_plan_sha256="2" * 64,
        sealed_archive_anchor_sha256="3" * 64,
        candidate_bindings_sha256="1" * 64,
        candidate_count=1000,
    )
    view = reclamation._ParticipantView(
        archived_view_sha256="6" * 64,
        current_view_sha256="6" * 64,
        child_reference_root_sha256="7" * 64,
    )
    receipt = journal.prepare(request, view_builder=lambda: view)
    assert receipt.candidate_count == 1000
    before = journal.audit()
    for _index in range(1000):
        assert journal.prepare(request, view_builder=lambda: view) == receipt
    after = journal.audit()
    assert after["prepare_count"] == 1
    assert after["database_bytes"] == before["database_bytes"]
    other_core = {**core, "candidate_bindings_sha256": "8" * 64}
    other = ReclamationPrepareRequest(
        request_sha256=hashlib.sha256(
            json.dumps(
                other_core,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
        ).hexdigest(),
        source_binding_sha256="4" * 64,
        source_view_sha256="5" * 64,
        retention_plan_sha256="2" * 64,
        sealed_archive_anchor_sha256="3" * 64,
        candidate_bindings_sha256="8" * 64,
        candidate_count=1000,
    )
    with pytest.raises(AcquisitionSourceReclamationCapacityError):
        journal.prepare(other, view_builder=lambda: view)
    assert journal.audit()["prepare_count"] == 1
