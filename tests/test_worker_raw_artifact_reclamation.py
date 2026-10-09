from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import sqlite3
from typing import Iterator

import pytest

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
    SourceLocalPrepareReceiptV2,
    SourceReclamationCandidateBindingV1,
    SourceReclamationRequestV2,
    archive_binding_root,
    candidate_binding_root,
    canonical_bytes,
    digest_mapping,
)
from storm_tracker_worker.acquisition_source_reclamation_views import (
    participant_candidate_view_sha256,
    participant_inventory_view_sha256,
)
from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_RECLAMATION_PREWIRING_BLOCKERS,
    RawArtifactConflictError,
    RawArtifactIntegrityError,
    RawArtifactStore,
)


PROVENANCE = {
    "representation": NATIVE_REPRESENTATION,
    "capture_stage": NATIVE_CAPTURE_STAGE,
    "collector": "raw-reclamation-test",
    "collector_version": "1",
    "transport": {"method": "GET", "content_encoding": "identity"},
}


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class _Clock:
    def __init__(self, value: float = 2_000_000_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        result = self.value
        self.value += 1.0
        return result


class _InjectedCrash(RuntimeError):
    pass


class _OneShotFault:
    def __init__(self, point: str) -> None:
        self.point = point
        self.hit = False

    def __call__(self, point: str) -> None:
        if not self.hit and point == self.point:
            self.hit = True
            raise _InjectedCrash(point)


class _DecisionAuthority(HeldCommittedDecisionAuthority):
    def __init__(self) -> None:
        super().__init__(_sha("raw-held-decision-authority"))
        self.reported_authority_sha256 = super().authority_sha256
        self.committed: CommittedReclamationDecisionReceiptV1 | None = None
        self.aborting: CoordinatorAbortDecisionReceiptV1 | None = None
        self.prepares: dict[str, ParticipantPrepareBindingV1] = {}

    @property
    def authority_sha256(self) -> str:
        return self.reported_authority_sha256

    @contextmanager
    def hold_committed_decision_v1(
        self,
        request_sha256: str,
        *,
        applying_role: str,
        expected_predecessor_apply_receipt_sha256: str | None,
    ) -> Iterator[CommittedReclamationDecisionReceiptV1]:
        decision = self.committed
        if decision is None or decision.request_sha256 != request_sha256:
            raise RawArtifactConflictError("committed decision ontbreekt")
        if applying_role != "raw_artifacts":
            raise RawArtifactConflictError("verkeerde held applyrol")
        if expected_predecessor_apply_receipt_sha256 is None:
            raise RawArtifactConflictError("raw predecessor ontbreekt")
        yield decision

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
        decision = self.aborting
        binding = self.prepares.get(aborting_role)
        if (
            decision is None
            or binding is None
            or aborting_role != "raw_artifacts"
            or decision.request_sha256 != request_sha256
            or decision.abort_decision_sha256
            != expected_abort_decision_sha256
            or binding.prepare_receipt_sha256
            != expected_prepare_receipt_sha256
            or binding.source_store_identity_sha256
            != expected_source_store_identity_sha256
            or binding.source_fence_sha256 != expected_source_fence_sha256
        ):
            raise RawArtifactConflictError("abort decisionbinding verschilt")
        yield decision


@dataclass
class _Harness:
    root: Path
    store: RawArtifactStore
    candidate_authority: SqliteSourceReclamationCandidateAuthority
    decisions: _DecisionAuthority
    request: SourceReclamationRequestV2
    receipts: dict[str, object]
    captures: dict[str, object]
    payloads: dict[str, bytes]
    store_identity_sha256: str
    clock: _Clock
    sync_calls: list[Path]


def _receipt_view(receipt) -> dict[str, object]:
    return RawArtifactStore._reclamation_receipt_view(receipt)


def _build_harness(
    tmp_path: Path,
    *,
    payloads: dict[str, bytes] | None = None,
    capture_members: dict[str, dict[str, str]] | None = None,
    candidate_names: tuple[str, ...] = ("candidate",),
    receipt_upstream_ids: dict[str, str] | None = None,
    receipt_products: dict[str, str] | None = None,
    capture_upstream_ids: dict[str, str] | None = None,
) -> _Harness:
    root = (tmp_path / "raw-v3").resolve()
    payloads = payloads or {"candidate-object": b"exact-candidate-raw-bytes"}
    capture_members = capture_members or {
        "candidate": {"native": "candidate-object"},
    }
    clock = _Clock()
    sync_calls: list[Path] = []

    # Build an ordinary append-only v3 store first.  Reclamation is an
    # explicit additive opt-in, never an implicit initialize migration.
    base = RawArtifactStore(
        root,
        reclamation_directory_fsync=lambda path: sync_calls.append(path),
        reclamation_clock=clock,
    )
    base.initialize()
    receipts = {}
    for name, payload in payloads.items():
        receipts[name] = base.store(
            provider="dwd",
            product=(
                receipt_products.get(name, f"raw-product-{name}")
                if receipt_products is not None
                else f"raw-product-{name}"
            ),
            fetch_id=f"fetch-{name}",
            content_type="application/octet-stream",
            received_at=1_900_000_000.0 + len(receipts),
            upstream_id=(
                receipt_upstream_ids.get(name, f"upstream-{name}")
                if receipt_upstream_ids is not None
                else f"upstream-{name}"
            ),
            provenance=PROVENANCE,
            payload=payload,
        )
    captures = {}
    for name, members in capture_members.items():
        captures[name] = base.commit_capture(
            provider="dwd",
            product="raw-capture",
            fetch_id=f"capture-fetch-{name}",
            upstream_capture_id=(
                capture_upstream_ids.get(name, f"capture-upstream-{name}")
                if capture_upstream_ids is not None
                else f"capture-upstream-{name}"
            ),
            product_timestamp=1_900_000_100.0,
            members={role: receipts[receipt_name].receipt_id
                     for role, receipt_name in members.items()},
        )
    store_identity = base.initialize_reclamation_extension_v1()
    assert base.source_store_identity_sha256 == store_identity

    bindings = []
    reference_payloads = []
    raw_views = []
    for ordinal, name in enumerate(candidate_names, start=1):
        capture = captures[name]
        members = capture_members[name]
        candidate_sha256 = f"{ordinal:064x}"
        binding = SourceReclamationCandidateBindingV1.create(
            candidate_sha256=candidate_sha256,
            acquisition_job_id=f"acquisition-{name}",
            scheduler_job_id=f"scheduler-{name}",
            raw_capture_id=capture.capture_id,
            evidence_stream_id="raw-reclamation-test-stream",
            evidence_stream_sequence=ordinal,
            sealed_archive_segment_sha256=_sha(f"segment-{name}"),
            sealed_archive_record_sha256=_sha(f"record-{name}"),
            consumer_closure_receipt_sha256=_sha(f"closure-{name}"),
            consumer_closure_root_sha256=_sha(f"closure-root-{name}"),
        )
        projection = {
            "raw_capture": capture.to_metadata(),
            "raw_objects": {
                role: {
                    "content_sha256": receipts[receipt_name].content_sha256,
                    "size_bytes": receipts[receipt_name].size_bytes,
                }
                for role, receipt_name in sorted(members.items())
            },
            "raw_receipts": {
                role: _receipt_view(receipts[receipt_name])
                for role, receipt_name in sorted(members.items())
            },
        }
        raw_view = participant_candidate_view_sha256(
            role="raw_artifacts",
            candidate_sha256=candidate_sha256,
            projection=projection,
        )
        raw_views.append((candidate_sha256, raw_view))
        reference_payloads.append(canonical_bytes({
            "archive_object_count": len(members),
            "archive_object_reference_root_sha256": _sha(
                f"archive-object-root-{name}"
            ),
            "archive_object_total_bytes": sum(
                receipts[item].size_bytes for item in members.values()
            ),
            "candidate_sha256": candidate_sha256,
            "capacity_ledger_view_sha256": _sha(f"capacity-view-{name}"),
            "downstream_lifecycle_view_sha256": _sha(
                f"lifecycle-view-{name}"
            ),
            "execution_receipts_view_sha256": _sha(
                f"execution-view-{name}"
            ),
            "metadata_sha256": _sha(f"metadata-{name}"),
            "raw_artifacts_view_sha256": raw_view,
            "record_sha256": binding.sealed_archive_record_sha256,
            "scheduler_view_sha256": _sha(f"scheduler-view-{name}"),
            "schema": CANDIDATE_REFERENCE_RECORD_SCHEMA,
            "sealed_archive_segment_sha256": (
                binding.sealed_archive_segment_sha256
            ),
        }))
        bindings.append(binding)

    candidate_authority = SqliteSourceReclamationCandidateAuthority.create(
        (tmp_path / "candidate-authority.sqlite3").resolve(),
        source_binding_sha256=_sha("raw-source-binding"),
        archive_store_identity_sha256=_sha("raw-archive-store"),
        directory_fsync=lambda _path: None,
        clock=clock,
    )
    binding_tuple = tuple(bindings)
    payload_tuple = tuple(reference_payloads)
    request = SourceReclamationRequestV2.create(
        source_binding_sha256=candidate_authority.source_binding_sha256,
        source_view_sha256=_sha("all-source-view"),
        sealed_archive_anchor_sha256=_sha("sealed-anchor"),
        archive_binding_root_sha256=archive_binding_root(
            binding_tuple,
            payload_tuple,
        ),
        candidate_bindings_sha256=candidate_binding_root(binding_tuple),
        candidate_authority_sha256=candidate_authority.authority_sha256,
        consumer_registry_root_sha256=_sha("consumer-registry"),
        consumer_epoch_root_sha256=_sha("consumer-epoch"),
        candidate_count=len(binding_tuple),
        created_at=clock(),
    )
    candidate_authority.seal_request_v2(
        request,
        bindings=binding_tuple,
        record_payloads=payload_tuple,
    )
    decisions = _DecisionAuthority()
    base.bind_reclamation_authorities_v1(
        candidate_authority,
        decisions,
    )
    store = base
    assert store.source_store_identity_sha256 == store_identity
    assert participant_inventory_view_sha256(
        role="raw_artifacts",
        candidate_views=tuple(raw_views),
    )
    return _Harness(
        root=root,
        store=store,
        candidate_authority=candidate_authority,
        decisions=decisions,
        request=request,
        receipts=receipts,
        captures=captures,
        payloads=payloads,
        store_identity_sha256=store_identity,
        clock=clock,
        sync_calls=sync_calls,
    )


def _new_candidate_authority(
    tmp_path: Path,
    name: str,
    clock: _Clock,
) -> SqliteSourceReclamationCandidateAuthority:
    return SqliteSourceReclamationCandidateAuthority.create(
        (tmp_path / f"{name}.sqlite3").resolve(),
        source_binding_sha256=_sha(f"source-binding-{name}"),
        archive_store_identity_sha256=_sha(f"archive-store-{name}"),
        directory_fsync=lambda _path: None,
        clock=clock,
    )


def test_reclamation_authorities_bind_create_once_across_initialization_phases(
    tmp_path: Path,
) -> None:
    for phase in ("before_initialize", "after_initialize", "after_extension"):
        clock = _Clock()
        candidate = _new_candidate_authority(tmp_path, f"candidate-{phase}", clock)
        decisions = _DecisionAuthority()
        store = RawArtifactStore((tmp_path / f"raw-{phase}").resolve())

        with pytest.raises(RuntimeError, match="gebonden"):
            _ = store.candidate_authority_sha256
        if phase != "before_initialize":
            store.initialize()
        identity_before_bind = None
        if phase == "after_extension":
            identity_before_bind = store.initialize_reclamation_extension_v1()

        assert store.bind_reclamation_authorities_v1(candidate, decisions) is None

        if phase == "before_initialize":
            store.initialize()
        identity = store.initialize_reclamation_extension_v1()
        assert identity_before_bind is None or identity == identity_before_bind
        assert store.candidate_authority_sha256 == candidate.authority_sha256
        assert store.coordinator_authority_sha256 == decisions.authority_sha256
        assert store.source_store_identity_sha256 == identity


def test_reclamation_authorities_bind_rejects_partial_invalid_mixed_and_rebind(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    candidate = _new_candidate_authority(tmp_path, "candidate-a", clock)
    other_candidate = _new_candidate_authority(
        tmp_path,
        "candidate-b",
        clock,
    )
    decisions = _DecisionAuthority()
    other_decisions = _DecisionAuthority()
    store = RawArtifactStore((tmp_path / "raw-bind-cas").resolve())

    with pytest.raises(TypeError, match="candidate-authority"):
        store.bind_reclamation_authorities_v1(object(), decisions)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="decision-authority"):
        store.bind_reclamation_authorities_v1(candidate, object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="candidate-authority"):
        store.bind_reclamation_authorities_v1(None, decisions)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="decision-authority"):
        store.bind_reclamation_authorities_v1(candidate, None)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="gebonden"):
        _ = store.coordinator_authority_sha256
    with pytest.raises(ValueError, match="beide tegelijk"):
        RawArtifactStore(
            (tmp_path / "raw-partial-candidate").resolve(),
            candidate_authority=candidate,
        )
    with pytest.raises(ValueError, match="beide tegelijk"):
        RawArtifactStore(
            (tmp_path / "raw-partial-decision").resolve(),
            committed_decision_authority=decisions,
        )

    store.bind_reclamation_authorities_v1(candidate, decisions)
    with pytest.raises(RawArtifactConflictError, match="ander exact paar"):
        store.bind_reclamation_authorities_v1(candidate, other_decisions)
    with pytest.raises(RawArtifactConflictError, match="ander exact paar"):
        store.bind_reclamation_authorities_v1(other_candidate, decisions)
    with pytest.raises(RawArtifactConflictError, match="ander exact paar"):
        store.bind_reclamation_authorities_v1(
            other_candidate,
            other_decisions,
        )
    assert store.candidate_authority_sha256 == candidate.authority_sha256
    assert store.coordinator_authority_sha256 == decisions.authority_sha256


def test_reclamation_authorities_bind_is_exact_pair_idempotent_and_preserves_identity(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    candidate = _new_candidate_authority(tmp_path, "candidate-stable", clock)
    decisions = _DecisionAuthority()
    root = (tmp_path / "existing-raw-v3").resolve()
    store = RawArtifactStore(root)
    store.initialize()
    identity = store.initialize_reclamation_extension_v1()
    profile_before = store.profile_path.read_bytes()

    assert store.bind_reclamation_authorities_v1(candidate, decisions) is None
    assert store.bind_reclamation_authorities_v1(candidate, decisions) is None
    assert store.source_store_identity_sha256 == identity
    assert store.profile_path.read_bytes() == profile_before

    stable_decision_sha256 = decisions.authority_sha256
    decisions.reported_authority_sha256 = _sha("changed-decision-authority")
    with pytest.raises(RawArtifactIntegrityError, match="gewijzigd"):
        _ = store.coordinator_authority_sha256
    with pytest.raises(RawArtifactIntegrityError, match="gewijzigd"):
        store.load_source_local_prepare_v2(_sha("absent-prepare"))
    with pytest.raises(RawArtifactIntegrityError, match="gewijzigd"):
        store.resume_journaled_raw_prune_v1(_sha("absent-journal"))
    decisions.reported_authority_sha256 = stable_decision_sha256
    assert store.bind_reclamation_authorities_v1(candidate, decisions) is None

    reopened = RawArtifactStore(
        root,
        candidate_authority=candidate,
        committed_decision_authority=decisions,
    )
    reopened.initialize()
    assert reopened.source_store_identity_sha256 == identity
    assert reopened.candidate_authority_sha256 == candidate.authority_sha256
    assert reopened.coordinator_authority_sha256 == decisions.authority_sha256


def _prepare_bindings(
    prepared: SourceLocalPrepareReceiptV2,
) -> tuple[ParticipantPrepareBindingV1, ...]:
    result = []
    for role in PARTICIPANT_APPLY_ORDER:
        if role == "raw_artifacts":
            result.append(ParticipantPrepareBindingV1(
                role=role,
                prepare_receipt_sha256=prepared.prepare_receipt_sha256,
                source_store_identity_sha256=(
                    prepared.source_store_identity_sha256
                ),
                source_fence_sha256=prepared.source_fence_sha256,
            ))
        else:
            result.append(ParticipantPrepareBindingV1(
                role=role,
                prepare_receipt_sha256=_sha(f"dummy-prepare-{role}"),
                source_store_identity_sha256=_sha(f"dummy-store-{role}"),
                source_fence_sha256=_sha(f"dummy-fence-{role}"),
            ))
    return tuple(result)


def _arm_commit(
    harness: _Harness,
    prepared: SourceLocalPrepareReceiptV2,
) -> str:
    prepares = _prepare_bindings(prepared)
    harness.decisions.prepares = {item.role: item for item in prepares}
    harness.decisions.committed = CommittedReclamationDecisionReceiptV1.create(
        request_sha256=harness.request.request_sha256,
        coordinator_store_identity_sha256=_sha("coordinator-store"),
        sealed_archive_anchor_sha256=(
            harness.request.sealed_archive_anchor_sha256
        ),
        archive_binding_root_sha256=harness.request.archive_binding_root_sha256,
        candidate_bindings_sha256=harness.request.candidate_bindings_sha256,
        candidate_authority_sha256=harness.request.candidate_authority_sha256,
        consumer_registry_root_sha256=(
            harness.request.consumer_registry_root_sha256
        ),
        consumer_epoch_root_sha256=harness.request.consumer_epoch_root_sha256,
        participant_prepares=prepares,
        candidate_count=harness.request.candidate_count,
        owner_epoch=1,
        committed_at=harness.clock(),
        commit_event_sequence=1,
        previous_event_sha256=_sha("previous-commit-event"),
        commit_event_sha256=_sha("commit-event"),
    )
    return _sha("scheduler-predecessor-apply")


def _arm_abort(
    harness: _Harness,
    prepared: SourceLocalPrepareReceiptV2,
) -> CoordinatorAbortDecisionReceiptV1:
    prepares = _prepare_bindings(prepared)
    harness.decisions.prepares = {item.role: item for item in prepares}
    decision = CoordinatorAbortDecisionReceiptV1.create(
        request_sha256=harness.request.request_sha256,
        coordinator_store_identity_sha256=_sha("coordinator-store"),
        participant_prepare_root_sha256=digest_mapping(
            [item.as_dict() for item in prepares]
        ),
        owner_epoch=1,
        aborting_at=harness.clock(),
        abort_event_sequence=1,
        previous_event_sha256=_sha("previous-abort-event"),
        abort_event_sha256=_sha("abort-event"),
    )
    harness.decisions.aborting = decision
    return decision


def _reopen(
    harness: _Harness,
    *,
    fault=None,
    with_directory_authority: bool = True,
) -> RawArtifactStore:
    kwargs = {}
    if with_directory_authority:
        kwargs["reclamation_directory_fsync"] = (
            lambda path: harness.sync_calls.append(path)
        )
    store = RawArtifactStore(
        harness.root,
        candidate_authority=harness.candidate_authority,
        committed_decision_authority=harness.decisions,
        reclamation_clock=harness.clock,
        fault_injector=fault,
        **kwargs,
    )
    store.initialize()
    assert store.source_store_identity_sha256 == harness.store_identity_sha256
    return store


def _schema_sql(path: Path, name: str) -> str:
    with closing(sqlite3.connect(path)) as database:
        row = database.execute(
            "SELECT sql FROM sqlite_master WHERE name=?",
            (name,),
        ).fetchone()
    assert row is not None and row[0] is not None
    return str(row[0])


def _rewrite_schema_sql(path: Path, name: str, old: str, new: str) -> None:
    with closing(sqlite3.connect(path)) as database:
        current = _schema_sql(path, name)
        changed = current.replace(old, new)
        assert changed != current
        database.execute("PRAGMA writable_schema=ON")
        cursor = database.execute(
            "UPDATE sqlite_master SET sql=? WHERE name=?",
            (changed, name),
        )
        assert cursor.rowcount == 1
        version = int(database.execute("PRAGMA schema_version").fetchone()[0])
        database.execute(f"PRAGMA schema_version={version + 1}")
        database.execute("PRAGMA writable_schema=OFF")
        database.commit()


def _create_files_pending(
    harness: _Harness,
) -> tuple[SourceLocalPrepareReceiptV2, str]:
    prepared = harness.store.prepare_source_local_v2(harness.request)
    predecessor = _arm_commit(harness, prepared)
    crashing = _reopen(
        harness,
        fault=_OneShotFault("reclamation_catalog_after_commit"),
    )
    with pytest.raises(_InjectedCrash):
        crashing.apply_source_local_v1(
            harness.request.request_sha256,
            predecessor_apply_receipt_sha256=predecessor,
        )
    return prepared, predecessor


def _apply_harness(harness: _Harness):
    prepared = harness.store.prepare_source_local_v2(harness.request)
    predecessor = _arm_commit(harness, prepared)
    applied = harness.store.apply_source_local_v1(
        harness.request.request_sha256,
        predecessor_apply_receipt_sha256=predecessor,
    )
    return applied, predecessor


def _insert_valid_receipt_without_identity_guard(
    harness: _Harness,
    *,
    provider: str,
    product: str,
    fetch_id: str,
    upstream_id: str,
) -> str:
    source = harness.receipts["candidate-object"]
    provenance_json, provenance_sha256 = RawArtifactStore._provenance(PROVENANCE)
    received_at = 2_200_000_000.0
    receipt_id = RawArtifactStore._receipt_id(
        provider=provider,
        product=product,
        fetch_id=fetch_id,
    )
    receipt_sha256 = RawArtifactStore._receipt_sha256(
        provider=provider,
        product=product,
        fetch_id=fetch_id,
        content_type="application/octet-stream",
        received_at=received_at,
        upstream_id=upstream_id,
        content_sha256=source.content_sha256,
        size_bytes=source.size_bytes,
        provenance_sha256=provenance_sha256,
    )
    trigger = "raw_artifact_reclamation_receipt_identity_guard"
    trigger_sql = _schema_sql(harness.store.catalog_path, trigger)
    with closing(sqlite3.connect(harness.store.catalog_path)) as database:
        database.execute(f"DROP TRIGGER {trigger}")
        database.execute(
            """
            INSERT INTO raw_artifact_receipts(
                receipt_id,provider,product,fetch_id,upstream_id,content_type,
                received_at,content_sha256,size_bytes,provenance_json,
                provenance_sha256,receipt_sha256,created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                receipt_id,
                provider,
                product,
                fetch_id,
                upstream_id,
                "application/octet-stream",
                received_at,
                source.content_sha256,
                source.size_bytes,
                provenance_json,
                provenance_sha256,
                receipt_sha256,
                received_at,
            ),
        )
        database.execute(trigger_sql)
        database.commit()
    return receipt_id


def _retarget_files_pending_intent(
    harness: _Harness,
    *,
    prepared: SourceLocalPrepareReceiptV2,
    predecessor: str,
    target_receipt,
    restore_canonical_triggers: bool,
) -> None:
    target_path = Path(target_receipt.artifact_path)
    target_identity = harness.store._reclamation_file_identity(  # noqa: SLF001
        target_path,
        expected_size=target_receipt.size_bytes,
        expected_sha256=target_receipt.content_sha256,
    )
    assert target_identity.size_bytes == prepared.releasable_bytes
    intent_sha256 = harness.store._reclamation_unlink_intent_sha256(  # noqa: SLF001
        request_sha256=harness.request.request_sha256,
        unlink_ordinal=0,
        identity=target_identity,
    )
    intent_root = harness.store._ordered_reclamation_root(  # noqa: SLF001
        schema="stv3-worker-raw-reclamation-unlink-item-root-v1",
        item_sha256s=(intent_sha256,),
    )
    decision = harness.decisions.committed
    assert decision is not None
    path = harness.store.catalog_path
    trigger_targets = {
        "raw_artifact_reclamation_unlink_intents_no_update": (
            "raw_artifact_reclamation_unlink_intents"
        ),
        "raw_artifact_reclamation_catalog_applies_no_update": (
            "raw_artifact_reclamation_catalog_applies"
        ),
    }
    trigger_sql = {
        name: _schema_sql(path, name) for name in trigger_targets
    }
    with closing(sqlite3.connect(path)) as database:
        database.row_factory = sqlite3.Row
        row = database.execute(
            "SELECT files_pending_at FROM "
            "raw_artifact_reclamation_catalog_applies WHERE request_sha256=?",
            (harness.request.request_sha256,),
        ).fetchone()
        assert row is not None
        catalog_apply_sha256 = (
            harness.store._reclamation_catalog_apply_sha256(  # noqa: SLF001
                request_sha256=harness.request.request_sha256,
                source_store_identity_sha256=(
                    prepared.source_store_identity_sha256
                ),
                prepare_receipt_sha256=prepared.prepare_receipt_sha256,
                committed_decision_sha256=decision.decision_sha256,
                committed_decision_receipt_sha256=decision.receipt_sha256,
                predecessor_apply_receipt_sha256=predecessor,
                shared_reference_root_sha256=(
                    prepared.child_reference_root_sha256
                ),
                unlink_item_root_sha256=intent_root,
                unlink_item_count=1,
                unlink_bytes=target_identity.size_bytes,
                files_pending_at=float(row["files_pending_at"]),
            )
        )
        for name in trigger_targets:
            database.execute(f"DROP TRIGGER {name}")
        database.execute(
            "UPDATE raw_artifact_reclamation_unlink_intents SET "
            "intent_sha256=?,content_sha256=?,relative_path=?,size_bytes=?,"
            "st_dev=?,st_ino=?,st_mode=?,st_nlink=? "
            "WHERE request_sha256=? AND unlink_ordinal=0",
            (
                intent_sha256,
                target_identity.content_sha256,
                target_identity.relative_path,
                target_identity.size_bytes,
                target_identity.st_dev,
                target_identity.st_ino,
                target_identity.st_mode,
                target_identity.st_nlink,
                harness.request.request_sha256,
            ),
        )
        database.execute(
            "UPDATE raw_artifact_reclamation_catalog_applies SET "
            "catalog_apply_sha256=?,unlink_item_root_sha256=?,"
            "unlink_item_count=1,unlink_bytes=? WHERE request_sha256=?",
            (
                catalog_apply_sha256,
                intent_root,
                target_identity.size_bytes,
                harness.request.request_sha256,
            ),
        )
        for name, target in trigger_targets.items():
            if restore_canonical_triggers:
                database.execute(trigger_sql[name])
            else:
                database.execute(
                    f"CREATE TRIGGER {name} BEFORE UPDATE ON {target} "
                    "BEGIN SELECT 1; END"
                )
        database.commit()


def test_existing_v3_opt_in_apply_unlinks_once_and_tombstones_identity(
    tmp_path: Path,
) -> None:
    harness = _build_harness(tmp_path)
    prepared = harness.store.prepare_source_local_v2(harness.request)
    assert prepared.source_store_identity_sha256 == harness.store_identity_sha256
    predecessor = _arm_commit(harness, prepared)
    object_path = Path(harness.receipts["candidate-object"].artifact_path)

    applied = harness.store.apply_source_local_v1(
        harness.request.request_sha256,
        predecessor_apply_receipt_sha256=predecessor,
    )

    assert applied.released_entries == 1
    assert applied.released_bytes == len(harness.payloads["candidate-object"])
    assert not object_path.exists()
    assert harness.store.load_capture(
        harness.captures["candidate"].capture_id
    ) is None
    assert harness.store.load_receipt(
        harness.receipts["candidate-object"].receipt_id
    ) is None
    assert harness.sync_calls == [harness.store.objects_path]
    assert harness.store.load_raw_unlink_journal_v1(
        harness.request.request_sha256
    ).unlink_item_count == 1
    assert harness.store.apply_source_local_v1(
        harness.request.request_sha256,
        predecessor_apply_receipt_sha256=predecessor,
    ) == applied

    with pytest.raises(RawArtifactConflictError, match="fence|tombstone"):
        harness.store.store(
            provider="dwd",
            product="raw-product-candidate-object",
            fetch_id="fetch-candidate-object",
            content_type="application/octet-stream",
            received_at=1_900_000_000.0,
            upstream_id="upstream-candidate-object",
            provenance=PROVENANCE,
            payload=harness.payloads["candidate-object"],
        )
    assert not object_path.exists()
    assert harness.store.integrity_check()["healthy"] is True


def test_shared_receipt_and_object_refcounts_are_exact_and_fenced(
    tmp_path: Path,
) -> None:
    harness = _build_harness(
        tmp_path,
        payloads={
            "shared": b"shared-content",
            "other": b"unrelated-content",
        },
        capture_members={
            "candidate": {"native": "shared"},
            "survivor": {"native": "shared"},
            "other": {"native": "other"},
        },
        candidate_names=("candidate",),
    )
    prepared = harness.store.prepare_source_local_v2(harness.request)
    assert prepared.releasable_entries == 0
    assert prepared.releasable_bytes == 0

    # A prepare freezes all reference counts, including non-delete-eligible
    # shared objects and receipts.
    with pytest.raises(RawArtifactConflictError, match="fence"):
        harness.store.store(
            provider="dwd",
            product="new-shared-reference",
            fetch_id="new-shared-reference",
            content_type="application/octet-stream",
            received_at=1_900_000_500.0,
            upstream_id="new-shared-reference",
            provenance=PROVENANCE,
            payload=harness.payloads["shared"],
        )
    with pytest.raises(RawArtifactConflictError, match="fence|tombstone"):
        harness.store.commit_capture(
            provider="dwd",
            product="raw-capture",
            fetch_id="capture-fetch-duplicate-upstream",
            upstream_capture_id="capture-upstream-candidate",
            product_timestamp=1_900_000_100.0,
            members={"native": harness.receipts["other"].receipt_id},
        )

    predecessor = _arm_commit(harness, prepared)
    applied = harness.store.apply_source_local_v1(
        harness.request.request_sha256,
        predecessor_apply_receipt_sha256=predecessor,
    )
    assert applied.released_entries == 0
    assert applied.released_bytes == 0
    assert harness.store.load_capture(
        harness.captures["candidate"].capture_id
    ) is None
    assert harness.store.load_capture(
        harness.captures["survivor"].capture_id
    ) is not None
    assert harness.store.load_receipt(
        harness.receipts["shared"].receipt_id
    ) is not None
    assert Path(harness.receipts["shared"].artifact_path).is_file()
    assert harness.store.integrity_check()["healthy"] is True


@pytest.mark.parametrize("duplicate_kind", ("capture", "receipt"))
def test_prepare_rejects_live_identity_that_a_tombstone_would_shadow(
    tmp_path: Path,
    duplicate_kind: str,
) -> None:
    kwargs = {
        "payloads": {"candidate": b"same", "survivor": b"same"},
        "capture_members": {
            "candidate": {"native": "candidate"},
            "survivor": {"native": "survivor"},
        },
        "candidate_names": ("candidate",),
    }
    if duplicate_kind == "capture":
        kwargs["capture_upstream_ids"] = {
            "candidate": "duplicate-capture-upstream",
            "survivor": "duplicate-capture-upstream",
        }
    else:
        kwargs["receipt_upstream_ids"] = {
            "candidate": "duplicate-receipt-upstream",
            "survivor": "duplicate-receipt-upstream",
        }
        kwargs["receipt_products"] = {
            "candidate": "duplicate-receipt-product",
            "survivor": "duplicate-receipt-product",
        }
    harness = _build_harness(tmp_path, **kwargs)

    with pytest.raises(RawArtifactConflictError, match="dubbele upstream"):
        harness.store.prepare_source_local_v2(harness.request)


@pytest.mark.parametrize(
    "point",
    (
        "reclamation_catalog_before_commit",
        "reclamation_catalog_after_commit",
        "reclamation_unlink_after_unlink",
        "reclamation_unlink_event_after_commit",
        "reclamation_unlinked_before_commit",
        "reclamation_unlinked_after_commit",
        "reclamation_apply_before_commit",
        "reclamation_apply_after_commit",
    ),
)
def test_apply_resumes_after_every_catalog_and_unlink_crash_boundary(
    tmp_path: Path,
    point: str,
) -> None:
    harness = _build_harness(tmp_path)
    prepared = harness.store.prepare_source_local_v2(harness.request)
    predecessor = _arm_commit(harness, prepared)
    fault = _OneShotFault(point)
    crashing = _reopen(harness, fault=fault)

    with pytest.raises(_InjectedCrash, match=point):
        crashing.apply_source_local_v1(
            harness.request.request_sha256,
            predecessor_apply_receipt_sha256=predecessor,
        )
    assert fault.hit is True

    reopened = _reopen(harness)
    applied = reopened.resume_journaled_raw_prune_v1(
        harness.request.request_sha256
    ) if point != "reclamation_catalog_before_commit" else (
        reopened.apply_source_local_v1(
            harness.request.request_sha256,
            predecessor_apply_receipt_sha256=predecessor,
        )
    )
    assert applied.released_entries == 1
    assert reopened.integrity_check()["healthy"] is True


@pytest.mark.parametrize(
    "point",
    ("reclamation_prepare_before_commit", "reclamation_prepare_after_commit"),
)
def test_prepare_retries_exactly_across_both_commit_boundaries(
    tmp_path: Path,
    point: str,
) -> None:
    harness = _build_harness(tmp_path)
    fault = _OneShotFault(point)
    crashing = _reopen(harness, fault=fault)
    with pytest.raises(_InjectedCrash, match=point):
        crashing.prepare_source_local_v2(harness.request)

    reopened = _reopen(harness)
    prepared = reopened.prepare_source_local_v2(harness.request)
    assert reopened.load_source_local_prepare_v2(
        harness.request.request_sha256
    ) == prepared
    assert Path(harness.receipts["candidate-object"].artifact_path).is_file()


@pytest.mark.parametrize(
    "point",
    ("reclamation_abort_before_commit", "reclamation_abort_after_commit"),
)
def test_abort_retries_exactly_and_releases_the_source_fence(
    tmp_path: Path,
    point: str,
) -> None:
    harness = _build_harness(tmp_path)
    prepared = harness.store.prepare_source_local_v2(harness.request)
    decision = _arm_abort(harness, prepared)
    fault = _OneShotFault(point)
    crashing = _reopen(harness, fault=fault)
    with pytest.raises(_InjectedCrash, match=point):
        crashing.abort_source_local_v1(
            harness.request.request_sha256,
            expected_abort_decision_sha256=decision.abort_decision_sha256,
        )

    reopened = _reopen(harness)
    aborted = reopened.abort_source_local_v1(
        harness.request.request_sha256,
        expected_abort_decision_sha256=decision.abort_decision_sha256,
    )
    assert reopened.load_source_local_abort_v1(
        harness.request.request_sha256
    ) == aborted
    assert reopened.load_capture(
        harness.captures["candidate"].capture_id
    ) is not None

    # Aborting releases the frozen content-reference fence.
    new_receipt = reopened.store(
        provider="dwd",
        product="post-abort-reference",
        fetch_id="post-abort-reference",
        content_type="application/octet-stream",
        received_at=1_900_000_500.0,
        upstream_id="post-abort-reference",
        provenance=PROVENANCE,
        payload=harness.payloads["candidate-object"],
    )
    assert new_receipt.content_sha256 == (
        harness.receipts["candidate-object"].content_sha256
    )


def test_same_bytes_with_replaced_physical_identity_fail_closed(
    tmp_path: Path,
) -> None:
    harness = _build_harness(tmp_path)
    prepared = harness.store.prepare_source_local_v2(harness.request)
    predecessor = _arm_commit(harness, prepared)
    crashing = _reopen(
        harness,
        fault=_OneShotFault("reclamation_catalog_after_commit"),
    )
    with pytest.raises(_InjectedCrash):
        crashing.apply_source_local_v1(
            harness.request.request_sha256,
            predecessor_apply_receipt_sha256=predecessor,
        )

    path = Path(harness.receipts["candidate-object"].artifact_path)
    payload = path.read_bytes()
    original = path.stat()
    path.unlink()
    path.write_bytes(payload)
    replacement = path.stat()
    if (original.st_dev, original.st_ino) == (
        replacement.st_dev,
        replacement.st_ino,
    ):
        pytest.skip("filesystem reused the same physical identity")

    with pytest.raises(RawArtifactIntegrityError, match="identity"):
        _reopen(harness).resume_journaled_raw_prune_v1(
            harness.request.request_sha256
        )


def test_journal_checksum_and_extension_schema_tamper_fail_closed(
    tmp_path: Path,
) -> None:
    harness = _build_harness(tmp_path)
    prepared = harness.store.prepare_source_local_v2(harness.request)
    predecessor = _arm_commit(harness, prepared)
    crashing = _reopen(
        harness,
        fault=_OneShotFault("reclamation_unlink_event_after_commit"),
    )
    with pytest.raises(_InjectedCrash):
        crashing.apply_source_local_v1(
            harness.request.request_sha256,
            predecessor_apply_receipt_sha256=predecessor,
        )

    trigger = "raw_artifact_reclamation_unlink_durable_no_update"
    with closing(sqlite3.connect(harness.store.catalog_path)) as database:
        trigger_sql = database.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
            (trigger,),
        ).fetchone()[0]
        database.execute(f"DROP TRIGGER {trigger}")
        database.execute(
            "UPDATE raw_artifact_reclamation_unlink_durable "
            "SET directory_barrier_sha256=? WHERE request_sha256=?",
            (_sha("tampered-directory-barrier"), harness.request.request_sha256),
        )
        database.execute(trigger_sql)
        database.commit()

    reopened = _reopen(harness)
    with pytest.raises(RawArtifactIntegrityError, match="checksum"):
        reopened.resume_journaled_raw_prune_v1(
            harness.request.request_sha256
        )

    with closing(sqlite3.connect(harness.store.catalog_path)) as database:
        database.execute(
            "DROP TRIGGER raw_artifact_reclamation_capture_member_fence"
        )
        database.commit()
    with pytest.raises(RawArtifactIntegrityError, match="trigger|schema"):
        _reopen(harness)


@pytest.mark.parametrize("restore_canonical_triggers", (False, True))
def test_files_pending_intent_must_remain_bound_to_durable_prepare_plan(
    tmp_path: Path,
    restore_canonical_triggers: bool,
) -> None:
    harness = _build_harness(
        tmp_path,
        payloads={
            "candidate-object": b"A" * 32,
            "unrelated-object": b"B" * 32,
        },
    )
    prepared, predecessor = _create_files_pending(harness)
    source_path = Path(harness.receipts["candidate-object"].artifact_path)
    unrelated = harness.receipts["unrelated-object"]
    unrelated_path = Path(unrelated.artifact_path)
    assert source_path.exists() and unrelated_path.exists()

    _retarget_files_pending_intent(
        harness,
        prepared=prepared,
        predecessor=predecessor,
        target_receipt=unrelated,
        restore_canonical_triggers=restore_canonical_triggers,
    )

    expected = "prepareplan" if restore_canonical_triggers else "canonieke"
    with pytest.raises(RawArtifactIntegrityError, match=expected):
        harness.store.resume_journaled_raw_prune_v1(
            harness.request.request_sha256
        )
    assert source_path.exists()
    assert unrelated_path.exists()


def test_files_pending_rejects_restored_base_catalog_references_before_unlink(
    tmp_path: Path,
) -> None:
    harness = _build_harness(tmp_path)
    _prepared, _predecessor = _create_files_pending(harness)
    receipt = harness.receipts["candidate-object"]
    object_path = Path(receipt.artifact_path)
    assert object_path.exists()
    trigger_targets = {
        "raw_artifact_reclamation_object_catalog_fence": "raw_artifact_objects",
        "raw_artifact_reclamation_receipt_reference_fence": (
            "raw_artifact_receipts"
        ),
    }
    trigger_sql = {
        name: _schema_sql(harness.store.catalog_path, name)
        for name in trigger_targets
    }
    with closing(sqlite3.connect(harness.store.catalog_path)) as database:
        database.execute("PRAGMA foreign_keys=ON")
        for name in trigger_targets:
            database.execute(f"DROP TRIGGER {name}")
        database.execute(
            "INSERT INTO raw_artifact_objects("
            "content_sha256,relative_path,size_bytes,created_at) VALUES(?,?,?,?)",
            (
                receipt.content_sha256,
                f"objects/{receipt.content_sha256}.raw",
                receipt.size_bytes,
                2_100_000_000.0,
            ),
        )
        database.execute(
            """
            INSERT INTO raw_artifact_receipts(
                receipt_id,provider,product,fetch_id,upstream_id,content_type,
                received_at,content_sha256,size_bytes,provenance_json,
                provenance_sha256,receipt_sha256,created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "offline-restored-receipt",
                "offline",
                "restored-content",
                "offline-restored-fetch",
                "offline-restored-upstream",
                "application/octet-stream",
                2_100_000_000.0,
                receipt.content_sha256,
                receipt.size_bytes,
                "{}",
                _sha("offline-restored-provenance"),
                _sha("offline-restored-receipt"),
                2_100_000_000.0,
            ),
        )
        for name in trigger_targets:
            database.execute(trigger_sql[name])
        database.commit()

    with pytest.raises(
        RawArtifactIntegrityError,
        match="opnieuw|catalogusreference",
    ):
        harness.store.resume_journaled_raw_prune_v1(
            harness.request.request_sha256
        )
    assert object_path.exists()
    with closing(sqlite3.connect(harness.store.catalog_path)) as database:
        assert database.execute(
            "SELECT count(*) FROM raw_artifact_receipts "
            "WHERE content_sha256=?",
            (receipt.content_sha256,),
        ).fetchone()[0] == 1


def test_full_base_and_extension_sqlite_schema_authority_is_canonical(
    tmp_path: Path,
) -> None:
    harness = _build_harness(tmp_path)
    path = harness.store.catalog_path

    _rewrite_schema_sql(
        path,
        "raw_artifact_receipts",
        "raw_artifact_objects(content_sha256)",
        "raw_artifact_objects(content_sha256) ON DELETE CASCADE",
    )
    with pytest.raises(RawArtifactIntegrityError, match="canonieke"):
        _reopen(harness)
    _rewrite_schema_sql(
        path,
        "raw_artifact_receipts",
        "raw_artifact_objects(content_sha256) ON DELETE CASCADE",
        "raw_artifact_objects(content_sha256)",
    )
    assert _reopen(harness).source_store_identity_sha256 == (
        harness.store_identity_sha256
    )

    index_name = "idx_raw_artifact_receipts_identity"
    index_sql = _schema_sql(path, index_name)
    with closing(sqlite3.connect(path)) as database:
        database.execute(f"DROP INDEX {index_name}")
        database.execute(
            f"CREATE INDEX {index_name} ON raw_artifact_receipts("
            "product,provider,upstream_id)"
        )
        database.commit()
    with pytest.raises(RawArtifactIntegrityError, match="canonieke"):
        _reopen(harness)
    with closing(sqlite3.connect(path)) as database:
        database.execute(f"DROP INDEX {index_name}")
        database.execute(index_sql)
        database.commit()

    with closing(sqlite3.connect(path)) as database:
        database.execute(
            "CREATE VIEW raw_artifact_unexpected_view AS "
            "SELECT content_sha256 FROM raw_artifact_objects"
        )
        database.commit()
    with pytest.raises(RawArtifactIntegrityError, match="canonieke"):
        _reopen(harness)
    with closing(sqlite3.connect(path)) as database:
        database.execute("DROP VIEW raw_artifact_unexpected_view")
        database.commit()

    trigger_name = "raw_artifact_reclamation_capture_member_fence"
    trigger_sql = _schema_sql(path, trigger_name)
    with closing(sqlite3.connect(path)) as database:
        database.execute(f"DROP TRIGGER {trigger_name}")
        database.execute(
            f"CREATE TRIGGER {trigger_name} BEFORE INSERT ON "
            "raw_artifact_capture_members BEGIN SELECT 1; END"
        )
        database.commit()
    with pytest.raises(RawArtifactIntegrityError, match="canonieke"):
        _reopen(harness)
    with closing(sqlite3.connect(path)) as database:
        database.execute(f"DROP TRIGGER {trigger_name}")
        database.execute(trigger_sql)
        database.commit()
    assert _reopen(harness).source_store_identity_sha256 == (
        harness.store_identity_sha256
    )


def test_prepare_retry_rejects_reopened_different_authority_pair(
    tmp_path: Path,
) -> None:
    harness = _build_harness(tmp_path)
    harness.store.prepare_source_local_v2(harness.request)
    other_decisions = _DecisionAuthority()
    other_decisions.reported_authority_sha256 = _sha(
        "other-raw-held-decision-authority"
    )
    reopened = RawArtifactStore(
        harness.root,
        candidate_authority=harness.candidate_authority,
        committed_decision_authority=other_decisions,
        reclamation_directory_fsync=lambda _path: None,
        reclamation_clock=harness.clock,
    )
    reopened.initialize()

    with pytest.raises(RawArtifactConflictError, match="instance-authority"):
        reopened.prepare_source_local_v2(harness.request)
    with pytest.raises(RawArtifactConflictError, match="instance-authority"):
        reopened.load_source_local_prepare_v2(harness.request.request_sha256)
    with pytest.raises(RawArtifactConflictError, match="instance-authority"):
        reopened.apply_source_local_v1(
            harness.request.request_sha256,
            predecessor_apply_receipt_sha256=_sha("different-pair-predecessor"),
        )
    with pytest.raises(RawArtifactConflictError, match="instance-authority"):
        reopened.abort_source_local_v1(
            harness.request.request_sha256,
            expected_abort_decision_sha256=_sha("different-pair-abort"),
        )


def test_directory_sync_failure_after_unlink_is_resumed_not_claimed_durable(
    tmp_path: Path,
) -> None:
    harness = _build_harness(tmp_path)
    prepared = harness.store.prepare_source_local_v2(harness.request)
    predecessor = _arm_commit(harness, prepared)

    def fail_directory_sync(_path: Path) -> None:
        raise OSError("simulated directory durability failure")

    failing = RawArtifactStore(
        harness.root,
        candidate_authority=harness.candidate_authority,
        committed_decision_authority=harness.decisions,
        reclamation_directory_fsync=fail_directory_sync,
        reclamation_clock=harness.clock,
    )
    failing.initialize()
    with pytest.raises(RawArtifactIntegrityError, match="syncauthority"):
        failing.apply_source_local_v1(
            harness.request.request_sha256,
            predecessor_apply_receipt_sha256=predecessor,
        )

    object_path = Path(harness.receipts["candidate-object"].artifact_path)
    assert not object_path.exists()
    assert failing.load_raw_unlink_journal_v1(
        harness.request.request_sha256
    ) is None

    recovered = _reopen(harness)
    applied = recovered.resume_journaled_raw_prune_v1(
        harness.request.request_sha256
    )
    assert applied.released_entries == 1
    with closing(sqlite3.connect(recovered.catalog_path)) as database:
        assert database.execute(
            "SELECT object_was_present FROM "
            "raw_artifact_reclamation_unlink_durable WHERE request_sha256=?",
            (harness.request.request_sha256,),
        ).fetchone()[0] == 0


def test_applied_replay_accepts_same_content_only_via_new_complete_capture_graph(
    tmp_path: Path,
) -> None:
    harness = _build_harness(tmp_path)
    applied, predecessor = _apply_harness(harness)
    source = harness.receipts["candidate-object"]
    object_path = Path(source.artifact_path)
    assert not object_path.exists()

    replacement = harness.store.store(
        provider="dwd",
        product="new-content-publication",
        fetch_id="new-content-fetch",
        content_type="application/octet-stream",
        received_at=2_200_000_000.0,
        upstream_id="new-content-upstream",
        provenance=PROVENANCE,
        payload=harness.payloads["candidate-object"],
    )
    assert replacement.content_sha256 == source.content_sha256
    assert replacement.receipt_id != source.receipt_id
    with pytest.raises(RawArtifactIntegrityError, match="capturegraph"):
        harness.store.load_source_local_apply_v1(
            harness.request.request_sha256
        )

    replacement_capture = harness.store.commit_capture(
        provider="dwd",
        product="new-content-capture",
        fetch_id="new-content-capture-fetch",
        upstream_capture_id="new-content-capture-upstream",
        product_timestamp=2_200_000_001.0,
        members={"native": replacement.receipt_id},
    )
    assert replacement_capture.capture_id != (
        harness.captures["candidate"].capture_id
    )
    assert harness.store.load_source_local_apply_v1(
        harness.request.request_sha256
    ) == applied
    assert harness.store.apply_source_local_v1(
        harness.request.request_sha256,
        predecessor_apply_receipt_sha256=predecessor,
    ) == applied


@pytest.mark.parametrize(
    "identity_kind",
    (
        "receipt_fetch",
        "receipt_upstream",
        "capture_fetch",
        "capture_upstream",
    ),
)
def test_applied_replay_rejects_trigger_bypass_of_old_semantic_identity_aliases(
    tmp_path: Path,
    identity_kind: str,
) -> None:
    harness = _build_harness(tmp_path)
    _apply_harness(harness)
    original_receipt = harness.receipts["candidate-object"]
    original_capture = harness.captures["candidate"]
    replacement = harness.store.store(
        provider="dwd",
        product="replay-safe-product",
        fetch_id="replay-safe-fetch",
        content_type="application/octet-stream",
        received_at=2_210_000_000.0,
        upstream_id="replay-safe-upstream",
        provenance=PROVENANCE,
        payload=harness.payloads["candidate-object"],
    )

    if identity_kind.startswith("receipt_"):
        receipt_id = _insert_valid_receipt_without_identity_guard(
            harness,
            provider=original_receipt.provider,
            product=original_receipt.product,
            fetch_id=(
                original_receipt.fetch_id
                if identity_kind == "receipt_fetch"
                else "bypassed-new-fetch"
            ),
            upstream_id=(
                original_receipt.upstream_id
                if identity_kind == "receipt_upstream"
                else "bypassed-new-upstream"
            ),
        )
        harness.store.commit_capture(
            provider="dwd",
            product="bypassed-receipt-capture",
            fetch_id=f"bypassed-capture-{identity_kind}",
            upstream_capture_id=f"bypassed-capture-upstream-{identity_kind}",
            product_timestamp=2_210_000_001.0,
            members={"native": receipt_id},
        )
        expected = "receipt-identiteit"
    else:
        trigger = "raw_artifact_reclamation_capture_identity_guard"
        trigger_sql = _schema_sql(harness.store.catalog_path, trigger)
        with closing(sqlite3.connect(harness.store.catalog_path)) as database:
            database.execute(f"DROP TRIGGER {trigger}")
            database.commit()
        try:
            harness.store.commit_capture(
                provider=original_capture.provider,
                product=original_capture.product,
                fetch_id=(
                    original_capture.fetch_id
                    if identity_kind == "capture_fetch"
                    else "bypassed-new-capture-fetch"
                ),
                upstream_capture_id=(
                    original_capture.upstream_capture_id
                    if identity_kind == "capture_upstream"
                    else "bypassed-new-capture-upstream"
                ),
                product_timestamp=2_210_000_001.0,
                members={"native": replacement.receipt_id},
            )
        finally:
            with closing(sqlite3.connect(harness.store.catalog_path)) as database:
                database.execute(trigger_sql)
                database.commit()
        expected = "capture-identiteit"

    with pytest.raises(RawArtifactIntegrityError, match=expected):
        _reopen(harness).load_source_local_apply_v1(
            harness.request.request_sha256
        )


@pytest.mark.parametrize(
    ("table", "column", "expected"),
    (
        (
            "raw_artifact_reclamation_capture_tombstones",
            "manifest_sha256",
            "capture-tombstonefacts",
        ),
        (
            "raw_artifact_reclamation_receipt_tombstones",
            "receipt_sha256",
            "receipt-tombstonefacts",
        ),
    ),
)
def test_applied_replay_reproves_exact_tombstone_facts(
    tmp_path: Path,
    table: str,
    column: str,
    expected: str,
) -> None:
    harness = _build_harness(tmp_path)
    _apply_harness(harness)
    trigger = f"{table}_no_update"
    trigger_sql = _schema_sql(harness.store.catalog_path, trigger)
    with closing(sqlite3.connect(harness.store.catalog_path)) as database:
        database.execute(f"DROP TRIGGER {trigger}")
        database.execute(
            f"UPDATE {table} SET {column}=? WHERE request_sha256=?",
            (_sha(f"tampered-{table}-{column}"), harness.request.request_sha256),
        )
        database.execute(trigger_sql)
        database.commit()
    with pytest.raises(RawArtifactIntegrityError, match=expected):
        _reopen(harness).load_source_local_apply_v1(
            harness.request.request_sha256
        )


def test_applied_receipt_fails_closed_if_object_path_reappears_without_graph(
    tmp_path: Path,
) -> None:
    harness = _build_harness(tmp_path)
    prepared = harness.store.prepare_source_local_v2(harness.request)
    predecessor = _arm_commit(harness, prepared)
    harness.store.apply_source_local_v1(
        harness.request.request_sha256,
        predecessor_apply_receipt_sha256=predecessor,
    )

    object_path = Path(harness.receipts["candidate-object"].artifact_path)
    object_path.write_bytes(harness.payloads["candidate-object"])
    with pytest.raises(RawArtifactIntegrityError, match="base-catalogauthority"):
        harness.store.load_source_local_apply_v1(
            harness.request.request_sha256
        )


def test_hardlink_interleaving_leaves_durable_terminal_failure_fence(
    tmp_path: Path,
) -> None:
    harness = _build_harness(tmp_path)
    prepared = harness.store.prepare_source_local_v2(harness.request)
    predecessor = _arm_commit(harness, prepared)
    object_path = Path(harness.receipts["candidate-object"].artifact_path)
    alias_path = tmp_path / "cooperative-hardlink-alias.raw"
    interleaved = False

    def add_hardlink(point: str) -> None:
        nonlocal interleaved
        if point != "reclamation_unlink_after_preunlink_reproof" or interleaved:
            return
        try:
            os.link(object_path, alias_path)
        except (NotImplementedError, OSError) as exc:
            pytest.skip(f"hardlinks unavailable on this filesystem: {exc}")
        interleaved = True

    racing = _reopen(harness, fault=add_hardlink)
    with pytest.raises(RawArtifactIntegrityError, match="duurzaam gefenced"):
        racing.apply_source_local_v1(
            harness.request.request_sha256,
            predecessor_apply_receipt_sha256=predecessor,
        )
    assert interleaved is True
    assert not object_path.exists()
    assert alias_path.read_bytes() == harness.payloads["candidate-object"]
    with closing(sqlite3.connect(racing.catalog_path)) as database:
        row = database.execute(
            "SELECT phase,observed_st_nlink,object_path_absent,state FROM "
            "raw_artifact_reclamation_unlink_failures WHERE request_sha256=?",
            (harness.request.request_sha256,),
        ).fetchone()
        assert row == ("POST_UNLINK", 1, 1, "FAILED_FENCED")
        assert database.execute(
            "SELECT count(*) FROM raw_artifact_reclamation_unlink_durable "
            "WHERE request_sha256=?",
            (harness.request.request_sha256,),
        ).fetchone()[0] == 0
        assert database.execute(
            "SELECT count(*) FROM raw_artifact_reclamation_unlinked_batches "
            "WHERE request_sha256=?",
            (harness.request.request_sha256,),
        ).fetchone()[0] == 0
        assert database.execute(
            "SELECT count(*) FROM raw_artifact_reclamation_applies "
            "WHERE request_sha256=?",
            (harness.request.request_sha256,),
        ).fetchone()[0] == 0

    for _attempt in range(2):
        with pytest.raises(RawArtifactIntegrityError, match="duurzaam gefenced"):
            _reopen(harness).resume_journaled_raw_prune_v1(
                harness.request.request_sha256
            )
    assert RAW_ARTIFACT_RECLAMATION_PREWIRING_BLOCKERS == (
        "exclusive_filesystem_owner_dirfd_policy_required_for_"
        "crash_atomic_hardlink_exclusion",
    )


@pytest.mark.skipif(os.name != "nt", reason="Windows fail-closed contract")
def test_windows_requires_explicit_directory_sync_authority(
    tmp_path: Path,
) -> None:
    harness = _build_harness(tmp_path)
    no_directory_authority = _reopen(
        harness,
        with_directory_authority=False,
    )
    prepared = no_directory_authority.prepare_source_local_v2(harness.request)
    predecessor = _arm_commit(harness, prepared)

    assert no_directory_authority.reclamation_directory_sync_available is False
    with pytest.raises(RawArtifactIntegrityError, match="directory-sync"):
        no_directory_authority.apply_source_local_v1(
            harness.request.request_sha256,
            predecessor_apply_receipt_sha256=predecessor,
        )
    assert Path(harness.receipts["candidate-object"].artifact_path).is_file()
    assert no_directory_authority.load_source_local_apply_v1(
        harness.request.request_sha256
    ) is None
