from __future__ import annotations

from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from typing import Any

import pytest

from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceIntegrityError,
    AcquisitionEvidenceLifecycleError,
    AcquisitionEvidenceLifecycleStore,
    EvidenceRetentionPolicy,
    LIFECYCLE_RECLAMATION_PREWIRING_BLOCKERS,
)

from test_worker_acquisition_control_reclamation import (
    _Clock,
    _committed,
    _prepare_bindings,
)
from test_worker_acquisition_lifecycle_retention_v2 import (
    NOW,
    SYNC,
    _FaultOnce,
    _participant,
    _v2_context,
)
from test_worker_acquisition_source_reclamation_registry_closure import (
    _exact_route_context,
)


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _reopen(store: AcquisitionEvidenceLifecycleStore, *, offset: float = 1_000.0):
    return AcquisitionEvidenceLifecycleStore(
        store.root,
        clock=_Clock(NOW + offset),
        directory_fsync=SYNC,
    )


def _prepare_and_commit(context):
    assert context.request is not None
    participant = _participant(context, context.decisions)
    prepared = participant.prepare_source_local_v2(context.request)
    bindings = _prepare_bindings((prepared,))
    context.decisions.prepares = {item.role: item for item in bindings}
    context.decisions.committed = _committed(context.request, bindings)
    return participant, prepared


def _target_inventory(context) -> dict[str, Any]:
    assert context.request is not None
    store = context.sources.evidence
    with closing(sqlite3.connect(store.path)) as database:
        database.row_factory = sqlite3.Row
        member_row = database.execute(
            "SELECT * FROM lifecycle_reclamation_members "
            "WHERE request_sha256=?",
            (context.request.request_sha256,),
        ).fetchone()
        assert member_row is not None
        member = dict(member_row)
        index_row = database.execute(
            "SELECT * FROM archive_index WHERE event_id=?",
            (member["event_id"],),
        ).fetchone()
        assert index_row is not None
        index = dict(index_row)
        object_row = database.execute(
            "SELECT * FROM archive_objects WHERE content_sha256=?",
            (index["content_sha256"],),
        ).fetchone()
        segment_row = database.execute(
            "SELECT * FROM archive_segments WHERE segment_id=?",
            (index["segment_id"],),
        ).fetchone()
        assert object_row is not None and segment_row is not None
        archive_object = dict(object_row)
        segment = dict(segment_row)
    terminal = store.load_terminal_consumption(
        str(member["stream_id"]), int(member["stream_sequence"])
    )
    assert terminal is not None and terminal.status == "archived"
    return {
        "archive_index": index,
        "archive_object": archive_object,
        "member": member,
        "producer": terminal.producer_receipt,
        "proof": terminal.proof,
        "segment": segment,
        "payload": terminal.payload,
    }


def _assert_sql_rejected(
    path: Path,
    sql: str,
    parameters: tuple[object, ...] = (),
) -> None:
    with closing(sqlite3.connect(path)) as database:
        database.execute("PRAGMA foreign_keys=OFF")
        database.execute("BEGIN IMMEDIATE")
        try:
            # A connection outside the lifecycle authority does not register
            # the floor-digest UDF; both an ABORT guard and the resulting
            # missing-function DatabaseError are fail-closed write outcomes.
            with pytest.raises(sqlite3.DatabaseError):
                database.execute(sql, parameters)
        finally:
            database.rollback()


def _attack_statement(
    attack: str,
    inventory: dict[str, Any],
) -> tuple[str, tuple[object, ...]]:
    member = inventory["member"]
    archived = inventory["archive_index"]
    archive_object = inventory["archive_object"]
    producer = inventory["producer"]
    proof_json = json.dumps(
        inventory["proof"],
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    if attack == "producer_reinsert":
        return (
            "INSERT INTO producer_receipts("
            "producer_receipt_sha256,acquisition_job_id,consumer_id,stream_id,"
            "stream_sequence,global_sequence,desired_state_sha256,"
            "authority_sha256,proof_sha256,proof_json,content_sha256,size_bytes,"
            "created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                producer.producer_receipt_sha256,
                producer.acquisition_job_id,
                producer.consumer_id,
                producer.stream_id,
                producer.stream_sequence,
                producer.global_sequence,
                producer.desired_state_sha256,
                producer.authority_sha256,
                producer.proof_sha256,
                sqlite3.Binary(proof_json),
                producer.content_sha256,
                producer.size_bytes,
                producer.created_at,
            ),
        )
    if attack == "outbox_reinsert":
        return (
            "INSERT INTO completion_outbox("
            "event_id,event_sha256,producer_receipt_sha256,consumer_id,stream_id,"
            "stream_sequence,state,attempt_count,lease_owner,lease_token_sha256,"
            "lease_expires_at,superseded_by_event_id,supersession_sha256,"
            "ready_at,terminal_at) VALUES(?,?,?,?,?,?,'consumed',0,NULL,NULL,"
            "NULL,NULL,NULL,?,?)",
            (
                producer.event_id,
                producer.event_sha256,
                producer.producer_receipt_sha256,
                producer.consumer_id,
                producer.stream_id,
                producer.stream_sequence,
                producer.ready_at,
                producer.ready_at + 1.0,
            ),
        )
    if attack == "archive_index_reinsert":
        return (
            "INSERT INTO archive_index("
            "producer_receipt_sha256,acquisition_job_id,event_id,event_sha256,"
            "consumer_id,stream_id,stream_sequence,global_sequence,content_sha256,"
            "segment_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
            tuple(
                archived[key]
                for key in (
                    "producer_receipt_sha256",
                    "acquisition_job_id",
                    "event_id",
                    "event_sha256",
                    "consumer_id",
                    "stream_id",
                    "stream_sequence",
                    "global_sequence",
                    "content_sha256",
                    "segment_id",
                )
            ),
        )
    if attack == "archive_object_reinsert":
        return (
            "INSERT INTO archive_objects("
            "content_sha256,size_bytes,filename,created_at) VALUES(?,?,?,?)",
            (
                archive_object["content_sha256"],
                archive_object["size_bytes"],
                archive_object["filename"],
                archive_object["created_at"],
            ),
        )
    if attack == "archive_segment_delete":
        return (
            "DELETE FROM archive_segments WHERE segment_id=?",
            (member["lifecycle_archive_segment_id"],),
        )
    if attack == "floor_second_transition":
        return (
            "UPDATE lifecycle_archive_floor SET reclaimed_completion_count="
            "reclaimed_completion_count+1 WHERE singleton=1",
            (),
        )
    raise AssertionError(f"unknown attack: {attack}")


def _crash_in_applying(context):
    participant, prepared = _prepare_and_commit(context)
    inventory = _target_inventory(context)
    fault = _FaultOnce("reclamation.apply.after_logical_commit")
    fault.armed = True
    context.sources.evidence._fault_injector = fault
    with pytest.raises(RuntimeError, match="after_logical_commit"):
        participant.apply_source_local_v1(
            context.request.request_sha256,
            predecessor_apply_receipt_sha256=None,
        )
    assert fault.hit
    context.sources.evidence._fault_injector = None
    with closing(sqlite3.connect(context.sources.evidence.path)) as database:
        state = database.execute(
            "SELECT state FROM lifecycle_reclamation_decisions "
            "WHERE request_sha256=?",
            (context.request.request_sha256,),
        ).fetchone()
    assert state == ("APPLYING",)
    return participant, prepared, inventory


def _mutate_v4_authority(store: AcquisitionEvidenceLifecycleStore, kind: str) -> None:
    with closing(sqlite3.connect(store.path)) as database:
        if kind == "table_sql":
            row = database.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' "
                "AND name='lifecycle_reclamation_files'"
            ).fetchone()
            assert row is not None and isinstance(row[0], str)
            tampered = row[0].replace(
                "CREATE TABLE",
                "CREATE TABLE /* lifecycle-v4-tamper */",
                1,
            )
            assert tampered != row[0]
            schema_version = int(
                database.execute("PRAGMA schema_version").fetchone()[0]
            )
            database.execute("PRAGMA writable_schema=ON")
            database.execute(
                "UPDATE sqlite_master SET sql=? WHERE type='table' "
                "AND name='lifecycle_reclamation_files'",
                (tampered,),
            )
            database.execute("PRAGMA writable_schema=OFF")
            database.execute(f"PRAGMA schema_version={schema_version + 1}")
        elif kind == "extra_table":
            database.execute(
                "CREATE TABLE lifecycle_reclamation_unexpected(value TEXT)"
            )
        elif kind == "extra_index":
            database.execute(
                "CREATE INDEX lifecycle_reclamation_unexpected_idx ON "
                "lifecycle_reclamation_members(request_sha256)"
            )
        elif kind == "trigger_sql":
            database.execute(
                "DROP TRIGGER lifecycle_reclamation_members_no_update"
            )
            database.execute(
                "CREATE TRIGGER lifecycle_reclamation_members_no_update "
                "BEFORE UPDATE ON lifecycle_reclamation_members BEGIN "
                "SELECT 1; END"
            )
        elif kind == "extra_trigger":
            database.execute(
                "CREATE TRIGGER lifecycle_reclamation_unexpected_trigger "
                "AFTER INSERT ON lifecycle_reclamation_members BEGIN "
                "SELECT 1; END"
            )
        else:  # pragma: no cover - parameter inventory below is closed
            raise AssertionError(kind)
        database.commit()


def _downgrade_to_canonical_v3(store: AcquisitionEvidenceLifecycleStore) -> None:
    with closing(sqlite3.connect(store.path)) as database:
        trigger_names = set(store._reclamation_trigger_definitions()) | {
            "lifecycle_schema_no_update",
            "lifecycle_schema_no_delete",
        }
        for name in sorted(trigger_names):
            database.execute(f'DROP TRIGGER IF EXISTS "{name}"')
        v4_tables = [
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND "
                "(name='lifecycle_archive_floor' OR "
                "name LIKE 'lifecycle_reclamation_%')"
            )
        ]
        for table in sorted(v4_tables, reverse=True):
            database.execute(f'DROP TABLE "{table}"')
        database.execute(
            "UPDATE lifecycle_schema SET schema_version=3 WHERE singleton=1"
        )
        database.commit()


@pytest.mark.parametrize(
    "mutation",
    ("table_sql", "extra_table", "extra_index", "trigger_sql", "extra_trigger"),
)
def test_lifecycle_v4_rejects_noncanonical_schema_inventory(tmp_path, mutation):
    sources, _source_archive, _plan, _candidate = _exact_route_context(tmp_path)
    store = sources.evidence
    _mutate_v4_authority(store, mutation)

    with pytest.raises(AcquisitionEvidenceIntegrityError):
        _reopen(store)


def test_lifecycle_v4_public_load_revalidates_trigger_inventory(tmp_path):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        participant, _prepared = _prepare_and_commit(context)
        with closing(sqlite3.connect(context.sources.evidence.path)) as database:
            database.execute(
                "CREATE TRIGGER lifecycle_reclamation_runtime_injected "
                "AFTER INSERT ON lifecycle_reclamation_members BEGIN "
                "SELECT 1; END"
            )
            database.commit()

        with pytest.raises(AcquisitionEvidenceIntegrityError):
            participant.load_source_local_prepare_v2(
                context.request.request_sha256
            )


def test_lifecycle_canonical_v3_fixture_migrates_to_exact_v4(tmp_path):
    sources, _source_archive, _plan, candidate = _exact_route_context(tmp_path)
    store = sources.evidence
    identity_before = store.identity_sha256
    terminal_before = store.terminal_event_identity(
        candidate.evidence_stream_id,
        candidate.evidence_stream_sequence,
    )
    _downgrade_to_canonical_v3(store)

    migrated = _reopen(store)

    assert migrated.identity_sha256 == identity_before
    assert migrated.terminal_event_identity(
        candidate.evidence_stream_id,
        candidate.evidence_stream_sequence,
    ) == terminal_before
    with closing(sqlite3.connect(migrated.path)) as database:
        actual = database.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND "
            "name='lifecycle_reclamation_object_references'"
        ).fetchone()
    assert actual is not None
    expected = migrated._v4_reclamation_table_definitions()[
        "lifecycle_reclamation_object_references"
    ]
    assert migrated._normalized_sql(str(actual[0])) == (
        migrated._normalized_sql(expected)
    )
    assert migrated.audit_archive()["healthy"] is True


def test_lifecycle_v3_migration_rejects_partial_inventory(tmp_path):
    sources, _source_archive, _plan, _candidate = _exact_route_context(tmp_path)
    store = sources.evidence
    _downgrade_to_canonical_v3(store)
    with closing(sqlite3.connect(store.path)) as database:
        database.execute("DROP TABLE consumer_cursors")
        database.commit()

    with pytest.raises(AcquisitionEvidenceIntegrityError):
        _reopen(store)


@pytest.mark.parametrize(
    "attack",
    ("producer_reinsert", "outbox_reinsert"),
)
def test_lifecycle_prepared_fence_blocks_archived_semantic_reinserts(
    tmp_path,
    attack,
):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        _prepare_and_commit(context)
        inventory = _target_inventory(context)
        sql, parameters = _attack_statement(attack, inventory)

        _assert_sql_rejected(context.sources.evidence.path, sql, parameters)

        with closing(sqlite3.connect(context.sources.evidence.path)) as database:
            assert database.execute(
                "SELECT count(*) FROM producer_receipts"
            ).fetchone()[0] == 0
            assert database.execute(
                "SELECT count(*) FROM completion_outbox"
            ).fetchone()[0] == 0


def test_lifecycle_prepared_fence_blocks_new_shared_archive_reference(tmp_path):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        _prepare_and_commit(context)
        inventory = _target_inventory(context)
        archived = inventory["archive_index"]
        sql = (
            "INSERT INTO archive_index("
            "producer_receipt_sha256,acquisition_job_id,event_id,event_sha256,"
            "consumer_id,stream_id,stream_sequence,global_sequence,content_sha256,"
            "segment_id) VALUES(?,?,?,?,?,?,?,?,?,?)"
        )
        parameters = (
            _sha("prepared-shared-producer"),
            "prepared-shared-job",
            "prepared-shared-event",
            _sha("prepared-shared-event"),
            "prepared-shared-consumer",
            "prepared.shared.stream",
            1,
            int(archived["global_sequence"]) + 10_000,
            archived["content_sha256"],
            archived["segment_id"],
        )

        _assert_sql_rejected(context.sources.evidence.path, sql, parameters)


def test_lifecycle_object_reference_duplicate_within_request_fails_closed(
    tmp_path,
):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        _prepare_and_commit(context)
        with closing(sqlite3.connect(context.sources.evidence.path)) as database:
            unique_indexes = {
                tuple(
                    str(column[2])
                    for column in database.execute(
                        f"PRAGMA index_info('{index[1]}')"
                    )
                )
                for index in database.execute(
                    "PRAGMA index_list("
                    "'lifecycle_reclamation_object_references')"
                )
                if int(index[2]) == 1
            }
            assert (
                "request_sha256",
                "reference_sha256",
            ) in unique_indexes
            with pytest.raises(sqlite3.DatabaseError):
                database.execute(
                    "INSERT INTO lifecycle_reclamation_object_references "
                    "SELECT * FROM lifecycle_reclamation_object_references "
                    "WHERE request_sha256=?",
                    (context.request.request_sha256,),
                )
            count = int(database.execute(
                "SELECT count(*) FROM "
                "lifecycle_reclamation_object_references "
                "WHERE request_sha256=?",
                (context.request.request_sha256,),
            ).fetchone()[0])
        assert count == 1


@pytest.mark.parametrize(
    "attack",
    (
        "floor_second_transition",
        "archive_index_reinsert",
        "producer_reinsert",
        "outbox_reinsert",
        "archive_object_reinsert",
        "archive_segment_delete",
    ),
)
def test_lifecycle_applying_fence_rejects_interleaving_and_segment_delete(
    tmp_path,
    attack,
):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        participant, _prepared, inventory = _crash_in_applying(context)
        sql, parameters = _attack_statement(attack, inventory)

        _assert_sql_rejected(context.sources.evidence.path, sql, parameters)

        applied = participant.apply_source_local_v1(
            context.request.request_sha256,
            predecessor_apply_receipt_sha256=None,
        )
        assert applied.released_entries == 1
        assert context.sources.evidence.audit_archive()["healthy"] is True


def test_lifecycle_applied_load_and_replay_reprove_absent_segment_bytes(tmp_path):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        participant, _prepared = _prepare_and_commit(context)
        inventory = _target_inventory(context)
        segment = inventory["segment"]
        segment_path = (
            context.sources.evidence.archive_segment_directory
            / str(segment["filename"])
        )
        segment_bytes = segment_path.read_bytes()
        applied = participant.apply_source_local_v1(
            context.request.request_sha256,
            predecessor_apply_receipt_sha256=None,
        )
        assert not segment_path.exists()

        segment_path.write_bytes(segment_bytes)

        with pytest.raises(AcquisitionEvidenceLifecycleError):
            participant.load_source_local_apply_v1(
                context.request.request_sha256
            )
        with pytest.raises(AcquisitionEvidenceLifecycleError):
            participant.apply_source_local_v1(
                context.request.request_sha256,
                predecessor_apply_receipt_sha256=None,
            )
        assert applied.released_entries == 1


def test_lifecycle_hardlink_alias_keeps_applying_fenced_until_removed(tmp_path):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        participant, _prepared = _prepare_and_commit(context)
        inventory = _target_inventory(context)
        segment = inventory["segment"]
        segment_path = (
            context.sources.evidence.archive_segment_directory
            / str(segment["filename"])
        )
        segment_bytes = segment_path.read_bytes()
        fault = _FaultOnce("reclamation.apply.after_logical_commit")
        fault.armed = True
        context.sources.evidence._fault_injector = fault
        with pytest.raises(RuntimeError, match="after_logical_commit"):
            participant.apply_source_local_v1(
                context.request.request_sha256,
                predecessor_apply_receipt_sha256=None,
            )
        assert fault.hit
        context.sources.evidence._fault_injector = None

        alias_path = tmp_path / "external-segment-hardlink.bin"
        os.link(segment_path, alias_path)
        assert segment_path.stat().st_nlink == 2
        assert alias_path.read_bytes() == segment_bytes

        reopened_store = _reopen(context.sources.evidence, offset=2_000.0)
        replay = reopened_store.reclamation_participant_v2(
            candidate_authority=context.archive,
            committed_decision_authority=context.decisions,
        )
        with pytest.raises(
            AcquisitionEvidenceIntegrityError,
            match="evidence object file differs",
        ):
            replay.apply_source_local_v1(
                context.request.request_sha256,
                predecessor_apply_receipt_sha256=None,
            )
        assert replay.load_source_local_apply_v1(
            context.request.request_sha256
        ) is None
        with closing(sqlite3.connect(reopened_store.path)) as database:
            state = database.execute(
                "SELECT state FROM lifecycle_reclamation_decisions "
                "WHERE request_sha256=?",
                (context.request.request_sha256,),
            ).fetchone()
        assert state == ("APPLYING",)
        assert segment_path.read_bytes() == segment_bytes
        assert alias_path.read_bytes() == segment_bytes

        alias_path.unlink()
        applied = replay.apply_source_local_v1(
            context.request.request_sha256,
            predecessor_apply_receipt_sha256=None,
        )
        assert applied.released_entries == 1
        assert not segment_path.exists()
        assert not alias_path.exists()


def test_lifecycle_hardlink_policy_blocker_is_machine_readable_and_unwired():
    assert LIFECYCLE_RECLAMATION_PREWIRING_BLOCKERS == (
        "exclusive_filesystem_owner_dirfd_policy_required_for_"
        "crash_atomic_hardlink_exclusion",
    )
    service = (
        Path(__file__).resolve().parents[1]
        / "storm_tracker_worker"
        / "service.py"
    ).read_text(encoding="utf-8")
    assert "acquisition_evidence_lifecycle" not in service


def test_lifecycle_applied_tombstones_block_old_identities_but_allow_exact_sha_reuse(
    tmp_path,
):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        participant, _prepared = _prepare_and_commit(context)
        inventory = _target_inventory(context)
        old_producer = inventory["producer"]
        old_content_sha = old_producer.content_sha256
        old_payload = inventory["payload"]
        participant.apply_source_local_v1(
            context.request.request_sha256,
            predecessor_apply_receipt_sha256=None,
        )

        for attack in (
            "archive_index_reinsert",
            "producer_reinsert",
            "outbox_reinsert",
        ):
            sql, parameters = _attack_statement(attack, inventory)
            _assert_sql_rejected(
                context.sources.evidence.path,
                sql,
                parameters,
            )

        orphan_sql, orphan_parameters = _attack_statement(
            "archive_object_reinsert", inventory
        )
        _assert_sql_rejected(
            context.sources.evidence.path,
            orphan_sql,
            orphan_parameters,
        )

        new_producer = context.sources.evidence.publish(
            acquisition_job_id="post-reclamation-same-content-job",
            consumer_id=old_producer.consumer_id,
            stream_id=old_producer.stream_id,
            desired_state_sha256="7" * 64,
            authority_sha256="8" * 64,
            proof={
                "predecessor_content_sha256": old_content_sha,
                "schema": "lifecycle-v4-content-reuse-proof-v1",
            },
            payload=old_payload,
        )
        assert new_producer.content_sha256 == old_content_sha
        assert new_producer.acquisition_job_id != old_producer.acquisition_job_id
        assert new_producer.stream_sequence > old_producer.stream_sequence
        lease = context.sources.evidence.claim_next(
            new_producer.consumer_id,
            new_producer.stream_id,
            "post-reclamation-consumer",
            now=NOW + 3_000.0,
        )
        assert lease is not None and lease.event_id == new_producer.event_id
        context.sources.evidence.acknowledge(
            lease.event_id,
            lease.lease_token,
            output_sha256=_sha("post-reclamation-output"),
            now=NOW + 3_001.0,
        )
        rotated = context.sources.evidence.rotate_archive(
            policy=EvidenceRetentionPolicy(max_segment_completions=1),
            now=NOW + 4_000.0,
        )
        assert rotated.archived_completions == 1

        with closing(sqlite3.connect(context.sources.evidence.path)) as database:
            database.row_factory = sqlite3.Row
            retained = database.execute(
                "SELECT object.size_bytes,object.filename,segment.segment_ordinal,"
                "floor.pruned_through_segment_ordinal FROM archive_index AS idx "
                "JOIN archive_objects AS object USING(content_sha256) "
                "JOIN archive_segments AS segment USING(segment_id) "
                "JOIN lifecycle_archive_floor AS floor ON floor.singleton=1 "
                "WHERE idx.producer_receipt_sha256=?",
                (new_producer.producer_receipt_sha256,),
            ).fetchone()
        assert retained is not None
        assert int(retained["size_bytes"]) == len(old_payload)
        assert retained["filename"] == f"{old_content_sha}.raw"
        assert int(retained["segment_ordinal"]) > int(
            retained["pruned_through_segment_ordinal"]
        )
        assert context.sources.evidence.audit_archive()["healthy"] is True
