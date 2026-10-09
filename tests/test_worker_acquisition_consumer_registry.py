from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys

import pytest

from storm_tracker_worker import acquisition_consumer_registry as subject
from storm_tracker_worker.acquisition_consumer_registry import (
    AcquisitionConsumerClosureReceipt,
    AcquisitionConsumerRegistryAuthority,
    AcquisitionConsumerRegistryCapacityError,
    AcquisitionConsumerRegistryConflict,
    AcquisitionConsumerRegistryIntegrityError,
    AcquisitionConsumerRegistryLimits,
    AcquisitionConsumerRegistryOwnershipError,
    build_consumer_registry_root,
    parse_consumer_closure_receipt,
)
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    CompletionProducerReceipt,
    ConsumptionReceipt,
    TerminalEventIdentity,
)
from storm_tracker_worker.completion_output_archive import (
    CompletionOutputArchiveSourceCatalog,
    CompletionOutputArchiveSourceProfile,
    CompletionOutputArchiveSourceReproofAuthority,
    CompletionOutputArchiveSourceReproofBinding,
)
from storm_tracker_worker.completion_output_archive_adapters import (
    ALL_ADAPTER_AUTHORITY_ID,
    ALL_ADAPTER_CATALOG_ID,
    ALL_ARCHIVE_WORKLOAD_IDS,
)
from storm_tracker_worker.terminal_raw_completion_router import (
    TERMINAL_RAW_CONSUMER_CATALOG,
)
from storm_tracker_worker.worker_acquisition_plan import (
    CATALOG_SHA256,
    acquisition_catalog_snapshot,
)


NOW = 1_786_100_000.0


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _cadences() -> dict[str, int]:
    return {
        str(row["workload_id"]): int(row["cadence_seconds"])
        for row in acquisition_catalog_snapshot()["workloads"]
        if row["workload_id"] in TERMINAL_RAW_CONSUMER_CATALOG
    }


def _resolver(_record):
    raise AssertionError("registry construction must not call archive resolvers")


def _archive_authorities(
    salt: str = "one",
) -> tuple[
    CompletionOutputArchiveSourceCatalog,
    CompletionOutputArchiveSourceReproofAuthority,
]:
    cadences = _cadences()
    profiles = []
    bindings = []
    for index, workload_id in enumerate(sorted(TERMINAL_RAW_CONSUMER_CATALOG)):
        consumer = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        profile = CompletionOutputArchiveSourceProfile(
            workload_id=workload_id,
            consumer_id=consumer.consumer_id,
            stream_id=consumer.stream_id,
            cadence_seconds=cadences[workload_id],
            source_store_contract=f"test-store-{index}-v1",
            source_store_schema_version=1,
            output_document_schema=f"test-output-{index}-v1",
            output_receipt_schema=f"test-output-receipt-{index}-v1",
            process_receipt_schema=f"test-process-receipt-{index}-v1",
            source_limits_sha256=_sha(f"{salt}:{workload_id}:limits"),
            effect_contract_sha256=_sha(f"{salt}:{workload_id}:effect"),
            acquisition_catalog_sha256=CATALOG_SHA256,
            consumer_binding_sha256=consumer.fingerprint,
            source_immutability_mode="append_only_sqlite_triggers",
            source_row_reproof_contract=f"test-reproof-{index}-v1",
        )
        profiles.append(profile)
        bindings.append(
            CompletionOutputArchiveSourceReproofBinding(
                source_profile_sha256=profile.fingerprint,
                resolver_id=f"all-terminal-source-row:{workload_id}:v1",
                resolver_sha256=_sha(f"{salt}:{workload_id}:resolver"),
                source_store_identity_sha256=_sha(
                    f"{salt}:{workload_id}:store"
                ),
                reproof_contract=profile.source_row_reproof_contract,
                resolver=_resolver,
            )
        )
    return (
        CompletionOutputArchiveSourceCatalog(
            catalog_id=ALL_ADAPTER_CATALOG_ID,
            profiles=tuple(profiles),
        ),
        CompletionOutputArchiveSourceReproofAuthority(
            authority_id=ALL_ADAPTER_AUTHORITY_ID,
            bindings=tuple(bindings),
        ),
    )


class FaultOnce:
    def __init__(self, point: str) -> None:
        self.point = point
        self.hit = False

    def __call__(self, point: str) -> None:
        if point == self.point and not self.hit:
            self.hit = True
            raise RuntimeError(f"fault:{point}")


def _process_open_attempt(path: str, queue) -> None:
    try:
        authority = AcquisitionConsumerRegistryAuthority(Path(path))
    except AcquisitionConsumerRegistryOwnershipError:
        queue.put("fenced")
        return
    except BaseException as error:
        queue.put(f"unexpected:{type(error).__name__}")
        return
    authority.close()
    queue.put("opened")


def test_exact_catalog_root_binds_consumers_streams_and_archive_resolvers():
    catalog, reproof = _archive_authorities()
    root = build_consumer_registry_root(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    )

    assert len(root.members) == subject.EXPECTED_MEMBER_COUNT == len(
        TERMINAL_RAW_CONSUMER_CATALOG
    )
    assert set(root.by_workload) == set(ALL_ARCHIVE_WORKLOAD_IDS)
    assert set(root.by_workload) == set(TERMINAL_RAW_CONSUMER_CATALOG)
    assert root.archive_source_catalog_sha256 == catalog.fingerprint
    assert root.archive_reproof_authority_sha256 == reproof.fingerprint
    assert subject._SHA256.fullmatch(root.root_sha256) is not None
    for workload_id, member in root.by_workload.items():
        consumer = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        assert member.consumer_id == consumer.consumer_id
        assert member.stream_id == consumer.stream_id == workload_id
        assert member.consumer_binding_sha256 == consumer.fingerprint
        assert member.archive_resolver_id == (
            f"all-terminal-source-row:{workload_id}:v1"
        )
    encoded = json.dumps(root.as_dict(), sort_keys=True).casefold()
    for forbidden in ("artifact_path", "password", "authorization", "https://"):
        assert forbidden not in encoded


def test_root_builder_rejects_missing_forged_or_unbound_membership():
    catalog, reproof = _archive_authorities()
    missing = CompletionOutputArchiveSourceCatalog(
        catalog_id=ALL_ADAPTER_CATALOG_ID,
        profiles=catalog.profiles[:-1],
    )
    with pytest.raises(
        AcquisitionConsumerRegistryIntegrityError,
        match="exact-catalog",
    ):
        build_consumer_registry_root(
            source_catalog=missing,
            source_reproof_authority=reproof,
        )

    bindings = list(reproof.bindings)
    bindings[0] = replace(bindings[0], resolver_id="forged-resolver-v1")
    forged = CompletionOutputArchiveSourceReproofAuthority(
        authority_id=ALL_ADAPTER_AUTHORITY_ID,
        bindings=tuple(bindings),
    )
    with pytest.raises(AcquisitionConsumerRegistryIntegrityError, match="resolver"):
        build_consumer_registry_root(
            source_catalog=catalog,
            source_reproof_authority=forged,
        )

    duplicate_hash_bindings = list(reproof.bindings)
    duplicate_hash_bindings[0] = replace(
        duplicate_hash_bindings[0],
        resolver_sha256=duplicate_hash_bindings[1].resolver_sha256,
    )
    duplicate_hashes = CompletionOutputArchiveSourceReproofAuthority(
        authority_id=ALL_ADAPTER_AUTHORITY_ID,
        bindings=tuple(duplicate_hash_bindings),
    )
    with pytest.raises(
        AcquisitionConsumerRegistryIntegrityError,
        match="exact-catalog",
    ):
        build_consumer_registry_root(
            source_catalog=catalog,
            source_reproof_authority=duplicate_hashes,
        )

    profiles = list(catalog.profiles)
    profiles[0] = replace(profiles[0], consumer_binding_sha256="f" * 64)
    drifted = CompletionOutputArchiveSourceCatalog(
        catalog_id=ALL_ADAPTER_CATALOG_ID,
        profiles=tuple(profiles),
    )
    with pytest.raises(AcquisitionConsumerRegistryIntegrityError, match="resolver"):
        build_consumer_registry_root(
            source_catalog=drifted,
            source_reproof_authority=reproof,
        )


def test_epoch_is_monotone_create_once_and_public_verifier_is_exact(tmp_path):
    catalog, reproof = _archive_authorities()
    authority = AcquisitionConsumerRegistryAuthority(
        (tmp_path / "registry.sqlite3").resolve(),
        clock=lambda: NOW,
        token_factory=lambda: "1" * 64,
    )
    first = authority.ensure_epoch(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    )
    replay = authority.ensure_epoch(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    )
    assert replay == first
    assert first.epoch == 1
    assert first.previous_epoch_root_sha256 is None
    current = authority.load_current()
    assert current is not None
    root, receipt = current
    assert receipt == first

    verifier = authority.verifier(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    )
    verified = verifier.verify(
        claimed_registry_root_sha256=root.root_sha256,
        claimed_epoch_root_sha256=receipt.epoch_root_sha256,
    )
    assert verified.registry_root_sha256 == root.root_sha256
    assert verified.epoch_root_sha256 == receipt.epoch_root_sha256
    assert verified.epoch_receipt_sha256 == receipt.receipt_sha256
    assert verified.member_count == subject.EXPECTED_MEMBER_COUNT
    with pytest.raises(
        AcquisitionConsumerRegistryConflict,
        match="live archive membership",
    ):
        verifier.verify(
            claimed_registry_root_sha256="f" * 64,
            claimed_epoch_root_sha256=receipt.epoch_root_sha256,
        )
    assert authority.diagnostics() == {
        "contract": subject.STORE_CONTRACT,
        "current_epoch": 1,
        "current_epoch_root_sha256": receipt.epoch_root_sha256,
        "current_registry_root_sha256": root.root_sha256,
        "healthy": True,
        "member_count": subject.EXPECTED_MEMBER_COUNT,
        "registry_roots": 1,
        "runtime_wiring": "inert",
    }
    authority.close()


def test_second_root_advances_epoch_and_historical_root_cannot_reactivate(tmp_path):
    first_catalog, first_reproof = _archive_authorities("first")
    second_catalog, second_reproof = _archive_authorities("second")
    with AcquisitionConsumerRegistryAuthority(
        (tmp_path / "registry.sqlite3").resolve(),
        clock=lambda: NOW,
    ) as authority:
        first = authority.ensure_epoch(
            source_catalog=first_catalog,
            source_reproof_authority=first_reproof,
        )
        first_verifier = authority.verifier(
            source_catalog=first_catalog,
            source_reproof_authority=first_reproof,
        )
        authority._clock = lambda: NOW + 1.0
        second = authority.ensure_epoch(
            source_catalog=second_catalog,
            source_reproof_authority=second_reproof,
        )
        assert second.epoch == 2
        assert second.previous_epoch_root_sha256 == first.epoch_root_sha256
        with pytest.raises(AcquisitionConsumerRegistryConflict, match="historical"):
            authority.ensure_epoch(
                source_catalog=first_catalog,
                source_reproof_authority=first_reproof,
            )
        with pytest.raises(AcquisitionConsumerRegistryConflict, match="not current"):
            authority.verify_current_epoch_roots(
                claimed_registry_root_sha256=first.registry_root_sha256,
                claimed_epoch_root_sha256=first.epoch_root_sha256,
            )
        with pytest.raises(
            AcquisitionConsumerRegistryConflict,
            match="live archive membership",
        ):
            first_verifier.verify(
                claimed_registry_root_sha256=second.registry_root_sha256,
                claimed_epoch_root_sha256=second.epoch_root_sha256,
            )


def test_restart_preserves_instance_and_epoch_but_advances_owner_fence(tmp_path):
    catalog, reproof = _archive_authorities()
    path = (tmp_path / "registry.sqlite3").resolve()
    first = AcquisitionConsumerRegistryAuthority(
        path,
        clock=lambda: NOW,
        token_factory=lambda: "1" * 64,
    )
    receipt = first.ensure_epoch(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    )
    identity = first.identity_sha256
    owner_epoch = first._owner_epoch
    first.close()

    reopened = AcquisitionConsumerRegistryAuthority(
        path,
        clock=lambda: NOW + 1.0,
        token_factory=lambda: "2" * 64,
    )
    assert reopened.identity_sha256 == identity
    assert reopened._owner_epoch == owner_epoch + 1
    current = reopened.load_current()
    assert current is not None and current[1] == receipt
    assert reopened.ensure_epoch(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    ) == receipt
    reopened.close()


def test_process_local_owner_fence_and_threaded_duplicate_are_exact(tmp_path):
    catalog, reproof = _archive_authorities()
    path = (tmp_path / "registry.sqlite3").resolve()
    authority = AcquisitionConsumerRegistryAuthority(path, clock=lambda: NOW)
    with pytest.raises(AcquisitionConsumerRegistryOwnershipError, match="process owner"):
        AcquisitionConsumerRegistryAuthority(path, clock=lambda: NOW)

    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = tuple(
            pool.map(
                lambda _index: authority.ensure_epoch(
                    source_catalog=catalog,
                    source_reproof_authority=reproof,
                ),
                range(16),
            )
        )
    assert len(set(receipts)) == 1
    assert authority.diagnostics()["registry_roots"] == 1
    authority.close()


def test_epoch_capacity_and_root_privacy_are_bounded(tmp_path):
    first_catalog, first_reproof = _archive_authorities("first")
    second_catalog, second_reproof = _archive_authorities("second")
    path = (tmp_path / "registry.sqlite3").resolve()
    limits = AcquisitionConsumerRegistryLimits(max_epochs=1)
    with AcquisitionConsumerRegistryAuthority(
        path,
        limits=limits,
        clock=lambda: NOW,
    ) as authority:
        authority.ensure_epoch(
            source_catalog=first_catalog,
            source_reproof_authority=first_reproof,
        )
        with pytest.raises(AcquisitionConsumerRegistryCapacityError, match="cap"):
            authority.ensure_epoch(
                source_catalog=second_catalog,
                source_reproof_authority=second_reproof,
            )
        diagnostic = json.dumps(authority.diagnostics(), sort_keys=True)
        assert str(tmp_path) not in diagnostic
        with closing(sqlite3.connect(path)) as database:
            body = bytes(
                database.execute("SELECT root_json FROM registry_roots").fetchone()[0]
            ).decode("ascii")
        assert str(tmp_path) not in body
        for forbidden in ("artifact_path", "password", "authorization", "https://"):
            assert forbidden not in body.casefold()


def test_metadata_root_owner_and_schema_tamper_fail_closed(tmp_path):
    catalog, reproof = _archive_authorities()
    path = (tmp_path / "registry.sqlite3").resolve()
    authority = AcquisitionConsumerRegistryAuthority(path, clock=lambda: NOW)
    authority.ensure_epoch(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    )
    with closing(sqlite3.connect(path)) as database:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "UPDATE registry_roots SET root_bytes=root_bytes+1"
            )
        database.rollback()
        database.execute("DROP TRIGGER registry_metadata_no_update")
        database.execute(
            "UPDATE registry_metadata SET store_instance_sha256=?",
            ("f" * 64,),
        )
        database.execute(
            authority._trigger_definitions()["registry_metadata_no_update"]
        )
        database.commit()
    with pytest.raises(AcquisitionConsumerRegistryIntegrityError, match="instance"):
        authority.diagnostics()
    authority.close()

    schema_path = (tmp_path / "schema.sqlite3").resolve()
    schema = AcquisitionConsumerRegistryAuthority(schema_path, clock=lambda: NOW)
    with closing(sqlite3.connect(schema_path)) as database:
        database.execute("CREATE TABLE injected(value TEXT)")
        database.commit()
    with pytest.raises(AcquisitionConsumerRegistryIntegrityError, match="objects"):
        schema.diagnostics()
    schema.close()

    owner_path = (tmp_path / "owner.sqlite3").resolve()
    owner = AcquisitionConsumerRegistryAuthority(owner_path, clock=lambda: NOW)
    with closing(sqlite3.connect(owner_path)) as database:
        database.execute("DROP TRIGGER registry_owner_update_guard")
        database.execute(
            "UPDATE registry_owner SET owner_epoch=owner_epoch+1,"
            "owner_token_sha256=?,owner_process_id=?,acquired_at=?",
            ("e" * 64, os.getpid(), NOW + 1.0),
        )
        database.execute(
            owner._trigger_definitions()["registry_owner_update_guard"]
        )
        database.commit()
    with pytest.raises(AcquisitionConsumerRegistryOwnershipError, match="fence"):
        owner.diagnostics()
    owner.close()


def test_same_path_replacement_is_detected_and_read_only_reopen_is_exact(tmp_path):
    catalog, reproof = _archive_authorities()
    path = (tmp_path / "registry.sqlite3").resolve()
    authority = AcquisitionConsumerRegistryAuthority(path, clock=lambda: NOW)
    receipt = authority.ensure_epoch(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    )
    identity = authority.identity_sha256
    replacement = (tmp_path / "replacement.sqlite3").resolve()
    shutil.copy2(path, replacement)
    os.replace(replacement, path)
    with pytest.raises(AcquisitionConsumerRegistryIntegrityError, match="instance changed"):
        authority.load_current()
    authority.close()

    reopened = AcquisitionConsumerRegistryAuthority(path, clock=lambda: NOW + 1.0)
    assert reopened.identity_sha256 != identity
    current = reopened.load_current()
    assert current is not None and current[1] == receipt
    reopened.close()


def test_hardlinked_database_is_rejected(tmp_path):
    path = (tmp_path / "registry.sqlite3").resolve()
    authority = AcquisitionConsumerRegistryAuthority(path)
    authority.close()
    alias = (tmp_path / "registry-hardlink.sqlite3").resolve()
    try:
        os.link(path, alias)
    except OSError:
        pytest.skip("hard links are unavailable")
    with pytest.raises(AcquisitionConsumerRegistryIntegrityError, match="unsafe"):
        AcquisitionConsumerRegistryAuthority(path)


def _bound_epoch_verifier(tmp_path, *, salt="closure"):
    catalog, reproof = _archive_authorities(salt)
    authority = AcquisitionConsumerRegistryAuthority(
        (tmp_path / "registry.sqlite3").resolve(),
        clock=lambda: NOW,
    )
    epoch = authority.ensure_epoch(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    )
    root_verifier = authority.verifier(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    )
    return (
        authority,
        root_verifier.bind_current_epoch(
            claimed_epoch_root_sha256=epoch.epoch_root_sha256,
        ),
    )


def _terminal_pair(*, consumer_id, stream_id):
    producer = CompletionProducerReceipt(
        producer_receipt_sha256=_sha("closure:producer"),
        event_id="closure-event-v1",
        event_sha256=_sha("closure:event"),
        acquisition_job_id="closure-job-v1",
        consumer_id=consumer_id,
        stream_id=stream_id,
        stream_sequence=7,
        global_sequence=7,
        desired_state_sha256=_sha("closure:desired"),
        authority_sha256=_sha("closure:authority"),
        proof_sha256=_sha("closure:proof"),
        content_sha256=_sha("closure:content"),
        size_bytes=123,
        created_at=NOW - 10.0,
        ready_at=NOW - 9.0,
    )
    consumption = ConsumptionReceipt(
        receipt_sha256=_sha("closure:consumption"),
        event_id=producer.event_id,
        event_sha256=producer.event_sha256,
        producer_receipt_sha256=producer.producer_receipt_sha256,
        consumer_id=producer.consumer_id,
        stream_id=producer.stream_id,
        stream_sequence=producer.stream_sequence,
        output_sha256=_sha("closure:output"),
        created_at=NOW - 8.0,
    )
    terminal = TerminalEventIdentity(
        status="hot",
        event_id=producer.event_id,
        event_sha256=producer.event_sha256,
        producer_receipt_sha256=producer.producer_receipt_sha256,
        consumer_id=producer.consumer_id,
        stream_id=producer.stream_id,
        stream_sequence=producer.stream_sequence,
        terminal_kind="consumed",
        terminal_output_sha256=consumption.output_sha256,
        terminal_at=consumption.created_at,
    )
    return producer, consumption, terminal


def test_bound_epoch_issues_exact_content_addressed_consumer_closure(tmp_path):
    authority, bound = _bound_epoch_verifier(tmp_path)
    workload = "radar.dwd.radolan_rv"
    member = bound.registry_root.by_workload[workload]
    producer, consumption, terminal = _terminal_pair(
        consumer_id=member.consumer_id,
        stream_id=member.stream_id,
    )
    closure = bound.issue_consumer_closure(
        workload_id=workload,
        candidate_sha256=_sha("closure:candidate"),
        acquisition_job_id=producer.acquisition_job_id,
        producer_receipt=producer,
        consumption_receipt=consumption,
        terminal_identity=terminal,
        source_archive_anchor_sha256=_sha("closure:archive-anchor"),
        source_archive_segment_sha256=_sha("closure:archive-segment"),
        source_archive_record_sha256=_sha("closure:archive-record"),
    )

    assert isinstance(closure, AcquisitionConsumerClosureReceipt)
    assert closure.registry_root_sha256 == bound.registry_root_sha256
    assert closure.registry_epoch_root_sha256 == bound.epoch_root_sha256
    assert closure.registry_store_identity_sha256 == bound.store_identity_sha256
    assert closure.registry_member_sha256 == member.fingerprint
    assert closure.lifecycle_terminal_identity_sha256 == terminal.identity_sha256
    assert parse_consumer_closure_receipt(closure.as_dict()) == closure
    bound.validate_consumer_closure(closure)

    tampered = {**closure.as_dict(), "stream_sequence": 8}
    with pytest.raises(
        AcquisitionConsumerRegistryIntegrityError,
        match="receipt checksum",
    ):
        parse_consumer_closure_receipt(tampered)
    wrong_producer = replace(producer, consumer_id="forged-consumer-v1")
    with pytest.raises(
        AcquisitionConsumerRegistryIntegrityError,
        match="route or lifecycle",
    ):
        bound.issue_consumer_closure(
            workload_id=workload,
            candidate_sha256=_sha("closure:candidate"),
            acquisition_job_id=producer.acquisition_job_id,
            producer_receipt=wrong_producer,
            consumption_receipt=consumption,
            terminal_identity=terminal,
            source_archive_anchor_sha256=_sha("closure:archive-anchor"),
            source_archive_segment_sha256=_sha("closure:archive-segment"),
            source_archive_record_sha256=_sha("closure:archive-record"),
        )
    authority.close()


def test_bound_epoch_fails_closed_after_registry_epoch_advances(tmp_path):
    authority, bound = _bound_epoch_verifier(tmp_path)
    second_catalog, second_reproof = _archive_authorities("closure-next")
    authority.ensure_epoch(
        source_catalog=second_catalog,
        source_reproof_authority=second_reproof,
    )
    with pytest.raises(AcquisitionConsumerRegistryConflict, match="not current"):
        bound.verify_held()
    authority.close()


def test_crash_before_epoch_commit_is_empty_and_after_commit_replays(tmp_path):
    catalog, reproof = _archive_authorities()
    rollback_path = (tmp_path / "rollback.sqlite3").resolve()
    rollback = AcquisitionConsumerRegistryAuthority(
        rollback_path,
        clock=lambda: NOW,
        fault_injector=FaultOnce("epoch.after_root_insert"),
    )
    with pytest.raises(RuntimeError, match="epoch.after_root_insert"):
        rollback.ensure_epoch(
            source_catalog=catalog,
            source_reproof_authority=reproof,
        )
    assert rollback.load_current() is None
    assert rollback.diagnostics()["registry_roots"] == 0
    rollback.close()

    committed_path = (tmp_path / "committed.sqlite3").resolve()
    committed = AcquisitionConsumerRegistryAuthority(
        committed_path,
        clock=lambda: NOW,
        fault_injector=FaultOnce("epoch.after_commit"),
    )
    with pytest.raises(RuntimeError, match="epoch.after_commit"):
        committed.ensure_epoch(
            source_catalog=catalog,
            source_reproof_authority=reproof,
        )
    current = committed.load_current()
    assert current is not None
    committed._fault_injector = None
    assert committed.ensure_epoch(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    ) == current[1]
    committed.close()


_CONSUMER_REGISTRY_ALL_NATIVE_CRASH_POINTS = {
    "_initialize": (
        "initialize.before_commit",
        "initialize.after_commit",
    ),
    "_claim_durable_owner": (
        "owner.before_commit",
        "owner.after_commit",
    ),
    "ensure_epoch": (
        "epoch.after_root_insert",
        "epoch.after_epoch_insert",
        "epoch.before_commit",
        "epoch.after_commit",
    ),
}
_CONSUMER_REGISTRY_NATIVE_CRASH_CASES = (
    ("initialize", "initialize.before_commit", False, 0),
    ("initialize", "initialize.after_commit", False, 0),
    ("owner", "owner.before_commit", False, 0),
    ("owner", "owner.after_commit", False, 1),
    ("epoch", "epoch.after_root_insert", False, 1),
    ("epoch", "epoch.after_epoch_insert", False, 1),
    ("epoch", "epoch.before_commit", False, 1),
    ("epoch", "epoch.after_commit", True, 1),
)
_CONSUMER_REGISTRY_TABLES = frozenset({
    "registry_metadata",
    "registry_owner",
    "registry_roots",
    "registry_epochs",
})


def _consumer_registry_faultpoint_inventory() -> dict[str, tuple[str, ...]]:
    source = Path(subject.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(subject.__file__))
    authority = next(
        node
        for node in tree.body
        if (
            isinstance(node, ast.ClassDef)
            and node.name == "AcquisitionConsumerRegistryAuthority"
        )
    )
    discovered: dict[str, tuple[str, ...]] = {}
    for method in authority.body:
        if not isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        calls = tuple(
            call
            for call in ast.walk(method)
            if (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and isinstance(call.func.value, ast.Name)
                and call.func.value.id == "self"
                and call.func.attr == "_fault"
                and len(call.args) == 1
                and isinstance(call.args[0], ast.Constant)
                and isinstance(call.args[0].value, str)
            )
        )
        if calls:
            discovered[method.name] = tuple(
                str(call.args[0].value)
                for call in sorted(
                    calls,
                    key=lambda item: (item.lineno, item.col_offset),
                )
            )
    return discovered


def _consumer_registry_raw_snapshot(path: Path) -> dict[str, object]:
    with closing(sqlite3.connect(path)) as database:
        tables = frozenset(
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            if not str(row[0]).startswith("sqlite_")
        )
        integrity = str(database.execute("PRAGMA integrity_check").fetchone()[0])
        if not tables:
            return {
                "epoch_rows": 0,
                "integrity": integrity,
                "owner_epoch": None,
                "owner_process_id": None,
                "root_rows": 0,
                "tables": tables,
            }
        owner = database.execute(
            "SELECT owner_epoch,owner_process_id FROM registry_owner "
            "WHERE singleton=1"
        ).fetchone()
        return {
            "epoch_rows": int(
                database.execute("SELECT count(*) FROM registry_epochs").fetchone()[0]
            ),
            "integrity": integrity,
            "owner_epoch": int(owner[0]),
            "owner_process_id": int(owner[1]),
            "root_rows": int(
                database.execute("SELECT count(*) FROM registry_roots").fetchone()[0]
            ),
            "tables": tables,
        }


def test_consumer_registry_native_crash_inventory_pins_every_occurrence():
    discovered = _consumer_registry_faultpoint_inventory()
    assert discovered == _CONSUMER_REGISTRY_ALL_NATIVE_CRASH_POINTS
    all_points = tuple(
        point for points in discovered.values() for point in points
    )
    cases = tuple(
        point
        for _operation, point, _committed, _owner_epoch
        in _CONSUMER_REGISTRY_NATIVE_CRASH_CASES
    )
    assert len(all_points) == len(set(all_points)) == 8
    assert cases == all_points


@pytest.mark.parametrize(
    ("operation", "checkpoint", "committed", "owner_epoch_at_crash"),
    _CONSUMER_REGISTRY_NATIVE_CRASH_CASES,
    ids=(
        "initialize-before-commit-old",
        "initialize-after-commit-schema-only",
        "owner-before-commit-old",
        "owner-after-commit-new",
        "epoch-after-root-insert-old",
        "epoch-after-epoch-insert-old",
        "epoch-before-commit-old",
        "epoch-after-commit-new",
    ),
)
def test_consumer_registry_native_crash_reopens_and_replays_exactly(
    tmp_path: Path,
    operation: str,
    checkpoint: str,
    committed: bool,
    owner_epoch_at_crash: int,
) -> None:
    path = (tmp_path / checkpoint.replace(".", "-") / "registry.sqlite3").resolve()
    helper = (
        Path(__file__).resolve().parent
        / "helpers"
        / "worker_acquisition_consumer_registry_crash_child.py"
    )
    environment = dict(os.environ)
    root = str(Path(__file__).resolve().parents[1])
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            str(Path(root) / ".testdeps"),
            root,
            environment.get("PYTHONPATH", ""),
        )
        if value
    )
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    crashed = subprocess.run(
        [sys.executable, str(helper), str(path), operation, checkpoint],
        cwd=root,
        env=environment,
        capture_output=True,
        check=False,
        timeout=30.0,
    )
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""

    at_crash = _consumer_registry_raw_snapshot(path)
    assert at_crash["integrity"] == "ok"
    if checkpoint == "initialize.before_commit":
        assert at_crash["tables"] == frozenset()
        assert at_crash["owner_epoch"] is None
    else:
        assert at_crash["tables"] == _CONSUMER_REGISTRY_TABLES
        assert at_crash["owner_epoch"] == owner_epoch_at_crash
        assert (int(at_crash["owner_process_id"]) > 0) is (
            owner_epoch_at_crash > 0
        )
    assert at_crash["root_rows"] == int(committed)
    assert at_crash["epoch_rows"] == int(committed)

    catalog, reproof = _archive_authorities()
    with AcquisitionConsumerRegistryAuthority(
        path,
        clock=lambda: NOW + 10.0,
        token_factory=lambda: "2" * 64,
    ) as reopened:
        assert reopened._owner_epoch == owner_epoch_at_crash + 1
        before = reopened.load_current()
        assert (before is not None) is committed
        recovered = reopened.ensure_epoch(
            source_catalog=catalog,
            source_reproof_authority=reproof,
        )
        current = reopened.load_current()
        assert current is not None and current[1] == recovered
        assert recovered.epoch == 1
        assert recovered.previous_epoch_root_sha256 is None
        assert recovered.created_at == (NOW if committed else NOW + 10.0)
        identity = reopened.identity_sha256
        diagnostics = reopened.diagnostics()
        assert diagnostics["healthy"] is True
        assert diagnostics["current_epoch"] == 1
        assert diagnostics["registry_roots"] == 1
        assert diagnostics["member_count"] == subject.EXPECTED_MEMBER_COUNT

    with AcquisitionConsumerRegistryAuthority(
        path,
        clock=lambda: NOW + 20.0,
        token_factory=lambda: "3" * 64,
    ) as replayed:
        assert replayed._owner_epoch == owner_epoch_at_crash + 2
        assert replayed.identity_sha256 == identity
        current = replayed.load_current()
        assert current is not None and current[1] == recovered
        replay = replayed.ensure_epoch(
            source_catalog=catalog,
            source_reproof_authority=reproof,
        )
        assert replay == recovered
        assert replay.receipt_sha256 == recovered.receipt_sha256
        assert replay.epoch_root_sha256 == recovered.epoch_root_sha256
        assert replayed.diagnostics()["registry_roots"] == 1

    final = _consumer_registry_raw_snapshot(path)
    assert final["integrity"] == "ok"
    assert final["tables"] == _CONSUMER_REGISTRY_TABLES
    assert final["root_rows"] == final["epoch_rows"] == 1
    assert final["owner_epoch"] == owner_epoch_at_crash + 2


def test_process_owner_fence_is_cross_process(tmp_path):
    path = (tmp_path / "registry.sqlite3").resolve()
    authority = AcquisitionConsumerRegistryAuthority(path)
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    process = context.Process(target=_process_open_attempt, args=(str(path), queue))
    process.start()
    process.join(timeout=20.0)
    assert process.exitcode == 0
    assert queue.get(timeout=2.0) == "fenced"
    authority.close()


def test_module_is_unwired_from_runtime_and_only_read_bound_to_reclamation():
    repository = Path(__file__).resolve().parents[1]
    entrypoints = (
        repository / "storm_tracker_worker" / "service.py",
        repository / "storm_tracker_worker" / "worker_first_composition.py",
        repository / "storm_tracker_worker" / "worker_first_service_composition.py",
        repository / "storm_tracker_worker" / "__init__.py",
    )
    assert all(
        "acquisition_consumer_registry" not in path.read_text(encoding="utf-8")
        for path in entrypoints
        if path.exists()
    )
    reclamation = (
        repository
        / "storm_tracker_worker"
        / "acquisition_source_reclamation_authority.py"
    ).read_text(encoding="utf-8")
    assert "acquisition_consumer_registry" in reclamation
    for forbidden in (".unlink(", "os.remove(", "os.replace(", "vacuum"):
        assert forbidden not in reclamation.casefold()
    source = Path(subject.__file__).read_text(encoding="utf-8")
    for forbidden in (
        "delete_source",
        "apply_retention",
        "publish_operational_snapshot",
        "import paho",
    ):
        assert forbidden not in source
