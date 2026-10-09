from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from dataclasses import replace
from pathlib import Path
import hashlib
import json
import multiprocessing
import os
import sqlite3
import subprocess
import sys
import threading

import pytest

from storm_tracker_worker.completion_output_archive import (
    ARCHIVE_STORE_CONTRACT,
    SOURCE_IMMUTABILITY_MODES,
    CompletionOutputArchiveCapacityError,
    CompletionOutputArchiveClockError,
    CompletionOutputArchiveConflict,
    CompletionOutputArchiveIntegrityError,
    CompletionOutputArchiveLimits,
    CompletionOutputArchivePrivacyError,
    CompletionOutputArchiveRecord,
    CompletionOutputArchiveSourceCatalog,
    CompletionOutputArchiveSourceProfile,
    CompletionOutputArchiveSourceReproofAuthority,
    CompletionOutputArchiveSourceReproofBinding,
    CompletionOutputArchiveSourceRowReproof,
    CompletionOutputArchiveStore,
    CompletionOutputHotCandidate,
    CompletionOutputHotWindowPolicy,
    calculate_cadence_horizon,
    plan_hot_window,
)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _sha(value: str | bytes) -> str:
    body = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(body).hexdigest()


def _event(value: str) -> str:
    return "completion-" + _sha(value)


def _profile(
    suffix: str = "a",
    *,
    cadence_seconds: int = 300,
    immutability: str = "checksum_reproof_required",
) -> CompletionOutputArchiveSourceProfile:
    return CompletionOutputArchiveSourceProfile(
        workload_id=f"test.workload.{suffix}",
        consumer_id=f"test-consumer-{suffix}",
        stream_id=f"test.workload.{suffix}",
        cadence_seconds=cadence_seconds,
        source_store_contract=f"test-source-store-{suffix}-v1",
        source_store_schema_version=1,
        output_document_schema=f"test-output-{suffix}-v1",
        output_receipt_schema=f"test-output-receipt-{suffix}-v1",
        process_receipt_schema=f"test-process-receipt-{suffix}-v1",
        source_limits_sha256=_sha(f"source-limits-{suffix}"),
        effect_contract_sha256=_sha(f"effect-{suffix}"),
        acquisition_catalog_sha256=_sha("acquisition-catalog"),
        consumer_binding_sha256=_sha(f"consumer-binding-{suffix}"),
        source_immutability_mode=immutability,
        source_row_reproof_contract=f"test-source-row-reproof-{suffix}-v1",
    )


def _catalog(
    *profiles: CompletionOutputArchiveSourceProfile,
) -> CompletionOutputArchiveSourceCatalog:
    return CompletionOutputArchiveSourceCatalog(
        catalog_id="test-completion-output-sources-v1",
        profiles=tuple(profiles or (_profile(),)),
    )


def _limits(**updates: int) -> CompletionOutputArchiveLimits:
    values = {
        "max_records": 256,
        "max_record_bytes": 64 * 1024,
        "max_total_record_bytes": 4 * 1024 * 1024,
        "max_database_bytes": 8 * 1024 * 1024,
        "busy_timeout_ms": 5_000,
    }
    values.update(updates)
    return CompletionOutputArchiveLimits(**values)


def _policy(**updates: int) -> CompletionOutputHotWindowPolicy:
    values = {
        "hot_window_seconds": 86_400,
        "safety_records_per_stream": 2,
        "max_plan_records": 10_000,
    }
    values.update(updates)
    return CompletionOutputHotWindowPolicy(**values)


class _MutableClock:
    def __init__(self, value: float = 100_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class _MemorySourceResolver:
    def __init__(
        self,
        rows: dict[str, bytes],
        profile: CompletionOutputArchiveSourceProfile,
    ) -> None:
        self.rows = rows
        self.profile = profile

    @contextmanager
    def __call__(self, record: CompletionOutputArchiveRecord):
        body = self.rows[record.event_id]
        yield CompletionOutputArchiveSourceRowReproof(
            source_profile_sha256=self.profile.fingerprint,
            event_id=record.event_id,
            source_row_sha256=_sha(body),
            source_row_bytes=len(body),
            reproof_contract=self.profile.source_row_reproof_contract,
            verified_at=100_000.0,
        )


class _SqliteSourceResolver:
    def __init__(
        self,
        path: str | Path,
        profile: CompletionOutputArchiveSourceProfile,
    ) -> None:
        self.path = Path(path)
        self.profile = profile

    @contextmanager
    def __call__(self, record: CompletionOutputArchiveRecord):
        database = sqlite3.connect(self.path, isolation_level=None, timeout=5.0)
        try:
            database.execute("BEGIN")
            row = database.execute(
                "SELECT row_json FROM source_outputs WHERE event_id=?",
                (record.event_id,),
            ).fetchone()
            if row is None:
                raise CompletionOutputArchiveIntegrityError("test source row missing")
            body = bytes(row[0])
            yield CompletionOutputArchiveSourceRowReproof(
                source_profile_sha256=self.profile.fingerprint,
                event_id=record.event_id,
                source_row_sha256=_sha(body),
                source_row_bytes=len(body),
                reproof_contract=self.profile.source_row_reproof_contract,
                verified_at=100_000.0,
            )
        finally:
            if database.in_transaction:
                database.rollback()
            database.close()


def _authority(
    catalog: CompletionOutputArchiveSourceCatalog,
    resolvers: dict[str, object],
) -> CompletionOutputArchiveSourceReproofAuthority:
    bindings = []
    for profile in catalog.profiles:
        resolver = resolvers[profile.fingerprint]
        bindings.append(
            CompletionOutputArchiveSourceReproofBinding(
                source_profile_sha256=profile.fingerprint,
                resolver_id=f"test-resolver-{profile.consumer_id}",
                resolver_sha256=_sha(f"resolver-code-{profile.consumer_id}"),
                source_store_identity_sha256=_sha(
                    f"source-store-identity-{profile.consumer_id}"
                ),
                reproof_contract=profile.source_row_reproof_contract,
                resolver=resolver,  # type: ignore[arg-type]
            )
        )
    return CompletionOutputArchiveSourceReproofAuthority(
        authority_id="test-source-reproof-authority-v1",
        bindings=tuple(bindings),
    )


def _record(
    profile: CompletionOutputArchiveSourceProfile,
    rows: dict[str, bytes],
    sequence: int,
    *,
    event_seed: str | None = None,
    created_at: float | None = None,
    filler: int = 0,
) -> CompletionOutputArchiveRecord:
    event_id = _event(event_seed or f"event-{profile.consumer_id}-{sequence}")
    output = {
        "claims": {"meteorological_admission": False},
        "consumer_id": profile.consumer_id,
        "event": {"event_id": event_id, "stream_sequence": sequence},
        "filler": "x" * filler,
        "mode": "shadow_inert",
        "schema": profile.output_document_schema,
        "stream_id": profile.stream_id,
        "workload_id": profile.workload_id,
    }
    timestamp = 1_000.0 + sequence if created_at is None else created_at
    source_row = _canonical(
        {
            "created_at": timestamp,
            "event_id": event_id,
            "output": output,
            "source_revision": 1,
        }
    )
    rows[event_id] = source_row
    return CompletionOutputArchiveRecord.from_output(
        source_profile_sha256=profile.fingerprint,
        event_id=event_id,
        event_sha256=_sha(f"event-sha-{event_id}"),
        producer_receipt_sha256=_sha(f"producer-{event_id}"),
        workload_id=profile.workload_id,
        consumer_id=profile.consumer_id,
        stream_id=profile.stream_id,
        stream_sequence=sequence,
        created_at=timestamp,
        output_receipt_sha256=_sha(f"output-receipt-{event_id}-{filler}"),
        process_receipt_sha256=_sha(f"process-receipt-{event_id}-{filler}"),
        source_row_sha256=_sha(source_row),
        source_row_bytes=len(source_row),
        output=output,
    )


def _environment(
    tmp_path: Path,
    *,
    profiles: tuple[CompletionOutputArchiveSourceProfile, ...] | None = None,
    limits: CompletionOutputArchiveLimits | None = None,
    policy: CompletionOutputHotWindowPolicy | None = None,
    clock: _MutableClock | None = None,
    fault_injector=None,
    database_name: str = "archive.sqlite",
):
    selected = profiles or (_profile(),)
    catalog = _catalog(*selected)
    rows: dict[str, bytes] = {}
    resolvers = {
        profile.fingerprint: _MemorySourceResolver(rows, profile)
        for profile in selected
    }
    authority = _authority(catalog, resolvers)
    store = CompletionOutputArchiveStore(
        (tmp_path / database_name).resolve(),
        source_catalog=catalog,
        source_reproof_authority=authority,
        expected_source_reproof_authority_sha256=authority.fingerprint,
        limits=limits or _limits(),
        hot_policy=policy or _policy(),
        clock=clock or _MutableClock(),
        fault_injector=fault_injector,
    )
    return store, rows, catalog, authority


def _sqlite_environment(tmp_path: Path):
    profile = _profile(immutability="checksum_reproof_required")
    catalog = _catalog(profile)
    source_path = (tmp_path / "unguarded-source.sqlite").resolve()
    with closing(sqlite3.connect(source_path)) as database:
        database.execute(
            "CREATE TABLE source_outputs(event_id TEXT PRIMARY KEY,row_json BLOB NOT NULL)"
        )
        database.commit()
    resolver = _SqliteSourceResolver(source_path, profile)
    authority = _authority(catalog, {profile.fingerprint: resolver})
    store = CompletionOutputArchiveStore(
        (tmp_path / "archive.sqlite").resolve(),
        source_catalog=catalog,
        source_reproof_authority=authority,
        expected_source_reproof_authority_sha256=authority.fingerprint,
        limits=_limits(),
        hot_policy=_policy(),
        disjoint_paths=(source_path,),
        clock=_MutableClock(),
    )
    return store, profile, source_path, catalog, authority


def _process_store(
    archive_path: str,
    source_path: str,
    *,
    fault_point: str | None = None,
) -> tuple[CompletionOutputArchiveStore, CompletionOutputArchiveSourceProfile]:
    profile = _profile(immutability="checksum_reproof_required")
    catalog = _catalog(profile)
    resolver = _SqliteSourceResolver(source_path, profile)
    authority = _authority(catalog, {profile.fingerprint: resolver})

    def fault(point: str) -> None:
        if point == fault_point:
            os._exit(91)

    return (
        CompletionOutputArchiveStore(
            Path(archive_path),
            source_catalog=catalog,
            source_reproof_authority=authority,
            expected_source_reproof_authority_sha256=authority.fingerprint,
            limits=_limits(),
            hot_policy=_policy(),
            disjoint_paths=(Path(source_path),),
            clock=_MutableClock(),
            fault_injector=None if fault_point is None else fault,
        ),
        profile,
    )


def _process_seal_worker(
    archive_path: str,
    source_path: str,
    record_mapping: dict[str, object],
    gate,
    queue,
    fault_point: str | None = None,
) -> None:
    store, _ = _process_store(
        archive_path, source_path, fault_point=fault_point
    )
    record = CompletionOutputArchiveRecord.from_mapping(record_mapping)
    gate.wait(10.0)
    seal = store.seal(record)
    queue.put(seal.seal_sha256)


COMPLETION_CORE_CRASH_CHILD = (
    Path(__file__).resolve().parent
    / "helpers"
    / "completion_core_os_exit73_child.py"
)


def _native_insert_crash_environment(
    root: Path,
    *,
    fault_injector=None,
) -> tuple[
    CompletionOutputArchiveStore,
    CompletionOutputArchiveSourceProfile,
    CompletionOutputArchiveRecord,
    Path,
]:
    """Reopen the exact SQLite-backed source/archive pair used by the child."""

    profile = _profile(immutability="checksum_reproof_required")
    catalog = _catalog(profile)
    source_path = (root / "native-insert-source.sqlite").resolve()
    if not source_path.exists():
        with closing(sqlite3.connect(source_path)) as database:
            database.execute(
                "CREATE TABLE source_outputs("
                "event_id TEXT PRIMARY KEY,row_json BLOB NOT NULL)"
            )
            database.commit()
    rows: dict[str, bytes] = {}
    record = _record(profile, rows, 41, event_seed="native-before-insert")
    with closing(sqlite3.connect(source_path)) as database:
        database.execute(
            "INSERT OR IGNORE INTO source_outputs VALUES(?,?)",
            (record.event_id, rows[record.event_id]),
        )
        stored = database.execute(
            "SELECT row_json FROM source_outputs WHERE event_id=?",
            (record.event_id,),
        ).fetchone()
        database.commit()
    assert stored is not None and bytes(stored[0]) == rows[record.event_id]
    resolver = _SqliteSourceResolver(source_path, profile)
    authority = _authority(catalog, {profile.fingerprint: resolver})
    store = CompletionOutputArchiveStore(
        (root / "native-insert-archive.sqlite").resolve(),
        source_catalog=catalog,
        source_reproof_authority=authority,
        expected_source_reproof_authority_sha256=authority.fingerprint,
        limits=_limits(),
        hot_policy=_policy(),
        disjoint_paths=(source_path,),
        clock=_MutableClock(),
        fault_injector=fault_injector,
    )
    return store, profile, record, source_path


def run_completion_archive_insert_crash_child(
    root: Path,
    checkpoint: str,
    crash,
) -> None:
    assert checkpoint == "archive.before_insert"
    store, _profile_value, record, _source_path = (
        _native_insert_crash_environment(root, fault_injector=crash)
    )
    store.seal(record)


def _run_completion_core_crash(
    scenario: str,
    root: Path,
    checkpoint: str,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [
            sys.executable,
            str(COMPLETION_CORE_CRASH_CHILD),
            scenario,
            str(root),
            checkpoint,
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        check=False,
        timeout=120.0,
    )


def test_source_profiles_bind_dwd_like_immutability_difference() -> None:
    unguarded = _profile(immutability="checksum_reproof_required")
    guarded = replace(
        unguarded, source_immutability_mode="append_only_sqlite_triggers"
    )
    assert SOURCE_IMMUTABILITY_MODES == {
        "append_only_sqlite_triggers",
        "checksum_reproof_required",
    }
    assert unguarded.fingerprint != guarded.fingerprint
    assert _catalog(unguarded).fingerprint != _catalog(guarded).fingerprint


def test_seal_load_and_exact_verify_for_ack(tmp_path: Path) -> None:
    store, rows, catalog, authority = _environment(tmp_path)
    profile = catalog.profiles[0]
    record = _record(profile, rows, 7)
    seal = store.seal(record)

    assert seal.archive_id == record.archive_id
    assert seal.record_sha256 == record.record_sha256
    assert seal.archive_authority_sha256 == store.authority_sha256
    loaded = store.load(profile.fingerprint, record.event_id)
    assert loaded is not None
    assert loaded.record.canonical_bytes == record.canonical_bytes
    assert loaded.seal == seal
    verified = store.verify_for_ack(
        profile.fingerprint,
        record.event_id,
        output_sha256=record.output_sha256,
        output_receipt_sha256=record.output_receipt_sha256,
        process_receipt_sha256=record.process_receipt_sha256,
        record_sha256=record.record_sha256,
    )
    assert verified == loaded
    diagnostics = store.diagnostics()
    assert diagnostics["contract"] == ARCHIVE_STORE_CONTRACT
    assert diagnostics["records"] == 1
    assert diagnostics["runtime_wiring"] == "inert"
    assert diagnostics["source_catalog_sha256"] == catalog.fingerprint
    assert diagnostics["source_reproof_authority_sha256"] == authority.fingerprint


def test_verify_for_ack_rejects_each_hash_mismatch(tmp_path: Path) -> None:
    store, rows, catalog, _ = _environment(tmp_path)
    profile = catalog.profiles[0]
    record = _record(profile, rows, 1)
    store.seal(record)
    baseline = {
        "output_sha256": record.output_sha256,
        "output_receipt_sha256": record.output_receipt_sha256,
        "process_receipt_sha256": record.process_receipt_sha256,
        "record_sha256": record.record_sha256,
    }
    for key in tuple(baseline):
        proof = dict(baseline)
        proof[key] = "0" * 64
        with pytest.raises(CompletionOutputArchiveConflict):
            store.verify_for_ack(
                profile.fingerprint, record.event_id, **proof
            )


def test_idempotent_seal_returns_first_timestamp_without_second_reproof(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(100.0)
    store, rows, catalog, _ = _environment(tmp_path, clock=clock)
    profile = catalog.profiles[0]
    record = _record(profile, rows, 1)
    first = store.seal(record)
    del rows[record.event_id]
    clock.value = 1.0
    second = store.seal(record)
    assert second == first
    assert second.archived_at == 100.0


def test_same_source_event_with_different_record_conflicts(tmp_path: Path) -> None:
    store, rows, catalog, _ = _environment(tmp_path)
    profile = catalog.profiles[0]
    first = _record(profile, rows, 1, event_seed="same")
    store.seal(first)
    different = _record(profile, rows, 1, event_seed="same", filler=3)
    with pytest.raises(CompletionOutputArchiveConflict):
        store.seal(different)
    assert store.load(profile.fingerprint, first.event_id).record == first  # type: ignore[union-attr]


def test_route_and_unknown_profile_fail_closed(tmp_path: Path) -> None:
    store, rows, catalog, _ = _environment(tmp_path)
    profile = catalog.profiles[0]
    record = _record(profile, rows, 1)
    wrong_route = replace(record, consumer_id="another-consumer")
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        store.seal(wrong_route)
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        store.load("0" * 64, record.event_id)
    assert store.diagnostics()["records"] == 0


@pytest.mark.parametrize("drift", ["catalog", "limits", "policy", "authority"])
def test_reopen_rejects_catalog_limits_policy_and_authority_drift(
    tmp_path: Path, drift: str
) -> None:
    store, _, catalog, authority = _environment(tmp_path)
    path = store.path
    selected_catalog = catalog
    selected_limits = store.limits
    selected_policy = store.hot_policy
    selected_authority = authority
    if drift == "catalog":
        changed = replace(
            catalog.profiles[0],
            source_immutability_mode="append_only_sqlite_triggers",
        )
        selected_catalog = _catalog(changed)
        selected_authority = _authority(
            selected_catalog,
            {changed.fingerprint: _MemorySourceResolver({}, changed)},
        )
    elif drift == "limits":
        selected_limits = _limits(max_records=257)
    elif drift == "policy":
        selected_policy = _policy(safety_records_per_stream=3)
    else:
        binding = authority.bindings[0]
        forged = replace(binding, resolver_sha256=_sha("different-resolver"))
        selected_authority = CompletionOutputArchiveSourceReproofAuthority(
            authority_id=authority.authority_id,
            bindings=(forged,),
        )
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        CompletionOutputArchiveStore(
            path,
            source_catalog=selected_catalog,
            source_reproof_authority=selected_authority,
            expected_source_reproof_authority_sha256=(
                selected_authority.fingerprint
            ),
            limits=selected_limits,
            hot_policy=selected_policy,
        )


def test_outer_source_reproof_authority_sha_is_mandatory(tmp_path: Path) -> None:
    profile = _profile()
    catalog = _catalog(profile)
    authority = _authority(
        catalog, {profile.fingerprint: _MemorySourceResolver({}, profile)}
    )
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        CompletionOutputArchiveStore(
            (tmp_path / "archive.sqlite").resolve(),
            source_catalog=catalog,
            source_reproof_authority=authority,
            expected_source_reproof_authority_sha256="0" * 64,
            limits=_limits(),
            hot_policy=_policy(),
        )


def test_per_call_forged_source_proof_is_not_an_api(tmp_path: Path) -> None:
    store, rows, catalog, _ = _environment(tmp_path)
    record = _record(catalog.profiles[0], rows, 1)
    forged = CompletionOutputArchiveSourceRowReproof(
        source_profile_sha256=record.source_profile_sha256,
        event_id=record.event_id,
        source_row_sha256=record.source_row_sha256,
        source_row_bytes=record.source_row_bytes,
        reproof_contract=catalog.profiles[0].source_row_reproof_contract,
        verified_at=1.0,
    )
    with pytest.raises(TypeError):
        store.seal(record, source_reproof=forged)  # type: ignore[call-arg]
    assert store.diagnostics()["records"] == 0


def test_direct_sql_source_mutation_between_plan_and_seal_has_no_archive_head(
    tmp_path: Path,
) -> None:
    store, profile, source_path, _, _ = _sqlite_environment(tmp_path)
    rows: dict[str, bytes] = {}
    record = _record(profile, rows, 1, created_at=1.0)
    with closing(sqlite3.connect(source_path)) as database:
        database.execute(
            "INSERT INTO source_outputs VALUES(?,?)",
            (record.event_id, rows[record.event_id]),
        )
        database.commit()
    plan = plan_hot_window(
        (CompletionOutputHotCandidate.from_record(record),),
        catalog=store.source_catalog,
        policy=store.hot_policy,
        evaluated_at=100_000.0,
    )
    assert plan.retained
    with closing(sqlite3.connect(source_path)) as database:
        database.execute(
            "UPDATE source_outputs SET row_json=? WHERE event_id=?",
            (_canonical({"mutated": True}), record.event_id),
        )
        database.commit()
    with pytest.raises(CompletionOutputArchiveConflict):
        store.seal(record)
    assert store.load(profile.fingerprint, record.event_id) is None
    assert store.diagnostics()["records"] == 0


def test_count_record_and_total_byte_caps(tmp_path: Path) -> None:
    profile = _profile()
    rows: dict[str, bytes] = {}
    first = _record(profile, rows, 1)
    second = _record(profile, rows, 2)
    record_size = max(len(first.canonical_bytes), len(second.canonical_bytes))

    count_store, count_rows, count_catalog, _ = _environment(
        tmp_path,
        limits=_limits(max_records=1),
        database_name="count.sqlite",
    )
    count_profile = count_catalog.profiles[0]
    count_store.seal(_record(count_profile, count_rows, 1))
    with pytest.raises(CompletionOutputArchiveCapacityError):
        count_store.seal(_record(count_profile, count_rows, 2))

    total_limits = _limits(
        max_record_bytes=max(1_024, record_size + 16),
        max_total_record_bytes=max(1_024, record_size + 16),
    )
    total_store, total_rows, total_catalog, _ = _environment(
        tmp_path,
        limits=total_limits,
        database_name="total.sqlite",
    )
    total_profile = total_catalog.profiles[0]
    total_store.seal(_record(total_profile, total_rows, 1))
    with pytest.raises(CompletionOutputArchiveCapacityError):
        total_store.seal(_record(total_profile, total_rows, 2))

    record_store, record_rows, record_catalog, _ = _environment(
        tmp_path,
        limits=_limits(max_record_bytes=1_024),
        database_name="record.sqlite",
    )
    with pytest.raises(CompletionOutputArchiveCapacityError):
        record_store.seal(
            _record(record_catalog.profiles[0], record_rows, 1, filler=2_000)
        )


def test_sqlite_page_cap_is_enforced_before_logical_caps(tmp_path: Path) -> None:
    store, rows, catalog, _ = _environment(
        tmp_path,
        limits=_limits(
            max_records=100,
            max_record_bytes=48 * 1024,
            max_total_record_bytes=2 * 1024 * 1024,
            max_database_bytes=64 * 1024,
        ),
    )
    profile = catalog.profiles[0]
    reached = False
    for sequence in range(20):
        try:
            store.seal(_record(profile, rows, sequence, filler=20_000))
        except CompletionOutputArchiveCapacityError:
            reached = True
            break
    assert reached
    assert store.diagnostics()["records"] < 100


def test_archive_tables_and_metadata_are_physically_append_only(tmp_path: Path) -> None:
    store, rows, catalog, _ = _environment(tmp_path)
    record = _record(catalog.profiles[0], rows, 1)
    store.seal(record)
    with closing(store._connect()) as database:
        assert str(database.execute("PRAGMA journal_mode").fetchone()[0]).lower() == "delete"
        assert int(database.execute("PRAGMA synchronous").fetchone()[0]) == 2
        page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
        assert int(database.execute("PRAGMA max_page_count").fetchone()[0]) == (
            store.limits.max_database_bytes // page_size
        )
        objects = {
            (row[0], row[1])
            for row in database.execute(
                "SELECT type,name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        assert objects == {
            ("table", "archive_metadata"),
            ("table", "archive_records"),
            ("index", "archive_records_source_event"),
            ("index", "archive_records_record_sha256"),
            ("trigger", "archive_metadata_no_update"),
            ("trigger", "archive_metadata_no_delete"),
            ("trigger", "archive_records_no_update"),
            ("trigger", "archive_records_no_delete"),
        }
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "UPDATE archive_records SET archived_at=archived_at+1"
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute("DELETE FROM archive_records")
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "UPDATE archive_metadata SET schema_version=schema_version+1"
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute("DELETE FROM archive_metadata")


@pytest.mark.parametrize("tamper", ["extra_table", "missing_trigger", "record", "seal"])
def test_schema_trigger_and_row_tamper_fail_closed(
    tmp_path: Path, tamper: str
) -> None:
    store, rows, catalog, _ = _environment(tmp_path)
    profile = catalog.profiles[0]
    record = _record(profile, rows, 1)
    store.seal(record)
    with closing(sqlite3.connect(store.path)) as database:
        if tamper == "extra_table":
            database.execute("CREATE TABLE forged(value TEXT)")
        elif tamper == "missing_trigger":
            database.execute("DROP TRIGGER archive_records_no_delete")
        else:
            trigger_sql = database.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE name='archive_records_no_update'"
            ).fetchone()[0]
            database.execute("DROP TRIGGER archive_records_no_update")
            if tamper == "record":
                database.execute(
                    "UPDATE archive_records SET record_json=?,record_bytes=?",
                    (b"{}", 2),
                )
            else:
                database.execute(
                    "UPDATE archive_records SET seal_sha256=?", ("0" * 64,)
                )
            database.execute(trigger_sql)
        database.commit()
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        store.load(profile.fingerprint, record.event_id)


def test_fault_before_commit_rolls_back_and_after_commit_reconciles(
    tmp_path: Path,
) -> None:
    before_calls: list[str] = []

    def before_fault(point: str) -> None:
        before_calls.append(point)
        if point == "archive.before_commit":
            raise RuntimeError("power cut")

    before, rows, catalog, _ = _environment(
        tmp_path, fault_injector=before_fault, database_name="before.sqlite"
    )
    profile = catalog.profiles[0]
    record = _record(profile, rows, 1)
    with pytest.raises(RuntimeError, match="power cut"):
        before.seal(record)
    assert before.load(profile.fingerprint, record.event_id) is None
    assert "archive.before_commit" in before_calls

    after_calls: list[str] = []

    def after_fault(point: str) -> None:
        after_calls.append(point)
        if point == "archive.after_commit":
            raise RuntimeError("lost child result")

    after, after_rows, after_catalog, _ = _environment(
        tmp_path, fault_injector=after_fault, database_name="after.sqlite"
    )
    after_profile = after_catalog.profiles[0]
    after_record = _record(after_profile, after_rows, 1)
    with pytest.raises(RuntimeError, match="lost child result"):
        after.seal(after_record)
    assert after.load(after_profile.fingerprint, after_record.event_id) is not None


def test_thread_cas_returns_one_exact_seal(tmp_path: Path) -> None:
    store, rows, catalog, authority = _environment(tmp_path)
    profile = catalog.profiles[0]
    record = _record(profile, rows, 1)
    barrier = threading.Barrier(8)

    def commit() -> str:
        local = CompletionOutputArchiveStore(
            store.path,
            source_catalog=catalog,
            source_reproof_authority=authority,
            expected_source_reproof_authority_sha256=authority.fingerprint,
            limits=store.limits,
            hot_policy=store.hot_policy,
            clock=_MutableClock(),
        )
        barrier.wait(10.0)
        return local.seal(record).seal_sha256

    with ThreadPoolExecutor(max_workers=8) as executor:
        seals = list(executor.map(lambda _: commit(), range(8)))
    assert len(set(seals)) == 1
    assert store.diagnostics()["records"] == 1


def test_two_process_cas_and_real_crash_boundaries(tmp_path: Path) -> None:
    store, profile, source_path, _, _ = _sqlite_environment(tmp_path)
    rows: dict[str, bytes] = {}
    record = _record(profile, rows, 1)
    with closing(sqlite3.connect(source_path)) as database:
        database.execute(
            "INSERT INTO source_outputs VALUES(?,?)",
            (record.event_id, rows[record.event_id]),
        )
        database.commit()
    context = multiprocessing.get_context("spawn")
    gate = context.Event()
    queue = context.Queue()
    processes = [
        context.Process(
            target=_process_seal_worker,
            args=(
                str(store.path),
                str(source_path),
                record.as_dict(),
                gate,
                queue,
            ),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    gate.set()
    for process in processes:
        process.join(20.0)
        assert process.exitcode == 0
    seals = [queue.get(timeout=5.0) for _ in processes]
    assert len(set(seals)) == 1
    assert store.diagnostics()["records"] == 1

    crash_rows: dict[str, bytes] = {}
    before_record = _record(profile, crash_rows, 2)
    after_record = _record(profile, crash_rows, 3)
    with closing(sqlite3.connect(source_path)) as database:
        database.executemany(
            "INSERT INTO source_outputs VALUES(?,?)",
            [
                (before_record.event_id, crash_rows[before_record.event_id]),
                (after_record.event_id, crash_rows[after_record.event_id]),
            ],
        )
        database.commit()
    for fault_point, target_record, expected_present in (
        ("archive.before_commit", before_record, False),
        ("archive.after_commit", after_record, True),
    ):
        crash_gate = context.Event()
        crash_gate.set()
        crash_queue = context.Queue()
        process = context.Process(
            target=_process_seal_worker,
            args=(
                str(store.path),
                str(source_path),
                target_record.as_dict(),
                crash_gate,
                crash_queue,
                fault_point,
            ),
        )
        process.start()
        process.join(20.0)
        assert process.exitcode == 91
        assert (
            store.load(profile.fingerprint, target_record.event_id) is not None
        ) is expected_present


def test_native_os_exit73_before_insert_recovers_replays_and_audits(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "archive-before-insert").resolve()
    root.mkdir()

    crashed = _run_completion_core_crash(
        "archive", root, "archive.before_insert"
    )
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""

    reopened, profile, record, source_path = _native_insert_crash_environment(root)
    assert reopened.load(profile.fingerprint, record.event_id) is None
    before = reopened.diagnostics()
    assert before["healthy"] is True
    assert before["records"] == 0
    assert before["logical_record_bytes"] == 0
    with closing(sqlite3.connect(reopened.path)) as database:
        assert database.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert database.execute(
            "SELECT count(*) FROM archive_records"
        ).fetchone() == (0,)
    with closing(sqlite3.connect(source_path)) as database:
        source = database.execute(
            "SELECT row_json FROM source_outputs WHERE event_id=?",
            (record.event_id,),
        ).fetchone()
        assert database.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    assert source is not None
    assert hashlib.sha256(bytes(source[0])).hexdigest() == record.source_row_sha256

    seal = reopened.seal(record)
    committed = reopened.verify_for_ack(
        profile.fingerprint,
        record.event_id,
        output_sha256=record.output_sha256,
        output_receipt_sha256=record.output_receipt_sha256,
        process_receipt_sha256=record.process_receipt_sha256,
        record_sha256=record.record_sha256,
    )
    assert committed.record == record
    assert committed.seal == seal
    after_commit = reopened.diagnostics()
    assert after_commit["healthy"] is True
    assert after_commit["records"] == 1
    assert after_commit["logical_record_bytes"] == len(record.canonical_bytes)

    replay = CompletionOutputArchiveStore(
        reopened.path,
        source_catalog=reopened.source_catalog,
        source_reproof_authority=reopened.source_reproof_authority,
        expected_source_reproof_authority_sha256=(
            reopened.source_reproof_authority.fingerprint
        ),
        limits=reopened.limits,
        hot_policy=reopened.hot_policy,
        disjoint_paths=(source_path,),
        clock=_MutableClock(1.0),
    )
    assert replay.seal(record) == seal
    replayed = replay.load(profile.fingerprint, record.event_id)
    assert replayed == committed
    after_replay = replay.diagnostics()
    assert after_replay["records"] == 1
    assert after_replay["logical_record_bytes"] == len(record.canonical_bytes)
    with closing(sqlite3.connect(replay.path)) as database:
        assert database.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert database.execute(
            "SELECT count(*) FROM archive_records"
        ).fetchone() == (1,)


def test_clock_rollback_rejects_new_head_but_not_exact_replay(tmp_path: Path) -> None:
    clock = _MutableClock(100.0)
    store, rows, catalog, _ = _environment(tmp_path, clock=clock)
    profile = catalog.profiles[0]
    first = _record(profile, rows, 1)
    first_seal = store.seal(first)
    clock.value = 99.0
    second = _record(profile, rows, 2)
    with pytest.raises(CompletionOutputArchiveClockError):
        store.seal(second)
    clock.value = 1.0
    assert store.seal(first) == first_seal
    assert store.diagnostics()["records"] == 1


@pytest.mark.parametrize(
    "output",
    [
        {"authorization": "redacted"},
        {"artifact_path": r"C:\\private\\artifact"},
        {"note": "https://private.invalid/object"},
        {"raw_payload": "bytes"},
        {"note": "/private/local/file"},
    ],
)
def test_record_privacy_rejects_secrets_paths_urls_and_raw_material(
    output: dict[str, str],
) -> None:
    profile = _profile()
    source_row = _canonical({"safe": True})
    with pytest.raises(CompletionOutputArchivePrivacyError):
        CompletionOutputArchiveRecord.from_output(
            source_profile_sha256=profile.fingerprint,
            event_id=_event("private"),
            event_sha256=_sha("event"),
            producer_receipt_sha256=_sha("producer"),
            workload_id=profile.workload_id,
            consumer_id=profile.consumer_id,
            stream_id=profile.stream_id,
            stream_sequence=1,
            created_at=1.0,
            output_receipt_sha256=_sha("output receipt"),
            process_receipt_sha256=_sha("process receipt"),
            source_row_sha256=_sha(source_row),
            source_row_bytes=len(source_row),
            output=output,
        )


def _privacy_record(output: dict[str, object]) -> CompletionOutputArchiveRecord:
    profile = _profile()
    source_row = _canonical({"safe": True})
    return CompletionOutputArchiveRecord.from_output(
        source_profile_sha256=profile.fingerprint,
        event_id=_event("raw verification assertion"),
        event_sha256=_sha("raw verification event"),
        producer_receipt_sha256=_sha("raw verification producer"),
        workload_id=profile.workload_id,
        consumer_id=profile.consumer_id,
        stream_id=profile.stream_id,
        stream_sequence=1,
        created_at=1.0,
        output_receipt_sha256=_sha("raw verification output receipt"),
        process_receipt_sha256=_sha("raw verification process receipt"),
        source_row_sha256=_sha(source_row),
        source_row_bytes=len(source_row),
        output=output,
    )


def test_record_privacy_allows_only_exact_true_raw_bytes_verification_assertion() -> None:
    record = _privacy_record(
        {
            "audit": {"raw_bytes_verified": True},
            "claims": {"decoded": False, "raw_bytes_copied": False},
        }
    )
    assert record.output["audit"]["raw_bytes_verified"] is True
    assert record.output["claims"]["raw_bytes_copied"] is False


@pytest.mark.parametrize(
    "output",
    [
        {"raw_bytes_verified": False},
        {"raw_bytes_verified": "true"},
        {"raw_bytes_verified": {"value": True}},
        {"audit": {"raw_bytes_verified": {"raw_bytes": True}}},
        {"raw_bytes_copied": True},
        {"raw_bytes_copied": "false"},
        {"raw_bytes_copied": {"value": False}},
        {"raw-bytes-verified": True},
        {"raw-bytes-copied": False},
        {"raw_bytes": True},
        {"raw_payload_verified": True},
    ],
)
def test_record_privacy_rejects_false_string_nested_and_alias_raw_assertions(
    output: dict[str, object],
) -> None:
    with pytest.raises(CompletionOutputArchivePrivacyError):
        _privacy_record(output)


def test_paths_are_absolute_non_symlink_and_disjoint(tmp_path: Path) -> None:
    profile = _profile()
    catalog = _catalog(profile)
    authority = _authority(
        catalog, {profile.fingerprint: _MemorySourceResolver({}, profile)}
    )
    kwargs = {
        "source_catalog": catalog,
        "source_reproof_authority": authority,
        "expected_source_reproof_authority_sha256": authority.fingerprint,
        "limits": _limits(),
        "hot_policy": _policy(),
    }
    with pytest.raises(ValueError):
        CompletionOutputArchiveStore(Path("relative.sqlite"), **kwargs)
    archive_path = (tmp_path / "archive.sqlite").resolve()
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        CompletionOutputArchiveStore(
            archive_path, disjoint_paths=(archive_path,), **kwargs
        )
    directory = (tmp_path / "directory.sqlite").resolve()
    directory.mkdir()
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        CompletionOutputArchiveStore(directory, **kwargs)

    target = (tmp_path / "target").resolve()
    target.mkdir()
    link = tmp_path / "linked"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        aliased = target / ".." / target.name / "archive.sqlite"
        with pytest.raises(CompletionOutputArchiveIntegrityError):
            CompletionOutputArchiveStore(aliased, **kwargs)
    else:
        with pytest.raises(CompletionOutputArchiveIntegrityError):
            CompletionOutputArchiveStore(link / "archive.sqlite", **kwargs)


def test_diagnostics_and_seals_do_not_disclose_paths(tmp_path: Path) -> None:
    store, rows, catalog, _ = _environment(tmp_path)
    record = _record(catalog.profiles[0], rows, 1)
    seal = store.seal(record)
    public = json.dumps({"seal": seal.as_dict(), "diagnostics": store.diagnostics()})
    assert str(tmp_path) not in public
    assert "archive.sqlite" not in public


def test_hot_window_plans_cadence_count_age_and_clock_rollback() -> None:
    profile = _profile(cadence_seconds=300)
    catalog = _catalog(profile)
    policy = _policy()
    candidates = tuple(
        CompletionOutputHotCandidate(
            source_profile_sha256=profile.fingerprint,
            event_id=_event(f"hot-{sequence}"),
            stream_sequence=sequence,
            created_at=float(sequence * 300),
            source_row_sha256=_sha(f"source-{sequence}"),
            source_row_bytes=100,
        )
        for sequence in range(292)
    )
    plan = plan_hot_window(
        candidates,
        catalog=catalog,
        policy=policy,
        evaluated_at=float(291 * 300),
    )
    assert plan.minimum_hot_records[profile.fingerprint] == 290
    assert len(plan.retained) == 290
    assert [item.stream_sequence for item in plan.eligible_for_archive] == [0, 1]
    assert not plan.clock_rollback_detected

    future = CompletionOutputHotCandidate(
        source_profile_sha256=profile.fingerprint,
        event_id=_event("future"),
        stream_sequence=292,
        created_at=float(291 * 300 + 1),
        source_row_sha256=_sha("future-source"),
        source_row_bytes=100,
    )
    rollback = plan_hot_window(
        candidates + (future,),
        catalog=catalog,
        policy=policy,
        evaluated_at=float(291 * 300),
    )
    assert rollback.clock_rollback_detected
    assert future in rollback.retained
    assert rollback.plan_sha256 != plan.plan_sha256


def test_hot_window_planner_is_bounded_and_rejects_ambiguous_sequences() -> None:
    profile = _profile()
    catalog = _catalog(profile)
    candidate = CompletionOutputHotCandidate(
        source_profile_sha256=profile.fingerprint,
        event_id=_event("one"),
        stream_sequence=1,
        created_at=1.0,
        source_row_sha256=_sha("source"),
        source_row_bytes=10,
    )
    with pytest.raises(CompletionOutputArchiveCapacityError):
        plan_hot_window(
            (candidate, replace(candidate, event_id=_event("two"))),
            catalog=catalog,
            policy=_policy(max_plan_records=1),
            evaluated_at=100.0,
        )
    with pytest.raises(CompletionOutputArchiveConflict):
        plan_hot_window(
            (candidate, replace(candidate, event_id=_event("two"))),
            catalog=catalog,
            policy=_policy(),
            evaluated_at=100.0,
        )


def test_cadence_horizon_uses_all_profiles_and_both_logical_caps() -> None:
    profiles = tuple(
        _profile(f"p{index}", cadence_seconds=300 if index < 3 else 600)
        for index in range(6)
    )
    catalog = _catalog(*profiles)
    limits = CompletionOutputArchiveLimits(
        max_records=8_192,
        max_record_bytes=64 * 1024,
        max_total_record_bytes=128 * 1024 * 1024,
        max_database_bytes=256 * 1024 * 1024,
        busy_timeout_ms=5_000,
    )
    horizon = calculate_cadence_horizon(catalog, limits)
    assert horizon.nominal_records_per_day == pytest.approx(1_296.0)
    assert horizon.count_cap_days == pytest.approx(8_192 / 1_296)
    assert horizon.worst_case_record_byte_cap_days == pytest.approx(2_048 / 1_296)
    assert horizon.logical_cap_days == horizon.worst_case_record_byte_cap_days
