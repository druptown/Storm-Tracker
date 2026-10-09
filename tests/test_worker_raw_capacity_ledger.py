from __future__ import annotations

import ast
from contextlib import closing
import hashlib
import inspect
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from storm_tracker_worker import raw_capacity_ledger as ledger_module
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityLedgerCapacityError,
    RawCapacityLedgerConflict,
    RawCapacityLedgerIntegrityError,
    RawCapacityProfile,
)


CONTENT = "1" * 64
RECEIPT = "2" * 64
CAPTURE = "3" * 64


def _profile(
    *,
    global_bytes: int = 1_000,
    global_entries: int = 20,
    provider_bytes: int = 800,
    provider_entries: int = 16,
    max_reservations: int = 100,
    max_catalog_bytes: int = 4 * 1024 * 1024,
    busy: float = 2.0,
) -> RawCapacityProfile:
    return RawCapacityProfile(
        global_max_bytes=global_bytes,
        global_max_entries=global_entries,
        providers=(
            ProviderCapacityLimit("kmi", provider_bytes, provider_entries),
            ProviderCapacityLimit("netatmo", provider_bytes, provider_entries),
        ),
        max_reservations=max_reservations,
        max_catalog_bytes=max_catalog_bytes,
        sqlite_busy_timeout_seconds=busy,
    )


def _ledger(tmp_path: Path, *, profile=None, **kwargs) -> RawCapacityLedger:
    return RawCapacityLedger(
        (tmp_path / "capacity.sqlite3").resolve(),
        profile=profile or _profile(),
        **kwargs,
    )


def _reserve(
    ledger: RawCapacityLedger,
    number: int = 1,
    *,
    provider: str = "kmi",
    byte_count: int = 100,
    entries: int = 2,
):
    return ledger.reserve(
        reservation_id=f"reservation-{number}",
        provider=provider,
        job_identity=f"job-{number}",
        requested_bytes=byte_count,
        requested_entries=entries,
    )


def _commit(
    ledger: RawCapacityLedger,
    number: int = 1,
    *,
    byte_count: int = 100,
    entries: int = 2,
    content: str = CONTENT,
    receipt: str = RECEIPT,
    capture: str = CAPTURE,
):
    return ledger.commit(
        reservation_id=f"reservation-{number}",
        actual_bytes=byte_count,
        actual_entries=entries,
        content_sha256=content,
        receipt_sha256=receipt,
        capture_sha256=capture,
    )


def test_profile_is_immutable_bounded_and_candidate_only(tmp_path):
    profile = _profile()
    ledger = _ledger(tmp_path, profile=profile)
    stats = ledger.stats()
    assert stats["reservation_rows"] == 0
    assert stats["global_max_bytes"] == 1_000
    assert stats["operational_admission_eligible"] == 0
    assert not Path(str(ledger.path) + "-wal").exists()
    assert not Path(str(ledger.path) + "-shm").exists()
    with closing(sqlite3.connect(ledger.path)) as database, database:
        assert database.execute("PRAGMA journal_mode").fetchone()[0].lower() == "delete"
        manifest = dict(database.execute("SELECT key,value FROM manifest"))
    assert manifest["profile_sha256"] == hashlib.sha256(
        manifest["profile_json"].encode()
    ).hexdigest()
    assert manifest["admission_status"] == "candidate_only_unadmitted"


def test_reserve_exact_retry_is_idempotent_and_counts_once(tmp_path):
    ledger = _ledger(tmp_path, clock=lambda: 100.0)
    first = _reserve(ledger)
    second = _reserve(ledger)
    assert second == first
    assert first.status == "RESERVED"
    assert first.actual_bytes is None
    stats = ledger.stats()
    assert stats["reservation_rows"] == 1
    assert stats["reserved_count"] == 1
    assert stats["reserved_bytes"] == 100
    assert stats["reserved_entries"] == 2
    assert stats["committed_count"] == 0
    assert ledger.audit()["reservation_rows"] == 1


@pytest.mark.parametrize(
    "change",
    [
        {"provider": "netatmo"},
        {"job_identity": "job-other"},
        {"requested_bytes": 101},
        {"requested_entries": 3},
    ],
)
def test_mismatched_reservation_retry_conflicts_without_counter_change(
    tmp_path, change
):
    ledger = _ledger(tmp_path)
    _reserve(ledger)
    request = {
        "reservation_id": "reservation-1",
        "provider": "kmi",
        "job_identity": "job-1",
        "requested_bytes": 100,
        "requested_entries": 2,
    }
    request.update(change)
    with pytest.raises(RawCapacityLedgerConflict):
        ledger.reserve(**request)
    assert ledger.stats()["reservation_rows"] == 1


def test_same_provider_job_cannot_acquire_a_second_reservation_id(tmp_path):
    ledger = _ledger(tmp_path)
    _reserve(ledger)
    with pytest.raises(RawCapacityLedgerConflict, match="job identity"):
        ledger.reserve(
            reservation_id="reservation-other",
            provider="kmi",
            job_identity="job-1",
            requested_bytes=100,
            requested_entries=2,
        )


def test_global_and_provider_capacity_are_checked_atomically(tmp_path):
    ledger = _ledger(
        tmp_path,
        profile=_profile(
            global_bytes=150,
            global_entries=3,
            provider_bytes=120,
            provider_entries=3,
        ),
    )
    _reserve(ledger, byte_count=100, entries=2)
    with pytest.raises(RawCapacityLedgerCapacityError, match="provider byte"):
        _reserve(ledger, 2, byte_count=30, entries=1)
    with pytest.raises(RawCapacityLedgerCapacityError, match="global byte"):
        _reserve(ledger, 3, provider="netatmo", byte_count=60, entries=1)
    assert ledger.stats()["reservation_rows"] == 1


def test_commit_binds_all_hashes_and_exact_retry_is_idempotent(tmp_path):
    times = iter((100.0, 101.0))
    ledger = _ledger(tmp_path, clock=lambda: next(times))
    reserved = _reserve(ledger)
    committed = _commit(ledger, byte_count=80, entries=1)
    retried = _commit(ledger, byte_count=80, entries=1)
    assert retried == committed
    assert committed.status == "COMMITTED"
    assert committed.reservation_sha256 == reserved.reservation_sha256
    assert committed.content_sha256 == CONTENT
    assert committed.receipt_sha256 == RECEIPT
    assert committed.capture_sha256 == CAPTURE
    assert committed.commit_sha256 is not None
    stats = ledger.stats()
    assert stats["reserved_count"] == 0
    assert stats["reserved_bytes"] == 0
    assert stats["committed_count"] == 1
    assert stats["committed_bytes"] == 80
    assert stats["committed_entries"] == 1
    assert ledger.audit()["committed_count"] == 1


@pytest.mark.parametrize(
    "change",
    [
        {"actual_bytes": 81},
        {"actual_entries": 2},
        {"content_sha256": "4" * 64},
        {"receipt_sha256": "5" * 64},
        {"capture_sha256": "6" * 64},
    ],
)
def test_mismatched_committed_retry_conflicts_and_never_mutates(tmp_path, change):
    ledger = _ledger(tmp_path)
    _reserve(ledger)
    original = _commit(ledger, byte_count=80, entries=1)
    request = {
        "reservation_id": "reservation-1",
        "actual_bytes": 80,
        "actual_entries": 1,
        "content_sha256": CONTENT,
        "receipt_sha256": RECEIPT,
        "capture_sha256": CAPTURE,
    }
    request.update(change)
    with pytest.raises(RawCapacityLedgerConflict):
        ledger.commit(**request)
    assert ledger.load("reservation-1") == original


def test_commit_above_reservation_rechecks_capacity_and_can_succeed(tmp_path):
    ledger = _ledger(tmp_path)
    _reserve(ledger, byte_count=100, entries=1)
    committed = _commit(ledger, byte_count=150, entries=3)
    assert committed.actual_bytes == 150
    assert committed.actual_entries == 3
    assert ledger.stats()["committed_bytes"] == 150


def test_commit_above_reservation_capacity_failure_is_atomic(tmp_path):
    ledger = _ledger(
        tmp_path,
        profile=_profile(
            global_bytes=200,
            global_entries=4,
            provider_bytes=200,
            provider_entries=4,
        ),
    )
    _reserve(ledger, byte_count=100, entries=2)
    _reserve(ledger, 2, provider="netatmo", byte_count=100, entries=2)
    before = ledger.stats()
    with pytest.raises(RawCapacityLedgerCapacityError, match="global byte"):
        _commit(ledger, byte_count=101, entries=2)
    assert ledger.stats() == before
    assert ledger.load("reservation-1").status == "RESERVED"


def test_smaller_commit_replaces_reservation_margin_then_committed_never_releases(
    tmp_path,
):
    ledger = _ledger(
        tmp_path,
        profile=_profile(
            global_bytes=150,
            global_entries=3,
            provider_bytes=150,
            provider_entries=3,
        ),
    )
    _reserve(ledger, byte_count=150, entries=3)
    _commit(ledger, byte_count=100, entries=2)
    _reserve(ledger, 2, byte_count=50, entries=1)
    assert ledger.stats()["committed_bytes"] == 100
    assert ledger.stats()["reserved_bytes"] == 50

    with closing(sqlite3.connect(ledger.path)) as database, database:
        with pytest.raises(sqlite3.IntegrityError, match="never released"):
            database.execute(
                "DELETE FROM reservations WHERE reservation_id='reservation-1'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="only RESERVED"):
            database.execute(
                "UPDATE reservations SET status='RESERVED' "
                "WHERE reservation_id='reservation-1'"
            )
    assert ledger.audit()["committed_bytes"] == 100


def test_unknown_provider_and_row_capacity_fail_without_writes(tmp_path):
    ledger = _ledger(tmp_path, profile=_profile(max_reservations=1))
    with pytest.raises(RawCapacityLedgerCapacityError, match="no immutable"):
        _reserve(ledger, provider="opera")
    _reserve(ledger)
    with pytest.raises(RawCapacityLedgerCapacityError, match="row capacity"):
        _reserve(ledger, 2, provider="netatmo")
    assert ledger.stats()["reservation_rows"] == 1


def test_profile_drift_is_rejected_before_use(tmp_path):
    first = _ledger(tmp_path)
    _reserve(first)
    drifted = _profile(global_bytes=999)
    with pytest.raises(RawCapacityLedgerIntegrityError, match="profile"):
        RawCapacityLedger(first.path, profile=drifted)
    assert first.audit()["reservation_rows"] == 1


def test_schema_and_aggregate_tamper_are_detected_read_only(tmp_path):
    ledger = _ledger(tmp_path)
    _reserve(ledger)
    with closing(sqlite3.connect(ledger.path)) as database, database:
        database.execute(
            "UPDATE global_capacity SET reserved_bytes=reserved_bytes+1 "
            "WHERE singleton=1"
        )
        database.commit()
    with pytest.raises(RawCapacityLedgerIntegrityError, match="do not sum"):
        ledger.stats()
    with pytest.raises(RawCapacityLedgerIntegrityError):
        ledger.audit()

    other_path = (tmp_path / "schema.sqlite3").resolve()
    other = RawCapacityLedger(other_path, profile=_profile())
    with closing(sqlite3.connect(other.path)) as database, database:
        database.execute("DROP INDEX reservations_provider_status")
        database.commit()
    with pytest.raises(RawCapacityLedgerIntegrityError, match="fingerprint"):
        RawCapacityLedger(other.path, profile=_profile())


def test_audit_is_read_only_and_stats_has_no_reservation_scan(tmp_path):
    ledger = _ledger(tmp_path)
    _reserve(ledger)
    before = ledger.path.read_bytes()
    result = ledger.audit()
    after = ledger.path.read_bytes()
    assert result["automatic_reclaim_performed"] == 0
    assert before == after
    source = inspect.getsource(RawCapacityLedger.stats)
    assert "FROM reservations" not in source
    assert "count(" not in source.lower()


def _profile_expression(profile: RawCapacityProfile) -> str:
    providers = ",".join(
        f"ProviderCapacityLimit({item.provider!r},{item.max_bytes},{item.max_entries})"
        for item in profile.providers
    )
    if len(profile.providers) == 1:
        providers += ","
    return (
        "RawCapacityProfile("
        f"global_max_bytes={profile.global_max_bytes},"
        f"global_max_entries={profile.global_max_entries},"
        f"providers=({providers}),"
        f"max_reservations={profile.max_reservations},"
        f"max_catalog_bytes={profile.max_catalog_bytes},"
        f"sqlite_busy_timeout_seconds={profile.sqlite_busy_timeout_seconds})"
    )


def test_cross_process_begin_immediate_prevents_global_oversubscription(tmp_path):
    profile = RawCapacityProfile(
        global_max_bytes=100,
        global_max_entries=2,
        providers=(
            ProviderCapacityLimit("kmi", 100, 2),
            ProviderCapacityLimit("netatmo", 100, 2),
        ),
        max_reservations=10,
        max_catalog_bytes=4 * 1024 * 1024,
        sqlite_busy_timeout_seconds=5.0,
    )
    ledger = _ledger(tmp_path, profile=profile)
    expression = _profile_expression(profile)
    source = (
        "import sys;"
        "from storm_tracker_worker.raw_capacity_ledger import *;"
        f"p={expression};"
        "x=RawCapacityLedger(sys.argv[1],profile=p);"
        "\ntry:\n"
        " x.reserve(reservation_id=sys.argv[2],provider=sys.argv[3],"
        "job_identity=sys.argv[4],requested_bytes=60,requested_entries=1)\n"
        "except RawCapacityLedgerCapacityError:\n sys.exit(2)\n"
    )
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                source,
                str(ledger.path),
                f"process-{index}",
                provider,
                f"job-process-{index}",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for index, provider in enumerate(("kmi", "netatmo"))
    ]
    results = [process.communicate(timeout=30) + (process.returncode,) for process in processes]
    assert sorted(item[2] for item in results) == [0, 2], results
    stats = ledger.stats()
    assert stats["reservation_rows"] == 1
    assert stats["reserved_bytes"] == 60
    assert ledger.audit()["reservation_rows"] == 1


def test_two_cross_process_growing_commits_cannot_oversubscribe(tmp_path):
    profile = RawCapacityProfile(
        global_max_bytes=220,
        global_max_entries=10,
        providers=(
            ProviderCapacityLimit("kmi", 220, 10),
            ProviderCapacityLimit("netatmo", 220, 10),
        ),
        max_reservations=10,
        max_catalog_bytes=4 * 1024 * 1024,
        sqlite_busy_timeout_seconds=5.0,
    )
    ledger = _ledger(tmp_path, profile=profile)
    _reserve(ledger, 1, provider="kmi", byte_count=50, entries=1)
    _reserve(ledger, 2, provider="netatmo", byte_count=50, entries=1)
    expression = _profile_expression(profile)
    source = (
        "import sys\n"
        "from storm_tracker_worker.raw_capacity_ledger import *\n"
        f"p={expression}\n"
        "x=RawCapacityLedger(sys.argv[1],profile=p)\n"
        "try:\n"
        f" x.commit(reservation_id=sys.argv[2],actual_bytes=150,actual_entries=1,"
        f"content_sha256={CONTENT!r},receipt_sha256={RECEIPT!r},"
        f"capture_sha256={CAPTURE!r})\n"
        "except RawCapacityLedgerCapacityError:\n sys.exit(2)\n"
    )
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", source, str(ledger.path), reservation],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for reservation in ("reservation-1", "reservation-2")
    ]
    results = [process.communicate(timeout=30) + (process.returncode,) for process in processes]
    assert sorted(item[2] for item in results) == [0, 2], results
    stats = ledger.stats()
    assert stats["committed_count"] == 1
    assert stats["reserved_count"] == 1
    assert stats["committed_bytes"] == 150
    assert stats["reserved_bytes"] == 50
    assert stats["committed_bytes"] + stats["reserved_bytes"] == 200
    assert ledger.audit()["reservation_rows"] == 2


def test_exact_retries_repeat_durability_barrier_and_refuse_return_on_failure(
    tmp_path, monkeypatch
):
    ledger = _ledger(tmp_path)
    reserved = _reserve(ledger)
    original_barrier = ledger._durability_barrier

    def fail_barrier():
        raise OSError("synthetic durability barrier failure")

    monkeypatch.setattr(ledger, "_durability_barrier", fail_barrier)
    with pytest.raises(OSError, match="durability"):
        _reserve(ledger)
    monkeypatch.setattr(ledger, "_durability_barrier", original_barrier)
    assert ledger.load("reservation-1") == reserved
    assert ledger.stats()["reservation_rows"] == 1

    committed = _commit(ledger, byte_count=80, entries=1)
    monkeypatch.setattr(ledger, "_durability_barrier", fail_barrier)
    with pytest.raises(OSError, match="durability"):
        _commit(ledger, byte_count=80, entries=1)
    monkeypatch.setattr(ledger, "_durability_barrier", original_barrier)
    assert ledger.load("reservation-1") == committed
    stats = ledger.stats()
    assert stats["committed_count"] == 1
    assert stats["committed_bytes"] == 80
    assert ledger.audit()["reservation_rows"] == 1


@pytest.mark.parametrize(
    "fault_point,persists",
    [
        ("reserve.after_aggregate", False),
        ("reserve.after_commit", True),
    ],
)
def test_real_process_crash_boundary_keeps_or_rolls_back_whole_reservation(
    tmp_path, fault_point, persists
):
    profile = _profile()
    ledger = _ledger(tmp_path, profile=profile)
    expression = _profile_expression(profile)
    source = (
        "import os,sys\n"
        "from storm_tracker_worker.raw_capacity_ledger import *\n"
        f"p={expression}\n"
        f"point={fault_point!r}\n"
        "def crash(value):\n"
        "  if value==point: os._exit(73)\n"
        "x=RawCapacityLedger(sys.argv[1],profile=p,fault_injector=crash)\n"
        "x.reserve(reservation_id='crash-reservation',provider='kmi',"
        "job_identity='crash-job',requested_bytes=100,requested_entries=2)"
    )
    completed = subprocess.run(
        [sys.executable, "-c", source, str(ledger.path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 73, completed.stderr
    reopened = RawCapacityLedger(ledger.path, profile=profile)
    receipt = reopened.load("crash-reservation")
    assert (receipt is not None) is persists
    assert reopened.stats()["reserved_bytes"] == (100 if persists else 0)
    assert reopened.audit()["reservation_rows"] == (1 if persists else 0)


def test_committed_after_process_crash_is_durable_and_never_reclaimed(tmp_path):
    profile = _profile()
    ledger = _ledger(tmp_path, profile=profile)
    _reserve(ledger)
    expression = _profile_expression(profile)
    source = (
        "import os,sys\n"
        "from storm_tracker_worker.raw_capacity_ledger import *\n"
        f"p={expression}\n"
        "def crash(value):\n"
        "  if value=='commit.after_commit': os._exit(74)\n"
        "x=RawCapacityLedger(sys.argv[1],profile=p,fault_injector=crash)\n"
        f"x.commit(reservation_id='reservation-1',actual_bytes=80,actual_entries=1,"
        f"content_sha256={CONTENT!r},receipt_sha256={RECEIPT!r},capture_sha256={CAPTURE!r})"
    )
    completed = subprocess.run(
        [sys.executable, "-c", source, str(ledger.path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 74, completed.stderr
    reopened = RawCapacityLedger(ledger.path, profile=profile)
    assert reopened.load("reservation-1").status == "COMMITTED"
    assert reopened.stats()["committed_bytes"] == 80
    assert reopened.audit()["automatic_reclaim_performed"] == 0


def test_catalog_full_is_normalized_and_failed_insert_is_atomic(tmp_path):
    profile = RawCapacityProfile(
        global_max_bytes=10_000_000,
        global_max_entries=20_000,
        providers=(ProviderCapacityLimit("kmi", 10_000_000, 20_000),),
        max_reservations=10_000,
        max_catalog_bytes=1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )
    ledger = _ledger(tmp_path, profile=profile)
    exhausted = False
    for index in range(2_000):
        suffix = f"{index:04d}"
        try:
            ledger.reserve(
                reservation_id="r" + suffix + "x" * 240,
                provider="kmi",
                job_identity="j" + suffix + "y" * 240,
                requested_bytes=1,
                requested_entries=1,
            )
        except RawCapacityLedgerCapacityError as exc:
            assert "full" in str(exc).lower() or "envelope" in str(exc).lower()
            exhausted = True
            break
    assert exhausted is True
    audit = ledger.audit()
    stats = ledger.stats()
    assert audit["reservation_rows"] == stats["reservation_rows"]
    assert audit["catalog_file_bytes"] <= profile.max_catalog_bytes
    assert audit["catalog_page_size_bytes"] == stats["catalog_page_size_bytes"]
    assert audit["catalog_main_database_bytes"] == (
        stats["catalog_main_database_bytes"]
    )
    assert audit["catalog_main_database_max_bytes"] == (
        profile.max_catalog_bytes
        // 4
        // stats["catalog_page_size_bytes"]
        * stats["catalog_page_size_bytes"]
    )
    assert audit["catalog_main_database_bytes"] <= (
        audit["catalog_main_database_max_bytes"]
    )


def test_no_release_abandon_or_automatic_reconcile_api_exists():
    public = {name for name in dir(RawCapacityLedger) if not name.startswith("_")}
    assert public == {
        "accounting_reservations",
        "audit",
        "commit",
        "initialize",
        "load",
        "load_for_job",
        "provider_stats",
        "reserve",
        "stats",
    }
    source = Path(ledger_module.__file__).read_text(encoding="utf-8")
    assert "raw_artifacts" not in source
    assert "service" not in source
    assert "homeassistant" not in source


_CAPACITY_LEDGER_ALL_NATIVE_CRASH_POINTS = {
    "initialize": (
        "initialize.before_commit",
        "initialize.after_commit",
    ),
    "reserve": (
        "reserve.after_begin",
        "reserve.after_reservation",
        "reserve.after_aggregate",
        "reserve.before_commit",
        "reserve.after_commit",
    ),
    "commit": (
        "commit.after_begin",
        "commit.after_reservation",
        "commit.after_aggregate",
        "commit.before_commit",
        "commit.after_commit",
    ),
}
_CAPACITY_LEDGER_LEGACY_NATIVE_CRASH_POINTS = frozenset({
    "reserve.after_aggregate",
    "reserve.after_commit",
    "commit.after_commit",
})
_CAPACITY_LEDGER_MISSING_NATIVE_CRASH_CASES = (
    ("initialize", "initialize.before_commit", "uninitialized"),
    ("initialize", "initialize.after_commit", "empty"),
    ("reserve", "reserve.after_begin", "empty"),
    ("reserve", "reserve.after_reservation", "empty"),
    ("reserve", "reserve.before_commit", "empty"),
    ("commit", "commit.after_begin", "reserved"),
    ("commit", "commit.after_reservation", "reserved"),
    ("commit", "commit.after_aggregate", "reserved"),
    ("commit", "commit.before_commit", "reserved"),
)
_CAPACITY_LEDGER_TABLES = frozenset({
    "manifest",
    "global_capacity",
    "provider_capacity",
    "reservations",
})


def _capacity_ledger_faultpoint_inventory() -> dict[str, tuple[str, ...]]:
    source = Path(ledger_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(ledger_module.__file__))
    ledger_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "RawCapacityLedger"
    )
    discovered: dict[str, tuple[str, ...]] = {}
    for method in ledger_class.body:
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


def _capacity_ledger_raw_snapshot(path: Path) -> dict[str, object]:
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
                "counters": (0, 0, 0, 0, 0, 0, 0),
                "integrity": integrity,
                "receipt": None,
                "tables": tables,
            }
        counters = tuple(
            int(value)
            for value in database.execute(
                "SELECT reservation_rows,reserved_count,committed_count,"
                "reserved_bytes,committed_bytes,reserved_entries,committed_entries "
                "FROM global_capacity WHERE singleton=1"
            ).fetchone()
        )
        receipt = database.execute(
            "SELECT status,requested_bytes,requested_entries,actual_bytes,"
            "actual_entries,content_sha256,receipt_sha256,capture_sha256,"
            "commit_sha256 FROM reservations WHERE reservation_id=?",
            ("crash-reservation",),
        ).fetchone()
        return {
            "counters": counters,
            "integrity": integrity,
            "receipt": None if receipt is None else tuple(receipt),
            "tables": tables,
        }


def test_capacity_ledger_native_crash_inventory_pins_exact_missing_partition():
    discovered = _capacity_ledger_faultpoint_inventory()
    assert discovered == _CAPACITY_LEDGER_ALL_NATIVE_CRASH_POINTS
    all_points = frozenset(
        point for points in discovered.values() for point in points
    )
    missing = frozenset(
        point
        for _operation, point, _state
        in _CAPACITY_LEDGER_MISSING_NATIVE_CRASH_CASES
    )
    assert len(all_points) == sum(map(len, discovered.values())) == 12
    assert len(missing) == len(_CAPACITY_LEDGER_MISSING_NATIVE_CRASH_CASES) == 9
    assert missing.isdisjoint(_CAPACITY_LEDGER_LEGACY_NATIVE_CRASH_POINTS)
    assert missing | _CAPACITY_LEDGER_LEGACY_NATIVE_CRASH_POINTS == all_points


@pytest.mark.parametrize(
    ("operation", "checkpoint", "state_at_crash"),
    _CAPACITY_LEDGER_MISSING_NATIVE_CRASH_CASES,
    ids=(
        "initialize-before-commit-old",
        "initialize-after-commit-new-empty",
        "reserve-after-begin-old",
        "reserve-after-reservation-old",
        "reserve-before-commit-old",
        "commit-after-begin-old",
        "commit-after-reservation-old",
        "commit-after-aggregate-old",
        "commit-before-commit-old",
    ),
)
def test_missing_capacity_ledger_native_crash_reopens_and_replays_exactly(
    tmp_path: Path,
    operation: str,
    checkpoint: str,
    state_at_crash: str,
) -> None:
    case_root = (tmp_path / checkpoint.replace(".", "-")).resolve()
    case_root.mkdir(parents=True)
    path = (case_root / "capacity.sqlite3").resolve()
    profile = _profile()
    if operation != "initialize":
        prepared = RawCapacityLedger(
            path,
            profile=profile,
            clock=lambda: 1_800_299_990.0,
        )
        if operation == "commit":
            prepared.reserve(
                reservation_id="crash-reservation",
                provider="kmi",
                job_identity="crash-job",
                requested_bytes=100,
                requested_entries=2,
            )

    helper = (
        Path(__file__).resolve().parent
        / "helpers"
        / "worker_raw_capacity_ledger_missing_crash_child.py"
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

    at_crash = _capacity_ledger_raw_snapshot(path)
    assert at_crash["integrity"] == "ok"
    if state_at_crash == "uninitialized":
        assert at_crash["tables"] == frozenset()
    else:
        assert at_crash["tables"] == _CAPACITY_LEDGER_TABLES
    if state_at_crash == "reserved":
        assert at_crash["counters"] == (1, 1, 0, 100, 0, 2, 0)
        receipt_at_crash = at_crash["receipt"]
        assert receipt_at_crash is not None
        assert receipt_at_crash[:3] == ("RESERVED", 100, 2)
        assert all(value is None for value in receipt_at_crash[3:])
    else:
        assert at_crash["counters"] == (0, 0, 0, 0, 0, 0, 0)
        assert at_crash["receipt"] is None

    reopened = RawCapacityLedger(
        path,
        profile=profile,
        clock=lambda: 1_800_300_010.0,
    )
    before = reopened.load("crash-reservation")
    assert (before is not None) is (state_at_crash == "reserved")
    reserved = reopened.reserve(
        reservation_id="crash-reservation",
        provider="kmi",
        job_identity="crash-job",
        requested_bytes=100,
        requested_entries=2,
    )
    assert reserved.status == "RESERVED"
    committed = reopened.commit(
        reservation_id="crash-reservation",
        actual_bytes=80,
        actual_entries=1,
        content_sha256=CONTENT,
        receipt_sha256=RECEIPT,
        capture_sha256=CAPTURE,
    )
    assert committed.status == "COMMITTED"
    assert committed.actual_bytes == 80
    assert committed.actual_entries == 1
    stable_stats = reopened.stats()
    stable_provider = reopened.provider_stats("kmi")
    stable_audit = reopened.audit()
    assert stable_stats["reservation_rows"] == 1
    assert stable_stats["reserved_count"] == 0
    assert stable_stats["committed_count"] == 1
    assert stable_stats["committed_bytes"] == 80
    assert stable_audit["reservation_rows"] == 1

    replayed = RawCapacityLedger(
        path,
        profile=profile,
        clock=lambda: 1_800_300_020.0,
    )
    assert replayed.reserve(
        reservation_id="crash-reservation",
        provider="kmi",
        job_identity="crash-job",
        requested_bytes=100,
        requested_entries=2,
    ) == committed
    assert replayed.commit(
        reservation_id="crash-reservation",
        actual_bytes=80,
        actual_entries=1,
        content_sha256=CONTENT,
        receipt_sha256=RECEIPT,
        capture_sha256=CAPTURE,
    ) == committed
    assert replayed.load("crash-reservation") == committed
    assert replayed.stats() == stable_stats
    assert replayed.provider_stats("kmi") == stable_provider
    assert replayed.audit() == stable_audit
