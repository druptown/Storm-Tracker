"""Crash, capacity and privacy proofs for sealed ground-shadow retention."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
import ast
import errno
from pathlib import Path
import sqlite3

import pytest

from storm_tracker_worker import ground_fusion_shadow_runtime as runtime_module
from storm_tracker_worker.ground_fusion_shadow_runtime import (
    GROUND_FUSION_SHADOW_ROTATION_FAULT_POINTS,
    GroundFusionShadowArchiveError,
    GroundFusionShadowCapacityError,
    GroundFusionShadowConflict,
    GroundFusionShadowDeadlineError,
    GroundFusionShadowDurabilityError,
    GroundFusionShadowLimits,
    GroundFusionShadowRetentionPolicy,
    GroundFusionShadowStoreError,
    GroundFusionShadowWindowExpired,
)
from tests.test_worker_ground_fusion_shadow_runtime import (
    NOW,
    _batch,
    _deadline,
    _install_batch,
    _request,
    _runtime,
)


class SimulatedCrash(BaseException):
    pass


def _policy(**changes) -> GroundFusionShadowRetentionPolicy:
    values = {
        "archive_namespace": "ground-shadow-retention-tests",
        "hot_receipts_to_keep": 2,
        "max_segment_receipts": 2,
        "max_segment_bytes": 256 * 1024,
        "max_archive_segments": 32,
        "max_archived_receipts": 64,
        "max_total_archive_bytes": 4 * 1024 * 1024,
    }
    values.update(changes)
    return GroundFusionShadowRetentionPolicy(**values)


def _seed(tmp_path, monkeypatch, count=5, *, limits=None):
    _install_batch(monkeypatch, lambda registry: _batch(registry))
    active_limits = limits or replace(
        GroundFusionShadowLimits(),
        max_receipts=32,
    )
    runtime = _runtime(tmp_path, limits=active_limits)
    requests = tuple(
        _request(
            attempt=f"retention-attempt-{index}",
            evaluated=NOW + float(index),
        )
        for index in range(count)
    )
    receipts = tuple(
        runtime.process(request, deadline_at=_deadline()).receipt
        for request in requests
    )
    return runtime, requests, receipts, active_limits


def _fsync_log():
    calls: list[Path] = []

    def barrier(path: Path) -> None:
        assert path.exists() and path.is_dir() and not path.is_symlink()
        calls.append(path)

    return calls, barrier


def test_rotation_seals_before_prune_preserves_hot_window_and_explicit_lookup(
    tmp_path,
    monkeypatch,
):
    runtime, requests, receipts, limits = _seed(tmp_path, monkeypatch)
    archive = tmp_path / "sealed-ground-archive"
    policy = _policy()
    fsync_calls, fsync = _fsync_log()

    first = runtime.receipt_store.rotate_archive(
        archive,
        policy=policy,
        deadline_at=_deadline(),
        directory_fsync=fsync,
    )
    second = runtime.receipt_store.rotate_archive(
        archive,
        policy=policy,
        deadline_at=_deadline(),
        directory_fsync=fsync,
    )

    assert first.archived_receipts == 2
    assert second.archived_receipts == 1
    assert second.hot_receipts == 2
    assert len(tuple(archive.glob("*.sealed.json"))) == 2
    assert len(fsync_calls) >= 6
    hot_retry = runtime.process(requests[-1], deadline_at=_deadline())
    assert hot_retry.duplicate is True
    assert hot_retry.receipt.receipt_sha256 == receipts[-1].receipt_sha256
    with pytest.raises(GroundFusionShadowWindowExpired) as expired:
        runtime.process(requests[0], deadline_at=_deadline())
    assert expired.value.segment_id == first.segment_id
    with pytest.raises(GroundFusionShadowWindowExpired):
        runtime.process(
            replace(requests[0], attempt_id="same-request-new-attempt"),
            deadline_at=_deadline(),
        )
    with pytest.raises(GroundFusionShadowConflict):
        runtime.process(
            replace(requests[0], evaluated_at=NOW + 20.0),
            deadline_at=_deadline(),
        )

    reopened = _runtime(tmp_path, limits=limits)
    restored = reopened.receipt_store.load_archived(
        archive,
        policy=policy,
        attempt_id=requests[0].attempt_id,
    )
    assert restored == receipts[0]
    assert reopened.receipt_store.load_archived(
        archive,
        policy=policy,
        request_sha256=receipts[2].request_sha256,
    ) == receipts[2]
    audit = runtime.receipt_store.audit_archive(archive, policy=policy)
    assert audit["healthy"] is True
    assert audit["archived_receipts"] == 3
    assert audit["hot_receipts"] == 2
    diagnostics = runtime.receipt_store.diagnostics()
    assert diagnostics["archive_segment_count"] == 2
    assert diagnostics["archived_receipt_count"] == 3
    assert diagnostics["receipt_count"] == 2

    archive_bytes = b"".join(
        item.read_bytes() for item in archive.glob("*.sealed.json")
    )
    for forbidden in (
        b"device_tracker.private_person",
        b"70:ee:50:29:59:56",
        b"40.75",
        b"-73.98",
        b"Authorization",
        b"super-secret-token",
    ):
        assert forbidden not in archive_bytes


@pytest.mark.parametrize("fault_point", GROUND_FUSION_SHADOW_ROTATION_FAULT_POINTS)
def test_every_rotation_boundary_is_restart_safe(
    tmp_path,
    monkeypatch,
    fault_point,
):
    runtime, _requests, _receipts, limits = _seed(
        tmp_path,
        monkeypatch,
        count=4,
    )
    archive = tmp_path / "fault-archive"
    policy = _policy()
    _calls, fsync = _fsync_log()

    def fail(point: str) -> None:
        if point == fault_point:
            raise SimulatedCrash(point)

    with pytest.raises(SimulatedCrash):
        runtime.receipt_store.rotate_archive(
            archive,
            policy=policy,
            deadline_at=_deadline(),
            fault_injector=fail,
            directory_fsync=fsync,
        )

    reopened = _runtime(tmp_path, limits=limits)
    committed = fault_point in {
        "rotate.after_commit",
        "rotate.after_database_directory_fsync",
    }
    manifest_committed_pending_prune = fault_point in {
        "rotate.after_manifest_commit",
        "rotate.after_manifest_database_fsync",
        "rotate.before_prune",
        "rotate.after_prune",
        "rotate.before_commit",
    }
    diagnostics = reopened.receipt_store.diagnostics()
    if committed:
        assert diagnostics["receipt_count"] == 2
        assert diagnostics["archived_receipt_count"] == 2
    elif manifest_committed_pending_prune:
        assert diagnostics["receipt_count"] == 4
        assert diagnostics["archived_receipt_count"] == 2
        recovered = reopened.receipt_store.rotate_archive(
            archive,
            policy=policy,
            deadline_at=_deadline(),
            directory_fsync=fsync,
        )
        assert recovered.archived_receipts == 0
        assert recovered.hot_receipts == 2
    else:
        assert diagnostics["receipt_count"] == 4
        assert diagnostics["archived_receipt_count"] == 0
        recovered = reopened.receipt_store.rotate_archive(
            archive,
            policy=policy,
            deadline_at=_deadline(),
            directory_fsync=fsync,
        )
        assert recovered.archived_receipts == 2
        assert recovered.duplicate_orphan_reused is (
            fault_point
            in {
                "rotate.after_publish_replace",
                "rotate.after_publish_directory_fsync",
                "rotate.after_manifest_insert",
                "rotate.after_archive_index",
                "rotate.after_anchor_cas",
                "rotate.before_manifest_commit",
            }
        )
    assert reopened.receipt_store.audit_archive(archive, policy=policy)[
        "healthy"
    ] is True


def test_disk_full_and_segment_capacity_never_prune_hot_receipts(
    tmp_path,
    monkeypatch,
):
    runtime, _requests, _receipts, _limits = _seed(
        tmp_path,
        monkeypatch,
        count=4,
    )
    archive = tmp_path / "capacity-archive"
    _calls, fsync = _fsync_log()

    def disk_full(point: str) -> None:
        if point == "rotate.after_temp_write":
            raise OSError(errno.ENOSPC, "simulated disk full")

    with pytest.raises(GroundFusionShadowCapacityError, match="full"):
        runtime.receipt_store.rotate_archive(
            archive,
            policy=_policy(),
            deadline_at=_deadline(),
            fault_injector=disk_full,
            directory_fsync=fsync,
        )
    assert runtime.receipt_store.diagnostics()["receipt_count"] == 4
    assert tuple(archive.iterdir()) == ()

    too_small = _policy(max_segment_bytes=4_096)
    with pytest.raises(GroundFusionShadowCapacityError, match="segment byte"):
        runtime.receipt_store.rotate_archive(
            archive,
            policy=too_small,
            deadline_at=_deadline(),
            directory_fsync=fsync,
        )
    assert runtime.receipt_store.diagnostics()["receipt_count"] == 4

    def manifest_disk_full(point: str) -> None:
        if point == "rotate.after_archive_index":
            raise sqlite3.OperationalError("database or disk is full")

    with pytest.raises(GroundFusionShadowCapacityError, match="database is full"):
        runtime.receipt_store.rotate_archive(
            archive,
            policy=_policy(),
            deadline_at=_deadline(),
            fault_injector=manifest_disk_full,
            directory_fsync=fsync,
        )
    assert runtime.receipt_store.diagnostics()["receipt_count"] == 4
    assert runtime.receipt_store.diagnostics()["archived_receipt_count"] == 0
    recovered = runtime.receipt_store.rotate_archive(
        archive,
        policy=_policy(),
        deadline_at=_deadline(),
        directory_fsync=fsync,
    )
    assert recovered.duplicate_orphan_reused is True


def test_missing_directory_durability_barrier_fails_before_export(
    tmp_path,
    monkeypatch,
):
    runtime, _requests, _receipts, _limits = _seed(
        tmp_path,
        monkeypatch,
        count=4,
    )

    def unavailable(_path: Path) -> None:
        raise GroundFusionShadowDurabilityError("simulated unsupported barrier")

    with pytest.raises(GroundFusionShadowDurabilityError):
        runtime.receipt_store.rotate_archive(
            tmp_path / "no-durability-archive",
            policy=_policy(),
            deadline_at=_deadline(),
            directory_fsync=unavailable,
        )
    assert runtime.receipt_store.diagnostics()["receipt_count"] == 4
    assert runtime.receipt_store.diagnostics()["archived_receipt_count"] == 0


def test_deadline_during_temp_publication_cleans_temp_and_keeps_hot(
    tmp_path,
    monkeypatch,
):
    runtime, _requests, _receipts, _limits = _seed(
        tmp_path,
        monkeypatch,
        count=4,
    )
    archive = tmp_path / "deadline-archive"
    archive.mkdir()
    now = [0.0]
    runtime.receipt_store._monotonic = lambda: now[0]

    def expire(_path: Path) -> None:
        now[0] = 6.0

    with pytest.raises(GroundFusionShadowDeadlineError):
        runtime.receipt_store.rotate_archive(
            archive,
            policy=_policy(),
            deadline_at=5.0,
            directory_fsync=expire,
        )
    assert tuple(archive.iterdir()) == ()
    assert runtime.receipt_store.diagnostics()["receipt_count"] == 4
    assert runtime.receipt_store.diagnostics()["archived_receipt_count"] == 0


def test_partial_or_unanchored_file_fails_closed_without_prune(
    tmp_path,
    monkeypatch,
):
    runtime, _requests, _receipts, _limits = _seed(
        tmp_path,
        monkeypatch,
        count=4,
    )
    archive = tmp_path / "partial-archive"
    archive.mkdir()
    (archive / ".partial.tmp").write_bytes(b"partial")
    _calls, fsync = _fsync_log()

    with pytest.raises(GroundFusionShadowArchiveError, match="partial"):
        runtime.receipt_store.rotate_archive(
            archive,
            policy=_policy(),
            deadline_at=_deadline(),
            directory_fsync=fsync,
        )
    assert runtime.receipt_store.diagnostics()["receipt_count"] == 4
    with pytest.raises(GroundFusionShadowArchiveError, match="partial"):
        runtime.receipt_store.audit_archive(archive, policy=_policy())


def test_archive_tamper_and_manifest_gap_are_detected(
    tmp_path,
    monkeypatch,
):
    runtime, requests, _receipts, _limits = _seed(
        tmp_path,
        monkeypatch,
        count=6,
    )
    archive = tmp_path / "tamper-archive"
    policy = _policy()
    _calls, fsync = _fsync_log()
    runtime.receipt_store.rotate_archive(
        archive,
        policy=policy,
        deadline_at=_deadline(),
        directory_fsync=fsync,
    )
    runtime.receipt_store.rotate_archive(
        archive,
        policy=policy,
        deadline_at=_deadline(),
        directory_fsync=fsync,
    )
    first_file = sorted(archive.glob("*.sealed.json"))[0]
    original = first_file.read_bytes()
    first_file.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
    with pytest.raises(GroundFusionShadowArchiveError):
        runtime.receipt_store.load_archived(
            archive,
            policy=policy,
            attempt_id=requests[0].attempt_id,
        )
    first_file.write_bytes(original)

    with closing(sqlite3.connect(runtime.receipt_store.path)) as database:
        database.execute("DROP TRIGGER archive_segments_no_update")
        database.execute(
            "UPDATE archive_segments SET previous_segment_id=NULL "
            "WHERE first_sequence>1"
        )
        database.executescript(
            "CREATE TRIGGER archive_segments_no_update "
            "BEFORE UPDATE ON archive_segments "
            "BEGIN SELECT RAISE(ABORT, "
            "'sealed archive segment is immutable'); END;"
        )
        database.commit()
    with pytest.raises(GroundFusionShadowStoreError):
        runtime.receipt_store.audit_archive(archive, policy=policy)


def test_archive_row_and_policy_caps_fail_closed(
    tmp_path,
    monkeypatch,
):
    runtime, _requests, _receipts, _limits = _seed(
        tmp_path,
        monkeypatch,
        count=6,
    )
    archive = tmp_path / "row-cap-archive"
    policy = _policy(max_archive_segments=1, max_archived_receipts=2)
    _calls, fsync = _fsync_log()
    runtime.receipt_store.rotate_archive(
        archive,
        policy=policy,
        deadline_at=_deadline(),
        directory_fsync=fsync,
    )
    with pytest.raises(GroundFusionShadowCapacityError):
        runtime.receipt_store.rotate_archive(
            archive,
            policy=policy,
            deadline_at=_deadline(),
            directory_fsync=fsync,
        )
    assert runtime.receipt_store.diagnostics()["receipt_count"] == 4
    with pytest.raises(GroundFusionShadowConflict):
        runtime.receipt_store.rotate_archive(
            archive,
            policy=replace(policy, archive_namespace="different-archive"),
            deadline_at=_deadline(),
            directory_fsync=fsync,
        )


def test_total_archive_byte_cap_fails_before_second_manifest_or_prune(
    tmp_path,
    monkeypatch,
):
    runtime, _requests, _receipts, _limits = _seed(
        tmp_path,
        monkeypatch,
        count=6,
    )
    archive = tmp_path / "total-byte-cap-archive"
    policy = _policy(
        max_segment_bytes=20_000,
        max_total_archive_bytes=20_000,
    )
    _calls, fsync = _fsync_log()
    first = runtime.receipt_store.rotate_archive(
        archive,
        policy=policy,
        deadline_at=_deadline(),
        directory_fsync=fsync,
    )
    assert first.archived_receipts == 2
    before = runtime.receipt_store.diagnostics()

    with pytest.raises(GroundFusionShadowCapacityError, match="total sealed"):
        runtime.receipt_store.rotate_archive(
            archive,
            policy=policy,
            deadline_at=_deadline(),
            directory_fsync=fsync,
        )

    after = runtime.receipt_store.diagnostics()
    assert after["archive_segment_count"] == before["archive_segment_count"] == 1
    assert after["archived_receipt_count"] == before["archived_receipt_count"] == 2
    assert after["receipt_count"] == before["receipt_count"] == 4
    assert len(tuple(archive.glob("*.sealed.json"))) == 1


def test_concurrent_rotators_serialize_with_anchor_cas_and_lose_nothing(
    tmp_path,
    monkeypatch,
):
    first, _requests, _receipts, limits = _seed(
        tmp_path,
        monkeypatch,
        count=6,
    )
    second = _runtime(tmp_path, limits=limits)
    archive = tmp_path / "concurrent-archive"
    policy = _policy()
    _calls, fsync = _fsync_log()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (
            executor.submit(
                first.receipt_store.rotate_archive,
                archive,
                policy=policy,
                deadline_at=_deadline(),
                directory_fsync=fsync,
            ),
            executor.submit(
                second.receipt_store.rotate_archive,
                archive,
                policy=policy,
                deadline_at=_deadline(),
                directory_fsync=fsync,
            ),
        )
        results = tuple(item.result(timeout=20.0) for item in futures)

    assert sum(item.archived_receipts for item in results) == 4
    audit = first.receipt_store.audit_archive(archive, policy=policy)
    assert audit["archived_receipts"] == 4
    assert audit["hot_receipts"] == 2
    assert audit["segment_count"] == 2


def test_writer_between_manifest_and_prune_advances_only_sequence_cas(
    tmp_path,
    monkeypatch,
):
    rotating, _requests, _receipts, limits = _seed(
        tmp_path,
        monkeypatch,
        count=4,
    )
    writer = _runtime(tmp_path, limits=limits)
    archive = tmp_path / "writer-cas-archive"
    policy = _policy()
    _calls, fsync = _fsync_log()
    inserted = []

    def write_after_manifest(point: str) -> None:
        if point == "rotate.after_manifest_commit" and not inserted:
            inserted.append(writer.process(
                _request(
                    attempt="retention-concurrent-writer",
                    evaluated=NOW + 10.0,
                ),
                deadline_at=_deadline(),
            ).receipt)

    result = rotating.receipt_store.rotate_archive(
        archive,
        policy=policy,
        deadline_at=_deadline(),
        fault_injector=write_after_manifest,
        directory_fsync=fsync,
    )

    assert result.archived_receipts == 2
    assert result.hot_receipts == 3
    assert len(inserted) == 1
    audit = rotating.receipt_store.audit_archive(archive, policy=policy)
    assert audit["archived_receipts"] == 2
    assert audit["hot_receipts"] == 3
    assert writer.receipt_store.load_existing(
        attempt_id="retention-concurrent-writer",
        request_sha256=inserted[0].request_sha256,
    ) == inserted[0]


def test_archive_code_has_no_vacuum_or_live_service_import():
    source = Path(runtime_module.__file__).read_text(encoding="utf-8")
    assert "VACUUM" not in source.upper()
    tree = ast.parse(source)
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.add(node.module)
    assert all(
        "homeassistant" not in name.casefold()
        and "mqtt" not in name.casefold()
        and not name.casefold().endswith("service")
        for name in imports
    )
