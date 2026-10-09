from __future__ import annotations

import ast
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from storm_tracker_worker.raw_capture_retention_guard import (
    CONTRACT,
    RawCaptureRetentionGuard,
    RawCaptureRetentionGuardBlocked,
    RawCaptureRetentionGuardConfig,
)
from storm_tracker_worker.raw_artifacts import raw_catalog_cycle_reserve_bytes
from storm_tracker_worker.raw_capacity_ledger import (
    capacity_ledger_catalog_cycle_reserve_bytes,
)


MIB = 1024 * 1024


def test_blocked_reason_code_is_closed_and_sanitized() -> None:
    blocked = RawCaptureRetentionGuardBlocked(
        "filesystem_envelope_capacity_exhausted"
    )
    assert blocked.reason_code == "filesystem_envelope_capacity_exhausted"

    private_detail = "private-storage-path-and-token"
    sanitized = RawCaptureRetentionGuardBlocked(private_detail)
    assert sanitized.reason_code == "storage_integrity_failed"
    assert str(sanitized) == "storage_integrity_failed"
    assert private_detail not in str(sanitized)


def _config(tmp_path: Path, *, create_cadence: bool = True):
    cadence_raw = (tmp_path / "cadence-raw").resolve()
    ledger = (tmp_path / "capacity" / "ledger.sqlite3").resolve()
    event_control = (tmp_path / "event-control").resolve()
    event_raw = (tmp_path / "event-raw").resolve()
    if create_cadence:
        cadence_raw.mkdir(parents=True)
        ledger.parent.mkdir(parents=True)
        ledger.write_bytes(b"fixed-test-ledger")
    return RawCaptureRetentionGuardConfig(
        cadence_raw_root=cadence_raw,
        cadence_ledger_path=ledger,
        event_control_root=event_control,
        event_raw_root=event_raw,
        event_capacity_claimed=True,
        configured_persistent_max_bytes=128 * MIB,
        configured_raw_object_max_entries=200,
        cadence_ledger_max_bytes=32 * MIB,
        cadence_ledger_max_entries=100,
        cadence_ledger_max_reservations=100,
        cadence_ledger_catalog_max_bytes=16 * MIB,
        cadence_raw_max_bytes=32 * MIB,
        cadence_raw_max_entries=100,
        cadence_raw_max_receipts=100,
        cadence_raw_max_captures=100,
        cadence_raw_catalog_max_bytes=20 * MIB,
        cadence_cycle_reserve_bytes=1 * MIB,
        cadence_cycle_reserve_entries=4,
        cadence_cycle_reserve_captures=2,
        event_payload_reserve_bytes=256 * 1024,
        event_control_receipt_max_bytes=128 * 1024,
        event_control_intake_reserve=10,
        event_control_receipt_capacity=100,
        event_queue_capacity=50,
        event_retained_payload_max_bytes=16 * MIB,
        event_retained_payload_max_entries=100,
        host_operational_free_reserve_bytes=2 * 1024 * MIB,
    )


def _state() -> dict:
    return {
        "ledger": {
            "catalog_file_bytes": 1 * MIB,
            "catalog_main_database_bytes": 1 * MIB,
            "catalog_main_database_max_bytes": 4 * MIB,
            "catalog_page_size_bytes": 4096,
            "committed_bytes": 0,
            "committed_count": 0,
            "committed_entries": 0,
            "global_max_bytes": 32 * MIB,
            "global_max_entries": 100,
            "max_catalog_bytes": 16 * MIB,
            "max_reservations": 100,
            "reservation_rows": 0,
            "reserved_bytes": 0,
            "reserved_count": 0,
            "reserved_entries": 0,
        },
        "ledger_audit": {
            "reservation_rows": 0,
            "reserved_count": 0,
            "committed_count": 0,
            "reserved_bytes": 0,
            "committed_bytes": 0,
            "reserved_entries": 0,
            "committed_entries": 0,
        },
        "raw": {
            "catalog_bytes": 1 * MIB,
            "catalog_database_bytes": 1 * MIB,
            "catalog_main_database_bytes": 1 * MIB,
            "catalog_main_database_max_bytes": 5 * MIB,
            "catalog_page_size_bytes": 4096,
            "filesystem_bytes": 1 * MIB,
            "filesystem_object_entries": 1,
            "filesystem_staging_entries": 0,
            "max_catalog_bytes": 20 * MIB,
            "max_capture_entries": 100,
            "max_object_entries": 100,
            "max_receipt_entries": 100,
            "max_total_bytes": 32 * MIB,
            "capture_count": 1,
            "object_count": 1,
            "receipt_count": 1,
            "retention_policy": "none",
            "unmanaged_file_count": 0,
        },
        "raw_audit": {
            "checked_captures": 1,
            "checked_objects": 1,
            "checked_receipts": 1,
            "healthy": True,
            "issues": [],
            "reclamation": {"enabled": False},
        },
        "event": {
            "access_retention_authority": "available",
            "alarm_codes": [],
            "capacity": {
                "control_intake_headroom": 90,
                "control_receipt_capacity": 100,
                "control_receipts": 0,
                "queue_capacity": 50,
                "queue_headroom": 50,
                "retained_payload_byte_capacity": 16 * MIB,
                "retained_payload_byte_headroom": 16 * MIB,
                "retained_payload_entry_capacity": 100,
                "retained_payload_entry_headroom": 100,
            },
            "fault_latched": False,
            "network_started": False,
            "queue_depth": 0,
            "retained_payload_bytes": 0,
            "retained_payload_entries": 0,
            "state": "new",
        },
        "disk": SimpleNamespace(
            total=8 * 1024 * MIB,
            used=4 * 1024 * MIB,
            free=4 * 1024 * MIB,
        ),
    }


def _guard(
    config: RawCaptureRetentionGuardConfig,
    state: dict,
    *,
    ownership_assertion=lambda: None,
    fault_injector=None,
) -> RawCaptureRetentionGuard:
    return RawCaptureRetentionGuard(
        config=config,
        ownership_assertion=ownership_assertion,
        ledger_stats_supplier=lambda: deepcopy(state["ledger"]),
        ledger_audit_supplier=lambda: deepcopy(state["ledger_audit"]),
        raw_store_stats_supplier=lambda: deepcopy(state["raw"]),
        raw_store_audit_supplier=lambda: deepcopy(state["raw_audit"]),
        event_diagnostics_supplier=(
            (lambda: deepcopy(state["event"]))
            if config.event_capacity_claimed
            else None
        ),
        disk_usage_supplier=lambda _path: state["disk"],
        fault_injector=fault_injector,
    )


def test_healthy_full_envelope_is_read_only_and_restart_recomputed(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    state = _state()
    guard = _guard(config, state)

    first = guard.inspect(full_audit=True)
    assert first["contract"] == CONTRACT
    assert first["event_capacity_claimed"] is True
    assert first["admission_ready"] is True
    assert first["alarm_codes"] == []
    assert first["full_audit_completed"] is True
    assert first["inspection_count"] == 1
    assert first["live_delete_performed"] is False
    assert first["capacity"]["host_operational_free_reserve_bytes"] == (
        2 * 1024 * MIB
    )
    assert first["capacity"]["host_operational_free_headroom_bytes"] > 0
    assert first["reclamation"] == {
        "automatic_reclaim": False,
        "live_apply_authorized": False,
        "live_apply_wired": False,
        "policy": "offline_five_role_only_never_live_apply_v1",
        "source_reclaim_authorized": False,
    }
    assert first["ownership"] == {
        "exclusive_filesystem_owner_proven": False,
        "exclusive_process_owner_held": True,
        "external_writer_exclusion_proven": False,
        "path_identity_reproved": True,
        "same_filesystem_reproved": True,
    }
    before_restart = deepcopy(first["capacity"])

    restarted = _guard(config, state)
    second = restarted.inspect(full_audit=True)
    assert second["capacity"] == before_restart
    assert second["inspection_count"] == 1
    assert second["live_delete_performed"] is False


def test_healthy_fast_inspection_admits_without_reading_deep_audits(
    tmp_path: Path,
) -> None:
    """A normal restart must not hash the retained corpus before admission."""

    config = _config(tmp_path)
    state = _state()
    deep_reads = {"ledger": 0, "raw": 0}
    guard = RawCaptureRetentionGuard(
        config=config,
        ownership_assertion=lambda: None,
        ledger_stats_supplier=lambda: deepcopy(state["ledger"]),
        ledger_audit_supplier=lambda: deep_reads.__setitem__(
            "ledger", deep_reads["ledger"] + 1
        ),
        raw_store_stats_supplier=lambda: deepcopy(state["raw"]),
        raw_store_audit_supplier=lambda: deep_reads.__setitem__(
            "raw", deep_reads["raw"] + 1
        ),
        event_diagnostics_supplier=lambda: deepcopy(state["event"]),
        disk_usage_supplier=lambda _path: state["disk"],
    )

    inspected = guard.inspect(full_audit=False)

    assert inspected["admission_ready"] is True
    assert inspected["alarm_codes"] == []
    assert inspected["full_audit_completed"] is False
    assert deep_reads == {"ledger": 0, "raw": 0}


def test_disabled_event_capacity_is_cadence_only_and_supplier_is_absent(
    tmp_path: Path,
) -> None:
    config = replace(
        _config(tmp_path),
        event_capacity_claimed=False,
        configured_persistent_max_bytes=68 * MIB,
        configured_raw_object_max_entries=100,
    )
    state = _state()
    guard = _guard(config, state)

    diagnostics = guard.inspect(full_audit=True)

    assert diagnostics["admission_ready"] is True
    assert diagnostics["event_capacity_claimed"] is False
    assert diagnostics["capacity"]["configured_persistent_max_bytes"] == (
        config.cadence_raw_max_bytes
        + config.cadence_raw_catalog_max_bytes
        + config.cadence_ledger_catalog_max_bytes
    )
    assert diagnostics["capacity"]["configured_raw_object_max_entries"] == (
        config.cadence_raw_max_entries
    )
    assert diagnostics["capacity"]["event_control_headroom"] == 0
    assert diagnostics["capacity"]["event_payload_byte_headroom"] == 0
    assert diagnostics["capacity"]["event_payload_entry_headroom"] == 0


def test_ownership_is_proved_before_any_store_supplier(tmp_path: Path) -> None:
    calls: list[str] = []

    def not_held() -> None:
        calls.append("owner")
        raise RuntimeError("private-owner-detail")

    guard = _guard(_config(tmp_path), _state(), ownership_assertion=not_held)
    with pytest.raises(
        RawCaptureRetentionGuardBlocked,
        match="exclusive_process_owner_not_held",
    ):
        guard.inspect(full_audit=True)
    assert calls == ["owner"]
    diagnostics = guard.diagnostics(ownership_held=False)
    assert diagnostics["admission_ready"] is False
    assert diagnostics["reason_code"] == "exclusive_process_owner_not_held"
    assert diagnostics["live_delete_performed"] is False


def test_ownership_is_reproved_after_the_complete_read_window(
    tmp_path: Path,
) -> None:
    calls = 0

    def lost_during_read_window() -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("private-owner-reproof-detail")

    guard = _guard(
        _config(tmp_path),
        _state(),
        ownership_assertion=lost_during_read_window,
    )
    with pytest.raises(
        RawCaptureRetentionGuardBlocked,
        match="exclusive_process_owner_not_held",
    ) as captured:
        guard.inspect(full_audit=True)
    assert "private-owner-reproof-detail" not in str(captured.value)
    assert calls == 2
    assert guard.diagnostics(ownership_held=False)["full_audit_completed"] is False


def test_fast_admission_blocks_on_concrete_integrity_failure_and_recovers(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    state = _state()
    guard = _guard(_config(tmp_path), state)
    first = guard.inspect(full_audit=False)
    assert first["admission_ready"] is True
    assert first["full_audit_completed"] is False

    state["raw"]["retention_policy"] = "unexpected"
    with pytest.raises(
        RawCaptureRetentionGuardBlocked,
        match="storage_integrity_failed",
    ):
        guard.inspect(full_audit=False)
    assert "checks=retention_policy" in caplog.text
    assert "phase=raw_store_validation" in caplog.text
    state["raw"]["retention_policy"] = "none"
    recovered = guard.inspect(full_audit=False)
    assert recovered["admission_ready"] is True
    assert recovered["full_audit_completed"] is False


def test_physical_envelope_and_logical_cycle_exhaustion_block_without_delete(
    tmp_path: Path,
) -> None:
    state = _state()
    state["disk"] = SimpleNamespace(total=64 * MIB, used=63 * MIB, free=1 * MIB)
    guard = _guard(_config(tmp_path), state)
    with pytest.raises(
        RawCaptureRetentionGuardBlocked,
        match="filesystem_envelope_capacity_exhausted",
    ):
        guard.inspect(full_audit=True)
    physical = guard.diagnostics(ownership_held=True)
    assert physical["alarm_codes"] == [
        "filesystem_envelope_capacity_exhausted"
    ]
    assert physical["live_delete_performed"] is False

    state = _state()
    state["ledger"]["reserved_bytes"] = 32 * MIB
    state["ledger"]["reserved_count"] = 1
    state["ledger"]["reserved_entries"] = 100
    state["ledger"]["reservation_rows"] = 1
    state["ledger_audit"].update({
        "reservation_rows": 1,
        "reserved_count": 1,
        "reserved_bytes": 32 * MIB,
        "reserved_entries": 100,
    })
    logical = _guard(_config(tmp_path / "logical"), state)
    with pytest.raises(RawCaptureRetentionGuardBlocked):
        logical.inspect(full_audit=True)
    diagnostics = logical.diagnostics(ownership_held=True)
    assert "cadence_cycle_byte_capacity_exhausted" in diagnostics["alarm_codes"]
    assert "cadence_cycle_entry_capacity_exhausted" in diagnostics["alarm_codes"]
    assert diagnostics["live_delete_performed"] is False


def test_catalog_cycle_reserve_covers_complete_current_worst_case_batch() -> None:
    raw_reserve = raw_catalog_cycle_reserve_bytes(
        artifact_entries=85,
        captures=27,
        page_size_bytes=4096,
    )
    ledger_reserve = capacity_ledger_catalog_cycle_reserve_bytes(
        reservations=27,
        page_size_bytes=4096,
    )

    assert raw_reserve > 1 * MIB
    assert raw_reserve >= 85 * 64 * 1024
    assert ledger_reserve > 27 * 4096


@pytest.mark.parametrize(
    ("scope", "expected_reason"),
    (
        ("raw", "cadence_catalog_capacity_exhausted"),
        ("ledger", "cadence_ledger_catalog_capacity_exhausted"),
    ),
)
def test_effective_sqlite_main_headroom_blocks_before_next_cycle(
    tmp_path: Path,
    scope: str,
    expected_reason: str,
) -> None:
    config = _config(tmp_path)
    state = _state()
    if scope == "raw":
        reserve = raw_catalog_cycle_reserve_bytes(
            artifact_entries=config.cadence_cycle_reserve_entries,
            captures=config.cadence_cycle_reserve_captures,
            page_size_bytes=state["raw"]["catalog_page_size_bytes"],
        )
        catalog = state["raw"]
        physical_field = "catalog_database_bytes"
    else:
        reserve = capacity_ledger_catalog_cycle_reserve_bytes(
            reservations=config.cadence_cycle_reserve_captures,
            page_size_bytes=state["ledger"]["catalog_page_size_bytes"],
        )
        catalog = state["ledger"]
        physical_field = "catalog_file_bytes"

    catalog["catalog_main_database_bytes"] = (
        catalog["catalog_main_database_max_bytes"] - reserve + 1
    )
    catalog[physical_field] = catalog["catalog_main_database_bytes"]
    blocked = _guard(config, state)
    with pytest.raises(RawCaptureRetentionGuardBlocked, match=expected_reason):
        blocked.inspect(full_audit=True)
    assert blocked.diagnostics(ownership_held=True)["reason_code"] == (
        expected_reason
    )

    catalog["catalog_main_database_bytes"] -= 1
    catalog[physical_field] = catalog["catalog_main_database_bytes"]
    admitted = _guard(config, state).inspect(full_audit=True)
    assert admitted["admission_ready"] is True
    assert expected_reason.replace("exhausted", "low") in admitted[
        "alarm_codes"
    ]


def test_forged_sqlite_main_envelope_fails_closed(tmp_path: Path) -> None:
    config = _config(tmp_path)
    state = _state()
    state["raw"]["catalog_main_database_max_bytes"] += 4096

    guard = _guard(config, state)
    with pytest.raises(
        RawCaptureRetentionGuardBlocked,
        match="storage_integrity_failed",
    ):
        guard.inspect(full_audit=True)


def test_host_reserve_remains_protected_after_raw_envelope_is_filled(
    tmp_path: Path,
) -> None:
    state = _state()
    state["raw"].update({
        "catalog_bytes": 32 * MIB,
        "catalog_database_bytes": 16 * MIB,
        "filesystem_bytes": 32 * MIB,
    })
    state["ledger"]["catalog_file_bytes"] = 16 * MIB
    state["event"]["capacity"].update({
        "retained_payload_byte_headroom": 0,
        "retained_payload_entry_headroom": 0,
    })
    state["event"].update({
        "retained_payload_bytes": 16 * MIB,
        "retained_payload_entries": 100,
    })
    state["disk"] = SimpleNamespace(
        total=4 * 1024 * MIB,
        used=(2 * 1024 - 45) * MIB,
        free=(2 * 1024 + 45) * MIB,
    )
    config = _config(tmp_path)
    config.event_control_root.mkdir(parents=True)
    config.event_raw_root.mkdir(parents=True)
    guard = _guard(config, state)
    with pytest.raises(
        RawCaptureRetentionGuardBlocked,
        match="filesystem_envelope_capacity_exhausted",
    ):
        guard.inspect(full_audit=True)
    capacity = guard.diagnostics(ownership_held=True)["capacity"]
    assert capacity["persistent_envelope_free_required_bytes"] == 48 * MIB
    assert capacity["host_operational_free_reserve_bytes"] == 2 * 1024 * MIB
    assert capacity["host_operational_free_headroom_bytes"] == 0


@pytest.mark.parametrize(
    ("counter", "audit_counter", "expected_reason"),
    (
        (
            "receipt_count",
            "checked_receipts",
            "cadence_cycle_entry_capacity_exhausted",
        ),
        (
            "capture_count",
            "checked_captures",
            "cadence_capture_capacity_exhausted",
        ),
    ),
)
def test_receipt_and_capture_table_capacity_are_cycle_bounded(
    tmp_path: Path,
    counter: str,
    audit_counter: str,
    expected_reason: str,
) -> None:
    state = _state()
    state["raw"][counter] = 99
    state["raw_audit"][audit_counter] = 99
    guard = _guard(_config(tmp_path), state)
    with pytest.raises(RawCaptureRetentionGuardBlocked, match=expected_reason):
        guard.inspect(full_audit=True)
    assert guard.diagnostics(ownership_held=True)["live_delete_performed"] is False


def test_full_raw_audit_counts_are_bound_to_fast_inventory(tmp_path: Path) -> None:
    state = _state()
    state["raw_audit"]["checked_receipts"] = 0
    guard = _guard(_config(tmp_path), state)
    with pytest.raises(
        RawCaptureRetentionGuardBlocked,
        match="storage_integrity_failed",
    ):
        guard.inspect(full_audit=True)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("filesystem_staging_entries", 1),
        ("unmanaged_file_count", 1),
    ),
)
def test_unmanaged_or_crash_staging_residue_blocks_for_offline_review(
    tmp_path: Path,
    field: str,
    value: int,
) -> None:
    state = _state()
    state["raw"][field] = value
    guard = _guard(_config(tmp_path), state)
    with pytest.raises(
        RawCaptureRetentionGuardBlocked,
        match="storage_integrity_failed",
    ):
        guard.inspect(full_audit=False)
    diagnostics = guard.diagnostics(ownership_held=True)
    assert diagnostics["unmanaged_data_action"] == (
        "alarm_and_block_never_delete"
    )


def test_missing_final_nodes_and_active_event_paths_fail_closed(
    tmp_path: Path,
) -> None:
    missing_cadence = _guard(
        _config(tmp_path / "missing", create_cadence=False),
        _state(),
    )
    with pytest.raises(
        RawCaptureRetentionGuardBlocked,
        match="path_identity_reproof_failed",
    ):
        missing_cadence.inspect()

    config = _config(tmp_path / "active-event")
    state = _state()
    state["event"]["access_retention_authority"] = "available"
    state["event"]["network_started"] = True
    state["event"]["state"] = "connecting"
    active_event = _guard(config, state)
    with pytest.raises(
        RawCaptureRetentionGuardBlocked,
        match="path_identity_reproof_failed",
    ):
        active_event.inspect()


def test_config_rejects_noncanonical_and_overlapping_paths(tmp_path: Path) -> None:
    config = _config(tmp_path)
    with pytest.raises(ValueError, match="retention_guard_path_not_canonical"):
        replace(
            config,
            event_raw_root=(config.event_raw_root / ".." / "event-other"),
        )
    with pytest.raises(ValueError, match="retention_guard_path_overlap"):
        replace(
            config,
            event_raw_root=config.cadence_raw_root / "nested-event",
        )
    with pytest.raises(ValueError, match="retention_guard_path_overlap"):
        replace(
            config,
            event_raw_root=config.cadence_ledger_path / "nested-event",
        )
    retained_claim = replace(
        config,
        event_capacity_claimed=False,
        configured_persistent_max_bytes=64 * MIB,
        configured_raw_object_max_entries=100,
    )
    assert retained_claim.configured_persistent_max_bytes < (
        retained_claim.cadence_raw_max_bytes
        + retained_claim.cadence_raw_catalog_max_bytes
        + retained_claim.cadence_ledger_catalog_max_bytes
    )
    with pytest.raises(ValueError, match="retention_guard_envelope_invalid"):
        replace(config, configured_persistent_max_bytes=36 * MIB)
    with pytest.raises(
        ValueError,
        match="host_operational_free_reserve_bytes_invalid",
    ):
        replace(config, host_operational_free_reserve_bytes=2 * 1024 * MIB - 1)


def test_event_capacity_is_bound_to_exact_configured_maxima(tmp_path: Path) -> None:
    state = _state()
    state["event"]["capacity"].update({
        "queue_capacity": 51,
        "queue_headroom": 51,
    })
    guard = _guard(_config(tmp_path), state)
    with pytest.raises(
        RawCaptureRetentionGuardBlocked,
        match="storage_integrity_failed",
    ):
        guard.inspect()


def test_path_identity_swap_during_inspection_is_detected(tmp_path: Path) -> None:
    config = _config(tmp_path)
    state = _state()
    original = config.cadence_raw_root
    displaced = (tmp_path / "cadence-raw-displaced").resolve()
    swapped = False

    def raw_stats():
        nonlocal swapped
        if not swapped:
            original.rename(displaced)
            original.mkdir()
            swapped = True
        return deepcopy(state["raw"])

    guard = RawCaptureRetentionGuard(
        config=config,
        ownership_assertion=lambda: None,
        ledger_stats_supplier=lambda: deepcopy(state["ledger"]),
        ledger_audit_supplier=lambda: deepcopy(state["ledger_audit"]),
        raw_store_stats_supplier=raw_stats,
        raw_store_audit_supplier=lambda: deepcopy(state["raw_audit"]),
        event_diagnostics_supplier=lambda: deepcopy(state["event"]),
        disk_usage_supplier=lambda _path: state["disk"],
    )
    with pytest.raises(
        RawCaptureRetentionGuardBlocked,
        match="path_identity_reproof_failed",
    ):
        guard.inspect()


@pytest.mark.parametrize(
    "checkpoint",
    (
        "after_ownership",
        "after_ledger",
        "after_raw_store",
        "after_capacity_probe",
        "after_identity_reproof",
        "after_ownership_reproof",
    ),
)
def test_failure_at_every_read_boundary_is_bounded_and_never_deletes(
    tmp_path: Path,
    checkpoint: str,
) -> None:
    def fail(value: str) -> None:
        if value == checkpoint:
            raise RuntimeError("private-fault-detail")

    guard = _guard(_config(tmp_path), _state(), fault_injector=fail)
    with pytest.raises(
        RawCaptureRetentionGuardBlocked,
        match="storage_integrity_failed",
    ) as captured:
        guard.inspect(full_audit=True)
    assert "private-fault-detail" not in str(captured.value)
    assert guard.diagnostics(ownership_held=True)["live_delete_performed"] is False


def test_os_exit_at_every_boundary_restarts_from_unchanged_store(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    before = tuple(
        (
            path.relative_to(tmp_path).as_posix(),
            "directory" if path.is_dir() else "file",
            b"" if path.is_dir() else path.read_bytes(),
        )
        for path in sorted(tmp_path.rglob("*"))
    )
    repository = Path(__file__).resolve().parents[1]
    helper = (
        repository
        / "tests"
        / "helpers"
        / "worker_raw_capture_retention_guard_restart_child.py"
    )
    for checkpoint in (
        "after_ownership",
        "after_ledger",
        "after_raw_store",
        "after_capacity_probe",
        "after_identity_reproof",
        "after_ownership_reproof",
    ):
        crashed = subprocess.run(
            [sys.executable, "-B", str(helper), str(tmp_path), checkpoint],
            cwd=repository,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=30,
        )
        assert crashed.returncode == 73
        assert crashed.stdout == b""
        assert crashed.stderr == b""
        after = tuple(
            (
                path.relative_to(tmp_path).as_posix(),
                "directory" if path.is_dir() else "file",
                b"" if path.is_dir() else path.read_bytes(),
            )
            for path in sorted(tmp_path.rglob("*"))
        )
        assert after == before

    restarted = _guard(config, _state()).inspect(full_audit=True)
    assert restarted["admission_ready"] is True
    assert restarted["inspection_count"] == 1
    assert restarted["live_delete_performed"] is False


def test_guard_source_has_no_delete_or_reclamation_apply_capability() -> None:
    repository = Path(__file__).resolve().parents[1]
    path = repository / "storm_tracker_worker" / "raw_capture_retention_guard.py"
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    forbidden_calls = {"unlink", "rmdir", "remove", "removedirs", "replace"}
    called_attributes = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    imported = {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert called_attributes.isdisjoint(forbidden_calls)
    assert not any("reclamation" in name for name in imported)
    assert '"live_delete_performed": False' in source
    assert '"live_apply_wired": False' in source
