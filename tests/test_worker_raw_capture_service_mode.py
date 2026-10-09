from __future__ import annotations

import ast
import asyncio
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from storm_tracker_worker import service
from storm_tracker_worker.autonomous_scheduler import (
    SchedulerDesiredStateError,
)
from storm_tracker_worker.acquisition_runtime import ShadowTickResult
from storm_tracker_worker.acquisition_runtime import (
    RAW_CAPTURE_CHILD_ENVIRONMENT_ALLOWLIST,
)
from storm_tracker_worker.acquisition_activation import NullSecretResolver
from storm_tracker_worker.blitzortung_event_ingress import (
    ACCESS_RETENTION_AUTHORITY_ID,
    BlitzortungEventIngressLimits,
)
from storm_tracker_worker.blitzortung_mqtt_raw_sidecar import (
    BlitzortungMqttRawSidecar,
)
from storm_tracker_worker.desired_state import DesiredStateConflict
from storm_tracker_worker.raw_artifacts import RawArtifactLimits
from storm_tracker_worker.raw_capture_service_composition import (
    BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES,
    BLITZORTUNG_CONTROL_STAGING_RECOVERY_EXTRA_ENTRIES,
    CONFIG_CONTRACT,
    RAW_CAPTURE_STARTUP_TIMEOUT_SECONDS,
    RUNTIME_CONTRACT,
    _ParentBrokerSecretResolver,
    RawCaptureServiceIntegrityError,
    RawCaptureServiceRuntime,
)
from storm_tracker_worker.raw_capture_retention_guard import (
    RawCaptureRetentionGuardBlocked,
)


ALLOWED = (
    "ground.metar.global",
    "radar.dwd.radolan_rv",
)


def _event_ingress_diagnostics(*, enabled: bool = True) -> dict:
    if not enabled:
        return {
            "blitzortung": {
                "capacity_claimed": False,
                "cadence_activation_route": False,
                "contract": "stv3-worker-blitzortung-raw-shadow-status-v2",
                "desired_state": {
                    "lightning_source_mode": None,
                    "region_count": 0,
                    "state_sha256": None,
                    "status": "disabled_by_config",
                },
                "enabled": False,
                "operational_admission": False,
                "raw_store_commit_is_terminal": True,
                "readiness": {
                    "alarm_codes": [],
                    "ready": True,
                    "reason_code": "disabled_by_config",
                    "required_for_service_admission": False,
                    "status": "intentionally_inert",
                },
                "sidecar": None,
            }
        }
    root = Path.cwd().resolve()
    sidecar = BlitzortungMqttRawSidecar(
        control_root=root / ".unused-blitzortung-control",
        raw_store_root=root / ".unused-blitzortung-raw",
        raw_store_limits=RawArtifactLimits(),
        ingress_limits=BlitzortungEventIngressLimits(),
        access_retention_authority=ACCESS_RETENTION_AUTHORITY_ID,
    )
    return {
        "blitzortung": {
            "capacity_claimed": True,
            "cadence_activation_route": False,
            "contract": "stv3-worker-blitzortung-raw-shadow-status-v2",
            "desired_state": {
                "lightning_source_mode": None,
                "region_count": 0,
                "state_sha256": None,
                "status": "unconfigured",
            },
            "enabled": True,
            "operational_admission": False,
            "raw_store_commit_is_terminal": True,
            "readiness": {
                "alarm_codes": [],
                "ready": False,
                "reason_code": "desired_state_unconfigured",
                "required_for_service_admission": False,
                "status": "waiting",
            },
            "sidecar": sidecar.diagnostics(),
        }
    }


def _storage_capacity_diagnostics(*, enabled: bool = True) -> dict:
    cadence = RawArtifactLimits()
    event = RawArtifactLimits()
    ingress = BlitzortungEventIngressLimits()
    receipt_max_bytes = BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES
    control_bytes = ingress.max_control_receipts * receipt_max_bytes
    staging_bytes = (
        ingress.control_receipt_reserve
        + BLITZORTUNG_CONTROL_STAGING_RECOVERY_EXTRA_ENTRIES
    ) * receipt_max_bytes
    event_bytes = (
        event.max_total_bytes
        + event.max_catalog_bytes
        + control_bytes
        + staging_bytes
        if enabled
        else 0
    )
    event_entries = event.max_object_entries if enabled else 0
    host_reserve = 2 * 1024 * 1024 * 1024
    return {
        "accounting_contract": "config_bound_optional_event_capacity_v2",
        "cadence": {
            "catalog_max_bytes": cadence.max_catalog_bytes,
            "ledger_accounted": True,
            "object_max_bytes": cadence.max_total_bytes,
            "object_max_entries": cadence.max_object_entries,
        },
        "combined_persistent_volume": {
            "configured_max_bytes": (
                cadence.max_total_bytes
                + cadence.max_catalog_bytes
                + event_bytes
            ),
            "raw_object_max_entries": (
                cadence.max_object_entries + event_entries
            ),
            "same_filesystem_capacity_claim": True,
        },
        "event": {
            "capacity_claimed": enabled,
            "catalog_max_bytes": event.max_catalog_bytes if enabled else 0,
            "control_receipt_max_bytes": receipt_max_bytes if enabled else 0,
            "control_receipt_storage_max_bytes": control_bytes if enabled else 0,
            "control_staging_residue_entry_capacity": (
                (
                    ingress.control_receipt_reserve
                    + BLITZORTUNG_CONTROL_STAGING_RECOVERY_EXTRA_ENTRIES
                ) if enabled else 0
            ),
            "control_staging_residue_max_bytes": staging_bytes if enabled else 0,
            "enabled": enabled,
            "ledger_accounted": False,
            "object_max_bytes": event.max_total_bytes if enabled else 0,
            "object_max_entries": event.max_object_entries if enabled else 0,
        },
        "host_operational_free_reserve_bytes": host_reserve,
    }


def _retention_guard_diagnostics(
    *,
    event_ingress: dict,
    storage_capacity: dict,
) -> dict:
    cadence = storage_capacity["cadence"]
    combined = storage_capacity["combined_persistent_volume"]
    event_claimed = event_ingress["blitzortung"]["enabled"]
    sidecar = event_ingress["blitzortung"]["sidecar"]
    event_capacity = (
        sidecar["capacity"]
        if event_claimed
        else {
            "control_intake_headroom": 0,
            "retained_payload_byte_headroom": 0,
            "retained_payload_entry_headroom": 0,
        }
    )
    ledger_catalog_max = 256 * 1024 * 1024
    configured_max = combined["configured_max_bytes"] + ledger_catalog_max
    filesystem_free = configured_max + 3 * 1024 * 1024 * 1024
    host_reserve = storage_capacity["host_operational_free_reserve_bytes"]
    cycle_bytes = 1024 * 1024
    cycle_entries = 2
    cycle_captures = 2
    raw_table_max_entries = cadence["object_max_entries"] * 4
    return {
        "admission_ready": True,
        "alarm_codes": [],
        "capacity": {
            "cadence_catalog_headroom_bytes": cadence["catalog_max_bytes"],
            "cadence_capture_headroom": raw_table_max_entries,
            "cadence_capture_max_entries": raw_table_max_entries,
            "cadence_cycle_reserve_bytes": cycle_bytes,
            "cadence_cycle_reserve_captures": cycle_captures,
            "cadence_cycle_reserve_entries": cycle_entries,
            "cadence_ledger_byte_headroom": cadence["object_max_bytes"],
            "cadence_ledger_catalog_headroom_bytes": ledger_catalog_max,
            "cadence_ledger_catalog_max_bytes": ledger_catalog_max,
            "cadence_ledger_entry_headroom": cadence["object_max_entries"],
            "cadence_ledger_max_bytes": cadence["object_max_bytes"],
            "cadence_ledger_max_entries": cadence["object_max_entries"],
            "cadence_ledger_max_reservations": (
                cadence["object_max_entries"] * 4
            ),
            "cadence_raw_byte_headroom": cadence["object_max_bytes"],
            "cadence_raw_catalog_max_bytes": cadence["catalog_max_bytes"],
            "cadence_raw_entry_headroom": cadence["object_max_entries"],
            "cadence_raw_max_bytes": cadence["object_max_bytes"],
            "cadence_raw_max_entries": cadence["object_max_entries"],
            "cadence_receipt_headroom": raw_table_max_entries,
            "cadence_receipt_max_entries": raw_table_max_entries,
            "cadence_reservation_headroom": raw_table_max_entries,
            "configured_persistent_max_bytes": configured_max,
            "configured_raw_object_max_entries": combined[
                "raw_object_max_entries"
            ],
            "event_control_headroom": event_capacity[
                "control_intake_headroom"
            ],
            "event_payload_byte_headroom": event_capacity[
                "retained_payload_byte_headroom"
            ],
            "event_payload_entry_headroom": event_capacity[
                "retained_payload_entry_headroom"
            ],
            "filesystem_free_bytes": filesystem_free,
            "filesystem_total_bytes": filesystem_free * 2,
            "host_operational_free_headroom_bytes": (
                filesystem_free - configured_max - host_reserve
            ),
            "host_operational_free_reserve_bytes": host_reserve,
            "persistent_envelope_free_required_bytes": configured_max,
            "physical_reserve_headroom_bytes": (
                filesystem_free - configured_max - host_reserve
            ),
        },
        "capacity_policy": (
            "config_bound_optional_event_envelope_reserved_fail_closed_v2"
        ),
        "contract": "stv3-worker-raw-capture-retention-guard-v2",
        "event_capacity_claimed": event_claimed,
        "full_audit_completed": True,
        "inspection_count": 2,
        "live_delete_performed": False,
        "ownership": {
            "exclusive_filesystem_owner_proven": False,
            "exclusive_process_owner_held": True,
            "external_writer_exclusion_proven": False,
            "path_identity_reproved": True,
            "same_filesystem_reproved": True,
        },
        "reason_code": "ready",
        "reclamation": {
            "automatic_reclaim": False,
            "live_apply_authorized": False,
            "live_apply_wired": False,
            "policy": "offline_five_role_only_never_live_apply_v1",
            "source_reclaim_authorized": False,
        },
        "unmanaged_data_action": "alarm_and_block_never_delete",
    }


def test_parent_broker_resolver_marks_only_current_ephemeral_requirements() -> None:
    resolver = _ParentBrokerSecretResolver(
        NullSecretResolver(),
        netatmo_broker_configured=True,
        eumetsat_broker_configured=True,
    )

    assert resolver.resolve("worker_secret.netatmo_access_token") == (
        "worker_parent_oauth_broker_configured"
    )
    for requirement_id in (
        "worker_secret.eumetsat_consumer_key",
        "worker_secret.eumetsat_consumer_secret",
    ):
        assert resolver.resolve(requirement_id) == (
            "worker_parent_oauth_broker_configured"
        )
    assert resolver.resolve("worker_secret.eumetsat_data_access") is None


def _diagnostics(*, operation: str, enabled: bool = True) -> dict:
    start = operation == "start"
    event_ingress = _event_ingress_diagnostics(enabled=enabled)
    storage_capacity = _storage_capacity_diagnostics(enabled=enabled)
    return {
        "acquisition_configuration": {
            "collector_scope": "all_sources",
            "configured_workload_ids": list(ALLOWED),
            "policy": "closed_active_raw_matrix_all_sources_v1",
        },
        "authority_sha256": "a" * 64,
        "blocked_catalog": {
            "count": 1,
            "reason_counts": {"cadence_or_event_ingress_unwired": 1},
            "workload_ids": ["lightning.example.event_ingress"],
        },
        "capture_storage": {
            "committed_bytes": 0,
            "committed_count": 0,
            "committed_entries": 0,
            "ledger_scope": "cadence_only_event_store_excluded",
            "reserved_bytes": 0,
            "reserved_count": 0,
            "reserved_entries": 0,
        },
        "config_sha256": "b" * 64,
        "contract": "stv3-worker-raw-capture-runtime-v6",
        "control_job_types": [
            "desired_state_apply",
            "desired_state_claim",
            "desired_state_snapshot",
        ],
        "counts": {
            "control_calls": 0,
            "event_failures": 0,
            "event_stops": 0,
            "event_syncs": 1,
            "failures": 0,
            "start_calls": 1,
            "stop_calls": 0,
            "ticks": 0 if start else 1,
            "timeouts": 0,
        },
        "dashboard_output": False,
        "downstream_consumption_required": False,
        "event_ingress": event_ingress,
        "heavy_lane_active": False,
        "last_error_code": None,
        "last_tick": {
            "acquisition_job_id": None,
            "attempt_count": None,
            "contract": "stv3-worker-acquisition-shadow-runtime-v3",
            "desired_state_sha256": None,
            "reason_code": "not_started" if start else "desired_state_unconfigured",
            "receipt_sha256": None,
            "scheduler_transition": None,
            "source_capture_id": None,
            "status": "idle",
            "terminal": False,
            "workload_id": None,
        },
        "legacy_heavy_admission": "forbidden",
        "live_wiring": True,
        "meteorological_admission": False,
        "meteorological_output": False,
        "mode": "service_raw_capture_shadow",
        "ownership_fence_held": True,
        "production_enable_ready": False,
        "provider_configuration": {
            "configured_count": len(ALLOWED),
            "requirements_unavailable_count": 0,
            "requirements_unavailable_workload_ids": [],
            "workloads": [
                {
                    "handler_configured": True,
                    "last_terminal": None,
                    "requirements_available": True,
                    "requirements_available_count": 0,
                    "requirements_total": 0,
                    "workload_id": "ground.metar.global",
                },
                {
                    "handler_configured": True,
                    "last_terminal": None,
                    "requirements_available": True,
                    "requirements_available_count": 0,
                    "requirements_total": 0,
                    "workload_id": "radar.dwd.radolan_rv",
                },
            ],
        },
        "raw_payload_transport": "worker_direct_not_mqtt",
        "raw_store_commit_is_terminal": True,
        "retention": {
            "automatic_reclaim": False,
            "capacity_exhaustion": "fail_closed",
            "source_reclaim_authorized": False,
        },
        "retention_guard": _retention_guard_diagnostics(
            event_ingress=event_ingress,
            storage_capacity=storage_capacity,
        ),
        "science_mode": "disabled",
        "shutdown_complete": False,
        "state": "running",
        "storage_capacity": storage_capacity,
        "terminal_router_wired": False,
    }


def _readiness_service():
    selected = object.__new__(service.WorkerService)
    selected._worker_first_runtime_mode = "raw_capture_shadow"
    selected._worker_first_allowed_workload_ids = ALLOWED
    return selected


class _FakeLease:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.held = True

    def assert_held(self) -> None:
        if not self.held:
            raise RuntimeError("lease_not_held")

    def release(self) -> None:
        self.events.append("lease_release")
        self.held = False


class _FakeDesiredStateStore:
    def __init__(self, active=None) -> None:
        self.active = active

    def load_active(self):
        return self.active

    def apply(self, _value):
        active = self.active
        assert active is not None
        return {
            "method": "worker_desired_state_store_v1",
            "status": "activated",
            "state_sha256": active.state_sha256,
        }


class _FakeAcquisition:
    def __init__(self, desired_store: _FakeDesiredStateStore) -> None:
        self.desired_state_store = desired_store
        self.startup_recovery_calls = 0
        self.tick_calls = 0
        self.stop_calls = 0

    def startup_recover(self) -> None:
        self.startup_recovery_calls += 1

    def diagnostics(self) -> dict:
        return {
            "activation": {
                "blocked_catalog": {
                    "count": 0,
                    "reason_counts": {},
                    "workload_ids": [],
                },
                "configured_workloads": [],
                "requirements_unavailable_count": 0,
                "requirements_unavailable_workload_ids": [],
                "selected_workload_ids": list(ALLOWED),
                "selection_explicit": True,
            },
            "capacity": {
                "committed_bytes": 0,
                "committed_count": 0,
                "committed_entries": 0,
                "global_max_bytes": 1024,
                "global_max_entries": 16,
                "reserved_bytes": 0,
                "reserved_count": 0,
                "reserved_entries": 0,
            },
            "collector_scope": "all_sources",
        }

    def tick(self) -> ShadowTickResult:
        self.tick_calls += 1
        return ShadowTickResult(
            "idle",
            "desired_state_unconfigured",
            None,
            None,
            None,
            None,
            False,
        )

    def request_stop(self) -> None:
        self.stop_calls += 1


class _FakeBlitzortungSidecar:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls = []
        self.state = "new"
        self.connected = False

    def start(self, regions, *, desired_state_sha256):
        exact = tuple(regions)
        self.calls.append(("start", exact, desired_state_sha256))
        self.state = "connecting" if exact else "waiting_for_regions"
        return self.diagnostics()

    def update_regions(self, regions, *, desired_state_sha256):
        exact = tuple(regions)
        self.calls.append(("update", exact, desired_state_sha256))
        self.state = "connecting" if exact else "waiting_for_regions"
        return self.diagnostics()

    def stop(self):
        self.events.append("event_stop")
        self.state = "stopped"
        self.connected = False
        return self.diagnostics()

    def diagnostics(self):
        return {
            "access_retention_authority": "available",
            "alarm_codes": [],
            "connected": self.connected,
            "fault_latched": False,
            "state": self.state,
        }


class _FakeRetentionGuard:
    def __init__(self) -> None:
        self.inspections: list[bool] = []

    def inspect(self, *, full_audit: bool):
        self.inspections.append(full_audit)
        return {
            "admission_ready": True,
            "live_delete_performed": False,
        }

    def diagnostics(self, *, ownership_held: bool):
        return {
            "admission_ready": ownership_held and bool(self.inspections),
            "alarm_codes": [],
            "live_delete_performed": False,
        }


def _active_state(*, mode: str = "auto", radius_km: float = 20.0):
    return SimpleNamespace(
        settings=SimpleNamespace(lightning_source_mode=mode),
        state_sha256=("a" if mode == "auto" else "b") * 64,
        targets=(
            SimpleNamespace(
                available=True,
                latitude=51.05,
                longitude=4.47,
                observation_radius_km=radius_km,
            ),
            SimpleNamespace(
                available=False,
                latitude=40.71,
                longitude=-74.0,
                observation_radius_km=1000.0,
            ),
        ),
    )


def _raw_runtime(
    active=None,
    *,
    retention_guard=None,
    blitzortung_enabled: bool = True,
):
    events: list[str] = []
    desired = _FakeDesiredStateStore(active)
    acquisition = _FakeAcquisition(desired)
    sidecar = (
        _FakeBlitzortungSidecar(events)
        if blitzortung_enabled
        else None
    )
    retention_guard = retention_guard or _FakeRetentionGuard()
    runtime = RawCaptureServiceRuntime(
        acquisition=acquisition,
        ownership_lease=_FakeLease(events),
        config_sha256="c" * 64,
        authority_sha256="d" * 64,
        allowed_workload_ids=ALLOWED,
        blitzortung_enabled=blitzortung_enabled,
        blitzortung_sidecar=sidecar,
        retention_guard=retention_guard,
        storage_capacity=_storage_capacity_diagnostics(
            enabled=blitzortung_enabled
        ),
        acquisition_timeout_seconds=30.0,
        stop_timeout_seconds=1.0,
    )
    return runtime, acquisition, desired, sidecar, events


def test_retention_block_stops_event_before_raw_start_and_preserves_data(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _BlockedGuard(_FakeRetentionGuard):
        def inspect(self, *, full_audit: bool):
            self.inspections.append(full_audit)
            raise RawCaptureRetentionGuardBlocked(
                "filesystem_envelope_capacity_exhausted"
            )

    blocked = _BlockedGuard()
    runtime, acquisition, _desired, sidecar, events = _raw_runtime(
        _active_state(),
        retention_guard=blocked,
    )
    try:
        asyncio.run(runtime.async_start())
        with pytest.raises(
            RuntimeError,
            match="raw_capture_retention_guard_blocked",
        ):
            asyncio.run(runtime.async_tick())
        assert blocked.inspections == [False]
        assert acquisition.tick_calls == 0
        assert sidecar.state == "stopped"
        assert events == ["event_stop"]
        assert "RAW retention guard blocked: reason=" in caplog.text
        assert "filesystem_envelope_capacity_exhausted" in caplog.text
        assert runtime.diagnostics()["retention_guard"][
            "live_delete_performed"
        ] is False
        with pytest.raises(
            RuntimeError,
            match="raw_capture_runtime_restart_required",
        ):
            asyncio.run(runtime.async_tick())
        assert blocked.inspections == [False]
    finally:
        asyncio.run(runtime.async_stop())


def test_retention_supplier_exception_is_sanitized_and_latched() -> None:
    class _FailedGuard(_FakeRetentionGuard):
        def inspect(self, *, full_audit: bool):
            self.inspections.append(full_audit)
            raise ValueError("private-storage-path-and-detail")

    failed = _FailedGuard()
    runtime, acquisition, _desired, sidecar, events = _raw_runtime(
        retention_guard=failed
    )
    try:
        asyncio.run(runtime.async_start())
        with pytest.raises(
            RuntimeError,
            match="raw_capture_retention_guard_failed",
        ) as captured:
            asyncio.run(runtime.async_tick())
        assert "private-storage-path-and-detail" not in str(captured.value)
        assert failed.inspections == [False]
        assert acquisition.tick_calls == 0
        assert sidecar.state == "stopped"
        assert events == ["event_stop"]
    finally:
        asyncio.run(runtime.async_stop())


def test_transient_storage_integrity_failure_gets_one_full_audit_retry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _TransientGuard(_FakeRetentionGuard):
        def inspect(self, *, full_audit: bool):
            self.inspections.append(full_audit)
            if len(self.inspections) == 1:
                raise RawCaptureRetentionGuardBlocked(
                    "storage_integrity_failed"
                )
            return {
                "admission_ready": True,
                "live_delete_performed": False,
            }

    guard = _TransientGuard()
    runtime, acquisition, _desired, sidecar, _events = _raw_runtime(
        _active_state(),
        retention_guard=guard,
    )
    try:
        asyncio.run(runtime.async_start())
        asyncio.run(runtime.async_tick())
        assert guard.inspections[:2] == [False, True]
        assert guard.inspections[2:] == [False]
        assert acquisition.tick_calls == 1
        assert sidecar.state != "stopped"
        assert runtime._restart_required is False
        assert "retrying one full stable audit" in caplog.text
    finally:
        asyncio.run(runtime.async_stop())


def test_storage_integrity_retry_recovers_verified_orphans_before_full_audit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    order: list[str] = []

    class _OrphanedGuard(_FakeRetentionGuard):
        def inspect(self, *, full_audit: bool):
            order.append(f"guard:{full_audit}")
            self.inspections.append(full_audit)
            if len(self.inspections) == 2:
                raise RawCaptureRetentionGuardBlocked(
                    "storage_integrity_failed"
                )
            return {
                "admission_ready": True,
                "live_delete_performed": False,
            }

    guard = _OrphanedGuard()
    runtime, _acquisition, _desired, _sidecar, _events = _raw_runtime(
        _active_state(),
        retention_guard=guard,
    )
    runtime._raw_store_recovery = lambda: order.append("recovery") or 3
    try:
        asyncio.run(runtime.async_start())
        asyncio.run(runtime.async_tick())
        assert order == [
            "guard:False",
            "guard:False",
            "recovery",
            "guard:True",
        ]
        assert guard.inspections == [False, False, True]
        assert runtime._restart_required is False
        assert "recovered 3 verified content objects" in caplog.text
    finally:
        asyncio.run(runtime.async_stop())


def test_startup_does_not_run_recovery_hooks_without_integrity_failure() -> None:
    order: list[str] = []

    class _OrderedGuard(_FakeRetentionGuard):
        def inspect(self, *, full_audit: bool):
            order.append(f"guard:{full_audit}")
            return super().inspect(full_audit=full_audit)

    runtime, acquisition, _desired, _sidecar, _events = _raw_runtime(
        _active_state(),
        retention_guard=_OrderedGuard(),
    )
    runtime._raw_store_recovery = lambda: order.append("recovery") or 2
    try:
        asyncio.run(runtime.async_start())
        assert acquisition.startup_recovery_calls == 0
        assert order == []
        assert runtime._restart_required is False
    finally:
        asyncio.run(runtime.async_stop())


def test_healthy_startup_skips_recovery_and_deep_audit() -> None:
    """Large healthy RAW stores must not delay worker admission."""

    guard = _FakeRetentionGuard()
    runtime, acquisition, _desired, _sidecar, _events = _raw_runtime(
        _active_state(),
        retention_guard=guard,
    )
    try:
        started = asyncio.run(runtime.async_start())
        assert started["state"] == "running"
        assert acquisition.startup_recovery_calls == 0
        assert guard.inspections == []
    finally:
        asyncio.run(runtime.async_stop())


def test_startup_uses_archive_audit_timeout_not_provider_tick_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _acquisition, _desired, _sidecar, _events = _raw_runtime(
        _active_state()
    )
    calls: list[tuple[float | None, str]] = []
    original = runtime._blocking

    async def _recording_blocking(
        function,
        *,
        timeout_seconds=None,
        timeout_code="raw_capture_tick_timeout",
    ):
        calls.append((timeout_seconds, timeout_code))
        return await original(
            function,
            timeout_seconds=timeout_seconds,
            timeout_code=timeout_code,
        )

    monkeypatch.setattr(runtime, "_blocking", _recording_blocking)
    try:
        asyncio.run(runtime.async_start())
        assert calls == [
            (
                RAW_CAPTURE_STARTUP_TIMEOUT_SECONDS,
                "raw_capture_start_timeout",
            )
        ]
        assert RAW_CAPTURE_STARTUP_TIMEOUT_SECONDS > 615.0
    finally:
        asyncio.run(runtime.async_stop())


def test_raw_runtime_projects_active_regions_and_satellite_test_is_inert() -> None:
    runtime, _acquisition, desired, sidecar, events = _raw_runtime(
        _active_state()
    )
    started = asyncio.run(runtime.async_start())
    start_call = sidecar.calls[0]
    assert start_call[0] == "start"
    assert len(start_call[1]) == 1
    assert start_call[1][0].radius_km == 20.0
    assert started["event_ingress"]["blitzortung"]["operational_admission"] is False

    desired.active = _active_state(mode="satellite_test")
    runtime.handle_control_job(
        "desired_state_apply",
        {"desired_state": {"opaque": "not_returned"}},
    )
    assert sidecar.calls[-1][0] == "update"
    assert sidecar.calls[-1][1] == ()
    event = runtime.diagnostics()["event_ingress"]["blitzortung"]
    assert event["readiness"] == {
        "alarm_codes": [],
        "ready": True,
        "reason_code": "satellite_test_selected",
        "required_for_service_admission": False,
        "status": "intentionally_inert",
    }

    asyncio.run(runtime.async_stop())
    assert events.index("event_stop") < events.index("lease_release")


def test_event_disabled_runtime_is_cadence_only_and_sidecar_free() -> None:
    runtime, acquisition, desired, sidecar, events = _raw_runtime(
        _active_state(),
        blitzortung_enabled=False,
    )
    assert sidecar is None

    started = asyncio.run(runtime.async_start())
    assert started["event_ingress"] == _event_ingress_diagnostics(
        enabled=False
    )
    assert started["counts"]["event_syncs"] == 0
    assert started["counts"]["event_stops"] == 0

    desired.active = _active_state(mode="satellite_test")
    runtime.handle_control_job(
        "desired_state_apply",
        {"desired_state": {"opaque": "not_returned"}},
    )
    asyncio.run(runtime.async_tick())
    assert acquisition.tick_calls == 1
    stopped = asyncio.run(runtime.async_stop())
    assert stopped["counts"]["event_syncs"] == 0
    assert stopped["counts"]["event_stops"] == 0
    assert events == ["lease_release"]


def test_stale_desired_state_keeps_control_open_and_fresh_apply_recovers() -> None:
    stale = _active_state()
    runtime, acquisition, desired, _sidecar, events = _raw_runtime(
        stale,
        blitzortung_enabled=False,
    )
    original_tick = acquisition.tick

    def stale_tick():
        acquisition.tick_calls += 1
        raise SchedulerDesiredStateError("desired_state_stale")

    acquisition.tick = stale_tick
    try:
        asyncio.run(runtime.async_start())
        blocked = asyncio.run(runtime.async_tick())

        assert blocked["state"] == "running"
        assert blocked["last_error_code"] is None
        assert blocked["last_tick"]["status"] == "idle"
        assert blocked["last_tick"]["reason_code"] == "desired_state_stale"
        assert blocked["last_tick"]["terminal"] is False
        assert blocked["counts"]["failures"] == 0

        desired.active = _active_state(mode="ground_only")
        applied = runtime.handle_control_job(
            "desired_state_apply",
            {"desired_state": {"fresh": True}},
        )
        assert applied["status"] == "activated"
        assert applied["state_sha256"] == "b" * 64

        acquisition.tick = original_tick
        recovered = asyncio.run(runtime.async_tick())
        assert recovered["state"] == "running"
        assert recovered["last_error_code"] is None
        assert recovered["last_tick"]["reason_code"] == (
            "desired_state_unconfigured"
        )
        assert acquisition.tick_calls == 2
        assert recovered["counts"]["control_calls"] == 1
        assert recovered["counts"]["failures"] == 0
    finally:
        asyncio.run(runtime.async_stop())
    assert events == ["lease_release"]


def test_desired_state_cas_conflict_keeps_a_machine_readable_reason() -> None:
    runtime, _acquisition, desired, _sidecar, _events = _raw_runtime(
        _active_state(),
        blitzortung_enabled=False,
    )

    def conflict(_value):
        raise DesiredStateConflict(
            "desired-state bouwt niet op de actieve SHA-256 voort"
        )

    desired.apply = conflict
    try:
        asyncio.run(runtime.async_start())
        with pytest.raises(
            RawCaptureServiceIntegrityError,
            match="raw_capture_desired_state_conflict",
        ):
            runtime.handle_control_job(
                "desired_state_apply",
                {"desired_state": {"fresh": True}},
            )
        assert runtime.diagnostics()["state"] == "running"
    finally:
        asyncio.run(runtime.async_stop())


@pytest.mark.parametrize(
    "reason_code",
    (
        "desired_state_changed_during_plan",
        "desired_state_future",
    ),
)
def test_other_transient_desired_state_blocks_remain_nonfatal(
    reason_code: str,
) -> None:
    runtime, acquisition, _desired, _sidecar, _events = _raw_runtime(
        _active_state(),
        blitzortung_enabled=False,
    )

    def blocked_tick():
        raise SchedulerDesiredStateError(reason_code)

    acquisition.tick = blocked_tick
    try:
        asyncio.run(runtime.async_start())
        result = asyncio.run(runtime.async_tick())
        assert result["state"] == "running"
        assert result["last_tick"]["status"] == "idle"
        assert result["last_tick"]["reason_code"] == reason_code
        assert result["counts"]["failures"] == 0
    finally:
        asyncio.run(runtime.async_stop())


def test_inconsistent_desired_state_remains_fatal() -> None:
    runtime, acquisition, _desired, _sidecar, _events = _raw_runtime(
        _active_state(),
        blitzortung_enabled=False,
    )

    def inconsistent_tick():
        raise SchedulerDesiredStateError("desired_state_inconsistent")

    acquisition.tick = inconsistent_tick
    try:
        asyncio.run(runtime.async_start())
        with pytest.raises(
            SchedulerDesiredStateError,
            match="desired_state_inconsistent",
        ):
            asyncio.run(runtime.async_tick())
        diagnostics = runtime.diagnostics()
        assert diagnostics["state"] == "degraded"
        assert diagnostics["last_error_code"] == "raw_capture_tick_failed"
        assert diagnostics["counts"]["failures"] == 1
    finally:
        asyncio.run(runtime.async_stop())


def test_invalid_event_region_fails_closed_without_stopping_cadence() -> None:
    runtime, acquisition, _desired, sidecar, _events = _raw_runtime(
        _active_state(radius_km=1000.0)
    )
    started = asyncio.run(runtime.async_start())
    event = started["event_ingress"]["blitzortung"]
    assert event["readiness"]["status"] == "degraded"
    assert event["readiness"]["ready"] is False
    assert event["readiness"]["alarm_codes"] == [
        "event_ingress_desired_state_apply_failed"
    ]
    assert event["desired_state"] == {
        "lightning_source_mode": None,
        "region_count": 0,
        "state_sha256": None,
        "status": "unconfigured",
    }
    assert sidecar.calls == []

    ticked = asyncio.run(runtime.async_tick())
    assert acquisition.tick_calls == 1
    assert ticked["state"] == "running"
    assert ticked["meteorological_admission"] is False
    asyncio.run(runtime.async_stop())


def test_raw_capture_readiness_is_operation_specific_and_exact() -> None:
    selected = _readiness_service()
    start = _diagnostics(operation="start")
    tick = _diagnostics(operation="tick")

    assert selected._worker_first_start_is_ready(start, operation="start")
    assert not selected._worker_first_start_is_ready(start, operation="tick")
    assert selected._worker_first_start_is_ready(tick, operation="tick")
    assert not selected._worker_first_start_is_ready(tick, operation="start")

    for path, value in (
        (("contract",), "stv3-worker-raw-capture-runtime-v3"),
        (("raw_payload_transport",), "mqtt"),
        (("science_mode",), "shadow"),
        (("meteorological_admission",), True),
        (("meteorological_output",), True),
        (("dashboard_output",), True),
        (("terminal_router_wired",), True),
        (("downstream_consumption_required",), True),
        (("raw_store_commit_is_terminal",), False),
        (("retention_guard", "live_delete_performed"), True),
        (("retention_guard", "full_audit_completed"), False),
        (
            (
                "retention_guard",
                "ownership",
                "exclusive_filesystem_owner_proven",
            ),
            True,
        ),
        (
            (
                "retention_guard",
                "capacity",
                "host_operational_free_reserve_bytes",
            ),
            0,
        ),
        (
            (
                "retention_guard",
                "capacity",
                "cadence_capture_headroom",
            ),
            1,
        ),
        (
            (
                "retention_guard",
                "capacity",
                "cadence_receipt_headroom",
            ),
            1,
        ),
        (
            (
                "retention_guard",
                "capacity",
                "event_payload_byte_headroom",
            ),
            1,
        ),
        (("event_ingress", "blitzortung", "operational_admission"), True),
        (("event_ingress", "blitzortung", "sidecar", "payload_parsed"), True),
        (
            (
                "event_ingress",
                "blitzortung",
                "sidecar",
                "transport_completeness",
            ),
            "complete",
        ),
        (
            (
                "storage_capacity",
                "combined_persistent_volume",
                "same_filesystem_capacity_claim",
            ),
            False,
        ),
        (("acquisition_configuration", "collector_scope"), "dwd_only"),
        (("provider_configuration", "configured_count"), 1),
        (("capture_storage", "committed_count"), 1),
        (("blocked_catalog", "count"), 2),
        (("last_tick", "contract"), "future"),
        (("last_tick", "attempt_count"), True),
        (("last_tick", "source_capture_id"), {"raw": "forbidden"}),
        (("last_tick", "terminal"), "false"),
    ):
        drifted = deepcopy(tick)
        target = drifted
        for part in path[:-1]:
            target = target[part]
        target[path[-1]] = value
        assert not selected._worker_first_start_is_ready(
            drifted,
            operation="tick",
        )
    historical = deepcopy(tick)
    historical["provider_configuration"]["workloads"][0][
        "last_terminal"
    ] = {
        "reason_code": "completed",
        "status": "completed_shadow_raw",
    }
    assert selected._worker_first_start_is_ready(
        historical,
        operation="tick",
    )
    assert not selected._worker_first_start_is_ready(
        tick,
        operation="unknown",
    )


def test_raw_capture_truth_contract_keeps_downstream_code_inert() -> None:
    assert CONFIG_CONTRACT == "stv3-worker-raw-capture-service-config-v6"
    assert RUNTIME_CONTRACT == "stv3-worker-raw-capture-runtime-v6"

    repository = Path(__file__).resolve().parents[1]
    forbidden_modules = {
        "completion_route_adapters",
        "serial_completion_dispatcher",
        "terminal_raw_completion_router",
    }
    forbidden_symbols = {
        "SerialCompletionDispatcher",
        "TerminalRawCompletionRouter",
    }
    for relative in (
        "storm_tracker_worker/raw_capture_service_composition.py",
        "storm_tracker_worker/service.py",
    ):
        source = (repository / relative).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_modules = {
            node.module.rsplit(".", 1)[-1]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        }
        imported_modules.update(
            alias.name.rsplit(".", 1)[-1]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        )
        called_symbols = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        assert imported_modules.isdisjoint(forbidden_modules)
        assert called_symbols.isdisjoint(forbidden_symbols)


def test_raw_control_results_are_small_scalar_operation_contracts() -> None:
    claim = {
        "method": "worker_desired_state_store_v1",
        "status": "claimed",
        "producer_id": "ha",
        "authority_epoch": 1,
        "claim_id": "claim-1",
        "claimed_at": 1.0,
    }
    assert service._raw_capture_control_result(
        "desired_state_claim", claim
    ) == claim
    snapshot = {
        "method": "worker_desired_state_store_v1",
        "status": "unconfigured",
    }
    assert service._raw_capture_control_result(
        "desired_state_snapshot", snapshot
    ) == snapshot
    applied = {
        "method": "worker_desired_state_store_v1",
        "status": "activated",
        "state_sha256": "a" * 64,
        "producer_id": "ha",
        "authority_epoch": 1,
        "generation": 0,
        "issued_at": 1.0,
        "first_received_at": 1.0,
        "last_received_at": 1.0,
        "receipt_count": 1,
        "target_count": 2,
    }
    assert service._raw_capture_control_result(
        "desired_state_apply", applied
    ) == applied

    with pytest.raises(ValueError, match="raw_capture_control_result_invalid"):
        service._raw_capture_control_result(
            "desired_state_snapshot",
            {**snapshot, "raw_bytes": b"forbidden"},
        )
    with pytest.raises(ValueError, match="raw_capture_control_result_invalid"):
        service._raw_capture_control_result("ping", {})
    assert service._raw_capture_ping_result({
        "echo": {},
        "worker_time": 1.0,
    }) == {"echo": {}, "worker_time": 1.0}
    with pytest.raises(ValueError, match="raw_capture_ping_result_invalid"):
        service._raw_capture_ping_result({
            "echo": {"provider_raw": "forbidden"},
            "worker_time": 1.0,
        })


def test_raw_capture_child_executor_uses_the_closed_environment_allowlist() -> None:
    executor = service._raw_capture_executor_factory(1.0)
    assert executor._environment_keys == tuple(sorted(
        RAW_CAPTURE_CHILD_ENVIRONMENT_ALLOWLIST
    ))
    assert "STV3_AEMET_API_KEY" in executor._environment_keys
    assert "STV3_EUMETSAT_EPHEMERAL_ACCESS_TOKEN" in (
        executor._environment_keys
    )
    assert "STV3_EUMETSAT_CONSUMER_KEY" not in executor._environment_keys
    assert "STV3_EUMETSAT_CONSUMER_SECRET" not in executor._environment_keys
    assert "STV3_MQTT_PASSWORD" not in executor._environment_keys
    assert "STV3_JOB_RECEIPT_DB" not in executor._environment_keys
