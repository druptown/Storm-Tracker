from __future__ import annotations

import ast
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import inspect
import json
import os
from pathlib import Path
import threading
from types import SimpleNamespace

import pytest

from storm_tracker_worker import worker_first_service_composition as composition
from storm_tracker_worker.raw_artifacts import RawArtifactLimits
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityProfile,
)
from test_worker_first_composition import _config as _worker_first_config


MODULE = (
    Path(__file__).resolve().parents[1]
    / "storm_tracker_worker"
    / "worker_first_service_composition.py"
)
DWD_WORKLOAD_ID = "radar.dwd.radolan_rv"


def test_canary_defaults_to_the_closed_child_environment_executor() -> None:
    parameter = inspect.signature(
        composition.build_worker_first_service_canary
    ).parameters["executor_factory"]

    assert parameter.default is composition._dwd_canary_executor_factory
    executor = parameter.default(1.0)
    assert executor._environment_keys == tuple(sorted(
        composition.CANARY_CHILD_ENVIRONMENT_ALLOWLIST
    ))
    assert not any(
        key.startswith("STV3_") for key in executor._environment_keys
    )


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _source_paths(root: Path) -> composition.WorkerFirstServiceSourcePaths:
    root = root.resolve()
    return composition.WorkerFirstServiceSourcePaths(
        scheduler_db=root / "scheduler.sqlite3",
        capacity_ledger_db=root / "capacity.sqlite3",
        execution_receipts_db=root / "executions.sqlite3",
        raw_store_root=root / "raw",
    )


def _capacity_profile() -> RawCapacityProfile:
    return RawCapacityProfile(
        global_max_bytes=64 * 1024 * 1024,
        global_max_entries=64,
        providers=(ProviderCapacityLimit(
            provider="dwd",
            max_bytes=64 * 1024 * 1024,
            max_entries=64,
        ),),
        max_reservations=64,
        max_catalog_bytes=4 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )


def _raw_limits() -> RawArtifactLimits:
    return RawArtifactLimits(
        max_object_bytes=8 * 1024 * 1024,
        max_total_bytes=64 * 1024 * 1024,
        max_object_entries=64,
        max_staging_entries=8,
        max_receipt_entries=64,
        max_capture_entries=64,
        max_capture_members=1,
        max_catalog_bytes=4 * 1024 * 1024,
    )


def _v1_config(root: Path) -> composition.WorkerFirstServiceCompositionConfig:
    root = root.resolve()
    return composition.WorkerFirstServiceCompositionConfig(
        worker_first=_worker_first_config(root / "pipeline"),
        source_paths=_source_paths(root / "source"),
        capacity_profile=_capacity_profile(),
        raw_store_limits=_raw_limits(),
    )


def _canary_config(
    root: Path,
) -> composition.WorkerFirstServiceCanaryConfig:
    v1 = _v1_config(root)
    return composition.WorkerFirstServiceCanaryConfig(
        worker_first=v1.worker_first,
        source_paths=v1.source_paths,
        capacity_profile=v1.capacity_profile,
        raw_store_limits=v1.raw_store_limits,
    )


def _coordinator_diagnostics(
    *,
    state: str = "running",
    started: bool = True,
    closed: bool = False,
    owns_heavy_lane: bool = True,
    fence_held: bool = True,
    last_error_code: str | None = None,
) -> dict[str, object]:
    return {
        "closed": closed,
        "contract": composition.WORKER_FIRST_COORDINATOR_CONTRACT,
        "counts": {
            name: 0 for name in composition._COORDINATOR_COUNT_FIELDS
        },
        "in_flight": False,
        "last_disposition": None,
        "last_error_code": last_error_code,
        "liveness": {
            "control_timeout_seconds": 5.0,
            "stop_timeout_seconds": 2.0,
        },
        "ownership": {
            "authority_scope_sha256": "b" * 64,
            "authority_sha256": "a" * 64,
            "detached_mutable_work_count": 0,
            "detached_snapshot_task_count": 0,
            "fence_held": fence_held,
            "mutable_work_count": 0,
            "runtime_flow_count": 0,
            "snapshot_operations": 0,
            "snapshot_quiesced": True,
            "snapshot_task_count": 0,
            "store_binding_sha256": "c" * 64,
            "store_count": 6,
        },
        "owns_heavy_lane": owns_heavy_lane,
        "snapshot": {
            "last_error_code": None,
            "state": "running",
        },
        "started": started,
        "state": state,
    }


class _CoordinatorHarness:
    def __init__(
        self,
        *,
        start: dict[str, object] | None = None,
        tick: dict[str, object] | None = None,
        stop: dict[str, object] | None = None,
    ) -> None:
        self.current = _coordinator_diagnostics()
        self.start_result = start or dict(self.current)
        self.tick_result = tick or dict(self.current)
        self.stop_result = stop or {
            "closed": True,
            "ownership": {"fence_held": False},
            "state": "stopped",
        }
        self.stop_calls = 0

    async def async_start(self) -> dict[str, object]:
        self.current = self.start_result
        return self.start_result

    async def async_tick(self) -> dict[str, object]:
        self.current = self.tick_result
        return self.tick_result

    async def async_stop(self) -> dict[str, object]:
        self.stop_calls += 1
        return self.stop_result

    def diagnostics(self) -> dict[str, object]:
        return self.current


def _canary_runtime_harness(
    coordinator: _CoordinatorHarness,
    *,
    started: bool,
) -> tuple[
    composition.WorkerFirstServiceCanaryRuntime,
    SimpleNamespace,
    SimpleNamespace,
]:
    acquisition = SimpleNamespace(
        diagnostics=lambda: {
            "capacity": {
                "committed_bytes": 0,
                "committed_entries": 0,
                "global_max_bytes": 1,
                "global_max_entries": 1,
            },
        },
        request_stop=lambda: None,
        tick_calls=0,
    )

    def acquisition_tick():
        acquisition.tick_calls += 1
        return composition.ShadowTickResult(
            "idle", "no_job", None, None, None, None, False
        )

    acquisition.tick = acquisition_tick
    bridge = SimpleNamespace(
        diagnostics=lambda: {"source_reclaim_authorized": False},
        publish_calls=0,
    )

    def publish_next():
        bridge.publish_calls += 1
        return composition.DwdCompletionBridgeResult(
            "idle", "no_completion", 0
        )

    bridge.publish_next = publish_next
    runtime = object.__new__(composition.WorkerFirstServiceCanaryRuntime)
    runtime._acquisition = acquisition
    runtime._bridge = bridge
    runtime._coordinator = coordinator
    runtime._config_sha256 = "d" * 64
    runtime._authority_sha256 = "a" * 64
    runtime._acquisition_timeout = 120.0
    runtime._bridge_timeout = 1.0
    runtime._stop_timeout = 1.0
    runtime._gate = asyncio.Lock()
    runtime._executor = ThreadPoolExecutor(max_workers=1)
    runtime._executor_closed = False
    runtime._active_future = None
    runtime._mutable_lock = threading.Lock()
    runtime._started = started
    runtime._stop_requested = False
    runtime._state = "running" if started else "new"
    runtime._last_error_code = None
    runtime._last_acquisition = composition.ShadowTickResult(
        "idle", "not_started", None, None, None, None, False
    )
    runtime._last_bridge = composition.DwdCompletionBridgeResult(
        "idle", "not_started", 0
    )
    runtime._last_coordinator_stop = composition._coordinator_stop_proof(
        None
    )
    runtime._last_coordinator_live = (
        composition._unobserved_coordinator_live_proof()
    )
    runtime._canary_allowed_workload_ids = (DWD_WORKLOAD_ID,)
    runtime._counts = {
        "acquisition_ticks": 0,
        "bridge_calls": 0,
        "coordinator_ticks": 0,
        "failures": 0,
        "start_calls": 0,
        "stop_calls": 0,
        "ticks": 0,
        "timeouts": 0,
    }
    return runtime, acquisition, bridge


def test_v1_outer_contract_remains_exactly_inert_and_has_no_allowlist(
    tmp_path: Path,
) -> None:
    config = _v1_config(tmp_path)
    mapping = config.to_mapping()

    assert composition.CONFIG_CONTRACT == (
        "stv3-worker-first-service-composition-config-v1"
    )
    assert composition.RUNTIME_CONTRACT == (
        "stv3-worker-first-service-composition-runtime-v1"
    )
    assert composition.MODE == "service_candidate_off_by_default"
    assert composition.SERVICE_WIRING_ENABLED is False
    assert composition.METEOROLOGICAL_ADMISSION == "exact_fact_binding_only"
    assert composition.KMI_APPV4_BINDING == (
        "gap_not_autonomous_lifecycle_bound"
    )
    assert mapping == {
        "acquisition": {
            "capacity_profile": config.capacity_profile.to_mapping(),
            "paths": config.source_paths.to_mapping(),
            "raw_store_limits": {
                name: getattr(config.raw_store_limits, name)
                for name in config.raw_store_limits.__dataclass_fields__
            },
        },
        "contract": "stv3-worker-first-service-composition-config-v1",
        "kmi_appv4_binding": "gap_not_autonomous_lifecycle_bound",
        "legacy_heavy_admission": "forbidden",
        "live_wiring": False,
        "meteorological_admission": "exact_fact_binding_only",
        "mode": "service_candidate_off_by_default",
        "retention": {
            "automatic_reclaim": False,
            "capacity_exhaustion": "fail_closed",
            "source_reclaim_authorized": False,
        },
        "timeouts": {
            "acquisition_seconds": 120.0,
            "bridge_seconds": 30.0,
            "stop_seconds": 6.0,
        },
        "worker_first": config.worker_first.to_mapping(),
    }
    assert "allowed_workload_ids" not in mapping["acquisition"]
    assert "production_enable_ready" not in mapping
    assert config.fingerprint == hashlib.sha256(_canonical(mapping)).hexdigest()

    tree = ast.parse(MODULE.read_text(encoding="utf-8"))
    v1_builder = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "build_worker_first_service_composition"
    )
    create_call = next(
        node for node in ast.walk(v1_builder)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "create_shadow_candidate"
    )
    assert "allowed_workload_ids" not in {
        keyword.arg for keyword in create_call.keywords
    }

    class _Result:
        @staticmethod
        def as_dict() -> dict[str, object]:
            return {"golden": True}

    runtime = object.__new__(composition.WorkerFirstServiceRuntime)
    runtime._mutable_lock = threading.Lock()
    runtime._active_future = None
    runtime._authority_sha256 = "a" * 64
    runtime._config_sha256 = "b" * 64
    runtime._counts = {"ticks": 0}
    runtime._last_acquisition = _Result()
    runtime._last_bridge = _Result()
    runtime._last_error_code = None
    runtime._state = "new"
    assert runtime.diagnostics() == {
        "authority_sha256": "a" * 64,
        "config_sha256": "b" * 64,
        "contract": "stv3-worker-first-service-composition-runtime-v1",
        "counts": {"ticks": 0},
        "dwd_fact_binding": "absent_factless",
        "heavy_lane_active": False,
        "kmi_appv4_binding": "gap_not_autonomous_lifecycle_bound",
        "last_acquisition": {"golden": True},
        "last_bridge": {"golden": True},
        "last_error_code": None,
        "legacy_heavy_admission": "forbidden",
        "live_wiring": False,
        "meteorological_admission": False,
        "mode": "service_candidate_off_by_default",
        "production_enable_ready": False,
        "retention": {
            "automatic_reclaim": False,
            "capacity_exhaustion": "fail_closed",
            "source_reclaim_authorized": False,
        },
        "state": "new",
    }
    for canary_only in (
        "coordinator_stop",
        "ownership_fence_held",
        "shutdown_complete",
    ):
        assert canary_only not in runtime.diagnostics()


def test_service_stop_waits_for_coordinator_terminal_fence_proof_and_retries(
) -> None:
    pending = {
        "closed": True,
        "ownership": {"fence_held": True},
        "state": "stopped_pending_processor",
    }
    terminal = {
        "closed": True,
        "ownership": {"fence_held": False},
        "state": "stopped",
    }

    class _Acquisition:
        stop_calls = 0

        def request_stop(self) -> None:
            self.stop_calls += 1

    class _Coordinator:
        def __init__(self) -> None:
            self.stop_calls = 0

        async def async_stop(self) -> dict[str, object]:
            self.stop_calls += 1
            return pending if self.stop_calls == 1 else terminal

    acquisition = _Acquisition()
    coordinator = _Coordinator()
    runtime = object.__new__(composition.WorkerFirstServiceRuntime)
    runtime._acquisition = acquisition
    runtime._coordinator = coordinator
    runtime._gate = asyncio.Lock()
    runtime._executor = ThreadPoolExecutor(max_workers=1)
    runtime._executor_closed = False
    runtime._active_future = None
    runtime._mutable_lock = threading.Lock()
    runtime._stop_requested = False
    runtime._state = "running"
    runtime._last_error_code = None
    runtime._last_coordinator_stop = composition._coordinator_stop_proof(None)
    runtime._stop_timeout = 1.0
    runtime._authority_sha256 = "a" * 64
    runtime._config_sha256 = "b" * 64
    runtime._counts = {
        "stop_calls": 0,
        "timeouts": 0,
    }
    runtime._last_acquisition = type(
        "AcquisitionResult",
        (),
        {"as_dict": staticmethod(dict)},
    )()
    runtime._last_bridge = type(
        "BridgeResult",
        (),
        {"as_dict": staticmethod(dict)},
    )()

    async def scenario():
        first = await runtime.async_stop()
        assert first["state"] == "stop_pending"
        assert first["last_error_code"] == "coordinator_stop_pending"
        assert runtime._last_coordinator_stop == {
            "closed": True,
            "fence_held": True,
            "state": "stopped_pending_processor",
            "terminal": False,
        }
        assert "coordinator_stop" not in first
        assert "ownership_fence_held" not in first
        assert "shutdown_complete" not in first
        assert runtime._executor_closed is False

        second = await runtime.async_stop()
        assert second["state"] == "stopped"
        assert second["last_error_code"] is None
        assert runtime._last_coordinator_stop == {
            "closed": True,
            "fence_held": False,
            "state": "stopped",
            "terminal": True,
        }
        assert "coordinator_stop" not in second
        assert "ownership_fence_held" not in second
        assert "shutdown_complete" not in second
        assert runtime._executor_closed is True

        third = await runtime.async_stop()
        assert third == runtime.diagnostics()
        return third

    result = asyncio.run(scenario())
    assert result["state"] == "stopped"
    assert coordinator.stop_calls == 2
    assert acquisition.stop_calls == 3
    assert result["counts"]["stop_calls"] == 3


def test_v1_start_and_tick_preserve_original_coordinator_read_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Coordinator:
        def __init__(self) -> None:
            self.diagnostic_calls = 0

        async def async_start(self) -> dict[str, object]:
            return {"legacy": "ignored"}

        async def async_tick(self) -> dict[str, object]:
            return {"legacy": "ignored"}

        def diagnostics(self) -> dict[str, object]:
            self.diagnostic_calls += 1
            return {"counts": {}}

    coordinator = _Coordinator()
    runtime = object.__new__(composition.WorkerFirstServiceRuntime)
    runtime._coordinator = coordinator
    runtime._gate = asyncio.Lock()
    runtime._started = False
    runtime._stop_requested = False
    runtime._state = "new"
    runtime._last_error_code = None
    runtime._bridge_timeout = 1.0
    runtime._acquisition_timeout = 1.0
    runtime._last_acquisition = composition.ShadowTickResult(
        "idle", "not_started", None, None, None, None, False
    )
    runtime._counts = {
        "acquisition_ticks": 0,
        "bridge_calls": 0,
        "coordinator_ticks": 0,
        "failures": 0,
        "start_calls": 0,
        "stop_calls": 0,
        "ticks": 0,
        "timeouts": 0,
    }
    runtime._resource_preflight = lambda: None
    runtime._bridge = SimpleNamespace(
        publish_next=lambda: composition.DwdCompletionBridgeResult(
            "idle", "no_completion", 0
        ),
    )
    runtime._acquisition = SimpleNamespace(
        tick=lambda: composition.ShadowTickResult(
            "idle", "no_job", None, None, None, None, False
        ),
    )
    runtime._last_bridge = composition.DwdCompletionBridgeResult(
        "idle", "not_started", 0
    )
    monkeypatch.setattr(
        runtime,
        "diagnostics",
        lambda: {"state": runtime._state},
    )
    async def blocking(function, **_kwargs):
        return function()

    runtime._blocking = blocking

    async def start_only() -> None:
        await runtime.async_start()

    asyncio.run(start_only())
    assert coordinator.diagnostic_calls == 0

    async def tick_once() -> None:
        await runtime.async_tick()

    asyncio.run(tick_once())
    assert coordinator.diagnostic_calls == 2


def test_v1_hooks_are_noops_and_never_read_or_validate_coordinator() -> None:
    runtime = object.__new__(composition.WorkerFirstServiceRuntime)
    runtime._coordinator = SimpleNamespace(
        diagnostics=lambda: pytest.fail("v1 hook read coordinator diagnostics"),
    )
    runtime._accept_coordinator_live_diagnostics(
        {"arbitrary": "legacy"},
        phase="not-a-v2-phase",
    )
    runtime._accept_current_coordinator_live_diagnostics(
        phase="not-a-v2-phase",
    )
    runtime._prepare_coordinator_tick_diagnostics(
        {"arbitrary": "legacy"},
        phase="not-a-v2-phase",
    )
    runtime._preflight_coordinator_live(phase="not-a-v2-phase")


def test_canary_start_rejects_degraded_coordinator_before_bridge() -> None:
    degraded = _coordinator_diagnostics(
        state="degraded",
        last_error_code="pipeline_transition_failed",
    )
    coordinator = _CoordinatorHarness(start=degraded)
    runtime, acquisition, bridge = _canary_runtime_harness(
        coordinator,
        started=False,
    )

    async def scenario() -> None:
        with pytest.raises(
            composition.WorkerFirstServiceCompositionIntegrityError,
            match="^canary_coordinator_reported_error$",
        ):
            await runtime.async_start()

    try:
        asyncio.run(scenario())
        diagnostics = runtime.diagnostics()
        assert diagnostics["state"] == "error"
        assert diagnostics["ownership_fence_held"] is True
        assert diagnostics["coordinator_live"]["state"] == "degraded"
        assert diagnostics["coordinator_live"]["ready"] is False
        assert bridge.publish_calls == 0
        assert acquisition.tick_calls == 0
        assert coordinator.stop_calls == 1
    finally:
        runtime._executor.shutdown(wait=True, cancel_futures=True)


@pytest.mark.parametrize(
    "tick_result,error_code,ownership_fence_held",
    (
        (
            _coordinator_diagnostics(
                state="degraded",
                last_error_code="pipeline_transition_failed",
            ),
            "canary_coordinator_reported_error",
            True,
        ),
        (
            _coordinator_diagnostics(
                state="error",
                fence_held=False,
                last_error_code="ownership_fence_not_held",
            ),
            "canary_coordinator_fence_not_held",
            False,
        ),
    ),
)
def test_canary_tick_rejects_nonready_coordinator_before_acquisition(
    tick_result: dict[str, object],
    error_code: str,
    ownership_fence_held: bool,
) -> None:
    coordinator = _CoordinatorHarness(tick=tick_result)
    coordinator.current = tick_result
    runtime, acquisition, bridge = _canary_runtime_harness(
        coordinator,
        started=True,
    )

    async def scenario() -> None:
        with pytest.raises(
            composition.WorkerFirstServiceCompositionIntegrityError,
            match=f"^{error_code}$",
        ):
            await runtime.async_tick()

    try:
        asyncio.run(scenario())
        diagnostics = runtime.diagnostics()
        assert diagnostics["state"] == "degraded"
        assert diagnostics["ownership_fence_held"] is (
            ownership_fence_held
        )
        assert diagnostics["coordinator_live"]["ready"] is False
        assert diagnostics["coordinator_live"]["state"] == (
            tick_result["state"]
        )
        assert bridge.publish_calls == 0
        assert acquisition.tick_calls == 0
    finally:
        runtime._executor.shutdown(wait=True, cancel_futures=True)


def test_canary_stop_pending_retains_fence_and_is_not_shutdown_complete(
) -> None:
    pending = {
        "closed": True,
        "ownership": {"fence_held": True},
        "state": "stopped_pending_processor",
    }
    coordinator = _CoordinatorHarness(stop=pending)
    runtime, _acquisition, _bridge = _canary_runtime_harness(
        coordinator,
        started=True,
    )

    async def scenario() -> dict[str, object]:
        runtime._preflight_coordinator_live(phase="tick_current")
        return await runtime.async_stop()

    try:
        diagnostics = asyncio.run(scenario())
        assert diagnostics["state"] == "stop_pending"
        assert diagnostics["ownership_fence_held"] is True
        assert diagnostics["shutdown_complete"] is False
        assert diagnostics["coordinator_stop"] == {
            "closed": True,
            "fence_held": True,
            "state": "stopped_pending_processor",
            "terminal": False,
        }
        assert diagnostics["coordinator_live"]["ready"] is True
        assert runtime._executor_closed is False
    finally:
        runtime._executor.shutdown(wait=True, cancel_futures=True)


def test_canary_v2_roundtrips_with_exact_dwd_only_live_shadow_authority(
    tmp_path: Path,
) -> None:
    config = _canary_config(tmp_path)
    mapping = config.to_mapping()

    assert mapping == {
        "acquisition": {
            "allowed_workload_ids": [DWD_WORKLOAD_ID],
            "capacity_profile": config.capacity_profile.to_mapping(),
            "paths": config.source_paths.to_mapping(),
            "raw_store_limits": {
                name: getattr(config.raw_store_limits, name)
                for name in config.raw_store_limits.__dataclass_fields__
            },
        },
        "contract": composition.CANARY_CONFIG_CONTRACT,
        "kmi_appv4_binding": composition.KMI_APPV4_BINDING,
        "legacy_heavy_admission": "forbidden",
        "live_wiring": True,
        "meteorological_admission": False,
        "mode": "service_worker_first_canary_opt_in",
        "production_enable_ready": False,
        "retention": {
            "automatic_reclaim": False,
            "capacity_exhaustion": "fail_closed",
            "source_reclaim_authorized": False,
        },
        "timeouts": {
            "acquisition_seconds": 120.0,
            "bridge_seconds": 30.0,
            "stop_seconds": 6.0,
        },
        "worker_first": config.worker_first.to_mapping(),
    }
    reparsed = composition.WorkerFirstServiceCanaryConfig.from_mapping(mapping)
    assert reparsed == config
    assert reparsed.to_mapping() == mapping
    assert reparsed.fingerprint == hashlib.sha256(_canonical(mapping)).hexdigest()
    assert reparsed.fingerprint != _v1_config(tmp_path).fingerprint


@pytest.mark.parametrize(
    "allowed",
    (
        [],
        [DWD_WORKLOAD_ID, DWD_WORKLOAD_ID],
        ["radar.knmi.rtcor", DWD_WORKLOAD_ID],
        [DWD_WORKLOAD_ID, "radar.knmi.rtcor"],
        ["radar.knmi.rtcor"],
        DWD_WORKLOAD_ID,
    ),
)
def test_canary_v2_parser_rejects_every_non_exact_allowlist(
    tmp_path: Path,
    allowed: object,
) -> None:
    mapping = _canary_config(tmp_path).to_mapping()
    mapping["acquisition"]["allowed_workload_ids"] = allowed

    with pytest.raises(
        composition.WorkerFirstServiceCompositionConfigError,
        match="^canary_allowed_workload_ids_invalid$",
    ):
        composition.WorkerFirstServiceCanaryConfig.from_mapping(mapping)


@pytest.mark.parametrize(
    "section,key,value",
    (
        (None, "contract", "stv3-worker-first-service-composition-config-v1"),
        (None, "live_wiring", False),
        (None, "meteorological_admission", True),
        (None, "mode", "service_candidate_off_by_default"),
        (None, "production_enable_ready", True),
        ("retention", "automatic_reclaim", True),
        ("retention", "source_reclaim_authorized", True),
        ("retention", "capacity_exhaustion", "reclaim"),
    ),
)
def test_canary_v2_parser_rejects_authority_drift(
    tmp_path: Path,
    section: str | None,
    key: str,
    value: object,
) -> None:
    mapping = _canary_config(tmp_path).to_mapping()
    target = mapping if section is None else mapping[section]
    target[key] = value

    with pytest.raises(
        composition.WorkerFirstServiceCompositionConfigError,
        match="^canary_service_config_authority_invalid$",
    ):
        composition.WorkerFirstServiceCanaryConfig.from_mapping(mapping)


def test_canary_v2_builder_passes_immutable_dwd_allowlist_and_is_truthful(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _canary_config(tmp_path)
    expected_authority = "a" * 64
    captured: dict[str, object] = {}
    canary_runtime_class = composition.WorkerFirstServiceCanaryRuntime

    class _Fence:
        def __init__(self, _path: Path, *, expected_authority_sha256: str):
            captured["authority_sha256"] = expected_authority_sha256
            self.binding = _Binding()

        def acquire(self):
            return _Lease()

    class _Binding:
        def store(self, role: str):
            return type("Store", (), {
                "path": config.worker_first.paths.to_mapping()[{
                    "acquisition_completion_store": "acquisition_completion_root",
                    "desired_state_store": "desired_state_db",
                    "operational_fact_store": "operational_fact_db",
                    "operational_snapshot_store": "operational_snapshot_db",
                    "pipeline_output_store": "pipeline_output_db",
                    "snapshot_outbox_store": "snapshot_outbox_db",
                }[role]],
            })()

        def assert_coordinator_binding(self, **_kwargs: object) -> None:
            return None

    class _Lease:
        def release(self) -> None:
            captured["released"] = True

    class _Acquisition:
        desired_state_store = object()
        scheduler = object()
        execution_store = object()
        capacity_ledger = object()
        raw_store = object()

    class _Bridge:
        pass

    class _Snapshots:
        pass

    class _Coordinator:
        pass

    class _Runtime:
        def __init__(self, **kwargs: object):
            captured["runtime_kwargs"] = kwargs

    parts = SimpleNamespace(
        completion=object(),
        consumer=SimpleNamespace(process_leased=lambda: None),
        desired=object(),
        facts=object(),
        handoff=object(),
        outbox=object(),
        outcome_authority=object(),
        snapshots=object(),
    )

    monkeypatch.setattr(composition, "WorkerFirstOwnershipFence", _Fence)
    monkeypatch.setattr(
        composition,
        "_construct_store_parts",
        lambda *_a, **_k: parts,
    )
    monkeypatch.setattr(composition, "_store_bindings", lambda *_a, **_k: {})
    monkeypatch.setattr(
        composition,
        "create_shadow_candidate",
        lambda *_a, **kwargs: (
            captured.setdefault("candidate_kwargs", kwargs),
            _Acquisition(),
        )[1],
    )
    monkeypatch.setattr(
        composition,
        "DwdAcquisitionCompletionBridge",
        lambda **_k: _Bridge(),
    )
    monkeypatch.setattr(
        composition,
        "OperationalSnapshotRuntime",
        lambda **_k: _Snapshots(),
    )
    monkeypatch.setattr(
        composition,
        "WorkerFirstRuntimeCoordinator",
        lambda **_k: _Coordinator(),
    )
    monkeypatch.setattr(composition, "WorkerFirstServiceCanaryRuntime", _Runtime)

    runtime = composition.build_worker_first_service_canary(
        config,
        expected_config_sha256=config.fingerprint,
        expected_authority_sha256=expected_authority,
        publisher=lambda _payload: None,
        status_supplier=dict,
        clock=lambda: 1_800_000_000.0,
    )

    assert isinstance(runtime, _Runtime)
    assert captured["authority_sha256"] == expected_authority
    candidate_kwargs = captured["candidate_kwargs"]
    assert candidate_kwargs["allowed_workload_ids"] == frozenset({
        DWD_WORKLOAD_ID,
    })
    assert type(candidate_kwargs["allowed_workload_ids"]) is frozenset
    runtime_kwargs = captured["runtime_kwargs"]
    assert runtime_kwargs["allowed_workload_ids"] == (DWD_WORKLOAD_ID,)

    diagnostics_runtime = object.__new__(
        canary_runtime_class
    )
    diagnostics_runtime._canary_allowed_workload_ids = (DWD_WORKLOAD_ID,)
    monkeypatch.setattr(
        composition.WorkerFirstServiceRuntime,
        "diagnostics",
        lambda _self: {
            "contract": composition.RUNTIME_CONTRACT,
            "last_error_code": None,
            "live_wiring": False,
            "meteorological_admission": False,
            "mode": composition.MODE,
            "production_enable_ready": False,
            "state": "running",
        },
    )
    diagnostics_runtime._coordinator = _CoordinatorHarness()
    diagnostics_runtime._authority_sha256 = "a" * 64
    diagnostics_runtime._last_coordinator_stop = (
        composition._coordinator_stop_proof(None)
    )
    diagnostics_runtime._last_coordinator_live = (
        composition._unobserved_coordinator_live_proof()
    )
    diagnostics_runtime._stop_requested = False
    diagnostics_runtime._state = "new"
    diagnostics_runtime._executor_closed = False
    assert diagnostics_runtime.diagnostics() == {
        "acquisition_admission": {
            "allowed_workload_ids": [DWD_WORKLOAD_ID],
            "policy": composition.CANARY_ADMISSION_POLICY,
        },
        "contract": composition.CANARY_RUNTIME_CONTRACT,
        "coordinator_live": {
            "closed": False,
            "fence_held": True,
            "last_error_code": None,
            "owns_heavy_lane": True,
            "phase": "diagnostic",
            "ready": True,
            "started": True,
            "state": "running",
            "valid": True,
        },
        "coordinator_stop": {
            "closed": None,
            "fence_held": None,
            "state": "not_observed",
            "terminal": False,
        },
        "last_error_code": None,
        "live_wiring": True,
        "meteorological_admission": False,
        "mode": composition.CANARY_MODE,
        "ownership_fence_held": True,
        "production_enable_ready": False,
        "science_mode": composition.CANARY_SCIENCE_MODE,
        "shutdown_complete": False,
        "state": "running",
    }


def test_canary_v2_constructor_and_builder_pins_fail_before_side_effects(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _canary_config(tmp_path)
    with pytest.raises(
        composition.WorkerFirstServiceCompositionConfigError,
        match="^canary_allowed_workload_ids_invalid$",
    ):
        replace(config, allowed_workload_ids=(DWD_WORKLOAD_ID, "radar.knmi.rtcor"))

    monkeypatch.setattr(
        composition,
        "WorkerFirstOwnershipFence",
        lambda *_a, **_k: pytest.fail("pin failure reached the ownership fence"),
    )
    with pytest.raises(
        composition.WorkerFirstServiceCompositionConfigError,
        match="^canary_service_config_checksum_mismatch$",
    ):
        composition.build_worker_first_service_canary(
            config,
            expected_config_sha256="0" * 64,
            expected_authority_sha256="a" * 64,
            publisher=lambda _payload: None,
            status_supplier=dict,
        )


def test_canary_v2_busy_fence_precedes_all_mutable_store_construction(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _canary_config(tmp_path)
    events: list[str] = []
    store_attributes = {
        "acquisition_completion_store": "acquisition_completion_root",
        "desired_state_store": "desired_state_db",
        "operational_fact_store": "operational_fact_db",
        "operational_snapshot_store": "operational_snapshot_db",
        "pipeline_output_store": "pipeline_output_db",
        "snapshot_outbox_store": "snapshot_outbox_db",
    }

    class _Binding:
        def store(self, role: str) -> SimpleNamespace:
            return SimpleNamespace(
                path=getattr(config.worker_first.paths, store_attributes[role])
            )

    class _BusyFence:
        binding = _Binding()

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            return None

        def acquire(self) -> None:
            events.append("fence.acquire")
            raise composition.WorkerFirstFenceBusy("ownership_busy")

    def _forbidden_store_construction(*_args: object, **_kwargs: object) -> None:
        events.append("stores.construct")
        pytest.fail("busy canary reached mutable store construction")

    monkeypatch.setattr(composition, "WorkerFirstOwnershipFence", _BusyFence)
    monkeypatch.setattr(
        composition,
        "_construct_store_parts",
        _forbidden_store_construction,
    )

    with pytest.raises(
        composition.WorkerFirstCompositionBusy,
        match="^canary_service_composition_owner_busy$",
    ):
        composition.build_worker_first_service_canary(
            config,
            expected_config_sha256=config.fingerprint,
            expected_authority_sha256="a" * 64,
            publisher=lambda _payload: None,
            status_supplier=dict,
        )

    assert events == ["fence.acquire"]


def test_canary_v2_store_construction_failure_releases_owned_fence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _canary_config(tmp_path)
    events: list[str] = []
    store_attributes = {
        "acquisition_completion_store": "acquisition_completion_root",
        "desired_state_store": "desired_state_db",
        "operational_fact_store": "operational_fact_db",
        "operational_snapshot_store": "operational_snapshot_db",
        "pipeline_output_store": "pipeline_output_db",
        "snapshot_outbox_store": "snapshot_outbox_db",
    }

    class _Binding:
        def store(self, role: str) -> SimpleNamespace:
            return SimpleNamespace(
                path=getattr(config.worker_first.paths, store_attributes[role])
            )

    class _Lease:
        def release(self) -> None:
            events.append("fence.release")

    class _Fence:
        binding = _Binding()

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            return None

        def acquire(self) -> _Lease:
            events.append("fence.acquire")
            return _Lease()

    def _failed_store_construction(*_args: object, **_kwargs: object) -> None:
        events.append("stores.construct")
        raise RuntimeError("sensitive-construction-detail")

    monkeypatch.setattr(composition, "WorkerFirstOwnershipFence", _Fence)
    monkeypatch.setattr(
        composition,
        "_construct_store_parts",
        _failed_store_construction,
    )

    with pytest.raises(
        composition.WorkerFirstCompositionError,
        match="^canary_service_composition_construction_failed$",
    ) as captured:
        composition.build_worker_first_service_canary(
            config,
            expected_config_sha256=config.fingerprint,
            expected_authority_sha256="a" * 64,
            publisher=lambda _payload: None,
            status_supplier=dict,
        )

    assert "sensitive-construction-detail" not in str(captured.value)
    assert events == ["fence.acquire", "stores.construct", "fence.release"]


@pytest.mark.parametrize(
    "suffix",
    ("source/../source/scheduler.sqlite3", "source/trailing./scheduler.sqlite3"),
)
def test_canary_v2_rejects_noncanonical_source_paths_before_builder(
    tmp_path: Path,
    suffix: str,
) -> None:
    config = _canary_config(tmp_path / "baseline")
    paths = config.source_paths.to_mapping()
    paths["scheduler_db"] = str((tmp_path / suffix).absolute())
    mapping = config.to_mapping()
    mapping["acquisition"]["paths"] = paths

    with pytest.raises(
        composition.WorkerFirstServiceCompositionConfigError,
        match="^canary_source_path_(?:not_canonical|unsafe)$",
    ):
        composition.WorkerFirstServiceCanaryConfig.from_mapping(mapping)


def test_canary_v2_rejects_casefolded_source_alias_before_builder(
    tmp_path: Path,
) -> None:
    config = _canary_config(tmp_path)
    paths = config.source_paths.to_mapping()
    paths["execution_receipts_db"] = paths["scheduler_db"].swapcase()
    mapping = config.to_mapping()
    mapping["acquisition"]["paths"] = paths

    with pytest.raises(
        composition.WorkerFirstServiceCompositionConfigError,
        match="^canary_source_paths_alias$",
    ):
        composition.WorkerFirstServiceCanaryConfig.from_mapping(mapping)


def test_canary_v2_rejects_source_alias_with_pipeline_path_before_builder(
    tmp_path: Path,
) -> None:
    config = _canary_config(tmp_path)
    paths = config.source_paths.to_mapping()
    paths["scheduler_db"] = str(config.worker_first.paths.desired_state_db)
    mapping = config.to_mapping()
    mapping["acquisition"]["paths"] = paths

    with pytest.raises(
        composition.WorkerFirstServiceCompositionConfigError,
        match="^canary_source_paths_alias$",
    ):
        composition.WorkerFirstServiceCanaryConfig.from_mapping(mapping)


def test_canary_v2_rejects_existing_source_symlink_ancestor_before_builder(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    link = tmp_path / "linked-source"
    config = _canary_config(tmp_path / "baseline")
    link.mkdir()
    real_is_symlink = Path.is_symlink
    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda path: path == link or real_is_symlink(path),
    )
    paths = config.source_paths.to_mapping()
    paths["scheduler_db"] = str(link / "scheduler.sqlite3")
    mapping = config.to_mapping()
    mapping["acquisition"]["paths"] = paths

    with pytest.raises(
        composition.WorkerFirstServiceCompositionConfigError,
        match="^canary_source_path_(?:not_canonical|unsafe)$",
    ):
        composition.WorkerFirstServiceCanaryConfig.from_mapping(mapping)


def test_canary_v2_rejects_trailing_separator_source_alias(
    tmp_path: Path,
) -> None:
    mapping = _canary_config(tmp_path).to_mapping()
    mapping["acquisition"]["paths"]["scheduler_db"] += os.sep

    with pytest.raises(
        composition.WorkerFirstServiceCompositionConfigError,
        match="^canary_source_path_not_canonical$",
    ):
        composition.WorkerFirstServiceCanaryConfig.from_mapping(mapping)


@pytest.mark.parametrize("target", ("source", "pipeline"))
def test_canary_v2_rejects_nested_physical_path_aliases(
    tmp_path: Path,
    target: str,
) -> None:
    config = _canary_config(tmp_path)
    mapping = config.to_mapping()
    if target == "source":
        raw_root = config.source_paths.scheduler_db.parent
    else:
        raw_root = (
            config.worker_first.paths.acquisition_completion_root / "source"
        )
    mapping["acquisition"]["paths"]["raw_store_root"] = str(raw_root)

    with pytest.raises(
        composition.WorkerFirstServiceCompositionConfigError,
        match="^canary_source_paths_alias$",
    ):
        composition.WorkerFirstServiceCanaryConfig.from_mapping(mapping)


def test_canary_v2_builder_rechecks_reparse_ancestors_before_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _canary_config(tmp_path / "baseline")
    link = config.source_paths.scheduler_db.parent
    link.parent.mkdir(parents=True, exist_ok=True)
    link.mkdir()
    metadata = link.lstat()
    reparse_identity = (metadata.st_dev, metadata.st_ino)
    real_is_reparse = composition._canary_is_reparse
    monkeypatch.setattr(
        composition,
        "_canary_is_reparse",
        lambda value: (
            (value.st_dev, value.st_ino) == reparse_identity
            or real_is_reparse(value)
        ),
    )
    monkeypatch.setattr(
        composition,
        "WorkerFirstOwnershipFence",
        lambda *_a, **_k: pytest.fail("unsafe path reached ownership fence"),
    )

    with pytest.raises(
        composition.WorkerFirstServiceCompositionConfigError,
        match="^canary_source_path_unsafe$",
    ):
        composition.build_worker_first_service_canary(
            config,
            expected_config_sha256=config.fingerprint,
            expected_authority_sha256="a" * 64,
            publisher=lambda _payload: None,
            status_supplier=dict,
        )
