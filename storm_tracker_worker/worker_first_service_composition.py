"""Off-by-default service-owned worker-first composition candidate.

This module is the sole boundary that may combine autonomous acquisition, the
raw-to-completion bridge, the DWD completion consumer/coordinator and the
operational snapshot QoS1 publisher supplied by ``WorkerService``.  It is not
loaded by the default legacy path.  ``service.main`` may import it lazily only
after the explicit ``worker_first_canary`` selector has verified independent
configuration and authority SHA pins; the service then rejects every legacy
heavy owner.

The candidate deliberately remains meteorologically fail-closed: current DWD
output is factless, source stores have hard caps but no reclaim authority, and
KMI app-v4 is not yet in the autonomous acquisition matrix.  Consequently it
can publish exact ``unknown`` snapshots and replay exact raw evidence, never a
rain/rate/track claim.
"""
from __future__ import annotations

import asyncio
from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import threading
import time
from typing import Any, Callable, Final

from .acquisition_activation import SecretResolver
from .acquisition_runtime import (
    AcquisitionShadowRuntime,
    CANARY_CHILD_ENVIRONMENT_ALLOWLIST,
    ExecutorFactory,
    MAX_EXECUTOR_WALL_SECONDS,
    ShadowRuntimePaths,
    ShadowTickResult,
    _dwd_canary_executor_factory,
    _default_executor_factory,
    create_shadow_candidate,
)
from .dwd_acquisition_completion_bridge import (
    DwdAcquisitionCompletionBridge,
    DwdCompletionBridgeResult,
)
from .dwd_rv_completion_consumer import CONSUMER_ID, STREAM_ID
from .operational_snapshot_runtime import OperationalSnapshotRuntime
from .raw_artifacts import RawArtifactLimits
from .raw_capacity_ledger import ProviderCapacityLimit, RawCapacityProfile
from .worker_first_composition import (
    WorkerFirstCompositionBusy,
    WorkerFirstCompositionConfig,
    WorkerFirstCompositionConfigError,
    WorkerFirstCompositionError,
    WorkerFirstCompositionIntegrityError,
    _construct_store_parts,
    _same_path,
    _store_bindings,
    _validate_config_pin,
)
from .worker_first_ownership import (
    REQUIRED_STORE_ROLES,
    WorkerFirstFenceBusy,
    WorkerFirstOwnershipError,
    WorkerFirstOwnershipFence,
)
from .worker_first_runtime import (
    CONTRACT as WORKER_FIRST_COORDINATOR_CONTRACT,
    WorkerFirstRuntimeCoordinator,
)


CONFIG_CONTRACT: Final = "stv3-worker-first-service-composition-config-v1"
RUNTIME_CONTRACT: Final = "stv3-worker-first-service-composition-runtime-v1"
MODE: Final = "service_candidate_off_by_default"
SERVICE_WIRING_ENABLED: Final = False
METEOROLOGICAL_ADMISSION: Final = "exact_fact_binding_only"
KMI_APPV4_BINDING: Final = "exact_raw_only_autonomous_lifecycle_bound"
MAX_CONFIG_BYTES: Final = 384 * 1024
MAX_DIAGNOSTIC_BYTES: Final = 16 * 1024
MAX_COUNTER: Final = 1_000_000_000
# One provider process remains hard-bounded to 300 seconds.  A full-parallel
# service tick also includes bounded process teardown plus pre/post storage
# proof, which can legitimately outlive that individual provider wall.
ACQUISITION_LANE_TIMEOUT_SECONDS: Final = (
    (2.0 * MAX_EXECUTOR_WALL_SECONDS) + 15.0
)

# Version 1 above remains the inert, byte-stable candidate contract.  Version
# 2 is a separate, explicitly selected local canary authority; it does not
# promote meteorological output, retention, reclamation or deployment.
CANARY_CONFIG_CONTRACT: Final = (
    "stv3-worker-first-service-composition-config-v2"
)
CANARY_RUNTIME_CONTRACT: Final = (
    "stv3-worker-first-service-composition-runtime-v2"
)
CANARY_MODE: Final = "service_worker_first_canary_opt_in"
CANARY_SERVICE_WIRING_ENABLED: Final = True
CANARY_PRODUCTION_ENABLE_READY: Final = False
CANARY_ALLOWED_WORKLOAD_IDS: Final = ("radar.dwd.radolan_rv",)
CANARY_ADMISSION_POLICY: Final = "exact_dwd_radolan_rv_only_v1"
CANARY_SCIENCE_MODE: Final = "shadow"

_CANARY_SERVICE_FIELDS = frozenset({
    "acquisition",
    "contract",
    "kmi_appv4_binding",
    "legacy_heavy_admission",
    "live_wiring",
    "meteorological_admission",
    "mode",
    "production_enable_ready",
    "retention",
    "timeouts",
    "worker_first",
})
_CANARY_ACQUISITION_FIELDS = frozenset({
    "allowed_workload_ids",
    "capacity_profile",
    "paths",
    "raw_store_limits",
})
_CANARY_SOURCE_PATH_FIELDS = frozenset({
    "capacity_ledger_db",
    "execution_receipts_db",
    "raw_store_root",
    "scheduler_db",
})
_CANARY_CAPACITY_PROFILE_FIELDS = frozenset({
    "automatic_reclaim",
    "directory_fsync_policy",
    "global_max_bytes",
    "global_max_entries",
    "journal_mode",
    "main_database_envelope_divisor",
    "max_catalog_bytes",
    "max_reservations",
    "operational_admission_eligible",
    "providers",
    "schema",
    "sqlite_busy_timeout_seconds",
})
_CANARY_PROVIDER_FIELDS = frozenset({"max_bytes", "max_entries", "provider"})
_CANARY_RAW_LIMIT_FIELDS = frozenset({
    "max_capture_entries",
    "max_capture_members",
    "max_catalog_bytes",
    "max_object_bytes",
    "max_object_entries",
    "max_receipt_entries",
    "max_staging_entries",
    "max_total_bytes",
})
_CANARY_RETENTION_FIELDS = frozenset({
    "automatic_reclaim",
    "capacity_exhaustion",
    "source_reclaim_authorized",
})
_CANARY_TIMEOUT_FIELDS = frozenset({
    "acquisition_seconds",
    "bridge_seconds",
    "stop_seconds",
})
_COORDINATOR_STOP_STATES = frozenset({
    "closed",
    "stopped",
    "stopped_pending_gate",
    "stopped_pending_mutable_work",
    "stopped_pending_processor",
    "stopped_pending_snapshot",
    "stopping",
})
_COORDINATOR_DIAGNOSTIC_FIELDS = frozenset({
    "closed",
    "contract",
    "counts",
    "in_flight",
    "last_disposition",
    "last_error_code",
    "liveness",
    "ownership",
    "owns_heavy_lane",
    "snapshot",
    "started",
    "state",
})
_COORDINATOR_COUNT_FIELDS = frozenset({
    "acknowledged",
    "advanced",
    "authority_withheld",
    "cancellations",
    "control_timeouts",
    "fact_triggers",
    "failures",
    "idle_ticks",
    "indeterminate",
    "late_completions",
    "leases",
    "processed",
    "rejected",
    "snapshot_timeouts",
    "stop_calls",
    "stop_timeouts",
    "terminalized",
    "ticks",
    "withheld",
    "start_calls",
})
_COORDINATOR_OWNERSHIP_FIELDS = frozenset({
    "authority_scope_sha256",
    "authority_sha256",
    "detached_mutable_work_count",
    "detached_snapshot_task_count",
    "fence_held",
    "mutable_work_count",
    "runtime_flow_count",
    "snapshot_operations",
    "snapshot_quiesced",
    "snapshot_task_count",
    "store_binding_sha256",
    "store_count",
})
_COORDINATOR_LIVENESS_FIELDS = frozenset({
    "control_timeout_seconds",
    "stop_timeout_seconds",
})
_COORDINATOR_SNAPSHOT_FIELDS = frozenset({
    "last_error_code",
    "state",
})
_COORDINATOR_STATES = frozenset({
    "degraded",
    "error",
    "new",
    "running",
    "starting",
    "stopped",
    "stopped_pending_gate",
    "stopped_pending_mutable_work",
    "stopped_pending_processor",
    "stopped_pending_snapshot",
    "stopping",
    "unconfigured",
})
_COORDINATOR_SNAPSHOT_STATES = frozenset({
    "degraded",
    "new",
    "running",
    "starting",
    "stopped",
    "stopping",
    "unconfigured",
    "unknown",
})
_COORDINATOR_ERROR_CODES = frozenset({
    "completion_ack_receipt_mismatch",
    "completion_reject_receipt_mismatch",
    "control_operation_pending",
    "control_operation_timeout",
    "fact_snapshot_publication_failed",
    "fact_snapshot_publication_mismatch",
    "mutable_work_pending",
    "ownership_fence_not_held",
    "pipeline_fact_authority_mismatch",
    "pipeline_output_authority_mismatch",
    "pipeline_processor_timeout",
    "pipeline_transition_failed",
    "processor_unresponsive",
    "runtime_stopping",
    "snapshot_operation_pending",
    "snapshot_operation_timeout",
    "snapshot_runtime_stop_failed",
    "snapshot_runtime_tick_failed",
    "stop_cancelled",
    "stop_gate_timeout",
    "snapshot_stop_timeout",
    "tick_cancelled",
})
_COORDINATOR_LIVE_PHASES = frozenset({
    "diagnostic",
    "not_observed",
    "start_current",
    "start_result",
    "tick_current",
    "tick_result",
})


class WorkerFirstServiceCompositionError(RuntimeError):
    """A fixed, privacy-safe service-composition failure."""


class WorkerFirstServiceCompositionConfigError(
    WorkerFirstServiceCompositionError
):
    pass


class WorkerFirstServiceCompositionIntegrityError(
    WorkerFirstServiceCompositionError
):
    pass


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as error:
        raise WorkerFirstServiceCompositionConfigError(
            "service_config_not_canonical"
        ) from error


def _duration(
    value: object,
    name: str,
    *,
    minimum: float,
    maximum: float,
) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not minimum <= float(value) <= maximum
    ):
        raise WorkerFirstServiceCompositionConfigError(f"{name}_invalid")
    return float(value)


def _coordinator_stop_proof(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {
            "closed": None,
            "fence_held": None,
            "state": "not_observed",
            "terminal": False,
        }
    raw_state = value.get("state")
    state = (
        raw_state
        if isinstance(raw_state, str)
        and raw_state in _COORDINATOR_STOP_STATES
        else "unknown"
    )
    raw_closed = value.get("closed")
    closed = raw_closed if type(raw_closed) is bool else None
    ownership = value.get("ownership")
    raw_fence_held = (
        ownership.get("fence_held")
        if isinstance(ownership, Mapping)
        else None
    )
    fence_held = (
        raw_fence_held if type(raw_fence_held) is bool else None
    )
    return {
        "closed": closed,
        "fence_held": fence_held,
        "state": state,
        "terminal": (
            state in {"stopped", "closed"}
            and closed is True
            and fence_held is False
        ),
    }


def _coordinator_counter(value: object) -> bool:
    return (
        type(value) is int
        and 0 <= value <= MAX_COUNTER
    )


def _coordinator_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _unobserved_coordinator_live_proof(
    phase: str = "not_observed",
) -> dict[str, Any]:
    return {
        "closed": None,
        "fence_held": None,
        "last_error_code": "diagnostics_not_observed",
        "owns_heavy_lane": None,
        "phase": phase,
        "ready": False,
        "started": None,
        "state": "not_observed",
        "valid": False,
    }


def _validated_coordinator_live_proof(
    value: object,
    *,
    expected_authority_sha256: str,
    phase: str,
) -> dict[str, Any]:
    invalid = WorkerFirstServiceCompositionIntegrityError(
        "canary_coordinator_diagnostics_invalid"
    )
    if phase not in _COORDINATOR_LIVE_PHASES:
        raise invalid
    if (
        not isinstance(value, Mapping)
        or set(value) != _COORDINATOR_DIAGNOSTIC_FIELDS
    ):
        raise invalid
    counts = value["counts"]
    ownership = value["ownership"]
    liveness = value["liveness"]
    snapshot = value["snapshot"]
    if (
        not isinstance(counts, Mapping)
        or set(counts) != _COORDINATOR_COUNT_FIELDS
        or any(not _coordinator_counter(item) for item in counts.values())
        or not isinstance(ownership, Mapping)
        or set(ownership) != _COORDINATOR_OWNERSHIP_FIELDS
        or not isinstance(liveness, Mapping)
        or set(liveness) != _COORDINATOR_LIVENESS_FIELDS
        or not isinstance(snapshot, Mapping)
        or set(snapshot) != _COORDINATOR_SNAPSHOT_FIELDS
    ):
        raise invalid
    state = value["state"]
    started = value["started"]
    closed = value["closed"]
    owns_heavy_lane = value["owns_heavy_lane"]
    in_flight = value["in_flight"]
    last_error_code = value["last_error_code"]
    last_disposition = value["last_disposition"]
    fence_held = ownership["fence_held"]
    ownership_counters = (
        ownership["detached_mutable_work_count"],
        ownership["detached_snapshot_task_count"],
        ownership["mutable_work_count"],
        ownership["runtime_flow_count"],
        ownership["snapshot_operations"],
        ownership["snapshot_task_count"],
    )
    if (
        value["contract"] != WORKER_FIRST_COORDINATOR_CONTRACT
        or not isinstance(state, str)
        or state not in _COORDINATOR_STATES
        or type(started) is not bool
        or type(closed) is not bool
        or type(owns_heavy_lane) is not bool
        or type(in_flight) is not bool
        or (
            last_error_code is not None
            and (
                not isinstance(last_error_code, str)
                or last_error_code not in _COORDINATOR_ERROR_CODES
            )
        )
        or (
            last_disposition is not None
            and (
                not isinstance(last_disposition, str)
                or last_disposition not in {
                    "advanced",
                    "indeterminate",
                    "withheld",
                }
            )
        )
        or type(fence_held) is not bool
        or any(not _coordinator_counter(item) for item in ownership_counters)
        or type(ownership["snapshot_quiesced"]) is not bool
        or ownership["store_count"] != len(REQUIRED_STORE_ROLES)
        or not _coordinator_sha256(ownership["authority_scope_sha256"])
        or ownership["authority_sha256"] != expected_authority_sha256
        or not _coordinator_sha256(ownership["store_binding_sha256"])
        or any(
            isinstance(item, bool)
            or not isinstance(item, (int, float))
            or not math.isfinite(float(item))
            or float(item) <= 0.0
            for item in liveness.values()
        )
        or not isinstance(snapshot["state"], str)
        or snapshot["state"] not in _COORDINATOR_SNAPSHOT_STATES
        or (
            snapshot["last_error_code"] is not None
            and snapshot["last_error_code"] != "snapshot_runtime_error"
        )
    ):
        raise invalid
    ready = (
        state == "running"
        and started is True
        and closed is False
        and owns_heavy_lane is True
        and fence_held is True
        and last_error_code is None
        and snapshot["state"] == "running"
        and snapshot["last_error_code"] is None
    )
    return {
        "closed": closed,
        "fence_held": fence_held,
        "last_error_code": last_error_code,
        "owns_heavy_lane": owns_heavy_lane,
        "phase": phase,
        "ready": ready,
        "started": started,
        "state": state,
        "valid": True,
    }


def _coordinator_live_failure_code(proof: Mapping[str, Any]) -> str:
    if proof.get("valid") is not True:
        return "canary_coordinator_diagnostics_invalid"
    if proof.get("fence_held") is not True:
        return "canary_coordinator_fence_not_held"
    if proof.get("closed") is not False:
        return "canary_coordinator_closed"
    if proof.get("last_error_code") is not None:
        return "canary_coordinator_reported_error"
    return "canary_coordinator_not_ready"


@dataclass(frozen=True, slots=True, repr=False)
class WorkerFirstServiceSourcePaths:
    scheduler_db: Path
    capacity_ledger_db: Path
    execution_receipts_db: Path
    raw_store_root: Path

    def __post_init__(self) -> None:
        values = tuple(Path(value) for value in asdict(self).values())
        if any(not value.is_absolute() or value == Path(value.anchor) for value in values):
            raise WorkerFirstServiceCompositionConfigError(
                "service_source_path_invalid"
            )
        if len({str(value) for value in values}) != len(values):
            raise WorkerFirstServiceCompositionConfigError(
                "service_source_paths_alias"
            )

    def to_mapping(self) -> dict[str, str]:
        return {
            "capacity_ledger_db": str(self.capacity_ledger_db),
            "execution_receipts_db": str(self.execution_receipts_db),
            "raw_store_root": str(self.raw_store_root),
            "scheduler_db": str(self.scheduler_db),
        }


@dataclass(frozen=True, slots=True, repr=False)
class WorkerFirstServiceCompositionConfig:
    worker_first: WorkerFirstCompositionConfig
    source_paths: WorkerFirstServiceSourcePaths
    capacity_profile: RawCapacityProfile
    raw_store_limits: RawArtifactLimits
    acquisition_timeout_seconds: float = ACQUISITION_LANE_TIMEOUT_SECONDS
    bridge_timeout_seconds: float = 30.0
    stop_timeout_seconds: float = 6.0

    def __post_init__(self) -> None:
        if not isinstance(self.worker_first, WorkerFirstCompositionConfig):
            raise WorkerFirstServiceCompositionConfigError(
                "worker_first_config_invalid"
            )
        if not isinstance(self.source_paths, WorkerFirstServiceSourcePaths):
            raise WorkerFirstServiceCompositionConfigError(
                "source_paths_invalid"
            )
        if not isinstance(self.capacity_profile, RawCapacityProfile):
            raise WorkerFirstServiceCompositionConfigError(
                "capacity_profile_invalid"
            )
        if not isinstance(self.raw_store_limits, RawArtifactLimits):
            raise WorkerFirstServiceCompositionConfigError(
                "raw_store_limits_invalid"
            )
        _duration(
            self.acquisition_timeout_seconds,
            "acquisition_timeout_seconds",
            minimum=ACQUISITION_LANE_TIMEOUT_SECONDS,
            maximum=ACQUISITION_LANE_TIMEOUT_SECONDS,
        )
        _duration(
            self.bridge_timeout_seconds,
            "bridge_timeout_seconds",
            minimum=0.1,
            maximum=30.0,
        )
        _duration(
            self.stop_timeout_seconds,
            "stop_timeout_seconds",
            minimum=0.1,
            maximum=6.0,
        )
        base_paths = self.worker_first.paths.to_mapping().values()
        source_paths = self.source_paths.to_mapping().values()
        if {str(value) for value in base_paths}.intersection(source_paths):
            raise WorkerFirstServiceCompositionConfigError(
                "service_and_pipeline_paths_alias"
            )
        body = _canonical(self.to_mapping())
        if len(body) > MAX_CONFIG_BYTES:
            raise WorkerFirstServiceCompositionConfigError(
                "service_config_capacity_exceeded"
            )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "acquisition": {
                "capacity_profile": self.capacity_profile.to_mapping(),
                "paths": self.source_paths.to_mapping(),
                "raw_store_limits": asdict(self.raw_store_limits),
            },
            "contract": CONFIG_CONTRACT,
            "kmi_appv4_binding": KMI_APPV4_BINDING,
            "legacy_heavy_admission": "forbidden",
            "live_wiring": SERVICE_WIRING_ENABLED,
            "meteorological_admission": METEOROLOGICAL_ADMISSION,
            "mode": MODE,
            "retention": {
                "automatic_reclaim": False,
                "capacity_exhaustion": "fail_closed",
                "source_reclaim_authorized": False,
            },
            "timeouts": {
                "acquisition_seconds": float(self.acquisition_timeout_seconds),
                "bridge_seconds": float(self.bridge_timeout_seconds),
                "stop_seconds": float(self.stop_timeout_seconds),
            },
            "worker_first": self.worker_first.to_mapping(),
        }

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(_canonical(self.to_mapping())).hexdigest()


def _exact_canary_mapping(
    value: object,
    fields: frozenset[str],
    error_code: str,
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise WorkerFirstServiceCompositionConfigError(error_code)
    return dict(value)


def _canary_capacity_profile(value: object) -> RawCapacityProfile:
    mapping = _exact_canary_mapping(
        value,
        _CANARY_CAPACITY_PROFILE_FIELDS,
        "canary_capacity_profile_schema_invalid",
    )
    providers_value = mapping["providers"]
    if not isinstance(providers_value, list):
        raise WorkerFirstServiceCompositionConfigError(
            "canary_capacity_providers_invalid"
        )
    try:
        providers = tuple(
            ProviderCapacityLimit(**_exact_canary_mapping(
                provider,
                _CANARY_PROVIDER_FIELDS,
                "canary_capacity_provider_schema_invalid",
            ))
            for provider in providers_value
        )
        result = RawCapacityProfile(
            global_max_bytes=mapping["global_max_bytes"],
            global_max_entries=mapping["global_max_entries"],
            providers=providers,
            max_reservations=mapping["max_reservations"],
            max_catalog_bytes=mapping["max_catalog_bytes"],
            sqlite_busy_timeout_seconds=mapping[
                "sqlite_busy_timeout_seconds"
            ],
        )
    except WorkerFirstServiceCompositionConfigError:
        raise
    except (TypeError, ValueError):
        raise WorkerFirstServiceCompositionConfigError(
            "canary_capacity_profile_invalid"
        ) from None
    if result.to_mapping() != mapping:
        raise WorkerFirstServiceCompositionConfigError(
            "canary_capacity_profile_identity_mismatch"
        )
    return result


def _canary_raw_store_limits(value: object) -> RawArtifactLimits:
    mapping = _exact_canary_mapping(
        value,
        _CANARY_RAW_LIMIT_FIELDS,
        "canary_raw_store_limits_schema_invalid",
    )
    try:
        result = RawArtifactLimits(**mapping)
    except (TypeError, ValueError):
        raise WorkerFirstServiceCompositionConfigError(
            "canary_raw_store_limits_invalid"
        ) from None
    if asdict(result) != mapping:
        raise WorkerFirstServiceCompositionConfigError(
            "canary_raw_store_limits_identity_mismatch"
        )
    return result


def _canary_is_reparse(metadata: os.stat_result) -> bool:
    flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(
        flag
        and int(getattr(metadata, "st_file_attributes", 0)) & flag
    )


def _canary_path_identity(path: Path) -> str:
    value = str(path)
    if os.name == "nt":
        value = value.rstrip(". ")
    return os.path.normcase(value).casefold()


def _canary_path_parts(path: Path) -> tuple[str, ...]:
    return tuple(part.casefold() for part in path.parts)


def _canary_paths_overlap(left: Path, right: Path) -> bool:
    left_parts = _canary_path_parts(left)
    right_parts = _canary_path_parts(right)
    common = min(len(left_parts), len(right_parts))
    if common and left_parts[:common] == right_parts[:common]:
        return True
    try:
        return left.exists() and right.exists() and os.path.samefile(left, right)
    except OSError as error:
        raise WorkerFirstServiceCompositionConfigError(
            "canary_source_path_invalid"
        ) from error


def _canary_source_path(value: object) -> Path:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise WorkerFirstServiceCompositionConfigError(
            "canary_source_path_invalid"
        )
    path = Path(value)
    if str(path) != value:
        raise WorkerFirstServiceCompositionConfigError(
            "canary_source_path_not_canonical"
        )
    return path


def _validated_canary_path(path: Path) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise WorkerFirstServiceCompositionConfigError(
            "canary_source_path_invalid"
        )
    supplied = str(path)
    if (
        not supplied
        or len(supplied) > 4096
        or "\x00" in supplied
        or any(part in {".", ".."} for part in path.parts)
        or any(
            part.endswith((".", " "))
            for part in path.parts
            if part != path.anchor
        )
        or supplied != os.path.normpath(supplied)
    ):
        raise WorkerFirstServiceCompositionConfigError(
            "canary_source_path_not_canonical"
        )
    probe = Path(path.anchor)
    for part in path.parts[1:]:
        probe /= part
        try:
            present = probe.exists() or probe.is_symlink()
        except OSError as error:
            raise WorkerFirstServiceCompositionConfigError(
                "canary_source_path_invalid"
            ) from error
        if not present:
            break
        try:
            metadata = probe.lstat()
        except OSError as error:
            raise WorkerFirstServiceCompositionConfigError(
                "canary_source_path_invalid"
            ) from error
        if probe.is_symlink() or _canary_is_reparse(metadata):
            raise WorkerFirstServiceCompositionConfigError(
                "canary_source_path_unsafe"
            )
    try:
        resolved = path.resolve(strict=False)
    except (OSError, RuntimeError) as error:
        raise WorkerFirstServiceCompositionConfigError(
            "canary_source_path_invalid"
        ) from error
    if _canary_path_identity(path) != _canary_path_identity(resolved):
        raise WorkerFirstServiceCompositionConfigError(
            "canary_source_path_not_canonical"
        )
    return resolved


def _validate_canary_path_set(config: "WorkerFirstServiceCanaryConfig") -> None:
    source_paths = tuple(
        _validated_canary_path(getattr(config.source_paths, name))
        for name in sorted(_CANARY_SOURCE_PATH_FIELDS)
    )
    try:
        pipeline_paths = tuple(
            _validated_canary_path(Path(value))
            for value in config.worker_first.paths.to_mapping().values()
        )
    except (OSError, RuntimeError) as error:
        raise WorkerFirstServiceCompositionConfigError(
            "canary_pipeline_path_invalid"
        ) from error
    source_identities = tuple(
        _canary_path_identity(path) for path in source_paths
    )
    if (
        len(set(source_identities)) != len(source_identities)
        or any(
            _canary_paths_overlap(left, right)
            for index, left in enumerate(source_paths)
            for right in source_paths[index + 1:]
        )
        or any(
            _canary_paths_overlap(source, pipeline)
            for source in source_paths
            for pipeline in pipeline_paths
        )
    ):
        raise WorkerFirstServiceCompositionConfigError(
            "canary_source_paths_alias"
        )


@dataclass(frozen=True, slots=True, repr=False)
class WorkerFirstServiceCanaryConfig:
    """Closed v2 authority for one explicitly selected DWD-only canary."""

    worker_first: WorkerFirstCompositionConfig
    source_paths: WorkerFirstServiceSourcePaths
    capacity_profile: RawCapacityProfile
    raw_store_limits: RawArtifactLimits
    allowed_workload_ids: tuple[str, ...] = CANARY_ALLOWED_WORKLOAD_IDS
    acquisition_timeout_seconds: float = ACQUISITION_LANE_TIMEOUT_SECONDS
    bridge_timeout_seconds: float = 30.0
    stop_timeout_seconds: float = 6.0

    def __post_init__(self) -> None:
        if (
            isinstance(self.worker_first, WorkerFirstCompositionConfig)
            and isinstance(
                self.source_paths,
                WorkerFirstServiceSourcePaths,
            )
        ):
            _validate_canary_path_set(self)
        # Reuse the complete v1 type/path/timeout/resource validation without
        # changing v1's canonical mapping or fingerprint.
        WorkerFirstServiceCompositionConfig(
            worker_first=self.worker_first,
            source_paths=self.source_paths,
            capacity_profile=self.capacity_profile,
            raw_store_limits=self.raw_store_limits,
            acquisition_timeout_seconds=self.acquisition_timeout_seconds,
            bridge_timeout_seconds=self.bridge_timeout_seconds,
            stop_timeout_seconds=self.stop_timeout_seconds,
        )
        allowed = self.allowed_workload_ids
        if (
            type(allowed) is not tuple
            or any(type(value) is not str for value in allowed)
            or allowed != tuple(sorted(set(allowed)))
            or allowed != CANARY_ALLOWED_WORKLOAD_IDS
        ):
            raise WorkerFirstServiceCompositionConfigError(
                "canary_allowed_workload_ids_invalid"
            )
        if len(_canonical(self.to_mapping())) > MAX_CONFIG_BYTES:
            raise WorkerFirstServiceCompositionConfigError(
                "canary_service_config_capacity_exceeded"
            )

    @classmethod
    def from_mapping(cls, value: object) -> "WorkerFirstServiceCanaryConfig":
        mapping = _exact_canary_mapping(
            value,
            _CANARY_SERVICE_FIELDS,
            "canary_service_config_schema_invalid",
        )
        acquisition = _exact_canary_mapping(
            mapping["acquisition"],
            _CANARY_ACQUISITION_FIELDS,
            "canary_acquisition_schema_invalid",
        )
        source_paths = _exact_canary_mapping(
            acquisition["paths"],
            _CANARY_SOURCE_PATH_FIELDS,
            "canary_source_paths_schema_invalid",
        )
        retention = _exact_canary_mapping(
            mapping["retention"],
            _CANARY_RETENTION_FIELDS,
            "canary_retention_schema_invalid",
        )
        timeouts = _exact_canary_mapping(
            mapping["timeouts"],
            _CANARY_TIMEOUT_FIELDS,
            "canary_timeouts_schema_invalid",
        )
        allowed_value = acquisition["allowed_workload_ids"]
        if not isinstance(allowed_value, list):
            raise WorkerFirstServiceCompositionConfigError(
                "canary_allowed_workload_ids_invalid"
            )
        if (
            mapping["contract"] != CANARY_CONFIG_CONTRACT
            or mapping["kmi_appv4_binding"] != KMI_APPV4_BINDING
            or mapping["legacy_heavy_admission"] != "forbidden"
            or mapping["live_wiring"] is not CANARY_SERVICE_WIRING_ENABLED
            or mapping["meteorological_admission"] is not False
            or mapping["mode"] != CANARY_MODE
            or mapping["production_enable_ready"] is not False
            or retention != {
                "automatic_reclaim": False,
                "capacity_exhaustion": "fail_closed",
                "source_reclaim_authorized": False,
            }
        ):
            raise WorkerFirstServiceCompositionConfigError(
                "canary_service_config_authority_invalid"
            )
        try:
            result = cls(
                worker_first=WorkerFirstCompositionConfig.from_mapping(
                    mapping["worker_first"]
                ),
                source_paths=WorkerFirstServiceSourcePaths(**{
                    name: _canary_source_path(source_paths[name])
                    for name in _CANARY_SOURCE_PATH_FIELDS
                }),
                capacity_profile=_canary_capacity_profile(
                    acquisition["capacity_profile"]
                ),
                raw_store_limits=_canary_raw_store_limits(
                    acquisition["raw_store_limits"]
                ),
                allowed_workload_ids=tuple(allowed_value),
                acquisition_timeout_seconds=timeouts[
                    "acquisition_seconds"
                ],
                bridge_timeout_seconds=timeouts["bridge_seconds"],
                stop_timeout_seconds=timeouts["stop_seconds"],
            )
        except WorkerFirstServiceCompositionConfigError:
            raise
        except (WorkerFirstCompositionConfigError, TypeError, ValueError):
            raise WorkerFirstServiceCompositionConfigError(
                "canary_service_config_invalid"
            ) from None
        if result.to_mapping() != mapping:
            raise WorkerFirstServiceCompositionConfigError(
                "canary_service_config_identity_mismatch"
            )
        return result

    def to_mapping(self) -> dict[str, Any]:
        return {
            "acquisition": {
                "allowed_workload_ids": list(self.allowed_workload_ids),
                "capacity_profile": self.capacity_profile.to_mapping(),
                "paths": self.source_paths.to_mapping(),
                "raw_store_limits": asdict(self.raw_store_limits),
            },
            "contract": CANARY_CONFIG_CONTRACT,
            "kmi_appv4_binding": KMI_APPV4_BINDING,
            "legacy_heavy_admission": "forbidden",
            "live_wiring": CANARY_SERVICE_WIRING_ENABLED,
            "meteorological_admission": False,
            "mode": CANARY_MODE,
            "production_enable_ready": CANARY_PRODUCTION_ENABLE_READY,
            "retention": {
                "automatic_reclaim": False,
                "capacity_exhaustion": "fail_closed",
                "source_reclaim_authorized": False,
            },
            "timeouts": {
                "acquisition_seconds": float(
                    self.acquisition_timeout_seconds
                ),
                "bridge_seconds": float(self.bridge_timeout_seconds),
                "stop_seconds": float(self.stop_timeout_seconds),
            },
            "worker_first": self.worker_first.to_mapping(),
        }

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(_canonical(self.to_mapping())).hexdigest()


class WorkerFirstServiceRuntime:
    """One serialized acquisition/science/publication service lane."""

    def __init__(
        self,
        *,
        acquisition: AcquisitionShadowRuntime,
        bridge: DwdAcquisitionCompletionBridge,
        coordinator: WorkerFirstRuntimeCoordinator,
        config_sha256: str,
        authority_sha256: str,
        acquisition_timeout_seconds: float,
        bridge_timeout_seconds: float,
        stop_timeout_seconds: float,
    ) -> None:
        if not isinstance(acquisition, AcquisitionShadowRuntime):
            raise TypeError("acquisition runtime invalid")
        if not isinstance(bridge, DwdAcquisitionCompletionBridge):
            raise TypeError("completion bridge invalid")
        if not isinstance(coordinator, WorkerFirstRuntimeCoordinator):
            raise TypeError("worker-first coordinator invalid")
        self._acquisition = acquisition
        self._bridge = bridge
        self._coordinator = coordinator
        self._config_sha256 = config_sha256
        self._authority_sha256 = authority_sha256
        self._acquisition_timeout = acquisition_timeout_seconds
        self._bridge_timeout = bridge_timeout_seconds
        self._stop_timeout = stop_timeout_seconds
        self._gate = asyncio.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="stv3-service-lane",
        )
        self._executor_closed = False
        self._active_future: Future[Any] | None = None
        self._mutable_lock = threading.Lock()
        self._started = False
        self._stop_requested = False
        self._state = "new"
        self._last_error_code: str | None = None
        self._last_acquisition = ShadowTickResult(
            "idle", "not_started", None, None, None, None, False
        )
        self._last_bridge = DwdCompletionBridgeResult(
            "idle", "not_started", 0
        )
        self._last_coordinator_stop = _coordinator_stop_proof(None)
        self._counts = {
            "start_calls": 0,
            "ticks": 0,
            "acquisition_ticks": 0,
            "bridge_calls": 0,
            "coordinator_ticks": 0,
            "stop_calls": 0,
            "timeouts": 0,
            "failures": 0,
        }

    def _increment(self, name: str) -> None:
        self._counts[name] = min(MAX_COUNTER, self._counts[name] + 1)

    def _accept_coordinator_live_diagnostics(
        self,
        value: object,
        *,
        phase: str,
    ) -> None:
        # Version 1 intentionally retains its exact behavior.  Version 2
        # overrides this hook with its closed coordinator proof contract.
        return None

    def _preflight_coordinator_live(self, *, phase: str) -> None:
        # Version 1 deliberately performs no additional coordinator read.
        return None

    def _accept_current_coordinator_live_diagnostics(
        self,
        *,
        phase: str,
    ) -> None:
        # Version 1 deliberately performs no additional coordinator read.
        return None

    def _prepare_coordinator_tick_diagnostics(
        self,
        value: object,
        *,
        phase: str,
    ) -> None:
        # Version 1 deliberately performs no additional coordinator read.
        return None

    async def _blocking(
        self,
        function: Callable[[], Any],
        *,
        timeout_seconds: float,
        timeout_code: str,
    ) -> Any:
        if self._executor_closed:
            raise WorkerFirstServiceCompositionError("service_lane_closed")
        loop = asyncio.get_running_loop()
        future = self._executor.submit(function)
        with self._mutable_lock:
            self._active_future = future
        try:
            return await asyncio.wait_for(
                asyncio.shield(asyncio.wrap_future(future, loop=loop)),
                timeout=timeout_seconds,
            )
        except TimeoutError as error:
            self._increment("timeouts")
            self._acquisition.request_stop()
            raise WorkerFirstServiceCompositionError(timeout_code) from error
        finally:
            with self._mutable_lock:
                if self._active_future is future and future.done():
                    self._active_future = None

    def _resource_preflight(self) -> None:
        acquisition = self._acquisition.diagnostics()
        bridge = self._bridge.diagnostics()
        capacity = acquisition.get("capacity")
        if (
            not isinstance(capacity, Mapping)
            or capacity.get("global_max_bytes", 0) <= 0
            or capacity.get("global_max_entries", 0) <= 0
            or capacity.get("committed_bytes", -1) < 0
            or capacity.get("committed_entries", -1) < 0
            or bridge.get("source_reclaim_authorized") is not False
        ):
            raise WorkerFirstServiceCompositionIntegrityError(
                "service_resource_envelope_invalid"
            )

    async def async_start(self) -> dict[str, Any]:
        async with self._gate:
            self._increment("start_calls")
            if self._started:
                return self.diagnostics()
            if self._stop_requested:
                raise WorkerFirstServiceCompositionError("service_runtime_stopping")
            self._state = "starting"
            try:
                await self._blocking(
                    self._resource_preflight,
                    timeout_seconds=self._bridge_timeout,
                    timeout_code="resource_preflight_timeout",
                )
                coordinator_start = await self._coordinator.async_start()
                self._accept_coordinator_live_diagnostics(
                    coordinator_start,
                    phase="start_result",
                )
                self._accept_current_coordinator_live_diagnostics(
                    phase="start_current",
                )
                # Recover at most one pre-existing source terminal before new
                # acquisition is admitted. Remaining bounded pages are drained
                # one per service tick under the same lane.
                self._last_bridge = await self._blocking(
                    self._bridge.publish_next,
                    timeout_seconds=self._bridge_timeout,
                    timeout_code="completion_bridge_timeout",
                )
                self._increment("bridge_calls")
                self._started = True
                self._state = "running"
                self._last_error_code = None
            except Exception:
                self._increment("failures")
                self._state = "error"
                self._last_error_code = "service_start_failed"
                try:
                    await self._coordinator.async_stop()
                except Exception:
                    pass
                raise
            return self.diagnostics()

    async def async_tick(self) -> dict[str, Any]:
        if not self._started or self._stop_requested:
            raise WorkerFirstServiceCompositionError("service_runtime_not_running")
        async with self._gate:
            if self._stop_requested:
                raise WorkerFirstServiceCompositionError("service_runtime_stopping")
            self._preflight_coordinator_live(phase="tick_current")
            self._increment("ticks")
            try:
                self._last_bridge = await self._blocking(
                    self._bridge.publish_next,
                    timeout_seconds=self._bridge_timeout,
                    timeout_code="completion_bridge_timeout",
                )
                self._increment("bridge_calls")
                coordinator_before = self._coordinator.diagnostics()
                before = coordinator_before.get("counts", {})
                self._prepare_coordinator_tick_diagnostics(
                    coordinator_before,
                    phase="tick_current",
                )
                coordinator_tick = await self._coordinator.async_tick()
                self._accept_coordinator_live_diagnostics(
                    coordinator_tick,
                    phase="tick_result",
                )
                self._increment("coordinator_ticks")
                coordinator_current = self._coordinator.diagnostics()
                self._accept_coordinator_live_diagnostics(
                    coordinator_current,
                    phase="tick_current",
                )
                after = coordinator_current.get("counts", {})
                coordinator_advanced = any(
                    int(after.get(name, 0)) > int(before.get(name, 0))
                    for name in ("leases", "processed", "terminalized")
                )
                if not coordinator_advanced and self._last_bridge.status != "published":
                    self._last_acquisition = await self._blocking(
                        self._acquisition.tick,
                        timeout_seconds=self._acquisition_timeout,
                        timeout_code="acquisition_tick_timeout",
                    )
                    self._increment("acquisition_ticks")
                    if (
                        self._last_acquisition.terminal
                        and self._last_acquisition.status
                        in {"completed_shadow_raw", "completion_replayed"}
                    ):
                        self._last_bridge = await self._blocking(
                            self._bridge.publish_next,
                            timeout_seconds=self._bridge_timeout,
                            timeout_code="completion_bridge_timeout",
                        )
                        self._increment("bridge_calls")
                self._state = "running"
                self._last_error_code = None
            except Exception:
                self._increment("failures")
                self._state = "degraded"
                self._last_error_code = "service_tick_failed"
                raise
            return self.diagnostics()

    async def async_stop(self) -> dict[str, Any]:
        self._increment("stop_calls")
        self._stop_requested = True
        self._acquisition.request_stop()
        if (
            self._executor_closed
            and self._last_coordinator_stop["terminal"] is True
        ):
            return self.diagnostics()
        try:
            await asyncio.wait_for(self._gate.acquire(), timeout=self._stop_timeout)
        except TimeoutError:
            self._increment("timeouts")
            self._state = "stop_pending"
            self._last_error_code = "service_lane_stop_timeout"
            return self.diagnostics()
        try:
            try:
                coordinator_stop = await self._coordinator.async_stop()
                self._last_coordinator_stop = _coordinator_stop_proof(
                    coordinator_stop
                )
            except asyncio.CancelledError:
                self._last_coordinator_stop = _coordinator_stop_proof(None)
                self._state = "stop_pending"
                self._last_error_code = "coordinator_stop_cancelled"
                raise
            except Exception:
                self._last_coordinator_stop = _coordinator_stop_proof(None)
                self._state = "stop_pending"
                self._last_error_code = "coordinator_stop_failed"
                raise
            with self._mutable_lock:
                active = self._active_future
            coordinator_terminal = self._last_coordinator_stop[
                "terminal"
            ] is True
            if (
                (active is None or active.done())
                and coordinator_terminal
            ):
                self._executor.shutdown(wait=True, cancel_futures=True)
                self._executor_closed = True
                self._state = "stopped"
                self._last_error_code = None
            elif active is None or active.done():
                self._state = "stop_pending"
                self._last_error_code = "coordinator_stop_pending"
            else:
                self._state = "stop_pending"
                self._last_error_code = "service_lane_still_active"
        finally:
            self._gate.release()
        return self.diagnostics()

    def diagnostics(self) -> dict[str, Any]:
        with self._mutable_lock:
            active = self._active_future is not None and not self._active_future.done()
        result = {
            "authority_sha256": self._authority_sha256,
            "config_sha256": self._config_sha256,
            "contract": RUNTIME_CONTRACT,
            "counts": dict(self._counts),
            "dwd_fact_binding": "absent_factless",
            "heavy_lane_active": active,
            "kmi_appv4_binding": KMI_APPV4_BINDING,
            "last_acquisition": self._last_acquisition.as_dict(),
            "last_bridge": self._last_bridge.as_dict(),
            "last_error_code": self._last_error_code,
            "legacy_heavy_admission": "forbidden",
            "live_wiring": SERVICE_WIRING_ENABLED,
            "meteorological_admission": False,
            "mode": MODE,
            "production_enable_ready": False,
            "retention": {
                "automatic_reclaim": False,
                "capacity_exhaustion": "fail_closed",
                "source_reclaim_authorized": False,
            },
            "state": self._state,
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise WorkerFirstServiceCompositionError(
                "service_diagnostics_capacity_exceeded"
            )
        return result


class WorkerFirstServiceCanaryRuntime(WorkerFirstServiceRuntime):
    """Versioned live-local wrapper around the still-shadow science lane."""

    def __init__(
        self,
        *,
        acquisition: AcquisitionShadowRuntime,
        bridge: DwdAcquisitionCompletionBridge,
        coordinator: WorkerFirstRuntimeCoordinator,
        config_sha256: str,
        authority_sha256: str,
        allowed_workload_ids: tuple[str, ...],
        acquisition_timeout_seconds: float,
        bridge_timeout_seconds: float,
        stop_timeout_seconds: float,
    ) -> None:
        if allowed_workload_ids != CANARY_ALLOWED_WORKLOAD_IDS:
            raise WorkerFirstServiceCompositionConfigError(
                "canary_runtime_allowed_workload_ids_invalid"
            )
        super().__init__(
            acquisition=acquisition,
            bridge=bridge,
            coordinator=coordinator,
            config_sha256=config_sha256,
            authority_sha256=authority_sha256,
            acquisition_timeout_seconds=acquisition_timeout_seconds,
            bridge_timeout_seconds=bridge_timeout_seconds,
            stop_timeout_seconds=stop_timeout_seconds,
        )
        self._canary_allowed_workload_ids = allowed_workload_ids
        self._last_coordinator_live = (
            _unobserved_coordinator_live_proof()
        )

    def _accept_coordinator_live_diagnostics(
        self,
        value: object,
        *,
        phase: str,
    ) -> None:
        try:
            proof = _validated_coordinator_live_proof(
                value,
                expected_authority_sha256=self._authority_sha256,
                phase=phase,
            )
        except WorkerFirstServiceCompositionIntegrityError:
            self._last_coordinator_live = (
                _unobserved_coordinator_live_proof(phase)
            )
            raise
        self._last_coordinator_live = proof
        if proof["ready"] is not True:
            raise WorkerFirstServiceCompositionIntegrityError(
                _coordinator_live_failure_code(proof)
            )

    def _preflight_coordinator_live(self, *, phase: str) -> None:
        try:
            value = self._coordinator.diagnostics()
        except Exception:
            self._last_coordinator_live = (
                _unobserved_coordinator_live_proof(phase)
            )
            raise WorkerFirstServiceCompositionIntegrityError(
                "canary_coordinator_diagnostics_invalid"
            ) from None
        self._accept_coordinator_live_diagnostics(
            value,
            phase=phase,
        )

    def _accept_current_coordinator_live_diagnostics(
        self,
        *,
        phase: str,
    ) -> None:
        self._preflight_coordinator_live(phase=phase)

    def _prepare_coordinator_tick_diagnostics(
        self,
        value: object,
        *,
        phase: str,
    ) -> None:
        self._accept_coordinator_live_diagnostics(value, phase=phase)

    def _current_coordinator_live_proof(self) -> dict[str, Any]:
        try:
            value = self._coordinator.diagnostics()
            proof = _validated_coordinator_live_proof(
                value,
                expected_authority_sha256=self._authority_sha256,
                phase="diagnostic",
            )
        except Exception:
            proof = _unobserved_coordinator_live_proof("diagnostic")
        self._last_coordinator_live = proof
        return dict(proof)

    def diagnostics(self) -> dict[str, Any]:
        result = super().diagnostics()
        stop_proof = dict(self._last_coordinator_stop)
        stopping = (
            self._stop_requested
            or self._state in {"stop_pending", "stopped", "stopping"}
        )
        live_proof = (
            dict(self._last_coordinator_live)
            if stopping
            else self._current_coordinator_live_proof()
        )
        live_ready = live_proof["ready"] is True and not stopping
        if result["state"] in {"degraded", "running"} and not live_ready:
            result["state"] = "degraded"
            if result["last_error_code"] is None:
                result["last_error_code"] = (
                    _coordinator_live_failure_code(live_proof)
                )
        result.update({
            "acquisition_admission": {
                "allowed_workload_ids": list(
                    self._canary_allowed_workload_ids
                ),
                "policy": CANARY_ADMISSION_POLICY,
            },
            "contract": CANARY_RUNTIME_CONTRACT,
            "coordinator_live": live_proof,
            "live_wiring": CANARY_SERVICE_WIRING_ENABLED,
            "meteorological_admission": False,
            "mode": CANARY_MODE,
            "ownership_fence_held": (
                stop_proof["fence_held"] is True
                if stopping
                else live_proof["fence_held"] is True
            ),
            "production_enable_ready": CANARY_PRODUCTION_ENABLE_READY,
            "science_mode": CANARY_SCIENCE_MODE,
            "shutdown_complete": (
                self._state == "stopped"
                and self._executor_closed
                and stop_proof["terminal"] is True
                and stop_proof["fence_held"] is False
            ),
            "coordinator_stop": stop_proof,
        })
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise WorkerFirstServiceCompositionError(
                "canary_service_diagnostics_capacity_exceeded"
            )
        return result


def build_worker_first_service_composition(
    config: WorkerFirstServiceCompositionConfig,
    *,
    expected_config_sha256: str,
    expected_authority_sha256: str,
    publisher: Callable[[bytes], Any],
    status_supplier: Callable[[], Mapping[str, Any]],
    secret_resolver: SecretResolver | None = None,
    executor_factory: ExecutorFactory = _default_executor_factory,
    clock: Callable[[], float] = time.time,
) -> WorkerFirstServiceRuntime:
    """Build the concrete candidate while keeping service main untouched."""

    if not isinstance(config, WorkerFirstServiceCompositionConfig):
        raise WorkerFirstServiceCompositionConfigError("service_config_type_invalid")
    if config.fingerprint != expected_config_sha256:
        raise WorkerFirstServiceCompositionConfigError(
            "service_config_checksum_mismatch"
        )
    if not callable(publisher) or not callable(status_supplier) or not callable(clock):
        raise WorkerFirstServiceCompositionConfigError(
            "service_runtime_callable_invalid"
        )
    base = config.worker_first
    _validate_config_pin(base, base.fingerprint)
    fence = None
    lease = None
    try:
        fence = WorkerFirstOwnershipFence(
            base.paths.authority_file,
            expected_authority_sha256=expected_authority_sha256,
        )
        configured = {
            "acquisition_completion_store": base.paths.acquisition_completion_root,
            "desired_state_store": base.paths.desired_state_db,
            "operational_fact_store": base.paths.operational_fact_db,
            "operational_snapshot_store": base.paths.operational_snapshot_db,
            "pipeline_output_store": base.paths.pipeline_output_db,
            "snapshot_outbox_store": base.paths.snapshot_outbox_db,
        }
        if set(configured) != set(REQUIRED_STORE_ROLES) or any(
            not _same_path(configured[role], fence.binding.store(role).path)
            for role in REQUIRED_STORE_ROLES
        ):
            raise WorkerFirstServiceCompositionIntegrityError(
                "service_authority_path_mismatch"
            )
        parts = _construct_store_parts(base, clock=clock)
        bindings = _store_bindings(base, parts, config_sha256=base.fingerprint)
        fence.binding.assert_coordinator_binding(
            consumer_id=CONSUMER_ID,
            stream_id=STREAM_ID,
            stores=bindings,
        )
        lease = fence.acquire()
        source_paths = ShadowRuntimePaths(
            desired_state=base.paths.desired_state_db,
            scheduler=config.source_paths.scheduler_db,
            capacity_ledger=config.source_paths.capacity_ledger_db,
            execution_receipts=config.source_paths.execution_receipts_db,
            raw_store_root=config.source_paths.raw_store_root,
        )
        acquisition = create_shadow_candidate(
            source_paths,
            capacity_profile=config.capacity_profile,
            raw_store_limits=config.raw_store_limits,
            secret_resolver=secret_resolver,
            executor_factory=executor_factory,
            clock=clock,
        )
        bridge = DwdAcquisitionCompletionBridge(
            lifecycle_store=parts.completion,
            desired_state_store=acquisition.desired_state_store,
            scheduler=acquisition.scheduler,
            execution_store=acquisition.execution_store,
            capacity_ledger=acquisition.capacity_ledger,
            source_raw_store=acquisition.raw_store,
            handoff_raw_store=parts.handoff,
        )
        snapshots = OperationalSnapshotRuntime(
            publisher=publisher,
            status_supplier=status_supplier,
            desired_state_store=parts.desired,
            snapshot_store=parts.snapshots,
            outbox_store=parts.outbox,
            fact_revision_store=parts.facts,
            clock=clock,
        )
        coordinator = WorkerFirstRuntimeCoordinator(
            desired_state_store=parts.desired,
            completion_lifecycle=parts.completion,
            fact_revision_lifecycle=parts.facts,
            snapshot_lifecycle=snapshots,
            processor=parts.consumer.process_leased,
            outcome_authority=parts.outcome_authority,
            ownership_lease=lease,
            store_bindings=bindings,
            consumer_id=CONSUMER_ID,
            stream_id=STREAM_ID,
            owner_id=base.owner_id,
            lease_seconds=base.lease_seconds,
            processing_timeout_seconds=base.processing_timeout_seconds,
            cancel_grace_seconds=base.cancel_grace_seconds,
            control_timeout_seconds=base.control_timeout_seconds,
            stop_timeout_seconds=base.stop_timeout_seconds,
            clock=clock,
        )
        return WorkerFirstServiceRuntime(
            acquisition=acquisition,
            bridge=bridge,
            coordinator=coordinator,
            config_sha256=config.fingerprint,
            authority_sha256=expected_authority_sha256,
            acquisition_timeout_seconds=config.acquisition_timeout_seconds,
            bridge_timeout_seconds=config.bridge_timeout_seconds,
            stop_timeout_seconds=config.stop_timeout_seconds,
        )
    except WorkerFirstFenceBusy:
        raise WorkerFirstCompositionBusy("service_composition_owner_busy") from None
    except (
        WorkerFirstServiceCompositionError,
        WorkerFirstCompositionConfigError,
        WorkerFirstCompositionIntegrityError,
    ):
        if lease is not None:
            try:
                lease.release()
            except WorkerFirstOwnershipError:
                pass
        raise
    except Exception:
        if lease is not None:
            try:
                lease.release()
            except WorkerFirstOwnershipError:
                pass
        raise WorkerFirstCompositionError(
            "service_composition_construction_failed"
        ) from None


def build_worker_first_service_canary(
    config: WorkerFirstServiceCanaryConfig,
    *,
    expected_config_sha256: str,
    expected_authority_sha256: str,
    publisher: Callable[[bytes], Any],
    status_supplier: Callable[[], Mapping[str, Any]],
    secret_resolver: SecretResolver | None = None,
    executor_factory: ExecutorFactory = _dwd_canary_executor_factory,
    clock: Callable[[], float] = time.time,
) -> WorkerFirstServiceCanaryRuntime:
    """Build the explicitly pinned DWD-only local canary contract."""

    if not isinstance(config, WorkerFirstServiceCanaryConfig):
        raise WorkerFirstServiceCompositionConfigError(
            "canary_service_config_type_invalid"
        )
    if config.fingerprint != expected_config_sha256:
        raise WorkerFirstServiceCompositionConfigError(
            "canary_service_config_checksum_mismatch"
        )
    if not callable(publisher) or not callable(status_supplier) or not callable(clock):
        raise WorkerFirstServiceCompositionConfigError(
            "canary_service_runtime_callable_invalid"
        )
    _validate_canary_path_set(config)
    base = config.worker_first
    _validate_config_pin(base, base.fingerprint)
    fence = None
    lease = None
    try:
        fence = WorkerFirstOwnershipFence(
            base.paths.authority_file,
            expected_authority_sha256=expected_authority_sha256,
        )
        configured = {
            "acquisition_completion_store": base.paths.acquisition_completion_root,
            "desired_state_store": base.paths.desired_state_db,
            "operational_fact_store": base.paths.operational_fact_db,
            "operational_snapshot_store": base.paths.operational_snapshot_db,
            "pipeline_output_store": base.paths.pipeline_output_db,
            "snapshot_outbox_store": base.paths.snapshot_outbox_db,
        }
        if set(configured) != set(REQUIRED_STORE_ROLES) or any(
            not _same_path(configured[role], fence.binding.store(role).path)
            for role in REQUIRED_STORE_ROLES
        ):
            raise WorkerFirstServiceCompositionIntegrityError(
                "canary_service_authority_path_mismatch"
            )
        # The explicit canary must never open, initialize or audit mutable
        # stores unless it already owns the process/OS authority fence.
        lease = fence.acquire()
        parts = _construct_store_parts(base, clock=clock)
        bindings = _store_bindings(base, parts, config_sha256=base.fingerprint)
        fence.binding.assert_coordinator_binding(
            consumer_id=CONSUMER_ID,
            stream_id=STREAM_ID,
            stores=bindings,
        )
        source_paths = ShadowRuntimePaths(
            desired_state=base.paths.desired_state_db,
            scheduler=config.source_paths.scheduler_db,
            capacity_ledger=config.source_paths.capacity_ledger_db,
            execution_receipts=config.source_paths.execution_receipts_db,
            raw_store_root=config.source_paths.raw_store_root,
        )
        allowed_workloads = frozenset(config.allowed_workload_ids)
        acquisition = create_shadow_candidate(
            source_paths,
            capacity_profile=config.capacity_profile,
            raw_store_limits=config.raw_store_limits,
            secret_resolver=secret_resolver,
            executor_factory=executor_factory,
            allowed_workload_ids=allowed_workloads,
            clock=clock,
        )
        bridge = DwdAcquisitionCompletionBridge(
            lifecycle_store=parts.completion,
            desired_state_store=acquisition.desired_state_store,
            scheduler=acquisition.scheduler,
            execution_store=acquisition.execution_store,
            capacity_ledger=acquisition.capacity_ledger,
            source_raw_store=acquisition.raw_store,
            handoff_raw_store=parts.handoff,
        )
        snapshots = OperationalSnapshotRuntime(
            publisher=publisher,
            status_supplier=status_supplier,
            desired_state_store=parts.desired,
            snapshot_store=parts.snapshots,
            outbox_store=parts.outbox,
            fact_revision_store=parts.facts,
            clock=clock,
        )
        coordinator = WorkerFirstRuntimeCoordinator(
            desired_state_store=parts.desired,
            completion_lifecycle=parts.completion,
            fact_revision_lifecycle=parts.facts,
            snapshot_lifecycle=snapshots,
            processor=parts.consumer.process_leased,
            outcome_authority=parts.outcome_authority,
            ownership_lease=lease,
            store_bindings=bindings,
            consumer_id=CONSUMER_ID,
            stream_id=STREAM_ID,
            owner_id=base.owner_id,
            lease_seconds=base.lease_seconds,
            processing_timeout_seconds=base.processing_timeout_seconds,
            cancel_grace_seconds=base.cancel_grace_seconds,
            control_timeout_seconds=base.control_timeout_seconds,
            stop_timeout_seconds=base.stop_timeout_seconds,
            clock=clock,
        )
        return WorkerFirstServiceCanaryRuntime(
            acquisition=acquisition,
            bridge=bridge,
            coordinator=coordinator,
            config_sha256=config.fingerprint,
            authority_sha256=expected_authority_sha256,
            allowed_workload_ids=config.allowed_workload_ids,
            acquisition_timeout_seconds=config.acquisition_timeout_seconds,
            bridge_timeout_seconds=config.bridge_timeout_seconds,
            stop_timeout_seconds=config.stop_timeout_seconds,
        )
    except WorkerFirstFenceBusy:
        raise WorkerFirstCompositionBusy(
            "canary_service_composition_owner_busy"
        ) from None
    except (
        WorkerFirstServiceCompositionError,
        WorkerFirstCompositionConfigError,
        WorkerFirstCompositionIntegrityError,
    ):
        if lease is not None:
            try:
                lease.release()
            except WorkerFirstOwnershipError:
                pass
        raise
    except Exception:
        if lease is not None:
            try:
                lease.release()
            except WorkerFirstOwnershipError:
                pass
        raise WorkerFirstCompositionError(
            "canary_service_composition_construction_failed"
        ) from None


__all__ = [
    "ACQUISITION_LANE_TIMEOUT_SECONDS",
    "CONFIG_CONTRACT",
    "KMI_APPV4_BINDING",
    "METEOROLOGICAL_ADMISSION",
    "MODE",
    "RUNTIME_CONTRACT",
    "SERVICE_WIRING_ENABLED",
    "WorkerFirstServiceCompositionConfig",
    "WorkerFirstServiceCompositionConfigError",
    "WorkerFirstServiceCompositionError",
    "WorkerFirstServiceCompositionIntegrityError",
    "WorkerFirstServiceRuntime",
    "WorkerFirstServiceSourcePaths",
    "build_worker_first_service_composition",
    "CANARY_ADMISSION_POLICY",
    "CANARY_ALLOWED_WORKLOAD_IDS",
    "CANARY_CONFIG_CONTRACT",
    "CANARY_MODE",
    "CANARY_PRODUCTION_ENABLE_READY",
    "CANARY_RUNTIME_CONTRACT",
    "CANARY_SCIENCE_MODE",
    "CANARY_SERVICE_WIRING_ENABLED",
    "WorkerFirstServiceCanaryConfig",
    "WorkerFirstServiceCanaryRuntime",
    "build_worker_first_service_canary",
]
