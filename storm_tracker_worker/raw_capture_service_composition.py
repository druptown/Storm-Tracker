"""Worker-only, output-free service composition for exact raw acquisition.

This mode deliberately separates collection from meteorological admission.
It accepts only the closed activation matrix, stores exact provider bytes in
the raw artifact store, and exposes three desired-state control jobs.  It does
not decode, calculate, publish dashboard facts, or put raw payloads on MQTT.
"""
from __future__ import annotations

import asyncio
from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable, Final

from .acquisition_activation import (
    ACTIVATION_RULES,
    EnvironmentSecretResolver,
    SecretResolver,
)
from .acquisition_runtime import (
    AcquisitionShadowRuntime,
    EumetsatAccessTokenBroker,
    ExecutorFactory,
    NetatmoAccessTokenBroker,
    ShadowRuntimePaths,
    ShadowTickResult,
    create_shadow_candidate,
    raw_capture_executor_factory,
)
from .autonomous_scheduler import SchedulerDesiredStateError
from .blitzortung_event_ingress import (
    ACCESS_RETENTION_AUTHORITY_ID,
    LIMITS_SCHEMA as BLITZORTUNG_LIMITS_SCHEMA,
    MAX_CONTROL_RECEIPT_BYTES as BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES,
    BlitzortungEventIngressLimits,
)
from .blitzortung_mqtt_raw_sidecar import (
    DEFAULT_KEEPALIVE_SECONDS as BLITZORTUNG_DEFAULT_KEEPALIVE_SECONDS,
    BlitzortungMqttRawSidecar,
    BlitzortungRegion,
)
from .blitzortung_raw_ingress import (
    MAX_PAYLOAD_BYTES as BLITZORTUNG_MAX_PAYLOAD_BYTES,
)
from .desired_state import DesiredStateConflict, DesiredStateError
from .raw_artifacts import RawArtifactLimits
from .raw_capacity_ledger import RawCapacityProfile
from .raw_capture_retention_guard import (
    RawCaptureRetentionGuard,
    RawCaptureRetentionGuardBlocked,
    RawCaptureRetentionGuardConfig,
)
from .worker_acquisition_plan import COLLECTOR_SCOPE_ALL_SOURCES
from .worker_first_composition import (
    WorkerFirstCompositionConfig,
    WorkerFirstCompositionConfigError,
)
from .worker_first_ownership import (
    REQUIRED_STORE_ROLES,
    WorkerFirstFenceBusy,
    WorkerFirstOwnershipError,
    WorkerFirstOwnershipFence,
    WorkerFirstOwnershipLease,
)
from .worker_first_service_composition import (
    ACQUISITION_LANE_TIMEOUT_SECONDS,
    MAX_CONFIG_BYTES,
    WorkerFirstServiceCompositionConfig,
    WorkerFirstServiceCompositionConfigError,
    WorkerFirstServiceSourcePaths,
    _canary_capacity_profile,
    _canary_paths_overlap,
    _canary_raw_store_limits,
    _canary_source_path,
    _validated_canary_path,
    _validate_canary_path_set,
)


_LOGGER = logging.getLogger(__name__)

CONFIG_CONTRACT: Final = "stv3-worker-raw-capture-service-config-v5"
RUNTIME_CONTRACT: Final = "stv3-worker-raw-capture-runtime-v5"
MODE: Final = "service_raw_capture_shadow"
SCIENCE_MODE: Final = "disabled"
RAW_PAYLOAD_TRANSPORT: Final = "worker_direct_not_mqtt"
ADMISSION_POLICY: Final = "closed_active_raw_matrix_all_sources_v1"
TERMINAL_ROUTER_WIRED: Final = False
DOWNSTREAM_CONSUMPTION_REQUIRED: Final = False
RAW_STORE_COMMIT_IS_TERMINAL: Final = True
METEOROLOGICAL_OUTPUT: Final = False
DASHBOARD_OUTPUT: Final = False
CONTROL_JOB_TYPES: Final = (
    "desired_state_apply",
    "desired_state_claim",
    "desired_state_snapshot",
)
RAW_CAPTURE_ALLOWED_WORKLOAD_IDS: Final = tuple(
    sorted(
        workload_id
        for workload_id, rule in ACTIVATION_RULES.items()
        if rule.executable
    )
)
MAX_DIAGNOSTIC_BYTES: Final = 32 * 1024
MAX_COUNTER: Final = 1_000_000_000
BLITZORTUNG_AUTHORITY_ENV: Final = (
    "STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY"
)
BLITZORTUNG_EVENT_STATUS_CONTRACT: Final = (
    "stv3-worker-blitzortung-raw-shadow-status-v2"
)
BLITZORTUNG_EVENT_MAX_QUEUE_ENTRIES: Final = 4_096
BLITZORTUNG_EVENT_MAX_CONTROL_RECEIPTS: Final = 1_000_000
BLITZORTUNG_EVENT_MAX_RETAINED_ENTRIES: Final = 1_000_000
BLITZORTUNG_EVENT_MAX_SESSIONS: Final = 100_000
BLITZORTUNG_CONTROL_STAGING_RECOVERY_EXTRA_ENTRIES: Final = 2
MAX_SIGNED_CAPACITY: Final = (1 << 63) - 1
HOST_OPERATIONAL_FREE_RESERVE_BYTES: Final = 2 * 1024 * 1024 * 1024

_RECOVERABLE_DESIRED_STATE_BLOCKS: Final = frozenset({
    "desired_state_changed_during_plan",
    "desired_state_future",
    "desired_state_stale",
})

_CONFIG_FIELDS = frozenset({
    "acquisition",
    "contract",
    "control",
    "dashboard_output",
    "downstream_consumption_required",
    "event_ingress",
    "legacy_heavy_admission",
    "live_wiring",
    "meteorological_admission",
    "meteorological_output",
    "mode",
    "production_enable_ready",
    "raw_payload_transport",
    "raw_store_commit_is_terminal",
    "retention",
    "science_mode",
    "terminal_router_wired",
    "timeouts",
    "worker_first",
})
_ACQUISITION_FIELDS = frozenset({
    "allowed_workload_ids",
    "capacity_profile",
    "collector_scope",
    "paths",
    "raw_store_limits",
})
_PATH_FIELDS = frozenset({
    "capacity_ledger_db",
    "execution_receipts_db",
    "raw_store_root",
    "scheduler_db",
})
_CONTROL_FIELDS = frozenset({"job_types", "raw_payloads_over_mqtt"})
_RETENTION_FIELDS = frozenset({
    "automatic_reclaim",
    "capacity_exhaustion",
    "host_operational_free_reserve_bytes",
    "retained_payload_claim_bytes",
    "source_reclaim_authorized",
})
_TIMEOUT_FIELDS = frozenset({"acquisition_seconds", "stop_seconds"})
_EVENT_INGRESS_FIELDS = frozenset({"blitzortung"})
_BLITZORTUNG_EVENT_FIELDS = frozenset({
    "capacity_claimed",
    "control_root",
    "enabled",
    "ingress_limits",
    "keepalive_seconds",
    "operational_admission",
    "raw_store_commit_is_terminal",
    "raw_store_limits",
    "raw_store_root",
    "runtime_authority_environment",
})
_BLITZORTUNG_LIMIT_FIELDS = frozenset({
    "claim_lease_seconds",
    "control_receipt_reserve",
    "lock_timeout_seconds",
    "max_control_receipts",
    "max_queue_entries",
    "max_retained_payload_bytes",
    "max_retained_payload_entries",
    "max_sessions",
    "retention_mode",
    "schema",
})


class RawCaptureServiceError(RuntimeError):
    """Privacy-safe base error for the raw-only service boundary."""


class RawCaptureServiceConfigError(RawCaptureServiceError):
    """The pinned raw service configuration is invalid."""


class RawCaptureServiceIntegrityError(RawCaptureServiceError):
    """A runtime, path, authority, or evidence binding drifted."""


class RawCaptureServiceBusy(RawCaptureServiceError):
    """Another process owns the worker-first authority fence."""


class _ParentBrokerSecretResolver:
    """Presence-only resolver for parent OAuth brokers.

    The marker proves only that the complete parent-side broker is configured;
    no seed or ephemeral bearer value is retained by this adapter.
    """

    def __init__(
        self,
        base: SecretResolver,
        *,
        netatmo_broker_configured: bool,
        eumetsat_broker_configured: bool,
    ) -> None:
        self._base = base
        self._netatmo_broker_configured = netatmo_broker_configured
        self._eumetsat_broker_configured = eumetsat_broker_configured

    def resolve(self, requirement_id: str) -> str | None:
        if requirement_id == "worker_secret.netatmo_access_token":
            return (
                "worker_parent_oauth_broker_configured"
                if self._netatmo_broker_configured
                else None
            )
        if requirement_id in {
            "worker_secret.eumetsat_consumer_key",
            "worker_secret.eumetsat_consumer_secret",
        }:
            return (
                "worker_parent_oauth_broker_configured"
                if self._eumetsat_broker_configured
                else None
            )
        return self._base.resolve(requirement_id)


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
        raise RawCaptureServiceConfigError("raw_capture_config_not_canonical") from error


def _exact(value: object, fields: frozenset[str], code: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != fields:
        raise RawCaptureServiceConfigError(code)
    return dict(value)


def _duration(value: object, *, name: str, minimum: float, maximum: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not minimum <= float(value) <= maximum
    ):
        raise RawCaptureServiceConfigError(f"{name}_invalid")
    return float(value)


def _same_path(left: Path, right: Path) -> bool:
    try:
        return left.resolve(strict=False) == right.resolve(strict=False)
    except (OSError, RuntimeError):
        return False


def _same_filesystem_capacity_claim(paths: tuple[Path, ...]) -> bool:
    devices: set[int] = set()
    try:
        for original in paths:
            probe = original
            while not probe.exists() and probe != probe.parent:
                probe = probe.parent
            devices.add(int(probe.stat().st_dev))
    except OSError:
        return False
    return len(devices) == 1


def _blitzortung_ingress_limits(
    value: object,
) -> BlitzortungEventIngressLimits:
    mapping = _exact(
        value,
        _BLITZORTUNG_LIMIT_FIELDS,
        "raw_capture_event_ingress_limits_schema_invalid",
    )
    if (
        mapping["schema"] != BLITZORTUNG_LIMITS_SCHEMA
        or mapping["retention_mode"] != "append_only_no_reclamation"
    ):
        raise RawCaptureServiceConfigError(
            "raw_capture_event_ingress_limits_authority_invalid"
        )
    try:
        return BlitzortungEventIngressLimits(**{
            field: mapping[field]
            for field in (
                "claim_lease_seconds",
                "control_receipt_reserve",
                "lock_timeout_seconds",
                "max_control_receipts",
                "max_queue_entries",
                "max_retained_payload_bytes",
                "max_retained_payload_entries",
                "max_sessions",
            )
        })
    except (TypeError, ValueError):
        raise RawCaptureServiceConfigError(
            "raw_capture_event_ingress_limits_invalid"
        ) from None


@dataclass(frozen=True, slots=True, repr=False)
class RawCaptureServiceConfig:
    worker_first: WorkerFirstCompositionConfig
    source_paths: WorkerFirstServiceSourcePaths
    capacity_profile: RawCapacityProfile
    raw_store_limits: RawArtifactLimits
    blitzortung_control_root: Path
    blitzortung_raw_store_root: Path
    blitzortung_raw_store_limits: RawArtifactLimits
    blitzortung_ingress_limits: BlitzortungEventIngressLimits
    blitzortung_enabled: bool
    host_operational_free_reserve_bytes: int
    retained_payload_claim_bytes: int | None = None
    blitzortung_keepalive_seconds: int = BLITZORTUNG_DEFAULT_KEEPALIVE_SECONDS
    allowed_workload_ids: tuple[str, ...] = RAW_CAPTURE_ALLOWED_WORKLOAD_IDS
    acquisition_timeout_seconds: float = ACQUISITION_LANE_TIMEOUT_SECONDS
    stop_timeout_seconds: float = 6.0

    def __post_init__(self) -> None:
        if type(self.blitzortung_enabled) is not bool:
            raise RawCaptureServiceConfigError(
                "raw_capture_event_ingress_enabled_invalid"
            )
        if (
            type(self.host_operational_free_reserve_bytes) is not int
            or not HOST_OPERATIONAL_FREE_RESERVE_BYTES
            <= self.host_operational_free_reserve_bytes
            <= MAX_SIGNED_CAPACITY
        ):
            raise RawCaptureServiceConfigError(
                "raw_capture_host_operational_free_reserve_invalid"
            )
        if self.retained_payload_claim_bytes is None:
            object.__setattr__(
                self,
                "retained_payload_claim_bytes",
                self.raw_store_limits.max_total_bytes,
            )
        elif (
            type(self.retained_payload_claim_bytes) is not int
            or not self.raw_store_limits.max_object_bytes
            <= self.retained_payload_claim_bytes
            <= self.raw_store_limits.max_total_bytes
        ):
            raise RawCaptureServiceConfigError(
                "raw_capture_retained_payload_claim_invalid"
            )
        try:
            WorkerFirstServiceCompositionConfig(
                worker_first=self.worker_first,
                source_paths=self.source_paths,
                capacity_profile=self.capacity_profile,
                raw_store_limits=self.raw_store_limits,
                acquisition_timeout_seconds=self.acquisition_timeout_seconds,
                bridge_timeout_seconds=30.0,
                stop_timeout_seconds=self.stop_timeout_seconds,
            )
            _validate_canary_path_set(self)
            event_control_root = _validated_canary_path(
                self.blitzortung_control_root
            )
            event_raw_store_root = _validated_canary_path(
                self.blitzortung_raw_store_root
            )
        except (
            WorkerFirstCompositionConfigError,
            WorkerFirstServiceCompositionConfigError,
            TypeError,
            ValueError,
        ):
            raise RawCaptureServiceConfigError("raw_capture_config_invalid") from None
        try:
            protected_paths = tuple(
                _validated_canary_path(getattr(self.source_paths, field))
                for field in sorted(_PATH_FIELDS)
            ) + tuple(
                _validated_canary_path(Path(value))
                for value in self.worker_first.paths.to_mapping().values()
            )
        except WorkerFirstServiceCompositionConfigError:
            raise RawCaptureServiceConfigError(
                "raw_capture_event_ingress_path_invalid"
            ) from None
        if (
            _canary_paths_overlap(
                event_control_root,
                event_raw_store_root,
            )
            or any(
                _canary_paths_overlap(event_control_root, path)
                or _canary_paths_overlap(event_raw_store_root, path)
                for path in protected_paths
            )
        ):
            raise RawCaptureServiceConfigError(
                "raw_capture_event_ingress_path_alias"
            )
        capacity_paths = [self.source_paths.raw_store_root]
        if self.blitzortung_enabled:
            capacity_paths.extend((event_raw_store_root, event_control_root))
        if not _same_filesystem_capacity_claim(tuple(capacity_paths)):
            raise RawCaptureServiceConfigError(
                "raw_capture_event_ingress_filesystem_mismatch"
            )
        limits = self.blitzortung_ingress_limits
        event_raw_limits = self.blitzortung_raw_store_limits
        if (
            not isinstance(limits, BlitzortungEventIngressLimits)
            or not isinstance(event_raw_limits, RawArtifactLimits)
        ):
            raise RawCaptureServiceConfigError(
                "raw_capture_event_ingress_limits_invalid"
            )
        if (
            limits.max_queue_entries
            > BLITZORTUNG_EVENT_MAX_QUEUE_ENTRIES
            or limits.max_control_receipts
            > BLITZORTUNG_EVENT_MAX_CONTROL_RECEIPTS
            or limits.max_retained_payload_entries
            > BLITZORTUNG_EVENT_MAX_RETAINED_ENTRIES
            or limits.max_sessions > BLITZORTUNG_EVENT_MAX_SESSIONS
            or event_raw_limits.max_object_bytes
            < BLITZORTUNG_MAX_PAYLOAD_BYTES
            or event_raw_limits.max_total_bytes
            < limits.max_retained_payload_bytes
            or event_raw_limits.max_object_entries
            < limits.max_retained_payload_entries
            or event_raw_limits.max_receipt_entries
            < limits.max_retained_payload_entries
            or event_raw_limits.max_capture_entries
            < limits.max_retained_payload_entries
        ):
            raise RawCaptureServiceConfigError(
                "raw_capture_event_ingress_storage_envelope_invalid"
            )
        control_receipt_bytes = (
            limits.max_control_receipts
            * BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES
        )
        control_staging_residue_bytes = (
            limits.control_receipt_reserve
            + BLITZORTUNG_CONTROL_STAGING_RECOVERY_EXTRA_ENTRIES
        ) * BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES
        combined_capacity_bytes = (
            self.cadence_retained_payload_claim_bytes
            + self.raw_store_limits.max_catalog_bytes
        )
        combined_object_entries = self.raw_store_limits.max_object_entries
        if self.blitzortung_enabled:
            combined_capacity_bytes += (
                event_raw_limits.max_total_bytes
                + event_raw_limits.max_catalog_bytes
                + control_receipt_bytes
                + control_staging_residue_bytes
            )
            combined_object_entries += event_raw_limits.max_object_entries
        if (
            control_receipt_bytes > MAX_SIGNED_CAPACITY
            or control_staging_residue_bytes > MAX_SIGNED_CAPACITY
            or combined_capacity_bytes > MAX_SIGNED_CAPACITY
            or combined_object_entries > MAX_SIGNED_CAPACITY
        ):
            raise RawCaptureServiceConfigError(
                "raw_capture_event_ingress_storage_envelope_invalid"
            )
        if (
            isinstance(self.blitzortung_keepalive_seconds, bool)
            or not isinstance(self.blitzortung_keepalive_seconds, int)
            or not 15 <= self.blitzortung_keepalive_seconds <= 300
        ):
            raise RawCaptureServiceConfigError(
                "raw_capture_event_ingress_keepalive_invalid"
            )
        if (
            type(self.allowed_workload_ids) is not tuple
            or self.allowed_workload_ids != RAW_CAPTURE_ALLOWED_WORKLOAD_IDS
            or not self.allowed_workload_ids
        ):
            raise RawCaptureServiceConfigError(
                "raw_capture_allowed_workload_ids_invalid"
            )
        _duration(
            self.acquisition_timeout_seconds,
            name="raw_capture_acquisition_timeout",
            minimum=ACQUISITION_LANE_TIMEOUT_SECONDS,
            maximum=ACQUISITION_LANE_TIMEOUT_SECONDS,
        )
        _duration(
            self.stop_timeout_seconds,
            name="raw_capture_stop_timeout",
            minimum=0.1,
            maximum=6.0,
        )
        if len(_canonical(self.to_mapping())) > MAX_CONFIG_BYTES:
            raise RawCaptureServiceConfigError("raw_capture_config_capacity_exceeded")

    @classmethod
    def from_mapping(cls, value: object) -> "RawCaptureServiceConfig":
        mapping = _exact(value, _CONFIG_FIELDS, "raw_capture_config_schema_invalid")
        acquisition = _exact(
            mapping["acquisition"],
            _ACQUISITION_FIELDS,
            "raw_capture_acquisition_schema_invalid",
        )
        paths = _exact(
            acquisition["paths"],
            _PATH_FIELDS,
            "raw_capture_paths_schema_invalid",
        )
        control = _exact(
            mapping["control"],
            _CONTROL_FIELDS,
            "raw_capture_control_schema_invalid",
        )
        retention = _exact(
            mapping["retention"],
            _RETENTION_FIELDS,
            "raw_capture_retention_schema_invalid",
        )
        timeouts = _exact(
            mapping["timeouts"],
            _TIMEOUT_FIELDS,
            "raw_capture_timeouts_schema_invalid",
        )
        event_ingress = _exact(
            mapping["event_ingress"],
            _EVENT_INGRESS_FIELDS,
            "raw_capture_event_ingress_schema_invalid",
        )
        blitzortung = _exact(
            event_ingress["blitzortung"],
            _BLITZORTUNG_EVENT_FIELDS,
            "raw_capture_blitzortung_event_schema_invalid",
        )
        allowed = acquisition["allowed_workload_ids"]
        if (
            mapping["contract"] != CONFIG_CONTRACT
            or mapping["dashboard_output"] is not DASHBOARD_OUTPUT
            or mapping["downstream_consumption_required"]
            is not DOWNSTREAM_CONSUMPTION_REQUIRED
            or mapping["legacy_heavy_admission"] != "forbidden"
            or mapping["live_wiring"] is not True
            or mapping["meteorological_admission"] is not False
            or mapping["meteorological_output"] is not METEOROLOGICAL_OUTPUT
            or mapping["mode"] != MODE
            or mapping["production_enable_ready"] is not False
            or mapping["raw_payload_transport"] != RAW_PAYLOAD_TRANSPORT
            or mapping["raw_store_commit_is_terminal"]
            is not RAW_STORE_COMMIT_IS_TERMINAL
            or mapping["science_mode"] != SCIENCE_MODE
            or mapping["terminal_router_wired"] is not TERMINAL_ROUTER_WIRED
            or blitzortung["operational_admission"] is not False
            or blitzortung["raw_store_commit_is_terminal"] is not True
            or blitzortung["runtime_authority_environment"]
            != BLITZORTUNG_AUTHORITY_ENV
            or type(blitzortung["enabled"]) is not bool
            or type(blitzortung["capacity_claimed"]) is not bool
            or blitzortung["capacity_claimed"] is not blitzortung["enabled"]
            or acquisition["collector_scope"] != COLLECTOR_SCOPE_ALL_SOURCES
            or control != {
                "job_types": list(CONTROL_JOB_TYPES),
                "raw_payloads_over_mqtt": False,
            }
            or retention != {
                "automatic_reclaim": False,
                "capacity_exhaustion": "fail_closed",
                "host_operational_free_reserve_bytes": retention[
                    "host_operational_free_reserve_bytes"
                ],
                "retained_payload_claim_bytes": retention[
                    "retained_payload_claim_bytes"
                ],
                "source_reclaim_authorized": False,
            }
            or type(allowed) is not list
            or tuple(allowed) != RAW_CAPTURE_ALLOWED_WORKLOAD_IDS
        ):
            raise RawCaptureServiceConfigError("raw_capture_config_authority_invalid")
        try:
            result = cls(
                worker_first=WorkerFirstCompositionConfig.from_mapping(
                    mapping["worker_first"]
                ),
                source_paths=WorkerFirstServiceSourcePaths(**{
                    field: _canary_source_path(paths[field])
                    for field in _PATH_FIELDS
                }),
                capacity_profile=_canary_capacity_profile(
                    acquisition["capacity_profile"]
                ),
                raw_store_limits=_canary_raw_store_limits(
                    acquisition["raw_store_limits"]
                ),
                blitzortung_control_root=_canary_source_path(
                    blitzortung["control_root"]
                ),
                blitzortung_raw_store_root=_canary_source_path(
                    blitzortung["raw_store_root"]
                ),
                blitzortung_raw_store_limits=_canary_raw_store_limits(
                    blitzortung["raw_store_limits"]
                ),
                blitzortung_ingress_limits=_blitzortung_ingress_limits(
                    blitzortung["ingress_limits"]
                ),
                blitzortung_enabled=blitzortung["enabled"],
                host_operational_free_reserve_bytes=retention[
                    "host_operational_free_reserve_bytes"
                ],
                retained_payload_claim_bytes=retention[
                    "retained_payload_claim_bytes"
                ],
                blitzortung_keepalive_seconds=blitzortung[
                    "keepalive_seconds"
                ],
                allowed_workload_ids=tuple(allowed),
                acquisition_timeout_seconds=timeouts["acquisition_seconds"],
                stop_timeout_seconds=timeouts["stop_seconds"],
            )
        except RawCaptureServiceError:
            raise
        except Exception:
            raise RawCaptureServiceConfigError("raw_capture_config_invalid") from None
        if result.to_mapping() != mapping:
            raise RawCaptureServiceConfigError("raw_capture_config_identity_mismatch")
        return result

    def to_mapping(self) -> dict[str, Any]:
        return {
            "acquisition": {
                "allowed_workload_ids": list(self.allowed_workload_ids),
                "capacity_profile": self.capacity_profile.to_mapping(),
                "collector_scope": COLLECTOR_SCOPE_ALL_SOURCES,
                "paths": self.source_paths.to_mapping(),
                "raw_store_limits": asdict(self.raw_store_limits),
            },
            "contract": CONFIG_CONTRACT,
            "control": {
                "job_types": list(CONTROL_JOB_TYPES),
                "raw_payloads_over_mqtt": False,
            },
            "dashboard_output": DASHBOARD_OUTPUT,
            "downstream_consumption_required": (
                DOWNSTREAM_CONSUMPTION_REQUIRED
            ),
            "event_ingress": {
                "blitzortung": {
                    "capacity_claimed": self.blitzortung_enabled,
                    "control_root": str(self.blitzortung_control_root),
                    "enabled": self.blitzortung_enabled,
                    "ingress_limits": (
                        self.blitzortung_ingress_limits.to_metadata()
                    ),
                    "keepalive_seconds": self.blitzortung_keepalive_seconds,
                    "operational_admission": False,
                    "raw_store_commit_is_terminal": True,
                    "raw_store_limits": asdict(
                        self.blitzortung_raw_store_limits
                    ),
                    "raw_store_root": str(
                        self.blitzortung_raw_store_root
                    ),
                    "runtime_authority_environment": (
                        BLITZORTUNG_AUTHORITY_ENV
                    ),
                }
            },
            "legacy_heavy_admission": "forbidden",
            "live_wiring": True,
            "meteorological_admission": False,
            "meteorological_output": METEOROLOGICAL_OUTPUT,
            "mode": MODE,
            "production_enable_ready": False,
            "raw_payload_transport": RAW_PAYLOAD_TRANSPORT,
            "raw_store_commit_is_terminal": RAW_STORE_COMMIT_IS_TERMINAL,
            "retention": {
                "automatic_reclaim": False,
                "capacity_exhaustion": "fail_closed",
                "host_operational_free_reserve_bytes": (
                    self.host_operational_free_reserve_bytes
                ),
                "retained_payload_claim_bytes": (
                    self.cadence_retained_payload_claim_bytes
                ),
                "source_reclaim_authorized": False,
            },
            "science_mode": SCIENCE_MODE,
            "terminal_router_wired": TERMINAL_ROUTER_WIRED,
            "timeouts": {
                "acquisition_seconds": float(self.acquisition_timeout_seconds),
                "stop_seconds": float(self.stop_timeout_seconds),
            },
            "worker_first": self.worker_first.to_mapping(),
        }

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(_canonical(self.to_mapping())).hexdigest()

    @property
    def cadence_retained_payload_claim_bytes(self) -> int:
        if self.retained_payload_claim_bytes is None:
            return self.raw_store_limits.max_total_bytes
        return self.retained_payload_claim_bytes

    @property
    def storage_capacity_diagnostics(self) -> dict[str, Any]:
        control_receipt_bytes = (
            self.blitzortung_ingress_limits.max_control_receipts
            * BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES
        )
        control_staging_residue_bytes = (
            self.blitzortung_ingress_limits.control_receipt_reserve
            + BLITZORTUNG_CONTROL_STAGING_RECOVERY_EXTRA_ENTRIES
        ) * BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES
        combined_bytes = (
            self.cadence_retained_payload_claim_bytes
            + self.raw_store_limits.max_catalog_bytes
        )
        combined_entries = self.raw_store_limits.max_object_entries
        if self.blitzortung_enabled:
            combined_bytes += (
                self.blitzortung_raw_store_limits.max_total_bytes
                + self.blitzortung_raw_store_limits.max_catalog_bytes
                + control_receipt_bytes
                + control_staging_residue_bytes
            )
            combined_entries += (
                self.blitzortung_raw_store_limits.max_object_entries
            )
        event_claim = self.blitzortung_enabled
        return {
            "accounting_contract": (
                "config_bound_optional_event_capacity_v2"
            ),
            "host_operational_free_reserve_bytes": (
                self.host_operational_free_reserve_bytes
            ),
            "cadence": {
                "catalog_max_bytes": self.raw_store_limits.max_catalog_bytes,
                "ledger_accounted": True,
                "object_max_bytes": self.raw_store_limits.max_total_bytes,
                "object_max_entries": self.raw_store_limits.max_object_entries,
            },
            "combined_persistent_volume": {
                "configured_max_bytes": combined_bytes,
                "raw_object_max_entries": (
                    combined_entries
                ),
                "same_filesystem_capacity_claim": True,
            },
            "event": {
                "capacity_claimed": event_claim,
                "catalog_max_bytes": (
                    self.blitzortung_raw_store_limits.max_catalog_bytes
                    if event_claim else 0
                ),
                "control_receipt_max_bytes": (
                    BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES if event_claim else 0
                ),
                "control_receipt_storage_max_bytes": (
                    control_receipt_bytes if event_claim else 0
                ),
                "control_staging_residue_entry_capacity": (
                    (
                        self.blitzortung_ingress_limits.control_receipt_reserve
                        + BLITZORTUNG_CONTROL_STAGING_RECOVERY_EXTRA_ENTRIES
                    ) if event_claim else 0
                ),
                "control_staging_residue_max_bytes": (
                    control_staging_residue_bytes if event_claim else 0
                ),
                "ledger_accounted": False,
                "enabled": self.blitzortung_enabled,
                "object_max_bytes": (
                    self.blitzortung_raw_store_limits.max_total_bytes
                    if event_claim else 0
                ),
                "object_max_entries": (
                    self.blitzortung_raw_store_limits.max_object_entries
                    if event_claim else 0
                ),
            },
        }


class RawCaptureServiceRuntime:
    """One serialized raw-acquisition lane plus bounded MQTT control calls."""

    def __init__(
        self,
        *,
        acquisition: AcquisitionShadowRuntime,
        ownership_lease: WorkerFirstOwnershipLease,
        config_sha256: str,
        authority_sha256: str,
        allowed_workload_ids: tuple[str, ...],
        blitzortung_enabled: bool,
        blitzortung_sidecar: Any | None,
        retention_guard: Any,
        storage_capacity: Mapping[str, Any],
        acquisition_timeout_seconds: float,
        stop_timeout_seconds: float,
    ) -> None:
        self._acquisition = acquisition
        self._lease = ownership_lease
        self._config_sha256 = config_sha256
        self._authority_sha256 = authority_sha256
        self._allowed_workload_ids = allowed_workload_ids
        if type(blitzortung_enabled) is not bool:
            raise RawCaptureServiceConfigError(
                "raw_capture_event_ingress_enabled_invalid"
            )
        if blitzortung_enabled:
            if blitzortung_sidecar is None or not all(
                callable(getattr(blitzortung_sidecar, name, None))
                for name in ("diagnostics", "start", "stop", "update_regions")
            ):
                raise RawCaptureServiceConfigError(
                    "raw_capture_event_ingress_sidecar_invalid"
                )
        elif blitzortung_sidecar is not None:
            raise RawCaptureServiceConfigError(
                "raw_capture_event_ingress_sidecar_forbidden"
            )
        self._blitzortung_enabled = blitzortung_enabled
        self._blitzortung_sidecar = blitzortung_sidecar
        if not all(
            callable(getattr(retention_guard, name, None))
            for name in ("diagnostics", "inspect")
        ):
            raise RawCaptureServiceConfigError(
                "raw_capture_retention_guard_invalid"
            )
        self._retention_guard = retention_guard
        self._storage_capacity = json.loads(_canonical(storage_capacity))
        self._acquisition_timeout = acquisition_timeout_seconds
        self._stop_timeout = stop_timeout_seconds
        self._gate = asyncio.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="stv3-raw-capture",
        )
        self._mutable_lock = threading.Lock()
        self._event_ingress_lock = threading.RLock()
        self._raw_ingress_integrity_lock = threading.RLock()
        self._active_future: Future[Any] | None = None
        self._executor_closed = False
        self._started = False
        self._stop_requested = False
        self._state = "new"
        self._last_error_code: str | None = None
        self._event_ingress_started = False
        self._event_ingress_stopped = False
        self._event_ingress_fault_latched = False
        self._restart_required = False
        self._event_ingress_error_code: str | None = None
        self._event_desired_state_sha256: str | None = None
        self._event_lightning_source_mode: str | None = None
        self._event_region_count = 0
        self._event_desired_state_status = "unconfigured"
        self._last_tick = ShadowTickResult(
            "idle", "not_started", None, None, None, None, False
        )
        self._counts = {
            "control_calls": 0,
            "event_failures": 0,
            "event_stops": 0,
            "event_syncs": 0,
            "failures": 0,
            "start_calls": 0,
            "stop_calls": 0,
            "ticks": 0,
            "timeouts": 0,
        }

    @property
    def desired_state_store(self):
        """Expose the worker-owned control store to an in-process output layer."""

        return self._acquisition.desired_state_store

    @property
    def raw_artifact_store(self):
        """Expose verified capture receipts, never raw bytes or provider secrets."""

        return self._acquisition.raw_store

    @property
    def latest_tick_result(self) -> ShadowTickResult:
        """Return the immutable result of the latest serialized acquisition tick."""

        return self._last_tick

    def retained_successful_captures(self):
        """Expose only verified retained captures for restart restoration."""

        return self._acquisition.retained_successful_captures()

    def latest_terminal_results(self):
        """Expose bounded provider outcomes without targets or credentials."""

        return self._acquisition.latest_terminal_results()

    def _increment(self, key: str) -> None:
        self._counts[key] = min(MAX_COUNTER, self._counts[key] + 1)

    def _assert_held(self) -> None:
        try:
            self._lease.assert_held()
        except WorkerFirstOwnershipError:
            self._state = "error"
            self._last_error_code = "raw_capture_ownership_lost"
            raise RawCaptureServiceIntegrityError(
                "raw_capture_ownership_lost"
            ) from None

    def _sync_blitzortung_event_ingress(self) -> None:
        """Apply only the active desired-state regions to exact RAW ingress."""

        if not self._blitzortung_enabled:
            return
        with self._event_ingress_lock:
            if self._event_ingress_stopped or self._event_ingress_fault_latched:
                return
            self._increment("event_syncs")
            try:
                active = self._acquisition.desired_state_store.load_active()
                if active is None:
                    self._event_desired_state_sha256 = None
                    self._event_lightning_source_mode = None
                    self._event_region_count = 0
                    self._event_desired_state_status = "unconfigured"
                    return
                mode = active.settings.lightning_source_mode
                if mode == "satellite_test":
                    regions: tuple[BlitzortungRegion, ...] = ()
                    desired_status = "satellite_test_suppressed"
                else:
                    regions = tuple(
                        BlitzortungRegion(
                            latitude=target.latitude,
                            longitude=target.longitude,
                            radius_km=target.observation_radius_km,
                        )
                        for target in active.targets
                        if target.available
                    )
                    desired_status = (
                        "active_regions"
                        if regions
                        else "no_available_targets"
                    )
                self._event_desired_state_sha256 = active.state_sha256
                self._event_lightning_source_mode = mode
                self._event_region_count = len(regions)
                self._event_desired_state_status = desired_status
                if self._event_ingress_started:
                    diagnostics = self._blitzortung_sidecar.update_regions(
                        regions,
                        desired_state_sha256=active.state_sha256,
                    )
                else:
                    diagnostics = self._blitzortung_sidecar.start(
                        regions,
                        desired_state_sha256=active.state_sha256,
                    )
                    self._event_ingress_started = True
                if diagnostics.get("fault_latched") is True:
                    self._event_ingress_fault_latched = True
                    self._event_ingress_error_code = (
                        "event_ingress_sidecar_fault_latched"
                    )
                else:
                    self._event_ingress_error_code = None
            except Exception:
                self._increment("event_failures")
                self._event_ingress_fault_latched = True
                self._event_ingress_error_code = (
                    "event_ingress_desired_state_apply_failed"
                )
                try:
                    self._blitzortung_sidecar.stop()
                except Exception:
                    pass

    def _stop_blitzortung_event_ingress(self) -> None:
        if not self._blitzortung_enabled:
            return
        with self._event_ingress_lock:
            if self._event_ingress_stopped:
                return
            self._increment("event_stops")
            try:
                self._blitzortung_sidecar.stop()
            except Exception:
                self._increment("event_failures")
                self._event_ingress_error_code = "event_ingress_stop_failed"
                self._event_ingress_fault_latched = True
            self._event_ingress_stopped = True

    def _blitzortung_event_ingress_diagnostics(self) -> dict[str, Any]:
        if not self._blitzortung_enabled:
            return {
                "capacity_claimed": False,
                "cadence_activation_route": False,
                "contract": BLITZORTUNG_EVENT_STATUS_CONTRACT,
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
        with self._event_ingress_lock:
            sidecar = dict(self._blitzortung_sidecar.diagnostics())
            sidecar_alarms = sidecar.get("alarm_codes")
            alarms = (
                list(sidecar_alarms)
                if type(sidecar_alarms) is list
                else ["event_ingress_diagnostics_schema_invalid"]
            )
            if self._event_ingress_error_code is not None:
                alarms.append(self._event_ingress_error_code)
            alarms = sorted(set(alarms))
            authority_available = (
                sidecar.get("access_retention_authority") == "available"
            )
            sidecar_fault = sidecar.get("fault_latched") is True
            sidecar_state = sidecar.get("state")
            connected = sidecar.get("connected") is True
            if self._event_ingress_stopped:
                status = "stopped"
                reason = "event_ingress_stopped"
                ready = False
            elif self._event_ingress_fault_latched or sidecar_fault:
                status = "degraded"
                reason = "event_ingress_fault_latched"
                ready = False
            elif not authority_available:
                status = "disabled"
                reason = "retention_authority_unavailable"
                ready = False
            elif self._event_desired_state_sha256 is None:
                status = "waiting"
                reason = "desired_state_unconfigured"
                ready = False
            elif self._event_desired_state_status == "satellite_test_suppressed":
                status = "intentionally_inert"
                reason = "satellite_test_selected"
                ready = True
            elif self._event_desired_state_status == "no_available_targets":
                status = "intentionally_inert"
                reason = "no_available_targets"
                ready = True
            elif connected and sidecar_state == "running_exact_raw":
                status = "running_exact_raw"
                reason = "regional_qos0_raw_capture_active"
                ready = True
            else:
                status = "starting"
                reason = "regional_qos0_raw_capture_not_connected"
                ready = False
            return {
                "capacity_claimed": True,
                "cadence_activation_route": False,
                "contract": BLITZORTUNG_EVENT_STATUS_CONTRACT,
                "desired_state": {
                    "lightning_source_mode": self._event_lightning_source_mode,
                    "region_count": self._event_region_count,
                    "state_sha256": self._event_desired_state_sha256,
                    "status": self._event_desired_state_status,
                },
                "enabled": True,
                "operational_admission": False,
                "raw_store_commit_is_terminal": True,
                "readiness": {
                    "alarm_codes": alarms,
                    "ready": ready,
                    "reason_code": reason,
                    "required_for_service_admission": False,
                    "status": status,
                },
                "sidecar": sidecar,
            }

    def handle_control_job(
        self,
        job_type: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        if (
            not self._started
            or self._stop_requested
            or self._state != "running"
        ):
            raise RawCaptureServiceError("raw_capture_runtime_not_running")
        if job_type not in CONTROL_JOB_TYPES or not isinstance(payload, Mapping):
            raise RawCaptureServiceConfigError("raw_capture_control_job_invalid")
        self._assert_held()
        value = dict(payload)
        try:
            if job_type == "desired_state_claim":
                if set(value) != {"claim_id", "producer_id"}:
                    raise RawCaptureServiceConfigError(
                        "raw_capture_desired_state_claim_invalid"
                    )
                result = self._acquisition.desired_state_store.claim_producer(
                    value["producer_id"],
                    value["claim_id"],
                )
            elif job_type == "desired_state_snapshot":
                if value:
                    raise RawCaptureServiceConfigError(
                        "raw_capture_desired_state_snapshot_invalid"
                    )
                result = self._acquisition.desired_state_store.current()
            else:
                if set(value) != {"desired_state"}:
                    raise RawCaptureServiceConfigError(
                        "raw_capture_desired_state_apply_invalid"
                    )
                with self._raw_ingress_integrity_lock:
                    if self._restart_required:
                        raise RawCaptureServiceIntegrityError(
                            "raw_capture_runtime_restart_required"
                        )
                    self._retention_preflight(full_audit=False)
                    result = self._acquisition.desired_state_store.apply(
                        value["desired_state"]
                    )
                    self._sync_blitzortung_event_ingress()
                    self._retention_preflight(full_audit=False)
        except RawCaptureServiceError:
            raise
        except DesiredStateConflict:
            raise RawCaptureServiceIntegrityError(
                "raw_capture_desired_state_conflict"
            ) from None
        except DesiredStateError:
            raise RawCaptureServiceIntegrityError(
                "raw_capture_desired_state_rejected"
            ) from None
        self._increment("control_calls")
        return result

    async def _blocking(self, function: Callable[[], Any]) -> Any:
        if self._executor_closed:
            raise RawCaptureServiceError("raw_capture_lane_closed")
        loop = asyncio.get_running_loop()
        future = self._executor.submit(function)
        with self._mutable_lock:
            self._active_future = future
        try:
            return await asyncio.wait_for(
                asyncio.shield(asyncio.wrap_future(future, loop=loop)),
                timeout=self._acquisition_timeout,
            )
        except TimeoutError:
            self._increment("timeouts")
            self._acquisition.request_stop()
            raise RawCaptureServiceError("raw_capture_tick_timeout") from None
        finally:
            with self._mutable_lock:
                if self._active_future is future and future.done():
                    self._active_future = None

    def _resource_preflight(self) -> None:
        self._assert_held()
        diagnostics = self._acquisition.diagnostics()
        activation = diagnostics.get("activation")
        capacity = diagnostics.get("capacity")
        if (
            diagnostics.get("collector_scope") != COLLECTOR_SCOPE_ALL_SOURCES
            or not isinstance(activation, Mapping)
            or tuple(activation.get("selected_workload_ids", ()))
            != self._allowed_workload_ids
            or activation.get("selection_explicit") is not True
            or not isinstance(capacity, Mapping)
            or capacity.get("global_max_bytes", 0) <= 0
            or capacity.get("global_max_entries", 0) <= 0
        ):
            raise RawCaptureServiceIntegrityError(
                "raw_capture_resource_envelope_invalid"
            )

    def _retention_preflight(self, *, full_audit: bool) -> None:
        self._assert_held()
        try:
            diagnostics = self._retention_guard.inspect(
                full_audit=full_audit
            )
        except RawCaptureRetentionGuardBlocked as error:
            _LOGGER.error(
                "RAW retention guard blocked: reason=%s",
                error.reason_code,
            )
            self._restart_required = True
            self._stop_blitzortung_event_ingress()
            raise RawCaptureServiceIntegrityError(
                "raw_capture_retention_guard_blocked"
            ) from None
        except Exception:
            self._restart_required = True
            self._stop_blitzortung_event_ingress()
            raise RawCaptureServiceIntegrityError(
                "raw_capture_retention_guard_failed"
            ) from None
        if (
            not isinstance(diagnostics, Mapping)
            or diagnostics.get("admission_ready") is not True
            or diagnostics.get("live_delete_performed") is not False
        ):
            self._restart_required = True
            self._stop_blitzortung_event_ingress()
            raise RawCaptureServiceIntegrityError(
                "raw_capture_retention_guard_blocked"
            )

    def _start_all_raw_ingress(self) -> None:
        with self._raw_ingress_integrity_lock:
            if self._restart_required:
                raise RawCaptureServiceIntegrityError(
                    "raw_capture_runtime_restart_required"
                )
            self._retention_preflight(full_audit=True)
            self._resource_preflight()
            self._sync_blitzortung_event_ingress()
            self._retention_preflight(full_audit=False)

    def _tick_all_raw_ingress(self) -> ShadowTickResult:
        with self._raw_ingress_integrity_lock:
            if self._restart_required:
                raise RawCaptureServiceIntegrityError(
                    "raw_capture_runtime_restart_required"
                )
            self._retention_preflight(full_audit=False)
            try:
                result = self._acquisition.tick()
            except SchedulerDesiredStateError as error:
                if error.reason_code not in _RECOVERABLE_DESIRED_STATE_BLOCKS:
                    raise
                # A stale/future head or a concurrently superseded planning
                # snapshot must block provider acquisition, but it must not
                # tear down the process that owns the desired-state control
                # jobs needed to install the replacement head.  Keep this a
                # non-terminal idle tick; the next fresh apply/tick recovers
                # through the normal durable scheduler path.
                result = ShadowTickResult(
                    "idle",
                    error.reason_code,
                    None,
                    None,
                    None,
                    None,
                    False,
                )
            else:
                self._sync_blitzortung_event_ingress()
            self._retention_preflight(full_audit=False)
            return result

    async def async_start(self) -> dict[str, Any]:
        async with self._gate:
            self._increment("start_calls")
            if self._started:
                return self.diagnostics()
            if self._stop_requested:
                raise RawCaptureServiceError("raw_capture_runtime_stopping")
            self._state = "starting"
            try:
                await self._blocking(self._start_all_raw_ingress)
                self._started = True
                self._state = "running"
                self._last_error_code = None
            except Exception:
                self._increment("failures")
                self._state = "error"
                self._last_error_code = "raw_capture_start_failed"
                raise
            return self.diagnostics()

    async def async_tick(self) -> dict[str, Any]:
        if not self._started or self._stop_requested:
            raise RawCaptureServiceError("raw_capture_runtime_not_running")
        async with self._gate:
            if self._stop_requested:
                raise RawCaptureServiceError("raw_capture_runtime_stopping")
            self._assert_held()
            self._increment("ticks")
            try:
                self._last_tick = await self._blocking(
                    self._tick_all_raw_ingress
                )
                self._state = "running"
                self._last_error_code = None
            except Exception:
                self._increment("failures")
                self._state = "degraded"
                self._last_error_code = "raw_capture_tick_failed"
                raise
            return self.diagnostics()

    async def async_stop(self) -> dict[str, Any]:
        self._increment("stop_calls")
        self._stop_requested = True
        self._stop_blitzortung_event_ingress()
        self._acquisition.request_stop()
        if self._executor_closed:
            return self.diagnostics()
        try:
            await asyncio.wait_for(self._gate.acquire(), timeout=self._stop_timeout)
        except TimeoutError:
            self._increment("timeouts")
            self._state = "stop_pending"
            self._last_error_code = "raw_capture_stop_timeout"
            return self.diagnostics()
        try:
            with self._mutable_lock:
                active = self._active_future
            if active is not None and not active.done():
                self._state = "stop_pending"
                self._last_error_code = "raw_capture_lane_still_active"
                return self.diagnostics()
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._executor_closed = True
            self._lease.release()
            self._state = "stopped"
            self._last_error_code = None
        except WorkerFirstOwnershipError:
            self._state = "stop_pending"
            self._last_error_code = "raw_capture_ownership_release_failed"
        finally:
            self._gate.release()
        return self.diagnostics()

    def diagnostics(self) -> dict[str, Any]:
        with self._mutable_lock:
            active = self._active_future is not None and not self._active_future.done()
        fence_held = False
        if not self._executor_closed:
            try:
                self._lease.assert_held()
                fence_held = True
            except WorkerFirstOwnershipError:
                fence_held = False
        acquisition = self._acquisition.diagnostics()
        activation = acquisition["activation"]
        capacity = acquisition["capacity"]
        configured_workloads = list(activation["configured_workloads"])
        result = {
            "acquisition_configuration": {
                "collector_scope": COLLECTOR_SCOPE_ALL_SOURCES,
                "configured_workload_ids": list(
                    self._allowed_workload_ids
                ),
                "policy": ADMISSION_POLICY,
            },
            "authority_sha256": self._authority_sha256,
            "blocked_catalog": dict(activation["blocked_catalog"]),
            "capture_storage": {
                "committed_bytes": capacity["committed_bytes"],
                "committed_count": capacity["committed_count"],
                "committed_entries": capacity["committed_entries"],
                "ledger_scope": "cadence_only_event_store_excluded",
                "reserved_bytes": capacity["reserved_bytes"],
                "reserved_count": capacity["reserved_count"],
                "reserved_entries": capacity["reserved_entries"],
            },
            "config_sha256": self._config_sha256,
            "contract": RUNTIME_CONTRACT,
            "control_job_types": list(CONTROL_JOB_TYPES),
            "counts": dict(self._counts),
            "dashboard_output": DASHBOARD_OUTPUT,
            "downstream_consumption_required": (
                DOWNSTREAM_CONSUMPTION_REQUIRED
            ),
            "event_ingress": {
                "blitzortung": (
                    self._blitzortung_event_ingress_diagnostics()
                )
            },
            "heavy_lane_active": active,
            "last_error_code": self._last_error_code,
            "last_tick": self._last_tick.as_dict(),
            "legacy_heavy_admission": "forbidden",
            "live_wiring": True,
            "meteorological_admission": False,
            "meteorological_output": METEOROLOGICAL_OUTPUT,
            "mode": MODE,
            "ownership_fence_held": fence_held,
            "production_enable_ready": False,
            "provider_configuration": {
                "configured_count": sum(
                    row["handler_configured"]
                    for row in configured_workloads
                ),
                "requirements_unavailable_count": activation[
                    "requirements_unavailable_count"
                ],
                "requirements_unavailable_workload_ids": list(
                    activation[
                        "requirements_unavailable_workload_ids"
                    ]
                ),
                "workloads": configured_workloads,
            },
            "raw_payload_transport": RAW_PAYLOAD_TRANSPORT,
            "raw_store_commit_is_terminal": RAW_STORE_COMMIT_IS_TERMINAL,
            "retention": {
                "automatic_reclaim": False,
                "capacity_exhaustion": "fail_closed",
                "source_reclaim_authorized": False,
            },
            "retention_guard": self._retention_guard.diagnostics(
                ownership_held=fence_held
            ),
            "science_mode": SCIENCE_MODE,
            "terminal_router_wired": TERMINAL_ROUTER_WIRED,
            "shutdown_complete": (
                self._state == "stopped"
                and self._executor_closed
                and not fence_held
            ),
            "state": self._state,
            "storage_capacity": self._storage_capacity,
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise RawCaptureServiceIntegrityError(
                "raw_capture_diagnostics_capacity_exceeded"
            )
        return result


def build_raw_capture_service(
    config: RawCaptureServiceConfig,
    *,
    expected_config_sha256: str,
    expected_authority_sha256: str,
    secret_resolver: SecretResolver | None = None,
    executor_factory: ExecutorFactory = raw_capture_executor_factory,
    netatmo_access_token_broker: NetatmoAccessTokenBroker | None = None,
    eumetsat_access_token_broker: EumetsatAccessTokenBroker | None = None,
    blitzortung_access_retention_authority: str | None = None,
    blitzortung_sidecar_factory: Callable[..., Any] = (
        BlitzortungMqttRawSidecar
    ),
    clock: Callable[[], float] = time.time,
    publisher: Callable[[bytes], Any] | None = None,
    status_supplier: Callable[[], Mapping[str, Any]] | None = None,
) -> RawCaptureServiceRuntime:
    """Build the raw-only service after acquiring the existing authority."""

    if not isinstance(config, RawCaptureServiceConfig):
        raise RawCaptureServiceConfigError("raw_capture_config_type_invalid")
    if config.fingerprint != expected_config_sha256:
        raise RawCaptureServiceConfigError("raw_capture_config_checksum_mismatch")
    if (
        not isinstance(expected_authority_sha256, str)
        or len(expected_authority_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_authority_sha256)
        or not callable(executor_factory)
        or (
            config.blitzortung_enabled
            and not callable(blitzortung_sidecar_factory)
        )
        or not callable(clock)
        or (
            netatmo_access_token_broker is not None
            and not callable(netatmo_access_token_broker)
        )
        or (
            eumetsat_access_token_broker is not None
            and not callable(eumetsat_access_token_broker)
        )
        or (publisher is not None and not callable(publisher))
        or (status_supplier is not None and not callable(status_supplier))
    ):
        raise RawCaptureServiceConfigError("raw_capture_runtime_argument_invalid")
    if config.blitzortung_enabled:
        if (
            blitzortung_access_retention_authority
            != ACCESS_RETENTION_AUTHORITY_ID
        ):
            raise RawCaptureServiceConfigError(
                "raw_capture_event_ingress_authority_invalid"
            )
    elif blitzortung_access_retention_authority is not None:
        raise RawCaptureServiceConfigError(
            "raw_capture_event_ingress_authority_forbidden"
        )
    # Accepted for WorkerService factory compatibility. Raw payloads and
    # operational snapshots are intentionally never sent to either callback.
    del publisher, status_supplier
    base = config.worker_first
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
            raise RawCaptureServiceIntegrityError(
                "raw_capture_authority_path_mismatch"
            )
        lease = fence.acquire()
    except WorkerFirstFenceBusy:
        raise RawCaptureServiceBusy("raw_capture_owner_busy") from None
    except RawCaptureServiceError:
        raise
    except WorkerFirstOwnershipError:
        raise RawCaptureServiceIntegrityError(
            "raw_capture_authority_invalid"
        ) from None
    try:
        paths = ShadowRuntimePaths(
            desired_state=base.paths.desired_state_db,
            scheduler=config.source_paths.scheduler_db,
            capacity_ledger=config.source_paths.capacity_ledger_db,
            execution_receipts=config.source_paths.execution_receipts_db,
            raw_store_root=config.source_paths.raw_store_root,
        )
        resolver = _ParentBrokerSecretResolver(
            secret_resolver or EnvironmentSecretResolver(),
            netatmo_broker_configured=(
                netatmo_access_token_broker is not None
            ),
            eumetsat_broker_configured=(
                eumetsat_access_token_broker is not None
            ),
        )
        acquisition = create_shadow_candidate(
            paths,
            capacity_profile=config.capacity_profile,
            raw_store_limits=config.raw_store_limits,
            secret_resolver=resolver,
            executor_factory=executor_factory,
            netatmo_access_token_broker=netatmo_access_token_broker,
            eumetsat_access_token_broker=eumetsat_access_token_broker,
            allowed_workload_ids=frozenset(config.allowed_workload_ids),
            collector_scope=COLLECTOR_SCOPE_ALL_SOURCES,
            clock=clock,
        )
        blitzortung_sidecar: Any | None = None
        if config.blitzortung_enabled:
            blitzortung_sidecar = blitzortung_sidecar_factory(
                control_root=config.blitzortung_control_root,
                raw_store_root=config.blitzortung_raw_store_root,
                raw_store_limits=config.blitzortung_raw_store_limits,
                ingress_limits=config.blitzortung_ingress_limits,
                access_retention_authority=(
                    blitzortung_access_retention_authority
                ),
                keepalive_seconds=config.blitzortung_keepalive_seconds,
                clock=clock,
            )
            if not all(
                callable(getattr(blitzortung_sidecar, name, None))
                for name in (
                    "diagnostics",
                    "start",
                    "stop",
                    "update_regions",
                )
            ):
                raise RawCaptureServiceConfigError(
                    "raw_capture_event_ingress_sidecar_invalid"
                )
        storage_capacity = config.storage_capacity_diagnostics
        active_rules = tuple(
            ACTIVATION_RULES[workload_id]
            for workload_id in config.allowed_workload_ids
        )
        cycle_reserve_bytes = sum(
            int(rule.maximum_transport_bytes or 0)
            for rule in active_rules
        )
        cycle_reserve_entries = sum(
            int(rule.maximum_artifacts or 0)
            for rule in active_rules
        )
        cycle_reserve_captures = len(active_rules)
        retention_guard_config = RawCaptureRetentionGuardConfig(
            cadence_raw_root=config.source_paths.raw_store_root,
            cadence_ledger_path=config.source_paths.capacity_ledger_db,
            event_control_root=config.blitzortung_control_root,
            event_raw_root=config.blitzortung_raw_store_root,
            event_capacity_claimed=config.blitzortung_enabled,
            configured_persistent_max_bytes=(
                storage_capacity["combined_persistent_volume"][
                    "configured_max_bytes"
                ]
                + config.capacity_profile.max_catalog_bytes
            ),
            configured_raw_object_max_entries=(
                storage_capacity["combined_persistent_volume"][
                    "raw_object_max_entries"
                ]
            ),
            cadence_ledger_max_bytes=(
                config.capacity_profile.global_max_bytes
            ),
            cadence_ledger_max_entries=(
                config.capacity_profile.global_max_entries
            ),
            cadence_ledger_max_reservations=(
                config.capacity_profile.max_reservations
            ),
            cadence_ledger_catalog_max_bytes=(
                config.capacity_profile.max_catalog_bytes
            ),
            cadence_raw_max_bytes=config.raw_store_limits.max_total_bytes,
            cadence_raw_max_entries=(
                config.raw_store_limits.max_object_entries
            ),
            cadence_raw_max_receipts=(
                config.raw_store_limits.max_receipt_entries
            ),
            cadence_raw_max_captures=(
                config.raw_store_limits.max_capture_entries
            ),
            cadence_raw_catalog_max_bytes=(
                config.raw_store_limits.max_catalog_bytes
            ),
            cadence_cycle_reserve_bytes=cycle_reserve_bytes,
            cadence_cycle_reserve_entries=cycle_reserve_entries,
            cadence_cycle_reserve_captures=cycle_reserve_captures,
            event_payload_reserve_bytes=BLITZORTUNG_MAX_PAYLOAD_BYTES,
            event_control_receipt_max_bytes=(
                BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES
            ),
            event_control_intake_reserve=(
                config.blitzortung_ingress_limits.control_receipt_reserve
            ),
            event_control_receipt_capacity=(
                config.blitzortung_ingress_limits.max_control_receipts
            ),
            event_queue_capacity=(
                config.blitzortung_ingress_limits.max_queue_entries
            ),
            event_retained_payload_max_bytes=(
                config.blitzortung_ingress_limits.max_retained_payload_bytes
            ),
            event_retained_payload_max_entries=(
                config.blitzortung_ingress_limits.max_retained_payload_entries
            ),
            host_operational_free_reserve_bytes=(
                config.host_operational_free_reserve_bytes
            ),
        )
        retention_guard = RawCaptureRetentionGuard(
            config=retention_guard_config,
            ownership_assertion=lease.assert_held,
            ledger_stats_supplier=acquisition.capacity_ledger.stats,
            ledger_audit_supplier=acquisition.capacity_ledger.audit,
            raw_store_stats_supplier=acquisition.raw_store.stats,
            raw_store_audit_supplier=acquisition.raw_store.integrity_check,
            event_diagnostics_supplier=(
                blitzortung_sidecar.diagnostics
                if blitzortung_sidecar is not None
                else None
            ),
        )
        return RawCaptureServiceRuntime(
            acquisition=acquisition,
            ownership_lease=lease,
            config_sha256=config.fingerprint,
            authority_sha256=expected_authority_sha256,
            allowed_workload_ids=config.allowed_workload_ids,
            blitzortung_enabled=config.blitzortung_enabled,
            blitzortung_sidecar=blitzortung_sidecar,
            retention_guard=retention_guard,
            storage_capacity=storage_capacity,
            acquisition_timeout_seconds=config.acquisition_timeout_seconds,
            stop_timeout_seconds=config.stop_timeout_seconds,
        )
    except Exception:
        try:
            lease.release()
        except WorkerFirstOwnershipError:
            pass
        raise


__all__ = [
    "ADMISSION_POLICY",
    "BLITZORTUNG_AUTHORITY_ENV",
    "BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES",
    "BLITZORTUNG_CONTROL_STAGING_RECOVERY_EXTRA_ENTRIES",
    "BLITZORTUNG_EVENT_MAX_CONTROL_RECEIPTS",
    "BLITZORTUNG_EVENT_MAX_QUEUE_ENTRIES",
    "BLITZORTUNG_EVENT_MAX_RETAINED_ENTRIES",
    "BLITZORTUNG_EVENT_MAX_SESSIONS",
    "BLITZORTUNG_EVENT_STATUS_CONTRACT",
    "CONFIG_CONTRACT",
    "CONTROL_JOB_TYPES",
    "DASHBOARD_OUTPUT",
    "DOWNSTREAM_CONSUMPTION_REQUIRED",
    "HOST_OPERATIONAL_FREE_RESERVE_BYTES",
    "METEOROLOGICAL_OUTPUT",
    "MODE",
    "RAW_CAPTURE_ALLOWED_WORKLOAD_IDS",
    "RAW_PAYLOAD_TRANSPORT",
    "RAW_STORE_COMMIT_IS_TERMINAL",
    "RUNTIME_CONTRACT",
    "RawCaptureServiceBusy",
    "RawCaptureServiceConfig",
    "RawCaptureServiceConfigError",
    "RawCaptureServiceError",
    "RawCaptureServiceIntegrityError",
    "RawCaptureServiceRuntime",
    "SCIENCE_MODE",
    "TERMINAL_ROUTER_WIRED",
    "build_raw_capture_service",
]
