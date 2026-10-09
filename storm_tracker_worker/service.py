"""Bounded Storm Tracker MQTT worker with isolated serial execution."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from contextlib import closing
import hashlib
import importlib
import json
import logging
import math
import multiprocessing
import os
from pathlib import Path
import pickle
import queue
import re
import signal
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field, replace
from multiprocessing.connection import Connection
from types import MappingProxyType
from typing import Any, Callable, Mapping, NoReturn

from . import __version__
from .autonomous_scheduler import (
    AutonomousScheduler,
    AutonomousSchedulerError,
    ClaimedJob,
    ScheduleIntentError,
    SchedulerCapacityError,
    SchedulerClockError,
    SchedulerConflict,
    SchedulerDesiredStateError,
)
from .operational_snapshot_runtime import OperationalSnapshotRuntime
from .protocol import DEFAULT_TOPIC_ROOT, decode, encode, envelope, job_payload, topic
from .worker_first_service_activation import (
    RAW_CAPTURE_SHADOW_MODE,
    WORKER_ONLY_MODE,
    WorkerFirstServiceActivationPlan,
    load_worker_first_service_activation,
)

_LOGGER = logging.getLogger("storm_tracker_worker")
JOB_STATUS_CACHE_MAX = 32
JOB_RECEIPT_RETENTION_SECONDS = 7 * 24 * 60 * 60
JOB_RECEIPT_MAX_ROWS = 10000
JOB_RECEIPT_SCHEMA_VERSION = 1
CHILD_POLL_SECONDS = 0.05
CHILD_JOIN_SECONDS = 1.0
OPERATIONAL_SNAPSHOT_ACK_POLL_SECONDS = 0.05
OPERATIONAL_SNAPSHOT_PUBLISH_TIMEOUT_SECONDS = 5.0
OPERATIONAL_SNAPSHOT_STATUS_WAKE_MIN_SECONDS = 2.0
OPERATIONAL_SNAPSHOT_THREAD_JOIN_SECONDS = 7.0
WORKER_FIRST_STOP_RETRY_SECONDS = 0.05
WORKER_FIRST_STOP_MAX_ATTEMPTS = 20
WORKER_FIRST_STOP_TOTAL_SECONDS = 7.0
WORKER_FIRST_FATAL_WATCHDOG_SECONDS = 8.0
WORKER_FIRST_CANARY_RUNTIME_CONTRACT = (
    "stv3-worker-first-service-composition-runtime-v2"
)
WORKER_FIRST_CANARY_MODE = "service_worker_first_canary_opt_in"
WORKER_FIRST_CANARY_ADMISSION = {
    "allowed_workload_ids": ["radar.dwd.radolan_rv"],
    "policy": "exact_dwd_radolan_rv_only_v1",
}
RAW_CAPTURE_RUNTIME_CONTRACT = "stv3-worker-raw-capture-runtime-v5"
RAW_CAPTURE_SERVICE_MODE = "service_raw_capture_shadow"
WORKER_ONLY_RUNTIME_CONTRACT = "stv3-worker-only-service-runtime-v1"
WORKER_ONLY_SERVICE_MODE = "service_worker_only"
RAW_CAPTURE_BLITZORTUNG_EVENT_STATUS_CONTRACT = (
    "stv3-worker-blitzortung-raw-shadow-status-v2"
)
RAW_CAPTURE_BLITZORTUNG_AUTHORITY_ENV = (
    "STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY"
)
RAW_CAPTURE_CONTROL_JOB_TYPES = frozenset({
    "desired_state_apply",
    "desired_state_claim",
    "desired_state_snapshot",
})
RAW_CAPTURE_RAW_PAYLOAD_TRANSPORT = "worker_direct_not_mqtt"
RAW_CAPTURE_ADMISSION_POLICY = "closed_active_raw_matrix_all_sources_v1"
RAW_CAPTURE_ACQUISITION_RUNTIME_CONTRACT = (
    "stv3-worker-acquisition-shadow-runtime-v3"
)
RAW_CAPTURE_RETENTION_GUARD_CONTRACT = (
    "stv3-worker-raw-capture-retention-guard-v2"
)
RAW_CAPTURE_RETENTION_GUARD_ALARM_PRIORITY = (
    "exclusive_process_owner_not_held",
    "path_identity_reproof_failed",
    "storage_integrity_failed",
    "filesystem_envelope_capacity_exhausted",
    "cadence_cycle_byte_capacity_exhausted",
    "cadence_cycle_entry_capacity_exhausted",
    "cadence_capture_capacity_exhausted",
    "cadence_reservation_capacity_exhausted",
    "cadence_catalog_capacity_exhausted",
    "cadence_ledger_catalog_capacity_exhausted",
    "event_capacity_exhausted",
    "event_capacity_claim_not_available",
    "filesystem_envelope_capacity_low",
    "cadence_cycle_byte_capacity_low",
    "cadence_cycle_entry_capacity_low",
    "cadence_capture_capacity_low",
    "cadence_reservation_capacity_low",
    "cadence_catalog_capacity_low",
    "cadence_ledger_catalog_capacity_low",
    "event_capacity_low",
    "event_transport_degraded",
    "retention_guard_not_inspected",
)
RAW_CAPTURE_RETENTION_GUARD_BLOCKING_ALARMS = frozenset({
    "cadence_catalog_capacity_exhausted",
    "cadence_capture_capacity_exhausted",
    "cadence_cycle_byte_capacity_exhausted",
    "cadence_cycle_entry_capacity_exhausted",
    "cadence_ledger_catalog_capacity_exhausted",
    "cadence_reservation_capacity_exhausted",
    "event_capacity_exhausted",
    "event_capacity_claim_not_available",
    "exclusive_process_owner_not_held",
    "filesystem_envelope_capacity_exhausted",
    "path_identity_reproof_failed",
    "retention_guard_not_inspected",
    "storage_integrity_failed",
})
RAW_CAPTURE_HOST_OPERATIONAL_FREE_RESERVE_BYTES = 2 * 1024 * 1024 * 1024
RAW_CAPTURE_MAX_CONTROL_RESULT_BYTES = 64 * 1024
_NETATMO_OAUTH_SEED_KEYS = (
    "STV3_NETATMO_CLIENT_ID",
    "STV3_NETATMO_CLIENT_SECRET",
    "STV3_NETATMO_REFRESH_TOKEN",
)
_EUMETSAT_OAUTH_SEED_KEYS = (
    "STV3_EUMETSAT_CONSUMER_KEY",
    "STV3_EUMETSAT_CONSUMER_SECRET",
)
WORKER_FIRST_FATAL_EXIT_CODE = 70
OPERATIONAL_SNAPSHOT_MAX_COUNTER = 1_000_000_000
JOB_PRIORITY_CONTROL = {
    "ping",
    "desired_state_claim",
    "desired_state_snapshot",
    "operational_snapshot_head",
}
JOB_PRIORITY_GROUND = {
    "netatmo_fetch",
    "synoptic_fetch",
    "metar_fetch",
}
JOB_PRIORITY_METEOROLOGICAL = {
    "desired_state_apply",
    "operational_snapshot_commit",
    "tracking_operational",
    "motion_preflight",
    "virtual_probe_operational",
    "dwd_radolan_fetch",
    "odim_hdf5_fetch",
    "opera_hdf5_fetch",
    "enterprise_rainrate_fetch",
    "rainviewer_fetch",
    "kmi_fetch",
    "knmi_fetch",
    "aemet_radar_fetch",
    "dpc_radar_fetch",
    "meteofrance_radar_fetch",
    "hsaf_h40b_fetch",
    "goes_rrqpe_fetch",
    "radar_frame_operational",
    "radar_frame_from_provider_artifacts",
    "geojson_operational",
}
JOB_PRIORITY_LOW = {
    "calibration_operational",
    "calibration_prepare_v2",
    "calibration_corpus_ingest",
    "calibration_corpus_snapshot",
    "phase3_replay",
}
JOB_PRIORITY_NAMES = MappingProxyType({
    0: "control",
    1: "ground",
    2: "meteorological",
    3: "standard",
    4: "background",
})


class _OperationalSnapshotMqttError(RuntimeError):
    """One operational snapshot was not acknowledged by MQTT."""


class _WorkerFirstRuntimeFatalError(RuntimeError):
    """The selected worker-first runtime failed and stopped the service."""


def _worker_first_safe_error_reason(error: BaseException) -> str:
    """Project only an explicit secret-free runtime reason code to logs."""

    reason = str(error)
    if re.fullmatch(r"[a-z][a-z0-9_]{0,95}", reason) is None:
        return "unclassified"
    return reason


def _raw_capture_executor_factory(wall_seconds: float):
    """Build one fresh raw-capture child with an exact environment allowlist."""

    # Keep legacy and the DWD canary import-inert.  This factory is reachable
    # only from the explicit raw-capture composition root.
    from .acquisition_runtime import raw_capture_executor_factory

    return raw_capture_executor_factory(wall_seconds)


def _netatmo_oauth_seeds_configured(
    environment: Mapping[str, str],
) -> bool:
    """Check presence only; no OAuth seed is retained or exposed."""

    return all(
        isinstance(value := environment.get(key), str)
        and bool(value)
        and len(value) <= 4096
        and value.isascii()
        and all(
            0x21 <= ord(character) < 0x7F
            for character in value
        )
        for key in _NETATMO_OAUTH_SEED_KEYS
    )


def _eumetsat_oauth_seeds_configured(
    environment: Mapping[str, str],
) -> bool:
    """Require both parent-only client-credential seeds, presence only."""

    return all(
        isinstance(value := environment.get(key), str)
        and bool(value)
        and len(value) <= 4096
        and value.isascii()
        and all(
            0x21 <= ord(character) < 0x7F
            for character in value
        )
        for key in _EUMETSAT_OAUTH_SEED_KEYS
    )


def _broker_raw_capture_netatmo_access_token(
    payload: Mapping[str, Any],
    deadline: float,
) -> str:
    """Refresh in the non-killable parent immediately before child spawn."""

    netatmo = importlib.import_module("storm_tracker_worker.netatmo")
    return netatmo.broker_netatmo_access_token(
        dict(payload),
        deadline=deadline,
    )


def _eumetsat_access_token_broker(
    environment: Mapping[str, str],
) -> Callable[[float], str] | None:
    """Build a parent-only mint callback without copying its seed bundle."""

    if not _eumetsat_oauth_seeds_configured(environment):
        return None

    def broker(deadline: float) -> str:
        runtime = importlib.import_module(
            "storm_tracker_worker.acquisition_runtime"
        )
        lightning = importlib.import_module(
            "storm_tracker_worker.lightning_exact_raw"
        )
        consumer_key = environment.get("STV3_EUMETSAT_CONSUMER_KEY", "")
        consumer_secret = environment.get(
            "STV3_EUMETSAT_CONSUMER_SECRET", ""
        )
        token = None
        authorization = ""
        child_token = ""
        try:
            try:
                if (
                    isinstance(deadline, bool)
                    or not isinstance(deadline, (int, float))
                    or not math.isfinite(float(deadline))
                ):
                    raise runtime.ParentCredentialPermanentError()
                remaining = float(deadline) - time.time()
                if remaining <= 0.0:
                    raise TimeoutError
                if remaining > 50.0:
                    raise runtime.ParentCredentialPermanentError()
                provider_deadline_at = time.monotonic() + remaining
                token = lightning.mint_eumetsat_access_token_parent(
                    consumer_key,
                    consumer_secret,
                    deadline_at=provider_deadline_at,
                )
                authorization = token.authorization_value()
                if not authorization.startswith("Bearer "):
                    raise lightning.EumetsatOAuthError(
                        "EUMETSAT bearercontract is ongeldig"
                    )
                child_token = authorization[7:]
                if (
                    not 16 <= len(child_token) <= 8192
                    or not child_token.isascii()
                    or any(
                        ord(character) <= 0x20
                        or ord(character) == 0x7F
                        for character in child_token
                    )
                ):
                    raise lightning.EumetsatOAuthError(
                        "EUMETSAT bearercontract is ongeldig"
                    )
                return child_token
            except TimeoutError:
                raise
            except lightning.LightningRawTransportError:
                raise runtime.ParentCredentialTemporaryError() from None
            except (
                lightning.EumetsatOAuthError,
                lightning.LightningRawCapacityError,
                lightning.LightningRawContractError,
            ):
                raise runtime.ParentCredentialPermanentError() from None
            except Exception:
                raise runtime.ParentCredentialPermanentError() from None
        finally:
            closer = getattr(token, "close", None)
            if callable(closer):
                closer()
            authorization = ""
            child_token = ""
            consumer_key = ""
            consumer_secret = ""
            provider_deadline_at = 0.0
            remaining = 0.0

    return broker


TERMINAL_JOB_STATES = frozenset(
    {"completed", "failed", "expired", "cancelled"}
)
WORKER_TECHNICAL_SOURCE_CONTRACT = (
    "stv3-worker-technical-diagnostics-source-v1"
)
WORKER_TECHNICAL_SOURCE_FIELDS = frozenset({
    "build_sha256",
    "cache_age_seconds",
    "cache_status",
    "capacity_alarm",
    "config_sha256",
    "contract",
    "current_version",
    "desired_state_sha256",
    "failed_jobs",
    "frame_age_seconds",
    "frame_status",
    "headroom_bytes",
    "headroom_percent",
    "next_version",
    "provider_age_seconds",
    "provider_status",
    "queue_depth",
    "replay_algorithm_version",
    "replay_contract_version",
    "retention_alarm",
    "storage_reason_codes",
    "terminal_jobs",
    "transition_reason_codes",
    "transition_status",
    "worker_observed_at",
    "worker_state",
    "worker_version",
})
WORKER_TECHNICAL_RETENTION_GUARD_FIELDS = (
    "admission_ready",
    "alarm_codes",
    "capacity",
    "capacity_policy",
    "contract",
    "event_capacity_claimed",
    "full_audit_completed",
    "inspection_count",
    "live_delete_performed",
    "ownership",
    "reason_code",
    "reclamation",
    "unmanaged_data_action",
)
WORKER_TECHNICAL_RETENTION_CAPACITY_FIELDS = (
    "cadence_catalog_headroom_bytes",
    "cadence_capture_headroom",
    "cadence_capture_max_entries",
    "cadence_cycle_reserve_bytes",
    "cadence_cycle_reserve_captures",
    "cadence_cycle_reserve_entries",
    "cadence_ledger_byte_headroom",
    "cadence_ledger_catalog_headroom_bytes",
    "cadence_ledger_catalog_max_bytes",
    "cadence_ledger_entry_headroom",
    "cadence_ledger_max_bytes",
    "cadence_ledger_max_entries",
    "cadence_ledger_max_reservations",
    "cadence_raw_byte_headroom",
    "cadence_raw_catalog_max_bytes",
    "cadence_raw_entry_headroom",
    "cadence_raw_max_bytes",
    "cadence_raw_max_entries",
    "cadence_receipt_headroom",
    "cadence_receipt_max_entries",
    "cadence_reservation_headroom",
    "configured_persistent_max_bytes",
    "configured_raw_object_max_entries",
    "event_control_headroom",
    "event_payload_byte_headroom",
    "event_payload_entry_headroom",
    "filesystem_free_bytes",
    "filesystem_total_bytes",
    "host_operational_free_headroom_bytes",
    "host_operational_free_reserve_bytes",
    "persistent_envelope_free_required_bytes",
    "physical_reserve_headroom_bytes",
)
WORKER_TECHNICAL_RETENTION_POSITIVE_CAPACITY_FIELDS = (
    "cadence_capture_max_entries",
    "cadence_cycle_reserve_bytes",
    "cadence_cycle_reserve_captures",
    "cadence_cycle_reserve_entries",
    "cadence_ledger_catalog_max_bytes",
    "cadence_ledger_max_bytes",
    "cadence_ledger_max_entries",
    "cadence_ledger_max_reservations",
    "cadence_raw_catalog_max_bytes",
    "cadence_raw_max_bytes",
    "cadence_raw_max_entries",
    "cadence_receipt_max_entries",
    "configured_persistent_max_bytes",
    "configured_raw_object_max_entries",
    "filesystem_total_bytes",
    "host_operational_free_reserve_bytes",
)
WORKER_TECHNICAL_RETENTION_BYTE_HEADROOM_FIELDS = (
    "cadence_catalog_headroom_bytes",
    "cadence_ledger_byte_headroom",
    "cadence_ledger_catalog_headroom_bytes",
    "cadence_raw_byte_headroom",
    "physical_reserve_headroom_bytes",
)
WORKER_TECHNICAL_RETENTION_INVALID_PROOF_ALARMS = frozenset({
    "exclusive_process_owner_not_held",
    "path_identity_reproof_failed",
    "retention_guard_not_inspected",
})
WORKER_TECHNICAL_RETENTION_OWNERSHIP = MappingProxyType({
    "exclusive_filesystem_owner_proven": False,
    "exclusive_process_owner_held": True,
    "external_writer_exclusion_proven": False,
    "path_identity_reproved": True,
    "same_filesystem_reproved": True,
})
WORKER_TECHNICAL_RETENTION_RECLAMATION = MappingProxyType({
    "automatic_reclaim": False,
    "live_apply_authorized": False,
    "live_apply_wired": False,
    "policy": "offline_five_role_only_never_live_apply_v1",
    "source_reclaim_authorized": False,
})
NONTERMINAL_JOB_STATE_ORDER = {
    "queued": 0,
    "running": 1,
}
SCHEDULER_MIN_POLL_SECONDS = 0.1
SCHEDULER_MAX_POLL_SECONDS = 60.0
SCHEDULER_TERMINAL_RETRY_CODES = MappingProxyType({
    "failed": "service_job_failed",
    "expired": "service_job_expired",
    "cancelled": "service_job_cancelled",
})


def _runtime_build_sha256() -> str:
    """Return the complete content address of the active worker Python tree."""
    digest = hashlib.sha256()
    package = Path(__file__).resolve().parent
    for path in sorted(package.rglob("*.py")):
        digest.update(path.relative_to(package).as_posix().encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _runtime_build_id() -> str:
    """Keep the legacy compact build id without weakening the full identity."""
    return _runtime_build_sha256()[:16]


@dataclass(frozen=True)
class Job:
    job_id: str
    job_type: str
    deadline: float
    payload: dict
    # Parent-owned execution material is deliberately excluded from the MQTT
    # request digest, receipts, repr and user payload.  It only crosses the
    # private spawn pipe to the selected handler.
    execution_context: dict | None = field(
        default=None,
        repr=False,
        compare=False,
    )


@dataclass(frozen=True, slots=True, repr=False)
class _SchedulerLease:
    """Parent-only link; the lease token never enters ``Job`` or receipts."""

    job_id: str
    job_type: str
    request_digest: str
    lease_token: str = field(repr=False)


def _job_request_digest(job: Job) -> str:
    """Bind an idempotency key to the exact semantic request."""
    canonical = json.dumps(
        {
            "job_type": job.job_type,
            "payload": job.payload,
        },
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


_REDACTED = "[redacted]"
_SENSITIVE_KEYS = frozenset({
    "access_token",
    "refresh_token",
    "client_secret",
    "password",
    "authorization",
    "api_key",
    "apikey",
    "token",
})


def _sensitive_key(value: object) -> bool:
    normalized = str(value).strip().lower().replace("-", "_")
    return (
        normalized in _SENSITIVE_KEYS
        or normalized.endswith("_token")
        or normalized.endswith("_secret")
        or normalized.endswith("_password")
        or normalized.endswith("_api_key")
    )


def _job_secret_values(job: Job) -> tuple[str, ...]:
    """Collect only known secret material for output redaction."""

    values: set[str] = set()

    def collect(value: object, *, key: object = "") -> None:
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                collect(nested_value, key=nested_key)
            return
        if isinstance(value, (list, tuple)):
            for nested_value in value:
                collect(nested_value, key=key)
            return
        if _sensitive_key(key) and isinstance(value, str) and len(value) >= 4:
            values.add(value)

    collect(job.payload)
    collect(job.execution_context or {})
    for name, value in os.environ.items():
        if _sensitive_key(name) and len(value) >= 4:
            values.add(value)
    return tuple(sorted(values, key=len, reverse=True))


def _redact_text(value: str, secrets: tuple[str, ...]) -> str:
    redacted = value
    for secret in secrets:
        redacted = redacted.replace(secret, _REDACTED)
    redacted = re.sub(
        r"(?i)(bearer\s+)[^\s,;]+",
        rf"\1{_REDACTED}",
        redacted,
    )
    redacted = re.sub(
        r"(?i)((?:access_token|refresh_token|client_secret|password|"
        r"api_?key|token)=)[^&\s,;]+",
        rf"\1{_REDACTED}",
        redacted,
    )
    return redacted


def _redact_external_value(
    value: object,
    *,
    secrets: tuple[str, ...],
    key: object = "",
) -> object:
    """Prevent credentials from crossing into MQTT, receipts or logs."""

    if _sensitive_key(key):
        return _REDACTED
    if isinstance(value, dict):
        return {
            nested_key: _redact_external_value(
                nested_value,
                secrets=secrets,
                key=nested_key,
            )
            for nested_key, nested_value in value.items()
        }
    if isinstance(value, list):
        return [
            _redact_external_value(item, secrets=secrets)
            for item in value
        ]
    if isinstance(value, tuple):
        return tuple(
            _redact_external_value(item, secrets=secrets)
            for item in value
        )
    if isinstance(value, str):
        return _redact_text(value, secrets)
    return value


def _raw_capture_control_result(
    job_type: str,
    value: object,
) -> dict[str, Any]:
    """Validate the complete small MQTT result contract for raw mode.

    Provider responses never enter this path.  Exact operation-specific keys
    keep a future runtime change from accidentally publishing target documents,
    raw bytes, provider payloads or arbitrary diagnostics over MQTT.
    """

    if job_type not in RAW_CAPTURE_CONTROL_JOB_TYPES or type(value) is not dict:
        raise ValueError("raw_capture_control_result_invalid")
    result = dict(value)
    status = result.get("status")
    if job_type == "desired_state_claim":
        expected = {
            "authority_epoch",
            "claim_id",
            "claimed_at",
            "method",
            "producer_id",
            "status",
        }
        valid_status = status in {"claimed", "claim_idempotent"}
    elif job_type == "desired_state_apply":
        expected = {
            "authority_epoch",
            "first_received_at",
            "generation",
            "issued_at",
            "last_received_at",
            "method",
            "producer_id",
            "receipt_count",
            "state_sha256",
            "status",
            "target_count",
        }
        valid_status = status in {"activated", "idempotent"}
    else:
        if status == "unconfigured":
            expected = {"method", "status"}
            if "producer_id" in result or "authority_epoch" in result:
                expected |= {"authority_epoch", "producer_id"}
            valid_status = True
        else:
            expected = {
                "activated_at",
                "authority_epoch",
                "first_received_at",
                "generation",
                "issued_at",
                "last_received_at",
                "method",
                "producer_id",
                "receipt_count",
                "state_sha256",
                "status",
                "target_count",
            }
            valid_status = status == "configured"
    if set(result) != expected or not valid_status:
        raise ValueError("raw_capture_control_result_invalid")
    for key, nested in result.items():
        if nested is None or type(nested) in {str, int, float, bool}:
            if isinstance(nested, float) and not math.isfinite(nested):
                raise ValueError("raw_capture_control_result_invalid")
            continue
        raise ValueError("raw_capture_control_result_binary_forbidden")
    identifier = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$")
    sha256 = re.compile(r"^[0-9a-f]{64}$")

    def valid_id(name: str) -> bool:
        nested = result.get(name)
        return isinstance(nested, str) and identifier.fullmatch(nested) is not None

    def valid_int(name: str, *, minimum: int = 0) -> bool:
        nested = result.get(name)
        return type(nested) is int and minimum <= nested <= 2**63 - 1

    def valid_time(name: str) -> bool:
        nested = result.get(name)
        return (
            not isinstance(nested, bool)
            and isinstance(nested, (int, float))
            and math.isfinite(float(nested))
            and 0.0 <= float(nested) <= float(2**53)
        )

    if result.get("method") != "worker_desired_state_store_v1":
        raise ValueError("raw_capture_control_result_invalid")
    if job_type == "desired_state_claim":
        semantic_valid = (
            valid_id("producer_id")
            and valid_id("claim_id")
            and valid_int("authority_epoch", minimum=1)
            and valid_time("claimed_at")
        )
    elif status == "unconfigured":
        semantic_valid = set(result) == {"method", "status"} or (
            valid_id("producer_id")
            and valid_int("authority_epoch", minimum=1)
        )
    else:
        semantic_valid = (
            isinstance(result.get("state_sha256"), str)
            and sha256.fullmatch(result["state_sha256"]) is not None
            and valid_id("producer_id")
            and valid_int("authority_epoch", minimum=1)
            and valid_int("generation")
            and valid_time("issued_at")
            and valid_time("first_received_at")
            and valid_time("last_received_at")
            and valid_int("receipt_count", minimum=1)
            and valid_int("target_count")
            and result["target_count"] <= 64
            and (
                job_type != "desired_state_snapshot"
                or valid_time("activated_at")
            )
        )
    if not semantic_valid:
        raise ValueError("raw_capture_control_result_invalid")
    try:
        encoded = json.dumps(
            result,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        raise ValueError("raw_capture_control_result_invalid") from None
    if len(encoded) > RAW_CAPTURE_MAX_CONTROL_RESULT_BYTES:
        raise ValueError("raw_capture_control_result_capacity_exceeded")
    return result


def _raw_capture_ping_result(value: object) -> dict[str, Any]:
    """Keep raw-mode liveness a scalar, empty-echo contract."""

    if (
        type(value) is not dict
        or set(value) != {"echo", "worker_time"}
        or value.get("echo") != {}
        or isinstance(value.get("worker_time"), bool)
        or not isinstance(value.get("worker_time"), (int, float))
        or not math.isfinite(float(value["worker_time"]))
    ):
        raise ValueError("raw_capture_ping_result_invalid")
    return dict(value)


@dataclass(frozen=True)
class _HandlerReference:
    """Import path for one handler inside a clean spawn process."""

    module: str
    qualname: str


@dataclass(frozen=True)
class _ExecutionOutcome:
    """One child-execution outcome, finalized once by the parent."""

    state: str
    values: dict


@dataclass(frozen=True)
class HandlerEffectContract:
    """Explicit retry/kill semantics for one worker job type."""

    contract_id: str
    effect_class: str
    hard_kill_safe: bool
    preparation: str
    compensation: str
    verified: bool = True

    def __post_init__(self) -> None:
        if type(self.hard_kill_safe) is not bool or type(self.verified) is not bool:
            raise TypeError(
                "hard_kill_safe en verified moeten booleans zijn"
            )
        for name in (
            "contract_id",
            "effect_class",
            "preparation",
            "compensation",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} moet een niet-lege string zijn")
        if self.preparation not in {
            "none",
            "parent_netatmo_oauth_broker_v1",
        }:
            raise ValueError("onbekende worker effectpreparation")


_PURE_COMPUTE_EFFECT = HandlerEffectContract(
    contract_id="stv3-worker-effect-pure-compute-v1",
    effect_class="pure_or_ephemeral_compute",
    hard_kill_safe=True,
    preparation="none",
    compensation="discard_child_state_and_retry_exact_request",
)
_IDEMPOTENT_READ_EFFECT = HandlerEffectContract(
    contract_id="stv3-worker-effect-idempotent-read-v1",
    effect_class="bounded_idempotent_remote_read",
    hard_kill_safe=True,
    preparation="none",
    compensation="retry_exact_request",
)
_READ_ATOMIC_ARTIFACT_EFFECT = HandlerEffectContract(
    contract_id="stv3-worker-effect-read-atomic-artifact-v1",
    effect_class="bounded_read_plus_job_id_atomic_replace",
    hard_kill_safe=True,
    preparation="none",
    compensation="retry_exact_job_id_overwrites_only_atomic_target",
)
_KMI_RAW_CAPTURE_EFFECT = HandlerEffectContract(
    contract_id="stv3-worker-effect-kmi-raw-capture-v1",
    effect_class=(
        "bounded_read_plus_immutable_objects_sqlite_receipts_and_capture"
    ),
    hard_kill_safe=True,
    preparation="none",
    compensation=(
        "retry_exact_job_id_reuses_receipts_adopts_orphan_objects_"
        "and_commits_idempotent_capture"
    ),
)
_DWD_RAW_CAPTURE_EFFECT = HandlerEffectContract(
    contract_id="stv3-worker-effect-dwd-rv-raw-capture-v1",
    effect_class=(
        "bounded_read_plus_immutable_objects_sqlite_receipts_and_capture"
    ),
    hard_kill_safe=True,
    preparation="none",
    compensation=(
        "retry_exact_job_id_reuses_receipt_adopts_orphan_object_"
        "commits_idempotent_capture_and_replaces_legacy_artifact_atomically"
    ),
)
_ATOMIC_ARTIFACT_EFFECT = HandlerEffectContract(
    contract_id="stv3-worker-effect-atomic-artifact-v1",
    effect_class="job_id_atomic_replace",
    hard_kill_safe=True,
    preparation="none",
    compensation="retry_exact_job_id_overwrites_only_atomic_target",
)
_TRANSACTIONAL_CORPUS_EFFECT = HandlerEffectContract(
    contract_id="stv3-worker-effect-transactional-corpus-v1",
    effect_class="bounded_receipted_sqlite_transaction",
    hard_kill_safe=True,
    preparation="none",
    compensation="retry_exact_request_uses_corpus_receipt",
)
_TRANSACTIONAL_DESIRED_STATE_EFFECT = HandlerEffectContract(
    contract_id="stv3-worker-effect-desired-state-cas-v1",
    effect_class="bounded_crash_durable_configuration_cas",
    hard_kill_safe=True,
    preparation="none",
    compensation="retry_exact_state_uses_content_hash_and_active_head_cas",
)
_TRANSACTIONAL_OPERATIONAL_SNAPSHOT_EFFECT = HandlerEffectContract(
    contract_id="stv3-worker-effect-operational-snapshot-cas-v1",
    effect_class="bounded_crash_durable_snapshot_publication_cas",
    hard_kill_safe=True,
    preparation="none",
    compensation=(
        "retry_exact_commit_id_uses_snapshot_receipt_and_active_head_cas"
    ),
)
_BROKERED_NETATMO_EFFECT = HandlerEffectContract(
    contract_id="stv3-worker-effect-netatmo-parent-broker-v1",
    effect_class="parent_committed_oauth_then_idempotent_remote_read",
    hard_kill_safe=True,
    preparation="parent_netatmo_oauth_broker_v1",
    compensation="persist_rotated_refresh_before_child_then_retry_query",
)
_UNVERIFIED_COMPATIBILITY_EFFECT = HandlerEffectContract(
    contract_id="stv3-worker-effect-unverified-compatibility-v1",
    effect_class="unclassified",
    hard_kill_safe=True,
    preparation="none",
    compensation="none_declared",
    verified=False,
)

PRODUCTION_JOB_EFFECT_CONTRACTS = MappingProxyType({
    "ping": _PURE_COMPUTE_EFFECT,
    "desired_state_claim": _TRANSACTIONAL_DESIRED_STATE_EFFECT,
    "desired_state_apply": _TRANSACTIONAL_DESIRED_STATE_EFFECT,
    "desired_state_snapshot": _IDEMPOTENT_READ_EFFECT,
    "operational_snapshot_commit": _TRANSACTIONAL_OPERATIONAL_SNAPSHOT_EFFECT,
    "operational_snapshot_head": _IDEMPOTENT_READ_EFFECT,
    "dwd_radolan_fetch": _DWD_RAW_CAPTURE_EFFECT,
    "odim_hdf5_fetch": _READ_ATOMIC_ARTIFACT_EFFECT,
    "opera_hdf5_fetch": _READ_ATOMIC_ARTIFACT_EFFECT,
    "motion_preflight": _PURE_COMPUTE_EFFECT,
    "tracking_operational": _PURE_COMPUTE_EFFECT,
    "virtual_probe_operational": _PURE_COMPUTE_EFFECT,
    "netatmo_fetch": _BROKERED_NETATMO_EFFECT,
    "synoptic_fetch": _IDEMPOTENT_READ_EFFECT,
    "metar_fetch": _IDEMPOTENT_READ_EFFECT,
    "enterprise_rainrate_fetch": _READ_ATOMIC_ARTIFACT_EFFECT,
    "calibration_operational": _PURE_COMPUTE_EFFECT,
    "calibration_prepare_v2": _ATOMIC_ARTIFACT_EFFECT,
    "calibration_corpus_ingest": _TRANSACTIONAL_CORPUS_EFFECT,
    "calibration_corpus_snapshot": _IDEMPOTENT_READ_EFFECT,
    "phase3_replay": _PURE_COMPUTE_EFFECT,
    "geojson_operational": _PURE_COMPUTE_EFFECT,
    "radar_frame_operational": _ATOMIC_ARTIFACT_EFFECT,
    "rainviewer_fetch": _READ_ATOMIC_ARTIFACT_EFFECT,
    "kmi_fetch": _KMI_RAW_CAPTURE_EFFECT,
    "knmi_fetch": _READ_ATOMIC_ARTIFACT_EFFECT,
    "aemet_radar_fetch": _READ_ATOMIC_ARTIFACT_EFFECT,
    "dpc_radar_fetch": _READ_ATOMIC_ARTIFACT_EFFECT,
    "meteofrance_radar_fetch": _READ_ATOMIC_ARTIFACT_EFFECT,
    "open_meteo_fetch": _IDEMPOTENT_READ_EFFECT,
    "goes_glm_fetch": _IDEMPOTENT_READ_EFFECT,
    "eumetsat_li_fetch": _IDEMPOTENT_READ_EFFECT,
    "hsaf_h40b_fetch": _READ_ATOMIC_ARTIFACT_EFFECT,
    "goes_rrqpe_fetch": _READ_ATOMIC_ARTIFACT_EFFECT,
    "radar_frame_from_provider_artifacts": _ATOMIC_ARTIFACT_EFFECT,
})


def _ping(job: Job) -> dict:
    """Built-in liveness handler; top-level so spawn can import it."""
    return {
        "echo": job.payload,
        "worker_time": time.time(),
    }


PRODUCTION_JOB_HANDLERS = MappingProxyType({
    "ping": _ping,
    "desired_state_claim": _HandlerReference(
        "storm_tracker_worker.desired_state", "desired_state_claim"
    ),
    "desired_state_apply": _HandlerReference(
        "storm_tracker_worker.desired_state", "desired_state_apply"
    ),
    "desired_state_snapshot": _HandlerReference(
        "storm_tracker_worker.desired_state", "desired_state_snapshot"
    ),
    "operational_snapshot_commit": _HandlerReference(
        "storm_tracker_worker.operational_snapshot_store",
        "operational_snapshot_commit",
    ),
    "operational_snapshot_head": _HandlerReference(
        "storm_tracker_worker.operational_snapshot_store",
        "operational_snapshot_head",
    ),
    "dwd_radolan_fetch": _HandlerReference(
        "storm_tracker_worker.radar", "dwd_radolan_fetch"
    ),
    "odim_hdf5_fetch": _HandlerReference(
        "storm_tracker_worker.radar", "odim_hdf5_fetch"
    ),
    "opera_hdf5_fetch": _HandlerReference(
        "storm_tracker_worker.opera", "opera_hdf5_fetch"
    ),
    "motion_preflight": _HandlerReference(
        "storm_tracker_worker.motion", "motion_preflight"
    ),
    "tracking_operational": _HandlerReference(
        "storm_tracker_worker.tracking", "tracking_operational"
    ),
    "virtual_probe_operational": _HandlerReference(
        "storm_tracker_worker.virtual_probe", "virtual_probe_operational"
    ),
    "netatmo_fetch": _HandlerReference(
        "storm_tracker_worker.netatmo", "netatmo_fetch"
    ),
    "synoptic_fetch": _HandlerReference(
        "storm_tracker_worker.synoptic", "synoptic_fetch"
    ),
    "metar_fetch": _HandlerReference(
        "storm_tracker_worker.metar", "metar_fetch"
    ),
    "enterprise_rainrate_fetch": _HandlerReference(
        "storm_tracker_worker.enterprise_rainrate",
        "enterprise_rainrate_fetch",
    ),
    "calibration_operational": _HandlerReference(
        "storm_tracker_worker.calibration", "calibration_operational"
    ),
    "calibration_prepare_v2": _HandlerReference(
        "storm_tracker_worker.calibration_stage", "calibration_prepare_v2"
    ),
    "calibration_corpus_ingest": _HandlerReference(
        "storm_tracker_worker.calibration_corpus",
        "calibration_corpus_ingest",
    ),
    "calibration_corpus_snapshot": _HandlerReference(
        "storm_tracker_worker.calibration_corpus",
        "calibration_corpus_snapshot",
    ),
    "phase3_replay": _HandlerReference(
        "storm_tracker_worker.replay", "phase3_replay"
    ),
    "geojson_operational": _HandlerReference(
        "storm_tracker_worker.geojson", "geojson_operational"
    ),
    "radar_frame_operational": _HandlerReference(
        "storm_tracker_worker.radar_frame", "radar_frame_operational"
    ),
    "rainviewer_fetch": _HandlerReference(
        "storm_tracker_worker.provider_radar", "rainviewer_fetch"
    ),
    "kmi_fetch": _HandlerReference(
        "storm_tracker_worker.provider_radar", "kmi_fetch"
    ),
    "knmi_fetch": _HandlerReference(
        "storm_tracker_worker.provider_radar", "knmi_fetch"
    ),
    "aemet_radar_fetch": _HandlerReference(
        "storm_tracker_worker.provider_radar", "aemet_radar_fetch"
    ),
    "dpc_radar_fetch": _HandlerReference(
        "storm_tracker_worker.provider_radar", "dpc_radar_fetch"
    ),
    "meteofrance_radar_fetch": _HandlerReference(
        "storm_tracker_worker.provider_radar", "meteofrance_radar_fetch"
    ),
    "open_meteo_fetch": _HandlerReference(
        "storm_tracker_worker.model_guidance", "open_meteo_fetch"
    ),
    "goes_glm_fetch": _HandlerReference(
        "storm_tracker_worker.lightning", "goes_glm_fetch"
    ),
    "eumetsat_li_fetch": _HandlerReference(
        "storm_tracker_worker.lightning", "eumetsat_li_fetch"
    ),
    "hsaf_h40b_fetch": _HandlerReference(
        "storm_tracker_worker.satellite_radar", "hsaf_h40b_fetch"
    ),
    "goes_rrqpe_fetch": _HandlerReference(
        "storm_tracker_worker.satellite_radar", "goes_rrqpe_fetch"
    ),
    "radar_frame_from_provider_artifacts": _HandlerReference(
        "storm_tracker_worker.radar_frame",
        "radar_frame_from_provider_artifacts",
    ),
})
if set(PRODUCTION_JOB_HANDLERS) != set(PRODUCTION_JOB_EFFECT_CONTRACTS):
    raise RuntimeError(
        "productiehandlers en effectcontracten zijn niet volledig gekoppeld"
    )


def _handler_reference(handler: Callable[[Job], dict]) -> _HandlerReference:
    """Return a spawn-safe import reference or fail before accepting work."""
    module = str(getattr(handler, "__module__", ""))
    qualname = str(getattr(handler, "__qualname__", ""))
    if not module or not qualname or "<locals>" in qualname:
        raise TypeError(
            "Procesgeisoleerde workerhandlers moeten top-level functies zijn"
        )
    if module == "__main__":
        raise TypeError(
            "Procesgeisoleerde workerhandlers mogen niet uit __main__ komen"
        )
    return _HandlerReference(module=module, qualname=qualname)


def _resolve_handler(reference: _HandlerReference) -> Callable[[Job], dict]:
    """Resolve a previously validated handler inside the child process."""
    value = importlib.import_module(reference.module)
    for part in reference.qualname.split("."):
        value = getattr(value, part)
    if not callable(value):
        raise TypeError(
            f"Workerhandler is niet aanroepbaar: "
            f"{reference.module}.{reference.qualname}"
        )
    return value


def _send_child_message(connection: Connection, message: tuple) -> bool:
    """Send one response, degrading unserializable results to an error."""
    try:
        payload = pickle.dumps(
            message,
            protocol=pickle.HIGHEST_PROTOCOL,
        )
    except (AttributeError, pickle.PickleError, TypeError) as err:
        job_id = str(message[1]) if len(message) > 1 else ""
        payload = pickle.dumps(
            (
                "failed",
                job_id,
                {
                    "error": "UnserializableResult",
                    "error_detail": str(err),
                },
                time.time(),
            ),
            protocol=pickle.HIGHEST_PROTOCOL,
        )
    try:
        connection.send_bytes(payload)
        return True
    except (BrokenPipeError, EOFError, OSError):
        return False


def _job_child_main(
    connection: Connection,
    references: dict[str, _HandlerReference],
) -> None:
    """Own all handlers and their process-local caches across serial jobs."""
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except (AttributeError, OSError, ValueError):
        pass
    handlers: dict[str, Callable[[Job], dict]] = {}
    try:
        while True:
            try:
                command = pickle.loads(connection.recv_bytes())
            except (EOFError, OSError, pickle.PickleError):
                break
            if not isinstance(command, tuple) or not command:
                break
            if command[0] == "shutdown":
                break
            if command[0] != "run" or len(command) != 2:
                break
            job = command[1]
            if not isinstance(job, Job):
                break
            secrets = _job_secret_values(job)
            try:
                handler = handlers.get(job.job_type)
                if handler is None:
                    handler = _resolve_handler(references[job.job_type])
                    handlers[job.job_type] = handler
                result = handler(job)
                response = (
                    "completed",
                    job.job_id,
                    {
                        "result": _redact_external_value(
                            result,
                            secrets=secrets,
                        )
                    },
                    time.time(),
                )
            except BaseException as err:
                response = (
                    "failed",
                    job.job_id,
                    {
                        "error": type(err).__name__,
                        "error_detail": _redact_text(str(err), secrets),
                    },
                    time.time(),
                )
            if not _send_child_message(connection, response):
                break
    finally:
        connection.close()


class _SpawnJobExecutor:
    """Persistent spawn child with a hard parent-enforced job deadline."""

    def __init__(
        self,
        handlers: dict[
            str,
            Callable[[Job], dict] | _HandlerReference,
        ],
        effect_contracts: Mapping[str, HandlerEffectContract],
    ) -> None:
        self._context = multiprocessing.get_context("spawn")
        self._references = {
            job_type: (
                handler
                if isinstance(handler, _HandlerReference)
                else _handler_reference(handler)
            )
            for job_type, handler in handlers.items()
        }
        self._effect_contracts = dict(effect_contracts)
        if set(self._references) != set(self._effect_contracts):
            raise ValueError(
                "iedere geïsoleerde workerhandler vereist exact één "
                "effectcontract"
            )
        self._process: multiprocessing.Process | None = None
        self._connection: Connection | None = None
        self._reader: threading.Thread | None = None
        self._responses: queue.Queue[tuple[int, tuple]] = queue.Queue()
        self._generation = 0

    @property
    def alive(self) -> bool:
        process = self._process
        return process is not None and process.is_alive()

    def _read_responses(
        self,
        connection: Connection,
        generation: int,
    ) -> None:
        try:
            while True:
                payload = connection.recv_bytes()
                response = pickle.loads(payload)
                if isinstance(response, tuple):
                    self._responses.put((generation, response))
        except (EOFError, OSError, pickle.PickleError):
            self._responses.put(
                (
                    generation,
                    (
                        "child_closed",
                        "",
                        {"error": "worker_child_closed"},
                        time.time(),
                    ),
                )
            )

    def _start(self) -> None:
        if self.alive:
            return
        self._discard_child(force=True)
        parent_connection, child_connection = self._context.Pipe(
            duplex=True
        )
        self._generation += 1
        generation = self._generation
        process = self._context.Process(
            target=_job_child_main,
            args=(child_connection, self._references),
            name=f"storm-tracker-handler-{generation}",
            daemon=False,
        )
        try:
            process.start()
        except BaseException:
            parent_connection.close()
            child_connection.close()
            raise
        child_connection.close()
        self._process = process
        self._connection = parent_connection
        self._reader = threading.Thread(
            target=self._read_responses,
            args=(parent_connection, generation),
            name=f"storm-tracker-handler-results-{generation}",
            daemon=True,
        )
        self._reader.start()

    def _discard_child(self, *, force: bool) -> None:
        process = self._process
        connection = self._connection
        reader = self._reader
        self._process = None
        self._connection = None
        self._reader = None
        if process is not None and process.is_alive():
            if not force and connection is not None:
                try:
                    connection.send_bytes(
                        pickle.dumps(
                            ("shutdown",),
                            protocol=pickle.HIGHEST_PROTOCOL,
                        )
                    )
                except (BrokenPipeError, EOFError, OSError):
                    pass
                process.join(timeout=CHILD_JOIN_SECONDS)
            if process.is_alive():
                process.terminate()
                process.join(timeout=CHILD_JOIN_SECONDS)
            if process.is_alive() and hasattr(process, "kill"):
                process.kill()
                process.join(timeout=CHILD_JOIN_SECONDS)
        if connection is not None:
            connection.close()
        if reader is not None and reader is not threading.current_thread():
            reader.join(timeout=CHILD_JOIN_SECONDS)
        if process is not None:
            try:
                process.close()
            except (OSError, ValueError):
                pass

    def execute(
        self,
        job: Job,
        stop_event: threading.Event,
    ) -> _ExecutionOutcome:
        """Execute one job or hard-stop its exact child at stop/deadline."""
        contract = self._effect_contracts.get(job.job_type)
        secrets = _job_secret_values(job)
        if contract is None or not contract.hard_kill_safe:
            return _ExecutionOutcome(
                "failed",
                {
                    "error": "unsafe_handler_effect_contract",
                    "effect_contract_id": (
                        contract.contract_id if contract is not None else None
                    ),
                },
            )
        try:
            self._start()
            connection = self._connection
            if connection is None:
                raise RuntimeError("worker_child_connection_missing")
            connection.send_bytes(
                pickle.dumps(
                    ("run", job),
                    protocol=pickle.HIGHEST_PROTOCOL,
                )
            )
        except BaseException as err:
            self._discard_child(force=True)
            return _ExecutionOutcome(
                "failed",
                {
                    "error": "worker_child_start_failed",
                    "error_detail": _redact_text(str(err), secrets),
                },
            )

        generation = self._generation
        monotonic_deadline = time.monotonic() + max(
            0.0,
            job.deadline - time.time(),
        )
        while True:
            try:
                response_generation, response = (
                    self._responses.get_nowait()
                )
            except queue.Empty:
                response_generation = -1
                response = ()
            if response_generation == generation:
                kind = str(response[0]) if response else ""
                response_job_id = str(response[1]) if len(response) > 1 else ""
                values = (
                    _redact_external_value(
                        dict(response[2]),
                        secrets=secrets,
                    )
                    if len(response) > 2 and isinstance(response[2], dict)
                    else {}
                )
                finished_at = (
                    float(response[3])
                    if len(response) > 3
                    else time.time()
                )
                if kind == "boot_failed":
                    self._discard_child(force=True)
                    return _ExecutionOutcome(
                        "failed",
                        {
                            "error": "worker_child_boot_failed",
                            **values,
                        },
                    )
                if kind == "child_closed":
                    self._discard_child(force=True)
                    return _ExecutionOutcome(
                        "failed",
                        {"error": "worker_child_crashed"},
                    )
                if response_job_id != job.job_id:
                    self._discard_child(force=True)
                    return _ExecutionOutcome(
                        "failed",
                        {"error": "worker_child_protocol_error"},
                    )
                if finished_at > job.deadline:
                    self._discard_child(force=True)
                    return _ExecutionOutcome(
                        "expired",
                        {"error": "deadline_exceeded"},
                    )
                if kind in {"completed", "failed"}:
                    return _ExecutionOutcome(kind, values)
                self._discard_child(force=True)
                return _ExecutionOutcome(
                    "failed",
                    {"error": "worker_child_protocol_error"},
                )

            if stop_event.is_set():
                self._discard_child(force=True)
                return _ExecutionOutcome(
                    "cancelled",
                    {"error": "worker_stopping"},
                )
            remaining = min(
                job.deadline - time.time(),
                monotonic_deadline - time.monotonic(),
            )
            if remaining <= 0:
                self._discard_child(force=True)
                return _ExecutionOutcome(
                    "expired",
                    {"error": "deadline_exceeded"},
                )
            try:
                response_generation, response = self._responses.get(
                    timeout=min(CHILD_POLL_SECONDS, remaining)
                )
            except queue.Empty:
                continue
            self._responses.put((response_generation, response))

    def shutdown(self, *, force: bool = False) -> None:
        self._discard_child(force=force)


@dataclass
class ProcessHealth:
    """Liveness en werkstatus van één logisch workerproces."""

    name: str
    state: str = "idle"
    last_heartbeat: float = 0.0
    active_job_id: str | None = None
    active_since: float | None = None
    completed_jobs: int = 0
    failed_jobs: int = 0
    last_duration_seconds: float | None = None
    error: str | None = None


class _JobReceiptStore:
    """Crash-durable workerstatus without involving Home Assistant Recorder."""

    _EXPECTED_TABLES = frozenset({
        "worker_receipt_metadata",
        "worker_job_receipts",
    })
    _METADATA_COLUMNS = frozenset({
        "metadata_key",
        "schema_version",
    })
    _RECEIPT_COLUMNS = frozenset({
        "job_id",
        "job_type",
        "request_digest",
        "deadline",
        "state",
        "values_json",
        "accepted",
        "owner_instance_id",
        "build_id",
        "created_at",
        "updated_at",
    })

    def __init__(
        self,
        path: str | Path,
        *,
        instance_id: str,
        build_id: str,
    ) -> None:
        self.path = Path(path)
        self.instance_id = str(instance_id)
        self.build_id = str(build_id)
        self._lock = threading.RLock()

    def initialize(self) -> tuple[list[dict], list[dict]]:
        """Create the journal and terminalize work orphaned by a prior boot."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        now = time.time()
        recovered: list[dict] = []
        with self._lock, closing(
            self._connect(configure_durability=False)
        ) as db, db:
            self._validate_or_create_schema(db)
            self._configure_durability(db)
            db.execute("BEGIN IMMEDIATE")
            try:
                orphaned = db.execute(
                    """
                    SELECT *
                    FROM worker_job_receipts
                    WHERE state IN ('queued', 'running')
                    ORDER BY created_at, job_id
                    """
                ).fetchall()
                for row in orphaned:
                    values = {
                        "error": "worker_restarted",
                        "error_detail": (
                            "worker herstartte voordat accepted job "
                            f"{row['state']} terminaal werd"
                        ),
                    }
                    values_json = self._values_json(values)
                    db.execute(
                        """
                        UPDATE worker_job_receipts
                        SET state='cancelled', values_json=?,
                            owner_instance_id=?, build_id=?, updated_at=?
                        WHERE job_id=? AND state IN ('queued', 'running')
                        """,
                        (
                            values_json,
                            self.instance_id,
                            self.build_id,
                            now,
                            row["job_id"],
                        ),
                    )
                    recovered.append(
                        {
                            "job_id": str(row["job_id"]),
                            "job_type": str(row["job_type"]),
                            "request_digest": str(row["request_digest"]),
                            "deadline": float(row["deadline"]),
                            "state": "cancelled",
                            "values": values,
                            "accepted": bool(row["accepted"]),
                            "updated_at": now,
                        }
                    )
                db.execute(
                    """
                    DELETE FROM worker_job_receipts
                    WHERE updated_at < ?
                      AND state IN (
                          'completed', 'failed', 'expired', 'cancelled'
                      )
                    """,
                    (now - JOB_RECEIPT_RETENTION_SECONDS,),
                )
                count = int(
                    db.execute(
                        "SELECT count(*) FROM worker_job_receipts"
                    ).fetchone()[0]
                )
                excess = max(0, count - JOB_RECEIPT_MAX_ROWS)
                if excess:
                    db.execute(
                        """
                        DELETE FROM worker_job_receipts
                        WHERE job_id IN (
                            SELECT job_id
                            FROM worker_job_receipts
                            WHERE state IN (
                                'completed', 'failed', 'expired', 'cancelled'
                            )
                            ORDER BY updated_at, job_id
                            LIMIT ?
                        )
                        """,
                        (excess,),
                    )
                db.commit()
            except Exception:
                db.rollback()
                raise
            cached = [
                self._decode_row(row)
                for row in db.execute(
                    """
                    SELECT *
                    FROM worker_job_receipts
                    WHERE state IN (
                        'completed', 'failed', 'expired', 'cancelled'
                    )
                    ORDER BY updated_at DESC, job_id
                    LIMIT ?
                    """,
                    (JOB_STATUS_CACHE_MAX,),
                ).fetchall()
            ]
        cached.reverse()
        return cached, recovered

    def _validate_or_create_schema(self, db: sqlite3.Connection) -> None:
        """Create schema v1 atomically or reject every unknown layout."""
        tables = {
            str(row["name"])
            for row in db.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                """
            ).fetchall()
        }
        if not tables:
            try:
                db.executescript(
                    f"""
                    BEGIN IMMEDIATE;
                    CREATE TABLE worker_receipt_metadata (
                        metadata_key TEXT PRIMARY KEY
                            CHECK(metadata_key='schema'),
                        schema_version INTEGER NOT NULL
                    ) WITHOUT ROWID;
                    INSERT INTO worker_receipt_metadata(
                        metadata_key, schema_version
                    ) VALUES('schema', {JOB_RECEIPT_SCHEMA_VERSION});
                    CREATE TABLE worker_job_receipts (
                        job_id TEXT PRIMARY KEY,
                        job_type TEXT NOT NULL,
                        request_digest TEXT NOT NULL,
                        deadline REAL NOT NULL,
                        state TEXT NOT NULL CHECK(state IN (
                            'queued', 'running', 'completed', 'failed',
                            'expired', 'cancelled'
                        )),
                        values_json TEXT NOT NULL,
                        accepted INTEGER NOT NULL CHECK(accepted IN (0, 1)),
                        owner_instance_id TEXT NOT NULL,
                        build_id TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL
                    ) WITHOUT ROWID;
                    CREATE INDEX idx_worker_receipts_updated
                        ON worker_job_receipts(updated_at);
                    COMMIT;
                    """
                )
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
            tables = set(self._EXPECTED_TABLES)

        if tables != self._EXPECTED_TABLES:
            raise RuntimeError(
                "onbekend of onvolledig worker-receiptschema; "
                f"gevonden tabellen: {sorted(tables)}"
            )

        metadata_rows = db.execute(
            """
            SELECT metadata_key, schema_version
            FROM worker_receipt_metadata
            """
        ).fetchall()
        if (
            len(metadata_rows) != 1
            or str(metadata_rows[0]["metadata_key"]) != "schema"
        ):
            raise RuntimeError(
                "worker-receiptschema heeft geen unieke metadata-identiteit"
            )
        schema_version = int(metadata_rows[0]["schema_version"])
        if schema_version != JOB_RECEIPT_SCHEMA_VERSION:
            raise RuntimeError(
                "onbekende worker-receiptschemaversie "
                f"{schema_version}; verwacht {JOB_RECEIPT_SCHEMA_VERSION}"
            )

        metadata_columns = {
            str(row["name"])
            for row in db.execute(
                "PRAGMA table_info(worker_receipt_metadata)"
            ).fetchall()
        }
        receipt_columns = {
            str(row["name"])
            for row in db.execute(
                "PRAGMA table_info(worker_job_receipts)"
            ).fetchall()
        }
        if metadata_columns != self._METADATA_COLUMNS:
            raise RuntimeError(
                "worker-receiptmetadata heeft onverwachte kolommen"
            )
        if receipt_columns != self._RECEIPT_COLUMNS:
            raise RuntimeError(
                "worker-jobreceipts hebben onverwachte kolommen"
            )

        indexes = {
            str(row["name"])
            for row in db.execute(
                "PRAGMA index_list(worker_job_receipts)"
            ).fetchall()
        }
        if "idx_worker_receipts_updated" not in indexes:
            raise RuntimeError(
                "worker-receiptschema mist de verplichte update-index"
            )
        integrity = [
            str(row[0])
            for row in db.execute("PRAGMA integrity_check").fetchall()
        ]
        if integrity != ["ok"]:
            raise RuntimeError(
                "worker-receiptdatabase faalt integrity_check: "
                + "; ".join(integrity)
            )

    def record(
        self,
        job: Job,
        state: str,
        values: dict,
        *,
        accepted: bool,
    ) -> tuple[dict, bool]:
        """Persist one monotone transition; a terminal state is immutable."""
        if state not in {
            "queued",
            "running",
            "completed",
            "failed",
            "expired",
            "cancelled",
        }:
            raise ValueError(f"ongeldige worker receiptstate: {state}")
        now = time.time()
        request_digest = _job_request_digest(job)
        values_json = self._values_json(values)
        with self._lock, closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            try:
                previous = db.execute(
                    """
                    SELECT *
                    FROM worker_job_receipts
                    WHERE job_id=?
                    """,
                    (job.job_id,),
                ).fetchone()
                if (
                    previous is not None
                    and (
                        str(previous["job_type"]) != job.job_type
                        or str(previous["request_digest"]) != request_digest
                    )
                ):
                    raise ValueError(
                        "job_id werd met een andere requestpayload hergebruikt"
                    )
                if (
                    previous is not None
                    and previous["state"] in TERMINAL_JOB_STATES
                ):
                    db.commit()
                    return self._decode_row(previous), False
                if previous is not None:
                    previous_state = str(previous["state"])
                    previous_order = NONTERMINAL_JOB_STATE_ORDER.get(
                        previous_state
                    )
                    next_order = NONTERMINAL_JOB_STATE_ORDER.get(state)
                    if (
                        state == previous_state
                        or (
                            previous_order is not None
                            and next_order is not None
                            and next_order < previous_order
                        )
                    ):
                        # QoS1 may redeliver an older queued/running status
                        # after a newer status was already made durable.  Keep
                        # the newest receipt byte-for-byte and never regress
                        # the restart state.
                        db.commit()
                        return self._decode_row(previous), False
                if previous is None:
                    db.execute(
                        """
                        INSERT INTO worker_job_receipts(
                            job_id, job_type, request_digest, deadline, state,
                            values_json, accepted, owner_instance_id, build_id,
                            created_at, updated_at
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            job.job_id,
                            job.job_type,
                            request_digest,
                            float(job.deadline),
                            state,
                            values_json,
                            int(bool(accepted)),
                            self.instance_id,
                            self.build_id,
                            now,
                            now,
                        ),
                    )
                else:
                    db.execute(
                        """
                        UPDATE worker_job_receipts
                        SET deadline=?, state=?, values_json=?,
                            accepted=max(accepted, ?),
                            owner_instance_id=?, build_id=?, updated_at=?
                        WHERE job_id=?
                        """,
                        (
                            float(job.deadline),
                            state,
                            values_json,
                            int(bool(accepted)),
                            self.instance_id,
                            self.build_id,
                            now,
                            job.job_id,
                        ),
                    )
                current = db.execute(
                    """
                    SELECT *
                    FROM worker_job_receipts
                    WHERE job_id=?
                    """,
                    (job.job_id,),
                ).fetchone()
                db.commit()
                return self._decode_row(current), True
            except Exception:
                db.rollback()
                raise

    def load(self, job_id: str) -> dict | None:
        with self._lock, closing(self._connect()) as db, db:
            row = db.execute(
                """
                SELECT *
                FROM worker_job_receipts
                WHERE job_id=?
                """,
                (str(job_id),),
            ).fetchone()
            return self._decode_row(row) if row is not None else None

    @staticmethod
    def _values_json(values: dict) -> str:
        return json.dumps(
            values,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @staticmethod
    def _decode_row(row: sqlite3.Row) -> dict:
        return {
            "job_id": str(row["job_id"]),
            "job_type": str(row["job_type"]),
            "request_digest": str(row["request_digest"]),
            "deadline": float(row["deadline"]),
            "state": str(row["state"]),
            "values": json.loads(row["values_json"]),
            "accepted": bool(row["accepted"]),
            "updated_at": float(row["updated_at"]),
        }

    def _connect(
        self,
        *,
        configure_durability: bool = True,
    ) -> sqlite3.Connection:
        db = sqlite3.connect(
            self.path,
            timeout=20,
            isolation_level=None,
        )
        try:
            db.row_factory = sqlite3.Row
            if configure_durability:
                self._configure_durability(db)
            db.execute("PRAGMA busy_timeout=20000")
            return db
        except BaseException:
            db.close()
            raise

    @staticmethod
    def _configure_durability(db: sqlite3.Connection) -> None:
        journal_mode = str(
            db.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        ).lower()
        if journal_mode != "wal":
            raise RuntimeError(
                f"worker-receiptjournal kon WAL niet activeren: {journal_mode}"
            )
        db.execute("PRAGMA synchronous=FULL")


class WorkerService:
    """MQTT worker with one active job and a strictly bounded queue."""

    def __init__(
        self,
        *,
        host: str,
        port: int = 1883,
        username: str | None = None,
        password: str | None = None,
        tls: bool = False,
        topic_root: str = DEFAULT_TOPIC_ROOT,
        queue_size: int = 16,
        heartbeat_seconds: float = 10.0,
        provider_cycle_seconds: float = 60.0,
        compute_profile: str = "shared_ha",
        cpu_limit_percent: int = 25,
        worker_nice: int = 15,
        cpu_threads: int = 2,
        job_handlers: dict[
            str,
            Callable[[Job], dict] | _HandlerReference,
        ]
        | None = None,
        process_isolation: bool = True,
        receipt_path: str | Path | None = None,
        job_effect_contracts: Mapping[
            str,
            HandlerEffectContract,
        ]
        | None = None,
        require_effect_contracts: bool = False,
        autonomous_scheduler: AutonomousScheduler | None = None,
        scheduler_poll_seconds: float = 1.0,
        operational_snapshot_runtime_factory: (
            Callable[..., OperationalSnapshotRuntime] | None
        ) = None,
        worker_first_runtime_factory: Callable[..., Any] | None = None,
        worker_first_runtime_mode: str | None = None,
        worker_first_allowed_workload_ids: tuple[str, ...] | None = None,
    ) -> None:
        import paho.mqtt.client as mqtt

        if compute_profile not in {"shared_ha", "dedicated_nas"}:
            raise ValueError("worker-computeprofiel is ongeldig")
        for name, value, minimum, maximum in (
            ("cpu_limit_percent", cpu_limit_percent, 1, 100),
            ("worker_nice", worker_nice, 0, 19),
            ("cpu_threads", cpu_threads, 1, 64),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise ValueError(f"{name} is ongeldig")

        for factory, name in (
            (
                operational_snapshot_runtime_factory,
                "operational_snapshot_runtime_factory",
            ),
            (worker_first_runtime_factory, "worker_first_runtime_factory"),
        ):
            if factory is not None and not callable(factory):
                raise TypeError(f"{name} moet callable of None zijn")
        if (
            operational_snapshot_runtime_factory is not None
            and worker_first_runtime_factory is not None
        ):
            raise ValueError(
                "snapshot- en worker-first-runtime mogen niet parallel draaien"
            )
        if worker_first_runtime_factory is None:
            if (
                worker_first_runtime_mode is not None
                or worker_first_allowed_workload_ids is not None
            ):
                raise ValueError(
                    "worker-first-runtime metadata vereist een runtimefactory"
                )
            selected_worker_first_mode = None
        else:
            selected_worker_first_mode = (
                "worker_first_canary"
                if worker_first_runtime_mode is None
                else worker_first_runtime_mode
            )
            if selected_worker_first_mode not in {
                "worker_first_canary",
                RAW_CAPTURE_SHADOW_MODE,
                WORKER_ONLY_MODE,
            }:
                raise ValueError("worker-first-runtimemodus is ongeldig")
        raw_capture_root = selected_worker_first_mode in {
            RAW_CAPTURE_SHADOW_MODE,
            WORKER_ONLY_MODE,
        }
        if raw_capture_root:
            supplied_handlers = set(job_handlers or {})
            supplied_effects = set(job_effect_contracts or {})
            allowed_ids = worker_first_allowed_workload_ids
            if (
                supplied_handlers != RAW_CAPTURE_CONTROL_JOB_TYPES
                or supplied_effects != RAW_CAPTURE_CONTROL_JOB_TYPES
                or autonomous_scheduler is not None
                or receipt_path is not None
                or bool(process_isolation)
                or type(allowed_ids) is not tuple
                or not allowed_ids
                or tuple(sorted(set(allowed_ids))) != allowed_ids
            ):
                raise ValueError(
                    "raw-capture-runtime vereist exact zijn control-eigenaars"
                )
        elif worker_first_runtime_factory is not None and (
            bool(job_handlers)
            or bool(job_effect_contracts)
            or autonomous_scheduler is not None
            or receipt_path is not None
            or bool(process_isolation)
            or worker_first_allowed_workload_ids is not None
        ):
            raise ValueError(
                "worker-first-runtime verbiedt legacy service-eigenaars"
            )
        self.worker_id = os.getenv("STV3_WORKER_ID", str(uuid.uuid4()))
        self.build_sha256 = _runtime_build_sha256()
        self.build_id = self.build_sha256[:16]
        self.topic_root = topic_root.rstrip("/")
        self.heartbeat_seconds = max(2.0, heartbeat_seconds)
        self.provider_cycle_seconds = max(
            30.0, provider_cycle_seconds
        )
        self.compute_profile = compute_profile
        self.cpu_limit_percent = cpu_limit_percent
        self.worker_nice = worker_nice
        self.cpu_threads = cpu_threads
        self.jobs: queue.PriorityQueue[tuple[int, int, Job]] = (
            queue.PriorityQueue(maxsize=max(1, queue_size))
        )
        self._queue_seq = 0
        self.stop_event = threading.Event()
        self._mqtt_connected = threading.Event()
        self._mqtt_connection_generation = 0
        self._operational_snapshot_lock = threading.RLock()
        self._operational_snapshot_loop: asyncio.AbstractEventLoop | None = None
        self._operational_snapshot_wake: asyncio.Event | None = None
        self._operational_snapshot_thread: threading.Thread | None = None
        self._operational_snapshot_runtime: OperationalSnapshotRuntime | None = None
        self._job_thread: threading.Thread | None = None
        self._runtime_role = (
            "worker_first"
            if worker_first_runtime_factory is not None
            else "operational_snapshot"
            if operational_snapshot_runtime_factory is not None
            else "disabled"
        )
        self._worker_first_runtime_mode = selected_worker_first_mode
        self._worker_first_allowed_workload_ids = (
            tuple(worker_first_allowed_workload_ids or ())
        )
        self._operational_snapshot_health: dict | None = None
        self._operational_snapshot_last_status_wake_at: float | None = None
        self._operational_snapshot_last_worker_status: (
            tuple[str, int, int] | None
        ) = None
        self._mqtt_success_rc: int | None = None
        self._admission_lock = threading.RLock()
        self._process_lock = threading.RLock()
        self._accepting = self._runtime_role != "worker_first"
        self._admission_rejection_code = (
            "worker_first_runtime_not_ready"
            if self._runtime_role == "worker_first"
            else None
        )
        self._worker_first_runtime_fatal_code: str | None = None
        self._worker_first_runtime_fatal_event = threading.Event()
        self._worker_first_fatal_watchdog_seconds = (
            WORKER_FIRST_FATAL_WATCHDOG_SECONDS
        )
        self._worker_first_runtime_ready = threading.Event()
        self._jobs_subscription_active = False
        self.active_job: Job | None = None
        self.started_at = time.time()
        self.completed = 0
        self.failed = 0
        self.accepted = 0
        self.accepted_terminal = 0
        self.accepted_completed = 0
        self.accepted_failed = 0
        self.expired = 0
        self.cancelled = 0
        self._accepted_job_ids: set[str] = set()
        self._seen: dict[str, float] = {}
        self._job_status_cache: dict[str, dict] = {}
        if (
            autonomous_scheduler is not None
            and not isinstance(autonomous_scheduler, AutonomousScheduler)
        ):
            raise TypeError(
                "autonomous_scheduler moet AutonomousScheduler of None zijn"
            )
        try:
            scheduler_poll = float(scheduler_poll_seconds)
        except (TypeError, ValueError) as err:
            raise TypeError("scheduler_poll_seconds moet numeriek zijn") from err
        if not (
            SCHEDULER_MIN_POLL_SECONDS
            <= scheduler_poll
            <= SCHEDULER_MAX_POLL_SECONDS
        ):
            raise ValueError("scheduler_poll_seconds ligt buiten de grens")
        self._autonomous_scheduler = autonomous_scheduler
        self._scheduler_poll_seconds = scheduler_poll
        self._scheduler_lock = threading.RLock()
        self._scheduler_lease: _SchedulerLease | None = None
        self._scheduler_health = {
            "state": "disabled" if autonomous_scheduler is None else "idle",
            "last_tick_at": None,
            "last_success_at": None,
            "error_code": None,
            "claimed_jobs": 0,
            "admitted_jobs": 0,
            "completed_jobs": 0,
            "retried_jobs": 0,
            "receipt_replays": 0,
            "queue_full_events": 0,
            "error_events": 0,
        }
        self._receipt_store = (
            _JobReceiptStore(
                receipt_path,
                instance_id=uuid.uuid4().hex,
                build_id=self.build_id,
            )
            if receipt_path is not None
            else None
        )
        self._recovered_terminal_statuses: list[dict] = []
        self.recovered_terminal_jobs = 0
        if self._receipt_store is not None:
            cached_statuses, recovered_statuses = (
                self._receipt_store.initialize()
            )
            for status in cached_statuses:
                self._job_status_cache[status["job_id"]] = {
                    "job_type": status["job_type"],
                    "request_digest": status["request_digest"],
                    "state": status["state"],
                    "values": dict(status["values"]),
                    "updated_at": status["updated_at"],
                }
                self._seen[status["job_id"]] = status["updated_at"]
            self._recovered_terminal_statuses = recovered_statuses
            self.recovered_terminal_jobs = len(recovered_statuses)
        self.job_handlers = {
            "ping": _ping,
            **(job_handlers or {}),
        }
        supplied_contracts = {
            "ping": _PURE_COMPUTE_EFFECT,
            **dict(job_effect_contracts or {}),
        }
        invalid_contracts = sorted(
            job_type
            for job_type, contract in supplied_contracts.items()
            if not isinstance(contract, HandlerEffectContract)
        )
        if invalid_contracts:
            raise TypeError(
                "ongeldig worker-effectcontract voor "
                f"{invalid_contracts}"
            )
        unknown_contracts = set(supplied_contracts) - set(self.job_handlers)
        if unknown_contracts:
            raise ValueError(
                "effectcontract zonder handler: "
                f"{sorted(unknown_contracts)}"
            )
        missing_contracts = set(self.job_handlers) - set(supplied_contracts)
        if require_effect_contracts and missing_contracts:
            raise ValueError(
                "workerhandler zonder expliciet effectcontract: "
                f"{sorted(missing_contracts)}"
            )
        self.job_effect_contracts = {
            job_type: supplied_contracts.get(
                job_type,
                _UNVERIFIED_COMPATIBILITY_EFFECT,
            )
            for job_type in self.job_handlers
        }
        self.require_effect_contracts = bool(require_effect_contracts)
        if require_effect_contracts:
            unverified = sorted(
                job_type
                for job_type, contract in self.job_effect_contracts.items()
                if not contract.verified
            )
            unsafe = sorted(
                job_type
                for job_type, contract in self.job_effect_contracts.items()
                if not contract.hard_kill_safe
            )
            if unverified or unsafe:
                raise ValueError(
                    "productieworker weigert onbewezen/onveilige "
                    f"effectcontracten: unverified={unverified}, "
                    f"unsafe={unsafe}"
                )
        self._process_isolation = bool(process_isolation)
        if not self._process_isolation and any(
            isinstance(handler, _HandlerReference)
            for handler in self.job_handlers.values()
        ):
            raise TypeError(
                "Importreferenties vereisen process_isolation=True"
            )
        self._executor = (
            _SpawnJobExecutor(
                self.job_handlers,
                self.job_effect_contracts,
            )
            if self._process_isolation
            else None
        )
        now = time.time()
        self.processes = {
            "mqtt": ProcessHealth("MQTT-verbinding", "starting", now),
            "dispatcher": ProcessHealth("Taakverdeler", "idle", now),
            **{
                job_type: ProcessHealth(
                    {
                        "ping": "Automatische controle",
                        "dwd_radolan_fetch": "DWD/RADOLAN",
                        "odim_hdf5_fetch": "Met Office HDF5 (VK)",
                        "opera_hdf5_fetch": "OPERA HDF5 (operationeel)",
                        "motion_preflight": "Bewegingsanalyse (operationeel)",
                        "tracking_operational": "Stormstatus (operationeel)",
                        "virtual_probe_operational": "Verkenners (operationeel)",
                        "netatmo_fetch": "Netatmo (operationeel)",
                        "synoptic_fetch": "Synoptic grondmetingen (operationeel)",
                        "enterprise_rainrate_fetch": "Wereldwijde satellietregen (NOAA ERR)",
                        "radar_frame_operational": "Regionaal radarraster (operationeel)",
                        "calibration_operational": "Radarkalibratie (operationeel)",
                        "calibration_prepare_v2": "Radarkalibratie staging (worker-only)",
                        "calibration_corpus_ingest": (
                            "Meteorologisch kalibratiecorpus (worker-only)"
                        ),
                        "calibration_corpus_snapshot": (
                            "Kalibratieprofielen lezen (worker-only)"
                        ),
                        "phase3_replay": "Fase 3 replay (geïsoleerde test)",
                        "geojson_operational": "Kaartopbouw (operationeel)",
                    }.get(job_type, job_type),
                    "idle",
                    now,
                )
                for job_type in self.job_handlers
            },
        }
        if self._autonomous_scheduler is not None:
            self.processes["autonomous_scheduler"] = ProcessHealth(
                "Autonome scheduler",
                "idle",
                now,
            )
        try:
            self.client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"storm-tracker-worker-{self.worker_id}",
                protocol=mqtt.MQTTv311,
            )
            if username:
                self.client.username_pw_set(username, password)
            if tls:
                self.client.tls_set()
        except BaseException:
            if self._runtime_role == "worker_first":
                self._abort_worker_first_bootstrap(
                    "worker_first_mqtt_setup_failed"
                )
            raise
        selected_runtime_factory = (
            worker_first_runtime_factory
            if worker_first_runtime_factory is not None
            else operational_snapshot_runtime_factory
        )
        if selected_runtime_factory is not None:
            self._mqtt_success_rc = mqtt.MQTT_ERR_SUCCESS
            try:
                runtime = selected_runtime_factory(
                    publisher=self._publish_operational_snapshot,
                    status_supplier=self._operational_snapshot_worker_status,
                )
                self._operational_snapshot_runtime = runtime
                required_methods = (
                    "async_start",
                    "async_tick",
                    "async_stop",
                    "diagnostics",
                    *(
                        ("handle_control_job",)
                        if raw_capture_root
                        else ()
                    ),
                )
                if not all(
                    callable(getattr(runtime, name, None))
                    for name in required_methods
                ):
                    raise TypeError(
                        "operational snapshot runtime mist lifecyclemethoden"
                    )
                self._cache_operational_snapshot_health(runtime.diagnostics())
            except BaseException as error:
                if self._runtime_role == "worker_first":
                    _LOGGER.error(
                        "Worker-first bootstrap fail-closed: class=%s "
                        "reason=%s",
                        type(error).__name__,
                        _worker_first_safe_error_reason(error),
                    )
                    self._abort_worker_first_bootstrap(
                        "worker_first_runtime_bootstrap_failed"
                    )
                raise
        try:
            self.client.will_set(
                topic(self.topic_root, "worker/status"),
                encode(self._status("offline")),
                qos=1,
                retain=True,
            )
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_message = self._on_message
        except BaseException:
            if self._runtime_role == "worker_first":
                self._abort_worker_first_bootstrap(
                    "worker_first_runtime_bootstrap_failed"
                )
            raise
        try:
            self.client.connect(host, port, keepalive=30)
        except BaseException:
            if self._runtime_role == "worker_first":
                self._abort_worker_first_bootstrap(
                    "worker_first_mqtt_connect_failed"
                )
            raise

    def _abort_worker_first_bootstrap(self, code: str) -> NoReturn:
        """Preserve one fixed fatal even when bounded cleanup itself fails."""

        _LOGGER.error("Worker-first bootstrap abort: code=%s", code)
        try:
            self._fail_worker_first_runtime(code)
        except BaseException:
            pass
        try:
            self._stop_worker_first_runtime_before_run()
        except BaseException:
            pass
        raise _WorkerFirstRuntimeFatalError(code) from None

    def _stop_worker_first_runtime_before_run(self) -> bool:
        """Bound one ownership-release attempt after constructor failure."""

        runtime = self._operational_snapshot_runtime
        if self._runtime_role != "worker_first" or runtime is None:
            return True
        terminal = threading.Event()
        done = threading.Event()

        async def _stop_until_terminal() -> None:
            loop = asyncio.get_running_loop()
            started = loop.time()
            for _attempt in range(WORKER_FIRST_STOP_MAX_ATTEMPTS):
                remaining = (
                    WORKER_FIRST_STOP_TOTAL_SECONDS
                    - (loop.time() - started)
                )
                if remaining <= 0:
                    return
                try:
                    diagnostics = await asyncio.wait_for(
                        runtime.async_stop(), timeout=remaining
                    )
                except BaseException:
                    return
                if self._worker_first_stop_is_terminal(diagnostics):
                    terminal.set()
                    return
                await asyncio.sleep(WORKER_FIRST_STOP_RETRY_SECONDS)

        def _runner() -> None:
            try:
                asyncio.run(_stop_until_terminal())
            except BaseException:
                pass
            finally:
                done.set()

        thread = threading.Thread(
            target=_runner,
            name="storm-tracker-worker-first-bootstrap-stop",
            daemon=True,
        )
        try:
            thread.start()
        except BaseException:
            return False
        done.wait(WORKER_FIRST_STOP_TOTAL_SECONDS + 0.25)
        return done.is_set() and terminal.is_set()

    def _cache_operational_snapshot_health(self, value: Mapping) -> None:
        """Detach one bounded runtime diagnostic for cross-thread status reads."""
        if not isinstance(value, Mapping):
            raise TypeError("operational snapshot diagnostics moeten een mapping zijn")
        detached = deepcopy(dict(value))
        with self._operational_snapshot_lock:
            self._operational_snapshot_health = detached

    def _record_operational_snapshot_error(self, code: str) -> None:
        """Record only one fixed, secret-free service-side failure code."""
        with self._operational_snapshot_lock:
            health = deepcopy(self._operational_snapshot_health or {})
            health["state"] = "degraded"
            health["last_error_code"] = code
            self._operational_snapshot_health = health

    def _open_worker_first_admission(self, mqtt_generation: int) -> None:
        """Open service admission only after worker-first recovery/start passed."""
        if self._runtime_role != "worker_first":
            return
        with self._admission_lock:
            if (
                self.stop_event.is_set()
                or not self._mqtt_connected.is_set()
                or self._mqtt_connection_generation != mqtt_generation
            ):
                return
            already_ready = self._worker_first_runtime_ready.is_set()
            if not already_ready:
                self._start_job_dispatcher_if_needed()
            self._activate_job_subscription(
                opening_worker_first=not already_ready
            )

    def _worker_first_mqtt_generation_if_connected(self) -> int | None:
        """Snapshot the transport generation that one proof may authorize."""

        with self._admission_lock:
            if not self._mqtt_connected.is_set():
                return None
            return self._mqtt_connection_generation

    def _worker_first_mqtt_proof_is_current(
        self,
        generation: int | None,
    ) -> bool:
        """Reject diagnostics that crossed a disconnect/reconnect boundary."""

        with self._admission_lock:
            return (
                self._mqtt_connected.is_set()
                and self._mqtt_connection_generation == generation
            )

    @staticmethod
    def _raw_capture_event_ingress_is_valid(value: object) -> bool:
        """Validate bounded RAW-shadow truth without requiring connectivity."""

        if not isinstance(value, Mapping) or set(value) != {"blitzortung"}:
            return False
        event = value.get("blitzortung")
        if not isinstance(event, Mapping) or set(event) != {
            "capacity_claimed",
            "cadence_activation_route",
            "contract",
            "desired_state",
            "enabled",
            "operational_admission",
            "raw_store_commit_is_terminal",
            "readiness",
            "sidecar",
        }:
            return False
        desired = event.get("desired_state")
        readiness = event.get("readiness")
        sidecar = event.get("sidecar")
        enabled = event.get("enabled")
        capacity_claimed = event.get("capacity_claimed")
        if (
            type(enabled) is not bool
            or type(capacity_claimed) is not bool
            or capacity_claimed is not enabled
        ):
            return False
        if not enabled:
            return event == {
                "capacity_claimed": False,
                "cadence_activation_route": False,
                "contract": RAW_CAPTURE_BLITZORTUNG_EVENT_STATUS_CONTRACT,
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
        if (
            event.get("cadence_activation_route") is not False
            or event.get("contract")
            != RAW_CAPTURE_BLITZORTUNG_EVENT_STATUS_CONTRACT
            or event.get("operational_admission") is not False
            or event.get("raw_store_commit_is_terminal") is not True
            or not isinstance(desired, Mapping)
            or set(desired)
            != {
                "lightning_source_mode",
                "region_count",
                "state_sha256",
                "status",
            }
            or not isinstance(readiness, Mapping)
            or set(readiness)
            != {
                "alarm_codes",
                "ready",
                "reason_code",
                "required_for_service_admission",
                "status",
            }
            or not isinstance(sidecar, Mapping)
            or set(sidecar)
            != {
                "absence_semantics",
                "access_retention_authority",
                "alarm_codes",
                "broker_profile",
                "callback_counts",
                "capacity",
                "connected",
                "contract",
                "diagnostics_schema",
                "fault_latched",
                "ingress_counters",
                "last_error_code",
                "meteorological_admission",
                "network_started",
                "operational_admission",
                "participant_or_explicit_permission_required",
                "payload_parsed",
                "queue_depth",
                "raw_payload_transport",
                "raw_payloads_over_ha_mqtt",
                "raw_shadow_limitations",
                "regional_subscription_count",
                "retained_payload_bytes",
                "retained_payload_entries",
                "silence_interpretation",
                "state",
                "transport_completeness",
            }
        ):
            return False
        state_sha256 = desired.get("state_sha256")
        region_count = desired.get("region_count")
        desired_status = desired.get("status")
        source_mode = desired.get("lightning_source_mode")
        if (
            source_mode not in {None, "auto", "satellite_test"}
            or type(region_count) is not int
            or not 0 <= region_count <= 64
            or (
                state_sha256 is not None
                and (
                    not isinstance(state_sha256, str)
                    or re.fullmatch(r"[0-9a-f]{64}", state_sha256) is None
                )
            )
            or desired_status
            not in {
                "active_regions",
                "no_available_targets",
                "satellite_test_suppressed",
                "unconfigured",
            }
            or (
                desired_status == "unconfigured"
                and (state_sha256 is not None or source_mode is not None or region_count)
            )
            or (
                desired_status == "active_regions"
                and (source_mode != "auto" or state_sha256 is None or region_count < 1)
            )
            or (
                desired_status == "no_available_targets"
                and (source_mode != "auto" or state_sha256 is None or region_count)
            )
            or (
                desired_status == "satellite_test_suppressed"
                and (
                    source_mode != "satellite_test"
                    or state_sha256 is None
                    or region_count
                )
            )
        ):
            return False
        alarm_codes = readiness.get("alarm_codes")
        readiness_status = readiness.get("status")
        reason_code = readiness.get("reason_code")
        if (
            type(alarm_codes) is not list
            or any(
                not isinstance(code, str)
                or re.fullmatch(r"[a-z][a-z0-9_]{0,95}", code) is None
                for code in alarm_codes
            )
            or alarm_codes != sorted(set(alarm_codes))
            or type(readiness.get("ready")) is not bool
            or readiness.get("required_for_service_admission") is not False
            or readiness_status
            not in {
                "degraded",
                "disabled",
                "intentionally_inert",
                "running_exact_raw",
                "starting",
                "stopped",
                "waiting",
            }
            or not isinstance(reason_code, str)
            or re.fullmatch(r"[a-z][a-z0-9_]{0,95}", reason_code) is None
            or (
                readiness.get("ready") is True
                and readiness_status
                not in {"intentionally_inert", "running_exact_raw"}
            )
        ):
            return False
        callback_counts = sidecar.get("callback_counts")
        capacity = sidecar.get("capacity")
        ingress_counters = sidecar.get("ingress_counters")
        sidecar_alarms = sidecar.get("alarm_codes")
        if (
            sidecar.get("absence_semantics") != "unknown_transport_coverage"
            or sidecar.get("access_retention_authority")
            not in {"available", "unavailable"}
            or sidecar.get("broker_profile")
            != "blitzortung_public_mqtt_v311_regional_qos0_v1"
            or sidecar.get("contract")
            != "stv3-worker-blitzortung-mqtt-raw-sidecar-v1"
            or sidecar.get("diagnostics_schema")
            != "stv3-worker-blitzortung-mqtt-raw-sidecar-diagnostics-v2"
            or type(sidecar.get("connected")) is not bool
            or type(sidecar.get("fault_latched")) is not bool
            or type(sidecar.get("network_started")) is not bool
            or sidecar.get("meteorological_admission") is not False
            or sidecar.get("operational_admission") is not False
            or sidecar.get("participant_or_explicit_permission_required")
            is not True
            or sidecar.get("payload_parsed") is not False
            or sidecar.get("raw_payload_transport")
            != "worker_direct_not_ha_mqtt"
            or sidecar.get("raw_payloads_over_ha_mqtt") is not False
            or sidecar.get("raw_shadow_limitations")
            != [
                "append_only_capacity_exhaustion_fails_closed",
                "qos0_upstream_loss_is_unobservable",
                "linux_disconnect_thermal_ha_z2m_soak_evidence_pending",
                "legacy_ha_cutover_exclusivity_not_proven",
            ]
            or sidecar.get("silence_interpretation")
            != "unknown_not_an_absence_observation"
            or sidecar.get("transport_completeness")
            != "best_effort_qos0_unacknowledged"
            or not isinstance(sidecar.get("state"), str)
            or re.fullmatch(r"[a-z][a-z0-9_]{0,63}", sidecar["state"])
            is None
            or not isinstance(callback_counts, Mapping)
            or set(callback_counts)
            != {
                "callbacks",
                "callback_failures",
                "connects",
                "disconnects",
                "inert_completions",
                "network_starts",
                "starts",
                "stops",
            }
            or not isinstance(capacity, Mapping)
            or set(capacity)
            != {
                "control_intake_headroom",
                "control_receipt_capacity",
                "control_receipts",
                "queue_capacity",
                "queue_headroom",
                "retained_payload_byte_capacity",
                "retained_payload_byte_headroom",
                "retained_payload_entry_capacity",
                "retained_payload_entry_headroom",
            }
            or not isinstance(ingress_counters, Mapping)
            or not isinstance(sidecar_alarms, list)
            or any(
                not isinstance(code, str)
                or re.fullmatch(r"[a-z][a-z0-9_]{0,95}", code) is None
                for code in sidecar_alarms
            )
            or len(sidecar_alarms) != len(set(sidecar_alarms))
            or (
                sidecar.get("last_error_code") is not None
                and (
                    not isinstance(sidecar.get("last_error_code"), str)
                    or re.fullmatch(
                        r"[a-z][a-z0-9_]{0,95}",
                        sidecar["last_error_code"],
                    )
                    is None
                )
            )
            or any(
                type(item) is not int
                or not 0 <= item <= OPERATIONAL_SNAPSHOT_MAX_COUNTER
                for item in callback_counts.values()
            )
            or any(
                not isinstance(key, str)
                or re.fullmatch(r"[a-z][a-z0-9_]{0,63}", key) is None
                or type(item) is not int
                or not 0 <= item <= OPERATIONAL_SNAPSHOT_MAX_COUNTER
                for key, item in ingress_counters.items()
            )
            or any(
                type(item) is not int or not 0 <= item <= 2**63 - 1
                for item in capacity.values()
            )
            or any(
                type(sidecar.get(field)) is not int
                or not 0 <= sidecar[field] <= 2**63 - 1
                for field in (
                    "queue_depth",
                    "regional_subscription_count",
                    "retained_payload_bytes",
                    "retained_payload_entries",
                )
            )
            or not 0 <= sidecar["regional_subscription_count"] <= 512
            or sidecar["queue_depth"] > capacity["queue_capacity"]
            or sidecar["queue_depth"] + capacity["queue_headroom"]
            != capacity["queue_capacity"]
            or sidecar["retained_payload_bytes"]
            + capacity["retained_payload_byte_headroom"]
            != capacity["retained_payload_byte_capacity"]
            or sidecar["retained_payload_entries"]
            + capacity["retained_payload_entry_headroom"]
            != capacity["retained_payload_entry_capacity"]
            or capacity["control_receipts"]
            + capacity["control_intake_headroom"]
            > capacity["control_receipt_capacity"]
            or (
                sidecar.get("access_retention_authority") == "unavailable"
                and (
                    sidecar.get("connected") is True
                    or sidecar.get("network_started") is True
                )
            )
            or (sidecar.get("fault_latched") is True and not sidecar_alarms)
            or (
                sidecar.get("fault_latched") is True
                and readiness.get("ready") is not False
            )
        ):
            return False
        return all(code in alarm_codes for code in sidecar_alarms)

    @staticmethod
    def _raw_capture_storage_capacity_is_valid(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {
            "accounting_contract",
            "cadence",
            "combined_persistent_volume",
            "event",
            "host_operational_free_reserve_bytes",
        }:
            return False
        cadence = value.get("cadence")
        combined = value.get("combined_persistent_volume")
        event = value.get("event")
        if (
            value.get("accounting_contract")
            != "config_bound_optional_event_capacity_v2"
            or not isinstance(cadence, Mapping)
            or set(cadence)
            != {
                "catalog_max_bytes",
                "ledger_accounted",
                "object_max_bytes",
                "object_max_entries",
            }
            or not isinstance(combined, Mapping)
            or set(combined)
            != {
                "configured_max_bytes",
                "raw_object_max_entries",
                "same_filesystem_capacity_claim",
            }
            or not isinstance(event, Mapping)
            or set(event)
            != {
                "capacity_claimed",
                "catalog_max_bytes",
                "control_receipt_max_bytes",
                "control_receipt_storage_max_bytes",
                "control_staging_residue_entry_capacity",
                "control_staging_residue_max_bytes",
                "enabled",
                "ledger_accounted",
                "object_max_bytes",
                "object_max_entries",
            }
            or cadence.get("ledger_accounted") is not True
            or event.get("ledger_accounted") is not False
            or combined.get("same_filesystem_capacity_claim") is not True
            or type(event.get("enabled")) is not bool
            or type(event.get("capacity_claimed")) is not bool
            or event.get("capacity_claimed") is not event.get("enabled")
            or type(value.get("host_operational_free_reserve_bytes")) is not int
            or not RAW_CAPTURE_HOST_OPERATIONAL_FREE_RESERVE_BYTES
            <= value["host_operational_free_reserve_bytes"]
            <= 2**63 - 1
        ):
            return False
        core_integer_values = (
            cadence.get("catalog_max_bytes"),
            cadence.get("object_max_bytes"),
            cadence.get("object_max_entries"),
            combined.get("configured_max_bytes"),
            combined.get("raw_object_max_entries"),
        )
        event_integer_values = (
            event.get("catalog_max_bytes"),
            event.get("control_receipt_max_bytes"),
            event.get("control_receipt_storage_max_bytes"),
            event.get("control_staging_residue_entry_capacity"),
            event.get("control_staging_residue_max_bytes"),
            event.get("object_max_bytes"),
            event.get("object_max_entries"),
        )
        if not all(
            type(item) is int and 0 < item <= 2**63 - 1
            for item in core_integer_values
        ) or not all(
            type(item) is int and 0 <= item <= 2**63 - 1
            for item in event_integer_values
        ):
            return False
        if event["enabled"] is False:
            return (
                all(item == 0 for item in event_integer_values)
                and combined["raw_object_max_entries"]
                == cadence["object_max_entries"]
                and combined["configured_max_bytes"]
                == cadence["object_max_bytes"] + cadence["catalog_max_bytes"]
            )
        return (
            all(item > 0 for item in event_integer_values)
            and combined["raw_object_max_entries"]
            == cadence["object_max_entries"] + event["object_max_entries"]
            and combined["configured_max_bytes"]
            == (
                cadence["object_max_bytes"]
                + cadence["catalog_max_bytes"]
                + event["object_max_bytes"]
                + event["catalog_max_bytes"]
                + event["control_receipt_storage_max_bytes"]
                + event["control_staging_residue_max_bytes"]
            )
            and event["control_staging_residue_max_bytes"]
            == (
                event["control_staging_residue_entry_capacity"]
                * event["control_receipt_max_bytes"]
            )
            and event["control_receipt_storage_max_bytes"]
            >= event["control_receipt_max_bytes"]
            and event["control_receipt_storage_max_bytes"]
            % event["control_receipt_max_bytes"]
            == 0
        )

    @staticmethod
    def _raw_capture_retention_guard_is_valid(
        value: object,
        *,
        capture_storage: object,
        event_ingress: object,
        storage_capacity: object,
    ) -> bool:
        """Validate the exact read-only capacity/reclamation guard proof."""

        if not isinstance(value, Mapping) or set(value) != {
            "admission_ready",
            "alarm_codes",
            "capacity",
            "capacity_policy",
            "contract",
            "event_capacity_claimed",
            "full_audit_completed",
            "inspection_count",
            "live_delete_performed",
            "ownership",
            "reason_code",
            "reclamation",
            "unmanaged_data_action",
        }:
            return False
        capacity = value.get("capacity")
        ownership = value.get("ownership")
        reclamation = value.get("reclamation")
        alarm_codes = value.get("alarm_codes")
        capacity_fields = {
            "cadence_catalog_headroom_bytes",
            "cadence_capture_headroom",
            "cadence_capture_max_entries",
            "cadence_cycle_reserve_bytes",
            "cadence_cycle_reserve_captures",
            "cadence_cycle_reserve_entries",
            "cadence_ledger_byte_headroom",
            "cadence_ledger_catalog_headroom_bytes",
            "cadence_ledger_catalog_max_bytes",
            "cadence_ledger_entry_headroom",
            "cadence_ledger_max_bytes",
            "cadence_ledger_max_entries",
            "cadence_ledger_max_reservations",
            "cadence_raw_byte_headroom",
            "cadence_raw_catalog_max_bytes",
            "cadence_raw_entry_headroom",
            "cadence_raw_max_bytes",
            "cadence_raw_max_entries",
            "cadence_receipt_headroom",
            "cadence_receipt_max_entries",
            "cadence_reservation_headroom",
            "configured_persistent_max_bytes",
            "configured_raw_object_max_entries",
            "event_control_headroom",
            "event_payload_byte_headroom",
            "event_payload_entry_headroom",
            "filesystem_free_bytes",
            "filesystem_total_bytes",
            "host_operational_free_headroom_bytes",
            "host_operational_free_reserve_bytes",
            "persistent_envelope_free_required_bytes",
            "physical_reserve_headroom_bytes",
        }
        if (
            not isinstance(capacity, Mapping)
            or set(capacity) != capacity_fields
            or any(
                type(item) is not int or not 0 <= item <= 2**63 - 1
                for item in capacity.values()
            )
            or not isinstance(ownership, Mapping)
            or ownership
            != {
                "exclusive_filesystem_owner_proven": False,
                "exclusive_process_owner_held": True,
                "external_writer_exclusion_proven": False,
                "path_identity_reproved": True,
                "same_filesystem_reproved": True,
            }
            or not isinstance(reclamation, Mapping)
            or reclamation
            != {
                "automatic_reclaim": False,
                "live_apply_authorized": False,
                "live_apply_wired": False,
                "policy": "offline_five_role_only_never_live_apply_v1",
                "source_reclaim_authorized": False,
            }
            or type(alarm_codes) is not list
            or len(alarm_codes) != len(set(alarm_codes))
            or any(
                code not in RAW_CAPTURE_RETENTION_GUARD_ALARM_PRIORITY
                for code in alarm_codes
            )
            or alarm_codes
            != [
                code
                for code in RAW_CAPTURE_RETENTION_GUARD_ALARM_PRIORITY
                if code in alarm_codes
            ]
            or value.get("admission_ready") is not True
            or set(alarm_codes) & RAW_CAPTURE_RETENTION_GUARD_BLOCKING_ALARMS
            or value.get("capacity_policy")
            != "config_bound_optional_event_envelope_reserved_fail_closed_v2"
            or value.get("contract")
            != RAW_CAPTURE_RETENTION_GUARD_CONTRACT
            or type(value.get("event_capacity_claimed")) is not bool
            or value.get("full_audit_completed") is not True
            or type(value.get("inspection_count")) is not int
            or not 2 <= value["inspection_count"] <= 2**63 - 1
            or value.get("live_delete_performed") is not False
            or value.get("reason_code")
            != (alarm_codes[0] if alarm_codes else "ready")
            or value.get("unmanaged_data_action")
            != "alarm_and_block_never_delete"
        ):
            return False
        positive_capacity_fields = (
            "cadence_capture_max_entries",
            "cadence_cycle_reserve_bytes",
            "cadence_cycle_reserve_captures",
            "cadence_cycle_reserve_entries",
            "cadence_ledger_catalog_max_bytes",
            "cadence_ledger_max_bytes",
            "cadence_ledger_max_entries",
            "cadence_ledger_max_reservations",
            "cadence_raw_catalog_max_bytes",
            "cadence_raw_max_bytes",
            "cadence_raw_max_entries",
            "cadence_receipt_max_entries",
            "configured_persistent_max_bytes",
            "configured_raw_object_max_entries",
            "filesystem_total_bytes",
            "host_operational_free_reserve_bytes",
        )
        if any(capacity[field] <= 0 for field in positive_capacity_fields):
            return False
        if (
            not isinstance(capture_storage, Mapping)
            or not isinstance(storage_capacity, Mapping)
            or not isinstance(event_ingress, Mapping)
        ):
            return False
        cadence = storage_capacity.get("cadence")
        combined = storage_capacity.get("combined_persistent_volume")
        storage_event = storage_capacity.get("event")
        blitzortung = event_ingress.get("blitzortung")
        sidecar = (
            blitzortung.get("sidecar")
            if isinstance(blitzortung, Mapping)
            else None
        )
        if (
            not isinstance(cadence, Mapping)
            or not isinstance(combined, Mapping)
            or not isinstance(storage_event, Mapping)
            or not isinstance(blitzortung, Mapping)
            or type(blitzortung.get("enabled")) is not bool
            or type(blitzortung.get("capacity_claimed")) is not bool
            or blitzortung.get("capacity_claimed")
            is not blitzortung.get("enabled")
            or type(storage_event.get("enabled")) is not bool
            or type(storage_event.get("capacity_claimed")) is not bool
            or storage_event.get("capacity_claimed")
            is not storage_event.get("enabled")
            or value.get("event_capacity_claimed")
            is not blitzortung.get("enabled")
            or value.get("event_capacity_claimed")
            is not storage_event.get("enabled")
            or type(
                storage_capacity.get(
                    "host_operational_free_reserve_bytes"
                )
            ) is not int
        ):
            return False
        if value["event_capacity_claimed"]:
            event_capacity = (
                sidecar.get("capacity")
                if isinstance(sidecar, Mapping)
                else None
            )
            if (
                not isinstance(event_capacity, Mapping)
                or sidecar.get("access_retention_authority") != "available"
            ):
                return False
        else:
            if sidecar is not None:
                return False
            event_capacity = {
                "control_intake_headroom": 0,
                "retained_payload_byte_headroom": 0,
                "retained_payload_entry_headroom": 0,
            }
        linked_values = (
            capture_storage.get("reserved_bytes"),
            capture_storage.get("committed_bytes"),
            capture_storage.get("reserved_entries"),
            capture_storage.get("committed_entries"),
            capture_storage.get("reserved_count"),
            capture_storage.get("committed_count"),
            cadence.get("object_max_bytes"),
            cadence.get("object_max_entries"),
            cadence.get("catalog_max_bytes"),
            combined.get("raw_object_max_entries"),
            combined.get("configured_max_bytes"),
            event_capacity.get("control_intake_headroom"),
            event_capacity.get("retained_payload_byte_headroom"),
            event_capacity.get("retained_payload_entry_headroom"),
        )
        if any(
            type(item) is not int or not 0 <= item <= 2**63 - 1
            for item in linked_values
        ):
            return False
        ledger_used_bytes = (
            capture_storage["reserved_bytes"]
            + capture_storage["committed_bytes"]
        )
        ledger_used_entries = (
            capture_storage["reserved_entries"]
            + capture_storage["committed_entries"]
        )
        ledger_used_reservations = (
            capture_storage["reserved_count"]
            + capture_storage["committed_count"]
        )
        return (
            capacity["cadence_raw_max_bytes"]
            == cadence.get("object_max_bytes")
            and capacity["cadence_raw_max_entries"]
            == cadence.get("object_max_entries")
            and capacity["cadence_raw_catalog_max_bytes"]
            == cadence.get("catalog_max_bytes")
            and capacity["configured_raw_object_max_entries"]
            == combined.get("raw_object_max_entries")
            and capacity["configured_persistent_max_bytes"]
            == combined.get("configured_max_bytes")
            + capacity["cadence_ledger_catalog_max_bytes"]
            and capacity["cadence_ledger_byte_headroom"]
            + ledger_used_bytes
            == capacity["cadence_ledger_max_bytes"]
            and capacity["cadence_ledger_entry_headroom"]
            + ledger_used_entries
            == capacity["cadence_ledger_max_entries"]
            and capacity["cadence_reservation_headroom"]
            + ledger_used_reservations
            == capacity["cadence_ledger_max_reservations"]
            and capacity["cadence_raw_byte_headroom"]
            <= capacity["cadence_raw_max_bytes"]
            and capacity["cadence_raw_entry_headroom"]
            <= capacity["cadence_raw_max_entries"]
            and capacity["cadence_receipt_headroom"]
            <= capacity["cadence_receipt_max_entries"]
            and capacity["cadence_capture_headroom"]
            <= capacity["cadence_capture_max_entries"]
            and capacity["cadence_catalog_headroom_bytes"]
            <= capacity["cadence_raw_catalog_max_bytes"]
            and capacity["cadence_ledger_catalog_headroom_bytes"]
            <= capacity["cadence_ledger_catalog_max_bytes"]
            and capacity["cadence_cycle_reserve_bytes"]
            <= min(
                capacity["cadence_ledger_max_bytes"],
                capacity["cadence_raw_max_bytes"],
            )
            and capacity["cadence_cycle_reserve_entries"]
            <= min(
                capacity["cadence_ledger_max_entries"],
                capacity["cadence_raw_max_entries"],
                capacity["cadence_receipt_max_entries"],
            )
            and capacity["cadence_cycle_reserve_captures"]
            <= min(
                capacity["cadence_capture_max_entries"],
                capacity["cadence_ledger_max_reservations"],
            )
            and capacity["event_control_headroom"]
            == event_capacity.get("control_intake_headroom")
            and capacity["event_payload_byte_headroom"]
            == event_capacity.get("retained_payload_byte_headroom")
            and capacity["event_payload_entry_headroom"]
            == event_capacity.get("retained_payload_entry_headroom")
            and capacity["filesystem_free_bytes"]
            <= capacity["filesystem_total_bytes"]
            and capacity["persistent_envelope_free_required_bytes"]
            <= capacity["configured_persistent_max_bytes"]
            and capacity["physical_reserve_headroom_bytes"]
            == max(
                0,
                capacity["filesystem_free_bytes"]
                - capacity["persistent_envelope_free_required_bytes"]
                - capacity["host_operational_free_reserve_bytes"],
            )
            and capacity["host_operational_free_headroom_bytes"]
            == capacity["physical_reserve_headroom_bytes"]
            and capacity["host_operational_free_reserve_bytes"]
            == storage_capacity.get(
                "host_operational_free_reserve_bytes"
            )
            and capacity["host_operational_free_reserve_bytes"]
            >= RAW_CAPTURE_HOST_OPERATIONAL_FREE_RESERVE_BYTES
            and capacity["filesystem_free_bytes"]
            >= (
                capacity["persistent_envelope_free_required_bytes"]
                + capacity["host_operational_free_reserve_bytes"]
            )
            and capacity["cadence_ledger_byte_headroom"]
            >= capacity["cadence_cycle_reserve_bytes"]
            and capacity["cadence_raw_byte_headroom"]
            >= capacity["cadence_cycle_reserve_bytes"]
            and capacity["cadence_ledger_entry_headroom"]
            >= capacity["cadence_cycle_reserve_entries"]
            and capacity["cadence_raw_entry_headroom"]
            >= capacity["cadence_cycle_reserve_entries"]
            and capacity["cadence_receipt_headroom"]
            >= capacity["cadence_cycle_reserve_entries"]
            and capacity["cadence_capture_headroom"]
            >= capacity["cadence_cycle_reserve_captures"]
            and capacity["cadence_reservation_headroom"]
            >= capacity["cadence_cycle_reserve_captures"]
        )

    def _worker_first_start_is_ready(
        self,
        diagnostics: Mapping,
        *,
        operation: str,
    ) -> bool:
        """Require the exact readiness schema for the selected operation."""

        if operation not in {"start", "tick"}:
            return False
        if self._worker_first_runtime_mode == WORKER_ONLY_MODE:
            expected_fields = {
                "captured_provider_count",
                "contract",
                "counts",
                "cycle_status_counts",
                "dashboard_output",
                "ground_errors",
                "ground_fact_count",
                "ground_source_count",
                "ground_track_match_count",
                "ground_track_match_errors",
                "ground_track_match_status",
                "heavy_lane_active",
                "last_error_code",
                "last_tick_reason_code",
                "last_tick_status",
                "last_tick_workload_id",
                "live_wiring",
                "meteorological_output",
                "mode",
                "ownership_fence_held",
                "provider_count",
                "quantitative_capture_count",
                "quantitative_errors",
                "raw_calibration",
                "shutdown_complete",
                "snapshot_rich_builder_failures",
                "snapshot_rich_built",
                "snapshot_rich_withheld",
                "snapshot_revision",
                "snapshot_transport_state",
                "snapshot_unknown_built",
                "state",
                "thin_ha_output",
                "tracking_active_tracks",
                "tracking_errors",
                "tracking_motion_ready_tracks",
                "tracking_region_count",
                "tracking_restart_persistent",
                "tracking_shadow_arrival_candidates",
                "tracking_shadow_arrival_status",
                "tracking_shadow_future_arrivals",
                "tracking_store_error",
                "tracking_tracked_tracks",
            }
            if set(diagnostics) != expected_fields:
                return False
            counts = diagnostics.get("counts")
            cycle_counts = diagnostics.get("cycle_status_counts")
            if (
                not isinstance(counts, Mapping)
                or set(counts)
                != {
                    "cycles_staged",
                    "failures",
                    "ground_failures",
                    "ground_successes",
                    "ground_track_match_failures",
                    "ground_track_match_successes",
                    "quantitative_failures",
                    "quantitative_skipped_stale",
                    "quantitative_successes",
                    "raw_calibration_index_failures",
                    "raw_calibration_indexed",
                    "restored_captures",
                    "snapshots_published",
                    "start_calls",
                    "stop_calls",
                    "terminal_updates",
                    "tracking_duplicates",
                    "tracking_failures",
                    "tracking_resets",
                    "tracking_successes",
                    "ticks",
                }
                or any(
                    type(value) is not int
                    or not 0 <= value <= OPERATIONAL_SNAPSHOT_MAX_COUNTER
                    for value in counts.values()
                )
                or not isinstance(cycle_counts, Mapping)
                or set(cycle_counts)
                != {"blocked", "current", "error", "not_applicable", "stale"}
                or any(
                    type(value) is not int or not 0 <= value <= 29
                    for value in cycle_counts.values()
                )
            ):
                return False
            cycle_total = sum(cycle_counts.values())
            revision = diagnostics.get("snapshot_revision")
            captured = diagnostics.get("captured_provider_count")
            quantitative_captured = diagnostics.get(
                "quantitative_capture_count"
            )
            quantitative_errors = diagnostics.get("quantitative_errors")
            ground_source_count = diagnostics.get("ground_source_count")
            ground_fact_count = diagnostics.get("ground_fact_count")
            ground_errors = diagnostics.get("ground_errors")
            ground_track_match_count = diagnostics.get("ground_track_match_count")
            ground_track_match_errors = diagnostics.get(
                "ground_track_match_errors"
            )
            tracking_active_tracks = diagnostics.get("tracking_active_tracks")
            tracking_tracked_tracks = diagnostics.get("tracking_tracked_tracks")
            tracking_motion_ready_tracks = diagnostics.get(
                "tracking_motion_ready_tracks"
            )
            tracking_region_count = diagnostics.get("tracking_region_count")
            tracking_shadow_arrival_candidates = diagnostics.get(
                "tracking_shadow_arrival_candidates"
            )
            tracking_shadow_future_arrivals = diagnostics.get(
                "tracking_shadow_future_arrivals"
            )
            tracking_errors = diagnostics.get("tracking_errors")
            raw_calibration = diagnostics.get("raw_calibration")
            snapshot_diagnostic_counts = tuple(
                diagnostics.get(key)
                for key in (
                    "snapshot_rich_built",
                    "snapshot_rich_withheld",
                    "snapshot_rich_builder_failures",
                    "snapshot_unknown_built",
                )
            )
            operation_ready = (
                counts["start_calls"] >= 1
                and counts["ticks"] == 0
                if operation == "start"
                else counts["start_calls"] >= 1 and counts["ticks"] >= 1
            )
            return (
                operation_ready
                and diagnostics.get("contract") == WORKER_ONLY_RUNTIME_CONTRACT
                and diagnostics.get("mode") == WORKER_ONLY_SERVICE_MODE
                and diagnostics.get("state") == "running"
                and diagnostics.get("live_wiring") is True
                and diagnostics.get("meteorological_output") is True
                and diagnostics.get("dashboard_output") is True
                and diagnostics.get("thin_ha_output") is True
                and diagnostics.get("heavy_lane_active") is False
                and diagnostics.get("ownership_fence_held") is True
                and diagnostics.get("shutdown_complete") is False
                and diagnostics.get("last_error_code") is None
                and diagnostics.get("provider_count") == 29
                and isinstance(raw_calibration, Mapping)
                and raw_calibration.get("status") in {"ready", "not_configured"}
                and raw_calibration.get("exact_raw") is True
                and all(
                    type(raw_calibration.get(key)) is int
                    and 0 <= raw_calibration[key] <= OPERATIONAL_SNAPSHOT_MAX_COUNTER
                    for key in ("captures", "sources", "providers", "database_bytes")
                )
                and 0 <= raw_calibration["sources"] <= 29
                and 0 <= raw_calibration["providers"] <= 29
                and all(
                    type(value) is int
                    and 0 <= value <= OPERATIONAL_SNAPSHOT_MAX_COUNTER
                    for value in snapshot_diagnostic_counts
                )
                and type(captured) is int
                and 0 <= captured <= 29
                and type(quantitative_captured) is int
                and 0 <= quantitative_captured <= 4
                and isinstance(quantitative_errors, Mapping)
                and len(quantitative_errors) <= 4
                and all(
                    isinstance(key, str)
                    and key in {
                        "radar.dwd.radolan_rv",
                        "radar.knmi.rtcor",
                        "radar.metoffice.uk_rate",
                        "satellite.noaa.err",
                    }
                    and isinstance(value, str)
                    and 1 <= len(value) <= 64
                    for key, value in quantitative_errors.items()
                )
                and type(ground_source_count) is int
                and 0 <= ground_source_count <= 3
                and type(ground_fact_count) is int
                and 0 <= ground_fact_count <= 4_096
                and isinstance(ground_errors, Mapping)
                and len(ground_errors) <= 3
                and all(
                    isinstance(key, str)
                    and key in {
                        "ground.metar.global",
                        "ground.netatmo.region",
                        "ground.synoptic.region",
                    }
                    and value == "ground_processing_failed"
                    for key, value in ground_errors.items()
                )
                and type(ground_track_match_count) is int
                and 0 <= ground_track_match_count <= 1_000_000
                and isinstance(ground_track_match_errors, Mapping)
                and len(ground_track_match_errors) <= 3
                and all(
                    isinstance(key, str)
                    and key in {
                        "ground.metar.global",
                        "ground.netatmo.region",
                        "ground.synoptic.region",
                    }
                    and value == "ground_track_matching_failed"
                    for key, value in ground_track_match_errors.items()
                )
                and diagnostics.get("ground_track_match_status")
                == "shadow_candidate_unadmitted"
                and type(tracking_active_tracks) is int
                and 0 <= tracking_active_tracks <= 1_000_000
                and type(tracking_tracked_tracks) is int
                and 0 <= tracking_tracked_tracks <= tracking_active_tracks
                and type(tracking_motion_ready_tracks) is int
                and 0 <= tracking_motion_ready_tracks <= tracking_tracked_tracks
                and type(tracking_region_count) is int
                and 0 <= tracking_region_count <= 128
                and type(diagnostics.get("tracking_restart_persistent")) is bool
                and type(tracking_shadow_arrival_candidates) is int
                and 0 <= tracking_shadow_arrival_candidates <= 1_000_000
                and type(tracking_shadow_future_arrivals) is int
                and 0 <= tracking_shadow_future_arrivals
                <= tracking_shadow_arrival_candidates
                and diagnostics.get("tracking_shadow_arrival_status")
                == "shadow_candidate_unadmitted"
                and diagnostics.get("tracking_store_error") in {
                    None,
                    "tracking_store_restore_failed",
                    "tracking_store_write_failed",
                }
                and isinstance(tracking_errors, Mapping)
                and len(tracking_errors) <= 4
                and all(
                    isinstance(key, str)
                    and key in {
                        "radar.dwd.radolan_rv",
                        "radar.knmi.rtcor",
                        "radar.metoffice.uk_rate",
                        "satellite.noaa.err",
                    }
                    and value == "tracking_processing_failed"
                    for key, value in tracking_errors.items()
                )
                and cycle_total in {0, 29}
                and type(revision) is int
                and 0 <= revision <= OPERATIONAL_SNAPSHOT_MAX_COUNTER
                and (
                    diagnostics.get("snapshot_transport_state") == "published"
                    if revision > 0
                    else diagnostics.get("snapshot_transport_state") is None
                )
                and isinstance(diagnostics.get("last_tick_status"), str)
                and isinstance(diagnostics.get("last_tick_reason_code"), str)
                and (
                    diagnostics.get("last_tick_workload_id") is None
                    or not self._worker_first_allowed_workload_ids
                    or diagnostics.get("last_tick_workload_id")
                    in self._worker_first_allowed_workload_ids
                )
            )
        if self._worker_first_runtime_mode == RAW_CAPTURE_SHADOW_MODE:
            expected_fields = {
                "acquisition_configuration",
                "authority_sha256",
                "blocked_catalog",
                "capture_storage",
                "config_sha256",
                "contract",
                "control_job_types",
                "counts",
                "dashboard_output",
                "downstream_consumption_required",
                "event_ingress",
                "heavy_lane_active",
                "last_error_code",
                "last_tick",
                "legacy_heavy_admission",
                "live_wiring",
                "meteorological_admission",
                "meteorological_output",
                "mode",
                "ownership_fence_held",
                "production_enable_ready",
                "provider_configuration",
                "raw_payload_transport",
                "raw_store_commit_is_terminal",
                "retention",
                "retention_guard",
                "science_mode",
                "shutdown_complete",
                "state",
                "storage_capacity",
                "terminal_router_wired",
            }
            if set(diagnostics) != expected_fields:
                return False
            configuration = diagnostics.get("acquisition_configuration")
            blocked_catalog = diagnostics.get("blocked_catalog")
            capture_storage = diagnostics.get("capture_storage")
            counts = diagnostics.get("counts")
            last_tick = diagnostics.get("last_tick")
            provider_configuration = diagnostics.get(
                "provider_configuration"
            )
            retention = diagnostics.get("retention")
            if (
                not isinstance(configuration, Mapping)
                or set(configuration)
                != {
                    "collector_scope",
                    "configured_workload_ids",
                    "policy",
                }
                or not isinstance(blocked_catalog, Mapping)
                or set(blocked_catalog)
                != {"count", "reason_counts", "workload_ids"}
                or not isinstance(capture_storage, Mapping)
                or set(capture_storage)
                != {
                    "committed_bytes",
                    "committed_count",
                    "committed_entries",
                    "ledger_scope",
                    "reserved_bytes",
                    "reserved_count",
                    "reserved_entries",
                }
                or not isinstance(counts, Mapping)
                or set(counts)
                != {
                    "control_calls",
                    "event_failures",
                    "event_stops",
                    "event_syncs",
                    "failures",
                    "start_calls",
                    "stop_calls",
                    "ticks",
                    "timeouts",
                }
                or any(
                    type(value) is not int
                    or not 0 <= value <= OPERATIONAL_SNAPSHOT_MAX_COUNTER
                    for value in counts.values()
                )
                or not isinstance(last_tick, Mapping)
                or set(last_tick)
                != {
                    "acquisition_job_id",
                    "attempt_count",
                    "contract",
                    "desired_state_sha256",
                    "reason_code",
                    "receipt_sha256",
                    "scheduler_transition",
                    "source_capture_id",
                    "status",
                    "terminal",
                    "workload_id",
                }
                or not isinstance(provider_configuration, Mapping)
                or set(provider_configuration)
                != {
                    "configured_count",
                    "requirements_unavailable_count",
                    "requirements_unavailable_workload_ids",
                    "workloads",
                }
                or retention
                != {
                    "automatic_reclaim": False,
                    "capacity_exhaustion": "fail_closed",
                    "source_reclaim_authorized": False,
                }
            ):
                return False
            configured_ids = list(
                self._worker_first_allowed_workload_ids
            )
            diagnostic_configured_ids = configuration.get(
                "configured_workload_ids"
            )
            blocked_ids = blocked_catalog.get("workload_ids")
            blocked_reasons = blocked_catalog.get("reason_counts")
            blocked_count = blocked_catalog.get("count")
            storage_values = tuple(capture_storage.values())
            if (
                diagnostic_configured_ids != configured_ids
                or configuration.get("collector_scope") != "all_sources"
                or configuration.get("policy")
                != RAW_CAPTURE_ADMISSION_POLICY
                or type(blocked_ids) is not list
                or blocked_ids != sorted(blocked_ids)
                or len(blocked_ids) != len(set(blocked_ids))
                or any(
                    not isinstance(value, str)
                    or re.fullmatch(
                        r"[a-z][a-z0-9_.-]{0,127}", value
                    )
                    is None
                    or value in configured_ids
                    for value in blocked_ids
                )
                or type(blocked_count) is not int
                or blocked_count != len(blocked_ids)
                or not 0 <= blocked_count <= 64
                or not isinstance(blocked_reasons, Mapping)
                or any(
                    not isinstance(key, str)
                    or re.fullmatch(r"[a-z][a-z0-9_]{0,95}", key)
                    is None
                    or type(value) is not int
                    or not 1 <= value <= 64
                    for key, value in blocked_reasons.items()
                )
                or sum(blocked_reasons.values()) != blocked_count
                or capture_storage.get("ledger_scope")
                != "cadence_only_event_store_excluded"
                or any(
                    type(value) is not int
                    or not 0 <= value <= 2**63 - 1
                    for value in storage_values
                    if value != "cadence_only_event_store_excluded"
                )
                or capture_storage["committed_count"]
                > capture_storage["committed_entries"]
                or capture_storage["reserved_count"]
                > capture_storage["reserved_entries"]
            ):
                return False
            # Start is allowed before Home Assistant has applied desired state.
            # A later tick must nevertheless prove that the acquisition lane
            # returned one bounded state/reason pair, not arbitrary payload.
            tick_status = last_tick.get("status")
            tick_reason = last_tick.get("reason_code")
            optional_ids = (
                last_tick.get("acquisition_job_id"),
                last_tick.get("scheduler_transition"),
                last_tick.get("source_capture_id"),
            )
            optional_hashes = (
                last_tick.get("desired_state_sha256"),
                last_tick.get("receipt_sha256"),
            )
            attempt_count = last_tick.get("attempt_count")
            workload_id = last_tick.get("workload_id")
            acquisition_ready = (
                isinstance(tick_status, str)
                and bool(tick_status)
                and len(tick_status) <= 64
                and re.fullmatch(r"[a-z][a-z0-9_]{0,63}", tick_status)
                is not None
                and isinstance(tick_reason, str)
                and bool(tick_reason)
                and len(tick_reason) <= 96
                and re.fullmatch(r"[a-z][a-z0-9_]{0,95}", tick_reason)
                is not None
                and last_tick.get("contract")
                == RAW_CAPTURE_ACQUISITION_RUNTIME_CONTRACT
                and type(last_tick.get("terminal")) is bool
                and all(
                    value is None
                    or (
                        isinstance(value, str)
                        and 1 <= len(value) <= 192
                        and re.fullmatch(
                            r"[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}",
                            value,
                        )
                        is not None
                    )
                    for value in optional_ids
                )
                and all(
                    value is None
                    or (
                        isinstance(value, str)
                        and re.fullmatch(r"[0-9a-f]{64}", value) is not None
                    )
                    for value in optional_hashes
                )
                and (
                    attempt_count is None
                    or type(attempt_count) is int
                    and 1 <= attempt_count <= OPERATIONAL_SNAPSHOT_MAX_COUNTER
                )
                and (
                    workload_id is None
                    or workload_id in self._worker_first_allowed_workload_ids
                )
                and (
                    not last_tick.get("terminal")
                    or workload_id is not None
                )
            )
            workloads = provider_configuration.get("workloads")
            unavailable_ids = provider_configuration.get(
                "requirements_unavailable_workload_ids"
            )
            configured_count = provider_configuration.get(
                "configured_count"
            )
            unavailable_count = provider_configuration.get(
                "requirements_unavailable_count"
            )
            provider_ready = (
                type(workloads) is list
                and len(workloads) == len(configured_ids)
                and type(unavailable_ids) is list
                and unavailable_ids == sorted(unavailable_ids)
                and len(unavailable_ids) == len(set(unavailable_ids))
                and set(unavailable_ids) <= set(configured_ids)
                and type(configured_count) is int
                and configured_count == len(configured_ids)
                and type(unavailable_count) is int
                and unavailable_count == len(unavailable_ids)
            )
            if provider_ready:
                observed_unavailable = []
                observed_terminal = []
                for expected_workload, row in zip(
                    configured_ids,
                    workloads,
                    strict=True,
                ):
                    if (
                        not isinstance(row, Mapping)
                        or set(row)
                        != {
                            "handler_configured",
                            "last_terminal",
                            "requirements_available",
                            "requirements_available_count",
                            "requirements_total",
                            "workload_id",
                        }
                        or row.get("workload_id") != expected_workload
                        or row.get("handler_configured") is not True
                        or type(row.get("requirements_available")) is not bool
                        or type(row.get("requirements_available_count"))
                        is not int
                        or type(row.get("requirements_total")) is not int
                        or not 0
                        <= row["requirements_available_count"]
                        <= row["requirements_total"]
                        <= 32
                        or row["requirements_available"]
                        != (
                            row["requirements_available_count"]
                            == row["requirements_total"]
                        )
                    ):
                        provider_ready = False
                        break
                    if not row["requirements_available"]:
                        observed_unavailable.append(expected_workload)
                    terminal = row.get("last_terminal")
                    if terminal is not None:
                        if (
                            not isinstance(terminal, Mapping)
                            or set(terminal) != {"reason_code", "status"}
                            or re.fullmatch(
                                r"[a-z][a-z0-9_]{0,95}",
                                str(terminal.get("reason_code")),
                            )
                            is None
                            or re.fullmatch(
                                r"[a-z][a-z0-9_]{0,63}",
                                str(terminal.get("status")),
                            )
                            is None
                        ):
                            provider_ready = False
                            break
                        observed_terminal.append((expected_workload, terminal))
                provider_ready = provider_ready and (
                    observed_unavailable == unavailable_ids
                )
                expected_terminal = []
                if last_tick.get("terminal"):
                    expected_terminal = [(
                        workload_id,
                        {
                            "reason_code": tick_reason,
                            "status": tick_status,
                        },
                    )]
                provider_ready = (
                    provider_ready
                    and observed_terminal == expected_terminal
                )
            operation_ready = (
                counts["start_calls"] >= 1
                and counts["ticks"] == 0
                and tick_status == "idle"
                and tick_reason == "not_started"
                and last_tick.get("terminal") is False
                if operation == "start"
                else counts["start_calls"] >= 1
                and counts["ticks"] >= 1
                and tick_reason != "not_started"
            )
            return (
                acquisition_ready
                and provider_ready
                and operation_ready
                and self._raw_capture_event_ingress_is_valid(
                    diagnostics.get("event_ingress")
                )
                and self._raw_capture_storage_capacity_is_valid(
                    diagnostics.get("storage_capacity")
                )
                and self._raw_capture_retention_guard_is_valid(
                    diagnostics.get("retention_guard"),
                    capture_storage=diagnostics.get("capture_storage"),
                    event_ingress=diagnostics.get("event_ingress"),
                    storage_capacity=diagnostics.get("storage_capacity"),
                )
                and diagnostics.get("contract")
                == RAW_CAPTURE_RUNTIME_CONTRACT
                and diagnostics.get("mode") == RAW_CAPTURE_SERVICE_MODE
                and diagnostics.get("state") == "running"
                and diagnostics.get("science_mode") == "disabled"
                and diagnostics.get("meteorological_admission") is False
                and diagnostics.get("meteorological_output") is False
                and diagnostics.get("dashboard_output") is False
                and diagnostics.get("terminal_router_wired") is False
                and diagnostics.get("downstream_consumption_required") is False
                and diagnostics.get("raw_store_commit_is_terminal") is True
                and diagnostics.get("production_enable_ready") is False
                and diagnostics.get("raw_payload_transport")
                == RAW_CAPTURE_RAW_PAYLOAD_TRANSPORT
                and diagnostics.get("shutdown_complete") is False
                and diagnostics.get("ownership_fence_held") is True
                and diagnostics.get("heavy_lane_active") is False
                and diagnostics.get("last_error_code") is None
                and diagnostics.get("legacy_heavy_admission") == "forbidden"
                and diagnostics.get("live_wiring") is True
                and diagnostics.get("control_job_types")
                == sorted(RAW_CAPTURE_CONTROL_JOB_TYPES)
                and isinstance(diagnostics.get("authority_sha256"), str)
                and re.fullmatch(
                    r"[0-9a-f]{64}", diagnostics["authority_sha256"]
                )
                is not None
                and isinstance(diagnostics.get("config_sha256"), str)
                and re.fullmatch(
                    r"[0-9a-f]{64}", diagnostics["config_sha256"]
                )
                is not None
            )
        # Preserve the exact DWD-only canary contract byte-for-byte.
        return (
            diagnostics.get("contract")
            == WORKER_FIRST_CANARY_RUNTIME_CONTRACT
            and diagnostics.get("mode") == WORKER_FIRST_CANARY_MODE
            and diagnostics.get("state") == "running"
            and diagnostics.get("live_wiring") is True
            and diagnostics.get("production_enable_ready") is False
            and diagnostics.get("meteorological_admission") is False
            and diagnostics.get("science_mode") == "shadow"
            and diagnostics.get("shutdown_complete") is False
            and diagnostics.get("ownership_fence_held") is True
            and diagnostics.get("acquisition_admission")
            == WORKER_FIRST_CANARY_ADMISSION
        )

    def _fail_worker_first_runtime(self, code: str) -> None:
        """Close admission and request process termination with one fixed code."""
        if self._runtime_role != "worker_first":
            return
        with self._admission_lock:
            self._accepting = False
            self._admission_rejection_code = "worker_first_runtime_failed"
            if self._worker_first_runtime_fatal_code is None:
                self._worker_first_runtime_fatal_code = code
            self._worker_first_runtime_fatal_event.set()
        self.stop_event.set()
        self._signal_operational_snapshot_runtime(immediate=True)

    def _start_job_dispatcher_if_needed(self) -> threading.Thread:
        """Start exactly one dispatcher after worker-first recovery is ready."""
        with self._admission_lock:
            thread = self._job_thread
            if thread is not None:
                return thread
            if self.stop_event.is_set():
                raise RuntimeError("job_dispatcher_start_after_stop")
            thread = threading.Thread(
                target=(
                    self._run_worker_first_jobs
                    if self._runtime_role == "worker_first"
                    else self._run_jobs
                ),
                name="storm-tracker-jobs",
                daemon=False,
            )
            try:
                thread.start()
            except BaseException:
                if self._runtime_role == "worker_first":
                    self._fail_worker_first_runtime(
                        "worker_first_dispatcher_start_failed"
                    )
                    raise _WorkerFirstRuntimeFatalError(
                        "worker_first_dispatcher_start_failed"
                    ) from None
                raise
            self._job_thread = thread
            return thread

    def _activate_job_subscription(
        self,
        *,
        opening_worker_first: bool = False,
    ) -> None:
        """Subscribe/replay only while transport and runtime admission are ready."""
        with self._admission_lock:
            if (
                self.stop_event.is_set()
                or not self._mqtt_connected.is_set()
                or (not self._accepting and not opening_worker_first)
                or self._jobs_subscription_active
            ):
                return
            try:
                result = self.client.subscribe(
                    topic(self.topic_root, "jobs/request"), qos=1
                )
            except BaseException:
                if self._runtime_role == "worker_first":
                    self._fail_worker_first_runtime(
                        "worker_first_subscription_failed"
                    )
                    raise _WorkerFirstRuntimeFatalError(
                        "worker_first_subscription_failed"
                    ) from None
                raise
            if self._runtime_role == "worker_first":
                valid_result = (
                    isinstance(result, tuple)
                    and len(result) == 2
                    and result[0] == self._mqtt_success_rc
                )
                if not valid_result:
                    self._fail_worker_first_runtime(
                        "worker_first_subscription_failed"
                    )
                    raise _WorkerFirstRuntimeFatalError(
                        "worker_first_subscription_failed"
                    )
            self._jobs_subscription_active = True
            if opening_worker_first:
                self._replay_recovered_terminal_statuses()
                self._worker_first_runtime_ready.set()
                self._accepting = True
                self._admission_rejection_code = None

    def _replay_recovered_terminal_statuses(self) -> None:
        """Publish durable terminal receipts once, in the caller's ordering."""
        recovered = self._recovered_terminal_statuses
        self._recovered_terminal_statuses = []
        for status in recovered:
            self._publish_job(
                Job(
                    job_id=status["job_id"],
                    job_type=status["job_type"],
                    deadline=status["deadline"],
                    payload={},
                ),
                status["state"],
                _replay=True,
                **dict(status["values"]),
            )

    @staticmethod
    def _worker_first_stop_is_terminal(diagnostics: Mapping) -> bool:
        """Accept stop only when the composition reports real quiescence."""
        return (
            diagnostics.get("state") == "stopped"
            and diagnostics.get("heavy_lane_active") is False
            and diagnostics.get("shutdown_complete") is True
            and diagnostics.get("ownership_fence_held") is False
        )

    def _operational_snapshot_status(self) -> dict | None:
        if self._operational_snapshot_runtime is None:
            return None
        with self._operational_snapshot_lock:
            health = deepcopy(self._operational_snapshot_health or {})
            thread = self._operational_snapshot_thread
        health["mqtt_connected"] = self._mqtt_connected.is_set()
        health["runtime_role"] = self._runtime_role
        health["sidecar_alive"] = thread is not None and thread.is_alive()
        return health

    def _operational_snapshot_worker_status(self) -> dict:
        """Expose only the bounded tuple that may create a snapshot revision."""
        with self._process_lock:
            active = self.active_job is not None
            failed_jobs = min(self.failed, OPERATIONAL_SNAPSHOT_MAX_COUNTER)
        if self.stop_event.is_set():
            state = "offline"
        elif active:
            state = "busy"
        else:
            state = "online"
        return {
            "state": state,
            "queue_depth": min(
                self.jobs.qsize(), OPERATIONAL_SNAPSHOT_MAX_COUNTER
            ),
            "failed_jobs": failed_jobs,
        }

    def _signal_operational_snapshot_runtime(self, *, immediate: bool) -> None:
        """Wake the sidecar without doing async or SQLite work in MQTT threads."""
        if self._operational_snapshot_runtime is None:
            return
        now = time.monotonic()
        with self._operational_snapshot_lock:
            last_status_wake = self._operational_snapshot_last_status_wake_at
            if (
                not immediate
                and last_status_wake is not None
                and now - last_status_wake
                < OPERATIONAL_SNAPSHOT_STATUS_WAKE_MIN_SECONDS
            ):
                return
            self._operational_snapshot_last_status_wake_at = now
            loop = self._operational_snapshot_loop
            wake = self._operational_snapshot_wake
        if loop is None or wake is None:
            return
        try:
            loop.call_soon_threadsafe(wake.set)
        except RuntimeError:
            # A concurrent normal shutdown may already have closed the loop.
            return

    async def _publish_operational_snapshot(self, payload: bytes) -> None:
        """Publish exact canonical bytes and return only after a QoS1 PUBACK."""
        if type(payload) is not bytes:
            raise _OperationalSnapshotMqttError("snapshot_payload_invalid")
        if self.stop_event.is_set():
            raise asyncio.CancelledError
        if not self._mqtt_connected.is_set():
            raise _OperationalSnapshotMqttError("mqtt_disconnected")
        try:
            info = self.client.publish(
                topic(self.topic_root, "operational/snapshot"),
                payload,
                qos=1,
                retain=True,
            )
        except Exception:
            raise _OperationalSnapshotMqttError("mqtt_publish_call_failed") from None
        if (
            self._mqtt_success_rc is None
            or getattr(info, "rc", None) != self._mqtt_success_rc
        ):
            raise _OperationalSnapshotMqttError("mqtt_publish_rejected")

        loop = asyncio.get_running_loop()
        deadline = loop.time() + OPERATIONAL_SNAPSHOT_PUBLISH_TIMEOUT_SECONDS
        while True:
            if self.stop_event.is_set():
                raise asyncio.CancelledError
            if not self._mqtt_connected.is_set():
                raise _OperationalSnapshotMqttError("mqtt_disconnected")
            try:
                published = info.is_published()
            except Exception:
                raise _OperationalSnapshotMqttError(
                    "mqtt_publish_ack_invalid"
                ) from None
            if published is True:
                return
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise _OperationalSnapshotMqttError("mqtt_publish_ack_timeout")
            await asyncio.sleep(
                min(OPERATIONAL_SNAPSHOT_ACK_POLL_SECONDS, remaining)
            )

    async def _operational_snapshot_lifecycle(self) -> None:
        """Own one runtime instance on one event loop until service shutdown."""
        runtime = self._operational_snapshot_runtime
        if runtime is None:
            return
        loop = asyncio.get_running_loop()
        wake = asyncio.Event()
        with self._operational_snapshot_lock:
            self._operational_snapshot_loop = loop
            self._operational_snapshot_wake = wake
        start_called = False
        try:
            while not self.stop_event.is_set():
                wake.clear()
                if self._runtime_role == "worker_first":
                    mqtt_generation = (
                        self._worker_first_mqtt_generation_if_connected()
                    )
                    mqtt_connected = mqtt_generation is not None
                else:
                    mqtt_generation = None
                    mqtt_connected = self._mqtt_connected.is_set()
                if mqtt_connected:
                    operation_stage = "prepare"
                    try:
                        readiness_operation = (
                            "start" if not start_called else "tick"
                        )
                        if not start_called:
                            if self._runtime_role != "worker_first":
                                # Preserve the legacy snapshot sidecar's retry
                                # semantics byte-for-byte: one failed start is
                                # followed by ticks, rather than another start.
                                start_called = True
                            operation_stage = "runtime_start"
                            diagnostics = await runtime.async_start()
                            if self._runtime_role == "worker_first":
                                start_called = True
                        else:
                            operation_stage = "runtime_tick"
                            diagnostics = await runtime.async_tick()
                        operation_stage = "mqtt_proof"
                        if (
                            self._runtime_role == "worker_first"
                            and not self._worker_first_mqtt_proof_is_current(
                                mqtt_generation
                            )
                        ):
                            # A disconnect/reconnect invalidates every proof
                            # that began on the previous transport generation.
                            # Loop once more so admission can only reopen from
                            # fresh exact diagnostics on the current session.
                            continue
                        operation_stage = "health_cache"
                        self._cache_operational_snapshot_health(diagnostics)
                        if (
                            self._runtime_role == "worker_first"
                            and start_called
                        ):
                            operation_stage = "readiness_check"
                            if not self._worker_first_start_is_ready(
                                diagnostics,
                                operation=readiness_operation,
                            ):
                                self._fail_worker_first_runtime(
                                    "worker_first_start_not_ready"
                                )
                                raise RuntimeError(
                                    "worker_first_start_not_ready"
                                )
                            operation_stage = "admission_open"
                            self._open_worker_first_admission(mqtt_generation)
                    except asyncio.CancelledError:
                        if self.stop_event.is_set():
                            break
                        if self._runtime_role == "worker_first":
                            if not self._worker_first_mqtt_proof_is_current(
                                mqtt_generation
                            ):
                                continue
                            self._fail_worker_first_runtime(
                                "worker_first_operation_cancelled"
                            )
                            raise
                        self._record_operational_snapshot_error(
                            "operation_cancelled"
                        )
                    except Exception as error:
                        if self._runtime_role == "worker_first":
                            if not self._worker_first_mqtt_proof_is_current(
                                mqtt_generation
                            ):
                                continue
                            _LOGGER.error(
                                "Worker-first operation fail-closed: "
                                "stage=%s class=%s reason=%s",
                                operation_stage,
                                type(error).__name__,
                                _worker_first_safe_error_reason(error),
                            )
                            self._fail_worker_first_runtime(
                                "worker_first_operation_failed"
                            )
                            raise
                        self._record_operational_snapshot_error(
                            "sidecar_operation_failed"
                        )
                    except BaseException:
                        if self._runtime_role == "worker_first":
                            self._fail_worker_first_runtime(
                                "worker_first_operation_failed"
                            )
                        raise
                if self.stop_event.is_set():
                    break
                try:
                    await asyncio.wait_for(
                        wake.wait(), timeout=self.provider_cycle_seconds
                    )
                except TimeoutError:
                    pass
        finally:
            try:
                stop_started = loop.time()
                stop_attempts = 0
                while True:
                    stop_attempts += 1
                    remaining = (
                        WORKER_FIRST_STOP_TOTAL_SECONDS
                        - (loop.time() - stop_started)
                    )
                    if self._runtime_role == "worker_first" and remaining <= 0:
                        self._fail_worker_first_runtime(
                            "worker_first_shutdown_incomplete"
                        )
                        break
                    try:
                        if self._runtime_role == "worker_first":
                            diagnostics = await asyncio.wait_for(
                                runtime.async_stop(), timeout=remaining
                            )
                        else:
                            diagnostics = await runtime.async_stop()
                    except TimeoutError:
                        self._fail_worker_first_runtime(
                            "worker_first_shutdown_timeout"
                        )
                        break
                    self._cache_operational_snapshot_health(diagnostics)
                    if (
                        self._runtime_role != "worker_first"
                        or self._worker_first_stop_is_terminal(diagnostics)
                    ):
                        break
                    if (
                        stop_attempts >= WORKER_FIRST_STOP_MAX_ATTEMPTS
                        or loop.time() - stop_started
                        >= WORKER_FIRST_STOP_TOTAL_SECONDS
                    ):
                        self._fail_worker_first_runtime(
                            "worker_first_shutdown_incomplete"
                        )
                        break
                    await asyncio.sleep(WORKER_FIRST_STOP_RETRY_SECONDS)
            except BaseException:
                if self._runtime_role == "worker_first":
                    self._fail_worker_first_runtime(
                        "worker_first_shutdown_failed"
                    )
                self._record_operational_snapshot_error(
                    "sidecar_shutdown_failed"
                )
            finally:
                with self._operational_snapshot_lock:
                    if self._operational_snapshot_loop is loop:
                        self._operational_snapshot_loop = None
                    if self._operational_snapshot_wake is wake:
                        self._operational_snapshot_wake = None

    def _run_operational_snapshot_runtime(self) -> None:
        try:
            asyncio.run(self._operational_snapshot_lifecycle())
        except BaseException:
            self._record_operational_snapshot_error("sidecar_lifecycle_failed")
            if self._runtime_role == "worker_first":
                self._fail_worker_first_runtime(
                    "worker_first_lifecycle_failed"
                )
                _LOGGER.error(
                    "Worker-first runtime is fail-closed gestopt: %s",
                    self._worker_first_runtime_fatal_code,
                )
            else:
                _LOGGER.exception("Operational snapshot sidecar is gestopt")

    def _start_operational_snapshot_runtime(self) -> threading.Thread | None:
        if self._operational_snapshot_runtime is None:
            return None
        with self._operational_snapshot_lock:
            if self._operational_snapshot_thread is not None:
                raise RuntimeError("operational snapshot sidecar is al gestart")
            thread = threading.Thread(
                target=self._run_operational_snapshot_runtime,
                name="storm-tracker-operational-snapshot",
                # A corrupted/cancellation-resistant canary must not keep the
                # process alive after the bounded fatal shutdown path. The OS
                # then releases the ownership lock. Legacy behavior remains
                # unchanged and non-daemon.
                daemon=self._runtime_role == "worker_first",
            )
            try:
                thread.start()
            except BaseException:
                if self._runtime_role == "worker_first":
                    self._fail_worker_first_runtime(
                        "worker_first_sidecar_start_failed"
                    )
                    raise _WorkerFirstRuntimeFatalError(
                        "worker_first_sidecar_start_failed"
                    ) from None
                raise
            self._operational_snapshot_thread = thread
        return thread

    def _technical_desired_state_sha256(self) -> str | None:
        """Read only an exact desired-state identity from bounded runtime health."""

        with self._operational_snapshot_lock:
            health = self._operational_snapshot_health
            last_tick = (
                health.get("last_tick")
                if isinstance(health, Mapping)
                else None
            )
            value = (
                last_tick.get("desired_state_sha256")
                if isinstance(last_tick, Mapping)
                else None
            )
        if (
            not isinstance(value, str)
            or re.fullmatch(r"[0-9a-f]{64}", value) is None
        ):
            return None
        return f"sha256:{value}"

    @staticmethod
    def _unknown_worker_technical_storage() -> dict[str, object]:
        """Return the fixed fail-closed storage projection."""

        return {
            "retention_alarm": "unknown",
            "capacity_alarm": "unknown",
            "headroom_bytes": None,
            "headroom_percent": None,
            "storage_reason_codes": None,
        }

    @staticmethod
    def _project_worker_technical_retention_guard(
        guard: object,
    ) -> dict[str, object] | None:
        """Project only an exact, audited guard-v2 technical storage proof."""

        try:
            if (
                not isinstance(guard, Mapping)
                or len(guard) != len(WORKER_TECHNICAL_RETENTION_GUARD_FIELDS)
                or not all(
                    field in guard
                    for field in WORKER_TECHNICAL_RETENTION_GUARD_FIELDS
                )
                or guard.get("contract")
                != RAW_CAPTURE_RETENTION_GUARD_CONTRACT
                or guard.get("capacity_policy")
                != "config_bound_optional_event_envelope_reserved_fail_closed_v2"
                or guard.get("full_audit_completed") is not True
                or type(guard.get("inspection_count")) is not int
                or not 2 <= guard["inspection_count"] <= 2**63 - 1
                or guard.get("live_delete_performed") is not False
                or guard.get("unmanaged_data_action")
                != "alarm_and_block_never_delete"
                or type(guard.get("event_capacity_claimed")) is not bool
                or type(guard.get("admission_ready")) is not bool
            ):
                return None

            ownership = guard.get("ownership")
            reclamation = guard.get("reclamation")
            if (
                not isinstance(ownership, Mapping)
                or len(ownership) != len(WORKER_TECHNICAL_RETENTION_OWNERSHIP)
                or not all(
                    field in ownership
                    for field in WORKER_TECHNICAL_RETENTION_OWNERSHIP
                )
                or any(
                    ownership.get(field) is not expected
                    for field, expected
                    in WORKER_TECHNICAL_RETENTION_OWNERSHIP.items()
                )
                or not isinstance(reclamation, Mapping)
                or len(reclamation)
                != len(WORKER_TECHNICAL_RETENTION_RECLAMATION)
                or not all(
                    field in reclamation
                    for field in WORKER_TECHNICAL_RETENTION_RECLAMATION
                )
                or any(
                    reclamation.get(field) != expected
                    for field, expected
                    in WORKER_TECHNICAL_RETENTION_RECLAMATION.items()
                )
            ):
                return None

            alarms = guard.get("alarm_codes")
            if (
                type(alarms) is not list
                or len(alarms) > len(RAW_CAPTURE_RETENTION_GUARD_ALARM_PRIORITY)
                or any(type(code) is not str for code in alarms)
                or len(alarms) != len(set(alarms))
                or alarms
                != [
                    code
                    for code in RAW_CAPTURE_RETENTION_GUARD_ALARM_PRIORITY
                    if code in alarms
                ]
                or guard.get("reason_code")
                != (alarms[0] if alarms else "ready")
            ):
                return None
            blocking = any(
                code in RAW_CAPTURE_RETENTION_GUARD_BLOCKING_ALARMS
                for code in alarms
            )
            if (
                guard.get("admission_ready") != (not blocking)
                or any(
                    code in WORKER_TECHNICAL_RETENTION_INVALID_PROOF_ALARMS
                    for code in alarms
                )
            ):
                return None

            capacity = guard.get("capacity")
            if (
                not isinstance(capacity, Mapping)
                or len(capacity)
                != len(WORKER_TECHNICAL_RETENTION_CAPACITY_FIELDS)
                or not all(
                    field in capacity
                    for field in WORKER_TECHNICAL_RETENTION_CAPACITY_FIELDS
                )
            ):
                return None
            capacity_values: dict[str, int] = {}
            for field in WORKER_TECHNICAL_RETENTION_CAPACITY_FIELDS:
                value = capacity.get(field)
                if type(value) is not int or not 0 <= value <= 2**63 - 1:
                    return None
                capacity_values[field] = value
            if any(
                capacity_values[field] <= 0
                for field in WORKER_TECHNICAL_RETENTION_POSITIVE_CAPACITY_FIELDS
            ):
                return None

            if not (
                capacity_values["cadence_ledger_byte_headroom"]
                <= capacity_values["cadence_ledger_max_bytes"]
                and capacity_values["cadence_ledger_entry_headroom"]
                <= capacity_values["cadence_ledger_max_entries"]
                and capacity_values["cadence_reservation_headroom"]
                <= capacity_values["cadence_ledger_max_reservations"]
                and capacity_values["cadence_raw_byte_headroom"]
                <= capacity_values["cadence_raw_max_bytes"]
                and capacity_values["cadence_raw_entry_headroom"]
                <= capacity_values["cadence_raw_max_entries"]
                and capacity_values["cadence_receipt_headroom"]
                <= capacity_values["cadence_receipt_max_entries"]
                and capacity_values["cadence_capture_headroom"]
                <= capacity_values["cadence_capture_max_entries"]
                and capacity_values["cadence_catalog_headroom_bytes"]
                <= capacity_values["cadence_raw_catalog_max_bytes"]
                and capacity_values["cadence_ledger_catalog_headroom_bytes"]
                <= capacity_values["cadence_ledger_catalog_max_bytes"]
                and capacity_values["cadence_cycle_reserve_bytes"]
                <= min(
                    capacity_values["cadence_ledger_max_bytes"],
                    capacity_values["cadence_raw_max_bytes"],
                )
                and capacity_values["cadence_cycle_reserve_entries"]
                <= min(
                    capacity_values["cadence_ledger_max_entries"],
                    capacity_values["cadence_raw_max_entries"],
                    capacity_values["cadence_receipt_max_entries"],
                )
                and capacity_values["cadence_cycle_reserve_captures"]
                <= min(
                    capacity_values["cadence_capture_max_entries"],
                    capacity_values["cadence_ledger_max_reservations"],
                )
                and capacity_values["filesystem_free_bytes"]
                <= capacity_values["filesystem_total_bytes"]
                and capacity_values["persistent_envelope_free_required_bytes"]
                <= capacity_values["configured_persistent_max_bytes"]
                and capacity_values["physical_reserve_headroom_bytes"]
                == max(
                    0,
                    capacity_values["filesystem_free_bytes"]
                    - capacity_values[
                        "persistent_envelope_free_required_bytes"
                    ]
                    - capacity_values[
                        "host_operational_free_reserve_bytes"
                    ],
                )
                and capacity_values["host_operational_free_headroom_bytes"]
                == capacity_values["physical_reserve_headroom_bytes"]
                and capacity_values["host_operational_free_reserve_bytes"]
                >= RAW_CAPTURE_HOST_OPERATIONAL_FREE_RESERVE_BYTES
                and capacity_values["cadence_ledger_byte_headroom"]
                >= capacity_values["cadence_cycle_reserve_bytes"]
                and capacity_values["cadence_raw_byte_headroom"]
                >= capacity_values["cadence_cycle_reserve_bytes"]
                and capacity_values["cadence_ledger_entry_headroom"]
                >= capacity_values["cadence_cycle_reserve_entries"]
                and capacity_values["cadence_raw_entry_headroom"]
                >= capacity_values["cadence_cycle_reserve_entries"]
                and capacity_values["cadence_receipt_headroom"]
                >= capacity_values["cadence_cycle_reserve_entries"]
                and capacity_values["cadence_capture_headroom"]
                >= capacity_values["cadence_cycle_reserve_captures"]
                and capacity_values["cadence_reservation_headroom"]
                >= capacity_values["cadence_cycle_reserve_captures"]
            ):
                return None

            severity = (
                "critical"
                if blocking
                else "warning" if alarms else "clear"
            )
            return {
                "retention_alarm": severity,
                "capacity_alarm": severity,
                "headroom_bytes": min(
                    capacity_values[field]
                    for field
                    in WORKER_TECHNICAL_RETENTION_BYTE_HEADROOM_FIELDS
                ),
                # No common, proven denominator exists in guard-v2.
                "headroom_percent": None,
                "storage_reason_codes": list(alarms),
            }
        except Exception:
            return None

    def _worker_technical_storage(self) -> dict[str, object]:
        """Read and project only the authoritative cached retention guard."""

        with self._operational_snapshot_lock:
            health = self._operational_snapshot_health
            guard = (
                health.get("retention_guard")
                if isinstance(health, Mapping)
                else None
            )
            projection = self._project_worker_technical_retention_guard(guard)
        return projection or self._unknown_worker_technical_storage()

    def _worker_technical_source(
        self,
        *,
        state: str,
        observed_at: float,
        queue_depth: int,
        terminal_jobs: int,
        failed_jobs: int,
    ) -> dict:
        """Build one exact secret-free source; unproven fields stay unknown."""

        worker_state = (
            state
            if state in {
                "waiting", "online", "busy", "degraded", "offline", "unknown",
            }
            else "unknown"
        )
        storage = self._worker_technical_storage()
        source = {
            "contract": WORKER_TECHNICAL_SOURCE_CONTRACT,
            "worker_state": worker_state,
            "worker_observed_at": (
                observed_at
                if isinstance(observed_at, (int, float))
                and not isinstance(observed_at, bool)
                and math.isfinite(float(observed_at))
                and observed_at >= 0.0
                else None
            ),
            "worker_version": __version__,
            "build_sha256": f"sha256:{self.build_sha256}",
            # Config fingerprints are intentionally withheld until the worker
            # owns an explicit secret-free configuration projection.
            "config_sha256": None,
            "desired_state_sha256": self._technical_desired_state_sha256(),
            "provider_age_seconds": None,
            "provider_status": "unknown",
            "frame_age_seconds": None,
            "frame_status": "unknown",
            "cache_age_seconds": None,
            "cache_status": "unknown",
            "queue_depth": min(
                OPERATIONAL_SNAPSHOT_MAX_COUNTER, max(0, queue_depth)
            ),
            "terminal_jobs": min(
                OPERATIONAL_SNAPSHOT_MAX_COUNTER, max(0, terminal_jobs)
            ),
            "failed_jobs": min(
                OPERATIONAL_SNAPSHOT_MAX_COUNTER, max(0, failed_jobs)
            ),
            "retention_alarm": storage["retention_alarm"],
            "capacity_alarm": storage["capacity_alarm"],
            "headroom_bytes": storage["headroom_bytes"],
            "headroom_percent": storage["headroom_percent"],
            "storage_reason_codes": storage["storage_reason_codes"],
            "transition_status": "unknown",
            # Worker release identity is authoritative above, but no separate
            # current-vs-next transition contract is wired yet.
            "current_version": None,
            "next_version": None,
            "transition_reason_codes": None,
            "replay_contract_version": None,
            "replay_algorithm_version": None,
        }
        if set(source) != WORKER_TECHNICAL_SOURCE_FIELDS:
            raise RuntimeError("worker technical source schema drift")
        return source

    def _status(self, state: str) -> dict:
        now = time.time()
        with self._admission_lock:
            accepted = self.accepted
            accepted_terminal = self.accepted_terminal
            accepted_completed = self.accepted_completed
            accepted_failed = self.accepted_failed
            expired = self.expired
            cancelled = self.cancelled
            accepted_inflight = len(self._accepted_job_ids)
            admission_open = self._accepting
            admission_rejection_code = self._admission_rejection_code
            worker_first_runtime_fatal_code = (
                self._worker_first_runtime_fatal_code
            )
        with self._process_lock:
            active = self.active_job
            processes = [
                {
                    "process_id": process_id,
                    "name": health.name,
                    "state": health.state,
                    "healthy": (
                        now - health.last_heartbeat
                        <= max(35.0, self.heartbeat_seconds * 3)
                        and health.state != "error"
                    ),
                    "last_heartbeat": health.last_heartbeat,
                    "heartbeat_age_seconds": round(
                        max(0.0, now - health.last_heartbeat), 1
                    ),
                    "active_job_id": health.active_job_id,
                    "active_seconds": (
                        round(max(0.0, now - health.active_since), 1)
                        if health.active_since is not None
                        else None
                    ),
                    "completed_jobs": health.completed_jobs,
                    "failed_jobs": health.failed_jobs,
                    "last_duration_seconds": health.last_duration_seconds,
                    "error": health.error,
                    "effect_contract_id": (
                        self.job_effect_contracts[process_id].contract_id
                        if process_id in self.job_effect_contracts
                        else None
                    ),
                    "effect_class": (
                        self.job_effect_contracts[process_id].effect_class
                        if process_id in self.job_effect_contracts
                        else None
                    ),
                    "hard_kill_safe": (
                        self.job_effect_contracts[
                            process_id
                        ].hard_kill_safe
                        if process_id in self.job_effect_contracts
                        else None
                    ),
                    "effect_preparation": (
                        self.job_effect_contracts[process_id].preparation
                        if process_id in self.job_effect_contracts
                        else None
                    ),
                    "effect_compensation": (
                        self.job_effect_contracts[process_id].compensation
                        if process_id in self.job_effect_contracts
                        else None
                    ),
                    "effect_contract_verified": (
                        self.job_effect_contracts[process_id].verified
                        if process_id in self.job_effect_contracts
                        else None
                    ),
                }
                for process_id, health in self.processes.items()
            ]
        queued_by_priority = self._queued_priority_counts()
        status = envelope(
            "worker_status",
            worker_id=self.worker_id,
            worker_version=__version__,
            build_id=self.build_id,
            observation_contract_version=2,
            state=state,
            compute_profile=self.compute_profile,
            cpu_limit_percent=self.cpu_limit_percent,
            worker_nice=self.worker_nice,
            cpu_threads=self.cpu_threads,
            uptime_seconds=round(time.time() - self.started_at, 1),
            queue_depth=self.jobs.qsize(),
            queue_capacity=self.jobs.maxsize,
            queue_depth_by_priority=queued_by_priority,
            active_job_id=active.job_id if active else None,
            active_job_type=active.job_type if active else None,
            active_job_priority=(
                self._job_priority_name(active.job_type) if active else None
            ),
            completed_jobs=self.completed,
            failed_jobs=self.failed,
            accepted_jobs=accepted,
            accepted_terminal_jobs=accepted_terminal,
            accepted_inflight_jobs=accepted_inflight,
            accepted_completed_jobs=accepted_completed,
            accepted_failed_jobs=accepted_failed,
            job_admission_open=admission_open,
            job_admission_rejection_code=admission_rejection_code,
            worker_first_runtime_fatal_code=worker_first_runtime_fatal_code,
            expired_jobs=expired,
            cancelled_jobs=cancelled,
            technical_diagnostics=self._worker_technical_source(
                state=state,
                observed_at=now,
                queue_depth=self.jobs.qsize(),
                terminal_jobs=accepted_terminal,
                failed_jobs=self.failed,
            ),
            durable_job_receipts=self._receipt_store is not None,
            job_receipt_schema_version=(
                JOB_RECEIPT_SCHEMA_VERSION
                if self._receipt_store is not None
                else None
            ),
            recovered_terminal_jobs=self.recovered_terminal_jobs,
            effect_contracts_required=self.require_effect_contracts,
            effect_contract_coverage_complete=(
                set(self.job_handlers) == set(self.job_effect_contracts)
                and all(
                    contract.verified and contract.hard_kill_safe
                    for contract in self.job_effect_contracts.values()
                )
            ),
            processes=processes,
        )
        if self._autonomous_scheduler is not None:
            with self._scheduler_lock:
                scheduler_health = dict(self._scheduler_health)
                scheduler_health["lease_inflight"] = (
                    self._scheduler_lease is not None
                )
            status["autonomous_scheduler"] = scheduler_health
        operational_snapshot_status = self._operational_snapshot_status()
        if operational_snapshot_status is not None:
            status["operational_snapshot_runtime"] = operational_snapshot_status
        return status

    def _touch_process(
        self,
        process_id: str,
        state: str | None = None,
        *,
        job_id: str | None = None,
        error: str | None = None,
    ) -> ProcessHealth:
        with self._process_lock:
            health = self.processes[process_id]
            now = time.time()
            health.last_heartbeat = now
            if state is not None:
                if state in {"busy", "working"} and health.active_since is None:
                    health.active_since = now
                elif state not in {"busy", "working"}:
                    if health.active_since is not None:
                        health.last_duration_seconds = round(
                            max(0.0, now - health.active_since), 1
                        )
                    health.active_since = None
                health.state = state
            health.active_job_id = job_id
            health.error = error
            return health

    def _publish_status(self, state: str = "online") -> None:
        status = self._status(state)
        self.client.publish(
            topic(self.topic_root, "worker/status"),
            encode(status),
            qos=1,
            retain=True,
        )
        worker_status = self._operational_snapshot_worker_status()
        projection = (
            worker_status["state"],
            worker_status["queue_depth"],
            worker_status["failed_jobs"],
        )
        with self._operational_snapshot_lock:
            previous = self._operational_snapshot_last_worker_status
            self._operational_snapshot_last_worker_status = projection
        # The periodic MQTT heartbeat is intentionally transport-only.  Wake
        # the heavy operational runtime only for a real bounded status change;
        # its own interval drives provider acquisition independently.
        if previous is not None and projection != previous:
            self._signal_operational_snapshot_runtime(immediate=False)

    def _heartbeat_process(self, process_id: str) -> None:
        """Ververs alleen liveness; behoud werkstatus, taak en foutdetail."""
        with self._process_lock:
            self.processes[process_id].last_heartbeat = time.time()

    def _publish_job(
        self,
        job: Job,
        state: str,
        *,
        _replay: bool = False,
        **values,
    ) -> bool:
        encoded_status = encode(
            envelope(
                "job_status",
                worker_id=self.worker_id,
                job_id=job.job_id,
                job_type=job.job_type,
                state=state,
                **values,
            )
        )
        with self._admission_lock:
            with self._process_lock:
                previous = self._job_status_cache.get(job.job_id)
                if (
                    not _replay
                    and previous is not None
                    and previous.get("state") in TERMINAL_JOB_STATES
                ):
                    # Een terminale overgang is definitief. Dit blokkeert ook
                    # een vertraagde queued/running-publicatie die door een
                    # shutdownrace pas na cancelled/completed aan bod komt.
                    return False
                if not _replay:
                    if self._receipt_store is not None:
                        persisted, changed = self._receipt_store.record(
                            job,
                            state,
                            dict(values),
                            accepted=(
                                job.job_id in self._accepted_job_ids
                            ),
                        )
                        if not changed:
                            self._job_status_cache[job.job_id] = {
                                "job_type": persisted["job_type"],
                                "request_digest": persisted[
                                    "request_digest"
                                ],
                                "state": persisted["state"],
                                "values": dict(persisted["values"]),
                                "updated_at": persisted["updated_at"],
                            }
                            self._seen[job.job_id] = persisted["updated_at"]
                            return False
                    self._job_status_cache[job.job_id] = {
                        "job_type": job.job_type,
                        "request_digest": _job_request_digest(job),
                        "state": state,
                        "values": dict(values),
                        "updated_at": time.time(),
                    }
                    while len(self._job_status_cache) > JOB_STATUS_CACHE_MAX:
                        evict = next(
                            (
                                job_id
                                for job_id, status
                                in self._job_status_cache.items()
                                if status.get("state")
                                in TERMINAL_JOB_STATES
                            ),
                            None,
                        )
                        if evict is None:
                            break
                        self._job_status_cache.pop(evict, None)
                        self._seen.pop(evict, None)
                    if (
                        state in TERMINAL_JOB_STATES
                        and job.job_id in self._accepted_job_ids
                    ):
                        self._accepted_job_ids.remove(job.job_id)
                        self.accepted_terminal += 1
                        if state == "completed":
                            self.accepted_completed += 1
                        elif state == "failed":
                            self.accepted_failed += 1
                        elif state == "expired":
                            self.expired += 1
                        elif state == "cancelled":
                            self.cancelled += 1
        self.client.publish(
            topic(self.topic_root, f"jobs/{job.job_id}/status"),
            encoded_status,
            qos=1,
            retain=False,
        )
        return True

    @staticmethod
    def _recover_job_reference(payload, data: dict | None = None) -> Job | None:
        """Recover enough request identity to publish a terminal failure."""
        candidate = data
        if not isinstance(candidate, dict):
            try:
                candidate = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
                return None
        if not isinstance(candidate, dict):
            return None
        job_id = candidate.get("job_id")
        job_type = candidate.get("job_type")
        if job_id is None or job_type is None:
            return None
        job_id = str(job_id).strip()
        job_type = str(job_type).strip()
        if not job_id or not job_type:
            return None
        return Job(
            job_id=job_id,
            job_type=job_type,
            deadline=0.0,
            payload={},
        )

    def _job_priority(self, job_type: str) -> int:
        """Return queue priority where lager getal = hogere prioriteit."""
        if job_type in JOB_PRIORITY_CONTROL:
            return 0
        if job_type in JOB_PRIORITY_GROUND:
            return 1
        if job_type in JOB_PRIORITY_METEOROLOGICAL:
            return 2
        if job_type in JOB_PRIORITY_LOW:
            return 4
        return 3

    def _job_priority_name(self, job_type: str) -> str:
        return JOB_PRIORITY_NAMES[self._job_priority(job_type)]

    def _queued_priority_counts(self) -> dict[str, int]:
        """Snapshot bounded queue composition without removing any job."""
        counts = {name: 0 for name in JOB_PRIORITY_NAMES.values()}
        with self.jobs.mutex:
            queued = tuple(self.jobs.queue)
        for value in queued:
            if isinstance(value, Job):
                priority = self._job_priority(value.job_type)
            else:
                priority = int(value[0])
            name = JOB_PRIORITY_NAMES.get(priority, "standard")
            counts[name] += 1
        return counts

    def _record_dispatch_failure(self, error: str) -> None:
        """Count an input-side failure without corrupting an active handler."""
        with self._process_lock:
            self.failed += 1
            dispatcher = self.processes["dispatcher"]
            dispatcher.failed_jobs += 1
            dispatcher.last_heartbeat = time.time()
            dispatcher.error = error

    def _publish_runtime_status(self) -> None:
        self._publish_status("busy" if self.active_job is not None else "online")

    def _scheduler_mark(
        self,
        state: str,
        *,
        error_code: str | None = None,
        success: bool = False,
        counter: str | None = None,
    ) -> None:
        """Update bounded sidecar health without exposing work identity."""
        if self._autonomous_scheduler is None:
            return
        now = time.time()
        with self._scheduler_lock:
            self._scheduler_health["state"] = state
            self._scheduler_health["last_tick_at"] = now
            self._scheduler_health["error_code"] = error_code
            if success:
                self._scheduler_health["last_success_at"] = now
            if counter is not None:
                self._scheduler_health[counter] = min(
                    1_000_000_000,
                    int(self._scheduler_health[counter]) + 1,
                )
        with self._process_lock:
            health = self.processes.get("autonomous_scheduler")
            if health is not None:
                health.last_heartbeat = now
                health.state = state
                health.active_job_id = None
                health.error = error_code

    @staticmethod
    def _scheduler_error_code(error: BaseException) -> str:
        """Map all scheduler failures to fixed privacy-safe codes."""
        if isinstance(error, SchedulerClockError):
            return "scheduler_clock_blocked"
        if isinstance(error, SchedulerDesiredStateError):
            return "scheduler_desired_state_blocked"
        if isinstance(error, SchedulerCapacityError):
            return "scheduler_capacity_blocked"
        if isinstance(error, SchedulerConflict):
            return "scheduler_conflict"
        if isinstance(error, ScheduleIntentError):
            return "scheduler_plan_invalid"
        if isinstance(error, AutonomousSchedulerError):
            return "scheduler_store_error"
        return "scheduler_internal_error"

    def _scheduler_record_error(self, error: BaseException) -> None:
        code = self._scheduler_error_code(error)
        self._scheduler_mark(
            "error",
            error_code=code,
            counter="error_events",
        )
        _LOGGER.warning("Autonome scheduler gepauzeerd: %s", code)

    def _scheduler_receipt_by_identity(
        self,
        job_id: str,
        job_type: str,
        request_digest: str,
    ) -> tuple[str | None, bool]:
        """Return receipt state and whether its immutable identity conflicts."""
        durable = (
            self._receipt_store.load(job_id)
            if self._receipt_store is not None
            else None
        )
        if durable is None:
            with self._process_lock:
                cached = dict(self._job_status_cache.get(job_id, {}))
        else:
            cached = durable
        if not cached:
            return None, False
        conflict = (
            cached.get("job_type") != job_type
            or cached.get("request_digest") != request_digest
        )
        return str(cached.get("state") or "") or None, conflict

    def _scheduler_job_present(self, job_id: str) -> bool:
        with self._process_lock:
            if self.active_job is not None and self.active_job.job_id == job_id:
                return True
        with self.jobs.mutex:
            queued = tuple(self.jobs.queue)
        for value in queued:
            job = value if isinstance(value, Job) else value[2]
            if job.job_id == job_id:
                return True
        return False

    def _scheduler_transition(
        self,
        job_id: str,
        lease_token: str,
        terminal_state: str,
        *,
        receipt_replay: bool,
    ) -> bool:
        """Apply one service terminal to the durable scheduler lease."""
        scheduler = self._autonomous_scheduler
        if scheduler is None:
            return False
        try:
            if terminal_state == "completed":
                scheduler.complete(job_id, lease_token)
                counter = "completed_jobs"
            else:
                error_code = SCHEDULER_TERMINAL_RETRY_CODES.get(
                    terminal_state,
                    "service_job_failed",
                )
                scheduler.retry(job_id, lease_token, error_code)
                counter = "retried_jobs"
        except BaseException as err:
            self._scheduler_record_error(err)
            return False
        self._scheduler_mark("idle", success=True, counter=counter)
        if receipt_replay:
            self._scheduler_mark(
                "idle",
                success=True,
                counter="receipt_replays",
            )
        return True

    def _scheduler_retry_claim(
        self,
        claimed: ClaimedJob,
        error_code: str,
        *,
        queue_full: bool = False,
    ) -> bool:
        scheduler = self._autonomous_scheduler
        if scheduler is None:
            return False
        try:
            scheduler.retry(
                claimed.job_id,
                claimed.lease_token,
                error_code,
            )
        except BaseException as err:
            self._scheduler_record_error(err)
            return False
        self._scheduler_mark(
            "degraded",
            error_code=error_code,
            success=True,
            counter="retried_jobs",
        )
        if queue_full:
            self._scheduler_mark(
                "degraded",
                error_code=error_code,
                counter="queue_full_events",
            )
        return True

    def _scheduler_settle_active_lease(
        self,
        job: Job,
        fallback_state: str,
    ) -> None:
        """Settle a sidecar lease after the service receipt became terminal."""
        if fallback_state not in TERMINAL_JOB_STATES:
            return
        with self._scheduler_lock:
            lease = self._scheduler_lease
        if lease is None or lease.job_id != job.job_id:
            return
        receipt_state, conflict = self._scheduler_receipt_by_identity(
            lease.job_id,
            lease.job_type,
            lease.request_digest,
        )
        if conflict:
            self._scheduler_mark(
                "error",
                error_code="scheduler_receipt_conflict",
                counter="error_events",
            )
            return
        terminal_state = (
            receipt_state
            if receipt_state in TERMINAL_JOB_STATES
            else fallback_state
        )
        # The dispatcher and scheduler sidecar may observe the same durable
        # terminal receipt concurrently.  Detach ownership before touching
        # SQLite so exactly one of them performs (and counts) the transition;
        # restore it only when the durable scheduler call did not close it.
        with self._scheduler_lock:
            if self._scheduler_lease != lease:
                return
            self._scheduler_lease = None
        if not self._scheduler_transition(
            lease.job_id,
            lease.lease_token,
            terminal_state,
            receipt_replay=False,
        ):
            with self._scheduler_lock:
                if self._scheduler_lease is None:
                    self._scheduler_lease = lease
            return

    def _scheduler_handle_existing_receipt(
        self,
        claimed: ClaimedJob,
        job: Job,
    ) -> bool:
        """Adopt or settle an exact service receipt after a crash window."""
        state, conflict = self._scheduler_receipt_by_identity(
            job.job_id,
            job.job_type,
            _job_request_digest(job),
        )
        if conflict:
            self._scheduler_retry_claim(
                claimed,
                "service_receipt_conflict",
            )
            return True
        if state in TERMINAL_JOB_STATES:
            self._scheduler_transition(
                claimed.job_id,
                claimed.lease_token,
                state,
                receipt_replay=True,
            )
            return True
        if state in NONTERMINAL_JOB_STATE_ORDER:
            if not self._scheduler_job_present(job.job_id):
                self._scheduler_retry_claim(
                    claimed,
                    "service_receipt_orphaned",
                )
                return True
            with self._scheduler_lock:
                self._scheduler_lease = _SchedulerLease(
                    job_id=job.job_id,
                    job_type=job.job_type,
                    request_digest=_job_request_digest(job),
                    lease_token=claimed.lease_token,
                )
            self._scheduler_mark("admitted", success=True)
            return True
        return False

    def _scheduler_admit(self, claimed: ClaimedJob) -> bool:
        """Place one claimed intent onto the existing priority/serial queue."""
        if claimed.job_type not in self.job_handlers:
            self._scheduler_retry_claim(
                claimed,
                "service_unknown_job_type",
            )
            return False
        if claimed.job_type not in self.job_effect_contracts:
            self._scheduler_retry_claim(
                claimed,
                "service_effect_contract_missing",
            )
            return False
        job = Job(
            job_id=claimed.job_id,
            job_type=claimed.job_type,
            deadline=claimed.deadline_at,
            payload=claimed.payload,
        )
        if self._scheduler_handle_existing_receipt(claimed, job):
            return False

        with self._admission_lock:
            if not self._accepting or self.stop_event.is_set():
                deferred = "service_stopping"
            elif self.jobs.full():
                deferred = "service_queue_full"
            elif job.job_id in self._seen:
                deferred = "service_receipt_orphaned"
            else:
                deferred = None
                with self._scheduler_lock:
                    if self._scheduler_lease is not None:
                        deferred = "service_scheduler_busy"
                    else:
                        self._scheduler_lease = _SchedulerLease(
                            job_id=job.job_id,
                            job_type=job.job_type,
                            request_digest=_job_request_digest(job),
                            lease_token=claimed.lease_token,
                        )
                if deferred is None:
                    self._queue_seq += 1
                    try:
                        self.jobs.put_nowait(
                            (
                                self._job_priority(job.job_type),
                                self._queue_seq,
                                job,
                            )
                        )
                    except queue.Full:
                        with self._scheduler_lock:
                            self._scheduler_lease = None
                        deferred = "service_queue_full"
                    else:
                        self._seen[job.job_id] = time.time()
                        self._accepted_job_ids.add(job.job_id)
                        self.accepted += 1
        if deferred is not None:
            self._scheduler_retry_claim(
                claimed,
                deferred,
                queue_full=deferred == "service_queue_full",
            )
            return False

        try:
            self._publish_job(
                job,
                "queued",
                queue_depth=self.jobs.qsize(),
            )
            self._publish_status()
        except BaseException as err:
            # The durable scheduler lease remains attached to the queued job.
            # Its terminal path or lease expiry closes this crash window.
            self._scheduler_record_error(err)
            return False
        self._scheduler_mark(
            "admitted",
            success=True,
            counter="admitted_jobs",
        )
        return True

    def _scheduler_tick(self) -> bool:
        """Run one bounded scheduler control-plane tick in its sidecar thread."""
        scheduler = self._autonomous_scheduler
        if scheduler is None or self.stop_event.is_set():
            return False
        try:
            with self._scheduler_lock:
                lease = self._scheduler_lease
            if lease is not None:
                receipt_state, conflict = self._scheduler_receipt_by_identity(
                    lease.job_id,
                    lease.job_type,
                    lease.request_digest,
                )
                if conflict:
                    self._scheduler_mark(
                        "error",
                        error_code="scheduler_receipt_conflict",
                        counter="error_events",
                    )
                    return False
                if receipt_state in TERMINAL_JOB_STATES:
                    with self._scheduler_lock:
                        if self._scheduler_lease != lease:
                            return False
                        self._scheduler_lease = None
                    if not self._scheduler_transition(
                        lease.job_id,
                        lease.lease_token,
                        receipt_state,
                        receipt_replay=True,
                    ):
                        with self._scheduler_lock:
                            if self._scheduler_lease is None:
                                self._scheduler_lease = lease
                    return False
                self._scheduler_mark("admitted", success=True)
                return False

            self._scheduler_mark("planning")
            scheduler.reconcile()
            claimed = scheduler.claim_next(self.worker_id)
            if claimed is None:
                self._scheduler_mark("idle", success=True)
                return False
            self._scheduler_mark(
                "claimed",
                success=True,
                counter="claimed_jobs",
            )
            return self._scheduler_admit(claimed)
        except BaseException as err:
            self._scheduler_record_error(err)
            return False

    def _run_scheduler(self) -> None:
        """Poll only in a sidecar thread so MQTT and dispatch never wait."""
        while not self.stop_event.is_set():
            self._scheduler_tick()
            if self.stop_event.wait(self._scheduler_poll_seconds):
                break

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code != 0:
            with self._admission_lock:
                self._mqtt_connected.clear()
                if self._runtime_role == "worker_first":
                    self._jobs_subscription_active = False
                    self._worker_first_runtime_ready.clear()
                    self._accepting = False
                    if (
                        self._worker_first_runtime_fatal_code is None
                        and not self.stop_event.is_set()
                    ):
                        self._admission_rejection_code = (
                            "worker_first_runtime_not_ready"
                        )
            self._touch_process(
                "mqtt", "error", error="mqtt_connect_refused"
            )
            self._signal_operational_snapshot_runtime(immediate=True)
            _LOGGER.error("MQTT-verbinding geweigerd: %s", reason_code)
            return
        with self._admission_lock:
            self._mqtt_connected.set()
            self._jobs_subscription_active = False
            if self._runtime_role == "worker_first":
                self._mqtt_connection_generation += 1
                self._worker_first_runtime_ready.clear()
                self._accepting = False
                if (
                    self._worker_first_runtime_fatal_code is None
                    and not self.stop_event.is_set()
                ):
                    self._admission_rejection_code = (
                        "worker_first_runtime_not_ready"
                    )
        self._signal_operational_snapshot_runtime(immediate=True)
        if (
            self._runtime_role != "worker_first"
            or self._worker_first_runtime_ready.is_set()
        ):
            self._activate_job_subscription()
        self._touch_process("mqtt", "active")
        self._publish_status()
        if self._runtime_role != "worker_first":
            self._replay_recovered_terminal_statuses()

    def _on_disconnect(
        self,
        client,
        userdata,
        disconnect_flags,
        reason_code,
        properties,
    ) -> None:
        with self._admission_lock:
            self._mqtt_connected.clear()
            self._jobs_subscription_active = False
            if self._runtime_role == "worker_first":
                self._worker_first_runtime_ready.clear()
                self._accepting = False
                if (
                    self._worker_first_runtime_fatal_code is None
                    and not self.stop_event.is_set()
                ):
                    self._admission_rejection_code = (
                        "worker_first_runtime_not_ready"
                    )
        if self.stop_event.is_set():
            self._touch_process("mqtt", "offline")
        else:
            self._touch_process(
                "mqtt", "error", error="mqtt_disconnected"
            )
        self._signal_operational_snapshot_runtime(immediate=True)

    def _on_message(self, client, userdata, message) -> None:
        data: dict | None = None
        job: Job | None = None
        # The network loop may deliver queued QoS1 work immediately after
        # subscribe() returns. Activation holds this same lock through the
        # subscribe + ready transition, so no accepted delivery can fall into
        # the tiny pre-ready interval and be silently acknowledged/lost.
        with self._admission_lock:
            if (
                self._runtime_role == "worker_first"
                and not self._worker_first_runtime_ready.is_set()
            ):
                return
        try:
            data = decode(message.payload)
            if data.get("message_type") != "job_request":
                raise ValueError("Onjuist berichttype")
            job = Job(
                job_id=str(data["job_id"]),
                job_type=str(data["job_type"]),
                deadline=float(data["deadline"]),
                payload=job_payload(data),
            )
            if job.job_type not in self.job_handlers:
                raise ValueError(f"Onbekend taaktype: {job.job_type}")
            if (
                self._worker_first_runtime_mode
                in {RAW_CAPTURE_SHADOW_MODE, WORKER_ONLY_MODE}
                and job.job_type == "ping"
                and job.payload != {}
            ):
                raise ValueError("raw_capture_ping_payload_invalid")
            now = time.time()
            durable_cached = (
                self._receipt_store.load(job.job_id)
                if self._receipt_store is not None
                else None
            )
            with self._admission_lock:
                self._seen = {
                    key: seen_at
                    for key, seen_at in self._seen.items()
                    if now - seen_at < 86400
                }
                with self._process_lock:
                    self._job_status_cache = {
                        key: status
                        for key, status in self._job_status_cache.items()
                        if key in self._seen
                        or now - float(status.get("updated_at", 0.0)) < 86400
                    }
                    cached = dict(
                        self._job_status_cache.get(job.job_id, {})
                    )
                    if (
                        not cached
                        and durable_cached is not None
                        and durable_cached["state"] in TERMINAL_JOB_STATES
                    ):
                        cached = {
                            "job_type": durable_cached["job_type"],
                            "request_digest": durable_cached[
                                "request_digest"
                            ],
                            "state": durable_cached["state"],
                            "values": dict(durable_cached["values"]),
                            "updated_at": durable_cached["updated_at"],
                        }
                        self._job_status_cache[job.job_id] = dict(cached)
                        self._seen[job.job_id] = durable_cached["updated_at"]
                request_digest = _job_request_digest(job)
                if cached and (
                    cached.get("job_type") != job.job_type
                    or cached.get("request_digest") != request_digest
                ):
                    action = "idempotency_conflict"
                elif job.job_id in self._seen:
                    action = "duplicate"
                elif job.deadline <= now:
                    action = "expired"
                elif not self._accepting:
                    self._seen[job.job_id] = now
                    action = "stopping"
                    admission_error = (
                        self._admission_rejection_code or "worker_stopping"
                    )
                else:
                    if self._receipt_store is not None:
                        self._receipt_store.record(
                            job,
                            "queued",
                            {"queue_depth": self.jobs.qsize() + 1},
                            accepted=True,
                        )
                    self._queue_seq += 1
                    self.jobs.put_nowait(
                        (self._job_priority(job.job_type), self._queue_seq, job)
                    )
                    self._seen[job.job_id] = now
                    self._accepted_job_ids.add(job.job_id)
                    self.accepted += 1
                    action = "queued"

            if action == "duplicate":
                self._publish_job(
                    job,
                    str(cached.get("state") or "queued"),
                    _replay=True,
                    **dict(cached.get("values") or {}),
                )
                return
            if action == "idempotency_conflict":
                self._record_dispatch_failure("idempotency_conflict")
                self._publish_job(
                    job,
                    "failed",
                    _replay=True,
                    error="idempotency_conflict",
                )
                self._publish_runtime_status()
                return
            if action == "expired":
                self._publish_job(job, "expired")
                return
            if action == "stopping":
                self._publish_job(
                    job,
                    "cancelled",
                    error=admission_error,
                )
                return
            self._publish_job(job, "queued", queue_depth=self.jobs.qsize())
            self._publish_status()
        except queue.Full:
            self._record_dispatch_failure("queue_full")
            if job is not None:
                with self._admission_lock:
                    self._seen[job.job_id] = time.time()
                self._publish_job(job, "failed", error="queue_full")
            self._publish_runtime_status()
            _LOGGER.warning(
                "Workerwachtrij vol; taak %s geweigerd",
                job.job_id if job is not None else "onbekend",
            )
        except (EOFError, KeyError, OSError, TypeError, ValueError) as err:
            self._record_dispatch_failure("invalid_request")
            failed_job = job or self._recover_job_reference(
                message.payload, data
            )
            if failed_job is not None:
                with self._admission_lock:
                    self._seen[failed_job.job_id] = time.time()
                self._publish_job(
                    failed_job,
                    "failed",
                    error="invalid_request",
                    error_detail=str(err),
                )
            self._publish_runtime_status()
            _LOGGER.warning("Ongeldig workerbericht: %s", err)

    def _execute_job(self, job: Job) -> _ExecutionOutcome:
        if self.stop_event.is_set():
            return _ExecutionOutcome(
                "cancelled",
                {"error": "worker_stopping"},
            )
        if job.deadline <= time.time():
            return _ExecutionOutcome(
                "expired",
                {"error": "deadline_exceeded"},
            )
        try:
            execution_job = self._prepare_execution_job(job)
        except TimeoutError:
            return _ExecutionOutcome(
                "expired",
                {"error": "effect_preparation_deadline_exceeded"},
            )
        except BaseException as err:
            # Credential providers can include sensitive response bodies.
            # Preserve the error class, never its raw text.
            return _ExecutionOutcome(
                "failed",
                {
                    "error": "effect_preparation_failed",
                    "error_class": type(err).__name__,
                },
            )
        if job.deadline <= time.time():
            return _ExecutionOutcome(
                "expired",
                {
                    "error": "deadline_exceeded_after_effect_preparation",
                },
            )
        if self.stop_event.is_set():
            return _ExecutionOutcome(
                "cancelled",
                {"error": "worker_stopping_after_effect_preparation"},
            )
        if self._executor is not None:
            return self._executor.execute(execution_job, self.stop_event)
        secrets = _job_secret_values(execution_job)
        try:
            handler = self.job_handlers[execution_job.job_type]
            if not callable(handler):
                raise TypeError("Workerhandler is niet aanroepbaar")
            result = handler(execution_job)
            if self._worker_first_runtime_mode in {
                RAW_CAPTURE_SHADOW_MODE,
                WORKER_ONLY_MODE,
            }:
                if execution_job.job_type == "ping":
                    result = _raw_capture_ping_result(result)
                elif execution_job.job_type in RAW_CAPTURE_CONTROL_JOB_TYPES:
                    result = _raw_capture_control_result(
                        execution_job.job_type,
                        result,
                    )
        except BaseException as err:
            return _ExecutionOutcome(
                "failed",
                {
                    "error": type(err).__name__,
                    "error_detail": _redact_text(str(err), secrets),
                },
            )
        if time.time() > job.deadline:
            return _ExecutionOutcome(
                "expired",
                {"error": "deadline_exceeded"},
            )
        if self.stop_event.is_set():
            return _ExecutionOutcome(
                "cancelled",
                {"error": "worker_stopping"},
            )
        return _ExecutionOutcome(
            "completed",
            {
                "result": _redact_external_value(
                    result,
                    secrets=secrets,
                )
            },
        )

    def _prepare_execution_job(self, job: Job) -> Job:
        """Complete every non-kill-safe precursor before child dispatch."""

        contract = self.job_effect_contracts[job.job_type]
        if contract.preparation == "none":
            return job
        if contract.preparation == "parent_netatmo_oauth_broker_v1":
            netatmo = importlib.import_module("storm_tracker_worker.netatmo")
            token = netatmo.broker_netatmo_access_token(
                job.payload,
                deadline=job.deadline,
            )
            return replace(
                job,
                execution_context={
                    "effect_contract_id": contract.contract_id,
                    "netatmo_access_token": token,
                },
            )
        raise RuntimeError("onbekende effectpreparation")

    def _finalize_job(
        self,
        job: Job,
        outcome: _ExecutionOutcome,
    ) -> None:
        published = False
        try:
            published = self._publish_job(
                job,
                outcome.state,
                **outcome.values,
            )
        finally:
            self._scheduler_settle_active_lease(job, outcome.state)
        if not published:
            return
        error = str(outcome.values.get("error") or "")
        with self._process_lock:
            handler_health = self.processes[job.job_type]
            if outcome.state == "completed":
                self.completed += 1
                handler_health.completed_jobs += 1
            elif outcome.state == "failed":
                self.failed += 1
                handler_health.failed_jobs += 1
        if outcome.state == "failed":
            self._touch_process(
                job.job_type,
                "error",
                error=error or "worker_job_failed",
            )
            _LOGGER.error(
                "Workerjob %s mislukt: %s",
                job.job_id,
                error or "worker_job_failed",
            )
        else:
            self._touch_process(job.job_type, "idle")

    def _cancel_queued_jobs(self) -> None:
        """Give every accepted but not started job one terminal status."""
        while True:
            try:
                _, _, job = self.jobs.get_nowait()
            except queue.Empty:
                return
            try:
                try:
                    self._publish_job(
                        job,
                        "cancelled",
                        error="worker_stopping",
                    )
                except BaseException:
                    if self._runtime_role == "worker_first":
                        self._fail_worker_first_runtime(
                            "worker_first_dispatcher_failed"
                        )
                    else:
                        _LOGGER.exception(
                            "Kon annulering voor workerjob %s niet publiceren",
                            job.job_id,
                        )
                finally:
                    self._scheduler_settle_active_lease(
                        job,
                        "cancelled",
                    )
            finally:
                self.jobs.task_done()

    def _run_jobs(self) -> None:
        try:
            while not self.stop_event.is_set():
                try:
                    queued = self.jobs.get(timeout=0.1)
                    # Backward-compatible direct queue injection is retained
                    # for recovery tooling and deterministic service tests.
                    if isinstance(queued, Job):
                        job = queued
                    else:
                        _, _, job = queued
                except queue.Empty:
                    continue
                if self.stop_event.is_set():
                    try:
                        self._publish_job(
                            job,
                            "cancelled",
                            error="worker_stopping",
                        )
                    finally:
                        self._scheduler_settle_active_lease(
                            job,
                            "cancelled",
                        )
                        self.jobs.task_done()
                    break
                with self._process_lock:
                    self.active_job = job
                try:
                    self._touch_process(
                        "dispatcher",
                        "busy",
                        job_id=job.job_id,
                    )
                    self._touch_process(
                        job.job_type,
                        "working",
                        job_id=job.job_id,
                    )
                    self._publish_job(job, "running")
                    self._publish_status("busy")
                    self._finalize_job(job, self._execute_job(job))
                except BaseException as err:
                    if self._runtime_role == "worker_first":
                        self._fail_worker_first_runtime(
                            "worker_first_dispatcher_failed"
                        )
                    else:
                        _LOGGER.exception(
                            "Onverwachte dispatcherfout voor workerjob %s",
                            job.job_id,
                        )
                    try:
                        self._finalize_job(
                            job,
                            _ExecutionOutcome(
                                "failed",
                                {
                                    "error": "worker_dispatch_error",
                                    **(
                                        {}
                                        if self._runtime_role
                                        == "worker_first"
                                        else {"error_detail": str(err)}
                                    ),
                                },
                            ),
                        )
                    except BaseException:
                        if self._runtime_role == "worker_first":
                            self._fail_worker_first_runtime(
                                "worker_first_dispatcher_failed"
                            )
                        else:
                            _LOGGER.exception(
                                "Kon terminale fout voor workerjob %s "
                                "niet publiceren",
                                job.job_id,
                            )
                finally:
                    with self._process_lock:
                        self.active_job = None
                    self._touch_process("dispatcher", "idle")
                    self.jobs.task_done()
                    self._publish_status()
        finally:
            self._cancel_queued_jobs()
            if self._executor is not None:
                self._executor.shutdown(force=self.stop_event.is_set())

    def _run_worker_first_jobs(self) -> None:
        """Turn every unexpected canary-dispatcher exit into a fixed fatal."""
        try:
            self._run_jobs()
        except BaseException:
            if not self.stop_event.is_set():
                self._fail_worker_first_runtime(
                    "worker_first_dispatcher_failed"
                )
            return
        if not self.stop_event.is_set():
            self._fail_worker_first_runtime(
                "worker_first_dispatcher_stopped"
            )

    def run(self) -> None:
        scheduler_thread = (
            threading.Thread(
                target=self._run_scheduler,
                name="storm-tracker-scheduler",
                daemon=False,
            )
            if self._autonomous_scheduler is not None
            else None
        )
        operational_snapshot_thread = None
        scheduler_started = False
        mqtt_loop_started = False
        try:
            if self._runtime_role != "worker_first":
                self._start_job_dispatcher_if_needed()
            else:
                # Own the canary lifecycle before the transport loop starts.
                # If MQTT startup then fails, normal cleanup can still ask the
                # runtime to release its ownership fence before the hard-exit
                # watchdog becomes the final boundary.
                operational_snapshot_thread = (
                    self._start_operational_snapshot_runtime()
                )
            try:
                self.client.loop_start()
            except BaseException:
                if self._runtime_role == "worker_first":
                    self._fail_worker_first_runtime(
                        "worker_first_mqtt_loop_start_failed"
                    )
                    raise _WorkerFirstRuntimeFatalError(
                        "worker_first_mqtt_loop_start_failed"
                    ) from None
                raise
            mqtt_loop_started = True
            if self._runtime_role != "worker_first":
                operational_snapshot_thread = (
                    self._start_operational_snapshot_runtime()
                )
            if scheduler_thread is not None:
                scheduler_thread.start()
                scheduler_started = True
            while not self.stop_event.wait(self.heartbeat_seconds):
                if (
                    self._runtime_role == "worker_first"
                    and self._worker_first_runtime_ready.is_set()
                    and (
                        self._job_thread is None
                        or not self._job_thread.is_alive()
                    )
                ):
                    self._fail_worker_first_runtime(
                        "worker_first_dispatcher_stopped"
                    )
                    raise _WorkerFirstRuntimeFatalError(
                        "worker_first_dispatcher_stopped"
                    )
                self._touch_process("mqtt", "active")
                if self.active_job is None:
                    self._touch_process("dispatcher", "idle")
                else:
                    self._touch_process(
                        "dispatcher", "busy", job_id=self.active_job.job_id
                    )
                    self._touch_process(
                        self.active_job.job_type,
                        "working",
                        job_id=self.active_job.job_id,
                    )
                for process_id in self.job_handlers:
                    if (
                        self.active_job is None
                        or self.active_job.job_type != process_id
                    ):
                        self._heartbeat_process(process_id)
                self._publish_status("busy" if self.active_job else "online")
        finally:
            self.stop()
            if scheduler_thread is not None and scheduler_started:
                scheduler_thread.join(timeout=5)
                if scheduler_thread.is_alive():
                    _LOGGER.critical(
                        "Autonome scheduler stopte niet binnen de "
                        "shutdowntermijn"
                    )
            job_thread = self._job_thread
            if job_thread is not None:
                job_thread.join(timeout=5)
            if (
                job_thread is not None
                and job_thread.is_alive()
                and self._executor is not None
            ):
                self._executor.shutdown(force=True)
                job_thread.join(timeout=2)
            if job_thread is not None and job_thread.is_alive():
                _LOGGER.critical(
                    "Workerdispatcher stopte niet binnen de shutdowntermijn"
                )
            self._cancel_queued_jobs()
            if operational_snapshot_thread is not None:
                if self._runtime_role == "worker_first":
                    operational_snapshot_thread.join(
                        timeout=OPERATIONAL_SNAPSHOT_THREAD_JOIN_SECONDS
                    )
                    if operational_snapshot_thread.is_alive():
                        self._fail_worker_first_runtime(
                            "worker_first_shutdown_timeout"
                        )
                else:
                    operational_snapshot_thread.join(
                        timeout=OPERATIONAL_SNAPSHOT_THREAD_JOIN_SECONDS
                    )
                if operational_snapshot_thread.is_alive():
                    if self._runtime_role == "worker_first":
                        _LOGGER.critical(
                            "Worker-first runtime overschreed de harde "
                            "shutdowntermijn; proces eindigt fail-closed"
                        )
                    else:
                        operational_snapshot_thread.join(
                            timeout=0
                        )
                    self._record_operational_snapshot_error(
                        "sidecar_shutdown_timeout"
                    )
                    _LOGGER.critical(
                        "Operational snapshot sidecar stopte niet binnen de "
                        "shutdowntermijn"
                    )
            self._mqtt_connected.clear()
            if self._runtime_role == "worker_first":
                try:
                    self._publish_status("offline")
                except BaseException:
                    _LOGGER.error(
                        "Kon finale canary-status niet publiceren"
                    )
                try:
                    self.client.disconnect()
                except BaseException:
                    _LOGGER.error(
                        "Kon canary-MQTT niet schoon verbreken"
                    )
                if mqtt_loop_started:
                    try:
                        self.client.loop_stop()
                    except BaseException:
                        _LOGGER.error(
                            "Kon canary-MQTT-loop niet schoon stoppen"
                        )
            else:
                # Preserve the legacy sequential cleanup contract, including
                # propagation of the first transport exception.
                self._publish_status("offline")
                self.client.disconnect()
                if mqtt_loop_started:
                    self.client.loop_stop()
        if self._worker_first_runtime_fatal_code is not None:
            raise _WorkerFirstRuntimeFatalError(
                self._worker_first_runtime_fatal_code
            )

    def stop(self, *_args) -> None:
        with self._admission_lock:
            self._accepting = False
            if self._worker_first_runtime_fatal_code is None:
                self._admission_rejection_code = "worker_stopping"
        self.stop_event.set()
        self._signal_operational_snapshot_runtime(immediate=True)


def _main_runtime_kwargs(
    environment: Mapping[str, str] | None = None,
) -> tuple[WorkerFirstServiceActivationPlan, dict[str, Any]]:
    """Select exactly one runtime root without fallback or external effects."""

    plan = load_worker_first_service_activation(environment)
    if (
        getattr(plan, "raw_capture_selected", False)
        or getattr(plan, "worker_only_selected", False)
    ):
        config = plan.composition_config
        config_sha256 = plan.config_sha256
        authority_sha256 = plan.authority_sha256
        if (
            config is None
            or config_sha256 is None
            or authority_sha256 is None
        ):
            raise RuntimeError("raw_capture_activation_plan_incomplete")
        from .acquisition_activation import (
            ACTIVATION_RULES,
            EnvironmentSecretResolver,
        )

        allowed_workload_ids = tuple(sorted(
            workload_id
            for workload_id, rule in ACTIVATION_RULES.items()
            if rule.executable
        ))
        if not allowed_workload_ids:
            raise RuntimeError("raw_capture_activation_matrix_empty")
        raw_environment = os.environ if environment is None else environment
        secret_resolver = EnvironmentSecretResolver(raw_environment)
        netatmo_access_token_broker = (
            _broker_raw_capture_netatmo_access_token
            if _netatmo_oauth_seeds_configured(raw_environment)
            else None
        )
        eumetsat_access_token_broker = _eumetsat_access_token_broker(
            raw_environment
        )
        runtime_holder: dict[str, Any] = {}

        def _raw_control_handler(job: Job) -> dict[str, Any]:
            runtime = runtime_holder.get("runtime")
            dispatcher = getattr(runtime, "handle_control_job", None)
            if not callable(dispatcher):
                raise RuntimeError("raw_capture_control_runtime_unavailable")
            return dispatcher(job.job_type, job.payload)

        def _raw_capture_runtime_factory(
            *,
            publisher: Callable[[bytes], Any],
            status_supplier: Callable[[], Mapping[str, Any]],
        ) -> Any:
            builder = (
                _build_worker_only_service
                if getattr(plan, "worker_only_selected", False)
                else _build_raw_capture_service
            )
            callback_kwargs = (
                {
                    "publisher": publisher,
                    "status_supplier": status_supplier,
                }
                if getattr(plan, "worker_only_selected", False)
                else {}
            )
            runtime = builder(
                config,
                expected_config_sha256=config_sha256,
                expected_authority_sha256=authority_sha256,
                secret_resolver=secret_resolver,
                executor_factory=_raw_capture_executor_factory,
                netatmo_access_token_broker=netatmo_access_token_broker,
                eumetsat_access_token_broker=(
                    eumetsat_access_token_broker
                ),
                blitzortung_access_retention_authority=(
                    (
                        raw_environment.get(
                            RAW_CAPTURE_BLITZORTUNG_AUTHORITY_ENV
                        )
                        or None
                    )
                    if config.blitzortung_enabled
                    else None
                ),
                clock=time.time,
                **callback_kwargs,
            )
            if not callable(getattr(runtime, "handle_control_job", None)):
                raise TypeError("raw_capture_runtime_mist_control_dispatcher")
            runtime_holder["runtime"] = runtime
            return runtime

        return plan, {
            "job_handlers": {
                job_type: _raw_control_handler
                for job_type in RAW_CAPTURE_CONTROL_JOB_TYPES
            },
            "job_effect_contracts": {
                "desired_state_apply": _TRANSACTIONAL_DESIRED_STATE_EFFECT,
                "desired_state_claim": _TRANSACTIONAL_DESIRED_STATE_EFFECT,
                "desired_state_snapshot": _IDEMPOTENT_READ_EFFECT,
            },
            "process_isolation": False,
            "require_effect_contracts": True,
            "worker_first_allowed_workload_ids": allowed_workload_ids,
            "worker_first_runtime_factory": _raw_capture_runtime_factory,
            "worker_first_runtime_mode": (
                WORKER_ONLY_MODE
                if getattr(plan, "worker_only_selected", False)
                else RAW_CAPTURE_SHADOW_MODE
            ),
        }
    if not plan.worker_first_selected:
        return plan, {
            "job_handlers": PRODUCTION_JOB_HANDLERS,
            "job_effect_contracts": PRODUCTION_JOB_EFFECT_CONTRACTS,
            "require_effect_contracts": True,
            "operational_snapshot_runtime_factory": (
                OperationalSnapshotRuntime
            ),
        }
    config = plan.composition_config
    config_sha256 = plan.config_sha256
    authority_sha256 = plan.authority_sha256
    if (
        config is None
        or config_sha256 is None
        or authority_sha256 is None
    ):
        raise RuntimeError("worker_first_activation_plan_incomplete")

    def _worker_first_runtime_factory(
        *,
        publisher: Callable[[bytes], Any],
        status_supplier: Callable[[], Mapping[str, Any]],
    ) -> Any:
        return _build_worker_first_service_canary(
            config,
            expected_config_sha256=config_sha256,
            expected_authority_sha256=authority_sha256,
            publisher=publisher,
            status_supplier=status_supplier,
        )

    return plan, {
        "process_isolation": False,
        "require_effect_contracts": True,
        "worker_first_runtime_factory": _worker_first_runtime_factory,
    }


def _build_worker_first_service_canary(*args: Any, **kwargs: Any) -> Any:
    """Import the heavy canary graph only after explicit mode selection."""

    from .worker_first_service_composition import (
        build_worker_first_service_canary,
    )

    return build_worker_first_service_canary(*args, **kwargs)


def _build_raw_capture_service(*args: Any, **kwargs: Any) -> Any:
    """Import the all-source raw-capture graph only in its explicit mode."""

    from .raw_capture_service_composition import build_raw_capture_service

    return build_raw_capture_service(*args, **kwargs)


def _build_worker_only_service(*args: Any, **kwargs: Any) -> Any:
    """Import the all-source production output graph only in its explicit mode."""

    from .worker_only_service_runtime import build_worker_only_service

    return build_worker_only_service(*args, **kwargs)


def main() -> None:
    logging.basicConfig(
        level=os.getenv("STV3_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    _activation_plan, runtime_kwargs = _main_runtime_kwargs()
    try:
        service = WorkerService(
            host=os.environ["STV3_MQTT_HOST"],
            port=int(os.getenv("STV3_MQTT_PORT", "1883")),
            username=os.getenv("STV3_MQTT_USERNAME"),
            password=os.getenv("STV3_MQTT_PASSWORD"),
            tls=os.getenv("STV3_MQTT_TLS", "false").lower() == "true",
            topic_root=os.getenv("STV3_MQTT_TOPIC_ROOT", DEFAULT_TOPIC_ROOT),
            queue_size=int(os.getenv("STV3_QUEUE_SIZE", "16")),
            heartbeat_seconds=float(os.getenv("STV3_HEARTBEAT_SECONDS", "10")),
            provider_cycle_seconds=float(
                os.getenv("STV3_PROVIDER_CYCLE_SECONDS", "60")
            ),
            compute_profile=os.getenv("STV3_COMPUTE_PROFILE", "shared_ha"),
            cpu_limit_percent=int(
                os.getenv("STV3_CPU_LIMIT_PERCENT", "25")
            ),
            worker_nice=int(os.getenv("STV3_WORKER_NICE", "15")),
            cpu_threads=int(os.getenv("STV3_CPU_THREADS", "2")),
            receipt_path=(
                None
                if getattr(
                    _activation_plan,
                    "isolated_runtime_selected",
                    _activation_plan.worker_first_selected,
                )
                else os.getenv(
                    "STV3_JOB_RECEIPT_DB",
                    "/data/storm_tracker_worker_receipts.sqlite3",
                )
            ),
            **runtime_kwargs,
        )
    except _WorkerFirstRuntimeFatalError:
        _hard_exit_worker_first_process()
    signal.signal(signal.SIGTERM, service.stop)
    signal.signal(signal.SIGINT, service.stop)
    _run_service_main(service)


def _run_service_main(service: WorkerService) -> None:
    """Terminate a fatally wedged canary without waiting for Python executors."""
    fatal_event = getattr(service, "_worker_first_runtime_fatal_event", None)
    runtime_role = getattr(service, "_runtime_role", None)
    watchdog_done = threading.Event()
    watchdog = None
    if runtime_role == "worker_first" and isinstance(
        fatal_event, threading.Event
    ):
        try:
            watchdog_seconds = float(
                service._worker_first_fatal_watchdog_seconds
            )
        except (AttributeError, TypeError, ValueError):
            watchdog_seconds = WORKER_FIRST_FATAL_WATCHDOG_SECONDS

        def _hard_exit_after_fatal_timeout() -> None:
            while not fatal_event.wait(0.05):
                if watchdog_done.is_set():
                    return
            if watchdog_done.is_set():
                return
            if not watchdog_done.wait(max(0.01, watchdog_seconds)):
                os._exit(WORKER_FIRST_FATAL_EXIT_CODE)

        watchdog = threading.Thread(
            target=_hard_exit_after_fatal_timeout,
            name="storm-tracker-worker-first-fatal-watchdog",
            daemon=True,
        )
        try:
            watchdog.start()
        except BaseException:
            os._exit(WORKER_FIRST_FATAL_EXIT_CODE)
    try:
        service.run()
    except _WorkerFirstRuntimeFatalError:
        # Never include the original exception: it may contain private runtime
        # detail. A hard process boundary is required because Python's executor
        # exit hook otherwise waits indefinitely for non-cooperative work.
        _hard_exit_worker_first_process()
    except BaseException:
        if fatal_event is not None and fatal_event.is_set():
            os._exit(WORKER_FIRST_FATAL_EXIT_CODE)
        raise
    else:
        if fatal_event is not None and fatal_event.is_set():
            os._exit(WORKER_FIRST_FATAL_EXIT_CODE)
    finally:
        watchdog_done.set()


def _hard_exit_worker_first_process() -> None:
    """Exit with one fixed code without exposing private exception details."""

    try:
        _LOGGER.critical("Worker-first runtime eindigt proces fail-closed")
    finally:
        os._exit(WORKER_FIRST_FATAL_EXIT_CODE)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
