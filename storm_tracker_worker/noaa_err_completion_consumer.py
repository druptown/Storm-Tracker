"""Crash-durable, hard-process NOAA ERR reference completion consumer.

This module consumes only the manifest-reference event emitted by
``terminal_raw_completion_router`` for ``satellite.noaa.err``.  It derives a
closed geographic window from the exact desired-state acquisition regions,
executes ``noaa_err_quantitative.decode_noaa_err_region`` in one spawn-only
CPU/RSS/wall bounded child, and commits a payload-free output receipt before a
lifecycle acknowledgement can occur.

The module is deliberately absent from live service composition.  Every output
remains a factless, unadmitted shadow candidate.
"""

from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass, field
import errno
import hashlib
import hmac
import json
import math
import multiprocessing
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import threading
import time
from typing import Any, Callable, Iterator, Literal, Mapping
from urllib.parse import quote

from . import noaa_err_native as native
from . import noaa_err_quantitative as quantitative
from .acquisition_evidence_lifecycle import (
    CONSUMPTION_RECEIPT_SCHEMA,
    AcquisitionEvidenceLifecycleStore,
    CompletionLease,
    ConsumptionReceipt,
    desired_authority_proof,
)
from .acquisition_scheduler_bridge import reconstruct_acquisition_job_for_slot
from .canonical_evidence_store import CanonicalEvidenceError, CanonicalEvidenceStore
from .desired_state import DesiredState, DesiredStateStore
from .hard_decoder_isolation import (
    DecoderEffectContract,
    DecoderProcessResult,
    DecoderResourceLimits,
    HardDecoderExecutor,
)
from .noaa_err_quantitative import (
    DECODER_CONFIG_SHA256,
    DECODER_IDENTITY,
    DECODER_VERSION,
    NoaaErrArtifactCapacityError,
    NoaaErrArtifactError,
    NoaaErrArtifactRef,
    NoaaErrDecodeDeadlineError,
    NoaaErrDerivedArtifactStore,
    NoaaErrRegion,
    decode_noaa_err_region,
)
from .raw_artifacts import (
    RawArtifactCapacityError,
    RawArtifactError,
    RawArtifactLimits,
    RawArtifactStore,
)
from .terminal_raw_completion_router import (
    REFERENCE_PAYLOAD_SCHEMA,
    REFERENCE_PROOF_SCHEMA,
    TERMINAL_RAW_CONSUMER_CATALOG,
)
from .worker_acquisition_plan import (
    EARTH_RADIUS_KM,
    AcquisitionJob,
    AcquisitionPlan,
    build_worker_acquisition_plan,
    validate_worker_acquisition_job,
    validate_worker_acquisition_plan,
)
from .worker_first_runtime import PipelineOutcome


WORKLOAD_ID = "satellite.noaa.err"
CONSUMER_ID = "noaa-err-quantitative-decoder-reference-v1"
STREAM_ID = WORKLOAD_ID
OUTPUT_DISPOSITION = "withheld"

REGION_POLICY_SCHEMA = "stv3-noaa-err-region-policy-v1"
REGION_AUTHORITY_SCHEMA = "stv3-noaa-err-target-region-authority-v1"
OUTPUT_DOCUMENT_SCHEMA = "stv3-noaa-err-completion-output-v1"
OUTPUT_RECEIPT_SCHEMA = "stv3-noaa-err-completion-output-receipt-v1"
PROCESS_RECEIPT_SCHEMA = "stv3-noaa-err-completion-process-receipt-v1"
OUTPUT_STORE_CONTRACT = "stv3-noaa-err-completion-output-store-v2"
LEGACY_OUTPUT_STORE_CONTRACT = "stv3-noaa-err-completion-output-store-v1"
OUTPUT_STORE_SCHEMA_VERSION = 2
LEGACY_OUTPUT_STORE_SCHEMA_VERSION = 1
OUTPUT_STORE_IDENTITY_SCHEMA = "stv3-noaa-err-completion-output-store-identity-v1"
PROCESS_AUTHORITY_SCHEMA = "stv3-noaa-err-hard-process-authority-v1"
PROCESS_REQUEST_SCHEMA = "stv3-noaa-err-hard-process-request-v1"
PROCESS_CHILD_RESULT_SCHEMA = "stv3-noaa-err-hard-process-child-result-v1"
PROCESS_RESULT_SCHEMA = "stv3-noaa-err-hard-process-parent-result-v1"
PENDING_ACK_SCHEMA = "stv3-noaa-err-completion-pending-ack-v1"
PROCESS_PURPOSE_DECODE = "decode_or_reconcile"
PROCESS_PURPOSE_FINAL_REPROOF = "final_authority_reproof"
PROCESS_TARGET = (
    "storm_tracker_worker.noaa_err_completion_consumer:" "_noaa_err_completion_child_entry"
)

PROCESS_EFFECT_CONTRACT = DecoderEffectContract(
    contract_id="noaa-err-completion.transactional-receipts.v1",
    hard_kill_safe=True,
    durable_effects="transactional",
    spawns_descendants=False,
)

DEFAULT_LEASE_SECONDS = 180.0
MAX_LEASE_SECONDS = 3600.0
MAX_DIAGNOSTIC_BYTES = 4096

_SHA = re.compile(r"^[0-9a-f]{64}$")
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_EVENT = re.compile(r"^completion-[0-9a-f]{64}$")
_DECODER_JOB = re.compile(r"^decoder-job-[0-9a-f]{64}$")
_DECODER_ATTEMPT = re.compile(r"^decoder-attempt-[0-9a-f]{64}$")
_DECODER_RUN = re.compile(r"^run-[0-9a-f]{64}$")
_DECODER_COMMIT = re.compile(r"^commit-[0-9a-f]{64}$")
_FORBIDDEN_OUTPUT_KEY = re.compile(
    r"(?:artifact[_-]?path|authorization|cookie|password|passwd|credential|"
    r"secret|api[_-]?key|access[_-]?token|refresh[_-]?token|private[_-]?key|"
    r"url|uri|raw[_-]?payload)",
    re.IGNORECASE,
)


class NoaaErrCompletionError(RuntimeError):
    """Base error for the closed NOAA completion consumer."""


class NoaaErrCompletionIntegrityError(NoaaErrCompletionError):
    """A manifest, event, desired, region or durable output binding differs."""


class NoaaErrCompletionConflict(NoaaErrCompletionError):
    """A stable event identity was reused for different immutable output."""


class NoaaErrCompletionCapacityError(NoaaErrCompletionError):
    """A configured hard storage/process/region bound was reached."""


class NoaaErrCompletionDeadlineError(NoaaErrCompletionError):
    """The lifecycle or hard-process deadline expired."""


class NoaaErrCompletionCancelled(NoaaErrCompletionError):
    """The owning coordinator cancelled the consumer lane."""


class NoaaErrCompletionProcessError(NoaaErrCompletionError):
    """The hard child could not prove a durable exact terminal."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as error:
        raise NoaaErrCompletionIntegrityError(
            "NOAA completion value is not canonical JSON"
        ) from error


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise NoaaErrCompletionIntegrityError(f"{label} is not SHA-256")
    return value


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise NoaaErrCompletionIntegrityError(f"{label} is invalid")
    return value


def _timestamp(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise NoaaErrCompletionIntegrityError(f"{label} is invalid")
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise NoaaErrCompletionIntegrityError(f"{label} is invalid")
    return result


def _coordinate(
    value: object,
    label: str,
    *,
    minimum: float,
    maximum: float,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise NoaaErrCompletionIntegrityError(f"{label} is invalid")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise NoaaErrCompletionIntegrityError(f"{label} is invalid")
    return result


def _exact(
    value: object,
    keys: set[str] | frozenset[str],
    label: str,
) -> Mapping[str, Any]:
    if (
        not isinstance(value, Mapping)
        or any(not isinstance(key, str) for key in value)
        or set(value) != set(keys)
    ):
        raise NoaaErrCompletionIntegrityError(f"{label} schema differs")
    return value


def _assert_output_safe(value: Any, *, depth: int = 0) -> None:
    if depth > 20:
        raise NoaaErrCompletionCapacityError("output nesting exceeds bound")
    if isinstance(value, Mapping):
        if len(value) > 128:
            raise NoaaErrCompletionCapacityError("output object exceeds bound")
        for key, item in value.items():
            if not isinstance(key, str) or _FORBIDDEN_OUTPUT_KEY.search(key):
                raise NoaaErrCompletionIntegrityError("output contains a private capability key")
            _assert_output_safe(item, depth=depth + 1)
        return
    if isinstance(value, (list, tuple)):
        if len(value) > 256:
            raise NoaaErrCompletionCapacityError("output sequence exceeds bound")
        for item in value:
            _assert_output_safe(item, depth=depth + 1)
        return
    if isinstance(value, str):
        if len(value.encode("utf-8")) > 4096:
            raise NoaaErrCompletionCapacityError("output text exceeds bound")
        return
    if value is None or isinstance(value, (bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise NoaaErrCompletionIntegrityError("output number is not finite")
        return
    raise NoaaErrCompletionIntegrityError("output value type is unsupported")


@dataclass(frozen=True, slots=True)
class NoaaErrRegionPolicy:
    """Bounded desired-state-to-GLB-window derivation policy."""

    max_regions: int = 8
    max_targets: int = 64
    max_radius_km: float = 500.0
    max_window_pixels: int = quantitative.MAX_REGION_PIXELS
    include_observation_radius: bool = True
    algorithm: str = "spherical_cap_union_nonwrapping_v1"

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_regions, bool)
            or not isinstance(self.max_regions, int)
            or not 1 <= self.max_regions <= 64
            or isinstance(self.max_targets, bool)
            or not isinstance(self.max_targets, int)
            or not 1 <= self.max_targets <= 512
            or isinstance(self.max_window_pixels, bool)
            or not isinstance(self.max_window_pixels, int)
            or not 1 <= self.max_window_pixels <= quantitative.MAX_REGION_PIXELS
            or isinstance(self.max_radius_km, bool)
            or not isinstance(self.max_radius_km, (int, float))
            or not math.isfinite(float(self.max_radius_km))
            or not 1.0 <= float(self.max_radius_km) <= 2000.0
            or type(self.include_observation_radius) is not bool
            or self.include_observation_radius is not True
            or self.algorithm != "spherical_cap_union_nonwrapping_v1"
        ):
            raise ValueError("NOAA region policy is outside its closed bounds")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "algorithm": self.algorithm,
            "include_observation_radius": self.include_observation_radius,
            "max_radius_km": float(self.max_radius_km),
            "max_regions": self.max_regions,
            "max_targets": self.max_targets,
            "max_window_pixels": self.max_window_pixels,
            "schema": REGION_POLICY_SCHEMA,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.to_mapping())


@dataclass(frozen=True, slots=True)
class NoaaErrRegionAuthority:
    desired_state_sha256: str
    plan_sha256: str
    acquisition_job_id: str
    acquisition_job_sha256: str
    policy_sha256: str
    region_ids: tuple[str, ...]
    region_sha256s: tuple[str, ...]
    target_ids: tuple[str, ...]
    minimum_latitude: float
    minimum_longitude: float
    maximum_latitude: float
    maximum_longitude: float
    row_start: int
    row_stop: int
    column_start: int
    column_stop: int
    grid_definition_sha256: str
    coverage_definition_sha256: str

    def __post_init__(self) -> None:
        try:
            requested = self.region
            window = quantitative._window_for_region(requested)  # noqa: SLF001
        except (TypeError, ValueError) as error:
            raise NoaaErrCompletionIntegrityError(
                "NOAA region authority is outside the closed GLB grid"
            ) from error
        if (
            not isinstance(self.region_ids, tuple)
            or not isinstance(self.region_sha256s, tuple)
            or not isinstance(self.target_ids, tuple)
            or not self.region_ids
            or not self.target_ids
            or len(self.region_ids) != len(self.region_sha256s)
            or len(set(self.region_ids)) != len(self.region_ids)
            or len(set(self.target_ids)) != len(self.target_ids)
            or any(
                not isinstance(item, str) or _ID.fullmatch(item) is None for item in self.region_ids
            )
            or any(
                not isinstance(item, str) or _SHA.fullmatch(item) is None
                for item in self.region_sha256s
            )
            or any(
                not isinstance(item, str) or _ID.fullmatch(item) is None for item in self.target_ids
            )
            or not isinstance(self.desired_state_sha256, str)
            or _SHA.fullmatch(self.desired_state_sha256) is None
            or not isinstance(self.plan_sha256, str)
            or _SHA.fullmatch(self.plan_sha256) is None
            or not isinstance(self.acquisition_job_id, str)
            or _ID.fullmatch(self.acquisition_job_id) is None
            or not isinstance(self.acquisition_job_sha256, str)
            or _SHA.fullmatch(self.acquisition_job_sha256) is None
            or not isinstance(self.policy_sha256, str)
            or _SHA.fullmatch(self.policy_sha256) is None
            or not isinstance(self.grid_definition_sha256, str)
            or _SHA.fullmatch(self.grid_definition_sha256) is None
            or not isinstance(self.coverage_definition_sha256, str)
            or _SHA.fullmatch(self.coverage_definition_sha256) is None
            or self.row_start != window.row_start
            or self.row_stop != window.row_stop
            or self.column_start != window.column_start
            or self.column_stop != window.column_stop
            or self.grid_definition_sha256 != window.grid_definition_sha256
            or self.coverage_definition_sha256 != window.coverage_definition_sha256
        ):
            raise NoaaErrCompletionIntegrityError(
                "NOAA region authority identity/window binding differs"
            )

    @property
    def rows(self) -> int:
        return self.row_stop - self.row_start

    @property
    def columns(self) -> int:
        return self.column_stop - self.column_start

    @property
    def pixels(self) -> int:
        return self.rows * self.columns

    @property
    def region(self) -> NoaaErrRegion:
        return NoaaErrRegion(
            self.minimum_latitude,
            self.minimum_longitude,
            self.maximum_latitude,
            self.maximum_longitude,
        )

    def to_mapping(self) -> dict[str, Any]:
        body = {
            "acquisition_job_id": self.acquisition_job_id,
            "acquisition_job_sha256": self.acquisition_job_sha256,
            "bounds": {
                "maximum_latitude": self.maximum_latitude,
                "maximum_longitude": self.maximum_longitude,
                "minimum_latitude": self.minimum_latitude,
                "minimum_longitude": self.minimum_longitude,
            },
            "coverage_definition_sha256": self.coverage_definition_sha256,
            "desired_state_sha256": self.desired_state_sha256,
            "grid_definition_sha256": self.grid_definition_sha256,
            "plan_sha256": self.plan_sha256,
            "policy_sha256": self.policy_sha256,
            "region_ids": list(self.region_ids),
            "region_sha256s": list(self.region_sha256s),
            "schema": REGION_AUTHORITY_SCHEMA,
            "target_ids": list(self.target_ids),
            "window": {
                "column_start": self.column_start,
                "column_stop": self.column_stop,
                "columns": self.columns,
                "pixels": self.pixels,
                "row_start": self.row_start,
                "row_stop": self.row_stop,
                "rows": self.rows,
            },
        }
        body["authority_sha256"] = _digest(body)
        return body

    @property
    def fingerprint(self) -> str:
        return str(self.to_mapping()["authority_sha256"])

    @classmethod
    def from_mapping(cls, value: object) -> "NoaaErrRegionAuthority":
        exact = _exact(
            value,
            {
                "acquisition_job_id",
                "acquisition_job_sha256",
                "authority_sha256",
                "bounds",
                "coverage_definition_sha256",
                "desired_state_sha256",
                "grid_definition_sha256",
                "plan_sha256",
                "policy_sha256",
                "region_ids",
                "region_sha256s",
                "schema",
                "target_ids",
                "window",
            },
            "NOAA region authority",
        )
        if exact["schema"] != REGION_AUTHORITY_SCHEMA:
            raise NoaaErrCompletionIntegrityError("NOAA region authority contract differs")
        body = dict(exact)
        authority_sha = body.pop("authority_sha256")
        if _digest(body) != _sha(authority_sha, "region authority SHA-256"):
            raise NoaaErrCompletionIntegrityError("NOAA region authority checksum differs")
        bounds = _exact(
            exact["bounds"],
            {
                "maximum_latitude",
                "maximum_longitude",
                "minimum_latitude",
                "minimum_longitude",
            },
            "NOAA region bounds",
        )
        window = _exact(
            exact["window"],
            {
                "column_start",
                "column_stop",
                "columns",
                "pixels",
                "row_start",
                "row_stop",
                "rows",
            },
            "NOAA region window",
        )
        for key in (
            "column_start",
            "column_stop",
            "columns",
            "pixels",
            "row_start",
            "row_stop",
            "rows",
        ):
            if isinstance(window[key], bool) or not isinstance(window[key], int):
                raise NoaaErrCompletionIntegrityError("NOAA region window integer differs")
        sequences: dict[str, tuple[str, ...]] = {}
        for key in ("region_ids", "region_sha256s", "target_ids"):
            raw = exact[key]
            if not isinstance(raw, list) or any(not isinstance(item, str) for item in raw):
                raise NoaaErrCompletionIntegrityError("NOAA region identity sequence differs")
            sequences[key] = tuple(raw)
        authority = cls(
            desired_state_sha256=_sha(exact["desired_state_sha256"], "region desired SHA-256"),
            plan_sha256=_sha(exact["plan_sha256"], "region plan SHA-256"),
            acquisition_job_id=_identifier(
                exact["acquisition_job_id"], "region acquisition job ID"
            ),
            acquisition_job_sha256=_sha(exact["acquisition_job_sha256"], "region job SHA-256"),
            policy_sha256=_sha(exact["policy_sha256"], "region policy SHA-256"),
            region_ids=sequences["region_ids"],
            region_sha256s=sequences["region_sha256s"],
            target_ids=sequences["target_ids"],
            minimum_latitude=_coordinate(
                bounds["minimum_latitude"],
                "region minimum latitude",
                minimum=-90.0,
                maximum=90.0,
            ),
            minimum_longitude=_coordinate(
                bounds["minimum_longitude"],
                "region minimum longitude",
                minimum=-180.0,
                maximum=180.0,
            ),
            maximum_latitude=_coordinate(
                bounds["maximum_latitude"],
                "region maximum latitude",
                minimum=-90.0,
                maximum=90.0,
            ),
            maximum_longitude=_coordinate(
                bounds["maximum_longitude"],
                "region maximum longitude",
                minimum=-180.0,
                maximum=180.0,
            ),
            row_start=window["row_start"],
            row_stop=window["row_stop"],
            column_start=window["column_start"],
            column_stop=window["column_stop"],
            grid_definition_sha256=_sha(exact["grid_definition_sha256"], "region grid SHA-256"),
            coverage_definition_sha256=_sha(
                exact["coverage_definition_sha256"], "region coverage SHA-256"
            ),
        )
        if (
            authority.to_mapping() != dict(exact)
            or window["rows"] != authority.rows
            or window["columns"] != authority.columns
            or window["pixels"] != authority.pixels
        ):
            raise NoaaErrCompletionIntegrityError("NOAA region authority roundtrip differs")
        return authority


def _job_from_router_proof(
    desired_state: DesiredState,
    lease: CompletionLease,
) -> tuple[AcquisitionPlan, AcquisitionJob]:
    proof = _exact(
        lease.proof,
        {
            "capacity_commit",
            "capture_reference",
            "claims",
            "consumer_binding",
            "consumer_binding_sha256",
            "desired_authority",
            "execution_receipt",
            "recovery_binding_sha256",
            "reference_content_sha256",
            "scheduler_terminal",
            "schema",
            "workload",
        },
        "terminal router proof",
    )
    if proof["schema"] != REFERENCE_PROOF_SCHEMA:
        raise NoaaErrCompletionIntegrityError("router proof contract differs")
    terminal = _exact(
        proof["scheduler_terminal"],
        {
            "attempt_count",
            "deadline_at",
            "desired_state_sha256",
            "intent_id",
            "job_id",
            "job_type",
            "last_error_code",
            "last_transition",
            "payload_sha256",
            "proof_sha256",
            "record_sha256",
            "scheduled_for",
            "slot_id",
            "slot_index",
            "state",
            "terminal_at",
        },
        "router scheduler terminal",
    )
    if (
        terminal["state"] != "completed"
        or terminal["last_transition"] != "completed"
        or terminal["desired_state_sha256"] != desired_state.state_sha256
    ):
        raise NoaaErrCompletionIntegrityError("router scheduler terminal is not completed")
    plan = validate_worker_acquisition_plan(build_worker_acquisition_plan(desired_state))
    try:
        job = validate_worker_acquisition_job(
            reconstruct_acquisition_job_for_slot(
                plan,
                intent_id=str(terminal["intent_id"]),
                scheduled_for=float(terminal["scheduled_for"]),
            )
        )
    except Exception as error:
        raise NoaaErrCompletionIntegrityError(
            "NOAA acquisition job cannot be reconstructed"
        ) from error
    workload = _exact(
        proof["workload"],
        {
            "job_id",
            "job_sha256",
            "plan_sha256",
            "product_id",
            "provider_id",
            "snapshot_sha256",
            "workload_id",
        },
        "router workload",
    )
    if (
        job.job_id != lease.acquisition_job_id
        or job.job_id != workload["job_id"]
        or job.job_sha256 != workload["job_sha256"]
        or job.plan_sha256 != workload["plan_sha256"]
        or job.snapshot_sha256 != workload["snapshot_sha256"]
        or job.workload_id != WORKLOAD_ID
        or workload["workload_id"] != WORKLOAD_ID
        or job.provider_id != "noaa"
        or workload["provider_id"] != "noaa"
        or job.product_id != "enterprise_rainrate_glb5_native_netcdf4"
        or workload["product_id"] != job.product_id
    ):
        raise NoaaErrCompletionIntegrityError("router workload differs from NOAA ERR")
    return plan, job


def issue_noaa_err_region_authority(
    *,
    desired_state: DesiredState,
    lease: CompletionLease,
    policy: NoaaErrRegionPolicy = NoaaErrRegionPolicy(),
) -> NoaaErrRegionAuthority:
    """Derive one exact, non-wrapping bounded union from desired regions."""

    if not isinstance(desired_state, DesiredState):
        raise TypeError("desired_state must be DesiredState")
    if not isinstance(lease, CompletionLease):
        raise TypeError("lease must be CompletionLease")
    if not isinstance(policy, NoaaErrRegionPolicy):
        raise TypeError("policy must be NoaaErrRegionPolicy")
    plan, job = _job_from_router_proof(desired_state, lease)
    if (
        not job.region_ids
        or not job.target_ids
        or len(job.region_ids) > policy.max_regions
        or len(job.target_ids) > policy.max_targets
    ):
        raise NoaaErrCompletionIntegrityError(
            "NOAA target-region binding is absent or exceeds its identity cap"
        )
    regions_by_id = {region.region_id: region for region in plan.regions}
    if set(job.region_ids) - set(regions_by_id):
        raise NoaaErrCompletionIntegrityError(
            "NOAA target-region binding is missing from acquisition plan"
        )
    regions = tuple(regions_by_id[item] for item in job.region_ids)
    if set(job.target_ids) != {target for region in regions for target in region.target_ids} or any(
        region.radius_km <= 0.0 or region.radius_km > float(policy.max_radius_km)
        for region in regions
    ):
        raise NoaaErrCompletionIntegrityError("NOAA target-region/target/radius authority differs")
    minima_lat: list[float] = []
    maxima_lat: list[float] = []
    minima_lon: list[float] = []
    maxima_lon: list[float] = []
    for region in regions:
        angular = float(region.radius_km) / EARTH_RADIUS_KM
        latitude_radians = math.radians(region.center_latitude)
        if (
            abs(latitude_radians) + angular >= math.pi / 2.0
            or abs(math.cos(latitude_radians)) < 1.0e-12
        ):
            raise NoaaErrCompletionIntegrityError("NOAA spherical target region reaches a pole")
        latitude_delta = math.degrees(angular)
        ratio = math.sin(angular) / math.cos(latitude_radians)
        if not -1.0 < ratio < 1.0:
            raise NoaaErrCompletionIntegrityError("NOAA longitude span cannot be bounded")
        longitude_delta = math.degrees(math.asin(ratio))
        minima_lat.append(region.center_latitude - latitude_delta)
        maxima_lat.append(region.center_latitude + latitude_delta)
        minima_lon.append(region.center_longitude - longitude_delta)
        maxima_lon.append(region.center_longitude + longitude_delta)
    requested = NoaaErrRegion(
        minimum_latitude=min(minima_lat),
        minimum_longitude=min(minima_lon),
        maximum_latitude=max(maxima_lat),
        maximum_longitude=max(maxima_lon),
    )
    try:
        window = quantitative._window_for_region(requested)  # noqa: SLF001
    except (TypeError, ValueError) as error:
        raise NoaaErrCompletionIntegrityError(
            "NOAA target-region union is outside the decoder grid/cap"
        ) from error
    if window.rows * window.columns > policy.max_window_pixels:
        raise NoaaErrCompletionCapacityError("NOAA target-region union exceeds its pixel cap")
    return NoaaErrRegionAuthority(
        desired_state_sha256=desired_state.state_sha256,
        plan_sha256=plan.plan_sha256,
        acquisition_job_id=job.job_id,
        acquisition_job_sha256=job.job_sha256,
        policy_sha256=policy.fingerprint,
        region_ids=tuple(job.region_ids),
        region_sha256s=tuple(regions_by_id[item].region_sha256 for item in job.region_ids),
        target_ids=tuple(job.target_ids),
        minimum_latitude=requested.minimum_latitude,
        minimum_longitude=requested.minimum_longitude,
        maximum_latitude=requested.maximum_latitude,
        maximum_longitude=requested.maximum_longitude,
        row_start=window.row_start,
        row_stop=window.row_stop,
        column_start=window.column_start,
        column_stop=window.column_stop,
        grid_definition_sha256=window.grid_definition_sha256,
        coverage_definition_sha256=window.coverage_definition_sha256,
    )


@dataclass(frozen=True, slots=True)
class NoaaErrLeaseBinding:
    event_id: str
    event_sha256: str
    producer_receipt_sha256: str
    acquisition_job_id: str
    acquisition_job_sha256: str
    plan_sha256: str
    desired_state_sha256: str
    desired_authority_sha256: str
    stream_sequence: int
    reference_content_sha256: str
    reference_size_bytes: int
    capture_id: str
    capture_receipt_sha256: str
    capture_manifest_sha256: str
    evaluated_at: float
    region_authority: NoaaErrRegionAuthority

    def to_mapping(self) -> dict[str, Any]:
        return {
            "acquisition_job_id": self.acquisition_job_id,
            "acquisition_job_sha256": self.acquisition_job_sha256,
            "capture_id": self.capture_id,
            "capture_manifest_sha256": self.capture_manifest_sha256,
            "capture_receipt_sha256": self.capture_receipt_sha256,
            "consumer_id": CONSUMER_ID,
            "desired_authority_sha256": self.desired_authority_sha256,
            "desired_state_sha256": self.desired_state_sha256,
            "evaluated_at": self.evaluated_at,
            "event_id": self.event_id,
            "event_sha256": self.event_sha256,
            "plan_sha256": self.plan_sha256,
            "producer_receipt_sha256": self.producer_receipt_sha256,
            "reference_content_sha256": self.reference_content_sha256,
            "reference_size_bytes": self.reference_size_bytes,
            "region_authority": self.region_authority.to_mapping(),
            "stream_id": STREAM_ID,
            "stream_sequence": self.stream_sequence,
            "workload_id": WORKLOAD_ID,
        }

    @classmethod
    def from_mapping(cls, value: object) -> "NoaaErrLeaseBinding":
        exact = _exact(
            value,
            {
                "acquisition_job_id",
                "acquisition_job_sha256",
                "capture_id",
                "capture_manifest_sha256",
                "capture_receipt_sha256",
                "consumer_id",
                "desired_authority_sha256",
                "desired_state_sha256",
                "evaluated_at",
                "event_id",
                "event_sha256",
                "plan_sha256",
                "producer_receipt_sha256",
                "reference_content_sha256",
                "reference_size_bytes",
                "region_authority",
                "stream_id",
                "stream_sequence",
                "workload_id",
            },
            "NOAA lease binding",
        )
        if (
            exact["consumer_id"] != CONSUMER_ID
            or exact["stream_id"] != STREAM_ID
            or exact["workload_id"] != WORKLOAD_ID
        ):
            raise NoaaErrCompletionIntegrityError("NOAA lease route differs")
        integers = ("reference_size_bytes", "stream_sequence")
        if any(
            isinstance(exact[key], bool) or not isinstance(exact[key], int) or int(exact[key]) < 1
            for key in integers
        ):
            raise NoaaErrCompletionIntegrityError("NOAA lease count differs")
        binding = cls(
            event_id=_identifier(exact["event_id"], "event ID"),
            event_sha256=_sha(exact["event_sha256"], "event SHA-256"),
            producer_receipt_sha256=_sha(
                exact["producer_receipt_sha256"], "producer receipt SHA-256"
            ),
            acquisition_job_id=_identifier(exact["acquisition_job_id"], "acquisition job ID"),
            acquisition_job_sha256=_sha(exact["acquisition_job_sha256"], "acquisition job SHA-256"),
            plan_sha256=_sha(exact["plan_sha256"], "plan SHA-256"),
            desired_state_sha256=_sha(exact["desired_state_sha256"], "desired SHA-256"),
            desired_authority_sha256=_sha(
                exact["desired_authority_sha256"], "desired authority SHA-256"
            ),
            stream_sequence=int(exact["stream_sequence"]),
            reference_content_sha256=_sha(exact["reference_content_sha256"], "reference SHA-256"),
            reference_size_bytes=int(exact["reference_size_bytes"]),
            capture_id=_identifier(exact["capture_id"], "capture ID"),
            capture_receipt_sha256=_sha(exact["capture_receipt_sha256"], "capture receipt SHA-256"),
            capture_manifest_sha256=_sha(
                exact["capture_manifest_sha256"], "capture manifest SHA-256"
            ),
            evaluated_at=_timestamp(exact["evaluated_at"], "evaluated_at"),
            region_authority=NoaaErrRegionAuthority.from_mapping(exact["region_authority"]),
        )
        if (
            binding.to_mapping() != dict(exact)
            or _EVENT.fullmatch(binding.event_id) is None
            or binding.region_authority.acquisition_job_id != binding.acquisition_job_id
            or binding.region_authority.acquisition_job_sha256 != binding.acquisition_job_sha256
            or binding.region_authority.plan_sha256 != binding.plan_sha256
            or binding.region_authority.desired_state_sha256 != binding.desired_state_sha256
        ):
            raise NoaaErrCompletionIntegrityError(
                "NOAA lease binding roundtrip/region relation differs"
            )
        return binding


def _safe_member(role: str, receipt: Any) -> dict[str, Any]:
    return {
        "content_sha256": receipt.content_sha256,
        "content_type": receipt.content_type,
        "product": receipt.product,
        "provider": receipt.provider,
        "receipt_id": receipt.receipt_id,
        "receipt_sha256": receipt.receipt_sha256,
        "role": role,
        "size_bytes": receipt.size_bytes,
    }


def _load_capture_bundle(
    raw_store: RawArtifactStore,
    capture_id: str,
):
    try:
        return raw_store.load_capture_bundle(capture_id)
    except RawArtifactCapacityError as error:
        raise NoaaErrCompletionCapacityError(
            "NOAA raw-reference verification exceeded its store cap"
        ) from error
    except RawArtifactError as error:
        raise NoaaErrCompletionIntegrityError("NOAA raw-reference verification failed") from error


def _validate_raw_binding(
    binding: NoaaErrLeaseBinding,
    raw_store: RawArtifactStore,
) -> None:
    bundle = _load_capture_bundle(raw_store, binding.capture_id)
    if bundle is None:
        raise NoaaErrCompletionIntegrityError("NOAA referenced capture is absent")
    capture, receipts = bundle
    if (
        capture.provider != "noaa_enterprise_rainrate"
        or capture.product != native.NOAA_ERR_CAPTURE_PRODUCT
        or capture.fetch_id != f"{binding.acquisition_job_id}:capture"
        or capture.capture_receipt_sha256 != binding.capture_receipt_sha256
        or capture.manifest_sha256 != binding.capture_manifest_sha256
        or set(receipts) != {"listing_xml", "netcdf4"}
        or receipts["listing_xml"].product != native.NOAA_ERR_LISTING_PRODUCT
        or receipts["netcdf4"].product != native.NOAA_ERR_NETCDF4_PRODUCT
    ):
        raise NoaaErrCompletionIntegrityError("NOAA referenced capture binding differs")


def _validate_router_lease(
    lease: CompletionLease,
    desired_state: DesiredState,
    raw_store: RawArtifactStore,
    policy: NoaaErrRegionPolicy,
) -> NoaaErrLeaseBinding:
    if (
        lease.consumer_id != CONSUMER_ID
        or lease.stream_id != STREAM_ID
        or _EVENT.fullmatch(lease.event_id) is None
    ):
        raise NoaaErrCompletionIntegrityError("NOAA lifecycle route differs")
    authority = desired_authority_proof(desired_state)
    if (
        lease.desired_state_sha256 != desired_state.state_sha256
        or lease.authority_sha256 != authority["authority_sha256"]
    ):
        raise NoaaErrCompletionConflict(
            "NOAA active desired authority differs from lifecycle lease"
        )
    if (
        hashlib.sha256(lease.payload).hexdigest() != lease.content_sha256
        or len(lease.payload) != lease.size_bytes
    ):
        raise NoaaErrCompletionIntegrityError("NOAA reference bytes differ")
    try:
        document = json.loads(lease.payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise NoaaErrCompletionIntegrityError("NOAA reference payload is not JSON") from error
    if _canonical(document) != lease.payload:
        raise NoaaErrCompletionIntegrityError("NOAA reference payload is not canonical")
    payload = _exact(
        document,
        {
            "acquisition_job_id",
            "capture_reference",
            "claims",
            "consumer_binding_sha256",
            "consumer_id",
            "schema",
            "stream_id",
            "workload_id",
        },
        "NOAA reference payload",
    )
    expected_consumer = TERMINAL_RAW_CONSUMER_CATALOG[WORKLOAD_ID]
    claims = _exact(
        payload["claims"],
        {
            "decoded",
            "fact_staged",
            "meteorological_admission",
            "operational_snapshot_published",
            "raw_bytes_copied",
            "raw_refetch_performed",
        },
        "NOAA reference claims",
    )
    proof = lease.proof
    proof_exact = _exact(
        proof,
        {
            "capacity_commit",
            "capture_reference",
            "claims",
            "consumer_binding",
            "consumer_binding_sha256",
            "desired_authority",
            "execution_receipt",
            "recovery_binding_sha256",
            "reference_content_sha256",
            "scheduler_terminal",
            "schema",
            "workload",
        },
        "NOAA router proof",
    )
    if (
        payload["schema"] != REFERENCE_PAYLOAD_SCHEMA
        or proof_exact["schema"] != REFERENCE_PROOF_SCHEMA
        or payload["consumer_id"] != CONSUMER_ID
        or payload["stream_id"] != STREAM_ID
        or payload["workload_id"] != WORKLOAD_ID
        or payload["acquisition_job_id"] != lease.acquisition_job_id
        or payload["consumer_binding_sha256"] != expected_consumer.fingerprint
        or proof_exact["consumer_binding_sha256"] != expected_consumer.fingerprint
        or proof_exact["consumer_binding"] != expected_consumer.as_dict()
        or proof_exact["desired_authority"] != authority
        or proof_exact["claims"] != dict(claims)
        or proof_exact["capture_reference"] != payload["capture_reference"]
        or proof_exact["reference_content_sha256"] != lease.content_sha256
        or set(claims.values()) != {False}
    ):
        raise NoaaErrCompletionIntegrityError("NOAA router event/proof/consumer authority differs")
    plan, job = _job_from_router_proof(desired_state, lease)
    capture_reference = _exact(
        payload["capture_reference"],
        {
            "capture_id",
            "capture_receipt_schema",
            "capture_receipt_sha256",
            "capture_schema",
            "manifest_sha256",
            "members",
            "product",
            "provider",
            "raw_store_schema_version",
            "role_set_content_sha256",
            "role_set_receipt_sha256",
            "total_bytes",
        },
        "NOAA capture reference",
    )
    bundle = _load_capture_bundle(raw_store, str(capture_reference["capture_id"]))
    if bundle is None:
        raise NoaaErrCompletionIntegrityError("NOAA referenced capture is absent")
    capture, receipts = bundle
    members = capture_reference["members"]
    expected_members = [_safe_member(role, receipt) for role, receipt in sorted(receipts.items())]
    if (
        capture_reference["provider"] != "noaa_enterprise_rainrate"
        or capture_reference["product"] != native.NOAA_ERR_CAPTURE_PRODUCT
        or capture_reference["capture_id"] != capture.capture_id
        or capture_reference["capture_receipt_sha256"] != capture.capture_receipt_sha256
        or capture_reference["manifest_sha256"] != capture.manifest_sha256
        or members != expected_members
        or set(receipts) != {"listing_xml", "netcdf4"}
        or capture.fetch_id != f"{job.job_id}:capture"
    ):
        raise NoaaErrCompletionIntegrityError("NOAA router capture manifest reference differs")
    region_authority = issue_noaa_err_region_authority(
        desired_state=desired_state,
        lease=lease,
        policy=policy,
    )
    terminal = proof_exact["scheduler_terminal"]
    evaluated_at = _timestamp(terminal["terminal_at"], "scheduler terminal time")
    if evaluated_at < max(receipt.received_at for receipt in receipts.values()):
        raise NoaaErrCompletionIntegrityError("NOAA completion predates referenced raw receipt")
    binding = NoaaErrLeaseBinding(
        event_id=lease.event_id,
        event_sha256=lease.event_sha256,
        producer_receipt_sha256=lease.producer_receipt_sha256,
        acquisition_job_id=job.job_id,
        acquisition_job_sha256=job.job_sha256,
        plan_sha256=plan.plan_sha256,
        desired_state_sha256=desired_state.state_sha256,
        desired_authority_sha256=lease.authority_sha256,
        stream_sequence=lease.stream_sequence,
        reference_content_sha256=lease.content_sha256,
        reference_size_bytes=lease.size_bytes,
        capture_id=capture.capture_id,
        capture_receipt_sha256=capture.capture_receipt_sha256,
        capture_manifest_sha256=capture.manifest_sha256,
        evaluated_at=evaluated_at,
        region_authority=region_authority,
    )
    _validate_raw_binding(binding, raw_store)
    return binding


@dataclass(frozen=True, slots=True)
class NoaaErrCompletionOutputLimits:
    max_receipts: int = 4096
    max_receipt_bytes: int = 128 * 1024
    max_database_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        for label, value, minimum, maximum in (
            ("max_receipts", self.max_receipts, 1, 1_000_000),
            ("max_receipt_bytes", self.max_receipt_bytes, 1024, 1024 * 1024),
            (
                "max_database_bytes",
                self.max_database_bytes,
                1024 * 1024,
                4 * 1024**3,
            ),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise ValueError(f"{label} is outside its output bound")

    @property
    def fingerprint(self) -> str:
        return _digest(
            {
                **asdict(self),
                "schema": "stv3-noaa-err-completion-output-limits-v1",
            }
        )


@dataclass(frozen=True, slots=True)
class NoaaErrCompletionOutputReceipt:
    receipt_sha256: str
    process_receipt_sha256: str
    output_sha256: str
    event_id: str
    event_sha256: str
    producer_receipt_sha256: str
    desired_state_sha256: str
    desired_authority_sha256: str
    region_authority_sha256: str
    isolation_authority_sha256: str
    isolation_limits_sha256: str
    candidate_content_sha256: str
    decoder_run_id: str
    created_at: float
    output: dict[str, Any]

    @property
    def disposition(self) -> str:
        return OUTPUT_DISPOSITION


_OUTPUT_KEYS = {
    "authority",
    "claims",
    "consumer_id",
    "decoder",
    "disposition",
    "event",
    "fact_revision",
    "input",
    "process_isolation",
    "region_authority",
    "schema",
    "shadow",
    "stream_id",
}


def _validated_output(value: object) -> dict[str, Any]:
    exact = _exact(value, _OUTPUT_KEYS, "NOAA completion output")
    document = json.loads(_canonical(dict(exact)))
    _assert_output_safe(document)
    authority = _exact(
        document["authority"],
        {
            "desired_authority_sha256",
            "desired_state_sha256",
        },
        "NOAA output desired authority",
    )
    event = _exact(
        document["event"],
        {
            "acquisition_job_id",
            "event_id",
            "event_sha256",
            "producer_receipt_sha256",
            "stream_sequence",
        },
        "NOAA output event",
    )
    claims = _exact(
        document["claims"],
        {
            "decoded_shadow_candidate",
            "fact_staged",
            "meteorological_admission_granted",
            "operational_snapshot_published",
            "raw_refetch_performed",
        },
        "NOAA output claims",
    )
    decoder = _exact(
        document["decoder"],
        {
            "artifact_handle",
            "artifact_sha256",
            "artifact_size_bytes",
            "attempt_id",
            "commit_id",
            "config_sha256",
            "identity",
            "job_id",
            "receipt_sha256",
            "result_sha256",
            "run_id",
            "version",
        },
        "NOAA output decoder",
    )
    shadow = _exact(
        document["shadow"],
        {
            "admission_status",
            "content_sha256",
            "quantity",
            "unit",
        },
        "NOAA output shadow",
    )
    isolation = _exact(
        document["process_isolation"],
        {
            "authority_sha256",
            "effect_contract_sha256",
            "limits_sha256",
            "state",
        },
        "NOAA output process isolation",
    )
    input_reference = _exact(
        document["input"],
        {
            "capture_id",
            "capture_manifest_sha256",
            "capture_receipt_sha256",
            "reference_content_sha256",
            "reference_size_bytes",
        },
        "NOAA output input reference",
    )
    region = NoaaErrRegionAuthority.from_mapping(document["region_authority"])
    if (
        document["schema"] != OUTPUT_DOCUMENT_SCHEMA
        or document["consumer_id"] != CONSUMER_ID
        or document["stream_id"] != STREAM_ID
        or document["disposition"] != OUTPUT_DISPOSITION
        or document["fact_revision"] is not None
        or claims
        != {
            "decoded_shadow_candidate": True,
            "fact_staged": False,
            "meteorological_admission_granted": False,
            "operational_snapshot_published": False,
            "raw_refetch_performed": False,
        }
        or shadow["admission_status"] != "withheld"
        or shadow["quantity"] != "instantaneous_precipitation_rate"
        or shadow["unit"] != "mm/h"
        or isolation["state"] != "enforced_before_target_start"
        or isolation["effect_contract_sha256"] != PROCESS_EFFECT_CONTRACT_SHA256
        or decoder["config_sha256"] != DECODER_CONFIG_SHA256
        or decoder["identity"] != DECODER_IDENTITY
        or decoder["version"] != DECODER_VERSION
        or not isinstance(decoder["artifact_size_bytes"], int)
        or isinstance(decoder["artifact_size_bytes"], bool)
        or decoder["artifact_size_bytes"] <= 0
        or decoder["artifact_sha256"] != shadow["content_sha256"]
        or decoder["artifact_handle"]
        != f"{quantitative.ARTIFACT_HANDLE_PREFIX}{decoder['artifact_sha256']}"
        or _DECODER_JOB.fullmatch(str(decoder["job_id"])) is None
        or _DECODER_ATTEMPT.fullmatch(str(decoder["attempt_id"])) is None
        or _DECODER_RUN.fullmatch(str(decoder["run_id"])) is None
        or _DECODER_COMMIT.fullmatch(str(decoder["commit_id"])) is None
        or not isinstance(event["stream_sequence"], int)
        or isinstance(event["stream_sequence"], bool)
        or event["stream_sequence"] < 1
        or not isinstance(input_reference["reference_size_bytes"], int)
        or isinstance(input_reference["reference_size_bytes"], bool)
        or input_reference["reference_size_bytes"] < 1
        or _ID.fullmatch(str(input_reference["capture_id"])) is None
        or event["acquisition_job_id"] != region.acquisition_job_id
        or authority["desired_state_sha256"] != region.desired_state_sha256
    ):
        raise NoaaErrCompletionIntegrityError(
            "NOAA output route, region, decoder or shadow claims differ"
        )
    for field, label in (
        (event["event_sha256"], "output event SHA-256"),
        (event["producer_receipt_sha256"], "output producer SHA-256"),
        (authority["desired_state_sha256"], "output desired SHA-256"),
        (authority["desired_authority_sha256"], "output authority SHA-256"),
        (isolation["authority_sha256"], "output isolation authority SHA-256"),
        (isolation["effect_contract_sha256"], "output effect SHA-256"),
        (isolation["limits_sha256"], "output limits SHA-256"),
        (decoder["artifact_sha256"], "output artifact SHA-256"),
        (decoder["receipt_sha256"], "output decoder receipt SHA-256"),
        (decoder["result_sha256"], "output decoder result SHA-256"),
        (input_reference["capture_manifest_sha256"], "output manifest SHA-256"),
        (input_reference["capture_receipt_sha256"], "output capture SHA-256"),
        (input_reference["reference_content_sha256"], "output reference SHA-256"),
    ):
        _sha(field, label)
    if _EVENT.fullmatch(str(event["event_id"])) is None:
        raise NoaaErrCompletionIntegrityError("NOAA output event ID differs")
    return document


class NoaaErrCompletionOutputStore:
    """Bounded, checksum-verified output-before-ack authority."""

    def __init__(
        self,
        path: str | Path,
        *,
        limits: NoaaErrCompletionOutputLimits | None = None,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(os.path.abspath(os.fspath(path)))
        self.limits = limits or NoaaErrCompletionOutputLimits()
        self._clock = clock
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self._database_witness: tuple[int, int, int]
        self._store_instance_sha256: str
        self.initialize()

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    @staticmethod
    def _normalized_path(path: Path) -> str:
        return os.path.normcase(os.path.normpath(str(path)))

    @staticmethod
    def _path_witness(path: Path) -> tuple[int, int, int]:
        try:
            details = path.lstat()
        except OSError as error:
            raise NoaaErrCompletionIntegrityError(
                "NOAA output store witness is unavailable"
            ) from error
        if (
            path.is_symlink()
            or not stat.S_ISREG(details.st_mode)
            or int(details.st_nlink) != 1
        ):
            raise NoaaErrCompletionIntegrityError("NOAA output path is unsafe")
        return (
            int(details.st_dev),
            int(details.st_ino),
            int(details.st_mode),
        )

    def _prepare_path(self) -> tuple[int, int, int]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        parent = self.path.parent
        for candidate in (parent, *parent.parents):
            try:
                details = candidate.lstat()
            except OSError as error:
                raise NoaaErrCompletionIntegrityError(
                    "NOAA output parent is unavailable"
                ) from error
            if candidate.is_symlink() or not stat.S_ISDIR(details.st_mode):
                raise NoaaErrCompletionIntegrityError(
                    "NOAA output parent is unsafe"
                )
        if (
            self._normalized_path(parent.resolve(strict=True))
            != self._normalized_path(parent)
        ):
            raise NoaaErrCompletionIntegrityError(
                "NOAA output parent is aliased"
            )
        if self.path.is_symlink():
            raise NoaaErrCompletionIntegrityError("NOAA output path is unsafe")
        if not self.path.exists():
            flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(self.path, flags, 0o600)
                try:
                    os.set_inheritable(descriptor, False)
                    details = os.fstat(descriptor)
                    if not stat.S_ISREG(details.st_mode):
                        raise NoaaErrCompletionIntegrityError(
                            "NOAA output leaf is not regular"
                        )
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            except NoaaErrCompletionError:
                raise
            except OSError as error:
                raise NoaaErrCompletionIntegrityError(
                    "NOAA output leaf creation failed"
                ) from error
        witness = self._path_witness(self.path)
        if (
            self._normalized_path(self.path.resolve(strict=True))
            != self._normalized_path(self.path)
        ):
            raise NoaaErrCompletionIntegrityError("NOAA output path is aliased")
        return witness

    def _verify_path(self) -> None:
        try:
            resolved = self.path.resolve(strict=True)
        except OSError as error:
            raise NoaaErrCompletionIntegrityError(
                "NOAA output store instance changed"
            ) from error
        if (
            self._normalized_path(resolved) != self._normalized_path(self.path)
            or self._path_witness(self.path) != self._database_witness
        ):
            raise NoaaErrCompletionIntegrityError(
                "NOAA output store instance changed"
            )

    def _connect(self) -> sqlite3.Connection:
        self._verify_path()
        uri_path = quote(self.path.as_posix(), safe="/:")
        database = sqlite3.connect(
            f"file:{uri_path}?mode=rw&nofollow=1",
            timeout=5.0,
            isolation_level=None,
            uri=True,
        )
        database.row_factory = sqlite3.Row
        try:
            self._verify_path()
            database.execute("PRAGMA foreign_keys=ON")
            journal = str(
                database.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            ).casefold()
            if journal != "delete":
                raise NoaaErrCompletionIntegrityError(
                    "NOAA output journal mode differs"
                )
            database.execute("PRAGMA synchronous=FULL")
            database.execute("PRAGMA trusted_schema=OFF")
            database.execute("PRAGMA busy_timeout=5000")
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            maximum_pages = max(1, self.limits.max_database_bytes // page_size)
            page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
            configured_pages = int(
                database.execute(
                    f"PRAGMA max_page_count={maximum_pages}"
                ).fetchone()[0]
            )
            if page_count > maximum_pages or configured_pages > maximum_pages:
                raise NoaaErrCompletionCapacityError(
                    "NOAA output database exceeds its page cap"
                )
        except BaseException:
            database.close()
            raise
        return database

    @staticmethod
    def _tables(database: sqlite3.Connection) -> set[str]:
        return {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            if not str(row[0]).startswith("sqlite_")
        }

    @staticmethod
    def _columns(database: sqlite3.Connection, table: str) -> set[str]:
        return {
            str(row[1]) for row in database.execute(f"PRAGMA table_info({table})")
        }

    @staticmethod
    def _column_definition(
        database: sqlite3.Connection,
        table: str,
    ) -> tuple[tuple[str, str, int, int], ...]:
        return tuple(
            (str(row[1]), str(row[2]), int(row[3]), int(row[5]))
            for row in database.execute(f"PRAGMA table_info({table})")
        )

    @staticmethod
    def _legacy_metadata_definition() -> tuple[tuple[str, str, int, int], ...]:
        return (
            ("singleton", "INTEGER", 0, 1),
            ("contract", "TEXT", 1, 0),
            ("limits_sha256", "TEXT", 1, 0),
        )

    @classmethod
    def _metadata_definition(cls) -> tuple[tuple[str, str, int, int], ...]:
        return cls._legacy_metadata_definition() + (
            ("schema_version", "INTEGER", 0, 0),
            ("store_instance_sha256", "TEXT", 0, 0),
        )

    @staticmethod
    def _output_definition() -> tuple[tuple[str, str, int, int], ...]:
        return (
            ("event_id", "TEXT", 0, 1),
            ("output_json", "BLOB", 1, 0),
            ("output_bytes", "INTEGER", 1, 0),
            ("output_sha256", "TEXT", 1, 0),
            ("receipt_sha256", "TEXT", 1, 0),
            ("process_receipt_sha256", "TEXT", 1, 0),
            ("created_at", "REAL", 1, 0),
        )

    @staticmethod
    def _normalized_sql(value: str) -> str:
        return " ".join(value.split()).casefold()

    @classmethod
    def _trigger_definitions(cls) -> dict[str, str]:
        return {
            "completion_outputs_no_delete": (
                "CREATE TRIGGER completion_outputs_no_delete BEFORE DELETE ON "
                "completion_outputs BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "completion_outputs_no_update": (
                "CREATE TRIGGER completion_outputs_no_update BEFORE UPDATE ON "
                "completion_outputs BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "output_metadata_no_delete": (
                "CREATE TRIGGER output_metadata_no_delete BEFORE DELETE ON "
                "output_metadata BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "output_metadata_no_update": (
                "CREATE TRIGGER output_metadata_no_update BEFORE UPDATE ON "
                "output_metadata BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
        }

    @classmethod
    def _install_immutability_triggers(
        cls, database: sqlite3.Connection
    ) -> None:
        for statement in cls._trigger_definitions().values():
            database.execute(statement)

    @classmethod
    def _validate_immutability_triggers(
        cls,
        database: sqlite3.Connection,
        *,
        legacy: bool = False,
    ) -> None:
        actual = {
            str(row[0]): str(row[1])
            for row in database.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger'"
            )
        }
        expected = cls._trigger_definitions()
        if legacy:
            expected = {
                name: statement
                for name, statement in expected.items()
                if name.startswith("completion_outputs_")
            }
        if set(actual) != set(expected) or any(
            cls._normalized_sql(actual[name])
            != cls._normalized_sql(statement)
            for name, statement in expected.items()
        ):
            raise NoaaErrCompletionIntegrityError(
                "NOAA output store immutability triggers differ"
            )

    @staticmethod
    def _output_columns() -> set[str]:
        return {
            "event_id",
            "output_json",
            "output_bytes",
            "output_sha256",
            "receipt_sha256",
            "process_receipt_sha256",
            "created_at",
        }

    @staticmethod
    def _validate_unique_indexes(database: sqlite3.Connection) -> None:
        unique_columns: set[str] = set()
        for row in database.execute("PRAGMA index_list(completion_outputs)"):
            if int(row[2]) != 1:
                continue
            members = tuple(
                database.execute("SELECT name FROM pragma_index_info(?)", (str(row[1]),))
            )
            if len(members) != 1:
                raise NoaaErrCompletionIntegrityError("NOAA output store uniqueness arity differs")
            unique_columns.add(str(members[0][0]))
        if unique_columns != {
            "event_id",
            "output_sha256",
            "receipt_sha256",
            "process_receipt_sha256",
        }:
            raise NoaaErrCompletionIntegrityError("NOAA output store uniqueness differs")

    def _create_schema(self, database: sqlite3.Connection) -> None:
        database.execute(
            """CREATE TABLE output_metadata(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                contract TEXT NOT NULL,
                limits_sha256 TEXT NOT NULL,
                schema_version INTEGER,
                store_instance_sha256 TEXT
            )"""
        )
        database.execute(
            """CREATE TABLE completion_outputs(
                event_id TEXT PRIMARY KEY,
                output_json BLOB NOT NULL,
                output_bytes INTEGER NOT NULL,
                output_sha256 TEXT NOT NULL UNIQUE,
                receipt_sha256 TEXT NOT NULL UNIQUE,
                process_receipt_sha256 TEXT NOT NULL UNIQUE,
                created_at REAL NOT NULL
            )"""
        )
        database.execute(
            "INSERT INTO output_metadata VALUES(1,?,?,?,?)",
            (
                OUTPUT_STORE_CONTRACT,
                self.limits.fingerprint,
                OUTPUT_STORE_SCHEMA_VERSION,
                secrets.token_hex(32),
            ),
        )
        self._install_immutability_triggers(database)

    def _validate_legacy_v1_schema(self, database: sqlite3.Connection) -> None:
        if self._tables(database) != {"completion_outputs", "output_metadata"}:
            raise NoaaErrCompletionIntegrityError(
                "legacy NOAA output store objects differ"
            )
        if self._column_definition(
            database,
            "output_metadata",
        ) != self._legacy_metadata_definition():
            raise NoaaErrCompletionIntegrityError(
                "legacy NOAA output metadata columns differ"
            )
        if self._column_definition(
            database,
            "completion_outputs",
        ) != self._output_definition():
            raise NoaaErrCompletionIntegrityError(
                "legacy NOAA output columns differ"
            )
        self._validate_unique_indexes(database)
        self._validate_immutability_triggers(database, legacy=True)
        metadata = database.execute(
            "SELECT * FROM output_metadata WHERE singleton=1"
        ).fetchone()
        if (
            metadata is None
            or int(
                database.execute(
                    "SELECT count(*) FROM output_metadata"
                ).fetchone()[0]
            )
            != 1
            or str(metadata["contract"]) != LEGACY_OUTPUT_STORE_CONTRACT
            or str(metadata["limits_sha256"]) != self.limits.fingerprint
        ):
            raise NoaaErrCompletionIntegrityError(
                "legacy NOAA output store metadata differs"
            )
        count = int(
            database.execute("SELECT count(*) FROM completion_outputs").fetchone()[0]
        )
        if count > self.limits.max_receipts:
            raise NoaaErrCompletionCapacityError(
                "legacy NOAA output receipt cap exceeded"
            )
        if database.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise NoaaErrCompletionIntegrityError(
                "legacy NOAA output integrity check failed"
            )
        for row in database.execute("SELECT * FROM completion_outputs"):
            self._from_row(row)

    def _is_legacy_v1(self, database: sqlite3.Connection) -> bool:
        if self._tables(database) != {"completion_outputs", "output_metadata"}:
            return False
        if self._column_definition(
            database,
            "output_metadata",
        ) != self._legacy_metadata_definition():
            return False
        metadata = database.execute(
            "SELECT contract FROM output_metadata WHERE singleton=1"
        ).fetchone()
        return bool(
            metadata is not None
            and str(metadata["contract"]) == LEGACY_OUTPUT_STORE_CONTRACT
        )

    def _migrate_v1_to_v2(self, database: sqlite3.Connection) -> None:
        self._validate_legacy_v1_schema(database)
        database.execute(
            "ALTER TABLE output_metadata ADD COLUMN schema_version INTEGER"
        )
        database.execute(
            "ALTER TABLE output_metadata ADD COLUMN store_instance_sha256 TEXT"
        )
        database.execute(
            "UPDATE output_metadata SET contract=?,schema_version=?,"
            "store_instance_sha256=? WHERE singleton=1",
            (
                OUTPUT_STORE_CONTRACT,
                OUTPUT_STORE_SCHEMA_VERSION,
                secrets.token_hex(32),
            ),
        )
        self._fault("migration.after_metadata")
        for name, statement in self._trigger_definitions().items():
            if name.startswith("output_metadata_"):
                database.execute(statement)
        self._fault("migration.after_triggers")

    def _validate_schema(
        self,
        database: sqlite3.Connection,
        *,
        full: bool = False,
    ) -> str:
        if self._tables(database) != {"completion_outputs", "output_metadata"}:
            raise NoaaErrCompletionIntegrityError("NOAA output store objects differ")
        if self._column_definition(
            database,
            "output_metadata",
        ) != self._metadata_definition():
            raise NoaaErrCompletionIntegrityError(
                "NOAA output metadata columns differ"
            )
        if self._column_definition(
            database,
            "completion_outputs",
        ) != self._output_definition():
            raise NoaaErrCompletionIntegrityError("NOAA output columns differ")
        self._validate_unique_indexes(database)
        self._validate_immutability_triggers(database)
        metadata = database.execute(
            "SELECT * FROM output_metadata WHERE singleton=1"
        ).fetchone()
        if (
            metadata is None
            or int(
                database.execute(
                    "SELECT count(*) FROM output_metadata"
                ).fetchone()[0]
            )
            != 1
            or str(metadata["contract"]) != OUTPUT_STORE_CONTRACT
            or str(metadata["limits_sha256"]) != self.limits.fingerprint
            or int(metadata["schema_version"]) != OUTPUT_STORE_SCHEMA_VERSION
        ):
            raise NoaaErrCompletionIntegrityError(
                "NOAA output store metadata differs"
            )
        instance_sha = _sha(
            metadata["store_instance_sha256"],
            "NOAA output store instance SHA-256",
        )
        if hasattr(self, "_store_instance_sha256") and not hmac.compare_digest(
            self._store_instance_sha256,
            instance_sha,
        ):
            raise NoaaErrCompletionIntegrityError(
                "NOAA output store instance authority changed"
            )
        count = int(
            database.execute("SELECT count(*) FROM completion_outputs").fetchone()[0]
        )
        if count > self.limits.max_receipts:
            raise NoaaErrCompletionCapacityError(
                "NOAA output receipt cap exceeded"
            )
        if full and database.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise NoaaErrCompletionIntegrityError(
                "NOAA output store integrity check failed"
            )
        if full:
            for row in database.execute("SELECT * FROM completion_outputs"):
                self._from_row(row)
        return instance_sha

    def initialize(self) -> None:
        with self._lock:
            self._database_witness = self._prepare_path()
            try:
                with closing(self._connect()) as database:
                    try:
                        database.execute("BEGIN IMMEDIATE")
                        if not self._tables(database):
                            self._create_schema(database)
                        elif self._is_legacy_v1(database):
                            self._migrate_v1_to_v2(database)
                        self._validate_schema(database)
                        self._fault("migration.before_commit")
                        database.commit()
                        self._fault("migration.after_commit")
                    except BaseException:
                        if database.in_transaction:
                            database.rollback()
                        raise
                with closing(self._connect()) as database:
                    self._store_instance_sha256 = self._validate_schema(
                        database,
                        full=True,
                    )
            except sqlite3.DatabaseError as error:
                raise NoaaErrCompletionIntegrityError(
                    "NOAA output receipt database is invalid"
                ) from error

    def _identity_sha256(self, instance_sha256: str) -> str:
        path_text = self._normalized_path(self.path)
        return _digest(
            {
                "contract": OUTPUT_STORE_CONTRACT,
                "file_witness": {
                    "st_dev": self._database_witness[0],
                    "st_ino": self._database_witness[1],
                    "st_mode": self._database_witness[2],
                },
                "limits_sha256": self.limits.fingerprint,
                "path_sha256": hashlib.sha256(
                    path_text.encode("utf-8")
                ).hexdigest(),
                "schema": OUTPUT_STORE_IDENTITY_SCHEMA,
                "schema_version": OUTPUT_STORE_SCHEMA_VERSION,
                "store_instance_sha256": instance_sha256,
            }
        )

    @property
    def store_instance_sha256(self) -> str:
        with self._lock, closing(self._connect()) as database:
            return self._validate_schema(database)

    @property
    def identity_sha256(self) -> str:
        with self._lock, closing(self._connect()) as database:
            instance_sha = self._validate_schema(database)
            return self._identity_sha256(instance_sha)

    def assert_instance_held(
        self,
        expected_identity_sha256: str | None = None,
    ) -> str:
        """Reprove the visible file, live sentinel and complete bounded store."""

        with self._lock, closing(self._connect()) as database:
            instance_sha = self._validate_schema(database, full=True)
            identity = self._identity_sha256(instance_sha)
        if expected_identity_sha256 is not None and not hmac.compare_digest(
            identity,
            _sha(
                expected_identity_sha256,
                "expected NOAA output store identity SHA-256",
            ),
        ):
            raise NoaaErrCompletionIntegrityError(
                "NOAA output store identity authority differs"
            )
        return identity

    @staticmethod
    def _receipt_hashes(
        document: Mapping[str, Any],
        *,
        created_at: float,
    ) -> tuple[str, str, str]:
        output_sha = hashlib.sha256(_canonical(document)).hexdigest()
        receipt_sha = _digest(
            {
                "created_at": created_at,
                "output_sha256": output_sha,
                "schema": OUTPUT_RECEIPT_SCHEMA,
            }
        )
        isolation = document["process_isolation"]
        process_sha = _digest(
            {
                "created_at": created_at,
                "effect_contract_sha256": isolation["effect_contract_sha256"],
                "event_id": document["event"]["event_id"],
                "isolation_authority_sha256": isolation["authority_sha256"],
                "limits_sha256": isolation["limits_sha256"],
                "output_receipt_sha256": receipt_sha,
                "output_sha256": output_sha,
                "schema": PROCESS_RECEIPT_SCHEMA,
            }
        )
        return output_sha, receipt_sha, process_sha

    def _from_row(self, row: sqlite3.Row) -> NoaaErrCompletionOutputReceipt:
        body = bytes(row["output_json"])
        if (
            len(body) != int(row["output_bytes"])
            or not 0 < len(body) <= self.limits.max_receipt_bytes
        ):
            raise NoaaErrCompletionCapacityError("stored NOAA output exceeds cap")
        try:
            document = _validated_output(json.loads(body))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise NoaaErrCompletionIntegrityError("stored NOAA output JSON is corrupt") from error
        if _canonical(document) != body:
            raise NoaaErrCompletionIntegrityError("stored NOAA output is not canonical")
        created_at = _timestamp(row["created_at"], "output created_at")
        output_sha, receipt_sha, process_sha = self._receipt_hashes(document, created_at=created_at)
        if (
            row["event_id"] != document["event"]["event_id"]
            or row["output_sha256"] != output_sha
            or row["receipt_sha256"] != receipt_sha
            or row["process_receipt_sha256"] != process_sha
        ):
            raise NoaaErrCompletionIntegrityError("stored NOAA output receipt checksum differs")
        region = NoaaErrRegionAuthority.from_mapping(document["region_authority"])
        return NoaaErrCompletionOutputReceipt(
            receipt_sha256=receipt_sha,
            process_receipt_sha256=process_sha,
            output_sha256=output_sha,
            event_id=str(document["event"]["event_id"]),
            event_sha256=str(document["event"]["event_sha256"]),
            producer_receipt_sha256=str(document["event"]["producer_receipt_sha256"]),
            desired_state_sha256=str(document["authority"]["desired_state_sha256"]),
            desired_authority_sha256=str(document["authority"]["desired_authority_sha256"]),
            region_authority_sha256=region.fingerprint,
            isolation_authority_sha256=str(document["process_isolation"]["authority_sha256"]),
            isolation_limits_sha256=str(document["process_isolation"]["limits_sha256"]),
            candidate_content_sha256=str(document["shadow"]["content_sha256"]),
            decoder_run_id=str(document["decoder"]["run_id"]),
            created_at=created_at,
            output=document,
        )

    def load(self, event_id: str) -> NoaaErrCompletionOutputReceipt | None:
        event = _identifier(event_id, "output event ID")
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT * FROM completion_outputs WHERE event_id=?", (event,)
            ).fetchone()
        return None if row is None else self._from_row(row)

    def commit(
        self,
        output: Mapping[str, Any],
    ) -> NoaaErrCompletionOutputReceipt:
        document = _validated_output(output)
        body = _canonical(document)
        if not 0 < len(body) <= self.limits.max_receipt_bytes:
            raise NoaaErrCompletionCapacityError("NOAA output exceeds byte cap")
        event_id = str(document["event"]["event_id"])
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                existing = database.execute(
                    "SELECT * FROM completion_outputs WHERE event_id=?",
                    (event_id,),
                ).fetchone()
                if existing is not None:
                    receipt = self._from_row(existing)
                    if receipt.output_sha256 != hashlib.sha256(body).hexdigest():
                        raise NoaaErrCompletionConflict("NOAA event already has a different output")
                    database.commit()
                    return receipt
                count = int(
                    database.execute("SELECT count(*) FROM completion_outputs").fetchone()[0]
                )
                if count >= self.limits.max_receipts:
                    raise NoaaErrCompletionCapacityError("NOAA output receipt cap reached")
                created_at = _timestamp(self._clock(), "output store clock")
                output_sha, receipt_sha, process_sha = self._receipt_hashes(
                    document, created_at=created_at
                )
                database.execute(
                    "INSERT INTO completion_outputs VALUES(?,?,?,?,?,?,?)",
                    (
                        event_id,
                        body,
                        len(body),
                        output_sha,
                        receipt_sha,
                        process_sha,
                        created_at,
                    ),
                )
                self._fault("output.before_commit")
                database.commit()
                self._fault("output.after_commit")
            except sqlite3.DatabaseError as error:
                if database.in_transaction:
                    database.rollback()
                if "full" in str(error).casefold():
                    raise NoaaErrCompletionCapacityError("NOAA output database is full") from error
                raise NoaaErrCompletionIntegrityError("NOAA output database failed") from error
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        receipt = self.load(event_id)
        if receipt is None:
            raise NoaaErrCompletionIntegrityError("NOAA output disappeared")
        return receipt

    @staticmethod
    def _verify_loaded_for_ack(
        receipt: NoaaErrCompletionOutputReceipt,
        *,
        lease: CompletionLease,
        region_authority: NoaaErrRegionAuthority,
        isolation_authority_sha256: str,
        isolation_limits_sha256: str,
        output_sha256: str,
        process_receipt_sha256: str,
    ) -> NoaaErrCompletionOutputReceipt:
        if not isinstance(receipt, NoaaErrCompletionOutputReceipt):
            raise TypeError("receipt must be NoaaErrCompletionOutputReceipt")
        if not isinstance(lease, CompletionLease):
            raise TypeError("lease must be CompletionLease")
        if not isinstance(region_authority, NoaaErrRegionAuthority):
            raise TypeError("region_authority must be NoaaErrRegionAuthority")
        checks = (
            (receipt.event_sha256, lease.event_sha256),
            (receipt.producer_receipt_sha256, lease.producer_receipt_sha256),
            (receipt.desired_state_sha256, lease.desired_state_sha256),
            (receipt.desired_authority_sha256, lease.authority_sha256),
            (receipt.region_authority_sha256, region_authority.fingerprint),
            (receipt.isolation_authority_sha256, isolation_authority_sha256),
            (receipt.isolation_limits_sha256, isolation_limits_sha256),
            (receipt.output_sha256, output_sha256),
            (receipt.process_receipt_sha256, process_receipt_sha256),
        )
        if any(actual != expected for actual, expected in checks):
            raise NoaaErrCompletionConflict(
                "NOAA acknowledgement proof differs from durable output"
            )
        return receipt

    def verify_for_ack(
        self,
        *,
        lease: CompletionLease,
        region_authority: NoaaErrRegionAuthority,
        isolation_authority_sha256: str,
        isolation_limits_sha256: str,
        output_sha256: str,
        process_receipt_sha256: str,
    ) -> NoaaErrCompletionOutputReceipt:
        receipt = self.load(lease.event_id)
        if receipt is None:
            raise NoaaErrCompletionIntegrityError(
                "NOAA durable output is unavailable"
            )
        return self._verify_loaded_for_ack(
            receipt,
            lease=lease,
            region_authority=region_authority,
            isolation_authority_sha256=isolation_authority_sha256,
            isolation_limits_sha256=isolation_limits_sha256,
            output_sha256=output_sha256,
            process_receipt_sha256=process_receipt_sha256,
        )

    @contextmanager
    def completion_reproof_lease(
        self,
        *,
        lease: CompletionLease,
        region_authority: NoaaErrRegionAuthority,
        isolation_authority_sha256: str,
        isolation_limits_sha256: str,
        output_sha256: str,
        process_receipt_sha256: str,
        expected_store_identity_sha256: str,
    ) -> Iterator[NoaaErrCompletionOutputReceipt]:
        """Fence the exact output row and store instance through lifecycle ACK.

        ``BEGIN IMMEDIATE`` excludes SQLite DDL/DML writers from the final
        checksum reproof until the caller leaves the context after ACK.  The
        persistent sentinel and constructor-bound identity also detect a
        same-inode reset before entry.  POSIX can still rename a different
        inode over an open database path between two Python instructions;
        deployment therefore retains the explicit exclusive-parent/no-rename
        OS-policy blocker for this instruction-level residual.
        """

        expected_identity = _sha(
            expected_store_identity_sha256,
            "expected NOAA output store identity SHA-256",
        )
        event = _identifier(lease.event_id, "output event ID")
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                instance_sha = self._validate_schema(database, full=True)
                if not hmac.compare_digest(
                    self._identity_sha256(instance_sha),
                    expected_identity,
                ):
                    raise NoaaErrCompletionIntegrityError(
                        "NOAA output store identity authority differs"
                    )
                row = database.execute(
                    "SELECT * FROM completion_outputs WHERE event_id=?",
                    (event,),
                ).fetchone()
                if row is None:
                    raise NoaaErrCompletionIntegrityError(
                        "NOAA durable output is unavailable"
                    )
                receipt = self._from_row(row)
                verified = self._verify_loaded_for_ack(
                    receipt,
                    lease=lease,
                    region_authority=region_authority,
                    isolation_authority_sha256=isolation_authority_sha256,
                    isolation_limits_sha256=isolation_limits_sha256,
                    output_sha256=output_sha256,
                    process_receipt_sha256=process_receipt_sha256,
                )
                yield verified
                self._verify_path()
                final_instance = self._validate_schema(database)
                if not hmac.compare_digest(final_instance, instance_sha):
                    raise NoaaErrCompletionIntegrityError(
                        "NOAA output store instance changed while fenced"
                    )
                final_row = database.execute(
                    "SELECT * FROM completion_outputs WHERE event_id=?",
                    (event,),
                ).fetchone()
                if final_row is None or self._from_row(final_row) != receipt:
                    raise NoaaErrCompletionIntegrityError(
                        "NOAA durable output changed while reproof was fenced"
                    )
            finally:
                if database.in_transaction:
                    database.rollback()

    def diagnostics(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database)
            count, total = database.execute(
                "SELECT count(*),coalesce(sum(output_bytes),0) " "FROM completion_outputs"
            ).fetchone()
        result = {
            "contract": OUTPUT_STORE_CONTRACT,
            "healthy": True,
            "receipt_bytes": int(total),
            "receipts": int(count),
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise NoaaErrCompletionCapacityError("NOAA output diagnostics exceed cap")
        return result


@dataclass(frozen=True, slots=True)
class NoaaErrCompletionIsolationLimits:
    wall_seconds: float = 75.0
    cpu_seconds: float = 60.0
    rss_bytes: int = 1536 * 1024 * 1024
    max_request_bytes: int = 1024 * 1024
    max_result_bytes: int = 16 * 1024

    def __post_init__(self) -> None:
        try:
            self.decoder_limits()
        except (TypeError, ValueError) as error:
            raise ValueError("NOAA process limits are invalid") from error

    def to_mapping(self) -> dict[str, int | float]:
        return {
            "cpu_seconds": float(self.cpu_seconds),
            "max_request_bytes": self.max_request_bytes,
            "max_result_bytes": self.max_result_bytes,
            "rss_bytes": self.rss_bytes,
            "wall_seconds": float(self.wall_seconds),
        }

    @property
    def fingerprint(self) -> str:
        return _digest(
            {
                **self.to_mapping(),
                "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
                "schema": "stv3-noaa-err-hard-process-limits-v1",
            }
        )

    def decoder_limits(self) -> DecoderResourceLimits:
        return DecoderResourceLimits(
            wall_seconds=float(self.wall_seconds),
            cpu_seconds=float(self.cpu_seconds),
            memory_bytes=self.rss_bytes,
            max_request_bytes=self.max_request_bytes,
            max_result_bytes=self.max_result_bytes,
        )


def _effect_mapping() -> dict[str, Any]:
    return {
        "contract_id": PROCESS_EFFECT_CONTRACT.contract_id,
        "durable_effects": PROCESS_EFFECT_CONTRACT.durable_effects,
        "hard_kill_safe": PROCESS_EFFECT_CONTRACT.hard_kill_safe,
        "spawns_descendants": PROCESS_EFFECT_CONTRACT.spawns_descendants,
    }


PROCESS_EFFECT_CONTRACT_SHA256 = _digest(_effect_mapping())


def _safe_existing_path(value: object, label: str, *, directory: bool) -> Path:
    if not isinstance(value, str) or not value or "\0" in value or len(value) > 1024:
        raise NoaaErrCompletionIntegrityError(f"{label} path is invalid")
    path = Path(value)
    if not path.is_absolute() or path.is_symlink() or not path.exists():
        raise NoaaErrCompletionIntegrityError(f"{label} path is unsafe")
    resolved = path.resolve(strict=True)
    if resolved != path:
        raise NoaaErrCompletionIntegrityError(f"{label} path is not normalized")
    mode = path.lstat().st_mode
    if directory != stat.S_ISDIR(mode):
        raise NoaaErrCompletionIntegrityError(f"{label} path kind differs")
    return path


def _assert_process_store_paths_disjoint(
    *,
    raw_root: Path,
    artifact_root: Path,
    evidence_path: Path,
    output_path: Path,
) -> None:
    """Reject authority recomputation with aliased store roles."""

    if (
        raw_root.samefile(artifact_root)
        or raw_root.is_relative_to(artifact_root)
        or artifact_root.is_relative_to(raw_root)
    ):
        raise NoaaErrCompletionIntegrityError("NOAA process object-store roots overlap")
    if evidence_path.samefile(output_path):
        raise NoaaErrCompletionIntegrityError("NOAA process database paths alias")
    for database_path in (evidence_path, output_path):
        if database_path.is_relative_to(raw_root) or database_path.is_relative_to(artifact_root):
            raise NoaaErrCompletionIntegrityError("NOAA process database overlaps an object store")


@dataclass(frozen=True, slots=True)
class _ProcessAuthority:
    authority_sha256: str
    raw_root: Path
    raw_limits: RawArtifactLimits
    artifact_root: Path
    artifact_max_total_bytes: int
    evidence_path: Path
    evidence_max_database_bytes: int
    output_path: Path
    output_limits: NoaaErrCompletionOutputLimits
    isolation_limits: NoaaErrCompletionIsolationLimits


def _load_process_authority(
    value: object,
    expected_sha256: str,
) -> _ProcessAuthority:
    exact = _exact(
        value,
        {
            "consumer_id",
            "decoder_config_sha256",
            "isolation_limits",
            "schema",
            "stores",
            "stream_id",
        },
        "NOAA process authority",
    )
    expected = _sha(expected_sha256, "process authority SHA-256")
    if _digest(exact) != expected:
        raise NoaaErrCompletionIntegrityError("NOAA process authority checksum differs")
    if (
        exact["schema"] != PROCESS_AUTHORITY_SCHEMA
        or exact["consumer_id"] != CONSUMER_ID
        or exact["stream_id"] != STREAM_ID
        or exact["decoder_config_sha256"] != DECODER_CONFIG_SHA256
    ):
        raise NoaaErrCompletionIntegrityError("NOAA process route differs")
    limit_value = _exact(
        exact["isolation_limits"],
        {
            "cpu_seconds",
            "max_request_bytes",
            "max_result_bytes",
            "rss_bytes",
            "wall_seconds",
        },
        "NOAA isolation limits",
    )
    limits = NoaaErrCompletionIsolationLimits(**dict(limit_value))
    stores = _exact(
        exact["stores"],
        {
            "canonical_evidence",
            "derived_artifacts",
            "outputs",
            "source_raw",
        },
        "NOAA process stores",
    )
    raw = _exact(stores["source_raw"], {"limits", "root"}, "NOAA raw store")
    raw_limits = RawArtifactLimits(
        **dict(
            _exact(
                raw["limits"],
                set(RawArtifactLimits.__dataclass_fields__),
                "NOAA raw limits",
            )
        )
    )
    artifact = _exact(
        stores["derived_artifacts"],
        {"max_total_bytes", "root"},
        "NOAA artifact store",
    )
    evidence = _exact(
        stores["canonical_evidence"],
        {"max_database_bytes", "path"},
        "NOAA evidence store",
    )
    output = _exact(stores["outputs"], {"limits", "path"}, "NOAA output store")
    output_limits = NoaaErrCompletionOutputLimits(
        **dict(
            _exact(
                output["limits"],
                set(NoaaErrCompletionOutputLimits.__dataclass_fields__),
                "NOAA output limits",
            )
        )
    )
    artifact_cap = artifact["max_total_bytes"]
    evidence_cap = evidence["max_database_bytes"]
    for value, label in (
        (artifact_cap, "NOAA artifact byte cap"),
        (evidence_cap, "NOAA evidence byte cap"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise NoaaErrCompletionIntegrityError(f"{label} differs")
    raw_root = _safe_existing_path(raw["root"], "NOAA raw", directory=True)
    artifact_root = _safe_existing_path(artifact["root"], "NOAA artifact", directory=True)
    evidence_path = _safe_existing_path(evidence["path"], "NOAA evidence", directory=False)
    output_path = _safe_existing_path(output["path"], "NOAA output", directory=False)
    _assert_process_store_paths_disjoint(
        raw_root=raw_root,
        artifact_root=artifact_root,
        evidence_path=evidence_path,
        output_path=output_path,
    )
    return _ProcessAuthority(
        authority_sha256=expected,
        raw_root=raw_root,
        raw_limits=raw_limits,
        artifact_root=artifact_root,
        artifact_max_total_bytes=artifact_cap,
        evidence_path=evidence_path,
        evidence_max_database_bytes=evidence_cap,
        output_path=output_path,
        output_limits=output_limits,
        isolation_limits=limits,
    )


def _decoder_identity(binding: NoaaErrLeaseBinding) -> tuple[str, str]:
    identity = {
        "event_id": binding.event_id,
        "event_sha256": binding.event_sha256,
        "region_authority_sha256": binding.region_authority.fingerprint,
        "schema": "stv3-noaa-err-decoder-job-binding-v1",
    }
    digest = _digest(identity)
    return f"decoder-job-{digest}", f"decoder-attempt-{digest}"


def _execute_decoder_chain(
    *,
    binding: NoaaErrLeaseBinding,
    raw_store: RawArtifactStore,
    artifact_store: NoaaErrDerivedArtifactStore,
    evidence_store: CanonicalEvidenceStore,
    process_isolation: Mapping[str, Any],
    deadline_at: float,
) -> dict[str, Any]:
    _validate_raw_binding(binding, raw_store)
    region = NoaaErrRegionAuthority.from_mapping(binding.region_authority.to_mapping())
    window = quantitative._window_for_region(region.region)  # noqa: SLF001
    if (
        window.row_start != region.row_start
        or window.row_stop != region.row_stop
        or window.column_start != region.column_start
        or window.column_stop != region.column_stop
        or window.grid_definition_sha256 != region.grid_definition_sha256
        or window.coverage_definition_sha256 != region.coverage_definition_sha256
    ):
        raise NoaaErrCompletionIntegrityError("NOAA child region/window authority differs")
    decoder_job_id, attempt_id = _decoder_identity(binding)
    decoded = decode_noaa_err_region(
        capture_id=binding.capture_id,
        decoder_job_id=decoder_job_id,
        attempt_id=attempt_id,
        region=region.region,
        raw_store=raw_store,
        artifact_store=artifact_store,
        evidence_store=evidence_store,
        evaluated_at=binding.evaluated_at,
        monotonic_deadline=deadline_at,
        monotonic_clock=time.monotonic,
    )
    if decoded.outcome != "decoded_complete" or decoded.artifact is None:
        raise NoaaErrCompletionIntegrityError(
            f"NOAA quantitative decode failed closed: {decoded.reason_code}"
        )
    if time.monotonic() >= deadline_at:
        raise NoaaErrCompletionDeadlineError("NOAA child deadline expired")
    isolation = _exact(
        process_isolation,
        {
            "authority_sha256",
            "effect_contract_sha256",
            "limits_sha256",
            "state",
        },
        "NOAA process isolation proof",
    )
    if isolation["state"] != "enforced_before_target_start":
        raise NoaaErrCompletionIntegrityError("NOAA hard process was not enforced before decode")
    return {
        "authority": {
            "desired_authority_sha256": binding.desired_authority_sha256,
            "desired_state_sha256": binding.desired_state_sha256,
        },
        "claims": {
            "decoded_shadow_candidate": True,
            "fact_staged": False,
            "meteorological_admission_granted": False,
            "operational_snapshot_published": False,
            "raw_refetch_performed": False,
        },
        "consumer_id": CONSUMER_ID,
        "decoder": {
            "artifact_handle": decoded.artifact.handle,
            "artifact_sha256": decoded.artifact.sha256,
            "artifact_size_bytes": decoded.artifact.size_bytes,
            "attempt_id": attempt_id,
            "commit_id": decoded.evidence_receipt.commit_id,
            "config_sha256": DECODER_CONFIG_SHA256,
            "identity": DECODER_IDENTITY,
            "job_id": decoder_job_id,
            "receipt_sha256": decoded.evidence_receipt.receipt_sha256,
            "result_sha256": decoded.evidence_receipt.result_sha256,
            "run_id": decoded.evidence_receipt.run_id,
            "version": DECODER_VERSION,
        },
        "disposition": OUTPUT_DISPOSITION,
        "event": {
            "acquisition_job_id": binding.acquisition_job_id,
            "event_id": binding.event_id,
            "event_sha256": binding.event_sha256,
            "producer_receipt_sha256": binding.producer_receipt_sha256,
            "stream_sequence": binding.stream_sequence,
        },
        "fact_revision": None,
        "input": {
            "capture_id": binding.capture_id,
            "capture_manifest_sha256": binding.capture_manifest_sha256,
            "capture_receipt_sha256": binding.capture_receipt_sha256,
            "reference_content_sha256": binding.reference_content_sha256,
            "reference_size_bytes": binding.reference_size_bytes,
        },
        "process_isolation": dict(isolation),
        "region_authority": region.to_mapping(),
        "schema": OUTPUT_DOCUMENT_SCHEMA,
        "shadow": {
            "admission_status": "withheld",
            "content_sha256": decoded.artifact.sha256,
            "quantity": "instantaneous_precipitation_rate",
            "unit": "mm/h",
        },
        "stream_id": STREAM_ID,
    }


def _reprove_committed_authorities(
    *,
    binding: NoaaErrLeaseBinding,
    raw_store: RawArtifactStore,
    artifact_store: NoaaErrDerivedArtifactStore,
    evidence_store: CanonicalEvidenceStore,
    output_store: NoaaErrCompletionOutputStore,
    isolation_authority_sha256: str,
    isolation_limits_sha256: str,
) -> NoaaErrCompletionOutputReceipt:
    """Reopen raw, canonical evidence, candidate artifact and exact output."""

    _validate_raw_binding(binding, raw_store)
    receipt = output_store.load(binding.event_id)
    if receipt is None:
        raise NoaaErrCompletionIntegrityError("NOAA final reproof has no durable output")
    if (
        receipt.event_sha256 != binding.event_sha256
        or receipt.producer_receipt_sha256 != binding.producer_receipt_sha256
        or receipt.desired_state_sha256 != binding.desired_state_sha256
        or receipt.desired_authority_sha256 != binding.desired_authority_sha256
        or receipt.region_authority_sha256 != binding.region_authority.fingerprint
        or receipt.isolation_authority_sha256 != isolation_authority_sha256
        or receipt.isolation_limits_sha256 != isolation_limits_sha256
    ):
        raise NoaaErrCompletionConflict("NOAA final output authority differs from lease binding")
    decoder = receipt.output["decoder"]
    try:
        evidence = evidence_store.load(receipt.decoder_run_id)
    except (CanonicalEvidenceError, OSError, sqlite3.DatabaseError, ValueError):
        raise NoaaErrCompletionIntegrityError(
            "NOAA canonical decoder evidence failed final reproof"
        ) from None
    if evidence is None:
        raise NoaaErrCompletionIntegrityError("NOAA canonical decoder evidence is unavailable")
    run = evidence.run
    decoder_job_id, attempt_id = _decoder_identity(binding)
    region = binding.region_authority
    window = quantitative._window_for_region(region.region)  # noqa: SLF001
    candidate = run.non_operational_candidate_artifact
    checks = (
        (evidence.run_id, decoder["run_id"]),
        (evidence.commit_id, decoder["commit_id"]),
        (evidence.receipt_sha256, decoder["receipt_sha256"]),
        (evidence.result_sha256, decoder["result_sha256"]),
        (evidence.capture_manifest_sha256, binding.capture_manifest_sha256),
        (run.decoder_job_id, decoder_job_id),
        (run.decoder_job_id, decoder["job_id"]),
        (run.attempt_id, attempt_id),
        (run.attempt_id, decoder["attempt_id"]),
        (run.capture_id, binding.capture_id),
        (run.capture_receipt_sha256, binding.capture_receipt_sha256),
        (run.decoder_identity, DECODER_IDENTITY),
        (run.decoder_identity, decoder["identity"]),
        (run.decoder_version, DECODER_VERSION),
        (run.decoder_version, decoder["version"]),
        (run.config_sha256, DECODER_CONFIG_SHA256),
        (run.config_sha256, decoder["config_sha256"]),
        (run.provider, "noaa_enterprise_rainrate"),
        (run.product, native.NOAA_ERR_CAPTURE_PRODUCT),
        (run.evaluated_at, binding.evaluated_at),
        (run.outcome, "decoded_complete"),
        (run.reason_code, "decoded_complete"),
        (run.quantity, "instantaneous_precipitation_rate"),
        (run.unit, "mm/h"),
        (run.native_crs, "EPSG:4326"),
    )
    if any(actual != expected for actual, expected in checks):
        raise NoaaErrCompletionIntegrityError("NOAA output differs from canonical decoder evidence")
    if (
        run.native_grid is None
        or run.native_grid.kind != "regular"
        or run.native_grid.axis_order != "yx"
        or run.native_grid.height != region.rows
        or run.native_grid.width != region.columns
        or run.native_grid.definition_sha256 != region.grid_definition_sha256
        or run.coverage is None
        or run.coverage.kind != "native_bbox"
        or run.coverage.minimum_x != window.minimum_longitude
        or run.coverage.minimum_y != window.minimum_latitude
        or run.coverage.maximum_x != window.maximum_longitude
        or run.coverage.maximum_y != window.maximum_latitude
        or run.coverage.definition_sha256 != region.coverage_definition_sha256
        or candidate is None
        or candidate.handle != decoder["artifact_handle"]
        or candidate.sha256 != decoder["artifact_sha256"]
    ):
        raise NoaaErrCompletionIntegrityError("NOAA target-region artifact authority differs")
    artifact = NoaaErrArtifactRef(
        handle=decoder["artifact_handle"],
        sha256=decoder["artifact_sha256"],
        size_bytes=decoder["artifact_size_bytes"],
    )
    try:
        artifact_store.read_bytes(artifact)
    except (NoaaErrArtifactError, OSError, ValueError, TypeError):
        raise NoaaErrCompletionIntegrityError(
            "NOAA derived candidate artifact failed final reproof"
        ) from None
    return receipt


def _child_envelope(
    status: str,
    *,
    authority_sha256: str,
    code: str,
    output: NoaaErrCompletionOutputReceipt | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "authority_sha256": authority_sha256,
        "code": code,
        "schema": PROCESS_CHILD_RESULT_SCHEMA,
        "status": status,
    }
    if output is not None:
        result.update(
            {
                "event_id": output.event_id,
                "output_receipt_sha256": output.receipt_sha256,
                "output_sha256": output.output_sha256,
                "process_receipt_sha256": output.process_receipt_sha256,
            }
        )
    return result


def _noaa_err_completion_child_entry(request: Mapping[str, Any]) -> dict[str, Any]:
    """Spawn target returning only bounded path-free terminal diagnostics."""

    authority_sha = "0" * 64
    try:
        exact = _exact(
            request,
            {
                "authority",
                "authority_sha256",
                "binding",
                "purpose",
                "request_sha256",
                "schema",
            },
            "NOAA process request",
        )
        if exact["schema"] != PROCESS_REQUEST_SCHEMA:
            raise NoaaErrCompletionIntegrityError("NOAA request contract differs")
        body = dict(exact)
        request_sha = body.pop("request_sha256")
        if _digest(body) != _sha(request_sha, "request SHA-256"):
            raise NoaaErrCompletionIntegrityError("NOAA request checksum differs")
        authority_sha = _sha(exact["authority_sha256"], "authority SHA-256")
        authority = _load_process_authority(exact["authority"], authority_sha)
        raw_store = RawArtifactStore(authority.raw_root, limits=authority.raw_limits)
        raw_store.initialize()
        artifact_store = NoaaErrDerivedArtifactStore(
            authority.artifact_root,
            max_total_bytes=authority.artifact_max_total_bytes,
        )
        artifact_store.initialize()
        evidence_store = CanonicalEvidenceStore(
            authority.evidence_path,
            raw_artifact_reader=raw_store,
            max_database_bytes=authority.evidence_max_database_bytes,
        )
        output_store = NoaaErrCompletionOutputStore(
            authority.output_path,
            limits=authority.output_limits,
        )
        binding = NoaaErrLeaseBinding.from_mapping(exact["binding"])
        purpose = exact["purpose"]
        if purpose not in {
            PROCESS_PURPOSE_DECODE,
            PROCESS_PURPOSE_FINAL_REPROOF,
        }:
            raise NoaaErrCompletionIntegrityError("NOAA process purpose differs")
        process_isolation = {
            "authority_sha256": authority.authority_sha256,
            "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
            "limits_sha256": authority.isolation_limits.fingerprint,
            "state": "enforced_before_target_start",
        }
        if purpose == PROCESS_PURPOSE_DECODE:
            deadline_at = time.monotonic() + min(
                float(authority.isolation_limits.wall_seconds),
                quantitative.DECODE_TOTAL_DEADLINE_SECONDS,
            )
            document = _execute_decoder_chain(
                binding=binding,
                raw_store=raw_store,
                artifact_store=artifact_store,
                evidence_store=evidence_store,
                process_isolation=process_isolation,
                deadline_at=deadline_at,
            )
            output = output_store.commit(document)
            code = "hard_process_output_committed"
        else:
            output = _reprove_committed_authorities(
                binding=binding,
                raw_store=raw_store,
                artifact_store=artifact_store,
                evidence_store=evidence_store,
                output_store=output_store,
                isolation_authority_sha256=authority.authority_sha256,
                isolation_limits_sha256=authority.isolation_limits.fingerprint,
            )
            code = "hard_process_final_authorities_reproved"
        return _child_envelope(
            "success",
            authority_sha256=authority.authority_sha256,
            code=code,
            output=output,
        )
    except MemoryError:
        raise
    except OSError as error:
        if error.errno == errno.ENOMEM:
            raise
        return _child_envelope("crash", authority_sha256=authority_sha, code="child_store_failed")
    except (NoaaErrCompletionDeadlineError, NoaaErrDecodeDeadlineError):
        return _child_envelope("deadline", authority_sha256=authority_sha, code="child_deadline")
    except (
        NoaaErrCompletionCapacityError,
        NoaaErrArtifactCapacityError,
        RawArtifactCapacityError,
    ):
        return _child_envelope("resource", authority_sha256=authority_sha, code="child_capacity")
    except (
        NoaaErrCompletionConflict,
        NoaaErrCompletionIntegrityError,
        CanonicalEvidenceError,
        NoaaErrArtifactError,
        RawArtifactError,
        ValueError,
        TypeError,
    ):
        return _child_envelope(
            "corrupt_contract",
            authority_sha256=authority_sha,
            code="authority_request_region_or_store_invalid",
        )
    except Exception:
        return _child_envelope("crash", authority_sha256=authority_sha, code="noaa_child_failed")


ProcessStatus = Literal[
    "committed",
    "deadline",
    "resource",
    "cancelled",
    "busy",
    "boundary",
    "corrupt_contract",
    "crash",
]


@dataclass(frozen=True, slots=True)
class NoaaErrCompletionProcessResult:
    status: ProcessStatus
    execution_outcome: str
    detail_code: str
    authority_sha256: str
    durable_disposition: Literal["committed", "not_committed", "unknown"]
    output_receipt: NoaaErrCompletionOutputReceipt | None
    isolation_enforced: bool
    child_exit_code: int | None
    elapsed_seconds: float
    child_cpu_seconds: float | None = None
    child_peak_rss_bytes: int | None = None
    child_peak_boundary_bytes: int | None = None
    schema: str = PROCESS_RESULT_SCHEMA

    def __post_init__(self) -> None:
        if (
            self.schema != PROCESS_RESULT_SCHEMA
            or self.status
            not in {
                "committed",
                "deadline",
                "resource",
                "cancelled",
                "busy",
                "boundary",
                "corrupt_contract",
                "crash",
            }
            or self.durable_disposition
            not in {
                "committed",
                "not_committed",
                "unknown",
            }
            or not isinstance(self.authority_sha256, str)
            or _SHA.fullmatch(self.authority_sha256) is None
            or not isinstance(self.execution_outcome, str)
            or not self.execution_outcome
            or not isinstance(self.detail_code, str)
            or not self.detail_code
            or type(self.isolation_enforced) is not bool
            or isinstance(self.elapsed_seconds, bool)
            or not isinstance(self.elapsed_seconds, (int, float))
            or not math.isfinite(float(self.elapsed_seconds))
            or float(self.elapsed_seconds) < 0.0
            or (self.status == "committed") != (self.output_receipt is not None)
            or (self.output_receipt is not None) != (self.durable_disposition == "committed")
        ):
            raise NoaaErrCompletionIntegrityError("NOAA process result contract differs")

    @property
    def committed(self) -> bool:
        return self.status == "committed" and self.output_receipt is not None

    def to_mapping(self) -> dict[str, Any]:
        result = {
            "authority_sha256": self.authority_sha256,
            "child_exit_code": self.child_exit_code,
            "child_cpu_seconds": self.child_cpu_seconds,
            "child_peak_boundary_bytes": self.child_peak_boundary_bytes,
            "child_peak_rss_bytes": self.child_peak_rss_bytes,
            "detail_code": self.detail_code,
            "durable_disposition": self.durable_disposition,
            "elapsed_seconds": self.elapsed_seconds,
            "execution_outcome": self.execution_outcome,
            "isolation_enforced": self.isolation_enforced,
            "meteorological_admission_granted": False,
            "operational_snapshot_claim_generated": False,
            "output_identity": (
                None
                if self.output_receipt is None
                else {
                    "event_id": self.output_receipt.event_id,
                    "output_receipt_sha256": self.output_receipt.receipt_sha256,
                    "output_sha256": self.output_receipt.output_sha256,
                    "process_receipt_sha256": (self.output_receipt.process_receipt_sha256),
                }
            ),
            "schema": self.schema,
            "status": self.status,
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise NoaaErrCompletionCapacityError("NOAA process diagnostic exceeds cap")
        return result


def _process_status(result: DecoderProcessResult) -> ProcessStatus:
    if result.outcome == "deadline_exceeded":
        return "deadline"
    if result.outcome in {
        "cpu_limit_exceeded",
        "memory_limit_exceeded",
        "result_too_large",
    }:
        return "resource"
    if result.outcome == "cancelled":
        return "cancelled"
    if result.outcome == "busy":
        return "busy"
    if result.outcome in {
        "unsupported_platform",
        "boundary_setup_failed",
        "unsafe_effect_contract",
    }:
        return "boundary"
    if result.outcome in {"invalid_request", "result_not_json", "protocol_error"}:
        return "corrupt_contract"
    return "crash"


class NoaaErrCompletionIsolatedRunner:
    """One hard child plus durable output reconciliation on every terminal."""

    def __init__(
        self,
        *,
        raw_store: RawArtifactStore,
        artifact_store: NoaaErrDerivedArtifactStore,
        evidence_store: CanonicalEvidenceStore,
        output_store: NoaaErrCompletionOutputStore,
        limits: NoaaErrCompletionIsolationLimits = NoaaErrCompletionIsolationLimits(),
        context: multiprocessing.context.BaseContext | None = None,
        executor: object | None = None,
    ) -> None:
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store has wrong type")
        if not isinstance(artifact_store, NoaaErrDerivedArtifactStore):
            raise TypeError("artifact_store has wrong type")
        if not isinstance(evidence_store, CanonicalEvidenceStore):
            raise TypeError("evidence_store has wrong type")
        if not isinstance(output_store, NoaaErrCompletionOutputStore):
            raise TypeError("output_store has wrong type")
        if not isinstance(limits, NoaaErrCompletionIsolationLimits):
            raise TypeError("limits has wrong type")
        artifact_store.initialize()
        self.raw_store = raw_store
        self.artifact_store = artifact_store
        self.evidence_store = evidence_store
        self.output_store = output_store
        self.limits = limits
        self._authority = self._authority_mapping()
        self.authority_sha256 = _digest(self._authority)
        _load_process_authority(self._authority, self.authority_sha256)
        if executor is not None and not callable(getattr(executor, "execute", None)):
            raise TypeError("executor must expose execute()")
        self._executor = executor or HardDecoderExecutor(
            limits=limits.decoder_limits(), context=context
        )

    @property
    def minimum_lease_seconds(self) -> float:
        return 2.0 * float(self.limits.wall_seconds) + 2.0

    @property
    def initial_process_seconds(self) -> float:
        return float(self.limits.wall_seconds) + 1.0

    @property
    def final_reproof_seconds(self) -> float:
        return float(self.limits.wall_seconds) + 1.0

    def _authority_mapping(self) -> dict[str, Any]:
        for path, label, directory in (
            (self.raw_store.root, "NOAA raw", True),
            (self.artifact_store.root, "NOAA artifact", True),
            (self.evidence_store.path, "NOAA evidence", False),
            (self.output_store.path, "NOAA output", False),
        ):
            _safe_existing_path(str(path), label, directory=directory)
        return {
            "consumer_id": CONSUMER_ID,
            "decoder_config_sha256": DECODER_CONFIG_SHA256,
            "isolation_limits": self.limits.to_mapping(),
            "schema": PROCESS_AUTHORITY_SCHEMA,
            "stores": {
                "canonical_evidence": {
                    "max_database_bytes": self.evidence_store._max_database_bytes,
                    "path": str(self.evidence_store.path),
                },
                "derived_artifacts": {
                    "max_total_bytes": self.artifact_store.max_total_bytes,
                    "root": str(self.artifact_store.root),
                },
                "outputs": {
                    "limits": asdict(self.output_store.limits),
                    "path": str(self.output_store.path),
                },
                "source_raw": {
                    "limits": asdict(self.raw_store.limits),
                    "root": str(self.raw_store.root),
                },
            },
            "stream_id": STREAM_ID,
        }

    def assert_parent_binding(
        self,
        *,
        raw_store: RawArtifactStore,
        artifact_store: NoaaErrDerivedArtifactStore,
        evidence_store: CanonicalEvidenceStore,
        output_store: NoaaErrCompletionOutputStore,
    ) -> None:
        if (
            raw_store.root != self.raw_store.root
            or artifact_store.root != self.artifact_store.root
            or evidence_store.path != self.evidence_store.path
            or output_store.path != self.output_store.path
            or self._authority_mapping() != self._authority
        ):
            raise NoaaErrCompletionConflict("NOAA isolated runner differs from parent stores")

    def _load_exact(
        self,
        binding: NoaaErrLeaseBinding,
    ) -> NoaaErrCompletionOutputReceipt | None:
        receipt = self.output_store.load(binding.event_id)
        if receipt is None:
            return None
        if (
            receipt.event_sha256 != binding.event_sha256
            or receipt.producer_receipt_sha256 != binding.producer_receipt_sha256
            or receipt.desired_state_sha256 != binding.desired_state_sha256
            or receipt.desired_authority_sha256 != binding.desired_authority_sha256
            or receipt.region_authority_sha256 != binding.region_authority.fingerprint
            or receipt.isolation_authority_sha256 != self.authority_sha256
            or receipt.isolation_limits_sha256 != self.limits.fingerprint
            or receipt.output["input"]
            != {
                "capture_id": binding.capture_id,
                "capture_manifest_sha256": binding.capture_manifest_sha256,
                "capture_receipt_sha256": binding.capture_receipt_sha256,
                "reference_content_sha256": binding.reference_content_sha256,
                "reference_size_bytes": binding.reference_size_bytes,
            }
            or receipt.output["event"]["stream_sequence"] != binding.stream_sequence
            or receipt.output["event"]["acquisition_job_id"] != binding.acquisition_job_id
        ):
            raise NoaaErrCompletionConflict(
                "NOAA durable isolated output differs from lease binding"
            )
        return receipt

    def _request(
        self,
        binding: NoaaErrLeaseBinding,
        *,
        purpose: str,
    ) -> dict[str, Any]:
        request_body = {
            "authority": self._authority,
            "authority_sha256": self.authority_sha256,
            "binding": binding.to_mapping(),
            "purpose": purpose,
            "schema": PROCESS_REQUEST_SCHEMA,
        }
        return {**request_body, "request_sha256": _digest(request_body)}

    def _terminal(
        self,
        *,
        status: ProcessStatus,
        execution_outcome: str,
        detail_code: str,
        started: float,
        decoder: DecoderProcessResult | None = None,
        receipt: NoaaErrCompletionOutputReceipt | None = None,
        isolation_enforced: bool = False,
        durable_unknown: bool = False,
    ) -> NoaaErrCompletionProcessResult:
        return NoaaErrCompletionProcessResult(
            status=status,
            execution_outcome=execution_outcome,
            detail_code=detail_code,
            authority_sha256=self.authority_sha256,
            durable_disposition=(
                "unknown"
                if durable_unknown
                else ("committed" if receipt is not None else "not_committed")
            ),
            output_receipt=receipt,
            isolation_enforced=isolation_enforced,
            child_exit_code=None if decoder is None else decoder.child_exit_code,
            elapsed_seconds=(
                max(0.0, time.monotonic() - started) if decoder is None else decoder.elapsed_seconds
            ),
            child_cpu_seconds=(None if decoder is None else decoder.child_cpu_seconds),
            child_peak_rss_bytes=(None if decoder is None else decoder.child_peak_rss_bytes),
            child_peak_boundary_bytes=(
                None if decoder is None else decoder.child_peak_boundary_bytes
            ),
        )

    def execute(
        self,
        *,
        binding: NoaaErrLeaseBinding,
        cancel_event: threading.Event,
    ) -> NoaaErrCompletionProcessResult:
        started = time.monotonic()
        if not isinstance(binding, NoaaErrLeaseBinding):
            raise TypeError("binding must be NoaaErrLeaseBinding")
        if not callable(getattr(cancel_event, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        existing = self._load_exact(binding)
        if existing is not None:
            return self._terminal(
                status="committed",
                execution_outcome="durable_duplicate",
                detail_code="exact_output_reconciled_before_spawn",
                started=started,
                receipt=existing,
                isolation_enforced=True,
            )
        decoder = self._executor.execute(
            target=PROCESS_TARGET,
            request=self._request(binding, purpose=PROCESS_PURPOSE_DECODE),
            effect_contract=PROCESS_EFFECT_CONTRACT,
            cancel_event=cancel_event,
        )
        isolation_enforced = decoder.outcome not in {
            "busy",
            "unsupported_platform",
            "boundary_setup_failed",
            "unsafe_effect_contract",
            "invalid_request",
        } and not (
            decoder.outcome == "cancelled" and decoder.detail_code == "cancelled_before_spawn"
        )
        durable_unknown = False
        try:
            receipt = self._load_exact(binding)
        except (NoaaErrCompletionConflict, NoaaErrCompletionIntegrityError):
            receipt = None
            durable_unknown = True
        if receipt is not None:
            return self._terminal(
                status="committed",
                execution_outcome=decoder.outcome,
                detail_code="durable_output_reconciled_after_child",
                started=started,
                decoder=decoder,
                receipt=receipt,
                isolation_enforced=True,
            )
        if decoder.outcome != "succeeded":
            status = _process_status(decoder)
            return self._terminal(
                status=status,
                execution_outcome=decoder.outcome,
                detail_code={
                    "deadline": "hard_parent_wall_deadline",
                    "resource": "hard_cpu_rss_or_output_limit",
                    "cancelled": "parent_cancelled_and_tree_killed",
                    "busy": "global_hard_process_slot_busy",
                    "boundary": "kernel_boundary_unavailable",
                    "corrupt_contract": "child_protocol_or_request_invalid",
                    "crash": "child_process_crashed",
                    "committed": "durable_output_committed",
                }[status],
                started=started,
                decoder=decoder,
                isolation_enforced=isolation_enforced,
                durable_unknown=durable_unknown,
            )
        envelope = decoder.result
        if (
            not isinstance(envelope, dict)
            or envelope.get("schema") != PROCESS_CHILD_RESULT_SCHEMA
            or envelope.get("authority_sha256") != self.authority_sha256
        ):
            return self._terminal(
                status="corrupt_contract",
                execution_outcome="succeeded",
                detail_code="child_envelope_invalid_without_output",
                started=started,
                decoder=decoder,
                isolation_enforced=isolation_enforced,
                durable_unknown=durable_unknown,
            )
        status = {
            "deadline": "deadline",
            "resource": "resource",
            "corrupt_contract": "corrupt_contract",
        }.get(envelope.get("status"), "crash")
        return self._terminal(
            status=status,  # type: ignore[arg-type]
            execution_outcome="succeeded",
            detail_code=f"child_{status}_without_durable_output",
            started=started,
            decoder=decoder,
            isolation_enforced=isolation_enforced,
            durable_unknown=durable_unknown,
        )

    def reprove_for_ack(
        self,
        *,
        binding: NoaaErrLeaseBinding,
        cancel_event: threading.Event,
    ) -> NoaaErrCompletionProcessResult:
        """Require a fresh hard-child raw/evidence/artifact/output reproof."""

        started = time.monotonic()
        if not isinstance(binding, NoaaErrLeaseBinding):
            raise TypeError("binding must be NoaaErrLeaseBinding")
        if not callable(getattr(cancel_event, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        decoder = self._executor.execute(
            target=PROCESS_TARGET,
            request=self._request(
                binding,
                purpose=PROCESS_PURPOSE_FINAL_REPROOF,
            ),
            effect_contract=PROCESS_EFFECT_CONTRACT,
            cancel_event=cancel_event,
        )
        isolation_enforced = decoder.outcome not in {
            "busy",
            "unsupported_platform",
            "boundary_setup_failed",
            "unsafe_effect_contract",
            "invalid_request",
        } and not (
            decoder.outcome == "cancelled" and decoder.detail_code == "cancelled_before_spawn"
        )
        if decoder.outcome != "succeeded":
            status = _process_status(decoder)
            return self._terminal(
                status=status,
                execution_outcome=decoder.outcome,
                detail_code=f"final_reproof_{status}",
                started=started,
                decoder=decoder,
                isolation_enforced=isolation_enforced,
            )
        envelope = decoder.result
        expected_keys = {
            "authority_sha256",
            "code",
            "event_id",
            "output_receipt_sha256",
            "output_sha256",
            "process_receipt_sha256",
            "schema",
            "status",
        }
        if not isinstance(envelope, Mapping) or set(envelope) != expected_keys:
            return self._terminal(
                status="corrupt_contract",
                execution_outcome="succeeded",
                detail_code="final_reproof_envelope_shape_invalid",
                started=started,
                decoder=decoder,
                isolation_enforced=isolation_enforced,
            )
        receipt = self._load_exact(binding)
        if (
            receipt is None
            or envelope["schema"] != PROCESS_CHILD_RESULT_SCHEMA
            or envelope["status"] != "success"
            or envelope["code"] != "hard_process_final_authorities_reproved"
            or envelope["authority_sha256"] != self.authority_sha256
            or envelope["event_id"] != receipt.event_id
            or envelope["output_sha256"] != receipt.output_sha256
            or envelope["output_receipt_sha256"] != receipt.receipt_sha256
            or envelope["process_receipt_sha256"] != receipt.process_receipt_sha256
        ):
            return self._terminal(
                status="corrupt_contract",
                execution_outcome="succeeded",
                detail_code="final_reproof_envelope_identity_invalid",
                started=started,
                decoder=decoder,
                isolation_enforced=isolation_enforced,
            )
        return self._terminal(
            status="committed",
            execution_outcome="succeeded",
            detail_code="final_authorities_reproved_before_ack",
            started=started,
            decoder=decoder,
            receipt=receipt,
            isolation_enforced=True,
        )

    @staticmethod
    def raise_for_terminal(result: NoaaErrCompletionProcessResult) -> None:
        if result.committed:
            return
        if result.status == "deadline":
            raise NoaaErrCompletionDeadlineError(result.detail_code)
        if result.status == "cancelled":
            raise NoaaErrCompletionCancelled(result.detail_code)
        if result.status == "resource":
            raise NoaaErrCompletionCapacityError(result.detail_code)
        if result.status == "corrupt_contract" or result.durable_disposition == "unknown":
            raise NoaaErrCompletionIntegrityError(result.detail_code)
        raise NoaaErrCompletionProcessError(result.detail_code)


@dataclass(frozen=True, slots=True)
class NoaaErrCompletionRunResult:
    disposition: str
    event_id: str | None
    output_receipt: NoaaErrCompletionOutputReceipt | None
    consumption_receipt: ConsumptionReceipt | None
    reason_code: str | None = None
    phase_child_invocations: int = 0


@dataclass(frozen=True, slots=True)
class NoaaErrCompletionPendingAck:
    """In-memory continuation for the separately scheduled final reproof.

    The lease token is intentionally held only inside the non-serializable
    ``CompletionLease`` object.  ``identity_mapping`` is the complete public
    continuation identity and therefore cannot leak or persist that token.
    """

    lease: CompletionLease = field(repr=False)
    desired_state: DesiredState = field(repr=False)
    binding: NoaaErrLeaseBinding = field(repr=False)
    outcome: PipelineOutcome
    output_receipt: NoaaErrCompletionOutputReceipt
    recovered_output: bool
    initial_child_invocations: int
    pending_sha256: str

    @classmethod
    def create(
        cls,
        *,
        lease: CompletionLease,
        desired_state: DesiredState,
        binding: NoaaErrLeaseBinding,
        outcome: PipelineOutcome,
        output_receipt: NoaaErrCompletionOutputReceipt,
        recovered_output: bool,
        initial_child_invocations: int,
    ) -> "NoaaErrCompletionPendingAck":
        provisional = cls(
            lease=lease,
            desired_state=desired_state,
            binding=binding,
            outcome=outcome,
            output_receipt=output_receipt,
            recovered_output=recovered_output,
            initial_child_invocations=initial_child_invocations,
            pending_sha256="0" * 64,
        )
        result = cls(
            lease=lease,
            desired_state=desired_state,
            binding=binding,
            outcome=outcome,
            output_receipt=output_receipt,
            recovered_output=recovered_output,
            initial_child_invocations=initial_child_invocations,
            pending_sha256=_digest(provisional.identity_mapping()),
        )
        result.validate()
        return result

    def identity_mapping(self) -> dict[str, Any]:
        return {
            "binding_sha256": _digest(self.binding.to_mapping()),
            "desired_state_sha256": self.desired_state.state_sha256,
            "event_sha256": self.lease.event_sha256,
            "initial_child_invocations": self.initial_child_invocations,
            "isolation_authority_sha256": self.output_receipt.isolation_authority_sha256,
            "isolation_limits_sha256": self.output_receipt.isolation_limits_sha256,
            "lease_expires_at": self.lease.lease_expires_at,
            "output_receipt_sha256": self.output_receipt.receipt_sha256,
            "output_sha256": self.outcome.output_sha256,
            "process_receipt_sha256": self.outcome.process_receipt_sha256,
            "producer_receipt_sha256": self.lease.producer_receipt_sha256,
            "recovered_output": self.recovered_output,
            "schema": PENDING_ACK_SCHEMA,
            "stream_sequence": self.lease.stream_sequence,
        }

    def validate(self) -> None:
        if (
            not isinstance(self.lease, CompletionLease)
            or not isinstance(self.desired_state, DesiredState)
            or not isinstance(self.binding, NoaaErrLeaseBinding)
            or not isinstance(self.outcome, PipelineOutcome)
            or not isinstance(self.output_receipt, NoaaErrCompletionOutputReceipt)
            or not isinstance(self.recovered_output, bool)
            or self.initial_child_invocations not in {0, 1}
            or self.initial_child_invocations != int(not self.recovered_output)
            or _SHA.fullmatch(self.pending_sha256) is None
            or _digest(self.identity_mapping()) != self.pending_sha256
            or self.lease.event_id != self.binding.event_id
            or self.lease.event_sha256 != self.binding.event_sha256
            or self.lease.desired_state_sha256 != self.desired_state.state_sha256
            or self.output_receipt.event_id != self.lease.event_id
            or self.output_receipt.output_sha256 != self.outcome.output_sha256
            or self.output_receipt.process_receipt_sha256
            != self.outcome.process_receipt_sha256
        ):
            raise NoaaErrCompletionIntegrityError(
                "NOAA pending acknowledgement identity differs"
            )


def _assert_consumer_store_paths_disjoint(
    *,
    lifecycle_store: AcquisitionEvidenceLifecycleStore,
    desired_state_store: DesiredStateStore,
    process_runner: NoaaErrCompletionIsolatedRunner,
) -> None:
    lifecycle_root = _safe_existing_path(
        str(lifecycle_store.root), "NOAA lifecycle", directory=True
    )
    lifecycle_path = _safe_existing_path(
        str(lifecycle_store.path), "NOAA lifecycle DB", directory=False
    )
    desired_path = _safe_existing_path(
        str(desired_state_store.path), "NOAA desired DB", directory=False
    )
    raw_root = process_runner.raw_store.root
    artifact_root = process_runner.artifact_store.root
    for object_root in (raw_root, artifact_root):
        if (
            object_root.samefile(lifecycle_root)
            or object_root.is_relative_to(lifecycle_root)
            or lifecycle_root.is_relative_to(object_root)
        ):
            raise NoaaErrCompletionIntegrityError(
                "NOAA lifecycle and process object-store roots overlap"
            )
    databases = (
        lifecycle_path,
        desired_path,
        process_runner.evidence_store.path,
        process_runner.output_store.path,
    )
    for index, left in enumerate(databases):
        for right in databases[index + 1 :]:
            if left.samefile(right):
                raise NoaaErrCompletionIntegrityError("NOAA consumer database store paths alias")
    for database_path in databases[1:]:
        if any(
            database_path.is_relative_to(root) for root in (lifecycle_root, raw_root, artifact_root)
        ):
            raise NoaaErrCompletionIntegrityError("NOAA consumer database overlaps an object store")


class NoaaErrCompletionConsumer:
    """One serial lifecycle consumer tick with no secondary queue."""

    def __init__(
        self,
        *,
        lifecycle_store: AcquisitionEvidenceLifecycleStore,
        desired_state_store: DesiredStateStore,
        raw_store: RawArtifactStore,
        artifact_store: NoaaErrDerivedArtifactStore,
        evidence_store: CanonicalEvidenceStore,
        output_store: NoaaErrCompletionOutputStore,
        process_runner: NoaaErrCompletionIsolatedRunner,
        region_policy: NoaaErrRegionPolicy = NoaaErrRegionPolicy(),
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        if not isinstance(lifecycle_store, AcquisitionEvidenceLifecycleStore):
            raise TypeError("lifecycle_store has wrong type")
        if not isinstance(desired_state_store, DesiredStateStore):
            raise TypeError("desired_state_store has wrong type")
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store has wrong type")
        if not isinstance(artifact_store, NoaaErrDerivedArtifactStore):
            raise TypeError("artifact_store has wrong type")
        if not isinstance(evidence_store, CanonicalEvidenceStore):
            raise TypeError("evidence_store has wrong type")
        if not isinstance(output_store, NoaaErrCompletionOutputStore):
            raise TypeError("output_store has wrong type")
        if not isinstance(process_runner, NoaaErrCompletionIsolatedRunner):
            raise TypeError("process_runner has wrong type")
        if not isinstance(region_policy, NoaaErrRegionPolicy):
            raise TypeError("region_policy has wrong type")
        process_runner.assert_parent_binding(
            raw_store=raw_store,
            artifact_store=artifact_store,
            evidence_store=evidence_store,
            output_store=output_store,
        )
        _assert_consumer_store_paths_disjoint(
            lifecycle_store=lifecycle_store,
            desired_state_store=desired_state_store,
            process_runner=process_runner,
        )
        output_store_identity_sha256 = output_store.assert_instance_held()
        self.lifecycle_store = lifecycle_store
        self.desired_state_store = desired_state_store
        self.raw_store = raw_store
        self.artifact_store = artifact_store
        self.evidence_store = evidence_store
        self.output_store = output_store
        self._output_store_identity_sha256 = output_store_identity_sha256
        self.process_runner = process_runner
        self.region_policy = region_policy
        self._clock = clock
        self._fault_injector = fault_injector

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    @staticmethod
    def _validate_consumption_receipt(
        receipt: ConsumptionReceipt,
        lease: CompletionLease,
        *,
        expected_output_sha256: str | None,
    ) -> None:
        if not isinstance(receipt, ConsumptionReceipt):
            raise NoaaErrCompletionIntegrityError(
                "NOAA lifecycle terminalization returned no receipt"
            )
        document = receipt.as_dict()
        receipt_sha = document.pop("receipt_sha256", None)
        if (
            set(document)
            != {
                "consumer_id",
                "created_at",
                "event_id",
                "event_sha256",
                "output_sha256",
                "producer_receipt_sha256",
                "schema",
                "stream_id",
                "stream_sequence",
            }
            or document["schema"] != CONSUMPTION_RECEIPT_SCHEMA
            or receipt.event_id != lease.event_id
            or receipt.event_sha256 != lease.event_sha256
            or receipt.producer_receipt_sha256 != lease.producer_receipt_sha256
            or receipt.consumer_id != lease.consumer_id
            or receipt.stream_id != lease.stream_id
            or receipt.stream_sequence != lease.stream_sequence
            or _SHA.fullmatch(receipt.output_sha256) is None
            or _SHA.fullmatch(receipt.receipt_sha256) is None
            or receipt_sha != receipt.receipt_sha256
            or _digest(document) != receipt.receipt_sha256
            or (
                expected_output_sha256 is not None
                and receipt.output_sha256 != expected_output_sha256
            )
        ):
            raise NoaaErrCompletionIntegrityError("NOAA lifecycle terminal receipt differs")
        _timestamp(receipt.created_at, "NOAA consumption receipt clock")

    def _reject_obsolete(
        self,
        lease: CompletionLease,
        *,
        active_desired: DesiredState | None,
        reason_code: str,
        now: float,
        phase_child_invocations: int = 0,
    ) -> NoaaErrCompletionRunResult:
        rejection_reason = (
            "active_desired_state_unavailable" if active_desired is None else reason_code
        )
        receipt = self.lifecycle_store.reject_leased(
            lease.event_id,
            lease.lease_token,
            expected_event_sha256=lease.event_sha256,
            reason_code=rejection_reason,
            active_desired_state_sha256=(
                None if active_desired is None else active_desired.state_sha256
            ),
            now=now,
        )
        self._validate_consumption_receipt(
            receipt,
            lease,
            expected_output_sha256=None,
        )
        return NoaaErrCompletionRunResult(
            "rejected",
            lease.event_id,
            None,
            receipt,
            rejection_reason,
            phase_child_invocations,
        )

    def process_leased(
        self,
        lease: CompletionLease,
        desired_state: DesiredState,
        cancel_event: threading.Event,
    ) -> PipelineOutcome:
        outcome, _diagnostic = self.process_leased_with_diagnostics(
            lease, desired_state, cancel_event
        )
        return outcome

    def process_leased_with_diagnostics(
        self,
        lease: CompletionLease,
        desired_state: DesiredState,
        cancel_event: threading.Event,
    ) -> tuple[PipelineOutcome, Mapping[str, Any]]:
        if not isinstance(lease, CompletionLease):
            raise TypeError("lease must be CompletionLease")
        if not isinstance(desired_state, DesiredState):
            raise TypeError("desired_state must be DesiredState")
        if not callable(getattr(cancel_event, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        if cancel_event.is_set():
            raise NoaaErrCompletionCancelled("NOAA completion was cancelled")
        reloaded = self.lifecycle_store.load_leased(lease.event_id, lease.lease_token)
        if reloaded != lease:
            raise NoaaErrCompletionConflict("NOAA lifecycle lease changed before processing")
        active = self.desired_state_store.load_active()
        if active is None or active.state_sha256 != desired_state.state_sha256:
            raise NoaaErrCompletionConflict("NOAA supplied desired state is not active")
        now = _timestamp(self._clock(), "NOAA consumer clock")
        remaining = lease.lease_expires_at - now
        if remaining <= self.process_runner.initial_process_seconds:
            raise NoaaErrCompletionDeadlineError("NOAA lease is shorter than hard-process boundary")
        binding = _validate_router_lease(lease, desired_state, self.raw_store, self.region_policy)
        isolated = self.process_runner.execute(
            binding=binding,
            cancel_event=cancel_event,
        )
        self.process_runner.raise_for_terminal(isolated)
        output = isolated.output_receipt
        if output is None:
            raise NoaaErrCompletionIntegrityError("NOAA committed process has no output receipt")
        self._fault("consumer.after_output_commit")
        if cancel_event.is_set():
            raise NoaaErrCompletionCancelled("NOAA completion was cancelled")
        if _timestamp(self._clock(), "NOAA post-process clock") >= (lease.lease_expires_at):
            raise NoaaErrCompletionDeadlineError("NOAA lease expired after durable output commit")
        verified = self.output_store.verify_for_ack(
            lease=lease,
            region_authority=binding.region_authority,
            isolation_authority_sha256=self.process_runner.authority_sha256,
            isolation_limits_sha256=self.process_runner.limits.fingerprint,
            output_sha256=output.output_sha256,
            process_receipt_sha256=output.process_receipt_sha256,
        )
        return (
            PipelineOutcome(
                output_sha256=verified.output_sha256,
                disposition=OUTPUT_DISPOSITION,
                fact_revision_staged=False,
                process_receipt_sha256=verified.process_receipt_sha256,
            ),
            isolated.to_mapping(),
        )

    def begin_leased(
        self,
        lease: CompletionLease,
        desired_state: DesiredState,
        cancel_event: threading.Event,
        *,
        before_child: Callable[[], None] | None = None,
    ) -> NoaaErrCompletionPendingAck:
        """Run at most one initial child and leave lifecycle ack for another tick.

        If an earlier attempt already committed the exact durable output, this
        method verifies and reuses it without spawning a duplicate initial
        child.  The returned continuation is intentionally non-serializable.
        """

        if not isinstance(lease, CompletionLease):
            raise TypeError("lease must be CompletionLease")
        if not isinstance(desired_state, DesiredState):
            raise TypeError("desired_state must be DesiredState")
        if not callable(getattr(cancel_event, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        if before_child is not None and not callable(before_child):
            raise TypeError("before_child must be callable")
        if cancel_event.is_set():
            raise NoaaErrCompletionCancelled("NOAA completion was cancelled")
        active = self.desired_state_store.load_active()
        if active is None or active.state_sha256 != desired_state.state_sha256:
            raise NoaaErrCompletionConflict(
                "NOAA supplied desired state is not active"
            )
        now = _timestamp(self._clock(), "NOAA begin clock")
        if lease.lease_expires_at - now <= self.process_runner.final_reproof_seconds:
            raise NoaaErrCompletionDeadlineError(
                "NOAA lease cannot contain final authority reproof"
            )
        binding = _validate_router_lease(
            lease, desired_state, self.raw_store, self.region_policy
        )
        existing = self.output_store.load(lease.event_id)
        recovered = existing is not None
        if existing is None:
            if before_child is not None:
                before_child()
            outcome = self.process_leased(lease, desired_state, cancel_event)
            initial_children = 1
            output = self.output_store.load(lease.event_id)
            if output is None:
                raise NoaaErrCompletionIntegrityError("NOAA output disappeared")
        else:
            reloaded = self.lifecycle_store.load_leased(
                lease.event_id, lease.lease_token
            )
            if reloaded != lease:
                raise NoaaErrCompletionConflict(
                    "NOAA lifecycle lease changed before pending output"
                )
            output = self.output_store.verify_for_ack(
                lease=lease,
                region_authority=binding.region_authority,
                isolation_authority_sha256=self.process_runner.authority_sha256,
                isolation_limits_sha256=self.process_runner.limits.fingerprint,
                output_sha256=existing.output_sha256,
                process_receipt_sha256=existing.process_receipt_sha256,
            )
            outcome = PipelineOutcome(
                output_sha256=output.output_sha256,
                disposition=OUTPUT_DISPOSITION,
                fact_revision_staged=False,
                process_receipt_sha256=output.process_receipt_sha256,
            )
            initial_children = 0
        verified = self.output_store.verify_for_ack(
            lease=lease,
            region_authority=binding.region_authority,
            isolation_authority_sha256=self.process_runner.authority_sha256,
            isolation_limits_sha256=self.process_runner.limits.fingerprint,
            output_sha256=outcome.output_sha256,
            process_receipt_sha256=outcome.process_receipt_sha256,
        )
        return NoaaErrCompletionPendingAck.create(
            lease=lease,
            desired_state=desired_state,
            binding=binding,
            outcome=outcome,
            output_receipt=verified,
            recovered_output=recovered,
            initial_child_invocations=initial_children,
        )

    def finalize_leased(
        self,
        pending: NoaaErrCompletionPendingAck,
        cancel_event: threading.Event,
        *,
        before_child: Callable[[], None] | None = None,
    ) -> NoaaErrCompletionRunResult:
        """Run exactly one final-reproof child and then terminalize the lease."""

        if not isinstance(pending, NoaaErrCompletionPendingAck):
            raise TypeError("pending must be NoaaErrCompletionPendingAck")
        if not callable(getattr(cancel_event, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        if before_child is not None and not callable(before_child):
            raise TypeError("before_child must be callable")
        pending.validate()
        lease = pending.lease
        desired_state = pending.desired_state
        binding = pending.binding
        verified = self.output_store.verify_for_ack(
            lease=lease,
            region_authority=binding.region_authority,
            isolation_authority_sha256=self.process_runner.authority_sha256,
            isolation_limits_sha256=self.process_runner.limits.fingerprint,
            output_sha256=pending.outcome.output_sha256,
            process_receipt_sha256=pending.outcome.process_receipt_sha256,
        )
        self._fault("consumer.before_final_reproof")
        if cancel_event.is_set():
            raise NoaaErrCompletionCancelled(
                "NOAA completion was cancelled before final reproof"
            )
        now = _timestamp(self._clock(), "NOAA final reproof clock")
        if lease.lease_expires_at - now <= self.process_runner.final_reproof_seconds:
            raise NoaaErrCompletionDeadlineError(
                "NOAA lease cannot contain final authority reproof"
            )
        final_lease = self.lifecycle_store.load_leased(
            lease.event_id, lease.lease_token
        )
        if final_lease != lease:
            raise NoaaErrCompletionConflict(
                "NOAA lifecycle lease changed before final reproof"
            )
        active_before_reproof = self.desired_state_store.load_active()
        if (
            active_before_reproof is None
            or active_before_reproof.state_sha256 != desired_state.state_sha256
        ):
            return self._reject_obsolete(
                final_lease,
                active_desired=active_before_reproof,
                reason_code="desired_state_changed_before_ack",
                now=now,
            )
        if before_child is not None:
            before_child()
        final_process = self.process_runner.reprove_for_ack(
            binding=binding,
            cancel_event=cancel_event,
        )
        self.process_runner.raise_for_terminal(final_process)
        final_output = final_process.output_receipt
        if final_output is None or final_output != verified:
            raise NoaaErrCompletionIntegrityError(
                "NOAA durable output changed during final reproof"
            )
        if cancel_event.is_set():
            raise NoaaErrCompletionCancelled(
                "NOAA completion was cancelled at ack boundary"
            )
        now = _timestamp(self._clock(), "NOAA acknowledgement clock")
        if now >= lease.lease_expires_at:
            raise NoaaErrCompletionDeadlineError(
                "NOAA lease expired at acknowledgement boundary"
            )
        ack_lease = self.lifecycle_store.load_leased(
            lease.event_id, lease.lease_token
        )
        if ack_lease != lease:
            raise NoaaErrCompletionConflict(
                "NOAA lifecycle lease changed at acknowledgement boundary"
            )
        active_before_ack = self.desired_state_store.load_active()
        if (
            active_before_ack is None
            or active_before_ack.state_sha256 != desired_state.state_sha256
        ):
            return self._reject_obsolete(
                ack_lease,
                active_desired=active_before_ack,
                reason_code="desired_state_changed_before_ack",
                now=now,
                phase_child_invocations=1,
            )
        with self.output_store.completion_reproof_lease(
            lease=ack_lease,
            region_authority=binding.region_authority,
            isolation_authority_sha256=self.process_runner.authority_sha256,
            isolation_limits_sha256=self.process_runner.limits.fingerprint,
            output_sha256=final_output.output_sha256,
            process_receipt_sha256=final_output.process_receipt_sha256,
            expected_store_identity_sha256=(
                self._output_store_identity_sha256
            ),
        ) as final_verified:
            if final_verified != final_output:
                raise NoaaErrCompletionIntegrityError(
                    "NOAA fenced completion proof differs"
                )
            self._fault("consumer.after_final_reproof_before_ack")
            self.output_store.assert_instance_held(
                self._output_store_identity_sha256
            )
            consumption = self.lifecycle_store.acknowledge(
                ack_lease.event_id,
                ack_lease.lease_token,
                output_sha256=final_verified.output_sha256,
                now=now,
            )
        self._validate_consumption_receipt(
            consumption,
            ack_lease,
            expected_output_sha256=final_verified.output_sha256,
        )
        self._fault("consumer.after_ack")
        return NoaaErrCompletionRunResult(
            "recovered_output" if pending.recovered_output else "processed",
            lease.event_id,
            final_verified,
            consumption,
            "durable_shadow_output_acknowledged",
            1,
        )

    def run_once(
        self,
        owner_id: str,
        *,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        cancel_event: threading.Event | None = None,
    ) -> NoaaErrCompletionRunResult:
        owner = _identifier(owner_id, "NOAA consumer owner")
        cancellation = cancel_event or threading.Event()
        if not callable(getattr(cancellation, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(float(lease_seconds))
            or not 0.0 < float(lease_seconds) <= MAX_LEASE_SECONDS
            or self.process_runner.minimum_lease_seconds >= float(lease_seconds)
        ):
            raise ValueError("NOAA standalone lease is outside its boundary")
        lease = self.lifecycle_store.claim_next(
            CONSUMER_ID,
            STREAM_ID,
            owner,
            lease_seconds=float(lease_seconds),
            now=_timestamp(self._clock(), "NOAA claim clock"),
        )
        if lease is None:
            return NoaaErrCompletionRunResult("idle", None, None, None)
        self._fault("consumer.after_claim")
        lease = self.lifecycle_store.load_leased(lease.event_id, lease.lease_token)
        now = _timestamp(self._clock(), "NOAA post-claim clock")
        active = self.desired_state_store.load_active()
        if active is None:
            return self._reject_obsolete(
                lease,
                active_desired=None,
                reason_code="active_desired_state_unavailable",
                now=now,
            )
        if active.state_sha256 != lease.desired_state_sha256:
            return self._reject_obsolete(
                lease,
                active_desired=active,
                reason_code="desired_state_superseded_before_processing",
                now=now,
            )
        pending = self.begin_leased(lease, active, cancellation)
        return self.finalize_leased(pending, cancellation)


__all__ = [
    "CONSUMER_ID",
    "NoaaErrCompletionCancelled",
    "NoaaErrCompletionCapacityError",
    "NoaaErrCompletionConflict",
    "NoaaErrCompletionConsumer",
    "NoaaErrCompletionDeadlineError",
    "NoaaErrCompletionError",
    "NoaaErrCompletionIntegrityError",
    "NoaaErrCompletionIsolatedRunner",
    "NoaaErrCompletionIsolationLimits",
    "NoaaErrCompletionOutputLimits",
    "NoaaErrCompletionOutputReceipt",
    "NoaaErrCompletionOutputStore",
    "NoaaErrCompletionPendingAck",
    "NoaaErrCompletionProcessError",
    "NoaaErrCompletionProcessResult",
    "NoaaErrCompletionRunResult",
    "NoaaErrLeaseBinding",
    "NoaaErrRegionAuthority",
    "NoaaErrRegionPolicy",
    "OUTPUT_DISPOSITION",
    "OUTPUT_DOCUMENT_SCHEMA",
    "PROCESS_EFFECT_CONTRACT",
    "PROCESS_EFFECT_CONTRACT_SHA256",
    "PROCESS_TARGET",
    "STREAM_ID",
    "WORKLOAD_ID",
    "issue_noaa_err_region_authority",
]
