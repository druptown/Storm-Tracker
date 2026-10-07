"""Closed, inert adapters for every terminal raw completion route.

The serial dispatcher owns lifecycle claims.  Adapters never call a consumer
``run_once`` method: they accept the exact parent-held lease, consume at most
one one-shot heavy-child permit, and return a path/token/payload-free result.
Only :class:`PermanentEventFailure` can request quarantine, and its reason is
restricted to the lifecycle's five event-bound permanent reason codes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time
from types import MappingProxyType
from typing import Any, Callable, Final, Literal, Mapping, Protocol

from .acquisition_evidence_lifecycle import (
    CONSUMPTION_RECEIPT_SCHEMA,
    QUARANTINE_REASON_CODES,
    AcquisitionEvidenceLifecycleStore,
    AcquisitionEvidenceCapacityError,
    AcquisitionEvidenceConflict,
    AcquisitionEvidenceDurabilityError,
    CompletionLease,
    CompletionWindowExpired,
    ConsumptionReceipt,
    desired_authority_proof,
)
from .desired_state import DesiredState, DesiredStateConflict, DesiredStateStore
from .dwd_rv_completion_consumer import (
    CONSUMER_ID as DWD_CONSUMER_ID,
    STREAM_ID as DWD_STREAM_ID,
    DwdRvCompletionConsumer,
    DwdRvCompletionCancelled,
    DwdRvCompletionCapacityError,
    DwdRvCompletionConflict,
    DwdRvCompletionDeadlineError,
    DwdRvPipelineOutcomeAuthority,
)
from .knmi_rtcor_completion_consumer import (
    WORKLOAD_ID as KNMI_WORKLOAD_ID,
    KnmiRtcorCompletionConsumer,
    KnmiRtcorCompletionCancelled,
    KnmiRtcorCompletionCapacityError,
    KnmiRtcorCompletionConflict,
    KnmiRtcorCompletionDeadlineError,
    KnmiRtcorCompletionRunResult,
)
from .met_office_rate_completion_consumer import (
    WORKLOAD_ID as METOFFICE_WORKLOAD_ID,
    MetOfficeRateCompletionConsumer,
    MetOfficeRateCompletionCancelled,
    MetOfficeRateCompletionCapacityError,
    MetOfficeRateCompletionConflict,
    MetOfficeRateCompletionDeadlineError,
    MetOfficeRateCompletionRunResult,
)
from .metar_ground_completion_consumer import (
    WORKLOAD_ID as METAR_WORKLOAD_ID,
    MetarGroundCompletionConsumer,
    MetarGroundCompletionCancelled,
    MetarGroundCompletionCapacityError,
    MetarGroundCompletionConflict,
    MetarGroundCompletionDeadlineError,
    MetarGroundCompletionRunResult,
)
from .noaa_err_completion_consumer import (
    WORKLOAD_ID as NOAA_WORKLOAD_ID,
    NoaaErrCompletionConsumer,
    NoaaErrCompletionCancelled,
    NoaaErrCompletionCapacityError,
    NoaaErrCompletionConflict,
    NoaaErrCompletionDeadlineError,
    NoaaErrCompletionPendingAck,
    NoaaErrCompletionRunResult,
    NoaaErrCompletionProcessError,
)
from .raw_manifest_audit_completion_consumer import (
    AUDIT_CONSUMER_CATALOG,
    RawManifestAuditCompletionConsumer,
    RawManifestAuditCancelled,
    RawManifestAuditCapacityError,
    RawManifestAuditConflict,
    RawManifestAuditDeadlineError,
    RawManifestAuditPendingAck,
    RawManifestAuditRunResult,
)
from .terminal_raw_completion_router import (
    TERMINAL_RAW_CONSUMER_CATALOG,
    TerminalRawConsumerBinding,
    validate_terminal_raw_consumer_catalog,
)
from .worker_first_runtime import PipelineOutcome, WorkerFirstRuntimeOwnershipError


CONTRACT: Final = "stv3-completion-route-adapters-v1"
ROUTE_CATALOG_SCHEMA: Final = "stv3-serial-completion-route-catalog-v1"
ADAPTER_RESULT_SCHEMA: Final = "stv3-completion-route-adapter-result-v1"
CONTINUATION_SCHEMA: Final = "stv3-completion-route-continuation-v1"
PERMANENT_FAILURE_SCHEMA: Final = "stv3-permanent-completion-event-failure-v1"
HEAVY_CHILD_PERMIT_SCHEMA: Final = "stv3-heavy-child-permit-v1"
DEFAULT_ROUTE_LEASE_SECONDS: Final = 300.0
MAX_ROUTES: Final = len(TERMINAL_RAW_CONSUMER_CATALOG)

_SHA = re.compile(r"^[0-9a-f]{64}$")
_SAFE_REASON = re.compile(r"^[a-z][a-z0-9_]{0,95}$")

PERMANENT_EVENT_REASONS: Final = frozenset({
    "canonical_evidence_integrity_failed",
    "completion_reference_integrity_failed",
    "decoder_output_integrity_failed",
    "source_raw_integrity_failed",
    "unsupported_completion_contract",
})
if PERMANENT_EVENT_REASONS != QUARANTINE_REASON_CODES:  # pragma: no cover
    raise RuntimeError("completion quarantine reason catalogue drifted")

_ADAPTER_KIND_BY_WORKLOAD = {
    METAR_WORKLOAD_ID: "metar_single",
    KNMI_WORKLOAD_ID: "knmi_single",
    METOFFICE_WORKLOAD_ID: "metoffice_single",
    NOAA_WORKLOAD_ID: "noaa_split",
    "radar.dwd.radolan_rv": "dwd_verified_single",
    **{workload_id: "raw_manifest_split" for workload_id in AUDIT_CONSUMER_CATALOG},
}


class CompletionRouteAdapterError(RuntimeError):
    """Base detail-free adapter error."""


class CompletionRouteAdapterIntegrityError(CompletionRouteAdapterError):
    """An adapter, result, continuation, or fixed route profile drifted."""


class HeavyChildPermitError(CompletionRouteAdapterError):
    """The one-shot child budget was absent, forged, or reused."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CompletionRouteAdapterIntegrityError(
            "adapter value is not canonical JSON"
        ) from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise CompletionRouteAdapterIntegrityError(f"{label} is not SHA-256")
    return value


def _route_catalog_mapping() -> dict[str, Any]:
    if set(_ADAPTER_KIND_BY_WORKLOAD) != set(TERMINAL_RAW_CONSUMER_CATALOG):
        raise CompletionRouteAdapterIntegrityError(
            "adapter route coverage differs from terminal catalog"
        )
    routes = []
    for workload_id, binding in TERMINAL_RAW_CONSUMER_CATALOG.items():
        routes.append({
            "adapter_kind": _ADAPTER_KIND_BY_WORKLOAD[workload_id],
            "binding": binding.as_dict(),
            "lease_seconds": DEFAULT_ROUTE_LEASE_SECONDS,
            "phase_contract": (
                "initial_then_final_reproof"
                if _ADAPTER_KIND_BY_WORKLOAD[workload_id]
                in {"noaa_split", "raw_manifest_split"}
                else "single"
            ),
            "workload_id": workload_id,
        })
    return {
        "contract": ROUTE_CATALOG_SCHEMA,
        "max_heavy_children_per_tick": 1,
        "max_idle_claims_per_tick": MAX_ROUTES,
        "route_count": len(routes),
        "routes": routes,
        "runtime_wiring": "inert",
    }


ROUTE_CATALOG_CANONICAL: Final = _canonical(_route_catalog_mapping())
ROUTE_CATALOG_SHA256: Final = hashlib.sha256(ROUTE_CATALOG_CANONICAL).hexdigest()
ROUTE_ORDER: Final = tuple(TERMINAL_RAW_CONSUMER_CATALOG)


def validate_completion_route_catalog() -> None:
    validate_terminal_raw_consumer_catalog(TERMINAL_RAW_CONSUMER_CATALOG)
    mapping = _route_catalog_mapping()
    if (
        len(ROUTE_ORDER) != MAX_ROUTES
        or tuple(TERMINAL_RAW_CONSUMER_CATALOG) != ROUTE_ORDER
        or _canonical(mapping) != ROUTE_CATALOG_CANONICAL
        or _digest(mapping) != ROUTE_CATALOG_SHA256
    ):
        raise CompletionRouteAdapterIntegrityError(
            "fixed completion route catalog drifted"
        )


validate_completion_route_catalog()


class PermanentEventFailure(CompletionRouteAdapterError):
    """Explicit typed proof that one exact event is permanently unusable."""

    __slots__ = (
        "event_sha256",
        "evidence_fingerprint_sha256",
        "failure_stage",
        "quarantine_authorized",
        "reason_code",
        "route_profile_sha256",
    )

    def __init__(
        self,
        *,
        lease: CompletionLease,
        reason_code: str,
        failure_stage: Literal[
            "canonical_evidence",
            "completion_reference",
            "decoder_output",
            "source_raw",
            "completion_contract",
        ],
        route_profile_sha256: str,
    ) -> None:
        if not isinstance(lease, CompletionLease):
            raise TypeError("lease must be CompletionLease")
        if reason_code not in PERMANENT_EVENT_REASONS:
            raise ValueError("permanent event reason is not allowlisted")
        expected_by_stage = {
            "canonical_evidence": "canonical_evidence_integrity_failed",
            "completion_reference": "completion_reference_integrity_failed",
            "decoder_output": "decoder_output_integrity_failed",
            "source_raw": "source_raw_integrity_failed",
            "completion_contract": "unsupported_completion_contract",
        }
        if expected_by_stage.get(failure_stage) != reason_code:
            raise ValueError("permanent event stage and reason differ")
        route_sha = _sha(route_profile_sha256, "route profile SHA-256")
        self.event_sha256 = _sha(lease.event_sha256, "event SHA-256")
        self.reason_code = reason_code
        self.failure_stage = failure_stage
        self.route_profile_sha256 = route_sha
        self.evidence_fingerprint_sha256 = _digest({
            "event_sha256": self.event_sha256,
            "failure_stage": failure_stage,
            "producer_receipt_sha256": lease.producer_receipt_sha256,
            "reason_code": reason_code,
            "route_profile_sha256": route_sha,
            "schema": PERMANENT_FAILURE_SCHEMA,
            "stream_sequence": lease.stream_sequence,
        })
        # No current completion consumer exposes an independently durable,
        # event-bound failure receipt.  A hash over caller-supplied fields is
        # identity, not evidence, so this dispatcher generation deliberately
        # leaves quarantine authorization unreachable.  The dispatcher treats
        # even this typed object as blocked/retryable and never terminalizes it.
        self.quarantine_authorized = False
        super().__init__("permanent_completion_event_failure")


class HeavyChildPermit:
    """One-shot, non-serializable permission for a single child-capable call."""

    __slots__ = (
        "_consumed", "_expected_phase", "_expected_workload_id", "_nonce",
        "_tick_sha256",
    )

    def __init__(
        self,
        *,
        tick_sha256: str,
        expected_workload_id: str,
        expected_phase: Literal["single", "initial", "final_reproof"],
        nonce: object,
    ) -> None:
        self._tick_sha256 = _sha(tick_sha256, "tick SHA-256")
        if expected_workload_id not in ROUTE_ORDER or expected_phase not in {
            "single", "initial", "final_reproof"
        }:
            raise HeavyChildPermitError("heavy child permit scope is invalid")
        self._expected_workload_id = expected_workload_id
        self._expected_phase = expected_phase
        if nonce is None:
            raise TypeError("permit nonce is required")
        self._nonce = nonce
        self._consumed = False

    @property
    def consumed(self) -> bool:
        return self._consumed

    def consume(self, *, workload_id: str, phase: str, nonce: object) -> None:
        if nonce is not self._nonce or self._consumed:
            raise HeavyChildPermitError("heavy child permit is invalid or spent")
        if (
            workload_id != self._expected_workload_id
            or phase != self._expected_phase
        ):
            raise HeavyChildPermitError("heavy child permit scope differs")
        self._consumed = True


@dataclass(frozen=True, slots=True)
class AdapterContinuation:
    workload_id: str
    event_sha256: str
    phase: Literal["final_reproof"]
    lease_expires_at: float
    route_profile_sha256: str
    opaque_pending: object = field(repr=False, compare=False)
    continuation_sha256: str = ""

    @classmethod
    def create(
        cls,
        *,
        workload_id: str,
        event_sha256: str,
        lease_expires_at: float,
        route_profile_sha256: str,
        opaque_pending: object,
    ) -> "AdapterContinuation":
        body = {
            "event_sha256": _sha(event_sha256, "continuation event SHA-256"),
            "lease_expires_at": _timestamp(lease_expires_at),
            "phase": "final_reproof",
            "route_profile_sha256": _sha(
                route_profile_sha256, "continuation route profile SHA-256"
            ),
            "schema": CONTINUATION_SCHEMA,
            "workload_id": workload_id,
        }
        if workload_id not in ROUTE_ORDER or opaque_pending is None:
            raise CompletionRouteAdapterIntegrityError(
                "continuation route or pending value differs"
            )
        return cls(
            workload_id=workload_id,
            event_sha256=body["event_sha256"],
            phase="final_reproof",
            lease_expires_at=body["lease_expires_at"],
            route_profile_sha256=body["route_profile_sha256"],
            opaque_pending=opaque_pending,
            continuation_sha256=_digest(body),
        )

    def validate(self) -> None:
        body = {
            "event_sha256": _sha(self.event_sha256, "continuation event SHA-256"),
            "lease_expires_at": _timestamp(self.lease_expires_at),
            "phase": self.phase,
            "route_profile_sha256": _sha(
                self.route_profile_sha256, "continuation route profile SHA-256"
            ),
            "schema": CONTINUATION_SCHEMA,
            "workload_id": self.workload_id,
        }
        if (
            self.workload_id not in ROUTE_ORDER
            or self.phase != "final_reproof"
            or self.opaque_pending is None
            or _SHA.fullmatch(self.continuation_sha256) is None
            or _digest(body) != self.continuation_sha256
        ):
            raise CompletionRouteAdapterIntegrityError(
                "completion continuation identity differs"
            )


def _timestamp(value: object) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0.0 <= float(value) <= 2**53
    ):
        raise CompletionRouteAdapterIntegrityError("adapter timestamp is invalid")
    return float(value)


@dataclass(frozen=True, slots=True)
class AdapterPhaseResult:
    status: Literal[
        "continuation_pending", "retryable", "acknowledged", "rejected"
    ]
    reason_code: str
    workload_id: str
    event_sha256: str
    phase: Literal["single", "initial", "final_reproof"]
    child_invocations: Literal[0, 1]
    terminal_kind: Literal["acknowledged", "rejected"] | None = None
    output_sha256: str | None = None
    output_receipt_sha256: str | None = None
    process_receipt_sha256: str | None = None
    consumption_receipt_sha256: str | None = None
    continuation: AdapterContinuation | None = field(
        default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if (
            self.status not in {
                "continuation_pending", "retryable", "acknowledged", "rejected"
            }
            or self.phase not in {"single", "initial", "final_reproof"}
            or self.workload_id not in ROUTE_ORDER
            or _SHA.fullmatch(self.event_sha256) is None
            or _SAFE_REASON.fullmatch(self.reason_code) is None
            or isinstance(self.child_invocations, bool)
            or self.child_invocations not in {0, 1}
        ):
            raise CompletionRouteAdapterIntegrityError(
                "adapter result identity is invalid"
            )
        hashes = (
            self.output_sha256,
            self.output_receipt_sha256,
            self.process_receipt_sha256,
            self.consumption_receipt_sha256,
        )
        if any(value is not None and _SHA.fullmatch(value) is None for value in hashes):
            raise CompletionRouteAdapterIntegrityError(
                "adapter result contains an invalid receipt hash"
            )
        if self.status == "continuation_pending":
            if (
                self.phase != "initial"
                or self.continuation is None
                or self.terminal_kind is not None
                or any(value is not None for value in hashes)
            ):
                raise CompletionRouteAdapterIntegrityError(
                    "continuation result shape differs"
                )
            self.continuation.validate()
            if (
                self.continuation.workload_id != self.workload_id
                or self.continuation.event_sha256 != self.event_sha256
            ):
                raise CompletionRouteAdapterIntegrityError(
                    "continuation result identity differs"
                )
        elif self.continuation is not None:
            raise CompletionRouteAdapterIntegrityError(
                "terminal/retry adapter result carries a continuation"
            )
        if self.status == "acknowledged":
            if (
                self.phase == "initial"
                or self.terminal_kind != "acknowledged"
                or any(value is None for value in hashes)
            ):
                raise CompletionRouteAdapterIntegrityError(
                    "acknowledged adapter result is incomplete"
                )
        elif self.status == "rejected":
            if (
                self.terminal_kind != "rejected"
                or self.consumption_receipt_sha256 is None
                or any(value is not None for value in hashes[:3])
            ):
                raise CompletionRouteAdapterIntegrityError(
                    "rejected adapter result shape differs"
                )
        else:
            if (
                self.terminal_kind is not None
                or any(value is not None for value in hashes)
            ):
                raise CompletionRouteAdapterIntegrityError(
                    "nonterminal adapter result shape differs"
                )

    def as_dict(self) -> dict[str, Any]:
        return {
            "child_invocations": self.child_invocations,
            "consumption_receipt_sha256": self.consumption_receipt_sha256,
            "contract": ADAPTER_RESULT_SCHEMA,
            "event_sha256": self.event_sha256,
            "output_receipt_sha256": self.output_receipt_sha256,
            "output_sha256": self.output_sha256,
            "phase": self.phase,
            "process_receipt_sha256": self.process_receipt_sha256,
            "reason_code": self.reason_code,
            "status": self.status,
            "terminal_kind": self.terminal_kind,
            "workload_id": self.workload_id,
        }


class CompletionRouteAdapter(Protocol):
    workload_id: str
    consumer_id: str
    stream_id: str
    lease_seconds: float
    fingerprint: str

    def begin(
        self,
        lease: CompletionLease,
        *,
        permit: HeavyChildPermit,
        permit_nonce: object,
        cancel_event: threading.Event,
    ) -> AdapterPhaseResult: ...

    def finalize(
        self,
        continuation: AdapterContinuation,
        *,
        permit: HeavyChildPermit,
        permit_nonce: object,
        cancel_event: threading.Event,
    ) -> AdapterPhaseResult: ...


def _runner_profile(consumer: object) -> dict[str, Any]:
    runner = getattr(consumer, "runner", None)
    if runner is None:
        runner = getattr(consumer, "process_runner", None)
    authority = getattr(runner, "authority_sha256", None)
    limits = getattr(getattr(runner, "limits", None), "fingerprint", None)
    if not isinstance(authority, str) or _SHA.fullmatch(authority) is None:
        raise CompletionRouteAdapterIntegrityError(
            "consumer process authority is unavailable"
        )
    if not isinstance(limits, str) or _SHA.fullmatch(limits) is None:
        raise CompletionRouteAdapterIntegrityError(
            "consumer process limits authority is unavailable"
        )
    return {
        "consumer_class": type(consumer).__name__,
        "process_authority_sha256": authority,
        "process_limits_sha256": limits,
    }


_DURABLE_STORE_ATTRIBUTES: Final = (
    "raw_store",
    "handoff_raw_store",
    "artifact_store",
    "evidence_store",
    "output_store",
)
_INSTANCE_ATTRIBUTES: Final = (
    "store_instance_sha256",
    "instance_sha256",
    "store_instance_id",
    "instance_id",
)


def _composition_store_bindings(consumer: object) -> tuple[dict[str, Any], ...]:
    """Bind every known mutable route store without exposing it in results."""

    runner = getattr(consumer, "runner", None)
    if runner is None:
        runner = getattr(consumer, "process_runner", None)
    containers = (("consumer", consumer), ("runner", runner))
    by_path: dict[str, dict[str, Any]] = {}
    saw_output_store = False
    for container_label, container in containers:
        if container is None:
            continue
        for attribute in _DURABLE_STORE_ATTRIBUTES:
            store = getattr(container, attribute, None)
            if store is None:
                continue
            raw_path = getattr(store, "root", None)
            if raw_path is None:
                raw_path = getattr(store, "path", None)
            if raw_path is None:
                raise CompletionRouteAdapterIntegrityError(
                    "durable route store path is unavailable"
                )
            path = Path(raw_path)
            if (
                not path.is_absolute()
                or path.is_symlink()
                or not path.exists()
            ):
                raise CompletionRouteAdapterIntegrityError(
                    "durable route store path is unsafe"
                )
            resolved = path.resolve(strict=True)
            if os.path.normcase(str(resolved)) != os.path.normcase(str(path)):
                raise CompletionRouteAdapterIntegrityError(
                    "durable route store path is not canonical"
                )
            info = path.lstat()
            if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
                raise CompletionRouteAdapterIntegrityError(
                    "durable route store type is unsafe"
                )
            if stat.S_ISREG(info.st_mode) and int(info.st_nlink) != 1:
                raise CompletionRouteAdapterIntegrityError(
                    "durable route store file is multiply linked"
                )
            instance_value: str | None = None
            if attribute == "output_store":
                candidate = getattr(store, "store_instance_sha256", None)
                if not isinstance(candidate, str) or _SHA.fullmatch(candidate) is None:
                    raise CompletionRouteAdapterIntegrityError(
                        "completion output store has no exact instance SHA-256"
                    )
                instance_value = candidate
            else:
                for instance_attribute in _INSTANCE_ATTRIBUTES:
                    candidate = getattr(store, instance_attribute, None)
                    if isinstance(candidate, str) and 1 <= len(candidate) <= 256:
                        instance_value = candidate
                        break
            instance_sha = None if instance_value is None else _digest({
                "store_class": type(store).__name__,
                "value": instance_value,
            })
            if attribute == "output_store":
                saw_output_store = True
            key = str(resolved)
            role = f"{container_label}.{attribute}"
            prior = by_path.get(key)
            if prior is None:
                by_path[key] = {
                    "directory": bool(stat.S_ISDIR(info.st_mode)),
                    "instance_authority_sha256": instance_sha,
                    "path": key,
                    "persistent_instance_authority": instance_sha is not None,
                    "roles": [role],
                    "store_class": type(store).__name__,
                    "witness": {
                        "device": int(info.st_dev),
                        "inode": int(info.st_ino),
                        "links": int(info.st_nlink),
                        "mode": int(info.st_mode),
                    },
                }
            else:
                if (
                    prior["store_class"] != type(store).__name__
                    or prior["witness"]["device"] != int(info.st_dev)
                    or prior["witness"]["inode"] != int(info.st_ino)
                    or prior["witness"]["links"] != int(info.st_nlink)
                    or prior["witness"]["mode"] != int(info.st_mode)
                    or prior["instance_authority_sha256"] != instance_sha
                ):
                    raise CompletionRouteAdapterIntegrityError(
                        "durable route store binding aliases different stores"
                    )
                prior["roles"].append(role)
    if not by_path:
        raise CompletionRouteAdapterIntegrityError(
            "route has no durable store composition binding"
        )
    if not saw_output_store:
        raise CompletionRouteAdapterIntegrityError(
            "route has no completion output store authority"
        )
    for value in by_path.values():
        value["roles"] = sorted(set(value["roles"]))
    return tuple(by_path[key] for key in sorted(by_path))


def _profile(
    *,
    workload_id: str,
    adapter_kind: str,
    consumer: object,
    composition_store_bindings_sha256: str,
    extra: Mapping[str, Any] | None = None,
) -> str:
    binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
    return _digest({
        "adapter_contract": CONTRACT,
        "adapter_kind": adapter_kind,
        "binding": binding.as_dict(),
        "composition_store_bindings_sha256": _sha(
            composition_store_bindings_sha256,
            "composition store bindings SHA-256",
        ),
        "extra": {} if extra is None else dict(extra),
        "lease_seconds": DEFAULT_ROUTE_LEASE_SECONDS,
        "route_catalog_sha256": ROUTE_CATALOG_SHA256,
        "runner": _runner_profile(consumer),
        "workload_id": workload_id,
    })


def _validate_lease_route(lease: CompletionLease, binding: TerminalRawConsumerBinding) -> None:
    if (
        not isinstance(lease, CompletionLease)
        or lease.consumer_id != binding.consumer_id
        or lease.stream_id != binding.stream_id
        or _SHA.fullmatch(lease.event_sha256) is None
        or _SHA.fullmatch(lease.producer_receipt_sha256) is None
    ):
        raise CompletionRouteAdapterIntegrityError(
            "parent-held lease differs from adapter route"
        )


def _validate_consumption(
    receipt: ConsumptionReceipt,
    lease: CompletionLease,
    *,
    expected_output_sha256: str | None,
) -> None:
    if not isinstance(receipt, ConsumptionReceipt):
        raise CompletionRouteAdapterIntegrityError(
            "lifecycle terminalization returned no receipt"
        )
    body = receipt.as_dict()
    receipt_sha = body.pop("receipt_sha256", None)
    if (
        set(body)
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
        or body["schema"] != CONSUMPTION_RECEIPT_SCHEMA
        or receipt.event_id != lease.event_id
        or receipt.event_sha256 != lease.event_sha256
        or receipt.producer_receipt_sha256 != lease.producer_receipt_sha256
        or receipt.consumer_id != lease.consumer_id
        or receipt.stream_id != lease.stream_id
        or receipt.stream_sequence != lease.stream_sequence
        or _SHA.fullmatch(receipt.output_sha256) is None
        or _SHA.fullmatch(receipt.receipt_sha256) is None
        or receipt_sha != receipt.receipt_sha256
        or _digest(body) != receipt.receipt_sha256
        or (
            expected_output_sha256 is not None
            and receipt.output_sha256 != expected_output_sha256
        )
    ):
        raise CompletionRouteAdapterIntegrityError(
            "lifecycle terminal receipt differs from parent-held lease"
        )


def _reject_obsolete(
    *,
    lifecycle: AcquisitionEvidenceLifecycleStore,
    lease: CompletionLease,
    active_desired: DesiredState | None,
    reason_code: str,
    now: float,
    workload_id: str,
    phase: Literal["single", "initial", "final_reproof"],
    child_invocations: Literal[0, 1],
) -> AdapterPhaseResult:
    reason = "active_desired_state_unavailable" if active_desired is None else reason_code
    receipt = lifecycle.reject_leased(
        lease.event_id,
        lease.lease_token,
        expected_event_sha256=lease.event_sha256,
        reason_code=reason,
        active_desired_state_sha256=(
            None if active_desired is None else active_desired.state_sha256
        ),
        now=_timestamp(now),
    )
    _validate_consumption(receipt, lease, expected_output_sha256=None)
    return AdapterPhaseResult(
        status="rejected",
        reason_code=reason,
        workload_id=workload_id,
        event_sha256=lease.event_sha256,
        phase=phase,
        child_invocations=child_invocations,
        terminal_kind="rejected",
        consumption_receipt_sha256=receipt.receipt_sha256,
    )


def _retry_reason_from_result(status: str) -> str:
    return {
        "busy": "child_busy",
        "cancelled": "cancelled",
        "deadline": "deadline_expired",
        "deadline_exceeded": "deadline_expired",
        "resource": "capacity_unavailable",
        "capacity": "capacity_unavailable",
    }.get(status, "adapter_retryable")


class _SinglePhaseAdapter:
    def __init__(self, workload_id: str, consumer: object, expected_type: type, result_type: type):
        if not isinstance(consumer, expected_type):
            raise TypeError("consumer has wrong exact route type")
        self.workload_id = workload_id
        binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        self.consumer_id = binding.consumer_id
        self.stream_id = binding.stream_id
        self.lease_seconds = DEFAULT_ROUTE_LEASE_SECONDS
        self._consumer = consumer
        self._result_type = result_type
        self.clock = consumer._clock
        self.composition_store_bindings = _composition_store_bindings(consumer)
        self.composition_store_bindings_sha256 = _digest(
            self.composition_store_bindings
        )
        self.fingerprint = _profile(
            workload_id=workload_id,
            adapter_kind=_ADAPTER_KIND_BY_WORKLOAD[workload_id],
            consumer=consumer,
            composition_store_bindings_sha256=(
                self.composition_store_bindings_sha256
            ),
        )

    def begin(self, lease: CompletionLease, *, permit: HeavyChildPermit, permit_nonce: object, cancel_event: threading.Event) -> AdapterPhaseResult:
        _validate_lease_route(lease, TERMINAL_RAW_CONSUMER_CATALOG[self.workload_id])
        permit.consume(workload_id=self.workload_id, phase="single", nonce=permit_nonce)
        result = self._consumer.process_leased(lease, cancel_event=cancel_event)
        if not isinstance(result, self._result_type):
            raise CompletionRouteAdapterIntegrityError(
                "single-phase consumer returned wrong result type"
            )
        return self._normalize(lease, result)

    def _normalize(self, lease: CompletionLease, result: object) -> AdapterPhaseResult:
        status = getattr(result, "status", None)
        reason = getattr(result, "reason_code", None)
        if not isinstance(status, str) or not isinstance(reason, str):
            raise CompletionRouteAdapterIntegrityError(
                "single-phase result fields differ"
            )
        if getattr(result, "event_id", None) != lease.event_id:
            raise CompletionRouteAdapterIntegrityError(
                "single-phase result event differs"
            )
        if status == "acknowledged":
            return AdapterPhaseResult(
                "acknowledged", reason, self.workload_id, lease.event_sha256,
                "single", 1, "acknowledged",
                getattr(result, "output_sha256", None),
                getattr(result, "output_receipt_sha256", None),
                getattr(result, "process_receipt_sha256", None),
                getattr(result, "consumption_receipt_sha256", None),
            )
        if status == "rejected":
            return AdapterPhaseResult(
                "rejected", reason, self.workload_id, lease.event_sha256,
                "single", 1, "rejected",
                consumption_receipt_sha256=getattr(
                    result, "consumption_receipt_sha256", None
                ),
            )
        return AdapterPhaseResult(
            "retryable", _retry_reason_from_result(status), self.workload_id,
            lease.event_sha256, "single", 1,
        )

    def finalize(self, continuation: AdapterContinuation, *, permit: HeavyChildPermit, permit_nonce: object, cancel_event: threading.Event) -> AdapterPhaseResult:
        raise CompletionRouteAdapterIntegrityError(
            "single-phase adapter has no continuation"
        )


class DwdCompletionRouteAdapter:
    """DWD adapter with three parent-held lease/desired checks and verifier."""

    def __init__(
        self,
        *,
        consumer: DwdRvCompletionConsumer,
        lifecycle_store: AcquisitionEvidenceLifecycleStore,
        desired_state_store: DesiredStateStore,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not isinstance(consumer, DwdRvCompletionConsumer):
            raise TypeError("consumer must be DwdRvCompletionConsumer")
        if consumer.lifecycle_store is not lifecycle_store:
            raise CompletionRouteAdapterIntegrityError(
                "DWD lifecycle store identity differs"
            )
        if not isinstance(desired_state_store, DesiredStateStore):
            raise TypeError("desired_state_store must be DesiredStateStore")
        if getattr(consumer, "_clock", None) is not clock:
            raise CompletionRouteAdapterIntegrityError(
                "DWD consumer and adapter guarded clock differ"
            )
        self.workload_id = "radar.dwd.radolan_rv"
        self.consumer_id = DWD_CONSUMER_ID
        self.stream_id = DWD_STREAM_ID
        self.lease_seconds = DEFAULT_ROUTE_LEASE_SECONDS
        self._consumer = consumer
        self._lifecycle = lifecycle_store
        self._desired = desired_state_store
        self._clock = clock
        self.clock = clock
        self._authority = DwdRvPipelineOutcomeAuthority(
            output_store=consumer.output_store,
            profile=consumer.profile,
            process_isolation_authority_sha256=consumer.process_runner.authority_sha256,
        )
        self._output_store_identity_sha256 = (
            consumer.output_store.identity_sha256
        )
        self.composition_store_bindings = _composition_store_bindings(consumer)
        self.composition_store_bindings_sha256 = _digest(
            self.composition_store_bindings
        )
        self.fingerprint = _profile(
            workload_id=self.workload_id,
            adapter_kind="dwd_verified_single",
            consumer=consumer,
            composition_store_bindings_sha256=(
                self.composition_store_bindings_sha256
            ),
            extra={"pipeline_outcome_authority_sha256": self._authority.fingerprint},
        )

    def _check(
        self,
        lease: CompletionLease,
        expected: DesiredState | None,
        *,
        reason: str,
        child_invocations: Literal[0, 1],
    ) -> DesiredState | AdapterPhaseResult:
        reloaded = self._lifecycle.load_leased(lease.event_id, lease.lease_token)
        if reloaded != lease:
            raise CompletionRouteAdapterIntegrityError(
                "DWD parent-held lifecycle lease changed"
            )
        active = self._desired.load_active()
        if expected is None:
            if (
                active is None
                or active.state_sha256 != lease.desired_state_sha256
                or desired_authority_proof(active)["authority_sha256"]
                != lease.authority_sha256
            ):
                return _reject_obsolete(
                    lifecycle=self._lifecycle,
                    lease=reloaded,
                    active_desired=active,
                    reason_code="desired_state_superseded_before_processing",
                    now=self._clock(),
                    workload_id=self.workload_id,
                    phase="single",
                    child_invocations=child_invocations,
                )
            return active
        if active is None or active.state_sha256 != expected.state_sha256:
            return _reject_obsolete(
                lifecycle=self._lifecycle,
                lease=reloaded,
                active_desired=active,
                reason_code=reason,
                now=self._clock(),
                workload_id=self.workload_id,
                phase="single",
                child_invocations=child_invocations,
            )
        return active

    def begin(self, lease: CompletionLease, *, permit: HeavyChildPermit, permit_nonce: object, cancel_event: threading.Event) -> AdapterPhaseResult:
        _validate_lease_route(lease, TERMINAL_RAW_CONSUMER_CATALOG[self.workload_id])
        if cancel_event.is_set():
            return AdapterPhaseResult(
                "retryable", "cancelled", self.workload_id,
                lease.event_sha256, "single", 0,
            )
        first = self._check(
            lease, None, reason="desired_state_superseded_before_processing",
            child_invocations=0,
        )
        if isinstance(first, AdapterPhaseResult):
            return first
        permit.consume(workload_id=self.workload_id, phase="single", nonce=permit_nonce)
        outcome = self._consumer.process_leased(lease, first, cancel_event)
        if not isinstance(outcome, PipelineOutcome):
            raise CompletionRouteAdapterIntegrityError(
                "DWD process_leased returned wrong outcome type"
            )
        if cancel_event.is_set():
            return AdapterPhaseResult(
                "retryable", "cancelled", self.workload_id,
                lease.event_sha256, "single", 1,
            )
        second = self._check(
            lease, first, reason="desired_state_changed_after_processing",
            child_invocations=1,
        )
        if isinstance(second, AdapterPhaseResult):
            return second
        verified = self._authority.verify(
            lease=lease, desired_state=second, outcome=outcome
        )
        if cancel_event.is_set():
            return AdapterPhaseResult(
                "retryable", "cancelled", self.workload_id,
                lease.event_sha256, "single", 1,
            )
        now = _timestamp(self._clock())
        if now >= lease.lease_expires_at:
            return AdapterPhaseResult(
                "retryable", "deadline_expired", self.workload_id,
                lease.event_sha256, "single", 1,
            )
        self._consumer._fault("adapter.before_final_output_verify")
        self._consumer.output_store.assert_instance_held(
            self._output_store_identity_sha256
        )
        # DWD output-store v3 holds BEGIN IMMEDIATE from this exact final
        # reproof through the separate lifecycle ACK.  Immutable triggers and
        # the persistent store-instance authority are revalidated on exit.
        with self._consumer.output_store.completion_reproof_lease(
            event_id=lease.event_id,
            event_sha256=lease.event_sha256,
            desired_state_sha256=second.state_sha256,
            authority_sha256=lease.authority_sha256,
            consumer_id=lease.consumer_id,
            stream_id=lease.stream_id,
            durable_output_receipt_sha256=verified.output_receipt_sha256,
            durable_process_receipt_sha256=verified.process_receipt_sha256,
            process_isolation_authority_sha256=(
                self._consumer.process_runner.authority_sha256
            ),
            output_sha256=verified.output_sha256,
            disposition=verified.disposition,
            staged_fact_revision=None,
        ) as fenced:
            if (
                fenced.output_sha256 != verified.output_sha256
                or fenced.durable_output_receipt_sha256
                != verified.output_receipt_sha256
                or fenced.durable_process_receipt_sha256
                != verified.process_receipt_sha256
            ):
                raise CompletionRouteAdapterIntegrityError(
                    "DWD fenced durable verifier receipt changed"
                )
            self._consumer._fault(
                "adapter.after_final_output_verify_before_barrier"
            )
            third = self._check(
                lease, second, reason="desired_state_changed_before_ack",
                child_invocations=1,
            )
            if isinstance(third, AdapterPhaseResult):
                return third
            if cancel_event.is_set():
                return AdapterPhaseResult(
                    "retryable", "cancelled", self.workload_id,
                    lease.event_sha256, "single", 1,
                )
            now = _timestamp(self._clock())
            if now >= lease.lease_expires_at:
                return AdapterPhaseResult(
                    "retryable", "deadline_expired", self.workload_id,
                    lease.event_sha256, "single", 1,
                )
            self._consumer.output_store.assert_instance_held(
                self._output_store_identity_sha256
            )
            receipt = self._lifecycle.acknowledge(
                lease.event_id,
                lease.lease_token,
                output_sha256=verified.output_sha256,
                now=now,
            )
            _validate_consumption(
                receipt, lease, expected_output_sha256=verified.output_sha256
            )
        return AdapterPhaseResult(
            "acknowledged",
            "durable_shadow_output_acknowledged",
            self.workload_id,
            lease.event_sha256,
            "single",
            1,
            "acknowledged",
            verified.output_sha256,
            verified.output_receipt_sha256,
            verified.process_receipt_sha256,
            receipt.receipt_sha256,
        )

    def finalize(self, continuation: AdapterContinuation, *, permit: HeavyChildPermit, permit_nonce: object, cancel_event: threading.Event) -> AdapterPhaseResult:
        raise CompletionRouteAdapterIntegrityError("DWD has no continuation")


class NoaaCompletionRouteAdapter:
    def __init__(self, *, consumer: NoaaErrCompletionConsumer) -> None:
        if not isinstance(consumer, NoaaErrCompletionConsumer):
            raise TypeError("consumer must be NoaaErrCompletionConsumer")
        self.workload_id = NOAA_WORKLOAD_ID
        binding = TERMINAL_RAW_CONSUMER_CATALOG[self.workload_id]
        self.consumer_id = binding.consumer_id
        self.stream_id = binding.stream_id
        self.lease_seconds = DEFAULT_ROUTE_LEASE_SECONDS
        self._consumer = consumer
        self.clock = consumer._clock
        self.composition_store_bindings = _composition_store_bindings(consumer)
        self.composition_store_bindings_sha256 = _digest(
            self.composition_store_bindings
        )
        self.fingerprint = _profile(
            workload_id=self.workload_id,
            adapter_kind="noaa_split",
            consumer=consumer,
            composition_store_bindings_sha256=(
                self.composition_store_bindings_sha256
            ),
        )

    def begin(self, lease: CompletionLease, *, permit: HeavyChildPermit, permit_nonce: object, cancel_event: threading.Event) -> AdapterPhaseResult:
        _validate_lease_route(lease, TERMINAL_RAW_CONSUMER_CATALOG[self.workload_id])
        active = self._consumer.desired_state_store.load_active()
        if active is None or active.state_sha256 != lease.desired_state_sha256:
            return _reject_obsolete(
                lifecycle=self._consumer.lifecycle_store,
                lease=lease,
                active_desired=active,
                reason_code="desired_state_superseded_before_processing",
                now=self._consumer._clock(),
                workload_id=self.workload_id,
                phase="initial",
                child_invocations=0,
            )
        pending = self._consumer.begin_leased(
            lease,
            active,
            cancel_event,
            before_child=lambda: permit.consume(
                workload_id=self.workload_id,
                phase="initial",
                nonce=permit_nonce,
            ),
        )
        if not isinstance(pending, NoaaErrCompletionPendingAck):
            raise CompletionRouteAdapterIntegrityError(
                "NOAA begin phase returned wrong continuation"
            )
        continuation = AdapterContinuation.create(
            workload_id=self.workload_id,
            event_sha256=lease.event_sha256,
            lease_expires_at=lease.lease_expires_at,
            route_profile_sha256=self.fingerprint,
            opaque_pending=pending,
        )
        return AdapterPhaseResult(
            "continuation_pending", "final_reproof_pending", self.workload_id,
            lease.event_sha256, "initial", pending.initial_child_invocations,
            continuation=continuation,
        )

    def finalize(self, continuation: AdapterContinuation, *, permit: HeavyChildPermit, permit_nonce: object, cancel_event: threading.Event) -> AdapterPhaseResult:
        continuation.validate()
        if (
            continuation.workload_id != self.workload_id
            or continuation.route_profile_sha256 != self.fingerprint
            or not isinstance(
                continuation.opaque_pending, NoaaErrCompletionPendingAck
            )
        ):
            raise CompletionRouteAdapterIntegrityError(
                "NOAA continuation differs from route"
            )
        pending = continuation.opaque_pending
        result = self._consumer.finalize_leased(
            pending,
            cancel_event,
            before_child=lambda: permit.consume(
                workload_id=self.workload_id,
                phase="final_reproof",
                nonce=permit_nonce,
            ),
        )
        return _normalize_noaa_result(
            pending.lease,
            result,
            child_invocations=result.phase_child_invocations,
        )


def _normalize_noaa_result(
    lease: CompletionLease,
    result: NoaaErrCompletionRunResult,
    *,
    child_invocations: Literal[0, 1],
) -> AdapterPhaseResult:
    if not isinstance(result, NoaaErrCompletionRunResult) or result.event_id != lease.event_id:
        raise CompletionRouteAdapterIntegrityError("NOAA terminal result differs")
    if result.disposition in {"processed", "recovered_output"}:
        output = result.output_receipt
        consumption = result.consumption_receipt
        if output is None or consumption is None:
            raise CompletionRouteAdapterIntegrityError("NOAA ack result is incomplete")
        _validate_consumption(
            consumption, lease, expected_output_sha256=output.output_sha256
        )
        return AdapterPhaseResult(
            "acknowledged", result.reason_code or "durable_shadow_output_acknowledged",
            NOAA_WORKLOAD_ID, lease.event_sha256, "final_reproof",
            child_invocations, "acknowledged", output.output_sha256,
            output.receipt_sha256, output.process_receipt_sha256,
            consumption.receipt_sha256,
        )
    if result.disposition == "rejected":
        if result.consumption_receipt is None:
            raise CompletionRouteAdapterIntegrityError("NOAA rejection receipt missing")
        _validate_consumption(
            result.consumption_receipt, lease, expected_output_sha256=None
        )
        return AdapterPhaseResult(
            "rejected", result.reason_code or "desired_state_changed_before_ack",
            NOAA_WORKLOAD_ID, lease.event_sha256, "final_reproof",
            child_invocations, "rejected",
            consumption_receipt_sha256=result.consumption_receipt.receipt_sha256,
        )
    raise CompletionRouteAdapterIntegrityError("NOAA terminal disposition differs")


class RawManifestCompletionRouteAdapter:
    def __init__(self, *, workload_id: str, consumer: RawManifestAuditCompletionConsumer) -> None:
        if workload_id not in AUDIT_CONSUMER_CATALOG:
            raise ValueError("workload is not a raw manifest audit route")
        if not isinstance(consumer, RawManifestAuditCompletionConsumer):
            raise TypeError("consumer must be RawManifestAuditCompletionConsumer")
        self.workload_id = workload_id
        binding = AUDIT_CONSUMER_CATALOG[workload_id]
        self.consumer_id = binding.consumer_id
        self.stream_id = binding.stream_id
        self.lease_seconds = DEFAULT_ROUTE_LEASE_SECONDS
        self._consumer = consumer
        self.clock = consumer._clock
        self.composition_store_bindings = _composition_store_bindings(consumer)
        self.composition_store_bindings_sha256 = _digest(
            self.composition_store_bindings
        )
        self.fingerprint = _profile(
            workload_id=workload_id,
            adapter_kind="raw_manifest_split",
            consumer=consumer,
            composition_store_bindings_sha256=(
                self.composition_store_bindings_sha256
            ),
        )

    def begin(self, lease: CompletionLease, *, permit: HeavyChildPermit, permit_nonce: object, cancel_event: threading.Event) -> AdapterPhaseResult:
        _validate_lease_route(lease, AUDIT_CONSUMER_CATALOG[self.workload_id])
        active = self._consumer.desired.load_active()
        if active is None or active.state_sha256 != lease.desired_state_sha256:
            return _reject_obsolete(
                lifecycle=self._consumer.lifecycle,
                lease=lease,
                active_desired=active,
                reason_code="desired_state_superseded_before_processing",
                now=self._consumer._clock(),
                workload_id=self.workload_id,
                phase="initial",
                child_invocations=0,
            )
        begun = self._consumer.begin_leased(
            lease,
            cancel_event,
            before_child=lambda: permit.consume(
                workload_id=self.workload_id,
                phase="initial",
                nonce=permit_nonce,
            ),
        )
        if isinstance(begun, RawManifestAuditRunResult):
            return _normalize_raw_result(
                self.workload_id, lease, begun, phase="initial",
            )
        if not isinstance(begun, RawManifestAuditPendingAck):
            raise CompletionRouteAdapterIntegrityError(
                "raw manifest begin phase returned wrong continuation"
            )
        continuation = AdapterContinuation.create(
            workload_id=self.workload_id,
            event_sha256=lease.event_sha256,
            lease_expires_at=lease.lease_expires_at,
            route_profile_sha256=self.fingerprint,
            opaque_pending=begun,
        )
        return AdapterPhaseResult(
            "continuation_pending", "final_reproof_pending", self.workload_id,
            lease.event_sha256, "initial", begun.initial_child_invocations,
            continuation=continuation,
        )

    def finalize(self, continuation: AdapterContinuation, *, permit: HeavyChildPermit, permit_nonce: object, cancel_event: threading.Event) -> AdapterPhaseResult:
        continuation.validate()
        if (
            continuation.workload_id != self.workload_id
            or continuation.route_profile_sha256 != self.fingerprint
            or not isinstance(
                continuation.opaque_pending, RawManifestAuditPendingAck
            )
        ):
            raise CompletionRouteAdapterIntegrityError(
                "raw manifest continuation differs from route"
            )
        pending = continuation.opaque_pending
        result = self._consumer.finalize_leased(
            pending,
            cancel_event,
            before_child=lambda: permit.consume(
                workload_id=self.workload_id,
                phase="final_reproof",
                nonce=permit_nonce,
            ),
        )
        return _normalize_raw_result(
            self.workload_id, pending.lease, result,
            phase="final_reproof",
        )


def _normalize_raw_result(
    workload_id: str,
    lease: CompletionLease,
    result: RawManifestAuditRunResult,
    *,
    phase: Literal["initial", "final_reproof"],
) -> AdapterPhaseResult:
    if not isinstance(result, RawManifestAuditRunResult) or result.event_id != lease.event_id:
        raise CompletionRouteAdapterIntegrityError("raw audit result differs")
    child_invocations = result.phase_child_invocations
    if result.status == "acknowledged":
        return AdapterPhaseResult(
            "acknowledged", result.reason_code, workload_id, lease.event_sha256,
            phase, child_invocations, "acknowledged", result.output_sha256,
            result.output_receipt_sha256, result.process_receipt_sha256,
            result.consumption_receipt_sha256,
        )
    if result.status == "rejected":
        child_invocations = result.phase_child_invocations
        return AdapterPhaseResult(
            "rejected", result.reason_code, workload_id, lease.event_sha256,
            phase, child_invocations, "rejected",
            consumption_receipt_sha256=result.consumption_receipt_sha256,
        )
    return AdapterPhaseResult(
        "retryable", _retry_reason_from_result(result.status), workload_id,
        lease.event_sha256, phase, result.phase_child_invocations,
    )


def build_completion_route_adapters(
    *,
    lifecycle_store: AcquisitionEvidenceLifecycleStore,
    desired_state_store: DesiredStateStore,
    consumers: Mapping[str, object],
    clock: Callable[[], float] = time.time,
) -> Mapping[str, CompletionRouteAdapter]:
    """Build the exact closed catalogue; partial or extra maps fail closed."""

    validate_completion_route_catalog()
    if not isinstance(lifecycle_store, AcquisitionEvidenceLifecycleStore):
        raise TypeError("lifecycle_store must be AcquisitionEvidenceLifecycleStore")
    if not isinstance(desired_state_store, DesiredStateStore):
        raise TypeError("desired_state_store must be DesiredStateStore")
    if set(consumers) != set(ROUTE_ORDER):
        raise CompletionRouteAdapterIntegrityError(
            "consumer map differs from the closed route catalog"
        )
    result: dict[str, CompletionRouteAdapter] = {}
    for workload_id in ROUTE_ORDER:
        consumer = consumers[workload_id]
        if workload_id == "radar.dwd.radolan_rv":
            result[workload_id] = DwdCompletionRouteAdapter(
                consumer=consumer,
                lifecycle_store=lifecycle_store,
                desired_state_store=desired_state_store,
                clock=clock,
            )
        elif workload_id == NOAA_WORKLOAD_ID:
            result[workload_id] = NoaaCompletionRouteAdapter(consumer=consumer)
        elif workload_id in AUDIT_CONSUMER_CATALOG:
            result[workload_id] = RawManifestCompletionRouteAdapter(
                workload_id=workload_id, consumer=consumer
            )
        elif workload_id == KNMI_WORKLOAD_ID:
            result[workload_id] = _SinglePhaseAdapter(
                workload_id, consumer, KnmiRtcorCompletionConsumer,
                KnmiRtcorCompletionRunResult,
            )
        elif workload_id == METOFFICE_WORKLOAD_ID:
            result[workload_id] = _SinglePhaseAdapter(
                workload_id, consumer, MetOfficeRateCompletionConsumer,
                MetOfficeRateCompletionRunResult,
            )
        elif workload_id == METAR_WORKLOAD_ID:
            result[workload_id] = _SinglePhaseAdapter(
                workload_id, consumer, MetarGroundCompletionConsumer,
                MetarGroundCompletionRunResult,
            )
        else:  # pragma: no cover - closed catalog validator above
            raise CompletionRouteAdapterIntegrityError("unknown completion route")
    return MappingProxyType(result)


RETRYABLE_EXCEPTION_TYPES: Final = (
    AcquisitionEvidenceCapacityError,
    AcquisitionEvidenceConflict,
    AcquisitionEvidenceDurabilityError,
    CompletionWindowExpired,
    DesiredStateConflict,
    DwdRvCompletionCancelled,
    DwdRvCompletionCapacityError,
    DwdRvCompletionConflict,
    DwdRvCompletionDeadlineError,
    KnmiRtcorCompletionCancelled,
    KnmiRtcorCompletionCapacityError,
    KnmiRtcorCompletionConflict,
    KnmiRtcorCompletionDeadlineError,
    MetOfficeRateCompletionCancelled,
    MetOfficeRateCompletionCapacityError,
    MetOfficeRateCompletionConflict,
    MetOfficeRateCompletionDeadlineError,
    MetarGroundCompletionCancelled,
    MetarGroundCompletionCapacityError,
    MetarGroundCompletionConflict,
    MetarGroundCompletionDeadlineError,
    NoaaErrCompletionCancelled,
    NoaaErrCompletionCapacityError,
    NoaaErrCompletionConflict,
    NoaaErrCompletionDeadlineError,
    NoaaErrCompletionProcessError,
    RawManifestAuditCancelled,
    RawManifestAuditCapacityError,
    RawManifestAuditConflict,
    RawManifestAuditDeadlineError,
    WorkerFirstRuntimeOwnershipError,
    OSError,
    TimeoutError,
)


def is_retryable_exception(error: Exception) -> bool:
    """Closed transient classifier; SQLite contract/schema failures are fatal."""

    if isinstance(error, RETRYABLE_EXCEPTION_TYPES):
        return True
    if not isinstance(error, sqlite3.OperationalError):
        return False
    sqlite_name = getattr(error, "sqlite_errorname", None)
    if isinstance(sqlite_name, str) and sqlite_name.startswith((
        "SQLITE_BUSY",
        "SQLITE_LOCKED",
        "SQLITE_IOERR",
        "SQLITE_FULL",
        "SQLITE_CANTOPEN",
    )):
        return True
    # Python-created OperationalError instances used at adapter boundaries do
    # not carry sqlite_errorname.  Only these exact transient families pass.
    detail = str(error).casefold()
    return any(token in detail for token in (
        "database is locked",
        "database table is locked",
        "disk i/o error",
        "database or disk is full",
        "unable to open database file",
    ))


def retryable_exception_reason(error: Exception) -> str:
    """Normalize non-permanent failures without exposing exception details."""

    if isinstance(error, PermanentEventFailure):
        raise TypeError("permanent event failure must use quarantine path")
    if not is_retryable_exception(error):
        raise CompletionRouteAdapterIntegrityError(
            "exception is not retry-authorized"
        ) from error
    name = type(error).__name__.casefold()
    if "cancel" in name:
        return "cancelled"
    if "deadline" in name or isinstance(error, TimeoutError):
        return "deadline_expired"
    if "capacity" in name or "resource" in name or "full" in name:
        return "capacity_unavailable"
    if "busy" in name or "owner" in name:
        return "busy"
    if "conflict" in name or "desired" in name:
        return "state_conflict"
    if isinstance(error, (OSError, sqlite3.OperationalError)):
        return "system_store_unavailable"
    return "adapter_retryable"


__all__ = [
    "ADAPTER_RESULT_SCHEMA",
    "AdapterContinuation",
    "AdapterPhaseResult",
    "CompletionRouteAdapter",
    "CompletionRouteAdapterError",
    "CompletionRouteAdapterIntegrityError",
    "CONTRACT",
    "DEFAULT_ROUTE_LEASE_SECONDS",
    "DwdCompletionRouteAdapter",
    "HeavyChildPermit",
    "HeavyChildPermitError",
    "is_retryable_exception",
    "MAX_ROUTES",
    "NoaaCompletionRouteAdapter",
    "PERMANENT_EVENT_REASONS",
    "PermanentEventFailure",
    "ROUTE_CATALOG_CANONICAL",
    "ROUTE_CATALOG_SHA256",
    "ROUTE_ORDER",
    "RawManifestCompletionRouteAdapter",
    "build_completion_route_adapters",
    "retryable_exception_reason",
    "validate_completion_route_catalog",
]
