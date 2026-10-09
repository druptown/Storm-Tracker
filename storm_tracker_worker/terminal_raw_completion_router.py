"""Inert provider-neutral terminal-raw to completion routing authority.

The acquisition lane owns network access and commits, in order, raw objects,
an atomic capture manifest, capacity accounting, an execution receipt and a
terminal scheduler transition.  This module starts strictly *after* those
authorities.  It scans the durable terminal inventory and publishes one
consumer-specific completion per acquisition job.

For every provider except DWD, the completion object is a small canonical
reference to the already verified schema-3 capture manifest.  Raw bytes are
never copied and no provider is fetched again.  DWD deliberately keeps the
existing byte-carrying handoff and consumer identity because its deployed
``dwd-rv-quantitative-decoder-v1`` contract reads the lifecycle payload as the
exact tar archive.  This compatibility exception is explicit in the closed
consumer catalogue.

The router is not imported by ``service.py`` and has no external publication,
snapshot, fact-store, science-orchestrator or operational-admission wiring.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
import threading
from types import MappingProxyType
from typing import Any, Callable, Final, Mapping

from .acquisition_activation import (
    ACTIVATION_RULES,
    ActivationRule,
    ActivationStatus,
)
from .acquisition_capture_recovery import (
    RECOVERY_BINDINGS,
    CaptureRecoveryError,
    RawRecoveryBinding,
    RecoveryEvidenceDigest,
    recovery_binding_sha256,
    recovery_evidence_digest,
)
from .acquisition_evidence_lifecycle import (
    AcquisitionCompletionLookup,
    AcquisitionEvidenceCapacityError,
    AcquisitionEvidenceConflict,
    AcquisitionEvidenceLifecycleError,
    AcquisitionEvidenceLifecycleStore,
    CompletionProducerReceipt,
    desired_authority_proof,
    publish_dwd_completion,
)
from .acquisition_execution_store import (
    ExecutionReceipt,
    ExecutionReceiptError,
    ExecutionReceiptStore,
)
from .acquisition_query_authority import safe_native_job_id
from .acquisition_scheduler_bridge import (
    JOB_TYPE,
    reconstruct_acquisition_job_for_slot,
)
from .autonomous_scheduler import (
    AutonomousScheduler,
    AutonomousSchedulerError,
    SchedulerTerminalProof,
)
from .desired_state import DesiredState, DesiredStateStore
from .raw_artifacts import (
    RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactError,
    RawArtifactReceipt,
    RawArtifactStore,
)
from .raw_capacity_ledger import (
    CapacityReservationReceipt,
    RawCapacityLedger,
    RawCapacityLedgerError,
)
from .worker_acquisition_plan import (
    AcquisitionJob,
    AcquisitionPlan,
    build_worker_acquisition_plan,
    validate_worker_acquisition_job,
    validate_worker_acquisition_plan,
)


CONTRACT: Final = "stv3-worker-terminal-raw-completion-router-v1"
CONSUMER_CATALOG_CONTRACT: Final = (
    "stv3-worker-terminal-raw-consumer-catalog-v1"
)
REFERENCE_PAYLOAD_SCHEMA: Final = (
    "stv3-worker-terminal-raw-capture-reference-v1"
)
REFERENCE_PROOF_SCHEMA: Final = (
    "stv3-worker-terminal-raw-completion-proof-v1"
)
MAX_SCAN_ROWS: Final = 64
MAX_REFERENCE_PAYLOAD_BYTES: Final = 64 * 1024
MAX_REFERENCE_ROLES: Final = 64
MAX_DIAGNOSTIC_BYTES: Final = 4096

DWD_WORKLOAD_ID: Final = "radar.dwd.radolan_rv"
DWD_CONSUMER_ID: Final = "dwd-rv-quantitative-decoder-v1"
DWD_STREAM_ID: Final = "radar.dwd.radolan_rv"
DWD_TRANSFER_MODE: Final = "legacy_exact_raw_payload_v1"
REFERENCE_TRANSFER_MODE: Final = "verified_capture_manifest_reference_v1"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_FORBIDDEN_REFERENCE_KEY = re.compile(
    r"(?:^|[_-])(?:artifact[_-]?path|path|url|uri|authorization|cookie|"
    r"password|passwd|secret|api[_-]?key|application[_-]?key|"
    r"access[_-]?token|refresh[_-]?token|private[_-]?key|provenance|"
    r"upstream[_-]?id|fetch[_-]?id)(?:$|[_-])",
    re.IGNORECASE,
)
_FORBIDDEN_REFERENCE_TEXT = re.compile(
    r"(?:[A-Za-z]:[\\/]|(?:https?|ftp)://|(?:bearer|basic)\s+|"
    r"private_[A-Za-z0-9_-]{8,}|(?:password|passwd|secret|token)=)",
    re.IGNORECASE,
)


class TerminalRawCompletionError(RuntimeError):
    """Base failure for the bounded inert routing boundary."""


class TerminalRawCompletionIntegrityError(TerminalRawCompletionError):
    """One or more durable authorities cannot be cross-bound exactly."""


class TerminalRawCompletionCapacityError(TerminalRawCompletionError):
    """A scan, proof, reference object or durable store reached its cap."""


@dataclass(frozen=True, slots=True)
class TerminalRawConsumerBinding:
    """Closed consumer and transfer semantics for one active workload."""

    workload_id: str
    consumer_id: str
    stream_id: str
    consumer_kind: str
    implementation_id: str
    transfer_mode: str

    @property
    def fingerprint(self) -> str:
        return _digest(self.as_dict())

    @property
    def uses_reference_payload(self) -> bool:
        return self.transfer_mode == REFERENCE_TRANSFER_MODE

    def as_dict(self) -> dict[str, Any]:
        return {
            "consumer_id": self.consumer_id,
            "consumer_kind": self.consumer_kind,
            "contract": CONSUMER_CATALOG_CONTRACT,
            "implementation_id": self.implementation_id,
            "meteorological_admission": False,
            "runtime_wiring": "inert",
            "stream_id": self.stream_id,
            "transfer_mode": self.transfer_mode,
            "workload_id": self.workload_id,
        }


def _consumer(
    workload_id: str,
    consumer_id: str,
    consumer_kind: str,
    implementation_id: str,
    *,
    transfer_mode: str = REFERENCE_TRANSFER_MODE,
) -> TerminalRawConsumerBinding:
    return TerminalRawConsumerBinding(
        workload_id=workload_id,
        consumer_id=consumer_id,
        stream_id=workload_id,
        consumer_kind=consumer_kind,
        implementation_id=implementation_id,
        transfer_mode=transfer_mode,
    )


# This is intentionally hand-written rather than inferred from provider names.
# Adding ACTIVE_SHADOW_RAW work therefore requires an atomic, reviewed consumer
# decision.  Quantitative decoder references below do not claim that decoding
# has run; audit consumers only validate the immutable manifest boundary.
TERMINAL_RAW_CONSUMER_CATALOG = MappingProxyType({
    "ground.awn.authorized_devices": _consumer(
        "ground.awn.authorized_devices",
        "awn-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "ground.kmi.aws_10min": _consumer(
        "ground.kmi.aws_10min",
        "kmi-aws-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "ground.metar.global": _consumer(
        "ground.metar.global",
        "metar-canonical-ground-decoder-reference-v1",
        "decoder_reference",
        "metar_canonical_ground_decoder_v1",
    ),
    "ground.netatmo.region": _consumer(
        "ground.netatmo.region",
        "netatmo-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "ground.synoptic.region": _consumer(
        "ground.synoptic.region",
        "synoptic-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "lightning.eumetsat.li": _consumer(
        "lightning.eumetsat.li",
        "eumetsat-li-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "lightning.goes18.glm": _consumer(
        "lightning.goes18.glm",
        "goes18-glm-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "lightning.goes19.glm": _consumer(
        "lightning.goes19.glm",
        "goes19-glm-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "model.openmeteo.italiameteo_icon2i.region": _consumer(
        "model.openmeteo.italiameteo_icon2i.region",
        "openmeteo-icon2i-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "model.openmeteo.region": _consumer(
        "model.openmeteo.region",
        "openmeteo-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "radar.aemet.composite": _consumer(
        "radar.aemet.composite",
        "aemet-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "radar.dpc.sri": _consumer(
        "radar.dpc.sri",
        "dpc-sri-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    DWD_WORKLOAD_ID: _consumer(
        DWD_WORKLOAD_ID,
        DWD_CONSUMER_ID,
        "decoder",
        "dwd_rv_completion_consumer_v1",
        transfer_mode=DWD_TRANSFER_MODE,
    ),
    "radar.knmi.rtcor": _consumer(
        "radar.knmi.rtcor",
        "knmi-rtcor-quantitative-decoder-reference-v1",
        "decoder_reference",
        "knmi_rtcor_quantitative_v1",
    ),
    "radar.kmi.public_wcs_unknown_quantity": _consumer(
        "radar.kmi.public_wcs_unknown_quantity",
        "kmi-public-wcs-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "radar.kmi.appv4_visual": _consumer(
        "radar.kmi.appv4_visual",
        "kmi-appv4-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "radar.meteofrance.dpradar": _consumer(
        "radar.meteofrance.dpradar",
        "meteofrance-dpradar-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "radar.metoffice.uk_rate": _consumer(
        "radar.metoffice.uk_rate",
        "metoffice-rate-quantitative-decoder-reference-v1",
        "decoder_reference",
        "met_office_rate_quantitative_v1",
    ),
    "radar.opera.dbzh": _consumer(
        "radar.opera.dbzh",
        "opera-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "radar.rainviewer.visual": _consumer(
        "radar.rainviewer.visual",
        "rainviewer-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "satellite.goes18.rrqpe": _consumer(
        "satellite.goes18.rrqpe",
        "goes18-rrqpe-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "satellite.goes19.rrqpe": _consumer(
        "satellite.goes19.rrqpe",
        "goes19-rrqpe-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "satellite.hsaf.h40b": _consumer(
        "satellite.hsaf.h40b",
        "hsaf-h40b-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "satellite.noaa.err": _consumer(
        "satellite.noaa.err",
        "noaa-err-quantitative-decoder-reference-v1",
        "decoder_reference",
        "noaa_err_quantitative_v1",
    ),
    "validation.geosphere.location": _consumer(
        "validation.geosphere.location",
        "geosphere-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "validation.italiameteo.dpc_sri_historical_catalog": _consumer(
        "validation.italiameteo.dpc_sri_historical_catalog",
        "italiameteo-catalog-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
    "validation.meteolux.location": _consumer(
        "validation.meteolux.location",
        "meteolux-raw-manifest-audit-v1",
        "raw_manifest_audit",
        "terminal_raw_reference_audit_v1",
    ),
})


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
        raise TerminalRawCompletionIntegrityError(
            "completion reference is not canonical JSON"
        ) from error


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _assert_reference_safe(value: Any, *, depth: int = 0) -> None:
    if depth > 16:
        raise TerminalRawCompletionCapacityError(
            "completion reference nesting exceeds bound"
        )
    if isinstance(value, Mapping):
        if len(value) > 128:
            raise TerminalRawCompletionCapacityError(
                "completion reference object exceeds bound"
            )
        for key, item in value.items():
            if (
                not isinstance(key, str)
                or _FORBIDDEN_REFERENCE_KEY.search(key) is not None
            ):
                raise TerminalRawCompletionIntegrityError(
                    "completion reference contains a private-capability key"
                )
            _assert_reference_safe(item, depth=depth + 1)
        return
    if isinstance(value, (list, tuple)):
        if len(value) > MAX_REFERENCE_ROLES:
            raise TerminalRawCompletionCapacityError(
                "completion reference sequence exceeds bound"
            )
        for item in value:
            _assert_reference_safe(item, depth=depth + 1)
        return
    if isinstance(value, str):
        if (
            len(value.encode("utf-8")) > 2048
            or _FORBIDDEN_REFERENCE_TEXT.search(value) is not None
        ):
            raise TerminalRawCompletionIntegrityError(
                "completion reference contains private-capability text"
            )
        return
    if value is None or isinstance(value, (bool, int, float)):
        return
    raise TerminalRawCompletionIntegrityError(
        "completion reference contains an unsupported value"
    )


def validate_terminal_raw_consumer_catalog(
    catalog: Mapping[str, TerminalRawConsumerBinding],
) -> None:
    """Prove exact coverage of the current ACTIVE_SHADOW_RAW matrix."""

    if not isinstance(catalog, Mapping):
        raise TerminalRawCompletionIntegrityError(
            "terminal raw consumer catalog must be a mapping"
        )
    active = {
        workload_id: rule
        for workload_id, rule in ACTIVATION_RULES.items()
        if rule.status is ActivationStatus.ACTIVE_SHADOW_RAW
    }
    if set(catalog) != set(active):
        raise TerminalRawCompletionIntegrityError(
            "terminal raw consumer catalog differs from ACTIVE_SHADOW_RAW"
        )
    consumers: set[tuple[str, str]] = set()
    for workload_id, entry in catalog.items():
        rule = active[workload_id]
        binding = RECOVERY_BINDINGS.get(str(rule.handler_id))
        if (
            not isinstance(entry, TerminalRawConsumerBinding)
            or entry.workload_id != workload_id
            or binding is None
            or binding.workload_id != workload_id
            or entry.stream_id != workload_id
            or entry.consumer_kind
            not in {"decoder", "decoder_reference", "raw_manifest_audit"}
            or entry.transfer_mode
            not in {DWD_TRANSFER_MODE, REFERENCE_TRANSFER_MODE}
            or any(
                not isinstance(value, str)
                or _IDENTIFIER.fullmatch(value) is None
                for value in (
                    entry.workload_id,
                    entry.consumer_id,
                    entry.stream_id,
                    entry.consumer_kind,
                    entry.implementation_id,
                )
            )
        ):
            raise TerminalRawCompletionIntegrityError(
                "terminal raw consumer binding is invalid"
            )
        identity = (entry.consumer_id, entry.stream_id)
        if identity in consumers:
            raise TerminalRawCompletionIntegrityError(
                "terminal raw consumer stream identity is duplicated"
            )
        consumers.add(identity)
        if workload_id == DWD_WORKLOAD_ID:
            if (
                entry.consumer_id != DWD_CONSUMER_ID
                or entry.stream_id != DWD_STREAM_ID
                or entry.consumer_kind != "decoder"
                or entry.transfer_mode != DWD_TRANSFER_MODE
            ):
                raise TerminalRawCompletionIntegrityError(
                    "DWD deployed consumer semantics changed"
                )
        elif entry.transfer_mode != REFERENCE_TRANSFER_MODE:
            raise TerminalRawCompletionIntegrityError(
                "non-DWD workload may not copy raw bytes into completion"
            )


validate_terminal_raw_consumer_catalog(TERMINAL_RAW_CONSUMER_CATALOG)


@dataclass(frozen=True, slots=True)
class TerminalRawCompletionResult:
    status: str
    reason_code: str
    scanned_rows: int
    workload_id: str | None = None
    consumer_id: str | None = None
    stream_id: str | None = None
    acquisition_job_id: str | None = None
    producer_receipt_sha256: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "acquisition_job_id": self.acquisition_job_id,
            "consumer_id": self.consumer_id,
            "contract": CONTRACT,
            "producer_receipt_sha256": self.producer_receipt_sha256,
            "reason_code": self.reason_code,
            "scanned_rows": self.scanned_rows,
            "status": self.status,
            "stream_id": self.stream_id,
            "workload_id": self.workload_id,
        }


@dataclass(frozen=True, slots=True)
class _VerifiedTerminalRaw:
    desired_state: DesiredState
    acquisition_plan: AcquisitionPlan
    job: AcquisitionJob
    rule: ActivationRule
    recovery_binding: RawRecoveryBinding
    consumer_binding: TerminalRawConsumerBinding
    scheduler_proof: SchedulerTerminalProof
    executions: tuple[ExecutionReceipt, ...]
    terminal_execution: ExecutionReceipt
    capacity: CapacityReservationReceipt
    capture: RawArtifactCaptureReceipt
    receipts_by_role: Mapping[str, RawArtifactReceipt]
    evidence: RecoveryEvidenceDigest


class TerminalRawCompletionRouter:
    """Publish at most one missing provider completion per serialized call."""

    def __init__(
        self,
        *,
        lifecycle_store: AcquisitionEvidenceLifecycleStore,
        desired_state_store: DesiredStateStore,
        scheduler: AutonomousScheduler,
        execution_store: ExecutionReceiptStore,
        capacity_ledger: RawCapacityLedger,
        source_raw_store: RawArtifactStore,
        dwd_handoff_raw_store: RawArtifactStore | None = None,
        consumer_catalog: Mapping[
            str, TerminalRawConsumerBinding
        ] = TERMINAL_RAW_CONSUMER_CATALOG,
        max_scan_rows: int = MAX_SCAN_ROWS,
        max_reference_payload_bytes: int = MAX_REFERENCE_PAYLOAD_BYTES,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        expected = (
            (lifecycle_store, AcquisitionEvidenceLifecycleStore),
            (desired_state_store, DesiredStateStore),
            (scheduler, AutonomousScheduler),
            (execution_store, ExecutionReceiptStore),
            (capacity_ledger, RawCapacityLedger),
            (source_raw_store, RawArtifactStore),
        )
        if any(not isinstance(value, kind) for value, kind in expected):
            raise TypeError("terminal raw completion authority type is invalid")
        if dwd_handoff_raw_store is not None and not isinstance(
            dwd_handoff_raw_store, RawArtifactStore
        ):
            raise TypeError("DWD handoff raw store type is invalid")
        if (
            isinstance(max_scan_rows, bool)
            or not isinstance(max_scan_rows, int)
            or not 1 <= max_scan_rows <= 1024
        ):
            raise ValueError("terminal raw completion scan bound is invalid")
        if (
            isinstance(max_reference_payload_bytes, bool)
            or not isinstance(max_reference_payload_bytes, int)
            or not 256 <= max_reference_payload_bytes
            <= MAX_REFERENCE_PAYLOAD_BYTES
        ):
            raise ValueError("terminal raw reference byte bound is invalid")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("terminal raw completion fault injector is invalid")
        validate_terminal_raw_consumer_catalog(consumer_catalog)
        self._lifecycle = lifecycle_store
        self._desired = desired_state_store
        self._scheduler = scheduler
        self._execution = execution_store
        self._capacity = capacity_ledger
        self._source_raw = source_raw_store
        self._dwd_handoff = dwd_handoff_raw_store
        self._consumers = MappingProxyType(dict(consumer_catalog))
        self._max_scan_rows = max_scan_rows
        self._max_reference_payload_bytes = max_reference_payload_bytes
        self._fault_injector = fault_injector
        self._cursor = 0
        self._lock = threading.Lock()
        self._last_result = TerminalRawCompletionResult(
            "idle", "not_started", 0
        )
        self._counts = {
            "already_published": 0,
            "calls": 0,
            "failures": 0,
            "legacy_dwd_published": 0,
            "non_success_terminal": 0,
            "reference_published": 0,
            "scanned": 0,
        }

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    @staticmethod
    def _raw_job_identity(
        binding: RawRecoveryBinding,
        acquisition_job_id: str,
    ) -> str:
        if binding.job_identity_kind == "origin_v1":
            return acquisition_job_id
        if binding.job_identity_kind == "safe_native_alias_v1":
            try:
                return safe_native_job_id(acquisition_job_id)
            except ValueError as error:
                raise TerminalRawCompletionIntegrityError(
                    "raw job alias cannot be derived"
                ) from error
        raise TerminalRawCompletionIntegrityError(
            "raw recovery job identity kind is unsupported"
        )

    @staticmethod
    def _receipt_belongs_to_raw_job(
        *,
        receipt: RawArtifactReceipt,
        binding: RawRecoveryBinding,
        role: str,
        raw_job_id: str,
    ) -> bool:
        """Reprove the role suffix without importing recovery internals."""

        member = binding.member(role)
        if member.receipt_attempt_limit is None:
            return receipt.fetch_id == f"{raw_job_id}{member.receipt_suffix}"
        prefix = f"{raw_job_id}{member.receipt_suffix}"
        if not receipt.fetch_id.startswith(prefix):
            return False
        attempt = receipt.fetch_id[len(prefix):]
        return (
            attempt.isascii()
            and attempt.isdigit()
            and not attempt.startswith("0")
            and 1 <= int(attempt) <= member.receipt_attempt_limit
        )

    def _verify_terminal(
        self,
        terminal: ExecutionReceipt,
    ) -> _VerifiedTerminalRaw:
        if (
            not terminal.terminal
            or terminal.outcome != "completed_shadow_raw"
        ):
            raise TerminalRawCompletionIntegrityError(
                "terminal raw verifier received a non-success receipt"
            )
        scheduler_proof = self._scheduler.terminal_proof(
            terminal.scheduler_job_id
        )
        if (
            not isinstance(scheduler_proof, SchedulerTerminalProof)
            or scheduler_proof.state != "completed"
            or scheduler_proof.last_transition != "completed"
            or scheduler_proof.job_type != JOB_TYPE
        ):
            raise TerminalRawCompletionIntegrityError(
                "terminal execution lacks a completed acquisition scheduler proof"
            )
        state = self._desired.load_version(
            scheduler_proof.desired_state_sha256
        )
        if not isinstance(state, DesiredState):
            raise TerminalRawCompletionIntegrityError(
                "terminal desired-state version is unavailable"
            )
        plan = validate_worker_acquisition_plan(
            build_worker_acquisition_plan(state)
        )
        try:
            job = validate_worker_acquisition_job(
                reconstruct_acquisition_job_for_slot(
                    plan,
                    intent_id=scheduler_proof.intent_id,
                    scheduled_for=scheduler_proof.scheduled_for,
                )
            )
        except Exception as error:
            raise TerminalRawCompletionIntegrityError(
                "terminal acquisition slot cannot be reconstructed"
            ) from error
        if (
            job.job_id != terminal.acquisition_job_id
            or job.snapshot_sha256 != state.state_sha256
            or scheduler_proof.desired_state_sha256 != state.state_sha256
        ):
            raise TerminalRawCompletionIntegrityError(
                "terminal acquisition job binding differs"
            )
        rule = ACTIVATION_RULES.get(job.workload_id)
        if (
            not isinstance(rule, ActivationRule)
            or rule.status is not ActivationStatus.ACTIVE_SHADOW_RAW
            or not rule.executable
            or rule.provider_id != job.provider_id
            or rule.product_id != job.product_id
            or job.budget.max_transport_bytes != rule.maximum_transport_bytes
            or job.budget.max_artifacts != rule.maximum_artifacts
            or job.budget.max_wall_seconds != rule.maximum_wall_seconds
        ):
            raise TerminalRawCompletionIntegrityError(
                "terminal workload is outside ACTIVE_SHADOW_RAW"
            )
        recovery = RECOVERY_BINDINGS.get(str(rule.handler_id))
        consumer = self._consumers.get(job.workload_id)
        if recovery is None or consumer is None:
            raise TerminalRawCompletionIntegrityError(
                "terminal workload has no closed recovery/consumer binding"
            )
        if (
            recovery.workload_id != job.workload_id
            or recovery.ledger_provider != job.provider_id
            or recovery.required_roles != rule.required_roles
            or recovery.dynamic_role_pattern != rule.dynamic_role_pattern
            or recovery.dynamic_role_minimum != rule.dynamic_role_minimum
            or recovery.dynamic_role_maximum != rule.dynamic_role_maximum
            or recovery.maximum_transport_bytes != rule.maximum_transport_bytes
            or recovery.maximum_artifacts != rule.maximum_artifacts
            or consumer.workload_id != job.workload_id
        ):
            raise TerminalRawCompletionIntegrityError(
                "terminal recovery or consumer binding drifted"
            )
        executions = self._execution.load_job_receipts(job.job_id)
        terminals = tuple(item for item in executions if item.terminal)
        if len(terminals) != 1 or terminals[0] != terminal:
            raise TerminalRawCompletionIntegrityError(
                "terminal execution inventory is not singular and exact"
            )
        raw_job_id = self._raw_job_identity(recovery, job.job_id)
        expected_capture_id = RawArtifactStore._capture_id(  # noqa: SLF001
            provider=recovery.raw_provider,
            product=recovery.capture_product,
            fetch_id=f"{raw_job_id}{recovery.capture_suffix}",
        )
        bundle = self._source_raw.load_capture_bundle(expected_capture_id)
        if bundle is None:
            raise TerminalRawCompletionIntegrityError(
                "terminal raw capture is unavailable"
            )
        capture, receipts_by_role = bundle
        if (
            capture.capture_id != expected_capture_id
            or capture.provider != recovery.raw_provider
            or capture.product != recovery.capture_product
            or capture.fetch_id != f"{raw_job_id}{recovery.capture_suffix}"
            or len(receipts_by_role) > MAX_REFERENCE_ROLES
        ):
            raise TerminalRawCompletionIntegrityError(
                "terminal raw capture differs from its closed role set"
            )
        try:
            recovery.members_for_roles(receipts_by_role)
        except Exception as error:
            raise TerminalRawCompletionIntegrityError(
                "terminal raw capture differs from its closed role set"
            ) from error
        if any(
            not self._receipt_belongs_to_raw_job(
                receipt=receipt,
                binding=recovery,
                role=role,
                raw_job_id=raw_job_id,
            )
            for role, receipt in receipts_by_role.items()
        ):
            raise TerminalRawCompletionIntegrityError(
                "terminal raw member changed acquisition job identity"
            )
        evidence = recovery_evidence_digest(
            binding=recovery,
            receipts_by_role=receipts_by_role,
            capture=capture,
        )
        capacity = self._capacity.load_for_job(
            provider=recovery.ledger_provider,
            job_identity=job.job_id,
        )
        if (
            not isinstance(capacity, CapacityReservationReceipt)
            or capacity.status != "COMMITTED"
            or capacity.requested_bytes != recovery.maximum_transport_bytes
            or capacity.requested_entries != recovery.maximum_artifacts
            or capacity.actual_bytes != evidence.actual_bytes
            or capacity.actual_entries != evidence.actual_entries
            or capacity.content_sha256 != evidence.content_sha256
            or capacity.receipt_sha256 != evidence.receipt_sha256
            or capacity.capture_sha256 != evidence.capture_sha256
            or capacity.commit_sha256 is None
            or terminal.content_sha256 != evidence.content_sha256
            or terminal.raw_receipt_sha256 != evidence.receipt_sha256
            or terminal.capture_sha256 != evidence.capture_sha256
            or terminal.capacity_commit_sha256 != capacity.commit_sha256
        ):
            raise TerminalRawCompletionIntegrityError(
                "execution, capacity and complete raw role set differ"
            )
        return _VerifiedTerminalRaw(
            desired_state=state,
            acquisition_plan=plan,
            job=job,
            rule=rule,
            recovery_binding=recovery,
            consumer_binding=consumer,
            scheduler_proof=scheduler_proof,
            executions=executions,
            terminal_execution=terminal,
            capacity=capacity,
            capture=capture,
            receipts_by_role=MappingProxyType(dict(receipts_by_role)),
            evidence=evidence,
        )

    @staticmethod
    def _safe_member_reference(
        role: str,
        receipt: RawArtifactReceipt,
    ) -> dict[str, Any]:
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

    def _reference_documents(
        self,
        verified: _VerifiedTerminalRaw,
    ) -> tuple[dict[str, Any], bytes]:
        consumer = verified.consumer_binding
        if not consumer.uses_reference_payload:
            raise TerminalRawCompletionIntegrityError(
                "legacy completion cannot use a manifest reference"
            )
        members = [
            self._safe_member_reference(role, receipt)
            for role, receipt in sorted(verified.receipts_by_role.items())
        ]
        capture_reference = {
            "capture_id": verified.capture.capture_id,
            "capture_receipt_schema": RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
            "capture_receipt_sha256": (
                verified.capture.capture_receipt_sha256
            ),
            "capture_schema": RAW_ARTIFACT_CAPTURE_SCHEMA,
            "manifest_sha256": verified.capture.manifest_sha256,
            "members": members,
            "product": verified.capture.product,
            "provider": verified.capture.provider,
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "role_set_content_sha256": verified.evidence.content_sha256,
            "role_set_receipt_sha256": verified.evidence.receipt_sha256,
            "total_bytes": verified.evidence.actual_bytes,
        }
        payload_document = {
            "acquisition_job_id": verified.job.job_id,
            "capture_reference": capture_reference,
            "claims": {
                "decoded": False,
                "fact_staged": False,
                "meteorological_admission": False,
                "operational_snapshot_published": False,
                "raw_bytes_copied": False,
                "raw_refetch_performed": False,
            },
            "consumer_binding_sha256": consumer.fingerprint,
            "consumer_id": consumer.consumer_id,
            "schema": REFERENCE_PAYLOAD_SCHEMA,
            "stream_id": consumer.stream_id,
            "workload_id": verified.job.workload_id,
        }
        _assert_reference_safe(payload_document)
        payload = _canonical(payload_document)
        if len(payload) > self._max_reference_payload_bytes:
            raise TerminalRawCompletionCapacityError(
                "completion manifest reference exceeds its byte cap"
            )
        authority = desired_authority_proof(verified.desired_state)
        proof = {
            "capacity_commit": {
                "actual_bytes": verified.capacity.actual_bytes,
                "actual_entries": verified.capacity.actual_entries,
                "commit_sha256": verified.capacity.commit_sha256,
                "provider": verified.capacity.provider,
                "request_sha256": verified.capacity.request_sha256,
                "reservation_sha256": verified.capacity.reservation_sha256,
                "status": verified.capacity.status,
            },
            "capture_reference": capture_reference,
            "claims": payload_document["claims"],
            "consumer_binding": consumer.as_dict(),
            "consumer_binding_sha256": consumer.fingerprint,
            "desired_authority": authority,
            "execution_receipt": verified.terminal_execution.as_dict(),
            "recovery_binding_sha256": recovery_binding_sha256(
                verified.recovery_binding
            ),
            "reference_content_sha256": hashlib.sha256(payload).hexdigest(),
            "scheduler_terminal": {
                **verified.scheduler_proof.as_dict(),
                "proof_sha256": verified.scheduler_proof.proof_sha256,
            },
            "schema": REFERENCE_PROOF_SCHEMA,
            "workload": {
                "job_id": verified.job.job_id,
                "job_sha256": verified.job.job_sha256,
                "plan_sha256": verified.job.plan_sha256,
                "product_id": verified.job.product_id,
                "provider_id": verified.job.provider_id,
                "snapshot_sha256": verified.job.snapshot_sha256,
                "workload_id": verified.job.workload_id,
            },
        }
        _assert_reference_safe(proof)
        return proof, payload

    def _publish_verified(
        self,
        verified: _VerifiedTerminalRaw,
    ) -> CompletionProducerReceipt:
        self._fault("completion.after_authorities_verified")
        consumer = verified.consumer_binding
        self._fault("completion.before_lifecycle_publish")
        if consumer.transfer_mode == DWD_TRANSFER_MODE:
            if self._dwd_handoff is None:
                raise TerminalRawCompletionIntegrityError(
                    "DWD legacy consumer requires its exact handoff raw store"
                )
            producer = publish_dwd_completion(
                lifecycle_store=self._lifecycle,
                acquisition_plan=verified.acquisition_plan,
                desired_state=verified.desired_state,
                scheduler=self._scheduler,
                execution_store=self._execution,
                capacity_ledger=self._capacity,
                source_raw_store=self._source_raw,
                handoff_raw_store=self._dwd_handoff,
                source_capture_id=verified.capture.capture_id,
                acquisition_job_id=verified.job.job_id,
                consumer_id=DWD_CONSUMER_ID,
            )
        else:
            proof, payload = self._reference_documents(verified)
            producer = self._lifecycle.publish(
                acquisition_job_id=verified.job.job_id,
                consumer_id=consumer.consumer_id,
                stream_id=consumer.stream_id,
                desired_state_sha256=verified.desired_state.state_sha256,
                authority_sha256=proof["desired_authority"][
                    "authority_sha256"
                ],
                proof=proof,
                payload=payload,
            )
        if (
            not isinstance(producer, CompletionProducerReceipt)
            or producer.acquisition_job_id != verified.job.job_id
            or producer.consumer_id != consumer.consumer_id
            or producer.stream_id != consumer.stream_id
        ):
            raise TerminalRawCompletionIntegrityError(
                "lifecycle publisher returned a different consumer binding"
            )
        self._fault("completion.after_lifecycle_publish")
        return producer

    def _reprove_existing(
        self,
        verified: _VerifiedTerminalRaw,
        existing: AcquisitionCompletionLookup,
    ) -> None:
        """Require byte-exact expected completion before cursor suppression."""

        if not isinstance(existing, AcquisitionCompletionLookup):
            raise TerminalRawCompletionIntegrityError(
                "existing lifecycle completion has an invalid lookup type"
            )
        producer = existing.producer_receipt
        consumer = verified.consumer_binding
        if (
            producer.acquisition_job_id != verified.job.job_id
            or producer.consumer_id != consumer.consumer_id
            or producer.stream_id != consumer.stream_id
            or producer.desired_state_sha256
            != verified.desired_state.state_sha256
        ):
            raise TerminalRawCompletionIntegrityError(
                "existing lifecycle completion changed its closed route"
            )
        if consumer.transfer_mode == DWD_TRANSFER_MODE:
            if self._dwd_handoff is None:
                raise TerminalRawCompletionIntegrityError(
                    "DWD existing completion cannot be reproved without handoff store"
                )
            # The compatibility publisher rebuilds the exact DWD proof/payload
            # and the lifecycle store compares it with the existing hot or
            # archived row. Its handoff writes are content-addressed/idempotent.
            replay = publish_dwd_completion(
                lifecycle_store=self._lifecycle,
                acquisition_plan=verified.acquisition_plan,
                desired_state=verified.desired_state,
                scheduler=self._scheduler,
                execution_store=self._execution,
                capacity_ledger=self._capacity,
                source_raw_store=self._source_raw,
                handoff_raw_store=self._dwd_handoff,
                source_capture_id=verified.capture.capture_id,
                acquisition_job_id=verified.job.job_id,
                consumer_id=DWD_CONSUMER_ID,
            )
            if replay != producer:
                raise TerminalRawCompletionIntegrityError(
                    "existing DWD completion differs after exact replay"
                )
            return
        proof, payload = self._reference_documents(verified)
        expected_content_sha = hashlib.sha256(payload).hexdigest()
        expected_proof_sha = hashlib.sha256(_canonical(proof)).hexdigest()
        if (
            existing.proof != proof
            or existing.payload != payload
            or producer.content_sha256 != expected_content_sha
            or producer.size_bytes != len(payload)
            or producer.proof_sha256 != expected_proof_sha
            or producer.authority_sha256
            != proof["desired_authority"]["authority_sha256"]
        ):
            raise TerminalRawCompletionIntegrityError(
                "existing lifecycle completion is not the exact expected reference"
            )

    def publish_next(self) -> TerminalRawCompletionResult:
        """Publish one missing completion, or make bounded scan progress."""

        if not self._lock.acquire(blocking=False):
            return TerminalRawCompletionResult(
                "busy", "router_call_in_progress", 0
            )
        try:
            self._counts["calls"] += 1
            page = self._execution.terminal_receipt_page(
                after_cursor=self._cursor,
                max_rows=self._max_scan_rows,
            )
            scanned = 0
            for terminal_cursor, terminal in zip(
                page.cursors, page.receipts, strict=True
            ):
                scanned += 1
                if terminal.outcome != "completed_shadow_raw":
                    self._cursor = terminal_cursor
                    self._counts["non_success_terminal"] += 1
                    continue
                verified = self._verify_terminal(terminal)
                existing = self._lifecycle.load_acquisition_completion(
                    terminal.acquisition_job_id
                )
                if existing is not None:
                    self._reprove_existing(verified, existing)
                    self._cursor = terminal_cursor
                    self._counts["already_published"] += 1
                    continue
                producer = self._publish_verified(verified)
                self._cursor = terminal_cursor
                self._counts["scanned"] += scanned
                key = (
                    "legacy_dwd_published"
                    if verified.consumer_binding.transfer_mode
                    == DWD_TRANSFER_MODE
                    else "reference_published"
                )
                self._counts[key] += 1
                result = TerminalRawCompletionResult(
                    "published",
                    (
                        "exact_legacy_dwd_completion_published"
                        if key == "legacy_dwd_published"
                        else "verified_capture_reference_published"
                    ),
                    scanned,
                    workload_id=verified.job.workload_id,
                    consumer_id=producer.consumer_id,
                    stream_id=producer.stream_id,
                    acquisition_job_id=verified.job.job_id,
                    producer_receipt_sha256=(
                        producer.producer_receipt_sha256
                    ),
                )
                self._last_result = result
                return result
            self._counts["scanned"] += scanned
            result = TerminalRawCompletionResult(
                "idle",
                (
                    "terminal_inventory_exhausted"
                    if page.exhausted
                    else "terminal_scan_page_complete"
                ),
                scanned,
            )
            self._last_result = result
            return result
        except TerminalRawCompletionError:
            self._counts["failures"] += 1
            raise
        except AcquisitionEvidenceCapacityError as error:
            self._counts["failures"] += 1
            raise TerminalRawCompletionCapacityError(
                "completion lifecycle rejected its bounded object"
            ) from error
        except (
            AcquisitionEvidenceLifecycleError,
            AutonomousSchedulerError,
            CaptureRecoveryError,
            ExecutionReceiptError,
            RawArtifactError,
            RawCapacityLedgerError,
            OSError,
            ValueError,
        ) as error:
            self._counts["failures"] += 1
            raise TerminalRawCompletionIntegrityError(
                "terminal raw completion authority verification failed"
            ) from error
        except Exception as error:
            self._counts["failures"] += 1
            raise TerminalRawCompletionIntegrityError(
                "terminal raw completion failed closed"
            ) from error
        finally:
            self._lock.release()

    def diagnostics(self) -> dict[str, Any]:
        result = {
            "active_consumer_bindings": len(self._consumers),
            "contract": CONTRACT,
            "counts": dict(self._counts),
            "cursor": self._cursor,
            "dwd_compatibility_mode": DWD_TRANSFER_MODE,
            "last_result": self._last_result.as_dict(),
            "max_reference_payload_bytes": (
                self._max_reference_payload_bytes
            ),
            "max_scan_rows": self._max_scan_rows,
            "meteorological_admission": False,
            "operational_fact_staging": False,
            "operational_snapshot_publication": False,
            "reference_mode": REFERENCE_TRANSFER_MODE,
            "runtime_wiring": "inert",
            "source_reclaim_authorized": False,
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise TerminalRawCompletionCapacityError(
                "terminal raw completion diagnostics exceed bound"
            )
        result["diagnostics_sha256"] = _digest(result)
        return result


__all__ = [
    "CONSUMER_CATALOG_CONTRACT",
    "CONTRACT",
    "DWD_CONSUMER_ID",
    "DWD_STREAM_ID",
    "DWD_TRANSFER_MODE",
    "DWD_WORKLOAD_ID",
    "MAX_REFERENCE_PAYLOAD_BYTES",
    "REFERENCE_PAYLOAD_SCHEMA",
    "REFERENCE_PROOF_SCHEMA",
    "REFERENCE_TRANSFER_MODE",
    "TERMINAL_RAW_CONSUMER_CATALOG",
    "TerminalRawCompletionCapacityError",
    "TerminalRawCompletionError",
    "TerminalRawCompletionIntegrityError",
    "TerminalRawCompletionResult",
    "TerminalRawCompletionRouter",
    "TerminalRawConsumerBinding",
    "validate_terminal_raw_consumer_catalog",
]
