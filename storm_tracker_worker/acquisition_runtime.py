"""Unwired end-to-end shadow runtime for worker-owned acquisition.

This candidate connects desired state, deterministic planning, the durable
serial scheduler, a closed activation matrix, bounded raw capacity, fresh
process execution and durable technical receipts.  It is intentionally not
imported by ``service.py`` and cannot publish an operational snapshot.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import asdict, dataclass
from functools import partial
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from types import MappingProxyType
from typing import Any, Callable, Mapping, Protocol

from . import acquisition_scheduler_bridge as bridge
from . import autonomous_scheduler as scheduler_module
from . import worker_acquisition_plan as acquisition
from .acquisition_activation import (
    ACTIVATION_RULES,
    DEFAULT_SHADOW_WORKLOAD_IDS,
    ActivationDecision,
    ActivationRule,
    NullSecretResolver,
    SecretResolver,
    activation_matrix_snapshot,
    assess_acquisition_job,
)
from .acquisition_capture_recovery import (
    RECOVERY_BINDINGS,
    CaptureCapacityReconciler,
    CaptureRecoveryBusyError,
    CaptureRecoveryCapacityError,
    CaptureRecoveryIntegrityError,
    RecoveredCaptureEvidence,
    RawRecoveryBinding,
)
from .acquisition_execution_store import ExecutionReceipt, ExecutionReceiptStore
from .acquisition_handler_child import (
    CONTRACT as CHILD_CONTRACT,
    RESULT_CONTRACT as CHILD_RESULT_CONTRACT,
)
from .desired_state import DesiredState, DesiredStateStore
from .hard_decoder_isolation import (
    DecoderEffectContract,
    DecoderProcessResult,
    DecoderResourceLimits,
    HardDecoderExecutor,
)
from .raw_artifacts import (
    RawArtifactCaptureReceipt,
    RawArtifactLimits,
    RawArtifactStore,
)
from .raw_capacity_ledger import (
    RawCapacityLedger,
    RawCapacityLedgerBusyError,
    RawCapacityLedgerCapacityError,
    RawCapacityLedgerConflict,
    RawCapacityLedgerIntegrityError,
    RawCapacityProfile,
)


CONTRACT = "stv3-worker-acquisition-shadow-runtime-v3"
MAX_DIAGNOSTIC_BYTES = 16 * 1024
MAX_CHILD_RESULT_BYTES = 16 * 1024
MAX_CHILD_REQUEST_BYTES = 16 * 1024
MAX_EXECUTOR_WALL_SECONDS = 300.0
ADAPTIVE_ATTEMPT_WALL_SECONDS = 20.0
ADAPTIVE_ATTEMPT_FAILURE_THRESHOLD = 2
ADAPTIVE_FULL_PROBE_INTERVAL = 6
DEFAULT_OWNER = "acquisition-shadow-v3"
DEADLINE_BINDING_CONTRACT = "stv3-worker-acquisition-deadline-binding-v1"
CANARY_CHILD_ENVIRONMENT_ALLOWLIST = frozenset({
    "COMSPEC",
    "GDAL_DATA",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "PATH",
    "PATHEXT",
    "PROJ_DATA",
    "PROJ_LIB",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "TMPDIR",
    "TZ",
    "WINDIR",
})
RAW_CAPTURE_CHILD_ENVIRONMENT_ALLOWLIST = frozenset(
    set(CANARY_CHILD_ENVIRONMENT_ALLOWLIST)
    | {
        "STV3_AEMET_API_KEY",
        "STV3_HSAF_PASSWORD",
        "STV3_HSAF_USERNAME",
        "STV3_KNMI_API_KEY",
        "STV3_METEOFRANCE_API_TOKEN",
        "STV3_METEOFRANCE_APPLICATION_ID",
        "STV3_EUMETSAT_EPHEMERAL_ACCESS_TOKEN",
        "STV3_NETATMO_ACCESS_TOKEN",
        "STV3_SYNOPTIC_TOKEN",
    }
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_REASON = re.compile(r"^[a-z][a-z0-9_]{0,95}$")
_RECOVERABLE_SCHEDULER_DESIRED_STATE_REASONS = frozenset({
    "desired_state_changed_during_plan",
    "desired_state_future",
    "desired_state_stale",
})
_RAW_CAPTURE_EFFECT = DecoderEffectContract(
    contract_id="acquisition.closed.raw.v3",
    hard_kill_safe=True,
    durable_effects="idempotent",
    spawns_descendants=False,
)
_ACTIVE_SHADOW_RAW_WORKLOAD_IDS = frozenset(
    workload_id
    for workload_id, rule in ACTIVATION_RULES.items()
    if rule.executable
)
_PRIMARY_QUANTITATIVE_WORKLOAD_IDS = frozenset({
    "radar.dwd.radolan_rv",
})
_STATIC_REQUIREMENT_UNAVAILABLE_REASONS = frozenset(
    {"secret_requirement_unavailable"}
    | {
        rule.authorization_unavailable_reason_code
        for rule in ACTIVATION_RULES.values()
        if rule.authorization_unavailable_reason_code is not None
    }
)


class AcquisitionRuntimeError(RuntimeError):
    pass


class ParentCredentialTemporaryError(RuntimeError):
    """A parent-only credential transport may be retried without detail."""


class ParentCredentialPermanentError(RuntimeError):
    """A parent-only credential contract failed closed without detail."""


def _validated_allowed_workload_ids(
    value: frozenset[str] | None,
) -> frozenset[str] | None:
    """Validate one immutable runtime-local subset of executable workloads."""

    if value is None:
        return None
    if type(value) is not frozenset:
        raise TypeError("allowed_workload_ids must be an immutable frozenset")
    if any(type(workload_id) is not str for workload_id in value):
        raise TypeError("allowed_workload_ids must contain only strings")
    if not value <= _ACTIVE_SHADOW_RAW_WORKLOAD_IDS:
        raise ValueError(
            "allowed_workload_ids contains a workload outside ACTIVE_SHADOW_RAW"
        )
    return value


@dataclass(frozen=True, slots=True)
class _RuntimeSelection:
    explicit: bool
    workload_ids: frozenset[str]
    activation_rules: Mapping[str, ActivationRule]
    recovery_bindings: Mapping[str, RawRecoveryBinding]


def _runtime_selection(
    value: frozenset[str] | None,
) -> _RuntimeSelection:
    allowed = _validated_allowed_workload_ids(value)
    workload_ids = (
        DEFAULT_SHADOW_WORKLOAD_IDS if allowed is None else allowed
    )
    rules = MappingProxyType({
        workload_id: ACTIVATION_RULES[workload_id]
        for workload_id in sorted(workload_ids)
    })
    handler_ids = {
        str(rule.handler_id) for rule in rules.values() if rule.handler_id is not None
    }
    bindings = MappingProxyType({
        handler_id: RECOVERY_BINDINGS[handler_id]
        for handler_id in sorted(handler_ids)
    })
    return _RuntimeSelection(
        explicit=allowed is not None,
        workload_ids=frozenset(workload_ids),
        activation_rules=rules,
        recovery_bindings=bindings,
    )


class IsolatedExecutor(Protocol):
    def execute(
        self,
        *,
        target: str,
        request: Mapping[str, Any],
        effect_contract: DecoderEffectContract,
        cancel_event: threading.Event | None = None,
        environment_overrides: Mapping[str, str] | None = None,
    ) -> DecoderProcessResult: ...


ExecutorFactory = Callable[[float], IsolatedExecutor]
NetatmoAccessTokenBroker = Callable[[Mapping[str, Any], float], str]
EumetsatAccessTokenBroker = Callable[[float], str]


@dataclass(frozen=True, slots=True)
class ShadowRuntimePaths:
    desired_state: Path
    scheduler: Path
    capacity_ledger: Path
    execution_receipts: Path
    raw_store_root: Path

    def __post_init__(self) -> None:
        for value in asdict(self).values():
            if not Path(value).is_absolute():
                raise ValueError("all shadow runtime paths must be absolute")


@dataclass(frozen=True, slots=True)
class ShadowTickResult:
    status: str
    reason_code: str
    scheduler_transition: str | None
    receipt_sha256: str | None
    attempt_count: int | None
    workload_id: str | None
    terminal: bool
    acquisition_job_id: str | None = None
    desired_state_sha256: str | None = None
    source_capture_id: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "attempt_count": self.attempt_count,
            "acquisition_job_id": self.acquisition_job_id,
            "contract": CONTRACT,
            "desired_state_sha256": self.desired_state_sha256,
            "reason_code": self.reason_code,
            "receipt_sha256": self.receipt_sha256,
            "scheduler_transition": self.scheduler_transition,
            "source_capture_id": self.source_capture_id,
            "status": self.status,
            "terminal": self.terminal,
            "workload_id": self.workload_id,
        }


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as err:
        raise AcquisitionRuntimeError("runtime document is not canonical JSON") from err


def _raw_limits_mapping(limits: RawArtifactLimits) -> dict[str, int]:
    return {
        "max_capture_entries": limits.max_capture_entries,
        "max_capture_members": limits.max_capture_members,
        "max_catalog_bytes": limits.max_catalog_bytes,
        "max_object_bytes": limits.max_object_bytes,
        "max_object_entries": limits.max_object_entries,
        "max_receipt_entries": limits.max_receipt_entries,
        "max_staging_entries": limits.max_staging_entries,
        "max_total_bytes": limits.max_total_bytes,
    }


def _default_executor_factory(wall_seconds: float) -> HardDecoderExecutor:
    # The largest closed candidate contract (GOES RRQPE) owns 105 seconds.
    # ``tick`` supplies the smaller of the exact admitted rule/job budget and
    # the remaining scheduler deadline, so this hard ceiling can only shorten
    # a particular attempt and can never extend its execution authority.
    wall = max(0.05, min(float(wall_seconds), MAX_EXECUTOR_WALL_SECONDS))
    return HardDecoderExecutor(
        limits=DecoderResourceLimits(
            wall_seconds=wall,
            cpu_seconds=wall,
            memory_bytes=512 * 1024 * 1024,
            max_request_bytes=MAX_CHILD_REQUEST_BYTES,
            max_result_bytes=MAX_CHILD_RESULT_BYTES,
        )
    )


def _dwd_canary_executor_factory(wall_seconds: float) -> HardDecoderExecutor:
    """Build the DWD-only executor without ambient worker credentials."""

    wall = max(0.05, min(float(wall_seconds), MAX_EXECUTOR_WALL_SECONDS))
    return HardDecoderExecutor(
        limits=DecoderResourceLimits(
            wall_seconds=wall,
            cpu_seconds=wall,
            memory_bytes=512 * 1024 * 1024,
            max_request_bytes=MAX_CHILD_REQUEST_BYTES,
            max_result_bytes=MAX_CHILD_RESULT_BYTES,
        ),
        environment_allowlist=CANARY_CHILD_ENVIRONMENT_ALLOWLIST,
    )


def raw_capture_executor_factory(wall_seconds: float) -> HardDecoderExecutor:
    """Build the all-source raw executor with a closed provider-secret set."""

    wall = max(0.05, min(float(wall_seconds), MAX_EXECUTOR_WALL_SECONDS))
    return HardDecoderExecutor(
        limits=DecoderResourceLimits(
            wall_seconds=wall,
            cpu_seconds=wall,
            memory_bytes=512 * 1024 * 1024,
            max_request_bytes=MAX_CHILD_REQUEST_BYTES,
            max_result_bytes=MAX_CHILD_RESULT_BYTES,
        ),
        environment_allowlist=RAW_CAPTURE_CHILD_ENVIRONMENT_ALLOWLIST,
    )


def _reservation_id(job: acquisition.AcquisitionJob) -> str:
    digest = hashlib.sha256(
        _canonical({
            "acquisition_job_id": job.job_id,
            "job_sha256": job.job_sha256,
            "plan_sha256": job.plan_sha256,
        })
    ).hexdigest()
    return f"acqcap:{digest}"


class AcquisitionShadowRuntime:
    """One-at-a-time, crash-replayable execution coordinator."""

    def __init__(
        self,
        *,
        desired_state_store: DesiredStateStore,
        scheduler: scheduler_module.AutonomousScheduler,
        capacity_ledger: RawCapacityLedger,
        execution_store: ExecutionReceiptStore,
        raw_store_root: str | Path,
        raw_store_limits: RawArtifactLimits,
        secret_resolver: SecretResolver | None = None,
        executor_factory: ExecutorFactory = _default_executor_factory,
        netatmo_access_token_broker: NetatmoAccessTokenBroker | None = None,
        eumetsat_access_token_broker: EumetsatAccessTokenBroker | None = None,
        lease_owner: str = DEFAULT_OWNER,
        allowed_workload_ids: frozenset[str] | None = None,
        collector_scope: str = acquisition.COLLECTOR_SCOPE_COVERAGE_RELEVANT,
        clock: Callable[[], float] = time.time,
        capture_recovery_fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        if not isinstance(desired_state_store, DesiredStateStore):
            raise TypeError("desired_state_store must be DesiredStateStore")
        if not isinstance(scheduler, scheduler_module.AutonomousScheduler):
            raise TypeError("scheduler must be AutonomousScheduler")
        if not isinstance(capacity_ledger, RawCapacityLedger):
            raise TypeError("capacity_ledger must be RawCapacityLedger")
        if not isinstance(execution_store, ExecutionReceiptStore):
            raise TypeError("execution_store must be ExecutionReceiptStore")
        if not isinstance(raw_store_limits, RawArtifactLimits):
            raise TypeError("raw_store_limits must be RawArtifactLimits")
        root = Path(raw_store_root)
        if not root.is_absolute() or root == Path(root.anchor):
            raise ValueError("raw store root must be a specific absolute directory")
        if (
            not callable(executor_factory)
            or not callable(clock)
            or (
                netatmo_access_token_broker is not None
                and not callable(netatmo_access_token_broker)
            )
            or (
                eumetsat_access_token_broker is not None
                and not callable(eumetsat_access_token_broker)
            )
        ):
            raise TypeError("runtime factories must be callable")
        if (
            not isinstance(lease_owner, str)
            or not lease_owner
            or len(lease_owner) > 192
        ):
            raise ValueError("invalid acquisition lease owner")
        selection = _runtime_selection(allowed_workload_ids)
        validated_collector_scope = acquisition.validate_collector_scope(
            collector_scope
        )
        self._desired_state_store = desired_state_store
        self._scheduler = scheduler
        self._capacity_ledger = capacity_ledger
        self._execution_store = execution_store
        self._raw_store_root = root
        self._raw_store_limits = raw_store_limits
        self._secret_resolver = secret_resolver or NullSecretResolver()
        self._executor_factory = executor_factory
        self._netatmo_access_token_broker = netatmo_access_token_broker
        self._eumetsat_access_token_broker = eumetsat_access_token_broker
        self._lease_owner = lease_owner
        self._selection = selection
        self._allowed_workload_ids = (
            selection.workload_ids if selection.explicit else None
        )
        self._collector_scope = validated_collector_scope
        self._primary_quantitative_workload_ids = (
            _PRIMARY_QUANTITATIVE_WORKLOAD_IDS.intersection(
                selection.workload_ids
            )
            if validated_collector_scope
            == acquisition.COLLECTOR_SCOPE_ALL_SOURCES
            else frozenset()
        )
        self._clock = clock
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._missing_requirement_suppressions: set[str] = set()
        self._last_tick = ShadowTickResult(
            "idle", "not_started", None, None, None, None, False
        )
        self._latest_terminal_observations: dict[
            str, tuple[tuple[float, str, str], dict[str, str]]
        ] = {}
        self._consecutive_terminal_failures: dict[str, int] = {}
        self._raw_store = self._validate_storage_envelope()
        self._capture_reconciler = CaptureCapacityReconciler(
            raw_store=self._raw_store,
            capacity_ledger=self._capacity_ledger,
            fault_injector=capture_recovery_fault_injector,
            recovery_bindings=selection.recovery_bindings,
            activation_rules=selection.activation_rules,
            selected_binding_ids=(
                frozenset(selection.recovery_bindings)
                if selection.explicit else None
            ),
            selected_workload_ids=(
                selection.workload_ids if selection.explicit else None
            ),
        )
        try:
            self._startup_recovery = self._capture_reconciler.audit_existing()
        except (CaptureRecoveryBusyError, CaptureRecoveryCapacityError,
                CaptureRecoveryIntegrityError) as err:
            raise AcquisitionRuntimeError(
                "existing raw capture accounting failed closed"
            ) from err
        self._recover_terminal_observations()

    @staticmethod
    def _terminal_status(receipt: ExecutionReceipt) -> str:
        return (
            "completed_shadow_raw"
            if receipt.outcome == "completed_shadow_raw"
            else "blocked"
        )

    def _remember_terminal_observation(
        self,
        workload_id: str,
        receipt: ExecutionReceipt,
    ) -> None:
        if workload_id not in self._selection.workload_ids or not receipt.terminal:
            return
        order = (
            receipt.created_at,
            receipt.acquisition_job_id,
            receipt.receipt_sha256,
        )
        previous = self._latest_terminal_observations.get(workload_id)
        if previous is not None and previous[0] >= order:
            return
        if receipt.outcome == "completed_shadow_raw":
            self._consecutive_terminal_failures.pop(workload_id, None)
        elif receipt.attempt_count > 0:
            self._consecutive_terminal_failures[workload_id] = min(
                1_000_000,
                self._consecutive_terminal_failures.get(workload_id, 0) + 1,
            )
        self._latest_terminal_observations[workload_id] = (
            order,
            {
                "reason_code": receipt.reason_code,
                "status": self._terminal_status(receipt),
            },
        )

    def _recover_terminal_observations(self) -> None:
        """Rebuild one safe terminal result per workload after restart."""

        terminals = self._execution_store.terminal_receipts()
        workloads_by_job = self._scheduler.workload_ids_for_jobs(tuple(
            receipt.scheduler_job_id for receipt in terminals
        ))
        for receipt in sorted(
            terminals,
            key=lambda item: (
                item.created_at,
                item.acquisition_job_id,
                item.receipt_sha256,
            ),
        ):
            workload_id = workloads_by_job.get(receipt.scheduler_job_id)
            if workload_id is not None:
                self._remember_terminal_observation(workload_id, receipt)

    def _attempt_wall_seconds(
        self,
        job: acquisition.AcquisitionJob,
        *,
        rule_wall_seconds: float,
        deadline_remaining_seconds: float,
    ) -> float:
        """Bound repeat failures without permanently excluding slow products.

        A provider that repeatedly consumes its complete wall budget can starve
        the single crash-safe acquisition lane.  After two executed terminal
        failures it therefore receives short probes.  One deterministic slot
        in every six retains the complete native budget, so a legitimately
        large product can still recover without an operator restart.
        """

        remaining = min(rule_wall_seconds, deadline_remaining_seconds)
        failures = self._consecutive_terminal_failures.get(job.workload_id, 0)
        if failures < ADAPTIVE_ATTEMPT_FAILURE_THRESHOLD:
            return remaining
        phase = int(
            hashlib.sha256(job.workload_id.encode("utf-8")).hexdigest()[:8],
            16,
        ) % ADAPTIVE_FULL_PROBE_INTERVAL
        if job.slot_index % ADAPTIVE_FULL_PROBE_INTERVAL == phase:
            return remaining
        return min(remaining, ADAPTIVE_ATTEMPT_WALL_SECONDS)

    def _validate_storage_envelope(self) -> RawArtifactStore:
        profile = self._capacity_ledger.profile
        limits = self._raw_store_limits
        provider_limits = {
            item.provider: item for item in profile.providers
        }
        active = tuple(self._selection.activation_rules.values())
        provider_batch_requirements: dict[str, tuple[int, int]] = {}
        for rule in active:
            provider_capacity = provider_limits.get(rule.provider_id)
            if (
                provider_capacity is None
                or rule.maximum_transport_bytes is None
                or rule.maximum_artifacts is None
                or provider_capacity.max_bytes < rule.maximum_transport_bytes
                or provider_capacity.max_entries < rule.maximum_artifacts
            ):
                raise AcquisitionRuntimeError(
                    "capacity profile cannot admit every active raw handler"
                )
            prior_bytes, prior_entries = provider_batch_requirements.get(
                rule.provider_id,
                (0, 0),
            )
            provider_batch_requirements[rule.provider_id] = (
                prior_bytes + int(rule.maximum_transport_bytes),
                prior_entries + int(rule.maximum_artifacts),
            )
        for provider, (required_bytes, required_entries) in (
            provider_batch_requirements.items()
        ):
            provider_capacity = provider_limits[provider]
            if (
                provider_capacity.max_bytes < required_bytes
                or provider_capacity.max_entries < required_entries
            ):
                raise AcquisitionRuntimeError(
                    "provider capacity cannot admit one closed active handler batch"
                )
        required_batch_bytes = sum(
            int(rule.maximum_transport_bytes or 0) for rule in active
        )
        required_batch_entries = sum(
            int(rule.maximum_artifacts or 0) for rule in active
        )
        if (
            profile.global_max_bytes < required_batch_bytes
            or profile.global_max_entries < required_batch_entries
            or profile.max_reservations < len(active)
        ):
            raise AcquisitionRuntimeError(
                "global capacity cannot admit one closed active handler batch"
            )
        if limits.max_total_bytes > profile.global_max_bytes:
            raise AcquisitionRuntimeError("raw store exceeds global capacity profile")
        if limits.max_object_entries > profile.global_max_entries:
            raise AcquisitionRuntimeError("raw object count exceeds capacity profile")
        if (
            limits.max_total_bytes < required_batch_bytes
            or limits.max_object_entries < required_batch_entries
            or limits.max_capture_entries < len(active)
            or limits.max_receipt_entries < required_batch_entries
        ):
            raise AcquisitionRuntimeError(
                "raw store cannot retain one closed active handler batch"
            )
        if active:
            maximum_object = max(
                int(rule.maximum_transport_bytes or 0) for rule in active
            )
            if limits.max_object_bytes != maximum_object:
                raise AcquisitionRuntimeError(
                    "raw object bound must equal the largest active transport envelope"
                )
            if limits.max_capture_members < max(
                int(rule.maximum_artifacts or 0) for rule in active
            ):
                raise AcquisitionRuntimeError("raw capture member cap is too small")
        store = RawArtifactStore(self._raw_store_root, limits=limits)
        raw = store.initialize()
        capacity = self._capacity_ledger.stats()
        accounted_bytes = capacity["reserved_bytes"] + capacity["committed_bytes"]
        accounted_entries = capacity["reserved_entries"] + capacity["committed_entries"]
        if raw["catalog_bytes"] > accounted_bytes:
            raise AcquisitionRuntimeError("raw bytes exist outside the capacity ledger")
        if raw["object_count"] > accounted_entries:
            raise AcquisitionRuntimeError("raw objects exist outside the capacity ledger")
        return store

    def _transition_time(self, requested: float) -> float:
        value = max(float(requested), float(self._clock()))
        if not math.isfinite(value):
            raise AcquisitionRuntimeError("runtime clock is not finite")
        return value

    def _workload_is_allowed(self, workload_id: str) -> bool:
        allowed = self._allowed_workload_ids
        if allowed is not None:
            return workload_id in allowed
        rule = ACTIVATION_RULES.get(workload_id)
        return (
            workload_id in self._selection.workload_ids
            or rule is None
            or not rule.executable
        )

    def _available_requirement_count(
        self,
        requirement_ids: tuple[str, ...],
    ) -> tuple[int, int]:
        """Resolve presence only; never retain, hash, or transport a value."""

        available = 0
        for requirement_id in requirement_ids:
            try:
                marker = self._secret_resolver.resolve(requirement_id)
            except Exception:
                marker = None
            if (
                isinstance(marker, str)
                and marker
                and len(marker) <= 16 * 1024
                and "\x00" not in marker
            ):
                available += 1
            marker = None
        return available, len(requirement_ids)

    def _scheduler_workload_ids(self) -> frozenset[str]:
        """Return the static workload ownership represented by this runtime."""

        if self._allowed_workload_ids is not None:
            return self._allowed_workload_ids
        return frozenset(ACTIVATION_RULES)

    def _requirements_ready_workload_ids(self) -> frozenset[str]:
        """Return current static-requirement-ready claims.

        The activation assessment still repeats the checks after the lease.
        This presence-only preview only keeps a known missing credential or
        operator authority from consuming one serial provider slot forever.
        """

        ready = set()
        for workload_id in self._scheduler_workload_ids():
            rule = ACTIVATION_RULES[workload_id]
            requirement_ids = tuple(
                rule.secret_requirement_ids
                + rule.authorization_requirement_ids
            )
            available, total = self._available_requirement_count(requirement_ids)
            if available == total:
                ready.add(workload_id)
        return frozenset(ready)

    def _record(
        self,
        bound: bridge.BoundAcquisitionClaim,
        claimed: scheduler_module.ClaimedJob,
        *,
        outcome: str,
        reason_code: str,
        terminal: bool,
        evidence: Mapping[str, str] | None = None,
    ):
        if _SAFE_REASON.fullmatch(reason_code) is None:
            reason_code = "unclassified_runtime_failure"
        evidence = evidence or {}
        receipt = self._execution_store.record(
            scheduler_job_id=claimed.job_id,
            acquisition_job_id=bound.acquisition_job.job_id,
            binding_sha256=bound.binding_receipt.binding_sha256,
            attempt_count=claimed.attempt_count,
            outcome=outcome,
            reason_code=reason_code,
            terminal=terminal,
            content_sha256=evidence.get("content_sha256"),
            raw_receipt_sha256=evidence.get("receipt_sha256"),
            capture_sha256=evidence.get("capture_sha256"),
            capacity_commit_sha256=evidence.get("capacity_commit_sha256"),
        )
        if terminal:
            self._remember_terminal_observation(
                bound.acquisition_job.workload_id,
                receipt,
            )
        return receipt

    def _complete(
        self,
        claimed: scheduler_module.ClaimedJob,
        *,
        now: float,
    ) -> scheduler_module.TransitionResult:
        return self._scheduler.complete(
            claimed.job_id,
            claimed.lease_token,
            now=self._transition_time(now),
        )

    def _retry_or_expire(
        self,
        bound: bridge.BoundAcquisitionClaim,
        claimed: scheduler_module.ClaimedJob,
        reason_code: str,
        *,
        now: float,
    ) -> ShadowTickResult:
        """Publish the attempt receipt before retry can become terminal."""

        effective_now = self._transition_time(now)
        expired = effective_now >= claimed.deadline_at
        stored_reason = "deadline_expired" if expired else reason_code
        receipt = self._record(
            bound,
            claimed,
            outcome="blocked_permanent" if expired else "retryable_failure",
            reason_code=stored_reason,
            terminal=expired,
        )
        transition = self._scheduler.retry(
            claimed.job_id,
            claimed.lease_token,
            reason_code,
            now=effective_now,
        )
        if expired and transition.status != "expired":
            raise AcquisitionRuntimeError("deadline receipt did not precede expiry")
        if not expired and transition.status == "expired":
            raise AcquisitionRuntimeError("scheduler expired without a terminal receipt")
        return ShadowTickResult(
            "blocked" if expired else "retry_scheduled",
            stored_reason,
            transition.status,
            receipt.receipt_sha256,
            claimed.attempt_count,
            bound.acquisition_job.workload_id,
            expired,
            acquisition_job_id=bound.acquisition_job.job_id,
            desired_state_sha256=bound.acquisition_job.snapshot_sha256,
        )

    @staticmethod
    def _deadline_binding_sha256(
        record: scheduler_module.OutboxRecord,
        job: acquisition.AcquisitionJob,
    ) -> str:
        return hashlib.sha256(_canonical({
            "acquisition_job_id": job.job_id,
            "acquisition_job_sha256": job.job_sha256,
            "attempt_count": record.attempt_count,
            "autoscheduler_job_id": record.job_id,
            "contract": DEADLINE_BINDING_CONTRACT,
            "deadline_at": record.deadline_at,
            "plan_sha256": job.plan_sha256,
            "scheduled_for": record.scheduled_for,
        })).hexdigest()

    @staticmethod
    def _missed_deadline_binding_sha256(
        scheduler_job_id: str,
        job: acquisition.AcquisitionJob,
    ) -> str:
        return hashlib.sha256(_canonical({
            "acquisition_job_id": job.job_id,
            "acquisition_job_sha256": job.job_sha256,
            "attempt_count": 0,
            "autoscheduler_job_id": scheduler_job_id,
            "contract": DEADLINE_BINDING_CONTRACT,
            "deadline_at": job.deadline_at,
            "plan_sha256": job.plan_sha256,
            "scheduled_for": job.scheduled_for,
        })).hexdigest()

    def _due_deadline_records(
        self,
        *,
        plan: acquisition.AcquisitionPlan,
        now: float,
    ) -> tuple[scheduler_module.OutboxRecord, ...]:
        """Read a bounded, payload-free preview before scheduler expiry."""

        path = Path(self._scheduler.path)
        if not path.exists():
            return ()
        try:
            with closing(sqlite3.connect(path, timeout=2.0)) as database:
                database.execute("PRAGMA query_only=ON")
                rows = database.execute(
                    "SELECT job_id,intent_id FROM scheduler_outbox "
                    "WHERE desired_state_sha256=? "
                    "AND state IN ('pending','retry','leased') "
                    "AND deadline_at<=? ORDER BY deadline_at,job_id LIMIT ?",
                    (
                        plan.snapshot_sha256,
                        now,
                        scheduler_module.MAX_OUTBOX_ROWS + 1,
                    ),
                ).fetchall()
        except sqlite3.DatabaseError as err:
            raise AcquisitionRuntimeError(
                "scheduler deadline preview failed closed"
            ) from err
        if len(rows) > scheduler_module.MAX_OUTBOX_ROWS:
            raise AcquisitionRuntimeError("scheduler deadline preview exceeds bound")
        records: list[scheduler_module.OutboxRecord] = []
        allowed_intents = {
            intent.intent_id
            for intent in plan.intents
            if self._workload_is_allowed(intent.workload_id)
        }
        for job_id, intent_id in rows:
            if intent_id not in allowed_intents:
                continue
            record = self._scheduler.get_record(str(job_id))
            if (
                record is None
                or record.desired_state_sha256 != plan.snapshot_sha256
                or record.state not in {"pending", "retry", "leased"}
                or record.deadline_at > now
            ):
                raise AcquisitionRuntimeError("scheduler deadline preview drifted")
            records.append(record)
        return tuple(records)

    def _account_due_deadlines(
        self,
        *,
        plan: acquisition.AcquisitionPlan,
        now: float,
    ) -> int:
        """Durably account due slots before scheduler makes them terminal."""

        accounted = 0
        intents = {item.intent_id: item for item in plan.intents}
        # The scheduler deliberately does not insert a current slot whose
        # deadline is already gone. Publish that zero-attempt receipt first so
        # `missed_deadline_count` can never be an unaccounted silent drop.
        current_jobs = acquisition.build_worker_acquisition_slot_jobs(
            plan,
            as_of=now,
        )
        for job in current_jobs:
            if not self._workload_is_allowed(job.workload_id):
                continue
            intent = intents[job.intent_id]
            slot_index = job.slot_index
            if job.deadline_at > now:
                continue
            scheduler_job_id = bridge.expected_autoscheduler_job_id(
                snapshot_sha256=plan.snapshot_sha256,
                intent_id=intent.intent_id,
                cadence_seconds=intent.cadence_seconds,
                slot_index=slot_index,
            )
            if self._scheduler.get_record(scheduler_job_id) is not None:
                continue
            if self._execution_store.load_terminal(job.job_id) is not None:
                continue
            receipt = self._execution_store.record(
                scheduler_job_id=scheduler_job_id,
                acquisition_job_id=job.job_id,
                binding_sha256=self._missed_deadline_binding_sha256(
                    scheduler_job_id, job
                ),
                attempt_count=0,
                outcome="blocked_permanent",
                reason_code="deadline_expired",
                terminal=True,
            )
            self._remember_terminal_observation(job.workload_id, receipt)
            accounted += 1
        jobs_by_scheduled_for: dict[float, dict[str, acquisition.AcquisitionJob]] = {}
        for record in self._due_deadline_records(plan=plan, now=now):
            try:
                intent = intents[record.intent_id]
            except KeyError as err:
                raise AcquisitionRuntimeError(
                    "deadline row is absent from the active acquisition plan"
                ) from err
            slot_jobs = jobs_by_scheduled_for.get(record.scheduled_for)
            if slot_jobs is None:
                slot_jobs = {
                    item.intent_id: item
                    for item in acquisition.build_worker_acquisition_slot_jobs(
                        plan,
                        as_of=record.scheduled_for,
                    )
                }
                jobs_by_scheduled_for[record.scheduled_for] = slot_jobs
            try:
                job = slot_jobs[record.intent_id]
            except KeyError as err:
                raise AcquisitionRuntimeError(
                    "deadline slot is absent from the active acquisition plan"
                ) from err
            if not self._workload_is_allowed(job.workload_id):
                continue
            expected_scheduler_id = bridge.expected_autoscheduler_job_id(
                snapshot_sha256=plan.snapshot_sha256,
                intent_id=record.intent_id,
                cadence_seconds=intent.cadence_seconds,
                slot_index=record.slot_index,
            )
            if (
                expected_scheduler_id != record.job_id
                or job.deadline_at != record.deadline_at
                or job.slot_index != record.slot_index
            ):
                raise AcquisitionRuntimeError("deadline scheduler/job binding drifted")
            if self._execution_store.load_terminal(job.job_id) is not None:
                continue
            receipt = self._execution_store.record(
                scheduler_job_id=record.job_id,
                acquisition_job_id=job.job_id,
                binding_sha256=self._deadline_binding_sha256(record, job),
                attempt_count=record.attempt_count,
                outcome="blocked_permanent",
                reason_code="deadline_expired",
                terminal=True,
            )
            self._remember_terminal_observation(job.workload_id, receipt)
            accounted += 1
        return accounted

    @staticmethod
    def _validate_child_result(
        result: DecoderProcessResult,
        job: acquisition.AcquisitionJob,
        decision: ActivationDecision,
    ) -> dict[str, Any]:
        if not result.succeeded or not isinstance(result.result, dict):
            raise AcquisitionRuntimeError("isolated handler did not succeed")
        value = result.result
        expected = {
            "actual_bytes", "actual_entries", "capture_sha256", "content_sha256",
            "contract", "evidence_sha256", "handler_id",
            "operational_admission_eligible", "receipt_sha256", "roles",
            "status", "workload_id",
        }
        if set(value) != expected or value["contract"] != CHILD_RESULT_CONTRACT:
            raise AcquisitionRuntimeError("isolated handler result shape drifted")
        try:
            recovery_binding = RECOVERY_BINDINGS[str(decision.rule.handler_id)]
            materialized_roles = tuple(
                member.role
                for member in recovery_binding.members_for_roles(value["roles"])
            )
        except (KeyError, CaptureRecoveryIntegrityError):
            raise AcquisitionRuntimeError(
                "isolated handler evidence role set drifted"
            ) from None
        if (
            value["workload_id"] != job.workload_id
            or value["handler_id"] != decision.rule.handler_id
            or value["status"] != "captured_shadow_raw"
            or value["operational_admission_eligible"] is not False
            or value["roles"] != list(materialized_roles)
            or isinstance(value["actual_bytes"], bool)
            or not isinstance(value["actual_bytes"], int)
            or not 1 <= value["actual_bytes"] <= job.budget.max_transport_bytes
            or value["actual_bytes"] > int(
                decision.rule.maximum_transport_bytes or 0
            )
            or value["actual_entries"] != len(materialized_roles)
            or value["actual_entries"] > job.budget.max_artifacts
        ):
            raise AcquisitionRuntimeError("isolated handler evidence violates budget")
        for name in ("capture_sha256", "content_sha256", "receipt_sha256"):
            if not isinstance(value[name], str) or _SHA256.fullmatch(value[name]) is None:
                raise AcquisitionRuntimeError("isolated handler evidence hash is invalid")
        evidence = dict(value)
        digest = evidence.pop("evidence_sha256")
        evidence.pop("contract")
        if digest != hashlib.sha256(_canonical(evidence)).hexdigest():
            raise AcquisitionRuntimeError("isolated handler evidence checksum drifted")
        return value

    @staticmethod
    def _child_matches_recovery(
        child_evidence: Mapping[str, Any],
        recovered: RecoveredCaptureEvidence,
    ) -> bool:
        return (
            child_evidence["actual_bytes"] == recovered.actual_bytes
            and child_evidence["actual_entries"] == recovered.actual_entries
            and child_evidence["content_sha256"] == recovered.content_sha256
            and child_evidence["receipt_sha256"] == recovered.receipt_sha256
            and child_evidence["capture_sha256"] == recovered.capture_sha256
        )

    @staticmethod
    def _terminal_matches_recovery(
        terminal: Any,
        recovered: RecoveredCaptureEvidence,
    ) -> bool:
        return (
            terminal.content_sha256 == recovered.content_sha256
            and terminal.raw_receipt_sha256 == recovered.receipt_sha256
            and terminal.capture_sha256 == recovered.capture_sha256
            and terminal.capacity_commit_sha256
            == recovered.capacity_commit_sha256
        )

    def _terminal_block(
        self,
        bound: bridge.BoundAcquisitionClaim,
        claimed: scheduler_module.ClaimedJob,
        *,
        reason_code: str,
        now: float,
    ) -> ShadowTickResult:
        receipt = self._record(
            bound,
            claimed,
            outcome="blocked_permanent",
            reason_code=reason_code,
            terminal=True,
        )
        transition = self._complete(claimed, now=now)
        return ShadowTickResult(
            "blocked",
            reason_code,
            transition.status,
            receipt.receipt_sha256,
            claimed.attempt_count,
            bound.acquisition_job.workload_id,
            True,
            acquisition_job_id=bound.acquisition_job.job_id,
            desired_state_sha256=bound.acquisition_job.snapshot_sha256,
        )

    def _finish_capture(
        self,
        bound: bridge.BoundAcquisitionClaim,
        claimed: scheduler_module.ClaimedJob,
        *,
        evidence: RecoveredCaptureEvidence,
        recovered_without_child_result: bool,
        now: float,
    ) -> ShadowTickResult:
        reason = (
            "shadow_raw_capture_recovered"
            if recovered_without_child_result
            else "shadow_raw_capture_complete"
        )
        receipt = self._record(
            bound,
            claimed,
            outcome="completed_shadow_raw",
            reason_code=reason,
            terminal=True,
            evidence={
                "capacity_commit_sha256": evidence.capacity_commit_sha256,
                "capture_sha256": evidence.capture_sha256,
                "content_sha256": evidence.content_sha256,
                "receipt_sha256": evidence.receipt_sha256,
            },
        )
        transition = self._complete(claimed, now=now)
        return ShadowTickResult(
            "completed_shadow_raw",
            reason,
            transition.status,
            receipt.receipt_sha256,
            claimed.attempt_count,
            bound.acquisition_job.workload_id,
            True,
            acquisition_job_id=bound.acquisition_job.job_id,
            desired_state_sha256=bound.acquisition_job.snapshot_sha256,
            source_capture_id=evidence.capture_id,
        )

    def _reconcile_capture(
        self,
        *,
        job: acquisition.AcquisitionJob,
        reservation_id: str,
    ) -> RecoveredCaptureEvidence | None:
        try:
            rule = ACTIVATION_RULES[job.workload_id]
        except KeyError as err:  # pragma: no cover - activation binding precedes this
            raise CaptureRecoveryIntegrityError(
                "job is absent from the activation matrix"
            ) from err
        return self._capture_reconciler.reconcile_job(
            job=job,
            rule=rule,
            reservation_id=reservation_id,
        )

    def _execute(
        self,
        bound: bridge.BoundAcquisitionClaim,
        claimed: scheduler_module.ClaimedJob,
        *,
        now: float,
        desired_state: DesiredState | None = None,
        plan: acquisition.AcquisitionPlan | None = None,
    ) -> ShadowTickResult:
        job = bound.acquisition_job
        if not self._workload_is_allowed(job.workload_id):
            receipt = self._record(
                bound,
                claimed,
                outcome="blocked_permanent",
                reason_code="workload_not_allowed",
                terminal=True,
            )
            transition = self._complete(claimed, now=now)
            return ShadowTickResult(
                "not_admitted",
                "workload_not_allowed",
                transition.status,
                receipt.receipt_sha256,
                claimed.attempt_count,
                job.workload_id,
                True,
                acquisition_job_id=job.job_id,
                desired_state_sha256=job.snapshot_sha256,
            )
        reservation_id = _reservation_id(job)
        # A terminal receipt is authoritative for the immutable acquisition
        # job identity even if worker options (for example secret presence)
        # changed after a crash but before scheduler completion.
        terminal = self._execution_store.load_terminal(job.job_id)
        if terminal is not None:
            if terminal.outcome == "completed_shadow_raw":
                try:
                    recovered = self._reconcile_capture(
                        job=job,
                        reservation_id=reservation_id,
                    )
                except (CaptureRecoveryBusyError, CaptureRecoveryCapacityError,
                        CaptureRecoveryIntegrityError) as err:
                    raise AcquisitionRuntimeError(
                        "terminal raw evidence no longer verifies"
                    ) from err
                if (
                    recovered is None
                    or not self._terminal_matches_recovery(terminal, recovered)
                ):
                    raise AcquisitionRuntimeError(
                        "terminal raw receipt differs from durable evidence"
                    )
            transition = self._complete(claimed, now=now)
            return ShadowTickResult(
                "completion_replayed",
                terminal.reason_code,
                transition.status,
                terminal.receipt_sha256,
                claimed.attempt_count,
                job.workload_id,
                True,
                acquisition_job_id=job.job_id,
                desired_state_sha256=job.snapshot_sha256,
                source_capture_id=(
                    None if terminal.outcome != "completed_shadow_raw"
                    else recovered.capture_id
                ),
            )
        decision = assess_acquisition_job(job, resolver=self._secret_resolver)
        # Recovery is credential-independent.  If a complete capture exists
        # after a parent/child crash, close its already-created reservation
        # before applying secret presence to any possible network refetch.
        if decision.rule.executable:
            try:
                existing_reservation = self._capacity_ledger.load(reservation_id)
                recovered = (
                    None
                    if existing_reservation is None
                    else self._reconcile_capture(
                        job=job,
                        reservation_id=reservation_id,
                    )
                )
            except CaptureRecoveryBusyError:
                return self._retry_or_expire(
                    bound, claimed, "capture_recovery_busy", now=now
                )
            except CaptureRecoveryCapacityError:
                return self._terminal_block(
                    bound,
                    claimed,
                    reason_code="capture_recovery_capacity_failure",
                    now=now,
                )
            except (CaptureRecoveryIntegrityError, RawCapacityLedgerConflict,
                    RawCapacityLedgerIntegrityError):
                return self._terminal_block(
                    bound,
                    claimed,
                    reason_code="capture_recovery_integrity_failure",
                    now=now,
                )
            if recovered is not None:
                return self._finish_capture(
                    bound,
                    claimed,
                    evidence=recovered,
                    recovered_without_child_result=True,
                    now=now,
                )
        if not decision.executable:
            transient = decision.reason_code == "secret_requirement_unavailable"
            if transient:
                return self._retry_or_expire(
                    bound, claimed, decision.reason_code, now=now
                )
            receipt = self._record(
                bound,
                claimed,
                outcome="blocked_permanent",
                reason_code=decision.reason_code,
                terminal=True,
            )
            transition = self._complete(claimed, now=now)
            return ShadowTickResult(
                "blocked",
                decision.reason_code,
                transition.status,
                receipt.receipt_sha256,
                claimed.attempt_count,
                job.workload_id,
                True,
                acquisition_job_id=job.job_id,
                desired_state_sha256=job.snapshot_sha256,
            )

        try:
            self._capacity_ledger.reserve(
                reservation_id=reservation_id,
                provider=job.provider_id,
                job_identity=job.job_id,
                requested_bytes=job.budget.max_transport_bytes,
                requested_entries=job.budget.max_artifacts,
            )
        except RawCapacityLedgerBusyError:
            reason = "capacity_ledger_busy"
            return self._retry_or_expire(bound, claimed, reason, now=now)
        except RawCapacityLedgerCapacityError:
            reason = "raw_capacity_exhausted"
            return self._terminal_block(
                bound, claimed, reason_code=reason, now=now
            )
        except (RawCapacityLedgerConflict, RawCapacityLedgerIntegrityError):
            reason = "capacity_ledger_integrity_failure"
            return self._terminal_block(
                bound, claimed, reason_code=reason, now=now
            )

        try:
            recovered = self._reconcile_capture(
                job=job,
                reservation_id=reservation_id,
            )
        except CaptureRecoveryBusyError:
            return self._retry_or_expire(
                bound, claimed, "capture_recovery_busy", now=now
            )
        except CaptureRecoveryCapacityError:
            return self._terminal_block(
                bound,
                claimed,
                reason_code="capture_recovery_capacity_failure",
                now=now,
            )
        except CaptureRecoveryIntegrityError:
            return self._terminal_block(
                bound,
                claimed,
                reason_code="capture_recovery_integrity_failure",
                now=now,
            )
        if recovered is not None:
            return self._finish_capture(
                bound,
                claimed,
                evidence=recovered,
                recovered_without_child_result=True,
                now=now,
            )

        rule_wall = decision.rule.maximum_wall_seconds
        if (
            isinstance(rule_wall, bool)
            or not isinstance(rule_wall, int)
            or rule_wall <= 0
            or job.budget.max_wall_seconds != rule_wall
            or rule_wall > MAX_EXECUTOR_WALL_SECONDS
        ):
            raise AcquisitionRuntimeError(
                "admitted handler wall budget is outside the executor boundary"
            )
        execution_started_at = self._transition_time(now)
        remaining = self._attempt_wall_seconds(
            job,
            rule_wall_seconds=float(rule_wall),
            deadline_remaining_seconds=max(
                0.0, claimed.deadline_at - execution_started_at
            ),
        )
        attempt_deadline_at = execution_started_at + remaining
        if remaining < 0.05:
            reason = "execution_deadline_exhausted"
            return self._retry_or_expire(bound, claimed, reason, now=now)
        from .acquisition_location_shadow import (
            GUARDED_RAW_WORKLOAD_IDS,
            LocationShadowBlockedError,
            build_runtime_guarded_shadow_child_request,
        )

        if job.workload_id in GUARDED_RAW_WORKLOAD_IDS:
            if desired_state is None or plan is None:
                raise AcquisitionRuntimeError(
                    "guarded raw capture is missing desired-state authority"
                )
            try:
                request = build_runtime_guarded_shadow_child_request(
                    desired_state,
                    plan,
                    job,
                    evaluated_at=now,
                    resolver=self._secret_resolver,
                    raw_store_root=str(self._raw_store_root),
                    raw_store_limits=self._raw_store_limits,
                )
            except LocationShadowBlockedError as error:
                # Missing/expired dynamic location authority is a truthful
                # source-level block.  It must produce a terminal receipt for
                # this slot, never tear down the all-provider worker process.
                return self._terminal_block(
                    bound,
                    claimed,
                    reason_code=error.reason_code,
                    now=now,
                )
        else:
            request = {
                "acquisition_job_id": job.job_id,
                "contract": CHILD_CONTRACT,
                "handler_id": decision.rule.handler_id,
                "handler_payload": decision.handler_payload,
                "raw_store_limits": _raw_limits_mapping(self._raw_store_limits),
                "raw_store_root": str(self._raw_store_root),
                "workload_id": job.workload_id,
            }
        if job.workload_id == "lightning.eumetsat.li":
            # One public deadline, bounded by both the scheduler and the
            # 50-second workload wall, spans parent OAuth minting and child
            # transport.  It is request metadata, never provider payload or
            # credential material; the child projects its remaining epoch
            # window onto the monotonic transport clock.
            request["execution_deadline_at"] = min(
                float(claimed.deadline_at),
                execution_started_at + remaining,
            )
        if len(_canonical(request)) > MAX_CHILD_REQUEST_BYTES:
            raise AcquisitionRuntimeError("isolated handler request exceeds bound")
        environment_overrides: dict[str, str] | None = None
        access_token = ""
        if job.workload_id == "ground.netatmo.region":
            broker = self._netatmo_access_token_broker
            if broker is None:
                return self._retry_or_expire(
                    bound,
                    claimed,
                    "netatmo_parent_broker_unavailable",
                    now=now,
                )
            try:
                access_token = broker(
                    dict(request["handler_payload"]),
                    float(claimed.deadline_at),
                )
            except TimeoutError:
                return self._retry_or_expire(
                    bound,
                    claimed,
                    "netatmo_oauth_deadline_unavailable",
                    now=now,
                )
            except Exception:
                # Provider failures can include sensitive response bodies.
                # Persist only a fixed code; never exception text or args.
                return self._retry_or_expire(
                    bound,
                    claimed,
                    "netatmo_oauth_broker_unavailable",
                    now=now,
                )
            if (
                type(access_token) is not str
                or not access_token
                or len(access_token) > 4096
                or not access_token.isascii()
                or any(
                    ord(character) <= 0x20 or ord(character) == 0x7F
                    for character in access_token
                )
            ):
                access_token = ""
                return self._retry_or_expire(
                    bound,
                    claimed,
                    "netatmo_oauth_broker_invalid",
                    now=now,
                )
            environment_overrides = {
                "STV3_NETATMO_ACCESS_TOKEN": access_token,
            }
        elif job.workload_id == "lightning.eumetsat.li":
            broker = self._eumetsat_access_token_broker
            if broker is None:
                return self._retry_or_expire(
                    bound,
                    claimed,
                    "eumetsat_parent_broker_unavailable",
                    now=now,
                )
            try:
                access_token = broker(request["execution_deadline_at"])
            except (TimeoutError, ParentCredentialTemporaryError):
                return self._retry_or_expire(
                    bound,
                    claimed,
                    "eumetsat_oauth_temporary_unavailable",
                    now=now,
                )
            except ParentCredentialPermanentError:
                return self._terminal_block(
                    bound,
                    claimed,
                    reason_code="eumetsat_oauth_contract_rejected",
                    now=now,
                )
            except Exception:
                return self._terminal_block(
                    bound,
                    claimed,
                    reason_code="eumetsat_oauth_parent_failure",
                    now=now,
                )
            if (
                type(access_token) is not str
                or not 16 <= len(access_token) <= 8192
                or not access_token.isascii()
                or any(
                    ord(character) <= 0x20 or ord(character) == 0x7F
                    for character in access_token
                )
            ):
                access_token = ""
                return self._terminal_block(
                    bound,
                    claimed,
                    reason_code="eumetsat_oauth_broker_invalid",
                    now=now,
                )
            environment_overrides = {
                "STV3_EUMETSAT_EPHEMERAL_ACCESS_TOKEN": access_token,
            }
        # OAuth minting consumes the same bounded absolute deadline as the
        # child.  Recompute the child wall after the parent broker returns.
        active_deadline_at = min(
            attempt_deadline_at,
            float(request.get("execution_deadline_at", claimed.deadline_at)),
        )
        remaining = self._attempt_wall_seconds(
            job,
            rule_wall_seconds=float(rule_wall),
            deadline_remaining_seconds=max(
                0.0,
                active_deadline_at - self._transition_time(now),
            ),
        )
        if remaining < 0.05:
            if environment_overrides is not None:
                environment_overrides.clear()
            access_token = ""
            return self._retry_or_expire(
                bound,
                claimed,
                "execution_deadline_exhausted_after_parent_broker",
                now=now,
            )
        execute_options: dict[str, Any] = {
            "target": str(decision.rule.process_target),
            "request": request,
            "effect_contract": _RAW_CAPTURE_EFFECT,
            "cancel_event": self._stop_event,
        }
        if environment_overrides is not None:
            execute_options["environment_overrides"] = environment_overrides
        try:
            executor = self._executor_factory(remaining)
            process = executor.execute(**execute_options)
        finally:
            # Drop the sole per-attempt parent reference immediately.  The
            # hard executor never stores this mapping or adds it to IPC.
            if environment_overrides is not None:
                environment_overrides.clear()
            access_token = ""
        if not process.succeeded:
            # A hard kill can race after RawArtifactStore committed its exact
            # capture but before the child returned technical evidence.
            try:
                recovered = self._reconcile_capture(
                    job=job,
                    reservation_id=reservation_id,
                )
            except CaptureRecoveryBusyError:
                return self._retry_or_expire(
                    bound, claimed, "capture_recovery_busy", now=now
                )
            except CaptureRecoveryCapacityError:
                return self._terminal_block(
                    bound,
                    claimed,
                    reason_code="capture_recovery_capacity_failure",
                    now=now,
                )
            except CaptureRecoveryIntegrityError:
                return self._terminal_block(
                    bound,
                    claimed,
                    reason_code="capture_recovery_integrity_failure",
                    now=now,
                )
            if recovered is not None:
                return self._finish_capture(
                    bound,
                    claimed,
                    evidence=recovered,
                    recovered_without_child_result=True,
                    now=now,
                )
            reason = f"process_{process.outcome}"
            return self._retry_or_expire(bound, claimed, reason, now=now)
        try:
            child_evidence = self._validate_child_result(process, job, decision)
            recovered = self._reconcile_capture(
                job=job,
                reservation_id=reservation_id,
            )
            if (
                recovered is None
                or not self._child_matches_recovery(child_evidence, recovered)
            ):
                raise AcquisitionRuntimeError(
                    "child evidence differs from exact durable raw capture"
                )
        except CaptureRecoveryBusyError:
            return self._retry_or_expire(
                bound, claimed, "capture_recovery_busy", now=now
            )
        except (AcquisitionRuntimeError, CaptureRecoveryCapacityError,
                CaptureRecoveryIntegrityError, RawCapacityLedgerConflict,
                RawCapacityLedgerCapacityError, RawCapacityLedgerIntegrityError):
            reason = "handler_evidence_commit_failed"
            return self._terminal_block(
                bound, claimed, reason_code=reason, now=now
            )
        return self._finish_capture(
            bound,
            claimed,
            evidence=recovered,
            recovered_without_child_result=False,
            now=now,
        )

    def tick(self, *, now: float | None = None) -> ShadowTickResult:
        """Reconcile and process at most one serial scheduler lease."""

        if self._stop_event.is_set():
            return ShadowTickResult(
                "stopped", "runtime_stop_requested", None, None, None, None, False
            )
        if not self._lock.acquire(blocking=False):
            return ShadowTickResult(
                "busy", "runtime_tick_in_progress", None, None, None, None, False
            )
        try:
            if self._stop_event.is_set():
                return ShadowTickResult(
                    "stopped", "runtime_stop_requested", None, None, None, None, False
                )
            moment = float(self._clock() if now is None else now)
            if not math.isfinite(moment) or moment < 0:
                raise AcquisitionRuntimeError("tick time is invalid")
            state_before_reconcile = self._desired_state_store.load_active()
            if state_before_reconcile is not None:
                plan_before_reconcile = acquisition.build_worker_acquisition_plan(
                    state_before_reconcile,
                    collector_scope=self._collector_scope,
                )
                self._account_due_deadlines(
                    plan=plan_before_reconcile,
                    now=moment,
                )
            try:
                reconciled = self._scheduler.reconcile(
                    now=moment,
                    allowed_workload_ids=self._allowed_workload_ids,
                )
            except scheduler_module.SchedulerDesiredStateError as error:
                # A stale/future head or a concurrent control-plane transition
                # must block all provider work, but it must not kill the worker
                # process that Home Assistant needs to install the repaired
                # desired state.  Store/schema inconsistencies remain fatal.
                if (
                    error.reason_code
                    not in _RECOVERABLE_SCHEDULER_DESIRED_STATE_REASONS
                ):
                    raise
                result = ShadowTickResult(
                    "idle",
                    error.reason_code,
                    None,
                    None,
                    None,
                    None,
                    False,
                )
                self._last_tick = result
                return result
            if reconciled.status == "idle_unconfigured":
                result = ShadowTickResult(
                    "idle", "desired_state_unconfigured", None, None, None, None, False
                )
            else:
                # Reconcile against static ownership so lease recovery and
                # deadline accounting remain complete. A missing static
                # requirement may probe once (and on restart, so exact durable
                # capture recovery remains possible), but is then suppressed
                # until the requirement becomes ready. Fairness prevents that
                # one probe from monopolising the serial provider lane.
                requirements_ready = self._requirements_ready_workload_ids()
                claimable = requirements_ready.union(
                    self._scheduler_workload_ids().difference(
                        self._missing_requirement_suppressions
                    )
                )
                primary_claimable = frozenset(claimable).intersection(
                    self._primary_quantitative_workload_ids
                )
                claimed = None
                primary_was_last = (
                    self._last_tick.workload_id
                    in self._primary_quantitative_workload_ids
                )
                if primary_claimable and not primary_was_last:
                    claimed = self._scheduler.claim_next(
                        self._lease_owner,
                        now=moment,
                        allowed_workload_ids=primary_claimable,
                        prefer_unattempted=False,
                    )
                if claimed is None:
                    claimed = self._scheduler.claim_next(
                        self._lease_owner,
                        now=moment,
                        allowed_workload_ids=frozenset(claimable),
                        prefer_unattempted=True,
                    )
                if claimed is None:
                    result = ShadowTickResult(
                        "idle", "no_ready_serial_job", None, None, None, None, False
                    )
                else:
                    state = self._desired_state_store.load_active()
                    if state is None:
                        raise AcquisitionRuntimeError("claimed job has no desired state")
                    plan = acquisition.build_worker_acquisition_plan(
                        state,
                        collector_scope=self._collector_scope,
                    )
                    bound = bridge.bind_claimed_acquisition_job(plan, claimed)
                    result = self._execute(
                        bound,
                        claimed,
                        now=moment,
                        desired_state=state,
                        plan=plan,
                    )
                    if (
                        result.reason_code
                        in _STATIC_REQUIREMENT_UNAVAILABLE_REASONS
                        and result.workload_id is not None
                    ):
                        self._missing_requirement_suppressions.add(
                            result.workload_id
                        )
                    elif result.workload_id is not None:
                        self._missing_requirement_suppressions.discard(
                            result.workload_id
                        )
            self._last_tick = result
            return result
        finally:
            self._lock.release()

    def diagnostics(self) -> dict[str, Any]:
        """Return only fixed aggregates and hashes; no targets or secrets."""

        matrix = activation_matrix_snapshot()
        capacity = self._capacity_ledger.stats()
        configured_workloads = []
        unavailable_workload_ids = []
        for workload_id in sorted(self._selection.workload_ids):
            rule = self._selection.activation_rules[workload_id]
            requirement_ids = (
                tuple(rule.secret_requirement_ids)
                + tuple(rule.authorization_requirement_ids)
            )
            available, total = self._available_requirement_count(
                requirement_ids
            )
            requirements_ready = available == total
            if not requirements_ready:
                unavailable_workload_ids.append(workload_id)
            observation = self._latest_terminal_observations.get(workload_id)
            last_terminal = None if observation is None else dict(observation[1])
            configured_workloads.append({
                "handler_configured": bool(
                    rule.executable
                    and rule.handler_id
                    and rule.process_target
                ),
                "last_terminal": last_terminal,
                "requirements_available": requirements_ready,
                "requirements_available_count": available,
                "requirements_total": total,
                "workload_id": workload_id,
            })
        blocked_rows = [
            row for row in matrix["rules"] if not row["executable"]
        ]
        blocked_reason_counts: dict[str, int] = {}
        for row in blocked_rows:
            reason = str(row["reason_code"])
            blocked_reason_counts[reason] = (
                blocked_reason_counts.get(reason, 0) + 1
            )
        result = {
            "activation": {
                "active_shadow_raw": matrix["active_shadow_raw"],
                "blocked_catalog": {
                    "count": len(blocked_rows),
                    "reason_counts": {
                        key: blocked_reason_counts[key]
                        for key in sorted(blocked_reason_counts)
                    },
                    "workload_ids": [
                        str(row["workload_id"]) for row in blocked_rows
                    ],
                },
                "configured_workloads": configured_workloads,
                "matrix_sha256": matrix["matrix_sha256"],
                "requirements_unavailable_count": len(
                    unavailable_workload_ids
                ),
                "requirements_unavailable_workload_ids": (
                    unavailable_workload_ids
                ),
                "selected_workload_ids": list(
                    sorted(self._selection.workload_ids)
                ),
                "selection_explicit": self._selection.explicit,
                "workload_count": matrix["workload_count"],
            },
            "capacity": {
                "committed_bytes": capacity["committed_bytes"],
                "committed_count": capacity["committed_count"],
                "committed_entries": capacity["committed_entries"],
                "global_max_bytes": capacity["global_max_bytes"],
                "global_max_entries": capacity["global_max_entries"],
                "reserved_bytes": capacity["reserved_bytes"],
                "reserved_count": capacity["reserved_count"],
                "reserved_entries": capacity["reserved_entries"],
            },
            "capture_recovery": self._capture_reconciler.diagnostics(),
            "contract": CONTRACT,
            "collector_scope": self._collector_scope,
            "adaptive_attempts": {
                "failure_threshold": ADAPTIVE_ATTEMPT_FAILURE_THRESHOLD,
                "full_probe_interval": ADAPTIVE_FULL_PROBE_INTERVAL,
                "short_wall_seconds": ADAPTIVE_ATTEMPT_WALL_SECONDS,
                "throttled_workload_count": len([
                    count
                    for count in self._consecutive_terminal_failures.values()
                    if count >= ADAPTIVE_ATTEMPT_FAILURE_THRESHOLD
                ]),
                "throttled_workload_ids": sorted(
                    workload_id
                    for workload_id, count in (
                        self._consecutive_terminal_failures.items()
                    )
                    if count >= ADAPTIVE_ATTEMPT_FAILURE_THRESHOLD
                ),
            },
            "execution_receipts": self._execution_store.stats(),
            "last_tick": self._last_tick.as_dict(),
            "operational_admission_eligible": False,
            "stop_requested": self._stop_event.is_set(),
            "scheduler": self._scheduler.diagnostics(),
            "startup_recovery": self._startup_recovery.as_dict(),
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise AcquisitionRuntimeError("runtime diagnostics exceed bound")
        return result

    def latest_terminal_results(
        self,
    ) -> tuple[tuple[str, str, str], ...]:
        """Return one bounded, secret-free terminal result per workload."""

        return tuple(
            (
                workload_id,
                observation[1]["status"],
                observation[1]["reason_code"],
            )
            for workload_id, observation in sorted(
                self._latest_terminal_observations.items()
            )
        )

    def request_stop(self) -> None:
        """Close acquisition admission and cancel an active isolated child."""

        self._stop_event.set()

    def retained_successful_captures(
        self,
    ) -> tuple[tuple[str, RawArtifactCaptureReceipt], ...]:
        """Recover the newest still-retained verified capture per workload.

        Execution and scheduler evidence provide the exact workload/fetch
        binding.  The raw store then re-verifies the immutable capture before
        it can be reused after a worker process restart.
        """

        latest_terminal: dict[str, ExecutionReceipt] = {}
        bindings_by_workload = {
            binding.workload_id: binding
            for binding in self._selection.recovery_bindings.values()
        }
        terminals = self._execution_store.terminal_receipts()
        workloads_by_job = self._scheduler.workload_ids_for_jobs(tuple(
            terminal.scheduler_job_id for terminal in terminals
        ))
        for terminal in terminals:
            if terminal.outcome != "completed_shadow_raw":
                continue
            workload_id = workloads_by_job.get(terminal.scheduler_job_id)
            if workload_id is None or workload_id not in self._selection.workload_ids:
                continue
            previous = latest_terminal.get(workload_id)
            if previous is None or terminal.created_at > previous.created_at:
                latest_terminal[workload_id] = terminal

        latest: dict[str, RawArtifactCaptureReceipt] = {}
        for workload_id, terminal in sorted(latest_terminal.items()):
            binding = bindings_by_workload.get(workload_id)
            if binding is None:
                continue
            capture = self._raw_store.load_capture_by_fetch(
                provider=binding.raw_provider,
                product=binding.capture_product,
                fetch_id=(
                    terminal.acquisition_job_id + binding.capture_suffix
                ),
            )
            if capture is None:
                # Retention may already have reclaimed old evidence.
                continue
            if capture.capture_receipt_sha256 != terminal.capture_sha256:
                raise AcquisitionRuntimeError(
                    "retained capture differs from terminal execution evidence"
                )
            latest[workload_id] = capture
        return tuple(
            (workload_id, latest[workload_id])
            for workload_id in sorted(latest)
        )

    @property
    def desired_state_store(self) -> DesiredStateStore:
        return self._desired_state_store

    @property
    def scheduler(self) -> scheduler_module.AutonomousScheduler:
        return self._scheduler

    @property
    def capacity_ledger(self) -> RawCapacityLedger:
        return self._capacity_ledger

    @property
    def execution_store(self) -> ExecutionReceiptStore:
        return self._execution_store

    @property
    def raw_store(self) -> RawArtifactStore:
        return self._raw_store


def create_shadow_candidate(
    paths: ShadowRuntimePaths,
    *,
    capacity_profile: RawCapacityProfile,
    raw_store_limits: RawArtifactLimits,
    secret_resolver: SecretResolver | None = None,
    executor_factory: ExecutorFactory = _default_executor_factory,
    netatmo_access_token_broker: NetatmoAccessTokenBroker | None = None,
    eumetsat_access_token_broker: EumetsatAccessTokenBroker | None = None,
    allowed_workload_ids: frozenset[str] | None = None,
    collector_scope: str = acquisition.COLLECTOR_SCOPE_COVERAGE_RELEVANT,
    clock: Callable[[], float] = time.time,
) -> AcquisitionShadowRuntime:
    """Construct the complete local candidate without starting any thread."""

    if not isinstance(paths, ShadowRuntimePaths):
        raise TypeError("paths must be ShadowRuntimePaths")
    validated_collector_scope = acquisition.validate_collector_scope(
        collector_scope
    )
    allowed_workloads = _validated_allowed_workload_ids(
        allowed_workload_ids
    )
    desired = DesiredStateStore(paths.desired_state)
    scheduler = scheduler_module.AutonomousScheduler(
        paths.scheduler,
        desired_state_store=desired,
        plan_builder=partial(
            bridge.acquisition_scheduler_plan_builder,
            collector_scope=validated_collector_scope,
        ),
        clock=clock,
    )
    capacity = RawCapacityLedger(
        paths.capacity_ledger,
        profile=capacity_profile,
        clock=clock,
    )
    receipts = ExecutionReceiptStore(paths.execution_receipts, clock=clock)
    return AcquisitionShadowRuntime(
        desired_state_store=desired,
        scheduler=scheduler,
        capacity_ledger=capacity,
        execution_store=receipts,
        raw_store_root=paths.raw_store_root,
        raw_store_limits=raw_store_limits,
        secret_resolver=secret_resolver,
        executor_factory=executor_factory,
        netatmo_access_token_broker=netatmo_access_token_broker,
        eumetsat_access_token_broker=eumetsat_access_token_broker,
        allowed_workload_ids=allowed_workloads,
        collector_scope=validated_collector_scope,
        clock=clock,
    )


__all__ = [
    "CONTRACT",
    "AcquisitionRuntimeError",
    "AcquisitionShadowRuntime",
    "CANARY_CHILD_ENVIRONMENT_ALLOWLIST",
    "EumetsatAccessTokenBroker",
    "ExecutorFactory",
    "IsolatedExecutor",
    "MAX_EXECUTOR_WALL_SECONDS",
    "NetatmoAccessTokenBroker",
    "ParentCredentialPermanentError",
    "ParentCredentialTemporaryError",
    "RAW_CAPTURE_CHILD_ENVIRONMENT_ALLOWLIST",
    "ShadowRuntimePaths",
    "ShadowTickResult",
    "create_shadow_candidate",
    "raw_capture_executor_factory",
]
