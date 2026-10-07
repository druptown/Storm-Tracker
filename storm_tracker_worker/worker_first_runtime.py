"""Single-owner worker-first orchestration candidate.

This module composes already durable authorities without importing the live
service, MQTT, Home Assistant or provider transports.  It consumes one
acquisition-completion stream through one bounded execution lane, rechecks the
mutable desired-state authority before and after processing, and advances the
completion cursor only after the consumer output (and any admitted snapshot)
is durable.

The coordinator is intentionally not instantiated by ``service.py``.  It is a
testable cutover boundary, not a go-live switch.
"""
from __future__ import annotations

import asyncio
from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Callable, Protocol

from .acquisition_evidence_lifecycle import (
    CompletionLease,
    ConsumptionReceipt,
    desired_authority_proof,
)
from .desired_state import DesiredState
from .worker_first_ownership import REQUIRED_STORE_ROLES


CONTRACT = "stv3-worker-first-runtime-v3"
MAX_COUNTER = 1_000_000_000
MAX_DIAGNOSTIC_BYTES = 8192
DEFAULT_LEASE_SECONDS = 600.0
DEFAULT_PROCESSING_TIMEOUT_SECONDS = 540.0
DEFAULT_CANCEL_GRACE_SECONDS = 10.0
DEFAULT_CONTROL_TIMEOUT_SECONDS = 30.0
DEFAULT_STOP_TIMEOUT_SECONDS = 2.0

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DISPOSITIONS = frozenset({"advanced", "withheld", "indeterminate"})
_SNAPSHOT_STATES = frozenset({
    "new", "starting", "unconfigured", "running", "degraded", "stopping",
    "stopped", "unknown",
})


class WorkerFirstRuntimeError(RuntimeError):
    """The composed transition cannot continue safely."""


class WorkerFirstRuntimeOwnershipError(WorkerFirstRuntimeError):
    """A second coordinator attempted to own the heavy worker lane."""


class DesiredStateReader(Protocol):
    def load_active(self) -> DesiredState | None: ...


class CompletionLifecycle(Protocol):
    def resume_pending_prunes(self) -> int: ...

    def audit_archive(self) -> Mapping[str, Any]: ...

    def diagnostics(self) -> Mapping[str, Any]: ...

    def claim_next(
        self,
        consumer_id: str,
        stream_id: str,
        owner_id: str,
        *,
        lease_seconds: float,
        now: float | None = None,
    ) -> CompletionLease | None: ...

    def acknowledge(
        self,
        event_id: str,
        lease_token: str,
        *,
        output_sha256: str,
        now: float | None = None,
    ) -> ConsumptionReceipt: ...

    def reject_leased(
        self,
        event_id: str,
        lease_token: str,
        *,
        expected_event_sha256: str,
        reason_code: str,
        active_desired_state_sha256: str | None,
        now: float | None = None,
    ) -> ConsumptionReceipt: ...


class FactRevisionLifecycle(Protocol):
    def audit(self) -> Mapping[str, Any]: ...

    def diagnostics(self) -> Mapping[str, Any]: ...

    def supersede_obsolete(self, active_desired_state: DesiredState) -> int: ...

    def supersede_all_ready(
        self,
        *,
        reason: str = "desired_state_unavailable",
    ) -> int: ...


class SnapshotLifecycle(Protocol):
    async def async_start(self) -> Mapping[str, Any]: ...

    async def async_tick(self) -> Mapping[str, Any]: ...

    async def async_publish_exact_fact(
        self,
        binding: FactPublicationBinding,
    ) -> FactPublicationReceipt: ...

    async def async_stop(self) -> Mapping[str, Any]: ...

    def diagnostics(self) -> Mapping[str, Any]: ...


class WorkerFirstOwnershipLeaseAuthority(Protocol):
    """Already-held cross-process authority transferred to the coordinator."""

    @property
    def authority_sha256(self) -> str: ...

    @property
    def authority_scope_sha256(self) -> str: ...

    def assert_held(self) -> None: ...

    def assert_coordinator_binding(
        self,
        *,
        consumer_id: str,
        stream_id: str,
        stores: Mapping[str, tuple[str | Path, str]],
    ) -> None: ...

    def release(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PipelineOutcome:
    """One durable consumer result; contains no payload or exception text."""

    output_sha256: str
    disposition: str
    fact_revision_staged: bool
    process_receipt_sha256: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.output_sha256, "output_sha256"),
            (self.process_receipt_sha256, "process_receipt_sha256"),
        ):
            if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                raise WorkerFirstRuntimeError(f"{label} is geen lowercase SHA-256")
        if self.disposition not in _DISPOSITIONS:
            raise WorkerFirstRuntimeError("pipeline disposition is ongeldig")
        if type(self.fact_revision_staged) is not bool:
            raise WorkerFirstRuntimeError("fact_revision_staged moet boolean zijn")
        if self.fact_revision_staged and self.disposition != "advanced":
            raise WorkerFirstRuntimeError(
                "alleen advanced output mag een fact revision klaarzetten"
            )


@dataclass(frozen=True, slots=True)
class FactPublicationBinding:
    """Exact durable fact identity expected to become the snapshot head."""

    pipeline_item_id: str
    revision_sha256: str
    snapshot_sha256: str
    process_receipt_sha256: str
    desired_state_sha256: str
    authority_epoch: int
    snapshot_revision: int

    def __post_init__(self) -> None:
        _identifier(self.pipeline_item_id, "fact pipeline_item_id")
        for value, label in (
            (self.revision_sha256, "fact revision_sha256"),
            (self.snapshot_sha256, "fact snapshot_sha256"),
            (self.process_receipt_sha256, "fact process_receipt_sha256"),
            (self.desired_state_sha256, "fact desired_state_sha256"),
        ):
            if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                raise WorkerFirstRuntimeError(f"{label} is geen lowercase SHA-256")
        for value, label in (
            (self.authority_epoch, "fact authority_epoch"),
            (self.snapshot_revision, "fact snapshot_revision"),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 1 <= value <= 2**63 - 1
            ):
                raise WorkerFirstRuntimeError(f"{label} is ongeldig")


@dataclass(frozen=True, slots=True)
class FactPublicationReceipt:
    """Exact QoS-acknowledged snapshot identity for one fact binding."""

    pipeline_item_id: str
    revision_sha256: str
    snapshot_sha256: str
    process_receipt_sha256: str
    desired_state_sha256: str
    authority_epoch: int
    snapshot_revision: int
    publication_envelope_sha256: str
    transport_receipt_sha256: str
    committed_at: float

    def __post_init__(self) -> None:
        binding = FactPublicationBinding(
            pipeline_item_id=self.pipeline_item_id,
            revision_sha256=self.revision_sha256,
            snapshot_sha256=self.snapshot_sha256,
            process_receipt_sha256=self.process_receipt_sha256,
            desired_state_sha256=self.desired_state_sha256,
            authority_epoch=self.authority_epoch,
            snapshot_revision=self.snapshot_revision,
        )
        del binding
        for value, label in (
            (
                self.publication_envelope_sha256,
                "publication_envelope_sha256",
            ),
            (self.transport_receipt_sha256, "transport_receipt_sha256"),
        ):
            if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                raise WorkerFirstRuntimeError(f"{label} is geen lowercase SHA-256")
        _duration(self.committed_at, "fact committed_at", maximum=2**63 - 1)

    def matches(self, binding: FactPublicationBinding) -> bool:
        return (
            self.pipeline_item_id == binding.pipeline_item_id
            and self.revision_sha256 == binding.revision_sha256
            and self.snapshot_sha256 == binding.snapshot_sha256
            and self.process_receipt_sha256
            == binding.process_receipt_sha256
            and self.desired_state_sha256 == binding.desired_state_sha256
            and self.authority_epoch == binding.authority_epoch
            and self.snapshot_revision == binding.snapshot_revision
        )


@dataclass(frozen=True, slots=True)
class VerifiedPipelineOutcome:
    """Durable output-store proof, independently reloaded after processing."""

    event_id: str
    event_sha256: str
    producer_receipt_sha256: str
    desired_state_sha256: str
    authority_epoch: int
    authority_sha256: str
    consumer_id: str
    stream_id: str
    stream_sequence: int
    output_sha256: str
    output_receipt_sha256: str
    process_receipt_sha256: str
    disposition: str
    verifier_profile_sha256: str
    fact_binding: FactPublicationBinding | None = None

    def __post_init__(self) -> None:
        for value, label in (
            (self.event_id, "verified event_id"),
            (self.consumer_id, "verified consumer_id"),
            (self.stream_id, "verified stream_id"),
        ):
            _identifier(value, label)
        for value, label in (
            (self.event_sha256, "verified event_sha256"),
            (
                self.producer_receipt_sha256,
                "verified producer_receipt_sha256",
            ),
            (self.desired_state_sha256, "verified desired_state_sha256"),
            (self.authority_sha256, "verified authority_sha256"),
            (self.output_sha256, "verified output_sha256"),
            (self.output_receipt_sha256, "verified output_receipt_sha256"),
            (self.process_receipt_sha256, "verified process_receipt_sha256"),
            (self.verifier_profile_sha256, "verified verifier_profile_sha256"),
        ):
            if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                raise WorkerFirstRuntimeError(f"{label} is geen lowercase SHA-256")
        for value, label in (
            (self.authority_epoch, "verified authority_epoch"),
            (self.stream_sequence, "verified stream_sequence"),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 1 <= value <= 2**63 - 1
            ):
                raise WorkerFirstRuntimeError(f"{label} is ongeldig")
        if self.disposition not in _DISPOSITIONS:
            raise WorkerFirstRuntimeError("verified disposition is ongeldig")
        if self.fact_binding is not None and not isinstance(
            self.fact_binding,
            FactPublicationBinding,
        ):
            raise WorkerFirstRuntimeError("verified fact binding is ongeldig")


class PipelineOutcomeAuthority(Protocol):
    def verify(
        self,
        *,
        lease: CompletionLease,
        desired_state: DesiredState,
        outcome: PipelineOutcome,
    ) -> VerifiedPipelineOutcome: ...


PipelineProcessor = Callable[
    [CompletionLease, DesiredState, threading.Event],
    PipelineOutcome,
]


def _identifier(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or _IDENTIFIER_RE.fullmatch(value) is None
        or "://" in value
    ):
        raise ValueError(f"{label} is geen veilig begrensd ID")
    return value


def _duration(value: object, label: str, *, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} is ongeldig")
    result = float(value)
    if not math.isfinite(result) or not 0.001 <= result <= maximum:
        raise ValueError(f"{label} is ongeldig")
    return result


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, RecursionError) as error:
        raise WorkerFirstRuntimeError("runtimewaarde is niet canoniek") from error


def _snapshot_status_ok(value: Mapping[str, Any]) -> bool:
    return (
        isinstance(value, Mapping)
        and value.get("state") == "running"
        and value.get("last_error_code") is None
        and isinstance(value.get("publication"), Mapping)
        and value["publication"].get("transport_state") == "published"
        and value["publication"].get("dirty") is False
    )


def _snapshot_status_unconfigured_ok(value: Mapping[str, Any]) -> bool:
    """Accept only a quiescent, explicitly unconfigured snapshot authority."""

    return (
        isinstance(value, Mapping)
        and value.get("state") == "unconfigured"
        and value.get("last_error_code") is None
        and isinstance(value.get("publication"), Mapping)
        and value["publication"].get("transport_state") in {None, "published"}
        and value["publication"].get("dirty") is False
    )


def _snapshot_status_stopped_ok(value: Mapping[str, Any]) -> bool:
    return (
        isinstance(value, Mapping)
        and value.get("state") == "stopped"
        and value.get("last_error_code") is None
        and isinstance(value.get("publication"), Mapping)
        and value["publication"].get("transport_state") in {None, "published"}
        and value["publication"].get("dirty") is False
    )


def _validated_store_bindings(
    value: object,
) -> dict[str, tuple[str, str]]:
    """Validate the exact six-role injection without reading private stores."""

    if not isinstance(value, Mapping) or set(value) != set(REQUIRED_STORE_ROLES):
        raise WorkerFirstRuntimeOwnershipError(
            "ownership_store_binding_invalid"
        )
    result: dict[str, tuple[str, str]] = {}
    normalized_paths: set[str] = set()
    for role in sorted(REQUIRED_STORE_ROLES):
        supplied = value.get(role)
        if not isinstance(supplied, tuple) or len(supplied) != 2:
            raise WorkerFirstRuntimeOwnershipError(
                "ownership_store_binding_invalid"
            )
        raw_path, raw_identity = supplied
        if not isinstance(raw_path, (str, Path)):
            raise WorkerFirstRuntimeOwnershipError(
                "ownership_store_binding_invalid"
            )
        path = Path(raw_path)
        path_text = str(path)
        if (
            not path.is_absolute()
            or not path_text
            or len(path_text) > 4096
            or "\x00" in path_text
            or not isinstance(raw_identity, str)
            or _SHA256_RE.fullmatch(raw_identity) is None
        ):
            raise WorkerFirstRuntimeOwnershipError(
                "ownership_store_binding_invalid"
            )
        normalized = os.path.normcase(os.path.normpath(path_text))
        if normalized in normalized_paths:
            raise WorkerFirstRuntimeOwnershipError(
                "ownership_store_binding_invalid"
            )
        normalized_paths.add(normalized)
        result[role] = (path_text, raw_identity)
    return result


class WorkerFirstRuntimeCoordinator:
    """Exactly one serialized completion→output→snapshot lifecycle."""

    _owner_lock = threading.Lock()
    _active_owner: object | None = None

    def __init__(
        self,
        *,
        desired_state_store: DesiredStateReader,
        completion_lifecycle: CompletionLifecycle,
        fact_revision_lifecycle: FactRevisionLifecycle,
        snapshot_lifecycle: SnapshotLifecycle,
        processor: PipelineProcessor,
        outcome_authority: PipelineOutcomeAuthority,
        ownership_lease: WorkerFirstOwnershipLeaseAuthority,
        store_bindings: Mapping[str, tuple[str | Path, str]],
        consumer_id: str,
        stream_id: str,
        owner_id: str = "worker-first-runtime",
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        processing_timeout_seconds: float = DEFAULT_PROCESSING_TIMEOUT_SECONDS,
        cancel_grace_seconds: float = DEFAULT_CANCEL_GRACE_SECONDS,
        control_timeout_seconds: float = DEFAULT_CONTROL_TIMEOUT_SECONDS,
        stop_timeout_seconds: float = DEFAULT_STOP_TIMEOUT_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        for value, label in (
            (desired_state_store, "desired_state_store"),
            (completion_lifecycle, "completion_lifecycle"),
            (fact_revision_lifecycle, "fact_revision_lifecycle"),
            (snapshot_lifecycle, "snapshot_lifecycle"),
        ):
            if value is None:
                raise TypeError(f"{label} ontbreekt")
        if not callable(processor):
            raise TypeError("processor moet callable zijn")
        if outcome_authority is None or not callable(
            getattr(outcome_authority, "verify", None)
        ):
            raise TypeError("outcome_authority mist verify")
        if ownership_lease is None or any(
            not callable(getattr(ownership_lease, name, None))
            for name in ("assert_held", "assert_coordinator_binding", "release")
        ):
            raise TypeError("ownership_lease mist verplichte authoritymethoden")
        if not callable(clock):
            raise TypeError("clock moet callable zijn")
        self._desired_store = desired_state_store
        self._completion = completion_lifecycle
        self._facts = fact_revision_lifecycle
        self._snapshots = snapshot_lifecycle
        self._processor = processor
        self._outcome_authority = outcome_authority
        self._consumer_id = _identifier(consumer_id, "consumer_id")
        self._stream_id = _identifier(stream_id, "stream_id")
        self._store_bindings = _validated_store_bindings(store_bindings)
        try:
            ownership_lease.assert_coordinator_binding(
                consumer_id=self._consumer_id,
                stream_id=self._stream_id,
                stores=self._store_bindings,
            )
            ownership_lease.assert_held()
            authority_sha256 = ownership_lease.authority_sha256
            authority_scope_sha256 = ownership_lease.authority_scope_sha256
            if (
                not isinstance(authority_sha256, str)
                or _SHA256_RE.fullmatch(authority_sha256) is None
                or not isinstance(authority_scope_sha256, str)
                or _SHA256_RE.fullmatch(authority_scope_sha256) is None
            ):
                raise ValueError("ownership identity invalid")
        except BaseException:
            raise WorkerFirstRuntimeOwnershipError(
                "ownership_authority_invalid"
            ) from None
        self._ownership = ownership_lease
        self._ownership_authority_sha256 = authority_sha256
        self._ownership_scope_sha256 = authority_scope_sha256
        self._store_binding_sha256 = hashlib.sha256(_canonical([
            {
                "identity_sha256": identity,
                "path_sha256": hashlib.sha256(
                    path.encode("utf-8")
                ).hexdigest(),
                "role": role,
            }
            for role, (path, identity) in sorted(self._store_bindings.items())
        ])).hexdigest()
        self._owner_id = _identifier(owner_id, "owner_id")
        self._lease_seconds = _duration(
            lease_seconds,
            "lease_seconds",
            maximum=3600.0,
        )
        self._processing_timeout = _duration(
            processing_timeout_seconds,
            "processing_timeout_seconds",
            maximum=self._lease_seconds,
        )
        self._cancel_grace = _duration(
            cancel_grace_seconds,
            "cancel_grace_seconds",
            maximum=60.0,
        )
        if self._processing_timeout + self._cancel_grace > self._lease_seconds:
            raise ValueError("processing timeout en cancelgrace overschrijden lease")
        self._control_timeout = _duration(
            control_timeout_seconds,
            "control_timeout_seconds",
            maximum=300.0,
        )
        self._stop_timeout = _duration(
            stop_timeout_seconds,
            "stop_timeout_seconds",
            maximum=30.0,
        )
        self._clock = clock
        self._token = object()
        self._gate = asyncio.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="stv3-worker-first",
        )
        self._executor_shutdown = False
        self._mutable_lock = threading.RLock()
        self._processor_lock = self._mutable_lock
        self._active_mutable_futures: set[Future[Any]] = set()
        self._detached_mutable_futures: set[Future[Any]] = set()
        self._active_processor_future: Future[PipelineOutcome] | None = None
        self._active_snapshot_tasks: dict[asyncio.Future[Any], str] = {}
        self._detached_snapshot_tasks: set[asyncio.Future[Any]] = set()
        self._active_snapshot_operations = 0
        self._active_runtime_flows = 0
        self._snapshot_quiesced = True
        self._ownership_released = False
        self._authority_release_started = False
        self._cancel_event = threading.Event()
        self._started = False
        self._closed = False
        self._stop_requested = False
        self._owns_lane = False
        self._in_flight = False
        self._state = "new"
        self._last_error_code: str | None = None
        self._last_disposition: str | None = None
        self._counts = {
            "start_calls": 0,
            "ticks": 0,
            "idle_ticks": 0,
            "leases": 0,
            "processed": 0,
            "advanced": 0,
            "withheld": 0,
            "indeterminate": 0,
            "authority_withheld": 0,
            "fact_triggers": 0,
            "acknowledged": 0,
            "rejected": 0,
            "terminalized": 0,
            "failures": 0,
            "cancellations": 0,
            "control_timeouts": 0,
            "snapshot_timeouts": 0,
            "stop_timeouts": 0,
            "late_completions": 0,
            "stop_calls": 0,
        }

    def _increment(self, name: str) -> None:
        with self._mutable_lock:
            self._counts[name] = min(MAX_COUNTER, self._counts[name] + 1)

    def _assert_admission_open(self) -> None:
        self._assert_ownership()
        with self._mutable_lock:
            if self._closed or self._stop_requested:
                raise WorkerFirstRuntimeError("runtime_stopping")

    def _prepare_processor_admission(self) -> None:
        self._assert_ownership()
        with self._mutable_lock:
            if self._closed or self._stop_requested:
                raise WorkerFirstRuntimeError("runtime_stopping")
            self._cancel_event.clear()

    def _begin_runtime_flow(self) -> None:
        with self._mutable_lock:
            self._active_runtime_flows += 1
            self._in_flight = True

    def _end_runtime_flow(self) -> None:
        with self._mutable_lock:
            if self._active_runtime_flows <= 0:
                raise WorkerFirstRuntimeError("runtime_flow_underflow")
            self._active_runtime_flows -= 1
            self._in_flight = self._active_runtime_flows > 0
        self._release_authorities_if_quiescent()

    def _pending_async_work(self) -> tuple[bool, bool, bool]:
        with self._mutable_lock:
            processor = (
                self._active_processor_future is not None
                and not self._active_processor_future.done()
            )
            mutable = bool(self._active_mutable_futures)
            snapshot = bool(self._active_snapshot_tasks)
        return processor, mutable, snapshot

    def _assert_ownership(self) -> None:
        if self._ownership_released:
            raise WorkerFirstRuntimeOwnershipError("ownership_fence_not_held")
        try:
            self._ownership.assert_held()
        except BaseException:
            raise WorkerFirstRuntimeOwnershipError(
                "ownership_fence_not_held"
            ) from None

    def _mark_ownership_failure(self) -> None:
        self._last_error_code = "ownership_fence_not_held"
        self._state = "error"

    def _claim_owner(self) -> None:
        self._assert_ownership()
        with self._owner_lock:
            if WorkerFirstRuntimeCoordinator._active_owner not in {
                None,
                self._token,
            }:
                raise WorkerFirstRuntimeOwnershipError(
                    "een andere worker-first coordinator bezit de zware lane"
                )
            WorkerFirstRuntimeCoordinator._active_owner = self._token
            self._owns_lane = True
        self._assert_ownership()

    def _release_owner(self) -> None:
        with self._owner_lock:
            if WorkerFirstRuntimeCoordinator._active_owner is self._token:
                WorkerFirstRuntimeCoordinator._active_owner = None
            self._owns_lane = False

    def _release_authorities_if_quiescent(self) -> bool:
        """Release both owners only after all mutable authorities are quiet."""

        with self._mutable_lock:
            if (
                not self._closed
                or self._authority_release_started
                or self._active_mutable_futures
                or self._active_snapshot_operations
                or self._active_snapshot_tasks
                or self._active_runtime_flows
                or not self._snapshot_quiesced
                or self._in_flight
            ):
                return False
            self._authority_release_started = True
        released_cleanly = True
        try:
            self._assert_ownership()
            self._ownership.release()
        except BaseException:
            released_cleanly = False
            self._mark_ownership_failure()
        finally:
            with self._mutable_lock:
                self._ownership_released = True
            self._release_owner()
        if released_cleanly and self._state != "error":
            self._state = "stopped"
        return released_cleanly

    def _mutable_future_finished(
        self,
        future: Future[Any],
        *,
        processor: bool,
    ) -> None:
        """Post-boundary fence and quiescence reconciliation run on completion."""

        try:
            self._assert_ownership()
        except WorkerFirstRuntimeOwnershipError:
            self._mark_ownership_failure()
        with self._mutable_lock:
            detached = future in self._detached_mutable_futures
            self._active_mutable_futures.discard(future)
            self._detached_mutable_futures.discard(future)
            if processor and self._active_processor_future is future:
                self._active_processor_future = None
            if detached:
                self._increment("late_completions")
        self._release_authorities_if_quiescent()

    def _register_mutable_future(
        self,
        future: Future[Any],
        *,
        processor: bool,
    ) -> None:
        with self._mutable_lock:
            self._active_mutable_futures.add(future)
            if processor:
                self._active_processor_future = future
        future.add_done_callback(
            lambda completed: self._mutable_future_finished(
                completed,
                processor=processor,
            )
        )

    def _shutdown_executor(self, *, wait: bool) -> None:
        if self._executor_shutdown:
            return
        self._executor_shutdown = True
        self._executor.shutdown(wait=wait, cancel_futures=True)

    async def _load_desired(self) -> DesiredState | None:
        desired = await self._run_blocking(self._desired_store.load_active)
        if desired is not None and not isinstance(desired, DesiredState):
            raise WorkerFirstRuntimeError(
                "desired-state store gaf geen gevalideerde DesiredState"
            )
        return desired

    @staticmethod
    def _same_desired(
        active: DesiredState | None,
        expected: DesiredState,
    ) -> bool:
        return (
            active is not None
            and active.state_sha256 == expected.state_sha256
            and active.authority_epoch == expected.authority_epoch
            and active.generation == expected.generation
        )

    def _record_failure(self, code: str) -> None:
        self._increment("failures")
        self._last_error_code = code
        if not self._closed and not self._stop_requested:
            self._state = "degraded"

    @staticmethod
    def _consume_async_future(future: asyncio.Future[Any]) -> None:
        if future.cancelled():
            return
        try:
            future.exception()
        except (asyncio.CancelledError, Exception):
            pass

    async def _run_blocking(
        self,
        function: Callable[..., Any],
        *args: Any,
        timeout_seconds: float | None = None,
        reconcile_after_stop: bool = False,
    ) -> Any:
        self._assert_admission_open()
        timeout = (
            self._control_timeout
            if timeout_seconds is None
            else _duration(
                timeout_seconds,
                "control operation timeout",
                maximum=300.0,
            )
        )
        loop = asyncio.get_running_loop()
        work = self._executor.submit(function, *args)
        self._register_mutable_future(work, processor=False)
        future = asyncio.wrap_future(work, loop=loop)
        future.add_done_callback(self._consume_async_future)
        try:
            result = await asyncio.wait_for(
                asyncio.shield(future),
                timeout=timeout,
            )
        except TimeoutError as error:
            with self._mutable_lock:
                if not work.done():
                    self._detached_mutable_futures.add(work)
            self._increment("control_timeouts")
            raise WorkerFirstRuntimeError("control_operation_timeout") from error
        except asyncio.CancelledError:
            with self._mutable_lock:
                if not work.done():
                    self._detached_mutable_futures.add(work)
            raise
        finally:
            self._assert_ownership()
        if (
            not reconcile_after_stop
            and (self._closed or self._stop_requested)
        ):
            raise WorkerFirstRuntimeError("runtime_stopping")
        return result

    def _snapshot_task_finished(self, task: asyncio.Future[Any]) -> None:
        try:
            self._assert_ownership()
        except WorkerFirstRuntimeOwnershipError:
            self._mark_ownership_failure()
        result: object | None = None
        try:
            if not task.cancelled():
                result = task.result()
        except (asyncio.CancelledError, Exception):
            result = None
        with self._mutable_lock:
            operation_kind = self._active_snapshot_tasks.pop(task, None)
            detached = task in self._detached_snapshot_tasks
            self._detached_snapshot_tasks.discard(task)
            if (
                operation_kind == "stop"
                and isinstance(result, Mapping)
                and _snapshot_status_stopped_ok(result)
            ):
                self._snapshot_quiesced = True
            if detached:
                self._increment("late_completions")
        self._release_authorities_if_quiescent()

    async def _run_snapshot_boundary(
        self,
        function: Callable[..., Any],
        *args: Any,
        operation_kind: str,
        timeout_seconds: float | None = None,
        allow_during_stop: bool = False,
        reconcile_after_stop: bool = False,
    ) -> Mapping[str, Any] | FactPublicationReceipt:
        if operation_kind not in {"start", "tick", "publish", "stop"}:
            raise WorkerFirstRuntimeError("snapshot_operation_invalid")
        if allow_during_stop:
            self._assert_ownership()
        else:
            self._assert_admission_open()
        timeout = (
            self._control_timeout
            if timeout_seconds is None
            else _duration(
                timeout_seconds,
                "snapshot operation timeout",
                maximum=300.0,
            )
        )
        try:
            awaitable = function(*args)
        except Exception:
            self._assert_ownership()
            raise
        if not inspect.isawaitable(awaitable):
            raise WorkerFirstRuntimeError("snapshot_operation_not_awaitable")
        task = asyncio.ensure_future(awaitable)
        with self._mutable_lock:
            self._active_snapshot_tasks[task] = operation_kind
            self._active_snapshot_operations += 1
        task.add_done_callback(self._snapshot_task_finished)
        try:
            result = await asyncio.wait_for(
                asyncio.shield(task),
                timeout=timeout,
            )
        except TimeoutError as error:
            with self._mutable_lock:
                if not task.done():
                    self._detached_snapshot_tasks.add(task)
            self._increment("snapshot_timeouts")
            code = (
                "snapshot_stop_timeout"
                if operation_kind == "stop"
                else "snapshot_operation_timeout"
            )
            raise WorkerFirstRuntimeError(code) from error
        except asyncio.CancelledError:
            with self._mutable_lock:
                if not task.done():
                    self._detached_snapshot_tasks.add(task)
            raise
        finally:
            try:
                self._assert_ownership()
            finally:
                with self._mutable_lock:
                    self._active_snapshot_operations -= 1
                self._release_authorities_if_quiescent()
        if (
            not reconcile_after_stop
            and (self._closed or self._stop_requested)
        ):
            raise WorkerFirstRuntimeError("runtime_stopping")
        return result

    async def _start_snapshots(self) -> Mapping[str, Any]:
        with self._mutable_lock:
            self._snapshot_quiesced = False
        result = await self._run_snapshot_boundary(
            self._snapshots.async_start,
            operation_kind="start",
        )
        if not isinstance(result, Mapping):
            raise WorkerFirstRuntimeError("snapshot_runtime_start_failed")
        return result

    async def _stop_snapshots(
        self,
        *,
        timeout_seconds: float,
    ) -> Mapping[str, Any]:
        result = await self._run_snapshot_boundary(
            self._snapshots.async_stop,
            operation_kind="stop",
            timeout_seconds=timeout_seconds,
            allow_during_stop=True,
            reconcile_after_stop=True,
        )
        if not isinstance(result, Mapping) or not _snapshot_status_stopped_ok(result):
            raise WorkerFirstRuntimeError("snapshot_runtime_stop_failed")
        with self._mutable_lock:
            self._snapshot_quiesced = True
        self._release_authorities_if_quiescent()
        return result

    async def _run_processor(
        self,
        lease: CompletionLease,
        desired: DesiredState,
    ) -> PipelineOutcome:
        loop = asyncio.get_running_loop()
        self._assert_admission_open()
        work = self._executor.submit(
            self._processor,
            lease,
            desired,
            self._cancel_event,
        )
        self._register_mutable_future(work, processor=True)
        future = asyncio.wrap_future(work, loop=loop)
        future.add_done_callback(self._consume_async_future)
        try:
            result = await asyncio.wait_for(
                asyncio.shield(future),
                timeout=self._processing_timeout,
            )
        except TimeoutError as error:
            self._cancel_event.set()
            try:
                await asyncio.wait_for(
                    asyncio.shield(future),
                    timeout=self._cancel_grace,
                )
            except TimeoutError:
                pass
            except asyncio.CancelledError:
                with self._mutable_lock:
                    if not work.done():
                        self._detached_mutable_futures.add(work)
                raise
            except Exception:
                pass
            with self._mutable_lock:
                if not work.done():
                    self._detached_mutable_futures.add(work)
            raise WorkerFirstRuntimeError("pipeline_processor_timeout") from error
        except asyncio.CancelledError:
            self._cancel_event.set()
            with self._mutable_lock:
                if not work.done():
                    self._detached_mutable_futures.add(work)
            raise
        finally:
            self._assert_ownership()
        if self._closed or self._stop_requested:
            raise WorkerFirstRuntimeError("runtime_stopping")
        if not isinstance(result, PipelineOutcome):
            raise WorkerFirstRuntimeError("processor gaf geen PipelineOutcome")
        return result

    async def _verify_outcome(
        self,
        lease: CompletionLease,
        desired: DesiredState,
        outcome: PipelineOutcome,
    ) -> VerifiedPipelineOutcome:
        verified = await self._run_blocking(
            lambda: self._outcome_authority.verify(
                lease=lease,
                desired_state=desired,
                outcome=outcome,
            )
        )
        if not isinstance(verified, VerifiedPipelineOutcome):
            raise WorkerFirstRuntimeError("outcome authority gaf geen verified proof")
        expected_authority = desired_authority_proof(desired)["authority_sha256"]
        if (
            verified.event_id != lease.event_id
            or verified.event_sha256 != lease.event_sha256
            or verified.producer_receipt_sha256
            != lease.producer_receipt_sha256
            or verified.desired_state_sha256 != desired.state_sha256
            or verified.authority_epoch != desired.authority_epoch
            or verified.authority_sha256 != expected_authority
            or verified.authority_sha256 != lease.authority_sha256
            or verified.consumer_id != lease.consumer_id
            or verified.consumer_id != self._consumer_id
            or verified.stream_id != lease.stream_id
            or verified.stream_id != self._stream_id
            or verified.stream_sequence != lease.stream_sequence
            or verified.output_sha256 != outcome.output_sha256
            or verified.process_receipt_sha256
            != outcome.process_receipt_sha256
            or verified.disposition != outcome.disposition
            or (verified.fact_binding is not None)
            is not outcome.fact_revision_staged
        ):
            raise WorkerFirstRuntimeError("pipeline_output_authority_mismatch")
        binding = verified.fact_binding
        if binding is not None and (
            binding.process_receipt_sha256
            != verified.process_receipt_sha256
            or binding.desired_state_sha256 != desired.state_sha256
            or binding.authority_epoch != desired.authority_epoch
        ):
            raise WorkerFirstRuntimeError("pipeline_fact_authority_mismatch")
        return verified

    async def async_start(self) -> dict[str, Any]:
        """Audit recovery authorities before accepting one completion."""

        async with self._gate:
            if self._closed or self._stop_requested:
                raise RuntimeError("worker-first runtime is gesloten")
            self._assert_ownership()
            self._increment("start_calls")
            if self._started:
                return self.diagnostics()
            self._state = "starting"
            snapshot_start_invoked = False
            self._begin_runtime_flow()
            try:
                self._claim_owner()
                await self._run_blocking(self._completion.resume_pending_prunes)
                archive = await self._run_blocking(self._completion.audit_archive)
                if not isinstance(archive, Mapping) or archive.get("healthy") is not True:
                    raise WorkerFirstRuntimeError("acquisition_archive_audit_failed")
                fact_audit = await self._run_blocking(self._facts.audit)
                if not isinstance(fact_audit, Mapping) or fact_audit.get("status") != "ok":
                    raise WorkerFirstRuntimeError("fact_store_audit_failed")
                desired = await self._load_desired()
                if desired is not None:
                    await self._run_blocking(self._facts.supersede_obsolete, desired)
                else:
                    await self._run_blocking(
                        lambda: self._facts.supersede_all_ready(
                            reason="desired_state_unavailable"
                        )
                    )
                snapshot_start_invoked = True
                snapshot_status = await self._start_snapshots()
                if desired is not None and not _snapshot_status_ok(snapshot_status):
                    raise WorkerFirstRuntimeError("snapshot_runtime_start_failed")
                if desired is None and not _snapshot_status_unconfigured_ok(
                    snapshot_status
                ):
                    raise WorkerFirstRuntimeError("snapshot_runtime_start_failed")
                self._started = True
                self._state = "running" if desired is not None else "unconfigured"
                self._last_error_code = None
            except BaseException as error:
                stop_was_requested = self._stop_requested
                snapshot_stop_failed = False
                if snapshot_start_invoked:
                    _processor, _mutable, snapshot_pending = (
                        self._pending_async_work()
                    )
                    if snapshot_pending:
                        snapshot_stop_failed = True
                    else:
                        try:
                            await self._stop_snapshots(
                                timeout_seconds=self._control_timeout
                            )
                        except BaseException:
                            snapshot_stop_failed = True
                self._started = False
                self._closed = True
                self._stop_requested = True
                self._state = (
                    "stopped_pending_snapshot"
                    if snapshot_stop_failed
                    else "stopping"
                    if stop_was_requested
                    else "error"
                )
                if snapshot_stop_failed:
                    self._last_error_code = "snapshot_runtime_stop_failed"
                self._shutdown_executor(wait=False)
                self._end_runtime_flow()
                if isinstance(
                    error,
                    (
                        asyncio.CancelledError,
                        WorkerFirstRuntimeError,
                        WorkerFirstRuntimeOwnershipError,
                    ),
                ):
                    raise
                raise WorkerFirstRuntimeError("runtime_start_failed") from None
            self._end_runtime_flow()
            return self.diagnostics()

    async def _acknowledge(
        self,
        lease: CompletionLease,
        output_sha256: str,
    ) -> ConsumptionReceipt:
        def commit_and_verify() -> ConsumptionReceipt:
            receipt = self._completion.acknowledge(
                lease.event_id,
                lease.lease_token,
                output_sha256=output_sha256,
                now=float(self._clock()),
            )
            try:
                self._validate_consumption_receipt(
                    receipt,
                    lease,
                    expected_output_sha256=output_sha256,
                )
            except WorkerFirstRuntimeError:
                self._record_failure("completion_ack_receipt_mismatch")
                raise
            with self._mutable_lock:
                self._increment("acknowledged")
                self._increment("terminalized")
            return receipt

        receipt = await self._run_blocking(
            commit_and_verify,
            reconcile_after_stop=True,
        )
        return receipt

    @staticmethod
    def _validate_consumption_receipt(
        receipt: object,
        lease: CompletionLease,
        *,
        expected_output_sha256: str | None,
    ) -> None:
        if not isinstance(receipt, ConsumptionReceipt):
            raise WorkerFirstRuntimeError("completion ack gaf geen receipt")
        if (
            receipt.event_id != lease.event_id
            or receipt.event_sha256 != lease.event_sha256
            or receipt.producer_receipt_sha256
            != lease.producer_receipt_sha256
            or receipt.consumer_id != lease.consumer_id
            or receipt.stream_id != lease.stream_id
            or receipt.stream_sequence != lease.stream_sequence
            or (
                expected_output_sha256 is not None
                and receipt.output_sha256 != expected_output_sha256
            )
            or _SHA256_RE.fullmatch(receipt.output_sha256) is None
        ):
            raise WorkerFirstRuntimeError("completion_ack_receipt_mismatch")

    async def _reject_obsolete(
        self,
        lease: CompletionLease,
        *,
        active_desired: DesiredState | None,
        reason: str,
    ) -> ConsumptionReceipt:
        rejection_reason = (
            "active_desired_state_unavailable"
            if active_desired is None
            else reason
        )
        def commit_and_verify() -> ConsumptionReceipt:
            receipt = self._completion.reject_leased(
                lease.event_id,
                lease.lease_token,
                expected_event_sha256=lease.event_sha256,
                reason_code=rejection_reason,
                active_desired_state_sha256=(
                    None
                    if active_desired is None
                    else active_desired.state_sha256
                ),
                now=float(self._clock()),
            )
            try:
                self._validate_consumption_receipt(
                    receipt,
                    lease,
                    expected_output_sha256=None,
                )
            except WorkerFirstRuntimeError:
                self._record_failure("completion_reject_receipt_mismatch")
                raise
            with self._mutable_lock:
                self._increment("rejected")
                self._increment("terminalized")
            return receipt

        receipt = await self._run_blocking(
            commit_and_verify,
            reconcile_after_stop=True,
        )
        return receipt

    async def async_tick(self) -> dict[str, Any]:
        """Process at most one contiguous completion through the heavy lane."""

        if not self._started:
            raise RuntimeError("worker-first runtime is niet gestart")
        if self._closed or self._stop_requested:
            raise RuntimeError("worker-first runtime is gesloten")
        async with self._gate:
            if self._closed or self._stop_requested:
                raise RuntimeError("worker-first runtime is gesloten")
            self._assert_ownership()
            self._increment("ticks")
            self._begin_runtime_flow()
            try:
                processor_running, mutable_running, snapshot_running = (
                    self._pending_async_work()
                )
                if processor_running or mutable_running or snapshot_running:
                    code = (
                        "processor_unresponsive"
                        if processor_running
                        else "control_operation_pending"
                        if mutable_running
                        else "snapshot_operation_pending"
                    )
                    self._record_failure(code)
                    return self.diagnostics()
                self._prepare_processor_admission()
                desired = await self._load_desired()
                if desired is None:
                    await self._run_blocking(
                        lambda: self._facts.supersede_all_ready(
                            reason="desired_state_unavailable"
                        )
                    )
                    snapshot_status = await self._run_snapshot_boundary(
                        self._snapshots.async_tick,
                        operation_kind="tick",
                    )
                    if not isinstance(
                        snapshot_status,
                        Mapping,
                    ) or not _snapshot_status_unconfigured_ok(snapshot_status):
                        raise WorkerFirstRuntimeError(
                            "snapshot_runtime_tick_failed"
                        )
                    lease = await self._run_blocking(
                        lambda: self._completion.claim_next(
                            self._consumer_id,
                            self._stream_id,
                            self._owner_id,
                            lease_seconds=self._lease_seconds,
                            now=float(self._clock()),
                        )
                    )
                    if lease is None:
                        self._increment("idle_ticks")
                    else:
                        if not isinstance(lease, CompletionLease):
                            raise WorkerFirstRuntimeError(
                                "completion lease heeft fout type"
                            )
                        self._increment("leases")
                        await self._reject_obsolete(
                            lease,
                            active_desired=None,
                            reason="active_desired_state_unavailable",
                        )
                        self._assert_admission_open()
                        self._increment("authority_withheld")
                        self._increment("withheld")
                        self._last_disposition = "withheld"
                    self._state = "unconfigured"
                    self._last_error_code = None
                    return self.diagnostics()
                await self._run_blocking(self._facts.supersede_obsolete, desired)
                snapshot_status = await self._run_snapshot_boundary(
                    self._snapshots.async_tick,
                    operation_kind="tick",
                )
                if not isinstance(snapshot_status, Mapping) or not _snapshot_status_ok(
                    snapshot_status
                ):
                    raise WorkerFirstRuntimeError("snapshot_runtime_tick_failed")
                lease = await self._run_blocking(
                    lambda: self._completion.claim_next(
                        self._consumer_id,
                        self._stream_id,
                        self._owner_id,
                        lease_seconds=self._lease_seconds,
                        now=float(self._clock()),
                    )
                )
                if lease is None:
                    self._increment("idle_ticks")
                    self._state = "running"
                    self._last_error_code = None
                    return self.diagnostics()
                if not isinstance(lease, CompletionLease):
                    raise WorkerFirstRuntimeError("completion lease heeft fout type")
                self._increment("leases")
                expected_authority = desired_authority_proof(desired)[
                    "authority_sha256"
                ]
                if (
                    lease.desired_state_sha256 != desired.state_sha256
                    or lease.authority_sha256 != expected_authority
                ):
                    await self._reject_obsolete(
                        lease,
                        active_desired=desired,
                        reason="desired_state_superseded_before_processing",
                    )
                    self._assert_admission_open()
                    self._increment("authority_withheld")
                    self._increment("withheld")
                    self._last_disposition = "withheld"
                    self._state = "running"
                    self._last_error_code = None
                    return self.diagnostics()

                outcome = await self._run_processor(lease, desired)
                self._increment("processed")
                verified = await self._verify_outcome(lease, desired, outcome)
                active_after = await self._load_desired()
                if not self._same_desired(active_after, desired):
                    if active_after is not None:
                        await self._run_blocking(
                            self._facts.supersede_obsolete,
                            active_after,
                        )
                    else:
                        await self._run_blocking(
                            lambda: self._facts.supersede_all_ready(
                                reason="desired_state_unavailable"
                            )
                        )
                    await self._reject_obsolete(
                        lease,
                        active_desired=active_after,
                        reason="desired_state_changed_after_processing",
                    )
                    self._assert_admission_open()
                    self._increment("authority_withheld")
                    self._increment("withheld")
                    self._last_disposition = "withheld"
                    self._state = (
                        "running" if active_after is not None else "unconfigured"
                    )
                    self._last_error_code = None
                    return self.diagnostics()

                if verified.fact_binding is not None:
                    self._increment("fact_triggers")
                    publication_receipt = (
                        await self._run_snapshot_boundary(
                            self._snapshots.async_publish_exact_fact,
                            verified.fact_binding,
                            operation_kind="publish",
                        )
                    )
                    if (
                        not isinstance(
                            publication_receipt,
                            FactPublicationReceipt,
                        )
                        or not publication_receipt.matches(
                            verified.fact_binding
                        )
                    ):
                        raise WorkerFirstRuntimeError(
                            "fact_snapshot_publication_mismatch"
                        )
                active_before_ack = await self._load_desired()
                if not self._same_desired(active_before_ack, desired):
                    if active_before_ack is not None:
                        await self._run_blocking(
                            self._facts.supersede_obsolete,
                            active_before_ack,
                        )
                    else:
                        await self._run_blocking(
                            lambda: self._facts.supersede_all_ready(
                                reason="desired_state_unavailable"
                            )
                        )
                    await self._reject_obsolete(
                        lease,
                        active_desired=active_before_ack,
                        reason="desired_state_changed_before_ack",
                    )
                    self._assert_admission_open()
                    self._increment("authority_withheld")
                    self._increment("withheld")
                    self._last_disposition = "withheld"
                    self._last_error_code = None
                    self._state = (
                        "running"
                        if active_before_ack is not None
                        else "unconfigured"
                    )
                    return self.diagnostics()
                await self._acknowledge(lease, verified.output_sha256)
                self._assert_admission_open()
                self._increment(verified.disposition)
                self._last_disposition = verified.disposition
                self._last_error_code = None
                self._state = "running"
            except asyncio.CancelledError:
                self._cancel_event.set()
                self._increment("cancellations")
                self._record_failure("tick_cancelled")
                raise
            except WorkerFirstRuntimeError as error:
                code = str(error)
                self._record_failure(
                    code
                    if code in {
                        "fact_snapshot_publication_failed",
                        "fact_snapshot_publication_mismatch",
                        "pipeline_fact_authority_mismatch",
                        "pipeline_output_authority_mismatch",
                        "pipeline_processor_timeout",
                        "processor_unresponsive",
                        "control_operation_pending",
                        "control_operation_timeout",
                        "ownership_fence_not_held",
                        "runtime_stopping",
                        "snapshot_operation_pending",
                        "snapshot_operation_timeout",
                        "snapshot_runtime_tick_failed",
                    }
                    else "pipeline_transition_failed"
                )
            except Exception:
                self._record_failure("pipeline_transition_failed")
            finally:
                self._end_runtime_flow()
            return self.diagnostics()

    def _finalize_stop_state(self, *, error_code: str | None = None) -> None:
        self._started = False
        self._closed = True
        self._shutdown_executor(wait=False)
        with self._mutable_lock:
            processor_running = (
                self._active_processor_future is not None
                and not self._active_processor_future.done()
            )
            mutable_running = bool(self._active_mutable_futures)
            snapshot_running = bool(self._active_snapshot_tasks)
            runtime_flow_running = self._active_runtime_flows > 0
            snapshot_quiesced = self._snapshot_quiesced
            ownership_released = self._ownership_released
        if ownership_released:
            self._state = "stopped"
        elif processor_running:
            self._state = "stopped_pending_processor"
            self._last_error_code = "processor_unresponsive"
        elif mutable_running:
            self._state = "stopped_pending_mutable_work"
            self._last_error_code = "mutable_work_pending"
        elif snapshot_running:
            self._state = "stopped_pending_snapshot"
            self._last_error_code = "snapshot_operation_pending"
        elif runtime_flow_running:
            self._state = "stopped_pending_gate"
            self._last_error_code = "stop_gate_timeout"
        elif not snapshot_quiesced:
            self._state = "stopped_pending_snapshot"
            self._last_error_code = "snapshot_runtime_stop_failed"
        else:
            self._state = "stopping"
            self._release_authorities_if_quiescent()
        if error_code is not None and not self._ownership_released:
            self._last_error_code = error_code

    async def async_stop(self) -> dict[str, Any]:
        """Bounded admission stop; ownership remains until real quiescence."""

        if self._closed and self._ownership_released:
            return self.diagnostics()
        with self._mutable_lock:
            self._stop_requested = True
            self._closed = True
            self._started = False
            self._cancel_event.set()
        self._increment("stop_calls")
        self._state = "stopping"
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._stop_timeout
        gate_acquired = False
        stop_error: str | None = None
        try:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError
            try:
                await asyncio.wait_for(
                    self._gate.acquire(),
                    timeout=remaining,
                )
                gate_acquired = True
            except TimeoutError:
                self._increment("stop_timeouts")
                stop_error = "stop_gate_timeout"

            if gate_acquired and not self._ownership_released:
                _processor, _mutable, snapshot_running = (
                    self._pending_async_work()
                )
                with self._mutable_lock:
                    snapshot_needs_stop = not self._snapshot_quiesced
                if snapshot_needs_stop and not snapshot_running:
                    remaining = deadline - loop.time()
                    if remaining < 0.001:
                        self._increment("stop_timeouts")
                        stop_error = "snapshot_stop_timeout"
                    else:
                        try:
                            await self._stop_snapshots(
                                timeout_seconds=remaining,
                            )
                        except asyncio.CancelledError:
                            raise
                        except WorkerFirstRuntimeError as error:
                            stop_error = str(error)
                            if stop_error == "snapshot_stop_timeout":
                                self._increment("stop_timeouts")
                            elif stop_error != "snapshot_runtime_stop_failed":
                                stop_error = "snapshot_runtime_stop_failed"
                        except Exception:
                            stop_error = "snapshot_runtime_stop_failed"
        except asyncio.CancelledError:
            self._increment("cancellations")
            stop_error = "stop_cancelled"
            raise
        finally:
            if gate_acquired:
                self._gate.release()
            self._finalize_stop_state(error_code=stop_error)
        return self.diagnostics()

    def diagnostics(self) -> dict[str, Any]:
        """O(1), fixed and privacy-minimal; performs no store reads."""

        snapshot = self._snapshots.diagnostics()
        with self._mutable_lock:
            mutable_work_count = len(self._active_mutable_futures)
            detached_work_count = len(self._detached_mutable_futures)
            snapshot_operations = self._active_snapshot_operations
            snapshot_task_count = len(self._active_snapshot_tasks)
            detached_snapshot_count = len(self._detached_snapshot_tasks)
            runtime_flow_count = self._active_runtime_flows
            snapshot_quiesced = self._snapshot_quiesced
            ownership_released = self._ownership_released
            counts = dict(self._counts)
        raw_snapshot_state = snapshot.get("state")
        snapshot_state = (
            raw_snapshot_state
            if isinstance(raw_snapshot_state, str)
            and raw_snapshot_state in _SNAPSHOT_STATES
            else "unknown"
        )
        snapshot_error = (
            None
            if snapshot.get("last_error_code") is None
            else "snapshot_runtime_error"
        )
        result = {
            "contract": CONTRACT,
            "state": self._state,
            "started": self._started,
            "closed": self._closed,
            "owns_heavy_lane": self._owns_lane,
            "in_flight": self._in_flight,
            "last_error_code": self._last_error_code,
            "last_disposition": self._last_disposition,
            "counts": counts,
            "ownership": {
                "authority_scope_sha256": self._ownership_scope_sha256,
                "authority_sha256": self._ownership_authority_sha256,
                "fence_held": (
                    not ownership_released
                    and self._last_error_code != "ownership_fence_not_held"
                ),
                "detached_mutable_work_count": detached_work_count,
                "detached_snapshot_task_count": detached_snapshot_count,
                "mutable_work_count": mutable_work_count,
                "runtime_flow_count": runtime_flow_count,
                "snapshot_operations": snapshot_operations,
                "snapshot_task_count": snapshot_task_count,
                "snapshot_quiesced": snapshot_quiesced,
                "store_binding_sha256": self._store_binding_sha256,
                "store_count": len(REQUIRED_STORE_ROLES),
            },
            "liveness": {
                "control_timeout_seconds": self._control_timeout,
                "stop_timeout_seconds": self._stop_timeout,
            },
            "snapshot": {
                "state": snapshot_state,
                "last_error_code": snapshot_error,
            },
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise WorkerFirstRuntimeError("runtime diagnostics overschrijdt bytebudget")
        return result


__all__ = [
    "CONTRACT",
    "DEFAULT_CANCEL_GRACE_SECONDS",
    "DEFAULT_CONTROL_TIMEOUT_SECONDS",
    "DEFAULT_LEASE_SECONDS",
    "DEFAULT_PROCESSING_TIMEOUT_SECONDS",
    "DEFAULT_STOP_TIMEOUT_SECONDS",
    "MAX_DIAGNOSTIC_BYTES",
    "FactPublicationBinding",
    "FactPublicationReceipt",
    "PipelineOutcome",
    "PipelineOutcomeAuthority",
    "VerifiedPipelineOutcome",
    "WorkerFirstOwnershipLeaseAuthority",
    "WorkerFirstRuntimeCoordinator",
    "WorkerFirstRuntimeError",
    "WorkerFirstRuntimeOwnershipError",
]
