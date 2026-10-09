"""Crash-replayable DWD acquisition-to-completion publication bridge.

The acquisition runtime commits raw bytes, capacity accounting, an execution
receipt and a terminal scheduler transition.  Those stores intentionally do
not call the DWD science consumer directly.  This bridge closes the remaining
crash window by scanning the append-only terminal inventory and publishing one
exact DWD completion into ``AcquisitionEvidenceLifecycleStore``.

It performs no network request, decode or meteorological admission.  KMI,
KNMI, METAR and every other workload are positively identified from their
historical desired-state plan and skipped; they can never enter the DWD stream
by provider-name inference.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import threading
from typing import Any, Final

from .acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
    CompletionProducerReceipt,
    publish_dwd_completion,
)
from .acquisition_execution_store import ExecutionReceiptStore
from .autonomous_scheduler import AutonomousScheduler
from .desired_state import DesiredState, DesiredStateStore
from .raw_artifacts import RawArtifactStore
from .raw_capacity_ledger import RawCapacityLedger
from .worker_acquisition_plan import (
    AcquisitionJob,
    AcquisitionPlan,
    build_worker_acquisition_plan,
)
from .acquisition_scheduler_bridge import reconstruct_acquisition_job_for_slot


CONTRACT: Final = "stv3-worker-dwd-acquisition-completion-bridge-v1"
MAX_SCAN_ROWS: Final = 64
MAX_DIAGNOSTIC_BYTES: Final = 4096
DWD_WORKLOAD_ID: Final = "radar.dwd.radolan_rv"
DWD_CAPTURE_PRODUCT: Final = "dwd_open_data_composite_rv_raw_capture"


class DwdAcquisitionCompletionBridgeError(RuntimeError):
    """A bounded, secret-free bridge failure."""


class DwdAcquisitionCompletionBridgeIntegrityError(
    DwdAcquisitionCompletionBridgeError
):
    """Durable source authorities cannot be cross-bound exactly."""


@dataclass(frozen=True, slots=True)
class DwdCompletionBridgeResult:
    status: str
    reason_code: str
    scanned_rows: int
    acquisition_job_id: str | None = None
    producer_receipt_sha256: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "acquisition_job_id": self.acquisition_job_id,
            "contract": CONTRACT,
            "producer_receipt_sha256": self.producer_receipt_sha256,
            "reason_code": self.reason_code,
            "scanned_rows": self.scanned_rows,
            "status": self.status,
        }


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
        raise DwdAcquisitionCompletionBridgeError(
            "bridge_diagnostics_not_canonical"
        ) from error


class DwdAcquisitionCompletionBridge:
    """Publish at most one missing DWD completion per serialized call."""

    def __init__(
        self,
        *,
        lifecycle_store: AcquisitionEvidenceLifecycleStore,
        desired_state_store: DesiredStateStore,
        scheduler: AutonomousScheduler,
        execution_store: ExecutionReceiptStore,
        capacity_ledger: RawCapacityLedger,
        source_raw_store: RawArtifactStore,
        handoff_raw_store: RawArtifactStore,
        max_scan_rows: int = MAX_SCAN_ROWS,
    ) -> None:
        expected = (
            (lifecycle_store, AcquisitionEvidenceLifecycleStore),
            (desired_state_store, DesiredStateStore),
            (scheduler, AutonomousScheduler),
            (execution_store, ExecutionReceiptStore),
            (capacity_ledger, RawCapacityLedger),
            (source_raw_store, RawArtifactStore),
            (handoff_raw_store, RawArtifactStore),
        )
        if any(not isinstance(value, kind) for value, kind in expected):
            raise TypeError("bridge authority type is invalid")
        if (
            isinstance(max_scan_rows, bool)
            or not isinstance(max_scan_rows, int)
            or not 1 <= max_scan_rows <= 1024
        ):
            raise ValueError("bridge scan bound is invalid")
        self._lifecycle = lifecycle_store
        self._desired = desired_state_store
        self._scheduler = scheduler
        self._execution = execution_store
        self._capacity = capacity_ledger
        self._source_raw = source_raw_store
        self._handoff_raw = handoff_raw_store
        self._max_scan_rows = max_scan_rows
        self._cursor = 0
        self._lock = threading.Lock()
        self._last_result = DwdCompletionBridgeResult(
            "idle", "not_started", 0
        )
        self._counts = {
            "calls": 0,
            "scanned": 0,
            "already_published": 0,
            "non_dwd": 0,
            "published": 0,
            "failures": 0,
        }

    @staticmethod
    def _resolve_job(
        *,
        desired_store: DesiredStateStore,
        scheduler: AutonomousScheduler,
        scheduler_job_id: str,
        acquisition_job_id: str,
    ) -> tuple[DesiredState, AcquisitionPlan, AcquisitionJob]:
        proof = scheduler.terminal_proof(scheduler_job_id)
        if (
            proof is None
            or proof.state != "completed"
            or proof.last_transition != "completed"
        ):
            raise DwdAcquisitionCompletionBridgeIntegrityError(
                "terminal_execution_lacks_completed_scheduler_proof"
            )
        desired = desired_store.load_version(proof.desired_state_sha256)
        if desired is None:
            raise DwdAcquisitionCompletionBridgeIntegrityError(
                "terminal_execution_desired_version_missing"
            )
        plan = build_worker_acquisition_plan(desired)
        try:
            job = reconstruct_acquisition_job_for_slot(
                plan,
                intent_id=proof.intent_id,
                scheduled_for=proof.scheduled_for,
            )
        except Exception as error:
            raise DwdAcquisitionCompletionBridgeIntegrityError(
                "terminal_execution_plan_binding_invalid"
            ) from error
        if (
            job.job_id != acquisition_job_id
            or job.snapshot_sha256 != desired.state_sha256
            or proof.desired_state_sha256 != desired.state_sha256
        ):
            raise DwdAcquisitionCompletionBridgeIntegrityError(
                "terminal_execution_job_binding_invalid"
            )
        return desired, plan, job

    def publish_next(self) -> DwdCompletionBridgeResult:
        """Recover or publish one missing DWD completion, never a prefix claim."""

        if not self._lock.acquire(blocking=False):
            return DwdCompletionBridgeResult("busy", "bridge_call_in_progress", 0)
        try:
            self._counts["calls"] += 1
            page = self._execution.terminal_receipt_page(
                after_cursor=self._cursor,
                max_rows=self._max_scan_rows,
            )
            scanned = 0
            for terminal_cursor, receipt in zip(
                page.cursors, page.receipts, strict=True
            ):
                scanned += 1
                if receipt.outcome != "completed_shadow_raw":
                    self._cursor = terminal_cursor
                    continue
                desired, plan, job = self._resolve_job(
                    desired_store=self._desired,
                    scheduler=self._scheduler,
                    scheduler_job_id=receipt.scheduler_job_id,
                    acquisition_job_id=receipt.acquisition_job_id,
                )
                if job.workload_id != DWD_WORKLOAD_ID:
                    self._cursor = terminal_cursor
                    self._counts["non_dwd"] += 1
                    continue
                if self._lifecycle.contains_acquisition_job(job.job_id):
                    self._cursor = terminal_cursor
                    self._counts["already_published"] += 1
                    continue
                source_capture_id = RawArtifactStore._capture_id(  # noqa: SLF001
                    provider="dwd",
                    product=DWD_CAPTURE_PRODUCT,
                    fetch_id=f"{job.job_id}:capture",
                )
                producer = publish_dwd_completion(
                    lifecycle_store=self._lifecycle,
                    acquisition_plan=plan,
                    desired_state=desired,
                    scheduler=self._scheduler,
                    execution_store=self._execution,
                    capacity_ledger=self._capacity,
                    source_raw_store=self._source_raw,
                    handoff_raw_store=self._handoff_raw,
                    source_capture_id=source_capture_id,
                    acquisition_job_id=job.job_id,
                )
                if not isinstance(producer, CompletionProducerReceipt):
                    raise DwdAcquisitionCompletionBridgeIntegrityError(
                        "completion_publisher_returned_invalid_receipt"
                    )
                self._cursor = terminal_cursor
                self._counts["published"] += 1
                self._counts["scanned"] += scanned
                result = DwdCompletionBridgeResult(
                    "published",
                    "exact_dwd_completion_published",
                    scanned,
                    acquisition_job_id=job.job_id,
                    producer_receipt_sha256=producer.producer_receipt_sha256,
                )
                self._last_result = result
                return result
            self._counts["scanned"] += scanned
            reason = (
                "terminal_inventory_exhausted"
                if page.exhausted
                else "terminal_scan_page_complete"
            )
            result = DwdCompletionBridgeResult("idle", reason, scanned)
            self._last_result = result
            return result
        except DwdAcquisitionCompletionBridgeError:
            self._counts["failures"] += 1
            raise
        except Exception as error:
            self._counts["failures"] += 1
            raise DwdAcquisitionCompletionBridgeIntegrityError(
                "dwd_completion_bridge_failed_closed"
            ) from error
        finally:
            self._lock.release()

    def diagnostics(self) -> dict[str, Any]:
        result = {
            "contract": CONTRACT,
            "counts": dict(self._counts),
            "cursor": self._cursor,
            "last_result": self._last_result.as_dict(),
            "max_scan_rows": self._max_scan_rows,
            "meteorological_admission": False,
            "source_reclaim_authorized": False,
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise DwdAcquisitionCompletionBridgeError(
                "bridge_diagnostics_capacity_exceeded"
            )
        result["diagnostics_sha256"] = hashlib.sha256(
            _canonical(result)
        ).hexdigest()
        return result


__all__ = [
    "CONTRACT",
    "DWD_CAPTURE_PRODUCT",
    "DWD_WORKLOAD_ID",
    "DwdAcquisitionCompletionBridge",
    "DwdAcquisitionCompletionBridgeError",
    "DwdAcquisitionCompletionBridgeIntegrityError",
    "DwdCompletionBridgeResult",
]
