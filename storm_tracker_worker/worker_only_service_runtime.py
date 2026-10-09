"""Production worker-only runtime: all-source capture plus thin-HA snapshots.

The raw acquisition runtime remains the sole owner of provider access, storage,
retention and desired-state control.  This layer consumes only verified capture
receipts and publishes the bounded operational contract that Home Assistant can
render without running meteorological provider code itself.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import math
import time
from typing import Any, Awaitable, Callable, Mapping

from .multi_source_cycle_assembler import (
    CapturedSourceEvidence,
    DEFAULT_FRESHNESS_SECONDS,
    MAX_FRESHNESS_SECONDS,
    build_raw_capture_operational_cycle,
    captured_evidence_from_raw_receipt,
)
from .multi_source_operational_cycle import CATALOG_SOURCES, CycleStatus
from .multi_source_operational_cycle import CycleTarget
from .multi_source_ground_facts import (
    GROUND_DECODER_BINDINGS,
    GROUND_SOURCE_IDS,
    GroundTargetScope,
    build_all_ground_slice,
    build_ground_cycle_contributions,
)
from .canonical_ground_evidence_store import GroundObservationReceipt
from .multi_source_snapshot import (
    build_multi_source_status_fact_revision,
    publication_safe_current_source_ids,
)
from .multi_source_radar_snapshot import (
    PositiveRadarTargetObservation,
    RadarTargetObservation,
    WithheldDryRadarTargetObservation,
    WithheldUndetectRadarTargetObservation,
    build_positive_radar_fact_revision,
)
from .operational_fact_store import (
    OperationalFactRevisionStore,
    OperationalFactStoreConflict,
)
from .operational_snapshot_publication import PublicationOutboxStore
from .operational_snapshot_runtime import (
    SNAPSHOT_REFRESH_MARGIN_SECONDS,
    OperationalSnapshotRuntime,
)
from .operational_snapshot_store import OperationalSnapshotStore
from .raw_capture_service_composition import (
    RawCaptureServiceConfig,
    RawCaptureServiceRuntime,
    build_raw_capture_service,
)
from .raw_calibration_index import RawCalibrationIndex
from .postgis_projection import (
    PostGISProjectionSettings,
    PostGISRawCalibrationProjector,
)


CONTRACT = "stv3-worker-only-service-runtime-v1"
MODE = "service_worker_only"
SOURCE_COUNT = len(CATALOG_SOURCES)
SUCCESS_STATUSES = frozenset({"completed_shadow_raw", "completion_replayed"})
MAX_COUNTER = 1_000_000_000
MAX_SNAPSHOT_SETTLE_TICKS = 16
SNAPSHOT_MAINTENANCE_POLL_SECONDS = 10.0

_LOGGER = logging.getLogger(__name__)
_SAFE_FAILURE_REASON_CODES = frozenset({
    "canonical ground catalog exceeded its physical file envelope",
    "canonical ground evidence database capacity was exhausted",
    "canonical ground evidence main database exceeds its reserved envelope",
    "canonical ground main database exceeded its physical envelope",
    "canonical ground rollback journal exceeded its physical reserve",
    "ground batch canonical-byte bound exceeded",
    "ground batch fact-count bound exceeded",
    "ground fact exceeds configured canonical byte bound",
    "ground fact row bound reached",
    "ground member-link row bound reached",
    "ground QC row bound reached",
    "desired_state_changed_during_plan",
    "desired_state_future",
    "desired_state_inconsistent",
    "desired_state_stale",
    "desired_state_unavailable",
    "raw_capture_authority_invalid",
    "raw_capture_authority_path_mismatch",
    "raw_capture_desired_state_rejected",
    "raw_capture_lane_closed",
    "raw_capture_ownership_lost",
    "raw_capture_resource_envelope_invalid",
    "raw_capture_retention_guard_blocked",
    "raw_capture_retention_guard_failed",
    "raw_capture_runtime_not_running",
    "raw_capture_runtime_restart_required",
    "raw_capture_runtime_stopping",
    "raw_capture_tick_failed",
    "raw_capture_tick_timeout",
    "worker_only_clock_invalid",
    "worker_only_fact_store_unavailable",
    "worker_only_fact_slot_recovery_failed",
    "worker_only_output_unavailable",
    "worker_only_snapshot_output_not_ready",
    "worker_only_status_invalid",
})


class WorkerOnlyServiceRuntimeError(RuntimeError):
    """The combined acquisition/output runtime cannot preserve its contract."""


def _bounded_counter(value: int) -> int:
    return min(MAX_COUNTER, max(0, int(value)))


def _safe_failure_reason(error: BaseException) -> str:
    """Return only a fixed internal reason code, never free exception text."""

    reason = str(error)
    return reason if reason in _SAFE_FAILURE_REASON_CODES else "unclassified"


class WorkerOnlyServiceRuntime:
    """Serialize provider acquisition, evidence projection and snapshot output."""

    def __init__(
        self,
        *,
        raw_runtime: RawCaptureServiceRuntime,
        publisher: Callable[[bytes], Any],
        status_supplier: Callable[[], Mapping[str, Any]],
        operational_fact_path,
        operational_snapshot_path,
        snapshot_outbox_path,
        radar_artifact_root=None,
        radar_evidence_path=None,
        radar_handoff_root=None,
        radar_processor: Callable[..., tuple[RadarTargetObservation, ...]]
        | None = None,
        ground_evidence_path=None,
        ground_processor: Callable[..., tuple[GroundObservationReceipt, ...]]
        | None = None,
        cell_tracker=None,
        raw_calibration_path=None,
        postgis_settings: PostGISProjectionSettings | None = None,
        postgis_connect_factory: Callable[..., Any] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not isinstance(raw_runtime, RawCaptureServiceRuntime):
            raise TypeError("raw_runtime heeft het verkeerde type")
        if not callable(publisher) or not callable(status_supplier):
            raise TypeError("worker-only callbacks moeten callable zijn")
        if not callable(clock):
            raise TypeError("worker-only clock moet callable zijn")
        self._raw = raw_runtime
        self._publisher = publisher
        self._status_supplier = status_supplier
        self._fact_path = operational_fact_path
        self._snapshot_path = operational_snapshot_path
        self._outbox_path = snapshot_outbox_path
        self._radar_artifact_root = radar_artifact_root
        self._radar_evidence_path = radar_evidence_path
        self._radar_handoff_root = radar_handoff_root
        self._radar_processor = radar_processor
        self._ground_evidence_path = ground_evidence_path
        self._ground_processor = ground_processor
        self._cell_tracker = cell_tracker
        self._raw_calibration_index = (
            None
            if raw_calibration_path is None
            else RawCalibrationIndex(raw_calibration_path)
        )
        self._raw_calibration_stats: dict[str, Any] = {
            "status": (
                "not_configured"
                if self._raw_calibration_index is None
                else "not_initialized"
            ),
            "captures": 0,
            "sources": 0,
            "providers": 0,
            "latest_received_at": None,
            "database_bytes": 0,
            "exact_raw": True,
            "source_statistics": [],
        }
        self._postgis_projector = (
            None
            if self._raw_calibration_index is None or postgis_settings is None
            else PostGISRawCalibrationProjector(
                self._raw_calibration_index,
                postgis_settings,
                clock=clock,
                connect_factory=postgis_connect_factory,
            )
        )
        self._postgis_projection_stats: dict[str, Any] = {
            "enabled": self._postgis_projector is not None,
            "status": (
                "waiting" if self._postgis_projector is not None else "disabled"
            ),
            "batch": 0,
            "ready": 0,
            "leased": 0,
            "projected": 0,
            "last_success_at": None,
            "last_error_code": None,
        }
        self._clock = clock
        self._gate = asyncio.Lock()
        self._output: OperationalSnapshotRuntime | None = None
        self._fact_store: OperationalFactRevisionStore | None = None
        self._snapshot_store: OperationalSnapshotStore | None = None
        self._captures: dict[str, CapturedSourceEvidence] = {}
        self._capture_ids: dict[str, str] = {}
        self._radar_observations: dict[
            tuple[str, str], RadarTargetObservation
        ] = {}
        self._quantitative_desired_state_sha256: str | None = None
        self._quantitative_refresh_in_progress = False
        self._source_errors: dict[str, str] = {}
        self._quantitative_errors: dict[str, str] = {}
        self._ground_receipts: dict[
            str, tuple[GroundObservationReceipt, ...]
        ] = {}
        self._ground_errors: dict[str, str] = {}
        self._ground_track_match_errors: dict[str, str] = {}
        self._tracking_errors: dict[str, str] = {}
        self._seen_terminal: set[tuple[str, str | None, str | None]] = set()
        self._cycle_sequence = 0
        self._started = False
        self._stop_requested = False
        self._state = "new"
        self._last_error_code: str | None = None
        self._last_cycle_counts = {
            "current": 0,
            "stale": 0,
            "error": 0,
            "blocked": 0,
            "not_applicable": 0,
        }
        self._counts = {
            "start_calls": 0,
            "ticks": 0,
            "stop_calls": 0,
            "terminal_updates": 0,
            "cycles_staged": 0,
            "snapshots_published": 0,
            "failures": 0,
            "quantitative_failures": 0,
            "quantitative_skipped_stale": 0,
            "quantitative_successes": 0,
            "ground_failures": 0,
            "ground_successes": 0,
            "ground_track_match_failures": 0,
            "ground_track_match_successes": 0,
            "tracking_duplicates": 0,
            "tracking_failures": 0,
            "tracking_resets": 0,
            "tracking_successes": 0,
            "restored_captures": 0,
            "raw_calibration_indexed": 0,
            "raw_calibration_index_failures": 0,
            "postgis_projection_flushes": 0,
            "postgis_projection_successes": 0,
            "postgis_projection_failures": 0,
        }

    def _increment(self, name: str) -> None:
        self._counts[name] = _bounded_counter(self._counts[name] + 1)

    def _now(self) -> float:
        value = self._clock()
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise WorkerOnlyServiceRuntimeError("worker_only_clock_invalid")
        result = float(value)
        if not math.isfinite(result) or result < 0:
            raise WorkerOnlyServiceRuntimeError("worker_only_clock_invalid")
        return result

    def _initialize_raw_calibration_index(self) -> None:
        index = self._raw_calibration_index
        if index is None:
            return
        try:
            initialized = index.initialize()
            initialized["source_statistics"] = [
                dict(row) for row in index.source_statistics()
            ]
            self._raw_calibration_stats = initialized
        except Exception:
            self._increment("raw_calibration_index_failures")
            self._raw_calibration_stats = {
                **self._raw_calibration_stats,
                "status": "error",
                "error_code": "raw_calibration_index_initialize_failed",
            }

    def _index_raw_calibration_capture(self, source_id: str, capture) -> None:
        """Bind verified raw provenance without duplicating provider bytes."""

        index = self._raw_calibration_index
        if index is None:
            return
        try:
            result = index.record(
                source_id,
                capture,
                indexed_at=self._now(),
            )
        except Exception:
            self._increment("raw_calibration_index_failures")
            self._raw_calibration_stats = {
                **self._raw_calibration_stats,
                "status": "error",
                "error_code": "raw_calibration_index_write_failed",
            }
            return
        already_indexed = result.pop("already_indexed", False)
        result["source_statistics"] = [
            dict(row) for row in index.source_statistics()
        ]
        self._raw_calibration_stats = result
        if not already_indexed:
            self._increment("raw_calibration_indexed")

    async def _flush_postgis_projection_noncritical(self) -> None:
        """Drain one optional batch without affecting raw acquisition truth."""

        projector = self._postgis_projector
        if projector is None:
            return
        self._increment("postgis_projection_flushes")
        try:
            result = await asyncio.to_thread(projector.flush_once)
        except Exception:
            self._increment("postgis_projection_failures")
            self._postgis_projection_stats = {
                **self._postgis_projection_stats,
                "status": "degraded",
                "last_error_code": "postgis_projection_runtime_failed",
            }
            return
        self._postgis_projection_stats = dict(result)
        if result.get("status") == "current":
            if int(result.get("batch", 0)) > 0:
                self._increment("postgis_projection_successes")
        else:
            self._increment("postgis_projection_failures")

    def _ensure_output(self) -> None:
        if self._output is not None:
            return
        self._fact_store = OperationalFactRevisionStore(
            self._fact_path,
            clock=self._clock,
        )
        self._snapshot_store = OperationalSnapshotStore(
            self._snapshot_path,
            clock=self._clock,
        )
        outbox = PublicationOutboxStore(
            self._outbox_path,
            clock=self._clock,
        )
        self._output = OperationalSnapshotRuntime(
            publisher=self._publisher,
            status_supplier=self._status_supplier,
            desired_state_store=self._raw.desired_state_store,
            snapshot_store=self._snapshot_store,
            outbox_store=outbox,
            fact_revision_store=self._fact_store,
            clock=self._clock,
        )

    @staticmethod
    def _snapshot_output_ready(output: Mapping[str, Any]) -> bool:
        publication = output.get("publication")
        if not isinstance(publication, Mapping):
            return False
        revision = publication.get("snapshot_revision")
        return (
            output.get("state") in {"running", "unconfigured"}
            and output.get("started") is True
            and output.get("closed") is False
            and output.get("in_flight") is False
            and output.get("last_error_code") is None
            and publication.get("dirty") is False
            and publication.get("last_error_code") is None
            and type(revision) is int
            and 0 <= revision <= MAX_COUNTER
            and (
                publication.get("transport_state") == "published"
                if revision > 0
                else publication.get("transport_state") is None
            )
        )

    @staticmethod
    def _snapshot_output_was_safely_superseded(
        output: Mapping[str, Any],
    ) -> bool:
        publication = output.get("publication")
        return (
            isinstance(publication, Mapping)
            and output.get("state") == "degraded"
            and output.get("started") is True
            and output.get("closed") is False
            and output.get("in_flight") is False
            and output.get("last_error_code") == "publication_incomplete"
            and publication.get("dirty") is False
            and publication.get("last_error_code") is None
            and type(publication.get("snapshot_revision")) is int
            and 0 < publication["snapshot_revision"] <= MAX_COUNTER
            and publication.get("transport_state") == "superseded"
        )

    async def _settle_snapshot_runtime(
        self,
        runtime: OperationalSnapshotRuntime,
        output: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        if self._snapshot_output_ready(output):
            return output
        # Life360 commonly updates several household members in one short
        # burst.  Each update legitimately replaces desired-state authority
        # and can supersede the envelope that was just committed.  Catch up
        # through a bounded series of output-only ticks: the coordinator has
        # already tombstoned every obsolete envelope, and provider I/O must
        # never be repeated here.
        for _ in range(MAX_SNAPSHOT_SETTLE_TICKS):
            if not self._snapshot_output_was_safely_superseded(output):
                break
            output = await runtime.async_tick()
            if self._snapshot_output_ready(output):
                return output
        raise WorkerOnlyServiceRuntimeError(
            "worker_only_snapshot_output_not_ready"
        )

    async def _settle_snapshot_output(
        self,
        output: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        assert self._output is not None
        return await self._settle_snapshot_runtime(self._output, output)

    def _snapshot_provider_projection_missing(self) -> bool:
        """Return whether the durable head is only the safe empty fallback."""

        if self._snapshot_store is None:
            return True
        head = self._snapshot_store.head()
        if not isinstance(head, Mapping):
            return True
        snapshot = head.get("snapshot")
        if not isinstance(snapshot, Mapping):
            return True
        providers = snapshot.get("providers")
        if not isinstance(providers, Mapping):
            return True
        items = providers.get("items")
        return not isinstance(items, list) or len(items) != SOURCE_COUNT

    async def _restore_provider_projection_if_missing(self) -> bool:
        """Replace an authority-race fallback with the complete status view.

        Settling an MQTT publication while Life360 updates desired-state can
        legitimately publish an all-unknown fallback for the newest authority.
        That fallback contains no provider rows.  Rebuild those rows from the
        already retained capture/error state; this performs no provider I/O.
        """

        if not self._snapshot_provider_projection_missing():
            return False
        if self._output is None:
            raise WorkerOnlyServiceRuntimeError("worker_only_output_unavailable")
        for _ in range(MAX_SNAPSHOT_SETTLE_TICKS):
            if not self._snapshot_provider_projection_missing():
                return True
            if not await self._stage_cycle():
                return False
            before = self._output.diagnostics()["publication"][
                "snapshot_revision"
            ]
            output = await self._output.async_publish_ready_fact()
            output = await self._settle_snapshot_output(output)
            after = output["publication"]["snapshot_revision"]
            if after > before:
                self._increment("snapshots_published")
        if self._snapshot_provider_projection_missing():
            raise WorkerOnlyServiceRuntimeError(
                "worker_only_snapshot_output_not_ready"
            )
        return True

    def _new_snapshot_output(
        self,
        *,
        fact_revision_store: OperationalFactRevisionStore | None,
    ) -> OperationalSnapshotRuntime:
        assert self._snapshot_store is not None
        return OperationalSnapshotRuntime(
            publisher=self._publisher,
            status_supplier=self._status_supplier,
            desired_state_store=self._raw.desired_state_store,
            snapshot_store=self._snapshot_store,
            outbox_store=PublicationOutboxStore(
                self._outbox_path,
                clock=self._clock,
            ),
            fact_revision_store=fact_revision_store,
            clock=self._clock,
        )

    async def _recover_terminalized_snapshot_head(self) -> None:
        """Advance an exact durable head whose fact was terminalized early.

        A crash can leave the snapshot committed while the transport outbox is
        still staged.  Older startup ordering then retired the matching ready
        fact before crash reconciliation ran.  The fact store is append-only:
        do not rewrite that terminal history.  Recover the independently
        verified snapshot/outbox without a fact source, publish a fresh
        fallback revision, then resume with a normal fact-aware runtime.
        """

        recovery = self._new_snapshot_output(fact_revision_store=None)
        try:
            output = await recovery.async_start()
            await self._settle_snapshot_runtime(recovery, output)
        finally:
            await recovery.async_stop()
        assert self._fact_store is not None
        self._output = self._new_snapshot_output(
            fact_revision_store=self._fact_store,
        )
        output = await self._output.async_start()
        await self._settle_snapshot_output(output)

    async def _worker_status(self) -> tuple[str, int, int]:
        supplied = self._status_supplier()
        if inspect.isawaitable(supplied):
            supplied = await supplied
        if not isinstance(supplied, Mapping):
            raise WorkerOnlyServiceRuntimeError("worker_only_status_invalid")
        state = supplied.get("state")
        queue_depth = supplied.get("queue_depth")
        failed_jobs = supplied.get("failed_jobs")
        if (
            state not in {"online", "busy", "degraded", "offline", "unknown"}
            or isinstance(queue_depth, bool)
            or not isinstance(queue_depth, int)
            or isinstance(failed_jobs, bool)
            or not isinstance(failed_jobs, int)
            or not 0 <= queue_depth <= MAX_COUNTER
            or not 0 <= failed_jobs <= MAX_COUNTER
        ):
            raise WorkerOnlyServiceRuntimeError("worker_only_status_invalid")
        return str(state), queue_depth, failed_jobs

    def _next_snapshot_revision(self) -> int:
        if self._snapshot_store is None:
            raise WorkerOnlyServiceRuntimeError("worker_only_output_unavailable")
        head = self._snapshot_store.head()
        return 1 if head is None else int(head["revision"]) + 1

    def _snapshot_requires_restaging(self) -> bool:
        """Return whether current captures must be rebound to live authority."""

        desired = self._raw.desired_state_store.load_active()
        if desired is None:
            return False
        if self._snapshot_store is None:
            return True
        head = self._snapshot_store.head()
        if not isinstance(head, Mapping):
            return True
        snapshot = head.get("snapshot")
        if not isinstance(snapshot, Mapping):
            return True
        if snapshot.get("desired_state_sha256") != desired.state_sha256:
            return True
        valid_until = snapshot.get("valid_until")
        if (
            isinstance(valid_until, bool)
            or not isinstance(valid_until, (int, float))
            or not math.isfinite(float(valid_until))
        ):
            return True
        return self._now() >= float(valid_until) - SNAPSHOT_REFRESH_MARGIN_SECONDS

    @staticmethod
    def _ground_target_scopes(desired) -> tuple[GroundTargetScope, ...]:
        return tuple(
            GroundTargetScope.create(
                CycleTarget.create(
                    target_id=target.target_id,
                    latitude=target.latitude,
                    longitude=target.longitude,
                    country_code=target.country_code,
                    available=target.available,
                ),
                radius_km=desired.settings.ground_radius_km,
            )
            for target in desired.targets
        )

    def _ground_contribution_overrides(
        self,
        desired,
        *,
        evaluated_at_ms: int,
    ) -> dict[str, Any]:
        """Build the exact five-source ground slice from canonical receipts."""

        if self._ground_processor is None or self._ground_evidence_path is None:
            return {}
        scopes = self._ground_target_scopes(desired)
        receipts_by_source = {
            source_id: self._ground_receipts.get(source_id, ())
            for source_id in GROUND_SOURCE_IDS
        }
        errors: dict[str, str] = {}
        for source_id, binding in GROUND_DECODER_BINDINGS.items():
            if not binding.decoder_available or receipts_by_source[source_id]:
                continue
            if source_id in self._ground_errors:
                errors[source_id] = self._ground_errors[source_id]
            elif source_id in self._source_errors:
                errors[source_id] = "ground_capture_unavailable"
            elif source_id not in self._captures:
                errors[source_id] = "awaiting_first_capture"
        ground_slice = build_all_ground_slice(
            desired_state_sha256=desired.state_sha256,
            authority_epoch=desired.authority_epoch,
            evaluated_at_ms=evaluated_at_ms,
            target_scopes=scopes,
            receipts_by_source=receipts_by_source,
            source_errors=errors,
        )
        return {
            item.source_id: item
            for item in build_ground_cycle_contributions(
                ground_slice,
                cycle_completed_at_ms=evaluated_at_ms,
            )
        }

    async def _stage_cycle(self) -> bool:
        desired = self._raw.desired_state_store.load_active()
        if desired is None:
            return False
        if self._fact_store is None:
            raise WorkerOnlyServiceRuntimeError("worker_only_fact_store_unavailable")
        next_revision = self._next_snapshot_revision()
        # HA target/location updates create a new desired-state identity.  A
        # ready fact from the previous identity can never become publishable
        # again and must not accumulate until it blocks bounded retention.
        self._fact_store.supersede_obsolete(desired)
        existing = self._fact_store.select_for_publication(
            desired,
            expected_snapshot_revision=next_revision,
        )
        if existing is not None:
            return True
        now = self._now()
        now_ms = int(now * 1000)
        self._cycle_sequence = _bounded_counter(self._cycle_sequence + 1)
        ground_overrides = self._ground_contribution_overrides(
            desired,
            evaluated_at_ms=now_ms,
        )
        cycle = build_raw_capture_operational_cycle(
            desired,
            tuple(self._captures.values()),
            source_errors=self._source_errors,
            contribution_overrides=ground_overrides,
            cycle_sequence=max(1, self._cycle_sequence),
            cycle_started_at_ms=now_ms,
            cycle_completed_at_ms=now_ms,
        )
        worker_state, queue_depth, failed_jobs = await self._worker_status()
        current_source_ids = publication_safe_current_source_ids(
            cycle,
            evaluated_at_ms=now_ms,
        )
        radar_observations = (
            ()
            if self._quantitative_refresh_in_progress
            else self._radar_observations.values()
        )
        observations = tuple(sorted(
            (
                item
                for item in radar_observations
                if item.source_id in current_source_ids
            ),
            key=lambda item: (item.target_id, item.source_id),
        ))
        committed = (
            build_positive_radar_fact_revision(
                desired,
                cycle,
                observations,
                revision=next_revision,
                evaluated_at=now,
                worker_state=worker_state,
                queue_depth=queue_depth,
                failed_jobs=failed_jobs,
            )
            if observations
            else build_multi_source_status_fact_revision(
                desired,
                cycle,
                revision=next_revision,
                evaluated_at=now,
                worker_state=worker_state,
                queue_depth=queue_depth,
                failed_jobs=failed_jobs,
            )
        )
        item_id = (
            f"worker-only:{self._cycle_sequence}:"
            f"{committed.revision_sha256[:24]}"
        )
        self._fact_store.stage(
            item_id,
            desired_state=desired,
            revision=committed,
        )
        counts = {status.value: 0 for status in CycleStatus}
        for contribution in cycle.contributions:
            counts[contribution.status.value] += 1
        self._last_cycle_counts = counts
        self._increment("cycles_staged")
        return True

    async def _publish_snapshot_refresh_if_due(self) -> bool:
        """Refresh snapshot TTL while one serial provider job is still active."""

        if not self._snapshot_requires_restaging():
            return False
        if self._output is None:
            raise WorkerOnlyServiceRuntimeError("worker_only_output_unavailable")
        if not await self._stage_cycle():
            return False
        before = self._output.diagnostics()["publication"]["snapshot_revision"]
        output = await self._output.async_publish_ready_fact()
        output = await self._settle_snapshot_output(output)
        after = output["publication"]["snapshot_revision"]
        if after > before:
            self._increment("snapshots_published")
            return True
        return False

    async def _await_with_snapshot_maintenance(
        self,
        operation: Awaitable[Any],
    ) -> tuple[Any, bool]:
        """Await one heavy operation without letting the HA snapshot expire."""

        operation_task = asyncio.ensure_future(operation)
        snapshot_published = False
        maintenance_error: Exception | None = None
        try:
            while True:
                try:
                    result = await asyncio.wait_for(
                        asyncio.shield(operation_task),
                        timeout=SNAPSHOT_MAINTENANCE_POLL_SECONDS,
                    )
                    break
                except TimeoutError:
                    if maintenance_error is not None:
                        continue
                    try:
                        snapshot_published = (
                            await self._publish_snapshot_refresh_if_due()
                            or snapshot_published
                        )
                    except Exception as error:
                        # Never abandon the sole heavy executor mid-operation.
                        # Preserve its lifecycle, then surface this failure.
                        maintenance_error = error
        except asyncio.CancelledError:
            operation_task.cancel()
            try:
                await operation_task
            except BaseException:
                pass
            raise
        if maintenance_error is not None:
            raise maintenance_error
        return result, snapshot_published

    async def _raw_tick_with_snapshot_maintenance(self) -> bool:
        """Await the provider lane without letting the HA snapshot expire."""

        _result, snapshot_published = await self._await_with_snapshot_maintenance(
            self._raw.async_tick()
        )
        return snapshot_published

    async def _refresh_quantitative_with_snapshot_maintenance(
        self,
        desired,
    ) -> bool:
        """Reproject every capture while publishing status-only TTL refreshes."""

        self._quantitative_refresh_in_progress = True
        try:
            _result, snapshot_published = (
                await self._await_with_snapshot_maintenance(
                    self._refresh_quantitative_for_desired(desired)
                )
            )
        finally:
            self._quantitative_refresh_in_progress = False
        return snapshot_published

    def _consume_latest_terminal(self) -> bool:
        result = self._raw.latest_tick_result
        if not result.terminal or result.workload_id is None:
            return False
        identity = (
            result.workload_id,
            result.receipt_sha256,
            result.source_capture_id,
        )
        if identity in self._seen_terminal:
            return False
        self._seen_terminal.add(identity)
        if len(self._seen_terminal) > SOURCE_COUNT * 8:
            self._seen_terminal = {identity}
        source_id = result.workload_id
        if result.status in SUCCESS_STATUSES and result.source_capture_id is not None:
            receipt = self._raw.raw_artifact_store.load_capture(
                result.source_capture_id
            )
            if receipt is None:
                self._source_errors[source_id] = "capture_receipt_unavailable"
            else:
                self._captures[source_id] = captured_evidence_from_raw_receipt(
                    source_id,
                    receipt,
                )
                self._capture_ids[source_id] = result.source_capture_id
                self._index_raw_calibration_capture(source_id, receipt)
                self._source_errors.pop(source_id, None)
        else:
            self._source_errors[source_id] = result.reason_code
        self._increment("terminal_updates")
        return True

    def _restore_retained_captures(self) -> None:
        """Restore verified latest evidence without re-fetching providers."""

        reader = getattr(self._raw, "retained_successful_captures", None)
        if not callable(reader):
            return
        restored = reader()
        if not isinstance(restored, tuple):
            raise WorkerOnlyServiceRuntimeError(
                "worker_only_retained_capture_inventory_invalid"
            )
        seen: set[str] = set()
        for item in restored:
            if (
                not isinstance(item, tuple)
                or len(item) != 2
                or not isinstance(item[0], str)
                or item[0] in seen
            ):
                raise WorkerOnlyServiceRuntimeError(
                    "worker_only_retained_capture_inventory_invalid"
                )
            workload_id, capture = item
            evidence = captured_evidence_from_raw_receipt(
                workload_id,
                capture,
            )
            self._captures[workload_id] = evidence
            self._capture_ids[workload_id] = capture.capture_id
            self._index_raw_calibration_capture(workload_id, capture)
            self._source_errors.pop(workload_id, None)
            seen.add(workload_id)
            self._increment("restored_captures")

    async def _process_ground_capture(
        self,
        *,
        workload_id: str,
        capture_id: str,
    ) -> None:
        """Decode one immutable ground capture into canonical evidence."""

        from .worker_only_ground_processing import GROUND_WORKLOAD_IDS

        if (
            self._ground_processor is None
            or self._ground_evidence_path is None
            or workload_id not in GROUND_WORKLOAD_IDS
        ):
            return
        desired = self._raw.desired_state_store.load_active()
        target_scopes = (
            () if desired is None else self._ground_target_scopes(desired)
        )
        try:
            receipts = await asyncio.to_thread(
                self._ground_processor,
                workload_id=workload_id,
                capture_id=capture_id,
                raw_store=self._raw.raw_artifact_store,
                evidence_path=self._ground_evidence_path,
                target_scopes=target_scopes,
                clock=self._clock,
            )
            if not isinstance(receipts, tuple) or any(
                not isinstance(item, GroundObservationReceipt)
                for item in receipts
            ):
                raise WorkerOnlyServiceRuntimeError(
                    "worker_only_ground_processor_invalid"
                )
            self._ground_receipts[workload_id] = receipts
            self._ground_errors.pop(workload_id, None)
            self._increment("ground_successes")
            ground_matcher = getattr(
                self._cell_tracker,
                "process_ground_receipts",
                None,
            )
            if callable(ground_matcher):
                try:
                    await asyncio.to_thread(
                        ground_matcher,
                        workload_id=workload_id,
                        receipts=receipts,
                    )
                    self._ground_track_match_errors.pop(workload_id, None)
                    self._increment("ground_track_match_successes")
                except Exception as match_error:
                    self._ground_track_match_errors[workload_id] = (
                        "ground_track_matching_failed"
                    )
                    self._increment("ground_track_match_failures")
                    _LOGGER.error(
                        "Worker-only ground-track match fail-closed: "
                        "workload=%s class=%s",
                        workload_id,
                        type(match_error).__name__,
                    )
        except Exception as error:
            self._ground_receipts.pop(workload_id, None)
            self._ground_errors[workload_id] = "ground_processing_failed"
            self._increment("ground_failures")
            ground_matcher = getattr(
                self._cell_tracker,
                "process_ground_receipts",
                None,
            )
            if callable(ground_matcher):
                try:
                    await asyncio.to_thread(
                        ground_matcher,
                        workload_id=workload_id,
                        receipts=(),
                    )
                except Exception:
                    self._ground_track_match_errors[workload_id] = (
                        "ground_track_matching_failed"
                    )
            if type(error).__name__ == "CanonicalGroundEvidenceCapacityError":
                # Capacity messages originate from fixed local contract
                # strings.  Logging that bounded reason makes live diagnosis
                # possible without exposing provider payloads or secrets.
                _LOGGER.error(
                    "Worker-only ground decode fail-closed: "
                    "workload=%s class=%s reason=%s",
                    workload_id,
                    type(error).__name__,
                    _safe_failure_reason(error),
                )
            else:
                _LOGGER.error(
                    "Worker-only ground decode fail-closed: workload=%s class=%s",
                    workload_id,
                    type(error).__name__,
                )

    async def _refresh_ground_captures(self) -> None:
        """Replay retained ground captures idempotently after worker start."""

        for workload_id, capture_id in sorted(self._capture_ids.items()):
            await self._process_ground_capture(
                workload_id=workload_id,
                capture_id=capture_id,
            )

    async def _process_latest_ground(self) -> None:
        result = self._raw.latest_tick_result
        if (
            result.status not in SUCCESS_STATUSES
            or result.source_capture_id is None
            or result.workload_id is None
        ):
            return
        await self._process_ground_capture(
            workload_id=result.workload_id,
            capture_id=result.source_capture_id,
        )

    def _quantitative_capture_is_current(self, workload_id: str) -> bool:
        evidence = self._captures.get(workload_id)
        source = next(
            (
                item
                for item in CATALOG_SOURCES
                if item.source_id == workload_id
            ),
            None,
        )
        if evidence is None or source is None:
            return False
        cadence = source.cadence_seconds or DEFAULT_FRESHNESS_SECONDS
        freshness = min(
            MAX_FRESHNESS_SECONDS,
            max(DEFAULT_FRESHNESS_SECONDS, cadence * 2),
        )
        return evidence.product_at_ms + freshness * 1000 >= int(self._now() * 1000)

    async def _process_quantitative_capture(
        self,
        *,
        workload_id: str,
        capture_id: str,
        desired,
    ) -> None:
        """Reproject one immutable quantitative capture for current targets."""

        from .worker_only_radar_processing import QUANTITATIVE_WORKLOAD_IDS

        if (
            self._radar_processor is None
            or self._radar_artifact_root is None
            or self._radar_evidence_path is None
            or workload_id not in QUANTITATIVE_WORKLOAD_IDS
        ):
            return
        if not self._quantitative_capture_is_current(workload_id):
            self._radar_observations = {
                key: value
                for key, value in self._radar_observations.items()
                if key[0] != workload_id
            }
            self._quantitative_errors.pop(workload_id, None)
            self._increment("quantitative_skipped_stale")
            return
        try:
            processed = await asyncio.to_thread(
                self._radar_processor,
                workload_id=workload_id,
                desired_state=desired,
                capture_id=capture_id,
                raw_store=self._raw.raw_artifact_store,
                artifact_root=self._radar_artifact_root,
                evidence_path=self._radar_evidence_path,
                handoff_root=self._radar_handoff_root,
                evaluated_at=self._now(),
                clock=self._clock,
            )
            verified_source = None
            from .worker_only_radar_processing import (
                WorkerOnlyRadarProcessingResult,
            )

            if isinstance(processed, WorkerOnlyRadarProcessingResult):
                observations = processed.observations
                verified_source = processed.verified_source
            else:
                observations = processed
            if not isinstance(observations, tuple) or any(
                not isinstance(
                    item,
                    (
                        PositiveRadarTargetObservation,
                        WithheldDryRadarTargetObservation,
                        WithheldUndetectRadarTargetObservation,
                    ),
                )
                for item in observations
            ):
                raise WorkerOnlyServiceRuntimeError(
                    "worker_only_radar_processor_invalid"
                )
            self._radar_observations = {
                key: value
                for key, value in self._radar_observations.items()
                if key[0] != workload_id
            }
            for observation in observations:
                self._radar_observations[
                    (observation.source_id, observation.target_id)
                ] = observation
            self._quantitative_errors.pop(workload_id, None)
            self._increment("quantitative_successes")
            if self._cell_tracker is not None and verified_source is not None:
                try:
                    tracking = await asyncio.to_thread(
                        self._cell_tracker.process_source,
                        workload_id=workload_id,
                        desired_state=desired,
                        source=verified_source,
                    )
                    self._tracking_errors.pop(workload_id, None)
                    self._increment("tracking_successes")
                    for _ in range(tracking.duplicate_regions):
                        self._increment("tracking_duplicates")
                    for _ in range(tracking.reset_regions):
                        self._increment("tracking_resets")
                except Exception as tracking_error:
                    self._tracking_errors[workload_id] = "tracking_processing_failed"
                    self._increment("tracking_failures")
                    _LOGGER.error(
                        "Worker-only cell tracking fail-closed: "
                        "workload=%s class=%s",
                        workload_id,
                        type(tracking_error).__name__,
                    )
        except Exception as error:
            self._radar_observations = {
                key: value
                for key, value in self._radar_observations.items()
                if key[0] != workload_id
            }
            reason = str(error)
            self._quantitative_errors[workload_id] = (
                reason
                if reason in {
                    "dwd_canonical_candidate_withheld",
                    "dwd_capture_handoff_failed",
                    "dwd_capture_receipt_unavailable",
                    "dwd_quantitative_decode_withheld",
                    "knmi_quantitative_decode_withheld",
                    "met_office_quantitative_decode_withheld",
                    "noaa_quantitative_decode_withheld",
                    "capture_receipt_unavailable",
                }
                else "quantitative_processing_failed"
            )
            self._increment("quantitative_failures")

    async def _refresh_quantitative_for_desired(self, desired) -> None:
        """Re-sample retained raw captures after any target-location change."""

        self._radar_observations.clear()
        for workload_id, capture_id in sorted(self._capture_ids.items()):
            await self._process_quantitative_capture(
                workload_id=workload_id,
                capture_id=capture_id,
                desired=desired,
            )
        self._quantitative_desired_state_sha256 = desired.state_sha256

    async def _process_latest_quantitative(self, desired) -> None:
        result = self._raw.latest_tick_result
        if (
            result.status not in SUCCESS_STATUSES
            or result.source_capture_id is None
            or result.workload_id is None
        ):
            return
        await self._process_quantitative_capture(
            workload_id=result.workload_id,
            capture_id=result.source_capture_id,
            desired=desired,
        )

    async def async_start(self) -> dict[str, Any]:
        async with self._gate:
            self._increment("start_calls")
            if self._started:
                return self.diagnostics()
            if self._stop_requested:
                raise WorkerOnlyServiceRuntimeError("worker_only_runtime_stopping")
            self._state = "starting"
            stage = "raw_start"
            try:
                await self._raw.async_start()
                stage = "raw_calibration_index"
                self._initialize_raw_calibration_index()
                stage = "capture_restore"
                self._restore_retained_captures()
                stage = "output_init"
                self._ensure_output()
                assert self._fact_store is not None
                assert self._snapshot_store is not None
                # Reconcile a crash-committed rich head before selecting the
                # next fact slot.  Selecting first can incorrectly retire the
                # exact head fact as "snapshot_revision_superseded".
                stage = "snapshot_reconcile"
                try:
                    self._fact_store.reconcile_snapshot_head(
                        self._snapshot_store.head()
                    )
                except OperationalFactStoreConflict:
                    stage = "snapshot_head_recovery"
                    await self._recover_terminalized_snapshot_head()
                stage = "snapshot_stage"
                assert self._output is not None
                try:
                    await self._stage_cycle()
                except OperationalFactStoreConflict:
                    # A previous fail-closed run can leave an expired,
                    # terminal fact occupying the next snapshot revision.
                    # The fact store is append-only, so that slot must never
                    # be overwritten.  Let the output runtime commit one
                    # fresh fallback snapshot for the occupied revision,
                    # then stage and publish the following rich revision.
                    stage = "snapshot_conflict_recovery"
                    output = await self._output.async_start()
                    stage = "snapshot_conflict_settle"
                    await self._settle_snapshot_output(output)
                    stage = "snapshot_restage"
                    if not await self._stage_cycle():
                        raise WorkerOnlyServiceRuntimeError(
                            "worker_only_fact_slot_recovery_failed"
                        )
                    stage = "snapshot_publish"
                    output = await self._output.async_publish_ready_fact()
                    stage = "snapshot_settle"
                    await self._settle_snapshot_output(output)
                else:
                    stage = "output_start"
                    output = await self._output.async_start()
                    stage = "output_settle"
                    await self._settle_snapshot_output(output)
                stage = "snapshot_provider_projection"
                await self._restore_provider_projection_if_missing()
                # Startup establishes the durable control plane and publishes
                # a bounded fallback snapshot only.  Replaying retained METAR
                # or radar captures can consume the full low-priority science
                # deadline; doing that here keeps MQTT job admission closed and
                # makes HA's desired-state handshake time out.  The first
                # normal tick already reprojects retained quantitative data and
                # fresh provider completions, after admission has opened.
                self._started = True
                self._state = "running"
                self._last_error_code = None
            except Exception as error:
                self._increment("failures")
                self._state = "error"
                self._last_error_code = "worker_only_start_failed"
                _LOGGER.error(
                    "Worker-only start fail-closed: "
                    "stage=%s class=%s reason=%s",
                    stage,
                    type(error).__name__,
                    _safe_failure_reason(error),
                )
                try:
                    await self._raw.async_stop()
                except Exception:
                    pass
                raise
            return self.diagnostics()

    async def async_tick(self) -> dict[str, Any]:
        if not self._started or self._stop_requested:
            raise WorkerOnlyServiceRuntimeError("worker_only_runtime_not_running")
        async with self._gate:
            self._increment("ticks")
            stage = "raw_tick"
            try:
                maintenance_snapshot_published = (
                    await self._raw_tick_with_snapshot_maintenance()
                )
                stage = "consume_terminal"
                changed = self._consume_latest_terminal()
                desired = self._raw.desired_state_store.load_active()
                if changed and desired is not None:
                    stage = "ground_processing"
                    _result, ground_snapshot_published = (
                        await self._await_with_snapshot_maintenance(
                            self._process_latest_ground()
                        )
                    )
                    maintenance_snapshot_published = (
                        ground_snapshot_published
                        or maintenance_snapshot_published
                    )
                quantitative_authority_changed = (
                    desired is not None
                    and desired.state_sha256
                    != self._quantitative_desired_state_sha256
                )
                authority_snapshot_published = (
                    maintenance_snapshot_published
                    and quantitative_authority_changed
                )
                if (
                    quantitative_authority_changed
                    and self._snapshot_requires_restaging()
                ):
                    # Never make HA wait for target-independent radar decoding.
                    # Old target samples are authority-bound, so clear them and
                    # publish the exact current target/region fallback first.
                    self._radar_observations.clear()
                    assert self._output is not None
                    stage = "authority_snapshot_stage"
                    if await self._stage_cycle():
                        stage = "authority_snapshot_publish"
                        before = self._output.diagnostics()["publication"][
                            "snapshot_revision"
                        ]
                        output = await self._output.async_publish_ready_fact()
                        stage = "authority_snapshot_settle"
                        output = await self._settle_snapshot_output(output)
                        after = output["publication"]["snapshot_revision"]
                        if after > before:
                            self._increment("snapshots_published")
                            authority_snapshot_published = True
                if quantitative_authority_changed:
                    stage = "quantitative_processing"
                    maintenance_snapshot_published = (
                        await self._refresh_quantitative_with_snapshot_maintenance(
                            desired
                        )
                        or maintenance_snapshot_published
                    )
                    authority_snapshot_published = (
                        authority_snapshot_published
                        or maintenance_snapshot_published
                    )
                elif changed and desired is not None:
                    stage = "quantitative_processing"
                    _result, quantitative_snapshot_published = (
                        await self._await_with_snapshot_maintenance(
                            self._process_latest_quantitative(desired)
                        )
                    )
                    maintenance_snapshot_published = (
                        quantitative_snapshot_published
                        or maintenance_snapshot_published
                    )
                assert self._output is not None
                stage = "snapshot_stage"
                restage = (
                    (changed and not authority_snapshot_published)
                    or self._snapshot_requires_restaging()
                    or (
                        authority_snapshot_published
                        and bool(self._radar_observations)
                    )
                )
                if restage and await self._stage_cycle():
                    stage = "snapshot_publish"
                    before = self._output.diagnostics()["publication"]["snapshot_revision"]
                    output = await self._output.async_publish_ready_fact()
                    stage = "snapshot_settle"
                    output = await self._settle_snapshot_output(output)
                    after = output["publication"]["snapshot_revision"]
                    if after > before:
                        self._increment("snapshots_published")
                else:
                    stage = "snapshot_tick"
                    output = await self._output.async_tick()
                    stage = "snapshot_settle"
                    await self._settle_snapshot_output(output)
                stage = "snapshot_provider_projection"
                await self._restore_provider_projection_if_missing()
                await self._flush_postgis_projection_noncritical()
                self._state = "running"
                self._last_error_code = None
            except Exception as error:
                self._increment("failures")
                self._state = "degraded"
                self._last_error_code = "worker_only_tick_failed"
                _LOGGER.error(
                    "Worker-only tick fail-closed: stage=%s class=%s reason=%s",
                    stage,
                    type(error).__name__,
                    _safe_failure_reason(error),
                )
                raise
            return self.diagnostics()

    async def async_stop(self) -> dict[str, Any]:
        self._increment("stop_calls")
        self._stop_requested = True
        async with self._gate:
            output = self._output
            if output is not None:
                await output.async_stop()
            raw = await self._raw.async_stop()
            if (
                raw.get("state") == "stopped"
                and raw.get("shutdown_complete") is True
                and raw.get("ownership_fence_held") is False
            ):
                self._state = "stopped"
                self._last_error_code = None
            else:
                self._state = "stop_pending"
                self._last_error_code = "worker_only_stop_pending"
            return self.diagnostics()

    def handle_control_job(
        self,
        job_type: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self._raw.handle_control_job(job_type, payload)

    def diagnostics(self) -> dict[str, Any]:
        raw = self._raw.diagnostics()
        raw_last_tick = raw.get("last_tick", {})
        if not isinstance(raw_last_tick, Mapping):
            raw_last_tick = {}
        output = None if self._output is None else self._output.diagnostics()
        publication = {} if output is None else output.get("publication", {})
        publication_counts = publication.get("counts", {})
        tracking = (
            {}
            if self._cell_tracker is None
            else self._cell_tracker.diagnostics()
        )
        return {
            "contract": CONTRACT,
            "mode": MODE,
            "state": self._state,
            "live_wiring": True,
            "provider_count": SOURCE_COUNT,
            "captured_provider_count": len(self._captures),
            "raw_calibration": dict(self._raw_calibration_stats),
            "postgis_projection": dict(self._postgis_projection_stats),
            "quantitative_capture_count": len(
                set(self._capture_ids).intersection(
                    {
                        "radar.dwd.radolan_rv",
                        "radar.knmi.rtcor",
                        "radar.metoffice.uk_rate",
                        "satellite.noaa.err",
                    }
                )
            ),
            "quantitative_errors": dict(sorted(self._quantitative_errors.items())),
            "ground_source_count": len(self._ground_receipts),
            "ground_fact_count": sum(
                len(items) for items in self._ground_receipts.values()
            ),
            "ground_errors": dict(sorted(self._ground_errors.items())),
            "ground_track_match_count": tracking.get(
                "ground_track_match_candidate_count",
                0,
            ),
            "ground_track_match_errors": dict(sorted(
                self._ground_track_match_errors.items()
            )),
            "ground_track_match_status": tracking.get(
                "ground_track_match_admission_status",
                "shadow_candidate_unadmitted",
            ),
            "tracking_active_tracks": tracking.get("active_tracks", 0),
            "tracking_tracked_tracks": tracking.get("tracked_tracks", 0),
            "tracking_motion_ready_tracks": tracking.get(
                "motion_ready_tracks",
                0,
            ),
            "tracking_region_count": tracking.get("region_states", 0),
            "tracking_restart_persistent": tracking.get(
                "restart_persistent",
                False,
            ),
            "tracking_shadow_arrival_candidates": tracking.get(
                "shadow_arrival_candidate_count",
                0,
            ),
            "tracking_shadow_future_arrivals": tracking.get(
                "shadow_arrival_future_count",
                0,
            ),
            "tracking_shadow_arrival_status": tracking.get(
                "shadow_arrival_admission_status",
                "shadow_candidate_unadmitted",
            ),
            "tracking_store_error": tracking.get("store_error"),
            "tracking_errors": dict(sorted(self._tracking_errors.items())),
            "cycle_status_counts": dict(self._last_cycle_counts),
            "snapshot_revision": publication.get("snapshot_revision", 0),
            "snapshot_transport_state": publication.get("transport_state"),
            "snapshot_rich_built": publication_counts.get(
                "rich_snapshots_built", 0
            ),
            "snapshot_rich_withheld": publication_counts.get(
                "rich_snapshots_withheld", 0
            ),
            "snapshot_rich_builder_failures": publication_counts.get(
                "rich_builder_failures", 0
            ),
            "snapshot_unknown_built": publication_counts.get(
                "unknown_snapshots_built", 0
            ),
            "meteorological_output": True,
            "dashboard_output": True,
            "thin_ha_output": True,
            "heavy_lane_active": raw.get("heavy_lane_active") is True,
            "ownership_fence_held": raw.get("ownership_fence_held") is True,
            "shutdown_complete": (
                self._state == "stopped"
                and raw.get("shutdown_complete") is True
            ),
            "last_tick_status": raw_last_tick.get("status"),
            "last_tick_reason_code": raw_last_tick.get("reason_code"),
            "last_tick_workload_id": raw_last_tick.get("workload_id"),
            "last_error_code": self._last_error_code,
            "counts": dict(self._counts),
        }


def build_worker_only_service(
    config: RawCaptureServiceConfig,
    *,
    expected_config_sha256: str,
    expected_authority_sha256: str,
    publisher: Callable[[bytes], Any],
    status_supplier: Callable[[], Mapping[str, Any]],
    clock: Callable[[], float] = time.time,
    **raw_kwargs: Any,
) -> WorkerOnlyServiceRuntime:
    """Acquire the raw worker authority and attach the output layer lazily."""

    raw = build_raw_capture_service(
        config,
        expected_config_sha256=expected_config_sha256,
        expected_authority_sha256=expected_authority_sha256,
        clock=clock,
        **raw_kwargs,
    )
    paths = config.worker_first.paths
    from .worker_only_radar_processing import decode_worker_only_radar_result
    from .worker_only_ground_processing import (
        decode_worker_only_ground_receipts,
    )
    from .worker_only_cell_tracking import WorkerOnlyCellTrackingRuntime

    postgis_settings = PostGISProjectionSettings.from_environment()

    return WorkerOnlyServiceRuntime(
        raw_runtime=raw,
        publisher=publisher,
        status_supplier=status_supplier,
        operational_fact_path=paths.operational_fact_db,
        operational_snapshot_path=paths.operational_snapshot_db,
        snapshot_outbox_path=paths.snapshot_outbox_db,
        radar_artifact_root=(
            paths.acquisition_completion_root
            / "worker_only_quantitative_artifacts"
        ),
        radar_evidence_path=(
            paths.acquisition_completion_root
            / "worker_only_quantitative_evidence"
        ),
        radar_handoff_root=paths.dwd_handoff_raw_root,
        radar_processor=decode_worker_only_radar_result,
        ground_evidence_path=(
            paths.acquisition_completion_root
            / "worker_only_ground_evidence_targeted_v1.sqlite3"
        ),
        ground_processor=decode_worker_only_ground_receipts,
        raw_calibration_path=(
            paths.acquisition_completion_root
            / "worker_only_raw_calibration_index.sqlite3"
        ),
        postgis_settings=postgis_settings,
        cell_tracker=WorkerOnlyCellTrackingRuntime(
            state_path=(
                paths.acquisition_completion_root
                / "worker_only_cell_tracking.sqlite3"
            ),
            clock=clock,
        ),
        clock=clock,
    )


__all__ = [
    "CONTRACT",
    "MODE",
    "SOURCE_COUNT",
    "WorkerOnlyServiceRuntime",
    "WorkerOnlyServiceRuntimeError",
    "build_worker_only_service",
]
