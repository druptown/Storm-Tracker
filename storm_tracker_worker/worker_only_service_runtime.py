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

from .desired_state import DesiredState
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
    MultiSourceRadarSnapshotError,
    PositiveRadarTargetObservation,
    RadarTargetObservation,
    ReflectivityEchoTargetObservation,
    WithheldDryRadarTargetObservation,
    WithheldUndetectRadarTargetObservation,
    build_positive_radar_fact_revision,
)
from .worker_only_motion_snapshot import (
    WorkerOnlyMotionSnapshotError,
    build_radar_motion_nowcast_revision,
)
from .openmeteo_operational_guidance import (
    SOURCE_ID as OPENMETEO_SOURCE_ID,
    OpenMeteoOperationalGuidanceError,
    OpenMeteoTargetGuidance,
    decode_openmeteo_guidance,
    enrich_revision_with_openmeteo_guidance,
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
CPU_DECODE_LANES = 2
BLITZORTUNG_SOURCE_ID = "lightning.blitzortung.event_ingress"
BLITZORTUNG_READINESS_REASONS = frozenset({
    "desired_state_unconfigured",
    "disabled_by_config",
    "event_ingress_fault_latched",
    "event_ingress_stopped",
    "no_available_targets",
    "regional_qos0_raw_capture_active",
    "regional_qos0_raw_capture_not_connected",
    "retention_authority_unavailable",
    "satellite_test_selected",
})

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
    "existing_raw_capture_accounting_busy",
    "existing_raw_capture_accounting_capacity",
    "existing_raw_capture_accounting_integrity",
    "existing_raw_capture_reservation_inventory_invalid",
    "existing_raw_capture_catalog_inventory_mismatch",
    "existing_raw_capture_reservation_identity_invalid",
    "existing_raw_capture_reservation_outside_recovery",
    "existing_raw_capture_duplicate_job_identity",
    "existing_raw_capture_job_alias_collision",
    "existing_raw_capture_missing_origin_reservation",
    "existing_raw_capture_reservation_binding_mismatch",
    "existing_raw_capture_object_size_conflict",
    "existing_raw_capture_multiple_bindings",
    "existing_raw_capture_duplicate_roles",
    "existing_raw_capture_transport_bound_exceeded",
    "existing_raw_capture_unaccounted_object",
    "existing_raw_capture_members_missing",
    "existing_raw_capture_member_receipt_missing",
    "existing_raw_capture_spans_bindings",
    "existing_raw_capture_identity_drift",
    "existing_raw_capture_handler_not_selected",
    "existing_raw_capture_provider_drift",
    "existing_raw_capture_product_drift",
    "existing_raw_capture_fetch_suffix_drift",
    "existing_raw_capture_bytes_unavailable",
    "existing_raw_capture_byte_inventory_drift",
    "existing_raw_capture_closed_binding_mismatch",
    "existing_raw_capture_origin_not_derivable",
    "existing_raw_capture_job_identity_drift",
    "existing_raw_capture_multiple_complete_captures",
    "existing_raw_capture_committed_incomplete",
    "existing_raw_capture_committed_missing",
    "existing_raw_capture_inventory_invalid",
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
    "raw_capture_start_timeout",
    "raw_capture_tick_failed",
    "raw_capture_tick_timeout",
    "worker_only_clock_invalid",
    "worker_only_fact_store_unavailable",
    "worker_only_fact_slot_recovery_failed",
    "worker_only_output_unavailable",
    "worker_only_snapshot_output_not_ready",
    "worker_only_status_invalid",
    "public_capture_contract_invalid",
    "public_response_contract_invalid",
    "public_response_identity_invalid",
    "public_response_provenance_invalid",
    "public_response_transport_invalid",
}) | frozenset(
    f"existing_raw_capture_fetch_suffix_drift_"
    f"{source.source_id.replace('.', '_')}_{shape}"
    for source in CATALOG_SOURCES
    for shape in ("capture_revision", "captured", "plain_capture", "other")
)


class WorkerOnlyServiceRuntimeError(RuntimeError):
    """The combined acquisition/output runtime cannot preserve its contract."""


def _bounded_counter(value: int) -> int:
    return min(MAX_COUNTER, max(0, int(value)))


def _safe_failure_reason(error: BaseException) -> str:
    """Return only a fixed internal reason code, never free exception text."""

    reason = str(error)
    return reason if reason in _SAFE_FAILURE_REASON_CODES else "unclassified"


def _meteorological_authority_equivalent(
    previous: DesiredState | None,
    current: DesiredState,
) -> bool:
    """Return whether retained target-bound science remains exactly reusable.

    A periodic Life360 lease refresh changes CAS metadata and observation
    timestamps, but not the coordinates/settings used for acquisition,
    sampling or fusion. Re-decoding every retained radar product for that
    control-plane-only change can take longer than the lease interval and keep
    the product permanently in ``desired_state_transition``.
    """

    if not isinstance(previous, DesiredState) or not isinstance(
        current, DesiredState
    ):
        return False
    if previous.settings != current.settings or not any(
        target.available
        and target.kind in {"device_tracker", "person", "test"}
        for target in current.targets
    ):
        return False

    def science_targets(state: DesiredState) -> tuple[tuple[Any, ...], ...]:
        return tuple(sorted((
            target.target_id,
            target.kind,
            target.available,
            target.latitude,
            target.longitude,
            target.observation_radius_km,
            target.country_code,
        ) for target in state.targets))

    return science_targets(previous) == science_targets(current)


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
        self._ready_source_ids: set[str] = set()
        self._capture_ids: dict[str, str] = {}
        self._capture_desired_state_sha256: dict[str, str] = {}
        self._radar_observations: dict[
            tuple[str, str], RadarTargetObservation
        ] = {}
        self._quantitative_desired_state_sha256: str | None = None
        self._quantitative_refresh_in_progress = False
        self._source_errors: dict[str, str] = {}
        self._quantitative_errors: dict[str, str] = {}
        self._model_guidance: dict[str, OpenMeteoTargetGuidance] = {}
        self._model_guidance_errors: dict[str, str] = {}
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
            "model_guidance_failures": 0,
            "model_guidance_skipped_stale": 0,
            "model_guidance_successes": 0,
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
            if self._postgis_projector is not None:
                self._postgis_projector.expedite_startup_retries()
            initialized["source_statistics"] = [
                dict(row) for row in index.source_statistics()
            ]
            self._ready_source_ids.update(
                row["source_id"]
                for row in initialized["source_statistics"]
                if isinstance(row.get("source_id"), str)
                and row.get("captures", 0) > 0
            )
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
        self._ready_source_ids.add(source_id)
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
        elif result.get("status") == "degraded":
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
        *,
        restage_current_authority: bool = False,
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
            if restage_current_authority and await self._stage_cycle():
                # The previous fact was bound to the authority that was live
                # before HA's coalesced target update.  An output-only tick can
                # publish a safe fallback, but it cannot turn that obsolete
                # fact into a rich current-authority snapshot.  Rebuild only
                # the bounded fact from already-retained captures and publish
                # it; no provider I/O is repeated here.
                output = await runtime.async_publish_ready_fact()
            else:
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
        return await self._settle_snapshot_runtime(
            self._output,
            output,
            restage_current_authority=True,
        )

    def _snapshot_provider_projection_missing(self) -> bool:
        """Return whether the durable head is only the safe fallback."""

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
        if not isinstance(items, list) or len(items) != SOURCE_COUNT:
            return True
        # The safe fallback now enumerates the full closed catalog so HA can
        # distinguish "not evaluated yet" from a missing entity.  It remains
        # a fallback and must still be replaced by the retained cycle view.
        return all(
            isinstance(item, Mapping)
            and item.get("state") == "unknown"
            and item.get("freshness") == "unknown"
            and item.get("reason_codes")
            == ["awaiting_meteorological_evidence"]
            for item in items
        )

    async def _restore_provider_projection_if_missing(self) -> bool:
        """Replace an authority-race fallback with the complete status view.

        Settling an MQTT publication while Life360 updates desired-state can
        legitimately publish an all-unknown fallback for the newest authority.
        That fallback now enumerates every provider as explicitly unknown.
        Rebuild those rows from the already retained capture/error state; this
        performs no provider I/O.
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

    def _provider_status_overrides(self) -> dict[str, tuple[str, str]]:
        """Project live push-ingress readiness without inventing weather data."""

        raw = self._raw.diagnostics()
        event_ingress = raw.get("event_ingress")
        blitzortung = (
            event_ingress.get("blitzortung")
            if isinstance(event_ingress, Mapping)
            else None
        )
        readiness = (
            blitzortung.get("readiness")
            if isinstance(blitzortung, Mapping)
            else None
        )
        if not isinstance(readiness, Mapping):
            return {}
        status = readiness.get("status")
        reason = readiness.get("reason_code")
        if reason not in BLITZORTUNG_READINESS_REASONS:
            return {}
        if status == "running_exact_raw" and readiness.get("ready") is True:
            state = "operational"
        elif status == "intentionally_inert":
            state = "unknown"
        elif status in {"starting", "waiting", "degraded"}:
            state = "degraded"
        else:
            state = "offline"
        return {BLITZORTUNG_SOURCE_ID: (state, reason)}

    def _provider_readiness_ids(
        self,
        provider_status_overrides: Mapping[str, tuple[str, str]],
    ) -> frozenset[str]:
        """Combine durable capture proof with current push-ingress proof."""

        ready = set(self._ready_source_ids)
        blitzortung = provider_status_overrides.get(BLITZORTUNG_SOURCE_ID)
        if blitzortung is not None and blitzortung[0] == "operational":
            ready.add(BLITZORTUNG_SOURCE_ID)
        return frozenset(ready)

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
        provider_status_overrides = self._provider_status_overrides()
        ready_source_ids = self._provider_readiness_ids(
            provider_status_overrides
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
        if observations:
            try:
                arrival_reader = (
                    None
                    if self._cell_tracker is None
                    else getattr(self._cell_tracker, "shadow_arrivals", None)
                )
                arrivals = () if not callable(arrival_reader) else arrival_reader()
                committed = (
                    build_radar_motion_nowcast_revision(
                        desired,
                        cycle,
                        observations,
                        arrivals,
                        revision=next_revision,
                        evaluated_at=now,
                        worker_state=worker_state,
                        queue_depth=queue_depth,
                        failed_jobs=failed_jobs,
                        provider_status_overrides=provider_status_overrides,
                        ready_source_ids=ready_source_ids,
                    )
                    if arrivals
                    else build_positive_radar_fact_revision(
                        desired,
                        cycle,
                        observations,
                        revision=next_revision,
                        evaluated_at=now,
                        worker_state=worker_state,
                        queue_depth=queue_depth,
                        failed_jobs=failed_jobs,
                        provider_status_overrides=provider_status_overrides,
                        ready_source_ids=ready_source_ids,
                    )
                )
            except (
                MultiSourceRadarSnapshotError,
                WorkerOnlyMotionSnapshotError,
            ):
                # Quantitative evidence is optional enrichment.  A source
                # becoming stale or authority-incompatible while a parallel
                # batch is being restaged must withhold that enrichment, not
                # terminate the complete acquisition worker.  The status-only
                # revision still carries the current provider truth and keeps
                # all precipitation claims unknown until a later verified
                # quantitative capture repopulates the observation set.
                affected = {
                    item.source_id for item in observations
                }
                self._radar_observations.clear()
                for source_id in affected:
                    self._quantitative_errors[source_id] = (
                        "quantitative_processing_failed"
                    )
                self._increment("quantitative_failures")
                _LOGGER.error(
                    "Worker-only radar fusion withheld one inconsistent "
                    "quantitative observation set"
                )
                committed = build_multi_source_status_fact_revision(
                    desired,
                    cycle,
                    revision=next_revision,
                    evaluated_at=now,
                    worker_state=worker_state,
                    queue_depth=queue_depth,
                    failed_jobs=failed_jobs,
                    provider_status_overrides=provider_status_overrides,
                    ready_source_ids=ready_source_ids,
                )
        else:
            committed = build_multi_source_status_fact_revision(
                desired,
                cycle,
                revision=next_revision,
                evaluated_at=now,
                worker_state=worker_state,
                queue_depth=queue_depth,
                failed_jobs=failed_jobs,
                provider_status_overrides=provider_status_overrides,
                ready_source_ids=ready_source_ids,
            )
        if self._model_guidance:
            try:
                committed = enrich_revision_with_openmeteo_guidance(
                    desired,
                    committed,
                    self._model_guidance.values(),
                )
            except OpenMeteoOperationalGuidanceError:
                self._model_guidance.clear()
                self._model_guidance_errors[OPENMETEO_SOURCE_ID] = (
                    "model_guidance_admission_failed"
                )
                self._increment("model_guidance_failures")
                _LOGGER.error(
                    "Worker-only Open-Meteo guidance admission withheld"
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

    async def _publish_authority_handoff_if_needed(self) -> bool:
        """Publish the new desired-state authority before provider I/O.

        Some production decoders perform bounded synchronous work and cannot
        yield to the maintenance poll while that work is in progress.  A
        Life360 lease renewal must therefore be rebound before the provider
        batch starts, otherwise Home Assistant correctly fails the previous
        authority closed until the whole batch finishes.
        """

        desired = self._raw.desired_state_store.load_active()
        if desired is None or self._snapshot_store is None:
            return False
        head = self._snapshot_store.head()
        if not isinstance(head, Mapping):
            return False
        snapshot = head.get("snapshot")
        if not isinstance(snapshot, Mapping):
            return False
        previous_sha256 = snapshot.get("desired_state_sha256")
        if previous_sha256 == desired.state_sha256:
            return False

        previous = (
            self._raw.desired_state_store.load_version(previous_sha256)
            if isinstance(previous_sha256, str)
            else None
        )
        if _meteorological_authority_equivalent(previous, desired):
            # Only the position observation time/CAS identity changed.  The
            # retained target-bound evidence is still exact for this geometry.
            self._quantitative_desired_state_sha256 = desired.state_sha256
        else:
            # A real geometry/settings change may not inherit old target-bound
            # radar/model output.  Publish a safe current-authority fallback;
            # the normal tick rebuilds evidence afterwards.
            self._radar_observations.clear()
            self._model_guidance.clear()

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

    def _latest_tick_results(self) -> tuple[Any, ...]:
        """Read the complete batch with a legacy single-result fallback."""

        results = getattr(self._raw, "latest_tick_results", None)
        if results is None:
            return (self._raw.latest_tick_result,)
        if not isinstance(results, tuple) or not results:
            raise WorkerOnlyServiceRuntimeError(
                "worker_only_raw_batch_result_invalid"
            )
        return results

    def _consume_terminal_result(self, result: Any) -> bool:
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
                self._ready_source_ids.add(source_id)
                self._capture_ids[source_id] = result.source_capture_id
                if isinstance(result.desired_state_sha256, str):
                    self._capture_desired_state_sha256[source_id] = (
                        result.desired_state_sha256
                    )
                self._index_raw_calibration_capture(source_id, receipt)
                self._source_errors.pop(source_id, None)
        else:
            self._source_errors[source_id] = result.reason_code
        self._increment("terminal_updates")
        return True

    def _consume_terminal_results(self) -> tuple[Any, ...]:
        changed = []
        for result in self._latest_tick_results():
            if self._consume_terminal_result(result):
                changed.append(result)
        return tuple(changed)

    def _raw_batch_waited_for_desired_state(self) -> bool:
        """Return whether this batch raced ahead of initial HA authority."""

        results = self._latest_tick_results()
        return (
            all(result.workload_id is None for result in results)
            and any(
                result.reason_code == "desired_state_unconfigured"
                for result in results
            )
        )

    def _consume_latest_terminal(self) -> bool:
        """Compatibility wrapper for callers that only need a change flag."""

        return bool(self._consume_terminal_results())

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
            self._ready_source_ids.add(workload_id)
            self._capture_ids[workload_id] = capture.capture_id
            self._index_raw_calibration_capture(workload_id, capture)
            self._source_errors.pop(workload_id, None)
            seen.add(workload_id)
            self._increment("restored_captures")

        terminal_reader = getattr(self._raw, "latest_terminal_results", None)
        if not callable(terminal_reader):
            return
        terminal_results = terminal_reader()
        if not isinstance(terminal_results, tuple):
            raise WorkerOnlyServiceRuntimeError(
                "worker_only_terminal_result_inventory_invalid"
            )
        for item in terminal_results:
            if (
                not isinstance(item, tuple)
                or len(item) != 3
                or not all(isinstance(value, str) for value in item)
            ):
                raise WorkerOnlyServiceRuntimeError(
                    "worker_only_terminal_result_inventory_invalid"
                )
            workload_id, status, reason_code = item
            if workload_id in seen or status in SUCCESS_STATUSES:
                continue
            self._source_errors[workload_id] = reason_code

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

    async def _process_ground_results(self, results: tuple[Any, ...]) -> None:
        for result in results:
            if (
                result.status in SUCCESS_STATUSES
                and result.source_capture_id is not None
                and result.workload_id is not None
            ):
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
                        ReflectivityEchoTargetObservation,
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
                    "opera_reflectivity_decode_withheld",
                    "capture_receipt_unavailable",
                }
                else "quantitative_processing_failed"
            )
            self._increment("quantitative_failures")

    async def _process_model_guidance_capture(
        self,
        *,
        capture_id: str,
        desired,
    ) -> None:
        """Decode one exact Open-Meteo capture for the same desired state."""

        if not desired.settings.model_guidance_enabled:
            self._model_guidance.clear()
            self._model_guidance_errors.pop(OPENMETEO_SOURCE_ID, None)
            return
        if (
            self._capture_desired_state_sha256.get(OPENMETEO_SOURCE_ID)
            != desired.state_sha256
        ):
            self._model_guidance.clear()
            return
        if not self._quantitative_capture_is_current(OPENMETEO_SOURCE_ID):
            self._model_guidance.clear()
            self._model_guidance_errors.pop(OPENMETEO_SOURCE_ID, None)
            self._increment("model_guidance_skipped_stale")
            return
        try:
            values = await asyncio.to_thread(
                decode_openmeteo_guidance,
                raw_store=self._raw.raw_artifact_store,
                capture_id=capture_id,
                desired_state=desired,
            )
            self._model_guidance = {
                item.target_id: item for item in values
            }
            self._model_guidance_errors.pop(OPENMETEO_SOURCE_ID, None)
            self._increment("model_guidance_successes")
        except Exception as error:
            self._model_guidance.clear()
            self._model_guidance_errors[OPENMETEO_SOURCE_ID] = (
                "model_guidance_decode_failed"
            )
            self._increment("model_guidance_failures")
            _LOGGER.error(
                "Worker-only Open-Meteo guidance decode fail-closed: "
                "class=%s reason=%s",
                type(error).__name__,
                _safe_failure_reason(error),
            )

    async def _refresh_model_guidance_for_desired(self, desired) -> None:
        self._model_guidance.clear()
        capture_id = self._capture_ids.get(OPENMETEO_SOURCE_ID)
        if capture_id is not None:
            await self._process_model_guidance_capture(
                capture_id=capture_id,
                desired=desired,
            )

    async def _process_model_guidance_results(
        self,
        results: tuple[Any, ...],
        desired,
    ) -> None:
        for result in results:
            if (
                result.status in SUCCESS_STATUSES
                and result.workload_id == OPENMETEO_SOURCE_ID
                and result.source_capture_id is not None
                and result.desired_state_sha256 == desired.state_sha256
            ):
                await self._process_model_guidance_capture(
                    capture_id=result.source_capture_id,
                    desired=desired,
                )

    async def _refresh_quantitative_for_desired(self, desired) -> None:
        """Re-sample retained raw captures after any target-location change."""

        self._radar_observations.clear()
        await self._process_quantitative_jobs(
            tuple(sorted(self._capture_ids.items())),
            desired,
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

    async def _process_quantitative_results(
        self,
        results: tuple[Any, ...],
        desired,
    ) -> None:
        latest_by_source = {}
        for result in results:
            if (
                result.status in SUCCESS_STATUSES
                and result.source_capture_id is not None
                and result.workload_id is not None
            ):
                # A recovery batch may contain more than one terminal receipt
                # for the same provider.  Decode only its newest result so one
                # provider can never occupy two CPU lanes simultaneously.
                latest_by_source[result.workload_id] = result.source_capture_id
        await self._process_quantitative_jobs(
            tuple(sorted(latest_by_source.items())),
            desired,
        )

    async def _process_quantitative_jobs(
        self,
        jobs: tuple[tuple[str, str], ...],
        desired,
    ) -> None:
        """Decode unique sources concurrently within the two-thread budget."""

        semaphore = asyncio.Semaphore(CPU_DECODE_LANES)

        async def process(workload_id: str, capture_id: str) -> None:
            async with semaphore:
                await self._process_quantitative_capture(
                    workload_id=workload_id,
                    capture_id=capture_id,
                    desired=desired,
                )

        await asyncio.gather(*(
            process(workload_id, capture_id)
            for workload_id, capture_id in jobs
        ))

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
            stage = "authority_handoff"
            try:
                maintenance_snapshot_published = (
                    await self._publish_authority_handoff_if_needed()
                )
                stage = "raw_tick"
                maintenance_snapshot_published = (
                    await self._raw_tick_with_snapshot_maintenance()
                    or maintenance_snapshot_published
                )
                stage = "consume_terminal"
                terminal_results = self._consume_terminal_results()
                changed = bool(terminal_results)
                desired = self._raw.desired_state_store.load_active()
                if (
                    desired is not None
                    and self._raw_batch_waited_for_desired_state()
                ):
                    # MQTT admission opens before HA can publish its first
                    # desired-state document.  When that document arrives
                    # during the first provider tick, fetch the now-due raw
                    # sources before starting minutes of retained-evidence
                    # reprojection.  Otherwise every live provider is delayed
                    # by the startup science replay despite authority already
                    # being available.
                    stage = "raw_tick_after_authority"
                    maintenance_snapshot_published = (
                        await self._raw_tick_with_snapshot_maintenance()
                        or maintenance_snapshot_published
                    )
                    stage = "consume_terminal_after_authority"
                    authority_results = self._consume_terminal_results()
                    terminal_results = terminal_results + authority_results
                    changed = bool(terminal_results)
                    desired = self._raw.desired_state_store.load_active()
                if changed and desired is not None:
                    stage = "ground_processing"
                    _result, ground_snapshot_published = (
                        await self._await_with_snapshot_maintenance(
                            self._process_ground_results(terminal_results)
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
                if (
                    quantitative_authority_changed
                    and self._quantitative_desired_state_sha256 is not None
                ):
                    previous_quantitative_desired = (
                        self._raw.desired_state_store.load_version(
                            self._quantitative_desired_state_sha256
                        )
                    )
                    if _meteorological_authority_equivalent(
                        previous_quantitative_desired,
                        desired,
                    ):
                        # Rebind retained radar/model/ground facts to the new
                        # CAS head in the normal snapshot stage. Their exact
                        # sampling geometry is unchanged, so a heavy replay
                        # would add no meteorological information.
                        self._quantitative_desired_state_sha256 = (
                            desired.state_sha256
                        )
                        quantitative_authority_changed = False
                authority_snapshot_published = (
                    maintenance_snapshot_published
                    and quantitative_authority_changed
                )
                ground_refresh_completed = False
                if (
                    quantitative_authority_changed
                    and self._snapshot_requires_restaging()
                ):
                    # A target/location change invalidates HA's accepted
                    # snapshot immediately. Publish the current-authority
                    # fallback before replaying retained ground or radar
                    # evidence: both reprocessing stages can take minutes on
                    # a large calibration corpus and may never keep the UI in
                    # a desired_state_transition during that work.
                    self._radar_observations.clear()
                    self._model_guidance.clear()
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
                if (
                    quantitative_authority_changed
                    and self._ground_processor is not None
                    and self._ground_evidence_path is not None
                ):
                    # Startup deliberately opens MQTT admission before doing
                    # science replay.  The first normal tick must therefore
                    # restore every retained canonical-ground capture; without
                    # this call restart-persistent raw KMI/Netatmo/METAR data
                    # remains visible but never reaches the ground slice.
                    stage = "ground_refresh"
                    _result, ground_snapshot_published = (
                        await self._await_with_snapshot_maintenance(
                            self._refresh_ground_captures()
                        )
                    )
                    maintenance_snapshot_published = (
                        ground_snapshot_published
                        or maintenance_snapshot_published
                    )
                    ground_refresh_completed = True
                if quantitative_authority_changed:
                    stage = "model_guidance_refresh"
                    _result, model_snapshot_published = (
                        await self._await_with_snapshot_maintenance(
                            self._refresh_model_guidance_for_desired(desired)
                        )
                    )
                    maintenance_snapshot_published = (
                        model_snapshot_published
                        or maintenance_snapshot_published
                    )
                elif changed and desired is not None:
                    stage = "model_guidance_processing"
                    _result, model_snapshot_published = (
                        await self._await_with_snapshot_maintenance(
                            self._process_model_guidance_results(
                                terminal_results,
                                desired,
                            )
                        )
                    )
                    maintenance_snapshot_published = (
                        model_snapshot_published
                        or maintenance_snapshot_published
                    )
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
                            self._process_quantitative_results(
                                terminal_results,
                                desired,
                            )
                        )
                    )
                    maintenance_snapshot_published = (
                        quantitative_snapshot_published
                        or maintenance_snapshot_published
                    )
                assert self._output is not None
                stage = "snapshot_stage"
                restage = (
                    ground_refresh_completed
                    or (changed and not authority_snapshot_published)
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
        acquisition_terminal_results = [
            {
                "workload_id": workload_id,
                "status": status,
                "reason_code": reason_code,
            }
            for workload_id, status, reason_code
            in self._raw.latest_terminal_results()
        ]
        raw_last_tick = raw.get("last_tick", {})
        if not isinstance(raw_last_tick, Mapping):
            raw_last_tick = {}
        parallel_acquisition = raw.get("parallel_acquisition", {
            "cooldown_remaining_seconds": 0.0,
            "fallback_lanes": 1,
            "lane_limit": 1,
            "last_batch_duration_seconds": 0.0,
            "last_batch_job_count": 0,
            "last_batch_mean_runtime_seconds": 0.0,
            "last_unhealthy_reasons": [],
            "maximum_lanes": 1,
            "mode": "serial_compatibility",
            "concurrency_scope": "workload",
        })
        if not isinstance(parallel_acquisition, Mapping):
            parallel_acquisition = {}
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
            # Keep one bounded, payload-free terminal outcome per workload in
            # the worker heartbeat.  A stale provider sensor alone cannot
            # distinguish an upstream failure from scheduler starvation; this
            # projection makes that distinction observable without exposing
            # coordinates, request URLs, credentials, or raw payloads.
            "acquisition_terminal_results": acquisition_terminal_results,
            "parallel_acquisition": dict(parallel_acquisition),
            "quantitative_capture_count": len(
                set(self._capture_ids).intersection(
                    {
                        "radar.dwd.radolan_rv",
                        "radar.knmi.rtcor",
                        "radar.metoffice.uk_rate",
                        "radar.opera.dbzh",
                        "satellite.noaa.err",
                    }
                )
            ),
            "quantitative_errors": dict(sorted(self._quantitative_errors.items())),
            "model_guidance_target_count": len(self._model_guidance),
            "model_guidance_errors": dict(sorted(
                self._model_guidance_errors.items()
            )),
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
