"""Dynamische runtime-regio's voor Storm Tracker V3.

Een regio is geen land en de observatiehorizon is niet de sharing distance.
De manager bezit de StormEngine/OFE-combinaties; providercontrollers kunnen via
de lifecycle callbacks per runtime-regio starten en stoppen.
"""
from __future__ import annotations

import inspect
import logging
import math
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

from .observation_fusion_engine import (
    ObservationBatch,
    ObservationFusionEngine,
    TransientInfrastructureError,
)
from .storm_engine import StormEngine

_LOGGER = logging.getLogger(__name__)

DEFAULT_ENGINE_SHARING_DISTANCE_KM = 150.0
DEFAULT_OBSERVATION_RADIUS_KM = 350.0


class WorkerTrackingRequiredError(TransientInfrastructureError):
    """Live tracking failed closed instead of consuming HA CPU locally."""

    durable_no_fallback = True


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    value = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return radius * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))


@dataclass
class RegionEngine:
    """Eén actieve meteorologische runtime-context."""

    engine_id: str
    center_lat: float
    center_lon: float
    observation_radius_km: float
    storm_engine: StormEngine
    ofe: ObservationFusionEngine
    projection_targets: set[str] = field(default_factory=set)
    target_locations: dict[str, tuple[float, float]] = field(default_factory=dict)
    runtime: object | None = None
    durable_region_id: str | None = None
    recovery_ready: bool = True
    compute_schedule_revision: int = 0
    compute_schedule_snapshot: dict = field(default_factory=dict)

    @property
    def storage_key(self) -> str:
        """Stabiele geografische sleutel voor restart-herstel."""
        if self.durable_region_id:
            return self.durable_region_id
        return f"{self.center_lat:.2f},{self.center_lon:.2f},{self.observation_radius_km:.0f}"

    def accepts_observation(self, lat: float, lon: float) -> bool:
        return _haversine(self.center_lat, self.center_lon, lat, lon) <= self.observation_radius_km

    def refresh_compute_schedule(self) -> dict:
        """Publiceer na state/targetwijziging een volledig uitvoeringsplan.

        Scheduling mag nooit trackingstate of forecastinput verwijderen. Een
        interne planningsfout valt daarom zichtbaar terug op een
        ongeprioriteerde execute-all-lijst in plaats van het reeds
        gecommitteerde observationbatch te laten mislukken.
        """

        next_revision = self.compute_schedule_revision + 1
        tracked_ids = tuple(sorted(
            str(storm.storm_id)
            for storm in self.storm_engine.get_storms()
        ))
        try:
            plan = self.storm_engine.build_compute_schedule(
                dict(self.target_locations),
                relevance_radius_km=self.observation_radius_km,
            )
            schedule = plan.get("schedule")
            scheduled_ids = (
                tuple(sorted(
                    str(entry["storm_id"])
                    for entry in schedule
                ))
                if isinstance(schedule, list)
                else ()
            )
            invariants = plan.get("invariants")
            if (
                scheduled_ids != tracked_ids
                or not isinstance(invariants, dict)
                or invariants.get("all_tracks_scheduled") is not True
                or invariants.get(
                    "all_tracks_retained_for_forecast_input"
                ) is not True
            ):
                raise RuntimeError(
                    "computeplan schendt lossless schedulingcontract"
                )
            snapshot = {
                **plan,
                "runtime_schema_version": 1,
                "state": "ready",
                "region_engine_id": self.engine_id,
                "tracking_revision": self.storm_engine.revision,
                "compute_schedule_revision": next_revision,
                "relevance_radius_source": (
                    "region_observation_radius_km"
                ),
                "fallback_execution_required": False,
                "error_category": None,
            }
        except Exception as err:
            fallback_schedule = [
                {
                    "storm_id": storm_id,
                    "tier": "unprioritized",
                    "rank": index + 1,
                    "preferred_pass": index,
                    "preferred_foreground": False,
                    "tracking_state_retained": True,
                    "history_retained": True,
                    "export_retained": True,
                    "forecast_input_retained": True,
                    "execution_required": True,
                }
                for index, storm_id in enumerate(tracked_ids)
            ]
            snapshot = {
                "schema_version": 1,
                "runtime_schema_version": 1,
                "state": "fallback_all_tracks",
                "region_engine_id": self.engine_id,
                "tracking_revision": self.storm_engine.revision,
                "compute_schedule_revision": next_revision,
                "target_count": len(self.target_locations),
                "relevance_radius_km": self.observation_radius_km,
                "relevance_radius_source": (
                    "region_observation_radius_km"
                ),
                "tracked_count": len(tracked_ids),
                "scheduled_track_count": len(tracked_ids),
                "forecast_input_count": len(tracked_ids),
                "preferred_foreground_count": 0,
                "deferred_advisory_count": len(tracked_ids),
                "tier_counts": {
                    "relevant": 0,
                    "background": 0,
                    "low_confidence": 0,
                    "unprioritized": len(tracked_ids),
                },
                "schedule": fallback_schedule,
                "invariants": {
                    "capacity_preserves_all_observations": True,
                    "tiering_is_non_evicting": True,
                    "all_tracks_scheduled": True,
                    "all_tracks_retained_for_forecast_input": True,
                    "schedule_ids_unique": (
                        len(set(tracked_ids)) == len(tracked_ids)
                    ),
                    "priority_plan_valid": False,
                },
                "fallback_execution_required": True,
                "fallback_policy": "execute_every_track_without_priority",
                "error_category": type(err).__name__,
                "limitations": {
                    "meteorological_priority_claim": False,
                    "wall_clock_benchmark": False,
                    "nuc_thermal_claim": False,
                },
            }
            _LOGGER.exception(
                "Computeprioriteit voor %s faalde; alle %d tracks blijven "
                "ongeprioriteerd uitvoerbaar",
                self.engine_id,
                len(tracked_ids),
            )
        self.compute_schedule_revision = next_revision
        self.compute_schedule_snapshot = snapshot
        return snapshot

    def compute_schedule_diagnostics(
        self, *, preview_limit: int = 25
    ) -> dict:
        """Lees een begrensde snapshot zonder nieuwe berekening of side effect."""

        if (
            isinstance(preview_limit, bool)
            or not isinstance(preview_limit, int)
            or preview_limit < 0
        ):
            raise ValueError("preview_limit moet een niet-negatieve integer zijn")
        snapshot = self.compute_schedule_snapshot
        if not snapshot:
            return {
                "state": "not_computed",
                "region_engine_id": self.engine_id,
                "tracking_revision": self.storm_engine.revision,
                "compute_schedule_revision": 0,
                "schedule_entries_total": 0,
                "schedule_entries_preview": [],
                "schedule_entries_overflow": 0,
            }
        schedule = list(snapshot.get("schedule") or ())
        return {
            key: snapshot.get(key)
            for key in (
                "runtime_schema_version",
                "state",
                "region_engine_id",
                "tracking_revision",
                "compute_schedule_revision",
                "policy_fingerprint",
                "schedule_fingerprint",
                "target_count",
                "relevance_radius_km",
                "relevance_radius_source",
                "tracked_count",
                "scheduled_track_count",
                "forecast_input_count",
                "foreground_slots",
                "preferred_foreground_count",
                "deferred_advisory_count",
                "preferred_pass_count",
                "tier_counts",
                "invariants",
                "fallback_execution_required",
                "fallback_policy",
                "error_category",
                "limitations",
            )
        } | {
            "schedule_entries_total": len(schedule),
            "schedule_entries_preview": schedule[:preview_limit],
            "schedule_entries_overflow": max(
                0, len(schedule) - preview_limit
            ),
        }


LifecycleCallback = Callable[[RegionEngine], object | Awaitable[object]]
ObservationBatchCallback = Callable[
    [RegionEngine, list, list[dict], int],
    object | Awaitable[object],
]


class StormManager:
    """Single source of truth voor alle actieve RegionEngines en targets."""

    def __init__(
        self,
        hass=None,
        *,
        sharing_distance_km: float = DEFAULT_ENGINE_SHARING_DISTANCE_KM,
        observation_radius_km: float = DEFAULT_OBSERVATION_RADIUS_KM,
        on_engine_created: Optional[LifecycleCallback] = None,
        on_engine_removed: Optional[LifecycleCallback] = None,
        on_observation_batch: Optional[ObservationBatchCallback] = None,
        allow_local_processing: bool = False,
    ) -> None:
        self._hass = hass
        self.sharing_distance_km = sharing_distance_km
        self.observation_radius_km = observation_radius_km
        self._on_engine_created = on_engine_created
        self._on_engine_removed = on_engine_removed
        self._on_observation_batch = on_observation_batch
        # Local StormEngine execution is an explicit composition decision.
        # Neither a missing hass object nor a failed worker may enable it.
        if not isinstance(allow_local_processing, bool):
            raise TypeError("allow_local_processing moet een bool zijn")
        self._allow_local_processing = allow_local_processing
        self._engines: dict[str, RegionEngine] = {}
        self._target_engine: dict[str, str] = {}
        self._next_engine_number = 1
        self._admission_open = True
        self._scheduled_tasks: set = set()
        self._durable_recovery_required = False
        self._durable_integrity_verified = True
        self._durable_recovery_complete = True

    def begin_durable_recovery(self) -> None:
        """Sluit globale en regionale admission tijdens duurzaam herstel."""
        self._durable_recovery_required = True
        self._durable_integrity_verified = False
        self._durable_recovery_complete = False
        for engine in self._engines.values():
            engine.recovery_ready = False

    def verify_durable_integrity(
        self, *, integrity_healthy: bool
    ) -> None:
        """Leg een geslaagde globale journalcontrole vast of faal gesloten."""
        if not self._durable_recovery_required:
            raise RuntimeError("duurzaam herstel is niet gestart")
        if not integrity_healthy:
            raise RuntimeError(
                "durable processing journal faalt integrity_check"
            )
        self._durable_integrity_verified = True

    def complete_durable_recovery(self) -> None:
        """Open globaal pas na integriteit en herstel van iedere regio."""
        if not self._durable_recovery_required:
            raise RuntimeError("duurzaam herstel is niet gestart")
        if not self._durable_integrity_verified:
            raise RuntimeError(
                "durable processing journal is niet geverifieerd"
            )
        pending = sorted(
            engine.engine_id
            for engine in self._engines.values()
            if not engine.recovery_ready
        )
        if pending:
            raise RuntimeError(
                "duurzaam regioherstel is nog niet voltooid: "
                + ", ".join(pending)
            )
        self._durable_recovery_complete = True

    @property
    def admission_ready(self) -> bool:
        """Of nieuwe input door zowel shutdown- als recoverypoort mag."""
        return (
            self._admission_open
            and (
                not self._durable_recovery_required
                or self._durable_recovery_complete
            )
        )

    def assign_target(self, target_id: str, lat: float, lon: float) -> RegionEngine:
        """Koppel een target aan de dichtstbijzijnde deelbare runtime-regio."""
        old = self.get_engine_for_target(target_id)
        if old and _haversine(old.center_lat, old.center_lon, lat, lon) <= self.sharing_distance_km:
            old.projection_targets.add(target_id)
            old.target_locations[target_id] = (lat, lon)
            old.refresh_compute_schedule()
            return old

        candidate = self._find_shareable(lat, lon)
        if old is not None and old is not candidate:
            self.release(target_id)
        if candidate is None:
            candidate = self._create_engine(lat, lon)
        candidate.projection_targets.add(target_id)
        candidate.target_locations[target_id] = (lat, lon)
        self._target_engine[target_id] = candidate.engine_id
        candidate.refresh_compute_schedule()
        return candidate

    def release(self, target_id: str) -> Optional[RegionEngine]:
        engine_id = self._target_engine.pop(target_id, None)
        engine = self._engines.get(engine_id) if engine_id else None
        if engine is None:
            return None
        engine.projection_targets.discard(target_id)
        engine.target_locations.pop(target_id, None)
        if not engine.projection_targets:
            self._engines.pop(engine.engine_id, None)
            self._invoke(self._on_engine_removed, engine)
            _LOGGER.info("RegionEngine %s verwijderd: geen targets", engine.engine_id)
        else:
            engine.refresh_compute_schedule()
        return engine

    def get_engine_for_target(self, target_id: str) -> Optional[RegionEngine]:
        return self._engines.get(self._target_engine.get(target_id, ""))

    def get_all_engines(self) -> list[RegionEngine]:
        return list(self._engines.values())

    def route_observation(self, observation) -> int:
        """Routeer globale pushdata éénmaal naar elke relevante engine."""
        if not self.admission_ready:
            return 0
        routed = 0
        for engine in self._engines.values():
            if (
                engine.recovery_ready
                and engine.accepts_observation(
                    observation.lat, observation.lon
                )
            ):
                self._schedule(engine.ofe.add_observation(observation))
                routed += 1
        return routed

    def route_observation_to_engine(self, engine_id: str, observation) -> bool:
        """Routeer pull-radardata uitsluitend naar de geselecteerde engine."""
        if not self.admission_ready:
            return False
        engine = self._engines.get(engine_id)
        if (
            engine is None
            or not engine.recovery_ready
            or not engine.accepts_observation(
                observation.lat, observation.lon
            )
        ):
            return False
        self._schedule(engine.ofe.add_observation(observation))
        return True

    async def async_route_observation(self, observation) -> int:
        """Routeer en wacht tot de observation duurzaam geaccepteerd is."""
        return await self.async_route_observations([observation])

    async def async_route_observations(self, observations) -> int:
        """Bulkroute met één OFE-/DB-transactie per relevante regio."""
        if not self.admission_ready:
            return 0
        observations = list(observations)
        routed = 0
        for engine in self._engines.values():
            if not engine.recovery_ready:
                continue
            selected = [
                observation
                for observation in observations
                if engine.accepts_observation(
                    observation.lat, observation.lon
                )
            ]
            if not selected:
                continue
            await engine.ofe.add_observations(selected)
            routed += len(selected)
        return routed

    async def async_route_observations_to_engine(
        self,
        engine_id: str,
        observations,
    ) -> int:
        """Bulkroute naar één expliciete regio en wacht de receipt af."""
        if not self.admission_ready:
            return 0
        engine = self._engines.get(engine_id)
        if engine is None or not engine.recovery_ready:
            return 0
        selected = [
            observation
            for observation in observations
            if engine.accepts_observation(
                observation.lat, observation.lon
            )
        ]
        if not selected:
            return 0
        await engine.ofe.add_observations(selected)
        return len(selected)

    def close_admission(self) -> None:
        """Weiger nieuwe routecalls vóór shutdown/drain."""
        self._admission_open = False

    async def async_drain_ingest(self) -> None:
        """Wacht alle eerder geschedulede push-/lifecyclecalls uit."""
        import asyncio

        current = asyncio.current_task()
        while True:
            pending = [
                task
                for task in self._scheduled_tasks
                if task is not current and not task.done()
            ]
            if not pending:
                return
            await asyncio.gather(*pending, return_exceptions=True)

    def _find_shareable(self, lat: float, lon: float) -> Optional[RegionEngine]:
        candidates = [
            engine for engine in self._engines.values()
            if _haversine(engine.center_lat, engine.center_lon, lat, lon)
            <= self.sharing_distance_km
        ]
        return min(
            candidates,
            key=lambda engine: _haversine(engine.center_lat, engine.center_lon, lat, lon),
            default=None,
        )

    def _create_engine(self, lat: float, lon: float) -> RegionEngine:
        engine_id = f"region-{self._next_engine_number}"
        self._next_engine_number += 1
        storm_engine = StormEngine()

        async def on_batch(observations: list) -> None:
            source_batch = (
                observations
                if isinstance(observations, ObservationBatch)
                else ObservationBatch(observations)
            )
            scoped = source_batch.subset([
                observation for observation in observations
                if _haversine(lat, lon, observation.lat, observation.lon)
                <= self.observation_radius_km
            ])
            start_revision = storm_engine.revision
            start_state = storm_engine.export_tracking_state()
            if self._on_observation_batch is not None:
                try:
                    result = self._on_observation_batch(
                        engine, scoped, start_state, start_revision
                    )
                    if inspect.isawaitable(result):
                        result = await result
                except Exception as err:
                    if storm_engine.has_applied_batch(source_batch.batch_id):
                        source_batch.acknowledge()
                        engine.refresh_compute_schedule()
                        return
                    if not self._allow_local_processing:
                        if isinstance(err, WorkerTrackingRequiredError):
                            raise
                        raise WorkerTrackingRequiredError(
                            "workertracking is tijdelijk niet beschikbaar; "
                            "rootbatch blijft intact wachten"
                        ) from err
                    if getattr(err, "durable_no_fallback", False):
                        raise
                    _LOGGER.exception(
                        "Workerpad faalde voor batch %s; lokale "
                        "idempotente fallback",
                        source_batch.batch_id,
                    )
                    result = False
                if result is True:
                    source_batch.acknowledge()
                    engine.refresh_compute_schedule()
                    return
                if storm_engine.has_applied_batch(source_batch.batch_id):
                    source_batch.acknowledge()
                    engine.refresh_compute_schedule()
                    return
                if not self._allow_local_processing:
                    raise WorkerTrackingRequiredError(
                        "workertracking leverde geen commitbewijs; batch blijft "
                        "duurzaam onbevestigd"
                    )
            elif not self._allow_local_processing:
                raise WorkerTrackingRequiredError(
                    "live RegionEngine heeft geen workertrackingcallback"
                )
            try:
                await storm_engine.process_batch(
                    scoped,
                    expected_revision=storm_engine.revision,
                    batch_id=source_batch.batch_id,
                )
            except Exception:
                if storm_engine.has_applied_batch(source_batch.batch_id):
                    source_batch.acknowledge()
                raise
            source_batch.acknowledge()
            engine.refresh_compute_schedule()

        engine = RegionEngine(
            engine_id=engine_id,
            center_lat=lat,
            center_lon=lon,
            observation_radius_km=self.observation_radius_km,
            storm_engine=storm_engine,
            ofe=ObservationFusionEngine(on_batch=on_batch),
            recovery_ready=not self._durable_recovery_required,
        )
        self._engines[engine_id] = engine
        self._invoke(self._on_engine_created, engine)
        _LOGGER.info(
            "RegionEngine %s aangemaakt op %.4f,%.4f (sharing %.0f km; observatie %.0f km)",
            engine_id, lat, lon, self.sharing_distance_km, self.observation_radius_km,
        )
        return engine

    def _invoke(self, callback: Optional[LifecycleCallback], engine: RegionEngine) -> None:
        if callback is None:
            return
        result = callback(engine)
        if inspect.isawaitable(result):
            self._schedule(result)

    def _schedule(self, awaitable) -> None:
        if self._hass is not None:
            task = self._hass.async_create_task(awaitable)
        else:
            import asyncio
            task = asyncio.get_running_loop().create_task(awaitable)
        self._scheduled_tasks.add(task)
        task.add_done_callback(self._scheduled_tasks.discard)
