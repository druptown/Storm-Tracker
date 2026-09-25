"""Small operational pipeline: select, fetch, fuse, nowcast and persist."""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
import math
import time
from types import MappingProxyType
from typing import Any, Callable, Mapping

from pyproj import Transformer

from .desired_state import METHOD, DesiredState, DesiredStateConflict, parse_desired_state
from .thin_fusion import (
    MAX_FUSION_AGE_SECONDS,
    PROVIDER_FUSION_AGE_SECONDS,
    FusedTargetObservation,
    FusionCycle,
    fuse_observations,
)
from .thin_ground_validation import ForecastValidation, GroundValidationRuntime
from .thin_nowcast import StormCell, ThinNowcast, ThinNowcastRuntime
from .thin_observation import ObservationKind
from .thin_radar_tracking import ThinRadarTracker
from .thin_radar_overlay import build_radar_overlay
from .thin_regional_radar_fusion import RegionalFusionSummary
from .thin_regional_radar_runtime import ThinRegionalRadarFusionCoordinator
from .thin_postgis import PostGISWriteResult
from .thin_provider_catalog import PROVIDERS, build_due_requests
from .thin_region_planner import ThinRegionPlan, ThinRegionPlanner
from .tracking_core.operational_snapshot import MAX_UI_STORM_SUMMARIES, validate_snapshot


SNAPSHOT_TTL_SECONDS = 120.0
MAX_GEOJSON_PAYLOAD_BYTES = 900_000
MAX_TOTAL_RADAR_OVERLAY_RUNS = 2_000
SNAPSHOT_REVISION_EPOCH_SECONDS = 1_577_836_800  # 2020-01-01T00:00:00Z
SNAPSHOT_PROVIDER_IDS = {
    "kmi_aws": "ground.kmi.aws_10min",
    "kmi_appv4": "radar.kmi.appv4_visual",
    "kmi_radqpe": "radar.kmi.radqpe_native",
    "opera_radar": "radar.opera.dbzh",
    "knmi_radar": "radar.knmi.rtcor",
    "dwd_radolan": "radar.dwd.radolan_rv",
    "open_meteo": "model.openmeteo.region",
    "metar": "ground.metar.global",
    "netatmo": "ground.netatmo.region",
    "wunderground": "ground.wunderground.station",
    "blitzortung": "lightning.blitzortung.event_ingress",
    "eumetsat_li": "lightning.eumetsat.li",
    "goes18_glm": "lightning.goes18.glm",
    "goes19_glm": "lightning.goes19.glm",
    "goes18_rrqpe": "satellite.goes18.rrqpe",
    "goes19_rrqpe": "satellite.goes19.rrqpe",
    "noaa_err": "satellite.noaa.err",
    "hsaf_h40b": "satellite.hsaf.h40b",
    "rainviewer": "radar.rainviewer.visual",
    "aemet_radar": "radar.aemet.composite",
    "meteofrance_radar": "radar.meteofrance.dpradar",
    "dpc_radar": "radar.dpc.sri",
    "italiameteo_icon2i": "model.openmeteo.italiameteo_icon2i.region",
    "geosphere": "validation.geosphere.location",
    "italiameteo_dpc_catalog": (
        "validation.italiameteo.dpc_sri_historical_catalog"
    ),
    "metoffice_radar": "radar.metoffice.uk_rate",
    "meteolux": "validation.meteolux.location",
}


def _snapshot_provider_id(provider_id: str) -> str:
    return SNAPSHOT_PROVIDER_IDS.get(provider_id, provider_id)


def _sample_regional_radar_at_target(
    regional: Any,
    target: Any,
) -> tuple[float, float] | None:
    field = regional.field
    if field is None:
        return None
    rain_rate = _sample_radar_field_at_target(field, target)
    if rain_rate is None:
        return None
    transformer = Transformer.from_crs("EPSG:4326", field.crs, always_xy=True)
    target_x_m, target_y_m = transformer.transform(
        float(target.longitude), float(target.latitude),
    )
    column = math.floor(
        (target_x_m - field.origin_x_m) / field.x_resolution_m
    )
    row = math.floor(
        (field.origin_y_m - target_y_m) / field.y_resolution_m
    )
    height, width = field.rain_rate_mm_h.shape
    if not (0 <= row < height and 0 <= column < width):
        return None
    if not bool(field.valid_mask[row, column]):
        return None
    confidence = float(regional.confidence[row, column])
    return rain_rate, confidence if math.isfinite(confidence) else 0.0


def _sample_radar_field_at_target(
    field: Any,
    target: Any,
) -> float | None:
    transformer = Transformer.from_crs("EPSG:4326", field.crs, always_xy=True)
    target_x_m, target_y_m = transformer.transform(
        float(target.longitude), float(target.latitude),
    )
    column = math.floor(
        (target_x_m - field.origin_x_m) / field.x_resolution_m
    )
    row = math.floor(
        (field.origin_y_m - target_y_m) / field.y_resolution_m
    )
    height, width = field.rain_rate_mm_h.shape
    if not (0 <= row < height and 0 <= column < width):
        return None
    if not bool(field.valid_mask[row, column]):
        return None
    rain_rate = float(field.rain_rate_mm_h[row, column])
    if not math.isfinite(rain_rate) or rain_rate <= 0.0:
        return None
    return rain_rate


def _apply_regional_radar_to_targets(
    fusion: FusionCycle,
    region_plan: ThinRegionPlan,
    regional_fields: Mapping[str, Any],
) -> FusionCycle:
    targets = dict(fusion.targets)
    changed = False
    for region in region_plan.regions:
        regional = regional_fields.get(region.region_id)
        if regional is None or regional.field is None:
            continue
        contributors = regional.summary.contributors or (
            regional.field.provider_id,
        )
        for target in region.targets:
            sample = _sample_regional_radar_at_target(regional, target)
            if sample is None:
                continue
            rain_rate, radar_confidence = sample
            current = targets.get(target.target_id)
            if current is None:
                current = FusedTargetObservation(
                    target_id=target.target_id,
                    status="onvoldoende_data",
                    evaluated_at=fusion.evaluated_at,
                    precipitation_rate_mm_h=None,
                    reflectivity_dbz=None,
                    radar_level=None,
                    rain_probability_percent=None,
                    pressure_hpa=None,
                    temperature_c=None,
                    lightning_count=0,
                    confidence=0.0,
                    contributors=(),
                    source_observed_at=MappingProxyType({}),
                )
            source_times = dict(current.source_observed_at)
            source_times.update({
                provider_id: regional.field.observed_at
                for provider_id in contributors
            })
            targets[target.target_id] = replace(
                current,
                status="operational",
                precipitation_rate_mm_h=max(
                    rain_rate,
                    float(current.precipitation_rate_mm_h or 0.0),
                ),
                confidence=max(current.confidence, radar_confidence),
                contributors=tuple(sorted(
                    set(current.contributors).union(contributors)
                )),
                source_observed_at=MappingProxyType(source_times),
            )
            changed = True
    if not changed:
        return fusion
    return replace(fusion, targets=MappingProxyType(targets))


def _apply_native_radar_to_targets(
    fusion: FusionCycle,
    region_plan: ThinRegionPlan,
    native_fields: Mapping[str, tuple[Any, ...]],
) -> FusionCycle:
    """Use the same native fallback radar fields as the map/tracker.

    Regional fusion is asynchronous. While it is pending, live native radar
    fields already drive the visible cell contours. Sampling those exact
    fields keeps the target status consistent with that visible evidence.
    """
    targets = dict(fusion.targets)
    changed = False
    for region in region_plan.regions:
        fields = native_fields.get(region.region_id, ())
        if not fields:
            continue
        for target in region.targets:
            positive_samples = tuple(
                (field, rain_rate)
                for field in fields
                if (rain_rate := _sample_radar_field_at_target(field, target))
                is not None
            )
            if not positive_samples:
                continue
            rain_rate = max(sample for _, sample in positive_samples)
            contributors = tuple(sorted({
                field.provider_id for field, _ in positive_samples
            }))
            current = targets.get(target.target_id)
            if current is None:
                current = FusedTargetObservation(
                    target_id=target.target_id,
                    status="onvoldoende_data",
                    evaluated_at=fusion.evaluated_at,
                    precipitation_rate_mm_h=None,
                    reflectivity_dbz=None,
                    radar_level=None,
                    rain_probability_percent=None,
                    pressure_hpa=None,
                    temperature_c=None,
                    lightning_count=0,
                    confidence=0.0,
                    contributors=(),
                    source_observed_at=MappingProxyType({}),
                )
            source_times = dict(current.source_observed_at)
            source_times.update({
                field.provider_id: field.observed_at
                for field, _ in positive_samples
            })
            targets[target.target_id] = replace(
                current,
                status="operational",
                precipitation_rate_mm_h=max(
                    rain_rate,
                    float(current.precipitation_rate_mm_h or 0.0),
                ),
                contributors=tuple(sorted(
                    set(current.contributors).union(contributors)
                )),
                source_observed_at=MappingProxyType(source_times),
            )
            changed = True
    if not changed:
        return fusion
    return replace(fusion, targets=MappingProxyType(targets))


def _ground_evidence(
    fusion: FusionCycle,
    *,
    target_id: str,
    current_is_rain: bool,
) -> dict[str, Any]:
    latest_by_site = {}
    for observation in fusion.observations:
        station_id = observation.metadata.get("station_id")
        if (
            observation.target_id != target_id
            or observation.kind is not ObservationKind.RAIN_RATE
            or not station_id
        ):
            continue
        site_key = (observation.provider_id, str(station_id))
        previous = latest_by_site.get(site_key)
        if previous is None or observation.observed_at > previous.observed_at:
            latest_by_site[site_key] = observation

    sites = tuple(latest_by_site.values())
    if not sites:
        return {
            "status": "unknown",
            "freshness": "unknown",
            "confirmation": None,
            "same_track": None,
            "evidence_valid_at": None,
            "observed_at_min": None,
            "observed_at_max": None,
            "received_at": None,
            "physical_site_count": 0,
            "wet_site_count": 0,
            "source_ids": [],
            "reason_codes": [],
        }

    wet_site_count = sum(observation.value > 0.0 for observation in sites)
    if wet_site_count == 0:
        status = "dry"
    elif wet_site_count == len(sites):
        status = "wet"
    else:
        status = "mixed"
    observed_times = [observation.observed_at for observation in sites]
    return {
        "status": status,
        "freshness": "current",
        "confirmation": bool(current_is_rain and wet_site_count > 0),
        "same_track": None,
        "evidence_valid_at": max(observed_times),
        "observed_at_min": min(observed_times),
        "observed_at_max": max(observed_times),
        "received_at": max(observation.received_at for observation in sites),
        "physical_site_count": len(sites),
        "wet_site_count": wet_site_count,
        "source_ids": sorted({
            _snapshot_provider_id(observation.provider_id)
            for observation in sites
        }),
        "reason_codes": [],
    }


@dataclass(frozen=True, slots=True)
class ThinCycleResult:
    snapshot: Mapping[str, Any]
    fusion: FusionCycle
    nowcast: ThinNowcast
    postgis: PostGISWriteResult
    validations: tuple[ForecastValidation, ...] = ()
    regional_radar: tuple[RegionalFusionSummary, ...] = ()


def _collection(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {"complete": True, "overflow_count": 0, "items": items}


def _storm_collection(items: list[dict[str, Any]]) -> dict[str, Any]:
    """Keep the strongest cells within the existing HA snapshot contract."""
    overflow = max(0, len(items) - MAX_UI_STORM_SUMMARIES)
    retained = sorted(
        items,
        key=lambda item: (
            item.get("max_dbz") is not None,
            float(item.get("max_dbz") or -100.0),
        ),
        reverse=True,
    )[:MAX_UI_STORM_SUMMARIES]
    retained.sort(key=lambda item: item["storm_id"])
    return {
        "complete": overflow == 0,
        "overflow_count": overflow,
        "items": retained,
    }


class ThinOperationalRuntime:
    """Own exactly the five requested stages without legacy scheduling state."""

    def __init__(
        self,
        *,
        provider_runtime: Any,
        postgis_writer: Any | None = None,
        clock: Callable[[], float] = time.time,
        regional_radar_coordinator: Any | None = None,
    ) -> None:
        self._providers = provider_runtime
        self._postgis = postgis_writer
        self._clock = clock
        self._state: DesiredState | None = None
        self._authority: tuple[str, int, str, float] | None = None
        self._receipt_count = 0
        self._first_received_at: float | None = None
        self._last_received_at: float | None = None
        self._activated_at: float | None = None
        self._last_attempts: dict[str, float] = {}
        self._latest_provider_results: dict[str, Any] = {}
        self._region_planner = ThinRegionPlanner()
        self._nowcast = ThinNowcastRuntime(targets={})
        self._radar_tracker = ThinRadarTracker()
        self._regional_radar = (
            ThinRegionalRadarFusionCoordinator(max_workers=2)
            if regional_radar_coordinator is None
            else regional_radar_coordinator
        )
        self._ground_validation = GroundValidationRuntime()
        # A worker App restart must not reset revisions below the snapshot still
        # cached by Home Assistant. Seconds since a recent fixed epoch fit the
        # snapshot contract's 1e9 counter ceiling. The service runs no faster
        # than once every two seconds, so a restarted seed advances faster than
        # the in-process counter it replaces.
        self._revision = max(
            0,
            int(float(self._clock())) - SNAPSHOT_REVISION_EPOCH_SECONDS,
        )
        self._failed_cycles = 0
        self._latest_geojson: dict[str, Any] = {
            "type": "FeatureCollection",
            "features": [],
            "metadata": {
                "schema_version": 1,
                "feature_count": 0,
                "state": "waiting_for_first_cycle",
                "builder": "thin_worker",
            },
        }

    @property
    def has_state(self) -> bool:
        return self._state is not None

    @property
    def targets(self) -> tuple[Any, ...]:
        return () if self._state is None else self._state.targets

    def geojson_collection(self) -> dict[str, Any]:
        """Return a detached, bounded map derived from the latest nowcast."""
        return json.loads(json.dumps(self._latest_geojson, allow_nan=False))

    def close(self) -> None:
        """Release bounded background compute owned by this runtime."""
        self._regional_radar.close()

    @staticmethod
    def _fallback_regional_summary(
        region: Any,
        *,
        evaluated_at: float,
        reason_code: str,
    ) -> RegionalFusionSummary:
        return RegionalFusionSummary(
            region_id=region.region_id,
            grid_id=None,
            status=(
                "onvoldoende_data"
                if reason_code == "onvoldoende_data"
                else "degraded"
            ),
            mode="fallback",
            evaluated_at=evaluated_at,
            field_observed_at=None,
            contributors=(),
            excluded={},
            standby={},
            reason_codes=(reason_code,),
            coverage_fraction=0.0,
            mean_confidence=0.0,
            disagreement_fraction=0.0,
        )

    def claim_authority(self, producer_id: str, claim_id: str) -> dict[str, Any]:
        producer = str(producer_id).strip()
        claim = str(claim_id).strip()
        if not producer or not claim:
            raise ValueError("producer_id_and_claim_id_required")
        now = float(self._clock())
        if self._authority is not None and self._authority[2] == claim:
            if self._authority[0] != producer:
                raise DesiredStateConflict("claim_id belongs to another producer")
            epoch = self._authority[1]
            status = "claim_idempotent"
            claimed_at = self._authority[3]
        else:
            epoch = 1 if self._authority is None else self._authority[1] + 1
            self._authority = (producer, epoch, claim, now)
            status = "claimed"
            claimed_at = now
        return {
            "method": METHOD,
            "status": status,
            "producer_id": producer,
            "authority_epoch": epoch,
            "claim_id": claim,
            "claimed_at": claimed_at,
        }

    def desired_state_snapshot(self) -> dict[str, Any]:
        state = self._state
        if state is None:
            result: dict[str, Any] = {"method": METHOD, "status": "unconfigured"}
            if self._authority is not None:
                result.update({
                    "producer_id": self._authority[0],
                    "authority_epoch": self._authority[1],
                })
            return result
        return {
            "method": METHOD,
            "status": "configured",
            "state_sha256": state.state_sha256,
            "producer_id": state.producer_id,
            "authority_epoch": state.authority_epoch,
            "generation": state.generation,
            "issued_at": state.issued_at,
            "first_received_at": self._first_received_at,
            "last_received_at": self._last_received_at,
            "receipt_count": self._receipt_count,
            "activated_at": self._activated_at,
            "target_count": len(state.targets),
        }

    def apply_desired_state(self, document: Mapping[str, Any]) -> dict[str, Any]:
        candidate = parse_desired_state(document)
        now = float(self._clock())
        if self._authority is not None and (
            candidate.producer_id != self._authority[0]
            or candidate.authority_epoch != self._authority[1]
        ):
            raise DesiredStateConflict("desired-state authority mismatch")
        current = self._state
        if current is not None and candidate.state_sha256 == current.state_sha256:
            self._receipt_count += 1
            self._last_received_at = now
            status = "idempotent"
        else:
            if current is not None:
                if candidate.previous_state_sha256 != current.state_sha256:
                    raise DesiredStateConflict("previous_state_sha256 mismatch")
                if (
                    candidate.producer_id == current.producer_id
                    and candidate.authority_epoch == current.authority_epoch
                    and candidate.generation <= current.generation
                ):
                    raise DesiredStateConflict("generation must strictly increase")
            elif candidate.previous_state_sha256 is not None:
                raise DesiredStateConflict("first desired-state cannot name a previous state")
            self._state = candidate
            self._receipt_count = 1
            self._first_received_at = now
            self._last_received_at = now
            self._activated_at = now
            status = "activated"
        return {
            "method": METHOD,
            "status": status,
            "state_sha256": candidate.state_sha256,
            "producer_id": candidate.producer_id,
            "authority_epoch": candidate.authority_epoch,
            "generation": candidate.generation,
            "issued_at": candidate.issued_at,
            "first_received_at": self._first_received_at,
            "last_received_at": self._last_received_at,
            "receipt_count": self._receipt_count,
            "target_count": len(candidate.targets),
        }

    @staticmethod
    def _scope_provider_results(
        results: Mapping[str, Any],
        region_plan: ThinRegionPlan,
    ) -> dict[str, Any]:
        target_ids = {target.target_id for target in region_plan.targets}
        scoped: dict[str, Any] = {}
        for provider_id, result in results.items():
            observations = tuple(
                observation
                for observation in result.observations
                if (
                    observation.target_id in target_ids
                    if observation.target_id is not None
                    else region_plan.accepts_position(
                        observation.latitude,
                        observation.longitude,
                    )
                )
            )
            scoped[provider_id] = (
                result
                if observations == result.observations
                else replace(result, observations=observations)
            )
        return scoped

    async def run_cycle(self) -> ThinCycleResult:
        state = self._state
        if state is None:
            raise RuntimeError("desired_state_unconfigured")
        # A desired-state message may arrive while provider I/O is pending.
        # Keep this cycle internally consistent: its fusion, nowcast, map and
        # snapshot must all describe the same captured target set.
        now = float(self._clock())
        region_plan = self._region_planner.plan(state, now=now)
        active_targets = region_plan.targets
        self._nowcast.update_targets({
            item.target_id: (
                item.latitude,
                item.longitude,
                max(0.0, float(item.location_accuracy_m or 0.0) / 1000.0),
            )
            for item in active_targets
        })
        requests = build_due_requests(
            state,
            now=now,
            last_attempts=self._last_attempts,
            region_plan=region_plan,
        )
        for request in requests:
            self._last_attempts[request.provider_id] = now
        fresh_results = self._scope_provider_results(
            dict(await self._providers.fetch(requests)),
            region_plan,
        )
        self._latest_provider_results.update(fresh_results)
        fusion = fuse_observations(
            self._latest_provider_results,
            evaluated_at=float(self._clock()),
            target_ids=tuple(item.target_id for item in active_targets),
        )
        tracked_cells: list[StormCell] = []
        self._regional_radar.submit(
            region_plan,
            self._latest_provider_results,
            evaluated_at=fusion.evaluated_at,
        )
        completed_regional = self._regional_radar.poll()
        pending_region_ids = self._regional_radar.pending_region_ids()
        regional_summaries: list[RegionalFusionSummary] = []
        native_fallback_fields: dict[str, tuple[Any, ...]] = {}

        def track_field(field: Any, targets: tuple[Any, ...]) -> None:
            for target in targets:
                try:
                    self._radar_tracker.advance(field, target)
                except (ValueError, RuntimeError):
                    continue
                for item in self._radar_tracker.tracks(field, target.target_id):
                    speed = math.hypot(
                        item.velocity_east_kmh, item.velocity_north_kmh,
                    )
                    heading = (
                        None
                        if speed < 0.5
                        else math.degrees(math.atan2(
                            item.velocity_east_kmh,
                            item.velocity_north_kmh,
                        )) % 360.0
                    )
                    tracked_cells.append(StormCell(
                        cell_id=f"{item.provider_id}:{target.target_id}:{item.track_id}",
                        provider_id=item.provider_id,
                        observed_at=item.observed_at,
                        latitude=item.latitude,
                        longitude=item.longitude,
                        geometry=item.geometry,
                        precipitation_rate_mm_h=item.rain_rate_mm_h,
                        reflectivity_dbz=None,
                        radar_level=None,
                        velocity_east_kmh=item.velocity_east_kmh,
                        velocity_north_kmh=item.velocity_north_kmh,
                        speed_kmh=speed,
                        heading_deg=heading,
                    ))

        for region in region_plan.regions:
            regional = completed_regional.get(region.region_id)
            if regional is not None and regional.field is not None:
                track_field(regional.field, region.targets)
                regional_summaries.append(regional.summary)
                continue

            # Provider I/O is less frequent than processing. Cached,
            # still-current native frames remain the explicit fallback while
            # regional fusion is pending or unavailable.
            current_native_fields: list[Any] = []
            for result in self._latest_provider_results.values():
                for field in result.radar_fields:
                    field_maximum_age = PROVIDER_FUSION_AGE_SECONDS.get(
                        field.provider_id,
                        MAX_FUSION_AGE_SECONDS,
                    )
                    if not 0 <= fusion.evaluated_at - field.observed_at <= field_maximum_age:
                        continue
                    current_native_fields.append(field)
                    track_field(field, region.targets)
            native_fallback_fields[region.region_id] = tuple(
                current_native_fields
            )

            if regional is not None:
                regional_summaries.append(regional.summary)
            else:
                reason_code = (
                    "fusion_pending"
                    if region.region_id in pending_region_ids
                    else "onvoldoende_data"
                )
                regional_summaries.append(self._fallback_regional_summary(
                    region,
                    evaluated_at=fusion.evaluated_at,
                    reason_code=reason_code,
                ))
        fusion = _apply_regional_radar_to_targets(
            fusion,
            region_plan,
            completed_regional,
        )
        fusion = _apply_native_radar_to_targets(
            fusion,
            region_plan,
            native_fallback_fields,
        )
        nowcast = self._nowcast.advance(
            fusion,
            tracked_cells=tuple({item.cell_id: item for item in tracked_cells}.values()) or None,
        )
        validations = self._ground_validation.advance(
            nowcast.target_passages,
            fusion.observations,
            evaluated_at=float(self._clock()),
        )
        self._latest_geojson = self._build_geojson(
            fusion,
            nowcast,
            state=state,
            region_plan=region_plan,
            regional_radar=tuple(regional_summaries),
            regional_radar_fields=completed_regional,
        )
        if self._postgis is None:
            postgis = PostGISWriteResult(
                status="disabled",
                last_error_code=None,
                written_rows={},
                queued_cycles=0,
                dropped_cycles=0,
            )
        else:
            postgis = await self._postgis.write_cycle(
                fusion,
                nowcast,
                validations,
                regional_radar=tuple(regional_summaries),
                region_plan=region_plan,
            )
        self._revision += 1
        snapshot = self._build_snapshot(
            fusion,
            nowcast,
            postgis,
            state=state,
            region_plan=region_plan,
            regional_radar=tuple(regional_summaries),
        )
        return ThinCycleResult(
            snapshot=snapshot,
            fusion=fusion,
            nowcast=nowcast,
            postgis=postgis,
            validations=validations,
            regional_radar=tuple(regional_summaries),
        )

    def _build_geojson(
        self,
        fusion: FusionCycle,
        nowcast: ThinNowcast,
        *,
        state: DesiredState | None = None,
        region_plan: ThinRegionPlan | None = None,
        regional_radar: tuple[RegionalFusionSummary, ...] = (),
        regional_radar_fields: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        state = self._state if state is None else state
        assert state is not None
        region_plan = (
            self._region_planner.plan(state, now=float(fusion.evaluated_at))
            if region_plan is None
            else region_plan
        )
        overlay_fields = [
            (region_id, result.field)
            for region_id, result in sorted(
                (regional_radar_fields or {}).items()
            )
            if result.field is not None
        ]
        per_overlay_run_budget = max(
            1,
            MAX_TOTAL_RADAR_OVERLAY_RUNS // max(1, len(overlay_fields)),
        )
        radar_overlays = {
            region_id: build_radar_overlay(
                field,
                maximum_runs=per_overlay_run_budget,
            )
            for region_id, field in overlay_fields
        }
        target_features: list[dict[str, Any]] = []
        cell_feature_groups: list[tuple[float, str, list[dict[str, Any]]]] = []
        lightning_features: list[dict[str, Any]] = []
        default_region_id = (
            region_plan.regions[0].region_id
            if region_plan.regions
            else "region:unassigned"
        )
        for target in region_plan.targets:
            region_id = region_plan.region_id_for(target.target_id) or default_region_id
            target_features.append({
                "type": "Feature",
                "id": f"target:{target.target_id}",
                "geometry": {
                    "type": "Point",
                    "coordinates": [target.longitude, target.latitude],
                },
                "properties": {
                    "layer": "target",
                    "target_id": target.target_id,
                    "name": target.display_name,
                    "available": True,
                    "primary": target.kind == "home",
                    "radar_covered": True,
                    "region_engine": region_id,
                },
            })

        for cell in nowcast.storm_cells:
            region_id = default_region_id
            geometry = json.loads(json.dumps(dict(cell.geometry), allow_nan=False))
            group = [{
                "type": "Feature",
                "id": f"cell:{cell.cell_id}",
                "geometry": geometry,
                "properties": {
                    "layer": "radar_cell",
                    "engine_id": region_id,
                    "storm_id": cell.cell_id,
                    "intensity": cell.precipitation_rate_mm_h,
                    "max_dbz": cell.reflectivity_dbz,
                    "radar_level": cell.radar_level,
                    "speed_kmh": cell.speed_kmh,
                    "heading_deg": cell.heading_deg,
                    "provider_id": cell.provider_id,
                    "observed_at": cell.observed_at,
                },
            }]
            if cell.heading_deg is None or cell.speed_kmh < 0.5:
                cell_feature_groups.append((
                    float(cell.reflectivity_dbz or -100.0),
                    cell.cell_id,
                    group,
                ))
                continue
            hours = 1.0
            latitude_delta = cell.velocity_north_kmh * hours / 110.574
            longitude_delta = cell.velocity_east_kmh * hours / (
                111.320 * max(0.05, abs(math.cos(math.radians(cell.latitude))))
            )
            group.append({
                "type": "Feature",
                "id": f"motion:{cell.cell_id}",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [cell.longitude, cell.latitude],
                        [cell.longitude + longitude_delta, cell.latitude + latitude_delta],
                    ],
                },
                "properties": {
                    "layer": "motion",
                    "engine_id": region_id,
                    "storm_id": cell.cell_id,
                    "minutes": 60,
                    "speed_kmh": cell.speed_kmh,
                    "heading_deg": cell.heading_deg,
                    "motion_model": "linear",
                },
            })
            cell_feature_groups.append((
                float(cell.reflectivity_dbz or -100.0),
                cell.cell_id,
                group,
            ))

        lightning_count = 0
        for index, observation in enumerate(fusion.observations):
            if observation.kind.value != "lightning_strike" or lightning_count >= 300:
                continue
            lightning_features.append({
                "type": "Feature",
                "id": f"lightning:{observation.observed_at:.3f}:{index}",
                "geometry": {
                    "type": "Point",
                    "coordinates": [observation.longitude, observation.latitude],
                },
                "properties": {
                    "layer": "lightning",
                    "engine_id": (
                        region_plan.region_id_for(observation.target_id)
                        if observation.target_id is not None
                        else default_region_id
                    ) or default_region_id,
                    "source": observation.provider_id,
                    "timestamp": observation.observed_at,
                    "age_seconds": max(
                        0,
                        round(fusion.evaluated_at - observation.observed_at),
                    ),
                },
            })
            lightning_count += 1

        total_cells = len(cell_feature_groups)

        def _collection(
            selected: list[tuple[float, str, list[dict[str, Any]]]],
        ) -> dict[str, Any]:
            selected_ids = {item[1] for item in selected}
            cell_features = [
                feature
                for item in cell_feature_groups
                if item[1] in selected_ids
                for feature in item[2]
            ]
            features = target_features + cell_features + lightning_features
            return {
                "type": "FeatureCollection",
                "features": features,
                "radar_overlays": radar_overlays,
                "metadata": {
                    "schema_version": 1,
                    "feature_count": len(features),
                    "radar_cells_included": len(selected),
                    "radar_cells_total": total_cells,
                    "lightning_events_included": lightning_count,
                    "truncated": len(selected) < total_cells,
                    "state": "operational",
                    "builder": "thin_worker",
                    "built_at": fusion.evaluated_at,
                    "radar_fusion": [
                        summary.as_dict()
                        for summary in sorted(
                            regional_radar,
                            key=lambda item: item.region_id,
                        )
                    ],
                },
            }

        def _encoded_size(collection: Mapping[str, Any]) -> int:
            return len(json.dumps(
                collection,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8"))

        complete = _collection(cell_feature_groups)
        if _encoded_size(complete) <= MAX_GEOJSON_PAYLOAD_BYTES:
            return complete

        strongest = sorted(
            cell_feature_groups,
            key=lambda item: (item[0], item[1]),
            reverse=True,
        )
        low = 0
        high = len(strongest)
        best = _collection([])
        while low <= high:
            middle = (low + high) // 2
            candidate = _collection(strongest[:middle])
            if _encoded_size(candidate) <= MAX_GEOJSON_PAYLOAD_BYTES:
                best = candidate
                low = middle + 1
            else:
                high = middle - 1
        return best

    def _build_snapshot(
        self,
        fusion: FusionCycle,
        nowcast: ThinNowcast,
        postgis: PostGISWriteResult,
        *,
        state: DesiredState,
        region_plan: ThinRegionPlan,
        regional_radar: tuple[RegionalFusionSummary, ...] = (),
    ) -> dict[str, Any]:
        # The PostGIS projection can take longer than the snapshot TTL for a
        # large first cycle.  Start the control-plane validity window when the
        # completed cycle is published, not when fusion began.
        evaluated_at = float(self._clock())
        observations = fusion.observations
        regional_with_data = tuple(
            item for item in regional_radar if item.field_observed_at is not None
        )
        product_valid_at = max(
            (
                *(item.observed_at for item in observations),
                *(float(item.field_observed_at) for item in regional_with_data),
            ),
            default=None,
        )
        received_at = max(
            (
                *(item.received_at for item in observations),
                *(item.evaluated_at for item in regional_with_data),
            ),
            default=None,
        )
        source_ids = sorted(
            {
                *(_snapshot_provider_id(item.provider_id) for item in observations),
                *(
                    _snapshot_provider_id(provider_id)
                    for item in regional_with_data
                    for provider_id in item.contributors
                ),
            }
        )

        provider_items: list[dict[str, Any]] = []
        ready_provider_ids = frozenset(
            getattr(self._providers, "ready_provider_ids", ())
        )
        for spec in PROVIDERS:
            applicable = any(
                spec.applies_to(target) for target in region_plan.targets
            )
            if not applicable:
                provider_items.append({
                    "provider_id": _snapshot_provider_id(spec.provider_id),
                    "display_name": spec.provider_id.replace("_", " ").title(),
                    "readiness": (
                        "ready"
                        if spec.provider_id in ready_provider_ids
                        else "not_ready"
                    ),
                    "state": "unknown",
                    "freshness": "unknown",
                    "product_valid_at": None,
                    "received_at": None,
                    "error": None,
                    "reason_codes": ["target_outside_catalog_scope"],
                })
                continue
            result = self._latest_provider_results.get(spec.provider_id)
            if result is None:
                provider_state, freshness = "unknown", "unknown"
                readiness = (
                    "ready"
                    if spec.provider_id in ready_provider_ids
                    else "not_ready"
                )
                valid_at = receipt = error = None
                reasons = ["not_attempted"]
            else:
                readiness = "ready" if result.ready else "not_ready"
                if result.status == "operational":
                    provider_state, freshness = "operational", "current"
                elif result.status == "empty":
                    provider_state, freshness = "operational", "unknown"
                elif result.status == "blocked" and result.reason_code == "external_access_required":
                    provider_state, freshness = "api_required", "unknown"
                elif result.status == "stale":
                    provider_state, freshness = "stale", "stale"
                elif result.status in {"error", "backoff"}:
                    provider_state, freshness = "degraded", "unknown"
                else:
                    provider_state, freshness = "unknown", "unknown"
                valid_at = max((item.observed_at for item in result.observations), default=None)
                receipt = max((item.received_at for item in result.observations), default=None)
                error = result.reason_code if result.status == "error" else None
                reasons = [result.reason_code] if result.reason_code else []
                if freshness == "current" and not any(
                    item.provider_id == spec.provider_id for item in observations
                ):
                    # Fusion may have aged out a cached result since its fetch.
                    freshness = "stale" if result.observations else "unknown"
                    reasons.append("no_current_observations")
            provider_items.append({
                "provider_id": _snapshot_provider_id(spec.provider_id),
                "display_name": spec.provider_id.replace("_", " ").title(),
                "readiness": readiness,
                "state": provider_state,
                "freshness": freshness,
                "product_valid_at": valid_at,
                "received_at": receipt,
                "error": error,
                "reason_codes": sorted(reasons),
            })
        provider_items.sort(key=lambda item: item["provider_id"])

        target_items: list[dict[str, Any]] = []
        regional_by_id = {
            summary.region_id: summary for summary in regional_radar
        }

        def source_provider_ids(
            values: set[str],
            *,
            region_id: str,
        ) -> set[str]:
            resolved: set[str] = set()
            for value in values:
                if value.startswith("fusion:"):
                    summary = regional_by_id.get(region_id)
                    if summary is not None:
                        resolved.update(summary.contributors)
                else:
                    resolved.add(value)
            return {_snapshot_provider_id(value) for value in resolved}

        region_contributors: dict[str, set[str]] = {
            region.region_id: set() for region in region_plan.regions
        }
        for target in sorted(region_plan.targets, key=lambda item: item.target_id):
            result = nowcast.target_results[target.target_id]
            fused_target = fusion.targets.get(target.target_id)
            target_product_valid_at = max(
                () if fused_target is None else fused_target.source_observed_at.values(),
                default=product_valid_at,
            )
            region_id = (
                region_plan.region_id_for(target.target_id)
                or "region:unassigned"
            )
            raw_provider_ids = set(result.contributors)
            if result.arrival_provider_id is not None:
                raw_provider_ids.add(result.arrival_provider_id)
            providers = sorted(source_provider_ids(
                raw_provider_ids,
                region_id=region_id,
            ))
            region_contributors.setdefault(region_id, set()).update(providers)
            current_is_rain = result.current_state == "regen"
            current_is_dry = result.current_state == "droog"
            current_freshness = "current" if providers else "unknown"
            current_state = (
                "raining" if current_is_rain
                else "dry" if current_is_dry and providers
                else "unknown"
            )
            if result.forecast_state == "regen_op_komst" and result.eta_minutes is not None:
                threat_state, threat_freshness = "available", "current"
                track_id = result.arrival_cell_id
                # Confidence currently ranks tracking evidence; it is not an
                # empirically calibrated probability. Keep the nullable
                # probability pair empty until calibration data exists.
                probability = calibration = None
                passage = "expected"
            elif result.forecast_state == "geen_dreiging" and providers:
                threat_state, threat_freshness = "none", "current"
                track_id = probability = calibration = None
                passage = "none"
            else:
                threat_state = "unknown"
                threat_freshness = "current" if providers else "unknown"
                track_id = probability = calibration = None
                passage = "unknown"
            target_reason_codes = [result.reason_code]
            if region_plan.location_status_for(target.target_id) == "stale":
                target_reason_codes.append("target_location_stale")
            target_reason_codes = sorted(set(target_reason_codes))
            target_items.append({
                "target_id": target.target_id,
                "display_name": target.display_name,
                "region_id": region_id,
                "status_text": (
                    "Regen gedetecteerd" if current_is_rain
                    else f"Regen verwacht over {result.eta_minutes:.0f} min"
                    if threat_state == "available" else ""
                ),
                "current_precipitation": {
                    "state": current_state,
                    "freshness": current_freshness,
                    "rain_rate_mm_h": (
                        result.precipitation_rate_mm_h
                        if current_state in {"raining", "dry"}
                        else None
                    ),
                    "product_valid_at": (
                        target_product_valid_at
                        if current_freshness == "current"
                        else None
                    ),
                    "provider_ids": providers,
                    "reason_codes": target_reason_codes,
                    "ground_evidence": _ground_evidence(
                        fusion,
                        target_id=target.target_id,
                        current_is_rain=current_is_rain,
                    ),
                },
                "next_threat": {
                    "state": threat_state,
                    "freshness": threat_freshness,
                    "track_id": track_id,
                    "calibrated_probability_percent": probability,
                    "probability_calibration_id": calibration,
                    "eta_minutes": result.eta_minutes if threat_state == "available" else None,
                    "distance_km": None,
                    "passage_status": passage,
                    "product_valid_at": (
                        target_product_valid_at
                        if threat_freshness == "current"
                        else None
                    ),
                    "provider_ids": providers,
                    "reason_codes": target_reason_codes,
                },
            })

        region_items: list[dict[str, Any]] = []
        for region in region_plan.regions:
            applicable_ids = sorted({
                _snapshot_provider_id(spec.provider_id)
                for spec in PROVIDERS
                if any(spec.applies_to(target) for target in region.targets)
            })
            home_target = next(
                (target for target in region.targets if target.kind == "home"),
                None,
            )
            region_items.append({
                "region_id": region.region_id,
                "display_name": (
                    home_target.display_name
                    if home_target is not None
                    else " / ".join(target.display_name for target in region.targets)
                ),
                "state": (
                    "current"
                    if region_contributors.get(region.region_id)
                    else "stale"
                    if len(region.stale_target_ids) == len(region.targets)
                    else "unknown"
                ),
                "provider_ids": applicable_ids,
                "target_ids": list(region.target_ids),
            })

        default_region = region_items[0]["region_id"] if region_items else "region-unassigned"
        storm_items = [{
            "storm_id": cell.cell_id,
            "region_id": default_region,
            "display_name": cell.cell_id,
            "state": "active",
            "diagnostic_quality_percent": None,
            "diagnostic_quality_kind": None,
            "motion_speed_kmh": cell.speed_kmh,
            "motion_heading_deg": cell.heading_deg,
            "max_dbz": cell.reflectivity_dbz,
            "area_km2": None,
            "product_valid_at": cell.observed_at,
            "reason_codes": [],
        } for cell in nowcast.storm_cells]
        storm_items.sort(key=lambda item: item["storm_id"])

        snapshot = {
            "contract": "operational_snapshot_v1",
            "revision": self._revision,
            "desired_state_sha256": state.state_sha256,
            "product_valid_at": product_valid_at,
            "received_at": received_at,
            "evaluated_at": evaluated_at,
            "valid_until": evaluated_at + SNAPSHOT_TTL_SECONDS,
            "provenance_complete": bool(source_ids),
            "provenance_source_ids": source_ids,
            "provenance_reason_codes": (
                [] if source_ids else ["insufficient_observations"]
            ),
            "worker": {
                "state": "degraded" if postgis.status == "degraded" else "online",
                "queue_depth": postgis.queued_cycles,
                "failed_jobs": self._failed_cycles,
                "status_text": "postgis gedegradeerd" if postgis.status == "degraded" else "operationeel",
            },
            "regions": _collection(region_items),
            "targets": _collection(target_items),
            "providers": _collection(provider_items),
            "ui_storms": _storm_collection(storm_items),
            "map_artifact": {"artifact_handle": None, "etag": None},
            "radar_fusion": _collection([
                summary.as_dict()
                for summary in sorted(
                    regional_radar,
                    key=lambda item: item.region_id,
                )
            ]),
        }
        return validate_snapshot(snapshot)


__all__ = [
    "MAX_GEOJSON_PAYLOAD_BYTES",
    "ThinCycleResult",
    "ThinOperationalRuntime",
]
