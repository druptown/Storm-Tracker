"""Home Assistant snapshot sensors for the explicit worker-only runtime.

The entities subscribe to one runtime-owned in-memory snapshot event, then
project only the already validated
``OperationalSnapshotCache`` through ``operational_snapshot_views``.

There is no coordinator, polling, provider access, meteorological compute,
database access or file/network I/O in this layer.  A future threat and ground
confirmation remain attributes; neither is allowed to manufacture the current
precipitation state.  Unknown or stale evidence is never rendered as dry.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import json
import math
from typing import Any

from homeassistant.components.sensor import SensorEntity
from homeassistant.core import callback

from .const import (
    DOMAIN,
    OPERATIONAL_SNAPSHOT_UPDATE_EVENT,
    RUNTIME_MODE_WORKER_ONLY,
)
from .operational_snapshot import OperationalSnapshotCache
from .operational_snapshot_views import (
    SnapshotSensorView,
    collection_count_view,
    provider_status_view,
    target_precipitation_view,
    worker_status_view,
)


CANDIDATE_CONTRACT = "stv3-ha-snapshot-sensor-candidate-v1"
# Four base entities (worker, home, storms, regions) leave sixty target
# entities inside the worker-only runtime's hard sixty-four entity ceiling.
MAX_ADDITIONAL_TARGET_SENSORS = 60
HA_ATTRIBUTE_WARNING_BYTES = 16_384
HA_ATTRIBUTE_BUDGET_REASON = "ha_attribute_budget_exceeded"

_KIND_WORKER = "worker_status"
_KIND_PRECIPITATION = "target_precipitation"
_KIND_COUNT = "collection_count"
_PENDING_REGION_ID = "region:awaiting_evidence"
_KIND_PROVIDER = "provider_status"
_ZERO_RADAR_OBSERVATION_REASON = (
    "certified_zero_radar_sample_dry_claim_withheld"
)
_UNDETECT_RADAR_OBSERVATION_REASON = (
    "certified_undetect_radar_sample_dry_claim_withheld"
)
_REFLECTIVITY_ECHO_REASON = (
    "certified_positive_reflectivity_echo_rain_rate_withheld"
)
_ZERO_RADAR_OBSERVATION_STATE = "geen_positieve_neerslag_waargenomen"
_REFLECTIVITY_ECHO_STATE = "radarecho_waargenomen"
WORKER_STATUS_UPDATE_EVENT = f"{DOMAIN}_worker_status_update"

WORKER_ONLY_PROVIDER_IDS = (
    "ground.kmi.aws_10min",
    "ground.metar.global",
    "ground.netatmo.region",
    "lightning.blitzortung.event_ingress",
    "lightning.eumetsat.li",
    "lightning.goes18.glm",
    "lightning.goes19.glm",
    "model.openmeteo.italiameteo_icon2i.region",
    "model.openmeteo.region",
    "radar.aemet.composite",
    "radar.dpc.sri",
    "radar.dwd.radolan_rv",
    "radar.kmi.appv4_visual",
    "radar.kmi.radqpe_native",
    "radar.knmi.rtcor",
    "radar.meteofrance.dpradar",
    "radar.metoffice.uk_rate",
    "radar.opera.dbzh",
    "radar.rainviewer.visual",
    "satellite.goes18.rrqpe",
    "satellite.goes19.rrqpe",
    "satellite.hsaf.h40b",
    "satellite.noaa.err",
    "validation.geosphere.location",
    "validation.italiameteo.dpc_sri_historical_catalog",
    "validation.meteolux.location",
)


def _canonical_attribute_bytes(attributes: Mapping[str, Any]) -> int:
    """Measure the exact compact UTF-8 JSON representation sent to HA."""

    return len(json.dumps(
        dict(attributes),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8"))


def _bounded_list_count(value: Any, maximum: int) -> int:
    if not isinstance(value, list):
        return 0
    return min(len(value), maximum)


def _budget_exceeded_projection(
    projected: "SnapshotSensorView",
    description: "SnapshotSensorDescription",
    *,
    measured_bytes: int,
) -> "SnapshotSensorView":
    """Replace an oversized projection with a small fail-closed diagnostic.

    Verbose identifiers and reason-code lists are deliberately not truncated:
    retaining a current weather claim beside incomplete evidence would be
    misleading.  Only bounded counts survive and every weather fact becomes
    unknown.
    """

    source = projected.attribute_copy()
    attributes = {
        "candidate_contract": CANDIDATE_CONTRACT,
        "candidate_sensor_key": description.key,
        "runtime_mode": RUNTIME_MODE_WORKER_ONLY,
        "snapshot_revision": source.get("snapshot_revision"),
        "snapshot_sha256": source.get("snapshot_sha256"),
        "snapshot_availability": source.get("snapshot_availability"),
        "worker_state": source.get("worker_state"),
        "ha_attribute_budget_bytes": HA_ATTRIBUTE_WARNING_BYTES,
        "ha_attribute_bytes_before_budget": min(measured_bytes, 1_000_000_000),
        "ha_attribute_budget_exceeded": True,
        "presentation_reason_codes": [HA_ATTRIBUTE_BUDGET_REASON],
    }
    if description.target_id is not None:
        attributes["configured_target_id"] = description.target_id
    if description.provider_id is not None:
        attributes["configured_provider_id"] = description.provider_id

    if description.kind == _KIND_PRECIPITATION:
        attributes.update({
            "current_precipitation_state": "unknown",
            "current_presentation_valid": False,
            "rain_rate_mm_h": None,
            "current_provider_count": _bounded_list_count(
                source.get("current_provider_ids"), 64
            ),
            "current_reason_count": _bounded_list_count(
                source.get("current_reason_codes"), 32
            ),
            "ground_status": "onbekend",
            "ground_confirmation": None,
            "ground_same_track": None,
            "ground_presentation_valid": False,
            "ground_source_count": _bounded_list_count(
                source.get("ground_source_ids"), 16
            ),
            "ground_reason_count": _bounded_list_count(
                source.get("ground_reason_codes"), 32
            ),
            "next_threat_state": "unknown",
            "next_threat": "onbekend",
            "next_threat_probability_percent": None,
            "next_threat_probability_calibration_id": None,
            "next_threat_eta_minutes": None,
            "next_threat_distance_km": None,
            "next_threat_passage_status": "unknown",
            "next_threat_presentation_valid": False,
            "next_threat_provider_count": _bounded_list_count(
                source.get("next_threat_provider_ids"), 64
            ),
            "next_threat_reason_count": _bounded_list_count(
                source.get("next_threat_reason_codes"), 32
            ),
        })
        native_value = "onvoldoende_data"
    elif description.kind == _KIND_COUNT:
        attributes.update({
            "bounded_snapshot_item_count": source.get(
                "bounded_snapshot_item_count"
            ),
            "count_valid": False,
        })
        native_value = None
    else:
        native_value = "unknown"

    # The compact form contains no producer-controlled lists or display text.
    # Keep an ultimate fixed-size fallback so even an unexpected non-JSON
    # scalar can never recreate the Recorder warning this guard handles.
    try:
        compact_bytes = _canonical_attribute_bytes(attributes)
    except (TypeError, ValueError, UnicodeError):
        compact_bytes = HA_ATTRIBUTE_WARNING_BYTES
    if compact_bytes >= HA_ATTRIBUTE_WARNING_BYTES:
        attributes = {
            "candidate_contract": CANDIDATE_CONTRACT,
            "candidate_sensor_key": description.key,
            "runtime_mode": RUNTIME_MODE_WORKER_ONLY,
            "ha_attribute_budget_bytes": HA_ATTRIBUTE_WARNING_BYTES,
            "ha_attribute_budget_exceeded": True,
            "presentation_reason_codes": [HA_ATTRIBUTE_BUDGET_REASON],
        }
    return SnapshotSensorView(
        native_value=native_value,
        available=True,
        attributes=attributes,
    )


def _enforce_ha_attribute_budget(
    projected: "SnapshotSensorView",
    description: "SnapshotSensorDescription",
) -> "SnapshotSensorView":
    try:
        measured = _canonical_attribute_bytes(projected.attributes)
    except (TypeError, ValueError, UnicodeError):
        measured = HA_ATTRIBUTE_WARNING_BYTES
    if measured < HA_ATTRIBUTE_WARNING_BYTES:
        return projected
    return _budget_exceeded_projection(
        projected,
        description,
        measured_bytes=measured,
    )


def _nonempty_text(value: Any, name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
    ):
        raise ValueError(f"{name} moet niet-lege tekst zijn")
    return value


def _worker_runtime_attribute_projection(hass: Any) -> dict[str, Any]:
    """Expose only the bridge's already bounded worker runtime settings."""

    hass_data = getattr(hass, "data", None)
    if not isinstance(hass_data, Mapping):
        return {}
    domain_data = hass_data.get(DOMAIN)
    if not isinstance(domain_data, Mapping):
        return {}
    status = domain_data.get("worker_status")
    if not isinstance(status, Mapping):
        return {}
    projection: dict[str, Any] = {}
    state = status.get("state")
    if state in {"waiting", "online", "busy", "degraded", "offline"}:
        projection["worker_heartbeat_state"] = state
    version = status.get("version")
    if isinstance(version, str) and 0 < len(version) <= 96:
        projection["worker_version"] = version
    profile = status.get("compute_profile")
    if profile in {"shared_ha", "dedicated_nas", "dedicated_nas_fast"}:
        projection["compute_profile"] = profile
    cpu_limit = status.get("cpu_limit_percent")
    if (
        isinstance(cpu_limit, int)
        and not isinstance(cpu_limit, bool)
        and 1 <= cpu_limit <= 200
    ):
        projection["cpu_limit_percent"] = cpu_limit
    worker_nice = status.get("worker_nice")
    if isinstance(worker_nice, int) and not isinstance(worker_nice, bool) and 0 <= worker_nice <= 19:
        projection["worker_nice"] = worker_nice
    cpu_threads = status.get("cpu_threads")
    if isinstance(cpu_threads, int) and not isinstance(cpu_threads, bool) and 1 <= cpu_threads <= 64:
        projection["cpu_threads"] = cpu_threads
    provider_cycle_seconds = status.get("provider_cycle_seconds")
    if (
        isinstance(provider_cycle_seconds, (int, float))
        and not isinstance(provider_cycle_seconds, bool)
        and math.isfinite(float(provider_cycle_seconds))
        and 2 <= float(provider_cycle_seconds) <= 300
    ):
        projection["provider_cycle_seconds"] = float(provider_cycle_seconds)
    for key in (
        "runtime_provider_count",
        "runtime_captured_provider_count",
        "worker_snapshot_revision",
        "snapshot_rich_built",
        "snapshot_rich_withheld",
        "snapshot_rich_builder_failures",
        "snapshot_unknown_built",
        "cycle_current",
        "cycle_stale",
        "cycle_error",
        "cycle_blocked",
        "cycle_not_applicable",
        "raw_calibration_captures",
        "raw_calibration_sources",
        "raw_calibration_providers",
        "raw_calibration_database_bytes",
        "postgis_projection_ready",
        "postgis_projection_leased",
        "postgis_projection_projected",
        "acquisition_parallel_lane_limit",
        "acquisition_parallel_maximum_lanes",
        "acquisition_parallel_last_batch_jobs",
        "tracking_active_tracks",
        "tracking_tracked_tracks",
        "tracking_motion_ready_tracks",
        "tracking_region_count",
        "tracking_shadow_arrival_candidates",
        "tracking_shadow_future_arrivals",
    ):
        value = status.get(key)
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and 0 <= value <= 1_000_000_000
        ):
            projection[key] = value
    parallel_mode = status.get("acquisition_parallel_mode")
    if parallel_mode in {
        "full_parallel", "recovery", "expanded", "serial_compatibility",
    }:
        projection["acquisition_parallel_mode"] = parallel_mode
    parallel_seconds = status.get("acquisition_parallel_last_batch_seconds")
    if (
        isinstance(parallel_seconds, (int, float))
        and not isinstance(parallel_seconds, bool)
        and math.isfinite(float(parallel_seconds))
        and 0.0 <= float(parallel_seconds) <= 86_400.0
    ):
        projection["acquisition_parallel_last_batch_seconds"] = float(
            parallel_seconds
        )
    parallel_reasons = status.get(
        "acquisition_parallel_last_unhealthy_reasons"
    )
    if (
        type(parallel_reasons) is list
        and len(parallel_reasons) <= 8
        and all(
            isinstance(reason, str) and 1 <= len(reason) <= 48
            for reason in parallel_reasons
        )
    ):
        projection["acquisition_parallel_last_unhealthy_reasons"] = list(
            parallel_reasons
        )
    tracking_status = status.get("tracking_shadow_arrival_status")
    if tracking_status == "shadow_candidate_unadmitted":
        projection["tracking_shadow_arrival_status"] = tracking_status
    postgis_enabled = status.get("postgis_projection_enabled")
    if isinstance(postgis_enabled, bool):
        projection["postgis_projection_enabled"] = postgis_enabled
    postgis_status = status.get("postgis_projection_status")
    if postgis_status in {"disabled", "waiting", "current", "degraded"}:
        projection["postgis_projection_status"] = postgis_status
    postgis_success = status.get("postgis_projection_last_success_at")
    if (
        isinstance(postgis_success, (int, float))
        and not isinstance(postgis_success, bool)
        and math.isfinite(float(postgis_success))
        and postgis_success >= 0
    ):
        projection["postgis_projection_last_success_at"] = float(
            postgis_success
        )
    postgis_error = status.get("postgis_projection_last_error_code")
    if postgis_error in {
        "postgis_projection_failed",
        "postgis_projection_runtime_failed",
        "postgis_provenance_conflict",
    }:
        projection["postgis_projection_last_error_code"] = postgis_error
    for key in (
        "snapshot_transport_state",
        "raw_calibration_status",
        "last_acquisition",
    ):
        value = status.get(key)
        if isinstance(value, str) and 0 < len(value) <= 96:
            projection[key] = value
    latest_raw = status.get("raw_calibration_latest_received_at")
    if (
        isinstance(latest_raw, (int, float))
        and not isinstance(latest_raw, bool)
        and math.isfinite(float(latest_raw))
        and latest_raw >= 0
    ):
        projection["raw_calibration_latest_received_at"] = float(latest_raw)
    source_statistics = status.get("raw_calibration_source_statistics")
    if isinstance(source_statistics, list) and len(source_statistics) <= 64:
        bounded_source_statistics = []
        for row in source_statistics:
            if not isinstance(row, Mapping):
                return projection
            source_id = row.get("source_id")
            provider = row.get("provider")
            captures = row.get("captures")
            latest = row.get("latest_received_at")
            if (
                not isinstance(source_id, str)
                or not 0 < len(source_id) <= 128
                or not isinstance(provider, str)
                or not 0 < len(provider) <= 128
                or not isinstance(captures, int)
                or isinstance(captures, bool)
                or not 1 <= captures <= 1_000_000_000
                or isinstance(latest, bool)
                or not isinstance(latest, (int, float))
                or not math.isfinite(float(latest))
                or latest < 0
            ):
                return projection
            bounded_source_statistics.append({
                "source_id": source_id,
                "provider": provider,
                "captures": captures,
                "latest_received_at": float(latest),
            })
        projection["raw_calibration_source_statistics"] = (
            bounded_source_statistics
        )
    return projection


@dataclass(frozen=True, slots=True)
class SnapshotSensorDescription:
    """Small immutable description with no coordinator or platform coupling."""

    key: str
    name: str
    kind: str
    target_id: str | None = None
    collection: str | None = None
    provider_id: str | None = None

    def __post_init__(self) -> None:
        _nonempty_text(self.key, "key")
        _nonempty_text(self.name, "name")
        if self.kind not in {
            _KIND_WORKER,
            _KIND_PRECIPITATION,
            _KIND_COUNT,
            _KIND_PROVIDER,
        }:
            raise ValueError("onbekend snapshotsensortype")
        if self.kind == _KIND_PRECIPITATION:
            _nonempty_text(self.target_id, "target_id")
            if self.collection is not None or self.provider_id is not None:
                raise ValueError("neerslagsensor heeft ongeldige autoriteit")
        elif self.kind == _KIND_COUNT:
            if self.collection not in {"ui_storms", "regions"}:
                raise ValueError("onbekende snapshotcollectie")
            if self.target_id is not None or self.provider_id is not None:
                raise ValueError("tellersensor heeft ongeldige autoriteit")
        elif self.kind == _KIND_PROVIDER:
            _nonempty_text(self.provider_id, "provider_id")
            if self.target_id is not None or self.collection is not None:
                raise ValueError("providerstatus heeft ongeldige autoriteit")
        elif any(
            value is not None
            for value in (self.target_id, self.collection, self.provider_id)
        ):
            raise ValueError("workerstatus mag geen autoriteit hebben")


class SnapshotSensorEventHub:
    """One HA event subscription shared by all candidates for one cache."""

    __slots__ = ("cache", "_entities", "_hass", "_unsubscribe")

    def __init__(self, cache: OperationalSnapshotCache) -> None:
        if not isinstance(cache, OperationalSnapshotCache):
            raise TypeError("cache moet OperationalSnapshotCache zijn")
        self.cache = cache
        self._entities: dict[int, SnapshotOnlySensorEntity] = {}
        self._hass = None
        self._unsubscribe: Callable[[], None] | None = None

    @callback
    def attach(self, entity: "SnapshotOnlySensorEntity") -> Callable[[], None]:
        """Attach one entity and return an idempotent removal callback."""
        hass = entity.hass
        if hass is None:
            raise RuntimeError(
                "snapshotsensor is nog niet aan Home Assistant gekoppeld"
            )
        if self._hass is not None and self._hass is not hass:
            raise RuntimeError("snapshot-eventhub mag maar bij één HA-instance horen")
        if self._unsubscribe is None:
            self._hass = hass
            self._unsubscribe = hass.bus.async_listen(
                OPERATIONAL_SNAPSHOT_UPDATE_EVENT,
                self._handle_snapshot_update,
            )
        key = id(entity)
        self._entities[key] = entity
        active = True

        @callback
        def detach() -> None:
            nonlocal active
            if not active:
                return
            active = False
            self._entities.pop(key, None)
            if self._entities or self._unsubscribe is None:
                return
            unsubscribe = self._unsubscribe
            self._unsubscribe = None
            self._hass = None
            unsubscribe()

        return detach

    @callback
    def _handle_snapshot_update(self, _event: Any) -> None:
        """Refresh each bounded entity projection after one atomic cache swap."""
        for entity in tuple(self._entities.values()):
            entity._refresh_from_snapshot(write_state=True)


class SnapshotOnlySensorEntity(SensorEntity):
    """Read-only, event-driven candidate entity backed by one pure view."""

    _attr_should_poll = False

    def __init__(
        self,
        hub: SnapshotSensorEventHub,
        *,
        unique_id_prefix: str,
        description: SnapshotSensorDescription,
    ) -> None:
        if not isinstance(hub, SnapshotSensorEventHub):
            raise TypeError("hub moet SnapshotSensorEventHub zijn")
        prefix = _nonempty_text(unique_id_prefix, "unique_id_prefix")
        if not isinstance(description, SnapshotSensorDescription):
            raise TypeError("description moet SnapshotSensorDescription zijn")
        self._hub = hub
        self._description = description
        self._view = SnapshotSensorView(
            native_value=None,
            available=True,
            attributes={},
        )
        self._attr_name = description.name
        self._attr_unique_id = f"{prefix}_snapshot_{description.key}"
        self._refresh_from_snapshot(write_state=False)

    @property
    def native_value(self) -> str | int | float | None:
        return self._view.native_value

    @property
    def available(self) -> bool:
        return self._view.available

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return self._view.attribute_copy()

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_on_remove(self._hub.attach(self))
        if self._description.kind == _KIND_WORKER:
            self.async_on_remove(self.hass.bus.async_listen(
                WORKER_STATUS_UPDATE_EVENT,
                self._handle_worker_status_update,
            ))
        self._refresh_from_snapshot(write_state=False)

    @callback
    def _handle_worker_status_update(self, _event: Any) -> None:
        """Refresh live worker diagnostics between snapshot publications."""

        self._refresh_from_snapshot(write_state=True)

    def _project(self) -> SnapshotSensorView:
        description = self._description
        cache = self._hub.cache
        if description.kind == _KIND_WORKER:
            projected = worker_status_view(cache)
        elif description.kind == _KIND_PRECIPITATION:
            projected = target_precipitation_view(cache, description.target_id)
        elif description.kind == _KIND_PROVIDER:
            projected = provider_status_view(cache, description.provider_id)
        else:
            projected = collection_count_view(cache, description.collection)
        return self._fail_closed(projected)

    def _fail_closed(self, projected: SnapshotSensorView) -> SnapshotSensorView:
        """Apply presentation safety only; never derive a weather fact."""
        attributes = projected.attribute_copy()
        attributes.update({
            "candidate_contract": CANDIDATE_CONTRACT,
            "candidate_sensor_key": self._description.key,
            "runtime_mode": RUNTIME_MODE_WORKER_ONLY,
            "ha_attribute_budget_bytes": HA_ATTRIBUTE_WARNING_BYTES,
            "ha_attribute_budget_exceeded": False,
        })
        if self._description.kind == _KIND_WORKER:
            runtime_attributes = _worker_runtime_attribute_projection(
                getattr(self, "hass", None)
            )
            heartbeat_state = runtime_attributes.get("worker_heartbeat_state")
            attributes["snapshot_worker_state"] = attributes.get("worker_state")
            attributes.update(runtime_attributes)
            return _enforce_ha_attribute_budget(SnapshotSensorView(
                native_value=(
                    heartbeat_state
                    if heartbeat_state is not None
                    else projected.native_value
                ),
                available=projected.available,
                attributes=attributes,
            ), self._description)
        if self._description.target_id is not None:
            attributes["configured_target_id"] = self._description.target_id
        if self._description.provider_id is not None:
            attributes["configured_provider_id"] = self._description.provider_id

        if self._description.kind == _KIND_PRECIPITATION:
            snapshot_current = attributes.get("snapshot_availability") == "current"
            fact_current = attributes.get("current_freshness") == "current"
            state_known = attributes.get("current_precipitation_state") in {
                "dry",
                "raining",
            }
            valid_current = snapshot_current and fact_current and state_known
            zero_radar_observation_current = (
                snapshot_current
                and fact_current
                and attributes.get("current_precipitation_state") == "unknown"
                and _ZERO_RADAR_OBSERVATION_REASON
                in attributes.get("current_reason_codes", [])
                and bool(attributes.get("current_provider_ids"))
            )
            undetect_radar_observation_current = (
                snapshot_current
                and fact_current
                and attributes.get("current_precipitation_state") == "unknown"
                and _UNDETECT_RADAR_OBSERVATION_REASON
                in attributes.get("current_reason_codes", [])
                and bool(attributes.get("current_provider_ids"))
            )
            nonpositive_radar_observation_current = (
                zero_radar_observation_current
                or undetect_radar_observation_current
            )
            reflectivity_echo_current = (
                snapshot_current
                and fact_current
                and attributes.get("current_precipitation_state") == "unknown"
                and _REFLECTIVITY_ECHO_REASON
                in attributes.get("current_reason_codes", [])
                and bool(attributes.get("current_provider_ids"))
            )
            reasons = []
            if not snapshot_current:
                reasons.append("snapshot_not_current")
            if not fact_current:
                reasons.append("current_precipitation_not_current")
            if (
                not state_known
                and not nonpositive_radar_observation_current
                and not reflectivity_echo_current
            ):
                reasons.append("current_precipitation_unknown")
            if nonpositive_radar_observation_current:
                reasons.append("dry_claim_withheld_requires_independent_truth")
            attributes["current_presentation_valid"] = valid_current
            attributes["zero_radar_observation_current"] = (
                zero_radar_observation_current
            )
            attributes["undetect_radar_observation_current"] = (
                undetect_radar_observation_current
            )
            attributes["nonpositive_radar_observation_current"] = (
                nonpositive_radar_observation_current
            )
            attributes["reflectivity_echo_current"] = reflectivity_echo_current
            attributes["presentation_reason_codes"] = reasons
            if not valid_current:
                attributes["current_precipitation_state"] = "unknown"
                attributes["rain_rate_mm_h"] = None
            ground_current = (
                snapshot_current
                and attributes.get("ground_freshness") == "current"
            )
            attributes["ground_presentation_valid"] = ground_current
            if not ground_current:
                attributes["ground_status"] = "onbekend"
                attributes["ground_confirmation"] = None
                attributes["ground_same_track"] = None
            threat_current = (
                snapshot_current
                and attributes.get("next_threat_freshness") == "current"
                and attributes.get("next_threat_state") in {"none", "available"}
            )
            attributes["next_threat_presentation_valid"] = threat_current
            if not snapshot_current:
                attributes.update({
                    "next_threat_state": "unknown",
                    "next_threat": "onbekend",
                    "next_threat_track_id": None,
                    "next_threat_probability_percent": None,
                    "next_threat_probability_calibration_id": None,
                    "next_threat_eta_minutes": None,
                    "next_threat_distance_km": None,
                    "next_threat_passage_status": "unknown",
                })
            return _enforce_ha_attribute_budget(SnapshotSensorView(
                native_value=(
                    projected.native_value
                    if valid_current
                    else (
                        _REFLECTIVITY_ECHO_STATE
                        if reflectivity_echo_current
                        else (
                            _ZERO_RADAR_OBSERVATION_STATE
                            if nonpositive_radar_observation_current
                            else "onvoldoende_data"
                        )
                    )
                ),
                available=True,
                attributes=attributes,
            ), self._description)

        if self._description.kind == _KIND_COUNT:
            snapshot_current = attributes.get("snapshot_availability") == "current"
            collection_complete = attributes.get("complete") is True
            provenance_complete = attributes.get("provenance_complete") is True
            region_assignment_pending = (
                self._description.collection == "regions"
                and self._hub.cache.get_region(_PENDING_REGION_ID) is not None
            )
            total_item_count = projected.native_value
            bounded_item_count = attributes.get(
                "bounded_item_count", total_item_count
            )
            omitted_item_count = attributes.get("overflow_count", 0)
            active_region_count = total_item_count
            if (
                self._description.collection == "regions"
                and type(bounded_item_count) is int
                and region_assignment_pending
            ):
                active_region_count = max(0, bounded_item_count - 1)
            # A region count describes the worker's complete target topology;
            # unlike an empty storm collection, it is not an absence-of-weather
            # claim.  It therefore remains valid while value fusion is still
            # collecting provenance for the meteorological facts themselves.
            provenance_required = self._description.collection == "ui_storms"
            exact_count_available = collection_complete or (
                self._description.collection == "ui_storms"
                and type(total_item_count) is int
                and type(bounded_item_count) is int
                and type(omitted_item_count) is int
                and omitted_item_count > 0
                and total_item_count
                == bounded_item_count + omitted_item_count
            )
            count_valid = (
                snapshot_current
                and exact_count_available
                and (provenance_complete or not provenance_required)
                and (
                    self._description.collection != "regions"
                    or not region_assignment_pending
                    or active_region_count > 0
                )
            )
            reasons = []
            if not snapshot_current:
                reasons.append("snapshot_not_current")
            if not collection_complete:
                reasons.append("collection_incomplete")
            if provenance_required and not provenance_complete:
                reasons.append("provenance_incomplete")
            if region_assignment_pending and not count_valid:
                reasons.append("region_assignment_pending")
            attributes.update({
                "bounded_snapshot_item_count": bounded_item_count,
                "omitted_snapshot_item_count": omitted_item_count,
                "total_item_count": total_item_count,
                "active_region_count": active_region_count,
                "count_valid": count_valid,
                "provenance_required": provenance_required,
                "region_assignment_pending": region_assignment_pending,
                "presentation_reason_codes": reasons,
            })
            return _enforce_ha_attribute_budget(SnapshotSensorView(
                native_value=active_region_count if count_valid else None,
                available=True,
                attributes=attributes,
            ), self._description)

        if self._description.kind == _KIND_PROVIDER:
            snapshot_current = attributes.get("snapshot_availability") == "current"
            attributes["provider_presentation_valid"] = snapshot_current
            attributes["presentation_reason_codes"] = (
                [] if snapshot_current else ["snapshot_not_current"]
            )
            return _enforce_ha_attribute_budget(SnapshotSensorView(
                native_value=(projected.native_value if snapshot_current else "unknown"),
                available=True,
                attributes=attributes,
            ), self._description)

        return _enforce_ha_attribute_budget(SnapshotSensorView(
            native_value=projected.native_value,
            available=projected.available,
            attributes=attributes,
        ), self._description)

    @callback
    def _refresh_from_snapshot(self, *, write_state: bool) -> None:
        self._view = self._project()
        if write_state:
            self.async_write_ha_state()


class SnapshotWorkerStatusSensor(SnapshotOnlySensorEntity):
    def __init__(self, hub: SnapshotSensorEventHub, unique_id_prefix: str) -> None:
        super().__init__(
            hub,
            unique_id_prefix=unique_id_prefix,
            description=SnapshotSensorDescription(
                key="worker_status",
                name="STV3 Worker Status",
                kind=_KIND_WORKER,
            ),
        )
        # Preserve the public entity identity used by the legacy runtime and
        # existing dashboards.  Worker-only and legacy are mutually exclusive,
        # so the active implementation can safely take over this registry row.
        self._attr_unique_id = "stv3_worker_status"


class SnapshotHomePrecipitationSensor(SnapshotOnlySensorEntity):
    def __init__(
        self,
        hub: SnapshotSensorEventHub,
        unique_id_prefix: str,
        home_target_id: str,
    ) -> None:
        super().__init__(
            hub,
            unique_id_prefix=unique_id_prefix,
            description=SnapshotSensorDescription(
                key="home_precipitation",
                name="Storm Tracker thuis neerslag",
                kind=_KIND_PRECIPITATION,
                target_id=home_target_id,
            ),
        )


class SnapshotTargetPrecipitationSensor(SnapshotOnlySensorEntity):
    def __init__(
        self,
        hub: SnapshotSensorEventHub,
        unique_id_prefix: str,
        target_id: str,
        target_name: str,
    ) -> None:
        target = _nonempty_text(target_id, "target_id")
        name = _nonempty_text(target_name, "target_name")
        super().__init__(
            hub,
            unique_id_prefix=unique_id_prefix,
            description=SnapshotSensorDescription(
                key=f"target_precipitation_{target}",
                name=f"Storm Tracker {name} neerslag",
                kind=_KIND_PRECIPITATION,
                target_id=target,
            ),
        )


class SnapshotProviderStatusSensor(SnapshotOnlySensorEntity):
    def __init__(
        self,
        hub: SnapshotSensorEventHub,
        unique_id_prefix: str,
        provider_id: str,
    ) -> None:
        provider = _nonempty_text(provider_id, "provider_id")
        super().__init__(
            hub,
            unique_id_prefix=unique_id_prefix,
            description=SnapshotSensorDescription(
                key=f"provider_{provider}",
                name=f"Storm Tracker bron {provider}",
                kind=_KIND_PROVIDER,
                provider_id=provider,
            ),
        )


class SnapshotStormCountSensor(SnapshotOnlySensorEntity):
    def __init__(self, hub: SnapshotSensorEventHub, unique_id_prefix: str) -> None:
        super().__init__(
            hub,
            unique_id_prefix=unique_id_prefix,
            description=SnapshotSensorDescription(
                key="storm_count",
                name="Storm Tracker stormen",
                kind=_KIND_COUNT,
                collection="ui_storms",
            ),
        )


class SnapshotRegionCountSensor(SnapshotOnlySensorEntity):
    def __init__(self, hub: SnapshotSensorEventHub, unique_id_prefix: str) -> None:
        super().__init__(
            hub,
            unique_id_prefix=unique_id_prefix,
            description=SnapshotSensorDescription(
                key="region_count",
                name="Storm Tracker regio's",
                kind=_KIND_COUNT,
                collection="regions",
            ),
        )


def build_snapshot_sensor_candidates(
    cache: OperationalSnapshotCache,
    *,
    unique_id_prefix: str,
    home_target_id: str,
    additional_targets: Mapping[str, str] | None = None,
    provider_ids: tuple[str, ...] = (),
) -> list[SnapshotOnlySensorEntity]:
    """Build candidates only; this function never registers them with HA."""
    hub = SnapshotSensorEventHub(cache)
    prefix = _nonempty_text(unique_id_prefix, "unique_id_prefix")
    home = _nonempty_text(home_target_id, "home_target_id")
    if additional_targets is not None and not isinstance(
        additional_targets,
        Mapping,
    ):
        raise TypeError("additional_targets moet een mapping zijn")
    if (
        additional_targets is not None
        and len(additional_targets) > MAX_ADDITIONAL_TARGET_SENSORS
    ):
        raise ValueError("te veel extra targets voor snapshotsensoren")
    targets = dict(additional_targets or {})
    if home in targets:
        raise ValueError("home_target_id mag niet opnieuw als extra target staan")
    for target_id, target_name in targets.items():
        _nonempty_text(target_id, "target_id")
        _nonempty_text(target_name, "target_name")
    if not isinstance(provider_ids, tuple):
        raise TypeError("provider_ids moet een tuple zijn")
    providers = tuple(_nonempty_text(value, "provider_id") for value in provider_ids)
    if providers != tuple(sorted(set(providers))):
        raise ValueError("provider_ids moet uniek en gesorteerd zijn")

    entities: list[SnapshotOnlySensorEntity] = [
        SnapshotWorkerStatusSensor(hub, prefix),
        SnapshotHomePrecipitationSensor(hub, prefix, home),
    ]
    for target_id in sorted(targets):
        entities.append(
            SnapshotTargetPrecipitationSensor(
                hub,
                prefix,
                target_id,
                targets[target_id],
            )
        )
    for provider_id in providers:
        entities.append(SnapshotProviderStatusSensor(hub, prefix, provider_id))
    entities.extend((
        SnapshotStormCountSensor(hub, prefix),
        SnapshotRegionCountSensor(hub, prefix),
    ))
    return entities


__all__ = [
    "CANDIDATE_CONTRACT",
    "HA_ATTRIBUTE_BUDGET_REASON",
    "HA_ATTRIBUTE_WARNING_BYTES",
    "MAX_ADDITIONAL_TARGET_SENSORS",
    "SnapshotHomePrecipitationSensor",
    "SnapshotOnlySensorEntity",
    "SnapshotProviderStatusSensor",
    "SnapshotRegionCountSensor",
    "SnapshotSensorDescription",
    "SnapshotSensorEventHub",
    "SnapshotStormCountSensor",
    "SnapshotTargetPrecipitationSensor",
    "SnapshotWorkerStatusSensor",
    "WORKER_ONLY_PROVIDER_IDS",
    "build_snapshot_sensor_candidates",
]
