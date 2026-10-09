"""Small operational pipeline: select, fetch, fuse, nowcast and persist."""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
import time
from typing import Any, Callable, Mapping

from .desired_state import METHOD, DesiredState, DesiredStateConflict, parse_desired_state
from .thin_fusion import FusionCycle, fuse_observations
from .thin_nowcast import ThinNowcast, ThinNowcastRuntime
from .thin_postgis import PostGISWriteResult
from .thin_provider_catalog import PROVIDERS, build_due_requests
from .tracking_core.operational_snapshot import MAX_UI_STORM_SUMMARIES, validate_snapshot


SNAPSHOT_TTL_SECONDS = 120.0
MAX_GEOJSON_PAYLOAD_BYTES = 900_000
SNAPSHOT_REVISION_EPOCH_SECONDS = 1_577_836_800  # 2020-01-01T00:00:00Z
SNAPSHOT_PROVIDER_IDS = {
    "kmi_radar": "radar.kmi.radqpe_native",
    "opera_radar": "radar.opera.dbzh",
    "knmi_radar": "radar.knmi.rtcor",
    "dwd_radolan": "radar.dwd.radolan_rv",
    "open_meteo": "model.openmeteo.region",
    "metar": "ground.metar.global",
    "netatmo": "ground.netatmo.region",
    "blitzortung": "lightning.blitzortung.event_ingress",
    "aemet_radar": "radar.aemet.composite",
}


def _snapshot_provider_id(provider_id: str) -> str:
    return SNAPSHOT_PROVIDER_IDS.get(provider_id, provider_id)


@dataclass(frozen=True, slots=True)
class ThinCycleResult:
    snapshot: Mapping[str, Any]
    fusion: FusionCycle
    nowcast: ThinNowcast
    postgis: PostGISWriteResult


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
        self._nowcast = ThinNowcastRuntime(targets={})
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
            self._nowcast.update_targets({
                item.target_id: (item.latitude, item.longitude)
                for item in candidate.targets if item.available
            })
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

    async def run_cycle(self) -> ThinCycleResult:
        state = self._state
        if state is None:
            raise RuntimeError("desired_state_unconfigured")
        now = float(self._clock())
        requests = build_due_requests(state, now=now, last_attempts=self._last_attempts)
        for request in requests:
            self._last_attempts[request.provider_id] = now
        fresh_results = dict(await self._providers.fetch(requests))
        self._latest_provider_results.update(fresh_results)
        fusion = fuse_observations(
            self._latest_provider_results,
            evaluated_at=float(self._clock()),
            target_ids=tuple(item.target_id for item in state.targets if item.available),
        )
        nowcast = self._nowcast.advance(fusion)
        self._latest_geojson = self._build_geojson(fusion, nowcast)
        if self._postgis is None:
            postgis = PostGISWriteResult(
                status="disabled",
                last_error_code=None,
                written_rows={},
                queued_cycles=0,
                dropped_cycles=0,
            )
        else:
            postgis = await self._postgis.write_cycle(fusion, nowcast)
        self._revision += 1
        snapshot = self._build_snapshot(fusion, nowcast, postgis)
        return ThinCycleResult(snapshot=snapshot, fusion=fusion, nowcast=nowcast, postgis=postgis)

    def _build_geojson(
        self,
        fusion: FusionCycle,
        nowcast: ThinNowcast,
    ) -> dict[str, Any]:
        assert self._state is not None
        target_features: list[dict[str, Any]] = []
        cell_feature_groups: list[tuple[float, str, list[dict[str, Any]]]] = []
        lightning_features: list[dict[str, Any]] = []
        region_id = "thin-operational"
        for target in self._state.targets:
            if not target.available:
                continue
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
                    "engine_id": region_id,
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
    ) -> dict[str, Any]:
        assert self._state is not None
        state = self._state
        evaluated_at = float(fusion.evaluated_at)
        observations = fusion.observations
        product_valid_at = max((item.observed_at for item in observations), default=None)
        received_at = max((item.received_at for item in observations), default=None)
        source_ids = sorted(
            {_snapshot_provider_id(item.provider_id) for item in observations}
        )

        provider_items: list[dict[str, Any]] = []
        for spec in PROVIDERS:
            applicable = any(spec.applies_to(target) for target in state.targets if target.available)
            if not applicable:
                continue
            result = self._latest_provider_results.get(spec.provider_id)
            if result is None:
                provider_state, readiness, freshness = "unknown", "not_ready", "unknown"
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
                elif result.status in {"error", "backoff"}:
                    provider_state, freshness = "degraded", "unknown"
                else:
                    provider_state, freshness = "unknown", "unknown"
                valid_at = max((item.observed_at for item in result.observations), default=None)
                receipt = max((item.received_at for item in result.observations), default=None)
                error = result.reason_code if result.status == "error" else None
                reasons = [result.reason_code] if result.reason_code else []
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
        region_items: list[dict[str, Any]] = []
        for target in sorted((item for item in state.targets if item.available), key=lambda item: item.target_id):
            result = nowcast.target_results[target.target_id]
            region_id = f"region-{target.target_id}"
            providers = sorted(
                {_snapshot_provider_id(item) for item in result.contributors}
            )
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
                track_id = nowcast.storm_cells[0].cell_id if nowcast.storm_cells else None
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
                    "product_valid_at": product_valid_at if current_freshness == "current" else None,
                    "provider_ids": providers,
                    "reason_codes": [result.reason_code],
                    "ground_evidence": {
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
                    },
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
                    "product_valid_at": product_valid_at if threat_freshness == "current" else None,
                    "provider_ids": providers,
                    "reason_codes": [result.reason_code],
                },
            })
            applicable_ids = sorted(
                _snapshot_provider_id(spec.provider_id)
                for spec in PROVIDERS
                if spec.applies_to(target)
            )
            region_items.append({
                "region_id": region_id,
                "display_name": target.display_name,
                "state": "current" if providers else "unknown",
                "provider_ids": applicable_ids,
                "target_ids": [target.target_id],
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
        }
        return validate_snapshot(snapshot)


__all__ = [
    "MAX_GEOJSON_PAYLOAD_BYTES",
    "ThinCycleResult",
    "ThinOperationalRuntime",
]
