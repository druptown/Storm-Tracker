"""Storm Tracker V3 — sensor.py v0.2.0

Sensoren die de ruwe provider-data tonen.
Nog geen fusion, geen projectie — puur data zichtbaar maken.

Versiegeschiedenis:
  v0.2.0 — storm sensoren toegevoegd (StormTellerSensor, StormDetailSensor)
  v0.1.0 — eerste versie; provider sensoren

Sensoren:
  sensor.stv3_blitzortung_inslagen   — aantal blikseminslagen (60min buffer)
  sensor.stv3_blitzortung_laatste    — lat/lon/timestamp van laatste inslag
  sensor.stv3_kmi_observaties        — aantal KMI-radarobservaties
  sensor.stv3_kmi_intensiteit        — gemiddelde intensiteit KMI
  sensor.stv3_rainviewer_observaties — aantal RainViewer-observaties
  sensor.stv3_netatmo_stations       — totaal Netatmo-stations in bereik
  sensor.stv3_netatmo_regen          — stations met meetbare neerslag
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import time
from collections.abc import Mapping
from datetime import datetime, timezone

from homeassistant.components.sensor import SensorEntity, SensorStateClass
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    DATA_ENTRY_RUNTIMES,
    DOMAIN,
    RUNTIME_MODE_LEGACY,
    RUNTIME_MODE_WORKER_ONLY,
)

_LOGGER = logging.getLogger(__name__)
_LEGACY_DEPENDENCIES_LOADED = False


def _load_legacy_sensor_dependencies() -> None:
    """Import legacy compute helpers only for the legacy discovery platform."""

    global _LEGACY_DEPENDENCIES_LOADED
    if _LEGACY_DEPENDENCIES_LOADED:
        return
    from .engine.nowcast import (
        build_precipitation_status as _build_precipitation_status,
        stabilize_eta_continuity as _stabilize_eta_continuity,
    )
    from .engine.tracker_forecast_contract import (
        CURRENT_FOOTPRINT_DISTANCE_TOLERANCE_KM as _CURRENT_FOOTPRINT_DISTANCE_TOLERANCE_KM,
        CURRENT_FOOTPRINT_FRESHNESS_LIMIT_SECONDS as _CURRENT_FOOTPRINT_FRESHNESS_LIMIT_SECONDS,
        CURRENT_FOOTPRINT_FRESHNESS_POLICY_ID as _CURRENT_FOOTPRINT_FRESHNESS_POLICY_ID,
        GROUND_FRESHNESS_LIMITS_SECONDS as _GROUND_FRESHNESS_LIMITS_SECONDS,
        GROUND_FRESHNESS_POLICY_ID as _GROUND_FRESHNESS_POLICY_ID,
        CurrentFootprintEvidence as _CurrentFootprintEvidence,
        CurrentGroundEvidence as _CurrentGroundEvidence,
        GroundEvidenceKind as _GroundEvidenceKind,
        SourceRecordIdKind as _SourceRecordIdKind,
        build_tracker_forecast_shadow as _build_tracker_forecast_shadow,
    )
    from .engine.netatmo_local import (
        build_local_netatmo_summary as _build_local_netatmo_summary,
    )
    from .engine.geojson import (
        build_feature_collection as _build_feature_collection,
    )
    from .geojson_worker import (
        async_refresh_worker_geojson as _async_refresh_worker_geojson,
    )

    globals().update({
        "CURRENT_FOOTPRINT_DISTANCE_TOLERANCE_KM": _CURRENT_FOOTPRINT_DISTANCE_TOLERANCE_KM,
        "CURRENT_FOOTPRINT_FRESHNESS_LIMIT_SECONDS": _CURRENT_FOOTPRINT_FRESHNESS_LIMIT_SECONDS,
        "CURRENT_FOOTPRINT_FRESHNESS_POLICY_ID": _CURRENT_FOOTPRINT_FRESHNESS_POLICY_ID,
        "GROUND_FRESHNESS_LIMITS_SECONDS": _GROUND_FRESHNESS_LIMITS_SECONDS,
        "GROUND_FRESHNESS_POLICY_ID": _GROUND_FRESHNESS_POLICY_ID,
        "CurrentFootprintEvidence": _CurrentFootprintEvidence,
        "CurrentGroundEvidence": _CurrentGroundEvidence,
        "GroundEvidenceKind": _GroundEvidenceKind,
        "SourceRecordIdKind": _SourceRecordIdKind,
        "async_refresh_worker_geojson": _async_refresh_worker_geojson,
        "build_local_netatmo_summary": _build_local_netatmo_summary,
        "build_feature_collection": _build_feature_collection,
        "build_precipitation_status": _build_precipitation_status,
        "build_tracker_forecast_shadow": _build_tracker_forecast_shadow,
        "stabilize_eta_continuity": _stabilize_eta_continuity,
    })
    _LEGACY_DEPENDENCIES_LOADED = True


def _current_footprint_evidence(
    storms,
    target_lat,
    target_lon,
    *,
    tracker_id,
    radar_source,
    radar_covered,
    evaluated_at=None,
):
    """Scan exact current-frame contours without using rounded UI distance."""

    if radar_covered is False:
        return None
    if not isinstance(tracker_id, str) or not tracker_id:
        return None
    source_id = (
        radar_source.strip()
        if isinstance(radar_source, str) and radar_source.strip()
        else None
    )
    if source_id is None:
        return None
    evaluated_time = time.time() if evaluated_at is None else evaluated_at
    if (
        isinstance(evaluated_time, bool)
        or not isinstance(evaluated_time, (int, float))
        or not math.isfinite(evaluated_time)
        or evaluated_time < 0.0
    ):
        return None
    input_storms = tuple(storms or ())
    scan_rows = []
    timestamps = []
    for storm in input_storms:
        storm_id = str(getattr(storm, "storm_id", "") or "")
        cell_timestamps = []
        for cell in getattr(storm, "radar_cells", {}).values():
            try:
                timestamp = float(cell.timestamp)
            except (TypeError, ValueError, AttributeError):
                continue
            if math.isfinite(timestamp) and timestamp >= 0.0:
                cell_timestamps.append(timestamp)
                timestamps.append(timestamp)
        scan_rows.append({
            "storm_id": storm_id,
            "cell_timestamps": sorted(cell_timestamps),
        })
    latest_timestamp = max(timestamps, default=None)
    if latest_timestamp is None:
        return None
    frame_age = float(evaluated_time) - latest_timestamp
    if frame_age < 0.0:
        return None
    minimum_timestamp = latest_timestamp - 60.0
    evaluated = []
    for storm in input_storms:
        try:
            point = storm.closest_radar_point(
                float(target_lat),
                float(target_lon),
                min_timestamp=minimum_timestamp,
            )
        except (AttributeError, TypeError, ValueError):
            continue
        if point is None:
            continue
        try:
            distance = float(point[0])
        except (TypeError, ValueError, IndexError):
            continue
        if not math.isfinite(distance) or distance < 0.0:
            continue
        storm_id = str(getattr(storm, "storm_id", "") or "")
        if not storm_id:
            continue
        evaluated.append((distance, storm_id))
    evaluated.sort(key=lambda item: (item[0], item[1]))
    minimum_distance, nearest_storm_id = (
        evaluated[0] if evaluated else (None, None)
    )
    evaluated_ids = tuple(sorted({storm_id for _, storm_id in evaluated}))
    containing_ids = tuple(sorted({
        storm_id
        for distance, storm_id in evaluated
        if distance <= CURRENT_FOOTPRINT_DISTANCE_TOLERANCE_KM
    }))
    scan_sha256 = hashlib.sha256(
        json.dumps(
            scan_rows,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    evidence_id = (
        f"legacy-current-footprint:{tracker_id}:{source_id}:"
        f"{latest_timestamp:.6f}:{scan_sha256[:24]}"
    )
    return CurrentFootprintEvidence(
        evidence_id=evidence_id,
        tracker_id=tracker_id,
        tracker_latitude=float(target_lat),
        tracker_longitude=float(target_lon),
        frame_valid_at=latest_timestamp,
        evaluated_at=float(evaluated_time),
        frame_age_seconds=frame_age,
        freshness_limit_seconds=(
            CURRENT_FOOTPRINT_FRESHNESS_LIMIT_SECONDS
        ),
        freshness_policy_id=CURRENT_FOOTPRINT_FRESHNESS_POLICY_ID,
        fresh=(
            frame_age <= CURRENT_FOOTPRINT_FRESHNESS_LIMIT_SECONDS
        ),
        source_id=source_id,
        source_algorithm_version="legacy-current-frame-polygon-scan-v2",
        input_storm_count=len(input_storms),
        scan_complete=len(evaluated_ids) == len(input_storms),
        scan_sha256=scan_sha256,
        evaluated_storm_ids=evaluated_ids,
        containing_storm_ids=containing_ids,
        nearest_storm_id=nearest_storm_id,
        minimum_distance_km=minimum_distance,
    )


def _current_ground_evidence(
    summary,
    target_id,
    target_lat,
    target_lon,
    *,
    evaluated_at=None,
):
    """Build typed positive evidence from the local ground assessment rows.

    The aggregate ``ground_precipitation_observed`` flag is necessary but not
    sufficient: every admitted row must independently retain source, physical
    site, freshness, age and tracker distance.  This prevents a confidence
    score, duplicate station or stale wet flag from becoming current rain.
    """

    if not isinstance(summary, Mapping):
        return ()
    if summary.get("ground_precipitation_observed") is not True:
        return ()
    if not isinstance(target_id, str) or not target_id:
        return ()
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        for value in (target_lat, target_lon)
    ):
        return ()
    if not -90.0 <= float(target_lat) <= 90.0:
        return ()
    if not -180.0 <= float(target_lon) <= 180.0:
        return ()
    raw_assessments = summary.get("ground_assessments")
    radius = summary.get("ground_local_radius_km")
    summary_evaluated_at = summary.get("ground_evaluated_at")
    if (
        not isinstance(raw_assessments, (list, tuple))
        or isinstance(radius, bool)
        or not isinstance(radius, (int, float))
        or not math.isfinite(radius)
        or radius < 0.0
        or isinstance(summary_evaluated_at, bool)
        or not isinstance(summary_evaluated_at, (int, float))
        or not math.isfinite(summary_evaluated_at)
        or summary_evaluated_at < 0.0
        or summary.get("ground_freshness_policy_id")
        != GROUND_FRESHNESS_POLICY_ID
    ):
        return ()
    evaluated = time.time() if evaluated_at is None else evaluated_at
    if (
        isinstance(evaluated, bool)
        or not isinstance(evaluated, (int, float))
        or not math.isfinite(evaluated)
        or evaluated < 0.0
    ):
        return ()
    if float(summary_evaluated_at) > float(evaluated) + 1e-6:
        return ()

    provider_limits = dict(GROUND_FRESHNESS_LIMITS_SECONDS)
    candidates = []
    for assessment in raw_assessments:
        if not isinstance(assessment, Mapping):
            continue
        if (
            assessment.get("wet") is not True
            or assessment.get("fresh") is not True
            or assessment.get("counts_as_independent") is not True
        ):
            continue
        provider = assessment.get("provider")
        station_id = assessment.get("station_id")
        physical_site_id = assessment.get("physical_site_id")
        physical_site_basis = assessment.get("physical_site_basis")
        source_record_id = assessment.get("source_record_id")
        source_record_id_kind = assessment.get("source_record_id_kind")
        if not all(
            isinstance(value, str) and value
            for value in (
                provider,
                station_id,
                physical_site_id,
                physical_site_basis,
                source_record_id,
                source_record_id_kind,
            )
        ):
            continue
        provider = provider.strip().lower()
        expected_limit = provider_limits.get(provider)
        if expected_limit is None:
            continue
        if (
            physical_site_basis
            not in {
                "provider_identity",
                "proximity_conservative_dedup",
                "unverified_coordinate",
            }
            or assessment.get("freshness_policy_known") is not True
            or assessment.get("freshness_policy_id")
            != GROUND_FRESHNESS_POLICY_ID
        ):
            continue
        reported_limit = assessment.get("freshness_limit_seconds")
        observed_at = assessment.get("observed_at")
        reported_age = assessment.get("age_seconds")
        distance = assessment.get("distance_target_km")
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            for value in (
                reported_limit,
                observed_at,
                reported_age,
                distance,
            )
        ):
            continue
        if (
            float(observed_at) <= 0.0
            or float(distance) < 0.0
            or float(distance) > float(radius)
            or not math.isclose(
                float(reported_limit),
                float(expected_limit),
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        ):
            continue
        adapter_age = (
            float(summary_evaluated_at) - float(observed_at)
        )
        current_age = float(evaluated) - float(observed_at)
        if (
            adapter_age < 0.0
            or current_age < 0.0
            or current_age > float(expected_limit)
            or not math.isclose(
                adapter_age,
                float(reported_age),
                rel_tol=0.0,
                abs_tol=0.11,
            )
        ):
            continue
        try:
            record_id_kind = SourceRecordIdKind(source_record_id_kind)
        except (TypeError, ValueError):
            continue
        raw_storm_id = assessment.get("storm_id")
        matched_storm_id = (
            raw_storm_id
            if (
                isinstance(raw_storm_id, str)
                and raw_storm_id
                and assessment.get("storm_match_ambiguous") is not True
            )
            else None
        )
        evidence_digest = hashlib.sha256(
            "\n".join((
                target_id,
                provider,
                physical_site_id,
                source_record_id,
            )).encode("utf-8")
        ).hexdigest()
        evidence_id = (
            f"legacy-local-ground:{provider}:{evidence_digest}"
        )
        try:
            evidence = CurrentGroundEvidence(
                evidence_id=evidence_id,
                kind=GroundEvidenceKind.PRECIPITATION_PRESENCE,
                tracker_id=target_id,
                tracker_latitude=float(target_lat),
                tracker_longitude=float(target_lon),
                provider_id=provider,
                station_id=station_id,
                physical_site_id=physical_site_id,
                physical_site_basis=physical_site_basis,
                source_record_id=source_record_id,
                source_record_id_kind=record_id_kind,
                source_algorithm_version=(
                    "legacy-local-ground-assessment-v2"
                ),
                observed_at=float(observed_at),
                evaluated_at=float(evaluated),
                observation_age_seconds=current_age,
                freshness_limit_seconds=float(expected_limit),
                freshness_policy_id=GROUND_FRESHNESS_POLICY_ID,
                distance_to_tracker_km=float(distance),
                precipitation_present=True,
                fresh=True,
                measurement_value=None,
                measurement_unit=None,
                matched_storm_id=matched_storm_id,
            )
        except (TypeError, ValueError):
            continue
        candidates.append({
            "distance": float(distance),
            "physical_site_id": physical_site_id,
            "provider_station": (provider, station_id),
            "source_record_id": source_record_id,
            "evidence": evidence,
        })

    candidates.sort(
        key=lambda item: (
            item["distance"],
            item["evidence"].evidence_id,
        )
    )
    independent = []
    seen_physical_sites = set()
    seen_provider_stations = set()
    seen_source_records = set()
    for candidate in candidates:
        if (
            candidate["physical_site_id"] in seen_physical_sites
            or candidate["provider_station"] in seen_provider_stations
            or candidate["source_record_id"] in seen_source_records
        ):
            continue
        seen_physical_sites.add(candidate["physical_site_id"])
        seen_provider_stations.add(candidate["provider_station"])
        seen_source_records.add(candidate["source_record_id"])
        independent.append(candidate["evidence"])
    if not independent:
        return ()
    if (
        len(independent) < 2
        and independent[0].distance_to_tracker_km > 3.0
    ):
        return ()
    return tuple(sorted(independent, key=lambda evidence: evidence.evidence_id))


def _attach_tracker_forecast_shadow(
    summary,
    storms,
    target_id,
    target_lat,
    target_lon,
    *,
    radar_source,
    radar_covered,
):
    """Add shadow semantics without changing any legacy status field."""

    evaluated_at = time.time()
    footprint = _current_footprint_evidence(
        storms,
        target_lat,
        target_lon,
        tracker_id=target_id,
        radar_source=radar_source,
        radar_covered=radar_covered,
        evaluated_at=evaluated_at,
    )
    ground = _current_ground_evidence(
        summary,
        target_id,
        target_lat,
        target_lon,
        evaluated_at=evaluated_at,
    )
    summary["forecast_semantics_shadow"] = build_tracker_forecast_shadow(
        {**summary, "target_id": target_id},
        current_footprint=footprint,
        current_ground=ground,
    ).to_dict()
    return summary


def _netatmo_target_summary(
    data,
    engine_id,
    target_lat,
    target_lon,
    target_id="unknown",
    *,
    record_audit=False,
):
    if target_lat is None or target_lon is None:
        return build_local_netatmo_summary([], 0.0, 0.0)
    observations = (
        data.get("ground_observations_by_engine", {}).get(engine_id, [])
        if engine_id else []
    )
    manager = data.get("storm_manager")
    region = next(
        (
            candidate for candidate in manager.get_all_engines()
            if candidate.engine_id == engine_id
        ),
        None,
    ) if manager and engine_id else None
    storms = (
        region.storm_engine.get_active_storms() if region is not None else []
    )
    summary = build_local_netatmo_summary(
        observations, target_lat, target_lon, storms=storms
    )
    if not record_audit:
        return summary
    evaluated_at = time.time()
    decision_key = (
        f"{target_id}:{engine_id or 'none'}:{int(evaluated_at // 60)}"
    )
    quality_pending = data.get("ground_quality_assessments_pending")
    decision_pending = data.get("ground_confirmation_decisions_pending")
    audit_keys = data.setdefault("ground_audit_decision_keys", {})
    if not isinstance(audit_keys, dict):
        audit_keys = {}
        data["ground_audit_decision_keys"] = audit_keys
    cutoff = evaluated_at - 24 * 3600
    for key, timestamp in tuple(audit_keys.items()):
        if float(timestamp) < cutoff:
            audit_keys.pop(key, None)
    if decision_key in audit_keys:
        return summary
    if isinstance(quality_pending, list) and isinstance(decision_pending, list):
        audit_keys[decision_key] = evaluated_at
        for assessment in summary.get("ground_assessments", []):
            observation_key = (
                f"{assessment['provider']}:{engine_id}:"
                f"{assessment['station_id']}:{int(evaluated_at // 60)}"
            )
            quality_pending.append({
                "decision_key": decision_key,
                "observation_key": observation_key,
                "target_id": target_id,
                "region_key": engine_id or "none",
                "evaluated_at": evaluated_at,
                **assessment,
                "reasons": [
                    reason for reason, active in (
                        ("stale", not assessment["fresh"]),
                        ("outside_radius", assessment["distance_target_km"] > 10),
                        ("outside_storm_footprint", (
                            summary["ground_same_system_required"]
                            and assessment["storm_id"] is None
                        )),
                    ) if active
                ],
            })
        decision_pending.append({
            "decision_key": decision_key,
            "target_id": target_id,
            "region_key": engine_id or "none",
            "evaluated_at": evaluated_at,
            "confirmed": summary["ground_confirmed"],
            "status": summary["ground_status"],
            "confidence_percent": summary["ground_confidence_percent"],
            "score": summary["ground_confirmation_score"],
            "storm_ids": summary["ground_matched_storm_ids"],
            "confirmed_storm_ids": summary.get(
                "ground_confirmed_storm_ids", []
            ),
            "confirmation_by_storm": summary.get(
                "ground_confirmation_by_storm", {}
            ),
            "sources": summary["ground_raining_sources"],
            "snapshot": {
                key: value for key, value in summary.items()
                if key != "ground_assessments"
            },
        })
    return summary


def _apply_local_netatmo_confirmation(result, netatmo):
    """Laat bevestigde regen ter plaatse primeren boven een naderende ETA."""
    precipitation_observed = bool(
        netatmo.get("ground_precipitation_observed")
    )
    confirmed_ids = {
        str(storm_id)
        for storm_id in netatmo.get("ground_confirmed_storm_ids", [])
        if storm_id is not None
    }
    selected_storm_id = (
        result.get("tracked_system_storm_id")
        or result.get("storm_id")
    )
    same_system_required = bool(
        netatmo.get("ground_same_system_required")
    )
    selected_confirmation = (
        netatmo.get("ground_confirmation_by_storm", {}).get(
            str(selected_storm_id), {}
        )
        if selected_storm_id is not None
        else {}
    )
    system_confirmed = bool(
        netatmo.get("ground_confirmed")
        and (
            not same_system_required
            or (
                selected_storm_id is not None
                and str(selected_storm_id) in confirmed_ids
            )
        )
    )
    if not (system_confirmed or precipitation_observed):
        return
    selected_confidence = (
        selected_confirmation.get(
            "confidence_percent",
            netatmo.get("ground_confidence_percent", 0),
        )
        if system_confirmed
        else netatmo.get("ground_confidence_percent", 0)
    )
    result.update({
        "status": "bevestigd" if system_confirmed else "waargenomen",
        "selected_reason": (
            "lokale_grondmeting_binnen_weersysteem"
            if system_confirmed else "actuele_lokale_grondmeting"
        ),
        "distance_km": 0.0,
        "eta_minutes": 0.0,
        "eta_basis": (
            selected_confirmation.get(
                "confirmation_basis",
                netatmo.get(
                    "ground_confirmation_basis", "netatmo_grondmeting"
                ),
            )
            if system_confirmed
            else "lokale_grondwaarneming"
        ),
        "eta_reliable": system_confirmed,
        "moving_towards": None,
        "approach_speed_kmh": None,
        "footprint_pass_distance_km": 0.0,
        "footprint_pass_minutes": 0.0,
        "passage_classification": "actueel",
        "forecast_available": True,
        "forecast_block_reason": None,
        "forecast_horizon_minutes": 0,
        "forecast_intensity_label": "grondmeting",
        "forecast_intensity_dbz": None,
        "forecast_confidence_percent": selected_confidence,
        "ground_selected_system_confirmed": system_confirmed,
        "ground_selected_system_storm_id": selected_storm_id,
        "ground_selected_system_confirmation_score": (
            selected_confirmation.get("confirmation_score")
            if system_confirmed else None
        ),
        "ground_selected_system_confirmation_basis": (
            selected_confirmation.get("confirmation_basis")
            if system_confirmed else None
        ),
        "ground_confirmation_mismatch": bool(
            netatmo.get("ground_confirmed") and not system_confirmed
        ),
    })


def _lightning_only_summary(data, engine_id, target_lat, target_lon):
    """Vat recente bliksem samen wanneer geen neerslagsysteem beschikbaar is."""
    cutoff = time.time() - 15 * 60
    events = [
        item for item in data.get("recent_lightning", [])
        if float(item.get("timestamp", 0)) >= cutoff
        and engine_id in item.get("engine_ids", [])
    ]
    if not events:
        return None

    def distance(item):
        lat1, lon1 = math.radians(target_lat), math.radians(target_lon)
        lat2, lon2 = math.radians(float(item["lat"])), math.radians(float(item["lon"]))
        dlat, dlon = lat2 - lat1, lon2 - lon1
        value = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        return 6371.0088 * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))

    closest = min(events, key=distance)
    return {
        "status": "alleen_bliksem",
        "selected_reason": "lightning_only",
        "tracking_status": "alleen_bliksem",
        "distance_km": round(distance(closest), 1),
        "impact_lat": round(float(closest["lat"]), 4),
        "impact_lon": round(float(closest["lon"]), 4),
        "lightning_events_15m": len(events),
        "last_lightning_source": closest.get("source"),
    }


def _timestamp_iso(value):
    """Zet een optionele Unix-timestamp om naar een expliciete UTC-tijd."""
    if value is None:
        return None
    return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()


def _apply_source_transition(result: dict, transition: dict | None) -> None:
    """Publiceer de overgang en pas uitsluitend de geleerde onzekerheidsmarge toe."""
    if not transition:
        result["source_transition"] = None
        result["source_transition_active"] = False
        return
    result["source_transition"] = transition
    result["source_transition_active"] = True
    confidence = result.get("forecast_confidence_percent")
    if confidence is None:
        return
    penalty = int(transition.get("confidence_penalty_percent", 10))
    result["forecast_confidence_percent"] = max(0, confidence - penalty)


def _open_meteo_target_summary(data: dict, target_id: str) -> dict:
    """Publiceer modelbegeleiding zonder ze als radarwaarneming te behandelen."""
    if not data.get("open_meteo_enabled", False):
        return {
            "open_meteo_enabled": False,
            "open_meteo_status": "disabled",
        }
    aggregate = data.get("open_meteo_result", {})
    details = data.get("open_meteo_results_by_target", {}).get(target_id, {})
    return {
        "open_meteo_status": aggregate.get("provider_status", "initializing"),
        "open_meteo_last_success": aggregate.get("last_success_at"),
        "open_meteo_data_age_seconds": aggregate.get("data_age_seconds"),
        "open_meteo_current_mm": details.get("current_precipitation_mm"),
        "open_meteo_max_90m_mm": details.get("forecast_90m_max_mm"),
        "open_meteo_total_90m_mm": details.get("forecast_90m_total_mm"),
        "open_meteo_first_wet_minutes": details.get(
            "forecast_90m_first_wet_minutes"
        ),
        "open_meteo_wet_steps_90m": details.get("forecast_90m_wet_steps"),
        "open_meteo_max_3h_mm": details.get("forecast_3h_max_mm"),
        "open_meteo_total_3h_mm": details.get("forecast_3h_total_mm"),
        "open_meteo_precip_probability_max_6h": details.get(
            "precipitation_probability_max_6h_percent"
        ),
        "open_meteo_cape_max_3h_jkg": details.get("cape_max_3h_jkg"),
        "open_meteo_lightning_potential_max_3h": details.get(
            "lightning_potential_max_3h"
        ),
        "open_meteo_lifted_index_min_6h": details.get(
            "lifted_index_min_6h"
        ),
        "open_meteo_cin_min_6h_jkg": details.get(
            "convective_inhibition_min_6h_jkg"
        ),
        "open_meteo_wind_gusts_max_3h_kmh": details.get(
            "wind_gusts_max_3h_kmh"
        ),
        "open_meteo_wind_850hpa_speed_kmh": details.get(
            "wind_850hpa_speed_kmh"
        ),
        "open_meteo_wind_850hpa_direction_deg": details.get(
            "wind_850hpa_direction_deg"
        ),
        "open_meteo_wind_700hpa_speed_kmh": details.get(
            "wind_700hpa_speed_kmh"
        ),
        "open_meteo_wind_700hpa_direction_deg": details.get(
            "wind_700hpa_direction_deg"
        ),
        "open_meteo_pressure_msl_hpa": details.get("pressure_msl_hpa"),
        "open_meteo_convective_guidance_available": details.get(
            "convective_guidance_available", False
        ),
        "open_meteo_aloft_wind_guidance_available": details.get(
            "aloft_wind_guidance_available", False
        ),
        "open_meteo_available_variables": details.get(
            "available_variables", []
        ),
    }


def _cardinal_direction(heading):
    """Zet een koers in graden om naar een compacte windroosrichting."""
    if heading is None:
        return None
    directions = (
        "N", "NNO", "NO", "ONO", "O", "OZO", "ZO", "ZZO",
        "Z", "ZZW", "ZW", "WZW", "W", "WNW", "NW", "NNW",
    )
    return directions[int((float(heading) + 11.25) // 22.5) % 16]


async def async_setup_entry(
    hass: HomeAssistant,
    entry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Register the entity set belonging to the exact entry owner mode."""

    record = (
        hass.data.get(DOMAIN, {})
        .get(DATA_ENTRY_RUNTIMES, {})
        .get(entry.entry_id)
    )
    mode = record.get("mode") if isinstance(record, Mapping) else None
    if mode == RUNTIME_MODE_LEGACY:
        async_add_entities(
            _legacy_sensor_entities(hass), update_before_add=True
        )
        return
    if mode != RUNTIME_MODE_WORKER_ONLY:
        raise RuntimeError(
            "Storm Tracker-sensorplatform mist een exacte entry-owner"
        )

    from .worker_only_entry import get_prepared_worker_only_runtime

    prepared = get_prepared_worker_only_runtime(hass, entry.entry_id)
    async_add_entities(list(prepared.entities), update_before_add=False)


def _legacy_sensor_entities(hass: HomeAssistant) -> list:
    """Build the unchanged full provider/OFE/StormEngine entity set."""

    _load_legacy_sensor_dependencies()
    entities = [
        BlitzortungInslagenSensor(hass),
        OperaObservatieSensor(hass),
        ActiveRadarSourceSensor(hass),
        ProviderLifecycleSensor(hass),
        WorkerStatusSensor(hass),
        RadarCalibrationSensor(hass),
        PystepsPreflightSensor(hass),
        VirtualProbeSensor(hass),
        SynopticGroundSensor(hass),
        MetarGroundSensor(hass),
        FictieveTrackerSensor(hass),
        BlitzortungLaatsteInslag(hass),
        KmiObservatieSensor(hass),
        KmiIntensiteitSensor(hass),
        RainViewerObservatieSensor(hass),
        OpenMeteoGearSensor(hass),
    ]

    # KNMI en Netatmo altijd toevoegen — providers worden later geïnitialiseerd
    entities.append(KnmiIntensiteitSensorNu(hass))
    entities.append(KnmiNowcastSensor(hass))
    entities.append(NetatmoStationsSensor(hass))
    entities.append(NetatmoRegenSensor(hass))
    entities.append(NetatmoPressureTrendSensor(hass))
    entities.append(PrecipitationStatusSensor(hass))
    entities.extend(
        TargetPrecipitationStatusSensor(hass, spec)
        for spec in hass.data.get(DOMAIN, {}).get("target_specs", [])
        if not spec.primary
    )

    # Storm sensoren
    entities.append(StormTellerSensor(hass))
    entities.append(StormDetailSensor(hass))
    entities.append(McsDetectieSensor(hass))
    entities.append(RegionEngineSensor(hass))
    entities.append(StormMapGeoJsonSensor(hass))
    return entities


async def async_setup_platform(
    hass: HomeAssistant,
    config,
    async_add_entities: AddEntitiesCallback,
    discovery_info=None,
):
    """Retain the legacy platform hook; YAML setup itself is rejected."""

    async_add_entities(
        _legacy_sensor_entities(hass), update_before_add=True
    )


class StormTrackerBaseSensor(SensorEntity):
    """Basis voor alle Storm Tracker V3 sensoren."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass
        self._attr_should_poll = False
        self._unsubs = []
        self._update_handle = None

    async def async_added_to_hass(self) -> None:
        """Luister op HA-events van de providers."""
        @callback
        def _write_pending_update():
            self._update_handle = None
            self._prepare_update()
            self.async_write_ha_state()

        @callback
        def _handle_update(event):
            if self._update_handle is None:
                # Providerfasen kunnen meerdere gerelateerde events vlak na
                # elkaar publiceren. Verwerk die als één consistente snapshot.
                self._update_handle = self.hass.loop.call_later(
                    0.1, _write_pending_update
                )

        for event_name in self._listen_events:
            self._unsubs.append(self.hass.bus.async_listen(event_name, _handle_update))

    async def async_will_remove_from_hass(self) -> None:
        if self._update_handle is not None:
            self._update_handle.cancel()
            self._update_handle = None
        for unsub in self._unsubs:
            unsub()
        self._unsubs.clear()

    @property
    def _listen_events(self) -> list[str]:
        return []

    def _prepare_update(self) -> None:
        """Bereid een consistente snapshot voor voordat HA de state uitleest."""

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, "storm_tracker_v3")},
            "name": "Storm Tracker V3",
            "manufacturer": "Custom",
            "model": "V3",
        }


class BlitzortungInslagenSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 Blitzortung Inslagen"
    _attr_unique_id = "stv3_blitzortung_inslagen"
    _attr_icon = "mdi:lightning-bolt"
    _attr_native_unit_of_measurement = "inslagen"
    _attr_state_class = SensorStateClass.TOTAL

    @property
    def _listen_events(self):
        return [
            f"{DOMAIN}_lightning_update",
            f"{DOMAIN}_lightning_status_update",
        ]

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("lightning_count", 0)

    @property
    def extra_state_attributes(self):
        runtime = self.hass.data.get(DOMAIN, {})
        last = self.hass.data.get(DOMAIN, {}).get("last_lightning")
        attributes = {
            "actieve_bron": runtime.get("lightning_source", "geen"),
            "blitzortung_verbonden": bool(
                getattr(runtime.get("blitz_provider"), "connected", False)
            ),
            "bronmodus": runtime.get("lightning_source_mode", "auto"),
            "eumetsat_status": runtime.get("eumetsat_li_status", "niet_geconfigureerd"),
            "goes18_status": runtime.get("goes18_glm_status", "niet_geconfigureerd"),
            "goes19_status": runtime.get("goes19_glm_status", "niet_geconfigureerd"),
        }
        for prefix, diagnostic in (
            ("eumetsat", runtime.get("eumetsat_poll")),
            ("goes", runtime.get("goes_poll")),
        ):
            if diagnostic:
                attributes.update({
                    f"{prefix}_laatste_poll": datetime.fromtimestamp(
                        diagnostic["timestamp"]
                    ).isoformat(),
                    f"{prefix}_opgehaald": diagnostic["fetched"],
                    f"{prefix}_aanvaard": diagnostic["accepted"],
                    f"{prefix}_fout": diagnostic["error"],
                })
        if last:
            attributes.update({
                "laatste_lat": last.lat,
                "laatste_lon": last.lon,
                "laatste_ts": datetime.fromtimestamp(last.timestamp).isoformat(),
            })
        return attributes


class BlitzortungLaatsteInslag(StormTrackerBaseSensor):
    _attr_name = "STV3 Blitzortung Laatste Inslag"
    _attr_unique_id = "stv3_blitzortung_laatste"
    _attr_icon = "mdi:map-marker-radius"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_lightning_update"]

    @property
    def native_value(self):
        last = self.hass.data.get(DOMAIN, {}).get("last_lightning")
        if not last:
            return "Geen data"
        return f"{last.lat:.4f},{last.lon:.4f}"

    @property
    def extra_state_attributes(self):
        last = self.hass.data.get(DOMAIN, {}).get("last_lightning")
        if not last:
            return {}
        return {
            "lat": last.lat,
            "lon": last.lon,
            "timestamp": last.timestamp,
            "source": last.source,
        }


class OperaObservatieSensor(StormTrackerBaseSensor):
    """Toont het aantal OPERA radarcellen in het dekkingsgebied."""
    _attr_name      = "STV3 OPERA Observaties"
    _attr_unique_id = "stv3_opera_observaties"
    _attr_icon      = "mdi:radar"
    _attr_native_unit_of_measurement = "cellen"

    @property
    def _listen_events(self): return [f"{DOMAIN}_radar_update"]

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("opera_count", 0)

    @property
    def extra_state_attributes(self):
        return self.hass.data.get(DOMAIN, {}).get("opera_diagnostics", {})


class ActiveRadarSourceSensor(StormTrackerBaseSensor):
    """Operationele radarbron geselecteerd door primary/fallback-beleid."""
    _attr_name = "STV3 Actieve Radarbron"
    _attr_unique_id = "stv3_active_radar_source"
    _attr_icon = "mdi:radar"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_radar_source_update", f"{DOMAIN}_radar_update"]

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("active_radar_source") or "geen"

    @property
    def extra_state_attributes(self):
        data = self.hass.data.get(DOMAIN, {})
        return {
            "reason": data.get("radar_source_reason", "nog niet geselecteerd"),
            "engines": data.get("radar_sources_by_engine", {}),
        }


class RadarCalibrationSensor(StormTrackerBaseSensor):
    """Passieve regionale overeenkomstscore tussen radar- en satellietbronnen."""
    _attr_name = "STV3 Radar Autokalibratie"
    _attr_unique_id = "stv3_radar_autokalibratie"
    _attr_icon = "mdi:tune-variant"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_calibration_update"]

    @property
    def native_value(self):
        diagnostics = self.hass.data.get(DOMAIN, {}).get("radar_calibration", {})
        score = diagnostics.get("mean_f1_score")
        return round(float(score) * 100) if score is not None else "observeren"

    @property
    def extra_state_attributes(self):
        diagnostics = self.hass.data.get(DOMAIN, {}).get(
            "radar_calibration", {}
        )
        storage = diagnostics.get("storage") or {}
        latest = diagnostics.get("latest") or {}
        bias_profiles = diagnostics.get("bias_profiles") or {}
        worker = diagnostics.get("worker_operation") or {}
        # De volledige analyse bevat duizenden targettellingen en tientallen
        # biasprofielen. Die gegevens horen in de tweede SQLite-database; ze
        # als HA-state publiceren dwingt state machine, WebSocket en Recorder
        # iedere vijf minuten tot een zeer grote serialisatie.
        return {
            "mode": diagnostics.get("mode"),
            "status": diagnostics.get("status"),
            "samples": diagnostics.get("samples"),
            "comparable_samples": diagnostics.get("comparable_samples"),
            "pending_frames": diagnostics.get("pending_frames"),
            "mean_precision": diagnostics.get("mean_precision"),
            "mean_recall": diagnostics.get("mean_recall"),
            "mean_f1_score": diagnostics.get("mean_f1_score"),
            "latest_provider_pair": latest.get("provider_pair"),
            "latest_f1_score": latest.get("f1_score"),
            "latest_timestamp": latest.get("timestamp"),
            "bias_profile_count": bias_profiles.get("total", 0),
            "operational_bias_profiles": bias_profiles.get("operational", 0),
            "worker_state": worker.get("state"),
            "worker_mode": worker.get("mode"),
            "worker_fallback_active": worker.get("fallback_active", False),
            "worker_cached_frames": worker.get("cached_frames"),
            "worker_comparisons_received": worker.get(
                "comparisons_received"
            ),
            "storage_status": storage.get("status"),
            "storage_bytes": storage.get("bytes"),
            "storage_schema_version": storage.get("schema_version"),
            "stored_frames": storage.get("total_frames"),
            "stored_comparisons": storage.get("total_comparisons"),
            "stored_verification_samples": storage.get(
                "total_verification_samples"
            ),
            "stored_performance_samples": storage.get(
                "total_system_performance_samples"
            ),
            "stored_lightning_strikes": storage.get(
                "total_lightning_strikes"
            ),
            "stored_provider_cycles": storage.get(
                "total_provider_cycle_samples"
            ),
            "full_details": "secundaire_kalibratiedatabase",
        }


class PystepsPreflightSensor(StormTrackerBaseSensor):
    """Compact zicht op de observerende rasterbewegingscontrole."""

    _attr_name = "STV3 pySTEPS Preflight"
    _attr_unique_id = "stv3_pysteps_preflight"
    _attr_icon = "mdi:radar"

    @property
    def _listen_events(self):
        return [
            f"{DOMAIN}_radar_source_update",
            f"{DOMAIN}_targets_updated",
        ]

    @property
    def native_value(self):
        preflight = self.hass.data.get(DOMAIN, {}).get(
            "pysteps_input", {}
        ).get("preflight", {})
        return sum(
            1 for result in preflight.values()
            if result.get("status") == "ready"
        )

    @property
    def extra_state_attributes(self):
        diagnostics = self.hass.data.get(DOMAIN, {}).get("pysteps_input", {})
        return {
            "contract": diagnostics.get("contract"),
            "max_frames": diagnostics.get("max_frames"),
            "feeds_detection": any(
                result.get("feeds_detection")
                for result in diagnostics.get("preflight", {}).values()
            ),
            "nowcast_horizon_minutes": 60,
            "nowcast_ready_regions": sum(
                1 for result in diagnostics.get("preflight", {}).values()
                if (result.get("nowcast") or {}).get("status") == "ready"
            ),
            "regions": diagnostics.get("preflight", {}),
            "restart_recovery_audit": self.hass.data.get(DOMAIN, {}).get(
                "restart_recovery_audit", {}
            ),
        }


class VirtualProbeSensor(StormTrackerBaseSensor):
    """Toont passieve grondverificatiepunten langs voorspelde buibanen."""

    _attr_name = "STV3 Virtuele Regenverkenners"
    _attr_unique_id = "stv3_virtual_rain_probes"
    _attr_icon = "mdi:map-marker-path"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_virtual_probe_update"]

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get(
            "virtual_probe_diagnostics", {}
        ).get("active_count", 0)

    @property
    def extra_state_attributes(self):
        return self.hass.data.get(DOMAIN, {}).get(
            "virtual_probe_diagnostics", {}
        )


class SynopticGroundSensor(StormTrackerBaseSensor):
    """Compacte operationele status van Synoptic-grondmetingen."""

    _attr_name = "STV3 Synoptic Grondmetingen"
    _attr_unique_id = "stv3_synoptic_ground_observations"
    _attr_icon = "mdi:weather-rainy"
    _attr_native_unit_of_measurement = "stations"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_synoptic_update"]

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get(
            "synoptic_diagnostics", {}
        ).get("station_count", 0)

    @property
    def extra_state_attributes(self):
        return self.hass.data.get(DOMAIN, {}).get(
            "synoptic_diagnostics", {}
        )


class MetarGroundSensor(StormTrackerBaseSensor):
    """Operationele status van wereldwijde METAR-grondwaarnemingen."""

    _attr_name = "STV3 METAR Grondmetingen"
    _attr_unique_id = "stv3_metar_ground_observations"
    _attr_icon = "mdi:airport"
    _attr_native_unit_of_measurement = "stations"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_metar_update"]

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get(
            "metar_diagnostics", {}
        ).get("station_count", 0)

    @property
    def extra_state_attributes(self):
        return self.hass.data.get(DOMAIN, {}).get("metar_diagnostics", {})


class KmiObservatieSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 KMI Observaties"
    _attr_unique_id = "stv3_kmi_observaties"
    _attr_icon = "mdi:radar"
    _attr_native_unit_of_measurement = "pixels"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_radar_update"]

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("kmi_count", 0)

    @property
    def extra_state_attributes(self):
        data = self.hass.data.get(DOMAIN, {})
        obs_list = data.get("last_kmi_observations", [])
        provider = data.get("kmi_provider")
        frame_timestamp = getattr(
            provider, "last_frame_timestamp", None
        ) if provider is not None else None
        frame_age_minutes = (
            round((time.time() - float(frame_timestamp)) / 60, 1)
            if frame_timestamp is not None else None
        )
        if not obs_list:
            return {
                "status": (
                    "niet_actief" if provider is None
                    else "wacht_op_frame" if frame_timestamp is None
                    else "droog" if frame_age_minutes <= 30
                    else "verouderd"
                ),
                "aantal": 0,
                "laatste_frame": _timestamp_iso(frame_timestamp),
                "frame_leeftijd_minuten": frame_age_minutes,
            }
        intens = [o.intensity for o in obs_list if o.intensity]
        return {
            "status": "neerslag",
            "aantal": len(obs_list),
            "gem_intensiteit": round(sum(intens) / len(intens), 1) if intens else 0,
            "max_intensiteit": max(intens) if intens else 0,
            "eerste_lat": obs_list[0].lat,
            "eerste_lon": obs_list[0].lon,
            "laatste_frame": _timestamp_iso(frame_timestamp),
            "frame_leeftijd_minuten": frame_age_minutes,
        }


class KmiIntensiteitSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 KMI Intensiteit"
    _attr_unique_id = "stv3_kmi_intensiteit"
    _attr_icon = "mdi:water"
    _attr_native_unit_of_measurement = "level"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_radar_update"]

    @property
    def native_value(self):
        obs_list = self.hass.data.get(DOMAIN, {}).get("last_kmi_observations", [])
        if not obs_list:
            return 0
        intens = [o.intensity for o in obs_list if o.intensity]
        return max(intens) if intens else 0

    @property
    def extra_state_attributes(self):
        p = self.hass.data.get(DOMAIN, {}).get("kmi_provider")
        if not p:
            return {}
        from .providers.kmi import _ww_to_text
        return {
            "weercode":    getattr(p, "_last_ww", 0),
            "weertype":    _ww_to_text(getattr(p, "_last_ww", 0)),
            "temperatuur": getattr(p, "_last_temp", None),
            "ww_ts":       getattr(p, "_last_ww_ts", ""),
        }


class RainViewerObservatieSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 RainViewer Observaties"
    _attr_unique_id = "stv3_rainviewer_observaties"
    _attr_icon = "mdi:radar"
    _attr_native_unit_of_measurement = "pixels"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_radar_update"]

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("rv_count", 0)

    @property
    def extra_state_attributes(self):
        domain_data = self.hass.data.get(DOMAIN, {})
        obs_list = domain_data.get("last_rv_observations", [])
        provider = domain_data.get("rv_provider")
        diagnostics = provider.diagnostics if provider else {}
        intens = [o.intensity for o in obs_list if o.intensity]
        return {
            "status": (
                "gezond" if diagnostics.get("healthy") and obs_list
                else "droog" if diagnostics.get("healthy")
                else "ongezond"
            ),
            "healthy": diagnostics.get("healthy", False),
            "aantal": len(obs_list),
            "gem_intensiteit": round(sum(intens) / len(intens), 1) if intens else 0,
            "max_intensiteit": max(intens) if intens else 0,
            "laatste_poll": _timestamp_iso(diagnostics.get("last_poll_ts")),
            "laatste_succes": _timestamp_iso(diagnostics.get("last_success_ts")),
            "laatste_frame": _timestamp_iso(diagnostics.get("last_frame_ts")),
            "frame_leeftijd_minuten": diagnostics.get("frame_age_minutes"),
            "max_frame_leeftijd_minuten": diagnostics.get("max_frame_age_minutes"),
            "laatste_fout": diagnostics.get("last_error"),
            "opeenvolgende_fouten": diagnostics.get("consecutive_failures", 0),
            "frame_pad": diagnostics.get("last_path"),
        }


class OpenMeteoGearSensor(StormTrackerBaseSensor):
    _attr_name      = "STV3 Open-Meteo Gear"
    _attr_unique_id = "stv3_open_meteo_gear"
    _attr_icon      = "mdi:speedometer"

    @property
    def _listen_events(self): return [f"{DOMAIN}_open_meteo_update"]

    @property
    def native_value(self):
        data = self.hass.data.get(DOMAIN, {})
        if not data.get("open_meteo_enabled", False):
            return "DISABLED"
        result = data.get("open_meteo_result", {})
        return result.get("gear", "INITIALIZING")

    @property
    def extra_state_attributes(self):
        data = self.hass.data.get(DOMAIN, {})
        if not data.get("open_meteo_enabled", False):
            return {
                "enabled": False,
                "role": "optional_model_guidance",
                "provider_status": "disabled",
                "reason": "uitgeschakeld_in_integratieopties",
            }
        result = data.get("open_meteo_result", {})
        target_results = result.get("target_results", {})
        home = target_results.get("home", {})
        return {
            "role":                  result.get("role", "model_guidance"),
            "provider_status":       result.get("provider_status", "initializing"),
            "is_raining":            result.get("is_raining"),
            "max_precipitation":     result.get("max_precipitation"),
            "wet_points":            result.get("wet_points"),
            "wet_now":               result.get("wet_now"),
            "wet_forecast_90m":      result.get("wet_forecast_90m"),
            "total_points":          result.get("total_points", 0),
            "targets_requested":     result.get("targets_requested", 0),
            "targets_received":      result.get("targets_received", 0),
            "fetch_sequence":        result.get("fetch_sequence", 0),
            "last_attempt_at":       result.get("last_attempt_at"),
            "last_success_at":       result.get("last_success_at"),
            "data_age_seconds":      result.get("data_age_seconds"),
            "last_http_status":      result.get("last_http_status"),
            "consecutive_failures":  result.get("consecutive_failures", 0),
            "last_error":            result.get("last_error"),
            "next_retry_at":         result.get("next_retry_at"),
            "requested_variable_count": result.get("requested_variable_count"),
            "forecast_15m_steps":    result.get("forecast_15m_steps"),
            "forecast_hours":        result.get("forecast_hours"),
            "home_current_mm":       home.get("current_precipitation_mm"),
            "home_max_90m_mm":       home.get("forecast_90m_max_mm"),
            "home_total_90m_mm":     home.get("forecast_90m_total_mm"),
            "home_first_wet_minutes": home.get(
                "forecast_90m_first_wet_minutes"
            ),
            "home_max_3h_mm":        home.get("forecast_3h_max_mm"),
            "home_total_3h_mm":      home.get("forecast_3h_total_mm"),
            "home_precip_probability_max_6h": home.get(
                "precipitation_probability_max_6h_percent"
            ),
            "home_cape_max_3h_jkg":  home.get("cape_max_3h_jkg"),
            "home_lightning_potential_max_3h": home.get(
                "lightning_potential_max_3h"
            ),
            "home_wind_700hpa_speed_kmh": home.get(
                "wind_700hpa_speed_kmh"
            ),
            "home_wind_700hpa_direction_deg": home.get(
                "wind_700hpa_direction_deg"
            ),
            "target_forecasts": {
                target_id: {
                    "current_mm": details.get("current_precipitation_mm"),
                    "max_90m_mm": details.get("forecast_90m_max_mm"),
                    "total_90m_mm": details.get("forecast_90m_total_mm"),
                    "first_wet_minutes": details.get(
                        "forecast_90m_first_wet_minutes"
                    ),
                    "max_3h_mm": details.get("forecast_3h_max_mm"),
                    "precip_probability_max_6h": details.get(
                        "precipitation_probability_max_6h_percent"
                    ),
                    "cape_max_3h_jkg": details.get("cape_max_3h_jkg"),
                    "lightning_potential_max_3h": details.get(
                        "lightning_potential_max_3h"
                    ),
                    "wind_700hpa_speed_kmh": details.get(
                        "wind_700hpa_speed_kmh"
                    ),
                    "wind_700hpa_direction_deg": details.get(
                        "wind_700hpa_direction_deg"
                    ),
                    "available_variables": details.get(
                        "available_variables", []
                    ),
                }
                for target_id, details in sorted(target_results.items())
            },
        }


class FictieveTrackerSensor(StormTrackerBaseSensor):
    """Legacy entity-id met de actuele geconfigureerde testtrackerlocatie."""
    _attr_name      = "STV3 Fictieve tracker locatie"
    _attr_unique_id = "stv3_fictieve_tracker"
    _attr_icon      = "mdi:map-marker-account"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_fictieve_update", f"{DOMAIN}_targets_updated"]

    @property
    def native_value(self):
        target = self.hass.data.get(DOMAIN, {}).get("targets", {}).get(
            "test_tracker", {}
        )
        lat = target.get("latitude")
        lon = target.get("longitude")
        if lat is None or lon is None:
            return "Onbekend"
        return f"{lat:.4f},{lon:.4f}"

    @property
    def extra_state_attributes(self):
        target = self.hass.data.get(DOMAIN, {}).get("targets", {}).get(
            "test_tracker", {}
        )
        return {
            "latitude": target.get("latitude"),
            "longitude": target.get("longitude"),
            "entity_id": target.get("entity_id"),
            "available": bool(target.get("available")),
            "region_engine_id": target.get("region_engine_id"),
        }


class ProviderLifecycleSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 Provider Lifecycle"
    _attr_unique_id = "stv3_provider_lifecycle"
    _attr_icon = "mdi:sleep"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_provider_lifecycle_update"]

    @property
    def native_value(self):
        diagnostics = self.hass.data.get(DOMAIN, {}).get(
            "provider_lifecycle_diagnostics", {}
        )
        return sum(
            1 for item in diagnostics.values() if item.get("status") == "active"
        )

    @property
    def extra_state_attributes(self):
        domain_data = self.hass.data.get(DOMAIN, {})
        diagnostics = dict(
            domain_data.get("provider_lifecycle_diagnostics", {})
        )
        diagnostics["configuration"] = dict(
            domain_data.get("legacy_provider_configuration", {})
        )
        return diagnostics


class WorkerStatusSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 Worker Status"
    _attr_unique_id = "stv3_worker_status"
    _attr_icon = "mdi:server"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_worker_status_update"]

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get(
            "worker_status", {}
        ).get("state", "waiting")

    @property
    def extra_state_attributes(self):
        domain_data = self.hass.data.get(DOMAIN, {})
        status = dict(domain_data.get("worker_status", {}))
        status["runtime_mode"] = domain_data.get("runtime_mode")
        last_seen = status.get("last_seen")
        status["age_seconds"] = (
            round(max(0.0, time.time() - float(last_seen)), 1)
            if last_seen is not None
            else None
        )
        return status


class KnmiIntensiteitSensorNu(StormTrackerBaseSensor):
    _attr_name       = "STV3 KNMI Intensiteit Nu"
    _attr_unique_id  = "stv3_knmi_intensiteit_nu"
    _attr_icon       = "mdi:radar"
    _attr_native_unit_of_measurement = "level"

    @property
    def _listen_events(self): return [f"{DOMAIN}_knmi_update"]

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        # Direct initiële waarde wegschrijven zodat sensor nooit unavailable is
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("knmi_intensity_now", 0)

    @property
    def extra_state_attributes(self):
        return {
            "30min":  self.hass.data.get(DOMAIN, {}).get("knmi_intensity_30min", 0),
            "60min":  self.hass.data.get(DOMAIN, {}).get("knmi_intensity_60min", 0),
            "90min":  self.hass.data.get(DOMAIN, {}).get("knmi_intensity_90min", 0),
            "nowcast_stappen": len(self.hass.data.get(DOMAIN, {}).get("knmi_forecast", [])),
        }


class KnmiNowcastSensor(StormTrackerBaseSensor):
    _attr_name       = "STV3 KNMI Nowcast"
    _attr_unique_id  = "stv3_knmi_nowcast"
    _attr_icon       = "mdi:clock-fast"
    _attr_native_unit_of_measurement = "level"

    @property
    def _listen_events(self): return [f"{DOMAIN}_knmi_update"]
    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("knmi_intensity_30min", 0)

    @property
    def extra_state_attributes(self):
        import time
        forecast = self.hass.data.get(DOMAIN, {}).get("knmi_forecast", [])
        now      = time.time()
        attrs    = {}
        for o in sorted(forecast, key=lambda x: x.timestamp):
            minuten = round((o.timestamp - now) / 60)
            if 0 <= minuten <= 90:
                attrs[f"+{minuten}min"] = o.intensity
        attrs["intensiteit_30min"]  = self.hass.data.get(DOMAIN, {}).get("knmi_intensity_30min", 0)
        attrs["intensiteit_60min"]  = self.hass.data.get(DOMAIN, {}).get("knmi_intensity_60min", 0)
        attrs["intensiteit_90min"]  = self.hass.data.get(DOMAIN, {}).get("knmi_intensity_90min", 0)
        return attrs


class NetatmoStationsSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 Netatmo Stations"
    _attr_unique_id = "stv3_netatmo_stations"
    _attr_icon = "mdi:weather-pouring"
    _attr_native_unit_of_measurement = "stations"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_netatmo_update"]

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("netatmo_station_count", 0)

    @property
    def extra_state_attributes(self):
        import math
        from .engine.observation import observation_is_wet
        obs_list  = self.hass.data.get(DOMAIN, {}).get("last_netatmo_observations", [])
        if not obs_list:
            return {}
        regen     = [o for o in obs_list if observation_is_wet(o)]
        rain_vals = sorted([o.rain_mm for o in regen if o.rain_mm], reverse=True)
        wind_obs  = [o for o in obs_list if getattr(o, "wind_speed", None) is not None]
        press_obs = [o for o in obs_list if getattr(o, "pressure", None) is not None]
        gusts     = [o.gust_speed for o in obs_list if getattr(o, "gust_speed", None) is not None]
        pressures = [o.pressure for o in press_obs]

        attrs = {
            "totaal_stations":   len(obs_list),
            "met_regen":         len(regen),
            "max_regen_mm":      rain_vals[0] if rain_vals else 0,
            "gem_regen_mm":      round(sum(rain_vals) / len(rain_vals), 2) if rain_vals else 0,
            "wind_stations":     len(wind_obs),
            "max_windstoot_kmh": round(max(gusts), 1) if gusts else None,
            "druk_stations":     len(press_obs),
            "gem_druk_mbar":     round(sum(pressures) / len(pressures), 1) if pressures else None,
            "min_druk_mbar":     round(min(pressures), 1) if pressures else None,
        }
        if wind_obs:
            speeds = [o.wind_speed for o in wind_obs]
            angles = [o.wind_angle for o in wind_obs if o.wind_angle is not None]
            attrs["gem_wind_kmh"] = round(sum(speeds) / len(speeds), 1)
            if angles:
                sin_sum = sum(math.sin(math.radians(a)) for a in angles)
                cos_sum = sum(math.cos(math.radians(a)) for a in angles)
                attrs["gem_windrichting"] = round(math.degrees(math.atan2(sin_sum, cos_sum)) % 360, 0)
        return attrs


class NetatmoRegenSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 Netatmo Regen Stations"
    _attr_unique_id = "stv3_netatmo_regen"
    _attr_icon = "mdi:water-check"
    _attr_native_unit_of_measurement = "stations"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_netatmo_update"]

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("netatmo_rain_count", 0)

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True


class NetatmoPressureTrendSensor(StormTrackerBaseSensor):
    """Regionale drukverandering op basis van gepaarde Netatmo-stations."""
    _attr_name = "STV3 Netatmo Luchtdruktrend"
    _attr_unique_id = "stv3_netatmo_luchtdruktrend"
    _attr_icon = "mdi:gauge"
    _attr_native_unit_of_measurement = "hPa"
    _attr_state_class = SensorStateClass.MEASUREMENT

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_netatmo_update"]

    @property
    def native_value(self):
        trend = self.hass.data.get(DOMAIN, {}).get("netatmo_pressure_trend", {})
        return trend.get("delta_60m_hpa")

    @property
    def available(self) -> bool:
        return True

    @property
    def extra_state_attributes(self):
        domain_data = self.hass.data.get(DOMAIN, {})
        trend = domain_data.get("netatmo_pressure_trend", {})
        manager = domain_data.get("storm_manager")
        home_region = manager.get_engine_for_target("zone.home") if manager else None
        regional_trends = domain_data.get("netatmo_pressure_trends_by_engine", {})
        return {
            "regio_engine": home_region.engine_id if home_region else None,
            "trend": trend.get("trend", "onvoldoende_data"),
            "opwarming": trend.get("warmup_status", "initializing"),
            "opwarming_voltooid": trend.get("warmup_complete", False),
            "opwarming_stations": trend.get("warmup_stations", 0),
            "snelle_daling": trend.get("rapid_fall", False),
            "druk_mediaan_hpa": trend.get("median_pressure_hpa"),
            "drukval_15min_hpa": trend.get("delta_15m_hpa"),
            "drukval_30min_hpa": trend.get("delta_30m_hpa"),
            "drukval_60min_hpa": trend.get("delta_60m_hpa"),
            "drukstations_nu": trend.get("pressure_station_count", 0),
            "vergelijkbare_stations_15min": trend.get("stations_15m", 0),
            "vergelijkbare_stations_30min": trend.get("stations_30m", 0),
            "vergelijkbare_stations_60min": trend.get("stations_60m", 0),
            "laatste_berekening": _timestamp_iso(trend.get("timestamp")),
            "actieve_regios": len(regional_trends),
            "regionale_trends": {
                engine_id: regional.get("trend", "onvoldoende_data")
                for engine_id, regional in regional_trends.items()
            },
        }


class PrecipitationStatusSensor(StormTrackerBaseSensor):
    """Eén operationele neerslagstatus voor dashboard en automatiseringen."""
    _attr_name = "STV3 Neerslagstatus"
    _attr_unique_id = "stv3_neerslagstatus"
    _attr_icon = "mdi:weather-rainy"

    def __init__(self, hass: HomeAssistant) -> None:
        super().__init__(hass)
        self._cached_summary = None
        self._last_published_summary = None
        self._last_published_monotonic = None

    @property
    def _listen_events(self):
        return [
            f"{DOMAIN}_targets_updated",
            f"{DOMAIN}_storms_updated",
            f"{DOMAIN}_radar_source_update",
            f"{DOMAIN}_netatmo_update",
            f"{DOMAIN}_open_meteo_update",
            f"{DOMAIN}_lightning_update",
            f"{DOMAIN}_lightning_status_update",
        ]

    def _summary(self, *, record_audit=False):
        data = self.hass.data.get(DOMAIN, {})
        home_target = data.get("targets", {}).get("home", {})
        engine_id = home_target.get("region_engine_id")
        manager = data.get("storm_manager")
        region = manager.get_engine_for_target("zone.home") if manager else None
        storms = region.storm_engine.get_active_storms() if region else []
        radar_source = (
            (data.get("radar_sources_by_engine", {}).get(engine_id) or {}).get("source")
            if engine_id else data.get("active_radar_source")
        )
        transition = (
            (data.get("radar_sources_by_engine", {}).get(engine_id) or {}).get(
                "transition"
            ) if engine_id else None
        )
        result = build_precipitation_status(
            storms,
            float(home_target.get("latitude", data.get("fictieve_lat", 0.0))),
            float(home_target.get("longitude", data.get("fictieve_lon", 0.0))),
            radar_source=radar_source,
            pressure_trend=(
                data.get("netatmo_pressure_trends_by_engine", {}).get(engine_id)
                if engine_id else data.get("netatmo_pressure_trend")
            ),
            motion_guidance=(
                data.get("pysteps_input", {}).get("preflight", {}).get(
                    engine_id
                )
                if engine_id else None
            ),
        )
        if result.get("status") == "droog" and engine_id:
            lightning = _lightning_only_summary(
                data,
                engine_id,
                float(home_target.get("latitude", data.get("fictieve_lat", 0.0))),
                float(home_target.get("longitude", data.get("fictieve_lon", 0.0))),
            )
            if lightning is not None:
                result.update(lightning)
        _apply_source_transition(result, transition)
        target = home_target
        netatmo = _netatmo_target_summary(
            data,
            engine_id,
            float(home_target.get("latitude", data.get("fictieve_lat", 0.0))),
            float(home_target.get("longitude", data.get("fictieve_lon", 0.0))),
            "home",
            record_audit=record_audit,
        )
        _apply_local_netatmo_confirmation(result, netatmo)
        summary = {
            **result,
            "location_place": target.get("location_place", "Thuis"),
            "location_address": target.get("location_address"),
            "country_code": target.get("country_code"),
            "location_accuracy_km": target.get("location_accuracy_km"),
            "radar_covered": target.get("radar_covered"),
            **netatmo,
            **_open_meteo_target_summary(data, "home"),
        }
        return _attach_tracker_forecast_shadow(
            summary,
            storms,
            "home",
            float(home_target.get("latitude", data.get("fictieve_lat", 0.0))),
            float(home_target.get("longitude", data.get("fictieve_lon", 0.0))),
            radar_source=radar_source,
            radar_covered=target.get("radar_covered"),
        )

    def _prepare_update(self, *, record_audit=True) -> None:
        now_monotonic = time.monotonic()
        elapsed_minutes = (
            (now_monotonic - self._last_published_monotonic) / 60.0
            if self._last_published_monotonic is not None else 0.0
        )
        self._cached_summary = stabilize_eta_continuity(
            self._summary(record_audit=record_audit),
            self._last_published_summary,
            elapsed_minutes=elapsed_minutes,
        )
        self._last_published_summary = dict(self._cached_summary)
        self._last_published_monotonic = now_monotonic

    def _current_summary(self):
        if self._cached_summary is None:
            self._prepare_update(record_audit=False)
        return self._cached_summary

    @property
    def native_value(self):
        return self._current_summary()["status"]

    @property
    def available(self) -> bool:
        return True

    @property
    def extra_state_attributes(self):
        return {
            key: value
            for key, value in self._current_summary().items()
            if key != "status"
        }


class TargetPrecipitationStatusSensor(StormTrackerBaseSensor):
    """Operationele neerslagstatus voor één geconfigureerd target."""
    _attr_icon = "mdi:weather-rainy"

    def __init__(self, hass: HomeAssistant, spec) -> None:
        super().__init__(hass)
        self._spec = spec
        self._cached_summary = None
        self._last_published_summary = None
        self._last_published_monotonic = None
        self._attr_name = f"STV3 {spec.name} Neerslagstatus"
        self._attr_unique_id = f"stv3_target_{spec.entity_suffix}_neerslagstatus"

    @property
    def _listen_events(self):
        return [
            f"{DOMAIN}_targets_updated",
            f"{DOMAIN}_radar_source_update",
            f"{DOMAIN}_storms_updated",
            f"{DOMAIN}_netatmo_update",
            f"{DOMAIN}_open_meteo_update",
            f"{DOMAIN}_lightning_update",
            f"{DOMAIN}_lightning_status_update",
        ]

    def _target_data(self):
        return self.hass.data.get(DOMAIN, {}).get("targets", {}).get(
            self._spec.target_id, {}
        )

    def _summary(self, *, record_audit=False):
        domain_data = self.hass.data.get(DOMAIN, {})
        target = self._target_data()
        manager = domain_data.get("storm_manager")
        region = manager.get_engine_for_target(self._spec.entity_id) if manager else None
        storms = region.storm_engine.get_active_storms() if region else []
        radar_source = (
            (domain_data.get("radar_sources_by_engine", {}).get(region.engine_id) or {}).get("source")
            if region else domain_data.get("active_radar_source")
        )
        transition = (
            (domain_data.get("radar_sources_by_engine", {}).get(region.engine_id) or {}).get(
                "transition"
            ) if region else None
        )
        result = build_precipitation_status(
            storms,
            target.get("latitude", 0.0),
            target.get("longitude", 0.0),
            radar_source=radar_source,
            pressure_trend=(
                domain_data.get("netatmo_pressure_trends_by_engine", {}).get(
                    region.engine_id
                )
                if region else None
            ),
            motion_guidance=(
                domain_data.get("pysteps_input", {}).get(
                    "preflight", {}
                ).get(region.engine_id)
                if region else None
            ),
        )
        if not target.get("radar_covered", False):
            result["status"] = "onvoldoende_data"
        elif result.get("status") == "droog" and region is not None:
            lightning = _lightning_only_summary(
                domain_data,
                region.engine_id,
                float(target.get("latitude", 0.0)),
                float(target.get("longitude", 0.0)),
            )
            if lightning is not None:
                result.update(lightning)
        _apply_source_transition(result, transition)
        netatmo = _netatmo_target_summary(
            domain_data,
            region.engine_id if region else target.get("region_engine_id"),
            target.get("latitude"),
            target.get("longitude"),
            self._spec.target_id,
            record_audit=record_audit,
        )
        _apply_local_netatmo_confirmation(result, netatmo)
        summary = {
            **result,
            "target_id": self._spec.target_id,
            "target_name": self._spec.name,
            "location_entity": self._spec.entity_id,
            "latitude": target.get("latitude"),
            "longitude": target.get("longitude"),
            "region_engine_id": target.get("region_engine_id"),
            "radar_covered": target.get("radar_covered", False),
            "location_place": target.get("location_place"),
            "location_address": target.get("location_address"),
            "country_code": target.get("country_code"),
            "location_accuracy_km": target.get("location_accuracy_km"),
            **netatmo,
            **_open_meteo_target_summary(
                domain_data, self._spec.target_id
            ),
        }
        return _attach_tracker_forecast_shadow(
            summary,
            storms,
            self._spec.target_id,
            target.get("latitude", 0.0),
            target.get("longitude", 0.0),
            radar_source=radar_source,
            radar_covered=target.get("radar_covered"),
        )

    def _prepare_update(self, *, record_audit=True) -> None:
        now_monotonic = time.monotonic()
        elapsed_minutes = (
            (now_monotonic - self._last_published_monotonic) / 60.0
            if self._last_published_monotonic is not None else 0.0
        )
        self._cached_summary = stabilize_eta_continuity(
            self._summary(record_audit=record_audit),
            self._last_published_summary,
            elapsed_minutes=elapsed_minutes,
        )
        self._last_published_summary = dict(self._cached_summary)
        self._last_published_monotonic = now_monotonic

    def _current_summary(self):
        if self._cached_summary is None:
            self._prepare_update(record_audit=False)
        return self._cached_summary

    @property
    def available(self) -> bool:
        return bool(self._target_data().get("available"))

    @property
    def native_value(self):
        return self._current_summary()["status"] if self.available else None

    @property
    def extra_state_attributes(self):
        return {
            key: value
            for key, value in self._current_summary().items()
            if key != "status"
        }


class StormTellerSensor(StormTrackerBaseSensor):
    """Toont het aantal actieve storms over alle RegionEngines."""
    _attr_name      = "STV3 Actieve Storms"
    _attr_unique_id = "stv3_storm_teller"
    _attr_icon      = "mdi:weather-lightning-rainy"
    _attr_native_unit_of_measurement = "storms"

    @property
    def _listen_events(self): return [f"{DOMAIN}_storms_updated"]

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True

    def _storms(self):
        manager = self.hass.data.get(DOMAIN, {}).get("storm_manager")
        if manager is None:
            return [
                (None, storm)
                for storm in self.hass.data.get(DOMAIN, {}).get(
                    "storms", []
                )
            ]
        return [
            (engine.engine_id, storm)
            for engine in manager.get_all_engines()
            for storm in engine.storm_engine.get_active_storms()
        ]

    @property
    def native_value(self):
        return len(self._storms())

    @property
    def extra_state_attributes(self):
        storms = self._storms()
        return {
            "storms": [
                {
                    "region_engine": engine_id,
                    "id":          s.storm_id,
                    "lat":         round(s.centroid_lat, 4),
                    "lon":         round(s.centroid_lon, 4),
                    "richting":    round(s.heading_deg, 0) if s.heading_deg is not None else None,
                    "richting_tekst": _cardinal_direction(s.heading_deg),
                    "snelheid":    round(s.speed_kmh, 1) if s.speed_kmh is not None else None,
                    "gemiddelde_snelheid": getattr(
                        s, "motion_average_speed_kmh", None
                    ),
                    "gemiddelde_richting": getattr(
                        s, "motion_average_heading_deg", None
                    ),
                    "bewegingsstatus": getattr(
                        s, "motion_estimate_status", "unavailable"
                    ),
                    "verwachte_snelheid_min": getattr(
                        s, "motion_forecast_speed_min_kmh", None
                    ),
                    "verwachte_snelheid_max": getattr(
                        s, "motion_forecast_speed_max_kmh", None
                    ),
                    "bewegingspunten": s.motion_sample_count,
                    "bewegingshistorie_min": round(s.motion_history_minutes, 1),
                    "bewegingsfit": round(s.motion_fit_quality, 3),
                    "baanmodel": getattr(s, "motion_model", "none"),
                    "baan_uitleg": (
                        "gebogen baan"
                        if getattr(s, "trajectory_curved", False)
                        else "rechte baan"
                        if getattr(s, "motion_model", "none") != "none"
                        else "nog onvoldoende beweging"
                    ),
                    "bochtstraal_km": getattr(
                        s, "curvature_radius_km", None
                    ),
                    "afgewezen_meetpunten": getattr(
                        s, "trajectory_outliers_rejected", 0
                    ),
                    "ontstaan_uit": sorted(
                        getattr(s, "parent_storm_ids", set())
                    ),
                    "samengevoegd_met": sorted(
                        getattr(s, "merged_storm_ids", set())
                    ),
                    "generatie": getattr(s, "lineage_generation", 0),
                    "bewegingsbasis": getattr(s, "motion_basis", "unknown"),
                    "hindcastfout_km": getattr(
                        s, "motion_prediction_error_km", None
                    ),
                    "modelwinst": getattr(s, "motion_model_gain", 0.0),
                    "versnelling_kmh2": round(
                        math.hypot(
                            getattr(s, "acceleration_east_kmh2", 0.0),
                            getattr(s, "acceleration_north_kmh2", 0.0),
                        ),
                        1,
                    ),
                    "tracking_status": s.tracking_status,
                    "opeenvolgende_radarframes": s.consecutive_radar_frames,
                    "laatste_radarframe": _timestamp_iso(s.last_radar_timestamp),
                    "inslagen":    s.strike_count,
                    "vertrouwen":  s.confidence,
                    "plaatsnaam":  getattr(s, "place_name", None),
                    "radarcellen": len(getattr(s, "radar_cells", {})),
                    "bron_systemen": len(getattr(s, "source_system_ids", set())),
                    "type": getattr(s, "system_type", "unknown"),
                    "mcs_status": getattr(s, "mcs_status", "not_evaluated"),
                    "mcs_duur_min": getattr(s, "mcs_duration_minutes", 0.0),
                    "convectieve_span_km": getattr(s, "mcs_convective_span_km", 0.0),
                }
                for engine_id, s in storms
            ],
            "scope": "alle_region_engines",
        }


class StormDetailSensor(StormTrackerBaseSensor):
    """Toont details van de dichtstbijzijnde actieve storm."""
    _attr_name      = "STV3 Dichtstbijzijnde Storm"
    _attr_unique_id = "stv3_storm_dichtstbij"
    _attr_icon      = "mdi:map-marker-alert"

    @property
    def _listen_events(self): return [f"{DOMAIN}_storms_updated"]

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True

    @property
    def native_value(self):
        """Afstand in km tot de dichtstbijzijnde storm."""
        storms = self.hass.data.get(DOMAIN, {}).get("storms", [])
        if not storms:
            return None
        lat = self.hass.data.get(DOMAIN, {}).get("fictieve_lat", 0)
        lon = self.hass.data.get(DOMAIN, {}).get("fictieve_lon", 0)
        import math
        def haversine(la1, lo1, la2, lo2):
            R = 6371.0
            dlat = math.radians(la2 - la1)
            dlon = math.radians(lo2 - lo1)
            a = math.sin(dlat/2)**2 + math.cos(math.radians(la1)) * math.cos(math.radians(la2)) * math.sin(dlon/2)**2
            return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        def closest_point(storm):
            radar = storm.closest_radar_point(lat, lon)
            return radar or (
                haversine(lat, lon, storm.centroid_lat, storm.centroid_lon),
                storm.centroid_lat,
                storm.centroid_lon,
            )
        closest_data = min(
            ((closest_point(storm), storm) for storm in storms),
            key=lambda item: item[0][0],
        )
        return round(closest_data[0][0], 1)

    @property
    def extra_state_attributes(self):
        storms = self.hass.data.get(DOMAIN, {}).get("storms", [])
        if not storms:
            return {}
        lat = self.hass.data.get(DOMAIN, {}).get("fictieve_lat", 0)
        lon = self.hass.data.get(DOMAIN, {}).get("fictieve_lon", 0)
        import math
        def haversine(la1, lo1, la2, lo2):
            R = 6371.0
            dlat = math.radians(la2 - la1)
            dlon = math.radians(lo2 - lo1)
            a = math.sin(dlat/2)**2 + math.cos(math.radians(la1)) * math.cos(math.radians(la2)) * math.sin(dlon/2)**2
            return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        def closest_point(storm):
            radar = storm.closest_radar_point(lat, lon)
            return radar or (
                haversine(lat, lon, storm.centroid_lat, storm.centroid_lon),
                storm.centroid_lat,
                storm.centroid_lon,
            )
        point, closest = min(
            ((closest_point(storm), storm) for storm in storms),
            key=lambda item: item[0][0],
        )
        afstand, impact_lat, impact_lon = point

        motion = closest.motion_to_target(lat, lon, distance_km=afstand)
        eta_min = motion["eta_minutes"]

        return {
            "storm_id":    closest.storm_id,
            "lat":         round(impact_lat, 4),
            "lon":         round(impact_lon, 4),
            "system_lat":  round(closest.centroid_lat, 4),
            "system_lon":  round(closest.centroid_lon, 4),
            "afstand_km":  round(afstand, 1),
            "richting":    round(closest.heading_deg, 0) if closest.heading_deg is not None else None,
            "richting_tekst": _cardinal_direction(closest.heading_deg),
            "snelheid_kmh": round(closest.speed_kmh, 1) if closest.speed_kmh is not None else None,
            "koers_naar_tracker": motion["bearing_to_target_deg"],
            "naderingssnelheid_kmh": motion["approach_speed_kmh"],
            "beweegt_naar_tracker": motion["moving_towards"],
            "eta_minuten": round(eta_min, 0) if eta_min is not None else None,
            "bewegingspunten": closest.motion_sample_count,
            "bewegingshistorie_min": round(closest.motion_history_minutes, 1),
            "bewegingsfit": round(closest.motion_fit_quality, 3),
            "tracking_status": closest.tracking_status,
            "opeenvolgende_radarframes": closest.consecutive_radar_frames,
            "laatste_radarframe": _timestamp_iso(closest.last_radar_timestamp),
            "inslagen":    closest.strike_count,
            "vertrouwen":  closest.confidence,
            "plaatsnaam":  getattr(closest, "place_name", None),
            "radius_km":   round(closest.radius_km, 1),
            "radarcellen": len(getattr(closest, "radar_cells", {})),
            "bron_systemen": len(getattr(closest, "source_system_ids", set())),
            "type": getattr(closest, "system_type", "unknown"),
            "mcs_status": getattr(closest, "mcs_status", "not_evaluated"),
            "mcs_duur_min": getattr(closest, "mcs_duration_minutes", 0.0),
            "convectieve_span_km": getattr(closest, "mcs_convective_span_km", 0.0),
            "neerslag_span_km": getattr(closest, "mcs_precipitation_span_km", 0.0),
            "convectieve_cellen": getattr(closest, "mcs_convective_cells", 0),
            "intense_cellen": getattr(closest, "mcs_intense_cells", 0),
            "parent_oppervlakte_km2": getattr(closest, "mcs_parent_area_km2", 0.0),
        }


class McsDetectieSensor(StormTrackerBaseSensor):
    """Aantal bevestigde MCS'en, met kandidaten afzonderlijk zichtbaar."""
    _attr_name = "STV3 MCS Detectie"
    _attr_unique_id = "stv3_mcs_detectie"
    _attr_icon = "mdi:weather-hurricane"
    _attr_native_unit_of_measurement = "systemen"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_storms_updated", f"{DOMAIN}_mcs_transition"]

    def _storms(self):
        manager = self.hass.data.get(DOMAIN, {}).get("storm_manager")
        if not manager:
            return [
                (None, storm)
                for storm in self.hass.data.get(DOMAIN, {}).get("storms", [])
            ]
        return [
            (engine.engine_id, storm)
            for engine in manager.get_all_engines()
            for storm in engine.storm_engine.get_storms()
        ]

    @property
    def native_value(self):
        storms = self._storms()
        return sum(
            1 for _, storm in storms
            if getattr(storm, "mcs_status", None) == "confirmed"
        )

    @property
    def extra_state_attributes(self):
        storms = self._storms()
        relevant = [
            (engine_id, storm) for engine_id, storm in storms
            if getattr(storm, "mcs_status", None) in {"candidate", "confirmed"}
        ]
        return {
            "kandidaten": sum(
                1 for _, storm in relevant if storm.mcs_status == "candidate"
            ),
            "bevestigd": sum(
                1 for _, storm in relevant if storm.mcs_status == "confirmed"
            ),
            "systemen": [
                {
                    "region_engine": engine_id,
                    **storm.mcs_diagnostics(),
                }
                for engine_id, storm in relevant
            ],
            "evaluaties": [
                {
                    "region_engine": engine_id,
                    **storm.mcs_diagnostics(),
                }
                for engine_id, storm in storms[:25]
            ],
            "criteria": {
                "min_convectieve_span_km": 100,
                "min_convectieve_cellen_40dbz": 2,
                "min_intense_cellen_50dbz": 1,
                "min_duur_min": 180,
            },
        }


class RegionEngineSensor(StormTrackerBaseSensor):
    """Diagnostiek van de werkelijk actieve dynamische runtime-regio's."""
    _attr_name = "STV3 Region Engines"
    _attr_unique_id = "stv3_region_engines"
    _attr_icon = "mdi:radar"
    _attr_native_unit_of_measurement = "engines"

    @property
    def _listen_events(self):
        return [
            f"{DOMAIN}_storms_updated",
            f"{DOMAIN}_fictieve_update",
            f"{DOMAIN}_targets_updated",
            f"{DOMAIN}_radar_source_update",
        ]

    @property
    def native_value(self):
        manager = self.hass.data.get(DOMAIN, {}).get("storm_manager")
        return len(manager.get_all_engines()) if manager else 0

    @property
    def extra_state_attributes(self):
        manager = self.hass.data.get(DOMAIN, {}).get("storm_manager")
        if not manager:
            return {"engines": []}
        return {
            "sharing_distance_km": manager.sharing_distance_km,
            "durable_processing": dict(
                self.hass.data.get(DOMAIN, {}).get(
                    "durable_processing", {}
                )
            ),
            "engines": [
                {
                    "id": engine.engine_id,
                    "durable_region_id": engine.durable_region_id,
                    "recovery_ready": engine.recovery_ready,
                    "centrum": [
                        round(engine.center_lat, 4),
                        round(engine.center_lon, 4),
                    ],
                    "observatieradius_km": engine.observation_radius_km,
                    "targets": sorted(engine.projection_targets),
                    "weather_systemen": len(engine.storm_engine.get_storms()),
                    "compute_schedule": (
                        engine.compute_schedule_diagnostics()
                    ),
                    "observation_delivery": (
                        engine.ofe.delivery_diagnostics()
                    ),
                    "radar": self.hass.data.get(DOMAIN, {}).get(
                        "radar_sources_by_engine", {}
                    ).get(engine.engine_id, {}),
                }
                for engine in manager.get_all_engines()
            ],
        }


class StormMapGeoJsonSensor(StormTrackerBaseSensor):
    """Compacte, versieerbare kaartfeed voor multi-target clients."""

    _attr_name = "STV3 Kaart GeoJSON"
    _attr_unique_id = "stv3_map_geojson"
    _attr_icon = "mdi:map-marker-path"

    def __init__(self, hass: HomeAssistant) -> None:
        super().__init__(hass)
        self._cached_collection = None

    @property
    def _listen_events(self):
        return [
            f"{DOMAIN}_storms_updated",
            f"{DOMAIN}_targets_updated",
            f"{DOMAIN}_fictieve_update",
            f"{DOMAIN}_radar_source_update",
            f"{DOMAIN}_lightning_map_update",
            f"{DOMAIN}_virtual_probe_update",
            f"{DOMAIN}_geojson_worker_update",
        ]

    def _collection(self):
        if self._cached_collection is not None:
            return self._cached_collection
        data = self.hass.data.get(DOMAIN, {})
        if data.get("runtime_mode") == RUNTIME_MODE_LEGACY:
            manager = data.get("storm_manager")
            self._cached_collection = build_feature_collection(
                data.get("targets", {}),
                manager.get_all_engines() if manager else [],
                active_radar_source=data.get("active_radar_source"),
                radar_sources_by_engine=data.get(
                    "radar_sources_by_engine"
                ),
                lightning_events=data.get("recent_lightning"),
                virtual_probe_diagnostics=data.get(
                    "virtual_probe_diagnostics"
                ),
            )
            self._cached_collection.setdefault("metadata", {})[
                "state"
            ] = "legacy_local"
            # Keep the authenticated HTTP endpoint on the same exact map
            # revision without scheduling a worker-only GeoJSON job.
            data["geojson_worker_collection"] = self._cached_collection
            return self._cached_collection
        worker_collection = data.get("geojson_worker_collection")
        if isinstance(worker_collection, dict):
            self._cached_collection = worker_collection
            return self._cached_collection
        self._cached_collection = {
            "type": "FeatureCollection",
            "features": [],
            "metadata": {
                "feature_count": 0,
                "state": "wacht_op_worker",
            },
        }
        return self._cached_collection

    def _prepare_update(self) -> None:
        self._cached_collection = None
        data = self.hass.data.get(DOMAIN, {})
        if data.get("runtime_mode") == RUNTIME_MODE_LEGACY:
            self._cached_collection = self._collection()
            return
        updated_at = float(
            (data.get("geojson_worker_status") or {}).get("updated_at", 0)
            or 0
        )
        if time.time() - updated_at > 1.0:
            self.hass.async_create_task(
                async_refresh_worker_geojson(self.hass),
                "storm_tracker_v3_geojson_worker",
            )
        self._cached_collection = self._collection()

    @property
    def native_value(self):
        return self._collection()["metadata"]["feature_count"]

    @property
    def extra_state_attributes(self):
        collection = self._collection()
        domain_data = self.hass.data.get(DOMAIN, {})
        if domain_data.get("runtime_mode") == RUNTIME_MODE_LEGACY:
            return {
                "endpoint": "/api/storm_tracker_v3/geojson",
                "runtime_mode": RUNTIME_MODE_LEGACY,
                **collection["metadata"],
            }
        worker = domain_data.get(
            "geojson_worker_status", {}
        )
        return {
            "endpoint": "/api/storm_tracker_v3/geojson",
            "worker_state": worker.get("state", "wachten"),
            "worker_mode": worker.get("mode", "worker_first"),
            "worker_fallback_active": worker.get(
                "fallback_active", True
            ),
            **collection["metadata"],
        }
