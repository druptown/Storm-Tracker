"""Compact operationeel neerslagbeeld voor sensoren en dashboards."""
from __future__ import annotations

import math
from copy import copy
from datetime import datetime, timedelta, timezone


def _intensity_label(dbz: float) -> str:
    if dbz < 20:
        return "zeer licht"
    if dbz < 30:
        return "licht"
    if dbz < 40:
        return "matig"
    if dbz < 50:
        return "zwaar"
    return "zeer zwaar"


def _target_forecast(storm, motion: dict, *, now_utc: datetime | None = None) -> dict:
    """Maak een conservatieve korte-termijnprognose voor één target."""
    empty = {
        "forecast_available": False,
        "expected_passage_at": None,
        "forecast_horizon_minutes": None,
        "forecast_confidence_percent": None,
        "forecast_intensity_dbz": None,
        "forecast_intensity_label": None,
        "intensity_trend_dbz_per_hour": None,
        "forecast_block_reason": None,
        "forecast_motion_model": getattr(storm, "motion_model", "none"),
    }
    if storm.tracking_status != "bevestigd":
        return {**empty, "forecast_block_reason": "systeem_nog_niet_bevestigd"}
    minutes = motion.get("closest_pass_minutes")
    passage_classification = motion.get("passage_classification")
    approach_speed = motion.get("approach_speed_kmh")
    # Communiceer eerst wat het systeem fysiek doet. Een lage confidence mag
    # een ETA/waarschuwing blokkeren, maar mag een duidelijke wegtrekkende
    # richting niet verbergen achter de generieke confidence-melding.
    if approach_speed is not None and approach_speed < -1.0:
        return {**empty, "forecast_block_reason": "systeem_trekt_weg"}
    if passage_classification == "mist":
        return {**empty, "forecast_block_reason": "geen_passage_binnen_corridor"}
    if storm.confidence not in {"Matig", "Hoog"}:
        return {**empty, "forecast_block_reason": "bewegingsvector_onvoldoende_betrouwbaar"}
    if minutes is None or passage_classification is None:
        return {**empty, "forecast_block_reason": "passage_nog_niet_berekenbaar"}
    if minutes < 0 or minutes > 90:
        return {**empty, "forecast_block_reason": "buiten_prognosehorizon"}
    if not any(cell.max_dbz is not None for cell in storm.radar_cells.values()):
        return {**empty, "forecast_block_reason": "intensiteit_onbekend"}

    cells = [cell for cell in storm.radar_cells.values() if cell.max_dbz is not None]
    if not cells:
        return empty
    timestamps = sorted({cell.timestamp for cell in cells})
    latest_ts = timestamps[-1]
    current_dbz = max(cell.max_dbz for cell in cells if cell.timestamp == latest_ts)
    trend = 0.0
    if len(timestamps) >= 2:
        earliest_ts = timestamps[0]
        elapsed_hours = (latest_ts - earliest_ts) / 3600.0
        if elapsed_hours > 0:
            earliest_dbz = max(
                cell.max_dbz for cell in cells if cell.timestamp == earliest_ts
            )
            trend = (current_dbz - earliest_dbz) / elapsed_hours

    # Intensiteitstrends zijn grillig: extrapoleer maximaal ±10 dBZ.
    change = max(-10.0, min(10.0, trend * float(minutes) / 60.0))
    forecast_dbz = max(0.0, min(75.0, current_dbz + change))
    base_confidence = 82 if storm.confidence == "Hoog" else 64
    frame_bonus = min(10, max(0, storm.consecutive_radar_frames - 2) * 3)
    horizon_penalty = round(float(minutes) / 90.0 * 24)
    confidence = max(25, min(95, base_confidence + frame_bonus - horizon_penalty))
    reference = now_utc or datetime.now(timezone.utc)
    passage_at = reference + timedelta(minutes=float(minutes))
    return {
        "forecast_available": True,
        "expected_passage_at": passage_at.isoformat(timespec="minutes"),
        "forecast_horizon_minutes": round(float(minutes)),
        "forecast_confidence_percent": confidence,
        "forecast_intensity_dbz": round(forecast_dbz, 1),
        "forecast_intensity_label": _intensity_label(forecast_dbz),
        "intensity_trend_dbz_per_hour": round(trend, 1),
        "forecast_block_reason": None,
        "forecast_motion_model": getattr(storm, "motion_model", "none"),
    }


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    value = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 6371.0088 * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))


def _reliability_score(
    storm,
    motion: dict,
    guidance_metadata: dict,
    *,
    radar_source: str | None,
) -> tuple[int, str, dict]:
    """Combineer uitlegbare kwaliteitsfactoren tot één conservatieve score."""
    frames = int(getattr(storm, "consecutive_radar_frames", 0) or 0)
    frame_score = min(100, round(frames / 6.0 * 100))
    tracking_score = {
        "waargenomen": 25,
        "bevestigd": 85,
        "alleen_bliksem": 15,
    }.get(getattr(storm, "tracking_status", None), 20)
    guidance_score = guidance_metadata.get("motion_guidance_score")
    fit_score = round(
        max(0.0, min(1.0, float(getattr(storm, "motion_fit_quality", 0.0))))
        * 100
    )
    motion_score = int(guidance_score) if guidance_score is not None else fit_score
    prediction_error = getattr(storm, "motion_prediction_error_km", None)
    hindcast_score = (
        max(0, round(100 - float(prediction_error) / 12.0 * 100))
        if prediction_error is not None
        else 35
    )
    passage_score = {
        "raak": 100,
        "rand": 55,
        "mist": 30,
    }.get(motion.get("passage_classification"), 20)
    source_score = 90 if radar_source else 35
    factors = {
        "tracking": tracking_score,
        "frames": frame_score,
        "motion": motion_score,
        "hindcast": hindcast_score,
        "passage": passage_score,
        "radar_source": source_score,
    }
    score = round(
        tracking_score * 0.15
        + frame_score * 0.15
        + motion_score * 0.30
        + hindcast_score * 0.20
        + passage_score * 0.10
        + source_score * 0.10
    )
    score = max(0, min(100, score))
    passage = motion.get("passage_classification")
    if passage == "rand":
        score = min(score, 74)
    elif passage == "mist":
        score = min(score, 54)
    elif passage not in {"raak", "rand"}:
        score = min(score, 34)
    if prediction_error is not None and float(prediction_error) > 5.0:
        score = min(score, 69)
    label = (
        "Hoog"
        if score >= 75
        else "Matig"
        if score >= 55
        else "Laag"
        if score >= 35
        else "Onvoldoende"
    )
    return score, label, factors


def _guided_storm(storm, guidance: dict | None):
    """Gebruik een numeriek geldige rastervector als de stormvector nog rijpt."""
    metadata = {
        "motion_guidance_source": None,
        "motion_guidance_stage": None,
        "motion_guidance_score": None,
        "motion_guidance_verified": False,
    }
    if (
        not guidance
        or storm.tracking_status != "bevestigd"
        or storm.confidence in {"Matig", "Hoog"}
    ):
        return storm, metadata

    status = guidance.get("status")
    frames = int(guidance.get("frames_used") or 0)
    speed = guidance.get("translation_km_per_hour")
    heading = guidance.get("heading_deg")
    match_iou = guidance.get("motion_match_iou")
    wet_ratio = guidance.get("wet_cell_ratio")
    try:
        speed_value = float(speed)
        heading_value = float(heading)
        match_iou_value = float(match_iou)
        wet_ratio_value = float(wet_ratio)
    except (TypeError, ValueError):
        return storm, metadata
    if (
        status not in {"warming_up_vectors", "ready"}
        or frames < 2
        or not math.isfinite(speed_value)
        or speed_value < 0.0
        or not math.isfinite(heading_value)
        or not math.isfinite(match_iou_value)
        or match_iou_value < 0.50
        or not math.isfinite(wet_ratio_value)
        or not 0.40 <= wet_ratio_value <= 2.50
    ):
        return storm, metadata

    vector_consistency = guidance.get("vector_consistency_deg")
    verified = (
        status == "ready"
        and frames >= 3
        and guidance.get("confidence") in {"medium", "high"}
        and float(guidance.get("verification_iou") or 0.0) >= 0.35
        and vector_consistency is not None
        and float(vector_consistency) <= 30.0
    )
    if verified:
        score = (
            (68 if guidance.get("confidence") == "high" else 55)
            + min(18, round(float(guidance["verification_iou"]) * 20))
            - min(10, round(float(vector_consistency) / 3))
        )
        stage = "verified"
    else:
        score = 25 + min(25, round(match_iou_value * 30))
        stage = "provisional"

    guided = copy(storm)
    guided.heading_deg = heading_value
    guided.speed_kmh = speed_value
    # Matig ontsluit de bestaande, conservatieve contourprojectie. De aparte
    # verified-gate hieronder bepaalt of een ETA ook waarschuwingen mag voeden.
    guided.confidence = "Matig"
    guided.motion_model = "pysteps_translation"
    guided.motion_basis = "pysteps_raster"
    guided.motion_sample_count = frames
    guided.motion_history_minutes = max(0.0, (frames - 1) * 10.0)
    guided.motion_fit_quality = match_iou_value
    guided.motion_prediction_error_km = max(
        2.0, (1.0 - match_iou_value) * 20.0
    )
    guided.velocity_east_kmh = speed_value * math.sin(
        math.radians(heading_value)
    )
    guided.velocity_north_kmh = speed_value * math.cos(
        math.radians(heading_value)
    )
    guided.acceleration_east_kmh2 = 0.0
    guided.acceleration_north_kmh2 = 0.0
    return guided, {
        "motion_guidance_source": "pysteps_preflight",
        "motion_guidance_stage": stage,
        "motion_guidance_score": max(1, min(95, score)),
        "motion_guidance_verified": verified,
    }


def _candidate_rank(point: tuple, storm, motion: dict) -> tuple[tuple, str]:
    """Rangschik systemen op relevantie voor één target, niet alleen afstand."""
    distance = float(point[0])
    confirmed = storm.tracking_status == "bevestigd"
    if not confirmed:
        return ((1, 8, distance), "observed")

    approach = motion.get("approach_speed_kmh")
    # Een contour die het target op t=0 nog raakt kan geometrisch als passage
    # scoren terwijl het volledige systeem al wegtrekt. Rangschik dat nooit als
    # een toekomstige hit: er is dan geen aankomst meer om voor te waarschuwen.
    if approach is not None and approach < -1.0:
        return ((0, 6, distance), "moving_away")
    passage = motion.get("passage_classification")
    minutes = motion.get("closest_pass_minutes")
    within_horizon = minutes is not None and 0 <= minutes <= 90
    future_passage = (
        within_horizon
        and (float(minutes) > 0.5 or (approach is not None and approach > 1.0))
    )
    if future_passage and passage == "raak":
        return ((0, 1, minutes, distance), "forecast_hit")
    if future_passage and passage == "rand":
        return ((0, 2, minutes, distance), "forecast_edge")

    if approach is not None and approach > 1.0:
        if within_horizon and passage != "mist":
            return ((0, 3, minutes, distance), "approaching")
        return ((0, 4, distance), "approaching")
    # Actuele neerslag blijft als dichtstbijzijnde waarneming beschikbaar, maar
    # blokkeert geen afzonderlijk systeem dat aantoonbaar onderweg is.
    if distance <= 5.0:
        return ((0, 5, distance), "current_precipitation")
    if approach is not None and abs(approach) <= 1.0:
        return ((0, 5, distance), "lateral")
    return ((0, 7, distance), "confirmed")


def stabilize_eta_continuity(
    current: dict,
    previous: dict | None,
    *,
    elapsed_minutes: float = 0.0,
) -> dict:
    """Blokkeer een onmogelijke latere ETA-sprong voor hetzelfde systeem.

    Een veranderende radarcontour kan bij een nieuwe framecyclus een latere
    lob als de eerste toekomstige overlap kiezen. Zonder continuiteitsgrens
    springt een bijna verstreken ETA dan terug naar tientallen minuten en
    vuurt dezelfde waarschuwing opnieuw. Echte systeemwissels en normale
    kleine bijstellingen blijven ongemoeid.
    """
    current["eta_continuity_guarded"] = False
    if not previous:
        return current

    current_eta = current.get("eta_minutes")
    previous_eta = previous.get("eta_minutes")
    if not isinstance(current_eta, (int, float)) or not isinstance(
        previous_eta, (int, float)
    ):
        return current
    if current.get("storm_id") != previous.get("storm_id"):
        return current
    if current.get("status") != "naderend" or previous.get("status") != "naderend":
        return current
    if current.get("passage_classification") not in {"raak", "rand"}:
        return current
    if previous.get("passage_classification") not in {"raak", "rand"}:
        return current
    approach = current.get("approach_speed_kmh")
    if not isinstance(approach, (int, float)) or approach <= 1.0:
        return current

    elapsed = max(0.0, float(elapsed_minutes))
    allowed_increase = max(10.0, elapsed + 5.0)
    if float(current_eta) <= float(previous_eta) + allowed_increase:
        return current

    stabilized_eta = max(0.0, float(previous_eta) - elapsed)
    current["raw_eta_minutes"] = current_eta
    current["eta_minutes"] = round(stabilized_eta)
    current["eta_continuity_guarded"] = True
    current["eta_continuity_reason"] = "same_system_eta_jump"
    for candidate in current.get("approaching_systems", []):
        if candidate.get("selected") and candidate.get("storm_id") == current.get(
            "storm_id"
        ):
            candidate["raw_eta_minutes"] = candidate.get("eta_minutes")
            candidate["eta_minutes"] = current["eta_minutes"]
            candidate["eta_continuity_guarded"] = True
    return current


def build_precipitation_status(
    storms: list,
    target_lat: float,
    target_lon: float,
    *,
    radar_source: str | None = None,
    pressure_trend: dict | None = None,
    motion_guidance: dict | None = None,
) -> dict:
    """Vat actieve systemen samen tot één dashboardvriendelijke toestand."""
    active = [storm for storm in storms if not getattr(storm, "is_dormant", False)]
    base = {
        "status": "droog",
        "radar_source": radar_source,
        "active_system_count": len(active),
        "pressure_trend": (pressure_trend or {}).get("trend", "onvoldoende_data"),
        "pressure_delta_60m_hpa": (pressure_trend or {}).get("delta_60m_hpa"),
        "rapid_pressure_fall": (pressure_trend or {}).get("rapid_fall", False),
        "selected_reason": None,
        "nearest_precipitation_storm_id": None,
        "tracked_system_storm_id": None,
        "nearest_precipitation_is_tracked_system": None,
        "distance_system_relation": None,
        "nearest_precipitation_distance_km": None,
        "nearest_precipitation_centroid_distance_km": None,
        "nearest_precipitation_max_dbz": None,
        "nearest_precipitation_frames": None,
        "tracked_system_distance_km": None,
        "tracked_system_centroid_distance_km": None,
        "tracked_system_frames": None,
        "approaching_system_count": 0,
        "approaching_systems": [],
        "local_passage_expected": False,
        "warning_eligible": False,
        "reliability_score_percent": 0,
        "reliability_label": "Onvoldoende",
        "reliability_factors": {},
        "eta_reliable": False,
        "eta_basis": None,
        "footprint_pass_minutes": None,
        "forecast_available": False,
        "expected_passage_at": None,
        "forecast_horizon_minutes": None,
        "forecast_confidence_percent": None,
        "forecast_intensity_dbz": None,
        "forecast_intensity_label": None,
        "intensity_trend_dbz_per_hour": None,
        "forecast_block_reason": None,
        "forecast_motion_model": None,
        "forecast_provisional": False,
        "motion_guidance_source": None,
        "motion_guidance_stage": None,
        "motion_guidance_score": None,
        "motion_guidance_verified": False,
    }
    if not active:
        return base

    latest_radar_timestamp = max(
        (
            float(cell.timestamp)
            for candidate_storm in active
            for cell in candidate_storm.radar_cells.values()
        ),
        default=None,
    )
    current_frame_min_timestamp = (
        latest_radar_timestamp - 60.0
        if latest_radar_timestamp is not None
        else None
    )
    candidates = []
    for original_storm in active:
        storm, guidance_metadata = _guided_storm(
            original_storm, motion_guidance
        )
        point = storm.closest_radar_point(
            target_lat,
            target_lon,
            min_timestamp=current_frame_min_timestamp,
        )
        if (
            point is None
            and storm.radar_cells
            and current_frame_min_timestamp is not None
        ):
            # Dit systeem heeft alleen cellen uit een ouder frame. De kaart
            # verbergt die terecht; laat het ook geen actuele afstand/ETA sturen.
            continue
        if point is None:
            point = (
                _haversine_km(target_lat, target_lon, storm.centroid_lat, storm.centroid_lon),
                storm.centroid_lat,
                storm.centroid_lon,
            )
        motion = storm.motion_to_target(target_lat, target_lon, distance_km=point[0])
        rank, reason = _candidate_rank(point, storm, motion)
        candidates.append(
            (rank, point, storm, motion, reason, guidance_metadata)
        )

    if not candidates:
        return base

    _, tracked_point, storm, motion, selected_reason, guidance_metadata = min(
        candidates, key=lambda item: item[0]
    )
    _, nearest_point, nearest_storm, _, _, _ = min(
        candidates, key=lambda item: float(item[1][0])
    )
    approaching_systems = []
    for rank, point, candidate, candidate_motion, reason, _ in sorted(
        candidates, key=lambda item: item[0]
    ):
        approach = candidate_motion.get("approach_speed_kmh")
        minutes = candidate_motion.get("closest_pass_minutes")
        passage = candidate_motion.get("passage_classification")
        is_approaching = approach is not None and approach > 1.0
        is_future_passage = (
            minutes is not None
            and float(minutes) > 0.5
            and float(minutes) <= 90.0
            and passage in {"raak", "rand"}
            and not (approach is not None and approach < -1.0)
        )
        if not (is_approaching or is_future_passage):
            continue
        approaching_systems.append({
            "storm_id": candidate.storm_id,
            "distance_km": round(float(point[0]), 1),
            "eta_minutes": (
                round(float(candidate_motion["eta_minutes"]))
                if candidate_motion.get("eta_minutes") is not None
                else None
            ),
            "passage_classification": passage,
            "approach_speed_kmh": candidate_motion.get("approach_speed_kmh"),
            "confidence": candidate.confidence,
            "selected": candidate.storm_id == storm.storm_id,
            "selection_reason": reason,
        })
    nearest_distance, impact_lat, impact_lon = nearest_point
    tracked_distance = float(tracked_point[0])
    nearest_centroid_distance = _haversine_km(
        target_lat,
        target_lon,
        nearest_storm.centroid_lat,
        nearest_storm.centroid_lon,
    )
    tracked_centroid_distance = _haversine_km(
        target_lat, target_lon, storm.centroid_lat, storm.centroid_lon
    )
    nearest_radar_dbz = [
        cell.max_dbz
        for cell in nearest_storm.radar_cells.values()
        if cell.max_dbz is not None
    ]
    tracking_status = storm.tracking_status
    status = "bevestigd" if tracking_status == "bevestigd" else "waargenomen"
    if tracking_status == "bevestigd":
        # Een gebogen, gevalideerd traject kan het target raken terwijl de
        # onmiddellijke snelheidsvector nog zijdelings of zelfs licht
        # weggericht is. In dat geval is de volledige trajectprojectie
        # betrouwbaarder dan de raaklijn van het laatste meetpunt. Toon de
        # waargenomen richting ook bij lage confidence; eta_reliable en
        # forecast_available blijven de aparte veiligheidsgrenzen.
        trajectory_passage = (
            motion.get("eta_minutes") is not None
            and motion.get("passage_classification") in {"raak", "rand"}
            and motion.get("closest_pass_minutes") is not None
            and 0 <= float(motion["closest_pass_minutes"]) <= 90
        )
        approach_speed = motion["approach_speed_kmh"]
        if approach_speed is not None and approach_speed < -1.0:
            status = "wegtrekkend"
        elif trajectory_passage:
            status = "naderend"
        elif approach_speed is not None and approach_speed > 1.0:
            status = (
                "nadert_regio"
                if motion.get("passage_classification") == "mist"
                else "naderend"
            )
        elif approach_speed is not None:
            status = "langs_trekkend"

    # eta_minutes betekent aankomst. Een t=0-contouroverlap van een systeem dat
    # wegtrekt is een actuele/aflopende passage, geen ETA 0.
    published_eta = (
        None
        if motion.get("approach_speed_kmh") is not None
        and motion["approach_speed_kmh"] < -1.0
        else motion["eta_minutes"]
    )
    radar_dbz = [
        cell.max_dbz for cell in storm.radar_cells.values()
        if cell.max_dbz is not None
    ]
    forecast = _target_forecast(storm, motion)
    guidance_verified = guidance_metadata["motion_guidance_verified"]
    guidance_used = guidance_metadata["motion_guidance_source"] is not None
    if guidance_used and forecast.get("forecast_available"):
        forecast["forecast_confidence_percent"] = min(
            int(forecast["forecast_confidence_percent"]),
            int(guidance_metadata["motion_guidance_score"]),
        )
        forecast["forecast_provisional"] = not guidance_verified
    local_passage_expected = motion.get("passage_classification") in {
        "raak",
        "rand",
    }
    eta_reliable = (
        tracking_status == "bevestigd"
        and storm.confidence in {"Matig", "Hoog"}
        and published_eta is not None
        and local_passage_expected
        and (not guidance_used or guidance_verified)
    )
    reliability_score, reliability_label, reliability_factors = (
        _reliability_score(
            storm,
            motion,
            guidance_metadata,
            radar_source=radar_source,
        )
    )
    return {
        **base,
        "status": status,
        "storm_id": storm.storm_id,
        "tracked_system_storm_id": storm.storm_id,
        "nearest_precipitation_storm_id": nearest_storm.storm_id,
        "nearest_precipitation_is_tracked_system": (
            nearest_storm.storm_id == storm.storm_id
        ),
        "distance_system_relation": (
            "zelfde_systeem"
            if nearest_storm.storm_id == storm.storm_id
            else "verschillende_systemen"
        ),
        "selected_reason": selected_reason,
        "tracking_status": tracking_status,
        # Backwards-compatible safety distance: always the nearest observed
        # precipitation edge, irrespective of which system has the best motion track.
        "distance_km": round(nearest_distance, 1),
        "nearest_precipitation_distance_km": round(nearest_distance, 1),
        "nearest_precipitation_centroid_distance_km": round(
            nearest_centroid_distance, 1
        ),
        "nearest_precipitation_max_dbz": (
            round(max(nearest_radar_dbz), 1) if nearest_radar_dbz else None
        ),
        "nearest_precipitation_frames": nearest_storm.consecutive_radar_frames,
        "tracked_system_distance_km": round(tracked_distance, 1),
        "tracked_system_centroid_distance_km": round(
            tracked_centroid_distance, 1
        ),
        "tracked_system_frames": storm.consecutive_radar_frames,
        "approaching_system_count": len(approaching_systems),
        "approaching_systems": approaching_systems,
        "local_passage_expected": local_passage_expected,
        "warning_eligible": status == "naderend" and eta_reliable,
        "reliability_score_percent": reliability_score,
        "reliability_label": reliability_label,
        "reliability_factors": reliability_factors,
        "impact_lat": round(impact_lat, 4),
        "impact_lon": round(impact_lon, 4),
        "system_lat": round(storm.centroid_lat, 4),
        "system_lon": round(storm.centroid_lon, 4),
        "system_type": storm.system_type,
        "consecutive_radar_frames": storm.consecutive_radar_frames,
        "max_dbz": round(max(radar_dbz), 1) if radar_dbz else None,
        "heading_deg": round(storm.heading_deg, 0) if storm.heading_deg is not None else None,
        "speed_kmh": round(storm.speed_kmh, 1) if storm.speed_kmh is not None else None,
        "average_speed_kmh": getattr(storm, "motion_average_speed_kmh", None),
        "average_heading_deg": getattr(
            storm, "motion_average_heading_deg", None
        ),
        "motion_estimate_status": getattr(
            storm, "motion_estimate_status", "unavailable"
        ),
        "forecast_speed_min_kmh": getattr(
            storm, "motion_forecast_speed_min_kmh", None
        ),
        "forecast_speed_max_kmh": getattr(
            storm, "motion_forecast_speed_max_kmh", None
        ),
        "approach_speed_kmh": motion["approach_speed_kmh"],
        "moving_towards": motion["moving_towards"],
        "eta_minutes": published_eta,
        "eta_basis": motion.get("eta_basis"),
        "eta_reliable": eta_reliable,
        "closest_pass_distance_km": motion["closest_pass_distance_km"],
        "closest_pass_minutes": motion["closest_pass_minutes"],
        "footprint_pass_distance_km": motion["footprint_pass_distance_km"],
        "footprint_pass_minutes": motion.get("footprint_pass_minutes"),
        "passage_classification": motion["passage_classification"],
        "passage_uncertainty_km": motion["passage_uncertainty_km"],
        "motion_confidence": storm.confidence,
        "direction_reliable": storm.confidence in {"Matig", "Hoog"},
        **guidance_metadata,
        "motion_sample_count": getattr(storm, "motion_sample_count", 0),
        "motion_history_minutes": getattr(storm, "motion_history_minutes", 0.0),
        "motion_fit_quality": getattr(storm, "motion_fit_quality", 0.0),
        "motion_model": getattr(storm, "motion_model", "none"),
        "motion_basis": getattr(storm, "motion_basis", "unknown"),
        "motion_prediction_error_km": getattr(
            storm, "motion_prediction_error_km", None
        ),
        "motion_model_gain": getattr(storm, "motion_model_gain", 0.0),
        "motion_acceleration_kmh2": round(
            math.hypot(
                getattr(storm, "acceleration_east_kmh2", 0.0),
                getattr(storm, "acceleration_north_kmh2", 0.0),
            ),
            1,
        ),
        **forecast,
    }
