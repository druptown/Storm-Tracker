"""Pure Home Assistant read views over an operational worker snapshot.

The worker owns every meteorological decision.  These helpers only project an
already validated :class:`OperationalSnapshotCache` record into small sensor
values and attributes.  They perform no I/O, scheduling, database access,
geometry, forecast calculation or fallback inference.

An unavailable or stale fact remains explicit ``onvoldoende_data``.  In
particular, absence of a worker fact is never converted to ``droog`` and a
future threat is never presented as current precipitation.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .operational_snapshot import OperationalSnapshotCache


VIEW_CONTRACT = "stv3-ha-operational-snapshot-view-v1"

_CURRENT_STATE = {
    "unknown": "onvoldoende_data",
    "dry": "droog",
    "raining": "regen",
}
_THREAT_STATE = {
    "unknown": "onbekend",
    "none": "geen",
    "available": "beschikbaar",
}
_GROUND_STATE = {
    "unknown": "onbekend",
    "dry": "droog",
    "wet": "nat",
    "mixed": "gemengd",
}


@dataclass(frozen=True, slots=True)
class SnapshotSensorView:
    """One bounded, side-effect-free sensor projection."""

    native_value: str | int | float | None
    available: bool
    attributes: Mapping[str, Any]

    def attribute_copy(self) -> dict[str, Any]:
        """Return an independent dictionary suitable for a HA entity."""
        return dict(self.attributes)


def _status_attributes(cache: OperationalSnapshotCache) -> dict[str, Any]:
    status = cache.get_status()
    return {
        "view_contract": VIEW_CONTRACT,
        "snapshot_revision": status["revision"],
        "snapshot_sha256": status["snapshot_sha256"],
        "snapshot_availability": status["availability"],
        "worker_state": status["worker_state"],
        "product_valid_at": status["product_valid_at"],
        "received_at": status["received_at"],
        "evaluated_at": status["evaluated_at"],
        "provenance_complete": status["provenance_complete"],
    }


def worker_status_view(cache: OperationalSnapshotCache) -> SnapshotSensorView:
    """Project only worker/snapshot health; never derive weather."""
    if not isinstance(cache, OperationalSnapshotCache):
        raise TypeError("cache moet OperationalSnapshotCache zijn")
    status = cache.get_status()
    return SnapshotSensorView(
        native_value=status["worker_state"],
        available=True,
        attributes=_status_attributes(cache),
    )


def target_precipitation_view(
    cache: OperationalSnapshotCache,
    target_id: str,
) -> SnapshotSensorView:
    """Project current rain and next threat as strictly separate facts."""
    if not isinstance(cache, OperationalSnapshotCache):
        raise TypeError("cache moet OperationalSnapshotCache zijn")
    if not isinstance(target_id, str) or not target_id:
        raise ValueError("target_id moet niet-lege tekst zijn")

    status_attributes = _status_attributes(cache)
    target = cache.get_target(target_id)
    if target is None:
        return SnapshotSensorView(
            native_value="onvoldoende_data",
            available=True,
            attributes={
                **status_attributes,
                "target_id": target_id,
                "status_text": "Geen gevalideerd workersnapshot voor dit target",
                "current_precipitation_state": "unknown",
                "current_freshness": "unknown",
                "current_reason_codes": ["target_snapshot_missing"],
                "rain_rate_mm_h": None,
                "next_threat_state": "unknown",
                "next_threat": "onbekend",
                "next_threat_freshness": "unknown",
                "next_threat_track_id": None,
                "next_threat_probability_percent": None,
                "next_threat_probability_calibration_id": None,
                "next_threat_eta_minutes": None,
                "next_threat_distance_km": None,
                "next_threat_passage_status": "unknown",
                "next_threat_reason_codes": ["target_snapshot_missing"],
                "ground_status": "onbekend",
                "ground_confirmation": None,
                "ground_same_track": None,
                "ground_source_ids": [],
                "ground_reason_codes": ["target_snapshot_missing"],
            },
        )

    current = target["current_precipitation"]
    threat = target["next_threat"]
    ground = current["ground_evidence"]
    current_state = current["state"]
    threat_state = threat["state"]
    return SnapshotSensorView(
        native_value=_CURRENT_STATE[current_state],
        available=True,
        attributes={
            **status_attributes,
            "target_id": target["target_id"],
            "target_name": target["display_name"],
            "region_id": target["region_id"],
            "status_text": target["status_text"],
            "current_precipitation_state": current_state,
            "current_freshness": current["freshness"],
            "current_product_valid_at": current["product_valid_at"],
            "current_provider_ids": list(current["provider_ids"]),
            "current_reason_codes": list(current["reason_codes"]),
            "rain_rate_mm_h": current["rain_rate_mm_h"],
            "next_threat_state": threat_state,
            "next_threat": _THREAT_STATE[threat_state],
            "next_threat_freshness": threat["freshness"],
            "next_threat_track_id": threat["track_id"],
            "next_threat_probability_percent": (
                threat["calibrated_probability_percent"]
            ),
            "next_threat_probability_calibration_id": (
                threat["probability_calibration_id"]
            ),
            "next_threat_eta_minutes": threat["eta_minutes"],
            "next_threat_distance_km": threat["distance_km"],
            "next_threat_passage_status": threat["passage_status"],
            "next_threat_product_valid_at": threat["product_valid_at"],
            "next_threat_provider_ids": list(threat["provider_ids"]),
            "next_threat_reason_codes": list(threat["reason_codes"]),
            "ground_status": _GROUND_STATE[ground["status"]],
            "ground_freshness": ground["freshness"],
            "ground_confirmation": ground["confirmation"],
            "ground_same_track": ground["same_track"],
            "ground_evidence_valid_at": ground["evidence_valid_at"],
            "ground_observed_at_min": ground["observed_at_min"],
            "ground_observed_at_max": ground["observed_at_max"],
            "ground_received_at": ground["received_at"],
            "ground_physical_site_count": ground["physical_site_count"],
            "ground_wet_site_count": ground["wet_site_count"],
            "ground_source_ids": list(ground["source_ids"]),
            "ground_reason_codes": list(ground["reason_codes"]),
        },
    )


def collection_count_view(
    cache: OperationalSnapshotCache,
    collection: str,
) -> SnapshotSensorView:
    """Expose one pre-indexed collection count without iterating its items."""
    if not isinstance(cache, OperationalSnapshotCache):
        raise TypeError("cache moet OperationalSnapshotCache zijn")
    if collection not in {"regions", "targets", "providers", "ui_storms"}:
        raise ValueError("onbekende operationele snapshotcollectie")
    status = cache.get_collection_status(collection)
    if status is None:
        status = {"complete": False, "overflow_count": 0, "item_count": 0}
    return SnapshotSensorView(
        native_value=status["item_count"],
        available=True,
        attributes={
            **_status_attributes(cache),
            "collection": collection,
            "complete": status["complete"],
            "overflow_count": status["overflow_count"],
        },
    )


def provider_status_view(
    cache: OperationalSnapshotCache,
    provider_id: str,
) -> SnapshotSensorView:
    """Expose one worker-produced provider fact through an O(1) lookup."""
    if not isinstance(cache, OperationalSnapshotCache):
        raise TypeError("cache moet OperationalSnapshotCache zijn")
    if not isinstance(provider_id, str) or not provider_id:
        raise ValueError("provider_id moet niet-lege tekst zijn")
    provider = cache.get_provider(provider_id)
    if provider is None:
        collection = cache.get_collection_status("providers")
        snapshot_status = cache.get_status()
        transport_omitted = (
            isinstance(collection, Mapping)
            and collection.get("complete") is False
            and collection.get("overflow_count", 0) > 0
        )
        safe_initial_fallback = (
            isinstance(collection, Mapping)
            and collection.get("complete") is True
            and collection.get("item_count") == 0
            and snapshot_status.get("provenance_complete") is False
        )
        if transport_omitted:
            reason_code = "provider_projection_omitted_for_transport"
            error = None
        elif safe_initial_fallback:
            reason_code = "awaiting_meteorological_evidence"
            error = None
        else:
            reason_code = "provider_snapshot_missing"
            error = reason_code
        return SnapshotSensorView(
            native_value="unknown",
            available=True,
            attributes={
                **_status_attributes(cache),
                "provider_id": provider_id,
                "readiness": "not_ready",
                "effective_status": "unknown",
                "freshness": "unknown",
                "product_valid_at": None,
                "provider_received_at": None,
                "error": error,
                "reason_codes": [reason_code],
            },
        )
    effective_status = provider["state"]
    readiness = provider.get("readiness")
    if readiness is None:
        readiness = (
            "ready"
            if effective_status in {"operational", "stale"}
            else "not_ready"
        )
    return SnapshotSensorView(
        native_value=provider["state"],
        available=True,
        attributes={
            **_status_attributes(cache),
            "provider_id": provider["provider_id"],
            "provider_name": provider["display_name"],
            "readiness": readiness,
            "effective_status": effective_status,
            "freshness": provider["freshness"],
            "product_valid_at": provider["product_valid_at"],
            "provider_received_at": provider["received_at"],
            "error": provider["error"],
            "reason_codes": list(provider["reason_codes"]),
        },
    )


__all__ = [
    "VIEW_CONTRACT",
    "SnapshotSensorView",
    "collection_count_view",
    "provider_status_view",
    "target_precipitation_view",
    "worker_status_view",
]
