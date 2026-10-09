"""Strict parent-side Open-Meteo guidance from retained exact raw bytes.

The capture child remains raw-only and never sees target IDs.  This module
revalidates the complete response, resolves response order inside the trusted
parent, and admits only positive model guidance.  A zero model value never
becomes a verified dry-weather claim.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
import hashlib
import json
import math
import re
from typing import Any, Iterable, Mapping

from .acquisition_query_authority import openmeteo_response_target_ids
from .desired_state import DesiredState
from .openmeteo_native import (
    OPENMETEO_CAPTURE_PRODUCT,
    OPENMETEO_RESPONSE_PRODUCT,
    decode_verified_openmeteo_response,
)
from .operational_fact_revision import (
    AdmissionDisposition,
    CommittedOperationalFactRevision,
    FactAdmissionReceipt,
    OperationalFactKind,
    validate_committed_operational_revision,
)
from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactStore,
)
from .tracking_core.operational_snapshot import (
    canonical_json_bytes,
    validate_snapshot,
)


CONTRACT = "stv3-openmeteo-operational-guidance-v1"
SOURCE_ID = "model.openmeteo.region"
PUBLIC_CAPTURE_PRODUCT = "openmeteo_forecast_native_unverified_exact_capture"
PUBLIC_RESPONSE_PRODUCT = "openmeteo_forecast_native_unverified_response_bytes"
PUBLIC_RESPONSE_ROLE = "response_bytes"
PUBLIC_RESPONSE_CONTENT_TYPE = "application/octet-stream"
PUBLIC_COLLECTOR = "storm_tracker_public_source_exact_raw"
PUBLIC_COLLECTOR_VERSION = "public-source-exact-raw-v1"
PUBLIC_PROVENANCE_KIND = "openmeteo_unverified_response_v1"
PUBLIC_ENDPOINT_HOST = "api.open-meteo.com"
PUBLIC_ENDPOINT_PATH = "/v1/forecast"
CURRENT_REASON = "openmeteo_model_current_precipitation"
NO_CURRENT_REASON = "openmeteo_model_no_current_precipitation"
THREAT_REASON = "openmeteo_model_precipitation_guidance"
NO_THREAT_REASON = "openmeteo_model_no_precipitation_in_90m"
WET_THRESHOLD_MM = 0.01
FORECAST_STEPS_90M = 6
MAX_MODEL_AGE_SECONDS = 30 * 60
MAX_FUTURE_SKEW_SECONDS = 5 * 60
MAX_PRECIPITATION_MM_15M = 250.0
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_CERTIFICATE_SHA256 = hashlib.sha256(CONTRACT.encode("ascii")).hexdigest()


class OpenMeteoOperationalGuidanceError(ValueError):
    """Retained model bytes cannot safely support operational guidance."""


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _public_capture_payload(
    *,
    capture: Any,
    receipts: Mapping[str, Any],
    payloads: Mapping[str, bytes],
    query_count: int,
) -> tuple[Any, bytes]:
    """Validate and unwrap the exact live public-source Open-Meteo capture."""

    if (
        capture.provider != "openmeteo"
        or capture.product != PUBLIC_CAPTURE_PRODUCT
        or capture.product_timestamp is not None
        or set(receipts) != {PUBLIC_RESPONSE_ROLE}
        or set(payloads) != {PUBLIC_RESPONSE_ROLE}
    ):
        raise OpenMeteoOperationalGuidanceError("public_capture_contract_invalid")
    receipt = receipts[PUBLIC_RESPONSE_ROLE]
    payload = payloads[PUBLIC_RESPONSE_ROLE]
    if (
        receipt.provider != "openmeteo"
        or receipt.product != PUBLIC_RESPONSE_PRODUCT
        or receipt.content_type != PUBLIC_RESPONSE_CONTENT_TYPE
        or not receipt.fetch_id.endswith(":response_bytes")
        or not capture.fetch_id.endswith(":capture")
        or receipt.fetch_id.removesuffix(":response_bytes")
        != capture.fetch_id.removesuffix(":capture")
        or len(payload) != receipt.size_bytes
        or hashlib.sha256(payload).hexdigest() != receipt.content_sha256
    ):
        raise OpenMeteoOperationalGuidanceError("public_response_contract_invalid")

    provenance = receipt.provenance
    fixed = {
        "access_class": "public_anonymous_no_secret",
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": PUBLIC_COLLECTOR,
        "collector_version": PUBLIC_COLLECTOR_VERSION,
        "content_encoding": "identity",
        "endpoint_host": PUBLIC_ENDPOINT_HOST,
        "endpoint_path": PUBLIC_ENDPOINT_PATH,
        "legal_advice": False,
        "operational_admission": False,
        "parse_attempted": False,
        "provider_rights_claim": False,
        "provenance_kind": PUBLIC_PROVENANCE_KIND,
        "query_count": query_count,
        "representation": NATIVE_REPRESENTATION,
        "reported_media_type": "application/json",
        "response_status": 200,
        "retention_right_claim": False,
        "semantic_status": "unverified",
        "weather_state": "unknown",
    }
    if any(provenance.get(key) != value for key, value in fixed.items()):
        raise OpenMeteoOperationalGuidanceError("public_response_provenance_invalid")
    if any(
        not _is_sha256(provenance.get(key))
        for key in (
            "final_url_sha256",
            "request_sha256",
            "response_headers_sha256",
            "transport_sha256",
        )
    ):
        raise OpenMeteoOperationalGuidanceError("public_response_provenance_invalid")
    declared_length = provenance.get("declared_length")
    if declared_length is not None and declared_length != receipt.size_bytes:
        raise OpenMeteoOperationalGuidanceError("public_response_provenance_invalid")
    transport = {
        "content_sha256": receipt.content_sha256,
        "declared_length": declared_length,
        "final_url_sha256": provenance["final_url_sha256"],
        "received_at": receipt.received_at,
        "reported_media_type": provenance["reported_media_type"],
        "request_sha256": provenance["request_sha256"],
        "response_headers_sha256": provenance["response_headers_sha256"],
        "size_bytes": receipt.size_bytes,
        "status": provenance["response_status"],
        "transport_contract": "stv3-public-source-https-transport-v1",
    }
    transport_sha256 = _sha(transport)
    if transport_sha256 != provenance["transport_sha256"]:
        raise OpenMeteoOperationalGuidanceError("public_response_transport_invalid")
    expected_upstream = (
        f"{PUBLIC_ENDPOINT_HOST}{PUBLIC_ENDPOINT_PATH}"
        f"|request={provenance['request_sha256']}"
        f"|transport={transport_sha256}|content={receipt.content_sha256}"
    )
    expected_capture_upstream = (
        f"{PUBLIC_CAPTURE_PRODUCT}|request={provenance['request_sha256']}"
        f"|transport={transport_sha256}|content={receipt.content_sha256}"
    )
    if (
        receipt.upstream_id != expected_upstream
        or capture.upstream_capture_id != expected_capture_upstream
    ):
        raise OpenMeteoOperationalGuidanceError("public_response_identity_invalid")
    return receipt, payload


def _finite(value: object, label: str, *, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OpenMeteoOperationalGuidanceError(f"{label}_invalid")
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= maximum:
        raise OpenMeteoOperationalGuidanceError(f"{label}_invalid")
    return result


def _sha(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class OpenMeteoTargetGuidance:
    target_id: str
    product_valid_at: float
    received_at: float
    current_precipitation_mm_15m: float
    current_rate_mm_h: float
    first_wet_at: float | None
    forecast_total_90m_mm: float
    source_receipt_sha256: str
    certificate_sha256: str
    process_receipt_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.target_id, str) or _SAFE_ID.fullmatch(self.target_id) is None:
            raise OpenMeteoOperationalGuidanceError("target_id_invalid")
        for label in ("product_valid_at", "received_at"):
            value = getattr(self, label)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise OpenMeteoOperationalGuidanceError(f"{label}_invalid")
            if not math.isfinite(float(value)) or float(value) < 0.0:
                raise OpenMeteoOperationalGuidanceError(f"{label}_invalid")
        _finite(
            self.current_precipitation_mm_15m,
            "current_precipitation",
            maximum=MAX_PRECIPITATION_MM_15M,
        )
        _finite(
            self.current_rate_mm_h,
            "current_rate",
            maximum=MAX_PRECIPITATION_MM_15M * 4.0,
        )
        _finite(
            self.forecast_total_90m_mm,
            "forecast_total",
            maximum=MAX_PRECIPITATION_MM_15M * FORECAST_STEPS_90M,
        )
        if self.first_wet_at is not None and (
            isinstance(self.first_wet_at, bool)
            or not isinstance(self.first_wet_at, (int, float))
            or not math.isfinite(float(self.first_wet_at))
            or float(self.first_wet_at) < float(self.product_valid_at)
        ):
            raise OpenMeteoOperationalGuidanceError("first_wet_at_invalid")
        for label in (
            "source_receipt_sha256",
            "certificate_sha256",
            "process_receipt_sha256",
        ):
            value = getattr(self, label)
            if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise OpenMeteoOperationalGuidanceError(f"{label}_invalid")

    def process_mapping(self) -> dict[str, Any]:
        return {
            "contract": CONTRACT,
            "current_precipitation_mm_15m": self.current_precipitation_mm_15m,
            "current_rate_mm_h": self.current_rate_mm_h,
            "first_wet_at": self.first_wet_at,
            "forecast_total_90m_mm": self.forecast_total_90m_mm,
            "product_valid_at": self.product_valid_at,
            "received_at": self.received_at,
            "source_receipt_sha256": self.source_receipt_sha256,
            "target_id": self.target_id,
        }


def decode_openmeteo_guidance(
    *,
    raw_store: RawArtifactStore,
    capture_id: str,
    desired_state: DesiredState,
) -> tuple[OpenMeteoTargetGuidance, ...]:
    """Decode one exact capture into bounded, target-bound model guidance."""

    if not isinstance(raw_store, RawArtifactStore):
        raise TypeError("raw_store_invalid")
    if not isinstance(desired_state, DesiredState):
        raise TypeError("desired_state_invalid")
    target_ids = openmeteo_response_target_ids(desired_state)
    if not target_ids:
        return ()
    bundle = raw_store.load_capture_bundle_bytes(capture_id)
    if bundle is None:
        raise OpenMeteoOperationalGuidanceError("capture_receipt_unavailable")
    capture, receipts, payloads = bundle
    if capture.product == PUBLIC_CAPTURE_PRODUCT:
        receipt, payload = _public_capture_payload(
            capture=capture,
            receipts=receipts,
            payloads=payloads,
            query_count=len(target_ids),
        )
    else:
        if (
            capture.provider != "openmeteo"
            or capture.product != OPENMETEO_CAPTURE_PRODUCT
            or set(receipts) != {"response_json"}
            or set(payloads) != {"response_json"}
        ):
            raise OpenMeteoOperationalGuidanceError("capture_contract_invalid")
        receipt = receipts["response_json"]
        payload = payloads["response_json"]
        if (
            receipt.provider != "openmeteo"
            or receipt.product != OPENMETEO_RESPONSE_PRODUCT
            or receipt.content_type != "application/json"
        ):
            raise OpenMeteoOperationalGuidanceError("response_receipt_invalid")
    items = decode_verified_openmeteo_response(
        payload,
        query_count=len(target_ids),
    )
    observations: list[OpenMeteoTargetGuidance] = []
    for target_id, item in zip(target_ids, items, strict=True):
        current = item["current"]
        minutely = item["minutely_15"]
        current_at = float(current["time"])
        current_mm = _finite(
            current["precipitation"],
            "current_precipitation",
            maximum=MAX_PRECIPITATION_MM_15M,
        )
        future_amounts = tuple(
            _finite(
                value,
                "forecast_precipitation",
                maximum=MAX_PRECIPITATION_MM_15M,
            )
            for value in minutely["precipitation"][1:FORECAST_STEPS_90M + 1]
        )
        future_times = tuple(
            float(value)
            for value in minutely["time"][1:FORECAST_STEPS_90M + 1]
        )
        first_wet_at = next(
            (
                timestamp
                for timestamp, amount in zip(future_times, future_amounts, strict=True)
                if amount >= WET_THRESHOLD_MM
            ),
            None,
        )
        values = {
            "current_precipitation_mm_15m": round(current_mm, 3),
            "current_rate_mm_h": round(current_mm * 4.0, 3),
            "first_wet_at": first_wet_at,
            "forecast_total_90m_mm": round(sum(future_amounts), 3),
            "product_valid_at": current_at,
            "received_at": float(receipt.received_at),
            "source_receipt_sha256": receipt.receipt_sha256,
            "target_id": target_id,
        }
        observations.append(OpenMeteoTargetGuidance(
            **values,
            certificate_sha256=_CERTIFICATE_SHA256,
            process_receipt_sha256=_sha({
                "contract": CONTRACT,
                **values,
            }),
        ))
    return tuple(observations)


def _fact_receipt(
    desired_state: DesiredState,
    snapshot: Mapping[str, Any],
    guidance: OpenMeteoTargetGuidance,
    *,
    fact_kind: OperationalFactKind,
    fact_value: Any,
    reason: str,
) -> FactAdmissionReceipt:
    return FactAdmissionReceipt.create(
        fact_kind=fact_kind,
        scope_id=guidance.target_id,
        fact_value=fact_value,
        desired_state_sha256=desired_state.state_sha256,
        authority_epoch=desired_state.authority_epoch,
        source_receipt_sha256s=(guidance.source_receipt_sha256,),
        certificate_sha256s=(guidance.certificate_sha256,),
        process_receipt_sha256=guidance.process_receipt_sha256,
        full_scan_complete=False,
        external_truth_validated=False,
        decided_at=float(snapshot["evaluated_at"]),
        valid_until=float(snapshot["valid_until"]),
        disposition=AdmissionDisposition.ADMITTED,
        reason_codes=(reason,),
    )


def enrich_revision_with_openmeteo_guidance(
    desired_state: DesiredState,
    base: CommittedOperationalFactRevision,
    guidances: Iterable[OpenMeteoTargetGuidance],
) -> CommittedOperationalFactRevision:
    """Add current positive and 90-minute model guidance to a base revision."""

    validate_committed_operational_revision(desired_state, base)
    values = tuple(sorted(guidances, key=lambda item: item.target_id))
    if not values:
        return base
    if len({item.target_id for item in values}) != len(values):
        raise OpenMeteoOperationalGuidanceError("duplicate_target_guidance")
    snapshot = copy.deepcopy(dict(base.snapshot))
    evaluated_at = float(snapshot["evaluated_at"])
    current_providers = {
        item["provider_id"]
        for item in snapshot["providers"]["items"]
        if item["freshness"] == "current"
    }
    if SOURCE_ID not in current_providers:
        return base
    targets = {item["target_id"]: item for item in snapshot["targets"]["items"]}
    available_targets = {
        item.target_id for item in desired_state.targets if item.available
    }
    receipts = list(base.admission_receipts)
    for guidance in values:
        target = targets.get(guidance.target_id)
        age = evaluated_at - guidance.product_valid_at
        if (
            target is None
            or guidance.target_id not in available_targets
            or age < -MAX_FUTURE_SKEW_SECONDS
            or age > MAX_MODEL_AGE_SECONDS
            or guidance.received_at > evaluated_at + MAX_FUTURE_SKEW_SECONDS
        ):
            continue
        current = target["current_precipitation"]
        if (
            guidance.current_precipitation_mm_15m >= WET_THRESHOLD_MM
            and current["state"] != "raining"
        ):
            current.update({
                "state": "raining",
                "freshness": "current",
                "rain_rate_mm_h": guidance.current_rate_mm_h,
                "product_valid_at": guidance.product_valid_at,
                "provider_ids": [SOURCE_ID],
                "reason_codes": [CURRENT_REASON],
            })
            target["status_text"] = (
                "Model: neerslag nu, "
                f"circa {guidance.current_rate_mm_h:.1f} mm/u"
            )
            receipts.append(_fact_receipt(
                desired_state,
                snapshot,
                guidance,
                fact_kind=OperationalFactKind.CURRENT_RAINING,
                fact_value=current,
                reason=CURRENT_REASON,
            ))
        elif current["freshness"] != "current":
            current.update({
                "state": "unknown",
                "freshness": "current",
                "rain_rate_mm_h": None,
                "product_valid_at": guidance.product_valid_at,
                "provider_ids": [SOURCE_ID],
                "reason_codes": [NO_CURRENT_REASON],
            })
        threat = target["next_threat"]
        if threat["state"] != "unknown":
            continue
        if guidance.first_wet_at is None:
            threat.update({
                "freshness": "current",
                "product_valid_at": guidance.product_valid_at,
                "provider_ids": [SOURCE_ID],
                "reason_codes": [NO_THREAT_REASON],
            })
            if not target["status_text"]:
                target["status_text"] = (
                    "Model: geen neerslagsignaal in de komende 90 min"
                )
            continue
        eta_minutes = round(max(0.0, guidance.first_wet_at - evaluated_at) / 60.0, 1)
        track_token = hashlib.sha256(
            f"{guidance.target_id}:{guidance.first_wet_at}".encode("utf-8")
        ).hexdigest()[:24]
        threat.update({
            "state": "available",
            "freshness": "current",
            "track_id": f"model:openmeteo:{track_token}",
            "calibrated_probability_percent": None,
            "probability_calibration_id": None,
            "eta_minutes": eta_minutes,
            "distance_km": None,
            "passage_status": "possible",
            "product_valid_at": guidance.product_valid_at,
            "provider_ids": [SOURCE_ID],
            "reason_codes": [THREAT_REASON],
        })
        if current["state"] != "raining":
            target["status_text"] = (
                "Modelverwachting: neerslag mogelijk over "
                f"{round(eta_minutes):d} min"
            )
        receipts.append(_fact_receipt(
            desired_state,
            snapshot,
            guidance,
            fact_kind=OperationalFactKind.NEXT_THREAT_AVAILABLE,
            fact_value=threat,
            reason=THREAT_REASON,
        ))
    committed = CommittedOperationalFactRevision.create(
        desired_state_sha256=desired_state.state_sha256,
        authority_epoch=desired_state.authority_epoch,
        snapshot=validate_snapshot(snapshot),
        admission_receipts=receipts,
    )
    validate_committed_operational_revision(desired_state, committed)
    return committed


__all__ = [
    "CURRENT_REASON",
    "NO_CURRENT_REASON",
    "NO_THREAT_REASON",
    "OpenMeteoOperationalGuidanceError",
    "OpenMeteoTargetGuidance",
    "SOURCE_ID",
    "THREAT_REASON",
    "decode_openmeteo_guidance",
    "enrich_revision_with_openmeteo_guidance",
]
