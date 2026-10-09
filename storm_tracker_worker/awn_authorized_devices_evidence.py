"""Pure availability evidence for one verified AWN authorized-devices capture.

This module deliberately does not decode weather.  The existing exact-raw AWN
collector certifies an authenticated inventory response, but explicitly does
not certify instantaneous-rain semantics.  We therefore expose only bounded,
privacy-preserving device/station availability facts.  No name, MAC address,
coordinate, raw value, credential, network call, filesystem access, admission
decision, or alert is produced here.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
from typing import Any, Literal, Mapping

from . import awn_native as native
from .raw_artifacts import (
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RawArtifactCaptureMember,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)


AWN_DEVICE_AVAILABILITY_CONTRACT = (
    "stv3-worker-awn-authorized-device-availability-v1"
)
AWN_DEVICE_AVAILABILITY_BATCH_CONTRACT = (
    "stv3-worker-awn-authorized-device-availability-batch-v1"
)
AWN_DEVICE_AVAILABILITY_PROCESS_RECEIPT = (
    "stv3-worker-awn-authorized-device-availability-process-receipt-v1"
)
DECODER_IDENTITY = "ambientweather_authorized_devices_availability"
DECODER_VERSION = "awn-authorized-devices-availability-v1"

HARD_MAX_PAYLOAD_BYTES = native.MAX_RESPONSE_BYTES
HARD_MAX_DEVICES = native.MAX_DEVICES
HARD_MAX_JSON_NODES = native.MAX_JSON_NODES
HARD_MAX_JSON_DEPTH = native.MAX_JSON_DEPTH
HARD_MAX_KEY_CHARS = 256
HARD_MAX_STRING_CHARS = 65_536

_JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,479}$")
_RAIN_FIELDS = frozenset({
    "hourlyrainin",
    "eventrainin",
    "dailyrainin",
    "weeklyrainin",
    "monthlyrainin",
    "yearlyrainin",
    "rainratein",
})
_KNOWN_DEVICE_KEYS = frozenset({"macAddress", "info", "coords", "lastData"})

IdentifierState = Literal["present_private", "absent_or_invalid"]
GeometryState = Literal["known_private", "absent_or_invalid"]
LatestDataState = Literal["present_uninterpreted", "absent_or_invalid"]
TimestampFieldState = Literal[
    "present_unverified_numeric",
    "present_unverified_non_numeric",
    "absent",
]


class AwnAvailabilityEvidenceError(RuntimeError):
    """Base failure; it never implies weather or device absence."""


class AwnAvailabilityAuthorityError(AwnAvailabilityEvidenceError):
    """The supplied raw bytes/receipts do not form the exact AWN authority."""


class AwnAvailabilityStructureError(AwnAvailabilityEvidenceError):
    """The verified payload is not the closed bounded inventory structure."""


class AwnAvailabilityCapacityError(AwnAvailabilityEvidenceError):
    """A hard parse/evidence bound was exceeded without partial output."""


class _JsonNumber(str):
    """An exact JSON numeric lexeme, kept distinct from a JSON string."""


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _tag_json(value: object) -> object:
    """Return a type-tagged tree so string/number hashes cannot collide."""
    if isinstance(value, _JsonNumber):
        return ["number", str(value)]
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["boolean", value]
    if isinstance(value, str):
        return ["string", value]
    if isinstance(value, list):
        return ["array", [_tag_json(item) for item in value]]
    if isinstance(value, dict):
        return [
            "object",
            [[key, _tag_json(value[key])] for key in sorted(value)],
        ]
    raise AwnAvailabilityStructureError("AWN JSON bevat een onbekend waardetype")


def _object_without_duplicates(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise AwnAvailabilityStructureError(
                f"AWN JSON bevat dubbele objectsleutel {key!r}"
            )
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    raise AwnAvailabilityStructureError(
        f"AWN JSON bevat verboden numerieke constante {value}"
    )


def _parse_json(payload: bytes) -> object:
    try:
        text = payload.decode("utf-8")
        parsed = json.loads(
            text,
            object_pairs_hook=_object_without_duplicates,
            parse_float=_JsonNumber,
            parse_int=_JsonNumber,
            parse_constant=_reject_constant,
        )
    except UnicodeDecodeError as exc:
        raise AwnAvailabilityStructureError("AWN payload is geen UTF-8") from exc
    except json.JSONDecodeError as exc:
        raise AwnAvailabilityStructureError("AWN payload is geen JSON") from exc

    nodes = 0
    stack: list[tuple[object, int]] = [(parsed, 1)]
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > HARD_MAX_JSON_NODES:
            raise AwnAvailabilityCapacityError("AWN JSON overschrijdt nodegrens")
        if depth > HARD_MAX_JSON_DEPTH:
            raise AwnAvailabilityCapacityError("AWN JSON overschrijdt dieptegrens")
        if isinstance(current, dict):
            for key, value in current.items():
                if len(key) > HARD_MAX_KEY_CHARS:
                    raise AwnAvailabilityCapacityError("AWN JSON-sleutel is te lang")
                stack.append((value, depth + 1))
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in current)
        elif isinstance(current, str) and len(current) > HARD_MAX_STRING_CHARS:
            raise AwnAvailabilityCapacityError("AWN JSON-string is te lang")
    return parsed


def _exact_decimal(value: object) -> Decimal | None:
    if not isinstance(value, _JsonNumber):
        return None
    try:
        parsed = Decimal(str(value))
    except InvalidOperation:
        return None
    return parsed if parsed.is_finite() else None


def _coordinates_are_valid(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    latitude = _exact_decimal(value.get("lat"))
    longitude = _exact_decimal(value.get("lon"))
    return (
        latitude is not None
        and longitude is not None
        and Decimal("-90") <= latitude <= Decimal("90")
        and Decimal("-180") <= longitude <= Decimal("180")
    )


def _raw_self_integrity(
    capture: RawArtifactCaptureReceipt,
    receipt: RawArtifactReceipt,
    payload: bytes,
) -> None:
    if len(payload) != receipt.size_bytes or hashlib.sha256(payload).hexdigest() != (
        receipt.content_sha256
    ):
        raise AwnAvailabilityAuthorityError("AWN payload wijkt af van raw receipt")
    try:
        provenance = receipt.provenance
        canonical_provenance = _canonical_json(provenance).decode("utf-8")
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise AwnAvailabilityAuthorityError("AWN provenance is niet canoniek") from exc
    if (
        receipt._provenance_json != canonical_provenance  # noqa: SLF001
        or receipt.provenance_sha256
        != hashlib.sha256(canonical_provenance.encode("utf-8")).hexdigest()
        or receipt.receipt_id
        != RawArtifactStore._receipt_id(  # noqa: SLF001
            provider=receipt.provider,
            product=receipt.product,
            fetch_id=receipt.fetch_id,
        )
        or receipt.receipt_sha256
        != RawArtifactStore._receipt_sha256(  # noqa: SLF001
            provider=receipt.provider,
            product=receipt.product,
            fetch_id=receipt.fetch_id,
            content_type=receipt.content_type,
            received_at=receipt.received_at,
            upstream_id=receipt.upstream_id,
            content_sha256=receipt.content_sha256,
            size_bytes=receipt.size_bytes,
            provenance_sha256=receipt.provenance_sha256,
        )
    ):
        raise AwnAvailabilityAuthorityError("AWN raw receiptchecksum klopt niet")
    if (
        not math.isfinite(receipt.received_at)
        or receipt.received_at < 0.0
        or not math.isfinite(receipt.created_at)
        or receipt.created_at < 0.0
    ):
        raise AwnAvailabilityAuthorityError("AWN raw receipttijd is ongeldig")
    if len(capture.members) != 1:
        raise AwnAvailabilityAuthorityError("AWN capture heeft niet exact één lid")
    member = capture.members[0]
    if (
        member.role != "response_json"
        or member.receipt_id != receipt.receipt_id
        or member.content_sha256 != receipt.content_sha256
        or member.receipt_sha256 != receipt.receipt_sha256
        or member.received_at != receipt.received_at
    ):
        raise AwnAvailabilityAuthorityError("AWN capturelid wijkt af van receipt")
    manifest = {
        "fetch_id": capture.fetch_id,
        "members": [{
            "content_sha256": member.content_sha256,
            "received_at": member.received_at,
            "receipt_id": member.receipt_id,
            "role": member.role,
        }],
        "product": capture.product,
        "product_timestamp": capture.product_timestamp,
        "provider": capture.provider,
        "schema": RAW_ARTIFACT_CAPTURE_SCHEMA,
        "upstream_capture_id": capture.upstream_capture_id,
    }
    if (
        capture.manifest_sha256 != _sha256_json(manifest)
        or capture.capture_id
        != RawArtifactStore._capture_id(  # noqa: SLF001
            provider=capture.provider,
            product=capture.product,
            fetch_id=capture.fetch_id,
        )
        or capture.capture_receipt_sha256
        != RawArtifactStore._capture_receipt_sha256(  # noqa: SLF001
            capture_id=capture.capture_id,
            provider=capture.provider,
            product=capture.product,
            fetch_id=capture.fetch_id,
            upstream_capture_id=capture.upstream_capture_id,
            product_timestamp=capture.product_timestamp,
            manifest_sha256=capture.manifest_sha256,
            members=(member,),
        )
        or not math.isfinite(capture.created_at)
        or capture.created_at < 0.0
    ):
        raise AwnAvailabilityAuthorityError("AWN capturechecksum klopt niet")


@dataclass(frozen=True, slots=True)
class AwnDeviceAvailabilityEvidence:
    device_ref: str
    source_record_locator: str
    source_record_sha256: str
    identifier_state: IdentifierState
    station_geometry_state: GeometryState
    latest_data_state: LatestDataState
    timestamp_field_state: TimestampFieldState
    rain_field_present: bool
    last_data_key_count: int
    last_data_keys_sha256: str
    unclassified_device_key_count: int
    unclassified_device_keys_sha256: str

    @property
    def meteorological_value_state(self) -> str:
        return "not_decoded_inventory_only"

    @property
    def qc_state(self) -> str:
        return "not_certified_by_authorized_devices_contract"

    @property
    def unit_state(self) -> str:
        return "not_applicable_no_value"

    def to_mapping(self) -> dict[str, object]:
        return {
            "contract": AWN_DEVICE_AVAILABILITY_CONTRACT,
            "device_ref": self.device_ref,
            "source_record_locator": self.source_record_locator,
            "source_record_sha256": self.source_record_sha256,
            "identifier_state": self.identifier_state,
            "station_geometry_state": self.station_geometry_state,
            "latest_data_state": self.latest_data_state,
            "timestamp_field_state": self.timestamp_field_state,
            "rain_field_present": self.rain_field_present,
            "last_data_key_count": self.last_data_key_count,
            "last_data_keys_sha256": self.last_data_keys_sha256,
            "unclassified_device_key_count": self.unclassified_device_key_count,
            "unclassified_device_keys_sha256": (
                self.unclassified_device_keys_sha256
            ),
            "meteorological_value_state": self.meteorological_value_state,
            "qc_state": self.qc_state,
            "unit_state": self.unit_state,
        }


@dataclass(frozen=True, slots=True)
class AwnDeviceAvailabilityBatch:
    capture_id: str
    capture_manifest_sha256: str
    capture_receipt_sha256: str
    raw_receipt_id: str
    raw_receipt_sha256: str
    raw_content_sha256: str
    raw_size_bytes: int
    received_at: float
    decoded_at: float
    devices: tuple[AwnDeviceAvailabilityEvidence, ...]

    @property
    def operational_admission_eligible(self) -> bool:
        return False

    @property
    def weather_state(self) -> str:
        return "unknown"

    @property
    def product_time_state(self) -> str:
        return "not_certified_inventory_capture"

    @property
    def meteorological_value_count(self) -> int:
        return 0

    def _body(self) -> dict[str, object]:
        return {
            "contract": AWN_DEVICE_AVAILABILITY_BATCH_CONTRACT,
            "decoder": {
                "identity": DECODER_IDENTITY,
                "version": DECODER_VERSION,
            },
            "raw_authority": {
                "capture_id": self.capture_id,
                "capture_manifest_sha256": self.capture_manifest_sha256,
                "capture_receipt_sha256": self.capture_receipt_sha256,
                "receipt_id": self.raw_receipt_id,
                "receipt_sha256": self.raw_receipt_sha256,
                "content_sha256": self.raw_content_sha256,
                "size_bytes": self.raw_size_bytes,
                "received_at": self.received_at,
            },
            "decoded_at": self.decoded_at,
            "source_scope": "authorized_account_devices_only",
            "privacy_class": native.AWN_PRIVACY_CLASS,
            "product_time_state": self.product_time_state,
            "weather_state": self.weather_state,
            "meteorological_value_count": self.meteorological_value_count,
            "operational_admission_eligible": False,
            "device_count": len(self.devices),
            "devices": [item.to_mapping() for item in self.devices],
        }

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json(self._body())

    @property
    def size_bytes(self) -> int:
        return len(self.canonical_bytes)

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @property
    def batch_id(self) -> str:
        return "awn-availability-batch-" + _sha256_json({
            "contract": AWN_DEVICE_AVAILABILITY_BATCH_CONTRACT,
            "content_sha256": self.content_sha256,
        })

    @property
    def process_receipt_sha256(self) -> str:
        return _sha256_json({
            "schema": AWN_DEVICE_AVAILABILITY_PROCESS_RECEIPT,
            "batch_id": self.batch_id,
            "content_sha256": self.content_sha256,
            "capture_receipt_sha256": self.capture_receipt_sha256,
            "raw_receipt_sha256": self.raw_receipt_sha256,
        })

    def to_mapping(self) -> dict[str, object]:
        result = self._body()
        result.update({
            "batch_id": self.batch_id,
            "content_sha256": self.content_sha256,
            "process_receipt_schema": AWN_DEVICE_AVAILABILITY_PROCESS_RECEIPT,
            "process_receipt_sha256": self.process_receipt_sha256,
            "canonical_size_bytes": self.size_bytes,
        })
        return result


def decode_awn_authorized_device_availability(
    capture: RawArtifactCaptureReceipt,
    receipt: RawArtifactReceipt,
    payload: bytes,
    *,
    job_id: str,
) -> AwnDeviceAvailabilityBatch:
    """Decode only privacy-safe inventory availability from verified bytes."""
    if not _JOB_ID_RE.fullmatch(job_id):
        raise AwnAvailabilityAuthorityError("AWN job_id is ongeldig")
    if not isinstance(capture, RawArtifactCaptureReceipt) or not isinstance(
        receipt, RawArtifactReceipt
    ):
        raise AwnAvailabilityAuthorityError("AWN raw authoritytype ontbreekt")
    if not isinstance(payload, bytes) or len(payload) > HARD_MAX_PAYLOAD_BYTES:
        raise AwnAvailabilityCapacityError("AWN payload valt buiten bytegrens")
    try:
        native._validate_receipt(receipt, job_id=job_id)  # noqa: SLF001
        native._validate_capture(  # noqa: SLF001
            capture,
            job_id=job_id,
            receipt=receipt,
        )
    except native.AwnNativeError as exc:
        raise AwnAvailabilityAuthorityError("AWN raw authority faalt") from exc
    _raw_self_integrity(capture, receipt, payload)

    parsed = _parse_json(payload)
    if not isinstance(parsed, list):
        raise AwnAvailabilityStructureError("AWN authorized_devices is geen lijst")
    if len(parsed) > HARD_MAX_DEVICES:
        raise AwnAvailabilityCapacityError("AWN devicegrens is overschreden")

    devices: list[AwnDeviceAvailabilityEvidence] = []
    private_identifiers: set[str] = set()
    for index, raw_device in enumerate(parsed):
        if not isinstance(raw_device, dict):
            raise AwnAvailabilityStructureError("AWN device is geen object")
        record_sha256 = _sha256_json(_tag_json(raw_device))
        identifier = raw_device.get("macAddress")
        identifier_valid = (
            isinstance(identifier, str)
            and bool(identifier)
            and identifier == identifier.strip()
            and len(identifier) <= 512
        )
        if identifier_valid:
            assert isinstance(identifier, str)
            if identifier in private_identifiers:
                raise AwnAvailabilityStructureError(
                    "AWN inventory bevat een dubbel device-ID"
                )
            private_identifiers.add(identifier)
        latest = raw_device.get("lastData")
        latest_object = latest if isinstance(latest, dict) else None
        timestamp_value = (
            latest_object.get("dateutc") if latest_object is not None else None
        )
        if latest_object is None or "dateutc" not in latest_object:
            timestamp_state: TimestampFieldState = "absent"
        elif _exact_decimal(timestamp_value) is not None:
            timestamp_state = "present_unverified_numeric"
        else:
            timestamp_state = "present_unverified_non_numeric"
        latest_keys = sorted(latest_object) if latest_object is not None else []
        unknown_keys = sorted(set(raw_device) - _KNOWN_DEVICE_KEYS)
        device_ref = "awn-device-" + _sha256_json({
            "schema": AWN_DEVICE_AVAILABILITY_CONTRACT,
            "capture_content_sha256": receipt.content_sha256,
            "source_record_index": index,
            "source_record_sha256": record_sha256,
        })
        devices.append(AwnDeviceAvailabilityEvidence(
            device_ref=device_ref,
            source_record_locator=f"json:device_index={index}",
            source_record_sha256=record_sha256,
            identifier_state=(
                "present_private" if identifier_valid else "absent_or_invalid"
            ),
            station_geometry_state=(
                "known_private"
                if _coordinates_are_valid(raw_device.get("coords"))
                else "absent_or_invalid"
            ),
            latest_data_state=(
                "present_uninterpreted"
                if latest_object is not None
                else "absent_or_invalid"
            ),
            timestamp_field_state=timestamp_state,
            rain_field_present=(
                latest_object is not None
                and bool(_RAIN_FIELDS.intersection(latest_object))
            ),
            last_data_key_count=len(latest_keys),
            last_data_keys_sha256=_sha256_json(latest_keys),
            unclassified_device_key_count=len(unknown_keys),
            unclassified_device_keys_sha256=_sha256_json(unknown_keys),
        ))

    return AwnDeviceAvailabilityBatch(
        capture_id=capture.capture_id,
        capture_manifest_sha256=capture.manifest_sha256,
        capture_receipt_sha256=capture.capture_receipt_sha256,
        raw_receipt_id=receipt.receipt_id,
        raw_receipt_sha256=receipt.receipt_sha256,
        raw_content_sha256=receipt.content_sha256,
        raw_size_bytes=receipt.size_bytes,
        received_at=receipt.received_at,
        decoded_at=max(receipt.received_at, capture.created_at),
        devices=tuple(devices),
    )


__all__ = [
    "AWN_DEVICE_AVAILABILITY_BATCH_CONTRACT",
    "AWN_DEVICE_AVAILABILITY_CONTRACT",
    "AWN_DEVICE_AVAILABILITY_PROCESS_RECEIPT",
    "AwnAvailabilityAuthorityError",
    "AwnAvailabilityCapacityError",
    "AwnAvailabilityEvidenceError",
    "AwnAvailabilityStructureError",
    "AwnDeviceAvailabilityBatch",
    "AwnDeviceAvailabilityEvidence",
    "DECODER_IDENTITY",
    "DECODER_VERSION",
    "decode_awn_authorized_device_availability",
]
