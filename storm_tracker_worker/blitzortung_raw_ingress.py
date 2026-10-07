"""Fail-closed exact-byte boundary for Blitzortung MQTT deliveries.

This module is intentionally *not* an MQTT client.  A future bounded worker
transport may hand one already-received MQTT envelope to
``persist_blitzortung_mqtt_envelope``.  The function validates caller-known
transport facts and makes the exact payload durable in the schema-3 raw store.
It never parses JSON and never emits a lightning or absence observation.

There is deliberately no queue, retry loop, scheduler, service handler,
configuration reader or Home Assistant integration here.  Capacity and
storage errors propagate to the caller; they can therefore never be silently
relabelled as dry weather or as an absence of lightning.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import re

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactStore,
)


METHOD = "worker_blitzortung_qos0_exact_raw_ingress_v1"
REFERENCE_SCHEMA = "stv3-worker-blitzortung-raw-ingress-reference-v1"
PROVIDER = "blitzortung"
PAYLOAD_PRODUCT = "blitzortung_mqtt_event_payload"
CAPTURE_PRODUCT = "blitzortung_mqtt_event_capture"
PAYLOAD_ROLE = "mqtt_payload"
CONTENT_TYPE = "application/octet-stream"
COLLECTOR = "blitzortung_raw_ingress"
COLLECTOR_VERSION = "blitzortung-raw-ingress-v1"

# MQTT QoS 0 has neither broker acknowledgement nor replay completeness.  This
# exact literal is part of the durable contract and may not be upgraded by a
# downstream consumer merely because one message was captured successfully.
TRANSPORT_COMPLETENESS = "best_effort_qos0_unacknowledged"
ABSENCE_SEMANTICS = "unknown_transport_coverage"
METEOROLOGICAL_INTERPRETATION = "not_performed"

MAX_PAYLOAD_BYTES = 256 * 1024
MAX_TOPIC_BYTES = 512

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TOPIC_RE = re.compile(
    r"^blitzortung/1\.1/[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*$"
)
_SENSITIVE_TOPIC_SEGMENTS = frozenset({
    "api_key",
    "apikey",
    "auth",
    "authorization",
    "key",
    "password",
    "secret",
    "token",
})


class BlitzortungRawIngressValidationError(ValueError):
    """The supplied transport envelope is unsafe, ambiguous or unbounded."""


@dataclass(frozen=True, slots=True)
class BlitzortungMqttEnvelope:
    """One exact, caller-captured MQTT delivery.

    Identity fields are lower-case SHA-256 values rather than arbitrary
    strings.  The ingress therefore has no credential-shaped or free-form
    identity fields.  They must identify public broker/session/delivery facts,
    never authentication material.
    """

    payload: bytes
    broker_identity_sha256: str
    session_identity_sha256: str
    delivery_identity_sha256: str
    topic: str
    qos: int
    retain: bool
    received_at: float
    desired_state_sha256: str
    subscription_set_sha256: str


@dataclass(frozen=True, slots=True)
class BlitzortungRawIngressReference:
    """Immutable, content-bound and payload-free raw delivery reference."""

    schema: str
    method: str
    raw_store_schema_version: int
    receipt_id: str
    receipt_sha256: str
    provenance_sha256: str
    capture_id: str
    capture_receipt_sha256: str
    manifest_sha256: str
    content_sha256: str
    size_bytes: int
    received_at: float
    topic: str
    qos: int
    retain: bool
    broker_identity_sha256: str
    session_identity_sha256: str
    delivery_identity_sha256: str
    desired_state_sha256: str
    subscription_set_sha256: str
    transport_completeness: str
    meteorological_interpretation: str
    absence_semantics: str
    payload_parsed: bool
    operational_admission: bool
    reference_sha256: str

    def __post_init__(self) -> None:
        if self.schema != REFERENCE_SCHEMA or self.method != METHOD:
            raise BlitzortungRawIngressValidationError(
                "raw-ingressreference heeft een onbekend contract"
            )
        if self.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION:
            raise BlitzortungRawIngressValidationError(
                "raw-ingressreference wijkt af van schema-3 authority"
            )
        for name in (
            "receipt_sha256",
            "provenance_sha256",
            "capture_receipt_sha256",
            "manifest_sha256",
            "content_sha256",
            "broker_identity_sha256",
            "session_identity_sha256",
            "delivery_identity_sha256",
            "desired_state_sha256",
            "subscription_set_sha256",
            "reference_sha256",
        ):
            _sha256(getattr(self, name), name)
        for name in ("receipt_id", "capture_id"):
            value = getattr(self, name)
            if (
                not isinstance(value, str)
                or not value
                or len(value) > 128
                or any(ord(character) < 33 or ord(character) > 126 for character in value)
            ):
                raise BlitzortungRawIngressValidationError(
                    f"raw-ingressreference bevat een ongeldig {name}"
                )
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or not 0 <= self.size_bytes <= MAX_PAYLOAD_BYTES
        ):
            raise BlitzortungRawIngressValidationError(
                "raw-ingressreference bevat een ongeldige payloadlengte"
            )
        _received_at(self.received_at)
        _topic(self.topic)
        if isinstance(self.qos, bool) or self.qos != 0:
            raise BlitzortungRawIngressValidationError(
                "raw-ingressreference vereist exact QoS 0"
            )
        if not isinstance(self.retain, bool):
            raise BlitzortungRawIngressValidationError(
                "raw-ingressreference retain moet boolean zijn"
            )
        if (
            self.transport_completeness != TRANSPORT_COMPLETENESS
            or self.meteorological_interpretation != METEOROLOGICAL_INTERPRETATION
            or self.absence_semantics != ABSENCE_SEMANTICS
            or self.payload_parsed is not False
            or self.operational_admission is not False
        ):
            raise BlitzortungRawIngressValidationError(
                "raw-ingressreference bevat een verboden transport- of weerclaim"
            )
        body = self.to_metadata()
        claimed = body.pop("reference_sha256")
        if hashlib.sha256(_canonical_json(body)).hexdigest() != claimed:
            raise BlitzortungRawIngressValidationError(
                "raw-ingressreferencechecksum bindt de velden niet"
            )

    def to_metadata(self) -> dict[str, object]:
        """Return a fresh payload-free metadata object."""

        return {
            "schema": self.schema,
            "method": self.method,
            "raw_store_schema_version": self.raw_store_schema_version,
            "receipt_id": self.receipt_id,
            "receipt_sha256": self.receipt_sha256,
            "provenance_sha256": self.provenance_sha256,
            "capture_id": self.capture_id,
            "capture_receipt_sha256": self.capture_receipt_sha256,
            "manifest_sha256": self.manifest_sha256,
            "content_sha256": self.content_sha256,
            "size_bytes": self.size_bytes,
            "received_at": self.received_at,
            "topic": self.topic,
            "qos": self.qos,
            "retain": self.retain,
            "broker_identity_sha256": self.broker_identity_sha256,
            "session_identity_sha256": self.session_identity_sha256,
            "delivery_identity_sha256": self.delivery_identity_sha256,
            "desired_state_sha256": self.desired_state_sha256,
            "subscription_set_sha256": self.subscription_set_sha256,
            "transport_completeness": self.transport_completeness,
            "meteorological_interpretation": (
                self.meteorological_interpretation
            ),
            "absence_semantics": self.absence_semantics,
            "payload_parsed": self.payload_parsed,
            "operational_admission": self.operational_admission,
            "reference_sha256": self.reference_sha256,
        }


def _sha256(value: object, name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise BlitzortungRawIngressValidationError(
            f"{name} moet exact één lower-case SHA-256 zijn"
        )
    return value


def _topic(value: object) -> str:
    if not isinstance(value, str):
        raise BlitzortungRawIngressValidationError("topic moet tekst zijn")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise BlitzortungRawIngressValidationError(
            "topic is geen geldige UTF-8"
        ) from exc
    if not encoded or len(encoded) > MAX_TOPIC_BYTES:
        raise BlitzortungRawIngressValidationError(
            "topic overschrijdt de harde bytegrens"
        )
    if _TOPIC_RE.fullmatch(value) is None or "+" in value or "#" in value:
        raise BlitzortungRawIngressValidationError(
            "topic is geen concreet publiek Blitzortung 1.1-topic"
        )
    if any(
        segment.lower() in _SENSITIVE_TOPIC_SEGMENTS
        for segment in value.split("/")
    ):
        raise BlitzortungRawIngressValidationError(
            "topic bevat credentialvormige tekst"
        )
    return value


def _received_at(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BlitzortungRawIngressValidationError(
            "received_at moet een eindig niet-negatief tijdstip zijn"
        )
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise BlitzortungRawIngressValidationError(
            "received_at moet een eindig niet-negatief tijdstip zijn"
        )
    return result


def _canonical_json(value: dict[str, object]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _delivery_key(envelope: BlitzortungMqttEnvelope) -> str:
    identity = {
        "broker_identity_sha256": envelope.broker_identity_sha256,
        "delivery_identity_sha256": envelope.delivery_identity_sha256,
        "schema": "stv3-blitzortung-mqtt-delivery-identity-v1",
        "session_identity_sha256": envelope.session_identity_sha256,
    }
    return hashlib.sha256(_canonical_json(identity)).hexdigest()


def _validate(
    store: object,
    envelope: object,
) -> tuple[RawArtifactStore, BlitzortungMqttEnvelope]:
    if not isinstance(store, RawArtifactStore):
        raise BlitzortungRawIngressValidationError(
            "raw_store moet een RawArtifactStore zijn"
        )
    if not isinstance(envelope, BlitzortungMqttEnvelope):
        raise BlitzortungRawIngressValidationError(
            "envelope moet BlitzortungMqttEnvelope zijn"
        )
    if not isinstance(envelope.payload, bytes):
        raise BlitzortungRawIngressValidationError(
            "payload moet exact immutable bytes zijn"
        )
    if len(envelope.payload) > MAX_PAYLOAD_BYTES:
        raise BlitzortungRawIngressValidationError(
            "payload overschrijdt de harde ingressgrens"
        )
    _sha256(envelope.broker_identity_sha256, "broker_identity_sha256")
    _sha256(envelope.session_identity_sha256, "session_identity_sha256")
    _sha256(envelope.delivery_identity_sha256, "delivery_identity_sha256")
    _sha256(envelope.desired_state_sha256, "desired_state_sha256")
    _sha256(envelope.subscription_set_sha256, "subscription_set_sha256")
    _topic(envelope.topic)
    if isinstance(envelope.qos, bool) or not isinstance(envelope.qos, int):
        raise BlitzortungRawIngressValidationError("qos moet exact integer 0 zijn")
    if envelope.qos != 0:
        raise BlitzortungRawIngressValidationError(
            "alleen expliciete Blitzortung QoS 0-deliveries zijn toegestaan"
        )
    if not isinstance(envelope.retain, bool):
        raise BlitzortungRawIngressValidationError("retain moet boolean zijn")
    _received_at(envelope.received_at)
    return store, envelope


def persist_blitzortung_mqtt_envelope(
    raw_store: RawArtifactStore,
    envelope: BlitzortungMqttEnvelope,
) -> BlitzortungRawIngressReference:
    """Persist exact bytes before any parse and return a closed reference.

    ``RawArtifactCapacityError``, ``RawArtifactConflictError``, integrity
    failures and filesystem errors intentionally propagate unchanged.  The
    caller must expose such a failure as an ingestion/storage fault.  It must
    never translate it to an empty lightning set or a weather-state claim.
    """

    store, exact = _validate(raw_store, envelope)
    received_at = _received_at(exact.received_at)
    delivery_key = _delivery_key(exact)
    fetch_prefix = f"blitzortung-mqtt-{delivery_key}"
    upstream_id = f"mqtt-delivery-{delivery_key}"

    provenance = {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": COLLECTOR,
        "collector_version": COLLECTOR_VERSION,
        "source_transport": "mqtt",
        "mqtt_protocol_profile": "mqtt_v3_1_1_delivery_envelope",
        "transport_completeness": TRANSPORT_COMPLETENESS,
        "broker_delivery_acknowledged": False,
        "topic": exact.topic,
        "qos": exact.qos,
        "retain": exact.retain,
        "received_at": received_at,
        "broker_identity_sha256": exact.broker_identity_sha256,
        "session_identity_sha256": exact.session_identity_sha256,
        "delivery_identity_sha256": exact.delivery_identity_sha256,
        "desired_state_sha256": exact.desired_state_sha256,
        "subscription_set_sha256": exact.subscription_set_sha256,
        "payload_parsed": False,
        "meteorological_interpretation": METEOROLOGICAL_INTERPRETATION,
        "absence_semantics": ABSENCE_SEMANTICS,
        "operational_admission": False,
    }

    # This is the only operation receiving ``exact.payload``.  No payload
    # inspection occurs before it, and this module performs no parsing after
    # it either.  A process crash can leave a valid immutable member receipt;
    # the identical delivery retry can then complete the one-member capture.
    receipt = store.store(
        provider=PROVIDER,
        product=PAYLOAD_PRODUCT,
        fetch_id=f"{fetch_prefix}:payload",
        content_type=CONTENT_TYPE,
        received_at=received_at,
        upstream_id=upstream_id,
        provenance=provenance,
        payload=exact.payload,
    )
    capture = store.commit_capture(
        provider=PROVIDER,
        product=CAPTURE_PRODUCT,
        fetch_id=f"{fetch_prefix}:capture",
        upstream_capture_id=upstream_id,
        product_timestamp=None,
        members={PAYLOAD_ROLE: receipt.receipt_id},
    )

    reference_body: dict[str, object] = {
        "schema": REFERENCE_SCHEMA,
        "method": METHOD,
        "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
        "receipt_id": receipt.receipt_id,
        "receipt_sha256": receipt.receipt_sha256,
        "provenance_sha256": receipt.provenance_sha256,
        "capture_id": capture.capture_id,
        "capture_receipt_sha256": capture.capture_receipt_sha256,
        "manifest_sha256": capture.manifest_sha256,
        "content_sha256": receipt.content_sha256,
        "size_bytes": receipt.size_bytes,
        "received_at": receipt.received_at,
        "topic": exact.topic,
        "qos": exact.qos,
        "retain": exact.retain,
        "broker_identity_sha256": exact.broker_identity_sha256,
        "session_identity_sha256": exact.session_identity_sha256,
        "delivery_identity_sha256": exact.delivery_identity_sha256,
        "desired_state_sha256": exact.desired_state_sha256,
        "subscription_set_sha256": exact.subscription_set_sha256,
        "transport_completeness": TRANSPORT_COMPLETENESS,
        "meteorological_interpretation": METEOROLOGICAL_INTERPRETATION,
        "absence_semantics": ABSENCE_SEMANTICS,
        "payload_parsed": False,
        "operational_admission": False,
    }
    reference_sha256 = hashlib.sha256(
        _canonical_json(reference_body)
    ).hexdigest()
    return BlitzortungRawIngressReference(
        **reference_body,
        reference_sha256=reference_sha256,
    )


__all__ = [
    "ABSENCE_SEMANTICS",
    "BlitzortungMqttEnvelope",
    "BlitzortungRawIngressReference",
    "BlitzortungRawIngressValidationError",
    "CAPTURE_PRODUCT",
    "MAX_PAYLOAD_BYTES",
    "MAX_TOPIC_BYTES",
    "METHOD",
    "PAYLOAD_PRODUCT",
    "REFERENCE_SCHEMA",
    "TRANSPORT_COMPLETENESS",
    "persist_blitzortung_mqtt_envelope",
]
