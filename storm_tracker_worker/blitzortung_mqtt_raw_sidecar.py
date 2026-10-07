"""Bounded worker-only MQTTv3.1.1 transport for Blitzortung exact bytes.

The sidecar subscribes only to regional precision-3 geohash prefixes.  A
callback is persisted through :class:`BlitzortungEventIngressAuthority`
before any other action; this module never parses the payload.  The durable
queue is then claimed and completed by an inert consumer so retained raw
objects remain available without allowing the event queue to fill normally.

No payload, topic, target coordinate, path, credential or identity hash is
returned by diagnostics.  Missing participant/permission authority prevents
raw-store initialization, MQTT client construction and network activation.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Callable, Iterable, Mapping

from .blitzortung_event_ingress import (
    ACCESS_RETENTION_AUTHORITY_ID,
    ABSENCE_SEMANTICS,
    TRANSPORT_COMPLETENESS,
    BlitzortungEventIngressAuthority,
    BlitzortungEventIngressBackpressure,
    BlitzortungEventIngressCapacityError,
    BlitzortungEventIngressConflict,
    BlitzortungEventIngressError,
    BlitzortungEventIngressLimits,
    BlitzortungEventIngressValidationError,
    BlitzortungRetentionDecision,
    BlitzortungSessionHandle,
)
from .blitzortung_raw_ingress import (
    MAX_PAYLOAD_BYTES,
    BlitzortungMqttEnvelope,
    BlitzortungRawIngressValidationError,
)
from .raw_artifacts import RawArtifactLimits, RawArtifactStore


CONTRACT = "stv3-worker-blitzortung-mqtt-raw-sidecar-v1"
DIAGNOSTICS_SCHEMA = "stv3-worker-blitzortung-mqtt-raw-sidecar-diagnostics-v2"
BROKER_PROFILE = "blitzortung_public_mqtt_v311_regional_qos0_v1"
BROKER_HOST = "blitzortung.ha.sed.pl"
BROKER_PORT = 1883
MQTT_PROTOCOL = "MQTTv311"
QOS = 0
DEFAULT_KEEPALIVE_SECONDS = 60
GEOHASH_PRECISION = 3
GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
MAX_REGIONS = 64
MAX_SUBSCRIPTION_TOPICS = 512
MAX_DIAGNOSTIC_COUNTER = 1_000_000_000
PRODUCTION_VALID_UNTIL = 253_402_300_799.0
SILENCE_INTERPRETATION = "unknown_not_an_absence_observation"
RAW_PAYLOAD_TRANSPORT = "worker_direct_not_ha_mqtt"
RAW_SHADOW_LIMITATIONS = (
    "append_only_capacity_exhaustion_fails_closed",
    "qos0_upstream_loss_is_unobservable",
    "linux_disconnect_thermal_ha_z2m_soak_evidence_pending",
    "legacy_ha_cutover_exclusivity_not_proven",
)

BROKER_IDENTITY_SHA256 = hashlib.sha256(
    f"{BROKER_PROFILE}:{BROKER_HOST}:{BROKER_PORT}".encode("ascii")
).hexdigest()
INERT_CONSUMER_IDENTITY_SHA256 = hashlib.sha256(
    b"stv3-blitzortung-inert-raw-completion-consumer-v1"
).hexdigest()

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DELIVERY_TOPIC_RE = re.compile(
    r"^blitzortung/1\.1/[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*$"
)
_SUBSCRIPTION_TOPIC_RE = re.compile(
    r"^blitzortung/1\.1/[0-9bcdefghjkmnpqrstuvwxyz]"
    r"/[0-9bcdefghjkmnpqrstuvwxyz]"
    r"/[0-9bcdefghjkmnpqrstuvwxyz]/#$"
)


class BlitzortungMqttRawSidecarError(RuntimeError):
    """Privacy-safe sidecar boundary failure."""


class BlitzortungMqttRawSidecarConfigError(BlitzortungMqttRawSidecarError):
    """A path, limit, region or explicit authority is invalid."""


@dataclass(frozen=True, slots=True)
class BlitzortungRegion:
    latitude: float
    longitude: float
    radius_km: float

    def __post_init__(self) -> None:
        latitude = _finite(self.latitude, "latitude")
        longitude = _finite(self.longitude, "longitude")
        radius = _finite(self.radius_km, "radius_km")
        if not -90.0 <= latitude <= 90.0:
            raise BlitzortungMqttRawSidecarConfigError("region_latitude_invalid")
        if not -180.0 <= longitude <= 180.0:
            raise BlitzortungMqttRawSidecarConfigError("region_longitude_invalid")
        if not 1.0 <= radius <= 500.0:
            raise BlitzortungMqttRawSidecarConfigError("region_radius_invalid")


def _finite(value: object, name: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise BlitzortungMqttRawSidecarConfigError(f"{name}_invalid")
    return float(value)


def _sha(value: object, name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise BlitzortungMqttRawSidecarConfigError(f"{name}_invalid")
    return value


def _canonical(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        dict(value),
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _encode_geohash(latitude: float, longitude: float) -> str:
    latitude_range = [-90.0, 90.0]
    longitude_range = [-180.0, 180.0]
    bits = (16, 8, 4, 2, 1)
    result: list[str] = []
    bit = 0
    value = 0
    even = True
    while len(result) < GEOHASH_PRECISION:
        bounds = longitude_range if even else latitude_range
        coordinate = longitude if even else latitude
        midpoint = (bounds[0] + bounds[1]) / 2.0
        if coordinate >= midpoint:
            value |= bits[bit]
            bounds[0] = midpoint
        else:
            bounds[1] = midpoint
        even = not even
        if bit < 4:
            bit += 1
        else:
            result.append(GEOHASH_BASE32[value])
            bit = 0
            value = 0
    return "".join(result)


def _geohashes_for_region(region: BlitzortungRegion) -> set[str]:
    latitude = float(region.latitude)
    longitude = float(region.longitude)
    radius = float(region.radius_km)
    latitude_margin = radius / 110.574
    cosine = max(0.05, abs(math.cos(math.radians(latitude))))
    longitude_margin = min(180.0, radius / (111.320 * cosine))
    # Half a precision-3 cell prevents gaps around cell boundaries.
    step = 1.40625 / 2.0
    hashes: set[str] = set()
    sample_latitude = max(-90.0, latitude - latitude_margin - step)
    maximum_latitude = min(90.0, latitude + latitude_margin + step)
    while sample_latitude <= maximum_latitude + 1e-9:
        sample_longitude = longitude - longitude_margin - step
        maximum_longitude = longitude + longitude_margin + step
        while sample_longitude <= maximum_longitude + 1e-9:
            wrapped = ((sample_longitude + 180.0) % 360.0) - 180.0
            hashes.add(_encode_geohash(sample_latitude, wrapped))
            if len(hashes) > MAX_SUBSCRIPTION_TOPICS:
                raise BlitzortungMqttRawSidecarConfigError(
                    "regional_subscription_capacity_exceeded"
                )
            sample_longitude += step
        sample_latitude += step
    return hashes


def regional_subscription_topics(
    regions: Iterable[BlitzortungRegion],
) -> tuple[str, ...]:
    """Return a bounded, deterministic union of regional geohash prefixes."""

    if isinstance(regions, (str, bytes, bytearray)):
        raise BlitzortungMqttRawSidecarConfigError("regions_invalid")
    try:
        exact = tuple(regions)
    except TypeError:
        raise BlitzortungMqttRawSidecarConfigError("regions_invalid") from None
    if len(exact) > MAX_REGIONS or any(
        not isinstance(region, BlitzortungRegion) for region in exact
    ):
        raise BlitzortungMqttRawSidecarConfigError("regions_invalid")
    geohashes: set[str] = set()
    for region in exact:
        geohashes.update(_geohashes_for_region(region))
        if len(geohashes) > MAX_SUBSCRIPTION_TOPICS:
            raise BlitzortungMqttRawSidecarConfigError(
                "regional_subscription_capacity_exceeded"
            )
    topics = tuple(sorted(
        f"blitzortung/1.1/{'/'.join(code)}/#" for code in geohashes
    ))
    if any(
        _SUBSCRIPTION_TOPIC_RE.fullmatch(topic) is None
        or topic == "blitzortung/1.1/#"
        for topic in topics
    ):
        raise BlitzortungMqttRawSidecarConfigError(
            "regional_subscription_invalid"
        )
    return topics


def _default_client_factory(client_id: str) -> Any:
    # Lazy import keeps module inspection and authority-blocked startup free of
    # network-client construction.
    import paho.mqtt.client as mqtt

    return mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        clean_session=True,
        protocol=mqtt.MQTTv311,
    )


def _mqtt_success(reason_code: object) -> bool:
    if bool(getattr(reason_code, "is_failure", False)):
        return False
    value = getattr(reason_code, "value", reason_code)
    try:
        return int(value) == 0
    except (TypeError, ValueError, OverflowError):
        return False


def _operation_succeeded(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, tuple):
        return bool(value) and _mqtt_success(value[0])
    return _mqtt_success(getattr(value, "rc", value))


class BlitzortungMqttRawSidecar:
    """One bounded public-broker client and one exact-raw authority."""

    def __init__(
        self,
        *,
        control_root: str | Path,
        raw_store_root: str | Path,
        raw_store_limits: RawArtifactLimits,
        ingress_limits: BlitzortungEventIngressLimits,
        access_retention_authority: str | None,
        keepalive_seconds: int = DEFAULT_KEEPALIVE_SECONDS,
        client_factory: Callable[[str], Any] = _default_client_factory,
        clock: Callable[[], float] = time.time,
        nonce_factory: Callable[[int], bytes] = os.urandom,
        directory_fsync: Callable[[Path], None] | None = None,
    ) -> None:
        control = Path(control_root)
        raw = Path(raw_store_root)
        if (
            not control.is_absolute()
            or control == Path(control.anchor)
            or not raw.is_absolute()
            or raw == Path(raw.anchor)
            or control.resolve(strict=False) == raw.resolve(strict=False)
        ):
            raise BlitzortungMqttRawSidecarConfigError(
                "event_ingress_paths_invalid"
            )
        if not isinstance(raw_store_limits, RawArtifactLimits):
            raise BlitzortungMqttRawSidecarConfigError(
                "event_raw_store_limits_invalid"
            )
        if not isinstance(ingress_limits, BlitzortungEventIngressLimits):
            raise BlitzortungMqttRawSidecarConfigError(
                "event_ingress_limits_invalid"
            )
        if (
            ingress_limits.max_retained_payload_bytes
            > raw_store_limits.max_total_bytes
            or ingress_limits.max_retained_payload_entries
            > raw_store_limits.max_object_entries
        ):
            raise BlitzortungMqttRawSidecarConfigError(
                "event_ingress_storage_envelope_invalid"
            )
        if (
            isinstance(keepalive_seconds, bool)
            or not isinstance(keepalive_seconds, int)
            or not 15 <= keepalive_seconds <= 300
        ):
            raise BlitzortungMqttRawSidecarConfigError(
                "event_ingress_keepalive_invalid"
            )
        if not callable(client_factory) or not callable(clock) or not callable(
            nonce_factory
        ):
            raise BlitzortungMqttRawSidecarConfigError(
                "event_ingress_factory_invalid"
            )
        if directory_fsync is not None and not callable(directory_fsync):
            raise BlitzortungMqttRawSidecarConfigError(
                "event_ingress_directory_fsync_invalid"
            )
        if access_retention_authority in {None, ""}:
            authority_value = None
        elif access_retention_authority == ACCESS_RETENTION_AUTHORITY_ID:
            authority_value = access_retention_authority
        else:
            raise BlitzortungMqttRawSidecarConfigError(
                "blitzortung_access_retention_authority_invalid"
            )

        self._control_root = control
        self._raw_store_root = raw
        self._raw_store_limits = raw_store_limits
        self._ingress_limits = ingress_limits
        self._access_authority = authority_value
        self._keepalive_seconds = keepalive_seconds
        self._client_factory = client_factory
        self._clock = clock
        self._nonce_factory = nonce_factory
        self._directory_fsync = directory_fsync
        self._lock = threading.RLock()
        self._authority: BlitzortungEventIngressAuthority | None = None
        self._client: Any | None = None
        self._session: BlitzortungSessionHandle | None = None
        self._topics: tuple[str, ...] = ()
        self._desired_state_sha256: str | None = None
        self._callback_sequence = 0
        self._session_sequence = 0
        self._started = False
        self._stopping = False
        self._connected = False
        self._network_started = False
        self._fault_latched = False
        self._state = (
            "authority_unavailable" if authority_value is None else "new"
        )
        self._last_error_code: str | None = None
        self._counts = {
            "callbacks": 0,
            "callback_failures": 0,
            "connects": 0,
            "disconnects": 0,
            "inert_completions": 0,
            "network_starts": 0,
            "starts": 0,
            "stops": 0,
        }

    def _increment(self, name: str) -> None:
        self._counts[name] = min(
            MAX_DIAGNOSTIC_COUNTER, self._counts[name] + 1
        )

    def _now(self) -> float:
        value = _finite(self._clock(), "clock")
        if value < 0:
            raise BlitzortungMqttRawSidecarError("sidecar_clock_invalid")
        return value

    def _new_session_identity(self) -> str:
        nonce = self._nonce_factory(32)
        if not isinstance(nonce, bytes) or len(nonce) != 32:
            raise BlitzortungMqttRawSidecarError("sidecar_nonce_invalid")
        self._session_sequence += 1
        return hashlib.sha256(
            _canonical({
                "broker_identity_sha256": BROKER_IDENTITY_SHA256,
                "nonce_sha256": hashlib.sha256(nonce).hexdigest(),
                "schema": "stv3-blitzortung-transport-session-identity-v1",
                "session_sequence": self._session_sequence,
            })
        ).hexdigest()

    @staticmethod
    def _subscription_sha256(topics: tuple[str, ...]) -> str:
        return hashlib.sha256(_canonical({
            "schema": "stv3-blitzortung-regional-subscription-set-v1",
            "topics": list(topics),
        })).hexdigest()

    def _connection_provenance(self, session_identity: str) -> str:
        return hashlib.sha256(_canonical({
            "authentication": "none",
            "broker_identity_sha256": BROKER_IDENTITY_SHA256,
            "protocol": MQTT_PROTOCOL,
            "qos": QOS,
            "schema": "stv3-blitzortung-public-connection-provenance-v1",
            "session_identity_sha256": session_identity,
        })).hexdigest()

    def _initialize_authority_locked(self) -> bool:
        if self._access_authority is None:
            self._state = "authority_unavailable"
            return False
        if self._authority is not None:
            return True
        try:
            raw_store = RawArtifactStore(
                self._raw_store_root,
                limits=self._raw_store_limits,
            )
            raw_store.initialize()
            decision = BlitzortungRetentionDecision.public_authorized(
                authority_id=self._access_authority,
                valid_from=0.0,
                valid_until=PRODUCTION_VALID_UNTIL,
            )
            self._authority = BlitzortungEventIngressAuthority(
                self._control_root,
                raw_store=raw_store,
                limits=self._ingress_limits,
                retention_decision=decision,
                synthetic_fixture_mode=False,
                clock=self._clock,
                directory_fsync=self._directory_fsync,
            )
            self._drain_inert_locked()
            return True
        except Exception:
            self._authority = None
            self._state = "storage_unavailable"
            self._last_error_code = "event_ingress_storage_unavailable"
            return False

    def _drain_inert_locked(self) -> None:
        authority = self._authority
        if authority is None:
            return
        for _unused in range(self._ingress_limits.max_queue_entries):
            claim = authority.claim_next(
                consumer_identity_sha256=INERT_CONSUMER_IDENTITY_SHA256,
                now=self._now(),
            )
            if claim is None:
                return
            authority.complete_claim(claim, completed_at=self._now())
            self._increment("inert_completions")
        if authority.diagnostics()["queue_depth"]:
            raise BlitzortungMqttRawSidecarError(
                "event_ingress_inert_drain_incomplete"
            )

    def _latch_fault_disconnect_locked(
        self,
        client: Any | None,
        *,
        state: str,
        error_code: str,
    ) -> None:
        """Latch one fail-closed transport stop without hiding its cause."""

        self._fault_latched = True
        self._connected = False
        self._state = state
        self._last_error_code = error_code
        if client is not None:
            try:
                client.disconnect()
            except Exception:
                pass

    def _start_network_locked(self) -> None:
        if (
            self._stopping
            or self._fault_latched
            or self._network_started
            or not self._topics
            or self._authority is None
        ):
            return
        client_id = "stv3-blitzortung-raw-" + self._new_session_identity()[:24]
        try:
            client = self._client_factory(client_id)
            client.on_connect = self._on_connect
            client.on_disconnect = self._on_disconnect
            client.on_message = self._on_message
            result = client.connect_async(
                BROKER_HOST,
                BROKER_PORT,
                self._keepalive_seconds,
            )
            if not _operation_succeeded(result):
                raise BlitzortungMqttRawSidecarError(
                    "event_ingress_connect_start_refused"
                )
            loop_result = client.loop_start()
            if not _operation_succeeded(loop_result):
                raise BlitzortungMqttRawSidecarError(
                    "event_ingress_loop_start_refused"
                )
        except Exception:
            self._client = None
            self._network_started = False
            self._state = "network_unavailable"
            self._last_error_code = "event_ingress_network_start_failed"
            return
        self._client = client
        self._network_started = True
        self._state = "connecting"
        self._last_error_code = None
        self._increment("network_starts")

    def start(
        self,
        regions: Iterable[BlitzortungRegion],
        *,
        desired_state_sha256: str,
    ) -> dict[str, object]:
        with self._lock:
            self._increment("starts")
            if self._stopping:
                raise BlitzortungMqttRawSidecarError(
                    "event_ingress_sidecar_stopping"
                )
            if self._access_authority is None:
                self._state = "authority_unavailable"
                return self.diagnostics()
            topics = regional_subscription_topics(regions)
            desired = _sha(desired_state_sha256, "desired_state_sha256")
            self._topics = topics
            self._desired_state_sha256 = desired
            self._started = True
            if not topics:
                self._state = "waiting_for_regions"
                return self.diagnostics()
            if self._initialize_authority_locked():
                self._start_network_locked()
            return self.diagnostics()

    def _end_session_locked(self, *, reason: str) -> None:
        handle = self._session
        self._session = None
        self._connected = False
        if handle is None or self._authority is None:
            return
        try:
            self._authority.end_session(
                handle,
                disconnected_at=self._now(),
                reason=reason,
            )
        except BlitzortungEventIngressConflict:
            pass
        except Exception:
            self._last_error_code = "event_ingress_session_end_failed"

    def _replace_logical_session_locked(self) -> None:
        authority = self._authority
        desired = self._desired_state_sha256
        if authority is None or desired is None or not self._topics:
            raise BlitzortungMqttRawSidecarError(
                "event_ingress_session_configuration_missing"
            )
        session_identity = self._new_session_identity()
        self._session = authority.begin_session(
            broker_identity_sha256=BROKER_IDENTITY_SHA256,
            session_identity_sha256=session_identity,
            desired_state_sha256=desired,
            subscription_set_sha256=self._subscription_sha256(self._topics),
            connection_provenance_sha256=self._connection_provenance(
                session_identity
            ),
            connected_at=self._now(),
        )
        self._callback_sequence = 0

    def _subscribe_locked(self, client: Any, topics: tuple[str, ...]) -> None:
        for topic in topics:
            if not _operation_succeeded(client.subscribe(topic, qos=QOS)):
                raise BlitzortungMqttRawSidecarError(
                    "event_ingress_subscribe_refused"
                )

    def _on_connect(
        self,
        client: Any,
        _userdata: object,
        _flags: object,
        reason_code: object,
        _properties: object = None,
    ) -> None:
        with self._lock:
            if self._stopping or client is not self._client:
                return
            if self._fault_latched:
                try:
                    client.disconnect()
                except Exception:
                    pass
                return
            if not _mqtt_success(reason_code):
                self._connected = False
                self._state = "connection_refused"
                self._last_error_code = "event_ingress_connection_refused"
                return
            try:
                self._subscribe_locked(client, self._topics)
                self._replace_logical_session_locked()
            except Exception:
                self._end_session_locked(reason="transport_error")
                self._latch_fault_disconnect_locked(
                    client,
                    state="subscription_unavailable",
                    error_code="event_ingress_subscription_failed",
                )
                return
            self._connected = True
            self._state = "running_exact_raw"
            self._last_error_code = None
            self._increment("connects")

    def _on_disconnect(
        self,
        client: Any,
        _userdata: object,
        _disconnect_flags: object = None,
        _reason_code: object = None,
        _properties: object = None,
    ) -> None:
        with self._lock:
            if client is not self._client:
                return
            self._end_session_locked(
                reason="operator_stop" if self._stopping else "broker_disconnect"
            )
            self._increment("disconnects")
            if self._fault_latched:
                self._network_started = False
            elif not self._stopping:
                self._state = "disconnected_reconnecting"

    def _topic_is_regional_delivery(self, topic: object) -> bool:
        if (
            not isinstance(topic, str)
            or _DELIVERY_TOPIC_RE.fullmatch(topic) is None
            or "+" in topic
            or "#" in topic
        ):
            return False
        return any(topic.startswith(subscription[:-1]) for subscription in self._topics)

    def _record_callback_refusal_locked(
        self,
        *,
        callback_sequence: int,
        refused_at: float,
        reason: str,
    ) -> bool:
        authority = self._authority
        session = self._session
        if authority is None or session is None:
            return False
        try:
            authority.record_callback_refusal(
                session,
                callback_sequence=callback_sequence,
                refused_at=refused_at,
                reason=reason,
            )
            return True
        except Exception:
            self._last_error_code = "event_ingress_refusal_receipt_failed"
            return False

    def _on_message(self, client: Any, _userdata: object, message: Any) -> None:
        with self._lock:
            if (
                self._stopping
                or not self._connected
                or client is not self._client
                or self._authority is None
                or self._session is None
            ):
                return
            callback = self._callback_sequence
            self._callback_sequence += 1
            self._increment("callbacks")
            received_at = self._now()
            payload = getattr(message, "payload", None)
            topic = getattr(message, "topic", None)
            qos = getattr(message, "qos", None)
            retain = getattr(message, "retain", None)
            if (
                not isinstance(payload, bytes)
                or len(payload) > MAX_PAYLOAD_BYTES
                or not self._topic_is_regional_delivery(topic)
                or isinstance(qos, bool)
                or qos != QOS
                or type(retain) is not bool
            ):
                self._increment("callback_failures")
                refusal_recorded = self._record_callback_refusal_locked(
                    callback_sequence=callback,
                    refused_at=received_at,
                    reason="invalid_delivery_envelope",
                )
                if not refusal_recorded:
                    self._latch_fault_disconnect_locked(
                        client,
                        state="control_receipt_degraded",
                        error_code="event_ingress_refusal_receipt_failed",
                    )
                return
            session = self._session
            assert session is not None
            delivery_identity = hashlib.sha256(_canonical({
                "callback_sequence": callback,
                "content_sha256": hashlib.sha256(payload).hexdigest(),
                "qos": qos,
                "received_at": received_at,
                "retain": retain,
                "schema": "stv3-blitzortung-qos0-callback-delivery-v1",
                "session_identity_sha256": session.session_identity_sha256,
                "size_bytes": len(payload),
                "topic": topic,
            })).hexdigest()
            envelope = BlitzortungMqttEnvelope(
                payload=payload,
                broker_identity_sha256=BROKER_IDENTITY_SHA256,
                session_identity_sha256=session.session_identity_sha256,
                delivery_identity_sha256=delivery_identity,
                topic=topic,
                qos=qos,
                retain=retain,
                received_at=received_at,
                desired_state_sha256=session.desired_state_sha256,
                subscription_set_sha256=session.subscription_set_sha256,
            )
            try:
                self._drain_inert_locked()
                self._authority.offer_delivery(
                    session,
                    envelope,
                    callback_sequence=callback,
                )
                self._drain_inert_locked()
            except (
                BlitzortungEventIngressValidationError,
                BlitzortungRawIngressValidationError,
            ):
                self._increment("callback_failures")
                refusal_recorded = self._record_callback_refusal_locked(
                    callback_sequence=callback,
                    refused_at=received_at,
                    reason="invalid_delivery_envelope",
                )
                if not refusal_recorded:
                    self._latch_fault_disconnect_locked(
                        client,
                        state="control_receipt_degraded",
                        error_code="event_ingress_refusal_receipt_failed",
                    )
            except (
                BlitzortungEventIngressBackpressure,
                BlitzortungEventIngressCapacityError,
            ):
                self._increment("callback_failures")
                self._latch_fault_disconnect_locked(
                    client,
                    state="capacity_blocked",
                    error_code="event_ingress_capacity_blocked",
                )
            except (BlitzortungEventIngressError, Exception):
                self._increment("callback_failures")
                self._latch_fault_disconnect_locked(
                    client,
                    state="storage_degraded",
                    error_code="event_ingress_callback_failed",
                )

    def update_regions(
        self,
        regions: Iterable[BlitzortungRegion],
        *,
        desired_state_sha256: str,
    ) -> dict[str, object]:
        with self._lock:
            if self._access_authority is None:
                self._state = "authority_unavailable"
                return self.diagnostics()
            desired = _sha(desired_state_sha256, "desired_state_sha256")
            topics = regional_subscription_topics(regions)
            if not self._started:
                return self.start(regions, desired_state_sha256=desired)
            if self._fault_latched:
                return self.diagnostics()
            unchanged = (
                topics == self._topics
                and desired == self._desired_state_sha256
            )
            if unchanged and self._client is not None:
                return self.diagnostics()
            client = self._client
            old_topics = self._topics
            self._topics = topics
            self._desired_state_sha256 = desired
            if not topics:
                self._end_session_locked(reason="desired_state_transition")
                self._client = None
                self._network_started = False
                if client is not None:
                    try:
                        client.disconnect()
                    except Exception:
                        pass
                self._connected = False
                self._state = "waiting_for_regions"
                return self.diagnostics()
            if client is None:
                if self._initialize_authority_locked():
                    self._start_network_locked()
                return self.diagnostics()
            if self._connected:
                try:
                    for topic in sorted(set(old_topics) - set(topics)):
                        if not _operation_succeeded(client.unsubscribe(topic)):
                            raise BlitzortungMqttRawSidecarError(
                                "event_ingress_unsubscribe_refused"
                            )
                    self._subscribe_locked(
                        client, tuple(sorted(set(topics) - set(old_topics)))
                    )
                    self._replace_logical_session_locked()
                except Exception:
                    self._end_session_locked(reason="transport_error")
                    self._latch_fault_disconnect_locked(
                        client,
                        state="subscription_unavailable",
                        error_code="event_ingress_subscription_failed",
                    )
            return self.diagnostics()

    def stop(self) -> dict[str, object]:
        with self._lock:
            self._increment("stops")
            if self._stopping:
                return self.diagnostics()
            self._stopping = True
            client = self._client
            authority = self._authority
            self._end_session_locked(reason="operator_stop")
            self._client = None
            self._network_started = False
            self._connected = False
        if client is not None:
            try:
                client.disconnect()
            except Exception:
                pass
            try:
                client.loop_stop()
            except Exception:
                pass
        if authority is not None:
            try:
                authority.close()
            except Exception:
                with self._lock:
                    self._last_error_code = "event_ingress_authority_close_failed"
        with self._lock:
            self._authority = None
            self._state = "stopped"
            return self.diagnostics()

    def diagnostics(self) -> dict[str, object]:
        with self._lock:
            authority_diagnostics: dict[str, object] | None = None
            if self._authority is not None:
                try:
                    authority_diagnostics = self._authority.diagnostics()
                except Exception:
                    self._latch_fault_disconnect_locked(
                        self._client,
                        state="diagnostics_degraded",
                        error_code="event_ingress_diagnostics_failed",
                    )
            queue_depth = (
                authority_diagnostics.get("queue_depth")
                if authority_diagnostics is not None
                else 0
            )
            retained_entries = (
                authority_diagnostics.get("retained_payload_entries")
                if authority_diagnostics is not None
                else 0
            )
            retained_bytes = (
                authority_diagnostics.get("retained_payload_bytes")
                if authority_diagnostics is not None
                else 0
            )
            control_receipts = (
                int(authority_diagnostics.get("control_receipts", 0))
                if authority_diagnostics is not None
                else 0
            )
            queue_capacity = self._ingress_limits.max_queue_entries
            control_capacity = self._ingress_limits.max_control_receipts
            retained_entry_capacity = (
                self._ingress_limits.max_retained_payload_entries
            )
            retained_byte_capacity = (
                self._ingress_limits.max_retained_payload_bytes
            )
            counters = (
                dict(authority_diagnostics.get("counters", {}))
                if authority_diagnostics is not None
                else {}
            )
            alarm_codes: list[str] = []
            if self._fault_latched:
                alarm_codes.append("event_ingress_fault_latched")
            if authority_diagnostics is not None and authority_diagnostics.get(
                "control_capacity_latched"
            ) is True:
                alarm_codes.append("event_ingress_control_capacity_latched")
            if int(counters.get("transport_gaps", 0)) > 0:
                alarm_codes.append("event_ingress_transport_gap_observed")
            if self._counts["callback_failures"] > 0:
                alarm_codes.append("event_ingress_callback_failure_observed")
            return {
                "absence_semantics": ABSENCE_SEMANTICS,
                "access_retention_authority": (
                    "available"
                    if self._access_authority is not None
                    else "unavailable"
                ),
                "broker_profile": BROKER_PROFILE,
                "callback_counts": dict(self._counts),
                "connected": self._connected,
                "contract": CONTRACT,
                "diagnostics_schema": DIAGNOSTICS_SCHEMA,
                "fault_latched": self._fault_latched,
                "alarm_codes": alarm_codes,
                "capacity": {
                    "control_intake_headroom": max(
                        0,
                        control_capacity
                        - self._ingress_limits.control_receipt_reserve
                        - control_receipts,
                    ),
                    "control_receipt_capacity": control_capacity,
                    "control_receipts": control_receipts,
                    "queue_capacity": queue_capacity,
                    "queue_headroom": max(0, queue_capacity - int(queue_depth)),
                    "retained_payload_byte_capacity": retained_byte_capacity,
                    "retained_payload_byte_headroom": max(
                        0, retained_byte_capacity - int(retained_bytes)
                    ),
                    "retained_payload_entry_capacity": retained_entry_capacity,
                    "retained_payload_entry_headroom": max(
                        0, retained_entry_capacity - int(retained_entries)
                    ),
                },
                "ingress_counters": counters,
                "last_error_code": self._last_error_code,
                "meteorological_admission": False,
                "network_started": self._network_started,
                "operational_admission": False,
                "participant_or_explicit_permission_required": True,
                "payload_parsed": False,
                "queue_depth": queue_depth,
                "raw_payload_transport": RAW_PAYLOAD_TRANSPORT,
                "raw_payloads_over_ha_mqtt": False,
                "raw_shadow_limitations": list(RAW_SHADOW_LIMITATIONS),
                "regional_subscription_count": len(self._topics),
                "retained_payload_bytes": retained_bytes,
                "retained_payload_entries": retained_entries,
                "silence_interpretation": SILENCE_INTERPRETATION,
                "state": self._state,
                "transport_completeness": TRANSPORT_COMPLETENESS,
            }


__all__ = [
    "BROKER_HOST",
    "BROKER_PORT",
    "BROKER_PROFILE",
    "CONTRACT",
    "DIAGNOSTICS_SCHEMA",
    "DEFAULT_KEEPALIVE_SECONDS",
    "GEOHASH_PRECISION",
    "MAX_REGIONS",
    "MAX_SUBSCRIPTION_TOPICS",
    "RAW_SHADOW_LIMITATIONS",
    "BlitzortungMqttRawSidecar",
    "BlitzortungMqttRawSidecarConfigError",
    "BlitzortungMqttRawSidecarError",
    "BlitzortungRegion",
    "regional_subscription_topics",
]
