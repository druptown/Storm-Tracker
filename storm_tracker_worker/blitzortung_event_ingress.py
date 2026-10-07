"""Crash-durable exact-raw authority for Blitzortung QoS 0 event ingress.

The module owns no socket.  It is a bounded control-plane authority around
:mod:`blitzortung_raw_ingress`: exact
bytes are handed to the schema-3 raw store before a queue reference can exist,
while every session, refusal, transport gap and queue transition is an
append-only content-addressed receipt.

Public payload retention is possible only when the worker supplies the one
exact participant/permission authority identifier declared below.  Missing or
different authority remains fail-closed.  Nothing in this module parses a
payload or emits a lightning/no-lightning fact.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import re
import threading
import time
from typing import Callable, Iterator, Mapping

from .blitzortung_raw_ingress import (
    ABSENCE_SEMANTICS,
    TRANSPORT_COMPLETENESS,
    BlitzortungMqttEnvelope,
    BlitzortungRawIngressReference,
    persist_blitzortung_mqtt_envelope,
)
from .raw_artifacts import (
    RawArtifactCapacityError,
    RawArtifactConflictError,
    RawArtifactStore,
)


INGRESS_ID = "lightning.blitzortung.event_ingress"
CONTRACT = "stv3-worker-blitzortung-event-ingress-authority-v1"
RECEIPT_SCHEMA = "stv3-worker-blitzortung-event-ingress-receipt-v1"
SESSION_SCHEMA = "stv3-worker-blitzortung-session-handle-v1"
CLAIM_SCHEMA = "stv3-worker-blitzortung-queue-claim-v1"
DIAGNOSTICS_SCHEMA = "stv3-worker-blitzortung-ingress-diagnostics-v1"
LIMITS_SCHEMA = "stv3-worker-blitzortung-ingress-limits-v1"
RETENTION_SCHEMA = "stv3-worker-blitzortung-retention-decision-v1"

ACCESS_RETENTION_AUTHORITY_ID = (
    "worker_authority.blitzortung_participant_raw_retention_v1"
)
SOURCE_TERMS_PROFILE = (
    "blitzortung_raw_access_requires_participant_or_explicit_permission_v1"
)
SOURCE_TERMS_SHA256 = hashlib.sha256(
    SOURCE_TERMS_PROFILE.encode("ascii")
).hexdigest()
PRODUCTION_AUTHORITY_SHA256 = hashlib.sha256(
    ACCESS_RETENTION_AUTHORITY_ID.encode("ascii")
).hexdigest()

ZERO_SHA256 = "0" * 64
LOCK_BYTES = b"stv3-blitzortung-event-ingress-lock-v1\n"
MAX_CONTROL_RECEIPT_BYTES = 128 * 1024
MAX_CALLBACK_SEQUENCE = (1 << 63) - 1
MAX_REASON_CODE_BYTES = 64

# These are exact release gates, not hints.  The module reports every one and
# never mutates them based on synthetic test execution.
PROMOTION_GATES = (
    "durable_decoder_and_event_time_validation_missing",
    "raw_and_control_receipt_reclamation_authority_missing",
    "capacity_alarm_and_operator_runbook_missing",
    "disconnect_loss_and_thermal_ha_z2m_soak_missing",
    "exclusive_legacy_ha_to_worker_cutover_not_proven",
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RECEIPT_NAME_RE = re.compile(r"^([0-9]{20})\.([0-9a-f]{64})\.json$")
_STAGE_NAME_RE = re.compile(
    r"^([0-9]{20})\.([0-9a-f]{64})\.pending$"
)
_SAFE_REASON_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_PUBLIC_TOPIC_RE = re.compile(
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

_EVENT_KINDS = frozenset({
    "store_initialized",
    "session_started",
    "session_ended",
    "sequence_gap",
    "delivery_intent",
    "delivery_accepted",
    "delivery_duplicate",
    "delivery_conflict",
    "delivery_refused",
    "callback_refused",
    "crash_gap",
    "delivery_claimed",
    "claim_released",
    "delivery_completed",
    "control_capacity_latched",
})
_SESSION_END_REASONS = frozenset({
    "broker_disconnect",
    "transport_error",
    "desired_state_transition",
    "operator_stop",
    "session_replaced",
    "process_restart",
    "cancellation",
})
_REFUSAL_REASONS = frozenset({
    "production_retention_rights_unproven",
    "connection_provenance_unproven",
    "queue_capacity",
    "retained_payload_entry_capacity",
    "retained_payload_byte_capacity",
    "raw_store_capacity",
    "raw_store_conflict",
    "raw_store_failure",
})
_CALLBACK_REFUSAL_REASONS = frozenset({
    "invalid_delivery_envelope",
    "sidecar_callback_busy",
    "sidecar_callback_overflow",
})
_CLAIM_RELEASE_REASONS = frozenset({
    "lease_expired",
    "process_restart",
    "consumer_cancellation",
    "consumer_failure",
})


class BlitzortungEventIngressError(RuntimeError):
    """Base failure for the inert event-ingress authority."""


class BlitzortungEventIngressValidationError(BlitzortungEventIngressError):
    """Input, receipt or state did not satisfy the closed contract."""


class BlitzortungEventIngressIntegrityError(BlitzortungEventIngressError):
    """Durable state, a hash chain or filesystem layout was unsafe."""


class BlitzortungEventIngressBusy(BlitzortungEventIngressError):
    """Another process or authority instance owns this ingress root."""


class BlitzortungEventIngressCapacityError(BlitzortungEventIngressError):
    """A hard queue, payload or control-receipt cap refused work."""


class BlitzortungEventIngressBlocked(BlitzortungEventIngressError):
    """Production retention/provenance authority is intentionally absent."""


class BlitzortungEventIngressBackpressure(BlitzortungEventIngressError):
    """The bounded durable queue refused a newly received QoS 0 delivery."""


class BlitzortungEventIngressConflict(BlitzortungEventIngressError):
    """A delivery identity was reused for different immutable facts."""


@dataclass(frozen=True, slots=True)
class BlitzortungEventIngressLimits:
    """Hard append-only and queue limits; there is no eviction fallback."""

    max_queue_entries: int = 128
    max_control_receipts: int = 100_000
    max_retained_payload_entries: int = 50_000
    max_retained_payload_bytes: int = 4 * 1024 * 1024 * 1024
    max_sessions: int = 10_000
    control_receipt_reserve: int = 8
    claim_lease_seconds: float = 30.0
    lock_timeout_seconds: float = 0.25

    def __post_init__(self) -> None:
        integer_fields = (
            "max_queue_entries",
            "max_control_receipts",
            "max_retained_payload_entries",
            "max_retained_payload_bytes",
            "max_sessions",
            "control_receipt_reserve",
        )
        for name in integer_fields:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.max_queue_entries > self.max_retained_payload_entries:
            raise ValueError("queue cap cannot exceed retained-entry cap")
        if self.max_control_receipts < self.control_receipt_reserve + 8:
            raise ValueError("control receipt cap leaves no recovery reserve")
        for name in ("claim_lease_seconds", "lock_timeout_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be finite and positive")
            if not math.isfinite(float(value)) or float(value) <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if float(self.claim_lease_seconds) > 300.0:
            raise ValueError("claim lease exceeds the hard five-minute bound")
        if float(self.lock_timeout_seconds) > 10.0:
            raise ValueError("lock timeout exceeds the hard ten-second bound")

    def to_metadata(self) -> dict[str, object]:
        return {
            "schema": LIMITS_SCHEMA,
            "max_queue_entries": self.max_queue_entries,
            "max_control_receipts": self.max_control_receipts,
            "max_retained_payload_entries": self.max_retained_payload_entries,
            "max_retained_payload_bytes": self.max_retained_payload_bytes,
            "max_sessions": self.max_sessions,
            "control_receipt_reserve": self.control_receipt_reserve,
            "claim_lease_seconds": float(self.claim_lease_seconds),
            "lock_timeout_seconds": float(self.lock_timeout_seconds),
            "retention_mode": "append_only_no_reclamation",
        }


@dataclass(frozen=True, slots=True)
class BlitzortungRetentionDecision:
    """Content-bound synthetic or explicitly-authorized retention decision.

    The public factory accepts one exact worker authority identifier.  The
    decision never claims that Blitzortung granted rights automatically: the
    operator remains responsible for participant status or explicit source
    permission before enabling that authority.
    """

    source_scope: str
    durable_payload_retention_allowed: bool
    source_terms_sha256: str
    provenance_authority_sha256: str
    valid_from: float
    valid_until: float
    decision_sha256: str

    def __post_init__(self) -> None:
        if self.source_scope not in {"synthetic_fixture_only", "public_blitzortung"}:
            raise BlitzortungEventIngressValidationError(
                "retention source scope is unknown"
            )
        if type(self.durable_payload_retention_allowed) is not bool:
            raise BlitzortungEventIngressValidationError(
                "retention allow flag must be boolean"
            )
        _sha(self.source_terms_sha256, "source_terms_sha256")
        _sha(self.provenance_authority_sha256, "provenance_authority_sha256")
        start = _time_value(self.valid_from, "valid_from")
        end = _time_value(self.valid_until, "valid_until")
        if end <= start:
            raise BlitzortungEventIngressValidationError(
                "retention validity interval is empty"
            )
        body = self._body()
        if _digest(body) != _sha(self.decision_sha256, "decision_sha256"):
            raise BlitzortungEventIngressValidationError(
                "retention decision checksum does not bind its fields"
            )

    def _body(self) -> dict[str, object]:
        return {
            "schema": RETENTION_SCHEMA,
            "source_scope": self.source_scope,
            "durable_payload_retention_allowed": (
                self.durable_payload_retention_allowed
            ),
            "source_terms_sha256": self.source_terms_sha256,
            "provenance_authority_sha256": self.provenance_authority_sha256,
            "valid_from": float(self.valid_from),
            "valid_until": float(self.valid_until),
            "external_signature_verified": False,
            "operational_admission": False,
        }

    @classmethod
    def synthetic_fixture(
        cls,
        *,
        source_terms_sha256: str,
        provenance_authority_sha256: str,
        valid_from: float,
        valid_until: float,
    ) -> "BlitzortungRetentionDecision":
        body = {
            "schema": RETENTION_SCHEMA,
            "source_scope": "synthetic_fixture_only",
            "durable_payload_retention_allowed": True,
            "source_terms_sha256": _sha(
                source_terms_sha256, "source_terms_sha256"
            ),
            "provenance_authority_sha256": _sha(
                provenance_authority_sha256,
                "provenance_authority_sha256",
            ),
            "valid_from": _time_value(valid_from, "valid_from"),
            "valid_until": _time_value(valid_until, "valid_until"),
            "external_signature_verified": False,
            "operational_admission": False,
        }
        return cls(
            source_scope="synthetic_fixture_only",
            durable_payload_retention_allowed=True,
            source_terms_sha256=source_terms_sha256,
            provenance_authority_sha256=provenance_authority_sha256,
            valid_from=float(valid_from),
            valid_until=float(valid_until),
            decision_sha256=_digest(body),
        )

    @classmethod
    def public_authorized(
        cls,
        *,
        authority_id: str,
        valid_from: float,
        valid_until: float,
    ) -> "BlitzortungRetentionDecision":
        """Bind the sole accepted production authority without broadening it."""

        if authority_id != ACCESS_RETENTION_AUTHORITY_ID:
            raise BlitzortungEventIngressValidationError(
                "public retention authority is unavailable"
            )
        start = _time_value(valid_from, "valid_from")
        end = _time_value(valid_until, "valid_until")
        body = {
            "schema": RETENTION_SCHEMA,
            "source_scope": "public_blitzortung",
            "durable_payload_retention_allowed": True,
            "source_terms_sha256": SOURCE_TERMS_SHA256,
            "provenance_authority_sha256": PRODUCTION_AUTHORITY_SHA256,
            "valid_from": start,
            "valid_until": end,
            # This flag deliberately remains false: an explicit local
            # authority is not mislabelled as a source-signed permission.
            "external_signature_verified": False,
            "operational_admission": False,
        }
        return cls(
            source_scope="public_blitzortung",
            durable_payload_retention_allowed=True,
            source_terms_sha256=SOURCE_TERMS_SHA256,
            provenance_authority_sha256=PRODUCTION_AUTHORITY_SHA256,
            valid_from=start,
            valid_until=end,
            decision_sha256=_digest(body),
        )


@dataclass(frozen=True, slots=True)
class BlitzortungSessionHandle:
    epoch: int
    broker_identity_sha256: str
    session_identity_sha256: str
    desired_state_sha256: str
    subscription_set_sha256: str
    connection_provenance_sha256: str
    binding_sha256: str

    def __post_init__(self) -> None:
        if isinstance(self.epoch, bool) or not isinstance(self.epoch, int) or self.epoch <= 0:
            raise BlitzortungEventIngressValidationError("session epoch is invalid")
        body = self._body()
        if _digest(body) != _sha(self.binding_sha256, "binding_sha256"):
            raise BlitzortungEventIngressValidationError(
                "session handle checksum does not bind its fields"
            )

    def _body(self) -> dict[str, object]:
        return {
            "schema": SESSION_SCHEMA,
            "epoch": self.epoch,
            "broker_identity_sha256": _sha(
                self.broker_identity_sha256, "broker_identity_sha256"
            ),
            "session_identity_sha256": _sha(
                self.session_identity_sha256, "session_identity_sha256"
            ),
            "desired_state_sha256": _sha(
                self.desired_state_sha256, "desired_state_sha256"
            ),
            "subscription_set_sha256": _sha(
                self.subscription_set_sha256, "subscription_set_sha256"
            ),
            "connection_provenance_sha256": _sha(
                self.connection_provenance_sha256,
                "connection_provenance_sha256",
            ),
        }

    def to_metadata(self) -> dict[str, object]:
        return {**self._body(), "binding_sha256": self.binding_sha256}


@dataclass(frozen=True, slots=True)
class BlitzortungOfferResult:
    status: str
    event_receipt_sha256: str
    raw_reference: BlitzortungRawIngressReference | None
    duplicate: bool
    out_of_order: bool
    operational_admission: bool = False
    absence_semantics: str = ABSENCE_SEMANTICS

    def __post_init__(self) -> None:
        if self.status not in {"accepted_shadow_raw", "exact_duplicate"}:
            raise BlitzortungEventIngressValidationError("offer result status is invalid")
        _sha(self.event_receipt_sha256, "event_receipt_sha256")
        if type(self.duplicate) is not bool or type(self.out_of_order) is not bool:
            raise BlitzortungEventIngressValidationError("offer flags must be boolean")
        if self.status == "accepted_shadow_raw" and self.raw_reference is None:
            raise BlitzortungEventIngressValidationError("accepted offer lacks raw reference")
        if self.status == "exact_duplicate" and (
            self.raw_reference is not None or not self.duplicate
        ):
            raise BlitzortungEventIngressValidationError("duplicate result is inconsistent")
        if self.operational_admission is not False or self.absence_semantics != ABSENCE_SEMANTICS:
            raise BlitzortungEventIngressValidationError("offer contains a forbidden weather claim")


@dataclass(frozen=True, slots=True)
class BlitzortungQueueClaim:
    claim_token_sha256: str
    queue_receipt_sha256: str
    consumer_identity_sha256: str
    raw_reference: BlitzortungRawIngressReference
    session_epoch: int
    callback_sequence: int
    claimed_at: float
    expires_at: float
    operational_admission: bool = False
    absence_semantics: str = ABSENCE_SEMANTICS

    def __post_init__(self) -> None:
        _sha(self.claim_token_sha256, "claim_token_sha256")
        _sha(self.queue_receipt_sha256, "queue_receipt_sha256")
        _sha(self.consumer_identity_sha256, "consumer_identity_sha256")
        if isinstance(self.session_epoch, bool) or not isinstance(self.session_epoch, int) or self.session_epoch <= 0:
            raise BlitzortungEventIngressValidationError("claim session epoch is invalid")
        _callback_sequence(self.callback_sequence)
        claimed = _time_value(self.claimed_at, "claimed_at")
        expires = _time_value(self.expires_at, "expires_at")
        if expires <= claimed:
            raise BlitzortungEventIngressValidationError("claim interval is empty")
        if self.operational_admission is not False or self.absence_semantics != ABSENCE_SEMANTICS:
            raise BlitzortungEventIngressValidationError("claim contains a forbidden weather claim")


@dataclass(slots=True)
class _IngressState:
    sequence: int = 0
    last_receipt_sha256: str = ZERO_SHA256
    initialized: bool = False
    profile_sha256: str | None = None
    session_epoch: int = 0
    current_session: dict[str, object] | None = None
    last_callback_sequence: int | None = None
    pending: dict[str, dict[str, object]] | None = None
    queue: list[dict[str, object]] | None = None
    known_deliveries: dict[str, dict[str, object]] | None = None
    active_claim: dict[str, object] | None = None
    counters: dict[str, int] | None = None
    retained_payload_entries: int = 0
    retained_payload_bytes: int = 0
    control_capacity_latched: bool = False

    def __post_init__(self) -> None:
        if self.pending is None:
            self.pending = {}
        if self.queue is None:
            self.queue = []
        if self.known_deliveries is None:
            self.known_deliveries = {}
        if self.counters is None:
            self.counters = {
                "sessions_started": 0,
                "sessions_ended": 0,
                "deliveries_received": 0,
                "deliveries_accepted": 0,
                "deliveries_completed": 0,
                "duplicates": 0,
                "out_of_order": 0,
                "sequence_gaps": 0,
                "callback_positions_missing": 0,
                "transport_gaps": 0,
                "drops": 0,
                "conflicts": 0,
                "raw_failures": 0,
                "crash_recoveries": 0,
                "claims_recovered": 0,
            }


def _canonical(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _digest(value: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: object, name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise BlitzortungEventIngressValidationError(
            f"{name} must be one lower-case SHA-256"
        )
    return value


def _time_value(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BlitzortungEventIngressValidationError(f"{name} must be finite")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise BlitzortungEventIngressValidationError(f"{name} must be finite")
    return result


def _callback_sequence(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= MAX_CALLBACK_SEQUENCE
    ):
        raise BlitzortungEventIngressValidationError(
            "callback sequence is outside the hard bound"
        )
    return value


def _safe_reason(value: object, allowed: frozenset[str]) -> str:
    if (
        not isinstance(value, str)
        or len(value.encode("ascii", errors="ignore")) > MAX_REASON_CODE_BYTES
        or _SAFE_REASON_RE.fullmatch(value) is None
        or value not in allowed
    ):
        raise BlitzortungEventIngressValidationError("reason code is not allow-listed")
    return value


def _json_no_duplicates(payload: bytes) -> dict[str, object]:
    def hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise BlitzortungEventIngressIntegrityError(
                    "control receipt contains duplicate JSON keys"
                )
            result[key] = value
        return result

    try:
        value = json.loads(payload.decode("ascii"), object_pairs_hook=hook)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise BlitzortungEventIngressIntegrityError(
            "control receipt is not canonical ASCII JSON"
        ) from exc
    if not isinstance(value, dict):
        raise BlitzortungEventIngressIntegrityError("control receipt is not an object")
    if _canonical(value) != payload:
        raise BlitzortungEventIngressIntegrityError("control receipt is not canonical")
    return value


def _delivery_key(envelope: BlitzortungMqttEnvelope) -> str:
    return _digest({
        "schema": "stv3-blitzortung-event-delivery-key-v1",
        "broker_identity_sha256": envelope.broker_identity_sha256,
        "session_identity_sha256": envelope.session_identity_sha256,
        "delivery_identity_sha256": envelope.delivery_identity_sha256,
    })


def _delivery_fingerprint(
    envelope: BlitzortungMqttEnvelope,
    callback_sequence: int,
) -> str:
    return _digest({
        "schema": "stv3-blitzortung-event-delivery-fingerprint-v1",
        "content_sha256": hashlib.sha256(envelope.payload).hexdigest(),
        "size_bytes": len(envelope.payload),
        "topic": envelope.topic,
        "qos": envelope.qos,
        "retain": envelope.retain,
        "received_at": float(envelope.received_at),
        "desired_state_sha256": envelope.desired_state_sha256,
        "subscription_set_sha256": envelope.subscription_set_sha256,
        "callback_sequence": callback_sequence,
    })


_PROCESS_GUARD = threading.Lock()
_PROCESS_ROOT_OWNERS: set[str] = set()


def _lock_descriptor(descriptor: int) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_descriptor(descriptor: int) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(descriptor, fcntl.LOCK_UN)


class BlitzortungEventIngressAuthority:
    """Exclusive append-only owner of one Blitzortung exact-raw ingress root."""

    def __init__(
        self,
        root: str | Path,
        *,
        raw_store: RawArtifactStore,
        limits: BlitzortungEventIngressLimits | None = None,
        retention_decision: BlitzortungRetentionDecision | None = None,
        synthetic_fixture_mode: bool = False,
        clock: Callable[[], float] = time.time,
        directory_fsync: Callable[[Path], None] | None = None,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        candidate_root = Path(root)
        if not candidate_root.is_absolute():
            raise ValueError("event ingress root must be absolute")
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store must be RawArtifactStore")
        if type(synthetic_fixture_mode) is not bool:
            raise TypeError("synthetic_fixture_mode must be boolean")
        if retention_decision is not None and not isinstance(
            retention_decision, BlitzortungRetentionDecision
        ):
            raise TypeError("retention_decision has an invalid type")
        if not callable(clock):
            raise TypeError("clock must be callable")
        if directory_fsync is not None and not callable(directory_fsync):
            raise TypeError("directory_fsync must be callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("fault_injector must be callable")

        self.root = candidate_root
        self.receipts_directory = self.root / "receipts"
        self.staging_directory = self.root / "staging"
        self.lock_path = self.root / "owner.lock"
        self._raw_store = raw_store
        self._limits = limits or BlitzortungEventIngressLimits()
        self._retention_decision = retention_decision
        self._synthetic_fixture_mode = synthetic_fixture_mode
        self._clock = clock
        self._directory_fsync = directory_fsync or self._default_directory_fsync
        self._fault_injector = fault_injector
        self._operation_lock = threading.RLock()
        self._lock_descriptor: int | None = None
        self._root_owner_key: str | None = None
        self._owner_pid = os.getpid()
        self._closed = False
        self._poisoned = False
        self._directory_fsync_supported = os.name != "nt" or directory_fsync is not None
        self._state = _IngressState()
        raw_stats = raw_store.stats()
        production_authorized = self._production_retention_allowed(self._now())
        self._profile = {
            "contract": CONTRACT,
            "ingress_id": INGRESS_ID,
            "limits": self._limits.to_metadata(),
            "raw_store_profile_sha256": _sha(
                raw_stats.get("store_profile_sha256"),
                "raw_store_profile_sha256",
            ),
            "raw_store_root_sha256": hashlib.sha256(
                str(raw_store.root.resolve()).encode("utf-8")
            ).hexdigest(),
            "synthetic_fixture_mode": self._synthetic_fixture_mode,
            "retention_scope": (
                self._retention_decision.source_scope
                if self._retention_decision is not None
                else "none"
            ),
            "retention_decision_sha256": (
                self._retention_decision.decision_sha256
                if self._retention_decision is not None
                else None
            ),
            "production_retention_authorized": production_authorized,
            "operational_admission": False,
        }
        self._profile_sha256 = _digest(self._profile)

        try:
            self._prepare_layout()
            self._acquire_owner()
            self._cleanup_staging()
            self._load_receipts()
            if not self._state.initialized:
                self._append_event(
                    "store_initialized",
                    self._now(),
                    {
                        "profile_sha256": self._profile_sha256,
                        "profile": self._profile,
                    },
                    use_reserve=True,
                )
            elif self._state.profile_sha256 != self._profile_sha256:
                raise BlitzortungEventIngressIntegrityError(
                    "ingress profile differs from durable initialization"
                )
            self._recover_interrupted_owner()
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _default_directory_fsync(path: Path) -> None:
        # The release target is Linux.  Windows is a development host where
        # directory fsync is unavailable; diagnostics keeps that promotion
        # blocker visible.  File fsync and atomic replace are still exercised.
        if os.name == "nt":
            return
        descriptor: int | None = None
        try:
            descriptor = os.open(
                str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            )
            os.fsync(descriptor)
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def _now(self) -> float:
        return _time_value(self._clock(), "clock")

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _offer_fault(self, point: str) -> None:
        """Poison the in-memory owner if cancellation crosses an offer edge."""

        try:
            self._fault(point)
        except BaseException:
            self._poisoned = True
            raise

    def _prepare_layout(self) -> None:
        if self.root.exists() and self.root.is_symlink():
            raise BlitzortungEventIngressIntegrityError("ingress root is a symlink")
        root_created = not self.root.exists()
        self.root.mkdir(parents=True, exist_ok=True)
        for directory in (self.receipts_directory, self.staging_directory):
            if directory.exists() and directory.is_symlink():
                raise BlitzortungEventIngressIntegrityError(
                    "ingress directory is a symlink"
                )
            directory.mkdir(exist_ok=True)
        if root_created:
            self._directory_fsync(self.root.parent)
        if not self.lock_path.exists():
            try:
                with self.lock_path.open("xb") as stream:
                    stream.write(LOCK_BYTES)
                    stream.flush()
                    os.fsync(stream.fileno())
                try:
                    os.chmod(self.lock_path, 0o600)
                except OSError:
                    pass
                self._directory_fsync(self.root)
            except FileExistsError:
                pass
        if self.lock_path.is_symlink() or not self.lock_path.is_file():
            raise BlitzortungEventIngressIntegrityError("owner lock is unsafe")
        try:
            lock_bytes = self.lock_path.read_bytes()
        except PermissionError as exc:
            # Windows can deny even a second read while the first instance
            # holds its byte-range lock.  Convert that platform detail to the
            # same explicit ownership refusal used on POSIX.
            key = os.path.normcase(os.path.normpath(str(self.root.resolve())))
            with _PROCESS_GUARD:
                process_owned = key in _PROCESS_ROOT_OWNERS
            if process_owned:
                raise BlitzortungEventIngressBusy(
                    "ingress root is already process-owned"
                ) from exc
            raise BlitzortungEventIngressIntegrityError(
                "owner lock cannot be inspected"
            ) from exc
        if lock_bytes != LOCK_BYTES:
            raise BlitzortungEventIngressIntegrityError("owner lock anchor differs")

    def _acquire_owner(self) -> None:
        key = os.path.normcase(os.path.normpath(str(self.root.resolve())))
        with _PROCESS_GUARD:
            if key in _PROCESS_ROOT_OWNERS:
                raise BlitzortungEventIngressBusy("ingress root is already process-owned")
            _PROCESS_ROOT_OWNERS.add(key)
            self._root_owner_key = key
        descriptor: int | None = None
        try:
            descriptor = os.open(str(self.lock_path), os.O_RDWR)
            deadline = time.monotonic() + float(self._limits.lock_timeout_seconds)
            while True:
                try:
                    _lock_descriptor(descriptor)
                    break
                except (BlockingIOError, PermissionError, OSError) as exc:
                    if isinstance(exc, OSError) and exc.errno not in {
                        None,
                        errno.EACCES,
                        errno.EAGAIN,
                        errno.EDEADLK,
                    }:
                        raise
                    if time.monotonic() >= deadline:
                        raise BlitzortungEventIngressBusy(
                            "ingress OS owner lock is busy"
                        ) from None
                    time.sleep(0.01)
            os.lseek(descriptor, 0, os.SEEK_SET)
            if os.read(descriptor, len(LOCK_BYTES) + 1) != LOCK_BYTES:
                raise BlitzortungEventIngressIntegrityError(
                    "owner lock changed after locking"
                )
            self._lock_descriptor = descriptor
        except BaseException:
            if descriptor is not None:
                os.close(descriptor)
            self._release_process_owner()
            raise

    def _release_process_owner(self) -> None:
        key = self._root_owner_key
        if key is not None:
            with _PROCESS_GUARD:
                _PROCESS_ROOT_OWNERS.discard(key)
            self._root_owner_key = None

    def _cleanup_staging(self) -> None:
        changed = False
        entries = tuple(self.staging_directory.iterdir())
        if len(entries) > self._limits.control_receipt_reserve + 2:
            raise BlitzortungEventIngressIntegrityError(
                "too many interrupted control receipt stages"
            )
        for entry in entries:
            if (
                entry.is_symlink()
                or not entry.is_file()
                or _STAGE_NAME_RE.fullmatch(entry.name) is None
            ):
                raise BlitzortungEventIngressIntegrityError(
                    "staging contains an unknown entry"
                )
            entry.unlink()
            changed = True
        if changed:
            self._directory_fsync(self.staging_directory)

    def _receipt_paths(self) -> tuple[Path, ...]:
        entries = tuple(self.receipts_directory.iterdir())
        if len(entries) > self._limits.max_control_receipts:
            raise BlitzortungEventIngressCapacityError(
                "control receipt count exceeds the configured cap"
            )
        indexed: list[tuple[int, Path]] = []
        for entry in entries:
            match = _RECEIPT_NAME_RE.fullmatch(entry.name)
            if entry.is_symlink() or not entry.is_file() or match is None:
                raise BlitzortungEventIngressIntegrityError(
                    "receipt directory contains an unknown entry"
                )
            indexed.append((int(match.group(1)), entry))
        indexed.sort(key=lambda item: item[0])
        return tuple(path for _, path in indexed)

    def _load_receipts(self) -> None:
        self._state = _IngressState()
        for expected_sequence, path in enumerate(self._receipt_paths(), start=1):
            match = _RECEIPT_NAME_RE.fullmatch(path.name)
            assert match is not None
            if int(match.group(1)) != expected_sequence:
                raise BlitzortungEventIngressIntegrityError(
                    "control receipt sequence is not contiguous"
                )
            size = path.stat().st_size
            if not 0 < size <= MAX_CONTROL_RECEIPT_BYTES:
                raise BlitzortungEventIngressIntegrityError(
                    "control receipt size is outside the hard bound"
                )
            payload = path.read_bytes()
            document = _json_no_duplicates(payload)
            self._validate_receipt_document(
                document,
                filename_sha256=match.group(2),
                expected_sequence=expected_sequence,
            )
            self._apply_event(document)

    def _validate_receipt_document(
        self,
        document: Mapping[str, object],
        *,
        filename_sha256: str,
        expected_sequence: int,
    ) -> None:
        required = {
            "schema",
            "contract",
            "ingress_id",
            "sequence",
            "kind",
            "occurred_at",
            "previous_receipt_sha256",
            "data",
            "transport_completeness",
            "absence_semantics",
            "meteorological_interpretation",
            "operational_admission",
            "receipt_sha256",
        }
        if set(document) != required:
            raise BlitzortungEventIngressIntegrityError(
                "control receipt field set differs"
            )
        if (
            document["schema"] != RECEIPT_SCHEMA
            or document["contract"] != CONTRACT
            or document["ingress_id"] != INGRESS_ID
            or document["transport_completeness"] != TRANSPORT_COMPLETENESS
            or document["absence_semantics"] != ABSENCE_SEMANTICS
            or document["meteorological_interpretation"] != "not_performed"
            or document["operational_admission"] is not False
        ):
            raise BlitzortungEventIngressIntegrityError(
                "control receipt contains a forbidden contract or weather claim"
            )
        if document["sequence"] != expected_sequence:
            raise BlitzortungEventIngressIntegrityError("control sequence differs")
        if document["kind"] not in _EVENT_KINDS:
            raise BlitzortungEventIngressIntegrityError("control event kind is unknown")
        _time_value(document["occurred_at"], "occurred_at")
        previous = _sha(document["previous_receipt_sha256"], "previous_receipt_sha256")
        if previous != self._state.last_receipt_sha256:
            raise BlitzortungEventIngressIntegrityError(
                "control receipt hash chain differs"
            )
        if not isinstance(document["data"], dict):
            raise BlitzortungEventIngressIntegrityError("control receipt data is not an object")
        claimed = _sha(document["receipt_sha256"], "receipt_sha256")
        body = dict(document)
        body.pop("receipt_sha256")
        calculated = _digest(body)
        if claimed != calculated or claimed != filename_sha256:
            raise BlitzortungEventIngressIntegrityError(
                "control receipt content address differs"
            )

    def _append_event(
        self,
        kind: str,
        occurred_at: float,
        data: Mapping[str, object],
        *,
        use_reserve: bool = False,
    ) -> str:
        self._assert_owner()
        if kind not in _EVENT_KINDS:
            raise BlitzortungEventIngressValidationError("event kind is unknown")
        sequence = self._state.sequence + 1
        if sequence > self._limits.max_control_receipts:
            raise BlitzortungEventIngressCapacityError("control receipt capacity exhausted")
        if (
            not use_reserve
            and sequence
            > self._limits.max_control_receipts - self._limits.control_receipt_reserve
        ):
            raise BlitzortungEventIngressCapacityError(
                "control receipt recovery reserve reached"
            )
        body: dict[str, object] = {
            "schema": RECEIPT_SCHEMA,
            "contract": CONTRACT,
            "ingress_id": INGRESS_ID,
            "sequence": sequence,
            "kind": kind,
            "occurred_at": _time_value(occurred_at, "occurred_at"),
            "previous_receipt_sha256": self._state.last_receipt_sha256,
            "data": dict(data),
            "transport_completeness": TRANSPORT_COMPLETENESS,
            "absence_semantics": ABSENCE_SEMANTICS,
            "meteorological_interpretation": "not_performed",
            "operational_admission": False,
        }
        receipt_sha256 = _digest(body)
        document = {**body, "receipt_sha256": receipt_sha256}
        payload = _canonical(document)
        if len(payload) > MAX_CONTROL_RECEIPT_BYTES:
            raise BlitzortungEventIngressCapacityError(
                "control receipt exceeds the hard byte bound"
            )
        stem = f"{sequence:020d}.{receipt_sha256}"
        stage = self.staging_directory / f"{stem}.pending"
        target = self.receipts_directory / f"{stem}.json"
        if stage.exists() or target.exists():
            raise BlitzortungEventIngressIntegrityError(
                "control receipt target already exists"
            )
        published = False
        try:
            self._fault("receipt.before_stage")
            with stage.open("xb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            self._fault("receipt.after_stage_fsync")
            os.replace(stage, target)
            published = True
            self._fault("receipt.after_publish")
            self._directory_fsync(self.receipts_directory)
            self._fault("receipt.after_directory_fsync")
        except BaseException:
            self._poisoned = True
            raise
        if not published:
            self._poisoned = True
            raise BlitzortungEventIngressIntegrityError(
                "control receipt was not published"
            )
        self._apply_event(document)
        return receipt_sha256

    def _apply_event(self, document: Mapping[str, object]) -> None:
        kind = str(document["kind"])
        data = document["data"]
        if not isinstance(data, dict):
            raise BlitzortungEventIngressIntegrityError("event data is invalid")
        receipt_sha = str(document["receipt_sha256"])
        state = self._state
        counters = state.counters
        pending = state.pending
        queue = state.queue
        known = state.known_deliveries
        assert counters is not None and pending is not None and queue is not None and known is not None

        if kind == "store_initialized":
            if state.initialized or document["sequence"] != 1:
                raise BlitzortungEventIngressIntegrityError("store initialized twice")
            if set(data) != {"profile_sha256", "profile"} or not isinstance(data["profile"], dict):
                raise BlitzortungEventIngressIntegrityError("initialization profile is invalid")
            profile_sha = _sha(data["profile_sha256"], "profile_sha256")
            if _digest(data["profile"]) != profile_sha:
                raise BlitzortungEventIngressIntegrityError("initialization profile hash differs")
            state.initialized = True
            state.profile_sha256 = profile_sha
        elif not state.initialized:
            raise BlitzortungEventIngressIntegrityError("event precedes initialization")
        elif kind == "session_started":
            handle = self._handle_from_mapping(data)
            if state.current_session is not None or handle.epoch != state.session_epoch + 1:
                raise BlitzortungEventIngressIntegrityError("session epoch transition is invalid")
            state.current_session = handle.to_metadata()
            state.session_epoch = handle.epoch
            state.last_callback_sequence = None
            counters["sessions_started"] += 1
        elif kind == "session_ended":
            if set(data) != {"binding_sha256", "reason"}:
                raise BlitzortungEventIngressIntegrityError(
                    "session end fields differ"
                )
            _safe_reason(data.get("reason"), _SESSION_END_REASONS)
            _sha(data.get("binding_sha256"), "binding_sha256")
            if state.current_session is None or data.get("binding_sha256") != state.current_session["binding_sha256"]:
                raise BlitzortungEventIngressIntegrityError("ended session is not current")
            state.current_session = None
            state.last_callback_sequence = None
            counters["sessions_ended"] += 1
            counters["transport_gaps"] += 1
        elif kind == "sequence_gap":
            if set(data) != {
                "session_epoch",
                "after_callback_sequence",
                "before_callback_sequence",
                "missing_count",
            }:
                raise BlitzortungEventIngressIntegrityError(
                    "sequence gap fields differ"
                )
            if state.current_session is None or data.get("session_epoch") != state.current_session["epoch"]:
                raise BlitzortungEventIngressIntegrityError(
                    "sequence gap session is not current"
                )
            after = _callback_sequence(data.get("after_callback_sequence"))
            before = _callback_sequence(data.get("before_callback_sequence"))
            missing = data.get("missing_count")
            if isinstance(missing, bool) or not isinstance(missing, int) or missing <= 0:
                raise BlitzortungEventIngressIntegrityError("sequence gap count is invalid")
            if (
                state.last_callback_sequence != after
                or before <= after + 1
                or missing != before - after - 1
            ):
                raise BlitzortungEventIngressIntegrityError(
                    "sequence gap accounting differs"
                )
            counters["sequence_gaps"] += 1
            counters["callback_positions_missing"] += missing
            counters["transport_gaps"] += 1
        elif kind == "delivery_intent":
            self._validate_intent_data(data)
            if receipt_sha in pending:
                raise BlitzortungEventIngressIntegrityError("intent is duplicated")
            if state.current_session is None or data["session_binding_sha256"] != state.current_session["binding_sha256"]:
                raise BlitzortungEventIngressIntegrityError("intent session is not current")
            pending[receipt_sha] = dict(data)
            callback = int(data["callback_sequence"])
            prior = state.last_callback_sequence
            state.last_callback_sequence = callback if prior is None else max(prior, callback)
            counters["deliveries_received"] += 1
        elif kind == "callback_refused":
            if set(data) != {
                "callback_sequence",
                "reason",
                "session_binding_sha256",
                "session_epoch",
            }:
                raise BlitzortungEventIngressIntegrityError(
                    "callback refusal fields differ"
                )
            if (
                state.current_session is None
                or data.get("session_epoch") != state.current_session["epoch"]
                or data.get("session_binding_sha256")
                != state.current_session["binding_sha256"]
            ):
                raise BlitzortungEventIngressIntegrityError(
                    "callback refusal session is not current"
                )
            callback = _callback_sequence(data.get("callback_sequence"))
            _safe_reason(data.get("reason"), _CALLBACK_REFUSAL_REASONS)
            prior = state.last_callback_sequence
            state.last_callback_sequence = (
                callback if prior is None else max(prior, callback)
            )
            counters["deliveries_received"] += 1
            counters["drops"] += 1
            counters["transport_gaps"] += 1
        elif kind in {
            "delivery_accepted",
            "delivery_duplicate",
            "delivery_conflict",
            "delivery_refused",
            "crash_gap",
        }:
            required_outcome_fields = {
                "delivery_accepted": {
                    "intent_receipt_sha256",
                    "raw_reference",
                    "out_of_order",
                },
                "delivery_refused": {"intent_receipt_sha256", "reason"},
                "delivery_duplicate": {"intent_receipt_sha256"},
                "delivery_conflict": {"intent_receipt_sha256"},
                "crash_gap": {"intent_receipt_sha256"},
            }
            if set(data) != required_outcome_fields[kind]:
                raise BlitzortungEventIngressIntegrityError(
                    "delivery outcome fields differ"
                )
            intent_sha = _sha(data.get("intent_receipt_sha256"), "intent_receipt_sha256")
            intent = pending.pop(intent_sha, None)
            if intent is None:
                raise BlitzortungEventIngressIntegrityError("outcome has no pending intent")
            if kind == "delivery_accepted":
                reference_mapping = data.get("raw_reference")
                if not isinstance(reference_mapping, dict):
                    raise BlitzortungEventIngressIntegrityError("accepted delivery lacks raw reference")
                reference = BlitzortungRawIngressReference(**reference_mapping)
                if reference.content_sha256 != intent["content_sha256"] or reference.size_bytes != intent["size_bytes"]:
                    raise BlitzortungEventIngressIntegrityError("raw reference differs from intent")
                reference_key = _digest({
                    "schema": "stv3-blitzortung-event-delivery-key-v1",
                    "broker_identity_sha256": reference.broker_identity_sha256,
                    "session_identity_sha256": reference.session_identity_sha256,
                    "delivery_identity_sha256": reference.delivery_identity_sha256,
                })
                reference_fingerprint = _digest({
                    "schema": "stv3-blitzortung-event-delivery-fingerprint-v1",
                    "content_sha256": reference.content_sha256,
                    "size_bytes": reference.size_bytes,
                    "topic": reference.topic,
                    "qos": reference.qos,
                    "retain": reference.retain,
                    "received_at": float(reference.received_at),
                    "desired_state_sha256": reference.desired_state_sha256,
                    "subscription_set_sha256": reference.subscription_set_sha256,
                    "callback_sequence": intent["callback_sequence"],
                })
                if (
                    reference_key != intent["delivery_key_sha256"]
                    or reference_fingerprint
                    != intent["delivery_fingerprint_sha256"]
                    or reference.topic != intent["topic"]
                    or reference.qos != intent["qos"]
                    or reference.retain is not intent["retain"]
                    or reference.received_at != intent["received_at"]
                ):
                    raise BlitzortungEventIngressIntegrityError(
                        "raw reference transport facts differ from intent"
                    )
                if len(queue) >= self._limits.max_queue_entries:
                    raise BlitzortungEventIngressIntegrityError("durable queue exceeds cap")
                delivery_key = str(intent["delivery_key_sha256"])
                if delivery_key in known:
                    raise BlitzortungEventIngressIntegrityError("accepted delivery identity repeats")
                item = {
                    "queue_receipt_sha256": receipt_sha,
                    "intent_receipt_sha256": intent_sha,
                    "delivery_key_sha256": delivery_key,
                    "delivery_fingerprint_sha256": intent["delivery_fingerprint_sha256"],
                    "session_epoch": intent["session_epoch"],
                    "callback_sequence": intent["callback_sequence"],
                    "raw_reference": reference.to_metadata(),
                }
                queue.append(item)
                known[delivery_key] = {
                    "delivery_fingerprint_sha256": intent["delivery_fingerprint_sha256"],
                    "queue_receipt_sha256": receipt_sha,
                }
                state.retained_payload_entries += 1
                state.retained_payload_bytes += int(intent["size_bytes"])
                counters["deliveries_accepted"] += 1
                expected_out_of_order = (
                    int(intent["callback_sequence"])
                    < int(state.last_callback_sequence)
                )
                if data.get("out_of_order") is not expected_out_of_order:
                    raise BlitzortungEventIngressIntegrityError(
                        "out-of-order flag differs from callback history"
                    )
                if expected_out_of_order:
                    counters["out_of_order"] += 1
            elif kind == "delivery_duplicate":
                prior_delivery = known.get(str(intent["delivery_key_sha256"]))
                if (
                    prior_delivery is None
                    or prior_delivery["delivery_fingerprint_sha256"]
                    != intent["delivery_fingerprint_sha256"]
                ):
                    raise BlitzortungEventIngressIntegrityError(
                        "duplicate outcome has no identical prior delivery"
                    )
                counters["duplicates"] += 1
            elif kind == "delivery_conflict":
                prior_delivery = known.get(str(intent["delivery_key_sha256"]))
                if (
                    prior_delivery is None
                    or prior_delivery["delivery_fingerprint_sha256"]
                    == intent["delivery_fingerprint_sha256"]
                ):
                    raise BlitzortungEventIngressIntegrityError(
                        "conflict outcome has no differing prior delivery"
                    )
                counters["conflicts"] += 1
                counters["drops"] += 1
                counters["transport_gaps"] += 1
            elif kind == "delivery_refused":
                reason = _safe_reason(data.get("reason"), _REFUSAL_REASONS)
                counters["drops"] += 1
                counters["transport_gaps"] += 1
                if reason.startswith("raw_store_"):
                    counters["raw_failures"] += 1
            else:
                counters["drops"] += 1
                counters["transport_gaps"] += 1
                counters["crash_recoveries"] += 1
        elif kind == "delivery_claimed":
            if set(data) != {
                "claim_token_sha256",
                "queue_receipt_sha256",
                "consumer_identity_sha256",
                "claimed_at",
                "expires_at",
            }:
                raise BlitzortungEventIngressIntegrityError(
                    "queue claim fields differ"
                )
            if state.active_claim is not None or not queue:
                raise BlitzortungEventIngressIntegrityError("queue claim transition is invalid")
            if data.get("queue_receipt_sha256") != queue[0]["queue_receipt_sha256"]:
                raise BlitzortungEventIngressIntegrityError("queue claim is not FIFO")
            consumer = _sha(
                data.get("consumer_identity_sha256"),
                "consumer_identity_sha256",
            )
            claimed = _time_value(data.get("claimed_at"), "claimed_at")
            expires = _time_value(data.get("expires_at"), "expires_at")
            if expires != claimed + float(self._limits.claim_lease_seconds):
                raise BlitzortungEventIngressIntegrityError("claim lease is invalid")
            expected_token = _digest({
                "schema": CLAIM_SCHEMA,
                "consumer_identity_sha256": consumer,
                "queue_receipt_sha256": data["queue_receipt_sha256"],
                "claimed_at": claimed,
                "authority_sequence": document["sequence"],
            })
            if data.get("claim_token_sha256") != expected_token:
                raise BlitzortungEventIngressIntegrityError(
                    "claim token does not bind its durable lease"
                )
            state.active_claim = dict(data)
        elif kind == "claim_released":
            if set(data) != {"claim_token_sha256", "reason"}:
                raise BlitzortungEventIngressIntegrityError(
                    "claim release fields differ"
                )
            _safe_reason(data.get("reason"), _CLAIM_RELEASE_REASONS)
            if state.active_claim is None or data.get("claim_token_sha256") != state.active_claim["claim_token_sha256"]:
                raise BlitzortungEventIngressIntegrityError("claim release token differs")
            state.active_claim = None
            counters["claims_recovered"] += 1
        elif kind == "delivery_completed":
            if set(data) != {"claim_token_sha256", "queue_receipt_sha256"}:
                raise BlitzortungEventIngressIntegrityError(
                    "completion fields differ"
                )
            if state.active_claim is None or not queue:
                raise BlitzortungEventIngressIntegrityError("completion has no active claim")
            if data.get("claim_token_sha256") != state.active_claim["claim_token_sha256"]:
                raise BlitzortungEventIngressIntegrityError("completion token differs")
            if data.get("queue_receipt_sha256") != queue[0]["queue_receipt_sha256"]:
                raise BlitzortungEventIngressIntegrityError("completion is not FIFO")
            queue.pop(0)
            state.active_claim = None
            counters["deliveries_completed"] += 1
        elif kind == "control_capacity_latched":
            if data != {"reason": "control_receipt_reserve_reached"}:
                raise BlitzortungEventIngressIntegrityError(
                    "control capacity latch fields differ"
                )
            if state.control_capacity_latched:
                raise BlitzortungEventIngressIntegrityError("control capacity latched twice")
            state.control_capacity_latched = True
            counters["drops"] += 1
            counters["transport_gaps"] += 1
        else:
            raise BlitzortungEventIngressIntegrityError("unhandled event kind")

        state.sequence = int(document["sequence"])
        state.last_receipt_sha256 = receipt_sha
        if state.retained_payload_entries > self._limits.max_retained_payload_entries or state.retained_payload_bytes > self._limits.max_retained_payload_bytes:
            raise BlitzortungEventIngressIntegrityError("retained payload accounting exceeds cap")

    @staticmethod
    def _validate_intent_data(data: Mapping[str, object]) -> None:
        required = {
            "session_epoch",
            "session_binding_sha256",
            "callback_sequence",
            "delivery_key_sha256",
            "delivery_fingerprint_sha256",
            "content_sha256",
            "size_bytes",
            "topic",
            "qos",
            "retain",
            "received_at",
        }
        if set(data) != required:
            raise BlitzortungEventIngressIntegrityError("delivery intent fields differ")
        if isinstance(data["session_epoch"], bool) or not isinstance(data["session_epoch"], int) or data["session_epoch"] <= 0:
            raise BlitzortungEventIngressIntegrityError("intent epoch is invalid")
        for name in (
            "session_binding_sha256",
            "delivery_key_sha256",
            "delivery_fingerprint_sha256",
            "content_sha256",
        ):
            _sha(data[name], name)
        _callback_sequence(data["callback_sequence"])
        if (
            isinstance(data["size_bytes"], bool)
            or not isinstance(data["size_bytes"], int)
            or not 0 <= data["size_bytes"] <= 256 * 1024
        ):
            raise BlitzortungEventIngressIntegrityError("intent payload size is invalid")
        topic = data["topic"]
        if not isinstance(topic, str):
            raise BlitzortungEventIngressIntegrityError(
                "intent MQTT facts are invalid"
            )
        try:
            topic_bytes = topic.encode("utf-8", errors="strict")
        except UnicodeError as exc:
            raise BlitzortungEventIngressIntegrityError(
                "intent MQTT topic is not strict UTF-8"
            ) from exc
        if (
            not topic_bytes
            or len(topic_bytes) > 512
            or _PUBLIC_TOPIC_RE.fullmatch(topic) is None
            or any(
                segment.lower() in _SENSITIVE_TOPIC_SEGMENTS
                for segment in topic.split("/")
            )
            or isinstance(data["qos"], bool)
            or data["qos"] != 0
            or type(data["retain"]) is not bool
        ):
            raise BlitzortungEventIngressIntegrityError("intent MQTT facts are invalid")
        _time_value(data["received_at"], "received_at")

    @staticmethod
    def _handle_from_mapping(value: Mapping[str, object]) -> BlitzortungSessionHandle:
        required = {
            "schema",
            "epoch",
            "broker_identity_sha256",
            "session_identity_sha256",
            "desired_state_sha256",
            "subscription_set_sha256",
            "connection_provenance_sha256",
            "binding_sha256",
        }
        if set(value) != required or value.get("schema") != SESSION_SCHEMA:
            raise BlitzortungEventIngressIntegrityError(
                "session handle fields differ"
            )
        try:
            return BlitzortungSessionHandle(
                epoch=value["epoch"],  # type: ignore[arg-type]
                broker_identity_sha256=value["broker_identity_sha256"],  # type: ignore[arg-type]
                session_identity_sha256=value["session_identity_sha256"],  # type: ignore[arg-type]
                desired_state_sha256=value["desired_state_sha256"],  # type: ignore[arg-type]
                subscription_set_sha256=value["subscription_set_sha256"],  # type: ignore[arg-type]
                connection_provenance_sha256=value["connection_provenance_sha256"],  # type: ignore[arg-type]
                binding_sha256=value["binding_sha256"],  # type: ignore[arg-type]
            )
        except KeyError as exc:
            raise BlitzortungEventIngressIntegrityError(
                "session handle field is missing"
            ) from exc

    def _recover_interrupted_owner(self) -> None:
        now = self._now()
        if self._state.active_claim is not None:
            self._append_event(
                "claim_released",
                now,
                {
                    "claim_token_sha256": self._state.active_claim["claim_token_sha256"],
                    "reason": "process_restart",
                },
                use_reserve=True,
            )
        if self._state.current_session is not None:
            self._append_event(
                "session_ended",
                now,
                {
                    "binding_sha256": self._state.current_session["binding_sha256"],
                    "reason": "process_restart",
                },
                use_reserve=True,
            )
        pending = tuple((self._state.pending or {}).keys())
        for intent_sha in pending:
            self._append_event(
                "crash_gap",
                now,
                {"intent_receipt_sha256": intent_sha},
                use_reserve=True,
            )

    def _assert_owner(self) -> None:
        if self._closed or self._lock_descriptor is None:
            raise BlitzortungEventIngressBusy("ingress authority is closed")
        if os.getpid() != self._owner_pid:
            raise BlitzortungEventIngressBusy("ingress authority crossed a process boundary")
        if self._poisoned:
            raise BlitzortungEventIngressIntegrityError(
                "ingress authority requires close and reopen after interrupted write"
            )

    @contextmanager
    def _exclusive_operation(self) -> Iterator[None]:
        with self._operation_lock:
            self._assert_owner()
            yield

    def _preflight_receipts(self, needed: int) -> None:
        if self._state.control_capacity_latched:
            raise BlitzortungEventIngressCapacityError(
                "control receipt capacity is durably latched"
            )
        remaining = self._limits.max_control_receipts - self._state.sequence
        if remaining < needed + self._limits.control_receipt_reserve:
            if remaining > 0:
                self._append_event(
                    "control_capacity_latched",
                    self._now(),
                    {"reason": "control_receipt_reserve_reached"},
                    use_reserve=True,
                )
            raise BlitzortungEventIngressCapacityError(
                "control receipt recovery reserve reached"
            )

    def _synthetic_retention_allowed(self, now: float) -> bool:
        decision = self._retention_decision
        return bool(
            self._synthetic_fixture_mode
            and decision is not None
            and decision.source_scope == "synthetic_fixture_only"
            and decision.durable_payload_retention_allowed
            and decision.valid_from <= now <= decision.valid_until
        )

    def _production_retention_allowed(self, now: float) -> bool:
        decision = self._retention_decision
        return bool(
            not self._synthetic_fixture_mode
            and decision is not None
            and decision.source_scope == "public_blitzortung"
            and decision.durable_payload_retention_allowed
            and decision.source_terms_sha256 == SOURCE_TERMS_SHA256
            and decision.provenance_authority_sha256
            == PRODUCTION_AUTHORITY_SHA256
            and decision.valid_from <= now <= decision.valid_until
        )

    def _retention_allowed(self, now: float) -> bool:
        return self._synthetic_retention_allowed(
            now
        ) or self._production_retention_allowed(now)

    def begin_session(
        self,
        *,
        broker_identity_sha256: str,
        session_identity_sha256: str,
        desired_state_sha256: str,
        subscription_set_sha256: str,
        connection_provenance_sha256: str,
        connected_at: float,
    ) -> BlitzortungSessionHandle:
        with self._exclusive_operation():
            when = _time_value(connected_at, "connected_at")
            if self._state.session_epoch >= self._limits.max_sessions:
                raise BlitzortungEventIngressCapacityError("session epoch capacity exhausted")
            body = {
                "schema": SESSION_SCHEMA,
                "epoch": self._state.session_epoch + 1,
                "broker_identity_sha256": _sha(
                    broker_identity_sha256, "broker_identity_sha256"
                ),
                "session_identity_sha256": _sha(
                    session_identity_sha256, "session_identity_sha256"
                ),
                "desired_state_sha256": _sha(
                    desired_state_sha256, "desired_state_sha256"
                ),
                "subscription_set_sha256": _sha(
                    subscription_set_sha256, "subscription_set_sha256"
                ),
                "connection_provenance_sha256": _sha(
                    connection_provenance_sha256,
                    "connection_provenance_sha256",
                ),
            }
            handle = BlitzortungSessionHandle(
                epoch=int(body["epoch"]),
                broker_identity_sha256=str(body["broker_identity_sha256"]),
                session_identity_sha256=str(body["session_identity_sha256"]),
                desired_state_sha256=str(body["desired_state_sha256"]),
                subscription_set_sha256=str(body["subscription_set_sha256"]),
                connection_provenance_sha256=str(body["connection_provenance_sha256"]),
                binding_sha256=_digest(body),
            )
            # Validate the complete replacement handle and the epoch cap before
            # ending a healthy current session.  Invalid caller input must not
            # create a transport gap or strand the authority without a session.
            self._preflight_receipts(2 if self._state.current_session else 1)
            if self._state.current_session is not None:
                self._append_event(
                    "session_ended",
                    when,
                    {
                        "binding_sha256": self._state.current_session["binding_sha256"],
                        "reason": "session_replaced",
                    },
                )
            self._append_event("session_started", when, handle.to_metadata())
            return handle

    def end_session(
        self,
        handle: BlitzortungSessionHandle,
        *,
        disconnected_at: float,
        reason: str,
    ) -> str:
        with self._exclusive_operation():
            self._require_current_session(handle)
            reason_code = _safe_reason(reason, _SESSION_END_REASONS - {"process_restart"})
            when = _time_value(disconnected_at, "disconnected_at")
            self._preflight_receipts(1)
            return self._append_event(
                "session_ended",
                when,
                {"binding_sha256": handle.binding_sha256, "reason": reason_code},
            )

    def _require_current_session(self, handle: object) -> BlitzortungSessionHandle:
        if not isinstance(handle, BlitzortungSessionHandle):
            raise BlitzortungEventIngressValidationError("session handle type is invalid")
        current = self._state.current_session
        if current is None or current != handle.to_metadata():
            raise BlitzortungEventIngressConflict("session handle is not the current epoch")
        return handle

    def _validate_envelope_binding(
        self,
        handle: BlitzortungSessionHandle,
        envelope: object,
    ) -> BlitzortungMqttEnvelope:
        if not isinstance(envelope, BlitzortungMqttEnvelope):
            raise BlitzortungEventIngressValidationError("MQTT envelope type is invalid")
        # Reuse the strict exact-byte ingress validation without writing by
        # checking all session-bound facts here.  The raw function revalidates
        # payload/topic/QoS/bounds immediately before storage.
        if (
            envelope.broker_identity_sha256 != handle.broker_identity_sha256
            or envelope.session_identity_sha256 != handle.session_identity_sha256
            or envelope.desired_state_sha256 != handle.desired_state_sha256
            or envelope.subscription_set_sha256 != handle.subscription_set_sha256
        ):
            raise BlitzortungEventIngressConflict(
                "MQTT envelope differs from the active session binding"
            )
        if not isinstance(envelope.payload, bytes):
            raise BlitzortungEventIngressValidationError(
                "MQTT payload must be exact immutable bytes"
            )
        _sha(envelope.delivery_identity_sha256, "delivery_identity_sha256")
        if len(envelope.payload) > 256 * 1024:
            raise BlitzortungEventIngressValidationError(
                "MQTT payload exceeds the exact-raw hard cap"
            )
        if isinstance(envelope.qos, bool) or envelope.qos != 0:
            raise BlitzortungEventIngressValidationError(
                "only exact integer QoS 0 is accepted"
            )
        if type(envelope.retain) is not bool:
            raise BlitzortungEventIngressValidationError(
                "MQTT retain must be an exact boolean"
            )
        _time_value(envelope.received_at, "received_at")
        if not isinstance(envelope.topic, str):
            raise BlitzortungEventIngressValidationError("MQTT topic must be text")
        try:
            topic_bytes = envelope.topic.encode("utf-8", errors="strict")
        except UnicodeError as exc:
            raise BlitzortungEventIngressValidationError(
                "MQTT topic is not strict UTF-8"
            ) from exc
        if not topic_bytes or len(topic_bytes) > 512:
            raise BlitzortungEventIngressValidationError(
                "MQTT topic exceeds the exact-raw hard cap"
            )
        if (
            _PUBLIC_TOPIC_RE.fullmatch(envelope.topic) is None
            or "+" in envelope.topic
            or "#" in envelope.topic
        ):
            raise BlitzortungEventIngressValidationError(
                "MQTT topic is not one concrete public Blitzortung topic"
            )
        if any(
            segment.lower() in _SENSITIVE_TOPIC_SEGMENTS
            for segment in envelope.topic.split("/")
        ):
            raise BlitzortungEventIngressValidationError(
                "MQTT topic contains credential-shaped text"
            )
        return envelope

    def record_callback_refusal(
        self,
        handle: BlitzortungSessionHandle,
        *,
        callback_sequence: int,
        refused_at: float,
        reason: str,
    ) -> str:
        """Durably account for one callback that cannot form an envelope.

        The receipt is deliberately payload-, topic- and identity-free.  It
        proves a transport/drop gap without turning the callback into a
        lightning or no-lightning observation.
        """

        with self._exclusive_operation():
            active = self._require_current_session(handle)
            callback = _callback_sequence(callback_sequence)
            when = _time_value(refused_at, "refused_at")
            reason_code = _safe_reason(reason, _CALLBACK_REFUSAL_REASONS)
            prior_callback = self._state.last_callback_sequence
            sequence_gap = (
                prior_callback is not None and callback > prior_callback + 1
            )
            self._preflight_receipts(2 if sequence_gap else 1)
            if sequence_gap:
                assert prior_callback is not None
                self._append_event(
                    "sequence_gap",
                    when,
                    {
                        "session_epoch": active.epoch,
                        "after_callback_sequence": prior_callback,
                        "before_callback_sequence": callback,
                        "missing_count": callback - prior_callback - 1,
                    },
                )
            return self._append_event(
                "callback_refused",
                when,
                {
                    "callback_sequence": callback,
                    "reason": reason_code,
                    "session_binding_sha256": active.binding_sha256,
                    "session_epoch": active.epoch,
                },
            )

    def offer_delivery(
        self,
        handle: BlitzortungSessionHandle,
        envelope: BlitzortungMqttEnvelope,
        *,
        callback_sequence: int,
    ) -> BlitzortungOfferResult:
        with self._exclusive_operation():
            active = self._require_current_session(handle)
            exact = self._validate_envelope_binding(active, envelope)
            callback = _callback_sequence(callback_sequence)
            received_at = _time_value(exact.received_at, "received_at")

            prior_callback = self._state.last_callback_sequence
            sequence_gap = (
                prior_callback is not None and callback > prior_callback + 1
            )
            self._preflight_receipts(3 if sequence_gap else 2)
            if sequence_gap:
                assert prior_callback is not None
                self._append_event(
                    "sequence_gap",
                    received_at,
                    {
                        "session_epoch": active.epoch,
                        "after_callback_sequence": prior_callback,
                        "before_callback_sequence": callback,
                        "missing_count": callback - prior_callback - 1,
                    },
                )

            delivery_key = _delivery_key(exact)
            fingerprint = _delivery_fingerprint(exact, callback)
            intent_data: dict[str, object] = {
                "session_epoch": active.epoch,
                "session_binding_sha256": active.binding_sha256,
                "callback_sequence": callback,
                "delivery_key_sha256": delivery_key,
                "delivery_fingerprint_sha256": fingerprint,
                "content_sha256": hashlib.sha256(exact.payload).hexdigest(),
                "size_bytes": len(exact.payload),
                "topic": exact.topic,
                "qos": exact.qos,
                "retain": exact.retain,
                "received_at": received_at,
            }
            intent_sha = self._append_event(
                "delivery_intent", received_at, intent_data
            )
            self._offer_fault("offer.after_intent")

            known = (self._state.known_deliveries or {}).get(delivery_key)
            if known is not None:
                if known["delivery_fingerprint_sha256"] != fingerprint:
                    receipt_sha = self._append_event(
                        "delivery_conflict",
                        received_at,
                        {"intent_receipt_sha256": intent_sha},
                    )
                    raise BlitzortungEventIngressConflict(
                        f"delivery identity conflict receipt={receipt_sha}"
                    )
                receipt_sha = self._append_event(
                    "delivery_duplicate",
                    received_at,
                    {"intent_receipt_sha256": intent_sha},
                )
                return BlitzortungOfferResult(
                    status="exact_duplicate",
                    event_receipt_sha256=receipt_sha,
                    raw_reference=None,
                    duplicate=True,
                    out_of_order=(prior_callback is not None and callback < prior_callback),
                )

            if len(self._state.queue or ()) >= self._limits.max_queue_entries:
                receipt_sha = self._append_event(
                    "delivery_refused",
                    received_at,
                    {
                        "intent_receipt_sha256": intent_sha,
                        "reason": "queue_capacity",
                    },
                )
                raise BlitzortungEventIngressBackpressure(
                    f"durable queue full receipt={receipt_sha}"
                )
            if self._state.retained_payload_entries >= self._limits.max_retained_payload_entries:
                receipt_sha = self._append_event(
                    "delivery_refused",
                    received_at,
                    {
                        "intent_receipt_sha256": intent_sha,
                        "reason": "retained_payload_entry_capacity",
                    },
                )
                raise BlitzortungEventIngressCapacityError(
                    f"retained entry capacity receipt={receipt_sha}"
                )
            if self._state.retained_payload_bytes + len(exact.payload) > self._limits.max_retained_payload_bytes:
                receipt_sha = self._append_event(
                    "delivery_refused",
                    received_at,
                    {
                        "intent_receipt_sha256": intent_sha,
                        "reason": "retained_payload_byte_capacity",
                    },
                )
                raise BlitzortungEventIngressCapacityError(
                    f"retained byte capacity receipt={receipt_sha}"
                )
            if not self._retention_allowed(received_at):
                reason = (
                    "connection_provenance_unproven"
                    if self._retention_decision is not None
                    and self._retention_decision.source_scope == "public_blitzortung"
                    else "production_retention_rights_unproven"
                )
                receipt_sha = self._append_event(
                    "delivery_refused",
                    received_at,
                    {"intent_receipt_sha256": intent_sha, "reason": reason},
                )
                raise BlitzortungEventIngressBlocked(
                    f"public payload retention is blocked receipt={receipt_sha}"
                )

            try:
                reference = persist_blitzortung_mqtt_envelope(
                    self._raw_store, exact
                )
            except Exception as exc:
                if isinstance(exc, RawArtifactCapacityError):
                    reason = "raw_store_capacity"
                elif isinstance(exc, RawArtifactConflictError):
                    reason = "raw_store_conflict"
                else:
                    reason = "raw_store_failure"
                try:
                    self._append_event(
                        "delivery_refused",
                        received_at,
                        {"intent_receipt_sha256": intent_sha, "reason": reason},
                    )
                except BaseException:
                    self._poisoned = True
                raise
            self._offer_fault("offer.after_raw_persist")
            out_of_order = prior_callback is not None and callback < prior_callback
            receipt_sha = self._append_event(
                "delivery_accepted",
                received_at,
                {
                    "intent_receipt_sha256": intent_sha,
                    "raw_reference": reference.to_metadata(),
                    "out_of_order": out_of_order,
                },
            )
            self._offer_fault("offer.after_outcome")
            return BlitzortungOfferResult(
                status="accepted_shadow_raw",
                event_receipt_sha256=receipt_sha,
                raw_reference=reference,
                duplicate=False,
                out_of_order=out_of_order,
            )

    def claim_next(
        self,
        *,
        consumer_identity_sha256: str,
        now: float | None = None,
    ) -> BlitzortungQueueClaim | None:
        with self._exclusive_operation():
            when = self._now() if now is None else _time_value(now, "now")
            consumer = _sha(consumer_identity_sha256, "consumer_identity_sha256")
            if self._state.active_claim is not None:
                expires = float(self._state.active_claim["expires_at"])
                if expires > when:
                    if self._state.active_claim["consumer_identity_sha256"] != consumer:
                        raise BlitzortungEventIngressBusy(
                            "queue head is leased to another consumer"
                        )
                    return self._claim_from_state()
                self._append_event(
                    "claim_released",
                    when,
                    {
                        "claim_token_sha256": self._state.active_claim["claim_token_sha256"],
                        "reason": "lease_expired",
                    },
                    use_reserve=True,
                )
            if not self._state.queue:
                return None
            self._preflight_receipts(1)
            head = self._state.queue[0]
            # Verify the complete schema-3 capture and exact object bytes
            # before issuing a decoder-facing claim.  This is one bounded raw
            # read for the single FIFO head, never a full queue scan.
            self._verified_queue_reference(head)
            token = _digest({
                "schema": CLAIM_SCHEMA,
                "consumer_identity_sha256": consumer,
                "queue_receipt_sha256": head["queue_receipt_sha256"],
                "claimed_at": when,
                "authority_sequence": self._state.sequence + 1,
            })
            self._append_event(
                "delivery_claimed",
                when,
                {
                    "claim_token_sha256": token,
                    "queue_receipt_sha256": head["queue_receipt_sha256"],
                    "consumer_identity_sha256": consumer,
                    "claimed_at": when,
                    "expires_at": when + float(self._limits.claim_lease_seconds),
                },
            )
            return self._claim_from_state()

    def _claim_from_state(
        self,
        *,
        verify_raw: bool = True,
    ) -> BlitzortungQueueClaim:
        claim = self._state.active_claim
        queue = self._state.queue
        if claim is None or not queue:
            raise BlitzortungEventIngressIntegrityError("claim state is incomplete")
        head = queue[0]
        if verify_raw:
            reference = self._verified_queue_reference(head)
        else:
            reference_mapping = head.get("raw_reference")
            if not isinstance(reference_mapping, dict):
                raise BlitzortungEventIngressIntegrityError(
                    "queued raw reference is missing"
                )
            reference = BlitzortungRawIngressReference(**reference_mapping)
        return BlitzortungQueueClaim(
            claim_token_sha256=str(claim["claim_token_sha256"]),
            queue_receipt_sha256=str(head["queue_receipt_sha256"]),
            consumer_identity_sha256=str(claim["consumer_identity_sha256"]),
            raw_reference=reference,
            session_epoch=int(head["session_epoch"]),
            callback_sequence=int(head["callback_sequence"]),
            claimed_at=float(claim["claimed_at"]),
            expires_at=float(claim["expires_at"]),
        )

    def _require_active_claim(
        self,
        claim: object,
        *,
        verify_raw: bool,
    ) -> BlitzortungQueueClaim:
        if not isinstance(claim, BlitzortungQueueClaim):
            raise BlitzortungEventIngressConflict("queue claim is not active")
        active = self._state.active_claim
        if active is None or active["claim_token_sha256"] != claim.claim_token_sha256:
            raise BlitzortungEventIngressConflict("queue claim is not active")
        expected = self._claim_from_state(verify_raw=verify_raw)
        if claim != expected:
            raise BlitzortungEventIngressConflict(
                "queue claim fields differ from the active durable lease"
            )
        return expected

    def _verified_queue_reference(
        self,
        item: Mapping[str, object],
    ) -> BlitzortungRawIngressReference:
        reference_mapping = item.get("raw_reference")
        if not isinstance(reference_mapping, dict):
            raise BlitzortungEventIngressIntegrityError(
                "queued raw reference is missing"
            )
        reference = BlitzortungRawIngressReference(**reference_mapping)
        bundle = self._raw_store.load_capture_bundle(reference.capture_id)
        if bundle is None:
            raise BlitzortungEventIngressIntegrityError(
                "queued raw capture no longer exists"
            )
        capture, receipts = bundle
        receipt = receipts.get("mqtt_payload")
        if (
            capture.capture_receipt_sha256 != reference.capture_receipt_sha256
            or capture.manifest_sha256 != reference.manifest_sha256
            or len(capture.members) != 1
            or receipt is None
            or receipt.receipt_id != reference.receipt_id
            or receipt.receipt_sha256 != reference.receipt_sha256
            or receipt.provenance_sha256 != reference.provenance_sha256
            or receipt.content_sha256 != reference.content_sha256
            or receipt.size_bytes != reference.size_bytes
        ):
            raise BlitzortungEventIngressIntegrityError(
                "queued raw capture differs from its control receipt"
            )
        return reference

    def release_claim(
        self,
        claim: BlitzortungQueueClaim,
        *,
        now: float,
        reason: str,
    ) -> str:
        with self._exclusive_operation():
            exact = self._require_active_claim(claim, verify_raw=False)
            reason_code = _safe_reason(
                reason,
                frozenset({"consumer_cancellation", "consumer_failure"}),
            )
            when = _time_value(now, "now")
            if when < exact.claimed_at:
                raise BlitzortungEventIngressValidationError(
                    "claim release precedes lease acquisition"
                )
            expired = when >= exact.expires_at
            if not expired:
                self._preflight_receipts(1)
            return self._append_event(
                "claim_released",
                when,
                {
                    "claim_token_sha256": exact.claim_token_sha256,
                    "reason": "lease_expired" if expired else reason_code,
                },
                use_reserve=expired,
            )

    def complete_claim(
        self,
        claim: BlitzortungQueueClaim,
        *,
        completed_at: float,
    ) -> str:
        with self._exclusive_operation():
            exact = self._require_active_claim(claim, verify_raw=False)
            when = _time_value(completed_at, "completed_at")
            if when < exact.claimed_at:
                raise BlitzortungEventIngressValidationError(
                    "claim completion precedes lease acquisition"
                )
            if when >= exact.expires_at:
                self._append_event(
                    "claim_released",
                    when,
                    {
                        "claim_token_sha256": exact.claim_token_sha256,
                        "reason": "lease_expired",
                    },
                    use_reserve=True,
                )
                raise BlitzortungEventIngressConflict(
                    "queue claim lease expired before completion"
                )
            # Completion removes the queue reference.  Re-verify the exact raw
            # object at this destructive boundary, not only when it was claimed.
            exact = self._require_active_claim(exact, verify_raw=True)
            self._preflight_receipts(1)
            return self._append_event(
                "delivery_completed",
                when,
                {
                    "claim_token_sha256": exact.claim_token_sha256,
                    "queue_receipt_sha256": exact.queue_receipt_sha256,
                },
            )

    def diagnostics(self) -> dict[str, object]:
        with self._exclusive_operation():
            counters = dict(self._state.counters or {})
            now = self._now()
            production_authorized = self._production_retention_allowed(now)
            synthetic_authorized = self._synthetic_retention_allowed(now)
            return {
                "schema": DIAGNOSTICS_SCHEMA,
                "ingress_id": INGRESS_ID,
                "ownership_status": (
                    "production_exact_raw_authorized"
                    if production_authorized
                    else "synthetic_shadow_only"
                    if synthetic_authorized
                    else "ownership_blocked"
                ),
                "production_retention_authorized": production_authorized,
                "operational_admission": False,
                "transport_completeness": TRANSPORT_COMPLETENESS,
                "absence_semantics": ABSENCE_SEMANTICS,
                "silence_interpretation": "unknown_not_an_absence_observation",
                "queue_depth": len(self._state.queue or ()),
                "queue_capacity": self._limits.max_queue_entries,
                "pending_intents": len(self._state.pending or {}),
                "inflight_claims": int(self._state.active_claim is not None),
                "current_session_epoch": (
                    self._state.session_epoch
                    if self._state.current_session is not None
                    else None
                ),
                "control_receipts": self._state.sequence,
                "control_receipt_capacity": self._limits.max_control_receipts,
                "control_capacity_latched": self._state.control_capacity_latched,
                "retained_payload_entries": self._state.retained_payload_entries,
                "retained_payload_entry_capacity": self._limits.max_retained_payload_entries,
                "retained_payload_bytes": self._state.retained_payload_bytes,
                "retained_payload_byte_capacity": self._limits.max_retained_payload_bytes,
                "retention_mode": "append_only_no_reclamation",
                "directory_fsync_supported": self._directory_fsync_supported,
                "counters": counters,
                "promotion_gates": list(PROMOTION_GATES),
            }

    def close(self) -> None:
        # Serialize lock release with offers/claims.  Otherwise a concurrent
        # close could revoke process ownership halfway through a durable write.
        with self._operation_lock:
            if self._closed:
                return
            descriptor = self._lock_descriptor
            self._lock_descriptor = None
            try:
                if descriptor is not None:
                    try:
                        _unlock_descriptor(descriptor)
                    finally:
                        os.close(descriptor)
            finally:
                self._release_process_owner()
                self._closed = True

    def __enter__(self) -> "BlitzortungEventIngressAuthority":
        self._assert_owner()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


def blitzortung_event_ingress_candidate_contract() -> dict[str, object]:
    """Return the immutable public status without opening stores or sockets."""

    body: dict[str, object] = {
        "contract": CONTRACT,
        "ingress_id": INGRESS_ID,
        "delivery_model": "mqtt_push_event_stream",
        "qos": 0,
        "transport_completeness": TRANSPORT_COMPLETENESS,
        "absence_semantics": ABSENCE_SEMANTICS,
        "silence_interpretation": "unknown_not_an_absence_observation",
        "ownership_status": "ownership_blocked",
        "production_retention_authorized": False,
        "operational_admission": False,
        "network_wired": False,
        "worker_transport_available": True,
        "decoder_wired": False,
        "retention_mode": "append_only_no_reclamation",
        "promotion_gates": list(PROMOTION_GATES),
    }
    return {**body, "contract_sha256": _digest(body)}


__all__ = [
    "ACCESS_RETENTION_AUTHORITY_ID",
    "ABSENCE_SEMANTICS",
    "CONTRACT",
    "DIAGNOSTICS_SCHEMA",
    "INGRESS_ID",
    "PROMOTION_GATES",
    "PRODUCTION_AUTHORITY_SHA256",
    "SOURCE_TERMS_SHA256",
    "TRANSPORT_COMPLETENESS",
    "BlitzortungEventIngressAuthority",
    "BlitzortungEventIngressBackpressure",
    "BlitzortungEventIngressBlocked",
    "BlitzortungEventIngressBusy",
    "BlitzortungEventIngressCapacityError",
    "BlitzortungEventIngressConflict",
    "BlitzortungEventIngressError",
    "BlitzortungEventIngressIntegrityError",
    "BlitzortungEventIngressLimits",
    "BlitzortungEventIngressValidationError",
    "BlitzortungOfferResult",
    "BlitzortungQueueClaim",
    "BlitzortungRetentionDecision",
    "BlitzortungSessionHandle",
    "blitzortung_event_ingress_candidate_contract",
]
