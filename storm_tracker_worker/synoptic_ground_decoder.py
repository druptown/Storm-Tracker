"""Shadow-only decoder for exact Synoptic Time Series precipitation captures.

Only a complete, byte-verified schema-3 capture produced by
``synoptic_native`` is accepted.  The decoder emits append-only candidate
facts; it never decides wet/dry, coverage, ground confirmation, confidence or
operational admission and it never performs network I/O.

The temporal meanings in this module are deliberately narrow:

* ``precip_intervals_set_*`` is precipitation between two consecutive
  provider timestamps.  The first returned value has no reconstructable start
  boundary and is therefore emitted as ``unknown`` rather than as a number.
* ``precip_accumulated_set_*`` is a cumulative series within the request
  window.  It is retained as such and is never relabelled as an instantaneous
  rate or a fixed-hour accumulation.
* every sensor set remains independent; sets are never selected, merged or
  summed.

Synoptic data and QC are provisional.  Native flags are preserved as facts,
not interpreted as automatic invalidation.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
import time
from typing import Final, Literal

from . import synoptic_native as native
from .canonical_ground_evidence_store import (
    CanonicalGroundEvidenceStore,
    GroundCaptureMemberBinding,
    GroundMeasurementValue,
    GroundObservationFact,
    GroundObservationReceipt,
    GroundStationSite,
    ProviderQcFact,
    parse_ground_observation,
)
from .raw_artifacts import RawArtifactError, RawArtifactStore


SYNOPTIC_GROUND_DECODER_IDENTITY = "synoptic_timeseries_ground_evidence"
SYNOPTIC_GROUND_DECODER_VERSION = "synoptic-ground-evidence-shadow-v1"
SYNOPTIC_GROUND_ADMISSION_STATUS = "candidate_only_unadmitted"
SYNOPTIC_PROVISIONAL_STATUS = "provider_data_provisional"
SYNOPTIC_INTERVAL_SEMANTICS = "between_consecutive_provider_observations"
SYNOPTIC_ACCUMULATED_SEMANTICS = "request_window_cumulative_series"

MAX_RESPONSE_BYTES: Final[int] = native.MAX_RESPONSE_BYTES
MAX_JSON_NODES: Final[int] = native.MAX_JSON_NODES
MAX_JSON_DEPTH: Final[int] = native.MAX_JSON_DEPTH
MAX_OBJECT_MEMBERS: Final[int] = native.MAX_JSON_OBJECT_MEMBERS
MAX_ARRAY_ITEMS: Final[int] = native.MAX_JSON_ARRAY_ITEMS
MAX_STRING_CHARS: Final[int] = native.MAX_JSON_STRING_CHARS
MAX_KEY_CHARS: Final[int] = native.MAX_JSON_KEY_CHARS
MAX_STATIONS: Final[int] = native.MAX_STATIONS_PER_QUERY
MAX_SENSOR_SETS_PER_KIND: Final[int] = 32
MAX_SAMPLES_PER_STATION: Final[int] = 20_000
MAX_DECODE_FACTS: Final[int] = 20_000
MAX_STATION_RECORD_BYTES: Final[int] = 2 * 1024 * 1024
MAX_IDENTIFIER_CHARS: Final[int] = 128
MAX_QC_TESTS: Final[int] = 32
MAX_PROVIDER_QC_PER_FACT: Final[int] = 48
MAX_INLINE_QC_BYTES: Final[int] = 512
DEFAULT_DECODE_SECONDS: Final[float] = 30.0

_NUMBER_RE = re.compile(
    rb"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?"
)
_INTERVAL_FIELD_RE = re.compile(r"^precip_intervals_set_[A-Za-z0-9_]+$")
_ACCUMULATED_FIELD_RE = re.compile(
    r"^precip_accumulated_set_[A-Za-z0-9_]+$"
)
_UTC_TIMESTAMP_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:"
    r"[0-9]{2}(?:\.[0-9]{1,6})?Z$"
)
_FORBIDDEN_SECRET_KEYS = frozenset({
    "token",
    "access_token",
    "refresh_token",
    "authorization",
    "cookie",
    "set_cookie",
    "client_secret",
    "api_key",
    "application_key",
})


class SynopticGroundDecoderError(RuntimeError):
    """Base class for fail-closed Synoptic candidate decoding."""


class SynopticGroundDecoderContractError(SynopticGroundDecoderError):
    """Verified bytes do not satisfy the closed meteorological contract."""


class SynopticGroundDecoderDeadlineError(SynopticGroundDecoderError):
    """The single absolute monotonic deadline elapsed."""


class SynopticGroundDecoderCapacityError(SynopticGroundDecoderError):
    """A hard input, tree, QC or output bound would be exceeded."""


class SynopticGroundCaptureError(SynopticGroundDecoderError):
    """The requested complete schema-3 capture is unavailable or invalid."""


@dataclass(frozen=True, slots=True)
class SynopticGroundDecodeResult:
    """Payload-free receipts from one deterministic shadow replay."""

    capture_id: str
    station_count: int
    sensor_set_count: int
    interval_fact_count: int
    accumulated_fact_count: int
    fact_receipts: tuple[GroundObservationReceipt, ...]
    config_sha256: str

    @property
    def fact_count(self) -> int:
        return len(self.fact_receipts)

    @property
    def operational_admission_eligible(self) -> bool:
        return False

    @property
    def ground_confirmation_eligible(self) -> bool:
        return False

    def to_metadata(self) -> dict[str, object]:
        return {
            "decoder_identity": SYNOPTIC_GROUND_DECODER_IDENTITY,
            "decoder_version": SYNOPTIC_GROUND_DECODER_VERSION,
            "capture_id": self.capture_id,
            "station_count": self.station_count,
            "sensor_set_count": self.sensor_set_count,
            "interval_fact_count": self.interval_fact_count,
            "accumulated_fact_count": self.accumulated_fact_count,
            "fact_count": self.fact_count,
            "fact_ids": [item.fact_id for item in self.fact_receipts],
            "config_sha256": self.config_sha256,
            "interval_semantics": SYNOPTIC_INTERVAL_SEMANTICS,
            "accumulated_semantics": SYNOPTIC_ACCUMULATED_SEMANTICS,
            "provider_status": SYNOPTIC_PROVISIONAL_STATUS,
            "wet_claim": False,
            "dry_claim": False,
            "coverage_claim": False,
            "ground_confirmation_eligible": False,
            "operational_admission_eligible": False,
            "admission_status": SYNOPTIC_GROUND_ADMISSION_STATUS,
        }


@dataclass(frozen=True, slots=True)
class _JsonMember:
    key: str
    start: int
    end: int
    value: "_JsonNode"


@dataclass(frozen=True, slots=True)
class _JsonNode:
    kind: Literal["object", "array", "string", "number", "bool", "null"]
    start: int
    end: int
    scalar: str | bool | None = None
    members: tuple[_JsonMember, ...] = ()
    items: tuple["_JsonNode", ...] = ()

    def member_map(self) -> dict[str, _JsonMember]:
        if self.kind != "object":
            raise SynopticGroundDecoderContractError(
                "Synoptic JSON-node is geen object"
            )
        return {member.key: member for member in self.members}


class _ExactJsonParser:
    """Bounded JSON parser retaining exact UTF-8 spans and number lexemes."""

    def __init__(self, payload: bytes, deadline_at: float) -> None:
        if type(payload) is not bytes:
            raise SynopticGroundDecoderContractError(
                "Synoptic responsemember moet exacte bytes bevatten"
            )
        if not payload or len(payload) > MAX_RESPONSE_BYTES:
            raise SynopticGroundDecoderCapacityError(
                "Synoptic responsemember valt buiten de bytegrens"
            )
        self.payload = payload
        self.deadline_at = deadline_at
        self.position = 0
        self.nodes = 0

    def parse(self) -> _JsonNode:
        if self.payload.startswith(b"\xef\xbb\xbf"):
            raise SynopticGroundDecoderContractError(
                "Synoptic JSON mag geen UTF-8 BOM bevatten"
            )
        self._space()
        node = self._value(0)
        self._space()
        if self.position != len(self.payload):
            raise SynopticGroundDecoderContractError(
                "Synoptic JSON bevat trailing bytes"
            )
        return node

    def _checkpoint(self) -> None:
        _check_deadline(self.deadline_at)

    def _space(self) -> None:
        while (
            self.position < len(self.payload)
            and self.payload[self.position] in b" \t\r\n"
        ):
            self.position += 1

    def _value(self, depth: int) -> _JsonNode:
        self._checkpoint()
        if depth > MAX_JSON_DEPTH:
            raise SynopticGroundDecoderCapacityError(
                "Synoptic JSON overschrijdt de dieptegrens"
            )
        self.nodes += 1
        if self.nodes > MAX_JSON_NODES:
            raise SynopticGroundDecoderCapacityError(
                "Synoptic JSON overschrijdt de nodegrens"
            )
        if self.position >= len(self.payload):
            raise SynopticGroundDecoderContractError("Synoptic JSON is afgekapt")
        token = self.payload[self.position]
        if token == 0x7B:
            return self._object(depth)
        if token == 0x5B:
            return self._array(depth)
        if token == 0x22:
            start = self.position
            value = self._string(MAX_STRING_CHARS)
            return _JsonNode("string", start, self.position, scalar=value)
        if token == 0x74 and self.payload.startswith(b"true", self.position):
            return self._literal("bool", b"true", True)
        if token == 0x66 and self.payload.startswith(b"false", self.position):
            return self._literal("bool", b"false", False)
        if token == 0x6E and self.payload.startswith(b"null", self.position):
            return self._literal("null", b"null", None)
        return self._number()

    def _literal(
        self,
        kind: Literal["bool", "null"],
        token: bytes,
        scalar: bool | None,
    ) -> _JsonNode:
        start = self.position
        self.position += len(token)
        return _JsonNode(kind, start, self.position, scalar=scalar)

    def _number(self) -> _JsonNode:
        start = self.position
        match = _NUMBER_RE.match(self.payload, start)
        if match is None:
            raise SynopticGroundDecoderContractError(
                "Synoptic JSON bevat een ongeldige waarde"
            )
        end = match.end()
        if end < len(self.payload) and self.payload[end] not in b" \t\r\n,]}":
            raise SynopticGroundDecoderContractError(
                "Synoptic JSON bevat een ongeldig getal"
            )
        lexeme = self.payload[start:end].decode("ascii")
        if len(lexeme) > 128:
            raise SynopticGroundDecoderCapacityError(
                "Synoptic getallexeme overschrijdt de grens"
            )
        try:
            parsed = Decimal(lexeme)
        except InvalidOperation as exc:
            raise SynopticGroundDecoderContractError(
                "Synoptic getallexeme is niet exact decimaal"
            ) from exc
        if not parsed.is_finite() or parsed.copy_abs() > Decimal("1e100"):
            raise SynopticGroundDecoderContractError(
                "Synoptic getallexeme is niet eindig"
            )
        self.position = end
        return _JsonNode("number", start, end, scalar=lexeme)

    def _string(self, maximum_chars: int) -> str:
        start = self.position
        self.position += 1
        escaped = False
        while self.position < len(self.payload):
            byte = self.payload[self.position]
            if byte < 0x20:
                raise SynopticGroundDecoderContractError(
                    "Synoptic JSON-string bevat een controlteken"
                )
            if escaped:
                if byte == 0x75:
                    digits = self.payload[self.position + 1 : self.position + 5]
                    if len(digits) != 4 or any(
                        item not in b"0123456789abcdefABCDEF" for item in digits
                    ):
                        raise SynopticGroundDecoderContractError(
                            "Synoptic JSON bevat een ongeldige Unicode-escape"
                        )
                    self.position += 5
                elif byte in b'"\\/bfnrt':
                    self.position += 1
                else:
                    raise SynopticGroundDecoderContractError(
                        "Synoptic JSON bevat een ongeldige escape"
                    )
                escaped = False
                continue
            if byte == 0x5C:
                escaped = True
                self.position += 1
                continue
            if byte == 0x22:
                self.position += 1
                try:
                    decoded = json.loads(
                        self.payload[start : self.position].decode(
                            "utf-8", errors="strict"
                        )
                    )
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise SynopticGroundDecoderContractError(
                        "Synoptic JSON-string is ongeldig"
                    ) from exc
                if (
                    not isinstance(decoded, str)
                    or len(decoded) > maximum_chars
                    or any(
                        ord(character) < 32
                        or 0xD800 <= ord(character) <= 0xDFFF
                        for character in decoded
                    )
                ):
                    raise SynopticGroundDecoderCapacityError(
                        "Synoptic JSON-string valt buiten de tekengrens"
                    )
                return decoded
            self.position += 1
        raise SynopticGroundDecoderContractError(
            "Synoptic JSON-string is niet afgesloten"
        )

    def _object(self, depth: int) -> _JsonNode:
        start = self.position
        self.position += 1
        self._space()
        members: list[_JsonMember] = []
        seen: set[str] = set()
        if self.position < len(self.payload) and self.payload[self.position] == 0x7D:
            self.position += 1
            return _JsonNode("object", start, self.position)
        while True:
            self._checkpoint()
            if len(members) >= MAX_OBJECT_MEMBERS:
                raise SynopticGroundDecoderCapacityError(
                    "Synoptic JSON-object overschrijdt de membergrens"
                )
            if self.position >= len(self.payload) or self.payload[self.position] != 0x22:
                raise SynopticGroundDecoderContractError(
                    "Synoptic JSON-object mist een stringkey"
                )
            member_start = self.position
            key = self._string(MAX_KEY_CHARS)
            if key in seen:
                raise SynopticGroundDecoderContractError(
                    "Synoptic JSON bevat een dubbele objectkey"
                )
            seen.add(key)
            if key.strip().lower().replace("-", "_") in _FORBIDDEN_SECRET_KEYS:
                raise SynopticGroundDecoderContractError(
                    "Synoptic JSON bevat verboden authmetadata"
                )
            self._space()
            if self.position >= len(self.payload) or self.payload[self.position] != 0x3A:
                raise SynopticGroundDecoderContractError(
                    "Synoptic JSON-object mist een dubbelepunt"
                )
            self.position += 1
            self._space()
            value = self._value(depth + 1)
            members.append(
                _JsonMember(key=key, start=member_start, end=value.end, value=value)
            )
            self._space()
            if self.position >= len(self.payload):
                raise SynopticGroundDecoderContractError(
                    "Synoptic JSON-object is afgekapt"
                )
            separator = self.payload[self.position]
            self.position += 1
            if separator == 0x7D:
                break
            if separator != 0x2C:
                raise SynopticGroundDecoderContractError(
                    "Synoptic JSON-object heeft een ongeldige separator"
                )
            self._space()
        return _JsonNode("object", start, self.position, members=tuple(members))

    def _array(self, depth: int) -> _JsonNode:
        start = self.position
        self.position += 1
        self._space()
        items: list[_JsonNode] = []
        if self.position < len(self.payload) and self.payload[self.position] == 0x5D:
            self.position += 1
            return _JsonNode("array", start, self.position)
        while True:
            self._checkpoint()
            if len(items) >= MAX_ARRAY_ITEMS:
                raise SynopticGroundDecoderCapacityError(
                    "Synoptic JSON-array overschrijdt de itemgrens"
                )
            items.append(self._value(depth + 1))
            self._space()
            if self.position >= len(self.payload):
                raise SynopticGroundDecoderContractError(
                    "Synoptic JSON-array is afgekapt"
                )
            separator = self.payload[self.position]
            self.position += 1
            if separator == 0x5D:
                break
            if separator != 0x2C:
                raise SynopticGroundDecoderContractError(
                    "Synoptic JSON-array heeft een ongeldige separator"
                )
            self._space()
        return _JsonNode("array", start, self.position, items=tuple(items))


@dataclass(frozen=True, slots=True)
class _PlannedObservation:
    station: GroundStationSite
    measurement: GroundMeasurementValue
    observed_at: float
    interval_start: float
    interval_end: float
    record_locator: str
    record_start: int
    record_end: int
    provider_qc: tuple[ProviderQcFact, ...]
    kind: Literal["interval", "accumulated"]


def _check_deadline(deadline_at: float) -> None:
    if time.monotonic() >= deadline_at:
        raise SynopticGroundDecoderDeadlineError(
            "Synoptic ground-decode overschrijdt de absolute deadline"
        )


def _deadline(value: float | None) -> float:
    if value is None:
        return time.monotonic() + DEFAULT_DECODE_SECONDS
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise ValueError("deadline_at is ongeldig")
    result = float(value)
    _check_deadline(result)
    return result


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _decoder_config_sha256() -> str:
    contract = {
        "decoder_identity": SYNOPTIC_GROUND_DECODER_IDENTITY,
        "decoder_version": SYNOPTIC_GROUND_DECODER_VERSION,
        "accepted_capture_product": native.SYNOPTIC_CAPTURE_PRODUCT,
        "accepted_member_product": native.SYNOPTIC_JSON_PRODUCT,
        "accepted_member_role": "response_json",
        "interval_semantics": SYNOPTIC_INTERVAL_SEMANTICS,
        "first_interval_policy": "unknown_without_explicit_previous_timestamp",
        "accumulated_semantics": SYNOPTIC_ACCUMULATED_SEMANTICS,
        "accumulated_time_support": "record_anchor_request_start_unbound",
        "sensor_set_policy": "preserve_every_exact_set_independently",
        "qc_policy": "preserve_exact_never_automatic_invalidation",
        "provider_status": SYNOPTIC_PROVISIONAL_STATUS,
        "null_policy": "explicit_missing",
        "false_or_empty_policy": "fail_closed_never_zero_or_dry",
        "station_elevation_policy": "unknown_no_feet_to_metres_derivation",
        "bounds": {
            "max_response_bytes": MAX_RESPONSE_BYTES,
            "max_json_nodes": MAX_JSON_NODES,
            "max_json_depth": MAX_JSON_DEPTH,
            "max_stations": MAX_STATIONS,
            "max_sensor_sets_per_kind": MAX_SENSOR_SETS_PER_KIND,
            "max_samples_per_station": MAX_SAMPLES_PER_STATION,
            "max_decode_facts": MAX_DECODE_FACTS,
            "max_station_record_bytes": MAX_STATION_RECORD_BYTES,
            "max_qc_tests": MAX_QC_TESTS,
            "max_provider_qc_per_fact": MAX_PROVIDER_QC_PER_FACT,
        },
        "wet_claim": False,
        "dry_claim": False,
        "ground_confirmation_eligible": False,
        "operational_admission_eligible": False,
    }
    return hashlib.sha256(_canonical_json(contract)).hexdigest()


def _string_node(node: _JsonNode, label: str, maximum: int) -> str:
    if node.kind != "string" or not isinstance(node.scalar, str):
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} is geen string"
        )
    value = node.scalar
    if (
        not value
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} valt buiten het identiteitscontract"
        )
    return value


def _number_lexeme(node: _JsonNode, label: str) -> tuple[str, Decimal]:
    if node.kind != "number" or not isinstance(node.scalar, str):
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} is geen exact JSON-getal"
        )
    try:
        parsed = Decimal(node.scalar)
    except InvalidOperation as exc:  # parser already checks this
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} is geen exact decimaal"
        ) from exc
    if not parsed.is_finite():
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} is niet eindig"
        )
    return node.scalar, parsed


def _decimal_text_node(
    node: _JsonNode,
    label: str,
    *,
    minimum: str | None = None,
    maximum: str | None = None,
) -> str:
    if node.kind == "number":
        text, parsed = _number_lexeme(node, label)
    elif node.kind == "string" and isinstance(node.scalar, str):
        text = node.scalar
        if not text or text != text.strip() or len(text) > 128:
            raise SynopticGroundDecoderContractError(
                f"Synoptic {label} is geen exact decimaal"
            )
        try:
            parsed = Decimal(text)
        except InvalidOperation as exc:
            raise SynopticGroundDecoderContractError(
                f"Synoptic {label} is geen exact decimaal"
            ) from exc
    else:
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} heeft een onbekend numeriek type"
        )
    if not parsed.is_finite():
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} is niet eindig"
        )
    if minimum is not None and parsed < Decimal(minimum):
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} valt buiten bereik"
        )
    if maximum is not None and parsed > Decimal(maximum):
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} valt buiten bereik"
        )
    return text


def _integer_node(node: _JsonNode, label: str) -> int:
    lexeme, parsed = _number_lexeme(node, label)
    if parsed != parsed.to_integral_value() or "." in lexeme or "e" in lexeme.lower():
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} is geen exact JSON-geheel getal"
        )
    try:
        return int(lexeme)
    except ValueError as exc:  # defensive
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} is geen geheel getal"
        ) from exc


def _timestamp_node(node: _JsonNode, label: str) -> tuple[float, str]:
    token = _string_node(node, label, 128)
    if _UTC_TIMESTAMP_RE.fullmatch(token) is None:
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} is niet de verwachte expliciete UTC ISO-8601-tijd"
        )
    try:
        value = datetime.fromisoformat(token[:-1] + "+00:00")
    except ValueError as exc:
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} is geen geldige kalenderdatum"
        ) from exc
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} is niet expliciet UTC"
        )
    result = value.timestamp()
    if not math.isfinite(result) or result < 0.0 or result > 253402300799.0:
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} valt buiten het tijdsbereik"
        )
    return result, token


def _node_token(payload: bytes, node: _JsonNode, label: str) -> str:
    raw = payload[node.start : node.end]
    if not raw:
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} heeft geen exact brontoken"
        )
    # Canonical ground QC strings reject control characters.  Remove only
    # insignificant JSON whitespace outside string literals; byte identity is
    # still protected by the station-record and capture hashes.
    compact = bytearray()
    in_string = False
    escaped = False
    for byte in raw:
        if in_string:
            compact.append(byte)
            if escaped:
                escaped = False
            elif byte == 0x5C:
                escaped = True
            elif byte == 0x22:
                in_string = False
        elif byte == 0x22:
            compact.append(byte)
            in_string = True
        elif byte not in b" \t\r\n":
            compact.append(byte)
    if in_string or escaped:
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} heeft een afgekapt JSON-token"
        )
    if not compact or len(compact) > MAX_INLINE_QC_BYTES:
        raise SynopticGroundDecoderCapacityError(
            f"Synoptic {label} overschrijdt de inline-QC-grens"
        )
    try:
        return bytes(compact).decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} is geen exacte UTF-8 JSON"
        ) from exc


def _json_pointer_token(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _measurement(
    node: _JsonNode,
    *,
    variable: str,
    unit: str,
) -> GroundMeasurementValue:
    if node.kind == "null":
        return GroundMeasurementValue(
            variable=variable,
            state="missing",
            representation="none",
            exact_value=None,
            unit=unit,
            censor_limit=None,
        )
    if node.kind != "number":
        # qc_remove_data=off means false is not a valid precipitation value;
        # never coerce false, an empty string or another placeholder to zero.
        raise SynopticGroundDecoderContractError(
            "Synoptic neerslagwaarde is geen getal of expliciete null"
        )
    lexeme, _parsed = _number_lexeme(node, variable)
    return GroundMeasurementValue(
        variable=variable,
        state="observed",
        representation="decimal",
        exact_value=lexeme,
        unit=unit,
        censor_limit=None,
    )


def _unknown_measurement(*, variable: str, unit: str) -> GroundMeasurementValue:
    return GroundMeasurementValue(
        variable=variable,
        state="unknown",
        representation="none",
        exact_value=None,
        unit=unit,
        censor_limit=None,
    )


def _variable_name(field: str, kind: Literal["interval", "accumulated"]) -> str:
    suffix = (
        "between_consecutive_reports"
        if kind == "interval"
        else "request_window_cumulative"
    )
    result = f"synoptic.native.{field}.{suffix}"
    if len(result) > 128:
        raise SynopticGroundDecoderCapacityError(
            "Synoptic sensorset maakt de canonieke variabelenaam te lang"
        )
    return result


def _station_site(station_node: _JsonNode) -> tuple[GroundStationSite, str | None]:
    members = station_node.member_map()
    stid_member = members.get("STID")
    latitude_member = members.get("LATITUDE")
    longitude_member = members.get("LONGITUDE")
    if stid_member is None or latitude_member is None or longitude_member is None:
        raise SynopticGroundDecoderContractError(
            "Synoptic station mist STID of exacte positie"
        )
    stid = _string_node(stid_member.value, "STID", 64)
    latitude = _decimal_text_node(
        latitude_member.value, "LATITUDE", minimum="-90", maximum="90"
    )
    longitude = _decimal_text_node(
        longitude_member.value, "LONGITUDE", minimum="-180", maximum="180"
    )
    network_member = members.get("MNET_ID")
    if network_member is None:
        network_id = "unspecified"
    elif network_member.value.kind == "string":
        network_id = _string_node(network_member.value, "MNET_ID", 96)
    elif network_member.value.kind == "number":
        network_id, _ = _number_lexeme(network_member.value, "MNET_ID")
    else:
        raise SynopticGroundDecoderContractError(
            "Synoptic MNET_ID heeft een onbekend type"
        )
    network = f"synoptic-mnet:{network_id}"
    if len(network) > 128:
        raise SynopticGroundDecoderCapacityError(
            "Synoptic netwerkidentiteit overschrijdt de grens"
        )
    name_member = members.get("NAME")
    station_name = (
        None
        if name_member is None
        else _string_node(name_member.value, "stationsnaam", 512)
    )
    elevation_member = members.get("ELEVATION")
    elevation_ft = (
        None
        if elevation_member is None
        else _decimal_text_node(elevation_member.value, "ELEVATION")
    )
    return (
        GroundStationSite(
            provider_station_id=stid,
            site_id=f"synoptic:{stid}",
            network=network,
            station_name=station_name,
            position_state="known",
            latitude_decimal=latitude,
            longitude_decimal=longitude,
            # The provider documents this field in feet.  The canonical site
            # contract requires metres; no silent conversion is made here.
            elevation_state="unknown",
            elevation_m_decimal=None,
        ),
        elevation_ft,
    )


def _qc_summary(
    root_members: dict[str, _JsonMember],
) -> tuple[tuple[str, ...], _JsonNode]:
    summary_member = root_members.get("QC_SUMMARY")
    if summary_member is None or summary_member.value.kind != "object":
        raise SynopticGroundDecoderContractError(
            "Synoptic response mist QC_SUMMARY"
        )
    members = summary_member.value.member_map()
    tests_member = members.get("QC_TESTS_APPLIED", members.get("QC_CHECKS_APPLIED"))
    total_member = members.get("TOTAL_OBSERVATIONS_FLAGGED")
    if (
        tests_member is None
        or tests_member.value.kind != "array"
        or total_member is None
    ):
        raise SynopticGroundDecoderContractError(
            "Synoptic QC_SUMMARY mist tests of totaalteller"
        )
    if len(tests_member.value.items) > MAX_QC_TESTS:
        raise SynopticGroundDecoderCapacityError(
            "Synoptic QC-testlijst overschrijdt de feitengrens"
        )
    tests = tuple(
        _string_node(item, "QC-test", 128) for item in tests_member.value.items
    )
    total = _integer_node(total_member.value, "TOTAL_OBSERVATIONS_FLAGGED")
    if total < 0:
        raise SynopticGroundDecoderContractError(
            "Synoptic QC-totaalteller is negatief"
        )
    return tests, total_member.value


def _qc_element(
    container: _JsonNode,
    *,
    field: str,
    index: int,
    sample_count: int,
) -> _JsonNode | None:
    if container.kind == "bool":
        return container
    if container.kind != "object":
        raise SynopticGroundDecoderContractError(
            "Synoptic QC-container heeft een onbekende vorm"
        )
    members = container.member_map()
    member = members.get(field, members.get(f"{field}_qc"))
    if member is None:
        return None
    node = member.value
    if node.kind == "array":
        if len(node.items) != sample_count:
            raise SynopticGroundDecoderContractError(
                "Synoptic QC-array volgt de provider-tijdas niet exact"
            )
        return node.items[index]
    if node.kind in {"bool", "null", "object"}:
        return node
    raise SynopticGroundDecoderContractError(
        "Synoptic QC-veld heeft een onbekende vorm"
    )


def _provider_qc(
    *,
    payload: bytes,
    station_members: dict[str, _JsonMember],
    observation_members: dict[str, _JsonMember],
    field: str,
    index: int,
    sample_count: int,
    sensor_metadata: _JsonNode,
    applied_tests: tuple[str, ...],
    total_flagged_node: _JsonNode,
    kind: Literal["interval", "accumulated"],
    source_value: _JsonNode,
    interval_start_unknown: bool,
    elevation_ft: str | None,
) -> tuple[ProviderQcFact, ...]:
    result: list[ProviderQcFact] = [
        ProviderQcFact(
            flag="synoptic.provider.provisional",
            value="true",
            text=(
                "Synoptic data and services are provisional; this flag is "
                "not an automatic invalidation"
            ),
        ),
        ProviderQcFact(
            flag="synoptic.sensor_set",
            value=field,
            text="exact independent provider sensor-set identity",
        ),
        ProviderQcFact(
            flag="synoptic.sensor_metadata",
            value=_node_token(payload, sensor_metadata, "sensormetadata"),
            text="exact native SENSOR_VARIABLES metadata; not merged or selected",
        ),
        ProviderQcFact(
            flag="synoptic.qc.total_observations_flagged",
            value=_node_token(payload, total_flagged_node, "QC-totaalteller"),
            text="response-level provider counter; not a validity decision",
        ),
    ]
    for test_name in applied_tests:
        result.append(
            ProviderQcFact(
                flag="synoptic.qc.test_applied",
                value=test_name,
                text="provider test name preserved without validity inference",
            )
        )
    if elevation_ft is not None:
        result.append(
            ProviderQcFact(
                flag="synoptic.station.elevation_ft",
                value=elevation_ft,
                text="provider-native feet; not converted into canonical metres",
            )
        )

    qc_flagged_member = station_members.get("QC_FLAGGED")
    if qc_flagged_member is None:
        raise SynopticGroundDecoderContractError(
            "Synoptic station mist QC_FLAGGED"
        )
    flagged = _qc_element(
        qc_flagged_member.value,
        field=field,
        index=index,
        sample_count=sample_count,
    )
    if flagged is None:
        raise SynopticGroundDecoderContractError(
            "Synoptic QC_FLAGGED mist de exacte neerslagsensorset"
        )
    result.append(
        ProviderQcFact(
            flag="synoptic.qc_flagged",
            value=_node_token(payload, flagged, "QC_FLAGGED"),
            text="exact native flag; true does not automatically invalidate data",
        )
    )

    qc_member = station_members.get("QC")
    if qc_member is not None:
        native_qc = _qc_element(
            qc_member.value,
            field=field,
            index=index,
            sample_count=sample_count,
        )
        if native_qc is not None:
            result.append(
                ProviderQcFact(
                    flag="synoptic.native_qc",
                    value=_node_token(payload, native_qc, "native QC"),
                    text="exact provider QC payload; no automatic invalidation",
                )
            )
    observation_qc_member = observation_members.get(f"{field}_qc")
    if observation_qc_member is not None:
        node = observation_qc_member.value
        if node.kind != "array" or len(node.items) != sample_count:
            raise SynopticGroundDecoderContractError(
                "Synoptic observation-QC volgt de provider-tijdas niet exact"
            )
        result.append(
            ProviderQcFact(
                flag="synoptic.observation_qc",
                value=_node_token(payload, node.items[index], "observation QC"),
                text="exact provider QC payload; no automatic invalidation",
            )
        )

    if kind == "interval":
        result.append(
            ProviderQcFact(
                flag="synoptic.decoder.temporal_semantics",
                value=SYNOPTIC_INTERVAL_SEMANTICS,
                text="never an instantaneous rate or universal fixed-hour total",
            )
        )
        if interval_start_unknown:
            result.append(
                ProviderQcFact(
                    flag="synoptic.decoder.interval_start_unknown",
                    value=_node_token(payload, source_value, "eerste intervalwaarde"),
                    text=(
                        "provider value preserved here only; measurement is unknown "
                        "because no explicit previous timestamp exists"
                    ),
                )
            )
    else:
        result.append(
            ProviderQcFact(
                flag="synoptic.decoder.temporal_semantics",
                value=SYNOPTIC_ACCUMULATED_SEMANTICS,
                text="never an instantaneous rate or independent fixed-hour total",
            )
        )
        result.append(
            ProviderQcFact(
                flag="synoptic.decoder.request_window_start_unbound",
                value=None,
                text=(
                    "start=end is only the observation anchor; the cumulative "
                    "request-window start is not reconstructed"
                ),
            )
        )
    if len(result) > MAX_PROVIDER_QC_PER_FACT:
        raise SynopticGroundDecoderCapacityError(
            "Synoptic provider-QC overschrijdt de canonieke feitengrens"
        )
    return tuple(result)


def _require_object_member(
    members: dict[str, _JsonMember],
    key: str,
    label: str,
) -> _JsonNode:
    member = members.get(key)
    if member is None or member.value.kind != "object":
        raise SynopticGroundDecoderContractError(
            f"Synoptic {label} mist object {key}"
        )
    return member.value


def _append_plan(
    plans: list[_PlannedObservation],
    plan: _PlannedObservation,
) -> None:
    if len(plans) >= MAX_DECODE_FACTS:
        raise SynopticGroundDecoderCapacityError(
            "Synoptic decode overschrijdt de feitengrens"
        )
    plans.append(plan)


def _build_plans(
    root: _JsonNode,
    *,
    payload: bytes,
    deadline_at: float,
) -> tuple[list[_PlannedObservation], int, int, int, int]:
    if root.kind != "object":
        raise SynopticGroundDecoderContractError(
            "Synoptic response-root is geen object"
        )
    root_members = root.member_map()
    units_node = _require_object_member(root_members, "UNITS", "response")
    units = units_node.member_map()
    interval_unit_member = units.get("precip_intervals")
    accumulated_unit_member = units.get("precip_accumulated")
    if interval_unit_member is None or accumulated_unit_member is None:
        raise SynopticGroundDecoderContractError(
            "Synoptic UNITS mist afgeleide neerslageenheden"
        )
    interval_unit = _string_node(
        interval_unit_member.value, "precip_intervals-eenheid", 128
    )
    accumulated_unit = _string_node(
        accumulated_unit_member.value, "precip_accumulated-eenheid", 128
    )
    applied_tests, total_flagged_node = _qc_summary(root_members)

    summary_node = _require_object_member(root_members, "SUMMARY", "response")
    summary = summary_node.member_map()
    response_code_member = summary.get("RESPONSE_CODE")
    object_count_member = summary.get("NUMBER_OF_OBJECTS")
    if response_code_member is None or object_count_member is None:
        raise SynopticGroundDecoderContractError(
            "Synoptic SUMMARY mist succescode of objectteller"
        )
    if _integer_node(response_code_member.value, "RESPONSE_CODE") != 1:
        raise SynopticGroundDecoderContractError(
            "Synoptic capture is geen succesvolle stationsresponse"
        )
    object_count = _integer_node(
        object_count_member.value, "NUMBER_OF_OBJECTS"
    )
    stations_member = root_members.get("STATION")
    if stations_member is None or stations_member.value.kind != "array":
        raise SynopticGroundDecoderContractError(
            "Synoptic succesresponse mist STATION-array"
        )
    stations = stations_member.value.items
    if (
        object_count != len(stations)
        or not 1 <= len(stations) <= MAX_STATIONS
    ):
        raise SynopticGroundDecoderContractError(
            "Synoptic STATION-array en SUMMARY komen niet exact overeen"
        )

    plans: list[_PlannedObservation] = []
    station_ids: set[str] = set()
    sensor_set_count = 0
    interval_fact_count = 0
    accumulated_fact_count = 0
    for station_index, station_node in enumerate(stations):
        _check_deadline(deadline_at)
        if station_node.kind != "object":
            raise SynopticGroundDecoderContractError(
                "Synoptic STATION-array bevat geen object"
            )
        record_size = station_node.end - station_node.start
        if not 1 <= record_size <= MAX_STATION_RECORD_BYTES:
            raise SynopticGroundDecoderCapacityError(
                "Synoptic stationrecord valt buiten de bytegrens"
            )
        station_members = station_node.member_map()
        station, elevation_ft = _station_site(station_node)
        if station.provider_station_id in station_ids:
            raise SynopticGroundDecoderContractError(
                "Synoptic response bevat een dubbel STID"
            )
        station_ids.add(station.provider_station_id)

        observations_node = _require_object_member(
            station_members, "OBSERVATIONS", "station"
        )
        observation_members = observations_node.member_map()
        date_member = observation_members.get("date_time")
        if date_member is None or date_member.value.kind != "array":
            raise SynopticGroundDecoderContractError(
                "Synoptic OBSERVATIONS mist de provider-tijdas"
            )
        if not 1 <= len(date_member.value.items) <= MAX_SAMPLES_PER_STATION:
            raise SynopticGroundDecoderCapacityError(
                "Synoptic provider-tijdas valt buiten de samplegrens"
            )
        timestamps: list[float] = []
        for time_index, time_node in enumerate(date_member.value.items):
            observed_at, _token = _timestamp_node(
                time_node, f"date_time[{time_index}]"
            )
            if timestamps and observed_at <= timestamps[-1]:
                raise SynopticGroundDecoderContractError(
                    "Synoptic provider-tijdas is niet strikt oplopend"
                )
            timestamps.append(observed_at)
        sample_count = len(timestamps)

        interval_fields = sorted(
            key
            for key in observation_members
            if _INTERVAL_FIELD_RE.fullmatch(key)
        )
        accumulated_fields = sorted(
            key
            for key in observation_members
            if _ACCUMULATED_FIELD_RE.fullmatch(key)
        )
        if not interval_fields or not accumulated_fields:
            raise SynopticGroundDecoderContractError(
                "Synoptic station mist interval- of cumulatieve sensorsets"
            )
        if (
            len(interval_fields) > MAX_SENSOR_SETS_PER_KIND
            or len(accumulated_fields) > MAX_SENSOR_SETS_PER_KIND
        ):
            raise SynopticGroundDecoderCapacityError(
                "Synoptic station overschrijdt de sensorsetgrens"
            )
        if any(len(field) > MAX_IDENTIFIER_CHARS for field in (
            *interval_fields,
            *accumulated_fields,
        )):
            raise SynopticGroundDecoderCapacityError(
                "Synoptic sensorsetidentiteit overschrijdt de grens"
            )

        sensor_variables_node = _require_object_member(
            station_members, "SENSOR_VARIABLES", "station"
        )
        sensor_variables = sensor_variables_node.member_map()
        interval_metadata_node = _require_object_member(
            sensor_variables, "precip_intervals", "SENSOR_VARIABLES"
        )
        accumulated_metadata_node = _require_object_member(
            sensor_variables, "precip_accumulated", "SENSOR_VARIABLES"
        )
        interval_metadata = interval_metadata_node.member_map()
        accumulated_metadata = accumulated_metadata_node.member_map()
        if set(interval_metadata) != set(interval_fields):
            raise SynopticGroundDecoderContractError(
                "Synoptic interval-SENSOR_VARIABLES en OBSERVATIONS verschillen"
            )
        if set(accumulated_metadata) != set(accumulated_fields):
            raise SynopticGroundDecoderContractError(
                "Synoptic accumulatie-SENSOR_VARIABLES en OBSERVATIONS verschillen"
            )

        qc_flagged_member = station_members.get("QC_FLAGGED")
        if qc_flagged_member is None:
            raise SynopticGroundDecoderContractError(
                "Synoptic station mist QC_FLAGGED"
            )
        if qc_flagged_member.value.kind == "object":
            qc_fields = qc_flagged_member.value.member_map()
            if not set((*interval_fields, *accumulated_fields)).issubset(qc_fields):
                raise SynopticGroundDecoderContractError(
                    "Synoptic QC_FLAGGED mist een neerslagsensorset"
                )
        elif qc_flagged_member.value.kind != "bool":
            raise SynopticGroundDecoderContractError(
                "Synoptic QC_FLAGGED heeft een onbekende vorm"
            )

        sensor_set_count += len(interval_fields) + len(accumulated_fields)
        for field in interval_fields:
            _check_deadline(deadline_at)
            values = observation_members[field].value
            metadata = interval_metadata[field].value
            if metadata.kind != "object":
                raise SynopticGroundDecoderContractError(
                    "Synoptic intervalsensorset mist exact metadata-object"
                )
            if values.kind != "array" or len(values.items) != sample_count:
                raise SynopticGroundDecoderContractError(
                    "Synoptic intervalwaarden volgen de tijdas niet exact"
                )
            variable = _variable_name(field, "interval")
            for index, (observed_at, value_node) in enumerate(
                zip(timestamps, values.items, strict=True)
            ):
                _check_deadline(deadline_at)
                start_unknown = index == 0
                measurement = (
                    _unknown_measurement(variable=variable, unit=interval_unit)
                    if start_unknown
                    else _measurement(
                        value_node, variable=variable, unit=interval_unit
                    )
                )
                _append_plan(
                    plans,
                    _PlannedObservation(
                        station=station,
                        measurement=measurement,
                        observed_at=observed_at,
                        interval_start=(
                            observed_at if start_unknown else timestamps[index - 1]
                        ),
                        interval_end=observed_at,
                        record_locator=(
                            f"/STATION/{station_index}/OBSERVATIONS/"
                            f"{_json_pointer_token(field)}/{index}"
                        ),
                        record_start=station_node.start,
                        record_end=station_node.end,
                        provider_qc=_provider_qc(
                            payload=payload,
                            station_members=station_members,
                            observation_members=observation_members,
                            field=field,
                            index=index,
                            sample_count=sample_count,
                            sensor_metadata=metadata,
                            applied_tests=applied_tests,
                            total_flagged_node=total_flagged_node,
                            kind="interval",
                            source_value=value_node,
                            interval_start_unknown=start_unknown,
                            elevation_ft=elevation_ft,
                        ),
                        kind="interval",
                    ),
                )
                interval_fact_count += 1

        for field in accumulated_fields:
            _check_deadline(deadline_at)
            values = observation_members[field].value
            metadata = accumulated_metadata[field].value
            if metadata.kind != "object":
                raise SynopticGroundDecoderContractError(
                    "Synoptic accumulatiesensorset mist exact metadata-object"
                )
            if values.kind != "array" or len(values.items) != sample_count:
                raise SynopticGroundDecoderContractError(
                    "Synoptic cumulatieve waarden volgen de tijdas niet exact"
                )
            variable = _variable_name(field, "accumulated")
            for index, (observed_at, value_node) in enumerate(
                zip(timestamps, values.items, strict=True)
            ):
                _check_deadline(deadline_at)
                _append_plan(
                    plans,
                    _PlannedObservation(
                        station=station,
                        measurement=_measurement(
                            value_node, variable=variable, unit=accumulated_unit
                        ),
                        observed_at=observed_at,
                        # The request used recent=60.  That does not expose an
                        # exact request-start timestamp, so this is only an
                        # observation anchor and is labelled as such in QC.
                        interval_start=observed_at,
                        interval_end=observed_at,
                        record_locator=(
                            f"/STATION/{station_index}/OBSERVATIONS/"
                            f"{_json_pointer_token(field)}/{index}"
                        ),
                        record_start=station_node.start,
                        record_end=station_node.end,
                        provider_qc=_provider_qc(
                            payload=payload,
                            station_members=station_members,
                            observation_members=observation_members,
                            field=field,
                            index=index,
                            sample_count=sample_count,
                            sensor_metadata=metadata,
                            applied_tests=applied_tests,
                            total_flagged_node=total_flagged_node,
                            kind="accumulated",
                            source_value=value_node,
                            interval_start_unknown=False,
                            elevation_ft=elevation_ft,
                        ),
                        kind="accumulated",
                    ),
                )
                accumulated_fact_count += 1

    return (
        plans,
        len(station_ids),
        sensor_set_count,
        interval_fact_count,
        accumulated_fact_count,
    )


def _validate_query_context(context: object) -> dict[str, object]:
    if not isinstance(context, dict):
        raise SynopticGroundCaptureError(
            "Synoptic querycontext is geen object"
        )
    expected_keys = {
        "schema",
        "method",
        "origin",
        "path",
        "query_identity",
        "query_kind",
        "station_selection",
        "information_preservation",
        "parameters",
    }
    query_identity = context.get("query_identity")
    if (
        set(context) != expected_keys
        or context.get("schema") != native.SYNOPTIC_QUERY_CONTEXT_SCHEMA
        or context.get("method") != "GET"
        or context.get("origin") != native.SYNOPTIC_ORIGIN
        or context.get("path") != native.SYNOPTIC_TIMESERIES_PATH
        or not isinstance(query_identity, str)
        or not query_identity
        or len(query_identity) > 256
        or "token" in query_identity.casefold()
        or context.get("query_kind") not in {"local", "roaming"}
        or context.get("station_selection")
        != {
            "mode": "precipitation_variables_only",
            "provider_derived_precipitation_intervals_requested": True,
            "exact_station_limit": native.MAX_STATIONS_PER_QUERY,
            "coverage_status": native.SYNOPTIC_COVERAGE_STATUS,
        }
        or context.get("information_preservation")
        != {
            "all_reports": True,
            "sensor_metadata": True,
            "provider_qc_values_removed": False,
            "provider_qc_flags_requested": True,
            "provider_qc_sources": ["synopticlabs", "madis", "mesowest"],
        }
    ):
        raise SynopticGroundCaptureError(
            "Synoptic querycontext wijkt af van het volledige native contract"
        )
    parameters = context.get("parameters")
    if not isinstance(parameters, list):
        raise SynopticGroundCaptureError(
            "Synoptic querycontext mist de exacte parameterlijst"
        )
    names = [
        "radius",
        "vars",
        "precip",
        "all_reports",
        "sensorvars",
        "qc",
        "qc_remove_data",
        "qc_flags",
        "qc_checks",
        "units",
        "output",
        "recent",
        "limit",
    ]
    if len(parameters) != len(names):
        raise SynopticGroundCaptureError(
            "Synoptic querycontext heeft een andere parameteromvang"
        )
    values: dict[str, str] = {}
    for expected_name, item in zip(names, parameters, strict=True):
        if (
            not isinstance(item, dict)
            or set(item) != {"name", "value"}
            or item.get("name") != expected_name
            or not isinstance(item.get("value"), str)
            or not item["value"]
            or len(item["value"]) > 256
        ):
            raise SynopticGroundCaptureError(
                "Synoptic querycontext heeft geen exacte parameterbinding"
            )
        values[expected_name] = item["value"]
    fixed_values = {
        "vars": "precip",
        "precip": "1",
        "all_reports": "1",
        "sensorvars": "1",
        "qc": "on",
        "qc_remove_data": "off",
        "qc_flags": "on",
        "qc_checks": "synopticlabs,madis,mesowest",
        "units": "metric,temp|C,speed|kph,pres|mb,precip|mm",
        "output": "json",
        "recent": str(native.RECENT_MINUTES),
        "limit": str(native.MAX_STATIONS_PER_QUERY),
    }
    if any(values[name] != expected for name, expected in fixed_values.items()):
        raise SynopticGroundCaptureError(
            "Synoptic querycontext mist informatiebehoud of neerslagsemantiek"
        )
    radius_parts = values["radius"].split(",")
    if len(radius_parts) != 3:
        raise SynopticGroundCaptureError(
            "Synoptic radiusparameter is niet exact gebonden"
        )
    try:
        latitude, longitude, radius_miles = map(Decimal, radius_parts)
    except InvalidOperation as exc:
        raise SynopticGroundCaptureError(
            "Synoptic radiusparameter is niet numeriek"
        ) from exc
    if (
        not all(item.is_finite() for item in (latitude, longitude, radius_miles))
        or not Decimal("-90") <= latitude <= Decimal("90")
        or not Decimal("-180") <= longitude <= Decimal("180")
        or radius_miles <= 0
    ):
        raise SynopticGroundCaptureError(
            "Synoptic radiusparameter valt buiten bereik"
        )
    return context


def _validate_capture_bundle(capture, receipts, payloads) -> tuple[object, bytes]:
    if (
        capture.provider != "synoptic"
        or capture.product != native.SYNOPTIC_CAPTURE_PRODUCT
        or capture.product_timestamp is not None
        or not capture.complete
        or set(receipts) != {"response_json"}
        or set(payloads) != {"response_json"}
        or len(capture.members) != 1
        or capture.members[0].role != "response_json"
    ):
        raise SynopticGroundCaptureError(
            "Synoptic capture heeft niet het exacte complete responsecontract"
        )
    receipt = receipts["response_json"]
    payload = payloads["response_json"]
    if (
        receipt.provider != "synoptic"
        or receipt.product != native.SYNOPTIC_JSON_PRODUCT
        or receipt.content_type.split(";", 1)[0].strip().lower()
        != "application/json"
        or receipt.size_bytes != len(payload)
        or receipt.content_sha256 != hashlib.sha256(payload).hexdigest()
        or not capture.fetch_id.endswith(":capture")
        or ":response_json:attempt:" not in receipt.fetch_id
    ):
        raise SynopticGroundCaptureError(
            "Synoptic responsemember heeft een verkeerde identiteit"
        )
    job_id = capture.fetch_id[: -len(":capture")]
    attempt_prefix = f"{job_id}:response_json:attempt:"
    if not receipt.fetch_id.startswith(attempt_prefix):
        raise SynopticGroundCaptureError(
            "Synoptic capture en responsemember hebben een andere jobbinding"
        )
    provenance = receipt.provenance
    context = _validate_query_context(
        provenance.get("query_context") if isinstance(provenance, dict) else None
    )
    request_attempt = provenance.get("request_attempt")
    if (
        isinstance(request_attempt, bool)
        or not isinstance(request_attempt, int)
        or not 1 <= request_attempt <= native.MAX_REQUEST_ATTEMPTS_PER_JOB
        or receipt.fetch_id != f"{attempt_prefix}{request_attempt}"
        or provenance != native._native_provenance(
            context, request_attempt=request_attempt
        )
    ):
        raise SynopticGroundCaptureError(
            "Synoptic responsemember mist de exacte native provenance"
        )
    query_sha = native._query_context_sha256(context)
    expected_receipt_upstream = (
        "synoptic/stations/timeseries"
        f"|query_sha256={query_sha}"
        f"|request_attempt={request_attempt}"
        f"|content_sha256={receipt.content_sha256}"
    )
    expected_capture_upstream = (
        f"{native.SYNOPTIC_CAPTURE_PRODUCT}"
        f"|query_sha256={query_sha}"
        f"|content_sha256={receipt.content_sha256}"
    )
    if (
        receipt.upstream_id != expected_receipt_upstream
        or capture.upstream_capture_id != expected_capture_upstream
    ):
        raise SynopticGroundCaptureError(
            "Synoptic capture is niet aan query en exacte response gebonden"
        )
    return receipt, payload


def decode_synoptic_ground_capture(
    capture_id: str,
    *,
    raw_store: RawArtifactStore,
    ground_store: CanonicalGroundEvidenceStore,
    deadline_at: float | None = None,
) -> SynopticGroundDecodeResult:
    """Replay one exact Synoptic capture into isolated candidate facts."""

    if (
        not isinstance(capture_id, str)
        or not capture_id.startswith("capture-")
        or len(capture_id) != 72
    ):
        raise ValueError("capture_id is geen schema-3 capture-identiteit")
    if not isinstance(raw_store, RawArtifactStore):
        raise TypeError("raw_store moet een RawArtifactStore zijn")
    if not isinstance(ground_store, CanonicalGroundEvidenceStore):
        raise TypeError("ground_store moet een CanonicalGroundEvidenceStore zijn")
    deadline = _deadline(deadline_at)
    try:
        bundle = raw_store.load_capture_bundle_bytes(capture_id)
    except RawArtifactError as exc:
        raise SynopticGroundCaptureError(
            "Synoptic schema-3 capture kon niet byte-exact worden geverifieerd"
        ) from exc
    _check_deadline(deadline)
    if bundle is None:
        raise SynopticGroundCaptureError(
            "Synoptic schema-3 capture bestaat niet of is incompleet"
        )
    capture, receipts, payloads = bundle
    receipt, payload = _validate_capture_bundle(capture, receipts, payloads)
    _check_deadline(deadline)
    try:
        identity = native._inspect_response(payload, deadline=deadline)
    except native.SynopticNativeDeadlineError as exc:
        raise SynopticGroundDecoderDeadlineError(
            "Synoptic structurele verificatie overschreed de deadline"
        ) from exc
    except native.SynopticNativeContractError as exc:
        raise SynopticGroundDecoderContractError(
            "Synoptic capture faalt de onafhankelijke native structuurverificatie"
        ) from exc
    if (
        identity.structural_status != "verified_precip_sensor_qc_structure"
        or not identity.precipitation_structure_verified
        or not identity.sensor_metadata_structure_verified
        or not identity.qc_structure_verified
    ):
        raise SynopticGroundDecoderContractError(
            "Synoptic capture is niet volledig structureel geverifieerd"
        )
    root = _ExactJsonParser(payload, deadline).parse()
    (
        plans,
        station_count,
        sensor_set_count,
        interval_fact_count,
        accumulated_fact_count,
    ) = _build_plans(root, payload=payload, deadline_at=deadline)
    _check_deadline(deadline)

    member_bindings = tuple(
        GroundCaptureMemberBinding(
            member_id=member.member_id,
            role=member.role,
            receipt_id=member.receipt_id,
            content_sha256=member.content_sha256,
            receipt_sha256=member.receipt_sha256,
            received_at=member.received_at,
        )
        for member in capture.members
    )
    source_member = member_bindings[0]
    ingested_at = max(
        float(capture.created_at),
        *(member.received_at for member in member_bindings),
    )
    config_sha256 = _decoder_config_sha256()
    facts: list[GroundObservationFact] = []
    for plan in plans:
        _check_deadline(deadline)
        record_bytes = payload[plan.record_start : plan.record_end]
        if not record_bytes or len(record_bytes) > MAX_STATION_RECORD_BYTES:
            raise SynopticGroundDecoderCapacityError(
                "Synoptic bronrecord valt buiten de bytegrens"
            )
        fact = GroundObservationFact(
            capture_id=capture.capture_id,
            capture_receipt_sha256=capture.capture_receipt_sha256,
            members=member_bindings,
            source_member_role="response_json",
            source_member_content_sha256=source_member.content_sha256,
            source_member_receipt_sha256=source_member.receipt_sha256,
            source_record_locator=plan.record_locator,
            source_record_sha256=hashlib.sha256(record_bytes).hexdigest(),
            decoder_identity=SYNOPTIC_GROUND_DECODER_IDENTITY,
            decoder_version=SYNOPTIC_GROUND_DECODER_VERSION,
            config_sha256=config_sha256,
            provider="synoptic",
            product=native.SYNOPTIC_CAPTURE_PRODUCT,
            station=plan.station,
            observed_at=plan.observed_at,
            representative_interval_start=plan.interval_start,
            representative_interval_end=plan.interval_end,
            received_at=float(receipt.received_at),
            ingested_at=ingested_at,
            measurement=plan.measurement,
            provider_qc=plan.provider_qc,
        )
        # Validate every fact before the first durable commit.  Shape errors
        # therefore cannot leave a prefix of an otherwise invalid batch.
        parse_ground_observation(fact.to_mapping())
        facts.append(fact)

    _check_deadline(deadline)
    committed = ground_store.commit_batch(tuple(facts), deadline_at=deadline)
    _check_deadline(deadline)
    return SynopticGroundDecodeResult(
        capture_id=capture.capture_id,
        station_count=station_count,
        sensor_set_count=sensor_set_count,
        interval_fact_count=interval_fact_count,
        accumulated_fact_count=accumulated_fact_count,
        fact_receipts=committed,
        config_sha256=config_sha256,
    )


__all__ = [
    "DEFAULT_DECODE_SECONDS",
    "MAX_DECODE_FACTS",
    "MAX_RESPONSE_BYTES",
    "SYNOPTIC_ACCUMULATED_SEMANTICS",
    "SYNOPTIC_GROUND_ADMISSION_STATUS",
    "SYNOPTIC_GROUND_DECODER_IDENTITY",
    "SYNOPTIC_GROUND_DECODER_VERSION",
    "SYNOPTIC_INTERVAL_SEMANTICS",
    "SYNOPTIC_PROVISIONAL_STATUS",
    "SynopticGroundCaptureError",
    "SynopticGroundDecodeResult",
    "SynopticGroundDecoderCapacityError",
    "SynopticGroundDecoderContractError",
    "SynopticGroundDecoderDeadlineError",
    "SynopticGroundDecoderError",
    "decode_synoptic_ground_capture",
]
