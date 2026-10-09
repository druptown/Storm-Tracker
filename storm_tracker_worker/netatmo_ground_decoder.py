"""Candidate-only Netatmo ``getpublicdata`` ground-evidence decoder.

This module only replays a complete, byte-verified schema-3 capture.  It does
not call Netatmo, is not wired into the worker service, and never derives wet,
dry, coverage, confidence or ground confirmation.

The public developer page currently does not expose a machine-verifiable
field-level contract for the legacy ``rain*`` fields.  Consequently values are
stored as exact, opaque Netatmo-native variables with an explicitly
unspecified unit and no inferred accumulation window or rate semantics.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
import time
from typing import Final, Literal

from .canonical_ground_evidence_store import (
    CanonicalGroundEvidenceStore,
    GroundCaptureMemberBinding,
    GroundMeasurementValue,
    GroundObservationFact,
    GroundObservationReceipt,
    GroundStationSite,
    ProviderQcFact,
)
from .netatmo_native import (
    NETATMO_CAPTURE_PRODUCT,
    NETATMO_JSON_PRODUCT,
    NETATMO_QUERY_CONTEXT_SCHEMA,
    NETATMO_RAW_COLLECTOR,
    NETATMO_RAW_COLLECTOR_VERSION,
    NETATMO_STRUCTURE_INSPECTOR_VERSION,
)
from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactError,
    RawArtifactStore,
)


NETATMO_GROUND_DECODER_IDENTITY = "netatmo_getpublicdata_ground_evidence"
NETATMO_GROUND_DECODER_VERSION = "netatmo-ground-evidence-shadow-v1"
NETATMO_GROUND_ADMISSION_STATUS = "candidate_only_unadmitted"
NETATMO_NATIVE_UNSPECIFIED_UNIT = "netatmo_native_unit_unspecified"
NETATMO_SEMANTICS_STATUS = "field_level_product_semantics_unverified"

MAX_RESPONSE_BYTES: Final[int] = 8 * 1024 * 1024
MAX_JSON_NODES: Final[int] = 200_000
MAX_JSON_DEPTH: Final[int] = 48
MAX_OBJECT_MEMBERS: Final[int] = 10_000
MAX_ARRAY_ITEMS: Final[int] = 20_000
MAX_STRING_CHARS: Final[int] = 256 * 1024
MAX_KEY_CHARS: Final[int] = 512
MAX_STATIONS: Final[int] = 4_096
MAX_MODULES_PER_STATION: Final[int] = 64
MAX_RESULTS_PER_MODULE: Final[int] = 2_048
MAX_DECODE_FACTS: Final[int] = 16_384
MAX_RECORD_BYTES: Final[int] = 512 * 1024
DEFAULT_DECODE_SECONDS: Final[float] = 30.0
MAX_IDENTIFIER_CHARS: Final[int] = 256
MAX_QC_FIELDS: Final[int] = 16

_NUMBER_RE = re.compile(
    rb"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?"
)
_INTEGER_TIMESTAMP_RE = re.compile(r"^(?:0|[1-9][0-9]*)$")
_KNOWN_DIRECT_RAIN_FIELDS = frozenset(
    {"rain", "rain_live", "rain_60min", "rain_24h"}
)
_KNOWN_RAIN_META_FIELDS = frozenset({"rain_timeutc"})
_PROVIDER_QC_FIELDS = (
    "mark",
    "quality",
    "status",
    "data_quality",
)
_FORBIDDEN_SECRET_KEYS = frozenset(
    {
        "access_token",
        "refresh_token",
        "authorization",
        "cookie",
        "set_cookie",
        "client_secret",
        "client_id",
        "application_key",
        "api_key",
    }
)


class NetatmoGroundDecoderError(RuntimeError):
    """Base class for a fail-closed candidate decode."""


class NetatmoGroundDecoderContractError(NetatmoGroundDecoderError):
    """The verified bytes do not satisfy the closed decoder contract."""


class NetatmoGroundDecoderDeadlineError(NetatmoGroundDecoderError):
    """The single absolute monotone deadline elapsed."""


class NetatmoGroundDecoderCapacityError(NetatmoGroundDecoderError):
    """A hard input or output bound would be exceeded."""


class NetatmoGroundCaptureError(NetatmoGroundDecoderError):
    """The requested complete schema-3 capture is unavailable or invalid."""


@dataclass(frozen=True, slots=True)
class NetatmoGroundDecodeResult:
    """Payload-free receipts from one deterministic shadow replay."""

    capture_id: str
    station_count: int
    module_count: int
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
            "decoder_identity": NETATMO_GROUND_DECODER_IDENTITY,
            "decoder_version": NETATMO_GROUND_DECODER_VERSION,
            "capture_id": self.capture_id,
            "station_count": self.station_count,
            "module_count": self.module_count,
            "fact_count": self.fact_count,
            "fact_ids": [item.fact_id for item in self.fact_receipts],
            "config_sha256": self.config_sha256,
            "measurement_semantics": NETATMO_SEMANTICS_STATUS,
            "wet_claim": False,
            "dry_claim": False,
            "coverage_claim": False,
            "ground_confirmation_eligible": False,
            "operational_admission_eligible": False,
            "admission_status": NETATMO_GROUND_ADMISSION_STATUS,
        }


@dataclass(frozen=True, slots=True)
class _JsonMember:
    key: str
    key_start: int
    key_end: int
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
            raise NetatmoGroundDecoderContractError(
                "Netatmo JSON-node is geen object"
            )
        return {member.key: member for member in self.members}


class _ExactJsonParser:
    """Small bounded JSON parser that retains exact byte spans and lexemes."""

    def __init__(self, payload: bytes, deadline_at: float) -> None:
        if type(payload) is not bytes:
            raise NetatmoGroundDecoderContractError(
                "Netatmo responsemember moet exacte bytes bevatten"
            )
        if not payload or len(payload) > MAX_RESPONSE_BYTES:
            raise NetatmoGroundDecoderCapacityError(
                "Netatmo responsemember valt buiten de bytegrens"
            )
        self.payload = payload
        self.deadline_at = deadline_at
        self.position = 0
        self.nodes = 0

    def parse(self) -> _JsonNode:
        if self.payload.startswith(b"\xef\xbb\xbf"):
            raise NetatmoGroundDecoderContractError(
                "Netatmo JSON mag geen UTF-8 BOM bevatten"
            )
        self._space()
        node = self._value(0)
        self._space()
        if self.position != len(self.payload):
            raise NetatmoGroundDecoderContractError(
                "Netatmo JSON bevat trailing bytes"
            )
        return node

    def _checkpoint(self) -> None:
        if time.monotonic() >= self.deadline_at:
            raise NetatmoGroundDecoderDeadlineError(
                "Netatmo ground-decode overschrijdt de absolute deadline"
            )

    def _space(self) -> None:
        payload = self.payload
        length = len(payload)
        while self.position < length and payload[self.position] in b" \t\r\n":
            self.position += 1

    def _value(self, depth: int) -> _JsonNode:
        self._checkpoint()
        if depth > MAX_JSON_DEPTH:
            raise NetatmoGroundDecoderCapacityError(
                "Netatmo JSON overschrijdt de dieptegrens"
            )
        self.nodes += 1
        if self.nodes > MAX_JSON_NODES:
            raise NetatmoGroundDecoderCapacityError(
                "Netatmo JSON overschrijdt de nodegrens"
            )
        if self.position >= len(self.payload):
            raise NetatmoGroundDecoderContractError("Netatmo JSON is afgekapt")
        token = self.payload[self.position]
        if token == 0x7B:
            return self._object(depth)
        if token == 0x5B:
            return self._array(depth)
        if token == 0x22:
            start = self.position
            value, end = self._string(MAX_STRING_CHARS)
            return _JsonNode("string", start, end, scalar=value)
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
        literal: bytes,
        value: bool | None,
    ) -> _JsonNode:
        start = self.position
        self.position += len(literal)
        return _JsonNode(kind, start, self.position, scalar=value)

    def _number(self) -> _JsonNode:
        start = self.position
        match = _NUMBER_RE.match(self.payload, start)
        if match is None:
            raise NetatmoGroundDecoderContractError(
                "Netatmo JSON bevat een ongeldige waarde"
            )
        end = match.end()
        if end < len(self.payload) and self.payload[end] not in b" \t\r\n,]}:":
            raise NetatmoGroundDecoderContractError(
                "Netatmo JSON bevat een ongeldig getal"
            )
        lexeme = self.payload[start:end].decode("ascii")
        if len(lexeme) > 128:
            raise NetatmoGroundDecoderCapacityError(
                "Netatmo getallexeme overschrijdt de grens"
            )
        try:
            parsed = Decimal(lexeme)
        except InvalidOperation as exc:
            raise NetatmoGroundDecoderContractError(
                "Netatmo getallexeme is niet exact decimaal"
            ) from exc
        if not parsed.is_finite() or parsed.copy_abs() > Decimal("1e100"):
            raise NetatmoGroundDecoderContractError(
                "Netatmo getallexeme is niet eindig"
            )
        self.position = end
        return _JsonNode("number", start, end, scalar=lexeme)

    def _string(self, maximum_chars: int) -> tuple[str, int]:
        start = self.position
        self.position += 1
        escaped = False
        while self.position < len(self.payload):
            byte = self.payload[self.position]
            if byte < 0x20:
                raise NetatmoGroundDecoderContractError(
                    "Netatmo JSON-string bevat een controlteken"
                )
            if escaped:
                if byte == 0x75:
                    digits = self.payload[self.position + 1 : self.position + 5]
                    if len(digits) != 4 or any(
                        item not in b"0123456789abcdefABCDEF" for item in digits
                    ):
                        raise NetatmoGroundDecoderContractError(
                            "Netatmo JSON bevat een ongeldige Unicode-escape"
                        )
                    self.position += 5
                elif byte in b'"\\/bfnrt':
                    self.position += 1
                else:
                    raise NetatmoGroundDecoderContractError(
                        "Netatmo JSON bevat een ongeldige escape"
                    )
                escaped = False
                continue
            if byte == 0x5C:
                escaped = True
                self.position += 1
                continue
            if byte == 0x22:
                self.position += 1
                token = self.payload[start : self.position]
                try:
                    decoded = json.loads(token.decode("utf-8", errors="strict"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise NetatmoGroundDecoderContractError(
                        "Netatmo JSON-string is ongeldig"
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
                    raise NetatmoGroundDecoderCapacityError(
                        "Netatmo JSON-string valt buiten de tekengrens"
                    )
                return decoded, self.position
            self.position += 1
        raise NetatmoGroundDecoderContractError(
            "Netatmo JSON-string is niet afgesloten"
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
                raise NetatmoGroundDecoderCapacityError(
                    "Netatmo JSON-object overschrijdt de membergrens"
                )
            if self.position >= len(self.payload) or self.payload[self.position] != 0x22:
                raise NetatmoGroundDecoderContractError(
                    "Netatmo JSON-object mist een stringkey"
                )
            member_start = self.position
            key_start = self.position
            key, key_end = self._string(MAX_KEY_CHARS)
            if key in seen:
                raise NetatmoGroundDecoderContractError(
                    "Netatmo JSON bevat een dubbele objectkey"
                )
            seen.add(key)
            if _normalized_key(key) in _FORBIDDEN_SECRET_KEYS:
                raise NetatmoGroundDecoderContractError(
                    "Netatmo JSON bevat verboden authmetadata"
                )
            self._space()
            if self.position >= len(self.payload) or self.payload[self.position] != 0x3A:
                raise NetatmoGroundDecoderContractError(
                    "Netatmo JSON-object mist een dubbelepunt"
                )
            self.position += 1
            self._space()
            value = self._value(depth + 1)
            members.append(
                _JsonMember(
                    key=key,
                    key_start=key_start,
                    key_end=key_end,
                    start=member_start,
                    end=value.end,
                    value=value,
                )
            )
            self._space()
            if self.position >= len(self.payload):
                raise NetatmoGroundDecoderContractError(
                    "Netatmo JSON-object is afgekapt"
                )
            separator = self.payload[self.position]
            self.position += 1
            if separator == 0x7D:
                break
            if separator != 0x2C:
                raise NetatmoGroundDecoderContractError(
                    "Netatmo JSON-object heeft een ongeldige separator"
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
                raise NetatmoGroundDecoderCapacityError(
                    "Netatmo JSON-array overschrijdt de itemgrens"
                )
            items.append(self._value(depth + 1))
            self._space()
            if self.position >= len(self.payload):
                raise NetatmoGroundDecoderContractError(
                    "Netatmo JSON-array is afgekapt"
                )
            separator = self.payload[self.position]
            self.position += 1
            if separator == 0x5D:
                break
            if separator != 0x2C:
                raise NetatmoGroundDecoderContractError(
                    "Netatmo JSON-array heeft een ongeldige separator"
                )
            self._space()
        return _JsonNode("array", start, self.position, items=tuple(items))


@dataclass(frozen=True, slots=True)
class _PlannedObservation:
    station: GroundStationSite
    module_id: str
    variable: str
    value_node: _JsonNode
    observed_at: float
    observed_at_token: str
    record_locator: str
    record_start: int
    record_end: int
    provider_qc: tuple[ProviderQcFact, ...]


def _normalized_key(value: str) -> str:
    return value.strip().lower().replace("-", "_")


def _check_deadline(deadline_at: float) -> None:
    if time.monotonic() >= deadline_at:
        raise NetatmoGroundDecoderDeadlineError(
            "Netatmo ground-decode overschrijdt de absolute deadline"
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
        "decoder_identity": NETATMO_GROUND_DECODER_IDENTITY,
        "decoder_version": NETATMO_GROUND_DECODER_VERSION,
        "accepted_capture_product": NETATMO_CAPTURE_PRODUCT,
        "accepted_member_product": NETATMO_JSON_PRODUCT,
        "accepted_member_role": "response_json",
        "direct_variables": sorted(_KNOWN_DIRECT_RAIN_FIELDS),
        "res_type_variables": ["rain"],
        "measurement_semantics": NETATMO_SEMANTICS_STATUS,
        "unit": NETATMO_NATIVE_UNSPECIFIED_UNIT,
        "interval_policy": "degenerate_at_exact_provider_record_time",
        "null_policy": "explicit_missing",
        "unknown_schema_policy": "fail_closed",
        "bounds": {
            "max_response_bytes": MAX_RESPONSE_BYTES,
            "max_json_nodes": MAX_JSON_NODES,
            "max_json_depth": MAX_JSON_DEPTH,
            "max_object_members": MAX_OBJECT_MEMBERS,
            "max_array_items": MAX_ARRAY_ITEMS,
            "max_stations": MAX_STATIONS,
            "max_modules_per_station": MAX_MODULES_PER_STATION,
            "max_results_per_module": MAX_RESULTS_PER_MODULE,
            "max_decode_facts": MAX_DECODE_FACTS,
            "max_record_bytes": MAX_RECORD_BYTES,
        },
        "operational_admission_eligible": False,
    }
    return hashlib.sha256(_canonical_json(contract)).hexdigest()


def _identifier(node: _JsonNode, label: str) -> str:
    if node.kind != "string" or not isinstance(node.scalar, str):
        raise NetatmoGroundDecoderContractError(f"Netatmo {label} is geen string")
    value = node.scalar
    if (
        not value
        or len(value) > MAX_IDENTIFIER_CHARS
        or any(ord(character) < 32 for character in value)
    ):
        raise NetatmoGroundDecoderContractError(
            f"Netatmo {label} valt buiten het identiteitscontract"
        )
    return value


def _decimal_node(node: _JsonNode, label: str) -> tuple[str, Decimal]:
    if node.kind != "number" or not isinstance(node.scalar, str):
        raise NetatmoGroundDecoderContractError(
            f"Netatmo {label} is geen exact JSON-getal"
        )
    try:
        parsed = Decimal(node.scalar)
    except InvalidOperation as exc:  # defensive: parser already checked this
        raise NetatmoGroundDecoderContractError(
            f"Netatmo {label} is geen exact decimaal"
        ) from exc
    if not parsed.is_finite():
        raise NetatmoGroundDecoderContractError(
            f"Netatmo {label} is niet eindig"
        )
    return node.scalar, parsed


def _timestamp_node(node: _JsonNode, label: str) -> tuple[float, str]:
    if node.kind == "number":
        lexeme, parsed = _decimal_node(node, label)
    elif node.kind == "string" and isinstance(node.scalar, str):
        lexeme = node.scalar
        if not _INTEGER_TIMESTAMP_RE.fullmatch(lexeme):
            raise NetatmoGroundDecoderContractError(
                f"Netatmo {label} heeft ambigue tijdsemantiek"
            )
        parsed = Decimal(lexeme)
    else:
        raise NetatmoGroundDecoderContractError(
            f"Netatmo {label} heeft een onbekend tijdtype"
        )
    if parsed < 0 or parsed > Decimal("253402300799"):
        raise NetatmoGroundDecoderContractError(
            f"Netatmo {label} valt buiten het tijdsbereik"
        )
    result = float(parsed)
    if not math.isfinite(result):
        raise NetatmoGroundDecoderContractError(
            f"Netatmo {label} kan niet veilig als tijdas worden gebonden"
        )
    return result, lexeme


def _coordinate(node: _JsonNode, label: str, minimum: str, maximum: str) -> str:
    lexeme, parsed = _decimal_node(node, label)
    if parsed < Decimal(minimum) or parsed > Decimal(maximum):
        raise NetatmoGroundDecoderContractError(
            f"Netatmo {label} valt buiten bereik"
        )
    return lexeme


def _qc_value(node: _JsonNode, label: str) -> str | None:
    if node.kind == "null":
        return None
    if node.kind == "string" and isinstance(node.scalar, str):
        if len(node.scalar) > 512:
            raise NetatmoGroundDecoderCapacityError(
                f"Netatmo {label} overschrijdt de QC-grens"
            )
        return node.scalar
    if node.kind == "number" and isinstance(node.scalar, str):
        return node.scalar
    if node.kind == "bool" and isinstance(node.scalar, bool):
        return "true" if node.scalar else "false"
    raise NetatmoGroundDecoderContractError(
        f"Netatmo {label} heeft een niet-scalair QC-type"
    )


def _provider_qc(
    station_members: dict[str, _JsonMember],
    measure_members: dict[str, _JsonMember],
) -> tuple[ProviderQcFact, ...]:
    # The ground contract has no separate decoder-QC extension point.  Keep
    # the two non-negotiable semantic caveats visible beside every fact rather
    # than letting a downstream consumer mistake start=end for proven
    # instantaneous support or the numeric value for a known unit.
    result: list[ProviderQcFact] = [
        ProviderQcFact(
            flag="netatmo.decoder.temporal_support_uncertified",
            value=None,
            text=(
                "representative start=end is only the exact provider "
                "record-time anchor; it is not instantaneous-rate support"
            ),
        ),
        ProviderQcFact(
            flag="netatmo.decoder.unit_semantics_uncertified",
            value=None,
            text=(
                "numeric lexeme is preserved without assigning a physical "
                "unit or accumulation meaning"
            ),
        ),
    ]
    for scope, members in (("station", station_members), ("module", measure_members)):
        for field in _PROVIDER_QC_FIELDS:
            member = members.get(field)
            if member is None:
                continue
            value = _qc_value(member.value, f"{scope}.{field}")
            result.append(
                ProviderQcFact(
                    flag=f"netatmo.{scope}.{field}",
                    value=value,
                    text="explicit_json_null" if member.value.kind == "null" else None,
                )
            )
    if len(result) > MAX_QC_FIELDS:
        raise NetatmoGroundDecoderCapacityError(
            "Netatmo provider-QC overschrijdt de feitengrens"
        )
    return tuple(result)


def _json_pointer_token(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _measurement(node: _JsonNode, variable: str) -> GroundMeasurementValue:
    if node.kind == "null":
        return GroundMeasurementValue(
            variable=variable,
            state="missing",
            representation="none",
            exact_value=None,
            unit=NETATMO_NATIVE_UNSPECIFIED_UNIT,
            censor_limit=None,
        )
    lexeme, parsed = _decimal_node(node, variable)
    if parsed < 0:
        raise NetatmoGroundDecoderContractError(
            "Netatmo neerslagwaarde is negatief en wordt niet gecorrigeerd"
        )
    return GroundMeasurementValue(
        variable=variable,
        state="observed",
        representation="decimal",
        exact_value=lexeme,
        unit=NETATMO_NATIVE_UNSPECIFIED_UNIT,
        censor_limit=None,
    )


def _station_site(station: _JsonNode) -> GroundStationSite:
    members = station.member_map()
    station_member = members.get("_id")
    place_member = members.get("place")
    if station_member is None or place_member is None:
        raise NetatmoGroundDecoderContractError(
            "Netatmo station mist identiteit of plaats"
        )
    station_id = _identifier(station_member.value, "station-ID")
    if place_member.value.kind != "object":
        raise NetatmoGroundDecoderContractError("Netatmo place is geen object")
    place = place_member.value.member_map()
    location_member = place.get("location")
    if location_member is None or location_member.value.kind != "array":
        raise NetatmoGroundDecoderContractError(
            "Netatmo station mist een exacte locatie-array"
        )
    location = location_member.value.items
    if len(location) != 2:
        raise NetatmoGroundDecoderContractError(
            "Netatmo locatie-array heeft niet exact twee assen"
        )
    longitude = _coordinate(location[0], "longitude", "-180", "180")
    latitude = _coordinate(location[1], "latitude", "-90", "90")
    altitude = place.get("altitude")
    if altitude is None or altitude.value.kind == "null":
        elevation_state = "unknown"
        elevation = None
    else:
        elevation = _coordinate(
            altitude.value,
            "altitude",
            "-1000",
            "100000",
        )
        elevation_state = "known"
    return GroundStationSite(
        provider_station_id=station_id,
        site_id=f"netatmo-public:{station_id}",
        network="netatmo_public_getpublicdata",
        # Public ``place.city`` is not a station name and is deliberately not
        # relabelled as one.
        station_name=None,
        position_state="known",
        latitude_decimal=latitude,
        longitude_decimal=longitude,
        elevation_state=elevation_state,  # type: ignore[arg-type]
        elevation_m_decimal=elevation,
    )


def _append_plan(
    plans: list[_PlannedObservation],
    *,
    station: GroundStationSite,
    module_id: str,
    variable: str,
    value_node: _JsonNode,
    observed_at: float,
    observed_at_token: str,
    record_locator: str,
    record_start: int,
    record_end: int,
    provider_qc: tuple[ProviderQcFact, ...],
) -> None:
    if record_end - record_start > MAX_RECORD_BYTES:
        raise NetatmoGroundDecoderCapacityError(
            "Netatmo bronrecord overschrijdt de bytegrens"
        )
    if len(plans) >= MAX_DECODE_FACTS:
        raise NetatmoGroundDecoderCapacityError(
            "Netatmo ground-decode overschrijdt de feitengrens"
        )
    # Validate the value before any ground-store mutation.
    _measurement(value_node, variable)
    plans.append(
        _PlannedObservation(
            station=station,
            module_id=module_id,
            variable=variable,
            value_node=value_node,
            observed_at=observed_at,
            observed_at_token=observed_at_token,
            record_locator=record_locator,
            record_start=record_start,
            record_end=record_end,
            provider_qc=provider_qc,
        )
    )


def _plans_from_measure(
    plans: list[_PlannedObservation],
    *,
    station: GroundStationSite,
    station_index: int,
    station_members: dict[str, _JsonMember],
    module_id: str,
    measure: _JsonNode,
) -> None:
    members = measure.member_map()
    qc = _provider_qc(station_members, members)
    module_pointer = _json_pointer_token(module_id)
    base = f"/body/{station_index}/measures/{module_pointer}"

    for key in members:
        normalized = key.lower()
        if normalized.startswith("rain") and key not in (
            _KNOWN_DIRECT_RAIN_FIELDS | _KNOWN_RAIN_META_FIELDS
        ):
            raise NetatmoGroundDecoderContractError(
                "Netatmo module bevat een onbekende rain-variabele"
            )

    direct = [field for field in sorted(_KNOWN_DIRECT_RAIN_FIELDS) if field in members]
    if direct:
        timestamp_member = members.get("rain_timeutc")
        if timestamp_member is None:
            raise NetatmoGroundDecoderContractError(
                "Netatmo directe rain-variabele mist rain_timeutc"
            )
        observed_at, timestamp_token = _timestamp_node(
            timestamp_member.value,
            "rain_timeutc",
        )
        for field in direct:
            member = members[field]
            _append_plan(
                plans,
                station=station,
                module_id=module_id,
                variable=f"netatmo_native.{field}",
                value_node=member.value,
                observed_at=observed_at,
                observed_at_token=timestamp_token,
                record_locator=f"{base}/{field}",
                record_start=member.start,
                record_end=member.end,
                provider_qc=qc,
            )

    type_member = members.get("type")
    res_member = members.get("res")
    if type_member is None and res_member is None:
        return
    if type_member is None or res_member is None:
        raise NetatmoGroundDecoderContractError(
            "Netatmo type/res-structuur is incompleet"
        )
    if type_member.value.kind != "array" or res_member.value.kind != "object":
        raise NetatmoGroundDecoderContractError(
            "Netatmo type/res heeft een onverwachte vorm"
        )
    types: list[str] = []
    for index, item in enumerate(type_member.value.items):
        value = _identifier(item, f"type[{index}]")
        if value.lower().startswith("rain") and value != "rain":
            raise NetatmoGroundDecoderContractError(
                "Netatmo type-array bevat ambigue rain-semantiek"
            )
        types.append(value)
    if len(types) != len(set(types)):
        raise NetatmoGroundDecoderContractError(
            "Netatmo type-array bevat dubbele variabelen"
        )
    rain_indexes = [index for index, value in enumerate(types) if value == "rain"]
    if not rain_indexes:
        return
    if len(rain_indexes) != 1:
        raise NetatmoGroundDecoderContractError(
            "Netatmo type-array bindt rain niet uniek"
        )
    rain_index = rain_indexes[0]
    results = res_member.value.members
    if len(results) > MAX_RESULTS_PER_MODULE:
        raise NetatmoGroundDecoderCapacityError(
            "Netatmo res overschrijdt de timestampgrens"
        )
    for result in results:
        if not _INTEGER_TIMESTAMP_RE.fullmatch(result.key):
            raise NetatmoGroundDecoderContractError(
                "Netatmo res-key heeft ambigue tijdsemantiek"
            )
        observed_at, timestamp_token = _timestamp_node(
            _JsonNode("string", result.key_start, result.key_end, scalar=result.key),
            "res timestamp",
        )
        if result.value.kind != "array" or len(result.value.items) != len(types):
            raise NetatmoGroundDecoderContractError(
                "Netatmo res/type-arraylengtes komen niet exact overeen"
            )
        _append_plan(
            plans,
            station=station,
            module_id=module_id,
            variable="netatmo_native.type.rain",
            value_node=result.value.items[rain_index],
            observed_at=observed_at,
            observed_at_token=timestamp_token,
            record_locator=(
                f"{base}/res/{_json_pointer_token(result.key)}/{rain_index}"
            ),
            record_start=result.start,
            record_end=result.end,
            provider_qc=qc,
        )


def _build_plans(root: _JsonNode, deadline_at: float) -> tuple[list[_PlannedObservation], int, int]:
    if root.kind != "object":
        raise NetatmoGroundDecoderContractError(
            "Netatmo response-root is geen object"
        )
    members = root.member_map()
    status = members.get("status")
    body = members.get("body")
    if (
        status is None
        or status.value.kind != "string"
        or status.value.scalar != "ok"
    ):
        raise NetatmoGroundDecoderContractError(
            "Netatmo response-status is niet exact ok"
        )
    if body is None or body.value.kind != "array":
        raise NetatmoGroundDecoderContractError(
            "Netatmo response mist een stationsarray"
        )
    if len(body.value.items) > MAX_STATIONS:
        raise NetatmoGroundDecoderCapacityError(
            "Netatmo response overschrijdt de stationsgrens"
        )
    plans: list[_PlannedObservation] = []
    station_ids: set[str] = set()
    module_count = 0
    for station_index, station_node in enumerate(body.value.items):
        _check_deadline(deadline_at)
        if station_node.kind != "object":
            raise NetatmoGroundDecoderContractError(
                "Netatmo stationsarray bevat geen object"
            )
        station_members = station_node.member_map()
        station = _station_site(station_node)
        if station.provider_station_id in station_ids:
            raise NetatmoGroundDecoderContractError(
                "Netatmo response bevat een dubbel station-ID"
            )
        station_ids.add(station.provider_station_id)
        measures_member = station_members.get("measures")
        if measures_member is None or measures_member.value.kind != "object":
            raise NetatmoGroundDecoderContractError(
                "Netatmo station mist een measures-object"
            )
        measures = measures_member.value.members
        if len(measures) > MAX_MODULES_PER_STATION:
            raise NetatmoGroundDecoderCapacityError(
                "Netatmo station overschrijdt de modulegrens"
            )
        module_ids: set[str] = set()
        for module_member in measures:
            _check_deadline(deadline_at)
            module_id = module_member.key
            if (
                not module_id
                or len(module_id) > MAX_IDENTIFIER_CHARS
                or any(ord(character) < 32 for character in module_id)
            ):
                raise NetatmoGroundDecoderContractError(
                    "Netatmo module-ID valt buiten het contract"
                )
            if module_id in module_ids:
                raise NetatmoGroundDecoderContractError(
                    "Netatmo station bevat een dubbel module-ID"
                )
            module_ids.add(module_id)
            if module_member.value.kind != "object":
                raise NetatmoGroundDecoderContractError(
                    "Netatmo measure is geen object"
                )
            module_count += 1
            _plans_from_measure(
                plans,
                station=station,
                station_index=station_index,
                station_members=station_members,
                module_id=module_id,
                measure=module_member.value,
            )
    return plans, len(station_ids), module_count


def _validate_capture_bundle(capture, receipts, payloads) -> tuple[object, bytes]:
    if (
        capture.provider != "netatmo"
        or capture.product != NETATMO_CAPTURE_PRODUCT
        or capture.product_timestamp is not None
        or not capture.complete
        or set(receipts) != {"response_json"}
        or set(payloads) != {"response_json"}
        or len(capture.members) != 1
        or capture.members[0].role != "response_json"
    ):
        raise NetatmoGroundCaptureError(
            "Netatmo capture heeft niet het exacte complete responsecontract"
        )
    receipt = receipts["response_json"]
    payload = payloads["response_json"]
    if (
        receipt.provider != "netatmo"
        or receipt.product != NETATMO_JSON_PRODUCT
        or receipt.content_type.split(";", 1)[0].strip().lower()
        != "application/json"
        or receipt.size_bytes != len(payload)
        or receipt.content_sha256 != hashlib.sha256(payload).hexdigest()
        or not capture.fetch_id.endswith(":capture")
        or not receipt.fetch_id.endswith(":response_json")
        or capture.fetch_id[: -len(":capture")]
        != receipt.fetch_id[: -len(":response_json")]
    ):
        raise NetatmoGroundCaptureError(
            "Netatmo responsemember heeft een verkeerde identiteit"
        )
    provenance = receipt.provenance
    expected_keys = {
        "representation",
        "capture_stage",
        "collector",
        "collector_version",
        "structure_inspector_version",
        "query_context",
        "query_context_sha256",
        "transport_attempt",
        "response_structure_claim",
        "meteorological_interpretation",
    }
    context = provenance.get("query_context") if isinstance(provenance, dict) else None
    if (
        not isinstance(provenance, dict)
        or set(provenance) != expected_keys
        or provenance.get("representation") != NATIVE_REPRESENTATION
        or provenance.get("capture_stage") != NATIVE_CAPTURE_STAGE
        or provenance.get("collector") != NETATMO_RAW_COLLECTOR
        or provenance.get("collector_version") != NETATMO_RAW_COLLECTOR_VERSION
        or provenance.get("structure_inspector_version")
        != NETATMO_STRUCTURE_INSPECTOR_VERSION
        or provenance.get("response_structure_claim")
        != "none_before_post_store_inspection"
        or provenance.get("meteorological_interpretation") != "none"
        or not isinstance(context, dict)
        or context.get("schema") != NETATMO_QUERY_CONTEXT_SCHEMA
        or provenance.get("query_context_sha256")
        != hashlib.sha256(_canonical_json(context)).hexdigest()
    ):
        raise NetatmoGroundCaptureError(
            "Netatmo responsemember mist de exacte native provenance"
        )
    query_sha = provenance["query_context_sha256"]
    expected_receipt_upstream = (
        "netatmo/getpublicdata"
        f"|query_sha256={query_sha}"
        f"|content_sha256={receipt.content_sha256}"
    )
    expected_capture_upstream = (
        f"{NETATMO_CAPTURE_PRODUCT}"
        f"|query_sha256={query_sha}"
        f"|content_sha256={receipt.content_sha256}"
    )
    if (
        receipt.upstream_id != expected_receipt_upstream
        or capture.upstream_capture_id != expected_capture_upstream
    ):
        raise NetatmoGroundCaptureError(
            "Netatmo capture is niet aan query en exacte response gebonden"
        )
    return receipt, payload


def decode_netatmo_ground_capture(
    capture_id: str,
    *,
    raw_store: RawArtifactStore,
    ground_store: CanonicalGroundEvidenceStore,
    deadline_at: float | None = None,
) -> NetatmoGroundDecodeResult:
    """Replay one complete capture into opaque canonical ground facts.

    The function is deterministic for a capture.  ``capture.created_at`` is
    used as the stable ingest axis, so an exact retry cannot conflict merely
    because it runs later.  Per-fact commits are append-only and crash-safe;
    retry after a mid-batch crash resumes idempotently.
    """

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
    _check_deadline(deadline)
    try:
        bundle = raw_store.load_capture_bundle_bytes(capture_id)
    except RawArtifactError as exc:
        raise NetatmoGroundCaptureError(
            "Netatmo schema-3 capture kon niet byte-exact worden geverifieerd"
        ) from exc
    _check_deadline(deadline)
    if bundle is None:
        raise NetatmoGroundCaptureError(
            "Netatmo schema-3 capture bestaat niet of is incompleet"
        )
    capture, receipts, payloads = bundle
    receipt, payload = _validate_capture_bundle(capture, receipts, payloads)
    root = _ExactJsonParser(payload, deadline).parse()
    plans, station_count, module_count = _build_plans(root, deadline)
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
        if not record_bytes or len(record_bytes) > MAX_RECORD_BYTES:
            raise NetatmoGroundDecoderCapacityError(
                "Netatmo bronrecord valt buiten de bytegrens"
            )
        facts.append(
            GroundObservationFact(
                capture_id=capture.capture_id,
                capture_receipt_sha256=capture.capture_receipt_sha256,
                members=member_bindings,
                source_member_role="response_json",
                source_member_content_sha256=source_member.content_sha256,
                source_member_receipt_sha256=source_member.receipt_sha256,
                source_record_locator=plan.record_locator,
                source_record_sha256=hashlib.sha256(record_bytes).hexdigest(),
                decoder_identity=NETATMO_GROUND_DECODER_IDENTITY,
                decoder_version=NETATMO_GROUND_DECODER_VERSION,
                config_sha256=config_sha256,
                provider="netatmo",
                product=NETATMO_CAPTURE_PRODUCT,
                station=plan.station,
                observed_at=plan.observed_at,
                # No official field-level interval contract is present.  A
                # degenerate interval binds only the exact record timestamp;
                # it is not an instantaneous-rate or accumulation claim.
                representative_interval_start=plan.observed_at,
                representative_interval_end=plan.observed_at,
                received_at=float(receipt.received_at),
                ingested_at=ingested_at,
                measurement=_measurement(plan.value_node, plan.variable),
                provider_qc=plan.provider_qc,
            )
        )

    _check_deadline(deadline)
    committed = ground_store.commit_batch(tuple(facts), deadline_at=deadline)
    _check_deadline(deadline)
    return NetatmoGroundDecodeResult(
        capture_id=capture.capture_id,
        station_count=station_count,
        module_count=module_count,
        fact_receipts=committed,
        config_sha256=config_sha256,
    )


__all__ = [
    "DEFAULT_DECODE_SECONDS",
    "MAX_DECODE_FACTS",
    "MAX_RESPONSE_BYTES",
    "NETATMO_GROUND_ADMISSION_STATUS",
    "NETATMO_GROUND_DECODER_IDENTITY",
    "NETATMO_GROUND_DECODER_VERSION",
    "NETATMO_NATIVE_UNSPECIFIED_UNIT",
    "NETATMO_SEMANTICS_STATUS",
    "NetatmoGroundCaptureError",
    "NetatmoGroundDecodeResult",
    "NetatmoGroundDecoderCapacityError",
    "NetatmoGroundDecoderContractError",
    "NetatmoGroundDecoderDeadlineError",
    "NetatmoGroundDecoderError",
    "decode_netatmo_ground_capture",
]
