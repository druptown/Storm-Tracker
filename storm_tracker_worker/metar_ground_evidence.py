"""Candidate-only METAR schema-3 capture to canonical ground facts.

The decoder consumes one already complete and byte-verified AviationWeather
``metars.cache.csv.gz`` capture.  It publishes only source-level station facts:
exact present-weather groups, unresolved precipitation remark tokens, and the
few accumulation intervals whose duration is explicit.  It never publishes a
rain/dry, station-coverage, confidence, confirmation, or admission decision.

Most importantly, ``Prrrr`` means *since the previous METAR*.  The current
cache does not contain that previous report time.  Such a token is therefore
retained as a coded remark (including ``P0000`` trace semantics), but is never
turned into an invented 3600-second accumulation.
"""
from __future__ import annotations

import codecs
from collections.abc import Sequence
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
import time
from typing import Any, Callable, Iterator, Protocol, runtime_checkable
import zlib

from .canonical_ground_evidence_store import (
    ADMISSION_STATUS,
    CanonicalGroundEvidenceStore,
    GroundCaptureMemberBinding,
    GroundMeasurementValue,
    GroundObservationFact,
    GroundObservationReceipt,
    GroundStationSite,
    ProviderQcFact,
    parse_ground_observation,
)
from . import metar_native as native
from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactStore,
)


DECODER_IDENTITY = "aviationweather_metar_canonical_ground"
DECODER_VERSION = "metar-canonical-ground-v1"
# The worker is intentionally capped at 25% total CPU on Home Assistant.  A
# worldwide METAR cache and its one atomic canonical-store commit can therefore
# take longer than the former 120-second budget without being unhealthy. Keep
# all byte, row, station and fact bounds unchanged; only let this low-priority
# background decode finish under the shared-HA or dedicated-NAS CPU caps.
DEFAULT_DECODE_SECONDS = 300.0

HARD_MAX_COMPRESSED_BYTES = native.MAX_COMPRESSED_BYTES
HARD_MAX_UNCOMPRESSED_BYTES = native.MAX_UNCOMPRESSED_BYTES
HARD_MAX_ROWS = native.MAX_CSV_ROWS
HARD_MAX_STATIONS = 20_000
HARD_MAX_FACTS = 64_000
HARD_MAX_FACT_BYTES = 64 * 1024 * 1024
HARD_MAX_WEATHER_TOKENS_PER_ROW = 8
HARD_MAX_REMARK_TOKENS_PER_ROW = 256
HARD_MAX_PROVIDER_FLAGS_PER_ROW = 24
HARD_MAX_FIELD_CHARS = native.MAX_CSV_FIELD_CHARS
HARD_MAX_RECORD_CHARS = native.MAX_CSV_RECORD_CHARS
HARD_MAX_EXPANSION_RATIO = 200.0
HARD_MAX_TARGET_SCOPES = 64
HARD_MAX_TARGET_RADIUS_KM = 250.0
MAX_QC_LEXEME_CHARS = 512
MAX_FUTURE_SECONDS = native.MAX_FUTURE_CLOCK_SKEW_SECONDS
MAX_STALE_SECONDS = native.MAX_CURRENT_REPORT_AGE_SECONDS

_CAPTURE_ID_RE = re.compile(r"^capture-[0-9a-f]{64}$")
_STATION_RE = re.compile(r"^[A-Z0-9]{4}$")
_DECIMAL_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")
_PRRRR_RE = re.compile(r"^P(?:\d{4}|/{4})$")
_SIX_RE = re.compile(r"^6(?:\d{4}|/{4})$")
_SEVEN_RE = re.compile(r"^7(?:\d{4}|/{4})$")
_PROVIDER_FLAG_RE = re.compile(
    r"^(?:AUTO|COR|AO1|AO2|AO2A|PWINO|PNO|FZRANO|TSNO|RVRNO|"
    r"VISNO(?:_[A-Z0-9]+)?|CHINO(?:_[A-Z0-9]+)?|\$)$"
)
_DESCRIPTORS = frozenset({"MI", "PR", "BC", "DR", "BL", "SH", "TS", "FZ"})
_PHENOMENA = frozenset({
    "DZ", "RA", "SN", "SG", "IC", "PL", "GR", "GS", "UP",
    "BR", "FG", "FU", "VA", "DU", "SA", "HZ", "PY",
    "PO", "SQ", "FC", "SS", "DS",
})

_RAW_TEXT = 0
_STATION = 1
_OBSERVED = 2
_LATITUDE = 3
_LONGITUDE = 4
_CORRECTED = 13
_AUTO = 14
_AUTO_STATION = 15
_MAINTENANCE = 16
_NO_SIGNAL = 17
_LIGHTNING_OFF = 18
_FREEZING_RAIN_OFF = 19
_PRESENT_WEATHER_OFF = 20
_WX_STRING = 21
_PRECIP_IN = 36
_PCP3_IN = 37
_PCP6_IN = 38
_PCP24_IN = 39
_METAR_TYPE = 42
_ELEVATION = 43

_FLAG_COLUMNS = (
    (_CORRECTED, "awc.csv.corrected"),
    (_AUTO, "awc.csv.auto"),
    (_AUTO_STATION, "awc.csv.auto_station"),
    (_MAINTENANCE, "awc.csv.maintenance_indicator_on"),
    (_NO_SIGNAL, "awc.csv.no_signal"),
    (_LIGHTNING_OFF, "awc.csv.lightning_sensor_off"),
    (_FREEZING_RAIN_OFF, "awc.csv.freezing_rain_sensor_off"),
    (_PRESENT_WEATHER_OFF, "awc.csv.present_weather_sensor_off"),
)


class MetarGroundEvidenceError(RuntimeError):
    """Base failure; it never implies dry weather."""


class MetarGroundCaptureMissingError(MetarGroundEvidenceError):
    pass


class MetarGroundStructureError(MetarGroundEvidenceError):
    pass


class MetarGroundCapacityError(MetarGroundEvidenceError):
    pass


class MetarGroundDeadlineError(MetarGroundEvidenceError):
    pass


@dataclass(frozen=True, slots=True)
class MetarGroundDecodeLimits:
    max_compressed_bytes: int = HARD_MAX_COMPRESSED_BYTES
    max_uncompressed_bytes: int = HARD_MAX_UNCOMPRESSED_BYTES
    max_rows: int = HARD_MAX_ROWS
    max_stations: int = HARD_MAX_STATIONS
    max_facts: int = HARD_MAX_FACTS
    max_fact_bytes: int = HARD_MAX_FACT_BYTES
    max_weather_tokens_per_row: int = HARD_MAX_WEATHER_TOKENS_PER_ROW
    max_remark_tokens_per_row: int = HARD_MAX_REMARK_TOKENS_PER_ROW
    max_field_chars: int = HARD_MAX_FIELD_CHARS
    max_record_chars: int = HARD_MAX_RECORD_CHARS
    max_expansion_ratio: float = HARD_MAX_EXPANSION_RATIO
    max_decode_seconds: float = DEFAULT_DECODE_SECONDS

    def __post_init__(self) -> None:
        hard = {
            "max_compressed_bytes": HARD_MAX_COMPRESSED_BYTES,
            "max_uncompressed_bytes": HARD_MAX_UNCOMPRESSED_BYTES,
            "max_rows": HARD_MAX_ROWS,
            "max_stations": HARD_MAX_STATIONS,
            "max_facts": HARD_MAX_FACTS,
            "max_fact_bytes": HARD_MAX_FACT_BYTES,
            "max_weather_tokens_per_row": HARD_MAX_WEATHER_TOKENS_PER_ROW,
            "max_remark_tokens_per_row": HARD_MAX_REMARK_TOKENS_PER_ROW,
            "max_field_chars": HARD_MAX_FIELD_CHARS,
            "max_record_chars": HARD_MAX_RECORD_CHARS,
        }
        for name, ceiling in hard.items():
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= ceiling:
                raise ValueError(f"{name} is outside the hard METAR decoder bound")
        for name, ceiling in (
            ("max_expansion_ratio", HARD_MAX_EXPANSION_RATIO),
            ("max_decode_seconds", DEFAULT_DECODE_SECONDS),
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or not 0.0 < float(value) <= ceiling
            ):
                raise ValueError(f"{name} is outside the hard METAR decoder bound")

    def to_mapping(self) -> dict[str, int | float]:
        return {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
        }


@dataclass(frozen=True, slots=True)
class MetarGroundDecodeResult:
    capture_id: str
    decoder_identity: str
    decoder_version: str
    config_sha256: str
    csv_row_count: int
    decoded_report_count: int
    out_of_scope_report_count: int
    rejected_report_count: int
    exact_duplicate_row_count: int
    revision_identity_count: int
    fact_count: int
    unresolved_prrrr_count: int
    quantitative_interval_fact_count: int
    receipts: tuple[GroundObservationReceipt, ...]

    @property
    def operational_admission_eligible(self) -> bool:
        return False

    @property
    def admission_status(self) -> str:
        return ADMISSION_STATUS


@dataclass(frozen=True, slots=True)
class MetarGroundTargetScope:
    """Bounded target used only to select canonical rows from global raw."""

    target_id: str
    latitude: float
    longitude: float
    radius_km: float
    available: bool = True

    def __post_init__(self) -> None:
        if (
            not isinstance(self.target_id, str)
            or not self.target_id
            or len(self.target_id) > 160
        ):
            raise ValueError("target_id is outside the METAR scope bound")
        for name, value, minimum, maximum in (
            ("latitude", self.latitude, -90.0, 90.0),
            ("longitude", self.longitude, -180.0, 180.0),
            ("radius_km", self.radius_km, 0.1, HARD_MAX_TARGET_RADIUS_KM),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or not minimum <= float(value) <= maximum
            ):
                raise ValueError(f"{name} is outside the METAR scope bound")
        if type(self.available) is not bool:
            raise TypeError("available must be boolean")


@runtime_checkable
class _GroundWriter(Protocol):
    def commit_batch(
        self,
        facts: Sequence[GroundObservationFact],
        *,
        deadline_at: float | None = None,
    ) -> tuple[GroundObservationReceipt, ...]: ...


@dataclass(frozen=True, slots=True)
class _SourceContext:
    capture_id: str
    capture_receipt_sha256: str
    members: tuple[GroundCaptureMemberBinding, ...]
    source_member: GroundCaptureMemberBinding
    product: str
    ingested_at: float
    config_sha256: str


@dataclass(frozen=True, slots=True)
class _Row:
    ordinal: int
    fields: tuple[str, ...]
    observed_at: float
    raw_sha256: str


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _config_sha256(limits: MetarGroundDecodeLimits) -> str:
    document = {
        "decoder_identity": DECODER_IDENTITY,
        "decoder_version": DECODER_VERSION,
        "header": list(native.METAR_EXPECTED_HEADER),
        "limits": limits.to_mapping(),
        "present_weather_semantics": "exact_categorical_token_no_rain_or_dry_claim",
        "prrrr_semantics": "coded_since_previous_metar_interval_start_unavailable",
        "six_rrrr_semantics": "3h_at_03_09_15_21Z_6h_at_00_06_12_18Z_metar_only",
        "seven_rrrr_semantics": "exact_previous_24h",
        "freshness": {
            "future_seconds": MAX_FUTURE_SECONDS,
            "stale_seconds": MAX_STALE_SECONDS,
            "non_current_value_state": "unknown",
        },
    }
    return hashlib.sha256(_canonical_json(document)).hexdigest()


DEFAULT_CONFIG_SHA256 = _config_sha256(MetarGroundDecodeLimits())


def _deadline(
    limits: MetarGroundDecodeLimits,
    deadline_at: float | None,
    monotonic: Callable[[], float],
) -> float:
    now = float(monotonic())
    if not math.isfinite(now):
        raise MetarGroundDeadlineError("monotonic clock is invalid")
    own = now + limits.max_decode_seconds
    if deadline_at is None:
        return own
    if isinstance(deadline_at, bool) or not isinstance(deadline_at, (int, float)):
        raise ValueError("deadline_at must be an absolute monotonic timestamp")
    supplied = float(deadline_at)
    if not math.isfinite(supplied):
        raise ValueError("deadline_at must be finite")
    return min(own, supplied)


def _check_deadline(deadline: float, monotonic: Callable[[], float]) -> None:
    if float(monotonic()) >= deadline:
        raise MetarGroundDeadlineError("absolute METAR decode deadline elapsed")


def _iter_gzip_chunks(
    payload: bytes,
    *,
    limits: MetarGroundDecodeLimits,
    deadline: float,
    monotonic: Callable[[], float],
) -> Iterator[bytes]:
    if not payload:
        raise MetarGroundStructureError("METAR gzip member is empty")
    if len(payload) > limits.max_compressed_bytes:
        raise MetarGroundCapacityError("METAR compressed-byte bound exceeded")
    expanded_limit = min(
        limits.max_uncompressed_bytes,
        max(1, math.floor(len(payload) * limits.max_expansion_ratio)),
    )
    decoder = zlib.decompressobj(zlib.MAX_WBITS | 16)
    pending = payload
    expanded = 0
    try:
        while pending:
            _check_deadline(deadline, monotonic)
            before = len(pending)
            decoded = decoder.decompress(
                pending,
                min(64 * 1024, expanded_limit - expanded + 1),
            )
            pending = decoder.unconsumed_tail
            if decoded:
                expanded += len(decoded)
                if expanded > expanded_limit:
                    raise MetarGroundCapacityError(
                        "METAR uncompressed-byte or expansion-ratio bound exceeded"
                    )
                yield decoded
            if decoder.eof:
                break
            if not pending:
                break
            if not decoded and len(pending) >= before:
                raise MetarGroundStructureError("METAR gzip made no progress")
    except zlib.error as exc:
        raise MetarGroundStructureError("METAR gzip member is invalid") from exc
    if not decoder.eof:
        raise MetarGroundStructureError("METAR gzip member is incomplete")
    if decoder.unused_data or decoder.unconsumed_tail or pending:
        raise MetarGroundStructureError(
            "METAR gzip contains a second member or trailing bytes"
        )


def _iter_csv_lines(
    payload: bytes,
    *,
    limits: MetarGroundDecodeLimits,
    deadline: float,
    monotonic: Callable[[], float],
) -> Iterator[str]:
    decoder = codecs.getincrementaldecoder("utf-8-sig")("strict")
    buffered = ""
    try:
        for chunk in _iter_gzip_chunks(
            payload, limits=limits, deadline=deadline, monotonic=monotonic
        ):
            text = decoder.decode(chunk, final=False)
            if "\x00" in text:
                raise MetarGroundStructureError("METAR CSV contains NUL")
            buffered += text
            while True:
                newline = buffered.find("\n")
                if newline < 0:
                    break
                line = buffered[: newline + 1]
                buffered = buffered[newline + 1 :]
                if len(line) > limits.max_record_chars:
                    raise MetarGroundCapacityError("METAR physical CSV line is too long")
                yield line
            if len(buffered) > limits.max_record_chars:
                raise MetarGroundCapacityError("METAR physical CSV line is too long")
        buffered += decoder.decode(b"", final=True)
    except UnicodeDecodeError as exc:
        raise MetarGroundStructureError("METAR CSV is not strict UTF-8") from exc
    if "\x00" in buffered:
        raise MetarGroundStructureError("METAR CSV contains NUL")
    if buffered:
        if len(buffered) > limits.max_record_chars:
            raise MetarGroundCapacityError("METAR physical CSV line is too long")
        yield buffered


class _BoundedLines:
    def __init__(self, values: Iterator[str], maximum: int) -> None:
        self._values = iter(values)
        self._maximum = maximum
        self._logical_chars = 0

    def __iter__(self):
        return self

    def __next__(self) -> str:
        value = next(self._values)
        self._logical_chars += len(value)
        if self._logical_chars > self._maximum:
            raise MetarGroundCapacityError("METAR logical CSV record is too long")
        return value

    def complete(self) -> None:
        self._logical_chars = 0


def _parse_timestamp(value: str) -> float | None:
    return native._observation_timestamp(value)  # exact native capture profile


def _strict_decimal(value: str) -> Decimal | None:
    if not value or value != value.strip() or _DECIMAL_RE.fullmatch(value) is None:
        return None
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        return None
    return parsed if parsed.is_finite() else None


def _known_weather_token(token: str) -> tuple[bool, bool]:
    """Return (known grammar, vicinity); never infer precipitation."""
    value = token
    if not value or len(value) > 32 or any(ord(ch) < 33 or ord(ch) > 126 for ch in value):
        return False, False
    if value[:1] in {"+", "-"}:
        value = value[1:]
        if value.startswith("VC"):
            return False, False
    vicinity = value.startswith("VC")
    if vicinity:
        value = value[2:]
    descriptor = value[:2] if value[:2] in _DESCRIPTORS else None
    if descriptor:
        value = value[2:]
    if not value:
        return (vicinity and descriptor in {"SH", "TS"}), vicinity
    if len(value) % 2:
        return False, vicinity
    return all(value[index:index + 2] in _PHENOMENA for index in range(0, len(value), 2)), vicinity


def _capture_context(
    capture_id: str,
    raw_store: RawArtifactStore,
    limits: MetarGroundDecodeLimits,
    config_sha256: str,
    deadline: float,
    monotonic: Callable[[], float],
) -> tuple[_SourceContext, bytes]:
    _check_deadline(deadline, monotonic)
    bundle = raw_store.load_capture_bundle_bytes(capture_id)
    _check_deadline(deadline, monotonic)
    if bundle is None:
        raise MetarGroundCaptureMissingError("verified schema-3 METAR capture is missing")
    capture, receipts, payloads = bundle
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.capture_schema != RAW_ARTIFACT_CAPTURE_SCHEMA
        or capture.capture_receipt_schema != RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA
        or capture.complete is not True
        or capture.capture_id != capture_id
        or capture.provider != "metar"
        or capture.product != native.METAR_CAPTURE_PRODUCT
        or capture.product_timestamp is not None
        or set(receipts) != {"csv_gzip"}
        or set(payloads) != {"csv_gzip"}
        or len(capture.members) != 1
    ):
        raise MetarGroundStructureError("capture is not the exact complete METAR profile")
    receipt = receipts["csv_gzip"]
    payload = payloads["csv_gzip"]
    if len(payload) > limits.max_compressed_bytes:
        raise MetarGroundCapacityError("METAR compressed-byte bound exceeded")
    suffix = ":capture"
    if not capture.fetch_id.endswith(suffix) or not capture.fetch_id[:-len(suffix)]:
        raise MetarGroundStructureError("METAR capture fetch identity is invalid")
    job_id = capture.fetch_id[:-len(suffix)]
    expected_provenance = {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": native.METAR_RAW_COLLECTOR,
        "collector_version": native.METAR_RAW_COLLECTOR_VERSION,
        "structure_inspector_version": native.METAR_STRUCTURE_INSPECTOR_VERSION,
        "upstream_filename": "metars.cache.csv.gz",
        "declared_semantic_class": native.METAR_SEMANTIC_CLASS,
        "coverage": "worldwide_current_reports",
        "provider_update_cadence_seconds": 60,
        "meteorological_interpretation_verified_after_decode": False,
    }
    expected_upstream = (
        "aviationweather/metars.cache.csv.gz"
        f"|content_sha256={receipt.content_sha256}"
    )
    expected_capture_upstream = (
        f"{native.METAR_CAPTURE_PRODUCT}|content_sha256={receipt.content_sha256}"
    )
    member = capture.members[0]
    if (
        member.role != "csv_gzip"
        or member.receipt_id != receipt.receipt_id
        or member.content_sha256 != receipt.content_sha256
        or member.receipt_sha256 != receipt.receipt_sha256
        or member.received_at != receipt.received_at
        or receipt.provider != "metar"
        or receipt.product != native.METAR_GZIP_PRODUCT
        or receipt.fetch_id != f"{job_id}:csv_gzip"
        or receipt.upstream_id != expected_upstream
        or receipt.provenance != expected_provenance
        or receipt.content_type not in native.METAR_ALLOWED_CONTENT_TYPES
        or capture.upstream_capture_id != expected_capture_upstream
        or receipt.size_bytes != len(payload)
        or not math.isfinite(capture.created_at)
    ):
        raise MetarGroundStructureError("METAR capture member identity is invalid")
    binding = GroundCaptureMemberBinding(
        member_id=member.member_id,
        role=member.role,
        receipt_id=member.receipt_id,
        content_sha256=member.content_sha256,
        receipt_sha256=member.receipt_sha256,
        received_at=member.received_at,
    )
    return _SourceContext(
        capture_id=capture.capture_id,
        capture_receipt_sha256=capture.capture_receipt_sha256,
        members=(binding,),
        source_member=binding,
        product=capture.product,
        ingested_at=max(float(capture.created_at), binding.received_at),
        config_sha256=config_sha256,
    ), payload


def _parse_rows(
    payload: bytes,
    *,
    limits: MetarGroundDecodeLimits,
    deadline: float,
    monotonic: Callable[[], float],
) -> tuple[list[_Row], int, int]:
    lines = _BoundedLines(
        _iter_csv_lines(payload, limits=limits, deadline=deadline, monotonic=monotonic),
        limits.max_record_chars,
    )
    reader = csv.reader(lines, strict=True)
    try:
        header = tuple(next(reader))
    except StopIteration as exc:
        raise MetarGroundStructureError("METAR CSV header is missing") from exc
    except csv.Error as exc:
        raise MetarGroundStructureError("METAR CSV quoting is malformed") from exc
    lines.complete()
    if header != native.METAR_EXPECTED_HEADER:
        raise MetarGroundStructureError("METAR CSV is not the exact 44-column schema")
    valid: list[_Row] = []
    rejected = 0
    stations: set[str] = set()
    row_count = 0
    while True:
        _check_deadline(deadline, monotonic)
        try:
            values = next(reader)
        except StopIteration:
            break
        except csv.Error as exc:
            raise MetarGroundStructureError("METAR CSV quoting is malformed") from exc
        lines.complete()
        row_count += 1
        if row_count > limits.max_rows:
            raise MetarGroundCapacityError("METAR row bound exceeded")
        if len(values) != 44:
            raise MetarGroundStructureError("METAR CSV row shifted from 44 columns")
        if any(len(value) > limits.max_field_chars for value in values):
            raise MetarGroundCapacityError("METAR field bound exceeded")
        fields = tuple(values)
        if (
            len(fields[_OBSERVED]) > 64
            or len(fields[_WX_STRING]) > MAX_QC_LEXEME_CHARS
            or any(
                len(fields[index]) > 128
                for index in (
                    _LATITUDE,
                    _LONGITUDE,
                    _PRECIP_IN,
                    _PCP3_IN,
                    _PCP6_IN,
                    _PCP24_IN,
                    _ELEVATION,
                )
            )
            or any(len(fields[index]) > 64 for index, _ in _FLAG_COLUMNS)
        ):
            raise MetarGroundCapacityError(
                "METAR canonical-QC lexeme bound exceeded"
            )
        station = fields[_STATION]
        observed_at = _parse_timestamp(fields[_OBSERVED])
        raw_text = fields[_RAW_TEXT]
        if (
            _STATION_RE.fullmatch(station) is None
            or observed_at is None
            or not raw_text.strip()
            or fields[_METAR_TYPE] not in {"METAR", "SPECI"}
            or any(ord(ch) < 32 for ch in raw_text)
        ):
            rejected += 1
            continue
        stations.add(station)
        if len(stations) > limits.max_stations:
            raise MetarGroundCapacityError("METAR station bound exceeded")
        valid.append(_Row(
            ordinal=row_count,
            fields=fields,
            observed_at=observed_at,
            raw_sha256=hashlib.sha256(raw_text.encode("utf-8")).hexdigest(),
        ))
    if row_count == 0 or not valid:
        raise MetarGroundStructureError("METAR CSV has no decodable reports")
    return valid, row_count, rejected


def _qc(flag: str, value: str | None = None, text: str | None = None) -> ProviderQcFact:
    return ProviderQcFact(flag=flag, value=value, text=text)


def _time_state(row: _Row, received_at: float) -> str:
    if row.observed_at > received_at + MAX_FUTURE_SECONDS:
        return "future"
    if row.observed_at < received_at - MAX_STALE_SECONDS:
        return "stale"
    return "current"


def _station_site(row: _Row) -> tuple[GroundStationSite, tuple[ProviderQcFact, ...]]:
    fields = row.fields
    latitude = _strict_decimal(fields[_LATITUDE])
    longitude = _strict_decimal(fields[_LONGITUDE])
    position_known = (
        latitude is not None
        and longitude is not None
        and Decimal("-90") <= latitude <= Decimal("90")
        and Decimal("-180") <= longitude <= Decimal("180")
    )
    elevation = _strict_decimal(fields[_ELEVATION])
    station = GroundStationSite(
        provider_station_id=fields[_STATION],
        site_id=f"metar:{fields[_STATION]}",
        network="aviationweather_metar",
        station_name=None,
        position_state="known" if position_known else "unknown",
        latitude_decimal=fields[_LATITUDE] if position_known else None,
        longitude_decimal=fields[_LONGITUDE] if position_known else None,
        elevation_state="known" if elevation is not None else "unknown",
        elevation_m_decimal=fields[_ELEVATION] if elevation is not None else None,
    )
    return station, (
        _qc("awc.csv.latitude_lexeme", fields[_LATITUDE]),
        _qc("awc.csv.longitude_lexeme", fields[_LONGITUDE]),
        _qc("awc.csv.elevation_m_lexeme", fields[_ELEVATION]),
        _qc("decoder.position_state", station.position_state),
        _qc("decoder.elevation_state", station.elevation_state),
    )


def _haversine_km(
    latitude_a: float,
    longitude_a: float,
    latitude_b: float,
    longitude_b: float,
) -> float:
    lat_a = math.radians(latitude_a)
    lat_b = math.radians(latitude_b)
    delta_lat = lat_b - lat_a
    delta_lon = math.radians(longitude_b - longitude_a)
    value = (
        math.sin(delta_lat / 2.0) ** 2
        + math.cos(lat_a) * math.cos(lat_b) * math.sin(delta_lon / 2.0) ** 2
    )
    return 6371.0088 * 2.0 * math.asin(min(1.0, math.sqrt(value)))


def _row_is_in_scope(
    row: _Row,
    scopes: tuple[MetarGroundTargetScope, ...],
) -> bool:
    latitude = _strict_decimal(row.fields[_LATITUDE])
    longitude = _strict_decimal(row.fields[_LONGITUDE])
    if (
        latitude is None
        or longitude is None
        or not Decimal("-90") <= latitude <= Decimal("90")
        or not Decimal("-180") <= longitude <= Decimal("180")
    ):
        return False
    station_latitude = float(latitude)
    station_longitude = float(longitude)
    return any(
        scope.available
        and _haversine_km(
            station_latitude,
            station_longitude,
            float(scope.latitude),
            float(scope.longitude),
        ) <= float(scope.radius_km)
        for scope in scopes
    )


def _common_qc(
    row: _Row,
    *,
    time_state: str,
    duplicate_count: int,
    revision_count: int,
    revision_rank: int,
    limits: MetarGroundDecodeLimits,
) -> tuple[ProviderQcFact, ...]:
    fields = row.fields
    values = [
        _qc("awc.csv.row_ordinal", str(row.ordinal)),
        _qc("awc.raw_metar_sha256", row.raw_sha256),
        _qc("awc.csv.station_id_lexeme", fields[_STATION]),
        _qc("awc.csv.observation_time_lexeme", fields[_OBSERVED]),
        _qc("awc.csv.metar_type_lexeme", fields[_METAR_TYPE]),
        _qc("awc.csv.wx_string_lexeme", fields[_WX_STRING]),
        _qc("decoder.time_state", time_state),
        _qc("decoder.exact_duplicate_row_count", str(duplicate_count)),
        _qc("decoder.revision_group_size", str(revision_count)),
        _qc("decoder.revision_rank_no_last_wins", str(revision_rank)),
    ]
    values.extend(_qc(flag, fields[index]) for index, flag in _FLAG_COLUMNS)
    raw_tokens = fields[_RAW_TEXT].split()
    if len(raw_tokens) > limits.max_remark_tokens_per_row:
        raise MetarGroundCapacityError("METAR remark token bound exceeded")
    provider_flags = [
        token for token in raw_tokens if _PROVIDER_FLAG_RE.fullmatch(token)
    ]
    if len(provider_flags) > HARD_MAX_PROVIDER_FLAGS_PER_ROW:
        raise MetarGroundCapacityError("METAR provider-flag bound exceeded")
    for token in provider_flags:
        values.append(_qc("metar.provider_flag", token))
    return tuple(values)


def _fact(
    context: _SourceContext,
    row: _Row,
    station: GroundStationSite,
    *,
    locator_suffix: str,
    interval_start: float,
    interval_end: float,
    measurement: GroundMeasurementValue,
    provider_qc: tuple[ProviderQcFact, ...],
) -> GroundObservationFact:
    locator = (
        f"csv_gzip:data_row={row.ordinal};raw_text_sha256={row.raw_sha256};"
        f"{locator_suffix}"
    )
    return parse_ground_observation(GroundObservationFact(
        capture_id=context.capture_id,
        capture_receipt_sha256=context.capture_receipt_sha256,
        members=context.members,
        source_member_role=context.source_member.role,
        source_member_content_sha256=context.source_member.content_sha256,
        source_member_receipt_sha256=context.source_member.receipt_sha256,
        source_record_locator=locator,
        source_record_sha256=row.raw_sha256,
        decoder_identity=DECODER_IDENTITY,
        decoder_version=DECODER_VERSION,
        config_sha256=context.config_sha256,
        provider="metar",
        product=context.product,
        station=station,
        observed_at=row.observed_at,
        representative_interval_start=interval_start,
        representative_interval_end=interval_end,
        received_at=context.source_member.received_at,
        ingested_at=context.ingested_at,
        measurement=measurement,
        provider_qc=provider_qc,
    ).to_mapping())


def _raw_remark_fact(
    context: _SourceContext,
    row: _Row,
    station: GroundStationSite,
    common: tuple[ProviderQcFact, ...],
    *,
    variable: str,
    token: str | None,
    suffix: str,
    time_state: str,
    extra_qc: tuple[ProviderQcFact, ...],
) -> GroundObservationFact:
    state = "missing" if token is None and time_state == "current" else (
        "observed" if token is not None and time_state == "current" else "unknown"
    )
    return _fact(
        context,
        row,
        station,
        locator_suffix=suffix,
        interval_start=row.observed_at,
        interval_end=row.observed_at,
        measurement=GroundMeasurementValue(
            variable=variable,
            state=state,  # type: ignore[arg-type]
            representation="text" if state == "observed" else "none",
            exact_value=token if state == "observed" else None,
            unit="metar_code",
            censor_limit=None,
        ),
        provider_qc=common + extra_qc,
    )


def _weather_facts(
    context: _SourceContext,
    row: _Row,
    station: GroundStationSite,
    common: tuple[ProviderQcFact, ...],
    *,
    time_state: str,
    limits: MetarGroundDecodeLimits,
) -> list[GroundObservationFact]:
    lexeme = row.fields[_WX_STRING]
    tokens = lexeme.split()
    if len(tokens) > limits.max_weather_tokens_per_row:
        raise MetarGroundCapacityError("METAR present-weather token bound exceeded")
    unavailable = (
        "PWINO" in row.fields[_RAW_TEXT].split()
        or row.fields[_PRESENT_WEATHER_OFF].upper() == "TRUE"
        or row.fields[_NO_SIGNAL].upper() == "TRUE"
    )
    if not tokens:
        state = "unknown" if time_state != "current" or unavailable else "missing"
        return [_fact(
            context,
            row,
            station,
            locator_suffix="field=wx_string;token=missing",
            interval_start=row.observed_at,
            interval_end=row.observed_at,
            measurement=GroundMeasurementValue(
                variable="present_weather",
                state=state,  # type: ignore[arg-type]
                representation="none",
                exact_value=None,
                unit="metar_code",
                censor_limit=None,
            ),
            provider_qc=common + (
                _qc("metar.present_weather_group_state", "absent_from_report"),
                _qc("metar.absence_semantics", "not_a_dry_claim"),
            ),
        )]
    result: list[GroundObservationFact] = []
    for ordinal, token in enumerate(tokens):
        known, vicinity = _known_weather_token(token)
        observed = time_state == "current" and known
        extra = [
            _qc("awc.present_weather_token", token),
            _qc("metar.present_weather_token_profile", "recognized" if known else "unknown"),
        ]
        if vicinity:
            extra.append(_qc("metar.station_relation", "vicinity_not_at_station"))
        result.append(_fact(
            context,
            row,
            station,
            locator_suffix=f"field=wx_string;token={ordinal}",
            interval_start=row.observed_at,
            interval_end=row.observed_at,
            measurement=GroundMeasurementValue(
                variable="present_weather",
                state="observed" if observed else "unknown",
                representation="text" if observed else "none",
                exact_value=token if observed else None,
                unit="metar_code",
                censor_limit=None,
            ),
            provider_qc=common + tuple(extra),
        ))
    return result


def _precipitation_facts(
    context: _SourceContext,
    row: _Row,
    station: GroundStationSite,
    common: tuple[ProviderQcFact, ...],
    *,
    time_state: str,
) -> tuple[list[GroundObservationFact], int, int]:
    fields = row.fields
    raw_tokens = fields[_RAW_TEXT].split()
    try:
        remarks = raw_tokens[raw_tokens.index("RMK") + 1 :]
    except ValueError:
        remarks = []
    p_tokens = [token for token in remarks if _PRRRR_RE.fullmatch(token)]
    six_tokens = [token for token in remarks if _SIX_RE.fullmatch(token)]
    seven_tokens = [token for token in remarks if _SEVEN_RE.fullmatch(token)]
    facts: list[GroundObservationFact] = []
    unresolved_prrrr = 0
    quantitative = 0

    # The exact prior report timestamp is absent from this current-cache
    # capture. Prrrr is therefore retained but never assigned a fake hour.
    if not p_tokens:
        facts.append(_raw_remark_fact(
            context, row, station, common,
            variable="metar_hourly_precipitation_remark",
            token=None,
            suffix="field=raw_text;remark=Prrrr;token=missing",
            time_state=time_state,
            extra_qc=(
                _qc("awc.csv.precip_in_lexeme", fields[_PRECIP_IN]),
                _qc("metar.prrrr_interval_state", "previous_report_time_not_in_capture"),
                _qc("metar.prrrr_omission_semantics", "not_zero_not_dry"),
            ),
        ))
    else:
        unresolved_prrrr += len(p_tokens)
        for ordinal, token in enumerate(p_tokens):
            amount_state = "unknown" if token.endswith("////") else (
                "trace" if token == "P0000" else "reported"
            )
            facts.append(_raw_remark_fact(
                context, row, station, common,
                variable="metar_hourly_precipitation_remark",
                token=token,
                suffix=f"field=raw_text;remark=Prrrr;token={ordinal}",
                time_state=time_state,
                extra_qc=(
                    _qc("metar.prrrr_token", token),
                    _qc("metar.prrrr_reported_amount_state", amount_state),
                    _qc("awc.csv.precip_in_lexeme", fields[_PRECIP_IN]),
                    _qc("metar.prrrr_interval_state", "previous_report_time_not_in_capture"),
                    _qc("metar.prrrr_quantitative_fact", "withheld_no_interval_start"),
                    _qc("metar.prrrr_duplicate_group_count", str(len(p_tokens))),
                ),
            ))

    try:
        observed_hour = datetime.fromtimestamp(row.observed_at, timezone.utc).hour
    except (OverflowError, OSError, ValueError):
        observed_hour = -1
    duration_hours = (
        3 if observed_hour in {3, 9, 15, 21}
        else 6 if observed_hour in {0, 6, 12, 18}
        else None
    )
    if six_tokens:
        ambiguous = len(six_tokens) != 1 or fields[_METAR_TYPE] != "METAR" or duration_hours is None
        token = six_tokens[0]
        relevant_index = _PCP3_IN if duration_hours == 3 else _PCP6_IN
        other_index = _PCP6_IN if duration_hours == 3 else _PCP3_IN
        csv_lexeme = fields[relevant_index] if duration_hours else ""
        digits = token[1:]
        parsed = _strict_decimal(csv_lexeme)
        expected = None if not digits.isdigit() else Decimal(int(digits)) / Decimal(100)
        exact_match = parsed is not None and expected is not None and parsed == expected
        if duration_hours is not None and fields[other_index]:
            ambiguous = True
        if duration_hours is not None and row.observed_at < duration_hours * 3600.0:
            ambiguous = True
        if not ambiguous and (digits == "////" or exact_match):
            state = "unknown" if time_state != "current" or digits == "////" else (
                "unknown" if digits == "0000" else "observed"
            )
            facts.append(_fact(
                context, row, station,
                locator_suffix="field=raw_text;remark=6RRRR;token=0",
                interval_start=row.observed_at - duration_hours * 3600.0,
                interval_end=row.observed_at,
                measurement=GroundMeasurementValue(
                    variable="precipitation_accumulation",
                    state=state,  # type: ignore[arg-type]
                    representation="decimal" if state == "observed" else "none",
                    exact_value=csv_lexeme if state == "observed" else None,
                    unit="in",
                    censor_limit=None,
                ),
                provider_qc=common + (
                    _qc("metar.6rrrr_token", token),
                    _qc("metar.6rrrr_interval_hours", str(duration_hours)),
                    _qc("awc.csv.accumulation_lexeme", csv_lexeme),
                    _qc("metar.6rrrr_zero_semantics", "origin_not_certified" if digits == "0000" else "not_applicable"),
                ),
            ))
            quantitative += 1
        else:
            for ordinal, item in enumerate(six_tokens):
                facts.append(_raw_remark_fact(
                    context, row, station, common,
                    variable="metar_3_or_6_hour_precipitation_remark",
                    token=item,
                    suffix=f"field=raw_text;remark=6RRRR;token={ordinal}",
                    time_state=time_state,
                    extra_qc=(
                        _qc("metar.6rrrr_decode_state", "ambiguous_withheld"),
                        _qc("awc.csv.pcp3hr_in_lexeme", fields[_PCP3_IN]),
                        _qc("awc.csv.pcp6hr_in_lexeme", fields[_PCP6_IN]),
                    ),
                ))
    elif fields[_PCP3_IN] or fields[_PCP6_IN]:
        facts.append(_raw_remark_fact(
            context, row, station, common,
            variable="metar_3_or_6_hour_precipitation_remark",
            token=None,
            suffix="field=raw_text;remark=6RRRR;token=missing",
            time_state="future",  # force unknown: decoded column lacks raw locator
            extra_qc=(
                _qc("metar.6rrrr_decode_state", "csv_value_without_raw_group"),
                _qc("awc.csv.pcp3hr_in_lexeme", fields[_PCP3_IN]),
                _qc("awc.csv.pcp6hr_in_lexeme", fields[_PCP6_IN]),
            ),
        ))

    if seven_tokens:
        token = seven_tokens[0]
        digits = token[1:]
        parsed = _strict_decimal(fields[_PCP24_IN])
        expected = None if not digits.isdigit() else Decimal(int(digits)) / Decimal(100)
        exact_match = parsed is not None and expected is not None and parsed == expected
        if (
            len(seven_tokens) == 1
            and row.observed_at >= 24 * 3600.0
            and (digits == "////" or (digits != "0000" and exact_match))
        ):
            state = "unknown" if time_state != "current" or digits == "////" else "observed"
            facts.append(_fact(
                context, row, station,
                locator_suffix="field=raw_text;remark=7RRRR;token=0",
                interval_start=row.observed_at - 24 * 3600.0,
                interval_end=row.observed_at,
                measurement=GroundMeasurementValue(
                    variable="precipitation_accumulation",
                    state=state,  # type: ignore[arg-type]
                    representation="decimal" if state == "observed" else "none",
                    exact_value=fields[_PCP24_IN] if state == "observed" else None,
                    unit="in",
                    censor_limit=None,
                ),
                provider_qc=common + (
                    _qc("metar.7rrrr_token", token),
                    _qc("metar.7rrrr_interval_hours", "24"),
                    _qc("awc.csv.pcp24hr_in_lexeme", fields[_PCP24_IN]),
                ),
            ))
            quantitative += 1
        else:
            for ordinal, item in enumerate(seven_tokens):
                facts.append(_raw_remark_fact(
                    context, row, station, common,
                    variable="metar_24_hour_precipitation_remark",
                    token=item,
                    suffix=f"field=raw_text;remark=7RRRR;token={ordinal}",
                    time_state=time_state,
                    extra_qc=(
                        _qc("metar.7rrrr_decode_state", "ambiguous_withheld"),
                        _qc("awc.csv.pcp24hr_in_lexeme", fields[_PCP24_IN]),
                    ),
                ))
    return facts, unresolved_prrrr, quantitative


def decode_metar_ground_capture(
    capture_id: str,
    *,
    raw_store: RawArtifactStore,
    evidence_store: CanonicalGroundEvidenceStore | _GroundWriter,
    limits: MetarGroundDecodeLimits | None = None,
    target_scopes: tuple[MetarGroundTargetScope, ...] | None = None,
    deadline_at: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> MetarGroundDecodeResult:
    """Decode and idempotently commit bounded, unadmitted source facts."""
    if not isinstance(capture_id, str) or _CAPTURE_ID_RE.fullmatch(capture_id) is None:
        raise ValueError("capture_id is not a schema-3 capture identity")
    if not isinstance(raw_store, RawArtifactStore):
        raise TypeError("raw_store must be RawArtifactStore")
    if not isinstance(evidence_store, _GroundWriter):
        raise TypeError("evidence_store must implement canonical batch commit")
    active_limits = limits or MetarGroundDecodeLimits()
    if not isinstance(active_limits, MetarGroundDecodeLimits):
        raise TypeError("limits must be MetarGroundDecodeLimits")
    if target_scopes is not None:
        if not isinstance(target_scopes, tuple):
            raise TypeError("target_scopes must be a concrete tuple")
        if len(target_scopes) > HARD_MAX_TARGET_SCOPES:
            raise MetarGroundCapacityError("METAR target-scope bound exceeded")
        if any(
            not isinstance(scope, MetarGroundTargetScope)
            for scope in target_scopes
        ):
            raise TypeError("target_scopes contains an invalid scope")
    absolute_deadline = _deadline(active_limits, deadline_at, monotonic)
    _check_deadline(absolute_deadline, monotonic)
    config_sha256 = _config_sha256(active_limits)
    context, payload = _capture_context(
        capture_id, raw_store, active_limits, config_sha256,
        absolute_deadline, monotonic,
    )
    rows, csv_row_count, rejected = _parse_rows(
        payload, limits=active_limits, deadline=absolute_deadline, monotonic=monotonic
    )
    out_of_scope = 0
    if target_scopes is not None:
        scoped_rows: list[_Row] = []
        for row in rows:
            _check_deadline(absolute_deadline, monotonic)
            if _row_is_in_scope(row, target_scopes):
                scoped_rows.append(row)
            else:
                out_of_scope += 1
        rows = scoped_rows

    # Collapse only byte-for-byte-equivalent decoded 44-field rows. Distinct
    # revisions sharing station/time are all retained and explicitly flagged.
    identities: dict[tuple[str, str], dict[tuple[str, ...], list[_Row]]] = {}
    for row in rows:
        identity = (row.fields[_STATION], row.fields[_OBSERVED])
        identities.setdefault(identity, {}).setdefault(row.fields, []).append(row)
    unique_rows: list[tuple[_Row, int, int, int]] = []
    duplicate_rows = 0
    revision_identities = 0
    for variants in identities.values():
        ordered = sorted(
            ((min(items, key=lambda item: item.ordinal), len(items)) for items in variants.values()),
            key=lambda item: item[0].ordinal,
        )
        if len(ordered) > 1:
            revision_identities += 1
        for rank, (row, duplicate_count) in enumerate(ordered):
            duplicate_rows += duplicate_count - 1
            unique_rows.append((row, duplicate_count, len(ordered), rank))
    unique_rows.sort(key=lambda item: item[0].ordinal)

    facts: list[GroundObservationFact] = []
    fact_bytes = 0
    unresolved_prrrr = 0
    quantitative = 0
    for row, duplicate_count, revision_count, revision_rank in unique_rows:
        _check_deadline(absolute_deadline, monotonic)
        station, site_qc = _station_site(row)
        state = _time_state(row, context.source_member.received_at)
        common = _common_qc(
            row,
            time_state=state,
            duplicate_count=duplicate_count,
            revision_count=revision_count,
            revision_rank=revision_rank,
            limits=active_limits,
        ) + site_qc
        row_facts = _weather_facts(
            context, row, station, common, time_state=state, limits=active_limits
        )
        precip_facts, unresolved, intervals = _precipitation_facts(
            context, row, station, common, time_state=state
        )
        row_facts.extend(precip_facts)
        unresolved_prrrr += unresolved
        quantitative += intervals
        if len(facts) + len(row_facts) > active_limits.max_facts:
            raise MetarGroundCapacityError("METAR fact bound exceeded")
        for fact in row_facts:
            fact_bytes += len(_canonical_json(fact.to_mapping()))
            if fact_bytes > active_limits.max_fact_bytes:
                raise MetarGroundCapacityError("METAR aggregate fact-byte bound exceeded")
        facts.extend(row_facts)

    # Parsing, schema validation, revision handling and all capacity checks are
    # complete before the one atomic append. The store reverifies this exact
    # capture once and commits the full bounded fact sequence in one SQLite
    # transaction; no durable prefix can survive a failed decode.
    _check_deadline(absolute_deadline, monotonic)
    receipts = evidence_store.commit_batch(
        tuple(facts), deadline_at=absolute_deadline
    )
    _check_deadline(absolute_deadline, monotonic)
    return MetarGroundDecodeResult(
        capture_id=capture_id,
        decoder_identity=DECODER_IDENTITY,
        decoder_version=DECODER_VERSION,
        config_sha256=config_sha256,
        csv_row_count=csv_row_count,
        decoded_report_count=len(unique_rows),
        out_of_scope_report_count=out_of_scope,
        rejected_report_count=rejected,
        exact_duplicate_row_count=duplicate_rows,
        revision_identity_count=revision_identities,
        fact_count=len(receipts),
        unresolved_prrrr_count=unresolved_prrrr,
        quantitative_interval_fact_count=quantitative,
        receipts=receipts,
    )


__all__ = [
    "DECODER_IDENTITY",
    "DECODER_VERSION",
    "DEFAULT_CONFIG_SHA256",
    "MetarGroundCapacityError",
    "MetarGroundCaptureMissingError",
    "MetarGroundDeadlineError",
    "MetarGroundDecodeLimits",
    "MetarGroundDecodeResult",
    "MetarGroundTargetScope",
    "MetarGroundEvidenceError",
    "MetarGroundStructureError",
    "decode_metar_ground_capture",
]
