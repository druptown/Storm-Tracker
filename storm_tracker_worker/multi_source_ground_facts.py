"""Pure all-ground candidate slice for the closed multi-source cycle.

The module consumes only already committed canonical-ground receipts.  It
never opens their store, reads raw provider bytes, performs network access, or
turns a candidate into an operational rain/dry claim.  Every configured
ground workload and every requested target receives an explicit result.

METAR, Netatmo and Synoptic currently have canonical receipt producers.  KMI
AWS and AWN have exact raw capture paths but no certified canonical ground
decoder, so applicable targets remain explicitly blocked here.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any

from . import metar_ground_evidence
from . import metar_native
from . import netatmo_ground_decoder
from . import netatmo_native
from . import synoptic_ground_decoder
from . import synoptic_native
from .multi_source_operational_cycle import (
    CATALOG_BY_ID,
    CycleStatus,
    CycleTarget,
    EvidenceReference,
    GroundInput,
    SourceContribution,
    SourceKind,
    TargetApplicability,
    empty_source_input,
    evaluate_target_applicability,
)


GroundObservationFact = metar_ground_evidence.GroundObservationFact
GroundObservationReceipt = metar_ground_evidence.GroundObservationReceipt
parse_ground_observation = metar_ground_evidence.parse_ground_observation

GROUND_EVIDENCE_RECEIPT_SCHEMA = (
    "stv3-worker-canonical-ground-fact-receipt-v1"
)
_GROUND_OBSERVATION_IDENTITY_SCHEMA = (
    "stv3-worker-canonical-ground-observation-identity-v1"
)
_GROUND_FACT_IDENTITY_SCHEMA = "stv3-worker-canonical-ground-fact-identity-v1"

GROUND_SLICE_CONTRACT = "stv3-multi-source-ground-slice-v1"
GROUND_WORKLOAD_CONTRACT = "stv3-multi-source-ground-workload-v1"
GROUND_TARGET_CONTRACT = "stv3-multi-source-ground-target-v1"
GROUND_CANDIDATE_CONTRACT = "stv3-multi-source-ground-candidate-v1"
GROUND_TARGET_SCOPE_CONTRACT = "stv3-multi-source-ground-target-scope-v1"
GROUND_WORKLOAD_RECEIPT_SCHEMA = "stv3-multi-source-ground-workload-receipt-v1"
GROUND_RECEIPT_REFERENCE_CONTRACT = (
    "stv3-multi-source-ground-receipt-reference-v1"
)
GROUND_RECEIPT_SET_CONTRACT = "stv3-multi-source-ground-receipt-set-v1"
GROUND_FRESHNESS_POLICY_CONTRACT = "stv3-multi-source-ground-freshness-policy-v1"
GROUND_CONSENSUS_AUTHORITY_CONTRACT = "stv3-ground-consensus-authority-v1"

MAX_TARGETS = 64
MAX_RECEIPTS_PER_SOURCE = 2048
MAX_TOTAL_RECEIPTS = 4096
MAX_CANDIDATES_PER_TARGET = 512
MAX_TOTAL_TARGET_CANDIDATES = 8192
MAX_QC_FLAGS_PER_CANDIDATE = 64
MAX_SLICE_BYTES = 16 * 1024 * 1024
MAX_TARGET_RADIUS_KM = 250.0
MAX_FUTURE_SKEW_MS = 5 * 60 * 1000
MAX_TIMESTAMP_MS = 2**53 - 1

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/+\-$]{0,255}$")
_REASON_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_FACT_ID_RE = re.compile(r"^ground-fact-[0-9a-f]{64}$")
_OBSERVATION_KEY_RE = re.compile(r"^ground-observation-[0-9a-f]{64}$")
_CAPTURE_ID_RE = re.compile(r"^capture-[0-9a-f]{64}$")

GROUND_SOURCE_IDS = frozenset({
    "ground.awn.authorized_devices",
    "ground.kmi.aws_10min",
    "ground.metar.global",
    "ground.netatmo.region",
    "ground.synoptic.region",
})


class MultiSourceGroundError(ValueError):
    """A receipt, target contribution, or complete ground slice is invalid."""


class GroundTargetStatus(str, Enum):
    CURRENT = "current"
    STALE = "stale"
    ERROR = "error"
    BLOCKED = "blocked"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True, slots=True)
class GroundDecoderBinding:
    source_id: str
    fact_provider: str | None
    fact_product: str | None
    decoder_identity: str | None
    decoder_version: str | None
    semantics_profile: str
    maximum_age_ms: int | None
    blocked_reason: str | None

    @property
    def decoder_available(self) -> bool:
        return self.blocked_reason is None


GROUND_DECODER_BINDINGS = MappingProxyType({
    "ground.awn.authorized_devices": GroundDecoderBinding(
        source_id="ground.awn.authorized_devices",
        fact_provider=None,
        fact_product=None,
        decoder_identity=None,
        decoder_version=None,
        semantics_profile="raw_structure_only_rain_semantics_uncertified",
        maximum_age_ms=None,
        blocked_reason="canonical_ground_decoder_unavailable",
    ),
    "ground.kmi.aws_10min": GroundDecoderBinding(
        source_id="ground.kmi.aws_10min",
        fact_provider=None,
        fact_product=None,
        decoder_identity=None,
        decoder_version=None,
        semantics_profile="exact_raw_wfs_response_not_ground_decoded",
        maximum_age_ms=None,
        blocked_reason="canonical_ground_decoder_unavailable",
    ),
    "ground.metar.global": GroundDecoderBinding(
        source_id="ground.metar.global",
        fact_provider="metar",
        fact_product=metar_native.METAR_CAPTURE_PRODUCT,
        decoder_identity=metar_ground_evidence.DECODER_IDENTITY,
        decoder_version=metar_ground_evidence.DECODER_VERSION,
        semantics_profile="metar_exact_candidate_semantics",
        maximum_age_ms=int(metar_ground_evidence.MAX_STALE_SECONDS * 1000),
        blocked_reason=None,
    ),
    "ground.netatmo.region": GroundDecoderBinding(
        source_id="ground.netatmo.region",
        fact_provider="netatmo",
        fact_product=netatmo_native.NETATMO_CAPTURE_PRODUCT,
        decoder_identity=(
            netatmo_ground_decoder.NETATMO_GROUND_DECODER_IDENTITY
        ),
        decoder_version=(
            netatmo_ground_decoder.NETATMO_GROUND_DECODER_VERSION
        ),
        semantics_profile=netatmo_ground_decoder.NETATMO_SEMANTICS_STATUS,
        maximum_age_ms=10 * 60 * 1000,
        blocked_reason=None,
    ),
    "ground.synoptic.region": GroundDecoderBinding(
        source_id="ground.synoptic.region",
        fact_provider="synoptic",
        fact_product=synoptic_native.SYNOPTIC_CAPTURE_PRODUCT,
        decoder_identity=(
            synoptic_ground_decoder.SYNOPTIC_GROUND_DECODER_IDENTITY
        ),
        decoder_version=(
            synoptic_ground_decoder.SYNOPTIC_GROUND_DECODER_VERSION
        ),
        semantics_profile=synoptic_ground_decoder.SYNOPTIC_PROVISIONAL_STATUS,
        maximum_age_ms=10 * 60 * 1000,
        blocked_reason=None,
    ),
})

if (
    set(GROUND_DECODER_BINDINGS) != GROUND_SOURCE_IDS
    or {
        source_id
        for source_id, source in CATALOG_BY_ID.items()
        if source.kind is SourceKind.GROUND
    }
    != GROUND_SOURCE_IDS
):
    raise RuntimeError("all-ground binding driftte van gesloten acquisitioncatalogus")

GROUND_CATALOG_BY_ID = MappingProxyType({
    source_id: CATALOG_BY_ID[source_id]
    for source_id in sorted(GROUND_SOURCE_IDS)
})


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as err:
        raise MultiSourceGroundError("groundcontract is geen canonieke JSON") from err


def _ground_canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as err:
        raise MultiSourceGroundError("canonical ground receipt is ongeldig") from err


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _exact(value: object, keys: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise MultiSourceGroundError(f"{label} schema is niet exact")
    return value


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise MultiSourceGroundError(f"{label} is geen lowercase SHA-256")
    return value


def _prefixed_sha(value: object, pattern: re.Pattern[str], label: str) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise MultiSourceGroundError(f"{label} is ongeldig")
    return value


def _token(value: object, label: str, maximum: int = 256) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or _TOKEN_RE.fullmatch(value) is None
        or "://" in value
    ):
        raise MultiSourceGroundError(f"{label} is geen begrensd token")
    return value


def _verbatim(value: object, label: str, maximum: int = 1024) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise MultiSourceGroundError(f"{label} is geen begrensde tekst")
    return value


def _reason(value: object, label: str = "reason_code") -> str:
    if not isinstance(value, str) or _REASON_RE.fullmatch(value) is None:
        raise MultiSourceGroundError(f"{label} is geen geldige reason code")
    return value


def _integer(
    value: object,
    label: str,
    *,
    minimum: int = 0,
    maximum: int = MAX_TIMESTAMP_MS,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise MultiSourceGroundError(f"{label} ligt buiten de integergrens")
    return value


def _finite(
    value: object,
    label: str,
    *,
    minimum: float,
    maximum: float,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MultiSourceGroundError(f"{label} is geen getal")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise MultiSourceGroundError(f"{label} ligt buiten de grens")
    return result


def _timestamp_ms(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MultiSourceGroundError(f"{label} is geen timestamp")
    seconds = float(value)
    if not math.isfinite(seconds) or not 0.0 <= seconds <= MAX_TIMESTAMP_MS / 1000:
        raise MultiSourceGroundError(f"{label} ligt buiten timestampbereik")
    return int(round(seconds * 1000.0))


def _decimal(value: object, label: str, minimum: str, maximum: str) -> Decimal:
    result = _canonical_decimal(value, label)
    if not Decimal(minimum) <= result <= Decimal(maximum):
        raise MultiSourceGroundError(f"{label} ligt buiten bereik")
    return result


def _canonical_decimal(value: object, label: str) -> Decimal:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 128
    ):
        raise MultiSourceGroundError(f"{label} is geen canoniek exact decimaal")
    try:
        result = Decimal(value)
    except InvalidOperation as err:
        raise MultiSourceGroundError(
            f"{label} is geen canoniek exact decimaal"
        ) from err
    if not result.is_finite() or result.copy_abs() > Decimal("1e100"):
        raise MultiSourceGroundError(f"{label} is geen canoniek exact decimaal")
    return result


def _bounded_iterable(
    value: object,
    *,
    label: str,
    maximum: int,
) -> tuple[object, ...]:
    if (
        isinstance(value, (str, bytes, bytearray, Mapping))
        or not isinstance(value, Iterable)
    ):
        raise MultiSourceGroundError(f"{label} is geen begrensde iterable")
    iterator = iter(value)
    result: list[object] = []
    for _index in range(maximum + 1):
        try:
            item = next(iterator)
        except StopIteration:
            return tuple(result)
        except Exception as err:
            raise MultiSourceGroundError(f"{label} faalde tijdens lezen") from err
        result.append(item)
    raise MultiSourceGroundError(f"{label} overschrijdt limiet")


def _bounded_sequence_count(value: object, *, label: str, maximum: int) -> int:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise MultiSourceGroundError(f"{label} is geen begrensde sequence")
    try:
        count = len(value)
    except (OverflowError, TypeError, ValueError) as err:
        raise MultiSourceGroundError(f"{label} count is niet eindig") from err
    if not 0 <= count <= maximum:
        raise MultiSourceGroundError(f"{label} overschrijdt limiet")
    return count


def _validate_candidate_measurement(
    *,
    state: str,
    representation: str,
    exact_value: str | None,
    censor_limit: str | None,
) -> None:
    if state == "observed":
        if representation not in {"decimal", "text"} or exact_value is None:
            raise MultiSourceGroundError("observed candidate mist expliciete waarde")
        if not exact_value:
            raise MultiSourceGroundError("observed candidate heeft lege waarde")
        if representation == "decimal":
            _canonical_decimal(exact_value, "candidate exact_value")
        else:
            _verbatim(exact_value, "candidate exact_value")
        if censor_limit is not None:
            raise MultiSourceGroundError("observed candidate heeft censor_limit")
    elif state in {"censored_below", "censored_above"}:
        if representation != "none" or exact_value is not None:
            raise MultiSourceGroundError("censored candidate verzint exacte waarde")
        _canonical_decimal(censor_limit, "candidate censor_limit")
    elif state == "trace":
        if (
            representation != "none"
            or exact_value is not None
            or censor_limit is not None
        ):
            raise MultiSourceGroundError("trace candidate verzint exacte waarde")
    else:
        raise MultiSourceGroundError("candidate bevat geen bruikbare waardestatus")


def _candidate_semantics_profile(source_id: str, variable: str) -> str:
    binding = GROUND_DECODER_BINDINGS[source_id]
    if source_id == "ground.synoptic.region":
        if variable.startswith("precip_intervals_set_"):
            return synoptic_ground_decoder.SYNOPTIC_INTERVAL_SEMANTICS
        if variable.startswith("precip_accumulated_set_"):
            return synoptic_ground_decoder.SYNOPTIC_ACCUMULATED_SEMANTICS
    return binding.semantics_profile


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
    haversine = (
        math.sin(delta_lat / 2.0) ** 2
        + math.cos(lat_a) * math.cos(lat_b) * math.sin(delta_lon / 2.0) ** 2
    )
    return 6371.0088 * 2.0 * math.asin(min(1.0, math.sqrt(haversine)))


def _receipt_sha256(receipt: GroundObservationReceipt) -> str:
    fact = receipt.fact
    body = {
        "capture_manifest_sha256": receipt.capture_manifest_sha256,
        "capture_receipt_sha256": fact.capture_receipt_sha256,
        "committed_at": receipt.committed_at,
        "fact_id": receipt.fact_id,
        "fact_sha256": receipt.fact_sha256,
        "observation_key": receipt.observation_key,
        "schema": GROUND_EVIDENCE_RECEIPT_SCHEMA,
    }
    return hashlib.sha256(_ground_canonical_json(body)).hexdigest()


def _ground_fact_sha256(fact: GroundObservationFact) -> str:
    return hashlib.sha256(_ground_canonical_json(fact.to_mapping())).hexdigest()


def _ground_observation_key(fact: GroundObservationFact) -> str:
    document = {
        "capture_id": fact.capture_id,
        "capture_receipt_sha256": fact.capture_receipt_sha256,
        "config_sha256": fact.config_sha256,
        "decoder_identity": fact.decoder_identity,
        "decoder_version": fact.decoder_version,
        "observed_at": fact.observed_at,
        "product": fact.product,
        "provider": fact.provider,
        "provider_station_id": fact.station.provider_station_id,
        "representative_interval_end": fact.representative_interval_end,
        "representative_interval_start": fact.representative_interval_start,
        "schema": _GROUND_OBSERVATION_IDENTITY_SCHEMA,
        "site_id": fact.station.site_id,
        "source_member_content_sha256": fact.source_member_content_sha256,
        "source_member_receipt_sha256": fact.source_member_receipt_sha256,
        "source_member_role": fact.source_member_role,
        "source_record_locator": fact.source_record_locator,
        "source_record_sha256": fact.source_record_sha256,
        "variable": fact.measurement.variable,
    }
    return "ground-observation-" + hashlib.sha256(
        _ground_canonical_json(document)
    ).hexdigest()


def _ground_fact_id(fact: GroundObservationFact) -> str:
    document = {
        "fact_sha256": _ground_fact_sha256(fact),
        "observation_key": _ground_observation_key(fact),
        "schema": _GROUND_FACT_IDENTITY_SCHEMA,
    }
    return "ground-fact-" + hashlib.sha256(
        _ground_canonical_json(document)
    ).hexdigest()


def _validated_receipt(
    source_id: str,
    receipt: GroundObservationReceipt,
) -> tuple[GroundObservationReceipt, GroundObservationFact]:
    binding = GROUND_DECODER_BINDINGS.get(source_id)
    if binding is None:
        raise MultiSourceGroundError("groundreceipt heeft onbekende source_id")
    if not binding.decoder_available:
        raise MultiSourceGroundError("raw-only groundbron mag geen canonical receipt geven")
    if not isinstance(receipt, GroundObservationReceipt):
        raise MultiSourceGroundError("groundinput bevat geen GroundObservationReceipt")
    if not isinstance(receipt.fact, GroundObservationFact):
        raise MultiSourceGroundError("groundreceipt bevat geen canonical groundfact")
    try:
        fact = parse_ground_observation(receipt.fact.to_mapping())
        expected_fact_id = _ground_fact_id(fact)
        expected_key = _ground_observation_key(fact)
        expected_fact_sha = _ground_fact_sha256(fact)
    except (TypeError, ValueError) as err:
        raise MultiSourceGroundError("canonical groundfact faalt herbewijs") from err
    committed_at_ms = _timestamp_ms(receipt.committed_at, "receipt.committed_at")
    ingested_at_ms = _timestamp_ms(fact.ingested_at, "fact.ingested_at")
    if (
        fact != receipt.fact
        or receipt.fact_id != expected_fact_id
        or receipt.observation_key != expected_key
        or receipt.fact_sha256 != expected_fact_sha
        or receipt.receipt_sha256 != _receipt_sha256(receipt)
        or fact.provider != binding.fact_provider
        or fact.product != binding.fact_product
        or fact.decoder_identity != binding.decoder_identity
        or fact.decoder_version != binding.decoder_version
        or committed_at_ms < ingested_at_ms
    ):
        raise MultiSourceGroundError("groundreceipt driftte van decoder/capture authority")
    _prefixed_sha(receipt.fact_id, _FACT_ID_RE, "groundreceipt fact_id")
    _prefixed_sha(
        receipt.observation_key,
        _OBSERVATION_KEY_RE,
        "groundreceipt observation_key",
    )
    _prefixed_sha(fact.capture_id, _CAPTURE_ID_RE, "groundreceipt capture_id")
    _sha(receipt.capture_manifest_sha256, "capture_manifest_sha256")
    _sha(receipt.receipt_sha256, "receipt_sha256")
    _sha(fact.config_sha256, "decoder config_sha256")
    return receipt, fact


def _receipt_set_sha256(receipts: Sequence["GroundReceiptReference"]) -> str:
    return _digest({
        "contract": GROUND_RECEIPT_SET_CONTRACT,
        "members": [item.to_mapping() for item in receipts],
    })


@dataclass(frozen=True, slots=True)
class GroundTargetScope:
    target: CycleTarget
    radius_km: float
    scope_sha256: str
    contract: str = GROUND_TARGET_SCOPE_CONTRACT

    def __post_init__(self) -> None:
        if not isinstance(self.target, CycleTarget):
            raise MultiSourceGroundError("ground targetscope mist gevalideerd target")
        _finite(
            self.radius_km,
            "ground radius_km",
            minimum=0.1,
            maximum=MAX_TARGET_RADIUS_KM,
        )
        if self.contract != GROUND_TARGET_SCOPE_CONTRACT:
            raise MultiSourceGroundError("ground targetscopecontract is onbekend")
        if self.scope_sha256 != _digest(self.payload_mapping()):
            raise MultiSourceGroundError("ground targetscopehash klopt niet")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "radius_km": float(self.radius_km),
            "target": self.target.to_mapping(),
        }

    def to_mapping(self) -> dict[str, Any]:
        return {**self.payload_mapping(), "scope_sha256": self.scope_sha256}

    @classmethod
    def create(cls, target: CycleTarget, *, radius_km: float) -> "GroundTargetScope":
        if not isinstance(target, CycleTarget):
            raise MultiSourceGroundError("ground targetscope vereist CycleTarget")
        payload = {
            "contract": GROUND_TARGET_SCOPE_CONTRACT,
            "radius_km": float(radius_km),
            "target": target.to_mapping(),
        }
        return cls(target=target, radius_km=radius_km, scope_sha256=_digest(payload))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "GroundTargetScope":
        raw = _exact(
            value,
            {"contract", "radius_km", "scope_sha256", "target"},
            "ground targetscope",
        )
        try:
            target = CycleTarget.from_mapping(raw["target"])
        except (TypeError, ValueError) as err:
            raise MultiSourceGroundError(
                "ground targetscope bevat ongeldig CycleTarget"
            ) from err
        return cls(
            target=target,
            radius_km=raw["radius_km"],
            scope_sha256=raw["scope_sha256"],
            contract=raw["contract"],
        )


@dataclass(frozen=True, slots=True)
class GroundReceiptReference:
    source_id: str
    fact_id: str
    observation_key: str
    fact_sha256: str
    receipt_sha256: str
    capture_id: str
    capture_manifest_sha256: str
    capture_receipt_sha256: str
    decoder_identity: str
    decoder_version: str
    decoder_config_sha256: str
    fact_provider: str
    fact_product: str
    observed_at_ms: int
    reference_sha256: str
    contract: str = GROUND_RECEIPT_REFERENCE_CONTRACT

    def __post_init__(self) -> None:
        binding = GROUND_DECODER_BINDINGS.get(self.source_id)
        if binding is None or not binding.decoder_available:
            raise MultiSourceGroundError("ground receiptref heeft onbekende decoderbron")
        _prefixed_sha(self.fact_id, _FACT_ID_RE, "ground receiptref fact_id")
        _prefixed_sha(
            self.observation_key,
            _OBSERVATION_KEY_RE,
            "ground receiptref observation_key",
        )
        _prefixed_sha(
            self.capture_id,
            _CAPTURE_ID_RE,
            "ground receiptref capture_id",
        )
        for value, label in (
            (self.fact_sha256, "fact_sha256"),
            (self.receipt_sha256, "receipt_sha256"),
            (self.capture_manifest_sha256, "capture_manifest_sha256"),
            (self.capture_receipt_sha256, "capture_receipt_sha256"),
            (self.decoder_config_sha256, "decoder_config_sha256"),
        ):
            _sha(value, f"ground receiptref {label}")
        if (
            self.decoder_identity != binding.decoder_identity
            or self.decoder_version != binding.decoder_version
            or self.fact_provider != binding.fact_provider
            or self.fact_product != binding.fact_product
        ):
            raise MultiSourceGroundError("ground receiptref decoderbinding driftte")
        _integer(self.observed_at_ms, "ground receiptref observed_at_ms")
        if self.contract != GROUND_RECEIPT_REFERENCE_CONTRACT:
            raise MultiSourceGroundError("ground receiptref contract is onbekend")
        if self.reference_sha256 != _digest(self.payload_mapping()):
            raise MultiSourceGroundError("ground receiptref hash klopt niet")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "capture_id": self.capture_id,
            "capture_manifest_sha256": self.capture_manifest_sha256,
            "capture_receipt_sha256": self.capture_receipt_sha256,
            "contract": self.contract,
            "decoder_config_sha256": self.decoder_config_sha256,
            "decoder_identity": self.decoder_identity,
            "decoder_version": self.decoder_version,
            "fact_id": self.fact_id,
            "fact_product": self.fact_product,
            "fact_provider": self.fact_provider,
            "fact_sha256": self.fact_sha256,
            "observation_key": self.observation_key,
            "observed_at_ms": self.observed_at_ms,
            "receipt_sha256": self.receipt_sha256,
            "source_id": self.source_id,
        }

    def to_mapping(self) -> dict[str, Any]:
        return {**self.payload_mapping(), "reference_sha256": self.reference_sha256}

    @classmethod
    def create(
        cls,
        source_id: str,
        receipt: GroundObservationReceipt,
    ) -> "GroundReceiptReference":
        receipt, fact = _validated_receipt(source_id, receipt)
        payload = {
            "capture_id": fact.capture_id,
            "capture_manifest_sha256": receipt.capture_manifest_sha256,
            "capture_receipt_sha256": fact.capture_receipt_sha256,
            "contract": GROUND_RECEIPT_REFERENCE_CONTRACT,
            "decoder_config_sha256": fact.config_sha256,
            "decoder_identity": fact.decoder_identity,
            "decoder_version": fact.decoder_version,
            "fact_id": receipt.fact_id,
            "fact_product": fact.product,
            "fact_provider": fact.provider,
            "fact_sha256": receipt.fact_sha256,
            "observation_key": receipt.observation_key,
            "observed_at_ms": _timestamp_ms(fact.observed_at, "fact.observed_at"),
            "receipt_sha256": receipt.receipt_sha256,
            "source_id": source_id,
        }
        return cls(
            source_id=source_id,
            fact_id=receipt.fact_id,
            observation_key=receipt.observation_key,
            fact_sha256=receipt.fact_sha256,
            receipt_sha256=receipt.receipt_sha256,
            capture_id=fact.capture_id,
            capture_manifest_sha256=receipt.capture_manifest_sha256,
            capture_receipt_sha256=fact.capture_receipt_sha256,
            decoder_identity=fact.decoder_identity,
            decoder_version=fact.decoder_version,
            decoder_config_sha256=fact.config_sha256,
            fact_provider=fact.provider,
            fact_product=fact.product,
            observed_at_ms=payload["observed_at_ms"],
            reference_sha256=_digest(payload),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "GroundReceiptReference":
        raw = _exact(value, {
            "capture_id", "capture_manifest_sha256", "capture_receipt_sha256",
            "contract", "decoder_config_sha256", "decoder_identity",
            "decoder_version", "fact_id", "fact_product", "fact_provider",
            "fact_sha256", "observation_key", "observed_at_ms", "receipt_sha256",
            "reference_sha256", "source_id",
        }, "ground receipt reference")
        return cls(
            source_id=raw["source_id"],
            fact_id=raw["fact_id"],
            observation_key=raw["observation_key"],
            fact_sha256=raw["fact_sha256"],
            receipt_sha256=raw["receipt_sha256"],
            capture_id=raw["capture_id"],
            capture_manifest_sha256=raw["capture_manifest_sha256"],
            capture_receipt_sha256=raw["capture_receipt_sha256"],
            decoder_identity=raw["decoder_identity"],
            decoder_version=raw["decoder_version"],
            decoder_config_sha256=raw["decoder_config_sha256"],
            fact_provider=raw["fact_provider"],
            fact_product=raw["fact_product"],
            observed_at_ms=raw["observed_at_ms"],
            reference_sha256=raw["reference_sha256"],
            contract=raw["contract"],
        )


@dataclass(frozen=True, slots=True)
class GroundFactCandidate:
    source_id: str
    target_id: str
    fact_id: str
    observation_key: str
    fact_sha256: str
    receipt_sha256: str
    capture_id: str
    capture_manifest_sha256: str
    capture_receipt_sha256: str
    source_member_content_sha256: str
    source_member_receipt_sha256: str
    source_record_sha256: str
    decoder_identity: str
    decoder_version: str
    decoder_config_sha256: str
    fact_provider: str
    fact_product: str
    provider_station_id: str
    site_id: str
    network: str
    latitude_decimal: str
    longitude_decimal: str
    distance_km: float
    observed_at_ms: int
    interval_start_ms: int
    interval_end_ms: int
    received_at_ms: int
    ingested_at_ms: int
    committed_at_ms: int
    fresh_until_ms: int
    freshness: str
    variable: str
    value_state: str
    value_representation: str
    exact_value: str | None
    unit: str
    censor_limit: str | None
    qc_flags: tuple[str, ...]
    qc_sha256: str
    semantics_profile: str
    consensus_eligible: bool
    candidate_sha256: str
    contract: str = GROUND_CANDIDATE_CONTRACT

    def __post_init__(self) -> None:
        binding = GROUND_DECODER_BINDINGS.get(self.source_id)
        if binding is None or not binding.decoder_available:
            raise MultiSourceGroundError("ground candidate heeft onbekende bron")
        for value, label in (
            (self.target_id, "target_id"),
            (self.decoder_identity, "decoder_identity"),
            (self.decoder_version, "decoder_version"),
            (self.fact_provider, "fact_provider"),
            (self.fact_product, "fact_product"),
            (self.provider_station_id, "provider_station_id"),
            (self.site_id, "site_id"),
            (self.network, "network"),
            (self.variable, "variable"),
            (self.value_state, "value_state"),
            (self.value_representation, "value_representation"),
            (self.unit, "unit"),
            (self.semantics_profile, "semantics_profile"),
        ):
            _token(value, label)
        _prefixed_sha(self.fact_id, _FACT_ID_RE, "candidate fact_id")
        _prefixed_sha(
            self.observation_key,
            _OBSERVATION_KEY_RE,
            "candidate observation_key",
        )
        _prefixed_sha(self.capture_id, _CAPTURE_ID_RE, "candidate capture_id")
        for value, label in (
            (self.fact_sha256, "fact_sha256"),
            (self.receipt_sha256, "receipt_sha256"),
            (self.capture_manifest_sha256, "capture_manifest_sha256"),
            (self.capture_receipt_sha256, "capture_receipt_sha256"),
            (self.source_member_content_sha256, "source_member_content_sha256"),
            (self.source_member_receipt_sha256, "source_member_receipt_sha256"),
            (self.source_record_sha256, "source_record_sha256"),
            (self.decoder_config_sha256, "decoder_config_sha256"),
            (self.qc_sha256, "qc_sha256"),
        ):
            _sha(value, label)
        _decimal(self.latitude_decimal, "candidate latitude", "-90", "90")
        _decimal(self.longitude_decimal, "candidate longitude", "-180", "180")
        _finite(
            self.distance_km,
            "candidate distance_km",
            minimum=0.0,
            maximum=20_100.0,
        )
        for name in (
            "observed_at_ms", "interval_start_ms", "interval_end_ms",
            "received_at_ms", "ingested_at_ms", "committed_at_ms",
            "fresh_until_ms",
        ):
            _integer(getattr(self, name), f"candidate.{name}")
        if not self.interval_start_ms <= self.observed_at_ms <= self.interval_end_ms:
            raise MultiSourceGroundError("candidate observatietijd ligt buiten interval")
        if not self.received_at_ms <= self.ingested_at_ms <= self.committed_at_ms:
            raise MultiSourceGroundError("candidate ontvangst/commitvolgorde is ongeldig")
        if self.fresh_until_ms < self.observed_at_ms:
            raise MultiSourceGroundError("candidate freshness eindigt voor observatie")
        maximum_age_ms = binding.maximum_age_ms
        if maximum_age_ms is None:
            raise MultiSourceGroundError("candidatebron mist freshnesspolicy")
        if self.fresh_until_ms != self.observed_at_ms + maximum_age_ms:
            raise MultiSourceGroundError("candidate freshnesspolicy driftte")
        if self.freshness not in {"current", "stale"}:
            raise MultiSourceGroundError("candidate freshness is onbekend")
        if (
            self.decoder_identity != binding.decoder_identity
            or self.decoder_version != binding.decoder_version
            or self.fact_provider != binding.fact_provider
            or self.fact_product != binding.fact_product
        ):
            raise MultiSourceGroundError("candidate decoderbinding driftte")
        if self.semantics_profile != _candidate_semantics_profile(
            self.source_id,
            self.variable,
        ):
            raise MultiSourceGroundError("candidate semanticsprofiel driftte")
        _validate_candidate_measurement(
            state=self.value_state,
            representation=self.value_representation,
            exact_value=self.exact_value,
            censor_limit=self.censor_limit,
        )
        if not isinstance(self.qc_flags, tuple):
            raise MultiSourceGroundError("candidate qc_flags moet tuple zijn")
        if len(self.qc_flags) > MAX_QC_FLAGS_PER_CANDIDATE:
            raise MultiSourceGroundError("candidate qc_flags overschrijdt limiet")
        if self.qc_flags != tuple(sorted(set(self.qc_flags))):
            raise MultiSourceGroundError("candidate qc_flags is niet canoniek")
        for value in self.qc_flags:
            _token(value, "candidate qc_flag")
        if self.consensus_eligible is not False:
            raise MultiSourceGroundError("groundcandidate mag geen consensus claimen")
        if self.contract != GROUND_CANDIDATE_CONTRACT:
            raise MultiSourceGroundError("groundcandidatecontract is onbekend")
        if self.candidate_sha256 != _digest(self.payload_mapping()):
            raise MultiSourceGroundError("groundcandidatehash klopt niet")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "capture_id": self.capture_id,
            "capture_manifest_sha256": self.capture_manifest_sha256,
            "capture_receipt_sha256": self.capture_receipt_sha256,
            "censor_limit": self.censor_limit,
            "committed_at_ms": self.committed_at_ms,
            "consensus_eligible": self.consensus_eligible,
            "contract": self.contract,
            "decoder_config_sha256": self.decoder_config_sha256,
            "decoder_identity": self.decoder_identity,
            "decoder_version": self.decoder_version,
            "distance_km": self.distance_km,
            "exact_value": self.exact_value,
            "fact_id": self.fact_id,
            "fact_product": self.fact_product,
            "fact_provider": self.fact_provider,
            "fact_sha256": self.fact_sha256,
            "fresh_until_ms": self.fresh_until_ms,
            "freshness": self.freshness,
            "ingested_at_ms": self.ingested_at_ms,
            "interval_end_ms": self.interval_end_ms,
            "interval_start_ms": self.interval_start_ms,
            "latitude_decimal": self.latitude_decimal,
            "longitude_decimal": self.longitude_decimal,
            "network": self.network,
            "observation_key": self.observation_key,
            "observed_at_ms": self.observed_at_ms,
            "provider_station_id": self.provider_station_id,
            "qc_flags": list(self.qc_flags),
            "qc_sha256": self.qc_sha256,
            "receipt_sha256": self.receipt_sha256,
            "received_at_ms": self.received_at_ms,
            "semantics_profile": self.semantics_profile,
            "site_id": self.site_id,
            "source_id": self.source_id,
            "source_member_content_sha256": self.source_member_content_sha256,
            "source_member_receipt_sha256": self.source_member_receipt_sha256,
            "source_record_sha256": self.source_record_sha256,
            "target_id": self.target_id,
            "unit": self.unit,
            "value_representation": self.value_representation,
            "value_state": self.value_state,
            "variable": self.variable,
        }

    def to_mapping(self) -> dict[str, Any]:
        result = self.payload_mapping()
        result["candidate_sha256"] = self.candidate_sha256
        return result

    @classmethod
    def create(
        cls,
        *,
        source_id: str,
        target_id: str,
        receipt: GroundObservationReceipt,
        fact: GroundObservationFact,
        distance_km: float,
        evaluated_at_ms: int,
    ) -> "GroundFactCandidate":
        receipt, validated_fact = _validated_receipt(source_id, receipt)
        if fact != validated_fact:
            raise MultiSourceGroundError("candidate fact driftte van groundreceipt")
        fact = validated_fact
        binding = GROUND_DECODER_BINDINGS[source_id]
        station = fact.station
        measurement = fact.measurement
        if (
            station.position_state != "known"
            or station.latitude_decimal is None
            or station.longitude_decimal is None
        ):
            raise MultiSourceGroundError("candidate vereist bekende stationpositie")
        observed_at_ms = _timestamp_ms(fact.observed_at, "fact.observed_at")
        maximum_age_ms = binding.maximum_age_ms
        if maximum_age_ms is None:
            raise MultiSourceGroundError("candidatebron heeft geen freshnesspolicy")
        fresh_until_ms = observed_at_ms + maximum_age_ms
        if fresh_until_ms > MAX_TIMESTAMP_MS:
            raise MultiSourceGroundError("candidate freshness overschrijdt timestampgrens")
        semantics_profile = _candidate_semantics_profile(
            source_id,
            measurement.variable,
        )
        qc_mapping = [item.to_mapping() for item in fact.provider_qc]
        qc_flags = tuple(sorted({item.flag for item in fact.provider_qc}))
        body = {
            "capture_id": fact.capture_id,
            "capture_manifest_sha256": receipt.capture_manifest_sha256,
            "capture_receipt_sha256": fact.capture_receipt_sha256,
            "censor_limit": measurement.censor_limit,
            "committed_at_ms": _timestamp_ms(receipt.committed_at, "committed_at"),
            "consensus_eligible": False,
            "contract": GROUND_CANDIDATE_CONTRACT,
            "decoder_config_sha256": fact.config_sha256,
            "decoder_identity": fact.decoder_identity,
            "decoder_version": fact.decoder_version,
            "distance_km": round(float(distance_km), 6),
            "exact_value": measurement.exact_value,
            "fact_id": receipt.fact_id,
            "fact_product": fact.product,
            "fact_provider": fact.provider,
            "fact_sha256": receipt.fact_sha256,
            "fresh_until_ms": fresh_until_ms,
            "freshness": (
                "current" if fresh_until_ms >= evaluated_at_ms else "stale"
            ),
            "ingested_at_ms": _timestamp_ms(fact.ingested_at, "fact.ingested_at"),
            "interval_end_ms": _timestamp_ms(
                fact.representative_interval_end,
                "fact.interval_end",
            ),
            "interval_start_ms": _timestamp_ms(
                fact.representative_interval_start,
                "fact.interval_start",
            ),
            "latitude_decimal": station.latitude_decimal,
            "longitude_decimal": station.longitude_decimal,
            "network": station.network,
            "observation_key": receipt.observation_key,
            "observed_at_ms": observed_at_ms,
            "provider_station_id": station.provider_station_id,
            "qc_flags": list(qc_flags),
            "qc_sha256": _digest(qc_mapping),
            "receipt_sha256": receipt.receipt_sha256,
            "received_at_ms": _timestamp_ms(fact.received_at, "fact.received_at"),
            "semantics_profile": semantics_profile,
            "site_id": station.site_id,
            "source_id": source_id,
            "source_member_content_sha256": fact.source_member_content_sha256,
            "source_member_receipt_sha256": fact.source_member_receipt_sha256,
            "source_record_sha256": fact.source_record_sha256,
            "target_id": target_id,
            "unit": measurement.unit,
            "value_representation": measurement.representation,
            "value_state": measurement.state,
            "variable": measurement.variable,
        }
        return cls(
            source_id=source_id,
            target_id=target_id,
            fact_id=receipt.fact_id,
            observation_key=receipt.observation_key,
            fact_sha256=receipt.fact_sha256,
            receipt_sha256=receipt.receipt_sha256,
            capture_id=fact.capture_id,
            capture_manifest_sha256=receipt.capture_manifest_sha256,
            capture_receipt_sha256=fact.capture_receipt_sha256,
            source_member_content_sha256=fact.source_member_content_sha256,
            source_member_receipt_sha256=fact.source_member_receipt_sha256,
            source_record_sha256=fact.source_record_sha256,
            decoder_identity=fact.decoder_identity,
            decoder_version=fact.decoder_version,
            decoder_config_sha256=fact.config_sha256,
            fact_provider=fact.provider,
            fact_product=fact.product,
            provider_station_id=station.provider_station_id,
            site_id=station.site_id,
            network=station.network,
            latitude_decimal=station.latitude_decimal,
            longitude_decimal=station.longitude_decimal,
            distance_km=body["distance_km"],
            observed_at_ms=observed_at_ms,
            interval_start_ms=body["interval_start_ms"],
            interval_end_ms=body["interval_end_ms"],
            received_at_ms=body["received_at_ms"],
            ingested_at_ms=body["ingested_at_ms"],
            committed_at_ms=body["committed_at_ms"],
            fresh_until_ms=fresh_until_ms,
            freshness=body["freshness"],
            variable=measurement.variable,
            value_state=measurement.state,
            value_representation=measurement.representation,
            exact_value=measurement.exact_value,
            unit=measurement.unit,
            censor_limit=measurement.censor_limit,
            qc_flags=qc_flags,
            qc_sha256=body["qc_sha256"],
            semantics_profile=semantics_profile,
            consensus_eligible=False,
            candidate_sha256=_digest(body),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "GroundFactCandidate":
        keys = {
            "candidate_sha256", "capture_id", "capture_manifest_sha256",
            "capture_receipt_sha256", "censor_limit", "committed_at_ms",
            "consensus_eligible", "contract", "decoder_config_sha256",
            "decoder_identity", "decoder_version", "distance_km", "exact_value",
            "fact_id", "fact_product", "fact_provider", "fact_sha256",
            "fresh_until_ms", "freshness", "ingested_at_ms", "interval_end_ms",
            "interval_start_ms", "latitude_decimal", "longitude_decimal", "network",
            "observation_key", "observed_at_ms", "provider_station_id", "qc_flags",
            "qc_sha256", "receipt_sha256", "received_at_ms", "semantics_profile",
            "site_id", "source_id", "source_member_content_sha256",
            "source_member_receipt_sha256", "source_record_sha256", "target_id",
            "unit", "value_representation", "value_state", "variable",
        }
        raw = _exact(value, keys, "ground candidate")
        if not isinstance(raw["qc_flags"], list):
            raise MultiSourceGroundError("ground candidate qc_flags is geen lijst")
        return cls(
            source_id=raw["source_id"],
            target_id=raw["target_id"],
            fact_id=raw["fact_id"],
            observation_key=raw["observation_key"],
            fact_sha256=raw["fact_sha256"],
            receipt_sha256=raw["receipt_sha256"],
            capture_id=raw["capture_id"],
            capture_manifest_sha256=raw["capture_manifest_sha256"],
            capture_receipt_sha256=raw["capture_receipt_sha256"],
            source_member_content_sha256=raw["source_member_content_sha256"],
            source_member_receipt_sha256=raw["source_member_receipt_sha256"],
            source_record_sha256=raw["source_record_sha256"],
            decoder_identity=raw["decoder_identity"],
            decoder_version=raw["decoder_version"],
            decoder_config_sha256=raw["decoder_config_sha256"],
            fact_provider=raw["fact_provider"],
            fact_product=raw["fact_product"],
            provider_station_id=raw["provider_station_id"],
            site_id=raw["site_id"],
            network=raw["network"],
            latitude_decimal=raw["latitude_decimal"],
            longitude_decimal=raw["longitude_decimal"],
            distance_km=raw["distance_km"],
            observed_at_ms=raw["observed_at_ms"],
            interval_start_ms=raw["interval_start_ms"],
            interval_end_ms=raw["interval_end_ms"],
            received_at_ms=raw["received_at_ms"],
            ingested_at_ms=raw["ingested_at_ms"],
            committed_at_ms=raw["committed_at_ms"],
            fresh_until_ms=raw["fresh_until_ms"],
            freshness=raw["freshness"],
            variable=raw["variable"],
            value_state=raw["value_state"],
            value_representation=raw["value_representation"],
            exact_value=raw["exact_value"],
            unit=raw["unit"],
            censor_limit=raw["censor_limit"],
            qc_flags=tuple(raw["qc_flags"]),
            qc_sha256=raw["qc_sha256"],
            semantics_profile=raw["semantics_profile"],
            consensus_eligible=raw["consensus_eligible"],
            candidate_sha256=raw["candidate_sha256"],
            contract=raw["contract"],
        )


@dataclass(frozen=True, slots=True)
class GroundTargetContribution:
    source_id: str
    target_scope_sha256: str
    target_id: str
    applicability: TargetApplicability
    status: GroundTargetStatus
    reason_code: str | None
    input_receipt_count: int
    excluded_unknown_position_count: int
    excluded_outside_radius_count: int
    excluded_non_value_count: int
    excluded_future_count: int
    candidates: tuple[GroundFactCandidate, ...]
    contribution_sha256: str
    contract: str = GROUND_TARGET_CONTRACT

    def __post_init__(self) -> None:
        binding = GROUND_DECODER_BINDINGS.get(self.source_id)
        if binding is None:
            raise MultiSourceGroundError("ground targetbijdrage heeft onbekende bron")
        _sha(self.target_scope_sha256, "target_scope_sha256")
        _token(self.target_id, "target_id")
        if (
            not isinstance(self.applicability, TargetApplicability)
            or self.applicability.target_id != self.target_id
        ):
            raise MultiSourceGroundError("ground targetbijdrage applicability driftte")
        if not isinstance(self.status, GroundTargetStatus):
            raise MultiSourceGroundError("ground targetstatus is onbekend")
        for name in (
            "input_receipt_count", "excluded_unknown_position_count",
            "excluded_outside_radius_count", "excluded_non_value_count",
            "excluded_future_count",
        ):
            _integer(
                getattr(self, name),
                f"ground target {name}",
                maximum=MAX_RECEIPTS_PER_SOURCE,
            )
        if not isinstance(self.candidates, tuple):
            raise MultiSourceGroundError("ground target candidates moet tuple zijn")
        if len(self.candidates) > MAX_CANDIDATES_PER_TARGET:
            raise MultiSourceGroundError("ground target candidates overschrijdt limiet")
        if any(not isinstance(item, GroundFactCandidate) for item in self.candidates):
            raise MultiSourceGroundError("ground target candidates bevat fout type")
        keys = tuple(
            (item.observed_at_ms, item.fact_id) for item in self.candidates
        )
        if (
            keys != tuple(sorted(keys))
            or len({item.fact_id for item in self.candidates})
            != len(self.candidates)
            or len({item.observation_key for item in self.candidates})
            != len(self.candidates)
        ):
            raise MultiSourceGroundError("ground target candidates is niet canoniek")
        if any(
            item.source_id != self.source_id or item.target_id != self.target_id
            for item in self.candidates
        ):
            raise MultiSourceGroundError("ground candidate heeft andere source/target")
        accounted = (
            len(self.candidates)
            + self.excluded_unknown_position_count
            + self.excluded_outside_radius_count
            + self.excluded_non_value_count
            + self.excluded_future_count
        )
        if (
            self.applicability.applicable
            and self.status is not GroundTargetStatus.BLOCKED
            and accounted != self.input_receipt_count
        ):
            raise MultiSourceGroundError("ground target receiptdekking is onvolledig")
        if self.status is GroundTargetStatus.CURRENT:
            if not self.candidates or not any(
                item.freshness == "current" for item in self.candidates
            ):
                raise MultiSourceGroundError("current groundtarget mist verse kandidaat")
            if self.reason_code is not None:
                raise MultiSourceGroundError("current groundtarget mag geen reden hebben")
        elif self.status is GroundTargetStatus.STALE:
            if not self.candidates or any(
                item.freshness == "current" for item in self.candidates
            ):
                raise MultiSourceGroundError("stale groundtarget heeft verkeerde freshness")
            if self.reason_code != "all_matching_ground_facts_stale":
                raise MultiSourceGroundError("stale groundtarget mist vaste reden")
        else:
            if self.candidates:
                raise MultiSourceGroundError("niet-data groundtarget bevat kandidaten")
            if self.reason_code is None:
                raise MultiSourceGroundError("niet-data groundtarget mist reden")
        if self.reason_code is not None:
            _reason(self.reason_code)
        if self.status is GroundTargetStatus.NOT_APPLICABLE:
            if self.applicability.applicable:
                raise MultiSourceGroundError("toepasselijk target mag niet not_applicable")
            if self.reason_code != "target_outside_catalog_scope":
                raise MultiSourceGroundError("not_applicable mist vaste reden")
        elif not self.applicability.applicable:
            raise MultiSourceGroundError("niet-toepasselijk target heeft datastatus")
        if self.applicability.applicable and not binding.decoder_available:
            if (
                self.status is not GroundTargetStatus.BLOCKED
                or self.reason_code != binding.blocked_reason
            ):
                raise MultiSourceGroundError("raw-only groundtarget is niet exact blocked")
        elif self.status is GroundTargetStatus.BLOCKED:
            raise MultiSourceGroundError("decoderbron mag niet blocked zijn")
        if self.contract != GROUND_TARGET_CONTRACT:
            raise MultiSourceGroundError("ground targetcontract is onbekend")
        if self.contribution_sha256 != _digest(self.payload_mapping()):
            raise MultiSourceGroundError("ground targetbijdragehash klopt niet")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "applicability": self.applicability.to_mapping(),
            "candidates": [item.to_mapping() for item in self.candidates],
            "contract": self.contract,
            "excluded_future_count": self.excluded_future_count,
            "excluded_non_value_count": self.excluded_non_value_count,
            "excluded_outside_radius_count": self.excluded_outside_radius_count,
            "excluded_unknown_position_count": self.excluded_unknown_position_count,
            "input_receipt_count": self.input_receipt_count,
            "reason_code": self.reason_code,
            "source_id": self.source_id,
            "status": self.status.value,
            "target_id": self.target_id,
            "target_scope_sha256": self.target_scope_sha256,
        }

    def to_mapping(self) -> dict[str, Any]:
        return {
            **self.payload_mapping(),
            "contribution_sha256": self.contribution_sha256,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "GroundTargetContribution":
        raw = _exact(value, {
            "applicability", "candidates", "contract", "contribution_sha256",
            "excluded_future_count", "excluded_non_value_count",
            "excluded_outside_radius_count", "excluded_unknown_position_count",
            "input_receipt_count", "reason_code", "source_id", "status", "target_id",
            "target_scope_sha256",
        }, "ground target contribution")
        if not isinstance(raw["candidates"], list):
            raise MultiSourceGroundError("ground candidates is geen lijst")
        try:
            status = GroundTargetStatus(raw["status"])
        except (TypeError, ValueError) as err:
            raise MultiSourceGroundError("ground targetstatus is onbekend") from err
        try:
            applicability = TargetApplicability.from_mapping(raw["applicability"])
        except (TypeError, ValueError) as err:
            raise MultiSourceGroundError(
                "ground target bevat ongeldige applicability"
            ) from err
        return cls(
            source_id=raw["source_id"],
            target_scope_sha256=raw["target_scope_sha256"],
            target_id=raw["target_id"],
            applicability=applicability,
            status=status,
            reason_code=raw["reason_code"],
            input_receipt_count=raw["input_receipt_count"],
            excluded_unknown_position_count=raw["excluded_unknown_position_count"],
            excluded_outside_radius_count=raw["excluded_outside_radius_count"],
            excluded_non_value_count=raw["excluded_non_value_count"],
            excluded_future_count=raw["excluded_future_count"],
            candidates=tuple(
                GroundFactCandidate.from_mapping(item) for item in raw["candidates"]
            ),
            contribution_sha256=raw["contribution_sha256"],
            contract=raw["contract"],
        )


@dataclass(frozen=True, slots=True)
class GroundWorkloadContribution:
    source_id: str
    source_descriptor_sha256: str
    decoder_available: bool
    decoder_identity: str | None
    decoder_version: str | None
    semantics_profile: str
    freshness_policy_sha256: str
    input_receipt_count: int
    input_receipts: tuple[GroundReceiptReference, ...]
    receipt_set_sha256: str
    targets: tuple[GroundTargetContribution, ...]
    workload_sha256: str
    workload_receipt_sha256: str
    contract: str = GROUND_WORKLOAD_CONTRACT

    def __post_init__(self) -> None:
        binding = GROUND_DECODER_BINDINGS.get(self.source_id)
        source = GROUND_CATALOG_BY_ID.get(self.source_id)
        if binding is None or source is None or source.kind is not SourceKind.GROUND:
            raise MultiSourceGroundError("ground workload heeft onbekende bron")
        if self.source_descriptor_sha256 != source.descriptor_sha256:
            raise MultiSourceGroundError("ground workload descriptor driftte")
        if (
            self.decoder_available is not binding.decoder_available
            or self.decoder_identity != binding.decoder_identity
            or self.decoder_version != binding.decoder_version
            or self.semantics_profile != binding.semantics_profile
        ):
            raise MultiSourceGroundError("ground workload decoderbinding driftte")
        _sha(self.freshness_policy_sha256, "freshness_policy_sha256")
        if self.freshness_policy_sha256 != GROUND_FRESHNESS_POLICY_SHA256:
            raise MultiSourceGroundError("ground workload freshnesspolicy driftte")
        _integer(
            self.input_receipt_count,
            "input_receipt_count",
            maximum=MAX_RECEIPTS_PER_SOURCE,
        )
        if not isinstance(self.input_receipts, tuple):
            raise MultiSourceGroundError("ground workload receiptrefs moet tuple zijn")
        if len(self.input_receipts) != self.input_receipt_count:
            raise MultiSourceGroundError("ground workload receiptrefcount klopt niet")
        if any(
            not isinstance(item, GroundReceiptReference)
            for item in self.input_receipts
        ):
            raise MultiSourceGroundError("ground workload receiptrefs bevat fout type")
        reference_keys = tuple(
            (item.observed_at_ms, item.fact_id) for item in self.input_receipts
        )
        if (
            reference_keys != tuple(sorted(reference_keys))
            or len({item.fact_id for item in self.input_receipts})
            != len(self.input_receipts)
            or len({item.observation_key for item in self.input_receipts})
            != len(self.input_receipts)
            or any(item.source_id != self.source_id for item in self.input_receipts)
        ):
            raise MultiSourceGroundError("ground workload receiptrefs is niet canoniek")
        _sha(self.receipt_set_sha256, "receipt_set_sha256")
        if self.receipt_set_sha256 != _receipt_set_sha256(self.input_receipts):
            raise MultiSourceGroundError("ground workload receiptsethash klopt niet")
        if not binding.decoder_available and self.input_receipts:
            raise MultiSourceGroundError("raw-only workload bevat canonical receiptrefs")
        if not isinstance(self.targets, tuple) or not 1 <= len(self.targets) <= MAX_TARGETS:
            raise MultiSourceGroundError("ground workload targetcount is ongeldig")
        if any(not isinstance(item, GroundTargetContribution) for item in self.targets):
            raise MultiSourceGroundError("ground workload targets bevat fout type")
        target_ids = tuple(item.target_id for item in self.targets)
        if target_ids != tuple(sorted(set(target_ids))):
            raise MultiSourceGroundError("ground workload targets is niet canoniek")
        if any(item.source_id != self.source_id for item in self.targets):
            raise MultiSourceGroundError("ground workload targets heeft andere bron")
        references = {item.fact_id: item for item in self.input_receipts}
        for target in self.targets:
            if target.input_receipt_count != self.input_receipt_count:
                raise MultiSourceGroundError("ground target receiptcount driftte")
            for candidate in target.candidates:
                reference = references.get(candidate.fact_id)
                if reference is None or (
                    candidate.observation_key != reference.observation_key
                    or candidate.fact_sha256 != reference.fact_sha256
                    or candidate.receipt_sha256 != reference.receipt_sha256
                    or candidate.capture_id != reference.capture_id
                    or candidate.capture_manifest_sha256
                    != reference.capture_manifest_sha256
                    or candidate.capture_receipt_sha256
                    != reference.capture_receipt_sha256
                    or candidate.decoder_identity != reference.decoder_identity
                    or candidate.decoder_version != reference.decoder_version
                    or candidate.decoder_config_sha256
                    != reference.decoder_config_sha256
                    or candidate.fact_provider != reference.fact_provider
                    or candidate.fact_product != reference.fact_product
                    or candidate.observed_at_ms != reference.observed_at_ms
                ):
                    raise MultiSourceGroundError(
                        "ground candidate driftte van input receiptref"
                    )
        if sum(len(item.candidates) for item in self.targets) > (
            MAX_TOTAL_TARGET_CANDIDATES
        ):
            raise MultiSourceGroundError("ground workload overschrijdt candidatelimiet")
        if self.contract != GROUND_WORKLOAD_CONTRACT:
            raise MultiSourceGroundError("ground workloadcontract is onbekend")
        if self.workload_sha256 != _digest(self.payload_mapping()):
            raise MultiSourceGroundError("ground workloadhash klopt niet")
        expected_receipt = _digest({
            "contract": GROUND_WORKLOAD_RECEIPT_SCHEMA,
            "freshness_policy_sha256": self.freshness_policy_sha256,
            "receipt_set_sha256": self.receipt_set_sha256,
            "source_descriptor_sha256": self.source_descriptor_sha256,
            "source_id": self.source_id,
            "workload_sha256": self.workload_sha256,
        })
        if self.workload_receipt_sha256 != expected_receipt:
            raise MultiSourceGroundError("ground workloadreceipthash klopt niet")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "decoder_available": self.decoder_available,
            "decoder_identity": self.decoder_identity,
            "decoder_version": self.decoder_version,
            "freshness_policy_sha256": self.freshness_policy_sha256,
            "input_receipt_count": self.input_receipt_count,
            "input_receipts": [item.to_mapping() for item in self.input_receipts],
            "receipt_set_sha256": self.receipt_set_sha256,
            "semantics_profile": self.semantics_profile,
            "source_descriptor_sha256": self.source_descriptor_sha256,
            "source_id": self.source_id,
            "targets": [item.to_mapping() for item in self.targets],
        }

    def to_mapping(self) -> dict[str, Any]:
        return {
            **self.payload_mapping(),
            "workload_receipt_sha256": self.workload_receipt_sha256,
            "workload_sha256": self.workload_sha256,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "GroundWorkloadContribution":
        raw = _exact(value, {
            "contract", "decoder_available", "decoder_identity", "decoder_version",
            "freshness_policy_sha256", "input_receipt_count", "input_receipts",
            "receipt_set_sha256", "semantics_profile", "source_descriptor_sha256",
            "source_id", "targets", "workload_receipt_sha256", "workload_sha256",
        }, "ground workload contribution")
        if not isinstance(raw["targets"], list) or not isinstance(
            raw["input_receipts"], list
        ):
            raise MultiSourceGroundError("ground workload targets/receipts is geen lijst")
        return cls(
            source_id=raw["source_id"],
            source_descriptor_sha256=raw["source_descriptor_sha256"],
            decoder_available=raw["decoder_available"],
            decoder_identity=raw["decoder_identity"],
            decoder_version=raw["decoder_version"],
            semantics_profile=raw["semantics_profile"],
            freshness_policy_sha256=raw["freshness_policy_sha256"],
            input_receipt_count=raw["input_receipt_count"],
            input_receipts=tuple(
                GroundReceiptReference.from_mapping(item)
                for item in raw["input_receipts"]
            ),
            receipt_set_sha256=raw["receipt_set_sha256"],
            targets=tuple(
                GroundTargetContribution.from_mapping(item) for item in raw["targets"]
            ),
            workload_sha256=raw["workload_sha256"],
            workload_receipt_sha256=raw["workload_receipt_sha256"],
            contract=raw["contract"],
        )


_FRESHNESS_POLICY = {
    "contract": GROUND_FRESHNESS_POLICY_CONTRACT,
    "maximum_age_ms_by_source": {
        source_id: binding.maximum_age_ms
        for source_id, binding in sorted(GROUND_DECODER_BINDINGS.items())
    },
    "reference_time": "canonical_fact_observed_at",
    "stale_semantics": "candidate_retained_not_current",
}
GROUND_FRESHNESS_POLICY_SHA256 = _digest(_FRESHNESS_POLICY)

_CONSENSUS_AUTHORITY = {
    "allowlisted_cross_provider_semantics": [],
    "contract": GROUND_CONSENSUS_AUTHORITY_CONTRACT,
    "decision": "withheld",
    "reason_code": "cross_provider_semantics_certificate_unavailable",
}
GROUND_CONSENSUS_AUTHORITY_SHA256 = _digest(_CONSENSUS_AUTHORITY)


def _materialize_receipts(
    source_id: str,
    value: object,
) -> tuple[GroundObservationReceipt, ...]:
    count = _bounded_sequence_count(
        value,
        label=f"{source_id} receipts",
        maximum=MAX_RECEIPTS_PER_SOURCE,
    )
    result: list[GroundObservationReceipt] = []
    for index in range(count):
        try:
            receipt = value[index]
        except (IndexError, KeyError, TypeError) as err:
            raise MultiSourceGroundError(f"{source_id} receipts wijzigde tijdens lezen") from err
        validated, _fact = _validated_receipt(source_id, receipt)
        result.append(validated)
    ordered = tuple(sorted(
        result,
        key=lambda item: (item.fact.observed_at, item.fact_id),
    ))
    identities = tuple(item.fact_id for item in ordered)
    observation_keys = tuple(item.observation_key for item in ordered)
    if (
        len(set(identities)) != len(identities)
        or len(set(observation_keys)) != len(observation_keys)
    ):
        raise MultiSourceGroundError(f"{source_id} bevat dubbele groundfacts")
    authorities = {
        (
            item.fact.capture_id,
            item.capture_manifest_sha256,
            item.fact.capture_receipt_sha256,
            item.fact.config_sha256,
        )
        for item in ordered
    }
    if len(authorities) > 1:
        raise MultiSourceGroundError(f"{source_id} receipts mengen meerdere decodebatches")
    return ordered


def _target_contribution(
    *,
    source_id: str,
    target_scope: GroundTargetScope,
    receipts: tuple[GroundObservationReceipt, ...],
    evaluated_at_ms: int,
    source_error: str | None,
) -> GroundTargetContribution:
    source = GROUND_CATALOG_BY_ID[source_id]
    binding = GROUND_DECODER_BINDINGS[source_id]
    applicability = evaluate_target_applicability(source, target_scope.target)
    candidates: list[GroundFactCandidate] = []
    unknown_position = 0
    outside_radius = 0
    non_value = 0
    future = 0
    reason_code: str | None = None

    if not applicability.applicable:
        status = GroundTargetStatus.NOT_APPLICABLE
        reason_code = "target_outside_catalog_scope"
    elif not binding.decoder_available:
        status = GroundTargetStatus.BLOCKED
        reason_code = binding.blocked_reason
    elif source_error is not None:
        status = GroundTargetStatus.ERROR
        reason_code = source_error
    else:
        for receipt in receipts:
            fact = receipt.fact
            measurement = fact.measurement
            if measurement.state not in {
                "observed", "trace", "censored_below", "censored_above",
            }:
                non_value += 1
                continue
            station = fact.station
            if (
                station.position_state != "known"
                or station.latitude_decimal is None
                or station.longitude_decimal is None
            ):
                unknown_position += 1
                continue
            observed_at_ms = _timestamp_ms(fact.observed_at, "fact.observed_at")
            if observed_at_ms > evaluated_at_ms + MAX_FUTURE_SKEW_MS:
                future += 1
                continue
            latitude = float(_decimal(
                station.latitude_decimal,
                "station.latitude_decimal",
                "-90",
                "90",
            ))
            longitude = float(_decimal(
                station.longitude_decimal,
                "station.longitude_decimal",
                "-180",
                "180",
            ))
            distance_km = _haversine_km(
                target_scope.target.latitude,
                target_scope.target.longitude,
                latitude,
                longitude,
            )
            if distance_km > target_scope.radius_km:
                outside_radius += 1
                continue
            candidates.append(GroundFactCandidate.create(
                source_id=source_id,
                target_id=target_scope.target.target_id,
                receipt=receipt,
                fact=fact,
                distance_km=distance_km,
                evaluated_at_ms=evaluated_at_ms,
            ))
            if len(candidates) > MAX_CANDIDATES_PER_TARGET:
                raise MultiSourceGroundError("groundtarget overschrijdt candidatelimiet")
        candidates.sort(key=lambda item: (item.observed_at_ms, item.fact_id))
        if any(item.freshness == "current" for item in candidates):
            status = GroundTargetStatus.CURRENT
        elif candidates:
            status = GroundTargetStatus.STALE
            reason_code = "all_matching_ground_facts_stale"
        else:
            status = GroundTargetStatus.ERROR
            reason_code = (
                "decoder_returned_no_ground_facts"
                if not receipts
                else "no_usable_ground_fact_for_target"
            )
    payload = {
        "applicability": applicability.to_mapping(),
        "candidates": [item.to_mapping() for item in candidates],
        "contract": GROUND_TARGET_CONTRACT,
        "excluded_future_count": future,
        "excluded_non_value_count": non_value,
        "excluded_outside_radius_count": outside_radius,
        "excluded_unknown_position_count": unknown_position,
        "input_receipt_count": len(receipts),
        "reason_code": reason_code,
        "source_id": source_id,
        "status": status.value,
        "target_id": target_scope.target.target_id,
        "target_scope_sha256": target_scope.scope_sha256,
    }
    return GroundTargetContribution(
        source_id=source_id,
        target_scope_sha256=target_scope.scope_sha256,
        target_id=target_scope.target.target_id,
        applicability=applicability,
        status=status,
        reason_code=reason_code,
        input_receipt_count=len(receipts),
        excluded_unknown_position_count=unknown_position,
        excluded_outside_radius_count=outside_radius,
        excluded_non_value_count=non_value,
        excluded_future_count=future,
        candidates=tuple(candidates),
        contribution_sha256=_digest(payload),
    )


def _workload_contribution(
    *,
    source_id: str,
    target_scopes: tuple[GroundTargetScope, ...],
    receipts: tuple[GroundObservationReceipt, ...],
    evaluated_at_ms: int,
    source_error: str | None,
) -> GroundWorkloadContribution:
    binding = GROUND_DECODER_BINDINGS[source_id]
    source = GROUND_CATALOG_BY_ID[source_id]
    receipt_references = tuple(
        GroundReceiptReference.create(source_id, item) for item in receipts
    )
    targets = tuple(
        _target_contribution(
            source_id=source_id,
            target_scope=target,
            receipts=receipts,
            evaluated_at_ms=evaluated_at_ms,
            source_error=source_error,
        )
        for target in target_scopes
    )
    receipt_set_sha = _receipt_set_sha256(receipt_references)
    payload = {
        "contract": GROUND_WORKLOAD_CONTRACT,
        "decoder_available": binding.decoder_available,
        "decoder_identity": binding.decoder_identity,
        "decoder_version": binding.decoder_version,
        "freshness_policy_sha256": GROUND_FRESHNESS_POLICY_SHA256,
        "input_receipt_count": len(receipts),
        "input_receipts": [item.to_mapping() for item in receipt_references],
        "receipt_set_sha256": receipt_set_sha,
        "semantics_profile": binding.semantics_profile,
        "source_descriptor_sha256": source.descriptor_sha256,
        "source_id": source_id,
        "targets": [item.to_mapping() for item in targets],
    }
    workload_sha = _digest(payload)
    workload_receipt_sha = _digest({
        "contract": GROUND_WORKLOAD_RECEIPT_SCHEMA,
        "freshness_policy_sha256": GROUND_FRESHNESS_POLICY_SHA256,
        "receipt_set_sha256": receipt_set_sha,
        "source_descriptor_sha256": source.descriptor_sha256,
        "source_id": source_id,
        "workload_sha256": workload_sha,
    })
    return GroundWorkloadContribution(
        source_id=source_id,
        source_descriptor_sha256=source.descriptor_sha256,
        decoder_available=binding.decoder_available,
        decoder_identity=binding.decoder_identity,
        decoder_version=binding.decoder_version,
        semantics_profile=binding.semantics_profile,
        freshness_policy_sha256=GROUND_FRESHNESS_POLICY_SHA256,
        input_receipt_count=len(receipts),
        input_receipts=receipt_references,
        receipt_set_sha256=receipt_set_sha,
        targets=targets,
        workload_sha256=workload_sha,
        workload_receipt_sha256=workload_receipt_sha,
    )


@dataclass(frozen=True, slots=True)
class AllGroundContributionSlice:
    desired_state_sha256: str
    authority_epoch: int
    evaluated_at_ms: int
    target_scopes: tuple[GroundTargetScope, ...]
    target_set_sha256: str
    workloads: tuple[GroundWorkloadContribution, ...]
    freshness_policy_sha256: str
    consensus_authority_sha256: str
    consensus_status: str
    consensus_reason_code: str
    consensus_candidates: tuple[object, ...]
    input_receipt_count: int
    target_candidate_count: int
    slice_sha256: str
    contract: str = GROUND_SLICE_CONTRACT

    def __post_init__(self) -> None:
        _sha(self.desired_state_sha256, "desired_state_sha256")
        _integer(self.authority_epoch, "authority_epoch", minimum=1)
        _integer(self.evaluated_at_ms, "evaluated_at_ms")
        if (
            not isinstance(self.target_scopes, tuple)
            or not 1 <= len(self.target_scopes) <= MAX_TARGETS
        ):
            raise MultiSourceGroundError("ground slice targetscopes is ongeldig")
        if any(
            not isinstance(item, GroundTargetScope)
            for item in self.target_scopes
        ):
            raise MultiSourceGroundError("ground slice targetscopes bevat fout type")
        target_ids = tuple(item.target.target_id for item in self.target_scopes)
        if target_ids != tuple(sorted(set(target_ids))):
            raise MultiSourceGroundError("ground slice targetscopes is niet canoniek")
        if self.target_set_sha256 != _digest([
            item.to_mapping() for item in self.target_scopes
        ]):
            raise MultiSourceGroundError("ground slice targetsethash klopt niet")
        if not isinstance(self.workloads, tuple):
            raise MultiSourceGroundError("ground slice workloads moet tuple zijn")
        if any(
            not isinstance(item, GroundWorkloadContribution)
            for item in self.workloads
        ):
            raise MultiSourceGroundError("ground slice workloads bevat fout type")
        source_ids = tuple(item.source_id for item in self.workloads)
        if source_ids != tuple(sorted(GROUND_SOURCE_IDS)):
            raise MultiSourceGroundError("ground slice mist of dupliceert workload")
        expected_scope_hashes = tuple(item.scope_sha256 for item in self.target_scopes)
        for workload in self.workloads:
            if tuple(item.target_scope_sha256 for item in workload.targets) != (
                expected_scope_hashes
            ):
                raise MultiSourceGroundError("ground workload mist targetdekking")
            source = GROUND_CATALOG_BY_ID[workload.source_id]
            for scope, target in zip(self.target_scopes, workload.targets):
                if target.target_id != scope.target.target_id:
                    raise MultiSourceGroundError("ground workload targetidentity driftte")
                expected_applicability = evaluate_target_applicability(
                    source,
                    scope.target,
                )
                if target.applicability != expected_applicability:
                    raise MultiSourceGroundError(
                        "ground target applicability driftte van catalogus"
                    )
                for candidate in target.candidates:
                    latitude = float(_decimal(
                        candidate.latitude_decimal,
                        "candidate latitude_decimal",
                        "-90",
                        "90",
                    ))
                    longitude = float(_decimal(
                        candidate.longitude_decimal,
                        "candidate longitude_decimal",
                        "-180",
                        "180",
                    ))
                    expected_distance = round(_haversine_km(
                        scope.target.latitude,
                        scope.target.longitude,
                        latitude,
                        longitude,
                    ), 6)
                    if (
                        candidate.distance_km != expected_distance
                        or candidate.distance_km > scope.radius_km
                    ):
                        raise MultiSourceGroundError(
                            "ground candidate targetafstand driftte"
                        )
                    if candidate.observed_at_ms > (
                        self.evaluated_at_ms + MAX_FUTURE_SKEW_MS
                    ):
                        raise MultiSourceGroundError(
                            "ground candidate ligt buiten future-skew"
                        )
                    expected_freshness = (
                        "current"
                        if candidate.fresh_until_ms >= self.evaluated_at_ms
                        else "stale"
                    )
                    if candidate.freshness != expected_freshness:
                        raise MultiSourceGroundError(
                            "ground candidate freshness driftte van slice"
                        )
        if self.freshness_policy_sha256 != GROUND_FRESHNESS_POLICY_SHA256:
            raise MultiSourceGroundError("ground freshnesspolicy driftte")
        if self.consensus_authority_sha256 != GROUND_CONSENSUS_AUTHORITY_SHA256:
            raise MultiSourceGroundError("ground consensusauthority driftte")
        if (
            self.consensus_status != "withheld"
            or self.consensus_reason_code
            != "cross_provider_semantics_certificate_unavailable"
            or self.consensus_candidates != ()
        ):
            raise MultiSourceGroundError("ground slice verzint cross-provider consensus")
        _integer(
            self.input_receipt_count,
            "slice input_receipt_count",
            maximum=MAX_TOTAL_RECEIPTS,
        )
        _integer(
            self.target_candidate_count,
            "slice target_candidate_count",
            maximum=MAX_TOTAL_TARGET_CANDIDATES,
        )
        expected_receipts = sum(item.input_receipt_count for item in self.workloads)
        expected_candidates = sum(
            len(target.candidates)
            for workload in self.workloads
            for target in workload.targets
        )
        if (
            self.input_receipt_count != expected_receipts
            or self.target_candidate_count != expected_candidates
        ):
            raise MultiSourceGroundError("ground slice telling klopt niet")
        if self.contract != GROUND_SLICE_CONTRACT:
            raise MultiSourceGroundError("ground slicecontract is onbekend")
        if self.slice_sha256 != _digest(self.payload_mapping()):
            raise MultiSourceGroundError("ground slicehash klopt niet")
        if len(_canonical_json(self.to_mapping())) > MAX_SLICE_BYTES:
            raise MultiSourceGroundError("ground slice overschrijdt documentlimiet")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "authority_epoch": self.authority_epoch,
            "consensus": {
                "authority_sha256": self.consensus_authority_sha256,
                "candidates": list(self.consensus_candidates),
                "reason_code": self.consensus_reason_code,
                "status": self.consensus_status,
            },
            "contract": self.contract,
            "desired_state_sha256": self.desired_state_sha256,
            "evaluated_at_ms": self.evaluated_at_ms,
            "freshness_policy_sha256": self.freshness_policy_sha256,
            "input_receipt_count": self.input_receipt_count,
            "target_candidate_count": self.target_candidate_count,
            "target_scopes": [item.to_mapping() for item in self.target_scopes],
            "target_set_sha256": self.target_set_sha256,
            "workloads": [item.to_mapping() for item in self.workloads],
        }

    def to_mapping(self) -> dict[str, Any]:
        return {**self.payload_mapping(), "slice_sha256": self.slice_sha256}

    def to_canonical_bytes(self) -> bytes:
        return _canonical_json(self.to_mapping())

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AllGroundContributionSlice":
        raw = _exact(value, {
            "authority_epoch", "consensus", "contract", "desired_state_sha256",
            "evaluated_at_ms", "freshness_policy_sha256", "input_receipt_count",
            "slice_sha256", "target_candidate_count", "target_scopes",
            "target_set_sha256", "workloads",
        }, "all-ground slice")
        consensus = _exact(
            raw["consensus"],
            {"authority_sha256", "candidates", "reason_code", "status"},
            "ground consensus",
        )
        if (
            not isinstance(raw["target_scopes"], list)
            or not isinstance(raw["workloads"], list)
            or not isinstance(consensus["candidates"], list)
        ):
            raise MultiSourceGroundError("ground slice bevat geen gesloten lijsten")
        return cls(
            desired_state_sha256=raw["desired_state_sha256"],
            authority_epoch=raw["authority_epoch"],
            evaluated_at_ms=raw["evaluated_at_ms"],
            target_scopes=tuple(
                GroundTargetScope.from_mapping(item) for item in raw["target_scopes"]
            ),
            target_set_sha256=raw["target_set_sha256"],
            workloads=tuple(
                GroundWorkloadContribution.from_mapping(item)
                for item in raw["workloads"]
            ),
            freshness_policy_sha256=raw["freshness_policy_sha256"],
            consensus_authority_sha256=consensus["authority_sha256"],
            consensus_status=consensus["status"],
            consensus_reason_code=consensus["reason_code"],
            consensus_candidates=tuple(consensus["candidates"]),
            input_receipt_count=raw["input_receipt_count"],
            target_candidate_count=raw["target_candidate_count"],
            slice_sha256=raw["slice_sha256"],
            contract=raw["contract"],
        )


def build_all_ground_slice(
    *,
    desired_state_sha256: str,
    authority_epoch: int,
    evaluated_at_ms: int,
    target_scopes: Iterable[GroundTargetScope],
    receipts_by_source: Mapping[str, Sequence[GroundObservationReceipt]],
    source_errors: Mapping[str, str] | None = None,
) -> AllGroundContributionSlice:
    """Build one complete five-workload, all-target candidate slice."""

    _sha(desired_state_sha256, "desired_state_sha256")
    _integer(authority_epoch, "authority_epoch", minimum=1)
    _integer(evaluated_at_ms, "evaluated_at_ms")
    unvalidated_scopes = _bounded_iterable(
        target_scopes,
        label="ground target_scopes",
        maximum=MAX_TARGETS,
    )
    if not unvalidated_scopes:
        raise MultiSourceGroundError("ground slice bevat geen of te veel targets")
    if any(
        not isinstance(item, GroundTargetScope)
        for item in unvalidated_scopes
    ):
        raise MultiSourceGroundError("ground slice targetscope heeft fout type")
    validated_scopes = tuple(
        item
        for item in unvalidated_scopes
        if isinstance(item, GroundTargetScope)
    )
    scopes = tuple(sorted(
        validated_scopes,
        key=lambda item: item.target.target_id,
    ))
    if len({item.target.target_id for item in scopes}) != len(scopes):
        raise MultiSourceGroundError("ground slice bevat dubbele targets")
    if not isinstance(receipts_by_source, Mapping) or set(receipts_by_source) != (
        GROUND_SOURCE_IDS
    ):
        raise MultiSourceGroundError("receipts_by_source mist of voegt groundbron toe")
    if source_errors is not None and not isinstance(source_errors, Mapping):
        raise MultiSourceGroundError("source_errors is geen mapping")
    errors = {} if source_errors is None else dict(source_errors)
    if any(source_id not in GROUND_SOURCE_IDS for source_id in errors):
        raise MultiSourceGroundError("source_errors bevat onbekende groundbron")
    for source_id, reason_code in errors.items():
        _reason(reason_code, f"source_errors.{source_id}")
        if not GROUND_DECODER_BINDINGS[source_id].decoder_available:
            raise MultiSourceGroundError("raw-only groundbron gebruikt vaste blockreden")

    materialized: dict[str, tuple[GroundObservationReceipt, ...]] = {}
    total_receipts = 0
    for source_id in sorted(GROUND_SOURCE_IDS):
        binding = GROUND_DECODER_BINDINGS[source_id]
        values = receipts_by_source[source_id]
        if not binding.decoder_available:
            count = _bounded_sequence_count(
                values,
                label=f"{source_id} receipts",
                maximum=MAX_RECEIPTS_PER_SOURCE,
            )
            if count != 0:
                raise MultiSourceGroundError("raw-only groundbron leverde canonical receipts")
            receipts: tuple[GroundObservationReceipt, ...] = ()
        else:
            receipts = _materialize_receipts(source_id, values)
        if source_id in errors and receipts:
            raise MultiSourceGroundError("groundbron kan niet tegelijk error en data zijn")
        total_receipts += len(receipts)
        if total_receipts > MAX_TOTAL_RECEIPTS:
            raise MultiSourceGroundError("ground slice overschrijdt receiptlimiet")
        materialized[source_id] = receipts

    workloads = tuple(
        _workload_contribution(
            source_id=source_id,
            target_scopes=scopes,
            receipts=materialized[source_id],
            evaluated_at_ms=evaluated_at_ms,
            source_error=errors.get(source_id),
        )
        for source_id in sorted(GROUND_SOURCE_IDS)
    )
    candidate_count = sum(
        len(target.candidates)
        for workload in workloads
        for target in workload.targets
    )
    if candidate_count > MAX_TOTAL_TARGET_CANDIDATES:
        raise MultiSourceGroundError("ground slice overschrijdt totale candidatelimiet")
    target_set_sha = _digest([item.to_mapping() for item in scopes])
    payload = {
        "authority_epoch": authority_epoch,
        "consensus": {
            "authority_sha256": GROUND_CONSENSUS_AUTHORITY_SHA256,
            "candidates": [],
            "reason_code": "cross_provider_semantics_certificate_unavailable",
            "status": "withheld",
        },
        "contract": GROUND_SLICE_CONTRACT,
        "desired_state_sha256": desired_state_sha256,
        "evaluated_at_ms": evaluated_at_ms,
        "freshness_policy_sha256": GROUND_FRESHNESS_POLICY_SHA256,
        "input_receipt_count": total_receipts,
        "target_candidate_count": candidate_count,
        "target_scopes": [item.to_mapping() for item in scopes],
        "target_set_sha256": target_set_sha,
        "workloads": [item.to_mapping() for item in workloads],
    }
    return AllGroundContributionSlice(
        desired_state_sha256=desired_state_sha256,
        authority_epoch=authority_epoch,
        evaluated_at_ms=evaluated_at_ms,
        target_scopes=scopes,
        target_set_sha256=target_set_sha,
        workloads=workloads,
        freshness_policy_sha256=GROUND_FRESHNESS_POLICY_SHA256,
        consensus_authority_sha256=GROUND_CONSENSUS_AUTHORITY_SHA256,
        consensus_status="withheld",
        consensus_reason_code=(
            "cross_provider_semantics_certificate_unavailable"
        ),
        consensus_candidates=(),
        input_receipt_count=total_receipts,
        target_candidate_count=candidate_count,
        slice_sha256=_digest(payload),
    )


def build_ground_cycle_contributions(
    ground_slice: AllGroundContributionSlice,
    *,
    cycle_completed_at_ms: int,
) -> tuple[SourceContribution, ...]:
    """Convert the exact five-source slice to cycle contribution envelopes."""

    if not isinstance(ground_slice, AllGroundContributionSlice):
        raise MultiSourceGroundError("cycleadapter vereist gevalideerde ground slice")
    completed = _integer(cycle_completed_at_ms, "cycle_completed_at_ms")
    if completed != ground_slice.evaluated_at_ms:
        raise MultiSourceGroundError("ground slice en cycle hebben ander evaluatiemoment")
    targets = tuple(item.target for item in ground_slice.target_scopes)
    result: list[SourceContribution] = []
    for workload in ground_slice.workloads:
        statuses = {item.status for item in workload.targets}
        candidates = tuple(
            candidate
            for target in workload.targets
            for candidate in target.candidates
        )
        unique_candidates: dict[str, GroundFactCandidate] = {}
        for candidate in candidates:
            previous = unique_candidates.get(candidate.fact_id)
            if previous is not None and (
                previous.fact_sha256 != candidate.fact_sha256
                or previous.receipt_sha256 != candidate.receipt_sha256
            ):
                raise MultiSourceGroundError("ground fact-id heeft twee authorities")
            unique_candidates.setdefault(candidate.fact_id, candidate)
        unique = tuple(sorted(
            unique_candidates.values(),
            key=lambda item: (item.observed_at_ms, item.fact_id),
        ))

        reason_code: str | None
        fresh_until_ms: int | None
        if GroundTargetStatus.CURRENT in statuses:
            status = CycleStatus.CURRENT
            reason_code = None
            fresh_until_ms = max(
                item.fresh_until_ms
                for item in unique
                if item.freshness == "current"
            )
        elif GroundTargetStatus.STALE in statuses:
            status = CycleStatus.STALE
            reason_code = "all_available_ground_candidates_stale"
            fresh_until_ms = max(item.fresh_until_ms for item in unique)
        elif GroundTargetStatus.ERROR in statuses:
            status = CycleStatus.ERROR
            reason_code = "ground_target_candidate_unavailable"
            fresh_until_ms = None
        elif GroundTargetStatus.BLOCKED in statuses:
            status = CycleStatus.BLOCKED
            reason_code = GROUND_DECODER_BINDINGS[workload.source_id].blocked_reason
            fresh_until_ms = None
        else:
            status = CycleStatus.NOT_APPLICABLE
            reason_code = "target_outside_catalog_scope"
            fresh_until_ms = None

        if status in {CycleStatus.CURRENT, CycleStatus.STALE}:
            if not unique:
                raise MultiSourceGroundError("ground datastatus heeft geen unieke facts")
            workload_bytes = _canonical_json(workload.to_mapping())
            evidence = EvidenceReference(
                role="target_ground_candidate_batch",
                content_sha256=workload.workload_sha256,
                receipt_sha256=workload.workload_receipt_sha256,
                size_bytes=len(workload_bytes),
                schema_id=GROUND_WORKLOAD_CONTRACT,
            )
            payload = GroundInput(
                window_start_ms=min(item.interval_start_ms for item in unique),
                window_end_ms=max(item.interval_end_ms for item in unique),
                station_count=len({item.site_id for item in unique}),
                observation_count=len(unique),
                quantity_ids=tuple(sorted({item.variable for item in unique})),
                evidence=(evidence,),
            )
        else:
            payload = empty_source_input(SourceKind.GROUND)
        result.append(SourceContribution.create(
            source_id=workload.source_id,
            targets=targets,
            status=status,
            payload=payload,
            reason_code=reason_code,
            fresh_until_ms=fresh_until_ms,
        ))
    return tuple(result)


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise MultiSourceGroundError(f"ground JSON bevat dubbele sleutel {key}")
        result[key] = value
    return result


def parse_all_ground_slice(
    value: bytes | bytearray | Mapping[str, Any],
) -> AllGroundContributionSlice:
    """Parse and fully rehash one untrusted all-ground slice."""

    raw_bytes: bytes | None = None
    if isinstance(value, (bytes, bytearray)):
        raw_bytes = bytes(value)
        if len(raw_bytes) > MAX_SLICE_BYTES:
            raise MultiSourceGroundError("ground slicebytes overschrijdt documentlimiet")
        try:
            decoded = json.loads(
                raw_bytes.decode("ascii"),
                object_pairs_hook=_reject_duplicate_json_keys,
            )
        except MultiSourceGroundError:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            raise MultiSourceGroundError("ground slicebytes zijn geen ASCII-JSON") from err
    elif isinstance(value, Mapping):
        if len(_canonical_json(value)) > MAX_SLICE_BYTES:
            raise MultiSourceGroundError("ground slice overschrijdt documentlimiet")
        decoded = value
    else:
        raise MultiSourceGroundError("ground slice moet bytes of mapping zijn")
    parsed = AllGroundContributionSlice.from_mapping(decoded)
    if raw_bytes is not None and parsed.to_canonical_bytes() != raw_bytes:
        raise MultiSourceGroundError("ground slicebytes zijn niet exact canoniek")
    return parsed


__all__ = [
    "AllGroundContributionSlice",
    "GROUND_CANDIDATE_CONTRACT",
    "GROUND_CATALOG_BY_ID",
    "GROUND_CONSENSUS_AUTHORITY_SHA256",
    "GROUND_DECODER_BINDINGS",
    "GROUND_FRESHNESS_POLICY_SHA256",
    "GROUND_RECEIPT_REFERENCE_CONTRACT",
    "GROUND_RECEIPT_SET_CONTRACT",
    "GROUND_SLICE_CONTRACT",
    "GROUND_SOURCE_IDS",
    "GROUND_TARGET_CONTRACT",
    "GROUND_TARGET_SCOPE_CONTRACT",
    "GROUND_WORKLOAD_CONTRACT",
    "GROUND_WORKLOAD_RECEIPT_SCHEMA",
    "GroundDecoderBinding",
    "GroundFactCandidate",
    "GroundReceiptReference",
    "GroundTargetContribution",
    "GroundTargetScope",
    "GroundTargetStatus",
    "GroundWorkloadContribution",
    "MAX_CANDIDATES_PER_TARGET",
    "MAX_FUTURE_SKEW_MS",
    "MAX_RECEIPTS_PER_SOURCE",
    "MAX_SLICE_BYTES",
    "MAX_TARGETS",
    "MAX_TOTAL_RECEIPTS",
    "MAX_TOTAL_TARGET_CANDIDATES",
    "MultiSourceGroundError",
    "build_all_ground_slice",
    "build_ground_cycle_contributions",
    "parse_all_ground_slice",
]
