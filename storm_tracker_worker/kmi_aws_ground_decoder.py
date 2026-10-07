"""Candidate-only decoder for the public KMI AWS 10-minute WFS feed.

The decoder replays only a complete, byte-verified schema-3 KMI capture.  It
uses the one field for which KMI publishes an exact interval and unit contract:
``PRECIP_QUANTITY`` is millimetres accumulated during the preceding ten
minutes.  The resulting facts remain unadmitted candidates; this module never
claims rain, dryness, coverage, confidence, or ground confirmation.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import time
from typing import Final

from .canonical_ground_evidence_store import (
    CanonicalGroundEvidenceStore,
    GroundCaptureMemberBinding,
    GroundMeasurementValue,
    GroundObservationFact,
    GroundObservationReceipt,
    GroundStationSite,
    ProviderQcFact,
)
from .kmi_opendata_raw import (
    AWS_10MIN_CAPTURE_PRODUCT,
    AWS_10MIN_LAYER,
    AWS_10MIN_RESPONSE_PRODUCT,
    KmiOpenDataError,
    validate_kmi_opendata_capture_for_recovery,
    validate_kmi_opendata_receipt_for_recovery,
)
from .raw_artifacts import RawArtifactError, RawArtifactStore


KMI_AWS_GROUND_DECODER_IDENTITY = "kmi_aws_10min_ground_evidence"
KMI_AWS_GROUND_DECODER_VERSION = "kmi-aws-ground-evidence-shadow-v1"
KMI_AWS_GROUND_ADMISSION_STATUS = "candidate_only_unadmitted"
KMI_AWS_PRECIPITATION_SEMANTICS = "preceding_10_minutes_official_kmi"
KMI_AWS_PRECIPITATION_VARIABLE = "precip_quantity"
KMI_AWS_PRECIPITATION_UNIT = "mm"

MAX_RESPONSE_BYTES: Final[int] = 32 * 1024 * 1024
MAX_JSON_DEPTH: Final[int] = 48
MAX_JSON_NODES: Final[int] = 250_000
MAX_FEATURES: Final[int] = 8_192
MAX_FEATURE_CANONICAL_BYTES: Final[int] = 256 * 1024
DEFAULT_DECODE_SECONDS: Final[float] = 30.0


class KmiAwsGroundDecoderError(RuntimeError):
    """Base class for a fail-closed KMI AWS candidate decode."""


class KmiAwsGroundDecoderContractError(KmiAwsGroundDecoderError):
    """The verified WFS bytes do not satisfy the bounded decoder contract."""


class KmiAwsGroundDecoderDeadlineError(KmiAwsGroundDecoderError):
    """The single absolute monotone deadline elapsed."""


class KmiAwsGroundDecoderCapacityError(KmiAwsGroundDecoderError):
    """A hard input or output bound would be exceeded."""


class KmiAwsGroundCaptureError(KmiAwsGroundDecoderError):
    """The requested complete KMI AWS capture is unavailable or invalid."""


class _JsonNumber(str):
    """Exact JSON number lexeme, kept distinct from provider strings."""


@dataclass(frozen=True, slots=True)
class KmiAwsGroundDecodeResult:
    capture_id: str
    feature_count: int
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
            "decoder_identity": KMI_AWS_GROUND_DECODER_IDENTITY,
            "decoder_version": KMI_AWS_GROUND_DECODER_VERSION,
            "capture_id": self.capture_id,
            "feature_count": self.feature_count,
            "fact_count": self.fact_count,
            "fact_ids": [item.fact_id for item in self.fact_receipts],
            "config_sha256": self.config_sha256,
            "measurement_semantics": KMI_AWS_PRECIPITATION_SEMANTICS,
            "wet_claim": False,
            "dry_claim": False,
            "coverage_claim": False,
            "ground_confirmation_eligible": False,
            "operational_admission_eligible": False,
            "admission_status": KMI_AWS_GROUND_ADMISSION_STATUS,
        }


def _check_deadline(deadline_at: float) -> None:
    if time.monotonic() >= deadline_at:
        raise KmiAwsGroundDecoderDeadlineError(
            "KMI AWS ground-decode overschrijdt de absolute deadline"
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


def _reject_constant(value: str) -> object:
    raise KmiAwsGroundDecoderContractError(
        f"KMI AWS JSON bevat niet-eindige constante {value}"
    )


def _object_without_duplicates(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise KmiAwsGroundDecoderContractError(
                "KMI AWS JSON bevat een dubbele objectkey"
            )
        result[key] = value
    return result


def _check_json_nesting(payload: bytes) -> None:
    depth = 0
    in_string = False
    escaped = False
    for byte in payload:
        if in_string:
            if escaped:
                escaped = False
            elif byte == 0x5C:
                escaped = True
            elif byte == 0x22:
                in_string = False
            continue
        if byte == 0x22:
            in_string = True
        elif byte in (0x7B, 0x5B):
            depth += 1
            if depth > MAX_JSON_DEPTH:
                raise KmiAwsGroundDecoderCapacityError(
                    "KMI AWS JSON overschrijdt de dieptegrens"
                )
        elif byte in (0x7D, 0x5D):
            depth -= 1
            if depth < 0:
                raise KmiAwsGroundDecoderContractError(
                    "KMI AWS JSON heeft ongeldige nesting"
                )
    if in_string or escaped or depth != 0:
        raise KmiAwsGroundDecoderContractError(
            "KMI AWS JSON is afgekapt of ongeldig genest"
        )


def _bounded_json(payload: bytes) -> object:
    if type(payload) is not bytes or not payload:
        raise KmiAwsGroundDecoderContractError(
            "KMI AWS responsemember bevat geen exacte bytes"
        )
    if len(payload) > MAX_RESPONSE_BYTES:
        raise KmiAwsGroundDecoderCapacityError(
            "KMI AWS responsemember overschrijdt de bytegrens"
        )
    if payload.startswith(b"\xef\xbb\xbf"):
        raise KmiAwsGroundDecoderContractError(
            "KMI AWS JSON mag geen UTF-8 BOM bevatten"
        )
    _check_json_nesting(payload)
    try:
        value = json.loads(
            payload.decode("utf-8", errors="strict"),
            object_pairs_hook=_object_without_duplicates,
            parse_int=_JsonNumber,
            parse_float=_JsonNumber,
            parse_constant=_reject_constant,
        )
    except KmiAwsGroundDecoderError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise KmiAwsGroundDecoderContractError(
            "KMI AWS response is geen geldige begrensde JSON"
        ) from exc

    nodes = 0
    stack: list[object] = [value]
    while stack:
        item = stack.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES:
            raise KmiAwsGroundDecoderCapacityError(
                "KMI AWS JSON overschrijdt de nodegrens"
            )
        if isinstance(item, dict):
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
    return value


def _decimal(value: object, label: str) -> Decimal:
    if not isinstance(value, _JsonNumber):
        raise KmiAwsGroundDecoderContractError(
            f"KMI AWS {label} is geen exact JSON-getal"
        )
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise KmiAwsGroundDecoderContractError(
            f"KMI AWS {label} is geen exact decimaal"
        ) from exc
    if not result.is_finite() or result.copy_abs() > Decimal("1e100"):
        raise KmiAwsGroundDecoderContractError(
            f"KMI AWS {label} ligt buiten het decimaalcontract"
        )
    return result


def _coordinate(value: object, label: str, low: str, high: str) -> str:
    result = _decimal(value, label)
    if not Decimal(low) <= result <= Decimal(high):
        raise KmiAwsGroundDecoderContractError(
            f"KMI AWS {label} ligt buiten het bereik"
        )
    return str(value)


def _station_code(value: object) -> str:
    if isinstance(value, _JsonNumber):
        parsed = _decimal(value, "stationcode")
        integral = parsed.to_integral_value()
        if parsed != integral:
            raise KmiAwsGroundDecoderContractError(
                "KMI AWS stationcode is niet integer"
            )
        result = str(integral)
    elif isinstance(value, str):
        result = value
    else:
        raise KmiAwsGroundDecoderContractError(
            "KMI AWS stationcode heeft een onbekend type"
        )
    if (
        not result
        or len(result) > 64
        or any(character not in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz._-" for character in result)
    ):
        raise KmiAwsGroundDecoderContractError(
            "KMI AWS stationcode valt buiten het identifiercontract"
        )
    return result


def _observed_at(value: object) -> float:
    if not isinstance(value, str) or len(value) != 20 or not value.endswith("Z"):
        raise KmiAwsGroundDecoderContractError(
            "KMI AWS timestamp is niet exact UTC per seconde"
        )
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise KmiAwsGroundDecoderContractError(
            "KMI AWS timestamp is ongeldig"
        ) from exc
    return parsed.timestamp()


def _canonical_record_value(value: object) -> object:
    if isinstance(value, _JsonNumber):
        return {"$number": str(value)}
    if isinstance(value, dict):
        return {
            key: _canonical_record_value(item)
            for key, item in sorted(value.items())
        }
    if isinstance(value, list):
        return [_canonical_record_value(item) for item in value]
    return value


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
        "decoder_identity": KMI_AWS_GROUND_DECODER_IDENTITY,
        "decoder_version": KMI_AWS_GROUND_DECODER_VERSION,
        "accepted_capture_product": AWS_10MIN_CAPTURE_PRODUCT,
        "accepted_member_product": AWS_10MIN_RESPONSE_PRODUCT,
        "accepted_member_role": "geoserver_response",
        "accepted_layer": AWS_10MIN_LAYER,
        "variable": KMI_AWS_PRECIPITATION_VARIABLE,
        "unit": KMI_AWS_PRECIPITATION_UNIT,
        "measurement_semantics": KMI_AWS_PRECIPITATION_SEMANTICS,
        "interval_seconds": 600,
        "timestamp_policy": "provider_timestamp_is_interval_end",
        "record_hash_policy": "canonical_feature_json_exact_number_lexemes_v1",
        "negative_or_extreme_value_policy": "explicit_unknown",
        "bounds": {
            "max_response_bytes": MAX_RESPONSE_BYTES,
            "max_json_depth": MAX_JSON_DEPTH,
            "max_json_nodes": MAX_JSON_NODES,
            "max_features": MAX_FEATURES,
            "max_feature_canonical_bytes": MAX_FEATURE_CANONICAL_BYTES,
        },
        "operational_admission_eligible": False,
    }
    return hashlib.sha256(_canonical_json(contract)).hexdigest()


def _provider_qc(properties: dict[str, object]) -> tuple[ProviderQcFact, ...]:
    raw = properties.get("qc_flags")
    if raw is None:
        return (
            ProviderQcFact(
                flag="kmi.precip_quantity.validation_unreported",
                value=None,
                text="provider QC field absent; not interpreted as admission",
            ),
        )
    if not isinstance(raw, str) or len(raw) > 8_192:
        raise KmiAwsGroundDecoderContractError(
            "KMI AWS qc_flags valt buiten het tekstcontract"
        )
    try:
        parsed = json.loads(
            raw,
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
    except KmiAwsGroundDecoderError:
        raise
    except (json.JSONDecodeError, RecursionError) as exc:
        raise KmiAwsGroundDecoderContractError(
            "KMI AWS qc_flags is geen geldige JSON"
        ) from exc
    validated = parsed.get("validated") if isinstance(parsed, dict) else None
    flag = validated.get("PRECIP_QUANTITY") if isinstance(validated, dict) else None
    if isinstance(flag, bool):
        return (
            ProviderQcFact(
                flag="kmi.precip_quantity.validated",
                value="true" if flag else "false",
                text="provider flag preserved; not interpreted as admission",
            ),
        )
    return (
        ProviderQcFact(
            flag="kmi.precip_quantity.validation_unreported",
            value=None,
            text="provider QC has no boolean PRECIP_QUANTITY flag",
        ),
    )


def _measurement(properties: dict[str, object]) -> GroundMeasurementValue:
    value = properties.get("precip_quantity")
    if value is None:
        return GroundMeasurementValue(
            variable=KMI_AWS_PRECIPITATION_VARIABLE,
            state="missing",
            representation="none",
            exact_value=None,
            unit=KMI_AWS_PRECIPITATION_UNIT,
            censor_limit=None,
        )
    amount = _decimal(value, "precip_quantity")
    if amount < 0 or amount > Decimal("10000"):
        return GroundMeasurementValue(
            variable=KMI_AWS_PRECIPITATION_VARIABLE,
            state="unknown",
            representation="none",
            exact_value=None,
            unit=KMI_AWS_PRECIPITATION_UNIT,
            censor_limit=None,
        )
    return GroundMeasurementValue(
        variable=KMI_AWS_PRECIPITATION_VARIABLE,
        state="observed",
        representation="decimal",
        exact_value=str(value),
        unit=KMI_AWS_PRECIPITATION_UNIT,
        censor_limit=None,
    )


def _validate_capture_bundle(capture, receipts, payloads):
    if (
        capture.provider != "kmi"
        or capture.product != AWS_10MIN_CAPTURE_PRODUCT
        or capture.product_timestamp is not None
        or not capture.complete
        or not capture.fetch_id.endswith(":capture")
        or set(receipts) != {"geoserver_response"}
        or set(payloads) != {"geoserver_response"}
        or len(capture.members) != 1
        or capture.members[0].role != "geoserver_response"
    ):
        raise KmiAwsGroundCaptureError(
            "KMI AWS capture heeft niet het exacte complete responsecontract"
        )
    job_id = capture.fetch_id[: -len(":capture")]
    receipt = receipts["geoserver_response"]
    payload = payloads["geoserver_response"]
    try:
        descriptor = validate_kmi_opendata_receipt_for_recovery(
            receipt,
            job_id=job_id,
        )
        validate_kmi_opendata_capture_for_recovery(
            capture,
            receipt,
            job_id=job_id,
        )
    except KmiOpenDataError as exc:
        raise KmiAwsGroundCaptureError(
            "KMI AWS capture faalt zijn exacte raw authority"
        ) from exc
    if (
        descriptor.service != "WFS"
        or descriptor.layer != AWS_10MIN_LAYER
        or receipt.product != AWS_10MIN_RESPONSE_PRODUCT
        or receipt.content_type not in {"application/json", "application/geo+json"}
        or receipt.size_bytes != len(payload)
        or receipt.content_sha256 != hashlib.sha256(payload).hexdigest()
    ):
        raise KmiAwsGroundCaptureError(
            "KMI AWS responsemember heeft een verkeerde identiteit"
        )
    return receipt, payload


def decode_kmi_aws_ground_capture(
    capture_id: str,
    *,
    raw_store: RawArtifactStore,
    ground_store: CanonicalGroundEvidenceStore,
    deadline_at: float | None = None,
) -> KmiAwsGroundDecodeResult:
    """Replay one complete KMI AWS capture into candidate-only ground facts."""

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
        raise KmiAwsGroundCaptureError(
            "KMI AWS schema-3 capture kon niet byte-exact worden geverifieerd"
        ) from exc
    _check_deadline(deadline)
    if bundle is None:
        raise KmiAwsGroundCaptureError(
            "KMI AWS schema-3 capture bestaat niet of is incompleet"
        )
    capture, receipts, payloads = bundle
    receipt, payload = _validate_capture_bundle(capture, receipts, payloads)
    root = _bounded_json(payload)
    _check_deadline(deadline)
    if not isinstance(root, dict) or root.get("type") != "FeatureCollection":
        raise KmiAwsGroundDecoderContractError(
            "KMI AWS response-root is geen FeatureCollection"
        )
    features = root.get("features")
    if not isinstance(features, list):
        raise KmiAwsGroundDecoderContractError(
            "KMI AWS response mist de features-array"
        )
    if len(features) > MAX_FEATURES:
        raise KmiAwsGroundDecoderCapacityError(
            "KMI AWS response overschrijdt de featuregrens"
        )

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
    feature_ids: set[str] = set()
    for index, feature in enumerate(features):
        _check_deadline(deadline)
        if not isinstance(feature, dict) or feature.get("type") != "Feature":
            raise KmiAwsGroundDecoderContractError(
                "KMI AWS features-array bevat geen Feature"
            )
        feature_id = feature.get("id")
        geometry = feature.get("geometry")
        properties = feature.get("properties")
        if (
            not isinstance(feature_id, str)
            or not feature_id
            or len(feature_id) > 512
            or feature_id in feature_ids
            or not isinstance(geometry, dict)
            or geometry.get("type") != "Point"
            or not isinstance(properties, dict)
        ):
            raise KmiAwsGroundDecoderContractError(
                "KMI AWS Feature-identiteit of structuur is ongeldig"
            )
        feature_ids.add(feature_id)
        coordinates = geometry.get("coordinates")
        if not isinstance(coordinates, list) or len(coordinates) != 2:
            raise KmiAwsGroundDecoderContractError(
                "KMI AWS Feature mist puntcoordinaten"
            )
        longitude = _coordinate(coordinates[0], "longitude", "-180", "180")
        latitude = _coordinate(coordinates[1], "latitude", "-90", "90")
        station_code = _station_code(properties.get("code"))
        observed_at = _observed_at(properties.get("timestamp"))
        record_bytes = _canonical_json(_canonical_record_value(feature))
        if not record_bytes or len(record_bytes) > MAX_FEATURE_CANONICAL_BYTES:
            raise KmiAwsGroundDecoderCapacityError(
                "KMI AWS canonical feature valt buiten de bytegrens"
            )
        facts.append(GroundObservationFact(
            capture_id=capture.capture_id,
            capture_receipt_sha256=capture.capture_receipt_sha256,
            members=member_bindings,
            source_member_role="geoserver_response",
            source_member_content_sha256=source_member.content_sha256,
            source_member_receipt_sha256=source_member.receipt_sha256,
            source_record_locator=f"/features/{index}:{feature_id}",
            source_record_sha256=hashlib.sha256(record_bytes).hexdigest(),
            decoder_identity=KMI_AWS_GROUND_DECODER_IDENTITY,
            decoder_version=KMI_AWS_GROUND_DECODER_VERSION,
            config_sha256=config_sha256,
            provider="kmi",
            product=AWS_10MIN_CAPTURE_PRODUCT,
            station=GroundStationSite(
                provider_station_id=station_code,
                site_id=f"kmi:aws:{station_code}",
                network="kmi_aws",
                station_name=None,
                position_state="known",
                latitude_decimal=latitude,
                longitude_decimal=longitude,
                elevation_state="unknown",
                elevation_m_decimal=None,
            ),
            observed_at=observed_at,
            representative_interval_start=observed_at - 600.0,
            representative_interval_end=observed_at,
            received_at=float(receipt.received_at),
            ingested_at=ingested_at,
            measurement=_measurement(properties),
            provider_qc=_provider_qc(properties),
        ))

    _check_deadline(deadline)
    committed = ground_store.commit_batch(tuple(facts), deadline_at=deadline)
    _check_deadline(deadline)
    return KmiAwsGroundDecodeResult(
        capture_id=capture.capture_id,
        feature_count=len(features),
        fact_receipts=committed,
        config_sha256=config_sha256,
    )


__all__ = [
    "DEFAULT_DECODE_SECONDS",
    "KMI_AWS_GROUND_ADMISSION_STATUS",
    "KMI_AWS_GROUND_DECODER_IDENTITY",
    "KMI_AWS_GROUND_DECODER_VERSION",
    "KMI_AWS_PRECIPITATION_SEMANTICS",
    "KMI_AWS_PRECIPITATION_UNIT",
    "KMI_AWS_PRECIPITATION_VARIABLE",
    "KmiAwsGroundCaptureError",
    "KmiAwsGroundDecodeResult",
    "KmiAwsGroundDecoderCapacityError",
    "KmiAwsGroundDecoderContractError",
    "KmiAwsGroundDecoderDeadlineError",
    "KmiAwsGroundDecoderError",
    "decode_kmi_aws_ground_capture",
]
