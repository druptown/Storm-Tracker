"""Strict, inert request contract for a future worker-owned radar cycle.

V1 carries geographic routing metadata, targets and compatibility artifact
handles.  Operational v2 replaces each single handle with a payload-free link
to every member of one immutable ``RawArtifactStore`` capture set.  Neither
contract carries decoded observations, rasters, cells, motion, speed or
intensity values, so this layer cannot clip or rewrite meteorological values.

An artifact handle is evidence *about bytes*, not evidence that a location is
dry.  In particular an empty decoded-legacy result, incomplete coverage or
unknown coverage always remains ``unknown`` at this boundary.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
from pathlib import PurePosixPath
import re
from typing import Any, Mapping

from .raw_artifacts import (
    RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RAW_ARTIFACT_SCHEMA_VERSION,
)


REQUEST_CONTRACT_V1 = "stv3-radar-cycle-request-v1"
REQUEST_CONTRACT_V2 = "stv3-radar-cycle-request-v2"
# Stable compatibility name for callers which still construct/read v1.
REQUEST_CONTRACT = REQUEST_CONTRACT_V1
OPERATIONAL_REQUEST_CONTRACT = REQUEST_CONTRACT_V2
MAX_REGIONS = 16
MAX_TARGETS = 64
MAX_INPUTS = 48
MAX_CAPTURE_MEMBERS = 16
MAX_REQUEST_BYTES = 96 * 1024
MAX_ARTIFACT_BYTES = (1 << 63) - 1

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PROVIDER_RE = re.compile(r"^[a-z0-9][a-z0-9_]{0,63}$")
_STABLE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_OPAQUE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}$")
_PRODUCT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_MEDIA_TYPE_RE = re.compile(
    r"^[a-z0-9][a-z0-9.+-]*/[a-z0-9][a-z0-9.+-]*$"
)
_CAPTURE_ID_RE = re.compile(r"^capture-[0-9a-f]{64}$")
_RAW_RECEIPT_ID_RE = re.compile(r"^raw-[0-9a-f]{64}$")

RAW_CAPTURE_PROVENANCE_GRADE = "lossless_raw_capture_set"
RAW_ARTIFACT_STORE_SCHEMA_VERSION = RAW_ARTIFACT_SCHEMA_VERSION


class ProvenanceGrade(str, Enum):
    """What the referenced artifact actually is, without inferred lineage."""

    DECODED_LEGACY = "decoded_legacy"
    RAW_NATIVE = "raw_native"


class ArtifactKind(str, Enum):
    DECODED_PROVIDER_RESULT = "decoded_provider_result"
    UPSTREAM_NATIVE_PRODUCT = "upstream_native_product"


class CoverageStatus(str, Enum):
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    UNKNOWN = "unknown"


class PrecipitationState(str, Enum):
    WET = "wet"
    DRY = "dry"
    UNKNOWN = "unknown"


def _finite_number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric and not boolean")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite")
    return number


def _timestamp(value: object, name: str) -> float:
    number = _finite_number(value, name)
    if number < 0.0:
        raise ValueError(f"{name} must be non-negative")
    return number


def _latitude(value: object, name: str) -> float:
    number = _finite_number(value, name)
    if not -90.0 <= number <= 90.0:
        raise ValueError(f"{name} must be in [-90, 90]")
    return number


def _longitude(value: object, name: str) -> float:
    number = _finite_number(value, name)
    if not -180.0 <= number <= 180.0:
        raise ValueError(f"{name} must be in [-180, 180]")
    return number


def _regex_string(
    value: object,
    name: str,
    pattern: re.Pattern[str],
) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ValueError(f"{name} has invalid canonical syntax")
    return value


def _optional_opaque(value: object, name: str) -> str | None:
    if value is None:
        return None
    result = _regex_string(value, name, _OPAQUE_ID_RE)
    if "://" in result:
        raise ValueError(f"{name} must be an opaque ID, not a URL")
    return result


def _exact_mapping(
    value: object,
    *,
    name: str,
    keys: frozenset[str],
) -> dict[str, Any]:
    if type(value) is not dict:
        raise TypeError(f"{name} must be a JSON object")
    if any(not isinstance(key, str) for key in value):
        raise TypeError(f"{name} keys must be strings")
    actual = frozenset(value)
    if actual != keys:
        missing = sorted(keys - actual)
        extra = sorted(actual - keys)
        raise ValueError(
            f"{name} keys do not match contract; missing={missing}, extra={extra}"
        )
    return value


def _exact_list(value: object, name: str, maximum: int) -> list:
    if type(value) is not list:
        raise TypeError(f"{name} must be a JSON array")
    if len(value) > maximum:
        raise ValueError(f"{name} exceeds maximum count {maximum}")
    return value


def _enum(value: object, enum_type: type[Enum], name: str):
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    try:
        return enum_type(value)
    except ValueError as err:
        raise ValueError(f"{name} has unsupported value {value!r}") from err


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sha256_document(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class ArtifactHandle:
    artifact_path: str
    artifact_sha256: str
    artifact_bytes: int
    content_type: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.artifact_path, str)
            or not self.artifact_path
            or len(self.artifact_path) > 2048
            or "\x00" in self.artifact_path
            or "?" in self.artifact_path
            or "#" in self.artifact_path
            or "\\" in self.artifact_path
        ):
            raise ValueError("artifact_path must be a bounded local POSIX path")
        path = PurePosixPath(self.artifact_path)
        if (
            not path.is_absolute()
            or ".." in path.parts
            or path.name in {"", ".", ".."}
            or str(path) != self.artifact_path
        ):
            raise ValueError("artifact_path must be absolute and traversal-free")
        if not isinstance(self.artifact_sha256, str) or not _SHA256_RE.fullmatch(
            self.artifact_sha256
        ):
            raise ValueError("artifact_sha256 must be lowercase SHA-256")
        if (
            isinstance(self.artifact_bytes, bool)
            or not isinstance(self.artifact_bytes, int)
            or not 0 < self.artifact_bytes <= MAX_ARTIFACT_BYTES
        ):
            raise ValueError("artifact_bytes must be a positive signed-64-bit integer")
        if not isinstance(self.content_type, str) or not _MEDIA_TYPE_RE.fullmatch(
            self.content_type
        ):
            raise ValueError("content_type must be canonical and parameter-free")

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_path": self.artifact_path,
            "artifact_sha256": self.artifact_sha256,
            "artifact_bytes": self.artifact_bytes,
            "content_type": self.content_type,
        }


@dataclass(frozen=True, slots=True)
class RadarInputProvenance:
    grade: ProvenanceGrade
    artifact_kind: ArtifactKind
    exact_raw_artifacts_complete: bool
    parser_id: str | None
    upstream_record_id: str | None
    raw_artifact_receipt_id: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.grade, ProvenanceGrade):
            raise TypeError("grade must be ProvenanceGrade")
        if not isinstance(self.artifact_kind, ArtifactKind):
            raise TypeError("artifact_kind must be ArtifactKind")
        if type(self.exact_raw_artifacts_complete) is not bool:
            raise TypeError("exact_raw_artifacts_complete must be boolean")
        object.__setattr__(
            self, "parser_id", _optional_opaque(self.parser_id, "parser_id")
        )
        object.__setattr__(
            self,
            "upstream_record_id",
            _optional_opaque(self.upstream_record_id, "upstream_record_id"),
        )
        object.__setattr__(
            self,
            "raw_artifact_receipt_id",
            _optional_opaque(
                self.raw_artifact_receipt_id, "raw_artifact_receipt_id"
            ),
        )
        if self.grade is ProvenanceGrade.DECODED_LEGACY:
            if self.artifact_kind is not ArtifactKind.DECODED_PROVIDER_RESULT:
                raise ValueError(
                    "decoded_legacy requires decoded_provider_result"
                )
            if self.exact_raw_artifacts_complete:
                raise ValueError("decoded_legacy cannot claim complete exact raw artifacts")
            if self.raw_artifact_receipt_id is not None:
                raise ValueError(
                    "decoded_legacy cannot claim a raw artifact receipt in v1"
                )
        else:
            if self.artifact_kind is not ArtifactKind.UPSTREAM_NATIVE_PRODUCT:
                raise ValueError("raw_native requires upstream_native_product")
            if not self.exact_raw_artifacts_complete:
                raise ValueError("raw_native requires complete exact raw artifacts")
            if self.raw_artifact_receipt_id is None:
                raise ValueError("raw_native requires raw_artifact_receipt_id")
            if self.parser_id is not None:
                raise ValueError("raw_native parser_id must remain unknown before decode")

    def to_dict(self) -> dict[str, Any]:
        return {
            "grade": self.grade.value,
            "artifact_kind": self.artifact_kind.value,
            "exact_raw_artifacts_complete": self.exact_raw_artifacts_complete,
            "parser_id": self.parser_id,
            "upstream_record_id": self.upstream_record_id,
            "raw_artifact_receipt_id": self.raw_artifact_receipt_id,
        }


@dataclass(frozen=True, slots=True)
class CoverageDescriptor:
    status: CoverageStatus
    bbox: tuple[float, float, float, float] | None

    def __post_init__(self) -> None:
        if not isinstance(self.status, CoverageStatus):
            raise TypeError("coverage status must be CoverageStatus")
        if self.bbox is None:
            if self.status is CoverageStatus.COMPLETE:
                raise ValueError("complete coverage requires an explicit bbox")
            return
        if not isinstance(self.bbox, tuple) or len(self.bbox) != 4:
            raise TypeError("coverage bbox must be a four-value tuple or null")
        west = _longitude(self.bbox[0], "coverage west")
        south = _latitude(self.bbox[1], "coverage south")
        east = _longitude(self.bbox[2], "coverage east")
        north = _latitude(self.bbox[3], "coverage north")
        if west == east or south >= north:
            raise ValueError("coverage bbox has no positive geographic extent")
        object.__setattr__(self, "bbox", (west, south, east, north))

    @property
    def crosses_antimeridian(self) -> bool:
        return self.bbox is not None and self.bbox[0] > self.bbox[2]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "bbox": list(self.bbox) if self.bbox is not None else None,
        }


@dataclass(frozen=True, slots=True)
class RadarRegion:
    region_id: str
    center_lat: float
    center_lon: float
    radius_km: float

    def __post_init__(self) -> None:
        region_id = _regex_string(self.region_id, "region_id", _STABLE_ID_RE)
        if "@" in region_id:
            raise ValueError("region_id must be stable and must not embed coordinates")
        object.__setattr__(self, "region_id", region_id)
        object.__setattr__(self, "center_lat", _latitude(self.center_lat, "center_lat"))
        object.__setattr__(self, "center_lon", _longitude(self.center_lon, "center_lon"))
        radius = _finite_number(self.radius_km, "radius_km")
        if radius <= 0.0:
            raise ValueError("radius_km must be positive")
        object.__setattr__(self, "radius_km", radius)

    @property
    def storage_region_id(self) -> str:
        """Caller-owned durable identity; never derived from coordinates."""
        return self.region_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "region_id": self.region_id,
            "center_lat": self.center_lat,
            "center_lon": self.center_lon,
            "radius_km": self.radius_km,
        }


@dataclass(frozen=True, slots=True)
class RadarTarget:
    target_id: str
    region_id: str
    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "target_id", _regex_string(self.target_id, "target_id", _STABLE_ID_RE)
        )
        region_id = _regex_string(self.region_id, "target region_id", _STABLE_ID_RE)
        if "@" in region_id:
            raise ValueError("target region_id must be a stable storage ID")
        object.__setattr__(self, "region_id", region_id)
        object.__setattr__(self, "latitude", _latitude(self.latitude, "target latitude"))
        object.__setattr__(
            self, "longitude", _longitude(self.longitude, "target longitude")
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_id": self.target_id,
            "region_id": self.region_id,
            "latitude": self.latitude,
            "longitude": self.longitude,
        }


@dataclass(frozen=True, slots=True)
class RadarInput:
    provider: str
    product: str
    region_ids: tuple[str, ...]
    artifact: ArtifactHandle
    provenance: RadarInputProvenance
    product_valid_at: float
    received_at: float
    coverage: CoverageDescriptor

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "provider", _regex_string(self.provider, "provider", _PROVIDER_RE)
        )
        object.__setattr__(
            self, "product", _regex_string(self.product, "product", _PRODUCT_RE)
        )
        if "://" in self.product:
            raise ValueError("product must be an opaque product ID, not a URL")
        if not isinstance(self.region_ids, tuple) or not self.region_ids:
            raise ValueError("input region_ids must be a non-empty tuple")
        region_ids = tuple(
            sorted(
                _regex_string(value, "input region_id", _STABLE_ID_RE)
                for value in self.region_ids
            )
        )
        if any("@" in value for value in region_ids):
            raise ValueError("input region_ids must be stable storage IDs")
        if len(region_ids) > MAX_REGIONS or len(region_ids) != len(set(region_ids)):
            raise ValueError("input region_ids exceed bounds or contain duplicates")
        object.__setattr__(self, "region_ids", region_ids)
        if not isinstance(self.artifact, ArtifactHandle):
            raise TypeError("artifact must be ArtifactHandle")
        if not isinstance(self.provenance, RadarInputProvenance):
            raise TypeError("provenance must be RadarInputProvenance")
        if not isinstance(self.coverage, CoverageDescriptor):
            raise TypeError("coverage must be CoverageDescriptor")
        # These two clocks are independent facts.  No ordering relation,
        # substitution, latency correction or rounding is applied here.
        object.__setattr__(
            self,
            "product_valid_at",
            _timestamp(self.product_valid_at, "product_valid_at"),
        )
        object.__setattr__(
            self, "received_at", _timestamp(self.received_at, "received_at")
        )

    @property
    def input_id(self) -> str:
        return "radar-input:" + _sha256_document(self.to_dict())

    @property
    def empty_precipitation_state(self) -> PrecipitationState:
        """A handle or empty decode alone never proves a dry atmosphere."""
        return PrecipitationState.UNKNOWN

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "product": self.product,
            "region_ids": list(self.region_ids),
            "artifact": self.artifact.to_dict(),
            "provenance": self.provenance.to_dict(),
            "product_valid_at": self.product_valid_at,
            "received_at": self.received_at,
            "coverage": self.coverage.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class RawCaptureMemberRef:
    """Payload-free, exact binding for one member of a raw capture set."""

    member_id: str
    role: str
    receipt_id: str
    content_sha256: str
    receipt_sha256: str
    received_at: float

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "member_id",
            _regex_string(self.member_id, "capture member_id", _STABLE_ID_RE),
        )
        object.__setattr__(
            self,
            "role",
            _regex_string(self.role, "capture member role", _STABLE_ID_RE),
        )
        object.__setattr__(
            self,
            "receipt_id",
            _regex_string(
                self.receipt_id,
                "capture member receipt_id",
                _RAW_RECEIPT_ID_RE,
            ),
        )
        for name in ("content_sha256", "receipt_sha256"):
            value = getattr(self, name)
            if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
                raise ValueError(f"{name} must be lowercase SHA-256")
        object.__setattr__(
            self,
            "received_at",
            _timestamp(self.received_at, "capture member received_at"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "member_id": self.member_id,
            "role": self.role,
            "receipt_id": self.receipt_id,
            "content_sha256": self.content_sha256,
            "receipt_sha256": self.receipt_sha256,
            "received_at": self.received_at,
        }


@dataclass(frozen=True, slots=True)
class RawCaptureSetProvenance:
    """Proof that the reference names one complete RawArtifactStore set."""

    grade: str
    complete: bool
    raw_store_schema_version: int
    capture_schema: str
    capture_receipt_schema: str
    capture_receipt_sha256: str

    def __post_init__(self) -> None:
        if self.grade != RAW_CAPTURE_PROVENANCE_GRADE:
            raise ValueError("capture provenance grade must be lossless raw set")
        if self.complete is not True:
            raise ValueError("capture provenance must explicitly be complete")
        if self.raw_store_schema_version != RAW_ARTIFACT_STORE_SCHEMA_VERSION:
            raise ValueError("unsupported raw artifact store schema version")
        if self.capture_schema != RAW_ARTIFACT_CAPTURE_SCHEMA:
            raise ValueError("unsupported raw artifact capture schema")
        if self.capture_receipt_schema != RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA:
            raise ValueError("unsupported raw artifact capture receipt schema")
        if (
            not isinstance(self.capture_receipt_sha256, str)
            or not _SHA256_RE.fullmatch(self.capture_receipt_sha256)
        ):
            raise ValueError("capture_receipt_sha256 must be lowercase SHA-256")

    def to_dict(self) -> dict[str, Any]:
        return {
            "grade": self.grade,
            "complete": self.complete,
            "raw_store_schema_version": self.raw_store_schema_version,
            "capture_schema": self.capture_schema,
            "capture_receipt_schema": self.capture_receipt_schema,
            "capture_receipt_sha256": self.capture_receipt_sha256,
        }


@dataclass(frozen=True, slots=True)
class RawCaptureSetRef:
    """Canonical, lossless link to every member of one atomic raw capture."""

    capture_id: str
    fetch_id: str
    upstream_capture_id: str
    manifest_sha256: str
    product_timestamp: float
    members: tuple[RawCaptureMemberRef, ...]
    provenance: RawCaptureSetProvenance

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "capture_id",
            _regex_string(self.capture_id, "capture_id", _CAPTURE_ID_RE),
        )
        fetch_id = _regex_string(
            self.fetch_id,
            "capture fetch_id",
            _OPAQUE_ID_RE,
        )
        upstream_capture_id = _regex_string(
            self.upstream_capture_id,
            "capture upstream_capture_id",
            _OPAQUE_ID_RE,
        )
        if "://" in fetch_id or "://" in upstream_capture_id:
            raise ValueError("capture identities must be opaque, not URLs")
        object.__setattr__(self, "fetch_id", fetch_id)
        object.__setattr__(self, "upstream_capture_id", upstream_capture_id)
        if (
            not isinstance(self.manifest_sha256, str)
            or not _SHA256_RE.fullmatch(self.manifest_sha256)
        ):
            raise ValueError("manifest_sha256 must be lowercase SHA-256")
        object.__setattr__(
            self,
            "product_timestamp",
            _timestamp(self.product_timestamp, "capture product_timestamp"),
        )
        if (
            not isinstance(self.members, tuple)
            or not self.members
            or len(self.members) > MAX_CAPTURE_MEMBERS
            or any(not isinstance(value, RawCaptureMemberRef) for value in self.members)
        ):
            raise ValueError("capture members must be a non-empty bounded tuple")
        members = tuple(
            sorted(self.members, key=lambda value: (value.role, value.member_id))
        )
        for name, values in (
            ("member_id", [value.member_id for value in members]),
            ("role", [value.role for value in members]),
            ("receipt_id", [value.receipt_id for value in members]),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"capture members contain duplicate {name}")
        object.__setattr__(self, "members", members)
        if not isinstance(self.provenance, RawCaptureSetProvenance):
            raise TypeError("capture provenance must be RawCaptureSetProvenance")

    @property
    def received_at(self) -> float:
        """The set cannot exist operationally before its last member arrived."""
        return max(value.received_at for value in self.members)

    @property
    def capture_ref_id(self) -> str:
        return "radar-capture-ref:" + _sha256_document(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "capture_id": self.capture_id,
            "fetch_id": self.fetch_id,
            "upstream_capture_id": self.upstream_capture_id,
            "manifest_sha256": self.manifest_sha256,
            "product_timestamp": self.product_timestamp,
            "members": [value.to_dict() for value in self.members],
            "provenance": self.provenance.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class RadarCaptureInput:
    """One provider product backed by exactly one complete raw capture set."""

    provider: str
    product: str
    region_ids: tuple[str, ...]
    capture_set: RawCaptureSetRef
    coverage: CoverageDescriptor

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "provider",
            _regex_string(self.provider, "provider", _PROVIDER_RE),
        )
        product = _regex_string(self.product, "product", _PRODUCT_RE)
        if "://" in product:
            raise ValueError("product must be an opaque product ID, not a URL")
        object.__setattr__(self, "product", product)
        if not isinstance(self.region_ids, tuple) or not self.region_ids:
            raise ValueError("input region_ids must be a non-empty tuple")
        region_ids = tuple(sorted(
            _regex_string(value, "input region_id", _STABLE_ID_RE)
            for value in self.region_ids
        ))
        if (
            any("@" in value for value in region_ids)
            or len(region_ids) > MAX_REGIONS
            or len(region_ids) != len(set(region_ids))
        ):
            raise ValueError("input region_ids exceed bounds or contain duplicates")
        object.__setattr__(self, "region_ids", region_ids)
        if not isinstance(self.capture_set, RawCaptureSetRef):
            raise TypeError("capture_set must be RawCaptureSetRef")
        if not isinstance(self.coverage, CoverageDescriptor):
            raise TypeError("coverage must be CoverageDescriptor")

    @property
    def product_valid_at(self) -> float:
        return self.capture_set.product_timestamp

    @property
    def received_at(self) -> float:
        return self.capture_set.received_at

    @property
    def input_id(self) -> str:
        return "radar-capture-input:" + _sha256_document(self.to_dict())

    @property
    def empty_precipitation_state(self) -> PrecipitationState:
        return PrecipitationState.UNKNOWN

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "product": self.product,
            "region_ids": list(self.region_ids),
            "capture_set": self.capture_set.to_dict(),
            "coverage": self.coverage.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class RadarCycleRequest:
    requested_at: float
    regions: tuple[RadarRegion, ...]
    targets: tuple[RadarTarget, ...]
    inputs: tuple[RadarInput, ...]
    contract: str = REQUEST_CONTRACT

    def __post_init__(self) -> None:
        if self.contract != REQUEST_CONTRACT:
            raise ValueError("unsupported radar cycle request contract")
        object.__setattr__(
            self, "requested_at", _timestamp(self.requested_at, "requested_at")
        )
        if not isinstance(self.regions, tuple) or not self.regions:
            raise ValueError("regions must be a non-empty tuple")
        if len(self.regions) > MAX_REGIONS or any(
            not isinstance(value, RadarRegion) for value in self.regions
        ):
            raise ValueError("regions exceed bounds or contain invalid records")
        regions = tuple(sorted(self.regions, key=lambda value: value.region_id))
        region_ids = [value.region_id for value in regions]
        if len(region_ids) != len(set(region_ids)):
            raise ValueError("region_id must be unique")
        object.__setattr__(self, "regions", regions)

        if not isinstance(self.targets, tuple) or len(self.targets) > MAX_TARGETS:
            raise ValueError("targets must be a bounded tuple")
        if any(not isinstance(value, RadarTarget) for value in self.targets):
            raise TypeError("targets contain invalid records")
        targets = tuple(
            sorted(self.targets, key=lambda value: (value.region_id, value.target_id))
        )
        target_ids = [value.target_id for value in targets]
        if len(target_ids) != len(set(target_ids)):
            raise ValueError("target_id must be globally unique")
        if any(value.region_id not in set(region_ids) for value in targets):
            raise ValueError("target references unknown storage region_id")
        object.__setattr__(self, "targets", targets)

        if not isinstance(self.inputs, tuple) or len(self.inputs) > MAX_INPUTS:
            raise ValueError("inputs must be a bounded tuple")
        if any(not isinstance(value, RadarInput) for value in self.inputs):
            raise TypeError("inputs contain invalid records")
        inputs = tuple(sorted(self.inputs, key=lambda value: value.input_id))
        input_ids = [value.input_id for value in inputs]
        if len(input_ids) != len(set(input_ids)):
            raise ValueError("identical radar inputs are duplicated")
        artifact_handles = [
            (value.artifact.artifact_path, value.artifact.artifact_sha256)
            for value in inputs
        ]
        if len(artifact_handles) != len(set(artifact_handles)):
            raise ValueError("one artifact handle has conflicting input records")
        known_regions = set(region_ids)
        if any(
            region_id not in known_regions
            for value in inputs
            for region_id in value.region_ids
        ):
            raise ValueError("input references unknown storage region_id")
        object.__setattr__(self, "inputs", inputs)

        if len(self.canonical_bytes) > MAX_REQUEST_BYTES:
            raise ValueError("canonical radar cycle request exceeds 96 KiB")

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "requested_at": self.requested_at,
            "regions": [value.to_dict() for value in self.regions],
            "targets": [value.to_dict() for value in self.targets],
            "inputs": [value.to_dict() for value in self.inputs],
        }

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.to_dict())

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @property
    def identity_sha256(self) -> str:
        identity = self.to_dict()
        identity.pop("requested_at")
        return _sha256_document(identity)

    @property
    def cycle_id(self) -> str:
        return "radar-cycle:" + self.identity_sha256

    @property
    def undecoded_precipitation_state(self) -> PrecipitationState:
        """No input, handle or provenance record is itself wet/dry evidence."""
        return PrecipitationState.UNKNOWN

    @property
    def regions_sha256(self) -> str:
        return _sha256_document({
            "contract": self.contract,
            "regions": [value.to_dict() for value in self.regions],
        })

    @property
    def targets_sha256(self) -> str:
        return _sha256_document({
            "contract": self.contract,
            "targets": [value.to_dict() for value in self.targets],
        })

    @property
    def inputs_sha256(self) -> str:
        return _sha256_document({
            "contract": self.contract,
            "inputs": [value.to_dict() for value in self.inputs],
        })

    def digests(self) -> dict[str, str]:
        return {
            "request_sha256": self.request_sha256,
            "identity_sha256": self.identity_sha256,
            "cycle_id": self.cycle_id,
            "regions_sha256": self.regions_sha256,
            "targets_sha256": self.targets_sha256,
            "inputs_sha256": self.inputs_sha256,
        }


@dataclass(frozen=True, slots=True)
class RadarCycleRequestV2:
    """Operational request whose every input is a linked complete raw set."""

    requested_at: float
    regions: tuple[RadarRegion, ...]
    targets: tuple[RadarTarget, ...]
    inputs: tuple[RadarCaptureInput, ...]
    contract: str = REQUEST_CONTRACT_V2

    def __post_init__(self) -> None:
        if self.contract != REQUEST_CONTRACT_V2:
            raise ValueError("unsupported radar cycle request contract")
        object.__setattr__(
            self,
            "requested_at",
            _timestamp(self.requested_at, "requested_at"),
        )
        if not isinstance(self.regions, tuple) or not self.regions:
            raise ValueError("regions must be a non-empty tuple")
        if len(self.regions) > MAX_REGIONS or any(
            not isinstance(value, RadarRegion) for value in self.regions
        ):
            raise ValueError("regions exceed bounds or contain invalid records")
        regions = tuple(sorted(self.regions, key=lambda value: value.region_id))
        region_ids = [value.region_id for value in regions]
        if len(region_ids) != len(set(region_ids)):
            raise ValueError("region_id must be unique")
        object.__setattr__(self, "regions", regions)

        if not isinstance(self.targets, tuple) or len(self.targets) > MAX_TARGETS:
            raise ValueError("targets must be a bounded tuple")
        if any(not isinstance(value, RadarTarget) for value in self.targets):
            raise TypeError("targets contain invalid records")
        targets = tuple(sorted(
            self.targets,
            key=lambda value: (value.region_id, value.target_id),
        ))
        target_ids = [value.target_id for value in targets]
        if len(target_ids) != len(set(target_ids)):
            raise ValueError("target_id must be globally unique")
        known_regions = set(region_ids)
        if any(value.region_id not in known_regions for value in targets):
            raise ValueError("target references unknown storage region_id")
        object.__setattr__(self, "targets", targets)

        if not isinstance(self.inputs, tuple) or len(self.inputs) > MAX_INPUTS:
            raise ValueError("inputs must be a bounded tuple")
        if any(not isinstance(value, RadarCaptureInput) for value in self.inputs):
            raise TypeError("v2 inputs must be complete capture-set records")
        inputs = tuple(sorted(self.inputs, key=lambda value: value.input_id))
        input_ids = [value.input_id for value in inputs]
        capture_ids = [value.capture_set.capture_id for value in inputs]
        if len(input_ids) != len(set(input_ids)):
            raise ValueError("identical radar capture inputs are duplicated")
        if len(capture_ids) != len(set(capture_ids)):
            raise ValueError("one raw capture set has conflicting input records")
        if any(
            region_id not in known_regions
            for value in inputs
            for region_id in value.region_ids
        ):
            raise ValueError("input references unknown storage region_id")
        object.__setattr__(self, "inputs", inputs)
        if len(self.canonical_bytes) > MAX_REQUEST_BYTES:
            raise ValueError("canonical radar cycle request exceeds 96 KiB")

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "requested_at": self.requested_at,
            "regions": [value.to_dict() for value in self.regions],
            "targets": [value.to_dict() for value in self.targets],
            "inputs": [value.to_dict() for value in self.inputs],
        }

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.to_dict())

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @property
    def identity_sha256(self) -> str:
        identity = self.to_dict()
        identity.pop("requested_at")
        return _sha256_document(identity)

    @property
    def cycle_id(self) -> str:
        return "radar-cycle:" + self.identity_sha256

    @property
    def undecoded_precipitation_state(self) -> PrecipitationState:
        return PrecipitationState.UNKNOWN

    @property
    def regions_sha256(self) -> str:
        return _sha256_document({
            "contract": self.contract,
            "regions": [value.to_dict() for value in self.regions],
        })

    @property
    def targets_sha256(self) -> str:
        return _sha256_document({
            "contract": self.contract,
            "targets": [value.to_dict() for value in self.targets],
        })

    @property
    def inputs_sha256(self) -> str:
        return _sha256_document({
            "contract": self.contract,
            "inputs": [value.to_dict() for value in self.inputs],
        })

    def digests(self) -> dict[str, str]:
        return {
            "request_sha256": self.request_sha256,
            "identity_sha256": self.identity_sha256,
            "cycle_id": self.cycle_id,
            "regions_sha256": self.regions_sha256,
            "targets_sha256": self.targets_sha256,
            "inputs_sha256": self.inputs_sha256,
        }


_REQUEST_KEYS = frozenset({
    "contract", "requested_at", "regions", "targets", "inputs",
})
_REGION_KEYS = frozenset({"region_id", "center_lat", "center_lon", "radius_km"})
_TARGET_KEYS = frozenset({"target_id", "region_id", "latitude", "longitude"})
_INPUT_KEYS = frozenset({
    "provider", "product", "region_ids", "artifact", "provenance",
    "product_valid_at", "received_at", "coverage",
})
_ARTIFACT_KEYS = frozenset({
    "artifact_path", "artifact_sha256", "artifact_bytes", "content_type",
})
_PROVENANCE_KEYS = frozenset({
    "grade", "artifact_kind", "exact_raw_artifacts_complete", "parser_id",
    "upstream_record_id", "raw_artifact_receipt_id",
})
_COVERAGE_KEYS = frozenset({"status", "bbox"})
_V2_INPUT_KEYS = frozenset({
    "provider", "product", "region_ids", "capture_set", "coverage",
})
_CAPTURE_SET_KEYS = frozenset({
    "capture_id", "fetch_id", "upstream_capture_id", "manifest_sha256",
    "product_timestamp", "members", "provenance",
})
_CAPTURE_MEMBER_KEYS = frozenset({
    "member_id", "role", "receipt_id", "content_sha256",
    "receipt_sha256", "received_at",
})
_CAPTURE_PROVENANCE_KEYS = frozenset({
    "grade", "complete", "raw_store_schema_version",
    "capture_schema", "capture_receipt_schema", "capture_receipt_sha256",
})


def _parse_artifact(value: object) -> ArtifactHandle:
    raw = _exact_mapping(value, name="artifact", keys=_ARTIFACT_KEYS)
    return ArtifactHandle(
        artifact_path=raw["artifact_path"],
        artifact_sha256=raw["artifact_sha256"],
        artifact_bytes=raw["artifact_bytes"],
        content_type=raw["content_type"],
    )


def _parse_provenance(value: object) -> RadarInputProvenance:
    raw = _exact_mapping(value, name="provenance", keys=_PROVENANCE_KEYS)
    return RadarInputProvenance(
        grade=_enum(raw["grade"], ProvenanceGrade, "provenance grade"),
        artifact_kind=_enum(
            raw["artifact_kind"], ArtifactKind, "artifact_kind"
        ),
        exact_raw_artifacts_complete=raw["exact_raw_artifacts_complete"],
        parser_id=raw["parser_id"],
        upstream_record_id=raw["upstream_record_id"],
        raw_artifact_receipt_id=raw["raw_artifact_receipt_id"],
    )


def _parse_coverage(value: object) -> CoverageDescriptor:
    raw = _exact_mapping(value, name="coverage", keys=_COVERAGE_KEYS)
    bbox = raw["bbox"]
    if bbox is not None:
        values = _exact_list(bbox, "coverage bbox", 4)
        if len(values) != 4:
            raise ValueError("coverage bbox must contain exactly four values")
        bbox = tuple(values)
    return CoverageDescriptor(
        status=_enum(raw["status"], CoverageStatus, "coverage status"),
        bbox=bbox,
    )


def _parse_region(value: object) -> RadarRegion:
    raw = _exact_mapping(value, name="region", keys=_REGION_KEYS)
    return RadarRegion(
        region_id=raw["region_id"],
        center_lat=raw["center_lat"],
        center_lon=raw["center_lon"],
        radius_km=raw["radius_km"],
    )


def _parse_target(value: object) -> RadarTarget:
    raw = _exact_mapping(value, name="target", keys=_TARGET_KEYS)
    return RadarTarget(
        target_id=raw["target_id"],
        region_id=raw["region_id"],
        latitude=raw["latitude"],
        longitude=raw["longitude"],
    )


def _parse_input(value: object) -> RadarInput:
    raw = _exact_mapping(value, name="input", keys=_INPUT_KEYS)
    raw_region_ids = _exact_list(raw["region_ids"], "input region_ids", MAX_REGIONS)
    return RadarInput(
        provider=raw["provider"],
        product=raw["product"],
        region_ids=tuple(raw_region_ids),
        artifact=_parse_artifact(raw["artifact"]),
        provenance=_parse_provenance(raw["provenance"]),
        product_valid_at=raw["product_valid_at"],
        received_at=raw["received_at"],
        coverage=_parse_coverage(raw["coverage"]),
    )


def _parse_capture_member(value: object) -> RawCaptureMemberRef:
    raw = _exact_mapping(
        value,
        name="capture member",
        keys=_CAPTURE_MEMBER_KEYS,
    )
    return RawCaptureMemberRef(
        member_id=raw["member_id"],
        role=raw["role"],
        receipt_id=raw["receipt_id"],
        content_sha256=raw["content_sha256"],
        receipt_sha256=raw["receipt_sha256"],
        received_at=raw["received_at"],
    )


def _parse_capture_provenance(value: object) -> RawCaptureSetProvenance:
    raw = _exact_mapping(
        value,
        name="capture provenance",
        keys=_CAPTURE_PROVENANCE_KEYS,
    )
    return RawCaptureSetProvenance(
        grade=raw["grade"],
        complete=raw["complete"],
        raw_store_schema_version=raw["raw_store_schema_version"],
        capture_schema=raw["capture_schema"],
        capture_receipt_schema=raw["capture_receipt_schema"],
        capture_receipt_sha256=raw["capture_receipt_sha256"],
    )


def _parse_capture_set(value: object) -> RawCaptureSetRef:
    raw = _exact_mapping(value, name="capture set", keys=_CAPTURE_SET_KEYS)
    members = _exact_list(
        raw["members"],
        "capture members",
        MAX_CAPTURE_MEMBERS,
    )
    return RawCaptureSetRef(
        capture_id=raw["capture_id"],
        fetch_id=raw["fetch_id"],
        upstream_capture_id=raw["upstream_capture_id"],
        manifest_sha256=raw["manifest_sha256"],
        product_timestamp=raw["product_timestamp"],
        members=tuple(_parse_capture_member(item) for item in members),
        provenance=_parse_capture_provenance(raw["provenance"]),
    )


def _parse_v2_input(value: object) -> RadarCaptureInput:
    raw = _exact_mapping(value, name="v2 input", keys=_V2_INPUT_KEYS)
    region_ids = _exact_list(
        raw["region_ids"],
        "input region_ids",
        MAX_REGIONS,
    )
    return RadarCaptureInput(
        provider=raw["provider"],
        product=raw["product"],
        region_ids=tuple(region_ids),
        capture_set=_parse_capture_set(raw["capture_set"]),
        coverage=_parse_coverage(raw["coverage"]),
    )


def _reject_constant(value: str):
    raise ValueError(f"non-finite JSON constant {value} is forbidden")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _decode_request(
    value: object,
) -> Mapping[str, Any] | RadarCycleRequest | RadarCycleRequestV2:
    if isinstance(value, (RadarCycleRequest, RadarCycleRequestV2)):
        return value
    if isinstance(value, bytes):
        if len(value) > MAX_REQUEST_BYTES:
            raise ValueError("encoded radar cycle request exceeds 96 KiB")
        try:
            text = value.decode("utf-8")
        except UnicodeDecodeError as err:
            raise ValueError("radar cycle request must be UTF-8") from err
    elif isinstance(value, str):
        try:
            encoded_length = len(value.encode("utf-8"))
        except UnicodeEncodeError as err:
            raise ValueError("radar cycle request must be valid Unicode") from err
        if encoded_length > MAX_REQUEST_BYTES:
            raise ValueError("encoded radar cycle request exceeds 96 KiB")
        text = value
    elif type(value) is dict:
        return value
    else:
        raise TypeError("radar cycle request must be dict, JSON text or bytes")
    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (json.JSONDecodeError, RecursionError) as err:
        raise ValueError("invalid radar cycle request JSON") from err
    if type(decoded) is not dict:
        raise TypeError("radar cycle request JSON root must be an object")
    return decoded


def parse_radar_cycle_request(
    value: object,
) -> RadarCycleRequest | RadarCycleRequestV2:
    """Read and validate either v1 compatibility data or strict v2 data."""
    decoded = _decode_request(value)
    if isinstance(decoded, (RadarCycleRequest, RadarCycleRequestV2)):
        return decoded
    raw = _exact_mapping(decoded, name="request", keys=_REQUEST_KEYS)
    if raw["contract"] not in {REQUEST_CONTRACT_V1, REQUEST_CONTRACT_V2}:
        raise ValueError("unsupported radar cycle request contract")
    regions = _exact_list(raw["regions"], "regions", MAX_REGIONS)
    targets = _exact_list(raw["targets"], "targets", MAX_TARGETS)
    inputs = _exact_list(raw["inputs"], "inputs", MAX_INPUTS)
    common = {
        "contract": raw["contract"],
        "requested_at": raw["requested_at"],
        "regions": tuple(_parse_region(item) for item in regions),
        "targets": tuple(_parse_target(item) for item in targets),
    }
    if raw["contract"] == REQUEST_CONTRACT_V1:
        request = RadarCycleRequest(
            **common,
            inputs=tuple(_parse_input(item) for item in inputs),
        )
    else:
        request = RadarCycleRequestV2(
            **common,
            inputs=tuple(_parse_v2_input(item) for item in inputs),
        )
    if len(request.canonical_bytes) > MAX_REQUEST_BYTES:
        raise ValueError("canonical radar cycle request exceeds 96 KiB")
    return request


def parse_operational_radar_cycle_request(value: object) -> RadarCycleRequestV2:
    """Admit only v2; a v1-shaped single artifact may not claim completeness."""
    request = parse_radar_cycle_request(value)
    if not isinstance(request, RadarCycleRequestV2):
        raise ValueError("operational radar cycle admission requires contract v2")
    return request


def canonical_radar_cycle_request(value: object) -> bytes:
    return parse_radar_cycle_request(value).canonical_bytes


def radar_cycle_request_digest(value: object) -> str:
    return parse_radar_cycle_request(value).request_sha256


__all__ = [
    "ArtifactHandle",
    "ArtifactKind",
    "CoverageDescriptor",
    "CoverageStatus",
    "MAX_CAPTURE_MEMBERS",
    "MAX_INPUTS",
    "MAX_REGIONS",
    "MAX_REQUEST_BYTES",
    "MAX_TARGETS",
    "PrecipitationState",
    "ProvenanceGrade",
    "OPERATIONAL_REQUEST_CONTRACT",
    "RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA",
    "RAW_ARTIFACT_CAPTURE_SCHEMA",
    "RAW_ARTIFACT_STORE_SCHEMA_VERSION",
    "RAW_CAPTURE_PROVENANCE_GRADE",
    "REQUEST_CONTRACT",
    "REQUEST_CONTRACT_V1",
    "REQUEST_CONTRACT_V2",
    "RadarCaptureInput",
    "RadarCycleRequest",
    "RadarCycleRequestV2",
    "RadarInput",
    "RadarInputProvenance",
    "RadarRegion",
    "RadarTarget",
    "RawCaptureMemberRef",
    "RawCaptureSetProvenance",
    "RawCaptureSetRef",
    "canonical_radar_cycle_request",
    "parse_operational_radar_cycle_request",
    "parse_radar_cycle_request",
    "radar_cycle_request_digest",
]
