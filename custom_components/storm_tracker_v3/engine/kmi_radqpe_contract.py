"""Fail-closed contract for KMI RADQPE and its public WCS candidate.

This module is a vNext shadow primitive.  It performs no network I/O and is
not imported by the operational provider registry.  Its two source profiles
must remain distinct:

* ``kmi_radqpe_native`` is the documented 1 km Belgian Lambert 2008 product,
  for which no usable transport/access agreement is configured in this repo.
* ``kmi_public_radar_composite`` is the anonymously readable GeoServer WCS
  layer.  Its public metadata currently does not prove RADQPE identity,
  precipitation quantity, unit or nodata semantics.

Consequently this module may audit and preserve exact bytes, but it never
turns the public raster into detections, rain rates or zero-rain evidence.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
import io
import math
import re
from typing import Any
from xml.etree import ElementTree


SCHEMA_VERSION = "kmi-radqpe-shadow-contract-v1"
SHADOW_ONLY = True

PUBLIC_SOURCE_ID = "kmi_public_radar_composite"
PUBLIC_OWS_ENDPOINT = "https://opendata.meteo.be/geoserver/ows"
PUBLIC_WMS_LAYER = "radar:belgian_rainfall_composite"
PUBLIC_WCS_COVERAGE_ID = "radar__belgian_rainfall_composite"

NATIVE_SOURCE_ID = "kmi_radqpe_native"
NATIVE_PRODUCT_RATE = "rad.best.comp.rate.qpe"
NATIVE_CRS_NAME = "Belgian Lambert 2008"
NATIVE_CRS_EPSG = 3812
NATIVE_RESOLUTION_METRES = 1000

EXPECTED_PUBLIC_CADENCE_SECONDS = 5 * 60
MAX_CAPABILITIES_BYTES = 1 * 1024 * 1024
MAX_DESCRIPTION_BYTES = 1 * 1024 * 1024
MAX_COVERAGE_BYTES = 16 * 1024 * 1024
MAX_RASTER_CELLS = 2_000_000

_DURATION_RE = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$")
_IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,199}$")
_NATIVE_FILENAME_RE = re.compile(
    r"^(?P<time>\d{14})\.rad\.best\.comp\."
    r"(?P<quantity>rate\.qpe|acrr\.qpe\.(?:5m|10m|1h|1d))\."
    r"(?P<extension>tif|hdf)$"
)


class ContractGate(str, Enum):
    """Explicit eligibility state; absence is never interpreted as approval."""

    VERIFIED = "VERIFIED"
    UNVERIFIED = "UNVERIFIED"
    BLOCKED = "BLOCKED"


class KmiRadqpeContractError(ValueError):
    """Upstream evidence violates the pinned shadow contract."""


@dataclass(frozen=True, slots=True)
class KmiRadqpeShadowContract:
    """Source facts and the gates that prevent an unsafe cutover."""

    schema_version: str = SCHEMA_VERSION
    shadow_only: bool = SHADOW_ONLY
    native_source_id: str = NATIVE_SOURCE_ID
    public_source_id: str = PUBLIC_SOURCE_ID
    upstream_authority: str = (
        "Royal Meteorological Institute of Belgium (KMI/IRM)"
    )
    native_product: str = NATIVE_PRODUCT_RATE
    native_crs: str = NATIVE_CRS_NAME
    native_crs_epsg: int = NATIVE_CRS_EPSG
    native_resolution_metres: int = NATIVE_RESOLUTION_METRES
    native_cadence_seconds: int = EXPECTED_PUBLIC_CADENCE_SECONDS
    native_transport_access: ContractGate = ContractGate.BLOCKED
    public_product_identity_as_radqpe: ContractGate = ContractGate.UNVERIFIED
    public_quantity_semantics: ContractGate = ContractGate.UNVERIFIED
    public_unit_semantics: ContractGate = ContractGate.UNVERIFIED
    public_nodata_semantics: ContractGate = ContractGate.UNVERIFIED
    retention_and_value_added_use: ContractGate = ContractGate.BLOCKED
    atomic_artifact_set_persistence: ContractGate = ContractGate.BLOCKED
    operational_cutover: ContractGate = ContractGate.BLOCKED

    @property
    def detection_eligible(self) -> bool:
        return False

    @property
    def retention_eligible(self) -> bool:
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            field: (
                value.value if isinstance(value, ContractGate) else value
            )
            for field, value in {
                "schema_version": self.schema_version,
                "shadow_only": self.shadow_only,
                "native_source_id": self.native_source_id,
                "public_source_id": self.public_source_id,
                "upstream_authority": self.upstream_authority,
                "native_product": self.native_product,
                "native_crs": self.native_crs,
                "native_crs_epsg": self.native_crs_epsg,
                "native_resolution_metres": self.native_resolution_metres,
                "native_cadence_seconds": self.native_cadence_seconds,
                "native_transport_access": self.native_transport_access,
                "public_product_identity_as_radqpe":
                    self.public_product_identity_as_radqpe,
                "public_quantity_semantics":
                    self.public_quantity_semantics,
                "public_unit_semantics": self.public_unit_semantics,
                "public_nodata_semantics": self.public_nodata_semantics,
                "retention_and_value_added_use":
                    self.retention_and_value_added_use,
                "atomic_artifact_set_persistence":
                    self.atomic_artifact_set_persistence,
                "operational_cutover": self.operational_cutover,
                "detection_eligible": self.detection_eligible,
                "retention_eligible": self.retention_eligible,
            }.items()
        }


KMI_RADQPE_SHADOW_CONTRACT = KmiRadqpeShadowContract()


@dataclass(frozen=True, slots=True)
class PublicCatalogSnapshot:
    """Validated WMS time/coverage metadata for the public candidate."""

    service_title: str
    fees: str
    access_constraints: str
    layer_name: str
    start_at: float
    end_at: float
    default_at: float
    cadence_seconds: int
    crs: tuple[str, ...]
    wgs84_bounds: tuple[float, float, float, float]
    payload_sha256: str

    def __post_init__(self) -> None:
        if self.layer_name != PUBLIC_WMS_LAYER.split(":", 1)[1]:
            raise KmiRadqpeContractError("onverwachte publieke WMS-laag")
        if self.cadence_seconds != EXPECTED_PUBLIC_CADENCE_SECONDS:
            raise KmiRadqpeContractError("publieke WMS-cadans is niet vijf minuten")
        if not (self.start_at <= self.default_at <= self.end_at):
            raise KmiRadqpeContractError(
                "WMS-standaardtijd valt buiten het tijddomein"
            )
        if "EPSG:4326" not in self.crs and "CRS:84" not in self.crs:
            raise KmiRadqpeContractError("publieke WMS-laag mist WGS84")

    @property
    def default_time(self) -> str:
        return format_utc(self.default_at)

    def to_dict(self) -> dict[str, Any]:
        return {
            "service_title": self.service_title,
            "fees": self.fees,
            "access_constraints": self.access_constraints,
            "layer_name": self.layer_name,
            "start_at": format_utc(self.start_at),
            "end_at": format_utc(self.end_at),
            "default_at": self.default_time,
            "cadence_seconds": self.cadence_seconds,
            "crs": list(self.crs),
            "wgs84_bounds": list(self.wgs84_bounds),
            "payload_sha256": self.payload_sha256,
        }


@dataclass(frozen=True, slots=True)
class PublicCoverageDescription:
    """WCS coverage facts, including semantic contradictions."""

    coverage_id: str
    coverage_name: str
    start_at: float
    end_at: float
    default_at: float
    cadence_seconds: int
    srs_name: str
    axis_labels: tuple[str, ...]
    width: int
    height: int
    origin: tuple[float, float]
    offset_vectors: tuple[tuple[float, float], ...]
    range_field: str
    declared_uom: str
    declared_nil_values: tuple[str, ...]
    native_format: str
    semantic_blockers: tuple[str, ...]
    payload_sha256: str

    @property
    def quantitative_semantics_eligible(self) -> bool:
        return not self.semantic_blockers

    def to_dict(self) -> dict[str, Any]:
        return {
            "coverage_id": self.coverage_id,
            "coverage_name": self.coverage_name,
            "start_at": format_utc(self.start_at),
            "end_at": format_utc(self.end_at),
            "default_at": format_utc(self.default_at),
            "cadence_seconds": self.cadence_seconds,
            "srs_name": self.srs_name,
            "axis_labels": list(self.axis_labels),
            "width": self.width,
            "height": self.height,
            "origin": list(self.origin),
            "offset_vectors": [list(item) for item in self.offset_vectors],
            "range_field": self.range_field,
            "declared_uom": self.declared_uom,
            "declared_nil_values": list(self.declared_nil_values),
            "native_format": self.native_format,
            "semantic_blockers": list(self.semantic_blockers),
            "quantitative_semantics_eligible":
                self.quantitative_semantics_eligible,
            "payload_sha256": self.payload_sha256,
        }


@dataclass(frozen=True, slots=True)
class PublicGeoTiffAudit:
    """Structural audit only; values retain UNKNOWN meteorological semantics."""

    payload_sha256: str
    size_bytes: int
    width: int
    height: int
    mode: str
    epsg: int | None
    transform: tuple[float, ...]
    bounds: tuple[float, float, float, float]
    embedded_nodata: float | None
    finite_samples: int
    nonfinite_samples: int
    negative_samples: int
    minimum: float | None
    maximum: float | None
    semantic_blockers: tuple[str, ...]

    @property
    def quantitative_semantics_eligible(self) -> bool:
        return not self.semantic_blockers

    def to_dict(self) -> dict[str, Any]:
        return {
            "payload_sha256": self.payload_sha256,
            "size_bytes": self.size_bytes,
            "width": self.width,
            "height": self.height,
            "mode": self.mode,
            "epsg": self.epsg,
            "transform": list(self.transform),
            "bounds": list(self.bounds),
            "embedded_nodata": self.embedded_nodata,
            "finite_samples": self.finite_samples,
            "nonfinite_samples": self.nonfinite_samples,
            "negative_samples": self.negative_samples,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "semantic_blockers": list(self.semantic_blockers),
            "quantitative_semantics_eligible":
                self.quantitative_semantics_eligible,
        }


@dataclass(frozen=True, slots=True)
class ShadowArtifact:
    """Exact, secret-free artifact descriptor; payload stays immutable bytes."""

    role: str
    source_record_id: str
    payload: bytes
    media_type: str
    fetched_at: float
    nominal_at: float | None

    def __post_init__(self) -> None:
        if not _IDENTIFIER_RE.fullmatch(self.role):
            raise KmiRadqpeContractError("ongeldige artifactrol")
        if not _IDENTIFIER_RE.fullmatch(self.source_record_id):
            raise KmiRadqpeContractError("ongeldige upstream record-ID")
        if not isinstance(self.payload, bytes):
            raise TypeError("artifactpayload moet exact bytes zijn")
        if not self.payload:
            raise KmiRadqpeContractError("artifactpayload is leeg")
        if not math.isfinite(self.fetched_at):
            raise KmiRadqpeContractError("fetch-tijd is niet eindig")
        if self.nominal_at is not None and not math.isfinite(self.nominal_at):
            raise KmiRadqpeContractError("nominale tijd is niet eindig")
        if "/" not in self.media_type:
            raise KmiRadqpeContractError("ongeldig media type")

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.payload).hexdigest()

    def receipt_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "source_record_id": self.source_record_id,
            "content_sha256": self.content_sha256,
            "size_bytes": len(self.payload),
            "media_type": self.media_type,
            "fetched_at": format_utc(self.fetched_at),
            "nominal_at": (
                format_utc(self.nominal_at)
                if self.nominal_at is not None
                else None
            ),
        }


@dataclass(frozen=True, slots=True)
class KmiPublicCompositeArtifactSet:
    """Complete three-artifact WMS/WCS evidence set for one nominal time."""

    nominal_at: float
    catalog: PublicCatalogSnapshot
    description: PublicCoverageDescription
    geotiff: PublicGeoTiffAudit
    artifacts: tuple[ShadowArtifact, ...]

    def __post_init__(self) -> None:
        expected_roles = {
            "wms_capabilities",
            "wcs_description",
            "wcs_coverage",
        }
        roles = [artifact.role for artifact in self.artifacts]
        if len(roles) != len(set(roles)) or set(roles) != expected_roles:
            raise KmiRadqpeContractError(
                "publieke composietset is dubbel of onvolledig"
            )
        by_role = {artifact.role: artifact for artifact in self.artifacts}
        if (
            by_role["wms_capabilities"].content_sha256
            != self.catalog.payload_sha256
            or by_role["wcs_description"].content_sha256
            != self.description.payload_sha256
            or by_role["wcs_coverage"].content_sha256
            != self.geotiff.payload_sha256
        ):
            raise KmiRadqpeContractError("artifactset en audits hebben andere bytes")
        if by_role["wcs_coverage"].nominal_at != self.nominal_at:
            raise KmiRadqpeContractError("coverage heeft een andere nominale tijd")
        if not (self.catalog.start_at <= self.nominal_at <= self.catalog.end_at):
            raise KmiRadqpeContractError("coverage valt buiten WMS-tijddomein")
        if not (
            self.description.start_at
            <= self.nominal_at
            <= self.description.end_at
        ):
            raise KmiRadqpeContractError("coverage valt buiten WCS-tijddomein")
        if any(
            not math.isclose(actual, expected, abs_tol=1e-8, rel_tol=0.0)
            for actual, expected in zip(
                self.geotiff.bounds,
                self.catalog.wgs84_bounds,
                strict=True,
            )
        ):
            raise KmiRadqpeContractError(
                "GeoTIFF-bounds wijken af van WMS-dekking"
            )

    @property
    def detection_eligible(self) -> bool:
        return False

    @property
    def retention_eligible(self) -> bool:
        return False

    @property
    def exact_raw_artifacts_complete_in_memory(self) -> bool:
        return True

    @property
    def exact_raw_artifacts_durably_committed(self) -> bool:
        return False

    def summary_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "source": PUBLIC_SOURCE_ID,
            "nominal_at": format_utc(self.nominal_at),
            "exact_raw_artifacts_complete_in_memory":
                self.exact_raw_artifacts_complete_in_memory,
            "exact_raw_artifacts_durably_committed":
                self.exact_raw_artifacts_durably_committed,
            "detection_eligible": self.detection_eligible,
            "retention_eligible": self.retention_eligible,
            "catalog": self.catalog.to_dict(),
            "description": self.description.to_dict(),
            "geotiff": self.geotiff.to_dict(),
            "artifacts": [
                artifact.receipt_dict() for artifact in self.artifacts
            ],
        }


def parse_utc(value: str) -> float:
    """Parse an explicit UTC ISO timestamp without assuming a timezone."""

    if not isinstance(value, str) or not value.endswith("Z"):
        raise KmiRadqpeContractError("timestamp moet expliciet UTC (Z) zijn")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as err:
        raise KmiRadqpeContractError("ongeldige ISO-timestamp") from err
    timestamp = parsed.timestamp()
    if not math.isfinite(timestamp):
        raise KmiRadqpeContractError("timestamp is niet eindig")
    return timestamp


def format_utc(timestamp: float) -> str:
    if not math.isfinite(timestamp):
        raise KmiRadqpeContractError("timestamp is niet eindig")
    return (
        datetime.fromtimestamp(timestamp, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _duration_seconds(value: str) -> int:
    match = _DURATION_RE.fullmatch(value)
    if match is None:
        raise KmiRadqpeContractError(
            f"niet-ondersteunde ISO-8601-duur {value!r}"
        )
    hours, minutes, seconds = (int(item or 0) for item in match.groups())
    total = hours * 3600 + minutes * 60 + seconds
    if total <= 0:
        raise KmiRadqpeContractError("tijdinterval moet positief zijn")
    return total


def _require_payload(payload: bytes, maximum: int, role: str) -> bytes:
    if not isinstance(payload, bytes):
        raise TypeError(f"{role}-payload moet exact bytes zijn")
    if not payload:
        raise KmiRadqpeContractError(f"{role}-payload is leeg")
    if len(payload) > maximum:
        raise KmiRadqpeContractError(f"{role}-payload overschrijdt bytecontract")
    return payload


def _local_name(element) -> str:
    return element.tag.rsplit("}", 1)[-1]


def _direct_text(element, name: str, *, required: bool = True) -> str:
    for child in element:
        if _local_name(child) == name:
            text = (child.text or "").strip()
            if text or not required:
                return text
    if required:
        raise KmiRadqpeContractError(f"XML-veld {name!r} ontbreekt")
    return ""


def _descendant(element, name: str):
    for child in element.iter():
        if _local_name(child) == name:
            return child
    raise KmiRadqpeContractError(f"XML-element {name!r} ontbreekt")


def parse_public_wms_capabilities(payload: bytes) -> PublicCatalogSnapshot:
    """Validate public WMS identity, domain, cadence and coverage."""

    payload = _require_payload(
        payload,
        MAX_CAPABILITIES_BYTES,
        "WMS-capabilities",
    )
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError as err:
        raise KmiRadqpeContractError("ongeldige WMS-capabilities XML") from err
    if _local_name(root) not in {"WMS_Capabilities", "WMT_MS_Capabilities"}:
        raise KmiRadqpeContractError("payload is geen WMS-capabilitiesdocument")
    if root.attrib.get("version") not in {"1.3.0", "1.1.1"}:
        raise KmiRadqpeContractError("onverwachte WMS-versie")
    service = next(
        (child for child in root if _local_name(child) == "Service"),
        None,
    )
    if service is None:
        raise KmiRadqpeContractError("WMS-serviceblok ontbreekt")
    service_title = _direct_text(service, "Title")
    if "KMI-IRM" not in service_title:
        raise KmiRadqpeContractError("WMS-identiteit is niet KMI-IRM")
    fees = _direct_text(service, "Fees", required=False)
    access = _direct_text(service, "AccessConstraints", required=False)

    expected_name = PUBLIC_WMS_LAYER.split(":", 1)[1]
    layer = None
    for candidate in (
        item for item in root.iter() if _local_name(item) == "Layer"
    ):
        if _direct_text(candidate, "Name", required=False) in {
            expected_name,
            PUBLIC_WMS_LAYER,
        }:
            layer = candidate
            break
    if layer is None:
        raise KmiRadqpeContractError(
            f"WMS-laag {expected_name!r} ontbreekt"
        )

    time_node = next(
        (
            child
            for child in layer
            if _local_name(child) in {"Dimension", "Extent"}
            and child.attrib.get("name") == "time"
            and (child.text or "").strip()
        ),
        None,
    )
    if time_node is None:
        raise KmiRadqpeContractError("WMS-tijddimensie ontbreekt")
    interval_parts = (time_node.text or "").strip().split("/")
    if len(interval_parts) != 3:
        raise KmiRadqpeContractError(
            "WMS-tijddimensie is geen expliciet interval"
        )
    start_at = parse_utc(interval_parts[0])
    end_at = parse_utc(interval_parts[1])
    cadence = _duration_seconds(interval_parts[2])
    default_at = parse_utc(time_node.attrib.get("default") or interval_parts[1])

    crs = tuple(sorted({
        (child.text or "").strip()
        for child in layer
        if _local_name(child) in {"CRS", "SRS"} and (child.text or "").strip()
    }))
    bbox_node = next(
        (
            child
            for child in layer
            if _local_name(child) in {
                "EX_GeographicBoundingBox",
                "LatLonBoundingBox",
            }
        ),
        None,
    )
    if bbox_node is None:
        raise KmiRadqpeContractError("WMS WGS84-bounds ontbreken")
    if _local_name(bbox_node) == "LatLonBoundingBox":
        bounds = tuple(
            float(bbox_node.attrib[key])
            for key in ("minx", "miny", "maxx", "maxy")
        )
    else:
        bounds = (
            float(_direct_text(bbox_node, "westBoundLongitude")),
            float(_direct_text(bbox_node, "southBoundLatitude")),
            float(_direct_text(bbox_node, "eastBoundLongitude")),
            float(_direct_text(bbox_node, "northBoundLatitude")),
        )
    if (
        len(bounds) != 4
        or not all(math.isfinite(value) for value in bounds)
        or not (bounds[0] < bounds[2] and bounds[1] < bounds[3])
    ):
        raise KmiRadqpeContractError("WMS-bounds zijn ongeldig")

    return PublicCatalogSnapshot(
        service_title=service_title,
        fees=fees,
        access_constraints=access,
        layer_name=expected_name,
        start_at=start_at,
        end_at=end_at,
        default_at=default_at,
        cadence_seconds=cadence,
        crs=crs,
        wgs84_bounds=bounds,
        payload_sha256=hashlib.sha256(payload).hexdigest(),
    )


def _float_pair(text: str, field: str) -> tuple[float, float]:
    try:
        values = tuple(float(item) for item in text.split())
    except ValueError as err:
        raise KmiRadqpeContractError(f"{field} is niet numeriek") from err
    if len(values) != 2 or not all(math.isfinite(item) for item in values):
        raise KmiRadqpeContractError(f"{field} moet twee eindige waarden hebben")
    return values


def parse_public_wcs_description(
    payload: bytes,
) -> PublicCoverageDescription:
    """Parse WCS structure while preserving UNKNOWN quantity semantics."""

    payload = _require_payload(
        payload,
        MAX_DESCRIPTION_BYTES,
        "WCS-beschrijving",
    )
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError as err:
        raise KmiRadqpeContractError("ongeldige WCS-beschrijving XML") from err
    description = _descendant(root, "CoverageDescription")
    coverage_id = _direct_text(description, "CoverageId")
    if coverage_id != PUBLIC_WCS_COVERAGE_ID:
        raise KmiRadqpeContractError("onverwachte WCS coverage-ID")
    coverage_name = _direct_text(description, "name")
    if coverage_name != PUBLIC_WMS_LAYER.split(":", 1)[1]:
        raise KmiRadqpeContractError("WCS coverage-naam wijkt af van WMS")

    envelope = _descendant(description, "EnvelopeWithTimePeriod")
    srs_name = envelope.attrib.get("srsName", "").strip()
    axis_labels = tuple(envelope.attrib.get("axisLabels", "").split())
    start_at = parse_utc(_direct_text(envelope, "beginPosition"))
    end_at = parse_utc(_direct_text(envelope, "endPosition"))

    time_domain = _descendant(description, "TimeDomain")
    default_at = parse_utc(time_domain.attrib.get("default", ""))
    interval_node = _descendant(time_domain, "timeInterval")
    interval_value = int((interval_node.text or "").strip())
    interval_unit = interval_node.attrib.get("unit", "").lower()
    unit_scale = {"second": 1, "minute": 60, "hour": 3600}.get(interval_unit)
    if unit_scale is None or interval_value <= 0:
        raise KmiRadqpeContractError("ongeldig WCS-tijdinterval")
    cadence = interval_value * unit_scale
    if cadence != EXPECTED_PUBLIC_CADENCE_SECONDS:
        raise KmiRadqpeContractError("WCS-cadans is niet vijf minuten")

    grid = _descendant(description, "RectifiedGrid")
    high_text = _direct_text(_descendant(grid, "GridEnvelope"), "high")
    try:
        high = tuple(int(item) for item in high_text.split())
    except ValueError as err:
        raise KmiRadqpeContractError("WCS-gridlimiet is niet integer") from err
    if len(high) != 2 or any(item < 0 for item in high):
        raise KmiRadqpeContractError("WCS-gridlimiet is ongeldig")
    width, height = high[0] + 1, high[1] + 1
    if width * height > MAX_RASTER_CELLS:
        raise KmiRadqpeContractError("WCS-grid overschrijdt celcontract")
    origin = _float_pair(
        _direct_text(_descendant(grid, "Point"), "pos"),
        "WCS-origin",
    )
    offsets = tuple(
        _float_pair((node.text or "").strip(), "WCS-offsetvector")
        for node in grid.iter()
        if _local_name(node) == "offsetVector"
    )
    if len(offsets) != 2:
        raise KmiRadqpeContractError("WCS-grid heeft niet twee offsetvectoren")

    field = _descendant(description, "field")
    range_field = field.attrib.get("name", "").strip()
    uom_node = _descendant(field, "uom")
    declared_uom = uom_node.attrib.get("code", "").strip()
    nil_values = tuple(
        (node.text or "").strip()
        for node in field.iter()
        if _local_name(node) == "nilValue" and (node.text or "").strip()
    )
    native_format = _direct_text(
        _descendant(description, "ServiceParameters"),
        "nativeFormat",
    )
    if native_format != "image/tiff":
        raise KmiRadqpeContractError("WCS nativeFormat is geen TIFF")

    blockers: list[str] = []
    if range_field.upper() not in {"RATE", "ACRR"}:
        blockers.append("range_field_does_not_declare_precipitation_quantity")
    normalized_uom = (
        declared_uom.lower()
        .replace(" ", "")
        .replace("·", ".")
        .replace("per", "/")
    )
    if normalized_uom not in {
        "mm/h",
        "mm.h-1",
        "mmhr-1",
        "mm",
    }:
        blockers.append("declared_uom_is_not_a_precipitation_unit")
    if not nil_values:
        blockers.append("wcs_description_has_no_nil_value")

    return PublicCoverageDescription(
        coverage_id=coverage_id,
        coverage_name=coverage_name,
        start_at=start_at,
        end_at=end_at,
        default_at=default_at,
        cadence_seconds=cadence,
        srs_name=srs_name,
        axis_labels=axis_labels,
        width=width,
        height=height,
        origin=origin,
        offset_vectors=offsets,
        range_field=range_field,
        declared_uom=declared_uom,
        declared_nil_values=nil_values,
        native_format=native_format,
        semantic_blockers=tuple(blockers),
        payload_sha256=hashlib.sha256(payload).hexdigest(),
    )


def _geotiff_epsg(values: tuple[int, ...]) -> int | None:
    if len(values) < 4:
        return None
    key_count = values[3]
    if len(values) < 4 + key_count * 4:
        raise KmiRadqpeContractError("afgekorte GeoKeyDirectory")
    found: dict[int, int] = {}
    for index in range(key_count):
        key_id, location, count, value = values[
            4 + index * 4: 8 + index * 4
        ]
        if location == 0 and count == 1:
            found[key_id] = value
    return found.get(3072) or found.get(2048)


def _geotiff_transform(tags) -> tuple[float, ...]:
    matrix = tags.get(34264)
    if matrix is not None:
        values = tuple(float(item) for item in matrix)
        if len(values) != 16 or not all(math.isfinite(item) for item in values):
            raise KmiRadqpeContractError("ongeldige ModelTransformationTag")
        return values
    tie = tags.get(33922)
    scale = tags.get(33550)
    if tie is None or scale is None:
        raise KmiRadqpeContractError("GeoTIFF mist georeferentie")
    tie_values = tuple(float(item) for item in tie)
    scale_values = tuple(float(item) for item in scale)
    if (
        len(tie_values) < 6
        or len(scale_values) < 2
        or not all(
            math.isfinite(item)
            for item in (*tie_values[:6], *scale_values[:2])
        )
        or scale_values[0] <= 0
        or scale_values[1] <= 0
    ):
        raise KmiRadqpeContractError("ongeldige GeoTIFF tiepoint/pixelschaal")
    raster_x, raster_y, _, model_x, model_y, _ = tie_values[:6]
    x_scale, y_scale = scale_values[:2]
    x_origin = model_x - raster_x * x_scale
    y_origin = model_y + raster_y * y_scale
    return (
        x_scale, 0.0, 0.0, x_origin,
        0.0, -y_scale, 0.0, y_origin,
        0.0, 0.0, 0.0, 0.0,
        0.0, 0.0, 0.0, 1.0,
    )


def _transform_point(
    transform: tuple[float, ...],
    column: float,
    row: float,
) -> tuple[float, float]:
    return (
        transform[0] * column + transform[1] * row + transform[3],
        transform[4] * column + transform[5] * row + transform[7],
    )


def _parse_nodata(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (tuple, list)):
        if len(value) != 1:
            raise KmiRadqpeContractError("meervoudige GDAL_NODATA-tag")
        value = value[0]
    if isinstance(value, bytes):
        value = value.decode("ascii", errors="strict")
    text = str(value).rstrip("\x00").strip()
    try:
        parsed = float(text)
    except ValueError as err:
        raise KmiRadqpeContractError("GDAL_NODATA is niet numeriek") from err
    return parsed


def audit_public_wcs_geotiff(
    payload: bytes,
    description: PublicCoverageDescription,
) -> PublicGeoTiffAudit:
    """Audit exact WCS TIFF bytes without assigning meteorological meaning."""

    payload = _require_payload(payload, MAX_COVERAGE_BYTES, "WCS-coverage")
    if not isinstance(description, PublicCoverageDescription):
        raise TypeError("description moet PublicCoverageDescription zijn")
    try:
        import numpy as np
        from PIL import Image
    except ImportError as err:
        raise RuntimeError("Pillow en NumPy zijn vereist voor TIFF-audit") from err

    try:
        with Image.open(io.BytesIO(payload)) as image:
            image.load()
            if image.format != "TIFF":
                raise KmiRadqpeContractError("WCS-coverage is geen TIFF")
            if getattr(image, "n_frames", 1) != 1:
                raise KmiRadqpeContractError("WCS-coverage is multi-frame")
            if len(image.getbands()) != 1:
                raise KmiRadqpeContractError(
                    "WCS-coverage is geen enkelbandsraster"
                )
            width, height = int(image.width), int(image.height)
            if (
                width <= 0
                or height <= 0
                or width * height > MAX_RASTER_CELLS
            ):
                raise KmiRadqpeContractError(
                    "WCS-coverage overschrijdt rastercontract"
                )
            if (width, height) != (description.width, description.height):
                raise KmiRadqpeContractError(
                    "TIFF-afmetingen wijken af van DescribeCoverage"
                )
            tags = image.tag_v2
            geokeys = tuple(int(item) for item in tags.get(34735, ()))
            epsg = _geotiff_epsg(geokeys)
            transform = _geotiff_transform(tags)
            embedded_nodata = _parse_nodata(tags.get(42113))
            data = np.asarray(image)
    except KmiRadqpeContractError:
        raise
    except Exception as err:
        raise KmiRadqpeContractError("ongeldige WCS GeoTIFF") from err

    if data.ndim != 2 or data.shape != (height, width):
        raise KmiRadqpeContractError("TIFF-rastervorm is ongeldig")
    if data.dtype.kind not in {"f", "i", "u"}:
        raise KmiRadqpeContractError("TIFF-raster is niet numeriek")
    if epsg != 4326:
        raise KmiRadqpeContractError(
            "TIFF-CRS wijkt af van WCS DescribeCoverage"
        )
    expected_transform = (
        description.offset_vectors[0][1],
        description.offset_vectors[1][1],
        0.0,
        description.origin[1],
        description.offset_vectors[0][0],
        description.offset_vectors[1][0],
        0.0,
        description.origin[0],
    )
    actual_transform = (
        transform[0],
        transform[1],
        transform[2],
        transform[3],
        transform[4],
        transform[5],
        transform[6],
        transform[7],
    )
    if any(
        not math.isclose(actual, expected, abs_tol=1e-10, rel_tol=1e-10)
        for actual, expected in zip(
            actual_transform,
            expected_transform,
            strict=True,
        )
    ):
        raise KmiRadqpeContractError(
            "TIFF-geotransformatie wijkt af van WCS DescribeCoverage"
        )
    numeric = data.astype(np.float64, copy=False)
    finite = np.isfinite(numeric)
    finite_values = numeric[finite]
    corners = (
        _transform_point(transform, 0, 0),
        _transform_point(transform, width, 0),
        _transform_point(transform, 0, height),
        _transform_point(transform, width, height),
    )
    xs = tuple(item[0] for item in corners)
    ys = tuple(item[1] for item in corners)
    bounds = (min(xs), min(ys), max(xs), max(ys))
    if not all(math.isfinite(value) for value in bounds):
        raise KmiRadqpeContractError("TIFF-bounds zijn niet eindig")

    blockers = list(description.semantic_blockers)
    blockers.extend((
        "public_tiff_has_no_embedded_precipitation_quantity",
        "public_tiff_has_no_embedded_precipitation_unit",
        "public_tiff_has_no_embedded_nominal_time",
    ))
    if embedded_nodata is None:
        blockers.append("public_tiff_has_no_embedded_nodata")
    if epsg != NATIVE_CRS_EPSG:
        blockers.append("public_tiff_is_not_native_lambert_2008_grid")

    return PublicGeoTiffAudit(
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        size_bytes=len(payload),
        width=width,
        height=height,
        mode=str(data.dtype),
        epsg=epsg,
        transform=transform,
        bounds=bounds,
        embedded_nodata=embedded_nodata,
        finite_samples=int(finite.sum()),
        nonfinite_samples=int((~finite).sum()),
        negative_samples=int((finite & (numeric < 0)).sum()),
        minimum=(
            float(np.min(finite_values)) if finite_values.size else None
        ),
        maximum=(
            float(np.max(finite_values)) if finite_values.size else None
        ),
        semantic_blockers=tuple(dict.fromkeys(blockers)),
    )


def public_capabilities_request() -> dict[str, str]:
    return {
        "SERVICE": "WMS",
        "VERSION": "1.3.0",
        "REQUEST": "GetCapabilities",
    }


def public_description_request() -> dict[str, str]:
    return {
        "SERVICE": "WCS",
        "VERSION": "2.0.1",
        "REQUEST": "DescribeCoverage",
        "COVERAGEID": PUBLIC_WCS_COVERAGE_ID,
    }


def public_coverage_request(nominal_at: float) -> dict[str, str]:
    return {
        "SERVICE": "WCS",
        "VERSION": "2.0.1",
        "REQUEST": "GetCoverage",
        "COVERAGEID": PUBLIC_WCS_COVERAGE_ID,
        "FORMAT": "image/tiff",
        "SUBSET": f'time("{format_utc(nominal_at)}")',
    }


def native_filename_facts(filename: str) -> dict[str, Any]:
    """Parse only documented native filename facts; reject every other shape."""

    if not isinstance(filename, str):
        raise TypeError("filename moet tekst zijn")
    match = _NATIVE_FILENAME_RE.fullmatch(filename)
    if match is None:
        raise KmiRadqpeContractError("filename is geen gedocumenteerd RADQPE-product")
    timestamp = datetime.strptime(
        match.group("time"),
        "%Y%m%d%H%M%S",
    ).replace(tzinfo=timezone.utc).timestamp()
    return {
        "filename": filename,
        "nominal_at": timestamp,
        "nominal_time": format_utc(timestamp),
        "quantity_token": match.group("quantity"),
        "extension": match.group("extension"),
        "native_crs": NATIVE_CRS_NAME,
        "native_resolution_metres": NATIVE_RESOLUTION_METRES,
        "access_configured": False,
        "detection_eligible": False,
    }


__all__ = [
    "ContractGate",
    "EXPECTED_PUBLIC_CADENCE_SECONDS",
    "KMI_RADQPE_SHADOW_CONTRACT",
    "KmiPublicCompositeArtifactSet",
    "KmiRadqpeContractError",
    "KmiRadqpeShadowContract",
    "MAX_CAPABILITIES_BYTES",
    "MAX_COVERAGE_BYTES",
    "MAX_DESCRIPTION_BYTES",
    "NATIVE_CRS_EPSG",
    "NATIVE_CRS_NAME",
    "NATIVE_PRODUCT_RATE",
    "NATIVE_RESOLUTION_METRES",
    "NATIVE_SOURCE_ID",
    "PUBLIC_OWS_ENDPOINT",
    "PUBLIC_SOURCE_ID",
    "PUBLIC_WCS_COVERAGE_ID",
    "PUBLIC_WMS_LAYER",
    "PublicCatalogSnapshot",
    "PublicCoverageDescription",
    "PublicGeoTiffAudit",
    "SCHEMA_VERSION",
    "SHADOW_ONLY",
    "ShadowArtifact",
    "audit_public_wcs_geotiff",
    "format_utc",
    "native_filename_facts",
    "parse_public_wcs_description",
    "parse_public_wms_capabilities",
    "parse_utc",
    "public_capabilities_request",
    "public_coverage_request",
    "public_description_request",
]
