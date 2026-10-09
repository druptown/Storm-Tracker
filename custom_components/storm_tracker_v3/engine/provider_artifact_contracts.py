"""Shadow-only provenance contracts for every operational weather provider.

This module is deliberately inert: it performs no I/O, imports no provider
transport and writes no evidence.  It describes the bytes that a future raw
capture boundary must retain before any decode, conversion or aggregation.

Unknown facts are represented by the literal ``UNKNOWN``.  Facts that must be
read from the upstream artifact are represented as ``ARTIFACT_METADATA``.
Neither state may silently acquire an assumed value.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import re
from types import MappingProxyType
from typing import Any, Mapping
from urllib.parse import parse_qsl, urlsplit


SCHEMA_VERSION = "provider-artifact-contract-v1"
SHADOW_ONLY = True
UNKNOWN = "UNKNOWN"
ARTIFACT_METADATA = "ARTIFACT_METADATA"


class FactKind(str, Enum):
    """How a contract fact is known."""

    FIXED = "FIXED"
    ARTIFACT_METADATA = ARTIFACT_METADATA
    UNKNOWN = UNKNOWN


class RawReplay(str, Enum):
    """Whether exact upstream bytes are sufficient for deterministic replay."""

    FULL = "FULL"
    CONDITIONAL = "CONDITIONAL"
    NOT_CURRENTLY_POSSIBLE = "NOT_CURRENTLY_POSSIBLE"


class RawCaptureStatus(str, Enum):
    """What the current transport exposes before this shadow layer is wired."""

    EXACT_BYTES_TRANSIENT = "EXACT_BYTES_TRANSIENT"
    MULTI_ARTIFACT_TRANSIENT = "MULTI_ARTIFACT_TRANSIENT"
    DECODED_BODY_ONLY = "DECODED_BODY_ONLY"
    WORKER_DERIVED_ARTIFACT_ONLY = "WORKER_DERIVED_ARTIFACT_ONLY"


class RetentionClass(str, Enum):
    """Local evidence-retention gate, not a claim about copyright ownership."""

    PUBLIC_REPLAY_EVIDENCE = "PUBLIC_REPLAY_EVIDENCE"
    ATTRIBUTION_REPLAY_EVIDENCE = "ATTRIBUTION_REPLAY_EVIDENCE"
    CONTRACT_REVIEW_HOLD = "CONTRACT_REVIEW_HOLD"


class CapabilityStatus(str, Enum):
    """What the implemented provider actually emits today."""

    PRECIPITATION_RASTER = "PRECIPITATION_RASTER"
    GROUND_OBSERVATIONS = "GROUND_OBSERVATIONS"
    CAPABILITY_MISMATCH = "CAPABILITY_MISMATCH"


@dataclass(frozen=True, slots=True)
class Fact:
    """A value with explicit fixed, artifact-derived or unknown semantics."""

    kind: FactKind
    value: str | None = None
    note: str = ""

    def __post_init__(self) -> None:
        if self.kind is FactKind.UNKNOWN and self.value is not None:
            raise ValueError("UNKNOWN fact cannot carry a value")
        if self.kind is not FactKind.UNKNOWN and not self.value:
            raise ValueError(f"{self.kind.value} fact requires a value")

    def to_dict(self) -> dict[str, str | None]:
        return {
            "kind": self.kind.value,
            "value": self.value,
            "note": self.note,
        }


def fixed(value: str, note: str = "") -> Fact:
    return Fact(FactKind.FIXED, value, note)


def artifact_metadata(path: str, note: str = "") -> Fact:
    return Fact(FactKind.ARTIFACT_METADATA, path, note)


def unknown(note: str) -> Fact:
    return Fact(FactKind.UNKNOWN, None, note)


class RedactionError(ValueError):
    """Raised when record identity or metadata may contain a credential."""


_SENSITIVE_KEY = re.compile(
    r"(?:^|[_-])(?:authorization|auth|password|passwd|secret|client_id|client_secret|"
    r"access_token|refresh_token|id_token|api_key|apikey|subscription_key|"
    r"x_api_key|token|credential|private_key|signature|sig)(?:$|[_-])",
    re.IGNORECASE,
)
_AUTH_VALUE = re.compile(r"\b(?:Bearer|Basic)\s+[A-Za-z0-9+/_.=-]{8,}", re.I)
_JWT_VALUE = re.compile(
    r"(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{12,}\."
    r"[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{8,}(?![A-Za-z0-9_-])"
)
_ASSIGNMENT_SECRET = re.compile(
    r"(?:access[_-]?token|refresh[_-]?token|api[_-]?key|client[_-]?secret|"
    r"password|x-amz-signature|signature|sig)=",
    re.I,
)
_SENSITIVE_QUERY_KEYS = {
    "access_token",
    "api_key",
    "apikey",
    "authorization",
    "client_secret",
    "credential",
    "k",
    "key",
    "password",
    "refresh_token",
    "sig",
    "signature",
    "subscription-key",
    "token",
    "x-amz-credential",
    "x-amz-signature",
    "x-api-key",
}


def _validate_string(value: str, path: str, *, record_identity: bool) -> None:
    if _AUTH_VALUE.search(value) or _JWT_VALUE.search(value):
        raise RedactionError(f"{path} contains an authorization credential")
    if _ASSIGNMENT_SECRET.search(value):
        raise RedactionError(f"{path} contains a secret assignment")
    parsed = urlsplit(value)
    if parsed.scheme and parsed.netloc:
        if record_identity:
            raise RedactionError(f"{path} must not contain a full URL")
        if parsed.username or parsed.password:
            raise RedactionError(f"{path} contains URL user information")
        query_keys = {
            key.lower() for key, _ in parse_qsl(parsed.query, keep_blank_values=True)
        }
        if query_keys & _SENSITIVE_QUERY_KEYS:
            raise RedactionError(f"{path} contains a signed or secret URL")
    elif record_identity and any(char in value for char in ("?", "#")):
        raise RedactionError(f"{path} contains URL query or fragment material")


def validate_redacted(
    value: Any,
    *,
    record_identity: bool = False,
    _path: str = "$",
) -> None:
    """Recursively reject credentials and secret/signed URL material.

    Record identities are stricter than descriptive metadata: they may not
    contain any full URL, query string or fragment.  Ordinary documentation
    URLs are allowed in metadata, but URLs with credentials or secret query
    parameters are not.
    """

    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            if _SENSITIVE_KEY.search(key_text):
                raise RedactionError(f"{_path}.{key_text} is a sensitive key")
            validate_redacted(
                item,
                record_identity=record_identity,
                _path=f"{_path}.{key_text}",
            )
        return
    if isinstance(value, (list, tuple, set, frozenset)):
        for index, item in enumerate(value):
            validate_redacted(
                item,
                record_identity=record_identity,
                _path=f"{_path}[{index}]",
            )
        return
    if isinstance(value, str):
        _validate_string(value, _path, record_identity=record_identity)


@dataclass(frozen=True, slots=True)
class ProviderArtifactContract:
    """Declarative upstream-artifact and replay contract."""

    provider: str
    registry_role: str
    capability_status: CapabilityStatus
    product: str
    quantity: str
    upstream_authority: str
    authoritative_artifact: str
    capture_boundary: str
    record_id_strategy: str
    record_id_components: tuple[str, ...]
    allowed_media_types: tuple[str, ...]
    nominal_timestamp_source: Fact
    timestamp_policy: str
    native_crs: Fact
    unit_semantics: Fact
    gain_offset_semantics: Fact
    nodata_semantics: Fact
    undetect_semantics: Fact
    qc_semantics: Fact
    native_resolution: Fact
    native_dimensions: Fact
    parser_responsibility: str
    parser_version: str
    license_terms: Fact
    attribution: Fact
    retention_class: RetentionClass
    raw_replay: RawReplay
    raw_capture_status: RawCaptureStatus
    transport_gap: str
    primary_sources: tuple[str, ...]

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z0-9_]+", self.provider):
            raise ValueError(f"invalid provider id {self.provider!r}")
        if not self.record_id_components:
            raise ValueError(f"{self.provider} has no record identity components")
        if (
            self.capability_status is CapabilityStatus.CAPABILITY_MISMATCH
            and self.raw_replay is not RawReplay.NOT_CURRENTLY_POSSIBLE
        ):
            raise ValueError(
                f"{self.provider} capability mismatch cannot be raw-replayable"
            )
        if not self.allowed_media_types:
            raise ValueError(f"{self.provider} has no media-type contract")
        if any(
            not item or (";" in item and item != UNKNOWN)
            for item in self.allowed_media_types
        ):
            raise ValueError(f"{self.provider} media types must be canonical")
        if not self.primary_sources or any(
            not source.startswith("https://") for source in self.primary_sources
        ):
            raise ValueError(f"{self.provider} requires HTTPS primary sources")
        validate_redacted(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "shadow_only": SHADOW_ONLY,
            "provider": self.provider,
            "registry_role": self.registry_role,
            "capability_status": self.capability_status.value,
            "operational_eligible": self.operational_eligible,
            "product": self.product,
            "quantity": self.quantity,
            "upstream_authority": self.upstream_authority,
            "authoritative_artifact": self.authoritative_artifact,
            "capture_boundary": self.capture_boundary,
            "record_id_strategy": self.record_id_strategy,
            "record_id_components": list(self.record_id_components),
            "allowed_media_types": list(self.allowed_media_types),
            "nominal_timestamp_source": self.nominal_timestamp_source.to_dict(),
            "timestamp_policy": self.timestamp_policy,
            "native_crs": self.native_crs.to_dict(),
            "unit_semantics": self.unit_semantics.to_dict(),
            "gain_offset_semantics": self.gain_offset_semantics.to_dict(),
            "nodata_semantics": self.nodata_semantics.to_dict(),
            "undetect_semantics": self.undetect_semantics.to_dict(),
            "qc_semantics": self.qc_semantics.to_dict(),
            "native_resolution": self.native_resolution.to_dict(),
            "native_dimensions": self.native_dimensions.to_dict(),
            "parser_responsibility": self.parser_responsibility,
            "parser_version": self.parser_version,
            "license_terms": self.license_terms.to_dict(),
            "attribution": self.attribution.to_dict(),
            "retention_class": self.retention_class.value,
            "raw_replay": self.raw_replay.value,
            "raw_capture_status": self.raw_capture_status.value,
            "transport_gap": self.transport_gap,
            "primary_sources": list(self.primary_sources),
        }

    @property
    def operational_eligible(self) -> bool:
        """Whether this implementation emits its declared evidence capability."""

        return self.capability_status is not CapabilityStatus.CAPABILITY_MISMATCH


_CLOCK_POLICY = (
    "Preserve source time and receipt time independently; an absent or "
    "unparseable source time remains UNKNOWN. Future time/clock skew is "
    "quality evidence to quarantine, never rewritten to receipt time."
)


def _contract(**values: Any) -> ProviderArtifactContract:
    values.setdefault("timestamp_policy", _CLOCK_POLICY)
    return ProviderArtifactContract(**values)


_CONTRACT_SEQUENCE = (
    _contract(
        provider="kmi",
        registry_role="radar",
        capability_status=CapabilityStatus.PRECIPITATION_RASTER,
        product="KMI appv4 radar animation image",
        quantity="rendered categorical precipitation intensity; not a calibrated rain rate",
        upstream_authority="Royal Meteorological Institute of Belgium (KMI/IRM)",
        authoritative_artifact="selected animation.sequence image response body",
        capture_boundary="exact HTTP image-body bytes before Pillow/RGBA conversion",
        record_id_strategy="hash canonical provider/product/sequence-time/URI-path/payload-digest",
        record_id_components=("sequence item time", "URI path without host/query", "payload SHA-256"),
        allowed_media_types=("image/png",),
        nominal_timestamp_source=artifact_metadata(
            "getForecasts.animation.sequence[].time",
            "Timestamp is catalogue metadata external to the image bytes.",
        ),
        native_crs=unknown(
            "The rendered image carries no CRS. Current code applies a fixed lat/lon box; "
            "that implementation geometry is not evidence of the native radar CRS."
        ),
        unit_semantics=fixed(
            "RGBA palette mapped to Storm Tracker classes 0..8",
            "Categorical rendering only; no physical unit may be inferred.",
        ),
        gain_offset_semantics=fixed("not applicable to rendered palette"),
        nodata_semantics=fixed("alpha below 128 or unrecognised colour is parser-empty"),
        undetect_semantics=unknown("No separate undetect value is exposed by the rendered image."),
        qc_semantics=unknown("No authoritative pixel QC layer is exposed by this endpoint."),
        native_resolution=unknown("Native radar resolution is not present in the rendered artifact."),
        native_dimensions=artifact_metadata("decoded image width and height", "Usually 512x512 in current transport."),
        parser_responsibility="providers.kmi.KmiProvider._extract_observations",
        parser_version="kmi-rendered-palette-unversioned",
        license_terms=unknown("Endpoint-specific reuse and raw-retention terms are not documented in-repo."),
        attribution=fixed("KMI/IRM"),
        retention_class=RetentionClass.CONTRACT_REVIEW_HOLD,
        raw_replay=RawReplay.CONDITIONAL,
        raw_capture_status=RawCaptureStatus.EXACT_BYTES_TRANSIENT,
        transport_gap=(
            "Persist catalogue bytes and selected image bytes as linked receipts; strip the "
            "credential-bearing query and never persist the hard-coded app credential."
        ),
        primary_sources=(
            "https://app.meteo.be/services/appv4/",
            "https://www.meteo.be/en/about-rmi/legal-information",
        ),
    ),
    _contract(
        provider="knmi",
        registry_role="radar",
        capability_status=CapabilityStatus.PRECIPITATION_RASTER,
        product="nl-rdr-data-rtcor-5m plus radar_forecast_2.0 WMS renderings",
        quantity="server-rendered precipitation accumulation/nowcast palette",
        upstream_authority="Royal Netherlands Meteorological Institute (KNMI)",
        authoritative_artifact="WMS GetMap PNG response for a canonical dataset/layer/time/grid request",
        capture_boundary="exact HTTP response-body bytes before Pillow/RGBA conversion",
        record_id_strategy="hash canonical WMS request fields and response payload digest; omit authorization",
        record_id_components=("dataset", "layer", "TIME", "CRS/BBOX/size/style", "payload SHA-256"),
        allowed_media_types=("image/png",),
        nominal_timestamp_source=fixed(
            "canonical WMS TIME request field",
            "It is the requested valid time; response-side product time is not currently verified.",
        ),
        native_crs=fixed("EPSG:4326 WMS output grid", "This is the requested render CRS, not the source HDF5 CRS."),
        unit_semantics=fixed("rendered radar/nearest palette mapped to classes 0..8"),
        gain_offset_semantics=fixed("not applicable to WMS rendering"),
        nodata_semantics=fixed("alpha below 64 or parser-empty colour"),
        undetect_semantics=unknown("No separate undetect code is available after WMS rendering."),
        qc_semantics=unknown("No response QC layer is captured by the WMS rendering."),
        native_resolution=fixed("requested output grid over fixed bbox; physical spacing varies with latitude"),
        native_dimensions=fixed("512x512 pixels"),
        parser_responsibility="providers.knmi.KnmiProvider._extract_observations",
        parser_version="knmi-wms-palette-unversioned",
        license_terms=fixed("CC BY 4.0 for the named KNMI radar datasets"),
        attribution=fixed("KNMI"),
        retention_class=RetentionClass.ATTRIBUTION_REPLAY_EVIDENCE,
        raw_replay=RawReplay.CONDITIONAL,
        raw_capture_status=RawCaptureStatus.EXACT_BYTES_TRANSIENT,
        transport_gap="Expose the exact PNG plus canonical non-secret WMS request and response headers.",
        primary_sources=(
            "https://dataplatform.knmi.nl/dataset/?tags=Radar&tags=WMS",
            "https://api.dataplatform.knmi.nl/wms/adaguc-server",
        ),
    ),
    _contract(
        provider="dwd_radolan",
        registry_role="radar",
        capability_status=CapabilityStatus.PRECIPITATION_RASTER,
        product="RADVOR RV +000 composite member in composite_rv_LATEST.tar",
        quantity="ACRR five-minute precipitation accumulation converted by parser to mm/h",
        upstream_authority="Deutscher Wetterdienst (DWD)",
        authoritative_artifact="complete RV tar response, retaining the selected _000-hd5 member identity",
        capture_boundary="exact tar response bytes before tar extraction or HDF5 decode",
        record_id_strategy="hash selected member name/product end-time/archive payload digest",
        record_id_components=("selected member name", "dataset1/what enddate+endtime", "archive SHA-256"),
        allowed_media_types=("application/x-tar", "application/octet-stream"),
        nominal_timestamp_source=artifact_metadata("dataset1/what enddate + endtime"),
        native_crs=artifact_metadata("/where/projdef"),
        unit_semantics=artifact_metadata(
            "dataset1/data1/what quantity",
            "Current parser treats decoded ACRR as a five-minute accumulation and multiplies by 12.",
        ),
        gain_offset_semantics=artifact_metadata("dataset1/data1/what gain + offset"),
        nodata_semantics=artifact_metadata("dataset1/data1/what nodata"),
        undetect_semantics=artifact_metadata("dataset1/data1/what undetect"),
        qc_semantics=unknown("The current +000 parser does not consume a separate quality field."),
        native_resolution=artifact_metadata("/where xscale + yscale"),
        native_dimensions=artifact_metadata("/where xsize + ysize and data shape"),
        parser_responsibility="worker.radar.parse_rv_archive / providers.dwd_radolan.parse_rv_archive",
        parser_version="dwd-rv-odim-current",
        license_terms=fixed("DWD Open Data under CC BY 4.0/GeoNutzV source-attribution terms"),
        attribution=fixed("Deutscher Wetterdienst (DWD)"),
        retention_class=RetentionClass.ATTRIBUTION_REPLAY_EVIDENCE,
        raw_replay=RawReplay.FULL,
        raw_capture_status=RawCaptureStatus.WORKER_DERIVED_ARTIFACT_ONLY,
        transport_gap="Worker retains only derived gzip-JSON; expose and persist the upstream tar before extraction.",
        primary_sources=(
            "https://opendata.dwd.de/weather/radar/composite/rv/",
            "https://www.dwd.de/DE/leistungen/radolan/radolan.html",
            "https://www.dwd.de/DE/derdwd/datenpolitik.html",
        ),
    ),
    _contract(
        provider="meteofrance_radar",
        registry_role="radar",
        capability_status=CapabilityStatus.PRECIPITATION_RASTER,
        product=(
            "DPRadar METROPOLE LAME_D_EAU IPRN20_C_LFPW 500 m mosaic"
        ),
        quantity=(
            "five-minute precipitation accumulation; not an instantaneous "
            "rain rate"
        ),
        upstream_authority="Météo-France",
        authoritative_artifact=(
            "exact DPRadar catalogue JSON linked to the selected original "
            "IPRN20_C_LFPW HDF5 response"
        ),
        capture_boundary=(
            "atomically retain catalogue and HDF5 response bodies before "
            "catalogue selection or HDF5 inspection; omit OAuth material"
        ),
        record_id_strategy=(
            "hash catalogue validity time, official filename and both payload "
            "digests; omit OAuth/token responses"
        ),
        record_id_components=(
            "catalogue validity_time",
            "T_IPRN20_C_LFPW_YYYYMMDDhhmmss.h5 filename",
            "catalogue SHA-256",
            "HDF5 SHA-256",
        ),
        allowed_media_types=(
            "application/json",
            "application/x-hdf5",
            "application/octet-stream",
        ),
        nominal_timestamp_source=artifact_metadata(
            "catalogue validity_time bound to UTC timestamp in the official filename"
        ),
        native_crs=unknown(
            "The audited product documentation does not pin the exact HDF5 "
            "CRS/projdef, origin and pixel support consumed by the decoder."
        ),
        unit_semantics=fixed(
            "five-minute precipitation accumulation in mm; the published "
            "storage increment is 0.01 mm"
        ),
        gain_offset_semantics=unknown(
            "The exact HDF5 dataset path and per-field scale mapping remain "
            "uncertified despite the published 0.01 mm storage increment."
        ),
        nodata_semantics=unknown(
            "The exact HDF5 missing-value field/code mapping is not certified."
        ),
        undetect_semantics=unknown(
            "No separate exact HDF5 undetect mapping is certified."
        ),
        qc_semantics=unknown(
            "Météo-France documents QC values from 0 to 100, but the exact "
            "HDF5 field/code mapping has not been certified."
        ),
        native_resolution=fixed("500 m square mesh"),
        native_dimensions=fixed("3472 x 3472"),
        parser_responsibility=(
            "providers.odim_hdf5.parse_odim_rainfall (legacy shared parser, "
            "not certified for this exact HDF5 field layout)"
        ),
        parser_version="meteofrance-iprn20-container-only-v1",
        license_terms=fixed(
            "French Licence Ouverte / Etalab 2.0 permits free commercial and "
            "non-commercial reuse subject to source and update attribution."
        ),
        attribution=fixed("Source: Météo-France; include product/update time"),
        retention_class=RetentionClass.ATTRIBUTION_REPLAY_EVIDENCE,
        raw_replay=RawReplay.FULL,
        raw_capture_status=RawCaptureStatus.MULTI_ARTIFACT_TRANSIENT,
        transport_gap=(
            "An isolated schema-3 catalogue+HDF5 capture candidate exists; "
            "certify exact HDF5 field/CRS/scale/missing/undetect/QC semantics, "
            "a real golden and held-out events before decoder admission or wiring."
        ),
        primary_sources=(
            "https://confluence-meteofrance.atlassian.net/wiki/spaces/OpenDataMeteoFrance/pages/853639381/API%2BCibl%2Be%2BDonn%2Bes%2BRadar%2BEN",
            "https://donneespubliques.meteofrance.fr/client/document/descriptiftechnique_radar_donneespubliques_v1-2_20250318_404.pdf",
            "https://confluence-meteofrance.atlassian.net/wiki/spaces/OpenDataMeteoFrance/pages/457803314/Quels%2Bsont%2Bles%2Bdroits%2Bde%2Br%2B-utilisation%2Bet%2Bcomment%2Bsourcer%2Bles%2Bdonn%2Bes%2Bpubliques%2Bde%2BM%2Bt%2Bo-France",
        ),
    ),
    _contract(
        provider="dpc_radar",
        registry_role="radar",
        capability_status=CapabilityStatus.PRECIPITATION_RASTER,
        product="Protezione Civile SRI GeoTIFF",
        quantity="SRI rendered/category values; physical rain-rate mapping is not established",
        upstream_authority="Dipartimento della Protezione Civile (Italy)",
        authoritative_artifact="downloadProduct GeoTIFF response linked to findLastProductByType catalogue",
        capture_boundary="exact GeoTIFF body before Pillow decode, plus exact catalogue body",
        record_id_strategy="hash product type/catalogue epoch/payload digest",
        record_id_components=("SRI", "catalogue productDate milliseconds", "GeoTIFF SHA-256"),
        allowed_media_types=("image/tiff", "application/octet-stream"),
        nominal_timestamp_source=artifact_metadata("findLastProductByType lastProducts[0].time"),
        native_crs=fixed("DPC Lambert conformal conic implementation constant"),
        unit_semantics=unknown("Parser currently treats TIFF byte values as intensity input without a verified legend."),
        gain_offset_semantics=unknown("No verified TIFF scale/offset contract is consumed."),
        nodata_semantics=fixed("zero is parser-empty"),
        undetect_semantics=unknown("No separate undetect code is consumed."),
        qc_semantics=unknown("No pixel QC layer is consumed."),
        native_resolution=artifact_metadata("GeoTIFF ModelPixelScaleTag"),
        native_dimensions=artifact_metadata("GeoTIFF image dimensions"),
        parser_responsibility="providers.dpc_radar._parse_sri_product",
        parser_version="dpc-sri-geotiff-unversioned",
        license_terms=unknown("Machine API reuse and raw-retention terms require source-specific review."),
        attribution=fixed("Dipartimento della Protezione Civile"),
        retention_class=RetentionClass.CONTRACT_REVIEW_HOLD,
        raw_replay=RawReplay.CONDITIONAL,
        raw_capture_status=RawCaptureStatus.MULTI_ARTIFACT_TRANSIENT,
        transport_gap="Persist catalogue JSON and downloaded TIFF as linked raw receipts before either is decoded.",
        primary_sources=(
            "https://radar-api.protezionecivile.it/",
            "https://radar.protezionecivile.it/",
        ),
    ),
    _contract(
        provider="aemet_radar",
        registry_role="radar",
        capability_status=CapabilityStatus.PRECIPITATION_RASTER,
        product=(
            "AEMET national lowest-elevation PPI reflectivity composite "
            "for mainland Spain and the Balearic Islands"
        ),
        quantity=(
            "radar reflectivity intervals in dBZ; not precipitation rate and "
            "not legacy byte classes 1..8"
        ),
        upstream_authority="Agencia Estatal de Meteorología (AEMET)",
        authoritative_artifact=(
            "official OpenData success envelope plus the exact datos response "
            "and exact metadatos response"
        ),
        capture_boundary=(
            "atomically retain all three response bodies before inspecting "
            "or decoding any member; never retain api_key"
        ),
        record_id_strategy=(
            "hash product identity, proven source time and all three payload "
            "digests; never use api_key or temporary response URLs"
        ),
        record_id_components=(
            "OpenData envelope SHA-256",
            "datos response SHA-256",
            "metadatos response SHA-256",
            "source product time only when metadata proves timezone and support",
        ),
        allowed_media_types=(
            "application/json",
            "image/tiff",
            "application/gzip",
            "application/x-gzip",
            "application/octet-stream",
        ),
        nominal_timestamp_source=unknown(
            "The audited official sources do not prove the timezone or support "
            "represented by the legacy down_radwYYYYMMDDHHMM filename token."
        ),
        native_crs=fixed("EPSG:4326 GeoTIFF"),
        unit_semantics=artifact_metadata(
            "per-file ESCALA metadata maps RGBA colours to reflectivity "
            "intervals expressed in dBZ"
        ),
        gain_offset_semantics=unknown(
            "AEMET documents an ESCALA colour-to-interval mapping, not a "
            "verified numeric gain/offset contract for the audited raster."
        ),
        nodata_semantics=unknown(
            "Exact codes distinguishing no-data, no-echo and no operational "
            "radar coverage are not proven by the audited sources."
        ),
        undetect_semantics=unknown(
            "No exact sample or RGBA code for undetect/no-echo is proven."
        ),
        qc_semantics=unknown(
            "No complete per-pixel quality field or code table is published "
            "for the audited national product."
        ),
        native_resolution=unknown(
            "The exact resolution, pixel support and affine transform must be "
            "verified from a current complete artifact set."
        ),
        native_dimensions=artifact_metadata(
            "current datos GeoTIFF dimensions, verified against metadatos"
        ),
        parser_responsibility=(
            "providers.aemet_radar.latest_frame_from_archive + "
            "parse_aemet_geotiff (legacy, not quantitatively certified)"
        ),
        parser_version="aemet-legacy-unverified-reflectivity-classes-v1",
        license_terms=fixed(
            "AEMET legal notice permits commercial and non-commercial reuse, "
            "reproduction, distribution and transformation with its duties."
        ),
        attribution=fixed(
            "Attribute AEMET, preserve supplied update/terms metadata, do not "
            "imply endorsement and do not distort the source meaning."
        ),
        retention_class=RetentionClass.ATTRIBUTION_REPLAY_EVIDENCE,
        raw_replay=RawReplay.CONDITIONAL,
        raw_capture_status=RawCaptureStatus.EXACT_BYTES_TRANSIENT,
        transport_gap=(
            "Replace the undocumented legacy web archive with a bounded exact "
            "three-member OpenData capture (envelope, datos, metadatos); pin "
            "grid/time/ESCALA/no-data/QC semantics and an official checksum "
            "before any quantitative or operational admission."
        ),
        primary_sources=(
            "https://www.aemet.es/en/eltiempo/observacion/radar/ayuda",
            "https://opendata.aemet.es/AEMET_OpenData_specification.json",
            "https://www.aemet.es/es/nota_legal",
        ),
    ),
    _contract(
        provider="met_office_radar",
        registry_role="radar",
        capability_status=CapabilityStatus.PRECIPITATION_RASTER,
        product="Met Office UK radar ODIM-HDF5 object",
        quantity="radar precipitation field; exact ODIM quantity must be read and validated",
        upstream_authority="UK Met Office",
        authoritative_artifact="selected public S3 .h5 object",
        capture_boundary="exact S3 object bytes before worker ODIM-HDF5 decode",
        record_id_strategy="hash S3 object key/ODIM time/payload digest; never store a signed URL",
        record_id_components=("bucket-relative object key", "ODIM product time", "payload SHA-256"),
        allowed_media_types=("application/x-hdf5", "application/octet-stream"),
        nominal_timestamp_source=artifact_metadata("dataset1/what enddate + endtime"),
        native_crs=artifact_metadata("/where/projdef"),
        unit_semantics=artifact_metadata("dataset1/data1/what quantity"),
        gain_offset_semantics=artifact_metadata("dataset1/data1/what gain + offset"),
        nodata_semantics=artifact_metadata("dataset1/data1/what nodata"),
        undetect_semantics=artifact_metadata("dataset1/data1/what undetect"),
        qc_semantics=unknown("Current generic worker parser does not consume a separate QC layer."),
        native_resolution=artifact_metadata("/where xscale + yscale", "Provider advertises a 1 km composite."),
        native_dimensions=artifact_metadata("/where xsize + ysize and data shape"),
        parser_responsibility="worker.radar.parse_odim_rainfall",
        parser_version="worker-shared-odim-current",
        license_terms=unknown("The S3 feed's redistribution and raw-retention terms need explicit confirmation."),
        attribution=fixed("Met Office"),
        retention_class=RetentionClass.CONTRACT_REVIEW_HOLD,
        raw_replay=RawReplay.FULL,
        raw_capture_status=RawCaptureStatus.WORKER_DERIVED_ARTIFACT_ONLY,
        transport_gap="Worker must expose exact S3 object bytes/key; current HA artifact is derived gzip-JSON only.",
        primary_sources=(
            "https://met-office-radar-obs-data.s3.eu-west-2.amazonaws.com/",
            "https://www.metoffice.gov.uk/services/data/datapoint",
        ),
    ),
    _contract(
        provider="opera",
        registry_role="radar",
        capability_status=CapabilityStatus.PRECIPITATION_RASTER,
        product="EUMETNET OPERA European composite DBZH",
        quantity="maximum composite radar reflectivity in dBZ plus quality field",
        upstream_authority="EUMETNET OPERA",
        authoritative_artifact="selected OPERA@YYYYMMDDTHHMM@0@DBZH.h5 S3 object",
        capture_boundary="exact HDF5 object bytes before regional crop or HDF5 decode",
        record_id_strategy="hash bucket-relative object key/ODIM time/payload digest",
        record_id_components=("S3 object key", "/what date+time", "payload SHA-256"),
        allowed_media_types=("application/x-hdf5", "application/octet-stream"),
        nominal_timestamp_source=artifact_metadata("/what date + time"),
        native_crs=artifact_metadata("/where/projdef"),
        unit_semantics=artifact_metadata("dataset1/data1/what quantity=DBZH and gain/offset to dBZ"),
        gain_offset_semantics=artifact_metadata("dataset1/data1/what gain + offset"),
        nodata_semantics=artifact_metadata("dataset1/data1/what nodata"),
        undetect_semantics=artifact_metadata("dataset1/data1/what undetect"),
        qc_semantics=artifact_metadata("dataset1/data1/quality1 data and its what/nodata metadata"),
        native_resolution=artifact_metadata("/where xscale + yscale"),
        native_dimensions=artifact_metadata("/where xsize + ysize and dataset shape"),
        parser_responsibility="worker.opera.parse_opera_slice / providers.opera parser",
        parser_version="opera-odim-dbzh-current",
        license_terms=unknown("OpenRadar object access is public; redistribution/raw-retention terms remain unrecorded."),
        attribution=fixed("EUMETNET OPERA"),
        retention_class=RetentionClass.CONTRACT_REVIEW_HOLD,
        raw_replay=RawReplay.FULL,
        raw_capture_status=RawCaptureStatus.WORKER_DERIVED_ARTIFACT_ONLY,
        transport_gap="Expose exact CloudFerro S3 object bytes/key before worker crop; derived JSON is not raw replay.",
        primary_sources=(
            "https://www.eumetnet.eu/observations/",
            "https://www.eumetnet.eu/wp-content/uploads/2024/10/ODIM_H5_v2.4.1_final.pdf",
            "https://s3.waw3-1.cloudferro.com/",
        ),
    ),
    _contract(
        provider="rainviewer",
        registry_role="radar fallback/aggregator",
        capability_status=CapabilityStatus.PRECIPITATION_RASTER,
        product="RainViewer Weather Maps v2 past radar tiles, Universal Blue palette",
        quantity="rendered composite reflectivity palette; low values inferred partly from alpha",
        upstream_authority="RainViewer",
        authoritative_artifact="weather-maps manifest plus the complete canonical set of requested PNG tiles",
        capture_boundary="exact manifest body and every tile body before JSON/Pillow decode",
        record_id_strategy="hash manifest frame path/time and ordered z/x/y/style/tile digests; omit host/query",
        record_id_components=("manifest frame path", "frame time", "z/x/y/style", "ordered payload SHA-256 values"),
        allowed_media_types=("application/json", "image/png"),
        nominal_timestamp_source=artifact_metadata(
            "manifest radar.past[].time",
            "RainViewer documents this as frame generation time, not necessarily each contributing radar scan time.",
        ),
        native_crs=fixed("Web Mercator XYZ tile matrix; decoded output uses tile bounds"),
        unit_semantics=fixed("Universal Blue rendered palette mapped heuristically to classes 0..8"),
        gain_offset_semantics=fixed("not applicable to rendered palette"),
        nodata_semantics=fixed("transparent/low-alpha and implausible grey pixels are parser-empty"),
        undetect_semantics=unknown("No distinct undetect value is present in the rendered tile."),
        qc_semantics=unknown("No per-pixel source/QC layer is supplied by the tile API."),
        native_resolution=fixed("zoom 5, 256-pixel XYZ tiles in current canonical request"),
        native_dimensions=fixed("256x256 pixels per tile; tile-set extent is request dependent"),
        parser_responsibility="providers.rainviewer.RainViewerProvider",
        parser_version="rainviewer-weather-maps-v2-universal-blue-current",
        license_terms=fixed("Public API is limited to personal, educational and small community use; no SLA."),
        attribution=fixed("Weather data by RainViewer with link to rainviewer.com"),
        retention_class=RetentionClass.CONTRACT_REVIEW_HOLD,
        raw_replay=RawReplay.CONDITIONAL,
        raw_capture_status=RawCaptureStatus.MULTI_ARTIFACT_TRANSIENT,
        transport_gap="Capture manifest bytes and all tile bytes atomically as one artifact set with missing-tile receipts.",
        primary_sources=(
            "https://www.rainviewer.com/api.html",
            "https://www.rainviewer.com/api/weather-maps-api.html",
            "https://www.rainviewer.com/terms.html",
        ),
    ),
    _contract(
        provider="noaa_enterprise_rainrate",
        registry_role="radar registry entry; satellite precipitation estimate",
        capability_status=CapabilityStatus.PRECIPITATION_RASTER,
        product="NESDIS Enterprise Rain Rate global blend RainRate-Blend-INST",
        quantity="instantaneous satellite precipitation rate (RRQPE) with DQF",
        upstream_authority="NOAA/NESDIS",
        authoritative_artifact="selected noaa-enterprise-rainrate-pds NetCDF-4 object",
        capture_boundary="exact S3 object bytes before NetCDF/HDF5 decode or regional windowing",
        record_id_strategy="hash bucket-relative key/time_coverage_start/payload digest",
        record_id_components=("S3 object key", "time_coverage_start", "payload SHA-256"),
        allowed_media_types=("application/x-netcdf", "application/octet-stream"),
        nominal_timestamp_source=artifact_metadata("global attribute time_coverage_start"),
        native_crs=fixed("regular latitude/longitude grid from geospatial_* attributes"),
        unit_semantics=artifact_metadata("RRQPE variable units"),
        gain_offset_semantics=artifact_metadata("RRQPE scale_factor + add_offset"),
        nodata_semantics=artifact_metadata("RRQPE _FillValue"),
        undetect_semantics=fixed("not separately represented; valid sub-threshold values remain below rain threshold"),
        qc_semantics=artifact_metadata("DQF; current parser accepts DQF=0 only"),
        native_resolution=artifact_metadata("geospatial_lat_resolution + geospatial_lon_resolution", "NOAA describes 2 km files."),
        native_dimensions=artifact_metadata("RRQPE variable shape"),
        parser_responsibility="worker.enterprise_rainrate.parse_enterprise_rainrate",
        parser_version="noaa-enterr-global-netcdf-current",
        license_terms=fixed("NOAA U.S. Government public environmental data; preserve NOAA/NESDIS attribution"),
        attribution=fixed("NOAA/NESDIS"),
        retention_class=RetentionClass.PUBLIC_REPLAY_EVIDENCE,
        raw_replay=RawReplay.FULL,
        raw_capture_status=RawCaptureStatus.WORKER_DERIVED_ARTIFACT_ONLY,
        transport_gap="Expose exact NetCDF object/key before worker parsing; current artifact contains derived JSON only.",
        primary_sources=(
            "https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C01710",
            "https://noaa-enterprise-rainrate-pds.s3.amazonaws.com/",
        ),
    ),
    _contract(
        provider="noaa_goes_rrqpe",
        registry_role="radar registry entry; satellite precipitation estimate",
        capability_status=CapabilityStatus.PRECIPITATION_RASTER,
        product="GOES ABI-L2-RRQPEF full-disk products from GOES-18/19",
        quantity="instantaneous rainfall rate with DQF",
        upstream_authority="NOAA/NESDIS",
        authoritative_artifact="selected ABI-L2-RRQPEF NetCDF-4 S3 object per satellite",
        capture_boundary="exact S3 object bytes before NetCDF/HDF5 decode or area crop",
        record_id_strategy="hash satellite/bucket-relative key/time_coverage_start/payload digest",
        record_id_components=("satellite number", "S3 object key", "time_coverage_start", "payload SHA-256"),
        allowed_media_types=("application/x-netcdf", "application/octet-stream"),
        nominal_timestamp_source=artifact_metadata("global attribute time_coverage_start"),
        native_crs=artifact_metadata("goes_imager_projection CF attributes"),
        unit_semantics=artifact_metadata("RRQPE variable units"),
        gain_offset_semantics=artifact_metadata("RRQPE scale_factor + add_offset"),
        nodata_semantics=artifact_metadata("RRQPE _FillValue"),
        undetect_semantics=fixed("not separately represented; below-threshold rate stays non-wet"),
        qc_semantics=artifact_metadata("DQF; current parser accepts DQF=0 only"),
        native_resolution=artifact_metadata("x/y coordinate arrays and geostationary projection height"),
        native_dimensions=artifact_metadata("RRQPE variable shape"),
        parser_responsibility="providers.noaa_goes_rrqpe.parse_rrqpe_netcdf",
        parser_version="goes-abi-l2-rrqpef-current",
        license_terms=fixed("NOAA U.S. Government public environmental data; preserve NOAA/NESDIS attribution"),
        attribution=fixed("NOAA/NESDIS"),
        retention_class=RetentionClass.PUBLIC_REPLAY_EVIDENCE,
        raw_replay=RawReplay.FULL,
        raw_capture_status=RawCaptureStatus.EXACT_BYTES_TRANSIENT,
        transport_gap="Persist each satellite NetCDF object/key before decode; retain failed-satellite receipts too.",
        primary_sources=(
            "https://registry.opendata.aws/noaa-goes/",
            "https://www.star.nesdis.noaa.gov/goesr/product_rain.php",
        ),
    ),
    _contract(
        provider="hsaf_h40b",
        registry_role="radar registry entry; satellite precipitation fallback",
        capability_status=CapabilityStatus.PRECIPITATION_RASTER,
        product="EUMETSAT H SAF H40B full-disk near-real-time precipitation",
        quantity="instantaneous rain rate rr with quality index qind",
        upstream_authority="EUMETSAT H SAF / Italian Meteorological Service",
        authoritative_artifact="selected h40_YYYYMMDD_HHMM_fdk.nc.gz FTP file",
        capture_boundary="exact compressed FTP file bytes before gzip/NetCDF decode",
        record_id_strategy="hash remote filename/time/payload digest; omit FTP credentials",
        record_id_components=("remote basename", "time encoded in basename", "payload SHA-256"),
        allowed_media_types=("application/gzip", "application/x-gzip", "application/octet-stream"),
        nominal_timestamp_source=artifact_metadata("remote filename h40_YYYYMMDD_HHMM_fdk.nc.gz"),
        native_crs=artifact_metadata("geostationary_projection CF attributes"),
        unit_semantics=artifact_metadata("rr variable units"),
        gain_offset_semantics=artifact_metadata("rr scale_factor + add_offset"),
        nodata_semantics=artifact_metadata("rr missing_value"),
        undetect_semantics=fixed("not separately represented; below-threshold rate stays non-wet"),
        qc_semantics=artifact_metadata("qind; current parser requires at least 15 percent"),
        native_resolution=artifact_metadata("nx/ny coordinate-axis spacing"),
        native_dimensions=artifact_metadata("rr variable shape"),
        parser_responsibility="providers.hsaf_h40b.parse_h40b_netcdf",
        parser_version="hsaf-h40b-netcdf-current",
        license_terms=unknown("Account/product licence and raw-retention terms must be recorded before persistence."),
        attribution=fixed("EUMETSAT H SAF"),
        retention_class=RetentionClass.CONTRACT_REVIEW_HOLD,
        raw_replay=RawReplay.FULL,
        raw_capture_status=RawCaptureStatus.EXACT_BYTES_TRANSIENT,
        transport_gap="Persist compressed bytes and remote basename before decompression; never persist FTP credentials.",
        primary_sources=(
            "https://hsaf.meteoam.it/",
            "https://ftphsaf.meteoam.it/",
        ),
    ),
    _contract(
        provider="netatmo",
        registry_role="ground",
        capability_status=CapabilityStatus.GROUND_OBSERVATIONS,
        product="Netatmo getpublicdata station/module measurements",
        quantity="rain_live mm/h and rain_60min accumulation plus ancillary station variables",
        upstream_authority="Netatmo",
        authoritative_artifact="complete getpublicdata JSON response for the canonical geographic query",
        capture_boundary="exact HTTP body before streaming JSON decode and per-module aggregation",
        record_id_strategy="hash station/module/measurement-time/fact-kind/payload digest; omit bearer and query URL",
        record_id_components=("station id", "module id", "measurement timestamp", "fact kind", "payload SHA-256"),
        allowed_media_types=("application/json",),
        nominal_timestamp_source=artifact_metadata("module res timestamp / res_latest fallback"),
        native_crs=fixed("WGS84-style latitude/longitude station coordinates"),
        unit_semantics=fixed("rain_live mm/h; rain_60min mm over preceding 60 minutes"),
        gain_offset_semantics=fixed("not applicable"),
        nodata_semantics=fixed("missing module field remains unknown; it is never numeric zero evidence"),
        undetect_semantics=fixed("not applicable to gauge facts"),
        qc_semantics=unknown("Public personal stations expose no common authoritative QC flag."),
        native_resolution=fixed("point station/module observation"),
        native_dimensions=fixed("not applicable"),
        parser_responsibility="worker.netatmo._parse_observations + engine.ground_observation_adapters",
        parser_version="netatmo-module-facts-v2-time-quality",
        license_terms=unknown("getpublicdata storage, redistribution and raw-retention terms require API-plan review."),
        attribution=fixed("Netatmo"),
        retention_class=RetentionClass.CONTRACT_REVIEW_HOLD,
        raw_replay=RawReplay.FULL,
        raw_capture_status=RawCaptureStatus.EXACT_BYTES_TRANSIENT,
        transport_gap="Persist bounded exact response bytes, SHA-256 and secret-free receipt context; redact bearer and OAuth data.",
        primary_sources=(
            "https://dev.netatmo.com/apidocumentation/weather#getpublicdata",
            "https://api.netatmo.com/api/getpublicdata",
        ),
    ),
    _contract(
        provider="synoptic",
        registry_role="ground",
        capability_status=CapabilityStatus.GROUND_OBSERVATIONS,
        product="Synoptic Time Series with precip=1 and full requested QC",
        quantity="derived precipitation interval amounts in mm plus ancillary station variables",
        upstream_authority="Synoptic Data PBC and contributing station networks",
        authoritative_artifact="complete Time Series JSON response for the canonical station-radius query",
        capture_boundary="exact HTTP body before UTF-8/JSON decode",
        record_id_strategy="hash STID/sensor-set/interval bounds/sample index/payload digest; omit API credential",
        record_id_components=("STID", "sensor set", "interval start/end", "sample index", "payload SHA-256"),
        allowed_media_types=("application/json",),
        nominal_timestamp_source=artifact_metadata("OBSERVATIONS.date_time interval endpoints"),
        native_crs=fixed("station latitude/longitude"),
        unit_semantics=fixed("precip_intervals in requested metric mm; never an instantaneous rate"),
        gain_offset_semantics=fixed("not applicable"),
        nodata_semantics=fixed("missing/incomplete interval remains unknown and cannot confirm dry"),
        undetect_semantics=fixed("not applicable to interval gauge facts"),
        qc_semantics=artifact_metadata(
            "QC_FLAGGED/QC_SUMMARY/SENSOR_VARIABLES and API SUMMARY",
            "Request enables Synoptic Labs QC and removes failed values.",
        ),
        native_resolution=fixed("point station observation with source-dependent reporting interval"),
        native_dimensions=fixed("not applicable"),
        parser_responsibility="worker.synoptic._precip_intervals/_observations + ground adapter",
        parser_version="synoptic_interval_v4",
        license_terms=unknown("API subscription and each contributing network may impose retention/attribution terms."),
        attribution=fixed("Synoptic Data and originating network/station where supplied"),
        retention_class=RetentionClass.CONTRACT_REVIEW_HOLD,
        raw_replay=RawReplay.FULL,
        raw_capture_status=RawCaptureStatus.EXACT_BYTES_TRANSIENT,
        transport_gap="Persist bounded exact response bytes, SHA-256 and secret-free receipt context; strip credential query from all metadata.",
        primary_sources=(
            "https://docs.synopticdata.com/services/time-series",
            "https://docs.synopticdata.com/services/precipitation-service-explained",
            "https://docs.synopticdata.com/services/qc",
        ),
    ),
    _contract(
        provider="metar",
        registry_role="ground",
        capability_status=CapabilityStatus.GROUND_OBSERVATIONS,
        product="AviationWeather current METAR CSV cache",
        quantity="coded point present-weather report plus separate Prrrr precipitation accumulation remark",
        upstream_authority="NOAA/NWS Aviation Weather Center and originating METAR networks",
        authoritative_artifact="complete metars.cache.csv.gz response",
        capture_boundary="exact gzip response bytes before decompression/CSV/TAC parsing",
        record_id_strategy="hash ICAO/report observation time/raw TAC digest/cache payload digest",
        record_id_components=("ICAO", "observation time", "raw_text SHA-256", "cache SHA-256"),
        allowed_media_types=("application/gzip", "application/x-gzip", "application/octet-stream"),
        nominal_timestamp_source=artifact_metadata("CSV observation time field and coded report time"),
        native_crs=fixed("station latitude/longitude from cache"),
        unit_semantics=fixed(
            "present weather is categorical local point state; Prrrr is inches since prior report converted to mm"
        ),
        gain_offset_semantics=fixed("Prrrr inches multiplied by 25.4; no other scale/offset"),
        nodata_semantics=fixed("absent capability/report group remains unknown; it is not dry evidence"),
        undetect_semantics=fixed("not applicable"),
        qc_semantics=artifact_metadata(
            "TAC capability/maintenance flags such as AO1/AO2, PWINO, PNO, FZRANO, TSNO and $"
        ),
        native_resolution=fixed("airport/station point observation"),
        native_dimensions=fixed("not applicable"),
        parser_responsibility="worker.metar._decode + engine.ground_observation_adapters",
        parser_version="metar_wx_v3",
        license_terms=fixed("NOAA/NWS public data access; preserve source and originating station identity"),
        attribution=fixed("NOAA/NWS Aviation Weather Center and originating METAR station"),
        retention_class=RetentionClass.PUBLIC_REPLAY_EVIDENCE,
        raw_replay=RawReplay.FULL,
        raw_capture_status=RawCaptureStatus.EXACT_BYTES_TRANSIENT,
        transport_gap="Persist the exact gzip cache body and receipt headers before _decode; current cache is in-memory parsed facts.",
        primary_sources=(
            "https://aviationweather.gov/data/api/",
            "https://aviationweather.gov/help/data/",
            "https://aviationweather.gov/data/cache/metars.cache.csv.gz",
        ),
    ),
)


_contracts = {contract.provider: contract for contract in _CONTRACT_SEQUENCE}
if len(_contracts) != len(_CONTRACT_SEQUENCE):
    raise RuntimeError("duplicate provider artifact contract")
CONTRACTS: Mapping[str, ProviderArtifactContract] = MappingProxyType(_contracts)


def validate_registry_complete(implemented_provider_ids: set[str] | frozenset[str]) -> None:
    """Fail closed when code and contract registries diverge."""

    expected = set(implemented_provider_ids)
    actual = set(CONTRACTS)
    if actual != expected:
        raise ValueError(
            "provider artifact contract mismatch: "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )


def require_operational_contract(provider: str) -> ProviderArtifactContract:
    """Return a contract only when implementation and registry capability agree."""

    try:
        contract = CONTRACTS[provider]
    except KeyError as err:
        raise KeyError(f"unknown provider {provider!r}") from err
    if not contract.operational_eligible:
        raise ValueError(
            f"{provider} is fail-closed: implemented capability does not match registry"
        )
    return contract


def contract_snapshot() -> dict[str, dict[str, Any]]:
    """Return a deterministic, JSON-serialisable registry snapshot."""

    return {
        provider: CONTRACTS[provider].to_dict()
        for provider in sorted(CONTRACTS)
    }


def contract_registry_sha256() -> str:
    payload = json.dumps(
        contract_snapshot(),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def payload_sha256(payload: bytes) -> str:
    if not isinstance(payload, bytes):
        raise TypeError("payload must be exact bytes")
    return hashlib.sha256(payload).hexdigest()


def derive_record_id(
    provider: str,
    *,
    source_record_id: str | None = None,
    nominal_timestamp: str | int | float | None = None,
    payload_digest: str | None = None,
) -> str:
    """Build a stable secret-free record ID from canonical upstream facts.

    ``source_record_id`` must be a bucket-relative key, archive-member name,
    station/report identity or equivalent opaque value—not a URL.  A payload
    digest is required whenever no stable upstream record identity exists.
    """

    if provider not in CONTRACTS:
        raise KeyError(f"unknown provider {provider!r}")
    if source_record_id is None and payload_digest is None:
        raise ValueError("source_record_id or payload_digest is required")
    if payload_digest is not None and not re.fullmatch(r"[0-9a-f]{64}", payload_digest):
        raise ValueError("payload_digest must be a lowercase SHA-256 hex digest")
    material = {
        "schema": SCHEMA_VERSION,
        "provider": provider,
        "source_record_id": source_record_id,
        "nominal_timestamp": nominal_timestamp,
        "payload_sha256": payload_digest,
    }
    validate_redacted(material, record_identity=True)
    canonical = json.dumps(
        material,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"{provider}:{hashlib.sha256(canonical).hexdigest()}"


__all__ = [
    "ARTIFACT_METADATA",
    "CapabilityStatus",
    "CONTRACTS",
    "Fact",
    "FactKind",
    "ProviderArtifactContract",
    "RawCaptureStatus",
    "RawReplay",
    "RedactionError",
    "RetentionClass",
    "SCHEMA_VERSION",
    "SHADOW_ONLY",
    "UNKNOWN",
    "artifact_metadata",
    "contract_registry_sha256",
    "contract_snapshot",
    "derive_record_id",
    "fixed",
    "payload_sha256",
    "require_operational_contract",
    "unknown",
    "validate_redacted",
    "validate_registry_complete",
]
