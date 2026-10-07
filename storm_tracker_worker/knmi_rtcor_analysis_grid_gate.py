"""Fail-closed KNMI RTCOR/NL25 analysis-grid geometry authority gate.

This module is deliberately worker-only and unwired.  It does not decode HDF5,
does not read a raster and, most importantly, cannot issue an
``AffineGridTransformCertificate``.  Its purpose is to retain the difference
between values that are printed in an official document and the stronger proof
needed to treat those values as exact pixel-edge coordinates for a real
provider field.

The current official sources disagree on the ellipsoid, the live HDF payload is
tagged as HDF 3.6 while the numeric NL25 appendix is published in the HDF 3.8
report, and the public documents do not formally expand ``LU`` into the exact
pixel-edge convention required by ``analysis_grid_transform``.  Returning a
candidate geometry from these values would therefore turn an inference into an
official certificate.  This gate refuses to do that.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any, Mapping, Sequence


KNMI_RTCOR_GRID_GATE_SCHEMA = "stv3-knmi-rtcor-analysis-grid-authority-gate-v1"
KNMI_RTCOR_PROVIDER = "knmi"
KNMI_RTCOR_PRODUCT = "knmi_nl_rdr_data_rtcor_5m_capture"
KNMI_RTCOR_DATASET = "nl_rdr_data_rtcor_5m/1.0"
KNMI_RTCOR_INTERNAL_PRODUCT = "RAD_NL25_COR_NA"

KNMI_DATASET_CATALOG_URL = (
    "https://dataplatform.knmi.nl/dataset/nl-rdr-data-rtcor-5m-1-0"
)
KNMI_HDF36_URL = (
    "https://cdn.knmi.nl/knmi/pdf/bibliotheek/knmipubIR/IR2009-01.pdf"
)
KNMI_HDF38_URL = (
    "https://cdn.knmi.nl/system/data_center_publications/files/000/072/301/"
    "original/hdftag38.pdf?1739444428="
)
KNMI_PRODUCT_PAPER_URL = (
    "https://essd.copernicus.org/articles/17/4715/2025/"
)

MAX_MANIFEST_BYTES = 256 * 1024
MAX_EVIDENCE_RECORDS = 32
MAX_FACTS_PER_RECORD = 64
MAX_TEXT_CHARS = 2048

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_CODE_RE = re.compile(r"^[a-z][a-z0-9_:-]{0,127}$")


class KnmiRtcorGridGateError(RuntimeError):
    """Base failure for an invalid authority manifest or issue request."""


class KnmiRtcorGridCertificateWithheldError(KnmiRtcorGridGateError):
    """Raised when a caller attempts to turn the open gate into a certificate."""


class KnmiRtcorGridGateState(str, Enum):
    WITHHELD = "withheld"


@dataclass(frozen=True, slots=True)
class KnmiRtcorGridGateBlocker:
    code: str
    detail: str
    required_evidence: str

    def __post_init__(self) -> None:
        if not isinstance(self.code, str) or _SAFE_CODE_RE.fullmatch(self.code) is None:
            raise ValueError("blocker code is invalid")
        for label, value in (
            ("detail", self.detail),
            ("required_evidence", self.required_evidence),
        ):
            if not isinstance(value, str) or not value or len(value) > MAX_TEXT_CHARS:
                raise ValueError(f"blocker {label} is invalid")

    def to_mapping(self) -> dict[str, str]:
        return {
            "code": self.code,
            "detail": self.detail,
            "required_evidence": self.required_evidence,
        }


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _bounded_plain_json(value: Any, *, depth: int = 0) -> Any:
    """Copy an authority manifest into a bounded JSON-only value graph."""

    if depth > 12:
        raise ValueError("authority manifest is too deeply nested")
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        if not value or len(value) > MAX_TEXT_CHARS or any(ord(ch) < 32 for ch in value):
            raise ValueError("authority manifest contains invalid text")
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        if abs(value) > 10**12:
            raise ValueError("authority manifest integer is outside its bound")
        return value
    if isinstance(value, float):
        if not math.isfinite(value) or abs(value) > 10**12:
            raise ValueError("authority manifest number is outside its bound")
        return value
    if isinstance(value, Mapping):
        if len(value) > 128:
            raise ValueError("authority manifest object is too large")
        result: dict[str, Any] = {}
        for key, item in value.items():
            if (
                not isinstance(key, str)
                or not key
                or len(key) > 128
                or any(ord(ch) < 32 for ch in key)
                or key in result
            ):
                raise ValueError("authority manifest key is invalid")
            result[key] = _bounded_plain_json(item, depth=depth + 1)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        if len(value) > 256:
            raise ValueError("authority manifest sequence is too large")
        return [_bounded_plain_json(item, depth=depth + 1) for item in value]
    raise TypeError("authority manifest must contain JSON values only")


def _manifest_copy(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("authority manifest must be a mapping")
    copied = _bounded_plain_json(value)
    if not isinstance(copied, dict):
        raise TypeError("authority manifest must be an object")
    encoded = _canonical_json(copied)
    if len(encoded) > MAX_MANIFEST_BYTES:
        raise ValueError("authority manifest exceeds its byte bound")
    return copied


def _source_index(manifest: Mapping[str, Any]) -> tuple[dict[str, Mapping[str, Any]], list[str]]:
    records = manifest.get("evidence_records")
    if not isinstance(records, list) or not 1 <= len(records) <= MAX_EVIDENCE_RECORDS:
        return {}, ["evidence_records_missing_or_invalid"]
    indexed: dict[str, Mapping[str, Any]] = {}
    problems: list[str] = []
    for record in records:
        if not isinstance(record, Mapping):
            problems.append("evidence_record_not_object")
            continue
        record_id = record.get("record_id")
        evidence_class = record.get("evidence_class")
        source_url = record.get("source_url")
        facts = record.get("facts")
        if (
            not isinstance(record_id, str)
            or _SAFE_CODE_RE.fullmatch(record_id) is None
            or record_id in indexed
        ):
            problems.append("evidence_record_identity_invalid_or_duplicate")
            continue
        if evidence_class not in {
            "official_knmi_dataset_catalog",
            "official_knmi_hdf_specification",
            "official_peer_reviewed_product_description",
            "local_unretained_live_observation",
            "synthetic_fixture",
        }:
            problems.append("evidence_class_unknown")
        if (
            not isinstance(source_url, str)
            or not source_url.startswith("https://")
            or len(source_url) > 1024
        ):
            problems.append("evidence_source_url_invalid")
        if not isinstance(facts, Mapping) or not 1 <= len(facts) <= MAX_FACTS_PER_RECORD:
            problems.append("evidence_facts_missing_or_invalid")
        indexed[record_id] = record
    return indexed, problems


_BASE_BLOCKERS = (
    KnmiRtcorGridGateBlocker(
        code="official_crs_authorities_conflict",
        detail=(
            "KNMI dataset edition 4 publishes semi-axes 6378.14/6356.75 km, "
            "while the KNMI HDF 3.8 NL25 profile and the unretained live-file "
            "observation use 6378.388/6356.906 km. The difference is material "
            "and cannot be selected by inference."
        ),
        required_evidence=(
            "A product-specific KNMI correction or versioned statement that "
            "declares which CRS is authoritative for nl_rdr_data_rtcor_5m/1.0."
        ),
    ),
    KnmiRtcorGridGateBlocker(
        code="pixel_lu_edge_semantics_not_formally_defined",
        detail=(
            "The KNMI HDF reports say geo_pixel_def describes a position inside "
            "a pixel and show the token LU, but they do not formally define LU "
            "as the exact outer left/upper pixel edge required by the affine "
            "analysis-grid certificate."
        ),
        required_evidence=(
            "An official KNMI definition or product example that maps LU, "
            "geo_column_offset and geo_row_offset to the exact pixel-edge "
            "coordinate of array element [0,0]."
        ),
    ),
    KnmiRtcorGridGateBlocker(
        code="hdf36_payload_nl25_profile_version_gap",
        detail=(
            "The observed RTCOR payload declares hdftag 3.6, but the exact NL25 "
            "numeric appendix currently available from KNMI is in the 3.8 "
            "report. HDF 3.6 defines the metadata fields but does not publish "
            "the later NL25 numeric profile."
        ),
        required_evidence=(
            "A product-specific KNMI statement binding the RTCOR 3.6-tagged "
            "payload to the published NL25 numeric profile, or retained current "
            "official HDF bytes whose complete geographic group is reverified."
        ),
    ),
    KnmiRtcorGridGateBlocker(
        code="official_live_hdf_grid_evidence_not_retained",
        detail=(
            "Only a filename, size and SHA-256 of one live KNMI HDF observation "
            "are recorded locally; the exact official bytes and a bounded "
            "geographic metadata receipt are not part of the validation corpus."
        ),
        required_evidence=(
            "Retain exact schema-3 official HDF bytes plus a content-bound "
            "receipt for geographic/map_projection and image orientation tags."
        ),
    ),
    KnmiRtcorGridGateBlocker(
        code="array_display_origin_not_decoder_bound",
        detail=(
            "The quantitative decoder binds rows, columns, X/Y, offsets and LU, "
            "but does not bind DISPLAY_ORIGIN for each semantic image dataset. "
            "Therefore array index orientation is not independently closed at "
            "the transform boundary."
        ),
        required_evidence=(
            "Bind absent/default-upper-left or explicit UL DISPLAY_ORIGIN for "
            "image1/image2/image3 and reject any other orientation before a "
            "grid certificate can match the decoded candidate."
        ),
    ),
    KnmiRtcorGridGateBlocker(
        code="kilometre_to_metre_crs_equivalence_not_certified",
        detail=(
            "KNMI encodes projection coordinates and ellipsoid axes in KM. The "
            "analysis-grid transform requires an exact target CRS with metre "
            "coordinates. Multiplying values by 1000 is arithmetically clear, "
            "but no reviewed certificate currently binds a canonical metre-CRS "
            "identifier to the KNMI kilometre definition."
        ),
        required_evidence=(
            "A reviewed canonical CRS normalization and round-trip golden that "
            "binds the KNMI kilometre definition to one exact metre-coordinate "
            "target CRS without changing the ellipsoid or axis order."
        ),
    ),
)


_EXPECTED_RECORD_PROFILES: Mapping[str, Mapping[str, Any]] = MappingProxyType({
    "knmi_dataset_catalog_edition_4": MappingProxyType({
        "evidence_class": "official_knmi_dataset_catalog",
        "source_url": KNMI_DATASET_CATALOG_URL,
        "facts": MappingProxyType({
            "dataset_edition": 4,
            "file_format": "HDF5",
            "reference_system": (
                "+proj=stere +lat_0=90 +lon_0=0 +lat_ts=60 "
                "+a=6378.14 +b=6356.75 +x_0=0 y_0=0"
            ),
            "spatial_representation": "grid",
        }),
    }),
    "knmi_hdf_spec_v3_6": MappingProxyType({
        "evidence_class": "official_knmi_hdf_specification",
        "source_url": KNMI_HDF36_URL,
        "facts": MappingProxyType({
            "display_origin_default": (
                "pixel_0_0_is_upper_left_unless_unusual_orientation_is_declared"
            ),
            "geo_column_offset_semantics": (
                "offset_of_pixel_0_0_from_projection_origin"
            ),
            "image_array_order": "Nrows_x_Ncolumns",
            "report_version": "3.6",
            "self_contained_metadata": True,
        }),
    }),
    "knmi_hdf_spec_v3_8_nl25": MappingProxyType({
        "evidence_class": "official_knmi_hdf_specification",
        "source_url": KNMI_HDF38_URL,
        "facts": MappingProxyType({
            "column_offset_km": 0.0,
            "columns": 700,
            "coordinate_dimensions": "KM,KM",
            "earth_model_axes_km": [6378.388, 6356.906],
            "pixel_definition_token": "LU",
            "pixel_parameters": "X,Y",
            "pixel_size_x_km": 1.0000035,
            "pixel_size_y_km": -1.0000049,
            "projection": "STEREOGRAPHIC",
            "report_version": "3.8",
            "row_offset_km": 3649.9814,
            "rows": 765,
        }),
    }),
    "knmi_essd_2025_product": MappingProxyType({
        "evidence_class": "official_peer_reviewed_product_description",
        "source_url": KNMI_PRODUCT_PAPER_URL,
        "facts": MappingProxyType({
            "grid_area": "approximately_1_km2",
            "hdf_contains_geographical_metadata": True,
            "product_semantics": (
                "gauge_adjusted_five_minute_precipitation_accumulation"
            ),
            "publication_year": 2025,
        }),
    }),
    "local_live_hdf_observation_20260801": MappingProxyType({
        "evidence_class": "local_unretained_live_observation",
        "source_url": (
            "https://api.dataplatform.knmi.nl/open-data/v1/datasets/"
            "nl_rdr_data_rtcor_5m/versions/1.0/files"
        ),
        "facts": MappingProxyType({
            "content_sha256": (
                "a138f091fb61181d2f83e5892e0ba5db19088211d3fc26ccff41bb90b9927d8d"
            ),
            "filename": "RAD_NL25_RAC_RT_202608012345.h5",
            "hdf_tag_version": "3.6",
            "official_bytes_retained_in_validation_corpus": False,
            "size_bytes": 39925,
        }),
    }),
})


@dataclass(frozen=True, slots=True)
class KnmiRtcorGridGateResult:
    """Machine-readable proof that the real-provider certificate is withheld."""

    state: KnmiRtcorGridGateState
    evidence_sha256: str
    blockers: tuple[KnmiRtcorGridGateBlocker, ...]
    informational_geometry: Mapping[str, Any]
    source_urls: tuple[str, ...]
    certificate: None = None
    official_certificate_eligible: bool = False

    def __post_init__(self) -> None:
        if self.state is not KnmiRtcorGridGateState.WITHHELD:
            raise ValueError("KNMI RTCOR grid gate can only be withheld")
        if _SHA256_RE.fullmatch(self.evidence_sha256) is None:
            raise ValueError("gate evidence SHA-256 is invalid")
        if not self.blockers or any(
            not isinstance(item, KnmiRtcorGridGateBlocker) for item in self.blockers
        ):
            raise ValueError("withheld grid gate needs explicit blockers")
        codes = [item.code for item in self.blockers]
        if len(codes) != len(set(codes)):
            raise ValueError("grid gate blockers are duplicated")
        if self.certificate is not None or self.official_certificate_eligible:
            raise ValueError("withheld gate cannot expose certificate authority")
        if (
            not isinstance(self.source_urls, tuple)
            or not self.source_urls
            or any(
                not isinstance(item, str)
                or not item.startswith("https://")
                or len(item) > 1024
                for item in self.source_urls
            )
        ):
            raise ValueError("grid gate source URLs are invalid")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "blockers": [item.to_mapping() for item in self.blockers],
            "certificate": None,
            "evidence_sha256": self.evidence_sha256,
            "informational_geometry": dict(self.informational_geometry),
            "official_certificate_eligible": False,
            "source_urls": list(self.source_urls),
            "state": self.state.value,
        }


def _informational_geometry() -> Mapping[str, Any]:
    """Known printed values, explicitly not an edge-map certificate."""

    return MappingProxyType({
        "authority": "informational_only_not_a_transform_certificate",
        "array_shape": (765, 700),
        "candidate_array_axis_order": "row_y_column_x",
        "candidate_column_direction": "west_to_east",
        "candidate_coordinate_unit": "km",
        "candidate_east_edge_if_lu_is_outer_edge_km": 700.00245,
        "candidate_north_anchor_km": 3649.9814,
        "candidate_pixel_definition_token": "LU",
        "candidate_pixel_size_x_km": 1.0000035,
        "candidate_pixel_size_y_km": -1.0000049,
        "candidate_row_direction": "north_to_south",
        "candidate_south_edge_if_lu_is_outer_edge_km": 2884.9776515,
        "candidate_west_anchor_km": 0.0,
        "catalog_crs_axes_km": (6378.14, 6356.75),
        "hdf38_nl25_crs_axes_km": (6378.388, 6356.906),
        "height": 765,
        "width": 700,
    })


def evaluate_knmi_rtcor_analysis_grid_gate(
    authority_manifest: Mapping[str, Any],
) -> KnmiRtcorGridGateResult:
    """Evaluate bounded evidence and always withhold the official certificate.

    The function is intentionally incapable of returning a certificate.  A
    future implementation must be a separately reviewed version after every
    blocker has received primary evidence; weakening this function is not an
    accepted way to open the gate.
    """

    manifest = _manifest_copy(authority_manifest)
    evidence_sha256 = hashlib.sha256(_canonical_json(manifest)).hexdigest()
    indexed, manifest_problems = _source_index(manifest)
    extra: list[KnmiRtcorGridGateBlocker] = []

    expected_top = {
        "dataset",
        "evidence_records",
        "internal_product",
        "product",
        "provider",
        "schema",
        "status",
    }
    if (
        set(manifest) != expected_top
        or manifest.get("schema") != KNMI_RTCOR_GRID_GATE_SCHEMA
        or manifest.get("provider") != KNMI_RTCOR_PROVIDER
        or manifest.get("product") != KNMI_RTCOR_PRODUCT
        or manifest.get("dataset") != KNMI_RTCOR_DATASET
        or manifest.get("internal_product") != KNMI_RTCOR_INTERNAL_PRODUCT
        or manifest.get("status") != "withheld"
    ):
        manifest_problems.append("authority_manifest_identity_mismatch")

    required_records = {
        "knmi_dataset_catalog_edition_4",
        "knmi_hdf_spec_v3_6",
        "knmi_hdf_spec_v3_8_nl25",
        "knmi_essd_2025_product",
        "local_live_hdf_observation_20260801",
    }
    missing = sorted(required_records - set(indexed))
    if missing:
        manifest_problems.append("required_evidence_records_missing:" + ",".join(missing))

    for record_id, expected in _EXPECTED_RECORD_PROFILES.items():
        actual = indexed.get(record_id)
        if actual is None:
            continue
        facts = actual.get("facts")
        expected_facts = expected["facts"]
        profile_matches = (
            actual.get("evidence_class") == expected["evidence_class"]
            and actual.get("source_url") == expected["source_url"]
            and isinstance(facts, Mapping)
            and all(facts.get(key) == value for key, value in expected_facts.items())
        )
        if not profile_matches:
            manifest_problems.append(
                "evidence_record_profile_mismatch:" + record_id
            )

    if any(
        record.get("evidence_class") == "synthetic_fixture"
        for record in indexed.values()
    ):
        manifest_problems.append("synthetic_evidence_cannot_open_official_gate")

    if manifest_problems:
        detail = "; ".join(sorted(set(manifest_problems)))
        extra.append(KnmiRtcorGridGateBlocker(
            code="authority_manifest_incomplete_or_untrusted",
            detail=(
                "The supplied evidence manifest cannot establish the fixed "
                f"official authority set: {detail}."
            )[:MAX_TEXT_CHARS],
            required_evidence=(
                "Provide the complete bounded authority manifest with only "
                "official KNMI/catalog/paper records and a separately labelled "
                "unretained observation; synthetic evidence is test-only."
            ),
        ))

    urls = tuple(sorted({
        KNMI_DATASET_CATALOG_URL,
        KNMI_HDF36_URL,
        KNMI_HDF38_URL,
        KNMI_PRODUCT_PAPER_URL,
    }))
    return KnmiRtcorGridGateResult(
        state=KnmiRtcorGridGateState.WITHHELD,
        evidence_sha256=evidence_sha256,
        blockers=_BASE_BLOCKERS + tuple(extra),
        informational_geometry=_informational_geometry(),
        source_urls=urls,
    )


def issue_knmi_rtcor_analysis_grid_certificate(
    authority_manifest: Mapping[str, Any],
) -> None:
    """Fail closed; no KNMI RTCOR transform certificate is currently issuable."""

    result = evaluate_knmi_rtcor_analysis_grid_gate(authority_manifest)
    codes = ",".join(item.code for item in result.blockers)
    raise KnmiRtcorGridCertificateWithheldError(
        f"KNMI RTCOR analysis-grid certificate withheld: {codes}"
    )


__all__ = [
    "KNMI_DATASET_CATALOG_URL",
    "KNMI_HDF36_URL",
    "KNMI_HDF38_URL",
    "KNMI_PRODUCT_PAPER_URL",
    "KNMI_RTCOR_GRID_GATE_SCHEMA",
    "KnmiRtcorGridCertificateWithheldError",
    "KnmiRtcorGridGateBlocker",
    "KnmiRtcorGridGateError",
    "KnmiRtcorGridGateResult",
    "KnmiRtcorGridGateState",
    "evaluate_knmi_rtcor_analysis_grid_gate",
    "issue_knmi_rtcor_analysis_grid_certificate",
]
