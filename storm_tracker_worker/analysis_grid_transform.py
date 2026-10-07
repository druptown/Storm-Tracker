"""Bounded, deterministic shadow transform onto an explicit analysis grid.

This module is deliberately worker-only and unwired.  It consumes only an
unadmitted :class:`NativeRainRateFieldCandidate` whose complete transform-
relevant content is bound by a caller-supplied SHA-256.  Coordinates are never
looked up or guessed: a separate transform adapter issues a receipt from an
explicit certificate.  The initial closed implementation accepts only
north-up, axis-aligned affine pixel-edge mappings.  It does not import pyproj,
GDAL, provider code, schedulers or Home Assistant code.

Rate is resampled as an area-overlap mean.  A target value is emitted only
when its complete area is covered by valid source pixels.  Nodata, undetect,
mixed and outside-support cells remain distinct and are always NaN in the
meteorological value array.  No threshold, wet/dry decision, extrapolation or
operational admission occurs here.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import re
import time
from typing import Any, Callable, Mapping, Protocol, runtime_checkable

import numpy as np

from .canonical_radar_field_bridge import (
    NativeRainRateFieldCandidate,
    NativeRasterPlane,
    RADAR_FIELD_ADMISSION_STATUS,
)
from .tracking_core.engine.analysis_grid import AnalysisGrid


ANALYSIS_GRID_TRANSFORM_ALGORITHM = "analysis-grid-transform-shadow-v1"
ANALYSIS_GRID_TRANSFORM_ADMISSION_STATUS = "candidate_only_unadmitted"
DEFAULT_TRANSFORM_DEADLINE_SECONDS = 30.0
HARD_MAX_SOURCE_PIXELS = 4_000_000
HARD_MAX_TARGET_PIXELS = 4_000_000
HARD_MAX_ARRAY_BYTES = 512 * 1024 * 1024
HARD_MAX_OVERLAP_CONTRIBUTIONS = 64_000_000
HARD_MAX_QC_PLANES = 32
DEADLINE_CHECK_OVERLAP_INTERVAL = 4_096

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[a-z][a-z0-9_.:-]{0,127}$")


class AnalysisGridTransformError(RuntimeError):
    """Base error for an invalid transform request or hard failure."""


class AnalysisGridTransformDeadlineError(AnalysisGridTransformError):
    """The one absolute monotonic deadline expired."""


class AnalysisGridTransformCapacityError(AnalysisGridTransformError):
    """A hard caller/resource boundary rejected the complete transform."""


class AnalysisGridTransformIssueState(str, Enum):
    WITHHELD = "withheld"
    INVALID = "invalid"


class TransformEvidenceClass(str, Enum):
    """The evidence class behind a coordinate certificate."""

    SYNTHETIC_EXACT_AFFINE_TEST = "synthetic_exact_affine_test"
    OFFICIAL_EXACT_GRID_ANCHORS = "official_exact_grid_anchors"


class AnalysisGridResamplingMethod(str, Enum):
    """Closed list of physically declared scalar-field methods."""

    CONSERVATIVE_AREA_OVERLAP_RATE = "conservative_area_overlap_rate_v1"


class QualityPlaneResamplingMethod(str, Enum):
    """Closed QC methods; values are never silently averaged."""

    EXACT_AREA_CONSENSUS = "exact_area_consensus_v1"


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha_mapping(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _require_sha(value: str, label: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256")


def _require_safe_id(value: str, label: str) -> None:
    if not isinstance(value, str) or _SAFE_ID_RE.fullmatch(value) is None:
        raise ValueError(f"{label} is not a safe identifier")


def _readonly_copy(array: np.ndarray, *, dtype: Any | None = None) -> np.ndarray:
    result = np.array(array, dtype=dtype, copy=True, order="C")
    result.setflags(write=False)
    return result


def _freeze_owned(array: np.ndarray) -> np.ndarray:
    """Freeze an array allocated by this module without a second peak copy."""

    if not isinstance(array, np.ndarray):
        raise TypeError("only an ndarray can be frozen")
    array.setflags(write=False)
    return array


def _array_receipt(array: np.ndarray) -> dict[str, Any]:
    value = np.asarray(array)
    if value.dtype.hasobject:
        raise ValueError("object arrays cannot be checksum-bound")
    contiguous = np.ascontiguousarray(value)
    return {
        "dtype": contiguous.dtype.str,
        "shape": list(contiguous.shape),
        "sha256": hashlib.sha256(contiguous.tobytes(order="C")).hexdigest(),
    }


def _grid_mapping(grid: AnalysisGrid | None) -> Mapping[str, Any] | None:
    if grid is None:
        return None
    return {
        "axis_order": grid.axis_order,
        "contract_version": grid.contract_version,
        "coordinate_unit": grid.coordinate_unit,
        "crs": grid.crs,
        "grid_id": grid.grid_id,
        "height": grid.height,
        "origin_x_m": grid.origin_x_m,
        "origin_y_m": grid.origin_y_m,
        "row_order": grid.row_order,
        "width": grid.width,
        "x_resolution_m": grid.x_resolution_m,
        "y_resolution_m": grid.y_resolution_m,
    }


def analysis_grid_sha256(grid: AnalysisGrid) -> str:
    """Hash every target-grid contract and geometry field."""

    if not isinstance(grid, AnalysisGrid):
        raise TypeError("grid must be AnalysisGrid")
    mapping = _grid_mapping(grid)
    assert mapping is not None
    return _sha_mapping(mapping)


def native_candidate_content_sha256(
    candidate: NativeRainRateFieldCandidate,
) -> str:
    """Hash all provenance and content, including optional redundant fields.

    This is intentionally recalculated before and after a transform.  Read-only
    NumPy views are not a security boundary because their writable base can be
    mutated by another owner.
    """

    if not isinstance(candidate, NativeRainRateFieldCandidate):
        raise TypeError("candidate must be NativeRainRateFieldCandidate")
    metric = candidate.metric_raster_field
    metric_mapping: Mapping[str, Any] | None = None
    if metric is not None:
        metric_mapping = {
            "field_id": metric.field_id,
            "grid": _grid_mapping(metric.grid),
            "quantity": metric.quantity,
            "unit": metric.unit,
            "valid_at": metric.valid_at,
            "valid_mask": [list(row) for row in metric.valid_mask],
            "values": [list(row) for row in metric.values],
        }
    auxiliary = [
        {
            "name": plane.name,
            "semantics": plane.semantics,
            "values": _array_receipt(plane.values),
        }
        for plane in candidate.auxiliary_planes
    ]
    mapping = {
        "admission_status": candidate.admission_status,
        "algorithm_version": candidate.algorithm_version,
        "analysis_grid": _grid_mapping(candidate.analysis_grid),
        "artifact_handle": candidate.artifact_handle,
        "artifact_metadata_sha256": candidate.artifact_metadata_sha256,
        "artifact_sha256": candidate.artifact_sha256,
        "auxiliary_planes": auxiliary,
        "candidate_id": candidate.candidate_id,
        "canonical_commit_id": candidate.canonical_commit_id,
        "canonical_receipt_sha256": candidate.canonical_receipt_sha256,
        "canonical_result_sha256": candidate.canonical_result_sha256,
        "canonical_run_id": candidate.canonical_run_id,
        "capture_id": candidate.capture_id,
        "capture_manifest_sha256": candidate.capture_manifest_sha256,
        "capture_receipt_sha256": candidate.capture_receipt_sha256,
        "conversion": candidate.conversion,
        "decoder_config_sha256": candidate.decoder_config_sha256,
        "decoder_identity": candidate.decoder_identity,
        "decoder_version": candidate.decoder_version,
        "metric_raster_field": metric_mapping,
        "native_coverage": candidate.native_coverage.to_mapping(),
        "native_crs": candidate.native_crs,
        "native_grid": candidate.native_grid.to_mapping(),
        "nodata_mask": _array_receipt(candidate.nodata_mask),
        "output_quantity": candidate.output_quantity,
        "output_unit": candidate.output_unit,
        "primary_sources": list(candidate.primary_sources),
        "product": candidate.product,
        "provider": candidate.provider,
        "quality_state": candidate.quality_state,
        "rain_rate_mm_h": _array_receipt(candidate.rain_rate_mm_h),
        "reprojection_applied": candidate.reprojection_applied,
        "resampling_applied": candidate.resampling_applied,
        "semantics_certificate_id": candidate.semantics_certificate_id,
        "semantics_certificate_sha256": candidate.semantics_certificate_sha256,
        "source_quantity": candidate.source_quantity,
        "source_unit": candidate.source_unit,
        "source_values": _array_receipt(candidate.source_values),
        "support_end": candidate.support_end,
        "support_start": candidate.support_start,
        "temporal_semantics": candidate.temporal_semantics.value,
        "thresholding_applied": candidate.thresholding_applied,
        "undetect_mask": _array_receipt(candidate.undetect_mask),
        "valid_at": candidate.valid_at,
        "valid_mask": _array_receipt(candidate.valid_mask),
        "wet_dry_classification_applied": candidate.wet_dry_classification_applied,
    }
    return _sha_mapping(mapping)


@dataclass(frozen=True, slots=True)
class NativeFieldCandidateBinding:
    """Caller commitment to one exact bridge candidate and certificate."""

    candidate: NativeRainRateFieldCandidate
    candidate_content_sha256: str
    semantics_certificate_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.candidate, NativeRainRateFieldCandidate):
            raise TypeError("binding candidate has the wrong type")
        _require_sha(self.candidate_content_sha256, "candidate_content_sha256")
        _require_sha(
            self.semantics_certificate_sha256,
            "semantics_certificate_sha256",
        )


@dataclass(frozen=True, slots=True)
class QualityPlaneTransformRule:
    """Exact rule for one named and semantics-bound source QC plane."""

    plane_name: str
    plane_semantics_sha256: str
    source_dtype: str
    method: QualityPlaneResamplingMethod

    def __post_init__(self) -> None:
        _require_safe_id(self.plane_name, "plane_name")
        _require_sha(self.plane_semantics_sha256, "plane_semantics_sha256")
        if not isinstance(self.source_dtype, str) or not self.source_dtype:
            raise ValueError("source_dtype is empty")
        if not isinstance(self.method, QualityPlaneResamplingMethod):
            raise TypeError("quality-plane method is invalid")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "method": self.method.value,
            "plane_name": self.plane_name,
            "plane_semantics_sha256": self.plane_semantics_sha256,
            "source_dtype": self.source_dtype,
        }


@dataclass(frozen=True, slots=True)
class AffineGridTransformCertificate:
    """Certificate for one exact source pixel-edge map into a target CRS.

    ``source_west_edge_target_m`` and ``source_north_edge_target_m`` are the
    target-CRS coordinates of source edge ``(row=0, column=0)``.  Positive
    source resolutions mean columns run east and rows run south.  Rotated,
    sheared, curvilinear and inferred maps are not represented by this class.
    """

    certificate_id: str
    adapter_id: str
    adapter_version: str
    transform_config_sha256: str
    evidence_class: TransformEvidenceClass
    evidence_sha256: str
    evidence_authority: str
    primary_sources: tuple[str, ...]
    source_provider: str
    source_product: str
    source_semantics_certificate_id: str
    source_semantics_certificate_sha256: str
    source_candidate_algorithm: str
    source_crs: str
    source_grid_definition_sha256: str
    source_coverage_definition_sha256: str
    source_width: int
    source_height: int
    source_array_axis_order: str
    target_crs: str
    source_west_edge_target_m: float
    source_north_edge_target_m: float
    source_x_resolution_target_m: float
    source_y_resolution_target_m: float
    quality_plane_rules: tuple[QualityPlaneTransformRule, ...] = ()
    mapping_kind: str = "certified_axis_aligned_pixel_edges"
    target_axis_order: str = "x_east_y_north"
    target_coordinate_unit: str = "m"
    source_column_order: str = "west_to_east"
    source_row_order: str = "north_to_south"

    def __post_init__(self) -> None:
        for label, value in (
            ("certificate_id", self.certificate_id),
            ("adapter_id", self.adapter_id),
            ("adapter_version", self.adapter_version),
        ):
            _require_safe_id(value, label)
        for label, value in (
            ("transform_config_sha256", self.transform_config_sha256),
            ("evidence_sha256", self.evidence_sha256),
            (
                "source_semantics_certificate_sha256",
                self.source_semantics_certificate_sha256,
            ),
            ("source_grid_definition_sha256", self.source_grid_definition_sha256),
            (
                "source_coverage_definition_sha256",
                self.source_coverage_definition_sha256,
            ),
        ):
            _require_sha(value, label)
        if not isinstance(self.evidence_class, TransformEvidenceClass):
            raise TypeError("evidence_class is invalid")
        for label, value in (
            ("evidence_authority", self.evidence_authority),
            ("source_provider", self.source_provider),
            ("source_product", self.source_product),
            ("source_semantics_certificate_id", self.source_semantics_certificate_id),
            ("source_candidate_algorithm", self.source_candidate_algorithm),
            ("source_crs", self.source_crs),
            ("source_array_axis_order", self.source_array_axis_order),
            ("target_crs", self.target_crs),
        ):
            if not isinstance(value, str) or not value or len(value) > 512:
                raise ValueError(f"{label} is invalid")
        if (
            isinstance(self.source_width, bool)
            or not isinstance(self.source_width, int)
            or self.source_width < 1
            or isinstance(self.source_height, bool)
            or not isinstance(self.source_height, int)
            or self.source_height < 1
        ):
            raise ValueError("certificate source dimensions are invalid")
        for label, value in (
            ("source_west_edge_target_m", self.source_west_edge_target_m),
            ("source_north_edge_target_m", self.source_north_edge_target_m),
            ("source_x_resolution_target_m", self.source_x_resolution_target_m),
            ("source_y_resolution_target_m", self.source_y_resolution_target_m),
        ):
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise TypeError(f"{label} must be numeric")
            if not math.isfinite(float(value)):
                raise ValueError(f"{label} must be finite")
        if (
            self.source_x_resolution_target_m <= 0.0
            or self.source_y_resolution_target_m <= 0.0
        ):
            raise ValueError("certificate source resolutions must be positive")
        if self.mapping_kind != "certified_axis_aligned_pixel_edges":
            raise ValueError("only exact axis-aligned pixel-edge maps are supported")
        if self.target_axis_order != "x_east_y_north":
            raise ValueError("target axis order must be explicit x-east/y-north")
        if self.target_coordinate_unit != "m":
            raise ValueError("target coordinate unit must be metres")
        if self.source_column_order != "west_to_east":
            raise ValueError("source columns must explicitly run west to east")
        if self.source_row_order != "north_to_south":
            raise ValueError("source rows must explicitly run north to south")
        if len(self.quality_plane_rules) > HARD_MAX_QC_PLANES:
            raise AnalysisGridTransformCapacityError("too many QC rules")
        if any(
            not isinstance(rule, QualityPlaneTransformRule)
            for rule in self.quality_plane_rules
        ):
            raise TypeError("quality plane rules are invalid")
        names = [rule.plane_name for rule in self.quality_plane_rules]
        if len(names) != len(set(names)):
            raise ValueError("quality plane rule names are not unique")
        if self.evidence_class is TransformEvidenceClass.OFFICIAL_EXACT_GRID_ANCHORS:
            if (
                not self.primary_sources
                or any(
                    not isinstance(source, str)
                    or not source.startswith("https://")
                    or len(source) > 1024
                    for source in self.primary_sources
                )
            ):
                raise ValueError("official transform evidence needs HTTPS sources")
        else:
            if not self.source_provider.lower().startswith("synthetic"):
                raise ValueError(
                    "synthetic affine evidence cannot certify a real provider"
                )
            if self.primary_sources and any(
                not isinstance(source, str) or len(source) > 1024
                for source in self.primary_sources
            ):
                raise ValueError("synthetic transform source labels are invalid")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "adapter_id": self.adapter_id,
            "adapter_version": self.adapter_version,
            "certificate_id": self.certificate_id,
            "evidence_authority": self.evidence_authority,
            "evidence_class": self.evidence_class.value,
            "evidence_sha256": self.evidence_sha256,
            "mapping_kind": self.mapping_kind,
            "primary_sources": list(self.primary_sources),
            "quality_plane_rules": [
                rule.to_mapping() for rule in self.quality_plane_rules
            ],
            "source_array_axis_order": self.source_array_axis_order,
            "source_candidate_algorithm": self.source_candidate_algorithm,
            "source_column_order": self.source_column_order,
            "source_coverage_definition_sha256": (
                self.source_coverage_definition_sha256
            ),
            "source_crs": self.source_crs,
            "source_grid_definition_sha256": self.source_grid_definition_sha256,
            "source_height": self.source_height,
            "source_north_edge_target_m": self.source_north_edge_target_m,
            "source_product": self.source_product,
            "source_provider": self.source_provider,
            "source_row_order": self.source_row_order,
            "source_semantics_certificate_id": (
                self.source_semantics_certificate_id
            ),
            "source_semantics_certificate_sha256": (
                self.source_semantics_certificate_sha256
            ),
            "source_west_edge_target_m": self.source_west_edge_target_m,
            "source_width": self.source_width,
            "source_x_resolution_target_m": self.source_x_resolution_target_m,
            "source_y_resolution_target_m": self.source_y_resolution_target_m,
            "target_axis_order": self.target_axis_order,
            "target_coordinate_unit": self.target_coordinate_unit,
            "target_crs": self.target_crs,
            "transform_config_sha256": self.transform_config_sha256,
        }

    @property
    def fingerprint(self) -> str:
        return _sha_mapping(self.to_mapping())


@dataclass(frozen=True, slots=True)
class AnalysisGridTransformConfig:
    """Explicit deterministic method, tolerances and caller resource limits."""

    resampling_method: AnalysisGridResamplingMethod = (
        AnalysisGridResamplingMethod.CONSERVATIVE_AREA_OVERLAP_RATE
    )
    roundtrip_tolerance_m: float = 1e-6
    area_fraction_tolerance: float = 1e-12
    max_source_pixels: int = 1_000_000
    max_target_pixels: int = 1_000_000
    max_array_bytes: int = 256 * 1024 * 1024
    max_overlap_contributions: int = 16_000_000

    def __post_init__(self) -> None:
        if not isinstance(self.resampling_method, AnalysisGridResamplingMethod):
            raise TypeError("resampling method is invalid")
        if (
            not math.isfinite(self.roundtrip_tolerance_m)
            or self.roundtrip_tolerance_m < 0.0
        ):
            raise ValueError("roundtrip tolerance must be finite and non-negative")
        if (
            not math.isfinite(self.area_fraction_tolerance)
            or not 0.0 <= self.area_fraction_tolerance <= 1e-6
        ):
            raise ValueError("area fraction tolerance is invalid")
        for label, value, hard in (
            ("max_source_pixels", self.max_source_pixels, HARD_MAX_SOURCE_PIXELS),
            ("max_target_pixels", self.max_target_pixels, HARD_MAX_TARGET_PIXELS),
            ("max_array_bytes", self.max_array_bytes, HARD_MAX_ARRAY_BYTES),
            (
                "max_overlap_contributions",
                self.max_overlap_contributions,
                HARD_MAX_OVERLAP_CONTRIBUTIONS,
            ),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 1
                or value > hard
            ):
                raise ValueError(f"{label} is outside the hard boundary")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "area_fraction_tolerance": self.area_fraction_tolerance,
            "max_array_bytes": self.max_array_bytes,
            "max_overlap_contributions": self.max_overlap_contributions,
            "max_source_pixels": self.max_source_pixels,
            "max_target_pixels": self.max_target_pixels,
            "resampling_method": self.resampling_method.value,
            "roundtrip_tolerance_m": self.roundtrip_tolerance_m,
        }

    @property
    def fingerprint(self) -> str:
        return _sha_mapping(self.to_mapping())


@dataclass(frozen=True, slots=True)
class CoordinateRoundTripSample:
    source_row_edge: float
    source_column_edge: float
    target_x_m: float
    target_y_m: float
    recovered_row_edge: float
    recovered_column_edge: float
    error_m: float

    def __post_init__(self) -> None:
        for value in (
            self.source_row_edge,
            self.source_column_edge,
            self.target_x_m,
            self.target_y_m,
            self.recovered_row_edge,
            self.recovered_column_edge,
            self.error_m,
        ):
            if not math.isfinite(value):
                raise ValueError("coordinate roundtrip sample must be finite")
        if self.error_m < 0.0:
            raise ValueError("coordinate roundtrip error cannot be negative")

    def to_mapping(self) -> dict[str, float]:
        return {
            "error_m": self.error_m,
            "recovered_column_edge": self.recovered_column_edge,
            "recovered_row_edge": self.recovered_row_edge,
            "source_column_edge": self.source_column_edge,
            "source_row_edge": self.source_row_edge,
            "target_x_m": self.target_x_m,
            "target_y_m": self.target_y_m,
        }


@dataclass(frozen=True, slots=True)
class CertifiedCoordinateTransformReceipt:
    """Adapter-issued, independently auditable coordinate receipt."""

    receipt_id: str
    certificate_id: str
    certificate_sha256: str
    adapter_id: str
    adapter_version: str
    transform_config_sha256: str
    source_candidate_id: str
    source_candidate_content_sha256: str
    source_semantics_certificate_sha256: str
    target_grid_sha256: str
    target_crs: str
    resampling_method: AnalysisGridResamplingMethod
    source_support_edges_target_m: tuple[float, float, float, float]
    roundtrip_samples: tuple[CoordinateRoundTripSample, ...]
    maximum_roundtrip_error_m: float

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "adapter_id": self.adapter_id,
            "adapter_version": self.adapter_version,
            "certificate_id": self.certificate_id,
            "certificate_sha256": self.certificate_sha256,
            "maximum_roundtrip_error_m": self.maximum_roundtrip_error_m,
            "resampling_method": self.resampling_method.value,
            "roundtrip_samples": [
                sample.to_mapping() for sample in self.roundtrip_samples
            ],
            "source_candidate_content_sha256": self.source_candidate_content_sha256,
            "source_candidate_id": self.source_candidate_id,
            "source_semantics_certificate_sha256": (
                self.source_semantics_certificate_sha256
            ),
            "source_support_edges_target_m": list(
                self.source_support_edges_target_m
            ),
            "target_crs": self.target_crs,
            "target_grid_sha256": self.target_grid_sha256,
            "transform_config_sha256": self.transform_config_sha256,
        }

    @property
    def fingerprint(self) -> str:
        return _sha_mapping(self.payload_mapping())

    def __post_init__(self) -> None:
        _require_safe_id(self.certificate_id, "receipt certificate_id")
        _require_safe_id(self.adapter_id, "receipt adapter_id")
        _require_safe_id(self.adapter_version, "receipt adapter_version")
        for label, value in (
            ("certificate_sha256", self.certificate_sha256),
            ("transform_config_sha256", self.transform_config_sha256),
            ("source_candidate_content_sha256", self.source_candidate_content_sha256),
            (
                "source_semantics_certificate_sha256",
                self.source_semantics_certificate_sha256,
            ),
            ("target_grid_sha256", self.target_grid_sha256),
        ):
            _require_sha(value, label)
        if (
            not isinstance(self.source_candidate_id, str)
            or not self.source_candidate_id
            or len(self.source_candidate_id) > 512
            or not isinstance(self.target_crs, str)
            or not self.target_crs
            or len(self.target_crs) > 512
        ):
            raise ValueError("receipt source candidate or target CRS is invalid")
        if not isinstance(self.resampling_method, AnalysisGridResamplingMethod):
            raise TypeError("receipt resampling method is invalid")
        if len(self.source_support_edges_target_m) != 4 or any(
            not math.isfinite(value)
            for value in self.source_support_edges_target_m
        ):
            raise ValueError("receipt source support is invalid")
        west, south, east, north = self.source_support_edges_target_m
        if not west < east or not south < north:
            raise ValueError("receipt source support is inverted")
        if not self.roundtrip_samples or any(
            not isinstance(sample, CoordinateRoundTripSample)
            for sample in self.roundtrip_samples
        ):
            raise ValueError("receipt roundtrip audit is empty or invalid")
        if (
            not math.isfinite(self.maximum_roundtrip_error_m)
            or self.maximum_roundtrip_error_m < 0.0
            or self.maximum_roundtrip_error_m
            != max(sample.error_m for sample in self.roundtrip_samples)
        ):
            raise ValueError("receipt maximum roundtrip error is invalid")
        expected = "transform-receipt-" + self.fingerprint
        if self.receipt_id != expected:
            raise ValueError("transform receipt identity is not content-derived")


@runtime_checkable
class CertifiedCoordinateTransformAdapter(Protocol):
    @property
    def certificate(self) -> AffineGridTransformCertificate: ...

    def issue_receipt(
        self,
        *,
        binding: NativeFieldCandidateBinding,
        target_grid: AnalysisGrid,
        config: AnalysisGridTransformConfig,
        deadline_at: float,
        monotonic: Callable[[], float],
    ) -> CertifiedCoordinateTransformReceipt: ...


def _expected_roundtrip_samples(
    certificate: AffineGridTransformCertificate,
) -> tuple[CoordinateRoundTripSample, ...]:
    samples: list[CoordinateRoundTripSample] = []
    points = (
        (0.0, 0.0),
        (0.0, float(certificate.source_width)),
        (float(certificate.source_height), 0.0),
        (float(certificate.source_height), float(certificate.source_width)),
        (certificate.source_height / 2.0, certificate.source_width / 2.0),
    )
    for row_edge, column_edge in points:
        x_m = (
            certificate.source_west_edge_target_m
            + column_edge * certificate.source_x_resolution_target_m
        )
        y_m = (
            certificate.source_north_edge_target_m
            - row_edge * certificate.source_y_resolution_target_m
        )
        recovered_column = (
            x_m - certificate.source_west_edge_target_m
        ) / certificate.source_x_resolution_target_m
        recovered_row = (
            certificate.source_north_edge_target_m - y_m
        ) / certificate.source_y_resolution_target_m
        error_m = math.hypot(
            (recovered_column - column_edge)
            * certificate.source_x_resolution_target_m,
            (recovered_row - row_edge)
            * certificate.source_y_resolution_target_m,
        )
        samples.append(CoordinateRoundTripSample(
            source_row_edge=row_edge,
            source_column_edge=column_edge,
            target_x_m=x_m,
            target_y_m=y_m,
            recovered_row_edge=recovered_row,
            recovered_column_edge=recovered_column,
            error_m=error_m,
        ))
    return tuple(samples)


class ExactAffineGridTransformAdapter:
    """Receipt issuer for the closed exact-affine edge-map certificate."""

    def __init__(self, certificate: AffineGridTransformCertificate) -> None:
        if not isinstance(certificate, AffineGridTransformCertificate):
            raise TypeError("certificate must be AffineGridTransformCertificate")
        self._certificate = certificate

    @property
    def certificate(self) -> AffineGridTransformCertificate:
        return self._certificate

    def issue_receipt(
        self,
        *,
        binding: NativeFieldCandidateBinding,
        target_grid: AnalysisGrid,
        config: AnalysisGridTransformConfig,
        deadline_at: float,
        monotonic: Callable[[], float],
    ) -> CertifiedCoordinateTransformReceipt:
        _check_deadline(deadline_at, monotonic)
        certificate = self._certificate
        samples = _expected_roundtrip_samples(certificate)
        south = (
            certificate.source_north_edge_target_m
            - certificate.source_height
            * certificate.source_y_resolution_target_m
        )
        east = (
            certificate.source_west_edge_target_m
            + certificate.source_width
            * certificate.source_x_resolution_target_m
        )
        payload = {
            "adapter_id": certificate.adapter_id,
            "adapter_version": certificate.adapter_version,
            "certificate_id": certificate.certificate_id,
            "certificate_sha256": certificate.fingerprint,
            "maximum_roundtrip_error_m": max(item.error_m for item in samples),
            "resampling_method": config.resampling_method.value,
            "roundtrip_samples": [item.to_mapping() for item in samples],
            "source_candidate_content_sha256": binding.candidate_content_sha256,
            "source_candidate_id": binding.candidate.candidate_id,
            "source_semantics_certificate_sha256": (
                binding.semantics_certificate_sha256
            ),
            "source_support_edges_target_m": [
                certificate.source_west_edge_target_m,
                south,
                east,
                certificate.source_north_edge_target_m,
            ],
            "target_crs": target_grid.crs,
            "target_grid_sha256": analysis_grid_sha256(target_grid),
            "transform_config_sha256": config.fingerprint,
        }
        receipt = CertifiedCoordinateTransformReceipt(
            receipt_id="transform-receipt-" + _sha_mapping(payload),
            certificate_id=certificate.certificate_id,
            certificate_sha256=certificate.fingerprint,
            adapter_id=certificate.adapter_id,
            adapter_version=certificate.adapter_version,
            transform_config_sha256=config.fingerprint,
            source_candidate_id=binding.candidate.candidate_id,
            source_candidate_content_sha256=binding.candidate_content_sha256,
            source_semantics_certificate_sha256=(
                binding.semantics_certificate_sha256
            ),
            target_grid_sha256=analysis_grid_sha256(target_grid),
            target_crs=target_grid.crs,
            resampling_method=config.resampling_method,
            source_support_edges_target_m=(
                certificate.source_west_edge_target_m,
                south,
                east,
                certificate.source_north_edge_target_m,
            ),
            roundtrip_samples=samples,
            maximum_roundtrip_error_m=max(item.error_m for item in samples),
        )
        _check_deadline(deadline_at, monotonic)
        return receipt


@dataclass(frozen=True, slots=True)
class AnalysisQualityPlane:
    """QC plane on the target grid; its validity means exact consensus."""

    name: str
    source_semantics: str
    source_values_sha256: str
    method: QualityPlaneResamplingMethod
    values: np.ndarray
    consensus_mask: np.ndarray

    def __post_init__(self) -> None:
        _require_safe_id(self.name, "analysis QC plane name")
        _require_sha(self.source_values_sha256, "source_values_sha256")
        if (
            not isinstance(self.source_semantics, str)
            or not self.source_semantics
            or len(self.source_semantics) > 512
        ):
            raise ValueError("analysis QC source semantics are invalid")
        if not isinstance(self.method, QualityPlaneResamplingMethod):
            raise TypeError("analysis QC plane method is invalid")
        if not isinstance(self.values, np.ndarray) or self.values.ndim != 2:
            raise TypeError("analysis QC plane values must be two-dimensional")
        if (
            not isinstance(self.consensus_mask, np.ndarray)
            or self.consensus_mask.shape != self.values.shape
            or self.consensus_mask.dtype != np.bool_
        ):
            raise ValueError("analysis QC consensus mask is invalid")
        if self.values.flags.writeable or self.consensus_mask.flags.writeable:
            raise ValueError("analysis QC arrays must be immutable")


@dataclass(frozen=True, slots=True)
class TransformPixelAccounting:
    total_target_pixels: int
    valid_pixels: int
    nodata_pixels: int
    undetect_pixels: int
    mixed_pixels: int
    outside_support_pixels: int
    overlap_contributions: int
    valid_source_overlap_area_m2: float
    nodata_source_overlap_area_m2: float
    undetect_source_overlap_area_m2: float
    outside_support_area_m2: float
    target_area_m2: float
    valid_rate_area_integral_mm_h_m2: float

    def __post_init__(self) -> None:
        integer_fields = (
            self.total_target_pixels,
            self.valid_pixels,
            self.nodata_pixels,
            self.undetect_pixels,
            self.mixed_pixels,
            self.outside_support_pixels,
            self.overlap_contributions,
        )
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in integer_fields
        ):
            raise ValueError("pixel accounting counts must be non-negative integers")
        if (
            self.valid_pixels
            + self.nodata_pixels
            + self.undetect_pixels
            + self.mixed_pixels
            + self.outside_support_pixels
            != self.total_target_pixels
        ):
            raise ValueError("pixel accounting states are not exhaustive")
        for value in (
            self.valid_source_overlap_area_m2,
            self.nodata_source_overlap_area_m2,
            self.undetect_source_overlap_area_m2,
            self.outside_support_area_m2,
            self.target_area_m2,
            self.valid_rate_area_integral_mm_h_m2,
        ):
            if not math.isfinite(value) or value < 0.0:
                raise ValueError("pixel accounting areas/integral must be non-negative")

    def to_mapping(self) -> dict[str, int | float]:
        return {
            field: getattr(self, field)
            for field in self.__dataclass_fields__
        }


@dataclass(frozen=True, slots=True)
class AnalysisGridRainRateFieldCandidate:
    """Immutable, still-unadmitted rain-rate candidate on one target grid."""

    field_id: str
    output_content_sha256: str
    source_candidate_id: str
    source_candidate_content_sha256: str
    source_semantics_certificate_id: str
    source_semantics_certificate_sha256: str
    transform_certificate: AffineGridTransformCertificate
    transform_certificate_id: str
    transform_certificate_sha256: str
    transform_receipt: CertifiedCoordinateTransformReceipt
    transform_receipt_id: str
    transform_receipt_sha256: str
    transform_config: AnalysisGridTransformConfig
    transform_config_sha256: str
    target_grid: AnalysisGrid
    target_grid_sha256: str
    resampling_method: AnalysisGridResamplingMethod
    rain_rate_mm_h: np.ndarray
    valid_mask: np.ndarray
    nodata_mask: np.ndarray
    undetect_mask: np.ndarray
    mixed_mask: np.ndarray
    outside_support_mask: np.ndarray
    valid_area_fraction: np.ndarray
    nodata_area_fraction: np.ndarray
    undetect_area_fraction: np.ndarray
    outside_support_area_fraction: np.ndarray
    source_auxiliary_planes: tuple[NativeRasterPlane, ...]
    analysis_quality_planes: tuple[AnalysisQualityPlane, ...]
    pixel_accounting: TransformPixelAccounting
    output_quantity: str
    output_unit: str
    temporal_semantics: str
    support_start: float
    support_end: float
    valid_at: float
    admission_status: str = ANALYSIS_GRID_TRANSFORM_ADMISSION_STATUS
    coordinate_transform_applied: bool = True
    reprojection_applied: bool = True
    resampling_applied: bool = True
    extrapolation_applied: bool = False
    thresholding_applied: bool = False
    wet_dry_classification_applied: bool = False
    algorithm_version: str = ANALYSIS_GRID_TRANSFORM_ALGORITHM

    def __post_init__(self) -> None:
        _require_sha(self.output_content_sha256, "output_content_sha256")
        if self.field_id != "analysis-field-" + self.output_content_sha256:
            raise ValueError("analysis field identity is not content-derived")
        if (
            not isinstance(self.transform_certificate, AffineGridTransformCertificate)
            or self.transform_certificate_id
            != self.transform_certificate.certificate_id
            or self.transform_certificate_sha256
            != self.transform_certificate.fingerprint
        ):
            raise ValueError("embedded transform certificate binding is invalid")
        if (
            not isinstance(self.transform_receipt, CertifiedCoordinateTransformReceipt)
            or self.transform_receipt_id != self.transform_receipt.receipt_id
            or self.transform_receipt_sha256 != self.transform_receipt.fingerprint
        ):
            raise ValueError("embedded transform receipt binding is invalid")
        if (
            not isinstance(self.transform_config, AnalysisGridTransformConfig)
            or self.transform_config_sha256 != self.transform_config.fingerprint
        ):
            raise ValueError("embedded transform config binding is invalid")
        if self.transform_receipt.certificate_sha256 != self.transform_certificate_sha256:
            raise ValueError("receipt and embedded transform certificate differ")
        shape = (self.target_grid.height, self.target_grid.width)
        for label, array in (
            ("rain_rate_mm_h", self.rain_rate_mm_h),
            ("valid_mask", self.valid_mask),
            ("nodata_mask", self.nodata_mask),
            ("undetect_mask", self.undetect_mask),
            ("mixed_mask", self.mixed_mask),
            ("outside_support_mask", self.outside_support_mask),
            ("valid_area_fraction", self.valid_area_fraction),
            ("nodata_area_fraction", self.nodata_area_fraction),
            ("undetect_area_fraction", self.undetect_area_fraction),
            ("outside_support_area_fraction", self.outside_support_area_fraction),
        ):
            if not isinstance(array, np.ndarray) or array.shape != shape:
                raise ValueError(f"{label} does not match target grid")
            if array.flags.writeable:
                raise ValueError(f"{label} must be immutable")
        for array in (
            self.valid_area_fraction,
            self.nodata_area_fraction,
            self.undetect_area_fraction,
            self.outside_support_area_fraction,
        ):
            if np.any(array < 0.0) or np.any(array > 1.0):
                raise ValueError("analysis area fraction lies outside [0, 1]")
        states = (
            self.valid_mask.astype(np.uint8)
            + self.nodata_mask.astype(np.uint8)
            + self.undetect_mask.astype(np.uint8)
            + self.mixed_mask.astype(np.uint8)
            + self.outside_support_mask.astype(np.uint8)
        )
        if not np.all(states == 1):
            raise ValueError("analysis cell-state masks are not exhaustive")
        if not np.array_equal(np.isnan(self.rain_rate_mm_h), ~self.valid_mask):
            raise ValueError("non-valid analysis rain rates must be NaN")
        if (
            not np.isfinite(self.rain_rate_mm_h[self.valid_mask]).all()
            or np.any(self.rain_rate_mm_h[self.valid_mask] < 0.0)
        ):
            raise ValueError("valid analysis rain rates are not physical")
        fractions = (
            self.valid_area_fraction
            + self.nodata_area_fraction
            + self.undetect_area_fraction
            + self.outside_support_area_fraction
        )
        if not np.isfinite(fractions).all() or np.any(np.abs(fractions - 1.0) > 1e-9):
            raise ValueError("analysis area fractions are not exhaustive")
        if self.admission_status != ANALYSIS_GRID_TRANSFORM_ADMISSION_STATUS:
            raise ValueError("analysis-grid output cannot grant admission")
        if (
            not self.coordinate_transform_applied
            or not isinstance(self.reprojection_applied, bool)
            or not self.resampling_applied
            or self.extrapolation_applied
            or self.thresholding_applied
            or self.wet_dry_classification_applied
        ):
            raise ValueError("analysis-grid operation flags are inconsistent")
        if self.output_unit != "mm/h":
            raise ValueError("analysis-grid field must remain mm/h")
        if self.target_grid_sha256 != analysis_grid_sha256(self.target_grid):
            raise ValueError("analysis target-grid checksum is inconsistent")
        for timestamp in (self.support_start, self.support_end, self.valid_at):
            if not math.isfinite(timestamp):
                raise ValueError("analysis-grid field time must be finite")
        if (
            self.support_end < self.support_start
            or not self.support_start <= self.valid_at <= self.support_end
        ):
            raise ValueError("analysis-grid field temporal support is invalid")
        for plane in self.analysis_quality_planes:
            if plane.values.shape != shape:
                raise ValueError("analysis QC plane does not match target grid")

    def value_or_none(self, row: int, column: int) -> float | None:
        self.target_grid._validate_index(row, column)
        if not self.valid_mask[row, column]:
            return None
        return float(self.rain_rate_mm_h[row, column])


@dataclass(frozen=True, slots=True)
class AnalysisGridTransformIssue:
    state: AnalysisGridTransformIssueState
    code: str
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.state, AnalysisGridTransformIssueState):
            raise TypeError("analysis-grid issue state is invalid")
        _require_safe_id(self.code, "analysis-grid issue code")
        if not isinstance(self.detail, str) or not self.detail or len(self.detail) > 1024:
            raise ValueError("analysis-grid issue detail is invalid")


@dataclass(frozen=True, slots=True)
class AnalysisGridTransformResult:
    source_candidate_id: str
    field: AnalysisGridRainRateFieldCandidate | None
    issues: tuple[AnalysisGridTransformIssue, ...]

    @property
    def is_candidate(self) -> bool:
        return self.field is not None and not self.issues


def _issue(
    state: AnalysisGridTransformIssueState,
    code: str,
    detail: str,
) -> AnalysisGridTransformIssue:
    return AnalysisGridTransformIssue(state=state, code=code, detail=detail)


def _deadline(
    deadline_at: float | None,
    monotonic: Callable[[], float],
) -> float:
    current = float(monotonic())
    if not math.isfinite(current):
        raise ValueError("monotonic clock must be finite")
    if deadline_at is None:
        return current + DEFAULT_TRANSFORM_DEADLINE_SECONDS
    if isinstance(deadline_at, bool) or not isinstance(deadline_at, (int, float)):
        raise ValueError("deadline_at must be finite")
    result = float(deadline_at)
    if not math.isfinite(result):
        raise ValueError("deadline_at must be finite")
    return result


def _check_deadline(deadline_at: float, monotonic: Callable[[], float]) -> None:
    current = float(monotonic())
    if not math.isfinite(current) or current >= deadline_at:
        raise AnalysisGridTransformDeadlineError(
            "analysis-grid transform deadline expired"
        )


def _validate_binding(
    binding: NativeFieldCandidateBinding,
) -> AnalysisGridTransformIssue | None:
    candidate = binding.candidate
    if (
        candidate.admission_status != RADAR_FIELD_ADMISSION_STATUS
        or candidate.reprojection_applied
        or candidate.resampling_applied
        or candidate.thresholding_applied
        or candidate.wet_dry_classification_applied
    ):
        return _issue(
            AnalysisGridTransformIssueState.INVALID,
            "source_not_native_candidate_only",
            "source is not an untouched candidate-only native radar field",
        )
    if candidate.output_unit != "mm/h":
        return _issue(
            AnalysisGridTransformIssueState.WITHHELD,
            "source_quantity_not_supported",
            "only an already-certified mm/h rate candidate is supported",
        )
    if candidate.semantics_certificate_sha256 != binding.semantics_certificate_sha256:
        return _issue(
            AnalysisGridTransformIssueState.INVALID,
            "source_semantics_binding_mismatch",
            "candidate semantics certificate differs from caller binding",
        )
    try:
        actual = native_candidate_content_sha256(candidate)
    except (TypeError, ValueError) as exc:
        return _issue(
            AnalysisGridTransformIssueState.INVALID,
            "source_candidate_checksum_failed",
            f"source candidate could not be checksummed: {type(exc).__name__}",
        )
    if actual != binding.candidate_content_sha256:
        return _issue(
            AnalysisGridTransformIssueState.INVALID,
            "source_candidate_checksum_mismatch",
            "source candidate content differs from its caller binding",
        )
    return None


def _validate_certificate(
    candidate: NativeRainRateFieldCandidate,
    certificate: AffineGridTransformCertificate,
    target_grid: AnalysisGrid,
    config: AnalysisGridTransformConfig,
) -> AnalysisGridTransformIssue | None:
    exact_pairs = (
        (candidate.provider, certificate.source_provider),
        (candidate.product, certificate.source_product),
        (
            candidate.semantics_certificate_id,
            certificate.source_semantics_certificate_id,
        ),
        (
            candidate.semantics_certificate_sha256,
            certificate.source_semantics_certificate_sha256,
        ),
        (candidate.algorithm_version, certificate.source_candidate_algorithm),
        (candidate.native_crs, certificate.source_crs),
        (
            candidate.native_grid.definition_sha256,
            certificate.source_grid_definition_sha256,
        ),
        (
            candidate.native_coverage.definition_sha256,
            certificate.source_coverage_definition_sha256,
        ),
        (candidate.native_grid.width, certificate.source_width),
        (candidate.native_grid.height, certificate.source_height),
        (candidate.native_grid.axis_order, certificate.source_array_axis_order),
        (target_grid.crs, certificate.target_crs),
        (config.fingerprint, certificate.transform_config_sha256),
    )
    if any(left != right for left, right in exact_pairs):
        return _issue(
            AnalysisGridTransformIssueState.WITHHELD,
            "transform_certificate_binding_mismatch",
            "coordinate certificate does not exactly bind source, target CRS and config",
        )
    if candidate.native_grid.kind not in {"regular", "affine"}:
        return _issue(
            AnalysisGridTransformIssueState.WITHHELD,
            "source_geometry_not_exact_affine",
            "curvilinear or unstructured source geometry has no supported adapter",
        )
    planes = {plane.name: plane for plane in candidate.auxiliary_planes}
    rules = {rule.plane_name: rule for rule in certificate.quality_plane_rules}
    if set(planes) != set(rules):
        return _issue(
            AnalysisGridTransformIssueState.WITHHELD,
            "quality_plane_policy_incomplete",
            "every source QC plane needs exactly one semantics-bound rule",
        )
    for name, plane in planes.items():
        rule = rules[name]
        if (
            hashlib.sha256(plane.semantics.encode("utf-8")).hexdigest()
            != rule.plane_semantics_sha256
            or plane.values.dtype.str != rule.source_dtype
            or plane.values.dtype.hasobject
            or plane.values.dtype.kind not in "biuf"
        ):
            return _issue(
                AnalysisGridTransformIssueState.WITHHELD,
                "quality_plane_semantics_mismatch",
                f"QC plane {name} is not covered by its exact certificate rule",
            )
    return None


def _validate_receipt(
    receipt: CertifiedCoordinateTransformReceipt,
    binding: NativeFieldCandidateBinding,
    target_grid: AnalysisGrid,
    certificate: AffineGridTransformCertificate,
    config: AnalysisGridTransformConfig,
) -> AnalysisGridTransformIssue | None:
    expected = (
        (receipt.certificate_id, certificate.certificate_id),
        (receipt.certificate_sha256, certificate.fingerprint),
        (receipt.adapter_id, certificate.adapter_id),
        (receipt.adapter_version, certificate.adapter_version),
        (receipt.transform_config_sha256, config.fingerprint),
        (receipt.source_candidate_id, binding.candidate.candidate_id),
        (
            receipt.source_candidate_content_sha256,
            binding.candidate_content_sha256,
        ),
        (
            receipt.source_semantics_certificate_sha256,
            binding.semantics_certificate_sha256,
        ),
        (receipt.target_grid_sha256, analysis_grid_sha256(target_grid)),
        (receipt.target_crs, target_grid.crs),
        (receipt.resampling_method, config.resampling_method),
    )
    if any(left != right for left, right in expected):
        return _issue(
            AnalysisGridTransformIssueState.INVALID,
            "coordinate_transform_receipt_mismatch",
            "transform receipt differs from request, certificate or adapter",
        )
    if receipt.receipt_id != "transform-receipt-" + receipt.fingerprint:
        return _issue(
            AnalysisGridTransformIssueState.INVALID,
            "coordinate_transform_receipt_checksum_mismatch",
            "transform receipt is not content-derived",
        )
    if receipt.maximum_roundtrip_error_m > config.roundtrip_tolerance_m:
        return _issue(
            AnalysisGridTransformIssueState.WITHHELD,
            "coordinate_roundtrip_tolerance_exceeded",
            "certified coordinate roundtrip exceeds caller tolerance",
        )
    expected_samples = _expected_roundtrip_samples(certificate)
    if (
        receipt.roundtrip_samples != expected_samples
        or receipt.maximum_roundtrip_error_m
        != max(sample.error_m for sample in expected_samples)
    ):
        return _issue(
            AnalysisGridTransformIssueState.INVALID,
            "coordinate_roundtrip_audit_mismatch",
            "receipt roundtrip audit differs from independent affine recomputation",
        )
    expected_support = (
        certificate.source_west_edge_target_m,
        certificate.source_north_edge_target_m
        - certificate.source_height * certificate.source_y_resolution_target_m,
        certificate.source_west_edge_target_m
        + certificate.source_width * certificate.source_x_resolution_target_m,
        certificate.source_north_edge_target_m,
    )
    if receipt.source_support_edges_target_m != expected_support:
        return _issue(
            AnalysisGridTransformIssueState.INVALID,
            "coordinate_support_receipt_mismatch",
            "receipt support differs from the certificate edge map",
        )
    return None


def _output_payload(
    *,
    source: NativeRainRateFieldCandidate,
    binding: NativeFieldCandidateBinding,
    target_grid: AnalysisGrid,
    certificate: AffineGridTransformCertificate,
    receipt: CertifiedCoordinateTransformReceipt,
    config: AnalysisGridTransformConfig,
    rates: np.ndarray,
    state_masks: Mapping[str, np.ndarray],
    fractions: Mapping[str, np.ndarray],
    source_planes: tuple[NativeRasterPlane, ...],
    quality_planes: tuple[AnalysisQualityPlane, ...],
    accounting: TransformPixelAccounting,
) -> dict[str, Any]:
    return {
        "accounting": accounting.to_mapping(),
        "admission_status": ANALYSIS_GRID_TRANSFORM_ADMISSION_STATUS,
        "algorithm_version": ANALYSIS_GRID_TRANSFORM_ALGORITHM,
        "analysis_quality_planes": [
            {
                "consensus_mask": _array_receipt(plane.consensus_mask),
                "method": plane.method.value,
                "name": plane.name,
                "source_semantics": plane.source_semantics,
                "source_values_sha256": plane.source_values_sha256,
                "values": _array_receipt(plane.values),
            }
            for plane in quality_planes
        ],
        "fractions": {
            name: _array_receipt(value) for name, value in fractions.items()
        },
        "output_quantity": source.output_quantity,
        "output_unit": source.output_unit,
        "operation_flags": {
            "coordinate_transform_applied": True,
            "extrapolation_applied": False,
            "reprojection_applied": source.native_crs != target_grid.crs,
            "resampling_applied": True,
            "thresholding_applied": False,
            "wet_dry_classification_applied": False,
        },
        "rain_rate_mm_h": _array_receipt(rates),
        "resampling_method": config.resampling_method.value,
        "source_auxiliary_planes": [
            {
                "name": plane.name,
                "semantics": plane.semantics,
                "values": _array_receipt(plane.values),
            }
            for plane in source_planes
        ],
        "source_candidate_content_sha256": binding.candidate_content_sha256,
        "source_candidate_id": source.candidate_id,
        "source_semantics_certificate_id": source.semantics_certificate_id,
        "source_semantics_certificate_sha256": (
            binding.semantics_certificate_sha256
        ),
        "state_masks": {
            name: _array_receipt(value) for name, value in state_masks.items()
        },
        "support_end": source.support_end,
        "support_start": source.support_start,
        "target_grid": _grid_mapping(target_grid),
        "target_grid_sha256": analysis_grid_sha256(target_grid),
        "temporal_semantics": source.temporal_semantics.value,
        "transform_certificate_id": certificate.certificate_id,
        "transform_certificate_sha256": certificate.fingerprint,
        "transform_certificate": certificate.to_mapping(),
        "transform_config": config.to_mapping(),
        "transform_config_sha256": config.fingerprint,
        "transform_receipt_id": receipt.receipt_id,
        "transform_receipt_sha256": receipt.fingerprint,
        "transform_receipt": receipt.payload_mapping(),
        "valid_at": source.valid_at,
    }


def analysis_grid_field_content_sha256(
    field: AnalysisGridRainRateFieldCandidate,
) -> str:
    """Recalculate the complete immutable output payload checksum."""

    if not isinstance(field, AnalysisGridRainRateFieldCandidate):
        raise TypeError("field must be AnalysisGridRainRateFieldCandidate")
    payload = {
        "accounting": field.pixel_accounting.to_mapping(),
        "admission_status": field.admission_status,
        "algorithm_version": field.algorithm_version,
        "analysis_quality_planes": [
            {
                "consensus_mask": _array_receipt(plane.consensus_mask),
                "method": plane.method.value,
                "name": plane.name,
                "source_semantics": plane.source_semantics,
                "source_values_sha256": plane.source_values_sha256,
                "values": _array_receipt(plane.values),
            }
            for plane in field.analysis_quality_planes
        ],
        "fractions": {
            "nodata": _array_receipt(field.nodata_area_fraction),
            "outside_support": _array_receipt(
                field.outside_support_area_fraction
            ),
            "undetect": _array_receipt(field.undetect_area_fraction),
            "valid": _array_receipt(field.valid_area_fraction),
        },
        "operation_flags": {
            "coordinate_transform_applied": field.coordinate_transform_applied,
            "extrapolation_applied": field.extrapolation_applied,
            "reprojection_applied": field.reprojection_applied,
            "resampling_applied": field.resampling_applied,
            "thresholding_applied": field.thresholding_applied,
            "wet_dry_classification_applied": (
                field.wet_dry_classification_applied
            ),
        },
        "output_quantity": field.output_quantity,
        "output_unit": field.output_unit,
        "rain_rate_mm_h": _array_receipt(field.rain_rate_mm_h),
        "resampling_method": field.resampling_method.value,
        "source_auxiliary_planes": [
            {
                "name": plane.name,
                "semantics": plane.semantics,
                "values": _array_receipt(plane.values),
            }
            for plane in field.source_auxiliary_planes
        ],
        "source_candidate_content_sha256": field.source_candidate_content_sha256,
        "source_candidate_id": field.source_candidate_id,
        "source_semantics_certificate_id": field.source_semantics_certificate_id,
        "source_semantics_certificate_sha256": (
            field.source_semantics_certificate_sha256
        ),
        "state_masks": {
            "mixed": _array_receipt(field.mixed_mask),
            "nodata": _array_receipt(field.nodata_mask),
            "outside_support": _array_receipt(field.outside_support_mask),
            "undetect": _array_receipt(field.undetect_mask),
            "valid": _array_receipt(field.valid_mask),
        },
        "support_end": field.support_end,
        "support_start": field.support_start,
        "target_grid": _grid_mapping(field.target_grid),
        "target_grid_sha256": field.target_grid_sha256,
        "temporal_semantics": field.temporal_semantics,
        "transform_certificate": field.transform_certificate.to_mapping(),
        "transform_certificate_id": field.transform_certificate_id,
        "transform_certificate_sha256": field.transform_certificate_sha256,
        "transform_config": field.transform_config.to_mapping(),
        "transform_config_sha256": field.transform_config_sha256,
        "transform_receipt": field.transform_receipt.payload_mapping(),
        "transform_receipt_id": field.transform_receipt_id,
        "transform_receipt_sha256": field.transform_receipt_sha256,
        "valid_at": field.valid_at,
    }
    return _sha_mapping(payload)


def transform_native_rain_rate_to_analysis_grid(
    binding: NativeFieldCandidateBinding,
    *,
    target_grid: AnalysisGrid,
    transform_adapter: CertifiedCoordinateTransformAdapter,
    config: AnalysisGridTransformConfig,
    deadline_at: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> AnalysisGridTransformResult:
    """Create one unadmitted analysis-grid field or a fail-closed issue."""

    if not isinstance(binding, NativeFieldCandidateBinding):
        raise TypeError("binding must be NativeFieldCandidateBinding")
    if not isinstance(target_grid, AnalysisGrid):
        raise TypeError("target_grid must be caller-supplied AnalysisGrid")
    if not isinstance(config, AnalysisGridTransformConfig):
        raise TypeError("config must be AnalysisGridTransformConfig")
    if not isinstance(transform_adapter, CertifiedCoordinateTransformAdapter):
        raise TypeError("transform_adapter must issue certified receipts")
    active_deadline = _deadline(deadline_at, monotonic)
    _check_deadline(active_deadline, monotonic)
    source = binding.candidate
    source_pixels = source.native_grid.width * source.native_grid.height
    target_pixels = target_grid.width * target_grid.height
    if source_pixels > config.max_source_pixels:
        raise AnalysisGridTransformCapacityError("source pixel limit exceeded")
    if target_pixels > config.max_target_pixels:
        raise AnalysisGridTransformCapacityError("target pixel limit exceeded")
    source_array_bytes = sum(
        array.nbytes
        for array in (
            source.source_values,
            source.rain_rate_mm_h,
            source.valid_mask,
            source.nodata_mask,
            source.undetect_mask,
            *(plane.values for plane in source.auxiliary_planes),
        )
    )
    if source_array_bytes > config.max_array_bytes:
        raise AnalysisGridTransformCapacityError("source array byte limit exceeded")
    binding_issue = _validate_binding(binding)
    if binding_issue is not None:
        return AnalysisGridTransformResult(source.candidate_id, None, (binding_issue,))

    target_grid.assert_round_trip(tolerance_m=config.roundtrip_tolerance_m)
    certificate = transform_adapter.certificate
    if not isinstance(certificate, AffineGridTransformCertificate):
        return AnalysisGridTransformResult(
            source.candidate_id,
            None,
            (_issue(
                AnalysisGridTransformIssueState.INVALID,
                "coordinate_certificate_type_invalid",
                "adapter did not expose the closed affine certificate type",
            ),),
        )
    certificate_issue = _validate_certificate(source, certificate, target_grid, config)
    if certificate_issue is not None:
        return AnalysisGridTransformResult(
            source.candidate_id, None, (certificate_issue,)
        )
    _check_deadline(active_deadline, monotonic)
    try:
        receipt = transform_adapter.issue_receipt(
            binding=binding,
            target_grid=target_grid,
            config=config,
            deadline_at=active_deadline,
            monotonic=monotonic,
        )
    except AnalysisGridTransformDeadlineError:
        raise
    except (RuntimeError, TypeError, ValueError) as exc:
        return AnalysisGridTransformResult(
            source.candidate_id,
            None,
            (_issue(
                AnalysisGridTransformIssueState.INVALID,
                "coordinate_transform_receipt_failed",
                f"adapter did not issue a valid receipt: {type(exc).__name__}",
            ),),
        )
    if not isinstance(receipt, CertifiedCoordinateTransformReceipt):
        return AnalysisGridTransformResult(
            source.candidate_id,
            None,
            (_issue(
                AnalysisGridTransformIssueState.INVALID,
                "coordinate_transform_receipt_type_invalid",
                "adapter receipt has the wrong type",
            ),),
        )
    receipt_issue = _validate_receipt(
        receipt, binding, target_grid, certificate, config
    )
    if receipt_issue is not None:
        return AnalysisGridTransformResult(source.candidate_id, None, (receipt_issue,))
    _check_deadline(active_deadline, monotonic)

    qc_count = len(source.auxiliary_planes)
    estimated_bytes = (
        target_pixels * (8 + 5 + 4 * 8)
        + sum(
            target_pixels * (plane.values.dtype.itemsize + 1)
            + plane.values.nbytes
            for plane in source.auxiliary_planes
        )
    )
    if source_array_bytes + estimated_bytes > config.max_array_bytes:
        raise AnalysisGridTransformCapacityError(
            "source-plus-output array byte limit exceeded"
        )

    shape = (target_grid.height, target_grid.width)
    rates = np.full(shape, np.nan, dtype=np.float64)
    valid_mask = np.zeros(shape, dtype=np.bool_)
    nodata_mask = np.zeros(shape, dtype=np.bool_)
    undetect_mask = np.zeros(shape, dtype=np.bool_)
    mixed_mask = np.zeros(shape, dtype=np.bool_)
    outside_mask = np.zeros(shape, dtype=np.bool_)
    valid_fraction = np.zeros(shape, dtype=np.float64)
    nodata_fraction = np.zeros(shape, dtype=np.float64)
    undetect_fraction = np.zeros(shape, dtype=np.float64)
    outside_fraction = np.zeros(shape, dtype=np.float64)
    qc_values = [np.zeros(shape, dtype=plane.values.dtype) for plane in source.auxiliary_planes]
    qc_consensus = [np.zeros(shape, dtype=np.bool_) for _ in source.auxiliary_planes]

    source_west, source_south, source_east, source_north = (
        receipt.source_support_edges_target_m
    )
    sx = certificate.source_x_resolution_target_m
    sy = certificate.source_y_resolution_target_m
    tolerance = config.area_fraction_tolerance
    contributions = 0
    valid_area_total = 0.0
    nodata_area_total = 0.0
    undetect_area_total = 0.0
    outside_area_total = 0.0
    valid_integral = 0.0
    target_area = target_grid.pixel_area_m2

    for target_row in range(target_grid.height):
        _check_deadline(active_deadline, monotonic)
        top = target_grid.origin_y_m - target_row * target_grid.y_resolution_m
        bottom = top - target_grid.y_resolution_m
        clipped_top = min(top, source_north)
        clipped_bottom = max(bottom, source_south)
        if clipped_bottom < clipped_top:
            row_start = max(0, math.floor((source_north - clipped_top) / sy))
            row_end = min(
                source.native_grid.height - 1,
                math.ceil((source_north - clipped_bottom) / sy) - 1,
            )
        else:
            row_start, row_end = 1, 0
        for target_column in range(target_grid.width):
            left = target_grid.origin_x_m + target_column * target_grid.x_resolution_m
            right = left + target_grid.x_resolution_m
            clipped_left = max(left, source_west)
            clipped_right = min(right, source_east)
            if clipped_left < clipped_right and row_start <= row_end:
                column_start = max(0, math.floor((clipped_left - source_west) / sx))
                column_end = min(
                    source.native_grid.width - 1,
                    math.ceil((clipped_right - source_west) / sx) - 1,
                )
            else:
                column_start, column_end = 1, 0

            # Keep overlap accounting constant-memory. A single coarse target
            # pixel may intersect millions of source pixels; retaining one
            # Python object per overlap would bypass the ndarray byte budget.
            support_area = 0.0
            area_valid = 0.0
            area_nodata = 0.0
            area_undetect = 0.0
            weighted_rate = 0.0
            cell_has_contributors = False
            qc_first_values: list[Any | None] = [None] * qc_count
            qc_finite = [True] * qc_count
            qc_equal = [True] * qc_count
            for source_row in range(row_start, row_end + 1):
                src_top = source_north - source_row * sy
                src_bottom = src_top - sy
                overlap_y = min(top, src_top) - max(bottom, src_bottom)
                if overlap_y <= 0.0:
                    continue
                for source_column in range(column_start, column_end + 1):
                    src_left = source_west + source_column * sx
                    src_right = src_left + sx
                    overlap_x = min(right, src_right) - max(left, src_left)
                    if overlap_x <= 0.0:
                        continue
                    area = overlap_x * overlap_y
                    contributions += 1
                    if contributions > config.max_overlap_contributions:
                        raise AnalysisGridTransformCapacityError(
                            "overlap contribution limit exceeded"
                        )
                    if contributions % DEADLINE_CHECK_OVERLAP_INTERVAL == 0:
                        _check_deadline(active_deadline, monotonic)
                    support_area = math.fsum((support_area, area))
                    cell_has_contributors = True
                    if source.valid_mask[source_row, source_column]:
                        area_valid = math.fsum((area_valid, area))
                        weighted_rate = math.fsum(
                            (
                                weighted_rate,
                                float(source.rain_rate_mm_h[
                                    source_row, source_column
                                ]) * area,
                            )
                        )
                    elif source.nodata_mask[source_row, source_column]:
                        area_nodata = math.fsum((area_nodata, area))
                    elif source.undetect_mask[source_row, source_column]:
                        area_undetect = math.fsum((area_undetect, area))
                    else:  # Native candidate invariant should make this unreachable.
                        raise AnalysisGridTransformError(
                            "source masks changed during transform"
                        )
                    for index, plane in enumerate(source.auxiliary_planes):
                        value = plane.values[source_row, source_column]
                        if isinstance(value, (float, np.floating)) and not math.isfinite(
                            float(value)
                        ):
                            qc_finite[index] = False
                        if qc_first_values[index] is None:
                            qc_first_values[index] = value
                        elif not bool(
                            np.asarray(value == qc_first_values[index]).item()
                        ):
                            qc_equal[index] = False

            area_outside = max(0.0, target_area - support_area)
            raw_fractions = (
                area_valid / target_area,
                area_nodata / target_area,
                area_undetect / target_area,
                area_outside / target_area,
            )
            total_fraction = math.fsum(raw_fractions)
            if abs(total_fraction - 1.0) > max(tolerance, 1e-12):
                raise AnalysisGridTransformError(
                    "area-overlap accounting is not exhaustive"
                )
            # Normalize a sub-ulp geometric closure deterministically, then put
            # the final residual in the largest component so all four remain
            # within [0, 1] and sum exactly under the canonical order.
            normalized = [max(0.0, item / total_fraction) for item in raw_fractions]
            largest = max(range(4), key=lambda index: (normalized[index], -index))
            normalized[largest] += 1.0 - math.fsum(normalized)
            fractions = tuple(normalized)
            valid_fraction[target_row, target_column] = fractions[0]
            nodata_fraction[target_row, target_column] = fractions[1]
            undetect_fraction[target_row, target_column] = fractions[2]
            outside_fraction[target_row, target_column] = fractions[3]
            valid_area_total += area_valid
            nodata_area_total += area_nodata
            undetect_area_total += area_undetect
            outside_area_total += area_outside

            if fractions[3] > tolerance:
                outside_mask[target_row, target_column] = True
            elif fractions[0] >= 1.0 - tolerance:
                valid_mask[target_row, target_column] = True
                rate = weighted_rate / target_area
                rates[target_row, target_column] = rate
                valid_integral += rate * target_area
            elif fractions[1] >= 1.0 - tolerance:
                nodata_mask[target_row, target_column] = True
            elif fractions[2] >= 1.0 - tolerance:
                undetect_mask[target_row, target_column] = True
            else:
                mixed_mask[target_row, target_column] = True

            if fractions[3] <= tolerance and cell_has_contributors:
                for index in range(qc_count):
                    first = qc_first_values[index]
                    if qc_finite[index] and qc_equal[index] and first is not None:
                        qc_values[index][target_row, target_column] = first
                        qc_consensus[index][target_row, target_column] = True

    _check_deadline(active_deadline, monotonic)
    final_source_sha = native_candidate_content_sha256(source)
    if final_source_sha != binding.candidate_content_sha256:
        return AnalysisGridTransformResult(
            source.candidate_id,
            None,
            (_issue(
                AnalysisGridTransformIssueState.INVALID,
                "source_candidate_changed_during_transform",
                "source candidate checksum changed while resampling",
            ),),
        )

    arrays = (
        rates,
        valid_mask,
        nodata_mask,
        undetect_mask,
        mixed_mask,
        outside_mask,
        valid_fraction,
        nodata_fraction,
        undetect_fraction,
        outside_fraction,
        *qc_values,
        *qc_consensus,
    )
    actual_bytes = sum(array.nbytes for array in arrays) + sum(
        plane.values.nbytes for plane in source.auxiliary_planes
    )
    if source_array_bytes + actual_bytes > config.max_array_bytes:
        raise AnalysisGridTransformCapacityError(
            "materialized source-plus-output arrays exceed byte limit"
        )
    immutable_arrays = [_freeze_owned(array) for array in arrays]
    (
        rates_ro,
        valid_ro,
        nodata_ro,
        undetect_ro,
        mixed_ro,
        outside_ro,
        valid_fraction_ro,
        nodata_fraction_ro,
        undetect_fraction_ro,
        outside_fraction_ro,
        *qc_immutable,
    ) = immutable_arrays
    qc_values_ro = qc_immutable[:qc_count]
    qc_consensus_ro = qc_immutable[qc_count:]
    source_planes = tuple(
        NativeRasterPlane(
            name=plane.name,
            semantics=plane.semantics,
            values=_readonly_copy(plane.values),
        )
        for plane in source.auxiliary_planes
    )
    rules = {rule.plane_name: rule for rule in certificate.quality_plane_rules}
    quality_planes = tuple(
        AnalysisQualityPlane(
            name=plane.name,
            source_semantics=plane.semantics,
            source_values_sha256=_array_receipt(plane.values)["sha256"],
            method=rules[plane.name].method,
            values=qc_values_ro[index],
            consensus_mask=qc_consensus_ro[index],
        )
        for index, plane in enumerate(source.auxiliary_planes)
    )
    accounting = TransformPixelAccounting(
        total_target_pixels=target_pixels,
        valid_pixels=int(np.count_nonzero(valid_ro)),
        nodata_pixels=int(np.count_nonzero(nodata_ro)),
        undetect_pixels=int(np.count_nonzero(undetect_ro)),
        mixed_pixels=int(np.count_nonzero(mixed_ro)),
        outside_support_pixels=int(np.count_nonzero(outside_ro)),
        overlap_contributions=contributions,
        valid_source_overlap_area_m2=valid_area_total,
        nodata_source_overlap_area_m2=nodata_area_total,
        undetect_source_overlap_area_m2=undetect_area_total,
        outside_support_area_m2=outside_area_total,
        target_area_m2=target_pixels * target_area,
        valid_rate_area_integral_mm_h_m2=valid_integral,
    )
    state_masks = {
        "mixed": mixed_ro,
        "nodata": nodata_ro,
        "outside_support": outside_ro,
        "undetect": undetect_ro,
        "valid": valid_ro,
    }
    fraction_arrays = {
        "nodata": nodata_fraction_ro,
        "outside_support": outside_fraction_ro,
        "undetect": undetect_fraction_ro,
        "valid": valid_fraction_ro,
    }
    payload = _output_payload(
        source=source,
        binding=binding,
        target_grid=target_grid,
        certificate=certificate,
        receipt=receipt,
        config=config,
        rates=rates_ro,
        state_masks=state_masks,
        fractions=fraction_arrays,
        source_planes=source_planes,
        quality_planes=quality_planes,
        accounting=accounting,
    )
    output_sha = _sha_mapping(payload)
    field = AnalysisGridRainRateFieldCandidate(
        field_id="analysis-field-" + output_sha,
        output_content_sha256=output_sha,
        source_candidate_id=source.candidate_id,
        source_candidate_content_sha256=binding.candidate_content_sha256,
        source_semantics_certificate_id=source.semantics_certificate_id,
        source_semantics_certificate_sha256=binding.semantics_certificate_sha256,
        transform_certificate=certificate,
        transform_certificate_id=certificate.certificate_id,
        transform_certificate_sha256=certificate.fingerprint,
        transform_receipt=receipt,
        transform_receipt_id=receipt.receipt_id,
        transform_receipt_sha256=receipt.fingerprint,
        transform_config=config,
        transform_config_sha256=config.fingerprint,
        target_grid=target_grid,
        target_grid_sha256=analysis_grid_sha256(target_grid),
        resampling_method=config.resampling_method,
        rain_rate_mm_h=rates_ro,
        valid_mask=valid_ro,
        nodata_mask=nodata_ro,
        undetect_mask=undetect_ro,
        mixed_mask=mixed_ro,
        outside_support_mask=outside_ro,
        valid_area_fraction=valid_fraction_ro,
        nodata_area_fraction=nodata_fraction_ro,
        undetect_area_fraction=undetect_fraction_ro,
        outside_support_area_fraction=outside_fraction_ro,
        source_auxiliary_planes=source_planes,
        analysis_quality_planes=quality_planes,
        pixel_accounting=accounting,
        output_quantity=source.output_quantity,
        output_unit=source.output_unit,
        temporal_semantics=source.temporal_semantics.value,
        support_start=source.support_start,
        support_end=source.support_end,
        valid_at=source.valid_at,
        reprojection_applied=source.native_crs != target_grid.crs,
    )
    if analysis_grid_field_content_sha256(field) != output_sha:
        raise AnalysisGridTransformError(
            "analysis-grid output checksum implementation is inconsistent"
        )
    _check_deadline(active_deadline, monotonic)
    return AnalysisGridTransformResult(source.candidate_id, field, ())


__all__ = [
    "ANALYSIS_GRID_TRANSFORM_ADMISSION_STATUS",
    "ANALYSIS_GRID_TRANSFORM_ALGORITHM",
    "AffineGridTransformCertificate",
    "AnalysisGridRainRateFieldCandidate",
    "AnalysisGridResamplingMethod",
    "AnalysisGridTransformCapacityError",
    "AnalysisGridTransformConfig",
    "AnalysisGridTransformDeadlineError",
    "AnalysisGridTransformError",
    "AnalysisGridTransformIssue",
    "AnalysisGridTransformIssueState",
    "AnalysisGridTransformResult",
    "AnalysisQualityPlane",
    "CertifiedCoordinateTransformAdapter",
    "CertifiedCoordinateTransformReceipt",
    "CoordinateRoundTripSample",
    "ExactAffineGridTransformAdapter",
    "NativeFieldCandidateBinding",
    "QualityPlaneResamplingMethod",
    "QualityPlaneTransformRule",
    "TransformEvidenceClass",
    "TransformPixelAccounting",
    "analysis_grid_sha256",
    "analysis_grid_field_content_sha256",
    "native_candidate_content_sha256",
    "transform_native_rain_rate_to_analysis_grid",
]
