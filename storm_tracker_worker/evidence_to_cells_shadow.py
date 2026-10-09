"""Unwired canonical-evidence to lossless-cell compositor.

This module is deliberately a shadow-only composition boundary.  Its only
meteorological input identity is a committed canonical ``run_id``.  Provider
arrays can therefore enter only through the canonical evidence store, the
verified radar artifact authority and the certified analysis-grid transform.

No result from this module is an admission, a wet/dry statement, a forecast or
an operational publication.  Detector thresholds are caller-supplied object
definition parameters.  Every analysis-grid pixel remains represented by an
immutable audit record, including mixed, nodata, undetect and outside-support
states that are withheld from the detector.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import time
from dataclasses import dataclass
from enum import Enum
from types import SimpleNamespace
from typing import Any, Callable, Sequence

import numpy as np

from .analysis_grid_transform import (
    AnalysisGridTransformCapacityError,
    AnalysisGridTransformConfig,
    AnalysisGridTransformDeadlineError,
    CertifiedCoordinateTransformAdapter,
    NativeFieldCandidateBinding,
    analysis_grid_field_content_sha256,
    analysis_grid_sha256,
    native_candidate_content_sha256,
    transform_native_rain_rate_to_analysis_grid,
)
from .canonical_evidence_store import CanonicalEvidenceStore
from .canonical_radar_field_bridge import (
    RadarDerivedArtifactAuthority,
    RadarFieldBridgeCapacityError,
    RadarFieldBridgeDeadlineError,
    RadarFieldSemanticsCertificate,
    derive_canonical_radar_field,
)
from .tracking_core.engine.analysis_grid import AnalysisGrid
from .tracking_core.engine.hierarchical_detection import (
    HIERARCHICAL_DETECTOR_VERSION,
    HierarchicalCellDetection,
    HierarchicalDetectionCapacityError,
    HierarchicalDetectionDeadlineError,
    HierarchicalDetectionFrame,
    HierarchicalDetectorConfig,
    NormalizedRasterFrame,
    detect_hierarchical_cells,
)


EVIDENCE_TO_CELLS_SHADOW_ALGORITHM = "stv3-evidence-to-cells-shadow-v1"
EVIDENCE_TO_CELLS_ADMISSION_STATUS = "shadow_candidate_unadmitted"
HARD_MAX_TARGET_PIXELS = 250_000
HARD_MAX_DETECTION_OBJECTS = 500_000
HARD_MAX_RECEIPT_BYTES = 64 * 1024 * 1024
HARD_MAX_RUNTIME_SECONDS = 120.0
PIXEL_DEADLINE_INTERVAL = 1_024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID_RE = re.compile(r"^run-[0-9a-f]{64}$")


class EvidenceToCellsShadowError(RuntimeError):
    """Base failure at the isolated composition boundary."""


class EvidenceToCellsShadowDeadlineError(EvidenceToCellsShadowError):
    """The caller's single absolute deadline was reached."""


class EvidenceToCellsShadowCapacityError(EvidenceToCellsShadowError):
    """A caller or hard resource boundary was exceeded."""


class EvidenceToCellsShadowIntegrityError(EvidenceToCellsShadowError):
    """A supposedly immutable upstream or derived fact changed."""


class PixelDisposition(str, Enum):
    """Exhaustive analysis-pixel disposition at the detector boundary."""

    VALID_BELOW_SHIELD = "valid_detector_input_below_shield_threshold"
    VALID_RAIN_SHIELD = "valid_detector_input_rain_shield"
    EXCLUDED_NODATA = "excluded_nodata"
    EXCLUDED_UNDETECT = "excluded_undetect"
    EXCLUDED_MIXED = "excluded_mixed"
    EXCLUDED_OUTSIDE_SUPPORT = "excluded_outside_support"


@dataclass(frozen=True, slots=True)
class EvidenceToCellsShadowConfig:
    """Pipeline-only capacity and deadline limits, all caller-visible."""

    max_target_pixels: int = 100_000
    max_detection_objects: int = 200_000
    max_receipt_bytes: int = 32 * 1024 * 1024
    max_runtime_seconds: float = 30.0

    def __post_init__(self) -> None:
        for label, value, hard in (
            ("max_target_pixels", self.max_target_pixels, HARD_MAX_TARGET_PIXELS),
            (
                "max_detection_objects",
                self.max_detection_objects,
                HARD_MAX_DETECTION_OBJECTS,
            ),
            ("max_receipt_bytes", self.max_receipt_bytes, HARD_MAX_RECEIPT_BYTES),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 1
                or value > hard
            ):
                raise ValueError(f"{label} is outside the hard boundary")
        if self.max_receipt_bytes < 1_024:
            raise ValueError("max_receipt_bytes must be at least 1024")
        if (
            isinstance(self.max_runtime_seconds, bool)
            or not isinstance(self.max_runtime_seconds, (int, float))
            or not math.isfinite(float(self.max_runtime_seconds))
            or not 0.0 < float(self.max_runtime_seconds) <= HARD_MAX_RUNTIME_SECONDS
        ):
            raise ValueError("max_runtime_seconds is outside the hard boundary")

    def to_mapping(self) -> dict[str, int | float | str]:
        return {
            "algorithm_version": EVIDENCE_TO_CELLS_SHADOW_ALGORITHM,
            "max_detection_objects": self.max_detection_objects,
            "max_receipt_bytes": self.max_receipt_bytes,
            "max_runtime_seconds": float(self.max_runtime_seconds),
            "max_target_pixels": self.max_target_pixels,
        }

    @property
    def fingerprint(self) -> str:
        return _sha_mapping(self.to_mapping())


@dataclass(frozen=True, slots=True)
class AnalysisPixelAudit:
    """One immutable pixel record; its tuple position is its row/column."""

    disposition: PixelDisposition
    rain_rate_mm_h: float | None
    valid_area_fraction: float
    nodata_area_fraction: float
    undetect_area_fraction: float
    outside_support_area_fraction: float
    rain_shield_detection_id: str | None
    embedded_core_detection_id: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.disposition, PixelDisposition):
            raise TypeError("pixel disposition is invalid")
        fractions = (
            self.valid_area_fraction,
            self.nodata_area_fraction,
            self.undetect_area_fraction,
            self.outside_support_area_fraction,
        )
        if any(
            not math.isfinite(value) or not 0.0 <= value <= 1.0
            for value in fractions
        ) or not math.isclose(sum(fractions), 1.0, rel_tol=0.0, abs_tol=1e-9):
            raise ValueError("pixel area fractions are not exhaustive")
        is_valid = self.disposition in (
            PixelDisposition.VALID_BELOW_SHIELD,
            PixelDisposition.VALID_RAIN_SHIELD,
        )
        if is_valid:
            if (
                self.rain_rate_mm_h is None
                or not math.isfinite(self.rain_rate_mm_h)
                or self.rain_rate_mm_h < 0.0
            ):
                raise ValueError("valid detector input needs a physical rate")
        elif self.rain_rate_mm_h is not None:
            raise ValueError("excluded detector input must not expose a rate")
        if (
            self.disposition is PixelDisposition.VALID_RAIN_SHIELD
        ) != (self.rain_shield_detection_id is not None):
            raise ValueError("rain-shield pixel identity is inconsistent")
        if self.embedded_core_detection_id is not None and (
            self.rain_shield_detection_id is None
        ):
            raise ValueError("an embedded-core pixel must belong to a shield")
        for value in (
            self.rain_shield_detection_id,
            self.embedded_core_detection_id,
        ):
            if value is not None and (not isinstance(value, str) or not value):
                raise ValueError("pixel detection identity is invalid")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "disposition": self.disposition.value,
            "embedded_core_detection_id": self.embedded_core_detection_id,
            "nodata_area_fraction": self.nodata_area_fraction,
            "outside_support_area_fraction": self.outside_support_area_fraction,
            "rain_rate_mm_h": self.rain_rate_mm_h,
            "rain_shield_detection_id": self.rain_shield_detection_id,
            "undetect_area_fraction": self.undetect_area_fraction,
            "valid_area_fraction": self.valid_area_fraction,
        }


@dataclass(frozen=True, slots=True)
class EvidenceToCellsPixelAccounting:
    """Counts that make every mutually exclusive disposition auditable."""

    total_analysis_pixels: int
    valid_detector_input_pixels: int
    valid_below_shield_threshold_pixels: int
    rain_shield_pixels: int
    embedded_core_pixels: int
    excluded_nodata_pixels: int
    excluded_undetect_pixels: int
    excluded_mixed_pixels: int
    excluded_outside_support_pixels: int
    detection_objects: int
    rain_shield_objects: int
    embedded_core_objects: int
    low_confidence_objects: int

    def __post_init__(self) -> None:
        for value in self.to_mapping().values():
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("pixel accounting values must be non-negative ints")
        if self.total_analysis_pixels != (
            self.valid_detector_input_pixels
            + self.excluded_nodata_pixels
            + self.excluded_undetect_pixels
            + self.excluded_mixed_pixels
            + self.excluded_outside_support_pixels
        ):
            raise ValueError("pixel dispositions are not exhaustive")
        if self.valid_detector_input_pixels != (
            self.valid_below_shield_threshold_pixels + self.rain_shield_pixels
        ):
            raise ValueError("valid detector-input accounting is inconsistent")
        if self.embedded_core_pixels > self.rain_shield_pixels:
            raise ValueError("core pixels cannot exceed shield pixels")
        if self.detection_objects != (
            self.rain_shield_objects + self.embedded_core_objects
        ):
            raise ValueError("detection object accounting is inconsistent")
        if self.low_confidence_objects > self.detection_objects:
            raise ValueError("low-confidence count exceeds detection count")

    def to_mapping(self) -> dict[str, int]:
        return {
            field: int(getattr(self, field))
            for field in self.__dataclass_fields__
        }


@dataclass(frozen=True, slots=True)
class EvidenceToCellsShadowIssue:
    stage: str
    state: str
    code: str
    detail: str

    def __post_init__(self) -> None:
        if self.stage not in ("radar_bridge", "analysis_transform", "detector"):
            raise ValueError("shadow issue stage is invalid")
        for label, value, maximum in (
            ("state", self.state, 64),
            ("code", self.code, 128),
            ("detail", self.detail, 1_024),
        ):
            if not isinstance(value, str) or not value or len(value) > maximum:
                raise ValueError(f"shadow issue {label} is invalid")


@dataclass(frozen=True, slots=True)
class EvidenceToCellsShadowCandidate:
    """Content-derived, raw-payload-free output of the complete shadow chain."""

    candidate_id: str
    content_sha256: str
    canonical_run_id: str
    canonical_commit_id: str
    canonical_receipt_sha256: str
    canonical_result_sha256: str
    capture_id: str
    capture_receipt_sha256: str
    capture_manifest_sha256: str
    native_candidate_id: str
    native_candidate_content_sha256: str
    native_lineage_json: str
    native_lineage_sha256: str
    radar_semantics_certificate_id: str
    radar_semantics_certificate_sha256: str
    radar_semantics_certificate_json: str
    analysis_field_id: str
    analysis_field_content_sha256: str
    analysis_lineage_json: str
    analysis_lineage_sha256: str
    transform_certificate_id: str
    transform_certificate_sha256: str
    transform_certificate_json: str
    transform_receipt_id: str
    transform_receipt_sha256: str
    transform_receipt_json: str
    transform_config_sha256: str
    transform_config_json: str
    target_grid_sha256: str
    detector_config_sha256: str
    detector_config_json: str
    pipeline_config_sha256: str
    pipeline_config_json: str
    normalized_frame: NormalizedRasterFrame
    detection_frame: HierarchicalDetectionFrame
    pixels: tuple[tuple[AnalysisPixelAudit, ...], ...]
    pixel_accounting: EvidenceToCellsPixelAccounting
    admission_status: str = EVIDENCE_TO_CELLS_ADMISSION_STATUS
    meteorological_admission_granted: bool = False
    wet_dry_classification_applied: bool = False
    forecast_generated: bool = False
    hard_realtime_deadline_certified: bool = False
    operational_admission_blockers: tuple[str, ...] = (
        "hard_process_isolation_not_wired",
    )
    algorithm_version: str = EVIDENCE_TO_CELLS_SHADOW_ALGORITHM

    def __post_init__(self) -> None:
        for label, value in (
            ("content_sha256", self.content_sha256),
            ("canonical_receipt_sha256", self.canonical_receipt_sha256),
            ("canonical_result_sha256", self.canonical_result_sha256),
            ("capture_receipt_sha256", self.capture_receipt_sha256),
            ("capture_manifest_sha256", self.capture_manifest_sha256),
            ("native_candidate_content_sha256", self.native_candidate_content_sha256),
            ("native_lineage_sha256", self.native_lineage_sha256),
            (
                "radar_semantics_certificate_sha256",
                self.radar_semantics_certificate_sha256,
            ),
            ("analysis_field_content_sha256", self.analysis_field_content_sha256),
            ("analysis_lineage_sha256", self.analysis_lineage_sha256),
            ("transform_certificate_sha256", self.transform_certificate_sha256),
            ("transform_receipt_sha256", self.transform_receipt_sha256),
            ("transform_config_sha256", self.transform_config_sha256),
            ("target_grid_sha256", self.target_grid_sha256),
            ("detector_config_sha256", self.detector_config_sha256),
            ("pipeline_config_sha256", self.pipeline_config_sha256),
        ):
            _require_sha(value, label)
        if not _RUN_ID_RE.fullmatch(self.canonical_run_id):
            raise ValueError("candidate canonical run identity is invalid")
        if self.candidate_id != "evidence-cells-" + self.content_sha256:
            raise ValueError("shadow candidate identity is not content-derived")
        if self.admission_status != EVIDENCE_TO_CELLS_ADMISSION_STATUS or any(
            (
                self.meteorological_admission_granted,
                self.wet_dry_classification_applied,
                self.forecast_generated,
                self.hard_realtime_deadline_certified,
            )
        ):
            raise ValueError("shadow candidate contains a forbidden claim")
        if self.operational_admission_blockers != (
            "hard_process_isolation_not_wired",
        ):
            raise ValueError("shadow candidate must retain its operational blocker")
        if not isinstance(self.normalized_frame, NormalizedRasterFrame):
            raise TypeError("candidate normalized frame is invalid")
        if not isinstance(self.detection_frame, HierarchicalDetectionFrame):
            raise TypeError("candidate detection frame is invalid")
        if (
            self.detection_frame.frame_id != self.normalized_frame.frame_id
            or self.detection_frame.valid_at != self.normalized_frame.valid_at
            or self.detection_frame.analysis_grid_id
            != self.normalized_frame.grid.grid_id
        ):
            raise ValueError("normalized and detected frames are not bound")
        if len(self.pixels) != self.normalized_frame.grid.height or any(
            len(row) != self.normalized_frame.grid.width for row in self.pixels
        ):
            raise ValueError("pixel audit does not cover the analysis grid")
        if any(
            not isinstance(pixel, AnalysisPixelAudit)
            for row in self.pixels
            for pixel in row
        ):
            raise TypeError("pixel audit contains an invalid record")
        for text, digest, label in (
            (self.native_lineage_json, self.native_lineage_sha256, "native lineage"),
            (
                self.radar_semantics_certificate_json,
                self.radar_semantics_certificate_sha256,
                "radar semantics certificate",
            ),
            (
                self.analysis_lineage_json,
                self.analysis_lineage_sha256,
                "analysis lineage",
            ),
            (
                self.transform_certificate_json,
                self.transform_certificate_sha256,
                "transform certificate",
            ),
            (
                self.transform_receipt_json,
                self.transform_receipt_sha256,
                "transform receipt",
            ),
            (self.transform_config_json, self.transform_config_sha256, "transform config"),
            (self.detector_config_json, self.detector_config_sha256, "detector config"),
            (self.pipeline_config_json, self.pipeline_config_sha256, "pipeline config"),
        ):
            _validate_canonical_json(text, digest, label)
        if self.target_grid_sha256 != analysis_grid_sha256(
            self.normalized_frame.grid
        ):
            raise ValueError("candidate target-grid checksum is inconsistent")
        if self.analysis_field_id != (
            "analysis-field-" + self.analysis_field_content_sha256
        ):
            raise ValueError("candidate analysis-field identity is inconsistent")
        if self.transform_receipt_id != (
            "transform-receipt-" + self.transform_receipt_sha256
        ):
            raise ValueError("candidate transform-receipt identity is inconsistent")
        native_document = json.loads(self.native_lineage_json)
        analysis_document = json.loads(self.analysis_lineage_json)
        semantics_document = json.loads(self.radar_semantics_certificate_json)
        certificate_document = json.loads(self.transform_certificate_json)
        receipt_document = json.loads(self.transform_receipt_json)
        if (
            native_document.get("candidate_id") != self.native_candidate_id
            or native_document.get("candidate_content_sha256")
            != self.native_candidate_content_sha256
            or native_document.get("canonical", {}).get("run_id")
            != self.canonical_run_id
            or native_document.get("canonical", {}).get("commit_id")
            != self.canonical_commit_id
            or native_document.get("canonical", {}).get("receipt_sha256")
            != self.canonical_receipt_sha256
            or native_document.get("canonical", {}).get("result_sha256")
            != self.canonical_result_sha256
            or native_document.get("capture", {}).get("capture_id")
            != self.capture_id
            or native_document.get("capture", {}).get("receipt_sha256")
            != self.capture_receipt_sha256
            or native_document.get("capture", {}).get("manifest_sha256")
            != self.capture_manifest_sha256
            or native_document.get("semantics_certificate_id")
            != self.radar_semantics_certificate_id
            or native_document.get("semantics_certificate_sha256")
            != self.radar_semantics_certificate_sha256
        ):
            raise ValueError("native lineage is not bound to candidate provenance")
        if (
            analysis_document.get("field_id") != self.analysis_field_id
            or analysis_document.get("output_content_sha256")
            != self.analysis_field_content_sha256
            or analysis_document.get("target_grid_sha256")
            != self.target_grid_sha256
            or analysis_document.get("source_candidate_id")
            != self.native_candidate_id
            or analysis_document.get("source_candidate_content_sha256")
            != self.native_candidate_content_sha256
            or analysis_document.get("source_semantics_certificate_id")
            != self.radar_semantics_certificate_id
            or analysis_document.get("source_semantics_certificate_sha256")
            != self.radar_semantics_certificate_sha256
            or analysis_document.get("transform_certificate_id")
            != self.transform_certificate_id
            or analysis_document.get("transform_certificate_sha256")
            != self.transform_certificate_sha256
            or analysis_document.get("transform_config_sha256")
            != self.transform_config_sha256
            or analysis_document.get("transform_receipt_id")
            != self.transform_receipt_id
            or analysis_document.get("transform_receipt_sha256")
            != self.transform_receipt_sha256
            or semantics_document.get("certificate_id")
            != self.radar_semantics_certificate_id
            or semantics_document.get("provider")
            != native_document.get("provider")
            or semantics_document.get("product")
            != native_document.get("product")
            or semantics_document.get("decoder_identity")
            != native_document.get("decoder", {}).get("identity")
            or semantics_document.get("decoder_version")
            != native_document.get("decoder", {}).get("version")
            or semantics_document.get("decoder_config_sha256")
            != native_document.get("decoder", {}).get("config_sha256")
            or semantics_document.get("receipt_quantity")
            != native_document.get("source", {}).get("quantity")
            or semantics_document.get("receipt_unit")
            != native_document.get("source", {}).get("unit")
            or semantics_document.get("output_quantity")
            != native_document.get("output", {}).get("quantity")
            or semantics_document.get("output_unit")
            != native_document.get("output", {}).get("unit")
            or semantics_document.get("temporal_semantics")
            != native_document.get("temporal", {}).get("semantics")
            or native_document.get("artifact", {}).get("handle")
            != (
                str(semantics_document.get("artifact_handle_prefix", ""))
                + str(native_document.get("artifact", {}).get("sha256", ""))
            )
            or analysis_document.get("output_quantity")
            != native_document.get("output", {}).get("quantity")
            or analysis_document.get("output_unit")
            != native_document.get("output", {}).get("unit")
            or analysis_document.get("temporal", {}).get("semantics")
            != native_document.get("temporal", {}).get("semantics")
            or certificate_document.get("certificate_id")
            != self.transform_certificate_id
            or certificate_document.get("transform_config_sha256")
            != self.transform_config_sha256
            or certificate_document.get("source_semantics_certificate_id")
            != self.radar_semantics_certificate_id
            or certificate_document.get("source_semantics_certificate_sha256")
            != self.radar_semantics_certificate_sha256
            or certificate_document.get("source_provider")
            != native_document.get("provider")
            or certificate_document.get("source_product")
            != native_document.get("product")
            or certificate_document.get("target_crs")
            != self.normalized_frame.grid.crs
            or receipt_document.get("certificate_id")
            != self.transform_certificate_id
            or receipt_document.get("certificate_sha256")
            != self.transform_certificate_sha256
            or receipt_document.get("adapter_id")
            != certificate_document.get("adapter_id")
            or receipt_document.get("adapter_version")
            != certificate_document.get("adapter_version")
            or receipt_document.get("source_candidate_id")
            != self.native_candidate_id
            or receipt_document.get("source_candidate_content_sha256")
            != self.native_candidate_content_sha256
            or receipt_document.get("source_semantics_certificate_sha256")
            != self.radar_semantics_certificate_sha256
            or receipt_document.get("transform_config_sha256")
            != self.transform_config_sha256
            or receipt_document.get("target_grid_sha256")
            != self.target_grid_sha256
            or receipt_document.get("target_crs")
            != self.normalized_frame.grid.crs
        ):
            raise ValueError("analysis certificates/receipts are not cross-bound")
        _validate_candidate_pixel_contract(self)
        if evidence_to_cells_content_sha256(self) != self.content_sha256:
            raise ValueError("shadow candidate content checksum is inconsistent")


@dataclass(frozen=True, slots=True)
class EvidenceToCellsShadowResult:
    run_id: str
    candidate: EvidenceToCellsShadowCandidate | None
    issues: tuple[EvidenceToCellsShadowIssue, ...]

    @property
    def is_candidate(self) -> bool:
        return self.candidate is not None and not self.issues


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _sha_mapping(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _streaming_json_sha256(
    value: Any,
    *,
    max_bytes: int | None = None,
    deadline_at: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> tuple[str, int]:
    """Hash canonical JSON without allocating a second complete byte buffer."""

    encoder = json.JSONEncoder(
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    digest = hashlib.sha256()
    total = 0
    for index, chunk in enumerate(encoder.iterencode(value), start=1):
        encoded = chunk.encode("ascii")
        total += len(encoded)
        if max_bytes is not None and total > max_bytes:
            raise EvidenceToCellsShadowCapacityError(
                "shadow result receipt exceeds the configured byte boundary"
            )
        if deadline_at is not None and index % PIXEL_DEADLINE_INTERVAL == 0:
            _check_deadline(deadline_at, monotonic)
        digest.update(encoded)
    if deadline_at is not None:
        _check_deadline(deadline_at, monotonic)
    return digest.hexdigest(), total


def _require_sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{label} is not a lowercase SHA-256")
    return value


def _canonical_json_text(value: Any) -> tuple[str, str]:
    payload = _canonical_json(value)
    return payload.decode("ascii"), hashlib.sha256(payload).hexdigest()


def _validate_canonical_json(text: str, digest: str, label: str) -> None:
    if not isinstance(text, str):
        raise TypeError(f"{label} JSON must be text")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} JSON is invalid") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{label} JSON must contain an object")
    payload = _canonical_json(parsed)
    if payload.decode("ascii") != text or hashlib.sha256(payload).hexdigest() != digest:
        raise ValueError(f"{label} JSON checksum/canonical form differs")


def _array_receipt(array: np.ndarray) -> dict[str, Any]:
    value = np.ascontiguousarray(np.asarray(array))
    if value.dtype.hasobject:
        raise ValueError("object arrays cannot be receipt-bound")
    return {
        "dtype": value.dtype.str,
        "shape": list(value.shape),
        "sha256": hashlib.sha256(value.tobytes(order="C")).hexdigest(),
    }


def _grid_mapping(grid: AnalysisGrid) -> dict[str, Any]:
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


def _detector_config_mapping(config: HierarchicalDetectorConfig) -> dict[str, Any]:
    return {
        "centroid_method": config.centroid_method,
        "connectivity": config.connectivity,
        "core_position_sigma_m": config.core_position_sigma_m,
        "core_threshold_mm_h": config.core_threshold,
        "detector_version": HIERARCHICAL_DETECTOR_VERSION,
        "minimum_core_area_m2": config.minimum_core_area_m2,
        "minimum_shield_area_m2": config.minimum_shield_area_m2,
        "shield_position_sigma_m": config.shield_position_sigma_m,
        "shield_threshold_mm_h": config.shield_threshold,
        "threshold_semantics": "object_definition_only_not_wet_dry_admission",
    }


def _detection_mapping(item: HierarchicalCellDetection) -> dict[str, Any]:
    detection = item.detection
    return {
        "area_m2": item.area_m2,
        "centroid_method": item.centroid_method,
        "detection": {
            "analysis_grid_id": detection.analysis_grid_id,
            "detection_id": detection.detection_id,
            "position_sigma_m": detection.position_sigma_m,
            "source": detection.source,
            "valid_at": detection.valid_at,
            "x_m": detection.x_m,
            "y_m": detection.y_m,
        },
        "footprint_pixels": [list(pixel) for pixel in item.footprint_pixels],
        "geometric_centroid_x_m": item.geometric_centroid_x_m,
        "geometric_centroid_y_m": item.geometric_centroid_y_m,
        "low_confidence_fragment": item.low_confidence_fragment,
        "maximum_value": item.maximum_value,
        "mean_value": item.mean_value,
        "minimum_area_m2": item.minimum_area_m2,
        "object_kind": item.object_kind,
        "parent_detection_id": item.parent_detection_id,
        "threshold": item.threshold,
        "value_weighted_centroid_x_m": item.value_weighted_centroid_x_m,
        "value_weighted_centroid_y_m": item.value_weighted_centroid_y_m,
    }


def _detection_frame_mapping(frame: HierarchicalDetectionFrame) -> dict[str, Any]:
    return {
        "analysis_grid_id": frame.analysis_grid_id,
        "detections": [_detection_mapping(item) for item in frame.detections],
        "detector_version": frame.detector_version,
        "frame_id": frame.frame_id,
        "quantity": frame.quantity,
        "unit": frame.unit,
        "valid_at": frame.valid_at,
    }


def _native_lineage_mapping(candidate: Any) -> dict[str, Any]:
    return {
        "admission_status": candidate.admission_status,
        "algorithm_version": candidate.algorithm_version,
        "analysis_grid": (
            None
            if candidate.analysis_grid is None
            else _grid_mapping(candidate.analysis_grid)
        ),
        "artifact": {
            "handle": candidate.artifact_handle,
            "metadata_sha256": candidate.artifact_metadata_sha256,
            "sha256": candidate.artifact_sha256,
        },
        "arrays": {
            "nodata_mask": _array_receipt(candidate.nodata_mask),
            "rain_rate_mm_h": _array_receipt(candidate.rain_rate_mm_h),
            "source_values": _array_receipt(candidate.source_values),
            "undetect_mask": _array_receipt(candidate.undetect_mask),
            "valid_mask": _array_receipt(candidate.valid_mask),
        },
        "auxiliary_planes": [
            {
                "name": plane.name,
                "semantics": plane.semantics,
                "values": _array_receipt(plane.values),
            }
            for plane in candidate.auxiliary_planes
        ],
        "canonical": {
            "commit_id": candidate.canonical_commit_id,
            "receipt_sha256": candidate.canonical_receipt_sha256,
            "result_sha256": candidate.canonical_result_sha256,
            "run_id": candidate.canonical_run_id,
        },
        "capture": {
            "capture_id": candidate.capture_id,
            "manifest_sha256": candidate.capture_manifest_sha256,
            "receipt_sha256": candidate.capture_receipt_sha256,
        },
        "candidate_content_sha256": native_candidate_content_sha256(candidate),
        "candidate_id": candidate.candidate_id,
        "conversion": candidate.conversion,
        "decoder": {
            "config_sha256": candidate.decoder_config_sha256,
            "identity": candidate.decoder_identity,
            "version": candidate.decoder_version,
        },
        "native_coverage": candidate.native_coverage.to_mapping(),
        "native_crs": candidate.native_crs,
        "native_grid": candidate.native_grid.to_mapping(),
        "metric_raster_field_materialized": (
            candidate.metric_raster_field is not None
        ),
        "operation_flags": {
            "reprojection_applied": candidate.reprojection_applied,
            "resampling_applied": candidate.resampling_applied,
            "thresholding_applied": candidate.thresholding_applied,
            "wet_dry_classification_applied": candidate.wet_dry_classification_applied,
        },
        "output": {
            "quantity": candidate.output_quantity,
            "unit": candidate.output_unit,
        },
        "primary_sources": list(candidate.primary_sources),
        "product": candidate.product,
        "provider": candidate.provider,
        "quality_state": candidate.quality_state,
        "semantics_certificate_id": candidate.semantics_certificate_id,
        "semantics_certificate_sha256": candidate.semantics_certificate_sha256,
        "source": {
            "quantity": candidate.source_quantity,
            "unit": candidate.source_unit,
        },
        "temporal": {
            "semantics": candidate.temporal_semantics.value,
            "support_end": candidate.support_end,
            "support_start": candidate.support_start,
            "valid_at": candidate.valid_at,
        },
    }


def _analysis_lineage_mapping(field: Any) -> dict[str, Any]:
    return {
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
        "source_auxiliary_planes": [
            {
                "name": plane.name,
                "semantics": plane.semantics,
                "values": _array_receipt(plane.values),
            }
            for plane in field.source_auxiliary_planes
        ],
        "arrays": {
            "mixed_mask": _array_receipt(field.mixed_mask),
            "nodata_area_fraction": _array_receipt(field.nodata_area_fraction),
            "nodata_mask": _array_receipt(field.nodata_mask),
            "outside_support_area_fraction": _array_receipt(
                field.outside_support_area_fraction
            ),
            "outside_support_mask": _array_receipt(field.outside_support_mask),
            "rain_rate_mm_h": _array_receipt(field.rain_rate_mm_h),
            "undetect_area_fraction": _array_receipt(field.undetect_area_fraction),
            "undetect_mask": _array_receipt(field.undetect_mask),
            "valid_area_fraction": _array_receipt(field.valid_area_fraction),
            "valid_mask": _array_receipt(field.valid_mask),
        },
        "field_id": field.field_id,
        "operation_flags": {
            "coordinate_transform_applied": field.coordinate_transform_applied,
            "extrapolation_applied": field.extrapolation_applied,
            "reprojection_applied": field.reprojection_applied,
            "resampling_applied": field.resampling_applied,
            "thresholding_applied": field.thresholding_applied,
            "wet_dry_classification_applied": field.wet_dry_classification_applied,
        },
        "output_content_sha256": field.output_content_sha256,
        "output_quantity": field.output_quantity,
        "output_unit": field.output_unit,
        "resampling_method": field.resampling_method.value,
        "source_candidate_content_sha256": field.source_candidate_content_sha256,
        "source_candidate_id": field.source_candidate_id,
        "source_semantics_certificate_id": field.source_semantics_certificate_id,
        "source_semantics_certificate_sha256": (
            field.source_semantics_certificate_sha256
        ),
        "target_grid": _grid_mapping(field.target_grid),
        "target_grid_sha256": field.target_grid_sha256,
        "temporal": {
            "semantics": field.temporal_semantics,
            "support_end": field.support_end,
            "support_start": field.support_start,
            "valid_at": field.valid_at,
        },
        "transform_certificate_id": field.transform_certificate_id,
        "transform_certificate_sha256": field.transform_certificate_sha256,
        "transform_config_sha256": field.transform_config_sha256,
        "transform_receipt_id": field.transform_receipt_id,
        "transform_receipt_sha256": field.transform_receipt_sha256,
    }


def _candidate_payload(
    candidate: Any,
    *,
    deadline_at: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    pixel_rows: list[list[dict[str, Any]]] = []
    processed = 0
    for row in candidate.pixels:
        mapped_row: list[dict[str, Any]] = []
        for pixel in row:
            processed += 1
            if (
                deadline_at is not None
                and processed % PIXEL_DEADLINE_INTERVAL == 0
            ):
                _check_deadline(deadline_at, monotonic)
            mapped_row.append(pixel.to_mapping())
        pixel_rows.append(mapped_row)
    return {
        "admission_status": candidate.admission_status,
        "algorithm_version": candidate.algorithm_version,
        "analysis_field_content_sha256": candidate.analysis_field_content_sha256,
        "analysis_field_id": candidate.analysis_field_id,
        "analysis_lineage_json": candidate.analysis_lineage_json,
        "analysis_lineage_sha256": candidate.analysis_lineage_sha256,
        "canonical": {
            "commit_id": candidate.canonical_commit_id,
            "receipt_sha256": candidate.canonical_receipt_sha256,
            "result_sha256": candidate.canonical_result_sha256,
            "run_id": candidate.canonical_run_id,
        },
        "capture": {
            "capture_id": candidate.capture_id,
            "manifest_sha256": candidate.capture_manifest_sha256,
            "receipt_sha256": candidate.capture_receipt_sha256,
        },
        "detector_config_json": candidate.detector_config_json,
        "detector_config_sha256": candidate.detector_config_sha256,
        "detection_frame": _detection_frame_mapping(candidate.detection_frame),
        "native_candidate_content_sha256": candidate.native_candidate_content_sha256,
        "native_candidate_id": candidate.native_candidate_id,
        "native_lineage_json": candidate.native_lineage_json,
        "native_lineage_sha256": candidate.native_lineage_sha256,
        "normalized_frame": {
            "frame_id": candidate.normalized_frame.frame_id,
            "grid": _grid_mapping(candidate.normalized_frame.grid),
            "quantity": candidate.normalized_frame.quantity,
            "source": candidate.normalized_frame.source,
            "unit": candidate.normalized_frame.unit,
            "valid_at": candidate.normalized_frame.valid_at,
            "values": candidate.normalized_frame.values,
        },
        "operation_flags": {
            "forecast_generated": candidate.forecast_generated,
            "hard_realtime_deadline_certified": (
                candidate.hard_realtime_deadline_certified
            ),
            "meteorological_admission_granted": (
                candidate.meteorological_admission_granted
            ),
            "wet_dry_classification_applied": (
                candidate.wet_dry_classification_applied
            ),
        },
        "operational_admission_blockers": list(
            candidate.operational_admission_blockers
        ),
        "pipeline_config_json": candidate.pipeline_config_json,
        "pipeline_config_sha256": candidate.pipeline_config_sha256,
        "pixel_accounting": candidate.pixel_accounting.to_mapping(),
        "pixels": pixel_rows,
        "radar_semantics_certificate_id": (
            candidate.radar_semantics_certificate_id
        ),
        "radar_semantics_certificate_json": (
            candidate.radar_semantics_certificate_json
        ),
        "radar_semantics_certificate_sha256": (
            candidate.radar_semantics_certificate_sha256
        ),
        "target_grid_sha256": candidate.target_grid_sha256,
        "transform_certificate_id": candidate.transform_certificate_id,
        "transform_certificate_json": candidate.transform_certificate_json,
        "transform_certificate_sha256": candidate.transform_certificate_sha256,
        "transform_config_json": candidate.transform_config_json,
        "transform_config_sha256": candidate.transform_config_sha256,
        "transform_receipt_id": candidate.transform_receipt_id,
        "transform_receipt_json": candidate.transform_receipt_json,
        "transform_receipt_sha256": candidate.transform_receipt_sha256,
    }


def evidence_to_cells_content_sha256(
    candidate: EvidenceToCellsShadowCandidate,
) -> str:
    """Recalculate the complete result identity for tamper auditing."""

    if not isinstance(candidate, EvidenceToCellsShadowCandidate):
        raise TypeError("candidate must be EvidenceToCellsShadowCandidate")
    return _streaming_json_sha256(_candidate_payload(candidate))[0]


def _validate_candidate_pixel_contract(
    candidate: EvidenceToCellsShadowCandidate,
) -> None:
    detector = json.loads(candidate.detector_config_json)
    shield_threshold = float(detector["shield_threshold_mm_h"])
    core_threshold = float(detector["core_threshold_mm_h"])
    detected_frame = candidate.detection_frame
    normalized = candidate.normalized_frame
    if (
        detected_frame.quantity != normalized.quantity
        or detected_frame.unit != normalized.unit
        or detected_frame.detector_version != HIERARCHICAL_DETECTOR_VERSION
        or detector.get("detector_version") != HIERARCHICAL_DETECTOR_VERSION
    ):
        raise ValueError("detector frame semantics differ from normalized input")
    shield_by_pixel: dict[tuple[int, int], str] = {}
    core_by_pixel: dict[tuple[int, int], str] = {}
    for item in detected_frame.detections:
        if item.object_kind == "rain_shield":
            expected_threshold = shield_threshold
            expected_minimum_area = float(detector["minimum_shield_area_m2"])
            expected_sigma = float(detector["shield_position_sigma_m"])
        else:
            expected_threshold = core_threshold
            expected_minimum_area = float(detector["minimum_core_area_m2"])
            expected_sigma = float(detector["core_position_sigma_m"])
        if (
            item.threshold != expected_threshold
            or item.minimum_area_m2 != expected_minimum_area
            or item.centroid_method != detector["centroid_method"]
            or item.detection.position_sigma_m != expected_sigma
            or item.detection.source != normalized.source
        ):
            raise ValueError("detector object differs from explicit detector config")
        target = shield_by_pixel if item.object_kind == "rain_shield" else core_by_pixel
        for pixel in item.footprint_pixels:
            if pixel in target:
                raise ValueError("candidate contains overlapping detector footprints")
            target[pixel] = item.detection.detection_id

    counts = {item: 0 for item in PixelDisposition}
    core_pixels = 0
    for row, audit_row in enumerate(candidate.pixels):
        for column, audit in enumerate(audit_row):
            pixel = (row, column)
            value = candidate.normalized_frame.values[row][column]
            shield_id = shield_by_pixel.get(pixel)
            core_id = core_by_pixel.get(pixel)
            counts[audit.disposition] += 1
            if audit.rain_shield_detection_id != shield_id:
                raise ValueError("pixel shield identity differs from detector footprint")
            if audit.embedded_core_detection_id != core_id:
                raise ValueError("pixel core identity differs from detector footprint")
            if core_id is not None:
                core_pixels += 1
            if audit.rain_rate_mm_h != value:
                raise ValueError("pixel audit rate differs from normalized detector input")
            if value is None:
                if audit.disposition in (
                    PixelDisposition.VALID_BELOW_SHIELD,
                    PixelDisposition.VALID_RAIN_SHIELD,
                ):
                    raise ValueError("excluded frame pixel is marked detector-valid")
            else:
                expected = (
                    PixelDisposition.VALID_RAIN_SHIELD
                    if value >= shield_threshold
                    else PixelDisposition.VALID_BELOW_SHIELD
                )
                if audit.disposition is not expected:
                    raise ValueError("valid frame pixel has the wrong disposition")
                if (core_id is not None) != (value >= core_threshold):
                    raise ValueError("pixel core membership differs from core threshold")

    detected = candidate.detection_frame
    expected = EvidenceToCellsPixelAccounting(
        total_analysis_pixels=sum(counts.values()),
        valid_detector_input_pixels=(
            counts[PixelDisposition.VALID_BELOW_SHIELD]
            + counts[PixelDisposition.VALID_RAIN_SHIELD]
        ),
        valid_below_shield_threshold_pixels=counts[
            PixelDisposition.VALID_BELOW_SHIELD
        ],
        rain_shield_pixels=counts[PixelDisposition.VALID_RAIN_SHIELD],
        embedded_core_pixels=core_pixels,
        excluded_nodata_pixels=counts[PixelDisposition.EXCLUDED_NODATA],
        excluded_undetect_pixels=counts[PixelDisposition.EXCLUDED_UNDETECT],
        excluded_mixed_pixels=counts[PixelDisposition.EXCLUDED_MIXED],
        excluded_outside_support_pixels=counts[
            PixelDisposition.EXCLUDED_OUTSIDE_SUPPORT
        ],
        detection_objects=len(detected.detections),
        rain_shield_objects=len(detected.rain_shields),
        embedded_core_objects=len(detected.embedded_cores),
        low_confidence_objects=len(detected.low_confidence_fragments),
    )
    if expected != candidate.pixel_accounting:
        raise ValueError("candidate pixel accounting differs from its records")


def _check_deadline(deadline_at: float, monotonic: Callable[[], float]) -> None:
    now = float(monotonic())
    if not math.isfinite(now) or now >= deadline_at:
        raise EvidenceToCellsShadowDeadlineError(
            "absolute shadow composition deadline reached"
        )


def _validate_deadline(
    deadline_at: float,
    monotonic: Callable[[], float],
    config: EvidenceToCellsShadowConfig,
) -> None:
    if (
        isinstance(deadline_at, bool)
        or not isinstance(deadline_at, (int, float))
        or not math.isfinite(float(deadline_at))
    ):
        raise ValueError("deadline_at must be a finite absolute monotonic time")
    now = float(monotonic())
    if not math.isfinite(now) or now >= float(deadline_at):
        raise EvidenceToCellsShadowDeadlineError(
            "absolute shadow composition deadline reached"
        )
    if float(deadline_at) - now > float(config.max_runtime_seconds):
        raise ValueError("absolute deadline exceeds the configured runtime bound")


def _issue_result(
    run_id: str,
    *,
    stage: str,
    state: str,
    code: str,
    detail: str,
) -> EvidenceToCellsShadowResult:
    return EvidenceToCellsShadowResult(
        run_id=run_id,
        candidate=None,
        issues=(EvidenceToCellsShadowIssue(stage, state, code, detail),),
    )


def _translate_bridge_issues(run_id: str, issues: Sequence[Any]) -> EvidenceToCellsShadowResult:
    return EvidenceToCellsShadowResult(
        run_id=run_id,
        candidate=None,
        issues=tuple(
            EvidenceToCellsShadowIssue(
                stage="radar_bridge",
                state=issue.state.value,
                code=issue.code,
                detail=issue.detail,
            )
            for issue in issues
        ),
    )


def _translate_transform_issues(
    run_id: str,
    issues: Sequence[Any],
) -> EvidenceToCellsShadowResult:
    return EvidenceToCellsShadowResult(
        run_id=run_id,
        candidate=None,
        issues=tuple(
            EvidenceToCellsShadowIssue(
                stage="analysis_transform",
                state=issue.state.value,
                code=issue.code,
                detail=issue.detail,
            )
            for issue in issues
        ),
    )


def _pixel_audit(
    field: Any,
    detected: HierarchicalDetectionFrame,
    detector_config: HierarchicalDetectorConfig,
    *,
    deadline_at: float,
    monotonic: Callable[[], float],
) -> tuple[
    tuple[tuple[AnalysisPixelAudit, ...], ...],
    EvidenceToCellsPixelAccounting,
]:
    shield_by_pixel: dict[tuple[int, int], str] = {}
    core_by_pixel: dict[tuple[int, int], str] = {}
    for item in detected.detections:
        target = shield_by_pixel if item.object_kind == "rain_shield" else core_by_pixel
        for pixel in item.footprint_pixels:
            if pixel in target:
                raise EvidenceToCellsShadowIntegrityError(
                    "detector emitted overlapping same-kind footprints"
                )
            target[pixel] = item.detection.detection_id

    expected_shields = {
        (row, column)
        for row in range(field.target_grid.height)
        for column in range(field.target_grid.width)
        if field.valid_mask[row, column]
        and float(field.rain_rate_mm_h[row, column])
        >= detector_config.shield_threshold
    }
    expected_cores = {
        (row, column)
        for row in range(field.target_grid.height)
        for column in range(field.target_grid.width)
        if field.valid_mask[row, column]
        and float(field.rain_rate_mm_h[row, column])
        >= detector_config.core_threshold
    }
    if set(shield_by_pixel) != expected_shields or set(core_by_pixel) != expected_cores:
        raise EvidenceToCellsShadowIntegrityError(
            "lossless detector footprint accounting differs from valid thresholds"
        )

    rows: list[tuple[AnalysisPixelAudit, ...]] = []
    counts = {item: 0 for item in PixelDisposition}
    processed = 0
    for row in range(field.target_grid.height):
        audit_row: list[AnalysisPixelAudit] = []
        for column in range(field.target_grid.width):
            processed += 1
            if processed % PIXEL_DEADLINE_INTERVAL == 0:
                _check_deadline(deadline_at, monotonic)
            pixel = (row, column)
            shield_id = shield_by_pixel.get(pixel)
            core_id = core_by_pixel.get(pixel)
            if field.valid_mask[pixel]:
                disposition = (
                    PixelDisposition.VALID_RAIN_SHIELD
                    if shield_id is not None
                    else PixelDisposition.VALID_BELOW_SHIELD
                )
                rate: float | None = float(field.rain_rate_mm_h[pixel])
            elif field.nodata_mask[pixel]:
                disposition = PixelDisposition.EXCLUDED_NODATA
                rate = None
            elif field.undetect_mask[pixel]:
                disposition = PixelDisposition.EXCLUDED_UNDETECT
                rate = None
            elif field.mixed_mask[pixel]:
                disposition = PixelDisposition.EXCLUDED_MIXED
                rate = None
            elif field.outside_support_mask[pixel]:
                disposition = PixelDisposition.EXCLUDED_OUTSIDE_SUPPORT
                rate = None
            else:
                raise EvidenceToCellsShadowIntegrityError(
                    "analysis transform left a pixel without an explicit state"
                )
            counts[disposition] += 1
            audit_row.append(AnalysisPixelAudit(
                disposition=disposition,
                rain_rate_mm_h=rate,
                valid_area_fraction=float(field.valid_area_fraction[pixel]),
                nodata_area_fraction=float(field.nodata_area_fraction[pixel]),
                undetect_area_fraction=float(field.undetect_area_fraction[pixel]),
                outside_support_area_fraction=float(
                    field.outside_support_area_fraction[pixel]
                ),
                rain_shield_detection_id=shield_id,
                embedded_core_detection_id=core_id,
            ))
        rows.append(tuple(audit_row))

    rain_shields = len(detected.rain_shields)
    embedded_cores = len(detected.embedded_cores)
    accounting = EvidenceToCellsPixelAccounting(
        total_analysis_pixels=processed,
        valid_detector_input_pixels=(
            counts[PixelDisposition.VALID_BELOW_SHIELD]
            + counts[PixelDisposition.VALID_RAIN_SHIELD]
        ),
        valid_below_shield_threshold_pixels=counts[
            PixelDisposition.VALID_BELOW_SHIELD
        ],
        rain_shield_pixels=counts[PixelDisposition.VALID_RAIN_SHIELD],
        embedded_core_pixels=len(core_by_pixel),
        excluded_nodata_pixels=counts[PixelDisposition.EXCLUDED_NODATA],
        excluded_undetect_pixels=counts[PixelDisposition.EXCLUDED_UNDETECT],
        excluded_mixed_pixels=counts[PixelDisposition.EXCLUDED_MIXED],
        excluded_outside_support_pixels=counts[
            PixelDisposition.EXCLUDED_OUTSIDE_SUPPORT
        ],
        detection_objects=len(detected.detections),
        rain_shield_objects=rain_shields,
        embedded_core_objects=embedded_cores,
        low_confidence_objects=len(detected.low_confidence_fragments),
    )
    transform = field.pixel_accounting
    if (
        accounting.total_analysis_pixels != transform.total_target_pixels
        or accounting.valid_detector_input_pixels != transform.valid_pixels
        or accounting.excluded_nodata_pixels != transform.nodata_pixels
        or accounting.excluded_undetect_pixels != transform.undetect_pixels
        or accounting.excluded_mixed_pixels != transform.mixed_pixels
        or accounting.excluded_outside_support_pixels
        != transform.outside_support_pixels
    ):
        raise EvidenceToCellsShadowIntegrityError(
            "pixel audit counts differ from transform accounting"
        )
    return tuple(rows), accounting


def compose_evidence_to_cells_shadow(
    evidence_store: CanonicalEvidenceStore,
    artifact_authority: RadarDerivedArtifactAuthority,
    *,
    run_id: str,
    radar_semantics_certificates: Sequence[RadarFieldSemanticsCertificate],
    target_grid: AnalysisGrid,
    transform_adapter: CertifiedCoordinateTransformAdapter,
    transform_config: AnalysisGridTransformConfig,
    detector_config: HierarchicalDetectorConfig,
    pipeline_config: EvidenceToCellsShadowConfig,
    deadline_at: float,
    monotonic: Callable[[], float] = time.monotonic,
) -> EvidenceToCellsShadowResult:
    """Compose committed evidence into lossless cell objects, never admission."""

    if not isinstance(evidence_store, CanonicalEvidenceStore):
        raise TypeError("evidence_store must be CanonicalEvidenceStore")
    if not isinstance(target_grid, AnalysisGrid):
        raise TypeError("target_grid must be AnalysisGrid")
    if not isinstance(transform_config, AnalysisGridTransformConfig):
        raise TypeError("transform_config must be AnalysisGridTransformConfig")
    if not isinstance(detector_config, HierarchicalDetectorConfig):
        raise TypeError("detector_config must be HierarchicalDetectorConfig")
    if not isinstance(pipeline_config, EvidenceToCellsShadowConfig):
        raise TypeError("pipeline_config must be EvidenceToCellsShadowConfig")
    if not isinstance(run_id, str) or _RUN_ID_RE.fullmatch(run_id) is None:
        raise ValueError("run_id must be a canonical run identity")
    if detector_config.shield_threshold <= 0.0:
        raise ValueError(
            "rain-rate shield_threshold must be strictly positive; zero is dry"
        )
    certificates = tuple(radar_semantics_certificates)
    if not certificates or any(
        not isinstance(item, RadarFieldSemanticsCertificate)
        for item in certificates
    ):
        raise ValueError("explicit radar semantics certificates are required")
    if len({(item.certificate_id, item.fingerprint) for item in certificates}) != len(
        certificates
    ):
        raise ValueError("radar semantics certificates must be unique")
    _validate_deadline(deadline_at, monotonic, pipeline_config)
    target_pixels = target_grid.width * target_grid.height
    if target_pixels > pipeline_config.max_target_pixels:
        raise EvidenceToCellsShadowCapacityError(
            "analysis target exceeds the pipeline pixel boundary"
        )
    if target_pixels > transform_config.max_target_pixels:
        raise EvidenceToCellsShadowCapacityError(
            "analysis target exceeds the transform pixel boundary"
        )
    if target_pixels * 2 > pipeline_config.max_detection_objects:
        raise EvidenceToCellsShadowCapacityError(
            "worst-case lossless detector objects exceed the pipeline boundary"
        )
    _check_deadline(deadline_at, monotonic)

    try:
        bridged = derive_canonical_radar_field(
            evidence_store,
            artifact_authority,
            run_id=run_id,
            certificates=certificates,
            materialize_metric_field=False,
            deadline_at=deadline_at,
            monotonic=monotonic,
        )
    except RadarFieldBridgeDeadlineError as exc:
        raise EvidenceToCellsShadowDeadlineError(str(exc)) from exc
    except RadarFieldBridgeCapacityError as exc:
        raise EvidenceToCellsShadowCapacityError(str(exc)) from exc
    if not bridged.is_candidate:
        return _translate_bridge_issues(run_id, bridged.issues)
    native = bridged.candidate
    assert native is not None
    if native.canonical_run_id != run_id:
        raise EvidenceToCellsShadowIntegrityError(
            "radar bridge returned a different canonical run"
        )
    if native.metric_raster_field is not None:
        raise EvidenceToCellsShadowIntegrityError(
            "radar bridge materialized a forbidden redundant metric field"
        )
    native_sha = native_candidate_content_sha256(native)
    matching_certificates = tuple(
        item
        for item in certificates
        if item.certificate_id == native.semantics_certificate_id
        and item.fingerprint == native.semantics_certificate_sha256
    )
    if len(matching_certificates) != 1:
        raise EvidenceToCellsShadowIntegrityError(
            "radar candidate has no unique explicit semantics certificate"
        )
    semantics_certificate = matching_certificates[0]
    _check_deadline(deadline_at, monotonic)

    binding = NativeFieldCandidateBinding(
        candidate=native,
        candidate_content_sha256=native_sha,
        semantics_certificate_sha256=native.semantics_certificate_sha256,
    )
    try:
        transformed = transform_native_rain_rate_to_analysis_grid(
            binding,
            target_grid=target_grid,
            transform_adapter=transform_adapter,
            config=transform_config,
            deadline_at=deadline_at,
            monotonic=monotonic,
        )
    except AnalysisGridTransformDeadlineError as exc:
        raise EvidenceToCellsShadowDeadlineError(str(exc)) from exc
    except AnalysisGridTransformCapacityError as exc:
        raise EvidenceToCellsShadowCapacityError(str(exc)) from exc
    if not transformed.is_candidate:
        return _translate_transform_issues(run_id, transformed.issues)
    field = transformed.field
    assert field is not None
    field_sha = analysis_grid_field_content_sha256(field)
    if field_sha != field.output_content_sha256:
        raise EvidenceToCellsShadowIntegrityError(
            "analysis field checksum differs before detector materialization"
        )
    if native_candidate_content_sha256(native) != native_sha:
        raise EvidenceToCellsShadowIntegrityError(
            "native candidate changed across analysis transform"
        )
    _check_deadline(deadline_at, monotonic)

    detector_mapping = _detector_config_mapping(detector_config)
    detector_json, detector_sha = _canonical_json_text(detector_mapping)
    frame_seed = {
        "analysis_field_content_sha256": field_sha,
        "detector_config_sha256": detector_sha,
        "pipeline_algorithm": EVIDENCE_TO_CELLS_SHADOW_ALGORITHM,
    }
    frame_id = "shadow-frame-" + _sha_mapping(frame_seed)
    value_rows: list[tuple[float | None, ...]] = []
    value_count = 0
    for row in range(target_grid.height):
        values_row: list[float | None] = []
        for column in range(target_grid.width):
            value_count += 1
            if value_count % PIXEL_DEADLINE_INTERVAL == 0:
                _check_deadline(deadline_at, monotonic)
            values_row.append(
                float(field.rain_rate_mm_h[row, column])
                if field.valid_mask[row, column]
                else None
            )
        value_rows.append(tuple(values_row))
    values = tuple(value_rows)
    frame = NormalizedRasterFrame(
        frame_id=frame_id,
        valid_at=field.valid_at,
        source=(
            f"canonical:{native.provider}/{native.product}:"
            f"semantics:{native.semantics_certificate_sha256}:"
            f"transform:{field.transform_certificate_sha256}"
        ),
        quantity=field.output_quantity,
        unit=field.output_unit,
        grid=target_grid,
        values=values,
    )
    try:
        detected = detect_hierarchical_cells(
            frame,
            detector_config,
            deadline_at=deadline_at,
            monotonic=monotonic,
            max_detection_objects=pipeline_config.max_detection_objects,
        )
    except HierarchicalDetectionDeadlineError as exc:
        raise EvidenceToCellsShadowDeadlineError(str(exc)) from exc
    except HierarchicalDetectionCapacityError as exc:
        raise EvidenceToCellsShadowCapacityError(str(exc)) from exc
    except (TypeError, ValueError) as exc:
        return _issue_result(
            run_id,
            stage="detector",
            state="invalid",
            code="hierarchical_detector_failed_closed",
            detail=f"hierarchical detector rejected explicit input: {type(exc).__name__}",
        )
    _check_deadline(deadline_at, monotonic)
    if len(detected.detections) > pipeline_config.max_detection_objects:
        raise EvidenceToCellsShadowCapacityError(
            "detector object count exceeds the pipeline boundary"
        )
    if analysis_grid_field_content_sha256(field) != field_sha:
        raise EvidenceToCellsShadowIntegrityError(
            "analysis field changed while the detector was running"
        )
    pixels, accounting = _pixel_audit(
        field,
        detected,
        detector_config,
        deadline_at=deadline_at,
        monotonic=monotonic,
    )
    _check_deadline(deadline_at, monotonic)

    native_lineage_json, native_lineage_sha = _canonical_json_text(
        _native_lineage_mapping(native)
    )
    analysis_lineage_json, analysis_lineage_sha = _canonical_json_text(
        _analysis_lineage_mapping(field)
    )
    if (
        native_candidate_content_sha256(native) != native_sha
        or analysis_grid_field_content_sha256(field) != field_sha
    ):
        raise EvidenceToCellsShadowIntegrityError(
            "upstream field changed during final lineage materialization"
        )
    semantics_json, semantics_sha = _canonical_json_text(
        semantics_certificate.to_mapping()
    )
    transform_certificate_json, transform_certificate_sha = _canonical_json_text(
        field.transform_certificate.to_mapping()
    )
    transform_receipt_json, transform_receipt_sha = _canonical_json_text(
        field.transform_receipt.payload_mapping()
    )
    transform_config_json, transform_config_sha = _canonical_json_text(
        field.transform_config.to_mapping()
    )
    pipeline_json, pipeline_sha = _canonical_json_text(
        pipeline_config.to_mapping()
    )
    if (
        semantics_sha != native.semantics_certificate_sha256
        or transform_certificate_sha != field.transform_certificate_sha256
        or transform_receipt_sha != field.transform_receipt_sha256
        or transform_config_sha != field.transform_config_sha256
        or pipeline_sha != pipeline_config.fingerprint
    ):
        raise EvidenceToCellsShadowIntegrityError(
            "embedded certificate, receipt or config identity differs"
        )

    candidate_values = {
        "canonical_run_id": native.canonical_run_id,
        "canonical_commit_id": native.canonical_commit_id,
        "canonical_receipt_sha256": native.canonical_receipt_sha256,
        "canonical_result_sha256": native.canonical_result_sha256,
        "capture_id": native.capture_id,
        "capture_receipt_sha256": native.capture_receipt_sha256,
        "capture_manifest_sha256": native.capture_manifest_sha256,
        "native_candidate_id": native.candidate_id,
        "native_candidate_content_sha256": native_sha,
        "native_lineage_json": native_lineage_json,
        "native_lineage_sha256": native_lineage_sha,
        "radar_semantics_certificate_id": semantics_certificate.certificate_id,
        "radar_semantics_certificate_sha256": semantics_sha,
        "radar_semantics_certificate_json": semantics_json,
        "analysis_field_id": field.field_id,
        "analysis_field_content_sha256": field_sha,
        "analysis_lineage_json": analysis_lineage_json,
        "analysis_lineage_sha256": analysis_lineage_sha,
        "transform_certificate_id": field.transform_certificate_id,
        "transform_certificate_sha256": transform_certificate_sha,
        "transform_certificate_json": transform_certificate_json,
        "transform_receipt_id": field.transform_receipt_id,
        "transform_receipt_sha256": transform_receipt_sha,
        "transform_receipt_json": transform_receipt_json,
        "transform_config_sha256": transform_config_sha,
        "transform_config_json": transform_config_json,
        "target_grid_sha256": analysis_grid_sha256(target_grid),
        "detector_config_sha256": detector_sha,
        "detector_config_json": detector_json,
        "pipeline_config_sha256": pipeline_sha,
        "pipeline_config_json": pipeline_json,
        "normalized_frame": frame,
        "detection_frame": detected,
        "pixels": pixels,
        "pixel_accounting": accounting,
    }
    # candidate_id/content_sha256 are intentionally absent from the identity
    # payload.  A private attribute view lets us compute them before invoking
    # the public dataclass constructor, whose __post_init__ rejects forged IDs.
    payload_view = SimpleNamespace(
        **candidate_values,
        admission_status=EVIDENCE_TO_CELLS_ADMISSION_STATUS,
        meteorological_admission_granted=False,
        wet_dry_classification_applied=False,
        forecast_generated=False,
        hard_realtime_deadline_certified=False,
        operational_admission_blockers=(
            "hard_process_isolation_not_wired",
        ),
        algorithm_version=EVIDENCE_TO_CELLS_SHADOW_ALGORITHM,
    )
    payload = _candidate_payload(
        payload_view,
        deadline_at=deadline_at,
        monotonic=monotonic,
    )
    content_sha, _ = _streaming_json_sha256(
        payload,
        max_bytes=pipeline_config.max_receipt_bytes,
        deadline_at=deadline_at,
        monotonic=monotonic,
    )
    final = EvidenceToCellsShadowCandidate(
        **candidate_values,
        candidate_id="evidence-cells-" + content_sha,
        content_sha256=content_sha,
    )
    _check_deadline(deadline_at, monotonic)
    return EvidenceToCellsShadowResult(run_id=run_id, candidate=final, issues=())


__all__ = [
    "EVIDENCE_TO_CELLS_ADMISSION_STATUS",
    "EVIDENCE_TO_CELLS_SHADOW_ALGORITHM",
    "EvidenceToCellsPixelAccounting",
    "EvidenceToCellsShadowCandidate",
    "EvidenceToCellsShadowCapacityError",
    "EvidenceToCellsShadowConfig",
    "EvidenceToCellsShadowDeadlineError",
    "EvidenceToCellsShadowError",
    "EvidenceToCellsShadowIntegrityError",
    "EvidenceToCellsShadowIssue",
    "EvidenceToCellsShadowResult",
    "HARD_MAX_DETECTION_OBJECTS",
    "HARD_MAX_RECEIPT_BYTES",
    "HARD_MAX_RUNTIME_SECONDS",
    "HARD_MAX_TARGET_PIXELS",
    "AnalysisPixelAudit",
    "PixelDisposition",
    "compose_evidence_to_cells_shadow",
    "evidence_to_cells_content_sha256",
]
