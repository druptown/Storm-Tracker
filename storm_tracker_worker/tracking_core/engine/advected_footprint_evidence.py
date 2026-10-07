"""Pure, bounded footprint-overlap evidence under a shared translation.

This module does not associate cells and does not remove candidates.  It
turns one already-computed :class:`SharedSystemTranslationResult` into
auditable pair evidence on an exact :class:`AnalysisGrid`.  A selected shift
is applied only when it is unique, complete, temporally aligned and bound to
the exact translation input frames and grid.  Otherwise the pair remains in
the complete cross product with an explicit unadvected fallback.

The integer shift is descriptive shadow evidence.  No speed, distance or IoU
threshold is used and this module grants no meteorological or operational
admission.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any, Iterable, Literal

from .analysis_grid import AnalysisGrid
from .shared_system_translation import (
    BinaryRainShieldFootprintFrame,
    BinaryShiftTranslationCandidate,
    SharedSystemTranslationResult,
)


ADVECTED_FOOTPRINT_EVIDENCE_VERSION = (
    "stv3-shared-translation-advected-footprint-evidence-v2"
)
HARD_MAX_ADVECTED_FOOTPRINT_PAIRS = 4_000_000

Pixel = tuple[int, int]
AdvectionMode = Literal["advected", "unadvected_fallback"]


class AdvectedFootprintCapacityError(ValueError):
    """The complete pair product does not fit the caller-owned bound."""


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha_mapping(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _sha256(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be lowercase SHA-256")
    return value


def _identifier(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > 512
        or "\0" in value
    ):
        raise ValueError(f"{label} is invalid")
    return value


def _finite(value: object, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise ValueError(f"{label} must be finite")
    return float(value)


def _optional_float_hex(value: float | None) -> str | None:
    return None if value is None else float(value).hex()


def _grid_mapping(grid: AnalysisGrid) -> dict[str, Any]:
    return {
        "axis_order": grid.axis_order,
        "contract_version": grid.contract_version,
        "coordinate_unit": grid.coordinate_unit,
        "crs": grid.crs,
        "grid_id": grid.grid_id,
        "height": grid.height,
        "origin_x_m": float(grid.origin_x_m).hex(),
        "origin_y_m": float(grid.origin_y_m).hex(),
        "row_order": grid.row_order,
        "width": grid.width,
        "x_resolution_m": float(grid.x_resolution_m).hex(),
        "y_resolution_m": float(grid.y_resolution_m).hex(),
    }


def _grid_sha256(grid: AnalysisGrid) -> str:
    return _sha_mapping(_grid_mapping(grid))


@dataclass(frozen=True, slots=True)
class FootprintEvidenceObject:
    """One canonical pair-side object without identity interpretation."""

    detection_id: str
    footprint_pixels: tuple[Pixel, ...]

    def __post_init__(self) -> None:
        _identifier(self.detection_id, "footprint detection_id")
        if not isinstance(self.footprint_pixels, tuple) or not self.footprint_pixels:
            raise ValueError("footprint evidence requires a non-empty tuple")
        canonical: list[Pixel] = []
        seen: set[Pixel] = set()
        for pixel in self.footprint_pixels:
            if (
                not isinstance(pixel, tuple)
                or len(pixel) != 2
                or isinstance(pixel[0], bool)
                or not isinstance(pixel[0], int)
                or isinstance(pixel[1], bool)
                or not isinstance(pixel[1], int)
                or pixel[0] < 0
                or pixel[1] < 0
            ):
                raise ValueError("footprint pixels must be non-negative integer pairs")
            if pixel in seen:
                raise ValueError("footprint evidence contains a duplicate pixel")
            seen.add(pixel)
            canonical.append(pixel)
        object.__setattr__(self, "footprint_pixels", tuple(sorted(canonical)))

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "contract": "stv3-footprint-evidence-object-v1",
            "detection_id": self.detection_id,
            "footprint_pixels": [list(pixel) for pixel in self.footprint_pixels],
        }

    @property
    def footprint_sha256(self) -> str:
        return _sha_mapping(self.payload_mapping())


@dataclass(frozen=True, slots=True)
class AdvectedFootprintEvidence:
    """Exactly accounted overlap evidence for one previous/current pair."""

    evidence_sha256: str
    previous_detection_id: str
    current_detection_id: str
    previous_footprint_sha256: str
    current_footprint_sha256: str
    previous_frame_id: str
    current_frame_id: str
    previous_valid_at: float
    current_valid_at: float
    supplied_previous_frame_sha256: str
    supplied_current_frame_sha256: str
    grid: AnalysisGrid
    grid_sha256: str
    translation_result_sha256: str
    translation_status: str
    translation_previous_frame_id: str
    translation_current_frame_id: str
    translation_previous_frame_sha256: str
    translation_current_frame_sha256: str
    translation_elapsed_seconds: float
    translation_unique_peak: bool | None
    translation_peak_tie_count: int
    translation_search_complete: bool
    translation_uncertainty_reason_codes: tuple[str, ...]
    translation_indeterminate_reason_codes: tuple[str, ...]
    translation_selected_candidate_id: str | None
    mode: AdvectionMode
    reason_codes: tuple[str, ...]
    requested_row_shift_pixels: int | None
    requested_column_shift_pixels: int | None
    requested_displacement_x_m: float | None
    requested_displacement_y_m: float | None
    applied_row_shift_pixels: int
    applied_column_shift_pixels: int
    applied_displacement_x_m: float
    applied_displacement_y_m: float
    quantization_residual_x_m: float | None
    quantization_residual_y_m: float | None
    previous_footprint_pixels: int
    current_footprint_pixels: int
    shifted_inside_grid_pixels: int
    shifted_outside_grid_pixels: int
    raw_intersection_pixels: int
    raw_union_pixels: int
    raw_iou: float
    intersection_pixels: int
    union_pixels: int
    iou: float
    candidate_retained: bool = True
    speed_gate_applied: bool = False
    distance_gate_applied: bool = False
    iou_gate_applied: bool = False
    meteorological_admission_granted: bool = False
    operational_claim_generated: bool = False
    algorithm_version: str = ADVECTED_FOOTPRINT_EVIDENCE_VERSION

    def __post_init__(self) -> None:
        _sha256(self.evidence_sha256, "advected footprint evidence_sha256")
        for value, label in (
            (self.previous_detection_id, "previous_detection_id"),
            (self.current_detection_id, "current_detection_id"),
            (self.previous_frame_id, "previous_frame_id"),
            (self.current_frame_id, "current_frame_id"),
            (
                self.translation_previous_frame_id,
                "translation_previous_frame_id",
            ),
            (
                self.translation_current_frame_id,
                "translation_current_frame_id",
            ),
        ):
            _identifier(value, label)
        for value, label in (
            (self.previous_footprint_sha256, "previous_footprint_sha256"),
            (self.current_footprint_sha256, "current_footprint_sha256"),
            (
                self.supplied_previous_frame_sha256,
                "supplied_previous_frame_sha256",
            ),
            (
                self.supplied_current_frame_sha256,
                "supplied_current_frame_sha256",
            ),
            (self.grid_sha256, "grid_sha256"),
            (self.translation_result_sha256, "translation_result_sha256"),
            (
                self.translation_previous_frame_sha256,
                "translation_previous_frame_sha256",
            ),
            (
                self.translation_current_frame_sha256,
                "translation_current_frame_sha256",
            ),
        ):
            _sha256(value, label)
        if not isinstance(self.grid, AnalysisGrid):
            raise TypeError("advected footprint evidence requires AnalysisGrid")
        if self.grid_sha256 != _grid_sha256(self.grid):
            raise ValueError("advected footprint grid checksum differs")
        previous_valid_at = _finite(self.previous_valid_at, "previous_valid_at")
        current_valid_at = _finite(self.current_valid_at, "current_valid_at")
        if current_valid_at <= previous_valid_at:
            raise ValueError("advected footprint frame time must advance")
        if _finite(
            self.translation_elapsed_seconds,
            "translation_elapsed_seconds",
        ) <= 0.0:
            raise ValueError("translation elapsed time must be positive")
        if self.translation_status not in (
            "resolved_unique_peak",
            "estimated_with_uncertainty",
            "indeterminate",
        ):
            raise ValueError("advected footprint translation status is invalid")
        if self.translation_unique_peak not in (None, True, False):
            raise ValueError("translation unique_peak is invalid")
        if (
            isinstance(self.translation_peak_tie_count, bool)
            or not isinstance(self.translation_peak_tie_count, int)
            or self.translation_peak_tie_count < 0
        ):
            raise ValueError("translation peak tie count is invalid")
        if type(self.translation_search_complete) is not bool:
            raise TypeError("translation_search_complete must be boolean")
        for values, label in (
            (
                self.translation_uncertainty_reason_codes,
                "translation uncertainty reasons",
            ),
            (
                self.translation_indeterminate_reason_codes,
                "translation indeterminate reasons",
            ),
            (self.reason_codes, "advection reasons"),
        ):
            if (
                not isinstance(values, tuple)
                or values != tuple(sorted(set(values)))
                or any(
                    not isinstance(value, str)
                    or not value
                    or len(value.encode("utf-8")) > 256
                    or "\0" in value
                    for value in values
                )
            ):
                raise ValueError(f"{label} are not canonical")
        if self.translation_selected_candidate_id is not None:
            _identifier(
                self.translation_selected_candidate_id,
                "translation_selected_candidate_id",
            )
        if self.mode not in ("advected", "unadvected_fallback"):
            raise ValueError("unknown advected footprint mode")

        requested_integers = (
            self.requested_row_shift_pixels,
            self.requested_column_shift_pixels,
        )
        requested_scalars = (
            self.requested_displacement_x_m,
            self.requested_displacement_y_m,
            self.quantization_residual_x_m,
            self.quantization_residual_y_m,
        )
        if self.translation_selected_candidate_id is None:
            if any(value is not None for value in (*requested_integers, *requested_scalars)):
                raise ValueError("missing selected shift exposes requested values")
        else:
            if any(
                value is None
                or isinstance(value, bool)
                or not isinstance(value, int)
                for value in requested_integers
            ) or any(value is None for value in requested_scalars):
                raise ValueError("selected shift requires complete requested values")
            for value in requested_scalars:
                assert value is not None
                _finite(value, "requested/quantization displacement")
            assert self.requested_column_shift_pixels is not None
            assert self.requested_row_shift_pixels is not None
            assert self.requested_displacement_x_m is not None
            assert self.requested_displacement_y_m is not None
            assert self.quantization_residual_x_m is not None
            assert self.quantization_residual_y_m is not None
            expected_residual_x = self.requested_displacement_x_m - (
                self.requested_column_shift_pixels * self.grid.x_resolution_m
            )
            expected_residual_y = self.requested_displacement_y_m - (
                -self.requested_row_shift_pixels * self.grid.y_resolution_m
            )
            if not math.isclose(
                self.quantization_residual_x_m,
                expected_residual_x,
                rel_tol=0.0,
                abs_tol=1e-9,
            ) or not math.isclose(
                self.quantization_residual_y_m,
                expected_residual_y,
                rel_tol=0.0,
                abs_tol=1e-9,
            ):
                raise ValueError("quantization residual accounting differs")

        for value, label in (
            (self.applied_row_shift_pixels, "applied_row_shift_pixels"),
            (self.applied_column_shift_pixels, "applied_column_shift_pixels"),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{label} must be integer")
        applied_x = _finite(
            self.applied_displacement_x_m,
            "applied_displacement_x_m",
        )
        applied_y = _finite(
            self.applied_displacement_y_m,
            "applied_displacement_y_m",
        )
        if not math.isclose(
            applied_x,
            self.applied_column_shift_pixels * self.grid.x_resolution_m,
            rel_tol=0.0,
            abs_tol=1e-9,
        ) or not math.isclose(
            applied_y,
            -self.applied_row_shift_pixels * self.grid.y_resolution_m,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise ValueError("applied metric displacement differs from pixel shift")
        if self.mode == "advected":
            if self.reason_codes:
                raise ValueError("advected evidence cannot have fallback reasons")
            if (
                self.translation_status == "indeterminate"
                or self.translation_unique_peak is not True
                or self.translation_peak_tie_count != 1
                or not self.translation_search_complete
                or self.translation_selected_candidate_id is None
                or self.previous_frame_id
                != self.translation_previous_frame_id
                or self.current_frame_id != self.translation_current_frame_id
                or self.supplied_previous_frame_sha256
                != self.translation_previous_frame_sha256
                or self.supplied_current_frame_sha256
                != self.translation_current_frame_sha256
                or not math.isclose(
                    self.current_valid_at - self.previous_valid_at,
                    self.translation_elapsed_seconds,
                    rel_tol=0.0,
                    abs_tol=1e-9,
                )
                or self.requested_row_shift_pixels is None
                or self.requested_column_shift_pixels is None
                or self.applied_row_shift_pixels
                != self.requested_row_shift_pixels
                or self.applied_column_shift_pixels
                != self.requested_column_shift_pixels
            ):
                raise ValueError("advected evidence did not apply selected shift")
        elif (
            not self.reason_codes
            or self.applied_row_shift_pixels != 0
            or self.applied_column_shift_pixels != 0
        ):
            raise ValueError("unadvected fallback requires reasons and zero shift")

        integer_counts = (
            self.previous_footprint_pixels,
            self.current_footprint_pixels,
            self.shifted_inside_grid_pixels,
            self.shifted_outside_grid_pixels,
            self.raw_intersection_pixels,
            self.raw_union_pixels,
            self.intersection_pixels,
            self.union_pixels,
        )
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in integer_counts
        ):
            raise ValueError("advected footprint pixel accounting is invalid")
        if (
            self.previous_footprint_pixels <= 0
            or self.current_footprint_pixels <= 0
            or self.shifted_inside_grid_pixels
            + self.shifted_outside_grid_pixels
            != self.previous_footprint_pixels
            or self.raw_union_pixels
            != self.previous_footprint_pixels
            + self.current_footprint_pixels
            - self.raw_intersection_pixels
            or self.union_pixels
            != self.previous_footprint_pixels
            + self.current_footprint_pixels
            - self.intersection_pixels
            or self.raw_intersection_pixels
            > min(self.previous_footprint_pixels, self.current_footprint_pixels)
            or self.intersection_pixels
            > min(self.shifted_inside_grid_pixels, self.current_footprint_pixels)
        ):
            raise ValueError("advected footprint set accounting differs")
        expected_raw_iou = self.raw_intersection_pixels / self.raw_union_pixels
        expected_iou = self.intersection_pixels / self.union_pixels
        for actual, expected, label in (
            (self.raw_iou, expected_raw_iou, "raw_iou"),
            (self.iou, expected_iou, "iou"),
        ):
            if (
                not math.isfinite(actual)
                or not 0.0 <= actual <= 1.0
                or not math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12)
            ):
                raise ValueError(f"{label} differs from exact set accounting")
        if self.mode == "unadvected_fallback" and (
            self.intersection_pixels != self.raw_intersection_pixels
            or self.union_pixels != self.raw_union_pixels
            or self.iou != self.raw_iou
            or self.shifted_outside_grid_pixels != 0
        ):
            raise ValueError("fallback evidence must equal raw unadvected overlap")
        for label in (
            "candidate_retained",
            "speed_gate_applied",
            "distance_gate_applied",
            "iou_gate_applied",
            "meteorological_admission_granted",
            "operational_claim_generated",
        ):
            if type(getattr(self, label)) is not bool:
                raise TypeError(f"{label} must be boolean")
        if (
            not self.candidate_retained
            or self.speed_gate_applied
            or self.distance_gate_applied
            or self.iou_gate_applied
            or self.meteorological_admission_granted
            or self.operational_claim_generated
        ):
            raise ValueError("advected footprint evidence contains a gate or claim")
        if self.algorithm_version != ADVECTED_FOOTPRINT_EVIDENCE_VERSION:
            raise ValueError("unknown advected footprint evidence version")
        if self.evidence_sha256 != _sha_mapping(self.payload_mapping()):
            raise ValueError("advected footprint evidence checksum differs")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "algorithm_version": self.algorithm_version,
            "applied_column_shift_pixels": self.applied_column_shift_pixels,
            "applied_displacement_x_m": float(
                self.applied_displacement_x_m
            ).hex(),
            "applied_displacement_y_m": float(
                self.applied_displacement_y_m
            ).hex(),
            "applied_row_shift_pixels": self.applied_row_shift_pixels,
            "candidate_retained": self.candidate_retained,
            "current_detection_id": self.current_detection_id,
            "current_footprint_pixels": self.current_footprint_pixels,
            "current_footprint_sha256": self.current_footprint_sha256,
            "current_frame_id": self.current_frame_id,
            "current_valid_at": float(self.current_valid_at).hex(),
            "distance_gate_applied": self.distance_gate_applied,
            "grid": _grid_mapping(self.grid),
            "grid_sha256": self.grid_sha256,
            "intersection_pixels": self.intersection_pixels,
            "iou": float(self.iou).hex(),
            "iou_gate_applied": self.iou_gate_applied,
            "meteorological_admission_granted": (
                self.meteorological_admission_granted
            ),
            "mode": self.mode,
            "operational_claim_generated": self.operational_claim_generated,
            "previous_detection_id": self.previous_detection_id,
            "previous_footprint_pixels": self.previous_footprint_pixels,
            "previous_footprint_sha256": self.previous_footprint_sha256,
            "previous_frame_id": self.previous_frame_id,
            "previous_valid_at": float(self.previous_valid_at).hex(),
            "quantization_residual_x_m": _optional_float_hex(
                self.quantization_residual_x_m
            ),
            "quantization_residual_y_m": _optional_float_hex(
                self.quantization_residual_y_m
            ),
            "raw_intersection_pixels": self.raw_intersection_pixels,
            "raw_iou": float(self.raw_iou).hex(),
            "raw_union_pixels": self.raw_union_pixels,
            "reason_codes": list(self.reason_codes),
            "requested_column_shift_pixels": self.requested_column_shift_pixels,
            "requested_displacement_x_m": _optional_float_hex(
                self.requested_displacement_x_m
            ),
            "requested_displacement_y_m": _optional_float_hex(
                self.requested_displacement_y_m
            ),
            "requested_row_shift_pixels": self.requested_row_shift_pixels,
            "shifted_inside_grid_pixels": self.shifted_inside_grid_pixels,
            "shifted_outside_grid_pixels": self.shifted_outside_grid_pixels,
            "speed_gate_applied": self.speed_gate_applied,
            "supplied_current_frame_sha256": self.supplied_current_frame_sha256,
            "supplied_previous_frame_sha256": self.supplied_previous_frame_sha256,
            "translation_current_frame_id": self.translation_current_frame_id,
            "translation_current_frame_sha256": (
                self.translation_current_frame_sha256
            ),
            "translation_elapsed_seconds": float(
                self.translation_elapsed_seconds
            ).hex(),
            "translation_indeterminate_reason_codes": list(
                self.translation_indeterminate_reason_codes
            ),
            "translation_peak_tie_count": self.translation_peak_tie_count,
            "translation_search_complete": self.translation_search_complete,
            "translation_previous_frame_id": self.translation_previous_frame_id,
            "translation_previous_frame_sha256": (
                self.translation_previous_frame_sha256
            ),
            "translation_result_sha256": self.translation_result_sha256,
            "translation_selected_candidate_id": (
                self.translation_selected_candidate_id
            ),
            "translation_status": self.translation_status,
            "translation_uncertainty_reason_codes": list(
                self.translation_uncertainty_reason_codes
            ),
            "translation_unique_peak": self.translation_unique_peak,
            "union_pixels": self.union_pixels,
        }

    def to_mapping(self) -> dict[str, Any]:
        value = self.payload_mapping()
        value["evidence_sha256"] = self.evidence_sha256
        return value


@dataclass(frozen=True, slots=True)
class AdvectedFootprintEvidenceTable:
    """A complete, canonically ordered previous/current cross product."""

    table_sha256: str
    previous_frame_id: str
    current_frame_id: str
    previous_valid_at: float
    current_valid_at: float
    supplied_previous_frame_sha256: str
    supplied_current_frame_sha256: str
    grid: AnalysisGrid
    grid_sha256: str
    translation_result_sha256: str
    previous_footprints: tuple[tuple[str, str], ...]
    current_footprints: tuple[tuple[str, str], ...]
    evidence: tuple[AdvectedFootprintEvidence, ...]
    expected_pair_count: int
    maximum_pair_count: int
    candidate_rejection_by_speed: bool = False
    candidate_rejection_by_distance: bool = False
    candidate_rejection_by_iou: bool = False
    algorithm_version: str = ADVECTED_FOOTPRINT_EVIDENCE_VERSION

    def __post_init__(self) -> None:
        _sha256(self.table_sha256, "advected footprint table_sha256")
        for value, label in (
            (self.previous_frame_id, "previous_frame_id"),
            (self.current_frame_id, "current_frame_id"),
        ):
            _identifier(value, label)
        for value, label in (
            (
                self.supplied_previous_frame_sha256,
                "supplied_previous_frame_sha256",
            ),
            (
                self.supplied_current_frame_sha256,
                "supplied_current_frame_sha256",
            ),
            (self.grid_sha256, "grid_sha256"),
            (self.translation_result_sha256, "translation_result_sha256"),
        ):
            _sha256(value, label)
        if not isinstance(self.grid, AnalysisGrid):
            raise TypeError("advected footprint table requires AnalysisGrid")
        if self.grid_sha256 != _grid_sha256(self.grid):
            raise ValueError("advected footprint table grid checksum differs")
        if (
            _finite(self.current_valid_at, "current_valid_at")
            <= _finite(self.previous_valid_at, "previous_valid_at")
        ):
            raise ValueError("advected footprint table time must advance")
        for values, label in (
            (self.previous_footprints, "previous footprints"),
            (self.current_footprints, "current footprints"),
        ):
            if (
                not isinstance(values, tuple)
                or values != tuple(sorted(values))
                or len({item[0] for item in values}) != len(values)
            ):
                raise ValueError(f"{label} are not canonical")
            for detection_id, footprint_sha256 in values:
                _identifier(detection_id, f"{label} detection_id")
                _sha256(footprint_sha256, f"{label} sha256")
        if (
            isinstance(self.expected_pair_count, bool)
            or not isinstance(self.expected_pair_count, int)
            or self.expected_pair_count < 0
            or isinstance(self.maximum_pair_count, bool)
            or not isinstance(self.maximum_pair_count, int)
            or not 0 <= self.maximum_pair_count <= HARD_MAX_ADVECTED_FOOTPRINT_PAIRS
            or self.expected_pair_count > self.maximum_pair_count
            or self.expected_pair_count
            != len(self.previous_footprints) * len(self.current_footprints)
        ):
            raise ValueError("advected footprint table pair bounds differ")
        if (
            not isinstance(self.evidence, tuple)
            or len(self.evidence) != self.expected_pair_count
            or self.evidence != tuple(
                sorted(
                    self.evidence,
                    key=lambda item: (
                        item.previous_detection_id,
                        item.current_detection_id,
                    ),
                )
            )
        ):
            raise ValueError("advected footprint table evidence is not complete")
        expected_pairs = {
            (previous_id, current_id)
            for previous_id, _sha in self.previous_footprints
            for current_id, _current_sha in self.current_footprints
        }
        actual_pairs = {
            (item.previous_detection_id, item.current_detection_id)
            for item in self.evidence
        }
        if actual_pairs != expected_pairs or len(actual_pairs) != len(self.evidence):
            raise ValueError("advected footprint table lost or duplicated a pair")
        previous_sha = dict(self.previous_footprints)
        current_sha = dict(self.current_footprints)
        for item in self.evidence:
            if (
                item.previous_frame_id != self.previous_frame_id
                or item.current_frame_id != self.current_frame_id
                or item.previous_valid_at != self.previous_valid_at
                or item.current_valid_at != self.current_valid_at
                or item.supplied_previous_frame_sha256
                != self.supplied_previous_frame_sha256
                or item.supplied_current_frame_sha256
                != self.supplied_current_frame_sha256
                or item.grid != self.grid
                or item.grid_sha256 != self.grid_sha256
                or item.translation_result_sha256
                != self.translation_result_sha256
                or item.previous_footprint_sha256
                != previous_sha[item.previous_detection_id]
                or item.current_footprint_sha256
                != current_sha[item.current_detection_id]
                or not item.candidate_retained
            ):
                raise ValueError("advected footprint table evidence context differs")
        for label in (
            "candidate_rejection_by_speed",
            "candidate_rejection_by_distance",
            "candidate_rejection_by_iou",
        ):
            if type(getattr(self, label)) is not bool or getattr(self, label):
                raise ValueError("advected footprint table cannot reject candidates")
        if self.algorithm_version != ADVECTED_FOOTPRINT_EVIDENCE_VERSION:
            raise ValueError("unknown advected footprint table version")
        if self.table_sha256 != _sha_mapping(self.payload_mapping()):
            raise ValueError("advected footprint table checksum differs")

    @property
    def is_complete(self) -> bool:
        return len(self.evidence) == self.expected_pair_count

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "algorithm_version": self.algorithm_version,
            "candidate_rejection_by_distance": self.candidate_rejection_by_distance,
            "candidate_rejection_by_iou": self.candidate_rejection_by_iou,
            "candidate_rejection_by_speed": self.candidate_rejection_by_speed,
            "current_footprints": [list(item) for item in self.current_footprints],
            "current_frame_id": self.current_frame_id,
            "current_valid_at": float(self.current_valid_at).hex(),
            "evidence_sha256": [item.evidence_sha256 for item in self.evidence],
            "expected_pair_count": self.expected_pair_count,
            "grid": _grid_mapping(self.grid),
            "grid_sha256": self.grid_sha256,
            "maximum_pair_count": self.maximum_pair_count,
            "previous_footprints": [list(item) for item in self.previous_footprints],
            "previous_frame_id": self.previous_frame_id,
            "previous_valid_at": float(self.previous_valid_at).hex(),
            "supplied_current_frame_sha256": self.supplied_current_frame_sha256,
            "supplied_previous_frame_sha256": self.supplied_previous_frame_sha256,
            "translation_result_sha256": self.translation_result_sha256,
        }

    def to_mapping(self) -> dict[str, Any]:
        value = self.payload_mapping()
        value["evidence"] = [item.to_mapping() for item in self.evidence]
        value["table_sha256"] = self.table_sha256
        return value


@dataclass(frozen=True, slots=True)
class _AdvectionContext:
    grid: AnalysisGrid
    previous_frame: BinaryRainShieldFootprintFrame
    current_frame: BinaryRainShieldFootprintFrame
    translation: SharedSystemTranslationResult
    selected: BinaryShiftTranslationCandidate | None
    mode: AdvectionMode
    reason_codes: tuple[str, ...]


def _translation_context(
    *,
    grid: AnalysisGrid,
    previous_frame: BinaryRainShieldFootprintFrame,
    current_frame: BinaryRainShieldFootprintFrame,
    translation: SharedSystemTranslationResult,
) -> _AdvectionContext:
    if not isinstance(grid, AnalysisGrid):
        raise TypeError("advected footprint context requires AnalysisGrid")
    if not isinstance(previous_frame, BinaryRainShieldFootprintFrame) or not isinstance(
        current_frame,
        BinaryRainShieldFootprintFrame,
    ):
        raise TypeError("advected footprint context requires binary source frames")
    if not isinstance(translation, SharedSystemTranslationResult):
        raise TypeError("advected footprint context requires shared translation")
    if translation.result_sha256 != _sha_mapping(translation.payload_mapping()):
        raise ValueError("shared translation checksum differs")

    reasons: set[str] = set()
    if grid != previous_frame.grid or grid != current_frame.grid:
        reasons.add("translation_grid_mismatch")
    if (
        translation.accounting.grid_width != grid.width
        or translation.accounting.grid_height != grid.height
        or translation.accounting.grid_pixels != grid.width * grid.height
    ):
        reasons.add("translation_accounting_grid_mismatch")
    if previous_frame.frame_id != translation.previous_frame_id:
        reasons.add("translation_previous_frame_id_mismatch")
    if current_frame.frame_id != translation.current_frame_id:
        reasons.add("translation_current_frame_id_mismatch")
    if previous_frame.fingerprint != translation.previous_frame_sha256:
        reasons.add("translation_previous_frame_sha_mismatch")
    if current_frame.fingerprint != translation.current_frame_sha256:
        reasons.add("translation_current_frame_sha_mismatch")
    supplied_elapsed = current_frame.valid_at - previous_frame.valid_at
    if (
        not math.isfinite(supplied_elapsed)
        or supplied_elapsed <= 0.0
        or not math.isclose(
            supplied_elapsed,
            translation.elapsed_seconds,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
    ):
        reasons.add("translation_elapsed_mismatch")

    selected = translation.selected_candidate
    if translation.status == "indeterminate":
        reasons.add("translation_indeterminate")
    if selected is None:
        reasons.add("translation_selected_shift_unavailable")
    if translation.unique_peak is not True or translation.peak_tie_count != 1:
        reasons.add("translation_peak_not_unique")
    if not translation.accounting.search_complete:
        reasons.add("translation_search_incomplete")
    if selected is not None and (
        selected.grid_width != grid.width
        or selected.grid_height != grid.height
        or selected.x_resolution_m != grid.x_resolution_m
        or selected.y_resolution_m != grid.y_resolution_m
        or selected.elapsed_seconds != translation.elapsed_seconds
    ):
        reasons.add("translation_selected_shift_grid_or_time_mismatch")

    # A complete, unique uncertain peak is still usable descriptive evidence;
    # its uncertainty remains bound verbatim below.  Ambiguity, incomplete
    # search or any authority/time/grid mismatch always falls back.
    mode: AdvectionMode = "advected" if not reasons else "unadvected_fallback"
    return _AdvectionContext(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
        selected=selected,
        mode=mode,
        reason_codes=tuple(sorted(reasons)),
    )


def _validate_object_on_grid(value: FootprintEvidenceObject, grid: AnalysisGrid) -> None:
    if not isinstance(value, FootprintEvidenceObject):
        raise TypeError("advected footprint pair requires FootprintEvidenceObject")
    if any(
        row >= grid.height or column >= grid.width
        for row, column in value.footprint_pixels
    ):
        raise ValueError("pair footprint lies outside AnalysisGrid")


def _build_pair_evidence(
    *,
    context: _AdvectionContext,
    previous: FootprintEvidenceObject,
    current: FootprintEvidenceObject,
    previous_pixels: frozenset[Pixel] | None = None,
    current_pixels: frozenset[Pixel] | None = None,
    previous_footprint_sha256: str | None = None,
    current_footprint_sha256: str | None = None,
    forced_fallback_reason_codes: tuple[str, ...] = (),
) -> AdvectedFootprintEvidence:
    grid = context.grid
    _validate_object_on_grid(previous, grid)
    _validate_object_on_grid(current, grid)
    selected = context.selected
    if (
        not isinstance(forced_fallback_reason_codes, tuple)
        or forced_fallback_reason_codes
        != tuple(sorted(set(forced_fallback_reason_codes)))
        or any(
            not isinstance(value, str)
            or not value
            or len(value.encode("utf-8")) > 256
            or "\0" in value
            for value in forced_fallback_reason_codes
        )
    ):
        raise ValueError("forced fallback reasons are not canonical")
    reason_codes = tuple(sorted({
        *context.reason_codes,
        *forced_fallback_reason_codes,
    }))
    mode: AdvectionMode = (
        "advected" if not reason_codes else "unadvected_fallback"
    )

    requested_row = None if selected is None else selected.row_shift_pixels
    requested_column = None if selected is None else selected.column_shift_pixels
    requested_x = None if selected is None else selected.displacement_x_m
    requested_y = None if selected is None else selected.displacement_y_m
    residual_x = (
        None
        if selected is None
        else selected.displacement_x_m
        - selected.column_shift_pixels * grid.x_resolution_m
    )
    residual_y = (
        None
        if selected is None
        else selected.displacement_y_m
        - (-selected.row_shift_pixels * grid.y_resolution_m)
    )
    if mode == "advected":
        assert selected is not None
        applied_row = selected.row_shift_pixels
        applied_column = selected.column_shift_pixels
    else:
        applied_row = 0
        applied_column = 0
    applied_x = applied_column * grid.x_resolution_m
    applied_y = -applied_row * grid.y_resolution_m

    old_pixels = (
        frozenset(previous.footprint_pixels)
        if previous_pixels is None
        else previous_pixels
    )
    new_pixels = (
        frozenset(current.footprint_pixels)
        if current_pixels is None
        else current_pixels
    )
    previous_sha256 = (
        previous.footprint_sha256
        if previous_footprint_sha256 is None
        else _sha256(previous_footprint_sha256, "previous_footprint_sha256")
    )
    current_sha256 = (
        current.footprint_sha256
        if current_footprint_sha256 is None
        else _sha256(current_footprint_sha256, "current_footprint_sha256")
    )
    raw_intersection = len(old_pixels & new_pixels)
    raw_union = len(old_pixels | new_pixels)
    shifted = frozenset(
        (row + applied_row, column + applied_column)
        for row, column in old_pixels
    )
    inside = sum(
        0 <= row < grid.height and 0 <= column < grid.width
        for row, column in shifted
    )
    intersection = len(shifted & new_pixels)
    union = len(shifted | new_pixels)

    values = {
        "previous_detection_id": previous.detection_id,
        "current_detection_id": current.detection_id,
        "previous_footprint_sha256": previous_sha256,
        "current_footprint_sha256": current_sha256,
        "previous_frame_id": context.previous_frame.frame_id,
        "current_frame_id": context.current_frame.frame_id,
        "previous_valid_at": context.previous_frame.valid_at,
        "current_valid_at": context.current_frame.valid_at,
        "supplied_previous_frame_sha256": context.previous_frame.fingerprint,
        "supplied_current_frame_sha256": context.current_frame.fingerprint,
        "grid": grid,
        "grid_sha256": _grid_sha256(grid),
        "translation_result_sha256": context.translation.result_sha256,
        "translation_status": context.translation.status,
        "translation_previous_frame_id": context.translation.previous_frame_id,
        "translation_current_frame_id": context.translation.current_frame_id,
        "translation_previous_frame_sha256": (
            context.translation.previous_frame_sha256
        ),
        "translation_current_frame_sha256": (
            context.translation.current_frame_sha256
        ),
        "translation_elapsed_seconds": context.translation.elapsed_seconds,
        "translation_unique_peak": context.translation.unique_peak,
        "translation_peak_tie_count": context.translation.peak_tie_count,
        "translation_search_complete": (
            context.translation.accounting.search_complete
        ),
        "translation_uncertainty_reason_codes": (
            context.translation.uncertainty_reason_codes
        ),
        "translation_indeterminate_reason_codes": (
            context.translation.indeterminate_reason_codes
        ),
        "translation_selected_candidate_id": (
            None if selected is None else selected.candidate_id
        ),
        "mode": mode,
        "reason_codes": reason_codes,
        "requested_row_shift_pixels": requested_row,
        "requested_column_shift_pixels": requested_column,
        "requested_displacement_x_m": requested_x,
        "requested_displacement_y_m": requested_y,
        "applied_row_shift_pixels": applied_row,
        "applied_column_shift_pixels": applied_column,
        "applied_displacement_x_m": applied_x,
        "applied_displacement_y_m": applied_y,
        "quantization_residual_x_m": residual_x,
        "quantization_residual_y_m": residual_y,
        "previous_footprint_pixels": len(old_pixels),
        "current_footprint_pixels": len(new_pixels),
        "shifted_inside_grid_pixels": inside,
        "shifted_outside_grid_pixels": len(shifted) - inside,
        "raw_intersection_pixels": raw_intersection,
        "raw_union_pixels": raw_union,
        "raw_iou": raw_intersection / raw_union,
        "intersection_pixels": intersection,
        "union_pixels": union,
        "iou": intersection / union,
    }
    temporary = object.__new__(AdvectedFootprintEvidence)
    for name, value in values.items():
        object.__setattr__(temporary, name, value)
    object.__setattr__(temporary, "evidence_sha256", "0" * 64)
    for name, value in (
        ("candidate_retained", True),
        ("speed_gate_applied", False),
        ("distance_gate_applied", False),
        ("iou_gate_applied", False),
        ("meteorological_admission_granted", False),
        ("operational_claim_generated", False),
        ("algorithm_version", ADVECTED_FOOTPRINT_EVIDENCE_VERSION),
    ):
        object.__setattr__(temporary, name, value)
    return AdvectedFootprintEvidence(
        evidence_sha256=_sha_mapping(temporary.payload_mapping()),
        **values,
    )


def build_advected_footprint_evidence(
    *,
    grid: AnalysisGrid,
    previous_frame: BinaryRainShieldFootprintFrame,
    current_frame: BinaryRainShieldFootprintFrame,
    translation: SharedSystemTranslationResult,
    previous: FootprintEvidenceObject,
    current: FootprintEvidenceObject,
    forced_fallback_reason_codes: tuple[str, ...] = (),
) -> AdvectedFootprintEvidence:
    """Build one retained pair with advected or explicit raw fallback overlap."""

    context = _translation_context(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
    )
    return _build_pair_evidence(
        context=context,
        previous=previous,
        current=current,
        forced_fallback_reason_codes=forced_fallback_reason_codes,
    )


def build_complete_advected_footprint_evidence(
    *,
    grid: AnalysisGrid,
    previous_frame: BinaryRainShieldFootprintFrame,
    current_frame: BinaryRainShieldFootprintFrame,
    translation: SharedSystemTranslationResult,
    previous_objects: Iterable[FootprintEvidenceObject],
    current_objects: Iterable[FootprintEvidenceObject],
    maximum_pair_count: int,
) -> AdvectedFootprintEvidenceTable:
    """Build the complete n×m evidence product or fail before partial work."""

    if (
        isinstance(maximum_pair_count, bool)
        or not isinstance(maximum_pair_count, int)
        or not 0 <= maximum_pair_count <= HARD_MAX_ADVECTED_FOOTPRINT_PAIRS
    ):
        raise ValueError("maximum_pair_count is outside the hard bound")
    previous_values = tuple(previous_objects)
    current_values = tuple(current_objects)
    if any(
        not isinstance(item, FootprintEvidenceObject)
        for item in (*previous_values, *current_values)
    ):
        raise TypeError("complete footprint evidence requires canonical objects")
    previous = tuple(
        sorted(previous_values, key=lambda item: item.detection_id)
    )
    current = tuple(
        sorted(current_values, key=lambda item: item.detection_id)
    )
    if len({item.detection_id for item in previous}) != len(previous) or len(
        {item.detection_id for item in current}
    ) != len(current):
        raise ValueError("complete footprint evidence requires unique side IDs")
    expected = len(previous) * len(current)
    if expected > maximum_pair_count:
        raise AdvectedFootprintCapacityError(
            "complete advected footprint pair capacity exceeded"
        )
    for item in (*previous, *current):
        _validate_object_on_grid(item, grid)
    context = _translation_context(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
    )
    previous_prepared = {
        item.detection_id: (
            frozenset(item.footprint_pixels),
            item.footprint_sha256,
        )
        for item in previous
    }
    current_prepared = {
        item.detection_id: (
            frozenset(item.footprint_pixels),
            item.footprint_sha256,
        )
        for item in current
    }
    evidence = tuple(
        _build_pair_evidence(
            context=context,
            previous=old,
            current=new,
            previous_pixels=previous_prepared[old.detection_id][0],
            current_pixels=current_prepared[new.detection_id][0],
            previous_footprint_sha256=previous_prepared[old.detection_id][1],
            current_footprint_sha256=current_prepared[new.detection_id][1],
        )
        for old in previous
        for new in current
    )
    values = {
        "previous_frame_id": previous_frame.frame_id,
        "current_frame_id": current_frame.frame_id,
        "previous_valid_at": previous_frame.valid_at,
        "current_valid_at": current_frame.valid_at,
        "supplied_previous_frame_sha256": previous_frame.fingerprint,
        "supplied_current_frame_sha256": current_frame.fingerprint,
        "grid": grid,
        "grid_sha256": _grid_sha256(grid),
        "translation_result_sha256": translation.result_sha256,
        "previous_footprints": tuple(
            (item.detection_id, previous_prepared[item.detection_id][1])
            for item in previous
        ),
        "current_footprints": tuple(
            (item.detection_id, current_prepared[item.detection_id][1])
            for item in current
        ),
        "evidence": evidence,
        "expected_pair_count": expected,
        "maximum_pair_count": maximum_pair_count,
    }
    temporary = object.__new__(AdvectedFootprintEvidenceTable)
    for name, value in values.items():
        object.__setattr__(temporary, name, value)
    object.__setattr__(temporary, "table_sha256", "0" * 64)
    for name, value in (
        ("candidate_rejection_by_speed", False),
        ("candidate_rejection_by_distance", False),
        ("candidate_rejection_by_iou", False),
        ("algorithm_version", ADVECTED_FOOTPRINT_EVIDENCE_VERSION),
    ):
        object.__setattr__(temporary, name, value)
    return AdvectedFootprintEvidenceTable(
        table_sha256=_sha_mapping(temporary.payload_mapping()),
        **values,
    )


__all__ = (
    "ADVECTED_FOOTPRINT_EVIDENCE_VERSION",
    "HARD_MAX_ADVECTED_FOOTPRINT_PAIRS",
    "AdvectedFootprintCapacityError",
    "AdvectedFootprintEvidence",
    "AdvectedFootprintEvidenceTable",
    "FootprintEvidenceObject",
    "build_advected_footprint_evidence",
    "build_complete_advected_footprint_evidence",
)
