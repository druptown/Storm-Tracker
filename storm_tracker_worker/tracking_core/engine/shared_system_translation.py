"""Pure assignment-independent translation from binary rain-shield support.

The estimator combines every rain-shield footprint in each frame into one
binary support field and searches the *complete* integer-shift domain on an
unchanged :class:`AnalysisGrid`.  Shifts are non-circular: support translated
outside the grid is explicitly accounted as outside support and never wraps.

The overlap peak is descriptive shadow evidence, not an identity, speed or
operational weather claim.  Low overlap is retained with an uncertainty code;
it is never rejected by a distance or speed threshold.  If the complete
search cannot fit caller-owned resource bounds, the result is explicitly
indeterminate and no partial peak is exposed.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any, Literal

from .analysis_grid import AnalysisGrid


SHARED_SYSTEM_TRANSLATION_VERSION = (
    "stv3-binary-rain-shield-shared-translation-v1"
)
HARD_MAX_GRID_PIXELS = 1_000_000
HARD_MAX_SUPPORT_PIXELS_PER_FRAME = 1_000_000
HARD_MAX_PAIR_CONTRIBUTIONS = 4_000_000
HARD_MAX_SHIFT_CANDIDATES = 4_000_000
HARD_MAX_NONZERO_SHIFT_CANDIDATES = 250_000

Pixel = tuple[int, int]
TranslationStatus = Literal[
    "resolved_unique_peak",
    "estimated_with_uncertainty",
    "indeterminate",
]


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


def _finite(value: object, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise ValueError(f"{label} must be finite")
    return float(value)


def _bounded_int(
    value: object,
    label: str,
    *,
    minimum: int = 0,
    maximum: int,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ValueError(f"{label} is outside {minimum}..{maximum}")
    return value


def _grid_mapping(grid: AnalysisGrid) -> dict[str, Any]:
    return {
        "axis_order": grid.axis_order,
        "coordinate_unit": grid.coordinate_unit,
        "crs": grid.crs,
        "grid_id": grid.grid_id,
        "height": grid.height,
        "origin_x_m": float(grid.origin_x_m),
        "origin_y_m": float(grid.origin_y_m),
        "row_order": grid.row_order,
        "width": grid.width,
        "x_resolution_m": float(grid.x_resolution_m),
        "y_resolution_m": float(grid.y_resolution_m),
    }


@dataclass(frozen=True, slots=True)
class BinaryRainShieldFootprintFrame:
    """One canonical binary field made from disjoint rain-shield footprints."""

    frame_id: str
    valid_at: float
    grid: AnalysisGrid
    rain_shield_footprints: tuple[tuple[Pixel, ...], ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.frame_id, str)
            or not self.frame_id
            or len(self.frame_id) > 256
        ):
            raise ValueError("binary shield frame_id is invalid")
        object.__setattr__(
            self,
            "valid_at",
            _finite(self.valid_at, "binary shield valid_at"),
        )
        if not isinstance(self.grid, AnalysisGrid):
            raise TypeError("binary shield frame requires AnalysisGrid")
        if not isinstance(self.rain_shield_footprints, tuple):
            raise TypeError("rain-shield footprints must be an immutable tuple")

        seen: set[Pixel] = set()
        canonical: list[tuple[Pixel, ...]] = []
        for footprint in self.rain_shield_footprints:
            if not isinstance(footprint, tuple) or not footprint:
                raise ValueError("every rain-shield footprint must be non-empty")
            pixels: list[Pixel] = []
            local: set[Pixel] = set()
            for pixel in footprint:
                if (
                    not isinstance(pixel, tuple)
                    or len(pixel) != 2
                    or isinstance(pixel[0], bool)
                    or not isinstance(pixel[0], int)
                    or isinstance(pixel[1], bool)
                    or not isinstance(pixel[1], int)
                ):
                    raise ValueError("rain-shield pixels must be integer pairs")
                row, column = pixel
                if not (
                    0 <= row < self.grid.height
                    and 0 <= column < self.grid.width
                ):
                    raise ValueError("rain-shield pixel lies outside AnalysisGrid")
                if pixel in local:
                    raise ValueError("rain-shield footprint contains a duplicate pixel")
                if pixel in seen:
                    raise ValueError("rain-shield footprints overlap")
                local.add(pixel)
                seen.add(pixel)
                pixels.append(pixel)
            canonical.append(tuple(sorted(pixels)))
        object.__setattr__(
            self,
            "rain_shield_footprints",
            tuple(sorted(canonical)),
        )

    @property
    def support_pixels(self) -> tuple[Pixel, ...]:
        return tuple(sorted(
            pixel
            for footprint in self.rain_shield_footprints
            for pixel in footprint
        ))

    def to_mapping(self) -> dict[str, Any]:
        return {
            "contract": "stv3-binary-rain-shield-footprint-frame-v1",
            "frame_id": self.frame_id,
            "grid": _grid_mapping(self.grid),
            "rain_shield_footprints": [
                [list(pixel) for pixel in footprint]
                for footprint in self.rain_shield_footprints
            ],
            "valid_at": self.valid_at,
        }

    @property
    def fingerprint(self) -> str:
        return _sha_mapping(self.to_mapping())


@dataclass(frozen=True, slots=True)
class SharedSystemTranslationConfig:
    """Caller-owned compute bounds, never meteorological rejection gates."""

    max_grid_pixels: int = 250_000
    max_support_pixels_per_frame: int = 250_000
    max_pair_contributions: int = 2_000_000
    max_shift_candidates: int = 1_000_000
    max_nonzero_shift_candidates: int = 100_000
    low_overlap_uncertainty_fraction: float = 0.25

    def __post_init__(self) -> None:
        _bounded_int(
            self.max_grid_pixels,
            "max_grid_pixels",
            minimum=1,
            maximum=HARD_MAX_GRID_PIXELS,
        )
        _bounded_int(
            self.max_support_pixels_per_frame,
            "max_support_pixels_per_frame",
            minimum=1,
            maximum=HARD_MAX_SUPPORT_PIXELS_PER_FRAME,
        )
        _bounded_int(
            self.max_pair_contributions,
            "max_pair_contributions",
            minimum=1,
            maximum=HARD_MAX_PAIR_CONTRIBUTIONS,
        )
        _bounded_int(
            self.max_shift_candidates,
            "max_shift_candidates",
            minimum=1,
            maximum=HARD_MAX_SHIFT_CANDIDATES,
        )
        _bounded_int(
            self.max_nonzero_shift_candidates,
            "max_nonzero_shift_candidates",
            minimum=1,
            maximum=HARD_MAX_NONZERO_SHIFT_CANDIDATES,
        )
        threshold = _finite(
            self.low_overlap_uncertainty_fraction,
            "low_overlap_uncertainty_fraction",
        )
        if not 0.0 < threshold <= 1.0:
            raise ValueError("low-overlap uncertainty fraction must be in (0,1]")
        object.__setattr__(
            self,
            "low_overlap_uncertainty_fraction",
            threshold,
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "algorithm": SHARED_SYSTEM_TRANSLATION_VERSION,
            "low_overlap_uncertainty_fraction": (
                self.low_overlap_uncertainty_fraction
            ),
            "max_grid_pixels": self.max_grid_pixels,
            "max_nonzero_shift_candidates": self.max_nonzero_shift_candidates,
            "max_pair_contributions": self.max_pair_contributions,
            "max_shift_candidates": self.max_shift_candidates,
            "max_support_pixels_per_frame": self.max_support_pixels_per_frame,
            "search_domain": "complete_non_circular_integer_shift_domain",
            "speed_or_distance_gate": None,
        }

    @property
    def fingerprint(self) -> str:
        return _sha_mapping(self.to_mapping())


@dataclass(frozen=True, slots=True)
class SharedSystemTranslationAccounting:
    grid_width: int
    grid_height: int
    grid_pixels: int
    previous_footprint_objects: int
    current_footprint_objects: int
    previous_pixel_references: int
    current_pixel_references: int
    previous_support_pixels: int
    current_support_pixels: int
    previous_background_pixels: int
    current_background_pixels: int
    full_shift_candidates: int
    shift_candidates_evaluated: int
    pair_contributions_expected: int
    pair_contributions_evaluated: int
    overlap_histogram_mass: int
    nonzero_shift_candidates_discovered: int
    search_complete: bool

    def __post_init__(self) -> None:
        values = (
            self.grid_width,
            self.grid_height,
            self.grid_pixels,
            self.previous_footprint_objects,
            self.current_footprint_objects,
            self.previous_pixel_references,
            self.current_pixel_references,
            self.previous_support_pixels,
            self.current_support_pixels,
            self.previous_background_pixels,
            self.current_background_pixels,
            self.full_shift_candidates,
            self.shift_candidates_evaluated,
            self.pair_contributions_expected,
            self.pair_contributions_evaluated,
            self.overlap_histogram_mass,
            self.nonzero_shift_candidates_discovered,
        )
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in values
        ):
            raise ValueError("translation accounting counts must be non-negative")
        if (
            self.grid_width <= 0
            or self.grid_height <= 0
            or self.grid_pixels != self.grid_width * self.grid_height
            or self.full_shift_candidates
            != (2 * self.grid_height - 1) * (2 * self.grid_width - 1)
        ):
            raise ValueError("translation grid/search accounting differs")
        if (
            self.previous_pixel_references != self.previous_support_pixels
            or self.current_pixel_references != self.current_support_pixels
            or self.previous_support_pixels + self.previous_background_pixels
            != self.grid_pixels
            or self.current_support_pixels + self.current_background_pixels
            != self.grid_pixels
            or self.pair_contributions_expected
            != self.previous_support_pixels * self.current_support_pixels
        ):
            raise ValueError("translation input pixel accounting is not exhaustive")
        if (
            self.shift_candidates_evaluated > self.full_shift_candidates
            or self.pair_contributions_evaluated
            > self.pair_contributions_expected
            or self.overlap_histogram_mass
            != self.pair_contributions_evaluated
            or self.nonzero_shift_candidates_discovered
            > min(
                self.full_shift_candidates,
                self.pair_contributions_evaluated,
            )
        ):
            raise ValueError("translation work accounting exceeds its authority")
        if type(self.search_complete) is not bool:
            raise TypeError("search_complete must be boolean")
        if self.search_complete and (
            self.shift_candidates_evaluated != self.full_shift_candidates
            or self.pair_contributions_evaluated
            != self.pair_contributions_expected
        ):
            raise ValueError("complete search accounting is incomplete")

    def to_mapping(self) -> dict[str, Any]:
        return {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
        }


@dataclass(frozen=True, slots=True)
class BinaryShiftTranslationCandidate:
    candidate_id: str
    row_shift_pixels: int
    column_shift_pixels: int
    displacement_x_m: float
    displacement_y_m: float
    displacement_distance_m: float
    interval_speed_kmh: float
    elapsed_seconds: float
    grid_width: int
    grid_height: int
    x_resolution_m: float
    y_resolution_m: float
    previous_support_pixels: int
    current_support_pixels: int
    comparison_window_pixels: int
    overlap_pixels: int
    previous_support_inside_grid: int
    previous_support_outside_grid: int
    current_support_inside_comparison_window: int
    current_support_outside_comparison_window: int
    previous_only_inside_window: int
    current_only_inside_window: int
    both_dry_inside_window: int
    previous_unmatched_support_pixels: int
    current_unmatched_support_pixels: int
    full_support_union_pixels: int
    overlap_fraction_previous: float
    overlap_fraction_current: float
    overlap_fraction_smaller_support: float
    jaccard_full_support: float
    dice_full_support: float

    def __post_init__(self) -> None:
        if (
            not isinstance(self.candidate_id, str)
            or not self.candidate_id.startswith("binary-shift:")
            or len(self.candidate_id) != len("binary-shift:") + 64
        ):
            raise ValueError("binary shift candidate_id is invalid")
        integer_values = (
            self.row_shift_pixels,
            self.column_shift_pixels,
            self.grid_width,
            self.grid_height,
            self.previous_support_pixels,
            self.current_support_pixels,
            self.comparison_window_pixels,
            self.overlap_pixels,
            self.previous_support_inside_grid,
            self.previous_support_outside_grid,
            self.current_support_inside_comparison_window,
            self.current_support_outside_comparison_window,
            self.previous_only_inside_window,
            self.current_only_inside_window,
            self.both_dry_inside_window,
            self.previous_unmatched_support_pixels,
            self.current_unmatched_support_pixels,
            self.full_support_union_pixels,
        )
        if any(isinstance(value, bool) or not isinstance(value, int) for value in integer_values):
            raise ValueError("binary shift integer accounting is invalid")
        if (
            self.grid_width <= 0
            or self.grid_height <= 0
            or not -(self.grid_height - 1)
            <= self.row_shift_pixels
            <= self.grid_height - 1
            or not -(self.grid_width - 1)
            <= self.column_shift_pixels
            <= self.grid_width - 1
        ):
            raise ValueError("binary shift lies outside the complete domain")
        for label, value in (
            ("x_resolution_m", self.x_resolution_m),
            ("y_resolution_m", self.y_resolution_m),
            ("elapsed_seconds", self.elapsed_seconds),
        ):
            if _finite(value, label) <= 0.0:
                raise ValueError(f"{label} must be positive")
        for label, value in (
            ("displacement_x_m", self.displacement_x_m),
            ("displacement_y_m", self.displacement_y_m),
            ("displacement_distance_m", self.displacement_distance_m),
            ("interval_speed_kmh", self.interval_speed_kmh),
        ):
            _finite(value, label)
        if not math.isclose(
            self.displacement_x_m,
            self.column_shift_pixels * self.x_resolution_m,
            rel_tol=0.0,
            abs_tol=1e-9,
        ) or not math.isclose(
            self.displacement_y_m,
            -self.row_shift_pixels * self.y_resolution_m,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise ValueError("binary shift metric displacement differs")
        expected_distance = math.hypot(
            self.displacement_x_m, self.displacement_y_m
        )
        if not math.isclose(
            self.displacement_distance_m,
            expected_distance,
            rel_tol=1e-12,
            abs_tol=1e-9,
        ) or not math.isclose(
            self.interval_speed_kmh,
            expected_distance / self.elapsed_seconds * 3.6,
            rel_tol=1e-12,
            abs_tol=1e-9,
        ):
            raise ValueError("binary shift speed/distance accounting differs")
        expected_window = (
            self.grid_height - abs(self.row_shift_pixels)
        ) * (
            self.grid_width - abs(self.column_shift_pixels)
        )
        if (
            self.comparison_window_pixels != expected_window
            or self.previous_support_inside_grid
            + self.previous_support_outside_grid
            != self.previous_support_pixels
            or self.current_support_inside_comparison_window
            + self.current_support_outside_comparison_window
            != self.current_support_pixels
            or self.previous_only_inside_window
            != self.previous_support_inside_grid - self.overlap_pixels
            or self.current_only_inside_window
            != self.current_support_inside_comparison_window - self.overlap_pixels
            or self.both_dry_inside_window
            != self.comparison_window_pixels
            - self.overlap_pixels
            - self.previous_only_inside_window
            - self.current_only_inside_window
            or self.previous_unmatched_support_pixels
            != self.previous_support_pixels - self.overlap_pixels
            or self.current_unmatched_support_pixels
            != self.current_support_pixels - self.overlap_pixels
            or self.full_support_union_pixels
            != self.previous_support_pixels
            + self.current_support_pixels
            - self.overlap_pixels
        ):
            raise ValueError("binary shift non-circular accounting differs")
        if min(
            self.comparison_window_pixels,
            self.overlap_pixels,
            self.previous_support_inside_grid,
            self.previous_support_outside_grid,
            self.current_support_inside_comparison_window,
            self.current_support_outside_comparison_window,
            self.previous_only_inside_window,
            self.current_only_inside_window,
            self.both_dry_inside_window,
            self.previous_unmatched_support_pixels,
            self.current_unmatched_support_pixels,
            self.full_support_union_pixels,
        ) < 0:
            raise ValueError("binary shift pixel counts cannot be negative")
        expected_fractions = (
            self.overlap_pixels / self.previous_support_pixels,
            self.overlap_pixels / self.current_support_pixels,
            self.overlap_pixels / min(
                self.previous_support_pixels, self.current_support_pixels
            ),
            self.overlap_pixels / self.full_support_union_pixels,
            2.0 * self.overlap_pixels
            / (self.previous_support_pixels + self.current_support_pixels),
        )
        actual_fractions = (
            self.overlap_fraction_previous,
            self.overlap_fraction_current,
            self.overlap_fraction_smaller_support,
            self.jaccard_full_support,
            self.dice_full_support,
        )
        if any(
            not math.isfinite(actual)
            or not 0.0 <= actual <= 1.0
            or not math.isclose(
                actual, expected, rel_tol=1e-12, abs_tol=1e-12
            )
            for actual, expected in zip(actual_fractions, expected_fractions)
        ):
            raise ValueError("binary shift overlap fractions differ")

    def to_mapping(self) -> dict[str, Any]:
        return {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
        }


@dataclass(frozen=True, slots=True)
class SharedSystemTranslationResult:
    result_sha256: str
    status: TranslationStatus
    previous_frame_id: str
    current_frame_id: str
    previous_frame_sha256: str
    current_frame_sha256: str
    config_sha256: str
    elapsed_seconds: float
    selected_candidate: BinaryShiftTranslationCandidate | None
    runner_up_candidate: BinaryShiftTranslationCandidate | None
    unique_peak: bool | None
    peak_tie_count: int
    peak_margin_pixels: int | None
    uncertainty_reason_codes: tuple[str, ...]
    indeterminate_reason_codes: tuple[str, ...]
    accounting: SharedSystemTranslationAccounting
    assignment_inputs_used: bool = False
    speed_or_distance_gate_applied: bool = False
    meteorological_admission_granted: bool = False
    operational_snapshot_claim_generated: bool = False
    algorithm_version: str = SHARED_SYSTEM_TRANSLATION_VERSION

    def __post_init__(self) -> None:
        if (
            not isinstance(self.result_sha256, str)
            or len(self.result_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.result_sha256)
        ):
            raise ValueError("shared translation result SHA is invalid")
        if self.status not in (
            "resolved_unique_peak",
            "estimated_with_uncertainty",
            "indeterminate",
        ):
            raise ValueError("shared translation status is invalid")
        for value, label in (
            (self.previous_frame_id, "previous_frame_id"),
            (self.current_frame_id, "current_frame_id"),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"{label} is invalid")
        for value, label in (
            (self.previous_frame_sha256, "previous_frame_sha256"),
            (self.current_frame_sha256, "current_frame_sha256"),
            (self.config_sha256, "config_sha256"),
        ):
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ValueError(f"{label} is invalid")
        if _finite(self.elapsed_seconds, "translation elapsed_seconds") <= 0.0:
            raise ValueError("translation elapsed_seconds must be positive")
        if self.uncertainty_reason_codes != tuple(
            sorted(set(self.uncertainty_reason_codes))
        ) or self.indeterminate_reason_codes != tuple(
            sorted(set(self.indeterminate_reason_codes))
        ):
            raise ValueError("translation reason codes must be canonical")
        if (
            isinstance(self.peak_tie_count, bool)
            or not isinstance(self.peak_tie_count, int)
            or self.peak_tie_count < 0
            or self.peak_margin_pixels is not None
            and (
                isinstance(self.peak_margin_pixels, bool)
                or not isinstance(self.peak_margin_pixels, int)
                or self.peak_margin_pixels < 0
            )
        ):
            raise ValueError("translation peak accounting is invalid")
        if not isinstance(self.accounting, SharedSystemTranslationAccounting):
            raise TypeError("translation result accounting type differs")
        for label in (
            "assignment_inputs_used",
            "speed_or_distance_gate_applied",
            "meteorological_admission_granted",
            "operational_snapshot_claim_generated",
        ):
            if type(getattr(self, label)) is not bool:
                raise TypeError(f"{label} must be boolean")
        if (
            self.assignment_inputs_used
            or self.speed_or_distance_gate_applied
            or self.meteorological_admission_granted
            or self.operational_snapshot_claim_generated
        ):
            raise ValueError("shared translation contains a forbidden claim or gate")
        if self.algorithm_version != SHARED_SYSTEM_TRANSLATION_VERSION:
            raise ValueError("shared translation algorithm version differs")

        if self.status == "resolved_unique_peak":
            if (
                self.uncertainty_reason_codes
                or self.indeterminate_reason_codes
                or not isinstance(
                    self.selected_candidate, BinaryShiftTranslationCandidate
                )
                or self.unique_peak is not True
                or self.peak_tie_count != 1
            ):
                raise ValueError("resolved shared translation is inconsistent")
        elif self.status == "estimated_with_uncertainty":
            if (
                not self.uncertainty_reason_codes
                or self.indeterminate_reason_codes
                or not isinstance(
                    self.selected_candidate, BinaryShiftTranslationCandidate
                )
            ):
                raise ValueError("uncertain shared translation is inconsistent")
        elif (
            not self.indeterminate_reason_codes
            or self.uncertainty_reason_codes
            or self.selected_candidate is not None
        ):
            raise ValueError("indeterminate translation is inconsistent")
        if self.selected_candidate is None:
            if (
                self.runner_up_candidate is not None
                or self.unique_peak is not None
                or self.peak_tie_count != 0
                or self.peak_margin_pixels is not None
            ):
                raise ValueError("translation without peak exposes peak metadata")
        else:
            if self.unique_peak not in (True, False) or self.peak_tie_count < 1:
                raise ValueError("translation peak uniqueness is incomplete")
            expected_margin = (
                None
                if self.runner_up_candidate is None
                else self.selected_candidate.overlap_pixels
                - self.runner_up_candidate.overlap_pixels
            )
            if self.peak_margin_pixels != expected_margin:
                raise ValueError("translation peak/runner-up margin differs")
            if self.unique_peak != (self.peak_tie_count == 1):
                raise ValueError("translation peak tie count differs")
        if self.result_sha256 != _sha_mapping(self.payload_mapping()):
            raise ValueError("shared translation result checksum differs")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "accounting": self.accounting.to_mapping(),
            "algorithm_version": self.algorithm_version,
            "assignment_inputs_used": self.assignment_inputs_used,
            "config_sha256": self.config_sha256,
            "current_frame_id": self.current_frame_id,
            "current_frame_sha256": self.current_frame_sha256,
            "elapsed_seconds": self.elapsed_seconds,
            "indeterminate_reason_codes": list(self.indeterminate_reason_codes),
            "meteorological_admission_granted": (
                self.meteorological_admission_granted
            ),
            "operational_snapshot_claim_generated": (
                self.operational_snapshot_claim_generated
            ),
            "peak_margin_pixels": self.peak_margin_pixels,
            "peak_tie_count": self.peak_tie_count,
            "previous_frame_id": self.previous_frame_id,
            "previous_frame_sha256": self.previous_frame_sha256,
            "runner_up_candidate": (
                None
                if self.runner_up_candidate is None
                else self.runner_up_candidate.to_mapping()
            ),
            "selected_candidate": (
                None
                if self.selected_candidate is None
                else self.selected_candidate.to_mapping()
            ),
            "speed_or_distance_gate_applied": (
                self.speed_or_distance_gate_applied
            ),
            "status": self.status,
            "uncertainty_reason_codes": list(self.uncertainty_reason_codes),
            "unique_peak": self.unique_peak,
        }

    def to_mapping(self) -> dict[str, Any]:
        value = self.payload_mapping()
        value["result_sha256"] = self.result_sha256
        return value


def _accounting(
    previous: BinaryRainShieldFootprintFrame,
    current: BinaryRainShieldFootprintFrame,
    *,
    pair_evaluated: int = 0,
    nonzero_discovered: int = 0,
    shifts_evaluated: int = 0,
    search_complete: bool = False,
) -> SharedSystemTranslationAccounting:
    grid = previous.grid
    previous_support = len(previous.support_pixels)
    current_support = len(current.support_pixels)
    pixels = grid.width * grid.height
    return SharedSystemTranslationAccounting(
        grid_width=grid.width,
        grid_height=grid.height,
        grid_pixels=pixels,
        previous_footprint_objects=len(previous.rain_shield_footprints),
        current_footprint_objects=len(current.rain_shield_footprints),
        previous_pixel_references=previous_support,
        current_pixel_references=current_support,
        previous_support_pixels=previous_support,
        current_support_pixels=current_support,
        previous_background_pixels=pixels - previous_support,
        current_background_pixels=pixels - current_support,
        full_shift_candidates=(2 * grid.height - 1) * (2 * grid.width - 1),
        shift_candidates_evaluated=shifts_evaluated,
        pair_contributions_expected=previous_support * current_support,
        pair_contributions_evaluated=pair_evaluated,
        overlap_histogram_mass=pair_evaluated,
        nonzero_shift_candidates_discovered=nonzero_discovered,
        search_complete=search_complete,
    )


def _build_result(
    *,
    status: TranslationStatus,
    previous: BinaryRainShieldFootprintFrame,
    current: BinaryRainShieldFootprintFrame,
    config: SharedSystemTranslationConfig,
    selected: BinaryShiftTranslationCandidate | None,
    runner_up: BinaryShiftTranslationCandidate | None,
    unique_peak: bool | None,
    peak_tie_count: int,
    peak_margin_pixels: int | None,
    uncertainty_reasons: tuple[str, ...],
    indeterminate_reasons: tuple[str, ...],
    accounting: SharedSystemTranslationAccounting,
) -> SharedSystemTranslationResult:
    values = {
        "status": status,
        "previous_frame_id": previous.frame_id,
        "current_frame_id": current.frame_id,
        "previous_frame_sha256": previous.fingerprint,
        "current_frame_sha256": current.fingerprint,
        "config_sha256": config.fingerprint,
        "elapsed_seconds": current.valid_at - previous.valid_at,
        "selected_candidate": selected,
        "runner_up_candidate": runner_up,
        "unique_peak": unique_peak,
        "peak_tie_count": peak_tie_count,
        "peak_margin_pixels": peak_margin_pixels,
        "uncertainty_reason_codes": tuple(sorted(set(uncertainty_reasons))),
        "indeterminate_reason_codes": tuple(sorted(set(indeterminate_reasons))),
        "accounting": accounting,
    }
    temporary = object.__new__(SharedSystemTranslationResult)
    for name, value in values.items():
        object.__setattr__(temporary, name, value)
    object.__setattr__(temporary, "result_sha256", "0" * 64)
    for name, value in (
        ("assignment_inputs_used", False),
        ("speed_or_distance_gate_applied", False),
        ("meteorological_admission_granted", False),
        ("operational_snapshot_claim_generated", False),
        ("algorithm_version", SHARED_SYSTEM_TRANSLATION_VERSION),
    ):
        object.__setattr__(temporary, name, value)
    return SharedSystemTranslationResult(
        result_sha256=_sha_mapping(temporary.payload_mapping()),
        **values,
    )


def _candidate(
    *,
    row_shift: int,
    column_shift: int,
    overlap: int,
    previous: BinaryRainShieldFootprintFrame,
    current: BinaryRainShieldFootprintFrame,
    config: SharedSystemTranslationConfig,
) -> BinaryShiftTranslationCandidate:
    grid = previous.grid
    previous_support = previous.support_pixels
    current_support = current.support_pixels
    previous_inside = sum(
        0 <= row + row_shift < grid.height
        and 0 <= column + column_shift < grid.width
        for row, column in previous_support
    )
    row_start = max(0, row_shift)
    row_stop = min(grid.height, grid.height + row_shift)
    column_start = max(0, column_shift)
    column_stop = min(grid.width, grid.width + column_shift)
    current_inside = sum(
        row_start <= row < row_stop
        and column_start <= column < column_stop
        for row, column in current_support
    )
    comparison_window = (row_stop - row_start) * (column_stop - column_start)
    previous_only = previous_inside - overlap
    current_only = current_inside - overlap
    both_dry = comparison_window - overlap - previous_only - current_only
    previous_count = len(previous_support)
    current_count = len(current_support)
    union = previous_count + current_count - overlap
    elapsed = current.valid_at - previous.valid_at
    dx = column_shift * grid.x_resolution_m
    dy = -row_shift * grid.y_resolution_m
    distance = math.hypot(dx, dy)
    payload = {
        "algorithm": SHARED_SYSTEM_TRANSLATION_VERSION,
        "column_shift_pixels": column_shift,
        "config_sha256": config.fingerprint,
        "current_frame_sha256": current.fingerprint,
        "overlap_pixels": overlap,
        "previous_frame_sha256": previous.fingerprint,
        "row_shift_pixels": row_shift,
    }
    return BinaryShiftTranslationCandidate(
        candidate_id="binary-shift:" + _sha_mapping(payload),
        row_shift_pixels=row_shift,
        column_shift_pixels=column_shift,
        displacement_x_m=dx,
        displacement_y_m=dy,
        displacement_distance_m=distance,
        interval_speed_kmh=distance / elapsed * 3.6,
        elapsed_seconds=elapsed,
        grid_width=grid.width,
        grid_height=grid.height,
        x_resolution_m=grid.x_resolution_m,
        y_resolution_m=grid.y_resolution_m,
        previous_support_pixels=previous_count,
        current_support_pixels=current_count,
        comparison_window_pixels=comparison_window,
        overlap_pixels=overlap,
        previous_support_inside_grid=previous_inside,
        previous_support_outside_grid=previous_count - previous_inside,
        current_support_inside_comparison_window=current_inside,
        current_support_outside_comparison_window=current_count - current_inside,
        previous_only_inside_window=previous_only,
        current_only_inside_window=current_only,
        both_dry_inside_window=both_dry,
        previous_unmatched_support_pixels=previous_count - overlap,
        current_unmatched_support_pixels=current_count - overlap,
        full_support_union_pixels=union,
        overlap_fraction_previous=overlap / previous_count,
        overlap_fraction_current=overlap / current_count,
        overlap_fraction_smaller_support=overlap / min(
            previous_count, current_count
        ),
        jaccard_full_support=overlap / union,
        dice_full_support=2.0 * overlap / (previous_count + current_count),
    )


def estimate_shared_system_translation(
    previous: BinaryRainShieldFootprintFrame,
    current: BinaryRainShieldFootprintFrame,
    config: SharedSystemTranslationConfig | None = None,
) -> SharedSystemTranslationResult:
    """Return the complete bounded binary-overlap translation evidence.

    Every possible overlap-producing integer shift on the unchanged grid is in
    the domain.  The compute caps only decide whether that complete search can
    be performed; they never reject a shift because of its distance or speed.
    """

    if not isinstance(previous, BinaryRainShieldFootprintFrame) or not isinstance(
        current, BinaryRainShieldFootprintFrame
    ):
        raise TypeError("shared translation requires two binary shield frames")
    selected_config = config or SharedSystemTranslationConfig()
    if not isinstance(selected_config, SharedSystemTranslationConfig):
        raise TypeError("shared translation config type differs")
    previous.grid.assert_same_geometry(current.grid)
    if current.valid_at <= previous.valid_at:
        raise ValueError("shared translation frames must advance in time")
    if current.frame_id == previous.frame_id:
        raise ValueError("shared translation frames require unique IDs")

    grid_pixels = previous.grid.width * previous.grid.height
    previous_count = len(previous.support_pixels)
    current_count = len(current.support_pixels)
    full_domain = (
        (2 * previous.grid.height - 1) * (2 * previous.grid.width - 1)
    )
    pair_expected = previous_count * current_count
    capacity_reasons: list[str] = []
    if grid_pixels > selected_config.max_grid_pixels:
        capacity_reasons.append("grid_pixel_capacity_exceeded")
    if max(previous_count, current_count) > (
        selected_config.max_support_pixels_per_frame
    ):
        capacity_reasons.append("support_pixel_capacity_exceeded")
    if full_domain > selected_config.max_shift_candidates:
        capacity_reasons.append("full_shift_domain_capacity_exceeded")
    if pair_expected > selected_config.max_pair_contributions:
        capacity_reasons.append("pair_contribution_capacity_exceeded")
    if capacity_reasons:
        return _build_result(
            status="indeterminate",
            previous=previous,
            current=current,
            config=selected_config,
            selected=None,
            runner_up=None,
            unique_peak=None,
            peak_tie_count=0,
            peak_margin_pixels=None,
            uncertainty_reasons=(),
            indeterminate_reasons=tuple(capacity_reasons),
            accounting=_accounting(previous, current),
        )
    absence_reasons = []
    if previous_count == 0:
        absence_reasons.append("previous_rain_shield_support_absent")
    if current_count == 0:
        absence_reasons.append("current_rain_shield_support_absent")
    if absence_reasons:
        return _build_result(
            status="indeterminate",
            previous=previous,
            current=current,
            config=selected_config,
            selected=None,
            runner_up=None,
            unique_peak=None,
            peak_tie_count=0,
            peak_margin_pixels=None,
            uncertainty_reasons=(),
            indeterminate_reasons=tuple(absence_reasons),
            accounting=_accounting(previous, current),
        )

    counts: dict[tuple[int, int], int] = {}
    pair_evaluated = 0
    for previous_row, previous_column in previous.support_pixels:
        for current_row, current_column in current.support_pixels:
            shift = (
                current_row - previous_row,
                current_column - previous_column,
            )
            if (
                shift not in counts
                and len(counts)
                >= selected_config.max_nonzero_shift_candidates
            ):
                return _build_result(
                    status="indeterminate",
                    previous=previous,
                    current=current,
                    config=selected_config,
                    selected=None,
                    runner_up=None,
                    unique_peak=None,
                    peak_tie_count=0,
                    peak_margin_pixels=None,
                    uncertainty_reasons=(),
                    indeterminate_reasons=(
                        "nonzero_shift_candidate_capacity_exceeded",
                    ),
                    accounting=_accounting(
                        previous,
                        current,
                        pair_evaluated=pair_evaluated,
                        nonzero_discovered=len(counts),
                    ),
                )
            counts[shift] = counts.get(shift, 0) + 1
            pair_evaluated += 1

    best: tuple[tuple[float, int, int, int], int, int, int] | None = None
    runner: tuple[tuple[float, int, int, int], int, int, int] | None = None
    peak_overlap = -1
    peak_ties = 0
    shifts_evaluated = 0
    grid = previous.grid
    for row_shift in range(-(grid.height - 1), grid.height):
        for column_shift in range(-(grid.width - 1), grid.width):
            overlap = counts.get((row_shift, column_shift), 0)
            distance_squared = (
                (column_shift * grid.x_resolution_m) ** 2
                + (row_shift * grid.y_resolution_m) ** 2
            )
            rank = (-overlap, distance_squared, row_shift, column_shift)
            item = (rank, row_shift, column_shift, overlap)
            if best is None or rank < best[0]:
                runner = best
                best = item
            elif runner is None or rank < runner[0]:
                runner = item
            if overlap > peak_overlap:
                peak_overlap = overlap
                peak_ties = 1
            elif overlap == peak_overlap:
                peak_ties += 1
            shifts_evaluated += 1
    if best is None or peak_overlap <= 0:
        raise AssertionError("non-empty binary supports produced no overlap shift")

    selected_candidate = _candidate(
        row_shift=best[1],
        column_shift=best[2],
        overlap=best[3],
        previous=previous,
        current=current,
        config=selected_config,
    )
    runner_candidate = (
        None
        if runner is None
        else _candidate(
            row_shift=runner[1],
            column_shift=runner[2],
            overlap=runner[3],
            previous=previous,
            current=current,
            config=selected_config,
        )
    )
    uncertainty_reasons = []
    if selected_candidate.overlap_fraction_smaller_support < (
        selected_config.low_overlap_uncertainty_fraction
    ):
        uncertainty_reasons.append("low_overlap_support")
    unique_peak = peak_ties == 1
    if not unique_peak:
        uncertainty_reasons.append("peak_not_unique")
    if (
        (
            grid.height > 1
            and abs(selected_candidate.row_shift_pixels) == grid.height - 1
        )
        or (
            grid.width > 1
            and abs(selected_candidate.column_shift_pixels) == grid.width - 1
        )
    ):
        uncertainty_reasons.append("peak_on_full_search_boundary")
    accounting = _accounting(
        previous,
        current,
        pair_evaluated=pair_evaluated,
        nonzero_discovered=len(counts),
        shifts_evaluated=shifts_evaluated,
        search_complete=True,
    )
    return _build_result(
        status=(
            "estimated_with_uncertainty"
            if uncertainty_reasons
            else "resolved_unique_peak"
        ),
        previous=previous,
        current=current,
        config=selected_config,
        selected=selected_candidate,
        runner_up=runner_candidate,
        unique_peak=unique_peak,
        peak_tie_count=peak_ties,
        peak_margin_pixels=(
            None
            if runner_candidate is None
            else selected_candidate.overlap_pixels
            - runner_candidate.overlap_pixels
        ),
        uncertainty_reasons=tuple(uncertainty_reasons),
        indeterminate_reasons=(),
        accounting=accounting,
    )


__all__ = [
    "BinaryRainShieldFootprintFrame",
    "BinaryShiftTranslationCandidate",
    "HARD_MAX_GRID_PIXELS",
    "HARD_MAX_NONZERO_SHIFT_CANDIDATES",
    "HARD_MAX_PAIR_CONTRIBUTIONS",
    "HARD_MAX_SHIFT_CANDIDATES",
    "HARD_MAX_SUPPORT_PIXELS_PER_FRAME",
    "SHARED_SYSTEM_TRANSLATION_VERSION",
    "SharedSystemTranslationAccounting",
    "SharedSystemTranslationConfig",
    "SharedSystemTranslationResult",
    "estimate_shared_system_translation",
]
