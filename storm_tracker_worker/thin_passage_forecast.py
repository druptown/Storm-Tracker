"""Deterministic labelled-QPE advection for target passage candidates."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import TYPE_CHECKING

from pyproj import Transformer

if TYPE_CHECKING:
    from .thin_provider_adapters import ThinRadarField


@dataclass(frozen=True, slots=True)
class PassageForecastPolicy:
    policy_id: str = "candidate_policy_v1"
    horizon_seconds: float = 2 * 60 * 60.0
    step_seconds: float = 60.0
    wet_threshold_mm_h: float = 0.1

    def __post_init__(self) -> None:
        if not self.policy_id:
            raise ValueError("passage_policy_id_invalid")
        if (
            not math.isfinite(self.horizon_seconds)
            or self.horizon_seconds <= 0.0
            or self.horizon_seconds > 2 * 60 * 60.0
        ):
            raise ValueError("passage_horizon_invalid")
        if (
            not math.isfinite(self.step_seconds)
            or self.step_seconds <= 0.0
            or self.step_seconds > self.horizon_seconds
        ):
            raise ValueError("passage_step_invalid")
        if (
            not math.isfinite(self.wet_threshold_mm_h)
            or self.wet_threshold_mm_h < 0.0
        ):
            raise ValueError("passage_wet_threshold_invalid")


@dataclass(frozen=True, slots=True)
class PassageTarget:
    target_id: str
    latitude: float
    longitude: float
    accuracy_km: float = 0.0

    def __post_init__(self) -> None:
        if not self.target_id:
            raise ValueError("passage_target_id_invalid")
        if not all(math.isfinite(value) for value in (
            self.latitude, self.longitude, self.accuracy_km,
        )) or self.accuracy_km < 0.0:
            raise ValueError("passage_target_position_invalid")


@dataclass(frozen=True, slots=True)
class AdvectionMotion:
    vx_m_s: float
    vy_m_s: float
    policy_id: str

    def __post_init__(self) -> None:
        if not all(math.isfinite(value) for value in (self.vx_m_s, self.vy_m_s)):
            raise ValueError("passage_motion_invalid")
        if not self.policy_id:
            raise ValueError("passage_motion_policy_invalid")


@dataclass(frozen=True, slots=True)
class PassageBracket:
    lower_at: float
    upper_at: float

    def __post_init__(self) -> None:
        if (
            not math.isfinite(self.lower_at)
            or not math.isfinite(self.upper_at)
            or self.upper_at < self.lower_at
        ):
            raise ValueError("passage_bracket_invalid")


@dataclass(frozen=True, slots=True)
class PassageInterval:
    target_id: str
    interval_ordinal: int
    entry_bracket: PassageBracket | None
    exit_bracket: PassageBracket | None
    left_censored: bool
    right_censored: bool
    coverage_censored: bool
    mean_intensity_mm_h: float
    peak_intensity_mm_h: float
    method: str
    field_fingerprint: str
    quantity: str
    unit: str
    support_times: tuple[float, ...]


@dataclass(frozen=True, slots=True)
class PassageScenario:
    scenario_id: str
    hit: bool
    miss: bool
    horizon_censored: bool
    intervals: tuple[PassageInterval, ...]


def _sample_source_pixel(
    field: ThinRadarField,
    labelled: frozenset[tuple[int, int]],
    *,
    source_x_m: float,
    source_y_m: float,
    support_radius_m: float,
    wet_threshold_mm_h: float,
) -> tuple[bool, float] | None:
    height, width = field.rain_rate_mm_h.shape
    center_column = math.floor(
        (source_x_m - field.origin_x_m) / field.x_resolution_m
    )
    center_row = math.floor(
        (field.origin_y_m - source_y_m) / field.y_resolution_m
    )
    radius_columns = math.ceil(support_radius_m / field.x_resolution_m)
    radius_rows = math.ceil(support_radius_m / field.y_resolution_m)
    selected: list[tuple[int, int]] = []
    for row in range(center_row - radius_rows, center_row + radius_rows + 1):
        for column in range(
            center_column - radius_columns,
            center_column + radius_columns + 1,
        ):
            if not (0 <= row < height and 0 <= column < width):
                return None
            pixel_x = field.origin_x_m + (column + 0.5) * field.x_resolution_m
            pixel_y = field.origin_y_m - (row + 0.5) * field.y_resolution_m
            if support_radius_m > 0.0 and math.hypot(
                pixel_x - source_x_m,
                pixel_y - source_y_m,
            ) > support_radius_m + 0.5 * math.hypot(
                field.x_resolution_m,
                field.y_resolution_m,
            ):
                continue
            selected.append((row, column))
    if not selected or any(not bool(field.valid_mask[row, column]) for row, column in selected):
        return None
    intensities = tuple(
        float(field.rain_rate_mm_h[row, column])
        for row, column in selected
        if (row, column) in labelled
        and float(field.rain_rate_mm_h[row, column]) >= wet_threshold_mm_h
    )
    return (bool(intensities), max(intensities, default=0.0))


def advect_labelled_qpe(
    field: ThinRadarField,
    labelled_pixels: tuple[tuple[int, int], ...],
    motion: AdvectionMotion,
    target: PassageTarget,
    *,
    evaluated_at: float,
    policy: PassageForecastPolicy = PassageForecastPolicy(),
) -> tuple[PassageInterval, ...]:
    """Advect one system label and its QPE values with a uniform vector."""

    if not field.is_quantitative_precipitation:
        return ()
    assert field.identity is not None
    if field.identity.quality_kind in {"unknown", "unvalidated"}:
        return ()
    evaluated = float(evaluated_at)
    if not math.isfinite(evaluated):
        raise ValueError("passage_evaluated_at_invalid")
    labelled = frozenset(labelled_pixels)
    height, width = field.rain_rate_mm_h.shape
    if not labelled or any(
        row < 0 or row >= height or column < 0 or column >= width
        for row, column in labelled
    ):
        return ()
    target_x, target_y = Transformer.from_crs(
        "EPSG:4326", field.crs, always_xy=True,
    ).transform(target.longitude, target.latitude)
    sample_count = int(math.floor(policy.horizon_seconds / policy.step_seconds))
    offsets = tuple(
        min(policy.horizon_seconds, index * policy.step_seconds)
        for index in range(sample_count + 1)
    )
    if offsets[-1] < policy.horizon_seconds:
        offsets = (*offsets, policy.horizon_seconds)
    samples: list[tuple[float, bool | None, float]] = []
    for offset in offsets:
        sampled = _sample_source_pixel(
            field,
            labelled,
            source_x_m=target_x - motion.vx_m_s * offset,
            source_y_m=target_y - motion.vy_m_s * offset,
            support_radius_m=target.accuracy_km * 1_000.0,
            wet_threshold_mm_h=policy.wet_threshold_mm_h,
        )
        wet, intensity = (None, 0.0) if sampled is None else sampled
        samples.append((evaluated + offset, wet, intensity))

    intervals: list[PassageInterval] = []
    start_index: int | None = None
    entry_bracket: PassageBracket | None = None
    left_censored = False
    invalid_unbracketed_start = False

    def finish(end_index: int, *, right: bool, coverage: bool) -> None:
        nonlocal start_index, entry_bracket, left_censored
        if start_index is None or invalid_unbracketed_start:
            start_index = None
            entry_bracket = None
            left_censored = False
            return
        wet_samples = tuple(
            item for item in samples[start_index:end_index + 1]
            if item[1] is True
        )
        if not wet_samples:
            start_index = None
            return
        exit_bracket = None if right or coverage else PassageBracket(
            samples[end_index][0], samples[end_index + 1][0],
        )
        intensities = tuple(item[2] for item in wet_samples)
        intervals.append(PassageInterval(
            target_id=target.target_id,
            interval_ordinal=len(intervals),
            entry_bracket=entry_bracket,
            exit_bracket=exit_bracket,
            left_censored=left_censored,
            right_censored=right,
            coverage_censored=coverage,
            mean_intensity_mm_h=math.fsum(intensities) / len(intensities),
            peak_intensity_mm_h=max(intensities),
            method="advected_labelled_qpe",
            field_fingerprint=field.identity.content_fingerprint,
            quantity=field.identity.quantity,
            unit=field.identity.unit,
            support_times=tuple(item[0] for item in wet_samples),
        ))
        start_index = None
        entry_bracket = None
        left_censored = False

    for index, (_at, wet, _intensity) in enumerate(samples):
        if wet is None:
            if start_index is not None:
                finish(index - 1, right=False, coverage=True)
            invalid_unbracketed_start = True
            continue
        if wet and start_index is None:
            start_index = index
            left_censored = index == 0
            invalid_unbracketed_start = index > 0 and samples[index - 1][1] is None
            entry_bracket = (
                None
                if left_censored or invalid_unbracketed_start
                else PassageBracket(samples[index - 1][0], samples[index][0])
            )
        last = index == len(samples) - 1
        if start_index is None:
            if not wet:
                invalid_unbracketed_start = False
            continue
        if wet and not last:
            continue
        end_index = index if wet and last else index - 1
        right_censored = wet and last
        finish(end_index, right=right_censored, coverage=False)
    return tuple(intervals)


__all__ = [
    "AdvectionMotion",
    "PassageBracket",
    "PassageForecastPolicy",
    "PassageInterval",
    "PassageScenario",
    "PassageTarget",
    "advect_labelled_qpe",
]
