"""Pure shadow-only deterministic nowcast baselines.

The module deliberately does not estimate probabilities, calibrate confidence,
or select a production forecast.  It provides two auditable references:

* Eulerian persistence: ``R(x, t + dt) = R(x, t)``;
* two-frame constant-motion (Lagrangian) persistence:
  ``R(x, t + dt) = R(x - u * dt, t)``.

All positions and velocities are SI metric values on an :class:`AnalysisGrid`.
Timestamps are UTC Unix seconds supplied by the caller.  Missing data is an
explicit boolean mask and is never silently converted to zero precipitation.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum

from .analysis_grid import AnalysisGrid


NOWCAST_BASELINE_CONTRACT_VERSION = "stv3-shadow-nowcast-baselines-v1"
EULERIAN_PERSISTENCE_VERSION = "stv3-eulerian-persistence-v1"
TWO_FRAME_TRANSLATION_VERSION = "stv3-two-frame-translation-v1"
_TIME_TOLERANCE_SECONDS = 1e-6
_INDEX_TOLERANCE = 1e-12


class BaselineMethod(str, Enum):
    """Deterministic reference methods; neither value is probabilistic."""

    EULERIAN_PERSISTENCE = "eulerian_persistence"
    TWO_FRAME_CONSTANT_MOTION = "two_frame_constant_motion"


@dataclass(frozen=True, slots=True)
class MetricRasterField:
    """One immutable physical field on a metric grid.

    ``values`` always contains finite numeric storage values.  ``valid_mask``
    is authoritative: a ``False`` cell is nodata regardless of its storage
    value.  This separation prevents zero-filled arrays from becoming false
    dry observations.
    """

    field_id: str
    valid_at: float
    quantity: str
    unit: str
    grid: AnalysisGrid
    values: tuple[tuple[float, ...], ...]
    valid_mask: tuple[tuple[bool, ...], ...]

    def __post_init__(self) -> None:
        for label, value in (
            ("field_id", self.field_id),
            ("quantity", self.quantity),
            ("unit", self.unit),
        ):
            _require_nonempty_string(value, label)
        _require_finite(self.valid_at, "valid_at")
        if not isinstance(self.grid, AnalysisGrid):
            raise TypeError("MetricRasterField requires an AnalysisGrid")
        _validate_matrix_shape(self.values, self.grid, "values")
        _validate_matrix_shape(self.valid_mask, self.grid, "valid_mask")
        for row in self.values:
            if any(
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                for value in row
            ):
                raise ValueError("raster values must be finite numbers")
        for row in self.valid_mask:
            if any(not isinstance(value, bool) for value in row):
                raise TypeError("valid_mask must contain only booleans")

    def value_or_none(self, row: int, column: int) -> float | None:
        """Return a physical value only when the explicit mask is valid."""

        self.grid._validate_index(row, column)
        if not self.valid_mask[row][column]:
            return None
        return float(self.values[row][column])


@dataclass(frozen=True, slots=True)
class ConstantMotionVector:
    """A caller-supplied, two-frame constant translation in SI units.

    The support timestamps must be strictly increasing and end at
    ``reference_valid_at``.  The class does not reject, clip, or smooth any
    finite velocity, however large it is.
    """

    motion_id: str
    analysis_grid_id: str
    reference_valid_at: float
    vx_m_s: float
    vy_m_s: float
    support_field_ids: tuple[str, str]
    support_valid_at: tuple[float, float]
    method: str
    algorithm_version: str

    def __post_init__(self) -> None:
        for label, value in (
            ("motion_id", self.motion_id),
            ("analysis_grid_id", self.analysis_grid_id),
            ("method", self.method),
            ("algorithm_version", self.algorithm_version),
        ):
            _require_nonempty_string(value, label)
        for label, value in (
            ("reference_valid_at", self.reference_valid_at),
            ("vx_m_s", self.vx_m_s),
            ("vy_m_s", self.vy_m_s),
        ):
            _require_finite(value, label)
        if (
            not isinstance(self.support_field_ids, tuple)
            or len(self.support_field_ids) != 2
            or any(
                not isinstance(value, str) or not value
                for value in self.support_field_ids
            )
            or len(set(self.support_field_ids)) != 2
        ):
            raise ValueError("constant motion requires two unique support fields")
        if (
            not isinstance(self.support_valid_at, tuple)
            or len(self.support_valid_at) != 2
        ):
            raise ValueError("constant motion requires two support timestamps")
        previous_at, current_at = self.support_valid_at
        _require_finite(previous_at, "support_valid_at[0]")
        _require_finite(current_at, "support_valid_at[1]")
        if current_at <= previous_at:
            raise ValueError("support timestamps must be strictly increasing")
        if not math.isclose(
            current_at,
            self.reference_valid_at,
            rel_tol=0.0,
            abs_tol=_TIME_TOLERANCE_SECONDS,
        ):
            raise ValueError("motion reference time must equal newest support time")

    @property
    def observed_interval_seconds(self) -> float:
        return self.support_valid_at[1] - self.support_valid_at[0]

    @property
    def speed_m_s(self) -> float:
        return math.hypot(self.vx_m_s, self.vy_m_s)

    @property
    def speed_kmh(self) -> float:
        return self.speed_m_s * 3.6


@dataclass(frozen=True, slots=True)
class BaselineForecastField:
    """One deterministic baseline field at one explicit lead time."""

    forecast_field_id: str
    method: BaselineMethod
    reference_valid_at: float
    target_valid_at: float
    lead_time_seconds: float
    quantity: str
    unit: str
    grid: AnalysisGrid
    values: tuple[tuple[float, ...], ...]
    valid_mask: tuple[tuple[bool, ...], ...]
    input_field_ids: tuple[str, ...]
    motion_id: str | None
    interpolation: str
    algorithm_version: str
    contract_version: str = NOWCAST_BASELINE_CONTRACT_VERSION

    def __post_init__(self) -> None:
        for label, value in (
            ("forecast_field_id", self.forecast_field_id),
            ("quantity", self.quantity),
            ("unit", self.unit),
            ("interpolation", self.interpolation),
            ("algorithm_version", self.algorithm_version),
            ("contract_version", self.contract_version),
        ):
            _require_nonempty_string(value, label)
        if not isinstance(self.method, BaselineMethod):
            raise TypeError("method must be a BaselineMethod")
        for label, value in (
            ("reference_valid_at", self.reference_valid_at),
            ("target_valid_at", self.target_valid_at),
            ("lead_time_seconds", self.lead_time_seconds),
        ):
            _require_finite(value, label)
        if self.lead_time_seconds <= 0.0:
            raise ValueError("lead_time_seconds must be positive")
        if not math.isclose(
            self.target_valid_at - self.reference_valid_at,
            self.lead_time_seconds,
            rel_tol=1e-12,
            abs_tol=_TIME_TOLERANCE_SECONDS,
        ):
            raise ValueError("lead time must equal target minus reference timestamp")
        if not isinstance(self.grid, AnalysisGrid):
            raise TypeError("forecast field requires an AnalysisGrid")
        _validate_matrix_shape(self.values, self.grid, "values")
        _validate_matrix_shape(self.valid_mask, self.grid, "valid_mask")
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            for row in self.values
            for value in row
        ):
            raise ValueError("forecast raster values must be finite")
        if any(
            not isinstance(value, bool)
            for row in self.valid_mask
            for value in row
        ):
            raise TypeError("forecast valid_mask must contain booleans")
        _validate_unique_ids(self.input_field_ids, "input_field_ids")
        if self.method is BaselineMethod.EULERIAN_PERSISTENCE:
            if self.motion_id is not None or self.interpolation != "none":
                raise ValueError("Eulerian persistence cannot carry motion metadata")
        elif (
            not isinstance(self.motion_id, str)
            or not self.motion_id
            or self.interpolation != "bilinear_strict_nodata"
        ):
            raise ValueError(
                "constant-motion forecast requires motion and interpolation metadata"
            )

    def value_or_none(self, row: int, column: int) -> float | None:
        self.grid._validate_index(row, column)
        if not self.valid_mask[row][column]:
            return None
        return float(self.values[row][column])


def derive_two_frame_constant_motion(
    *,
    motion_id: str,
    analysis_grid_id: str,
    previous_field_id: str,
    current_field_id: str,
    previous_valid_at: float,
    current_valid_at: float,
    previous_x_m: float,
    previous_y_m: float,
    current_x_m: float,
    current_y_m: float,
    method: str,
    algorithm_version: str,
) -> ConstantMotionVector:
    """Derive exact two-position linear motion using the real frame interval."""

    for label, value in (
        ("previous_valid_at", previous_valid_at),
        ("current_valid_at", current_valid_at),
        ("previous_x_m", previous_x_m),
        ("previous_y_m", previous_y_m),
        ("current_x_m", current_x_m),
        ("current_y_m", current_y_m),
    ):
        _require_finite(value, label)
    elapsed = current_valid_at - previous_valid_at
    if elapsed <= 0.0:
        raise ValueError("two-frame motion requires increasing real timestamps")
    return ConstantMotionVector(
        motion_id=motion_id,
        analysis_grid_id=analysis_grid_id,
        reference_valid_at=current_valid_at,
        vx_m_s=(current_x_m - previous_x_m) / elapsed,
        vy_m_s=(current_y_m - previous_y_m) / elapsed,
        support_field_ids=(previous_field_id, current_field_id),
        support_valid_at=(previous_valid_at, current_valid_at),
        method=method,
        algorithm_version=algorithm_version,
    )


def eulerian_persistence(
    field: MetricRasterField,
    *,
    target_valid_at: float,
    forecast_field_id: str,
) -> BaselineForecastField:
    """Copy the latest observed field to a future time without modification."""

    _validate_future_target(field, target_valid_at)
    values, mask = _canonicalize_masked_values(field.values, field.valid_mask)
    return BaselineForecastField(
        forecast_field_id=forecast_field_id,
        method=BaselineMethod.EULERIAN_PERSISTENCE,
        reference_valid_at=field.valid_at,
        target_valid_at=target_valid_at,
        lead_time_seconds=target_valid_at - field.valid_at,
        quantity=field.quantity,
        unit=field.unit,
        grid=field.grid,
        values=values,
        valid_mask=mask,
        input_field_ids=(field.field_id,),
        motion_id=None,
        interpolation="none",
        algorithm_version=EULERIAN_PERSISTENCE_VERSION,
    )


def lagrangian_constant_motion(
    field: MetricRasterField,
    motion: ConstantMotionVector,
    *,
    target_valid_at: float,
    forecast_field_id: str,
) -> BaselineForecastField:
    """Advect a field with strict-nodata semi-Lagrangian bilinear sampling."""

    if not isinstance(field, MetricRasterField):
        raise TypeError("constant-motion baseline requires a MetricRasterField")
    if not isinstance(motion, ConstantMotionVector):
        raise TypeError("constant-motion baseline requires ConstantMotionVector")
    _validate_future_target(field, target_valid_at)
    if motion.analysis_grid_id != field.grid.grid_id:
        raise ValueError("motion and raster must use the same analysis grid")
    if field.field_id != motion.support_field_ids[-1]:
        raise ValueError("forecast field must be the newest motion support field")
    if not math.isclose(
        motion.reference_valid_at,
        field.valid_at,
        rel_tol=0.0,
        abs_tol=_TIME_TOLERANCE_SECONDS,
    ):
        raise ValueError("motion reference time must equal raster valid time")

    elapsed = target_valid_at - field.valid_at
    displacement_x_m = motion.vx_m_s * elapsed
    displacement_y_m = motion.vy_m_s * elapsed
    output_values: list[tuple[float, ...]] = []
    output_mask: list[tuple[bool, ...]] = []
    for row in range(field.grid.height):
        value_row: list[float] = []
        mask_row: list[bool] = []
        for column in range(field.grid.width):
            target_x_m, target_y_m = field.grid.pixel_center(row, column)
            sampled = _strict_bilinear_sample(
                field,
                source_x_m=target_x_m - displacement_x_m,
                source_y_m=target_y_m - displacement_y_m,
            )
            if sampled is None:
                value_row.append(0.0)
                mask_row.append(False)
            else:
                value_row.append(sampled)
                mask_row.append(True)
        output_values.append(tuple(value_row))
        output_mask.append(tuple(mask_row))

    return BaselineForecastField(
        forecast_field_id=forecast_field_id,
        method=BaselineMethod.TWO_FRAME_CONSTANT_MOTION,
        reference_valid_at=field.valid_at,
        target_valid_at=target_valid_at,
        lead_time_seconds=elapsed,
        quantity=field.quantity,
        unit=field.unit,
        grid=field.grid,
        values=tuple(output_values),
        valid_mask=tuple(output_mask),
        input_field_ids=motion.support_field_ids,
        motion_id=motion.motion_id,
        interpolation="bilinear_strict_nodata",
        algorithm_version=TWO_FRAME_TRANSLATION_VERSION,
    )


def _strict_bilinear_sample(
    field: MetricRasterField,
    *,
    source_x_m: float,
    source_y_m: float,
) -> float | None:
    fractional_row, fractional_column = field.grid.fractional_center_index(
        source_x_m,
        source_y_m,
    )
    fractional_row = _snap_index(fractional_row)
    fractional_column = _snap_index(fractional_column)
    row0 = math.floor(fractional_row)
    column0 = math.floor(fractional_column)
    row_fraction = fractional_row - row0
    column_fraction = fractional_column - column0
    contributors = (
        (row0, column0, (1.0 - row_fraction) * (1.0 - column_fraction)),
        (row0, column0 + 1, (1.0 - row_fraction) * column_fraction),
        (row0 + 1, column0, row_fraction * (1.0 - column_fraction)),
        (row0 + 1, column0 + 1, row_fraction * column_fraction),
    )
    value = 0.0
    for row, column, weight in contributors:
        if weight <= _INDEX_TOLERANCE:
            continue
        if (
            row < 0
            or row >= field.grid.height
            or column < 0
            or column >= field.grid.width
            or not field.valid_mask[row][column]
        ):
            return None
        value += float(field.values[row][column]) * weight
    return value


def _snap_index(value: float) -> float:
    nearest = round(value)
    if math.isclose(value, nearest, rel_tol=0.0, abs_tol=_INDEX_TOLERANCE):
        return float(nearest)
    return value


def _validate_future_target(
    field: MetricRasterField,
    target_valid_at: float,
) -> None:
    if not isinstance(field, MetricRasterField):
        raise TypeError("baseline requires a MetricRasterField")
    _require_finite(target_valid_at, "target_valid_at")
    if target_valid_at <= field.valid_at:
        raise ValueError("target_valid_at must be later than the observed field")


def _canonicalize_masked_values(
    values: tuple[tuple[float, ...], ...],
    mask: tuple[tuple[bool, ...], ...],
) -> tuple[tuple[tuple[float, ...], ...], tuple[tuple[bool, ...], ...]]:
    return (
        tuple(
            tuple(float(value) if valid else 0.0 for value, valid in zip(row, row_mask))
            for row, row_mask in zip(values, mask)
        ),
        tuple(tuple(row) for row in mask),
    )


def _validate_matrix_shape(
    matrix: object,
    grid: AnalysisGrid,
    label: str,
) -> None:
    if not isinstance(matrix, tuple) or any(
        not isinstance(row, tuple) for row in matrix
    ):
        raise TypeError(f"{label} must be immutable tuples")
    grid.validate_raster_shape(matrix)


def _validate_unique_ids(values: tuple[str, ...], label: str) -> None:
    if (
        not isinstance(values, tuple)
        or not values
        or any(not isinstance(value, str) or not value for value in values)
        or len(values) != len(set(values))
    ):
        raise ValueError(f"{label} must contain unique non-empty IDs")


def _require_nonempty_string(value: object, label: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty string")


def _require_finite(value: object, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"{label} must be a finite number")
