"""Masked metric Lucas-Kanade optical-flow baseline for shadow tracking.

This is a deterministic, single-scale, windowed Lucas-Kanade estimator.  It
does not clip velocity and does not turn an unsolvable aperture/nodata case
into fake zero motion.  Every pixel carries an explicit status; object-level
aggregation can therefore return ``unavailable`` and let the caller choose a
broad cold-start prior.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import OrderedDict
from dataclasses import dataclass

import numpy as np

from .hierarchical_detection import (
    HierarchicalCellDetection,
    NormalizedRasterFrame,
)
from .motion_prediction import Covariance2D, OpticalFlowSample


LUCAS_KANADE_FLOW_VERSION = "stv3-lucas-kanade-metric-v1"
_AVAILABLE = "available"
_NODATA = "nodata"
_INSUFFICIENT = "insufficient_support"
_ILL_CONDITIONED = "ill_conditioned"


@dataclass(frozen=True, slots=True)
class LucasKanadeConfig:
    """Every numerical/uncertainty choice that changes the flow field."""

    window_radius_pixels: int
    minimum_valid_samples: int
    minimum_structure_eigenvalue: float
    maximum_condition_number: float
    regularization: float
    residual_variance_floor: float
    velocity_process_noise_covariance_m2_s2: Covariance2D

    def __post_init__(self) -> None:
        if (
            isinstance(self.window_radius_pixels, bool)
            or not isinstance(self.window_radius_pixels, int)
            or self.window_radius_pixels < 1
        ):
            raise ValueError("window_radius_pixels moet een positief geheel getal zijn")
        if (
            isinstance(self.minimum_valid_samples, bool)
            or not isinstance(self.minimum_valid_samples, int)
            or self.minimum_valid_samples < 2
        ):
            raise ValueError("minimum_valid_samples moet minstens twee zijn")
        for label, value in (
            ("minimum_structure_eigenvalue", self.minimum_structure_eigenvalue),
            ("maximum_condition_number", self.maximum_condition_number),
            ("regularization", self.regularization),
            ("residual_variance_floor", self.residual_variance_floor),
        ):
            if not math.isfinite(value):
                raise ValueError(f"{label} moet eindig zijn")
        if self.minimum_structure_eigenvalue < 0.0:
            raise ValueError("minimum_structure_eigenvalue mag niet negatief zijn")
        if self.maximum_condition_number < 1.0:
            raise ValueError("maximum_condition_number moet minstens één zijn")
        if self.regularization < 0.0:
            raise ValueError("regularization mag niet negatief zijn")
        if self.residual_variance_floor < 0.0:
            raise ValueError("residual_variance_floor mag niet negatief zijn")
        _validate_covariance2(
            self.velocity_process_noise_covariance_m2_s2,
            "velocity_process_noise_covariance_m2_s2",
        )

    @property
    def fingerprint(self) -> str:
        return _sha256_json(
            {
                "algorithm": LUCAS_KANADE_FLOW_VERSION,
                "window_radius_pixels": self.window_radius_pixels,
                "minimum_valid_samples": self.minimum_valid_samples,
                "minimum_structure_eigenvalue": _float_token(
                    self.minimum_structure_eigenvalue
                ),
                "maximum_condition_number": _float_token(
                    self.maximum_condition_number
                ),
                "regularization": _float_token(self.regularization),
                "residual_variance_floor": _float_token(
                    self.residual_variance_floor
                ),
                "velocity_process_noise_covariance_m2_s2": _covariance_token(
                    self.velocity_process_noise_covariance_m2_s2
                ),
            }
        )


@dataclass(frozen=True, slots=True)
class FlowPixel:
    """One local solution.

    ``quality`` is the structure-tensor eigenvalue ratio ``lambda_min /
    lambda_max``.  It describes two-dimensional gradient geometry only; it is
    neither a calibrated confidence nor an identity probability.
    ``residual_rms`` remains in the normalized raster's physical value unit.
    """

    row: int
    column: int
    status: str
    support_samples: int
    vx_m_s: float | None
    vy_m_s: float | None
    structure_min_eigenvalue: float | None
    structure_max_eigenvalue: float | None
    condition_number: float | None
    quality: float | None
    residual_rms: float | None
    velocity_covariance_m2_s2: Covariance2D | None

    def __post_init__(self) -> None:
        if self.status not in (
            _AVAILABLE,
            _NODATA,
            _INSUFFICIENT,
            _ILL_CONDITIONED,
        ):
            raise ValueError("onbekende Lucas-Kanade pixelstatus")
        if self.row < 0 or self.column < 0 or self.support_samples < 0:
            raise ValueError("flowpixelindices/counters mogen niet negatief zijn")
        if self.status == _AVAILABLE:
            values = (
                self.vx_m_s,
                self.vy_m_s,
                self.structure_min_eigenvalue,
                self.structure_max_eigenvalue,
                self.condition_number,
                self.quality,
                self.residual_rms,
            )
            if any(value is None or not math.isfinite(value) for value in values):
                raise ValueError("available flowpixel vereist alle eindige kenmerken")
            if self.velocity_covariance_m2_s2 is None:
                raise ValueError("available flowpixel vereist covariantie")
            _validate_covariance2(
                self.velocity_covariance_m2_s2,
                "flowpixel velocity covariance",
            )
            if (
                self.structure_min_eigenvalue < 0.0
                or self.structure_max_eigenvalue
                < self.structure_min_eigenvalue
                or self.condition_number < 1.0
                or self.quality < 0.0
                or self.quality > 1.0
                or self.residual_rms < 0.0
            ):
                raise ValueError("available flowpixelkenmerken zijn inconsistent")
        elif self.vx_m_s is not None or self.vy_m_s is not None:
            raise ValueError("unavailable flowpixel mag geen velocity publiceren")
        elif (
            self.velocity_covariance_m2_s2 is not None
            or self.residual_rms is not None
        ):
            raise ValueError(
                "unavailable flowpixel mag geen residual/covariantie publiceren"
            )

    @property
    def available(self) -> bool:
        return self.status == _AVAILABLE


@dataclass(frozen=True, slots=True)
class LucasKanadeFlowField:
    previous_frame_id: str
    current_frame_id: str
    previous_valid_at: float
    current_valid_at: float
    elapsed_seconds: float
    analysis_grid_id: str
    quantity: str
    unit: str
    config_fingerprint: str
    pixels: tuple[tuple[FlowPixel, ...], ...]
    checksum: str
    algorithm_version: str = LUCAS_KANADE_FLOW_VERSION

    def pixel(self, row: int, column: int) -> FlowPixel:
        if (
            isinstance(row, bool)
            or not isinstance(row, int)
            or isinstance(column, bool)
            or not isinstance(column, int)
            or row < 0
            or row >= len(self.pixels)
            or column < 0
            or column >= len(self.pixels[0])
        ):
            raise IndexError("flowpixel ligt buiten het veld")
        return self.pixels[row][column]

    @property
    def status_counts(self) -> tuple[tuple[str, int], ...]:
        counts: dict[str, int] = {}
        for row in self.pixels:
            for pixel in row:
                counts[pixel.status] = counts.get(pixel.status, 0) + 1
        return tuple(sorted(counts.items()))


@dataclass(frozen=True, slots=True)
class DetectionFlowAggregationConfig:
    minimum_valid_pixels: int
    minimum_valid_fraction: float
    velocity_covariance_floor_m2_s2: Covariance2D
    residual_position_covariance_m2: Covariance2D
    method: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.minimum_valid_pixels, bool)
            or not isinstance(self.minimum_valid_pixels, int)
            or self.minimum_valid_pixels < 1
        ):
            raise ValueError("minimum_valid_pixels moet positief zijn")
        if (
            not math.isfinite(self.minimum_valid_fraction)
            or self.minimum_valid_fraction <= 0.0
            or self.minimum_valid_fraction > 1.0
        ):
            raise ValueError("minimum_valid_fraction moet in ]0,1] liggen")
        _validate_covariance2(
            self.velocity_covariance_floor_m2_s2,
            "velocity_covariance_floor_m2_s2",
        )
        _validate_covariance2(
            self.residual_position_covariance_m2,
            "residual_position_covariance_m2",
        )
        if self.method != "conservative_mean":
            raise ValueError("enkel conservative_mean is expliciet geïmplementeerd")

    @property
    def fingerprint(self) -> str:
        return _sha256_json(
            {
                "minimum_valid_pixels": self.minimum_valid_pixels,
                "minimum_valid_fraction": _float_token(
                    self.minimum_valid_fraction
                ),
                "velocity_covariance_floor_m2_s2": _covariance_token(
                    self.velocity_covariance_floor_m2_s2
                ),
                "residual_position_covariance_m2": _covariance_token(
                    self.residual_position_covariance_m2
                ),
                "method": self.method,
            }
        )


@dataclass(frozen=True, slots=True)
class DetectionFlowAggregation:
    status: str
    detection_id: str
    footprint_pixels: int
    valid_pixels: int
    valid_fraction: float
    pixel_status_counts: tuple[tuple[str, int], ...]
    mean_quality: float | None
    mean_residual_rms: float | None
    sample: OpticalFlowSample | None

    @property
    def available(self) -> bool:
        return self.status == "available"


def estimate_lucas_kanade_flow(
    previous: NormalizedRasterFrame,
    current: NormalizedRasterFrame,
    config: LucasKanadeConfig,
) -> LucasKanadeFlowField:
    """Estimate a metric velocity field from two compatible raster frames."""

    _validate_frame_pair(previous, current)
    if not isinstance(config, LucasKanadeConfig):
        raise TypeError("Lucas-Kanade vereist LucasKanadeConfig")
    elapsed_seconds = current.valid_at - previous.valid_at
    old = _frame_array(previous)
    new = _frame_array(current)
    finite = np.isfinite(old) & np.isfinite(new)
    average = np.where(finite, (old + new) * 0.5, np.nan)
    temporal = new - old
    height, width = old.shape
    gradient_x = np.full_like(old, np.nan, dtype=np.float64)
    gradient_y = np.full_like(old, np.nan, dtype=np.float64)
    gradient_valid = np.zeros_like(finite, dtype=bool)
    if height >= 3 and width >= 3:
        interior_valid = (
            finite[1:-1, 1:-1]
            & finite[1:-1, :-2]
            & finite[1:-1, 2:]
            & finite[:-2, 1:-1]
            & finite[2:, 1:-1]
        )
        gradient_valid[1:-1, 1:-1] = interior_valid
        gradient_x[1:-1, 1:-1] = (
            average[1:-1, 2:] - average[1:-1, :-2]
        ) * 0.5
        gradient_y[1:-1, 1:-1] = (
            average[2:, 1:-1] - average[:-2, 1:-1]
        ) * 0.5
        gradient_x[~gradient_valid] = np.nan
        gradient_y[~gradient_valid] = np.nan

    output: list[tuple[FlowPixel, ...]] = []
    radius = config.window_radius_pixels
    process_covariance = np.asarray(
        config.velocity_process_noise_covariance_m2_s2,
        dtype=np.float64,
    )
    for row in range(height):
        output_row: list[FlowPixel] = []
        for column in range(width):
            if not finite[row, column]:
                output_row.append(
                    _unavailable_pixel(row, column, _NODATA, 0)
                )
                continue
            row_start = max(0, row - radius)
            row_stop = min(height, row + radius + 1)
            column_start = max(0, column - radius)
            column_stop = min(width, column + radius + 1)
            local_valid = gradient_valid[
                row_start:row_stop,
                column_start:column_stop,
            ]
            sample_count = int(np.count_nonzero(local_valid))
            if sample_count < config.minimum_valid_samples:
                output_row.append(
                    _unavailable_pixel(
                        row,
                        column,
                        _INSUFFICIENT,
                        sample_count,
                    )
                )
                continue
            local_x = gradient_x[
                row_start:row_stop,
                column_start:column_stop,
            ][local_valid]
            local_y = gradient_y[
                row_start:row_stop,
                column_start:column_stop,
            ][local_valid]
            local_t = temporal[
                row_start:row_stop,
                column_start:column_stop,
            ][local_valid]
            design = np.column_stack((local_x, local_y))
            structure = design.T @ design
            eigenvalues = np.linalg.eigvalsh(structure)
            minimum_eigenvalue = float(eigenvalues[0])
            maximum_eigenvalue = float(eigenvalues[1])
            condition_number = (
                math.inf
                if minimum_eigenvalue <= 0.0
                else maximum_eigenvalue / minimum_eigenvalue
            )
            quality = (
                0.0
                if maximum_eigenvalue <= 0.0
                else minimum_eigenvalue / maximum_eigenvalue
            )
            if (
                minimum_eigenvalue < config.minimum_structure_eigenvalue
                or condition_number > config.maximum_condition_number
            ):
                output_row.append(
                    FlowPixel(
                        row=row,
                        column=column,
                        status=_ILL_CONDITIONED,
                        support_samples=sample_count,
                        vx_m_s=None,
                        vy_m_s=None,
                        structure_min_eigenvalue=minimum_eigenvalue,
                        structure_max_eigenvalue=maximum_eigenvalue,
                        condition_number=condition_number,
                        quality=quality,
                        residual_rms=None,
                        velocity_covariance_m2_s2=None,
                    )
                )
                continue
            normal = structure + np.eye(2) * config.regularization
            displacement_pixels = np.linalg.solve(
                normal,
                -(design.T @ local_t),
            )
            residual = design @ displacement_pixels + local_t
            residual_sum_squares = float(residual @ residual)
            residual_variance = max(
                residual_sum_squares / max(sample_count - 2, 1),
                config.residual_variance_floor,
            )
            inverse_normal = np.linalg.inv(normal)
            displacement_covariance = (
                residual_variance
                * inverse_normal
                @ structure
                @ inverse_normal.T
            )
            metric_transform = np.asarray(
                (
                    (
                        previous.grid.x_resolution_m / elapsed_seconds,
                        0.0,
                    ),
                    (
                        0.0,
                        -previous.grid.y_resolution_m / elapsed_seconds,
                    ),
                ),
                dtype=np.float64,
            )
            velocity_covariance = (
                metric_transform
                @ displacement_covariance
                @ metric_transform.T
                + process_covariance
            )
            vx_m_s = (
                float(displacement_pixels[0])
                * previous.grid.x_resolution_m
                / elapsed_seconds
            )
            vy_m_s = (
                -float(displacement_pixels[1])
                * previous.grid.y_resolution_m
                / elapsed_seconds
            )
            output_row.append(
                FlowPixel(
                    row=row,
                    column=column,
                    status=_AVAILABLE,
                    support_samples=sample_count,
                    vx_m_s=vx_m_s,
                    vy_m_s=vy_m_s,
                    structure_min_eigenvalue=minimum_eigenvalue,
                    structure_max_eigenvalue=maximum_eigenvalue,
                    condition_number=condition_number,
                    quality=quality,
                    residual_rms=math.sqrt(
                        residual_sum_squares / sample_count
                    ),
                    velocity_covariance_m2_s2=_array_covariance(
                        velocity_covariance
                    ),
                )
            )
        output.append(tuple(output_row))
    pixels = tuple(output)
    checksum = _flow_checksum(
        previous,
        current,
        config,
        pixels,
    )
    return LucasKanadeFlowField(
        previous_frame_id=previous.frame_id,
        current_frame_id=current.frame_id,
        previous_valid_at=previous.valid_at,
        current_valid_at=current.valid_at,
        elapsed_seconds=elapsed_seconds,
        analysis_grid_id=previous.grid.grid_id,
        quantity=previous.quantity,
        unit=previous.unit,
        config_fingerprint=config.fingerprint,
        pixels=pixels,
        checksum=checksum,
    )


def aggregate_detection_flow_sample(
    field: LucasKanadeFlowField,
    detection: HierarchicalCellDetection,
    config: DetectionFlowAggregationConfig,
) -> DetectionFlowAggregation:
    """Aggregate valid footprint vectors without rejecting the detection."""

    if not isinstance(field, LucasKanadeFlowField):
        raise TypeError("flowaggregatie vereist LucasKanadeFlowField")
    if not isinstance(detection, HierarchicalCellDetection):
        raise TypeError("flowaggregatie vereist HierarchicalCellDetection")
    if not isinstance(config, DetectionFlowAggregationConfig):
        raise TypeError("flowaggregatie vereist DetectionFlowAggregationConfig")
    if detection.detection.analysis_grid_id != field.analysis_grid_id:
        raise ValueError("detectie en flowveld gebruiken een ander analysis grid")
    if not math.isclose(
        detection.detection.valid_at,
        field.previous_valid_at,
        rel_tol=0.0,
        abs_tol=1e-6,
    ):
        raise ValueError("detectie hoort niet bij het vorige flowframe")

    footprint_pixels: list[FlowPixel] = []
    for row, column in detection.footprint_pixels:
        footprint_pixels.append(field.pixel(row, column))
    valid = tuple(item for item in footprint_pixels if item.available)
    valid_fraction = len(valid) / len(footprint_pixels)
    status_counts: dict[str, int] = {}
    for item in footprint_pixels:
        status_counts[item.status] = status_counts.get(item.status, 0) + 1
    common = {
        "detection_id": detection.detection.detection_id,
        "footprint_pixels": len(footprint_pixels),
        "valid_pixels": len(valid),
        "valid_fraction": valid_fraction,
        "pixel_status_counts": tuple(sorted(status_counts.items())),
    }
    if not valid:
        return DetectionFlowAggregation(
            status="unavailable_no_valid_flow",
            mean_quality=None,
            mean_residual_rms=None,
            sample=None,
            **common,
        )
    mean_quality = sum(float(item.quality) for item in valid) / len(valid)
    mean_residual = sum(float(item.residual_rms) for item in valid) / len(valid)
    if (
        len(valid) < config.minimum_valid_pixels
        or valid_fraction < config.minimum_valid_fraction
    ):
        return DetectionFlowAggregation(
            status="unavailable_insufficient_coverage",
            mean_quality=mean_quality,
            mean_residual_rms=mean_residual,
            sample=None,
            **common,
        )

    vectors = np.asarray(
        [(float(item.vx_m_s), float(item.vy_m_s)) for item in valid],
        dtype=np.float64,
    )
    mean_vector = np.mean(vectors, axis=0)
    mean_local_covariance = np.mean(
        np.asarray(
            [item.velocity_covariance_m2_s2 for item in valid],
            dtype=np.float64,
        ),
        axis=0,
    )
    if len(valid) > 1:
        empirical_spread = np.cov(vectors, rowvar=False, ddof=1)
    else:
        empirical_spread = np.zeros((2, 2), dtype=np.float64)
    aggregate_covariance = (
        mean_local_covariance
        + empirical_spread
        + np.asarray(
            config.velocity_covariance_floor_m2_s2,
            dtype=np.float64,
        )
    )
    sample_id = "lk:" + hashlib.sha256(
        (
            f"{field.checksum}\0{detection.detection.detection_id}\0"
            f"{config.fingerprint}"
        ).encode("utf-8")
    ).hexdigest()
    sample = OpticalFlowSample(
        sample_id=sample_id,
        analysis_grid_id=field.analysis_grid_id,
        reference_valid_at=field.previous_valid_at,
        vx_m_s=float(mean_vector[0]),
        vy_m_s=float(mean_vector[1]),
        velocity_covariance_m2_s2=_array_covariance(
            aggregate_covariance
        ),
        residual_covariance_m2=config.residual_position_covariance_m2,
        method="lucas_kanade_conservative_mean_v1",
        support_frame_ids=(
            field.previous_frame_id,
            field.current_frame_id,
        ),
    )
    return DetectionFlowAggregation(
        status="available",
        mean_quality=mean_quality,
        mean_residual_rms=mean_residual,
        sample=sample,
        **common,
    )


class LucasKanadeFlowCache:
    """Small deterministic in-memory cache with immutable-ID collision guard."""

    def __init__(self, maximum_entries: int) -> None:
        if (
            isinstance(maximum_entries, bool)
            or not isinstance(maximum_entries, int)
            or maximum_entries < 1
        ):
            raise ValueError("maximum_entries moet positief zijn")
        self._maximum_entries = maximum_entries
        self._values: OrderedDict[
            tuple[str, str, str, str],
            tuple[str, LucasKanadeFlowField],
        ] = OrderedDict()
        self.hits = 0
        self.misses = 0

    def estimate(
        self,
        previous: NormalizedRasterFrame,
        current: NormalizedRasterFrame,
        config: LucasKanadeConfig,
    ) -> LucasKanadeFlowField:
        key = (
            previous.frame_id,
            current.frame_id,
            LUCAS_KANADE_FLOW_VERSION,
            config.fingerprint,
        )
        input_checksum = _input_checksum(previous, current)
        cached = self._values.get(key)
        if cached is not None:
            cached_input_checksum, field = cached
            if cached_input_checksum != input_checksum:
                raise ValueError(
                    "frame-ID/config-cachekey verwijst naar gewijzigde rasterbytes"
                )
            self._values.move_to_end(key)
            self.hits += 1
            return field
        field = estimate_lucas_kanade_flow(previous, current, config)
        self._values[key] = (input_checksum, field)
        self._values.move_to_end(key)
        while len(self._values) > self._maximum_entries:
            self._values.popitem(last=False)
        self.misses += 1
        return field

    @property
    def size(self) -> int:
        return len(self._values)


def _validate_frame_pair(
    previous: NormalizedRasterFrame,
    current: NormalizedRasterFrame,
) -> None:
    if not isinstance(previous, NormalizedRasterFrame) or not isinstance(
        current,
        NormalizedRasterFrame,
    ):
        raise TypeError("Lucas-Kanade vereist twee NormalizedRasterFrame-objecten")
    if current.valid_at <= previous.valid_at:
        raise ValueError("huidig rasterframe moet meteorologisch later zijn")
    previous.grid.assert_same_geometry(current.grid)
    if previous.quantity != current.quantity or previous.unit != current.unit:
        raise ValueError("rasterframes moeten dezelfde quantity en unit hebben")
    if previous.frame_id == current.frame_id:
        raise ValueError("opeenvolgende rasterframes moeten unieke frame-ID's hebben")


def _frame_array(frame: NormalizedRasterFrame) -> np.ndarray:
    return np.asarray(
        tuple(
            tuple(np.nan if value is None else float(value) for value in row)
            for row in frame.values
        ),
        dtype=np.float64,
    )


def _unavailable_pixel(
    row: int,
    column: int,
    status: str,
    support_samples: int,
) -> FlowPixel:
    return FlowPixel(
        row=row,
        column=column,
        status=status,
        support_samples=support_samples,
        vx_m_s=None,
        vy_m_s=None,
        structure_min_eigenvalue=None,
        structure_max_eigenvalue=None,
        condition_number=None,
        quality=None,
        residual_rms=None,
        velocity_covariance_m2_s2=None,
    )


def _array_covariance(value: np.ndarray) -> Covariance2D:
    # Sums/means of individually symmetric matrices can acquire a tiny
    # off-diagonal mismatch from floating-point reduction order.  Canonicalise
    # that numerical noise here, before the stricter public covariance contract
    # is constructed.  Material asymmetry still fails the PSD validation below.
    raw: Covariance2D = (
        (float(value[0, 0]), float(value[0, 1])),
        (float(value[1, 0]), float(value[1, 1])),
    )
    _validate_covariance2(raw, "berekende velocity covariance")
    symmetric = (value + value.T) * 0.5
    result: Covariance2D = (
        (float(symmetric[0, 0]), float(symmetric[0, 1])),
        (float(symmetric[1, 0]), float(symmetric[1, 1])),
    )
    _validate_covariance2(result, "berekende velocity covariance")
    return result


def _validate_covariance2(value: Covariance2D, label: str) -> None:
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or any(not isinstance(row, tuple) or len(row) != 2 for row in value)
    ):
        raise ValueError(f"{label} moet een immutable 2x2 tuple zijn")
    xx, xy = value[0]
    yx, yy = value[1]
    if not all(math.isfinite(item) for item in (xx, xy, yx, yy)):
        raise ValueError(f"{label} moet eindig zijn")
    if not math.isclose(xy, yx, rel_tol=1e-10, abs_tol=1e-10):
        raise ValueError(f"{label} moet symmetrisch zijn")
    determinant = xx * yy - xy * yx
    tolerance = 1e-10 * max(1.0, abs(xx * yy), abs(xy * yx))
    if xx < 0.0 or yy < 0.0 or determinant < -tolerance:
        raise ValueError(f"{label} moet positief semidefiniet zijn")


def _flow_checksum(
    previous: NormalizedRasterFrame,
    current: NormalizedRasterFrame,
    config: LucasKanadeConfig,
    pixels: tuple[tuple[FlowPixel, ...], ...],
) -> str:
    return _sha256_json(
        {
            "algorithm": LUCAS_KANADE_FLOW_VERSION,
            "input_checksum": _input_checksum(previous, current),
            "config": config.fingerprint,
            "pixels": [
                [
                    {
                        "row": pixel.row,
                        "column": pixel.column,
                        "status": pixel.status,
                        "support": pixel.support_samples,
                        "vx": _optional_float_token(pixel.vx_m_s),
                        "vy": _optional_float_token(pixel.vy_m_s),
                        "min_eigen": _optional_float_token(
                            pixel.structure_min_eigenvalue
                        ),
                        "max_eigen": _optional_float_token(
                            pixel.structure_max_eigenvalue
                        ),
                        "condition": _optional_float_token(
                            pixel.condition_number
                        ),
                        "quality": _optional_float_token(pixel.quality),
                        "residual": _optional_float_token(pixel.residual_rms),
                        "covariance": (
                            None
                            if pixel.velocity_covariance_m2_s2 is None
                            else _covariance_token(
                                pixel.velocity_covariance_m2_s2
                            )
                        ),
                    }
                    for pixel in row
                ]
                for row in pixels
            ],
        }
    )


def _input_checksum(
    previous: NormalizedRasterFrame,
    current: NormalizedRasterFrame,
) -> str:
    return _sha256_json(
        {
            "previous": _frame_token(previous),
            "current": _frame_token(current),
        }
    )


def _frame_token(frame: NormalizedRasterFrame) -> dict[str, object]:
    return {
        "frame_id": frame.frame_id,
        "valid_at": _float_token(frame.valid_at),
        "source": frame.source,
        "quantity": frame.quantity,
        "unit": frame.unit,
        "grid": {
            "grid_id": frame.grid.grid_id,
            "crs": frame.grid.crs,
            "origin_x_m": _float_token(frame.grid.origin_x_m),
            "origin_y_m": _float_token(frame.grid.origin_y_m),
            "x_resolution_m": _float_token(frame.grid.x_resolution_m),
            "y_resolution_m": _float_token(frame.grid.y_resolution_m),
            "width": frame.grid.width,
            "height": frame.grid.height,
        },
        "values": [
            [None if value is None else _float_token(value) for value in row]
            for row in frame.values
        ],
    }


def _covariance_token(value: Covariance2D) -> list[list[str]]:
    return [[_float_token(item) for item in row] for row in value]


def _optional_float_token(value: float | None) -> str | None:
    return None if value is None else _float_token(value)


def _float_token(value: float) -> str:
    return float(value).hex()


def _sha256_json(value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
