"""Lichte, adaptieve trajectschatting voor convectieve systemen.

De operationele integratie draait in Home Assistant en mag daarom geen zware
nowcasting-stack in het HA-proces laden. Dit model vergelijkt een constante
snelheid met een constante versnelling en kiest het complexere model alleen
wanneer een out-of-sample controle werkelijk beter is.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Protocol


class PositionPoint(Protocol):
    lat: float
    lon: float
    ts: float


@dataclass(frozen=True)
class TrajectoryEstimate:
    model: str
    velocity_east_kmh: float
    velocity_north_kmh: float
    acceleration_east_kmh2: float
    acceleration_north_kmh2: float
    fit_quality: float
    prediction_error_km: float
    linear_prediction_error_km: float
    model_gain: float

    @property
    def speed_kmh(self) -> float:
        return math.hypot(self.velocity_east_kmh, self.velocity_north_kmh)

    @property
    def heading_deg(self) -> float | None:
        if self.speed_kmh < 0.5:
            return None
        return math.degrees(
            math.atan2(self.velocity_east_kmh, self.velocity_north_kmh)
        ) % 360.0

    @property
    def acceleration_kmh2(self) -> float:
        return math.hypot(
            self.acceleration_east_kmh2,
            self.acceleration_north_kmh2,
        )

    def displacement_km(self, minutes: float) -> tuple[float, float]:
        """Geef oost/noord-verplaatsing vanaf de huidige positie."""
        hours = max(0.0, float(minutes)) / 60.0
        return (
            self.velocity_east_kmh * hours
            + 0.5 * self.acceleration_east_kmh2 * hours * hours,
            self.velocity_north_kmh * hours
            + 0.5 * self.acceleration_north_kmh2 * hours * hours,
        )

    def speed_at(self, minutes: float) -> float:
        hours = max(0.0, float(minutes)) / 60.0
        return math.hypot(
            self.velocity_east_kmh + self.acceleration_east_kmh2 * hours,
            self.velocity_north_kmh + self.acceleration_north_kmh2 * hours,
        )


def fit_adaptive_trajectory(
    history: Iterable[PositionPoint],
) -> TrajectoryEstimate | None:
    """Pas lineair en constant-versneld model en kies via hindcastfout.

    Een parabool krijgt alleen voorrang wanneer voldoende historie beschikbaar
    is, de afgeleide snelheden meteorologisch plausibel blijven en de laatste
    waarnemingen aantoonbaar beter worden voorspeld dan door een rechte baan.
    """
    points = sorted(history, key=lambda item: item.ts)
    if len(points) < 2:
        return None
    points = _deduplicate_timestamps(points)
    if len(points) < 2:
        return None

    origin = points[-1]
    cos_lat = max(0.05, math.cos(math.radians(origin.lat)))
    samples = [
        (
            (point.ts - origin.ts) / 3600.0,
            (point.lon - origin.lon) * 111.32 * cos_lat,
            (point.lat - origin.lat) * 110.574,
        )
        for point in points
    ]

    linear = _fit_model(samples, degree=1)
    if linear is None:
        return None
    linear_error = _rolling_prediction_error(samples, degree=1)
    selected = linear
    selected_degree = 1
    selected_error = linear_error
    model_gain = 0.0

    span_minutes = (points[-1].ts - points[0].ts) / 60.0
    if len(points) >= 6 and span_minutes >= 10.0:
        accelerated = _fit_model(samples, degree=2)
        accelerated_error = _rolling_prediction_error(samples, degree=2)
        if accelerated is not None:
            acceleration = math.hypot(
                2.0 * accelerated[0][2],
                2.0 * accelerated[1][2],
            )
            future_speed = math.hypot(
                accelerated[0][1] + 2.0 * accelerated[0][2] * 1.5,
                accelerated[1][1] + 2.0 * accelerated[1][2] * 1.5,
            )
            improvement = (
                (linear_error - accelerated_error) / max(linear_error, 0.1)
            )
            fit_improvement = accelerated[2] - linear[2]
            plausible = acceleration <= 160.0 and future_speed <= 180.0
            demonstrably_better = (
                accelerated_error + 0.25 < linear_error * 0.85
                or (
                    accelerated_error <= linear_error
                    and fit_improvement >= 0.12
                )
            )
            if plausible and demonstrably_better:
                selected = accelerated
                selected_degree = 2
                selected_error = accelerated_error
                model_gain = max(0.0, improvement)

    east, north, quality = selected
    acceleration_east = 2.0 * east[2] if selected_degree == 2 else 0.0
    acceleration_north = 2.0 * north[2] if selected_degree == 2 else 0.0
    return TrajectoryEstimate(
        model="constant_acceleration" if selected_degree == 2 else "linear",
        velocity_east_kmh=east[1],
        velocity_north_kmh=north[1],
        acceleration_east_kmh2=acceleration_east,
        acceleration_north_kmh2=acceleration_north,
        fit_quality=quality,
        prediction_error_km=selected_error,
        linear_prediction_error_km=linear_error,
        model_gain=model_gain,
    )


def _deduplicate_timestamps(points: list[PositionPoint]) -> list[PositionPoint]:
    result: list[PositionPoint] = []
    for point in points:
        if result and abs(result[-1].ts - point.ts) < 1.0:
            result[-1] = point
        else:
            result.append(point)
    return result


def _fit_model(
    samples: list[tuple[float, float, float]], degree: int
) -> tuple[list[float], list[float], float] | None:
    east = _polynomial_fit(
        [sample[0] for sample in samples],
        [sample[1] for sample in samples],
        degree,
    )
    north = _polynomial_fit(
        [sample[0] for sample in samples],
        [sample[2] for sample in samples],
        degree,
    )
    if east is None or north is None:
        return None
    quality = (
        _r_squared(samples, east, axis=1)
        + _r_squared(samples, north, axis=2)
    ) / 2.0
    return east, north, round(max(0.0, min(1.0, quality)), 3)


def _rolling_prediction_error(
    samples: list[tuple[float, float, float]], degree: int
) -> float:
    """Hindcast de laatste punten met alleen de toen beschikbare historie."""
    minimum = degree + 2
    errors: list[float] = []
    start = max(minimum, len(samples) - 3)
    for index in range(start, len(samples)):
        training = samples[:index]
        fitted = _fit_model(training, degree)
        if fitted is None:
            continue
        east, north, _ = fitted
        timestamp, observed_east, observed_north = samples[index]
        predicted_east = _evaluate(east, timestamp)
        predicted_north = _evaluate(north, timestamp)
        errors.append(
            math.hypot(
                observed_east - predicted_east,
                observed_north - predicted_north,
            )
        )
    if errors:
        return round(math.sqrt(sum(value * value for value in errors) / len(errors)), 3)

    fitted = _fit_model(samples, degree)
    if fitted is None:
        return math.inf
    east, north, _ = fitted
    residuals = [
        math.hypot(
            sample[1] - _evaluate(east, sample[0]),
            sample[2] - _evaluate(north, sample[0]),
        )
        for sample in samples
    ]
    return round(
        math.sqrt(sum(value * value for value in residuals) / len(residuals)),
        3,
    )


def _polynomial_fit(
    times: list[float], values: list[float], degree: int
) -> list[float] | None:
    size = degree + 1
    matrix = [
        [sum(timestamp ** (row + column) for timestamp in times)
         for column in range(size)]
        for row in range(size)
    ]
    vector = [
        sum(value * timestamp ** row for timestamp, value in zip(times, values))
        for row in range(size)
    ]
    return _solve_linear_system(matrix, vector)


def _solve_linear_system(
    matrix: list[list[float]], vector: list[float]
) -> list[float] | None:
    size = len(vector)
    augmented = [row[:] + [vector[index]] for index, row in enumerate(matrix)]
    for column in range(size):
        pivot = max(range(column, size), key=lambda row: abs(augmented[row][column]))
        if abs(augmented[pivot][column]) < 1e-12:
            return None
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        divisor = augmented[column][column]
        augmented[column] = [value / divisor for value in augmented[column]]
        for row in range(size):
            if row == column:
                continue
            factor = augmented[row][column]
            augmented[row] = [
                value - factor * pivot_value
                for value, pivot_value in zip(augmented[row], augmented[column])
            ]
    return [augmented[row][-1] for row in range(size)]


def _evaluate(coefficients: list[float], timestamp: float) -> float:
    return sum(
        coefficient * timestamp ** power
        for power, coefficient in enumerate(coefficients)
    )


def _r_squared(
    samples: list[tuple[float, float, float]],
    coefficients: list[float],
    *,
    axis: int,
) -> float:
    values = [sample[axis] for sample in samples]
    average = sum(values) / len(values)
    residual = sum(
        (sample[axis] - _evaluate(coefficients, sample[0])) ** 2
        for sample in samples
    )
    total = sum((value - average) ** 2 for value in values)
    if total <= 1e-12:
        return 1.0 if residual <= 1e-12 else 0.0
    return max(0.0, 1.0 - residual / total)
