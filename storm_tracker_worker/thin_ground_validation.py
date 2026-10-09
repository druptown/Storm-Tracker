"""Pure comparison of predicted cell passages with ground rain evidence."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Iterable

from .thin_footprint_forecast import TargetPassage
from .thin_observation import NormalizedObservation, ObservationKind


@dataclass(frozen=True, slots=True)
class ForecastValidation:
    validation_key: str
    target_id: str
    evaluated_at: float
    cell_id: str | None
    provider_id: str
    station_provider_id: str
    station_id: str
    outcome: str
    entry_error_seconds: float | None
    exit_error_seconds: float | None
    predicted_intensity_mm_h: float | None
    observed_intensity_mm_h: float | None
    intensity_error_mm_h: float | None
    contributing_stations: tuple[str, ...]


def _station_key(observation: NormalizedObservation) -> str | None:
    station_id = observation.metadata.get("station_id")
    if not station_id:
        return None
    return f"{observation.provider_id}:{str(station_id).casefold()}"


def _key(payload: object) -> str:
    serialized = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _outcome(
    *,
    passage: TargetPassage | None,
    target_id: str,
    evaluated_at: float,
    outcome: str,
    observations: tuple[NormalizedObservation, ...],
    entry_error_seconds: float | None,
    exit_error_seconds: float | None,
) -> ForecastValidation:
    stations = tuple(sorted({
        station_key
        for observation in observations
        if (station_key := _station_key(observation)) is not None
    }))
    primary = stations[0]
    station_provider_id, station_id = primary.split(":", 1)
    observed_intensity = max(
        (observation.value for observation in observations),
        default=None,
    )
    predicted_intensity = (
        None if passage is None else passage.predicted_intensity_mm_h
    )
    intensity_error = (
        None
        if observed_intensity is None or predicted_intensity is None
        else observed_intensity - predicted_intensity
    )
    identity = {
        "target_id": target_id,
        "forecast_evaluated_at": (
            None if passage is None else passage.evaluated_at
        ),
        "cell_id": None if passage is None else passage.cell_id,
        "outcome": outcome,
        "stations": stations,
        "ground_observed_at": sorted({
            item.observed_at for item in observations
        }),
    }
    return ForecastValidation(
        validation_key=_key(identity),
        target_id=target_id,
        evaluated_at=evaluated_at,
        cell_id=None if passage is None else passage.cell_id,
        provider_id=(
            "ground_validation" if passage is None else passage.provider_id
        ),
        station_provider_id=station_provider_id,
        station_id=station_id,
        outcome=outcome,
        entry_error_seconds=entry_error_seconds,
        exit_error_seconds=exit_error_seconds,
        predicted_intensity_mm_h=predicted_intensity,
        observed_intensity_mm_h=observed_intensity,
        intensity_error_mm_h=intensity_error,
        contributing_stations=stations,
    )


def _known_exit(passage: TargetPassage) -> float | None:
    """A model/coverage boundary is not a predicted dry transition."""

    if passage.horizon_clipped or passage.right_censored or passage.coverage_censored:
        return None
    return passage.exit_at


def validate_passages(
    passages: Iterable[TargetPassage],
    observations: Iterable[NormalizedObservation],
    *,
    evaluated_at: float,
    tolerance_seconds: float = 300.0,
    maximum_ground_age_seconds: float = 2 * 60 * 60.0,
) -> tuple[ForecastValidation, ...]:
    """Return terminal evidence only; forecasts remain observer-independent."""

    evaluated = float(evaluated_at)
    tolerance = max(0.0, float(tolerance_seconds))
    maximum_age = max(0.0, float(maximum_ground_age_seconds))
    unique: dict[tuple[str, str, float], NormalizedObservation] = {}
    for observation in observations:
        station_key = _station_key(observation)
        if (
            station_key is None
            or observation.target_id is None
            or observation.kind is not ObservationKind.RAIN_RATE
            or evaluated - observation.observed_at > maximum_age
            or observation.observed_at > evaluated + 30.0
        ):
            continue
        identity = (
            observation.target_id,
            station_key,
            observation.observed_at,
        )
        previous = unique.get(identity)
        if previous is None or observation.received_at > previous.received_at:
            unique[identity] = observation
    ground = tuple(sorted(
        unique.values(),
        key=lambda item: (
            item.target_id or "", item.observed_at,
            _station_key(item) or "",
        ),
    ))
    ordered_passages = tuple(sorted(
        passages,
        key=lambda item: (item.target_id, item.entry_at, item.cell_id),
    ))
    used_wet: set[tuple[str, str, float]] = set()
    outcomes: list[ForecastValidation] = []
    for passage in ordered_passages:
        known_exit = _known_exit(passage)
        relevant = tuple(
            item for item in ground
            if item.target_id == passage.target_id
            and passage.entry_at - maximum_age
            <= item.observed_at
            and (known_exit is None
                 or item.observed_at <= known_exit + maximum_age)
        )
        wet = tuple(item for item in relevant if item.value > 0.0)
        if wet:
            first_wet = min(wet, key=lambda item: item.observed_at)
            entry_error = (
                None if passage.left_censored
                else first_wet.observed_at - passage.entry_at
            )
            if (
                not passage.left_censored
                and first_wet.observed_at < passage.entry_at - tolerance
            ):
                label = "early"
            elif known_exit is not None and first_wet.observed_at > known_exit + tolerance:
                label = "late"
            else:
                label = "hit"
            for item in wet:
                used_wet.add((
                    item.target_id or "",
                    _station_key(item) or "",
                    item.observed_at,
                ))
            dry_after = [
                item for item in relevant
                if item.value == 0.0 and item.observed_at > first_wet.observed_at
            ]
            exit_error = (
                None
                if not dry_after or known_exit is None
                else min(item.observed_at for item in dry_after) - known_exit
            )
            outcomes.append(_outcome(
                passage=passage,
                target_id=passage.target_id,
                evaluated_at=evaluated,
                outcome=label,
                observations=wet,
                entry_error_seconds=entry_error,
                exit_error_seconds=exit_error,
            ))
            continue
        dry = tuple(item for item in relevant if item.value == 0.0)
        if known_exit is not None and evaluated > known_exit + tolerance and dry:
            outcomes.append(_outcome(
                passage=passage,
                target_id=passage.target_id,
                evaluated_at=evaluated,
                outcome="false_alarm",
                observations=dry,
                entry_error_seconds=None,
                exit_error_seconds=None,
            ))

    for observation in ground:
        if observation.value <= 0.0:
            continue
        identity = (
            observation.target_id or "",
            _station_key(observation) or "",
            observation.observed_at,
        )
        if identity in used_wet:
            continue
        outcomes.append(_outcome(
            passage=None,
            target_id=observation.target_id or "unknown",
            evaluated_at=evaluated,
            outcome="miss",
            observations=(observation,),
            entry_error_seconds=None,
            exit_error_seconds=None,
        ))
    return tuple(sorted(
        {item.validation_key: item for item in outcomes}.values(),
        key=lambda item: item.validation_key,
    ))


class GroundValidationRuntime:
    """Small bounded memory for passages that finish in later cycles."""

    def __init__(self) -> None:
        self._passages: dict[tuple[str, float, str], TargetPassage] = {}
        self._observations: dict[tuple[str, str, float], NormalizedObservation] = {}
        self._emitted: set[str] = set()

    def advance(
        self,
        passages: Iterable[TargetPassage],
        observations: Iterable[NormalizedObservation],
        *,
        evaluated_at: float,
    ) -> tuple[ForecastValidation, ...]:
        evaluated = float(evaluated_at)
        for passage in passages:
            self._passages[(
                passage.target_id, passage.evaluated_at, passage.cell_id,
            )] = passage
        for observation in observations:
            station_key = _station_key(observation)
            if station_key and observation.target_id:
                self._observations[(
                    observation.target_id,
                    station_key,
                    observation.observed_at,
                )] = observation
        self._passages = {
            key: passage for key, passage in self._passages.items()
            # An unknown end is not a dry transition. Keep a bounded history
            # relative to its known entry/issue, without inventing an exit.
            if evaluated - (
                known_exit if (known_exit := _known_exit(passage)) is not None
                else max(passage.entry_at, passage.evaluated_at)
            ) <= 2 * 60 * 60.0
        }
        self._observations = {
            key: observation for key, observation in self._observations.items()
            if evaluated - observation.observed_at <= 2 * 60 * 60.0
        }
        candidates = validate_passages(
            self._passages.values(),
            self._observations.values(),
            evaluated_at=evaluated,
        )
        new = tuple(
            item for item in candidates if item.validation_key not in self._emitted
        )
        self._emitted.update(item.validation_key for item in new)
        if len(self._emitted) > 20_000:
            self._emitted = {item.validation_key for item in candidates}
        return new


__all__ = [
    "ForecastValidation",
    "GroundValidationRuntime",
    "validate_passages",
]
