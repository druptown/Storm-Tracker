"""Offline, leakage-safe verification for immutable passage forecasts."""

from __future__ import annotations

from dataclasses import dataclass
import math
from types import MappingProxyType
from typing import Any, Iterable, Mapping


@dataclass(frozen=True, slots=True)
class RadarForecastIssue:
    forecast_issue_id: str
    issued_at: float
    input_fingerprints: tuple[str, ...]
    input_fields: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class TemporalRadarReference:
    forecast_issue_id: str
    input_fields: tuple[Any, ...]
    later_fields: tuple[Any, ...]
    provenance: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True, slots=True)
class PassageForecastEvidence:
    event_id: str
    passage_id: str
    status: str
    entry_bracket: tuple[float, float] | None
    exit_bracket: tuple[float, float] | None
    left_censored: bool = False
    right_censored: bool = False
    coverage_censored: bool = False


@dataclass(frozen=True, slots=True)
class PassageReferenceEvidence:
    event_id: str
    occurred: bool
    entry_bracket: tuple[float, float] | None = None
    exit_bracket: tuple[float, float] | None = None


@dataclass(frozen=True, slots=True)
class PassageEventScore:
    event_id: str
    forecast_count: int
    observed_count: int
    hit_count: int
    false_alarm_count: int
    miss_count: int
    censored_forecast_count: int
    entry_intersects: bool | None
    entry_contains_reference: bool | None
    duration_error_seconds: float | None


@dataclass(frozen=True, slots=True)
class PassageVerificationReport:
    events: tuple[PassageEventScore, ...]
    event_count: int
    hit_count: int
    false_alarm_count: int
    miss_count: int
    censored_forecast_count: int
    duration_error_mean_seconds: float | None


def _identity(field: Any) -> Any:
    identity = getattr(field, "identity", None)
    if identity is None:
        raise ValueError("radar_field_identity_missing")
    return identity


def _complete(field: Any) -> bool:
    mask = getattr(field, "valid_mask", None)
    return bool(mask is not None and bool(mask.all()))


def build_temporal_radar_reference(
    issue: RadarForecastIssue,
    later_fields: Iterable[Any],
) -> TemporalRadarReference:
    """Bind exact issue inputs and strictly later complete radar revisions."""

    if not math.isfinite(float(issue.issued_at)):
        raise ValueError("forecast_issue_time_invalid")
    required = tuple(issue.input_fingerprints)
    if not required or len(set(required)) != len(required):
        raise ValueError("forecast_input_fingerprints_invalid")
    indexed: dict[str, list[Any]] = {}
    for field in issue.input_fields:
        indexed.setdefault(
            str(_identity(field).content_fingerprint), []
        ).append(field)
    selected: list[Any] = []
    for fingerprint in required:
        matches = indexed.get(fingerprint, [])
        if len(matches) != 1:
            raise ValueError("forecast_input_revision_not_exact")
        field = matches[0]
        identity = _identity(field)
        if (
            float(identity.received_at) > issue.issued_at
            or float(identity.available_at) > issue.issued_at
        ):
            raise ValueError("issue_time_input_leakage")
        selected.append(field)
    selected.sort(key=lambda item: (
        float(_identity(item).canonical_valid_at),
        str(_identity(item).content_fingerprint),
    ))
    anchor_valid_at = max(
        float(_identity(item).canonical_valid_at) for item in selected
    )
    compatibility = {
        (
            str(_identity(item).grid_id),
            str(_identity(item).quantity),
            str(_identity(item).unit),
        )
        for item in selected
    }
    by_valid_at: dict[float, Any] = {}
    for field in later_fields:
        identity = _identity(field)
        valid_at = float(identity.canonical_valid_at)
        key = (
            str(identity.grid_id), str(identity.quantity), str(identity.unit),
        )
        if valid_at <= anchor_valid_at or key not in compatibility or not _complete(field):
            continue
        previous = by_valid_at.get(valid_at)
        if previous is None or (
            float(identity.available_at),
            float(identity.received_at),
            str(identity.content_fingerprint),
        ) < (
            float(_identity(previous).available_at),
            float(_identity(previous).received_at),
            str(_identity(previous).content_fingerprint),
        ):
            by_valid_at[valid_at] = field
    later = tuple(by_valid_at[key] for key in sorted(by_valid_at))
    identities = tuple(_identity(item) for item in (*selected, *later))
    return TemporalRadarReference(
        forecast_issue_id=issue.forecast_issue_id,
        input_fields=tuple(selected),
        later_fields=later,
        provenance=MappingProxyType({
            "provider_ids": tuple(sorted({
                str(getattr(item, "provider_id", "unknown"))
                for item in (*selected, *later)
            })),
            "grid_ids": tuple(sorted({str(item.grid_id) for item in identities})),
            "quantities": tuple(sorted({
                f"{item.quantity}:{item.unit}" for item in identities
            })),
            "fingerprints": tuple(
                str(item.content_fingerprint) for item in identities
            ),
        }),
    )


def _bracket(value: tuple[float, float] | None) -> tuple[float, float] | None:
    if value is None:
        return None
    lower, upper = map(float, value)
    if not all(map(math.isfinite, (lower, upper))) or upper < lower:
        raise ValueError("passage_bracket_invalid")
    return lower, upper


def _midpoint(value: tuple[float, float]) -> float:
    return (value[0] + value[1]) / 2.0


def verify_passages_by_event(
    forecasts: Iterable[PassageForecastEvidence],
    references: Iterable[PassageReferenceEvidence],
) -> PassageVerificationReport:
    """Score passages by meteorological event, never by repeated frame."""

    active = {"candidate", "confirmed", "coasting", "current", "ended"}
    forecast_by_event: dict[str, list[PassageForecastEvidence]] = {}
    reference_by_event: dict[str, list[PassageReferenceEvidence]] = {}
    for item in forecasts:
        _bracket(item.entry_bracket)
        _bracket(item.exit_bracket)
        if item.status in active:
            forecast_by_event.setdefault(item.event_id, []).append(item)
    for item in references:
        _bracket(item.entry_bracket)
        _bracket(item.exit_bracket)
        reference_by_event.setdefault(item.event_id, []).append(item)
    event_ids = sorted(set(forecast_by_event) | set(reference_by_event))
    scores: list[PassageEventScore] = []
    duration_errors: list[float] = []
    for event_id in event_ids:
        predicted = sorted(
            forecast_by_event.get(event_id, []),
            key=lambda item: (
                math.inf if item.entry_bracket is None else item.entry_bracket[0],
                item.passage_id,
            ),
        )
        observed = sorted(
            (
                item for item in reference_by_event.get(event_id, [])
                if item.occurred
            ),
            key=lambda item: (
                math.inf if item.entry_bracket is None else item.entry_bracket[0]
            ),
        )
        hit_count = min(len(predicted), len(observed))
        false_count = max(0, len(predicted) - len(observed))
        miss_count = max(0, len(observed) - len(predicted))
        entry_intersections: list[bool] = []
        entry_contains: list[bool] = []
        paired_duration_errors: list[float] = []
        for forecast, reference in zip(predicted, observed):
            forecast_entry = _bracket(forecast.entry_bracket)
            reference_entry = _bracket(reference.entry_bracket)
            if forecast_entry is not None and reference_entry is not None:
                entry_intersections.append(
                    max(forecast_entry[0], reference_entry[0])
                    <= min(forecast_entry[1], reference_entry[1])
                )
                entry_contains.append(
                    forecast_entry[0] <= reference_entry[0]
                    and forecast_entry[1] >= reference_entry[1]
                )
            forecast_exit = _bracket(forecast.exit_bracket)
            reference_exit = _bracket(reference.exit_bracket)
            if (
                forecast_entry is not None and forecast_exit is not None
                and reference_entry is not None and reference_exit is not None
                and not forecast.right_censored
                and not forecast.coverage_censored
            ):
                error = abs(
                    (_midpoint(forecast_exit) - _midpoint(forecast_entry))
                    - (_midpoint(reference_exit) - _midpoint(reference_entry))
                )
                paired_duration_errors.append(error)
                duration_errors.append(error)
        scores.append(PassageEventScore(
            event_id=event_id,
            forecast_count=len(predicted),
            observed_count=len(observed),
            hit_count=hit_count,
            false_alarm_count=false_count,
            miss_count=miss_count,
            censored_forecast_count=sum(
                item.left_censored or item.right_censored or item.coverage_censored
                for item in predicted
            ),
            entry_intersects=(
                all(entry_intersections) if entry_intersections else None
            ),
            entry_contains_reference=(
                all(entry_contains) if entry_contains else None
            ),
            duration_error_seconds=(
                sum(paired_duration_errors) / len(paired_duration_errors)
                if paired_duration_errors else None
            ),
        ))
    return PassageVerificationReport(
        events=tuple(scores),
        event_count=len(scores),
        hit_count=sum(item.hit_count for item in scores),
        false_alarm_count=sum(item.false_alarm_count for item in scores),
        miss_count=sum(item.miss_count for item in scores),
        censored_forecast_count=sum(
            item.censored_forecast_count for item in scores
        ),
        duration_error_mean_seconds=(
            sum(duration_errors) / len(duration_errors)
            if duration_errors else None
        ),
    )


__all__ = [
    "PassageEventScore", "PassageForecastEvidence",
    "PassageReferenceEvidence", "PassageVerificationReport",
    "RadarForecastIssue", "TemporalRadarReference",
    "build_temporal_radar_reference", "verify_passages_by_event",
]
