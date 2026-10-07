"""Pure, provider-neutral observation-time and clock-health assessment.

Negative ages are retained as evidence.  A future timestamp is usable only
when an explicit caller policy allows the skew and fresh independent
clock-health evidence confirms synchronization.  The module has no defaults
and never changes the original timestamps.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import math


TIME_QUALITY_VERSION = "stv3-observation-time-quality-v1"


class ClockHealthState(str, Enum):
    SYNCED = "synced"
    UNSYNCED = "unsynced"
    UNKNOWN = "unknown"


class ObservationTimeState(str, Enum):
    CURRENT = "current"
    STALE = "stale"
    FUTURE_CLOCK_SKEW = "future_clock_skew"
    FUTURE_WITHIN_CONFIRMED_SKEW = "future_within_confirmed_clock_skew"
    RECEIPT_CLOCK_SKEW = "receipt_clock_skew"


@dataclass(frozen=True, slots=True)
class ObservationTimePolicy:
    policy_id: str
    maximum_age_seconds: float
    maximum_future_skew_seconds: float
    maximum_clock_evidence_age_seconds: float
    algorithm_version: str = TIME_QUALITY_VERSION

    def __post_init__(self) -> None:
        _identifier(self.policy_id, "policy_id")
        for name, value in (
            ("maximum_age_seconds", self.maximum_age_seconds),
            ("maximum_future_skew_seconds", self.maximum_future_skew_seconds),
            (
                "maximum_clock_evidence_age_seconds",
                self.maximum_clock_evidence_age_seconds,
            ),
        ):
            if _finite(value, name) < 0.0:
                raise ValueError(f"{name} must be non-negative")
        if self.algorithm_version != TIME_QUALITY_VERSION:
            raise ValueError("unsupported time-quality algorithm version")

    @property
    def fingerprint(self) -> str:
        return _digest(
            self.policy_id,
            format(self.maximum_age_seconds, ".17g"),
            format(self.maximum_future_skew_seconds, ".17g"),
            format(self.maximum_clock_evidence_age_seconds, ".17g"),
            self.algorithm_version,
        )


@dataclass(frozen=True, slots=True)
class ClockHealthEvidence:
    evidence_id: str
    measured_at: float
    state: ClockHealthState
    maximum_absolute_offset_seconds: float | None
    sources: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.evidence_id, "evidence_id")
        _finite(self.measured_at, "measured_at")
        if not isinstance(self.state, ClockHealthState):
            raise TypeError("state must be ClockHealthState")
        if not isinstance(self.sources, tuple) or not self.sources:
            raise ValueError("sources must be a non-empty immutable tuple")
        if (
            self.sources != tuple(sorted(self.sources))
            or len(self.sources) != len(set(self.sources))
        ):
            raise ValueError("sources must be unique and sorted")
        for source in self.sources:
            _identifier(source, "clock source")
        if self.state is ClockHealthState.SYNCED:
            if self.maximum_absolute_offset_seconds is None:
                raise ValueError("synced clock evidence requires measured offset")
            if _finite(
                self.maximum_absolute_offset_seconds,
                "maximum_absolute_offset_seconds",
            ) < 0.0:
                raise ValueError("clock offset must be non-negative")
        elif self.maximum_absolute_offset_seconds is not None:
            if _finite(
                self.maximum_absolute_offset_seconds,
                "maximum_absolute_offset_seconds",
            ) < 0.0:
                raise ValueError("clock offset must be non-negative")


@dataclass(frozen=True, slots=True)
class ObservationTimeAssessment:
    observed_at: float
    received_at: float
    evaluated_at: float
    observation_age_seconds: float
    transport_delay_seconds: float
    future_at_evaluation_seconds: float
    future_at_receipt_seconds: float
    receipt_future_at_evaluation_seconds: float
    state: ObservationTimeState
    usable_for_current_decision: bool
    policy_id: str
    policy_fingerprint: str
    clock_evidence_id: str | None
    reasons: tuple[str, ...]
    algorithm_version: str = TIME_QUALITY_VERSION

    def __post_init__(self) -> None:
        for name, value in (
            ("observed_at", self.observed_at),
            ("received_at", self.received_at),
            ("evaluated_at", self.evaluated_at),
            ("observation_age_seconds", self.observation_age_seconds),
            ("transport_delay_seconds", self.transport_delay_seconds),
            (
                "future_at_evaluation_seconds",
                self.future_at_evaluation_seconds,
            ),
            ("future_at_receipt_seconds", self.future_at_receipt_seconds),
            (
                "receipt_future_at_evaluation_seconds",
                self.receipt_future_at_evaluation_seconds,
            ),
        ):
            _finite(value, name)
        if not isinstance(self.state, ObservationTimeState):
            raise TypeError("state must be ObservationTimeState")
        if not isinstance(self.usable_for_current_decision, bool):
            raise TypeError("usable_for_current_decision must be bool")
        if self.usable_for_current_decision != (
            self.state
            in {
                ObservationTimeState.CURRENT,
                ObservationTimeState.FUTURE_WITHIN_CONFIRMED_SKEW,
            }
        ):
            raise ValueError("usable flag contradicts time-quality state")
        _identifier(self.policy_id, "policy_id")
        _sha256(self.policy_fingerprint, "policy_fingerprint")
        if self.clock_evidence_id is not None:
            _identifier(self.clock_evidence_id, "clock_evidence_id")
        if (
            not isinstance(self.reasons, tuple)
            or not self.reasons
            or self.reasons != tuple(sorted(self.reasons))
            or len(self.reasons) != len(set(self.reasons))
        ):
            raise ValueError("reasons must be non-empty, unique and sorted")
        if self.algorithm_version != TIME_QUALITY_VERSION:
            raise ValueError("unsupported time-quality assessment version")


def assess_observation_time(
    *,
    observed_at: float,
    received_at: float,
    evaluated_at: float,
    policy: ObservationTimePolicy,
    clock_health: ClockHealthEvidence | None,
) -> ObservationTimeAssessment:
    """Assess timestamps without clamping, rewriting, or inventing clock health."""

    if not isinstance(policy, ObservationTimePolicy):
        raise TypeError("policy must be ObservationTimePolicy")
    observed = _finite(observed_at, "observed_at")
    received = _finite(received_at, "received_at")
    evaluated = _finite(evaluated_at, "evaluated_at")
    age = evaluated - observed
    transport_delay = received - observed
    future_evaluation = max(0.0, observed - evaluated)
    future_receipt = max(0.0, observed - received)
    receipt_future = max(0.0, received - evaluated)
    reasons: set[str] = set()

    if receipt_future > policy.maximum_future_skew_seconds:
        state = ObservationTimeState.RECEIPT_CLOCK_SKEW
        reasons.add("receipt_timestamp_exceeds_evaluation_clock")
        usable = False
    elif receipt_future > 0.0:
        clock_valid, clock_reasons = _clock_support(
            clock_health,
            evaluated_at=evaluated,
            maximum_evidence_age_seconds=(
                policy.maximum_clock_evidence_age_seconds
            ),
            maximum_offset_seconds=policy.maximum_future_skew_seconds,
        )
        reasons.update(clock_reasons)
        if clock_valid:
            state = ObservationTimeState.FUTURE_WITHIN_CONFIRMED_SKEW
            reasons.add("receipt_timestamp_within_confirmed_clock_skew")
            usable = True
        else:
            state = ObservationTimeState.RECEIPT_CLOCK_SKEW
            reasons.add("future_receipt_timestamp_quarantined")
            usable = False
    elif future_evaluation > 0.0 or future_receipt > 0.0:
        skew = max(future_evaluation, future_receipt)
        clock_valid, clock_reasons = _clock_support(
            clock_health,
            evaluated_at=evaluated,
            maximum_evidence_age_seconds=(
                policy.maximum_clock_evidence_age_seconds
            ),
            maximum_offset_seconds=policy.maximum_future_skew_seconds,
        )
        reasons.update(clock_reasons)
        if (
            skew <= policy.maximum_future_skew_seconds
            and clock_valid
        ):
            state = ObservationTimeState.FUTURE_WITHIN_CONFIRMED_SKEW
            reasons.add("future_timestamp_within_confirmed_clock_skew")
            usable = True
        else:
            state = ObservationTimeState.FUTURE_CLOCK_SKEW
            reasons.add("future_timestamp_quarantined")
            usable = False
    elif age > policy.maximum_age_seconds:
        state = ObservationTimeState.STALE
        reasons.add("observation_exceeds_freshness_policy")
        usable = False
    else:
        state = ObservationTimeState.CURRENT
        reasons.add("observation_time_current")
        usable = True

    return ObservationTimeAssessment(
        observed_at=observed,
        received_at=received,
        evaluated_at=evaluated,
        observation_age_seconds=age,
        transport_delay_seconds=transport_delay,
        future_at_evaluation_seconds=future_evaluation,
        future_at_receipt_seconds=future_receipt,
        receipt_future_at_evaluation_seconds=receipt_future,
        state=state,
        usable_for_current_decision=usable,
        policy_id=policy.policy_id,
        policy_fingerprint=policy.fingerprint,
        clock_evidence_id=(
            clock_health.evidence_id if clock_health is not None else None
        ),
        reasons=tuple(sorted(reasons)),
    )


def _clock_support(
    evidence: ClockHealthEvidence | None,
    *,
    evaluated_at: float,
    maximum_evidence_age_seconds: float,
    maximum_offset_seconds: float,
) -> tuple[bool, tuple[str, ...]]:
    if evidence is None:
        return False, ("clock_health_evidence_missing",)
    evidence_age = evaluated_at - evidence.measured_at
    if evidence_age < 0.0:
        return False, ("clock_health_evidence_from_future",)
    if evidence_age > maximum_evidence_age_seconds:
        return False, ("clock_health_evidence_stale",)
    if evidence.state is not ClockHealthState.SYNCED:
        return False, (f"clock_health_{evidence.state.value}",)
    if (
        evidence.maximum_absolute_offset_seconds is None
        or evidence.maximum_absolute_offset_seconds > maximum_offset_seconds
    ):
        return False, ("clock_offset_exceeds_policy",)
    return True, ("clock_health_synced",)


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"{name} must be a non-empty safe string")
    return value


def _finite(value: object, name: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"{name} must be finite")
    return float(value)


def _sha256(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be lowercase SHA-256")
    return value


def _digest(*parts: str) -> str:
    return hashlib.sha256("\x00".join(parts).encode("utf-8")).hexdigest()


__all__ = [
    "ClockHealthEvidence",
    "ClockHealthState",
    "ObservationTimeAssessment",
    "ObservationTimePolicy",
    "ObservationTimeState",
    "TIME_QUALITY_VERSION",
    "assess_observation_time",
]
