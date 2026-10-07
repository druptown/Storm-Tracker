"""Bounded target/passage evidence state without global object matching."""

from __future__ import annotations

from dataclasses import dataclass, replace
import math


_OUTCOMES = frozenset({
    "support", "counter", "unusable", "ground_wet", "ground_dry", "ended",
})


@dataclass(frozen=True, slots=True)
class PassageStatePolicy:
    policy_id: str = "candidate_policy_v1"
    confirmation_scans: int = 2
    cancellation_scans: int = 2
    maximum_coasting_seconds: float = 15 * 60.0

    def __post_init__(self) -> None:
        if not self.policy_id:
            raise ValueError("passage_state_policy_id_invalid")
        if self.confirmation_scans < 1 or self.cancellation_scans < 1:
            raise ValueError("passage_state_scan_count_invalid")
        if (
            not math.isfinite(self.maximum_coasting_seconds)
            or self.maximum_coasting_seconds <= 0.0
        ):
            raise ValueError("passage_state_coasting_limit_invalid")


@dataclass(frozen=True, slots=True)
class PassageEvidence:
    target_id: str
    passage_system_id: str
    radar_valid_at: float
    evaluated_at: float
    field_fingerprint: str
    outcome: str
    complete_corridor: bool
    passage_ids: tuple[str, ...]
    lineage_status: str

    def __post_init__(self) -> None:
        if not self.target_id or not self.passage_system_id:
            raise ValueError("passage_evidence_identity_invalid")
        if self.outcome not in _OUTCOMES:
            raise ValueError("passage_evidence_outcome_invalid")
        if not all(math.isfinite(value) for value in (
            self.radar_valid_at, self.evaluated_at,
        )):
            raise ValueError("passage_evidence_time_invalid")
        if not self.field_fingerprint:
            raise ValueError("passage_evidence_fingerprint_invalid")
        if len(set(self.passage_ids)) != len(self.passage_ids):
            raise ValueError("passage_evidence_duplicate_passage_id")


@dataclass(frozen=True, slots=True)
class PassageStateSnapshot:
    target_id: str
    passage_system_id: str
    status: str
    support_scan_count: int
    counterevidence_scan_count: int
    last_radar_valid_at: float | None
    last_support_evaluated_at: float | None
    evaluated_at: float
    passage_ids: tuple[str, ...]
    lineage_status: str
    reason_code: str
    policy_id: str
    validation_status: str = "provisional_unvalidated"


class PassageStateRegistry:
    def __init__(
        self,
        policy: PassageStatePolicy = PassageStatePolicy(),
    ) -> None:
        self._policy = policy
        self._states: dict[tuple[str, str], PassageStateSnapshot] = {}

    def snapshot(
        self,
        target_id: str,
        passage_system_id: str,
    ) -> PassageStateSnapshot:
        return self._states[(target_id, passage_system_id)]

    def recover(
        self,
        target_id: str,
        passage_system_id: str,
        *,
        evaluated_at: float,
    ) -> PassageStateSnapshot:
        state = PassageStateSnapshot(
            target_id=target_id,
            passage_system_id=passage_system_id,
            status="indeterminate",
            support_scan_count=0,
            counterevidence_scan_count=0,
            last_radar_valid_at=None,
            last_support_evaluated_at=None,
            evaluated_at=float(evaluated_at),
            passage_ids=(),
            lineage_status="unknown",
            reason_code="recovering_after_restart",
            policy_id=self._policy.policy_id,
        )
        self._states[(target_id, passage_system_id)] = state
        return state

    def advance(self, evidence: PassageEvidence) -> PassageStateSnapshot:
        key = (evidence.target_id, evidence.passage_system_id)
        previous = self._states.get(key)
        if previous is None:
            previous = PassageStateSnapshot(
                target_id=evidence.target_id,
                passage_system_id=evidence.passage_system_id,
                status="indeterminate",
                support_scan_count=0,
                counterevidence_scan_count=0,
                last_radar_valid_at=None,
                last_support_evaluated_at=None,
                evaluated_at=evidence.evaluated_at,
                passage_ids=(),
                lineage_status=evidence.lineage_status,
                reason_code="no_unique_support_yet",
                policy_id=self._policy.policy_id,
            )

        radar_outcome = evidence.outcome in {"support", "counter", "unusable"}
        if (
            radar_outcome
            and previous.last_radar_valid_at is not None
            and evidence.radar_valid_at <= previous.last_radar_valid_at
        ):
            return previous

        base = replace(
            previous,
            evaluated_at=evidence.evaluated_at,
            passage_ids=evidence.passage_ids or previous.passage_ids,
            lineage_status=evidence.lineage_status,
            last_radar_valid_at=(
                evidence.radar_valid_at
                if radar_outcome
                else previous.last_radar_valid_at
            ),
        )
        if evidence.outcome == "support":
            support = previous.support_scan_count + 1
            state = replace(
                base,
                status=(
                    "confirmed"
                    if support >= self._policy.confirmation_scans
                    else "candidate"
                ),
                support_scan_count=support,
                counterevidence_scan_count=0,
                last_support_evaluated_at=evidence.evaluated_at,
                reason_code=(
                    "consecutive_support_confirmed"
                    if support >= self._policy.confirmation_scans
                    else "first_unique_support"
                ),
            )
        elif evidence.outcome == "counter":
            if not evidence.complete_corridor:
                state = replace(
                    base,
                    status=(
                        "coasting"
                        if previous.support_scan_count >= self._policy.confirmation_scans
                        else previous.status
                    ),
                    reason_code="counterevidence_corridor_incomplete",
                )
            else:
                counters = previous.counterevidence_scan_count + 1
                state = replace(
                    base,
                    status=(
                        "cancelled"
                        if counters >= self._policy.cancellation_scans
                        else "coasting"
                    ),
                    counterevidence_scan_count=counters,
                    reason_code=(
                        "complete_counterevidence_cancelled"
                        if counters >= self._policy.cancellation_scans
                        else "first_complete_counterevidence"
                    ),
                )
        elif evidence.outcome == "unusable":
            expired = (
                previous.last_support_evaluated_at is None
                or evidence.evaluated_at - previous.last_support_evaluated_at
                > self._policy.maximum_coasting_seconds
            )
            state = replace(
                base,
                status="indeterminate" if expired else "coasting",
                reason_code=(
                    "coasting_freshness_exceeded"
                    if expired else "usable_radar_evidence_missing"
                ),
            )
        elif evidence.outcome == "ground_wet":
            state = replace(
                base,
                status="current",
                reason_code="representative_ground_wet",
            )
        elif evidence.outcome == "ground_dry":
            state = replace(
                base,
                status=("coasting" if previous.status == "current" else previous.status),
                reason_code="ground_dry_not_radar_counterevidence",
            )
        else:
            state = replace(base, status="ended", reason_code="exit_bracket_observed")
        self._states[key] = state
        return state


__all__ = [
    "PassageEvidence",
    "PassageStatePolicy",
    "PassageStateRegistry",
    "PassageStateSnapshot",
]
