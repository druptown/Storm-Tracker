from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.observation_time_quality import (
    ClockHealthEvidence,
    ClockHealthState,
    ObservationTimePolicy,
    ObservationTimeState,
    assess_observation_time,
)


ROOT = Path(__file__).resolve().parents[1]
POLICY = ObservationTimePolicy(
    policy_id="test-provider-time-v1",
    maximum_age_seconds=600.0,
    maximum_future_skew_seconds=5.0,
    maximum_clock_evidence_age_seconds=60.0,
)


def _synced(measured_at=1000.0, offset=0.2):
    return ClockHealthEvidence(
        evidence_id="clock-health:test",
        measured_at=measured_at,
        state=ClockHealthState.SYNCED,
        maximum_absolute_offset_seconds=offset,
        sources=("ha_host", "worker_host"),
    )


def test_ha_and_worker_modules_are_exact_mirrors():
    paths = (
        ROOT / "custom_components/storm_tracker_v3/engine/observation_time_quality.py",
        ROOT / "storm_tracker_worker/tracking_core/engine/observation_time_quality.py",
    )
    assert len({
        hashlib.sha256(path.read_bytes()).hexdigest()
        for path in paths
    }) == 1


def test_current_and_stale_keep_signed_age_and_transport_delay():
    current = assess_observation_time(
        observed_at=990.0,
        received_at=995.0,
        evaluated_at=1000.0,
        policy=POLICY,
        clock_health=None,
    )
    assert current.state is ObservationTimeState.CURRENT
    assert current.usable_for_current_decision is True
    assert current.observation_age_seconds == 10.0
    assert current.transport_delay_seconds == 5.0

    stale = assess_observation_time(
        observed_at=300.0,
        received_at=301.0,
        evaluated_at=1000.0,
        policy=POLICY,
        clock_health=None,
    )
    assert stale.state is ObservationTimeState.STALE
    assert stale.usable_for_current_decision is False
    assert stale.observation_age_seconds == 700.0


def test_future_without_clock_evidence_is_quarantined_not_clamped():
    result = assess_observation_time(
        observed_at=1003.0,
        received_at=1000.0,
        evaluated_at=1000.0,
        policy=POLICY,
        clock_health=None,
    )
    assert result.state is ObservationTimeState.FUTURE_CLOCK_SKEW
    assert result.usable_for_current_decision is False
    assert result.observation_age_seconds == -3.0
    assert result.transport_delay_seconds == -3.0
    assert result.future_at_evaluation_seconds == 3.0
    assert "clock_health_evidence_missing" in result.reasons


def test_future_is_usable_only_with_fresh_synced_evidence_and_policy():
    result = assess_observation_time(
        observed_at=1003.0,
        received_at=1000.0,
        evaluated_at=1000.0,
        policy=POLICY,
        clock_health=_synced(),
    )
    assert result.state is (
        ObservationTimeState.FUTURE_WITHIN_CONFIRMED_SKEW
    )
    assert result.usable_for_current_decision is True
    assert result.clock_evidence_id == "clock-health:test"

    too_far = assess_observation_time(
        observed_at=1006.0,
        received_at=1000.0,
        evaluated_at=1000.0,
        policy=POLICY,
        clock_health=_synced(),
    )
    assert too_far.state is ObservationTimeState.FUTURE_CLOCK_SKEW
    assert too_far.usable_for_current_decision is False


@pytest.mark.parametrize(
    ("evidence", "reason"),
    (
        (_synced(measured_at=900.0), "clock_health_evidence_stale"),
        (
            ClockHealthEvidence(
                "clock-health:bad",
                1000.0,
                ClockHealthState.UNSYNCED,
                20.0,
                ("worker_host",),
            ),
            "clock_health_unsynced",
        ),
    ),
)
def test_stale_or_unsynced_clock_evidence_never_unquarantines(
    evidence,
    reason,
):
    result = assess_observation_time(
        observed_at=1002.0,
        received_at=1000.0,
        evaluated_at=1000.0,
        policy=POLICY,
        clock_health=evidence,
    )
    assert result.state is ObservationTimeState.FUTURE_CLOCK_SKEW
    assert reason in result.reasons


def test_timestamp_future_at_receipt_remains_explicit_at_later_evaluation():
    result = assess_observation_time(
        observed_at=1003.0,
        received_at=1000.0,
        evaluated_at=1004.0,
        policy=POLICY,
        clock_health=None,
    )
    assert result.observation_age_seconds == 1.0
    assert result.future_at_evaluation_seconds == 0.0
    assert result.future_at_receipt_seconds == 3.0
    assert result.state is ObservationTimeState.FUTURE_CLOCK_SKEW
    assert result.usable_for_current_decision is False


def test_receipt_clock_far_ahead_of_evaluation_is_quarantined():
    result = assess_observation_time(
        observed_at=999.0,
        received_at=1010.0,
        evaluated_at=1000.0,
        policy=POLICY,
        clock_health=_synced(),
    )
    assert result.state is ObservationTimeState.RECEIPT_CLOCK_SKEW
    assert result.usable_for_current_decision is False
    assert result.receipt_future_at_evaluation_seconds == 10.0


def test_small_future_receipt_requires_fresh_synced_clock_evidence():
    quarantined = assess_observation_time(
        observed_at=999.0,
        received_at=1003.0,
        evaluated_at=1000.0,
        policy=POLICY,
        clock_health=None,
    )
    assert quarantined.state is ObservationTimeState.RECEIPT_CLOCK_SKEW
    assert quarantined.usable_for_current_decision is False
    assert quarantined.receipt_future_at_evaluation_seconds == 3.0
    assert "clock_health_evidence_missing" in quarantined.reasons
    assert "future_receipt_timestamp_quarantined" in quarantined.reasons

    confirmed = assess_observation_time(
        observed_at=999.0,
        received_at=1003.0,
        evaluated_at=1000.0,
        policy=POLICY,
        clock_health=_synced(),
    )
    assert confirmed.state is (
        ObservationTimeState.FUTURE_WITHIN_CONFIRMED_SKEW
    )
    assert confirmed.usable_for_current_decision is True
    assert confirmed.receipt_future_at_evaluation_seconds == 3.0
    assert "receipt_timestamp_within_confirmed_clock_skew" in (
        confirmed.reasons
    )
