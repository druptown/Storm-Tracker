"""Tests for fail-closed, preregistered scientific release gates."""
from __future__ import annotations

import pytest

from storm_tracker_worker.scientific_validation import (
    Comparator,
    GateState,
    MetricEvidence,
    MetricGate,
    ValidationManifest,
    evaluate_manifest,
    roadmap_release_manifest,
)


def _locked_manifest(*gates: MetricGate) -> ValidationManifest:
    return ValidationManifest(
        manifest_version="test-manifest-v1",
        corpus_id="held-out-events-sha256:abc",
        corpus_split_policy="event-held-out",
        gates=tuple(gates),
    ).lock(lock_note="locked before release evidence")


def test_missing_evidence_is_indeterminate_and_never_release_ready():
    manifest = _locked_manifest(MetricGate(
        "loss",
        "storage",
        Comparator.EQUAL,
        "No loss.",
        threshold=0.0,
    ))

    report = evaluate_manifest(manifest, ())

    assert report.release_ready is False
    assert report.results[0].state is GateState.INDETERMINATE
    assert report.results[0].reason == "evidence ontbreekt"


def test_external_truth_cannot_be_replaced_by_internal_track_speed():
    # An external-truth gate cannot be locked without a declared label/baseline
    # contract.  It may remain draft while internal diagnostics are inspected.
    manifest = ValidationManifest(
        manifest_version="test-manifest-v1",
        corpus_id="held-out-events-sha256:abc",
        corpus_split_policy="event-held-out",
        gates=(MetricGate(
            "tracking.speed_error",
            "tracking",
            Comparator.NOT_GREATER_THAN_BASELINE,
            "Speed error needs independent truth.",
            requires_external_truth=True,
            minimum_events=1,
        ),),
    )
    kmi_internal_diagnostic = MetricEvidence(
        gate_id="tracking.speed_error",
        candidate_value=131.74,
        baseline_value=123.93,
        event_count=36,
        external_truth_available=False,
        provenance_ids=("kmi-normalized-cache:region-1",),
    )

    report = evaluate_manifest(manifest, (kmi_internal_diagnostic,))

    assert report.release_ready is False
    assert report.results[0].state is GateState.INDETERMINATE
    assert report.results[0].reason == "onafhankelijke ground truth ontbreekt"


def test_release_gate_reports_failure_without_filtering_observations():
    manifest = _locked_manifest(MetricGate(
        "p0.silently_dropped_detections",
        "tracking",
        Comparator.EQUAL,
        "No silent drops.",
        threshold=0.0,
    ))

    report = evaluate_manifest(manifest, (
        MetricEvidence(
            "p0.silently_dropped_detections",
            candidate_value=1.0,
            provenance_ids=("fault-replay:drop-1",),
        ),
    ))

    assert report.release_ready is False
    assert report.results[0].state is GateState.FAIL
    assert report.results[0].candidate_value == 1.0
    assert report.results[0].provenance_ids == ("fault-replay:drop-1",)


def test_comparison_direction_is_explicit_for_skill_and_error():
    manifest = _locked_manifest(
        MetricGate(
            "tracking.completeness",
            "tracking",
            Comparator.NOT_LESS_THAN_BASELINE,
            "Higher is better.",
        ),
        MetricGate(
            "tracking.position_error",
            "tracking",
            Comparator.NOT_GREATER_THAN_BASELINE,
            "Lower is better.",
        ),
    )
    report = evaluate_manifest(manifest, (
        MetricEvidence(
            "tracking.completeness",
            candidate_value=0.91,
            baseline_value=0.90,
        ),
        MetricEvidence(
            "tracking.position_error",
            candidate_value=1.9,
            baseline_value=2.0,
        ),
    ))

    assert report.release_ready is True
    assert {result.state for result in report.results} == {GateState.PASS}


def test_roadmap_manifest_is_deterministic_but_deliberately_not_lockable():
    first = roadmap_release_manifest()
    second = roadmap_release_manifest()

    assert first.sha256 == second.sha256
    assert first.to_dict() == second.to_dict()
    assert first.locked is False
    with pytest.raises(ValueError, match="onopgeloste"):
        first.lock(lock_note="must not lock unset split/merge margins")
    report = evaluate_manifest(first, ())
    assert report.release_ready is False
    assert "preregistratiemanifest is niet vergrendeld" in (
        report.blocking_reasons
    )


def test_seven_day_soak_threshold_is_fixed_in_roadmap_manifest():
    manifest = roadmap_release_manifest()
    soak = next(
        gate
        for gate in manifest.gates
        if gate.gate_id == "operations.shadow_soak_seconds"
    )
    assert soak.comparator is Comparator.MINIMUM
    assert soak.threshold == 604_800.0


def test_temperature_peak_alone_is_not_a_release_failure():
    gate_ids = {
        gate.gate_id
        for gate in roadmap_release_manifest().gates
    }
    assert "operations.thermal_throttling_events" in gate_ids
    assert "operations.maximum_temperature_c" not in gate_ids


def test_ha_z2m_worker_and_storage_slos_are_hard_release_gates():
    gates = {
        gate.gate_id: gate
        for gate in roadmap_release_manifest().gates
    }

    expected = {
        "operations.ha_event_loop_lag_p99_ms": (Comparator.MAXIMUM, 20.0),
        "operations.ha_event_loop_lag_regression_p99_ms": (
            Comparator.MAXIMUM,
            10.0,
        ),
        "operations.ha_event_loop_stalls_over_100ms": (Comparator.EQUAL, 0.0),
        "operations.heavy_ha_runtime_invocations": (Comparator.EQUAL, 0.0),
        "operations.z2m_ingress_state_regression_p95_ms": (
            Comparator.MAXIMUM,
            50.0,
        ),
        "operations.z2m_ingress_state_regression_p99_ms": (
            Comparator.MAXIMUM,
            100.0,
        ),
        "operations.z2m_message_loss": (Comparator.EQUAL, 0.0),
        "operations.z2m_duplicate_or_out_of_order_messages": (
            Comparator.EQUAL,
            0.0,
        ),
        "operations.worker_queue_age_p95_cadence_fraction": (
            Comparator.MAXIMUM,
            1.0,
        ),
        "operations.snapshot_stale_after_missed_cadences": (
            Comparator.MAXIMUM,
            2.0,
        ),
        "operations.snapshot_authority_violations": (Comparator.EQUAL, 0.0),
        "operations.snapshot_expired_current_claims": (Comparator.EQUAL, 0.0),
        "operations.snapshot_publishes_without_qos1_puback": (
            Comparator.EQUAL,
            0.0,
        ),
        "operations.disk_projected_days_remaining": (Comparator.MINIMUM, 7.0),
        "operations.worker_cpu_threads": (Comparator.EQUAL, 2.0),
        "operations.worker_nice": (Comparator.EQUAL, 15.0),
        "operations.reserved_physical_cores": (Comparator.MINIMUM, 1.0),
    }
    assert set(expected) <= set(gates)
    for gate_id, (comparator, threshold) in expected.items():
        assert gates[gate_id].comparator is comparator
        assert gates[gate_id].threshold == threshold


def test_unknown_or_duplicate_evidence_is_rejected():
    manifest = _locked_manifest(MetricGate(
        "known",
        "test",
        Comparator.POSITIVE,
        "Positive metric.",
    ))
    with pytest.raises(ValueError, match="onbekende"):
        evaluate_manifest(
            manifest,
            (MetricEvidence("unknown", candidate_value=1.0),),
        )
    duplicate = MetricEvidence("known", candidate_value=1.0)
    with pytest.raises(ValueError, match="dubbele"):
        evaluate_manifest(manifest, (duplicate, duplicate))


def test_manifest_hash_changes_when_a_threshold_changes():
    first = _locked_manifest(MetricGate(
        "threshold",
        "test",
        Comparator.MINIMUM,
        "Fixed threshold.",
        threshold=0.80,
    ))
    second = _locked_manifest(MetricGate(
        "threshold",
        "test",
        Comparator.MINIMUM,
        "Fixed threshold.",
        threshold=0.81,
    ))
    assert first.sha256 != second.sha256


@pytest.mark.parametrize("value", (float("nan"), float("inf")))
def test_nonfinite_metric_evidence_is_rejected(value):
    with pytest.raises(ValueError, match="eindig"):
        MetricEvidence("finite", candidate_value=value)
