from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys

import pytest

from storm_tracker_worker.operational_slo_evidence import (
    CandidateRuntimeFacts,
    CompleteMetricSeries,
    LatencyStratum,
    OperationalSloEvidenceError,
    OperationalSoakRun,
    SequenceIntegrity,
    SnapshotIntegrity,
    RUN_ARTIFACT_CONTRACT,
    SoakPhase,
    build_operational_slo_evidence,
    parse_operational_soak_run_artifact,
)
from storm_tracker_worker.scientific_validation import (
    GateState,
    evaluate_manifest,
    roadmap_release_manifest,
)


SHA_ENV = "1" * 64
SHA_METHOD = "2" * 64


def _series(name: str, values, *, missing: int = 0, unit: str = "ms"):
    values = tuple(float(value) for value in values)
    return CompleteMetricSeries(
        series_id=name,
        artifact_sha256=("3" if "baseline" in name else "4") * 64,
        unit=unit,
        values=values,
        expected_sample_count=len(values) + missing,
        missing_sample_count=missing,
    )


def _baseline(**changes) -> OperationalSoakRun:
    values = {
        "phase": SoakPhase.BASELINE,
        "run_id": "baseline-run",
        "started_at": 1_800_000_000.0,
        "ended_at": 1_800_000_000.0 + 604_800.0,
        "comparison_environment_sha256": SHA_ENV,
        "measurement_method_sha256": SHA_METHOD,
        "run_artifact_sha256": "5" * 64,
        "clock_integrity_violations": 0,
        "ha_event_loop_lag_ms": _series(
            "ha-baseline", range(1, 101)
        ),
        "z2m_ingress_state_latency_ms": _series(
            "z2m-baseline", range(1, 101)
        ),
        "z2m_ingress_state_latency_strata_ms": (
            LatencyStratum(
                "device_a.temperature",
                _series("z2m-device-a-baseline", range(1, 101)),
            ),
            LatencyStratum(
                "device_b.motion",
                _series("z2m-device-b-baseline", range(2, 102)),
            ),
        ),
        "z2m_sequence": SequenceIntegrity(100, 100, 100, 0),
    }
    values.update(changes)
    return OperationalSoakRun(**values)


def _candidate(**changes) -> OperationalSoakRun:
    runtime = CandidateRuntimeFacts(
        queue_age_cadence_fraction=_series(
            "queue-candidate",
            (0.1, 0.2, 0.3, 0.4),
            unit="cadence_fraction",
        ),
        heavy_ha_runtime_invocations=0,
        snapshot=SnapshotIntegrity(0, 0, 0, 1.0),
        disk_projected_days_remaining=21.0,
        worker_cpu_threads=2,
        worker_nice=15,
        reserved_physical_cores=1,
        thermal_throttling_events=0,
    )
    values = {
        "phase": SoakPhase.CANDIDATE,
        "run_id": "candidate-run",
        "started_at": 1_801_000_000.0,
        "ended_at": 1_801_000_000.0 + 604_800.0,
        "comparison_environment_sha256": SHA_ENV,
        "measurement_method_sha256": SHA_METHOD,
        "run_artifact_sha256": "6" * 64,
        "clock_integrity_violations": 0,
        "ha_event_loop_lag_ms": _series(
            "ha-candidate", (*range(1, 100), 109)
        ),
        "z2m_ingress_state_latency_ms": _series(
            "z2m-candidate", range(11, 111)
        ),
        "z2m_ingress_state_latency_strata_ms": (
            LatencyStratum(
                "device_a.temperature",
                _series("z2m-device-a-candidate", range(11, 111)),
            ),
            LatencyStratum(
                "device_b.motion",
                _series("z2m-device-b-candidate", range(12, 112)),
            ),
        ),
        "z2m_sequence": SequenceIntegrity(100, 100, 100, 0),
        "candidate_runtime": runtime,
    }
    values.update(changes)
    return OperationalSoakRun(**values)


def _evidence_map(bundle):
    return {item.gate_id: item.candidate_value for item in bundle.evidence}


def _series_document(series: CompleteMetricSeries):
    return {
        "expected_sample_count": series.expected_sample_count,
        "missing_sample_count": series.missing_sample_count,
        "quantile_method": series.quantile_method,
        "series_id": series.series_id,
        "unit": series.unit,
        "values": list(series.values),
    }


def _run_document(run: OperationalSoakRun):
    runtime = run.candidate_runtime
    candidate_runtime = None
    if runtime is not None:
        candidate_runtime = {
            "disk_projected_days_remaining": runtime.disk_projected_days_remaining,
            "heavy_ha_runtime_invocations": runtime.heavy_ha_runtime_invocations,
            "queue_age_cadence_fraction": _series_document(
                runtime.queue_age_cadence_fraction
            ),
            "reserved_physical_cores": runtime.reserved_physical_cores,
            "snapshot": runtime.snapshot.to_mapping(),
            "thermal_throttling_events": runtime.thermal_throttling_events,
            "worker_cpu_threads": runtime.worker_cpu_threads,
            "worker_nice": runtime.worker_nice,
        }
    return {
        "candidate_runtime": candidate_runtime,
        "clock_integrity_violations": run.clock_integrity_violations,
        "comparison_environment_sha256": run.comparison_environment_sha256,
        "contract": RUN_ARTIFACT_CONTRACT,
        "ended_at": run.ended_at,
        "ha_event_loop_lag_ms": _series_document(run.ha_event_loop_lag_ms),
        "measurement_method_sha256": run.measurement_method_sha256,
        "phase": run.phase.value,
        "run_id": run.run_id,
        "started_at": run.started_at,
        "z2m_ingress_state_latency_ms": _series_document(
            run.z2m_ingress_state_latency_ms
        ),
        "z2m_ingress_state_latency_strata_ms": [
            {
                "series": _series_document(item.series),
                "stratum_id": item.stratum_id,
            }
            for item in run.z2m_ingress_state_latency_strata_ms
        ],
        "z2m_sequence": {
            "expected_messages": run.z2m_sequence.expected_messages,
            "observed_messages": run.z2m_sequence.observed_messages,
            "out_of_order_messages": run.z2m_sequence.out_of_order_messages,
            "unique_messages": run.z2m_sequence.unique_messages,
        },
    }


def test_complete_runs_cover_every_operational_gate_deterministically():
    first = build_operational_slo_evidence(_baseline(), _candidate())
    second = build_operational_slo_evidence(_baseline(), _candidate())
    values = _evidence_map(first)
    gate_ids = {
        gate.gate_id
        for gate in roadmap_release_manifest().gates
        if gate.gate_id.startswith("operations.")
    }

    assert set(values) == gate_ids
    assert first == second
    assert first.bundle_sha256 == second.bundle_sha256
    assert values["operations.shadow_soak_seconds"] == 604_800.0
    assert values["operations.baseline_soak_seconds"] == 604_800.0
    assert values["operations.clock_integrity_violations"] == 0.0
    assert values["operations.ha_event_loop_lag_p99_ms"] == 99.0
    assert values["operations.ha_event_loop_lag_regression_p99_ms"] == 0.0
    assert values["operations.ha_event_loop_stalls_over_100ms"] == 1.0
    assert values["operations.z2m_ingress_state_regression_p95_ms"] == 10.0
    assert values["operations.z2m_ingress_state_regression_p99_ms"] == 10.0
    assert values["operations.worker_queue_age_p95_cadence_fraction"] == 0.4
    assert values["operations.worker_cpu_threads"] == 2.0
    assert values["operations.worker_nice"] == 15.0
    assert values["operations.reserved_physical_cores"] == 1.0
    assert list(first.to_mapping()) == [
        "baseline_run_id",
        "bundle_sha256",
        "candidate_run_id",
        "contract",
        "evidence",
    ]


def test_z2m_gaps_duplicates_and_order_are_not_hidden():
    candidate = _candidate(
        z2m_sequence=SequenceIntegrity(
            expected_messages=100,
            observed_messages=99,
            unique_messages=97,
            out_of_order_messages=3,
        )
    )
    values = _evidence_map(
        build_operational_slo_evidence(_baseline(), candidate)
    )
    assert values["operations.z2m_message_loss"] == 3.0
    assert values[
        "operations.z2m_duplicate_or_out_of_order_messages"
    ] == 5.0


def test_worst_preregistered_z2m_stratum_cannot_hide_in_global_percentile():
    candidate = _candidate(
        z2m_ingress_state_latency_strata_ms=(
            LatencyStratum(
                "device_a.temperature",
                _series("z2m-device-a-candidate", range(11, 111)),
            ),
            LatencyStratum(
                "device_b.motion",
                _series("z2m-device-b-candidate", range(72, 172)),
            ),
        ),
    )
    values = _evidence_map(
        build_operational_slo_evidence(_baseline(), candidate)
    )
    assert values["operations.z2m_ingress_state_regression_p95_ms"] == 70.0
    assert values["operations.z2m_ingress_state_regression_p99_ms"] == 70.0

    with pytest.raises(OperationalSloEvidenceError, match="strata differ"):
        build_operational_slo_evidence(
            _baseline(),
            replace(
                candidate,
                z2m_ingress_state_latency_strata_ms=(
                    candidate.z2m_ingress_state_latency_strata_ms[0],
                ),
            ),
        )


def test_incomplete_series_or_comparison_drift_stays_indeterminate():
    incomplete = _candidate(
        ha_event_loop_lag_ms=_series(
            "ha-candidate", range(1, 101), missing=1
        )
    )
    with pytest.raises(OperationalSloEvidenceError, match="incomplete"):
        build_operational_slo_evidence(_baseline(), incomplete)

    with pytest.raises(OperationalSloEvidenceError, match="environments differ"):
        build_operational_slo_evidence(
            _baseline(),
            _candidate(comparison_environment_sha256="9" * 64),
        )
    with pytest.raises(OperationalSloEvidenceError, match="methods differ"):
        build_operational_slo_evidence(
            _baseline(),
            _candidate(measurement_method_sha256="8" * 64),
        )


def test_clock_integrity_violation_remains_visible_release_evidence():
    values = _evidence_map(build_operational_slo_evidence(
        _baseline(clock_integrity_violations=1),
        _candidate(clock_integrity_violations=2),
    ))
    assert values["operations.clock_integrity_violations"] == 3.0


def test_nearest_rank_keeps_tail_and_rejects_nonfinite_or_bad_accounting():
    series = _series("tail-candidate", (1.0, 2.0, 3.0, 100.0))
    assert series.quantile(0.95) == 100.0
    assert series.quantile(0.99) == 100.0

    with pytest.raises(OperationalSloEvidenceError):
        CompleteMetricSeries(
            "bad",
            "a" * 64,
            "ms",
            (1.0, math.inf),
            2,
        )
    with pytest.raises(OperationalSloEvidenceError, match="inconsistent"):
        SequenceIntegrity(1, 0, 1, 0)


def test_phase_contract_prevents_worker_facts_in_baseline_or_missing_candidate():
    with pytest.raises(OperationalSloEvidenceError, match="baseline cannot"):
        replace(_baseline(), candidate_runtime=_candidate().candidate_runtime)
    with pytest.raises(OperationalSloEvidenceError, match="requires worker"):
        replace(_candidate(), candidate_runtime=None)


def test_latency_series_need_preregistered_minimum_support():
    with pytest.raises(OperationalSloEvidenceError, match="HA latency support"):
        replace(
            _baseline(),
            ha_event_loop_lag_ms=_series("ha-baseline", range(1, 100)),
        )
    with pytest.raises(OperationalSloEvidenceError, match="stratum support"):
        replace(
            _candidate(),
            z2m_ingress_state_latency_strata_ms=(
                LatencyStratum(
                    "device_a.temperature",
                    _series("z2m-device-a-candidate", range(1, 100)),
                ),
                _candidate().z2m_ingress_state_latency_strata_ms[1],
            ),
        )


def test_bundle_provenance_is_bounded_sorted_and_contains_no_run_payload():
    bundle = build_operational_slo_evidence(_baseline(), _candidate())
    for item in bundle.evidence:
        assert item.provenance_ids == tuple(sorted(set(item.provenance_ids)))
        assert all(len(value) <= 192 for value in item.provenance_ids)
    encoded = str(bundle.to_mapping()).lower()
    for forbidden in ("token", "password", "latitude", "longitude"):
        assert forbidden not in encoded


def test_complete_good_soak_passes_every_operational_release_gate():
    baseline = _baseline(
        ha_event_loop_lag_ms=_series("ha-baseline", (5.0,) * 100),
        z2m_ingress_state_latency_ms=_series("z2m-baseline", (10.0,) * 100),
    )
    candidate = _candidate(
        ha_event_loop_lag_ms=_series("ha-candidate", (6.0,) * 100),
        z2m_ingress_state_latency_ms=_series("z2m-candidate", (20.0,) * 100),
    )
    bundle = build_operational_slo_evidence(baseline, candidate)
    report = evaluate_manifest(roadmap_release_manifest(), bundle.evidence)
    operational = tuple(
        result
        for result in report.results
        if result.gate_id.startswith("operations.")
    )

    assert operational
    assert all(result.state is GateState.PASS for result in operational)
    # The full meteorological release still remains blocked by its independent
    # truth gates and by the deliberately unlocked preregistration manifest.
    assert report.release_ready is False


def test_closed_run_artifact_derives_run_and_series_hashes_from_content():
    document = _run_document(_baseline())
    payload = json.dumps(document, indent=2, sort_keys=False).encode("utf-8")
    parsed = parse_operational_soak_run_artifact(payload)
    canonical = json.dumps(
        document,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    series_document = document["ha_event_loop_lag_ms"]
    series_canonical = json.dumps(
        series_document,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")

    assert parsed.run_artifact_sha256 == hashlib.sha256(canonical).hexdigest()
    assert parsed.ha_event_loop_lag_ms.artifact_sha256 == hashlib.sha256(
        series_canonical
    ).hexdigest()
    assert parsed.run_artifact_sha256 != _baseline().run_artifact_sha256


def test_parsed_artifacts_build_the_same_deterministic_gate_set():
    baseline = parse_operational_soak_run_artifact(
        json.dumps(_run_document(_baseline())).encode("utf-8")
    )
    candidate = parse_operational_soak_run_artifact(
        json.dumps(_run_document(_candidate())).encode("utf-8")
    )
    bundle = build_operational_slo_evidence(baseline, candidate)
    manifest_ids = {
        gate.gate_id
        for gate in roadmap_release_manifest().gates
        if gate.gate_id.startswith("operations.")
    }
    assert {item.gate_id for item in bundle.evidence} == manifest_ids


@pytest.mark.parametrize("mutation", ("extra", "duplicate", "nan"))
def test_run_artifact_rejects_unknown_duplicate_or_nonfinite_fields(mutation):
    document = _run_document(_baseline())
    if mutation == "extra":
        document["password"] = "must-not-enter-provenance"
        payload = json.dumps(document).encode("utf-8")
        match = "schema drifted"
    elif mutation == "duplicate":
        payload = (
            '{"contract":"' + RUN_ARTIFACT_CONTRACT + '",'
            '"contract":"' + RUN_ARTIFACT_CONTRACT + '"}'
        ).encode("utf-8")
        match = "duplicate key"
    else:
        document["started_at"] = math.nan
        payload = json.dumps(document).encode("utf-8")
        match = "non-finite"
    with pytest.raises(OperationalSloEvidenceError, match=match):
        parse_operational_soak_run_artifact(payload)


def test_cli_atomically_publishes_bundle_and_refuses_overwrite(tmp_path: Path):
    baseline = tmp_path / "baseline.json"
    candidate = tmp_path / "candidate.json"
    output = tmp_path / "evidence.json"
    baseline.write_text(json.dumps(_run_document(_baseline())), encoding="utf-8")
    candidate.write_text(json.dumps(_run_document(_candidate())), encoding="utf-8")
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "build_operational_slo_evidence.py"
    )
    command = [
        sys.executable,
        str(script),
        str(baseline),
        str(candidate),
        "--output",
        str(output),
    ]

    first = subprocess.run(command, check=False, capture_output=True, text=True)
    assert first.returncode == 0, first.stderr
    first_bytes = output.read_bytes()
    document = json.loads(first_bytes)
    assert document["contract"] == "stv3-operational-slo-evidence-v1"
    assert len(document["evidence"]) == len({
        gate.gate_id
        for gate in roadmap_release_manifest().gates
        if gate.gate_id.startswith("operations.")
    })

    second = subprocess.run(command, check=False, capture_output=True, text=True)
    assert second.returncode == 2
    assert "refusing to overwrite" in second.stderr
    assert output.read_bytes() == first_bytes
    assert not tuple(tmp_path.glob(".*.staging"))
