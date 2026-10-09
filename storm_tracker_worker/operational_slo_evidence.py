"""Fail-closed operational soak evidence for the release manifest.

The collector that runs beside Home Assistant is intentionally outside this
pure module.  This boundary accepts only bounded, content-addressed run data,
checks that baseline and candidate used the same measurement environment, and
derives every ``operations.*`` metric in the roadmap release manifest.  Missing
samples or a phase/configuration mismatch remain indeterminate by raising
before any release evidence is emitted.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import re
from typing import Any

from .scientific_validation import MetricEvidence, roadmap_release_manifest


CONTRACT = "stv3-operational-slo-evidence-v1"
RUN_ARTIFACT_CONTRACT = "stv3-operational-soak-run-artifact-v1"
QUANTILE_METHOD = "nearest_rank_complete_series_v1"
MAX_SERIES_SAMPLES = 1_000_000
MIN_LATENCY_SERIES_SAMPLES = 100
MAX_RUN_ARTIFACT_BYTES = 128 * 1024 * 1024
MAX_SAMPLE_VALUE = 1_000_000_000.0
MIN_RUN_TIMESTAMP = 1_577_836_800.0  # 2020-01-01T00:00:00Z
MAX_RUN_TIMESTAMP = 4_102_444_800.0  # 2100-01-01T00:00:00Z
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_.:@/-]{1,192}$")


class OperationalSloEvidenceError(ValueError):
    """The supplied soak evidence cannot support a release decision."""


class SoakPhase(str, Enum):
    BASELINE = "integration_off_baseline"
    CANDIDATE = "worker_only_candidate"


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise OperationalSloEvidenceError(
            "operational evidence is not canonical JSON"
        ) from exc


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise OperationalSloEvidenceError(f"{label} is not a lowercase SHA-256")
    return value


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise OperationalSloEvidenceError(f"{label} is not a safe identifier")
    return value


def _finite(
    value: object,
    label: str,
    *,
    minimum: float = 0.0,
    maximum: float = MAX_SAMPLE_VALUE,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OperationalSloEvidenceError(f"{label} is not numeric")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise OperationalSloEvidenceError(f"{label} is outside its bound")
    return result


def _counter(value: object, label: str) -> int:
    if type(value) is not int or not 0 <= value <= 1_000_000_000:
        raise OperationalSloEvidenceError(f"{label} is not a bounded counter")
    return value


@dataclass(frozen=True, slots=True)
class CompleteMetricSeries:
    """One immutable, gap-accounted numeric sample series."""

    series_id: str
    artifact_sha256: str
    unit: str
    values: tuple[float, ...]
    expected_sample_count: int
    missing_sample_count: int = 0
    quantile_method: str = QUANTILE_METHOD

    def __post_init__(self) -> None:
        _identifier(self.series_id, "series_id")
        _sha(self.artifact_sha256, "series artifact")
        _identifier(self.unit, "series unit")
        if self.quantile_method != QUANTILE_METHOD:
            raise OperationalSloEvidenceError("series quantile method drifted")
        if not isinstance(self.values, tuple) or not self.values:
            raise OperationalSloEvidenceError("series must contain samples")
        if len(self.values) > MAX_SERIES_SAMPLES:
            raise OperationalSloEvidenceError("series exceeds sample capacity")
        for index, value in enumerate(self.values):
            _finite(value, f"series sample {index}")
        expected = _counter(self.expected_sample_count, "expected sample count")
        missing = _counter(self.missing_sample_count, "missing sample count")
        if expected != len(self.values) + missing:
            raise OperationalSloEvidenceError(
                "expected sample count does not match values plus gaps"
            )

    @property
    def complete(self) -> bool:
        return self.missing_sample_count == 0

    def quantile(self, fraction: float) -> float:
        if not 0.0 < fraction <= 1.0:
            raise ValueError("quantile fraction must be in (0, 1]")
        ordered = sorted(float(value) for value in self.values)
        # Nearest-rank is deliberately simple and preregistered.  It does not
        # interpolate away a bad tail sample.
        rank = max(1, math.ceil(fraction * len(ordered)))
        return ordered[rank - 1]

    def to_mapping(self) -> dict[str, Any]:
        return {
            "artifact_sha256": self.artifact_sha256,
            "expected_sample_count": self.expected_sample_count,
            "missing_sample_count": self.missing_sample_count,
            "quantile_method": self.quantile_method,
            "series_id": self.series_id,
            "unit": self.unit,
            "values": list(self.values),
        }


@dataclass(frozen=True, slots=True)
class SequenceIntegrity:
    """Exact correlated-message accounting for one Z2M run."""

    expected_messages: int
    observed_messages: int
    unique_messages: int
    out_of_order_messages: int

    def __post_init__(self) -> None:
        expected = _counter(self.expected_messages, "expected messages")
        observed = _counter(self.observed_messages, "observed messages")
        unique = _counter(self.unique_messages, "unique messages")
        out_of_order = _counter(
            self.out_of_order_messages,
            "out-of-order messages",
        )
        if unique > expected or unique > observed or out_of_order > observed:
            raise OperationalSloEvidenceError("sequence accounting is inconsistent")

    @property
    def missing_messages(self) -> int:
        return self.expected_messages - self.unique_messages

    @property
    def duplicate_messages(self) -> int:
        return self.observed_messages - self.unique_messages

    def to_mapping(self) -> dict[str, int]:
        return {
            "duplicate_messages": self.duplicate_messages,
            "expected_messages": self.expected_messages,
            "missing_messages": self.missing_messages,
            "observed_messages": self.observed_messages,
            "out_of_order_messages": self.out_of_order_messages,
            "unique_messages": self.unique_messages,
        }


@dataclass(frozen=True, slots=True)
class LatencyStratum:
    """One preregistered Z2M device/cluster latency stratum."""

    stratum_id: str
    series: CompleteMetricSeries

    def __post_init__(self) -> None:
        _identifier(self.stratum_id, "latency stratum_id")
        if not isinstance(self.series, CompleteMetricSeries):
            raise TypeError("latency stratum series is invalid")
        if self.series.unit != "ms":
            raise OperationalSloEvidenceError("latency stratum unit is invalid")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "series": self.series.to_mapping(),
            "stratum_id": self.stratum_id,
        }


@dataclass(frozen=True, slots=True)
class SnapshotIntegrity:
    authority_violations: int
    expired_current_claims: int
    publishes_without_qos1_puback: int
    stale_after_missed_cadences: float

    def __post_init__(self) -> None:
        _counter(self.authority_violations, "snapshot authority violations")
        _counter(self.expired_current_claims, "expired current claims")
        _counter(
            self.publishes_without_qos1_puback,
            "publishes without PUBACK",
        )
        _finite(
            self.stale_after_missed_cadences,
            "stale missed cadences",
        )

    def to_mapping(self) -> dict[str, float | int]:
        return {
            "authority_violations": self.authority_violations,
            "expired_current_claims": self.expired_current_claims,
            "publishes_without_qos1_puback": self.publishes_without_qos1_puback,
            "stale_after_missed_cadences": self.stale_after_missed_cadences,
        }


@dataclass(frozen=True, slots=True)
class CandidateRuntimeFacts:
    queue_age_cadence_fraction: CompleteMetricSeries
    heavy_ha_runtime_invocations: int
    snapshot: SnapshotIntegrity
    disk_projected_days_remaining: float
    worker_cpu_threads: int
    worker_nice: int
    reserved_physical_cores: int
    thermal_throttling_events: int

    def __post_init__(self) -> None:
        if not isinstance(self.queue_age_cadence_fraction, CompleteMetricSeries):
            raise TypeError("queue series is invalid")
        if self.queue_age_cadence_fraction.unit != "cadence_fraction":
            raise OperationalSloEvidenceError("queue series unit is invalid")
        if not isinstance(self.snapshot, SnapshotIntegrity):
            raise TypeError("snapshot integrity is invalid")
        for label, value in (
            ("heavy HA runtime invocations", self.heavy_ha_runtime_invocations),
            ("worker CPU threads", self.worker_cpu_threads),
            ("worker nice", self.worker_nice),
            ("reserved physical cores", self.reserved_physical_cores),
            ("thermal throttling events", self.thermal_throttling_events),
        ):
            _counter(value, label)
        _finite(
            self.disk_projected_days_remaining,
            "projected disk days",
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "disk_projected_days_remaining": self.disk_projected_days_remaining,
            "heavy_ha_runtime_invocations": self.heavy_ha_runtime_invocations,
            "queue_age_cadence_fraction": (
                self.queue_age_cadence_fraction.to_mapping()
            ),
            "reserved_physical_cores": self.reserved_physical_cores,
            "snapshot": self.snapshot.to_mapping(),
            "thermal_throttling_events": self.thermal_throttling_events,
            "worker_cpu_threads": self.worker_cpu_threads,
            "worker_nice": self.worker_nice,
        }


@dataclass(frozen=True, slots=True)
class OperationalSoakRun:
    phase: SoakPhase
    run_id: str
    started_at: float
    ended_at: float
    comparison_environment_sha256: str
    measurement_method_sha256: str
    run_artifact_sha256: str
    clock_integrity_violations: int
    ha_event_loop_lag_ms: CompleteMetricSeries
    z2m_ingress_state_latency_ms: CompleteMetricSeries
    z2m_ingress_state_latency_strata_ms: tuple[LatencyStratum, ...]
    z2m_sequence: SequenceIntegrity
    candidate_runtime: CandidateRuntimeFacts | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.phase, SoakPhase):
            raise TypeError("soak phase is invalid")
        _identifier(self.run_id, "run_id")
        started = _finite(
            self.started_at,
            "started_at",
            minimum=MIN_RUN_TIMESTAMP,
            maximum=MAX_RUN_TIMESTAMP,
        )
        ended = _finite(
            self.ended_at,
            "ended_at",
            minimum=MIN_RUN_TIMESTAMP,
            maximum=MAX_RUN_TIMESTAMP,
        )
        if ended <= started:
            raise OperationalSloEvidenceError("soak end must be after start")
        _sha(self.comparison_environment_sha256, "comparison environment")
        _sha(self.measurement_method_sha256, "measurement method")
        _sha(self.run_artifact_sha256, "run artifact")
        _counter(self.clock_integrity_violations, "clock integrity violations")
        if not isinstance(self.ha_event_loop_lag_ms, CompleteMetricSeries):
            raise TypeError("HA latency series is invalid")
        if self.ha_event_loop_lag_ms.unit != "ms":
            raise OperationalSloEvidenceError("HA latency unit is invalid")
        if len(self.ha_event_loop_lag_ms.values) < MIN_LATENCY_SERIES_SAMPLES:
            raise OperationalSloEvidenceError("HA latency support is insufficient")
        if not isinstance(self.z2m_ingress_state_latency_ms, CompleteMetricSeries):
            raise TypeError("Z2M latency series is invalid")
        if self.z2m_ingress_state_latency_ms.unit != "ms":
            raise OperationalSloEvidenceError("Z2M latency unit is invalid")
        if (
            len(self.z2m_ingress_state_latency_ms.values)
            < MIN_LATENCY_SERIES_SAMPLES
        ):
            raise OperationalSloEvidenceError("Z2M latency support is insufficient")
        if (
            not isinstance(self.z2m_ingress_state_latency_strata_ms, tuple)
            or not self.z2m_ingress_state_latency_strata_ms
            or any(
                not isinstance(item, LatencyStratum)
                for item in self.z2m_ingress_state_latency_strata_ms
            )
        ):
            raise OperationalSloEvidenceError(
                "Z2M latency strata are absent or invalid"
            )
        stratum_ids = tuple(
            item.stratum_id
            for item in self.z2m_ingress_state_latency_strata_ms
        )
        if stratum_ids != tuple(sorted(set(stratum_ids))):
            raise OperationalSloEvidenceError(
                "Z2M latency strata must be unique and sorted"
            )
        if any(
            len(item.series.values) < MIN_LATENCY_SERIES_SAMPLES
            for item in self.z2m_ingress_state_latency_strata_ms
        ):
            raise OperationalSloEvidenceError(
                "Z2M latency stratum support is insufficient"
            )
        if not isinstance(self.z2m_sequence, SequenceIntegrity):
            raise TypeError("Z2M sequence audit is invalid")
        if self.phase is SoakPhase.BASELINE and self.candidate_runtime is not None:
            raise OperationalSloEvidenceError(
                "integration-off baseline cannot contain worker runtime facts"
            )
        if self.phase is SoakPhase.CANDIDATE and not isinstance(
            self.candidate_runtime,
            CandidateRuntimeFacts,
        ):
            raise OperationalSloEvidenceError(
                "candidate soak requires worker runtime facts"
            )

    @property
    def duration_seconds(self) -> float:
        return float(self.ended_at) - float(self.started_at)

    @property
    def complete(self) -> bool:
        runtime_complete = (
            True
            if self.candidate_runtime is None
            else self.candidate_runtime.queue_age_cadence_fraction.complete
        )
        return (
            self.ha_event_loop_lag_ms.complete
            and self.z2m_ingress_state_latency_ms.complete
            and all(
                item.series.complete
                for item in self.z2m_ingress_state_latency_strata_ms
            )
            and runtime_complete
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "candidate_runtime": (
                None
                if self.candidate_runtime is None
                else self.candidate_runtime.to_mapping()
            ),
            "clock_integrity_violations": self.clock_integrity_violations,
            "comparison_environment_sha256": self.comparison_environment_sha256,
            "ended_at": self.ended_at,
            "ha_event_loop_lag_ms": self.ha_event_loop_lag_ms.to_mapping(),
            "measurement_method_sha256": self.measurement_method_sha256,
            "phase": self.phase.value,
            "run_artifact_sha256": self.run_artifact_sha256,
            "run_id": self.run_id,
            "started_at": self.started_at,
            "z2m_ingress_state_latency_ms": (
                self.z2m_ingress_state_latency_ms.to_mapping()
            ),
            "z2m_ingress_state_latency_strata_ms": [
                item.to_mapping()
                for item in self.z2m_ingress_state_latency_strata_ms
            ],
            "z2m_sequence": self.z2m_sequence.to_mapping(),
        }


@dataclass(frozen=True, slots=True)
class OperationalSloEvidenceBundle:
    baseline_run_id: str
    candidate_run_id: str
    evidence: tuple[MetricEvidence, ...]
    bundle_sha256: str
    contract: str = CONTRACT

    def __post_init__(self) -> None:
        _identifier(self.baseline_run_id, "baseline_run_id")
        _identifier(self.candidate_run_id, "candidate_run_id")
        _sha(self.bundle_sha256, "bundle_sha256")
        if self.contract != CONTRACT:
            raise OperationalSloEvidenceError("bundle contract drifted")
        if not isinstance(self.evidence, tuple) or not self.evidence:
            raise OperationalSloEvidenceError("bundle evidence is empty")
        identifiers = tuple(item.gate_id for item in self.evidence)
        if identifiers != tuple(sorted(set(identifiers))):
            raise OperationalSloEvidenceError(
                "bundle evidence must be unique and sorted"
            )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "baseline_run_id": self.baseline_run_id,
            "bundle_sha256": self.bundle_sha256,
            "candidate_run_id": self.candidate_run_id,
            "contract": self.contract,
            "evidence": [_metric_mapping(item) for item in self.evidence],
        }


def _metric_mapping(value: MetricEvidence) -> dict[str, Any]:
    return {
        "baseline_source_ids": list(value.baseline_source_ids),
        "baseline_value": value.baseline_value,
        "candidate_value": value.candidate_value,
        "event_count": value.event_count,
        "event_ids": list(value.event_ids),
        "external_label_ids": list(value.external_label_ids),
        "external_truth_available": value.external_truth_available,
        "gate_id": value.gate_id,
        "partition": None if value.partition is None else value.partition.value,
        "provenance_ids": list(value.provenance_ids),
    }


def _provenance(*runs: OperationalSoakRun) -> tuple[str, ...]:
    values: set[str] = {f"method:{runs[0].measurement_method_sha256}"}
    for run in runs:
        values.update((
            f"artifact:{run.run_artifact_sha256}",
            f"run:{run.run_id}",
            f"series:{run.ha_event_loop_lag_ms.artifact_sha256}",
            f"series:{run.z2m_ingress_state_latency_ms.artifact_sha256}",
        ))
        values.update(
            f"series:{item.series.artifact_sha256}"
            for item in run.z2m_ingress_state_latency_strata_ms
        )
        if run.candidate_runtime is not None:
            values.add(
                "series:"
                + run.candidate_runtime.queue_age_cadence_fraction.artifact_sha256
            )
    return tuple(sorted(values))


def build_operational_slo_evidence(
    baseline: OperationalSoakRun,
    candidate: OperationalSoakRun,
) -> OperationalSloEvidenceBundle:
    """Derive every operations release metric from two complete soak runs."""

    if not isinstance(baseline, OperationalSoakRun) or not isinstance(
        candidate,
        OperationalSoakRun,
    ):
        raise TypeError("baseline and candidate must be OperationalSoakRun")
    if baseline.phase is not SoakPhase.BASELINE:
        raise OperationalSloEvidenceError("baseline phase is not integration-off")
    if candidate.phase is not SoakPhase.CANDIDATE:
        raise OperationalSloEvidenceError("candidate phase is not worker-only")
    if baseline.run_id == candidate.run_id:
        raise OperationalSloEvidenceError("baseline and candidate run IDs collide")
    if (
        baseline.comparison_environment_sha256
        != candidate.comparison_environment_sha256
    ):
        raise OperationalSloEvidenceError("comparison environments differ")
    if baseline.measurement_method_sha256 != candidate.measurement_method_sha256:
        raise OperationalSloEvidenceError("measurement methods differ")
    baseline_strata = {
        item.stratum_id: item.series
        for item in baseline.z2m_ingress_state_latency_strata_ms
    }
    candidate_strata = {
        item.stratum_id: item.series
        for item in candidate.z2m_ingress_state_latency_strata_ms
    }
    if set(baseline_strata) != set(candidate_strata):
        raise OperationalSloEvidenceError("Z2M latency strata differ")
    if not baseline.complete or not candidate.complete:
        raise OperationalSloEvidenceError(
            "sample series is incomplete; release evidence stays indeterminate"
        )
    runtime = candidate.candidate_runtime
    assert runtime is not None
    common = _provenance(baseline, candidate)

    baseline_ha_p99 = baseline.ha_event_loop_lag_ms.quantile(0.99)
    candidate_ha_p99 = candidate.ha_event_loop_lag_ms.quantile(0.99)
    baseline_z2m_p95 = baseline.z2m_ingress_state_latency_ms.quantile(0.95)
    candidate_z2m_p95 = candidate.z2m_ingress_state_latency_ms.quantile(0.95)
    baseline_z2m_p99 = baseline.z2m_ingress_state_latency_ms.quantile(0.99)
    candidate_z2m_p99 = candidate.z2m_ingress_state_latency_ms.quantile(0.99)
    z2m_p95_regressions = [candidate_z2m_p95 - baseline_z2m_p95]
    z2m_p99_regressions = [candidate_z2m_p99 - baseline_z2m_p99]
    for stratum_id in sorted(baseline_strata):
        z2m_p95_regressions.append(
            candidate_strata[stratum_id].quantile(0.95)
            - baseline_strata[stratum_id].quantile(0.95)
        )
        z2m_p99_regressions.append(
            candidate_strata[stratum_id].quantile(0.99)
            - baseline_strata[stratum_id].quantile(0.99)
        )
    stalls = sum(
        float(value) > 100.0 for value in candidate.ha_event_loop_lag_ms.values
    )
    sequence = candidate.z2m_sequence
    snapshot = runtime.snapshot

    values = {
        "operations.baseline_soak_seconds": baseline.duration_seconds,
        "operations.clock_integrity_violations": float(
            baseline.clock_integrity_violations
            + candidate.clock_integrity_violations
        ),
        "operations.disk_projected_days_remaining": (
            runtime.disk_projected_days_remaining
        ),
        "operations.ha_event_loop_lag_p99_ms": candidate_ha_p99,
        "operations.ha_event_loop_lag_regression_p99_ms": (
            candidate_ha_p99 - baseline_ha_p99
        ),
        "operations.ha_event_loop_stalls_over_100ms": float(stalls),
        "operations.heavy_ha_runtime_invocations": float(
            runtime.heavy_ha_runtime_invocations
        ),
        "operations.reserved_physical_cores": float(
            runtime.reserved_physical_cores
        ),
        "operations.shadow_soak_seconds": candidate.duration_seconds,
        "operations.snapshot_authority_violations": float(
            snapshot.authority_violations
        ),
        "operations.snapshot_expired_current_claims": float(
            snapshot.expired_current_claims
        ),
        "operations.snapshot_publishes_without_qos1_puback": float(
            snapshot.publishes_without_qos1_puback
        ),
        "operations.snapshot_stale_after_missed_cadences": float(
            snapshot.stale_after_missed_cadences
        ),
        "operations.thermal_throttling_events": float(
            runtime.thermal_throttling_events
        ),
        "operations.worker_cpu_threads": float(runtime.worker_cpu_threads),
        "operations.worker_nice": float(runtime.worker_nice),
        "operations.worker_queue_age_p95_cadence_fraction": (
            runtime.queue_age_cadence_fraction.quantile(0.95)
        ),
        "operations.z2m_duplicate_or_out_of_order_messages": float(
            sequence.duplicate_messages + sequence.out_of_order_messages
        ),
        "operations.z2m_ingress_state_regression_p95_ms": (
            max(z2m_p95_regressions)
        ),
        "operations.z2m_ingress_state_regression_p99_ms": (
            max(z2m_p99_regressions)
        ),
        "operations.z2m_message_loss": float(sequence.missing_messages),
    }
    manifest_ids = {
        gate.gate_id
        for gate in roadmap_release_manifest().gates
        if gate.gate_id.startswith("operations.")
    }
    if set(values) != manifest_ids:
        missing = sorted(manifest_ids - set(values))
        extra = sorted(set(values) - manifest_ids)
        raise OperationalSloEvidenceError(
            f"operational release-gate coverage drifted: missing={missing}, extra={extra}"
        )
    evidence = tuple(
        MetricEvidence(
            gate_id=gate_id,
            candidate_value=float(values[gate_id]),
            provenance_ids=common,
        )
        for gate_id in sorted(values)
    )
    unsigned = {
        "baseline": baseline.to_mapping(),
        "candidate": candidate.to_mapping(),
        "contract": CONTRACT,
        "evidence": [_metric_mapping(item) for item in evidence],
    }
    digest = hashlib.sha256(_canonical(unsigned)).hexdigest()
    return OperationalSloEvidenceBundle(
        baseline_run_id=baseline.run_id,
        candidate_run_id=candidate.run_id,
        evidence=evidence,
        bundle_sha256=digest,
    )


def _json_object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise OperationalSloEvidenceError(
                f"operational run artifact contains duplicate key {key!r}"
            )
        result[key] = value
    return result


def _json_constant(value: str) -> None:
    raise OperationalSloEvidenceError(
        f"operational run artifact contains non-finite constant {value}"
    )


def _object(value: object, label: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise OperationalSloEvidenceError(f"{label} is not an object")
    return value


def _exact_keys(
    value: dict[str, Any],
    expected: set[str],
    label: str,
) -> None:
    if set(value) != expected:
        missing = sorted(expected - set(value))
        extra = sorted(set(value) - expected)
        raise OperationalSloEvidenceError(
            f"{label} schema drifted: missing={missing}, extra={extra}"
        )


def _series_from_artifact(value: object, label: str) -> CompleteMetricSeries:
    document = _object(value, label)
    _exact_keys(document, {
        "expected_sample_count",
        "missing_sample_count",
        "quantile_method",
        "series_id",
        "unit",
        "values",
    }, label)
    raw_values = document["values"]
    if type(raw_values) is not list:
        raise OperationalSloEvidenceError(f"{label} values are not an array")
    digest = hashlib.sha256(_canonical(document)).hexdigest()
    return CompleteMetricSeries(
        series_id=document["series_id"],
        artifact_sha256=digest,
        unit=document["unit"],
        values=tuple(raw_values),
        expected_sample_count=document["expected_sample_count"],
        missing_sample_count=document["missing_sample_count"],
        quantile_method=document["quantile_method"],
    )


def _sequence_from_artifact(value: object) -> SequenceIntegrity:
    document = _object(value, "z2m_sequence")
    _exact_keys(document, {
        "expected_messages",
        "observed_messages",
        "out_of_order_messages",
        "unique_messages",
    }, "z2m_sequence")
    return SequenceIntegrity(
        expected_messages=document["expected_messages"],
        observed_messages=document["observed_messages"],
        unique_messages=document["unique_messages"],
        out_of_order_messages=document["out_of_order_messages"],
    )


def _snapshot_from_artifact(value: object) -> SnapshotIntegrity:
    document = _object(value, "candidate_runtime.snapshot")
    _exact_keys(document, {
        "authority_violations",
        "expired_current_claims",
        "publishes_without_qos1_puback",
        "stale_after_missed_cadences",
    }, "candidate_runtime.snapshot")
    return SnapshotIntegrity(
        authority_violations=document["authority_violations"],
        expired_current_claims=document["expired_current_claims"],
        publishes_without_qos1_puback=(
            document["publishes_without_qos1_puback"]
        ),
        stale_after_missed_cadences=(
            document["stale_after_missed_cadences"]
        ),
    )


def _runtime_from_artifact(value: object) -> CandidateRuntimeFacts | None:
    if value is None:
        return None
    document = _object(value, "candidate_runtime")
    _exact_keys(document, {
        "disk_projected_days_remaining",
        "heavy_ha_runtime_invocations",
        "queue_age_cadence_fraction",
        "reserved_physical_cores",
        "snapshot",
        "thermal_throttling_events",
        "worker_cpu_threads",
        "worker_nice",
    }, "candidate_runtime")
    return CandidateRuntimeFacts(
        queue_age_cadence_fraction=_series_from_artifact(
            document["queue_age_cadence_fraction"],
            "candidate_runtime.queue_age_cadence_fraction",
        ),
        heavy_ha_runtime_invocations=document["heavy_ha_runtime_invocations"],
        snapshot=_snapshot_from_artifact(document["snapshot"]),
        disk_projected_days_remaining=document["disk_projected_days_remaining"],
        worker_cpu_threads=document["worker_cpu_threads"],
        worker_nice=document["worker_nice"],
        reserved_physical_cores=document["reserved_physical_cores"],
        thermal_throttling_events=document["thermal_throttling_events"],
    )


def _strata_from_artifact(value: object) -> tuple[LatencyStratum, ...]:
    if type(value) is not list or not value:
        raise OperationalSloEvidenceError(
            "z2m_ingress_state_latency_strata_ms is not a non-empty array"
        )
    result: list[LatencyStratum] = []
    for index, raw in enumerate(value):
        label = f"z2m_ingress_state_latency_strata_ms[{index}]"
        document = _object(raw, label)
        _exact_keys(document, {"series", "stratum_id"}, label)
        result.append(LatencyStratum(
            stratum_id=document["stratum_id"],
            series=_series_from_artifact(document["series"], label + ".series"),
        ))
    return tuple(result)


def parse_operational_soak_run_artifact(payload: bytes) -> OperationalSoakRun:
    """Parse one closed run artifact and derive every content digest locally.

    The input deliberately contains no caller-supplied run or series hash.
    Unknown fields are rejected, so credentials, coordinates and unreviewed
    counters cannot silently enter release provenance.
    """

    if not isinstance(payload, bytes):
        raise TypeError("operational run artifact must be bytes")
    if not payload or len(payload) > MAX_RUN_ARTIFACT_BYTES:
        raise OperationalSloEvidenceError(
            "operational run artifact is empty or exceeds its byte bound"
        )
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise OperationalSloEvidenceError(
            "operational run artifact is not UTF-8"
        ) from exc
    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_json_object_pairs,
            parse_constant=_json_constant,
        )
    except OperationalSloEvidenceError:
        raise
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise OperationalSloEvidenceError(
            "operational run artifact is not valid JSON"
        ) from exc
    document = _object(decoded, "operational run artifact")
    _exact_keys(document, {
        "candidate_runtime",
        "clock_integrity_violations",
        "comparison_environment_sha256",
        "contract",
        "ended_at",
        "ha_event_loop_lag_ms",
        "measurement_method_sha256",
        "phase",
        "run_id",
        "started_at",
        "z2m_ingress_state_latency_ms",
        "z2m_ingress_state_latency_strata_ms",
        "z2m_sequence",
    }, "operational run artifact")
    if document["contract"] != RUN_ARTIFACT_CONTRACT:
        raise OperationalSloEvidenceError("operational run contract drifted")
    try:
        phase = SoakPhase(document["phase"])
    except (TypeError, ValueError) as exc:
        raise OperationalSloEvidenceError(
            "operational run phase is invalid"
        ) from exc
    run_digest = hashlib.sha256(_canonical(document)).hexdigest()
    return OperationalSoakRun(
        phase=phase,
        run_id=document["run_id"],
        started_at=document["started_at"],
        ended_at=document["ended_at"],
        comparison_environment_sha256=(
            document["comparison_environment_sha256"]
        ),
        measurement_method_sha256=document["measurement_method_sha256"],
        run_artifact_sha256=run_digest,
        clock_integrity_violations=document["clock_integrity_violations"],
        ha_event_loop_lag_ms=_series_from_artifact(
            document["ha_event_loop_lag_ms"],
            "ha_event_loop_lag_ms",
        ),
        z2m_ingress_state_latency_ms=_series_from_artifact(
            document["z2m_ingress_state_latency_ms"],
            "z2m_ingress_state_latency_ms",
        ),
        z2m_ingress_state_latency_strata_ms=_strata_from_artifact(
            document["z2m_ingress_state_latency_strata_ms"]
        ),
        z2m_sequence=_sequence_from_artifact(document["z2m_sequence"]),
        candidate_runtime=_runtime_from_artifact(document["candidate_runtime"]),
    )


__all__ = [
    "CONTRACT",
    "RUN_ARTIFACT_CONTRACT",
    "MAX_RUN_ARTIFACT_BYTES",
    "MIN_LATENCY_SERIES_SAMPLES",
    "MIN_RUN_TIMESTAMP",
    "MAX_RUN_TIMESTAMP",
    "MAX_SERIES_SAMPLES",
    "QUANTILE_METHOD",
    "CandidateRuntimeFacts",
    "CompleteMetricSeries",
    "LatencyStratum",
    "OperationalSloEvidenceBundle",
    "OperationalSloEvidenceError",
    "OperationalSoakRun",
    "SequenceIntegrity",
    "SnapshotIntegrity",
    "SoakPhase",
    "build_operational_slo_evidence",
    "parse_operational_soak_run_artifact",
]
