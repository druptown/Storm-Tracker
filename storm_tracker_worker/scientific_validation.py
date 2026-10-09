"""Fail-closed scientific release gates for the shadow meteorological engine.

This module evaluates *releases*, never weather observations.  A failed or
unknown gate therefore cannot discard a detection, association candidate or
track.  Missing truth, baseline evidence or event support stays explicitly
INDETERMINATE instead of being converted into a pass.

The roadmap manifest is intentionally a draft until the still-unset practical
split/merge margins are preregistered against an event-held-out corpus.  Its
stable hash makes later threshold changes visible.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
import hashlib
import json
import math
import re
from typing import Iterable, Mapping

from .validation_corpus import (
    CorpusPartition,
    ExternalLabelKind,
    ExternalTruthContext,
    LabelState,
)


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class Comparator(str, Enum):
    """Supported preregistered metric comparisons."""

    EQUAL = "equal"
    MAXIMUM = "maximum"
    MINIMUM = "minimum"
    LOWER_THAN_BASELINE = "lower_than_baseline"
    NOT_GREATER_THAN_BASELINE = "not_greater_than_baseline"
    NOT_LESS_THAN_BASELINE = "not_less_than_baseline"
    GREATER_THAN_BASELINE = "greater_than_baseline"
    POSITIVE = "positive"


class GateState(str, Enum):
    """One scientific gate outcome."""

    PASS = "pass"
    FAIL = "fail"
    INDETERMINATE = "indeterminate"


class PreregistrationSourceRole(str, Enum):
    """Why an immutable source is frozen into the preregistration."""

    EXTERNAL_LABEL = "external_label"
    BASELINE = "baseline"
    MARGIN_RATIONALE = "margin_rationale"


class PracticalMarginMode(str, Enum):
    """Unambiguous interpretation of one preregistered practical margin."""

    ABSOLUTE_THRESHOLD = "absolute_threshold"
    MINIMUM_IMPROVEMENT = "minimum_improvement"
    NONINFERIORITY_TOLERANCE = "noninferiority_tolerance"


@dataclass(frozen=True, slots=True)
class MetricGate:
    """A metric definition frozen before release-test evaluation."""

    gate_id: str
    category: str
    comparator: Comparator
    description: str
    threshold: float | None = None
    required: bool = True
    requires_external_truth: bool = False
    minimum_events: int = 0
    requires_practical_margin: bool = False
    requires_baseline_source: bool = False

    def __post_init__(self) -> None:
        _identifier(self.gate_id, "gate_id")
        _identifier(self.category, "category")
        if not isinstance(self.comparator, Comparator):
            raise TypeError("comparator vereist Comparator")
        if not isinstance(self.description, str) or not self.description.strip():
            raise ValueError("description mag niet leeg zijn")
        for field, value in (
            ("required", self.required),
            ("requires_external_truth", self.requires_external_truth),
            ("requires_practical_margin", self.requires_practical_margin),
            ("requires_baseline_source", self.requires_baseline_source),
        ):
            if not isinstance(value, bool):
                raise TypeError(f"{field} moet bool zijn")
        if self.threshold is not None and not math.isfinite(self.threshold):
            raise ValueError("threshold moet eindig of None zijn")
        if (
            isinstance(self.minimum_events, bool)
            or not isinstance(self.minimum_events, int)
            or self.minimum_events < 0
        ):
            raise ValueError("minimum_events moet een niet-negatief geheel getal zijn")
        if self.comparator in {
            Comparator.EQUAL,
            Comparator.MAXIMUM,
            Comparator.MINIMUM,
        } and self.threshold is None:
            # Draft manifests may carry intentionally unset practical margins.
            return
        if self.comparator in {
            Comparator.LOWER_THAN_BASELINE,
            Comparator.NOT_GREATER_THAN_BASELINE,
            Comparator.NOT_LESS_THAN_BASELINE,
            Comparator.GREATER_THAN_BASELINE,
            Comparator.POSITIVE,
        } and self.threshold is not None:
            raise ValueError(
                f"{self.comparator.value} gebruikt geen absolute threshold"
            )

    @property
    def threshold_is_resolved(self) -> bool:
        return not (
            self.comparator
            in {Comparator.EQUAL, Comparator.MAXIMUM, Comparator.MINIMUM}
            and self.threshold is None
        )

    def to_dict(self) -> dict:
        return {
            "gate_id": self.gate_id,
            "category": self.category,
            "comparator": self.comparator.value,
            "description": self.description,
            "threshold": self.threshold,
            "required": self.required,
            "requires_external_truth": self.requires_external_truth,
            "minimum_events": self.minimum_events,
            "requires_practical_margin": self.requires_practical_margin,
            "requires_baseline_source": self.requires_baseline_source,
        }


@dataclass(frozen=True, slots=True)
class PreregistrationSource:
    """Immutable source identity frozen before held-out evaluation."""

    source_id: str
    role: PreregistrationSourceRole
    artifact_sha256: str
    description: str
    candidate_independent: bool

    def __post_init__(self) -> None:
        _identifier(self.source_id, "source_id")
        if not isinstance(self.role, PreregistrationSourceRole):
            raise TypeError("role vereist PreregistrationSourceRole")
        _sha256(self.artifact_sha256, "artifact_sha256")
        _identifier(self.description, "description")
        if not isinstance(self.candidate_independent, bool):
            raise TypeError("candidate_independent moet bool zijn")
        if (
            self.role
            in {
                PreregistrationSourceRole.EXTERNAL_LABEL,
                PreregistrationSourceRole.BASELINE,
            }
            and not self.candidate_independent
        ):
            raise ValueError(
                "label/baseline source moet onafhankelijk van de kandidaat zijn"
            )

    def to_dict(self) -> dict:
        return {
            "source_id": self.source_id,
            "role": self.role.value,
            "artifact_sha256": self.artifact_sha256,
            "description": self.description,
            "candidate_independent": self.candidate_independent,
        }


@dataclass(frozen=True, slots=True)
class GateEvidenceContract:
    """External labels and baselines permitted for one scientific gate."""

    gate_id: str
    required_label_kinds: tuple[ExternalLabelKind, ...]
    external_label_source_ids: tuple[str, ...]
    baseline_source_ids: tuple[str, ...] = ()
    minimum_distinct_label_sources: int = 1
    partition: CorpusPartition = CorpusPartition.TEST

    def __post_init__(self) -> None:
        _identifier(self.gate_id, "gate_id")
        if not isinstance(self.required_label_kinds, tuple):
            raise TypeError("required_label_kinds moet tuple zijn")
        if any(
            not isinstance(item, ExternalLabelKind)
            for item in self.required_label_kinds
        ):
            raise TypeError(
                "required_label_kinds bevat geen ExternalLabelKind"
            )
        if (
            len(self.required_label_kinds)
            != len(set(self.required_label_kinds))
        ):
            raise ValueError("required_label_kinds bevat duplicaten")
        if self.required_label_kinds != tuple(
            sorted(self.required_label_kinds, key=lambda item: item.value)
        ):
            raise ValueError("required_label_kinds moet gesorteerd zijn")
        _sorted_unique_identifiers(
            self.external_label_source_ids,
            "external_label_source_ids",
            allow_empty=True,
        )
        _sorted_unique_identifiers(
            self.baseline_source_ids,
            "baseline_source_ids",
            allow_empty=True,
        )
        if (
            isinstance(self.minimum_distinct_label_sources, bool)
            or not isinstance(self.minimum_distinct_label_sources, int)
            or self.minimum_distinct_label_sources <= 0
        ):
            raise ValueError(
                "minimum_distinct_label_sources moet positief geheel zijn"
            )
        if not isinstance(self.partition, CorpusPartition):
            raise TypeError("partition vereist CorpusPartition")

    def to_dict(self) -> dict:
        return {
            "gate_id": self.gate_id,
            "required_label_kinds": [
                kind.value for kind in self.required_label_kinds
            ],
            "external_label_source_ids": list(
                self.external_label_source_ids
            ),
            "baseline_source_ids": list(self.baseline_source_ids),
            "minimum_distinct_label_sources": (
                self.minimum_distinct_label_sources
            ),
            "partition": self.partition.value,
        }


@dataclass(frozen=True, slots=True)
class PracticalMargin:
    """A release-relevant margin plus its pre-test rationale provenance."""

    gate_id: str
    mode: PracticalMarginMode
    value: float | None
    unit: str | None
    rationale: str | None
    rationale_source_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.gate_id, "gate_id")
        if not isinstance(self.mode, PracticalMarginMode):
            raise TypeError("mode vereist PracticalMarginMode")
        if self.value is not None:
            value = _finite(self.value, "margin value")
            if value < 0.0:
                raise ValueError("praktische marge moet niet-negatief zijn")
        if self.unit is not None:
            _identifier(self.unit, "margin unit")
        if self.rationale is not None:
            _identifier(self.rationale, "margin rationale")
        _sorted_unique_identifiers(
            self.rationale_source_ids,
            "rationale_source_ids",
            allow_empty=True,
        )

    @property
    def is_resolved(self) -> bool:
        return (
            self.value is not None
            and isinstance(self.unit, str)
            and bool(self.unit.strip())
            and isinstance(self.rationale, str)
            and bool(self.rationale.strip())
            and bool(self.rationale_source_ids)
        )

    def to_dict(self) -> dict:
        return {
            "gate_id": self.gate_id,
            "mode": self.mode.value,
            "value": self.value,
            "unit": self.unit,
            "rationale": self.rationale,
            "rationale_source_ids": list(self.rationale_source_ids),
        }


@dataclass(frozen=True, slots=True)
class MetricEvidence:
    """Measured evidence supplied to one preregistered gate."""

    gate_id: str
    candidate_value: float
    baseline_value: float | None = None
    event_count: int = 0
    external_truth_available: bool = False
    provenance_ids: tuple[str, ...] = ()
    event_ids: tuple[str, ...] = ()
    external_label_ids: tuple[str, ...] = ()
    baseline_source_ids: tuple[str, ...] = ()
    partition: CorpusPartition | None = None

    def __post_init__(self) -> None:
        _identifier(self.gate_id, "gate_id")
        _finite(self.candidate_value, "candidate_value")
        if self.baseline_value is not None:
            _finite(self.baseline_value, "baseline_value")
        if (
            isinstance(self.event_count, bool)
            or not isinstance(self.event_count, int)
            or self.event_count < 0
        ):
            raise ValueError("event_count moet een niet-negatief geheel getal zijn")
        if not isinstance(self.external_truth_available, bool):
            raise TypeError("external_truth_available moet bool zijn")
        _sorted_unique_identifiers(
            self.provenance_ids,
            "provenance_ids",
            allow_empty=True,
        )
        _sorted_unique_identifiers(
            self.event_ids,
            "event_ids",
            allow_empty=True,
        )
        _sorted_unique_identifiers(
            self.external_label_ids,
            "external_label_ids",
            allow_empty=True,
        )
        _sorted_unique_identifiers(
            self.baseline_source_ids,
            "baseline_source_ids",
            allow_empty=True,
        )
        if self.partition is not None and not isinstance(
            self.partition,
            CorpusPartition,
        ):
            raise TypeError("partition vereist CorpusPartition of None")


@dataclass(frozen=True, slots=True)
class ValidationManifest:
    """Immutable, hashable preregistration manifest."""

    manifest_version: str
    corpus_id: str
    corpus_split_policy: str
    gates: tuple[MetricGate, ...]
    locked: bool = False
    lock_note: str | None = None
    sources: tuple[PreregistrationSource, ...] = ()
    evidence_contracts: tuple[GateEvidenceContract, ...] = ()
    practical_margins: tuple[PracticalMargin, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.manifest_version, "manifest_version")
        _identifier(self.corpus_id, "corpus_id")
        _identifier(self.corpus_split_policy, "corpus_split_policy")
        if not isinstance(self.gates, tuple) or not self.gates:
            raise ValueError("gates moet een niet-lege tuple zijn")
        ids = [gate.gate_id for gate in self.gates]
        if len(ids) != len(set(ids)):
            raise ValueError("gate_id moet uniek zijn")
        if not isinstance(self.sources, tuple):
            raise TypeError("sources moet tuple zijn")
        if not isinstance(self.evidence_contracts, tuple):
            raise TypeError("evidence_contracts moet tuple zijn")
        if not isinstance(self.practical_margins, tuple):
            raise TypeError("practical_margins moet tuple zijn")
        _unique_attribute(self.sources, "source_id")
        _unique_attribute(self.evidence_contracts, "gate_id")
        _unique_attribute(self.practical_margins, "gate_id")
        known_gates = set(ids)
        gate_by_id = {gate.gate_id: gate for gate in self.gates}
        for contract in self.evidence_contracts:
            if contract.gate_id not in known_gates:
                raise ValueError(
                    f"evidencecontract voor onbekende gate: {contract.gate_id}"
                )
            if not gate_by_id[contract.gate_id].requires_external_truth:
                raise ValueError(
                    (
                        "extern evidencecontract hoort bij gate zonder "
                        f"requires_external_truth: {contract.gate_id}"
                    )
                )
        for margin in self.practical_margins:
            if margin.gate_id not in known_gates:
                raise ValueError(
                    f"praktische marge voor onbekende gate: {margin.gate_id}"
                )
            if not gate_by_id[margin.gate_id].requires_practical_margin:
                raise ValueError(
                    (
                        "praktische marge hoort bij gate zonder "
                        f"requires_practical_margin: {margin.gate_id}"
                    )
                )
        if self.locked and (
            not isinstance(self.lock_note, str) or not self.lock_note.strip()
        ):
            raise ValueError("een vergrendeld manifest vereist lock_note")
        blockers = self.lock_blockers
        if self.locked and blockers:
            raise ValueError(
                "manifest kan niet vergrendelen: " + "; ".join(blockers)
            )

    def to_dict(self) -> dict:
        return {
            "manifest_version": self.manifest_version,
            "corpus_id": self.corpus_id,
            "corpus_split_policy": self.corpus_split_policy,
            "locked": self.locked,
            "lock_note": self.lock_note,
            "sources": [
                source.to_dict()
                for source in sorted(
                    self.sources,
                    key=lambda item: item.source_id,
                )
            ],
            "evidence_contracts": [
                contract.to_dict()
                for contract in sorted(
                    self.evidence_contracts,
                    key=lambda item: item.gate_id,
                )
            ],
            "practical_margins": [
                margin.to_dict()
                for margin in sorted(
                    self.practical_margins,
                    key=lambda item: item.gate_id,
                )
            ],
            "gates": [
                gate.to_dict()
                for gate in sorted(self.gates, key=lambda item: item.gate_id)
            ],
        }

    @property
    def sha256(self) -> str:
        encoded = json.dumps(
            self.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        return hashlib.sha256(encoded).hexdigest()

    def lock(self, *, lock_note: str) -> "ValidationManifest":
        """Freeze a fully specified manifest before examining release data."""

        if self.locked:
            if lock_note != self.lock_note:
                raise ValueError("vergrendeld manifest kan niet worden gewijzigd")
            return self
        return replace(self, locked=True, lock_note=lock_note)

    @property
    def lock_blockers(self) -> tuple[str, ...]:
        """List every unresolved preregistration requirement deterministically."""

        blockers: list[str] = []
        if self.corpus_id.startswith("UNSET"):
            blockers.append("event-held-out corpus_id is niet vastgelegd")
        if self.corpus_split_policy.startswith("UNSET"):
            blockers.append("event-held-out splitpolicy is niet vastgelegd")

        source_by_id = {source.source_id: source for source in self.sources}
        contract_by_gate = {
            contract.gate_id: contract
            for contract in self.evidence_contracts
        }
        margin_by_gate = {
            margin.gate_id: margin
            for margin in self.practical_margins
        }
        baseline_comparators = {
            Comparator.LOWER_THAN_BASELINE,
            Comparator.NOT_GREATER_THAN_BASELINE,
            Comparator.NOT_LESS_THAN_BASELINE,
            Comparator.GREATER_THAN_BASELINE,
        }
        for gate in sorted(self.gates, key=lambda item: item.gate_id):
            if gate.required and not gate.threshold_is_resolved:
                blockers.append(
                    f"{gate.gate_id}: onopgeloste vereiste threshold"
                )
            if gate.requires_practical_margin:
                margin = margin_by_gate.get(gate.gate_id)
                if margin is None or not margin.is_resolved:
                    blockers.append(
                        f"{gate.gate_id}: onopgeloste praktische marge/rationale"
                    )
                else:
                    blockers.extend(
                        _validate_margin_contract(
                            gate,
                            margin,
                            source_by_id,
                        )
                    )
            if not gate.requires_external_truth:
                continue
            contract = contract_by_gate.get(gate.gate_id)
            if contract is None:
                blockers.append(
                    f"{gate.gate_id}: extern evidencecontract ontbreekt"
                )
                continue
            if not contract.required_label_kinds:
                blockers.append(
                    f"{gate.gate_id}: vereiste externe labeltypes ontbreken"
                )
            if not contract.external_label_source_ids:
                blockers.append(
                    f"{gate.gate_id}: externe labelbronnen ontbreken"
                )
            elif (
                len(contract.external_label_source_ids)
                < contract.minimum_distinct_label_sources
            ):
                blockers.append(
                    (
                        f"{gate.gate_id}: minder preregistreerde labelbronnen "
                        "dan minimum_distinct_label_sources"
                    )
                )
            for source_id in contract.external_label_source_ids:
                source = source_by_id.get(source_id)
                if source is None:
                    blockers.append(
                        f"{gate.gate_id}: onbekende labelbron {source_id}"
                    )
                elif source.role is not PreregistrationSourceRole.EXTERNAL_LABEL:
                    blockers.append(
                        f"{gate.gate_id}: {source_id} is geen external-label source"
                    )
            if (
                gate.requires_baseline_source
                or gate.comparator in baseline_comparators
            ):
                if not contract.baseline_source_ids:
                    blockers.append(
                        f"{gate.gate_id}: baselinebronnen ontbreken"
                    )
                for source_id in contract.baseline_source_ids:
                    source = source_by_id.get(source_id)
                    if source is None:
                        blockers.append(
                            f"{gate.gate_id}: onbekende baselinebron {source_id}"
                        )
                    elif source.role is not PreregistrationSourceRole.BASELINE:
                        blockers.append(
                            f"{gate.gate_id}: {source_id} is geen baseline source"
                        )
        return tuple(blockers)


@dataclass(frozen=True, slots=True)
class GateResult:
    gate_id: str
    state: GateState
    reason: str
    candidate_value: float | None
    baseline_value: float | None
    event_count: int
    provenance_ids: tuple[str, ...]
    event_ids: tuple[str, ...]
    external_label_ids: tuple[str, ...]
    partition: CorpusPartition | None

    def to_dict(self) -> dict:
        return {
            "gate_id": self.gate_id,
            "state": self.state.value,
            "reason": self.reason,
            "candidate_value": self.candidate_value,
            "baseline_value": self.baseline_value,
            "event_count": self.event_count,
            "provenance_ids": list(self.provenance_ids),
            "event_ids": list(self.event_ids),
            "external_label_ids": list(self.external_label_ids),
            "partition": (
                self.partition.value if self.partition is not None else None
            ),
        }


@dataclass(frozen=True, slots=True)
class ValidationReport:
    manifest_sha256: str
    manifest_locked: bool
    results: tuple[GateResult, ...]
    release_ready: bool
    blocking_reasons: tuple[str, ...]

    def to_dict(self) -> dict:
        return {
            "manifest_sha256": self.manifest_sha256,
            "manifest_locked": self.manifest_locked,
            "release_ready": self.release_ready,
            "blocking_reasons": list(self.blocking_reasons),
            "results": [result.to_dict() for result in self.results],
        }


def evaluate_manifest(
    manifest: ValidationManifest,
    evidence: Iterable[MetricEvidence],
    *,
    external_truth: ExternalTruthContext | None = None,
) -> ValidationReport:
    """Evaluate without treating missing science or labels as success.

    Held-out metrics are only evaluated after the manifest is locked.  Merely
    setting ``external_truth_available=True`` is insufficient: every referenced
    label, event partition, source digest and baseline source must satisfy the
    preregistered contracts.
    """

    supplied: dict[str, MetricEvidence] = {}
    known = {gate.gate_id for gate in manifest.gates}
    for item in evidence:
        if item.gate_id not in known:
            raise ValueError(f"evidence voor onbekende gate: {item.gate_id}")
        if item.gate_id in supplied:
            raise ValueError(f"dubbele evidence voor gate: {item.gate_id}")
        supplied[item.gate_id] = item

    margins = {
        margin.gate_id: margin
        for margin in manifest.practical_margins
    }
    results: list[GateResult] = []
    for gate in sorted(manifest.gates, key=lambda item: item.gate_id):
        item = supplied.get(gate.gate_id)
        if item is None:
            results.append(_indeterminate(gate, "evidence ontbreekt"))
            continue
        if not gate.threshold_is_resolved:
            results.append(_indeterminate(
                gate,
                "praktische vrijgavemarge is nog niet vooraf vastgelegd",
                item,
            ))
            continue
        margin = margins.get(gate.gate_id)
        if gate.requires_practical_margin and (
            margin is None or not margin.is_resolved
        ):
            results.append(_indeterminate(
                gate,
                "praktische marge, bron of rationale is niet vooraf vastgelegd",
                item,
            ))
            continue
        if gate.requires_external_truth:
            if not item.external_truth_available:
                results.append(_indeterminate(
                    gate,
                    "onafhankelijke ground truth ontbreekt",
                    item,
                ))
                continue
            if not manifest.locked:
                results.append(_indeterminate(
                    gate,
                    (
                        "held-out labels worden niet geëvalueerd vóór "
                        "manifest-lock"
                    ),
                    item,
                ))
                continue
            if external_truth is None:
                results.append(_indeterminate(
                    gate,
                    "gevalideerd extern labelcorpus ontbreekt",
                    item,
                ))
                continue
            truth_blocker = _external_truth_blocker(
                manifest,
                gate,
                item,
                external_truth,
            )
            if truth_blocker is not None:
                results.append(_indeterminate(gate, truth_blocker, item))
                continue
        if item.event_count < gate.minimum_events:
            results.append(_indeterminate(
                gate,
                (
                    f"eventsteun {item.event_count} is kleiner dan "
                    f"minimum {gate.minimum_events}"
                ),
                item,
            ))
            continue
        result = _compare(gate, item, margin)
        results.append(result)

    blocking: list[str] = []
    if not manifest.locked:
        blocking.append("preregistratiemanifest is niet vergrendeld")
    for gate, result in zip(
        sorted(manifest.gates, key=lambda item: item.gate_id),
        results,
        strict=True,
    ):
        if gate.required and result.state is not GateState.PASS:
            blocking.append(f"{gate.gate_id}:{result.state.value}")
    return ValidationReport(
        manifest_sha256=manifest.sha256,
        manifest_locked=manifest.locked,
        results=tuple(results),
        release_ready=not blocking,
        blocking_reasons=tuple(blocking),
    )


def roadmap_release_manifest() -> ValidationManifest:
    """Return the roadmap gates before unresolved margins are locked."""

    gates = (
        _absolute_zero("p0.raw_artifact_loss", "storage"),
        _absolute_zero("p0.observation_loss", "storage"),
        _absolute_zero("p0.batch_loss", "processing"),
        _absolute_zero("p0.unterminated_jobs", "processing"),
        _absolute_zero("p0.silently_dropped_detections", "tracking"),
        _absolute_zero("p0.cross_storm_ground_confirmations", "ground"),
        _absolute_zero("p0.sensor_read_side_effects", "home_assistant"),
        MetricGate(
            "p0.polygon_contract_fraction",
            "geometry",
            Comparator.EQUAL,
            "Golden polygon inside/edge contract fraction.",
            threshold=1.0,
        ),
        MetricGate(
            "tracking.id_switches",
            "tracking",
            Comparator.LOWER_THAN_BASELINE,
            "Event-held-out identity switches versus legacy.",
            requires_external_truth=True,
            minimum_events=1,
            requires_practical_margin=True,
            requires_baseline_source=True,
        ),
        MetricGate(
            "tracking.fragmentation",
            "tracking",
            Comparator.LOWER_THAN_BASELINE,
            "Event-held-out track fragmentation versus legacy.",
            requires_external_truth=True,
            minimum_events=1,
            requires_practical_margin=True,
            requires_baseline_source=True,
        ),
        MetricGate(
            "tracking.completeness",
            "tracking",
            Comparator.NOT_LESS_THAN_BASELINE,
            "Event-held-out track completeness versus legacy.",
            requires_external_truth=True,
            minimum_events=1,
            requires_practical_margin=True,
            requires_baseline_source=True,
        ),
        MetricGate(
            "tracking.position_error",
            "tracking",
            Comparator.NOT_GREATER_THAN_BASELINE,
            "Event-held-out position error versus legacy.",
            requires_external_truth=True,
            minimum_events=1,
            requires_practical_margin=True,
            requires_baseline_source=True,
        ),
        MetricGate(
            "tracking.speed_error",
            "tracking",
            Comparator.NOT_GREATER_THAN_BASELINE,
            "Event-held-out speed error versus legacy; never a speed gate.",
            requires_external_truth=True,
            minimum_events=1,
            requires_practical_margin=True,
            requires_baseline_source=True,
        ),
        MetricGate(
            "tracking.split_precision",
            "tracking",
            Comparator.MINIMUM,
            "Preregistered split precision.",
            threshold=None,
            requires_external_truth=True,
            minimum_events=1,
            requires_practical_margin=True,
        ),
        MetricGate(
            "tracking.split_recall",
            "tracking",
            Comparator.MINIMUM,
            "Preregistered split recall.",
            threshold=None,
            requires_external_truth=True,
            minimum_events=1,
            requires_practical_margin=True,
        ),
        MetricGate(
            "tracking.merge_precision",
            "tracking",
            Comparator.MINIMUM,
            "Preregistered merge precision.",
            threshold=None,
            requires_external_truth=True,
            minimum_events=1,
            requires_practical_margin=True,
        ),
        MetricGate(
            "tracking.merge_recall",
            "tracking",
            Comparator.MINIMUM,
            "Preregistered merge recall.",
            threshold=None,
            requires_external_truth=True,
            minimum_events=1,
            requires_practical_margin=True,
        ),
        MetricGate(
            "nowcast.passage_brier_skill",
            "nowcast",
            Comparator.POSITIVE,
            "Passage Brier Skill Score versus preregistered baseline.",
            requires_external_truth=True,
            minimum_events=1,
            requires_practical_margin=True,
            requires_baseline_source=True,
        ),
        MetricGate(
            "nowcast.fractions_skill",
            "nowcast",
            Comparator.NOT_LESS_THAN_BASELINE,
            "Fractions Skill Score at preregistered lead/scale strata.",
            requires_external_truth=True,
            minimum_events=1,
            requires_practical_margin=True,
            requires_baseline_source=True,
        ),
        MetricGate(
            "operations.shadow_soak_seconds",
            "operations",
            Comparator.MINIMUM,
            "Lossless continuous live shadow-soak duration.",
            threshold=7.0 * 24.0 * 60.0 * 60.0,
        ),
        MetricGate(
            "operations.baseline_soak_seconds",
            "operations",
            Comparator.MINIMUM,
            "Lossless integration-off baseline duration used for comparison.",
            threshold=7.0 * 24.0 * 60.0 * 60.0,
        ),
        _absolute_zero(
            "operations.clock_integrity_violations",
            "operations",
        ),
        MetricGate(
            "operations.ha_event_loop_lag_p99_ms",
            "home_assistant",
            Comparator.MAXIMUM,
            "HA event-loop lag p99 during the representative shadow soak.",
            threshold=20.0,
        ),
        MetricGate(
            "operations.ha_event_loop_lag_regression_p99_ms",
            "home_assistant",
            Comparator.MAXIMUM,
            "HA event-loop lag p99 regression versus integration-off baseline.",
            threshold=10.0,
        ),
        _absolute_zero(
            "operations.ha_event_loop_stalls_over_100ms",
            "home_assistant",
        ),
        _absolute_zero(
            "operations.heavy_ha_runtime_invocations",
            "home_assistant",
        ),
        MetricGate(
            "operations.z2m_ingress_state_regression_p95_ms",
            "zigbee2mqtt",
            Comparator.MAXIMUM,
            "Worst preregistered/global Z2M broker-ingress to HA-state p95 regression versus baseline.",
            threshold=50.0,
        ),
        MetricGate(
            "operations.z2m_ingress_state_regression_p99_ms",
            "zigbee2mqtt",
            Comparator.MAXIMUM,
            "Worst preregistered/global Z2M broker-ingress to HA-state p99 regression versus baseline.",
            threshold=100.0,
        ),
        _absolute_zero("operations.z2m_message_loss", "zigbee2mqtt"),
        _absolute_zero(
            "operations.z2m_duplicate_or_out_of_order_messages",
            "zigbee2mqtt",
        ),
        MetricGate(
            "operations.worker_queue_age_p95_cadence_fraction",
            "operations",
            Comparator.MAXIMUM,
            "Worker queue-age p95 as a fraction of active product cadence.",
            threshold=1.0,
        ),
        MetricGate(
            "operations.snapshot_stale_after_missed_cadences",
            "operations",
            Comparator.MAXIMUM,
            "Missed product cadences before the snapshot becomes stale.",
            threshold=2.0,
        ),
        _absolute_zero(
            "operations.snapshot_authority_violations",
            "operations",
        ),
        _absolute_zero(
            "operations.snapshot_expired_current_claims",
            "operations",
        ),
        _absolute_zero(
            "operations.snapshot_publishes_without_qos1_puback",
            "operations",
        ),
        MetricGate(
            "operations.disk_projected_days_remaining",
            "storage",
            Comparator.MINIMUM,
            "Projected writable storage remaining at observed steady-state growth.",
            threshold=7.0,
        ),
        MetricGate(
            "operations.worker_cpu_threads",
            "operations",
            Comparator.EQUAL,
            "Effective worker numerical thread limit.",
            threshold=2.0,
        ),
        MetricGate(
            "operations.worker_nice",
            "operations",
            Comparator.EQUAL,
            "Effective worker process nice value.",
            threshold=15.0,
        ),
        MetricGate(
            "operations.reserved_physical_cores",
            "operations",
            Comparator.MINIMUM,
            "Physical CPU cores excluded from worker affinity for HA/Z2M.",
            threshold=1.0,
        ),
        _absolute_zero("operations.thermal_throttling_events", "operations"),
    )
    identity = (ExternalLabelKind.CELL_IDENTITY,)
    evidence_contracts = (
        _draft_evidence_contract("tracking.id_switches", identity),
        _draft_evidence_contract("tracking.fragmentation", identity),
        _draft_evidence_contract("tracking.completeness", identity),
        _draft_evidence_contract(
            "tracking.position_error",
            (ExternalLabelKind.CELL_POSITION,),
        ),
        _draft_evidence_contract(
            "tracking.speed_error",
            (ExternalLabelKind.CELL_SPEED,),
        ),
        _draft_evidence_contract("tracking.split_precision", identity),
        _draft_evidence_contract("tracking.split_recall", identity),
        _draft_evidence_contract("tracking.merge_precision", identity),
        _draft_evidence_contract("tracking.merge_recall", identity),
        _draft_evidence_contract(
            "nowcast.passage_brier_skill",
            (ExternalLabelKind.PASSAGE,),
        ),
        _draft_evidence_contract(
            "nowcast.fractions_skill",
            (ExternalLabelKind.PRECIPITATION_FIELD,),
        ),
    )
    practical_margins = (
        _unset_margin(
            "tracking.id_switches",
            PracticalMarginMode.MINIMUM_IMPROVEMENT,
        ),
        _unset_margin(
            "tracking.fragmentation",
            PracticalMarginMode.MINIMUM_IMPROVEMENT,
        ),
        _unset_margin(
            "tracking.completeness",
            PracticalMarginMode.NONINFERIORITY_TOLERANCE,
        ),
        _unset_margin(
            "tracking.position_error",
            PracticalMarginMode.NONINFERIORITY_TOLERANCE,
        ),
        _unset_margin(
            "tracking.speed_error",
            PracticalMarginMode.NONINFERIORITY_TOLERANCE,
        ),
        _unset_margin(
            "tracking.split_precision",
            PracticalMarginMode.ABSOLUTE_THRESHOLD,
        ),
        _unset_margin(
            "tracking.split_recall",
            PracticalMarginMode.ABSOLUTE_THRESHOLD,
        ),
        _unset_margin(
            "tracking.merge_precision",
            PracticalMarginMode.ABSOLUTE_THRESHOLD,
        ),
        _unset_margin(
            "tracking.merge_recall",
            PracticalMarginMode.ABSOLUTE_THRESHOLD,
        ),
        _unset_margin(
            "nowcast.passage_brier_skill",
            PracticalMarginMode.MINIMUM_IMPROVEMENT,
        ),
        _unset_margin(
            "nowcast.fractions_skill",
            PracticalMarginMode.NONINFERIORITY_TOLERANCE,
        ),
    )
    return ValidationManifest(
        manifest_version="storm-tracker-roadmap-gates-v2",
        corpus_id="UNSET_EVENT_HELD_OUT_CORPUS",
        corpus_split_policy="UNSET_EVENT_GROUP_SPLIT_POLICY",
        gates=gates,
        sources=(),
        evidence_contracts=evidence_contracts,
        practical_margins=practical_margins,
        locked=False,
        lock_note=None,
    )


def _absolute_zero(gate_id: str, category: str) -> MetricGate:
    return MetricGate(
        gate_id=gate_id,
        category=category,
        comparator=Comparator.EQUAL,
        threshold=0.0,
        description=f"{gate_id} must remain exactly zero.",
    )


def _draft_evidence_contract(
    gate_id: str,
    label_kinds: tuple[ExternalLabelKind, ...],
) -> GateEvidenceContract:
    return GateEvidenceContract(
        gate_id=gate_id,
        required_label_kinds=tuple(
            sorted(label_kinds, key=lambda item: item.value)
        ),
        external_label_source_ids=(),
        baseline_source_ids=(),
        minimum_distinct_label_sources=1,
        partition=CorpusPartition.TEST,
    )


def _unset_margin(
    gate_id: str,
    mode: PracticalMarginMode,
) -> PracticalMargin:
    return PracticalMargin(
        gate_id=gate_id,
        mode=mode,
        value=None,
        unit=None,
        rationale=None,
        rationale_source_ids=(),
    )


def _compare(
    gate: MetricGate,
    item: MetricEvidence,
    margin: PracticalMargin | None,
) -> GateResult:
    candidate = item.candidate_value
    baseline = item.baseline_value
    comparator = gate.comparator
    if comparator in {
        Comparator.LOWER_THAN_BASELINE,
        Comparator.NOT_GREATER_THAN_BASELINE,
        Comparator.NOT_LESS_THAN_BASELINE,
        Comparator.GREATER_THAN_BASELINE,
    } and baseline is None:
        return _indeterminate(gate, "baseline-evidence ontbreekt", item)

    if (
        margin is not None
        and margin.mode is PracticalMarginMode.MINIMUM_IMPROVEMENT
    ):
        assert margin.value is not None
        if comparator is Comparator.LOWER_THAN_BASELINE:
            improvement = baseline - candidate
            passed = candidate < baseline and improvement >= margin.value
            expression = (
                f"baseline {baseline} - {candidate} >= margin {margin.value} "
                "and candidate < baseline"
            )
        elif comparator is Comparator.GREATER_THAN_BASELINE:
            improvement = candidate - baseline
            passed = candidate > baseline and improvement >= margin.value
            expression = (
                f"{candidate} - baseline {baseline} >= margin {margin.value} "
                "and candidate > baseline"
            )
        elif comparator is Comparator.POSITIVE:
            passed = candidate > 0.0 and candidate >= margin.value
            expression = (
                f"{candidate} > 0 and {candidate} >= margin {margin.value}"
            )
        else:  # pragma: no cover - rejected during manifest locking
            raise AssertionError((comparator, margin.mode))
    elif (
        margin is not None
        and margin.mode is PracticalMarginMode.NONINFERIORITY_TOLERANCE
    ):
        assert margin.value is not None
        if comparator is Comparator.NOT_GREATER_THAN_BASELINE:
            limit = baseline + margin.value
            passed = candidate <= limit
            expression = (
                f"{candidate} <= baseline {baseline} + tolerance "
                f"{margin.value}"
            )
        elif comparator is Comparator.NOT_LESS_THAN_BASELINE:
            limit = baseline - margin.value
            passed = candidate >= limit
            expression = (
                f"{candidate} >= baseline {baseline} - tolerance "
                f"{margin.value}"
            )
        else:  # pragma: no cover - rejected during manifest locking
            raise AssertionError((comparator, margin.mode))
    elif comparator is Comparator.EQUAL:
        passed = candidate == gate.threshold
        expression = f"{candidate} == {gate.threshold}"
    elif comparator is Comparator.MAXIMUM:
        passed = candidate <= gate.threshold
        expression = f"{candidate} <= {gate.threshold}"
    elif comparator is Comparator.MINIMUM:
        passed = candidate >= gate.threshold
        expression = f"{candidate} >= {gate.threshold}"
    elif comparator is Comparator.LOWER_THAN_BASELINE:
        passed = candidate < baseline
        expression = f"{candidate} < baseline {baseline}"
    elif comparator is Comparator.NOT_GREATER_THAN_BASELINE:
        passed = candidate <= baseline
        expression = f"{candidate} <= baseline {baseline}"
    elif comparator is Comparator.NOT_LESS_THAN_BASELINE:
        passed = candidate >= baseline
        expression = f"{candidate} >= baseline {baseline}"
    elif comparator is Comparator.GREATER_THAN_BASELINE:
        passed = candidate > baseline
        expression = f"{candidate} > baseline {baseline}"
    elif comparator is Comparator.POSITIVE:
        passed = candidate > 0.0
        expression = f"{candidate} > 0"
    else:  # pragma: no cover - exhaustive defensive branch
        raise AssertionError(comparator)
    return GateResult(
        gate_id=gate.gate_id,
        state=GateState.PASS if passed else GateState.FAIL,
        reason=expression,
        candidate_value=candidate,
        baseline_value=baseline,
        event_count=item.event_count,
        provenance_ids=item.provenance_ids,
        event_ids=item.event_ids,
        external_label_ids=item.external_label_ids,
        partition=item.partition,
    )


def _indeterminate(
    gate: MetricGate,
    reason: str,
    evidence: MetricEvidence | None = None,
) -> GateResult:
    return GateResult(
        gate_id=gate.gate_id,
        state=GateState.INDETERMINATE,
        reason=reason,
        candidate_value=(
            evidence.candidate_value if evidence is not None else None
        ),
        baseline_value=(
            evidence.baseline_value if evidence is not None else None
        ),
        event_count=evidence.event_count if evidence is not None else 0,
        provenance_ids=(
            evidence.provenance_ids if evidence is not None else ()
        ),
        event_ids=evidence.event_ids if evidence is not None else (),
        external_label_ids=(
            evidence.external_label_ids if evidence is not None else ()
        ),
        partition=evidence.partition if evidence is not None else None,
    )


def _external_truth_blocker(
    manifest: ValidationManifest,
    gate: MetricGate,
    item: MetricEvidence,
    context: ExternalTruthContext,
) -> str | None:
    if context.split.corpus_id != manifest.corpus_id:
        return "extern labelcorpus hoort niet bij preregistreerde eventcorpus"
    if context.split.policy_fingerprint != manifest.corpus_split_policy:
        return "eventsplit wijkt af van preregistreerde splitpolicy"
    contract = next(
        (
            value
            for value in manifest.evidence_contracts
            if value.gate_id == gate.gate_id
        ),
        None,
    )
    if contract is None:
        return "extern evidencecontract ontbreekt"
    if item.partition is not contract.partition:
        return "metric-evidence vermeldt niet de preregistreerde partition"
    if not item.event_ids:
        return "event_ids voor externe metric ontbreken"
    if item.event_count != len(item.event_ids):
        return "event_count komt niet overeen met unieke event_ids"
    for event_id in item.event_ids:
        partition = context.split.partition_for_event(event_id)
        if partition is None:
            return f"onbekend event_id in metric-evidence: {event_id}"
        if partition is not contract.partition:
            return f"event {event_id} komt niet uit held-out {contract.partition.value}"
    if not item.provenance_ids:
        return "metricberekeningsprovenance ontbreekt"
    if not item.external_label_ids:
        return "external_label_ids ontbreken"

    prereg_sources = {
        source.source_id: source
        for source in manifest.sources
    }
    selected_labels = []
    for label_id in item.external_label_ids:
        label = context.labels.label(label_id)
        if label is None:
            return f"extern label ontbreekt: {label_id}"
        if label.state is not LabelState.CONFIRMED:
            return f"extern label is niet confirmed: {label_id}"
        if label.label_kind not in contract.required_label_kinds:
            return f"extern labeltype is niet preregistreerd: {label_id}"
        if label.event_id not in item.event_ids:
            return f"extern label hoort bij ander event: {label_id}"
        if label.source_id not in contract.external_label_source_ids:
            return f"labelbron is niet preregistreerd: {label.source_id}"
        actual_source = context.labels.source(label.source_id)
        if actual_source is None:  # pragma: no cover - corpus invariant
            return f"labelbron ontbreekt: {label.source_id}"
        if not actual_source.is_external_truth_eligible:
            return f"labelbron is niet onafhankelijk/blind: {label.source_id}"
        prereg_source = prereg_sources.get(label.source_id)
        if prereg_source is None:
            return f"preregistratiebron ontbreekt: {label.source_id}"
        if (
            prereg_source.role
            is not PreregistrationSourceRole.EXTERNAL_LABEL
        ):
            return f"preregistratiebron heeft verkeerde rol: {label.source_id}"
        if prereg_source.artifact_sha256 != (
            actual_source.source_artifact_sha256
        ):
            return f"labelbrondigest wijkt af: {label.source_id}"
        selected_labels.append(label)

    source_count = len({label.source_id for label in selected_labels})
    if source_count < contract.minimum_distinct_label_sources:
        return (
            f"slechts {source_count} onafhankelijke labelbronnen; "
            f"{contract.minimum_distinct_label_sources} vereist"
        )
    for event_id in item.event_ids:
        for label_kind in contract.required_label_kinds:
            covering_sources = {
                label.source_id
                for label in selected_labels
                if (
                    label.event_id == event_id
                    and label.label_kind is label_kind
                )
            }
            if not covering_sources:
                return (
                    f"event {event_id} mist confirmed "
                    f"{label_kind.value}-label"
                )
            if (
                len(covering_sources)
                < contract.minimum_distinct_label_sources
            ):
                return (
                    f"event {event_id}/{label_kind.value} heeft slechts "
                    f"{len(covering_sources)} onafhankelijke labelbronnen"
                )

    baseline_required = (
        gate.requires_baseline_source
        or gate.comparator
        in {
            Comparator.LOWER_THAN_BASELINE,
            Comparator.NOT_GREATER_THAN_BASELINE,
            Comparator.NOT_LESS_THAN_BASELINE,
            Comparator.GREATER_THAN_BASELINE,
        }
    )
    if baseline_required:
        if not item.baseline_source_ids:
            return "baseline-sourceprovenance ontbreekt"
        if not set(item.baseline_source_ids) <= set(
            contract.baseline_source_ids
        ):
            return "metric gebruikt niet-preregisterde baselinebron"
        for source_id in item.baseline_source_ids:
            source = prereg_sources.get(source_id)
            if source is None:
                return f"baselinebron ontbreekt: {source_id}"
            if source.role is not PreregistrationSourceRole.BASELINE:
                return f"baselinebron heeft verkeerde rol: {source_id}"
    return None


def _validate_margin_contract(
    gate: MetricGate,
    margin: PracticalMargin,
    source_by_id: Mapping[str, PreregistrationSource],
) -> list[str]:
    blockers: list[str] = []
    if margin.mode is PracticalMarginMode.ABSOLUTE_THRESHOLD:
        if gate.comparator not in {
            Comparator.EQUAL,
            Comparator.MINIMUM,
            Comparator.MAXIMUM,
        }:
            blockers.append(
                f"{gate.gate_id}: absolute marge past niet bij comparator"
            )
        elif gate.threshold != margin.value:
            blockers.append(
                f"{gate.gate_id}: threshold en praktische marge verschillen"
            )
    elif margin.mode is PracticalMarginMode.MINIMUM_IMPROVEMENT:
        if gate.comparator not in {
            Comparator.LOWER_THAN_BASELINE,
            Comparator.GREATER_THAN_BASELINE,
            Comparator.POSITIVE,
        }:
            blockers.append(
                f"{gate.gate_id}: improvementmarge past niet bij comparator"
            )
    elif margin.mode is PracticalMarginMode.NONINFERIORITY_TOLERANCE:
        if gate.comparator not in {
            Comparator.NOT_GREATER_THAN_BASELINE,
            Comparator.NOT_LESS_THAN_BASELINE,
        }:
            blockers.append(
                f"{gate.gate_id}: noninferioritymarge past niet bij comparator"
            )
    for source_id in margin.rationale_source_ids:
        source = source_by_id.get(source_id)
        if source is None:
            blockers.append(
                f"{gate.gate_id}: onbekende rationalebron {source_id}"
            )
        elif source.role is not PreregistrationSourceRole.MARGIN_RATIONALE:
            blockers.append(
                f"{gate.gate_id}: {source_id} is geen margin-rationale source"
            )
    return blockers


def _identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} mag niet leeg zijn")
    if "\x00" in value or len(value) > 512:
        raise ValueError(f"{field} is ongeldig")
    return value


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{field} vereist lowercase SHA-256")
    return value


def _sorted_unique_identifiers(
    values: object,
    field: str,
    *,
    allow_empty: bool,
) -> tuple[str, ...]:
    if not isinstance(values, tuple):
        raise TypeError(f"{field} moet tuple zijn")
    if not allow_empty and not values:
        raise ValueError(f"{field} mag niet leeg zijn")
    for value in values:
        _identifier(value, field)
    if len(values) != len(set(values)):
        raise ValueError(f"{field} bevat duplicaten")
    if values != tuple(sorted(values)):
        raise ValueError(f"{field} moet gesorteerd zijn")
    return values


def _unique_attribute(values: tuple, attribute: str) -> None:
    keys = [getattr(item, attribute) for item in values]
    if len(keys) != len(set(keys)):
        raise ValueError(f"{attribute} moet uniek zijn")


def _finite(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} moet numeriek zijn")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{field} moet eindig zijn")
    return numeric
