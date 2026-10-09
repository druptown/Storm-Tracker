"""Scientific release evaluation backed by independent held-out labels."""
from __future__ import annotations

from dataclasses import replace

import pytest

from storm_tracker_worker.scientific_validation import (
    Comparator,
    GateEvidenceContract,
    GateState,
    MetricEvidence,
    MetricGate,
    PracticalMargin,
    PracticalMarginMode,
    PreregistrationSource,
    PreregistrationSourceRole,
    ValidationManifest,
    evaluate_manifest,
    roadmap_release_manifest,
)
from storm_tracker_worker.validation_corpus import (
    CellSpeedLabel,
    CorpusPartition,
    EventFrame,
    EventSplitPolicy,
    ExternalLabel,
    ExternalLabelCorpus,
    ExternalLabelKind,
    ExternalLabelSource,
    ExternalSourceKind,
    ExternalTruthContext,
    LabelState,
    build_event_held_out_split,
)


LABEL_SOURCE_ID = "synthetic-independent-speed-labels"
BASELINE_SOURCE_ID = "synthetic-frozen-baseline"
RATIONALE_SOURCE_ID = "synthetic-margin-rationale"
LABEL_DIGEST = "a" * 64
BASELINE_DIGEST = "b" * 64
RATIONALE_DIGEST = "c" * 64


def _split():
    frames = tuple(
        EventFrame(
            frame_id=f"synthetic-frame-{index:03d}",
            event_id=f"synthetic-event-{index:03d}",
            valid_time_utc=float(index + 1),
            leakage_group_ids=(),
        )
        for index in range(200)
    )
    policy = EventSplitPolicy(
        policy_id="synthetic-heldout-policy",
        hash_salt="synthetic-heldout-salt",
        train_basis_points=5000,
        validation_basis_points=2500,
        test_basis_points=2500,
    )
    return build_event_held_out_split(frames, policy)


def _event_frame(split, partition: CorpusPartition) -> tuple[str, str]:
    event_id = next(
        event
        for event, assigned in split.event_partitions
        if assigned is partition
    )
    frame_id = next(
        frame
        for frame, frame_event in split.frame_events
        if frame_event == event_id
    )
    return event_id, frame_id


def _label_source(*, digest: str = LABEL_DIGEST):
    return ExternalLabelSource(
        source_id=LABEL_SOURCE_ID,
        source_kind=ExternalSourceKind.INDEPENDENT_REFERENCE_ANALYSIS,
        producer="synthetic-test-producer",
        dataset_version="synthetic-test-v1",
        methodology_id="synthetic-speed-method-v1",
        source_artifact_sha256=digest,
        candidate_independent=True,
        blinded_to_candidate=True,
    )


def _label(
    split,
    *,
    partition: CorpusPartition = CorpusPartition.TEST,
    state: LabelState = LabelState.CONFIRMED,
):
    event_id, frame_id = _event_frame(split, partition)
    return ExternalLabel(
        label_id=f"synthetic-speed-label-{partition.value}",
        label_kind=ExternalLabelKind.CELL_SPEED,
        source_id=LABEL_SOURCE_ID,
        event_id=event_id,
        frame_id=frame_id,
        state=state,
        payload=CellSpeedLabel(
            reference_cell_id=f"synthetic-cell-{partition.value}",
            eastward_mps=3.0,
            northward_mps=1.0,
            interval_start_utc=1.0,
            interval_end_utc=2.0,
            vector_uncertainty_mps=None,
        ),
    )


def _context(
    split,
    *,
    partition: CorpusPartition = CorpusPartition.TEST,
    state: LabelState = LabelState.CONFIRMED,
    digest: str = LABEL_DIGEST,
) -> tuple[ExternalTruthContext, ExternalLabel]:
    label = _label(split, partition=partition, state=state)
    corpus = ExternalLabelCorpus(
        sources=(_label_source(digest=digest),),
        labels=(label,),
    )
    return ExternalTruthContext(split=split, labels=corpus), label


def _manifest(split) -> ValidationManifest:
    gate = MetricGate(
        gate_id="tracking.speed_error",
        category="tracking",
        comparator=Comparator.NOT_GREATER_THAN_BASELINE,
        description="Synthetic test-only speed error gate.",
        requires_external_truth=True,
        minimum_events=1,
        requires_practical_margin=True,
        requires_baseline_source=True,
    )
    sources = (
        PreregistrationSource(
            source_id=BASELINE_SOURCE_ID,
            role=PreregistrationSourceRole.BASELINE,
            artifact_sha256=BASELINE_DIGEST,
            description="Synthetic frozen baseline fixture.",
            candidate_independent=True,
        ),
        PreregistrationSource(
            source_id=LABEL_SOURCE_ID,
            role=PreregistrationSourceRole.EXTERNAL_LABEL,
            artifact_sha256=LABEL_DIGEST,
            description="Synthetic independent label fixture.",
            candidate_independent=True,
        ),
        PreregistrationSource(
            source_id=RATIONALE_SOURCE_ID,
            role=PreregistrationSourceRole.MARGIN_RATIONALE,
            artifact_sha256=RATIONALE_DIGEST,
            description="Synthetic rationale fixture.",
            candidate_independent=True,
        ),
    )
    contract = GateEvidenceContract(
        gate_id=gate.gate_id,
        required_label_kinds=(ExternalLabelKind.CELL_SPEED,),
        external_label_source_ids=(LABEL_SOURCE_ID,),
        baseline_source_ids=(BASELINE_SOURCE_ID,),
        minimum_distinct_label_sources=1,
        partition=CorpusPartition.TEST,
    )
    # This number is solely a synthetic unit-test fixture.  The roadmap
    # manifest keeps the real value, unit and rationale unset.
    margin = PracticalMargin(
        gate_id=gate.gate_id,
        mode=PracticalMarginMode.NONINFERIORITY_TOLERANCE,
        value=0.5,
        unit="synthetic_test_unit",
        rationale="Synthetic fixture exercising explicit tolerance semantics.",
        rationale_source_ids=(RATIONALE_SOURCE_ID,),
    )
    return ValidationManifest(
        manifest_version="synthetic-heldout-manifest-v1",
        corpus_id=split.corpus_id,
        corpus_split_policy=split.policy_fingerprint,
        gates=(gate,),
        sources=sources,
        evidence_contracts=(contract,),
        practical_margins=(margin,),
    )


def _evidence(label: ExternalLabel) -> MetricEvidence:
    return MetricEvidence(
        gate_id="tracking.speed_error",
        candidate_value=10.4,
        baseline_value=10.0,
        event_count=1,
        external_truth_available=True,
        provenance_ids=("synthetic-metric-computation-sha256:fixture",),
        event_ids=(label.event_id,),
        external_label_ids=(label.label_id,),
        baseline_source_ids=(BASELINE_SOURCE_ID,),
        partition=CorpusPartition.TEST,
    )


def test_complete_preregistered_external_chain_can_lock_and_evaluate():
    split = _split()
    context, label = _context(split)
    manifest = _manifest(split).lock(
        lock_note="Synthetic fixture locked before test evidence."
    )

    report = evaluate_manifest(
        manifest,
        (_evidence(label),),
        external_truth=context,
    )

    assert report.release_ready is True
    assert report.results[0].state is GateState.PASS
    assert report.results[0].event_ids == (label.event_id,)
    assert report.results[0].external_label_ids == (label.label_id,)
    assert "tolerance 0.5" in report.results[0].reason


def test_external_truth_boolean_without_label_context_is_indeterminate():
    split = _split()
    _, label = _context(split)
    manifest = _manifest(split).lock(
        lock_note="Synthetic fixture locked before test evidence."
    )

    report = evaluate_manifest(manifest, (_evidence(label),))

    assert report.release_ready is False
    assert report.results[0].state is GateState.INDETERMINATE
    assert report.results[0].reason == (
        "gevalideerd extern labelcorpus ontbreekt"
    )


def test_unlocked_manifest_never_discloses_heldout_gate_result():
    split = _split()
    context, label = _context(split)

    report = evaluate_manifest(
        _manifest(split),
        (_evidence(label),),
        external_truth=context,
    )

    assert report.release_ready is False
    assert report.results[0].state is GateState.INDETERMINATE
    assert "vóór manifest-lock" in report.results[0].reason


def test_non_test_event_is_rejected_even_with_valid_external_label():
    split = _split()
    context, label = _context(split, partition=CorpusPartition.TRAIN)
    manifest = _manifest(split).lock(
        lock_note="Synthetic fixture locked before test evidence."
    )
    evidence = replace(
        _evidence(label),
        partition=CorpusPartition.TEST,
    )

    report = evaluate_manifest(
        manifest,
        (evidence,),
        external_truth=context,
    )

    assert report.results[0].state is GateState.INDETERMINATE
    assert "held-out test" in report.results[0].reason


def test_ambiguous_label_and_source_digest_mismatch_fail_closed():
    split = _split()
    ambiguous_context, ambiguous_label = _context(
        split,
        state=LabelState.AMBIGUOUS,
    )
    manifest = _manifest(split).lock(
        lock_note="Synthetic fixture locked before test evidence."
    )
    ambiguous = evaluate_manifest(
        manifest,
        (_evidence(ambiguous_label),),
        external_truth=ambiguous_context,
    )
    assert ambiguous.results[0].state is GateState.INDETERMINATE
    assert "niet confirmed" in ambiguous.results[0].reason

    wrong_digest_context, label = _context(split, digest="d" * 64)
    wrong_digest = evaluate_manifest(
        manifest,
        (_evidence(label),),
        external_truth=wrong_digest_context,
    )
    assert wrong_digest.results[0].state is GateState.INDETERMINATE
    assert "labelbrondigest wijkt af" in wrong_digest.results[0].reason


def test_missing_event_ids_labels_or_baseline_provenance_is_indeterminate():
    split = _split()
    context, label = _context(split)
    manifest = _manifest(split).lock(
        lock_note="Synthetic fixture locked before test evidence."
    )

    cases = (
        (
            replace(_evidence(label), event_ids=()),
            "event_ids",
        ),
        (
            replace(_evidence(label), external_label_ids=()),
            "external_label_ids",
        ),
        (
            replace(_evidence(label), baseline_source_ids=()),
            "baseline-sourceprovenance",
        ),
    )
    for evidence, reason in cases:
        report = evaluate_manifest(
            manifest,
            (evidence,),
            external_truth=context,
        )
        assert report.results[0].state is GateState.INDETERMINATE
        assert reason in report.results[0].reason


def test_lock_rejects_missing_margin_rationale_or_required_sources():
    split = _split()
    complete = _manifest(split)
    unresolved_margin = replace(
        complete.practical_margins[0],
        rationale=None,
        rationale_source_ids=(),
    )
    incomplete = replace(
        complete,
        sources=(),
        practical_margins=(unresolved_margin,),
    )

    assert any(
        "onopgeloste praktische marge" in blocker
        for blocker in incomplete.lock_blockers
    )
    assert any(
        "labelbron" in blocker or "baselinebron" in blocker
        for blocker in incomplete.lock_blockers
    )
    with pytest.raises(ValueError, match="niet vergrendelen"):
        incomplete.lock(lock_note="Must fail.")


def test_manifest_rejects_contracts_or_margins_on_unmarked_gate():
    plain = MetricGate(
        gate_id="plain",
        category="synthetic",
        comparator=Comparator.MINIMUM,
        description="Synthetic plain gate.",
        threshold=1.0,
    )
    stray_margin = PracticalMargin(
        gate_id="plain",
        mode=PracticalMarginMode.ABSOLUTE_THRESHOLD,
        value=1.0,
        unit="synthetic",
        rationale="Synthetic stray margin.",
        rationale_source_ids=("source",),
    )
    with pytest.raises(ValueError, match="zonder requires_practical_margin"):
        ValidationManifest(
            manifest_version="synthetic",
            corpus_id="synthetic",
            corpus_split_policy="synthetic",
            gates=(plain,),
            practical_margins=(stray_margin,),
        )

    stray_contract = GateEvidenceContract(
        gate_id="plain",
        required_label_kinds=(ExternalLabelKind.CELL_SPEED,),
        external_label_source_ids=("source",),
    )
    with pytest.raises(ValueError, match="zonder requires_external_truth"):
        ValidationManifest(
            manifest_version="synthetic",
            corpus_id="synthetic",
            corpus_split_policy="synthetic",
            gates=(plain,),
            evidence_contracts=(stray_contract,),
        )


def test_roadmap_practical_margins_and_sources_remain_explicitly_unset():
    manifest = roadmap_release_manifest()
    external_gate_ids = {
        gate.gate_id
        for gate in manifest.gates
        if gate.requires_external_truth
    }

    assert external_gate_ids == {
        contract.gate_id
        for contract in manifest.evidence_contracts
    }
    assert external_gate_ids == {
        margin.gate_id
        for margin in manifest.practical_margins
    }
    assert manifest.sources == ()
    assert all(margin.value is None for margin in manifest.practical_margins)
    assert all(margin.unit is None for margin in manifest.practical_margins)
    assert all(
        margin.rationale is None
        for margin in manifest.practical_margins
    )
    assert all(
        not contract.external_label_source_ids
        for contract in manifest.evidence_contracts
    )
    assert manifest.locked is False
    assert manifest.lock_blockers
