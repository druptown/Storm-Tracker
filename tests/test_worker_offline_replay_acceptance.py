"""Offline-only acceptance boundary for deterministic science replays."""
from __future__ import annotations

import ast
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from storm_tracker_worker.offline_replay_acceptance import (
    ArtifactBinding,
    ArtifactRole,
    BaselineId,
    BaselineReplayEvidence,
    MINIMUM_ACCEPTANCE_FRAMES,
    OfflineMetricBundle,
    OfflineReplayInput,
    REQUIRED_ARTIFACT_ROLES,
    REQUIRED_BASELINES,
    REQUIRED_OFFLINE_GATE_IDS,
    REQUIRED_SCIENCE_STAGES,
    ReplayFrameBinding,
    StageReplayEvidence,
    audit_local_science_component_sources,
    baseline_execution_manifest_sha256,
    build_preflight_mapping,
    candidate_execution_manifest_sha256,
    evaluate_offline_replay_acceptance,
    external_truth_context_sha256,
    load_offline_replay_input,
    offline_replay_input_from_mapping,
)
from storm_tracker_worker.scientific_validation import (
    Comparator,
    GateEvidenceContract,
    MetricEvidence,
    MetricGate,
    PreregistrationSource,
    PreregistrationSourceRole,
    ValidationManifest,
)
from storm_tracker_worker.validation_corpus import (
    CellIdentityLabel,
    CellSpeedLabel,
    CorpusPartition,
    EventSplit,
    ExternalLabel,
    ExternalLabelCorpus,
    ExternalLabelKind,
    ExternalLabelSource,
    ExternalSourceKind,
    ExternalTruthContext,
    LabelState,
    PassageLabel,
    WetDryLabel,
    WetDryState,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "offline_replay_acceptance_golden.json"
LABEL_SOURCE_ID = "synthetic-blinded-consensus"
LABEL_SOURCE_SHA = "9" * 64
EVENT_ID = "synthetic-heldout-event"


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _mapping_sha(value) -> str:
    body = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(body).hexdigest()


def _truth_context() -> ExternalTruthContext:
    frame_ids = tuple(
        f"synthetic-heldout-frame-{index:03d}"
        for index in range(MINIMUM_ACCEPTANCE_FRAMES)
    )
    split = EventSplit(
        corpus_id="sha256:" + "a" * 64,
        policy_fingerprint="sha256:" + "b" * 64,
        frame_events=tuple((frame_id, EVENT_ID) for frame_id in frame_ids),
        frame_partitions=tuple(
            (frame_id, CorpusPartition.TEST) for frame_id in frame_ids
        ),
        event_partitions=((EVENT_ID, CorpusPartition.TEST),),
        group_partitions=(),
        event_components=((EVENT_ID, EVENT_ID),),
    )
    source = ExternalLabelSource(
        source_id=LABEL_SOURCE_ID,
        source_kind=ExternalSourceKind.BLINDED_HUMAN_CONSENSUS,
        producer="synthetic-independent-reviewers",
        dataset_version="synthetic-golden-v1",
        methodology_id="synthetic-consensus-v1",
        source_artifact_sha256=LABEL_SOURCE_SHA,
        candidate_independent=True,
        blinded_to_candidate=True,
    )
    frame_id = frame_ids[0]
    labels = (
        ExternalLabel(
            label_id="label-cell-identity",
            label_kind=ExternalLabelKind.CELL_IDENTITY,
            source_id=LABEL_SOURCE_ID,
            event_id=EVENT_ID,
            frame_id=frame_id,
            state=LabelState.CONFIRMED,
            payload=CellIdentityLabel(reference_cell_id="reference-cell"),
        ),
        ExternalLabel(
            label_id="label-cell-speed",
            label_kind=ExternalLabelKind.CELL_SPEED,
            source_id=LABEL_SOURCE_ID,
            event_id=EVENT_ID,
            frame_id=frame_id,
            state=LabelState.CONFIRMED,
            payload=CellSpeedLabel(
                reference_cell_id="reference-cell",
                eastward_mps=3.0,
                northward_mps=1.0,
                interval_start_utc=1_800_000_000.0,
                interval_end_utc=1_800_000_300.0,
                vector_uncertainty_mps=0.5,
            ),
        ),
        ExternalLabel(
            label_id="label-passage",
            label_kind=ExternalLabelKind.PASSAGE,
            source_id=LABEL_SOURCE_ID,
            event_id=EVENT_ID,
            frame_id=frame_id,
            state=LabelState.CONFIRMED,
            payload=PassageLabel(
                tracker_id="synthetic-tracker",
                window_start_utc=1_800_000_000.0,
                window_end_utc=1_800_003_600.0,
                occurred=True,
                passage_time_utc=1_800_001_800.0,
                time_uncertainty_s=60.0,
            ),
        ),
        ExternalLabel(
            label_id="label-wet-dry",
            label_kind=ExternalLabelKind.WET_DRY,
            source_id=LABEL_SOURCE_ID,
            event_id=EVENT_ID,
            frame_id=frame_id,
            state=LabelState.CONFIRMED,
            payload=WetDryLabel(
                site_id="synthetic-site",
                interval_start_utc=1_800_000_000.0,
                interval_end_utc=1_800_000_300.0,
                state=WetDryState.WET,
                capable_of_dry_detection=True,
                accumulation_mm=1.0,
            ),
        ),
    )
    return ExternalTruthContext(
        split=split,
        labels=ExternalLabelCorpus(sources=(source,), labels=labels),
    )


def _label_kind(gate_id: str) -> ExternalLabelKind:
    if gate_id == "flow.one_step_advection_skill":
        return ExternalLabelKind.CELL_SPEED
    if gate_id == "ground.cross_storm_false_confirmations":
        return ExternalLabelKind.WET_DRY
    if gate_id in {
        "calibration.heldout_brier_skill",
        "nowcast.passage_brier_skill",
    }:
        return ExternalLabelKind.PASSAGE
    return ExternalLabelKind.CELL_IDENTITY


def _label_id(kind: ExternalLabelKind) -> str:
    return {
        ExternalLabelKind.CELL_IDENTITY: "label-cell-identity",
        ExternalLabelKind.CELL_SPEED: "label-cell-speed",
        ExternalLabelKind.PASSAGE: "label-passage",
        ExternalLabelKind.WET_DRY: "label-wet-dry",
    }[kind]


def _locked_manifest(context: ExternalTruthContext) -> ValidationManifest:
    gates = []
    contracts = []
    for gate_id in REQUIRED_OFFLINE_GATE_IDS:
        if gate_id == "ground.cross_storm_false_confirmations":
            comparator = Comparator.EQUAL
            threshold = 0.0
        elif gate_id in {
            "tracking.id_switches",
        }:
            comparator = Comparator.MAXIMUM
            threshold = 2.0
        elif gate_id == "tracking.lineage_integrity":
            comparator = Comparator.EQUAL
            threshold = 1.0
        else:
            comparator = Comparator.POSITIVE
            threshold = None
        category = gate_id.split(".", maxsplit=1)[0]
        gates.append(
            MetricGate(
                gate_id=gate_id,
                category=category,
                comparator=comparator,
                description=f"Synthetic held-out contract for {gate_id}.",
                threshold=threshold,
                requires_external_truth=True,
                minimum_events=1,
            )
        )
        contracts.append(
            GateEvidenceContract(
                gate_id=gate_id,
                required_label_kinds=(_label_kind(gate_id),),
                external_label_source_ids=(LABEL_SOURCE_ID,),
                minimum_distinct_label_sources=1,
                partition=CorpusPartition.TEST,
            )
        )
    source = PreregistrationSource(
        source_id=LABEL_SOURCE_ID,
        role=PreregistrationSourceRole.EXTERNAL_LABEL,
        artifact_sha256=LABEL_SOURCE_SHA,
        description="Synthetic blinded consensus fixture.",
        candidate_independent=True,
    )
    baseline_sources = tuple(
        PreregistrationSource(
            source_id=f"synthetic-baseline-source-{baseline_id.value}",
            role=PreregistrationSourceRole.BASELINE,
            artifact_sha256=_sha(f"baseline-output:{baseline_id.value}"),
            description=f"Synthetic frozen {baseline_id.value} output.",
            candidate_independent=True,
        )
        for baseline_id in REQUIRED_BASELINES
    )
    return ValidationManifest(
        manifest_version="synthetic-offline-science-gates-v1",
        corpus_id=context.split.corpus_id,
        corpus_split_policy=context.split.policy_fingerprint,
        gates=tuple(gates),
        sources=tuple(
            sorted((source, *baseline_sources), key=lambda item: item.source_id)
        ),
        evidence_contracts=tuple(contracts),
    ).lock(lock_note="Synthetic manifest locked before candidate execution.")


def _artifact_bindings(
    context: ExternalTruthContext,
    frames: tuple[ReplayFrameBinding, ...],
) -> tuple[ArtifactBinding, ...]:
    role_hashes = {
        ArtifactRole.ANNOTATION_PROTOCOL: _sha("synthetic-annotation-protocol"),
        ArtifactRole.EVENT_SPLIT_MANIFEST: context.split.sha256,
        ArtifactRole.GOLDEN_LABEL_COMMITMENT: context.labels.sha256,
        ArtifactRole.RAW_INPUT_MANIFEST: _mapping_sha(
            [frame.to_mapping() for frame in frames]
        ),
    }
    return tuple(
        ArtifactBinding(
            artifact_id=f"synthetic-{role.value}",
            role=role,
            sha256=role_hashes[role],
            size_bytes=100 + index,
            media_type="application/json",
        )
        for index, role in enumerate(REQUIRED_ARTIFACT_ROLES)
    )


def _replay_input(
    context: ExternalTruthContext,
    manifest: ValidationManifest,
) -> OfflineReplayInput:
    frames = tuple(
        ReplayFrameBinding(
            frame_id=frame_id,
            event_id=EVENT_ID,
            valid_time_utc=1_800_000_000.0 + index * 300.0,
            raw_frame_sha256=_sha(f"raw-frame:{index}"),
        )
        for index, (frame_id, _) in enumerate(context.split.frame_events)
    )
    return OfflineReplayInput(
        contract_id="synthetic-offline-heldout-contract",
        corpus_id=context.split.corpus_id,
        split_policy_fingerprint=context.split.policy_fingerprint,
        scientific_manifest_sha256=manifest.sha256,
        external_truth_context_sha256=external_truth_context_sha256(context),
        partition=CorpusPartition.TEST,
        frames=frames,
        artifacts=_artifact_bindings(context, frames),
        labels_blinded_to_candidate=True,
        label_sources_candidate_independent=True,
        golden_consensus_confirmed=True,
    )


def _stages(replay_input: OfflineReplayInput):
    sources = audit_local_science_component_sources()
    result = tuple(
        StageReplayEvidence(
            stage=source.stage,
            implementation_version=source.implementation_version,
            implementation_sha256=source.source_sha256,
            input_contract_sha256=replay_input.sha256,
            primary_output_sha256=_sha(f"stage:{source.stage.value}"),
            reproduction_output_sha256=_sha(f"stage:{source.stage.value}"),
            event_ids=replay_input.event_ids,
            frame_count=len(replay_input.frames),
            completed=True,
            shadow_only=True,
            operational_state_mutated=False,
        )
        for source in sources
    )
    return sources, result


def _baselines(replay_input: OfflineReplayInput):
    return tuple(
        BaselineReplayEvidence(
            baseline_id=baseline_id,
            preregistration_source_id=(
                f"synthetic-baseline-source-{baseline_id.value}"
            ),
            implementation_id=f"synthetic-{baseline_id.value}",
            implementation_version="synthetic-v1",
            implementation_sha256=_sha(
                f"baseline-source:{baseline_id.value}"
            ),
            input_contract_sha256=replay_input.sha256,
            primary_output_sha256=_sha(
                f"baseline-output:{baseline_id.value}"
            ),
            reproduction_output_sha256=_sha(
                f"baseline-output:{baseline_id.value}"
            ),
            event_ids=replay_input.event_ids,
            frame_count=len(replay_input.frames),
            completed=True,
            candidate_independent=True,
        )
        for baseline_id in REQUIRED_BASELINES
    )


def _metric_bundle(
    replay_input: OfflineReplayInput,
    stages,
    baselines,
) -> OfflineMetricBundle:
    algorithm_id = "synthetic-offline-metric-computer"
    algorithm_version = "synthetic-v1"
    algorithm_sha = _sha("synthetic-offline-metric-computer-source")
    provenance = (
        f"{algorithm_id}@{algorithm_version}:sha256:{algorithm_sha}"
    )
    evidence = []
    for gate_id in REQUIRED_OFFLINE_GATE_IDS:
        value = (
            0.0
            if gate_id == "ground.cross_storm_false_confirmations"
            else 1.0
        )
        evidence.append(
            MetricEvidence(
                gate_id=gate_id,
                candidate_value=value,
                event_count=1,
                external_truth_available=True,
                provenance_ids=(provenance,),
                event_ids=replay_input.event_ids,
                external_label_ids=(_label_id(_label_kind(gate_id)),),
                partition=CorpusPartition.TEST,
            )
        )
    return OfflineMetricBundle(
        algorithm_id=algorithm_id,
        algorithm_version=algorithm_version,
        algorithm_sha256=algorithm_sha,
        input_contract_sha256=replay_input.sha256,
        candidate_manifest_sha256=candidate_execution_manifest_sha256(stages),
        baseline_manifest_sha256=baseline_execution_manifest_sha256(baselines),
        external_truth_context_sha256=replay_input.external_truth_context_sha256,
        evidence=tuple(evidence),
    )


def _complete_dossier():
    context = _truth_context()
    manifest = _locked_manifest(context)
    replay_input = _replay_input(context, manifest)
    sources, stages = _stages(replay_input)
    baselines = _baselines(replay_input)
    metrics = _metric_bundle(replay_input, stages, baselines)
    return context, manifest, replay_input, sources, stages, baselines, metrics


def test_golden_input_contract_is_strict_content_addressed_and_tamper_evident():
    loaded = load_offline_replay_input(FIXTURE)
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))

    assert loaded == offline_replay_input_from_mapping(raw)
    assert len(loaded.sha256) == 64
    assert loaded.partition is CorpusPartition.TEST
    assert tuple(item.role for item in loaded.artifacts) == REQUIRED_ARTIFACT_ROLES

    tampered = json.loads(FIXTURE.read_text(encoding="utf-8"))
    tampered["frames"][0]["raw_frame_sha256"] = "f" * 64
    changed = offline_replay_input_from_mapping(tampered)
    assert changed.sha256 != loaded.sha256

    pinned = loaded.to_mapping()
    pinned["input_contract_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="input_contract_sha256 differs"):
        offline_replay_input_from_mapping(pinned)


def test_component_audit_binds_every_existing_science_stage_once():
    first = audit_local_science_component_sources()
    second = audit_local_science_component_sources(ROOT / "storm_tracker_worker")

    assert first == second
    assert tuple(item.stage for item in first) == REQUIRED_SCIENCE_STAGES
    assert len({item.source_sha256 for item in first}) == len(first)
    assert all(len(item.source_sha256) == 64 for item in first)
    assert all(item.size_bytes > 0 for item in first)


def test_preflight_is_explicitly_non_admitting_and_has_no_fake_results():
    replay_input = load_offline_replay_input(FIXTURE)
    preflight = build_preflight_mapping(
        replay_input,
        component_sources=audit_local_science_component_sources(),
    )

    assert preflight["offline_science_gate_passed"] is False
    assert preflight["meteorological_admission"] is False
    assert preflight["live_wiring_allowed"] is False
    assert preflight["dashboard_publication_allowed"] is False
    assert "candidate_stage_evidence_not_supplied" in preflight["preflight_blockers"]
    assert "baseline_evidence_not_supplied" in preflight["preflight_blockers"]


def test_complete_synthetic_dossier_passes_only_the_offline_science_scope():
    (
        context,
        manifest,
        replay_input,
        sources,
        stages,
        baselines,
        metrics,
    ) = _complete_dossier()

    first = evaluate_offline_replay_acceptance(
        replay_input,
        component_sources=sources,
        stages=stages,
        baselines=baselines,
        metric_bundle=metrics,
        validation_manifest=manifest,
        external_truth=context,
    )
    second = evaluate_offline_replay_acceptance(
        replay_input,
        component_sources=sources,
        stages=tuple(reversed(stages)),
        baselines=tuple(reversed(baselines)),
        metric_bundle=metrics,
        validation_manifest=manifest,
        external_truth=context,
    )

    assert first == second
    assert first.offline_science_gate_passed is True
    assert first.blocking_reasons == ()
    assert first.validation_report is not None
    assert first.validation_report.release_ready is True
    assert first.meteorological_admission is False
    assert first.live_wiring_allowed is False
    assert first.dashboard_publication_allowed is False
    assert first.home_assistant_state_mutated is False
    assert first.to_mapping()["acceptance_receipt_sha256"] == first.sha256


def test_any_stage_or_baseline_reproducibility_break_fails_closed():
    (
        context,
        manifest,
        replay_input,
        sources,
        stages,
        baselines,
        _,
    ) = _complete_dossier()
    damaged_stages = (
        replace(stages[0], reproduction_output_sha256=_sha("different")),
        *stages[1:],
    )
    damaged_baselines = (
        replace(baselines[0], candidate_independent=False),
        *baselines[1:],
    )
    metrics = _metric_bundle(replay_input, damaged_stages, damaged_baselines)

    receipt = evaluate_offline_replay_acceptance(
        replay_input,
        component_sources=sources,
        stages=damaged_stages,
        baselines=damaged_baselines,
        metric_bundle=metrics,
        validation_manifest=manifest,
        external_truth=context,
    )

    assert receipt.offline_science_gate_passed is False
    assert any("stage." in reason and "not_reproducible" in reason for reason in receipt.blocking_reasons)
    assert any("baseline." in reason and "candidate_dependent" in reason for reason in receipt.blocking_reasons)
    assert receipt.meteorological_admission is False


def test_missing_roadmap_baselines_and_gates_can_never_be_hidden():
    (
        context,
        manifest,
        replay_input,
        sources,
        stages,
        baselines,
        _,
    ) = _complete_dossier()
    reduced_baselines = baselines[1:]
    reduced_manifest = replace(
        manifest,
        locked=False,
        lock_note=None,
        gates=manifest.gates[1:],
        evidence_contracts=manifest.evidence_contracts[1:],
    ).lock(lock_note="Synthetic reduced manifest remains deliberately incomplete.")
    reduced_input = replace(
        replay_input,
        scientific_manifest_sha256=reduced_manifest.sha256,
    )
    sources, stages = _stages(reduced_input)
    reduced_baselines = _baselines(reduced_input)[1:]
    metrics = _metric_bundle(reduced_input, stages, reduced_baselines)

    receipt = evaluate_offline_replay_acceptance(
        reduced_input,
        component_sources=sources,
        stages=stages,
        baselines=reduced_baselines,
        metric_bundle=metrics,
        validation_manifest=reduced_manifest,
        external_truth=context,
    )

    assert receipt.offline_science_gate_passed is False
    assert any(reason.startswith("baselines.missing:") for reason in receipt.blocking_reasons)
    assert any(
        reason.startswith("validation.required_gate_missing:")
        for reason in receipt.blocking_reasons
    )


def test_truth_or_input_tamper_is_not_reinterpreted_as_a_metric_failure():
    (
        context,
        manifest,
        replay_input,
        sources,
        stages,
        baselines,
        _,
    ) = _complete_dossier()
    changed_input = replace(
        replay_input,
        frames=(
            replace(replay_input.frames[0], raw_frame_sha256=_sha("tampered")),
            *replay_input.frames[1:],
        ),
    )
    metrics = _metric_bundle(replay_input, stages, baselines)

    receipt = evaluate_offline_replay_acceptance(
        changed_input,
        component_sources=sources,
        stages=stages,
        baselines=baselines,
        metric_bundle=metrics,
        validation_manifest=manifest,
        external_truth=context,
    )

    assert receipt.offline_science_gate_passed is False
    assert "metrics.input_contract_mismatch" in receipt.blocking_reasons
    assert any("input_contract_mismatch" in reason for reason in receipt.blocking_reasons)


def test_module_is_inert_and_not_imported_by_runtime_or_ha():
    module = ROOT / "storm_tracker_worker" / "offline_replay_acceptance.py"
    tree = ast.parse(module.read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    forbidden = ("service", "mqtt", "acquisition", "custom_components")
    assert not any(
        any(part in name.lower() for part in forbidden)
        for name in imported
    )

    for path in (
        ROOT / "storm_tracker_worker" / "service.py",
        ROOT / "storm_tracker_worker" / "worker_first_runtime.py",
        ROOT / "custom_components" / "storm_tracker_v3" / "__init__.py",
    ):
        assert "offline_replay_acceptance" not in path.read_text(
            encoding="utf-8"
        )
