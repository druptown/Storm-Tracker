"""Tests for leakage-safe event splits and typed external label contracts."""
from __future__ import annotations

import pytest

from storm_tracker_worker.validation_corpus import (
    CellIdentityLabel,
    CellPositionLabel,
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
    PassageLabel,
    WetDryLabel,
    WetDryState,
    build_event_held_out_split,
)


def _policy() -> EventSplitPolicy:
    return EventSplitPolicy(
        policy_id="synthetic-test-policy",
        hash_salt="synthetic-test-salt",
        train_basis_points=6000,
        validation_basis_points=2000,
        test_basis_points=2000,
    )


def _frames() -> tuple[EventFrame, ...]:
    return (
        EventFrame("frame-a1", "event-a", 1.0, ("family-ab",)),
        EventFrame("frame-a2", "event-a", 2.0, ("family-ab",)),
        EventFrame(
            "frame-b1",
            "event-b",
            3.0,
            ("family-ab", "handoff-bc"),
        ),
        EventFrame("frame-c1", "event-c", 4.0, ("handoff-bc",)),
        EventFrame("frame-d1", "event-d", 5.0, ()),
    )


def _source(
    *,
    source_id: str = "synthetic-label-source",
    digest: str = "a" * 64,
    kind: ExternalSourceKind = (
        ExternalSourceKind.BLINDED_HUMAN_CONSENSUS
    ),
    independent: bool = True,
    blinded: bool = True,
) -> ExternalLabelSource:
    return ExternalLabelSource(
        source_id=source_id,
        source_kind=kind,
        producer="synthetic-test-producer",
        dataset_version="synthetic-v1",
        methodology_id="synthetic-method-v1",
        source_artifact_sha256=digest,
        candidate_independent=independent,
        blinded_to_candidate=blinded,
    )


def test_split_is_order_independent_and_keeps_transitive_groups_together():
    first = build_event_held_out_split(_frames(), _policy())
    second = build_event_held_out_split(reversed(_frames()), _policy())

    assert first.to_dict() == second.to_dict()
    assert first.sha256 == second.sha256
    event_partitions = dict(first.event_partitions)
    assert {
        event_partitions["event-a"],
        event_partitions["event-b"],
        event_partitions["event-c"],
    } == {event_partitions["event-a"]}
    assert dict(first.frame_partitions)["frame-a1"] is (
        dict(first.frame_partitions)["frame-a2"]
    )
    component_ids = dict(first.event_components)
    assert {
        component_ids["event-a"],
        component_ids["event-b"],
        component_ids["event-c"],
    } == {component_ids["event-a"]}


def test_split_has_no_default_ratios_and_rejects_invalid_weights():
    with pytest.raises(TypeError):
        EventSplitPolicy(  # type: ignore[call-arg]
            policy_id="missing-ratios",
            hash_salt="synthetic",
        )
    with pytest.raises(ValueError, match="10000"):
        EventSplitPolicy(
            policy_id="bad-ratios",
            hash_salt="synthetic",
            train_basis_points=6000,
            validation_basis_points=2000,
            test_basis_points=1999,
        )
    with pytest.raises(ValueError, match="niet-ondersteund"):
        EventSplitPolicy(
            policy_id="future-algorithm",
            hash_salt="synthetic",
            train_basis_points=6000,
            validation_basis_points=2000,
            test_basis_points=2000,
            algorithm_version="unimplemented-v2",
        )


def test_split_rejects_empty_corpus_and_duplicate_frame_identity():
    with pytest.raises(ValueError, match="leeg"):
        build_event_held_out_split((), _policy())
    duplicate = EventFrame("same", "event-a", 1.0, ())
    with pytest.raises(ValueError, match="frame_id"):
        build_event_held_out_split((duplicate, duplicate), _policy())


def test_typed_label_contracts_reject_semantic_substitution():
    with pytest.raises(TypeError, match="CellPositionLabel"):
        ExternalLabel(
            label_id="position-with-identity-payload",
            label_kind=ExternalLabelKind.CELL_POSITION,
            source_id="synthetic-label-source",
            event_id="event-a",
            frame_id="frame-a1",
            state=LabelState.CONFIRMED,
            payload=CellIdentityLabel("cell-a"),
        )
    with pytest.raises(ValueError, match="dry-capable"):
        WetDryLabel(
            site_id="site-a",
            interval_start_utc=1.0,
            interval_end_utc=2.0,
            state=WetDryState.DRY,
            capable_of_dry_detection=False,
        )
    with pytest.raises(ValueError, match="passage_time"):
        PassageLabel(
            tracker_id="tracker-a",
            window_start_utc=1.0,
            window_end_utc=2.0,
            occurred=True,
            passage_time_utc=None,
            time_uncertainty_s=None,
        )


@pytest.mark.parametrize(
    ("kind", "payload"),
    (
        (
            ExternalLabelKind.CELL_IDENTITY,
            CellIdentityLabel("cell-a"),
        ),
        (
            ExternalLabelKind.CELL_POSITION,
            CellPositionLabel("cell-a", 50.0, 4.0, None),
        ),
        (
            ExternalLabelKind.CELL_SPEED,
            CellSpeedLabel("cell-a", 2.0, -1.0, 1.0, 2.0, None),
        ),
        (
            ExternalLabelKind.PASSAGE,
            PassageLabel("tracker-a", 1.0, 2.0, False, None, None),
        ),
        (
            ExternalLabelKind.WET_DRY,
            WetDryLabel(
                "site-a",
                1.0,
                2.0,
                WetDryState.WET,
                True,
                accumulation_mm=0.1,
            ),
        ),
    ),
)
def test_requested_external_label_payloads_are_losslessly_typed(kind, payload):
    label = ExternalLabel(
        label_id=f"label-{kind.value}",
        label_kind=kind,
        source_id="synthetic-label-source",
        event_id="event-a",
        frame_id="frame-a1",
        state=LabelState.CONFIRMED,
        payload=payload,
    )

    assert label.to_dict()["label_kind"] == kind.value
    assert label.to_dict()["payload"] == payload.to_dict()


def test_unblinded_or_engine_derived_source_is_not_external_truth():
    assert _source(blinded=False).is_external_truth_eligible is False
    assert _source(
        kind=ExternalSourceKind.ENGINE_DERIVED_DIAGNOSTIC,
        independent=False,
    ).is_external_truth_eligible is False
    assert _source(
        kind=ExternalSourceKind.IN_SITU_INSTRUMENT,
        blinded=False,
    ).is_external_truth_eligible is True


def test_corpus_rejects_unknown_source_and_context_rejects_wrong_event():
    split = build_event_held_out_split(_frames(), _policy())
    unknown_source_label = ExternalLabel(
        label_id="label-unknown-source",
        label_kind=ExternalLabelKind.CELL_IDENTITY,
        source_id="missing-source",
        event_id="event-a",
        frame_id="frame-a1",
        state=LabelState.CONFIRMED,
        payload=CellIdentityLabel("cell-a"),
    )
    with pytest.raises(ValueError, match="onbekende sources"):
        ExternalLabelCorpus(
            sources=(_source(),),
            labels=(unknown_source_label,),
        )

    wrong_event = ExternalLabel(
        label_id="label-wrong-event",
        label_kind=ExternalLabelKind.CELL_IDENTITY,
        source_id="synthetic-label-source",
        event_id="event-d",
        frame_id="frame-a1",
        state=LabelState.CONFIRMED,
        payload=CellIdentityLabel("cell-a"),
    )
    labels = ExternalLabelCorpus(
        sources=(_source(),),
        labels=(wrong_event,),
    )
    with pytest.raises(ValueError, match="verkeerd event"):
        ExternalTruthContext(split=split, labels=labels)


def test_every_frame_and_group_has_exactly_one_partition():
    split = build_event_held_out_split(_frames(), _policy())

    assert len(dict(split.frame_partitions)) == len(_frames())
    assert all(
        isinstance(partition, CorpusPartition)
        for _, partition in split.frame_partitions
    )
    assert len(dict(split.group_partitions)) == 2
