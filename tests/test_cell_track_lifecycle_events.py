"""Focused tests for immutable, caller-identified CellTrack lifecycle events."""
from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.cell_track import CellDetection
from storm_tracker_worker.tracking_core.engine.cell_track_lifecycle import (
    CELL_TRACK_LIFECYCLE_EVENT_VERSION,
    CellTrackLedger,
    CellTrackLifecycleEvent,
    CellTrackLifecyclePolicy,
    CellTrackLineageEvent,
    add_track_to_ledger,
    advance_ledger_from_assignment,
    record_cell_track_birth,
    record_cell_track_death,
    record_cell_track_gap,
    record_cell_track_merge,
    record_cell_track_missed,
    record_cell_track_reacquired,
    record_cell_track_split,
    start_cell_track,
)
from storm_tracker_worker.tracking_core.engine.global_assignment import (
    AssignmentSolution,
)
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalCellDetection,
)


def _cell(
    detection_id: str,
    valid_at: float,
    x_m: float = 0.0,
    y_m: float = 0.0,
) -> CellDetection:
    return CellDetection(
        detection_id=detection_id,
        valid_at=valid_at,
        analysis_grid_id="metric-grid-v1",
        x_m=x_m,
        y_m=y_m,
        position_sigma_m=500.0,
        source="caller-test-radar",
    )


def _hierarchical(
    detection_id: str,
    valid_at: float,
    x_m: float = 0.0,
    y_m: float = 0.0,
) -> HierarchicalCellDetection:
    detection = _cell(detection_id, valid_at, x_m, y_m)
    return HierarchicalCellDetection(
        detection=detection,
        object_kind="rain_shield",
        parent_detection_id=None,
        footprint_pixels=((0, 0),),
        area_m2=1_000_000.0,
        geometric_centroid_x_m=x_m,
        geometric_centroid_y_m=y_m,
        value_weighted_centroid_x_m=x_m,
        value_weighted_centroid_y_m=y_m,
        mean_value=2.0,
        maximum_value=2.0,
        threshold=1.0,
        centroid_method="geometric",
    )


def _policy(*, maximum_gap_frames: int = 3) -> CellTrackLifecyclePolicy:
    return CellTrackLifecyclePolicy(
        policy_id="caller-owned-lifecycle-policy",
        confirmation_observations=2,
        maximum_gap_frames=maximum_gap_frames,
    )


def _legacy_ledger(
    *items: tuple[str, CellDetection],
    policy: CellTrackLifecyclePolicy | None = None,
) -> CellTrackLedger:
    selected_policy = policy or _policy()
    ledger = CellTrackLedger()
    for track_id, detection in items:
        ledger = add_track_to_ledger(
            ledger,
            start_cell_track(
                track_id,
                detection,
                "rain_shield",
                selected_policy,
            ),
        )
    return ledger


def test_birth_event_preserves_exact_caller_identity_time_and_audit_refs():
    original = CellTrackLedger()
    born = record_cell_track_birth(
        original,
        event_id="event:birth:caller-001",
        frame_valid_at=1_000.0,
        track_id="track:caller-001",
        detection=_cell("detection:caller-001", 1_000.0),
        object_kind="rain_shield",
        policy=_policy(),
        reason="unassigned detection selected by caller",
    )

    assert original == CellTrackLedger()
    assert born.revision == 1
    assert born.lifecycle_events is born.lineage_events
    event = born.lifecycle_events[0]
    track = born.track("track:caller-001")
    assert isinstance(event, CellTrackLifecycleEvent)
    assert isinstance(event, CellTrackLineageEvent)
    assert event.event_id == "event:birth:caller-001"
    assert event.event_type == "birth"
    assert event.valid_at == 1_000.0
    assert event.track_ids == ("track:caller-001",)
    assert event.detection_ids == ("detection:caller-001",)
    assert event.reason == "unassigned detection selected by caller"
    assert event.ledger_revision == born.revision
    assert event.track_revisions == (("track:caller-001", track.revision),)
    assert event.parent_track_ids == ()
    assert event.child_track_ids == ()
    assert event.algorithm_version == CELL_TRACK_LIFECYCLE_EVENT_VERSION
    assert track.lifecycle_event_ids == ("event:birth:caller-001",)

    with pytest.raises(FrozenInstanceError):
        event.reason = "mutated"  # type: ignore[misc]


def test_missed_gap_and_extreme_reacquisition_keep_one_track_identity():
    policy = _policy(maximum_gap_frames=3)
    born = record_cell_track_birth(
        CellTrackLedger(),
        event_id="event:01:birth",
        frame_valid_at=1_000.0,
        track_id="track:stable",
        detection=_cell("detection:old", 1_000.0),
        object_kind="rain_shield",
        policy=policy,
        reason="caller admitted birth",
    )
    missed = record_cell_track_missed(
        born,
        event_id="event:02:missed",
        frame_valid_at=1_060.0,
        track_id="track:stable",
        policy=policy,
        reason="no assigned detection in frame",
    )
    gapped = record_cell_track_gap(
        missed,
        event_id="event:03:gap",
        frame_valid_at=1_120.0,
        track_id="track:stable",
        policy=policy,
        reason="second observed data gap",
    )
    reacquired = record_cell_track_reacquired(
        gapped,
        event_id="event:04:reacquired",
        frame_valid_at=1_180.0,
        track_id="track:stable",
        detection=_cell("detection:far", 1_180.0, x_m=9_000_000_000.0),
        object_kind="rain_shield",
        policy=policy,
        reason="caller assignment reacquired the same identity",
    )

    track = reacquired.track("track:stable")
    assert tuple(item.event_type for item in reacquired.lifecycle_events) == (
        "birth",
        "missed",
        "gap",
        "reacquired",
    )
    assert tuple(item.valid_at for item in reacquired.lifecycle_events) == (
        1_000.0,
        1_060.0,
        1_120.0,
        1_180.0,
    )
    assert track.track_id == "track:stable"
    assert track.parent_track_ids == ()
    assert track.child_track_ids == ()
    assert track.missed_frames == 0
    assert track.sample_count == 2
    assert track.latest_detection.x_m == 9_000_000_000.0
    assert track.revision == 4
    reacquired_event = reacquired.lifecycle_events[-1]
    assert reacquired_event.track_ids == ("track:stable",)
    assert reacquired_event.parent_track_ids == ()
    assert reacquired_event.child_track_ids == ()
    assert reacquired_event.track_revisions == (("track:stable", 4),)


def test_gap_limit_and_explicit_end_create_auditable_death_events():
    strict_policy = _policy(maximum_gap_frames=0)
    born = record_cell_track_birth(
        CellTrackLedger(),
        event_id="event:birth",
        frame_valid_at=1_000.0,
        track_id="track:gap-death",
        detection=_cell("detection:gap-death", 1_000.0),
        object_kind="rain_shield",
        policy=strict_policy,
        reason="caller admitted birth",
    )
    ended_by_gap = record_cell_track_missed(
        born,
        event_id="event:death:gap",
        frame_valid_at=1_060.0,
        track_id="track:gap-death",
        policy=strict_policy,
        reason="unmatched frame crossed caller gap policy",
    )

    gap_track = ended_by_gap.track("track:gap-death")
    gap_event = ended_by_gap.lifecycle_events[-1]
    assert gap_track.lifecycle_state == "ended"
    assert gap_track.termination_reason == "gap_limit_exceeded"
    assert gap_event.event_type == "death"
    assert gap_event.reason == "unmatched frame crossed caller gap policy"
    assert gap_event.detection_ids == ("detection:gap-death",)
    assert gap_event.ledger_revision == ended_by_gap.revision
    assert gap_event.track_revisions == (("track:gap-death", 2),)

    explicit_start = _legacy_ledger(
        ("track:explicit-death", _cell("detection:explicit", 2_000.0)),
        policy=strict_policy,
    )
    explicit_end = record_cell_track_death(
        explicit_start,
        event_id="event:death:explicit",
        frame_valid_at=2_060.0,
        track_id="track:explicit-death",
        reason="caller declared domain exit",
    )
    explicit_track = explicit_end.track("track:explicit-death")
    explicit_event = explicit_end.lifecycle_events[0]
    assert explicit_track.termination_reason == "caller declared domain exit"
    assert explicit_event.event_type == "death"
    assert explicit_event.valid_at == 2_060.0
    assert explicit_event.reason == "caller declared domain exit"
    assert explicit_event.detection_ids == ("detection:explicit",)


def test_eventful_assignment_commits_birth_miss_and_reacquisition_atomically():
    policy = _policy(maximum_gap_frames=3)
    ledger = _legacy_ledger(
        ("track:a", _cell("old:a", 1_000.0)),
        ("track:b", _cell("old:b", 1_000.0)),
        ("track:c", _cell("old:c", 1_000.0)),
        policy=policy,
    )
    ledger = record_cell_track_gap(
        ledger,
        event_id="event:prior-gap",
        frame_valid_at=1_060.0,
        track_id="track:b",
        policy=policy,
        reason="previous frame gap",
    )
    original = ledger
    solution = AssignmentSolution(
        rank=1,
        total_cost=0.0,
        matches=(("old:a", "new:a"), ("old:b", "new:b")),
        deaths=("old:c",),
        births=("new:d",),
    )
    current = {
        "new:a": _hierarchical("new:a", 1_120.0, 100.0),
        "new:b": _hierarchical("new:b", 1_120.0, 8_000_000_000.0),
        "new:d": _hierarchical("new:d", 1_120.0, -5_000.0),
    }

    advanced = advance_ledger_from_assignment(
        ledger,
        solution,
        current,
        {"new:d": "track:d"},
        policy,
        frame_valid_at=1_120.0,
        birth_event_ids={"new:d": "event:birth:d"},
        missed_event_ids={"old:c": "event:missed:c"},
        reacquired_event_ids={"new:b": "event:reacquired:b"},
        event_reasons={
            "event:birth:d": "caller assignment birth",
            "event:missed:c": "caller assignment unmatched",
            "event:reacquired:b": "caller assignment same-track reacquisition",
        },
    )

    assert original.revision == 4
    assert original.track("track:b").missed_frames == 1
    assert original.track("track:b").sample_count == 1
    assert advanced.revision == 5
    batch = advanced.lifecycle_events[-3:]
    assert tuple(item.event_id for item in batch) == (
        "event:birth:d",
        "event:missed:c",
        "event:reacquired:b",
    )
    assert {item.ledger_revision for item in batch} == {advanced.revision}
    assert {item.valid_at for item in batch} == {1_120.0}
    assert {
        item.event_id: item.track_revisions
        for item in batch
    } == {
        "event:birth:d": (("track:d", 1),),
        "event:missed:c": (("track:c", 2),),
        "event:reacquired:b": (("track:b", 3),),
    }
    assert advanced.track("track:a").sample_count == 2
    assert advanced.track("track:b").track_id == "track:b"
    assert advanced.track("track:b").parent_track_ids == ()
    assert advanced.track("track:b").child_track_ids == ()
    assert advanced.track("track:b").missed_frames == 0
    assert advanced.track("track:b").latest_detection.x_m == 8_000_000_000.0
    assert advanced.track("track:c").lifecycle_state == "coasting"
    assert advanced.track("track:d").latest_detection.detection_id == "new:d"


def test_assignment_event_context_is_all_or_nothing_and_legacy_call_still_works():
    policy = _policy()
    ledger = _legacy_ledger(
        ("track:one", _cell("old:one", 1_000.0)),
        policy=policy,
    )
    current = {"new:one": _hierarchical("new:one", 1_060.0)}
    solution = AssignmentSolution(
        rank=1,
        total_cost=0.0,
        matches=(("old:one", "new:one"),),
        deaths=(),
        births=(),
    )

    legacy = advance_ledger_from_assignment(
        ledger,
        solution,
        current,
        {},
        policy,
    )
    assert legacy.lifecycle_events == ()
    assert legacy.track("track:one").sample_count == 2

    with pytest.raises(ValueError, match="volledige eventcontext"):
        advance_ledger_from_assignment(
            ledger,
            solution,
            current,
            {},
            policy,
            frame_valid_at=1_060.0,
        )
    assert ledger.track("track:one").sample_count == 1
    assert ledger.lifecycle_events == ()


def test_assignment_rejects_duplicate_ids_wrong_times_and_inexact_reason_maps():
    policy = _policy()
    ledger = _legacy_ledger(
        ("track:old", _cell("old", 1_000.0)),
        policy=policy,
    )
    solution = AssignmentSolution(
        rank=1,
        total_cost=0.0,
        matches=(),
        deaths=("old",),
        births=("new",),
    )
    current = {"new": _hierarchical("new", 1_060.0)}

    with pytest.raises(ValueError, match="nieuw, uniek"):
        advance_ledger_from_assignment(
            ledger,
            solution,
            current,
            {"new": "track:new"},
            policy,
            frame_valid_at=1_060.0,
            birth_event_ids={"new": "event:duplicate"},
            missed_event_ids={"old": "event:duplicate"},
            reacquired_event_ids={},
            event_reasons={"event:duplicate": "ambiguous"},
        )
    with pytest.raises(ValueError, match="exact alle events"):
        advance_ledger_from_assignment(
            ledger,
            solution,
            current,
            {"new": "track:new"},
            policy,
            frame_valid_at=1_060.0,
            birth_event_ids={"new": "event:birth"},
            missed_event_ids={"old": "event:missed"},
            reacquired_event_ids={},
            event_reasons={"event:birth": "only one reason"},
        )
    with pytest.raises(ValueError, match="frame_valid_at delen"):
        advance_ledger_from_assignment(
            ledger,
            solution,
            current,
            {"new": "track:new"},
            policy,
            frame_valid_at=1_061.0,
            birth_event_ids={"new": "event:birth"},
            missed_event_ids={"old": "event:missed"},
            reacquired_event_ids={},
            event_reasons={
                "event:birth": "caller birth",
                "event:missed": "caller miss",
            },
        )
    assert ledger.revision == 1
    assert ledger.lifecycle_events == ()


def test_helpers_reject_missing_reused_nonfinite_or_nonmonotonic_caller_metadata():
    policy = _policy()
    detection = _cell("detection:one", 1_000.0)
    with pytest.raises(ValueError, match="caller event_id"):
        record_cell_track_birth(
            CellTrackLedger(),
            event_id="",
            frame_valid_at=1_000.0,
            track_id="track:one",
            detection=detection,
            object_kind="rain_shield",
            policy=policy,
            reason="caller birth",
        )
    with pytest.raises(ValueError, match="detectietijd"):
        record_cell_track_birth(
            CellTrackLedger(),
            event_id="event:birth",
            frame_valid_at=1_001.0,
            track_id="track:one",
            detection=detection,
            object_kind="rain_shield",
            policy=policy,
            reason="caller birth",
        )
    born = record_cell_track_birth(
        CellTrackLedger(),
        event_id="event:birth",
        frame_valid_at=1_000.0,
        track_id="track:one",
        detection=detection,
        object_kind="rain_shield",
        policy=policy,
        reason="caller birth",
    )
    with pytest.raises(ValueError, match="bestaat al"):
        record_cell_track_missed(
            born,
            event_id="event:birth",
            frame_valid_at=1_060.0,
            track_id="track:one",
            policy=policy,
            reason="caller miss",
        )
    with pytest.raises(ValueError, match="strikt later"):
        record_cell_track_missed(
            born,
            event_id="event:missed",
            frame_valid_at=1_000.0,
            track_id="track:one",
            policy=policy,
            reason="caller miss",
        )
    with pytest.raises(ValueError, match="eindig"):
        record_cell_track_missed(
            born,
            event_id="event:missed",
            frame_valid_at=float("nan"),
            track_id="track:one",
            policy=policy,
            reason="caller miss",
        )
    assert born.revision == 1
    assert tuple(item.event_id for item in born.lifecycle_events) == ("event:birth",)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            lambda event: replace(event, detection_ids=("detection:missing",)),
            "onbekende detectie",
        ),
        (
            lambda event: replace(event, ledger_revision=2),
            "ledger revision",
        ),
        (
            lambda event: replace(
                event,
                track_revisions=(("track:audit", 2),),
            ),
            "track revision",
        ),
    ),
)
def test_ledger_rejects_tampered_event_audit_references(mutation, message):
    ledger = record_cell_track_birth(
        CellTrackLedger(),
        event_id="event:audit",
        frame_valid_at=1_000.0,
        track_id="track:audit",
        detection=_cell("detection:audit", 1_000.0),
        object_kind="rain_shield",
        policy=_policy(),
        reason="caller birth",
    )
    tampered = mutation(ledger.lifecycle_events[0])
    with pytest.raises(ValueError, match=message):
        CellTrackLedger(
            tracks=ledger.tracks,
            lineage_events=(tampered,),
            revision=ledger.revision,
        )


def test_ledger_rejects_event_missing_from_involved_track_revision():
    ledger = record_cell_track_birth(
        CellTrackLedger(),
        event_id="event:audit",
        frame_valid_at=1_000.0,
        track_id="track:audit",
        detection=_cell("detection:audit", 1_000.0),
        object_kind="rain_shield",
        policy=_policy(),
        reason="caller birth",
    )
    detached_track = replace(ledger.tracks[0], lineage_event_ids=())
    with pytest.raises(ValueError, match="ontbreekt op betrokken track"):
        CellTrackLedger(
            tracks=(detached_track,),
            lineage_events=ledger.lifecycle_events,
            revision=ledger.revision,
        )


def test_split_merge_events_include_exact_revision_detection_and_lineage_audit():
    policy = _policy()
    ledger = _legacy_ledger(
        ("track:parent", _cell("detection:parent", 1_000.0)),
        policy=policy,
    )
    child_a = _hierarchical("detection:child-a", 1_060.0, -1_000.0)
    child_b = _hierarchical("detection:child-b", 1_060.0, 1_000.0)
    split = record_cell_track_split(
        ledger,
        event_id="event:split",
        parent_track_id="track:parent",
        children=(("track:child-a", child_a), ("track:child-b", child_b)),
        policy=policy,
    )

    split_event = split.lifecycle_events[0]
    assert split_event.event_type == "split"
    assert split_event.valid_at == 1_060.0
    assert split_event.track_ids == (
        "track:child-a",
        "track:child-b",
        "track:parent",
    )
    assert split_event.detection_ids == (
        "detection:child-a",
        "detection:child-b",
    )
    assert split_event.reason == "split"
    assert split_event.ledger_revision == split.revision
    assert split_event.track_revisions == (
        ("track:child-a", 1),
        ("track:child-b", 1),
        ("track:parent", 2),
    )
    assert split_event.parent_track_ids == ("track:parent",)
    assert split_event.child_track_ids == ("track:child-a", "track:child-b")

    merged_child = _hierarchical("detection:merged", 1_120.0)
    merged = record_cell_track_merge(
        split,
        event_id="event:merge",
        parent_track_ids=("track:child-a", "track:child-b"),
        child_track_id="track:merged",
        child=merged_child,
        policy=policy,
    )
    merge_event = merged.lifecycle_events[-1]
    assert merge_event.event_type == "merge"
    assert merge_event.valid_at == 1_120.0
    assert merge_event.track_ids == (
        "track:child-a",
        "track:child-b",
        "track:merged",
    )
    assert merge_event.detection_ids == ("detection:merged",)
    assert merge_event.reason == "merge"
    assert merge_event.ledger_revision == merged.revision
    assert merge_event.track_revisions == (
        ("track:child-a", 2),
        ("track:child-b", 2),
        ("track:merged", 1),
    )
    assert merge_event.parent_track_ids == ("track:child-a", "track:child-b")
    assert merge_event.child_track_ids == ("track:merged",)


def test_lifecycle_event_code_is_exact_mirror_and_remains_unwired():
    repository = Path(__file__).resolve().parents[1]
    integration = (
        repository
        / "custom_components"
        / "storm_tracker_v3"
        / "engine"
        / "cell_track_lifecycle.py"
    )
    worker = (
        repository
        / "storm_tracker_worker"
        / "tracking_core"
        / "engine"
        / "cell_track_lifecycle.py"
    )
    assert integration.read_bytes() == worker.read_bytes()

    integration_runtime = (
        repository / "custom_components" / "storm_tracker_v3" / "__init__.py"
    ).read_text(encoding="utf-8")
    worker_service = (
        repository / "storm_tracker_worker" / "service.py"
    ).read_text(encoding="utf-8")
    for symbol in (
        "record_cell_track_birth",
        "record_cell_track_missed",
        "record_cell_track_gap",
        "record_cell_track_reacquired",
        "record_cell_track_death",
    ):
        assert symbol not in integration_runtime
        assert symbol not in worker_service
