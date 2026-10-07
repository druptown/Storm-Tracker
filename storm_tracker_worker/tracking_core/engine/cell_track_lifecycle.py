"""Immutable CellTrack lifecycle, gap handling and lineage for shadow mode.

Tracks are never silently deleted. Birth, missed/gap, reacquired, death,
split and merge transitions can be committed as caller-identified immutable
events. Confirmation and gap limits remain caller-owned policy values.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Iterable, Mapping

from .cell_track import CellDetection
from .global_assignment import AssignmentSolution
from .hierarchical_detection import HierarchicalCellDetection


CELL_TRACK_LIFECYCLE_VERSION = "stv3-immutable-celltrack-lifecycle-v1"
CELL_TRACK_LIFECYCLE_EVENT_VERSION = "stv3-celltrack-lifecycle-event-v2"
_ACTIVE_STATES = frozenset(("tentative", "confirmed", "coasting"))
_LIFECYCLE_EVENT_TYPES = frozenset(
    ("birth", "missed", "gap", "reacquired", "death", "split", "merge")
)
_SAME_TRACK_EVENT_TYPES = frozenset(
    ("birth", "missed", "gap", "reacquired", "death")
)


@dataclass(frozen=True, slots=True)
class CellTrackLifecyclePolicy:
    policy_id: str
    confirmation_observations: int
    maximum_gap_frames: int

    def __post_init__(self) -> None:
        if not isinstance(self.policy_id, str) or not self.policy_id:
            raise ValueError("lifecyclebeleid vereist policy_id")
        if (
            isinstance(self.confirmation_observations, bool)
            or not isinstance(self.confirmation_observations, int)
            or self.confirmation_observations <= 0
        ):
            raise ValueError("confirmation_observations moet positief zijn")
        if (
            isinstance(self.maximum_gap_frames, bool)
            or not isinstance(self.maximum_gap_frames, int)
            or self.maximum_gap_frames < 0
        ):
            raise ValueError("maximum_gap_frames moet niet-negatief zijn")


@dataclass(frozen=True, slots=True)
class ImmutableCellTrack:
    track_id: str
    analysis_grid_id: str
    lifecycle_state: str
    detections: tuple[CellDetection, ...]
    observation_kinds: tuple[str, ...]
    missed_frames: int
    parent_track_ids: tuple[str, ...]
    child_track_ids: tuple[str, ...]
    lineage_event_ids: tuple[str, ...]
    termination_reason: str | None
    revision: int
    algorithm_version: str = CELL_TRACK_LIFECYCLE_VERSION

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, tuple)
            for value in (
                self.detections,
                self.observation_kinds,
                self.parent_track_ids,
                self.child_track_ids,
                self.lineage_event_ids,
            )
        ):
            raise TypeError("CellTrack-collecties moeten immutable tuples zijn")
        if not isinstance(self.track_id, str) or not self.track_id:
            raise ValueError("CellTrack vereist track_id")
        if not isinstance(self.analysis_grid_id, str) or not self.analysis_grid_id:
            raise ValueError("CellTrack vereist analysis_grid_id")
        if self.lifecycle_state not in _ACTIVE_STATES | {"ended"}:
            raise ValueError("ongeldige CellTrack lifecycle_state")
        if not self.detections:
            raise ValueError("CellTrack vereist minstens één detectie")
        if len(self.observation_kinds) != len(self.detections):
            raise ValueError("observation_kinds en detections verschillen in lengte")
        if any(item.analysis_grid_id != self.analysis_grid_id for item in self.detections):
            raise ValueError("CellTrack-detecties gebruiken meerdere grids")
        times = tuple(item.valid_at for item in self.detections)
        if any(later <= earlier for earlier, later in zip(times, times[1:])):
            raise ValueError("CellTrack-detectietijden moeten strikt oplopen")
        identifiers = tuple(item.detection_id for item in self.detections)
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("CellTrack bevat dubbele detectie-ID")
        if self.missed_frames < 0 or self.revision < 1:
            raise ValueError("CellTrack counters mogen niet negatief zijn")
        if self.lifecycle_state == "ended" and not self.termination_reason:
            raise ValueError("ended CellTrack vereist termination_reason")
        if self.lifecycle_state != "ended" and self.termination_reason is not None:
            raise ValueError("actieve CellTrack mag geen termination_reason hebben")
        for label, values in (
            ("parent_track_ids", self.parent_track_ids),
            ("child_track_ids", self.child_track_ids),
            ("lineage_event_ids", self.lineage_event_ids),
        ):
            if (
                len(values) != len(set(values))
                or any(not isinstance(item, str) or not item for item in values)
            ):
                raise ValueError(f"{label} moeten unieke niet-lege ID's zijn")
        if self.track_id in self.parent_track_ids or self.track_id in self.child_track_ids:
            raise ValueError("CellTrack kan niet zijn eigen parent of child zijn")

    @property
    def latest_detection(self) -> CellDetection:
        return self.detections[-1]

    @property
    def sample_count(self) -> int:
        return len(self.detections)

    @property
    def lifecycle_event_ids(self) -> tuple[str, ...]:
        """Backward-compatible semantic view of ``lineage_event_ids``."""
        return self.lineage_event_ids


@dataclass(frozen=True, slots=True)
class CellTrackLifecycleEvent:
    event_id: str
    event_type: str
    valid_at: float
    track_ids: tuple[str, ...]
    detection_ids: tuple[str, ...]
    reason: str
    ledger_revision: int
    track_revisions: tuple[tuple[str, int], ...]
    parent_track_ids: tuple[str, ...]
    child_track_ids: tuple[str, ...]
    algorithm_version: str = CELL_TRACK_LIFECYCLE_EVENT_VERSION

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, tuple)
            for value in (
                self.track_ids,
                self.detection_ids,
                self.track_revisions,
                self.parent_track_ids,
                self.child_track_ids,
            )
        ):
            raise TypeError("lifecyclecollecties moeten immutable tuples zijn")
        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValueError("lifecycle event vereist event_id")
        if self.event_type not in _LIFECYCLE_EVENT_TYPES:
            raise ValueError("ongeldig lifecycle event_type")
        if (
            isinstance(self.valid_at, bool)
            or not isinstance(self.valid_at, (int, float))
            or not math.isfinite(self.valid_at)
        ):
            raise ValueError("lifecycle valid_at moet eindig zijn")
        if not isinstance(self.reason, str) or not self.reason:
            raise ValueError("lifecycle event vereist een reden")
        if (
            isinstance(self.ledger_revision, bool)
            or not isinstance(self.ledger_revision, int)
            or self.ledger_revision < 1
        ):
            raise ValueError("lifecycle event vereist positieve ledger revision")
        if self.algorithm_version != CELL_TRACK_LIFECYCLE_EVENT_VERSION:
            raise ValueError("onbekende lifecycle event-algoritmeversie")
        for label, values in (
            ("track_ids", self.track_ids),
            ("parent_track_ids", self.parent_track_ids),
            ("child_track_ids", self.child_track_ids),
        ):
            if (
                values != tuple(sorted(values))
                or len(values) != len(set(values))
                or any(not isinstance(item, str) or not item for item in values)
            ):
                raise ValueError(
                    f"lifecycle {label} moeten gesorteerd, uniek en geldig zijn"
                )
        if (
            len(self.detection_ids) != len(set(self.detection_ids))
            or any(
                not isinstance(item, str) or not item
                for item in self.detection_ids
            )
        ):
            raise ValueError("lifecycle detection_ids moeten uniek en geldig zijn")
        revision_ids: list[str] = []
        for value in self.track_revisions:
            if (
                not isinstance(value, tuple)
                or len(value) != 2
                or not isinstance(value[0], str)
                or not value[0]
                or isinstance(value[1], bool)
                or not isinstance(value[1], int)
                or value[1] < 1
            ):
                raise ValueError("ongeldige lifecycle track revision")
            revision_ids.append(value[0])
        if (
            tuple(revision_ids) != tuple(sorted(revision_ids))
            or len(revision_ids) != len(set(revision_ids))
            or set(revision_ids) != set(self.track_ids)
        ):
            raise ValueError("track_revisions moeten exact alle eventtracks dekken")
        if self.event_type in _SAME_TRACK_EVENT_TYPES:
            if (
                len(self.track_ids) != 1
                or self.parent_track_ids
                or self.child_track_ids
                or len(self.detection_ids) != 1
            ):
                raise ValueError(
                    "same-track lifecycle event vereist één track zonder lineage"
                )
        if self.event_type == "split" and (
            len(self.parent_track_ids) != 1 or len(self.child_track_ids) < 2
        ):
            raise ValueError("split vereist één parent en minstens twee children")
        if self.event_type == "merge" and (
            len(self.parent_track_ids) < 2 or len(self.child_track_ids) != 1
        ):
            raise ValueError("merge vereist minstens twee parents en één child")
        if self.event_type in ("split", "merge") and (
            self.track_ids
            != tuple(sorted(self.parent_track_ids + self.child_track_ids))
        ):
            raise ValueError("lineage event track_ids dekken parent en child niet")
        if self.event_type in ("split", "merge") and (
            len(self.detection_ids) != len(self.child_track_ids)
        ):
            raise ValueError("lineage detection_ids moeten children afdekken")


# Bestaande imports en ledger-veldnaam blijven werken.
CellTrackLineageEvent = CellTrackLifecycleEvent


@dataclass(frozen=True, slots=True)
class CellTrackLedger:
    tracks: tuple[ImmutableCellTrack, ...] = ()
    lineage_events: tuple[CellTrackLifecycleEvent, ...] = ()
    revision: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.tracks, tuple) or not isinstance(
            self.lineage_events,
            tuple,
        ):
            raise TypeError("CellTrackLedger-collecties moeten tuples zijn")
        track_ids = [item.track_id for item in self.tracks]
        event_ids = [item.event_id for item in self.lineage_events]
        if len(track_ids) != len(set(track_ids)):
            raise ValueError("CellTrackLedger bevat dubbele track_id")
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("CellTrackLedger bevat dubbele lineage event_id")
        if self.revision < 0:
            raise ValueError("ledger revision mag niet negatief zijn")
        track_id_set = set(track_ids)
        event_id_set = set(event_ids)
        for event in self.lineage_events:
            if not set(event.track_ids) <= track_id_set:
                raise ValueError("lifecycle event verwijst naar onbekende track")
            if event.ledger_revision > self.revision:
                raise ValueError("event ledger revision ligt na ledger")
            event_tracks = {item: self.track(item) for item in event.track_ids}
            for track_id, revision in event.track_revisions:
                if revision > event_tracks[track_id].revision:
                    raise ValueError("event track revision ligt na track")
            detection_times = {
                detection.detection_id: detection.valid_at
                for track in event_tracks.values()
                for detection in track.detections
            }
            if not set(event.detection_ids) <= set(detection_times):
                raise ValueError("lifecycle event verwijst naar onbekende detectie")
            if event.event_type in ("birth", "reacquired", "split", "merge"):
                if any(
                    detection_times[item] != event.valid_at
                    for item in event.detection_ids
                ):
                    raise ValueError("eventtijd verschilt van detectietijd")
            elif any(
                detection_times[item] > event.valid_at
                for item in event.detection_ids
            ):
                raise ValueError("miss/death verwijst naar toekomstige detectie")
            if any(
                event.event_id not in track.lineage_event_ids
                for track in event_tracks.values()
            ):
                raise ValueError("event ontbreekt op betrokken track")
        for track in self.tracks:
            if not set(track.parent_track_ids + track.child_track_ids) <= track_id_set:
                raise ValueError("CellTrack verwijst naar onbekende lineage-track")
            if not set(track.lineage_event_ids) <= event_id_set:
                raise ValueError("CellTrack verwijst naar onbekend lineage event")

    def track(self, track_id: str) -> ImmutableCellTrack:
        for item in self.tracks:
            if item.track_id == track_id:
                return item
        raise KeyError(track_id)

    @property
    def lifecycle_events(self) -> tuple[CellTrackLifecycleEvent, ...]:
        """Semantic alias; ``lineage_events`` blijft het duurzame v1-veld."""
        return self.lineage_events


def start_cell_track(
    track_id: str,
    detection: CellDetection,
    object_kind: str,
    policy: CellTrackLifecyclePolicy,
    *,
    parent_track_ids: tuple[str, ...] = (),
    lineage_event_ids: tuple[str, ...] = (),
) -> ImmutableCellTrack:
    _validate_policy(policy)
    if not isinstance(detection, CellDetection):
        raise TypeError("trackstart vereist een CellDetection")
    _validate_object_kind(object_kind)
    state = (
        "confirmed"
        if policy.confirmation_observations <= 1
        else "tentative"
    )
    return ImmutableCellTrack(
        track_id=track_id,
        analysis_grid_id=detection.analysis_grid_id,
        lifecycle_state=state,
        detections=(detection,),
        observation_kinds=(object_kind,),
        missed_frames=0,
        parent_track_ids=tuple(sorted(parent_track_ids)),
        child_track_ids=(),
        lineage_event_ids=tuple(sorted(lineage_event_ids)),
        termination_reason=None,
        revision=1,
    )


def append_cell_detection(
    track: ImmutableCellTrack,
    detection: CellDetection,
    object_kind: str,
    policy: CellTrackLifecyclePolicy,
) -> ImmutableCellTrack:
    _validate_policy(policy)
    if track.lifecycle_state == "ended":
        raise ValueError("ended CellTrack kan geen detectie krijgen")
    if not isinstance(detection, CellDetection):
        raise TypeError("trackupdate vereist een CellDetection")
    _validate_object_kind(object_kind)
    if detection.analysis_grid_id != track.analysis_grid_id:
        raise ValueError("trackupdate gebruikt een ander analysis grid")
    if detection.valid_at <= track.latest_detection.valid_at:
        raise ValueError("trackupdate moet meteorologisch later zijn")
    state = (
        "confirmed"
        if track.sample_count + 1 >= policy.confirmation_observations
        else "tentative"
    )
    return replace(
        track,
        lifecycle_state=state,
        detections=track.detections + (detection,),
        observation_kinds=track.observation_kinds + (object_kind,),
        missed_frames=0,
        termination_reason=None,
        revision=track.revision + 1,
    )


def mark_cell_track_missed(
    track: ImmutableCellTrack,
    policy: CellTrackLifecyclePolicy,
) -> ImmutableCellTrack:
    _validate_policy(policy)
    if track.lifecycle_state == "ended":
        raise ValueError("ended CellTrack kan geen nieuwe miss krijgen")
    missed = track.missed_frames + 1
    if missed > policy.maximum_gap_frames:
        state = "ended"
        reason = "gap_limit_exceeded"
    else:
        state = "coasting"
        reason = None
    return replace(
        track,
        lifecycle_state=state,
        missed_frames=missed,
        termination_reason=reason,
        revision=track.revision + 1,
    )


def end_cell_track(
    track: ImmutableCellTrack,
    reason: str,
) -> ImmutableCellTrack:
    if track.lifecycle_state == "ended":
        raise ValueError("CellTrack is al beëindigd")
    if not isinstance(reason, str) or not reason:
        raise ValueError("trackeinde vereist een reden")
    return replace(
        track,
        lifecycle_state="ended",
        termination_reason=reason,
        revision=track.revision + 1,
    )


def add_track_to_ledger(
    ledger: CellTrackLedger,
    track: ImmutableCellTrack,
) -> CellTrackLedger:
    if any(item.track_id == track.track_id for item in ledger.tracks):
        raise ValueError("track_id bestaat al in ledger")
    return replace(
        ledger,
        tracks=tuple(sorted(ledger.tracks + (track,), key=lambda item: item.track_id)),
        revision=ledger.revision + 1,
    )


def record_cell_track_birth(
    ledger: CellTrackLedger,
    *,
    event_id: str,
    frame_valid_at: float,
    track_id: str,
    detection: CellDetection,
    object_kind: str,
    policy: CellTrackLifecyclePolicy,
    reason: str,
) -> CellTrackLedger:
    """Atomically add one caller-identified track and its birth event."""
    _validate_event_metadata(ledger, event_id, frame_valid_at, reason)
    if not isinstance(detection, CellDetection):
        raise TypeError("birth vereist een CellDetection")
    if detection.valid_at != frame_valid_at:
        raise ValueError("birth eventtijd moet de detectietijd zijn")
    track = start_cell_track(
        track_id,
        detection,
        object_kind,
        policy,
        lineage_event_ids=(event_id,),
    )
    if any(item.track_id == track_id for item in ledger.tracks):
        raise ValueError("birth track_id bestaat al")
    return _commit_single_track_event(
        ledger,
        track=track,
        event_id=event_id,
        event_type="birth",
        frame_valid_at=frame_valid_at,
        detection_id=detection.detection_id,
        reason=reason,
        add=True,
    )


def record_cell_track_missed(
    ledger: CellTrackLedger,
    *,
    event_id: str,
    frame_valid_at: float,
    track_id: str,
    policy: CellTrackLifecyclePolicy,
    reason: str,
) -> CellTrackLedger:
    """Record one unmatched frame; crossing the gap policy emits death."""
    return _record_cell_track_gap_transition(
        ledger,
        event_id=event_id,
        event_type="missed",
        frame_valid_at=frame_valid_at,
        track_id=track_id,
        policy=policy,
        reason=reason,
    )


def record_cell_track_gap(
    ledger: CellTrackLedger,
    *,
    event_id: str,
    frame_valid_at: float,
    track_id: str,
    policy: CellTrackLifecyclePolicy,
    reason: str,
) -> CellTrackLedger:
    """Semantic alias that records a gap event instead of a missed event."""
    return _record_cell_track_gap_transition(
        ledger,
        event_id=event_id,
        event_type="gap",
        frame_valid_at=frame_valid_at,
        track_id=track_id,
        policy=policy,
        reason=reason,
    )


def record_cell_track_reacquired(
    ledger: CellTrackLedger,
    *,
    event_id: str,
    frame_valid_at: float,
    track_id: str,
    detection: CellDetection,
    object_kind: str,
    policy: CellTrackLifecyclePolicy,
    reason: str,
) -> CellTrackLedger:
    """Append a post-gap detection without creating a new track identity."""
    _validate_event_metadata(ledger, event_id, frame_valid_at, reason)
    track = ledger.track(track_id)
    if track.lifecycle_state != "coasting" or track.missed_frames < 1:
        raise ValueError("reacquired vereist een coasting track met een gap")
    if not isinstance(detection, CellDetection):
        raise TypeError("reacquired vereist een CellDetection")
    if detection.valid_at != frame_valid_at:
        raise ValueError("reacquired eventtijd moet de detectietijd zijn")
    _validate_frame_after_track_history(ledger, track, frame_valid_at)
    updated = append_cell_detection(
        track,
        detection,
        object_kind,
        policy,
    )
    updated = replace(
        updated,
        lineage_event_ids=tuple(
            sorted(updated.lineage_event_ids + (event_id,))
        ),
    )
    return _commit_single_track_event(
        ledger,
        track=updated,
        event_id=event_id,
        event_type="reacquired",
        frame_valid_at=frame_valid_at,
        detection_id=detection.detection_id,
        reason=reason,
    )


def record_cell_track_death(
    ledger: CellTrackLedger,
    *,
    event_id: str,
    frame_valid_at: float,
    track_id: str,
    reason: str,
) -> CellTrackLedger:
    """Atomically end one track at a caller-supplied frame time."""
    _validate_event_metadata(ledger, event_id, frame_valid_at, reason)
    track = ledger.track(track_id)
    _validate_frame_after_track_history(ledger, track, frame_valid_at)
    ended = end_cell_track(track, reason)
    ended = replace(
        ended,
        lineage_event_ids=tuple(
            sorted(ended.lineage_event_ids + (event_id,))
        ),
    )
    return _commit_single_track_event(
        ledger,
        track=ended,
        event_id=event_id,
        event_type="death",
        frame_valid_at=frame_valid_at,
        detection_id=track.latest_detection.detection_id,
        reason=reason,
    )


def advance_ledger_from_assignment(
    ledger: CellTrackLedger,
    solution: AssignmentSolution,
    current_detections: Mapping[str, HierarchicalCellDetection],
    birth_track_ids: Mapping[str, str],
    policy: CellTrackLifecyclePolicy,
    *,
    frame_valid_at: float | None = None,
    birth_event_ids: Mapping[str, str] | None = None,
    missed_event_ids: Mapping[str, str] | None = None,
    reacquired_event_ids: Mapping[str, str] | None = None,
    event_reasons: Mapping[str, str] | None = None,
) -> CellTrackLedger:
    """Apply one complete 1:1 assignment without mutating the input ledger.

    The original five-argument call remains eventless for replay compatibility.
    Supplying any event argument activates strict event mode: the caller must
    then provide the frame time and an exact event-ID/reason mapping for every
    birth, unmatched track and post-gap reacquisition in this assignment.
    """

    if not isinstance(solution, AssignmentSolution):
        raise TypeError("ledgeradvance vereist AssignmentSolution")
    _validate_policy(policy)
    current_ids = {current_id for _previous_id, current_id in solution.matches}
    current_ids.update(solution.births)
    if set(current_detections) != current_ids:
        raise ValueError("current_detections dekken assignment niet exact")
    if set(birth_track_ids) != set(solution.births):
        raise ValueError("birth_track_ids dekken births niet exact")
    latest_to_track: dict[str, ImmutableCellTrack] = {}
    for track in ledger.tracks:
        if track.lifecycle_state in _ACTIVE_STATES:
            latest_id = track.latest_detection.detection_id
            if latest_id in latest_to_track:
                raise ValueError("meerdere tracks claimen dezelfde latest detection")
            latest_to_track[latest_id] = track
    referenced_previous = {
        previous_id for previous_id, _current_id in solution.matches
    } | set(solution.deaths)
    if referenced_previous != set(latest_to_track):
        raise ValueError(
            "assignment moet exact alle actieve latest trackdetecties afdekken"
        )
    new_track_ids = tuple(birth_track_ids.values())
    if (
        len(new_track_ids) != len(set(new_track_ids))
        or any(not isinstance(item, str) or not item for item in new_track_ids)
        or set(new_track_ids) & {item.track_id for item in ledger.tracks}
    ):
        raise ValueError("birth track-ID's moeten nieuw, uniek en niet-leeg zijn")

    event_arguments = (
        frame_valid_at,
        birth_event_ids,
        missed_event_ids,
        reacquired_event_ids,
        event_reasons,
    )
    eventful = any(item is not None for item in event_arguments)
    birth_ids: Mapping[str, str] = {}
    missed_ids: Mapping[str, str] = {}
    reacquired_ids: Mapping[str, str] = {}
    reasons: Mapping[str, str] = {}
    if eventful:
        if (
            frame_valid_at is None
            or birth_event_ids is None
            or missed_event_ids is None
            or reacquired_event_ids is None
            or event_reasons is None
        ):
            raise ValueError("eventful assignment vereist volledige eventcontext")
        for label, value in (
            ("birth_event_ids", birth_event_ids),
            ("missed_event_ids", missed_event_ids),
            ("reacquired_event_ids", reacquired_event_ids),
            ("event_reasons", event_reasons),
        ):
            if not isinstance(value, Mapping):
                raise TypeError(f"{label} moet een mapping zijn")
        _validate_frame_valid_at(frame_valid_at)
        expected_reacquired = {
            current_id
            for previous_id, current_id in solution.matches
            if latest_to_track[previous_id].missed_frames > 0
        }
        if set(birth_event_ids) != set(solution.births):
            raise ValueError("birth_event_ids dekken births niet exact")
        if set(missed_event_ids) != set(solution.deaths):
            raise ValueError("missed_event_ids dekken unmatched tracks niet exact")
        if set(reacquired_event_ids) != expected_reacquired:
            raise ValueError("reacquired_event_ids dekken heropnames niet exact")
        all_event_ids = tuple(
            birth_event_ids.values()
        ) + tuple(missed_event_ids.values()) + tuple(
            reacquired_event_ids.values()
        )
        if (
            len(all_event_ids) != len(set(all_event_ids))
            or any(
                not isinstance(item, str) or not item
                for item in all_event_ids
            )
            or set(all_event_ids)
            & {item.event_id for item in ledger.lineage_events}
        ):
            raise ValueError("assignment event-ID's moeten nieuw, uniek en geldig zijn")
        if set(event_reasons) != set(all_event_ids) or any(
            not isinstance(item, str) or not item
            for item in event_reasons.values()
        ):
            raise ValueError("event_reasons moeten exact alle events verklaren")
        if any(
            item.detection.valid_at != frame_valid_at
            for item in current_detections.values()
        ):
            raise ValueError("assignmentdetecties moeten frame_valid_at delen")
        for track in latest_to_track.values():
            _validate_frame_after_track_history(
                ledger,
                track,
                frame_valid_at,
            )
        birth_ids = birth_event_ids
        missed_ids = missed_event_ids
        reacquired_ids = reacquired_event_ids
        reasons = event_reasons

    replacements: dict[str, ImmutableCellTrack] = {}
    for previous_id, current_id in solution.matches:
        old_track = latest_to_track[previous_id]
        detected = current_detections[current_id]
        updated = append_cell_detection(
            old_track,
            detected.detection,
            detected.object_kind,
            policy,
        )
        if current_id in reacquired_ids:
            updated = replace(
                updated,
                lineage_event_ids=tuple(
                    sorted(
                        updated.lineage_event_ids
                        + (reacquired_ids[current_id],)
                    )
                ),
            )
        replacements[old_track.track_id] = updated
    for previous_id in solution.deaths:
        old_track = latest_to_track[previous_id]
        updated = mark_cell_track_missed(
            old_track,
            policy,
        )
        if previous_id in missed_ids:
            updated = replace(
                updated,
                lineage_event_ids=tuple(
                    sorted(
                        updated.lineage_event_ids
                        + (missed_ids[previous_id],)
                    )
                ),
            )
        replacements[old_track.track_id] = updated
    tracks = tuple(
        replacements.get(item.track_id, item) for item in ledger.tracks
    )
    birth_tracks: list[ImmutableCellTrack] = []
    for current_id in solution.births:
        detected = current_detections[current_id]
        birth_tracks.append(
            start_cell_track(
                birth_track_ids[current_id],
                detected.detection,
                detected.object_kind,
                policy,
                lineage_event_ids=(
                    (birth_ids[current_id],)
                    if current_id in birth_ids
                    else ()
                ),
            ),
        )
    final_tracks = tuple(
        sorted(
            tracks + tuple(birth_tracks),
            key=lambda item: item.track_id,
        )
    )
    lifecycle_events: list[CellTrackLifecycleEvent] = []
    if eventful:
        next_ledger_revision = ledger.revision + 1
        final_by_id = {item.track_id: item for item in final_tracks}
        for current_id, event_id in birth_ids.items():
            track_id = birth_track_ids[current_id]
            track = final_by_id[track_id]
            lifecycle_events.append(
                _same_track_event(
                    event_id=event_id,
                    event_type="birth",
                    frame_valid_at=frame_valid_at,
                    track=track,
                    detection_id=current_id,
                    reason=reasons[event_id],
                    ledger_revision=next_ledger_revision,
                )
            )
        for previous_id, event_id in missed_ids.items():
            old_track = latest_to_track[previous_id]
            track = final_by_id[old_track.track_id]
            lifecycle_events.append(
                _same_track_event(
                    event_id=event_id,
                    event_type=(
                        "death"
                        if track.lifecycle_state == "ended"
                        else "missed"
                    ),
                    frame_valid_at=frame_valid_at,
                    track=track,
                    detection_id=previous_id,
                    reason=reasons[event_id],
                    ledger_revision=next_ledger_revision,
                )
            )
        current_to_previous = {
            current_id: previous_id
            for previous_id, current_id in solution.matches
        }
        for current_id, event_id in reacquired_ids.items():
            track = final_by_id[
                latest_to_track[current_to_previous[current_id]].track_id
            ]
            lifecycle_events.append(
                _same_track_event(
                    event_id=event_id,
                    event_type="reacquired",
                    frame_valid_at=frame_valid_at,
                    track=track,
                    detection_id=current_id,
                    reason=reasons[event_id],
                    ledger_revision=next_ledger_revision,
                )
            )
    return CellTrackLedger(
        tracks=final_tracks,
        lineage_events=ledger.lineage_events
        + tuple(sorted(lifecycle_events, key=lambda item: item.event_id)),
        revision=ledger.revision + 1,
    )


def record_cell_track_split(
    ledger: CellTrackLedger,
    *,
    event_id: str,
    parent_track_id: str,
    children: Iterable[tuple[str, HierarchicalCellDetection]],
    policy: CellTrackLifecyclePolicy,
) -> CellTrackLedger:
    _validate_policy(policy)
    child_values = tuple(children)
    if len(child_values) < 2:
        raise ValueError("split vereist minstens twee child tracks")
    parent = ledger.track(parent_track_id)
    if parent.lifecycle_state == "ended":
        raise ValueError("split parent is al beëindigd")
    _validate_new_children(ledger, child_values)
    if any(
        item.detection.analysis_grid_id != parent.analysis_grid_id
        for _track_id, item in child_values
    ):
        raise ValueError("split children moeten het parent analysis grid delen")
    valid_times = {item.detection.valid_at for _track_id, item in child_values}
    if len(valid_times) != 1 or next(iter(valid_times)) <= parent.latest_detection.valid_at:
        raise ValueError("split children moeten één latere geldigheidstijd delen")
    valid_at = next(iter(valid_times))
    _validate_event_metadata(ledger, event_id, valid_at, "split")
    _validate_frame_after_track_history(ledger, parent, valid_at)
    child_ids = tuple(sorted(track_id for track_id, _item in child_values))
    ended_parent = replace(
        end_cell_track(parent, "split"),
        child_track_ids=child_ids,
        lineage_event_ids=tuple(sorted(parent.lineage_event_ids + (event_id,))),
    )
    tracks = tuple(
        ended_parent if item.track_id == parent_track_id else item
        for item in ledger.tracks
    )
    for child_track_id, detected in sorted(child_values):
        tracks += (
            start_cell_track(
                child_track_id,
                detected.detection,
                detected.object_kind,
                policy,
                parent_track_ids=(parent_track_id,),
                lineage_event_ids=(event_id,),
            ),
        )
    tracks = tuple(sorted(tracks, key=lambda item: item.track_id))
    event_tracks = {
        item.track_id: item
        for item in tracks
        if item.track_id == parent_track_id or item.track_id in child_ids
    }
    event = CellTrackLifecycleEvent(
        event_id=event_id,
        event_type="split",
        valid_at=valid_at,
        track_ids=tuple(sorted((parent_track_id, *child_ids))),
        detection_ids=tuple(
            item.detection.detection_id
            for _track_id, item in sorted(child_values)
        ),
        reason="split",
        ledger_revision=ledger.revision + 1,
        track_revisions=tuple(
            (track_id, event_tracks[track_id].revision)
            for track_id in sorted(event_tracks)
        ),
        parent_track_ids=(parent_track_id,),
        child_track_ids=child_ids,
    )
    return CellTrackLedger(
        tracks=tracks,
        lineage_events=ledger.lineage_events + (event,),
        revision=ledger.revision + 1,
    )


def record_cell_track_merge(
    ledger: CellTrackLedger,
    *,
    event_id: str,
    parent_track_ids: Iterable[str],
    child_track_id: str,
    child: HierarchicalCellDetection,
    policy: CellTrackLifecyclePolicy,
) -> CellTrackLedger:
    _validate_policy(policy)
    if not isinstance(child, HierarchicalCellDetection):
        raise TypeError("merge child vereist HierarchicalCellDetection")
    parent_ids = tuple(sorted(parent_track_ids))
    if len(parent_ids) < 2 or len(parent_ids) != len(set(parent_ids)):
        raise ValueError("merge vereist minstens twee unieke parent tracks")
    if any(item.track_id == child_track_id for item in ledger.tracks):
        raise ValueError("merge child track_id bestaat al")
    parents = tuple(ledger.track(item) for item in parent_ids)
    if any(item.lifecycle_state == "ended" for item in parents):
        raise ValueError("merge parent is al beëindigd")
    if any(
        child.detection.valid_at <= item.latest_detection.valid_at
        for item in parents
    ):
        raise ValueError("merge child moet later zijn dan alle parents")
    parent_grids = {item.analysis_grid_id for item in parents}
    if parent_grids != {child.detection.analysis_grid_id}:
        raise ValueError("merge parents en child moeten één analysis grid delen")
    _validate_event_metadata(
        ledger,
        event_id,
        child.detection.valid_at,
        "merge",
    )
    for parent in parents:
        _validate_frame_after_track_history(
            ledger,
            parent,
            child.detection.valid_at,
        )
    parent_set = set(parent_ids)
    tracks = tuple(
        replace(
            end_cell_track(item, "merge"),
            child_track_ids=(child_track_id,),
            lineage_event_ids=tuple(
                sorted(item.lineage_event_ids + (event_id,))
            ),
        )
        if item.track_id in parent_set
        else item
        for item in ledger.tracks
    )
    tracks += (
        start_cell_track(
            child_track_id,
            child.detection,
            child.object_kind,
            policy,
            parent_track_ids=parent_ids,
            lineage_event_ids=(event_id,),
        ),
    )
    tracks = tuple(sorted(tracks, key=lambda item: item.track_id))
    event_track_ids = tuple(sorted((*parent_ids, child_track_id)))
    event_tracks = {
        item.track_id: item
        for item in tracks
        if item.track_id in event_track_ids
    }
    event = CellTrackLifecycleEvent(
        event_id=event_id,
        event_type="merge",
        valid_at=child.detection.valid_at,
        track_ids=event_track_ids,
        detection_ids=(child.detection.detection_id,),
        reason="merge",
        ledger_revision=ledger.revision + 1,
        track_revisions=tuple(
            (track_id, event_tracks[track_id].revision)
            for track_id in sorted(event_tracks)
        ),
        parent_track_ids=parent_ids,
        child_track_ids=(child_track_id,),
    )
    return CellTrackLedger(
        tracks=tracks,
        lineage_events=ledger.lineage_events + (event,),
        revision=ledger.revision + 1,
    )


def _record_cell_track_gap_transition(
    ledger: CellTrackLedger,
    *,
    event_id: str,
    event_type: str,
    frame_valid_at: float,
    track_id: str,
    policy: CellTrackLifecyclePolicy,
    reason: str,
) -> CellTrackLedger:
    _validate_policy(policy)
    _validate_event_metadata(ledger, event_id, frame_valid_at, reason)
    track = ledger.track(track_id)
    _validate_frame_after_track_history(ledger, track, frame_valid_at)
    updated = mark_cell_track_missed(track, policy)
    updated = replace(
        updated,
        lineage_event_ids=tuple(
            sorted(updated.lineage_event_ids + (event_id,))
        ),
    )
    return _commit_single_track_event(
        ledger,
        track=updated,
        event_id=event_id,
        event_type=(
            "death" if updated.lifecycle_state == "ended" else event_type
        ),
        frame_valid_at=frame_valid_at,
        detection_id=track.latest_detection.detection_id,
        reason=reason,
    )


def _commit_single_track_event(
    ledger: CellTrackLedger,
    *,
    track: ImmutableCellTrack,
    event_id: str,
    event_type: str,
    frame_valid_at: float,
    detection_id: str,
    reason: str,
    add: bool = False,
) -> CellTrackLedger:
    next_revision = ledger.revision + 1
    event = _same_track_event(
        event_id=event_id,
        event_type=event_type,
        frame_valid_at=frame_valid_at,
        track=track,
        detection_id=detection_id,
        reason=reason,
        ledger_revision=next_revision,
    )
    if add:
        tracks = ledger.tracks + (track,)
    else:
        if all(item.track_id != track.track_id for item in ledger.tracks):
            raise ValueError("eventtrack ontbreekt in ledger")
        tracks = tuple(
            track if item.track_id == track.track_id else item
            for item in ledger.tracks
        )
    return CellTrackLedger(
        tracks=tuple(sorted(tracks, key=lambda item: item.track_id)),
        lineage_events=ledger.lineage_events + (event,),
        revision=next_revision,
    )


def _same_track_event(
    *,
    event_id: str,
    event_type: str,
    frame_valid_at: float,
    track: ImmutableCellTrack,
    detection_id: str,
    reason: str,
    ledger_revision: int,
) -> CellTrackLifecycleEvent:
    return CellTrackLifecycleEvent(
        event_id=event_id,
        event_type=event_type,
        valid_at=frame_valid_at,
        track_ids=(track.track_id,),
        detection_ids=(detection_id,),
        reason=reason,
        ledger_revision=ledger_revision,
        track_revisions=((track.track_id, track.revision),),
        parent_track_ids=(),
        child_track_ids=(),
    )


def _validate_event_metadata(
    ledger: CellTrackLedger,
    event_id: str,
    frame_valid_at: float,
    reason: str,
) -> None:
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("lifecycle event vereist caller event_id")
    if any(item.event_id == event_id for item in ledger.lineage_events):
        raise ValueError("lifecycle event_id bestaat al")
    _validate_frame_valid_at(frame_valid_at)
    if not isinstance(reason, str) or not reason:
        raise ValueError("lifecycle event vereist een reden")


def _validate_frame_valid_at(frame_valid_at: float) -> None:
    if (
        isinstance(frame_valid_at, bool)
        or not isinstance(frame_valid_at, (int, float))
        or not math.isfinite(frame_valid_at)
    ):
        raise ValueError("frame_valid_at moet eindig zijn")


def _validate_frame_after_track_history(
    ledger: CellTrackLedger,
    track: ImmutableCellTrack,
    frame_valid_at: float,
) -> None:
    _validate_frame_valid_at(frame_valid_at)
    event_times = [
        item.valid_at
        for item in ledger.lineage_events
        if track.track_id in item.track_ids
    ]
    last_known_time = max(
        (track.latest_detection.valid_at, *event_times),
    )
    if frame_valid_at <= last_known_time:
        raise ValueError("lifecycle frame moet strikt later zijn dan trackhistorie")


def _validate_object_kind(value: str) -> None:
    if value not in ("rain_shield", "embedded_core"):
        raise ValueError("object_kind moet rain_shield of embedded_core zijn")


def _validate_policy(value: CellTrackLifecyclePolicy) -> None:
    if not isinstance(value, CellTrackLifecyclePolicy):
        raise TypeError("CellTrack lifecycle vereist CellTrackLifecyclePolicy")


def _validate_new_children(
    ledger: CellTrackLedger,
    values: tuple[tuple[str, HierarchicalCellDetection], ...],
) -> None:
    track_ids = [track_id for track_id, _item in values]
    if (
        len(track_ids) != len(set(track_ids))
        or any(not isinstance(track_id, str) or not track_id for track_id in track_ids)
    ):
        raise ValueError("child track-ID's moeten uniek en niet-leeg zijn")
    existing = {item.track_id for item in ledger.tracks}
    if existing & set(track_ids):
        raise ValueError("child track_id bestaat al")
    if any(
        not isinstance(item, HierarchicalCellDetection)
        for _track_id, item in values
    ):
        raise TypeError("lineage children vereisen HierarchicalCellDetection")
