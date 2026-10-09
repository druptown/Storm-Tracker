"""Immutable shadow-only WeatherSystem grouping above individual CellTracks.

WeatherSystems are secondary views.  Every input track survives exactly once
in the output, including its lineage and identity alternatives.  Grouping uses
only current footprints on one analysis grid, explicit rain-shield membership,
current polygon distance/overlap, and time-aware motion coherence.

System motion is descriptive output.  This module has no API that can write it
back to a CellTrack and it never caps or rejects an absolute track velocity.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import math
from statistics import median
from typing import Iterable

from .analysis_grid import AnalysisGrid


WEATHER_SYSTEM_GROUPING_VERSION = "stv3-weather-system-secondary-grouping-v1"
Pixel = tuple[int, int]


@dataclass(frozen=True, slots=True)
class WeatherSystemGroupingPolicy:
    """Caller-owned grouping limits; this module supplies no product defaults."""

    policy_id: str
    pixel_connectivity: int
    maximum_footprint_gap_m: float
    maximum_motion_difference_m_s: float
    maximum_motion_age_s: float
    maximum_motion_time_difference_s: float
    algorithm_version: str = WEATHER_SYSTEM_GROUPING_VERSION

    def __post_init__(self) -> None:
        _identifier(self.policy_id, "policy_id")
        if self.pixel_connectivity not in (4, 8):
            raise ValueError("pixel_connectivity moet 4 of 8 zijn")
        for field, value in (
            ("maximum_footprint_gap_m", self.maximum_footprint_gap_m),
            (
                "maximum_motion_difference_m_s",
                self.maximum_motion_difference_m_s,
            ),
            ("maximum_motion_age_s", self.maximum_motion_age_s),
            (
                "maximum_motion_time_difference_s",
                self.maximum_motion_time_difference_s,
            ),
        ):
            numeric = _finite(value, field)
            if numeric < 0.0:
                raise ValueError(f"{field} moet niet-negatief zijn")
        if self.algorithm_version != WEATHER_SYSTEM_GROUPING_VERSION:
            raise ValueError("niet-ondersteunde WeatherSystem-algoritmeversie")

    @property
    def fingerprint(self) -> str:
        return _digest_id(
            "weather-system-policy",
            self.policy_id,
            str(self.pixel_connectivity),
            format(self.maximum_footprint_gap_m, ".17g"),
            format(self.maximum_motion_difference_m_s, ".17g"),
            format(self.maximum_motion_age_s, ".17g"),
            format(self.maximum_motion_time_difference_s, ".17g"),
            self.algorithm_version,
        )


@dataclass(frozen=True, slots=True)
class WeatherSystemTrackInput:
    """One current immutable CellTrack view supplied by the tracking layer."""

    track_snapshot_id: str
    track_id: str
    analysis_grid_id: str
    valid_at: float
    current_detection_id: str
    object_kind: str
    current_footprint_pixels: tuple[Pixel, ...]
    rain_shield_detection_ids: tuple[str, ...]
    x_m: float
    y_m: float
    vx_m_s: float | None
    vy_m_s: float | None
    motion_reference_valid_at: float | None
    motion_method: str | None
    lifecycle_state: str
    parent_track_ids: tuple[str, ...] = ()
    child_track_ids: tuple[str, ...] = ()
    lineage_event_ids: tuple[str, ...] = ()
    identity_alternative_track_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field, value in (
            ("track_snapshot_id", self.track_snapshot_id),
            ("track_id", self.track_id),
            ("analysis_grid_id", self.analysis_grid_id),
            ("current_detection_id", self.current_detection_id),
        ):
            _identifier(value, field)
        valid_at = _finite(self.valid_at, "valid_at")
        _finite(self.x_m, "x_m")
        _finite(self.y_m, "y_m")
        if self.object_kind not in ("rain_shield", "embedded_core"):
            raise ValueError("object_kind moet rain_shield of embedded_core zijn")
        if self.lifecycle_state not in ("tentative", "confirmed", "coasting"):
            raise ValueError("WeatherSystem-input vereist een actieve CellTrack")
        _pixels(self.current_footprint_pixels, "current_footprint_pixels")
        _sorted_identifiers(
            self.rain_shield_detection_ids,
            "rain_shield_detection_ids",
            allow_empty=False,
        )
        if (
            self.object_kind == "rain_shield"
            and self.current_detection_id
            not in self.rain_shield_detection_ids
        ):
            raise ValueError(
                "rain-shieldtrack moet zijn actuele detectie als shield bewaren"
            )
        for field, values in (
            ("parent_track_ids", self.parent_track_ids),
            ("child_track_ids", self.child_track_ids),
            ("lineage_event_ids", self.lineage_event_ids),
            (
                "identity_alternative_track_ids",
                self.identity_alternative_track_ids,
            ),
        ):
            _sorted_identifiers(values, field, allow_empty=True)
        if (self.vx_m_s is None) != (self.vy_m_s is None):
            raise ValueError("trackmotion moet beide vectorcomponenten of geen hebben")
        if self.vx_m_s is None:
            if (
                self.motion_reference_valid_at is not None
                or self.motion_method is not None
            ):
                raise ValueError("ontbrekende trackmotion mag geen motionprovenance hebben")
        else:
            _finite(self.vx_m_s, "vx_m_s")
            _finite(self.vy_m_s, "vy_m_s")
            reference = _finite(
                self.motion_reference_valid_at,
                "motion_reference_valid_at",
            )
            if reference > valid_at:
                raise ValueError("motion_reference_valid_at mag niet future zijn")
            _identifier(self.motion_method, "motion_method")

    @property
    def has_motion(self) -> bool:
        return self.vx_m_s is not None


@dataclass(frozen=True, slots=True)
class WeatherSystemConnectionEvidence:
    """Auditable pair decision; uncertain pairs never silently become members."""

    connection_id: str
    left_track_id: str
    right_track_id: str
    shared_rain_shield_detection_ids: tuple[str, ...]
    overlap_pixel_count: int
    footprint_gap_m: float
    valid_time_difference_s: float
    motion_time_difference_s: float | None
    motion_difference_m_s: float | None
    motion_coherence: str
    spatial_relation: str
    decision: str
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.connection_id, "connection_id")
        _identifier(self.left_track_id, "left_track_id")
        _identifier(self.right_track_id, "right_track_id")
        if self.left_track_id >= self.right_track_id:
            raise ValueError("connection track-ID's moeten canoniek gesorteerd zijn")
        _sorted_identifiers(
            self.shared_rain_shield_detection_ids,
            "shared_rain_shield_detection_ids",
            allow_empty=True,
        )
        if (
            isinstance(self.overlap_pixel_count, bool)
            or not isinstance(self.overlap_pixel_count, int)
            or self.overlap_pixel_count < 0
        ):
            raise ValueError("overlap_pixel_count moet niet-negatief geheel zijn")
        for field, value in (
            ("footprint_gap_m", self.footprint_gap_m),
            ("valid_time_difference_s", self.valid_time_difference_s),
        ):
            if _finite(value, field) < 0.0:
                raise ValueError(f"{field} moet niet-negatief zijn")
        for field, value in (
            ("motion_time_difference_s", self.motion_time_difference_s),
            ("motion_difference_m_s", self.motion_difference_m_s),
        ):
            if value is not None and _finite(value, field) < 0.0:
                raise ValueError(f"{field} moet niet-negatief zijn")
        if self.motion_coherence not in (
            "coherent",
            "incoherent",
            "stale",
            "unknown",
        ):
            raise ValueError("ongeldige motion_coherence")
        if self.spatial_relation not in (
            "shared_rain_shield",
            "overlap",
            "touching",
            "near",
            "far",
        ):
            raise ValueError("ongeldige spatial_relation")
        if self.decision not in ("connected", "uncertain", "separate"):
            raise ValueError("ongeldige connection decision")
        _sorted_identifiers(self.reasons, "reasons", allow_empty=False)


@dataclass(frozen=True, slots=True)
class DescriptiveSystemMotion:
    vx_m_s: float | None
    vy_m_s: float | None
    method: str
    supporting_track_ids: tuple[str, ...]
    maximum_member_residual_m_s: float | None

    def __post_init__(self) -> None:
        _identifier(self.method, "descriptive motion method")
        _sorted_identifiers(
            self.supporting_track_ids,
            "supporting_track_ids",
            allow_empty=True,
        )
        if (self.vx_m_s is None) != (self.vy_m_s is None):
            raise ValueError("descriptive motion moet volledig of None zijn")
        if self.vx_m_s is None:
            if self.supporting_track_ids or self.maximum_member_residual_m_s is not None:
                raise ValueError("motion zonder vector mag geen support/residual hebben")
        else:
            _finite(self.vx_m_s, "descriptive vx_m_s")
            _finite(self.vy_m_s, "descriptive vy_m_s")
            if not self.supporting_track_ids:
                raise ValueError("descriptive motion vereist supporting tracks")
            residual = _finite(
                self.maximum_member_residual_m_s,
                "maximum_member_residual_m_s",
            )
            if residual < 0.0:
                raise ValueError("motionresidual moet niet-negatief zijn")


@dataclass(frozen=True, slots=True)
class WeatherSystemPredecessorCandidate:
    previous_system_id: str
    direct_member_track_ids: tuple[str, ...]
    lineage_supported_track_ids: tuple[str, ...]
    selected: bool

    def __post_init__(self) -> None:
        _identifier(self.previous_system_id, "previous_system_id")
        _sorted_identifiers(
            self.direct_member_track_ids,
            "direct_member_track_ids",
            allow_empty=True,
        )
        _sorted_identifiers(
            self.lineage_supported_track_ids,
            "lineage_supported_track_ids",
            allow_empty=True,
        )
        if not self.direct_member_track_ids and not self.lineage_supported_track_ids:
            raise ValueError("predecessorcandidate vereist aantoonbare tracksupport")
        if not isinstance(self.selected, bool):
            raise TypeError("selected moet bool zijn")

    @property
    def support_rank(self) -> tuple[int, int]:
        return (
            len(self.direct_member_track_ids),
            len(self.lineage_supported_track_ids),
        )


@dataclass(frozen=True, slots=True)
class WeatherSystemSnapshot:
    system_snapshot_id: str
    system_id: str
    valid_at: float
    analysis_grid_id: str
    member_tracks: tuple[WeatherSystemTrackInput, ...]
    rain_shield_detection_ids: tuple[str, ...]
    aggregate_current_footprint: tuple[Pixel, ...]
    descriptive_motion: DescriptiveSystemMotion
    classification: str
    system_uncertainty: tuple[str, ...]
    predecessor_candidates: tuple[WeatherSystemPredecessorCandidate, ...]
    uncertain_connection_ids: tuple[str, ...]
    algorithm_version: str = WEATHER_SYSTEM_GROUPING_VERSION

    def __post_init__(self) -> None:
        _identifier(self.system_snapshot_id, "system_snapshot_id")
        _identifier(self.system_id, "system_id")
        _finite(self.valid_at, "valid_at")
        _identifier(self.analysis_grid_id, "analysis_grid_id")
        if not self.member_tracks:
            raise ValueError("WeatherSystem vereist minstens één membertrack")
        if self.member_tracks != tuple(
            sorted(self.member_tracks, key=lambda item: item.track_id)
        ):
            raise ValueError("member_tracks moeten op track_id gesorteerd zijn")
        if len({item.track_id for item in self.member_tracks}) != len(
            self.member_tracks
        ):
            raise ValueError("WeatherSystem bevat dubbele track_id")
        if any(
            item.valid_at != self.valid_at
            or item.analysis_grid_id != self.analysis_grid_id
            for item in self.member_tracks
        ):
            raise ValueError("WeatherSystemmembers moeten exact tijd/grid delen")
        _sorted_identifiers(
            self.rain_shield_detection_ids,
            "rain_shield_detection_ids",
            allow_empty=False,
        )
        expected_shields = tuple(
            sorted(
                {
                    shield_id
                    for track in self.member_tracks
                    for shield_id in track.rain_shield_detection_ids
                }
            )
        )
        if self.rain_shield_detection_ids != expected_shields:
            raise ValueError(
                "system shield-ID's moeten exacte actuele member-union zijn"
            )
        _pixels(
            self.aggregate_current_footprint,
            "aggregate_current_footprint",
        )
        expected_union = tuple(
            sorted(
                {
                    pixel
                    for track in self.member_tracks
                    for pixel in track.current_footprint_pixels
                }
            )
        )
        if self.aggregate_current_footprint != expected_union:
            raise ValueError(
                "aggregate footprint moet exacte actuele member-union zijn"
            )
        if not isinstance(self.descriptive_motion, DescriptiveSystemMotion):
            raise TypeError("descriptive_motion heeft verkeerd type")
        if self.classification != "not_classified":
            raise ValueError("shadow grouping mag geen MCS-classificatie verzinnen")
        _sorted_identifiers(
            self.system_uncertainty,
            "system_uncertainty",
            allow_empty=True,
        )
        if self.predecessor_candidates != tuple(
            sorted(
                self.predecessor_candidates,
                key=lambda item: item.previous_system_id,
            )
        ):
            raise ValueError("predecessor_candidates moeten gesorteerd zijn")
        if len(
            {item.previous_system_id for item in self.predecessor_candidates}
        ) != len(self.predecessor_candidates):
            raise ValueError("predecessor system-ID moet uniek zijn")
        if sum(item.selected for item in self.predecessor_candidates) > 1:
            raise ValueError("WeatherSystem mag maar één predecessor-ID behouden")
        _sorted_identifiers(
            self.uncertain_connection_ids,
            "uncertain_connection_ids",
            allow_empty=True,
        )
        if self.algorithm_version != WEATHER_SYSTEM_GROUPING_VERSION:
            raise ValueError("niet-ondersteunde WeatherSystem snapshotversie")

    @property
    def member_track_ids(self) -> tuple[str, ...]:
        return tuple(item.track_id for item in self.member_tracks)


@dataclass(frozen=True, slots=True)
class WeatherSystemMembershipEvent:
    event_id: str
    event_type: str
    current_system_id: str
    selected_predecessor_system_id: str | None
    predecessor_system_ids: tuple[str, ...]
    alternative_predecessor_system_ids: tuple[str, ...]
    current_member_track_ids: tuple[str, ...]
    added_track_ids: tuple[str, ...]
    removed_track_ids: tuple[str, ...]
    member_lineage_event_ids: tuple[str, ...]
    member_identity_alternative_track_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.event_id, "membership event_id")
        if self.event_type not in (
            "birth",
            "continued",
            "membership_changed",
            "merge",
            "split_retained",
            "split_child",
            "complex_reconfiguration",
        ):
            raise ValueError("ongeldig WeatherSystem membership event_type")
        _identifier(self.current_system_id, "current_system_id")
        if self.selected_predecessor_system_id is not None:
            _identifier(
                self.selected_predecessor_system_id,
                "selected_predecessor_system_id",
            )
        for field, values in (
            ("predecessor_system_ids", self.predecessor_system_ids),
            (
                "alternative_predecessor_system_ids",
                self.alternative_predecessor_system_ids,
            ),
            ("current_member_track_ids", self.current_member_track_ids),
            ("added_track_ids", self.added_track_ids),
            ("removed_track_ids", self.removed_track_ids),
            ("member_lineage_event_ids", self.member_lineage_event_ids),
            (
                "member_identity_alternative_track_ids",
                self.member_identity_alternative_track_ids,
            ),
        ):
            _sorted_identifiers(values, field, allow_empty=True)
        if not self.current_member_track_ids:
            raise ValueError("membership event vereist actuele membertracks")
        if self.selected_predecessor_system_id is not None and (
            self.selected_predecessor_system_id not in self.predecessor_system_ids
        ):
            raise ValueError("selected predecessor ontbreekt uit candidatelijst")
        expected_alternatives = tuple(
            item
            for item in self.predecessor_system_ids
            if item != self.selected_predecessor_system_id
        )
        if self.alternative_predecessor_system_ids != expected_alternatives:
            raise ValueError("predecessoralternatieven zijn niet lossless")
        if not set(self.added_track_ids) <= set(self.current_member_track_ids):
            raise ValueError("added tracks moeten actuele members zijn")
        if set(self.added_track_ids) & set(self.removed_track_ids):
            raise ValueError("track kan niet tegelijk added en removed zijn")


@dataclass(frozen=True, slots=True)
class WeatherSystemGroupingSnapshot:
    grouping_snapshot_id: str
    valid_at: float
    analysis_grid_id: str
    policy: WeatherSystemGroupingPolicy
    input_tracks: tuple[WeatherSystemTrackInput, ...]
    systems: tuple[WeatherSystemSnapshot, ...]
    connections: tuple[WeatherSystemConnectionEvidence, ...]
    membership_events: tuple[WeatherSystemMembershipEvent, ...]
    previous_grouping_snapshot_id: str | None
    retired_system_ids: tuple[str, ...]
    algorithm_version: str = WEATHER_SYSTEM_GROUPING_VERSION

    def __post_init__(self) -> None:
        _identifier(self.grouping_snapshot_id, "grouping_snapshot_id")
        _finite(self.valid_at, "valid_at")
        _identifier(self.analysis_grid_id, "analysis_grid_id")
        if not isinstance(self.policy, WeatherSystemGroupingPolicy):
            raise TypeError("grouping snapshot vereist volledige policy")
        if self.previous_grouping_snapshot_id is not None:
            _identifier(
                self.previous_grouping_snapshot_id,
                "previous_grouping_snapshot_id",
            )
        if self.input_tracks != tuple(
            sorted(self.input_tracks, key=lambda item: item.track_id)
        ):
            raise ValueError("input_tracks moeten gesorteerd zijn")
        input_ids = tuple(item.track_id for item in self.input_tracks)
        if len(input_ids) != len(set(input_ids)):
            raise ValueError("input track_id moet uniek zijn")
        if self.systems != tuple(
            sorted(self.systems, key=lambda item: item.system_id)
        ):
            raise ValueError("systems moeten op system_id gesorteerd zijn")
        if len({item.system_id for item in self.systems}) != len(self.systems):
            raise ValueError("system_id moet uniek zijn")
        if any(
            item.valid_at != self.valid_at
            or item.analysis_grid_id != self.analysis_grid_id
            for item in self.systems
        ):
            raise ValueError("systems moeten groupingtijd en grid delen")
        member_ids = tuple(
            item
            for system in self.systems
            for item in system.member_track_ids
        )
        if sorted(member_ids) != sorted(input_ids):
            raise ValueError("iedere inputtrack moet exact één systemmember blijven")
        if len(member_ids) != len(set(member_ids)):
            raise ValueError("track mag niet in meerdere WeatherSystems zitten")
        if self.connections != tuple(
            sorted(
                self.connections,
                key=lambda item: (item.left_track_id, item.right_track_id),
            )
        ):
            raise ValueError("connections moeten canoniek gesorteerd zijn")
        expected_connections = len(input_ids) * (len(input_ids) - 1) // 2
        if len(self.connections) != expected_connections:
            raise ValueError("iedere trackpair moet expliciete connection evidence hebben")
        expected_pairs = {
            (left, right)
            for index, left in enumerate(input_ids)
            for right in input_ids[index + 1 :]
        }
        actual_pairs = {
            (item.left_track_id, item.right_track_id)
            for item in self.connections
        }
        if actual_pairs != expected_pairs:
            raise ValueError("connection evidence dekt niet exact iedere trackpair")
        if self.membership_events != tuple(
            sorted(
                self.membership_events,
                key=lambda item: item.current_system_id,
            )
        ):
            raise ValueError("membership_events moeten gesorteerd zijn")
        if len(self.membership_events) != len(self.systems):
            raise ValueError("ieder actueel systeem vereist één membership event")
        event_by_system = {
            item.current_system_id: item
            for item in self.membership_events
        }
        if set(event_by_system) != {item.system_id for item in self.systems}:
            raise ValueError("membership events horen niet exact bij actuele systems")
        if any(
            event_by_system[system.system_id].current_member_track_ids
            != system.member_track_ids
            for system in self.systems
        ):
            raise ValueError("membership eventmembers verschillen van systemmembers")
        _sorted_identifiers(
            self.retired_system_ids,
            "retired_system_ids",
            allow_empty=True,
        )
        if set(self.retired_system_ids) & {item.system_id for item in self.systems}:
            raise ValueError("actueel system kan niet tegelijk retired zijn")
        if self.algorithm_version != WEATHER_SYSTEM_GROUPING_VERSION:
            raise ValueError("niet-ondersteunde grouping snapshotversie")

    @property
    def policy_id(self) -> str:
        return self.policy.policy_id


def group_weather_systems(
    *,
    valid_at: float,
    grid: AnalysisGrid,
    tracks: Iterable[WeatherSystemTrackInput],
    policy: WeatherSystemGroupingPolicy,
    previous: WeatherSystemGroupingSnapshot | None = None,
) -> WeatherSystemGroupingSnapshot:
    """Build a deterministic secondary grouping without mutating CellTracks."""

    analysis_time = _finite(valid_at, "valid_at")
    if not isinstance(grid, AnalysisGrid):
        raise TypeError("WeatherSystem grouping vereist AnalysisGrid")
    if not isinstance(policy, WeatherSystemGroupingPolicy):
        raise TypeError("WeatherSystem grouping vereist groupingpolicy")
    ordered = tuple(sorted(tracks, key=lambda item: item.track_id))
    if any(not isinstance(item, WeatherSystemTrackInput) for item in ordered):
        raise TypeError("tracks moeten WeatherSystemTrackInput-records zijn")
    track_ids = tuple(item.track_id for item in ordered)
    snapshot_ids = tuple(item.track_snapshot_id for item in ordered)
    detection_ids = tuple(item.current_detection_id for item in ordered)
    if len(track_ids) != len(set(track_ids)):
        raise ValueError("track_id moet uniek zijn")
    if len(snapshot_ids) != len(set(snapshot_ids)):
        raise ValueError("track_snapshot_id moet uniek zijn")
    if len(detection_ids) != len(set(detection_ids)):
        raise ValueError("current_detection_id moet uniek zijn")
    for track in ordered:
        if track.valid_at != analysis_time:
            raise ValueError(
                "aggregate current footprint vereist exact hetzelfde valid_at"
            )
        if track.analysis_grid_id != grid.grid_id:
            raise ValueError("alle WeatherSystemtracks moeten hetzelfde grid delen")
        for row, column in track.current_footprint_pixels:
            if row >= grid.height or column >= grid.width:
                raise ValueError("trackfootprint bevat pixel buiten AnalysisGrid")
    if previous is not None:
        if not isinstance(previous, WeatherSystemGroupingSnapshot):
            raise TypeError("previous heeft verkeerd snapshottype")
        if previous.valid_at >= analysis_time:
            raise ValueError("previous WeatherSystem-snapshot moet ouder zijn")
        if previous.analysis_grid_id != grid.grid_id:
            raise ValueError("previous WeatherSystem-snapshot gebruikt ander grid")

    connections = tuple(
        _connection(left, right, grid, policy, analysis_time)
        for index, left in enumerate(ordered)
        for right in ordered[index + 1 :]
    )
    components = _connected_components(track_ids, connections)
    previous_candidates = _predecessor_candidates(components, ordered, previous)
    selections = _select_predecessors(previous_candidates)
    previous_use_counts = _previous_candidate_use_counts(previous_candidates)
    track_by_id = {item.track_id: item for item in ordered}
    previous_by_id = (
        {item.system_id: item for item in previous.systems}
        if previous is not None
        else {}
    )
    component_system_ids: dict[tuple[str, ...], str] = {}
    used_system_ids: set[str] = set()
    reserved_previous_ids = set(previous_by_id)
    for component in components:
        selected_previous = selections.get(component)
        if selected_previous is not None:
            system_id = selected_previous
        else:
            component_members = tuple(track_by_id[item] for item in component)
            system_id = _new_system_id(grid.grid_id, component_members)
            if (
                system_id in reserved_previous_ids
                or system_id in used_system_ids
            ):
                system_id = _digest_id(
                    "weather-system",
                    grid.grid_id,
                    "reconfigured-new-identity",
                    format(analysis_time, ".17g"),
                    *component,
                )
        if system_id in used_system_ids:
            raise ValueError("deterministische WeatherSystem-ID collision")
        component_system_ids[component] = system_id
        used_system_ids.add(system_id)

    systems: list[WeatherSystemSnapshot] = []
    events: list[WeatherSystemMembershipEvent] = []
    selected_previous_ids: set[str] = set()
    for component in components:
        members = tuple(track_by_id[item] for item in component)
        selected_previous = selections.get(component)
        system_id = component_system_ids[component]
        if selected_previous is not None:
            selected_previous_ids.add(selected_previous)
        candidates = tuple(
            WeatherSystemPredecessorCandidate(
                previous_system_id=candidate.previous_system_id,
                direct_member_track_ids=candidate.direct_member_track_ids,
                lineage_supported_track_ids=(
                    candidate.lineage_supported_track_ids
                ),
                selected=candidate.previous_system_id == selected_previous,
            )
            for candidate in previous_candidates.get(component, ())
        )
        component_set = set(component)
        component_connection_ids = tuple(
            sorted(
                item.connection_id
                for item in connections
                if item.decision == "uncertain"
                and (
                    item.left_track_id in component
                    or item.right_track_id in component
                )
            )
        )
        strong_connection_conflict = any(
            item.decision == "connected"
            and (
                item.motion_coherence != "coherent"
                or item.footprint_gap_m
                > policy.maximum_footprint_gap_m
            )
            and item.left_track_id in component_set
            and item.right_track_id in component_set
            for item in connections
        )
        motion = _descriptive_motion(members, policy, analysis_time)
        uncertainty = _system_uncertainty(
            members,
            candidates,
            component_connection_ids,
            motion,
            strong_connection_conflict,
        )
        footprint = tuple(
            sorted(
                {
                    pixel
                    for member in members
                    for pixel in member.current_footprint_pixels
                }
            )
        )
        shield_ids = tuple(
            sorted(
                {
                    shield_id
                    for member in members
                    for shield_id in member.rain_shield_detection_ids
                }
            )
        )
        snapshot_id = _digest_id(
            "weather-system-snapshot",
            system_id,
            policy.fingerprint,
            format(analysis_time, ".17g"),
            *(item.track_snapshot_id for item in members),
            *(f"{row},{column}" for row, column in footprint),
        )
        system = WeatherSystemSnapshot(
            system_snapshot_id=snapshot_id,
            system_id=system_id,
            valid_at=analysis_time,
            analysis_grid_id=grid.grid_id,
            member_tracks=members,
            rain_shield_detection_ids=shield_ids,
            aggregate_current_footprint=footprint,
            descriptive_motion=motion,
            classification="not_classified",
            system_uncertainty=uncertainty,
            predecessor_candidates=candidates,
            uncertain_connection_ids=component_connection_ids,
        )
        systems.append(system)
        events.append(
            _membership_event(
                system,
                previous_by_id,
                previous_use_counts,
            )
        )

    systems_tuple = tuple(sorted(systems, key=lambda item: item.system_id))
    events_tuple = tuple(
        sorted(events, key=lambda item: item.current_system_id)
    )
    retired = tuple(
        sorted(set(previous_by_id) - selected_previous_ids)
    )
    grouping_id = _digest_id(
        "weather-system-grouping",
        policy.fingerprint,
        grid.grid_id,
        format(analysis_time, ".17g"),
        previous.grouping_snapshot_id if previous is not None else "none",
        *(item.system_snapshot_id for item in systems_tuple),
    )
    return WeatherSystemGroupingSnapshot(
        grouping_snapshot_id=grouping_id,
        valid_at=analysis_time,
        analysis_grid_id=grid.grid_id,
        policy=policy,
        input_tracks=ordered,
        systems=systems_tuple,
        connections=connections,
        membership_events=events_tuple,
        previous_grouping_snapshot_id=(
            previous.grouping_snapshot_id if previous is not None else None
        ),
        retired_system_ids=retired,
    )


@dataclass(frozen=True, slots=True)
class _Candidate:
    previous_system_id: str
    direct_member_track_ids: tuple[str, ...]
    lineage_supported_track_ids: tuple[str, ...]

    @property
    def rank(self) -> tuple[int, int]:
        return (
            len(self.direct_member_track_ids),
            len(self.lineage_supported_track_ids),
        )


def _connection(
    left: WeatherSystemTrackInput,
    right: WeatherSystemTrackInput,
    grid: AnalysisGrid,
    policy: WeatherSystemGroupingPolicy,
    analysis_time: float,
) -> WeatherSystemConnectionEvidence:
    shared_shields = tuple(
        sorted(
            set(left.rain_shield_detection_ids)
            & set(right.rain_shield_detection_ids)
        )
    )
    left_pixels = set(left.current_footprint_pixels)
    right_pixels = set(right.current_footprint_pixels)
    overlap = len(left_pixels & right_pixels)
    touching = _footprints_touch(
        left_pixels,
        right_pixels,
        policy.pixel_connectivity,
    )
    gap = _footprint_gap_m(left_pixels, right_pixels, grid)
    time_difference = abs(left.valid_at - right.valid_at)
    motion_coherence, motion_time_difference, motion_difference = (
        _motion_coherence(left, right, policy, analysis_time)
    )
    if shared_shields:
        spatial_relation = "shared_rain_shield"
    elif overlap:
        spatial_relation = "overlap"
    elif touching:
        spatial_relation = "touching"
    elif gap <= policy.maximum_footprint_gap_m:
        spatial_relation = "near"
    else:
        spatial_relation = "far"

    reasons: set[str] = {
        f"spatial:{spatial_relation}",
        f"motion:{motion_coherence}",
        "time:current",
    }
    if shared_shields:
        decision = "connected"
        reasons.add("shared_current_rain_shield_is_strong_connection")
        if gap > policy.maximum_footprint_gap_m:
            reasons.add("shared_shield_geometry_exceeds_proximity_limit")
        if motion_coherence != "coherent":
            reasons.add("local_puppy_motion_does_not_break_shared_shield")
    elif spatial_relation in ("overlap", "touching", "near"):
        if motion_coherence == "coherent":
            decision = "connected"
            reasons.add("current_space_and_motion_coherent")
        else:
            decision = "uncertain"
            reasons.add("spatially_plausible_but_motion_not_confirmed")
    else:
        decision = "separate"
        reasons.add("current_footprints_too_far")
    connection_id = _digest_id(
        "weather-system-connection",
        left.track_snapshot_id,
        right.track_snapshot_id,
        policy.fingerprint,
    )
    return WeatherSystemConnectionEvidence(
        connection_id=connection_id,
        left_track_id=left.track_id,
        right_track_id=right.track_id,
        shared_rain_shield_detection_ids=shared_shields,
        overlap_pixel_count=overlap,
        footprint_gap_m=gap,
        valid_time_difference_s=time_difference,
        motion_time_difference_s=motion_time_difference,
        motion_difference_m_s=motion_difference,
        motion_coherence=motion_coherence,
        spatial_relation=spatial_relation,
        decision=decision,
        reasons=tuple(sorted(reasons)),
    )


def _motion_coherence(
    left: WeatherSystemTrackInput,
    right: WeatherSystemTrackInput,
    policy: WeatherSystemGroupingPolicy,
    analysis_time: float,
) -> tuple[str, float | None, float | None]:
    if not left.has_motion or not right.has_motion:
        return "unknown", None, None
    left_age = analysis_time - float(left.motion_reference_valid_at)
    right_age = analysis_time - float(right.motion_reference_valid_at)
    time_difference = abs(
        float(left.motion_reference_valid_at)
        - float(right.motion_reference_valid_at)
    )
    difference = math.hypot(
        float(left.vx_m_s) - float(right.vx_m_s),
        float(left.vy_m_s) - float(right.vy_m_s),
    )
    if (
        left_age > policy.maximum_motion_age_s
        or right_age > policy.maximum_motion_age_s
        or time_difference > policy.maximum_motion_time_difference_s
    ):
        return "stale", time_difference, difference
    if difference <= policy.maximum_motion_difference_m_s:
        return "coherent", time_difference, difference
    return "incoherent", time_difference, difference


def _connected_components(
    track_ids: tuple[str, ...],
    connections: tuple[WeatherSystemConnectionEvidence, ...],
) -> tuple[tuple[str, ...], ...]:
    neighbours = {track_id: set() for track_id in track_ids}
    for item in connections:
        if item.decision == "connected":
            neighbours[item.left_track_id].add(item.right_track_id)
            neighbours[item.right_track_id].add(item.left_track_id)
    remaining = set(track_ids)
    components: list[tuple[str, ...]] = []
    while remaining:
        seed = min(remaining)
        remaining.remove(seed)
        stack = [seed]
        members: list[str] = []
        while stack:
            current = stack.pop()
            members.append(current)
            for neighbour in sorted(neighbours[current], reverse=True):
                if neighbour in remaining:
                    remaining.remove(neighbour)
                    stack.append(neighbour)
        components.append(tuple(sorted(members)))
    return tuple(sorted(components))


def _predecessor_candidates(
    components: tuple[tuple[str, ...], ...],
    tracks: tuple[WeatherSystemTrackInput, ...],
    previous: WeatherSystemGroupingSnapshot | None,
) -> dict[tuple[str, ...], tuple[_Candidate, ...]]:
    if previous is None:
        return {component: () for component in components}
    track_by_id = {item.track_id: item for item in tracks}
    result: dict[tuple[str, ...], tuple[_Candidate, ...]] = {}
    for component in components:
        candidates: list[_Candidate] = []
        for system in previous.systems:
            previous_ids = set(system.member_track_ids)
            direct = tuple(sorted(previous_ids & set(component)))
            lineage = tuple(
                track_id
                for track_id in component
                if (
                    set(track_by_id[track_id].parent_track_ids) & previous_ids
                    or set(
                        track_by_id[track_id].identity_alternative_track_ids
                    )
                    & previous_ids
                )
            )
            lineage = tuple(sorted(set(lineage) - set(direct)))
            if direct or lineage:
                candidates.append(
                    _Candidate(system.system_id, direct, lineage)
                )
        result[component] = tuple(
            sorted(candidates, key=lambda item: item.previous_system_id)
        )
    return result


def _select_predecessors(
    candidates: dict[tuple[str, ...], tuple[_Candidate, ...]],
) -> dict[tuple[str, ...], str]:
    ranked = sorted(
        (
            (
                -candidate.rank[0],
                -candidate.rank[1],
                candidate.previous_system_id,
                component,
            )
            for component, values in candidates.items()
            for candidate in values
        )
    )
    selected_components: set[tuple[str, ...]] = set()
    selected_previous: set[str] = set()
    result: dict[tuple[str, ...], str] = {}
    for _direct, _lineage, previous_id, component in ranked:
        if component in selected_components or previous_id in selected_previous:
            continue
        result[component] = previous_id
        selected_components.add(component)
        selected_previous.add(previous_id)
    return result


def _previous_candidate_use_counts(
    candidates: dict[tuple[str, ...], tuple[_Candidate, ...]],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for values in candidates.values():
        for item in values:
            counts[item.previous_system_id] = (
                counts.get(item.previous_system_id, 0) + 1
            )
    return counts


def _descriptive_motion(
    members: tuple[WeatherSystemTrackInput, ...],
    policy: WeatherSystemGroupingPolicy,
    analysis_time: float,
) -> DescriptiveSystemMotion:
    valid = tuple(
        item
        for item in members
        if item.has_motion
        and analysis_time - float(item.motion_reference_valid_at)
        <= policy.maximum_motion_age_s
    )
    shield_support = tuple(
        item for item in valid if item.object_kind == "rain_shield"
    )
    support = shield_support or valid
    if not support:
        return DescriptiveSystemMotion(
            vx_m_s=None,
            vy_m_s=None,
            method="no_current_member_motion",
            supporting_track_ids=(),
            maximum_member_residual_m_s=None,
        )
    vx = float(median(float(item.vx_m_s) for item in support))
    vy = float(median(float(item.vy_m_s) for item in support))
    residual = max(
        math.hypot(float(item.vx_m_s) - vx, float(item.vy_m_s) - vy)
        for item in support
    )
    method = (
        "componentwise_median_current_rain_shield_motion"
        if shield_support
        else "componentwise_median_current_member_motion"
    )
    return DescriptiveSystemMotion(
        vx_m_s=vx,
        vy_m_s=vy,
        method=method,
        supporting_track_ids=tuple(
            sorted(item.track_id for item in support)
        ),
        maximum_member_residual_m_s=residual,
    )


def _system_uncertainty(
    members: tuple[WeatherSystemTrackInput, ...],
    candidates: tuple[WeatherSystemPredecessorCandidate, ...],
    uncertain_connection_ids: tuple[str, ...],
    motion: DescriptiveSystemMotion,
    strong_connection_conflict: bool,
) -> tuple[str, ...]:
    reasons: set[str] = set()
    if uncertain_connection_ids:
        reasons.add("uncertain_spatial_membership_connections_present")
    if strong_connection_conflict:
        reasons.add("strong_shield_connection_has_geometry_or_motion_conflict")
    if len(candidates) > 1:
        reasons.add("multiple_predecessor_system_candidates")
    if not any(item.object_kind == "rain_shield" for item in members):
        reasons.add("no_current_rain_shield_track")
    if motion.vx_m_s is None:
        reasons.add("descriptive_system_motion_unavailable")
    if any(not item.has_motion for item in members):
        reasons.add("one_or_more_member_motions_unknown")
    return tuple(sorted(reasons))


def _membership_event(
    system: WeatherSystemSnapshot,
    previous_by_id: dict[str, WeatherSystemSnapshot],
    previous_use_counts: dict[str, int],
) -> WeatherSystemMembershipEvent:
    predecessor_ids = tuple(
        item.previous_system_id for item in system.predecessor_candidates
    )
    selected = next(
        (
            item.previous_system_id
            for item in system.predecessor_candidates
            if item.selected
        ),
        None,
    )
    alternatives = tuple(
        item for item in predecessor_ids if item != selected
    )
    reference_predecessor = selected
    if reference_predecessor is None and system.predecessor_candidates:
        reference_predecessor = min(
            system.predecessor_candidates,
            key=lambda item: (
                -item.support_rank[0],
                -item.support_rank[1],
                item.previous_system_id,
            ),
        ).previous_system_id
    previous_members = (
        set(previous_by_id[reference_predecessor].member_track_ids)
        if reference_predecessor is not None
        else set()
    )
    current_members = set(system.member_track_ids)
    added = tuple(sorted(current_members - previous_members))
    removed = tuple(sorted(previous_members - current_members))
    split_related = any(
        previous_use_counts.get(item, 0) > 1
        for item in predecessor_ids
    )
    merge_related = len(predecessor_ids) > 1
    if merge_related and split_related:
        event_type = "complex_reconfiguration"
    elif merge_related:
        event_type = "merge"
    elif split_related and selected is not None:
        event_type = "split_retained"
    elif split_related:
        event_type = "split_child"
    elif selected is None:
        event_type = "birth"
    elif added or removed:
        event_type = "membership_changed"
    else:
        event_type = "continued"
    lineage_ids = tuple(
        sorted(
            {
                event_id
                for track in system.member_tracks
                for event_id in track.lineage_event_ids
            }
        )
    )
    identity_alternatives = tuple(
        sorted(
            {
                alternative
                for track in system.member_tracks
                for alternative in track.identity_alternative_track_ids
            }
        )
    )
    event_id = _digest_id(
        "weather-system-membership-event",
        system.system_snapshot_id,
        event_type,
        *(predecessor_ids or ("none",)),
    )
    return WeatherSystemMembershipEvent(
        event_id=event_id,
        event_type=event_type,
        current_system_id=system.system_id,
        selected_predecessor_system_id=selected,
        predecessor_system_ids=predecessor_ids,
        alternative_predecessor_system_ids=alternatives,
        current_member_track_ids=system.member_track_ids,
        added_track_ids=added,
        removed_track_ids=removed,
        member_lineage_event_ids=lineage_ids,
        member_identity_alternative_track_ids=identity_alternatives,
    )


def _new_system_id(
    grid_id: str,
    members: tuple[WeatherSystemTrackInput, ...],
) -> str:
    shield_track_ids = tuple(
        item.track_id
        for item in members
        if item.object_kind == "rain_shield"
    )
    anchors = shield_track_ids or tuple(item.track_id for item in members)
    return _digest_id("weather-system", grid_id, *anchors)


def _footprints_touch(
    left: set[Pixel],
    right: set[Pixel],
    connectivity: int,
) -> bool:
    if left & right:
        return False
    offsets = (
        ((-1, 0), (0, -1), (0, 1), (1, 0))
        if connectivity == 4
        else (
            (-1, -1),
            (-1, 0),
            (-1, 1),
            (0, -1),
            (0, 1),
            (1, -1),
            (1, 0),
            (1, 1),
        )
    )
    return any(
        (row + row_delta, column + column_delta) in right
        for row, column in left
        for row_delta, column_delta in offsets
    )


def _footprint_gap_m(
    left: set[Pixel],
    right: set[Pixel],
    grid: AnalysisGrid,
) -> float:
    return min(
        math.hypot(
            max(0, abs(left_column - right_column) - 1)
            * grid.x_resolution_m,
            max(0, abs(left_row - right_row) - 1)
            * grid.y_resolution_m,
        )
        for left_row, left_column in left
        for right_row, right_column in right
    )


def _pixels(values: object, field: str) -> tuple[Pixel, ...]:
    if not isinstance(values, tuple) or not values:
        raise ValueError(f"{field} vereist niet-lege immutable tuple")
    if values != tuple(sorted(values)) or len(values) != len(set(values)):
        raise ValueError(f"{field} moet uniek en gesorteerd zijn")
    for value in values:
        if (
            not isinstance(value, tuple)
            or len(value) != 2
            or isinstance(value[0], bool)
            or not isinstance(value[0], int)
            or value[0] < 0
            or isinstance(value[1], bool)
            or not isinstance(value[1], int)
            or value[1] < 0
        ):
            raise ValueError(f"{field} bevat ongeldige pixel")
    return values


def _sorted_identifiers(
    values: object,
    field: str,
    *,
    allow_empty: bool,
) -> tuple[str, ...]:
    if not isinstance(values, tuple):
        raise TypeError(f"{field} moet immutable tuple zijn")
    if not allow_empty and not values:
        raise ValueError(f"{field} mag niet leeg zijn")
    for value in values:
        _identifier(value, field)
    if values != tuple(sorted(values)) or len(values) != len(set(values)):
        raise ValueError(f"{field} moet uniek en gesorteerd zijn")
    return values


def _identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"{field} vereist niet-lege veilige string")
    return value


def _finite(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} moet numeriek zijn")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{field} moet eindig zijn")
    return numeric


def _digest_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256(
        "\x00".join((WEATHER_SYSTEM_GROUPING_VERSION, prefix, *parts)).encode(
            "utf-8"
        )
    ).hexdigest()[:24]
    return f"{prefix}:{digest}"
