"""Leakage-safe event splits and external meteorological label contracts.

This module is deliberately independent from the tracking and nowcast engines.
It neither creates labels nor infers meteorological truth from candidate output.
Callers must supply:

* immutable event/frame identities and every known leakage group;
* an explicit, preregistered split policy (there is no production default);
* external source provenance and typed labels.

The deterministic splitter assigns connected *event components*, never frames.
Events connected by a leakage group (for example one physical storm spanning a
provider hand-off) are therefore guaranteed to remain in one partition.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import re
from typing import Iterable


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SPLIT_BUCKETS = 10_000
_SPLIT_ALGORITHM_VERSION = "sha256-connected-event-components-v1"


class CorpusPartition(str, Enum):
    TRAIN = "train"
    VALIDATION = "validation"
    TEST = "test"


class ExternalSourceKind(str, Enum):
    BLINDED_HUMAN_CONSENSUS = "blinded_human_consensus"
    IN_SITU_INSTRUMENT = "in_situ_instrument"
    INDEPENDENT_REFERENCE_ANALYSIS = "independent_reference_analysis"
    ENGINE_DERIVED_DIAGNOSTIC = "engine_derived_diagnostic"


class ExternalLabelKind(str, Enum):
    CELL_IDENTITY = "cell_identity"
    CELL_POSITION = "cell_position"
    CELL_SPEED = "cell_speed"
    PASSAGE = "passage"
    WET_DRY = "wet_dry"
    PRECIPITATION_FIELD = "precipitation_field"


class LabelState(str, Enum):
    CONFIRMED = "confirmed"
    AMBIGUOUS = "ambiguous"
    UNKNOWN = "unknown"


class WetDryState(str, Enum):
    WET = "wet"
    DRY = "dry"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class EventFrame:
    """One immutable frame/sample belonging to one meteorological event."""

    frame_id: str
    event_id: str
    valid_time_utc: float
    leakage_group_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.frame_id, "frame_id")
        _identifier(self.event_id, "event_id")
        _finite(self.valid_time_utc, "valid_time_utc")
        _sorted_unique_identifiers(
            self.leakage_group_ids,
            "leakage_group_ids",
            allow_empty=True,
        )

    def to_dict(self) -> dict:
        return {
            "frame_id": self.frame_id,
            "event_id": self.event_id,
            "valid_time_utc": float(self.valid_time_utc),
            "leakage_group_ids": list(self.leakage_group_ids),
        }


@dataclass(frozen=True, slots=True)
class EventSplitPolicy:
    """Explicit deterministic partition policy; weights use exact basis points."""

    policy_id: str
    hash_salt: str
    train_basis_points: int
    validation_basis_points: int
    test_basis_points: int
    algorithm_version: str = _SPLIT_ALGORITHM_VERSION

    def __post_init__(self) -> None:
        _identifier(self.policy_id, "policy_id")
        _identifier(self.hash_salt, "hash_salt")
        _identifier(self.algorithm_version, "algorithm_version")
        if self.algorithm_version != _SPLIT_ALGORITHM_VERSION:
            raise ValueError(
                f"niet-ondersteund splitalgoritme: {self.algorithm_version}"
            )
        weights = (
            self.train_basis_points,
            self.validation_basis_points,
            self.test_basis_points,
        )
        for value in weights:
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError("alle splitgewichten moeten positieve gehele basispunten zijn")
        if sum(weights) != _SPLIT_BUCKETS:
            raise ValueError("splitgewichten moeten exact 10000 basispunten vormen")

    @property
    def fingerprint(self) -> str:
        return "sha256:" + _sha256_json(self.to_dict())

    def to_dict(self) -> dict:
        return {
            "policy_id": self.policy_id,
            "hash_salt": self.hash_salt,
            "train_basis_points": self.train_basis_points,
            "validation_basis_points": self.validation_basis_points,
            "test_basis_points": self.test_basis_points,
            "algorithm_version": self.algorithm_version,
        }


@dataclass(frozen=True, slots=True)
class EventSplit:
    """Immutable result of assigning connected event components."""

    corpus_id: str
    policy_fingerprint: str
    frame_events: tuple[tuple[str, str], ...]
    frame_partitions: tuple[tuple[str, CorpusPartition], ...]
    event_partitions: tuple[tuple[str, CorpusPartition], ...]
    group_partitions: tuple[tuple[str, CorpusPartition], ...]
    event_components: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        _sha256_identifier(self.corpus_id, "corpus_id")
        _sha256_identifier(self.policy_fingerprint, "policy_fingerprint")
        _sorted_unique_mapping(self.frame_events, "frame_events")
        _sorted_unique_mapping(self.frame_partitions, "frame_partitions")
        _sorted_unique_mapping(self.event_partitions, "event_partitions")
        _sorted_unique_mapping(self.group_partitions, "group_partitions")
        _sorted_unique_mapping(self.event_components, "event_components")
        for frame_id, event_id in self.frame_events:
            _identifier(frame_id, "frame_events frame_id")
            _identifier(event_id, "frame_events event_id")
        for mapping in (
            self.frame_partitions,
            self.event_partitions,
            self.group_partitions,
        ):
            if any(not isinstance(value, CorpusPartition) for _, value in mapping):
                raise TypeError("splitmapping bevat geen CorpusPartition")

    def partition_for_event(self, event_id: str) -> CorpusPartition | None:
        return dict(self.event_partitions).get(event_id)

    def partition_for_frame(self, frame_id: str) -> CorpusPartition | None:
        return dict(self.frame_partitions).get(frame_id)

    def to_dict(self) -> dict:
        return {
            "corpus_id": self.corpus_id,
            "policy_fingerprint": self.policy_fingerprint,
            "frame_events": [list(item) for item in self.frame_events],
            "frame_partitions": [
                [key, value.value] for key, value in self.frame_partitions
            ],
            "event_partitions": [
                [key, value.value] for key, value in self.event_partitions
            ],
            "group_partitions": [
                [key, value.value] for key, value in self.group_partitions
            ],
            "event_components": [list(item) for item in self.event_components],
        }

    @property
    def sha256(self) -> str:
        return _sha256_json(self.to_dict())


def build_event_held_out_split(
    frames: Iterable[EventFrame],
    policy: EventSplitPolicy,
) -> EventSplit:
    """Assign whole connected event groups with a stable SHA-256 bucket.

    Input order has no effect.  A leakage group shared by multiple events joins
    them into one connected component before hashing.  Empty corpora fail
    closed because they cannot support scientific validation.
    """

    if not isinstance(policy, EventSplitPolicy):
        raise TypeError("policy vereist EventSplitPolicy")
    ordered = tuple(sorted(frames, key=lambda item: item.frame_id))
    if not ordered:
        raise ValueError("eventcorpus mag niet leeg zijn")
    if any(not isinstance(item, EventFrame) for item in ordered):
        raise TypeError("frames moeten EventFrame-records zijn")
    frame_ids = [item.frame_id for item in ordered]
    if len(frame_ids) != len(set(frame_ids)):
        raise ValueError("frame_id moet uniek zijn")

    event_ids = sorted({item.event_id for item in ordered})
    parent = {event_id: event_id for event_id in event_ids}

    def find(event_id: str) -> str:
        root = event_id
        while parent[root] != root:
            root = parent[root]
        while parent[event_id] != event_id:
            next_event = parent[event_id]
            parent[event_id] = root
            event_id = next_event
        return root

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return
        low, high = sorted((left_root, right_root))
        parent[high] = low

    group_events: dict[str, set[str]] = {}
    for frame in ordered:
        for group_id in frame.leakage_group_ids:
            group_events.setdefault(group_id, set()).add(frame.event_id)
    for members in group_events.values():
        first, *rest = sorted(members)
        for event_id in rest:
            union(first, event_id)

    components: dict[str, list[str]] = {}
    for event_id in event_ids:
        components.setdefault(find(event_id), []).append(event_id)

    event_partition: dict[str, CorpusPartition] = {}
    event_component: dict[str, str] = {}
    for members in sorted(components.values()):
        component_id = "sha256:" + _sha256_json({"event_ids": members})
        bucket_digest = hashlib.sha256(
            (
                policy.algorithm_version
                + "\x00"
                + policy.hash_salt
                + "\x00"
                + component_id
            ).encode("utf-8")
        ).digest()
        bucket = int.from_bytes(bucket_digest[:8], "big") % _SPLIT_BUCKETS
        if bucket < policy.train_basis_points:
            partition = CorpusPartition.TRAIN
        elif bucket < (
            policy.train_basis_points + policy.validation_basis_points
        ):
            partition = CorpusPartition.VALIDATION
        else:
            partition = CorpusPartition.TEST
        for event_id in members:
            event_partition[event_id] = partition
            event_component[event_id] = component_id

    frame_partitions = tuple(
        (frame.frame_id, event_partition[frame.event_id])
        for frame in ordered
    )
    group_partitions: list[tuple[str, CorpusPartition]] = []
    for group_id, members in sorted(group_events.items()):
        partitions = {event_partition[event_id] for event_id in members}
        if len(partitions) != 1:  # pragma: no cover - union invariant
            raise AssertionError("leakagegroep kruist partitions")
        group_partitions.append((group_id, partitions.pop()))

    corpus_document = {
        "frames": [frame.to_dict() for frame in ordered],
        "grouping_rule": "transitive_shared_leakage_group",
    }
    result = EventSplit(
        corpus_id="sha256:" + _sha256_json(corpus_document),
        policy_fingerprint=policy.fingerprint,
        frame_events=tuple(
            (frame.frame_id, frame.event_id)
            for frame in ordered
        ),
        frame_partitions=frame_partitions,
        event_partitions=tuple(sorted(event_partition.items())),
        group_partitions=tuple(group_partitions),
        event_components=tuple(sorted(event_component.items())),
    )
    _assert_no_split_leakage(ordered, result)
    return result


@dataclass(frozen=True, slots=True)
class ExternalLabelSource:
    """Immutable provenance contract for a label-producing source."""

    source_id: str
    source_kind: ExternalSourceKind
    producer: str
    dataset_version: str
    methodology_id: str
    source_artifact_sha256: str
    candidate_independent: bool
    blinded_to_candidate: bool

    def __post_init__(self) -> None:
        _identifier(self.source_id, "source_id")
        if not isinstance(self.source_kind, ExternalSourceKind):
            raise TypeError("source_kind vereist ExternalSourceKind")
        _identifier(self.producer, "producer")
        _identifier(self.dataset_version, "dataset_version")
        _identifier(self.methodology_id, "methodology_id")
        _sha256(self.source_artifact_sha256, "source_artifact_sha256")
        if not isinstance(self.candidate_independent, bool):
            raise TypeError("candidate_independent moet bool zijn")
        if not isinstance(self.blinded_to_candidate, bool):
            raise TypeError("blinded_to_candidate moet bool zijn")

    @property
    def is_external_truth_eligible(self) -> bool:
        if not self.candidate_independent:
            return False
        if self.source_kind is ExternalSourceKind.ENGINE_DERIVED_DIAGNOSTIC:
            return False
        if self.source_kind is ExternalSourceKind.BLINDED_HUMAN_CONSENSUS:
            return self.blinded_to_candidate
        return True

    def to_dict(self) -> dict:
        return {
            "source_id": self.source_id,
            "source_kind": self.source_kind.value,
            "producer": self.producer,
            "dataset_version": self.dataset_version,
            "methodology_id": self.methodology_id,
            "source_artifact_sha256": self.source_artifact_sha256,
            "candidate_independent": self.candidate_independent,
            "blinded_to_candidate": self.blinded_to_candidate,
        }


@dataclass(frozen=True, slots=True)
class CellIdentityLabel:
    reference_cell_id: str
    parent_reference_cell_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.reference_cell_id, "reference_cell_id")
        _sorted_unique_identifiers(
            self.parent_reference_cell_ids,
            "parent_reference_cell_ids",
            allow_empty=True,
        )

    def to_dict(self) -> dict:
        return {
            "reference_cell_id": self.reference_cell_id,
            "parent_reference_cell_ids": list(self.parent_reference_cell_ids),
        }


@dataclass(frozen=True, slots=True)
class CellPositionLabel:
    reference_cell_id: str
    latitude: float
    longitude: float
    horizontal_uncertainty_m: float | None

    def __post_init__(self) -> None:
        _identifier(self.reference_cell_id, "reference_cell_id")
        latitude = _finite(self.latitude, "latitude")
        longitude = _finite(self.longitude, "longitude")
        if not -90.0 <= latitude <= 90.0:
            raise ValueError("latitude ligt buiten [-90, 90]")
        if not -180.0 <= longitude <= 180.0:
            raise ValueError("longitude ligt buiten [-180, 180]")
        _optional_nonnegative(
            self.horizontal_uncertainty_m,
            "horizontal_uncertainty_m",
        )

    def to_dict(self) -> dict:
        return {
            "reference_cell_id": self.reference_cell_id,
            "latitude": float(self.latitude),
            "longitude": float(self.longitude),
            "horizontal_uncertainty_m": self.horizontal_uncertainty_m,
        }


@dataclass(frozen=True, slots=True)
class CellSpeedLabel:
    reference_cell_id: str
    eastward_mps: float
    northward_mps: float
    interval_start_utc: float
    interval_end_utc: float
    vector_uncertainty_mps: float | None

    def __post_init__(self) -> None:
        _identifier(self.reference_cell_id, "reference_cell_id")
        _finite(self.eastward_mps, "eastward_mps")
        _finite(self.northward_mps, "northward_mps")
        start = _finite(self.interval_start_utc, "interval_start_utc")
        end = _finite(self.interval_end_utc, "interval_end_utc")
        if end <= start:
            raise ValueError("speedinterval moet een positieve duur hebben")
        _optional_nonnegative(
            self.vector_uncertainty_mps,
            "vector_uncertainty_mps",
        )

    def to_dict(self) -> dict:
        return {
            "reference_cell_id": self.reference_cell_id,
            "eastward_mps": float(self.eastward_mps),
            "northward_mps": float(self.northward_mps),
            "interval_start_utc": float(self.interval_start_utc),
            "interval_end_utc": float(self.interval_end_utc),
            "vector_uncertainty_mps": self.vector_uncertainty_mps,
        }


@dataclass(frozen=True, slots=True)
class PassageLabel:
    tracker_id: str
    window_start_utc: float
    window_end_utc: float
    occurred: bool | None
    passage_time_utc: float | None
    time_uncertainty_s: float | None

    def __post_init__(self) -> None:
        _identifier(self.tracker_id, "tracker_id")
        start = _finite(self.window_start_utc, "window_start_utc")
        end = _finite(self.window_end_utc, "window_end_utc")
        if end <= start:
            raise ValueError("passagevenster moet een positieve duur hebben")
        if self.occurred is not None and not isinstance(self.occurred, bool):
            raise TypeError("occurred moet bool of None zijn")
        if self.passage_time_utc is not None:
            passage_time = _finite(self.passage_time_utc, "passage_time_utc")
            if not start <= passage_time <= end:
                raise ValueError("passage_time_utc ligt buiten het evaluatievenster")
        if self.occurred is True and self.passage_time_utc is None:
            raise ValueError("bevestigde passage vereist passage_time_utc")
        if self.occurred in (False, None) and self.passage_time_utc is not None:
            raise ValueError("geen/unknown passage mag geen passage_time_utc hebben")
        _optional_nonnegative(self.time_uncertainty_s, "time_uncertainty_s")

    def to_dict(self) -> dict:
        return {
            "tracker_id": self.tracker_id,
            "window_start_utc": float(self.window_start_utc),
            "window_end_utc": float(self.window_end_utc),
            "occurred": self.occurred,
            "passage_time_utc": self.passage_time_utc,
            "time_uncertainty_s": self.time_uncertainty_s,
        }


@dataclass(frozen=True, slots=True)
class WetDryLabel:
    site_id: str
    interval_start_utc: float
    interval_end_utc: float
    state: WetDryState
    capable_of_dry_detection: bool
    accumulation_mm: float | None = None

    def __post_init__(self) -> None:
        _identifier(self.site_id, "site_id")
        start = _finite(self.interval_start_utc, "interval_start_utc")
        end = _finite(self.interval_end_utc, "interval_end_utc")
        if end <= start:
            raise ValueError("wet/dry-interval moet een positieve duur hebben")
        if not isinstance(self.state, WetDryState):
            raise TypeError("state vereist WetDryState")
        if not isinstance(self.capable_of_dry_detection, bool):
            raise TypeError("capable_of_dry_detection moet bool zijn")
        if self.state is WetDryState.DRY and not self.capable_of_dry_detection:
            raise ValueError("dry-label vereist aantoonbaar dry-capable sensor")
        _optional_nonnegative(self.accumulation_mm, "accumulation_mm")

    def to_dict(self) -> dict:
        return {
            "site_id": self.site_id,
            "interval_start_utc": float(self.interval_start_utc),
            "interval_end_utc": float(self.interval_end_utc),
            "state": self.state.value,
            "capable_of_dry_detection": self.capable_of_dry_detection,
            "accumulation_mm": self.accumulation_mm,
        }


@dataclass(frozen=True, slots=True)
class PrecipitationFieldLabel:
    grid_id: str
    valid_time_utc: float
    variable_id: str
    unit_id: str
    field_artifact_sha256: str

    def __post_init__(self) -> None:
        _identifier(self.grid_id, "grid_id")
        _finite(self.valid_time_utc, "valid_time_utc")
        _identifier(self.variable_id, "variable_id")
        _identifier(self.unit_id, "unit_id")
        _sha256(self.field_artifact_sha256, "field_artifact_sha256")

    def to_dict(self) -> dict:
        return {
            "grid_id": self.grid_id,
            "valid_time_utc": float(self.valid_time_utc),
            "variable_id": self.variable_id,
            "unit_id": self.unit_id,
            "field_artifact_sha256": self.field_artifact_sha256,
        }


LabelPayload = (
    CellIdentityLabel
    | CellPositionLabel
    | CellSpeedLabel
    | PassageLabel
    | WetDryLabel
    | PrecipitationFieldLabel
)


_PAYLOAD_TYPES = {
    ExternalLabelKind.CELL_IDENTITY: CellIdentityLabel,
    ExternalLabelKind.CELL_POSITION: CellPositionLabel,
    ExternalLabelKind.CELL_SPEED: CellSpeedLabel,
    ExternalLabelKind.PASSAGE: PassageLabel,
    ExternalLabelKind.WET_DRY: WetDryLabel,
    ExternalLabelKind.PRECIPITATION_FIELD: PrecipitationFieldLabel,
}


@dataclass(frozen=True, slots=True)
class ExternalLabel:
    label_id: str
    label_kind: ExternalLabelKind
    source_id: str
    event_id: str
    frame_id: str
    state: LabelState
    payload: LabelPayload

    def __post_init__(self) -> None:
        _identifier(self.label_id, "label_id")
        if not isinstance(self.label_kind, ExternalLabelKind):
            raise TypeError("label_kind vereist ExternalLabelKind")
        _identifier(self.source_id, "source_id")
        _identifier(self.event_id, "event_id")
        _identifier(self.frame_id, "frame_id")
        if not isinstance(self.state, LabelState):
            raise TypeError("state vereist LabelState")
        expected = _PAYLOAD_TYPES[self.label_kind]
        if not isinstance(self.payload, expected):
            raise TypeError(
                f"{self.label_kind.value} vereist payload {expected.__name__}"
            )
        if (
            self.state is LabelState.CONFIRMED
            and isinstance(self.payload, PassageLabel)
            and self.payload.occurred is None
        ):
            raise ValueError("confirmed passage-label mag geen unknown outcome hebben")
        if (
            self.state is LabelState.CONFIRMED
            and isinstance(self.payload, WetDryLabel)
            and self.payload.state is WetDryState.UNKNOWN
        ):
            raise ValueError("confirmed wet/dry-label mag geen unknown state hebben")

    def to_dict(self) -> dict:
        return {
            "label_id": self.label_id,
            "label_kind": self.label_kind.value,
            "source_id": self.source_id,
            "event_id": self.event_id,
            "frame_id": self.frame_id,
            "state": self.state.value,
            "payload": self.payload.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class ExternalLabelCorpus:
    sources: tuple[ExternalLabelSource, ...]
    labels: tuple[ExternalLabel, ...]

    def __post_init__(self) -> None:
        if not self.sources:
            raise ValueError("labelcorpus vereist minstens één source")
        _unique_objects(self.sources, "source_id")
        _unique_objects(self.labels, "label_id")
        if tuple(sorted(self.sources, key=lambda item: item.source_id)) != self.sources:
            raise ValueError("sources moeten deterministisch op source_id gesorteerd zijn")
        if tuple(sorted(self.labels, key=lambda item: item.label_id)) != self.labels:
            raise ValueError("labels moeten deterministisch op label_id gesorteerd zijn")
        known_sources = {source.source_id for source in self.sources}
        unknown = sorted(
            {
                label.source_id
                for label in self.labels
                if label.source_id not in known_sources
            }
        )
        if unknown:
            raise ValueError(f"labels verwijzen naar onbekende sources: {unknown}")

    @property
    def sha256(self) -> str:
        return _sha256_json(self.to_dict())

    def source(self, source_id: str) -> ExternalLabelSource | None:
        return next(
            (source for source in self.sources if source.source_id == source_id),
            None,
        )

    def label(self, label_id: str) -> ExternalLabel | None:
        return next(
            (label for label in self.labels if label.label_id == label_id),
            None,
        )

    def to_dict(self) -> dict:
        return {
            "sources": [source.to_dict() for source in self.sources],
            "labels": [label.to_dict() for label in self.labels],
        }


@dataclass(frozen=True, slots=True)
class ExternalTruthContext:
    """Validated external labels plus their immutable held-out split."""

    split: EventSplit
    labels: ExternalLabelCorpus

    def __post_init__(self) -> None:
        if not isinstance(self.split, EventSplit):
            raise TypeError("split vereist EventSplit")
        if not isinstance(self.labels, ExternalLabelCorpus):
            raise TypeError("labels vereist ExternalLabelCorpus")
        frame_events = dict(self.split.frame_events)
        known_frames = dict(self.split.frame_partitions)
        known_events = dict(self.split.event_partitions)
        for label in self.labels.labels:
            if label.event_id not in known_events:
                raise ValueError(
                    f"label {label.label_id} gebruikt onbekend event_id"
                )
            if label.frame_id not in known_frames:
                raise ValueError(
                    f"label {label.label_id} gebruikt onbekend frame_id"
                )
            if frame_events[label.frame_id] != label.event_id:
                raise ValueError(
                    f"label {label.label_id} koppelt frame aan verkeerd event"
                )
            if known_frames[label.frame_id] is not known_events[label.event_id]:
                raise ValueError(
                    f"label {label.label_id} kruist event/frame partitions"
                )


def _assert_no_split_leakage(
    frames: tuple[EventFrame, ...],
    split: EventSplit,
) -> None:
    event_partitions = dict(split.event_partitions)
    frame_partitions = dict(split.frame_partitions)
    frame_events = dict(split.frame_events)
    for frame in frames:
        if frame_events[frame.frame_id] != frame.event_id:
            raise AssertionError("frame-eventmapping is gewijzigd")
        if frame_partitions[frame.frame_id] is not event_partitions[frame.event_id]:
            raise AssertionError("frame en meteorologisch event kruisen partitions")
    group_partitions: dict[str, set[CorpusPartition]] = {}
    for frame in frames:
        for group_id in frame.leakage_group_ids:
            group_partitions.setdefault(group_id, set()).add(
                event_partitions[frame.event_id]
            )
    if any(len(values) != 1 for values in group_partitions.values()):
        raise AssertionError("leakagegroep kruist partitions")


def _sorted_unique_mapping(values: tuple, field: str) -> None:
    if not isinstance(values, tuple):
        raise TypeError(f"{field} moet tuple zijn")
    keys = [item[0] for item in values]
    for key in keys:
        _identifier(key, f"{field} key")
    if len(keys) != len(set(keys)):
        raise ValueError(f"{field} bevat dubbele keys")
    if keys != sorted(keys):
        raise ValueError(f"{field} moet op key gesorteerd zijn")


def _unique_objects(values: tuple, attribute: str) -> None:
    if not isinstance(values, tuple):
        raise TypeError("corpusrecords moeten tuple zijn")
    keys = [getattr(item, attribute) for item in values]
    if len(keys) != len(set(keys)):
        raise ValueError(f"{attribute} moet uniek zijn")


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


def _sha256_identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        raise ValueError(f"{field} vereist sha256:<digest>")
    _sha256(value[7:], field)
    return value


def _finite(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} moet numeriek zijn")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{field} moet eindig zijn")
    return numeric


def _optional_nonnegative(value: object, field: str) -> float | None:
    if value is None:
        return None
    numeric = _finite(value, field)
    if numeric < 0.0:
        raise ValueError(f"{field} moet niet-negatief zijn")
    return numeric


def _sha256_json(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()
