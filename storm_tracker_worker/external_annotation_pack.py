"""Immutable, leakage-safe external annotation packs.

This module is an offline boundary around :mod:`validation_corpus`.  It never
infers truth from the Storm Tracker engine, never writes to the evidence
database and never computes validation metrics.  Export only inventories
caller-selected event windows and emits empty annotation templates.  Ingest
accepts independently completed templates only after strict pack, provenance,
time, unit and event checks.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterable, Mapping

from .validation_corpus import (
    CellIdentityLabel,
    CellPositionLabel,
    CellSpeedLabel,
    EventFrame,
    EventSplit,
    EventSplitPolicy,
    ExternalLabel,
    ExternalLabelCorpus,
    ExternalLabelKind,
    ExternalLabelSource,
    ExternalSourceKind,
    ExternalTruthContext,
    LabelState,
    PassageLabel,
    PrecipitationFieldLabel,
    WetDryLabel,
    WetDryState,
    build_event_held_out_split,
)


PACK_FORMAT_VERSION = "storm-tracker-external-annotation-pack-v1"
SUBMISSION_FORMAT_VERSION = "storm-tracker-annotation-submission-v1"
TEMPLATE_FORMAT_VERSION = SUBMISSION_FORMAT_VERSION
INGEST_FORMAT_VERSION = "storm-tracker-annotation-ingest-v1"
TIME_STANDARD = "UTC_UNIX_SECONDS"
UNIT_POLICY = "payload_units_match_template"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ENGINE_TABLES = frozenset(
    {
        "forecast_verification_samples",
        "provider_cycle_samples",
        "pysteps_frame_cache",
        "meteorological_analysis_frames",
    }
)


class EvidenceKind(str, Enum):
    """Evidence classes an export request can explicitly ask for."""

    UPSTREAM_ARTIFACT = "upstream_artifact"
    RADAR_FRAME_METADATA = "radar_frame_metadata"
    GROUND_FACT = "ground_fact"
    ENGINE_CONTEXT = "engine_context"


class TargetKind(str, Enum):
    TRACKER = "tracker"
    SITE = "site"
    GRID = "grid"
    CELL = "cell"


@dataclass(frozen=True, slots=True)
class AnnotationProtocol:
    protocol_id: str
    protocol_version: str
    instructions_sha256: str
    required_blinding: bool
    annotator_role_ids: tuple[str, ...]
    allowed_source_kinds: tuple[ExternalSourceKind, ...]

    def __post_init__(self) -> None:
        _identifier(self.protocol_id, "protocol_id")
        _identifier(self.protocol_version, "protocol_version")
        _sha256(self.instructions_sha256, "instructions_sha256")
        if not isinstance(self.required_blinding, bool):
            raise TypeError("required_blinding moet bool zijn")
        _sorted_identifiers(
            self.annotator_role_ids,
            "annotator_role_ids",
            allow_empty=False,
        )
        if not isinstance(self.allowed_source_kinds, tuple):
            raise TypeError("allowed_source_kinds moet tuple zijn")
        if not self.allowed_source_kinds:
            raise ValueError("allowed_source_kinds mag niet leeg zijn")
        if any(
            not isinstance(value, ExternalSourceKind)
            for value in self.allowed_source_kinds
        ):
            raise TypeError("allowed_source_kinds bevat een ongeldig type")
        if (
            ExternalSourceKind.ENGINE_DERIVED_DIAGNOSTIC
            in self.allowed_source_kinds
        ):
            raise ValueError("engine-derived sources mogen nooit waarheid leveren")
        values = tuple(item.value for item in self.allowed_source_kinds)
        if values != tuple(sorted(set(values))):
            raise ValueError("allowed_source_kinds moet uniek en gesorteerd zijn")

    def to_dict(self) -> dict[str, Any]:
        return {
            "protocol_id": self.protocol_id,
            "protocol_version": self.protocol_version,
            "instructions_sha256": self.instructions_sha256,
            "required_blinding": self.required_blinding,
            "annotator_role_ids": list(self.annotator_role_ids),
            "allowed_source_kinds": [
                value.value for value in self.allowed_source_kinds
            ],
        }


@dataclass(frozen=True, slots=True)
class AnnotationFrameSpec:
    frame_id: str
    valid_time_utc: float
    grid_id: str
    variable_id: str
    unit_id: str

    def __post_init__(self) -> None:
        _identifier(self.frame_id, "frame_id")
        _finite(self.valid_time_utc, "valid_time_utc")
        _identifier(self.grid_id, "grid_id")
        _identifier(self.variable_id, "variable_id")
        _identifier(self.unit_id, "unit_id")

    def to_dict(self) -> dict[str, Any]:
        return {
            "frame_id": self.frame_id,
            "valid_time_utc": float(self.valid_time_utc),
            "grid_id": self.grid_id,
            "variable_id": self.variable_id,
            "unit_id": self.unit_id,
        }


@dataclass(frozen=True, slots=True)
class AnnotationTargetSpec:
    target_id: str
    target_kind: TargetKind
    subject_id: str
    latitude: float | None
    longitude: float | None

    def __post_init__(self) -> None:
        _identifier(self.target_id, "target_id")
        if not isinstance(self.target_kind, TargetKind):
            raise TypeError("target_kind vereist TargetKind")
        _identifier(self.subject_id, "subject_id")
        if (self.latitude is None) != (self.longitude is None):
            raise ValueError("target latitude en longitude moeten samen aanwezig zijn")
        if self.latitude is not None:
            latitude = _finite(self.latitude, "latitude")
            longitude = _finite(self.longitude, "longitude")
            if not -90.0 <= latitude <= 90.0:
                raise ValueError("target latitude ligt buiten [-90, 90]")
            if not -180.0 <= longitude <= 180.0:
                raise ValueError("target longitude ligt buiten [-180, 180]")

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_id": self.target_id,
            "target_kind": self.target_kind.value,
            "subject_id": self.subject_id,
            "latitude": self.latitude,
            "longitude": self.longitude,
        }


@dataclass(frozen=True, slots=True)
class AnnotationLabelRequest:
    request_id: str
    label_kind: ExternalLabelKind
    frame_id: str
    target_id: str

    def __post_init__(self) -> None:
        _identifier(self.request_id, "request_id")
        if not isinstance(self.label_kind, ExternalLabelKind):
            raise TypeError("label_kind vereist ExternalLabelKind")
        _identifier(self.frame_id, "frame_id")
        _identifier(self.target_id, "target_id")

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "label_kind": self.label_kind.value,
            "frame_id": self.frame_id,
            "target_id": self.target_id,
        }


@dataclass(frozen=True, slots=True)
class AnnotationEventSpec:
    event_id: str
    window_start_utc: float
    window_end_utc: float
    leakage_group_ids: tuple[str, ...]
    provider_ids: tuple[str, ...]
    evidence_kinds: tuple[EvidenceKind, ...]
    frames: tuple[AnnotationFrameSpec, ...]
    targets: tuple[AnnotationTargetSpec, ...]
    label_requests: tuple[AnnotationLabelRequest, ...]

    def __post_init__(self) -> None:
        _identifier(self.event_id, "event_id")
        start = _finite(self.window_start_utc, "window_start_utc")
        end = _finite(self.window_end_utc, "window_end_utc")
        if end <= start:
            raise ValueError("eventvenster moet een positieve duur hebben")
        _sorted_identifiers(
            self.leakage_group_ids,
            "leakage_group_ids",
            allow_empty=True,
        )
        _sorted_identifiers(
            self.provider_ids,
            "provider_ids",
            allow_empty=False,
        )
        if not isinstance(self.evidence_kinds, tuple) or not self.evidence_kinds:
            raise ValueError("evidence_kinds moet een niet-lege tuple zijn")
        if any(not isinstance(value, EvidenceKind) for value in self.evidence_kinds):
            raise TypeError("evidence_kinds bevat een ongeldig type")
        evidence_values = tuple(value.value for value in self.evidence_kinds)
        if evidence_values != tuple(sorted(set(evidence_values))):
            raise ValueError("evidence_kinds moet uniek en gesorteerd zijn")
        _sorted_objects(self.frames, "frame_id", "frames")
        _sorted_objects(self.targets, "target_id", "targets")
        _sorted_objects(self.label_requests, "request_id", "label_requests")
        if not self.frames or not self.targets or not self.label_requests:
            raise ValueError("event vereist frames, targets en label_requests")
        frame_ids = {frame.frame_id for frame in self.frames}
        target_map = {target.target_id: target for target in self.targets}
        for frame in self.frames:
            if not start <= frame.valid_time_utc <= end:
                raise ValueError(
                    f"frame {frame.frame_id} ligt buiten het eventvenster"
                )
        for request in self.label_requests:
            if request.frame_id not in frame_ids:
                raise ValueError(
                    f"label request {request.request_id} gebruikt onbekend frame"
                )
            target = target_map.get(request.target_id)
            if target is None:
                raise ValueError(
                    f"label request {request.request_id} gebruikt onbekend target"
                )
            _validate_target_label_kind(target.target_kind, request.label_kind)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "window_start_utc": float(self.window_start_utc),
            "window_end_utc": float(self.window_end_utc),
            "leakage_group_ids": list(self.leakage_group_ids),
            "provider_ids": list(self.provider_ids),
            "evidence_kinds": [value.value for value in self.evidence_kinds],
            "frames": [frame.to_dict() for frame in self.frames],
            "targets": [target.to_dict() for target in self.targets],
            "label_requests": [
                request.to_dict() for request in self.label_requests
            ],
        }


@dataclass(frozen=True, slots=True)
class AnnotationPackRequest:
    request_id: str
    created_at_utc: float
    source_database_id: str
    time_standard: str
    protocol: AnnotationProtocol
    split_policy: EventSplitPolicy
    events: tuple[AnnotationEventSpec, ...]

    def __post_init__(self) -> None:
        _identifier(self.request_id, "request_id")
        _finite(self.created_at_utc, "created_at_utc")
        _identifier(self.source_database_id, "source_database_id")
        if self.time_standard != TIME_STANDARD:
            raise ValueError(f"time_standard moet exact {TIME_STANDARD} zijn")
        if not isinstance(self.protocol, AnnotationProtocol):
            raise TypeError("protocol vereist AnnotationProtocol")
        if not isinstance(self.split_policy, EventSplitPolicy):
            raise TypeError("split_policy vereist EventSplitPolicy")
        _sorted_objects(self.events, "event_id", "events")
        if not self.events:
            raise ValueError("events mag niet leeg zijn")
        all_frames: set[str] = set()
        all_requests: set[str] = set()
        for event in self.events:
            frame_ids = {frame.frame_id for frame in event.frames}
            request_ids = {item.request_id for item in event.label_requests}
            if all_frames & frame_ids:
                raise ValueError("frame_id moet globaal uniek zijn")
            if all_requests & request_ids:
                raise ValueError("label request_id moet globaal uniek zijn")
            all_frames.update(frame_ids)
            all_requests.update(request_ids)

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "created_at_utc": float(self.created_at_utc),
            "source_database_id": self.source_database_id,
            "time_standard": self.time_standard,
            "protocol": self.protocol.to_dict(),
            "split_policy": self.split_policy.to_dict(),
            "events": [event.to_dict() for event in self.events],
        }


@dataclass(frozen=True, slots=True)
class SourceDatabaseManifest:
    source_database_id: str
    database_sha256: str
    size_bytes: int
    schema_sha256: str
    table_names: tuple[str, ...]
    connection_mode: str = "sqlite-uri-mode-ro-immutable-query-only"

    def __post_init__(self) -> None:
        _identifier(self.source_database_id, "source_database_id")
        _sha256(self.database_sha256, "database_sha256")
        _nonnegative_int(self.size_bytes, "size_bytes")
        _sha256(self.schema_sha256, "schema_sha256")
        _sorted_identifiers(self.table_names, "table_names", allow_empty=True)
        if self.connection_mode != "sqlite-uri-mode-ro-immutable-query-only":
            raise ValueError("ongeldige database connection_mode")

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_database_id": self.source_database_id,
            "database_sha256": self.database_sha256,
            "size_bytes": self.size_bytes,
            "schema_sha256": self.schema_sha256,
            "table_names": list(self.table_names),
            "connection_mode": self.connection_mode,
        }


@dataclass(frozen=True, slots=True)
class EvidenceReference:
    reference_id: str
    event_id: str
    evidence_kind: EvidenceKind
    provider_id: str | None
    target_id: str | None
    source_table: str
    source_record_id: str
    observed_start_utc: float | None
    observed_end_utc: float | None
    received_at_utc: float | None
    artifact_sha256: str | None
    record_sha256: str
    quantity: str | None
    unit_id: str | None
    grid_id: str | None
    truth_eligibility: str
    annotator_visibility: str
    metadata_json: str

    def __post_init__(self) -> None:
        _identifier(self.reference_id, "reference_id")
        _identifier(self.event_id, "event_id")
        if not isinstance(self.evidence_kind, EvidenceKind):
            raise TypeError("evidence_kind vereist EvidenceKind")
        _optional_identifier(self.provider_id, "provider_id")
        _optional_identifier(self.target_id, "target_id")
        _identifier(self.source_table, "source_table")
        _identifier(self.source_record_id, "source_record_id")
        _optional_finite(self.observed_start_utc, "observed_start_utc")
        _optional_finite(self.observed_end_utc, "observed_end_utc")
        _optional_finite(self.received_at_utc, "received_at_utc")
        if (
            self.observed_start_utc is not None
            and self.observed_end_utc is not None
            and self.observed_end_utc < self.observed_start_utc
        ):
            raise ValueError("evidence-interval heeft negatieve duur")
        if self.artifact_sha256 is not None:
            _sha256(self.artifact_sha256, "artifact_sha256")
        _sha256(self.record_sha256, "record_sha256")
        _optional_identifier(self.quantity, "quantity")
        _optional_identifier(self.unit_id, "unit_id")
        _optional_identifier(self.grid_id, "grid_id")
        if self.truth_eligibility not in {
            "unadjudicated_external_evidence",
            "engine_context_not_truth",
            "legacy_provider_overlay_not_exact_raw",
        }:
            raise ValueError("ongeldige truth_eligibility")
        if self.annotator_visibility not in {"allowed", "prohibited"}:
            raise ValueError("ongeldige annotator_visibility")
        if self.evidence_kind is EvidenceKind.ENGINE_CONTEXT:
            if self.truth_eligibility != "engine_context_not_truth":
                raise ValueError("engine context mag nooit truth-eligible zijn")
            if self.annotator_visibility != "prohibited":
                raise ValueError("engine context moet blind voor annotators blijven")
        elif self.truth_eligibility == "engine_context_not_truth":
            raise ValueError(
                "alleen engine context mag engine_context_not_truth gebruiken"
            )
        elif self.annotator_visibility != "allowed":
            raise ValueError(
                "niet-engine evidence moet zichtbaar zijn in de annotation pack"
            )
        if self.truth_eligibility == "legacy_provider_overlay_not_exact_raw":
            if (
                self.evidence_kind is not EvidenceKind.UPSTREAM_ARTIFACT
                or self.source_table != "original_radar_frames"
            ):
                raise ValueError(
                    "legacy overlay-fidelity hoort alleen bij original_radar_frames"
                )
        _canonical_json_string(self.metadata_json, "metadata_json")

    @property
    def metadata(self) -> dict[str, Any]:
        return json.loads(self.metadata_json)

    def to_dict(self) -> dict[str, Any]:
        return {
            "reference_id": self.reference_id,
            "event_id": self.event_id,
            "evidence_kind": self.evidence_kind.value,
            "provider_id": self.provider_id,
            "target_id": self.target_id,
            "source_table": self.source_table,
            "source_record_id": self.source_record_id,
            "observed_start_utc": self.observed_start_utc,
            "observed_end_utc": self.observed_end_utc,
            "received_at_utc": self.received_at_utc,
            "artifact_sha256": self.artifact_sha256,
            "record_sha256": self.record_sha256,
            "quantity": self.quantity,
            "unit_id": self.unit_id,
            "grid_id": self.grid_id,
            "truth_eligibility": self.truth_eligibility,
            "annotator_visibility": self.annotator_visibility,
            "metadata": self.metadata,
        }


@dataclass(frozen=True, slots=True)
class MissingEvidenceRecord:
    missing_id: str
    event_id: str
    evidence_kind: EvidenceKind
    provider_id: str | None
    target_id: str | None
    window_start_utc: float
    window_end_utc: float
    reason_code: str
    queried_tables: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.missing_id, "missing_id")
        _identifier(self.event_id, "event_id")
        if not isinstance(self.evidence_kind, EvidenceKind):
            raise TypeError("evidence_kind vereist EvidenceKind")
        _optional_identifier(self.provider_id, "provider_id")
        _optional_identifier(self.target_id, "target_id")
        start = _finite(self.window_start_utc, "window_start_utc")
        end = _finite(self.window_end_utc, "window_end_utc")
        if end <= start:
            raise ValueError("missing-evidencevenster moet positieve duur hebben")
        _identifier(self.reason_code, "reason_code")
        _sorted_identifiers(
            self.queried_tables,
            "queried_tables",
            allow_empty=False,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "missing_id": self.missing_id,
            "event_id": self.event_id,
            "evidence_kind": self.evidence_kind.value,
            "provider_id": self.provider_id,
            "target_id": self.target_id,
            "window_start_utc": float(self.window_start_utc),
            "window_end_utc": float(self.window_end_utc),
            "reason_code": self.reason_code,
            "queried_tables": list(self.queried_tables),
        }


@dataclass(frozen=True, slots=True)
class LabelTemplateSpec:
    template_id: str
    request_id: str
    event_id: str
    frame_id: str
    target_id: str
    label_kind: ExternalLabelKind
    fixed_payload_json: str
    empty_payload_json: str
    allowed_evidence_reference_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        for value, field in (
            (self.template_id, "template_id"),
            (self.request_id, "request_id"),
            (self.event_id, "event_id"),
            (self.frame_id, "frame_id"),
            (self.target_id, "target_id"),
        ):
            _identifier(value, field)
        if not isinstance(self.label_kind, ExternalLabelKind):
            raise TypeError("label_kind vereist ExternalLabelKind")
        _canonical_json_string(self.fixed_payload_json, "fixed_payload_json")
        _canonical_json_string(self.empty_payload_json, "empty_payload_json")
        _sorted_identifiers(
            self.allowed_evidence_reference_ids,
            "allowed_evidence_reference_ids",
            allow_empty=True,
        )

    @property
    def fixed_payload(self) -> dict[str, Any]:
        return json.loads(self.fixed_payload_json)

    @property
    def empty_payload(self) -> dict[str, Any]:
        return json.loads(self.empty_payload_json)

    def to_dict(self) -> dict[str, Any]:
        return {
            "template_id": self.template_id,
            "request_id": self.request_id,
            "event_id": self.event_id,
            "frame_id": self.frame_id,
            "target_id": self.target_id,
            "label_kind": self.label_kind.value,
            "fixed_payload": self.fixed_payload,
            "empty_payload": self.empty_payload,
            "allowed_evidence_reference_ids": list(
                self.allowed_evidence_reference_ids
            ),
        }


@dataclass(frozen=True, slots=True)
class AnnotationPack:
    request: AnnotationPackRequest
    source_database: SourceDatabaseManifest
    split: EventSplit
    evidence_references: tuple[EvidenceReference, ...]
    missing_evidence: tuple[MissingEvidenceRecord, ...]
    templates: tuple[LabelTemplateSpec, ...]
    format_version: str = PACK_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != PACK_FORMAT_VERSION:
            raise ValueError("ongeldige annotation pack-versie")
        if self.source_database.source_database_id != self.request.source_database_id:
            raise ValueError("source_database_id komt niet overeen met request")
        _sorted_objects(
            self.evidence_references,
            "reference_id",
            "evidence_references",
        )
        _sorted_objects(self.missing_evidence, "missing_id", "missing_evidence")
        _sorted_objects(self.templates, "template_id", "templates")
        expected_events = {event.event_id for event in self.request.events}
        expected_frames = {
            frame.frame_id
            for event in self.request.events
            for frame in event.frames
        }
        if set(dict(self.split.event_partitions)) != expected_events:
            raise ValueError("split bevat niet exact de aangevraagde events")
        if set(dict(self.split.frame_partitions)) != expected_frames:
            raise ValueError("split bevat niet exact de aangevraagde frames")
        if self.split != _build_split(
            self.request.events,
            self.request.split_policy,
        ):
            raise ValueError(
                "split is niet exact uit de aangevraagde events/policy afgeleid"
            )
        events = {event.event_id: event for event in self.request.events}
        reference_ids = {
            reference.reference_id for reference in self.evidence_references
        }
        for reference in self.evidence_references:
            event = events.get(reference.event_id)
            if event is None:
                raise ValueError("evidence verwijst naar onbekend event")
            if reference.evidence_kind not in event.evidence_kinds:
                raise ValueError("evidence-kind is niet door het event aangevraagd")
            if (
                reference.provider_id is not None
                and reference.provider_id not in event.provider_ids
            ):
                raise ValueError("evidence verwijst naar niet-aangevraagde provider")
            event_target_ids = {target.target_id for target in event.targets}
            if (
                reference.target_id is not None
                and reference.target_id not in event_target_ids
            ):
                raise ValueError("evidence verwijst naar onbekend eventtarget")
            if reference.source_table not in self.source_database.table_names:
                raise ValueError("evidence verwijst naar tabel buiten de bronmanifest")
            effective_start = (
                reference.observed_start_utc
                if reference.observed_start_utc is not None
                else reference.received_at_utc
            )
            effective_end = (
                reference.observed_end_utc
                if reference.observed_end_utc is not None
                else effective_start
            )
            if (
                effective_start is None
                or effective_end is None
                or effective_start > event.window_end_utc
                or effective_end < event.window_start_utc
            ):
                raise ValueError("evidence overlapt het eventvenster niet")
            expected_reference_id = "evidence:" + _sha256_json(
                {
                    "event_id": reference.event_id,
                    "evidence_kind": reference.evidence_kind.value,
                    "provider_id": reference.provider_id,
                    "target_id": reference.target_id,
                    "source_table": reference.source_table,
                    "source_record_id": reference.source_record_id,
                    "record_sha256": reference.record_sha256,
                }
            )
            if reference.reference_id != expected_reference_id:
                raise ValueError("evidence reference_id is niet content-addressed")
        for missing in self.missing_evidence:
            if missing.event_id not in expected_events:
                raise ValueError("missing evidence verwijst naar onbekend event")
        if self.missing_evidence != _missing_evidence(
            self.request.events,
            self.evidence_references,
        ):
            raise ValueError(
                "missing-evidencelijst is niet exact uit request/evidence afgeleid"
            )
        request_ids = {
            request.request_id
            for event in self.request.events
            for request in event.label_requests
        }
        if {template.request_id for template in self.templates} != request_ids:
            raise ValueError("templates dekken niet exact alle label requests")
        if self.templates != _build_templates(
            self.request.events,
            self.evidence_references,
        ):
            raise ValueError(
                "templates zijn niet exact uit request/evidence afgeleid"
            )
        if any(
            reference_id not in reference_ids
            for template in self.templates
            for reference_id in template.allowed_evidence_reference_ids
        ):
            raise ValueError("template verwijst naar onbekende evidence")

    @property
    def sha256(self) -> str:
        return _sha256_json(self._content_dict())

    def _content_dict(self) -> dict[str, Any]:
        return {
            "format_version": self.format_version,
            "request": self.request.to_dict(),
            "source_database": self.source_database.to_dict(),
            "split": self.split.to_dict(),
            "evidence_references": [
                reference.to_dict() for reference in self.evidence_references
            ],
            "missing_evidence": [
                missing.to_dict() for missing in self.missing_evidence
            ],
            "templates": [template.to_dict() for template in self.templates],
            "scientific_contract": {
                "labels_created_by_export": False,
                "metrics_computed_by_export": False,
                "margins_selected_by_export": False,
                "frame_level_split_allowed": False,
                "engine_context_is_truth": False,
            },
        }

    def to_dict(self) -> dict[str, Any]:
        result = self._content_dict()
        result["annotation_pack_sha256"] = self.sha256
        return result


@dataclass(frozen=True, slots=True)
class AnnotationProvenance:
    template_id: str
    label_id: str | None
    source_id: str
    annotator_id: str
    annotator_role_id: str
    annotation_started_at_utc: float
    annotation_completed_at_utc: float
    blinded_to_candidate: bool
    evidence_reference_ids_used: tuple[str, ...]
    missing_annotation_reason: str | None

    def __post_init__(self) -> None:
        _identifier(self.template_id, "template_id")
        _optional_identifier(self.label_id, "label_id")
        _identifier(self.source_id, "source_id")
        _identifier(self.annotator_id, "annotator_id")
        _identifier(self.annotator_role_id, "annotator_role_id")
        started = _finite(
            self.annotation_started_at_utc,
            "annotation_started_at_utc",
        )
        completed = _finite(
            self.annotation_completed_at_utc,
            "annotation_completed_at_utc",
        )
        if completed < started:
            raise ValueError("annotation provenance heeft negatieve duur")
        if not isinstance(self.blinded_to_candidate, bool):
            raise TypeError("blinded_to_candidate moet bool zijn")
        _sorted_identifiers(
            self.evidence_reference_ids_used,
            "evidence_reference_ids_used",
            allow_empty=True,
        )
        _optional_identifier(
            self.missing_annotation_reason,
            "missing_annotation_reason",
        )
        if (self.label_id is None) != (
            self.missing_annotation_reason is not None
        ):
            raise ValueError(
                "provenance vereist exact een label of missing reason"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "template_id": self.template_id,
            "label_id": self.label_id,
            "source_id": self.source_id,
            "annotator_id": self.annotator_id,
            "annotator_role_id": self.annotator_role_id,
            "annotation_started_at_utc": self.annotation_started_at_utc,
            "annotation_completed_at_utc": self.annotation_completed_at_utc,
            "blinded_to_candidate": self.blinded_to_candidate,
            "evidence_reference_ids_used": list(
                self.evidence_reference_ids_used
            ),
            "missing_annotation_reason": self.missing_annotation_reason,
        }


@dataclass(frozen=True, slots=True)
class AnnotationIngestResult:
    annotation_pack_sha256: str
    submission_id: str
    submitted_at_utc: float
    corpus: ExternalLabelCorpus
    truth_context: ExternalTruthContext
    provenance: tuple[AnnotationProvenance, ...]
    unresolved_template_ids: tuple[str, ...]
    format_version: str = INGEST_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != INGEST_FORMAT_VERSION:
            raise ValueError("ongeldige annotation ingest-versie")
        _sha256(self.annotation_pack_sha256, "annotation_pack_sha256")
        _identifier(self.submission_id, "submission_id")
        submitted_at = _finite(self.submitted_at_utc, "submitted_at_utc")
        if not isinstance(self.corpus, ExternalLabelCorpus):
            raise TypeError("corpus vereist ExternalLabelCorpus")
        if not isinstance(self.truth_context, ExternalTruthContext):
            raise TypeError("truth_context vereist ExternalTruthContext")
        if self.truth_context.labels != self.corpus:
            raise ValueError("truth_context en corpus verschillen")
        if not isinstance(self.provenance, tuple):
            raise TypeError("provenance moet tuple zijn")
        provenance_ids = [item.template_id for item in self.provenance]
        if provenance_ids != sorted(set(provenance_ids)):
            raise ValueError(
                "provenance moet uniek en op template_id gesorteerd zijn"
            )
        if any(
            item.annotation_completed_at_utc > submitted_at
            for item in self.provenance
        ):
            raise ValueError("provenance eindigt na submitted_at_utc")
        _sorted_identifiers(
            self.unresolved_template_ids,
            "unresolved_template_ids",
            allow_empty=True,
        )
        unresolved_from_provenance = {
            item.template_id
            for item in self.provenance
            if item.label_id is None
        }
        if unresolved_from_provenance != set(self.unresolved_template_ids):
            raise ValueError(
                "unresolved templates en provenance verschillen"
            )
        provenance_label_ids = {
            item.label_id
            for item in self.provenance
            if item.label_id is not None
        }
        corpus_label_ids = {label.label_id for label in self.corpus.labels}
        if provenance_label_ids != corpus_label_ids:
            raise ValueError("provenance en corpuslabels verschillen")
        source_ids = {source.source_id for source in self.corpus.sources}
        if any(item.source_id not in source_ids for item in self.provenance):
            raise ValueError("provenance verwijst naar onbekende source")

    def _content_dict(self) -> dict[str, Any]:
        return {
            "format_version": self.format_version,
            "annotation_pack_sha256": self.annotation_pack_sha256,
            "submission_id": self.submission_id,
            "submitted_at_utc": self.submitted_at_utc,
            "corpus_sha256": self.corpus.sha256,
            "corpus": self.corpus.to_dict(),
            "split": self.truth_context.split.to_dict(),
            "provenance": [item.to_dict() for item in self.provenance],
            "unresolved_template_ids": list(self.unresolved_template_ids),
            "scientific_contract": {
                "metrics_computed": False,
                "margins_selected": False,
                "engine_derived_labels_accepted": False,
            },
        }

    @property
    def sha256(self) -> str:
        return _sha256_json(self._content_dict())

    def to_dict(self) -> dict[str, Any]:
        result = self._content_dict()
        result["annotation_ingest_sha256"] = self.sha256
        return result


def export_annotation_pack(
    database_path: str | Path,
    request: AnnotationPackRequest,
) -> AnnotationPack:
    """Export immutable evidence references and empty templates, read-only."""

    if not isinstance(request, AnnotationPackRequest):
        raise TypeError("request vereist AnnotationPackRequest")
    path = Path(database_path).expanduser().resolve(strict=True)
    if not path.is_file():
        raise ValueError(f"databasepad is geen bestand: {path}")
    for suffix, description in (
        ("-wal", "WAL"),
        ("-journal", "rollback-journal"),
    ):
        sidecar = path.with_name(path.name + suffix)
        if sidecar.exists() and sidecar.stat().st_size:
            raise RuntimeError(
                f"bron heeft een niet-lege {description}; maak eerst een "
                "consistente read-only snapshot"
            )
    sha_before = _sha256_file(path)
    size_before = path.stat().st_size
    uri = f"{path.as_uri()}?mode=ro&immutable=1"
    try:
        database = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as err:
        raise RuntimeError(f"database kan niet immutable read-only open: {err}") from err
    try:
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA query_only=ON")
        query_only = int(database.execute("PRAGMA query_only").fetchone()[0])
        if query_only != 1:
            raise RuntimeError("SQLite query_only kon niet geactiveerd worden")
        integrity = str(
            database.execute("PRAGMA integrity_check(1)").fetchone()[0]
        )
        if integrity != "ok":
            raise RuntimeError(
                f"SQLite bron faalt integrity_check: {integrity}"
            )
        foreign_key_problem = database.execute(
            "PRAGMA foreign_key_check"
        ).fetchone()
        if foreign_key_problem is not None:
            raise RuntimeError(
                "SQLite bron bevat een foreign-key schending"
            )
        schema_rows = database.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        schema_document = [
            {
                "type": row["type"],
                "name": row["name"],
                "table": row["tbl_name"],
                "sql": row["sql"],
            }
            for row in schema_rows
        ]
        tables = {
            str(row["name"])
            for row in schema_rows
            if row["type"] == "table"
        }
        references = _collect_evidence(database, tables, request.events)
    except sqlite3.Error as err:
        raise RuntimeError(f"read-only annotation export faalde: {err}") from err
    finally:
        database.close()
    sha_after = _sha256_file(path)
    size_after = path.stat().st_size
    if sha_before != sha_after or size_before != size_after:
        raise RuntimeError("brondatabase wijzigde tijdens annotation export")

    manifest = SourceDatabaseManifest(
        source_database_id=request.source_database_id,
        database_sha256=sha_before,
        size_bytes=size_before,
        schema_sha256=_sha256_json(schema_document),
        table_names=tuple(sorted(tables)),
    )
    split = _build_split(request.events, request.split_policy)
    missing = _missing_evidence(request.events, references)
    templates = _build_templates(request.events, references)
    return AnnotationPack(
        request=request,
        source_database=manifest,
        split=split,
        evidence_references=references,
        missing_evidence=missing,
        templates=templates,
    )


def empty_label_template_document(pack: AnnotationPack) -> dict[str, Any]:
    """Return a separate fillable document with no truth fields populated."""

    return {
        "format_version": TEMPLATE_FORMAT_VERSION,
        "annotation_pack_sha256": pack.sha256,
        "time_standard": TIME_STANDARD,
        "unit_policy": UNIT_POLICY,
        "sources": [],
        "submission_id": None,
        "submitted_at_utc": None,
        "annotations": [
            {
                "template_id": template.template_id,
                "request_id": template.request_id,
                "event_id": template.event_id,
                "frame_id": template.frame_id,
                "target_id": template.target_id,
                "label_kind": template.label_kind.value,
                "allowed_evidence_reference_ids": list(
                    template.allowed_evidence_reference_ids
                ),
                "evidence_reference_ids_used": [],
                "source_id": None,
                "annotator_id": None,
                "annotator_role_id": None,
                "annotation_started_at_utc": None,
                "annotation_completed_at_utc": None,
                "blinded_to_candidate": None,
                "label_state": None,
                "payload": template.empty_payload,
                "missing_annotation_reason": None,
            }
            for template in pack.templates
        ],
    }


def ingest_annotation_submission(
    pack: AnnotationPack,
    submission: Mapping[str, Any],
) -> AnnotationIngestResult:
    """Validate and ingest externally completed templates, fail-closed."""

    if not isinstance(pack, AnnotationPack):
        raise TypeError("pack vereist AnnotationPack")
    document = _mapping(submission, "submission")
    _exact_keys(
        document,
        {
            "format_version",
            "annotation_pack_sha256",
            "submission_id",
            "submitted_at_utc",
            "time_standard",
            "unit_policy",
            "sources",
            "annotations",
        },
        "submission",
    )
    if document["format_version"] != SUBMISSION_FORMAT_VERSION:
        raise ValueError("ongeldige submission-versie")
    if document["annotation_pack_sha256"] != pack.sha256:
        raise ValueError("annotation_pack_sha256 komt niet overeen")
    if document["time_standard"] != TIME_STANDARD:
        raise ValueError("submission gebruikt niet exact UTC_UNIX_SECONDS")
    if document["unit_policy"] != UNIT_POLICY:
        raise ValueError("submission gebruikt een niet-toegelaten unit policy")
    submission_id = _identifier(document["submission_id"], "submission_id")
    submitted_at = _finite(document["submitted_at_utc"], "submitted_at_utc")
    if submitted_at < pack.request.created_at_utc:
        raise ValueError("submission dateert van vóór het pack-request")

    sources, source_annotators = _ingest_sources(
        document["sources"],
        pack,
    )
    templates = {template.template_id: template for template in pack.templates}
    annotations = _list(document["annotations"], "annotations")
    if len(annotations) != len(templates):
        raise ValueError("submission moet elk template exact eenmaal bevatten")
    seen: set[str] = set()
    labels: list[ExternalLabel] = []
    provenance: list[AnnotationProvenance] = []
    unresolved: list[str] = []
    source_map = {source.source_id: source for source in sources}
    for raw in annotations:
        annotation = _mapping(raw, "annotation")
        _exact_keys(
            annotation,
            {
                "template_id",
                "request_id",
                "event_id",
                "frame_id",
                "target_id",
                "label_kind",
                "allowed_evidence_reference_ids",
                "evidence_reference_ids_used",
                "source_id",
                "annotator_id",
                "annotator_role_id",
                "annotation_started_at_utc",
                "annotation_completed_at_utc",
                "blinded_to_candidate",
                "label_state",
                "payload",
                "missing_annotation_reason",
            },
            "annotation",
        )
        template_id = _identifier(annotation["template_id"], "template_id")
        if template_id in seen:
            raise ValueError("template_id komt dubbel voor")
        seen.add(template_id)
        template = templates.get(template_id)
        if template is None:
            raise ValueError(f"onbekend template_id: {template_id}")
        fixed_identity = {
            "request_id": template.request_id,
            "event_id": template.event_id,
            "frame_id": template.frame_id,
            "target_id": template.target_id,
            "label_kind": template.label_kind.value,
            "allowed_evidence_reference_ids": list(
                template.allowed_evidence_reference_ids
            ),
        }
        for key, expected in fixed_identity.items():
            if annotation[key] != expected:
                raise ValueError(f"annotation wijzigt vaste template-identiteit: {key}")
        used_reference_ids = tuple(
            _list(
                annotation["evidence_reference_ids_used"],
                "evidence_reference_ids_used",
            )
        )
        _sorted_identifiers(
            used_reference_ids,
            "evidence_reference_ids_used",
            allow_empty=True,
        )
        if not set(used_reference_ids) <= set(
            template.allowed_evidence_reference_ids
        ):
            raise ValueError(
                "annotation gebruikt evidence buiten de toegelaten template-set"
            )
        source_id = _identifier(annotation["source_id"], "source_id")
        source = source_map.get(source_id)
        if source is None:
            raise ValueError(f"onbekende label source: {source_id}")
        annotator_id = _identifier(annotation["annotator_id"], "annotator_id")
        if annotator_id not in source_annotators[source_id]:
            raise ValueError("annotator_id is niet aan de source gekoppeld")
        role_id = _identifier(annotation["annotator_role_id"], "annotator_role_id")
        if role_id not in pack.request.protocol.annotator_role_ids:
            raise ValueError("annotator_role_id is niet door het protocol toegestaan")
        started = _finite(
            annotation["annotation_started_at_utc"],
            "annotation_started_at_utc",
        )
        completed = _finite(
            annotation["annotation_completed_at_utc"],
            "annotation_completed_at_utc",
        )
        if (
            started < pack.request.created_at_utc
            or completed < started
            or completed > submitted_at
        ):
            raise ValueError("ongeldige annotatie-/submissiontijd")
        blinded = annotation["blinded_to_candidate"]
        if not isinstance(blinded, bool):
            raise TypeError("blinded_to_candidate moet bool zijn")
        if blinded != source.blinded_to_candidate:
            raise ValueError("annotation/source blindingattestatie verschilt")
        if pack.request.protocol.required_blinding and not blinded:
            raise ValueError("protocol vereist geblindeerde annotatie")
        state = LabelState(annotation["label_state"])
        payload = _mapping(annotation["payload"], "payload")
        missing_reason = annotation["missing_annotation_reason"]
        if missing_reason is not None:
            _identifier(missing_reason, "missing_annotation_reason")
            if state is not LabelState.UNKNOWN:
                raise ValueError("missing annotation vereist label_state unknown")
            if payload != template.empty_payload:
                raise ValueError("missing annotation mag geen truth payload invullen")
            label_id = None
            unresolved.append(template_id)
        else:
            _enforce_fixed_payload(template, payload)
            typed_payload = _typed_payload(template.label_kind, payload)
            label_id = "external-label:" + _sha256_json(
                {
                    "pack": pack.sha256,
                    "template_id": template_id,
                    "source_id": source_id,
                    "payload": payload,
                    "state": state.value,
                }
            )
            labels.append(
                ExternalLabel(
                    label_id=label_id,
                    label_kind=template.label_kind,
                    source_id=source_id,
                    event_id=template.event_id,
                    frame_id=template.frame_id,
                    state=state,
                    payload=typed_payload,
                )
            )
        provenance.append(
            AnnotationProvenance(
                template_id=template_id,
                label_id=label_id,
                source_id=source_id,
                annotator_id=annotator_id,
                annotator_role_id=role_id,
                annotation_started_at_utc=started,
                annotation_completed_at_utc=completed,
                blinded_to_candidate=blinded,
                evidence_reference_ids_used=used_reference_ids,
                missing_annotation_reason=missing_reason,
            )
        )
    if seen != set(templates):
        raise ValueError("submission mist een of meer templates")
    corpus = ExternalLabelCorpus(
        sources=tuple(sorted(sources, key=lambda item: item.source_id)),
        labels=tuple(sorted(labels, key=lambda item: item.label_id)),
    )
    truth_context = ExternalTruthContext(split=pack.split, labels=corpus)
    return AnnotationIngestResult(
        annotation_pack_sha256=pack.sha256,
        submission_id=submission_id,
        submitted_at_utc=submitted_at,
        corpus=corpus,
        truth_context=truth_context,
        provenance=tuple(sorted(provenance, key=lambda item: item.template_id)),
        unresolved_template_ids=tuple(sorted(unresolved)),
    )


def annotation_pack_request_from_dict(
    raw: Mapping[str, Any],
) -> AnnotationPackRequest:
    """Parse a strict caller-supplied export request."""

    document = _mapping(raw, "request")
    _exact_keys(
        document,
        {
            "request_id",
            "created_at_utc",
            "source_database_id",
            "time_standard",
            "protocol",
            "split_policy",
            "events",
        },
        "request",
    )
    protocol_raw = _mapping(document["protocol"], "protocol")
    _exact_keys(
        protocol_raw,
        {
            "protocol_id",
            "protocol_version",
            "instructions_sha256",
            "required_blinding",
            "annotator_role_ids",
            "allowed_source_kinds",
        },
        "protocol",
    )
    protocol = AnnotationProtocol(
        protocol_id=protocol_raw["protocol_id"],
        protocol_version=protocol_raw["protocol_version"],
        instructions_sha256=protocol_raw["instructions_sha256"],
        required_blinding=protocol_raw["required_blinding"],
        annotator_role_ids=tuple(
            _list(protocol_raw["annotator_role_ids"], "annotator_role_ids")
        ),
        allowed_source_kinds=tuple(
            ExternalSourceKind(value)
            for value in _list(
                protocol_raw["allowed_source_kinds"],
                "allowed_source_kinds",
            )
        ),
    )
    policy_raw = _mapping(document["split_policy"], "split_policy")
    _exact_keys(
        policy_raw,
        {
            "policy_id",
            "hash_salt",
            "train_basis_points",
            "validation_basis_points",
            "test_basis_points",
            "algorithm_version",
        },
        "split_policy",
    )
    policy = EventSplitPolicy(
        policy_id=policy_raw["policy_id"],
        hash_salt=policy_raw["hash_salt"],
        train_basis_points=policy_raw["train_basis_points"],
        validation_basis_points=policy_raw["validation_basis_points"],
        test_basis_points=policy_raw["test_basis_points"],
        algorithm_version=policy_raw["algorithm_version"],
    )
    events = tuple(
        _event_from_dict(item)
        for item in _list(document["events"], "events")
    )
    return AnnotationPackRequest(
        request_id=document["request_id"],
        created_at_utc=document["created_at_utc"],
        source_database_id=document["source_database_id"],
        time_standard=document["time_standard"],
        protocol=protocol,
        split_policy=policy,
        events=events,
    )


def annotation_pack_from_dict(raw: Mapping[str, Any]) -> AnnotationPack:
    """Load a pack and verify its full canonical SHA-256."""

    document = _mapping(raw, "annotation pack")
    _exact_keys(
        document,
        {
            "format_version",
            "annotation_pack_sha256",
            "request",
            "source_database",
            "split",
            "evidence_references",
            "missing_evidence",
            "templates",
            "scientific_contract",
        },
        "annotation pack",
    )
    request = annotation_pack_request_from_dict(
        _mapping(document["request"], "request")
    )
    database_raw = _mapping(document["source_database"], "source_database")
    _exact_keys(
        database_raw,
        {
            "source_database_id",
            "database_sha256",
            "size_bytes",
            "schema_sha256",
            "table_names",
            "connection_mode",
        },
        "source_database",
    )
    source_database = SourceDatabaseManifest(
        source_database_id=database_raw["source_database_id"],
        database_sha256=database_raw["database_sha256"],
        size_bytes=database_raw["size_bytes"],
        schema_sha256=database_raw["schema_sha256"],
        table_names=tuple(
            _list(database_raw["table_names"], "table_names")
        ),
        connection_mode=database_raw["connection_mode"],
    )
    split = _split_from_dict(_mapping(document["split"], "split"))
    references = tuple(
        _reference_from_dict(item)
        for item in _list(
            document["evidence_references"],
            "evidence_references",
        )
    )
    missing = tuple(
        _missing_from_dict(item)
        for item in _list(document["missing_evidence"], "missing_evidence")
    )
    templates = tuple(
        _template_from_dict(item)
        for item in _list(document["templates"], "templates")
    )
    expected_contract = {
        "labels_created_by_export": False,
        "metrics_computed_by_export": False,
        "margins_selected_by_export": False,
        "frame_level_split_allowed": False,
        "engine_context_is_truth": False,
    }
    if document["scientific_contract"] != expected_contract:
        raise ValueError("scientific_contract is gewijzigd")
    pack = AnnotationPack(
        request=request,
        source_database=source_database,
        split=split,
        evidence_references=references,
        missing_evidence=missing,
        templates=templates,
        format_version=document["format_version"],
    )
    if document["annotation_pack_sha256"] != pack.sha256:
        raise ValueError("annotation pack-inhoud faalt SHA-256-verificatie")
    return pack


def _event_from_dict(raw: Any) -> AnnotationEventSpec:
    value = _mapping(raw, "event")
    _exact_keys(
        value,
        {
            "event_id",
            "window_start_utc",
            "window_end_utc",
            "leakage_group_ids",
            "provider_ids",
            "evidence_kinds",
            "frames",
            "targets",
            "label_requests",
        },
        "event",
    )
    frames: list[AnnotationFrameSpec] = []
    for raw_frame in _list(value["frames"], "frames"):
        frame = _mapping(raw_frame, "frame")
        _exact_keys(
            frame,
            {"frame_id", "valid_time_utc", "grid_id", "variable_id", "unit_id"},
            "frame",
        )
        frames.append(AnnotationFrameSpec(**frame))
    targets: list[AnnotationTargetSpec] = []
    for raw_target in _list(value["targets"], "targets"):
        target = _mapping(raw_target, "target")
        _exact_keys(
            target,
            {
                "target_id",
                "target_kind",
                "subject_id",
                "latitude",
                "longitude",
            },
            "target",
        )
        targets.append(
            AnnotationTargetSpec(
                target_id=target["target_id"],
                target_kind=TargetKind(target["target_kind"]),
                subject_id=target["subject_id"],
                latitude=target["latitude"],
                longitude=target["longitude"],
            )
        )
    requests: list[AnnotationLabelRequest] = []
    for raw_request in _list(value["label_requests"], "label_requests"):
        request = _mapping(raw_request, "label_request")
        _exact_keys(
            request,
            {"request_id", "label_kind", "frame_id", "target_id"},
            "label_request",
        )
        requests.append(
            AnnotationLabelRequest(
                request_id=request["request_id"],
                label_kind=ExternalLabelKind(request["label_kind"]),
                frame_id=request["frame_id"],
                target_id=request["target_id"],
            )
        )
    return AnnotationEventSpec(
        event_id=value["event_id"],
        window_start_utc=value["window_start_utc"],
        window_end_utc=value["window_end_utc"],
        leakage_group_ids=tuple(
            _list(value["leakage_group_ids"], "leakage_group_ids")
        ),
        provider_ids=tuple(_list(value["provider_ids"], "provider_ids")),
        evidence_kinds=tuple(
            EvidenceKind(item)
            for item in _list(value["evidence_kinds"], "evidence_kinds")
        ),
        frames=tuple(frames),
        targets=tuple(targets),
        label_requests=tuple(requests),
    )


def _split_from_dict(raw: Mapping[str, Any]) -> EventSplit:
    _exact_keys(
        raw,
        {
            "corpus_id",
            "policy_fingerprint",
            "frame_events",
            "frame_partitions",
            "event_partitions",
            "group_partitions",
            "event_components",
        },
        "split",
    )
    from .validation_corpus import CorpusPartition

    return EventSplit(
        corpus_id=raw["corpus_id"],
        policy_fingerprint=raw["policy_fingerprint"],
        frame_events=tuple(
            (str(item[0]), str(item[1]))
            for item in _pairs(raw["frame_events"], "frame_events")
        ),
        frame_partitions=tuple(
            (str(item[0]), CorpusPartition(item[1]))
            for item in _pairs(raw["frame_partitions"], "frame_partitions")
        ),
        event_partitions=tuple(
            (str(item[0]), CorpusPartition(item[1]))
            for item in _pairs(raw["event_partitions"], "event_partitions")
        ),
        group_partitions=tuple(
            (str(item[0]), CorpusPartition(item[1]))
            for item in _pairs(raw["group_partitions"], "group_partitions")
        ),
        event_components=tuple(
            (str(item[0]), str(item[1]))
            for item in _pairs(raw["event_components"], "event_components")
        ),
    )


def _reference_from_dict(raw: Any) -> EvidenceReference:
    value = _mapping(raw, "evidence reference")
    _exact_keys(
        value,
        {
            "reference_id",
            "event_id",
            "evidence_kind",
            "provider_id",
            "target_id",
            "source_table",
            "source_record_id",
            "observed_start_utc",
            "observed_end_utc",
            "received_at_utc",
            "artifact_sha256",
            "record_sha256",
            "quantity",
            "unit_id",
            "grid_id",
            "truth_eligibility",
            "annotator_visibility",
            "metadata",
        },
        "evidence reference",
    )
    return EvidenceReference(
        reference_id=value["reference_id"],
        event_id=value["event_id"],
        evidence_kind=EvidenceKind(value["evidence_kind"]),
        provider_id=value["provider_id"],
        target_id=value["target_id"],
        source_table=value["source_table"],
        source_record_id=value["source_record_id"],
        observed_start_utc=value["observed_start_utc"],
        observed_end_utc=value["observed_end_utc"],
        received_at_utc=value["received_at_utc"],
        artifact_sha256=value["artifact_sha256"],
        record_sha256=value["record_sha256"],
        quantity=value["quantity"],
        unit_id=value["unit_id"],
        grid_id=value["grid_id"],
        truth_eligibility=value["truth_eligibility"],
        annotator_visibility=value["annotator_visibility"],
        metadata_json=_canonical_json(value["metadata"]),
    )


def _missing_from_dict(raw: Any) -> MissingEvidenceRecord:
    value = _mapping(raw, "missing evidence")
    _exact_keys(
        value,
        {
            "missing_id",
            "event_id",
            "evidence_kind",
            "provider_id",
            "target_id",
            "window_start_utc",
            "window_end_utc",
            "reason_code",
            "queried_tables",
        },
        "missing evidence",
    )
    return MissingEvidenceRecord(
        missing_id=value["missing_id"],
        event_id=value["event_id"],
        evidence_kind=EvidenceKind(value["evidence_kind"]),
        provider_id=value["provider_id"],
        target_id=value["target_id"],
        window_start_utc=value["window_start_utc"],
        window_end_utc=value["window_end_utc"],
        reason_code=value["reason_code"],
        queried_tables=tuple(
            _list(value["queried_tables"], "queried_tables")
        ),
    )


def _template_from_dict(raw: Any) -> LabelTemplateSpec:
    value = _mapping(raw, "template")
    _exact_keys(
        value,
        {
            "template_id",
            "request_id",
            "event_id",
            "frame_id",
            "target_id",
            "label_kind",
            "fixed_payload",
            "empty_payload",
            "allowed_evidence_reference_ids",
        },
        "template",
    )
    return LabelTemplateSpec(
        template_id=value["template_id"],
        request_id=value["request_id"],
        event_id=value["event_id"],
        frame_id=value["frame_id"],
        target_id=value["target_id"],
        label_kind=ExternalLabelKind(value["label_kind"]),
        fixed_payload_json=_canonical_json(value["fixed_payload"]),
        empty_payload_json=_canonical_json(value["empty_payload"]),
        allowed_evidence_reference_ids=tuple(
            _list(
                value["allowed_evidence_reference_ids"],
                "allowed_evidence_reference_ids",
            )
        ),
    )


def _collect_evidence(
    database: sqlite3.Connection,
    tables: set[str],
    events: tuple[AnnotationEventSpec, ...],
) -> tuple[EvidenceReference, ...]:
    references: dict[str, EvidenceReference] = {}
    for event in events:
        event_references: list[EvidenceReference] = []
        if EvidenceKind.UPSTREAM_ARTIFACT in event.evidence_kinds:
            event_references.extend(
                _vnext_raw_receipts(database, tables, event)
            )
            event_references.extend(
                _legacy_original_radar(database, tables, event)
            )
        if EvidenceKind.RADAR_FRAME_METADATA in event.evidence_kinds:
            event_references.extend(_legacy_frames(database, tables, event))
            event_references.extend(_vnext_native_frames(database, tables, event))
        if EvidenceKind.GROUND_FACT in event.evidence_kinds:
            event_references.extend(
                _vnext_ground_observations(database, tables, event)
            )
            event_references.extend(
                _legacy_ground_observations(database, tables, event)
            )
        if EvidenceKind.ENGINE_CONTEXT in event.evidence_kinds:
            event_references.extend(
                _legacy_engine_context(database, tables, event)
            )
            event_references.extend(
                _vnext_analysis_context(database, tables, event)
            )
        for reference in event_references:
            prior = references.get(reference.reference_id)
            if prior is not None and prior != reference:
                raise RuntimeError("evidence reference-id collision")
            references[reference.reference_id] = reference
    return tuple(sorted(references.values(), key=lambda item: item.reference_id))


def _vnext_raw_receipts(
    database: sqlite3.Connection,
    tables: set[str],
    event: AnnotationEventSpec,
) -> list[EvidenceReference]:
    table = "meteorological_raw_receipts"
    required = {
        "receipt_id",
        "record_hash",
        "provider_id",
        "upstream_record_id",
        "content_sha256",
        "observed_start",
        "observed_end",
        "received_at",
        "time_quality_json",
        "media_type",
        "parser_version",
    }
    if table not in tables or not required <= _columns(database, table):
        return []
    placeholders = ",".join("?" for _ in event.provider_ids)
    rows = database.execute(
        "SELECT receipt_id, record_hash, provider_id, upstream_record_id, "
        "content_sha256, observed_start, observed_end, received_at, "
        "time_quality_json, media_type, parser_version "
        f"FROM {table} WHERE provider_id IN ({placeholders}) "
        "AND COALESCE(observed_start, received_at) <= ? "
        "AND COALESCE(observed_end, observed_start, received_at) >= ? "
        "ORDER BY provider_id, receipt_id",
        (*event.provider_ids, event.window_end_utc, event.window_start_utc),
    ).fetchall()
    result: list[EvidenceReference] = []
    for row in rows:
        result.append(
            _reference(
                event_id=event.event_id,
                evidence_kind=EvidenceKind.UPSTREAM_ARTIFACT,
                provider_id=str(row["provider_id"]),
                target_id=None,
                source_table=table,
                source_record_id=str(row["receipt_id"]),
                observed_start=_optional_timestamp(row["observed_start"]),
                observed_end=_optional_timestamp(row["observed_end"]),
                received_at=_optional_timestamp(row["received_at"]),
                artifact_sha256=_valid_sha_or_none(row["content_sha256"]),
                record_sha256=_record_hash(row["record_hash"], dict(row)),
                quantity=None,
                unit_id=None,
                grid_id=None,
                truth_eligibility="unadjudicated_external_evidence",
                annotator_visibility="allowed",
                metadata={
                    "upstream_record_id": str(row["upstream_record_id"]),
                    "time_quality": _json_or_literal(row["time_quality_json"]),
                    "media_type": str(row["media_type"]),
                    "parser_version": str(row["parser_version"]),
                    "capture_fidelity": "exact_content_addressed_upstream_blob",
                },
            )
        )
    return result


def _legacy_original_radar(
    database: sqlite3.Connection,
    tables: set[str],
    event: AnnotationEventSpec,
) -> list[EvidenceReference]:
    table = "original_radar_frames"
    required = {
        "region_id",
        "source",
        "nominal_timestamp",
        "saved_at",
        "encoding",
        "payload",
    }
    if table not in tables or not required <= _columns(database, table):
        return []
    rows = database.execute(
        "SELECT region_id, source, nominal_timestamp, saved_at, encoding, payload "
        f"FROM {table} ORDER BY source, nominal_timestamp, region_id"
    ).fetchall()
    result: list[EvidenceReference] = []
    providers = set(event.provider_ids)
    for row in rows:
        provider = str(row["source"])
        observed = _optional_timestamp(row["nominal_timestamp"])
        if (
            provider not in providers
            or observed is None
            or not event.window_start_utc <= observed <= event.window_end_utc
        ):
            continue
        payload = row["payload"]
        artifact_sha = _sha256_blob(payload)
        source_id = (
            f"{row['region_id']}:{provider}:{row['nominal_timestamp']}:"
            f"{row['saved_at']}"
        )
        result.append(
            _reference(
                event_id=event.event_id,
                evidence_kind=EvidenceKind.UPSTREAM_ARTIFACT,
                provider_id=provider,
                target_id=None,
                source_table=table,
                source_record_id=source_id,
                observed_start=observed,
                observed_end=observed,
                received_at=_optional_timestamp(row["saved_at"]),
                artifact_sha256=artifact_sha,
                record_sha256=_sha256_json(
                    {
                        "region_id": row["region_id"],
                        "source": provider,
                        "nominal_timestamp": row["nominal_timestamp"],
                        "saved_at": row["saved_at"],
                        "encoding": row["encoding"],
                        "payload_sha256": artifact_sha,
                    }
                ),
                quantity=None,
                unit_id=None,
                grid_id=None,
                truth_eligibility="legacy_provider_overlay_not_exact_raw",
                annotator_visibility="allowed",
                metadata={
                    "region_id": str(row["region_id"]),
                    "encoding": str(row["encoding"]),
                    "capture_fidelity": (
                        "legacy_provider_overlay; not exact upstream bytes"
                    ),
                },
            )
        )
    return result


def _legacy_frames(
    database: sqlite3.Connection,
    tables: set[str],
    event: AnnotationEventSpec,
) -> list[EvidenceReference]:
    table = "frames"
    required = {"id", "source", "product_timestamp"}
    if table not in tables or not required <= _columns(database, table):
        return []
    columns = _columns(database, table)
    optional = [
        name
        for name in (
            "region_key",
            "nominal_minute",
            "collected_at",
            "grid_deg",
            "observation_count",
            "grid_point_count",
            "coverage_lon_min",
            "coverage_lat_min",
            "coverage_lon_max",
            "coverage_lat_max",
            "coverage_fraction",
        )
        if name in columns
    ]
    selected = ["id", "source", "product_timestamp", *optional]
    rows = database.execute(
        "SELECT "
        + ", ".join(_quoted_identifier(name) for name in selected)
        + f" FROM {table} ORDER BY source, product_timestamp, id"
    ).fetchall()
    result: list[EvidenceReference] = []
    providers = set(event.provider_ids)
    for row in rows:
        provider = str(row["source"])
        observed = _optional_timestamp(row["product_timestamp"])
        if (
            provider not in providers
            or observed is None
            or not event.window_start_utc <= observed <= event.window_end_utc
        ):
            continue
        metadata = {name: row[name] for name in optional}
        record_document = {
            "id": row["id"],
            "source": provider,
            "product_timestamp": row["product_timestamp"],
            **metadata,
        }
        result.append(
            _reference(
                event_id=event.event_id,
                evidence_kind=EvidenceKind.RADAR_FRAME_METADATA,
                provider_id=provider,
                target_id=None,
                source_table=table,
                source_record_id=str(row["id"]),
                observed_start=observed,
                observed_end=observed,
                received_at=_optional_timestamp(
                    row["collected_at"] if "collected_at" in columns else None
                ),
                artifact_sha256=None,
                record_sha256=_sha256_json(record_document),
                quantity=None,
                unit_id=None,
                grid_id=None,
                truth_eligibility="unadjudicated_external_evidence",
                annotator_visibility="allowed",
                metadata=metadata,
            )
        )
    return result


def _vnext_native_frames(
    database: sqlite3.Connection,
    tables: set[str],
    event: AnnotationEventSpec,
) -> list[EvidenceReference]:
    table = "meteorological_native_frames"
    receipt_table = "meteorological_raw_receipts"
    required_frame = {
        "native_frame_id",
        "record_hash",
        "receipt_id",
        "source_frame_id",
        "observed_start",
        "observed_end",
        "received_at",
        "time_quality_json",
        "quantity",
        "unit",
        "native_projection",
        "width",
        "height",
        "native_resolution_x",
        "native_resolution_y",
        "encoding",
        "frame_sha256",
        "parser_version",
        "calibration_id",
    }
    required_receipt = {"receipt_id", "provider_id"}
    if (
        table not in tables
        or receipt_table not in tables
        or not required_frame <= _columns(database, table)
        or not required_receipt <= _columns(database, receipt_table)
    ):
        return []
    placeholders = ",".join("?" for _ in event.provider_ids)
    rows = database.execute(
        "SELECT f.native_frame_id, f.record_hash, f.source_frame_id, "
        "f.observed_start, f.observed_end, f.received_at, f.time_quality_json, "
        "f.quantity, f.unit, f.native_projection, f.width, f.height, "
        "f.native_resolution_x, f.native_resolution_y, f.encoding, "
        "f.frame_sha256, f.parser_version, f.calibration_id, r.provider_id "
        f"FROM {table} AS f JOIN {receipt_table} AS r "
        "ON r.receipt_id=f.receipt_id "
        f"WHERE r.provider_id IN ({placeholders}) "
        "AND COALESCE(f.observed_start, f.received_at) <= ? "
        "AND COALESCE(f.observed_end, f.observed_start, f.received_at) >= ? "
        "ORDER BY r.provider_id, f.native_frame_id",
        (*event.provider_ids, event.window_end_utc, event.window_start_utc),
    ).fetchall()
    result = []
    for row in rows:
        result.append(
            _reference(
                event_id=event.event_id,
                evidence_kind=EvidenceKind.RADAR_FRAME_METADATA,
                provider_id=str(row["provider_id"]),
                target_id=None,
                source_table=table,
                source_record_id=str(row["native_frame_id"]),
                observed_start=_optional_timestamp(row["observed_start"]),
                observed_end=_optional_timestamp(row["observed_end"]),
                received_at=_optional_timestamp(row["received_at"]),
                artifact_sha256=_valid_sha_or_none(row["frame_sha256"]),
                record_sha256=_record_hash(row["record_hash"], dict(row)),
                quantity=str(row["quantity"]),
                unit_id=str(row["unit"]),
                grid_id=None,
                truth_eligibility="unadjudicated_external_evidence",
                annotator_visibility="allowed",
                metadata={
                    "source_frame_id": str(row["source_frame_id"]),
                    "time_quality": _json_or_literal(row["time_quality_json"]),
                    "native_projection": str(row["native_projection"]),
                    "width": int(row["width"]),
                    "height": int(row["height"]),
                    "native_resolution_x": float(row["native_resolution_x"]),
                    "native_resolution_y": float(row["native_resolution_y"]),
                    "encoding": str(row["encoding"]),
                    "parser_version": str(row["parser_version"]),
                    "calibration_id": str(row["calibration_id"]),
                },
            )
        )
    return result


def _vnext_ground_observations(
    database: sqlite3.Connection,
    tables: set[str],
    event: AnnotationEventSpec,
) -> list[EvidenceReference]:
    required_tables = {
        "meteorological_ground_observations",
        "meteorological_raw_receipts",
        "meteorological_physical_site_aliases",
        "meteorological_physical_sites",
    }
    if not required_tables <= tables:
        return []
    ground_required = {
        "ground_observation_id",
        "record_hash",
        "receipt_id",
        "site_alias_id",
        "source_observation_id",
        "observed_start",
        "observed_end",
        "received_at",
        "time_quality_json",
        "quantity",
        "unit",
        "value_numeric",
        "categorical_json",
        "parser_version",
        "calibration_id",
        "quality_json",
    }
    receipt_required = {
        "receipt_id",
        "provider_id",
        "content_sha256",
        "upstream_record_id",
    }
    alias_required = {
        "site_alias_id",
        "physical_site_id",
        "source_id",
        "external_station_id",
    }
    site_required = {
        "physical_site_id",
        "latitude",
        "longitude",
        "elevation_m",
        "independence_group",
    }
    if (
        not ground_required
        <= _columns(database, "meteorological_ground_observations")
        or not receipt_required
        <= _columns(database, "meteorological_raw_receipts")
        or not alias_required
        <= _columns(database, "meteorological_physical_site_aliases")
        or not site_required
        <= _columns(database, "meteorological_physical_sites")
    ):
        return []
    placeholders = ",".join("?" for _ in event.provider_ids)
    rows = database.execute(
        "SELECT g.ground_observation_id, g.record_hash, "
        "g.source_observation_id, g.observed_start, g.observed_end, "
        "g.received_at, g.time_quality_json, g.quantity, g.unit, "
        "g.value_numeric, g.categorical_json, g.parser_version, "
        "g.calibration_id, g.quality_json, r.provider_id, "
        "r.content_sha256, r.upstream_record_id, a.site_alias_id, "
        "a.external_station_id, s.physical_site_id, s.latitude, "
        "s.longitude, s.elevation_m, s.independence_group "
        "FROM meteorological_ground_observations AS g "
        "JOIN meteorological_raw_receipts AS r ON r.receipt_id=g.receipt_id "
        "JOIN meteorological_physical_site_aliases AS a "
        "ON a.site_alias_id=g.site_alias_id "
        "JOIN meteorological_physical_sites AS s "
        "ON s.physical_site_id=a.physical_site_id "
        f"WHERE r.provider_id IN ({placeholders}) "
        "AND COALESCE(g.observed_start, g.received_at) <= ? "
        "AND COALESCE(g.observed_end, g.observed_start, g.received_at) >= ? "
        "ORDER BY r.provider_id, g.ground_observation_id",
        (*event.provider_ids, event.window_end_utc, event.window_start_utc),
    ).fetchall()
    target_by_subject = {
        target.subject_id: target.target_id
        for target in event.targets
        if target.target_kind is TargetKind.SITE
    }
    result: list[EvidenceReference] = []
    for row in rows:
        target_id = next(
            (
                target_by_subject[value]
                for value in (
                    str(row["site_alias_id"]),
                    str(row["external_station_id"]),
                    str(row["physical_site_id"]),
                )
                if value in target_by_subject
            ),
            None,
        )
        result.append(
            _reference(
                event_id=event.event_id,
                evidence_kind=EvidenceKind.GROUND_FACT,
                provider_id=str(row["provider_id"]),
                target_id=target_id,
                source_table="meteorological_ground_observations",
                source_record_id=str(row["ground_observation_id"]),
                observed_start=_optional_timestamp(row["observed_start"]),
                observed_end=_optional_timestamp(row["observed_end"]),
                received_at=_optional_timestamp(row["received_at"]),
                artifact_sha256=_valid_sha_or_none(row["content_sha256"]),
                record_sha256=_record_hash(row["record_hash"], dict(row)),
                quantity=str(row["quantity"]),
                unit_id=str(row["unit"]),
                grid_id=None,
                truth_eligibility="unadjudicated_external_evidence",
                annotator_visibility="allowed",
                metadata={
                    "source_observation_id": str(row["source_observation_id"]),
                    "upstream_record_id": str(row["upstream_record_id"]),
                    "time_quality": _json_or_literal(row["time_quality_json"]),
                    "value_numeric": row["value_numeric"],
                    "categorical": _json_or_literal(row["categorical_json"]),
                    "quality": _json_or_literal(row["quality_json"]),
                    "parser_version": str(row["parser_version"]),
                    "calibration_id": str(row["calibration_id"]),
                    "site_alias_id": str(row["site_alias_id"]),
                    "external_station_id": str(row["external_station_id"]),
                    "physical_site_id": str(row["physical_site_id"]),
                    "latitude": float(row["latitude"]),
                    "longitude": float(row["longitude"]),
                    "elevation_m": row["elevation_m"],
                    "independence_group": str(row["independence_group"]),
                    "automatic_truth_promotion": False,
                },
            )
        )
    return result


def _legacy_ground_observations(
    database: sqlite3.Connection,
    tables: set[str],
    event: AnnotationEventSpec,
) -> list[EvidenceReference]:
    table = "ground_observations"
    if table not in tables:
        return []
    columns = _columns(database, table)
    if not {"provider", "precipitation_present"} <= columns:
        return []
    time_column = next(
        (
            name
            for name in ("observed_at", "timestamp", "received_at")
            if name in columns
        ),
        None,
    )
    if time_column is None:
        return []
    optional = [
        name
        for name in (
            "id",
            "site_id",
            "station_id",
            "received_at",
            "quantity",
            "unit",
            "value_numeric",
            "capable_of_dry_detection",
            "quality_json",
        )
        if name in columns and name != time_column
    ]
    selected = ["provider", "precipitation_present", time_column, *optional]
    rows = database.execute(
        "SELECT "
        + ", ".join(_quoted_identifier(name) for name in selected)
        + f" FROM {table} ORDER BY provider, {_quoted_identifier(time_column)}"
    ).fetchall()
    providers = set(event.provider_ids)
    target_by_subject = {
        target.subject_id: target.target_id
        for target in event.targets
        if target.target_kind is TargetKind.SITE
    }
    result: list[EvidenceReference] = []
    for index, row in enumerate(rows):
        provider = str(row["provider"])
        observed = _optional_timestamp(row[time_column])
        if (
            provider not in providers
            or observed is None
            or not event.window_start_utc <= observed <= event.window_end_utc
        ):
            continue
        subject = next(
            (
                str(row[name])
                for name in ("site_id", "station_id")
                if name in columns and row[name] is not None
            ),
            None,
        )
        target_id = target_by_subject.get(subject) if subject else None
        metadata = {
            "precipitation_present": row["precipitation_present"],
            "automatic_truth_promotion": False,
            **{name: row[name] for name in optional},
        }
        source_id = (
            str(row["id"])
            if "id" in columns and row["id"] is not None
            else f"legacy-ground:{provider}:{observed}:{index}"
        )
        result.append(
            _reference(
                event_id=event.event_id,
                evidence_kind=EvidenceKind.GROUND_FACT,
                provider_id=provider,
                target_id=target_id,
                source_table=table,
                source_record_id=source_id,
                observed_start=observed,
                observed_end=observed,
                received_at=_optional_timestamp(
                    row["received_at"] if "received_at" in columns else None
                ),
                artifact_sha256=None,
                record_sha256=_sha256_json(
                    {
                        "provider": provider,
                        "observed_at": observed,
                        **metadata,
                    }
                ),
                quantity=(
                    str(row["quantity"])
                    if "quantity" in columns and row["quantity"] is not None
                    else "precipitation_presence"
                ),
                unit_id=(
                    str(row["unit"])
                    if "unit" in columns and row["unit"] is not None
                    else "boolean"
                ),
                grid_id=None,
                truth_eligibility="unadjudicated_external_evidence",
                annotator_visibility="allowed",
                metadata=metadata,
            )
        )
    return result


def _legacy_engine_context(
    database: sqlite3.Connection,
    tables: set[str],
    event: AnnotationEventSpec,
) -> list[EvidenceReference]:
    result: list[EvidenceReference] = []
    configurations = (
        (
            "forecast_verification_samples",
            "observed_at",
            "id",
            "snapshot_json",
            "target_id",
            None,
        ),
        (
            "provider_cycle_samples",
            "observed_at",
            "sample_key",
            "metrics_json",
            None,
            "provider",
        ),
        (
            "pysteps_frame_cache",
            "nominal_timestamp",
            None,
            "frame_json",
            None,
            "source",
        ),
    )
    for (
        table,
        time_column,
        id_column,
        payload_column,
        target_column,
        provider_column,
    ) in configurations:
        if table not in tables:
            continue
        columns = _columns(database, table)
        required = {time_column, payload_column}
        if id_column:
            required.add(id_column)
        if not required <= columns:
            continue
        selected = [time_column, payload_column]
        for value in (id_column, target_column, provider_column):
            if value and value in columns and value not in selected:
                selected.append(value)
        rows = database.execute(
            "SELECT "
            + ", ".join(_quoted_identifier(name) for name in selected)
            + f" FROM {table} ORDER BY {_quoted_identifier(time_column)}"
        ).fetchall()
        for index, row in enumerate(rows):
            observed = _optional_timestamp(row[time_column])
            if (
                observed is None
                or not event.window_start_utc <= observed <= event.window_end_utc
            ):
                continue
            provider = (
                str(row[provider_column])
                if provider_column
                and provider_column in columns
                and row[provider_column] is not None
                else None
            )
            if provider is not None and provider not in event.provider_ids:
                continue
            target = (
                str(row[target_column])
                if target_column
                and target_column in columns
                and row[target_column] is not None
                else None
            )
            if target is not None and target not in {
                value.target_id for value in event.targets
            }:
                target = None
            artifact_sha = _sha256_blob(row[payload_column])
            source_id = (
                str(row[id_column])
                if id_column is not None
                else f"{table}:{provider or 'none'}:{observed}:{index}"
            )
            result.append(
                _reference(
                    event_id=event.event_id,
                    evidence_kind=EvidenceKind.ENGINE_CONTEXT,
                    provider_id=provider,
                    target_id=target,
                    source_table=table,
                    source_record_id=source_id,
                    observed_start=observed,
                    observed_end=observed,
                    received_at=None,
                    artifact_sha256=artifact_sha,
                    record_sha256=_sha256_json(
                        {
                            "source_record_id": source_id,
                            "time": observed,
                            "payload_sha256": artifact_sha,
                        }
                    ),
                    quantity=None,
                    unit_id=None,
                    grid_id=None,
                    truth_eligibility="engine_context_not_truth",
                    annotator_visibility="prohibited",
                    metadata={
                        "payload_field": payload_column,
                        "candidate_engine_output": True,
                    },
                )
            )
    return result


def _vnext_analysis_context(
    database: sqlite3.Connection,
    tables: set[str],
    event: AnnotationEventSpec,
) -> list[EvidenceReference]:
    table = "meteorological_analysis_frames"
    native = "meteorological_native_frames"
    receipt = "meteorological_raw_receipts"
    if not {table, native, receipt} <= tables:
        return []
    required_analysis = {
        "analysis_frame_id",
        "record_hash",
        "native_frame_id",
        "analysis_grid_id",
        "observed_start",
        "observed_end",
        "quantity",
        "unit",
        "frame_sha256",
        "transform_version",
        "calibration_id",
    }
    if (
        not required_analysis <= _columns(database, table)
        or not {"native_frame_id", "receipt_id"} <= _columns(database, native)
        or not {"receipt_id", "provider_id"} <= _columns(database, receipt)
    ):
        return []
    placeholders = ",".join("?" for _ in event.provider_ids)
    rows = database.execute(
        "SELECT a.analysis_frame_id, a.record_hash, a.analysis_grid_id, "
        "a.observed_start, a.observed_end, a.quantity, a.unit, "
        "a.frame_sha256, a.transform_version, a.calibration_id, r.provider_id "
        f"FROM {table} AS a JOIN {native} AS n "
        "ON n.native_frame_id=a.native_frame_id "
        f"JOIN {receipt} AS r ON r.receipt_id=n.receipt_id "
        f"WHERE r.provider_id IN ({placeholders}) "
        "AND a.observed_start <= ? AND a.observed_end >= ? "
        "ORDER BY r.provider_id, a.analysis_frame_id",
        (*event.provider_ids, event.window_end_utc, event.window_start_utc),
    ).fetchall()
    result = []
    for row in rows:
        result.append(
            _reference(
                event_id=event.event_id,
                evidence_kind=EvidenceKind.ENGINE_CONTEXT,
                provider_id=str(row["provider_id"]),
                target_id=None,
                source_table=table,
                source_record_id=str(row["analysis_frame_id"]),
                observed_start=_optional_timestamp(row["observed_start"]),
                observed_end=_optional_timestamp(row["observed_end"]),
                received_at=None,
                artifact_sha256=_valid_sha_or_none(row["frame_sha256"]),
                record_sha256=_record_hash(row["record_hash"], dict(row)),
                quantity=str(row["quantity"]),
                unit_id=str(row["unit"]),
                grid_id=str(row["analysis_grid_id"]),
                truth_eligibility="engine_context_not_truth",
                annotator_visibility="prohibited",
                metadata={
                    "transform_version": str(row["transform_version"]),
                    "calibration_id": str(row["calibration_id"]),
                    "candidate_engine_output": True,
                },
            )
        )
    return result


def _missing_evidence(
    events: tuple[AnnotationEventSpec, ...],
    references: tuple[EvidenceReference, ...],
) -> tuple[MissingEvidenceRecord, ...]:
    result: list[MissingEvidenceRecord] = []
    table_map = {
        EvidenceKind.UPSTREAM_ARTIFACT: (
            "meteorological_raw_receipts",
            "original_radar_frames",
        ),
        EvidenceKind.RADAR_FRAME_METADATA: (
            "frames",
            "meteorological_native_frames",
        ),
        EvidenceKind.GROUND_FACT: (
            "ground_observations",
            "meteorological_ground_observations",
        ),
        EvidenceKind.ENGINE_CONTEXT: tuple(sorted(_ENGINE_TABLES)),
    }
    for event in events:
        event_refs = [
            reference
            for reference in references
            if reference.event_id == event.event_id
        ]
        for kind in event.evidence_kinds:
            kind_refs = [
                reference
                for reference in event_refs
                if reference.evidence_kind is kind
            ]
            if kind is EvidenceKind.GROUND_FACT:
                site_targets = [
                    target
                    for target in event.targets
                    if target.target_kind is TargetKind.SITE
                ]
                if site_targets:
                    scopes = [
                        (provider, target.target_id)
                        for provider in event.provider_ids
                        for target in site_targets
                    ]
                else:
                    scopes = [
                        (provider, None) for provider in event.provider_ids
                    ]
            elif kind is EvidenceKind.ENGINE_CONTEXT:
                scopes = [(None, None)]
            else:
                scopes = [(provider, None) for provider in event.provider_ids]
            for provider_id, target_id in scopes:
                matching = [
                    reference
                    for reference in kind_refs
                    if (
                        provider_id is None
                        or reference.provider_id == provider_id
                    )
                    and (
                        target_id is None
                        or reference.target_id == target_id
                    )
                ]
                if matching:
                    continue
                body = {
                    "event_id": event.event_id,
                    "evidence_kind": kind.value,
                    "provider_id": provider_id,
                    "target_id": target_id,
                    "window_start_utc": event.window_start_utc,
                    "window_end_utc": event.window_end_utc,
                    "reason_code": (
                        "no_exact_target_ground_reference"
                        if target_id is not None
                        else "no_matching_record"
                    ),
                    "queried_tables": list(table_map[kind]),
                }
                result.append(
                    MissingEvidenceRecord(
                        missing_id="missing:" + _sha256_json(body),
                        event_id=event.event_id,
                        evidence_kind=kind,
                        provider_id=provider_id,
                        target_id=target_id,
                        window_start_utc=event.window_start_utc,
                        window_end_utc=event.window_end_utc,
                        reason_code=body["reason_code"],
                        queried_tables=table_map[kind],
                    )
                )
    return tuple(sorted(result, key=lambda item: item.missing_id))


def _build_split(
    events: tuple[AnnotationEventSpec, ...],
    policy: EventSplitPolicy,
) -> EventSplit:
    frames = tuple(
        EventFrame(
            frame_id=frame.frame_id,
            event_id=event.event_id,
            valid_time_utc=frame.valid_time_utc,
            leakage_group_ids=event.leakage_group_ids,
        )
        for event in events
        for frame in event.frames
    )
    return build_event_held_out_split(frames, policy)


def _build_templates(
    events: tuple[AnnotationEventSpec, ...],
    references: tuple[EvidenceReference, ...],
) -> tuple[LabelTemplateSpec, ...]:
    templates: list[LabelTemplateSpec] = []
    for event in events:
        frames = {frame.frame_id: frame for frame in event.frames}
        targets = {target.target_id: target for target in event.targets}
        visible_references = tuple(
            sorted(
                reference.reference_id
                for reference in references
                if reference.event_id == event.event_id
                and reference.annotator_visibility == "allowed"
            )
        )
        for request in event.label_requests:
            frame = frames[request.frame_id]
            target = targets[request.target_id]
            fixed, empty = _payload_templates(
                request.label_kind,
                event,
                frame,
                target,
            )
            identity = {
                "event_id": event.event_id,
                "request_id": request.request_id,
                "frame_id": request.frame_id,
                "target_id": request.target_id,
                "label_kind": request.label_kind.value,
            }
            templates.append(
                LabelTemplateSpec(
                    template_id="annotation-template:" + _sha256_json(identity),
                    request_id=request.request_id,
                    event_id=event.event_id,
                    frame_id=request.frame_id,
                    target_id=request.target_id,
                    label_kind=request.label_kind,
                    fixed_payload_json=_canonical_json(fixed),
                    empty_payload_json=_canonical_json(empty),
                    allowed_evidence_reference_ids=visible_references,
                )
            )
    return tuple(sorted(templates, key=lambda item: item.template_id))


def _payload_templates(
    kind: ExternalLabelKind,
    event: AnnotationEventSpec,
    frame: AnnotationFrameSpec,
    target: AnnotationTargetSpec,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if kind is ExternalLabelKind.CELL_IDENTITY:
        fixed: dict[str, Any] = {}
        empty = {
            "reference_cell_id": None,
            "parent_reference_cell_ids": [],
        }
    elif kind is ExternalLabelKind.CELL_POSITION:
        fixed = {}
        empty = {
            "reference_cell_id": None,
            "latitude": None,
            "longitude": None,
            "horizontal_uncertainty_m": None,
        }
    elif kind is ExternalLabelKind.CELL_SPEED:
        fixed = {
            "interval_start_utc": float(event.window_start_utc),
            "interval_end_utc": float(event.window_end_utc),
        }
        empty = {
            "reference_cell_id": None,
            "eastward_mps": None,
            "northward_mps": None,
            **fixed,
            "vector_uncertainty_mps": None,
        }
    elif kind is ExternalLabelKind.PASSAGE:
        fixed = {
            "tracker_id": target.subject_id,
            "window_start_utc": float(event.window_start_utc),
            "window_end_utc": float(event.window_end_utc),
        }
        empty = {
            **fixed,
            "occurred": None,
            "passage_time_utc": None,
            "time_uncertainty_s": None,
        }
    elif kind is ExternalLabelKind.WET_DRY:
        fixed = {
            "site_id": target.subject_id,
            "interval_start_utc": float(event.window_start_utc),
            "interval_end_utc": float(event.window_end_utc),
        }
        empty = {
            **fixed,
            "state": None,
            "capable_of_dry_detection": None,
            "accumulation_mm": None,
        }
    elif kind is ExternalLabelKind.PRECIPITATION_FIELD:
        fixed = {
            "grid_id": frame.grid_id,
            "valid_time_utc": float(frame.valid_time_utc),
            "variable_id": frame.variable_id,
            "unit_id": frame.unit_id,
        }
        empty = {**fixed, "field_artifact_sha256": None}
    else:  # pragma: no cover - closed enum
        raise AssertionError(f"unsupported label kind: {kind}")
    return fixed, empty


def _ingest_sources(
    raw_sources: Any,
    pack: AnnotationPack,
) -> tuple[tuple[ExternalLabelSource, ...], dict[str, frozenset[str]]]:
    values = _list(raw_sources, "sources")
    if not values:
        raise ValueError("submission vereist minstens één externe source")
    sources: list[ExternalLabelSource] = []
    annotators: dict[str, frozenset[str]] = {}
    engine_hashes = {
        reference.artifact_sha256
        for reference in pack.evidence_references
        if reference.truth_eligibility == "engine_context_not_truth"
        and reference.artifact_sha256 is not None
    }
    engine_hashes.add(pack.source_database.database_sha256)
    allowed = set(pack.request.protocol.allowed_source_kinds)
    for raw in values:
        value = _mapping(raw, "source")
        _exact_keys(
            value,
            {
                "source_id",
                "source_kind",
                "producer",
                "dataset_version",
                "methodology_id",
                "source_artifact_sha256",
                "candidate_independent",
                "blinded_to_candidate",
                "annotator_ids",
                "protocol_id",
                "protocol_version",
                "instructions_sha256",
                "time_standard",
                "unit_policy",
            },
            "source",
        )
        kind = ExternalSourceKind(value["source_kind"])
        if kind not in allowed:
            raise ValueError("source_kind is niet door het protocol toegestaan")
        if kind is ExternalSourceKind.ENGINE_DERIVED_DIAGNOSTIC:
            raise ValueError("engine-derived labels worden nooit geaccepteerd")
        if value["source_artifact_sha256"] in engine_hashes:
            raise ValueError("label source verwijst naar engine/DB-artifact")
        if value["protocol_id"] != pack.request.protocol.protocol_id:
            raise ValueError("source protocol_id komt niet overeen")
        if value["protocol_version"] != pack.request.protocol.protocol_version:
            raise ValueError("source protocol_version komt niet overeen")
        if (
            value["instructions_sha256"]
            != pack.request.protocol.instructions_sha256
        ):
            raise ValueError("source instructions_sha256 komt niet overeen")
        if value["time_standard"] != TIME_STANDARD:
            raise ValueError("source gebruikt niet exact UTC_UNIX_SECONDS")
        if value["unit_policy"] != UNIT_POLICY:
            raise ValueError("source gebruikt een niet-toegelaten unit policy")
        source = ExternalLabelSource(
            source_id=value["source_id"],
            source_kind=kind,
            producer=value["producer"],
            dataset_version=value["dataset_version"],
            methodology_id=value["methodology_id"],
            source_artifact_sha256=value["source_artifact_sha256"],
            candidate_independent=value["candidate_independent"],
            blinded_to_candidate=value["blinded_to_candidate"],
        )
        if not source.is_external_truth_eligible:
            raise ValueError("source is niet eligible als externe waarheid")
        if pack.request.protocol.required_blinding and not source.blinded_to_candidate:
            raise ValueError("protocol vereist geblindeerde source")
        ids = tuple(_list(value["annotator_ids"], "annotator_ids"))
        _sorted_identifiers(ids, "annotator_ids", allow_empty=False)
        if source.source_id in annotators:
            raise ValueError("source_id komt dubbel voor")
        annotators[source.source_id] = frozenset(ids)
        sources.append(source)
    return tuple(sources), annotators


def _enforce_fixed_payload(
    template: LabelTemplateSpec,
    payload: Mapping[str, Any],
) -> None:
    if set(payload) != set(template.empty_payload):
        raise ValueError("payloadvelden komen niet exact overeen met template")
    for key, expected in template.fixed_payload.items():
        if payload.get(key) != expected:
            raise ValueError(f"payload wijzigt vaste tijd/unit/source metadata: {key}")


def _typed_payload(
    kind: ExternalLabelKind,
    payload: Mapping[str, Any],
) -> (
    CellIdentityLabel
    | CellPositionLabel
    | CellSpeedLabel
    | PassageLabel
    | WetDryLabel
    | PrecipitationFieldLabel
):
    if kind is ExternalLabelKind.CELL_IDENTITY:
        return CellIdentityLabel(
            reference_cell_id=payload["reference_cell_id"],
            parent_reference_cell_ids=tuple(payload["parent_reference_cell_ids"]),
        )
    if kind is ExternalLabelKind.CELL_POSITION:
        return CellPositionLabel(
            reference_cell_id=payload["reference_cell_id"],
            latitude=payload["latitude"],
            longitude=payload["longitude"],
            horizontal_uncertainty_m=payload["horizontal_uncertainty_m"],
        )
    if kind is ExternalLabelKind.CELL_SPEED:
        return CellSpeedLabel(
            reference_cell_id=payload["reference_cell_id"],
            eastward_mps=payload["eastward_mps"],
            northward_mps=payload["northward_mps"],
            interval_start_utc=payload["interval_start_utc"],
            interval_end_utc=payload["interval_end_utc"],
            vector_uncertainty_mps=payload["vector_uncertainty_mps"],
        )
    if kind is ExternalLabelKind.PASSAGE:
        return PassageLabel(
            tracker_id=payload["tracker_id"],
            window_start_utc=payload["window_start_utc"],
            window_end_utc=payload["window_end_utc"],
            occurred=payload["occurred"],
            passage_time_utc=payload["passage_time_utc"],
            time_uncertainty_s=payload["time_uncertainty_s"],
        )
    if kind is ExternalLabelKind.WET_DRY:
        return WetDryLabel(
            site_id=payload["site_id"],
            interval_start_utc=payload["interval_start_utc"],
            interval_end_utc=payload["interval_end_utc"],
            state=WetDryState(payload["state"]),
            capable_of_dry_detection=payload["capable_of_dry_detection"],
            accumulation_mm=payload["accumulation_mm"],
        )
    if kind is ExternalLabelKind.PRECIPITATION_FIELD:
        return PrecipitationFieldLabel(
            grid_id=payload["grid_id"],
            valid_time_utc=payload["valid_time_utc"],
            variable_id=payload["variable_id"],
            unit_id=payload["unit_id"],
            field_artifact_sha256=payload["field_artifact_sha256"],
        )
    raise AssertionError(f"unsupported label kind: {kind}")  # pragma: no cover


def _reference(
    *,
    event_id: str,
    evidence_kind: EvidenceKind,
    provider_id: str | None,
    target_id: str | None,
    source_table: str,
    source_record_id: str,
    observed_start: float | None,
    observed_end: float | None,
    received_at: float | None,
    artifact_sha256: str | None,
    record_sha256: str,
    quantity: str | None,
    unit_id: str | None,
    grid_id: str | None,
    truth_eligibility: str,
    annotator_visibility: str,
    metadata: Mapping[str, Any],
) -> EvidenceReference:
    identity = {
        "event_id": event_id,
        "evidence_kind": evidence_kind.value,
        "provider_id": provider_id,
        "target_id": target_id,
        "source_table": source_table,
        "source_record_id": source_record_id,
        "record_sha256": record_sha256,
    }
    return EvidenceReference(
        reference_id="evidence:" + _sha256_json(identity),
        event_id=event_id,
        evidence_kind=evidence_kind,
        provider_id=provider_id,
        target_id=target_id,
        source_table=source_table,
        source_record_id=source_record_id,
        observed_start_utc=observed_start,
        observed_end_utc=observed_end,
        received_at_utc=received_at,
        artifact_sha256=artifact_sha256,
        record_sha256=record_sha256,
        quantity=quantity,
        unit_id=unit_id,
        grid_id=grid_id,
        truth_eligibility=truth_eligibility,
        annotator_visibility=annotator_visibility,
        metadata_json=_canonical_json(metadata),
    )


def _validate_target_label_kind(
    target_kind: TargetKind,
    label_kind: ExternalLabelKind,
) -> None:
    expected = {
        ExternalLabelKind.CELL_IDENTITY: TargetKind.CELL,
        ExternalLabelKind.CELL_POSITION: TargetKind.CELL,
        ExternalLabelKind.CELL_SPEED: TargetKind.CELL,
        ExternalLabelKind.PASSAGE: TargetKind.TRACKER,
        ExternalLabelKind.WET_DRY: TargetKind.SITE,
        ExternalLabelKind.PRECIPITATION_FIELD: TargetKind.GRID,
    }[label_kind]
    if target_kind is not expected:
        raise ValueError(
            f"{label_kind.value} vereist target_kind {expected.value}"
        )


def _columns(database: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row["name"])
        for row in database.execute(
            f"PRAGMA table_info({_quoted_identifier(table)})"
        )
    }


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _optional_timestamp(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        integer_text = str(int(numeric))
        if numeric.is_integer() and len(integer_text) in {12, 14}:
            pattern = "%Y%m%d%H%M" if len(integer_text) == 12 else "%Y%m%d%H%M%S"
            try:
                return datetime.strptime(integer_text, pattern).replace(
                    tzinfo=timezone.utc
                ).timestamp()
            except ValueError:
                pass
        return numeric
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdigit() and len(stripped) in {12, 14}:
            pattern = "%Y%m%d%H%M" if len(stripped) == 12 else "%Y%m%d%H%M%S"
            try:
                return datetime.strptime(stripped, pattern).replace(
                    tzinfo=timezone.utc
                ).timestamp()
            except ValueError:
                return None
        try:
            numeric = float(stripped)
        except ValueError:
            try:
                parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
            except ValueError:
                return None
            if parsed.tzinfo is None:
                return None
            return parsed.astimezone(timezone.utc).timestamp()
        return numeric if math.isfinite(numeric) else None
    return None


def _json_or_literal(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return {"invalid_json_literal": value}


def _record_hash(value: Any, fallback: Mapping[str, Any]) -> str:
    if isinstance(value, str) and _SHA256_RE.fullmatch(value):
        return value
    sanitized = {
        key: (
            "sha256:" + _sha256_blob(item)
            if isinstance(item, (bytes, bytearray, memoryview))
            else item
        )
        for key, item in fallback.items()
    }
    return _sha256_json(sanitized)


def _valid_sha_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and _SHA256_RE.fullmatch(value) else None


def _sha256_blob(value: Any) -> str:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, bytes):
        data = value
    elif isinstance(value, str):
        data = value.encode("utf-8")
    else:
        data = _canonical_json(value).encode("ascii")
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("ascii")).hexdigest()


def _canonical_json_string(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} moet JSON-string zijn")
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError) as err:
        raise ValueError(f"{field} bevat ongeldige JSON") from err
    if value != _canonical_json(decoded):
        raise ValueError(f"{field} moet canonieke JSON zijn")
    return value


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} moet een object zijn")
    if any(not isinstance(key, str) for key in value):
        raise TypeError(f"{field} keys moeten strings zijn")
    return value


def _list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise TypeError(f"{field} moet een lijst zijn")
    return value


def _pairs(value: Any, field: str) -> list[list[Any]]:
    values = _list(value, field)
    if any(not isinstance(item, list) or len(item) != 2 for item in values):
        raise TypeError(f"{field} moet uit paren bestaan")
    return values


def _exact_keys(
    value: Mapping[str, Any],
    expected: set[str],
    field: str,
) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise ValueError(
            f"{field} velden komen niet exact overeen; "
            f"missing={missing}, extra={extra}"
        )


def _identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} mag niet leeg zijn")
    if "\x00" in value or len(value) > 512:
        raise ValueError(f"{field} is ongeldig")
    return value


def _optional_identifier(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _identifier(value, field)


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} moet numeriek zijn")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{field} moet eindig zijn")
    return numeric


def _optional_finite(value: Any, field: str) -> float | None:
    if value is None:
        return None
    return _finite(value, field)


def _nonnegative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} moet een niet-negatieve int zijn")
    return value


def _sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{field} vereist lowercase SHA-256")
    return value


def _sorted_identifiers(
    values: Any,
    field: str,
    *,
    allow_empty: bool,
) -> None:
    if not isinstance(values, tuple):
        raise TypeError(f"{field} moet tuple zijn")
    if not values and not allow_empty:
        raise ValueError(f"{field} mag niet leeg zijn")
    for value in values:
        _identifier(value, field)
    if values != tuple(sorted(set(values))):
        raise ValueError(f"{field} moet uniek en gesorteerd zijn")


def _sorted_objects(values: Any, attribute: str, field: str) -> None:
    if not isinstance(values, tuple):
        raise TypeError(f"{field} moet tuple zijn")
    keys = [getattr(value, attribute) for value in values]
    if keys != sorted(set(keys)):
        raise ValueError(f"{field} moet uniek en op {attribute} gesorteerd zijn")
