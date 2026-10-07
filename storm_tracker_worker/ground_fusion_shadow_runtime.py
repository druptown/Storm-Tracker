"""Durable worker-side shadow orchestration for cell-specific ground fusion.

This module composes existing authorities without changing their scientific
meaning::

    canonical ground store -> certified semantics bridge -> physical sites
    -> exact CellTrack footprint -> interval-aware ground fusion

It is deliberately not imported by the worker service and cannot publish an
operational confirmation.  Every fact returned by the bounded source query
receives exactly one terminal disposition.  A partial/overflowing query, an
invalid target/track binding, corrupt evidence or an exhausted bound produces
an indeterminate shadow receipt and never a weather claim.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
import base64
import binascii
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping, Sequence

from .canonical_ground_evidence_store import (
    CanonicalGroundEvidenceCapacityError,
    CanonicalGroundEvidenceDeadlineError,
    CanonicalGroundEvidenceError,
    CanonicalGroundEvidenceStore,
)
from .tracking_core.engine.canonical_ground_fusion_bridge import (
    AdaptedCanonicalGroundFact,
    CanonicalGroundBridgeBatch,
    GroundBridgeCapacityError,
    GroundBridgeDeadlineError,
    GroundBridgeIssue,
    GroundBridgeIssueState,
    GroundSemanticsCertificate,
    query_and_adapt_canonical_ground,
)
from .tracking_core.engine.cell_track_lifecycle import ImmutableCellTrack
from .tracking_core.engine.ground_fusion import (
    CellTrackFootprint,
    GroundConfirmationDecision,
    GroundConfirmationState,
    GroundEvidenceRole,
    GroundFusionPolicy,
    GroundMatchOutcome,
    MeasurementSemantics,
    PhysicalSiteRegistry,
    fuse_ground_observations,
)
from .tracking_core.engine.hierarchical_detection import HierarchicalCellDetection
from .tracking_core.engine.physical_site_catalog import PhysicalSiteCatalog
from .tracking_core.geometry.polygon import point_to_polygon_distance_km


GROUND_FUSION_SHADOW_ALGORITHM = "stv3-ground-fusion-shadow-runtime-v1"
GROUND_FUSION_SHADOW_RECEIPT_CONTRACT = "stv3-ground-fusion-shadow-receipt-v1"
GROUND_FUSION_SHADOW_SCHEMA_VERSION = 2
GROUND_FUSION_SHADOW_ADMISSION_STATUS = "shadow_candidate_unadmitted"
GROUND_FUSION_SHADOW_ARCHIVE_CONTRACT = (
    "stv3-ground-fusion-shadow-archive-segment-v1"
)
GROUND_FUSION_SHADOW_ARCHIVE_MANIFEST_CONTRACT = (
    "stv3-ground-fusion-shadow-archive-manifest-v1"
)

HARD_MAX_SOURCE_FACTS = 999  # one extra sentinel remains within bridge hard 1000
HARD_MAX_ACCOUNTING_BYTES = 16 * 1024 * 1024
HARD_MAX_RECEIPT_BYTES = 16 * 1024 * 1024
HARD_MAX_RECEIPTS = 65_536
HARD_MAX_DATABASE_BYTES = 512 * 1024 * 1024
HARD_MAX_SITES = 250_000
HARD_MAX_ALIASES = 500_000
HARD_MAX_RUNTIME_SECONDS = 120.0
MAX_DIAGNOSTIC_BYTES = 8_192
HARD_MAX_ARCHIVE_SEGMENT_BYTES = 64 * 1024 * 1024
HARD_MAX_ARCHIVE_TOTAL_BYTES = 4 * 1024 * 1024 * 1024
HARD_MAX_ARCHIVE_SEGMENTS = 65_536
HARD_MAX_ARCHIVED_RECEIPTS = 4_000_000

GROUND_FUSION_SHADOW_ROTATION_FAULT_POINTS = (
    "rotate.after_snapshot",
    "rotate.after_temp_write",
    "rotate.after_temp_fsync",
    "rotate.after_temp_directory_fsync",
    "rotate.after_publish_replace",
    "rotate.after_publish_directory_fsync",
    "rotate.after_manifest_insert",
    "rotate.after_archive_index",
    "rotate.after_anchor_cas",
    "rotate.before_manifest_commit",
    "rotate.after_manifest_commit",
    "rotate.after_manifest_database_fsync",
    "rotate.before_prune",
    "rotate.after_prune",
    "rotate.before_commit",
    "rotate.after_commit",
    "rotate.after_database_directory_fsync",
)

GROUND_FUSION_SHADOW_OPERATIONAL_BLOCKERS = (
    "ground_shadow_not_live_wired",
    "geographic_footprint_derivation_not_operationally_admitted",
    "provider_semantics_admission_incomplete",
    "archive_restore_and_retention_not_operationally_admitted",
    "operational_snapshot_confirmation_forbidden",
)

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
_ISSUE_RE = re.compile(r"^[a-z][a-z0-9_:.-]{0,191}$")


class GroundFusionShadowError(RuntimeError):
    """Base error for this isolated shadow boundary."""


class GroundFusionShadowDeadlineError(GroundFusionShadowError):
    """The caller-owned absolute monotonic deadline elapsed."""


class GroundFusionShadowCapacityError(GroundFusionShadowError):
    """A configured hard capacity boundary was exceeded."""


class GroundFusionShadowStoreError(GroundFusionShadowError):
    """The durable receipt ledger is corrupt or incompatible."""


class GroundFusionShadowConflict(GroundFusionShadowStoreError):
    """An idempotency key or immutable request identity was reused."""


class GroundFusionShadowWindowExpired(GroundFusionShadowStoreError):
    """The exact receipt exists only in a sealed archive segment."""

    def __init__(self, segment_id: str) -> None:
        self.segment_id = _safe_id(segment_id, "archive segment_id")
        super().__init__(
            "window_expired: exact ground receipt is archived in " + segment_id
        )


class GroundFusionShadowArchiveError(GroundFusionShadowStoreError):
    """A sealed archive, anchor or archive index failed verification."""


class GroundFusionShadowDurabilityError(GroundFusionShadowArchiveError):
    """The platform could not prove the requested filesystem barrier."""


class GroundFactDispositionState(str, Enum):
    USED = "used"
    DUPLICATE = "duplicate"
    OUTSIDE = "outside"
    STALE = "stale"
    WITHHELD = "withheld"
    OVERFLOW = "overflow"
    INDETERMINATE = "indeterminate"


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return value


def _optional_sha(value: object, label: str) -> str | None:
    return None if value is None else _sha(value, label)


def _safe_id(value: object, label: str) -> str:
    if not isinstance(value, str) or _SAFE_ID_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a bounded safe identifier")
    return value


def _issue(value: object) -> str:
    if not isinstance(value, str) or _ISSUE_RE.fullmatch(value) is None:
        raise ValueError("reason code is not bounded machine-safe text")
    return value


def _finite(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be finite")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{label} must be finite")
    return result


def _count(value: object, label: str, maximum: int = 2**63 - 1) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= maximum
    ):
        raise ValueError(f"{label} is outside its bound")
    return value


def _boolean(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{label} must be a JSON boolean")
    return value


def _check_deadline(deadline_at: float, monotonic: Callable[[], float]) -> None:
    now = _finite(monotonic(), "monotonic clock")
    if now >= deadline_at:
        raise GroundFusionShadowDeadlineError("ground fusion shadow deadline elapsed")


def _hash_identifier(namespace: str, value: str) -> str:
    return hashlib.sha256(
        namespace.encode("ascii") + b"\0" + value.encode("utf-8")
    ).hexdigest()


def _footprint_geometry_mapping(footprint: CellTrackFootprint) -> dict[str, Any]:
    return {
        "cell_detection_id": footprint.cell_detection_id,
        "cell_track_id": footprint.cell_track_id,
        "footprint_id": footprint.footprint_id,
        "geometry": [
            [[list(point) for point in ring] for ring in polygon]
            for polygon in footprint.geometry
        ],
        "radar_source": footprint.radar_source,
        "raw_artifact_receipt_id": footprint.raw_artifact_receipt_id,
        "representative_at": footprint.representative_at,
        "valid_from": footprint.valid_from,
        "valid_until": footprint.valid_until,
    }


def footprint_geometry_sha256(footprint: CellTrackFootprint) -> str:
    if not isinstance(footprint, CellTrackFootprint):
        raise TypeError("footprint must be CellTrackFootprint")
    return _digest(_footprint_geometry_mapping(footprint))


def _policy_mapping(policy: GroundFusionPolicy) -> dict[str, Any]:
    return {
        "freshness_seconds_by_provider": [
            [provider.casefold(), float(seconds)]
            for provider, seconds in sorted(
                policy.freshness_seconds_by_provider,
                key=lambda item: item[0].casefold(),
            )
        ],
        "maximum_future_skew_seconds": policy.maximum_future_skew_seconds,
        "maximum_site_coordinate_disagreement_km": (
            policy.maximum_site_coordinate_disagreement_km
        ),
        "minimum_independent_wet_groups": (
            policy.minimum_independent_wet_groups
        ),
        "minimum_interval_overlap_fraction": (
            policy.minimum_interval_overlap_fraction
        ),
        "spatial_tolerance_km": policy.spatial_tolerance_km,
        "temporal_tolerance_seconds": policy.temporal_tolerance_seconds,
    }


@dataclass(frozen=True, slots=True)
class GroundFusionShadowLimits:
    max_source_facts: int = 256
    max_accounting_bytes: int = 2 * 1024 * 1024
    max_receipt_bytes: int = 4 * 1024 * 1024
    max_receipts: int = 4_096
    max_database_bytes: int = 128 * 1024 * 1024
    max_sites: int = 100_000
    max_aliases: int = 200_000
    max_runtime_seconds: float = 20.0
    max_query_lookback_seconds: float = 6 * 60 * 60.0
    max_target_age_seconds: float = 10 * 60.0
    max_target_future_skew_seconds: float = 60.0
    max_cell_age_seconds: float = 30 * 60.0
    max_footprint_window_seconds: float = 30 * 60.0
    maximum_target_to_footprint_km: float = 250.0

    def __post_init__(self) -> None:
        for label, value, minimum, maximum in (
            ("max_source_facts", self.max_source_facts, 1, HARD_MAX_SOURCE_FACTS),
            (
                "max_accounting_bytes",
                self.max_accounting_bytes,
                4_096,
                HARD_MAX_ACCOUNTING_BYTES,
            ),
            (
                "max_receipt_bytes",
                self.max_receipt_bytes,
                4_096,
                HARD_MAX_RECEIPT_BYTES,
            ),
            ("max_receipts", self.max_receipts, 1, HARD_MAX_RECEIPTS),
            (
                "max_database_bytes",
                self.max_database_bytes,
                1024 * 1024,
                HARD_MAX_DATABASE_BYTES,
            ),
            ("max_sites", self.max_sites, 1, HARD_MAX_SITES),
            ("max_aliases", self.max_aliases, 1, HARD_MAX_ALIASES),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise ValueError(f"{label} is outside its hard boundary")
        if self.max_accounting_bytes > self.max_receipt_bytes:
            raise ValueError("accounting byte bound cannot exceed receipt byte bound")
        for label, value, maximum in (
            ("max_runtime_seconds", self.max_runtime_seconds, HARD_MAX_RUNTIME_SECONDS),
            ("max_query_lookback_seconds", self.max_query_lookback_seconds, 7 * 86400.0),
            ("max_target_age_seconds", self.max_target_age_seconds, 86400.0),
            (
                "max_target_future_skew_seconds",
                self.max_target_future_skew_seconds,
                3600.0,
            ),
            ("max_cell_age_seconds", self.max_cell_age_seconds, 86400.0),
            (
                "max_footprint_window_seconds",
                self.max_footprint_window_seconds,
                86400.0,
            ),
            (
                "maximum_target_to_footprint_km",
                self.maximum_target_to_footprint_km,
                20_000.0,
            ),
        ):
            number = _finite(value, label)
            if number <= 0.0 or number > maximum:
                raise ValueError(f"{label} is outside its hard boundary")

    def to_mapping(self) -> dict[str, int | float]:
        return {
            field: getattr(self, field) for field in self.__dataclass_fields__
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.to_mapping())


@dataclass(frozen=True, slots=True)
class GroundFusionShadowRetentionPolicy:
    """Immutable hot-window and sealed-archive capacity contract."""

    archive_namespace: str = "ground-shadow-archive"
    hot_receipts_to_keep: int = 256
    max_segment_receipts: int = 256
    max_segment_bytes: int = 8 * 1024 * 1024
    max_archive_segments: int = 4_096
    max_archived_receipts: int = 1_000_000
    max_total_archive_bytes: int = 1024 * 1024 * 1024

    def __post_init__(self) -> None:
        _safe_id(self.archive_namespace, "archive_namespace")
        for label, value, minimum, maximum in (
            (
                "hot_receipts_to_keep",
                self.hot_receipts_to_keep,
                1,
                HARD_MAX_RECEIPTS,
            ),
            (
                "max_segment_receipts",
                self.max_segment_receipts,
                1,
                HARD_MAX_SOURCE_FACTS + 1,
            ),
            (
                "max_segment_bytes",
                self.max_segment_bytes,
                4_096,
                HARD_MAX_ARCHIVE_SEGMENT_BYTES,
            ),
            (
                "max_archive_segments",
                self.max_archive_segments,
                1,
                HARD_MAX_ARCHIVE_SEGMENTS,
            ),
            (
                "max_archived_receipts",
                self.max_archived_receipts,
                1,
                HARD_MAX_ARCHIVED_RECEIPTS,
            ),
            (
                "max_total_archive_bytes",
                self.max_total_archive_bytes,
                4_096,
                HARD_MAX_ARCHIVE_TOTAL_BYTES,
            ),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise ValueError(f"{label} is outside its archive hard boundary")
        if self.max_segment_bytes > self.max_total_archive_bytes:
            raise ValueError("one archive segment cannot exceed total archive bytes")

    def to_mapping(self) -> dict[str, int | str]:
        return {
            field: getattr(self, field) for field in self.__dataclass_fields__
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.to_mapping())


@dataclass(frozen=True, slots=True)
class GroundFusionShadowRotationResult:
    segment_id: str | None
    archived_receipts: int
    hot_receipts: int
    duplicate_orphan_reused: bool
    archive_anchor_sha256: str

    def __post_init__(self) -> None:
        if self.segment_id is not None:
            _safe_id(self.segment_id, "segment_id")
        _count(self.archived_receipts, "archived_receipts", HARD_MAX_SOURCE_FACTS + 1)
        _count(self.hot_receipts, "hot_receipts", HARD_MAX_RECEIPTS)
        if type(self.duplicate_orphan_reused) is not bool:
            raise TypeError("duplicate_orphan_reused must be bool")
        _sha(self.archive_anchor_sha256, "archive_anchor_sha256")


@dataclass(frozen=True, slots=True)
class GroundTargetSnapshot:
    """One exact, source-bound tracker position; never persisted in plaintext."""

    target_id: str
    target_revision: str
    latitude: float
    longitude: float
    observed_at: float
    source_state_sha256: str

    def __post_init__(self) -> None:
        _safe_id(self.target_id, "target_id")
        _safe_id(self.target_revision, "target_revision")
        latitude = _finite(self.latitude, "target latitude")
        longitude = _finite(self.longitude, "target longitude")
        if not -90.0 <= latitude <= 90.0 or not -180.0 <= longitude <= 180.0:
            raise ValueError("target coordinates are outside WGS84 bounds")
        _finite(self.observed_at, "target observed_at")
        _sha(self.source_state_sha256, "target source_state_sha256")

    @property
    def target_id_sha256(self) -> str:
        return _hash_identifier("stv3-ground-target-id-v1", self.target_id)

    @property
    def fingerprint(self) -> str:
        return _digest({
            "latitude": self.latitude,
            "longitude": self.longitude,
            "observed_at": self.observed_at,
            "source_state_sha256": self.source_state_sha256,
            "target_id": self.target_id,
            "target_revision": self.target_revision,
        })


@dataclass(frozen=True, slots=True)
class GeographicFootprintProof:
    """External proof binding geographic geometry to one science-grid cell."""

    proof_id: str
    proof_version: str
    source_science_receipt_sha256: str
    source_target_grid_sha256: str
    source_transform_receipt_sha256: str
    geometry_builder_id: str
    geometry_certificate_sha256: str
    footprint_geometry_sha256: str

    def __post_init__(self) -> None:
        for label in ("proof_id", "proof_version", "geometry_builder_id"):
            _safe_id(getattr(self, label), label)
        for label in (
            "source_science_receipt_sha256",
            "source_target_grid_sha256",
            "source_transform_receipt_sha256",
            "geometry_certificate_sha256",
            "footprint_geometry_sha256",
        ):
            _sha(getattr(self, label), label)

    @property
    def fingerprint(self) -> str:
        return _digest({
            field: getattr(self, field) for field in self.__dataclass_fields__
        })


@dataclass(frozen=True, slots=True)
class GroundCellTrackBinding:
    """Compact immutable binding to a real CellTrack detection and geometry."""

    footprint: CellTrackFootprint
    engine_id: str
    engine_state_revision: int
    track_revision: int
    analysis_grid_id: str
    science_run_id: str
    science_transition_id: str
    science_receipt_sha256: str
    source_frame_id: str
    target_grid_sha256: str
    radar_semantics_certificate_sha256: str
    transform_certificate_sha256: str
    parent_track_ids: tuple[str, ...]
    child_track_ids: tuple[str, ...]
    lineage_event_ids: tuple[str, ...]
    applied_lineage_hypothesis_ids: tuple[str, ...]
    footprint_proof: GeographicFootprintProof

    def __post_init__(self) -> None:
        if not isinstance(self.footprint, CellTrackFootprint):
            raise TypeError("footprint must be CellTrackFootprint")
        for label in (
            "engine_id",
            "analysis_grid_id",
            "science_run_id",
            "science_transition_id",
            "source_frame_id",
        ):
            _safe_id(getattr(self, label), label)
        _count(self.engine_state_revision, "engine_state_revision", 2**31 - 1)
        _count(self.track_revision, "track_revision", 2**31 - 1)
        if self.engine_state_revision < 1 or self.track_revision < 1:
            raise ValueError("track binding revisions must be positive")
        if self.footprint.raw_artifact_receipt_id is None:
            raise ValueError("track footprint requires exact radar artifact provenance")
        for label in (
            "science_receipt_sha256",
            "target_grid_sha256",
            "radar_semantics_certificate_sha256",
            "transform_certificate_sha256",
        ):
            _sha(getattr(self, label), label)
        for label in (
            "parent_track_ids",
            "child_track_ids",
            "lineage_event_ids",
            "applied_lineage_hypothesis_ids",
        ):
            values = getattr(self, label)
            if (
                not isinstance(values, tuple)
                or values != tuple(sorted(set(values)))
            ):
                raise ValueError(f"{label} must be a canonical unique tuple")
            for item in values:
                _safe_id(item, label)
        if not isinstance(self.footprint_proof, GeographicFootprintProof):
            raise TypeError("footprint_proof must be GeographicFootprintProof")
        proof = self.footprint_proof
        if (
            proof.source_science_receipt_sha256 != self.science_receipt_sha256
            or proof.source_target_grid_sha256 != self.target_grid_sha256
            or proof.footprint_geometry_sha256
            != footprint_geometry_sha256(self.footprint)
        ):
            raise ValueError("footprint proof differs from the exact track binding")

    @property
    def fingerprint(self) -> str:
        return _digest({
            "analysis_grid_id": self.analysis_grid_id,
            "applied_lineage_hypothesis_ids": list(
                self.applied_lineage_hypothesis_ids
            ),
            "child_track_ids": list(self.child_track_ids),
            "engine_id": self.engine_id,
            "engine_state_revision": self.engine_state_revision,
            "footprint_geometry_sha256": footprint_geometry_sha256(self.footprint),
            "footprint_proof_sha256": self.footprint_proof.fingerprint,
            "lineage_event_ids": list(self.lineage_event_ids),
            "parent_track_ids": list(self.parent_track_ids),
            "radar_semantics_certificate_sha256": (
                self.radar_semantics_certificate_sha256
            ),
            "science_receipt_sha256": self.science_receipt_sha256,
            "science_run_id": self.science_run_id,
            "science_transition_id": self.science_transition_id,
            "source_frame_id": self.source_frame_id,
            "target_grid_sha256": self.target_grid_sha256,
            "track_revision": self.track_revision,
            "transform_certificate_sha256": self.transform_certificate_sha256,
        })


def bind_cell_track_footprint(
    track: ImmutableCellTrack,
    hierarchical_detection: HierarchicalCellDetection,
    footprint: CellTrackFootprint,
    *,
    engine_id: str,
    engine_state_revision: int,
    science_run_id: str,
    science_transition_id: str,
    science_receipt_sha256: str,
    source_frame_id: str,
    target_grid_sha256: str,
    radar_semantics_certificate_sha256: str,
    transform_certificate_sha256: str,
    applied_lineage_hypothesis_ids: tuple[str, ...],
    footprint_proof: GeographicFootprintProof,
) -> GroundCellTrackBinding:
    """Bind an actual immutable CellTrack latest detection to WGS84 geometry."""

    if not isinstance(track, ImmutableCellTrack):
        raise TypeError("track must be ImmutableCellTrack")
    if not isinstance(hierarchical_detection, HierarchicalCellDetection):
        raise TypeError("hierarchical_detection has the wrong type")
    if hierarchical_detection.detection != track.latest_detection:
        raise ValueError("hierarchical detection is not the track's exact latest cell")
    if (
        footprint.cell_track_id != track.track_id
        or footprint.cell_detection_id != track.latest_detection.detection_id
        or footprint.representative_at != track.latest_detection.valid_at
        or track.analysis_grid_id != hierarchical_detection.detection.analysis_grid_id
    ):
        raise ValueError("footprint does not bind the exact CellTrack/time identity")
    if footprint.raw_artifact_receipt_id is None:
        raise ValueError("ground footprint requires an exact radar artifact receipt")
    return GroundCellTrackBinding(
        footprint=footprint,
        engine_id=engine_id,
        engine_state_revision=engine_state_revision,
        track_revision=track.revision,
        analysis_grid_id=track.analysis_grid_id,
        science_run_id=science_run_id,
        science_transition_id=science_transition_id,
        science_receipt_sha256=science_receipt_sha256,
        source_frame_id=source_frame_id,
        target_grid_sha256=target_grid_sha256,
        radar_semantics_certificate_sha256=radar_semantics_certificate_sha256,
        transform_certificate_sha256=transform_certificate_sha256,
        parent_track_ids=track.parent_track_ids,
        child_track_ids=track.child_track_ids,
        lineage_event_ids=track.lineage_event_ids,
        applied_lineage_hypothesis_ids=applied_lineage_hypothesis_ids,
        footprint_proof=footprint_proof,
    )


def bind_science_shadow_footprint(
    result: object,
    footprint: CellTrackFootprint,
    *,
    footprint_proof: GeographicFootprintProof,
) -> GroundCellTrackBinding:
    """Bind only a fresh advanced science result that still carries arrays.

    Duplicate/restart receipts deliberately contain no recomputed raster or
    transition arrays and therefore cannot be used to fabricate geometry.
    The import is local so importing this ground module never activates or
    registers the science orchestrator.
    """

    from .science_shadow_orchestrator import (  # local shadow-only boundary
        ScienceShadowOutcome,
        ScienceShadowProvenance,
        ScienceShadowRunResult,
    )

    if not isinstance(result, ScienceShadowRunResult):
        raise TypeError("result must be ScienceShadowRunResult")
    if (
        result.duplicate
        or result.receipt.outcome is not ScienceShadowOutcome.ADVANCED
        or result.transition is None
        or result.evidence is None
    ):
        raise ValueError(
            "only a fresh advanced science result can bind geographic geometry"
        )
    receipt = result.receipt
    transition = result.transition
    evidence = result.evidence
    provenance = receipt.provenance
    if provenance is None or provenance != ScienceShadowProvenance.from_evidence(
        evidence
    ):
        raise ValueError("science result provenance differs from its receipt")
    if (
        transition.status != "advanced"
        or transition.transition_id != receipt.transition_id
        or transition.next_state.revision != receipt.next_state_revision
        or transition.detection_frame != evidence.detection_frame
        or transition.applied_lineage_hypothesis_ids
        != receipt.applied_lineage_hypothesis_ids
    ):
        raise ValueError("science transition differs from its durable receipt")
    try:
        track = transition.next_state.track(footprint.cell_track_id)
        hierarchical = transition.next_state.hierarchy(footprint.cell_track_id)
    except KeyError as exc:
        raise ValueError("footprint references no exact science CellTrack") from exc
    if track.latest_detection.valid_at != evidence.detection_frame.valid_at:
        raise ValueError("coasting/stale CellTrack geometry cannot be newly bound")
    if (
        footprint_proof.source_science_receipt_sha256
        != receipt.receipt_sha256
        or footprint_proof.source_target_grid_sha256
        != provenance.target_grid_sha256
        or footprint_proof.source_transform_receipt_sha256
        != provenance.transform_receipt_sha256
        or footprint.raw_artifact_receipt_id
        != provenance.capture_receipt_sha256
    ):
        raise ValueError("geographic proof differs from science provenance")
    return bind_cell_track_footprint(
        track,
        hierarchical,
        footprint,
        engine_id=receipt.engine_id,
        engine_state_revision=transition.next_state.revision,
        science_run_id=receipt.canonical_run_id,
        science_transition_id=transition.transition_id,
        science_receipt_sha256=receipt.receipt_sha256,
        source_frame_id=evidence.normalized_frame.frame_id,
        target_grid_sha256=provenance.target_grid_sha256,
        radar_semantics_certificate_sha256=(
            provenance.radar_semantics_certificate_sha256
        ),
        transform_certificate_sha256=provenance.transform_certificate_sha256,
        applied_lineage_hypothesis_ids=(
            receipt.applied_lineage_hypothesis_ids
        ),
        footprint_proof=footprint_proof,
    )


@dataclass(frozen=True, slots=True)
class GroundFusionShadowRequest:
    attempt_id: str
    decision_key: str
    region_key: str
    target: GroundTargetSnapshot
    cell: GroundCellTrackBinding
    evaluated_at: float

    def __post_init__(self) -> None:
        _safe_id(self.attempt_id, "attempt_id")
        _safe_id(self.decision_key, "decision_key")
        _safe_id(self.region_key, "region_key")
        if not isinstance(self.target, GroundTargetSnapshot):
            raise TypeError("target must be GroundTargetSnapshot")
        if not isinstance(self.cell, GroundCellTrackBinding):
            raise TypeError("cell must be GroundCellTrackBinding")
        _finite(self.evaluated_at, "evaluated_at")

    @property
    def decision_key_sha256(self) -> str:
        return _hash_identifier("stv3-ground-decision-key-v1", self.decision_key)

    @property
    def region_key_sha256(self) -> str:
        return _hash_identifier("stv3-ground-region-key-v1", self.region_key)


@dataclass(frozen=True, slots=True)
class GroundFactDisposition:
    """One privacy-safe terminal accounting row for one queried fact."""

    source_index: int
    fact_id: str
    fact_sha256: str
    receipt_sha256: str
    provider: str
    station_identity_sha256: str
    state: GroundFactDispositionState
    reason_codes: tuple[str, ...]
    observation_id: str | None = None
    physical_site_id: str | None = None
    independence_group: str | None = None
    certificate_fingerprint: str | None = None
    mapping_fingerprint: str | None = None
    dry_capability_fingerprint: str | None = None
    measurement_semantics: str | None = None
    measurement_amount: float | None = None
    measurement_unit: str | None = None
    supports_precipitation: bool | None = None
    temporal_overlap_fraction: float | None = None
    distance_to_footprint_km: float | None = None

    def __post_init__(self) -> None:
        _count(self.source_index, "source_index", HARD_MAX_SOURCE_FACTS)
        _safe_id(self.fact_id, "fact_id")
        _sha(self.fact_sha256, "fact_sha256")
        _sha(self.receipt_sha256, "receipt_sha256")
        _safe_id(self.provider, "provider")
        _sha(self.station_identity_sha256, "station_identity_sha256")
        if not isinstance(self.state, GroundFactDispositionState):
            raise TypeError("disposition state has the wrong type")
        if (
            not isinstance(self.reason_codes, tuple)
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
            or not self.reason_codes
        ):
            raise ValueError("disposition reason codes must be canonical and non-empty")
        for item in self.reason_codes:
            _issue(item)
        for label in (
            "observation_id",
            "physical_site_id",
            "independence_group",
        ):
            value = getattr(self, label)
            if value is not None:
                _safe_id(value, label)
        _optional_sha(self.certificate_fingerprint, "certificate_fingerprint")
        _optional_sha(self.mapping_fingerprint, "mapping_fingerprint")
        _optional_sha(
            self.dry_capability_fingerprint,
            "dry_capability_fingerprint",
        )
        if self.measurement_semantics is not None:
            if self.measurement_semantics not in {
                item.value for item in MeasurementSemantics
            }:
                raise ValueError("measurement semantics are unknown")
        if self.measurement_amount is not None and _finite(
            self.measurement_amount,
            "measurement_amount",
        ) < 0.0:
            raise ValueError("measurement_amount cannot be negative")
        if self.measurement_unit is not None:
            _safe_id(self.measurement_unit, "measurement_unit")
        if (self.measurement_amount is None) != (self.measurement_unit is None):
            raise ValueError("measurement amount and unit must be both present or absent")
        if self.supports_precipitation is not None and type(
            self.supports_precipitation
        ) is not bool:
            raise TypeError("supports_precipitation must be bool or None")
        if self.temporal_overlap_fraction is not None:
            overlap = _finite(
                self.temporal_overlap_fraction,
                "temporal_overlap_fraction",
            )
            if not 0.0 <= overlap <= 1.0:
                raise ValueError("temporal overlap fraction is outside [0,1]")
        if self.distance_to_footprint_km is not None and _finite(
            self.distance_to_footprint_km,
            "distance_to_footprint_km",
        ) < 0.0:
            raise ValueError("distance_to_footprint_km cannot be negative")
        if self.state is GroundFactDispositionState.USED and (
            self.observation_id is None
            or self.physical_site_id is None
            or self.independence_group is None
            or self.certificate_fingerprint is None
            or self.mapping_fingerprint is None
            or self.measurement_semantics is None
            or self.supports_precipitation is None
        ):
            raise ValueError("used evidence must retain complete scientific provenance")
        if (
            self.state is GroundFactDispositionState.USED
            and self.supports_precipitation is False
            and self.dry_capability_fingerprint is None
        ):
            raise ValueError("used dry evidence requires an exact absence capability")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "certificate_fingerprint": self.certificate_fingerprint,
            "distance_to_footprint_km": self.distance_to_footprint_km,
            "dry_capability_fingerprint": self.dry_capability_fingerprint,
            "fact_id": self.fact_id,
            "fact_sha256": self.fact_sha256,
            "independence_group": self.independence_group,
            "mapping_fingerprint": self.mapping_fingerprint,
            "measurement_semantics": self.measurement_semantics,
            "measurement_amount": self.measurement_amount,
            "measurement_unit": self.measurement_unit,
            "observation_id": self.observation_id,
            "physical_site_id": self.physical_site_id,
            "provider": self.provider,
            "reason_codes": list(self.reason_codes),
            "receipt_sha256": self.receipt_sha256,
            "source_index": self.source_index,
            "state": self.state.value,
            "station_identity_sha256": self.station_identity_sha256,
            "supports_precipitation": self.supports_precipitation,
            "temporal_overlap_fraction": self.temporal_overlap_fraction,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "GroundFactDisposition":
        return cls(
            source_index=int(value["source_index"]),
            fact_id=str(value["fact_id"]),
            fact_sha256=str(value["fact_sha256"]),
            receipt_sha256=str(value["receipt_sha256"]),
            provider=str(value["provider"]),
            station_identity_sha256=str(value["station_identity_sha256"]),
            state=GroundFactDispositionState(str(value["state"])),
            reason_codes=tuple(str(item) for item in value["reason_codes"]),
            observation_id=(
                None if value.get("observation_id") is None
                else str(value["observation_id"])
            ),
            physical_site_id=(
                None if value.get("physical_site_id") is None
                else str(value["physical_site_id"])
            ),
            independence_group=(
                None if value.get("independence_group") is None
                else str(value["independence_group"])
            ),
            certificate_fingerprint=(
                None if value.get("certificate_fingerprint") is None
                else str(value["certificate_fingerprint"])
            ),
            mapping_fingerprint=(
                None if value.get("mapping_fingerprint") is None
                else str(value["mapping_fingerprint"])
            ),
            dry_capability_fingerprint=(
                None if value.get("dry_capability_fingerprint") is None
                else str(value["dry_capability_fingerprint"])
            ),
            measurement_semantics=(
                None if value.get("measurement_semantics") is None
                else str(value["measurement_semantics"])
            ),
            measurement_amount=(
                None if value.get("measurement_amount") is None
                else float(value["measurement_amount"])
            ),
            measurement_unit=(
                None if value.get("measurement_unit") is None
                else str(value["measurement_unit"])
            ),
            supports_precipitation=value.get("supports_precipitation"),
            temporal_overlap_fraction=(
                None if value.get("temporal_overlap_fraction") is None
                else float(value["temporal_overlap_fraction"])
            ),
            distance_to_footprint_km=(
                None if value.get("distance_to_footprint_km") is None
                else float(value["distance_to_footprint_km"])
            ),
        )


@dataclass(frozen=True, slots=True)
class GroundFusionShadowAccounting:
    query_executed: bool
    full_scan_complete: bool
    overflowed: bool
    source_fact_count: int
    unscanned_fact_count_lower_bound: int
    encoded_bytes: int
    dispositions: tuple[GroundFactDisposition, ...]

    def __post_init__(self) -> None:
        if any(type(value) is not bool for value in (
            self.query_executed,
            self.full_scan_complete,
            self.overflowed,
        )):
            raise TypeError("accounting flags must be booleans")
        _count(self.source_fact_count, "source_fact_count", HARD_MAX_SOURCE_FACTS + 1)
        _count(
            self.unscanned_fact_count_lower_bound,
            "unscanned_fact_count_lower_bound",
        )
        _count(self.encoded_bytes, "encoded_bytes", HARD_MAX_ACCOUNTING_BYTES)
        if not isinstance(self.dispositions, tuple) or any(
            not isinstance(item, GroundFactDisposition)
            for item in self.dispositions
        ):
            raise TypeError("accounting dispositions have the wrong type")
        indexes = tuple(item.source_index for item in self.dispositions)
        if self.query_executed:
            if (
                indexes != tuple(range(self.source_fact_count))
                or len(indexes) != self.source_fact_count
            ):
                raise ValueError("every queried fact needs exactly one disposition")
        elif self.source_fact_count or self.dispositions:
            raise ValueError("a skipped query cannot contain source facts")
        if self.full_scan_complete and (
            not self.query_executed
            or self.overflowed
            or self.unscanned_fact_count_lower_bound
        ):
            raise ValueError("full-scan completeness contradicts capacity accounting")
        if self.overflowed and not self.query_executed:
            raise ValueError("overflow requires an executed source query")
        if self.overflowed and any(
            item.state is not GroundFactDispositionState.OVERFLOW
            for item in self.dispositions
        ):
            raise ValueError("overflow must account every returned fact as overflow")

    @property
    def counts(self) -> dict[str, int]:
        return {
            state.value: sum(item.state is state for item in self.dispositions)
            for state in GroundFactDispositionState
        }

    def to_mapping(self) -> dict[str, Any]:
        return {
            "counts": self.counts,
            "dispositions": [item.to_mapping() for item in self.dispositions],
            "encoded_bytes": self.encoded_bytes,
            "full_scan_complete": self.full_scan_complete,
            "overflowed": self.overflowed,
            "query_executed": self.query_executed,
            "source_fact_count": self.source_fact_count,
            "unscanned_fact_count_lower_bound": (
                self.unscanned_fact_count_lower_bound
            ),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "GroundFusionShadowAccounting":
        result = cls(
            query_executed=_boolean(value["query_executed"], "query_executed"),
            full_scan_complete=_boolean(
                value["full_scan_complete"],
                "full_scan_complete",
            ),
            overflowed=_boolean(value["overflowed"], "overflowed"),
            source_fact_count=int(value["source_fact_count"]),
            unscanned_fact_count_lower_bound=int(
                value["unscanned_fact_count_lower_bound"]
            ),
            encoded_bytes=int(value["encoded_bytes"]),
            dispositions=tuple(
                GroundFactDisposition.from_mapping(item)
                for item in value["dispositions"]
            ),
        )
        if dict(value.get("counts", {})) != result.counts:
            raise GroundFusionShadowStoreError("accounting count checksum differs")
        return result


@dataclass(frozen=True, slots=True)
class GroundFusionShadowReceipt:
    receipt_sha256: str
    attempt_id: str
    request_sha256: str
    runtime_profile_sha256: str
    decision_key_sha256: str
    region_key_sha256: str
    target_id_sha256: str
    target_snapshot_sha256: str
    cell_binding_sha256: str
    engine_id: str
    engine_state_revision: int
    track_revision: int
    analysis_grid_id: str
    cell_track_id: str
    cell_detection_id: str
    footprint_id: str
    footprint_geometry_sha256: str
    footprint_proof_sha256: str
    footprint_proof_id: str
    footprint_proof_version: str
    geometry_builder_id: str
    geometry_certificate_sha256: str
    source_transform_receipt_sha256: str
    science_run_id: str
    science_transition_id: str
    source_frame_id: str
    science_receipt_sha256: str
    target_grid_sha256: str
    radar_semantics_certificate_sha256: str
    transform_certificate_sha256: str
    parent_track_ids: tuple[str, ...]
    child_track_ids: tuple[str, ...]
    lineage_event_ids: tuple[str, ...]
    applied_lineage_hypothesis_ids: tuple[str, ...]
    ground_semantics_certificate_fingerprints: tuple[str, ...]
    registry_version: str
    registry_fingerprint: str
    catalog_fingerprint: str
    policy_fingerprint: str
    evaluated_at: float
    registry_effective_at: float
    query_observed_from: float | None
    query_observed_through: float | None
    target_distance_to_footprint_km: float | None
    bridge_fingerprint: str | None
    candidate_state: GroundConfirmationState
    candidate_confirmed: bool
    reason_codes: tuple[str, ...]
    independent_wet_groups: tuple[str, ...]
    independent_dry_groups: tuple[str, ...]
    conflicted_groups: tuple[str, ...]
    accounting: GroundFusionShadowAccounting
    admission_status: str = GROUND_FUSION_SHADOW_ADMISSION_STATUS
    meteorological_admission_granted: bool = False
    operational_confirmation_claim: bool = False
    operational_snapshot_claim_generated: bool = False
    operational_admission_blockers: tuple[str, ...] = (
        GROUND_FUSION_SHADOW_OPERATIONAL_BLOCKERS
    )
    algorithm_version: str = GROUND_FUSION_SHADOW_ALGORITHM

    def __post_init__(self) -> None:
        _sha(self.receipt_sha256, "receipt_sha256")
        _safe_id(self.attempt_id, "attempt_id")
        for label in (
            "request_sha256",
            "runtime_profile_sha256",
            "decision_key_sha256",
            "region_key_sha256",
            "target_id_sha256",
            "target_snapshot_sha256",
            "cell_binding_sha256",
            "footprint_geometry_sha256",
            "footprint_proof_sha256",
            "geometry_certificate_sha256",
            "source_transform_receipt_sha256",
            "science_receipt_sha256",
            "target_grid_sha256",
            "radar_semantics_certificate_sha256",
            "transform_certificate_sha256",
            "registry_fingerprint",
            "catalog_fingerprint",
            "policy_fingerprint",
        ):
            _sha(getattr(self, label), label)
        for label in (
            "engine_id",
            "analysis_grid_id",
            "cell_track_id",
            "cell_detection_id",
            "footprint_id",
            "science_run_id",
            "science_transition_id",
            "source_frame_id",
            "registry_version",
            "footprint_proof_id",
            "footprint_proof_version",
            "geometry_builder_id",
        ):
            _safe_id(getattr(self, label), label)
        _count(self.engine_state_revision, "engine_state_revision", 2**31 - 1)
        _count(self.track_revision, "track_revision", 2**31 - 1)
        if self.engine_state_revision < 1 or self.track_revision < 1:
            raise ValueError("receipt track revisions must be positive")
        for label in (
            "parent_track_ids",
            "child_track_ids",
            "lineage_event_ids",
            "applied_lineage_hypothesis_ids",
        ):
            values = getattr(self, label)
            if not isinstance(values, tuple) or values != tuple(sorted(set(values))):
                raise ValueError(f"receipt {label} must be canonical")
            for item in values:
                _safe_id(item, label)
        if (
            not isinstance(self.ground_semantics_certificate_fingerprints, tuple)
            or self.ground_semantics_certificate_fingerprints
            != tuple(sorted(set(self.ground_semantics_certificate_fingerprints)))
        ):
            raise ValueError("ground certificate fingerprints must be canonical")
        for item in self.ground_semantics_certificate_fingerprints:
            _sha(item, "ground semantics certificate fingerprint")
        expected_proof = _digest({
            "footprint_geometry_sha256": self.footprint_geometry_sha256,
            "geometry_builder_id": self.geometry_builder_id,
            "geometry_certificate_sha256": self.geometry_certificate_sha256,
            "proof_id": self.footprint_proof_id,
            "proof_version": self.footprint_proof_version,
            "source_science_receipt_sha256": self.science_receipt_sha256,
            "source_target_grid_sha256": self.target_grid_sha256,
            "source_transform_receipt_sha256": (
                self.source_transform_receipt_sha256
            ),
        })
        if expected_proof != self.footprint_proof_sha256:
            raise ValueError("receipt footprint proof fingerprint differs")
        expected_binding = _digest({
            "analysis_grid_id": self.analysis_grid_id,
            "applied_lineage_hypothesis_ids": list(
                self.applied_lineage_hypothesis_ids
            ),
            "child_track_ids": list(self.child_track_ids),
            "engine_id": self.engine_id,
            "engine_state_revision": self.engine_state_revision,
            "footprint_geometry_sha256": self.footprint_geometry_sha256,
            "footprint_proof_sha256": self.footprint_proof_sha256,
            "lineage_event_ids": list(self.lineage_event_ids),
            "parent_track_ids": list(self.parent_track_ids),
            "radar_semantics_certificate_sha256": (
                self.radar_semantics_certificate_sha256
            ),
            "science_receipt_sha256": self.science_receipt_sha256,
            "science_run_id": self.science_run_id,
            "science_transition_id": self.science_transition_id,
            "source_frame_id": self.source_frame_id,
            "target_grid_sha256": self.target_grid_sha256,
            "track_revision": self.track_revision,
            "transform_certificate_sha256": self.transform_certificate_sha256,
        })
        if expected_binding != self.cell_binding_sha256:
            raise ValueError("receipt CellTrack binding fingerprint differs")
        _finite(self.evaluated_at, "evaluated_at")
        _finite(self.registry_effective_at, "registry_effective_at")
        if (self.query_observed_from is None) != (
            self.query_observed_through is None
        ):
            raise ValueError("query interval must be complete or absent")
        if self.query_observed_from is not None:
            start = _finite(self.query_observed_from, "query_observed_from")
            end = _finite(self.query_observed_through, "query_observed_through")
            if start > end:
                raise ValueError("query interval is inverted")
        if self.target_distance_to_footprint_km is not None and _finite(
            self.target_distance_to_footprint_km,
            "target_distance_to_footprint_km",
        ) < 0.0:
            raise ValueError("target distance cannot be negative")
        _optional_sha(self.bridge_fingerprint, "bridge_fingerprint")
        if not isinstance(self.candidate_state, GroundConfirmationState):
            raise TypeError("candidate state has the wrong type")
        if type(self.candidate_confirmed) is not bool or (
            self.candidate_confirmed
            != (self.candidate_state is GroundConfirmationState.CONFIRMED)
        ):
            raise ValueError("candidate confirmation differs from candidate state")
        if (
            not isinstance(self.reason_codes, tuple)
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
            or not self.reason_codes
        ):
            raise ValueError("receipt reason codes must be canonical and non-empty")
        for item in self.reason_codes:
            _issue(item)
        for label in (
            "independent_wet_groups",
            "independent_dry_groups",
            "conflicted_groups",
        ):
            values = getattr(self, label)
            if not isinstance(values, tuple) or values != tuple(sorted(set(values))):
                raise ValueError(f"{label} must be canonical")
            for item in values:
                _safe_id(item, label)
        if not isinstance(self.accounting, GroundFusionShadowAccounting):
            raise TypeError("receipt accounting has the wrong type")
        if self.accounting.query_executed != (self.query_observed_from is not None):
            raise ValueError("receipt query interval differs from accounting")
        if (
            self.admission_status != GROUND_FUSION_SHADOW_ADMISSION_STATUS
            or self.meteorological_admission_granted
            or self.operational_confirmation_claim
            or self.operational_snapshot_claim_generated
            or self.operational_admission_blockers
            != GROUND_FUSION_SHADOW_OPERATIONAL_BLOCKERS
            or self.algorithm_version != GROUND_FUSION_SHADOW_ALGORITHM
        ):
            raise ValueError("shadow receipt contains a forbidden operational claim")
        if (
            not self.accounting.full_scan_complete
            and self.candidate_state is not GroundConfirmationState.INDETERMINATE
        ):
            raise ValueError("partial source evidence cannot yield a determinate state")
        if self.receipt_sha256 != _digest(self.payload_mapping()):
            raise ValueError("ground shadow receipt checksum differs")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "accounting": self.accounting.to_mapping(),
            "admission_status": self.admission_status,
            "algorithm_version": self.algorithm_version,
            "analysis_grid_id": self.analysis_grid_id,
            "attempt_id": self.attempt_id,
            "applied_lineage_hypothesis_ids": list(
                self.applied_lineage_hypothesis_ids
            ),
            "bridge_fingerprint": self.bridge_fingerprint,
            "candidate_confirmed": self.candidate_confirmed,
            "candidate_state": self.candidate_state.value,
            "catalog_fingerprint": self.catalog_fingerprint,
            "cell_binding_sha256": self.cell_binding_sha256,
            "cell_detection_id": self.cell_detection_id,
            "cell_track_id": self.cell_track_id,
            "child_track_ids": list(self.child_track_ids),
            "conflicted_groups": list(self.conflicted_groups),
            "contract": GROUND_FUSION_SHADOW_RECEIPT_CONTRACT,
            "decision_key_sha256": self.decision_key_sha256,
            "evaluated_at": self.evaluated_at,
            "engine_id": self.engine_id,
            "engine_state_revision": self.engine_state_revision,
            "footprint_id": self.footprint_id,
            "footprint_geometry_sha256": self.footprint_geometry_sha256,
            "footprint_proof_id": self.footprint_proof_id,
            "footprint_proof_sha256": self.footprint_proof_sha256,
            "footprint_proof_version": self.footprint_proof_version,
            "geometry_builder_id": self.geometry_builder_id,
            "geometry_certificate_sha256": self.geometry_certificate_sha256,
            "ground_semantics_certificate_fingerprints": list(
                self.ground_semantics_certificate_fingerprints
            ),
            "independent_dry_groups": list(self.independent_dry_groups),
            "independent_wet_groups": list(self.independent_wet_groups),
            "lineage_event_ids": list(self.lineage_event_ids),
            "meteorological_admission_granted": (
                self.meteorological_admission_granted
            ),
            "operational_admission_blockers": list(
                self.operational_admission_blockers
            ),
            "operational_confirmation_claim": self.operational_confirmation_claim,
            "operational_snapshot_claim_generated": (
                self.operational_snapshot_claim_generated
            ),
            "policy_fingerprint": self.policy_fingerprint,
            "parent_track_ids": list(self.parent_track_ids),
            "query_observed_from": self.query_observed_from,
            "query_observed_through": self.query_observed_through,
            "radar_semantics_certificate_sha256": (
                self.radar_semantics_certificate_sha256
            ),
            "reason_codes": list(self.reason_codes),
            "region_key_sha256": self.region_key_sha256,
            "registry_effective_at": self.registry_effective_at,
            "registry_fingerprint": self.registry_fingerprint,
            "registry_version": self.registry_version,
            "request_sha256": self.request_sha256,
            "runtime_profile_sha256": self.runtime_profile_sha256,
            "science_run_id": self.science_run_id,
            "science_receipt_sha256": self.science_receipt_sha256,
            "science_transition_id": self.science_transition_id,
            "source_frame_id": self.source_frame_id,
            "source_transform_receipt_sha256": (
                self.source_transform_receipt_sha256
            ),
            "target_distance_to_footprint_km": (
                self.target_distance_to_footprint_km
            ),
            "target_grid_sha256": self.target_grid_sha256,
            "target_id_sha256": self.target_id_sha256,
            "target_snapshot_sha256": self.target_snapshot_sha256,
            "transform_certificate_sha256": self.transform_certificate_sha256,
            "track_revision": self.track_revision,
        }

    def to_mapping(self) -> dict[str, Any]:
        result = self.payload_mapping()
        result["receipt_sha256"] = self.receipt_sha256
        return result

    @property
    def operational_admission_eligible(self) -> bool:
        """Hard denial retained for repository-wide isolation audits."""

        return False

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "GroundFusionShadowReceipt":
        if value.get("contract") != GROUND_FUSION_SHADOW_RECEIPT_CONTRACT:
            raise GroundFusionShadowStoreError("ground receipt contract drift")
        return cls(
            receipt_sha256=str(value["receipt_sha256"]),
            attempt_id=str(value["attempt_id"]),
            request_sha256=str(value["request_sha256"]),
            runtime_profile_sha256=str(value["runtime_profile_sha256"]),
            decision_key_sha256=str(value["decision_key_sha256"]),
            region_key_sha256=str(value["region_key_sha256"]),
            target_id_sha256=str(value["target_id_sha256"]),
            target_snapshot_sha256=str(value["target_snapshot_sha256"]),
            cell_binding_sha256=str(value["cell_binding_sha256"]),
            engine_id=str(value["engine_id"]),
            engine_state_revision=int(value["engine_state_revision"]),
            track_revision=int(value["track_revision"]),
            analysis_grid_id=str(value["analysis_grid_id"]),
            cell_track_id=str(value["cell_track_id"]),
            cell_detection_id=str(value["cell_detection_id"]),
            footprint_id=str(value["footprint_id"]),
            footprint_geometry_sha256=str(value["footprint_geometry_sha256"]),
            footprint_proof_sha256=str(value["footprint_proof_sha256"]),
            footprint_proof_id=str(value["footprint_proof_id"]),
            footprint_proof_version=str(value["footprint_proof_version"]),
            geometry_builder_id=str(value["geometry_builder_id"]),
            geometry_certificate_sha256=str(value["geometry_certificate_sha256"]),
            source_transform_receipt_sha256=str(
                value["source_transform_receipt_sha256"]
            ),
            science_run_id=str(value["science_run_id"]),
            science_transition_id=str(value["science_transition_id"]),
            source_frame_id=str(value["source_frame_id"]),
            science_receipt_sha256=str(value["science_receipt_sha256"]),
            target_grid_sha256=str(value["target_grid_sha256"]),
            radar_semantics_certificate_sha256=str(
                value["radar_semantics_certificate_sha256"]
            ),
            transform_certificate_sha256=str(
                value["transform_certificate_sha256"]
            ),
            parent_track_ids=tuple(str(item) for item in value["parent_track_ids"]),
            child_track_ids=tuple(str(item) for item in value["child_track_ids"]),
            lineage_event_ids=tuple(
                str(item) for item in value["lineage_event_ids"]
            ),
            applied_lineage_hypothesis_ids=tuple(
                str(item) for item in value["applied_lineage_hypothesis_ids"]
            ),
            ground_semantics_certificate_fingerprints=tuple(
                str(item)
                for item in value["ground_semantics_certificate_fingerprints"]
            ),
            registry_version=str(value["registry_version"]),
            registry_fingerprint=str(value["registry_fingerprint"]),
            catalog_fingerprint=str(value["catalog_fingerprint"]),
            policy_fingerprint=str(value["policy_fingerprint"]),
            evaluated_at=float(value["evaluated_at"]),
            registry_effective_at=float(value["registry_effective_at"]),
            query_observed_from=(
                None if value.get("query_observed_from") is None
                else float(value["query_observed_from"])
            ),
            query_observed_through=(
                None if value.get("query_observed_through") is None
                else float(value["query_observed_through"])
            ),
            target_distance_to_footprint_km=(
                None if value.get("target_distance_to_footprint_km") is None
                else float(value["target_distance_to_footprint_km"])
            ),
            bridge_fingerprint=(
                None if value.get("bridge_fingerprint") is None
                else str(value["bridge_fingerprint"])
            ),
            candidate_state=GroundConfirmationState(str(value["candidate_state"])),
            candidate_confirmed=_boolean(
                value["candidate_confirmed"],
                "candidate_confirmed",
            ),
            reason_codes=tuple(str(item) for item in value["reason_codes"]),
            independent_wet_groups=tuple(
                str(item) for item in value["independent_wet_groups"]
            ),
            independent_dry_groups=tuple(
                str(item) for item in value["independent_dry_groups"]
            ),
            conflicted_groups=tuple(str(item) for item in value["conflicted_groups"]),
            accounting=GroundFusionShadowAccounting.from_mapping(value["accounting"]),
            admission_status=str(value["admission_status"]),
            meteorological_admission_granted=_boolean(
                value["meteorological_admission_granted"],
                "meteorological_admission_granted",
            ),
            operational_confirmation_claim=_boolean(
                value["operational_confirmation_claim"],
                "operational_confirmation_claim",
            ),
            operational_snapshot_claim_generated=_boolean(
                value["operational_snapshot_claim_generated"],
                "operational_snapshot_claim_generated",
            ),
            operational_admission_blockers=tuple(
                str(item) for item in value["operational_admission_blockers"]
            ),
            algorithm_version=str(value["algorithm_version"]),
        )


def _build_receipt(**values: Any) -> GroundFusionShadowReceipt:
    provisional = object.__new__(GroundFusionShadowReceipt)
    object.__setattr__(provisional, "receipt_sha256", "0" * 64)
    defaults = {
        "admission_status": GROUND_FUSION_SHADOW_ADMISSION_STATUS,
        "meteorological_admission_granted": False,
        "operational_confirmation_claim": False,
        "operational_snapshot_claim_generated": False,
        "operational_admission_blockers": (
            GROUND_FUSION_SHADOW_OPERATIONAL_BLOCKERS
        ),
        "algorithm_version": GROUND_FUSION_SHADOW_ALGORITHM,
    }
    for field_name in GroundFusionShadowReceipt.__dataclass_fields__:
        if field_name == "receipt_sha256":
            continue
        if field_name in values:
            object.__setattr__(provisional, field_name, values[field_name])
        elif field_name in defaults:
            object.__setattr__(provisional, field_name, defaults[field_name])
        else:
            raise TypeError(f"missing receipt field: {field_name}")
    digest = _digest(provisional.payload_mapping())
    return GroundFusionShadowReceipt(receipt_sha256=digest, **values)


_STORE_SCHEMA = """
CREATE TABLE manifest (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE receipts (
    sequence INTEGER PRIMARY KEY CHECK(sequence>0),
    receipt_sha256 TEXT NOT NULL UNIQUE CHECK(length(receipt_sha256)=64),
    attempt_id TEXT NOT NULL UNIQUE,
    request_sha256 TEXT NOT NULL UNIQUE CHECK(length(request_sha256)=64),
    payload BLOB NOT NULL,
    payload_bytes INTEGER NOT NULL CHECK(payload_bytes>0),
    created_at_ns INTEGER NOT NULL CHECK(created_at_ns>0)
);
CREATE TABLE archive_segments (
    segment_id TEXT PRIMARY KEY,
    previous_segment_id TEXT,
    first_sequence INTEGER NOT NULL UNIQUE CHECK(first_sequence>0),
    last_sequence INTEGER NOT NULL UNIQUE CHECK(last_sequence>=first_sequence),
    receipt_count INTEGER NOT NULL CHECK(receipt_count>0),
    content_sha256 TEXT NOT NULL UNIQUE CHECK(length(content_sha256)=64),
    file_sha256 TEXT NOT NULL UNIQUE CHECK(length(file_sha256)=64),
    filename TEXT NOT NULL UNIQUE,
    file_bytes INTEGER NOT NULL CHECK(file_bytes>0),
    manifest_json BLOB NOT NULL,
    created_at_ns INTEGER NOT NULL CHECK(created_at_ns>0),
    FOREIGN KEY(previous_segment_id) REFERENCES archive_segments(segment_id)
) WITHOUT ROWID;
CREATE TABLE archive_receipts (
    receipt_sha256 TEXT PRIMARY KEY CHECK(length(receipt_sha256)=64),
    attempt_id_sha256 TEXT NOT NULL UNIQUE CHECK(length(attempt_id_sha256)=64),
    request_sha256 TEXT NOT NULL UNIQUE CHECK(length(request_sha256)=64),
    sequence INTEGER NOT NULL UNIQUE CHECK(sequence>0),
    segment_id TEXT NOT NULL REFERENCES archive_segments(segment_id)
) WITHOUT ROWID;
CREATE TABLE archive_anchor (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    next_sequence INTEGER NOT NULL CHECK(next_sequence>0),
    archive_namespace TEXT,
    retention_policy_sha256 TEXT,
    last_segment_id TEXT,
    last_segment_file_sha256 TEXT,
    last_archived_sequence INTEGER NOT NULL CHECK(last_archived_sequence>=0),
    segment_count INTEGER NOT NULL CHECK(segment_count>=0),
    archived_receipt_count INTEGER NOT NULL CHECK(archived_receipt_count>=0),
    archive_bytes INTEGER NOT NULL CHECK(archive_bytes>=0),
    anchor_sha256 TEXT NOT NULL CHECK(length(anchor_sha256)=64)
);
CREATE TRIGGER manifest_no_update BEFORE UPDATE ON manifest
BEGIN SELECT RAISE(ABORT, 'append-only manifest'); END;
CREATE TRIGGER manifest_no_delete BEFORE DELETE ON manifest
BEGIN SELECT RAISE(ABORT, 'append-only manifest'); END;
CREATE TRIGGER receipts_no_update BEFORE UPDATE ON receipts
BEGIN SELECT RAISE(ABORT, 'append-only receipts'); END;
CREATE TRIGGER receipts_delete_requires_archive BEFORE DELETE ON receipts
WHEN NOT EXISTS (
    SELECT 1 FROM archive_receipts AS archived
    WHERE archived.receipt_sha256=OLD.receipt_sha256
      AND archived.request_sha256=OLD.request_sha256
      AND archived.sequence=OLD.sequence
)
BEGIN SELECT RAISE(ABORT, 'receipt is not durably indexed as archived'); END;
CREATE TRIGGER archive_segments_no_update BEFORE UPDATE ON archive_segments
BEGIN SELECT RAISE(ABORT, 'sealed archive segment is immutable'); END;
CREATE TRIGGER archive_segments_no_delete BEFORE DELETE ON archive_segments
BEGIN SELECT RAISE(ABORT, 'sealed archive segment is immutable'); END;
CREATE TRIGGER archive_receipts_no_update BEFORE UPDATE ON archive_receipts
BEGIN SELECT RAISE(ABORT, 'archive receipt index is immutable'); END;
CREATE TRIGGER archive_receipts_no_delete BEFORE DELETE ON archive_receipts
BEGIN SELECT RAISE(ABORT, 'archive receipt index is immutable'); END;
CREATE TRIGGER archive_anchor_no_delete BEFORE DELETE ON archive_anchor
BEGIN SELECT RAISE(ABORT, 'archive anchor is durable'); END;
"""


@dataclass(frozen=True, slots=True)
class _ArchiveAnchor:
    next_sequence: int
    archive_namespace: str | None
    retention_policy_sha256: str | None
    last_segment_id: str | None
    last_segment_file_sha256: str | None
    last_archived_sequence: int
    segment_count: int
    archived_receipt_count: int
    archive_bytes: int
    anchor_sha256: str

    def __post_init__(self) -> None:
        _count(self.next_sequence, "archive next_sequence")
        if self.next_sequence < 1:
            raise GroundFusionShadowArchiveError("archive next sequence is invalid")
        if self.archive_namespace is not None:
            _safe_id(self.archive_namespace, "archive_namespace")
        _optional_sha(
            self.retention_policy_sha256,
            "retention_policy_sha256",
        )
        if self.last_segment_id is not None:
            _safe_id(self.last_segment_id, "last_segment_id")
        _optional_sha(
            self.last_segment_file_sha256,
            "last_segment_file_sha256",
        )
        _count(self.last_archived_sequence, "last_archived_sequence")
        _count(self.segment_count, "archive segment_count", HARD_MAX_ARCHIVE_SEGMENTS)
        _count(
            self.archived_receipt_count,
            "archived_receipt_count",
            HARD_MAX_ARCHIVED_RECEIPTS,
        )
        _count(self.archive_bytes, "archive_bytes", HARD_MAX_ARCHIVE_TOTAL_BYTES)
        configured = self.archive_namespace is not None
        if any(
            value is not None
            for value in (
                self.retention_policy_sha256,
                self.last_segment_id,
                self.last_segment_file_sha256,
            )
        ) and not configured:
            raise GroundFusionShadowArchiveError("partial archive anchor configuration")
        if (self.last_segment_id is None) != (
            self.last_segment_file_sha256 is None
        ):
            raise GroundFusionShadowArchiveError("archive head identity is partial")
        if self.segment_count == 0:
            if (
                self.last_segment_id is not None
                or self.last_archived_sequence
                or self.archived_receipt_count
                or self.archive_bytes
            ):
                raise GroundFusionShadowArchiveError("empty archive anchor has history")
        elif (
            self.last_segment_id is None
            or self.last_archived_sequence < self.segment_count
            or self.archived_receipt_count < self.segment_count
            or self.last_archived_sequence != self.archived_receipt_count
        ):
            raise GroundFusionShadowArchiveError("archive anchor counters are inconsistent")
        if self.last_archived_sequence >= self.next_sequence:
            raise GroundFusionShadowArchiveError("archive sequence exceeds hot sequence head")
        _sha(self.anchor_sha256, "anchor_sha256")
        if self.anchor_sha256 != _digest(self.payload_mapping()):
            raise GroundFusionShadowArchiveError("archive anchor checksum differs")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "archive_bytes": self.archive_bytes,
            "archive_namespace": self.archive_namespace,
            "archived_receipt_count": self.archived_receipt_count,
            "last_archived_sequence": self.last_archived_sequence,
            "last_segment_file_sha256": self.last_segment_file_sha256,
            "last_segment_id": self.last_segment_id,
            "next_sequence": self.next_sequence,
            "retention_policy_sha256": self.retention_policy_sha256,
            "segment_count": self.segment_count,
        }


def _build_archive_anchor(**values: Any) -> _ArchiveAnchor:
    provisional = object.__new__(_ArchiveAnchor)
    for name in _ArchiveAnchor.__dataclass_fields__:
        object.__setattr__(
            provisional,
            name,
            "0" * 64 if name == "anchor_sha256" else values[name],
        )
    return _ArchiveAnchor(
        anchor_sha256=_digest(provisional.payload_mapping()),
        **values,
    )


def _store_schema_fingerprint(database: sqlite3.Connection) -> str:
    return _digest([
        [str(row[0]), str(row[1]), str(row[2])]
        for row in database.execute(
            "SELECT type,name,sql FROM sqlite_schema "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
        )
    ])


@lru_cache(maxsize=1)
def _expected_store_schema_fingerprint() -> str:
    with closing(sqlite3.connect(":memory:")) as database:
        database.executescript(_STORE_SCHEMA)
        return _store_schema_fingerprint(database)


@dataclass(frozen=True, slots=True)
class _ArchiveSegmentBundle:
    segment_id: str
    previous_segment_id: str | None
    first_sequence: int
    last_sequence: int
    receipt_count: int
    content_sha256: str
    file_sha256: str
    filename: str
    body: bytes
    manifest_body: bytes
    receipts: tuple[GroundFusionShadowReceipt, ...]

    def __post_init__(self) -> None:
        _safe_id(self.segment_id, "segment_id")
        if self.previous_segment_id is not None:
            _safe_id(self.previous_segment_id, "previous_segment_id")
        _count(self.first_sequence, "first_sequence")
        _count(self.last_sequence, "last_sequence")
        _count(self.receipt_count, "receipt_count", HARD_MAX_SOURCE_FACTS + 1)
        if (
            self.first_sequence < 1
            or self.last_sequence < self.first_sequence
            or self.receipt_count != self.last_sequence - self.first_sequence + 1
            or self.receipt_count != len(self.receipts)
        ):
            raise GroundFusionShadowArchiveError("archive segment range is invalid")
        _sha(self.content_sha256, "content_sha256")
        _sha(self.file_sha256, "file_sha256")
        if (
            not isinstance(self.filename, str)
            or Path(self.filename).name != self.filename
            or not self.filename.endswith(".sealed.json")
            or len(self.filename) > 255
        ):
            raise GroundFusionShadowArchiveError("archive filename is unsafe")
        if not isinstance(self.body, bytes) or not self.body:
            raise GroundFusionShadowArchiveError("archive body is empty")
        if hashlib.sha256(self.body).hexdigest() != self.file_sha256:
            raise GroundFusionShadowArchiveError("archive file checksum differs")
        if not isinstance(self.manifest_body, bytes) or not self.manifest_body:
            raise GroundFusionShadowArchiveError("archive manifest is empty")
        if any(
            not isinstance(item, GroundFusionShadowReceipt)
            for item in self.receipts
        ):
            raise GroundFusionShadowArchiveError("archive receipts are invalid")


class GroundFusionShadowReceiptStore:
    """Small append-only SQLite authority for deterministic shadow receipts."""

    def __init__(
        self,
        path: str | Path,
        *,
        runtime_profile_sha256: str,
        limits: GroundFusionShadowLimits,
        monotonic: Callable[[], float] = time.monotonic,
        clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.path = Path(path)
        self._profile = _sha(runtime_profile_sha256, "runtime_profile_sha256")
        if not isinstance(limits, GroundFusionShadowLimits):
            raise TypeError("limits must be GroundFusionShadowLimits")
        self._limits = limits
        self._monotonic = monotonic
        self._clock_ns = clock_ns
        self._lock = threading.RLock()
        self._initialized = False

    def _database_bytes(self) -> int:
        total = 0
        for suffix in ("", "-journal"):
            candidate = Path(str(self.path) + suffix)
            if candidate.exists():
                if candidate.is_symlink() or not candidate.is_file():
                    raise GroundFusionShadowStoreError(
                        "ground receipt database envelope is unsafe"
                    )
                total += candidate.stat().st_size
        if total > self._limits.max_database_bytes:
            raise GroundFusionShadowCapacityError(
                "ground receipt database byte capacity exceeded"
            )
        return total

    def _connect(self, deadline_at: float | None = None) -> sqlite3.Connection:
        if deadline_at is not None:
            _check_deadline(deadline_at, self._monotonic)
            remaining = max(0.001, deadline_at - float(self._monotonic()))
        else:
            remaining = 5.0
        self._database_bytes()
        database = sqlite3.connect(self.path, timeout=min(5.0, remaining))
        database.row_factory = sqlite3.Row
        try:
            database.execute(
                f"PRAGMA busy_timeout={max(1, int(min(5.0, remaining) * 1000))}"
            )
            mode = str(database.execute("PRAGMA journal_mode=DELETE").fetchone()[0])
            if mode.casefold() != "delete":
                raise GroundFusionShadowStoreError(
                    "ground receipt store could not enforce DELETE journaling"
                )
            database.execute("PRAGMA synchronous=FULL")
            database.execute("PRAGMA foreign_keys=ON")
            database.execute("PRAGMA temp_store=MEMORY")
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            max_pages = max(1, self._limits.max_database_bytes // page_size)
            page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
            if page_count > max_pages:
                raise GroundFusionShadowCapacityError(
                    "ground receipt database page capacity exceeded"
                )
            applied = int(
                database.execute(f"PRAGMA max_page_count={max_pages}").fetchone()[0]
            )
            if applied > max_pages:
                raise GroundFusionShadowCapacityError(
                    "SQLite could not enforce ground receipt page capacity"
                )
            if deadline_at is not None:
                database.set_progress_handler(
                    lambda: 1 if float(self._monotonic()) >= deadline_at else 0,
                    1000,
                )
            return database
        except BaseException:
            database.close()
            raise

    @staticmethod
    def _objects(database: sqlite3.Connection) -> tuple[tuple[str, str], ...]:
        return tuple(
            (str(row[0]), str(row[1]))
            for row in database.execute(
                "SELECT type,name FROM sqlite_schema "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
            )
        )

    def _manifest(self) -> dict[str, str]:
        return {
            "admission_status": GROUND_FUSION_SHADOW_ADMISSION_STATUS,
            "algorithm_version": GROUND_FUSION_SHADOW_ALGORITHM,
            "contract": GROUND_FUSION_SHADOW_RECEIPT_CONTRACT,
            "limits_sha256": self._limits.fingerprint,
            "runtime_profile_sha256": self._profile,
            "schema_sha256": _expected_store_schema_fingerprint(),
            "schema_version": str(GROUND_FUSION_SHADOW_SCHEMA_VERSION),
        }

    def _validate_schema(self, database: sqlite3.Connection) -> None:
        expected = (
            ("table", "archive_anchor"),
            ("table", "archive_receipts"),
            ("table", "archive_segments"),
            ("table", "manifest"),
            ("table", "receipts"),
            ("trigger", "archive_anchor_no_delete"),
            ("trigger", "archive_receipts_no_delete"),
            ("trigger", "archive_receipts_no_update"),
            ("trigger", "archive_segments_no_delete"),
            ("trigger", "archive_segments_no_update"),
            ("trigger", "manifest_no_delete"),
            ("trigger", "manifest_no_update"),
            ("trigger", "receipts_delete_requires_archive"),
            ("trigger", "receipts_no_update"),
        )
        if self._objects(database) != expected:
            raise GroundFusionShadowStoreError("ground receipt schema drift")
        if _store_schema_fingerprint(database) != _expected_store_schema_fingerprint():
            raise GroundFusionShadowStoreError("ground receipt schema SQL drift")
        actual = {
            str(row[0]): str(row[1])
            for row in database.execute("SELECT key,value FROM manifest")
        }
        if actual != self._manifest():
            raise GroundFusionShadowStoreError("ground receipt manifest drift")

    @staticmethod
    def _read_archive_anchor(database: sqlite3.Connection) -> _ArchiveAnchor:
        row = database.execute(
            "SELECT * FROM archive_anchor WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise GroundFusionShadowArchiveError("archive anchor is missing")
        try:
            return _ArchiveAnchor(
                next_sequence=int(row["next_sequence"]),
                archive_namespace=(
                    None
                    if row["archive_namespace"] is None
                    else str(row["archive_namespace"])
                ),
                retention_policy_sha256=(
                    None
                    if row["retention_policy_sha256"] is None
                    else str(row["retention_policy_sha256"])
                ),
                last_segment_id=(
                    None
                    if row["last_segment_id"] is None
                    else str(row["last_segment_id"])
                ),
                last_segment_file_sha256=(
                    None
                    if row["last_segment_file_sha256"] is None
                    else str(row["last_segment_file_sha256"])
                ),
                last_archived_sequence=int(row["last_archived_sequence"]),
                segment_count=int(row["segment_count"]),
                archived_receipt_count=int(row["archived_receipt_count"]),
                archive_bytes=int(row["archive_bytes"]),
                anchor_sha256=str(row["anchor_sha256"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise GroundFusionShadowArchiveError(
                "archive anchor failed exact validation"
            ) from exc

    @staticmethod
    def _write_archive_anchor_cas(
        database: sqlite3.Connection,
        previous: _ArchiveAnchor,
        next_anchor: _ArchiveAnchor,
    ) -> None:
        changed = database.execute(
            "UPDATE archive_anchor SET next_sequence=?,archive_namespace=?,"
            "retention_policy_sha256=?,last_segment_id=?,"
            "last_segment_file_sha256=?,last_archived_sequence=?,"
            "segment_count=?,archived_receipt_count=?,archive_bytes=?,"
            "anchor_sha256=? WHERE singleton=1 AND anchor_sha256=? "
            "AND next_sequence=?",
            (
                next_anchor.next_sequence,
                next_anchor.archive_namespace,
                next_anchor.retention_policy_sha256,
                next_anchor.last_segment_id,
                next_anchor.last_segment_file_sha256,
                next_anchor.last_archived_sequence,
                next_anchor.segment_count,
                next_anchor.archived_receipt_count,
                next_anchor.archive_bytes,
                next_anchor.anchor_sha256,
                previous.anchor_sha256,
                previous.next_sequence,
            ),
        ).rowcount
        if changed != 1:
            raise GroundFusionShadowConflict("archive anchor compare-and-swap failed")

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists() and (self.path.is_symlink() or not self.path.is_file()):
            raise GroundFusionShadowStoreError("ground receipt database path is unsafe")
        with self._lock, closing(self._connect()) as database:
            objects = self._objects(database)
            if not objects:
                try:
                    database.executescript("BEGIN IMMEDIATE;\n" + _STORE_SCHEMA)
                    for key, value in self._manifest().items():
                        database.execute(
                            "INSERT INTO manifest(key,value) VALUES(?,?)",
                            (key, value),
                        )
                    anchor = _build_archive_anchor(
                        next_sequence=1,
                        archive_namespace=None,
                        retention_policy_sha256=None,
                        last_segment_id=None,
                        last_segment_file_sha256=None,
                        last_archived_sequence=0,
                        segment_count=0,
                        archived_receipt_count=0,
                        archive_bytes=0,
                    )
                    database.execute(
                        "INSERT INTO archive_anchor VALUES(1,?,?,?,?,?,?,?,?,?,?)",
                        (
                            anchor.next_sequence,
                            anchor.archive_namespace,
                            anchor.retention_policy_sha256,
                            anchor.last_segment_id,
                            anchor.last_segment_file_sha256,
                            anchor.last_archived_sequence,
                            anchor.segment_count,
                            anchor.archived_receipt_count,
                            anchor.archive_bytes,
                            anchor.anchor_sha256,
                        ),
                    )
                    database.commit()
                except BaseException:
                    if database.in_transaction:
                        database.rollback()
                    raise
            self._validate_schema(database)
            self._read_archive_anchor(database)
            result = database.execute("PRAGMA quick_check(1)").fetchone()
            if result is None or str(result[0]).casefold() != "ok":
                raise GroundFusionShadowStoreError(
                    "ground receipt database integrity check failed"
                )
        self._database_bytes()
        self._initialized = True

    def _require_initialized(self) -> None:
        if not self._initialized:
            raise GroundFusionShadowStoreError("ground receipt store is not initialized")

    @staticmethod
    def _decode_row(row: sqlite3.Row) -> GroundFusionShadowReceipt:
        try:
            payload = bytes(row["payload"])
            value = json.loads(payload.decode("ascii"))
            if not isinstance(value, Mapping) or _canonical_json(value) != payload:
                raise ValueError("receipt JSON is not canonical")
            receipt = GroundFusionShadowReceipt.from_mapping(value)
            if _canonical_json(receipt.to_mapping()) != payload:
                raise ValueError("receipt payload contains unknown or coerced fields")
        except (KeyError, TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
            raise GroundFusionShadowStoreError(
                "ground receipt payload failed immutable verification"
            ) from exc
        if (
            receipt.receipt_sha256 != str(row["receipt_sha256"])
            or receipt.attempt_id != str(row["attempt_id"])
            or receipt.request_sha256 != str(row["request_sha256"])
            or len(payload) != int(row["payload_bytes"])
        ):
            raise GroundFusionShadowStoreError(
                "ground receipt materialized columns differ from payload"
            )
        return receipt

    @staticmethod
    def _attempt_archive_sha256(attempt_id: str) -> str:
        return _hash_identifier("stv3-ground-archive-attempt-v1", attempt_id)

    def _raise_if_archived(
        self,
        database: sqlite3.Connection,
        *,
        attempt_id: str,
        request_sha256: str,
    ) -> None:
        row = database.execute(
            "SELECT segment_id,request_sha256 FROM archive_receipts "
            "WHERE attempt_id_sha256=?",
            (self._attempt_archive_sha256(attempt_id),),
        ).fetchone()
        if row is not None:
            if str(row["request_sha256"]) != request_sha256:
                raise GroundFusionShadowConflict(
                    "archived attempt identity belongs to another request"
                )
            raise GroundFusionShadowWindowExpired(str(row["segment_id"]))
        row = database.execute(
            "SELECT segment_id FROM archive_receipts WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()
        if row is not None:
            raise GroundFusionShadowWindowExpired(str(row["segment_id"]))

    def load_existing(
        self,
        *,
        attempt_id: str,
        request_sha256: str,
        deadline_at: float | None = None,
    ) -> GroundFusionShadowReceipt | None:
        self._require_initialized()
        attempt = _safe_id(attempt_id, "attempt_id")
        request = _sha(request_sha256, "request_sha256")
        with self._lock, closing(self._connect(deadline_at)) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT * FROM receipts WHERE attempt_id=?",
                (attempt,),
            ).fetchone()
            if row is not None:
                receipt = self._decode_row(row)
                if receipt.request_sha256 != request:
                    raise GroundFusionShadowConflict(
                        "ground attempt identity was reused for another request"
                    )
                return receipt
            row = database.execute(
                "SELECT * FROM receipts WHERE request_sha256=?",
                (request,),
            ).fetchone()
            if row is not None:
                return self._decode_row(row)
            self._raise_if_archived(
                database,
                attempt_id=attempt,
                request_sha256=request,
            )
            return None

    def record_with_status(
        self,
        receipt: GroundFusionShadowReceipt,
        *,
        deadline_at: float | None = None,
    ) -> tuple[GroundFusionShadowReceipt, bool]:
        self._require_initialized()
        if not isinstance(receipt, GroundFusionShadowReceipt):
            raise TypeError("receipt has the wrong type")
        if receipt.runtime_profile_sha256 != self._profile:
            raise GroundFusionShadowConflict("receipt runtime profile differs")
        payload = _canonical_json(receipt.to_mapping())
        if len(payload) > self._limits.max_receipt_bytes:
            raise GroundFusionShadowCapacityError("ground receipt byte bound exceeded")
        with self._lock, closing(self._connect(deadline_at)) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                attempt_row = database.execute(
                    "SELECT * FROM receipts WHERE attempt_id=?",
                    (receipt.attempt_id,),
                ).fetchone()
                if attempt_row is not None:
                    existing = self._decode_row(attempt_row)
                    if existing.request_sha256 != receipt.request_sha256:
                        raise GroundFusionShadowConflict(
                            "ground attempt identity was reused for another request"
                        )
                    if existing.receipt_sha256 != receipt.receipt_sha256:
                        raise GroundFusionShadowConflict(
                            "idempotent ground retry produced another receipt"
                        )
                    database.rollback()
                    return existing, True
                request_row = database.execute(
                    "SELECT * FROM receipts WHERE request_sha256=?",
                    (receipt.request_sha256,),
                ).fetchone()
                if request_row is not None:
                    existing = self._decode_row(request_row)
                    database.rollback()
                    return existing, True
                self._raise_if_archived(
                    database,
                    attempt_id=receipt.attempt_id,
                    request_sha256=receipt.request_sha256,
                )
                total = int(database.execute("SELECT count(*) FROM receipts").fetchone()[0])
                if total >= self._limits.max_receipts:
                    raise GroundFusionShadowCapacityError(
                        "ground receipt row capacity exceeded"
                    )
                _check_deadline(deadline_at, self._monotonic) if deadline_at is not None else None
                created_at_ns = int(self._clock_ns())
                if created_at_ns <= 0:
                    raise GroundFusionShadowStoreError("ground receipt clock is invalid")
                anchor = self._read_archive_anchor(database)
                database.execute(
                    "INSERT INTO receipts(sequence,receipt_sha256,attempt_id,"
                    "request_sha256,payload,payload_bytes,created_at_ns) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (
                        anchor.next_sequence,
                        receipt.receipt_sha256,
                        receipt.attempt_id,
                        receipt.request_sha256,
                        payload,
                        len(payload),
                        created_at_ns,
                    ),
                )
                next_anchor = _build_archive_anchor(
                    next_sequence=anchor.next_sequence + 1,
                    archive_namespace=anchor.archive_namespace,
                    retention_policy_sha256=anchor.retention_policy_sha256,
                    last_segment_id=anchor.last_segment_id,
                    last_segment_file_sha256=(
                        anchor.last_segment_file_sha256
                    ),
                    last_archived_sequence=anchor.last_archived_sequence,
                    segment_count=anchor.segment_count,
                    archived_receipt_count=anchor.archived_receipt_count,
                    archive_bytes=anchor.archive_bytes,
                )
                self._write_archive_anchor_cas(database, anchor, next_anchor)
                database.commit()
            except (GroundFusionShadowError, ValueError):
                if database.in_transaction:
                    database.rollback()
                raise
            except sqlite3.DatabaseError as exc:
                if database.in_transaction:
                    database.rollback()
                if deadline_at is not None and float(self._monotonic()) >= deadline_at:
                    raise GroundFusionShadowDeadlineError(
                        "ground receipt commit deadline elapsed"
                    ) from exc
                if "full" in str(exc).casefold():
                    raise GroundFusionShadowCapacityError(
                        "ground receipt SQLite capacity exceeded"
                    ) from exc
                raise GroundFusionShadowStoreError(
                    "ground receipt SQLite write failed closed"
                ) from exc
        self._database_bytes()
        return receipt, False

    def record(
        self,
        receipt: GroundFusionShadowReceipt,
        *,
        deadline_at: float | None = None,
    ) -> GroundFusionShadowReceipt:
        """Compatibility view returning only the immutable receipt."""

        return self.record_with_status(
            receipt,
            deadline_at=deadline_at,
        )[0]

    @staticmethod
    def _default_directory_fsync(path: Path) -> None:
        if os.name == "nt":
            raise GroundFusionShadowDurabilityError(
                "directory fsync is unavailable on this platform"
            )
        descriptor: int | None = None
        try:
            descriptor = os.open(
                str(path),
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            os.fsync(descriptor)
        except OSError as exc:
            if exc.errno == errno.ENOSPC:
                raise GroundFusionShadowCapacityError(
                    "archive durability barrier ran out of space"
                ) from exc
            raise GroundFusionShadowDurabilityError(
                "archive directory fsync failed"
            ) from exc
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError as exc:
                    raise GroundFusionShadowDurabilityError(
                        "archive directory descriptor close failed"
                    ) from exc

    @staticmethod
    def _invoke_fault(
        fault_injector: Callable[[str], None] | None,
        point: str,
    ) -> None:
        if point not in GROUND_FUSION_SHADOW_ROTATION_FAULT_POINTS:
            raise AssertionError("unknown ground archive fault point")
        if fault_injector is not None:
            fault_injector(point)

    @staticmethod
    def _prepare_archive_directory(
        archive_directory: Path,
        directory_fsync: Callable[[Path], None],
    ) -> None:
        missing: list[Path] = []
        probe = archive_directory
        while not probe.exists():
            missing.append(probe)
            if probe.parent == probe:
                break
            probe = probe.parent
        archive_directory.mkdir(parents=True, exist_ok=True)
        if archive_directory.is_symlink() or not archive_directory.is_dir():
            raise GroundFusionShadowArchiveError("archive directory is unsafe")
        for created in reversed(missing):
            if created.exists():
                directory_fsync(created)
                if created.parent.exists():
                    directory_fsync(created.parent)

    @staticmethod
    def _bounded_archive_entries(
        archive_directory: Path,
        maximum: int,
    ) -> tuple[Path, ...]:
        entries: list[Path] = []
        for item in archive_directory.iterdir():
            entries.append(item)
            if len(entries) > maximum:
                raise GroundFusionShadowCapacityError(
                    "archive directory entry cap exceeded"
                )
        return tuple(entries)

    def _build_archive_segment(
        self,
        rows: Sequence[sqlite3.Row],
        *,
        anchor: _ArchiveAnchor,
        policy: GroundFusionShadowRetentionPolicy,
    ) -> _ArchiveSegmentBundle:
        if not rows:
            raise GroundFusionShadowArchiveError("archive segment cannot be empty")
        records: list[dict[str, Any]] = []
        receipts: list[GroundFusionShadowReceipt] = []
        sequences: list[int] = []
        for row in rows:
            receipt = self._decode_row(row)
            sequence = int(row["sequence"])
            payload = bytes(row["payload"])
            sequences.append(sequence)
            receipts.append(receipt)
            records.append({
                "payload_base64": base64.b64encode(payload).decode("ascii"),
                "payload_bytes": len(payload),
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "receipt_sha256": receipt.receipt_sha256,
                "sequence": sequence,
            })
        first = sequences[0]
        last = sequences[-1]
        expected = list(range(anchor.last_archived_sequence + 1, last + 1))
        if sequences != expected or first != anchor.last_archived_sequence + 1:
            raise GroundFusionShadowArchiveError(
                "hot/archive sequence gap prevents rotation"
            )
        core = {
            "archive_namespace": policy.archive_namespace,
            "first_sequence": first,
            "last_sequence": last,
            "previous_segment_id": anchor.last_segment_id,
            "receipt_count": len(records),
            "receipts": records,
            "retention_policy_sha256": policy.fingerprint,
            "runtime_profile_sha256": self._profile,
        }
        content_sha = _digest(core)
        segment_id = "ground-segment-" + content_sha
        sealed = {
            "content_sha256": content_sha,
            "contract": GROUND_FUSION_SHADOW_ARCHIVE_CONTRACT,
            "segment": core,
            "segment_id": segment_id,
        }
        body = _canonical_json(sealed)
        file_sha = hashlib.sha256(body).hexdigest()
        filename = (
            f"ground-{first:020d}-{last:020d}-{content_sha}.sealed.json"
        )
        manifest = {
            "archive_namespace": policy.archive_namespace,
            "content_sha256": content_sha,
            "contract": GROUND_FUSION_SHADOW_ARCHIVE_MANIFEST_CONTRACT,
            "file_bytes": len(body),
            "file_sha256": file_sha,
            "filename": filename,
            "first_sequence": first,
            "last_sequence": last,
            "previous_segment_id": anchor.last_segment_id,
            "receipt_count": len(records),
            "retention_policy_sha256": policy.fingerprint,
            "runtime_profile_sha256": self._profile,
            "segment_id": segment_id,
        }
        return _ArchiveSegmentBundle(
            segment_id=segment_id,
            previous_segment_id=anchor.last_segment_id,
            first_sequence=first,
            last_sequence=last,
            receipt_count=len(records),
            content_sha256=content_sha,
            file_sha256=file_sha,
            filename=filename,
            body=body,
            manifest_body=_canonical_json(manifest),
            receipts=tuple(receipts),
        )

    def _decode_archive_segment(
        self,
        body: bytes,
        *,
        policy: GroundFusionShadowRetentionPolicy,
    ) -> _ArchiveSegmentBundle:
        if not isinstance(body, bytes) or not 1 <= len(body) <= policy.max_segment_bytes:
            raise GroundFusionShadowArchiveError("archive segment byte bound failed")
        try:
            value = json.loads(body.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise GroundFusionShadowArchiveError("archive JSON is invalid") from exc
        if not isinstance(value, Mapping) or _canonical_json(value) != body:
            raise GroundFusionShadowArchiveError("archive JSON is not canonical")
        if set(value) != {"content_sha256", "contract", "segment", "segment_id"}:
            raise GroundFusionShadowArchiveError("archive envelope schema differs")
        if value["contract"] != GROUND_FUSION_SHADOW_ARCHIVE_CONTRACT:
            raise GroundFusionShadowArchiveError("archive contract differs")
        core = value["segment"]
        if not isinstance(core, Mapping) or set(core) != {
            "archive_namespace",
            "first_sequence",
            "last_sequence",
            "previous_segment_id",
            "receipt_count",
            "receipts",
            "retention_policy_sha256",
            "runtime_profile_sha256",
        }:
            raise GroundFusionShadowArchiveError("archive segment schema differs")
        content_sha = _digest(core)
        segment_id = "ground-segment-" + content_sha
        if (
            str(value["content_sha256"]) != content_sha
            or str(value["segment_id"]) != segment_id
            or str(core["archive_namespace"]) != policy.archive_namespace
            or str(core["retention_policy_sha256"]) != policy.fingerprint
            or str(core["runtime_profile_sha256"]) != self._profile
        ):
            raise GroundFusionShadowArchiveError("archive identity/profile differs")
        first = _count(core["first_sequence"], "archive first_sequence")
        last = _count(core["last_sequence"], "archive last_sequence")
        count = _count(
            core["receipt_count"],
            "archive receipt_count",
            policy.max_segment_receipts,
        )
        raw_records = core["receipts"]
        if not isinstance(raw_records, list) or len(raw_records) != count:
            raise GroundFusionShadowArchiveError("archive receipt count differs")
        receipts: list[GroundFusionShadowReceipt] = []
        sequences: list[int] = []
        for record in raw_records:
            if not isinstance(record, Mapping) or set(record) != {
                "payload_base64",
                "payload_bytes",
                "payload_sha256",
                "receipt_sha256",
                "sequence",
            }:
                raise GroundFusionShadowArchiveError("archive record schema differs")
            try:
                payload = base64.b64decode(
                    str(record["payload_base64"]).encode("ascii"),
                    validate=True,
                )
            except (UnicodeEncodeError, ValueError, binascii.Error) as exc:
                raise GroundFusionShadowArchiveError(
                    "archive receipt encoding is invalid"
                ) from exc
            payload_bytes = _count(
                record["payload_bytes"],
                "archive payload_bytes",
                self._limits.max_receipt_bytes,
            )
            sequence = _count(record["sequence"], "archive receipt sequence")
            if (
                len(payload) != payload_bytes
                or hashlib.sha256(payload).hexdigest()
                != str(record["payload_sha256"])
            ):
                raise GroundFusionShadowArchiveError(
                    "archive receipt payload checksum differs"
                )
            try:
                receipt_value = json.loads(payload.decode("ascii"))
                receipt = GroundFusionShadowReceipt.from_mapping(receipt_value)
            except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                raise GroundFusionShadowArchiveError(
                    "archived receipt failed immutable validation"
                ) from exc
            if (
                not isinstance(receipt_value, Mapping)
                or _canonical_json(receipt_value) != payload
                or _canonical_json(receipt.to_mapping()) != payload
                or receipt.receipt_sha256 != str(record["receipt_sha256"])
                or receipt.runtime_profile_sha256 != self._profile
            ):
                raise GroundFusionShadowArchiveError(
                    "archived receipt differs from exact payload"
                )
            sequences.append(sequence)
            receipts.append(receipt)
        if sequences != list(range(first, last + 1)):
            raise GroundFusionShadowArchiveError("archive record sequence has a gap")
        file_sha = hashlib.sha256(body).hexdigest()
        filename = f"ground-{first:020d}-{last:020d}-{content_sha}.sealed.json"
        manifest = {
            "archive_namespace": policy.archive_namespace,
            "content_sha256": content_sha,
            "contract": GROUND_FUSION_SHADOW_ARCHIVE_MANIFEST_CONTRACT,
            "file_bytes": len(body),
            "file_sha256": file_sha,
            "filename": filename,
            "first_sequence": first,
            "last_sequence": last,
            "previous_segment_id": core["previous_segment_id"],
            "receipt_count": count,
            "retention_policy_sha256": policy.fingerprint,
            "runtime_profile_sha256": self._profile,
            "segment_id": segment_id,
        }
        return _ArchiveSegmentBundle(
            segment_id=segment_id,
            previous_segment_id=(
                None
                if core["previous_segment_id"] is None
                else str(core["previous_segment_id"])
            ),
            first_sequence=first,
            last_sequence=last,
            receipt_count=count,
            content_sha256=content_sha,
            file_sha256=file_sha,
            filename=filename,
            body=body,
            manifest_body=_canonical_json(manifest),
            receipts=tuple(receipts),
        )

    def _prune_durably_archived_hot_rows(
        self,
        database: sqlite3.Connection,
        archive_directory: Path,
        policy: GroundFusionShadowRetentionPolicy,
        anchor: _ArchiveAnchor,
        fault_injector: Callable[[str], None] | None,
    ) -> int:
        """Resume only a manifest-committed prune after verifying exact files."""

        pending = database.execute(
            "SELECT hot.* FROM receipts AS hot JOIN archive_receipts AS archived "
            "ON archived.sequence=hot.sequence "
            "AND archived.receipt_sha256=hot.receipt_sha256 "
            "WHERE hot.sequence<=? ORDER BY hot.sequence",
            (anchor.last_archived_sequence,),
        ).fetchall()
        unexplained = int(database.execute(
            "SELECT count(*) FROM receipts WHERE sequence<=? AND sequence NOT IN "
            "(SELECT sequence FROM archive_receipts)",
            (anchor.last_archived_sequence,),
        ).fetchone()[0])
        if unexplained:
            raise GroundFusionShadowArchiveError(
                "hot/archive overlap lacks a durable archive index"
            )
        if not pending:
            return 0
        if len(pending) > policy.max_segment_receipts:
            raise GroundFusionShadowCapacityError(
                "pending archive prune exceeds recovery bound"
            )
        bundles: dict[str, _ArchiveSegmentBundle] = {}
        for row in pending:
            index = database.execute(
                "SELECT * FROM archive_receipts WHERE sequence=?",
                (int(row["sequence"]),),
            ).fetchone()
            if index is None:
                raise GroundFusionShadowArchiveError(
                    "pending prune lost archive index"
                )
            segment_id = str(index["segment_id"])
            bundle = bundles.get(segment_id)
            if bundle is None:
                segment_row = database.execute(
                    "SELECT * FROM archive_segments WHERE segment_id=?",
                    (segment_id,),
                ).fetchone()
                if segment_row is None:
                    raise GroundFusionShadowArchiveError(
                        "pending prune references no durable manifest"
                    )
                bundle = self._bundle_from_segment_row(
                    archive_directory,
                    segment_row,
                    policy,
                )
                bundles[segment_id] = bundle
            offset = int(row["sequence"]) - bundle.first_sequence
            if not 0 <= offset < len(bundle.receipts):
                raise GroundFusionShadowArchiveError(
                    "pending prune sequence lies outside sealed segment"
                )
            receipt = self._decode_row(row)
            if bundle.receipts[offset] != receipt:
                raise GroundFusionShadowArchiveError(
                    "pending hot receipt differs from sealed archive"
                )
        self._invoke_fault(fault_injector, "rotate.before_prune")
        for row in pending:
            changed = database.execute(
                "DELETE FROM receipts WHERE sequence=? AND receipt_sha256=?",
                (int(row["sequence"]), str(row["receipt_sha256"])),
            ).rowcount
            if changed != 1:
                raise GroundFusionShadowConflict(
                    "pending archived receipt changed before recovery prune"
                )
        self._invoke_fault(fault_injector, "rotate.after_prune")
        return len(pending)

    def rotate_archive(
        self,
        archive_directory: str | Path,
        *,
        policy: GroundFusionShadowRetentionPolicy,
        deadline_at: float,
        fault_injector: Callable[[str], None] | None = None,
        directory_fsync: Callable[[Path], None] | None = None,
    ) -> GroundFusionShadowRotationResult:
        """Seal the oldest contiguous page before pruning it from hot storage."""

        self._require_initialized()
        if not isinstance(policy, GroundFusionShadowRetentionPolicy):
            raise TypeError("policy must be GroundFusionShadowRetentionPolicy")
        if policy.hot_receipts_to_keep >= self._limits.max_receipts:
            raise ValueError("hot replay window must be smaller than hot row capacity")
        deadline = _finite(deadline_at, "deadline_at")
        started_at = _finite(self._monotonic(), "monotonic clock")
        if deadline <= started_at:
            raise GroundFusionShadowDeadlineError(
                "ground archive rotation deadline elapsed"
            )
        if deadline - started_at > self._limits.max_runtime_seconds:
            raise ValueError("archive deadline exceeds runtime boundary")
        archive_path = Path(archive_directory)
        fsync_directory = directory_fsync or self._default_directory_fsync
        if not callable(fsync_directory):
            raise TypeError("directory_fsync must be callable")
        self._prepare_archive_directory(archive_path, fsync_directory)
        temporary_path: Path | None = None
        reused = False
        committed = False
        with self._lock, closing(self._connect(deadline)) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                anchor = self._read_archive_anchor(database)
                if anchor.archive_namespace is not None and (
                    anchor.archive_namespace != policy.archive_namespace
                    or anchor.retention_policy_sha256 != policy.fingerprint
                ):
                    raise GroundFusionShadowConflict(
                        "archive namespace or retention policy differs from anchor"
                    )
                initial_entries = self._bounded_archive_entries(
                    archive_path,
                    policy.max_archive_segments + 1,
                )
                if any(
                    candidate.name.startswith(".")
                    or candidate.is_symlink()
                    or not candidate.is_file()
                    or not candidate.name.endswith(".sealed.json")
                    for candidate in initial_entries
                ):
                    raise GroundFusionShadowArchiveError(
                        "partial or unsafe archive publication requires recovery"
                    )
                pending_count = int(database.execute(
                    "SELECT count(*) FROM receipts AS hot JOIN archive_receipts "
                    "AS archived ON archived.sequence=hot.sequence "
                    "AND archived.receipt_sha256=hot.receipt_sha256 "
                    "WHERE hot.sequence<=?",
                    (anchor.last_archived_sequence,),
                ).fetchone()[0])
                if pending_count:
                    durable_names = {
                        str(row[0])
                        for row in database.execute(
                            "SELECT filename FROM archive_segments"
                        )
                    }
                    if {item.name for item in initial_entries} != durable_names:
                        raise GroundFusionShadowArchiveError(
                            "pending prune archive set differs from durable manifest"
                        )
                _check_deadline(deadline, self._monotonic)
                recovered_prune = self._prune_durably_archived_hot_rows(
                    database,
                    archive_path,
                    policy,
                    anchor,
                    fault_injector,
                )
                _check_deadline(deadline, self._monotonic)
                if recovered_prune:
                    self._invoke_fault(fault_injector, "rotate.before_commit")
                    _check_deadline(deadline, self._monotonic)
                    database.commit()
                    self._invoke_fault(fault_injector, "rotate.after_commit")
                    fsync_directory(self.path.parent)
                    _check_deadline(deadline, self._monotonic)
                    self._invoke_fault(
                        fault_injector,
                        "rotate.after_database_directory_fsync",
                    )
                    database.execute("BEGIN IMMEDIATE")
                    self._validate_schema(database)
                    anchor = self._read_archive_anchor(database)
                hot_count = int(
                    database.execute("SELECT count(*) FROM receipts").fetchone()[0]
                )
                eligible_count = max(0, hot_count - policy.hot_receipts_to_keep)
                if eligible_count == 0:
                    database.rollback()
                    return GroundFusionShadowRotationResult(
                        segment_id=None,
                        archived_receipts=0,
                        hot_receipts=hot_count,
                        duplicate_orphan_reused=False,
                        archive_anchor_sha256=anchor.anchor_sha256,
                    )
                row_count = min(eligible_count, policy.max_segment_receipts)
                rows = database.execute(
                    "SELECT * FROM receipts ORDER BY sequence LIMIT ?",
                    (row_count,),
                ).fetchall()
                if len(rows) != row_count:
                    raise GroundFusionShadowArchiveError(
                        "hot receipt snapshot changed during rotation"
                    )
                bundle = self._build_archive_segment(
                    rows,
                    anchor=anchor,
                    policy=policy,
                )
                _check_deadline(deadline, self._monotonic)
                if len(bundle.body) > policy.max_segment_bytes:
                    raise GroundFusionShadowCapacityError(
                        "sealed archive segment exceeds segment byte cap"
                    )
                if anchor.segment_count + 1 > policy.max_archive_segments:
                    raise GroundFusionShadowCapacityError(
                        "archive segment count capacity exceeded"
                    )
                if (
                    anchor.archived_receipt_count + bundle.receipt_count
                    > policy.max_archived_receipts
                ):
                    raise GroundFusionShadowCapacityError(
                        "archived receipt count capacity exceeded"
                    )
                if (
                    anchor.archive_bytes + len(bundle.body)
                    > policy.max_total_archive_bytes
                ):
                    raise GroundFusionShadowCapacityError(
                        "total sealed archive byte capacity exceeded"
                    )
                self._invoke_fault(fault_injector, "rotate.after_snapshot")

                manifest_names = {
                    str(row[0])
                    for row in database.execute(
                        "SELECT filename FROM archive_segments"
                    )
                }
                archive_entries = self._bounded_archive_entries(
                    archive_path,
                    policy.max_archive_segments + 1,
                )
                if any(
                    candidate.is_symlink()
                    or not candidate.is_file()
                    or candidate.name.startswith(".")
                    or not candidate.name.endswith(".sealed.json")
                    for candidate in archive_entries
                ):
                    raise GroundFusionShadowArchiveError(
                        "archive directory contains unsafe or unexpected entries"
                    )
                unexpected = (
                    {candidate.name for candidate in archive_entries}
                    - manifest_names
                    - {bundle.filename}
                )
                if unexpected:
                    raise GroundFusionShadowArchiveError(
                        "unanchored archive segment requires runbook recovery"
                    )
                final_path = archive_path / bundle.filename
                if final_path.exists():
                    if final_path.is_symlink() or not final_path.is_file():
                        raise GroundFusionShadowArchiveError(
                            "archive segment path is unsafe"
                        )
                    if (
                        final_path.stat().st_size != len(bundle.body)
                        or final_path.read_bytes() != bundle.body
                    ):
                        raise GroundFusionShadowArchiveError(
                            "published orphan archive differs from retry"
                        )
                    reused = True
                    fsync_directory(archive_path)
                    _check_deadline(deadline, self._monotonic)
                    self._invoke_fault(
                        fault_injector,
                        "rotate.after_publish_directory_fsync",
                    )
                else:
                    temporary_path = archive_path / ("." + bundle.filename + ".tmp")
                    if temporary_path.exists():
                        raise GroundFusionShadowArchiveError(
                            "archive temporary file already exists"
                        )
                    with open(temporary_path, "xb") as output:
                        output.write(bundle.body)
                        output.flush()
                        _check_deadline(deadline, self._monotonic)
                        self._invoke_fault(
                            fault_injector,
                            "rotate.after_temp_write",
                        )
                        os.fsync(output.fileno())
                        _check_deadline(deadline, self._monotonic)
                        self._invoke_fault(
                            fault_injector,
                            "rotate.after_temp_fsync",
                        )
                    fsync_directory(archive_path)
                    _check_deadline(deadline, self._monotonic)
                    self._invoke_fault(
                        fault_injector,
                        "rotate.after_temp_directory_fsync",
                    )
                    os.replace(temporary_path, final_path)
                    temporary_path = None
                    self._invoke_fault(
                        fault_injector,
                        "rotate.after_publish_replace",
                    )
                    fsync_directory(archive_path)
                    _check_deadline(deadline, self._monotonic)
                    self._invoke_fault(
                        fault_injector,
                        "rotate.after_publish_directory_fsync",
                    )
                    if final_path.read_bytes() != bundle.body:
                        raise GroundFusionShadowArchiveError(
                            "archive changed during atomic publication"
                        )

                created_at_ns = int(self._clock_ns())
                if created_at_ns <= 0:
                    raise GroundFusionShadowStoreError("archive clock is invalid")
                database.execute(
                    "INSERT INTO archive_segments VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        bundle.segment_id,
                        bundle.previous_segment_id,
                        bundle.first_sequence,
                        bundle.last_sequence,
                        bundle.receipt_count,
                        bundle.content_sha256,
                        bundle.file_sha256,
                        bundle.filename,
                        len(bundle.body),
                        sqlite3.Binary(bundle.manifest_body),
                        created_at_ns,
                    ),
                )
                self._invoke_fault(fault_injector, "rotate.after_manifest_insert")
                for row, receipt in zip(rows, bundle.receipts, strict=True):
                    database.execute(
                        "INSERT INTO archive_receipts VALUES(?,?,?,?,?)",
                        (
                            receipt.receipt_sha256,
                            self._attempt_archive_sha256(receipt.attempt_id),
                            receipt.request_sha256,
                            int(row["sequence"]),
                            bundle.segment_id,
                        ),
                    )
                self._invoke_fault(fault_injector, "rotate.after_archive_index")
                next_anchor = _build_archive_anchor(
                    next_sequence=anchor.next_sequence,
                    archive_namespace=policy.archive_namespace,
                    retention_policy_sha256=policy.fingerprint,
                    last_segment_id=bundle.segment_id,
                    last_segment_file_sha256=bundle.file_sha256,
                    last_archived_sequence=bundle.last_sequence,
                    segment_count=anchor.segment_count + 1,
                    archived_receipt_count=(
                        anchor.archived_receipt_count + bundle.receipt_count
                    ),
                    archive_bytes=anchor.archive_bytes + len(bundle.body),
                )
                self._write_archive_anchor_cas(database, anchor, next_anchor)
                self._invoke_fault(fault_injector, "rotate.after_anchor_cas")
                self._invoke_fault(fault_injector, "rotate.before_manifest_commit")
                _check_deadline(deadline, self._monotonic)
                database.commit()
                self._invoke_fault(fault_injector, "rotate.after_manifest_commit")
                fsync_directory(self.path.parent)
                _check_deadline(deadline, self._monotonic)
                self._invoke_fault(
                    fault_injector,
                    "rotate.after_manifest_database_fsync",
                )

                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                durable_anchor = self._read_archive_anchor(database)
                durable_segment = database.execute(
                    "SELECT * FROM archive_segments WHERE segment_id=?",
                    (bundle.segment_id,),
                ).fetchone()
                if durable_segment is None or self._bundle_from_segment_row(
                    archive_path,
                    durable_segment,
                    policy,
                ) != bundle:
                    raise GroundFusionShadowArchiveError(
                        "durable manifest/file verification failed before prune"
                    )
                remaining = int(database.execute(
                    "SELECT count(*) FROM receipts WHERE sequence BETWEEN ? AND ?",
                    (bundle.first_sequence, bundle.last_sequence),
                ).fetchone()[0])
                archive_head_matches = (
                    durable_anchor.next_sequence >= next_anchor.next_sequence
                    and durable_anchor.archive_namespace
                    == next_anchor.archive_namespace
                    and durable_anchor.retention_policy_sha256
                    == next_anchor.retention_policy_sha256
                    and durable_anchor.last_segment_id
                    == next_anchor.last_segment_id
                    and durable_anchor.last_segment_file_sha256
                    == next_anchor.last_segment_file_sha256
                    and durable_anchor.last_archived_sequence
                    == next_anchor.last_archived_sequence
                    and durable_anchor.segment_count == next_anchor.segment_count
                    and durable_anchor.archived_receipt_count
                    == next_anchor.archived_receipt_count
                    and durable_anchor.archive_bytes == next_anchor.archive_bytes
                )
                if remaining not in (0, bundle.receipt_count):
                    raise GroundFusionShadowArchiveError(
                        "archive prune was only partially applied"
                    )
                if not archive_head_matches and remaining:
                    raise GroundFusionShadowConflict(
                        "archive anchor advanced before this segment was pruned"
                    )
                if remaining:
                    _check_deadline(deadline, self._monotonic)
                    self._invoke_fault(fault_injector, "rotate.before_prune")
                    for row, receipt in zip(rows, bundle.receipts, strict=True):
                        changed = database.execute(
                            "DELETE FROM receipts WHERE sequence=? "
                            "AND receipt_sha256=?",
                            (int(row["sequence"]), receipt.receipt_sha256),
                        ).rowcount
                        if changed != 1:
                            raise GroundFusionShadowConflict(
                                "hot receipt changed before archive prune"
                            )
                final_hot_count = int(
                    database.execute("SELECT count(*) FROM receipts").fetchone()[0]
                )
                self._invoke_fault(fault_injector, "rotate.after_prune")
                self._invoke_fault(fault_injector, "rotate.before_commit")
                _check_deadline(deadline, self._monotonic)
                database.commit()
                committed = True
                self._invoke_fault(fault_injector, "rotate.after_commit")
            except OSError as exc:
                if database.in_transaction:
                    database.rollback()
                if temporary_path is not None and temporary_path.exists():
                    try:
                        temporary_path.unlink()
                    except OSError:
                        pass
                if exc.errno == errno.ENOSPC:
                    raise GroundFusionShadowCapacityError(
                        "archive filesystem is full"
                    ) from exc
                raise GroundFusionShadowArchiveError(
                    "archive filesystem operation failed"
                ) from exc
            except sqlite3.DatabaseError as exc:
                if database.in_transaction:
                    database.rollback()
                if "full" in str(exc).casefold():
                    raise GroundFusionShadowCapacityError(
                        "archive manifest database is full"
                    ) from exc
                raise GroundFusionShadowArchiveError(
                    "archive manifest transaction failed"
                ) from exc
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                if temporary_path is not None and temporary_path.exists():
                    try:
                        temporary_path.unlink()
                    except OSError:
                        pass
                raise
        if not committed:
            raise GroundFusionShadowArchiveError("archive commit did not complete")
        fsync_directory(self.path.parent)
        _check_deadline(deadline, self._monotonic)
        self._invoke_fault(
            fault_injector,
            "rotate.after_database_directory_fsync",
        )
        self._database_bytes()
        return GroundFusionShadowRotationResult(
            segment_id=bundle.segment_id,
            archived_receipts=bundle.receipt_count,
            hot_receipts=final_hot_count,
            duplicate_orphan_reused=reused,
            archive_anchor_sha256=durable_anchor.anchor_sha256,
        )

    def _bundle_from_segment_row(
        self,
        archive_directory: Path,
        row: sqlite3.Row,
        policy: GroundFusionShadowRetentionPolicy,
    ) -> _ArchiveSegmentBundle:
        filename = str(row["filename"])
        if Path(filename).name != filename or not filename.endswith(".sealed.json"):
            raise GroundFusionShadowArchiveError("archive manifest filename is unsafe")
        path = archive_directory / filename
        if not path.exists() or path.is_symlink() or not path.is_file():
            raise GroundFusionShadowArchiveError("anchored archive file is unavailable")
        expected_size = int(row["file_bytes"])
        if (
            expected_size < 1
            or expected_size > policy.max_segment_bytes
            or path.stat().st_size != expected_size
        ):
            raise GroundFusionShadowArchiveError("archive file size differs")
        body = path.read_bytes()
        bundle = self._decode_archive_segment(body, policy=policy)
        materialized_manifest = bytes(row["manifest_json"])
        if (
            bundle.segment_id != str(row["segment_id"])
            or bundle.previous_segment_id
            != (
                None
                if row["previous_segment_id"] is None
                else str(row["previous_segment_id"])
            )
            or bundle.first_sequence != int(row["first_sequence"])
            or bundle.last_sequence != int(row["last_sequence"])
            or bundle.receipt_count != int(row["receipt_count"])
            or bundle.content_sha256 != str(row["content_sha256"])
            or bundle.file_sha256 != str(row["file_sha256"])
            or bundle.filename != filename
            or bundle.manifest_body != materialized_manifest
        ):
            raise GroundFusionShadowArchiveError(
                "archive file and durable manifest differ"
            )
        return bundle

    def load_archived(
        self,
        archive_directory: str | Path,
        *,
        policy: GroundFusionShadowRetentionPolicy,
        attempt_id: str | None = None,
        request_sha256: str | None = None,
    ) -> GroundFusionShadowReceipt | None:
        """Perform an explicit byte-verified lookup outside the hot window."""

        self._require_initialized()
        if not isinstance(policy, GroundFusionShadowRetentionPolicy):
            raise TypeError("policy must be GroundFusionShadowRetentionPolicy")
        if (attempt_id is None) == (request_sha256 is None):
            raise ValueError("provide exactly one archived lookup identity")
        attempt = None if attempt_id is None else _safe_id(attempt_id, "attempt_id")
        request = (
            None
            if request_sha256 is None
            else _sha(request_sha256, "request_sha256")
        )
        archive_path = Path(archive_directory)
        if archive_path.is_symlink() or not archive_path.is_dir():
            raise GroundFusionShadowArchiveError("archive directory is unavailable")
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database)
            anchor = self._read_archive_anchor(database)
            if (
                anchor.archive_namespace != policy.archive_namespace
                or anchor.retention_policy_sha256 != policy.fingerprint
            ):
                raise GroundFusionShadowConflict(
                    "archived lookup policy differs from durable anchor"
                )
            if attempt is not None:
                index = database.execute(
                    "SELECT * FROM archive_receipts WHERE attempt_id_sha256=?",
                    (self._attempt_archive_sha256(attempt),),
                ).fetchone()
            else:
                index = database.execute(
                    "SELECT * FROM archive_receipts WHERE request_sha256=?",
                    (request,),
                ).fetchone()
            if index is None:
                return None
            segment = database.execute(
                "SELECT * FROM archive_segments WHERE segment_id=?",
                (str(index["segment_id"]),),
            ).fetchone()
            if segment is None:
                raise GroundFusionShadowArchiveError(
                    "archive receipt index references no segment"
                )
            expected_receipt_sha = str(index["receipt_sha256"])
            expected_sequence = int(index["sequence"])
            expected_request = str(index["request_sha256"])
        bundle = self._bundle_from_segment_row(archive_path, segment, policy)
        offset = expected_sequence - bundle.first_sequence
        if not 0 <= offset < len(bundle.receipts):
            raise GroundFusionShadowArchiveError(
                "archive receipt index lies outside its segment"
            )
        receipt = bundle.receipts[offset]
        if (
            receipt.receipt_sha256 != expected_receipt_sha
            or receipt.request_sha256 != expected_request
            or (attempt is not None and receipt.attempt_id != attempt)
            or (request is not None and receipt.request_sha256 != request)
        ):
            raise GroundFusionShadowArchiveError(
                "archive lookup identity differs from sealed receipt"
            )
        return receipt

    def audit_archive(
        self,
        archive_directory: str | Path,
        *,
        policy: GroundFusionShadowRetentionPolicy,
    ) -> dict[str, Any]:
        """Verify the complete bounded archive chain and exact hot boundary."""

        self._require_initialized()
        if not isinstance(policy, GroundFusionShadowRetentionPolicy):
            raise TypeError("policy must be GroundFusionShadowRetentionPolicy")
        archive_path = Path(archive_directory)
        if archive_path.is_symlink() or not archive_path.is_dir():
            raise GroundFusionShadowArchiveError("archive directory is unavailable")
        entries = self._bounded_archive_entries(
            archive_path,
            policy.max_archive_segments,
        )
        if any(
            item.is_symlink()
            or not item.is_file()
            or item.name.startswith(".")
            or not item.name.endswith(".sealed.json")
            for item in entries
        ):
            raise GroundFusionShadowArchiveError(
                "archive directory contains partial or unsafe entries"
            )
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database)
            anchor = self._read_archive_anchor(database)
            if anchor.segment_count:
                if (
                    anchor.archive_namespace != policy.archive_namespace
                    or anchor.retention_policy_sha256 != policy.fingerprint
                ):
                    raise GroundFusionShadowConflict(
                        "archive audit policy differs from durable anchor"
                    )
            segments = database.execute(
                "SELECT * FROM archive_segments ORDER BY first_sequence"
            ).fetchall()
            archived_count = int(
                database.execute(
                    "SELECT count(*) FROM archive_receipts"
                ).fetchone()[0]
            )
            hot = database.execute(
                "SELECT count(*),min(sequence),max(sequence) FROM receipts"
            ).fetchone()
            hot_count = int(hot[0])
            hot_min = None if hot[1] is None else int(hot[1])
            hot_max = None if hot[2] is None else int(hot[2])
            if (
                len(segments) != anchor.segment_count
                or archived_count != anchor.archived_receipt_count
                or len(segments) > policy.max_archive_segments
                or archived_count > policy.max_archived_receipts
            ):
                raise GroundFusionShadowArchiveError("archive anchor counts differ")
            if hot_count:
                if (
                    hot_min != anchor.last_archived_sequence + 1
                    or hot_max != anchor.next_sequence - 1
                    or hot_count != hot_max - hot_min + 1
                ):
                    raise GroundFusionShadowArchiveError(
                        "hot/archive sequence boundary has a gap"
                    )
            elif anchor.next_sequence - 1 != anchor.last_archived_sequence:
                raise GroundFusionShadowArchiveError(
                    "empty hot window has an unexplained sequence gap"
                )
            manifest_filenames = {str(row["filename"]) for row in segments}
            if {item.name for item in entries} != manifest_filenames:
                raise GroundFusionShadowArchiveError(
                    "archive files and durable manifest set differ"
                )
            previous: str | None = None
            expected_sequence = 1
            checked_receipts = 0
            checked_bytes = 0
            last_bundle: _ArchiveSegmentBundle | None = None
            for row in segments:
                bundle = self._bundle_from_segment_row(archive_path, row, policy)
                if (
                    bundle.previous_segment_id != previous
                    or bundle.first_sequence != expected_sequence
                ):
                    raise GroundFusionShadowArchiveError(
                        "archive segment chain has a gap or fork"
                    )
                indexes = database.execute(
                    "SELECT * FROM archive_receipts WHERE segment_id=? "
                    "ORDER BY sequence",
                    (bundle.segment_id,),
                ).fetchall()
                if len(indexes) != bundle.receipt_count:
                    raise GroundFusionShadowArchiveError(
                        "archive segment index count differs"
                    )
                for offset, (index, receipt) in enumerate(
                    zip(indexes, bundle.receipts, strict=True)
                ):
                    if (
                        int(index["sequence"])
                        != bundle.first_sequence + offset
                        or str(index["receipt_sha256"])
                        != receipt.receipt_sha256
                        or str(index["request_sha256"])
                        != receipt.request_sha256
                        or str(index["attempt_id_sha256"])
                        != self._attempt_archive_sha256(receipt.attempt_id)
                    ):
                        raise GroundFusionShadowArchiveError(
                            "archive lookup index differs from sealed receipt"
                        )
                previous = bundle.segment_id
                expected_sequence = bundle.last_sequence + 1
                checked_receipts += bundle.receipt_count
                checked_bytes += len(bundle.body)
                last_bundle = bundle
            if (
                checked_receipts != anchor.archived_receipt_count
                or checked_bytes != anchor.archive_bytes
                or checked_bytes > policy.max_total_archive_bytes
                or (
                    last_bundle is None
                    and (anchor.last_segment_id is not None or anchor.archive_bytes)
                )
                or (
                    last_bundle is not None
                    and (
                        last_bundle.segment_id != anchor.last_segment_id
                        or last_bundle.file_sha256
                        != anchor.last_segment_file_sha256
                        or last_bundle.last_sequence
                        != anchor.last_archived_sequence
                    )
                )
            ):
                raise GroundFusionShadowArchiveError(
                    "archive chain head differs from durable anchor"
                )
        result = {
            "archive_anchor_sha256": anchor.anchor_sha256,
            "archive_bytes": anchor.archive_bytes,
            "archived_receipts": anchor.archived_receipt_count,
            "healthy": True,
            "hot_receipts": hot_count,
            "last_archived_sequence": anchor.last_archived_sequence,
            "segment_count": anchor.segment_count,
        }
        if len(_canonical_json(result)) > MAX_DIAGNOSTIC_BYTES:
            raise GroundFusionShadowArchiveError("archive audit output exceeds bound")
        return result

    def diagnostics(self) -> dict[str, Any]:
        """Return only bounded aggregate state; no target/site/path/secret data."""
        self._require_initialized()
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database)
            states = {item.value: 0 for item in GroundConfirmationState}
            receipt_count = 0
            for row in database.execute(
                "SELECT * FROM receipts ORDER BY sequence"
            ):
                receipt = self._decode_row(row)
                states[receipt.candidate_state.value] += 1
                receipt_count += 1
            anchor = self._read_archive_anchor(database)
        result = {
            "admission_status": GROUND_FUSION_SHADOW_ADMISSION_STATUS,
            "archive_anchor_sha256": anchor.anchor_sha256,
            "archive_bytes": anchor.archive_bytes,
            "archive_segment_count": anchor.segment_count,
            "archived_receipt_count": anchor.archived_receipt_count,
            "candidate_states": states,
            "database_bytes": self._database_bytes(),
            "last_archived_sequence": anchor.last_archived_sequence,
            "max_database_bytes": self._limits.max_database_bytes,
            "max_receipts": self._limits.max_receipts,
            "next_sequence": anchor.next_sequence,
            "operational_confirmation_claim": False,
            "receipt_count": receipt_count,
        }
        if len(_canonical_json(result)) > MAX_DIAGNOSTIC_BYTES:
            raise GroundFusionShadowStoreError("ground diagnostics exceed byte bound")
        return result


def _safe_persisted_id(value: object, namespace: str) -> str:
    if isinstance(value, str) and _SAFE_ID_RE.fullmatch(value) is not None:
        return value
    encoded = repr(value)
    return f"invalid-{namespace}-{_hash_identifier(namespace, encoded)[:32]}"


def _safe_persisted_sha(value: object, namespace: str) -> str:
    if isinstance(value, str) and _SHA_RE.fullmatch(value) is not None:
        return value
    return _hash_identifier(namespace, repr(value))


def _station_hash(provider: object, station: object) -> str:
    return _digest({
        "provider": provider if isinstance(provider, str) else repr(provider),
        "station": station if isinstance(station, str) else repr(station),
    })


def _batch_sources(
    batch: CanonicalGroundBridgeBatch,
) -> tuple[AdaptedCanonicalGroundFact | GroundBridgeIssue, ...]:
    sources: dict[int, AdaptedCanonicalGroundFact | GroundBridgeIssue] = {}
    for item in (*batch.facts, *batch.issues):
        if item.source_index in sources:
            raise GroundFusionShadowError("bridge returned duplicate source accounting")
        sources[item.source_index] = item
    if tuple(sorted(sources)) != tuple(range(batch.source_fact_count)):
        raise GroundFusionShadowError("bridge omitted source fact accounting")
    return tuple(sources[index] for index in range(batch.source_fact_count))


def _adapted_source_index(
    batch: CanonicalGroundBridgeBatch,
) -> dict[int, AdaptedCanonicalGroundFact]:
    """Retain provenance when a later guard withholds an adapted fact."""

    return {item.source_index: item for item in batch.facts}


def _overflow_dispositions(
    batch: CanonicalGroundBridgeBatch,
    reason: str,
    adapted_sources: Mapping[int, AdaptedCanonicalGroundFact] | None = None,
) -> tuple[GroundFactDisposition, ...]:
    reason_code = _issue(reason)
    result: list[GroundFactDisposition] = []
    retained = {} if adapted_sources is None else adapted_sources
    for index, item in enumerate(_batch_sources(batch)):
        provenance = (
            item
            if isinstance(item, AdaptedCanonicalGroundFact)
            else retained.get(index)
        )
        if provenance is not None:
            provider = provenance.observation.provider_id
            station = provenance.observation.station_id
            observation_id = provenance.observation.observation_id
            physical_site_id = provenance.physical_site_id
            certificate = provenance.certificate_fingerprint
            mapping = provenance.mapping_fingerprint
            semantics = provenance.observation.measurement_semantics.value
            amount = provenance.observation.amount
            unit = provenance.observation.measurement_unit
        else:
            provider = item.provider
            station = item.provider_station_id
            observation_id = None
            physical_site_id = None
            certificate = None
            mapping = None
            semantics = None
            amount = None
            unit = None
        result.append(GroundFactDisposition(
            source_index=index,
            fact_id=_safe_persisted_id(item.fact_id, "fact-id"),
            fact_sha256=_safe_persisted_sha(item.fact_sha256, "fact-sha"),
            receipt_sha256=_safe_persisted_sha(
                item.receipt_sha256,
                "receipt-sha",
            ),
            provider=_safe_persisted_id(provider, "provider"),
            station_identity_sha256=_station_hash(provider, station),
            state=GroundFactDispositionState.OVERFLOW,
            reason_codes=(reason_code,),
            observation_id=(
                None
                if observation_id is None
                else _safe_persisted_id(observation_id, "observation-id")
            ),
            physical_site_id=(
                None
                if physical_site_id is None
                else _safe_persisted_id(physical_site_id, "physical-site")
            ),
            certificate_fingerprint=certificate,
            mapping_fingerprint=mapping,
            measurement_semantics=semantics,
            measurement_amount=amount,
            measurement_unit=unit,
        ))
    return tuple(result)


def _decision_dispositions(
    batch: CanonicalGroundBridgeBatch,
    decision: GroundConfirmationDecision,
    dry_capability_fingerprints: Mapping[int, str],
    adapted_sources: Mapping[int, AdaptedCanonicalGroundFact],
) -> tuple[GroundFactDisposition, ...]:
    assessments = {item.observation_id: item for item in decision.assessments}
    contributions = {
        item.observation_id: item for item in decision.contributions
    }
    result: dict[int, GroundFactDisposition] = {}
    for item in batch.issues:
        original = adapted_sources.get(item.source_index)
        if item.state is GroundBridgeIssueState.DUPLICATE_INPUT:
            state = GroundFactDispositionState.DUPLICATE
            terminal_reason = "bridge_duplicate_input"
        elif item.state is GroundBridgeIssueState.WITHHELD:
            state = GroundFactDispositionState.WITHHELD
            terminal_reason = "bridge_evidence_withheld"
        else:
            state = GroundFactDispositionState.INDETERMINATE
            terminal_reason = "bridge_evidence_invalid"
        observation = None if original is None else original.observation
        result[item.source_index] = GroundFactDisposition(
            source_index=item.source_index,
            fact_id=_safe_persisted_id(item.fact_id, "fact-id"),
            fact_sha256=_safe_persisted_sha(item.fact_sha256, "fact-sha"),
            receipt_sha256=_safe_persisted_sha(
                item.receipt_sha256,
                "receipt-sha",
            ),
            provider=_safe_persisted_id(item.provider, "provider"),
            station_identity_sha256=_station_hash(
                item.provider,
                item.provider_station_id,
            ),
            state=state,
            reason_codes=tuple(sorted({
                _issue(item.code),
                terminal_reason,
            })),
            observation_id=(
                None
                if observation is None
                else _safe_persisted_id(
                    observation.observation_id,
                    "observation-id",
                )
            ),
            physical_site_id=(
                None if original is None else original.physical_site_id
            ),
            certificate_fingerprint=(
                None if original is None else original.certificate_fingerprint
            ),
            mapping_fingerprint=(
                None if original is None else original.mapping_fingerprint
            ),
            measurement_semantics=(
                None
                if observation is None
                else observation.measurement_semantics.value
            ),
            measurement_amount=(
                None if observation is None else observation.amount
            ),
            measurement_unit=(
                None if observation is None else observation.measurement_unit
            ),
        )
    outside = {
        GroundMatchOutcome.OUTSIDE_CELL_TIME,
        GroundMatchOutcome.INSUFFICIENT_INTERVAL_OVERLAP,
        GroundMatchOutcome.OUTSIDE_CELL_FOOTPRINT,
    }
    stale = {
        GroundMatchOutcome.OBSERVATION_FROM_FUTURE,
        GroundMatchOutcome.OBSERVATION_STALE,
    }
    for item in batch.facts:
        observation = item.observation
        try:
            assessment = assessments[observation.observation_id]
            contribution = contributions[observation.observation_id]
        except KeyError as exc:
            raise GroundFusionShadowError(
                "fusion decision omitted an adapted observation"
            ) from exc
        if contribution.role in {
            GroundEvidenceRole.INDEPENDENT_WET,
            GroundEvidenceRole.INDEPENDENT_DRY,
        }:
            state = GroundFactDispositionState.USED
        elif contribution.role is GroundEvidenceRole.DUPLICATE_INDEPENDENCE_GROUP:
            state = GroundFactDispositionState.DUPLICATE
        elif contribution.role is GroundEvidenceRole.CONFLICTED_INDEPENDENCE_GROUP:
            state = GroundFactDispositionState.INDETERMINATE
        elif assessment.outcome in outside:
            state = GroundFactDispositionState.OUTSIDE
        elif assessment.outcome in stale:
            state = GroundFactDispositionState.STALE
        else:
            state = GroundFactDispositionState.INDETERMINATE
        result[item.source_index] = GroundFactDisposition(
            source_index=item.source_index,
            fact_id=item.fact_id,
            fact_sha256=item.fact_sha256,
            receipt_sha256=item.receipt_sha256,
            provider=_safe_persisted_id(observation.provider_id, "provider"),
            station_identity_sha256=_station_hash(
                observation.provider_id,
                observation.station_id,
            ),
            state=state,
            reason_codes=tuple(sorted({
                _issue(assessment.outcome.value),
                _issue(contribution.reason),
            })),
            observation_id=_safe_persisted_id(
                observation.observation_id,
                "observation-id",
            ),
            physical_site_id=assessment.physical_site_id,
            independence_group=assessment.independence_group,
            certificate_fingerprint=item.certificate_fingerprint,
            mapping_fingerprint=item.mapping_fingerprint,
            dry_capability_fingerprint=dry_capability_fingerprints.get(
                item.source_index
            ),
            measurement_semantics=observation.measurement_semantics.value,
            measurement_amount=observation.amount,
            measurement_unit=observation.measurement_unit,
            supports_precipitation=assessment.supports_precipitation,
            temporal_overlap_fraction=assessment.temporal_overlap_fraction,
            distance_to_footprint_km=assessment.distance_to_footprint_km,
        )
    if tuple(sorted(result)) != tuple(range(batch.source_fact_count)):
        raise GroundFusionShadowError("final ground accounting omitted a source fact")
    return tuple(result[index] for index in range(batch.source_fact_count))


def _accounting_bytes(dispositions: Sequence[GroundFactDisposition]) -> int:
    return len(_canonical_json([item.to_mapping() for item in dispositions]))


@dataclass(frozen=True, slots=True)
class GroundFusionShadowRunResult:
    receipt: GroundFusionShadowReceipt
    duplicate: bool
    decision: GroundConfirmationDecision | None

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, GroundFusionShadowReceipt):
            raise TypeError("run result receipt has the wrong type")
        if type(self.duplicate) is not bool:
            raise TypeError("run result duplicate flag has the wrong type")
        if self.duplicate and self.decision is not None:
            raise ValueError("durable duplicate replay cannot expose recomputed decision")
        if self.decision is not None and not isinstance(
            self.decision,
            GroundConfirmationDecision,
        ):
            raise TypeError("run result decision has the wrong type")


class DurableGroundFusionShadowRuntime:
    """Serialize one exact target/cell ground evaluation into durable shadow."""

    def __init__(
        self,
        evidence_store: CanonicalGroundEvidenceStore,
        *,
        site_catalog: PhysicalSiteCatalog,
        semantics_certificates: tuple[GroundSemanticsCertificate, ...],
        fusion_policy: GroundFusionPolicy,
        receipt_path: str | Path,
        limits: GroundFusionShadowLimits = GroundFusionShadowLimits(),
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if type(evidence_store) is not CanonicalGroundEvidenceStore:
            raise TypeError(
                "evidence_store must be concrete CanonicalGroundEvidenceStore"
            )
        if not isinstance(site_catalog, PhysicalSiteCatalog):
            raise TypeError("site_catalog must be PhysicalSiteCatalog")
        if not isinstance(semantics_certificates, tuple) or any(
            not isinstance(item, GroundSemanticsCertificate)
            for item in semantics_certificates
        ):
            raise TypeError("semantics_certificates must be an immutable typed tuple")
        if not semantics_certificates:
            raise ValueError("at least one exact ground semantics certificate is required")
        fingerprints = tuple(sorted(item.fingerprint for item in semantics_certificates))
        if len(fingerprints) != len(set(fingerprints)):
            raise ValueError("ground semantics certificates contain duplicates")
        if not isinstance(fusion_policy, GroundFusionPolicy):
            raise TypeError("fusion_policy must be GroundFusionPolicy")
        if not isinstance(limits, GroundFusionShadowLimits):
            raise TypeError("limits must be GroundFusionShadowLimits")
        if len(site_catalog.records) > limits.max_sites:
            raise GroundFusionShadowCapacityError("physical-site catalog exceeds site bound")
        alias_count = sum(len(item.aliases) for item in site_catalog.records)
        if alias_count > limits.max_aliases:
            raise GroundFusionShadowCapacityError("physical-site catalog exceeds alias bound")
        maximum_freshness = max(
            float(seconds)
            for _provider, seconds in fusion_policy.freshness_seconds_by_provider
        )
        if (
            maximum_freshness + fusion_policy.temporal_tolerance_seconds
            > limits.max_query_lookback_seconds
        ):
            raise ValueError(
                "query lookback must cover every configured provider freshness window"
            )
        self._evidence_store = evidence_store
        self._catalog = site_catalog
        self._certificates = semantics_certificates
        self._certificate_fingerprints = fingerprints
        self._certificate_by_fingerprint = {
            item.fingerprint: item for item in semantics_certificates
        }
        self._catalog_mapping_index: dict[
            tuple[str, str, str, str],
            tuple[Any, Any],
        ] = {}
        self._capability_index: dict[
            tuple[str, str, str, str],
            tuple[Any, ...],
        ] = {}
        mutable_capabilities: dict[
            tuple[str, str, str, str],
            list[Any],
        ] = {}
        for record in site_catalog.records:
            for alias in record.aliases:
                self._catalog_mapping_index[
                    (
                        record.physical_site_id,
                        alias.provider_id.casefold(),
                        alias.station_id.casefold(),
                        alias.fingerprint,
                    )
                ] = (record, alias)
            for capability in record.capabilities:
                mutable_capabilities.setdefault(
                    (
                        record.physical_site_id,
                        capability.provider_id.casefold(),
                        capability.sensor_id.casefold(),
                        capability.measurement_kind.casefold(),
                    ),
                    [],
                ).append(capability)
        self._capability_index = {
            key: tuple(values) for key, values in mutable_capabilities.items()
        }
        self._policy = fusion_policy
        self._policy_fingerprint = _digest(_policy_mapping(fusion_policy))
        self._limits = limits
        self._monotonic = monotonic
        self._maximum_freshness = maximum_freshness
        self._runtime_profile_sha256 = _digest({
            "algorithm": GROUND_FUSION_SHADOW_ALGORITHM,
            "catalog_fingerprint": site_catalog.fingerprint,
            "ground_semantics_certificate_fingerprints": list(fingerprints),
            "limits_sha256": limits.fingerprint,
            "policy_sha256": self._policy_fingerprint,
        })
        self._store = GroundFusionShadowReceiptStore(
            receipt_path,
            runtime_profile_sha256=self._runtime_profile_sha256,
            limits=limits,
            monotonic=monotonic,
        )
        self._store.initialize()
        self._lock = threading.RLock()

    @property
    def runtime_profile_sha256(self) -> str:
        return self._runtime_profile_sha256

    @property
    def receipt_store(self) -> GroundFusionShadowReceiptStore:
        return self._store

    def _deadline(self, value: float) -> float:
        deadline = _finite(value, "deadline_at")
        now = _finite(self._monotonic(), "monotonic clock")
        if deadline <= now:
            raise GroundFusionShadowDeadlineError("ground fusion shadow deadline elapsed")
        if deadline - now > self._limits.max_runtime_seconds:
            raise ValueError("deadline exceeds the configured runtime boundary")
        return deadline

    def _request_sha256(self, request: GroundFusionShadowRequest) -> str:
        return _digest({
            "algorithm": GROUND_FUSION_SHADOW_ALGORITHM,
            "cell_binding_sha256": request.cell.fingerprint,
            "decision_key_sha256": request.decision_key_sha256,
            "evaluated_at": request.evaluated_at,
            "region_key_sha256": request.region_key_sha256,
            "runtime_profile_sha256": self._runtime_profile_sha256,
            "target_snapshot_sha256": request.target.fingerprint,
        })

    def _registry(self, request: GroundFusionShadowRequest) -> PhysicalSiteRegistry:
        effective = request.cell.footprint.representative_at
        registry = self._catalog.to_ground_fusion_registry(effective_at=effective)
        if len(registry.sites) > self._limits.max_sites:
            raise GroundFusionShadowCapacityError("active site registry exceeds site bound")
        aliases = sum(len(item.aliases) for item in registry.sites)
        if aliases > self._limits.max_aliases:
            raise GroundFusionShadowCapacityError("active site registry exceeds alias bound")
        if registry.catalog_fingerprint != self._catalog.fingerprint:
            raise GroundFusionShadowError("site registry lost catalog provenance")
        return registry

    @staticmethod
    def _interval_within_active_mapping(
        start: float,
        end: float,
        valid_from: float | None,
        valid_until: float | None,
    ) -> bool:
        return (
            valid_from is None or start >= valid_from
        ) and (
            valid_until is None or end < valid_until
        )

    def _mapping_time_guard(
        self,
        batch: CanonicalGroundBridgeBatch,
        *,
        deadline_at: float,
    ) -> tuple[CanonicalGroundBridgeBatch, dict[int, str]]:
        """Withhold invalid alias intervals and unproven dry capability."""

        retained: list[AdaptedCanonicalGroundFact] = []
        issues = list(batch.issues)
        dry_capability_fingerprints: dict[int, str] = {}
        for ordinal, item in enumerate(batch.facts):
            if ordinal % 64 == 0:
                _check_deadline(deadline_at, self._monotonic)
            observation = item.observation
            mapping = self._catalog_mapping_index.get((
                item.physical_site_id,
                observation.provider_id.casefold(),
                observation.station_id.casefold(),
                item.mapping_fingerprint,
            ))
            exact_record = None if mapping is None else mapping[0]
            exact_mapping = None if mapping is None else mapping[1]
            active = (
                exact_record is not None
                and exact_mapping is not None
                and self._interval_within_active_mapping(
                    observation.interval_start_at,
                    observation.interval_end_at,
                    exact_record.valid_from,
                    exact_record.valid_until,
                )
                and self._interval_within_active_mapping(
                    observation.interval_start_at,
                    observation.interval_end_at,
                    exact_mapping.valid_from,
                    exact_mapping.valid_until,
                )
            )
            code = None
            if not active:
                code = "physical_site_mapping_inactive_for_observation_interval"
            elif observation.precipitation_state.value == "absent":
                certificate = self._certificate_by_fingerprint.get(
                    item.certificate_fingerprint
                )
                if certificate is None:
                    raise GroundFusionShadowError(
                        "adapted fact references an unknown semantics certificate"
                    )
                capability_key = (
                    item.physical_site_id,
                    observation.provider_id.casefold(),
                    observation.station_id.casefold(),
                    certificate.profile.value.casefold(),
                )
                eligible = tuple(
                    capability
                    for capability in self._capability_index.get(
                        capability_key,
                        (),
                    )
                    if capability.can_observe_absence
                    and self._interval_within_active_mapping(
                        observation.interval_start_at,
                        observation.interval_end_at,
                        capability.valid_from,
                        capability.valid_until,
                    )
                )
                if len(eligible) != 1:
                    code = "dry_absence_capability_unverified"
                else:
                    capability = eligible[0]
                    dry_capability_fingerprints[item.source_index] = _digest({
                        "can_observe_absence": capability.can_observe_absence,
                        "evidence_fingerprint": capability.evidence.fingerprint,
                        "measurement_kind": capability.measurement_kind,
                        "physical_site_id": item.physical_site_id,
                        "provider_id": capability.provider_id,
                        "sensor_id": capability.sensor_id,
                        "valid_from": capability.valid_from,
                        "valid_until": capability.valid_until,
                    })
            if code is None:
                retained.append(item)
            else:
                issues.append(GroundBridgeIssue(
                    source_index=item.source_index,
                    fact_id=item.fact_id,
                    fact_sha256=item.fact_sha256,
                    receipt_sha256=item.receipt_sha256,
                    provider=observation.provider_id,
                    provider_station_id=observation.station_id,
                    state=GroundBridgeIssueState.WITHHELD,
                    code=code,
                    detail="site identity or dry-observation capability failed closed",
                ))
        guarded = CanonicalGroundBridgeBatch(
            source_fact_count=batch.source_fact_count,
            facts=tuple(sorted(retained, key=lambda item: item.source_index)),
            issues=tuple(sorted(issues, key=lambda item: item.source_index)),
            registry_fingerprint=batch.registry_fingerprint,
        )
        return guarded, dry_capability_fingerprints

    def _context_reasons(
        self,
        request: GroundFusionShadowRequest,
        target_distance: float | None,
    ) -> tuple[str, ...]:
        reasons: set[str] = set()
        target_age = request.evaluated_at - request.target.observed_at
        if target_age > self._limits.max_target_age_seconds:
            reasons.add("target_context_stale")
        if target_age < -self._limits.max_target_future_skew_seconds:
            reasons.add("target_context_from_future")
        footprint = request.cell.footprint
        if footprint.valid_until - footprint.valid_from > (
            self._limits.max_footprint_window_seconds
        ):
            reasons.add("cell_footprint_window_exceeds_bound")
        if request.evaluated_at - footprint.valid_until > self._limits.max_cell_age_seconds:
            reasons.add("cell_footprint_stale")
        if (
            footprint.valid_from
            > request.evaluated_at + self._policy.maximum_future_skew_seconds
        ):
            reasons.add("cell_footprint_from_future")
        if target_distance is None:
            reasons.add("target_cell_geometry_indeterminate")
        elif target_distance > self._limits.maximum_target_to_footprint_km:
            reasons.add("target_outside_cell_relevance_bound")
        return tuple(sorted(reasons))

    def _empty_accounting(self) -> GroundFusionShadowAccounting:
        return GroundFusionShadowAccounting(
            query_executed=False,
            full_scan_complete=False,
            overflowed=False,
            source_fact_count=0,
            unscanned_fact_count_lower_bound=0,
            encoded_bytes=2,
            dispositions=(),
        )

    def _receipt(
        self,
        request: GroundFusionShadowRequest,
        *,
        request_sha256: str,
        registry: PhysicalSiteRegistry,
        target_distance: float | None,
        accounting: GroundFusionShadowAccounting,
        state: GroundConfirmationState,
        reason_codes: tuple[str, ...],
        decision: GroundConfirmationDecision | None,
        query_from: float | None,
        query_through: float | None,
        bridge_fingerprint: str | None,
    ) -> GroundFusionShadowReceipt:
        cell = request.cell
        return _build_receipt(
            attempt_id=request.attempt_id,
            request_sha256=request_sha256,
            runtime_profile_sha256=self._runtime_profile_sha256,
            decision_key_sha256=request.decision_key_sha256,
            region_key_sha256=request.region_key_sha256,
            target_id_sha256=request.target.target_id_sha256,
            target_snapshot_sha256=request.target.fingerprint,
            cell_binding_sha256=cell.fingerprint,
            engine_id=cell.engine_id,
            engine_state_revision=cell.engine_state_revision,
            track_revision=cell.track_revision,
            analysis_grid_id=cell.analysis_grid_id,
            cell_track_id=cell.footprint.cell_track_id,
            cell_detection_id=cell.footprint.cell_detection_id,
            footprint_id=cell.footprint.footprint_id,
            footprint_geometry_sha256=footprint_geometry_sha256(cell.footprint),
            footprint_proof_sha256=cell.footprint_proof.fingerprint,
            footprint_proof_id=cell.footprint_proof.proof_id,
            footprint_proof_version=cell.footprint_proof.proof_version,
            geometry_builder_id=cell.footprint_proof.geometry_builder_id,
            geometry_certificate_sha256=(
                cell.footprint_proof.geometry_certificate_sha256
            ),
            source_transform_receipt_sha256=(
                cell.footprint_proof.source_transform_receipt_sha256
            ),
            science_run_id=cell.science_run_id,
            science_transition_id=cell.science_transition_id,
            source_frame_id=cell.source_frame_id,
            science_receipt_sha256=cell.science_receipt_sha256,
            target_grid_sha256=cell.target_grid_sha256,
            radar_semantics_certificate_sha256=(
                cell.radar_semantics_certificate_sha256
            ),
            transform_certificate_sha256=cell.transform_certificate_sha256,
            parent_track_ids=cell.parent_track_ids,
            child_track_ids=cell.child_track_ids,
            lineage_event_ids=cell.lineage_event_ids,
            applied_lineage_hypothesis_ids=(
                cell.applied_lineage_hypothesis_ids
            ),
            ground_semantics_certificate_fingerprints=(
                self._certificate_fingerprints
            ),
            registry_version=registry.registry_version,
            registry_fingerprint=registry.registry_fingerprint,
            catalog_fingerprint=self._catalog.fingerprint,
            policy_fingerprint=self._policy_fingerprint,
            evaluated_at=request.evaluated_at,
            registry_effective_at=cell.footprint.representative_at,
            query_observed_from=query_from,
            query_observed_through=query_through,
            target_distance_to_footprint_km=target_distance,
            bridge_fingerprint=bridge_fingerprint,
            candidate_state=state,
            candidate_confirmed=state is GroundConfirmationState.CONFIRMED,
            reason_codes=tuple(sorted(set(reason_codes))),
            independent_wet_groups=(
                () if decision is None else decision.independent_wet_groups
            ),
            independent_dry_groups=(
                () if decision is None else decision.independent_dry_groups
            ),
            conflicted_groups=(
                () if decision is None else decision.conflicted_groups
            ),
            accounting=accounting,
        )

    def _persist_result(
        self,
        receipt: GroundFusionShadowReceipt,
        decision: GroundConfirmationDecision | None,
        deadline_at: float,
    ) -> GroundFusionShadowRunResult:
        recorded, duplicate = self._store.record_with_status(
            receipt,
            deadline_at=deadline_at,
        )
        return GroundFusionShadowRunResult(
            receipt=recorded,
            duplicate=duplicate,
            decision=None if duplicate else decision,
        )

    def process(
        self,
        request: GroundFusionShadowRequest,
        *,
        deadline_at: float,
    ) -> GroundFusionShadowRunResult:
        """Evaluate one immutable target/cell request under one bounded scan."""

        if not isinstance(request, GroundFusionShadowRequest):
            raise TypeError("request must be GroundFusionShadowRequest")
        deadline = self._deadline(deadline_at)
        request_sha = self._request_sha256(request)
        with self._lock:
            existing = self._store.load_existing(
                attempt_id=request.attempt_id,
                request_sha256=request_sha,
                deadline_at=deadline,
            )
            if existing is not None:
                return GroundFusionShadowRunResult(existing, True, None)
            _check_deadline(deadline, self._monotonic)
            registry = self._registry(request)
            _check_deadline(deadline, self._monotonic)
            target_distance = point_to_polygon_distance_km(
                request.target.latitude,
                request.target.longitude,
                request.cell.footprint.geometry,
            )
            context_reasons = self._context_reasons(request, target_distance)
            if context_reasons:
                receipt = self._receipt(
                    request,
                    request_sha256=request_sha,
                    registry=registry,
                    target_distance=target_distance,
                    accounting=self._empty_accounting(),
                    state=GroundConfirmationState.INDETERMINATE,
                    reason_codes=context_reasons,
                    decision=None,
                    query_from=None,
                    query_through=None,
                    bridge_fingerprint=None,
                )
                return self._persist_result(receipt, None, deadline)

            footprint = request.cell.footprint
            query_from = max(0.0, min(
                request.evaluated_at - self._maximum_freshness,
                footprint.valid_from - self._policy.temporal_tolerance_seconds,
            ))
            query_through = (
                request.evaluated_at + self._policy.maximum_future_skew_seconds
            )
            if request.evaluated_at - query_from > (
                self._limits.max_query_lookback_seconds
            ):
                receipt = self._receipt(
                    request,
                    request_sha256=request_sha,
                    registry=registry,
                    target_distance=target_distance,
                    accounting=self._empty_accounting(),
                    state=GroundConfirmationState.INDETERMINATE,
                    reason_codes=("ground_query_window_exceeds_bound",),
                    decision=None,
                    query_from=None,
                    query_through=None,
                    bridge_fingerprint=None,
                )
                return self._persist_result(receipt, None, deadline)
            try:
                batch = query_and_adapt_canonical_ground(
                    self._evidence_store,
                    certificates=self._certificates,
                    site_registry=registry,
                    observed_from=query_from,
                    observed_through=query_through,
                    limit=self._limits.max_source_facts + 1,
                    deadline_at=deadline,
                    monotonic=self._monotonic,
                )
            except (
                CanonicalGroundEvidenceDeadlineError,
                GroundBridgeDeadlineError,
            ) as exc:
                raise GroundFusionShadowDeadlineError(
                    "ground source query deadline elapsed"
                ) from exc
            except (
                CanonicalGroundEvidenceCapacityError,
                GroundBridgeCapacityError,
            ):
                receipt = self._receipt(
                    request,
                    request_sha256=request_sha,
                    registry=registry,
                    target_distance=target_distance,
                    accounting=self._empty_accounting(),
                    state=GroundConfirmationState.INDETERMINATE,
                    reason_codes=("ground_source_capacity_exceeded",),
                    decision=None,
                    query_from=None,
                    query_through=None,
                    bridge_fingerprint=None,
                )
                return self._persist_result(receipt, None, deadline)
            except CanonicalGroundEvidenceError:
                receipt = self._receipt(
                    request,
                    request_sha256=request_sha,
                    registry=registry,
                    target_distance=target_distance,
                    accounting=self._empty_accounting(),
                    state=GroundConfirmationState.INDETERMINATE,
                    reason_codes=("ground_source_integrity_failed",),
                    decision=None,
                    query_from=None,
                    query_through=None,
                    bridge_fingerprint=None,
                )
                return self._persist_result(receipt, None, deadline)
            _check_deadline(deadline, self._monotonic)
            if not isinstance(batch, CanonicalGroundBridgeBatch):
                raise TypeError("canonical ground bridge returned the wrong contract")
            if batch.registry_fingerprint != registry.registry_fingerprint:
                raise GroundFusionShadowError("bridge registry fingerprint differs")
            _batch_sources(batch)
            adapted_sources = _adapted_source_index(batch)
            source_bridge_fingerprint = batch.fingerprint
            batch, dry_capability_fingerprints = self._mapping_time_guard(
                batch,
                deadline_at=deadline,
            )
            _batch_sources(batch)

            if batch.source_fact_count > self._limits.max_source_facts:
                dispositions = _overflow_dispositions(
                    batch,
                    "ground_source_fact_capacity_exceeded",
                    adapted_sources,
                )
                encoded = _accounting_bytes(dispositions)
                if encoded > self._limits.max_accounting_bytes:
                    raise GroundFusionShadowCapacityError(
                        "overflow accounting itself exceeds the durable byte bound"
                    )
                accounting = GroundFusionShadowAccounting(
                    query_executed=True,
                    full_scan_complete=False,
                    overflowed=True,
                    source_fact_count=batch.source_fact_count,
                    unscanned_fact_count_lower_bound=(
                        batch.source_fact_count - self._limits.max_source_facts
                    ),
                    encoded_bytes=encoded,
                    dispositions=dispositions,
                )
                receipt = self._receipt(
                    request,
                    request_sha256=request_sha,
                    registry=registry,
                    target_distance=target_distance,
                    accounting=accounting,
                    state=GroundConfirmationState.INDETERMINATE,
                    reason_codes=("ground_source_fact_capacity_exceeded",),
                    decision=None,
                    query_from=query_from,
                    query_through=query_through,
                    bridge_fingerprint=source_bridge_fingerprint,
                )
                return self._persist_result(receipt, None, deadline)

            decision = fuse_ground_observations(
                batch.observations,
                registry,
                footprint,
                decision_key=request.decision_key,
                target_id=request.target.target_id,
                region_key=request.region_key,
                evaluated_at=request.evaluated_at,
                policy=self._policy,
            )
            _check_deadline(deadline, self._monotonic)
            dispositions = _decision_dispositions(
                batch,
                decision,
                dry_capability_fingerprints,
                adapted_sources,
            )
            encoded = _accounting_bytes(dispositions)
            if encoded > self._limits.max_accounting_bytes:
                dispositions = _overflow_dispositions(
                    batch,
                    "ground_accounting_byte_capacity_exceeded",
                    adapted_sources,
                )
                encoded = _accounting_bytes(dispositions)
                if encoded > self._limits.max_accounting_bytes:
                    raise GroundFusionShadowCapacityError(
                        "minimal ground accounting exceeds the durable byte bound"
                    )
                accounting = GroundFusionShadowAccounting(
                    query_executed=True,
                    full_scan_complete=False,
                    overflowed=True,
                    source_fact_count=batch.source_fact_count,
                    unscanned_fact_count_lower_bound=0,
                    encoded_bytes=encoded,
                    dispositions=dispositions,
                )
                state = GroundConfirmationState.INDETERMINATE
                reasons = ("ground_accounting_byte_capacity_exceeded",)
                durable_decision = None
            else:
                accounting = GroundFusionShadowAccounting(
                    query_executed=True,
                    full_scan_complete=True,
                    overflowed=False,
                    source_fact_count=batch.source_fact_count,
                    unscanned_fact_count_lower_bound=0,
                    encoded_bytes=encoded,
                    dispositions=dispositions,
                )
                invalid_bridge = any(
                    item.state is GroundBridgeIssueState.INVALID
                    for item in batch.issues
                )
                if invalid_bridge:
                    state = GroundConfirmationState.INDETERMINATE
                    reasons = tuple(sorted(set(
                        decision.reason_codes + ("invalid_canonical_ground_fact",)
                    )))
                elif decision.conflicted_groups:
                    state = GroundConfirmationState.INDETERMINATE
                    reasons = tuple(sorted(set(
                        decision.reason_codes + ("conflicted_independence_group",)
                    )))
                else:
                    state = decision.state
                    reasons = tuple(sorted(set(decision.reason_codes)))
                durable_decision = decision
            receipt = self._receipt(
                request,
                request_sha256=request_sha,
                registry=registry,
                target_distance=target_distance,
                accounting=accounting,
                state=state,
                reason_codes=reasons,
                decision=durable_decision,
                query_from=query_from,
                query_through=query_through,
                bridge_fingerprint=source_bridge_fingerprint,
            )
            return self._persist_result(receipt, durable_decision, deadline)


__all__ = [
    "DurableGroundFusionShadowRuntime",
    "GROUND_FUSION_SHADOW_ADMISSION_STATUS",
    "GROUND_FUSION_SHADOW_ALGORITHM",
    "GROUND_FUSION_SHADOW_ARCHIVE_CONTRACT",
    "GROUND_FUSION_SHADOW_ARCHIVE_MANIFEST_CONTRACT",
    "GROUND_FUSION_SHADOW_OPERATIONAL_BLOCKERS",
    "GROUND_FUSION_SHADOW_RECEIPT_CONTRACT",
    "GROUND_FUSION_SHADOW_ROTATION_FAULT_POINTS",
    "GeographicFootprintProof",
    "GroundCellTrackBinding",
    "GroundFactDisposition",
    "GroundFactDispositionState",
    "GroundFusionShadowAccounting",
    "GroundFusionShadowArchiveError",
    "GroundFusionShadowCapacityError",
    "GroundFusionShadowConflict",
    "GroundFusionShadowDeadlineError",
    "GroundFusionShadowDurabilityError",
    "GroundFusionShadowError",
    "GroundFusionShadowLimits",
    "GroundFusionShadowReceipt",
    "GroundFusionShadowReceiptStore",
    "GroundFusionShadowRetentionPolicy",
    "GroundFusionShadowRotationResult",
    "GroundFusionShadowRequest",
    "GroundFusionShadowRunResult",
    "GroundFusionShadowStoreError",
    "GroundFusionShadowWindowExpired",
    "GroundTargetSnapshot",
    "bind_cell_track_footprint",
    "bind_science_shadow_footprint",
    "footprint_geometry_sha256",
]
