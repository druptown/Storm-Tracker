"""Durable worker-only orchestration of the complete scientific shadow chain.

The existing scientific modules remain the sole authorities for their stages:

``canonical evidence -> native radar field -> analysis grid -> lossless cells``
``-> immutable CellTrackEngine``

This module only binds those outputs, accounts for every terminal disposition
and persists compact content-derived receipts.  It deliberately has no import
or registration path from :mod:`storm_tracker_worker.service`, Home Assistant,
MQTT or an operational snapshot builder.

The result is never operational admission.  In particular, the existing cell
compositor is not yet executed by a hard-killable child process and the track
engine has no pre-emptive deadline primitive.  Those blockers are retained in
every receipt instead of being papered over by a timeout check after the fact.

Receipts contain only identifiers, hashes, counts and explicitly selected
lineage hypotheses.  The same database additionally retains the exact bounded
normalized raster tail and a checksummed canonical-JSON CellTrack checkpoint;
it never stores raw artifacts, provider response metadata, credentials or free
error details.  On restart the immutable state is loaded from the newest
checkpoint and only its retained contiguous tail is replayed and checked
against the durable receipt/provenance chain.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence, runtime_checkable
import zlib

from .analysis_grid_transform import (
    AnalysisGridTransformConfig,
    CertifiedCoordinateTransformAdapter,
    analysis_grid_sha256,
)
from .canonical_evidence_store import CanonicalEvidenceStore
from .canonical_radar_field_bridge import (
    RadarDerivedArtifactAuthority,
    RadarFieldSemanticsCertificate,
)
from .evidence_to_cells_shadow import (
    EvidenceToCellsPixelAccounting,
    EvidenceToCellsShadowCapacityError,
    EvidenceToCellsShadowConfig,
    EvidenceToCellsShadowDeadlineError,
    EvidenceToCellsShadowIntegrityError,
    compose_evidence_to_cells_shadow,
    evidence_to_cells_content_sha256,
)
from .tracking_core.engine.analysis_grid import AnalysisGrid
from .tracking_core.engine.advected_footprint_evidence import (
    AdvectedFootprintEvidence,
)
from .tracking_core.engine.cell_track import CellDetection
from .tracking_core.engine.cell_track_engine import (
    CellTrackEnginePolicy,
    CellTrackEngineState,
    CellTrackLineageHypothesis,
    CellTrackTransitionResult,
    LineageChildIdentity,
    advance_cell_track_engine,
    create_cell_track_engine_state,
)
from .tracking_core.engine.cell_track_state_codec import (
    CellTrackStateCodecCapacityError,
    CellTrackStateCodecError,
    decode_cell_track_state_value,
    encode_cell_track_state_value,
)
from .tracking_core.engine.cell_track_lifecycle import (
    CellTrackLedger,
    CellTrackLifecycleEvent,
    ImmutableCellTrack,
)
from .tracking_core.engine.hierarchical_detection import (
    HierarchicalCellDetection,
    HierarchicalDetectionFrame,
    HierarchicalDetectorConfig,
    NormalizedRasterFrame,
)
from .tracking_core.engine.recursive_track_estimator import RecursiveTrackEstimate
from .tracking_core.engine.multi_hypothesis_identity import (
    IdentityAssignmentHypothesis,
    LocalCellDeformationCandidate,
    MultiHypothesisIdentitySnapshot,
    MultiScanBranch,
    MultiScanBranchStep,
    MultiScanIdentityPosterior,
    MultiScanLineageAlternative,
    MultiScanTrackPath,
    SystemTranslationEvidence,
)
from .tracking_core.engine.weather_system_grouping import (
    DescriptiveSystemMotion,
    WeatherSystemConnectionEvidence,
    WeatherSystemGroupingPolicy,
    WeatherSystemGroupingSnapshot,
    WeatherSystemMembershipEvent,
    WeatherSystemPredecessorCandidate,
    WeatherSystemSnapshot,
    WeatherSystemTrackInput,
)


SCIENCE_SHADOW_ORCHESTRATOR_ALGORITHM = "stv3-science-shadow-orchestrator-v2"
SCIENCE_SHADOW_RECEIPT_CONTRACT = "stv3-science-shadow-receipt-v2"
SCIENCE_SHADOW_CHECKPOINT_CONTRACT = "stv3-celltrack-state-checkpoint-v1"
SCIENCE_SHADOW_RETAINED_INPUT_CONTRACT = "stv3-celltrack-retained-input-v1"
SCIENCE_SHADOW_SCHEMA_VERSION = 2
SCIENCE_SHADOW_ADMISSION_STATUS = "shadow_candidate_unadmitted"

HARD_MAX_RECEIPTS = 8_192
HARD_MAX_DATABASE_BYTES = 128 * 1024 * 1024
HARD_MAX_RECEIPT_BYTES = 256 * 1024
HARD_MAX_REPLAY_FRAMES = 2_048
HARD_MAX_CHECKPOINT_INTERVAL_FRAMES = 2_048
HARD_MAX_CHECKPOINT_BYTES = 64 * 1024 * 1024
HARD_MAX_RETAINED_INPUT_BYTES = 64 * 1024 * 1024
HARD_MAX_EXPIRED_FILTER_BYTES = 2 * 1024 * 1024
HARD_MAX_EXPIRED_MEMBERSHIPS = 8_000_000
HARD_MAX_LINEAGE_HYPOTHESES = 512
HARD_MAX_RUNTIME_SECONDS = 120.0
MAX_DIAGNOSTIC_BYTES = 8_192
EXPIRED_FILTER_HASH_COUNT = 7

SCIENCE_SHADOW_OPERATIONAL_BLOCKERS = (
    "hard_process_isolation_not_wired",
    "operational_admission_certificates_incomplete",
    "operational_snapshot_publication_forbidden",
    "tracking_deadline_not_preemptive_without_process_isolation",
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID_RE = re.compile(r"^run-[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.:/-]{0,191}$")
_SAFE_RETAINED_SOURCE_RE = re.compile(r"^[a-z0-9][a-z0-9_.:/-]{0,511}$")
_SECRET_LABEL_RE = re.compile(
    r"(?:authorization|api[_-]?key|access[_-]?token|secret|password)",
    re.IGNORECASE,
)
_ISSUE_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")


class ScienceShadowError(RuntimeError):
    """Base error for the durable shadow orchestration boundary."""


class ScienceShadowStoreError(ScienceShadowError):
    """The durable receipt authority is corrupt or incompatible."""


class ScienceShadowStoreConflict(ScienceShadowStoreError):
    """An idempotency key, binding or concurrent state head conflicts."""


class ScienceShadowWindowExpired(ScienceShadowStoreError):
    """An exact retry identity is known but its durable receipt was pruned."""

    code = "window_expired"


class ScienceShadowCapacityError(ScienceShadowError):
    """A configured or hard capacity boundary was reached."""


class ScienceShadowCheckpointCapacityError(ScienceShadowCapacityError):
    """A state cannot fit the configured checkpoint representation bound."""


class ScienceShadowRetainedInputCapacityError(ScienceShadowCapacityError):
    """A replay input cannot fit the configured retained-input bound."""


class ScienceShadowRetainedInputPrivacyError(ScienceShadowError):
    """A normalized replay input exposes a forbidden secret-like label."""


class ScienceShadowDeadlineError(ScienceShadowError):
    """The caller-owned single absolute deadline expired."""


class ScienceShadowRecoveryError(ScienceShadowError):
    """A committed state chain could not be deterministically replayed."""


class ScienceShadowOutcome(str, Enum):
    ADVANCED = "advanced"
    INDETERMINATE = "indeterminate"
    WITHHELD = "withheld"
    INVALID = "invalid"
    CAPACITY_EXCEEDED = "capacity_exceeded"
    DEADLINE_EXCEEDED = "deadline_exceeded"


class ScienceShadowStage(str, Enum):
    RADAR_BRIDGE = "radar_bridge"
    ANALYSIS_TRANSFORM = "analysis_transform"
    DETECTOR = "detector"
    TRACKING = "tracking"
    ORCHESTRATOR = "orchestrator"


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sha_mapping(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _require_sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return value


def _require_optional_sha(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _require_sha(value, label)


def _require_safe_id(value: object, label: str) -> str:
    if not isinstance(value, str) or _SAFE_ID_RE.fullmatch(value) is None:
        raise ValueError(f"{label} is not a bounded safe identifier")
    return value


def _require_issue_code(value: object) -> str:
    if not isinstance(value, str) or _ISSUE_RE.fullmatch(value) is None:
        raise ValueError("shadow issue code is invalid")
    return value


def _bounded_count(value: object, label: str, maximum: int = 2**63 - 1) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= maximum
    ):
        raise ValueError(f"{label} must be a bounded non-negative integer")
    return value


def _check_deadline(deadline_at: float, monotonic: Callable[[], float]) -> None:
    now = float(monotonic())
    if not math.isfinite(now) or now >= deadline_at:
        raise ScienceShadowDeadlineError("science shadow deadline expired")


def _encode_state_value(
    value: Any,
    *,
    _depth: int = 0,
    _nodes: list[int] | None = None,
) -> Any:
    """Compatibility wrapper over the shared closed state codec."""

    try:
        return encode_cell_track_state_value(
            value,
            _depth=_depth,
            _nodes=_nodes,
        )
    except CellTrackStateCodecCapacityError as exc:
        raise ScienceShadowCapacityError(str(exc)) from exc
    except CellTrackStateCodecError as exc:
        raise ScienceShadowStoreError(str(exc)) from exc


def _decode_state_value(
    value: Any,
    *,
    _depth: int = 0,
    _nodes: list[int] | None = None,
) -> Any:
    """Compatibility wrapper over the shared closed state codec."""

    try:
        return decode_cell_track_state_value(
            value,
            _depth=_depth,
            _nodes=_nodes,
        )
    except CellTrackStateCodecError as exc:
        raise ScienceShadowStoreError(str(exc)) from exc


def _bounded_decompress(
    body: bytes,
    *,
    expected_size: int,
    maximum_size: int,
    label: str,
) -> bytes:
    if (
        not isinstance(body, bytes)
        or not 1 <= len(body) <= maximum_size
        or not 1 <= expected_size <= maximum_size
    ):
        raise ScienceShadowStoreError(f"{label} compressed bounds are invalid")
    inflater = zlib.decompressobj()
    try:
        raw = inflater.decompress(body, maximum_size + 1)
        raw += inflater.flush(maximum_size + 1 - len(raw))
    except zlib.error as exc:
        raise ScienceShadowStoreError(f"{label} compression stream is invalid") from exc
    if (
        len(raw) != expected_size
        or len(raw) > maximum_size
        or not inflater.eof
        or inflater.unused_data
        or inflater.unconsumed_tail
    ):
        raise ScienceShadowStoreError(f"{label} decompressed bounds differ")
    return raw


def _provenance_chain_sha256(
    previous_chain_sha256: str | None,
    receipt: "ScienceShadowReceipt",
) -> str:
    if receipt.outcome is not ScienceShadowOutcome.ADVANCED or receipt.provenance is None:
        raise ScienceShadowStoreError("only advanced provenance can extend the chain")
    _require_optional_sha(previous_chain_sha256, "previous provenance chain")
    return _sha_mapping({
        "previous_provenance_chain_sha256": previous_chain_sha256,
        "provenance": receipt.provenance.to_mapping(),
        "receipt_sha256": receipt.receipt_sha256,
        "revision": receipt.next_state_revision,
    })


def _expired_tag(kind: str, engine_id: str, identifier: str) -> bytes:
    return _canonical_json({"engine_id": engine_id, "identifier": identifier, "kind": kind})


def _expired_filter_positions(tag: bytes, size_bytes: int) -> tuple[int, ...]:
    digest = hashlib.sha256(tag).digest()
    first = int.from_bytes(digest[:8], "big")
    second = int.from_bytes(digest[8:16], "big") | 1
    bits = size_bytes * 8
    return tuple((first + index * second) % bits for index in range(EXPIRED_FILTER_HASH_COUNT))


def _expired_filter_contains(body: bytes, tag: bytes) -> bool:
    return all(
        body[position // 8] & (1 << (position % 8))
        for position in _expired_filter_positions(tag, len(body))
    )


def _expired_filter_add(body: bytearray, tag: bytes) -> None:
    for position in _expired_filter_positions(tag, len(body)):
        body[position // 8] |= 1 << (position % 8)


@dataclass(frozen=True, slots=True)
class ScienceShadowLimits:
    """Caller-owned orchestration, checkpoint, retention and replay bounds."""

    max_receipts: int = 2_048
    max_database_bytes: int = 64 * 1024 * 1024
    max_receipt_bytes: int = 64 * 1024
    max_replay_frames: int = 512
    checkpoint_interval_frames: int = 32
    max_checkpoint_bytes: int = 16 * 1024 * 1024
    max_retained_input_bytes: int = 16 * 1024 * 1024
    expired_filter_bytes: int = 1024 * 1024
    max_expired_memberships: int = 150_000
    max_lineage_hypotheses: int = 128
    max_runtime_seconds: float = 60.0

    def __post_init__(self) -> None:
        for label, value, minimum, maximum in (
            ("max_receipts", self.max_receipts, 1, HARD_MAX_RECEIPTS),
            (
                "max_database_bytes",
                self.max_database_bytes,
                1024 * 1024,
                HARD_MAX_DATABASE_BYTES,
            ),
            (
                "max_receipt_bytes",
                self.max_receipt_bytes,
                4_096,
                HARD_MAX_RECEIPT_BYTES,
            ),
            (
                "max_replay_frames",
                self.max_replay_frames,
                1,
                HARD_MAX_REPLAY_FRAMES,
            ),
            (
                "checkpoint_interval_frames",
                self.checkpoint_interval_frames,
                1,
                HARD_MAX_CHECKPOINT_INTERVAL_FRAMES,
            ),
            (
                "max_checkpoint_bytes",
                self.max_checkpoint_bytes,
                4_096,
                HARD_MAX_CHECKPOINT_BYTES,
            ),
            (
                "max_retained_input_bytes",
                self.max_retained_input_bytes,
                4_096,
                HARD_MAX_RETAINED_INPUT_BYTES,
            ),
            (
                "expired_filter_bytes",
                self.expired_filter_bytes,
                1_024,
                HARD_MAX_EXPIRED_FILTER_BYTES,
            ),
            (
                "max_expired_memberships",
                self.max_expired_memberships,
                1,
                HARD_MAX_EXPIRED_MEMBERSHIPS,
            ),
            (
                "max_lineage_hypotheses",
                self.max_lineage_hypotheses,
                0,
                HARD_MAX_LINEAGE_HYPOTHESES,
            ),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise ValueError(f"{label} is outside its hard boundary")
        if self.checkpoint_interval_frames > self.max_replay_frames:
            raise ValueError("checkpoint interval cannot exceed replay tail capacity")
        if self.checkpoint_interval_frames > self.max_receipts:
            raise ValueError("checkpoint interval cannot exceed receipt retention")
        if self.max_checkpoint_bytes >= self.max_database_bytes:
            raise ValueError("checkpoint bound must be smaller than database bound")
        if self.max_retained_input_bytes >= self.max_database_bytes:
            raise ValueError("retained-input bound must be smaller than database bound")
        if self.expired_filter_bytes >= self.max_database_bytes:
            raise ValueError("expired-membership filter must fit the database bound")
        bloom_bits = self.expired_filter_bytes * 8
        false_positive_bound = (
            1.0
            - math.exp(
                -EXPIRED_FILTER_HASH_COUNT
                * self.max_expired_memberships
                / bloom_bits
            )
        ) ** EXPIRED_FILTER_HASH_COUNT
        if false_positive_bound > 1e-6:
            raise ValueError(
                "expired-membership filter is undersized for its lifetime bound"
            )
        if (
            isinstance(self.max_runtime_seconds, bool)
            or not isinstance(self.max_runtime_seconds, (int, float))
            or not 0.0 < float(self.max_runtime_seconds) <= HARD_MAX_RUNTIME_SECONDS
            or not math.isfinite(float(self.max_runtime_seconds))
        ):
            raise ValueError("max_runtime_seconds is outside its hard boundary")

    def to_mapping(self) -> dict[str, int | float]:
        return {
            "checkpoint_interval_frames": self.checkpoint_interval_frames,
            "expired_filter_bytes": self.expired_filter_bytes,
            "max_checkpoint_bytes": self.max_checkpoint_bytes,
            "max_database_bytes": self.max_database_bytes,
            "max_expired_memberships": self.max_expired_memberships,
            "max_lineage_hypotheses": self.max_lineage_hypotheses,
            "max_receipt_bytes": self.max_receipt_bytes,
            "max_receipts": self.max_receipts,
            "max_replay_frames": self.max_replay_frames,
            "max_retained_input_bytes": self.max_retained_input_bytes,
            "max_runtime_seconds": float(self.max_runtime_seconds),
        }

    @property
    def fingerprint(self) -> str:
        return _sha_mapping(self.to_mapping())


@dataclass(frozen=True, slots=True)
class ShadowCompositionIssue:
    stage: ScienceShadowStage
    state: str
    code: str

    def __post_init__(self) -> None:
        if self.stage not in (
            ScienceShadowStage.RADAR_BRIDGE,
            ScienceShadowStage.ANALYSIS_TRANSFORM,
            ScienceShadowStage.DETECTOR,
        ):
            raise ValueError("composition issue stage is invalid")
        if self.state not in ("withheld", "invalid"):
            raise ValueError("composition issue state is invalid")
        _require_issue_code(self.code)


@dataclass(frozen=True, slots=True)
class ShadowEvidenceFrame:
    """In-memory, raw-payload-free binding of a lossless cell candidate."""

    canonical_run_id: str
    canonical_commit_id: str
    canonical_receipt_sha256: str
    canonical_result_sha256: str
    capture_id: str
    capture_receipt_sha256: str
    capture_manifest_sha256: str
    evidence_candidate_id: str
    evidence_content_sha256: str
    native_candidate_id: str
    native_candidate_content_sha256: str
    native_lineage_sha256: str
    analysis_field_id: str
    analysis_field_content_sha256: str
    analysis_lineage_sha256: str
    radar_semantics_certificate_id: str
    radar_semantics_certificate_sha256: str
    transform_certificate_id: str
    transform_certificate_sha256: str
    transform_receipt_id: str
    transform_receipt_sha256: str
    transform_config_sha256: str
    target_grid_sha256: str
    detector_config_sha256: str
    pipeline_config_sha256: str
    normalized_frame: NormalizedRasterFrame
    detection_frame: HierarchicalDetectionFrame
    pixel_accounting: EvidenceToCellsPixelAccounting
    operational_admission_blockers: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.canonical_run_id, str) or _RUN_ID_RE.fullmatch(
            self.canonical_run_id
        ) is None:
            raise ValueError("shadow evidence canonical run ID is invalid")
        for label, value in (
            ("canonical_receipt_sha256", self.canonical_receipt_sha256),
            ("canonical_result_sha256", self.canonical_result_sha256),
            ("capture_receipt_sha256", self.capture_receipt_sha256),
            ("capture_manifest_sha256", self.capture_manifest_sha256),
            ("evidence_content_sha256", self.evidence_content_sha256),
            ("native_candidate_content_sha256", self.native_candidate_content_sha256),
            ("native_lineage_sha256", self.native_lineage_sha256),
            ("analysis_field_content_sha256", self.analysis_field_content_sha256),
            ("analysis_lineage_sha256", self.analysis_lineage_sha256),
            (
                "radar_semantics_certificate_sha256",
                self.radar_semantics_certificate_sha256,
            ),
            ("transform_certificate_sha256", self.transform_certificate_sha256),
            ("transform_receipt_sha256", self.transform_receipt_sha256),
            ("transform_config_sha256", self.transform_config_sha256),
            ("target_grid_sha256", self.target_grid_sha256),
            ("detector_config_sha256", self.detector_config_sha256),
            ("pipeline_config_sha256", self.pipeline_config_sha256),
        ):
            _require_sha(value, label)
        for label, value in (
            ("canonical_commit_id", self.canonical_commit_id),
            ("capture_id", self.capture_id),
            ("evidence_candidate_id", self.evidence_candidate_id),
            ("native_candidate_id", self.native_candidate_id),
            ("analysis_field_id", self.analysis_field_id),
            (
                "radar_semantics_certificate_id",
                self.radar_semantics_certificate_id,
            ),
            ("transform_certificate_id", self.transform_certificate_id),
            ("transform_receipt_id", self.transform_receipt_id),
        ):
            if not isinstance(value, str) or not value or "\0" in value or len(value) > 512:
                raise ValueError(f"{label} is invalid")
        if self.evidence_candidate_id != "evidence-cells-" + self.evidence_content_sha256:
            raise ValueError("evidence candidate ID is not content-derived")
        if self.analysis_field_id != "analysis-field-" + self.analysis_field_content_sha256:
            raise ValueError("analysis field ID is not content-derived")
        if self.transform_receipt_id != "transform-receipt-" + self.transform_receipt_sha256:
            raise ValueError("transform receipt ID is not content-derived")
        if not isinstance(self.normalized_frame, NormalizedRasterFrame):
            raise TypeError("shadow evidence normalized frame is invalid")
        if not isinstance(self.detection_frame, HierarchicalDetectionFrame):
            raise TypeError("shadow evidence detection frame is invalid")
        if not isinstance(self.pixel_accounting, EvidenceToCellsPixelAccounting):
            raise TypeError("shadow evidence pixel accounting is invalid")
        if (
            self.detection_frame.frame_id != self.normalized_frame.frame_id
            or self.detection_frame.valid_at != self.normalized_frame.valid_at
            or self.detection_frame.analysis_grid_id
            != self.normalized_frame.grid.grid_id
            or self.target_grid_sha256
            != analysis_grid_sha256(self.normalized_frame.grid)
            or self.pixel_accounting.total_analysis_pixels
            != self.normalized_frame.grid.width * self.normalized_frame.grid.height
            or self.pixel_accounting.detection_objects
            != len(self.detection_frame.detections)
        ):
            raise ValueError("shadow evidence frame bindings are inconsistent")
        if (
            not isinstance(self.operational_admission_blockers, tuple)
            or "hard_process_isolation_not_wired"
            not in self.operational_admission_blockers
        ):
            raise ValueError("shadow evidence lost its process-isolation blocker")


@dataclass(frozen=True, slots=True)
class ShadowCompositionResult:
    run_id: str
    evidence: ShadowEvidenceFrame | None
    issues: tuple[ShadowCompositionIssue, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or _RUN_ID_RE.fullmatch(self.run_id) is None:
            raise ValueError("composition result run ID is invalid")
        if self.evidence is not None and self.evidence.canonical_run_id != self.run_id:
            raise ValueError("composition result evidence belongs to another run")
        if (self.evidence is None) == (not self.issues):
            raise ValueError("composition result must contain evidence XOR issues")
        if any(not isinstance(item, ShadowCompositionIssue) for item in self.issues):
            raise TypeError("composition result issues are invalid")

    @property
    def is_candidate(self) -> bool:
        return self.evidence is not None and not self.issues


@runtime_checkable
class ShadowEvidenceComposer(Protocol):
    @property
    def profile_sha256(self) -> str: ...

    @property
    def detector_config(self) -> HierarchicalDetectorConfig: ...

    @property
    def maximum_runtime_seconds(self) -> float: ...

    def compose(
        self,
        *,
        run_id: str,
        deadline_at: float,
        monotonic: Callable[[], float],
    ) -> ShadowCompositionResult: ...


class CanonicalEvidenceCellComposer:
    """Closed adapter over the existing bridge/transform/cell compositor."""

    def __init__(
        self,
        evidence_store: CanonicalEvidenceStore,
        artifact_authority: RadarDerivedArtifactAuthority,
        *,
        radar_semantics_certificates: Sequence[RadarFieldSemanticsCertificate],
        target_grid: AnalysisGrid,
        transform_adapter: CertifiedCoordinateTransformAdapter,
        transform_config: AnalysisGridTransformConfig,
        detector_config: HierarchicalDetectorConfig,
        pipeline_config: EvidenceToCellsShadowConfig,
    ) -> None:
        if not isinstance(evidence_store, CanonicalEvidenceStore):
            raise TypeError("evidence_store must be CanonicalEvidenceStore")
        if not isinstance(artifact_authority, RadarDerivedArtifactAuthority):
            raise TypeError("artifact_authority has the wrong contract")
        certificates = tuple(radar_semantics_certificates)
        if not certificates or any(
            not isinstance(item, RadarFieldSemanticsCertificate)
            for item in certificates
        ):
            raise TypeError("explicit radar semantics certificates are required")
        if not isinstance(target_grid, AnalysisGrid):
            raise TypeError("target_grid must be AnalysisGrid")
        if not isinstance(transform_adapter, CertifiedCoordinateTransformAdapter):
            raise TypeError("transform_adapter has the wrong contract")
        if not isinstance(transform_config, AnalysisGridTransformConfig):
            raise TypeError("transform_config has the wrong type")
        if not isinstance(detector_config, HierarchicalDetectorConfig):
            raise TypeError("detector_config has the wrong type")
        if not isinstance(pipeline_config, EvidenceToCellsShadowConfig):
            raise TypeError("pipeline_config has the wrong type")
        self._evidence_store = evidence_store
        self._artifact_authority = artifact_authority
        self._certificates = certificates
        self._target_grid = target_grid
        self._transform_adapter = transform_adapter
        self._transform_config = transform_config
        self._detector_config = detector_config
        self._pipeline_config = pipeline_config
        certificate = transform_adapter.certificate
        self._profile_sha256 = _sha_mapping({
            "algorithm": SCIENCE_SHADOW_ORCHESTRATOR_ALGORITHM,
            "detector": {
                "centroid_method": detector_config.centroid_method,
                "connectivity": detector_config.connectivity,
                "core_position_sigma_m": detector_config.core_position_sigma_m,
                "core_threshold": detector_config.core_threshold,
                "minimum_core_area_m2": detector_config.minimum_core_area_m2,
                "minimum_shield_area_m2": detector_config.minimum_shield_area_m2,
                "shield_position_sigma_m": detector_config.shield_position_sigma_m,
                "shield_threshold": detector_config.shield_threshold,
            },
            "pipeline_config": pipeline_config.to_mapping(),
            "radar_semantics_certificates": [
                item.to_mapping()
                for item in sorted(
                    certificates,
                    key=lambda item: (item.certificate_id, item.fingerprint),
                )
            ],
            "target_grid_sha256": analysis_grid_sha256(target_grid),
            "transform_certificate_sha256": certificate.fingerprint,
            "transform_config": transform_config.to_mapping(),
        })

    @property
    def profile_sha256(self) -> str:
        return self._profile_sha256

    @property
    def detector_config(self) -> HierarchicalDetectorConfig:
        return self._detector_config

    @property
    def maximum_runtime_seconds(self) -> float:
        return float(self._pipeline_config.max_runtime_seconds)

    def compose(
        self,
        *,
        run_id: str,
        deadline_at: float,
        monotonic: Callable[[], float],
    ) -> ShadowCompositionResult:
        result = compose_evidence_to_cells_shadow(
            self._evidence_store,
            self._artifact_authority,
            run_id=run_id,
            radar_semantics_certificates=self._certificates,
            target_grid=self._target_grid,
            transform_adapter=self._transform_adapter,
            transform_config=self._transform_config,
            detector_config=self._detector_config,
            pipeline_config=self._pipeline_config,
            deadline_at=deadline_at,
            monotonic=monotonic,
        )
        if not result.is_candidate:
            return ShadowCompositionResult(
                run_id=run_id,
                evidence=None,
                issues=tuple(
                    ShadowCompositionIssue(
                        stage=ScienceShadowStage(item.stage),
                        state=item.state,
                        code=item.code,
                    )
                    for item in result.issues
                ),
            )
        candidate = result.candidate
        assert candidate is not None
        if evidence_to_cells_content_sha256(candidate) != candidate.content_sha256:
            raise EvidenceToCellsShadowIntegrityError(
                "evidence-to-cells candidate changed at orchestration ingress"
            )
        evidence = ShadowEvidenceFrame(
            canonical_run_id=candidate.canonical_run_id,
            canonical_commit_id=candidate.canonical_commit_id,
            canonical_receipt_sha256=candidate.canonical_receipt_sha256,
            canonical_result_sha256=candidate.canonical_result_sha256,
            capture_id=candidate.capture_id,
            capture_receipt_sha256=candidate.capture_receipt_sha256,
            capture_manifest_sha256=candidate.capture_manifest_sha256,
            evidence_candidate_id=candidate.candidate_id,
            evidence_content_sha256=candidate.content_sha256,
            native_candidate_id=candidate.native_candidate_id,
            native_candidate_content_sha256=candidate.native_candidate_content_sha256,
            native_lineage_sha256=candidate.native_lineage_sha256,
            analysis_field_id=candidate.analysis_field_id,
            analysis_field_content_sha256=candidate.analysis_field_content_sha256,
            analysis_lineage_sha256=candidate.analysis_lineage_sha256,
            radar_semantics_certificate_id=(
                candidate.radar_semantics_certificate_id
            ),
            radar_semantics_certificate_sha256=(
                candidate.radar_semantics_certificate_sha256
            ),
            transform_certificate_id=candidate.transform_certificate_id,
            transform_certificate_sha256=candidate.transform_certificate_sha256,
            transform_receipt_id=candidate.transform_receipt_id,
            transform_receipt_sha256=candidate.transform_receipt_sha256,
            transform_config_sha256=candidate.transform_config_sha256,
            target_grid_sha256=candidate.target_grid_sha256,
            detector_config_sha256=candidate.detector_config_sha256,
            pipeline_config_sha256=candidate.pipeline_config_sha256,
            normalized_frame=candidate.normalized_frame,
            detection_frame=candidate.detection_frame,
            pixel_accounting=candidate.pixel_accounting,
            operational_admission_blockers=(
                candidate.operational_admission_blockers
            ),
        )
        return ShadowCompositionResult(run_id=run_id, evidence=evidence, issues=())


@dataclass(frozen=True, slots=True)
class ScienceShadowProvenance:
    canonical_commit_id: str
    canonical_receipt_sha256: str
    canonical_result_sha256: str
    capture_id: str
    capture_receipt_sha256: str
    capture_manifest_sha256: str
    evidence_candidate_id: str
    evidence_content_sha256: str
    native_candidate_id: str
    native_candidate_content_sha256: str
    native_lineage_sha256: str
    analysis_field_id: str
    analysis_field_content_sha256: str
    analysis_lineage_sha256: str
    radar_semantics_certificate_id: str
    radar_semantics_certificate_sha256: str
    transform_certificate_id: str
    transform_certificate_sha256: str
    transform_receipt_id: str
    transform_receipt_sha256: str
    transform_config_sha256: str
    target_grid_sha256: str
    detector_config_sha256: str
    pipeline_config_sha256: str
    normalized_frame_id: str

    def __post_init__(self) -> None:
        for label, value in self.to_mapping().items():
            if label.endswith("_sha256"):
                _require_sha(value, label)
            elif not isinstance(value, str) or not value or "\0" in value or len(value) > 512:
                raise ValueError(f"provenance {label} is invalid")
        if self.evidence_candidate_id != "evidence-cells-" + self.evidence_content_sha256:
            raise ValueError("receipt evidence candidate ID is inconsistent")
        if self.analysis_field_id != "analysis-field-" + self.analysis_field_content_sha256:
            raise ValueError("receipt analysis field ID is inconsistent")
        if self.transform_receipt_id != "transform-receipt-" + self.transform_receipt_sha256:
            raise ValueError("receipt transform identity is inconsistent")

    @classmethod
    def from_evidence(cls, evidence: ShadowEvidenceFrame) -> "ScienceShadowProvenance":
        return cls(
            canonical_commit_id=evidence.canonical_commit_id,
            canonical_receipt_sha256=evidence.canonical_receipt_sha256,
            canonical_result_sha256=evidence.canonical_result_sha256,
            capture_id=evidence.capture_id,
            capture_receipt_sha256=evidence.capture_receipt_sha256,
            capture_manifest_sha256=evidence.capture_manifest_sha256,
            evidence_candidate_id=evidence.evidence_candidate_id,
            evidence_content_sha256=evidence.evidence_content_sha256,
            native_candidate_id=evidence.native_candidate_id,
            native_candidate_content_sha256=evidence.native_candidate_content_sha256,
            native_lineage_sha256=evidence.native_lineage_sha256,
            analysis_field_id=evidence.analysis_field_id,
            analysis_field_content_sha256=evidence.analysis_field_content_sha256,
            analysis_lineage_sha256=evidence.analysis_lineage_sha256,
            radar_semantics_certificate_id=evidence.radar_semantics_certificate_id,
            radar_semantics_certificate_sha256=(
                evidence.radar_semantics_certificate_sha256
            ),
            transform_certificate_id=evidence.transform_certificate_id,
            transform_certificate_sha256=evidence.transform_certificate_sha256,
            transform_receipt_id=evidence.transform_receipt_id,
            transform_receipt_sha256=evidence.transform_receipt_sha256,
            transform_config_sha256=evidence.transform_config_sha256,
            target_grid_sha256=evidence.target_grid_sha256,
            detector_config_sha256=evidence.detector_config_sha256,
            pipeline_config_sha256=evidence.pipeline_config_sha256,
            normalized_frame_id=evidence.normalized_frame.frame_id,
        )

    def to_mapping(self) -> dict[str, str]:
        return {
            field: str(getattr(self, field))
            for field in self.__dataclass_fields__
        }


@dataclass(frozen=True, slots=True)
class ScienceShadowAccounting:
    total_analysis_pixels: int
    valid_detector_input_pixels: int
    excluded_nodata_pixels: int
    excluded_undetect_pixels: int
    excluded_mixed_pixels: int
    excluded_outside_support_pixels: int
    compositor_detection_objects: int
    tracker_detection_objects: int
    input_tracks: int
    output_tracks: int
    input_lifecycle_events: int
    output_lifecycle_events: int
    candidate_edges: int

    def __post_init__(self) -> None:
        for label, value in self.to_mapping().items():
            _bounded_count(value, label)
        if self.total_analysis_pixels != (
            self.valid_detector_input_pixels
            + self.excluded_nodata_pixels
            + self.excluded_undetect_pixels
            + self.excluded_mixed_pixels
            + self.excluded_outside_support_pixels
        ):
            raise ValueError("science shadow pixel accounting is not exhaustive")
        if self.tracker_detection_objects > self.compositor_detection_objects:
            raise ValueError("tracker cannot account more detections than compositor")

    @classmethod
    def without_evidence(cls, state: CellTrackEngineState) -> "ScienceShadowAccounting":
        return cls(
            total_analysis_pixels=0,
            valid_detector_input_pixels=0,
            excluded_nodata_pixels=0,
            excluded_undetect_pixels=0,
            excluded_mixed_pixels=0,
            excluded_outside_support_pixels=0,
            compositor_detection_objects=0,
            tracker_detection_objects=0,
            input_tracks=len(state.ledger.tracks),
            output_tracks=len(state.ledger.tracks),
            input_lifecycle_events=len(state.ledger.lineage_events),
            output_lifecycle_events=len(state.ledger.lineage_events),
            candidate_edges=0,
        )

    @classmethod
    def from_evidence(
        cls,
        evidence: ShadowEvidenceFrame,
        state: CellTrackEngineState,
        transition: CellTrackTransitionResult | None,
    ) -> "ScienceShadowAccounting":
        pixels = evidence.pixel_accounting
        next_state = state if transition is None else transition.next_state
        return cls(
            total_analysis_pixels=pixels.total_analysis_pixels,
            valid_detector_input_pixels=pixels.valid_detector_input_pixels,
            excluded_nodata_pixels=pixels.excluded_nodata_pixels,
            excluded_undetect_pixels=pixels.excluded_undetect_pixels,
            excluded_mixed_pixels=pixels.excluded_mixed_pixels,
            excluded_outside_support_pixels=pixels.excluded_outside_support_pixels,
            compositor_detection_objects=pixels.detection_objects,
            tracker_detection_objects=(
                0 if transition is None else len(transition.detection_frame.detections)
            ),
            input_tracks=len(state.ledger.tracks),
            output_tracks=len(next_state.ledger.tracks),
            input_lifecycle_events=len(state.ledger.lineage_events),
            output_lifecycle_events=len(next_state.ledger.lineage_events),
            candidate_edges=0 if transition is None else transition.candidate_edge_count,
        )

    @classmethod
    def from_uncommitted_evidence(
        cls,
        evidence: ShadowEvidenceFrame,
        state: CellTrackEngineState,
        transition: CellTrackTransitionResult | None = None,
    ) -> "ScienceShadowAccounting":
        """Account completed tracking work while retaining the exact old state."""

        pixels = evidence.pixel_accounting
        return cls(
            total_analysis_pixels=pixels.total_analysis_pixels,
            valid_detector_input_pixels=pixels.valid_detector_input_pixels,
            excluded_nodata_pixels=pixels.excluded_nodata_pixels,
            excluded_undetect_pixels=pixels.excluded_undetect_pixels,
            excluded_mixed_pixels=pixels.excluded_mixed_pixels,
            excluded_outside_support_pixels=pixels.excluded_outside_support_pixels,
            compositor_detection_objects=pixels.detection_objects,
            tracker_detection_objects=(
                0 if transition is None else len(transition.detection_frame.detections)
            ),
            input_tracks=len(state.ledger.tracks),
            output_tracks=len(state.ledger.tracks),
            input_lifecycle_events=len(state.ledger.lineage_events),
            output_lifecycle_events=len(state.ledger.lineage_events),
            candidate_edges=0 if transition is None else transition.candidate_edge_count,
        )

    def to_mapping(self) -> dict[str, int]:
        return {
            field: int(getattr(self, field))
            for field in self.__dataclass_fields__
        }


def _lineage_mapping(value: CellTrackLineageHypothesis) -> dict[str, Any]:
    return {
        "children": [
            {"detection_id": item.detection_id, "track_id": item.track_id}
            for item in value.children
        ],
        "cost": value.cost,
        "cost_components": [list(item) for item in value.cost_components],
        "cost_model_id": value.cost_model_id,
        "event_type": value.event_type,
        "hypothesis_id": value.hypothesis_id,
        "parent_track_ids": list(value.parent_track_ids),
        "provenance_ids": list(value.provenance_ids),
        "selected": value.selected,
    }


def _lineage_from_mapping(value: Mapping[str, Any]) -> CellTrackLineageHypothesis:
    return CellTrackLineageHypothesis(
        hypothesis_id=str(value["hypothesis_id"]),
        event_type=str(value["event_type"]),  # type: ignore[arg-type]
        parent_track_ids=tuple(str(item) for item in value["parent_track_ids"]),
        children=tuple(
            LineageChildIdentity(
                detection_id=str(item["detection_id"]),
                track_id=str(item["track_id"]),
            )
            for item in value["children"]
        ),
        cost=float(value["cost"]),
        cost_model_id=str(value["cost_model_id"]),
        cost_components=tuple(
            (str(item[0]), float(item[1])) for item in value["cost_components"]
        ),
        provenance_ids=tuple(str(item) for item in value["provenance_ids"]),
        selected=bool(value["selected"]),
    )


def _canonical_lineage_hypotheses(
    values: Iterable[CellTrackLineageHypothesis],
    maximum: int,
) -> tuple[CellTrackLineageHypothesis, ...]:
    result = tuple(values)
    if len(result) > maximum:
        raise ScienceShadowCapacityError("lineage hypothesis capacity exceeded")
    if any(not isinstance(item, CellTrackLineageHypothesis) for item in result):
        raise TypeError("lineage_hypotheses contains an invalid record")
    identifiers = tuple(item.hypothesis_id for item in result)
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("lineage hypothesis identities are duplicated")
    return tuple(sorted(result, key=lambda item: item.hypothesis_id))


def science_shadow_request_sha256(
    *,
    attempt_id: str,
    canonical_run_id: str,
    engine_id: str,
    runtime_profile_sha256: str,
    lineage_hypotheses: Iterable[CellTrackLineageHypothesis] = (),
    maximum_lineage_hypotheses: int = HARD_MAX_LINEAGE_HYPOTHESES,
) -> str:
    """Return the exact public idempotency digest used by the orchestrator."""

    attempt = _require_safe_id(attempt_id, "attempt_id")
    engine = _require_safe_id(engine_id, "engine_id")
    runtime_sha = _require_sha(
        runtime_profile_sha256,
        "runtime_profile_sha256",
    )
    if not isinstance(canonical_run_id, str) or _RUN_ID_RE.fullmatch(
        canonical_run_id
    ) is None:
        raise ValueError("canonical_run_id is invalid")
    if (
        isinstance(maximum_lineage_hypotheses, bool)
        or not isinstance(maximum_lineage_hypotheses, int)
        or not 0
        <= maximum_lineage_hypotheses
        <= HARD_MAX_LINEAGE_HYPOTHESES
    ):
        raise ValueError("maximum_lineage_hypotheses is invalid")
    hypotheses = _canonical_lineage_hypotheses(
        lineage_hypotheses,
        maximum_lineage_hypotheses,
    )
    return _sha_mapping({
        "attempt_id": attempt,
        "canonical_run_id": canonical_run_id,
        "engine_id": engine,
        "lineage_hypotheses": [_lineage_mapping(item) for item in hypotheses],
        "runtime_profile_sha256": runtime_sha,
    })


@dataclass(frozen=True, slots=True)
class ScienceShadowReceipt:
    receipt_sha256: str
    attempt_id: str
    request_sha256: str
    binding_sha256: str
    engine_id: str
    runtime_profile_sha256: str
    canonical_run_id: str
    outcome: ScienceShadowOutcome
    stage: ScienceShadowStage
    issue_codes: tuple[str, ...]
    previous_advanced_receipt_sha256: str | None
    provenance: ScienceShadowProvenance | None
    input_state_revision: int
    next_state_revision: int
    transition_id: str | None
    transition_reason_codes: tuple[str, ...]
    lineage_hypotheses: tuple[CellTrackLineageHypothesis, ...]
    applied_lineage_hypothesis_ids: tuple[str, ...]
    accounting: ScienceShadowAccounting
    admission_status: str = SCIENCE_SHADOW_ADMISSION_STATUS
    meteorological_admission_granted: bool = False
    forecast_generated: bool = False
    operational_snapshot_claim_generated: bool = False
    process_isolation_certified: bool = False
    operational_admission_blockers: tuple[str, ...] = (
        SCIENCE_SHADOW_OPERATIONAL_BLOCKERS
    )
    algorithm_version: str = SCIENCE_SHADOW_ORCHESTRATOR_ALGORITHM

    def __post_init__(self) -> None:
        _require_sha(self.receipt_sha256, "receipt_sha256")
        _require_safe_id(self.attempt_id, "attempt_id")
        _require_sha(self.request_sha256, "request_sha256")
        _require_sha(self.binding_sha256, "binding_sha256")
        _require_safe_id(self.engine_id, "engine_id")
        _require_sha(self.runtime_profile_sha256, "runtime_profile_sha256")
        if _RUN_ID_RE.fullmatch(self.canonical_run_id) is None:
            raise ValueError("receipt canonical run ID is invalid")
        if not isinstance(self.outcome, ScienceShadowOutcome):
            raise TypeError("receipt outcome is invalid")
        if not isinstance(self.stage, ScienceShadowStage):
            raise TypeError("receipt stage is invalid")
        if self.issue_codes != tuple(sorted(set(self.issue_codes))):
            raise ValueError("receipt issue codes must be canonical")
        for item in self.issue_codes:
            _require_issue_code(item)
        _require_optional_sha(
            self.previous_advanced_receipt_sha256,
            "previous_advanced_receipt_sha256",
        )
        _bounded_count(self.input_state_revision, "input_state_revision", 2**31 - 1)
        _bounded_count(self.next_state_revision, "next_state_revision", 2**31 - 1)
        if self.transition_id is not None and (
            not isinstance(self.transition_id, str)
            or not self.transition_id
            or "\0" in self.transition_id
            or len(self.transition_id) > 256
        ):
            raise ValueError("receipt transition ID is invalid")
        if self.transition_reason_codes != tuple(
            sorted(set(self.transition_reason_codes))
        ):
            raise ValueError("transition reason codes must be canonical")
        for item in self.transition_reason_codes:
            _require_issue_code(item)
        if any(
            not isinstance(item, CellTrackLineageHypothesis)
            for item in self.lineage_hypotheses
        ) or self.lineage_hypotheses != tuple(
            sorted(self.lineage_hypotheses, key=lambda item: item.hypothesis_id)
        ):
            raise ValueError("receipt lineage hypotheses are not canonical")
        if self.applied_lineage_hypothesis_ids != tuple(
            sorted(set(self.applied_lineage_hypothesis_ids))
        ):
            raise ValueError("applied lineage IDs are not canonical")
        selected = {
            item.hypothesis_id for item in self.lineage_hypotheses if item.selected
        }
        if not set(self.applied_lineage_hypothesis_ids) <= selected:
            raise ValueError("receipt applied an unselected lineage hypothesis")
        if not isinstance(self.accounting, ScienceShadowAccounting):
            raise TypeError("receipt accounting is invalid")
        if (
            self.admission_status != SCIENCE_SHADOW_ADMISSION_STATUS
            or self.meteorological_admission_granted
            or self.forecast_generated
            or self.operational_snapshot_claim_generated
            or self.process_isolation_certified
            or self.operational_admission_blockers
            != SCIENCE_SHADOW_OPERATIONAL_BLOCKERS
            or self.algorithm_version != SCIENCE_SHADOW_ORCHESTRATOR_ALGORITHM
        ):
            raise ValueError("receipt contains a forbidden operational claim")
        if self.outcome is ScienceShadowOutcome.ADVANCED:
            if (
                self.issue_codes
                or self.provenance is None
                or self.transition_id is None
                or self.transition_reason_codes
                or self.next_state_revision != self.input_state_revision + 1
                or self.accounting.tracker_detection_objects
                != self.accounting.compositor_detection_objects
            ):
                raise ValueError("advanced receipt is internally inconsistent")
        elif self.outcome is ScienceShadowOutcome.INDETERMINATE:
            if (
                not self.issue_codes
                or self.provenance is None
                or self.transition_id is None
                or not self.transition_reason_codes
                or self.issue_codes != self.transition_reason_codes
                or self.next_state_revision != self.input_state_revision
                or self.applied_lineage_hypothesis_ids
                or self.accounting.tracker_detection_objects
                != self.accounting.compositor_detection_objects
            ):
                raise ValueError("indeterminate receipt is internally inconsistent")
        else:
            if (
                not self.issue_codes
                or self.transition_id is not None
                or self.transition_reason_codes
                or self.next_state_revision != self.input_state_revision
                or self.applied_lineage_hypothesis_ids
            ):
                raise ValueError("non-transition receipt is internally inconsistent")
        if self.receipt_sha256 != _sha_mapping(self.payload_mapping()):
            raise ValueError("science shadow receipt checksum differs")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "accounting": self.accounting.to_mapping(),
            "admission_status": self.admission_status,
            "algorithm_version": self.algorithm_version,
            "applied_lineage_hypothesis_ids": list(
                self.applied_lineage_hypothesis_ids
            ),
            "attempt_id": self.attempt_id,
            "binding_sha256": self.binding_sha256,
            "canonical_run_id": self.canonical_run_id,
            "contract": SCIENCE_SHADOW_RECEIPT_CONTRACT,
            "engine_id": self.engine_id,
            "forecast_generated": self.forecast_generated,
            "input_state_revision": self.input_state_revision,
            "issue_codes": list(self.issue_codes),
            "lineage_hypotheses": [
                _lineage_mapping(item) for item in self.lineage_hypotheses
            ],
            "meteorological_admission_granted": (
                self.meteorological_admission_granted
            ),
            "next_state_revision": self.next_state_revision,
            "operational_admission_blockers": list(
                self.operational_admission_blockers
            ),
            "operational_snapshot_claim_generated": (
                self.operational_snapshot_claim_generated
            ),
            "outcome": self.outcome.value,
            "previous_advanced_receipt_sha256": (
                self.previous_advanced_receipt_sha256
            ),
            "process_isolation_certified": self.process_isolation_certified,
            "provenance": (
                None if self.provenance is None else self.provenance.to_mapping()
            ),
            "request_sha256": self.request_sha256,
            "runtime_profile_sha256": self.runtime_profile_sha256,
            "stage": self.stage.value,
            "transition_id": self.transition_id,
            "transition_reason_codes": list(self.transition_reason_codes),
        }

    def to_mapping(self) -> dict[str, Any]:
        result = self.payload_mapping()
        result["receipt_sha256"] = self.receipt_sha256
        return result

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ScienceShadowReceipt":
        if value.get("contract") != SCIENCE_SHADOW_RECEIPT_CONTRACT:
            raise ScienceShadowStoreError("science shadow receipt contract drift")
        provenance_value = value.get("provenance")
        provenance = (
            None
            if provenance_value is None
            else ScienceShadowProvenance(**dict(provenance_value))
        )
        accounting = ScienceShadowAccounting(**dict(value["accounting"]))
        return cls(
            receipt_sha256=str(value["receipt_sha256"]),
            attempt_id=str(value["attempt_id"]),
            request_sha256=str(value["request_sha256"]),
            binding_sha256=str(value["binding_sha256"]),
            engine_id=str(value["engine_id"]),
            runtime_profile_sha256=str(value["runtime_profile_sha256"]),
            canonical_run_id=str(value["canonical_run_id"]),
            outcome=ScienceShadowOutcome(str(value["outcome"])),
            stage=ScienceShadowStage(str(value["stage"])),
            issue_codes=tuple(str(item) for item in value["issue_codes"]),
            previous_advanced_receipt_sha256=value[
                "previous_advanced_receipt_sha256"
            ],
            provenance=provenance,
            input_state_revision=int(value["input_state_revision"]),
            next_state_revision=int(value["next_state_revision"]),
            transition_id=value["transition_id"],
            transition_reason_codes=tuple(
                str(item) for item in value["transition_reason_codes"]
            ),
            lineage_hypotheses=tuple(
                _lineage_from_mapping(item) for item in value["lineage_hypotheses"]
            ),
            applied_lineage_hypothesis_ids=tuple(
                str(item) for item in value["applied_lineage_hypothesis_ids"]
            ),
            accounting=accounting,
            admission_status=str(value["admission_status"]),
            meteorological_admission_granted=bool(
                value["meteorological_admission_granted"]
            ),
            forecast_generated=bool(value["forecast_generated"]),
            operational_snapshot_claim_generated=bool(
                value["operational_snapshot_claim_generated"]
            ),
            process_isolation_certified=bool(value["process_isolation_certified"]),
            operational_admission_blockers=tuple(
                str(item) for item in value["operational_admission_blockers"]
            ),
            algorithm_version=str(value["algorithm_version"]),
        )


def _build_receipt(
    *,
    attempt_id: str,
    request_sha256: str,
    binding_sha256: str,
    engine_id: str,
    runtime_profile_sha256: str,
    canonical_run_id: str,
    outcome: ScienceShadowOutcome,
    stage: ScienceShadowStage,
    issue_codes: tuple[str, ...],
    previous_advanced_receipt_sha256: str | None,
    provenance: ScienceShadowProvenance | None,
    input_state_revision: int,
    next_state_revision: int,
    transition_id: str | None,
    transition_reason_codes: tuple[str, ...],
    lineage_hypotheses: tuple[CellTrackLineageHypothesis, ...],
    applied_lineage_hypothesis_ids: tuple[str, ...],
    accounting: ScienceShadowAccounting,
) -> ScienceShadowReceipt:
    values = {
        "attempt_id": attempt_id,
        "request_sha256": request_sha256,
        "binding_sha256": binding_sha256,
        "engine_id": engine_id,
        "runtime_profile_sha256": runtime_profile_sha256,
        "canonical_run_id": canonical_run_id,
        "outcome": outcome,
        "stage": stage,
        "issue_codes": issue_codes,
        "previous_advanced_receipt_sha256": previous_advanced_receipt_sha256,
        "provenance": provenance,
        "input_state_revision": input_state_revision,
        "next_state_revision": next_state_revision,
        "transition_id": transition_id,
        "transition_reason_codes": transition_reason_codes,
        "lineage_hypotheses": lineage_hypotheses,
        "applied_lineage_hypothesis_ids": applied_lineage_hypothesis_ids,
        "accounting": accounting,
    }
    temporary = object.__new__(ScienceShadowReceipt)
    for field, value in values.items():
        object.__setattr__(temporary, field, value)
    object.__setattr__(temporary, "receipt_sha256", "0" * 64)
    object.__setattr__(temporary, "admission_status", SCIENCE_SHADOW_ADMISSION_STATUS)
    object.__setattr__(temporary, "meteorological_admission_granted", False)
    object.__setattr__(temporary, "forecast_generated", False)
    object.__setattr__(temporary, "operational_snapshot_claim_generated", False)
    object.__setattr__(temporary, "process_isolation_certified", False)
    object.__setattr__(
        temporary,
        "operational_admission_blockers",
        SCIENCE_SHADOW_OPERATIONAL_BLOCKERS,
    )
    object.__setattr__(
        temporary,
        "algorithm_version",
        SCIENCE_SHADOW_ORCHESTRATOR_ALGORITHM,
    )
    digest = _sha_mapping(temporary.payload_mapping())
    return ScienceShadowReceipt(receipt_sha256=digest, **values)


@dataclass(frozen=True, slots=True)
class ScienceShadowRetainedInput:
    """Exact normalized replay input; never provider metadata or credentials."""

    retained_input_sha256: str
    receipt_sha256: str
    provenance_sha256: str
    detection_frame_sha256: str
    frame: NormalizedRasterFrame

    def __post_init__(self) -> None:
        _require_sha(self.retained_input_sha256, "retained_input_sha256")
        _require_sha(self.receipt_sha256, "retained receipt_sha256")
        _require_sha(self.provenance_sha256, "retained provenance_sha256")
        _require_sha(self.detection_frame_sha256, "retained detection_frame_sha256")
        if not isinstance(self.frame, NormalizedRasterFrame):
            raise TypeError("retained input frame is invalid")
        if (
            _SAFE_RETAINED_SOURCE_RE.fullmatch(self.frame.source) is None
            or _SAFE_RETAINED_SOURCE_RE.fullmatch(self.frame.frame_id) is None
            or _SAFE_RETAINED_SOURCE_RE.fullmatch(self.frame.quantity) is None
            or _SAFE_RETAINED_SOURCE_RE.fullmatch(self.frame.unit) is None
        ):
            raise ScienceShadowRetainedInputPrivacyError(
                "retained frame identifiers are not privacy-bounded"
            )
        if any(
            _SECRET_LABEL_RE.search(value) is not None
            for value in (
                self.frame.source,
                self.frame.frame_id,
                self.frame.quantity,
                self.frame.unit,
            )
        ):
            raise ScienceShadowRetainedInputPrivacyError(
                "retained frame contains a secret-like label"
            )
        if self.retained_input_sha256 != _sha_mapping(self.payload_mapping()):
            raise ValueError("retained input checksum differs")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "contract": SCIENCE_SHADOW_RETAINED_INPUT_CONTRACT,
            "detection_frame_sha256": self.detection_frame_sha256,
            "frame": _encode_state_value(self.frame),
            "provenance_sha256": self.provenance_sha256,
            "receipt_sha256": self.receipt_sha256,
        }

    def to_mapping(self) -> dict[str, Any]:
        value = self.payload_mapping()
        value["retained_input_sha256"] = self.retained_input_sha256
        return value

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ScienceShadowRetainedInput":
        if set(value) != {
            "contract",
            "detection_frame_sha256",
            "frame",
            "provenance_sha256",
            "receipt_sha256",
            "retained_input_sha256",
        } or value.get("contract") != SCIENCE_SHADOW_RETAINED_INPUT_CONTRACT:
            raise ScienceShadowStoreError("retained input schema is not exact")
        frame = _decode_state_value(value["frame"])
        if not isinstance(frame, NormalizedRasterFrame):
            raise ScienceShadowStoreError("retained input root is not a raster frame")
        try:
            result = cls(
                retained_input_sha256=str(value["retained_input_sha256"]),
                receipt_sha256=str(value["receipt_sha256"]),
                provenance_sha256=str(value["provenance_sha256"]),
                detection_frame_sha256=str(value["detection_frame_sha256"]),
                frame=frame,
            )
        except (TypeError, ValueError) as exc:
            raise ScienceShadowStoreError("retained input failed validation") from exc
        if result.to_mapping() != value:
            raise ScienceShadowStoreError("retained input is not canonical")
        return result


def _build_retained_input(
    receipt: ScienceShadowReceipt,
    evidence: ShadowEvidenceFrame,
) -> ScienceShadowRetainedInput:
    if receipt.outcome is not ScienceShadowOutcome.ADVANCED or receipt.provenance is None:
        raise ScienceShadowStoreError("retained input requires advanced provenance")
    if receipt.provenance != ScienceShadowProvenance.from_evidence(evidence):
        raise ScienceShadowStoreConflict("retained input provenance differs")
    values = {
        "receipt_sha256": receipt.receipt_sha256,
        "provenance_sha256": _sha_mapping(receipt.provenance.to_mapping()),
        "detection_frame_sha256": _sha_mapping(
            _encode_state_value(evidence.detection_frame)
        ),
        "frame": evidence.normalized_frame,
    }
    temporary = object.__new__(ScienceShadowRetainedInput)
    for name, value in values.items():
        object.__setattr__(temporary, name, value)
    object.__setattr__(temporary, "retained_input_sha256", "0" * 64)
    return ScienceShadowRetainedInput(
        retained_input_sha256=_sha_mapping(temporary.payload_mapping()),
        **values,
    )


@dataclass(frozen=True, slots=True)
class ScienceShadowStateCheckpoint:
    """Checksummed immutable CellTrack state bound to one provenance-chain head."""

    checkpoint_sha256: str
    previous_checkpoint_sha256: str | None
    engine_id: str
    runtime_profile_sha256: str
    revision: int
    advanced_receipt_sha256: str
    advanced_receipt_json_sha256: str
    provenance_chain_sha256: str
    state_sha256: str
    state_compressed_sha256: str
    state_uncompressed_bytes: int
    state_compressed_bytes: int
    state: CellTrackEngineState

    def __post_init__(self) -> None:
        _require_sha(self.checkpoint_sha256, "checkpoint_sha256")
        _require_optional_sha(self.previous_checkpoint_sha256, "previous checkpoint")
        _require_safe_id(self.engine_id, "checkpoint engine_id")
        for label, value in (
            ("runtime_profile_sha256", self.runtime_profile_sha256),
            ("advanced_receipt_sha256", self.advanced_receipt_sha256),
            ("advanced_receipt_json_sha256", self.advanced_receipt_json_sha256),
            ("provenance_chain_sha256", self.provenance_chain_sha256),
            ("state_sha256", self.state_sha256),
            ("state_compressed_sha256", self.state_compressed_sha256),
        ):
            _require_sha(value, label)
        _bounded_count(self.revision, "checkpoint revision", 2**31 - 1)
        _bounded_count(
            self.state_uncompressed_bytes,
            "checkpoint uncompressed bytes",
            HARD_MAX_CHECKPOINT_BYTES,
        )
        _bounded_count(
            self.state_compressed_bytes,
            "checkpoint compressed bytes",
            HARD_MAX_CHECKPOINT_BYTES,
        )
        if self.state_uncompressed_bytes < 1 or self.state_compressed_bytes < 1:
            raise ValueError("checkpoint byte counts must be positive")
        if not isinstance(self.state, CellTrackEngineState):
            raise TypeError("checkpoint state is invalid")
        if self.state.engine_id != self.engine_id or self.state.revision != self.revision:
            raise ValueError("checkpoint state identity/revision differs")
        state_raw = _canonical_json(_encode_state_value(self.state))
        if (
            len(state_raw) != self.state_uncompressed_bytes
            or hashlib.sha256(state_raw).hexdigest() != self.state_sha256
            or self.checkpoint_sha256 != _sha_mapping(self.payload_mapping())
        ):
            raise ValueError("checkpoint state or metadata checksum differs")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "advanced_receipt_json_sha256": self.advanced_receipt_json_sha256,
            "advanced_receipt_sha256": self.advanced_receipt_sha256,
            "contract": SCIENCE_SHADOW_CHECKPOINT_CONTRACT,
            "engine_id": self.engine_id,
            "previous_checkpoint_sha256": self.previous_checkpoint_sha256,
            "provenance_chain_sha256": self.provenance_chain_sha256,
            "revision": self.revision,
            "runtime_profile_sha256": self.runtime_profile_sha256,
            "state_compressed_bytes": self.state_compressed_bytes,
            "state_compressed_sha256": self.state_compressed_sha256,
            "state_sha256": self.state_sha256,
            "state_uncompressed_bytes": self.state_uncompressed_bytes,
        }


@dataclass(frozen=True, slots=True)
class _ScienceShadowRecoveryTail:
    receipt: ScienceShadowReceipt
    retained_input: ScienceShadowRetainedInput
    provenance_chain_sha256: str


@dataclass(frozen=True, slots=True)
class _ScienceShadowRecoverySnapshot:
    checkpoint: ScienceShadowStateCheckpoint | None
    checkpoint_receipt: ScienceShadowReceipt | None
    tail: tuple[_ScienceShadowRecoveryTail, ...]
    head_receipt_sha256: str | None
    head_revision: int
    head_provenance_chain_sha256: str | None


class ScienceShadowReceiptStore:
    """Crash-durable bounded receipt, replay-input and state-checkpoint authority.

    The store uses rollback-journal SQLite with ``synchronous=FULL``.  One
    ``BEGIN IMMEDIATE`` transaction commits an advanced receipt, exact replay
    input, CAS head, optional checkpoint, expired-membership filter and all
    pruning.  No hot-path ``VACUUM`` or executable serialization is used.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        engine_id: str,
        runtime_profile_sha256: str,
        limits: ScienceShadowLimits,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(path)
        if not self.path.is_absolute():
            raise ValueError("science shadow receipt path must be absolute")
        self._engine_id = _require_safe_id(engine_id, "engine_id")
        self._runtime_profile_sha256 = _require_sha(
            runtime_profile_sha256,
            "runtime_profile_sha256",
        )
        if not isinstance(limits, ScienceShadowLimits):
            raise TypeError("limits must be ScienceShadowLimits")
        self._limits = limits
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self.initialize()

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _connect(self) -> sqlite3.Connection:
        if self.path.exists() and self.path.is_symlink():
            raise ScienceShadowStoreError("science shadow database is a symlink")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        database = sqlite3.connect(self.path, timeout=2.0)
        try:
            database.row_factory = sqlite3.Row
            database.execute("PRAGMA foreign_keys=ON")
            database.execute("PRAGMA synchronous=FULL")
            database.execute("PRAGMA trusted_schema=OFF")
            mode = str(
                database.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            ).lower()
            if mode != "delete":
                raise ScienceShadowStoreError(
                    "science shadow journal mode drift"
                )
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            maximum_pages = max(
                1,
                self._limits.max_database_bytes // page_size,
            )
            database.execute(f"PRAGMA max_page_count={maximum_pages}")
        except BaseException:
            database.close()
            raise
        return database

    @staticmethod
    def _retention_checksum(
        *,
        next_sequence: int,
        expired_filter: bytes,
        expired_memberships: int,
        last_pruned_sequence: int,
    ) -> str:
        return _sha_mapping({
            "expired_filter_sha256": hashlib.sha256(expired_filter).hexdigest(),
            "expired_memberships": expired_memberships,
            "last_pruned_sequence": last_pruned_sequence,
            "next_sequence": next_sequence,
        })

    def initialize(self) -> None:
        with self._lock, closing(self._connect()) as database:
            existing = database.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='science_shadow_schema'"
            ).fetchone()
            if existing is not None:
                row = database.execute(
                    "SELECT schema_version,engine_id,runtime_profile_sha256 "
                    "FROM science_shadow_schema WHERE singleton=1"
                ).fetchone()
                if row is None or (
                    int(row["schema_version"]) != SCIENCE_SHADOW_SCHEMA_VERSION
                    or str(row["engine_id"]) != self._engine_id
                    or str(row["runtime_profile_sha256"])
                    != self._runtime_profile_sha256
                ):
                    raise ScienceShadowStoreError(
                        "science shadow schema/profile is incompatible"
                    )
            database.executescript(
                """
                CREATE TABLE IF NOT EXISTS science_shadow_schema(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    schema_version INTEGER NOT NULL,
                    engine_id TEXT NOT NULL,
                    runtime_profile_sha256 TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_shadow_receipts(
                    sequence INTEGER PRIMARY KEY,
                    receipt_sha256 TEXT NOT NULL UNIQUE,
                    engine_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL,
                    binding_sha256 TEXT NOT NULL UNIQUE,
                    canonical_run_id TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    input_state_revision INTEGER NOT NULL,
                    next_state_revision INTEGER NOT NULL,
                    provenance_chain_sha256 TEXT,
                    receipt_json BLOB NOT NULL,
                    UNIQUE(engine_id, attempt_id)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS
                    science_shadow_one_advanced_run
                ON science_shadow_receipts(engine_id, canonical_run_id)
                WHERE outcome='advanced';
                CREATE INDEX IF NOT EXISTS science_shadow_receipt_revision
                ON science_shadow_receipts(engine_id,next_state_revision,sequence);
                CREATE TABLE IF NOT EXISTS science_shadow_head(
                    engine_id TEXT PRIMARY KEY,
                    receipt_sha256 TEXT NOT NULL UNIQUE,
                    revision INTEGER NOT NULL,
                    provenance_chain_sha256 TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_shadow_retained_inputs(
                    receipt_sha256 TEXT PRIMARY KEY REFERENCES
                        science_shadow_receipts(receipt_sha256) ON DELETE CASCADE,
                    revision INTEGER NOT NULL UNIQUE,
                    retained_input_sha256 TEXT NOT NULL UNIQUE,
                    uncompressed_bytes INTEGER NOT NULL,
                    compressed_bytes INTEGER NOT NULL,
                    input_zlib BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_shadow_checkpoint(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    checkpoint_sha256 TEXT NOT NULL UNIQUE,
                    revision INTEGER NOT NULL,
                    advanced_receipt_sha256 TEXT NOT NULL,
                    provenance_chain_sha256 TEXT NOT NULL,
                    state_sha256 TEXT NOT NULL,
                    state_uncompressed_bytes INTEGER NOT NULL,
                    state_compressed_bytes INTEGER NOT NULL,
                    checkpoint_json BLOB NOT NULL,
                    receipt_json BLOB NOT NULL,
                    state_zlib BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_shadow_retention(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    next_sequence INTEGER NOT NULL,
                    expired_filter BLOB NOT NULL,
                    expired_memberships INTEGER NOT NULL,
                    last_pruned_sequence INTEGER NOT NULL,
                    retention_sha256 TEXT NOT NULL
                );
                """
            )
            try:
                database.execute("BEGIN IMMEDIATE")
                database.execute(
                    "INSERT OR IGNORE INTO science_shadow_schema VALUES(1,?,?,?)",
                    (
                        SCIENCE_SHADOW_SCHEMA_VERSION,
                        self._engine_id,
                        self._runtime_profile_sha256,
                    ),
                )
                zero_filter = bytes(self._limits.expired_filter_bytes)
                retention_sha = self._retention_checksum(
                    next_sequence=1,
                    expired_filter=zero_filter,
                    expired_memberships=0,
                    last_pruned_sequence=0,
                )
                database.execute(
                    "INSERT OR IGNORE INTO science_shadow_retention "
                    "VALUES(1,?,?,?,?,?)",
                    (1, sqlite3.Binary(zero_filter), 0, 0, retention_sha),
                )
                row = database.execute(
                    "SELECT schema_version,engine_id,runtime_profile_sha256 "
                    "FROM science_shadow_schema WHERE singleton=1"
                ).fetchone()
                if row is None or (
                    int(row["schema_version"]) != SCIENCE_SHADOW_SCHEMA_VERSION
                    or str(row["engine_id"]) != self._engine_id
                    or str(row["runtime_profile_sha256"])
                    != self._runtime_profile_sha256
                ):
                    raise ScienceShadowStoreError(
                        "science shadow schema/profile is incompatible"
                    )
                self._read_retention(database)
                forbidden = database.execute(
                    "SELECT type,name FROM sqlite_master "
                    "WHERE type IN ('trigger','view')"
                ).fetchall()
                if forbidden:
                    raise ScienceShadowStoreError(
                        "science shadow database contains executable schema objects"
                    )
                database.commit()
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise

    def _read_retention(
        self,
        database: sqlite3.Connection,
    ) -> tuple[int, bytes, int, int]:
        row = database.execute(
            "SELECT * FROM science_shadow_retention WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise ScienceShadowStoreError("science shadow retention state is missing")
        body = row["expired_filter"]
        if not isinstance(body, bytes) or len(body) != self._limits.expired_filter_bytes:
            raise ScienceShadowStoreError("expired-membership filter size differs")
        next_sequence = _bounded_count(
            int(row["next_sequence"]), "next receipt sequence"
        )
        memberships = _bounded_count(
            int(row["expired_memberships"]),
            "expired memberships",
            self._limits.max_expired_memberships,
        )
        last_pruned = _bounded_count(
            int(row["last_pruned_sequence"]), "last pruned sequence"
        )
        if next_sequence < 1 or last_pruned >= next_sequence:
            raise ScienceShadowStoreError("retention sequence state is invalid")
        expected = self._retention_checksum(
            next_sequence=next_sequence,
            expired_filter=body,
            expired_memberships=memberships,
            last_pruned_sequence=last_pruned,
        )
        if str(row["retention_sha256"]) != expected:
            raise ScienceShadowStoreError("retention state checksum differs")
        return next_sequence, body, memberships, last_pruned

    def _write_retention(
        self,
        database: sqlite3.Connection,
        *,
        next_sequence: int,
        expired_filter: bytes,
        expired_memberships: int,
        last_pruned_sequence: int,
    ) -> None:
        checksum = self._retention_checksum(
            next_sequence=next_sequence,
            expired_filter=expired_filter,
            expired_memberships=expired_memberships,
            last_pruned_sequence=last_pruned_sequence,
        )
        changed = database.execute(
            "UPDATE science_shadow_retention SET next_sequence=?,"
            "expired_filter=?,expired_memberships=?,last_pruned_sequence=?,"
            "retention_sha256=? WHERE singleton=1",
            (
                next_sequence,
                sqlite3.Binary(expired_filter),
                expired_memberships,
                last_pruned_sequence,
                checksum,
            ),
        ).rowcount
        if changed != 1:
            raise ScienceShadowStoreError("retention state compare-and-swap failed")

    def _receipt_from_blob(self, raw: object) -> ScienceShadowReceipt:
        if not isinstance(raw, bytes) or not 1 <= len(raw) <= self._limits.max_receipt_bytes:
            raise ScienceShadowStoreError("science shadow receipt payload is invalid")
        try:
            value = json.loads(raw.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ScienceShadowStoreError("science shadow receipt JSON is invalid") from exc
        if not isinstance(value, dict) or _canonical_json(value) != raw:
            raise ScienceShadowStoreError("science shadow receipt is not canonical JSON")
        try:
            receipt = ScienceShadowReceipt.from_mapping(value)
        except (KeyError, TypeError, ValueError) as exc:
            raise ScienceShadowStoreError("science shadow receipt failed validation") from exc
        if receipt.to_mapping() != value:
            raise ScienceShadowStoreError(
                "science shadow receipt contains unknown or non-exact fields"
            )
        return receipt

    def _from_row(self, row: sqlite3.Row) -> ScienceShadowReceipt:
        receipt = self._receipt_from_blob(row["receipt_json"])
        chain = row["provenance_chain_sha256"]
        if (
            receipt.receipt_sha256 != str(row["receipt_sha256"])
            or receipt.engine_id != str(row["engine_id"])
            or receipt.attempt_id != str(row["attempt_id"])
            or receipt.request_sha256 != str(row["request_sha256"])
            or receipt.binding_sha256 != str(row["binding_sha256"])
            or receipt.canonical_run_id != str(row["canonical_run_id"])
            or receipt.outcome.value != str(row["outcome"])
            or receipt.input_state_revision != int(row["input_state_revision"])
            or receipt.next_state_revision != int(row["next_state_revision"])
            or receipt.engine_id != self._engine_id
            or receipt.runtime_profile_sha256 != self._runtime_profile_sha256
            or (
                receipt.outcome is ScienceShadowOutcome.ADVANCED
                and _SHA256_RE.fullmatch(str(chain)) is None
            )
            or (
                receipt.outcome is not ScienceShadowOutcome.ADVANCED
                and chain is not None
            )
        ):
            raise ScienceShadowStoreError("science shadow row and receipt differ")
        return receipt

    def _known_expired(
        self,
        database: sqlite3.Connection,
        *,
        kind: str,
        identifier: str,
    ) -> bool:
        _next, body, _memberships, _last = self._read_retention(database)
        return _expired_filter_contains(
            body,
            _expired_tag(kind, self._engine_id, identifier),
        )

    def load_attempt(
        self,
        attempt_id: str,
        *,
        request_sha256: str | None = None,
    ) -> ScienceShadowReceipt | None:
        attempt = _require_safe_id(attempt_id, "attempt_id")
        expected = (
            None
            if request_sha256 is None
            else _require_sha(request_sha256, "request_sha256")
        )
        with self._lock, closing(self._connect()) as database:
            row = database.execute(
                "SELECT * FROM science_shadow_receipts "
                "WHERE engine_id=? AND attempt_id=?",
                (self._engine_id, attempt),
            ).fetchone()
            if row is None:
                if self._known_expired(database, kind="attempt", identifier=attempt):
                    raise ScienceShadowWindowExpired(
                        "window_expired: attempt receipt is outside retention"
                    )
                return None
        receipt = self._from_row(row)
        if expected is not None and receipt.request_sha256 != expected:
            raise ScienceShadowStoreConflict(
                "attempt ID was already bound to another request"
            )
        return receipt

    def load_advanced_run(self, run_id: str) -> ScienceShadowReceipt | None:
        if not isinstance(run_id, str) or _RUN_ID_RE.fullmatch(run_id) is None:
            raise ValueError("run_id is invalid")
        with self._lock, closing(self._connect()) as database:
            row = database.execute(
                "SELECT * FROM science_shadow_receipts WHERE engine_id=? "
                "AND canonical_run_id=? AND outcome='advanced'",
                (self._engine_id, run_id),
            ).fetchone()
            if row is None:
                if self._known_expired(database, kind="run", identifier=run_id):
                    raise ScienceShadowWindowExpired(
                        "window_expired: advanced run is outside retention"
                    )
                return None
        return self._from_row(row)

    def _retained_from_row(self, row: sqlite3.Row) -> ScienceShadowRetainedInput:
        compressed = row["input_zlib"]
        raw = _bounded_decompress(
            compressed,
            expected_size=int(row["uncompressed_bytes"]),
            maximum_size=self._limits.max_retained_input_bytes,
            label="retained input",
        )
        if len(compressed) != int(row["compressed_bytes"]):
            raise ScienceShadowStoreError("retained input compressed size differs")
        try:
            value = json.loads(raw.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ScienceShadowStoreError("retained input JSON is invalid") from exc
        if not isinstance(value, dict) or _canonical_json(value) != raw:
            raise ScienceShadowStoreError("retained input is not canonical JSON")
        retained = ScienceShadowRetainedInput.from_mapping(value)
        if (
            retained.receipt_sha256 != str(row["receipt_sha256"])
            or retained.retained_input_sha256 != str(row["retained_input_sha256"])
        ):
            raise ScienceShadowStoreError("retained input row differs")
        return retained

    def _checkpoint_from_database(
        self,
        database: sqlite3.Connection,
    ) -> tuple[ScienceShadowStateCheckpoint, ScienceShadowReceipt] | None:
        row = database.execute(
            "SELECT * FROM science_shadow_checkpoint WHERE singleton=1"
        ).fetchone()
        if row is None:
            return None
        raw_metadata = row["checkpoint_json"]
        if not isinstance(raw_metadata, bytes) or not 1 <= len(raw_metadata) <= 16_384:
            raise ScienceShadowStoreError("checkpoint metadata bounds are invalid")
        try:
            value = json.loads(raw_metadata.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ScienceShadowStoreError("checkpoint metadata JSON is invalid") from exc
        if not isinstance(value, dict) or _canonical_json(value) != raw_metadata:
            raise ScienceShadowStoreError("checkpoint metadata is not canonical JSON")
        expected_keys = {
            "advanced_receipt_json_sha256",
            "advanced_receipt_sha256",
            "checkpoint_sha256",
            "contract",
            "engine_id",
            "previous_checkpoint_sha256",
            "provenance_chain_sha256",
            "revision",
            "runtime_profile_sha256",
            "state_compressed_bytes",
            "state_compressed_sha256",
            "state_sha256",
            "state_uncompressed_bytes",
        }
        if set(value) != expected_keys or value.get("contract") != SCIENCE_SHADOW_CHECKPOINT_CONTRACT:
            raise ScienceShadowStoreError("checkpoint metadata schema is not exact")
        state_raw = _bounded_decompress(
            row["state_zlib"],
            expected_size=int(value["state_uncompressed_bytes"]),
            maximum_size=self._limits.max_checkpoint_bytes,
            label="state checkpoint",
        )
        if (
            len(row["state_zlib"]) != int(value["state_compressed_bytes"])
            or hashlib.sha256(state_raw).hexdigest() != str(value["state_sha256"])
            or hashlib.sha256(row["state_zlib"]).hexdigest()
            != str(value["state_compressed_sha256"])
        ):
            raise ScienceShadowStoreError("checkpoint state checksum differs")
        try:
            state_value = json.loads(state_raw.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ScienceShadowStoreError("checkpoint state JSON is invalid") from exc
        if _canonical_json(state_value) != state_raw:
            raise ScienceShadowStoreError("checkpoint state is not canonical JSON")
        state = _decode_state_value(state_value)
        if not isinstance(state, CellTrackEngineState):
            raise ScienceShadowStoreError("checkpoint root is not CellTrackEngineState")
        try:
            checkpoint = ScienceShadowStateCheckpoint(
                checkpoint_sha256=str(value["checkpoint_sha256"]),
                previous_checkpoint_sha256=value["previous_checkpoint_sha256"],
                engine_id=str(value["engine_id"]),
                runtime_profile_sha256=str(value["runtime_profile_sha256"]),
                revision=int(value["revision"]),
                advanced_receipt_sha256=str(value["advanced_receipt_sha256"]),
                advanced_receipt_json_sha256=str(
                    value["advanced_receipt_json_sha256"]
                ),
                provenance_chain_sha256=str(value["provenance_chain_sha256"]),
                state_sha256=str(value["state_sha256"]),
                state_compressed_sha256=str(value["state_compressed_sha256"]),
                state_uncompressed_bytes=int(value["state_uncompressed_bytes"]),
                state_compressed_bytes=int(value["state_compressed_bytes"]),
                state=state,
            )
        except (TypeError, ValueError) as exc:
            raise ScienceShadowStoreError("checkpoint failed validation") from exc
        if (
            checkpoint.checkpoint_sha256 != str(row["checkpoint_sha256"])
            or checkpoint.revision != int(row["revision"])
            or checkpoint.advanced_receipt_sha256
            != str(row["advanced_receipt_sha256"])
            or checkpoint.provenance_chain_sha256
            != str(row["provenance_chain_sha256"])
            or checkpoint.state_sha256 != str(row["state_sha256"])
            or checkpoint.state_uncompressed_bytes
            != int(row["state_uncompressed_bytes"])
            or checkpoint.state_compressed_bytes
            != int(row["state_compressed_bytes"])
            or checkpoint.engine_id != self._engine_id
            or checkpoint.runtime_profile_sha256 != self._runtime_profile_sha256
        ):
            raise ScienceShadowStoreError("checkpoint row and metadata differ")
        receipt_raw = row["receipt_json"]
        receipt = self._receipt_from_blob(receipt_raw)
        if (
            hashlib.sha256(receipt_raw).hexdigest()
            != checkpoint.advanced_receipt_json_sha256
            or receipt.receipt_sha256 != checkpoint.advanced_receipt_sha256
            or receipt.outcome is not ScienceShadowOutcome.ADVANCED
            or receipt.next_state_revision != checkpoint.revision
        ):
            raise ScienceShadowStoreError("checkpoint receipt binding differs")
        return checkpoint, receipt

    def recovery_snapshot(
        self,
        *,
        expected_policy_fingerprint: str,
    ) -> _ScienceShadowRecoverySnapshot:
        policy_sha = _require_sha(
            expected_policy_fingerprint,
            "expected policy fingerprint",
        )
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN")
                self._read_retention(database)
                all_rows = database.execute(
                    "SELECT * FROM science_shadow_receipts ORDER BY sequence"
                ).fetchall()
                retained_receipts: dict[str, ScienceShadowReceipt] = {}
                for row in all_rows:
                    retained_receipt = self._from_row(row)
                    retained_receipts[retained_receipt.receipt_sha256] = (
                        retained_receipt
                    )
                head = database.execute(
                    "SELECT * FROM science_shadow_head WHERE engine_id=?",
                    (self._engine_id,),
                ).fetchone()
                checkpoint_pair = self._checkpoint_from_database(database)
                if head is None:
                    if checkpoint_pair is not None:
                        raise ScienceShadowStoreError("checkpoint exists without a head")
                    advanced_count = sum(
                        item.outcome is ScienceShadowOutcome.ADVANCED
                        for item in retained_receipts.values()
                    )
                    if advanced_count:
                        raise ScienceShadowStoreError("advanced receipt exists without a head")
                    database.commit()
                    return _ScienceShadowRecoverySnapshot(
                        checkpoint=None,
                        checkpoint_receipt=None,
                        tail=(),
                        head_receipt_sha256=None,
                        head_revision=0,
                        head_provenance_chain_sha256=None,
                    )
                head_receipt_sha = _require_sha(
                    str(head["receipt_sha256"]), "head receipt"
                )
                head_revision = _bounded_count(
                    int(head["revision"]), "head revision", 2**31 - 1
                )
                head_chain = _require_sha(
                    str(head["provenance_chain_sha256"]), "head provenance chain"
                )
                if head_revision < 1:
                    raise ScienceShadowStoreError("non-empty head revision is invalid")
                checkpoint: ScienceShadowStateCheckpoint | None = None
                checkpoint_receipt: ScienceShadowReceipt | None = None
                revision = 0
                previous_receipt: str | None = None
                chain: str | None = None
                if checkpoint_pair is not None:
                    checkpoint, checkpoint_receipt = checkpoint_pair
                    if (
                        checkpoint.revision % self._limits.checkpoint_interval_frames != 0
                        or checkpoint.revision > head_revision
                        or checkpoint.state.policy_fingerprint != policy_sha
                    ):
                        raise ScienceShadowStoreError(
                            "checkpoint cadence/head/policy binding differs"
                        )
                    revision = checkpoint.revision
                    previous_receipt = checkpoint.advanced_receipt_sha256
                    chain = checkpoint.provenance_chain_sha256
                    retained_copy = retained_receipts.get(previous_receipt)
                    if retained_copy is not None and retained_copy != checkpoint_receipt:
                        raise ScienceShadowStoreError(
                            "retained checkpoint receipt differs from its copy"
                        )
                elif head_revision >= self._limits.checkpoint_interval_frames:
                    raise ScienceShadowStoreError(
                        "durable checkpoint is missing beyond first interval"
                    )
                tail_rows = database.execute(
                    "SELECT * FROM science_shadow_receipts WHERE engine_id=? "
                    "AND outcome='advanced' AND next_state_revision>? "
                    "ORDER BY next_state_revision",
                    (self._engine_id, revision),
                ).fetchall()
                if len(tail_rows) > self._limits.max_replay_frames:
                    raise ScienceShadowRecoveryError(
                        "retained advanced tail exceeds replay capacity"
                    )
                tail: list[_ScienceShadowRecoveryTail] = []
                tail_input_ids: set[str] = set()
                for row in tail_rows:
                    receipt = self._from_row(row)
                    if (
                        receipt.input_state_revision != revision
                        or receipt.next_state_revision != revision + 1
                        or receipt.previous_advanced_receipt_sha256 != previous_receipt
                    ):
                        raise ScienceShadowStoreError(
                            "retained advanced receipt tail is discontinuous"
                        )
                    expected_chain = _provenance_chain_sha256(chain, receipt)
                    if str(row["provenance_chain_sha256"]) != expected_chain:
                        raise ScienceShadowStoreError(
                            "retained provenance chain checksum differs"
                        )
                    input_row = database.execute(
                        "SELECT * FROM science_shadow_retained_inputs "
                        "WHERE receipt_sha256=?",
                        (receipt.receipt_sha256,),
                    ).fetchone()
                    if input_row is None or int(input_row["revision"]) != receipt.next_state_revision:
                        raise ScienceShadowStoreError(
                            "retained replay input is missing or revision-bound incorrectly"
                        )
                    retained = self._retained_from_row(input_row)
                    if (
                        retained.receipt_sha256 != receipt.receipt_sha256
                        or receipt.provenance is None
                        or retained.provenance_sha256
                        != _sha_mapping(receipt.provenance.to_mapping())
                        or retained.frame.frame_id != receipt.provenance.normalized_frame_id
                    ):
                        raise ScienceShadowStoreError(
                            "retained replay input provenance binding differs"
                        )
                    tail.append(_ScienceShadowRecoveryTail(
                        receipt=receipt,
                        retained_input=retained,
                        provenance_chain_sha256=expected_chain,
                    ))
                    tail_input_ids.add(receipt.receipt_sha256)
                    revision += 1
                    previous_receipt = receipt.receipt_sha256
                    chain = expected_chain
                all_inputs = {
                    str(row[0])
                    for row in database.execute(
                        "SELECT receipt_sha256 FROM science_shadow_retained_inputs"
                    )
                }
                if all_inputs != tail_input_ids:
                    raise ScienceShadowStoreError(
                        "retained replay-input set is not exactly the contiguous tail"
                    )
                if (
                    revision != head_revision
                    or previous_receipt != head_receipt_sha
                    or chain != head_chain
                ):
                    raise ScienceShadowStoreError(
                        "checkpoint/tail and durable state head differ"
                    )
                database.commit()
                return _ScienceShadowRecoverySnapshot(
                    checkpoint=checkpoint,
                    checkpoint_receipt=checkpoint_receipt,
                    tail=tuple(tail),
                    head_receipt_sha256=head_receipt_sha,
                    head_revision=head_revision,
                    head_provenance_chain_sha256=head_chain,
                )
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise

    def advanced_receipts(self) -> tuple[ScienceShadowReceipt, ...]:
        """Return only currently retained advanced receipts in revision order."""

        with self._lock, closing(self._connect()) as database:
            rows = database.execute(
                "SELECT * FROM science_shadow_receipts WHERE engine_id=? "
                "AND outcome='advanced' ORDER BY next_state_revision",
                (self._engine_id,),
            ).fetchall()
        return tuple(self._from_row(row) for row in rows)

    @staticmethod
    def _checkpoint_value(
        *,
        previous_checkpoint_sha256: str | None,
        engine_id: str,
        runtime_profile_sha256: str,
        receipt: ScienceShadowReceipt,
        receipt_body: bytes,
        provenance_chain_sha256: str,
        state: CellTrackEngineState,
        state_raw: bytes,
        state_compressed: bytes,
    ) -> ScienceShadowStateCheckpoint:
        values = {
            "previous_checkpoint_sha256": previous_checkpoint_sha256,
            "engine_id": engine_id,
            "runtime_profile_sha256": runtime_profile_sha256,
            "revision": receipt.next_state_revision,
            "advanced_receipt_sha256": receipt.receipt_sha256,
            "advanced_receipt_json_sha256": hashlib.sha256(receipt_body).hexdigest(),
            "provenance_chain_sha256": provenance_chain_sha256,
            "state_sha256": hashlib.sha256(state_raw).hexdigest(),
            "state_compressed_sha256": hashlib.sha256(state_compressed).hexdigest(),
            "state_uncompressed_bytes": len(state_raw),
            "state_compressed_bytes": len(state_compressed),
            "state": state,
        }
        temporary = object.__new__(ScienceShadowStateCheckpoint)
        for name, value in values.items():
            object.__setattr__(temporary, name, value)
        object.__setattr__(temporary, "checkpoint_sha256", "0" * 64)
        return ScienceShadowStateCheckpoint(
            checkpoint_sha256=_sha_mapping(temporary.payload_mapping()),
            **values,
        )

    def record(
        self,
        receipt: ScienceShadowReceipt,
        *,
        expected_previous_advanced_receipt_sha256: str | None,
        expected_input_state_revision: int,
        advanced_state: CellTrackEngineState | None = None,
        evidence: ShadowEvidenceFrame | None = None,
    ) -> ScienceShadowReceipt:
        if not isinstance(receipt, ScienceShadowReceipt):
            raise TypeError("receipt must be ScienceShadowReceipt")
        if (
            receipt.engine_id != self._engine_id
            or receipt.runtime_profile_sha256 != self._runtime_profile_sha256
            or receipt.previous_advanced_receipt_sha256
            != expected_previous_advanced_receipt_sha256
            or receipt.input_state_revision != expected_input_state_revision
        ):
            raise ScienceShadowStoreConflict("receipt differs from expected state head")
        body = _canonical_json(receipt.to_mapping())
        if len(body) > self._limits.max_receipt_bytes:
            raise ScienceShadowCapacityError("science shadow receipt byte bound exceeded")
        retained: ScienceShadowRetainedInput | None = None
        input_raw: bytes | None = None
        input_compressed: bytes | None = None
        checkpoint_state_raw: bytes | None = None
        checkpoint_state_compressed: bytes | None = None
        checkpoint_due = False
        if receipt.outcome is ScienceShadowOutcome.ADVANCED:
            if (
                not isinstance(advanced_state, CellTrackEngineState)
                or not isinstance(evidence, ShadowEvidenceFrame)
                or advanced_state.revision != receipt.next_state_revision
                or advanced_state.engine_id != self._engine_id
                or advanced_state.last_frame != evidence.normalized_frame
            ):
                raise ScienceShadowStoreConflict(
                    "advanced receipt requires its exact next state and evidence"
                )
            retained = _build_retained_input(receipt, evidence)
            input_raw = _canonical_json(retained.to_mapping())
            if len(input_raw) > self._limits.max_retained_input_bytes:
                raise ScienceShadowRetainedInputCapacityError(
                    "retained replay input byte bound exceeded"
                )
            input_compressed = zlib.compress(input_raw, level=9)
            if len(input_compressed) > self._limits.max_retained_input_bytes:
                raise ScienceShadowRetainedInputCapacityError(
                    "compressed retained replay input byte bound exceeded"
                )
            checkpoint_due = (
                receipt.next_state_revision
                % self._limits.checkpoint_interval_frames
                == 0
            )
            if checkpoint_due:
                checkpoint_state_raw = _canonical_json(
                    _encode_state_value(advanced_state)
                )
                if len(checkpoint_state_raw) > self._limits.max_checkpoint_bytes:
                    raise ScienceShadowCheckpointCapacityError(
                        "state checkpoint byte bound exceeded"
                    )
                checkpoint_state_compressed = zlib.compress(
                    checkpoint_state_raw,
                    level=9,
                )
                if len(checkpoint_state_compressed) > self._limits.max_checkpoint_bytes:
                    raise ScienceShadowCheckpointCapacityError(
                        "compressed state checkpoint byte bound exceeded"
                    )
        elif advanced_state is not None or evidence is not None:
            raise ScienceShadowStoreConflict(
                "non-advanced receipt cannot carry checkpoint state/input"
            )

        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                next_sequence, filter_body, memberships, last_pruned = (
                    self._read_retention(database)
                )
                existing_attempt = database.execute(
                    "SELECT * FROM science_shadow_receipts "
                    "WHERE engine_id=? AND attempt_id=?",
                    (self._engine_id, receipt.attempt_id),
                ).fetchone()
                if existing_attempt is not None:
                    existing = self._from_row(existing_attempt)
                    if (
                        existing.request_sha256 != receipt.request_sha256
                        or existing.receipt_sha256 != receipt.receipt_sha256
                    ):
                        raise ScienceShadowStoreConflict(
                            "idempotent science shadow attempt conflicts"
                        )
                    database.commit()
                    return existing
                if _expired_filter_contains(
                    filter_body,
                    _expired_tag("attempt", self._engine_id, receipt.attempt_id),
                ):
                    raise ScienceShadowWindowExpired(
                        "window_expired: attempt receipt is outside retention"
                    )
                if receipt.outcome is ScienceShadowOutcome.ADVANCED:
                    prior_run = database.execute(
                        "SELECT * FROM science_shadow_receipts WHERE engine_id=? "
                        "AND canonical_run_id=? AND outcome='advanced'",
                        (self._engine_id, receipt.canonical_run_id),
                    ).fetchone()
                    if prior_run is not None:
                        database.commit()
                        return self._from_row(prior_run)
                    if _expired_filter_contains(
                        filter_body,
                        _expired_tag(
                            "run", self._engine_id, receipt.canonical_run_id
                        ),
                    ):
                        raise ScienceShadowWindowExpired(
                            "window_expired: advanced run is outside retention"
                        )
                head = database.execute(
                    "SELECT * FROM science_shadow_head WHERE engine_id=?",
                    (self._engine_id,),
                ).fetchone()
                actual_previous = None if head is None else str(head["receipt_sha256"])
                actual_revision = 0 if head is None else int(head["revision"])
                previous_chain = (
                    None if head is None else str(head["provenance_chain_sha256"])
                )
                if (
                    actual_previous != expected_previous_advanced_receipt_sha256
                    or actual_revision != expected_input_state_revision
                ):
                    raise ScienceShadowStoreConflict(
                        "science shadow state head changed concurrently"
                    )
                provenance_chain = (
                    None
                    if receipt.outcome is not ScienceShadowOutcome.ADVANCED
                    else _provenance_chain_sha256(previous_chain, receipt)
                )
                database.execute(
                    "INSERT INTO science_shadow_receipts VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        next_sequence,
                        receipt.receipt_sha256,
                        receipt.engine_id,
                        receipt.attempt_id,
                        receipt.request_sha256,
                        receipt.binding_sha256,
                        receipt.canonical_run_id,
                        receipt.outcome.value,
                        receipt.input_state_revision,
                        receipt.next_state_revision,
                        provenance_chain,
                        sqlite3.Binary(body),
                    ),
                )
                self._fault("record.after_receipt")
                if receipt.outcome is ScienceShadowOutcome.ADVANCED:
                    assert retained is not None
                    assert input_raw is not None and input_compressed is not None
                    database.execute(
                        "INSERT INTO science_shadow_retained_inputs "
                        "VALUES(?,?,?,?,?,?)",
                        (
                            receipt.receipt_sha256,
                            receipt.next_state_revision,
                            retained.retained_input_sha256,
                            len(input_raw),
                            len(input_compressed),
                            sqlite3.Binary(input_compressed),
                        ),
                    )
                    self._fault("record.after_input")
                    assert provenance_chain is not None
                    if head is None:
                        database.execute(
                            "INSERT INTO science_shadow_head VALUES(?,?,?,?)",
                            (
                                self._engine_id,
                                receipt.receipt_sha256,
                                receipt.next_state_revision,
                                provenance_chain,
                            ),
                        )
                    else:
                        changed = database.execute(
                            "UPDATE science_shadow_head SET receipt_sha256=?,"
                            "revision=?,provenance_chain_sha256=? WHERE engine_id=? "
                            "AND receipt_sha256=? AND revision=? "
                            "AND provenance_chain_sha256=?",
                            (
                                receipt.receipt_sha256,
                                receipt.next_state_revision,
                                provenance_chain,
                                self._engine_id,
                                actual_previous,
                                actual_revision,
                                previous_chain,
                            ),
                        ).rowcount
                        if changed != 1:
                            raise ScienceShadowStoreConflict(
                                "science shadow head compare-and-swap failed"
                            )
                    self._fault("record.after_head")

                checkpoint_revision = 0
                checkpoint_row = database.execute(
                    "SELECT checkpoint_sha256,revision FROM "
                    "science_shadow_checkpoint WHERE singleton=1"
                ).fetchone()
                if checkpoint_row is not None:
                    checkpoint_revision = int(checkpoint_row["revision"])
                if checkpoint_due:
                    assert advanced_state is not None
                    assert checkpoint_state_raw is not None
                    assert checkpoint_state_compressed is not None
                    assert provenance_chain is not None
                    previous_checkpoint = (
                        None
                        if checkpoint_row is None
                        else str(checkpoint_row["checkpoint_sha256"])
                    )
                    checkpoint = self._checkpoint_value(
                        previous_checkpoint_sha256=previous_checkpoint,
                        engine_id=self._engine_id,
                        runtime_profile_sha256=self._runtime_profile_sha256,
                        receipt=receipt,
                        receipt_body=body,
                        provenance_chain_sha256=provenance_chain,
                        state=advanced_state,
                        state_raw=checkpoint_state_raw,
                        state_compressed=checkpoint_state_compressed,
                    )
                    checkpoint_document = checkpoint.payload_mapping()
                    checkpoint_document["checkpoint_sha256"] = (
                        checkpoint.checkpoint_sha256
                    )
                    checkpoint_body = _canonical_json(checkpoint_document)
                    database.execute(
                        "INSERT OR REPLACE INTO science_shadow_checkpoint "
                        "VALUES(1,?,?,?,?,?,?,?,?,?,?)",
                        (
                            checkpoint.checkpoint_sha256,
                            checkpoint.revision,
                            checkpoint.advanced_receipt_sha256,
                            checkpoint.provenance_chain_sha256,
                            checkpoint.state_sha256,
                            checkpoint.state_uncompressed_bytes,
                            checkpoint.state_compressed_bytes,
                            sqlite3.Binary(checkpoint_body),
                            sqlite3.Binary(body),
                            sqlite3.Binary(checkpoint_state_compressed),
                        ),
                    )
                    checkpoint_revision = checkpoint.revision
                    self._fault("record.after_checkpoint_blob")
                    database.execute(
                        "DELETE FROM science_shadow_retained_inputs "
                        "WHERE revision<=?",
                        (checkpoint_revision,),
                    )
                    self._fault("record.after_prune_input")

                total = int(
                    database.execute(
                        "SELECT count(*) FROM science_shadow_receipts"
                    ).fetchone()[0]
                )
                prune_count = max(0, total - self._limits.max_receipts)
                prune_rows: list[sqlite3.Row] = []
                if prune_count:
                    eligible = database.execute(
                        "SELECT sequence,attempt_id,canonical_run_id,outcome "
                        "FROM science_shadow_receipts WHERE NOT "
                        "(outcome='advanced' AND next_state_revision>?) "
                        "ORDER BY sequence LIMIT ?",
                        (checkpoint_revision, prune_count),
                    ).fetchall()
                    if len(eligible) != prune_count:
                        raise ScienceShadowCapacityError(
                            "receipt retention cannot prune an uncheckpointed tail"
                        )
                    prune_rows = list(eligible)
                mutable_filter = bytearray(filter_body)
                membership_increment = sum(
                    1 + (1 if str(row["outcome"]) == "advanced" else 0)
                    for row in prune_rows
                )
                if memberships + membership_increment > self._limits.max_expired_memberships:
                    raise ScienceShadowCapacityError(
                        "expired-membership lifetime capacity exhausted"
                    )
                for row in prune_rows:
                    _expired_filter_add(
                        mutable_filter,
                        _expired_tag(
                            "attempt", self._engine_id, str(row["attempt_id"])
                        ),
                    )
                    if str(row["outcome"]) == "advanced":
                        _expired_filter_add(
                            mutable_filter,
                            _expired_tag(
                                "run",
                                self._engine_id,
                                str(row["canonical_run_id"]),
                            ),
                        )
                    last_pruned = max(last_pruned, int(row["sequence"]))
                self._write_retention(
                    database,
                    next_sequence=next_sequence + 1,
                    expired_filter=bytes(mutable_filter),
                    expired_memberships=memberships + membership_increment,
                    last_pruned_sequence=last_pruned,
                )
                self._fault("record.after_expired_filter")
                for row in prune_rows:
                    database.execute(
                        "DELETE FROM science_shadow_receipts WHERE sequence=?",
                        (int(row["sequence"]),),
                    )
                if prune_rows:
                    self._fault("record.after_prune_receipt")
                page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
                page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
                if page_count * page_size > self._limits.max_database_bytes:
                    raise ScienceShadowCapacityError(
                        "science shadow database byte bound exceeded"
                    )
                self._fault("record.before_commit")
                database.commit()
            except sqlite3.OperationalError as exc:
                if database.in_transaction:
                    database.rollback()
                if "full" in str(exc).lower():
                    raise ScienceShadowCapacityError(
                        "science shadow database is full"
                    ) from exc
                raise ScienceShadowStoreError(
                    "science shadow SQLite operation failed"
                ) from exc
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        return receipt

    def stats(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as database:
            counts = {
                str(row[0]): int(row[1])
                for row in database.execute(
                    "SELECT outcome,count(*) FROM science_shadow_receipts "
                    "GROUP BY outcome"
                )
            }
            total = int(
                database.execute(
                    "SELECT count(*) FROM science_shadow_receipts"
                ).fetchone()[0]
            )
            head = database.execute(
                "SELECT revision FROM science_shadow_head WHERE engine_id=?",
                (self._engine_id,),
            ).fetchone()
            checkpoint = database.execute(
                "SELECT revision,state_uncompressed_bytes,state_compressed_bytes "
                "FROM science_shadow_checkpoint WHERE singleton=1"
            ).fetchone()
            retained_inputs = int(
                database.execute(
                    "SELECT count(*) FROM science_shadow_retained_inputs"
                ).fetchone()[0]
            )
            _next, _filter, memberships, _last = self._read_retention(database)
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
            freelist = int(database.execute("PRAGMA freelist_count").fetchone()[0])
        head_revision = 0 if head is None else int(head[0])
        checkpoint_revision = 0 if checkpoint is None else int(checkpoint[0])
        result = {
            "checkpoint_compressed_bytes": 0 if checkpoint is None else int(checkpoint[2]),
            "checkpoint_revision": checkpoint_revision,
            "checkpoint_uncompressed_bytes": 0 if checkpoint is None else int(checkpoint[1]),
            "contract": SCIENCE_SHADOW_RECEIPT_CONTRACT,
            "database_file_bytes": page_count * page_size,
            "database_used_bytes": (page_count - freelist) * page_size,
            "expired_filter_bytes": self._limits.expired_filter_bytes,
            "expired_memberships": memberships,
            "head_revision": head_revision,
            "max_database_bytes": self._limits.max_database_bytes,
            "max_receipts": self._limits.max_receipts,
            "outcomes": {
                item.value: counts.get(item.value, 0) for item in ScienceShadowOutcome
            },
            "receipt_count": total,
            "replay_tail_frames": retained_inputs,
        }
        if (
            retained_inputs != head_revision - checkpoint_revision
            or len(_canonical_json(result)) > MAX_DIAGNOSTIC_BYTES
        ):
            raise ScienceShadowStoreError("science shadow diagnostics are inconsistent")
        return result


@dataclass(frozen=True, slots=True)
class ScienceShadowRunResult:
    receipt: ScienceShadowReceipt
    duplicate: bool
    evidence: ShadowEvidenceFrame | None
    transition: CellTrackTransitionResult | None

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, ScienceShadowReceipt):
            raise TypeError("run result receipt is invalid")
        if type(self.duplicate) is not bool:
            raise TypeError("run result duplicate flag is invalid")
        if self.duplicate and (self.evidence is not None or self.transition is not None):
            raise ValueError("duplicate replay must not expose recomputed arrays")
        if self.transition is not None and self.evidence is None:
            raise ValueError("transition without evidence is invalid")


class DurableScienceShadowOrchestrator:
    """Serialize, account and replay one exact worker-side shadow profile."""

    def __init__(
        self,
        *,
        engine_id: str,
        composer: ShadowEvidenceComposer,
        tracking_policy: CellTrackEnginePolicy,
        receipt_path: str | Path,
        limits: ScienceShadowLimits = ScienceShadowLimits(),
        monotonic: Callable[[], float] = time.monotonic,
        store_fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self._engine_id = _require_safe_id(engine_id, "engine_id")
        if not isinstance(composer, ShadowEvidenceComposer):
            raise TypeError("composer must implement ShadowEvidenceComposer")
        if not isinstance(tracking_policy, CellTrackEnginePolicy):
            raise TypeError("tracking_policy must be CellTrackEnginePolicy")
        if tracking_policy.detector != composer.detector_config:
            raise ValueError(
                "tracking detector must exactly equal the evidence detector"
            )
        if not isinstance(limits, ScienceShadowLimits):
            raise TypeError("limits must be ScienceShadowLimits")
        if (
            tracking_policy.multi_hypothesis_identity is not None
            and tracking_policy.multi_hypothesis_identity.maximum_derived_lineage_hypotheses
            > limits.max_lineage_hypotheses
        ):
            raise ValueError(
                "tracking derived-lineage cap exceeds durable receipt cap"
            )
        _require_sha(composer.profile_sha256, "composer profile_sha256")
        maximum_runtime = float(composer.maximum_runtime_seconds)
        if not math.isfinite(maximum_runtime) or maximum_runtime <= 0.0:
            raise ValueError("composer maximum runtime is invalid")
        self._composer = composer
        self._policy = tracking_policy
        self._limits = limits
        self._monotonic = monotonic
        self._runtime_profile_sha256 = _sha_mapping({
            "algorithm": SCIENCE_SHADOW_ORCHESTRATOR_ALGORITHM,
            "composer_profile_sha256": composer.profile_sha256,
            "limits_sha256": limits.fingerprint,
            "tracking_policy_sha256": tracking_policy.fingerprint,
        })
        self._store = ScienceShadowReceiptStore(
            receipt_path,
            engine_id=self._engine_id,
            runtime_profile_sha256=self._runtime_profile_sha256,
            limits=limits,
            fault_injector=store_fault_injector,
        )
        self._lock = threading.RLock()
        self._state: CellTrackEngineState | None = None
        self._head_receipt_sha256: str | None = None

    @property
    def runtime_profile_sha256(self) -> str:
        return self._runtime_profile_sha256

    @property
    def receipt_store(self) -> ScienceShadowReceiptStore:
        return self._store

    @property
    def maximum_runtime_seconds(self) -> float:
        return min(
            float(self._limits.max_runtime_seconds),
            float(self._composer.maximum_runtime_seconds),
        )

    def _validate_deadline(self, deadline_at: float) -> float:
        if (
            isinstance(deadline_at, bool)
            or not isinstance(deadline_at, (int, float))
            or not math.isfinite(float(deadline_at))
        ):
            raise ValueError("deadline_at must be an absolute finite monotonic time")
        value = float(deadline_at)
        now = float(self._monotonic())
        if not math.isfinite(now) or now >= value:
            raise ScienceShadowDeadlineError("science shadow deadline expired")
        maximum = self.maximum_runtime_seconds
        if value - now > maximum:
            raise ValueError("deadline exceeds the exact profile runtime bound")
        return value

    def _request_sha256(
        self,
        *,
        attempt_id: str,
        run_id: str,
        hypotheses: tuple[CellTrackLineageHypothesis, ...],
    ) -> str:
        return science_shadow_request_sha256(
            attempt_id=attempt_id,
            canonical_run_id=run_id,
            engine_id=self._engine_id,
            runtime_profile_sha256=self._runtime_profile_sha256,
            lineage_hypotheses=hypotheses,
            maximum_lineage_hypotheses=self._limits.max_lineage_hypotheses,
        )

    def _binding_sha256(
        self,
        *,
        request_sha256: str,
        state: CellTrackEngineState,
        previous_receipt_sha256: str | None,
    ) -> str:
        return _sha_mapping({
            "input_state_revision": state.revision,
            "previous_advanced_receipt_sha256": previous_receipt_sha256,
            "request_sha256": request_sha256,
            "tracking_policy_sha256": self._policy.fingerprint,
        })

    def _recover(self, deadline_at: float) -> None:
        if self._state is not None:
            return
        snapshot = self._store.recovery_snapshot(
            expected_policy_fingerprint=self._policy.fingerprint,
        )
        state = (
            create_cell_track_engine_state(self._engine_id, self._policy)
            if snapshot.checkpoint is None
            else snapshot.checkpoint.state
        )
        previous = (
            None
            if snapshot.checkpoint is None
            else snapshot.checkpoint.advanced_receipt_sha256
        )
        for tail_item in snapshot.tail:
            _check_deadline(deadline_at, self._monotonic)
            receipt = tail_item.receipt
            retained = tail_item.retained_input
            if (
                receipt.previous_advanced_receipt_sha256 != previous
                or receipt.input_state_revision != state.revision
                or receipt.provenance is None
                or retained.receipt_sha256 != receipt.receipt_sha256
                or retained.provenance_sha256
                != _sha_mapping(receipt.provenance.to_mapping())
            ):
                raise ScienceShadowRecoveryError(
                    "retained science provenance differs from durable receipt"
                )
            try:
                transition = advance_cell_track_engine(
                    state,
                    retained.frame,
                    self._policy,
                    lineage_hypotheses=receipt.lineage_hypotheses,
                )
            except Exception as exc:
                raise ScienceShadowRecoveryError(
                    "retained tracking input could not be replayed"
                ) from exc
            _check_deadline(deadline_at, self._monotonic)
            detection_sha = _sha_mapping(
                _encode_state_value(transition.detection_frame)
            )
            accounting = receipt.accounting
            if (
                transition.status != "advanced"
                or transition.transition_id != receipt.transition_id
                or transition.next_state.revision != receipt.next_state_revision
                or detection_sha != retained.detection_frame_sha256
                or transition.applied_lineage_hypothesis_ids
                != receipt.applied_lineage_hypothesis_ids
                or accounting.total_analysis_pixels
                != retained.frame.grid.width * retained.frame.grid.height
                or accounting.tracker_detection_objects
                != len(transition.detection_frame.detections)
                or accounting.input_tracks != len(state.ledger.tracks)
                or accounting.output_tracks != len(transition.next_state.ledger.tracks)
                or accounting.input_lifecycle_events
                != len(state.ledger.lineage_events)
                or accounting.output_lifecycle_events
                != len(transition.next_state.ledger.lineage_events)
                or accounting.candidate_edges != transition.candidate_edge_count
            ):
                raise ScienceShadowRecoveryError(
                    "replayed track transition differs from durable receipt"
                )
            state = transition.next_state
            previous = receipt.receipt_sha256
        self._state = state
        self._head_receipt_sha256 = snapshot.head_receipt_sha256

    def _record_nontransition(
        self,
        *,
        attempt_id: str,
        request_sha256: str,
        binding_sha256: str,
        run_id: str,
        outcome: ScienceShadowOutcome,
        stage: ScienceShadowStage,
        issue_codes: tuple[str, ...],
        state: CellTrackEngineState,
        evidence: ShadowEvidenceFrame | None,
        hypotheses: tuple[CellTrackLineageHypothesis, ...],
        transition_for_accounting: CellTrackTransitionResult | None = None,
    ) -> ScienceShadowReceipt:
        receipt = _build_receipt(
            attempt_id=attempt_id,
            request_sha256=request_sha256,
            binding_sha256=binding_sha256,
            engine_id=self._engine_id,
            runtime_profile_sha256=self._runtime_profile_sha256,
            canonical_run_id=run_id,
            outcome=outcome,
            stage=stage,
            issue_codes=tuple(sorted(set(issue_codes))),
            previous_advanced_receipt_sha256=self._head_receipt_sha256,
            provenance=(
                None
                if evidence is None
                else ScienceShadowProvenance.from_evidence(evidence)
            ),
            input_state_revision=state.revision,
            next_state_revision=state.revision,
            transition_id=None,
            transition_reason_codes=(),
            lineage_hypotheses=hypotheses,
            applied_lineage_hypothesis_ids=(),
            accounting=(
                ScienceShadowAccounting.without_evidence(state)
                if evidence is None
                else ScienceShadowAccounting.from_uncommitted_evidence(
                    evidence,
                    state,
                    transition_for_accounting,
                )
            ),
        )
        return self._store.record(
            receipt,
            expected_previous_advanced_receipt_sha256=self._head_receipt_sha256,
            expected_input_state_revision=state.revision,
        )

    def process(
        self,
        *,
        attempt_id: str,
        run_id: str,
        deadline_at: float,
        lineage_hypotheses: Iterable[CellTrackLineageHypothesis] = (),
    ) -> ScienceShadowRunResult:
        """Process exactly one canonical run under one idempotent attempt key."""

        attempt = _require_safe_id(attempt_id, "attempt_id")
        if not isinstance(run_id, str) or _RUN_ID_RE.fullmatch(run_id) is None:
            raise ValueError("run_id must be a canonical run identity")
        hypotheses = _canonical_lineage_hypotheses(
            lineage_hypotheses,
            self._limits.max_lineage_hypotheses,
        )
        active_deadline = self._validate_deadline(deadline_at)
        request_sha = self._request_sha256(
            attempt_id=attempt,
            run_id=run_id,
            hypotheses=hypotheses,
        )
        with self._lock:
            existing = self._store.load_attempt(
                attempt,
                request_sha256=request_sha,
            )
            if existing is not None:
                return ScienceShadowRunResult(existing, True, None, None)
            advanced = self._store.load_advanced_run(run_id)
            if advanced is not None:
                return ScienceShadowRunResult(advanced, True, None, None)
            self._recover(active_deadline)
            state = self._state
            assert state is not None
            binding_sha = self._binding_sha256(
                request_sha256=request_sha,
                state=state,
                previous_receipt_sha256=self._head_receipt_sha256,
            )
            try:
                composed = self._composer.compose(
                    run_id=run_id,
                    deadline_at=active_deadline,
                    monotonic=self._monotonic,
                )
            except EvidenceToCellsShadowDeadlineError:
                receipt = self._record_nontransition(
                    attempt_id=attempt,
                    request_sha256=request_sha,
                    binding_sha256=binding_sha,
                    run_id=run_id,
                    outcome=ScienceShadowOutcome.DEADLINE_EXCEEDED,
                    stage=ScienceShadowStage.ORCHESTRATOR,
                    issue_codes=("composition_deadline_exceeded",),
                    state=state,
                    evidence=None,
                    hypotheses=hypotheses,
                )
                return ScienceShadowRunResult(receipt, False, None, None)
            except EvidenceToCellsShadowCapacityError:
                receipt = self._record_nontransition(
                    attempt_id=attempt,
                    request_sha256=request_sha,
                    binding_sha256=binding_sha,
                    run_id=run_id,
                    outcome=ScienceShadowOutcome.CAPACITY_EXCEEDED,
                    stage=ScienceShadowStage.ORCHESTRATOR,
                    issue_codes=("composition_capacity_exceeded",),
                    state=state,
                    evidence=None,
                    hypotheses=hypotheses,
                )
                return ScienceShadowRunResult(receipt, False, None, None)
            except EvidenceToCellsShadowIntegrityError:
                receipt = self._record_nontransition(
                    attempt_id=attempt,
                    request_sha256=request_sha,
                    binding_sha256=binding_sha,
                    run_id=run_id,
                    outcome=ScienceShadowOutcome.INVALID,
                    stage=ScienceShadowStage.ORCHESTRATOR,
                    issue_codes=("composition_integrity_failed",),
                    state=state,
                    evidence=None,
                    hypotheses=hypotheses,
                )
                return ScienceShadowRunResult(receipt, False, None, None)
            if not isinstance(composed, ShadowCompositionResult):
                raise TypeError("composer returned an invalid result contract")
            if not composed.is_candidate:
                states = {item.state for item in composed.issues}
                outcome = (
                    ScienceShadowOutcome.INVALID
                    if "invalid" in states
                    else ScienceShadowOutcome.WITHHELD
                )
                stages = {item.stage for item in composed.issues}
                stage = (
                    next(iter(stages))
                    if len(stages) == 1
                    else ScienceShadowStage.ORCHESTRATOR
                )
                receipt = self._record_nontransition(
                    attempt_id=attempt,
                    request_sha256=request_sha,
                    binding_sha256=binding_sha,
                    run_id=run_id,
                    outcome=outcome,
                    stage=stage,
                    issue_codes=tuple(item.code for item in composed.issues),
                    state=state,
                    evidence=None,
                    hypotheses=hypotheses,
                )
                return ScienceShadowRunResult(receipt, False, None, None)
            evidence = composed.evidence
            assert evidence is not None
            if evidence.canonical_run_id != run_id:
                raise EvidenceToCellsShadowIntegrityError(
                    "composer returned evidence for another run"
                )
            if (
                evidence.normalized_frame.grid.width
                * evidence.normalized_frame.grid.height
                > self._policy.maximum_frame_pixels
            ):
                receipt = self._record_nontransition(
                    attempt_id=attempt,
                    request_sha256=request_sha,
                    binding_sha256=binding_sha,
                    run_id=run_id,
                    outcome=ScienceShadowOutcome.CAPACITY_EXCEEDED,
                    stage=ScienceShadowStage.TRACKING,
                    issue_codes=("tracking_frame_pixel_capacity_exceeded",),
                    state=state,
                    evidence=evidence,
                    hypotheses=hypotheses,
                )
                return ScienceShadowRunResult(receipt, False, evidence, None)
            try:
                _check_deadline(active_deadline, self._monotonic)
                transition = advance_cell_track_engine(
                    state,
                    evidence.normalized_frame,
                    self._policy,
                    lineage_hypotheses=hypotheses,
                )
            except ScienceShadowDeadlineError:
                receipt = self._record_nontransition(
                    attempt_id=attempt,
                    request_sha256=request_sha,
                    binding_sha256=binding_sha,
                    run_id=run_id,
                    outcome=ScienceShadowOutcome.DEADLINE_EXCEEDED,
                    stage=ScienceShadowStage.TRACKING,
                    issue_codes=("tracking_deadline_exceeded",),
                    state=state,
                    evidence=evidence,
                    hypotheses=hypotheses,
                )
                return ScienceShadowRunResult(receipt, False, evidence, None)
            except (TypeError, ValueError):
                receipt = self._record_nontransition(
                    attempt_id=attempt,
                    request_sha256=request_sha,
                    binding_sha256=binding_sha,
                    run_id=run_id,
                    outcome=ScienceShadowOutcome.INVALID,
                    stage=ScienceShadowStage.TRACKING,
                    issue_codes=("tracking_input_invalid",),
                    state=state,
                    evidence=evidence,
                    hypotheses=hypotheses,
                )
                return ScienceShadowRunResult(receipt, False, evidence, None)
            try:
                _check_deadline(active_deadline, self._monotonic)
            except ScienceShadowDeadlineError:
                receipt = self._record_nontransition(
                    attempt_id=attempt,
                    request_sha256=request_sha,
                    binding_sha256=binding_sha,
                    run_id=run_id,
                    outcome=ScienceShadowOutcome.DEADLINE_EXCEEDED,
                    stage=ScienceShadowStage.TRACKING,
                    issue_codes=("tracking_deadline_exceeded",),
                    state=state,
                    evidence=evidence,
                    hypotheses=hypotheses,
                    transition_for_accounting=transition,
                )
                return ScienceShadowRunResult(receipt, False, evidence, None)
            if transition.detection_frame != evidence.detection_frame:
                receipt = self._record_nontransition(
                    attempt_id=attempt,
                    request_sha256=request_sha,
                    binding_sha256=binding_sha,
                    run_id=run_id,
                    outcome=ScienceShadowOutcome.INVALID,
                    stage=ScienceShadowStage.TRACKING,
                    issue_codes=("detector_replay_mismatch",),
                    state=state,
                    evidence=evidence,
                    hypotheses=hypotheses,
                    transition_for_accounting=transition,
                )
                return ScienceShadowRunResult(receipt, False, evidence, None)
            outcome = (
                ScienceShadowOutcome.ADVANCED
                if transition.status == "advanced"
                else ScienceShadowOutcome.INDETERMINATE
            )
            reasons = tuple(sorted(set(transition.reason_codes)))
            output_hypotheses = _canonical_lineage_hypotheses(
                transition.lineage_hypotheses,
                self._limits.max_lineage_hypotheses,
            )
            receipt = _build_receipt(
                attempt_id=attempt,
                request_sha256=request_sha,
                binding_sha256=binding_sha,
                engine_id=self._engine_id,
                runtime_profile_sha256=self._runtime_profile_sha256,
                canonical_run_id=run_id,
                outcome=outcome,
                stage=ScienceShadowStage.TRACKING,
                issue_codes=() if outcome is ScienceShadowOutcome.ADVANCED else reasons,
                previous_advanced_receipt_sha256=self._head_receipt_sha256,
                provenance=ScienceShadowProvenance.from_evidence(evidence),
                input_state_revision=state.revision,
                next_state_revision=transition.next_state.revision,
                transition_id=transition.transition_id,
                transition_reason_codes=reasons,
                lineage_hypotheses=output_hypotheses,
                applied_lineage_hypothesis_ids=(
                    transition.applied_lineage_hypothesis_ids
                ),
                accounting=ScienceShadowAccounting.from_evidence(
                    evidence,
                    state,
                    transition,
                ),
            )
            try:
                recorded = self._store.record(
                    receipt,
                    expected_previous_advanced_receipt_sha256=(
                        self._head_receipt_sha256
                    ),
                    expected_input_state_revision=state.revision,
                    advanced_state=(
                        transition.next_state
                        if outcome is ScienceShadowOutcome.ADVANCED
                        else None
                    ),
                    evidence=(
                        evidence
                        if outcome is ScienceShadowOutcome.ADVANCED
                        else None
                    ),
                )
            except ScienceShadowCheckpointCapacityError:
                recorded = self._record_nontransition(
                    attempt_id=attempt,
                    request_sha256=request_sha,
                    binding_sha256=binding_sha,
                    run_id=run_id,
                    outcome=ScienceShadowOutcome.CAPACITY_EXCEEDED,
                    stage=ScienceShadowStage.ORCHESTRATOR,
                    issue_codes=("state_checkpoint_capacity_exceeded",),
                    state=state,
                    evidence=evidence,
                    hypotheses=hypotheses,
                    transition_for_accounting=transition,
                )
                return ScienceShadowRunResult(recorded, False, evidence, None)
            except ScienceShadowRetainedInputCapacityError:
                recorded = self._record_nontransition(
                    attempt_id=attempt,
                    request_sha256=request_sha,
                    binding_sha256=binding_sha,
                    run_id=run_id,
                    outcome=ScienceShadowOutcome.CAPACITY_EXCEEDED,
                    stage=ScienceShadowStage.ORCHESTRATOR,
                    issue_codes=("retained_replay_input_capacity_exceeded",),
                    state=state,
                    evidence=evidence,
                    hypotheses=hypotheses,
                    transition_for_accounting=transition,
                )
                return ScienceShadowRunResult(recorded, False, evidence, None)
            except ScienceShadowRetainedInputPrivacyError:
                recorded = self._record_nontransition(
                    attempt_id=attempt,
                    request_sha256=request_sha,
                    binding_sha256=binding_sha,
                    run_id=run_id,
                    outcome=ScienceShadowOutcome.INVALID,
                    stage=ScienceShadowStage.ORCHESTRATOR,
                    issue_codes=("retained_input_privacy_rejected",),
                    state=state,
                    evidence=evidence,
                    hypotheses=hypotheses,
                    transition_for_accounting=transition,
                )
                return ScienceShadowRunResult(recorded, False, evidence, None)
            if recorded.receipt_sha256 != receipt.receipt_sha256:
                self._state = None
                self._head_receipt_sha256 = None
                return ScienceShadowRunResult(recorded, True, None, None)
            if outcome is ScienceShadowOutcome.ADVANCED:
                self._state = transition.next_state
                self._head_receipt_sha256 = receipt.receipt_sha256
            return ScienceShadowRunResult(recorded, False, evidence, transition)


__all__ = [
    "CanonicalEvidenceCellComposer",
    "DurableScienceShadowOrchestrator",
    "HARD_MAX_CHECKPOINT_BYTES",
    "HARD_MAX_CHECKPOINT_INTERVAL_FRAMES",
    "HARD_MAX_DATABASE_BYTES",
    "HARD_MAX_EXPIRED_FILTER_BYTES",
    "HARD_MAX_EXPIRED_MEMBERSHIPS",
    "HARD_MAX_LINEAGE_HYPOTHESES",
    "HARD_MAX_RECEIPTS",
    "HARD_MAX_RECEIPT_BYTES",
    "HARD_MAX_REPLAY_FRAMES",
    "HARD_MAX_RETAINED_INPUT_BYTES",
    "SCIENCE_SHADOW_ADMISSION_STATUS",
    "SCIENCE_SHADOW_CHECKPOINT_CONTRACT",
    "SCIENCE_SHADOW_OPERATIONAL_BLOCKERS",
    "SCIENCE_SHADOW_ORCHESTRATOR_ALGORITHM",
    "SCIENCE_SHADOW_RECEIPT_CONTRACT",
    "SCIENCE_SHADOW_RETAINED_INPUT_CONTRACT",
    "ScienceShadowAccounting",
    "ScienceShadowCapacityError",
    "ScienceShadowCheckpointCapacityError",
    "ScienceShadowDeadlineError",
    "ScienceShadowError",
    "ScienceShadowLimits",
    "ScienceShadowOutcome",
    "ScienceShadowProvenance",
    "ScienceShadowReceipt",
    "ScienceShadowReceiptStore",
    "ScienceShadowRetainedInput",
    "ScienceShadowRetainedInputCapacityError",
    "ScienceShadowRetainedInputPrivacyError",
    "ScienceShadowRecoveryError",
    "ScienceShadowRunResult",
    "ScienceShadowStage",
    "ScienceShadowStoreConflict",
    "ScienceShadowStoreError",
    "ScienceShadowStateCheckpoint",
    "ScienceShadowWindowExpired",
    "science_shadow_request_sha256",
    "ShadowCompositionIssue",
    "ShadowCompositionResult",
    "ShadowEvidenceComposer",
    "ShadowEvidenceFrame",
]
