"""Content-addressed, fail-closed admission for offline science replays.

This module evaluates a *replay dossier*, never a weather observation.  It is
deliberately absent from the worker service, acquisition, MQTT, Home Assistant,
snapshot publication and dashboard paths.  Even a passing receipt only admits
the candidate to the end of the offline replay phase; meteorological production
admission and live wiring remain unconditionally false.

The existing science modules already provide useful local primitives.  This
module closes a different boundary: one immutable held-out/golden input
contract must be bound to exact component sources, deterministic primary and
repeat outputs, the complete roadmap baseline set, a metric bundle, external
truth and a locked :mod:`scientific_validation` manifest.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
from typing import Any, Iterable, Mapping

from .analysis_grid_transform import ANALYSIS_GRID_TRANSFORM_ALGORITHM
from .scientific_validation import (
    MetricEvidence,
    ValidationManifest,
    ValidationReport,
    evaluate_manifest,
)
from .tracking_core.engine.cell_track_engine import CELL_TRACK_ENGINE_VERSION
from .tracking_core.engine.event_probability_calibration import (
    EVENT_PROBABILITY_CALIBRATION_VERSION,
)
from .tracking_core.engine.forecast_verification import (
    FORECAST_VERIFICATION_VERSION,
)
from .tracking_core.engine.ground_verification import GROUND_VERIFICATION_VERSION
from .tracking_core.engine.hierarchical_detection import (
    HIERARCHICAL_DETECTOR_VERSION,
)
from .tracking_core.engine.lucas_kanade_flow import LUCAS_KANADE_FLOW_VERSION
from .tracking_core.engine.nowcast_baselines import (
    NOWCAST_BASELINE_CONTRACT_VERSION,
)
from .tracking_core.engine.pyramidal_sparse_flow import (
    SPARSE_PYRAMIDAL_FLOW_VERSION,
)
from .validation_corpus import CorpusPartition, ExternalTruthContext


OFFLINE_REPLAY_ACCEPTANCE_CONTRACT = "stv3-offline-replay-acceptance-v1"
OFFLINE_REPLAY_INPUT_CONTRACT = "stv3-offline-heldout-golden-input-v1"
OFFLINE_REPLAY_METRIC_BUNDLE_CONTRACT = "stv3-offline-metric-bundle-v1"
OFFLINE_REPLAY_ADMISSION_SCOPE = "offline_science_shadow_only"
MINIMUM_ACCEPTANCE_FRAMES = 20
MAXIMUM_ACCEPTANCE_FRAMES = 16_384
MAXIMUM_LOCAL_ARTIFACT_BYTES = 16 * 1024 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SHA256_IDENTIFIER_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class OfflineReplayAcceptanceError(RuntimeError):
    """The replay dossier is structurally invalid or cannot be read safely."""


class ArtifactRole(str, Enum):
    """Required immutable authorities for a held-out/golden replay."""

    ANNOTATION_PROTOCOL = "annotation_protocol"
    EVENT_SPLIT_MANIFEST = "event_split_manifest"
    GOLDEN_LABEL_COMMITMENT = "golden_label_commitment"
    RAW_INPUT_MANIFEST = "raw_input_manifest"


class ScienceStage(str, Enum):
    """Existing worker science stages that require exact replay evidence."""

    ANALYSIS_GRID = "analysis_grid"
    CELL_TRACK = "cell_track"
    EVENT_CALIBRATION = "event_calibration"
    FORECAST_VERIFICATION = "forecast_verification"
    GROUND_VERIFICATION = "ground_verification"
    HIERARCHICAL_DETECTION = "hierarchical_detection"
    LUCAS_KANADE_FLOW = "lucas_kanade_flow"
    NOWCAST_BASELINES = "nowcast_baselines"
    PYRAMIDAL_SPARSE_FLOW = "pyramidal_sparse_flow"


class BaselineId(str, Enum):
    """Roadmap section 12.4 baselines; none may be silently omitted."""

    EULERIAN_PERSISTENCE = "eulerian_persistence"
    LAST_RELEASED_VNEXT = "last_released_vnext"
    LEGACY_ENGINE = "legacy_engine"
    PYSTEPS_DETERMINISTIC = "pysteps_deterministic_extrapolation"
    TWO_FRAME_TRANSLATION = "two_frame_linear_translation"


REQUIRED_ARTIFACT_ROLES = tuple(sorted(ArtifactRole, key=lambda item: item.value))
REQUIRED_SCIENCE_STAGES = tuple(sorted(ScienceStage, key=lambda item: item.value))
REQUIRED_BASELINES = tuple(sorted(BaselineId, key=lambda item: item.value))

# A compact cross-layer minimum.  A locked release manifest may add stricter
# strata and metrics, but omitting any of these prevents offline admission.
REQUIRED_OFFLINE_GATE_IDS = (
    "calibration.heldout_brier_skill",
    "detection.csi",
    "flow.one_step_advection_skill",
    "ground.cross_storm_false_confirmations",
    "nowcast.passage_brier_skill",
    "tracking.id_switches",
    "tracking.lineage_integrity",
)

_ALLOWED_SCIENCE_CATEGORIES = frozenset(
    {
        "calibration",
        "detection",
        "flow",
        "geometry",
        "ground",
        "nowcast",
        "tracking",
    }
)

_COMPONENT_DEFINITIONS = {
    ScienceStage.ANALYSIS_GRID: (
        "analysis_grid_transform.py",
        ANALYSIS_GRID_TRANSFORM_ALGORITHM,
    ),
    ScienceStage.CELL_TRACK: (
        "tracking_core/engine/cell_track_engine.py",
        CELL_TRACK_ENGINE_VERSION,
    ),
    ScienceStage.EVENT_CALIBRATION: (
        "tracking_core/engine/event_probability_calibration.py",
        EVENT_PROBABILITY_CALIBRATION_VERSION,
    ),
    ScienceStage.FORECAST_VERIFICATION: (
        "tracking_core/engine/forecast_verification.py",
        FORECAST_VERIFICATION_VERSION,
    ),
    ScienceStage.GROUND_VERIFICATION: (
        "tracking_core/engine/ground_verification.py",
        GROUND_VERIFICATION_VERSION,
    ),
    ScienceStage.HIERARCHICAL_DETECTION: (
        "tracking_core/engine/hierarchical_detection.py",
        HIERARCHICAL_DETECTOR_VERSION,
    ),
    ScienceStage.LUCAS_KANADE_FLOW: (
        "tracking_core/engine/lucas_kanade_flow.py",
        LUCAS_KANADE_FLOW_VERSION,
    ),
    ScienceStage.NOWCAST_BASELINES: (
        "tracking_core/engine/nowcast_baselines.py",
        NOWCAST_BASELINE_CONTRACT_VERSION,
    ),
    ScienceStage.PYRAMIDAL_SPARSE_FLOW: (
        "tracking_core/engine/pyramidal_sparse_flow.py",
        SPARSE_PYRAMIDAL_FLOW_VERSION,
    ),
}


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _sha_mapping(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _require_sha(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} requires lowercase SHA-256")
    return value


def _require_sha_identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256_IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(f"{field} requires sha256:<lowercase SHA-256>")
    return value


def _identifier(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or "\x00" in value
        or len(value) > 512
    ):
        raise ValueError(f"{field} is invalid")
    return value


def _finite(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field} must be finite")
    return result


def _nonnegative_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _sorted_unique_strings(
    values: object,
    field: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(values, tuple):
        raise TypeError(f"{field} must be a tuple")
    if not allow_empty and not values:
        raise ValueError(f"{field} may not be empty")
    for value in values:
        _identifier(value, field)
    if len(values) != len(set(values)) or values != tuple(sorted(values)):
        raise ValueError(f"{field} must be sorted and unique")
    return values


def _read_regular_bytes(path: Path, *, maximum_bytes: int) -> bytes:
    """Read one stable regular file without following a final symlink."""

    if not path.is_absolute():
        raise OfflineReplayAcceptanceError("artifact path must be absolute")
    if path.is_symlink():
        raise OfflineReplayAcceptanceError("artifact path may not be a symlink")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise OfflineReplayAcceptanceError("artifact cannot be opened") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > maximum_bytes
        ):
            raise OfflineReplayAcceptanceError("artifact type/size is invalid")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(
                descriptor,
                min(1024 * 1024, maximum_bytes + 1 - total),
            )
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum_bytes:
                raise OfflineReplayAcceptanceError("artifact exceeds byte bound")
        after = os.fstat(descriptor)
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        )
        if identity_before != identity_after or total != before.st_size:
            raise OfflineReplayAcceptanceError("artifact changed while reading")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class ArtifactBinding:
    artifact_id: str
    role: ArtifactRole
    sha256: str
    size_bytes: int
    media_type: str

    def __post_init__(self) -> None:
        _identifier(self.artifact_id, "artifact_id")
        if not isinstance(self.role, ArtifactRole):
            raise TypeError("role requires ArtifactRole")
        _require_sha(self.sha256, "artifact sha256")
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes <= 0
        ):
            raise ValueError("artifact size_bytes must be positive")
        _identifier(self.media_type, "media_type")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "media_type": self.media_type,
            "role": self.role.value,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


def bind_local_artifact(
    path: str | Path,
    *,
    artifact_id: str,
    role: ArtifactRole,
    media_type: str = "application/json",
    maximum_bytes: int = MAXIMUM_LOCAL_ARTIFACT_BYTES,
) -> ArtifactBinding:
    """Content-address a bounded local artifact without persisting its path."""

    if (
        isinstance(maximum_bytes, bool)
        or not isinstance(maximum_bytes, int)
        or maximum_bytes <= 0
        or maximum_bytes > MAXIMUM_LOCAL_ARTIFACT_BYTES
    ):
        raise ValueError("maximum_bytes is outside the safe bound")
    absolute = Path(os.path.abspath(os.fspath(path)))
    body = _read_regular_bytes(absolute, maximum_bytes=maximum_bytes)
    return ArtifactBinding(
        artifact_id=artifact_id,
        role=role,
        sha256=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
        media_type=media_type,
    )


@dataclass(frozen=True, slots=True)
class ReplayFrameBinding:
    frame_id: str
    event_id: str
    valid_time_utc: float
    raw_frame_sha256: str

    def __post_init__(self) -> None:
        _identifier(self.frame_id, "frame_id")
        _identifier(self.event_id, "event_id")
        _finite(self.valid_time_utc, "valid_time_utc")
        _require_sha(self.raw_frame_sha256, "raw_frame_sha256")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "frame_id": self.frame_id,
            "raw_frame_sha256": self.raw_frame_sha256,
            "valid_time_utc": float(self.valid_time_utc),
        }


@dataclass(frozen=True, slots=True)
class OfflineReplayInput:
    """Immutable test-partition input and preregistration commitment."""

    contract_id: str
    corpus_id: str
    split_policy_fingerprint: str
    scientific_manifest_sha256: str
    external_truth_context_sha256: str
    partition: CorpusPartition
    frames: tuple[ReplayFrameBinding, ...]
    artifacts: tuple[ArtifactBinding, ...]
    labels_blinded_to_candidate: bool
    label_sources_candidate_independent: bool
    golden_consensus_confirmed: bool
    contract_version: str = OFFLINE_REPLAY_INPUT_CONTRACT

    def __post_init__(self) -> None:
        if self.contract_version != OFFLINE_REPLAY_INPUT_CONTRACT:
            raise ValueError("unsupported offline replay input contract")
        _identifier(self.contract_id, "contract_id")
        _identifier(self.corpus_id, "corpus_id")
        _require_sha_identifier(
            self.split_policy_fingerprint,
            "split_policy_fingerprint",
        )
        _require_sha(self.scientific_manifest_sha256, "scientific manifest SHA")
        _require_sha(
            self.external_truth_context_sha256,
            "external truth context SHA",
        )
        if self.partition is not CorpusPartition.TEST:
            raise ValueError("offline admission accepts only the held-out test partition")
        if not isinstance(self.frames, tuple) or not self.frames:
            raise ValueError("frames must be a non-empty tuple")
        if len(self.frames) > MAXIMUM_ACCEPTANCE_FRAMES:
            raise ValueError("frame count exceeds hard acceptance bound")
        if self.frames != tuple(
            sorted(
                self.frames,
                key=lambda item: (item.event_id, item.valid_time_utc, item.frame_id),
            )
        ):
            raise ValueError("frames must use canonical event/time/id ordering")
        frame_ids = tuple(frame.frame_id for frame in self.frames)
        if len(frame_ids) != len(set(frame_ids)):
            raise ValueError("frame_id must be unique")
        if not isinstance(self.artifacts, tuple):
            raise TypeError("artifacts must be a tuple")
        expected_artifacts = tuple(
            sorted(self.artifacts, key=lambda item: item.role.value)
        )
        if self.artifacts != expected_artifacts:
            raise ValueError("artifacts must be ordered by role")
        roles = tuple(artifact.role for artifact in self.artifacts)
        if roles != REQUIRED_ARTIFACT_ROLES:
            raise ValueError("input contract requires each exact authority role once")
        artifact_ids = tuple(artifact.artifact_id for artifact in self.artifacts)
        if len(artifact_ids) != len(set(artifact_ids)):
            raise ValueError("artifact_id must be unique")
        for field, value in (
            ("labels_blinded_to_candidate", self.labels_blinded_to_candidate),
            (
                "label_sources_candidate_independent",
                self.label_sources_candidate_independent,
            ),
            ("golden_consensus_confirmed", self.golden_consensus_confirmed),
        ):
            if not isinstance(value, bool):
                raise TypeError(f"{field} must be bool")

    @property
    def event_ids(self) -> tuple[str, ...]:
        return tuple(sorted({frame.event_id for frame in self.frames}))

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "artifacts": [artifact.to_mapping() for artifact in self.artifacts],
            "contract_id": self.contract_id,
            "contract_version": self.contract_version,
            "corpus_id": self.corpus_id,
            "external_truth_context_sha256": self.external_truth_context_sha256,
            "frames": [frame.to_mapping() for frame in self.frames],
            "golden_consensus_confirmed": self.golden_consensus_confirmed,
            "label_sources_candidate_independent": (
                self.label_sources_candidate_independent
            ),
            "labels_blinded_to_candidate": self.labels_blinded_to_candidate,
            "partition": self.partition.value,
            "scientific_manifest_sha256": self.scientific_manifest_sha256,
            "split_policy_fingerprint": self.split_policy_fingerprint,
        }

    @property
    def sha256(self) -> str:
        return _sha_mapping(self.payload_mapping())

    def to_mapping(self) -> dict[str, Any]:
        return {**self.payload_mapping(), "input_contract_sha256": self.sha256}


def offline_replay_input_from_mapping(value: Mapping[str, Any]) -> OfflineReplayInput:
    """Strictly reconstruct a portable golden input contract."""

    expected = {
        "artifacts",
        "contract_id",
        "contract_version",
        "corpus_id",
        "external_truth_context_sha256",
        "frames",
        "golden_consensus_confirmed",
        "label_sources_candidate_independent",
        "labels_blinded_to_candidate",
        "partition",
        "scientific_manifest_sha256",
        "split_policy_fingerprint",
    }
    if set(value) not in (expected, expected | {"input_contract_sha256"}):
        raise ValueError("offline replay input has unexpected/missing keys")
    raw_frames = value["frames"]
    raw_artifacts = value["artifacts"]
    if not isinstance(raw_frames, list) or not isinstance(raw_artifacts, list):
        raise TypeError("frames/artifacts must be lists")
    frames = tuple(
        ReplayFrameBinding(
            frame_id=_mapping_item(item, "frame", "frame_id"),
            event_id=_mapping_item(item, "frame", "event_id"),
            valid_time_utc=_mapping_item(item, "frame", "valid_time_utc"),
            raw_frame_sha256=_mapping_item(item, "frame", "raw_frame_sha256"),
        )
        for item in raw_frames
    )
    artifacts = tuple(
        ArtifactBinding(
            artifact_id=_mapping_item(item, "artifact", "artifact_id"),
            role=ArtifactRole(_mapping_item(item, "artifact", "role")),
            sha256=_mapping_item(item, "artifact", "sha256"),
            size_bytes=_mapping_item(item, "artifact", "size_bytes"),
            media_type=_mapping_item(item, "artifact", "media_type"),
        )
        for item in raw_artifacts
    )
    result = OfflineReplayInput(
        contract_id=value["contract_id"],
        corpus_id=value["corpus_id"],
        split_policy_fingerprint=value["split_policy_fingerprint"],
        scientific_manifest_sha256=value["scientific_manifest_sha256"],
        external_truth_context_sha256=value["external_truth_context_sha256"],
        partition=CorpusPartition(value["partition"]),
        frames=frames,
        artifacts=artifacts,
        labels_blinded_to_candidate=value["labels_blinded_to_candidate"],
        label_sources_candidate_independent=(
            value["label_sources_candidate_independent"]
        ),
        golden_consensus_confirmed=value["golden_consensus_confirmed"],
        contract_version=value["contract_version"],
    )
    supplied_sha = value.get("input_contract_sha256")
    if supplied_sha is not None and supplied_sha != result.sha256:
        raise ValueError("input_contract_sha256 differs")
    return result


def load_offline_replay_input(path: str | Path) -> OfflineReplayInput:
    """Load a bounded duplicate-key-free local JSON input contract."""

    body = _read_regular_bytes(
        Path(os.path.abspath(os.fspath(path))),
        maximum_bytes=MAXIMUM_LOCAL_ARTIFACT_BYTES,
    )

    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = item
        return result

    try:
        value = json.loads(body, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OfflineReplayAcceptanceError("input contract is invalid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("input contract root must be an object")
    return offline_replay_input_from_mapping(value)


@dataclass(frozen=True, slots=True)
class ScienceComponentSource:
    stage: ScienceStage
    relative_source: str
    implementation_version: str
    source_sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        if not isinstance(self.stage, ScienceStage):
            raise TypeError("stage requires ScienceStage")
        relative = Path(self.relative_source)
        if (
            relative.is_absolute()
            or not relative.parts
            or ".." in relative.parts
            or "\\" in self.relative_source
        ):
            raise ValueError("relative_source is invalid")
        _identifier(self.implementation_version, "implementation_version")
        _require_sha(self.source_sha256, "source_sha256")
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes <= 0
        ):
            raise ValueError("component size_bytes must be positive")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "implementation_version": self.implementation_version,
            "relative_source": self.relative_source,
            "size_bytes": self.size_bytes,
            "source_sha256": self.source_sha256,
            "stage": self.stage.value,
        }


def audit_local_science_component_sources(
    package_directory: str | Path | None = None,
) -> tuple[ScienceComponentSource, ...]:
    """Hash the exact existing science source modules used by this gate."""

    root = Path(
        os.path.abspath(
            os.fspath(
                package_directory
                if package_directory is not None
                else Path(__file__).parent
            )
        )
    )
    if root.is_symlink() or not root.is_dir():
        raise OfflineReplayAcceptanceError("worker package root is invalid")
    result = []
    for stage in REQUIRED_SCIENCE_STAGES:
        relative, version = _COMPONENT_DEFINITIONS[stage]
        body = _read_regular_bytes(
            root / Path(relative),
            maximum_bytes=MAXIMUM_LOCAL_ARTIFACT_BYTES,
        )
        result.append(
            ScienceComponentSource(
                stage=stage,
                relative_source=Path(relative).as_posix(),
                implementation_version=version,
                source_sha256=hashlib.sha256(body).hexdigest(),
                size_bytes=len(body),
            )
        )
    return tuple(result)


@dataclass(frozen=True, slots=True)
class StageReplayEvidence:
    stage: ScienceStage
    implementation_version: str
    implementation_sha256: str
    input_contract_sha256: str
    primary_output_sha256: str | None
    reproduction_output_sha256: str | None
    event_ids: tuple[str, ...]
    frame_count: int
    completed: bool
    shadow_only: bool
    operational_state_mutated: bool
    issue_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.stage, ScienceStage):
            raise TypeError("stage requires ScienceStage")
        _identifier(self.implementation_version, "implementation_version")
        _require_sha(self.implementation_sha256, "implementation_sha256")
        _require_sha(self.input_contract_sha256, "input_contract_sha256")
        for field, value in (
            ("primary_output_sha256", self.primary_output_sha256),
            ("reproduction_output_sha256", self.reproduction_output_sha256),
        ):
            if value is not None:
                _require_sha(value, field)
        _sorted_unique_strings(self.event_ids, "event_ids")
        _nonnegative_integer(self.frame_count, "frame_count")
        for field, value in (
            ("completed", self.completed),
            ("shadow_only", self.shadow_only),
            ("operational_state_mutated", self.operational_state_mutated),
        ):
            if not isinstance(value, bool):
                raise TypeError(f"{field} must be bool")
        _sorted_unique_strings(self.issue_codes, "issue_codes", allow_empty=True)

    @property
    def reproducible(self) -> bool:
        return (
            self.primary_output_sha256 is not None
            and self.primary_output_sha256 == self.reproduction_output_sha256
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "completed": self.completed,
            "event_ids": list(self.event_ids),
            "frame_count": self.frame_count,
            "implementation_sha256": self.implementation_sha256,
            "implementation_version": self.implementation_version,
            "input_contract_sha256": self.input_contract_sha256,
            "issue_codes": list(self.issue_codes),
            "operational_state_mutated": self.operational_state_mutated,
            "primary_output_sha256": self.primary_output_sha256,
            "reproduction_output_sha256": self.reproduction_output_sha256,
            "reproducible": self.reproducible,
            "shadow_only": self.shadow_only,
            "stage": self.stage.value,
        }


@dataclass(frozen=True, slots=True)
class BaselineReplayEvidence:
    baseline_id: BaselineId
    preregistration_source_id: str
    implementation_id: str
    implementation_version: str
    implementation_sha256: str
    input_contract_sha256: str
    primary_output_sha256: str | None
    reproduction_output_sha256: str | None
    event_ids: tuple[str, ...]
    frame_count: int
    completed: bool
    candidate_independent: bool
    issue_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.baseline_id, BaselineId):
            raise TypeError("baseline_id requires BaselineId")
        _identifier(
            self.preregistration_source_id,
            "preregistration_source_id",
        )
        _identifier(self.implementation_id, "implementation_id")
        _identifier(self.implementation_version, "implementation_version")
        _require_sha(self.implementation_sha256, "implementation_sha256")
        _require_sha(self.input_contract_sha256, "input_contract_sha256")
        for field, value in (
            ("primary_output_sha256", self.primary_output_sha256),
            ("reproduction_output_sha256", self.reproduction_output_sha256),
        ):
            if value is not None:
                _require_sha(value, field)
        _sorted_unique_strings(self.event_ids, "event_ids")
        _nonnegative_integer(self.frame_count, "frame_count")
        if not isinstance(self.completed, bool):
            raise TypeError("completed must be bool")
        if not isinstance(self.candidate_independent, bool):
            raise TypeError("candidate_independent must be bool")
        _sorted_unique_strings(self.issue_codes, "issue_codes", allow_empty=True)

    @property
    def reproducible(self) -> bool:
        return (
            self.primary_output_sha256 is not None
            and self.primary_output_sha256 == self.reproduction_output_sha256
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "baseline_id": self.baseline_id.value,
            "candidate_independent": self.candidate_independent,
            "completed": self.completed,
            "event_ids": list(self.event_ids),
            "frame_count": self.frame_count,
            "implementation_id": self.implementation_id,
            "implementation_sha256": self.implementation_sha256,
            "implementation_version": self.implementation_version,
            "input_contract_sha256": self.input_contract_sha256,
            "issue_codes": list(self.issue_codes),
            "primary_output_sha256": self.primary_output_sha256,
            "preregistration_source_id": self.preregistration_source_id,
            "reproduction_output_sha256": self.reproduction_output_sha256,
            "reproducible": self.reproducible,
        }


def candidate_execution_manifest_sha256(
    stages: Iterable[StageReplayEvidence],
) -> str:
    ordered = tuple(sorted(stages, key=lambda item: item.stage.value))
    return _sha_mapping([stage.to_mapping() for stage in ordered])


def baseline_execution_manifest_sha256(
    baselines: Iterable[BaselineReplayEvidence],
) -> str:
    ordered = tuple(sorted(baselines, key=lambda item: item.baseline_id.value))
    return _sha_mapping([baseline.to_mapping() for baseline in ordered])


def _metric_evidence_mapping(value: MetricEvidence) -> dict[str, Any]:
    return {
        "baseline_source_ids": list(value.baseline_source_ids),
        "baseline_value": value.baseline_value,
        "candidate_value": value.candidate_value,
        "event_count": value.event_count,
        "event_ids": list(value.event_ids),
        "external_label_ids": list(value.external_label_ids),
        "external_truth_available": value.external_truth_available,
        "gate_id": value.gate_id,
        "partition": value.partition.value if value.partition is not None else None,
        "provenance_ids": list(value.provenance_ids),
    }


@dataclass(frozen=True, slots=True)
class OfflineMetricBundle:
    algorithm_id: str
    algorithm_version: str
    algorithm_sha256: str
    input_contract_sha256: str
    candidate_manifest_sha256: str
    baseline_manifest_sha256: str
    external_truth_context_sha256: str
    evidence: tuple[MetricEvidence, ...]
    contract_version: str = OFFLINE_REPLAY_METRIC_BUNDLE_CONTRACT

    def __post_init__(self) -> None:
        if self.contract_version != OFFLINE_REPLAY_METRIC_BUNDLE_CONTRACT:
            raise ValueError("unsupported metric bundle contract")
        _identifier(self.algorithm_id, "algorithm_id")
        _identifier(self.algorithm_version, "algorithm_version")
        for field, value in (
            ("algorithm_sha256", self.algorithm_sha256),
            ("input_contract_sha256", self.input_contract_sha256),
            ("candidate_manifest_sha256", self.candidate_manifest_sha256),
            ("baseline_manifest_sha256", self.baseline_manifest_sha256),
            (
                "external_truth_context_sha256",
                self.external_truth_context_sha256,
            ),
        ):
            _require_sha(value, field)
        if not isinstance(self.evidence, tuple) or not self.evidence:
            raise ValueError("metric evidence must be a non-empty tuple")
        gate_ids = tuple(item.gate_id for item in self.evidence)
        if gate_ids != tuple(sorted(gate_ids)) or len(gate_ids) != len(set(gate_ids)):
            raise ValueError("metric evidence must be sorted and unique by gate_id")
        provenance = self.provenance_id
        for item in self.evidence:
            if provenance not in item.provenance_ids:
                raise ValueError(
                    f"metric {item.gate_id} lacks exact algorithm provenance"
                )

    @property
    def provenance_id(self) -> str:
        return (
            f"{self.algorithm_id}@{self.algorithm_version}:"
            f"sha256:{self.algorithm_sha256}"
        )

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "algorithm_id": self.algorithm_id,
            "algorithm_sha256": self.algorithm_sha256,
            "algorithm_version": self.algorithm_version,
            "baseline_manifest_sha256": self.baseline_manifest_sha256,
            "candidate_manifest_sha256": self.candidate_manifest_sha256,
            "contract_version": self.contract_version,
            "evidence": [
                _metric_evidence_mapping(item) for item in self.evidence
            ],
            "external_truth_context_sha256": (
                self.external_truth_context_sha256
            ),
            "input_contract_sha256": self.input_contract_sha256,
        }

    @property
    def sha256(self) -> str:
        return _sha_mapping(self.payload_mapping())

    def to_mapping(self) -> dict[str, Any]:
        return {**self.payload_mapping(), "metric_bundle_sha256": self.sha256}


def external_truth_context_sha256(context: ExternalTruthContext) -> str:
    """Bind the split and independent labels as one exact truth authority."""

    if not isinstance(context, ExternalTruthContext):
        raise TypeError("context requires ExternalTruthContext")
    return _sha_mapping(
        {
            "labels": context.labels.to_dict(),
            "split": context.split.to_dict(),
        }
    )


def _validation_report_sha256(report: ValidationReport | None) -> str | None:
    return None if report is None else _sha_mapping(report.to_dict())


@dataclass(frozen=True, slots=True)
class OfflineReplayAcceptanceReceipt:
    input_contract_sha256: str
    component_inventory_sha256: str
    candidate_manifest_sha256: str
    baseline_manifest_sha256: str
    metric_bundle_sha256: str
    validation_manifest_sha256: str
    validation_report: ValidationReport | None
    stages: tuple[StageReplayEvidence, ...]
    baselines: tuple[BaselineReplayEvidence, ...]
    blocking_reasons: tuple[str, ...]
    offline_science_gate_passed: bool
    contract_version: str = OFFLINE_REPLAY_ACCEPTANCE_CONTRACT
    admission_scope: str = OFFLINE_REPLAY_ADMISSION_SCOPE
    meteorological_admission: bool = False
    live_wiring_allowed: bool = False
    dashboard_publication_allowed: bool = False
    home_assistant_state_mutated: bool = False

    def __post_init__(self) -> None:
        if self.contract_version != OFFLINE_REPLAY_ACCEPTANCE_CONTRACT:
            raise ValueError("unsupported acceptance receipt contract")
        if self.admission_scope != OFFLINE_REPLAY_ADMISSION_SCOPE:
            raise ValueError("receipt scope may only be offline science shadow")
        for field, value in (
            ("input_contract_sha256", self.input_contract_sha256),
            ("component_inventory_sha256", self.component_inventory_sha256),
            ("candidate_manifest_sha256", self.candidate_manifest_sha256),
            ("baseline_manifest_sha256", self.baseline_manifest_sha256),
            ("metric_bundle_sha256", self.metric_bundle_sha256),
            ("validation_manifest_sha256", self.validation_manifest_sha256),
        ):
            _require_sha(value, field)
        _sorted_unique_strings(
            self.blocking_reasons,
            "blocking_reasons",
            allow_empty=True,
        )
        for field, value in (
            ("offline_science_gate_passed", self.offline_science_gate_passed),
            ("meteorological_admission", self.meteorological_admission),
            ("live_wiring_allowed", self.live_wiring_allowed),
            (
                "dashboard_publication_allowed",
                self.dashboard_publication_allowed,
            ),
            ("home_assistant_state_mutated", self.home_assistant_state_mutated),
        ):
            if not isinstance(value, bool):
                raise TypeError(f"{field} must be bool")
        if any(
            (
                self.meteorological_admission,
                self.live_wiring_allowed,
                self.dashboard_publication_allowed,
                self.home_assistant_state_mutated,
            )
        ):
            raise ValueError("offline receipt can never authorize/mutate live state")
        if self.offline_science_gate_passed != (not self.blocking_reasons):
            raise ValueError("offline pass state must equal the absence of blockers")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "admission_scope": self.admission_scope,
            "baseline_manifest_sha256": self.baseline_manifest_sha256,
            "baselines": [item.to_mapping() for item in self.baselines],
            "blocking_reasons": list(self.blocking_reasons),
            "candidate_manifest_sha256": self.candidate_manifest_sha256,
            "component_inventory_sha256": self.component_inventory_sha256,
            "contract_version": self.contract_version,
            "dashboard_publication_allowed": self.dashboard_publication_allowed,
            "home_assistant_state_mutated": self.home_assistant_state_mutated,
            "input_contract_sha256": self.input_contract_sha256,
            "live_wiring_allowed": self.live_wiring_allowed,
            "meteorological_admission": self.meteorological_admission,
            "metric_bundle_sha256": self.metric_bundle_sha256,
            "offline_science_gate_passed": self.offline_science_gate_passed,
            "stages": [item.to_mapping() for item in self.stages],
            "validation_manifest_sha256": self.validation_manifest_sha256,
            "validation_report": (
                None
                if self.validation_report is None
                else self.validation_report.to_dict()
            ),
            "validation_report_sha256": _validation_report_sha256(
                self.validation_report
            ),
        }

    @property
    def sha256(self) -> str:
        return _sha_mapping(self.payload_mapping())

    def to_mapping(self) -> dict[str, Any]:
        return {
            **self.payload_mapping(),
            "acceptance_receipt_sha256": self.sha256,
        }


def evaluate_offline_replay_acceptance(
    replay_input: OfflineReplayInput,
    *,
    component_sources: tuple[ScienceComponentSource, ...],
    stages: tuple[StageReplayEvidence, ...],
    baselines: tuple[BaselineReplayEvidence, ...],
    metric_bundle: OfflineMetricBundle,
    validation_manifest: ValidationManifest,
    external_truth: ExternalTruthContext,
) -> OfflineReplayAcceptanceReceipt:
    """Evaluate the exact offline dossier without granting any live authority."""

    if not isinstance(replay_input, OfflineReplayInput):
        raise TypeError("replay_input requires OfflineReplayInput")
    if not isinstance(component_sources, tuple):
        raise TypeError("component_sources must be a tuple")
    if not isinstance(stages, tuple) or not isinstance(baselines, tuple):
        raise TypeError("stages/baselines must be tuples")
    if not isinstance(metric_bundle, OfflineMetricBundle):
        raise TypeError("metric_bundle requires OfflineMetricBundle")
    if not isinstance(validation_manifest, ValidationManifest):
        raise TypeError("validation_manifest requires ValidationManifest")
    if not isinstance(external_truth, ExternalTruthContext):
        raise TypeError("external_truth requires ExternalTruthContext")

    ordered_sources = tuple(sorted(component_sources, key=lambda item: item.stage.value))
    ordered_stages = tuple(sorted(stages, key=lambda item: item.stage.value))
    ordered_baselines = tuple(
        sorted(baselines, key=lambda item: item.baseline_id.value)
    )
    _unique_enum(ordered_sources, "stage", "component source")
    _unique_enum(ordered_stages, "stage", "stage evidence")
    _unique_enum(ordered_baselines, "baseline_id", "baseline evidence")

    component_inventory_sha = _sha_mapping(
        [source.to_mapping() for source in ordered_sources]
    )
    candidate_manifest_sha = candidate_execution_manifest_sha256(ordered_stages)
    baseline_manifest_sha = baseline_execution_manifest_sha256(ordered_baselines)
    blockers: list[str] = []

    if len(replay_input.frames) < MINIMUM_ACCEPTANCE_FRAMES:
        blockers.append(
            f"input.minimum_frames:{len(replay_input.frames)}<{MINIMUM_ACCEPTANCE_FRAMES}"
        )
    for field, ready in (
        ("labels_blinded_to_candidate", replay_input.labels_blinded_to_candidate),
        (
            "label_sources_candidate_independent",
            replay_input.label_sources_candidate_independent,
        ),
        ("golden_consensus_confirmed", replay_input.golden_consensus_confirmed),
    ):
        if not ready:
            blockers.append(f"input.{field}:false")

    source_by_stage = {source.stage: source for source in ordered_sources}
    stage_by_id = {stage.stage: stage for stage in ordered_stages}
    _coverage_blockers(
        blockers,
        "component_sources",
        set(source_by_stage),
        set(REQUIRED_SCIENCE_STAGES),
    )
    _coverage_blockers(
        blockers,
        "stages",
        set(stage_by_id),
        set(REQUIRED_SCIENCE_STAGES),
    )
    expected_events = replay_input.event_ids
    expected_frames = len(replay_input.frames)
    for stage in REQUIRED_SCIENCE_STAGES:
        source = source_by_stage.get(stage)
        evidence = stage_by_id.get(stage)
        if source is None or evidence is None:
            continue
        expected_relative, expected_version = _COMPONENT_DEFINITIONS[stage]
        if source.relative_source != Path(expected_relative).as_posix():
            blockers.append(f"component.{stage.value}:source_path_mismatch")
        if source.implementation_version != expected_version:
            blockers.append(f"component.{stage.value}:version_mismatch")
        if evidence.implementation_version != source.implementation_version:
            blockers.append(f"stage.{stage.value}:version_mismatch")
        if evidence.implementation_sha256 != source.source_sha256:
            blockers.append(f"stage.{stage.value}:source_sha_mismatch")
        if evidence.input_contract_sha256 != replay_input.sha256:
            blockers.append(f"stage.{stage.value}:input_contract_mismatch")
        if evidence.event_ids != expected_events:
            blockers.append(f"stage.{stage.value}:event_coverage_mismatch")
        if evidence.frame_count != expected_frames:
            blockers.append(f"stage.{stage.value}:frame_count_mismatch")
        if not evidence.completed:
            blockers.append(f"stage.{stage.value}:incomplete")
        if not evidence.reproducible:
            blockers.append(f"stage.{stage.value}:not_reproducible")
        if not evidence.shadow_only:
            blockers.append(f"stage.{stage.value}:not_shadow_only")
        if evidence.operational_state_mutated:
            blockers.append(f"stage.{stage.value}:operational_state_mutated")
        for issue in evidence.issue_codes:
            blockers.append(f"stage.{stage.value}:issue:{issue}")

    baseline_by_id = {item.baseline_id: item for item in ordered_baselines}
    _coverage_blockers(
        blockers,
        "baselines",
        set(baseline_by_id),
        set(REQUIRED_BASELINES),
    )
    for baseline_id in REQUIRED_BASELINES:
        evidence = baseline_by_id.get(baseline_id)
        if evidence is None:
            continue
        if evidence.input_contract_sha256 != replay_input.sha256:
            blockers.append(f"baseline.{baseline_id.value}:input_contract_mismatch")
        if evidence.event_ids != expected_events:
            blockers.append(f"baseline.{baseline_id.value}:event_coverage_mismatch")
        if evidence.frame_count != expected_frames:
            blockers.append(f"baseline.{baseline_id.value}:frame_count_mismatch")
        if not evidence.completed:
            blockers.append(f"baseline.{baseline_id.value}:incomplete")
        if not evidence.reproducible:
            blockers.append(f"baseline.{baseline_id.value}:not_reproducible")
        if not evidence.candidate_independent:
            blockers.append(f"baseline.{baseline_id.value}:candidate_dependent")
        for issue in evidence.issue_codes:
            blockers.append(f"baseline.{baseline_id.value}:issue:{issue}")

    truth_sha = external_truth_context_sha256(external_truth)
    artifacts_by_role = {artifact.role: artifact for artifact in replay_input.artifacts}
    raw_manifest_sha = _sha_mapping(
        [frame.to_mapping() for frame in replay_input.frames]
    )
    if (
        artifacts_by_role[ArtifactRole.RAW_INPUT_MANIFEST].sha256
        != raw_manifest_sha
    ):
        blockers.append("input.raw_manifest_sha_mismatch")
    if (
        artifacts_by_role[ArtifactRole.EVENT_SPLIT_MANIFEST].sha256
        != external_truth.split.sha256
    ):
        blockers.append("input.event_split_manifest_sha_mismatch")
    if (
        artifacts_by_role[ArtifactRole.GOLDEN_LABEL_COMMITMENT].sha256
        != external_truth.labels.sha256
    ):
        blockers.append("input.golden_label_commitment_sha_mismatch")
    if truth_sha != replay_input.external_truth_context_sha256:
        blockers.append("truth.context_sha_mismatch")
    if external_truth.split.corpus_id != replay_input.corpus_id:
        blockers.append("truth.corpus_id_mismatch")
    if external_truth.split.policy_fingerprint != replay_input.split_policy_fingerprint:
        blockers.append("truth.split_policy_mismatch")
    split_frames = dict(external_truth.split.frame_events)
    split_partitions = dict(external_truth.split.frame_partitions)
    for frame in replay_input.frames:
        if split_frames.get(frame.frame_id) != frame.event_id:
            blockers.append(f"truth.frame_event_mismatch:{frame.frame_id}")
        if split_partitions.get(frame.frame_id) is not CorpusPartition.TEST:
            blockers.append(f"truth.frame_not_test:{frame.frame_id}")
    for source in external_truth.labels.sources:
        if not source.candidate_independent:
            blockers.append(f"truth.source_candidate_dependent:{source.source_id}")
        if not source.blinded_to_candidate:
            blockers.append(f"truth.source_not_blinded:{source.source_id}")
    if not any(
        source.source_kind.value == "blinded_human_consensus"
        for source in external_truth.labels.sources
    ):
        blockers.append("truth.blinded_human_consensus_missing")

    if validation_manifest.sha256 != replay_input.scientific_manifest_sha256:
        blockers.append("validation.manifest_sha_mismatch")
    if validation_manifest.corpus_id != replay_input.corpus_id:
        blockers.append("validation.corpus_id_mismatch")
    if validation_manifest.corpus_split_policy != replay_input.split_policy_fingerprint:
        blockers.append("validation.split_policy_mismatch")
    if not validation_manifest.locked:
        blockers.append("validation.manifest_not_locked")
    gate_by_id = {gate.gate_id: gate for gate in validation_manifest.gates}
    preregistration_sources = {
        source.source_id: source for source in validation_manifest.sources
    }
    used_baseline_sources: list[str] = []
    for baseline_id in REQUIRED_BASELINES:
        evidence = baseline_by_id.get(baseline_id)
        if evidence is None:
            continue
        source = preregistration_sources.get(evidence.preregistration_source_id)
        if source is None:
            blockers.append(
                f"baseline.{baseline_id.value}:preregistration_source_missing"
            )
            continue
        used_baseline_sources.append(evidence.preregistration_source_id)
        if source.role.value != "baseline":
            blockers.append(
                f"baseline.{baseline_id.value}:preregistration_role_mismatch"
            )
        if source.artifact_sha256 != evidence.primary_output_sha256:
            blockers.append(
                f"baseline.{baseline_id.value}:preregistered_output_sha_mismatch"
            )
        if not source.candidate_independent:
            blockers.append(
                f"baseline.{baseline_id.value}:preregistered_source_dependent"
            )
    if len(used_baseline_sources) != len(set(used_baseline_sources)):
        blockers.append("baselines.preregistration_source_reused")
    missing_gates = sorted(set(REQUIRED_OFFLINE_GATE_IDS) - set(gate_by_id))
    blockers.extend(f"validation.required_gate_missing:{gate}" for gate in missing_gates)
    for gate_id in REQUIRED_OFFLINE_GATE_IDS:
        gate = gate_by_id.get(gate_id)
        if gate is not None and not gate.requires_external_truth:
            blockers.append(f"validation.required_gate_not_heldout:{gate_id}")
    for gate in validation_manifest.gates:
        if gate.category not in _ALLOWED_SCIENCE_CATEGORIES:
            blockers.append(f"validation.non_science_category:{gate.category}")

    if metric_bundle.input_contract_sha256 != replay_input.sha256:
        blockers.append("metrics.input_contract_mismatch")
    if metric_bundle.candidate_manifest_sha256 != candidate_manifest_sha:
        blockers.append("metrics.candidate_manifest_mismatch")
    if metric_bundle.baseline_manifest_sha256 != baseline_manifest_sha:
        blockers.append("metrics.baseline_manifest_mismatch")
    if metric_bundle.external_truth_context_sha256 != truth_sha:
        blockers.append("metrics.truth_context_mismatch")
    metric_by_gate = {item.gate_id: item for item in metric_bundle.evidence}
    missing_metrics = sorted(set(REQUIRED_OFFLINE_GATE_IDS) - set(metric_by_gate))
    blockers.extend(f"metrics.required_metric_missing:{gate}" for gate in missing_metrics)
    for gate_id, item in metric_by_gate.items():
        gate = gate_by_id.get(gate_id)
        if gate is None:
            continue
        if gate.requires_external_truth:
            if item.partition is not CorpusPartition.TEST:
                blockers.append(f"metrics.{gate_id}:partition_not_test")
            if item.event_ids != expected_events:
                blockers.append(f"metrics.{gate_id}:event_coverage_mismatch")
            if item.event_count != len(expected_events):
                blockers.append(f"metrics.{gate_id}:event_count_mismatch")
            if not item.external_truth_available:
                blockers.append(f"metrics.{gate_id}:external_truth_false")

    validation_report: ValidationReport | None = None
    try:
        validation_report = evaluate_manifest(
            validation_manifest,
            metric_bundle.evidence,
            external_truth=external_truth,
        )
    except (TypeError, ValueError) as exc:
        # Exception text can contain caller-controlled identifiers.  The
        # durable receipt therefore records only the bounded exception class.
        blockers.append(f"validation.evaluation_error:{type(exc).__name__}")
    if validation_report is not None and not validation_report.release_ready:
        blockers.extend(
            f"validation.report:{reason}"
            for reason in validation_report.blocking_reasons
        )

    canonical_blockers = tuple(sorted(set(blockers)))
    return OfflineReplayAcceptanceReceipt(
        input_contract_sha256=replay_input.sha256,
        component_inventory_sha256=component_inventory_sha,
        candidate_manifest_sha256=candidate_manifest_sha,
        baseline_manifest_sha256=baseline_manifest_sha,
        metric_bundle_sha256=metric_bundle.sha256,
        validation_manifest_sha256=validation_manifest.sha256,
        validation_report=validation_report,
        stages=ordered_stages,
        baselines=ordered_baselines,
        blocking_reasons=canonical_blockers,
        offline_science_gate_passed=not canonical_blockers,
    )


def build_preflight_mapping(
    replay_input: OfflineReplayInput,
    *,
    component_sources: tuple[ScienceComponentSource, ...],
) -> dict[str, Any]:
    """Emit an honest local preflight without pretending results exist."""

    ordered = tuple(sorted(component_sources, key=lambda item: item.stage.value))
    _unique_enum(ordered, "stage", "component source")
    _coverage = {item.stage for item in ordered}
    blockers = []
    _coverage_blockers(
        blockers,
        "component_sources",
        _coverage,
        set(REQUIRED_SCIENCE_STAGES),
    )
    blockers.extend(
        (
            "baseline_evidence_not_supplied",
            "candidate_stage_evidence_not_supplied",
            "external_truth_not_evaluated",
            "metric_bundle_not_supplied",
        )
    )
    raw_manifest = next(
        artifact
        for artifact in replay_input.artifacts
        if artifact.role is ArtifactRole.RAW_INPUT_MANIFEST
    )
    if raw_manifest.sha256 != _sha_mapping(
        [frame.to_mapping() for frame in replay_input.frames]
    ):
        blockers.append("input.raw_manifest_sha_mismatch")
    return {
        "admission_scope": OFFLINE_REPLAY_ADMISSION_SCOPE,
        "component_inventory": [item.to_mapping() for item in ordered],
        "component_inventory_sha256": _sha_mapping(
            [item.to_mapping() for item in ordered]
        ),
        "contract_version": OFFLINE_REPLAY_ACCEPTANCE_CONTRACT,
        "dashboard_publication_allowed": False,
        "input": replay_input.to_mapping(),
        "live_wiring_allowed": False,
        "meteorological_admission": False,
        "offline_science_gate_passed": False,
        "preflight_blockers": sorted(set(blockers)),
    }


def _mapping_item(value: Any, label: str, field: str) -> Any:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be an object")
    expected = (
        {"event_id", "frame_id", "raw_frame_sha256", "valid_time_utc"}
        if label == "frame"
        else {"artifact_id", "media_type", "role", "sha256", "size_bytes"}
    )
    if set(value) != expected:
        raise ValueError(f"{label} has unexpected/missing keys")
    return value[field]


def _unique_enum(values: tuple[Any, ...], field: str, label: str) -> None:
    identities = tuple(getattr(item, field) for item in values)
    if len(identities) != len(set(identities)):
        raise ValueError(f"duplicate {label} identity")


def _coverage_blockers(
    blockers: list[str],
    label: str,
    actual: set[Enum],
    expected: set[Enum],
) -> None:
    blockers.extend(
        f"{label}.missing:{value.value}"
        for value in sorted(expected - actual, key=lambda item: item.value)
    )
    blockers.extend(
        f"{label}.unexpected:{value.value}"
        for value in sorted(actual - expected, key=lambda item: item.value)
    )
