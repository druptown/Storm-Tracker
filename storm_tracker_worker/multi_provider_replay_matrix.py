"""Offline, fail-closed multi-provider replay evidence.

This module is deliberately inert: it is not imported by the worker service,
Home Assistant, MQTT, the acquisition scheduler or snapshot publication.  It
turns explicitly supplied local fixtures into either:

* structural/categorical multi-frame science-shadow replays; or
* single-frame decoder/grid/accounting probes that cannot make a motion claim.

The public KMI WCS composite is intentionally *not* called RADQPE.  Its raw
positive values are converted to one derived structural category only after
the existing GeoTIFF audit has confirmed the exact container and grid.  That
category has no precipitation quantity or unit and can never grant admission.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import tempfile
import time
from typing import Any, Iterable, Mapping, Sequence

from .analysis_grid_transform import analysis_grid_sha256
from .calibration_replay import (
    NormalizedCacheFrame,
    NormalizedCacheSequence,
    NormalizedRasterReplayView,
    build_normalized_raster_replay_view,
)
from .evidence_to_cells_shadow import EvidenceToCellsPixelAccounting
from .hard_decoder_isolation import _canonical_json_bytes
from .science_shadow_calibration_replay import (
    calibration_replay_detector_config,
    calibration_replay_tracking_policy,
)
from .science_shadow_orchestrator import (
    DurableScienceShadowOrchestrator,
    ScienceShadowLimits,
    ShadowCompositionResult,
    ShadowEvidenceFrame,
)
from .tracking_core.engine.cell_track_engine import CellTrackEnginePolicy
from .tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
    detect_hierarchical_cells,
)
from .tracking_core.engine.kmi_radqpe_contract import (
    KmiRadqpeContractError,
    audit_public_wcs_geotiff,
    parse_public_wcs_description,
    parse_utc,
)


MULTI_PROVIDER_REPLAY_MATRIX_CONTRACT = "stv3-multi-provider-replay-matrix-v1"
CATEGORICAL_REPLAY_COMPOSER_VERSION = (
    "stv3-multi-provider-categorical-composer-v1"
)
MATRIX_TRACKING_PROFILE_ID = "multi-provider-categorical-shadow-tracking-v1"
MAX_SOURCE_FILES = 256
MAX_SOURCE_FILE_BYTES = 128 * 1024**2
MAX_SOURCE_TREE_BYTES = 4 * 1024**3
MAX_REPLAY_FRAMES = 64
MAX_SOURCE_GRID_CELLS = 2_000_000
MAX_ANALYSIS_PIXELS = 30_000
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_RE = re.compile(r"^run-[0-9a-f]{64}$")


class MultiProviderReplayError(RuntimeError):
    """A fixture, replay or accounting contract is absent or differs."""


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_mapping(value: Any) -> str:
    return _sha_bytes(_canonical_json_bytes(value))


def _require_sha(value: object, label: str) -> str:
    if type(value) is not str or _SHA_RE.fullmatch(value) is None:
        raise MultiProviderReplayError(f"{label} is not a SHA-256")
    return value


def _json_value(value: Any) -> Any:
    """Convert immutable tuples to the closed canonical JSON data model."""

    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return value


def _read_regular(
    path: Path,
    *,
    maximum_bytes: int,
    allow_empty: bool = False,
) -> bytes:
    """Read one non-symlink regular file and detect path replacement."""

    if not path.is_absolute() or path.is_symlink():
        raise MultiProviderReplayError("source path is not an absolute regular file")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise MultiProviderReplayError("source file cannot be opened") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size < (0 if allow_empty else 1)
            or before.st_size > maximum_bytes
        ):
            raise MultiProviderReplayError("source file type/size is invalid")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, maximum_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum_bytes:
                raise MultiProviderReplayError("source file exceeds byte bound")
        after = os.fstat(descriptor)
        if (
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            or total != before.st_size
        ):
            raise MultiProviderReplayError("source file changed while reading")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class SourceFileDigest:
    relative_name: str
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.relative_name, str)
            or not self.relative_name
            or self.relative_name.startswith(("/", "\\"))
            or ".." in Path(self.relative_name).parts
            or isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes < 0
        ):
            raise ValueError("source file digest is invalid")
        _require_sha(self.sha256, "source file SHA")

    def to_mapping(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SourceTreeSnapshot:
    manifest_sha256: str
    total_bytes: int
    files: tuple[SourceFileDigest, ...]

    def __post_init__(self) -> None:
        _require_sha(self.manifest_sha256, "source manifest SHA")
        if (
            isinstance(self.total_bytes, bool)
            or not isinstance(self.total_bytes, int)
            or self.total_bytes <= 0
            or not self.files
            or len(self.files) > MAX_SOURCE_FILES
            or self.files != tuple(sorted(self.files, key=lambda item: item.relative_name))
            or len({item.relative_name for item in self.files}) != len(self.files)
            or sum(item.size_bytes for item in self.files) != self.total_bytes
        ):
            raise ValueError("source tree snapshot is invalid")
        expected = _sha_mapping([item.to_mapping() for item in self.files])
        if self.manifest_sha256 != expected:
            raise ValueError("source tree manifest checksum differs")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "files": [item.to_mapping() for item in self.files],
            "manifest_sha256": self.manifest_sha256,
            "total_bytes": self.total_bytes,
        }


def snapshot_source_tree(path: str | Path) -> SourceTreeSnapshot:
    """Content-address a bounded local source without exposing an absolute path."""

    root = Path(path).resolve(strict=True)
    if root.is_symlink():
        raise MultiProviderReplayError("source root may not be a symlink")
    candidates = (root,) if root.is_file() else tuple(root.rglob("*"))
    files: list[SourceFileDigest] = []
    total = 0
    for candidate in candidates:
        if candidate.is_dir():
            continue
        if candidate.is_symlink():
            raise MultiProviderReplayError("source tree contains a symlink")
        try:
            info = candidate.lstat()
        except OSError as exc:
            raise MultiProviderReplayError("source tree cannot be inventoried") from exc
        if not stat.S_ISREG(info.st_mode):
            raise MultiProviderReplayError("source tree contains a non-regular entry")
        # Cross-process coordination files are mutable control state, not
        # source evidence.  Empty lock sentinels may appear/disappear during a
        # read-only store session and are therefore excluded from the exact
        # artifact manifest; every database/profile/object byte remains bound.
        if info.st_size == 0 and candidate.name.lower().endswith(".lock"):
            continue
        if not 0 <= info.st_size <= MAX_SOURCE_FILE_BYTES:
            raise MultiProviderReplayError("source tree file exceeds byte bound")
        body = _read_regular(
            candidate,
            maximum_bytes=MAX_SOURCE_FILE_BYTES,
            allow_empty=True,
        )
        relative = candidate.name if root.is_file() else candidate.relative_to(root).as_posix()
        files.append(SourceFileDigest(relative, len(body), _sha_bytes(body)))
        total += len(body)
        if len(files) > MAX_SOURCE_FILES or total > MAX_SOURCE_TREE_BYTES:
            raise MultiProviderReplayError("source tree exceeds bounded inventory")
    ordered = tuple(sorted(files, key=lambda item: item.relative_name))
    if not ordered:
        raise MultiProviderReplayError("source tree contains no files")
    return SourceTreeSnapshot(
        manifest_sha256=_sha_mapping([item.to_mapping() for item in ordered]),
        total_bytes=total,
        files=ordered,
    )


@dataclass(frozen=True, slots=True)
class CategoricalSourceFrame:
    run_id: str
    source_content_sha256: str
    source_size_bytes: int
    frame: NormalizedCacheFrame

    def __post_init__(self) -> None:
        if _RUN_RE.fullmatch(self.run_id) is None:
            raise ValueError("categorical frame run identity is invalid")
        _require_sha(self.source_content_sha256, "categorical source SHA")
        if (
            isinstance(self.source_size_bytes, bool)
            or not isinstance(self.source_size_bytes, int)
            or self.source_size_bytes <= 0
        ):
            raise ValueError("categorical source byte count is invalid")


@dataclass(frozen=True, slots=True)
class CategoricalReplaySequence:
    sequence_id: str
    provider: str
    material_kind: str
    cadence_seconds: int
    source_snapshot: SourceTreeSnapshot
    frames: tuple[CategoricalSourceFrame, ...]
    caveats: tuple[str, ...]
    blockers: tuple[str, ...]
    meteorological_admission_granted: bool = False
    motion_claim_generated: bool = False
    operational_snapshot_claim_generated: bool = False

    def __post_init__(self) -> None:
        if (
            not isinstance(self.sequence_id, str)
            or not self.sequence_id
            or not isinstance(self.provider, str)
            or not self.provider
            or self.material_kind not in (
                "structural_categorical_from_exact_geotiff",
                "derived_categorical_normalized_cache",
            )
            or isinstance(self.cadence_seconds, bool)
            or not isinstance(self.cadence_seconds, int)
            or self.cadence_seconds <= 0
            or not 2 <= len(self.frames) <= MAX_REPLAY_FRAMES
            or not self.caveats
            or "hard_process_isolation_not_wired" not in self.blockers
            or self.meteorological_admission_granted
            or self.motion_claim_generated
            or self.operational_snapshot_claim_generated
        ):
            raise ValueError("categorical replay sequence contract differs")
        timestamps = tuple(item.frame.nominal_timestamp for item in self.frames)
        if timestamps != tuple(sorted(set(timestamps))) or any(
            later - earlier != self.cadence_seconds
            for earlier, later in zip(timestamps, timestamps[1:])
        ):
            raise ValueError("categorical replay is not one contiguous sequence")
        geometry = self.frames[0].frame.geometry_key
        if any(item.frame.geometry_key != geometry for item in self.frames):
            raise ValueError("categorical replay contains multiple geometries")
        if len({item.run_id for item in self.frames}) != len(self.frames):
            raise ValueError("categorical replay run identities are duplicated")


KMI_PUBLIC_CAVEATS = (
    "public_wcs_composite_is_not_verified_as_native_radqpe",
    "positive_raw_value_is_only_a_derived_structural_category",
    "precipitation_quantity_unknown",
    "precipitation_unit_unknown",
    "nodata_semantics_unknown",
    "categorical_association_is_not_meteorological_motion_truth",
    "public_data_reuse_rights_not_admitted_for_operation",
)
HOUSTON_CAVEATS = (
    "replay_json_is_derived_normalized_cache_not_raw_rainviewer_tiles",
    "rainviewer_categories_are_not_physical_precipitation_units",
    "provider_nodata_mask_unavailable",
    "categorical_association_is_not_meteorological_motion_truth",
    "cohorts_are_split_on_geometry_or_cadence_discontinuity",
)
COMMON_BLOCKERS = (
    "hard_process_isolation_not_wired",
    "independent_identity_truth_unavailable",
    "independent_motion_truth_unavailable",
    "operational_admission_certificates_incomplete",
    "operational_snapshot_publication_forbidden",
)


def _sequence_id(payload: Mapping[str, Any]) -> str:
    return "sequence-" + _sha_mapping(payload)


def _run_id(
    *,
    sequence_id: str,
    timestamp: int,
    source_sha256: str,
) -> str:
    return "run-" + _sha_mapping({
        "contract": MULTI_PROVIDER_REPLAY_MATRIX_CONTRACT,
        "sequence_id": sequence_id,
        "source_sha256": source_sha256,
        "timestamp": timestamp,
    })


def load_kmi_public_composite_sequence(
    source_directory: str | Path,
) -> CategoricalReplaySequence:
    """Load exactly 20 real public KMI WCS frames as structural categories."""

    root = Path(source_directory).resolve(strict=True)
    snapshot = snapshot_source_tree(root)
    receipt_body = _read_regular(root / "fetch_receipt.json", maximum_bytes=2 * 1024**2)
    description_body = _read_regular(root / "wcs_description.xml", maximum_bytes=1024**2)
    try:
        receipt = json.loads(receipt_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MultiProviderReplayError("KMI capture receipt is not JSON") from exc
    if not isinstance(receipt, dict) or receipt.get("schema") != (
        "kmi-public-radar-composite-capture-v1"
    ):
        raise MultiProviderReplayError("KMI capture receipt schema differs")
    gates = receipt.get("gates")
    if not isinstance(gates, dict) or any(
        gates.get(key) is not False
        for key in (
            "operational_cutover_allowed",
            "precipitation_quantity_verified",
            "precipitation_unit_verified",
            "nodata_semantics_verified",
            "product_identity_verified_as_radqpe",
        )
    ):
        raise MultiProviderReplayError("KMI public semantic gates are not closed")
    artifacts = receipt.get("artifacts")
    if not isinstance(artifacts, list):
        raise MultiProviderReplayError("KMI receipt artifacts are invalid")
    coverage = tuple(
        item for item in artifacts
        if isinstance(item, dict) and item.get("role") == "public_wcs_coverage"
    )
    if len(coverage) != 20:
        raise MultiProviderReplayError("KMI public sequence is not exactly 20 frames")
    description = parse_public_wcs_description(description_body)
    try:
        import numpy as np
        from PIL import Image
    except ImportError as exc:
        raise MultiProviderReplayError("KMI replay requires NumPy and Pillow") from exc

    ordered = tuple(sorted(coverage, key=lambda item: str(item.get("nominal_at"))))
    timestamps = tuple(int(parse_utc(str(item.get("nominal_at")))) for item in ordered)
    if any(later - earlier != 300 for earlier, later in zip(timestamps, timestamps[1:])):
        raise MultiProviderReplayError("KMI public sequence cadence differs")
    sequence_id = _sequence_id({
        "frames": [
            {
                "nominal_at": item.get("nominal_at"),
                "sha256": (item.get("transport") or {}).get("sha256"),
            }
            for item in ordered
        ],
        "material_kind": "structural_categorical_from_exact_geotiff",
        "source_manifest_sha256": snapshot.manifest_sha256,
    })
    frames: list[CategoricalSourceFrame] = []
    for artifact, timestamp in zip(ordered, timestamps, strict=True):
        filename = artifact.get("local_file")
        transport = artifact.get("transport")
        semantics = artifact.get("meteorological_semantics")
        if (
            not isinstance(filename, str)
            or Path(filename).name != filename
            or not isinstance(transport, dict)
            or not isinstance(semantics, dict)
            or semantics.get("detection_eligible") is not False
            or semantics.get("quantity") != "UNKNOWN"
            or semantics.get("unit") != "UNKNOWN"
            or semantics.get("nodata") != "UNKNOWN"
        ):
            raise MultiProviderReplayError("KMI public frame semantics differ")
        path = (root / filename).resolve(strict=True)
        if path.parent != root or path.is_symlink():
            raise MultiProviderReplayError("KMI frame path escapes source root")
        body = _read_regular(path, maximum_bytes=16 * 1024**2)
        digest = _sha_bytes(body)
        if (
            digest != _require_sha(transport.get("sha256"), "KMI receipt SHA")
            or transport.get("received_bytes") != len(body)
        ):
            raise MultiProviderReplayError("KMI frame differs from capture receipt")
        try:
            audited = audit_public_wcs_geotiff(body, description)
            with Image.open(path) as image:
                image.load()
                values = np.asarray(image)
        except (KmiRadqpeContractError, OSError, ValueError) as exc:
            raise MultiProviderReplayError("KMI GeoTIFF structural audit failed") from exc
        if audited.payload_sha256 != digest or values.shape != (
            audited.height,
            audited.width,
        ):
            raise MultiProviderReplayError("KMI audited pixels differ from source")
        # This threshold is a declared structural heuristic, not a rain-rate
        # or dry/wet observation.  Nonpositive and unknown negative codes map
        # to category zero solely for this exploratory footprint replay.
        indices = np.argwhere(np.isfinite(values) & (values > 0.0))
        cells = tuple((int(row), int(column), 2.0) for row, column in indices)
        frame = NormalizedCacheFrame(
            region_id="kmi-public-composite-belgium",
            source="kmi_public_radar_composite_structural_category",
            nominal_timestamp=timestamp,
            saved_at=float(timestamp),
            bounds=tuple(float(item) for item in audited.bounds),
            grid_deg=abs(float(audited.transform[0])),
            height=audited.height,
            width=audited.width,
            cells=cells,
            input_runs=1,
        )
        frames.append(CategoricalSourceFrame(
            run_id=_run_id(
                sequence_id=sequence_id,
                timestamp=timestamp,
                source_sha256=digest,
            ),
            source_content_sha256=digest,
            source_size_bytes=len(body),
            frame=frame,
        ))
    if snapshot_source_tree(root) != snapshot:
        raise MultiProviderReplayError("KMI source changed while loading")
    return CategoricalReplaySequence(
        sequence_id=sequence_id,
        provider="kmi_public_radar_composite",
        material_kind="structural_categorical_from_exact_geotiff",
        cadence_seconds=300,
        source_snapshot=snapshot,
        frames=tuple(frames),
        caveats=KMI_PUBLIC_CAVEATS,
        blockers=COMMON_BLOCKERS + (
            "public_composite_quantity_unit_nodata_unverified",
            "native_kmi_radqpe_capture_unavailable",
            "kmi_reuse_rights_not_operationally_cleared",
        ),
    )


def load_houston_rainviewer_sequences(
    replay_path: str | Path,
) -> tuple[CategoricalReplaySequence, ...]:
    """Split every Houston frame into complete geometry/cadence cohorts."""

    path = Path(replay_path).resolve(strict=True)
    snapshot = snapshot_source_tree(path)
    body = _read_regular(path, maximum_bytes=64 * 1024**2)
    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MultiProviderReplayError("Houston replay is not JSON") from exc
    if not isinstance(document, dict) or not isinstance(document.get("frames"), list):
        raise MultiProviderReplayError("Houston replay schema differs")
    decoded: list[tuple[NormalizedCacheFrame, str, int]] = []
    for wrapper in document["frames"]:
        if not isinstance(wrapper, dict) or type(wrapper.get("frame_json")) is not str:
            raise MultiProviderReplayError("Houston replay frame wrapper differs")
        raw = wrapper["frame_json"].encode("utf-8")
        try:
            value = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise MultiProviderReplayError("Houston embedded frame is invalid") from exc
        if not isinstance(value, dict) or set(value) != {
            "bounds", "cells", "grid_deg", "height", "input_runs",
            "nominal_timestamp", "source", "width",
        }:
            raise MultiProviderReplayError("Houston embedded frame schema differs")
        try:
            frame = NormalizedCacheFrame(
                region_id="houston",
                source=str(value["source"]),
                nominal_timestamp=int(value["nominal_timestamp"]),
                saved_at=float(value["nominal_timestamp"]),
                bounds=tuple(float(item) for item in value["bounds"]),
                grid_deg=float(value["grid_deg"]),
                height=int(value["height"]),
                width=int(value["width"]),
                cells=tuple(
                    (int(item[0]), int(item[1]), float(item[2]))
                    for item in value["cells"]
                ),
                input_runs=int(value["input_runs"]),
            )
        except (TypeError, ValueError, IndexError) as exc:
            raise MultiProviderReplayError("Houston embedded frame values differ") from exc
        if (
            frame.source != "rainviewer"
            or frame.height <= 0
            or frame.width <= 0
            or frame.height * frame.width > MAX_SOURCE_GRID_CELLS
            or len(frame.cells) != len({(r, c) for r, c, _v in frame.cells})
            or any(
                row < 0 or row >= frame.height or column < 0 or column >= frame.width
                or not math.isfinite(intensity) or intensity <= 0.0
                for row, column, intensity in frame.cells
            )
        ):
            raise MultiProviderReplayError("Houston categorical frame is invalid")
        decoded.append((frame, _sha_bytes(raw), len(raw)))
    if not 2 <= len(decoded) <= MAX_REPLAY_FRAMES:
        raise MultiProviderReplayError("Houston replay frame count is invalid")

    by_geometry: dict[tuple[Any, ...], list[tuple[NormalizedCacheFrame, str, int]]] = {}
    for item in decoded:
        by_geometry.setdefault(item[0].geometry_key, []).append(item)
    cohorts: list[list[tuple[NormalizedCacheFrame, str, int]]] = []
    for values in by_geometry.values():
        ordered = sorted(values, key=lambda item: item[0].nominal_timestamp)
        current: list[tuple[NormalizedCacheFrame, str, int]] = []
        for item in ordered:
            if current and item[0].nominal_timestamp - current[-1][0].nominal_timestamp != 600:
                cohorts.append(current)
                current = []
            current.append(item)
        if current:
            cohorts.append(current)
    if any(len(items) < 2 for items in cohorts):
        raise MultiProviderReplayError("Houston contains an unaccounted singleton cohort")
    sequences: list[CategoricalReplaySequence] = []
    for index, items in enumerate(sorted(
        cohorts,
        key=lambda values: values[0][0].nominal_timestamp,
    ), start=1):
        sequence_id = _sequence_id({
            "cohort": index,
            "frame_sha256": [item[1] for item in items],
            "material_kind": "derived_categorical_normalized_cache",
            "source_manifest_sha256": snapshot.manifest_sha256,
        })
        frames = tuple(CategoricalSourceFrame(
            run_id=_run_id(
                sequence_id=sequence_id,
                timestamp=item[0].nominal_timestamp,
                source_sha256=item[1],
            ),
            source_content_sha256=item[1],
            source_size_bytes=item[2],
            frame=item[0],
        ) for item in items)
        sequences.append(CategoricalReplaySequence(
            sequence_id=sequence_id,
            provider="rainviewer_houston",
            material_kind="derived_categorical_normalized_cache",
            cadence_seconds=600,
            source_snapshot=snapshot,
            frames=frames,
            caveats=HOUSTON_CAVEATS,
            blockers=COMMON_BLOCKERS + (
                "rainviewer_raw_tiles_not_bound_to_replay_json",
                "normalized_categories_not_quantitative",
            ),
        ))
    if sum(len(item.frames) for item in sequences) != len(decoded):
        raise MultiProviderReplayError("Houston frame accounting is incomplete")
    if snapshot_source_tree(path) != snapshot:
        raise MultiProviderReplayError("Houston source changed while loading")
    return tuple(sequences)


def matrix_tracking_policy(
    detector: HierarchicalDetectorConfig,
    *,
    maximum_analysis_pixels: int,
) -> CellTrackEnginePolicy:
    """Build the explicit ungated v4 replay policy used by this matrix."""

    base = calibration_replay_tracking_policy(
        detector,
        maximum_analysis_pixels=maximum_analysis_pixels,
    )
    identity = base.multi_hypothesis_identity
    if identity is None:
        raise MultiProviderReplayError("matrix requires multi-hypothesis identity")
    multi_scan = identity.multi_scan
    if multi_scan is None:
        raise MultiProviderReplayError("matrix requires multi-scan identity")
    return replace(
        base,
        policy_id=MATRIX_TRACKING_PROFILE_ID,
        multi_hypothesis_identity=replace(
            identity,
            policy_id="multi-provider-categorical-multi-hypothesis-v1",
            minimum_system_translation_support_tracks=1,
            multi_scan=replace(
                multi_scan,
                policy_id="multi-provider-categorical-multi-scan-v1",
            ),
        ),
    )


class CategoricalReplayComposer:
    """Raw-hash-bound adapter from one categorical sequence to science input."""

    def __init__(
        self,
        sequence: CategoricalReplaySequence,
        *,
        detector_config: HierarchicalDetectorConfig,
        maximum_source_grid_cells: int = MAX_SOURCE_GRID_CELLS,
        maximum_analysis_pixels: int = MAX_ANALYSIS_PIXELS,
        maximum_runtime_seconds: float = 120.0,
    ) -> None:
        if not isinstance(sequence, CategoricalReplaySequence):
            raise TypeError("sequence must be CategoricalReplaySequence")
        if (
            isinstance(maximum_source_grid_cells, bool)
            or not isinstance(maximum_source_grid_cells, int)
            or maximum_source_grid_cells <= 0
            or isinstance(maximum_analysis_pixels, bool)
            or not isinstance(maximum_analysis_pixels, int)
            or maximum_analysis_pixels <= 0
            or not math.isfinite(float(maximum_runtime_seconds))
            or not 0.05 <= float(maximum_runtime_seconds) <= 300.0
        ):
            raise ValueError("categorical composer limits are invalid")
        cache_sequence = NormalizedCacheSequence(
            sequence_id=sequence.sequence_id,
            region_id=sequence.frames[0].frame.region_id,
            source=sequence.frames[0].frame.source,
            cadence_seconds=sequence.cadence_seconds,
            frames=tuple(item.frame for item in sequence.frames),
            minimum_frame_count=2,
        )
        view = build_normalized_raster_replay_view(
            cache_sequence,
            maximum_source_grid_cells=maximum_source_grid_cells,
            maximum_analysis_pixels=maximum_analysis_pixels,
        )
        self._sequence = sequence
        self._view = view
        self._detector = detector_config
        self._maximum_runtime_seconds = float(maximum_runtime_seconds)
        self._frames = dict(zip(
            (item.run_id for item in sequence.frames),
            view.frames,
            strict=True,
        ))
        self._sources = {item.run_id: item for item in sequence.frames}
        self._detector_sha256 = _sha_mapping(asdict(detector_config))
        self._profile_sha256 = _sha_mapping({
            "algorithm": CATEGORICAL_REPLAY_COMPOSER_VERSION,
            "blockers": list(sequence.blockers),
            "caveats": list(sequence.caveats),
            "detector": asdict(detector_config),
            "material_kind": sequence.material_kind,
            "maximum_analysis_pixels": maximum_analysis_pixels,
            "maximum_source_grid_cells": maximum_source_grid_cells,
            "provider": sequence.provider,
            "sequence_id": sequence.sequence_id,
            "source_manifest_sha256": sequence.source_snapshot.manifest_sha256,
            "transform_fingerprint": view.transform.fingerprint,
        })

    @property
    def profile_sha256(self) -> str:
        return self._profile_sha256

    @property
    def detector_config(self) -> HierarchicalDetectorConfig:
        return self._detector

    @property
    def maximum_runtime_seconds(self) -> float:
        return self._maximum_runtime_seconds

    @property
    def view(self) -> NormalizedRasterReplayView:
        return self._view

    def compose(self, *, run_id: str, deadline_at: float, monotonic) -> ShadowCompositionResult:
        if float(monotonic()) >= deadline_at:
            from .evidence_to_cells_shadow import EvidenceToCellsShadowDeadlineError
            raise EvidenceToCellsShadowDeadlineError(
                "categorical replay composition deadline expired"
            )
        frame = self._frames.get(run_id)
        source = self._sources.get(run_id)
        if frame is None or source is None:
            raise MultiProviderReplayError("run is absent from categorical sequence")
        detected = detect_hierarchical_cells(frame, self._detector)
        flattened = tuple(value for row in frame.values for value in row)
        valid = sum(value is not None for value in flattened)
        shield = sum(
            value is not None and value >= self._detector.shield_threshold
            for value in flattened
        )
        core = sum(
            value is not None and value >= self._detector.core_threshold
            for value in flattened
        )
        accounting = EvidenceToCellsPixelAccounting(
            total_analysis_pixels=len(flattened),
            valid_detector_input_pixels=valid,
            valid_below_shield_threshold_pixels=valid - shield,
            rain_shield_pixels=shield,
            embedded_core_pixels=core,
            excluded_nodata_pixels=len(flattened) - valid,
            excluded_undetect_pixels=0,
            excluded_mixed_pixels=0,
            excluded_outside_support_pixels=0,
            detection_objects=len(detected.detections),
            rain_shield_objects=len(detected.rain_shields),
            embedded_core_objects=len(detected.embedded_cores),
            low_confidence_objects=len(detected.low_confidence_fragments),
        )
        values_sha = _sha_mapping([list(row) for row in frame.values])
        grid_sha = analysis_grid_sha256(frame.grid)
        transform_sha = _sha_mapping({
            "frame_id": frame.frame_id,
            "source_content_sha256": source.source_content_sha256,
            "source_manifest_sha256": self._sequence.source_snapshot.manifest_sha256,
            "transform_fingerprint": self._view.transform.fingerprint,
            "values_sha256": values_sha,
        })
        evidence_sha = _sha_mapping({
            "accounting": accounting.to_mapping(),
            "detector_sha256": self._detector_sha256,
            "frame_id": frame.frame_id,
            "grid_sha256": grid_sha,
            "run_id": run_id,
            "source_content_sha256": source.source_content_sha256,
            "source_manifest_sha256": self._sequence.source_snapshot.manifest_sha256,
            "transform_receipt_sha256": transform_sha,
            "values_sha256": values_sha,
        })
        semantics_sha = _sha_mapping({
            "caveats": list(self._sequence.caveats),
            "material_kind": self._sequence.material_kind,
            "motion_claim_generated": False,
            "operational_admission": False,
            "provider": self._sequence.provider,
            "quantity": frame.quantity,
            "unit": frame.unit,
        })
        analysis_sha = _sha_mapping({
            "frame_id": frame.frame_id,
            "grid_sha256": grid_sha,
            "values_sha256": values_sha,
        })
        evidence = ShadowEvidenceFrame(
            canonical_run_id=run_id,
            canonical_commit_id="commit-" + _sha_mapping({
                "run": run_id,
                "source": source.source_content_sha256,
            }),
            canonical_receipt_sha256=_sha_mapping({
                "run": run_id,
                "source": source.source_content_sha256,
            }),
            canonical_result_sha256=evidence_sha,
            capture_id="capture-offline-replay-" + source.source_content_sha256,
            capture_receipt_sha256=_sha_mapping({
                "bytes": source.source_size_bytes,
                "sha256": source.source_content_sha256,
            }),
            capture_manifest_sha256=self._sequence.source_snapshot.manifest_sha256,
            evidence_candidate_id="evidence-cells-" + evidence_sha,
            evidence_content_sha256=evidence_sha,
            native_candidate_id="categorical-field-" + source.source_content_sha256,
            native_candidate_content_sha256=source.source_content_sha256,
            native_lineage_sha256=_sha_mapping({
                "material_kind": self._sequence.material_kind,
                "source": source.source_content_sha256,
                "source_manifest": self._sequence.source_snapshot.manifest_sha256,
            }),
            analysis_field_id="analysis-field-" + analysis_sha,
            analysis_field_content_sha256=analysis_sha,
            analysis_lineage_sha256=_sha_mapping({
                "native": source.source_content_sha256,
                "transform": transform_sha,
            }),
            radar_semantics_certificate_id=(
                "exploratory.categorical-structure-not-physical.v1"
            ),
            radar_semantics_certificate_sha256=semantics_sha,
            transform_certificate_id=(
                "exploratory.bounded-spatial-bin-maximum.v1"
            ),
            transform_certificate_sha256=self._view.transform.fingerprint,
            transform_receipt_id="transform-receipt-" + transform_sha,
            transform_receipt_sha256=transform_sha,
            transform_config_sha256=_sha_mapping({
                "maximum_analysis_pixels": self._view.transform.maximum_analysis_pixels,
                "maximum_source_grid_cells": self._view.transform.maximum_source_grid_cells,
            }),
            target_grid_sha256=grid_sha,
            detector_config_sha256=self._detector_sha256,
            pipeline_config_sha256=self._profile_sha256,
            normalized_frame=frame,
            detection_frame=detected,
            pixel_accounting=accounting,
            operational_admission_blockers=self._sequence.blockers,
        )
        return ShadowCompositionResult(run_id=run_id, evidence=evidence, issues=())


@dataclass(frozen=True, slots=True)
class SequenceReplayResult:
    sequence_id: str
    provider: str
    material_kind: str
    source_manifest_sha256_before: str
    source_manifest_sha256_after: str
    source_unchanged: bool
    frame_count: int
    cadence_seconds: int
    first_timestamp: int
    last_timestamp: int
    processed_frames: int
    advanced_frames: int
    duplicate_retries: int
    restart_count: int
    final_revision: int
    final_track_count: int
    candidate_edges: int
    expected_candidate_edges: int
    advected_pair_count: int
    unadvected_fallback_pair_count: int
    unique_association_evidence_count: int
    translation_available_frames: int
    motion_claim_generated: bool
    meteorological_admission_granted: bool
    operational_snapshot_claim_generated: bool
    runtime_profile_sha256: str
    receipt_head_sha256: str
    wall_seconds: float
    process_cpu_seconds: float
    rss_before_bytes: int | None
    rss_after_bytes: int | None
    process_peak_rss_bytes: int | None
    blockers: tuple[str, ...]

    def to_mapping(self) -> dict[str, Any]:
        return _json_value(asdict(self))


def _memory_bytes() -> tuple[int | None, int | None]:
    """Return current RSS and process-lifetime peak RSS where available."""

    try:
        import psutil
        current = int(psutil.Process(os.getpid()).memory_info().rss)
    except (ImportError, OSError):
        current = None
    peak: int | None = None
    if os.name == "nt":  # pragma: no cover - selected by platform
        try:
            import ctypes
            from ctypes import wintypes

            class _ProcessMemoryCounters(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = _ProcessMemoryCounters()
            counters.cb = ctypes.sizeof(counters)
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            psapi = ctypes.WinDLL("psapi", use_last_error=True)
            kernel.GetCurrentProcess.restype = wintypes.HANDLE
            psapi.GetProcessMemoryInfo.argtypes = [
                wintypes.HANDLE,
                ctypes.POINTER(_ProcessMemoryCounters),
                wintypes.DWORD,
            ]
            psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
            handle = kernel.GetCurrentProcess()
            success = psapi.GetProcessMemoryInfo(
                handle,
                ctypes.byref(counters),
                counters.cb,
            )
            if success:
                current = int(counters.WorkingSetSize)
                peak = int(counters.PeakWorkingSetSize)
        except (AttributeError, OSError, ValueError):
            pass
    else:  # pragma: no cover - selected by platform
        try:
            import resource

            value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            peak = value if value > 1024**2 else value * 1024
        except (ImportError, OSError, ValueError):
            pass
    return current, peak


def run_categorical_sequence(
    sequence: CategoricalReplaySequence,
    *,
    source_path: str | Path,
    receipt_path: str | Path,
    maximum_analysis_pixels: int = MAX_ANALYSIS_PIXELS,
    restart_after_frames: int | None = None,
) -> SequenceReplayResult:
    """Run every frame, recreate the orchestrator once, then retry exactly."""

    before = snapshot_source_tree(source_path)
    if before != sequence.source_snapshot:
        raise MultiProviderReplayError("replay source differs from loaded manifest")
    detector = calibration_replay_detector_config()
    composer = CategoricalReplayComposer(
        sequence,
        detector_config=detector,
        maximum_analysis_pixels=maximum_analysis_pixels,
    )
    policy = matrix_tracking_policy(
        detector,
        maximum_analysis_pixels=maximum_analysis_pixels,
    )
    limits = ScienceShadowLimits(
        max_receipts=max(128, len(sequence.frames) * 4),
        checkpoint_interval_frames=max(2, min(8, len(sequence.frames))),
        max_replay_frames=max(64, len(sequence.frames) * 2),
        max_runtime_seconds=120.0,
    )
    receipt = Path(receipt_path).resolve(strict=False)
    receipt.parent.mkdir(parents=True, exist_ok=True)

    def construct() -> DurableScienceShadowOrchestrator:
        return DurableScienceShadowOrchestrator(
            engine_id="matrix-" + _sha_mapping({
                "profile": composer.profile_sha256,
                "sequence": sequence.sequence_id,
            })[:32],
            composer=composer,
            tracking_policy=policy,
            receipt_path=receipt,
            limits=limits,
        )

    boundary = (
        max(1, len(sequence.frames) // 2)
        if restart_after_frames is None
        else restart_after_frames
    )
    if not 1 <= boundary < len(sequence.frames):
        raise ValueError("restart boundary must fall inside the sequence")
    orchestrator = construct()
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    rss_before, _peak_before = _memory_bytes()
    advanced = 0
    candidate_edges = 0
    expected_edges = 0
    advected_pairs = 0
    fallback_pairs = 0
    association_hashes: set[str] = set()
    translation_available = 0
    last_receipt_sha: str | None = None
    for index, item in enumerate(sequence.frames):
        if index == boundary:
            orchestrator = construct()
        result = orchestrator.process(
            attempt_id=f"matrix-{index + 1:03d}-{item.run_id[4:20]}",
            run_id=item.run_id,
            deadline_at=time.monotonic() + 120.0,
        )
        if result.duplicate or result.evidence is None or result.transition is None:
            raise MultiProviderReplayError("fresh replay frame did not advance")
        if result.receipt.outcome.value != "advanced":
            raise MultiProviderReplayError("science replay receipt is not advanced")
        if (
            result.receipt.meteorological_admission_granted
            or result.receipt.operational_snapshot_claim_generated
        ):
            raise MultiProviderReplayError("categorical replay escaped admission gate")
        advanced += 1
        last_receipt_sha = result.receipt.receipt_sha256
        snapshot = result.transition.identity_snapshot
        if snapshot is not None:
            candidate_edges += snapshot.candidate_edge_count
            expected_edges += snapshot.expected_candidate_edge_count
            translation_available += int(snapshot.translation.available)
            for candidate in snapshot.local_candidates:
                mode = candidate.advected_footprint_evidence.mode
                if mode == "advected":
                    advected_pairs += 1
                elif mode == "unadvected_fallback":
                    fallback_pairs += 1
                else:
                    raise MultiProviderReplayError("unknown advected footprint mode")
                association_hashes.add(candidate.association_evidence_sha256)
    duplicate = construct().process(
        attempt_id=f"matrix-duplicate-{sequence.frames[-1].run_id[4:20]}",
        run_id=sequence.frames[-1].run_id,
        deadline_at=time.monotonic() + 120.0,
    )
    if not duplicate.duplicate or duplicate.evidence is not None or duplicate.transition is not None:
        raise MultiProviderReplayError("durable replay duplicate is not exact")
    recovered = construct()
    recovered._recover(time.monotonic() + 120.0)
    state = recovered._state
    if state is None or last_receipt_sha is None:
        raise MultiProviderReplayError("durable replay state is absent")
    after = snapshot_source_tree(source_path)
    wall = time.perf_counter() - started_wall
    cpu = time.process_time() - started_cpu
    rss_after, peak_rss = _memory_bytes()
    return SequenceReplayResult(
        sequence_id=sequence.sequence_id,
        provider=sequence.provider,
        material_kind=sequence.material_kind,
        source_manifest_sha256_before=before.manifest_sha256,
        source_manifest_sha256_after=after.manifest_sha256,
        source_unchanged=before == after,
        frame_count=len(sequence.frames),
        cadence_seconds=sequence.cadence_seconds,
        first_timestamp=sequence.frames[0].frame.nominal_timestamp,
        last_timestamp=sequence.frames[-1].frame.nominal_timestamp,
        processed_frames=len(sequence.frames),
        advanced_frames=advanced,
        duplicate_retries=1,
        restart_count=1,
        final_revision=state.revision,
        final_track_count=len(state.ledger.tracks),
        candidate_edges=candidate_edges,
        expected_candidate_edges=expected_edges,
        advected_pair_count=advected_pairs,
        unadvected_fallback_pair_count=fallback_pairs,
        unique_association_evidence_count=len(association_hashes),
        translation_available_frames=translation_available,
        motion_claim_generated=False,
        meteorological_admission_granted=False,
        operational_snapshot_claim_generated=False,
        runtime_profile_sha256=recovered.runtime_profile_sha256,
        receipt_head_sha256=last_receipt_sha,
        wall_seconds=wall,
        process_cpu_seconds=cpu,
        rss_before_bytes=rss_before,
        rss_after_bytes=rss_after,
        process_peak_rss_bytes=peak_rss,
        blockers=sequence.blockers,
    )


def _only_capture_id(raw_root: Path) -> str:
    database_path = (raw_root / "raw_artifacts.sqlite3").resolve(strict=True)
    uri = f"{database_path.as_uri()}?mode=ro&immutable=1"
    try:
        database = sqlite3.connect(uri, uri=True)
        rows = database.execute(
            "SELECT capture_id FROM raw_artifact_captures ORDER BY capture_id"
        ).fetchall()
    except sqlite3.Error as exc:
        raise MultiProviderReplayError("raw golden capture lookup failed") from exc
    finally:
        if "database" in locals():
            database.close()
    if len(rows) != 1 or type(rows[0][0]) is not str:
        raise MultiProviderReplayError("raw golden must contain exactly one capture")
    return rows[0][0]


def _copy_source_tree_for_probe(
    source: Path,
    destination: Path,
    snapshot: SourceTreeSnapshot,
) -> Path:
    """Copy exact source evidence so store initialization never touches it."""

    if destination.exists() and any(destination.iterdir()):
        raise MultiProviderReplayError("probe raw-copy destination is not empty")
    destination.mkdir(parents=True, exist_ok=True)
    source_root = source.resolve(strict=True)
    for item in snapshot.files:
        original = (source_root / Path(item.relative_name)).resolve(strict=True)
        try:
            original.relative_to(source_root)
        except ValueError as exc:
            raise MultiProviderReplayError("probe source file escapes root") from exc
        body = _read_regular(
            original,
            maximum_bytes=MAX_SOURCE_FILE_BYTES,
            allow_empty=True,
        )
        if len(body) != item.size_bytes or _sha_bytes(body) != item.sha256:
            raise MultiProviderReplayError("probe source changed before copy")
        target = destination / Path(item.relative_name)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with target.open("xb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError as exc:
            raise MultiProviderReplayError("probe copy appeared concurrently") from exc
    return destination.resolve(strict=True)


@dataclass(frozen=True, slots=True)
class SingleFrameProbeResult:
    provider: str
    evidence_level: str
    outcome: str
    reason_code: str
    source_manifest_sha256_before: str
    source_manifest_sha256_after: str
    source_unchanged: bool
    source_file_count: int
    source_bytes: int
    frame_count: int
    grid_shape: tuple[int, int] | None
    total_pixels: int | None
    accounted_pixels: int | None
    capture_member_count: int | None
    accounted_members: int | None
    duplicate_retry_exact: bool | None
    motion_claim_generated: bool = False
    meteorological_admission_granted: bool = False
    operational_snapshot_claim_generated: bool = False
    blockers: tuple[str, ...] = ()

    def to_mapping(self) -> dict[str, Any]:
        return _json_value(asdict(self))


def probe_noaa_err_golden(
    raw_root: str | Path,
    work_root: str | Path,
) -> SingleFrameProbeResult:
    """Rerun the real NOAA ERR raw capture over its bounded golden region."""

    from .canonical_evidence_store import CanonicalEvidenceStore
    from .noaa_err_quantitative import (
        DECODER_CONFIG_SHA256,
        NoaaErrDerivedArtifactStore,
        NoaaErrRegion,
        decode_noaa_err_region,
    )
    from .raw_artifacts import RawArtifactStore

    source = Path(raw_root).resolve(strict=True)
    output = Path(work_root).resolve(strict=False)
    output.mkdir(parents=True, exist_ok=True)
    before = snapshot_source_tree(source)
    probe_source = _copy_source_tree_for_probe(
        source,
        output / "raw-copy",
        before,
    )
    raw = RawArtifactStore(probe_source)
    raw.initialize()
    capture_id = _only_capture_id(probe_source)
    capture = raw.load_capture_receipt(capture_id)
    if capture is None:
        raise MultiProviderReplayError("NOAA raw capture is absent")
    evaluated = max(item.received_at for item in capture.members) + 60.0
    artifacts = NoaaErrDerivedArtifactStore((output / "derived").resolve())
    evidence = CanonicalEvidenceStore(
        (output / "canonical.sqlite3").resolve(),
        raw_artifact_reader=raw,
        clock=lambda: evaluated + 1.0,
    )
    identity = _sha_mapping({
        "capture": capture_id,
        "config": DECODER_CONFIG_SHA256,
        "region": [48.0, 1.0, 53.0, 8.0],
    })
    kwargs = dict(
        capture_id=capture_id,
        decoder_job_id="decoder-job-" + identity,
        attempt_id="decoder-attempt-" + identity,
        region=NoaaErrRegion(48.0, 1.0, 53.0, 8.0),
        raw_store=raw,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=evaluated,
    )
    first = decode_noaa_err_region(**kwargs)
    second = decode_noaa_err_region(**kwargs)
    if first.outcome != "decoded_complete" or first.artifact is None:
        raise MultiProviderReplayError("NOAA real golden did not decode completely")
    decoded = artifacts.read_decoded(first.artifact)
    shape = tuple(int(item) for item in decoded.precipitation_rate_mm_h.shape)
    total = shape[0] * shape[1]
    valid = int(decoded.rate_valid_mask.sum())
    missing = int(decoded.rate_missing_mask.sum())
    if valid + missing != total:
        raise MultiProviderReplayError("NOAA rate pixels are not exactly accounted")
    after = snapshot_source_tree(source)
    return SingleFrameProbeResult(
        provider="noaa_err",
        evidence_level="exact_raw_capture_redecoded_single_frame",
        outcome=first.outcome,
        reason_code=first.reason_code,
        source_manifest_sha256_before=before.manifest_sha256,
        source_manifest_sha256_after=after.manifest_sha256,
        source_unchanged=before == after,
        source_file_count=len(before.files),
        source_bytes=before.total_bytes,
        frame_count=1,
        grid_shape=shape,
        total_pixels=total,
        accounted_pixels=valid + missing,
        capture_member_count=len(capture.members),
        accounted_members=len(capture.members),
        duplicate_retry_exact=(
            second.evidence_receipt.commit_id == first.evidence_receipt.commit_id
            and second.artifact == first.artifact
        ),
        blockers=(
            "single_frame_cannot_support_motion",
            "decoder_candidate_is_non_operational",
            "no_cross_provider_admission_from_replay",
        ),
    )


def probe_opera_golden(
    raw_root: str | Path,
    work_root: str | Path,
) -> SingleFrameProbeResult:
    """Rerun the exact local OPERA DBZH capture over a bounded Belgium ROI."""

    from .opera_dbzh_quantitative import (
        DECODER_CONFIG_SHA256,
        OperaDbzhDerivedArtifactStore,
        OperaDbzhRawAuthority,
        OperaWgs84Region,
        decode_opera_dbzh_region,
        open_opera_evidence_store,
    )
    from .raw_artifacts import RawArtifactStore

    source = Path(raw_root).resolve(strict=True)
    output = Path(work_root).resolve(strict=False)
    output.mkdir(parents=True, exist_ok=True)
    before = snapshot_source_tree(source)
    probe_source = _copy_source_tree_for_probe(
        source,
        output / "raw-copy",
        before,
    )
    raw = RawArtifactStore(probe_source)
    raw.initialize()
    capture_id = _only_capture_id(probe_source)
    capture = raw.load_capture_receipt(capture_id)
    if capture is None:
        raise MultiProviderReplayError("OPERA raw capture is absent")
    evaluated = max(item.received_at for item in capture.members) + 60.0
    authority = OperaDbzhRawAuthority(raw)
    artifacts = OperaDbzhDerivedArtifactStore((output / "derived").resolve())
    evidence = open_opera_evidence_store(
        (output / "canonical.sqlite3").resolve(),
        raw_authority=authority,
        clock=lambda: evaluated + 1.0,
    )
    identity = _sha_mapping({
        "capture": capture_id,
        "config": DECODER_CONFIG_SHA256,
        "region": [50.90, 4.25, 51.00, 4.40],
    })
    kwargs = dict(
        capture_id=capture_id,
        decoder_job_id="decoder-job-" + identity,
        attempt_id="decoder-attempt-" + identity,
        region=OperaWgs84Region(50.90, 4.25, 51.00, 4.40),
        raw_authority=authority,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=evaluated,
    )
    first = decode_opera_dbzh_region(**kwargs)
    second = decode_opera_dbzh_region(**kwargs)
    accounted: int | None
    if first.outcome == "decoded_complete" and first.artifact is not None:
        evidence_level = "exact_raw_capture_redecoded_single_frame"
        decoded = artifacts.read_decoded(first.artifact, checkpoint=lambda _stage: None)
        shape = tuple(int(item) for item in decoded.dbzh_raw.shape)
        total = shape[0] * shape[1]
        detected = int(decoded.dbzh_detected_mask.sum())
        undetect = int(decoded.dbzh_undetect_mask.sum())
        nodata = int(decoded.dbzh_nodata_mask.sum())
        accounted = detected + undetect + nodata
        if accounted != total:
            raise MultiProviderReplayError("OPERA DBZH pixels are not exactly accounted")
        if decoded.metadata["product"].get("motion_truth_eligible") is not False:
            raise MultiProviderReplayError("OPERA golden unexpectedly claims motion truth")
    elif (
        first.outcome == "unsupported"
        and first.reason_code == "unsupported:raw_provenance"
        and first.artifact is None
    ):
        evidence_level = (
            "exact_raw_capture_decoder_fail_closed_native_grid_audited"
        )
        # The retained live capture predates the current stricter provenance
        # certificate.  The quantitative decoder must therefore reject it.
        # Independently rerun the existing native container/grid inspector so
        # the exact HDF bytes still have bounded structural evidence, without
        # inventing a replacement provenance certificate or pixel semantics.
        from . import opera_native

        bundle = raw.load_capture_bundle_bytes(capture_id)
        if bundle is None or set(bundle[1]) != {"hdf5"}:
            raise MultiProviderReplayError("OPERA fail-closed capture is incomplete")
        member = bundle[1]["hdf5"]
        object_path = (
            probe_source / "objects" / f"{member.content_sha256}.raw"
        ).resolve(strict=True)
        if object_path.parent != (probe_source / "objects").resolve(strict=True):
            raise MultiProviderReplayError("OPERA raw object path escapes store")
        identity_record = opera_native._inspect_opera_hdf5(
            object_path,
            expected_size=member.size_bytes,
            deadline=time.monotonic() + 120.0,
        )
        if identity_record.motion_truth_eligible:
            raise MultiProviderReplayError("OPERA native identity claims motion truth")
        shape = (identity_record.rows, identity_record.columns)
        total = shape[0] * shape[1]
        accounted = None
    else:
        raise MultiProviderReplayError("OPERA real golden failed unexpectedly")
    after = snapshot_source_tree(source)
    return SingleFrameProbeResult(
        provider="opera_dbzh",
        evidence_level=evidence_level,
        outcome=first.outcome,
        reason_code=first.reason_code,
        source_manifest_sha256_before=before.manifest_sha256,
        source_manifest_sha256_after=after.manifest_sha256,
        source_unchanged=before == after,
        source_file_count=len(before.files),
        source_bytes=before.total_bytes,
        frame_count=1,
        grid_shape=shape,
        total_pixels=total,
        accounted_pixels=accounted,
        capture_member_count=len(capture.members),
        accounted_members=len(capture.members),
        duplicate_retry_exact=(
            second.evidence_receipt.commit_id == first.evidence_receipt.commit_id
            and second.artifact == first.artifact
        ),
        blockers=(
            "single_frame_cannot_support_motion",
            "dbzh_is_reflectivity_not_rain_rate",
            "upstream_advected_contributions_are_not_motion_truth",
            "decoder_candidate_is_non_operational",
            "retained_capture_predates_current_raw_provenance_certificate",
            "quantitative_pixel_accounting_withheld_when_decode_is_rejected",
        ),
    )


def probe_knmi_authority_golden(authority_path: str | Path) -> SingleFrameProbeResult:
    """Validate the KNMI grid authority and preserve its absent-raw blocker."""

    path = Path(authority_path).resolve(strict=True)
    before = snapshot_source_tree(path)
    body = _read_regular(path, maximum_bytes=2 * 1024**2)
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MultiProviderReplayError("KNMI authority golden is not JSON") from exc
    if (
        not isinstance(value, dict)
        or value.get("schema") != "stv3-knmi-rtcor-analysis-grid-authority-gate-v1"
        or value.get("status") != "withheld"
        or not isinstance(value.get("evidence_records"), list)
    ):
        raise MultiProviderReplayError("KNMI authority golden schema differs")
    facts = [
        item.get("facts") for item in value["evidence_records"]
        if isinstance(item, dict)
        and item.get("record_id") == "knmi_hdf_spec_v3_8_nl25"
    ]
    live = [
        item.get("facts") for item in value["evidence_records"]
        if isinstance(item, dict)
        and item.get("evidence_class") == "local_unretained_live_observation"
    ]
    if (
        len(facts) != 1
        or len(live) != 1
        or facts[0].get("rows") != 765
        or facts[0].get("columns") != 700
        or live[0].get("official_bytes_retained_in_validation_corpus") is not False
        or not _SHA_RE.fullmatch(str(live[0].get("content_sha256")))
    ):
        raise MultiProviderReplayError("KNMI authority facts differ")
    after = snapshot_source_tree(path)
    return SingleFrameProbeResult(
        provider="knmi_rtcor",
        evidence_level="authority_metadata_only_raw_golden_absent",
        outcome="withheld",
        reason_code="exact_raw_knmi_rtcor_golden_not_retained",
        source_manifest_sha256_before=before.manifest_sha256,
        source_manifest_sha256_after=after.manifest_sha256,
        source_unchanged=before == after,
        source_file_count=1,
        source_bytes=before.total_bytes,
        frame_count=0,
        grid_shape=(765, 700),
        total_pixels=None,
        accounted_pixels=None,
        capture_member_count=None,
        accounted_members=None,
        duplicate_retry_exact=None,
        blockers=(
            "single_frame_motion_claim_forbidden",
            "exact_raw_hdf5_golden_absent",
            "analysis_grid_authority_status_withheld",
            "decoder_replay_not_performed",
        ),
    )


def probe_rainviewer_raw_golden(raw_root: str | Path) -> SingleFrameProbeResult:
    """Validate exact RainViewer manifest/3x3 tile capture without decoding rain."""

    from . import rainviewer_native
    from .raw_artifacts import RawArtifactStore

    source = Path(raw_root).resolve(strict=True)
    before = snapshot_source_tree(source)
    with tempfile.TemporaryDirectory(prefix="stv3-rainviewer-probe-") as temporary:
        probe_source = _copy_source_tree_for_probe(
            source,
            Path(temporary) / "raw-copy",
            before,
        )
        raw = RawArtifactStore(probe_source)
        raw.initialize()
        capture_id = _only_capture_id(probe_source)
        bundle = raw.load_capture_bundle_bytes(capture_id)
    # Exact payload dictionaries outlive the copied store; all source reads are
    # complete before the temporary tree is removed.
    if bundle is None:
        raise MultiProviderReplayError("RainViewer raw capture is absent")
    capture, receipts, payloads = bundle
    roles = tuple(member.role for member in capture.members)
    if roles != rainviewer_native.RAINVIEWER_CANDIDATE_REQUIRED_ROLES:
        raise MultiProviderReplayError("RainViewer capture role set differs")
    if set(receipts) != set(roles) or set(payloads) != set(roles):
        raise MultiProviderReplayError("RainViewer capture accounting differs")
    for role, payload in payloads.items():
        if role == "manifest":
            rainviewer_native._inspect_manifest(payload)
        else:
            rainviewer_native._inspect_png(payload)
    after = snapshot_source_tree(source)
    return SingleFrameProbeResult(
        provider="rainviewer_raw_capture",
        evidence_level="exact_raw_single_frame_capture_structure_only",
        outcome="withheld",
        reason_code="categorical_tiles_not_quantitatively_decoded",
        source_manifest_sha256_before=before.manifest_sha256,
        source_manifest_sha256_after=after.manifest_sha256,
        source_unchanged=before == after,
        source_file_count=len(before.files),
        source_bytes=before.total_bytes,
        frame_count=1,
        grid_shape=None,
        total_pixels=None,
        accounted_pixels=None,
        capture_member_count=len(roles),
        accounted_members=len(roles),
        duplicate_retry_exact=None,
        blockers=(
            "single_frame_cannot_support_motion",
            "categorical_reflectivity_not_physical_rate",
            "tile_decode_not_certified_for_weather_truth",
        ),
    )


def probe_metadata_golden(
    provider: str,
    path: str | Path,
) -> SingleFrameProbeResult:
    """Account a JSON contract fixture without pretending it contains pixels."""

    source = Path(path).resolve(strict=True)
    before = snapshot_source_tree(source)
    body = _read_regular(source, maximum_bytes=4 * 1024**2)
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MultiProviderReplayError("metadata golden is not JSON") from exc
    if not isinstance(value, (dict, list)):
        raise MultiProviderReplayError("metadata golden root is invalid")
    after = snapshot_source_tree(source)
    return SingleFrameProbeResult(
        provider=provider,
        evidence_level="metadata_contract_only_no_raw_raster",
        outcome="withheld",
        reason_code="raw_single_frame_payload_absent",
        source_manifest_sha256_before=before.manifest_sha256,
        source_manifest_sha256_after=after.manifest_sha256,
        source_unchanged=before == after,
        source_file_count=1,
        source_bytes=before.total_bytes,
        frame_count=0,
        grid_shape=None,
        total_pixels=None,
        accounted_pixels=None,
        capture_member_count=None,
        accounted_members=None,
        duplicate_retry_exact=None,
        blockers=(
            "metadata_is_not_a_decoded_raster",
            "single_frame_motion_claim_forbidden",
            "operational_admission_forbidden",
        ),
    )


@dataclass(frozen=True, slots=True)
class MultiProviderReplayReport:
    generated_at_utc: str
    sequence_results: tuple[SequenceReplayResult, ...]
    single_frame_results: tuple[SingleFrameProbeResult, ...]
    report_sha256: str
    contract: str = MULTI_PROVIDER_REPLAY_MATRIX_CONTRACT
    meteorological_admission_granted: bool = False
    operational_snapshot_claim_generated: bool = False

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "generated_at_utc": self.generated_at_utc,
            "meteorological_admission_granted": False,
            "operational_snapshot_claim_generated": False,
            "sequence_results": [item.to_mapping() for item in self.sequence_results],
            "single_frame_results": [item.to_mapping() for item in self.single_frame_results],
        }

    def to_mapping(self) -> dict[str, Any]:
        value = self.payload_mapping()
        value["report_sha256"] = self.report_sha256
        return value


def build_report(
    sequence_results: Iterable[SequenceReplayResult],
    single_frame_results: Iterable[SingleFrameProbeResult],
) -> MultiProviderReplayReport:
    sequences = tuple(sequence_results)
    singles = tuple(single_frame_results)
    if (
        not sequences
        or not singles
        or any(
            item.meteorological_admission_granted
            or item.motion_claim_generated
            or item.operational_snapshot_claim_generated
            or not item.source_unchanged
            or item.processed_frames != item.frame_count
            or item.advanced_frames != item.frame_count
            or item.candidate_edges != item.expected_candidate_edges
            for item in sequences
        )
        or any(
            item.meteorological_admission_granted
            or item.motion_claim_generated
            or item.operational_snapshot_claim_generated
            or not item.source_unchanged
            for item in singles
        )
    ):
        raise MultiProviderReplayError("report contains an unclosed proof boundary")
    generated = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    temporary = object.__new__(MultiProviderReplayReport)
    object.__setattr__(temporary, "generated_at_utc", generated)
    object.__setattr__(temporary, "sequence_results", sequences)
    object.__setattr__(temporary, "single_frame_results", singles)
    object.__setattr__(temporary, "report_sha256", "0" * 64)
    object.__setattr__(temporary, "contract", MULTI_PROVIDER_REPLAY_MATRIX_CONTRACT)
    object.__setattr__(temporary, "meteorological_admission_granted", False)
    object.__setattr__(temporary, "operational_snapshot_claim_generated", False)
    digest = _sha_mapping(temporary.payload_mapping())
    return MultiProviderReplayReport(generated, sequences, singles, digest)


def write_report(report: MultiProviderReplayReport, path: str | Path) -> Path:
    if not isinstance(report, MultiProviderReplayReport):
        raise TypeError("report must be MultiProviderReplayReport")
    if report.report_sha256 != _sha_mapping(report.payload_mapping()):
        raise MultiProviderReplayError("report checksum differs")
    target = Path(path).resolve(strict=False)
    target.parent.mkdir(parents=True, exist_ok=True)
    body = _canonical_json_bytes(report.to_mapping())
    descriptor, temporary_text = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent,
    )
    temporary = Path(temporary_text)
    try:
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, target)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
    return target


__all__ = [
    "CategoricalReplayComposer",
    "CategoricalReplaySequence",
    "CategoricalSourceFrame",
    "MultiProviderReplayError",
    "MultiProviderReplayReport",
    "SequenceReplayResult",
    "SingleFrameProbeResult",
    "SourceFileDigest",
    "SourceTreeSnapshot",
    "build_report",
    "load_houston_rainviewer_sequences",
    "load_kmi_public_composite_sequence",
    "matrix_tracking_policy",
    "probe_knmi_authority_golden",
    "probe_metadata_golden",
    "probe_noaa_err_golden",
    "probe_opera_golden",
    "probe_rainviewer_raw_golden",
    "run_categorical_sequence",
    "snapshot_source_tree",
    "write_report",
]
