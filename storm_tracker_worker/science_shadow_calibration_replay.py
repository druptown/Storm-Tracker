"""Read-only normalized-cache replay factory for the isolated science shadow.

This module is intentionally offline and unwired.  It exports one validated
``pysteps_frame_cache`` sequence from an immutable/query-only calibration
database into bounded canonical JSON, then builds the existing durable science
orchestrator from that content-addressed export.  The cache is normalized Storm
Tracker data, not raw provider data, and its intensity classes are not physical
precipitation units.
"""
from __future__ import annotations

from dataclasses import dataclass
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
from typing import Any, Mapping

from .analysis_grid_transform import analysis_grid_sha256
from .calibration_replay import (
    NORMALIZED_CACHE_KIND,
    NormalizedCacheCorpus,
    NormalizedCacheFrame,
    NormalizedCacheSequence,
    _validated_normalized_cache_frame,
    build_normalized_cache_inventory,
    build_normalized_raster_replay_view,
)
from .evidence_to_cells_shadow import (
    EvidenceToCellsPixelAccounting,
    EvidenceToCellsShadowDeadlineError,
)
from .hard_decoder_isolation import _canonical_json_bytes
from .science_shadow_orchestrator import (
    DurableScienceShadowOrchestrator,
    ScienceShadowLimits,
    ShadowCompositionResult,
    ShadowEvidenceFrame,
)
from .science_shadow_process_isolation import (
    HARD_MAX_AUTHORITY_BYTES,
    SCIENCE_SHADOW_PROCESS_AUTHORITY_CONTRACT,
    ScienceShadowFactoryContract,
    ScienceShadowIsolationLimits,
    _read_regular_file,
)
from .tracking_core.engine.cell_track_engine import (
    CellTrackEnginePolicy,
    CellTrackEngineState,
)
from .tracking_core.engine.cell_track_lifecycle import CellTrackLifecyclePolicy
from .tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
    NormalizedRasterFrame,
    detect_hierarchical_cells,
)
from .tracking_core.engine.lucas_kanade_flow import (
    DetectionFlowAggregationConfig,
    LucasKanadeConfig,
)
from .tracking_core.engine.multi_hypothesis_identity import (
    MultiHypothesisIdentityPolicy,
    MultiScanIdentityPolicy,
)
from .tracking_core.engine.recursive_track_estimator import (
    RecursiveTrackEstimatorPolicy,
)
from .tracking_core.engine.weather_system_grouping import (
    WeatherSystemGroupingPolicy,
)


CALIBRATION_REPLAY_EXPORT_CONTRACT = (
    "stv3-normalized-cache-science-export-v1"
)
CALIBRATION_REPLAY_FRAME_CONTRACT = (
    "stv3-normalized-cache-science-export-frame-v1"
)
CALIBRATION_REPLAY_FACTORY_ID = "science-calibration-replay-factory-v3"
CALIBRATION_REPLAY_FACTORY_TARGET = (
    "storm_tracker_worker.science_shadow_calibration_replay:"
    "build_calibration_replay_orchestrator"
)
CALIBRATION_REPLAY_CONNECTION_MODE = (
    "sqlite-uri-mode-ro-immutable-query-only"
)
MAX_CALIBRATION_EXPORT_BYTES = HARD_MAX_AUTHORITY_BYTES
MAX_CALIBRATION_EXPORT_FRAMES = 64
MAX_CALIBRATION_DATABASE_BYTES = 4 * 1024**3

CALIBRATION_REPLAY_CAVEATS = (
    "normalized_cache_is_not_raw_kmi_data",
    "normalized_intensity_classes_are_not_physical_precipitation_units",
    "provider_nodata_mask_is_unavailable",
    "independent_cell_identity_truth_is_unavailable",
    "independent_speed_truth_is_unavailable",
    "dense_lucas_kanade_motion_is_forced_unavailable_for_normalized_classes",
    "top_k_weights_are_conditional_not_calibrated_identity_probabilities",
    "multi_scan_branch_weights_are_conditional_not_calibrated_probabilities",
    "n_scan_map_commit_is_not_external_identity_truth",
    "system_translation_uses_normalized_scene_centroids_not_external_wind_truth",
    "bounded_max_pool_analysis_view_is_not_native_resolution",
    "local_metric_projection_is_sequence_specific",
    "results_are_exploratory_and_never_meteorological_admission",
)
CALIBRATION_REPLAY_BLOCKERS = (
    # Required by the evidence algebra: composition remains unwired even when
    # an explicit parent runner later supplies the hard process boundary.
    "hard_process_isolation_not_wired",
    "normalized_cache_not_raw_provider_data",
    "normalized_class_units_not_physical",
    "provider_nodata_mask_unavailable",
    "independent_identity_truth_unavailable",
    "independent_speed_truth_unavailable",
    "independent_holdout_calibration_unavailable",
    "soft_costs_selected_on_same_normalized_cache_are_exploratory_overfit",
    "optical_flow_motion_evidence_unavailable",
    "multi_hypothesis_identity_and_system_translation_are_not_externally_validated",
    "multi_scan_posterior_not_externally_validated",
    "multi_scan_lineage_alternatives_unselected_without_truth",
    "multi_scan_posterior_does_not_retroactively_rewrite_primary_lifecycle",
    "operational_admission_certificates_incomplete",
    "operational_snapshot_publication_forbidden",
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID_RE = re.compile(r"^run-[0-9a-f]{64}$")
_ZERO4 = (
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
)
_INITIAL4 = (
    (1_000_000.0, 0.0, 0.0, 0.0),
    (0.0, 1_000_000.0, 0.0, 0.0),
    (0.0, 0.0, 400.0, 0.0),
    (0.0, 0.0, 0.0, 400.0),
)


class CalibrationScienceReplayError(RuntimeError):
    """The offline export/factory contract is absent, corrupt or stale."""


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_mapping(value: Any) -> str:
    return _sha_bytes(_canonical_json_bytes(value))


def _require_sha(value: object, label: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise CalibrationScienceReplayError(f"{label} is not a SHA-256")
    return value


def _stream_regular_file_sha256(
    path: Path,
    *,
    maximum_bytes: int,
) -> tuple[str, int, tuple[int, int, int, int]]:
    if not path.is_absolute() or path.is_symlink():
        raise CalibrationScienceReplayError("source path is not an absolute file")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise CalibrationScienceReplayError("source file cannot be opened") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or not 1 <= before.st_size <= maximum_bytes
        ):
            raise CalibrationScienceReplayError("source file size/type is invalid")
        digest = hashlib.sha256()
        total = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > maximum_bytes:
                raise CalibrationScienceReplayError("source file grew past bound")
            digest.update(chunk)
        after = os.fstat(descriptor)
        identity = (
            int(before.st_dev),
            int(before.st_ino),
            int(before.st_size),
            int(before.st_mtime_ns),
        )
        if identity != (
            int(after.st_dev),
            int(after.st_ino),
            int(after.st_size),
            int(after.st_mtime_ns),
        ) or total != before.st_size:
            raise CalibrationScienceReplayError("source file changed while hashing")
        return digest.hexdigest(), total, identity
    finally:
        os.close(descriptor)


def _atomic_write_new_or_identical(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.is_symlink() or not path.is_file() or path.read_bytes() != body:
            raise CalibrationScienceReplayError(
                "output already exists with different or unsafe content"
            )
        return
    descriptor, temporary_text = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_text)
    try:
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        if path.exists():
            raise CalibrationScienceReplayError("output appeared concurrently")
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary.exists():
            temporary.unlink()


def _frame_payload(frame: NormalizedCacheFrame) -> dict[str, Any]:
    return {
        "bounds": list(frame.bounds),
        "cells": [list(item) for item in frame.cells],
        "grid_deg": frame.grid_deg,
        "height": frame.height,
        "input_runs": frame.input_runs,
        "nominal_timestamp": frame.nominal_timestamp,
        "region_id": frame.region_id,
        "saved_at": frame.saved_at,
        "source": frame.source,
        "width": frame.width,
    }


@dataclass(frozen=True, slots=True)
class CalibrationReplayFrame:
    run_id: str
    frame_sha256: str
    stored_frame_json_sha256: str
    frame: NormalizedCacheFrame

    def __post_init__(self) -> None:
        if _RUN_ID_RE.fullmatch(self.run_id) is None:
            raise ValueError("calibration replay run ID is invalid")
        _require_sha(self.frame_sha256, "frame SHA")
        _require_sha(self.stored_frame_json_sha256, "stored frame JSON SHA")
        if self.frame_sha256 != _sha_mapping(_frame_payload(self.frame)):
            raise ValueError("calibration replay frame checksum differs")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "contract": CALIBRATION_REPLAY_FRAME_CONTRACT,
            "frame": _frame_payload(self.frame),
            "frame_sha256": self.frame_sha256,
            "run_id": self.run_id,
            "stored_frame_json_sha256": self.stored_frame_json_sha256,
        }


@dataclass(frozen=True, slots=True)
class CalibrationReplayExport:
    export_sha256: str
    source_database_sha256: str
    source_database_bytes: int
    sequence_id: str
    region_id: str
    source: str
    cadence_seconds: int
    frames: tuple[CalibrationReplayFrame, ...]
    connection_mode: str = CALIBRATION_REPLAY_CONNECTION_MODE
    data_kind: str = NORMALIZED_CACHE_KIND
    source_table: str = "pysteps_frame_cache"
    caveats: tuple[str, ...] = CALIBRATION_REPLAY_CAVEATS
    contract: str = CALIBRATION_REPLAY_EXPORT_CONTRACT

    def __post_init__(self) -> None:
        _require_sha(self.export_sha256, "export SHA")
        _require_sha(self.source_database_sha256, "source database SHA")
        if (
            isinstance(self.source_database_bytes, bool)
            or not isinstance(self.source_database_bytes, int)
            or self.source_database_bytes <= 0
            or not isinstance(self.cadence_seconds, int)
            or self.cadence_seconds <= 0
            or not 1 <= len(self.frames) <= MAX_CALIBRATION_EXPORT_FRAMES
            or self.connection_mode != CALIBRATION_REPLAY_CONNECTION_MODE
            or self.data_kind != NORMALIZED_CACHE_KIND
            or self.source_table != "pysteps_frame_cache"
            or self.caveats != CALIBRATION_REPLAY_CAVEATS
            or self.contract != CALIBRATION_REPLAY_EXPORT_CONTRACT
        ):
            raise ValueError("calibration replay export contract differs")
        timestamps = tuple(item.frame.nominal_timestamp for item in self.frames)
        if timestamps != tuple(sorted(set(timestamps))) or any(
            later - earlier != self.cadence_seconds
            for earlier, later in zip(timestamps, timestamps[1:])
        ):
            raise ValueError("calibration replay timestamps are not contiguous")
        if any(
            item.frame.region_id != self.region_id
            or item.frame.source != self.source
            or item.frame.geometry_key != self.frames[0].frame.geometry_key
            for item in self.frames
        ):
            raise ValueError("calibration replay frame group/geometry differs")
        if len({item.run_id for item in self.frames}) != len(self.frames):
            raise ValueError("calibration replay run IDs are duplicated")

    @property
    def run_ids(self) -> tuple[str, ...]:
        return tuple(item.run_id for item in self.frames)

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "cadence_seconds": self.cadence_seconds,
            "caveats": list(self.caveats),
            "connection_mode": self.connection_mode,
            "contract": self.contract,
            "data_kind": self.data_kind,
            "frames": [item.to_mapping() for item in self.frames],
            "region_id": self.region_id,
            "sequence_id": self.sequence_id,
            "source": self.source,
            "source_database_bytes": self.source_database_bytes,
            "source_database_sha256": self.source_database_sha256,
            "source_table": self.source_table,
        }


@dataclass(frozen=True, slots=True)
class CalibrationReplayExportReceipt:
    export_path: Path
    export_sha256: str
    export_bytes: int
    source_database_sha256: str
    source_database_bytes: int
    sequence_id: str
    run_ids: tuple[str, ...]
    source_database_unchanged: bool
    connection_mode: str = CALIBRATION_REPLAY_CONNECTION_MODE
    meteorological_admission_granted: bool = False


def export_normalized_cache_sequence(
    database_path: str | Path,
    export_path: str | Path,
    *,
    region_id: str,
    source: str,
    expected_database_sha256: str | None = None,
    expected_frame_count: int | None = None,
    minimum_sequence_frames: int = 20,
) -> CalibrationReplayExportReceipt:
    """Export one exact contiguous normalized-cache sequence without DB writes."""

    source_path = Path(database_path).resolve(strict=True)
    target = Path(export_path).resolve(strict=False)
    if source_path == target or source_path.is_symlink() or not source_path.is_file():
        raise CalibrationScienceReplayError("source/output path contract is invalid")
    before_sha, before_bytes, before_identity = _stream_regular_file_sha256(
        source_path,
        maximum_bytes=MAX_CALIBRATION_DATABASE_BYTES,
    )
    if expected_database_sha256 is not None and before_sha != _require_sha(
        expected_database_sha256,
        "expected database SHA",
    ):
        raise CalibrationScienceReplayError("calibration database SHA differs")

    uri = f"{source_path.as_uri()}?mode=ro&immutable=1"
    try:
        database = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as exc:
        raise CalibrationScienceReplayError(
            "calibration database cannot be opened immutable/read-only"
        ) from exc
    raw_by_timestamp: dict[int, bytes] = {}
    frames: list[NormalizedCacheFrame] = []
    try:
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA query_only=ON")
        rows = database.execute(
            "SELECT region_id, source, nominal_timestamp, saved_at, frame_json "
            "FROM pysteps_frame_cache WHERE region_id=? AND source=? "
            "ORDER BY nominal_timestamp",
            (region_id, source),
        ).fetchall()
        for row in rows:
            timestamp = int(row["nominal_timestamp"])
            raw = str(row["frame_json"]).encode("utf-8")
            if timestamp in raw_by_timestamp:
                raise CalibrationScienceReplayError("duplicate cache timestamp")
            raw_by_timestamp[timestamp] = raw
            try:
                value = json.loads(raw.decode("utf-8"))
                frames.append(_validated_normalized_cache_frame(
                    str(row["region_id"]),
                    str(row["source"]),
                    timestamp,
                    float(row["saved_at"]),
                    value,
                ))
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                raise CalibrationScienceReplayError(
                    "normalized cache row is invalid"
                ) from exc
    except sqlite3.Error as exc:
        raise CalibrationScienceReplayError("read-only cache query failed") from exc
    finally:
        database.close()
    if expected_frame_count is not None and len(frames) != expected_frame_count:
        raise CalibrationScienceReplayError("normalized cache frame count differs")
    if not frames:
        raise CalibrationScienceReplayError("normalized cache group is empty")
    corpus = NormalizedCacheCorpus(
        database_path="content-addressed-source:" + before_sha,
        connection_mode=CALIBRATION_REPLAY_CONNECTION_MODE,
        frames=tuple(frames),
        issues=(),
    )
    inventories = build_normalized_cache_inventory(
        corpus,
        minimum_sequence_frames=minimum_sequence_frames,
    )
    qualifying = tuple(
        sequence
        for inventory in inventories
        if inventory.region_id == region_id and inventory.source == source
        for sequence in inventory.qualifying_sequences
    )
    if len(qualifying) != 1 or len(qualifying[0].frames) != len(frames):
        raise CalibrationScienceReplayError(
            "cache group is not one qualifying contiguous sequence"
        )
    sequence = qualifying[0]
    if sequence.cadence_seconds is None:
        raise CalibrationScienceReplayError("cache cadence is indeterminate")

    replay_frames: list[CalibrationReplayFrame] = []
    for frame in sequence.frames:
        payload = _frame_payload(frame)
        frame_sha = _sha_mapping(payload)
        run_id = "run-" + _sha_mapping({
            "contract": CALIBRATION_REPLAY_FRAME_CONTRACT,
            "frame_sha256": frame_sha,
            "sequence_id": sequence.sequence_id,
            "source_database_sha256": before_sha,
        })
        replay_frames.append(CalibrationReplayFrame(
            run_id=run_id,
            frame_sha256=frame_sha,
            stored_frame_json_sha256=_sha_bytes(
                raw_by_timestamp[frame.nominal_timestamp]
            ),
            frame=frame,
        ))
    temporary = object.__new__(CalibrationReplayExport)
    values = {
        "source_database_sha256": before_sha,
        "source_database_bytes": before_bytes,
        "sequence_id": sequence.sequence_id,
        "region_id": region_id,
        "source": source,
        "cadence_seconds": sequence.cadence_seconds,
        "frames": tuple(replay_frames),
    }
    for key, value in values.items():
        object.__setattr__(temporary, key, value)
    for key, value in (
        ("export_sha256", "0" * 64),
        ("connection_mode", CALIBRATION_REPLAY_CONNECTION_MODE),
        ("data_kind", NORMALIZED_CACHE_KIND),
        ("source_table", "pysteps_frame_cache"),
        ("caveats", CALIBRATION_REPLAY_CAVEATS),
        ("contract", CALIBRATION_REPLAY_EXPORT_CONTRACT),
    ):
        object.__setattr__(temporary, key, value)
    payload = temporary.payload_mapping()
    export_sha = _sha_mapping(payload)
    export = CalibrationReplayExport(export_sha256=export_sha, **values)
    document = export.payload_mapping()
    document["export_sha256"] = export.export_sha256
    body = _canonical_json_bytes(document)
    if len(body) > MAX_CALIBRATION_EXPORT_BYTES:
        raise CalibrationScienceReplayError("canonical cache export exceeds bound")
    _atomic_write_new_or_identical(target, body)

    after_sha, after_bytes, after_identity = _stream_regular_file_sha256(
        source_path,
        maximum_bytes=MAX_CALIBRATION_DATABASE_BYTES,
    )
    unchanged = (
        before_sha == after_sha
        and before_bytes == after_bytes
        and before_identity == after_identity
    )
    if not unchanged:
        raise CalibrationScienceReplayError("calibration database changed during export")
    return CalibrationReplayExportReceipt(
        export_path=target,
        export_sha256=_sha_bytes(body),
        export_bytes=len(body),
        source_database_sha256=before_sha,
        source_database_bytes=before_bytes,
        sequence_id=sequence.sequence_id,
        run_ids=export.run_ids,
        source_database_unchanged=True,
    )


def load_calibration_replay_export(
    export_path: str | Path,
    *,
    expected_file_sha256: str,
) -> CalibrationReplayExport:
    path = Path(export_path)
    body = _read_regular_file(path, maximum_bytes=MAX_CALIBRATION_EXPORT_BYTES)
    if _sha_bytes(body) != _require_sha(expected_file_sha256, "export file SHA"):
        raise CalibrationScienceReplayError("calibration export file SHA differs")
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise CalibrationScienceReplayError("calibration export is not JSON") from exc
    if not isinstance(value, dict) or _canonical_json_bytes(value) != body:
        raise CalibrationScienceReplayError("calibration export is not canonical")
    expected_fields = {
        "cadence_seconds",
        "caveats",
        "connection_mode",
        "contract",
        "data_kind",
        "export_sha256",
        "frames",
        "region_id",
        "sequence_id",
        "source",
        "source_database_bytes",
        "source_database_sha256",
        "source_table",
    }
    if set(value) != expected_fields:
        raise CalibrationScienceReplayError("calibration export fields differ")
    raw_frames = value["frames"]
    if not isinstance(raw_frames, list) or not 1 <= len(raw_frames) <= MAX_CALIBRATION_EXPORT_FRAMES:
        raise CalibrationScienceReplayError("calibration export frame count is invalid")
    frames: list[CalibrationReplayFrame] = []
    for item in raw_frames:
        if not isinstance(item, dict) or set(item) != {
            "contract",
            "frame",
            "frame_sha256",
            "run_id",
            "stored_frame_json_sha256",
        } or item.get("contract") != CALIBRATION_REPLAY_FRAME_CONTRACT:
            raise CalibrationScienceReplayError("calibration export frame schema differs")
        payload = item["frame"]
        if not isinstance(payload, dict) or set(payload) != {
            "bounds",
            "cells",
            "grid_deg",
            "height",
            "input_runs",
            "nominal_timestamp",
            "region_id",
            "saved_at",
            "source",
            "width",
        }:
            raise CalibrationScienceReplayError("calibration frame payload differs")
        try:
            frame = _validated_normalized_cache_frame(
                str(payload["region_id"]),
                str(payload["source"]),
                int(payload["nominal_timestamp"]),
                float(payload["saved_at"]),
                payload,
            )
            replay_frame = CalibrationReplayFrame(
                run_id=item["run_id"],
                frame_sha256=item["frame_sha256"],
                stored_frame_json_sha256=item["stored_frame_json_sha256"],
                frame=frame,
            )
        except (TypeError, ValueError, CalibrationScienceReplayError) as exc:
            raise CalibrationScienceReplayError("calibration frame is invalid") from exc
        expected_run = "run-" + _sha_mapping({
            "contract": CALIBRATION_REPLAY_FRAME_CONTRACT,
            "frame_sha256": replay_frame.frame_sha256,
            "sequence_id": value["sequence_id"],
            "source_database_sha256": value["source_database_sha256"],
        })
        if replay_frame.run_id != expected_run:
            raise CalibrationScienceReplayError("calibration run identity differs")
        frames.append(replay_frame)
    payload_without_export_sha = dict(value)
    declared_export_sha = payload_without_export_sha.pop("export_sha256")
    if _require_sha(declared_export_sha, "declared export SHA") != _sha_mapping(
        payload_without_export_sha
    ):
        raise CalibrationScienceReplayError("calibration export checksum differs")
    try:
        return CalibrationReplayExport(
            export_sha256=declared_export_sha,
            source_database_sha256=value["source_database_sha256"],
            source_database_bytes=value["source_database_bytes"],
            sequence_id=value["sequence_id"],
            region_id=value["region_id"],
            source=value["source"],
            cadence_seconds=value["cadence_seconds"],
            frames=tuple(frames),
            connection_mode=value["connection_mode"],
            data_kind=value["data_kind"],
            source_table=value["source_table"],
            caveats=tuple(value["caveats"]),
            contract=value["contract"],
        )
    except (TypeError, ValueError, CalibrationScienceReplayError) as exc:
        raise CalibrationScienceReplayError("calibration export contract is invalid") from exc


def calibration_replay_detector_config() -> HierarchicalDetectorConfig:
    """The explicit detector used by the earlier 36-frame replay reports."""

    return HierarchicalDetectorConfig(
        shield_threshold=1.0,
        core_threshold=5.0,
        minimum_shield_area_m2=10.0 * 1_000_000.0,
        minimum_core_area_m2=2.0 * 1_000_000.0,
        shield_position_sigma_m=1_000.0,
        core_position_sigma_m=750.0,
        connectivity=8,
        centroid_method="value_weighted",
    )


def _detector_mapping(value: HierarchicalDetectorConfig) -> dict[str, Any]:
    return {
        "centroid_method": value.centroid_method,
        "connectivity": value.connectivity,
        "core_position_sigma_m": value.core_position_sigma_m,
        "core_threshold": value.core_threshold,
        "minimum_core_area_m2": value.minimum_core_area_m2,
        "minimum_shield_area_m2": value.minimum_shield_area_m2,
        "shield_position_sigma_m": value.shield_position_sigma_m,
        "shield_threshold": value.shield_threshold,
    }


def calibration_replay_tracking_policy(
    detector: HierarchicalDetectorConfig,
    *,
    maximum_analysis_pixels: int,
) -> CellTrackEnginePolicy:
    """Explicit ungated exploratory policy; ranking values are not calibrated."""

    return CellTrackEnginePolicy(
        policy_id="calibration-normalized-cache-exploratory-tracking-v5",
        detector=detector,
        lifecycle=CellTrackLifecyclePolicy(
            policy_id="calibration-normalized-cache-lifecycle-v2",
            confirmation_observations=2,
            maximum_gap_frames=2,
        ),
        estimator=RecursiveTrackEstimatorPolicy(
            policy_id="calibration-normalized-cache-estimator-v2",
            acceleration_spectral_density_m2_s3=1.0,
        ),
        lucas_kanade=LucasKanadeConfig(
            window_radius_pixels=1,
            # A radius-one window contains at most nine samples.  Requiring
            # ten makes dense LK explicitly unavailable instead of deriving a
            # motion claim from normalized intensity classes that have no
            # admitted physical semantics.  Mature tracks still use their
            # last two associated positions; cold starts retain the existing
            # broad zero-centred uncertainty prior.
            minimum_valid_samples=10,
            minimum_structure_eigenvalue=1e-12,
            maximum_condition_number=1e12,
            regularization=0.0,
            residual_variance_floor=1e-6,
            velocity_process_noise_covariance_m2_s2=((1.0, 0.0), (0.0, 1.0)),
        ),
        flow_aggregation=DetectionFlowAggregationConfig(
            minimum_valid_pixels=1,
            minimum_valid_fraction=0.1,
            velocity_covariance_floor_m2_s2=((1.0, 0.0), (0.0, 1.0)),
            residual_position_covariance_m2=(
                (1_000_000.0, 0.0),
                (0.0, 1_000_000.0),
            ),
            method="conservative_mean",
        ),
        weather_system=WeatherSystemGroupingPolicy(
            policy_id="calibration-normalized-cache-systems-v2",
            pixel_connectivity=8,
            maximum_footprint_gap_m=20_000.0,
            maximum_motion_difference_m_s=1_000_000.0,
            maximum_motion_age_s=10_000.0,
            maximum_motion_time_difference_s=10_000.0,
        ),
        initial_state_covariance=_INITIAL4,
        prior_process_noise_covariance=_ZERO4,
        association_motion_sigma_m_s=100.0,
        motion_nll_weight=0.0,
        object_kind_change_cost=50.0,
        absolute_log_area_weight=1.0,
        mean_value_difference_weight=0.0,
        maximum_value_difference_weight=0.0,
        footprint_nonoverlap_weight=1.0,
        birth_cost=15.0,
        death_cost=15.0,
        exact_solution_limit=4,
        maximum_exact_current_detections=8,
        maximum_frame_pixels=maximum_analysis_pixels,
        maximum_detected_objects=512,
        maximum_active_tracks=512,
        maximum_candidate_edges=500_000,
        maximum_total_tracks=10_000,
        maximum_lifecycle_events=50_000,
        maximum_track_history_detections=128,
        multi_hypothesis_identity=MultiHypothesisIdentityPolicy(
            policy_id="calibration-normalized-cache-multi-hypothesis-v2",
            solution_limit=4,
            maximum_assignment_subproblems=4_096,
            maximum_retained_local_candidates=500_000,
            maximum_derived_lineage_hypotheses=128,
            maximum_lineage_members=64,
            minimum_system_translation_support_tracks=512,
            conditional_cost_temperature=2.0,
            system_translation_sigma_floor_m=5_000.0,
            system_translation_sigma_ceiling_m=50_000.0,
            local_deformation_sigma_floor_m=10_000.0,
            local_deformation_sigma_ceiling_m=20_000.0,
            previous_local_deformation_weight=0.5,
            local_deformation_cost_weight=1.0,
            multi_scan=MultiScanIdentityPolicy(
                policy_id="calibration-normalized-cache-multi-scan-v1",
                horizon_frames=4,
                beam_width=8,
                maximum_generated_branches=64,
                maximum_assignment_subproblems_total=16_384,
                maximum_candidate_edges_total=500_000,
                maximum_active_paths_per_branch=512,
                maximum_buffered_detections=10_000,
                maximum_lineage_alternatives=4_096,
                conditional_cost_temperature=2.0,
            ),
        ),
        scalable_one_best_fallback=False,
    )


class CalibrationReplayComposer:
    """Read-only adapter from one checked export to science evidence frames."""

    def __init__(
        self,
        export: CalibrationReplayExport,
        *,
        export_file_sha256: str,
        maximum_source_grid_cells: int,
        maximum_analysis_pixels: int,
        maximum_runtime_seconds: float,
        detector_config: HierarchicalDetectorConfig,
    ) -> None:
        if not isinstance(export, CalibrationReplayExport):
            raise TypeError("export must be CalibrationReplayExport")
        for label, value in (
            ("maximum_source_grid_cells", maximum_source_grid_cells),
            ("maximum_analysis_pixels", maximum_analysis_pixels),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{label} must be positive")
        if (
            isinstance(maximum_runtime_seconds, bool)
            or not isinstance(maximum_runtime_seconds, (int, float))
            or not math.isfinite(float(maximum_runtime_seconds))
            or not 0.05 <= float(maximum_runtime_seconds) <= 300.0
        ):
            raise ValueError("maximum runtime is invalid")
        _require_sha(export_file_sha256, "export file SHA")
        sequence = NormalizedCacheSequence(
            sequence_id=export.sequence_id,
            region_id=export.region_id,
            source=export.source,
            cadence_seconds=export.cadence_seconds,
            frames=tuple(item.frame for item in export.frames),
            minimum_frame_count=min(20, len(export.frames)),
        )
        view = build_normalized_raster_replay_view(
            sequence,
            maximum_source_grid_cells=maximum_source_grid_cells,
            maximum_analysis_pixels=maximum_analysis_pixels,
        )
        self._export = export
        self._export_file_sha256 = export_file_sha256
        self._view = view
        self._detector = detector_config
        self._maximum_runtime_seconds = float(maximum_runtime_seconds)
        self._frames = dict(zip(export.run_ids, view.frames, strict=True))
        self._export_frames = {item.run_id: item for item in export.frames}
        self._detector_sha256 = _sha_mapping(_detector_mapping(detector_config))
        self._profile_sha256 = _sha_mapping({
            "algorithm": "stv3-calibration-science-composer-v1",
            "caveats": list(CALIBRATION_REPLAY_CAVEATS),
            "detector": _detector_mapping(detector_config),
            "export_file_sha256": export_file_sha256,
            "export_sha256": export.export_sha256,
            "maximum_analysis_pixels": maximum_analysis_pixels,
            "maximum_source_grid_cells": maximum_source_grid_cells,
            "sequence_id": export.sequence_id,
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
    def transform_fingerprint(self) -> str:
        return self._view.transform.fingerprint

    def compose(
        self,
        *,
        run_id: str,
        deadline_at: float,
        monotonic,
    ) -> ShadowCompositionResult:
        if float(monotonic()) >= deadline_at:
            raise EvidenceToCellsShadowDeadlineError(
                "calibration science composition deadline expired"
            )
        frame = self._frames.get(run_id)
        exported = self._export_frames.get(run_id)
        if frame is None or exported is None:
            raise CalibrationScienceReplayError("run is absent from calibration export")
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
        evidence_sha = _sha_mapping({
            "accounting": accounting.to_mapping(),
            "detector_sha256": self._detector_sha256,
            "export_file_sha256": self._export_file_sha256,
            "export_frame_sha256": exported.frame_sha256,
            "frame_id": frame.frame_id,
            "frame_values_sha256": values_sha,
            "run_id": run_id,
            "transform_fingerprint": self._view.transform.fingerprint,
        })
        transform_receipt_sha = _sha_mapping({
            "export_frame_sha256": exported.frame_sha256,
            "output_frame_id": frame.frame_id,
            "transform_fingerprint": self._view.transform.fingerprint,
            "values_sha256": values_sha,
        })
        semantics_sha = _sha_mapping({
            "caveats": list(CALIBRATION_REPLAY_CAVEATS),
            "quantity": frame.quantity,
            "unit": frame.unit,
        })
        analysis_sha = _sha_mapping({
            "frame_id": frame.frame_id,
            "grid_sha256": analysis_grid_sha256(frame.grid),
            "values_sha256": values_sha,
        })
        evidence = ShadowEvidenceFrame(
            canonical_run_id=run_id,
            canonical_commit_id="commit-" + _sha_mapping({
                "export": self._export_file_sha256,
                "run": run_id,
            }),
            canonical_receipt_sha256=_sha_mapping({
                "export_frame": exported.frame_sha256,
                "stored_frame": exported.stored_frame_json_sha256,
            }),
            canonical_result_sha256=evidence_sha,
            capture_id="capture-normalized-cache-" + exported.frame_sha256,
            capture_receipt_sha256=_sha_mapping({
                "data_kind": NORMALIZED_CACHE_KIND,
                "frame": exported.frame_sha256,
            }),
            capture_manifest_sha256=self._export_file_sha256,
            evidence_candidate_id="evidence-cells-" + evidence_sha,
            evidence_content_sha256=evidence_sha,
            native_candidate_id="radar-field-normalized-cache-" + exported.frame_sha256,
            native_candidate_content_sha256=exported.frame_sha256,
            native_lineage_sha256=_sha_mapping({
                "export": self._export.export_sha256,
                "frame": exported.frame_sha256,
            }),
            analysis_field_id="analysis-field-" + analysis_sha,
            analysis_field_content_sha256=analysis_sha,
            analysis_lineage_sha256=_sha_mapping({
                "native": exported.frame_sha256,
                "transform": transform_receipt_sha,
            }),
            radar_semantics_certificate_id=(
                "exploratory.normalized-cache-class-not-physical.v1"
            ),
            radar_semantics_certificate_sha256=semantics_sha,
            transform_certificate_id=(
                "exploratory.local-metric-max-pool-not-native.v1"
            ),
            transform_certificate_sha256=self._view.transform.fingerprint,
            transform_receipt_id="transform-receipt-" + transform_receipt_sha,
            transform_receipt_sha256=transform_receipt_sha,
            transform_config_sha256=_sha_mapping({
                "maximum_analysis_pixels": self._view.transform.maximum_analysis_pixels,
                "maximum_source_grid_cells": (
                    self._view.transform.maximum_source_grid_cells
                ),
            }),
            target_grid_sha256=analysis_grid_sha256(frame.grid),
            detector_config_sha256=self._detector_sha256,
            pipeline_config_sha256=self._profile_sha256,
            normalized_frame=frame,
            detection_frame=detected,
            pixel_accounting=accounting,
            operational_admission_blockers=CALIBRATION_REPLAY_BLOCKERS,
        )
        if float(monotonic()) >= deadline_at:
            raise EvidenceToCellsShadowDeadlineError(
                "calibration science composition deadline expired"
            )
        return ShadowCompositionResult(run_id=run_id, evidence=evidence, issues=())


def _authorized_path(
    authority: Mapping[str, Any],
    *,
    label: str,
    access: str,
) -> tuple[Path, str | None]:
    paths = authority.get("authorized_paths")
    if not isinstance(paths, list):
        raise CalibrationScienceReplayError("authority paths are invalid")
    matches = tuple(
        item
        for item in paths
        if isinstance(item, dict) and item.get("label") == label
    )
    if (
        len(matches) != 1
        or matches[0].get("access") != access
        or type(matches[0].get("path")) is not str
    ):
        raise CalibrationScienceReplayError("authorized path access differs")
    return Path(matches[0]["path"]), matches[0].get("content_sha256")


def build_calibration_replay_orchestrator(
    authority: Mapping[str, Any],
) -> DurableScienceShadowOrchestrator:
    """Spawn-importable audited factory; opens only export and receipt paths."""

    config = authority.get("factory_config")
    if not isinstance(config, dict) or set(config) != {
        "detector",
        "export_file_sha256",
        "export_label",
        "maximum_analysis_pixels",
        "maximum_runtime_seconds",
        "maximum_source_grid_cells",
        "sequence_id",
    }:
        raise CalibrationScienceReplayError("calibration factory config differs")
    export_path, authorized_sha = _authorized_path(
        authority,
        label=config["export_label"],
        access="read_file",
    )
    expected_export_sha = _require_sha(
        config["export_file_sha256"],
        "factory export SHA",
    )
    if authorized_sha != expected_export_sha:
        raise CalibrationScienceReplayError("factory/authority export SHA differs")
    export = load_calibration_replay_export(
        export_path,
        expected_file_sha256=expected_export_sha,
    )
    if export.sequence_id != config["sequence_id"]:
        raise CalibrationScienceReplayError("factory sequence identity differs")
    try:
        detector = HierarchicalDetectorConfig(**dict(config["detector"]))
    except (TypeError, ValueError) as exc:
        raise CalibrationScienceReplayError("factory detector config is invalid") from exc
    if _detector_mapping(detector) != config["detector"]:
        raise CalibrationScienceReplayError("factory detector mapping differs")
    composer = CalibrationReplayComposer(
        export,
        export_file_sha256=expected_export_sha,
        maximum_source_grid_cells=config["maximum_source_grid_cells"],
        maximum_analysis_pixels=config["maximum_analysis_pixels"],
        maximum_runtime_seconds=config["maximum_runtime_seconds"],
        detector_config=detector,
    )
    receipt_path, _unused = _authorized_path(
        authority,
        label=authority["receipt_store_label"],
        access="transactional_file",
    )
    try:
        limits = ScienceShadowLimits(**dict(authority["science_limits"]))
    except (TypeError, ValueError) as exc:
        raise CalibrationScienceReplayError("factory science limits are invalid") from exc
    return DurableScienceShadowOrchestrator(
        engine_id=authority["engine_id"],
        composer=composer,
        tracking_policy=calibration_replay_tracking_policy(
            detector,
            maximum_analysis_pixels=config["maximum_analysis_pixels"],
        ),
        receipt_path=receipt_path,
        limits=limits,
    )


@dataclass(frozen=True, slots=True)
class CalibrationReplayAuthority:
    authority_path: Path
    authority_sha256: str
    authority_document: Mapping[str, Any]
    export: CalibrationReplayExport
    factory_contract: ScienceShadowFactoryContract
    isolation_limits: ScienceShadowIsolationLimits
    science_limits: ScienceShadowLimits
    receipt_path: Path


def create_calibration_replay_authority(
    *,
    export_path: str | Path,
    receipt_path: str | Path,
    authority_path: str | Path,
    isolation_limits: ScienceShadowIsolationLimits | None = None,
    science_limits: ScienceShadowLimits | None = None,
    maximum_source_grid_cells: int = 700_000,
    maximum_analysis_pixels: int = 20_000,
    maximum_runtime_seconds: float = 45.0,
) -> CalibrationReplayAuthority:
    """Create one exact authority and initialize only its separate receipt DB."""

    export_file = Path(export_path).resolve(strict=True)
    export_body = _read_regular_file(
        export_file,
        maximum_bytes=MAX_CALIBRATION_EXPORT_BYTES,
    )
    export_file_sha = _sha_bytes(export_body)
    export = load_calibration_replay_export(
        export_file,
        expected_file_sha256=export_file_sha,
    )
    receipt_file = Path(receipt_path).resolve(strict=False)
    authority_file = Path(authority_path).resolve(strict=False)
    if len({export_file, receipt_file, authority_file}) != 3:
        raise CalibrationScienceReplayError("authority/export/receipt paths overlap")
    receipt_file.parent.mkdir(parents=True, exist_ok=True)
    authority_file.parent.mkdir(parents=True, exist_ok=True)
    process_limits = isolation_limits or ScienceShadowIsolationLimits(
        wall_seconds=60.0,
        cpu_seconds=45.0,
        rss_bytes=1024 * 1024**2,
        max_authority_bytes=MAX_CALIBRATION_EXPORT_BYTES,
        max_request_bytes=32 * 1024,
        max_result_bytes=64 * 1024,
        max_authorized_paths=8,
    )
    if process_limits.max_authority_bytes < len(export_body):
        raise CalibrationScienceReplayError("process authority bound is below export")
    durable_limits = science_limits or ScienceShadowLimits(
        max_receipts=256,
        checkpoint_interval_frames=32,
        max_replay_frames=512,
        max_runtime_seconds=min(45.0, float(maximum_runtime_seconds)),
    )
    factory = ScienceShadowFactoryContract(
        factory_id=CALIBRATION_REPLAY_FACTORY_ID,
        target=CALIBRATION_REPLAY_FACTORY_TARGET,
        opens_only_authorized_paths=True,
        hard_kill_safe=True,
        transactional_receipt_only=True,
        spawns_descendants=False,
    )
    detector = calibration_replay_detector_config()
    config = {
        "detector": _detector_mapping(detector),
        "export_file_sha256": export_file_sha,
        "export_label": "calibration_export",
        "maximum_analysis_pixels": maximum_analysis_pixels,
        "maximum_runtime_seconds": float(maximum_runtime_seconds),
        "maximum_source_grid_cells": maximum_source_grid_cells,
        "sequence_id": export.sequence_id,
    }
    document: dict[str, Any] = {
        "authorized_paths": [
            {
                "access": "read_file",
                "content_sha256": export_file_sha,
                "label": "calibration_export",
                "path": str(export_file),
            },
            {
                "access": "transactional_file",
                "content_sha256": None,
                "label": "science_receipts",
                "path": str(receipt_file),
            },
        ],
        "contract": SCIENCE_SHADOW_PROCESS_AUTHORITY_CONTRACT,
        "engine_id": "science-calibration-" + export_file_sha[:32],
        "factory_config": config,
        "factory_contract_sha256": factory.fingerprint,
        "factory_id": factory.factory_id,
        "isolation_limits": process_limits.to_mapping(),
        "receipt_store_label": "science_receipts",
        "runtime_profile_sha256": "0" * 64,
        "science_limits": durable_limits.to_mapping(),
    }
    orchestrator = build_calibration_replay_orchestrator(document)
    document["runtime_profile_sha256"] = orchestrator.runtime_profile_sha256
    body = _canonical_json_bytes(document)
    if len(body) > process_limits.max_authority_bytes:
        raise CalibrationScienceReplayError("calibration authority exceeds bound")
    _atomic_write_new_or_identical(authority_file, body)
    return CalibrationReplayAuthority(
        authority_path=authority_file,
        authority_sha256=_sha_bytes(body),
        authority_document=document,
        export=export,
        factory_contract=factory,
        isolation_limits=process_limits,
        science_limits=durable_limits,
        receipt_path=receipt_file,
    )


def recover_calibration_replay_state(
    authority: CalibrationReplayAuthority,
    *,
    deadline_seconds: float = 45.0,
) -> CellTrackEngineState:
    """Offline audit helper that reuses the exact durable restart algorithm."""

    orchestrator = build_calibration_replay_orchestrator(
        authority.authority_document
    )
    orchestrator._recover(time.monotonic() + deadline_seconds)
    if orchestrator._state is None:
        raise CalibrationScienceReplayError("durable calibration state is absent")
    return orchestrator._state


__all__ = [
    "CALIBRATION_REPLAY_BLOCKERS",
    "CALIBRATION_REPLAY_CAVEATS",
    "CALIBRATION_REPLAY_CONNECTION_MODE",
    "CALIBRATION_REPLAY_EXPORT_CONTRACT",
    "CALIBRATION_REPLAY_FACTORY_ID",
    "CALIBRATION_REPLAY_FACTORY_TARGET",
    "CalibrationReplayAuthority",
    "CalibrationReplayComposer",
    "CalibrationReplayExport",
    "CalibrationReplayExportReceipt",
    "CalibrationReplayFrame",
    "CalibrationScienceReplayError",
    "calibration_replay_detector_config",
    "calibration_replay_tracking_policy",
    "create_calibration_replay_authority",
    "export_normalized_cache_sequence",
    "load_calibration_replay_export",
    "recover_calibration_replay_state",
]
