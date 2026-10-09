"""Closed read-only replay authority for one immutable DWD RV sequence.

This module is deliberately offline and unwired.  It accepts only the
content-addressed 36-frame export named by the constants below, revalidates
the complete +000 observation, physical-quantity, mask, grid, detector and
lineage chain, and exposes the already existing durable science orchestrator
through the already existing hard process boundary.

The export is useful robustness evidence.  It is not cell-identity or speed
truth and can never grant meteorological admission or publish a live result.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time
from typing import Any, Literal, Mapping

from .analysis_grid_transform import analysis_grid_sha256
from .dwd_rv_observation_replay import default_belgium_observation_profile
from .dwd_rv_science_sequence import (
    CADENCE_SECONDS,
    DwdRvProcessMetric,
    DwdRvScienceSequenceIntegrityError,
    DwdRvSourceManifestReceipt,
    OBSERVATION_LEAD_SECONDS,
    PROCESS_METRICS_CONTRACT,
    SEQUENCE_PRODUCT,
    SEQUENCE_PROVIDER,
    SEQUENCE_QUANTITY,
    SEQUENCE_UNIT,
    load_evidence_record,
    load_sequence_manifest,
    load_source_manifest,
)
from .hard_decoder_isolation import _canonical_json_bytes
from .science_shadow_calibration_replay import calibration_replay_tracking_policy
from .science_shadow_orchestrator import (
    DurableScienceShadowOrchestrator,
    ScienceShadowLimits,
    ShadowCompositionResult,
    ShadowEvidenceFrame,
)
from .science_shadow_process_isolation import (
    SCIENCE_SHADOW_PROCESS_AUTHORITY_CONTRACT,
    ScienceShadowFactoryContract,
    ScienceShadowIsolationLimits,
    _read_regular_file,
)
from .tracking_core.engine.cell_track_engine import (
    CellTrackEnginePolicy,
    CellTrackEngineState,
)
from .tracking_core.engine.hierarchical_detection import (
    HIERARCHICAL_DETECTOR_VERSION,
    HierarchicalDetectorConfig,
    detect_hierarchical_cells,
)


DWD_SCIENCE_REPLAY_FACTORY_ID = "science-dwd-rv-replay-factory-v1"
DWD_SCIENCE_REPLAY_FACTORY_TARGET = (
    "storm_tracker_worker.science_shadow_dwd_replay:"
    "build_dwd_science_replay_orchestrator"
)
DWD_SCIENCE_REPLAY_CONTRACT = "stv3-science-shadow-dwd-replay-authority-v1"

EXPECTED_SEQUENCE_MANIFEST_SHA256 = (
    "e94b8f14393fb0e2076fd54ff7b0a2c0133416cd1cde87bb18acd01df5eda48a"
)
EXPECTED_SEQUENCE_ID = (
    "0fc414def8b41ddd27c433a73875d625ea40f5b485a2d330c6edd654c3848dc2"
)
EXPECTED_SOURCE_MANIFEST_SHA256 = (
    "20335e6c50b35d57dd111bea7e3670f515e933da5239ba39e74ccebc41030852"
)
EXPECTED_METRICS_MANIFEST_SHA256 = (
    "7e501592a64faf9bece7af064a425a2d7a3f96719ae90847f7b9753a9a308b60"
)
EXPECTED_TARGET_GRID_SHA256 = (
    "95c7c9a3e805bdad47474b803b49332d891b60f2dee270bb1706bf8ae4dbd364"
)
EXPECTED_RADAR_SEMANTICS_CERTIFICATE_ID = (
    "dwd.rv.accumulation-to-interval-mean-rate.v1"
)
EXPECTED_RADAR_SEMANTICS_CERTIFICATE_SHA256 = (
    "a726c4a5aad6dda908d32be98aec54ab9d9459ff9597642ae5eb68e163697b62"
)
EXPECTED_DETECTOR_CONFIG_SHA256 = (
    "df31dc7ab19bb51d758775a2911111e418c35922268809f4af04465581578f27"
)
EXPECTED_PIPELINE_CONFIG_SHA256 = (
    "4783cfe32d8f98d439750b2bb2c42702852938e33431691e2f5e3cf6e7aee115"
)
EXPECTED_TRANSFORM_CONFIG_SHA256 = (
    "0176a6f5785e1785a8598ef772c16d2772297c806ab66b56a77d2faec6df5267"
)
EXPECTED_FRAME_COUNT = 36
EXPECTED_ANALYSIS_PIXELS = 316 * 316
DWD_TRACKING_SOURCE = (
    "canonical:dwd_radolan/dwd_open_data_composite_rv_capture"
)

MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_EVIDENCE_RECORD_BYTES = 1024 * 1024
MAX_EVIDENCE_AGGREGATE_BYTES = 32 * 1024 * 1024

DwdIdentityMode = Literal["v4_1_per_transition", "v5_multi_scan"]

DWD_SCIENCE_REPLAY_CAVEATS = (
    "physical_dwd_interval_mean_precipitation_rate_mm_per_hour",
    "provider_nodata_and_undetect_are_preserved_as_missing_not_dry",
    "per_frame_transform_hash_is_lineage_not_tracking_source_identity",
    "only_plus000_observations_are_opened",
    "forecast_leads_are_inventory_only_and_never_tracking_inputs",
    "independent_cell_identity_truth_is_unavailable",
    "independent_speed_truth_is_unavailable",
    "sequence_has_no_cells_at_the_certified_one_mm_per_hour_threshold",
    "finite_spatial_support_is_extremely_sparse",
    "association_costs_and_multi_scan_weights_are_not_calibrated_probabilities",
    "results_are_cross_provider_robustness_evidence_not_accuracy_evidence",
)

DWD_SCIENCE_REPLAY_BLOCKERS = (
    "hard_process_isolation_not_wired",
    "independent_identity_truth_unavailable",
    "independent_speed_truth_unavailable",
    "independent_holdout_calibration_unavailable",
    "finite_coverage_insufficient_for_identity_evaluation",
    "no_observable_cells_at_certified_detector_threshold",
    "multi_hypothesis_identity_not_externally_validated",
    "weather_system_rebinding_forbidden",
    "operational_admission_certificates_incomplete",
    "operational_snapshot_publication_forbidden",
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID_RE = re.compile(r"^run-[0-9a-f]{64}$")


class DwdScienceReplayError(RuntimeError):
    """The immutable DWD science replay contract is absent or inconsistent."""


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_mapping(value: Any) -> str:
    return _sha_bytes(_canonical_json_bytes(value))


def _require_sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise DwdScienceReplayError(f"{label} is not a lowercase SHA-256")
    return value


def _require_exact_mapping(
    value: object,
    keys: set[str],
    label: str,
) -> Mapping[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise DwdScienceReplayError(f"{label} fields are not exact")
    return value


def _read_canonical_json(
    path: Path,
    *,
    expected_sha256: str,
    maximum_bytes: int,
) -> tuple[bytes, Mapping[str, Any]]:
    expected = _require_sha(expected_sha256, "expected file SHA-256")
    try:
        body = _read_regular_file(path, maximum_bytes=maximum_bytes)
    except Exception as exc:
        raise DwdScienceReplayError("replay file cannot be opened safely") from exc
    if _sha_bytes(body) != expected:
        raise DwdScienceReplayError("replay file checksum differs")
    try:
        value = json.loads(body.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise DwdScienceReplayError("replay file is not bounded ASCII JSON") from exc
    if not isinstance(value, dict) or _canonical_json_bytes(value) != body:
        raise DwdScienceReplayError("replay file is not canonical JSON")
    return body, value


def _nearest_rank(values: tuple[float | int, ...], fraction: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise DwdScienceReplayError("metric distribution is empty")
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def _detector_mapping(config: HierarchicalDetectorConfig) -> dict[str, Any]:
    return {
        "centroid_method": config.centroid_method,
        "connectivity": config.connectivity,
        "core_position_sigma_m": config.core_position_sigma_m,
        "core_threshold_mm_h": config.core_threshold,
        "detector_version": HIERARCHICAL_DETECTOR_VERSION,
        "minimum_core_area_m2": config.minimum_core_area_m2,
        "minimum_shield_area_m2": config.minimum_shield_area_m2,
        "shield_position_sigma_m": config.shield_position_sigma_m,
        "shield_threshold_mm_h": config.shield_threshold,
        "threshold_semantics": "object_definition_only_not_wet_dry_admission",
    }


@dataclass(frozen=True, slots=True)
class DwdScienceReplayEntry:
    frame_number: int
    valid_at: float
    canonical_run_id: str
    evidence_candidate_id: str
    evidence_content_sha256: str
    evidence_record_sha256: str
    evidence_record_bytes: int
    normalized_frame_id: str
    completion_event_id: str
    completion_event_sha256: str
    completion_output_sha256: str
    finite_pixels: int
    nodata_pixels: int
    undetect_pixels: int
    mixed_pixels: int
    outside_support_pixels: int
    detection_objects: int
    maximum_rate_mm_h: float | None

    def __post_init__(self) -> None:
        if self.frame_number < 0:
            raise ValueError("negative DWD frame number")
        if _RUN_ID_RE.fullmatch(self.canonical_run_id) is None:
            raise ValueError("DWD canonical run ID is invalid")
        for label, value in (
            ("evidence content", self.evidence_content_sha256),
            ("evidence record", self.evidence_record_sha256),
            ("completion event", self.completion_event_sha256),
            ("completion output", self.completion_output_sha256),
        ):
            _require_sha(value, label)


@dataclass(frozen=True, slots=True)
class DwdScienceReplayExport:
    export_root: Path
    sequence_manifest_path: Path
    sequence_manifest_sha256: str
    sequence_id: str
    source_manifest_sha256: str
    metrics_manifest_sha256: str
    target_grid_sha256: str
    entries: tuple[DwdScienceReplayEntry, ...]
    source_manifest_bytes: int
    metrics_manifest_bytes: int
    evidence_aggregate_bytes: int
    upstream_process_metrics: Mapping[str, Any]

    @property
    def run_ids(self) -> tuple[str, ...]:
        return tuple(item.canonical_run_id for item in self.entries)

    @property
    def finite_pixels(self) -> tuple[int, ...]:
        return tuple(item.finite_pixels for item in self.entries)


def _exact_content_addressed_path(
    root: Path,
    directory_name: str,
    digest: str,
) -> Path:
    expected = root / directory_name / f"{_require_sha(digest, 'object SHA-256')}.json"
    if expected.is_symlink() or not expected.is_file():
        raise DwdScienceReplayError("content-addressed replay object is absent or unsafe")
    resolved = expected.resolve(strict=True)
    if resolved != expected or root not in resolved.parents:
        raise DwdScienceReplayError("content-addressed object escaped export root")
    return expected


def _validate_exact_inventory(
    root: Path,
    *,
    evidence_shas: tuple[str, ...],
) -> None:
    expected_objects = {f"{value}.json" for value in evidence_shas}
    expected_manifests = {
        f"{EXPECTED_SEQUENCE_MANIFEST_SHA256}.json",
        f"{EXPECTED_SOURCE_MANIFEST_SHA256}.json",
        f"{EXPECTED_METRICS_MANIFEST_SHA256}.json",
    }
    for name, expected in (
        ("objects", expected_objects),
        ("manifests", expected_manifests),
    ):
        directory = root / name
        if directory.is_symlink() or not directory.is_dir():
            raise DwdScienceReplayError("replay inventory directory is unsafe")
        actual: set[str] = set()
        for path in directory.iterdir():
            if path.is_symlink() or not path.is_file():
                raise DwdScienceReplayError("replay inventory contains unsafe entries")
            actual.add(path.name)
        if actual != expected:
            raise DwdScienceReplayError("replay inventory differs from sealed object set")


def _load_metrics_manifest(
    path: Path,
    *,
    valid_times: tuple[float, ...],
) -> tuple[bytes, Mapping[str, Any], tuple[DwdRvProcessMetric, ...]]:
    body, value = _read_canonical_json(
        path,
        expected_sha256=EXPECTED_METRICS_MANIFEST_SHA256,
        maximum_bytes=MAX_MANIFEST_BYTES,
    )
    manifest = _require_exact_mapping(value, {
        "aggregates", "configured_boundary_memory_cap_bytes",
        "configured_cpu_cap_seconds", "configured_wall_cap_seconds", "contract",
        "entries", "frame_count", "isolation_authority_sha256",
        "isolation_limits_sha256", "meteorological_admission_granted",
        "percentile_method", "sequence_id", "sequence_manifest_sha256",
        "source_manifest_sha256",
    }, "DWD process metrics manifest")
    if (
        manifest["contract"] != PROCESS_METRICS_CONTRACT
        or manifest["frame_count"] != EXPECTED_FRAME_COUNT
        or manifest["sequence_id"] != EXPECTED_SEQUENCE_ID
        or manifest["sequence_manifest_sha256"]
        != EXPECTED_SEQUENCE_MANIFEST_SHA256
        or manifest["source_manifest_sha256"] != EXPECTED_SOURCE_MANIFEST_SHA256
        or manifest["meteorological_admission_granted"] is not False
        or manifest["percentile_method"] != "nearest_rank"
    ):
        raise DwdScienceReplayError("DWD process metrics binding differs")
    _require_sha(manifest["isolation_authority_sha256"], "upstream authority SHA")
    _require_sha(manifest["isolation_limits_sha256"], "upstream limits SHA")
    raw_entries = manifest["entries"]
    if not isinstance(raw_entries, list) or len(raw_entries) != EXPECTED_FRAME_COUNT:
        raise DwdScienceReplayError("DWD process metrics entry accounting differs")
    metrics: list[DwdRvProcessMetric] = []
    for index, raw in enumerate(raw_entries):
        item = _require_exact_mapping(raw, {
            "child_cpu_seconds", "child_peak_boundary_bytes",
            "child_peak_rss_bytes", "elapsed_seconds", "event_id",
            "event_sha256", "frame_number", "output_sha256", "valid_at",
        }, "DWD process metric")
        try:
            metric = DwdRvProcessMetric(**dict(item))
        except (TypeError, ValueError) as exc:
            raise DwdScienceReplayError("DWD process metric is invalid") from exc
        if (
            metric.to_mapping() != item
            or metric.frame_number != index
            or metric.valid_at != valid_times[index]
        ):
            raise DwdScienceReplayError("DWD process metric order differs")
        metrics.append(metric)
    records = tuple(metrics)
    if len({item.event_id for item in records}) != len(records):
        raise DwdScienceReplayError("DWD process metric event is duplicated")
    boundary_cap = manifest["configured_boundary_memory_cap_bytes"]
    cpu_cap = manifest["configured_cpu_cap_seconds"]
    wall_cap = manifest["configured_wall_cap_seconds"]
    if (
        isinstance(boundary_cap, bool)
        or not isinstance(boundary_cap, int)
        or boundary_cap <= max(item.child_peak_boundary_bytes for item in records)
        or isinstance(cpu_cap, bool)
        or not isinstance(cpu_cap, (int, float))
        or not math.isfinite(float(cpu_cap))
        or float(cpu_cap) <= max(item.child_cpu_seconds for item in records)
        or isinstance(wall_cap, bool)
        or not isinstance(wall_cap, (int, float))
        or not math.isfinite(float(wall_cap))
        or float(wall_cap) <= max(item.elapsed_seconds for item in records)
    ):
        raise DwdScienceReplayError("DWD upstream resource cap evidence differs")
    distributions = {
        "child_cpu_seconds": tuple(item.child_cpu_seconds for item in records),
        "child_peak_boundary_bytes": tuple(
            item.child_peak_boundary_bytes for item in records
        ),
        "child_peak_rss_bytes": tuple(item.child_peak_rss_bytes for item in records),
        "elapsed_seconds": tuple(item.elapsed_seconds for item in records),
    }
    aggregates = _require_exact_mapping(
        manifest["aggregates"], set(distributions), "DWD metrics aggregates"
    )
    for label, values in distributions.items():
        expected = {
            "max": max(values),
            "p50": int(_nearest_rank(values, 0.50))
            if label.endswith("bytes") else _nearest_rank(values, 0.50),
            "p95": int(_nearest_rank(values, 0.95))
            if label.endswith("bytes") else _nearest_rank(values, 0.95),
        }
        if aggregates[label] != expected:
            raise DwdScienceReplayError("DWD process metric aggregate differs")
    return body, manifest, records


def _validate_evidence(
    evidence: ShadowEvidenceFrame,
    completion: Mapping[str, Any],
    *,
    sequence_entry: Mapping[str, Any],
    source_frame: Mapping[str, Any],
    process_metric: DwdRvProcessMetric,
    detector: HierarchicalDetectorConfig,
) -> DwdScienceReplayEntry:
    frame_number = sequence_entry["frame_number"]
    frame = evidence.normalized_frame
    if (
        source_frame["frame_number"] != frame_number
        or source_frame["base_time"] != sequence_entry["valid_at"]
        or source_frame["lead_seconds"] != OBSERVATION_LEAD_SECONDS
        or evidence.canonical_run_id != sequence_entry["canonical_run_id"]
        or evidence.evidence_candidate_id
        != sequence_entry["evidence_candidate_id"]
        or evidence.evidence_content_sha256
        != sequence_entry["evidence_content_sha256"]
        or frame.frame_id != sequence_entry["normalized_frame_id"]
        or frame.valid_at != sequence_entry["valid_at"]
        or frame.quantity != SEQUENCE_QUANTITY
        or frame.unit != SEQUENCE_UNIT
        or analysis_grid_sha256(frame.grid) != EXPECTED_TARGET_GRID_SHA256
        or evidence.target_grid_sha256 != EXPECTED_TARGET_GRID_SHA256
    ):
        raise DwdScienceReplayError("DWD evidence/sequence/source binding differs")
    expected_source = (
        f"canonical:{SEQUENCE_PROVIDER}/{SEQUENCE_PRODUCT}:semantics:"
        f"{EXPECTED_RADAR_SEMANTICS_CERTIFICATE_SHA256}:transform:"
        f"{evidence.transform_certificate_sha256}"
    )
    if (
        frame.source != expected_source
        or evidence.radar_semantics_certificate_id
        != EXPECTED_RADAR_SEMANTICS_CERTIFICATE_ID
        or evidence.radar_semantics_certificate_sha256
        != EXPECTED_RADAR_SEMANTICS_CERTIFICATE_SHA256
        or evidence.detector_config_sha256 != EXPECTED_DETECTOR_CONFIG_SHA256
        or evidence.pipeline_config_sha256 != EXPECTED_PIPELINE_CONFIG_SHA256
        or evidence.transform_config_sha256 != EXPECTED_TRANSFORM_CONFIG_SHA256
        or not evidence.transform_certificate_id.startswith(
            "dwd.rv.de1200.pixel-edges.v1:"
        )
        or not evidence.operational_admission_blockers
        or "hard_process_isolation_not_wired"
        not in evidence.operational_admission_blockers
    ):
        raise DwdScienceReplayError("DWD physical semantics/profile binding differs")
    if (
        completion["event_id"] != process_metric.event_id
        or completion["event_sha256"] != process_metric.event_sha256
        or completion["output_sha256"] != process_metric.output_sha256
    ):
        raise DwdScienceReplayError("DWD completion/process lineage differs")
    flattened = tuple(value for row in frame.values for value in row)
    finite = tuple(value for value in flattened if value is not None)
    if any(
        not math.isfinite(float(value)) or float(value) < 0.0
        for value in finite
    ):
        raise DwdScienceReplayError("DWD physical rate contains invalid values")
    accounting = evidence.pixel_accounting
    missing = len(flattened) - len(finite)
    shield = sum(value >= detector.shield_threshold for value in finite)
    core = sum(value >= detector.core_threshold for value in finite)
    excluded = (
        accounting.excluded_nodata_pixels
        + accounting.excluded_undetect_pixels
        + accounting.excluded_mixed_pixels
        + accounting.excluded_outside_support_pixels
    )
    if (
        len(flattened) != EXPECTED_ANALYSIS_PIXELS
        or accounting.total_analysis_pixels != len(flattened)
        or accounting.valid_detector_input_pixels != len(finite)
        or accounting.valid_below_shield_threshold_pixels != len(finite) - shield
        or accounting.rain_shield_pixels != shield
        or accounting.embedded_core_pixels != core
        or excluded != missing
    ):
        raise DwdScienceReplayError("DWD pixel accounting is not lossless")
    detected = detect_hierarchical_cells(frame, detector)
    if (
        detected != evidence.detection_frame
        or accounting.detection_objects != len(detected.detections)
        or accounting.rain_shield_objects != len(detected.rain_shields)
        or accounting.embedded_core_objects != len(detected.embedded_cores)
        or accounting.low_confidence_objects
        != len(detected.low_confidence_fragments)
    ):
        raise DwdScienceReplayError("DWD detector replay differs from evidence")
    return DwdScienceReplayEntry(
        frame_number=frame_number,
        valid_at=frame.valid_at,
        canonical_run_id=evidence.canonical_run_id,
        evidence_candidate_id=evidence.evidence_candidate_id,
        evidence_content_sha256=evidence.evidence_content_sha256,
        evidence_record_sha256=sequence_entry["evidence_record_sha256"],
        evidence_record_bytes=sequence_entry["evidence_record_bytes"],
        normalized_frame_id=frame.frame_id,
        completion_event_id=completion["event_id"],
        completion_event_sha256=completion["event_sha256"],
        completion_output_sha256=completion["output_sha256"],
        finite_pixels=len(finite),
        nodata_pixels=accounting.excluded_nodata_pixels,
        undetect_pixels=accounting.excluded_undetect_pixels,
        mixed_pixels=accounting.excluded_mixed_pixels,
        outside_support_pixels=accounting.excluded_outside_support_pixels,
        detection_objects=accounting.detection_objects,
        maximum_rate_mm_h=max(finite) if finite else None,
    )


def load_dwd_science_replay_export(
    export_root: str | Path,
    sequence_manifest_path: str | Path,
) -> DwdScienceReplayExport:
    """Verify the exact immutable 36-frame +000 evidence export read-only."""

    root = Path(export_root)
    manifest_path = Path(sequence_manifest_path)
    if (
        not root.is_absolute()
        or root.is_symlink()
        or not root.is_dir()
        or not manifest_path.is_absolute()
        or manifest_path.is_symlink()
        or not manifest_path.is_file()
    ):
        raise DwdScienceReplayError("DWD replay paths are absent, relative or unsafe")
    root = root.resolve(strict=True)
    manifest_path = manifest_path.resolve(strict=True)
    expected_manifest = _exact_content_addressed_path(
        root, "manifests", EXPECTED_SEQUENCE_MANIFEST_SHA256
    )
    if manifest_path != expected_manifest:
        raise DwdScienceReplayError("DWD sequence manifest path is not definitive")
    try:
        sequence = load_sequence_manifest(
            manifest_path,
            expected_sha256=EXPECTED_SEQUENCE_MANIFEST_SHA256,
        )
    except DwdRvScienceSequenceIntegrityError as exc:
        raise DwdScienceReplayError("DWD sequence manifest failed validation") from exc
    if (
        sequence["sequence_id"] != EXPECTED_SEQUENCE_ID
        or sequence["source_manifest_sha256"]
        != EXPECTED_SOURCE_MANIFEST_SHA256
        or sequence["target_grid_sha256"] != EXPECTED_TARGET_GRID_SHA256
        or sequence["frame_count"] != EXPECTED_FRAME_COUNT
        or sequence["cadence_seconds"] != CADENCE_SECONDS
        or sequence["lead_seconds"] != OBSERVATION_LEAD_SECONDS
        or sequence["provider"] != SEQUENCE_PROVIDER
        or sequence["product"] != SEQUENCE_PRODUCT
        or sequence["quantity"] != SEQUENCE_QUANTITY
        or sequence["unit"] != SEQUENCE_UNIT
        or sequence["forecast_generated"] is not False
        or sequence["meteorological_admission_granted"] is not False
        or sequence["motion_skill_claimed"] is not False
    ):
        raise DwdScienceReplayError("DWD definitive sequence identity differs")
    raw_entries = tuple(sequence["entries"])
    valid_times = tuple(float(item["valid_at"]) for item in raw_entries)
    source_path = _exact_content_addressed_path(
        root, "manifests", EXPECTED_SOURCE_MANIFEST_SHA256
    )
    source_body, _source_value = _read_canonical_json(
        source_path,
        expected_sha256=EXPECTED_SOURCE_MANIFEST_SHA256,
        maximum_bytes=MAX_MANIFEST_BYTES,
    )
    if len(source_body) != sequence["source_manifest_bytes"]:
        raise DwdScienceReplayError("DWD source manifest byte binding differs")
    source_receipt = DwdRvSourceManifestReceipt(
        manifest_sha256=EXPECTED_SOURCE_MANIFEST_SHA256,
        manifest_bytes=len(source_body),
        manifest_path=source_path,
        frame_count=EXPECTED_FRAME_COUNT,
        total_archive_bytes=41_072_640,
        valid_times=valid_times,
    )
    try:
        source = load_source_manifest(source_receipt)
    except DwdRvScienceSequenceIntegrityError as exc:
        raise DwdScienceReplayError("DWD source manifest failed validation") from exc
    metrics_path = _exact_content_addressed_path(
        root, "manifests", EXPECTED_METRICS_MANIFEST_SHA256
    )
    metrics_body, metrics_manifest, metrics = _load_metrics_manifest(
        metrics_path,
        valid_times=valid_times,
    )
    evidence_shas = tuple(item["evidence_record_sha256"] for item in raw_entries)
    if len(set(evidence_shas)) != EXPECTED_FRAME_COUNT:
        raise DwdScienceReplayError("DWD evidence record identity is duplicated")
    _validate_exact_inventory(root, evidence_shas=evidence_shas)
    profile = default_belgium_observation_profile()
    if (
        analysis_grid_sha256(profile.target_grid) != EXPECTED_TARGET_GRID_SHA256
        or _sha_mapping(_detector_mapping(profile.detector_config))
        != EXPECTED_DETECTOR_CONFIG_SHA256
        or profile.pipeline_config.fingerprint != EXPECTED_PIPELINE_CONFIG_SHA256
        or profile.transform_config.fingerprint != EXPECTED_TRANSFORM_CONFIG_SHA256
    ):
        raise DwdScienceReplayError("local DWD science profile differs from export")
    source_frames = tuple(source["frames"])
    loaded: list[DwdScienceReplayEntry] = []
    aggregate_bytes = 0
    for index, raw in enumerate(raw_entries):
        record_path = _exact_content_addressed_path(
            root, "objects", raw["evidence_record_sha256"]
        )
        size = record_path.stat().st_size
        aggregate_bytes += size
        if (
            size != raw["evidence_record_bytes"]
            or not 1 <= size <= MAX_EVIDENCE_RECORD_BYTES
            or aggregate_bytes > MAX_EVIDENCE_AGGREGATE_BYTES
        ):
            raise DwdScienceReplayError("DWD evidence byte accounting exceeds bound")
        try:
            evidence, completion = load_evidence_record(
                record_path,
                expected_sha256=raw["evidence_record_sha256"],
            )
        except DwdRvScienceSequenceIntegrityError as exc:
            raise DwdScienceReplayError("DWD evidence record failed validation") from exc
        loaded.append(_validate_evidence(
            evidence,
            completion,
            sequence_entry=raw,
            source_frame=source_frames[index],
            process_metric=metrics[index],
            detector=profile.detector_config,
        ))
    entries = tuple(loaded)
    if (
        tuple(item.frame_number for item in entries)
        != tuple(range(EXPECTED_FRAME_COUNT))
        or tuple(item.valid_at for item in entries) != valid_times
        or any(
            later - earlier != CADENCE_SECONDS
            for earlier, later in zip(valid_times, valid_times[1:])
        )
    ):
        raise DwdScienceReplayError("DWD replay order/cadence differs")
    return DwdScienceReplayExport(
        export_root=root,
        sequence_manifest_path=manifest_path,
        sequence_manifest_sha256=EXPECTED_SEQUENCE_MANIFEST_SHA256,
        sequence_id=EXPECTED_SEQUENCE_ID,
        source_manifest_sha256=EXPECTED_SOURCE_MANIFEST_SHA256,
        metrics_manifest_sha256=EXPECTED_METRICS_MANIFEST_SHA256,
        target_grid_sha256=EXPECTED_TARGET_GRID_SHA256,
        entries=entries,
        source_manifest_bytes=len(source_body),
        metrics_manifest_bytes=len(metrics_body),
        evidence_aggregate_bytes=aggregate_bytes,
        upstream_process_metrics=metrics_manifest,
    )


def dwd_science_tracking_policy(
    detector: HierarchicalDetectorConfig,
    *,
    identity_mode: DwdIdentityMode,
) -> CellTrackEnginePolicy:
    """DWD-labelled, ungated identity comparison with identical soft costs."""

    if identity_mode not in {"v4_1_per_transition", "v5_multi_scan"}:
        raise DwdScienceReplayError("DWD identity mode is unsupported")
    seed = calibration_replay_tracking_policy(
        detector,
        maximum_analysis_pixels=EXPECTED_ANALYSIS_PIXELS,
    )
    identity = seed.multi_hypothesis_identity
    assert identity is not None
    multi_scan = identity.multi_scan
    if identity_mode == "v4_1_per_transition":
        multi_scan = None
    else:
        assert multi_scan is not None
        multi_scan = replace(
            multi_scan,
            policy_id="dwd-rv-physical-multi-scan-v1",
        )
    return replace(
        seed,
        policy_id=f"dwd-rv-physical-{identity_mode}-tracking-v1",
        lifecycle=replace(
            seed.lifecycle,
            policy_id=f"dwd-rv-physical-{identity_mode}-lifecycle-v1",
        ),
        estimator=replace(
            seed.estimator,
            policy_id=f"dwd-rv-physical-{identity_mode}-estimator-v1",
        ),
        weather_system=replace(
            seed.weather_system,
            policy_id=f"dwd-rv-physical-{identity_mode}-systems-v1",
        ),
        multi_hypothesis_identity=replace(
            identity,
            policy_id=f"dwd-rv-physical-{identity_mode}-identity-v1",
            multi_scan=multi_scan,
        ),
    )


class DwdScienceReplayComposer:
    """On-demand reader for one already verified immutable evidence object."""

    def __init__(
        self,
        export: DwdScienceReplayExport,
        *,
        maximum_runtime_seconds: float,
    ) -> None:
        if not isinstance(export, DwdScienceReplayExport):
            raise TypeError("export must be DwdScienceReplayExport")
        if (
            isinstance(maximum_runtime_seconds, bool)
            or not isinstance(maximum_runtime_seconds, (int, float))
            or not math.isfinite(float(maximum_runtime_seconds))
            or not 0.05 <= float(maximum_runtime_seconds) <= 300.0
        ):
            raise ValueError("DWD composer runtime bound is invalid")
        profile = default_belgium_observation_profile()
        self._export = export
        self._detector = profile.detector_config
        self._maximum_runtime_seconds = float(maximum_runtime_seconds)
        self._entries = {item.canonical_run_id: item for item in export.entries}
        self._profile_sha256 = _sha_mapping({
            "algorithm": "stv3-science-shadow-dwd-replay-composer-v1",
            "caveats": list(DWD_SCIENCE_REPLAY_CAVEATS),
            "detector_config_sha256": EXPECTED_DETECTOR_CONFIG_SHA256,
            "metrics_manifest_sha256": export.metrics_manifest_sha256,
            "sequence_id": export.sequence_id,
            "sequence_manifest_sha256": export.sequence_manifest_sha256,
            "source_manifest_sha256": export.source_manifest_sha256,
            "target_grid_sha256": export.target_grid_sha256,
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

    def _tracking_evidence(
        self,
        upstream: ShadowEvidenceFrame,
    ) -> ShadowEvidenceFrame:
        """Derive a stable provider/product source without losing lineage.

        The immutable acquisition frame embeds its per-frame transform
        certificate in ``source``.  That is valuable provenance, but treating
        the transform hash as provider identity makes every adjacent frame a
        false provider handoff.  The science adapter therefore moves that
        distinction into a checksummed derived lineage record and gives the
        tracker the stable provider/product identity it actually expects.
        Values, grid, masks and physical units are unchanged.
        """

        upstream_frame = upstream.normalized_frame
        frame_id_sha = _sha_mapping({
            "contract": "stv3-dwd-tracking-source-alias-frame-v1",
            "stable_source": DWD_TRACKING_SOURCE,
            "upstream_frame_id": upstream_frame.frame_id,
            "upstream_source": upstream_frame.source,
        })
        frame = replace(
            upstream_frame,
            frame_id="shadow-frame-" + frame_id_sha,
            source=DWD_TRACKING_SOURCE,
        )
        detection = detect_hierarchical_cells(frame, self._detector)
        if len(detection.detections) != upstream.pixel_accounting.detection_objects:
            raise DwdScienceReplayError(
                "stable DWD source alias changed detector accounting"
            )
        analysis_sha = _sha_mapping({
            "contract": "stv3-dwd-tracking-source-alias-analysis-v1",
            "stable_frame_id": frame.frame_id,
            "stable_source": frame.source,
            "upstream_analysis_field_content_sha256": (
                upstream.analysis_field_content_sha256
            ),
            "upstream_frame_id": upstream_frame.frame_id,
            "upstream_source": upstream_frame.source,
        })
        lineage_sha = _sha_mapping({
            "contract": "stv3-dwd-tracking-source-alias-lineage-v1",
            "stable_analysis_field_content_sha256": analysis_sha,
            "upstream_analysis_lineage_sha256": upstream.analysis_lineage_sha256,
            "upstream_evidence_content_sha256": upstream.evidence_content_sha256,
            "upstream_transform_certificate_sha256": (
                upstream.transform_certificate_sha256
            ),
            "upstream_transform_receipt_sha256": upstream.transform_receipt_sha256,
        })
        evidence_sha = _sha_mapping({
            "analysis_field_content_sha256": analysis_sha,
            "analysis_lineage_sha256": lineage_sha,
            "contract": "stv3-dwd-tracking-source-alias-evidence-v1",
            "detector_config_sha256": upstream.detector_config_sha256,
            "pipeline_config_sha256": self._profile_sha256,
            "stable_frame_id": frame.frame_id,
            "upstream_evidence_content_sha256": upstream.evidence_content_sha256,
        })
        return replace(
            upstream,
            evidence_candidate_id="evidence-cells-" + evidence_sha,
            evidence_content_sha256=evidence_sha,
            analysis_field_id="analysis-field-" + analysis_sha,
            analysis_field_content_sha256=analysis_sha,
            analysis_lineage_sha256=lineage_sha,
            pipeline_config_sha256=self._profile_sha256,
            normalized_frame=frame,
            detection_frame=detection,
            operational_admission_blockers=tuple(sorted(set(
                upstream.operational_admission_blockers
                + DWD_SCIENCE_REPLAY_BLOCKERS
            ))),
        )

    def compose(self, *, run_id: str, deadline_at: float, monotonic) -> ShadowCompositionResult:
        if float(monotonic()) >= deadline_at:
            from .evidence_to_cells_shadow import EvidenceToCellsShadowDeadlineError
            raise EvidenceToCellsShadowDeadlineError("DWD science replay deadline expired")
        entry = self._entries.get(run_id)
        if entry is None:
            raise DwdScienceReplayError("run is absent from definitive DWD export")
        record_path = _exact_content_addressed_path(
            self._export.export_root, "objects", entry.evidence_record_sha256
        )
        try:
            evidence, completion = load_evidence_record(
                record_path,
                expected_sha256=entry.evidence_record_sha256,
            )
        except DwdRvScienceSequenceIntegrityError as exc:
            raise DwdScienceReplayError("DWD replay object changed after preflight") from exc
        if (
            evidence.canonical_run_id != entry.canonical_run_id
            or evidence.evidence_candidate_id != entry.evidence_candidate_id
            or evidence.evidence_content_sha256 != entry.evidence_content_sha256
            or evidence.normalized_frame.frame_id != entry.normalized_frame_id
            or evidence.normalized_frame.valid_at != entry.valid_at
            or completion["event_id"] != entry.completion_event_id
            or completion["event_sha256"] != entry.completion_event_sha256
            or completion["output_sha256"] != entry.completion_output_sha256
            or evidence.pixel_accounting.valid_detector_input_pixels
            != entry.finite_pixels
            or evidence.pixel_accounting.detection_objects
            != entry.detection_objects
            or detect_hierarchical_cells(evidence.normalized_frame, self._detector)
            != evidence.detection_frame
        ):
            raise DwdScienceReplayError("DWD replay object lineage changed after preflight")
        evidence = self._tracking_evidence(evidence)
        if float(monotonic()) >= deadline_at:
            from .evidence_to_cells_shadow import EvidenceToCellsShadowDeadlineError
            raise EvidenceToCellsShadowDeadlineError("DWD science replay deadline expired")
        return ShadowCompositionResult(run_id=run_id, evidence=evidence, issues=())


def _authorized_path(
    authority: Mapping[str, Any],
    *,
    label: str,
    access: str,
) -> tuple[Path, str | None]:
    paths = authority.get("authorized_paths")
    if not isinstance(paths, list):
        raise DwdScienceReplayError("DWD authority paths are invalid")
    matches = tuple(
        item for item in paths
        if isinstance(item, dict) and item.get("label") == label
    )
    if (
        len(matches) != 1
        or matches[0].get("access") != access
        or type(matches[0].get("path")) is not str
    ):
        raise DwdScienceReplayError("DWD authorized path access differs")
    return Path(matches[0]["path"]), matches[0].get("content_sha256")


def build_dwd_science_replay_orchestrator(
    authority: Mapping[str, Any],
) -> DurableScienceShadowOrchestrator:
    """Spawn-importable closed factory; no network or unlisted path access."""

    config = authority.get("factory_config")
    expected_keys = {
        "contract", "export_root_label", "identity_mode",
        "maximum_runtime_seconds", "metrics_manifest_sha256",
        "sequence_id", "sequence_manifest_label", "sequence_manifest_sha256",
        "source_manifest_sha256", "target_grid_sha256",
    }
    if not isinstance(config, dict) or set(config) != expected_keys:
        raise DwdScienceReplayError("DWD factory config fields differ")
    if (
        config["contract"] != DWD_SCIENCE_REPLAY_CONTRACT
        or config["sequence_manifest_sha256"]
        != EXPECTED_SEQUENCE_MANIFEST_SHA256
        or config["sequence_id"] != EXPECTED_SEQUENCE_ID
        or config["source_manifest_sha256"] != EXPECTED_SOURCE_MANIFEST_SHA256
        or config["metrics_manifest_sha256"] != EXPECTED_METRICS_MANIFEST_SHA256
        or config["target_grid_sha256"] != EXPECTED_TARGET_GRID_SHA256
    ):
        raise DwdScienceReplayError("DWD factory is not bound to definitive export")
    identity_mode = config["identity_mode"]
    if identity_mode not in {"v4_1_per_transition", "v5_multi_scan"}:
        raise DwdScienceReplayError("DWD factory identity mode differs")
    root, root_sha = _authorized_path(
        authority,
        label=config["export_root_label"],
        access="read_directory",
    )
    manifest_path, manifest_sha = _authorized_path(
        authority,
        label=config["sequence_manifest_label"],
        access="read_file",
    )
    if root_sha is not None or manifest_sha != EXPECTED_SEQUENCE_MANIFEST_SHA256:
        raise DwdScienceReplayError("DWD factory/authority content binding differs")
    export = load_dwd_science_replay_export(root, manifest_path)
    composer = DwdScienceReplayComposer(
        export,
        maximum_runtime_seconds=config["maximum_runtime_seconds"],
    )
    receipt_path, receipt_sha = _authorized_path(
        authority,
        label=authority["receipt_store_label"],
        access="transactional_file",
    )
    if receipt_sha is not None:
        raise DwdScienceReplayError("DWD receipt path must remain transactional")
    try:
        limits = ScienceShadowLimits(**dict(authority["science_limits"]))
    except (TypeError, ValueError) as exc:
        raise DwdScienceReplayError("DWD science limits are invalid") from exc
    return DurableScienceShadowOrchestrator(
        engine_id=authority["engine_id"],
        composer=composer,
        tracking_policy=dwd_science_tracking_policy(
            composer.detector_config,
            identity_mode=identity_mode,
        ),
        receipt_path=receipt_path,
        limits=limits,
    )


@dataclass(frozen=True, slots=True)
class DwdScienceReplayAuthority:
    authority_path: Path
    authority_sha256: str
    authority_document: Mapping[str, Any]
    export: DwdScienceReplayExport
    identity_mode: DwdIdentityMode
    factory_contract: ScienceShadowFactoryContract
    isolation_limits: ScienceShadowIsolationLimits
    science_limits: ScienceShadowLimits
    receipt_path: Path


def _atomic_write_new_or_identical(path: Path, body: bytes) -> None:
    if path.exists():
        try:
            existing = _read_regular_file(path, maximum_bytes=max(1, len(body)))
        except Exception as exc:
            raise DwdScienceReplayError("existing DWD authority is unsafe") from exc
        if existing != body:
            raise DwdScienceReplayError("existing DWD authority content differs")
        return
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise DwdScienceReplayError("DWD authority write did not progress")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def create_dwd_science_replay_authority(
    *,
    export_root: str | Path,
    sequence_manifest_path: str | Path,
    receipt_path: str | Path,
    authority_path: str | Path,
    identity_mode: DwdIdentityMode,
    isolation_limits: ScienceShadowIsolationLimits | None = None,
    science_limits: ScienceShadowLimits | None = None,
    maximum_runtime_seconds: float = 60.0,
) -> DwdScienceReplayAuthority:
    """Create a process authority and separate durable store for one mode."""

    if identity_mode not in {"v4_1_per_transition", "v5_multi_scan"}:
        raise DwdScienceReplayError("DWD identity mode is unsupported")
    root = Path(export_root).resolve(strict=True)
    manifest = Path(sequence_manifest_path).resolve(strict=True)
    receipt = Path(receipt_path).resolve(strict=False)
    authority_file = Path(authority_path).resolve(strict=False)
    if len({root, manifest, receipt, authority_file}) != 4:
        raise DwdScienceReplayError("DWD authority paths overlap")
    export = load_dwd_science_replay_export(root, manifest)
    receipt.parent.mkdir(parents=True, exist_ok=True)
    authority_file.parent.mkdir(parents=True, exist_ok=True)
    process_limits = isolation_limits or ScienceShadowIsolationLimits(
        wall_seconds=90.0,
        cpu_seconds=60.0,
        rss_bytes=1024 * 1024**2,
        max_authority_bytes=256 * 1024,
        max_request_bytes=32 * 1024,
        max_result_bytes=64 * 1024,
        max_authorized_paths=8,
    )
    durable_limits = science_limits or ScienceShadowLimits(
        max_receipts=128,
        checkpoint_interval_frames=20,
        max_replay_frames=64,
        max_runtime_seconds=min(60.0, float(maximum_runtime_seconds)),
    )
    factory = ScienceShadowFactoryContract(
        factory_id=DWD_SCIENCE_REPLAY_FACTORY_ID,
        target=DWD_SCIENCE_REPLAY_FACTORY_TARGET,
        opens_only_authorized_paths=True,
        hard_kill_safe=True,
        transactional_receipt_only=True,
        spawns_descendants=False,
    )
    config = {
        "contract": DWD_SCIENCE_REPLAY_CONTRACT,
        "export_root_label": "dwd_export_root",
        "identity_mode": identity_mode,
        "maximum_runtime_seconds": float(maximum_runtime_seconds),
        "metrics_manifest_sha256": EXPECTED_METRICS_MANIFEST_SHA256,
        "sequence_id": EXPECTED_SEQUENCE_ID,
        "sequence_manifest_label": "dwd_sequence_manifest",
        "sequence_manifest_sha256": EXPECTED_SEQUENCE_MANIFEST_SHA256,
        "source_manifest_sha256": EXPECTED_SOURCE_MANIFEST_SHA256,
        "target_grid_sha256": EXPECTED_TARGET_GRID_SHA256,
    }
    document: dict[str, Any] = {
        "authorized_paths": [
            {
                "access": "read_directory",
                "content_sha256": None,
                "label": "dwd_export_root",
                "path": str(root),
            },
            {
                "access": "read_file",
                "content_sha256": EXPECTED_SEQUENCE_MANIFEST_SHA256,
                "label": "dwd_sequence_manifest",
                "path": str(manifest),
            },
            {
                "access": "transactional_file",
                "content_sha256": None,
                "label": "science_receipts",
                "path": str(receipt),
            },
        ],
        "contract": SCIENCE_SHADOW_PROCESS_AUTHORITY_CONTRACT,
        "engine_id": (
            f"science-dwd-{identity_mode.replace('_', '-')}-"
            f"{EXPECTED_SEQUENCE_ID[:24]}"
        ),
        "factory_config": config,
        "factory_contract_sha256": factory.fingerprint,
        "factory_id": factory.factory_id,
        "isolation_limits": process_limits.to_mapping(),
        "receipt_store_label": "science_receipts",
        "runtime_profile_sha256": "0" * 64,
        "science_limits": durable_limits.to_mapping(),
    }
    orchestrator = build_dwd_science_replay_orchestrator(document)
    document["runtime_profile_sha256"] = orchestrator.runtime_profile_sha256
    body = _canonical_json_bytes(document)
    if len(body) > process_limits.max_authority_bytes:
        raise DwdScienceReplayError("DWD process authority exceeds byte cap")
    _atomic_write_new_or_identical(authority_file, body)
    return DwdScienceReplayAuthority(
        authority_path=authority_file,
        authority_sha256=_sha_bytes(body),
        authority_document=document,
        export=export,
        identity_mode=identity_mode,
        factory_contract=factory,
        isolation_limits=process_limits,
        science_limits=durable_limits,
        receipt_path=receipt,
    )


def recover_dwd_science_replay_state(
    authority: DwdScienceReplayAuthority,
    *,
    deadline_seconds: float = 60.0,
) -> CellTrackEngineState:
    orchestrator = build_dwd_science_replay_orchestrator(
        authority.authority_document
    )
    orchestrator._recover(time.monotonic() + deadline_seconds)
    if orchestrator._state is None:
        raise DwdScienceReplayError("durable DWD science state is absent")
    return orchestrator._state


__all__ = [
    "DWD_SCIENCE_REPLAY_BLOCKERS",
    "DWD_SCIENCE_REPLAY_CAVEATS",
    "DWD_SCIENCE_REPLAY_FACTORY_ID",
    "DWD_SCIENCE_REPLAY_FACTORY_TARGET",
    "DwdIdentityMode",
    "DwdScienceReplayAuthority",
    "DwdScienceReplayComposer",
    "DwdScienceReplayEntry",
    "DwdScienceReplayError",
    "DwdScienceReplayExport",
    "EXPECTED_FRAME_COUNT",
    "EXPECTED_METRICS_MANIFEST_SHA256",
    "EXPECTED_SEQUENCE_ID",
    "EXPECTED_SEQUENCE_MANIFEST_SHA256",
    "EXPECTED_SOURCE_MANIFEST_SHA256",
    "EXPECTED_TARGET_GRID_SHA256",
    "build_dwd_science_replay_orchestrator",
    "create_dwd_science_replay_authority",
    "dwd_science_tracking_policy",
    "load_dwd_science_replay_export",
    "recover_dwd_science_replay_state",
]
