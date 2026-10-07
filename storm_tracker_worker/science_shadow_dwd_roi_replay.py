"""Closed read-only science authority for the fixed 2-km DWD Germany ROI.

The module is offline and unwired.  It opens only one immutable 36-frame +000
evidence export, preserves physical mm/h and every missing-data category, and
feeds the existing durable/hard-isolated science orchestrator.  The two
identity modes are robustness challengers, never truth, skill or admission.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import json
import math
import os
from pathlib import Path
import time
from typing import Any, Mapping

from .analysis_grid_transform import analysis_grid_sha256
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
from .science_shadow_dwd_replay import (
    DWD_TRACKING_SOURCE,
    DwdIdentityMode,
    DwdScienceReplayEntry,
    DwdScienceReplayError,
    _atomic_write_new_or_identical,
    _authorized_path,
    _detector_mapping,
    _exact_content_addressed_path,
    _nearest_rank,
    _read_canonical_json,
    _require_exact_mapping,
    _require_sha,
    _sha_bytes,
    _sha_mapping,
)
from .science_shadow_dwd_roi_export import dwd_germany_roi_profile
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
)
from .tracking_core.engine.cell_track_engine import (
    CellTrackEnginePolicy,
    CellTrackEngineState,
)
from .tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
    detect_hierarchical_cells,
)


DWD_ROI_SCIENCE_FACTORY_ID = "science-dwd-rv-germany-roi-2km-factory-v1"
DWD_ROI_SCIENCE_FACTORY_TARGET = (
    "storm_tracker_worker.science_shadow_dwd_roi_replay:"
    "build_dwd_roi_science_replay_orchestrator"
)
DWD_ROI_SCIENCE_CONTRACT = "stv3-science-shadow-dwd-germany-roi-2km-v1"

EXPECTED_SEQUENCE_MANIFEST_SHA256 = (
    "aaa9045acb6407c3ca714563c6293f5a66f9dd284d1fb00b508be4d1d991e7dc"
)
EXPECTED_SEQUENCE_ID = (
    "1a4f4e14a407832c9763251779de63f9ff683407d7f8fde6c54c8b8a0ad74fc4"
)
EXPECTED_SOURCE_MANIFEST_SHA256 = (
    "20335e6c50b35d57dd111bea7e3670f515e933da5239ba39e74ccebc41030852"
)
EXPECTED_METRICS_MANIFEST_SHA256 = (
    "78430d02543bc3b87062c0e93db8a8cd72bd622f7a63e6d5e2360bdbdd524ccd"
)
EXPECTED_TARGET_GRID_SHA256 = (
    "cf99c2b4c08a46bde9f74f55a0b690d8496ce7b44d4a0aa379f4132023cab636"
)
EXPECTED_RADAR_SEMANTICS_CERTIFICATE_ID = (
    "dwd.rv.accumulation-to-interval-mean-rate.v1"
)
EXPECTED_RADAR_SEMANTICS_CERTIFICATE_SHA256 = (
    "a726c4a5aad6dda908d32be98aec54ab9d9459ff9597642ae5eb68e163697b62"
)
EXPECTED_DETECTOR_CONFIG_SHA256 = (
    "f83700f116b36dab5633c649a0b96b8cf685fddce3e575fb2bac61bfc658f6fe"
)
EXPECTED_PIPELINE_CONFIG_SHA256 = (
    "51dab0077b75c839e26a6634aa23327ea4df25a41b98b81d60adf77b3a64cc71"
)
EXPECTED_TRANSFORM_CONFIG_SHA256 = (
    "486877c02d3e89bc3284eba033e941a6e44569e8995b1bc4985d2471df78c303"
)
EXPECTED_FRAME_COUNT = 36
EXPECTED_ANALYSIS_PIXELS = 350 * 425
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_EVIDENCE_RECORD_BYTES = 1024 * 1024
MAX_EVIDENCE_AGGREGATE_BYTES = 32 * 1024 * 1024

DWD_ROI_SCIENCE_CAVEATS = (
    "physical_dwd_interval_mean_precipitation_rate_mm_per_hour",
    "source_aligned_two_kilometre_area_mean_changes_one_kilometre_topology",
    "provider_nodata_undetect_and_mixed_are_missing_not_dry",
    "two_kilometre_position_sigma_is_uncalibrated_half_pixel_support",
    "only_plus000_observations_are_opened",
    "forecast_leads_are_inventory_only_and_never_tracking_inputs",
    "independent_cell_identity_truth_is_unavailable",
    "independent_speed_truth_is_unavailable",
    "association_costs_and_multi_scan_weights_are_not_probabilities",
    "results_are_robustness_evidence_not_accuracy_evidence",
)
DWD_ROI_SCIENCE_BLOCKERS = (
    "independent_identity_truth_unavailable",
    "independent_speed_truth_unavailable",
    "independent_holdout_calibration_unavailable",
    "two_kilometre_aggregation_changes_object_and_core_topology",
    "mixed_source_support_is_fail_closed_withheld",
    "multi_hypothesis_identity_not_externally_validated",
    "weather_system_rebinding_forbidden",
    "operational_admission_certificates_incomplete",
    "operational_snapshot_publication_forbidden",
)


@dataclass(frozen=True, slots=True)
class DwdRoiScienceReplayExport:
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


def _validate_exact_inventory(
    root: Path,
    *,
    evidence_shas: tuple[str, ...],
) -> None:
    expected = {
        "objects": {f"{value}.json" for value in evidence_shas},
        "manifests": {
            f"{EXPECTED_SEQUENCE_MANIFEST_SHA256}.json",
            f"{EXPECTED_SOURCE_MANIFEST_SHA256}.json",
            f"{EXPECTED_METRICS_MANIFEST_SHA256}.json",
        },
    }
    for name, names in expected.items():
        directory = root / name
        if directory.is_symlink() or not directory.is_dir():
            raise DwdScienceReplayError("DWD ROI inventory directory is unsafe")
        actual = {
            path.name for path in directory.iterdir()
            if path.is_file() and not path.is_symlink()
        }
        if actual != names or len(tuple(directory.iterdir())) != len(names):
            raise DwdScienceReplayError("DWD ROI inventory differs")


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
    }, "DWD ROI process metrics")
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
        raise DwdScienceReplayError("DWD ROI process metric binding differs")
    _require_sha(manifest["isolation_authority_sha256"], "upstream authority")
    _require_sha(manifest["isolation_limits_sha256"], "upstream limits")
    raw_entries = manifest["entries"]
    if not isinstance(raw_entries, list) or len(raw_entries) != EXPECTED_FRAME_COUNT:
        raise DwdScienceReplayError("DWD ROI process metric count differs")
    metrics: list[DwdRvProcessMetric] = []
    for index, raw in enumerate(raw_entries):
        item = _require_exact_mapping(raw, {
            "child_cpu_seconds", "child_peak_boundary_bytes",
            "child_peak_rss_bytes", "elapsed_seconds", "event_id",
            "event_sha256", "frame_number", "output_sha256", "valid_at",
        }, "DWD ROI process metric")
        try:
            metric = DwdRvProcessMetric(**dict(item))
        except (TypeError, ValueError) as exc:
            raise DwdScienceReplayError("DWD ROI process metric is invalid") from exc
        if (
            metric.to_mapping() != item
            or metric.frame_number != index
            or metric.valid_at != valid_times[index]
        ):
            raise DwdScienceReplayError("DWD ROI process metric order differs")
        metrics.append(metric)
    records = tuple(metrics)
    if len({item.event_id for item in records}) != EXPECTED_FRAME_COUNT:
        raise DwdScienceReplayError("DWD ROI process event is duplicated")
    distributions = {
        "child_cpu_seconds": tuple(item.child_cpu_seconds for item in records),
        "child_peak_boundary_bytes": tuple(
            item.child_peak_boundary_bytes for item in records
        ),
        "child_peak_rss_bytes": tuple(item.child_peak_rss_bytes for item in records),
        "elapsed_seconds": tuple(item.elapsed_seconds for item in records),
    }
    caps = (
        manifest["configured_boundary_memory_cap_bytes"],
        manifest["configured_cpu_cap_seconds"],
        manifest["configured_wall_cap_seconds"],
    )
    if (
        caps[0] != 1_073_741_824
        or float(caps[1]) != 120.0
        or float(caps[2]) != 180.0
        or caps[0] <= max(distributions["child_peak_boundary_bytes"])
        or float(caps[1]) <= max(distributions["child_cpu_seconds"])
        or float(caps[2]) <= max(distributions["elapsed_seconds"])
    ):
        raise DwdScienceReplayError("DWD ROI upstream resource evidence differs")
    aggregates = _require_exact_mapping(
        manifest["aggregates"], set(distributions), "DWD ROI aggregates"
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
            raise DwdScienceReplayError("DWD ROI metric aggregate differs")
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
        or evidence.evidence_candidate_id != sequence_entry["evidence_candidate_id"]
        or evidence.evidence_content_sha256
        != sequence_entry["evidence_content_sha256"]
        or frame.frame_id != sequence_entry["normalized_frame_id"]
        or frame.valid_at != sequence_entry["valid_at"]
        or frame.quantity != SEQUENCE_QUANTITY
        or frame.unit != SEQUENCE_UNIT
        or analysis_grid_sha256(frame.grid) != EXPECTED_TARGET_GRID_SHA256
        or evidence.target_grid_sha256 != EXPECTED_TARGET_GRID_SHA256
    ):
        raise DwdScienceReplayError("DWD ROI evidence binding differs")
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
        or "hard_process_isolation_not_wired"
        not in evidence.operational_admission_blockers
    ):
        raise DwdScienceReplayError("DWD ROI physical semantics differ")
    if (
        completion["event_id"] != process_metric.event_id
        or completion["event_sha256"] != process_metric.event_sha256
        or completion["output_sha256"] != process_metric.output_sha256
    ):
        raise DwdScienceReplayError("DWD ROI completion lineage differs")
    flattened = tuple(value for row in frame.values for value in row)
    finite = tuple(value for value in flattened if value is not None)
    if any(not math.isfinite(float(value)) or float(value) < 0.0 for value in finite):
        raise DwdScienceReplayError("DWD ROI physical rate is invalid")
    accounting = evidence.pixel_accounting
    shield = sum(value >= detector.shield_threshold for value in finite)
    core = sum(value >= detector.core_threshold for value in finite)
    excluded = sum((
        accounting.excluded_nodata_pixels,
        accounting.excluded_undetect_pixels,
        accounting.excluded_mixed_pixels,
        accounting.excluded_outside_support_pixels,
    ))
    if (
        len(flattened) != EXPECTED_ANALYSIS_PIXELS
        or accounting.total_analysis_pixels != EXPECTED_ANALYSIS_PIXELS
        or accounting.valid_detector_input_pixels != len(finite)
        or accounting.valid_below_shield_threshold_pixels != len(finite) - shield
        or accounting.rain_shield_pixels != shield
        or accounting.embedded_core_pixels != core
        or excluded != EXPECTED_ANALYSIS_PIXELS - len(finite)
    ):
        raise DwdScienceReplayError("DWD ROI pixel accounting is not lossless")
    detected = detect_hierarchical_cells(frame, detector)
    if (
        detected != evidence.detection_frame
        or accounting.detection_objects != len(detected.detections)
        or accounting.rain_shield_objects != len(detected.rain_shields)
        or accounting.embedded_core_objects != len(detected.embedded_cores)
        or accounting.low_confidence_objects
        != len(detected.low_confidence_fragments)
    ):
        raise DwdScienceReplayError("DWD ROI detector replay differs")
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


def load_dwd_roi_science_replay_export(
    export_root: str | Path,
    sequence_manifest_path: str | Path,
) -> DwdRoiScienceReplayExport:
    """Verify every byte-bound object in the immutable 36-frame ROI export."""

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
        raise DwdScienceReplayError("DWD ROI paths are absent, relative or unsafe")
    root = root.resolve(strict=True)
    manifest_path = manifest_path.resolve(strict=True)
    expected_manifest = _exact_content_addressed_path(
        root, "manifests", EXPECTED_SEQUENCE_MANIFEST_SHA256
    )
    if manifest_path != expected_manifest:
        raise DwdScienceReplayError("DWD ROI manifest path is not definitive")
    try:
        sequence = load_sequence_manifest(
            manifest_path,
            expected_sha256=EXPECTED_SEQUENCE_MANIFEST_SHA256,
        )
    except DwdRvScienceSequenceIntegrityError as exc:
        raise DwdScienceReplayError("DWD ROI sequence failed validation") from exc
    if (
        sequence["sequence_id"] != EXPECTED_SEQUENCE_ID
        or sequence["source_manifest_sha256"] != EXPECTED_SOURCE_MANIFEST_SHA256
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
        raise DwdScienceReplayError("DWD ROI sequence identity differs")
    raw_entries = tuple(sequence["entries"])
    valid_times = tuple(float(item["valid_at"]) for item in raw_entries)
    source_path = _exact_content_addressed_path(
        root, "manifests", EXPECTED_SOURCE_MANIFEST_SHA256
    )
    source_body, _ = _read_canonical_json(
        source_path,
        expected_sha256=EXPECTED_SOURCE_MANIFEST_SHA256,
        maximum_bytes=MAX_MANIFEST_BYTES,
    )
    if len(source_body) != sequence["source_manifest_bytes"]:
        raise DwdScienceReplayError("DWD ROI source byte binding differs")
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
        raise DwdScienceReplayError("DWD ROI source failed validation") from exc
    metrics_path = _exact_content_addressed_path(
        root, "manifests", EXPECTED_METRICS_MANIFEST_SHA256
    )
    metrics_body, metrics_manifest, metrics = _load_metrics_manifest(
        metrics_path, valid_times=valid_times
    )
    evidence_shas = tuple(item["evidence_record_sha256"] for item in raw_entries)
    if len(set(evidence_shas)) != EXPECTED_FRAME_COUNT:
        raise DwdScienceReplayError("DWD ROI evidence identity is duplicated")
    _validate_exact_inventory(root, evidence_shas=evidence_shas)
    profile = dwd_germany_roi_profile()
    if (
        analysis_grid_sha256(profile.target_grid) != EXPECTED_TARGET_GRID_SHA256
        or _sha_mapping(_detector_mapping(profile.detector_config))
        != EXPECTED_DETECTOR_CONFIG_SHA256
        or profile.pipeline_config.fingerprint != EXPECTED_PIPELINE_CONFIG_SHA256
        or profile.transform_config.fingerprint != EXPECTED_TRANSFORM_CONFIG_SHA256
    ):
        raise DwdScienceReplayError("local DWD ROI profile differs from export")
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
            raise DwdScienceReplayError("DWD ROI evidence byte bound differs")
        try:
            evidence, completion = load_evidence_record(
                record_path,
                expected_sha256=raw["evidence_record_sha256"],
            )
        except DwdRvScienceSequenceIntegrityError as exc:
            raise DwdScienceReplayError("DWD ROI evidence failed validation") from exc
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
        raise DwdScienceReplayError("DWD ROI order or cadence differs")
    return DwdRoiScienceReplayExport(
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


def load_dwd_roi_science_evidence(
    export: DwdRoiScienceReplayExport,
    entry: DwdScienceReplayEntry,
) -> ShadowEvidenceFrame:
    """Reopen one preflighted evidence object and recheck immutable identity."""

    if not isinstance(export, DwdRoiScienceReplayExport) or entry not in export.entries:
        raise DwdScienceReplayError("DWD ROI entry is outside the closed export")
    path = _exact_content_addressed_path(
        export.export_root, "objects", entry.evidence_record_sha256
    )
    try:
        evidence, completion = load_evidence_record(
            path, expected_sha256=entry.evidence_record_sha256
        )
    except DwdRvScienceSequenceIntegrityError as exc:
        raise DwdScienceReplayError("DWD ROI evidence changed after preflight") from exc
    if (
        evidence.canonical_run_id != entry.canonical_run_id
        or evidence.evidence_candidate_id != entry.evidence_candidate_id
        or evidence.evidence_content_sha256 != entry.evidence_content_sha256
        or evidence.normalized_frame.frame_id != entry.normalized_frame_id
        or completion["event_id"] != entry.completion_event_id
        or completion["event_sha256"] != entry.completion_event_sha256
        or completion["output_sha256"] != entry.completion_output_sha256
    ):
        raise DwdScienceReplayError("DWD ROI evidence lineage changed")
    return evidence


def dwd_roi_science_tracking_policy(
    detector: HierarchicalDetectorConfig,
    *,
    identity_mode: DwdIdentityMode,
) -> CellTrackEnginePolicy:
    """Keep identical ungated soft costs; vary only multi-scan memory."""

    if identity_mode not in {"v4_1_per_transition", "v5_multi_scan"}:
        raise DwdScienceReplayError("DWD ROI identity mode is unsupported")
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


class DwdRoiScienceReplayComposer:
    """On-demand immutable reader with stable provider/product tracking alias."""

    def __init__(
        self,
        export: DwdRoiScienceReplayExport,
        *,
        maximum_runtime_seconds: float,
    ) -> None:
        if not isinstance(export, DwdRoiScienceReplayExport):
            raise TypeError("export must be DwdRoiScienceReplayExport")
        if (
            isinstance(maximum_runtime_seconds, bool)
            or not isinstance(maximum_runtime_seconds, (int, float))
            or not math.isfinite(float(maximum_runtime_seconds))
            or not 0.05 <= float(maximum_runtime_seconds) <= 300.0
        ):
            raise ValueError("DWD ROI composer runtime bound is invalid")
        profile = dwd_germany_roi_profile()
        self._export = export
        self._detector = profile.detector_config
        self._maximum_runtime_seconds = float(maximum_runtime_seconds)
        self._entries = {item.canonical_run_id: item for item in export.entries}
        self._profile_sha256 = _sha_mapping({
            "algorithm": "stv3-science-shadow-dwd-germany-roi-composer-v1",
            "caveats": list(DWD_ROI_SCIENCE_CAVEATS),
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

    def _tracking_evidence(self, upstream: ShadowEvidenceFrame) -> ShadowEvidenceFrame:
        upstream_frame = upstream.normalized_frame
        frame_sha = _sha_mapping({
            "contract": "stv3-dwd-roi-tracking-source-alias-frame-v1",
            "stable_source": DWD_TRACKING_SOURCE,
            "upstream_frame_id": upstream_frame.frame_id,
            "upstream_source": upstream_frame.source,
        })
        frame = replace(
            upstream_frame,
            frame_id="shadow-frame-" + frame_sha,
            source=DWD_TRACKING_SOURCE,
        )
        detection = detect_hierarchical_cells(frame, self._detector)
        if len(detection.detections) != upstream.pixel_accounting.detection_objects:
            raise DwdScienceReplayError("DWD ROI alias changed detector accounting")
        analysis_sha = _sha_mapping({
            "contract": "stv3-dwd-roi-tracking-source-alias-analysis-v1",
            "stable_frame_id": frame.frame_id,
            "stable_source": frame.source,
            "upstream_analysis_field_content_sha256": (
                upstream.analysis_field_content_sha256
            ),
            "upstream_frame_id": upstream_frame.frame_id,
            "upstream_source": upstream_frame.source,
        })
        lineage_sha = _sha_mapping({
            "contract": "stv3-dwd-roi-tracking-source-alias-lineage-v1",
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
            "contract": "stv3-dwd-roi-tracking-source-alias-evidence-v1",
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
                + DWD_ROI_SCIENCE_BLOCKERS
            ))),
        )

    def compose(self, *, run_id: str, deadline_at: float, monotonic) -> ShadowCompositionResult:
        if float(monotonic()) >= deadline_at:
            from .evidence_to_cells_shadow import EvidenceToCellsShadowDeadlineError
            raise EvidenceToCellsShadowDeadlineError("DWD ROI deadline expired")
        entry = self._entries.get(run_id)
        if entry is None:
            raise DwdScienceReplayError("run is absent from definitive DWD ROI export")
        upstream = load_dwd_roi_science_evidence(self._export, entry)
        if (
            upstream.normalized_frame.valid_at != entry.valid_at
            or upstream.pixel_accounting.valid_detector_input_pixels
            != entry.finite_pixels
            or upstream.pixel_accounting.detection_objects != entry.detection_objects
            or detect_hierarchical_cells(upstream.normalized_frame, self._detector)
            != upstream.detection_frame
        ):
            raise DwdScienceReplayError("DWD ROI object changed after preflight")
        evidence = self._tracking_evidence(upstream)
        if float(monotonic()) >= deadline_at:
            from .evidence_to_cells_shadow import EvidenceToCellsShadowDeadlineError
            raise EvidenceToCellsShadowDeadlineError("DWD ROI deadline expired")
        return ShadowCompositionResult(run_id=run_id, evidence=evidence, issues=())


def build_dwd_roi_science_replay_orchestrator(
    authority: Mapping[str, Any],
) -> DurableScienceShadowOrchestrator:
    """Spawn-importable factory restricted to three authorized local paths."""

    config = authority.get("factory_config")
    expected_keys = {
        "contract", "export_root_label", "identity_mode",
        "maximum_runtime_seconds", "metrics_manifest_sha256", "sequence_id",
        "sequence_manifest_label", "sequence_manifest_sha256",
        "source_manifest_sha256", "target_grid_sha256",
    }
    if not isinstance(config, dict) or set(config) != expected_keys:
        raise DwdScienceReplayError("DWD ROI factory config differs")
    if (
        config["contract"] != DWD_ROI_SCIENCE_CONTRACT
        or config["sequence_manifest_sha256"]
        != EXPECTED_SEQUENCE_MANIFEST_SHA256
        or config["sequence_id"] != EXPECTED_SEQUENCE_ID
        or config["source_manifest_sha256"] != EXPECTED_SOURCE_MANIFEST_SHA256
        or config["metrics_manifest_sha256"] != EXPECTED_METRICS_MANIFEST_SHA256
        or config["target_grid_sha256"] != EXPECTED_TARGET_GRID_SHA256
    ):
        raise DwdScienceReplayError("DWD ROI factory export binding differs")
    identity_mode = config["identity_mode"]
    if identity_mode not in {"v4_1_per_transition", "v5_multi_scan"}:
        raise DwdScienceReplayError("DWD ROI factory identity mode differs")
    root, root_sha = _authorized_path(
        authority,
        label=config["export_root_label"],
        access="read_directory",
    )
    manifest, manifest_sha = _authorized_path(
        authority,
        label=config["sequence_manifest_label"],
        access="read_file",
    )
    if root_sha is not None or manifest_sha != EXPECTED_SEQUENCE_MANIFEST_SHA256:
        raise DwdScienceReplayError("DWD ROI path authority differs")
    export = load_dwd_roi_science_replay_export(root, manifest)
    composer = DwdRoiScienceReplayComposer(
        export,
        maximum_runtime_seconds=config["maximum_runtime_seconds"],
    )
    receipt_path, receipt_sha = _authorized_path(
        authority,
        label=authority["receipt_store_label"],
        access="transactional_file",
    )
    if receipt_sha is not None:
        raise DwdScienceReplayError("DWD ROI receipt store cannot be immutable")
    try:
        limits = ScienceShadowLimits(**dict(authority["science_limits"]))
    except (TypeError, ValueError) as exc:
        raise DwdScienceReplayError("DWD ROI science limits are invalid") from exc
    return DurableScienceShadowOrchestrator(
        engine_id=authority["engine_id"],
        composer=composer,
        tracking_policy=dwd_roi_science_tracking_policy(
            composer.detector_config,
            identity_mode=identity_mode,
        ),
        receipt_path=receipt_path,
        limits=limits,
    )


@dataclass(frozen=True, slots=True)
class DwdRoiScienceReplayAuthority:
    authority_path: Path
    authority_sha256: str
    authority_document: Mapping[str, Any]
    export: DwdRoiScienceReplayExport
    identity_mode: DwdIdentityMode
    factory_contract: ScienceShadowFactoryContract
    isolation_limits: ScienceShadowIsolationLimits
    science_limits: ScienceShadowLimits
    receipt_path: Path


def create_dwd_roi_science_replay_authority(
    *,
    export_root: str | Path,
    sequence_manifest_path: str | Path,
    receipt_path: str | Path,
    authority_path: str | Path,
    identity_mode: DwdIdentityMode,
    isolation_limits: ScienceShadowIsolationLimits | None = None,
    science_limits: ScienceShadowLimits | None = None,
    maximum_runtime_seconds: float = 60.0,
) -> DwdRoiScienceReplayAuthority:
    """Create one mode-specific authority and separate durable receipt store."""

    if identity_mode not in {"v4_1_per_transition", "v5_multi_scan"}:
        raise DwdScienceReplayError("DWD ROI identity mode is unsupported")
    root = Path(export_root).resolve(strict=True)
    manifest = Path(sequence_manifest_path).resolve(strict=True)
    receipt = Path(receipt_path).resolve(strict=False)
    authority_file = Path(authority_path).resolve(strict=False)
    if len({root, manifest, receipt, authority_file}) != 4:
        raise DwdScienceReplayError("DWD ROI authority paths overlap")
    export = load_dwd_roi_science_replay_export(root, manifest)
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
        factory_id=DWD_ROI_SCIENCE_FACTORY_ID,
        target=DWD_ROI_SCIENCE_FACTORY_TARGET,
        opens_only_authorized_paths=True,
        hard_kill_safe=True,
        transactional_receipt_only=True,
        spawns_descendants=False,
    )
    config = {
        "contract": DWD_ROI_SCIENCE_CONTRACT,
        "export_root_label": "dwd_roi_export_root",
        "identity_mode": identity_mode,
        "maximum_runtime_seconds": float(maximum_runtime_seconds),
        "metrics_manifest_sha256": EXPECTED_METRICS_MANIFEST_SHA256,
        "sequence_id": EXPECTED_SEQUENCE_ID,
        "sequence_manifest_label": "dwd_roi_sequence_manifest",
        "sequence_manifest_sha256": EXPECTED_SEQUENCE_MANIFEST_SHA256,
        "source_manifest_sha256": EXPECTED_SOURCE_MANIFEST_SHA256,
        "target_grid_sha256": EXPECTED_TARGET_GRID_SHA256,
    }
    document: dict[str, Any] = {
        "authorized_paths": [
            {
                "access": "read_directory",
                "content_sha256": None,
                "label": "dwd_roi_export_root",
                "path": str(root),
            },
            {
                "access": "read_file",
                "content_sha256": EXPECTED_SEQUENCE_MANIFEST_SHA256,
                "label": "dwd_roi_sequence_manifest",
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
            f"science-dwd-roi-{identity_mode.replace('_', '-')}-"
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
    orchestrator = build_dwd_roi_science_replay_orchestrator(document)
    document["runtime_profile_sha256"] = orchestrator.runtime_profile_sha256
    body = _canonical_json_bytes(document)
    if len(body) > process_limits.max_authority_bytes:
        raise DwdScienceReplayError("DWD ROI authority exceeds byte cap")
    _atomic_write_new_or_identical(authority_file, body)
    return DwdRoiScienceReplayAuthority(
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


def recover_dwd_roi_science_replay_state(
    authority: DwdRoiScienceReplayAuthority,
    *,
    deadline_seconds: float = 60.0,
) -> CellTrackEngineState:
    orchestrator = build_dwd_roi_science_replay_orchestrator(
        authority.authority_document
    )
    orchestrator._recover(time.monotonic() + deadline_seconds)
    if orchestrator._state is None:
        raise DwdScienceReplayError("durable DWD ROI science state is absent")
    return orchestrator._state


__all__ = [
    "DWD_ROI_SCIENCE_BLOCKERS",
    "DWD_ROI_SCIENCE_CAVEATS",
    "DWD_ROI_SCIENCE_FACTORY_ID",
    "DWD_ROI_SCIENCE_FACTORY_TARGET",
    "DwdRoiScienceReplayAuthority",
    "DwdRoiScienceReplayComposer",
    "DwdRoiScienceReplayExport",
    "EXPECTED_FRAME_COUNT",
    "EXPECTED_METRICS_MANIFEST_SHA256",
    "EXPECTED_SEQUENCE_ID",
    "EXPECTED_SEQUENCE_MANIFEST_SHA256",
    "EXPECTED_SOURCE_MANIFEST_SHA256",
    "EXPECTED_TARGET_GRID_SHA256",
    "build_dwd_roi_science_replay_orchestrator",
    "create_dwd_roi_science_replay_authority",
    "dwd_roi_science_tracking_policy",
    "load_dwd_roi_science_evidence",
    "load_dwd_roi_science_replay_export",
    "recover_dwd_roi_science_replay_state",
]
