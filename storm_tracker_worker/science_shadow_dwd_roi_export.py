"""Offline-only authority helpers for the predeclared DWD Germany ROI.

The module contains no acquisition loop, service, MQTT, Home Assistant or live
admission wiring.  It supplies one fixed profile to the existing inert DWD
observation replay, seals why that ROI was chosen before tracking, and derives
a lower replay cap only from one explicitly measured calibration frame.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
from typing import Any, Mapping

from .analysis_grid_transform import analysis_grid_sha256
from .dwd_rv_completion_consumer import DwdRvShadowChainProfile
from .dwd_rv_observation_replay import (
    DWD_PROJDEF,
    default_belgium_observation_profile,
)
from .hard_decoder_isolation import _canonical_json_bytes
from .tracking_core.engine.analysis_grid import AnalysisGrid


DWD_GERMANY_ROI_GRID_ID = "dwd-rv-union-support-observation-2km-v1"
DWD_GERMANY_ROI_SELECTION_CONTRACT = "stv3-dwd-rv-germany-roi-selection-v1"
DWD_GERMANY_ROI_CALIBRATION_CONTRACT = "stv3-dwd-rv-germany-roi-calibration-v1"
DWD_GERMANY_ROI_RUN_CONTRACT = "stv3-dwd-rv-germany-roi-run-v1"

SOURCE_MANIFEST_SHA256 = (
    "20335e6c50b35d57dd111bea7e3670f515e933da5239ba39e74ccebc41030852"
)
SOURCE_FRAME_COUNT = 36
SOURCE_FIRST_VALID_AT = 1_786_079_400.0
SOURCE_LAST_VALID_AT = 1_786_089_900.0
SOURCE_CADENCE_SECONDS = 300
CALIBRATION_ARCHIVE_NAME = "composite_rv_20260807_0510.tar"
CALIBRATION_ARCHIVE_BYTES = 1_126_400
CALIBRATION_ARCHIVE_SHA256 = (
    "956a3feac1298e57457beb3186b69e0e8c565080adf9eb3f56075380f6332977"
)
CALIBRATION_SOURCE_MANIFEST_SHA256 = (
    "0b88691f70a895714f53901edaf964be72fc42c52216dd55119ecbb08d29b9b4"
)
CALIBRATION_ISOLATION_LIMITS_SHA256 = (
    "b6607e65521db7c9bda92a18c9dfd714daefaecf62dd610c1fa6b5921a0ede90"
)

UNION_THRESHOLD_MM_H = 1.0
UNION_SOURCE_ROW_MIN = 185
UNION_SOURCE_ROW_MAX = 981
UNION_SOURCE_COLUMN_MIN = 178
UNION_SOURCE_COLUMN_MAX = 838

ROI_SOURCE_ROW_MIN = 150
ROI_SOURCE_ROW_MAX = 999
ROI_SOURCE_COLUMN_MIN = 150
ROI_SOURCE_COLUMN_MAX = 849
ROI_SOURCE_WIDTH = ROI_SOURCE_COLUMN_MAX - ROI_SOURCE_COLUMN_MIN + 1
ROI_SOURCE_HEIGHT = ROI_SOURCE_ROW_MAX - ROI_SOURCE_ROW_MIN + 1
# AnalysisGrid origins are north-west *outer edges*.  Retaining the source
# edge (-500 + 150*1000, 500 - 150*1000) lets each 2-km target pixel cover
# exactly one 2x2 source-pixel block, with no half-pixel shift or interpolation.
ROI_ORIGIN_X_M = 149_500.0
ROI_ORIGIN_Y_M = -149_500.0
ROI_RESOLUTION_M = 2_000.0
ROI_WIDTH = 350
ROI_HEIGHT = 425
ROI_PIXELS = ROI_WIDTH * ROI_HEIGHT

CALIBRATION_RSS_CAP_BYTES = 1_536 * 1024 * 1024
CAP_MARGIN_BYTES = 256 * 1024 * 1024
CAP_QUANTUM_BYTES = 64 * 1024 * 1024

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class DwdGermanyRoiError(RuntimeError):
    """A fixed ROI, calibration or content-addressed manifest is invalid."""


@dataclass(frozen=True, slots=True)
class DwdGermanyRoiCalibrationSource:
    archive_name: str
    archive_bytes: int
    archive_sha256: str


@dataclass(frozen=True, slots=True)
class DwdGermanyRoiFullReplayAuthority:
    selection_manifest_sha256: str
    calibration_manifest_sha256: str
    replay_rss_cap_bytes: int
    profile_sha256: str


def verify_dwd_germany_roi_calibration_source(
    source_directory: str | Path,
) -> DwdGermanyRoiCalibrationSource:
    """Verify the exact immutable first-frame calibration corpus."""

    source = Path(source_directory)
    if not source.is_absolute() or source.is_symlink() or not source.is_dir():
        raise DwdGermanyRoiError("calibration source directory is unsafe")
    if source.resolve(strict=True) != source:
        raise DwdGermanyRoiError("calibration source directory is not normalized")
    entries = tuple(source.iterdir())
    if len(entries) != 1:
        raise DwdGermanyRoiError("calibration source is not the exact one-frame set")
    archive = entries[0]
    if (
        archive.name != CALIBRATION_ARCHIVE_NAME
        or archive.is_symlink()
        or not archive.is_file()
    ):
        raise DwdGermanyRoiError("calibration archive identity differs")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(archive, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size != CALIBRATION_ARCHIVE_BYTES:
            raise DwdGermanyRoiError("calibration archive size differs")
        digest = hashlib.sha256()
        total = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, CALIBRATION_ARCHIVE_BYTES + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > CALIBRATION_ARCHIVE_BYTES:
                raise DwdGermanyRoiError("calibration archive exceeds byte bound")
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        total != CALIBRATION_ARCHIVE_BYTES
        or digest.hexdigest() != CALIBRATION_ARCHIVE_SHA256
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
    ):
        raise DwdGermanyRoiError("calibration archive bytes differ")
    return DwdGermanyRoiCalibrationSource(
        archive_name=CALIBRATION_ARCHIVE_NAME,
        archive_bytes=CALIBRATION_ARCHIVE_BYTES,
        archive_sha256=CALIBRATION_ARCHIVE_SHA256,
    )


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_mapping(value: Any) -> str:
    return _sha_bytes(_canonical_json_bytes(value))


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
        raise DwdGermanyRoiError(f"{label} is not a lowercase SHA-256")
    return value


def _exact(value: object, keys: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise DwdGermanyRoiError(f"{label} fields are not exact")
    return value


def dwd_germany_roi_profile() -> DwdRvShadowChainProfile:
    """Return the predeclared source-aligned 2-km aggregate ROI profile."""

    base = default_belgium_observation_profile()
    return DwdRvShadowChainProfile(
        target_grid=AnalysisGrid(
            grid_id=DWD_GERMANY_ROI_GRID_ID,
            crs=DWD_PROJDEF,
            origin_x_m=ROI_ORIGIN_X_M,
            origin_y_m=ROI_ORIGIN_Y_M,
            x_resolution_m=ROI_RESOLUTION_M,
            y_resolution_m=ROI_RESOLUTION_M,
            width=ROI_WIDTH,
            height=ROI_HEIGHT,
        ),
        transform_config=replace(
            base.transform_config,
            max_target_pixels=ROI_PIXELS,
        ),
        # These uncertainty/area floors follow solely from the predeclared
        # 2-km sampling support, not from this episode's tracker output.
        detector_config=replace(
            base.detector_config,
            minimum_shield_area_m2=4_000_000.0,
            minimum_core_area_m2=4_000_000.0,
            shield_position_sigma_m=1_000.0,
            core_position_sigma_m=1_000.0,
        ),
        pipeline_config=replace(
            base.pipeline_config,
            max_target_pixels=ROI_PIXELS,
            max_detection_objects=ROI_PIXELS * 2,
            max_runtime_seconds=120.0,
        ),
    )


@dataclass(frozen=True, slots=True)
class DwdGermanyRoiManifestReceipt:
    contract: str
    manifest_sha256: str
    manifest_bytes: int
    manifest_path: Path
    document: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class DwdGermanyRoiCapDerivation:
    calibration_cap_bytes: int
    observed_peak_boundary_bytes: int
    observed_peak_rss_bytes: int
    safety_margin_bytes: int
    rounding_quantum_bytes: int
    derived_replay_cap_bytes: int

    def to_mapping(self) -> dict[str, int | str]:
        return {
            "calibration_cap_bytes": self.calibration_cap_bytes,
            "derivation": "ceil((peak_boundary+256MiB)/64MiB)*64MiB",
            "derived_replay_cap_bytes": self.derived_replay_cap_bytes,
            "observed_peak_boundary_bytes": self.observed_peak_boundary_bytes,
            "observed_peak_rss_bytes": self.observed_peak_rss_bytes,
            "rounding_quantum_bytes": self.rounding_quantum_bytes,
            "safety_margin_bytes": self.safety_margin_bytes,
        }


def derive_dwd_germany_roi_replay_cap(
    *,
    observed_peak_boundary_bytes: int,
    observed_peak_rss_bytes: int,
    active_children_after_run: int,
    calibration_cap_bytes: int = CALIBRATION_RSS_CAP_BYTES,
) -> DwdGermanyRoiCapDerivation:
    """Derive a lower cap with fixed 256 MiB margin and 64 MiB rounding."""

    for label, value in (
        ("calibration cap", calibration_cap_bytes),
        ("peak boundary", observed_peak_boundary_bytes),
        ("peak RSS", observed_peak_rss_bytes),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise DwdGermanyRoiError(f"{label} is invalid")
    if active_children_after_run != 0:
        raise DwdGermanyRoiError("calibration left an active decoder child")
    if (
        observed_peak_rss_bytes > observed_peak_boundary_bytes
        or observed_peak_boundary_bytes >= calibration_cap_bytes
    ):
        raise DwdGermanyRoiError("calibration did not remain below its hard cap")
    required = observed_peak_boundary_bytes + CAP_MARGIN_BYTES
    derived = math.ceil(required / CAP_QUANTUM_BYTES) * CAP_QUANTUM_BYTES
    if derived >= calibration_cap_bytes:
        raise DwdGermanyRoiError(
            "fixed safety margin cannot produce a lower replay cap"
        )
    return DwdGermanyRoiCapDerivation(
        calibration_cap_bytes=calibration_cap_bytes,
        observed_peak_boundary_bytes=observed_peak_boundary_bytes,
        observed_peak_rss_bytes=observed_peak_rss_bytes,
        safety_margin_bytes=CAP_MARGIN_BYTES,
        rounding_quantum_bytes=CAP_QUANTUM_BYTES,
        derived_replay_cap_bytes=derived,
    )


def _write_content_addressed(
    root: Path,
    document: Mapping[str, Any],
) -> DwdGermanyRoiManifestReceipt:
    if not root.is_absolute():
        raise DwdGermanyRoiError("manifest root must be absolute")
    root.mkdir(parents=True, exist_ok=True)
    manifests = root / "manifests"
    manifests.mkdir(exist_ok=True)
    if root.is_symlink() or manifests.is_symlink():
        raise DwdGermanyRoiError("manifest root is unsafe")
    body = _canonical_json_bytes(document)
    if not 1 <= len(body) <= 1024 * 1024:
        raise DwdGermanyRoiError("ROI manifest exceeds byte cap")
    digest = _sha_bytes(body)
    path = manifests / f"{digest}.json"
    if path.exists():
        if path.is_symlink() or path.read_bytes() != body:
            raise DwdGermanyRoiError("content-addressed manifest conflicts")
    else:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
        descriptor = os.open(path, flags, 0o600)
        try:
            view = memoryview(body)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise DwdGermanyRoiError("manifest write did not progress")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return DwdGermanyRoiManifestReceipt(
        contract=str(document["contract"]),
        manifest_sha256=digest,
        manifest_bytes=len(body),
        manifest_path=path,
        document=document,
    )


def seal_dwd_germany_roi_selection_manifest(
    root: str | Path,
) -> DwdGermanyRoiManifestReceipt:
    """Seal the selection fixed before any identity tracking is executed."""

    profile = dwd_germany_roi_profile()
    identity = {
        "cadence_seconds": SOURCE_CADENCE_SECONDS,
        "contract": DWD_GERMANY_ROI_SELECTION_CONTRACT,
        "frame_count": SOURCE_FRAME_COUNT,
        "full_domain_union_bbox": {
            "column_max_inclusive": UNION_SOURCE_COLUMN_MAX,
            "column_min_inclusive": UNION_SOURCE_COLUMN_MIN,
            "row_max_inclusive": UNION_SOURCE_ROW_MAX,
            "row_min_inclusive": UNION_SOURCE_ROW_MIN,
            "threshold_mm_h": UNION_THRESHOLD_MM_H,
        },
        "lead_seconds": 0,
        "profile_sha256": profile.fingerprint,
        "roi_grid_sha256": analysis_grid_sha256(profile.target_grid),
        "roi_source_edges": {
            "column_max_inclusive": ROI_SOURCE_COLUMN_MAX,
            "column_min_inclusive": ROI_SOURCE_COLUMN_MIN,
            "row_max_inclusive": ROI_SOURCE_ROW_MAX,
            "row_min_inclusive": ROI_SOURCE_ROW_MIN,
        },
        "spatial_aggregation": {
            "area_weighted": True,
            "source_resolution_m": 1_000,
            "source_window_height": ROI_SOURCE_HEIGHT,
            "source_window_width": ROI_SOURCE_WIDTH,
            "target_resolution_m": int(ROI_RESOLUTION_M),
            "target_window_height": ROI_HEIGHT,
            "target_window_width": ROI_WIDTH,
            "target_pixels": ROI_PIXELS,
            "truth_or_accuracy_claim": False,
        },
        "selection_method": (
            "union_of_plus000_pixels_ge_1mmh_rounded_outward_to_50km_edges"
        ),
        "source_manifest_sha256": SOURCE_MANIFEST_SHA256,
        "source_valid_at": {
            "first": SOURCE_FIRST_VALID_AT,
            "last": SOURCE_LAST_VALID_AT,
        },
        "tracker_outputs_observed_during_selection": False,
        "tracker_parameters_consulted_during_selection": False,
    }
    document = {
        **identity,
        "selection_id": _sha_mapping(identity),
        "weather_system_rebinding_permitted": False,
    }
    return _write_content_addressed(Path(root).resolve(strict=False), document)


def load_dwd_germany_roi_manifest(
    path: str | Path,
    *,
    expected_sha256: str,
    expected_contract: str,
) -> Mapping[str, Any]:
    target = Path(path)
    if (
        not target.is_absolute()
        or target.is_symlink()
        or not target.is_file()
        or target.name != f"{_sha(expected_sha256, 'manifest SHA-256')}.json"
    ):
        raise DwdGermanyRoiError("ROI manifest path is unsafe or not content-addressed")
    body = target.read_bytes()
    if not 1 <= len(body) <= 1024 * 1024 or _sha_bytes(body) != expected_sha256:
        raise DwdGermanyRoiError("ROI manifest bytes differ")
    try:
        value = json.loads(body.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise DwdGermanyRoiError("ROI manifest is not canonical JSON") from exc
    if (
        not isinstance(value, dict)
        or _canonical_json_bytes(value) != body
        or value.get("contract") != expected_contract
    ):
        raise DwdGermanyRoiError("ROI manifest contract differs")
    return value


def authorize_dwd_germany_roi_full_replay(
    *,
    selection: DwdGermanyRoiManifestReceipt,
    calibration_manifest_path: str | Path,
    expected_calibration_sha256: str,
) -> DwdGermanyRoiFullReplayAuthority:
    """Re-derive the one-frame cap before permitting an offline 36-frame run."""

    if (
        not isinstance(selection, DwdGermanyRoiManifestReceipt)
        or selection.contract != DWD_GERMANY_ROI_SELECTION_CONTRACT
        or selection.document.get("profile_sha256")
        != dwd_germany_roi_profile().fingerprint
        or selection.document.get("source_manifest_sha256")
        != SOURCE_MANIFEST_SHA256
    ):
        raise DwdGermanyRoiError("selection authority differs")
    document = _exact(load_dwd_germany_roi_manifest(
        calibration_manifest_path,
        expected_sha256=expected_calibration_sha256,
        expected_contract=DWD_GERMANY_ROI_CALIBRATION_CONTRACT,
    ), {
        "active_decoder_children_after_run", "calibration",
        "calibration_source_archive_sha256", "cap_derivation", "contract",
        "full_replay_authorized_by_calibration", "nuc_or_live_admission_granted",
        "selection_manifest_sha256",
    }, "calibration manifest")
    if (
        document["active_decoder_children_after_run"] != 0
        or document["calibration_source_archive_sha256"]
        != CALIBRATION_ARCHIVE_SHA256
        or document["full_replay_authorized_by_calibration"] is not True
        or document["nuc_or_live_admission_granted"] is not False
        or document["selection_manifest_sha256"] != selection.manifest_sha256
    ):
        raise DwdGermanyRoiError("calibration authority is not replay-safe")
    calibration = _exact(document["calibration"], {
        "active_decoder_children_after_run", "calibration_hard_rss_cap_bytes",
        "child_cpu_max_seconds", "child_elapsed_max_seconds", "contract",
        "evidence_records", "frame_count", "isolation_authority_sha256",
        "isolation_limits_sha256", "metrics_manifest_sha256",
        "parent_cpu_seconds", "parent_peak_rss_bytes",
        "parent_rss_baseline_bytes", "parent_wall_seconds",
        "peak_boundary_max_bytes", "peak_rss_max_bytes", "profile_sha256",
        "sequence_id", "sequence_manifest_sha256", "source_manifest_sha256",
        "spatial_aggregation_is_truth",
    }, "calibration summary")
    if (
        calibration["active_decoder_children_after_run"] != 0
        or calibration["calibration_hard_rss_cap_bytes"]
        != CALIBRATION_RSS_CAP_BYTES
        or calibration["contract"] != "stv3-dwd-rv-observation-replay-v1"
        or calibration["evidence_records"] != 1
        or calibration["frame_count"] != 1
        or calibration["isolation_limits_sha256"]
        != CALIBRATION_ISOLATION_LIMITS_SHA256
        or calibration["profile_sha256"]
        != dwd_germany_roi_profile().fingerprint
        or calibration["source_manifest_sha256"]
        != CALIBRATION_SOURCE_MANIFEST_SHA256
        or calibration["spatial_aggregation_is_truth"] is not False
    ):
        raise DwdGermanyRoiError("calibration summary identity differs")
    for key in (
        "isolation_authority_sha256", "isolation_limits_sha256",
        "metrics_manifest_sha256", "profile_sha256", "sequence_id",
        "sequence_manifest_sha256", "source_manifest_sha256",
    ):
        _sha(calibration[key], f"calibration {key}")
    for key in (
        "child_cpu_max_seconds", "child_elapsed_max_seconds",
        "parent_cpu_seconds", "parent_wall_seconds",
    ):
        value = calibration[key]
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) <= 0.0
        ):
            raise DwdGermanyRoiError(f"calibration {key} is invalid")
    for key in (
        "parent_peak_rss_bytes", "parent_rss_baseline_bytes",
        "peak_boundary_max_bytes", "peak_rss_max_bytes",
    ):
        value = calibration[key]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise DwdGermanyRoiError(f"calibration {key} is invalid")
    derived = derive_dwd_germany_roi_replay_cap(
        observed_peak_boundary_bytes=calibration["peak_boundary_max_bytes"],
        observed_peak_rss_bytes=calibration["peak_rss_max_bytes"],
        active_children_after_run=calibration["active_decoder_children_after_run"],
        calibration_cap_bytes=calibration["calibration_hard_rss_cap_bytes"],
    )
    cap_mapping = _exact(document["cap_derivation"], set(derived.to_mapping()), "cap")
    if dict(cap_mapping) != derived.to_mapping():
        raise DwdGermanyRoiError("calibration cap derivation differs")
    return DwdGermanyRoiFullReplayAuthority(
        selection_manifest_sha256=selection.manifest_sha256,
        calibration_manifest_sha256=expected_calibration_sha256,
        replay_rss_cap_bytes=derived.derived_replay_cap_bytes,
        profile_sha256=calibration["profile_sha256"],
    )


def seal_dwd_germany_roi_calibration_manifest(
    root: str | Path,
    *,
    selection: DwdGermanyRoiManifestReceipt,
    calibration_summary: Mapping[str, Any],
    calibration_source_archive_sha256: str,
    cap: DwdGermanyRoiCapDerivation,
) -> DwdGermanyRoiManifestReceipt:
    required = {
        "active_decoder_children_after_run", "evidence_records", "frame_count",
        "isolation_authority_sha256", "isolation_limits_sha256",
        "metrics_manifest_sha256", "peak_boundary_max_bytes",
        "peak_rss_max_bytes", "profile_sha256", "sequence_id",
        "sequence_manifest_sha256", "source_manifest_sha256",
    }
    if not required <= set(calibration_summary):
        raise DwdGermanyRoiError("calibration summary is incomplete")
    if (
        calibration_summary["frame_count"] != 1
        or calibration_summary["evidence_records"] != 1
        or calibration_summary["active_decoder_children_after_run"] != 0
        or calibration_summary["profile_sha256"]
        != dwd_germany_roi_profile().fingerprint
        or calibration_summary["peak_boundary_max_bytes"]
        != cap.observed_peak_boundary_bytes
        or calibration_summary["peak_rss_max_bytes"]
        != cap.observed_peak_rss_bytes
    ):
        raise DwdGermanyRoiError("calibration summary differs from cap evidence")
    for key in (
        "isolation_authority_sha256", "isolation_limits_sha256",
        "metrics_manifest_sha256", "profile_sha256", "sequence_id",
        "sequence_manifest_sha256", "source_manifest_sha256",
    ):
        _sha(calibration_summary[key], f"calibration {key}")
    document = {
        "active_decoder_children_after_run": 0,
        "calibration": dict(calibration_summary),
        "calibration_source_archive_sha256": _sha(
            calibration_source_archive_sha256,
            "calibration source archive SHA-256",
        ),
        "cap_derivation": cap.to_mapping(),
        "contract": DWD_GERMANY_ROI_CALIBRATION_CONTRACT,
        "full_replay_authorized_by_calibration": True,
        "nuc_or_live_admission_granted": False,
        "selection_manifest_sha256": selection.manifest_sha256,
    }
    return _write_content_addressed(Path(root).resolve(strict=False), document)


def seal_dwd_germany_roi_run_manifest(
    root: str | Path,
    *,
    selection: DwdGermanyRoiManifestReceipt,
    calibration: DwdGermanyRoiManifestReceipt,
    replay_summary: Mapping[str, Any],
) -> DwdGermanyRoiManifestReceipt:
    if (
        replay_summary.get("frame_count") != SOURCE_FRAME_COUNT
        or replay_summary.get("evidence_records") != SOURCE_FRAME_COUNT
        or replay_summary.get("source_manifest_sha256") != SOURCE_MANIFEST_SHA256
        or replay_summary.get("profile_sha256")
        != dwd_germany_roi_profile().fingerprint
        or replay_summary.get("active_decoder_children_after_run") != 0
    ):
        raise DwdGermanyRoiError("full replay summary differs from fixed authority")
    for key in (
        "isolation_authority_sha256", "isolation_limits_sha256",
        "metrics_manifest_sha256", "profile_sha256", "sequence_id",
        "sequence_manifest_sha256", "source_manifest_sha256",
    ):
        _sha(replay_summary[key], f"full replay {key}")
    document = {
        "admission_granted": False,
        "calibration_manifest_sha256": calibration.manifest_sha256,
        "contract": DWD_GERMANY_ROI_RUN_CONTRACT,
        "forecast_leads_used_as_observations": False,
        "replay": dict(replay_summary),
        "selection_manifest_sha256": selection.manifest_sha256,
        "weather_system_rebinding_performed": False,
    }
    return _write_content_addressed(Path(root).resolve(strict=False), document)


__all__ = [
    "CALIBRATION_ARCHIVE_BYTES",
    "CALIBRATION_ARCHIVE_NAME",
    "CALIBRATION_ARCHIVE_SHA256",
    "CALIBRATION_ISOLATION_LIMITS_SHA256",
    "CALIBRATION_RSS_CAP_BYTES",
    "CALIBRATION_SOURCE_MANIFEST_SHA256",
    "CAP_MARGIN_BYTES",
    "CAP_QUANTUM_BYTES",
    "DWD_GERMANY_ROI_CALIBRATION_CONTRACT",
    "DWD_GERMANY_ROI_RUN_CONTRACT",
    "DWD_GERMANY_ROI_SELECTION_CONTRACT",
    "DwdGermanyRoiCapDerivation",
    "DwdGermanyRoiCalibrationSource",
    "DwdGermanyRoiError",
    "DwdGermanyRoiFullReplayAuthority",
    "DwdGermanyRoiManifestReceipt",
    "ROI_HEIGHT",
    "ROI_PIXELS",
    "ROI_RESOLUTION_M",
    "ROI_SOURCE_HEIGHT",
    "ROI_SOURCE_WIDTH",
    "ROI_WIDTH",
    "SOURCE_MANIFEST_SHA256",
    "derive_dwd_germany_roi_replay_cap",
    "dwd_germany_roi_profile",
    "authorize_dwd_germany_roi_full_replay",
    "load_dwd_germany_roi_manifest",
    "seal_dwd_germany_roi_calibration_manifest",
    "seal_dwd_germany_roi_run_manifest",
    "seal_dwd_germany_roi_selection_manifest",
    "verify_dwd_germany_roi_calibration_source",
]
