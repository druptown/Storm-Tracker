"""Content-addressed DWD +000 evidence sequence for the science shadow.

This is an offline, unwired export boundary.  It serializes the existing
``ShadowEvidenceFrame`` algebra without inventing another raster meaning and
seals one ordered DWD observation manifest.  Forecast members are inventoried
but can never enter an evidence record: ``lead_seconds`` is fixed to zero.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import stat
import tarfile
import tempfile
from typing import Any, Iterable, Mapping

from .dwd_rv_completion_consumer import DwdRvCompletionOutputReceipt
from .evidence_to_cells_shadow import EvidenceToCellsPixelAccounting
from .science_shadow_orchestrator import ShadowEvidenceFrame
from .tracking_core.engine.analysis_grid import AnalysisGrid
from .tracking_core.engine.cell_track import CellDetection
from .tracking_core.engine.hierarchical_detection import (
    HierarchicalCellDetection,
    HierarchicalDetectionFrame,
    NormalizedRasterFrame,
)


SOURCE_MANIFEST_CONTRACT = "stv3-dwd-rv-observation-source-manifest-v1"
EVIDENCE_RECORD_CONTRACT = "stv3-science-shadow-dwd-evidence-record-v1"
SEQUENCE_MANIFEST_CONTRACT = "stv3-science-shadow-canonical-sequence-v1"
PROCESS_METRICS_CONTRACT = "stv3-dwd-rv-observation-process-metrics-v1"
SEQUENCE_PROVIDER = "dwd_radolan"
SEQUENCE_PRODUCT = "dwd_open_data_composite_rv_capture"
SEQUENCE_QUANTITY = "interval_mean_precipitation_rate"
SEQUENCE_UNIT = "mm/h"
OBSERVATION_LEAD_SECONDS = 0
CADENCE_SECONDS = 300
EXPECTED_FORECAST_LEADS_SECONDS = tuple(range(300, 7_201, 300))
MAX_ARCHIVE_BYTES = 8 * 1024 * 1024
MAX_SOURCE_BYTES = 512 * 1024 * 1024
MAX_SOURCE_FRAMES = 512
MAX_EVIDENCE_RECORD_BYTES = 64 * 1024 * 1024
MAX_SEQUENCE_MANIFEST_BYTES = 4 * 1024 * 1024

_SHA = re.compile(r"^[0-9a-f]{64}$")
_ARCHIVE_NAME = re.compile(r"^composite_rv_(\d{8})_(\d{4})\.tar$")
_MEMBER_NAME = re.compile(
    r"^composite_rv_(\d{8})_(\d{4})_(\d{3})-hd5$"
)


class DwdRvScienceSequenceError(RuntimeError):
    """The offline source/export failed its exact closed contract."""


class DwdRvScienceSequenceIntegrityError(DwdRvScienceSequenceError):
    """Immutable bytes, identity or evidence differ."""


class DwdRvScienceSequenceCapacityError(DwdRvScienceSequenceError):
    """A complete record cannot fit the configured hard cap."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, RecursionError) as exc:
        raise DwdRvScienceSequenceIntegrityError(
            "science-sequence value is not canonical JSON"
        ) from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise DwdRvScienceSequenceIntegrityError(f"{label} is not SHA-256")
    return value


def _exact(value: object, keys: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise DwdRvScienceSequenceIntegrityError(f"{label} schema differs")
    if any(not isinstance(key, str) for key in value):
        raise DwdRvScienceSequenceIntegrityError(f"{label} has a non-text key")
    return value


def _bounded_bytes(path: Path, maximum: int) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise DwdRvScienceSequenceIntegrityError("export object path is unsafe")
    size = path.stat().st_size
    if not 1 <= size <= maximum:
        raise DwdRvScienceSequenceCapacityError("export object size is invalid")
    body = path.read_bytes()
    if len(body) != size:
        raise DwdRvScienceSequenceIntegrityError("export object changed while read")
    return body


def _safe_root(root: str | Path) -> Path:
    supplied = Path(root)
    if not supplied.is_absolute():
        raise ValueError("science-sequence root must be absolute")
    root_path = supplied.resolve(strict=False)
    if root_path.exists() and (
        root_path.is_symlink() or not stat.S_ISDIR(root_path.lstat().st_mode)
    ):
        raise DwdRvScienceSequenceIntegrityError("science-sequence root is unsafe")
    root_path.mkdir(parents=True, exist_ok=True)
    for name in ("objects", "manifests"):
        path = root_path / name
        path.mkdir(exist_ok=True)
        if path.is_symlink() or not stat.S_ISDIR(path.lstat().st_mode):
            raise DwdRvScienceSequenceIntegrityError("science-sequence layout is unsafe")
    return root_path


def _fsync_directory(path: Path) -> None:
    if os.name != "posix":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_content_addressed(directory: Path, body: bytes, suffix: str) -> Path:
    digest = hashlib.sha256(body).hexdigest()
    target = directory / f"{digest}{suffix}"
    if target.exists() or target.is_symlink():
        if target.is_symlink() or _bounded_bytes(target, len(body)) != body:
            raise DwdRvScienceSequenceIntegrityError(
                "content-addressed export identity conflicts"
            )
        return target
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=directory,
            prefix=f".{digest}.",
            suffix=".stage",
            delete=False,
        ) as temporary:
            temporary.write(body)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path = Path(temporary.name)
        try:
            os.link(temporary_path, target)
        except FileExistsError:
            pass
        _fsync_directory(directory)
        if _bounded_bytes(target, len(body)) != body:
            raise DwdRvScienceSequenceIntegrityError(
                "published content-addressed export differs"
            )
        return target
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
            _fsync_directory(directory)


def _grid_mapping(grid: AnalysisGrid) -> dict[str, Any]:
    return {
        "axis_order": grid.axis_order,
        "contract_version": grid.contract_version,
        "coordinate_unit": grid.coordinate_unit,
        "crs": grid.crs,
        "grid_id": grid.grid_id,
        "height": grid.height,
        "origin_x_m": grid.origin_x_m,
        "origin_y_m": grid.origin_y_m,
        "row_order": grid.row_order,
        "width": grid.width,
        "x_resolution_m": grid.x_resolution_m,
        "y_resolution_m": grid.y_resolution_m,
    }


def _grid_from_mapping(value: object) -> AnalysisGrid:
    mapping = _exact(value, {
        "axis_order", "contract_version", "coordinate_unit", "crs", "grid_id",
        "height", "origin_x_m", "origin_y_m", "row_order", "width",
        "x_resolution_m", "y_resolution_m",
    }, "analysis grid")
    try:
        grid = AnalysisGrid(**dict(mapping))
    except (TypeError, ValueError) as exc:
        raise DwdRvScienceSequenceIntegrityError("analysis grid is invalid") from exc
    if _grid_mapping(grid) != dict(mapping):
        raise DwdRvScienceSequenceIntegrityError("analysis grid roundtrip differs")
    return grid


def _normalized_mapping(frame: NormalizedRasterFrame) -> dict[str, Any]:
    return {
        "frame_id": frame.frame_id,
        "grid": _grid_mapping(frame.grid),
        "quantity": frame.quantity,
        "source": frame.source,
        "unit": frame.unit,
        "valid_at": frame.valid_at,
        "values": [list(row) for row in frame.values],
    }


def _normalized_from_mapping(value: object) -> NormalizedRasterFrame:
    mapping = _exact(value, {
        "frame_id", "grid", "quantity", "source", "unit", "valid_at", "values",
    }, "normalized frame")
    raw_values = mapping["values"]
    if not isinstance(raw_values, list) or any(
        not isinstance(row, list) for row in raw_values
    ):
        raise DwdRvScienceSequenceIntegrityError("normalized values are invalid")
    try:
        values = tuple(tuple(item for item in row) for row in raw_values)
        frame = NormalizedRasterFrame(
            frame_id=mapping["frame_id"],
            valid_at=mapping["valid_at"],
            source=mapping["source"],
            quantity=mapping["quantity"],
            unit=mapping["unit"],
            grid=_grid_from_mapping(mapping["grid"]),
            values=values,
        )
    except (TypeError, ValueError) as exc:
        raise DwdRvScienceSequenceIntegrityError(
            "normalized frame is invalid"
        ) from exc
    if _normalized_mapping(frame) != dict(mapping):
        raise DwdRvScienceSequenceIntegrityError(
            "normalized frame roundtrip differs"
        )
    return frame


def _cell_detection_mapping(value: CellDetection) -> dict[str, Any]:
    return {
        "analysis_grid_id": value.analysis_grid_id,
        "detection_id": value.detection_id,
        "position_sigma_m": value.position_sigma_m,
        "source": value.source,
        "valid_at": value.valid_at,
        "x_m": value.x_m,
        "y_m": value.y_m,
    }


def _hierarchical_detection_mapping(value: HierarchicalCellDetection) -> dict[str, Any]:
    return {
        "area_m2": value.area_m2,
        "centroid_method": value.centroid_method,
        "detection": _cell_detection_mapping(value.detection),
        "footprint_pixels": [list(item) for item in value.footprint_pixels],
        "geometric_centroid_x_m": value.geometric_centroid_x_m,
        "geometric_centroid_y_m": value.geometric_centroid_y_m,
        "low_confidence_fragment": value.low_confidence_fragment,
        "maximum_value": value.maximum_value,
        "mean_value": value.mean_value,
        "minimum_area_m2": value.minimum_area_m2,
        "object_kind": value.object_kind,
        "parent_detection_id": value.parent_detection_id,
        "threshold": value.threshold,
        "value_weighted_centroid_x_m": value.value_weighted_centroid_x_m,
        "value_weighted_centroid_y_m": value.value_weighted_centroid_y_m,
    }


def _hierarchical_detection_from_mapping(value: object) -> HierarchicalCellDetection:
    mapping = _exact(value, {
        "area_m2", "centroid_method", "detection", "footprint_pixels",
        "geometric_centroid_x_m", "geometric_centroid_y_m",
        "low_confidence_fragment", "maximum_value", "mean_value",
        "minimum_area_m2", "object_kind", "parent_detection_id", "threshold",
        "value_weighted_centroid_x_m", "value_weighted_centroid_y_m",
    }, "hierarchical detection")
    detection_mapping = _exact(mapping["detection"], {
        "analysis_grid_id", "detection_id", "position_sigma_m", "source",
        "valid_at", "x_m", "y_m",
    }, "cell detection")
    raw_pixels = mapping["footprint_pixels"]
    if not isinstance(raw_pixels, list) or any(
        not isinstance(item, list) or len(item) != 2 for item in raw_pixels
    ):
        raise DwdRvScienceSequenceIntegrityError("detection footprint is invalid")
    try:
        detection = CellDetection(**dict(detection_mapping))
        result = HierarchicalCellDetection(
            detection=detection,
            object_kind=mapping["object_kind"],
            parent_detection_id=mapping["parent_detection_id"],
            footprint_pixels=tuple((item[0], item[1]) for item in raw_pixels),
            area_m2=mapping["area_m2"],
            geometric_centroid_x_m=mapping["geometric_centroid_x_m"],
            geometric_centroid_y_m=mapping["geometric_centroid_y_m"],
            value_weighted_centroid_x_m=mapping["value_weighted_centroid_x_m"],
            value_weighted_centroid_y_m=mapping["value_weighted_centroid_y_m"],
            mean_value=mapping["mean_value"],
            maximum_value=mapping["maximum_value"],
            threshold=mapping["threshold"],
            centroid_method=mapping["centroid_method"],
            minimum_area_m2=mapping["minimum_area_m2"],
            low_confidence_fragment=mapping["low_confidence_fragment"],
        )
    except (TypeError, ValueError) as exc:
        raise DwdRvScienceSequenceIntegrityError(
            "hierarchical detection is invalid"
        ) from exc
    if _hierarchical_detection_mapping(result) != dict(mapping):
        raise DwdRvScienceSequenceIntegrityError(
            "hierarchical detection roundtrip differs"
        )
    return result


def _detection_frame_mapping(frame: HierarchicalDetectionFrame) -> dict[str, Any]:
    return {
        "analysis_grid_id": frame.analysis_grid_id,
        "detections": [
            _hierarchical_detection_mapping(item) for item in frame.detections
        ],
        "detector_version": frame.detector_version,
        "frame_id": frame.frame_id,
        "quantity": frame.quantity,
        "unit": frame.unit,
        "valid_at": frame.valid_at,
    }


def _detection_frame_from_mapping(value: object) -> HierarchicalDetectionFrame:
    mapping = _exact(value, {
        "analysis_grid_id", "detections", "detector_version", "frame_id",
        "quantity", "unit", "valid_at",
    }, "detection frame")
    raw_detections = mapping["detections"]
    if not isinstance(raw_detections, list):
        raise DwdRvScienceSequenceIntegrityError("detections are invalid")
    try:
        frame = HierarchicalDetectionFrame(
            frame_id=mapping["frame_id"],
            valid_at=mapping["valid_at"],
            analysis_grid_id=mapping["analysis_grid_id"],
            quantity=mapping["quantity"],
            unit=mapping["unit"],
            detections=tuple(
                _hierarchical_detection_from_mapping(item)
                for item in raw_detections
            ),
            detector_version=mapping["detector_version"],
        )
    except (TypeError, ValueError) as exc:
        raise DwdRvScienceSequenceIntegrityError(
            "detection frame is invalid"
        ) from exc
    if _detection_frame_mapping(frame) != dict(mapping):
        raise DwdRvScienceSequenceIntegrityError(
            "detection frame roundtrip differs"
        )
    return frame


_EVIDENCE_SCALAR_FIELDS = (
    "canonical_run_id", "canonical_commit_id", "canonical_receipt_sha256",
    "canonical_result_sha256", "capture_id", "capture_receipt_sha256",
    "capture_manifest_sha256", "evidence_candidate_id",
    "evidence_content_sha256", "native_candidate_id",
    "native_candidate_content_sha256", "native_lineage_sha256",
    "analysis_field_id", "analysis_field_content_sha256",
    "analysis_lineage_sha256", "radar_semantics_certificate_id",
    "radar_semantics_certificate_sha256", "transform_certificate_id",
    "transform_certificate_sha256", "transform_receipt_id",
    "transform_receipt_sha256", "transform_config_sha256",
    "target_grid_sha256", "detector_config_sha256", "pipeline_config_sha256",
)


def evidence_to_mapping(evidence: ShadowEvidenceFrame) -> dict[str, Any]:
    if not isinstance(evidence, ShadowEvidenceFrame):
        raise TypeError("evidence must be ShadowEvidenceFrame")
    return {
        **{field: getattr(evidence, field) for field in _EVIDENCE_SCALAR_FIELDS},
        "detection_frame": _detection_frame_mapping(evidence.detection_frame),
        "normalized_frame": _normalized_mapping(evidence.normalized_frame),
        "operational_admission_blockers": list(
            evidence.operational_admission_blockers
        ),
        "pixel_accounting": evidence.pixel_accounting.to_mapping(),
    }


def evidence_from_mapping(value: object) -> ShadowEvidenceFrame:
    mapping = _exact(value, set(_EVIDENCE_SCALAR_FIELDS) | {
        "detection_frame", "normalized_frame", "operational_admission_blockers",
        "pixel_accounting",
    }, "science evidence")
    blockers = mapping["operational_admission_blockers"]
    accounting = _exact(
        mapping["pixel_accounting"],
        set(EvidenceToCellsPixelAccounting.__dataclass_fields__),
        "pixel accounting",
    )
    if not isinstance(blockers, list) or any(
        not isinstance(item, str) for item in blockers
    ):
        raise DwdRvScienceSequenceIntegrityError("admission blockers are invalid")
    try:
        result = ShadowEvidenceFrame(
            **{field: mapping[field] for field in _EVIDENCE_SCALAR_FIELDS},
            normalized_frame=_normalized_from_mapping(mapping["normalized_frame"]),
            detection_frame=_detection_frame_from_mapping(mapping["detection_frame"]),
            pixel_accounting=EvidenceToCellsPixelAccounting(**dict(accounting)),
            operational_admission_blockers=tuple(blockers),
        )
    except (TypeError, ValueError) as exc:
        raise DwdRvScienceSequenceIntegrityError(
            "science evidence is invalid"
        ) from exc
    if evidence_to_mapping(result) != dict(mapping):
        raise DwdRvScienceSequenceIntegrityError("science evidence roundtrip differs")
    return result


def _completion_mapping(receipt: DwdRvCompletionOutputReceipt) -> dict[str, Any]:
    return {
        "authority_sha256": receipt.authority_sha256,
        "desired_state_sha256": receipt.desired_state_sha256,
        "event_id": receipt.event_id,
        "event_sha256": receipt.event_sha256,
        "isolation_authority_sha256": receipt.isolation_authority_sha256,
        "output_receipt_sha256": receipt.receipt_sha256,
        "output_sha256": receipt.output_sha256,
        "process_receipt_sha256": receipt.process_receipt_sha256,
        "producer_receipt_sha256": receipt.producer_receipt_sha256,
    }


def _validate_completion_evidence(
    evidence: ShadowEvidenceFrame,
    receipt: DwdRvCompletionOutputReceipt,
) -> None:
    output = receipt.output
    decoder = output.get("decoder")
    handoff = output.get("handoff")
    shadow = output.get("shadow")
    isolation = output.get("process_isolation")
    if not all(isinstance(item, Mapping) for item in (
        decoder, handoff, shadow, isolation,
    )):
        raise DwdRvScienceSequenceIntegrityError(
            "completion output is missing evidence bindings"
        )
    if (
        evidence.canonical_run_id != decoder.get("run_id")
        or evidence.canonical_commit_id != decoder.get("commit_id")
        or evidence.canonical_receipt_sha256 != decoder.get("receipt_sha256")
        or evidence.canonical_result_sha256 != decoder.get("result_sha256")
        or evidence.capture_id != handoff.get("capture_id")
        or evidence.capture_receipt_sha256
        != handoff.get("capture_receipt_sha256")
        or evidence.capture_manifest_sha256
        != handoff.get("capture_manifest_sha256")
        or evidence.evidence_content_sha256 != shadow.get("content_sha256")
        or evidence.analysis_field_content_sha256
        != shadow.get("analysis_field_content_sha256")
        or evidence.target_grid_sha256 != shadow.get("target_grid_sha256")
        or evidence.normalized_frame.valid_at != shadow.get("valid_at")
        or evidence.normalized_frame.quantity != SEQUENCE_QUANTITY
        or evidence.normalized_frame.unit != SEQUENCE_UNIT
        or receipt.isolation_authority_sha256
        != isolation.get("authority_sha256")
        or receipt.fact_revision_sha256 is not None
        or receipt.disposition != "withheld"
    ):
        raise DwdRvScienceSequenceIntegrityError(
            "completion and science evidence differ"
        )


@dataclass(frozen=True, slots=True)
class DwdRvScienceEvidenceReceipt:
    record_sha256: str
    record_bytes: int
    record_path: Path
    valid_at: float
    canonical_run_id: str
    evidence_candidate_id: str
    evidence_content_sha256: str
    normalized_frame_id: str
    completion_output_sha256: str


def write_evidence_record(
    root: str | Path,
    *,
    evidence: ShadowEvidenceFrame,
    completion_receipt: DwdRvCompletionOutputReceipt,
    lead_seconds: int = OBSERVATION_LEAD_SECONDS,
) -> DwdRvScienceEvidenceReceipt:
    if lead_seconds != OBSERVATION_LEAD_SECONDS or isinstance(lead_seconds, bool):
        raise DwdRvScienceSequenceIntegrityError(
            "only the +000 observation member may enter science evidence"
        )
    if not isinstance(completion_receipt, DwdRvCompletionOutputReceipt):
        raise TypeError("completion_receipt has wrong type")
    _validate_completion_evidence(evidence, completion_receipt)
    document = {
        "completion": _completion_mapping(completion_receipt),
        "contract": EVIDENCE_RECORD_CONTRACT,
        "evidence": evidence_to_mapping(evidence),
        "lead_seconds": OBSERVATION_LEAD_SECONDS,
        "product": SEQUENCE_PRODUCT,
        "provider": SEQUENCE_PROVIDER,
        "quantity": SEQUENCE_QUANTITY,
        "unit": SEQUENCE_UNIT,
    }
    body = _canonical(document)
    if len(body) > MAX_EVIDENCE_RECORD_BYTES:
        raise DwdRvScienceSequenceCapacityError(
            "DWD science evidence record exceeds byte cap"
        )
    export_root = _safe_root(root)
    path = _write_content_addressed(export_root / "objects", body, ".json")
    return DwdRvScienceEvidenceReceipt(
        record_sha256=hashlib.sha256(body).hexdigest(),
        record_bytes=len(body),
        record_path=path,
        valid_at=evidence.normalized_frame.valid_at,
        canonical_run_id=evidence.canonical_run_id,
        evidence_candidate_id=evidence.evidence_candidate_id,
        evidence_content_sha256=evidence.evidence_content_sha256,
        normalized_frame_id=evidence.normalized_frame.frame_id,
        completion_output_sha256=completion_receipt.output_sha256,
    )


def load_evidence_record(
    path: str | Path,
    *,
    expected_sha256: str,
) -> tuple[ShadowEvidenceFrame, Mapping[str, Any]]:
    target = Path(path).resolve(strict=True)
    body = _bounded_bytes(target, MAX_EVIDENCE_RECORD_BYTES)
    if hashlib.sha256(body).hexdigest() != _sha(
        expected_sha256, "expected evidence record SHA-256"
    ):
        raise DwdRvScienceSequenceIntegrityError("evidence record checksum differs")
    try:
        value = json.loads(body.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise DwdRvScienceSequenceIntegrityError("evidence record is not JSON") from exc
    if _canonical(value) != body:
        raise DwdRvScienceSequenceIntegrityError("evidence record is not canonical")
    record = _exact(value, {
        "completion", "contract", "evidence", "lead_seconds", "product",
        "provider", "quantity", "unit",
    }, "evidence record")
    if (
        record["contract"] != EVIDENCE_RECORD_CONTRACT
        or record["lead_seconds"] != OBSERVATION_LEAD_SECONDS
        or record["provider"] != SEQUENCE_PROVIDER
        or record["product"] != SEQUENCE_PRODUCT
        or record["quantity"] != SEQUENCE_QUANTITY
        or record["unit"] != SEQUENCE_UNIT
    ):
        raise DwdRvScienceSequenceIntegrityError("evidence record route differs")
    completion = _exact(record["completion"], {
        "authority_sha256", "desired_state_sha256", "event_id", "event_sha256",
        "isolation_authority_sha256", "output_receipt_sha256", "output_sha256",
        "process_receipt_sha256", "producer_receipt_sha256",
    }, "completion binding")
    for key in completion:
        if key != "event_id":
            _sha(completion[key], f"completion {key}")
    evidence = evidence_from_mapping(record["evidence"])
    return evidence, completion


@dataclass(frozen=True, slots=True)
class DwdRvSourceManifestReceipt:
    manifest_sha256: str
    manifest_bytes: int
    manifest_path: Path
    frame_count: int
    total_archive_bytes: int
    valid_times: tuple[float, ...]


def load_source_manifest(
    receipt: DwdRvSourceManifestReceipt,
) -> Mapping[str, Any]:
    if not isinstance(receipt, DwdRvSourceManifestReceipt):
        raise TypeError("source manifest receipt has wrong type")
    body = _bounded_bytes(receipt.manifest_path, MAX_SEQUENCE_MANIFEST_BYTES)
    if (
        len(body) != receipt.manifest_bytes
        or hashlib.sha256(body).hexdigest()
        != _sha(receipt.manifest_sha256, "source manifest SHA-256")
    ):
        raise DwdRvScienceSequenceIntegrityError("source manifest bytes differ")
    try:
        value = json.loads(body.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise DwdRvScienceSequenceIntegrityError("source manifest is not JSON") from exc
    if _canonical(value) != body:
        raise DwdRvScienceSequenceIntegrityError("source manifest is not canonical")
    manifest = _exact(value, {
        "cadence_seconds", "contract", "frame_count", "frames",
        "motion_member_policy", "raw_refetch_performed", "total_archive_bytes",
    }, "source manifest")
    frames = manifest["frames"]
    if (
        manifest["contract"] != SOURCE_MANIFEST_CONTRACT
        or manifest["cadence_seconds"] != CADENCE_SECONDS
        or manifest["motion_member_policy"] != "base_plus000_only"
        or manifest["raw_refetch_performed"] is not False
        or not isinstance(frames, list)
        or len(frames) != manifest["frame_count"]
        or len(frames) != receipt.frame_count
    ):
        raise DwdRvScienceSequenceIntegrityError("source manifest contract differs")
    valid_times: list[float] = []
    total = 0
    for index, raw in enumerate(frames):
        frame = _exact(raw, {
            "archive_bytes", "archive_name", "archive_sha256",
            "available_forecast_leads_seconds", "base_time", "frame_number",
            "lead_seconds", "observation_member",
        }, "source frame")
        if (
            frame["frame_number"] != index
            or frame["lead_seconds"] != OBSERVATION_LEAD_SECONDS
            or frame["available_forecast_leads_seconds"]
            != list(EXPECTED_FORECAST_LEADS_SECONDS)
            or not isinstance(frame["archive_bytes"], int)
            or frame["archive_bytes"] <= 0
        ):
            raise DwdRvScienceSequenceIntegrityError("source frame contract differs")
        _sha(frame["archive_sha256"], "source archive SHA-256")
        valid_times.append(frame["base_time"])
        total += frame["archive_bytes"]
    if (
        tuple(valid_times) != receipt.valid_times
        or total != manifest["total_archive_bytes"]
        or total != receipt.total_archive_bytes
    ):
        raise DwdRvScienceSequenceIntegrityError("source manifest accounting differs")
    return manifest


def inventory_source_archives(
    source_directory: str | Path,
    export_root: str | Path,
    *,
    expected_frame_count: int = 36,
) -> DwdRvSourceManifestReceipt:
    source = Path(source_directory).resolve(strict=True)
    if source.is_symlink() or not source.is_dir():
        raise DwdRvScienceSequenceIntegrityError("source directory is unsafe")
    if not 1 <= expected_frame_count <= MAX_SOURCE_FRAMES:
        raise ValueError("expected frame count is outside bound")
    paths = sorted(source.iterdir(), key=lambda item: item.name)
    if len(paths) != expected_frame_count or any(
        path.is_symlink() or not path.is_file() for path in paths
    ):
        raise DwdRvScienceSequenceIntegrityError(
            "source directory does not contain the exact frame set"
        )
    entries: list[dict[str, Any]] = []
    total = 0
    for index, path in enumerate(paths):
        match = _ARCHIVE_NAME.fullmatch(path.name)
        if match is None:
            raise DwdRvScienceSequenceIntegrityError("archive filename is invalid")
        base = datetime.strptime(
            match.group(1) + match.group(2), "%Y%m%d%H%M"
        ).replace(tzinfo=timezone.utc).timestamp()
        size = path.stat().st_size
        if not 1 <= size <= MAX_ARCHIVE_BYTES:
            raise DwdRvScienceSequenceCapacityError("archive size exceeds cap")
        total += size
        if total > MAX_SOURCE_BYTES:
            raise DwdRvScienceSequenceCapacityError("source sequence exceeds byte cap")
        archive_bytes = _bounded_bytes(path, MAX_ARCHIVE_BYTES)
        if len(archive_bytes) != size:
            raise DwdRvScienceSequenceIntegrityError(
                "archive changed between inventory checks"
            )
        digest = hashlib.sha256(archive_bytes)
        try:
            with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:") as archive:
                members = archive.getmembers()
        except (OSError, tarfile.TarError) as exc:
            raise DwdRvScienceSequenceIntegrityError("archive is not valid tar") from exc
        leads: list[int] = []
        observation_name: str | None = None
        for member in members:
            member_match = _MEMBER_NAME.fullmatch(str(member.name))
            if (
                member_match is None
                or not member.isfile()
                or member.size <= 0
                or "/" in member.name
                or "\\" in member.name
                or member_match.group(1) != match.group(1)
                or member_match.group(2) != match.group(2)
            ):
                raise DwdRvScienceSequenceIntegrityError(
                    "archive member identity is invalid"
                )
            lead = int(member_match.group(3)) * 60
            leads.append(lead)
            if lead == 0:
                observation_name = member.name
        expected_leads = (0,) + EXPECTED_FORECAST_LEADS_SECONDS
        if tuple(leads) != expected_leads or observation_name is None:
            raise DwdRvScienceSequenceIntegrityError(
                "archive does not bind exact +000..+120 members"
            )
        entries.append({
            "archive_bytes": size,
            "archive_name": path.name,
            "archive_sha256": digest.hexdigest(),
            "available_forecast_leads_seconds": list(
                EXPECTED_FORECAST_LEADS_SECONDS
            ),
            "base_time": base,
            "frame_number": index,
            "lead_seconds": OBSERVATION_LEAD_SECONDS,
            "observation_member": observation_name,
        })
    valid_times = tuple(item["base_time"] for item in entries)
    if any(
        later - earlier != CADENCE_SECONDS
        for earlier, later in zip(valid_times, valid_times[1:])
    ) or valid_times != tuple(sorted(set(valid_times))):
        raise DwdRvScienceSequenceIntegrityError(
            "source observation sequence is not exact five-minute cadence"
        )
    document = {
        "cadence_seconds": CADENCE_SECONDS,
        "contract": SOURCE_MANIFEST_CONTRACT,
        "frame_count": len(entries),
        "frames": entries,
        "motion_member_policy": "base_plus000_only",
        "raw_refetch_performed": False,
        "total_archive_bytes": total,
    }
    body = _canonical(document)
    if len(body) > MAX_SEQUENCE_MANIFEST_BYTES:
        raise DwdRvScienceSequenceCapacityError("source manifest exceeds byte cap")
    root_path = _safe_root(export_root)
    target = _write_content_addressed(root_path / "manifests", body, ".json")
    return DwdRvSourceManifestReceipt(
        manifest_sha256=hashlib.sha256(body).hexdigest(),
        manifest_bytes=len(body),
        manifest_path=target,
        frame_count=len(entries),
        total_archive_bytes=total,
        valid_times=valid_times,
    )


@dataclass(frozen=True, slots=True)
class DwdRvScienceSequenceReceipt:
    sequence_id: str
    manifest_sha256: str
    manifest_bytes: int
    manifest_path: Path
    frame_count: int
    first_valid_at: float
    last_valid_at: float


def seal_sequence_manifest(
    export_root: str | Path,
    *,
    source_manifest: DwdRvSourceManifestReceipt,
    evidence_records: Iterable[DwdRvScienceEvidenceReceipt],
) -> DwdRvScienceSequenceReceipt:
    if not isinstance(source_manifest, DwdRvSourceManifestReceipt):
        raise TypeError("source_manifest has wrong type")
    load_source_manifest(source_manifest)
    records = tuple(evidence_records)
    if len(records) != source_manifest.frame_count or not records:
        raise DwdRvScienceSequenceIntegrityError(
            "source/evidence frame accounting differs"
        )
    valid_times = tuple(item.valid_at for item in records)
    if valid_times != source_manifest.valid_times or any(
        later - earlier != CADENCE_SECONDS
        for earlier, later in zip(valid_times, valid_times[1:])
    ):
        raise DwdRvScienceSequenceIntegrityError(
            "evidence times differ from source +000 cadence"
        )
    if len({item.record_sha256 for item in records}) != len(records):
        raise DwdRvScienceSequenceIntegrityError("evidence record is duplicated")
    entries = [
        {
            "evidence_candidate_id": item.evidence_candidate_id,
            "evidence_content_sha256": item.evidence_content_sha256,
            "evidence_record_bytes": item.record_bytes,
            "evidence_record_sha256": item.record_sha256,
            "frame_number": index,
            "lead_seconds": OBSERVATION_LEAD_SECONDS,
            "canonical_run_id": item.canonical_run_id,
            "normalized_frame_id": item.normalized_frame_id,
            "valid_at": item.valid_at,
        }
        for index, item in enumerate(records)
    ]
    identity = {
        "contract": SEQUENCE_MANIFEST_CONTRACT,
        "ordered_frames": [
            {
                "canonical_run_id": item.canonical_run_id,
                "evidence_content_sha256": item.evidence_content_sha256,
            }
            for item in records
        ],
        "source_manifest_sha256": source_manifest.manifest_sha256,
    }
    sequence_id = _digest(identity)
    # The grid SHA is read from every immutable evidence record instead of
    # being accepted from a caller.  This also proves all records still exist
    # before the sequence authority is sealed.
    root_path = _safe_root(export_root)
    grid_shas: list[str] = []
    for record in records:
        evidence, completion = load_evidence_record(
            record.record_path,
            expected_sha256=record.record_sha256,
        )
        if (
            record.record_path.stat().st_size != record.record_bytes
            or evidence.normalized_frame.valid_at != record.valid_at
            or evidence.canonical_run_id != record.canonical_run_id
            or evidence.evidence_candidate_id != record.evidence_candidate_id
            or evidence.evidence_content_sha256 != record.evidence_content_sha256
            or evidence.normalized_frame.frame_id != record.normalized_frame_id
            or completion["output_sha256"] != record.completion_output_sha256
        ):
            raise DwdRvScienceSequenceIntegrityError(
                "evidence receipt differs from immutable record"
            )
        grid_shas.append(evidence.target_grid_sha256)
    if len(set(grid_shas)) != 1:
        raise DwdRvScienceSequenceIntegrityError(
            "evidence sequence target grids differ"
        )
    document = {
        "cadence_seconds": CADENCE_SECONDS,
        "contract": SEQUENCE_MANIFEST_CONTRACT,
        "entries": entries,
        "forecast_generated": False,
        "frame_count": len(entries),
        "lead_seconds": OBSERVATION_LEAD_SECONDS,
        "meteorological_admission_granted": False,
        "motion_skill_claimed": False,
        "product": SEQUENCE_PRODUCT,
        "provider": SEQUENCE_PROVIDER,
        "quantity": SEQUENCE_QUANTITY,
        "sequence_id": sequence_id,
        "source_manifest_bytes": source_manifest.manifest_bytes,
        "source_manifest_sha256": source_manifest.manifest_sha256,
        "target_grid_sha256": grid_shas[0],
        "unit": SEQUENCE_UNIT,
    }
    body = _canonical(document)
    if len(body) > MAX_SEQUENCE_MANIFEST_BYTES:
        raise DwdRvScienceSequenceCapacityError("sequence manifest exceeds byte cap")
    target = _write_content_addressed(root_path / "manifests", body, ".json")
    return DwdRvScienceSequenceReceipt(
        sequence_id=sequence_id,
        manifest_sha256=hashlib.sha256(body).hexdigest(),
        manifest_bytes=len(body),
        manifest_path=target,
        frame_count=len(records),
        first_valid_at=valid_times[0],
        last_valid_at=valid_times[-1],
    )


def load_sequence_manifest(
    path: str | Path,
    *,
    expected_sha256: str,
) -> Mapping[str, Any]:
    body = _bounded_bytes(Path(path).resolve(strict=True), MAX_SEQUENCE_MANIFEST_BYTES)
    if hashlib.sha256(body).hexdigest() != _sha(
        expected_sha256, "expected sequence manifest SHA-256"
    ):
        raise DwdRvScienceSequenceIntegrityError("sequence manifest checksum differs")
    try:
        value = json.loads(body.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise DwdRvScienceSequenceIntegrityError("sequence manifest is not JSON") from exc
    if _canonical(value) != body:
        raise DwdRvScienceSequenceIntegrityError("sequence manifest is not canonical")
    manifest = _exact(value, {
        "cadence_seconds", "contract", "entries", "forecast_generated",
        "frame_count", "lead_seconds", "meteorological_admission_granted",
        "motion_skill_claimed", "product", "provider", "quantity", "sequence_id",
        "source_manifest_bytes", "source_manifest_sha256", "target_grid_sha256",
        "unit",
    }, "sequence manifest")
    if (
        manifest["contract"] != SEQUENCE_MANIFEST_CONTRACT
        or manifest["cadence_seconds"] != CADENCE_SECONDS
        or manifest["lead_seconds"] != OBSERVATION_LEAD_SECONDS
        or manifest["provider"] != SEQUENCE_PROVIDER
        or manifest["product"] != SEQUENCE_PRODUCT
        or manifest["quantity"] != SEQUENCE_QUANTITY
        or manifest["unit"] != SEQUENCE_UNIT
        or manifest["forecast_generated"] is not False
        or manifest["meteorological_admission_granted"] is not False
        or manifest["motion_skill_claimed"] is not False
    ):
        raise DwdRvScienceSequenceIntegrityError("sequence manifest route differs")
    entries = manifest["entries"]
    if not isinstance(entries, list) or len(entries) != manifest["frame_count"]:
        raise DwdRvScienceSequenceIntegrityError("sequence entry accounting differs")
    expected_entry_keys = {
        "canonical_run_id", "evidence_candidate_id", "evidence_content_sha256",
        "evidence_record_bytes", "evidence_record_sha256", "frame_number",
        "lead_seconds", "normalized_frame_id", "valid_at",
    }
    for index, entry in enumerate(entries):
        item = _exact(entry, expected_entry_keys, "sequence entry")
        if item["frame_number"] != index or item["lead_seconds"] != 0:
            raise DwdRvScienceSequenceIntegrityError("sequence entry order differs")
        for key in ("evidence_content_sha256", "evidence_record_sha256"):
            _sha(item[key], f"sequence {key}")
    valid_times = tuple(item["valid_at"] for item in entries)
    if valid_times != tuple(sorted(set(valid_times))) or any(
        later - earlier != CADENCE_SECONDS
        for earlier, later in zip(valid_times, valid_times[1:])
    ):
        raise DwdRvScienceSequenceIntegrityError("sequence cadence differs")
    identity = {
        "contract": SEQUENCE_MANIFEST_CONTRACT,
        "ordered_frames": [
            {
                "canonical_run_id": item["canonical_run_id"],
                "evidence_content_sha256": item["evidence_content_sha256"],
            }
            for item in entries
        ],
        "source_manifest_sha256": manifest["source_manifest_sha256"],
    }
    if manifest["sequence_id"] != _digest(identity):
        raise DwdRvScienceSequenceIntegrityError("sequence identity differs")
    _sha(manifest["source_manifest_sha256"], "source manifest SHA-256")
    _sha(manifest["target_grid_sha256"], "target grid SHA-256")
    return manifest


@dataclass(frozen=True, slots=True)
class DwdRvProcessMetric:
    frame_number: int
    valid_at: float
    event_id: str
    event_sha256: str
    output_sha256: str
    elapsed_seconds: float
    child_cpu_seconds: float
    child_peak_rss_bytes: int
    child_peak_boundary_bytes: int

    def __post_init__(self) -> None:
        if isinstance(self.frame_number, bool) or not isinstance(
            self.frame_number, int
        ) or self.frame_number < 0:
            raise ValueError("process metric frame number is invalid")
        for label, value in (
            ("valid_at", self.valid_at),
            ("elapsed_seconds", self.elapsed_seconds),
            ("child_cpu_seconds", self.child_cpu_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) < 0.0
            ):
                raise ValueError(f"process metric {label} is invalid")
        for label, value in (
            ("peak RSS", self.child_peak_rss_bytes),
            ("peak boundary memory", self.child_peak_boundary_bytes),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
            ):
                raise ValueError(f"process metric {label} is invalid")
        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValueError("process metric event ID is invalid")
        _sha(self.event_sha256, "process metric event SHA-256")
        _sha(self.output_sha256, "process metric output SHA-256")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "child_cpu_seconds": float(self.child_cpu_seconds),
            "child_peak_rss_bytes": self.child_peak_rss_bytes,
            "child_peak_boundary_bytes": self.child_peak_boundary_bytes,
            "elapsed_seconds": float(self.elapsed_seconds),
            "event_id": self.event_id,
            "event_sha256": self.event_sha256,
            "frame_number": self.frame_number,
            "output_sha256": self.output_sha256,
            "valid_at": float(self.valid_at),
        }


@dataclass(frozen=True, slots=True)
class DwdRvProcessMetricsReceipt:
    manifest_sha256: str
    manifest_bytes: int
    manifest_path: Path
    frame_count: int
    peak_rss_max_bytes: int
    peak_boundary_max_bytes: int
    child_cpu_max_seconds: float
    elapsed_max_seconds: float


def _nearest_rank(values: Iterable[float | int], percentile: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise DwdRvScienceSequenceIntegrityError("metrics set is empty")
    rank = max(1, int(math.ceil(percentile * len(ordered))))
    return ordered[rank - 1]


def seal_process_metrics_manifest(
    export_root: str | Path,
    *,
    source_manifest: DwdRvSourceManifestReceipt,
    sequence: DwdRvScienceSequenceReceipt,
    isolation_authority_sha256: str,
    isolation_limits_sha256: str,
    metrics: Iterable[DwdRvProcessMetric],
    configured_boundary_memory_cap_bytes: int,
    configured_cpu_cap_seconds: float,
    configured_wall_cap_seconds: float,
) -> DwdRvProcessMetricsReceipt:
    """Seal exact child resource evidence separately from science evidence."""

    load_source_manifest(source_manifest)
    loaded_sequence = load_sequence_manifest(
        sequence.manifest_path, expected_sha256=sequence.manifest_sha256
    )
    if loaded_sequence["sequence_id"] != sequence.sequence_id:
        raise DwdRvScienceSequenceIntegrityError("metrics sequence identity differs")
    authority_sha = _sha(
        isolation_authority_sha256, "metrics isolation authority SHA-256"
    )
    limits_sha = _sha(
        isolation_limits_sha256, "metrics isolation limits SHA-256"
    )
    records = tuple(metrics)
    if (
        len(records) != source_manifest.frame_count
        or len(records) != sequence.frame_count
        or not records
    ):
        raise DwdRvScienceSequenceIntegrityError(
            "process/source/sequence frame accounting differs"
        )
    if tuple(item.frame_number for item in records) != tuple(range(len(records))):
        raise DwdRvScienceSequenceIntegrityError("process metric order differs")
    if tuple(item.valid_at for item in records) != source_manifest.valid_times:
        raise DwdRvScienceSequenceIntegrityError("process metric times differ")
    if len({item.event_id for item in records}) != len(records):
        raise DwdRvScienceSequenceIntegrityError("process event is duplicated")
    if (
        isinstance(configured_boundary_memory_cap_bytes, bool)
        or not isinstance(configured_boundary_memory_cap_bytes, int)
        or configured_boundary_memory_cap_bytes <= 0
    ):
        raise ValueError("configured boundary-memory cap is invalid")
    for label, cap in (
        ("CPU", configured_cpu_cap_seconds),
        ("wall", configured_wall_cap_seconds),
    ):
        if (
            isinstance(cap, bool)
            or not isinstance(cap, (int, float))
            or not math.isfinite(float(cap))
            or float(cap) <= 0.0
        ):
            raise ValueError(f"configured {label} cap is invalid")
    rss = tuple(item.child_peak_rss_bytes for item in records)
    boundary = tuple(item.child_peak_boundary_bytes for item in records)
    cpu = tuple(item.child_cpu_seconds for item in records)
    elapsed = tuple(item.elapsed_seconds for item in records)
    if max(boundary) >= configured_boundary_memory_cap_bytes:
        raise DwdRvScienceSequenceIntegrityError(
            "observed boundary memory does not remain below configured cap"
        )
    if max(cpu) >= float(configured_cpu_cap_seconds):
        raise DwdRvScienceSequenceIntegrityError(
            "observed child CPU does not remain below configured cap"
        )
    if max(elapsed) >= float(configured_wall_cap_seconds):
        raise DwdRvScienceSequenceIntegrityError(
            "observed child walltime does not remain below configured cap"
        )
    aggregates = {
        "child_cpu_seconds": {
            "max": max(cpu), "p50": _nearest_rank(cpu, 0.50),
            "p95": _nearest_rank(cpu, 0.95),
        },
        "child_peak_rss_bytes": {
            "max": max(rss), "p50": int(_nearest_rank(rss, 0.50)),
            "p95": int(_nearest_rank(rss, 0.95)),
        },
        "child_peak_boundary_bytes": {
            "max": max(boundary),
            "p50": int(_nearest_rank(boundary, 0.50)),
            "p95": int(_nearest_rank(boundary, 0.95)),
        },
        "elapsed_seconds": {
            "max": max(elapsed), "p50": _nearest_rank(elapsed, 0.50),
            "p95": _nearest_rank(elapsed, 0.95),
        },
    }
    document = {
        "aggregates": aggregates,
        "configured_boundary_memory_cap_bytes": (
            configured_boundary_memory_cap_bytes
        ),
        "configured_cpu_cap_seconds": float(configured_cpu_cap_seconds),
        "configured_wall_cap_seconds": float(configured_wall_cap_seconds),
        "contract": PROCESS_METRICS_CONTRACT,
        "entries": [item.to_mapping() for item in records],
        "frame_count": len(records),
        "isolation_authority_sha256": authority_sha,
        "isolation_limits_sha256": limits_sha,
        "meteorological_admission_granted": False,
        "percentile_method": "nearest_rank",
        "sequence_id": sequence.sequence_id,
        "sequence_manifest_sha256": sequence.manifest_sha256,
        "source_manifest_sha256": source_manifest.manifest_sha256,
    }
    body = _canonical(document)
    if len(body) > MAX_SEQUENCE_MANIFEST_BYTES:
        raise DwdRvScienceSequenceCapacityError("metrics manifest exceeds byte cap")
    root_path = _safe_root(export_root)
    target = _write_content_addressed(root_path / "manifests", body, ".json")
    return DwdRvProcessMetricsReceipt(
        manifest_sha256=hashlib.sha256(body).hexdigest(),
        manifest_bytes=len(body),
        manifest_path=target,
        frame_count=len(records),
        peak_rss_max_bytes=max(rss),
        peak_boundary_max_bytes=max(boundary),
        child_cpu_max_seconds=max(cpu),
        elapsed_max_seconds=max(elapsed),
    )


__all__ = [
    "CADENCE_SECONDS",
    "DwdRvScienceEvidenceReceipt",
    "DwdRvScienceSequenceCapacityError",
    "DwdRvScienceSequenceError",
    "DwdRvScienceSequenceIntegrityError",
    "DwdRvScienceSequenceReceipt",
    "DwdRvProcessMetric",
    "DwdRvProcessMetricsReceipt",
    "DwdRvSourceManifestReceipt",
    "EVIDENCE_RECORD_CONTRACT",
    "OBSERVATION_LEAD_SECONDS",
    "PROCESS_METRICS_CONTRACT",
    "SEQUENCE_MANIFEST_CONTRACT",
    "SOURCE_MANIFEST_CONTRACT",
    "evidence_from_mapping",
    "evidence_to_mapping",
    "inventory_source_archives",
    "load_evidence_record",
    "load_source_manifest",
    "load_sequence_manifest",
    "seal_sequence_manifest",
    "seal_process_metrics_manifest",
    "write_evidence_record",
]
