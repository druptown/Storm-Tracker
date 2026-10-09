from __future__ import annotations

import ast

from dataclasses import replace
import hashlib
from pathlib import Path

import numpy as np
import pytest

from storm_tracker_worker.analysis_grid_transform import (
    AnalysisGridTransformConfig,
    ExactAffineGridTransformAdapter,
    NativeFieldCandidateBinding,
    TransformEvidenceClass,
    native_candidate_content_sha256,
    transform_native_rain_rate_to_analysis_grid,
)
from storm_tracker_worker.canonical_evidence_store import CoverageSpec, NativeGridSpec
from storm_tracker_worker.canonical_radar_field_bridge import (
    NativeRainRateFieldCandidate,
    NativeRasterPlane,
    RadarTemporalSemantics,
    default_radar_semantics_certificates,
)
from storm_tracker_worker.dwd_rv_analysis_grid_certificate import (
    DWD_RV_GRID_EVIDENCE_SHA256,
    DwdRvGridCertificateError,
    SOURCE_EAST_EDGE_M,
    SOURCE_NORTH_EDGE_M,
    SOURCE_SOUTH_EDGE_M,
    SOURCE_WEST_EDGE_M,
    issue_dwd_rv_native_grid_certificate,
)
from storm_tracker_worker.dwd_rv_quantitative import (
    ARTIFACT_HANDLE_PREFIX,
    DECODER_CONFIG_SHA256,
    DECODER_IDENTITY,
    DECODER_VERSION,
    DWD_RV_CAPTURE_PRODUCT,
    DWD_RV_PROVIDER,
)
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid


def _python_imports_module(content: str, module_name: str) -> bool:
    tree = ast.parse(content)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import) and any(
            alias.name.endswith(module_name) for alias in node.names
        ):
            return True
        if isinstance(node, ast.ImportFrom) and (
            (node.module or "").endswith(module_name)
            or any(alias.name == module_name for alias in node.names)
        ):
            return True
        if not isinstance(node, ast.Call) or not node.args:
            continue
        argument = node.args[0]
        if not (
            isinstance(argument, ast.Constant)
            and isinstance(argument.value, str)
            and argument.value.endswith(module_name)
        ):
            continue
        if isinstance(node.func, ast.Name) and node.func.id == "__import__":
            return True
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "import_module"
        ):
            return True
    return False


ROOT = Path(__file__).resolve().parents[1]
PROJDEF = (
    "+proj=stere +lat_ts=60 +lat_0=90 +lon_0=10 "
    "+x_0=543196.83521776402 +y_0=3622588.8619310022 "
    "+units=m +a=6378137 +b=6356752.3142451802 +no_defs"
)
GRID_SHA = hashlib.sha256(b"dwd-official-de1200-grid").hexdigest()
COVERAGE_SHA = hashlib.sha256(b"dwd-official-de1200-coverage").hexdigest()
ARTIFACT_SHA = hashlib.sha256(b"dwd-derived-artifact").hexdigest()


def _readonly_fill(value, dtype):
    result = np.full((1200, 1100), value, dtype=dtype)
    result.setflags(write=False)
    return result


SOURCE_VALUES = _readonly_fill(0.0, np.dtype("<f8"))
RAIN_RATES = _readonly_fill(0.0, np.dtype("<f8"))
VALID = _readonly_fill(True, np.bool_)
NODATA = _readonly_fill(False, np.bool_)
UNDETECT = _readonly_fill(False, np.bool_)
RAW_VALUES = _readonly_fill(0, np.dtype("<u4"))


def _semantics():
    return next(
        item
        for item in default_radar_semantics_certificates()
        if item.provider == DWD_RV_PROVIDER
    )


def _candidate(**changes) -> NativeRainRateFieldCandidate:
    semantics = _semantics()
    values = {
        "candidate_id": "radar-field-" + "1" * 64,
        "canonical_run_id": "run-" + "2" * 64,
        "canonical_commit_id": "commit-" + "3" * 64,
        "canonical_receipt_sha256": "4" * 64,
        "canonical_result_sha256": "5" * 64,
        "capture_id": "capture-" + "6" * 64,
        "capture_receipt_sha256": "7" * 64,
        "capture_manifest_sha256": "8" * 64,
        "artifact_handle": ARTIFACT_HANDLE_PREFIX + ARTIFACT_SHA,
        "artifact_sha256": ARTIFACT_SHA,
        "artifact_metadata_sha256": "9" * 64,
        "semantics_certificate_id": semantics.certificate_id,
        "semantics_certificate_sha256": semantics.fingerprint,
        "provider": DWD_RV_PROVIDER,
        "product": DWD_RV_CAPTURE_PRODUCT,
        "decoder_identity": DECODER_IDENTITY,
        "decoder_version": DECODER_VERSION,
        "decoder_config_sha256": DECODER_CONFIG_SHA256,
        "source_quantity": "precipitation_accumulation_5_minute",
        "source_unit": "mm",
        "source_values": SOURCE_VALUES,
        "output_quantity": "interval_mean_precipitation_rate",
        "output_unit": "mm/h",
        "rain_rate_mm_h": RAIN_RATES,
        "valid_mask": VALID,
        "nodata_mask": NODATA,
        "undetect_mask": UNDETECT,
        "auxiliary_planes": (
            NativeRasterPlane(
                name="raw_values",
                semantics="native_uint32_with_sentinels",
                values=RAW_VALUES,
            ),
        ),
        "temporal_semantics": RadarTemporalSemantics.INTERVAL_MEAN_RATE,
        "support_start": 1_000.0,
        "support_end": 1_300.0,
        "valid_at": 1_300.0,
        "conversion": "exact_interval_mean_rate=accumulation_mm*3600/300",
        "quality_state": "unavailable:no_native_quality_plane",
        "native_crs": PROJDEF,
        "native_grid": NativeGridSpec(
            kind="regular",
            width=1100,
            height=1200,
            axis_order="yx",
            definition_sha256=GRID_SHA,
        ),
        "native_coverage": CoverageSpec(
            kind="native_bbox",
            minimum_x=SOURCE_WEST_EDGE_M,
            minimum_y=SOURCE_SOUTH_EDGE_M,
            maximum_x=SOURCE_EAST_EDGE_M,
            maximum_y=SOURCE_NORTH_EDGE_M,
            definition_sha256=COVERAGE_SHA,
        ),
        "analysis_grid": None,
        "metric_raster_field": None,
        "primary_sources": semantics.primary_sources,
    }
    values.update(changes)
    return NativeRainRateFieldCandidate(**values)


def _binding(candidate: NativeRainRateFieldCandidate) -> NativeFieldCandidateBinding:
    return NativeFieldCandidateBinding(
        candidate=candidate,
        candidate_content_sha256=native_candidate_content_sha256(candidate),
        semantics_certificate_sha256=candidate.semantics_certificate_sha256,
    )


def _target(*, crs: str = PROJDEF) -> AnalysisGrid:
    # Deliberately outside DWD support: the end-to-end transform test exercises
    # all bindings without iterating through 1.32 million overlap contributions.
    return AnalysisGrid(
        grid_id="dwd-certificate-test-outside-support",
        crs=crs,
        origin_x_m=2_000_000.0,
        origin_y_m=2_000_000.0,
        x_resolution_m=1000.0,
        y_resolution_m=1000.0,
        width=1,
        height=1,
    )


def _config() -> AnalysisGridTransformConfig:
    return AnalysisGridTransformConfig(
        max_source_pixels=2_000_000,
        max_target_pixels=10,
        max_array_bytes=128 * 1024 * 1024,
        max_overlap_contributions=100,
    )


def _issue(candidate: NativeRainRateFieldCandidate | None = None, *, target=None):
    candidate = candidate or _candidate()
    return issue_dwd_rv_native_grid_certificate(
        binding=_binding(candidate),
        target_grid=target or _target(),
        config=_config(),
    )


def test_official_certificate_binds_exact_de1200_edges_and_candidate_receipts():
    candidate = _candidate()
    certificate = _issue(candidate)

    assert certificate.evidence_class is TransformEvidenceClass.OFFICIAL_EXACT_GRID_ANCHORS
    assert certificate.evidence_sha256 == DWD_RV_GRID_EVIDENCE_SHA256
    assert certificate.source_grid_definition_sha256 == GRID_SHA
    assert certificate.source_coverage_definition_sha256 == COVERAGE_SHA
    assert certificate.source_west_edge_target_m == -500.0
    assert certificate.source_north_edge_target_m == 500.0
    assert certificate.source_x_resolution_target_m == 1000.0
    assert certificate.source_y_resolution_target_m == 1000.0
    assert certificate.source_width == 1100
    assert certificate.source_height == 1200
    assert certificate.source_crs == PROJDEF
    assert certificate.target_crs == PROJDEF
    assert len(certificate.primary_sources) == 3
    assert certificate.quality_plane_rules[0].plane_name == "raw_values"


def test_identical_bound_request_has_identical_certificate_fingerprint():
    first = _issue()
    second = _issue()

    assert first == second
    assert first.fingerprint == second.fingerprint
    assert first.certificate_id.endswith(first.certificate_id.rsplit(":", 1)[1])


def test_real_certificate_is_consumed_by_transform_without_reprojection():
    candidate = _candidate()
    binding = _binding(candidate)
    target = _target()
    config = _config()
    certificate = issue_dwd_rv_native_grid_certificate(
        binding=binding,
        target_grid=target,
        config=config,
    )

    result = transform_native_rain_rate_to_analysis_grid(
        binding,
        target_grid=target,
        transform_adapter=ExactAffineGridTransformAdapter(certificate),
        config=config,
        deadline_at=100.0,
        monotonic=lambda: 10.0,
    )

    assert result.is_candidate
    assert result.field is not None
    assert result.field.outside_support_mask[0, 0]
    assert result.field.transform_receipt.source_support_edges_target_m == (
        SOURCE_WEST_EDGE_M,
        SOURCE_SOUTH_EDGE_M,
        SOURCE_EAST_EDGE_M,
        SOURCE_NORTH_EDGE_M,
    )
    assert result.field.reprojection_applied is False


@pytest.mark.parametrize(
    "candidate",
    (
        _candidate(provider="synthetic-dwd"),
        _candidate(decoder_version="drifted-decoder"),
        _candidate(
            native_grid=NativeGridSpec(
                kind="regular",
                width=1099,
                height=1200,
                axis_order="yx",
                definition_sha256=GRID_SHA,
            ),
            source_values=SOURCE_VALUES[:, :1099],
            rain_rate_mm_h=RAIN_RATES[:, :1099],
            valid_mask=VALID[:, :1099],
            nodata_mask=NODATA[:, :1099],
            undetect_mask=UNDETECT[:, :1099],
            auxiliary_planes=(
                NativeRasterPlane(
                    name="raw_values",
                    semantics="native_uint32_with_sentinels",
                    values=RAW_VALUES[:, :1099],
                ),
            ),
        ),
        _candidate(
            native_coverage=CoverageSpec(
                kind="native_bbox",
                minimum_x=-499.999,
                minimum_y=SOURCE_SOUTH_EDGE_M,
                maximum_x=SOURCE_EAST_EDGE_M,
                maximum_y=SOURCE_NORTH_EDGE_M,
                definition_sha256=COVERAGE_SHA,
            )
        ),
        _candidate(native_crs=PROJDEF.replace("+lon_0=10", "+lon_0=11")),
        _candidate(support_start=1_001.0),
        _candidate(
            auxiliary_planes=(
                NativeRasterPlane(
                    name="raw_values",
                    semantics="invented_quality_score",
                    values=RAW_VALUES,
                ),
            )
        ),
    ),
)
def test_identity_semantics_geometry_and_plane_drift_fail_closed(candidate):
    with pytest.raises(DwdRvGridCertificateError):
        _issue(candidate)


def test_target_crs_must_be_the_exact_bound_native_crs():
    with pytest.raises(DwdRvGridCertificateError, match="drifted"):
        _issue(target=_target(crs="EPSG:3857"))


def test_mutation_after_caller_binding_is_detected_before_certificate_issue():
    rates = np.array(RAIN_RATES, copy=True)
    rates.setflags(write=False)
    candidate = _candidate(rain_rate_mm_h=rates)
    binding = _binding(candidate)
    rates.setflags(write=True)
    rates[0, 0] = 1.0
    rates.setflags(write=False)

    with pytest.raises(DwdRvGridCertificateError, match="caller binding"):
        issue_dwd_rv_native_grid_certificate(
            binding=binding,
            target_grid=_target(),
            config=_config(),
        )


def test_certificate_module_is_limited_to_certified_worker_consumers_and_not_ha():
    needle = "dwd_rv_analysis_grid_certificate"
    inert_shadow_consumers = {
        "storm_tracker_worker/dwd_rv_completion_consumer.py",
        "storm_tracker_worker/dwd_rv_observation_replay.py",
        "storm_tracker_worker/worker_only_radar_processing.py",
    }
    hits: list[str] = []
    for base in (ROOT / "storm_tracker_worker", ROOT / "custom_components"):
        for path in base.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in {
                ".py", ".json", ".yaml", ".yml", ".sh"
            }:
                continue
            if path.name in {
                "dwd_rv_analysis_grid_certificate.py",
                "worker_cutover_manifest.py",
            }:
                continue
            try:
                content = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            referenced = (
                _python_imports_module(content, needle)
                if path.suffix.lower() == ".py"
                else needle in content
            )
            if referenced:
                hits.append(path.relative_to(ROOT).as_posix())
    assert set(hits) == inert_shadow_consumers
    for live_path in (
        ROOT / "storm_tracker_worker" / "service.py",
        ROOT / "storm_tracker_worker" / "autonomous_scheduler.py",
        ROOT / "storm_tracker_worker" / "config.yaml",
    ):
        if live_path.is_file():
            assert needle not in live_path.read_text(encoding="utf-8")
    for path in (ROOT / "custom_components").rglob("*"):
        if path.is_file() and path.suffix.lower() in {
            ".py", ".json", ".yaml", ".yml",
        }:
            content = path.read_text(encoding="utf-8")
            assert not (
                _python_imports_module(content, needle)
                if path.suffix.lower() == ".py"
                else needle in content
            )
    assert all(
        not item.startswith("custom_components/")
        and item
        not in {
            "storm_tracker_worker/service.py",
            "storm_tracker_worker/autonomous_scheduler.py",
            "storm_tracker_worker/config.yaml",
        }
        for item in hits
    )
