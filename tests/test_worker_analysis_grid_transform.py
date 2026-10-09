from __future__ import annotations

from dataclasses import replace
import hashlib

import numpy as np
import pytest

from storm_tracker_worker.analysis_grid_transform import (
    AffineGridTransformCertificate,
    AnalysisGridResamplingMethod,
    AnalysisGridTransformCapacityError,
    AnalysisGridTransformConfig,
    AnalysisGridTransformDeadlineError,
    ExactAffineGridTransformAdapter,
    NativeFieldCandidateBinding,
    QualityPlaneResamplingMethod,
    QualityPlaneTransformRule,
    TransformEvidenceClass,
    analysis_grid_field_content_sha256,
    analysis_grid_sha256,
    native_candidate_content_sha256,
    transform_native_rain_rate_to_analysis_grid,
)
from storm_tracker_worker.canonical_evidence_store import CoverageSpec, NativeGridSpec
from storm_tracker_worker.canonical_radar_field_bridge import (
    NativeRainRateFieldCandidate,
    NativeRasterPlane,
    RADAR_FIELD_BRIDGE_ALGORITHM,
    RadarTemporalSemantics,
)
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64
SHA_E = "e" * 64
SEMANTICS = "synthetic quality flag; larger is a different category"


def _readonly(values, dtype=None):
    result = np.asarray(values, dtype=dtype).copy()
    result.setflags(write=False)
    return result


def _candidate(
    *,
    rates=None,
    valid=None,
    nodata=None,
    undetect=None,
    width: int = 2,
    height: int = 2,
    kind: str = "regular",
    provider: str = "synthetic",
    native_crs: str = "SYNTHETIC:METRE",
    auxiliary: bool = False,
) -> NativeRainRateFieldCandidate:
    if rates is None:
        rates = [[0.0, 2.0], [4.0, np.nan]]
    if valid is None:
        valid = [[1, 1], [1, 0]]
    if nodata is None:
        nodata = [[0, 0], [0, 1]]
    if undetect is None:
        undetect = [[0, 0], [0, 0]]
    rate_array = _readonly(rates, np.float64)
    valid_array = _readonly(valid, np.bool_)
    nodata_array = _readonly(nodata, np.bool_)
    undetect_array = _readonly(undetect, np.bool_)
    source_values = np.where(valid_array, rate_array, 999.0)
    source_values = _readonly(source_values, np.float64)
    planes = ()
    if auxiliary:
        planes = (
            NativeRasterPlane(
                name="quality_flag",
                semantics=SEMANTICS,
                values=_readonly([[1, 1], [2, 2]], np.uint8),
            ),
        )
    return NativeRainRateFieldCandidate(
        candidate_id="radar-field-" + SHA_A,
        canonical_run_id="run-" + SHA_A,
        canonical_commit_id="commit-" + SHA_B,
        canonical_receipt_sha256=SHA_C,
        canonical_result_sha256=SHA_D,
        capture_id="capture-" + SHA_A,
        capture_receipt_sha256=SHA_B,
        capture_manifest_sha256=SHA_C,
        artifact_handle="artifact:synthetic:" + SHA_D,
        artifact_sha256=SHA_D,
        artifact_metadata_sha256=SHA_E,
        semantics_certificate_id="synthetic.rate.v1",
        semantics_certificate_sha256=SHA_A,
        provider=provider,
        product="synthetic_exact_rate",
        decoder_identity="synthetic.decoder",
        decoder_version="v1",
        decoder_config_sha256=SHA_B,
        source_quantity="instantaneous_precipitation_rate",
        source_unit="mm/h",
        source_values=source_values,
        output_quantity="instantaneous_precipitation_rate",
        output_unit="mm/h",
        rain_rate_mm_h=rate_array,
        valid_mask=valid_array,
        nodata_mask=nodata_array,
        undetect_mask=undetect_array,
        auxiliary_planes=planes,
        temporal_semantics=RadarTemporalSemantics.INSTANTANEOUS_RATE_ESTIMATE,
        support_start=1_000.0,
        support_end=1_300.0,
        valid_at=1_150.0,
        conversion="identity_mm_h",
        quality_state="synthetic_explicit",
        native_crs=native_crs,
        native_grid=NativeGridSpec(
            kind=kind,
            width=width,
            height=height,
            axis_order="yx",
            definition_sha256=SHA_C,
        ),
        native_coverage=CoverageSpec(
            kind="native_bbox",
            minimum_x=0.0,
            minimum_y=0.0,
            maximum_x=float(width * 1000),
            maximum_y=float(height * 1000),
            definition_sha256=SHA_D,
        ),
        analysis_grid=None,
        metric_raster_field=None,
        primary_sources=("https://example.invalid/synthetic-rate",),
    )


def _grid(
    *,
    width: int = 2,
    height: int = 2,
    west: float = 0.0,
    north: float = 2000.0,
    resolution: float = 1000.0,
    crs: str = "SYNTHETIC:ANALYSIS",
    grid_id: str = "synthetic-analysis-grid",
) -> AnalysisGrid:
    return AnalysisGrid(
        grid_id=grid_id,
        crs=crs,
        origin_x_m=west,
        origin_y_m=north,
        x_resolution_m=resolution,
        y_resolution_m=resolution,
        width=width,
        height=height,
    )


def _config(**changes) -> AnalysisGridTransformConfig:
    values = {
        "max_array_bytes": 2 * 1024 * 1024,
        "max_overlap_contributions": 100_000,
        "max_source_pixels": 10_000,
        "max_target_pixels": 10_000,
    }
    values.update(changes)
    return AnalysisGridTransformConfig(**values)


def _certificate(candidate, config, *, target_crs="SYNTHETIC:ANALYSIS"):
    rules = tuple(
        QualityPlaneTransformRule(
            plane_name=plane.name,
            plane_semantics_sha256=hashlib.sha256(
                plane.semantics.encode("utf-8")
            ).hexdigest(),
            source_dtype=plane.values.dtype.str,
            method=QualityPlaneResamplingMethod.EXACT_AREA_CONSENSUS,
        )
        for plane in candidate.auxiliary_planes
    )
    return AffineGridTransformCertificate(
        certificate_id="synthetic.exact-affine.v1",
        adapter_id="synthetic.affine-adapter",
        adapter_version="v1",
        transform_config_sha256=config.fingerprint,
        evidence_class=TransformEvidenceClass.SYNTHETIC_EXACT_AFFINE_TEST,
        evidence_sha256=SHA_E,
        evidence_authority="test-fixture",
        primary_sources=(),
        source_provider=candidate.provider,
        source_product=candidate.product,
        source_semantics_certificate_id=candidate.semantics_certificate_id,
        source_semantics_certificate_sha256=(
            candidate.semantics_certificate_sha256
        ),
        source_candidate_algorithm=RADAR_FIELD_BRIDGE_ALGORITHM,
        source_crs=candidate.native_crs,
        source_grid_definition_sha256=candidate.native_grid.definition_sha256,
        source_coverage_definition_sha256=(
            candidate.native_coverage.definition_sha256
        ),
        source_width=candidate.native_grid.width,
        source_height=candidate.native_grid.height,
        source_array_axis_order=candidate.native_grid.axis_order,
        target_crs=target_crs,
        source_west_edge_target_m=0.0,
        source_north_edge_target_m=2000.0,
        source_x_resolution_target_m=1000.0,
        source_y_resolution_target_m=1000.0,
        quality_plane_rules=rules,
    )


def _request(candidate=None, *, grid=None, config=None, certificate=None):
    candidate = candidate or _candidate()
    grid = grid or _grid()
    config = config or _config()
    certificate = certificate or _certificate(candidate, config)
    binding = NativeFieldCandidateBinding(
        candidate=candidate,
        candidate_content_sha256=native_candidate_content_sha256(candidate),
        semantics_certificate_sha256=candidate.semantics_certificate_sha256,
    )
    return transform_native_rain_rate_to_analysis_grid(
        binding,
        target_grid=grid,
        transform_adapter=ExactAffineGridTransformAdapter(certificate),
        config=config,
        deadline_at=100.0,
        monotonic=lambda: 10.0,
    )


def test_exact_identity_grid_preserves_zero_nodata_masks_time_and_quantity():
    result = _request()
    assert result.is_candidate
    field = result.field
    assert field is not None
    assert field.value_or_none(0, 0) == 0.0
    assert field.value_or_none(0, 1) == 2.0
    assert field.value_or_none(1, 0) == 4.0
    assert field.value_or_none(1, 1) is None
    assert np.array_equal(field.valid_mask, [[1, 1], [1, 0]])
    assert np.array_equal(field.nodata_mask, [[0, 0], [0, 1]])
    assert not np.any(field.undetect_mask)
    assert np.isnan(field.rain_rate_mm_h[1, 1])
    assert field.support_start == 1_000.0
    assert field.support_end == 1_300.0
    assert field.valid_at == 1_150.0
    assert field.output_unit == "mm/h"
    assert field.admission_status == "candidate_only_unadmitted"
    assert not field.thresholding_applied
    assert not field.wet_dry_classification_applied
    assert not field.extrapolation_applied


def test_area_overlap_coarsening_is_conservative_for_complete_valid_rate():
    candidate = _candidate(
        rates=[[1.0, 2.0], [3.0, 4.0]],
        valid=[[1, 1], [1, 1]],
        nodata=[[0, 0], [0, 0]],
        undetect=[[0, 0], [0, 0]],
    )
    result = _request(candidate, grid=_grid(width=1, height=1, resolution=2000.0))
    field = result.field
    assert field is not None
    assert field.value_or_none(0, 0) == 2.5
    assert field.pixel_accounting.overlap_contributions == 4
    assert field.pixel_accounting.valid_rate_area_integral_mm_h_m2 == 10_000_000.0
    assert field.resampling_method is AnalysisGridResamplingMethod.CONSERVATIVE_AREA_OVERLAP_RATE


def test_area_overlap_refinement_does_not_change_supported_constant_values():
    candidate = _candidate(
        rates=[[1.0, 2.0], [3.0, 4.0]],
        valid=[[1, 1], [1, 1]],
        nodata=[[0, 0], [0, 0]],
        undetect=[[0, 0], [0, 0]],
    )
    result = _request(candidate, grid=_grid(width=4, height=4, resolution=500.0))
    field = result.field
    assert field is not None
    assert np.array_equal(
        field.rain_rate_mm_h,
        [[1, 1, 2, 2], [1, 1, 2, 2], [3, 3, 4, 4], [3, 3, 4, 4]],
    )


def test_mixed_valid_and_nodata_is_withheld_with_area_fractions_not_zero_filled():
    candidate = _candidate()
    result = _request(candidate, grid=_grid(width=1, height=1, resolution=2000.0))
    field = result.field
    assert field is not None
    assert field.mixed_mask[0, 0]
    assert not field.valid_mask[0, 0]
    assert np.isnan(field.rain_rate_mm_h[0, 0])
    assert field.valid_area_fraction[0, 0] == 0.75
    assert field.nodata_area_fraction[0, 0] == 0.25
    assert field.outside_support_area_fraction[0, 0] == 0.0


def test_undetect_remains_distinct_from_nodata_and_real_zero():
    candidate = _candidate(
        rates=[[0.0, np.nan], [4.0, np.nan]],
        valid=[[1, 0], [1, 0]],
        nodata=[[0, 0], [0, 1]],
        undetect=[[0, 1], [0, 0]],
    )
    field = _request(candidate).field
    assert field is not None
    assert field.value_or_none(0, 0) == 0.0
    assert field.undetect_mask[0, 1]
    assert not field.nodata_mask[0, 1]
    assert field.nodata_mask[1, 1]
    assert np.isnan(field.rain_rate_mm_h[0, 1])


def test_target_partially_outside_support_is_never_extrapolated():
    field = _request(grid=_grid(width=2, height=2, west=-500.0, north=2500.0)).field
    assert field is not None
    assert field.outside_support_mask[0, 0]
    assert field.outside_support_area_fraction[0, 0] == 0.75
    assert np.isnan(field.rain_rate_mm_h[0, 0])
    assert field.pixel_accounting.outside_support_area_m2 > 0.0


def test_qc_is_copied_exactly_and_target_values_require_area_consensus():
    candidate = _candidate(
        rates=[[1.0, 2.0], [3.0, 4.0]],
        valid=[[1, 1], [1, 1]],
        nodata=[[0, 0], [0, 0]],
        undetect=[[0, 0], [0, 0]],
        auxiliary=True,
    )
    identity = _request(candidate).field
    assert identity is not None
    assert np.array_equal(identity.source_auxiliary_planes[0].values, [[1, 1], [2, 2]])
    assert np.array_equal(identity.analysis_quality_planes[0].values, [[1, 1], [2, 2]])
    assert np.all(identity.analysis_quality_planes[0].consensus_mask)
    coarse = _request(
        candidate,
        grid=_grid(width=1, height=1, resolution=2000.0),
    ).field
    assert coarse is not None
    assert not coarse.analysis_quality_planes[0].consensus_mask[0, 0]


def test_missing_qc_rule_fails_closed_without_field():
    candidate = _candidate(auxiliary=True)
    config = _config()
    certificate = replace(
        _certificate(candidate, config),
        quality_plane_rules=(),
    )
    result = _request(candidate, config=config, certificate=certificate)
    assert result.field is None
    assert result.issues[0].code == "quality_plane_policy_incomplete"


def test_curvilinear_geometry_is_withheld():
    result = _request(_candidate(kind="curvilinear"))
    assert result.field is None
    assert result.issues[0].code == "source_geometry_not_exact_affine"


def test_certificate_must_bind_target_crs_and_exact_config():
    candidate = _candidate()
    config = _config()
    wrong_crs = _certificate(candidate, config, target_crs="SYNTHETIC:OTHER")
    result = _request(candidate, config=config, certificate=wrong_crs)
    assert result.field is None
    assert result.issues[0].code == "transform_certificate_binding_mismatch"
    other_config = _config(roundtrip_tolerance_m=1e-7)
    result = _request(
        candidate,
        config=other_config,
        certificate=_certificate(candidate, config),
    )
    assert result.field is None
    assert result.issues[0].code == "transform_certificate_binding_mismatch"


def test_binding_rehash_detects_mutated_readonly_view_base():
    candidate = _candidate()
    binding = NativeFieldCandidateBinding(
        candidate=candidate,
        candidate_content_sha256=native_candidate_content_sha256(candidate),
        semantics_certificate_sha256=candidate.semantics_certificate_sha256,
    )
    # The array itself is read-only, but this demonstrates why a fresh hash is
    # still required at the service boundary.
    candidate.rain_rate_mm_h.setflags(write=True)
    candidate.rain_rate_mm_h[0, 0] = 8.0
    candidate.rain_rate_mm_h.setflags(write=False)
    config = _config()
    result = transform_native_rain_rate_to_analysis_grid(
        binding,
        target_grid=_grid(),
        transform_adapter=ExactAffineGridTransformAdapter(
            _certificate(candidate, config)
        ),
        config=config,
        deadline_at=100.0,
        monotonic=lambda: 10.0,
    )
    assert result.field is None
    assert result.issues[0].code == "source_candidate_checksum_mismatch"


def test_semantics_certificate_binding_mismatch_is_invalid():
    candidate = _candidate()
    binding = NativeFieldCandidateBinding(
        candidate=candidate,
        candidate_content_sha256=native_candidate_content_sha256(candidate),
        semantics_certificate_sha256=SHA_B,
    )
    config = _config()
    result = transform_native_rain_rate_to_analysis_grid(
        binding,
        target_grid=_grid(),
        transform_adapter=ExactAffineGridTransformAdapter(
            _certificate(candidate, config)
        ),
        config=config,
        deadline_at=100.0,
        monotonic=lambda: 10.0,
    )
    assert result.field is None
    assert result.issues[0].code == "source_semantics_binding_mismatch"


def test_identical_request_has_identical_receipt_and_output_checksum():
    first = _request().field
    second = _request().field
    assert first is not None and second is not None
    assert first.transform_receipt_id == second.transform_receipt_id
    assert first.transform_receipt_sha256 == second.transform_receipt_sha256
    assert first.output_content_sha256 == second.output_content_sha256
    assert first.field_id == second.field_id
    assert analysis_grid_field_content_sha256(first) == first.output_content_sha256
    assert first.transform_receipt.roundtrip_samples
    assert first.transform_certificate.fingerprint == first.transform_certificate_sha256
    assert first.transform_config.fingerprint == first.transform_config_sha256


def test_target_grid_identity_and_geometry_are_both_checksum_bound():
    first = _grid(grid_id="grid-one")
    second = _grid(grid_id="grid-two")
    assert analysis_grid_sha256(first) != analysis_grid_sha256(second)
    assert _request(grid=first).field.output_content_sha256 != _request(
        grid=second
    ).field.output_content_sha256


def test_output_arrays_and_preserved_qc_are_immutable():
    field = _request(_candidate(auxiliary=True)).field
    assert field is not None
    arrays = (
        field.rain_rate_mm_h,
        field.valid_mask,
        field.nodata_mask,
        field.undetect_mask,
        field.mixed_mask,
        field.outside_support_mask,
        field.valid_area_fraction,
        field.nodata_area_fraction,
        field.undetect_area_fraction,
        field.outside_support_area_fraction,
        field.source_auxiliary_planes[0].values,
        field.analysis_quality_planes[0].values,
        field.analysis_quality_planes[0].consensus_mask,
    )
    assert all(not array.flags.writeable for array in arrays)


def test_deadline_is_absolute_and_checked_before_work():
    candidate = _candidate()
    config = _config()
    binding = NativeFieldCandidateBinding(
        candidate,
        native_candidate_content_sha256(candidate),
        candidate.semantics_certificate_sha256,
    )
    with pytest.raises(AnalysisGridTransformDeadlineError):
        transform_native_rain_rate_to_analysis_grid(
            binding,
            target_grid=_grid(),
            transform_adapter=ExactAffineGridTransformAdapter(
                _certificate(candidate, config)
            ),
            config=config,
            deadline_at=10.0,
            monotonic=lambda: 10.0,
        )


def test_deadline_is_checked_inside_one_large_overlap_cell():
    side = 65
    candidate = _candidate(
        rates=np.ones((side, side), dtype=np.float64),
        valid=np.ones((side, side), dtype=np.bool_),
        nodata=np.zeros((side, side), dtype=np.bool_),
        undetect=np.zeros((side, side), dtype=np.bool_),
        width=side,
        height=side,
    )
    grid = _grid(width=1, height=1, north=float(side), resolution=float(side))
    config = _config()
    certificate = replace(
        _certificate(candidate, config),
        source_north_edge_target_m=float(side),
        source_x_resolution_target_m=1.0,
        source_y_resolution_target_m=1.0,
    )
    binding = NativeFieldCandidateBinding(
        candidate=candidate,
        candidate_content_sha256=native_candidate_content_sha256(candidate),
        semantics_certificate_sha256=candidate.semantics_certificate_sha256,
    )
    calls = 0

    def monotonic():
        nonlocal calls
        calls += 1
        return 0.0 if calls < 8 else 6.0

    with pytest.raises(AnalysisGridTransformDeadlineError):
        transform_native_rain_rate_to_analysis_grid(
            binding,
            target_grid=grid,
            transform_adapter=ExactAffineGridTransformAdapter(certificate),
            config=config,
            deadline_at=5.0,
            monotonic=monotonic,
        )
    assert calls == 8


def test_source_target_overlap_and_byte_capacity_are_hard_bounds():
    candidate = _candidate()
    with pytest.raises(AnalysisGridTransformCapacityError, match="source pixel"):
        _request(candidate, config=_config(max_source_pixels=3))
    with pytest.raises(AnalysisGridTransformCapacityError, match="target pixel"):
        _request(candidate, config=_config(max_target_pixels=3))
    with pytest.raises(AnalysisGridTransformCapacityError, match="byte"):
        _request(candidate, config=_config(max_array_bytes=16))
    with pytest.raises(AnalysisGridTransformCapacityError, match="overlap"):
        _request(
            _candidate(
                rates=[[1.0, 2.0], [3.0, 4.0]],
                valid=[[1, 1], [1, 1]],
                nodata=[[0, 0], [0, 0]],
                undetect=[[0, 0], [0, 0]],
            ),
            grid=_grid(width=1, height=1, resolution=2000.0),
            config=_config(max_overlap_contributions=3),
        )


def test_adapter_receipt_contains_corner_and_centre_roundtrip_audit():
    candidate = _candidate()
    config = _config()
    binding = NativeFieldCandidateBinding(
        candidate,
        native_candidate_content_sha256(candidate),
        candidate.semantics_certificate_sha256,
    )
    receipt = ExactAffineGridTransformAdapter(
        _certificate(candidate, config)
    ).issue_receipt(
        binding=binding,
        target_grid=_grid(),
        config=config,
        deadline_at=100.0,
        monotonic=lambda: 10.0,
    )
    assert len(receipt.roundtrip_samples) == 5
    assert receipt.maximum_roundtrip_error_m == 0.0
    assert receipt.receipt_id == "transform-receipt-" + receipt.fingerprint


def test_stale_valid_receipt_for_another_target_grid_is_rejected():
    candidate = _candidate()
    config = _config()
    certificate = _certificate(candidate, config)
    binding = NativeFieldCandidateBinding(
        candidate,
        native_candidate_content_sha256(candidate),
        candidate.semantics_certificate_sha256,
    )
    exact = ExactAffineGridTransformAdapter(certificate)
    stale = exact.issue_receipt(
        binding=binding,
        target_grid=_grid(grid_id="old-grid"),
        config=config,
        deadline_at=100.0,
        monotonic=lambda: 10.0,
    )

    class StaleAdapter:
        @property
        def certificate(self):
            return certificate

        def issue_receipt(self, **_kwargs):
            return stale

    result = transform_native_rain_rate_to_analysis_grid(
        binding,
        target_grid=_grid(grid_id="new-grid"),
        transform_adapter=StaleAdapter(),
        config=config,
        deadline_at=100.0,
        monotonic=lambda: 10.0,
    )
    assert result.field is None
    assert result.issues[0].code == "coordinate_transform_receipt_mismatch"


def test_coordinate_roundtrip_above_caller_tolerance_is_withheld():
    candidate = _candidate()
    config = _config(roundtrip_tolerance_m=0.0)
    certificate = replace(
        _certificate(candidate, config),
        source_west_edge_target_m=1e17,
        source_north_edge_target_m=1e17,
    )
    result = _request(candidate, config=config, certificate=certificate)
    assert result.field is None
    assert result.issues[0].code == "coordinate_roundtrip_tolerance_exceeded"


def test_source_mutation_by_adapter_is_caught_after_resampling():
    candidate = _candidate()
    config = _config()
    certificate = _certificate(candidate, config)
    binding = NativeFieldCandidateBinding(
        candidate,
        native_candidate_content_sha256(candidate),
        candidate.semantics_certificate_sha256,
    )
    exact = ExactAffineGridTransformAdapter(certificate)

    class MutatingAdapter:
        @property
        def certificate(self):
            return certificate

        def issue_receipt(self, **kwargs):
            receipt = exact.issue_receipt(**kwargs)
            candidate.rain_rate_mm_h.setflags(write=True)
            candidate.rain_rate_mm_h[0, 0] = 7.0
            candidate.rain_rate_mm_h.setflags(write=False)
            return receipt

    result = transform_native_rain_rate_to_analysis_grid(
        binding,
        target_grid=_grid(),
        transform_adapter=MutatingAdapter(),
        config=config,
        deadline_at=100.0,
        monotonic=lambda: 10.0,
    )
    assert result.field is None
    assert result.issues[0].code == "source_candidate_changed_during_transform"


def test_public_output_rehash_detects_postconstruction_array_tamper():
    field = _request().field
    assert field is not None
    original = field.output_content_sha256
    field.rain_rate_mm_h.setflags(write=True)
    field.rain_rate_mm_h[0, 0] = 9.0
    field.rain_rate_mm_h.setflags(write=False)
    assert analysis_grid_field_content_sha256(field) != original


def test_synthetic_evidence_cannot_be_relabelled_as_a_real_provider():
    candidate = _candidate(provider="knmi")
    config = _config()
    with pytest.raises(ValueError, match="synthetic affine evidence"):
        _certificate(candidate, config)


def test_same_crs_is_a_coordinate_transform_and_resample_but_not_reprojection():
    candidate = _candidate(native_crs="SYNTHETIC:SAME")
    config = _config()
    certificate = _certificate(candidate, config, target_crs="SYNTHETIC:SAME")
    field = _request(
        candidate,
        grid=_grid(crs="SYNTHETIC:SAME"),
        config=config,
        certificate=certificate,
    ).field
    assert field is not None
    assert field.coordinate_transform_applied
    assert field.resampling_applied
    assert not field.reprojection_applied


def test_module_has_no_implicit_projection_or_live_dependencies():
    import ast
    from pathlib import Path

    source = Path("storm_tracker_worker/analysis_grid_transform.py").read_text(
        encoding="utf-8"
    )
    imports = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    assert not any(
        name == forbidden or name.startswith(forbidden + ".")
        for forbidden in ("pyproj", "osgeo", "homeassistant", "paho", "mqtt")
        for name in imports
    )
