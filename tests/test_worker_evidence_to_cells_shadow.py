from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path

import numpy as np
import pytest

from storm_tracker_worker.analysis_grid_transform import (
    AffineGridTransformCertificate,
    AnalysisGridTransformConfig,
    ExactAffineGridTransformAdapter,
    TransformEvidenceClass,
)
from storm_tracker_worker.canonical_evidence_store import (
    CanonicalEvidenceStore,
    EVIDENCE_CONTRACT,
)
from storm_tracker_worker.canonical_radar_field_bridge import (
    AnalysisGridMapping,
    RADAR_FIELD_BRIDGE_ALGORITHM,
    RadarFieldSemanticsCertificate,
    RadarTemporalSemantics,
    VerifiedProviderRaster,
)
from storm_tracker_worker.evidence_to_cells_shadow import (
    EVIDENCE_TO_CELLS_ADMISSION_STATUS,
    EvidenceToCellsShadowCapacityError,
    EvidenceToCellsShadowConfig,
    EvidenceToCellsShadowDeadlineError,
    PixelDisposition,
    compose_evidence_to_cells_shadow,
    evidence_to_cells_content_sha256,
)
from storm_tracker_worker.native_capture import (
    NativeCaptureMemberInput,
    capture_native_set,
)
from storm_tracker_worker.science_shadow_orchestrator import (
    CanonicalEvidenceCellComposer,
)
from storm_tracker_worker.raw_artifacts import RawArtifactStore
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
)


PRODUCT_AT = 1_900_000_000.0
RECEIVED_AT = PRODUCT_AT + 5.0
CONFIG_SHA = "c" * 64
GRID_SHA = "d" * 64
COVERAGE_SHA = "e" * 64
ARTIFACT_SHA = "f" * 64


def _readonly(values, dtype=None):
    result = np.asarray(values, dtype=dtype).copy()
    result.setflags(write=False)
    return result


def _commit(tmp_path: Path, *, width: int, height: int):
    raw = RawArtifactStore((tmp_path / "raw").resolve())
    raw.initialize()
    capture = capture_native_set(
        raw,
        provider="synthetic-radar",
        capture_product="synthetic_rate_capture",
        fetch_id="synthetic:rate",
        upstream_capture_id="synthetic/rate.bin",
        product_timestamp=PRODUCT_AT,
        collector="evidence-to-cells-test",
        collector_version="v1",
        members=(NativeCaptureMemberInput(
            role="native_raster",
            product="synthetic_rate_native",
            content_type="application/octet-stream",
            received_at=RECEIVED_AT,
            upstream_id="synthetic/rate.bin",
            payload=b"exact-native-synthetic-raster",
            provenance={"semantic_class": "quantitative_precipitation"},
        ),),
    ).capture
    evidence = CanonicalEvidenceStore(
        (tmp_path / "evidence.sqlite3").resolve(),
        raw_artifact_reader=raw,
        clock=lambda: PRODUCT_AT + 20.0,
    )
    receipt = evidence.commit({
        "attempt_id": "decoder-attempt-" + "b" * 64,
        "capture": {
            "capture_id": capture.capture_id,
            "capture_receipt_sha256": capture.capture_receipt_sha256,
            "members": [
                {
                    "content_sha256": member.content_sha256,
                    "member_id": member.member_id,
                    "receipt_id": member.receipt_id,
                    "receipt_sha256": member.receipt_sha256,
                    "received_at": member.received_at,
                    "role": member.role,
                }
                for member in capture.members
            ],
        },
        "contract": EVIDENCE_CONTRACT,
        "coverage": {
            "definition_sha256": COVERAGE_SHA,
            "kind": "native_bbox",
            "maximum_x": float(width * 1000),
            "maximum_y": float(height * 1000),
            "minimum_x": 0.0,
            "minimum_y": 0.0,
        },
        "decoder": {
            "config_sha256": CONFIG_SHA,
            "identity": "synthetic-rate-decoder",
            "version": "v1",
        },
        "decoder_job_id": "decoder-job-" + "a" * 64,
        "evaluated_at": PRODUCT_AT + 10.0,
        "native_crs": "SYNTHETIC:METRE",
        "native_grid": {
            "axis_order": "yx",
            "definition_sha256": GRID_SHA,
            "height": height,
            "kind": "regular",
            "width": width,
        },
        "non_operational_candidate_artifact": {
            "handle": "artifact:synthetic-rate:" + ARTIFACT_SHA,
            "sha256": ARTIFACT_SHA,
        },
        "outcome": "decoded_complete",
        "product": capture.product,
        "product_time": capture.product_timestamp,
        "provider": capture.provider,
        "quantity": "instantaneous_precipitation_rate",
        "reason_code": "decoded_complete",
        "received_at": RECEIVED_AT,
        "unit": "mm/h",
    })
    return evidence, receipt


class SyntheticAuthority:
    def __init__(
        self,
        receipt,
        *,
        values,
        valid,
        nodata,
        undetect,
        metadata_tamper: bool = False,
        after_load=None,
    ) -> None:
        self.receipt = receipt
        self.values = _readonly(values, np.float64)
        self.valid = _readonly(valid, np.bool_)
        self.nodata = _readonly(nodata, np.bool_)
        self.undetect = _readonly(undetect, np.bool_)
        self.metadata_tamper = metadata_tamper
        self.after_load = after_load
        self.calls = 0

    def load_verified(self, receipt, certificate, *, deadline_at, monotonic):
        self.calls += 1
        run = receipt.run
        metadata = {
            "capture": {
                "capture_id": run.capture_id,
                "capture_receipt_sha256": run.capture_receipt_sha256,
                "manifest_sha256": receipt.capture_manifest_sha256,
                "product": run.product,
                "product_timestamp": run.product_time,
                "provider": run.provider,
            },
            "decoder": {
                "config_sha256": run.config_sha256,
                "identity": run.decoder_identity,
                "version": run.decoder_version,
            },
            "grid": {
                "columns": run.native_grid.width,
                "coverage": {
                    "maximum_x": run.coverage.maximum_x,
                    "maximum_y": run.coverage.maximum_y,
                    "minimum_x": run.coverage.minimum_x,
                    "minimum_y": run.coverage.minimum_y,
                },
                "coverage_definition_sha256": run.coverage.definition_sha256,
                "grid_definition_sha256": run.native_grid.definition_sha256,
                "native_crs": run.native_crs,
                "rows": run.native_grid.height,
            },
            "product": {"quantity": run.quantity, "unit": run.unit},
            "schema": "stv3-synthetic-raster-v1",
        }
        if self.metadata_tamper:
            metadata["capture"]["capture_id"] = "capture-" + "0" * 64
        if self.after_load is not None:
            self.after_load()
        return VerifiedProviderRaster(
            artifact_schema="stv3-synthetic-raster-v1",
            artifact_metadata=metadata,
            source_values=self.values,
            valid_mask=self.valid,
            nodata_mask=self.nodata,
            undetect_mask=self.undetect,
            auxiliary_planes=(),
            support_start=PRODUCT_AT,
            support_end=PRODUCT_AT + 300.0,
            representative_at=PRODUCT_AT,
            source_quantity=str(run.quantity),
            source_unit=str(run.unit),
            quality_state="synthetic_explicit_masks",
            analysis_grid=None,
        )


def _semantics(receipt):
    run = receipt.run
    return RadarFieldSemanticsCertificate(
        certificate_id="synthetic.exact-rate.v1",
        provider=run.provider,
        product=run.product,
        decoder_identity=run.decoder_identity,
        decoder_version=run.decoder_version,
        decoder_config_sha256=run.config_sha256,
        artifact_schema="stv3-synthetic-raster-v1",
        artifact_handle_prefix="artifact:synthetic-rate:",
        artifact_authority_key="synthetic_rate_v1",
        receipt_quantity=str(run.quantity),
        receipt_unit=str(run.unit),
        output_quantity="instantaneous_precipitation_rate",
        output_unit="mm/h",
        temporal_semantics=RadarTemporalSemantics.INSTANTANEOUS_RATE_ESTIMATE,
        exact_accumulation_seconds=None,
        analysis_grid_mapping=AnalysisGridMapping.NONE,
        qc_policy="explicit_synthetic_masks",
        primary_sources=("https://example.invalid/synthetic-spec",),
    )


def _grid(*, width: int, height: int, x_resolution: float = 1000.0):
    return AnalysisGrid(
        grid_id=f"synthetic-analysis-{width}x{height}-{int(x_resolution)}",
        crs="SYNTHETIC:ANALYSIS",
        origin_x_m=0.0,
        origin_y_m=float(height * 1000),
        x_resolution_m=x_resolution,
        y_resolution_m=1000.0,
        width=width,
        height=height,
    )


def _transform(receipt, semantics, config, *, width: int, height: int):
    return ExactAffineGridTransformAdapter(AffineGridTransformCertificate(
        certificate_id="synthetic.exact-affine.v1",
        adapter_id="synthetic.affine-adapter",
        adapter_version="v1",
        transform_config_sha256=config.fingerprint,
        evidence_class=TransformEvidenceClass.SYNTHETIC_EXACT_AFFINE_TEST,
        evidence_sha256="9" * 64,
        evidence_authority="test-fixture",
        primary_sources=(),
        source_provider=receipt.run.provider,
        source_product=receipt.run.product,
        source_semantics_certificate_id=semantics.certificate_id,
        source_semantics_certificate_sha256=semantics.fingerprint,
        source_candidate_algorithm=RADAR_FIELD_BRIDGE_ALGORITHM,
        source_crs=str(receipt.run.native_crs),
        source_grid_definition_sha256=receipt.run.native_grid.definition_sha256,
        source_coverage_definition_sha256=receipt.run.coverage.definition_sha256,
        source_width=width,
        source_height=height,
        source_array_axis_order=receipt.run.native_grid.axis_order,
        target_crs="SYNTHETIC:ANALYSIS",
        source_west_edge_target_m=0.0,
        source_north_edge_target_m=float(height * 1000),
        source_x_resolution_target_m=1000.0,
        source_y_resolution_target_m=1000.0,
    ))


def _detector():
    return HierarchicalDetectorConfig(
        shield_threshold=1.0,
        core_threshold=4.0,
        minimum_shield_area_m2=3_000_000.0,
        minimum_core_area_m2=2_000_000.0,
        shield_position_sigma_m=500.0,
        core_position_sigma_m=350.0,
        connectivity=8,
        centroid_method="geometric",
    )


def _compose(
    evidence,
    receipt,
    authority,
    *,
    target_grid,
    transform_config=None,
    pipeline_config=None,
    detector_config=None,
    deadline_at=20.0,
    monotonic=lambda: 10.0,
):
    semantics = _semantics(receipt)
    transform_config = transform_config or AnalysisGridTransformConfig(
        max_source_pixels=10_000,
        max_target_pixels=10_000,
        max_array_bytes=4 * 1024 * 1024,
        max_overlap_contributions=100_000,
    )
    return compose_evidence_to_cells_shadow(
        evidence,
        authority,
        run_id=receipt.run_id,
        radar_semantics_certificates=(semantics,),
        target_grid=target_grid,
        transform_adapter=_transform(
            receipt,
            semantics,
            transform_config,
            width=receipt.run.native_grid.width,
            height=receipt.run.native_grid.height,
        ),
        transform_config=transform_config,
        detector_config=detector_config or _detector(),
        pipeline_config=pipeline_config or EvidenceToCellsShadowConfig(),
        deadline_at=deadline_at,
        monotonic=monotonic,
    )


def test_committed_run_to_analysis_grid_to_lossless_cells(tmp_path):
    evidence, receipt = _commit(tmp_path, width=2, height=2)
    authority = SyntheticAuthority(
        receipt,
        values=[[0.0, 2.0], [5.0, 99.0]],
        valid=[[1, 1], [1, 0]],
        nodata=[[0, 0], [0, 1]],
        undetect=[[0, 0], [0, 0]],
    )
    result = _compose(
        evidence,
        receipt,
        authority,
        target_grid=_grid(width=2, height=2),
    )

    assert result.is_candidate
    candidate = result.candidate
    assert candidate is not None
    assert candidate.canonical_run_id == receipt.run_id
    assert candidate.candidate_id == "evidence-cells-" + candidate.content_sha256
    assert evidence_to_cells_content_sha256(candidate) == candidate.content_sha256
    assert candidate.admission_status == EVIDENCE_TO_CELLS_ADMISSION_STATUS
    assert not candidate.meteorological_admission_granted
    assert not candidate.wet_dry_classification_applied
    assert not candidate.forecast_generated
    assert not candidate.hard_realtime_deadline_certified
    assert candidate.operational_admission_blockers == (
        "hard_process_isolation_not_wired",
    )
    assert candidate.normalized_frame.values == ((0.0, 2.0), (5.0, None))
    assert len(candidate.detection_frame.rain_shields) == 1
    assert len(candidate.detection_frame.embedded_cores) == 1
    assert candidate.pixel_accounting.total_analysis_pixels == 4
    assert candidate.pixel_accounting.valid_detector_input_pixels == 3
    assert candidate.pixel_accounting.valid_below_shield_threshold_pixels == 1
    assert candidate.pixel_accounting.rain_shield_pixels == 2
    assert candidate.pixel_accounting.embedded_core_pixels == 1
    assert candidate.pixel_accounting.excluded_nodata_pixels == 1
    assert candidate.pixel_accounting.low_confidence_objects == 2
    assert len(candidate.detection_frame.low_confidence_fragments) == 2
    assert candidate.pixels[1][1].disposition is PixelDisposition.EXCLUDED_NODATA
    assert candidate.pixels[1][1].rain_rate_mm_h is None
    assert candidate.pixels[0][0].disposition is PixelDisposition.VALID_BELOW_SHIELD
    assert candidate.pixels[0][0].rain_rate_mm_h == 0.0
    assert "candidate_only_unadmitted" in candidate.analysis_lineage_json
    assert receipt.receipt_sha256 in candidate.native_lineage_json


def test_concrete_worker_composer_binds_real_chain_with_stable_source(tmp_path):
    evidence, receipt = _commit(tmp_path, width=2, height=2)
    authority = SyntheticAuthority(
        receipt,
        values=[[0.0, 2.0], [5.0, 99.0]],
        valid=[[1, 1], [1, 0]],
        nodata=[[0, 0], [0, 1]],
        undetect=[[0, 0], [0, 0]],
    )
    semantics = _semantics(receipt)
    transform_config = AnalysisGridTransformConfig(
        max_source_pixels=10_000,
        max_target_pixels=10_000,
        max_array_bytes=4 * 1024 * 1024,
        max_overlap_contributions=100_000,
    )
    composer = CanonicalEvidenceCellComposer(
        evidence,
        authority,
        radar_semantics_certificates=(semantics,),
        target_grid=_grid(width=2, height=2),
        transform_adapter=_transform(
            receipt,
            semantics,
            transform_config,
            width=2,
            height=2,
        ),
        transform_config=transform_config,
        detector_config=_detector(),
        pipeline_config=EvidenceToCellsShadowConfig(),
    )

    result = composer.compose(
        run_id=receipt.run_id,
        deadline_at=20.0,
        monotonic=lambda: 10.0,
    )

    assert result.is_candidate
    bound = result.evidence
    assert bound is not None
    assert bound.canonical_run_id == receipt.run_id
    assert bound.pixel_accounting.total_analysis_pixels == 4
    assert bound.detection_frame.frame_id == bound.normalized_frame.frame_id
    assert len(bound.detection_frame.detections) == (
        bound.pixel_accounting.detection_objects
    )
    assert bound.normalized_frame.source == (
        "canonical:synthetic-radar/synthetic_rate_capture:semantics:"
        + semantics.fingerprint
        + ":transform:"
        + bound.transform_certificate_sha256
    )
    assert bound.analysis_field_id not in bound.normalized_frame.source
    assert composer.profile_sha256 != semantics.fingerprint


def test_every_exclusion_state_and_fraction_remains_explicit(tmp_path):
    evidence, receipt = _commit(tmp_path, width=8, height=1)
    authority = SyntheticAuthority(
        receipt,
        values=[[0.0, 2.0, 4.0, 99.0, 99.0, 99.0, 99.0, 99.0]],
        valid=[[1, 1, 1, 0, 0, 0, 0, 0]],
        nodata=[[0, 0, 0, 1, 1, 1, 0, 0]],
        undetect=[[0, 0, 0, 0, 0, 0, 1, 1]],
    )
    result = _compose(
        evidence,
        receipt,
        authority,
        target_grid=_grid(width=5, height=1, x_resolution=2000.0),
    )
    candidate = result.candidate
    assert candidate is not None
    dispositions = tuple(pixel.disposition for pixel in candidate.pixels[0])
    assert dispositions == (
        PixelDisposition.VALID_RAIN_SHIELD,
        PixelDisposition.EXCLUDED_MIXED,
        PixelDisposition.EXCLUDED_NODATA,
        PixelDisposition.EXCLUDED_UNDETECT,
        PixelDisposition.EXCLUDED_OUTSIDE_SUPPORT,
    )
    accounting = candidate.pixel_accounting
    assert accounting.total_analysis_pixels == 5
    assert accounting.valid_detector_input_pixels == 1
    assert accounting.excluded_mixed_pixels == 1
    assert accounting.excluded_nodata_pixels == 1
    assert accounting.excluded_undetect_pixels == 1
    assert accounting.excluded_outside_support_pixels == 1
    mixed = candidate.pixels[0][1]
    assert mixed.rain_rate_mm_h is None
    assert mixed.valid_area_fraction == 0.5
    assert mixed.nodata_area_fraction == 0.5
    outside = candidate.pixels[0][4]
    assert outside.outside_support_area_fraction == 1.0


def test_artifact_binding_tamper_is_withheld_before_transform(tmp_path):
    evidence, receipt = _commit(tmp_path, width=2, height=2)
    authority = SyntheticAuthority(
        receipt,
        values=[[0.0, 2.0], [5.0, 99.0]],
        valid=[[1, 1], [1, 0]],
        nodata=[[0, 0], [0, 1]],
        undetect=[[0, 0], [0, 0]],
        metadata_tamper=True,
    )
    result = _compose(
        evidence,
        receipt,
        authority,
        target_grid=_grid(width=2, height=2),
    )
    assert not result.is_candidate
    assert result.candidate is None
    assert result.issues[0].stage == "radar_bridge"
    assert result.issues[0].code == "artifact_capture_binding_mismatch"


def test_output_identity_recalculation_detects_post_creation_tamper(tmp_path):
    evidence, receipt = _commit(tmp_path, width=2, height=2)
    authority = SyntheticAuthority(
        receipt,
        values=[[0.0, 2.0], [5.0, 99.0]],
        valid=[[1, 1], [1, 0]],
        nodata=[[0, 0], [0, 1]],
        undetect=[[0, 0], [0, 0]],
    )
    candidate = _compose(
        evidence,
        receipt,
        authority,
        target_grid=_grid(width=2, height=2),
    ).candidate
    assert candidate is not None
    original = candidate.content_sha256
    object.__setattr__(candidate.pixel_accounting, "rain_shield_pixels", 99)
    assert evidence_to_cells_content_sha256(candidate) != original


def test_output_identity_binds_normalized_detector_values(tmp_path):
    evidence, receipt = _commit(tmp_path, width=2, height=2)
    authority = SyntheticAuthority(
        receipt,
        values=[[0.0, 2.0], [5.0, 99.0]],
        valid=[[1, 1], [1, 0]],
        nodata=[[0, 0], [0, 1]],
        undetect=[[0, 0], [0, 0]],
    )
    candidate = _compose(
        evidence,
        receipt,
        authority,
        target_grid=_grid(width=2, height=2),
    ).candidate
    assert candidate is not None
    original = candidate.content_sha256
    object.__setattr__(
        candidate.normalized_frame,
        "values",
        ((9.0, 2.0), (5.0, None)),
    )
    assert evidence_to_cells_content_sha256(candidate) != original


def test_public_candidate_constructor_rejects_a_forged_content_identity(tmp_path):
    evidence, receipt = _commit(tmp_path, width=2, height=2)
    authority = SyntheticAuthority(
        receipt,
        values=[[0.0, 2.0], [5.0, 99.0]],
        valid=[[1, 1], [1, 0]],
        nodata=[[0, 0], [0, 1]],
        undetect=[[0, 0], [0, 0]],
    )
    candidate = _compose(
        evidence,
        receipt,
        authority,
        target_grid=_grid(width=2, height=2),
    ).candidate
    assert candidate is not None
    with pytest.raises(ValueError, match="content checksum"):
        replace(
            candidate,
            candidate_id="evidence-cells-" + "0" * 64,
            content_sha256="0" * 64,
        )


def test_transform_receipt_cannot_be_rebound_to_another_native_candidate(tmp_path):
    evidence, receipt = _commit(tmp_path, width=2, height=2)
    authority = SyntheticAuthority(
        receipt,
        values=[[0.0, 2.0], [5.0, 99.0]],
        valid=[[1, 1], [1, 0]],
        nodata=[[0, 0], [0, 1]],
        undetect=[[0, 0], [0, 0]],
    )
    candidate = _compose(
        evidence,
        receipt,
        authority,
        target_grid=_grid(width=2, height=2),
    ).candidate
    assert candidate is not None
    document = json.loads(candidate.transform_receipt_json)
    document["source_candidate_id"] = "radar-field-" + "0" * 64
    encoded = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    digest = hashlib.sha256(encoded.encode("ascii")).hexdigest()
    with pytest.raises(ValueError, match="cross-bound"):
        replace(
            candidate,
            transform_receipt_json=encoded,
            transform_receipt_sha256=digest,
            transform_receipt_id="transform-receipt-" + digest,
        )


def test_detection_frame_semantics_are_bound_to_input_and_detector_config(tmp_path):
    evidence, receipt = _commit(tmp_path, width=2, height=2)
    authority = SyntheticAuthority(
        receipt,
        values=[[0.0, 2.0], [5.0, 99.0]],
        valid=[[1, 1], [1, 0]],
        nodata=[[0, 0], [0, 1]],
        undetect=[[0, 0], [0, 0]],
    )
    candidate = _compose(
        evidence,
        receipt,
        authority,
        target_grid=_grid(width=2, height=2),
    ).candidate
    assert candidate is not None
    contradictory = replace(candidate.detection_frame, quantity="other_quantity")
    with pytest.raises(ValueError, match="detector frame semantics"):
        replace(candidate, detection_frame=contradictory)


def test_absolute_deadline_and_capacity_fail_before_provider_work(tmp_path):
    evidence, receipt = _commit(tmp_path, width=2, height=2)
    authority = SyntheticAuthority(
        receipt,
        values=[[0.0, 2.0], [5.0, 99.0]],
        valid=[[1, 1], [1, 0]],
        nodata=[[0, 0], [0, 1]],
        undetect=[[0, 0], [0, 0]],
    )
    with pytest.raises(EvidenceToCellsShadowDeadlineError):
        _compose(
            evidence,
            receipt,
            authority,
            target_grid=_grid(width=2, height=2),
            deadline_at=10.0,
            monotonic=lambda: 10.0,
        )
    assert authority.calls == 0
    zero_threshold = replace(_detector(), shield_threshold=0.0)
    with pytest.raises(ValueError, match="strictly positive"):
        _compose(
            evidence,
            receipt,
            authority,
            target_grid=_grid(width=2, height=2),
            detector_config=zero_threshold,
        )
    assert authority.calls == 0
    with pytest.raises(EvidenceToCellsShadowCapacityError):
        _compose(
            evidence,
            receipt,
            authority,
            target_grid=_grid(width=2, height=2),
            pipeline_config=EvidenceToCellsShadowConfig(max_target_pixels=3),
        )
    assert authority.calls == 0
    transform_config = AnalysisGridTransformConfig(
        max_source_pixels=10_000,
        max_target_pixels=1,
        max_array_bytes=4 * 1024 * 1024,
        max_overlap_contributions=100_000,
    )
    with pytest.raises(EvidenceToCellsShadowCapacityError, match="transform"):
        _compose(
            evidence,
            receipt,
            authority,
            target_grid=_grid(width=2, height=2),
            transform_config=transform_config,
        )
    assert authority.calls == 0


def test_same_absolute_deadline_is_enforced_after_provider_io(tmp_path):
    evidence, receipt = _commit(tmp_path, width=2, height=2)
    clock = {"now": 10.0}
    authority = SyntheticAuthority(
        receipt,
        values=[[0.0, 2.0], [5.0, 99.0]],
        valid=[[1, 1], [1, 0]],
        nodata=[[0, 0], [0, 1]],
        undetect=[[0, 0], [0, 0]],
        after_load=lambda: clock.update(now=20.0),
    )
    with pytest.raises(EvidenceToCellsShadowDeadlineError):
        _compose(
            evidence,
            receipt,
            authority,
            target_grid=_grid(width=2, height=2),
            deadline_at=20.0,
            monotonic=lambda: clock["now"],
        )
    assert authority.calls == 1


def test_detection_and_receipt_capacity_are_explicit(tmp_path):
    evidence, receipt = _commit(tmp_path, width=2, height=2)
    authority = SyntheticAuthority(
        receipt,
        values=[[0.0, 2.0], [5.0, 99.0]],
        valid=[[1, 1], [1, 0]],
        nodata=[[0, 0], [0, 1]],
        undetect=[[0, 0], [0, 0]],
    )
    with pytest.raises(EvidenceToCellsShadowCapacityError, match="detector objects"):
        _compose(
            evidence,
            receipt,
            authority,
            target_grid=_grid(width=2, height=2),
            pipeline_config=EvidenceToCellsShadowConfig(max_detection_objects=1),
        )
    assert authority.calls == 0
    with pytest.raises(EvidenceToCellsShadowCapacityError, match="receipt"):
        _compose(
            evidence,
            receipt,
            authority,
            target_grid=_grid(width=2, height=2),
            pipeline_config=EvidenceToCellsShadowConfig(max_receipt_bytes=1_024),
        )

def test_compositor_has_no_loose_array_ingress_and_is_not_wired():
    with pytest.raises(TypeError):
        compose_evidence_to_cells_shadow(  # type: ignore[call-arg]
            None,
            None,
            run_id="run-" + "0" * 64,
            rain_rate_mm_h=np.zeros((1, 1)),
        )
    repository = Path(__file__).resolve().parents[1]
    for relative in (
        "storm_tracker_worker/run.py",
        "storm_tracker_worker/service.py",
        "storm_tracker_worker/config.py",
        "storm_tracker_worker/provider_capture_registry.py",
        "custom_components/storm_tracker_v3/__init__.py",
    ):
        path = repository / relative
        if path.exists():
            assert "evidence_to_cells_shadow" not in path.read_text(
                encoding="utf-8"
            )
