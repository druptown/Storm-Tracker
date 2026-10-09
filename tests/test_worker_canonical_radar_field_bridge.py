from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

from storm_tracker_worker.canonical_evidence_store import (
    CanonicalEvidenceStore,
    EVIDENCE_CONTRACT,
)
from storm_tracker_worker.canonical_radar_field_bridge import (
    AnalysisGridMapping,
    HARD_MAX_NATIVE_PIXELS,
    NativeRasterPlane,
    RADAR_FIELD_ADMISSION_STATUS,
    RadarArtifactAuthorityError,
    RadarFieldBridgeCapacityError,
    RadarFieldBridgeDeadlineError,
    RadarFieldSemanticsCertificate,
    RadarTemporalSemantics,
    VerifiedProviderRaster,
    default_radar_semantics_certificates,
    derive_canonical_radar_field,
)
from storm_tracker_worker.native_capture import (
    NativeCaptureMemberInput,
    capture_native_set,
)
from storm_tracker_worker.raw_artifacts import RawArtifactStore
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid


PRODUCT_AT = 1_800_000_300.0
RECEIVED_AT = PRODUCT_AT + 10.0
EVALUATED_AT = PRODUCT_AT + 20.0
COMMITTED_AT = PRODUCT_AT + 30.0
CONFIG_SHA = "c" * 64
GRID_SHA = "d" * 64
COVERAGE_SHA = "e" * 64
ARTIFACT_SHA = "f" * 64


def _raw_store(tmp_path: Path, *, provider: str = "synthetic-radar"):
    store = RawArtifactStore((tmp_path / f"raw-{provider}").resolve())
    store.initialize()
    capture = capture_native_set(
        store,
        provider=provider,
        capture_product=f"{provider}_capture",
        fetch_id=f"{provider}:field",
        upstream_capture_id=f"{provider}/field",
        product_timestamp=PRODUCT_AT,
        collector="canonical-radar-field-test",
        collector_version="test-v1",
        members=(
            NativeCaptureMemberInput(
                role="native_raster",
                product=f"{provider}_native",
                content_type="application/octet-stream",
                received_at=RECEIVED_AT,
                upstream_id=f"{provider}/native.bin",
                payload=b"exact-native-raster-bytes",
                provenance={"semantic_class": "quantitative_precipitation"},
            ),
        ),
    ).capture
    return store, capture


def _members(capture) -> list[dict]:
    return [
        {
            "content_sha256": member.content_sha256,
            "member_id": member.member_id,
            "receipt_id": member.receipt_id,
            "receipt_sha256": member.receipt_sha256,
            "received_at": member.received_at,
            "role": member.role,
        }
        for member in capture.members
    ]


def _commit(
    tmp_path: Path,
    *,
    provider: str = "synthetic-radar",
    decoder_identity: str = "synthetic-rate-decoder",
    decoder_version: str = "decoder-v1",
    quantity: str = "instantaneous_precipitation_rate",
    unit: str = "mm/h",
    width: int = 2,
    height: int = 2,
):
    raw_store, capture = _raw_store(tmp_path, provider=provider)
    evidence = CanonicalEvidenceStore(
        (tmp_path / f"evidence-{provider}.sqlite3").resolve(),
        raw_artifact_reader=raw_store,
        clock=lambda: COMMITTED_AT,
    )
    run = {
        "attempt_id": "decoder-attempt-" + "b" * 64,
        "capture": {
            "capture_id": capture.capture_id,
            "capture_receipt_sha256": capture.capture_receipt_sha256,
            "members": _members(capture),
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
            "identity": decoder_identity,
            "version": decoder_version,
        },
        "decoder_job_id": "decoder-job-" + "a" * 64,
        "evaluated_at": EVALUATED_AT,
        "native_crs": "EPSG:3857",
        "native_grid": {
            "axis_order": "yx",
            "definition_sha256": GRID_SHA,
            "height": height,
            "kind": "regular",
            "width": width,
        },
        "non_operational_candidate_artifact": {
            "handle": f"artifact:synthetic-rate:{ARTIFACT_SHA}",
            "sha256": ARTIFACT_SHA,
        },
        "outcome": "decoded_complete",
        "product": capture.product,
        "product_time": capture.product_timestamp,
        "provider": capture.provider,
        "quantity": quantity,
        "reason_code": "decoded_complete",
        "received_at": RECEIVED_AT,
        "unit": unit,
    }
    return raw_store, evidence, evidence.commit(run)


def _certificate(
    receipt,
    *,
    temporal: RadarTemporalSemantics = RadarTemporalSemantics.INSTANTANEOUS_RATE_ESTIMATE,
    seconds: int | None = None,
    analysis: AnalysisGridMapping = AnalysisGridMapping.METRIC_BBOX_EDGES,
):
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
        output_quantity=(
            "interval_mean_precipitation_rate"
            if temporal is RadarTemporalSemantics.INTERVAL_MEAN_RATE
            else str(run.quantity)
        ),
        output_unit="mm/h",
        temporal_semantics=temporal,
        exact_accumulation_seconds=seconds,
        analysis_grid_mapping=analysis,
        qc_policy="retain_test_qc",
        primary_sources=("https://example.invalid/primary-spec",),
    )


def _readonly(values, dtype=None):
    result = np.asarray(values, dtype=dtype).copy()
    result.setflags(write=False)
    return result


class FakeAuthority:
    def __init__(
        self,
        receipt,
        *,
        values=None,
        valid=None,
        nodata=None,
        undetect=None,
        support_start: float = PRODUCT_AT,
        support_end: float = PRODUCT_AT + 600.0,
        representative_at: float = PRODUCT_AT,
        metadata_changes: dict | None = None,
        fail: bool = False,
        analysis_grid: AnalysisGrid | None = None,
    ) -> None:
        self.receipt = receipt
        self.calls = 0
        self.values = _readonly(
            [[0.0, 1.5], [2.0, 99.0]] if values is None else values,
            np.float64,
        )
        self.valid = _readonly(
            [[1, 1], [1, 0]] if valid is None else valid,
            np.uint8,
        )
        self.nodata = _readonly(
            [[0, 0], [0, 1]] if nodata is None else nodata,
            np.uint8,
        )
        self.undetect = _readonly(
            [[0, 0], [0, 0]] if undetect is None else undetect,
            np.uint8,
        )
        self.support_start = support_start
        self.support_end = support_end
        self.representative_at = representative_at
        self.metadata_changes = metadata_changes or {}
        self.fail = fail
        self.analysis_grid = analysis_grid or AnalysisGrid(
            grid_id=f"native:{receipt.run.provider}:{GRID_SHA}",
            crs="EPSG:3857",
            origin_x_m=0.0,
            origin_y_m=2000.0,
            x_resolution_m=1000.0,
            y_resolution_m=1000.0,
            width=2,
            height=2,
        )

    def load_verified(self, receipt, certificate, *, deadline_at, monotonic):
        self.calls += 1
        if self.fail:
            raise RadarArtifactAuthorityError("synthetic artifact tamper")
        metadata = {
            "capture": {
                "capture_id": receipt.run.capture_id,
                "capture_receipt_sha256": receipt.run.capture_receipt_sha256,
                "manifest_sha256": receipt.capture_manifest_sha256,
                "product": receipt.run.product,
                "product_timestamp": receipt.run.product_time,
                "provider": receipt.run.provider,
            },
            "decoder": {
                "config_sha256": receipt.run.config_sha256,
                "identity": receipt.run.decoder_identity,
                "version": receipt.run.decoder_version,
            },
            "grid": {
                "columns": receipt.run.native_grid.width,
                "coverage": {
                    "maximum_x": receipt.run.coverage.maximum_x,
                    "maximum_y": receipt.run.coverage.maximum_y,
                    "minimum_x": receipt.run.coverage.minimum_x,
                    "minimum_y": receipt.run.coverage.minimum_y,
                },
                "coverage_definition_sha256": receipt.run.coverage.definition_sha256,
                "grid_definition_sha256": receipt.run.native_grid.definition_sha256,
                "native_crs": receipt.run.native_crs,
                "rows": receipt.run.native_grid.height,
            },
            "product": {"quantity": receipt.run.quantity, "unit": receipt.run.unit},
            "schema": "stv3-synthetic-raster-v1",
        }
        for key, value in self.metadata_changes.items():
            metadata[key] = value
        return VerifiedProviderRaster(
            artifact_schema="stv3-synthetic-raster-v1",
            artifact_metadata=metadata,
            source_values=self.values,
            valid_mask=self.valid,
            nodata_mask=self.nodata,
            undetect_mask=self.undetect,
            auxiliary_planes=(
                NativeRasterPlane(
                    name="quality_native",
                    semantics="retained_not_interpreted",
                    values=_readonly([[0, 1], [2, 3]], np.uint8),
                ),
            ),
            support_start=self.support_start,
            support_end=self.support_end,
            representative_at=self.representative_at,
            source_quantity=str(receipt.run.quantity),
            source_unit=str(receipt.run.unit),
            quality_state="native_qc_retained_not_applied",
            analysis_grid=self.analysis_grid,
        )


def test_direct_rate_keeps_masks_qc_provenance_and_no_admission(tmp_path) -> None:
    _raw, evidence, receipt = _commit(tmp_path)
    certificate = _certificate(receipt)
    authority = FakeAuthority(receipt)

    result = derive_canonical_radar_field(
        evidence,
        authority,
        run_id=receipt.run_id,
        certificates=(certificate,),
    )

    assert result.is_candidate is True
    assert result.issues == ()
    candidate = result.candidate
    assert candidate is not None
    assert authority.calls == 1
    assert candidate.admission_status == RADAR_FIELD_ADMISSION_STATUS
    assert candidate.canonical_receipt_sha256 == receipt.receipt_sha256
    assert candidate.capture_manifest_sha256 == receipt.capture_manifest_sha256
    assert candidate.artifact_sha256 == ARTIFACT_SHA
    assert candidate.semantics_certificate_sha256 == certificate.fingerprint
    assert candidate.conversion == "identity_mm_h"
    assert candidate.value_or_none(0, 0) == 0.0
    assert candidate.value_or_none(1, 1) is None
    assert np.isnan(candidate.rain_rate_mm_h[1, 1])
    assert candidate.nodata_mask[1, 1]
    assert not candidate.valid_mask[1, 1]
    assert candidate.auxiliary_planes[0].name == "quality_native"
    assert candidate.quality_state == "native_qc_retained_not_applied"
    assert candidate.metric_raster_field is None
    assert candidate.reprojection_applied is False
    assert candidate.resampling_applied is False
    assert candidate.thresholding_applied is False
    assert candidate.wet_dry_classification_applied is False


def test_explicit_small_metric_field_materialization_retains_mask(tmp_path) -> None:
    _raw, evidence, receipt = _commit(tmp_path)
    certificate = _certificate(receipt)

    result = derive_canonical_radar_field(
        evidence,
        FakeAuthority(receipt),
        run_id=receipt.run_id,
        certificates=(certificate,),
        materialize_metric_field=True,
        max_metric_field_pixels=4,
    )

    candidate = result.candidate
    assert candidate is not None
    field = candidate.metric_raster_field
    assert field is not None
    assert field.value_or_none(1, 1) is None
    assert field.values[1][1] == 0.0
    assert field.valid_mask[1][1] is False
    assert field.value_or_none(0, 1) == 1.5


def test_exact_accumulation_becomes_interval_mean_not_instantaneous(tmp_path) -> None:
    _raw, evidence, receipt = _commit(
        tmp_path,
        quantity="precipitation_accumulation_5_minute",
        unit="mm",
    )
    certificate = _certificate(
        receipt,
        temporal=RadarTemporalSemantics.INTERVAL_MEAN_RATE,
        seconds=300,
    )
    authority = FakeAuthority(
        receipt,
        values=[[0.0, 1.0], [2.0, 9.0]],
        support_start=PRODUCT_AT - 300.0,
        support_end=PRODUCT_AT,
        representative_at=PRODUCT_AT,
    )

    result = derive_canonical_radar_field(
        evidence,
        authority,
        run_id=receipt.run_id,
        certificates=(certificate,),
    )

    candidate = result.candidate
    assert candidate is not None
    assert candidate.temporal_semantics is RadarTemporalSemantics.INTERVAL_MEAN_RATE
    assert candidate.output_quantity == "interval_mean_precipitation_rate"
    assert candidate.rain_rate_mm_h[0, 1] == 12.0
    assert candidate.rain_rate_mm_h[1, 0] == 24.0
    assert candidate.support_end - candidate.support_start == 300.0
    assert "3600/300" in candidate.conversion


def test_accumulation_conversion_is_withheld_when_interval_differs(tmp_path) -> None:
    _raw, evidence, receipt = _commit(
        tmp_path,
        quantity="precipitation_accumulation_5_minute",
        unit="mm",
    )
    certificate = _certificate(
        receipt,
        temporal=RadarTemporalSemantics.INTERVAL_MEAN_RATE,
        seconds=300,
    )
    result = derive_canonical_radar_field(
        evidence,
        FakeAuthority(
            receipt,
            support_start=PRODUCT_AT - 299.0,
            support_end=PRODUCT_AT,
            representative_at=PRODUCT_AT,
        ),
        run_id=receipt.run_id,
        certificates=(certificate,),
    )

    assert result.candidate is None
    assert {issue.code for issue in result.issues} == {
        "accumulation_interval_not_exact"
    }


def test_certificate_config_drift_fails_before_artifact_read(tmp_path) -> None:
    _raw, evidence, receipt = _commit(tmp_path)
    authority = FakeAuthority(receipt)
    drifted = replace(_certificate(receipt), decoder_config_sha256="9" * 64)

    result = derive_canonical_radar_field(
        evidence,
        authority,
        run_id=receipt.run_id,
        certificates=(drifted,),
    )

    assert result.candidate is None
    assert authority.calls == 0
    assert result.issues[0].code == "semantics_certificate_missing_or_drifted"
    assert "decoder_config_sha256" in result.issues[0].detail


def test_opera_dbzh_is_hard_excluded_even_with_matching_certificate(tmp_path) -> None:
    _raw, evidence, receipt = _commit(
        tmp_path,
        provider="opera",
        decoder_identity="opera-cirrus-dbzh-qi-bounded-region",
        quantity="equivalent_reflectivity_factor",
        unit="dBZ",
    )
    certificate = _certificate(receipt, analysis=AnalysisGridMapping.NONE)
    authority = FakeAuthority(receipt, analysis_grid=None)

    result = derive_canonical_radar_field(
        evidence,
        authority,
        run_id=receipt.run_id,
        certificates=(certificate,),
    )

    assert result.candidate is None
    assert authority.calls == 0
    assert result.issues[0].code == "reflectivity_requires_uncertified_zr"


def test_raw_tamper_fails_before_derived_artifact_read(tmp_path) -> None:
    raw, evidence, receipt = _commit(tmp_path)
    authority = FakeAuthority(receipt)
    raw_receipt = raw.load_receipt(receipt.run.members[0].receipt_id)
    assert raw_receipt is not None
    Path(raw_receipt.artifact_path).write_bytes(b"tampered-native-raster")

    result = derive_canonical_radar_field(
        evidence,
        authority,
        run_id=receipt.run_id,
        certificates=(_certificate(receipt),),
    )

    assert result.candidate is None
    assert authority.calls == 0
    assert result.issues[0].code == "canonical_raw_reverification_failed"


def test_artifact_binding_retains_all_detected_mismatch_reasons(tmp_path) -> None:
    _raw, evidence, receipt = _commit(tmp_path)
    authority = FakeAuthority(
        receipt,
        metadata_changes={
            "capture": {
                "capture_id": "capture-" + "0" * 64,
                "capture_receipt_sha256": "1" * 64,
                "manifest_sha256": "2" * 64,
                "product": "wrong",
                "product_timestamp": PRODUCT_AT + 1,
                "provider": "wrong",
            },
            "decoder": {
                "config_sha256": "3" * 64,
                "identity": "wrong",
                "version": "wrong",
            },
            "grid": {
                "columns": 2,
                "coverage": {
                    "maximum_x": 1999.0,
                    "maximum_y": 2000.0,
                    "minimum_x": 0.0,
                    "minimum_y": 0.0,
                },
                "coverage_definition_sha256": "4" * 64,
                "grid_definition_sha256": "5" * 64,
                "rows": 2,
            },
        },
    )

    result = derive_canonical_radar_field(
        evidence,
        authority,
        run_id=receipt.run_id,
        certificates=(_certificate(receipt),),
    )

    assert result.candidate is None
    assert {
        "artifact_decoder_binding_mismatch",
        "artifact_capture_binding_mismatch",
        "artifact_provider_binding_mismatch",
        "artifact_product_binding_mismatch",
        "artifact_product_time_binding_mismatch",
        "artifact_grid_binding_mismatch",
        "artifact_coverage_bounds_mismatch",
    }.issubset({issue.code for issue in result.issues})


def test_non_exhaustive_masks_are_invalid_not_dry(tmp_path) -> None:
    _raw, evidence, receipt = _commit(tmp_path)
    result = derive_canonical_radar_field(
        evidence,
        FakeAuthority(
            receipt,
            valid=[[1, 1], [1, 1]],
            nodata=[[0, 0], [0, 1]],
            undetect=[[0, 0], [0, 0]],
        ),
        run_id=receipt.run_id,
        certificates=(_certificate(receipt),),
    )

    assert result.candidate is None
    assert "artifact_masks_not_exhaustive" in {
        issue.code for issue in result.issues
    }


def test_artifact_failure_is_retained_without_partial_candidate(tmp_path) -> None:
    _raw, evidence, receipt = _commit(tmp_path)
    result = derive_canonical_radar_field(
        evidence,
        FakeAuthority(receipt, fail=True),
        run_id=receipt.run_id,
        certificates=(_certificate(receipt),),
    )

    assert result.candidate is None
    assert result.issues[0].code == "derived_artifact_verification_failed"


def test_expired_deadline_prevents_raw_and_artifact_work(tmp_path) -> None:
    _raw, evidence, receipt = _commit(tmp_path)
    authority = FakeAuthority(receipt)

    with pytest.raises(RadarFieldBridgeDeadlineError):
        derive_canonical_radar_field(
            evidence,
            authority,
            run_id=receipt.run_id,
            certificates=(_certificate(receipt),),
            deadline_at=10.0,
            monotonic=lambda: 10.0,
        )
    assert authority.calls == 0


def test_native_pixel_bound_fails_closed(tmp_path) -> None:
    width = 2001
    height = 2000
    assert width * height > HARD_MAX_NATIVE_PIXELS
    _raw, evidence, receipt = _commit(
        tmp_path,
        width=width,
        height=height,
    )
    authority = FakeAuthority(receipt)

    with pytest.raises(RadarFieldBridgeCapacityError):
        derive_canonical_radar_field(
            evidence,
            authority,
            run_id=receipt.run_id,
            certificates=(_certificate(receipt),),
        )


def test_metric_materialization_has_independent_hard_bound(tmp_path) -> None:
    _raw, evidence, receipt = _commit(tmp_path)
    with pytest.raises(RadarFieldBridgeCapacityError):
        derive_canonical_radar_field(
            evidence,
            FakeAuthority(receipt),
            run_id=receipt.run_id,
            certificates=(_certificate(receipt),),
            materialize_metric_field=True,
            max_metric_field_pixels=3,
        )


def test_default_certificates_are_exact_and_opera_is_absent() -> None:
    certificates = default_radar_semantics_certificates()

    assert len(certificates) == 4
    assert len({item.fingerprint for item in certificates}) == 4
    assert {item.provider for item in certificates} == {
        "dwd_radolan",
        "knmi",
        "met_office",
        "noaa_enterprise_rainrate",
    }
    assert all(item.provider != "opera" for item in certificates)
    dwd = next(item for item in certificates if item.provider == "dwd_radolan")
    knmi = next(item for item in certificates if item.provider == "knmi")
    noaa = next(
        item for item in certificates
        if item.provider == "noaa_enterprise_rainrate"
    )
    met_office = next(
        item for item in certificates if item.provider == "met_office"
    )
    assert dwd.exact_accumulation_seconds == 300
    assert dwd.analysis_grid_mapping is AnalysisGridMapping.NONE
    assert knmi.exact_accumulation_seconds == 300
    assert noaa.temporal_semantics is RadarTemporalSemantics.INSTANTANEOUS_RATE_ESTIMATE
    assert met_office.qc_policy == "quality_unavailable_must_remain_visible"
