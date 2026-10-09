from __future__ import annotations

from dataclasses import replace
import hashlib
import json

import numpy as np
import pytest
from pyproj import Transformer

from storm_tracker_worker.analysis_grid_transform import (
    AnalysisGridTransformConfig,
    NativeFieldCandidateBinding,
    native_candidate_content_sha256,
)
from storm_tracker_worker.canonical_evidence_store import CoverageSpec, NativeGridSpec
from storm_tracker_worker.canonical_radar_field_bridge import (
    AnalysisGridMapping,
    NativeRainRateFieldCandidate,
    NativeRasterPlane,
    RADAR_FIELD_BRIDGE_ALGORITHM,
    default_radar_semantics_certificates,
)
from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker.dwd_rv_analysis_grid_certificate import (
    SOURCE_EAST_EDGE_M,
    SOURCE_HEIGHT,
    SOURCE_NORTH_EDGE_M,
    SOURCE_SOUTH_EDGE_M,
    SOURCE_WEST_EDGE_M,
    SOURCE_WIDTH,
    issue_dwd_rv_native_grid_certificate,
)
from storm_tracker_worker.multi_source_radar_facts import (
    RADAR_PROVIDER_PROFILES,
    RADAR_TARGET_BATCH_CONTRACT,
    RADAR_TARGET_BATCH_PROCESS_RECEIPT_CONTRACT,
    RADAR_TARGET_NO_SAMPLE_METHOD,
    RADAR_TARGET_SAMPLING_METHOD,
    RadarClaimStatus,
    RadarCoverageState,
    RadarObservationState,
    RadarTargetFactsError,
    VerifiedRadarFieldSource,
    build_radar_target_contribution_batch,
)
from storm_tracker_worker.multi_source_radar_snapshot import (
    WithheldDryRadarTargetObservation,
    WithheldUndetectRadarTargetObservation,
    radar_observations_from_radar_batch,
)
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid


VALID_AT = 1_800_000_300.0
RECEIVED_AT = VALID_AT + 10.0
TARGET_LATITUDE = 51.04
TARGET_LONGITUDE = 4.48
DWD_PROJDEF = (
    "+proj=stere +lat_ts=60 +lat_0=90 +lon_0=10 "
    "+x_0=543196.83521776402 +y_0=3622588.8619310022 "
    "+units=m +a=6378137 +b=6356752.3142451802 +no_defs"
)
DWD_CERTIFICATE_ID = "dwd.rv.accumulation-to-interval-mean-rate.v1"
KNMI_CERTIFICATE_ID = "knmi.rtcor.accumulation-to-interval-mean-rate.v1"
MET_OFFICE_CERTIFICATE_ID = "met-office.odim-rate.v1"
NOAA_CERTIFICATE_ID = "noaa.err.instantaneous-rate.v1"


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _target(
    target_id: str = "zone.home",
    *,
    available: bool = True,
    latitude: float = TARGET_LATITUDE,
    longitude: float = TARGET_LONGITUDE,
) -> dict:
    return {
        "available": available,
        "country_code": "BE",
        "display_name": target_id,
        "kind": "home" if target_id == "zone.home" else "device_tracker",
        "latitude": latitude,
        "location_accuracy_m": None,
        "longitude": longitude,
        "observation_radius_km": 300.0,
        "position_observed_at": VALID_AT - 60.0 if available else None,
        "target_id": target_id,
    }


def _desired_state(*targets: dict):
    return parse_desired_state({
        "authority_epoch": 3,
        "contract": "stv3-worker-desired-state-v1",
        "generation": 7,
        "issued_at": VALID_AT - 30.0,
        "previous_state_sha256": "a" * 64,
        "producer_id": "ha-test-boot",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 150.0,
        },
        "targets": list(targets),
    })


def _readonly(values, dtype=None) -> np.ndarray:
    result = np.asarray(values, dtype=dtype).copy()
    result.setflags(write=False)
    return result


def _profile(certificate_id: str):
    return next(
        item
        for item in RADAR_PROVIDER_PROFILES
        if item.certificate_id == certificate_id
    )


def _qc_planes(
    certificate_id: str,
    shape: tuple[int, int],
) -> tuple[NativeRasterPlane, ...]:
    if certificate_id == DWD_CERTIFICATE_ID:
        values = np.broadcast_to(np.asarray(0, dtype="<u4"), shape)
        return (
            NativeRasterPlane(
                name="raw_values",
                semantics="native_uint32_with_sentinels",
                values=values,
            ),
        )
    planes = []
    for name in _profile(certificate_id).required_qc_planes:
        if name == "quality_index":
            values = _readonly([[0.9, 0.8], [0.7, 0.6]], np.float64)
        elif name == "dqf_raw":
            values = _readonly([[0, 0], [0, 1]], np.int16)
        elif name in {
            "dqf_bad_quality_mask",
            "dqf_degraded_mask",
            "dqf_missing_mask",
            "quality_missing_mask",
            "quality_out_of_image_mask",
        }:
            values = _readonly([[0, 0], [0, 0]], np.uint8)
        else:
            values = _readonly([[1, 1], [1, 1]], np.uint8)
        planes.append(
            NativeRasterPlane(
                name=name,
                semantics=f"retained_test_semantics_for_{name}",
                values=values,
            )
        )
    return tuple(planes)


def _candidate(
    certificate_id: str,
    *,
    rate: float = 2.5,
    pixel_state: str = "valid",
    inside: bool = True,
) -> NativeRainRateFieldCandidate:
    certificate = next(
        item
        for item in default_radar_semantics_certificates()
        if item.certificate_id == certificate_id
    )
    quality_state = _profile(certificate_id).expected_quality_state
    if certificate_id == DWD_CERTIFICATE_ID:
        width = SOURCE_WIDTH
        height = SOURCE_HEIGHT
        native_crs = DWD_PROJDEF
        minimum_x, maximum_x = SOURCE_WEST_EDGE_M, SOURCE_EAST_EDGE_M
        minimum_y, maximum_y = SOURCE_SOUTH_EDGE_M, SOURCE_NORTH_EDGE_M
        analysis_grid = None
    elif certificate.analysis_grid_mapping is AnalysisGridMapping.METRIC_BBOX_EDGES:
        width = height = 2
        native_crs = "EPSG:3857"
        x, y = Transformer.from_crs(
            "EPSG:4326", native_crs, always_xy=True
        ).transform(TARGET_LONGITUDE, TARGET_LATITUDE)
        if inside:
            minimum_x, maximum_x = x - 1_000.0, x + 1_000.0
            minimum_y, maximum_y = y - 1_000.0, y + 1_000.0
        else:
            minimum_x, maximum_x = 0.0, 2_000.0
            minimum_y, maximum_y = 0.0, 2_000.0
        analysis_grid = AnalysisGrid(
            grid_id=f"native:test:{certificate_id}",
            crs=native_crs,
            origin_x_m=minimum_x,
            origin_y_m=maximum_y,
            x_resolution_m=(maximum_x - minimum_x) / width,
            y_resolution_m=(maximum_y - minimum_y) / height,
            width=width,
            height=height,
        )
    elif certificate_id == NOAA_CERTIFICATE_ID:
        width = height = 2
        native_crs = "EPSG:4326"
        if inside:
            minimum_x, maximum_x = 4.45, 4.49
            minimum_y, maximum_y = 51.03, 51.07
        else:
            minimum_x, maximum_x = -180.01, -179.97
            minimum_y, maximum_y = 69.97, 70.01
        analysis_grid = None
    else:
        width = height = 2
        native_crs = "EPSG:4326"
        minimum_x = TARGET_LONGITUDE - 1.0
        maximum_x = TARGET_LONGITUDE + 1.0
        minimum_y = TARGET_LATITUDE - 1.0
        maximum_y = TARGET_LATITUDE + 1.0
        analysis_grid = None
    grid_sha = _sha(f"grid:{certificate_id}:{inside}")
    coverage_sha = _sha(f"coverage:{certificate_id}:{inside}")
    artifact_sha = _sha(f"artifact:{certificate_id}")
    artifact_metadata_sha = _sha(f"artifact-metadata:{certificate_id}")
    canonical_receipt_sha = _sha(f"canonical-receipt:{certificate_id}")
    candidate_digest = hashlib.sha256(_canonical({
        "algorithm": RADAR_FIELD_BRIDGE_ALGORITHM,
        "artifact_metadata_sha256": artifact_metadata_sha,
        "artifact_sha256": artifact_sha,
        "canonical_receipt_sha256": canonical_receipt_sha,
        "certificate_sha256": certificate.fingerprint,
    })).hexdigest()
    shape = (height, width)
    if certificate_id == DWD_CERTIFICATE_ID:
        rates = np.broadcast_to(np.asarray(rate, dtype="<f8"), shape)
        valid = np.broadcast_to(np.asarray(True, dtype=np.bool_), shape)
        nodata = np.broadcast_to(np.asarray(False, dtype=np.bool_), shape)
        undetect = np.broadcast_to(np.asarray(False, dtype=np.bool_), shape)
    else:
        rates = np.asarray([[0.5, 1.0], [1.5, rate]], dtype=np.float64)
        valid = np.ones((2, 2), dtype=np.bool_)
        nodata = np.zeros((2, 2), dtype=np.bool_)
        undetect = np.zeros((2, 2), dtype=np.bool_)
    if pixel_state == "nodata":
        valid[1, 1] = False
        nodata[1, 1] = True
        rates[1, 1] = np.nan
    elif pixel_state == "undetect":
        valid[1, 1] = False
        undetect[1, 1] = True
        rates[1, 1] = np.nan
    elif pixel_state != "valid":
        raise AssertionError(f"unsupported test pixel state: {pixel_state}")
    source_values = rates.copy()
    if certificate.exact_accumulation_seconds is not None:
        source_values *= certificate.exact_accumulation_seconds / 3_600.0
    return NativeRainRateFieldCandidate(
        candidate_id=f"radar-field-{candidate_digest}",
        canonical_run_id="run-" + _sha(f"run:{certificate_id}"),
        canonical_commit_id="commit-" + _sha(f"commit:{certificate_id}"),
        canonical_receipt_sha256=canonical_receipt_sha,
        canonical_result_sha256=_sha(f"canonical-result:{certificate_id}"),
        capture_id="capture-" + _sha(f"capture:{certificate_id}"),
        capture_receipt_sha256=_sha(f"capture-receipt:{certificate_id}"),
        capture_manifest_sha256=_sha(f"capture-manifest:{certificate_id}"),
        artifact_handle=certificate.artifact_handle_prefix + artifact_sha,
        artifact_sha256=artifact_sha,
        artifact_metadata_sha256=artifact_metadata_sha,
        semantics_certificate_id=certificate.certificate_id,
        semantics_certificate_sha256=certificate.fingerprint,
        provider=certificate.provider,
        product=certificate.product,
        decoder_identity=certificate.decoder_identity,
        decoder_version=certificate.decoder_version,
        decoder_config_sha256=certificate.decoder_config_sha256,
        source_quantity=certificate.receipt_quantity,
        source_unit=certificate.receipt_unit,
        source_values=_readonly(source_values, np.float64),
        output_quantity=certificate.output_quantity,
        output_unit=certificate.output_unit,
        rain_rate_mm_h=_readonly(rates, np.float64),
        valid_mask=_readonly(valid, np.bool_),
        nodata_mask=_readonly(nodata, np.bool_),
        undetect_mask=_readonly(undetect, np.bool_),
        auxiliary_planes=_qc_planes(certificate_id, shape),
        temporal_semantics=certificate.temporal_semantics,
        support_start=VALID_AT - 300.0,
        support_end=VALID_AT,
        valid_at=VALID_AT,
        conversion=(
            "identity_mm_h"
            if certificate.exact_accumulation_seconds is None
            else "exact_interval_mean_rate=accumulation_mm*3600/300"
        ),
        quality_state=quality_state,
        native_crs=native_crs,
        native_grid=NativeGridSpec(
            kind="regular",
            width=width,
            height=height,
            axis_order="yx",
            definition_sha256=grid_sha,
        ),
        native_coverage=CoverageSpec(
            kind="native_bbox",
            minimum_x=minimum_x,
            minimum_y=minimum_y,
            maximum_x=maximum_x,
            maximum_y=maximum_y,
            definition_sha256=coverage_sha,
        ),
        analysis_grid=analysis_grid,
        metric_raster_field=None,
        primary_sources=certificate.primary_sources,
    )


def _source(certificate_id: str, **changes) -> VerifiedRadarFieldSource:
    field = _candidate(certificate_id, **changes)
    grid_certificate = None
    if certificate_id == DWD_CERTIFICATE_ID:
        grid_certificate = issue_dwd_rv_native_grid_certificate(
            binding=NativeFieldCandidateBinding(
                candidate=field,
                candidate_content_sha256=native_candidate_content_sha256(field),
                semantics_certificate_sha256=field.semantics_certificate_sha256,
            ),
            target_grid=AnalysisGrid(
                grid_id="dwd-source-grid-for-target-facts-test",
                crs=field.native_crs,
                origin_x_m=SOURCE_WEST_EDGE_M,
                origin_y_m=SOURCE_NORTH_EDGE_M,
                x_resolution_m=1_000.0,
                y_resolution_m=1_000.0,
                width=SOURCE_WIDTH,
                height=SOURCE_HEIGHT,
            ),
            config=AnalysisGridTransformConfig(
                max_source_pixels=2_000_000,
                max_target_pixels=2_000_000,
            ),
        )
    return VerifiedRadarFieldSource(
        field=field,
        received_at=RECEIVED_AT,
        native_grid_certificate=grid_certificate,
    )


def test_exact_four_provider_allowlist_is_bound_to_official_certificates():
    certificates = default_radar_semantics_certificates()
    assert {item.certificate_id for item in certificates} == {
        item.certificate_id for item in RADAR_PROVIDER_PROFILES
    }
    assert {item.workload_id for item in RADAR_PROVIDER_PROFILES} == {
        "radar.dwd.radolan_rv",
        "radar.knmi.rtcor",
        "radar.metoffice.uk_rate",
        "satellite.noaa.err",
    }


def test_all_four_sources_produce_provider_neutral_content_addressed_targets():
    state = _desired_state(
        _target("zone.home"),
        _target("device_tracker.unavailable", available=False),
    )
    batch = build_radar_target_contribution_batch(
        state,
        tuple(_source(item.certificate_id) for item in RADAR_PROVIDER_PROFILES),
    )

    assert batch.source_count == 4
    assert batch.target_count == 2
    assert batch.sample_count == 8
    assert len(batch.contributions) == 8
    assert batch.positive_candidate_count == 3
    assert batch.valid_sample_count == 3
    assert batch.as_dict()["contract"] == RADAR_TARGET_BATCH_CONTRACT
    assert hashlib.sha256(batch.canonical_bytes).hexdigest() == batch.content_sha256
    assert batch.canonical_bytes == _canonical(batch._body())
    assert batch.size_bytes == len(batch.canonical_bytes)
    assert (
        batch._process_receipt_body()["contract"]
        == RADAR_TARGET_BATCH_PROCESS_RECEIPT_CONTRACT
    )
    assert (
        hashlib.sha256(_canonical(batch._process_receipt_body())).hexdigest()
        == batch.process_receipt_sha256
    )
    assert [
        (item.workload_id, item.target_id) for item in batch.contributions
    ] == sorted(
        (item.workload_id, item.target_id) for item in batch.contributions
    )

    home = [item for item in batch.contributions if item.target_id == "zone.home"]
    sampled_home = [
        item for item in home if item.workload_id != "radar.knmi.rtcor"
    ]
    assert all(
        item.observation_state is RadarObservationState.POSITIVE_RAIN_CANDIDATE
        and item.coverage_state is RadarCoverageState.IN_DOMAIN_VALID
        and item.claim_status is RadarClaimStatus.POSITIVE_CANDIDATE_ONLY
        and item.current_rain_candidate
        and item.rain_rate_mm_h == pytest.approx(2.5)
        and item.sampled_row is not None
        and item.sampled_column is not None
        and item.sampling_method == RADAR_TARGET_SAMPLING_METHOD
        and item.full_scan_bound is False
        and item.external_truth_bound is False
        for item in sampled_home
    )
    knmi = next(item for item in home if item.workload_id == "radar.knmi.rtcor")
    assert knmi.observation_state is RadarObservationState.GRID_SEMANTICS_WITHHELD
    assert knmi.coverage_state is RadarCoverageState.GRID_SEMANTICS_WITHHELD
    assert knmi.geometry_state == "withheld_knmi_pixel_edge_authority"
    assert knmi.claim_status is RadarClaimStatus.NO_CLAIM
    assert knmi.rain_rate_mm_h is None
    assert knmi.sampled_row is knmi.sampled_column is None
    assert knmi.sampling_method == RADAR_TARGET_NO_SAMPLE_METHOD
    dwd = next(item for item in home if item.workload_id == "radar.dwd.radolan_rv")
    assert dwd.geometry_state == "certified_dwd_de1200_pixel_edges"
    assert dwd.sampled_row is not None and dwd.sampled_column is not None
    unavailable = [
        item
        for item in batch.contributions
        if item.target_id == "device_tracker.unavailable"
    ]
    assert all(
        item.observation_state is RadarObservationState.TARGET_UNAVAILABLE
        and item.coverage_state is RadarCoverageState.TARGET_UNAVAILABLE
        and item.claim_status is RadarClaimStatus.NO_CLAIM
        and item.rain_rate_mm_h is None
        and item.sampled_row is None
        and item.sampled_column is None
        and item.sampling_method == RADAR_TARGET_NO_SAMPLE_METHOD
        for item in unavailable
    )
    assert all(
        item.contribution_id == f"radar-target-{item.content_sha256}"
        and len(item.source_evidence_sha256) == 64
        and len(item.process_evidence_sha256) == 64
        and len(item.semantics_certificate_sha256) == 64
        for item in batch.contributions
    )
    assert len({item.process_evidence_sha256 for item in batch.contributions}) == 8


def test_zero_and_undetect_are_negative_claims_withheld_fail_closed():
    certificate_id = NOAA_CERTIFICATE_ID
    state = _desired_state(_target())
    zero_batch = build_radar_target_contribution_batch(
        state,
        (_source(certificate_id, rate=0.0),),
    )
    undetect_batch = build_radar_target_contribution_batch(
        state,
        (_source(certificate_id, pixel_state="undetect"),),
    )
    zero = zero_batch.contributions[0]
    undetect = undetect_batch.contributions[0]

    assert zero.observation_state is RadarObservationState.DRY_WITHHELD
    assert zero.coverage_state is RadarCoverageState.IN_DOMAIN_VALID
    assert zero.rain_rate_mm_h == 0.0
    assert undetect.observation_state is RadarObservationState.DRY_WITHHELD
    assert undetect.coverage_state is RadarCoverageState.IN_DOMAIN_UNDETECT
    assert undetect.rain_rate_mm_h is None
    assert zero.claim_status is undetect.claim_status is RadarClaimStatus.NEGATIVE_WITHHELD
    assert not zero.current_rain_candidate
    assert not undetect.current_rain_candidate
    assert not zero.full_scan_bound and not zero.external_truth_bound
    projected_zero = radar_observations_from_radar_batch(zero_batch)
    assert len(projected_zero) == 1
    assert isinstance(projected_zero[0], WithheldDryRadarTargetObservation)
    assert projected_zero[0].rain_rate_mm_h == 0.0
    projected_undetect = radar_observations_from_radar_batch(undetect_batch)
    assert len(projected_undetect) == 1
    assert isinstance(
        projected_undetect[0],
        WithheldUndetectRadarTargetObservation,
    )
    assert projected_undetect[0].rain_rate_mm_h is None


def test_nodata_and_outside_domain_remain_explicit_without_claim():
    certificate_id = NOAA_CERTIFICATE_ID
    state = _desired_state(_target())
    nodata = build_radar_target_contribution_batch(
        state,
        (_source(certificate_id, pixel_state="nodata"),),
    ).contributions[0]
    outside = build_radar_target_contribution_batch(
        state,
        (_source(certificate_id, inside=False),),
    ).contributions[0]

    assert nodata.observation_state is RadarObservationState.NODATA_WITHHELD
    assert nodata.coverage_state is RadarCoverageState.IN_DOMAIN_NODATA
    assert nodata.sampled_row == nodata.sampled_column == 1
    assert nodata.rain_rate_mm_h is None
    assert outside.observation_state is RadarObservationState.OUTSIDE_DOMAIN
    assert outside.coverage_state is RadarCoverageState.OUTSIDE_DOMAIN
    assert outside.sampled_row is outside.sampled_column is None
    assert outside.rain_rate_mm_h is None
    assert nodata.claim_status is outside.claim_status is RadarClaimStatus.NO_CLAIM
    assert {item.plane for item in nodata.qc_samples} == {
        "dqf_bad_quality_mask",
        "dqf_degraded_mask",
        "dqf_missing_mask",
        "dqf_raw",
        "quality_optimal_mask",
    }
    assert outside.qc_samples == ()


def test_east_and_south_coverage_edges_are_excluded_half_open():
    certificate_id = NOAA_CERTIFICATE_ID
    source = _source(certificate_id)
    coverage = source.field.native_coverage
    east = _desired_state(
        _target(longitude=coverage.maximum_x, latitude=TARGET_LATITUDE)
    )
    south = _desired_state(
        _target(longitude=TARGET_LONGITUDE, latitude=coverage.minimum_y)
    )

    east_result = build_radar_target_contribution_batch(
        east, (source,)
    ).contributions[0]
    south_result = build_radar_target_contribution_batch(
        south, (source,)
    ).contributions[0]
    assert east_result.coverage_state is RadarCoverageState.OUTSIDE_DOMAIN
    assert south_result.coverage_state is RadarCoverageState.OUTSIDE_DOMAIN


@pytest.mark.parametrize(
    "change,match",
    [
        ({"semantics_certificate_sha256": "f" * 64}, "certificate"),
        ({"artifact_metadata_sha256": "e" * 64}, "candidate address"),
        ({"algorithm_version": "invented-bridge-v99"}, "bridge algorithm"),
        ({"quality_state": "invented-quality"}, "certificate"),
    ],
)
def test_tampered_certificate_process_or_qc_binding_is_rejected(change, match):
    field = _candidate(NOAA_CERTIFICATE_ID)
    tampered = replace(field, **change)
    with pytest.raises(RadarTargetFactsError, match=match):
        build_radar_target_contribution_batch(
            _desired_state(_target()),
            (VerifiedRadarFieldSource(tampered, RECEIVED_AT),),
        )


def test_exact_provider_grid_authorities_and_axis_fail_closed():
    noaa = _candidate(NOAA_CERTIFICATE_ID)
    bad_grid = replace(noaa.native_grid, axis_order="xy")
    with pytest.raises(RadarTargetFactsError, match="regular y/x"):
        build_radar_target_contribution_batch(
            _desired_state(_target()),
            (
                VerifiedRadarFieldSource(
                    replace(noaa, native_grid=bad_grid), RECEIVED_AT
                ),
            ),
        )

    shifted_noaa_coverage = replace(
        noaa.native_coverage,
        minimum_x=noaa.native_coverage.minimum_x + 0.001,
    )
    with pytest.raises(RadarTargetFactsError, match="exact GLB-5 window"):
        build_radar_target_contribution_batch(
            _desired_state(_target()),
            (
                VerifiedRadarFieldSource(
                    replace(noaa, native_coverage=shifted_noaa_coverage),
                    RECEIVED_AT,
                ),
            ),
        )

    met_office = _candidate(MET_OFFICE_CERTIFICATE_ID)
    with pytest.raises(RadarTargetFactsError, match="exact analysis grid"):
        build_radar_target_contribution_batch(
            _desired_state(_target()),
            (
                VerifiedRadarFieldSource(
                    replace(met_office, analysis_grid=None), RECEIVED_AT
                ),
            ),
        )

    dwd_source = _source(DWD_CERTIFICATE_ID)
    dwd = dwd_source.field
    shifted_dwd_coverage = replace(
        dwd.native_coverage,
        maximum_x=dwd.native_coverage.maximum_x - 1.0,
    )
    with pytest.raises(RadarTargetFactsError, match="official DE1200"):
        build_radar_target_contribution_batch(
            _desired_state(_target()),
            (
                VerifiedRadarFieldSource(
                    field=replace(dwd, native_coverage=shifted_dwd_coverage),
                    received_at=RECEIVED_AT,
                    native_grid_certificate=dwd_source.native_grid_certificate,
                ),
            ),
        )


def test_published_contribution_detects_process_hash_tampering():
    contribution = build_radar_target_contribution_batch(
        _desired_state(_target()),
        (_source(NOAA_CERTIFICATE_ID),),
    ).contributions[0]
    with pytest.raises(RadarTargetFactsError, match="content address"):
        replace(contribution, canonical_result_sha256="b" * 64)
    with pytest.raises(RadarTargetFactsError, match="content address"):
        replace(contribution, process_evidence_sha256="c" * 64)


def test_published_batch_detects_process_receipt_tampering():
    batch = build_radar_target_contribution_batch(
        _desired_state(_target()),
        (_source(NOAA_CERTIFICATE_ID),),
    )
    with pytest.raises(RadarTargetFactsError, match="process receipt"):
        replace(batch, process_receipt_sha256="b" * 64)


def test_candidate_content_address_binds_exact_raster_bytes():
    state = _desired_state(_target())
    first = build_radar_target_contribution_batch(
        state,
        (_source(NOAA_CERTIFICATE_ID, rate=2.5),),
    ).contributions[0]
    second = build_radar_target_contribution_batch(
        state,
        (_source(NOAA_CERTIFICATE_ID, rate=3.5),),
    ).contributions[0]
    assert first.candidate_id == second.candidate_id
    assert first.candidate_content_sha256 != second.candidate_content_sha256
    assert first.process_evidence_sha256 != second.process_evidence_sha256
    assert first.content_sha256 != second.content_sha256


def test_knmi_withheld_target_still_binds_full_quantitative_field_bytes():
    state = _desired_state(_target())
    first = build_radar_target_contribution_batch(
        state,
        (_source(KNMI_CERTIFICATE_ID, rate=2.5),),
    ).contributions[0]
    second = build_radar_target_contribution_batch(
        state,
        (_source(KNMI_CERTIFICATE_ID, rate=3.5),),
    ).contributions[0]

    assert first.observation_state is RadarObservationState.GRID_SEMANTICS_WITHHELD
    assert second.observation_state is RadarObservationState.GRID_SEMANTICS_WITHHELD
    assert first.rain_rate_mm_h is second.rain_rate_mm_h is None
    assert first.candidate_content_sha256 != second.candidate_content_sha256
    assert first.source_evidence_sha256 != second.source_evidence_sha256
    assert first.process_evidence_sha256 != second.process_evidence_sha256
    assert first.content_sha256 != second.content_sha256


def test_duplicate_provider_is_rejected_and_input_order_is_deterministic():
    state = _desired_state(_target())
    met_office = _source(MET_OFFICE_CERTIFICATE_ID)
    noaa = _source(NOAA_CERTIFICATE_ID)
    left = build_radar_target_contribution_batch(state, (met_office, noaa))
    right = build_radar_target_contribution_batch(state, (noaa, met_office))
    assert left.as_dict() == right.as_dict()

    with pytest.raises(RadarTargetFactsError, match="duplicate"):
        build_radar_target_contribution_batch(state, (noaa, noaa))


def test_received_at_cannot_precede_product_and_empty_targets_fail_closed():
    field = _candidate(NOAA_CERTIFICATE_ID)
    with pytest.raises(RadarTargetFactsError, match="cannot precede"):
        VerifiedRadarFieldSource(field, VALID_AT - 1.0)
    with pytest.raises(RadarTargetFactsError, match="explicit desired target"):
        build_radar_target_contribution_batch(
            _desired_state(),
            (VerifiedRadarFieldSource(field, RECEIVED_AT),),
        )
