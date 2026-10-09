from __future__ import annotations

import ast
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import numpy as np
import pytest

from storm_tracker_worker.canonical_evidence_store import CoverageSpec, NativeGridSpec
from storm_tracker_worker.canonical_radar_field_bridge import (
    NativeRainRateFieldCandidate,
    NativeRasterPlane,
    RADAR_FIELD_BRIDGE_ALGORITHM,
    default_radar_semantics_certificates,
)
from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker.multi_source_operational_cycle import EvidenceReference
from storm_tracker_worker.multi_source_radar_facts import (
    RADAR_PROVIDER_PROFILES,
    RadarTargetFactsError,
    VerifiedRadarFieldSource,
)
from storm_tracker_worker.multi_source_satellite_facts import (
    ACQUISITION_CATALOG_AUTHORITY_SHA256,
    MAX_RAW_EVIDENCE_ROLES,
    NOAA_ERR_SEMANTICS_CERTIFICATE_ID,
    SATELLITE_CYCLE_EVIDENCE_ROLE,
    SATELLITE_PROVIDER_PROFILES,
    SATELLITE_TARGET_BATCH_CONTRACT,
    SATELLITE_TARGET_BATCH_PROCESS_RECEIPT_CONTRACT,
    SatelliteClaimStatus,
    SatelliteCoverageState,
    SatelliteObservationState,
    SatelliteTargetFactsError,
    VerifiedRawSatelliteSource,
    build_satellite_target_contribution_batch,
)
from storm_tracker_worker.raw_manifest_audit_completion_consumer import (
    OUTPUT_DOCUMENT_SCHEMA,
    OUTPUT_MODE,
    OUTPUT_RECEIPT_SCHEMA,
    PROCESS_EFFECT_CONTRACT_SHA256,
    PROCESS_RECEIPT_SCHEMA,
    RawManifestAuditOutputReceipt,
)
from storm_tracker_worker.worker_acquisition_plan import (
    CATALOG_SHA256,
    acquisition_catalog_snapshot,
)


VALID_AT = 1_800_000_300.0
RECEIVED_AT = VALID_AT + 10.0
TARGET_LATITUDE = 51.04
TARGET_LONGITUDE = 4.48


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical(value, *, ascii_only: bool = False) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=ascii_only,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii" if ascii_only else "utf-8")


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
    return parse_desired_state(
        {
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
        }
    )


def _readonly(values, dtype=None) -> np.ndarray:
    result = np.asarray(values, dtype=dtype).copy()
    result.setflags(write=False)
    return result


def _noaa_candidate(
    *,
    rate: float = 2.5,
    pixel_state: str = "valid",
    inside: bool = True,
) -> NativeRainRateFieldCandidate:
    certificate = next(
        item
        for item in default_radar_semantics_certificates()
        if item.certificate_id == NOAA_ERR_SEMANTICS_CERTIFICATE_ID
    )
    minimum_x, maximum_x = ((4.45, 4.49) if inside else (-180.01, -179.97))
    minimum_y, maximum_y = ((51.03, 51.07) if inside else (69.97, 70.01))
    grid_sha = _sha(f"noaa-grid:{inside}")
    coverage_sha = _sha(f"noaa-coverage:{inside}")
    artifact_sha = _sha("noaa-artifact")
    artifact_metadata_sha = _sha("noaa-artifact-metadata")
    canonical_receipt_sha = _sha("noaa-canonical-receipt")
    candidate_digest = hashlib.sha256(
        _canonical(
            {
                "algorithm": RADAR_FIELD_BRIDGE_ALGORITHM,
                "artifact_metadata_sha256": artifact_metadata_sha,
                "artifact_sha256": artifact_sha,
                "canonical_receipt_sha256": canonical_receipt_sha,
                "certificate_sha256": certificate.fingerprint,
            }
        )
    ).hexdigest()
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
        raise AssertionError(pixel_state)
    planes = []
    profile = next(
        item
        for item in RADAR_PROVIDER_PROFILES
        if item.certificate_id == NOAA_ERR_SEMANTICS_CERTIFICATE_ID
    )
    for name in profile.required_qc_planes:
        if name == "dqf_raw":
            values = _readonly([[0, 0], [0, 1]], np.int16)
        elif name in {
            "dqf_bad_quality_mask",
            "dqf_degraded_mask",
            "dqf_missing_mask",
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
    return NativeRainRateFieldCandidate(
        candidate_id=f"radar-field-{candidate_digest}",
        canonical_run_id="run-" + _sha("noaa-run"),
        canonical_commit_id="commit-" + _sha("noaa-commit"),
        canonical_receipt_sha256=canonical_receipt_sha,
        canonical_result_sha256=_sha("noaa-canonical-result"),
        capture_id="capture-" + _sha("noaa-capture"),
        capture_receipt_sha256=_sha("noaa-capture-receipt"),
        capture_manifest_sha256=_sha("noaa-capture-manifest"),
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
        source_values=_readonly(rates, np.float64),
        output_quantity=certificate.output_quantity,
        output_unit=certificate.output_unit,
        rain_rate_mm_h=_readonly(rates, np.float64),
        valid_mask=_readonly(valid, np.bool_),
        nodata_mask=_readonly(nodata, np.bool_),
        undetect_mask=_readonly(undetect, np.bool_),
        auxiliary_planes=tuple(planes),
        temporal_semantics=certificate.temporal_semantics,
        support_start=VALID_AT - 300.0,
        support_end=VALID_AT,
        valid_at=VALID_AT,
        conversion="identity_mm_h",
        quality_state="native_dqf_flags_retained_not_applied",
        native_crs="EPSG:4326",
        native_grid=NativeGridSpec(
            kind="regular",
            width=2,
            height=2,
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
        analysis_grid=None,
        metric_raster_field=None,
        primary_sources=certificate.primary_sources,
    )


def _noaa_source(**changes) -> VerifiedRadarFieldSource:
    return VerifiedRadarFieldSource(
        field=_noaa_candidate(**changes),
        received_at=RECEIVED_AT,
    )


def _raw_receipt(
    workload_id: str,
    desired_state_sha256: str,
    *,
    role_count: int = 2,
) -> RawManifestAuditOutputReceipt:
    profile = next(
        item for item in SATELLITE_PROVIDER_PROFILES if item.workload_id == workload_id
    )
    assert profile.raw_consumer_id is not None
    roles = [
        {
            "content_sha256": _sha(f"{workload_id}:content:{index}"),
            "content_type": "application/x-netcdf" if index else "text/plain",
            "product": profile.product_id,
            "provider": profile.provider_id,
            "receipt_id": f"raw-{index}-{_sha(workload_id)[:12]}",
            "receipt_sha256": _sha(f"{workload_id}:receipt:{index}"),
            "role": f"role_{index:02d}",
            "size_bytes": index + 1,
        }
        for index in range(role_count)
    ]
    event_id = "completion-" + _sha(f"event:{workload_id}")
    created_at = RECEIVED_AT + 20.0
    authority_sha = _sha(f"authority:{workload_id}")
    isolation_sha = _sha(f"isolation:{workload_id}")
    limits_sha = _sha(f"limits:{workload_id}")
    event_sha = _sha(f"event-proof:{workload_id}")
    producer_receipt_sha = _sha(f"producer:{workload_id}")
    capture_id = "capture-" + _sha(f"raw-capture:{workload_id}")
    capture_manifest_sha = _sha(f"manifest:{workload_id}")
    doc = {
        "audit": {
            "acquisition_job_id": "job-" + _sha(f"job:{workload_id}"),
            "capture_id": capture_id,
            "capture_manifest_sha256": capture_manifest_sha,
            "capture_receipt_sha256": _sha(f"capture-receipt:{workload_id}"),
            "product_id": profile.product_id,
            "provider_id": profile.provider_id,
            "raw_bytes_verified": True,
            "role_set_content_sha256": _sha(f"role-content:{workload_id}"),
            "role_set_receipt_sha256": _sha(f"role-receipt:{workload_id}"),
            "roles": roles,
            "total_bytes": sum(item["size_bytes"] for item in roles),
        },
        "authority": {
            "authority_sha256": authority_sha,
            "desired_state_sha256": desired_state_sha256,
        },
        "claims": {
            "decoded": False,
            "fact_staged": False,
            "meteorological_admission": False,
            "operational_snapshot_published": False,
            "raw_bytes_copied": False,
            "raw_refetch_performed": False,
        },
        "consumer_id": profile.raw_consumer_id,
        "disposition": "raw_manifest_verified_shadow",
        "event": {
            "event_id": event_id,
            "event_sha256": event_sha,
            "producer_receipt_sha256": producer_receipt_sha,
            "stream_sequence": 1,
        },
        "fact_revision": None,
        "isolation": {
            "authority_sha256": isolation_sha,
            "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
            "limits_sha256": limits_sha,
            "process_boundary": "fresh_spawn_hard_cpu_rss_wall_output_caps",
            "resource_reason": "full_sha_pass_over_bounded_multi_role_raw_bytes",
            "runtime_wiring": "inert",
        },
        "mode": OUTPUT_MODE,
        "schema": OUTPUT_DOCUMENT_SCHEMA,
        "stream_id": workload_id,
        "workload_id": workload_id,
    }
    output_sha = hashlib.sha256(_canonical(doc, ascii_only=True)).hexdigest()
    receipt_sha = hashlib.sha256(
        _canonical(
            {
                "created_at": created_at,
                "output_sha256": output_sha,
                "schema": OUTPUT_RECEIPT_SCHEMA,
            },
            ascii_only=True,
        )
    ).hexdigest()
    process_sha = hashlib.sha256(
        _canonical(
            {
                "created_at": created_at,
                "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
                "event_id": event_id,
                "isolation_authority_sha256": isolation_sha,
                "limits_sha256": limits_sha,
                "output_receipt_sha256": receipt_sha,
                "output_sha256": output_sha,
                "schema": PROCESS_RECEIPT_SCHEMA,
            },
            ascii_only=True,
        )
    ).hexdigest()
    return RawManifestAuditOutputReceipt(
        receipt_sha256=receipt_sha,
        process_receipt_sha256=process_sha,
        output_sha256=output_sha,
        event_id=event_id,
        event_sha256=event_sha,
        producer_receipt_sha256=producer_receipt_sha,
        workload_id=workload_id,
        consumer_id=profile.raw_consumer_id,
        stream_id=workload_id,
        desired_state_sha256=desired_state_sha256,
        authority_sha256=authority_sha,
        capture_id=capture_id,
        capture_manifest_sha256=capture_manifest_sha,
        isolation_authority_sha256=isolation_sha,
        isolation_limits_sha256=limits_sha,
        created_at=created_at,
        output=doc,
    )


def _sources(state, **noaa_changes):
    raw = tuple(
        VerifiedRawSatelliteSource(_raw_receipt(profile.workload_id, state.state_sha256))
        for profile in SATELLITE_PROVIDER_PROFILES
        if profile.raw_consumer_id is not None
    )
    return (*raw, _noaa_source(**noaa_changes))


def _contribution(batch, workload_id: str, target_id: str = "zone.home"):
    return next(
        item
        for item in batch.contributions
        if item.workload_id == workload_id and item.target_id == target_id
    )


def test_exact_four_allowlist_matches_catalog_and_decoder_authorities():
    rows = {
        row["workload_id"]: row
        for row in acquisition_catalog_snapshot()["workloads"]
        if row["workload_id"].startswith("satellite.")
    }
    assert CATALOG_SHA256 == ACQUISITION_CATALOG_AUTHORITY_SHA256
    assert tuple(item.workload_id for item in SATELLITE_PROVIDER_PROFILES) == tuple(
        sorted(rows)
    )
    for profile in SATELLITE_PROVIDER_PROFILES:
        row = rows[profile.workload_id]
        assert row["provider_id"] == profile.provider_id
        assert row["product_id"] == profile.product_id
        assert row["satellite_id"] == profile.platform_id
        assert (
            None if row["coverage_bbox"] is None else tuple(row["coverage_bbox"])
        ) == profile.coverage_bbox
    certificate = next(
        item
        for item in default_radar_semantics_certificates()
        if item.certificate_id == NOAA_ERR_SEMANTICS_CERTIFICATE_ID
    )
    assert certificate.output_quantity == "instantaneous_precipitation_rate"
    assert certificate.output_unit == "mm/h"


def test_exact_cartesian_matrix_keeps_all_sources_and_real_noaa_sample():
    state = _desired_state(
        _target("device_tracker.unavailable", available=False),
        _target(),
    )
    batch = build_satellite_target_contribution_batch(state, _sources(state))

    assert batch.source_count == 4
    assert batch.target_count == 2
    assert batch.sample_count == 8
    assert batch.valid_sample_count == 1
    assert batch.positive_candidate_count == 1
    assert batch.withheld_count == 2
    assert hashlib.sha256(batch.canonical_bytes).hexdigest() == batch.content_sha256
    assert batch.size_bytes == len(batch.canonical_bytes)
    assert batch._process_receipt_body()["contract"] == (
        SATELLITE_TARGET_BATCH_PROCESS_RECEIPT_CONTRACT
    )
    assert hashlib.sha256(_canonical(batch._process_receipt_body())).hexdigest() == (
        batch.process_receipt_sha256
    )
    assert [(item.workload_id, item.target_id) for item in batch.contributions] == sorted(
        (item.workload_id, item.target_id) for item in batch.contributions
    )

    noaa = _contribution(batch, "satellite.noaa.err")
    assert noaa.observation_state is SatelliteObservationState.POSITIVE_RAIN_CANDIDATE
    assert noaa.coverage_state is SatelliteCoverageState.IN_NATIVE_DOMAIN_VALID
    assert noaa.claim_status is SatelliteClaimStatus.POSITIVE_CANDIDATE_ONLY
    assert noaa.rain_rate_mm_h == pytest.approx(2.5)
    assert noaa.quantity_id == "instantaneous_precipitation_rate"
    assert noaa.unit_id == "mm/h"
    assert noaa.sampled_row == noaa.sampled_column == 1
    assert {item.plane for item in noaa.quality_samples} == {
        "dqf_bad_quality_mask",
        "dqf_degraded_mask",
        "dqf_missing_mask",
        "dqf_raw",
        "quality_optimal_mask",
    }
    assert noaa.accumulation_mm is None
    assert not noaa.full_scan_bound and not noaa.external_truth_bound

    goes18 = _contribution(batch, "satellite.goes18.rrqpe")
    assert goes18.observation_state is (
        SatelliteObservationState.OUTSIDE_CATALOG_APPLICABILITY
    )
    for workload_id in ("satellite.goes19.rrqpe", "satellite.hsaf.h40b"):
        raw = _contribution(batch, workload_id)
        assert raw.observation_state is (
            SatelliteObservationState.QUANTITATIVE_DECODER_WITHHELD
        )
        assert raw.claim_status is SatelliteClaimStatus.QUANTITATIVE_WITHHELD
        assert raw.rain_rate_mm_h is raw.accumulation_mm is None
        assert raw.quantity_id is raw.unit_id is None
        assert raw.product_valid_at is None
        assert raw.sampled_row is raw.sampled_column is None
        assert any(
            item.role == "verified_raw_manifest_audit" for item in raw.source_evidence
        )
    unavailable = [
        item
        for item in batch.contributions
        if item.target_id == "device_tracker.unavailable"
    ]
    assert len(unavailable) == 4
    assert all(
        item.observation_state is SatelliteObservationState.TARGET_UNAVAILABLE
        and item.claim_status is SatelliteClaimStatus.NO_CLAIM
        for item in unavailable
    )


def test_input_order_is_deterministic_and_no_provider_can_disappear():
    state = _desired_state(_target())
    sources = _sources(state)
    first = build_satellite_target_contribution_batch(state, sources)
    second = build_satellite_target_contribution_batch(state, tuple(reversed(sources)))
    assert first == second
    with pytest.raises(SatelliteTargetFactsError, match="all four"):
        build_satellite_target_contribution_batch(state, sources[:-1])
    with pytest.raises(SatelliteTargetFactsError, match="duplicate|incomplete"):
        build_satellite_target_contribution_batch(
            state,
            (sources[0], sources[0], sources[2], sources[3]),
        )


def test_noaa_zero_undetect_nodata_and_outside_are_fail_closed():
    state = _desired_state(_target())
    zero = _contribution(
        build_satellite_target_contribution_batch(state, _sources(state, rate=0.0)),
        "satellite.noaa.err",
    )
    undetect = _contribution(
        build_satellite_target_contribution_batch(
            state, _sources(state, pixel_state="undetect")
        ),
        "satellite.noaa.err",
    )
    nodata = _contribution(
        build_satellite_target_contribution_batch(
            state, _sources(state, pixel_state="nodata")
        ),
        "satellite.noaa.err",
    )
    outside = _contribution(
        build_satellite_target_contribution_batch(state, _sources(state, inside=False)),
        "satellite.noaa.err",
    )
    assert zero.observation_state is SatelliteObservationState.DRY_WITHHELD
    assert zero.rain_rate_mm_h == 0.0
    assert undetect.observation_state is SatelliteObservationState.DRY_WITHHELD
    assert undetect.rain_rate_mm_h is None
    assert zero.claim_status is undetect.claim_status is SatelliteClaimStatus.NEGATIVE_WITHHELD
    assert nodata.observation_state is SatelliteObservationState.NODATA_WITHHELD
    assert nodata.rain_rate_mm_h is None
    assert outside.observation_state is SatelliteObservationState.OUTSIDE_NATIVE_DOMAIN
    assert outside.catalog_applicable is True
    assert outside.rain_rate_mm_h is None


def test_raw_receipt_content_receipt_and_claim_tamper_fail_closed():
    state = _desired_state(_target())
    receipt = _raw_receipt("satellite.goes19.rrqpe", state.state_sha256)
    tampered_output = json.loads(json.dumps(receipt.output))
    tampered_output["audit"]["roles"][0]["content_sha256"] = "f" * 64
    with pytest.raises(SatelliteTargetFactsError, match="receipt|output"):
        VerifiedRawSatelliteSource(replace(receipt, output=tampered_output))
    with pytest.raises(SatelliteTargetFactsError, match="receipt|output"):
        VerifiedRawSatelliteSource(replace(receipt, receipt_sha256="f" * 64))
    forged_claim = json.loads(json.dumps(receipt.output))
    forged_claim["claims"]["decoded"] = True
    forged_body = _canonical(forged_claim, ascii_only=True)
    with pytest.raises(SatelliteTargetFactsError, match="claims"):
        VerifiedRawSatelliteSource(
            replace(
                receipt,
                output=forged_claim,
                output_sha256=hashlib.sha256(forged_body).hexdigest(),
            )
        )


def test_raw_receipt_must_bind_the_same_desired_state():
    active = _desired_state(_target())
    other = _desired_state(_target("zone.other"))
    sources = list(_sources(active))
    sources[0] = VerifiedRawSatelliteSource(
        _raw_receipt("satellite.goes18.rrqpe", other.state_sha256)
    )
    with pytest.raises(SatelliteTargetFactsError, match="desired-state"):
        build_satellite_target_contribution_batch(active, tuple(sources))


def test_raw_role_and_target_bounds_are_hard():
    state = _desired_state(_target())
    with pytest.raises(SatelliteTargetFactsError, match="role count"):
        VerifiedRawSatelliteSource(
            _raw_receipt(
                "satellite.hsaf.h40b",
                state.state_sha256,
                role_count=MAX_RAW_EVIDENCE_ROLES + 1,
            )
        )
    too_many = replace(state, targets=state.targets * 65)
    with pytest.raises(SatelliteTargetFactsError, match="target count"):
        build_satellite_target_contribution_batch(too_many, _sources(too_many))


def test_catalog_bbox_edges_are_inclusive_and_outside_is_explicit():
    state = _desired_state(
        _target("device_tracker.east_edge", latitude=0.0, longitude=-55.0),
        _target("device_tracker.west_edge", latitude=0.0, longitude=-180.0),
        _target("device_tracker.outside", latitude=0.0, longitude=-54.99),
    )
    batch = build_satellite_target_contribution_batch(state, _sources(state))
    for target_id in ("device_tracker.east_edge", "device_tracker.west_edge"):
        assert _contribution(
            batch, "satellite.goes18.rrqpe", target_id
        ).observation_state is SatelliteObservationState.QUANTITATIVE_DECODER_WITHHELD
    outside = _contribution(
        batch, "satellite.goes18.rrqpe", "device_tracker.outside"
    )
    assert outside.observation_state is (
        SatelliteObservationState.OUTSIDE_CATALOG_APPLICABILITY
    )
    assert outside.catalog_applicability_reason == "catalog_bbox_mismatch"


def test_noaa_certificate_qc_grid_and_source_type_tamper_fail_closed():
    state = _desired_state(_target())
    sources = list(_sources(state))
    noaa = sources[-1]
    assert isinstance(noaa, VerifiedRadarFieldSource)
    forged_certificate = replace(
        noaa.field, semantics_certificate_sha256="f" * 64
    )
    with pytest.raises(RadarTargetFactsError, match="certificate|semantics"):
        build_satellite_target_contribution_batch(
            state, (*sources[:-1], replace(noaa, field=forged_certificate))
        )
    missing_qc = replace(noaa.field, auxiliary_planes=noaa.field.auxiliary_planes[:-1])
    with pytest.raises(RadarTargetFactsError, match="QC|plane|candidate"):
        build_satellite_target_contribution_batch(
            state, (*sources[:-1], replace(noaa, field=missing_qc))
        )
    shifted = replace(
        noaa.field,
        native_coverage=replace(
            noaa.field.native_coverage,
            maximum_x=noaa.field.native_coverage.maximum_x + 0.01,
        ),
    )
    with pytest.raises(
        RadarTargetFactsError, match="NOAA|grid|coverage|window|aligned"
    ):
        build_satellite_target_contribution_batch(
            state, (*sources[:-1], replace(noaa, field=shifted))
        )


def test_contribution_batch_and_cycle_reference_are_content_addressed():
    state = _desired_state(_target())
    batch = build_satellite_target_contribution_batch(state, _sources(state))
    contribution = _contribution(batch, "satellite.noaa.err")
    with pytest.raises(SatelliteTargetFactsError, match="content address|positive"):
        replace(contribution, rain_rate_mm_h=9.0)
    with pytest.raises(SatelliteTargetFactsError, match="sorted|matrix"):
        replace(batch, contributions=tuple(reversed(batch.contributions)))
    reference = batch.to_cycle_evidence_reference()
    cycle_reference = EvidenceReference.from_mapping(reference.to_mapping())
    assert cycle_reference.role == SATELLITE_CYCLE_EVIDENCE_ROLE
    assert cycle_reference.schema_id == SATELLITE_TARGET_BATCH_CONTRACT
    assert cycle_reference.content_sha256 == batch.content_sha256
    assert cycle_reference.receipt_sha256 == batch.process_receipt_sha256
    assert cycle_reference.size_bytes == len(batch.canonical_bytes)


def test_module_has_no_io_service_forecast_or_alert_wiring():
    source_path = (
        Path(__file__).parents[1]
        / "storm_tracker_worker"
        / "multi_source_satellite_facts.py"
    )
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported_roots = {
        alias.name.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_roots.update(
        (node.module or "").split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.level == 0
    )
    assert imported_roots.isdisjoint(
        {"aiohttp", "httpx", "os", "pathlib", "requests", "socket", "sqlite3", "urllib"}
    )
    text = source_path.read_text(encoding="utf-8")
    assert "async_setup_entry" not in text
    assert "CoordinatorEntity" not in text
    assert "publish_alert" not in text
    assert "forecast_eligible=True" not in text
