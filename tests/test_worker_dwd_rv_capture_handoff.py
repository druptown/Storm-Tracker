from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import time

import pytest

from storm_tracker_worker.analysis_grid_transform import (
    AnalysisGridTransformConfig,
    ExactAffineGridTransformAdapter,
    NativeFieldCandidateBinding,
    native_candidate_content_sha256,
)
from storm_tracker_worker.canonical_evidence_store import CanonicalEvidenceStore
from storm_tracker_worker.canonical_radar_field_bridge import (
    ProviderDerivedArtifactAuthority,
    default_radar_semantics_certificates,
    derive_canonical_radar_field,
)
from storm_tracker_worker.dwd_rv_analysis_grid_certificate import (
    issue_dwd_rv_native_grid_certificate,
)
from storm_tracker_worker.dwd_rv_capture_handoff import (
    DwdRvCaptureHandoffError,
    EXPECTED_ALL_LEADS,
    EXPECTED_FORECAST_LEADS,
    HANDOFF_CONFIG_SHA256,
    HANDOFF_FORECAST_POLICY,
    HANDOFF_OBSERVATION_POLICY,
    HANDOFF_TARGET_COLLECTOR_VERSION,
    SOURCE_ARCHIVE_PRODUCT,
    SOURCE_CAPTURE_PRODUCT,
    SOURCE_COLLECTOR_VERSION,
    SOURCE_PROVIDER,
    TARGET_ARCHIVE_PRODUCT,
    TARGET_CAPTURE_PRODUCT,
    TARGET_PROVIDER,
    adapt_dwd_rv_raw_capture,
    inspect_dwd_rv_archive,
)
from storm_tracker_worker.dwd_rv_quantitative import (
    DwdRvDerivedArtifactStore,
    decode_dwd_rv_capture,
)
from storm_tracker_worker.evidence_to_cells_shadow import (
    EvidenceToCellsShadowConfig,
    compose_evidence_to_cells_shadow,
)
from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactIntegrityError,
    RawArtifactStore,
)
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
)


REPOSITORY = Path(__file__).resolve().parents[1]
LIVE_ARCHIVE = REPOSITORY / ".analysis" / "dwd-rv-live.tar"
LIVE_EVIDENCE = REPOSITORY / "tests" / "fixtures" / "dwd_rv_live_evidence.json"
CRASH_HELPER = (
    REPOSITORY / "tests" / "helpers" / "provider_crash_durable_bridge_child.py"
)
BASE_TIME = datetime(2026, 8, 1, 23, 5, tzinfo=timezone.utc).timestamp()
RECEIVED_AT = BASE_TIME + 60.0
EVALUATED_AT = RECEIVED_AT + 30.0


def _identity(prefix: str, seed: str) -> str:
    return prefix + hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _live_bytes() -> bytes:
    assert LIVE_ARCHIVE.is_file(), "fixed .analysis/dwd-rv-live.tar evidence is absent"
    return LIVE_ARCHIVE.read_bytes()


def _synthetic_contract_archive() -> bytes:
    """Return one deterministic structure-valid +000..+120 DWD RV tar."""

    assert EXPECTED_ALL_LEADS == tuple(range(0, 121, 5))
    output = io.BytesIO()
    with tarfile.open(
        fileobj=output,
        mode="w",
        format=tarfile.USTAR_FORMAT,
    ) as archive:
        for lead in EXPECTED_ALL_LEADS:
            payload = f"synthetic-dwd-rv-hdf5-member-{lead:03d}".encode("ascii")
            member = tarfile.TarInfo(
                f"composite_rv_20260801_2305_{lead:03d}-hd5"
            )
            member.size = len(payload)
            member.mode = 0o644
            member.mtime = 0
            archive.addfile(member, io.BytesIO(payload))
    payload = output.getvalue()
    inventory = inspect_dwd_rv_archive(payload)
    assert tuple(item.lead_minutes for item in inventory.members) == EXPECTED_ALL_LEADS
    assert tuple(item.member_name for item in inventory.members) == tuple(
        f"composite_rv_20260801_2305_{lead:03d}-hd5"
        for lead in EXPECTED_ALL_LEADS
    )
    assert len(inventory.members) == 25
    assert all(item.size_bytes > 0 for item in inventory.members)
    return payload


def _source_provenance(**changes):
    value = {
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": "dwd_open_data_composite_rv",
        "collector_version": SOURCE_COLLECTOR_VERSION,
        "provenance_scope": "exact_upstream_tar_bytes_only",
        "representation": NATIVE_REPRESENTATION,
        "structure_inspector_version": "dwd-rv-tar-layout-only-v1",
    }
    value.update(changes)
    return value


def _source_capture(
    tmp_path: Path,
    *,
    payload: bytes | None = None,
    provider: str = SOURCE_PROVIDER,
    archive_product: str = SOURCE_ARCHIVE_PRODUCT,
    capture_product: str = SOURCE_CAPTURE_PRODUCT,
    provenance: dict | None = None,
    upstream_capture_id: str | None = None,
    source_job: str = "dwd-live-fixture",
):
    archive = _live_bytes() if payload is None else payload
    digest = hashlib.sha256(archive).hexdigest()
    raw = RawArtifactStore((tmp_path / "source-raw").resolve())
    raw.initialize()
    receipt = raw.store(
        provider=provider,
        product=archive_product,
        fetch_id=f"{source_job}:rv_tar",
        content_type="application/x-tar",
        received_at=RECEIVED_AT,
        upstream_id=f"{SOURCE_ARCHIVE_PRODUCT}|latest|content_sha256={digest}",
        provenance=provenance or _source_provenance(),
        payload=archive,
    )
    capture = raw.commit_capture(
        provider=provider,
        product=capture_product,
        fetch_id=f"{source_job}:capture",
        upstream_capture_id=(
            upstream_capture_id
            or (
                f"{SOURCE_CAPTURE_PRODUCT}"
                "|member=composite_rv_20260801_2305_000-hd5"
                f"|content_sha256={digest}"
            )
        ),
        product_timestamp=BASE_TIME,
        members={"rv_tar": receipt.receipt_id},
    )
    return raw, capture, receipt


def _handoff_store(tmp_path: Path, suffix: str = "") -> RawArtifactStore:
    value = RawArtifactStore((tmp_path / f"handoff-raw{suffix}").resolve())
    value.initialize()
    return value


def _raw_growth_proof(store: RawArtifactStore) -> tuple[int, ...]:
    stats = store.stats()
    return tuple(
        stats[field]
        for field in (
            "object_count",
            "receipt_count",
            "capture_count",
            "catalog_bytes",
            "filesystem_bytes",
            "filesystem_object_entries",
            "filesystem_staging_entries",
            "unmanaged_file_count",
        )
    )


def _raw_identity_proof(store: RawArtifactStore) -> tuple[tuple, tuple]:
    receipts = store.accounting_receipts()
    captures = store.accounting_captures()
    return (
        tuple(
            (
                item.receipt_id,
                item.content_sha256,
                item.provenance_sha256,
                item.receipt_sha256,
            )
            for item in receipts
        ),
        tuple(
            (
                item.capture_id,
                item.manifest_sha256,
                item.capture_receipt_sha256,
                tuple(
                    (
                        member.role,
                        member.receipt_id,
                        member.content_sha256,
                        member.receipt_sha256,
                    )
                    for member in item.members
                ),
            )
            for item in captures
        ),
    )


def _decoder_stores(tmp_path: Path, raw: RawArtifactStore, suffix: str = ""):
    artifacts = DwdRvDerivedArtifactStore((tmp_path / f"derived{suffix}").resolve())
    evidence = CanonicalEvidenceStore(
        (tmp_path / f"canonical{suffix}.sqlite3").resolve(),
        raw_artifact_reader=raw,
        clock=lambda: EVALUATED_AT,
    )
    return artifacts, evidence


def _decode(tmp_path: Path, raw: RawArtifactStore, capture_id: str, suffix: str = ""):
    artifacts, evidence = _decoder_stores(tmp_path, raw, suffix)
    result = decode_dwd_rv_capture(
        capture_id=capture_id,
        decoder_job_id=_identity("decoder-job-", f"handoff-job{suffix}"),
        attempt_id=_identity("decoder-attempt-", f"handoff-attempt{suffix}"),
        raw_store=raw,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=EVALUATED_AT,
    )
    return result, artifacts, evidence


def _archive_without_lead(payload: bytes, removed_lead: int) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:") as source:
        with tarfile.open(fileobj=output, mode="w") as target:
            for member in source.getmembers():
                if member.name.endswith(f"_{removed_lead:03d}-hd5"):
                    continue
                stream = source.extractfile(member)
                assert stream is not None
                body = stream.read()
                copied = tarfile.TarInfo(member.name)
                copied.size = len(body)
                target.addfile(copied, io.BytesIO(body))
    return output.getvalue()


def test_live_fixture_manifest_binds_every_exact_archive_member():
    payload = _live_bytes()
    expected = json.loads(LIVE_EVIDENCE.read_text(encoding="utf-8"))
    inventory = inspect_dwd_rv_archive(payload)

    assert expected["schema"] == "stv3-dwd-rv-live-fixture-evidence-v1"
    assert len(payload) == expected["archive_size_bytes"] == 880_640
    assert hashlib.sha256(payload).hexdigest() == expected["archive_sha256"]
    assert inventory.archive_sha256 == expected["archive_sha256"]
    assert inventory.base_time == BASE_TIME
    assert [item.to_mapping() for item in inventory.members] == expected["members"]
    assert tuple(item.lead_minutes for item in inventory.members) == EXPECTED_ALL_LEADS


def test_live_capture_v2_to_decoder_to_canonical_cells_shadow(tmp_path):
    source, source_capture, source_receipt = _source_capture(tmp_path)
    handoff = _handoff_store(tmp_path)
    adapted = adapt_dwd_rv_raw_capture(
        source_capture_id=source_capture.capture_id,
        source_raw_store=source,
        handoff_raw_store=handoff,
    )

    target_bundle = handoff.load_capture_bundle_bytes(adapted.target_capture_id)
    assert target_bundle is not None
    target_capture, target_receipts, target_payloads = target_bundle
    target_receipt = target_receipts["rv_tar"]
    provenance = target_receipt.provenance
    archive_evidence = provenance["capture_handoff"]["archive"]
    assert target_payloads["rv_tar"] == _live_bytes()
    assert target_receipt.content_sha256 == source_receipt.content_sha256
    assert adapted.archive_sha256 == source_receipt.content_sha256
    assert target_capture.provider == TARGET_PROVIDER
    assert target_capture.product == TARGET_CAPTURE_PRODUCT
    assert target_receipt.product == TARGET_ARCHIVE_PRODUCT
    assert provenance["collector_version"] == HANDOFF_TARGET_COLLECTOR_VERSION
    assert provenance["capture_handoff"]["adapter_config_sha256"] == HANDOFF_CONFIG_SHA256
    assert archive_evidence["observation_policy"] == HANDOFF_OBSERVATION_POLICY
    assert archive_evidence["observation_member"]["lead_minutes"] == 0
    assert archive_evidence["forecast_policy"] == HANDOFF_FORECAST_POLICY
    assert [
        item["lead_minutes"] for item in archive_evidence["forecast_members"]
    ] == list(EXPECTED_FORECAST_LEADS)

    decoded, artifacts, evidence = _decode(
        tmp_path, handoff, adapted.target_capture_id
    )
    assert decoded.outcome == "decoded_complete"
    assert decoded.reason_code == "decoded_complete"
    assert decoded.artifact is not None
    raster = artifacts.read_decoded(decoded.artifact)
    assert raster.metadata["product"] == {
        "accumulation_seconds": 300,
        "forecast_lead_minutes": 0,
        "gain": pytest.approx(0.0009999999317806213),
        "interval_end": BASE_TIME,
        "interval_start": BASE_TIME - 300.0,
        "nodata": 4294967295,
        "offset": pytest.approx(-0.0009999999317806213),
        "product_time": BASE_TIME,
        "quantity": "ACRR",
        "semantic_quantity": "precipitation_accumulation",
        "undetect": 0,
        "unit": "mm",
    }
    assert raster.metadata["grid"]["width"] == 1100
    assert raster.metadata["grid"]["height"] == 1200
    assert raster.metadata["grid"]["xscale_m"] == 1000.0
    assert raster.metadata["grid"]["yscale_m"] == 1000.0

    authority = ProviderDerivedArtifactAuthority({"dwd_rv_v1": artifacts})
    semantics = tuple(
        item
        for item in default_radar_semantics_certificates()
        if item.provider == TARGET_PROVIDER and item.product == TARGET_CAPTURE_PRODUCT
    )
    assert len(semantics) == 1
    bridged = derive_canonical_radar_field(
        evidence,
        authority,
        run_id=decoded.evidence_receipt.run_id,
        certificates=semantics,
        deadline_at=time.monotonic() + 60.0,
    )
    assert bridged.is_candidate
    native_candidate = bridged.candidate
    assert native_candidate is not None
    assert native_candidate.support_start == BASE_TIME - 300.0
    assert native_candidate.support_end == BASE_TIME
    assert native_candidate.valid_at == BASE_TIME
    assert native_candidate.source_quantity == "precipitation_accumulation_5_minute"
    assert native_candidate.output_quantity == "interval_mean_precipitation_rate"
    assert native_candidate.output_unit == "mm/h"

    transform_config = AnalysisGridTransformConfig(
        max_source_pixels=1_500_000,
        max_target_pixels=16,
        max_array_bytes=256 * 1024 * 1024,
        max_overlap_contributions=2_000_000,
    )
    target_grid = AnalysisGrid(
        grid_id="dwd-rv-live-top-left-2x2-shadow",
        crs=native_candidate.native_crs,
        origin_x_m=-500.0,
        origin_y_m=500.0,
        x_resolution_m=1000.0,
        y_resolution_m=1000.0,
        width=2,
        height=2,
    )
    binding = NativeFieldCandidateBinding(
        candidate=native_candidate,
        candidate_content_sha256=native_candidate_content_sha256(native_candidate),
        semantics_certificate_sha256=native_candidate.semantics_certificate_sha256,
    )
    grid_certificate = issue_dwd_rv_native_grid_certificate(
        binding=binding,
        target_grid=target_grid,
        config=transform_config,
    )
    composed = compose_evidence_to_cells_shadow(
        evidence,
        authority,
        run_id=decoded.evidence_receipt.run_id,
        radar_semantics_certificates=semantics,
        target_grid=target_grid,
        transform_adapter=ExactAffineGridTransformAdapter(grid_certificate),
        transform_config=transform_config,
        detector_config=HierarchicalDetectorConfig(
            shield_threshold=1.0,
            core_threshold=4.0,
            minimum_shield_area_m2=1_000_000.0,
            minimum_core_area_m2=1_000_000.0,
            shield_position_sigma_m=500.0,
            core_position_sigma_m=350.0,
            connectivity=8,
            centroid_method="geometric",
        ),
        pipeline_config=EvidenceToCellsShadowConfig(
            max_target_pixels=16,
            max_detection_objects=32,
            max_receipt_bytes=4 * 1024 * 1024,
            max_runtime_seconds=60.0,
        ),
        deadline_at=time.monotonic() + 60.0,
    )
    assert composed.is_candidate
    cells = composed.candidate
    assert cells is not None
    assert cells.normalized_frame.valid_at == BASE_TIME
    assert cells.forecast_generated is False
    assert cells.meteorological_admission_granted is False
    assert "motion" not in cells.analysis_lineage_json.lower()
    assert "forecast" not in cells.native_lineage_json.lower()


@pytest.mark.parametrize(
    ("fault_stage", "expected_target_receipts", "expected_target_captures"),
    (
        pytest.param(
            "handoff.after_source_verified", 0, 0, id="after_source_verified"
        ),
        pytest.param(
            "handoff.after_target_receipt", 1, 0, id="after_target_receipt"
        ),
        pytest.param(
            "handoff.before_target_capture", 1, 0, id="before_target_capture"
        ),
        pytest.param(
            "handoff.after_target_capture", 1, 1, id="after_target_capture"
        ),
    ),
)
def test_real_process_crash_handoff_recovers_without_identity_or_hash_growth(
    tmp_path,
    fault_stage,
    expected_target_receipts,
    expected_target_captures,
):
    payload = _synthetic_contract_archive()
    assert payload == _synthetic_contract_archive()
    inventory = inspect_dwd_rv_archive(payload)
    source, source_capture, source_receipt = _source_capture(
        tmp_path,
        payload=payload,
        source_job="dwd-synthetic-crash",
    )
    handoff = _handoff_store(tmp_path)
    source_growth = _raw_growth_proof(source)
    source_identities = _raw_identity_proof(source)
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (str(REPOSITORY), environment.get("PYTHONPATH"))
        if value
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(CRASH_HELPER),
            "dwd-handoff",
            str(tmp_path.resolve()),
            fault_stage,
            source_capture.capture_id,
        ],
        cwd=REPOSITORY,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    inspected_source = RawArtifactStore(source.root)
    inspected_source.initialize()
    inspected_handoff = RawArtifactStore(handoff.root)
    inspected_handoff.initialize()
    assert inspected_source.stats()["object_count"] == 1
    assert inspected_source.stats()["receipt_count"] == 1
    assert inspected_source.stats()["capture_count"] == 1
    assert inspected_handoff.stats()["receipt_count"] == expected_target_receipts
    assert inspected_handoff.stats()["capture_count"] == expected_target_captures
    assert inspected_handoff.stats()["object_count"] == expected_target_receipts
    assert _raw_growth_proof(inspected_source) == source_growth
    assert _raw_identity_proof(inspected_source) == source_identities
    inspected_handoff_identities = _raw_identity_proof(inspected_handoff)
    assert inspected_source.integrity_check()["healthy"] is True
    assert inspected_handoff.integrity_check()["healthy"] is True

    recovery_source = RawArtifactStore(source.root)
    recovery_source.initialize()
    recovery_handoff = RawArtifactStore(handoff.root)
    recovery_handoff.initialize()
    recovered = adapt_dwd_rv_raw_capture(
        source_capture_id=source_capture.capture_id,
        source_raw_store=recovery_source,
        handoff_raw_store=recovery_handoff,
    )
    recovered_growth = _raw_growth_proof(recovery_handoff)
    recovered_identities = _raw_identity_proof(recovery_handoff)
    if expected_target_receipts:
        assert recovered_identities[0] == inspected_handoff_identities[0]
    if expected_target_captures:
        assert recovered_identities == inspected_handoff_identities

    replay_source = RawArtifactStore(source.root)
    replay_source.initialize()
    replay_handoff = RawArtifactStore(handoff.root)
    replay_handoff.initialize()
    replay = adapt_dwd_rv_raw_capture(
        source_capture_id=source_capture.capture_id,
        source_raw_store=replay_source,
        handoff_raw_store=replay_handoff,
    )

    assert recovered == replay
    assert recovered.source_capture_id == source_capture.capture_id
    assert recovered.source_capture_receipt_sha256 == (
        source_capture.capture_receipt_sha256
    )
    assert recovered.source_manifest_sha256 == source_capture.manifest_sha256
    assert recovered.source_receipt_id == source_receipt.receipt_id
    assert recovered.archive_sha256 == hashlib.sha256(payload).hexdigest()
    assert recovered.observation_member_name == inventory.observation.member_name
    assert recovered.observation_member_sha256 == inventory.observation.content_sha256
    assert recovered.forecast_inventory_sha256 == inventory.forecast_inventory_sha256
    target_bundle = replay_handoff.load_capture_bundle_bytes(replay.target_capture_id)
    assert target_bundle is not None
    target_capture, target_receipts, target_payloads = target_bundle
    target_receipt = target_receipts["rv_tar"]
    assert recovered.target_capture_id == target_capture.capture_id
    assert recovered.target_capture_receipt_sha256 == (
        target_capture.capture_receipt_sha256
    )
    assert recovered.target_manifest_sha256 == target_capture.manifest_sha256
    assert recovered.target_receipt_id == target_receipt.receipt_id
    assert target_receipt.content_sha256 == source_receipt.content_sha256
    assert target_receipt.receipt_sha256 == target_capture.members[0].receipt_sha256
    assert target_payloads == {"rv_tar": payload}
    assert _raw_growth_proof(replay_handoff) == recovered_growth
    assert _raw_identity_proof(replay_handoff) == recovered_identities
    assert _raw_growth_proof(replay_source) == source_growth
    assert _raw_identity_proof(replay_source) == source_identities
    assert replay_handoff.stats()["object_count"] == 1
    assert replay_handoff.stats()["receipt_count"] == 1
    assert replay_handoff.stats()["capture_count"] == 1
    assert replay_source.integrity_check()["healthy"] is True
    assert replay_handoff.integrity_check()["healthy"] is True


def test_concurrent_handoffs_publish_one_exact_target(tmp_path):
    source, source_capture, _source_receipt = _source_capture(tmp_path)
    handoff = _handoff_store(tmp_path)

    def run(_index: int):
        return adapt_dwd_rv_raw_capture(
            source_capture_id=source_capture.capture_id,
            source_raw_store=source,
            handoff_raw_store=handoff,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts = tuple(pool.map(run, range(2)))
    assert receipts[0] == receipts[1]
    assert handoff.stats()["object_count"] == 1
    assert handoff.stats()["receipt_count"] == 1
    assert handoff.stats()["capture_count"] == 1


def test_source_raw_tamper_stops_before_target_receipt(tmp_path):
    source, source_capture, source_receipt = _source_capture(tmp_path)
    path = Path(source_receipt.artifact_path)
    payload = path.read_bytes()
    path.write_bytes(bytes([payload[0] ^ 0xFF]) + payload[1:])
    handoff = _handoff_store(tmp_path)

    with pytest.raises(RawArtifactIntegrityError):
        adapt_dwd_rv_raw_capture(
            source_capture_id=source_capture.capture_id,
            source_raw_store=source,
            handoff_raw_store=handoff,
        )
    assert handoff.stats()["receipt_count"] == 0
    assert handoff.stats()["capture_count"] == 0


@pytest.mark.parametrize(
    "source_changes",
    (
        {"provider": "dwd_radolan"},
        {"archive_product": "dwd_open_data_composite_rv_other"},
        {"capture_product": "dwd_open_data_composite_rv_other_capture"},
        {"provenance": _source_provenance(collector_version="drifted-v3")},
        {"provenance": _source_provenance(provenance_scope="decoded_values")},
        {"upstream_capture_id": "wrong-capture-upstream"},
    ),
)
def test_wrong_source_identity_fails_closed_without_target(tmp_path, source_changes):
    source, source_capture, _receipt = _source_capture(tmp_path, **source_changes)
    handoff = _handoff_store(tmp_path)
    with pytest.raises(DwdRvCaptureHandoffError):
        adapt_dwd_rv_raw_capture(
            source_capture_id=source_capture.capture_id,
            source_raw_store=source,
            handoff_raw_store=handoff,
        )
    assert handoff.stats()["receipt_count"] == 0


@pytest.mark.parametrize("removed_lead", (0, 5, 120))
def test_missing_observation_or_forecast_member_fails_closed(tmp_path, removed_lead):
    payload = _archive_without_lead(_live_bytes(), removed_lead)
    source, source_capture, _receipt = _source_capture(tmp_path, payload=payload)
    handoff = _handoff_store(tmp_path)
    with pytest.raises(
        DwdRvCaptureHandoffError,
        match=r"exact \+000\.\.\+120 five-minute member set",
    ):
        adapt_dwd_rv_raw_capture(
            source_capture_id=source_capture.capture_id,
            source_raw_store=source,
            handoff_raw_store=handoff,
        )


@pytest.mark.parametrize(
    "mutation",
    ("missing_archive_metadata", "wrong_source", "wrong_observation_member"),
)
def test_decoder_rejects_incomplete_or_wrong_handoff_metadata(tmp_path, mutation):
    source, source_capture, _source_receipt = _source_capture(tmp_path)
    handoff = _handoff_store(tmp_path)
    adapted = adapt_dwd_rv_raw_capture(
        source_capture_id=source_capture.capture_id,
        source_raw_store=source,
        handoff_raw_store=handoff,
    )
    bundle = handoff.load_capture_bundle_bytes(adapted.target_capture_id)
    assert bundle is not None
    _capture, receipts, payloads = bundle
    original_receipt = receipts["rv_tar"]
    provenance = deepcopy(original_receipt.provenance)
    if mutation == "missing_archive_metadata":
        del provenance["capture_handoff"]["archive"]["forecast_policy"]
    elif mutation == "wrong_source":
        provenance["capture_handoff"]["source"]["provider"] = "dwd_radolan"
    else:
        archive = provenance["capture_handoff"]["archive"]
        archive["observation_member"] = deepcopy(archive["forecast_members"][0])

    malformed = _handoff_store(tmp_path, suffix=f"-{mutation}")
    receipt = malformed.store(
        provider=TARGET_PROVIDER,
        product=TARGET_ARCHIVE_PRODUCT,
        fetch_id=f"malformed-{mutation}:rv_tar",
        content_type="application/x-tar",
        received_at=RECEIVED_AT,
        upstream_id=original_receipt.upstream_id,
        provenance=provenance,
        payload=payloads["rv_tar"],
    )
    capture = malformed.commit_capture(
        provider=TARGET_PROVIDER,
        product=TARGET_CAPTURE_PRODUCT,
        fetch_id=f"malformed-{mutation}:capture",
        upstream_capture_id=(
            f"{TARGET_CAPTURE_PRODUCT}"
            "|member=composite_rv_20260801_2305_000-hd5"
            f"|product_timestamp={BASE_TIME:.6f}"
            f"|content_sha256={receipt.content_sha256}"
        ),
        product_timestamp=BASE_TIME,
        members={"rv_tar": receipt.receipt_id},
    )
    decoded, artifacts, _evidence = _decode(
        tmp_path, malformed, capture.capture_id, suffix=f"-{mutation}"
    )
    assert decoded.outcome == "unsupported"
    assert decoded.reason_code == "unsupported:capture_handoff"
    assert decoded.artifact is None
    assert artifacts.object_count() == 0


def test_target_raw_tamper_cannot_produce_canonical_evidence(tmp_path):
    source, source_capture, _source_receipt = _source_capture(tmp_path)
    handoff = _handoff_store(tmp_path)
    adapted = adapt_dwd_rv_raw_capture(
        source_capture_id=source_capture.capture_id,
        source_raw_store=source,
        handoff_raw_store=handoff,
    )
    capture = handoff.load_capture(adapted.target_capture_id)
    assert capture is not None
    receipt = handoff.load_receipt(capture.members[0].receipt_id)
    assert receipt is not None
    path = Path(receipt.artifact_path)
    payload = path.read_bytes()
    path.write_bytes(payload[:-1] + bytes([payload[-1] ^ 0xFF]))
    artifacts, evidence = _decoder_stores(tmp_path, handoff, suffix="-tamper")

    with pytest.raises(RawArtifactIntegrityError):
        decode_dwd_rv_capture(
            capture_id=adapted.target_capture_id,
            decoder_job_id=_identity("decoder-job-", "target-tamper"),
            attempt_id=_identity("decoder-attempt-", "target-tamper"),
            raw_store=handoff,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
        )
    assert evidence.stats()["decoder_runs"] == 0
    assert artifacts.object_count() == 0


def test_handoff_is_unwired_from_live_and_science_orchestrator():
    needle = "dwd_rv_capture_handoff"
    for relative in (
        "storm_tracker_worker/service.py",
        "storm_tracker_worker/run.py",
        "storm_tracker_worker/science_shadow_orchestrator.py",
        "custom_components/storm_tracker_v3/__init__.py",
    ):
        path = REPOSITORY / relative
        if path.exists():
            assert needle not in path.read_text(encoding="utf-8")
