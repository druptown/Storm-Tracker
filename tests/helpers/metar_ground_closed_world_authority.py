"""Build an unadmitted, exact-real-METAR authority for process proofs."""
from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Iterable

from storm_tracker_worker import metar_ground_evidence, metar_native
from storm_tracker_worker.canonical_ground_evidence_store import (
    CanonicalGroundEvidenceStore,
    GroundEvidenceLimits,
)
from storm_tracker_worker.ground_fusion_shadow_process_isolation import (
    GROUND_SHADOW_PROCESS_AUTHORITY_CONTRACT,
    GroundShadowFactoryContract,
    GroundShadowIsolatedRunner,
    GroundShadowIsolationLimits,
    canonical_ground_store_identity_sha256,
)
from storm_tracker_worker.ground_fusion_shadow_runtime import (
    GeographicFootprintProof,
    GroundCellTrackBinding,
    GroundFusionShadowLimits,
    GroundFusionShadowReceiptStore,
    GroundFusionShadowRequest,
    GroundFusionShadowRetentionPolicy,
    GroundTargetSnapshot,
    footprint_geometry_sha256,
)
from storm_tracker_worker.hard_decoder_isolation import _canonical_json_bytes
from storm_tracker_worker.metar_ground_shadow_factory import (
    METAR_CLOSED_WORLD_FACTORY_ID,
    METAR_FOOTPRINT_AUTHORITY_CONTRACT,
    METAR_GROUND_FACTORY_CONFIG_CONTRACT,
    METAR_RAW_TREE_MANIFEST_CONTRACT,
    METAR_SEMANTICS_AUTHORITY_CONTRACT,
    METAR_SITE_AUTHORITY_CONTRACT,
    runtime_profile_for_authority,
)
from storm_tracker_worker.raw_artifacts import RawArtifactLimits, RawArtifactStore
from storm_tracker_worker.tracking_core.engine.canonical_ground_fusion_bridge import (
    GroundSemanticsCertificate,
    GroundSemanticsProfile,
)
from storm_tracker_worker.tracking_core.engine.ground_fusion import (
    CellTrackFootprint,
    GroundFusionPolicy,
)
from storm_tracker_worker.tracking_core.engine.physical_site_catalog import (
    PhysicalSiteCatalog,
)


FACTORY_TARGET = (
    "storm_tracker_worker.metar_ground_shadow_factory:"
    "build_closed_world_metar_runtime"
)
REAL_OBSERVED_AT = 1_785_325_500.0  # 2026-07-29T11:45:00Z
REAL_CAPTURE_RECEIVED_AT = 1_785_325_689.2723334
PRIVATE_TARGET = "device_tracker.closed_world_private_target"


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def fixture_request(
    *,
    ordinal: int = 1,
    attempt_id: str | None = None,
) -> GroundFusionShadowRequest:
    """An explicitly unadmitted geometry around a real-cache rain record."""

    latitude = 70.331
    longitude = -149.613
    fixture_science_sha256 = _digest({"fixture_ordinal": ordinal})
    footprint = CellTrackFootprint(
        footprint_id=f"metar-fixture-footprint-{ordinal}",
        cell_track_id=f"metar-fixture-track-{ordinal}",
        cell_detection_id=f"metar-fixture-detection-{ordinal}",
        valid_from=REAL_OBSERVED_AT,
        valid_until=REAL_OBSERVED_AT + 60.0,
        representative_at=REAL_OBSERVED_AT,
        rings=((
            (latitude - 0.04, longitude - 0.08),
            (latitude + 0.04, longitude - 0.08),
            (latitude + 0.04, longitude + 0.08),
            (latitude - 0.04, longitude + 0.08),
        ),),
        radar_source="fixture-only-no-preserved-science-receipt",
        raw_artifact_receipt_id=f"fixture-only-radar-receipt-{ordinal}",
    )
    proof = GeographicFootprintProof(
        proof_id=f"metar-fixture-proof-{ordinal}",
        proof_version="1",
        source_science_receipt_sha256=fixture_science_sha256,
        source_target_grid_sha256="2" * 64,
        source_transform_receipt_sha256="3" * 64,
        geometry_builder_id="fixture-only-unadmitted-polygon",
        geometry_certificate_sha256="4" * 64,
        footprint_geometry_sha256=footprint_geometry_sha256(footprint),
    )
    cell = GroundCellTrackBinding(
        footprint=footprint,
        engine_id="fixture-only-ground-engine",
        engine_state_revision=ordinal,
        track_revision=ordinal,
        analysis_grid_id="fixture-only-grid",
        science_run_id=f"fixture-only-science-run-{ordinal}",
        science_transition_id=f"fixture-only-transition-{ordinal}",
        science_receipt_sha256=fixture_science_sha256,
        source_frame_id=f"fixture-only-frame-{ordinal}",
        target_grid_sha256="2" * 64,
        radar_semantics_certificate_sha256="5" * 64,
        transform_certificate_sha256="6" * 64,
        parent_track_ids=(),
        child_track_ids=(),
        lineage_event_ids=(f"fixture-only-lineage-{ordinal}",),
        applied_lineage_hypothesis_ids=(),
        footprint_proof=proof,
    )
    return GroundFusionShadowRequest(
        attempt_id=attempt_id or f"metar-closed-world-attempt-{ordinal}",
        decision_key=f"private-metAR-decision-{ordinal}",
        region_key="private-metAR-region",
        target=GroundTargetSnapshot(
            target_id=PRIVATE_TARGET,
            target_revision=f"private-revision-{ordinal}",
            latitude=latitude,
            longitude=longitude,
            observed_at=REAL_OBSERVED_AT,
            source_state_sha256="7" * 64,
        ),
        cell=cell,
        evaluated_at=REAL_OBSERVED_AT,
    )


def retry(request: GroundFusionShadowRequest, attempt_id: str) -> GroundFusionShadowRequest:
    return replace(request, attempt_id=attempt_id)


@dataclass(frozen=True)
class ClosedWorldAuthorityBundle:
    root: Path
    authority_path: Path
    authority_sha256: str
    document: dict[str, Any]
    raw_root: Path
    raw_manifest_path: Path
    canonical_path: Path
    receipt_path: Path
    archive_path: Path
    site_path: Path
    semantics_path: Path
    footprint_path: Path
    capture_id: str
    fixture_sha256: str
    decode_fact_count: int
    factory: GroundShadowFactoryContract
    isolation_limits: GroundShadowIsolationLimits
    ground_limits: GroundFusionShadowLimits
    retention: GroundFusionShadowRetentionPolicy

    def runner(self) -> GroundShadowIsolatedRunner:
        return GroundShadowIsolatedRunner(
            authority_path=self.authority_path,
            expected_authority_sha256=self.authority_sha256,
            factory_contract=self.factory,
            limits=self.isolation_limits,
        )

    def receipt_store(self) -> GroundFusionShadowReceiptStore:
        store = GroundFusionShadowReceiptStore(
            self.receipt_path,
            runtime_profile_sha256=self.document["runtime_profile_sha256"],
            limits=self.ground_limits,
        )
        store.initialize()
        return store


def _write_document(path: Path, value: Any) -> str:
    body = _canonical(value)
    path.write_bytes(body)
    return hashlib.sha256(body).hexdigest()


def _raw_manifest(raw_root: Path) -> dict[str, Any]:
    entries = []
    directories = []
    for path in sorted(raw_root.rglob("*")):
        if path.is_dir():
            directories.append(path.relative_to(raw_root).as_posix())
            continue
        body = path.read_bytes()
        entries.append({
            "relative_path": path.relative_to(raw_root).as_posix(),
            "sha256": hashlib.sha256(body).hexdigest(),
            "size_bytes": len(body),
        })
    return {
        "contract": METAR_RAW_TREE_MANIFEST_CONTRACT,
        "directories": directories,
        "directory_count": len(directories),
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": sum(item["size_bytes"] for item in entries),
        "tree_sha256": _digest({
            "directories": directories,
            "entries": entries,
        }),
    }


def _policy_mapping(policy: GroundFusionPolicy) -> dict[str, Any]:
    return {
        "freshness_seconds_by_provider": [
            [provider.casefold(), float(seconds)]
            for provider, seconds in sorted(
                policy.freshness_seconds_by_provider,
                key=lambda item: item[0].casefold(),
            )
        ],
        "maximum_future_skew_seconds": policy.maximum_future_skew_seconds,
        "maximum_site_coordinate_disagreement_km": policy.maximum_site_coordinate_disagreement_km,
        "minimum_independent_wet_groups": policy.minimum_independent_wet_groups,
        "minimum_interval_overlap_fraction": policy.minimum_interval_overlap_fraction,
        "spatial_tolerance_km": policy.spatial_tolerance_km,
        "temporal_tolerance_seconds": policy.temporal_tolerance_seconds,
    }


def _limits_mapping(value: Any) -> dict[str, Any]:
    return {field: getattr(value, field) for field in value.__dataclass_fields__}


def build_closed_world_authority(
    root: Path,
    *,
    cache_path: Path,
    requests: Iterable[GroundFusionShadowRequest] = (),
    isolation_changes: dict[str, Any] | None = None,
) -> ClosedWorldAuthorityBundle:
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    immutable = root / "immutable"
    mutable = root / "mutable"
    raw_root = immutable / "raw"
    immutable.mkdir(exist_ok=True)
    mutable.mkdir(exist_ok=True)
    archive_path = mutable / "archive"
    archive_path.mkdir(exist_ok=True)
    canonical_path = immutable / "canonical-ground.sqlite3"
    receipt_path = mutable / "ground-shadow.sqlite3"
    raw_manifest_path = immutable / "raw-tree.json"
    site_path = immutable / "site-authority.json"
    semantics_path = immutable / "semantics-authority.json"
    footprint_path = immutable / "footprint-authority.json"
    authority_path = root / "metar-ground-authority.json"

    payload = cache_path.read_bytes()
    fixture_sha = hashlib.sha256(payload).hexdigest()
    raw = RawArtifactStore(raw_root, limits=RawArtifactLimits())
    raw.initialize()
    metar_native._inspect_cache(payload, received_at=REAL_CAPTURE_RECEIVED_AT)
    receipt = raw.store(
        provider="metar",
        product=metar_native.METAR_GZIP_PRODUCT,
        fetch_id="closed-world-real-cache:csv_gzip",
        content_type="application/gzip",
        received_at=REAL_CAPTURE_RECEIVED_AT,
        upstream_id=f"aviationweather/metars.cache.csv.gz|content_sha256={fixture_sha}",
        provenance=metar_native._native_provenance(),
        payload=payload,
    )
    capture = raw.commit_capture(
        provider="metar",
        product=metar_native.METAR_CAPTURE_PRODUCT,
        fetch_id="closed-world-real-cache:capture",
        upstream_capture_id=f"{metar_native.METAR_CAPTURE_PRODUCT}|content_sha256={fixture_sha}",
        product_timestamp=None,
        members={"csv_gzip": receipt.receipt_id},
    )
    # The worldwide cache expands beyond the default 64 MiB main-database
    # envelope (one quarter of max_catalog_bytes).  This is an explicit,
    # authority-hashed storage envelope; fact/query/decoder bounds stay exact.
    evidence_limits = GroundEvidenceLimits(
        max_catalog_bytes=1024 * 1024 * 1024,
    )
    evidence = CanonicalGroundEvidenceStore(
        canonical_path,
        raw_artifact_reader=raw,
        limits=evidence_limits,
    )
    decoded = metar_ground_evidence.decode_metar_ground_capture(
        capture.capture_id,
        raw_store=raw,
        evidence_store=evidence,
    )

    review_evidence = {
        "captured_primary_source_documents": False,
        "decoder_config_sha256": metar_ground_evidence.DEFAULT_CONFIG_SHA256,
        "decoder_identity": metar_ground_evidence.DECODER_IDENTITY,
        "decoder_version": metar_ground_evidence.DECODER_VERSION,
        "limits": metar_ground_evidence.MetarGroundDecodeLimits().to_mapping(),
        "status": "internal_review_only_candidate_unadmitted",
    }
    certificate = GroundSemanticsCertificate(
        certificate_id="metar-present-weather-internal-review-candidate",
        certificate_version="1",
        provider="metar",
        product=metar_native.METAR_CAPTURE_PRODUCT,
        decoder_identity=metar_ground_evidence.DECODER_IDENTITY,
        decoder_version=metar_ground_evidence.DECODER_VERSION,
        config_sha256=metar_ground_evidence.DEFAULT_CONFIG_SHA256,
        profile=GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1,
        primary_sources=(
            "https://aviationweather.gov/data/api/",
            "https://aviationweather.gov/help/data/",
            "https://www.weather.gov/media/asos/aum-toc.pdf",
        ),
        review_evidence_sha256=_digest(review_evidence),
    )
    semantics_document = {
        "admission": {
            "granted": False,
            "missing": ["frozen_primary_source_capture", "independent_meteorological_review"],
            "status": "candidate_only_unadmitted",
        },
        "certificates": [{
            "certificate_id": certificate.certificate_id,
            "certificate_version": certificate.certificate_version,
            "config_sha256": certificate.config_sha256,
            "decoder_identity": certificate.decoder_identity,
            "decoder_version": certificate.decoder_version,
            "fingerprint": certificate.fingerprint,
            "primary_sources": list(certificate.primary_sources),
            "product": certificate.product,
            "profile": certificate.profile.value,
            "provider": certificate.provider,
            "review_evidence": review_evidence,
            "review_evidence_sha256": certificate.review_evidence_sha256,
        }],
        "contract": METAR_SEMANTICS_AUTHORITY_CONTRACT,
    }
    semantics_sha = _write_document(semantics_path, semantics_document)

    bindings = []
    for request in sorted(requests, key=lambda item: item.cell.fingerprint):
        bindings.append({
            "cell_binding_sha256": request.cell.fingerprint,
            "decision_key_sha256": request.decision_key_sha256,
            "evaluated_at": request.evaluated_at,
            "footprint_geometry_sha256": footprint_geometry_sha256(request.cell.footprint),
            "footprint_proof_sha256": request.cell.footprint_proof.fingerprint,
            "region_key_sha256": request.region_key_sha256,
            "source_kind": "fixture_only_unadmitted",
            "target_snapshot_sha256": request.target.fingerprint,
        })
    footprint_document = {
        "admission": {
            "granted": False,
            "missing": ["preserved_science_receipt", "certified_target_grid_geometry"],
            "status": "candidate_only_unadmitted",
        },
        "bindings": bindings,
        "contract": METAR_FOOTPRINT_AUTHORITY_CONTRACT,
    }
    footprint_sha = _write_document(footprint_path, footprint_document)

    raw_manifest = _raw_manifest(raw_root)
    raw_manifest_sha = _write_document(raw_manifest_path, raw_manifest)
    canonical_sha = hashlib.sha256(canonical_path.read_bytes()).hexdigest()
    site_admission = {
        "granted": False,
        "missing": [
            "independent_alias_to_physical_site_mapping",
            "independent_station_coordinates",
            "sensor_absence_capability",
            "site_independence_group_evidence",
        ],
        "status": "candidate_only_unadmitted",
    }
    site_assertions = {
        "admission": site_admission,
        "catalog_name": "metar-closed-world-empty-unadmitted",
        "proximity_audit_radius_m": 1_000.0,
        "records": [],
    }
    site_assertions_sha = _digest(site_assertions)
    scope = _digest({
        "canonical_content_sha256": canonical_sha,
        "capture_id": capture.capture_id,
        "footprint_document_sha256": footprint_sha,
        "ground_evidence_limits_sha256": _digest(
            _limits_mapping(evidence_limits)
        ),
        "raw_artifact_limits_sha256": _digest(
            _limits_mapping(RawArtifactLimits())
        ),
        "raw_tree_manifest_sha256": raw_manifest_sha,
        "semantics_document_sha256": semantics_sha,
        "site_assertions_sha256": site_assertions_sha,
    })
    catalog = PhysicalSiteCatalog(
        catalog_name=site_assertions["catalog_name"],
        catalog_version=f"closed-world-scope-{scope[:32]}",
        records=(),
        proximity_audit_radius_m=site_assertions["proximity_audit_radius_m"],
    )
    site_document = {
        "admission": site_admission,
        "authority_scope_sha256": scope,
        "catalog": json.loads(catalog.canonical_json),
        "contract": METAR_SITE_AUTHORITY_CONTRACT,
        "site_assertions_sha256": site_assertions_sha,
    }
    site_sha = _write_document(site_path, site_document)

    policy = GroundFusionPolicy(
        freshness_seconds_by_provider=(("metar", 60.0),),
        minimum_independent_wet_groups=2,
        spatial_tolerance_km=0.0,
        temporal_tolerance_seconds=0.0,
        minimum_interval_overlap_fraction=0.5,
        maximum_site_coordinate_disagreement_km=1.0,
        maximum_future_skew_seconds=0.0,
    )
    ground_limits = GroundFusionShadowLimits(
        max_source_facts=256,
        max_receipts=32,
        max_runtime_seconds=20.0,
        max_query_lookback_seconds=120.0,
        max_target_age_seconds=120.0,
        max_target_future_skew_seconds=60.0,
        max_cell_age_seconds=120.0,
        max_footprint_window_seconds=120.0,
    )
    retention = GroundFusionShadowRetentionPolicy(
        archive_namespace="metar-closed-world-shadow-archive",
        hot_receipts_to_keep=1,
        max_segment_receipts=8,
        max_segment_bytes=4 * 1024 * 1024,
        max_archive_segments=32,
        max_archived_receipts=64,
        max_total_archive_bytes=32 * 1024 * 1024,
    )
    isolation_values = {
        # Full immutable-store hashing happens inside the spawned boundary
        # before the bounded runtime query.  Keep an explicit parent budget
        # large enough for the 129 MiB proof DB on slower Windows hosts.
        "wall_seconds": 60.0,
        "cpu_seconds": 45.0,
        "rss_bytes": 1024 * 1024**2,
        "max_authority_bytes": 256 * 1024,
        "max_request_bytes": 256 * 1024,
        "max_result_bytes": 64 * 1024,
        "max_authorized_paths": 8,
    }
    isolation_values.update(isolation_changes or {})
    isolation = GroundShadowIsolationLimits(**isolation_values)
    factory = GroundShadowFactoryContract(
        factory_id=METAR_CLOSED_WORLD_FACTORY_ID,
        target=FACTORY_TARGET,
        opens_only_authorized_paths=True,
        canonical_raw_reverification=True,
        hard_kill_safe=True,
        transactional_receipt_only=True,
        spawns_descendants=False,
    )
    receipt_path.touch()
    blockers = sorted({
        *site_admission["missing"],
        *semantics_document["admission"]["missing"],
        *footprint_document["admission"]["missing"],
        "held_out_meteorological_validation",
    })
    config = {
        "admission_blockers": blockers,
        "authority_scope_sha256": scope,
        "canonical_content_sha256": canonical_sha,
        "capture_id": capture.capture_id,
        "contract": METAR_GROUND_FACTORY_CONFIG_CONTRACT,
        "footprint_document_sha256": footprint_sha,
        "footprint_label": "footprint_authority",
        "fusion_policy": _policy_mapping(policy),
        "ground_evidence_limits": _limits_mapping(evidence_limits),
        "operational_admission_granted": False,
        "raw_artifact_limits": _limits_mapping(RawArtifactLimits()),
        "raw_manifest_label": "raw_manifest",
        "raw_store_label": "raw_store",
        "raw_tree_manifest_sha256": raw_manifest_sha,
        "semantics_document_sha256": semantics_sha,
        "semantics_label": "semantics_authority",
        "site_catalog_document_sha256": site_sha,
        "site_catalog_label": "site_authority",
    }
    document: dict[str, Any] = {
        "archive_directory_label": "archive",
        "authorized_paths": [
            {"access": "canonical_ground_database", "identity_sha256": canonical_ground_store_identity_sha256(canonical_path), "label": "canonical_ground", "path": str(canonical_path)},
            {"access": "ground_receipt_database", "identity_sha256": None, "label": "receipts", "path": str(receipt_path)},
            {"access": "ground_archive_directory", "identity_sha256": None, "label": "archive", "path": str(archive_path)},
            {"access": "read_directory", "identity_sha256": None, "label": "raw_store", "path": str(raw_root)},
            {"access": "read_file", "identity_sha256": raw_manifest_sha, "label": "raw_manifest", "path": str(raw_manifest_path)},
            {"access": "read_file", "identity_sha256": site_sha, "label": "site_authority", "path": str(site_path)},
            {"access": "read_file", "identity_sha256": semantics_sha, "label": "semantics_authority", "path": str(semantics_path)},
            {"access": "read_file", "identity_sha256": footprint_sha, "label": "footprint_authority", "path": str(footprint_path)},
        ],
        "canonical_ground_label": "canonical_ground",
        "contract": GROUND_SHADOW_PROCESS_AUTHORITY_CONTRACT,
        "factory_config": config,
        "factory_contract_sha256": factory.fingerprint,
        "factory_id": factory.factory_id,
        "ground_limits": ground_limits.to_mapping(),
        "isolation_limits": isolation.to_mapping(),
        "receipt_store_label": "receipts",
        "retention_policy": retention.to_mapping(),
        "runtime_profile_sha256": "0" * 64,
    }
    document["runtime_profile_sha256"] = runtime_profile_for_authority(document)
    authority_body = _canonical_json_bytes(document)
    authority_path.write_bytes(authority_body)
    return ClosedWorldAuthorityBundle(
        root=root,
        authority_path=authority_path,
        authority_sha256=hashlib.sha256(authority_body).hexdigest(),
        document=document,
        raw_root=raw_root,
        raw_manifest_path=raw_manifest_path,
        canonical_path=canonical_path,
        receipt_path=receipt_path,
        archive_path=archive_path,
        site_path=site_path,
        semantics_path=semantics_path,
        footprint_path=footprint_path,
        capture_id=capture.capture_id,
        fixture_sha256=fixture_sha,
        decode_fact_count=decoded.fact_count,
        factory=factory,
        isolation_limits=isolation,
        ground_limits=ground_limits,
        retention=retention,
    )


__all__ = [
    "ClosedWorldAuthorityBundle",
    "PRIVATE_TARGET",
    "REAL_CAPTURE_RECEIVED_AT",
    "REAL_OBSERVED_AT",
    "build_closed_world_authority",
    "fixture_request",
    "retry",
]
