"""Spawn-importable synthetic factory for ground process-boundary proofs.

The factory constructs the real canonical store and durable ground runtime.
The canonical store is empty, so no synthetic observation or bulk evidence is
transported through IPC.  Fault modes alter only process timing/termination.
"""
from __future__ import annotations

import os
from pathlib import Path
import time
from typing import Any, Mapping

from storm_tracker_worker.canonical_ground_evidence_store import (
    CanonicalGroundEvidenceStore,
)
from storm_tracker_worker.ground_fusion_shadow_runtime import (
    DurableGroundFusionShadowRuntime,
    GroundFusionShadowLimits,
)
from storm_tracker_worker.tracking_core.engine.canonical_ground_fusion_bridge import (
    GroundSemanticsCertificate,
    GroundSemanticsProfile,
)
from storm_tracker_worker.tracking_core.engine.ground_fusion import GroundFusionPolicy
from storm_tracker_worker.tracking_core.engine.physical_site_catalog import (
    PhysicalSiteCatalog,
    PhysicalSiteRecord,
    SensorCapability,
    VerificationEvidence,
    VerifiedStationAlias,
)


NOW = 1_800_000_000.0


class _EmptyRawReader:
    def load_capture_receipt(self, _capture_id: str):
        raise AssertionError("an empty canonical store must never request raw evidence")


def initialize_canonical_ground(path: str | Path) -> CanonicalGroundEvidenceStore:
    return CanonicalGroundEvidenceStore(
        Path(path),
        raw_artifact_reader=_EmptyRawReader(),
    )


def _verification(label: str) -> VerificationEvidence:
    return VerificationEvidence(
        authority="process-test-authority",
        reference=f"https://example.test/{label}",
        verified_at=NOW - 10_000.0,
        content_sha256="c" * 64,
    )


def _catalog() -> PhysicalSiteCatalog:
    alias = VerifiedStationAlias(
        provider_id="metar",
        station_id="PROCESS-STATION-MUST-NOT-LEAK",
        mapping_evidence=_verification("alias"),
    )
    record = PhysicalSiteRecord(
        site_key="process-test-site",
        latitude=40.75,
        longitude=-73.98,
        coordinate_tolerance_m=500.0,
        independence_group_key="process-test-independent-site",
        site_evidence=_verification("site"),
        independence_evidence=_verification("group"),
        aliases=(alias,),
        capabilities=(
            SensorCapability(
                provider_id="metar",
                sensor_id=alias.station_id,
                measurement_kind=(
                    GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1.value
                ),
                can_observe_absence=True,
                evidence=_verification("capability"),
            ),
        ),
    )
    return PhysicalSiteCatalog(
        catalog_name="ground-process-test-catalog",
        catalog_version="1",
        records=(record,),
        proximity_audit_radius_m=1_000.0,
    )


def _certificate() -> GroundSemanticsCertificate:
    return GroundSemanticsCertificate(
        certificate_id="process-test-metar-semantics",
        certificate_version="1",
        provider="metar",
        product="metar-process-test",
        decoder_identity="metar-process-test-decoder",
        decoder_version="1",
        config_sha256="a" * 64,
        profile=GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1,
        primary_sources=("https://example.test/metar-contract",),
        review_evidence_sha256="b" * 64,
    )


def _policy() -> GroundFusionPolicy:
    return GroundFusionPolicy(
        freshness_seconds_by_provider=(("metar", 600.0),),
        minimum_independent_wet_groups=2,
        spatial_tolerance_km=0.0,
        temporal_tolerance_seconds=0.0,
        minimum_interval_overlap_fraction=0.5,
        maximum_site_coordinate_disagreement_km=1.0,
        maximum_future_skew_seconds=60.0,
    )


def _path(authority: Mapping[str, Any], field: str) -> Path:
    label = authority[field]
    return Path(next(
        item["path"]
        for item in authority["authorized_paths"]
        if item["label"] == label
    ))


def _core_runtime(authority: Mapping[str, Any]) -> DurableGroundFusionShadowRuntime:
    evidence = initialize_canonical_ground(
        _path(authority, "canonical_ground_label")
    )
    return DurableGroundFusionShadowRuntime(
        evidence,
        site_catalog=_catalog(),
        semantics_certificates=(_certificate(),),
        fusion_policy=_policy(),
        receipt_path=_path(authority, "receipt_store_label"),
        limits=GroundFusionShadowLimits(**dict(authority["ground_limits"])),
    )


def runtime_profile_for_authority(authority: Mapping[str, Any]) -> str:
    """Initialize the exact test store and derive its deterministic profile."""

    return _core_runtime(authority).runtime_profile_sha256


def _hang(*_args, **_kwargs):
    while True:
        time.sleep(60.0)


def _burn_cpu(*_args, **_kwargs):
    value = 1
    while True:
        value = (value * 1_103_515_245 + 12_345) & 0x7FFFFFFF


def _crash(*_args, **_kwargs):
    os._exit(83)


def _exhaust_memory(chunk_bytes: int) -> None:
    chunks: list[bytearray] = []
    while True:
        chunk = bytearray(chunk_bytes)
        for offset in range(0, len(chunk), 4096):
            chunk[offset] = 1
        chunks.append(chunk)


def build_test_runtime(
    authority: Mapping[str, Any],
) -> DurableGroundFusionShadowRuntime:
    """Audited factory target selected by the checksummed test authority."""

    mode = authority["factory_config"].get("mode", "normal")
    if mode == "rss":
        _exhaust_memory(
            int(authority["factory_config"].get("chunk_bytes", 8 * 1024**2))
        )
    if mode == "factory_crash":
        os._exit(82)
    runtime = _core_runtime(authority)
    if mode == "hang":
        runtime.process = _hang
    elif mode == "cpu":
        runtime.process = _burn_cpu
    elif mode == "crash":
        runtime.process = _crash
    elif mode == "after_commit_hang":
        store = runtime.receipt_store
        original = store.record_with_status

        def commit_then_hang(*args, **kwargs):
            original(*args, **kwargs)
            while True:
                time.sleep(60.0)

        store.record_with_status = commit_then_hang
    elif mode == "output":
        from storm_tracker_worker import ground_fusion_shadow_process_isolation as isolation

        original_metadata = isolation._ground_receipt_metadata

        def oversized_metadata(*args, **kwargs):
            result = original_metadata(*args, **kwargs)
            result["process_test_padding"] = "x" * 16_384
            return result

        isolation._ground_receipt_metadata = oversized_metadata
    elif mode != "normal":
        raise ValueError("unknown ground process test mode")
    return runtime


__all__ = [
    "build_test_runtime",
    "initialize_canonical_ground",
    "runtime_profile_for_authority",
]
