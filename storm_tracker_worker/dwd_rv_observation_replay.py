"""Offline official-DWD +000 replay through the closed completion boundary.

This module is intentionally unwired.  It imports exact tar bytes already on
disk, preserves all +005..+120 members only inside the raw archive, publishes
one lifecycle completion per +000 base time, invokes the existing hard DWD
child, acknowledges only a verified durable output, and exports the existing
``ShadowEvidenceFrame`` algebra.  It performs no fetch, forecast, operational
admission, service, MQTT or Home Assistant action.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import multiprocessing
import os
from pathlib import Path
import threading
import time
from typing import Any, Mapping

from .acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
    DWD_COMPLETION_PROOF_SCHEMA,
    desired_authority_proof,
)
from .analysis_grid_transform import (
    AnalysisGridTransformConfig,
    ExactAffineGridTransformAdapter,
    NativeFieldCandidateBinding,
    native_candidate_content_sha256,
)
from .canonical_evidence_store import CanonicalEvidenceStore
from .canonical_radar_field_bridge import (
    ProviderDerivedArtifactAuthority,
    default_radar_semantics_certificates,
    derive_canonical_radar_field,
)
from .dwd_rv_analysis_grid_certificate import (
    issue_dwd_rv_native_grid_certificate,
)
from .dwd_rv_capture_handoff import (
    HANDOFF_ADAPTER_ID,
    HANDOFF_ADAPTER_VERSION,
    HANDOFF_CONFIG_SHA256,
    SOURCE_ARCHIVE_PRODUCT,
    SOURCE_CAPTURE_PRODUCT,
    SOURCE_COLLECTOR,
    SOURCE_COLLECTOR_VERSION,
    SOURCE_PROVIDER,
    SOURCE_TAR_INSPECTOR_VERSION,
    TARGET_CAPTURE_PRODUCT,
    TARGET_PROVIDER,
    adapt_dwd_rv_raw_capture,
)
from .dwd_rv_completion_consumer import (
    CONSUMER_ID,
    STREAM_ID,
    DwdRvCompletionConsumer,
    DwdRvCompletionOutputStore,
    DwdRvPipelineOutcomeAuthority,
    DwdRvShadowChainProfile,
)
from .dwd_rv_completion_process_isolation import (
    DwdRvCompletionIsolatedRunner,
    DwdRvCompletionIsolationLimits,
)
from .dwd_rv_quantitative import DwdRvDerivedArtifactStore
from .dwd_rv_science_sequence import (
    DwdRvProcessMetric,
    DwdRvProcessMetricsReceipt,
    DwdRvScienceEvidenceReceipt,
    DwdRvScienceSequenceIntegrityError,
    DwdRvScienceSequenceReceipt,
    DwdRvSourceManifestReceipt,
    inventory_source_archives,
    load_source_manifest,
    seal_process_metrics_manifest,
    seal_sequence_manifest,
    write_evidence_record,
)
from .desired_state import DesiredState, parse_desired_state
from .evidence_to_cells_shadow import EvidenceToCellsShadowConfig
from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactReceipt,
    RawArtifactStore,
)
from .science_shadow_orchestrator import (
    CanonicalEvidenceCellComposer,
    ShadowEvidenceFrame,
)
from .tracking_core.engine.analysis_grid import AnalysisGrid
from .tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
)


DWD_PROJDEF = (
    "+proj=stere +lat_ts=60 +lat_0=90 +lon_0=10 "
    "+x_0=543196.83521776402 +y_0=3622588.8619310022 "
    "+units=m +a=6378137 +b=6356752.3142451802 +no_defs"
)
REPLAY_SCHEMA = "stv3-dwd-rv-observation-replay-v1"


class DwdRvObservationReplayError(RuntimeError):
    """The offline replay cannot preserve an exact closed contract."""


class _MutableClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class _TokenFactory:
    def __init__(self) -> None:
        self._counter = 0

    def __call__(self) -> str:
        self._counter += 1
        return hashlib.sha256(
            f"dwd-observation-replay-lease:{self._counter}".encode("ascii")
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class DwdRvObservationReplayReceipt:
    source_manifest: DwdRvSourceManifestReceipt
    sequence: DwdRvScienceSequenceReceipt
    process_metrics: DwdRvProcessMetricsReceipt
    evidence_records: tuple[DwdRvScienceEvidenceReceipt, ...]
    isolation_authority_sha256: str
    isolation_limits_sha256: str
    profile_sha256: str
    active_decoder_children_after_run: int


def default_belgium_observation_profile() -> DwdRvShadowChainProfile:
    """A 1-km source-aligned grid covering Belgium and its near surroundings."""

    pixels = 316 * 316
    return DwdRvShadowChainProfile(
        target_grid=AnalysisGrid(
            grid_id="dwd-rv-belgium-observation-1km-v1",
            crs=DWD_PROJDEF,
            origin_x_m=-500.0,
            origin_y_m=-449_500.0,
            x_resolution_m=1_000.0,
            y_resolution_m=1_000.0,
            width=316,
            height=316,
        ),
        transform_config=AnalysisGridTransformConfig(
            max_source_pixels=1_500_000,
            max_target_pixels=pixels,
            max_array_bytes=512 * 1024 * 1024,
            max_overlap_contributions=2_000_000,
        ),
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
            max_target_pixels=pixels,
            max_detection_objects=pixels * 2,
            max_receipt_bytes=64 * 1024 * 1024,
            max_runtime_seconds=75.0,
        ),
    )


def default_replay_desired_state(first_valid_at: float) -> DesiredState:
    issued_at = float(first_valid_at) - 3_600.0
    return parse_desired_state({
        "authority_epoch": 1,
        "contract": "stv3-worker-desired-state-v1",
        "generation": 1,
        "issued_at": issued_at,
        "previous_state_sha256": None,
        "producer_id": "offline-dwd-observation-replay",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": "Offline Belgium replay target",
            "kind": "home",
            "latitude": 51.04,
            "location_accuracy_m": None,
            "longitude": 4.48,
            "observation_radius_km": 250.0,
            "position_observed_at": issued_at,
            "target_id": "offline.replay.belgium",
        }],
    })


def _safe_receipt(receipt: RawArtifactReceipt) -> dict[str, Any]:
    return {
        "content_sha256": receipt.content_sha256,
        "content_type": receipt.content_type,
        "created_at": receipt.created_at,
        "fetch_id": receipt.fetch_id,
        "product": receipt.product,
        "provenance": receipt.provenance,
        "provenance_sha256": receipt.provenance_sha256,
        "provider": receipt.provider,
        "receipt_id": receipt.receipt_id,
        "receipt_sha256": receipt.receipt_sha256,
        "received_at": receipt.received_at,
        "schema": "stv3-worker-raw-receipt-safe-evidence-v1",
        "size_bytes": receipt.size_bytes,
        "upstream_id": receipt.upstream_id,
    }


def _proof(
    *,
    job_id: str,
    evaluated_at: float,
    desired: DesiredState,
    source_capture: Any,
    source_receipt: RawArtifactReceipt,
    adapted: Any,
    target_capture: Any,
    target_receipts: Mapping[str, RawArtifactReceipt],
) -> dict[str, Any]:
    authority = desired_authority_proof(desired)
    return {
        "adapter": {
            "adapter_config_sha256": HANDOFF_CONFIG_SHA256,
            "adapter_id": HANDOFF_ADAPTER_ID,
            "adapter_version": HANDOFF_ADAPTER_VERSION,
            "receipt": asdict(adapted),
        },
        "capacity_receipt": {
            "accounting_scope": "offline_exact_raw_archive",
            "schema": "stv3-offline-replay-capacity-proof-v1",
        },
        "desired_authority": authority,
        "execution_receipts": [],
        "handoff_capture": target_capture.to_metadata(),
        "handoff_receipts": {
            role: _safe_receipt(receipt)
            for role, receipt in target_receipts.items()
        },
        "meteorological_claim": False,
        "observation_policy": "base_plus000_only",
        "raw_refetch_performed": False,
        "scheduler_terminal": {
            "desired_state_sha256": desired.state_sha256,
            "last_error_code": None,
            "last_transition": "completed",
            "state": "completed",
            "terminal_at": evaluated_at,
        },
        "schema": DWD_COMPLETION_PROOF_SCHEMA,
        "source_capture": source_capture.to_metadata(),
        "source_receipts": {"rv_tar": _safe_receipt(source_receipt)},
        "workload": {
            "job_id": job_id,
            "product_id": "dwd_radolan_rv_native_tar",
            "provider_id": "dwd",
            "secret_requirement_ids": [],
            "snapshot_sha256": desired.state_sha256,
            "workload_id": STREAM_ID,
        },
    }


def _compose_evidence(
    *,
    run_id: str,
    artifact_store: DwdRvDerivedArtifactStore,
    evidence_store: CanonicalEvidenceStore,
    profile: DwdRvShadowChainProfile,
) -> ShadowEvidenceFrame:
    authority = ProviderDerivedArtifactAuthority({"dwd_rv_v1": artifact_store})
    semantics = tuple(
        item for item in default_radar_semantics_certificates()
        if item.provider == TARGET_PROVIDER and item.product == TARGET_CAPTURE_PRODUCT
    )
    if len(semantics) != 1:
        raise DwdRvObservationReplayError("DWD semantics authority is ambiguous")
    deadline = time.monotonic() + profile.pipeline_config.max_runtime_seconds
    bridged = derive_canonical_radar_field(
        evidence_store,
        authority,
        run_id=run_id,
        certificates=semantics,
        deadline_at=deadline,
        monotonic=time.monotonic,
    )
    if not bridged.is_candidate or bridged.candidate is None:
        raise DwdRvObservationReplayError("DWD canonical bridge withheld replay")
    native = bridged.candidate
    native_sha = native_candidate_content_sha256(native)
    certificate = issue_dwd_rv_native_grid_certificate(
        binding=NativeFieldCandidateBinding(
            candidate=native,
            candidate_content_sha256=native_sha,
            semantics_certificate_sha256=native.semantics_certificate_sha256,
        ),
        target_grid=profile.target_grid,
        config=profile.transform_config,
    )
    composer = CanonicalEvidenceCellComposer(
        evidence_store,
        authority,
        radar_semantics_certificates=semantics,
        target_grid=profile.target_grid,
        transform_adapter=ExactAffineGridTransformAdapter(certificate),
        transform_config=profile.transform_config,
        detector_config=profile.detector_config,
        pipeline_config=profile.pipeline_config,
    )
    result = composer.compose(
        run_id=run_id,
        deadline_at=time.monotonic() + profile.pipeline_config.max_runtime_seconds,
        monotonic=time.monotonic,
    )
    if not result.is_candidate or result.evidence is None:
        raise DwdRvObservationReplayError("DWD cells replay was withheld")
    return result.evidence


def _source_frame(path: Path) -> tuple[float, str, str]:
    try:
        stamp = path.stem.removeprefix("composite_rv_")
        valid_at = datetime.strptime(stamp, "%Y%m%d_%H%M").replace(
            tzinfo=timezone.utc
        ).timestamp()
    except ValueError as exc:
        raise DwdRvObservationReplayError("DWD archive name is invalid") from exc
    member = f"composite_rv_{stamp}_000-hd5"
    compact = stamp.replace("_", "")
    return valid_at, member, compact


def build_dwd_rv_observation_replay(
    *,
    source_directory: str | Path,
    work_root: str | Path,
    export_root: str | Path,
    isolation_limits: DwdRvCompletionIsolationLimits,
    profile: DwdRvShadowChainProfile | None = None,
    expected_frame_count: int = 36,
) -> DwdRvObservationReplayReceipt:
    """Build one content-addressed sequence using one serial hard-child lane."""

    if not isinstance(isolation_limits, DwdRvCompletionIsolationLimits):
        raise TypeError("isolation_limits has wrong type")
    source = Path(source_directory).resolve(strict=True)
    work = Path(work_root).resolve(strict=False)
    export = Path(export_root).resolve(strict=False)
    if not work.is_absolute() or not export.is_absolute():
        raise ValueError("replay roots must be absolute")
    work.mkdir(parents=True, exist_ok=True)
    export.mkdir(parents=True, exist_ok=True)
    source_manifest = inventory_source_archives(
        source, export, expected_frame_count=expected_frame_count
    )
    source_document = load_source_manifest(source_manifest)
    desired = default_replay_desired_state(source_manifest.valid_times[0])
    authority = desired_authority_proof(desired)
    profile = profile or default_belgium_observation_profile()
    clock = _MutableClock(source_manifest.valid_times[0] + 120.0)
    source_store = RawArtifactStore((work / "source-raw").resolve())
    source_store.initialize()
    handoff_store = RawArtifactStore((work / "handoff-raw").resolve())
    handoff_store.initialize()
    lifecycle_kwargs: dict[str, Any] = {
        "clock": clock,
        "token_factory": _TokenFactory(),
    }
    if os.name == "nt":
        # The store intentionally has no generic Windows directory-fsync
        # primitive.  This exception is confined to the offline replay
        # artifact builder; Linux worker execution retains the real fsync.
        lifecycle_kwargs["directory_fsync"] = lambda _path: None
    lifecycle = AcquisitionEvidenceLifecycleStore(
        (work / "lifecycle").resolve(), **lifecycle_kwargs
    )
    artifacts = DwdRvDerivedArtifactStore((work / "derived").resolve())
    evidence_store = CanonicalEvidenceStore(
        (work / "canonical.sqlite3").resolve(),
        raw_artifact_reader=handoff_store,
        clock=clock,
    )
    output_store = DwdRvCompletionOutputStore(
        (work / "outputs.sqlite3").resolve(), clock=clock
    )
    runner = DwdRvCompletionIsolatedRunner(
        handoff_raw_store=handoff_store,
        artifact_store=artifacts,
        evidence_store=evidence_store,
        output_store=output_store,
        profile=profile,
        limits=isolation_limits,
    )
    consumer = DwdRvCompletionConsumer(
        lifecycle_store=lifecycle,
        handoff_raw_store=handoff_store,
        artifact_store=artifacts,
        evidence_store=evidence_store,
        output_store=output_store,
        profile=profile,
        process_runner=runner,
        clock=clock,
    )
    outcome_authority = DwdRvPipelineOutcomeAuthority(
        output_store=output_store,
        profile=profile,
        process_isolation_authority_sha256=runner.authority_sha256,
    )
    records: list[DwdRvScienceEvidenceReceipt] = []
    metrics: list[DwdRvProcessMetric] = []
    paths = sorted(source.iterdir(), key=lambda item: item.name)
    if len(paths) != expected_frame_count:
        raise DwdRvScienceSequenceIntegrityError("source count changed after inventory")
    for frame_number, path in enumerate(paths):
        valid_at, observation_member, compact = _source_frame(path)
        if valid_at != source_manifest.valid_times[frame_number]:
            raise DwdRvScienceSequenceIntegrityError("source time changed after inventory")
        source_entry = source_document["frames"][frame_number]
        expected_bytes = source_entry["archive_bytes"]
        with path.open("rb") as archive_file:
            payload = archive_file.read(expected_bytes + 1)
        content_sha = hashlib.sha256(payload).hexdigest()
        if (
            source_entry["archive_name"] != path.name
            or source_entry["archive_bytes"] != len(payload)
            or source_entry["archive_sha256"] != content_sha
            or source_entry["observation_member"] != observation_member
        ):
            raise DwdRvScienceSequenceIntegrityError(
                "source archive differs after fixed inventory"
            )
        received_at = valid_at + 60.0
        evaluated_at = received_at + 30.0
        clock.value = evaluated_at + 1.0
        job_id = f"job-dwd-offline-replay-{compact}"
        source_receipt = source_store.store(
            provider=SOURCE_PROVIDER,
            product=SOURCE_ARCHIVE_PRODUCT,
            fetch_id=f"{job_id}:rv_tar",
            content_type="application/x-tar",
            received_at=received_at,
            upstream_id=(
                f"{SOURCE_ARCHIVE_PRODUCT}|latest|content_sha256={content_sha}"
            ),
            provenance={
                "capture_stage": NATIVE_CAPTURE_STAGE,
                "collector": SOURCE_COLLECTOR,
                "collector_version": SOURCE_COLLECTOR_VERSION,
                "provenance_scope": "exact_upstream_tar_bytes_only",
                "representation": NATIVE_REPRESENTATION,
                "structure_inspector_version": SOURCE_TAR_INSPECTOR_VERSION,
            },
            payload=payload,
        )
        source_capture = source_store.commit_capture(
            provider=SOURCE_PROVIDER,
            product=SOURCE_CAPTURE_PRODUCT,
            fetch_id=f"{job_id}:capture",
            upstream_capture_id=(
                f"{SOURCE_CAPTURE_PRODUCT}|member={observation_member}"
                f"|content_sha256={content_sha}"
            ),
            product_timestamp=valid_at,
            members={"rv_tar": source_receipt.receipt_id},
        )
        adapted = adapt_dwd_rv_raw_capture(
            source_capture_id=source_capture.capture_id,
            source_raw_store=source_store,
            handoff_raw_store=handoff_store,
        )
        target_bundle = handoff_store.load_capture_bundle_bytes(
            adapted.target_capture_id
        )
        if target_bundle is None:
            raise DwdRvObservationReplayError("handoff capture disappeared")
        target_capture, target_receipts, target_payloads = target_bundle
        if target_payloads != {"rv_tar": payload}:
            raise DwdRvObservationReplayError("handoff did not preserve exact tar")
        producer = lifecycle.publish(
            acquisition_job_id=job_id,
            consumer_id=CONSUMER_ID,
            stream_id=STREAM_ID,
            desired_state_sha256=desired.state_sha256,
            authority_sha256=authority["authority_sha256"],
            proof=_proof(
                job_id=job_id,
                evaluated_at=evaluated_at,
                desired=desired,
                source_capture=source_capture,
                source_receipt=source_receipt,
                adapted=adapted,
                target_capture=target_capture,
                target_receipts=target_receipts,
            ),
            payload=payload,
        )
        lease = lifecycle.claim_next(
            CONSUMER_ID,
            STREAM_ID,
            "offline-dwd-replay-owner",
            lease_seconds=isolation_limits.wall_seconds + 30.0,
            now=clock(),
        )
        if lease is None or lease.event_id != producer.event_id:
            raise DwdRvObservationReplayError("serial completion cursor differs")
        outcome, diagnostic = consumer.process_leased_with_diagnostics(
            lease, desired, threading.Event()
        )
        verified = outcome_authority.verify(
            lease=lease, desired_state=desired, outcome=outcome
        )
        consumption = lifecycle.acknowledge(
            lease.event_id,
            lease.lease_token,
            output_sha256=verified.output_sha256,
            now=clock(),
        )
        if consumption.output_sha256 != verified.output_sha256:
            raise DwdRvObservationReplayError("completion acknowledgement differs")
        output = output_store.load(lease.event_id)
        if output is None:
            raise DwdRvObservationReplayError("durable completion output is absent")
        elapsed = diagnostic.get("elapsed_seconds")
        cpu = diagnostic.get("child_cpu_seconds")
        rss = diagnostic.get("child_peak_rss_bytes")
        boundary = diagnostic.get("child_peak_boundary_bytes")
        if (
            diagnostic.get("status") != "committed"
            or isinstance(elapsed, bool)
            or not isinstance(elapsed, (int, float))
            or isinstance(cpu, bool)
            or not isinstance(cpu, (int, float))
            or isinstance(rss, bool)
            or not isinstance(rss, int)
            or rss <= 0
            or isinstance(boundary, bool)
            or not isinstance(boundary, int)
            or boundary <= 0
        ):
            raise DwdRvObservationReplayError("hard-child metrics are incomplete")
        shadow = _compose_evidence(
            run_id=output.decoder_run_id,
            artifact_store=artifacts,
            evidence_store=evidence_store,
            profile=profile,
        )
        if (
            shadow.evidence_content_sha256 != output.candidate_content_sha256
            or shadow.normalized_frame.valid_at != valid_at
        ):
            raise DwdRvObservationReplayError("recomposed science evidence differs")
        records.append(write_evidence_record(
            export,
            evidence=shadow,
            completion_receipt=output,
            lead_seconds=0,
        ))
        metrics.append(DwdRvProcessMetric(
            frame_number=frame_number,
            valid_at=valid_at,
            event_id=lease.event_id,
            event_sha256=lease.event_sha256,
            output_sha256=output.output_sha256,
            elapsed_seconds=float(elapsed),
            child_cpu_seconds=float(cpu),
            child_peak_rss_bytes=rss,
            child_peak_boundary_bytes=boundary,
        ))
    sequence = seal_sequence_manifest(
        export, source_manifest=source_manifest, evidence_records=records
    )
    process_metrics = seal_process_metrics_manifest(
        export,
        source_manifest=source_manifest,
        sequence=sequence,
        isolation_authority_sha256=runner.authority_sha256,
        isolation_limits_sha256=isolation_limits.fingerprint,
        metrics=metrics,
        configured_boundary_memory_cap_bytes=isolation_limits.rss_bytes,
        configured_cpu_cap_seconds=isolation_limits.cpu_seconds,
        configured_wall_cap_seconds=isolation_limits.wall_seconds,
    )
    active_decoder_children = tuple(
        child for child in multiprocessing.active_children()
        if child.name == "storm-tracker-decoder-one-shot" and child.is_alive()
    )
    if active_decoder_children:
        raise DwdRvObservationReplayError(
            "hard decoder child remained alive after serial replay"
        )
    return DwdRvObservationReplayReceipt(
        source_manifest=source_manifest,
        sequence=sequence,
        process_metrics=process_metrics,
        evidence_records=tuple(records),
        isolation_authority_sha256=runner.authority_sha256,
        isolation_limits_sha256=isolation_limits.fingerprint,
        profile_sha256=profile.fingerprint,
        active_decoder_children_after_run=0,
    )


def receipt_summary(receipt: DwdRvObservationReplayReceipt) -> dict[str, Any]:
    """Return a path-free JSON summary suitable for a script or test log."""

    return {
        "contract": REPLAY_SCHEMA,
        "active_decoder_children_after_run": (
            receipt.active_decoder_children_after_run
        ),
        "evidence_records": len(receipt.evidence_records),
        "frame_count": receipt.sequence.frame_count,
        "isolation_authority_sha256": receipt.isolation_authority_sha256,
        "isolation_limits_sha256": receipt.isolation_limits_sha256,
        "metrics_manifest_sha256": receipt.process_metrics.manifest_sha256,
        "peak_rss_max_bytes": receipt.process_metrics.peak_rss_max_bytes,
        "peak_boundary_max_bytes": (
            receipt.process_metrics.peak_boundary_max_bytes
        ),
        "profile_sha256": receipt.profile_sha256,
        "sequence_id": receipt.sequence.sequence_id,
        "sequence_manifest_sha256": receipt.sequence.manifest_sha256,
        "source_manifest_sha256": receipt.source_manifest.manifest_sha256,
    }


__all__ = [
    "DWD_PROJDEF",
    "DwdRvObservationReplayError",
    "DwdRvObservationReplayReceipt",
    "build_dwd_rv_observation_replay",
    "default_belgium_observation_profile",
    "default_replay_desired_state",
    "receipt_summary",
]
