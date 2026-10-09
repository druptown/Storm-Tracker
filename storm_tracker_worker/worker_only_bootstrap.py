"""Idempotently materialize the pinned worker-only runtime on HA Supervisor.

The bootstrap writes only worker-local configuration and immutable authority
state below one explicit persistent root.  It never reads provider secrets,
connects to MQTT or starts acquisition.  Existing bytes are accepted only when
they are identical to the freshly derived canonical configuration.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile

from .acquisition_activation import ACTIVATION_RULES
from .analysis_grid_transform import AnalysisGridTransformConfig
from .blitzortung_event_ingress import BlitzortungEventIngressLimits
from .dwd_rv_completion_consumer import DwdRvShadowChainProfile
from .dwd_rv_completion_process_isolation import DwdRvCompletionIsolationLimits
from .evidence_to_cells_shadow import EvidenceToCellsShadowConfig
from .raw_artifacts import RawArtifactLimits
from .raw_capacity_ledger import ProviderCapacityLimit, RawCapacityProfile
from .raw_capture_service_composition import RawCaptureServiceConfig
from .tracking_core.engine.analysis_grid import AnalysisGrid
from .tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
)
from .worker_first_composition import (
    WorkerFirstCompositionConfig,
    WorkerFirstCompositionPaths,
    initialize_worker_first_composition_authority,
)
from .worker_first_service_composition import WorkerFirstServiceSourcePaths


CONTRACT = "stv3-worker-only-bootstrap-v3"
DEFAULT_ROOT = Path("/data/storm_tracker_worker_runtime_production_v1")
CONFIG_FILENAME = "worker-only-config.json"
# Keep the configured envelope comfortably below the actual 160 GB NAS VM
# filesystem.  The retention guard reserves the complete unused RAW envelope,
# both 4 GiB catalog ceilings and 2 GiB of host headroom before it admits a
# provider cycle.  A 128 GiB RAW ceiling therefore required roughly 138 GiB
# free and incorrectly blocked an otherwise empty 160 GB installation.
PRODUCTION_RAW_CAPACITY_BYTES = 96 * 1024 * 1024 * 1024
_LEGACY_PRODUCTION_RAW_CAPACITY_BYTES = 128 * 1024 * 1024 * 1024
PRODUCTION_ENTRY_CAPACITY = 1_000_000
PRODUCTION_RESERVATION_CAPACITY = 250_000
PRODUCTION_CAPTURE_CAPACITY = 250_000
PRODUCTION_CATALOG_CAPACITY_BYTES = 4 * 1024 * 1024 * 1024
PROJDEF = (
    "+proj=stere +lat_ts=60 +lat_0=90 +lon_0=10 "
    "+x_0=543196.83521776402 +y_0=3622588.8619310022 "
    "+units=m +a=6378137 +b=6356752.3142451802 +no_defs"
)


class WorkerOnlyBootstrapError(RuntimeError):
    """Persistent configuration cannot be derived or safely published."""


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _paths(root: Path) -> WorkerFirstCompositionPaths:
    pipeline = root / "pipeline"
    control = pipeline / "control"
    return WorkerFirstCompositionPaths.from_mapping({
        "acquisition_completion_root": str(pipeline / "completion"),
        "authority_file": str(control / "authority.json"),
        "desired_state_db": str(pipeline / "desired.sqlite3"),
        "dwd_canonical_evidence_db": str(pipeline / "canonical.sqlite3"),
        "dwd_derived_artifact_root": str(pipeline / "derived"),
        "dwd_handoff_raw_root": str(pipeline / "handoff"),
        "operational_fact_db": str(pipeline / "facts.sqlite3"),
        "operational_snapshot_db": str(pipeline / "snapshots.sqlite3"),
        "ownership_lock_file": str(control / "ownership.lock"),
        "pipeline_output_db": str(pipeline / "outputs.sqlite3"),
        "snapshot_outbox_db": str(pipeline / "publication.sqlite3"),
    })


def _profile() -> DwdRvShadowChainProfile:
    return DwdRvShadowChainProfile(
        target_grid=AnalysisGrid(
            grid_id="worker-only-bootstrap-dwd-shadow-2x2",
            crs=PROJDEF,
            origin_x_m=-500.0,
            origin_y_m=500.0,
            x_resolution_m=1000.0,
            y_resolution_m=1000.0,
            width=2,
            height=2,
        ),
        transform_config=AnalysisGridTransformConfig(
            max_source_pixels=1_500_000,
            max_target_pixels=16,
            max_array_bytes=256 * 1024 * 1024,
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
            max_target_pixels=16,
            max_detection_objects=32,
            max_receipt_bytes=4 * 1024 * 1024,
            max_runtime_seconds=5.0,
        ),
    )


def _capacity_contract(
    *,
    raw_capacity_bytes: int = PRODUCTION_RAW_CAPACITY_BYTES,
) -> tuple[RawCapacityProfile, RawArtifactLimits]:
    active = tuple(
        rule for rule in ACTIVATION_RULES.values() if rule.executable
    )
    if not active:
        raise WorkerOnlyBootstrapError("executable acquisition catalog is empty")
    required_bytes = sum(int(rule.maximum_transport_bytes or 0) for rule in active)
    required_entries = sum(int(rule.maximum_artifacts or 0) for rule in active)
    maximum_object_bytes = max(
        int(rule.maximum_transport_bytes or 0) for rule in active
    )
    global_bytes = max(
        raw_capacity_bytes,
        required_bytes * 2,
        maximum_object_bytes * 2,
    )
    global_entries = max(PRODUCTION_ENTRY_CAPACITY, required_entries * 2)
    providers = tuple(sorted({rule.provider_id for rule in active}))
    return (
        RawCapacityProfile(
            global_max_bytes=global_bytes,
            global_max_entries=global_entries,
            providers=tuple(
                ProviderCapacityLimit(provider, global_bytes, global_entries)
                for provider in providers
            ),
            max_reservations=max(
                PRODUCTION_RESERVATION_CAPACITY,
                len(active) * 2,
            ),
            max_catalog_bytes=PRODUCTION_CATALOG_CAPACITY_BYTES,
            sqlite_busy_timeout_seconds=2.0,
        ),
        RawArtifactLimits(
            max_object_bytes=maximum_object_bytes,
            max_total_bytes=global_bytes,
            max_object_entries=global_entries,
            max_staging_entries=max(required_entries, 256),
            max_receipt_entries=global_entries,
            max_capture_entries=max(
                PRODUCTION_CAPTURE_CAPACITY,
                len(active) * 2,
            ),
            max_capture_members=max(
                1,
                max(len(rule.required_roles) for rule in active),
            ),
            max_catalog_bytes=PRODUCTION_CATALOG_CAPACITY_BYTES,
        ),
    )


def _build_worker_only_config(
    root: str | Path,
    *,
    raw_capacity_bytes: int,
) -> RawCaptureServiceConfig:
    supplied = Path(root)
    if not supplied.is_absolute():
        raise WorkerOnlyBootstrapError("bootstrap root must be absolute")
    resolved = supplied.resolve(strict=False)
    if resolved != supplied or resolved == Path(resolved.anchor):
        raise WorkerOnlyBootstrapError("bootstrap root is not canonical")
    capacity, raw_limits = _capacity_contract(
        raw_capacity_bytes=raw_capacity_bytes,
    )
    worker_first = WorkerFirstCompositionConfig.create(
        paths=_paths(resolved),
        profile=_profile(),
        process_limits=DwdRvCompletionIsolationLimits(),
        owner_id="worker-only-production",
    )
    source = resolved / "source"
    return RawCaptureServiceConfig(
        worker_first=worker_first,
        source_paths=WorkerFirstServiceSourcePaths(
            scheduler_db=source / "scheduler.sqlite3",
            capacity_ledger_db=source / "capacity.sqlite3",
            execution_receipts_db=source / "executions.sqlite3",
            raw_store_root=source / "raw",
        ),
        capacity_profile=capacity,
        raw_store_limits=raw_limits,
        blitzortung_control_root=source / "blitzortung-control",
        blitzortung_raw_store_root=source / "blitzortung-raw",
        blitzortung_raw_store_limits=RawArtifactLimits(),
        blitzortung_ingress_limits=BlitzortungEventIngressLimits(),
        blitzortung_enabled=False,
        host_operational_free_reserve_bytes=2 * 1024 * 1024 * 1024,
    )


def build_worker_only_config(root: str | Path) -> RawCaptureServiceConfig:
    return _build_worker_only_config(
        root,
        raw_capacity_bytes=PRODUCTION_RAW_CAPACITY_BYTES,
    )


def _publish_identical(
    path: Path,
    payload: bytes,
    *,
    accepted_previous: tuple[bytes, ...] = (),
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.is_symlink() or not path.is_file():
            raise WorkerOnlyBootstrapError("existing bootstrap config differs")
        existing = path.read_bytes()
        if existing == payload:
            return
        if existing not in accepted_previous:
            raise WorkerOnlyBootstrapError("existing bootstrap config differs")
        temporary: Path | None = None
        try:
            descriptor, raw_path = tempfile.mkstemp(
                dir=path.parent,
                prefix=".worker-only-config-migration-",
                suffix=".tmp",
            )
            temporary = Path(raw_path)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            if path.is_symlink() or path.read_bytes() != existing:
                raise WorkerOnlyBootstrapError("bootstrap config raced with drift")
            os.replace(temporary, path)
            temporary = None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        return
    temporary: Path | None = None
    try:
        descriptor, raw_path = tempfile.mkstemp(
            dir=path.parent,
            prefix=".worker-only-config-",
            suffix=".tmp",
        )
        temporary = Path(raw_path)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise WorkerOnlyBootstrapError("bootstrap config raced with drift")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def materialize_worker_only_runtime(root: str | Path = DEFAULT_ROOT) -> dict[str, str]:
    config = build_worker_only_config(root)
    authority = initialize_worker_first_composition_authority(
        config.worker_first,
        expected_config_sha256=config.worker_first.fingerprint,
    )
    payload = _canonical(config.to_mapping())
    legacy_payload = _canonical(_build_worker_only_config(
        root,
        raw_capacity_bytes=_LEGACY_PRODUCTION_RAW_CAPACITY_BYTES,
    ).to_mapping())
    config_path = Path(root) / CONFIG_FILENAME
    _publish_identical(
        config_path,
        payload,
        accepted_previous=(legacy_payload,),
    )
    return {
        "authority_sha256": authority.authority_sha256,
        "config_path": str(config_path),
        "config_sha256": hashlib.sha256(payload).hexdigest(),
        "contract": CONTRACT,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args()
    result = materialize_worker_only_runtime(args.root)
    print(
        result["config_path"],
        result["config_sha256"],
        result["authority_sha256"],
        sep="\t",
    )


if __name__ == "__main__":
    main()


__all__ = [
    "CONFIG_FILENAME",
    "CONTRACT",
    "DEFAULT_ROOT",
    "PRODUCTION_CAPTURE_CAPACITY",
    "PRODUCTION_CATALOG_CAPACITY_BYTES",
    "PRODUCTION_ENTRY_CAPACITY",
    "PRODUCTION_RAW_CAPACITY_BYTES",
    "PRODUCTION_RESERVATION_CAPACITY",
    "WorkerOnlyBootstrapError",
    "build_worker_only_config",
    "materialize_worker_only_runtime",
]
