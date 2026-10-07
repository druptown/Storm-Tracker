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
from typing import Callable

from .acquisition_activation import ACTIVATION_RULES
from .analysis_grid_transform import AnalysisGridTransformConfig
from .blitzortung_event_ingress import (
    ACCESS_RETENTION_AUTHORITY_ID,
    BlitzortungEventIngressLimits,
)
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
from .worker_first_ownership import (
    REQUIRED_STORE_ROLES,
    WorkerFirstOwnershipError,
    load_worker_first_authority,
)


CONTRACT = "stv3-worker-only-bootstrap-v8"
BLITZORTUNG_AUTHORITY_ENV = "STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY"
DEFAULT_ROOT = Path("/data/storm_tracker_worker_runtime_production_v1")
CONFIG_FILENAME = "worker-only-config.json"
# Start a fresh acquisition epoch after the original append-only raw store
# became too large to re-verify inside a bounded service start.  The previous
# ``source`` directory is deliberately left untouched: no historical provider
# bytes are deleted or rewritten, while new collection gets a small, coherent
# catalog that can open immediately.
PRODUCTION_ACQUISITION_EPOCH = "source-v2"
# Existing raw and capacity-ledger catalogs bind this physical ceiling into
# their immutable schema. Keep it unchanged so every retained byte remains
# readable. Admission uses the lower claim below, leaving ample NAS headroom.
PRODUCTION_RAW_CAPACITY_BYTES = 128 * 1024 * 1024 * 1024
# Reserve a large but realistic raw payload envelope on the dedicated 160 GB
# NAS-HA volume.  The complete guard also reserves both 4 GiB cadence catalogs,
# the Blitzortung object/catalog/control envelope and 2 GiB for HA itself.  An
# 80 GiB cadence claim therefore required about 111 GiB of free filesystem
# space and blocked a healthy VM that still had about 94 GiB free.  This changes
# only the admission claim; the immutable 128 GiB store/ledger schema ceilings
# remain unchanged and every existing object stays readable.
PRODUCTION_RETENTION_CLAIM_BYTES = 48 * 1024 * 1024 * 1024
_PREVIOUS_PRODUCTION_RETENTION_CLAIM_BYTES = 80 * 1024 * 1024 * 1024
_LEGACY_PRODUCTION_RETENTION_CLAIM_BYTES = 96 * 1024 * 1024 * 1024
_FAILED_INTERMEDIATE_RAW_CAPACITY_BYTES = 96 * 1024 * 1024 * 1024
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
    # Keep the removed AWN provider as storage-only ledger history.  Its
    # immutable provider ID was ``ambientweather``; ``awn`` was only the
    # workload/source label.  No AWN workload remains executable, so this can
    # never schedule new AWN data.
    providers = tuple(
        sorted({rule.provider_id for rule in active} | {"ambientweather"})
    )
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
    retained_payload_claim_bytes: int | None,
    blitzortung_enabled: bool = False,
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
    source = resolved / PRODUCTION_ACQUISITION_EPOCH
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
        blitzortung_enabled=blitzortung_enabled,
        host_operational_free_reserve_bytes=2 * 1024 * 1024 * 1024,
        retained_payload_claim_bytes=retained_payload_claim_bytes,
    )


def build_worker_only_config(
    root: str | Path,
    *,
    blitzortung_enabled: bool = False,
) -> RawCaptureServiceConfig:
    return _build_worker_only_config(
        root,
        raw_capacity_bytes=PRODUCTION_RAW_CAPACITY_BYTES,
        retained_payload_claim_bytes=PRODUCTION_RETENTION_CLAIM_BYTES,
        blitzortung_enabled=blitzortung_enabled,
    )


def _configured_blitzortung_enabled() -> bool:
    authority = os.environ.get(BLITZORTUNG_AUTHORITY_ENV, "")
    if not authority:
        return False
    if authority != ACCESS_RETENTION_AUTHORITY_ID:
        raise WorkerOnlyBootstrapError(
            "blitzortung retention authority is invalid"
        )
    return True


def _legacy_v4_payload(config: RawCaptureServiceConfig) -> bytes:
    mapping = config.to_mapping()
    mapping["contract"] = "stv3-worker-raw-capture-service-config-v4"
    retention = mapping["retention"]
    if not isinstance(retention, dict):
        raise WorkerOnlyBootstrapError("legacy bootstrap mapping invalid")
    retention.pop("retained_payload_claim_bytes", None)
    return _canonical(mapping)


def _previous_awn_payload(config: RawCaptureServiceConfig) -> bytes:
    """Reconstruct the exact pre-removal AWN bootstrap for one migration."""
    mapping = config.to_mapping()
    acquisition = mapping.get("acquisition")
    if not isinstance(acquisition, dict):
        raise WorkerOnlyBootstrapError("previous AWN bootstrap mapping invalid")
    workload_ids = acquisition.get("allowed_workload_ids")
    capacity = acquisition.get("capacity_profile")
    if not isinstance(workload_ids, list) or not isinstance(capacity, dict):
        raise WorkerOnlyBootstrapError("previous AWN bootstrap mapping invalid")
    providers = capacity.get("providers")
    global_max_bytes = capacity.get("global_max_bytes")
    global_max_entries = capacity.get("global_max_entries")
    if not isinstance(providers, list):
        raise WorkerOnlyBootstrapError("previous AWN bootstrap mapping invalid")
    awn_providers = [
        item
        for item in providers
        if isinstance(item, dict) and item.get("provider") == "ambientweather"
    ]
    if (
        not isinstance(global_max_bytes, int)
        or not isinstance(global_max_entries, int)
        or "ground.awn.api" in workload_ids
        or len(awn_providers) != 1
    ):
        raise WorkerOnlyBootstrapError("previous AWN bootstrap mapping invalid")
    acquisition["allowed_workload_ids"] = sorted(
        [*workload_ids, "ground.awn.api"]
    )
    return _canonical(mapping)


def _previous_timeout_payload(config: RawCaptureServiceConfig) -> bytes:
    """Reconstruct the exact v5 production config for one safe migration."""

    mapping = config.to_mapping()
    mapping["contract"] = "stv3-worker-raw-capture-service-config-v5"
    timeouts = mapping.get("timeouts")
    if not isinstance(timeouts, dict):
        raise WorkerOnlyBootstrapError("previous timeout mapping invalid")
    timeouts["acquisition_seconds"] = 315.0
    return _canonical(mapping)


def _previous_retention_claim_payload(config: RawCaptureServiceConfig) -> bytes:
    """Reconstruct the exact former 80 GiB admission claim for migration."""

    mapping = config.to_mapping()
    retention = mapping.get("retention")
    if not isinstance(retention, dict):
        raise WorkerOnlyBootstrapError("previous retention mapping invalid")
    retention["retained_payload_claim_bytes"] = (
        _PREVIOUS_PRODUCTION_RETENTION_CLAIM_BYTES
    )
    return _canonical(mapping)


def _legacy_retention_claim_payload(config: RawCaptureServiceConfig) -> bytes:
    """Reconstruct the older 96 GiB admission claim for direct migration."""

    mapping = config.to_mapping()
    retention = mapping.get("retention")
    if not isinstance(retention, dict):
        raise WorkerOnlyBootstrapError("legacy retention mapping invalid")
    retention["retained_payload_claim_bytes"] = (
        _LEGACY_PRODUCTION_RETENTION_CLAIM_BYTES
    )
    return _canonical(mapping)


def _previous_acquisition_epoch_payload(
    config: RawCaptureServiceConfig,
    *,
    previous_timeout: bool = False,
) -> bytes:
    """Reconstruct the exact former ``source`` path without touching it."""

    mapping = config.to_mapping()
    acquisition = mapping.get("acquisition")
    event_ingress = mapping.get("event_ingress")
    if not isinstance(acquisition, dict) or not isinstance(event_ingress, dict):
        raise WorkerOnlyBootstrapError("previous acquisition epoch mapping invalid")
    paths = acquisition.get("paths")
    blitzortung = event_ingress.get("blitzortung")
    if not isinstance(paths, dict) or not isinstance(blitzortung, dict):
        raise WorkerOnlyBootstrapError("previous acquisition epoch mapping invalid")
    current_source = Path(config.source_paths.raw_store_root).parent
    previous_source = current_source.parent / "source"
    paths.update({
        "capacity_ledger_db": str(previous_source / "capacity.sqlite3"),
        "execution_receipts_db": str(previous_source / "executions.sqlite3"),
        "raw_store_root": str(previous_source / "raw"),
        "scheduler_db": str(previous_source / "scheduler.sqlite3"),
    })
    blitzortung.update({
        "control_root": str(previous_source / "blitzortung-control"),
        "raw_store_root": str(previous_source / "blitzortung-raw"),
    })
    if previous_timeout:
        mapping["contract"] = "stv3-worker-raw-capture-service-config-v5"
        timeouts = mapping.get("timeouts")
        if not isinstance(timeouts, dict):
            raise WorkerOnlyBootstrapError(
                "previous acquisition timeout mapping invalid"
            )
        timeouts["acquisition_seconds"] = 315.0
    return _canonical(mapping)


def _catalog_membership_only_predecessor(
    existing: bytes,
    current: RawCaptureServiceConfig,
) -> bool:
    try:
        previous = json.loads(existing.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(previous, dict) or _canonical(previous) != existing:
        return False
    current_mapping = current.to_mapping()
    previous_acquisition = previous.get("acquisition")
    current_acquisition = current_mapping.get("acquisition")
    if not isinstance(previous_acquisition, dict) or not isinstance(
        current_acquisition, dict
    ):
        return False
    previous_capacity = previous_acquisition.get("capacity_profile")
    current_capacity = current_acquisition.get("capacity_profile")
    if not isinstance(previous_capacity, dict) or not isinstance(
        current_capacity, dict
    ):
        return False
    if not isinstance(previous_acquisition.get("allowed_workload_ids"), list):
        return False
    if not isinstance(previous_capacity.get("providers"), list):
        return False
    previous_acquisition["allowed_workload_ids"] = current_acquisition[
        "allowed_workload_ids"
    ]
    previous_capacity["providers"] = current_capacity["providers"]
    return _canonical(previous) == _canonical(current_mapping)


def _publish_identical(
    path: Path,
    payload: bytes,
    *,
    accepted_previous: tuple[bytes, ...] = (),
    accepted_previous_check: Callable[[bytes], bool] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.is_symlink() or not path.is_file():
            raise WorkerOnlyBootstrapError("existing bootstrap config differs")
        existing = path.read_bytes()
        if existing == payload:
            return
        if existing not in accepted_previous and not (
            accepted_previous_check is not None
            and accepted_previous_check(existing)
        ):
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


def _load_existing_runtime_authority(
    config: RawCaptureServiceConfig,
    payload: bytes,
) -> str | None:
    """Validate and reuse exact immutable pins without re-auditing history.

    A complete historical audit belongs to first materialization or an exact
    configuration migration. Repeating it before every process start makes
    restart time grow with retained calibration history and can prevent the
    worker from ever reaching its heartbeat. Runtime construction still
    validates the immutable authority, paths and ownership fence.
    """

    config_path = config.worker_first.paths.authority_file.parent.parent.parent
    config_path /= CONFIG_FILENAME
    authority_path = config.worker_first.paths.authority_file
    if not config_path.exists() or not authority_path.exists():
        return None
    try:
        existing_config = config_path.read_bytes()
        previous_retention_claim_payload = _previous_retention_claim_payload(config)
        legacy_retention_claim_payload = _legacy_retention_claim_payload(config)
        previous_timeout_payload = _previous_timeout_payload(config)
        previous_epoch_payload = _previous_acquisition_epoch_payload(config)
        previous_epoch_timeout_payload = _previous_acquisition_epoch_payload(
            config,
            previous_timeout=True,
        )
        previous_blitzortung_disabled_payload = _canonical(
            _build_worker_only_config(
                config.worker_first.paths.authority_file.parent.parent.parent,
                raw_capacity_bytes=PRODUCTION_RAW_CAPACITY_BYTES,
                retained_payload_claim_bytes=PRODUCTION_RETENTION_CLAIM_BYTES,
                blitzortung_enabled=False,
            ).to_mapping()
        )
        if (
            config_path.is_symlink()
            or authority_path.is_symlink()
            or not config_path.is_file()
            or not authority_path.is_file()
            or existing_config not in (
                payload,
                previous_retention_claim_payload,
                legacy_retention_claim_payload,
                previous_timeout_payload,
                previous_epoch_payload,
                previous_epoch_timeout_payload,
                previous_blitzortung_disabled_payload,
            )
        ):
            return None
        authority_body = authority_path.read_bytes()
        authority_sha256 = hashlib.sha256(authority_body).hexdigest()
        binding = load_worker_first_authority(
            authority_path,
            expected_authority_sha256=authority_sha256,
        )
        configured_paths = {
            "acquisition_completion_store": (
                config.worker_first.paths.acquisition_completion_root
            ),
            "desired_state_store": config.worker_first.paths.desired_state_db,
            "operational_fact_store": (
                config.worker_first.paths.operational_fact_db
            ),
            "operational_snapshot_store": (
                config.worker_first.paths.operational_snapshot_db
            ),
            "pipeline_output_store": config.worker_first.paths.pipeline_output_db,
            "snapshot_outbox_store": (
                config.worker_first.paths.snapshot_outbox_db
            ),
        }
        if (
            set(configured_paths) != set(REQUIRED_STORE_ROLES)
            or binding.lock_path
            != config.worker_first.paths.ownership_lock_file.resolve(strict=True)
            or any(
                binding.store(role).path
                != path.resolve(strict=True)
                for role, path in configured_paths.items()
            )
        ):
            raise WorkerOnlyBootstrapError(
                "existing bootstrap authority path differs"
            )
        if existing_config != payload:
            _publish_identical(
                config_path,
                payload,
                accepted_previous=(
                    previous_retention_claim_payload,
                    legacy_retention_claim_payload,
                    previous_timeout_payload,
                    previous_epoch_payload,
                    previous_epoch_timeout_payload,
                    previous_blitzortung_disabled_payload,
                ),
            )
    except WorkerOnlyBootstrapError:
        raise
    except (OSError, RuntimeError, WorkerFirstOwnershipError):
        raise WorkerOnlyBootstrapError(
            "existing bootstrap authority is invalid"
        ) from None
    return authority_sha256


def materialize_worker_only_runtime(
    root: str | Path = DEFAULT_ROOT,
    *,
    blitzortung_enabled: bool | None = None,
) -> dict[str, str]:
    if blitzortung_enabled is None:
        blitzortung_enabled = _configured_blitzortung_enabled()
    if type(blitzortung_enabled) is not bool:
        raise WorkerOnlyBootstrapError("blitzortung enabled flag is invalid")
    config = build_worker_only_config(
        root,
        blitzortung_enabled=blitzortung_enabled,
    )
    payload = _canonical(config.to_mapping())
    existing_authority_sha256 = _load_existing_runtime_authority(config, payload)
    if existing_authority_sha256 is not None:
        return {
            "authority_sha256": existing_authority_sha256,
            "config_path": str(Path(root) / CONFIG_FILENAME),
            "config_sha256": hashlib.sha256(payload).hexdigest(),
            "contract": CONTRACT,
        }
    authority = initialize_worker_first_composition_authority(
        config.worker_first,
        expected_config_sha256=config.worker_first.fingerprint,
    )
    original_legacy_payload = _legacy_v4_payload(_build_worker_only_config(
        root,
        raw_capacity_bytes=PRODUCTION_RAW_CAPACITY_BYTES,
        retained_payload_claim_bytes=None,
        blitzortung_enabled=blitzortung_enabled,
    ))
    failed_intermediate_payload = _legacy_v4_payload(_build_worker_only_config(
        root,
        raw_capacity_bytes=_FAILED_INTERMEDIATE_RAW_CAPACITY_BYTES,
        retained_payload_claim_bytes=None,
        blitzortung_enabled=blitzortung_enabled,
    ))
    previous_awn_payload = _previous_awn_payload(config)
    previous_retention_claim_payload = _previous_retention_claim_payload(config)
    legacy_retention_claim_payload = _legacy_retention_claim_payload(config)
    previous_timeout_payload = _previous_timeout_payload(config)
    previous_epoch_payload = _previous_acquisition_epoch_payload(config)
    previous_epoch_timeout_payload = _previous_acquisition_epoch_payload(
        config,
        previous_timeout=True,
    )
    config_path = Path(root) / CONFIG_FILENAME
    _publish_identical(
        config_path,
        payload,
        accepted_previous=(
            original_legacy_payload,
            failed_intermediate_payload,
            previous_awn_payload,
            previous_retention_claim_payload,
            legacy_retention_claim_payload,
            previous_timeout_payload,
            previous_epoch_payload,
            previous_epoch_timeout_payload,
        ),
        accepted_previous_check=lambda existing: (
            _catalog_membership_only_predecessor(existing, config)
        ),
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
    "BLITZORTUNG_AUTHORITY_ENV",
    "CONFIG_FILENAME",
    "CONTRACT",
    "DEFAULT_ROOT",
    "PRODUCTION_CAPTURE_CAPACITY",
    "PRODUCTION_ACQUISITION_EPOCH",
    "PRODUCTION_CATALOG_CAPACITY_BYTES",
    "PRODUCTION_ENTRY_CAPACITY",
    "PRODUCTION_RAW_CAPACITY_BYTES",
    "PRODUCTION_RETENTION_CLAIM_BYTES",
    "PRODUCTION_RESERVATION_CAPACITY",
    "WorkerOnlyBootstrapError",
    "build_worker_only_config",
    "materialize_worker_only_runtime",
]
