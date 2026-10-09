"""Real-store proofs for the inert worker-first composition root."""
from __future__ import annotations

import ast
import asyncio
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from storm_tracker_worker import worker_first_composition as composition_module
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
)
from storm_tracker_worker.analysis_grid_transform import AnalysisGridTransformConfig
from storm_tracker_worker.dwd_rv_completion_consumer import (
    DwdRvCompletionConsumer,
    DwdRvPipelineOutcomeAuthority,
    DwdRvShadowChainProfile,
)
from storm_tracker_worker.dwd_rv_completion_process_isolation import (
    DwdRvCompletionIsolationLimits,
)
from storm_tracker_worker.desired_state import DesiredStateStore
from storm_tracker_worker.evidence_to_cells_shadow import (
    EvidenceToCellsShadowConfig,
)
from storm_tracker_worker.operational_fact_store import OperationalFactRevisionStore
from storm_tracker_worker.operational_snapshot_publication import PublicationOutboxStore
from storm_tracker_worker.operational_snapshot_store import OperationalSnapshotStore
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
)
from storm_tracker_worker.worker_first_composition import (
    CONFIG_CONTRACT,
    METEOROLOGICAL_ADMISSION,
    MODE,
    WorkerFirstCompositionBusy,
    WorkerFirstCompositionConfig,
    WorkerFirstCompositionConfigError,
    WorkerFirstCompositionError,
    WorkerFirstCompositionIntegrityError,
    WorkerFirstCompositionPaths,
    build_worker_first_composition,
    initialize_worker_first_composition_authority,
)
from storm_tracker_worker.worker_first_ownership import (
    LOCK_ANCHOR_BYTES,
    REQUIRED_STORE_ROLES,
)
from storm_tracker_worker.worker_first_runtime import WorkerFirstRuntimeCoordinator


ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "storm_tracker_worker" / "worker_first_composition.py"
PROJDEF = (
    "+proj=stere +lat_ts=60 +lat_0=90 +lon_0=10 "
    "+x_0=543196.83521776402 +y_0=3622588.8619310022 "
    "+units=m +a=6378137 +b=6356752.3142451802 +no_defs"
)
NOW = 1_800_000_000.0


@pytest.fixture(autouse=True)
def _windows_directory_durability_test_seam(monkeypatch):
    # The production lifecycle deliberately requires directory fsync.  Windows
    # test hosts cannot provide it; all composition behavior is still exercised
    # against the real store and SQLite durability modes.
    monkeypatch.setattr(
        AcquisitionEvidenceLifecycleStore,
        "_default_directory_fsync",
        staticmethod(lambda _path: None),
    )


def _profile(*, shield_threshold: float = 1.0) -> DwdRvShadowChainProfile:
    return DwdRvShadowChainProfile(
        target_grid=AnalysisGrid(
            grid_id="composition-dwd-shadow-2x2",
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
            shield_threshold=shield_threshold,
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


def _paths(root: Path) -> WorkerFirstCompositionPaths:
    root = root.resolve()
    control = root / "control"
    return WorkerFirstCompositionPaths.from_mapping({
        "acquisition_completion_root": str(root / "completion"),
        "authority_file": str(control / "authority.json"),
        "desired_state_db": str(root / "desired.sqlite3"),
        "dwd_canonical_evidence_db": str(root / "canonical.sqlite3"),
        "dwd_derived_artifact_root": str(root / "derived"),
        "dwd_handoff_raw_root": str(root / "handoff"),
        "operational_fact_db": str(root / "facts.sqlite3"),
        "operational_snapshot_db": str(root / "snapshots.sqlite3"),
        "ownership_lock_file": str(control / "ownership.lock"),
        "pipeline_output_db": str(root / "outputs.sqlite3"),
        "snapshot_outbox_db": str(root / "publication.sqlite3"),
    })


def _config(root: Path, *, profile=None) -> WorkerFirstCompositionConfig:
    return WorkerFirstCompositionConfig.create(
        paths=_paths(root),
        profile=profile or _profile(),
        process_limits=DwdRvCompletionIsolationLimits(
            wall_seconds=5.0,
            cpu_seconds=4.0,
            rss_bytes=64 * 1024 * 1024,
            max_request_bytes=256 * 1024,
            max_result_bytes=16 * 1024,
        ),
        owner_id="worker-first-composition-test",
        lease_seconds=30.0,
        processing_timeout_seconds=20.0,
        cancel_grace_seconds=2.0,
        control_timeout_seconds=5.0,
        stop_timeout_seconds=2.0,
    )


def _bootstrap(config: WorkerFirstCompositionConfig):
    return initialize_worker_first_composition_authority(
        config,
        expected_config_sha256=config.fingerprint,
        clock=lambda: NOW,
    )


def _build(config, receipt):
    return build_worker_first_composition(
        config,
        expected_config_sha256=config.fingerprint,
        expected_authority_sha256=receipt.authority_sha256,
        clock=lambda: NOW,
    )


def _run_start_stop(composition):
    async def scenario():
        started = await composition.async_start()
        stopped = await composition.async_stop()
        return started, stopped

    return asyncio.run(scenario())


def test_closed_config_roundtrips_exactly_and_rejects_drift(tmp_path):
    config = _config(tmp_path / "closed-config")
    mapping = json.loads(json.dumps(config.to_mapping()))
    rebuilt = WorkerFirstCompositionConfig.from_mapping(mapping)
    assert rebuilt.to_mapping() == mapping
    assert rebuilt.fingerprint == config.fingerprint
    assert mapping["contract"] == CONFIG_CONTRACT
    assert mapping["mode"] == MODE
    assert mapping["live_wiring"] is False
    assert mapping["meteorological_admission"] == METEOROLOGICAL_ADMISSION

    mapping["unexpected"] = True
    with pytest.raises(WorkerFirstCompositionConfigError) as error:
        WorkerFirstCompositionConfig.from_mapping(mapping)
    assert str(error.value) == "config_schema_invalid"

    alias = _config(tmp_path / "alias").to_mapping()
    alias["paths"]["pipeline_output_db"] = alias["paths"]["desired_state_db"]
    with pytest.raises(WorkerFirstCompositionConfigError) as error:
        WorkerFirstCompositionConfig.from_mapping(alias)
    assert str(error.value) == "config_paths_alias"

    profile_drift = _config(tmp_path / "profile-drift").to_mapping()
    profile_drift["dwd"]["profile"]["unexpected"] = True
    with pytest.raises(WorkerFirstCompositionConfigError) as error:
        WorkerFirstCompositionConfig.from_mapping(profile_drift)
    assert str(error.value) == "dwd_profile_invalid"


def test_bootstrap_is_idempotent_canonical_and_content_addressed(tmp_path):
    config = _config(tmp_path / "bootstrap")
    first = _bootstrap(config)
    authority_bytes = config.paths.authority_file.read_bytes()
    second = _bootstrap(config)

    assert first == second
    assert authority_bytes == config.paths.authority_file.read_bytes()
    assert hashlib.sha256(authority_bytes).hexdigest() == first.authority_sha256
    document = json.loads(authority_bytes)
    assert authority_bytes == json.dumps(
        document,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    assert {item["role"] for item in document["stores"]} == set(
        REQUIRED_STORE_ROLES
    )
    assert len({item["identity_sha256"] for item in document["stores"]}) == 6

    other = _config(
        tmp_path / "different-profile",
        profile=_profile(shield_threshold=1.5),
    )
    other_receipt = _bootstrap(other)
    assert other.fingerprint != config.fingerprint
    assert other_receipt.authority_sha256 != first.authority_sha256
    assert (
        other_receipt.process_authority_sha256
        != first.process_authority_sha256
    )

    drift_same_paths = WorkerFirstCompositionConfig.create(
        paths=config.paths,
        profile=_profile(shield_threshold=1.5),
        process_limits=config.process_limits,
        owner_id=config.owner_id,
        lease_seconds=config.lease_seconds,
        processing_timeout_seconds=config.processing_timeout_seconds,
        cancel_grace_seconds=config.cancel_grace_seconds,
        control_timeout_seconds=config.control_timeout_seconds,
        stop_timeout_seconds=config.stop_timeout_seconds,
    )
    with pytest.raises(WorkerFirstCompositionIntegrityError) as error:
        _bootstrap(drift_same_paths)
    assert str(error.value) == "authority_file_conflict"
    assert config.paths.authority_file.read_bytes() == authority_bytes
    assert config.paths.ownership_lock_file.read_bytes() == LOCK_ANCHOR_BYTES


def test_real_store_runtime_starts_stops_and_restarts_factless(tmp_path):
    config = _config(tmp_path / "restart")
    receipt = _bootstrap(config)

    first = _build(config, receipt)
    assert isinstance(first.coordinator, WorkerFirstRuntimeCoordinator)
    assert isinstance(first._consumer, DwdRvCompletionConsumer)
    assert isinstance(first._outcome_authority, DwdRvPipelineOutcomeAuthority)
    assert first.coordinator._processor.__self__ is first._consumer
    started, stopped = _run_start_stop(first)
    assert started["state"] == "unconfigured"
    assert stopped["state"] == "stopped"
    assert first.diagnostics()["pipeline_fact_binding"] == "absent_dwd_factless"

    facts = OperationalFactRevisionStore(config.paths.operational_fact_db)
    assert facts.audit()["ready"] == 0
    assert facts.audit()["snapshot_committed"] == 0

    second = _build(config, receipt)
    started_again, stopped_again = _run_start_stop(second)
    assert started_again["state"] == "unconfigured"
    assert stopped_again["state"] == "stopped"


def test_active_desired_publishes_one_local_unknown_hash_and_no_fact(tmp_path):
    config = _config(tmp_path / "active-desired")
    receipt = _bootstrap(config)
    desired_store = DesiredStateStore(config.paths.desired_state_db)
    desired_store.claim_producer(
        "ha-composition-test",
        "composition-claim-1",
        claimed_at=NOW - 20.0,
    )
    activated = desired_store.apply({
        "authority_epoch": 1,
        "contract": "stv3-worker-desired-state-v1",
        "generation": 1,
        "issued_at": NOW - 10.0,
        "previous_state_sha256": None,
        "producer_id": "ha-composition-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": False,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": "Home",
            "kind": "home",
            "latitude": 51.04,
            "location_accuracy_m": None,
            "longitude": 4.48,
            "observation_radius_km": 250.0,
            "position_observed_at": NOW - 20.0,
            "target_id": "zone.home",
        }],
    }, received_at=NOW)
    assert activated["status"] == "activated"

    composition = _build(config, receipt)

    async def scenario():
        started = await composition.async_start()
        ticked = await composition.async_tick()
        stopped = await composition.async_stop()
        return started, ticked, stopped

    started, ticked, stopped = asyncio.run(scenario())
    assert started["state"] == "running"
    assert ticked["counts"]["idle_ticks"] == 1
    assert stopped["state"] == "stopped"

    snapshots = OperationalSnapshotStore(
        config.paths.operational_snapshot_db,
        clock=lambda: NOW,
    )
    head = snapshots.head()
    assert head is not None
    assert head["revision"] == 1
    assert head["snapshot"]["desired_state_sha256"] == activated["state_sha256"]
    assert head["snapshot"]["provenance_complete"] is False
    assert head["snapshot"]["targets"]["items"][0][
        "current_precipitation"
    ]["state"] == "unknown"
    outbox = PublicationOutboxStore(
        config.paths.snapshot_outbox_db,
        clock=lambda: NOW,
    ).load()
    assert outbox is not None and outbox.state == "published"

    diagnostic = composition.diagnostics()
    assert diagnostic["publisher"] == {
        "contract": "stv3-worker-first-hash-shadow-sink-v1",
        "last_envelope_sha256": head["envelope_sha256"],
        "publication_count": 1,
        "transport": "local_hash_only",
    }
    assert composition._publisher.__slots__ == (
        "_count", "_last_sha256", "_lock"
    )
    assert not any(
        isinstance(getattr(composition._publisher, slot), (bytes, bytearray))
        for slot in composition._publisher.__slots__
    )
    fact_audit = OperationalFactRevisionStore(
        config.paths.operational_fact_db,
        clock=lambda: NOW,
    ).audit()
    assert fact_audit["ready"] == 0
    assert fact_audit["snapshot_committed"] == 0


def test_double_owner_is_rejected_until_exact_owner_stops(tmp_path):
    config = _config(tmp_path / "double-owner")
    receipt = _bootstrap(config)
    first = _build(config, receipt)

    with pytest.raises(WorkerFirstCompositionBusy) as error:
        _build(config, receipt)
    assert str(error.value) == "composition_owner_busy"

    asyncio.run(first.async_stop())
    successor = _build(config, receipt)
    assert asyncio.run(successor.async_stop())["state"] == "stopped"


def test_authority_and_config_tamper_fail_closed_without_paths(tmp_path):
    private = tmp_path / "private-token-50.99"
    config = _config(private)
    receipt = _bootstrap(config)

    with pytest.raises(WorkerFirstCompositionConfigError) as error:
        build_worker_first_composition(
            config,
            expected_config_sha256="0" * 64,
            expected_authority_sha256=receipt.authority_sha256,
            clock=lambda: NOW,
        )
    assert str(error.value) == "config_checksum_mismatch"

    config.paths.authority_file.write_bytes(b"{}")
    with pytest.raises(WorkerFirstCompositionIntegrityError) as error:
        _build(config, receipt)
    assert str(error.value) == "composition_authority_invalid"
    assert str(private) not in str(error.value)


def test_corrupt_bound_store_is_rejected_before_ownership(tmp_path):
    config = _config(tmp_path / "corrupt-store")
    receipt = _bootstrap(config)
    config.paths.pipeline_output_db.unlink()
    with closing(sqlite3.connect(config.paths.pipeline_output_db)) as database:
        database.execute("CREATE TABLE wrong_schema(value TEXT)")
        database.commit()

    with pytest.raises(WorkerFirstCompositionIntegrityError) as error:
        _build(config, receipt)
    assert str(error.value) == "composition_store_validation_failed"

    # Failure happened before lease acquisition, so the immutable lock remains
    # immediately obtainable; the corrupt store still blocks construction.
    from storm_tracker_worker.worker_first_ownership import (
        WorkerFirstOwnershipFence,
    )
    fence = WorkerFirstOwnershipFence(
        config.paths.authority_file,
        expected_authority_sha256=receipt.authority_sha256,
    )
    with fence.acquire():
        pass


def test_partial_runtime_construction_releases_fence_for_retry(
    tmp_path,
    monkeypatch,
):
    config = _config(tmp_path / "rollback")
    receipt = _bootstrap(config)
    real_coordinator = composition_module.WorkerFirstRuntimeCoordinator

    def fail_constructor(**_kwargs):
        raise RuntimeError("private path and credential must not escape")

    monkeypatch.setattr(
        composition_module,
        "WorkerFirstRuntimeCoordinator",
        fail_constructor,
    )
    with pytest.raises(WorkerFirstCompositionError) as error:
        _build(config, receipt)
    assert str(error.value) == "composition_runtime_construction_failed"

    monkeypatch.setattr(
        composition_module,
        "WorkerFirstRuntimeCoordinator",
        real_coordinator,
    )
    recovered = _build(config, receipt)
    assert asyncio.run(recovered.async_stop())["state"] == "stopped"


def test_diagnostics_are_hash_only_bounded_and_module_has_no_live_imports(tmp_path):
    private = tmp_path / "private-secret-coordinate-50.99"
    config = _config(private)
    receipt = _bootstrap(config)
    composition = _build(config, receipt)
    diagnostic = composition.diagnostics()
    serialized = json.dumps(diagnostic, sort_keys=True)

    assert len(serialized.encode("ascii")) <= 16 * 1024
    assert str(private) not in serialized
    assert config.owner_id not in serialized
    assert "50.99" not in serialized
    assert diagnostic["live_wiring"] is False
    assert diagnostic["publisher"]["transport"] == "local_hash_only"
    assert diagnostic["store_count"] == 6
    assert str(private) not in repr(config)
    assert str(private) not in repr(config.paths)
    asyncio.run(composition.async_stop())

    tree = ast.parse(MODULE.read_text(encoding="utf-8"))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    assert not any(
        name == "service"
        or name.endswith(".service")
        or "mqtt" in name.casefold()
        or name.startswith("custom_components")
        or "homeassistant" in name.casefold()
        for name in imported
    )
