from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest
import yaml

from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
)
from storm_tracker_worker.blitzortung_event_ingress import (
    ACCESS_RETENTION_AUTHORITY_ID,
)
from storm_tracker_worker.worker_only_bootstrap import (
    BLITZORTUNG_AUTHORITY_ENV,
    CONTRACT,
    DEFAULT_ROOT,
    PRODUCTION_ACQUISITION_EPOCH,
    PRODUCTION_CAPTURE_CAPACITY,
    PRODUCTION_CATALOG_CAPACITY_BYTES,
    PRODUCTION_ENTRY_CAPACITY,
    PRODUCTION_RAW_CAPACITY_BYTES,
    PRODUCTION_RETENTION_CLAIM_BYTES,
    PRODUCTION_RESERVATION_CAPACITY,
    WorkerOnlyBootstrapError,
    build_worker_only_config,
    materialize_worker_only_runtime,
)
import storm_tracker_worker.worker_only_bootstrap as bootstrap_module


@pytest.fixture(autouse=True)
def _windows_directory_durability_test_seam(monkeypatch):
    if os.name == "nt":
        monkeypatch.setattr(
            AcquisitionEvidenceLifecycleStore,
            "_default_directory_fsync",
            staticmethod(lambda _path: None),
        )


def test_worker_only_bootstrap_materializes_exact_all_source_pins(tmp_path):
    root = (tmp_path / "worker-only").resolve()
    config = build_worker_only_config(root)

    first = materialize_worker_only_runtime(root)
    second = materialize_worker_only_runtime(root)

    assert first == second
    assert first["contract"] == CONTRACT
    assert len(config.allowed_workload_ids) == 26
    assert config.fingerprint == first["config_sha256"]
    assert first["config_path"] == str(root / "worker-only-config.json")
    assert len(first["authority_sha256"]) == 64


def test_worker_only_bootstrap_reuses_exact_pins_without_historical_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "worker-only").resolve()
    first = materialize_worker_only_runtime(root)

    def reject_repeated_audit(*_args, **_kwargs):
        raise AssertionError("historical bootstrap audit repeated")

    monkeypatch.setattr(
        bootstrap_module,
        "initialize_worker_first_composition_authority",
        reject_repeated_audit,
    )

    assert materialize_worker_only_runtime(root) == first


def test_worker_only_bootstrap_enables_direct_blitzortung_from_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "worker-only").resolve()
    monkeypatch.setenv(
        BLITZORTUNG_AUTHORITY_ENV,
        ACCESS_RETENTION_AUTHORITY_ID,
    )

    result = materialize_worker_only_runtime(root)
    config = build_worker_only_config(root, blitzortung_enabled=True)

    assert config.blitzortung_enabled is True
    assert result["config_sha256"] == config.fingerprint


def test_worker_only_bootstrap_migrates_blitzortung_activation_without_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "worker-only").resolve()
    previous = materialize_worker_only_runtime(
        root,
        blitzortung_enabled=False,
    )

    def reject_repeated_audit(*_args, **_kwargs):
        raise AssertionError("historical bootstrap audit repeated")

    monkeypatch.setattr(
        bootstrap_module,
        "initialize_worker_first_composition_authority",
        reject_repeated_audit,
    )

    migrated = materialize_worker_only_runtime(
        root,
        blitzortung_enabled=True,
    )
    config = build_worker_only_config(root, blitzortung_enabled=True)

    assert migrated["authority_sha256"] == previous["authority_sha256"]
    assert migrated["config_sha256"] == config.fingerprint
    assert (root / "worker-only-config.json").read_bytes() == (
        bootstrap_module._canonical(config.to_mapping())
    )


def test_worker_only_bootstrap_rejects_unknown_blitzortung_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "worker-only").resolve()
    monkeypatch.setenv(BLITZORTUNG_AUTHORITY_ENV, "wrong-authority")

    with pytest.raises(
        WorkerOnlyBootstrapError,
        match="blitzortung retention authority is invalid",
    ):
        materialize_worker_only_runtime(root)


def test_worker_only_bootstrap_migrates_exact_timeout_without_historical_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "worker-only").resolve()
    current = materialize_worker_only_runtime(root)
    config = build_worker_only_config(root)
    config_path = root / "worker-only-config.json"
    previous = bootstrap_module._previous_timeout_payload(config)
    config_path.write_bytes(previous)

    def reject_repeated_audit(*_args, **_kwargs):
        raise AssertionError("historical bootstrap audit repeated")

    monkeypatch.setattr(
        bootstrap_module,
        "initialize_worker_first_composition_authority",
        reject_repeated_audit,
    )

    migrated = materialize_worker_only_runtime(root)
    assert migrated == current
    assert config_path.read_bytes() == bootstrap_module._canonical(
        config.to_mapping()
    )


def test_worker_only_bootstrap_migrates_exact_retention_claim_without_historical_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "worker-only").resolve()
    current = materialize_worker_only_runtime(root)
    config = build_worker_only_config(root)
    config_path = root / "worker-only-config.json"
    previous = bootstrap_module._previous_retention_claim_payload(config)
    previous_document = json.loads(previous.decode("ascii"))
    config_path.write_bytes(previous)

    def reject_repeated_audit(*_args, **_kwargs):
        raise AssertionError("historical bootstrap audit repeated")

    monkeypatch.setattr(
        bootstrap_module,
        "initialize_worker_first_composition_authority",
        reject_repeated_audit,
    )

    migrated = materialize_worker_only_runtime(root)
    assert migrated == current
    assert config_path.read_bytes() == bootstrap_module._canonical(
        config.to_mapping()
    )
    assert previous_document["retention"]["retained_payload_claim_bytes"] == (
        80 * 1024 * 1024 * 1024
    )
    assert config.retained_payload_claim_bytes == 48 * 1024 * 1024 * 1024


def test_worker_only_blitzortung_storage_envelope_fits_live_nas_ha_headroom(
    tmp_path: Path,
) -> None:
    config = build_worker_only_config(
        (tmp_path / "worker-only").resolve(),
        blitzortung_enabled=True,
    )
    storage = config.storage_capacity_diagnostics
    live_free_bytes = 100_940_595_200
    protected_free_required = (
        storage["combined_persistent_volume"]["configured_max_bytes"]
        + config.capacity_profile.max_catalog_bytes
        + storage["host_operational_free_reserve_bytes"]
    )

    assert protected_free_required <= live_free_bytes


def test_worker_only_bootstrap_migrates_legacy_96_gib_claim_without_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "worker-only").resolve()
    current = materialize_worker_only_runtime(root)
    config = build_worker_only_config(root)
    legacy_document = config.to_mapping()
    legacy_document["retention"]["retained_payload_claim_bytes"] = (
        96 * 1024 * 1024 * 1024
    )
    legacy_payload = bootstrap_module._canonical(legacy_document)
    config_path = root / "worker-only-config.json"
    config_path.write_bytes(legacy_payload)

    def reject_repeated_audit(*_args, **_kwargs):
        raise AssertionError("historical bootstrap audit repeated")

    monkeypatch.setattr(
        bootstrap_module,
        "initialize_worker_first_composition_authority",
        reject_repeated_audit,
    )

    migrated = materialize_worker_only_runtime(root)
    assert migrated == current
    assert config_path.read_bytes() == bootstrap_module._canonical(
        config.to_mapping()
    )


def test_worker_only_bootstrap_starts_fresh_epoch_without_rewriting_old_raw(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "worker-only").resolve()
    current = materialize_worker_only_runtime(root)
    config = build_worker_only_config(root)
    config_path = root / "worker-only-config.json"
    old_marker = root / "source" / "raw" / "preserved-provider-object"
    old_marker.parent.mkdir(parents=True)
    old_marker.write_bytes(b"preserved")
    config_path.write_bytes(
        bootstrap_module._previous_acquisition_epoch_payload(config)
    )

    def reject_repeated_audit(*_args, **_kwargs):
        raise AssertionError("historical bootstrap audit repeated")

    monkeypatch.setattr(
        bootstrap_module,
        "initialize_worker_first_composition_authority",
        reject_repeated_audit,
    )

    migrated = materialize_worker_only_runtime(root)
    assert migrated == current
    assert old_marker.read_bytes() == b"preserved"
    assert config.source_paths.raw_store_root == (
        root / PRODUCTION_ACQUISITION_EPOCH / "raw"
    )
    assert config_path.read_bytes() == bootstrap_module._canonical(
        config.to_mapping()
    )


def test_worker_only_bootstrap_rejects_tampered_existing_authority(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "worker-only").resolve()
    materialize_worker_only_runtime(root)
    authority_path = root / "pipeline" / "control" / "authority.json"
    authority_path.write_bytes(authority_path.read_bytes() + b" ")

    with pytest.raises(WorkerOnlyBootstrapError, match="authority is invalid"):
        materialize_worker_only_runtime(root)


def test_worker_only_bootstrap_uses_versioned_production_capacity(
    tmp_path: Path,
) -> None:
    config = build_worker_only_config((tmp_path / "production").resolve())

    assert DEFAULT_ROOT.as_posix() == (
        "/data/storm_tracker_worker_runtime_production_v1"
    )
    assert config.source_paths.raw_store_root.parent.name == (
        PRODUCTION_ACQUISITION_EPOCH
    )
    assert config.capacity_profile.global_max_bytes == (
        PRODUCTION_RAW_CAPACITY_BYTES
    )
    assert config.raw_store_limits.max_total_bytes == (
        PRODUCTION_RAW_CAPACITY_BYTES
    )
    assert config.capacity_profile.global_max_entries == (
        PRODUCTION_ENTRY_CAPACITY
    )
    assert config.raw_store_limits.max_object_entries == (
        PRODUCTION_ENTRY_CAPACITY
    )
    assert config.raw_store_limits.max_receipt_entries == (
        PRODUCTION_ENTRY_CAPACITY
    )
    assert config.capacity_profile.max_reservations == (
        PRODUCTION_RESERVATION_CAPACITY
    )
    assert config.raw_store_limits.max_capture_entries == (
        PRODUCTION_CAPTURE_CAPACITY
    )
    assert config.capacity_profile.max_catalog_bytes == (
        PRODUCTION_CATALOG_CAPACITY_BYTES
    )
    assert config.raw_store_limits.max_catalog_bytes == (
        PRODUCTION_CATALOG_CAPACITY_BYTES
    )


def test_worker_app_backup_excludes_only_the_coherent_rebuildable_runtime():
    repository = Path(__file__).resolve().parents[1]
    document = yaml.safe_load(
        (repository / "storm_tracker_worker" / "config.yaml").read_text(
            encoding="utf-8"
        )
    )
    runtime_name = DEFAULT_ROOT.relative_to("/data").as_posix()

    assert document["backup_exclude"] == [f"{runtime_name}*"]
    assert "options.json" not in document["backup_exclude"]
    assert all("pipeline" not in item for item in document["backup_exclude"])


def test_worker_only_bootstrap_refuses_existing_config_drift(tmp_path):
    root = (tmp_path / "worker-only").resolve()
    result = materialize_worker_only_runtime(root)
    path = root / "worker-only-config.json"
    path.write_bytes(path.read_bytes() + b" ")

    with pytest.raises(WorkerOnlyBootstrapError, match="differs"):
        materialize_worker_only_runtime(root)

    assert result["authority_sha256"]


def test_worker_only_bootstrap_migrates_only_the_exact_legacy_capacity(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "worker-only").resolve()
    legacy = bootstrap_module._build_worker_only_config(
        root,
        raw_capacity_bytes=128 * 1024 * 1024 * 1024,
        retained_payload_claim_bytes=None,
    )
    legacy_payload = bootstrap_module._legacy_v4_payload(legacy)
    path = root / "worker-only-config.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(legacy_payload)

    result = materialize_worker_only_runtime(root)
    migrated = path.read_bytes()

    assert migrated != legacy_payload
    assert hashlib.sha256(migrated).hexdigest() == result["config_sha256"]
    config = build_worker_only_config(root)
    assert config.capacity_profile.global_max_bytes == (
        128 * 1024 * 1024 * 1024
    )
    assert config.retained_payload_claim_bytes == (
        PRODUCTION_RETENTION_CLAIM_BYTES
    )


def test_worker_only_bootstrap_recovers_failed_intermediate_capacity_config(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "worker-only").resolve()
    failed = bootstrap_module._build_worker_only_config(
        root,
        raw_capacity_bytes=96 * 1024 * 1024 * 1024,
        retained_payload_claim_bytes=None,
    )
    path = root / "worker-only-config.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(bootstrap_module._legacy_v4_payload(failed))

    result = materialize_worker_only_runtime(root)

    assert hashlib.sha256(path.read_bytes()).hexdigest() == result["config_sha256"]
    assert build_worker_only_config(root).capacity_profile.global_max_bytes == (
        PRODUCTION_RAW_CAPACITY_BYTES
    )


def test_worker_only_bootstrap_migrates_exact_previous_awn_config(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "worker-only").resolve()
    config = build_worker_only_config(root)
    previous = bootstrap_module._previous_awn_payload(config)
    path = root / "worker-only-config.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(previous)

    result = materialize_worker_only_runtime(root)

    migrated = path.read_bytes()
    assert migrated != previous
    assert hashlib.sha256(migrated).hexdigest() == result["config_sha256"]
    assert b'"ground.awn.api"' not in migrated
    assert b'"provider":"ambientweather"' in migrated
    assert b'"provider":"awn"' not in migrated


def test_worker_only_bootstrap_migrates_catalog_membership_only(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "worker-only").resolve()
    config = build_worker_only_config(root)
    previous = config.to_mapping()
    acquisition = previous["acquisition"]
    acquisition["allowed_workload_ids"] = ["ground.awn.api"]
    acquisition["capacity_profile"]["providers"] = [
        {
            "max_bytes": PRODUCTION_RAW_CAPACITY_BYTES,
            "max_entries": PRODUCTION_ENTRY_CAPACITY,
            "provider": "awn",
        }
    ]
    path = root / "worker-only-config.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(bootstrap_module._canonical(previous))

    result = materialize_worker_only_runtime(root)

    assert hashlib.sha256(path.read_bytes()).hexdigest() == result["config_sha256"]
    assert b'"provider":"ambientweather"' in path.read_bytes()
    assert b'"provider":"awn"' not in path.read_bytes()
