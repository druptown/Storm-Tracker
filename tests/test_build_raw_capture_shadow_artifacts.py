from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import build_raw_capture_shadow_artifacts as cli
from storm_tracker_worker import acquisition_evidence_lifecycle
from storm_tracker_worker import raw_capture_activation_artifacts as subject
from storm_tracker_worker.raw_capture_service_composition import (
    CONFIG_CONTRACT as RAW_CAPTURE_CONFIG_CONTRACT,
    RUNTIME_CONTRACT as RAW_CAPTURE_RUNTIME_CONTRACT,
)


CATALOG_SHA256 = (
    "9cd9af5bf6324cea930e6eb044a7af5ed2f7783bedbceacb9056faf0a88199fb"
)
ACTIVATION_MATRIX_SHA256 = (
    "341c0cc8a28b76679397f8ee4fed7203632d872bd11418c2168c4d928224a31b"
)
MAX_SIGNED_INT64 = (1 << 63) - 1
HOST_RESERVE_BYTES = 2 * 1024**3


@pytest.fixture(autouse=True)
def _portable_directory_fsync(monkeypatch) -> None:
    monkeypatch.setattr(
        acquisition_evidence_lifecycle.AcquisitionEvidenceLifecycleStore,
        "_default_directory_fsync",
        staticmethod(lambda _path: None),
    )


def _derive(
    hours: int = 1,
    *,
    target: int = MAX_SIGNED_INT64,
    blitzortung_enabled: bool = True,
    host_reserve_bytes: int = HOST_RESERVE_BYTES,
) -> subject.RawCaptureCapacityPlan:
    return subject.derive_raw_capture_shadow_capacity(
        capture_window_hours=hours,
        target_raw_capacity_bytes=target,
        blitzortung_enabled=blitzortung_enabled,
        operator_host_operational_free_reserve_bytes=host_reserve_bytes,
        expected_catalog_sha256=CATALOG_SHA256,
        expected_activation_matrix_sha256=ACTIVATION_MATRIX_SHA256,
    )


def _build(
    tmp_path: Path,
    name: str = "activation",
    *,
    blitzortung_enabled: bool = True,
    host_reserve_bytes: int = HOST_RESERVE_BYTES,
):
    base = tmp_path.resolve()
    return subject.build_raw_capture_shadow_artifacts(
        persistent_base_root=base,
        output_directory=(base / name).resolve(),
        capture_window_hours=1,
        target_raw_capacity_bytes=MAX_SIGNED_INT64,
        blitzortung_enabled=blitzortung_enabled,
        operator_host_operational_free_reserve_bytes=host_reserve_bytes,
        expected_catalog_sha256=CATALOG_SHA256,
        expected_activation_matrix_sha256=ACTIVATION_MATRIX_SHA256,
    )


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="ascii"))


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def test_capacity_floor_and_one_hour_envelope_are_exact() -> None:
    plan = _derive()
    document = plan.to_mapping()

    assert document["activation"]["active_workload_count"] == 26
    assert document["startup_floor"] == {
        "basis": "one_closed_batch_plus_componentwise_largest_capture_inflight",
        "batch_bytes": 1_202_716_672,
        "batch_entries": 84,
        "batch_reservations": 26,
        "bytes": 1_605_369_856,
        "entries": 103,
        "reservations": 27,
        "retention_capacity_claim": False,
    }
    assert plan.capacity_profile.global_max_bytes == 16_559_112_192
    assert plan.capacity_profile.global_max_entries == 1_404
    assert plan.capacity_profile.max_reservations == 318
    assert plan.raw_store_limits.max_object_bytes == 402_653_184
    assert plan.raw_store_limits.max_capture_members == 19
    assert plan.raw_store_limits.max_staging_entries == 19
    assert document["inflight_margin"]["bytes"] == 402_653_184
    assert document["inflight_margin"]["entries"] == 19
    assert document["automatic_reclaim"] is False
    assert document["fail_closed_on_full"] is True
    assert document["derivation"]["horizon_semantics"] == (
        "sizing_horizon_not_automatic_retention_or_reclamation"
    )


@pytest.mark.parametrize(
    ("hours", "expected_bytes", "expected_entries", "expected_reservations"),
    (
        (24, 388_061_200_384, 33_236, 7_586),
        (168, 2_714_012_483_584, 232_538, 53_096),
    ),
)
def test_capacity_window_scaling_is_exact(
    hours: int,
    expected_bytes: int,
    expected_entries: int,
    expected_reservations: int,
) -> None:
    plan = _derive(hours)

    assert plan.capacity_profile.global_max_bytes == expected_bytes
    assert plan.capacity_profile.global_max_entries == expected_entries
    assert plan.capacity_profile.max_reservations == expected_reservations
    assert plan.to_mapping()["startup_floor"]["bytes"] == 1_605_369_856


@pytest.mark.parametrize(
    (
        "blitzortung_enabled",
        "event_raw_quota",
        "metadata_reserve",
        "required_bytes",
        "service_combined_bytes",
    ),
    (
            (False, 0, 1_476_395_008, 20_594_032_640, 17_632_854_016),
        (
            True,
            4_294_967_296,
            2_550_136_832,
                39_071_252_480,
                36_110_073_856,
        ),
    ),
)
def test_event_profiles_and_same_filesystem_capacity_are_exact(
    blitzortung_enabled: bool,
    event_raw_quota: int,
    metadata_reserve: int,
    required_bytes: int,
    service_combined_bytes: int,
) -> None:
    plan = _derive(blitzortung_enabled=blitzortung_enabled)
    document = plan.to_mapping()
    event = document["event_ingress"]
    same_filesystem = document["same_filesystem_capacity"]
    diagnostics = document["service_storage_capacity_diagnostics"]
    retention = document["retention_guard"]

    assert plan.blitzortung_enabled is blitzortung_enabled
    assert plan.host_operational_free_reserve_bytes == HOST_RESERVE_BYTES
    assert event["enabled"] is blitzortung_enabled
    assert event["capacity_claimed"] is blitzortung_enabled
    assert event["limits_status"] == (
        "configured_enabled_fail_closed"
        if blitzortung_enabled
        else "configured_dormant_unclaimed"
    )
    assert event["catalog_ownership_status"] == "ownership_blocked"
    assert event["ledger_accounted"] is False
    assert event["included_in_cadence_quota"] is False
    assert event["included_in_same_filesystem_requirement"] is (
        blitzortung_enabled
    )
    assert event["operational_admission"] is False
    assert event["raw_store_commit_is_terminal"] is True
    assert event["ingress_limits"] == (
        plan.blitzortung_ingress_limits.to_metadata()
    )
    assert event["raw_store_limits"] == asdict(
        plan.blitzortung_raw_store_limits
    )
    assert plan.blitzortung_raw_store_limits.max_object_bytes == 256 * 1024
    assert plan.blitzortung_raw_store_limits.max_total_bytes == 4 * 1024**3
    assert plan.blitzortung_raw_store_limits.max_object_entries == 50_000
    assert plan.blitzortung_raw_store_limits.max_staging_entries == 128
    assert plan.blitzortung_raw_store_limits.max_capture_members == 1

    assert same_filesystem["configured_cadence_raw_quota"] == 16_559_112_192
    assert same_filesystem["configured_event_raw_quota"] == event_raw_quota
    assert same_filesystem["configured_raw_quota"] == (
        16_559_112_192 + event_raw_quota
    )
    assert same_filesystem["transport_spool_reserve"] == 392 * 1024**2
    assert same_filesystem["metadata_reserve"] == metadata_reserve
    assert same_filesystem["event_control_receipt_storage_reserve"] == (
        100_000 * 128 * 1024 if blitzortung_enabled else 0
    )
    assert same_filesystem["event_control_staging_residue_reserve"] == (
        (8 + 2) * 128 * 1024 if blitzortung_enabled else 0
    )
    assert same_filesystem["operator_host_operational_free_reserve_bytes"] == (
        HOST_RESERVE_BYTES
    )
    assert same_filesystem["required_same_filesystem_bytes"] == required_bytes
    required_without_host_reserve = sum((
        same_filesystem["configured_raw_quota"],
        same_filesystem["transport_spool_reserve"],
        same_filesystem["metadata_reserve"],
        same_filesystem["event_control_receipt_storage_reserve"],
        same_filesystem["event_control_staging_residue_reserve"],
    ))
    assert required_bytes - required_without_host_reserve == HOST_RESERVE_BYTES
    assert same_filesystem["requirement_formula"].count(
        "operator_host_operational_free_reserve_bytes"
    ) == 1
    assert same_filesystem["shared_ledger_claim"] is False
    assert same_filesystem["live_disk_free_space_verified"] is False
    assert diagnostics["accounting_contract"] == (
        "config_bound_optional_event_capacity_v2"
    )
    assert diagnostics["host_operational_free_reserve_bytes"] == (
        HOST_RESERVE_BYTES
    )
    assert diagnostics["combined_persistent_volume"][
        "configured_max_bytes"
    ] == service_combined_bytes
    assert diagnostics["event"] == {
        "capacity_claimed": blitzortung_enabled,
        "catalog_max_bytes": 1024**3 if blitzortung_enabled else 0,
        "control_receipt_max_bytes": (
            128 * 1024 if blitzortung_enabled else 0
        ),
        "control_receipt_storage_max_bytes": (
            100_000 * 128 * 1024 if blitzortung_enabled else 0
        ),
        "control_staging_residue_entry_capacity": (
            10 if blitzortung_enabled else 0
        ),
        "control_staging_residue_max_bytes": (
            10 * 128 * 1024 if blitzortung_enabled else 0
        ),
        "enabled": blitzortung_enabled,
        "ledger_accounted": False,
        "object_max_bytes": event_raw_quota,
        "object_max_entries": 50_000 if blitzortung_enabled else 0,
    }
    assert retention == {
        "blitzortung_enabled": blitzortung_enabled,
        "capacity_policy": (
            "config_bound_optional_event_envelope_reserved_fail_closed_v2"
        ),
        "contract": "stv3-worker-raw-capture-retention-guard-v2",
        "event_capacity_claimed": blitzortung_enabled,
        "guard_configured_persistent_max_bytes": (
            service_combined_bytes + 256 * 1024**2
        ),
        "host_operational_free_reserve_bytes": HOST_RESERVE_BYTES,
        "selected_profile": (
            "event_enabled" if blitzortung_enabled else "cadence_only"
        ),
        "service_configured_persistent_max_bytes": service_combined_bytes,
        "source_sha256": subject.EXPECTED_RAW_CAPTURE_RETENTION_GUARD_SHA256,
    }


def test_capacity_derivation_is_target_path_independent(
    monkeypatch,
    tmp_path,
) -> None:
    first = _derive().to_mapping()
    nested = tmp_path / "unrelated-working-directory"
    nested.mkdir()
    monkeypatch.chdir(nested)

    assert _derive().to_mapping() == first
    encoded = _canonical(first)
    assert str(tmp_path).encode() not in encoded


@pytest.mark.parametrize("hours", (True, False, 0, -1, 169, 1.0, "1"))
def test_capture_window_is_required_bounded_integer(hours: object) -> None:
    with pytest.raises(
        subject.RawCaptureArtifactBuildError,
        match="capture_window_hours_invalid",
    ):
        subject.derive_raw_capture_shadow_capacity(
            capture_window_hours=hours,  # type: ignore[arg-type]
            target_raw_capacity_bytes=MAX_SIGNED_INT64,
            blitzortung_enabled=True,
            operator_host_operational_free_reserve_bytes=(
                HOST_RESERVE_BYTES
            ),
            expected_catalog_sha256=CATALOG_SHA256,
            expected_activation_matrix_sha256=ACTIVATION_MATRIX_SHA256,
        )


@pytest.mark.parametrize("target", (True, False, 0, -1, 1.0, 1 << 63))
def test_target_capacity_is_positive_signed_int64(target: object) -> None:
    expected = (
        "capacity_integer_overflow"
        if target == 1 << 63
        else "target_raw_capacity_bytes_invalid"
    )
    with pytest.raises(subject.RawCaptureArtifactBuildError, match=expected):
        subject.derive_raw_capture_shadow_capacity(
            capture_window_hours=1,
            target_raw_capacity_bytes=target,  # type: ignore[arg-type]
            blitzortung_enabled=True,
            operator_host_operational_free_reserve_bytes=(
                HOST_RESERVE_BYTES
            ),
            expected_catalog_sha256=CATALOG_SHA256,
            expected_activation_matrix_sha256=ACTIVATION_MATRIX_SHA256,
        )


@pytest.mark.parametrize(
    "blitzortung_enabled",
    (None, 0, 1, "disabled", "enabled"),
)
def test_blitzortung_profile_is_an_exact_boolean(
    blitzortung_enabled: object,
) -> None:
    with pytest.raises(
        subject.RawCaptureArtifactBuildError,
        match="blitzortung_profile_invalid",
    ):
        subject.derive_raw_capture_shadow_capacity(
            capture_window_hours=1,
            target_raw_capacity_bytes=MAX_SIGNED_INT64,
            blitzortung_enabled=blitzortung_enabled,  # type: ignore[arg-type]
            operator_host_operational_free_reserve_bytes=(
                HOST_RESERVE_BYTES
            ),
            expected_catalog_sha256=CATALOG_SHA256,
            expected_activation_matrix_sha256=ACTIVATION_MATRIX_SHA256,
        )


@pytest.mark.parametrize(
    ("reserve", "error"),
    (
        (True, "operator_host_operational_free_reserve_bytes_invalid"),
        (False, "operator_host_operational_free_reserve_bytes_invalid"),
        (0, "operator_host_operational_free_reserve_bytes_invalid"),
        (-1, "operator_host_operational_free_reserve_bytes_invalid"),
        (1.0, "operator_host_operational_free_reserve_bytes_invalid"),
        (
            HOST_RESERVE_BYTES - 1,
            "operator_host_operational_free_reserve_bytes_insufficient",
        ),
        (1 << 63, "capacity_integer_overflow"),
    ),
)
def test_host_operational_reserve_is_at_least_two_gib_and_signed_i64(
    reserve: object,
    error: str,
) -> None:
    with pytest.raises(subject.RawCaptureArtifactBuildError, match=error):
        subject.derive_raw_capture_shadow_capacity(
            capture_window_hours=1,
            target_raw_capacity_bytes=MAX_SIGNED_INT64,
            blitzortung_enabled=False,
            operator_host_operational_free_reserve_bytes=reserve,  # type: ignore[arg-type]
            expected_catalog_sha256=CATALOG_SHA256,
            expected_activation_matrix_sha256=ACTIVATION_MATRIX_SHA256,
        )


def test_insufficient_target_fails_closed_before_materialization(
    tmp_path,
) -> None:
    required = _derive().required_same_filesystem_bytes
    output = (tmp_path / "activation").resolve()

    with pytest.raises(
        subject.RawCaptureArtifactBuildError,
        match="target_raw_capacity_insufficient",
    ):
        subject.build_raw_capture_shadow_artifacts(
            persistent_base_root=tmp_path.resolve(),
            output_directory=output,
            capture_window_hours=1,
            target_raw_capacity_bytes=required - 1,
            blitzortung_enabled=True,
            operator_host_operational_free_reserve_bytes=(
                HOST_RESERVE_BYTES
            ),
            expected_catalog_sha256=CATALOG_SHA256,
            expected_activation_matrix_sha256=ACTIVATION_MATRIX_SHA256,
        )
    assert not os.path.lexists(output)


def test_pin_and_frozen_contract_drift_fail_closed(monkeypatch) -> None:
    with pytest.raises(
        subject.RawCaptureArtifactBuildError,
        match="closed_activation_pin_mismatch",
    ):
        subject.derive_raw_capture_shadow_capacity(
            capture_window_hours=1,
            target_raw_capacity_bytes=MAX_SIGNED_INT64,
            blitzortung_enabled=True,
            operator_host_operational_free_reserve_bytes=(
                HOST_RESERVE_BYTES
            ),
            expected_catalog_sha256="0" * 64,
            expected_activation_matrix_sha256=ACTIVATION_MATRIX_SHA256,
        )

    cases = (
        (
            "EXPECTED_OPERA_MAX_BYTES",
            subject.EXPECTED_OPERA_MAX_BYTES + 1,
            "opera_capacity_contract_drift",
        ),
        (
            "EXPECTED_RECOVERY_INVENTORY_SHA256",
            "0" * 64,
            "recovery_inventory_drift",
        ),
        (
            "EXPECTED_BLITZORTUNG_HARD_MAX_QUEUE_ENTRIES",
            subject.EXPECTED_BLITZORTUNG_HARD_MAX_QUEUE_ENTRIES + 1,
            "blitzortung_capacity_contract_drift",
        ),
        (
            "RAW_CAPTURE_ALLOWED_WORKLOAD_IDS",
            subject.RAW_CAPTURE_ALLOWED_WORKLOAD_IDS[:-1],
            "active_workload_membership_drift",
        ),
        (
            "EXPECTED_RAW_CAPTURE_COMPOSITION_SHA256",
            "0" * 64,
            "raw_capture_source_pin_drift",
        ),
        (
            "EXPECTED_RAW_CAPTURE_RETENTION_GUARD_SHA256",
            "0" * 64,
            "raw_capture_source_pin_drift",
        ),
        (
            "EXPECTED_RAW_CAPTURE_RETENTION_CAPACITY_POLICY",
            "drifted_policy_v2",
            "raw_capture_contract_drift",
        ),
    )
    for name, value, error in cases:
        with monkeypatch.context() as isolated:
            isolated.setattr(subject, name, value)
            with pytest.raises(subject.RawCaptureArtifactBuildError, match=error):
                _derive()


@pytest.mark.parametrize("blitzortung_enabled", (False, True))
def test_build_roundtrip_is_raw_only_secret_free_and_target_pinned(
    monkeypatch,
    tmp_path,
    blitzortung_enabled: bool,
) -> None:
    sentinel = "must-never-enter-the-artifacts-7f41e9"
    monkeypatch.setenv("STV3_PROVIDER_API_KEY", sentinel)
    result = _build(
        tmp_path,
        blitzortung_enabled=blitzortung_enabled,
    )
    verified = subject.verify_raw_capture_shadow_artifacts(
        result.manifest_path
    )
    manifest = _load(result.manifest_path)
    config = _load(result.config_path)
    capacity = _load(result.capacity_path)

    assert verified["manifest_sha256"] == result.manifest_sha256
    assert manifest["artifact_role"] == "target_side_materialization"
    assert manifest["host_path_pinned"] is True
    assert manifest["portable_release_artifact"] is False
    assert manifest["target_materialization_required"] is True
    assert manifest["deployment_performed"] is False
    assert manifest["target_path_flavour"] == (
        "windows" if os.name == "nt" else "posix"
    )
    assert manifest["reproducibility_scope"] == (
        "same_canonical_target_root_and_path_semantics_only"
    )
    assert manifest["materializer"]["module_path"] == (
        "storm_tracker_worker/raw_capture_activation_artifacts.py"
    )
    assert manifest["materializer"]["contract"] == (
        "stv3-worker-raw-capture-activation-materializer-v2"
    )
    assert manifest["materializer"][
        "capacity_derivation_target_path_independent"
    ] is True
    assert manifest["raw_capture_config_contract"] == (
        RAW_CAPTURE_CONFIG_CONTRACT
    )
    assert manifest["raw_capture_runtime_contract"] == (
        RAW_CAPTURE_RUNTIME_CONTRACT
    )
    assert manifest["capacity_parameters"] == {
        "blitzortung_enabled": blitzortung_enabled,
        "capture_window_hours": 1,
        "operator_declared_target_bytes": MAX_SIGNED_INT64,
        "operator_host_operational_free_reserve_bytes": (
            HOST_RESERVE_BYTES
        ),
    }
    assert config["dashboard_output"] is False
    assert config["downstream_consumption_required"] is False
    assert config["meteorological_output"] is False
    assert config["raw_store_commit_is_terminal"] is True
    assert config["terminal_router_wired"] is False
    cadence_root = Path(config["acquisition"]["paths"]["raw_store_root"])
    event = config["event_ingress"]["blitzortung"]
    event_root = Path(event["raw_store_root"])
    control_root = Path(event["control_root"])
    assert event["enabled"] is blitzortung_enabled
    assert event["capacity_claimed"] is blitzortung_enabled
    assert config["retention"][
        "host_operational_free_reserve_bytes"
    ] == HOST_RESERVE_BYTES
    configured_paths = tuple(Path(value) for value in (
        *config["worker_first"]["paths"].values(),
        *config["acquisition"]["paths"].values(),
        event["raw_store_root"],
        event["control_root"],
    ))
    assert all(path.is_absolute() and path.resolve() == path for path in configured_paths)
    assert len(set(configured_paths)) == len(configured_paths)
    assert not any(
        left == right or left in right.parents or right in left.parents
        for index, left in enumerate(configured_paths)
        for right in configured_paths[index + 1 :]
    )
    assert {cadence_root, event_root, control_root}.issubset(configured_paths)
    assert event["raw_store_limits"] == capacity["event_ingress"][
        "raw_store_limits"
    ]
    assert event["ingress_limits"] == capacity["event_ingress"][
        "ingress_limits"
    ]
    assert event_root.is_dir() is blitzortung_enabled
    assert control_root.is_dir() is blitzortung_enabled
    mutable_paths = manifest["mutable_runtime_paths"]
    mutable_roles = {row["role"] for row in mutable_paths}
    assert all(
        row["hash_policy"] == "excluded_mutable_runtime_state"
        for row in mutable_paths
    )
    assert all(row["role"] not in {
        "worker_first.authority_file",
        "worker_first.ownership_lock_file",
    } for row in mutable_paths)
    assert (
        "event.blitzortung_control_root" in mutable_roles
    ) is blitzortung_enabled
    assert (
        "event.blitzortung_raw_store_root" in mutable_roles
    ) is blitzortung_enabled
    for path in result.output_directory.rglob("*"):
        if path.is_file():
            assert sentinel.encode() not in path.read_bytes()


@pytest.mark.parametrize("kind", ("directory", "file"))
def test_output_must_be_absent(kind: str, tmp_path) -> None:
    output = (tmp_path / "activation").resolve()
    if kind == "directory":
        output.mkdir()
    else:
        output.write_bytes(b"occupied")

    with pytest.raises(
        subject.RawCaptureArtifactBuildError,
        match="output_directory_already_exists",
    ):
        subject.build_raw_capture_shadow_artifacts(
            persistent_base_root=tmp_path.resolve(),
            output_directory=output,
            capture_window_hours=1,
            target_raw_capacity_bytes=MAX_SIGNED_INT64,
            blitzortung_enabled=True,
            operator_host_operational_free_reserve_bytes=(
                HOST_RESERVE_BYTES
            ),
            expected_catalog_sha256=CATALOG_SHA256,
            expected_activation_matrix_sha256=ACTIVATION_MATRIX_SHA256,
        )


def test_output_must_be_absolute_canonical_direct_child(tmp_path) -> None:
    base = tmp_path.resolve()
    cases = (
        Path("relative-activation"),
        base / "nested" / "activation",
        Path(str(base / "nested" / ".." / "activation")),
    )
    for output in cases:
        with pytest.raises(subject.RawCaptureArtifactBuildError):
            subject.build_raw_capture_shadow_artifacts(
                persistent_base_root=base,
                output_directory=output,
                capture_window_hours=1,
                target_raw_capacity_bytes=MAX_SIGNED_INT64,
                blitzortung_enabled=True,
                operator_host_operational_free_reserve_bytes=(
                    HOST_RESERVE_BYTES
                ),
                expected_catalog_sha256=CATALOG_SHA256,
                expected_activation_matrix_sha256=ACTIVATION_MATRIX_SHA256,
            )


def test_partial_failure_retains_manifestless_root(
    monkeypatch,
    tmp_path,
) -> None:
    original = subject._write_exclusive

    def fail_capacity(path: Path, body: bytes) -> None:
        if path.name == subject.CAPACITY_FILENAME:
            raise OSError("injected capacity write failure")
        original(path, body)

    monkeypatch.setattr(subject, "_write_exclusive", fail_capacity)
    output = (tmp_path / "activation").resolve()
    with pytest.raises(
        subject.RawCaptureArtifactBuildError,
        match="artifact_build_failed",
    ):
        subject.build_raw_capture_shadow_artifacts(
            persistent_base_root=tmp_path.resolve(),
            output_directory=output,
            capture_window_hours=1,
            target_raw_capacity_bytes=MAX_SIGNED_INT64,
            blitzortung_enabled=True,
            operator_host_operational_free_reserve_bytes=(
                HOST_RESERVE_BYTES
            ),
            expected_catalog_sha256=CATALOG_SHA256,
            expected_activation_matrix_sha256=ACTIVATION_MATRIX_SHA256,
        )

    assert output.is_dir()
    assert (output / "artifacts" / subject.CONFIG_FILENAME).is_file()
    assert not (output / "artifacts" / subject.MANIFEST_FILENAME).exists()


def test_verifier_rejects_artifact_tamper(tmp_path) -> None:
    result = _build(tmp_path)
    result.capacity_path.write_bytes(result.capacity_path.read_bytes() + b" ")

    with pytest.raises(
        subject.RawCaptureArtifactBuildError,
        match="manifest_artifact_hash_mismatch",
    ):
        subject.verify_raw_capture_shadow_artifacts(result.manifest_path)


def test_verifier_requires_exact_profile_bound_capacity_parameters(
    tmp_path,
) -> None:
    extra = _build(
        tmp_path,
        "extra-capacity-parameter",
        blitzortung_enabled=False,
    )
    manifest = _load(extra.manifest_path)
    manifest["capacity_parameters"]["unexpected"] = 1
    extra.manifest_path.write_bytes(_canonical(manifest))
    with pytest.raises(
        subject.RawCaptureArtifactBuildError,
        match="manifest_capacity_binding_invalid",
    ):
        subject.verify_raw_capture_shadow_artifacts(extra.manifest_path)

    profile_flip = _build(
        tmp_path,
        "profile-flip",
        blitzortung_enabled=False,
    )
    manifest = _load(profile_flip.manifest_path)
    manifest["capacity_parameters"]["blitzortung_enabled"] = True
    profile_flip.manifest_path.write_bytes(_canonical(manifest))
    with pytest.raises(
        subject.RawCaptureArtifactBuildError,
        match="manifest_runtime_path_inventory_invalid",
    ):
        subject.verify_raw_capture_shadow_artifacts(
            profile_flip.manifest_path
        )


def test_cross_flavour_or_portable_claim_is_rejected(tmp_path) -> None:
    portable = _build(tmp_path, "portable-claim")
    manifest = _load(portable.manifest_path)
    manifest["portable_release_artifact"] = True
    portable.manifest_path.write_bytes(_canonical(manifest))
    with pytest.raises(
        subject.RawCaptureArtifactBuildError,
        match="manifest_schema_invalid",
    ):
        subject.verify_raw_capture_shadow_artifacts(portable.manifest_path)

    cross_flavour = _build(tmp_path, "cross-flavour")
    manifest = _load(cross_flavour.manifest_path)
    manifest["target_path_flavour"] = (
        "posix" if os.name == "nt" else "windows"
    )
    cross_flavour.manifest_path.write_bytes(_canonical(manifest))
    with pytest.raises(
        subject.RawCaptureArtifactBuildError,
        match="manifest_schema_invalid",
    ):
        subject.verify_raw_capture_shadow_artifacts(
            cross_flavour.manifest_path
        )


def test_thin_cli_requires_explicit_inputs_and_reports_no_deploy(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    base = tmp_path.resolve()
    output = (base / "activation").resolve()
    captured: dict[str, object] = {}
    fake = SimpleNamespace(
        activation_environment={"STV3_RUNTIME_MODE": "raw_capture_shadow"},
        authority_sha256="a" * 64,
        capacity_path=output / "capacity.json",
        capacity_sha256="b" * 64,
        config_path=output / "config.json",
        config_sha256="c" * 64,
        manifest_path=output / "manifest.json",
        manifest_sha256="d" * 64,
        output_directory=output,
    )

    def fake_build(**kwargs):
        captured.update(kwargs)
        return fake

    monkeypatch.setattr(cli, "build_raw_capture_shadow_artifacts", fake_build)
    assert cli.main([
        "--persistent-base-root", str(base),
        "--output-directory", str(output),
        "--capture-window-hours", "24",
        "--target-raw-capacity-bytes", "123456",
        "--blitzortung-profile", "disabled",
        "--operator-host-reserve-bytes", str(HOST_RESERVE_BYTES),
        "--expected-catalog-sha256", CATALOG_SHA256,
        "--expected-activation-matrix-sha256", ACTIVATION_MATRIX_SHA256,
    ]) == 0
    result = json.loads(capsys.readouterr().out)

    assert captured == {
        "blitzortung_enabled": False,
        "capture_window_hours": 24,
        "expected_activation_matrix_sha256": ACTIVATION_MATRIX_SHA256,
        "expected_catalog_sha256": CATALOG_SHA256,
        "operator_host_operational_free_reserve_bytes": (
            HOST_RESERVE_BYTES
        ),
        "output_directory": output,
        "persistent_base_root": base,
        "target_raw_capacity_bytes": 123456,
    }
    assert result["artifact_role"] == "target_side_materialization"
    assert result["deployment_performed"] is False
    assert result["host_path_pinned"] is True
    assert result["portable_release_artifact"] is False
    assert result["target_materialization_required"] is True


@pytest.mark.parametrize(
    "invalid_tail",
    (
        (
            "--operator-host-reserve-bytes",
            str(HOST_RESERVE_BYTES),
        ),
        ("--blitzortung-profile", "disabled"),
        (
            "--blitzortung-profile",
            "automatic",
            "--operator-host-reserve-bytes",
            str(HOST_RESERVE_BYTES),
        ),
        (
            "--blitzortung-profile",
            "enabled",
            "--operator-host-reserve-bytes",
            "2GiB",
        ),
    ),
)
def test_cli_rejects_missing_or_invalid_profile_and_reserve(
    invalid_tail: tuple[str, ...],
    tmp_path,
) -> None:
    base = tmp_path.resolve()
    output = (base / "activation").resolve()
    argv = [
        "--persistent-base-root",
        str(base),
        "--output-directory",
        str(output),
        "--capture-window-hours",
        "1",
        "--target-raw-capacity-bytes",
        str(MAX_SIGNED_INT64),
        "--expected-catalog-sha256",
        CATALOG_SHA256,
        "--expected-activation-matrix-sha256",
        ACTIVATION_MATRIX_SHA256,
        *invalid_tail,
    ]

    with pytest.raises(SystemExit, match="2"):
        cli._arguments(argv)
