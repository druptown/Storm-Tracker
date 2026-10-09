"""Read-only target preflight and recovery-plan contract tests."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import raw_capture_target_preflight as subject


HOST_RESERVE_BYTES = 2 * 1024 * 1024 * 1024
REQUIRED_SAME_FILESYSTEM_BYTES = 20_694_695_936
DECLARED_TARGET_BYTES = 21_000_000_000
AVAILABLE_TARGET_BYTES = 22_000_000_000
SERVICE_PERSISTENT_MAX_BYTES = 17_733_517_312
GUARD_PERSISTENT_MAX_BYTES = 18_001_952_768
INTEGRATION_VERSION = "1.13.2"
WORKER_VERSION = "1.16.2"
BUILD_ID = (
    f"integration-{INTEGRATION_VERSION}-worker-{WORKER_VERSION}"
)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write(path: Path, value: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)
    return path


def _harness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    available_bytes: int = AVAILABLE_TARGET_BYTES,
) -> dict[str, object]:
    inputs = tmp_path / "inputs"
    requirements_path = _write(inputs / "requirements.json", b"requirements\n")
    materializer_path = _write(inputs / "materializer.py", b"MATERIALIZER = 1\n")
    options_document = {
        "contract": "stv3-sanitized-supervisor-options-v1",
        "options": {
            "blitzortung_raw_retention_authorized": False,
            "heartbeat_seconds": 10,
            "kmi_appv4_raw_retention_authorized": False,
            "log_level": "INFO",
            "opera_raw_retention_authorized": False,
            "queue_size": 16,
            "rainviewer_cache_attribution_accepted": False,
            "runtime_mode": "raw_capture_shadow",
            "synoptic_raw_retention_terms_accepted": False,
            "worker_first_authority_sha256": "",
            "worker_first_config_path": "",
            "worker_first_config_sha256": "",
            "worker_nice": 15,
        },
        "secret_presence": {
            "netatmo_client_id": True,
            "netatmo_client_secret": False,
            "netatmo_refresh_token": False,
        },
    }
    options_body = _canonical(options_document) + b"\n"
    options_path = _write(inputs / "options.json", options_body)
    rollback_integration = _write(
        inputs / "storm_tracker_v3_v1.11.0_deploy.zip",
        b"known integration rollback",
    )
    rollback_worker = _write(
        inputs / "storm_tracker_worker_v1.14.0_deploy.tar.gz",
        b"known worker rollback",
    )
    policy = subject.LegacyRollbackPolicy(
        integration_version="1.11.0",
        integration_sha256=_sha(rollback_integration.read_bytes()),
        worker_version="1.14.0",
        worker_sha256=_sha(rollback_worker.read_bytes()),
    )
    pinset = {
        "activation": {
            "activation_matrix_sha256": "a" * 64,
            "catalog_sha256": "b" * 64,
            "raw_capture_composition_sha256": "c" * 64,
            "recovery_binding_count": 27,
            "recovery_inventory_sha256": "d" * 64,
            "worker_service_sha256": "e" * 64,
        },
        "blitzortung": {
            "ingress_limits_sha256": "f" * 64,
            "operational_admission": False,
            "raw_store_limits_sha256": "1" * 64,
        },
        "capacity": {
            "capture_window_hours": 1,
            "contract": "stv3-worker-raw-capture-capacity-artifact-v2",
            "derivation_fixture_sha256": "2" * 64,
            "derivation_sentinel_target_bytes": (1 << 63) - 1,
            "minimum_required_same_filesystem_bytes": (
                REQUIRED_SAME_FILESYSTEM_BYTES
            ),
            "target_capacity_claimed": False,
        },
        "materializer": {
            "contract": "stv3-worker-raw-capture-activation-materializer-v2",
            "sha256": _sha(materializer_path.read_bytes()),
        },
        "retention": {
            "blitzortung_enabled": False,
            "capacity_policy": (
                "config_bound_optional_event_envelope_reserved_fail_closed_v2"
            ),
            "contract": "stv3-worker-raw-capture-retention-guard-v2",
            "event_capacity_claimed": False,
            "guard_configured_persistent_max_bytes": (
                GUARD_PERSISTENT_MAX_BYTES
            ),
            "host_operational_free_reserve_bytes": HOST_RESERVE_BYTES,
            "selected_profile": "cadence_only",
            "service_configured_persistent_max_bytes": (
                SERVICE_PERSISTENT_MAX_BYTES
            ),
            "source_sha256": "7" * 64,
        },
        "runtime_contracts": {
            "config": "stv3-worker-raw-capture-service-config-v6",
            "runtime": "stv3-worker-raw-capture-runtime-v6",
            "target_manifest": (
                "stv3-worker-raw-capture-shadow-artifact-manifest-v2"
            ),
        },
    }
    materializer_arguments = {
        "blitzortung_enabled": False,
        "capture_window_hours": 1,
        "expected_activation_matrix_sha256": "a" * 64,
        "expected_catalog_sha256": "b" * 64,
        "operator_host_operational_free_reserve_bytes": HOST_RESERVE_BYTES,
    }
    requirements = {
        "archives": {
            "integration": {
                "bytes": 123,
                "filename": (
                    f"storm_tracker_v3_v{INTEGRATION_VERSION}_deploy.zip"
                ),
                "root_name": "storm_tracker_v3",
                "sha256": "3" * 64,
                "source_tree_sha256": "4" * 64,
            },
            "worker": {
                "bytes": 456,
                "filename": (
                    f"storm_tracker_worker_v{WORKER_VERSION}_deploy.tar.gz"
                ),
                "root_name": "storm_tracker_worker",
                "sha256": "5" * 64,
                "source_tree_sha256": "6" * 64,
            },
        },
        "contract": "stv3-raw-capture-release-requirements-v2",
        "expected_build_id": BUILD_ID,
        "integration_version": INTEGRATION_VERSION,
        "materializer": {
            "bytes": len(materializer_path.read_bytes()),
            "contract": "stv3-worker-raw-capture-activation-materializer-v2",
            "member_path": (
                "storm_tracker_worker/raw_capture_activation_artifacts.py"
            ),
            "sha256": _sha(materializer_path.read_bytes()),
            "target_side_execution_required": True,
        },
        "materializer_arguments": materializer_arguments,
        "pinset": pinset,
        "release_evidence": {
            "bytes": 789,
            "contract": "stv3-release-evidence-v1",
            "filename": (
                f"storm_tracker_release_v{INTEGRATION_VERSION}_worker_v"
                f"{WORKER_VERSION}_evidence.json"
            ),
            "internal_sha256": "7" * 64,
            "sha256": "8" * 64,
        },
        "requirements_sha256": "9" * 64,
        "runtime_mode": "raw_capture_shadow",
        "sanitized_supervisor_options": {
            "bytes": len(options_body),
            "contract": "stv3-sanitized-supervisor-options-v1",
            "filename": "options.json",
            "sha256": _sha(options_body),
        },
        "target_profile": {
            "blitzortung_enabled": False,
            "canonical_absolute_path_selected_at_target": True,
            "contract": "stv3-raw-capture-target-materialization-profile-v2",
            "direct_child_output_required": True,
            "event_capacity_claimed": False,
            "host_identity_bound": False,
            "host_operational_free_reserve_bytes": HOST_RESERVE_BYTES,
            "path_semantics_selected_at_target": True,
            "reparse_or_symlink_allowed": False,
            "same_filesystem_layout_required": True,
            "selected_profile": "cadence_only",
        },
        "worker_version": WORKER_VERSION,
    }
    state = {"requirements": requirements}
    monkeypatch.setattr(
        subject,
        "verify_raw_capture_release_requirements",
        lambda *_args, **_kwargs: deepcopy(state["requirements"]),
    )
    monkeypatch.setattr(
        subject,
        "validate_frozen_sanitized_options_snapshot",
        lambda _path: deepcopy(options_document),
    )
    monkeypatch.setattr(
        subject,
        "derive_frozen_raw_capture_release_pinset",
        lambda **_kwargs: SimpleNamespace(
            to_mapping=lambda: deepcopy(state["requirements"]["pinset"])
        ),
    )
    monkeypatch.setattr(
        subject.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=available_bytes),
    )
    persistent = tmp_path / "persistent"
    persistent.mkdir()
    evidence_parent = tmp_path / "evidence"
    evidence_parent.mkdir()
    return {
        "evidence_output_directory": evidence_parent / "preflight-run",
        "materializer_path": materializer_path,
        "operator_declared_target_bytes": DECLARED_TARGET_BYTES,
        "persistent_base_root": persistent,
        "planned_output_directory": persistent / "raw-activation",
        "requirements_path": requirements_path,
        "rollback_integration_archive": rollback_integration,
        "rollback_policy": policy,
        "rollback_worker_archive": rollback_worker,
        "sanitized_options_snapshot": options_path,
        "state": state,
    }


def _build(arguments: dict[str, object]):
    supplied = dict(arguments)
    supplied.pop("state")
    return subject.build_raw_capture_target_canary_preflight(**supplied)


def test_preflight_publishes_one_atomic_non_deploy_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)

    result = _build(arguments)
    document = subject.verify_raw_capture_target_canary_preflight_evidence(
        result.evidence_path
    )

    assert result.local_preflight_passed is True
    assert result.local_preflight_blockers == ()
    assert [path.name for path in result.output_directory.iterdir()] == [
        subject.TARGET_CANARY_PREFLIGHT_FILENAME
    ]
    assert result.evidence_sha256 == _sha(result.evidence_path.read_bytes())
    assert subject.TARGET_CANARY_PREFLIGHT_CONTRACT == (
        "stv3-raw-capture-target-canary-preflight-v2"
    )
    assert subject.TARGET_CANARY_RECOVERY_PLAN_CONTRACT == (
        "stv3-raw-capture-target-canary-recovery-plan-v2"
    )
    assert document["contract"] == subject.TARGET_CANARY_PREFLIGHT_CONTRACT
    assert document["local_preflight"]["passed"] is True
    assert document["local_preflight"]["blockers"] == []
    assert document["claims"] == {
        "canary_execution_authorized": False,
        "canary_started": False,
        "dashboard_output": False,
        "deployment_bundle_complete": False,
        "deployment_performed": False,
        "human_deploy_approval_observed": False,
        "live_ha_mutation_performed": False,
        "live_target_verified": False,
        "meteorological_admission": False,
        "meteorological_output": False,
        "network_performed": False,
        "production_enable_ready": False,
        "real_installation_ready": False,
        "supervisor_mutation_performed": False,
        "target_live_verification_required": True,
        "target_materialization_performed": False,
        "target_read_only_observation_performed": True,
        "target_activation_ready": False,
        "trusted_target_attestation_unavailable": True,
    }
    expected_materializer_arguments = {
        "blitzortung_enabled": False,
        "capture_window_hours": 1,
        "expected_activation_matrix_sha256": "a" * 64,
        "expected_catalog_sha256": "b" * 64,
        "operator_host_operational_free_reserve_bytes": HOST_RESERVE_BYTES,
    }
    assert document["materializer_arguments"] == (
        expected_materializer_arguments
    )
    assert document["release"]["materializer_arguments"] == (
        expected_materializer_arguments
    )
    assert document["target_profile"]["selected_profile"] == "cadence_only"
    assert document["target_profile"]["blitzortung_enabled"] is False
    assert document["target_profile"]["event_capacity_claimed"] is False
    assert document["target_profile"][
        "host_operational_free_reserve_bytes"
    ] == HOST_RESERVE_BYTES
    assert document["capacity"]["selected_profile"] == "cadence_only"
    assert document["capacity"]["blitzortung_enabled"] is False
    assert document["capacity"]["event_capacity_claimed"] is False
    assert document["supervisor_options"]["present_secret_keys"] == [
        "netatmo_client_id"
    ]
    assert document["supervisor_options"][
        "live_supervisor_export_verified"
    ] is False
    assert document["recovery_plan"]["automatic_execution"] is False
    assert document["recovery_plan"]["execution_authorized"] is False
    assert document["recovery_plan"]["contract"] == (
        "stv3-raw-capture-target-canary-recovery-plan-v2"
    )
    assert document["recovery_plan"]["materialization"][
        "materializer_arguments"
    ] == {
        **expected_materializer_arguments,
        "output_directory": str(arguments["planned_output_directory"]),
        "persistent_base_root": str(arguments["persistent_base_root"]),
        "target_raw_capacity_bytes": arguments[
            "operator_declared_target_bytes"
        ],
    }
    assert [
        row["id"] for row in document["recovery_plan"]["forward_steps"]
    ] == list(subject.FORWARD_STEP_IDS)
    assert [
        row["id"] for row in document["recovery_plan"]["rollback_steps"]
    ] == list(subject.ROLLBACK_STEP_IDS)
    assert b"credential-secret-value" not in result.evidence_path.read_bytes()


@pytest.mark.parametrize(
    "declared,available,expected",
    (
        (
            DECLARED_TARGET_BYTES,
            DECLARED_TARGET_BYTES - 1,
            ["observed_available_capacity_below_declaration"],
        ),
        (
            REQUIRED_SAME_FILESYSTEM_BYTES - 1,
            AVAILABLE_TARGET_BYTES,
            ["operator_declared_capacity_below_release_minimum"],
        ),
        (
            REQUIRED_SAME_FILESYSTEM_BYTES - 1,
            REQUIRED_SAME_FILESYSTEM_BYTES - 2,
            [
                "observed_available_capacity_below_declaration",
                "operator_declared_capacity_below_release_minimum",
            ],
        ),
    ),
)
def test_capacity_blockers_publish_negative_evidence_without_authorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    declared: int,
    available: int,
    expected: list[str],
) -> None:
    arguments = _harness(
        tmp_path,
        monkeypatch,
        available_bytes=available,
    )
    arguments["operator_declared_target_bytes"] = declared

    result = _build(arguments)
    document = subject.verify_raw_capture_target_canary_preflight_evidence(
        result.evidence_path
    )

    assert result.local_preflight_passed is False
    assert list(result.local_preflight_blockers) == expected
    assert document["local_preflight"]["blockers"] == expected
    assert document["claims"]["deployment_performed"] is False
    assert document["claims"]["canary_execution_authorized"] is False


@pytest.mark.parametrize(
    "field,value",
    (
        ("selected_profile", "event_enabled"),
        ("blitzortung_enabled", True),
        ("event_capacity_claimed", True),
        ("host_operational_free_reserve_bytes", HOST_RESERVE_BYTES - 1),
    ),
)
def test_non_cadence_only_profile_fails_before_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)
    arguments["state"]["requirements"]["target_profile"][field] = value

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^requirements_target_profile_invalid$",
    ):
        _build(arguments)

    assert not Path(arguments["evidence_output_directory"]).exists()


@pytest.mark.parametrize(
    "field,value",
    (
        ("blitzortung_enabled", True),
        ("operator_host_operational_free_reserve_bytes", HOST_RESERVE_BYTES - 1),
        ("expected_catalog_sha256", "f" * 64),
    ),
)
def test_materializer_argument_drift_fails_before_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)
    arguments["state"]["requirements"]["materializer_arguments"][field] = value

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^requirements_materializer_arguments_invalid$",
    ):
        _build(arguments)

    assert not Path(arguments["evidence_output_directory"]).exists()


@pytest.mark.parametrize(
    "field,value",
    (
        ("selected_profile", "event_enabled"),
        ("blitzortung_enabled", True),
        ("event_capacity_claimed", True),
        ("host_operational_free_reserve_bytes", HOST_RESERVE_BYTES - 1),
    ),
)
def test_retention_pin_profile_drift_fails_before_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)
    arguments["state"]["requirements"]["pinset"]["retention"][field] = value

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^requirements_retention_pin_invalid$",
    ):
        _build(arguments)

    assert not Path(arguments["evidence_output_directory"]).exists()


def test_catalog_and_matrix_hashes_are_copied_from_verified_requirements(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)
    requirements = arguments["state"]["requirements"]
    activation = requirements["pinset"]["activation"]
    materializer_arguments = requirements["materializer_arguments"]
    activation["activation_matrix_sha256"] = "8" * 64
    activation["catalog_sha256"] = "9" * 64
    materializer_arguments["expected_activation_matrix_sha256"] = "8" * 64
    materializer_arguments["expected_catalog_sha256"] = "9" * 64

    result = _build(arguments)
    document = subject.verify_raw_capture_target_canary_preflight_evidence(
        result.evidence_path
    )

    assert document["materializer_arguments"][
        "expected_activation_matrix_sha256"
    ] == "8" * 64
    assert document["materializer_arguments"][
        "expected_catalog_sha256"
    ] == "9" * 64


def test_v1_requirements_contract_fails_before_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)
    arguments["state"]["requirements"]["contract"] = (
        "stv3-raw-capture-release-requirements-v1"
    )

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^requirements_contract_invalid$",
    ):
        _build(arguments)

    assert not Path(arguments["evidence_output_directory"]).exists()


def test_materializer_pinset_drift_fails_before_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)
    monkeypatch.setattr(
        subject,
        "derive_frozen_raw_capture_release_pinset",
        lambda **_kwargs: SimpleNamespace(to_mapping=lambda: {"drift": True}),
    )

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^materializer_pinset_mismatch$",
    ):
        _build(arguments)

    assert not Path(arguments["evidence_output_directory"]).exists()


def test_rollback_hash_drift_fails_before_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)
    Path(arguments["rollback_worker_archive"]).write_bytes(b"tampered")

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^rollback_worker_identity_mismatch$",
    ):
        _build(arguments)

    assert not Path(arguments["evidence_output_directory"]).exists()


def test_release_binding_change_between_reproofs_fails_before_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)
    state = arguments["state"]
    calls = 0

    def changing_requirements(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        document = deepcopy(state["requirements"])
        if calls > 1:
            document["expected_build_id"] = (
                f"integration-{INTEGRATION_VERSION}-worker-9.9.9"
            )
        return document

    monkeypatch.setattr(
        subject,
        "verify_raw_capture_release_requirements",
        changing_requirements,
    )

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^preflight_source_bindings_changed$",
    ):
        _build(arguments)

    assert not Path(arguments["evidence_output_directory"]).exists()


def test_target_and_evidence_roots_must_be_absent_and_disjoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)
    planned = Path(arguments["planned_output_directory"])
    planned.mkdir()

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^planned_output_directory_invalid$",
    ):
        _build(arguments)

    planned.rmdir()
    arguments["evidence_output_directory"] = (
        Path(arguments["persistent_base_root"]) / "evidence"
    )
    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^evidence_output_overlaps_target$",
    ):
        _build(arguments)


def test_existing_evidence_run_is_never_overwritten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)
    output = Path(arguments["evidence_output_directory"])
    output.mkdir()
    sentinel = _write(output / "keep.txt", b"keep")

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^evidence_output_directory_invalid$",
    ):
        _build(arguments)

    assert sentinel.read_bytes() == b"keep"


def test_committed_evidence_survives_run_authority_close_error_as_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)
    real_close = subject.ReleaseInstallOutputAuthority.close

    def close_then_report_error(authority) -> None:
        real_close(authority)
        raise RuntimeError("injected run-authority close error")

    monkeypatch.setattr(
        subject.ReleaseInstallOutputAuthority,
        "close",
        close_then_report_error,
    )

    result = _build(arguments)

    assert result.local_preflight_passed is True
    assert subject.verify_raw_capture_target_canary_preflight_evidence(
        result.evidence_path
    )["claims"]["deployment_performed"] is False


def test_committed_evidence_survives_outer_authority_close_error_as_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)
    real_open = subject.DirectoryAuthority.open_absolute
    real_close = subject.DirectoryAuthority.close
    tracked: dict[str, object] = {}

    def tracking_open(path: Path):
        authority = real_open(path)
        tracked["outer"] = authority
        return authority

    def close_then_report_outer_error(authority) -> None:
        real_close(authority)
        if authority is tracked.get("outer"):
            raise RuntimeError("injected outer-authority close error")

    monkeypatch.setattr(
        subject.DirectoryAuthority,
        "open_absolute",
        staticmethod(tracking_open),
    )
    monkeypatch.setattr(
        subject.DirectoryAuthority,
        "close",
        close_then_report_outer_error,
    )

    result = _build(arguments)

    assert result.local_preflight_passed is True
    assert subject.verify_raw_capture_target_canary_preflight_evidence(
        result.evidence_path
    )["claims"]["deployment_performed"] is False


def test_precommit_publication_failure_removes_owned_evidence_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)

    def fail_write(*_args, **_kwargs):
        raise RuntimeError("injected precommit write failure")

    monkeypatch.setattr(
        subject.ReleaseInstallOutputAuthority,
        "write_bytes_noreplace",
        fail_write,
    )

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^preflight_evidence_publication_failed$",
    ):
        _build(arguments)

    assert not Path(arguments["evidence_output_directory"]).exists()


def test_unproved_precommit_cleanup_retains_forensic_root_and_blocks_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments = _harness(tmp_path, monkeypatch)

    def fail_write(*_args, **_kwargs):
        raise RuntimeError("injected precommit write failure")

    def fail_cleanup(*_args, **_kwargs):
        raise RuntimeError("injected identity cleanup failure")

    monkeypatch.setattr(
        subject.ReleaseInstallOutputAuthority,
        "write_bytes_noreplace",
        fail_write,
    )
    monkeypatch.setattr(
        subject.ReleaseInstallOutputAuthority,
        "remove_created_run_root",
        fail_cleanup,
    )

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^preflight_evidence_publication_cleanup_failed$",
    ):
        _build(arguments)

    output = Path(arguments["evidence_output_directory"])
    assert output.is_dir()
    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^evidence_output_directory_invalid$",
    ):
        _build(arguments)


def test_verifier_rejects_tampered_readiness_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _build(_harness(tmp_path, monkeypatch))
    document = json.loads(result.evidence_path.read_text(encoding="ascii"))
    document["claims"]["deployment_performed"] = True
    unsigned = dict(document)
    unsigned.pop("preflight_evidence_sha256")
    document["preflight_evidence_sha256"] = _sha(_canonical(unsigned))
    result.evidence_path.write_bytes(_canonical(document) + b"\n")

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^preflight_claims_invalid$",
    ):
        subject.verify_raw_capture_target_canary_preflight_evidence(
            result.evidence_path
        )


def test_verifier_rejects_forged_target_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _build(_harness(tmp_path, monkeypatch))
    document = json.loads(result.evidence_path.read_text(encoding="ascii"))
    document["target_profile"]["event_capacity_claimed"] = True
    unsigned = dict(document)
    unsigned.pop("preflight_evidence_sha256")
    document["preflight_evidence_sha256"] = _sha(_canonical(unsigned))
    result.evidence_path.write_bytes(_canonical(document) + b"\n")

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^preflight_target_profile_invalid$",
    ):
        subject.verify_raw_capture_target_canary_preflight_evidence(
            result.evidence_path
        )


def test_verifier_rejects_divergent_materializer_argument_copies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _build(_harness(tmp_path, monkeypatch))
    document = json.loads(result.evidence_path.read_text(encoding="ascii"))
    document["materializer_arguments"]["expected_catalog_sha256"] = "0" * 64
    unsigned = dict(document)
    unsigned.pop("preflight_evidence_sha256")
    document["preflight_evidence_sha256"] = _sha(_canonical(unsigned))
    result.evidence_path.write_bytes(_canonical(document) + b"\n")

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^preflight_release_invalid$",
    ):
        subject.verify_raw_capture_target_canary_preflight_evidence(
            result.evidence_path
        )


def test_verifier_rejects_forged_performed_plan_step(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _build(_harness(tmp_path, monkeypatch))
    document = json.loads(result.evidence_path.read_text(encoding="ascii"))
    document["recovery_plan"]["forward_steps"][0]["performed"] = True
    document["recovery_plan_sha256"] = _sha(
        _canonical(document["recovery_plan"])
    )
    unsigned = dict(document)
    unsigned.pop("preflight_evidence_sha256")
    document["preflight_evidence_sha256"] = _sha(_canonical(unsigned))
    result.evidence_path.write_bytes(_canonical(document) + b"\n")

    with pytest.raises(
        subject.TargetCanaryPreflightError,
        match="^recovery_plan_invalid$",
    ):
        subject.verify_raw_capture_target_canary_preflight_evidence(
            result.evidence_path
        )


def test_cli_summary_keeps_every_mutation_and_readiness_claim_false(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import scripts.preflight_raw_capture_target as cli

    arguments = SimpleNamespace(
        evidence_output_directory=Path("evidence"),
        materializer_module=Path("materializer"),
        operator_declared_target_bytes=1,
        persistent_base_root=Path("persistent"),
        planned_output_directory=Path("persistent/raw"),
        raw_requirements=Path("requirements"),
        rollback_integration_archive=Path("integration"),
        rollback_worker_archive=Path("worker"),
        sanitized_options_snapshot=Path("options"),
    )
    monkeypatch.setattr(cli, "_arguments", lambda: arguments)
    monkeypatch.setattr(
        cli,
        "build_raw_capture_target_canary_preflight",
        lambda **_kwargs: subject.TargetCanaryPreflightResult(
            output_directory=Path("evidence"),
            evidence_path=Path("evidence/preflight.json"),
            evidence_sha256="a" * 64,
            local_preflight_passed=True,
            local_preflight_blockers=(),
        ),
    )

    assert cli.main() == 0
    report = json.loads(capsys.readouterr().out)
    assert report["deployment_performed"] is False
    assert report["canary_execution_authorized"] is False
    assert report["canary_started"] is False
    assert report["dashboard_output"] is False
    assert report["deployment_bundle_complete"] is False
    assert report["live_ha_mutation_performed"] is False
    assert report["live_target_verified"] is False
    assert report["meteorological_admission"] is False
    assert report["meteorological_output"] is False
    assert report["network_performed"] is False
    assert report["production_enable_ready"] is False
    assert report["real_installation_ready"] is False
    assert report["supervisor_mutation_performed"] is False
    assert report["target_materialization_performed"] is False
    assert report["target_activation_ready"] is False
