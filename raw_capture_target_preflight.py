"""Read-only target preflight and recovery plan for a RAW release quartet.

This module is deliberately external release tooling.  It validates the
portable RAW quartet, re-derives the archived materializer pins, observes one
planned target filesystem without creating the target root, binds the proven
rollback packages, and publishes one diagnostic evidence document.  It never
contacts Home Assistant, Supervisor, MQTT, or a network service and it never
authorizes or performs deployment.

The resulting document is a local diagnostic, not a target trust anchor.  A
real target export, materialization, Supervisor build, runtime canary, and
human approval remain mandatory external gates.

The v2 contract accepts only the release-selected cadence-only profile.  Its
event ingress is disabled and claims no event capacity; the exact five
materializer arguments are copied from the verified requirements and their
activation/capacity pins into both evidence and the inert recovery plan.

Publication has one explicit commit boundary: after the descriptor-relative
final identity and bytes have been reproved, the evidence is successful and
is retained even if a later descriptor close reports an error.  Before that
boundary every failure rolls back only the newly owned evidence directory.  If
that identity-bound rollback itself cannot be proved, the residue is retained
for forensic inspection and the fixed ``*_cleanup_failed`` error forbids an
ambiguous retry into the same directory.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
from typing import Final, Mapping

from descriptor_relative_authority import DirectoryAuthority, RelativePath
from release_install_dry_run import (
    LegacyRollbackPolicy,
    RECOVERY_BASELINE_POLICY,
)
from release_install_output_authority import (
    ReleaseInstallOutputAuthority,
)
from storm_tracker_worker.archive_member_stream import PRIVATE_KEY_MARKERS
from storm_tracker_worker.raw_capture_release_requirements import (
    RAW_CAPTURE_RUNTIME_MODE,
    RAW_RELEASE_REQUIREMENTS_CONTRACT,
    TARGET_PROFILE_CONTRACT,
    RawCaptureReleaseRequirementsError,
    derive_frozen_raw_capture_release_pinset,
    validate_frozen_sanitized_options_snapshot,
    verify_raw_capture_release_requirements,
)


TARGET_CANARY_PREFLIGHT_CONTRACT: Final = (
    "stv3-raw-capture-target-canary-preflight-v2"
)
TARGET_CANARY_RECOVERY_PLAN_CONTRACT: Final = (
    "stv3-raw-capture-target-canary-recovery-plan-v2"
)
SELECTED_TARGET_PROFILE: Final = "cadence_only"
REQUIRED_HOST_OPERATIONAL_FREE_RESERVE_BYTES: Final = 2 * 1024 * 1024 * 1024
TARGET_CANARY_PREFLIGHT_FILENAME: Final = "target_canary_preflight_evidence.json"
MAX_PREFLIGHT_EVIDENCE_BYTES: Final = 2 * 1024 * 1024
MAX_BOUND_INPUT_BYTES: Final = 512 * 1024 * 1024
STREAM_CHUNK_BYTES: Final = 1024 * 1024

EXTERNAL_EXECUTION_GATES: Final = (
    "human_deploy_approval_not_granted",
    "live_supervisor_export_not_verified",
    "supervisor_build_and_cache_not_exercised",
    "target_materialization_not_performed",
    "trusted_target_attestation_unavailable",
    "runtime_stop_start_not_exercised",
    "live_ha_mqtt_health_not_verified",
    "physical_power_loss_not_exercised",
)
FORWARD_STEP_IDS: Final = (
    "capture_live_state",
    "create_active_code_backups",
    "materialize_absent_raw_root",
    "verify_target_manifest_and_capacity",
    "stage_and_verify_candidate_archives",
    "apply_manifest_derived_supervisor_pins",
    "start_bounded_raw_canary",
    "run_live_acceptance_checks",
)
ROLLBACK_STEP_IDS: Final = (
    "stop_candidate_worker",
    "restore_active_code_backups",
    "restore_exact_supervisor_export",
    "restart_recovered_components",
    "run_full_recovery_checks",
    "retain_preflight_and_rollback_evidence",
)
STOP_CONDITIONS: Final = (
    "archive_or_version_identity_mismatch",
    "target_manifest_or_materializer_pin_mismatch",
    "same_filesystem_capacity_below_declaration",
    "supervisor_slug_or_active_path_unproven",
    "worker_error_or_crashloop",
    "worker_status_not_online",
    "failed_jobs_increase",
    "ha_or_zigbee2mqtt_latency_regression",
    "history_or_database_loss",
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


class TargetCanaryPreflightError(RuntimeError):
    """The target preflight cannot continue without weakening its contract."""


@dataclass(frozen=True, slots=True)
class TargetCanaryPreflightResult:
    output_directory: Path
    evidence_path: Path
    evidence_sha256: str
    local_preflight_passed: bool
    local_preflight_blockers: tuple[str, ...]


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as error:
        raise TargetCanaryPreflightError(
            "preflight_document_not_canonicalizable"
        ) from error


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _checked_sha256(value: object, code: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise TargetCanaryPreflightError(code)
    return value


def _checked_version(value: object, code: str) -> str:
    if not isinstance(value, str) or _VERSION.fullmatch(value) is None:
        raise TargetCanaryPreflightError(code)
    return value


def _mapping(value: object, code: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(
        not isinstance(key, str) for key in value
    ):
        raise TargetCanaryPreflightError(code)
    return value


def _positive_integer(value: object, code: str) -> int:
    if type(value) is not int or value <= 0 or value > (1 << 63) - 1:
        raise TargetCanaryPreflightError(code)
    return value


def _required_target_profile() -> dict[str, object]:
    return {
        "blitzortung_enabled": False,
        "canonical_absolute_path_selected_at_target": True,
        "contract": TARGET_PROFILE_CONTRACT,
        "direct_child_output_required": True,
        "event_capacity_claimed": False,
        "host_identity_bound": False,
        "host_operational_free_reserve_bytes": (
            REQUIRED_HOST_OPERATIONAL_FREE_RESERVE_BYTES
        ),
        "path_semantics_selected_at_target": True,
        "reparse_or_symlink_allowed": False,
        "same_filesystem_layout_required": True,
        "selected_profile": SELECTED_TARGET_PROFILE,
    }


def _selected_materialization_profile(
    requirements: Mapping[str, object],
    *,
    activation: Mapping[str, object],
    capacity: Mapping[str, object],
    retention: Mapping[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    """Bind the exact v2 cadence-only profile to executable arguments."""

    if requirements.get("contract") != RAW_RELEASE_REQUIREMENTS_CONTRACT:
        raise TargetCanaryPreflightError("requirements_contract_invalid")
    profile = _mapping(
        requirements.get("target_profile"),
        "requirements_target_profile_invalid",
    )
    if dict(profile) != _required_target_profile():
        raise TargetCanaryPreflightError(
            "requirements_target_profile_invalid"
        )
    if (
        set(retention) != {
            "blitzortung_enabled",
            "capacity_policy",
            "contract",
            "event_capacity_claimed",
            "guard_configured_persistent_max_bytes",
            "host_operational_free_reserve_bytes",
            "selected_profile",
            "service_configured_persistent_max_bytes",
            "source_sha256",
        }
        or retention.get("selected_profile")
        != profile.get("selected_profile")
        or retention.get("blitzortung_enabled")
        is not profile.get("blitzortung_enabled")
        or retention.get("event_capacity_claimed")
        is not profile.get("event_capacity_claimed")
        or retention.get("host_operational_free_reserve_bytes")
        != profile.get("host_operational_free_reserve_bytes")
    ):
        raise TargetCanaryPreflightError(
            "requirements_retention_pin_invalid"
        )
    _checked_sha256(
        retention.get("source_sha256"),
        "requirements_retention_pin_invalid",
    )
    _positive_integer(
        retention.get("service_configured_persistent_max_bytes"),
        "requirements_retention_pin_invalid",
    )
    _positive_integer(
        retention.get("guard_configured_persistent_max_bytes"),
        "requirements_retention_pin_invalid",
    )

    arguments = _mapping(
        requirements.get("materializer_arguments"),
        "requirements_materializer_arguments_invalid",
    )
    expected_arguments = {
        "blitzortung_enabled": False,
        "capture_window_hours": capacity.get("capture_window_hours"),
        "expected_activation_matrix_sha256": activation.get(
            "activation_matrix_sha256"
        ),
        "expected_catalog_sha256": activation.get("catalog_sha256"),
        "operator_host_operational_free_reserve_bytes": (
            REQUIRED_HOST_OPERATIONAL_FREE_RESERVE_BYTES
        ),
    }
    if dict(arguments) != expected_arguments:
        raise TargetCanaryPreflightError(
            "requirements_materializer_arguments_invalid"
        )
    _positive_integer(
        arguments.get("capture_window_hours"),
        "requirements_materializer_arguments_invalid",
    )
    _checked_sha256(
        arguments.get("expected_activation_matrix_sha256"),
        "requirements_materializer_arguments_invalid",
    )
    _checked_sha256(
        arguments.get("expected_catalog_sha256"),
        "requirements_materializer_arguments_invalid",
    )
    return dict(profile), dict(arguments)


def _is_reparse(metadata: os.stat_result) -> bool:
    marker = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    attributes = int(getattr(metadata, "st_file_attributes", 0))
    return bool(attributes & marker)


def _path_identity(path: Path) -> str:
    return os.path.normcase(str(path))


def _canonical_existing_directory(path: Path, *, code: str) -> Path:
    supplied = Path(os.path.abspath(Path(path)))
    if not supplied.is_absolute() or supplied == Path(supplied.anchor):
        raise TargetCanaryPreflightError(code)
    try:
        resolved = supplied.resolve(strict=True)
        for component in (supplied, *supplied.parents):
            metadata = component.lstat()
            if (
                stat.S_ISLNK(metadata.st_mode)
                or _is_reparse(metadata)
                or not stat.S_ISDIR(metadata.st_mode)
            ):
                raise TargetCanaryPreflightError(code)
    except TargetCanaryPreflightError:
        raise
    except (OSError, RuntimeError) as error:
        raise TargetCanaryPreflightError(code) from error
    if _path_identity(supplied) != _path_identity(resolved):
        raise TargetCanaryPreflightError(code)
    return resolved


def _canonical_absent_direct_child(
    path: Path,
    *,
    parent: Path,
    code: str,
) -> Path:
    supplied = Path(os.path.abspath(Path(path)))
    if (
        not supplied.is_absolute()
        or supplied == Path(supplied.anchor)
        or supplied.parent != parent
        or supplied.name in {"", ".", ".."}
    ):
        raise TargetCanaryPreflightError(code)
    if os.path.lexists(supplied):
        raise TargetCanaryPreflightError(code)
    return supplied


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _metadata_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        int(metadata.st_dev),
        int(metadata.st_ino),
        int(metadata.st_mode),
        int(metadata.st_nlink),
        int(metadata.st_size),
        int(metadata.st_mtime_ns),
    )


def _bound_regular_file(
    path: Path,
    *,
    maximum_bytes: int = MAX_BOUND_INPUT_BYTES,
    code: str,
) -> dict[str, object]:
    supplied = Path(os.path.abspath(Path(path)))
    try:
        resolved = supplied.resolve(strict=True)
        lexical = supplied.lstat()
    except (OSError, RuntimeError) as error:
        raise TargetCanaryPreflightError(code) from error
    if (
        _path_identity(supplied) != _path_identity(resolved)
        or stat.S_ISLNK(lexical.st_mode)
        or _is_reparse(lexical)
        or not stat.S_ISREG(lexical.st_mode)
        or int(lexical.st_nlink) != 1
        or int(lexical.st_size) > maximum_bytes
    ):
        raise TargetCanaryPreflightError(code)
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = -1
    digest = hashlib.sha256()
    observed_bytes = 0
    try:
        descriptor = os.open(resolved, flags)
        opened = os.fstat(descriptor)
        if (
            _metadata_identity(opened) != _metadata_identity(lexical)
            or not stat.S_ISREG(opened.st_mode)
            or int(opened.st_nlink) != 1
            or int(opened.st_size) > maximum_bytes
        ):
            raise TargetCanaryPreflightError(code)
        while True:
            chunk = os.read(descriptor, STREAM_CHUNK_BYTES)
            if not chunk:
                break
            observed_bytes += len(chunk)
            if observed_bytes > maximum_bytes:
                raise TargetCanaryPreflightError(code)
            digest.update(chunk)
        after = os.fstat(descriptor)
        if (
            _metadata_identity(after) != _metadata_identity(opened)
            or observed_bytes != int(opened.st_size)
        ):
            raise TargetCanaryPreflightError(code)
    except TargetCanaryPreflightError:
        raise
    except OSError as error:
        raise TargetCanaryPreflightError(code) from error
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass
    try:
        final = resolved.lstat()
    except OSError as error:
        raise TargetCanaryPreflightError(code) from error
    if _metadata_identity(final) != _metadata_identity(lexical):
        raise TargetCanaryPreflightError(code)
    return {
        "bytes": observed_bytes,
        "filename": resolved.name,
        "sha256": digest.hexdigest(),
    }


def _strict_document(path: Path) -> dict[str, object]:
    descriptor = _bound_regular_file(
        path,
        maximum_bytes=MAX_PREFLIGHT_EVIDENCE_BYTES,
        code="preflight_evidence_unavailable",
    )
    try:
        body = Path(path).read_bytes()
    except OSError as error:
        raise TargetCanaryPreflightError(
            "preflight_evidence_unavailable"
        ) from error
    if len(body) != descriptor["bytes"] or _sha_bytes(body) != descriptor["sha256"]:
        raise TargetCanaryPreflightError("preflight_evidence_changed")
    final_descriptor = _bound_regular_file(
        path,
        maximum_bytes=MAX_PREFLIGHT_EVIDENCE_BYTES,
        code="preflight_evidence_unavailable",
    )
    if final_descriptor != descriptor:
        raise TargetCanaryPreflightError("preflight_evidence_changed")
    duplicates = False

    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        nonlocal duplicates
        result: dict[str, object] = {}
        for key, value in items:
            if key in result:
                duplicates = True
            result[key] = value
        return result

    try:
        document = json.loads(
            body.decode("ascii"),
            object_pairs_hook=pairs,
            parse_float=lambda _value: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise TargetCanaryPreflightError(
            "preflight_evidence_json_invalid"
        ) from error
    if (
        duplicates
        or not isinstance(document, dict)
        or _canonical(document) + b"\n" != body
    ):
        raise TargetCanaryPreflightError(
            "preflight_evidence_not_canonical"
        )
    return document


def _release_bindings(
    *,
    requirements_path: Path,
    sanitized_options_snapshot: Path,
    materializer_path: Path,
    rollback_integration_archive: Path,
    rollback_worker_archive: Path,
    rollback_policy: LegacyRollbackPolicy,
) -> dict[str, object]:
    try:
        requirements = verify_raw_capture_release_requirements(
            Path(requirements_path),
            sanitized_options_snapshot=Path(sanitized_options_snapshot),
        )
        options = validate_frozen_sanitized_options_snapshot(
            Path(sanitized_options_snapshot)
        )
        derived_pinset = derive_frozen_raw_capture_release_pinset(
            materializer_path=Path(materializer_path)
        ).to_mapping()
    except (RawCaptureReleaseRequirementsError, OSError, ValueError) as error:
        raise TargetCanaryPreflightError(
            "raw_release_quartet_or_pinset_invalid"
        ) from error
    if derived_pinset != requirements.get("pinset"):
        raise TargetCanaryPreflightError("materializer_pinset_mismatch")

    requirement_file = _bound_regular_file(
        Path(requirements_path),
        maximum_bytes=MAX_PREFLIGHT_EVIDENCE_BYTES,
        code="requirements_file_binding_failed",
    )
    options_file = _bound_regular_file(
        Path(sanitized_options_snapshot),
        maximum_bytes=MAX_PREFLIGHT_EVIDENCE_BYTES,
        code="supervisor_options_binding_failed",
    )
    materializer_file = _bound_regular_file(
        Path(materializer_path),
        maximum_bytes=MAX_PREFLIGHT_EVIDENCE_BYTES,
        code="materializer_file_binding_failed",
    )
    materializer = _mapping(
        requirements.get("materializer"),
        "requirements_materializer_invalid",
    )
    if (
        materializer_file["sha256"] != materializer.get("sha256")
        or materializer_file["bytes"] != materializer.get("bytes")
    ):
        raise TargetCanaryPreflightError("materializer_file_binding_mismatch")
    expected_options = _mapping(
        requirements.get("sanitized_supervisor_options"),
        "requirements_supervisor_options_invalid",
    )
    if (
        options_file["sha256"] != expected_options.get("sha256")
        or options_file["bytes"] != expected_options.get("bytes")
    ):
        raise TargetCanaryPreflightError("supervisor_options_binding_mismatch")

    integration = _bound_regular_file(
        Path(rollback_integration_archive),
        code="rollback_integration_binding_failed",
    )
    worker = _bound_regular_file(
        Path(rollback_worker_archive),
        code="rollback_worker_binding_failed",
    )
    expected_integration_name = (
        f"storm_tracker_v3_v{rollback_policy.integration_version}_deploy.zip"
    )
    expected_worker_name = (
        f"storm_tracker_worker_v{rollback_policy.worker_version}_deploy.tar.gz"
    )
    if (
        integration["filename"] != expected_integration_name
        or integration["sha256"] != rollback_policy.integration_sha256
    ):
        raise TargetCanaryPreflightError("rollback_integration_identity_mismatch")
    if (
        worker["filename"] != expected_worker_name
        or worker["sha256"] != rollback_policy.worker_sha256
    ):
        raise TargetCanaryPreflightError("rollback_worker_identity_mismatch")

    option_values = _mapping(options.get("options"), "supervisor_options_invalid")
    secret_presence = _mapping(
        options.get("secret_presence"),
        "supervisor_secret_presence_invalid",
    )
    retention_keys = tuple(sorted(
        key for key in option_values
        if key.endswith("_authorized") or key.endswith("_accepted")
    ))
    if (
        option_values.get("runtime_mode") != RAW_CAPTURE_RUNTIME_MODE
        or option_values.get("worker_first_config_path") != ""
        or option_values.get("worker_first_config_sha256") != ""
        or option_values.get("worker_first_authority_sha256") != ""
        or not retention_keys
        or any(option_values[key] is not False for key in retention_keys)
        or any(type(value) is not bool for value in secret_presence.values())
    ):
        raise TargetCanaryPreflightError("supervisor_options_posture_invalid")

    archives = _mapping(requirements.get("archives"), "release_archives_invalid")
    release_evidence = _mapping(
        requirements.get("release_evidence"),
        "release_evidence_descriptor_invalid",
    )
    capacity = _mapping(
        _mapping(requirements.get("pinset"), "release_pinset_invalid").get(
            "capacity"
        ),
        "release_capacity_pin_invalid",
    )
    activation = _mapping(
        _mapping(requirements.get("pinset"), "release_pinset_invalid").get(
            "activation"
        ),
        "release_activation_pin_invalid",
    )
    retention = _mapping(
        _mapping(requirements.get("pinset"), "release_pinset_invalid").get(
            "retention"
        ),
        "release_retention_pin_invalid",
    )
    target_profile, materializer_arguments = _selected_materialization_profile(
        requirements,
        activation=activation,
        capacity=capacity,
        retention=retention,
    )
    return {
        "activation": dict(activation),
        "capacity": dict(capacity),
        "materializer": {
            "bytes": materializer_file["bytes"],
            "contract": materializer.get("contract"),
            "member_path": materializer.get("member_path"),
            "sha256": materializer_file["sha256"],
        },
        "retention": dict(retention),
        "release": {
            "archives": {
                "integration": dict(_mapping(
                    archives.get("integration"),
                    "integration_archive_descriptor_invalid",
                )),
                "worker": dict(_mapping(
                    archives.get("worker"),
                    "worker_archive_descriptor_invalid",
                )),
            },
            "build_id": requirements.get("expected_build_id"),
            "integration_version": requirements.get("integration_version"),
            "materializer_arguments": materializer_arguments,
            "release_evidence": dict(release_evidence),
            "requirements": {
                **requirement_file,
                "contract": requirements.get("contract"),
                "internal_sha256": requirements.get("requirements_sha256"),
            },
            "runtime_mode": requirements.get("runtime_mode"),
            "selected_target_profile": target_profile.get(
                "selected_profile"
            ),
            "worker_version": requirements.get("worker_version"),
        },
        "rollback": {
            "integration": integration,
            "integration_version": rollback_policy.integration_version,
            "worker": worker,
            "worker_version": rollback_policy.worker_version,
        },
        "supervisor": {
            "bytes": options_file["bytes"],
            "contract": options.get("contract"),
            "exact_release_snapshot_verified": True,
            "live_supervisor_export_verified": False,
            "option_keys": sorted(option_values),
            "present_secret_keys": sorted(
                key for key, present in secret_presence.items() if present is True
            ),
            "retention_authorities_default_closed": True,
            "retention_authority_keys": list(retention_keys),
            "runtime_mode": RAW_CAPTURE_RUNTIME_MODE,
            "secret_presence_keys": sorted(secret_presence),
            "sha256": options_file["sha256"],
            "worker_first_pins_empty": True,
        },
        "target_profile": target_profile,
    }


def _target_observation(
    *,
    persistent_base_root: Path,
    planned_output_directory: Path,
    evidence_output_directory: Path,
    operator_declared_target_bytes: int,
    required_same_filesystem_bytes: int,
) -> dict[str, object]:
    base = _canonical_existing_directory(
        Path(persistent_base_root),
        code="persistent_base_root_invalid",
    )
    planned = _canonical_absent_direct_child(
        Path(planned_output_directory),
        parent=base,
        code="planned_output_directory_invalid",
    )
    evidence_parent = _canonical_existing_directory(
        Path(evidence_output_directory).parent,
        code="evidence_output_parent_invalid",
    )
    evidence_output = _canonical_absent_direct_child(
        Path(evidence_output_directory),
        parent=evidence_parent,
        code="evidence_output_directory_invalid",
    )
    if (
        _paths_overlap(evidence_output, base)
        or _paths_overlap(evidence_output, planned)
    ):
        raise TargetCanaryPreflightError("evidence_output_overlaps_target")
    declared = _positive_integer(
        operator_declared_target_bytes,
        "operator_declared_target_bytes_invalid",
    )
    required = _positive_integer(
        required_same_filesystem_bytes,
        "required_same_filesystem_bytes_invalid",
    )
    try:
        available = int(shutil.disk_usage(base).free)
        device = int(base.stat(follow_symlinks=False).st_dev)
    except (OSError, TypeError, ValueError) as error:
        raise TargetCanaryPreflightError(
            "target_capacity_observation_failed"
        ) from error
    if available < 0 or device < 0:
        raise TargetCanaryPreflightError("target_capacity_observation_invalid")
    blockers: list[str] = []
    if declared < required:
        blockers.append("operator_declared_capacity_below_release_minimum")
    if available < declared:
        blockers.append("observed_available_capacity_below_declaration")
    return {
        "blockers": tuple(sorted(blockers)),
        "capacity": {
            "availability_satisfied": available >= declared,
            "capture_window_hours": None,
            "contract": None,
            "fail_closed_on_full": True,
            "observed_available_bytes": available,
            "observed_filesystem_device": device,
            "observation_scope": "read_only_local_host",
            "operator_declared_target_bytes": declared,
            "recheck_required_before_materialization": True,
            "release_requirement_satisfied": declared >= required,
            "required_same_filesystem_bytes": required,
            "trusted_target_attestation": False,
        },
        "target": {
            "canonical_existing_base_verified": True,
            "direct_child_output_required": True,
            "evidence_output_directory": str(evidence_output),
            "evidence_output_external_to_runtime": True,
            "path_flavour": "windows" if os.name == "nt" else "posix",
            "persistent_base_root": str(base),
            "planned_output_absent": True,
            "planned_output_directory": str(planned),
            "target_mutation_performed": False,
        },
    }


def _recovery_plan(
    *,
    bindings: Mapping[str, object],
    target: Mapping[str, object],
    capacity: Mapping[str, object],
) -> dict[str, object]:
    release = _mapping(bindings.get("release"), "release_binding_invalid")
    materializer = _mapping(
        bindings.get("materializer"),
        "materializer_binding_invalid",
    )
    supervisor = _mapping(
        bindings.get("supervisor"),
        "supervisor_binding_invalid",
    )
    rollback = _mapping(bindings.get("rollback"), "rollback_binding_invalid")
    materializer_arguments = _mapping(
        release.get("materializer_arguments"),
        "materializer_arguments_binding_invalid",
    )
    return {
        "automatic_execution": False,
        "candidate": {
            "build_id": release.get("build_id"),
            "integration_version": release.get("integration_version"),
            "requirements_sha256": _mapping(
                release.get("requirements"),
                "requirements_binding_invalid",
            ).get("sha256"),
            "worker_version": release.get("worker_version"),
        },
        "canary_acceptance_required": [
            "supervisor_reports_exact_worker_version_and_started",
            "worker_status_online_without_startup_traceback",
            "queue_depth_bounded_and_failed_jobs_not_increasing",
            "worker_processes_healthy_with_bounded_cpu_and_nice",
            "home_assistant_and_zigbee2mqtt_latency_not_regressed",
            "history_and_databases_still_present",
        ],
        "contract": TARGET_CANARY_RECOVERY_PLAN_CONTRACT,
        "deployment_performed": False,
        "execution_authorized": False,
        "execution_mode": "operator_review_only",
        "forward_steps": [
            {"id": step, "performed": False} for step in FORWARD_STEP_IDS
        ],
        "materialization": {
            "manifest_verification_required": True,
            "materializer_arguments": {
                **dict(materializer_arguments),
                "output_directory": target.get("planned_output_directory"),
                "persistent_base_root": target.get("persistent_base_root"),
                "target_raw_capacity_bytes": capacity.get(
                    "operator_declared_target_bytes"
                ),
            },
            "materializer_contract": materializer.get("contract"),
            "materializer_member_path": materializer.get("member_path"),
            "materializer_sha256": materializer.get("sha256"),
            "selected_target_profile": SELECTED_TARGET_PROFILE,
            "target_materialization_performed": False,
        },
        "rollback": {
            "active_code_backup_required": True,
            "database_delete_forbidden": True,
            "integration": rollback.get("integration"),
            "restore_exact_live_supervisor_export_required": True,
            "worker": rollback.get("worker"),
        },
        "rollback_steps": [
            {"id": step, "performed": False} for step in ROLLBACK_STEP_IDS
        ],
        "stop_conditions": list(STOP_CONDITIONS),
        "supervisor_transition": {
            "candidate_runtime_mode": RAW_CAPTURE_RUNTIME_MODE,
            "credential_values_in_plan": False,
            "live_export_required_before_mutation": True,
            "manifest_derived_worker_first_pins_required": True,
            "pre_target_snapshot_sha256": supervisor.get("sha256"),
        },
    }


def _evidence_document(
    *,
    bindings: Mapping[str, object],
    observation: Mapping[str, object],
) -> dict[str, object]:
    release = _mapping(bindings.get("release"), "release_binding_invalid")
    target_profile = _mapping(
        bindings.get("target_profile"),
        "target_profile_binding_invalid",
    )
    materializer_arguments = _mapping(
        release.get("materializer_arguments"),
        "materializer_arguments_binding_invalid",
    )
    capacity = dict(_mapping(observation.get("capacity"), "capacity_invalid"))
    release_capacity = _mapping(
        bindings.get("capacity"),
        "release_capacity_invalid",
    )
    capacity["capture_window_hours"] = release_capacity.get(
        "capture_window_hours"
    )
    capacity["contract"] = release_capacity.get("contract")
    capacity["blitzortung_enabled"] = target_profile.get(
        "blitzortung_enabled"
    )
    capacity["event_capacity_claimed"] = target_profile.get(
        "event_capacity_claimed"
    )
    capacity["host_operational_free_reserve_bytes"] = target_profile.get(
        "host_operational_free_reserve_bytes"
    )
    capacity["selected_profile"] = target_profile.get("selected_profile")
    target = dict(_mapping(observation.get("target"), "target_invalid"))
    blockers_value = observation.get("blockers")
    if not isinstance(blockers_value, tuple) or any(
        not isinstance(item, str) for item in blockers_value
    ):
        raise TargetCanaryPreflightError("capacity_blockers_invalid")
    blockers = list(blockers_value)
    plan = _recovery_plan(
        bindings=bindings,
        target=target,
        capacity=capacity,
    )
    unsigned: dict[str, object] = {
        "capacity": capacity,
        "claims": {
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
        },
        "contract": TARGET_CANARY_PREFLIGHT_CONTRACT,
        "external_execution_gates": list(EXTERNAL_EXECUTION_GATES),
        "local_preflight": {
            "blockers": blockers,
            "capacity_checked": True,
            "materializer_pins_verified": True,
            "passed": not blockers,
            "quartet_verified": True,
            "recovery_plan_complete": True,
            "rollback_packages_verified": True,
            "supervisor_release_snapshot_verified": True,
        },
        "materializer": bindings.get("materializer"),
        "materializer_arguments": dict(materializer_arguments),
        "recovery_plan": plan,
        "recovery_plan_sha256": _sha_bytes(_canonical(plan)),
        "release": dict(release),
        "rollback": bindings.get("rollback"),
        "supervisor_options": bindings.get("supervisor"),
        "target": target,
        "target_profile": dict(target_profile),
    }
    return {
        **unsigned,
        "preflight_evidence_sha256": _sha_bytes(_canonical(unsigned)),
    }


def _validate_evidence_document(document: Mapping[str, object]) -> dict[str, object]:
    expected_keys = {
        "capacity",
        "claims",
        "contract",
        "external_execution_gates",
        "local_preflight",
        "materializer",
        "materializer_arguments",
        "preflight_evidence_sha256",
        "recovery_plan",
        "recovery_plan_sha256",
        "release",
        "rollback",
        "supervisor_options",
        "target",
        "target_profile",
    }
    if set(document) != expected_keys:
        raise TargetCanaryPreflightError("preflight_evidence_schema_invalid")
    unsigned = dict(document)
    supplied_sha = unsigned.pop("preflight_evidence_sha256", None)
    if (
        document.get("contract") != TARGET_CANARY_PREFLIGHT_CONTRACT
        or supplied_sha != _sha_bytes(_canonical(unsigned))
        or document.get("external_execution_gates")
        != list(EXTERNAL_EXECUTION_GATES)
    ):
        raise TargetCanaryPreflightError("preflight_evidence_identity_invalid")
    claims = _mapping(document.get("claims"), "preflight_claims_invalid")
    expected_claims = {
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
    if dict(claims) != expected_claims:
        raise TargetCanaryPreflightError("preflight_claims_invalid")
    capacity = _mapping(document.get("capacity"), "preflight_capacity_invalid")
    declared = _positive_integer(
        capacity.get("operator_declared_target_bytes"),
        "preflight_capacity_invalid",
    )
    required = _positive_integer(
        capacity.get("required_same_filesystem_bytes"),
        "preflight_capacity_invalid",
    )
    observed = capacity.get("observed_available_bytes")
    profile = _mapping(
        document.get("target_profile"),
        "preflight_target_profile_invalid",
    )
    if dict(profile) != _required_target_profile():
        raise TargetCanaryPreflightError(
            "preflight_target_profile_invalid"
        )
    materializer_arguments = _mapping(
        document.get("materializer_arguments"),
        "preflight_materializer_arguments_invalid",
    )
    expected_materializer_argument_keys = {
        "blitzortung_enabled",
        "capture_window_hours",
        "expected_activation_matrix_sha256",
        "expected_catalog_sha256",
        "operator_host_operational_free_reserve_bytes",
    }
    if (
        type(observed) is not int
        or observed < 0
        or set(materializer_arguments) != expected_materializer_argument_keys
        or materializer_arguments.get("blitzortung_enabled") is not False
        or materializer_arguments.get("capture_window_hours")
        != capacity.get("capture_window_hours")
        or materializer_arguments.get(
            "operator_host_operational_free_reserve_bytes"
        )
        != REQUIRED_HOST_OPERATIONAL_FREE_RESERVE_BYTES
        or capacity.get("blitzortung_enabled") is not False
        or capacity.get("event_capacity_claimed") is not False
        or capacity.get("host_operational_free_reserve_bytes")
        != REQUIRED_HOST_OPERATIONAL_FREE_RESERVE_BYTES
        or capacity.get("selected_profile") != SELECTED_TARGET_PROFILE
    ):
        raise TargetCanaryPreflightError("preflight_capacity_invalid")
    _checked_sha256(
        materializer_arguments.get("expected_activation_matrix_sha256"),
        "preflight_materializer_arguments_invalid",
    )
    _checked_sha256(
        materializer_arguments.get("expected_catalog_sha256"),
        "preflight_materializer_arguments_invalid",
    )
    expected_blockers: list[str] = []
    if declared < required:
        expected_blockers.append(
            "operator_declared_capacity_below_release_minimum"
        )
    if observed < declared:
        expected_blockers.append(
            "observed_available_capacity_below_declaration"
        )
    expected_blockers.sort()
    local = _mapping(document.get("local_preflight"), "local_preflight_invalid")
    if (
        local.get("blockers") != expected_blockers
        or local.get("passed") is not (not expected_blockers)
        or set(local) != {
            "blockers",
            "capacity_checked",
            "materializer_pins_verified",
            "passed",
            "quartet_verified",
            "recovery_plan_complete",
            "rollback_packages_verified",
            "supervisor_release_snapshot_verified",
        }
        or any(
            local.get(key) is not True
            for key in set(local) - {"blockers", "passed"}
        )
    ):
        raise TargetCanaryPreflightError("local_preflight_invalid")
    plan = _mapping(document.get("recovery_plan"), "recovery_plan_invalid")
    forward = plan.get("forward_steps")
    rollback_steps = plan.get("rollback_steps")
    if (
        not isinstance(forward, list)
        or not isinstance(rollback_steps, list)
        or any(not isinstance(row, Mapping) for row in forward)
        or any(not isinstance(row, Mapping) for row in rollback_steps)
    ):
        raise TargetCanaryPreflightError("recovery_plan_invalid")
    rows = [*forward, *rollback_steps]
    if (
        document.get("recovery_plan_sha256") != _sha_bytes(_canonical(plan))
        or plan.get("contract") != TARGET_CANARY_RECOVERY_PLAN_CONTRACT
        or plan.get("automatic_execution") is not False
        or plan.get("execution_authorized") is not False
        or plan.get("deployment_performed") is not False
        or [row.get("id") for row in forward]
        != list(FORWARD_STEP_IDS)
        or [row.get("id") for row in rollback_steps]
        != list(ROLLBACK_STEP_IDS)
        or any(
            row != {"id": row.get("id"), "performed": False}
            for row in rows
        )
        or plan.get("stop_conditions") != list(STOP_CONDITIONS)
    ):
        raise TargetCanaryPreflightError("recovery_plan_invalid")
    release = _mapping(document.get("release"), "preflight_release_invalid")
    integration_version = _checked_version(
        release.get("integration_version"),
        "preflight_release_invalid",
    )
    worker_version = _checked_version(
        release.get("worker_version"),
        "preflight_release_invalid",
    )
    release_arguments = _mapping(
        release.get("materializer_arguments"),
        "preflight_release_invalid",
    )
    release_requirements = _mapping(
        release.get("requirements"),
        "preflight_release_invalid",
    )
    if (
        release.get("runtime_mode") != RAW_CAPTURE_RUNTIME_MODE
        or release.get("build_id")
        != f"integration-{integration_version}-worker-{worker_version}"
        or dict(release_arguments) != dict(materializer_arguments)
        or release.get("selected_target_profile")
        != SELECTED_TARGET_PROFILE
        or release_requirements.get("contract")
        != RAW_RELEASE_REQUIREMENTS_CONTRACT
    ):
        raise TargetCanaryPreflightError("preflight_release_invalid")
    supervisor = _mapping(
        document.get("supervisor_options"),
        "preflight_supervisor_options_invalid",
    )
    if (
        supervisor.get("runtime_mode") != RAW_CAPTURE_RUNTIME_MODE
        or supervisor.get("exact_release_snapshot_verified") is not True
        or supervisor.get("live_supervisor_export_verified") is not False
        or supervisor.get("worker_first_pins_empty") is not True
        or supervisor.get("retention_authorities_default_closed") is not True
        or not isinstance(supervisor.get("present_secret_keys"), list)
    ):
        raise TargetCanaryPreflightError(
            "preflight_supervisor_options_invalid"
        )
    target = _mapping(document.get("target"), "preflight_target_invalid")
    if (
        target.get("target_mutation_performed") is not False
        or target.get("planned_output_absent") is not True
        or target.get("evidence_output_external_to_runtime") is not True
    ):
        raise TargetCanaryPreflightError("preflight_target_invalid")
    materializer = _mapping(
        document.get("materializer"),
        "preflight_materializer_invalid",
    )
    plan_materialization = _mapping(
        plan.get("materialization"),
        "recovery_plan_invalid",
    )
    expected_target_arguments = {
        **dict(materializer_arguments),
        "output_directory": target.get("planned_output_directory"),
        "persistent_base_root": target.get("persistent_base_root"),
        "target_raw_capacity_bytes": declared,
    }
    if dict(plan_materialization) != {
        "manifest_verification_required": True,
        "materializer_arguments": expected_target_arguments,
        "materializer_contract": materializer.get("contract"),
        "materializer_member_path": materializer.get("member_path"),
        "materializer_sha256": materializer.get("sha256"),
        "selected_target_profile": SELECTED_TARGET_PROFILE,
        "target_materialization_performed": False,
    }:
        raise TargetCanaryPreflightError("recovery_plan_invalid")
    _checked_sha256(
        document.get("preflight_evidence_sha256"),
        "preflight_evidence_identity_invalid",
    )
    _checked_sha256(
        document.get("recovery_plan_sha256"),
        "recovery_plan_invalid",
    )
    return dict(document)


def verify_raw_capture_target_canary_preflight_evidence(
    evidence_path: Path,
) -> dict[str, object]:
    """Verify one canonical diagnostic document without trusting its host."""

    path = Path(evidence_path)
    if path.name != TARGET_CANARY_PREFLIGHT_FILENAME:
        raise TargetCanaryPreflightError("preflight_evidence_filename_invalid")
    return _validate_evidence_document(_strict_document(path))


def _publish_evidence(
    *,
    output_directory: Path,
    document: Mapping[str, object],
) -> tuple[Path, str]:
    output = Path(os.path.abspath(Path(output_directory)))
    parent = _canonical_existing_directory(
        output.parent,
        code="evidence_output_parent_invalid",
    )
    output = _canonical_absent_direct_child(
        output,
        parent=parent,
        code="evidence_output_directory_invalid",
    )
    body = _canonical(document) + b"\n"
    run: ReleaseInstallOutputAuthority | None = None
    allowed: DirectoryAuthority | None = None
    committed = False
    publication_sha256: str | None = None
    primary_error: BaseException | None = None
    try:
        allowed = DirectoryAuthority.open_absolute(parent)
        run = ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            RelativePath.parse(output.name),
            mode=0o700,
        )
        publication = run.write_bytes_noreplace(
            RelativePath.parse(TARGET_CANARY_PREFLIGHT_FILENAME),
            body,
            mode=0o600,
            forbidden_markers=PRIVATE_KEY_MARKERS,
        )
        rebound = run.read(
            RelativePath.parse(TARGET_CANARY_PREFLIGHT_FILENAME),
            maximum_bytes=MAX_PREFLIGHT_EVIDENCE_BYTES,
        )
        if (
            rebound != body
            or publication.stream.sha256 != _sha_bytes(body)
            or publication.stream.size != len(body)
        ):
            raise TargetCanaryPreflightError(
                "preflight_evidence_publication_reproof_failed"
            )
        run.reprove_run_root()
        publication_sha256 = publication.stream.sha256
        # This is the only publication commit boundary.  Descriptor close
        # failures after this line cannot turn a valid immutable final into an
        # ambiguous error that invites an unsafe retry.
        committed = True
    except BaseException as error:
        primary_error = error

    cleanup_error: BaseException | None = None
    if not committed and run is not None and not run.closed:
        try:
            run.remove_created_run_root(maximum_entries=1)
        except BaseException as error:
            cleanup_error = error

    close_errors: list[BaseException] = []
    if run is not None and not run.closed:
        try:
            run.close()
        except BaseException as error:
            close_errors.append(error)
    if allowed is not None and not allowed.closed:
        try:
            allowed.close()
        except BaseException as error:
            close_errors.append(error)

    if committed:
        if publication_sha256 is None:
            raise TargetCanaryPreflightError(
                "preflight_evidence_publication_outcome_invalid"
            )
        # The final identity and bytes were already descriptor-bound.  Close
        # errors are intentionally non-fatal after that commit boundary.
        return (
            output / TARGET_CANARY_PREFLIGHT_FILENAME,
            publication_sha256,
        )
    if cleanup_error is not None:
        failures = ([primary_error] if primary_error is not None else [])
        failures.extend([cleanup_error, *close_errors])
        raise TargetCanaryPreflightError(
            "preflight_evidence_publication_cleanup_failed"
        ) from BaseExceptionGroup(
            "preflight publication and cleanup failures",
            failures,
        )
    if close_errors:
        failures = ([primary_error] if primary_error is not None else [])
        failures.extend(close_errors)
        raise TargetCanaryPreflightError(
            "preflight_evidence_publication_close_failed"
        ) from BaseExceptionGroup(
            "preflight publication and close failures",
            failures,
        )
    if isinstance(primary_error, TargetCanaryPreflightError):
        raise primary_error
    raise TargetCanaryPreflightError(
        "preflight_evidence_publication_failed"
    ) from primary_error


def build_raw_capture_target_canary_preflight(
    *,
    requirements_path: Path,
    sanitized_options_snapshot: Path,
    materializer_path: Path,
    rollback_integration_archive: Path,
    rollback_worker_archive: Path,
    persistent_base_root: Path,
    planned_output_directory: Path,
    operator_declared_target_bytes: int,
    evidence_output_directory: Path,
    rollback_policy: LegacyRollbackPolicy = RECOVERY_BASELINE_POLICY,
) -> TargetCanaryPreflightResult:
    """Build a read-only target preflight; never materialize or deploy."""

    first = _release_bindings(
        requirements_path=requirements_path,
        sanitized_options_snapshot=sanitized_options_snapshot,
        materializer_path=materializer_path,
        rollback_integration_archive=rollback_integration_archive,
        rollback_worker_archive=rollback_worker_archive,
        rollback_policy=rollback_policy,
    )
    capacity_pin = _mapping(first.get("capacity"), "release_capacity_invalid")
    required = _positive_integer(
        capacity_pin.get("minimum_required_same_filesystem_bytes"),
        "release_capacity_invalid",
    )
    first_observation = _target_observation(
        persistent_base_root=persistent_base_root,
        planned_output_directory=planned_output_directory,
        evidence_output_directory=evidence_output_directory,
        operator_declared_target_bytes=operator_declared_target_bytes,
        required_same_filesystem_bytes=required,
    )
    second = _release_bindings(
        requirements_path=requirements_path,
        sanitized_options_snapshot=sanitized_options_snapshot,
        materializer_path=materializer_path,
        rollback_integration_archive=rollback_integration_archive,
        rollback_worker_archive=rollback_worker_archive,
        rollback_policy=rollback_policy,
    )
    second_observation = _target_observation(
        persistent_base_root=persistent_base_root,
        planned_output_directory=planned_output_directory,
        evidence_output_directory=evidence_output_directory,
        operator_declared_target_bytes=operator_declared_target_bytes,
        required_same_filesystem_bytes=required,
    )
    if first != second:
        raise TargetCanaryPreflightError("preflight_source_bindings_changed")
    first_target = _mapping(first_observation.get("target"), "target_invalid")
    second_target = _mapping(second_observation.get("target"), "target_invalid")
    first_capacity = _mapping(
        first_observation.get("capacity"),
        "capacity_invalid",
    )
    second_capacity = dict(_mapping(
        second_observation.get("capacity"),
        "capacity_invalid",
    ))
    if (
        first_target != second_target
        or first_capacity.get("observed_filesystem_device")
        != second_capacity.get("observed_filesystem_device")
    ):
        raise TargetCanaryPreflightError("preflight_target_layout_changed")
    conservative_available = min(
        int(first_capacity["observed_available_bytes"]),
        int(second_capacity["observed_available_bytes"]),
    )
    second_capacity["observed_available_bytes"] = conservative_available
    second_capacity["availability_satisfied"] = (
        conservative_available >= operator_declared_target_bytes
    )
    second_capacity["release_requirement_satisfied"] = (
        operator_declared_target_bytes >= required
    )
    blockers: list[str] = []
    if operator_declared_target_bytes < required:
        blockers.append("operator_declared_capacity_below_release_minimum")
    if conservative_available < operator_declared_target_bytes:
        blockers.append("observed_available_capacity_below_declaration")
    observation = {
        "blockers": tuple(sorted(blockers)),
        "capacity": second_capacity,
        "target": dict(second_target),
    }
    # Final read-only reproof happens before publication.  Once the immutable
    # evidence final is committed there is no later failure path that leaves a
    # valid evidence directory while returning an ambiguous error.
    final_bindings = _release_bindings(
        requirements_path=requirements_path,
        sanitized_options_snapshot=sanitized_options_snapshot,
        materializer_path=materializer_path,
        rollback_integration_archive=rollback_integration_archive,
        rollback_worker_archive=rollback_worker_archive,
        rollback_policy=rollback_policy,
    )
    final_base = _canonical_existing_directory(
        Path(persistent_base_root),
        code="persistent_base_root_invalid",
    )
    _canonical_absent_direct_child(
        Path(planned_output_directory),
        parent=final_base,
        code="planned_output_directory_changed",
    )
    final_free = int(shutil.disk_usage(final_base).free)
    if (
        final_bindings != second
        or (not blockers and final_free < operator_declared_target_bytes)
    ):
        raise TargetCanaryPreflightError(
            "preflight_commit_boundary_reproof_failed"
        )
    document = _evidence_document(bindings=second, observation=observation)
    _validate_evidence_document(document)
    evidence_path, evidence_file_sha256 = _publish_evidence(
        output_directory=evidence_output_directory,
        document=document,
    )
    verified = _validate_evidence_document(document)
    local = _mapping(verified.get("local_preflight"), "local_preflight_invalid")
    blocker_rows = local.get("blockers")
    assert isinstance(blocker_rows, list)
    return TargetCanaryPreflightResult(
        output_directory=evidence_path.parent,
        evidence_path=evidence_path,
        evidence_sha256=evidence_file_sha256,
        local_preflight_passed=local.get("passed") is True,
        local_preflight_blockers=tuple(str(value) for value in blocker_rows),
    )


__all__ = [
    "EXTERNAL_EXECUTION_GATES",
    "TARGET_CANARY_PREFLIGHT_CONTRACT",
    "TARGET_CANARY_PREFLIGHT_FILENAME",
    "TARGET_CANARY_RECOVERY_PLAN_CONTRACT",
    "TargetCanaryPreflightError",
    "TargetCanaryPreflightResult",
    "build_raw_capture_target_canary_preflight",
    "verify_raw_capture_target_canary_preflight_evidence",
]
