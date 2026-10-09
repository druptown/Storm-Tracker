"""Build one pinned raw-capture-shadow activation root; never deploy it.

The builder consumes the current closed acquisition catalogue and recovery
bindings.  It creates a brand-new, absolute persistent root containing the
typed raw service configuration, the immutable worker-first authority, a
capacity derivation and a SHA-256 manifest.  Provider credentials are neither
accepted nor read.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
from typing import Any, Final, Mapping, Sequence

from storm_tracker_worker.acquisition_activation import (  # noqa: E402
    ACTIVATION_RULES,
    activation_matrix_snapshot,
)
from storm_tracker_worker.acquisition_capture_recovery import (  # noqa: E402
    RECOVERY_BINDINGS,
    recovery_binding_sha256,
    validate_closed_recovery_bindings,
)
from storm_tracker_worker.blitzortung_event_ingress import (  # noqa: E402
    BlitzortungEventIngressLimits,
)
from storm_tracker_worker.blitzortung_raw_ingress import (  # noqa: E402
    MAX_PAYLOAD_BYTES as BLITZORTUNG_MAX_PAYLOAD_BYTES,
)
from storm_tracker_worker.dwd_rv_completion_process_isolation import (  # noqa: E402
    DwdRvCompletionIsolationLimits,
)
from storm_tracker_worker.dwd_rv_observation_replay import (  # noqa: E402
    default_belgium_observation_profile,
)
from storm_tracker_worker.opera_native import (  # noqa: E402
    MAX_OPERA_HDF5_BYTES,
    OPERA_TRANSPORT_FREE_MARGIN_BYTES,
    OPERA_TRANSPORT_SPOOL_MAX_ENTRIES,
)
from storm_tracker_worker.raw_artifacts import RawArtifactLimits  # noqa: E402
from storm_tracker_worker.raw_capacity_ledger import (  # noqa: E402
    ProviderCapacityLimit,
    RawCapacityProfile,
)
from storm_tracker_worker.raw_capture_retention_guard import (  # noqa: E402
    CAPACITY_POLICY as RAW_CAPTURE_RETENTION_CAPACITY_POLICY,
    CONTRACT as RAW_CAPTURE_RETENTION_GUARD_CONTRACT,
)
from storm_tracker_worker.raw_capture_service_composition import (  # noqa: E402
    BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES,
    BLITZORTUNG_CONTROL_STAGING_RECOVERY_EXTRA_ENTRIES,
    BLITZORTUNG_EVENT_MAX_CONTROL_RECEIPTS,
    BLITZORTUNG_EVENT_MAX_QUEUE_ENTRIES,
    BLITZORTUNG_EVENT_MAX_RETAINED_ENTRIES,
    BLITZORTUNG_EVENT_MAX_SESSIONS,
    CONFIG_CONTRACT as RAW_CAPTURE_CONFIG_CONTRACT,
    RAW_CAPTURE_ALLOWED_WORKLOAD_IDS,
    RUNTIME_CONTRACT as RAW_CAPTURE_RUNTIME_CONTRACT,
    RawCaptureServiceConfig,
)
from storm_tracker_worker.worker_acquisition_plan import (  # noqa: E402
    acquisition_catalog_snapshot,
)
from storm_tracker_worker.worker_first_composition import (  # noqa: E402
    BOOTSTRAP_RECEIPT_CONTRACT,
    WorkerFirstCompositionConfig,
    WorkerFirstCompositionPaths,
    initialize_worker_first_composition_authority,
)
from storm_tracker_worker.worker_first_ownership import (  # noqa: E402
    LOCK_ANCHOR_SHA256,
    REQUIRED_STORE_ROLES,
    load_worker_first_authority,
)
from storm_tracker_worker.worker_first_service_activation import (  # noqa: E402
    AUTHORITY_SHA256_ENV,
    CONFIG_PATH_ENV,
    CONFIG_SHA256_ENV,
    RAW_CAPTURE_SHADOW_MODE,
    RUNTIME_MODE_ENV,
    load_worker_first_service_activation,
)
from storm_tracker_worker.worker_first_service_composition import (  # noqa: E402
    WorkerFirstServiceSourcePaths,
)


CAPACITY_CONTRACT: Final = "stv3-worker-raw-capture-capacity-artifact-v2"
MANIFEST_CONTRACT: Final = (
    "stv3-worker-raw-capture-shadow-artifact-manifest-v2"
)
MATERIALIZER_CONTRACT: Final = (
    "stv3-worker-raw-capture-activation-materializer-v2"
)
EXPECTED_RAW_CAPTURE_CONFIG_CONTRACT: Final = (
    "stv3-worker-raw-capture-service-config-v5"
)
EXPECTED_RAW_CAPTURE_RUNTIME_CONTRACT: Final = (
    "stv3-worker-raw-capture-runtime-v5"
)
EXPECTED_RAW_CAPTURE_COMPOSITION_SHA256: Final = (
    "df65a977ddf45b5975b5c04815eea8d586264362c9604d66735adb275a1d5b2f"
)
EXPECTED_WORKER_SERVICE_SHA256: Final = (
    "86ac3d73291aa5d951217bdd4e1a9c6c4090add92626dac6b0606c467f44fd74"
)
EXPECTED_RAW_CAPTURE_RETENTION_GUARD_SHA256: Final = (
    "7b30859e780b66de0547d1fcceb5fe9d01fc29d93abed5b21ec42b219e176b69"
)
EXPECTED_RAW_CAPTURE_RETENTION_GUARD_CONTRACT: Final = (
    "stv3-worker-raw-capture-retention-guard-v2"
)
EXPECTED_RAW_CAPTURE_RETENTION_CAPACITY_POLICY: Final = (
    "config_bound_optional_event_envelope_reserved_fail_closed_v2"
)
EXPECTED_RECOVERY_BINDING_COUNT: Final = 27
EXPECTED_RECOVERY_INVENTORY_SHA256: Final = (
    "5b38cbc6d9ee66b6528987ac633b6ed15e48cad66deaf214a962ba278bfe2b1e"
)
EXPECTED_OPERA_RECOVERY_SHA256: Final = (
    "f1b6216c0a78311c21d2ef2104d91a0be08fe514b143d39a835491ab2ecbbf61"
)
EXPECTED_OPERA_MAX_BYTES: Final = 384 * 1024 * 1024
EXPECTED_OPERA_SPOOL_MARGIN_BYTES: Final = 8 * 1024 * 1024
EXPECTED_OPERA_SPOOL_MAX_ENTRIES: Final = 2
EXPECTED_OPERA_CADENCE_SECONDS: Final = 300
EXPECTED_OPERA_MAX_ARTIFACTS: Final = 1
RAW_STORE_CATALOG_RESERVE_BYTES: Final = 1024 * 1024 * 1024
EVENT_RAW_STORE_CATALOG_RESERVE_BYTES: Final = 1024 * 1024 * 1024
CAPACITY_LEDGER_CATALOG_RESERVE_BYTES: Final = 256 * 1024 * 1024
CONTROL_DB_WAL_METADATA_RESERVE_BYTES: Final = 128 * 1024 * 1024
MINIMUM_HOST_OPERATIONAL_FREE_RESERVE_BYTES: Final = 2 * 1024**3
EXPECTED_BLITZORTUNG_MAX_QUEUE_ENTRIES: Final = 128
EXPECTED_BLITZORTUNG_MAX_CONTROL_RECEIPTS: Final = 100_000
EXPECTED_BLITZORTUNG_MAX_RETAINED_PAYLOAD_ENTRIES: Final = 50_000
EXPECTED_BLITZORTUNG_MAX_RETAINED_PAYLOAD_BYTES: Final = 4 * 1024**3
EXPECTED_BLITZORTUNG_MAX_SESSIONS: Final = 10_000
EXPECTED_BLITZORTUNG_CONTROL_RECEIPT_RESERVE: Final = 8
EXPECTED_BLITZORTUNG_CLAIM_LEASE_SECONDS: Final = 30.0
EXPECTED_BLITZORTUNG_LOCK_TIMEOUT_SECONDS: Final = 0.25
EXPECTED_BLITZORTUNG_MAX_PAYLOAD_BYTES: Final = 256 * 1024
EXPECTED_BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES: Final = 128 * 1024
EXPECTED_BLITZORTUNG_STAGING_RECOVERY_EXTRA_ENTRIES: Final = 2
EXPECTED_BLITZORTUNG_HARD_MAX_QUEUE_ENTRIES: Final = 4_096
EXPECTED_BLITZORTUNG_HARD_MAX_CONTROL_RECEIPTS: Final = 1_000_000
EXPECTED_BLITZORTUNG_HARD_MAX_RETAINED_ENTRIES: Final = 1_000_000
EXPECTED_BLITZORTUNG_HARD_MAX_SESSIONS: Final = 100_000
MIN_CAPTURE_WINDOW_HOURS: Final = 1
MAX_CAPTURE_WINDOW_HOURS: Final = 168
MAX_SIGNED_INT64: Final = (1 << 63) - 1
MAX_ARTIFACT_DOCUMENT_BYTES: Final = 4 * 1024 * 1024
CONFIG_FILENAME: Final = "raw-capture-shadow-config.json"
CAPACITY_FILENAME: Final = "raw-capture-capacity-profile.json"
MANIFEST_FILENAME: Final = "raw-capture-shadow-sha256-manifest.json"
OPERA_WORKLOAD_ID: Final = "radar.opera.dbzh"
BLITZORTUNG_INGRESS_ID: Final = "lightning.blitzortung.event_ingress"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SECRET_KEY_RE = re.compile(
    r"(?:^|_)(?:api_?key|access_?token|refresh_?token|password|passwd|"
    r"credential|private_?key|secret)(?:$|_)",
    re.IGNORECASE,
)


class RawCaptureArtifactBuildError(RuntimeError):
    """A pinned artifact root could not be proven or created safely."""


@dataclass(frozen=True, slots=True, repr=False)
class RawCaptureCapacityPlan:
    capacity_profile: RawCapacityProfile
    raw_store_limits: RawArtifactLimits
    blitzortung_ingress_limits: BlitzortungEventIngressLimits
    blitzortung_raw_store_limits: RawArtifactLimits
    blitzortung_enabled: bool
    host_operational_free_reserve_bytes: int
    _document: Mapping[str, Any]

    @property
    def configured_raw_quota(self) -> int:
        value = self._document["same_filesystem_capacity"]
        assert isinstance(value, Mapping)
        return int(value["configured_raw_quota"])

    @property
    def required_same_filesystem_bytes(self) -> int:
        value = self._document["same_filesystem_capacity"]
        assert isinstance(value, Mapping)
        return int(value["required_same_filesystem_bytes"])

    def to_mapping(self) -> dict[str, Any]:
        return _canonical_copy(self._document)


@dataclass(frozen=True, slots=True, repr=False)
class RawCaptureArtifactBuild:
    persistent_base_root: Path
    output_directory: Path
    config_path: Path
    config_sha256: str
    capacity_path: Path
    capacity_sha256: str
    authority_path: Path
    authority_sha256: str
    manifest_path: Path
    manifest_sha256: str
    activation_environment: Mapping[str, str]


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (
        ArithmeticError,
        RecursionError,
        TypeError,
        UnicodeError,
        ValueError,
    ) as error:
        raise RawCaptureArtifactBuildError("artifact_json_not_canonical") from error


def _canonical_copy(value: object) -> Any:
    return json.loads(_canonical(value).decode("ascii"))


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _source_sha256(filename: str) -> str:
    path = Path(__file__).with_name(filename)
    try:
        metadata = path.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or _is_reparse(metadata)
            or not stat.S_ISREG(metadata.st_mode)
        ):
            raise RawCaptureArtifactBuildError("pinned_source_file_invalid")
        body = path.read_bytes()
    except RawCaptureArtifactBuildError:
        raise
    except OSError as error:
        raise RawCaptureArtifactBuildError(
            "pinned_source_file_unavailable"
        ) from error
    if not body or len(body) != metadata.st_size:
        raise RawCaptureArtifactBuildError("pinned_source_file_changed")
    return hashlib.sha256(body).hexdigest()


def _sha256(value: object, code: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise RawCaptureArtifactBuildError(code)
    return value


def _positive_i64(value: object, code: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RawCaptureArtifactBuildError(code)
    if value > MAX_SIGNED_INT64:
        raise RawCaptureArtifactBuildError("capacity_integer_overflow")
    return value


def _checked_add(left: int, right: int) -> int:
    if left < 0 or right < 0 or left > MAX_SIGNED_INT64 - right:
        raise RawCaptureArtifactBuildError("capacity_integer_overflow")
    return left + right


def _checked_multiply(left: int, right: int) -> int:
    if left < 0 or right < 0 or (left and right > MAX_SIGNED_INT64 // left):
        raise RawCaptureArtifactBuildError("capacity_integer_overflow")
    return left * right


def _sum_i64(values: Sequence[int]) -> int:
    total = 0
    for value in values:
        total = _checked_add(total, value)
    return total


def _is_reparse(metadata: os.stat_result) -> bool:
    flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(flag and int(getattr(metadata, "st_file_attributes", 0)) & flag)


def _path_identity(path: Path) -> str:
    return os.path.normcase(str(path))


def _validate_existing_base(path: Path) -> Path:
    supplied = Path(path)
    if not supplied.is_absolute() or supplied == Path(supplied.anchor):
        raise RawCaptureArtifactBuildError("persistent_base_root_invalid")
    try:
        resolved = supplied.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise RawCaptureArtifactBuildError(
            "persistent_base_root_unavailable"
        ) from error
    if _path_identity(supplied) != _path_identity(resolved):
        raise RawCaptureArtifactBuildError("persistent_base_root_not_canonical")
    try:
        for component in (supplied, *supplied.parents):
            metadata = component.lstat()
            if stat.S_ISLNK(metadata.st_mode) or _is_reparse(metadata):
                raise RawCaptureArtifactBuildError(
                    "persistent_base_root_unsafe"
                )
        metadata = supplied.lstat()
    except RawCaptureArtifactBuildError:
        raise
    except OSError as error:
        raise RawCaptureArtifactBuildError(
            "persistent_base_root_unavailable"
        ) from error
    if not stat.S_ISDIR(metadata.st_mode):
        raise RawCaptureArtifactBuildError("persistent_base_root_not_directory")
    return resolved


def _validate_absent_output(path: Path, base: Path) -> Path:
    supplied = Path(path)
    if not supplied.is_absolute() or supplied == Path(supplied.anchor):
        raise RawCaptureArtifactBuildError("output_directory_invalid")
    try:
        resolved = supplied.resolve(strict=False)
    except (OSError, RuntimeError) as error:
        raise RawCaptureArtifactBuildError("output_directory_invalid") from error
    if _path_identity(supplied) != _path_identity(resolved):
        raise RawCaptureArtifactBuildError("output_directory_not_canonical")
    if resolved.parent != base:
        raise RawCaptureArtifactBuildError(
            "output_directory_not_direct_persistent_child"
        )
    if os.path.lexists(resolved):
        raise RawCaptureArtifactBuildError("output_directory_already_exists")
    return resolved


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _assert_path_set(paths: Mapping[str, Path], output: Path) -> None:
    ordered = [(name, Path(path)) for name, path in sorted(paths.items())]
    for _name, path in ordered:
        if not path.is_absolute() or path == Path(path.anchor):
            raise RawCaptureArtifactBuildError("generated_path_invalid")
        try:
            resolved = path.resolve(strict=False)
            resolved.relative_to(output)
        except (OSError, RuntimeError, ValueError) as error:
            raise RawCaptureArtifactBuildError("generated_path_escapes_root") from error
        if _path_identity(path) != _path_identity(resolved):
            raise RawCaptureArtifactBuildError("generated_path_not_canonical")
    if any(
        _paths_overlap(left, right)
        for index, (_left_name, left) in enumerate(ordered)
        for _right_name, right in ordered[index + 1 :]
    ):
        raise RawCaptureArtifactBuildError("generated_paths_overlap")


def _layout(
    output: Path,
) -> tuple[
    WorkerFirstCompositionPaths,
    WorkerFirstServiceSourcePaths,
    Path,
    Path,
    dict[str, Path],
]:
    control = output / "state" / "control"
    pipeline = output / "state" / "pipeline"
    source = output / "state" / "source"
    raw = output / "state" / "raw"
    event_control_root = (
        output / "state" / "event-ingress" / "blitzortung-control"
    )
    event_raw_store_root = raw / "blitzortung-events"
    worker_paths = WorkerFirstCompositionPaths.from_mapping({
        "acquisition_completion_root": str(
            pipeline / "acquisition-completion"
        ),
        "authority_file": str(control / "authority.json"),
        "desired_state_db": str(control / "desired-state.sqlite3"),
        "dwd_canonical_evidence_db": str(
            pipeline / "dwd-canonical-evidence.sqlite3"
        ),
        "dwd_derived_artifact_root": str(
            pipeline / "dwd-derived-artifacts"
        ),
        "dwd_handoff_raw_root": str(pipeline / "dwd-handoff-raw"),
        "operational_fact_db": str(control / "operational-facts.sqlite3"),
        "operational_snapshot_db": str(
            control / "operational-snapshots.sqlite3"
        ),
        "ownership_lock_file": str(control / "ownership.lock"),
        "pipeline_output_db": str(pipeline / "pipeline-output.sqlite3"),
        "snapshot_outbox_db": str(control / "snapshot-outbox.sqlite3"),
    })
    source_paths = WorkerFirstServiceSourcePaths(
        scheduler_db=source / "scheduler.sqlite3",
        capacity_ledger_db=source / "capacity-ledger.sqlite3",
        execution_receipts_db=source / "execution-receipts.sqlite3",
        raw_store_root=raw / "cadence-artifacts",
    )
    combined = {
        **{
            f"worker_first.{name}": Path(value)
            for name, value in worker_paths.to_mapping().items()
        },
        **{
            f"source.{name}": Path(value)
            for name, value in source_paths.to_mapping().items()
        },
        "event.blitzortung_control_root": event_control_root,
        "event.blitzortung_raw_store_root": event_raw_store_root,
    }
    _assert_path_set(combined, output)
    return (
        worker_paths,
        source_paths,
        event_control_root,
        event_raw_store_root,
        combined,
    )


def _binding_maximum_member_bytes(binding: object) -> int:
    members = getattr(binding, "members", ())
    values = [int(member.maximum_transport_bytes) for member in members]
    dynamic = getattr(binding, "dynamic_member", None)
    if dynamic is not None:
        values.append(int(dynamic.maximum_transport_bytes))
    if not values or any(value <= 0 for value in values):
        raise RawCaptureArtifactBuildError("recovery_member_capacity_invalid")
    return max(values)


def _closed_capacity_inputs(
    *,
    expected_catalog_sha256: str,
    expected_activation_matrix_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any], list[object], dict[str, object]]:
    expected_catalog = _sha256(
        expected_catalog_sha256,
        "expected_catalog_sha256_invalid",
    )
    expected_matrix = _sha256(
        expected_activation_matrix_sha256,
        "expected_activation_matrix_sha256_invalid",
    )
    if (
        RAW_CAPTURE_CONFIG_CONTRACT != EXPECTED_RAW_CAPTURE_CONFIG_CONTRACT
        or RAW_CAPTURE_RUNTIME_CONTRACT != EXPECTED_RAW_CAPTURE_RUNTIME_CONTRACT
        or RAW_CAPTURE_RETENTION_GUARD_CONTRACT
        != EXPECTED_RAW_CAPTURE_RETENTION_GUARD_CONTRACT
        or RAW_CAPTURE_RETENTION_CAPACITY_POLICY
        != EXPECTED_RAW_CAPTURE_RETENTION_CAPACITY_POLICY
    ):
        raise RawCaptureArtifactBuildError("raw_capture_contract_drift")
    if (
        _source_sha256("raw_capture_service_composition.py")
        != EXPECTED_RAW_CAPTURE_COMPOSITION_SHA256
        or _source_sha256("service.py")
        != EXPECTED_WORKER_SERVICE_SHA256
        or _source_sha256("raw_capture_retention_guard.py")
        != EXPECTED_RAW_CAPTURE_RETENTION_GUARD_SHA256
    ):
        raise RawCaptureArtifactBuildError("raw_capture_source_pin_drift")
    catalog = acquisition_catalog_snapshot()
    matrix = activation_matrix_snapshot()
    if (
        catalog.get("catalog_sha256") != expected_catalog
        or matrix.get("matrix_sha256") != expected_matrix
        or matrix.get("catalog_sha256") != expected_catalog
    ):
        raise RawCaptureArtifactBuildError("closed_activation_pin_mismatch")
    try:
        validate_closed_recovery_bindings(RECOVERY_BINDINGS, ACTIVATION_RULES)
    except Exception as error:
        raise RawCaptureArtifactBuildError("closed_recovery_binding_invalid") from error
    binding_hashes = {
        handler_id: recovery_binding_sha256(binding)
        for handler_id, binding in sorted(RECOVERY_BINDINGS.items())
    }
    if (
        len(binding_hashes) != EXPECTED_RECOVERY_BINDING_COUNT
        or _digest(binding_hashes) != EXPECTED_RECOVERY_INVENTORY_SHA256
    ):
        raise RawCaptureArtifactBuildError("recovery_inventory_drift")
    active = sorted(
        (rule for rule in ACTIVATION_RULES.values() if rule.executable),
        key=lambda rule: rule.workload_id,
    )
    if (
        tuple(rule.workload_id for rule in active)
        != RAW_CAPTURE_ALLOWED_WORKLOAD_IDS
        or len(active) != int(matrix.get("active_shadow_raw", -1))
        or len(active) != len(binding_hashes)
    ):
        raise RawCaptureArtifactBuildError("active_workload_membership_drift")
    workloads = catalog.get("workloads")
    if not isinstance(workloads, list):
        raise RawCaptureArtifactBuildError("catalog_workloads_invalid")
    workload_rows = {
        row.get("workload_id"): row
        for row in workloads
        if isinstance(row, dict) and isinstance(row.get("workload_id"), str)
    }
    if len(workload_rows) != len(workloads):
        raise RawCaptureArtifactBuildError("catalog_workloads_invalid")
    opera = ACTIVATION_RULES.get(OPERA_WORKLOAD_ID)
    if opera is None or not opera.executable or opera.handler_id is None:
        raise RawCaptureArtifactBuildError("opera_closed_activation_missing")
    opera_binding = RECOVERY_BINDINGS.get(opera.handler_id)
    opera_catalog = workload_rows.get(OPERA_WORKLOAD_ID)
    if (
        MAX_OPERA_HDF5_BYTES != EXPECTED_OPERA_MAX_BYTES
        or OPERA_TRANSPORT_FREE_MARGIN_BYTES
        != EXPECTED_OPERA_SPOOL_MARGIN_BYTES
        or OPERA_TRANSPORT_SPOOL_MAX_ENTRIES
        != EXPECTED_OPERA_SPOOL_MAX_ENTRIES
        or opera.maximum_transport_bytes != EXPECTED_OPERA_MAX_BYTES
        or opera.maximum_artifacts != EXPECTED_OPERA_MAX_ARTIFACTS
        or not isinstance(opera_catalog, dict)
        or opera_catalog.get("cadence_seconds")
        != EXPECTED_OPERA_CADENCE_SECONDS
        or opera_binding is None
        or recovery_binding_sha256(opera_binding)
        != EXPECTED_OPERA_RECOVERY_SHA256
    ):
        raise RawCaptureArtifactBuildError("opera_capacity_contract_drift")
    event_rows = catalog.get("event_ingresses")
    if (
        not isinstance(event_rows, list)
        or len(event_rows) != 1
        or not isinstance(event_rows[0], dict)
        or event_rows[0].get("ingress_id") != BLITZORTUNG_INGRESS_ID
        or event_rows[0].get("status") != "ownership_blocked"
        or event_rows[0].get("cadence_seconds") is not None
    ):
        raise RawCaptureArtifactBuildError("blitzortung_catalog_contract_drift")
    if (
        BLITZORTUNG_MAX_PAYLOAD_BYTES
        != EXPECTED_BLITZORTUNG_MAX_PAYLOAD_BYTES
        or BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES
        != EXPECTED_BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES
        or BLITZORTUNG_CONTROL_STAGING_RECOVERY_EXTRA_ENTRIES
        != EXPECTED_BLITZORTUNG_STAGING_RECOVERY_EXTRA_ENTRIES
        or BLITZORTUNG_EVENT_MAX_QUEUE_ENTRIES
        != EXPECTED_BLITZORTUNG_HARD_MAX_QUEUE_ENTRIES
        or BLITZORTUNG_EVENT_MAX_CONTROL_RECEIPTS
        != EXPECTED_BLITZORTUNG_HARD_MAX_CONTROL_RECEIPTS
        or BLITZORTUNG_EVENT_MAX_RETAINED_ENTRIES
        != EXPECTED_BLITZORTUNG_HARD_MAX_RETAINED_ENTRIES
        or BLITZORTUNG_EVENT_MAX_SESSIONS
        != EXPECTED_BLITZORTUNG_HARD_MAX_SESSIONS
    ):
        raise RawCaptureArtifactBuildError("blitzortung_capacity_contract_drift")
    return catalog, matrix, active, workload_rows


def derive_raw_capture_shadow_capacity(
    *,
    capture_window_hours: int,
    target_raw_capacity_bytes: int,
    blitzortung_enabled: bool,
    operator_host_operational_free_reserve_bytes: int,
    expected_catalog_sha256: str,
    expected_activation_matrix_sha256: str,
) -> RawCaptureCapacityPlan:
    """Derive a no-reclaim worst-case envelope from the closed catalogue."""

    if (
        isinstance(capture_window_hours, bool)
        or not isinstance(capture_window_hours, int)
        or not MIN_CAPTURE_WINDOW_HOURS
        <= capture_window_hours
        <= MAX_CAPTURE_WINDOW_HOURS
    ):
        raise RawCaptureArtifactBuildError("capture_window_hours_invalid")
    target = _positive_i64(
        target_raw_capacity_bytes,
        "target_raw_capacity_bytes_invalid",
    )
    if type(blitzortung_enabled) is not bool:
        raise RawCaptureArtifactBuildError("blitzortung_profile_invalid")
    host_reserve = _positive_i64(
        operator_host_operational_free_reserve_bytes,
        "operator_host_operational_free_reserve_bytes_invalid",
    )
    if host_reserve < MINIMUM_HOST_OPERATIONAL_FREE_RESERVE_BYTES:
        raise RawCaptureArtifactBuildError(
            "operator_host_operational_free_reserve_bytes_insufficient"
        )
    window_seconds = _checked_multiply(capture_window_hours, 3600)
    catalog, matrix, active, workload_rows = _closed_capacity_inputs(
        expected_catalog_sha256=expected_catalog_sha256,
        expected_activation_matrix_sha256=(
            expected_activation_matrix_sha256
        ),
    )
    workload_evidence: list[dict[str, Any]] = []
    batch_bytes_values: list[int] = []
    batch_entries_values: list[int] = []
    window_bytes_values: list[int] = []
    window_entries_values: list[int] = []
    window_reservations_values: list[int] = []
    provider_inputs: dict[str, list[dict[str, Any]]] = {}
    for rule in active:
        if rule.handler_id is None:
            raise RawCaptureArtifactBuildError("active_handler_missing")
        catalog_row = workload_rows.get(rule.workload_id)
        binding = RECOVERY_BINDINGS.get(rule.handler_id)
        if not isinstance(catalog_row, dict) or binding is None:
            raise RawCaptureArtifactBuildError("active_capacity_binding_missing")
        budget = catalog_row.get("budget")
        cadence = catalog_row.get("cadence_seconds")
        if (
            not isinstance(budget, dict)
            or isinstance(cadence, bool)
            or not isinstance(cadence, int)
            or cadence <= 0
            or budget.get("max_transport_bytes")
            != rule.maximum_transport_bytes
            or budget.get("max_artifacts") != rule.maximum_artifacts
            or budget.get("max_wall_seconds") != rule.maximum_wall_seconds
        ):
            raise RawCaptureArtifactBuildError("catalog_activation_budget_drift")
        maximum_bytes = _positive_i64(
            rule.maximum_transport_bytes,
            "workload_transport_capacity_invalid",
        )
        maximum_artifacts = _positive_i64(
            rule.maximum_artifacts,
            "workload_artifact_capacity_invalid",
        )
        cycles = (window_seconds + cadence - 1) // cadence
        cycles = _positive_i64(cycles, "workload_cycle_count_invalid")
        window_bytes = _checked_multiply(cycles, maximum_bytes)
        window_entries = _checked_multiply(cycles, maximum_artifacts)
        maximum_member_bytes = _binding_maximum_member_bytes(binding)
        row = {
            "cadence_seconds": cadence,
            "cycles": cycles,
            "maximum_artifacts": maximum_artifacts,
            "maximum_member_bytes": maximum_member_bytes,
            "maximum_transport_bytes": maximum_bytes,
            "provider_id": rule.provider_id,
            "recovery_binding_sha256": recovery_binding_sha256(binding),
            "window_bytes": window_bytes,
            "window_entries": window_entries,
            "workload_id": rule.workload_id,
        }
        workload_evidence.append(row)
        provider_inputs.setdefault(rule.provider_id, []).append(row)
        batch_bytes_values.append(maximum_bytes)
        batch_entries_values.append(maximum_artifacts)
        window_bytes_values.append(window_bytes)
        window_entries_values.append(window_entries)
        window_reservations_values.append(cycles)
    batch_bytes = _sum_i64(batch_bytes_values)
    batch_entries = _sum_i64(batch_entries_values)
    batch_reservations = len(workload_evidence)
    window_bytes = _sum_i64(window_bytes_values)
    window_entries = _sum_i64(window_entries_values)
    window_reservations = _sum_i64(window_reservations_values)
    maximum_capture_bytes = max(batch_bytes_values)
    maximum_capture_entries = max(batch_entries_values)
    maximum_member_bytes = max(
        row["maximum_member_bytes"] for row in workload_evidence
    )
    byte_margin_workloads = sorted(
        row["workload_id"]
        for row in workload_evidence
        if row["maximum_transport_bytes"] == maximum_capture_bytes
    )
    entry_margin_workloads = sorted(
        row["workload_id"]
        for row in workload_evidence
        if row["maximum_artifacts"] == maximum_capture_entries
    )
    configured_bytes = _checked_add(window_bytes, maximum_capture_bytes)
    configured_entries = _checked_add(window_entries, maximum_capture_entries)
    configured_reservations = _checked_add(window_reservations, 1)
    provider_limits: list[ProviderCapacityLimit] = []
    provider_evidence: list[dict[str, Any]] = []
    for provider, rows in sorted(provider_inputs.items()):
        provider_window_bytes = _sum_i64(
            [int(row["window_bytes"]) for row in rows]
        )
        provider_window_entries = _sum_i64(
            [int(row["window_entries"]) for row in rows]
        )
        provider_window_reservations = _sum_i64(
            [int(row["cycles"]) for row in rows]
        )
        provider_margin_bytes = max(
            int(row["maximum_transport_bytes"]) for row in rows
        )
        provider_margin_entries = max(
            int(row["maximum_artifacts"]) for row in rows
        )
        provider_max_bytes = _checked_add(
            provider_window_bytes,
            provider_margin_bytes,
        )
        provider_max_entries = _checked_add(
            provider_window_entries,
            provider_margin_entries,
        )
        provider_limits.append(ProviderCapacityLimit(
            provider=provider,
            max_bytes=provider_max_bytes,
            max_entries=provider_max_entries,
        ))
        provider_evidence.append({
            "configured_max_bytes": provider_max_bytes,
            "configured_max_entries": provider_max_entries,
            "inflight_margin_bytes": provider_margin_bytes,
            "inflight_margin_entries": provider_margin_entries,
            "inflight_margin_reservations": 1,
            "provider_id": provider,
            "window_bytes": provider_window_bytes,
            "window_entries": provider_window_entries,
            "window_reservations": provider_window_reservations,
            "workload_count": len(rows),
        })
    try:
        blitzortung_ingress_limits = BlitzortungEventIngressLimits()
        capacity_profile = RawCapacityProfile(
            global_max_bytes=configured_bytes,
            global_max_entries=configured_entries,
            providers=tuple(provider_limits),
            max_reservations=configured_reservations,
            max_catalog_bytes=CAPACITY_LEDGER_CATALOG_RESERVE_BYTES,
            sqlite_busy_timeout_seconds=5.0,
        )
        raw_store_limits = RawArtifactLimits(
            max_object_bytes=maximum_member_bytes,
            max_total_bytes=configured_bytes,
            max_object_entries=configured_entries,
            max_staging_entries=maximum_capture_entries,
            max_receipt_entries=configured_entries,
            max_capture_entries=configured_reservations,
            max_capture_members=maximum_capture_entries,
            max_catalog_bytes=RAW_STORE_CATALOG_RESERVE_BYTES,
        )
        blitzortung_raw_store_limits = RawArtifactLimits(
            max_object_bytes=BLITZORTUNG_MAX_PAYLOAD_BYTES,
            max_total_bytes=(
                blitzortung_ingress_limits.max_retained_payload_bytes
            ),
            max_object_entries=(
                blitzortung_ingress_limits.max_retained_payload_entries
            ),
            max_staging_entries=(
                blitzortung_ingress_limits.max_queue_entries
            ),
            max_receipt_entries=(
                blitzortung_ingress_limits.max_retained_payload_entries
            ),
            max_capture_entries=(
                blitzortung_ingress_limits.max_retained_payload_entries
            ),
            max_capture_members=1,
            max_catalog_bytes=EVENT_RAW_STORE_CATALOG_RESERVE_BYTES,
        )
    except (TypeError, ValueError, ArithmeticError) as error:
        raise RawCaptureArtifactBuildError(
            "derived_typed_capacity_invalid"
        ) from error
    if maximum_member_bytes != EXPECTED_OPERA_MAX_BYTES:
        raise RawCaptureArtifactBuildError("maximum_object_capacity_drift")
    if (
        blitzortung_ingress_limits.max_queue_entries
        != EXPECTED_BLITZORTUNG_MAX_QUEUE_ENTRIES
        or blitzortung_ingress_limits.max_control_receipts
        != EXPECTED_BLITZORTUNG_MAX_CONTROL_RECEIPTS
        or blitzortung_ingress_limits.max_retained_payload_entries
        != EXPECTED_BLITZORTUNG_MAX_RETAINED_PAYLOAD_ENTRIES
        or blitzortung_ingress_limits.max_retained_payload_bytes
        != EXPECTED_BLITZORTUNG_MAX_RETAINED_PAYLOAD_BYTES
        or blitzortung_ingress_limits.max_sessions
        != EXPECTED_BLITZORTUNG_MAX_SESSIONS
        or blitzortung_ingress_limits.control_receipt_reserve
        != EXPECTED_BLITZORTUNG_CONTROL_RECEIPT_RESERVE
        or float(blitzortung_ingress_limits.claim_lease_seconds)
        != EXPECTED_BLITZORTUNG_CLAIM_LEASE_SECONDS
        or float(blitzortung_ingress_limits.lock_timeout_seconds)
        != EXPECTED_BLITZORTUNG_LOCK_TIMEOUT_SECONDS
    ):
        raise RawCaptureArtifactBuildError(
            "blitzortung_default_capacity_contract_drift"
        )
    transport_spool_reserve = _checked_add(
        MAX_OPERA_HDF5_BYTES,
        OPERA_TRANSPORT_FREE_MARGIN_BYTES,
    )
    event_control_receipt_storage = (
        _checked_multiply(
            blitzortung_ingress_limits.max_control_receipts,
            BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES,
        )
        if blitzortung_enabled
        else 0
    )
    event_control_staging_residue_entries = (
        _checked_add(
            blitzortung_ingress_limits.control_receipt_reserve,
            BLITZORTUNG_CONTROL_STAGING_RECOVERY_EXTRA_ENTRIES,
        )
        if blitzortung_enabled
        else 0
    )
    event_control_staging_residue = _checked_multiply(
        event_control_staging_residue_entries,
        BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES,
    )
    event_raw_quota = (
        blitzortung_raw_store_limits.max_total_bytes
        if blitzortung_enabled
        else 0
    )
    event_catalog_reserve = (
        blitzortung_raw_store_limits.max_catalog_bytes
        if blitzortung_enabled
        else 0
    )
    event_object_entries = (
        blitzortung_raw_store_limits.max_object_entries
        if blitzortung_enabled
        else 0
    )
    configured_raw_quota = _checked_add(
        raw_store_limits.max_total_bytes,
        event_raw_quota,
    )
    metadata_reserve = _sum_i64([
        raw_store_limits.max_catalog_bytes,
        event_catalog_reserve,
        capacity_profile.max_catalog_bytes,
        CONTROL_DB_WAL_METADATA_RESERVE_BYTES,
    ])
    required_same_filesystem = _sum_i64([
        configured_raw_quota,
        transport_spool_reserve,
        metadata_reserve,
        event_control_receipt_storage,
        event_control_staging_residue,
        host_reserve,
    ])
    if target < required_same_filesystem:
        raise RawCaptureArtifactBuildError("target_raw_capacity_insufficient")
    service_combined_persistent_bytes = _sum_i64([
        raw_store_limits.max_total_bytes,
        raw_store_limits.max_catalog_bytes,
        event_raw_quota,
        event_catalog_reserve,
        event_control_receipt_storage,
        event_control_staging_residue,
    ])
    service_storage_capacity_diagnostics = {
        "accounting_contract": (
            "config_bound_optional_event_capacity_v2"
        ),
        "host_operational_free_reserve_bytes": host_reserve,
        "cadence": {
            "catalog_max_bytes": raw_store_limits.max_catalog_bytes,
            "ledger_accounted": True,
            "object_max_bytes": raw_store_limits.max_total_bytes,
            "object_max_entries": raw_store_limits.max_object_entries,
        },
        "combined_persistent_volume": {
            "configured_max_bytes": service_combined_persistent_bytes,
            "raw_object_max_entries": _checked_add(
                raw_store_limits.max_object_entries,
                event_object_entries,
            ),
            "same_filesystem_capacity_claim": True,
        },
        "event": {
            "capacity_claimed": blitzortung_enabled,
            "catalog_max_bytes": event_catalog_reserve,
            "control_receipt_max_bytes": (
                BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES
                if blitzortung_enabled
                else 0
            ),
            "control_receipt_storage_max_bytes": (
                event_control_receipt_storage
            ),
            "control_staging_residue_entry_capacity": (
                event_control_staging_residue_entries
            ),
            "control_staging_residue_max_bytes": (
                event_control_staging_residue
            ),
            "enabled": blitzortung_enabled,
            "ledger_accounted": False,
            "object_max_bytes": event_raw_quota,
            "object_max_entries": event_object_entries,
        },
    }
    guard_configured_persistent_max_bytes = _checked_add(
        service_combined_persistent_bytes,
        capacity_profile.max_catalog_bytes,
    )
    matrix_rules = matrix.get("rules")
    if not isinstance(matrix_rules, list):
        raise RawCaptureArtifactBuildError("activation_matrix_rules_invalid")
    blocked = [
        {
            "reason_code": row.get("reason_code"),
            "status": row.get("status"),
            "workload_id": row.get("workload_id"),
        }
        for row in matrix_rules
        if isinstance(row, dict) and row.get("executable") is False
    ]
    recovery_inventory = {
        handler_id: recovery_binding_sha256(binding)
        for handler_id, binding in sorted(RECOVERY_BINDINGS.items())
    }
    document = {
        "activation": {
            "activation_matrix_sha256": matrix["matrix_sha256"],
            "active_workload_count": len(workload_evidence),
            "active_workload_ids_sha256": _digest(
                [row["workload_id"] for row in workload_evidence]
            ),
            "blocked_cadence_workloads": blocked,
            "catalog_sha256": catalog["catalog_sha256"],
            "recovery_binding_count": len(recovery_inventory),
            "recovery_inventory_sha256": _digest(recovery_inventory),
            "raw_capture_composition_sha256": (
                EXPECTED_RAW_CAPTURE_COMPOSITION_SHA256
            ),
            "worker_service_sha256": EXPECTED_WORKER_SERVICE_SHA256,
        },
        "automatic_reclaim": False,
        "capacity_exhaustion": "fail_closed",
        "contract": CAPACITY_CONTRACT,
        "derivation": {
            "capture_window_hours": capture_window_hours,
            "capture_window_seconds": window_seconds,
            "formulas": {
                "cycles": "ceil(capture_window_seconds/cadence_seconds)",
                "global_max_bytes": (
                    "sum(workload_window_bytes)+componentwise_largest_"
                    "closed_capture_bytes"
                ),
                "global_max_entries": (
                    "sum(workload_window_entries)+componentwise_largest_"
                    "closed_capture_entries"
                ),
                "global_max_reservations": (
                    "sum(workload_cycles)+one_inflight_reservation"
                ),
                "workload_window_bytes": (
                    "cycles*maximum_transport_bytes"
                ),
                "workload_window_entries": "cycles*maximum_artifacts",
            },
            "horizon_semantics": (
                "sizing_horizon_not_automatic_retention_or_reclamation"
            ),
        },
        "event_ingress": {
            "capacity_authority": (
                "raw_capture_service_config_v4_explicit_event_profile"
            ),
            "capacity_claimed": blitzortung_enabled,
            "catalog_ownership_status": "ownership_blocked",
            "enabled": blitzortung_enabled,
            "included_in_cadence_quota": False,
            "included_in_same_filesystem_requirement": blitzortung_enabled,
            "ingress_id": BLITZORTUNG_INGRESS_ID,
            "ingress_limits": blitzortung_ingress_limits.to_metadata(),
            "ledger_accounted": False,
            "limits_status": (
                "configured_enabled_fail_closed"
                if blitzortung_enabled
                else "configured_dormant_unclaimed"
            ),
            "operational_admission": False,
            "raw_store_commit_is_terminal": True,
            "raw_store_limits": asdict(blitzortung_raw_store_limits),
            "raw_store_relationship": (
                "separate_from_cadence_raw_store"
            ),
            "runtime_authority_environment": (
                "STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY"
            ),
        },
        "fail_closed_on_full": True,
        "inflight_margin": {
            "byte_margin_workload_ids": byte_margin_workloads,
            "bytes": maximum_capture_bytes,
            "entries": maximum_capture_entries,
            "entry_margin_workload_ids": entry_margin_workloads,
            "method": "componentwise_largest_closed_capture",
            "reservations": 1,
        },
        "providers": provider_evidence,
        "raw_capacity_profile": capacity_profile.to_mapping(),
        "raw_store_limits": asdict(raw_store_limits),
        "retention_guard": {
            "blitzortung_enabled": blitzortung_enabled,
            "capacity_policy": RAW_CAPTURE_RETENTION_CAPACITY_POLICY,
            "contract": RAW_CAPTURE_RETENTION_GUARD_CONTRACT,
            "event_capacity_claimed": blitzortung_enabled,
            "guard_configured_persistent_max_bytes": (
                guard_configured_persistent_max_bytes
            ),
            "host_operational_free_reserve_bytes": host_reserve,
            "selected_profile": (
                "event_enabled" if blitzortung_enabled else "cadence_only"
            ),
            "service_configured_persistent_max_bytes": (
                service_combined_persistent_bytes
            ),
            "source_sha256": EXPECTED_RAW_CAPTURE_RETENTION_GUARD_SHA256,
        },
        "service_storage_capacity_diagnostics": (
            service_storage_capacity_diagnostics
        ),
        "same_filesystem_capacity": {
            "component_evidence": {
                "capacity_ledger_catalog": {
                    "bytes": capacity_profile.max_catalog_bytes,
                    "source": "RawCapacityProfile.max_catalog_bytes",
                },
                "cadence_raw_artifact_catalog": {
                    "bytes": raw_store_limits.max_catalog_bytes,
                    "source": (
                        "cadence.RawArtifactLimits.max_catalog_bytes"
                    ),
                },
                "control_db_wal_fixed_reserve": {
                    "bytes": CONTROL_DB_WAL_METADATA_RESERVE_BYTES,
                    "source": (
                        "raw_capture_shadow_generator_v2_fixed_bound"
                    ),
                },
                "event_control_receipt_storage": {
                    "capacity_claimed": blitzortung_enabled,
                    "bytes": event_control_receipt_storage,
                    "entry_bytes": (
                        BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES
                        if blitzortung_enabled
                        else 0
                    ),
                    "entries": (
                        blitzortung_ingress_limits.max_control_receipts
                        if blitzortung_enabled
                        else 0
                    ),
                    "formula": (
                        "max_control_receipts*control_receipt_max_bytes"
                    ),
                    "source": (
                        "RawCaptureServiceConfig.storage_capacity_"
                        "diagnostics"
                    ),
                },
                "event_control_staging_residue": {
                    "capacity_claimed": blitzortung_enabled,
                    "bytes": event_control_staging_residue,
                    "entry_bytes": (
                        BLITZORTUNG_CONTROL_RECEIPT_MAX_BYTES
                        if blitzortung_enabled
                        else 0
                    ),
                    "entries": event_control_staging_residue_entries,
                    "formula": (
                        "(control_receipt_reserve+staging_recovery_extra_"
                        "entries)*control_receipt_max_bytes"
                    ),
                    "recovery_extra_entries": (
                        BLITZORTUNG_CONTROL_STAGING_RECOVERY_EXTRA_ENTRIES
                        if blitzortung_enabled
                        else 0
                    ),
                    "source": (
                        "RawCaptureServiceConfig.storage_capacity_"
                        "diagnostics"
                    ),
                },
                "event_raw_artifact_catalog": {
                    "capacity_claimed": blitzortung_enabled,
                    "bytes": event_catalog_reserve,
                    "source": (
                        "event.RawArtifactLimits.max_catalog_bytes"
                    ),
                },
                "host_operational_free_reserve": {
                    "bytes": host_reserve,
                    "source": (
                        "operator_host_operational_free_reserve_bytes"
                    ),
                },
                "opera_transport_free_margin": {
                    "bytes": OPERA_TRANSPORT_FREE_MARGIN_BYTES,
                    "source": "opera_native.OPERA_TRANSPORT_FREE_MARGIN_BYTES",
                },
                "opera_transport_max_object": {
                    "bytes": MAX_OPERA_HDF5_BYTES,
                    "source": "opera_native.MAX_OPERA_HDF5_BYTES",
                },
                "opera_transport_spool_inventory": {
                    "entries": OPERA_TRANSPORT_SPOOL_MAX_ENTRIES,
                    "semantics": (
                        "fail_closed_crash_residue_inventory_not_parallel_"
                        "download_capacity"
                    ),
                    "source": (
                        "opera_native.OPERA_TRANSPORT_SPOOL_MAX_ENTRIES"
                    ),
                },
            },
            "blitzortung_enabled": blitzortung_enabled,
            "configured_cadence_raw_quota": configured_bytes,
            "configured_event_raw_quota": event_raw_quota,
            "configured_raw_quota": configured_raw_quota,
            "event_control_receipt_storage_reserve": (
                event_control_receipt_storage
            ),
            "event_control_staging_residue_reserve": (
                event_control_staging_residue
            ),
            "layout_proof": (
                "all_persistent_paths_under_one_new_direct_child_root"
            ),
            "live_disk_free_space_verified": False,
            "metadata_reserve": metadata_reserve,
            "operator_declared_headroom_bytes": (
                target - required_same_filesystem
            ),
            "operator_declared_target_bytes": target,
            "operator_host_operational_free_reserve_bytes": host_reserve,
            "required_same_filesystem_bytes": required_same_filesystem,
            "requirement_formula": (
                "configured_cadence_raw_quota+configured_event_raw_quota+"
                "transport_spool_reserve+metadata_reserve+"
                "event_control_receipt_storage_reserve+"
                "event_control_staging_residue_reserve+"
                "operator_host_operational_free_reserve_bytes"
            ),
            "shared_ledger_claim": False,
            "target_semantics": (
                "operator_declaration_not_live_disk_free_space_proof"
            ),
            "transport_spool_concurrency": (
                "one_global_spool_lock_one_inflight_download"
            ),
            "transport_spool_preflight_formula": (
                "2*content_length+free_margin_on_same_filesystem"
            ),
            "transport_spool_reserve": transport_spool_reserve,
            "transport_spool_reserve_reasoning": (
                "cadence_raw_quota_already_counts_the_committed_cas_copy;"
                "one_global_spool_lock_requires_only_one_additional_"
                "maximum_object_plus_free_margin"
            ),
        },
        "startup_floor": {
            "batch_bytes": batch_bytes,
            "batch_entries": batch_entries,
            "batch_reservations": batch_reservations,
            "basis": (
                "one_closed_batch_plus_componentwise_largest_"
                "capture_inflight"
            ),
            "bytes": _checked_add(batch_bytes, maximum_capture_bytes),
            "entries": _checked_add(batch_entries, maximum_capture_entries),
            "reservations": _checked_add(batch_reservations, 1),
            "retention_capacity_claim": False,
        },
        "workloads": workload_evidence,
    }
    return RawCaptureCapacityPlan(
        capacity_profile=capacity_profile,
        raw_store_limits=raw_store_limits,
        blitzortung_ingress_limits=blitzortung_ingress_limits,
        blitzortung_raw_store_limits=blitzortung_raw_store_limits,
        blitzortung_enabled=blitzortung_enabled,
        host_operational_free_reserve_bytes=host_reserve,
        _document=_canonical_copy(document),
    )


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_exclusive(path: Path, body: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags, 0o600)
        written = 0
        while written < len(body):
            count = os.write(descriptor, body[written:])
            if count <= 0:
                raise RawCaptureArtifactBuildError("artifact_write_incomplete")
            written += count
        os.fsync(descriptor)
    except FileExistsError as error:
        raise RawCaptureArtifactBuildError("artifact_already_exists") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
    _fsync_directory(path.parent)


def _read_bounded(path: Path) -> bytes:
    try:
        metadata = path.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or _is_reparse(metadata)
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or not 0 < metadata.st_size <= MAX_ARTIFACT_DOCUMENT_BYTES
        ):
            raise RawCaptureArtifactBuildError("artifact_file_invalid")
        body = path.read_bytes()
    except RawCaptureArtifactBuildError:
        raise
    except OSError as error:
        raise RawCaptureArtifactBuildError("artifact_read_failed") from error
    if len(body) != metadata.st_size:
        raise RawCaptureArtifactBuildError("artifact_file_changed")
    return body


def _decode_canonical_document(body: bytes) -> dict[str, Any]:
    duplicate = False

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        nonlocal duplicate
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                duplicate = True
            result[key] = value
        return result

    try:
        value = json.loads(body.decode("ascii"), object_pairs_hook=unique)
    except (UnicodeError, ValueError, RecursionError) as error:
        raise RawCaptureArtifactBuildError("artifact_json_invalid") from error
    if duplicate or not isinstance(value, dict) or _canonical(value) != body:
        raise RawCaptureArtifactBuildError("artifact_json_not_canonical")
    return value


def _reject_secret_keys(value: object) -> None:
    pending = [value]
    nodes = 0
    while pending:
        item = pending.pop()
        nodes += 1
        if nodes > 200_000:
            raise RawCaptureArtifactBuildError("artifact_json_resource_limit")
        if isinstance(item, dict):
            for key, nested in item.items():
                if not isinstance(key, str) or _SECRET_KEY_RE.search(key):
                    raise RawCaptureArtifactBuildError(
                        "artifact_secret_field_forbidden"
                    )
                pending.append(nested)
        elif isinstance(item, list):
            pending.extend(item)


def _artifact_entry(path: Path, output: Path, role: str) -> dict[str, Any]:
    body = _read_bounded(path)
    return {
        "bytes": len(body),
        "path": path.relative_to(output).as_posix(),
        "role": role,
        "sha256": hashlib.sha256(body).hexdigest(),
    }


def _runtime_path_inventory(
    paths: Mapping[str, Path],
    output: Path,
) -> list[dict[str, str]]:
    result = []
    for role, path in sorted(paths.items()):
        result.append({
            "hash_policy": "excluded_mutable_runtime_state",
            "path": path.relative_to(output).as_posix(),
            "role": role,
        })
    return result


def _materializer_provenance() -> dict[str, Any]:
    source = Path(__file__).resolve(strict=True)
    body = _read_bounded(source)
    return {
        "capacity_derivation_target_path_independent": True,
        "contract": MATERIALIZER_CONTRACT,
        "module_path": (
            "storm_tracker_worker/raw_capture_activation_artifacts.py"
        ),
        "module_sha256": hashlib.sha256(body).hexdigest(),
        "target_side_execution_required": True,
    }


def build_raw_capture_shadow_artifacts(
    *,
    persistent_base_root: Path,
    output_directory: Path,
    capture_window_hours: int,
    target_raw_capacity_bytes: int,
    blitzortung_enabled: bool,
    operator_host_operational_free_reserve_bytes: int,
    expected_catalog_sha256: str,
    expected_activation_matrix_sha256: str,
) -> RawCaptureArtifactBuild:
    """Create a new manifest-last activation root without starting a runtime."""

    base = _validate_existing_base(Path(persistent_base_root))
    output = _validate_absent_output(Path(output_directory), base)
    plan = derive_raw_capture_shadow_capacity(
        capture_window_hours=capture_window_hours,
        target_raw_capacity_bytes=target_raw_capacity_bytes,
        blitzortung_enabled=blitzortung_enabled,
        operator_host_operational_free_reserve_bytes=(
            operator_host_operational_free_reserve_bytes
        ),
        expected_catalog_sha256=expected_catalog_sha256,
        expected_activation_matrix_sha256=(
            expected_activation_matrix_sha256
        ),
    )
    (
        worker_paths,
        source_paths,
        blitzortung_control_root,
        blitzortung_raw_store_root,
        all_paths,
    ) = _layout(output)
    try:
        worker_first = WorkerFirstCompositionConfig.create(
            paths=worker_paths,
            profile=default_belgium_observation_profile(),
            process_limits=DwdRvCompletionIsolationLimits(),
            owner_id="worker-first-raw-capture-shadow",
        )
        config = RawCaptureServiceConfig(
            worker_first=worker_first,
            source_paths=source_paths,
            capacity_profile=plan.capacity_profile,
            raw_store_limits=plan.raw_store_limits,
            blitzortung_enabled=plan.blitzortung_enabled,
            blitzortung_control_root=blitzortung_control_root,
            blitzortung_raw_store_root=blitzortung_raw_store_root,
            blitzortung_raw_store_limits=(
                plan.blitzortung_raw_store_limits
            ),
            blitzortung_ingress_limits=plan.blitzortung_ingress_limits,
            host_operational_free_reserve_bytes=(
                plan.host_operational_free_reserve_bytes
            ),
        )
        if (
            config.storage_capacity_diagnostics
            != plan.to_mapping()["service_storage_capacity_diagnostics"]
        ):
            raise RawCaptureArtifactBuildError(
                "service_capacity_diagnostics_drift"
            )
    except Exception as error:
        if isinstance(error, RawCaptureArtifactBuildError):
            raise
        raise RawCaptureArtifactBuildError(
            "raw_capture_service_config_invalid"
        ) from error
    config_path = output / "artifacts" / CONFIG_FILENAME
    capacity_path = output / "artifacts" / CAPACITY_FILENAME
    manifest_path = output / "artifacts" / MANIFEST_FILENAME
    try:
        output.mkdir(mode=0o700)
        if _validate_existing_base(output) != output or output.parent != base:
            raise RawCaptureArtifactBuildError(
                "created_output_directory_not_canonical"
            )
        _fsync_directory(base)
        directories = [
            output / "artifacts",
            output / "state",
            output / "state" / "control",
            output / "state" / "pipeline",
            output / "state" / "source",
            output / "state" / "raw",
            source_paths.raw_store_root,
        ]
        if blitzortung_enabled:
            directories.extend((
                output / "state" / "event-ingress",
                blitzortung_control_root,
                blitzortung_raw_store_root,
            ))
        for directory in directories:
            directory.mkdir(mode=0o700)
            _fsync_directory(directory.parent)
        bootstrap = initialize_worker_first_composition_authority(
            worker_first,
            expected_config_sha256=worker_first.fingerprint,
        )
        authority_path = worker_paths.authority_file
        authority_body = _read_bounded(authority_path)
        authority_sha = hashlib.sha256(authority_body).hexdigest()
        if authority_sha != bootstrap.authority_sha256:
            raise RawCaptureArtifactBuildError("authority_checksum_mismatch")
        config_body = _canonical(config.to_mapping())
        capacity_body = _canonical(plan.to_mapping())
        _reject_secret_keys(_decode_canonical_document(config_body))
        _reject_secret_keys(_decode_canonical_document(capacity_body))
        _reject_secret_keys(_decode_canonical_document(authority_body))
        _write_exclusive(config_path, config_body)
        _write_exclusive(capacity_path, capacity_body)
        config_sha = hashlib.sha256(config_body).hexdigest()
        activation_environment = {
            AUTHORITY_SHA256_ENV: authority_sha,
            CONFIG_PATH_ENV: str(config_path),
            CONFIG_SHA256_ENV: config_sha,
            RUNTIME_MODE_ENV: RAW_CAPTURE_SHADOW_MODE,
        }
        activation = load_worker_first_service_activation(
            activation_environment
        )
        if (
            not activation.raw_capture_selected
            or activation.config_sha256 != config_sha
            or activation.authority_sha256 != authority_sha
            or activation.composition_config != config
            or activation.to_mapping().get("activation_performed") is not False
        ):
            raise RawCaptureArtifactBuildError(
                "activation_roundtrip_mismatch"
            )
        authority = load_worker_first_authority(
            authority_path,
            expected_authority_sha256=authority_sha,
        )
        authority.verify_paths()
        artifacts = sorted(
            (
                _artifact_entry(config_path, output, "raw_capture_config"),
                _artifact_entry(capacity_path, output, "capacity_profile"),
                _artifact_entry(authority_path, output, "worker_first_authority"),
                _artifact_entry(
                    worker_paths.ownership_lock_file,
                    output,
                    "ownership_lock_anchor",
                ),
            ),
            key=lambda item: item["path"],
        )
        mutable_paths = {
            role: path
            for role, path in all_paths.items()
            if path not in {
                worker_paths.authority_file,
                worker_paths.ownership_lock_file,
            }
            and (
                blitzortung_enabled
                or not role.startswith("event.blitzortung_")
            )
        }
        manifest = {
            "activation_environment": activation_environment,
            "artifact_role": "target_side_materialization",
            "artifacts": artifacts,
            "bootstrap_receipt": bootstrap.to_mapping(),
            "catalog_pins": {
                "activation_matrix_sha256": (
                    expected_activation_matrix_sha256
                ),
                "catalog_sha256": expected_catalog_sha256,
            },
            "capacity_parameters": {
                "blitzortung_enabled": blitzortung_enabled,
                "capture_window_hours": capture_window_hours,
                "operator_host_operational_free_reserve_bytes": (
                    operator_host_operational_free_reserve_bytes
                ),
                "operator_declared_target_bytes": (
                    target_raw_capacity_bytes
                ),
            },
            "contract": MANIFEST_CONTRACT,
            "deployment_performed": False,
            "host_path_pinned": True,
            "immutable_artifact_count": len(artifacts),
            "materializer": _materializer_provenance(),
            "mutable_runtime_paths": _runtime_path_inventory(
                mutable_paths,
                output,
            ),
            "output_directory": str(output),
            "persistent_base_root": str(base),
            "portable_release_artifact": False,
            "raw_capture_config_contract": RAW_CAPTURE_CONFIG_CONTRACT,
            "raw_capture_runtime_contract": RAW_CAPTURE_RUNTIME_CONTRACT,
            "reproducibility_scope": (
                "same_canonical_target_root_and_path_semantics_only"
            ),
            "runtime_mode": RAW_CAPTURE_SHADOW_MODE,
            "target_materialization_required": True,
            "target_path_flavour": (
                "windows" if os.name == "nt" else "posix"
            ),
        }
        _reject_secret_keys(manifest)
        manifest_body = _canonical(manifest)
        _write_exclusive(manifest_path, manifest_body)
        manifest_sha = hashlib.sha256(manifest_body).hexdigest()
        verified = verify_raw_capture_shadow_artifacts(manifest_path)
        if verified.get("manifest_sha256") != manifest_sha:
            raise RawCaptureArtifactBuildError("manifest_roundtrip_mismatch")
    except RawCaptureArtifactBuildError:
        raise
    except Exception as error:
        # The new directory is deliberately retained without a manifest.  A
        # caller can inspect it; the builder never destroys or overwrites it.
        raise RawCaptureArtifactBuildError("artifact_build_failed") from error
    return RawCaptureArtifactBuild(
        persistent_base_root=base,
        output_directory=output,
        config_path=config_path,
        config_sha256=config_sha,
        capacity_path=capacity_path,
        capacity_sha256=hashlib.sha256(capacity_body).hexdigest(),
        authority_path=authority_path,
        authority_sha256=authority_sha,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
        activation_environment=dict(activation_environment),
    )


def _manifest_artifact_paths(
    manifest: Mapping[str, Any],
    output: Path,
) -> dict[str, Path]:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 4:
        raise RawCaptureArtifactBuildError("manifest_artifact_set_invalid")
    expected_roles = {
        "capacity_profile",
        "ownership_lock_anchor",
        "raw_capture_config",
        "worker_first_authority",
    }
    expected_relative_paths = {
        "capacity_profile": f"artifacts/{CAPACITY_FILENAME}",
        "ownership_lock_anchor": "state/control/ownership.lock",
        "raw_capture_config": f"artifacts/{CONFIG_FILENAME}",
        "worker_first_authority": "state/control/authority.json",
    }
    result: dict[str, Path] = {}
    for item in artifacts:
        if not isinstance(item, dict) or set(item) != {
            "bytes",
            "path",
            "role",
            "sha256",
        }:
            raise RawCaptureArtifactBuildError("manifest_artifact_invalid")
        role = item["role"]
        relative = item["path"]
        if role not in expected_roles or role in result or not isinstance(relative, str):
            raise RawCaptureArtifactBuildError("manifest_artifact_invalid")
        pure = PurePosixPath(relative)
        if (
            pure.is_absolute()
            or not pure.parts
            or any(part in {"", ".", ".."} for part in pure.parts)
            or "\\" in relative
        ):
            raise RawCaptureArtifactBuildError("manifest_artifact_path_invalid")
        candidate = output.joinpath(*pure.parts)
        try:
            metadata = candidate.lstat()
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(output)
        except (OSError, RuntimeError, ValueError) as error:
            raise RawCaptureArtifactBuildError(
                "manifest_artifact_path_invalid"
            ) from error
        if (
            stat.S_ISLNK(metadata.st_mode)
            or _is_reparse(metadata)
            or not stat.S_ISREG(metadata.st_mode)
            or _path_identity(candidate) != _path_identity(resolved)
            or relative != expected_relative_paths[role]
        ):
            raise RawCaptureArtifactBuildError(
                "manifest_artifact_path_invalid"
            )
        body = _read_bounded(resolved)
        if (
            item["bytes"] != len(body)
            or _sha256(item["sha256"], "manifest_artifact_sha_invalid")
            != hashlib.sha256(body).hexdigest()
        ):
            raise RawCaptureArtifactBuildError("manifest_artifact_hash_mismatch")
        result[role] = resolved
    if set(result) != expected_roles:
        raise RawCaptureArtifactBuildError("manifest_artifact_set_invalid")
    return result


def verify_raw_capture_shadow_artifacts(
    manifest_path: Path,
) -> dict[str, Any]:
    """Re-read immutable artifacts and bind them to the current closed code."""

    supplied = Path(manifest_path)
    try:
        resolved_manifest = supplied.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise RawCaptureArtifactBuildError("manifest_path_unavailable") from error
    if (
        not supplied.is_absolute()
        or _path_identity(supplied) != _path_identity(resolved_manifest)
        or resolved_manifest.name != MANIFEST_FILENAME
        or resolved_manifest.parent.name != "artifacts"
    ):
        raise RawCaptureArtifactBuildError("manifest_path_invalid")
    manifest_body = _read_bounded(resolved_manifest)
    manifest = _decode_canonical_document(manifest_body)
    _reject_secret_keys(manifest)
    expected_fields = {
        "activation_environment",
        "artifact_role",
        "artifacts",
        "bootstrap_receipt",
        "catalog_pins",
        "capacity_parameters",
        "contract",
        "deployment_performed",
        "host_path_pinned",
        "immutable_artifact_count",
        "materializer",
        "mutable_runtime_paths",
        "output_directory",
        "persistent_base_root",
        "portable_release_artifact",
        "raw_capture_config_contract",
        "raw_capture_runtime_contract",
        "reproducibility_scope",
        "runtime_mode",
        "target_materialization_required",
        "target_path_flavour",
    }
    if (
        set(manifest) != expected_fields
        or manifest.get("contract") != MANIFEST_CONTRACT
        or manifest.get("artifact_role")
        != "target_side_materialization"
        or manifest.get("deployment_performed") is not False
        or manifest.get("host_path_pinned") is not True
        or manifest.get("immutable_artifact_count") != 4
        or manifest.get("materializer") != _materializer_provenance()
        or manifest.get("portable_release_artifact") is not False
        or manifest.get("reproducibility_scope")
        != "same_canonical_target_root_and_path_semantics_only"
        or manifest.get("runtime_mode") != RAW_CAPTURE_SHADOW_MODE
        or manifest.get("target_materialization_required") is not True
        or manifest.get("target_path_flavour")
        != ("windows" if os.name == "nt" else "posix")
        or manifest.get("raw_capture_config_contract")
        != RAW_CAPTURE_CONFIG_CONTRACT
        or manifest.get("raw_capture_runtime_contract")
        != RAW_CAPTURE_RUNTIME_CONTRACT
    ):
        raise RawCaptureArtifactBuildError("manifest_schema_invalid")
    output = resolved_manifest.parent.parent
    base = output.parent
    if (
        manifest.get("output_directory") != str(output)
        or manifest.get("persistent_base_root") != str(base)
        or resolved_manifest
        != output / "artifacts" / MANIFEST_FILENAME
    ):
        raise RawCaptureArtifactBuildError("manifest_root_binding_invalid")
    if (
        _validate_existing_base(base) != base
        or _validate_existing_base(output) != output
        or output.parent != base
    ):
        raise RawCaptureArtifactBuildError("manifest_root_binding_invalid")
    parameters = manifest.get("capacity_parameters")
    if (
        type(parameters) is not dict
        or set(parameters) != {
            "blitzortung_enabled",
            "capture_window_hours",
            "operator_declared_target_bytes",
            "operator_host_operational_free_reserve_bytes",
        }
        or type(parameters.get("blitzortung_enabled")) is not bool
    ):
        raise RawCaptureArtifactBuildError("manifest_capacity_binding_invalid")
    blitzortung_enabled = parameters["blitzortung_enabled"]
    (
        expected_worker_paths,
        expected_source_paths,
        expected_blitzortung_control_root,
        expected_blitzortung_raw_store_root,
        expected_all_paths,
    ) = _layout(output)
    expected_mutable_paths = {
        role: path
        for role, path in expected_all_paths.items()
        if path not in {
            expected_worker_paths.authority_file,
            expected_worker_paths.ownership_lock_file,
        }
        and (
            blitzortung_enabled
            or not role.startswith("event.blitzortung_")
        )
    }
    if manifest.get("mutable_runtime_paths") != _runtime_path_inventory(
        expected_mutable_paths,
        output,
    ):
        raise RawCaptureArtifactBuildError(
            "manifest_runtime_path_inventory_invalid"
        )
    artifact_paths = _manifest_artifact_paths(manifest, output)
    config_body = _read_bounded(artifact_paths["raw_capture_config"])
    capacity_body = _read_bounded(artifact_paths["capacity_profile"])
    authority_body = _read_bounded(artifact_paths["worker_first_authority"])
    lock_body = _read_bounded(artifact_paths["ownership_lock_anchor"])
    config_mapping = _decode_canonical_document(config_body)
    capacity_mapping = _decode_canonical_document(capacity_body)
    authority_mapping = _decode_canonical_document(authority_body)
    _reject_secret_keys(config_mapping)
    _reject_secret_keys(capacity_mapping)
    _reject_secret_keys(authority_mapping)
    try:
        config = RawCaptureServiceConfig.from_mapping(config_mapping)
    except Exception as error:
        raise RawCaptureArtifactBuildError("config_roundtrip_invalid") from error
    pins = manifest.get("catalog_pins")
    if (
        not isinstance(pins, dict)
        or set(pins) != {
            "activation_matrix_sha256",
            "catalog_sha256",
        }
    ):
        raise RawCaptureArtifactBuildError("manifest_capacity_binding_invalid")
    plan = derive_raw_capture_shadow_capacity(
        capture_window_hours=parameters.get("capture_window_hours"),
        target_raw_capacity_bytes=parameters.get(
            "operator_declared_target_bytes"
        ),
        blitzortung_enabled=parameters.get("blitzortung_enabled"),
        operator_host_operational_free_reserve_bytes=parameters.get(
            "operator_host_operational_free_reserve_bytes"
        ),
        expected_catalog_sha256=pins.get("catalog_sha256"),
        expected_activation_matrix_sha256=pins.get(
            "activation_matrix_sha256"
        ),
    )
    if (
        capacity_mapping != plan.to_mapping()
        or config.capacity_profile.to_mapping()
        != plan.capacity_profile.to_mapping()
        or asdict(config.raw_store_limits) != asdict(plan.raw_store_limits)
        or asdict(config.blitzortung_raw_store_limits)
        != asdict(plan.blitzortung_raw_store_limits)
        or config.blitzortung_enabled is not plan.blitzortung_enabled
        or config.blitzortung_ingress_limits.to_metadata()
        != plan.blitzortung_ingress_limits.to_metadata()
        or config.host_operational_free_reserve_bytes
        != plan.host_operational_free_reserve_bytes
        or config.storage_capacity_diagnostics
        != plan.to_mapping()["service_storage_capacity_diagnostics"]
    ):
        raise RawCaptureArtifactBuildError("capacity_config_binding_mismatch")
    if (
        config.worker_first.paths.to_mapping()
        != expected_worker_paths.to_mapping()
        or config.source_paths != expected_source_paths
        or config.blitzortung_control_root
        != expected_blitzortung_control_root
        or config.blitzortung_raw_store_root
        != expected_blitzortung_raw_store_root
        or config.blitzortung_keepalive_seconds != 60
    ):
        raise RawCaptureArtifactBuildError("config_target_path_binding_mismatch")
    config_sha = hashlib.sha256(config_body).hexdigest()
    authority_sha = hashlib.sha256(authority_body).hexdigest()
    activation_environment = manifest.get("activation_environment")
    expected_environment = {
        AUTHORITY_SHA256_ENV: authority_sha,
        CONFIG_PATH_ENV: str(artifact_paths["raw_capture_config"]),
        CONFIG_SHA256_ENV: config_sha,
        RUNTIME_MODE_ENV: RAW_CAPTURE_SHADOW_MODE,
    }
    if activation_environment != expected_environment:
        raise RawCaptureArtifactBuildError("manifest_activation_binding_invalid")
    try:
        activation = load_worker_first_service_activation(expected_environment)
        authority = load_worker_first_authority(
            artifact_paths["worker_first_authority"],
            expected_authority_sha256=authority_sha,
        )
        authority.verify_paths()
    except Exception as error:
        raise RawCaptureArtifactBuildError(
            "activation_authority_roundtrip_invalid"
        ) from error
    bootstrap = manifest.get("bootstrap_receipt")
    if (
        not activation.raw_capture_selected
        or activation.composition_config != config
        or not isinstance(bootstrap, dict)
        or set(bootstrap) != {
            "authority_scope_sha256",
            "authority_sha256",
            "config_sha256",
            "contract",
            "live_wiring",
            "process_authority_sha256",
            "store_binding_sha256",
            "store_count",
        }
        or bootstrap.get("contract") != BOOTSTRAP_RECEIPT_CONTRACT
        or bootstrap.get("live_wiring") is not False
        or bootstrap.get("store_count") != len(REQUIRED_STORE_ROLES)
        or _sha256(
            bootstrap.get("process_authority_sha256"),
            "bootstrap_process_authority_sha_invalid",
        )
        != bootstrap.get("process_authority_sha256")
        or bootstrap.get("authority_sha256") != authority_sha
        or bootstrap.get("config_sha256") != config.worker_first.fingerprint
        or bootstrap.get("authority_scope_sha256")
        != authority.authority_scope_sha256
        or bootstrap.get("store_binding_sha256")
        != authority.store_binding_sha256
        or hashlib.sha256(lock_body).hexdigest() != LOCK_ANCHOR_SHA256
    ):
        raise RawCaptureArtifactBuildError("bootstrap_manifest_binding_invalid")
    return {
        **_canonical_copy(manifest),
        "manifest_sha256": hashlib.sha256(manifest_body).hexdigest(),
    }


__all__ = [
    "CAPACITY_CONTRACT",
    "MANIFEST_CONTRACT",
    "MATERIALIZER_CONTRACT",
    "RawCaptureArtifactBuild",
    "RawCaptureArtifactBuildError",
    "RawCaptureCapacityPlan",
    "build_raw_capture_shadow_artifacts",
    "derive_raw_capture_shadow_capacity",
    "verify_raw_capture_shadow_artifacts",
]
