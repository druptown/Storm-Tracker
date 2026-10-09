"""Closed, inert composition root for the worker-first DWD shadow lane.

This module is a construction boundary, not a service entry point.  It opens
the exact durable stores already used by the worker-first contracts, binds
them to one immutable ownership authority and constructs exactly one
``WorkerFirstRuntimeCoordinator``.  The only publication target is a local
hash-only shadow sink; no envelope can leave this process through this module.

Authority creation is a separate, explicit bootstrap action.  The authority
and lock files are created once and are never truncated or replaced.  Runtime
construction requires independently pinned configuration and authority
SHA-256 values and fails closed on every mismatch.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import threading
import time
from typing import Any, Callable, Final

from .acquisition_evidence_lifecycle import (
    CONTRACT as ACQUISITION_CONTRACT,
    SCHEMA_VERSION as ACQUISITION_SCHEMA_VERSION,
    AcquisitionEvidenceLifecycleStore,
    AcquisitionEvidenceLimits,
)
from .canonical_evidence_store import (
    DEFAULT_MAX_DATABASE_BYTES as DEFAULT_CANONICAL_DATABASE_BYTES,
    CanonicalEvidenceStore,
)
from .desired_state import (
    CONTRACT as DESIRED_CONTRACT,
    MAX_AUTHORITY_CLAIMS as DESIRED_MAX_AUTHORITY_CLAIMS,
    MAX_DATABASE_BYTES as DESIRED_MAX_DATABASE_BYTES,
    MAX_STATE_ROWS as DESIRED_MAX_STATE_ROWS,
    METHOD as DESIRED_METHOD,
    SCHEMA_VERSION as DESIRED_SCHEMA_VERSION,
    DesiredStateStore,
)
from .dwd_rv_completion_consumer import (
    CONSUMER_ID,
    RESULT_STORE_CONTRACT,
    SCHEMA_VERSION as DWD_OUTPUT_SCHEMA_VERSION,
    STREAM_ID,
    DwdRvCompletionConsumer,
    DwdRvCompletionOutputLimits,
    DwdRvCompletionOutputStore,
    DwdRvPipelineOutcomeAuthority,
    DwdRvShadowChainProfile,
)
from .dwd_rv_completion_process_isolation import (
    DwdRvCompletionIsolatedRunner,
    DwdRvCompletionIsolationLimits,
    _profile_from_mapping as _load_dwd_profile,
)
from .dwd_rv_quantitative import (
    DEFAULT_MAX_DERIVED_BYTES,
    DEFAULT_MAX_DERIVED_OBJECTS,
    DEFAULT_MAX_STAGING_OBJECTS,
    DwdRvDerivedArtifactStore,
)
from .operational_fact_store import (
    ALGORITHM_VERSION as FACT_ALGORITHM_VERSION,
    CONTRACT as FACT_CONTRACT,
    DEFAULT_RETENTION_ROWS as FACT_RETENTION_ROWS,
    MAX_DATABASE_BYTES as FACT_MAX_DATABASE_BYTES,
    MAX_REVISION_BYTES as FACT_MAX_REVISION_BYTES,
    SCHEMA_VERSION as FACT_SCHEMA_VERSION,
    OperationalFactRevisionStore,
)
from .operational_snapshot_publication import (
    MAX_OUTBOX_DATABASE_BYTES,
    OUTBOX_ALGORITHM_VERSION,
    OUTBOX_SCHEMA_VERSION,
    PublicationOutboxStore,
)
from .operational_snapshot_runtime import OperationalSnapshotRuntime
from .operational_snapshot_store import (
    DEFAULT_RETENTION_ROWS as SNAPSHOT_RETENTION_ROWS,
    MAX_COMMIT_RECEIPTS as SNAPSHOT_MAX_COMMIT_RECEIPTS,
    MAX_DATABASE_BYTES as SNAPSHOT_MAX_DATABASE_BYTES,
    MAX_SNAPSHOT_ROWS as SNAPSHOT_MAX_ROWS,
    STORE_ALGORITHM_VERSION as SNAPSHOT_ALGORITHM_VERSION,
    STORE_SCHEMA_VERSION as SNAPSHOT_SCHEMA_VERSION,
    OperationalSnapshotStore,
)
from .raw_artifacts import RawArtifactStore
from .tracking_core.operational_snapshot import decode_control_envelope
from .worker_first_ownership import (
    REQUIRED_STORE_ROLES,
    WorkerFirstFenceBusy,
    WorkerFirstOwnershipFence,
    WorkerFirstOwnershipError,
    WorkerFirstStoreAuthority,
    build_worker_first_authority_bytes,
    initialize_worker_first_lock_file,
    load_worker_first_authority,
)
from .worker_first_runtime import WorkerFirstRuntimeCoordinator


CONFIG_CONTRACT: Final = "stv3-worker-first-composition-config-v1"
STORE_IDENTITY_CONTRACT: Final = "stv3-worker-first-store-identity-v1"
BOOTSTRAP_RECEIPT_CONTRACT: Final = (
    "stv3-worker-first-composition-bootstrap-receipt-v1"
)
COMPOSITION_DIAGNOSTICS_CONTRACT: Final = (
    "stv3-worker-first-composition-diagnostics-v1"
)
SHADOW_PUBLISHER_CONTRACT: Final = "stv3-worker-first-hash-shadow-sink-v1"
MODE: Final = "shadow_unwired"
METEOROLOGICAL_ADMISSION: Final = "exact_fact_binding_only"

MAX_CONFIG_BYTES: Final = 256 * 1024
MAX_DIAGNOSTIC_BYTES: Final = 16 * 1024
MAX_PATH_CHARS: Final = 4096
MAX_SHADOW_PUBLICATIONS: Final = 1_000_000_000

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_PATH_FIELDS = frozenset({
    "acquisition_completion_root",
    "authority_file",
    "desired_state_db",
    "dwd_canonical_evidence_db",
    "dwd_derived_artifact_root",
    "dwd_handoff_raw_root",
    "operational_fact_db",
    "operational_snapshot_db",
    "ownership_lock_file",
    "pipeline_output_db",
    "snapshot_outbox_db",
})
_DIRECTORY_PATH_FIELDS = frozenset({
    "acquisition_completion_root",
    "dwd_derived_artifact_root",
    "dwd_handoff_raw_root",
})
_RUNTIME_FIELDS = frozenset({
    "cancel_grace_seconds",
    "control_timeout_seconds",
    "lease_seconds",
    "processing_timeout_seconds",
    "stop_timeout_seconds",
})
_PROCESS_LIMIT_FIELDS = frozenset({
    "cpu_seconds",
    "max_request_bytes",
    "max_result_bytes",
    "rss_bytes",
    "wall_seconds",
})


class WorkerFirstCompositionError(RuntimeError):
    """A bounded, path-free composition failure."""


class WorkerFirstCompositionConfigError(WorkerFirstCompositionError):
    """The closed configuration document differs from its contract."""


class WorkerFirstCompositionIntegrityError(WorkerFirstCompositionError):
    """A durable store or immutable authority failed validation."""


class WorkerFirstCompositionBusy(WorkerFirstCompositionError):
    """Another process or coordinator owns the exact composition."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, RecursionError) as error:
        raise WorkerFirstCompositionConfigError("config_not_canonical") from error


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: object, code: str) -> str:
    if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
        raise WorkerFirstCompositionConfigError(code)
    return value


def _identifier(value: object, code: str) -> str:
    if (
        not isinstance(value, str)
        or _ID_RE.fullmatch(value) is None
        or "://" in value
    ):
        raise WorkerFirstCompositionConfigError(code)
    return value


def _exact(value: object, fields: frozenset[str], code: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise WorkerFirstCompositionConfigError(code)
    return dict(value)


def _duration(value: object, code: str, *, maximum: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0.0 < float(value) <= maximum
    ):
        raise WorkerFirstCompositionConfigError(code)
    return float(value)


def _is_reparse(metadata: os.stat_result) -> bool:
    flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(flag and int(getattr(metadata, "st_file_attributes", 0)) & flag)


def _same_path(left: str | Path, right: str | Path) -> bool:
    return os.path.normcase(str(left)) == os.path.normcase(str(right))


def _closed_path(value: object) -> Path:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > MAX_PATH_CHARS
        or "\x00" in value
    ):
        raise WorkerFirstCompositionConfigError("config_path_invalid")
    supplied = Path(value)
    if not supplied.is_absolute():
        raise WorkerFirstCompositionConfigError("config_path_invalid")
    try:
        resolved = supplied.resolve(strict=False)
    except (OSError, RuntimeError) as error:
        raise WorkerFirstCompositionConfigError("config_path_invalid") from error
    if not _same_path(supplied, resolved):
        raise WorkerFirstCompositionConfigError("config_path_not_canonical")
    probe = supplied
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    try:
        metadata = probe.lstat()
    except OSError as error:
        raise WorkerFirstCompositionConfigError("config_path_invalid") from error
    if probe.is_symlink() or _is_reparse(metadata):
        raise WorkerFirstCompositionConfigError("config_path_unsafe")
    if supplied.exists():
        metadata = supplied.lstat()
        if supplied.is_symlink() or _is_reparse(metadata):
            raise WorkerFirstCompositionConfigError("config_path_unsafe")
    return resolved


def _is_within(child: Path, parent: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return child != parent


@dataclass(frozen=True, slots=True, repr=False)
class WorkerFirstCompositionPaths:
    acquisition_completion_root: Path
    authority_file: Path
    desired_state_db: Path
    dwd_canonical_evidence_db: Path
    dwd_derived_artifact_root: Path
    dwd_handoff_raw_root: Path
    operational_fact_db: Path
    operational_snapshot_db: Path
    ownership_lock_file: Path
    pipeline_output_db: Path
    snapshot_outbox_db: Path

    @classmethod
    def from_mapping(cls, value: object) -> "WorkerFirstCompositionPaths":
        mapping = _exact(value, _PATH_FIELDS, "config_paths_schema_invalid")
        paths = {name: _closed_path(mapping[name]) for name in _PATH_FIELDS}
        normalized = {os.path.normcase(str(path)) for path in paths.values()}
        if len(normalized) != len(paths):
            raise WorkerFirstCompositionConfigError("config_paths_alias")
        directories = [paths[name] for name in _DIRECTORY_PATH_FIELDS]
        if any(
            _is_within(left, right) or _is_within(right, left)
            for index, left in enumerate(directories)
            for right in directories[index + 1:]
        ):
            raise WorkerFirstCompositionConfigError("config_paths_nested_alias")
        for name, path in paths.items():
            if name in _DIRECTORY_PATH_FIELDS:
                continue
            if any(_is_within(path, directory) for directory in directories):
                raise WorkerFirstCompositionConfigError("config_paths_nested_alias")
        if paths["authority_file"].parent != paths["ownership_lock_file"].parent:
            raise WorkerFirstCompositionConfigError("authority_lock_parent_mismatch")
        return cls(**paths)

    def to_mapping(self) -> dict[str, str]:
        return {name: str(getattr(self, name)) for name in sorted(_PATH_FIELDS)}


@dataclass(frozen=True, slots=True, repr=False)
class WorkerFirstCompositionConfig:
    paths: WorkerFirstCompositionPaths
    profile: DwdRvShadowChainProfile
    process_limits: DwdRvCompletionIsolationLimits
    owner_id: str
    lease_seconds: float
    processing_timeout_seconds: float
    cancel_grace_seconds: float
    control_timeout_seconds: float
    stop_timeout_seconds: float

    @classmethod
    def create(
        cls,
        *,
        paths: WorkerFirstCompositionPaths,
        profile: DwdRvShadowChainProfile,
        process_limits: DwdRvCompletionIsolationLimits = (
            DwdRvCompletionIsolationLimits()
        ),
        owner_id: str = "worker-first-dwd-shadow",
        lease_seconds: float = 600.0,
        processing_timeout_seconds: float = 540.0,
        cancel_grace_seconds: float = 10.0,
        control_timeout_seconds: float = 30.0,
        stop_timeout_seconds: float = 2.0,
    ) -> "WorkerFirstCompositionConfig":
        if not isinstance(paths, WorkerFirstCompositionPaths):
            raise WorkerFirstCompositionConfigError("config_paths_invalid")
        if not isinstance(profile, DwdRvShadowChainProfile):
            raise WorkerFirstCompositionConfigError("dwd_profile_invalid")
        if not isinstance(process_limits, DwdRvCompletionIsolationLimits):
            raise WorkerFirstCompositionConfigError("process_limits_invalid")
        mapping = {
            "consumer_id": CONSUMER_ID,
            "contract": CONFIG_CONTRACT,
            "dwd": {
                "process_limits": process_limits.to_mapping(),
                "process_limits_sha256": process_limits.fingerprint,
                "profile": profile.to_mapping(),
                "profile_sha256": profile.fingerprint,
            },
            "live_wiring": False,
            "meteorological_admission": METEOROLOGICAL_ADMISSION,
            "mode": MODE,
            "owner_id": owner_id,
            "paths": paths.to_mapping(),
            "runtime": {
                "cancel_grace_seconds": cancel_grace_seconds,
                "control_timeout_seconds": control_timeout_seconds,
                "lease_seconds": lease_seconds,
                "processing_timeout_seconds": processing_timeout_seconds,
                "stop_timeout_seconds": stop_timeout_seconds,
            },
            "stream_id": STREAM_ID,
        }
        return cls.from_mapping(mapping)

    @classmethod
    def from_mapping(cls, value: object) -> "WorkerFirstCompositionConfig":
        mapping = _exact(value, frozenset({
            "consumer_id", "contract", "dwd", "live_wiring",
            "meteorological_admission", "mode", "owner_id", "paths",
            "runtime", "stream_id",
        }), "config_schema_invalid")
        if (
            mapping["contract"] != CONFIG_CONTRACT
            or mapping["mode"] != MODE
            or mapping["live_wiring"] is not False
            or mapping["meteorological_admission"] != METEOROLOGICAL_ADMISSION
            or mapping["consumer_id"] != CONSUMER_ID
            or mapping["stream_id"] != STREAM_ID
        ):
            raise WorkerFirstCompositionConfigError("config_authority_invalid")
        paths = WorkerFirstCompositionPaths.from_mapping(mapping["paths"])
        runtime = _exact(
            mapping["runtime"], _RUNTIME_FIELDS, "runtime_schema_invalid"
        )
        lease = _duration(runtime["lease_seconds"], "lease_invalid", maximum=3600.0)
        processing = _duration(
            runtime["processing_timeout_seconds"],
            "processing_timeout_invalid",
            maximum=lease,
        )
        cancel = _duration(
            runtime["cancel_grace_seconds"],
            "cancel_grace_invalid",
            maximum=60.0,
        )
        control = _duration(
            runtime["control_timeout_seconds"],
            "control_timeout_invalid",
            maximum=300.0,
        )
        stop = _duration(
            runtime["stop_timeout_seconds"],
            "stop_timeout_invalid",
            maximum=30.0,
        )
        if processing + cancel > lease:
            raise WorkerFirstCompositionConfigError("runtime_lease_budget_invalid")
        dwd = _exact(
            mapping["dwd"],
            frozenset({
                "process_limits", "process_limits_sha256", "profile",
                "profile_sha256",
            }),
            "dwd_schema_invalid",
        )
        process_value = _exact(
            dwd["process_limits"],
            _PROCESS_LIMIT_FIELDS,
            "process_limits_schema_invalid",
        )
        try:
            process_limits = DwdRvCompletionIsolationLimits(**process_value)
            profile = _load_dwd_profile(dwd["profile"])
        except Exception:
            raise WorkerFirstCompositionConfigError("dwd_profile_invalid") from None
        if (
            process_limits.to_mapping() != process_value
            or process_limits.fingerprint
            != _sha(dwd["process_limits_sha256"], "process_limits_sha_invalid")
            or profile.to_mapping() != dwd["profile"]
            or profile.fingerprint
            != _sha(dwd["profile_sha256"], "profile_sha_invalid")
        ):
            raise WorkerFirstCompositionConfigError("dwd_identity_mismatch")
        if lease <= process_limits.wall_seconds + 1.0:
            raise WorkerFirstCompositionConfigError("dwd_lease_too_short")
        if processing < process_limits.wall_seconds + 1.0:
            raise WorkerFirstCompositionConfigError(
                "dwd_processing_timeout_too_short"
            )
        result = cls(
            paths=paths,
            profile=profile,
            process_limits=process_limits,
            owner_id=_identifier(mapping["owner_id"], "owner_id_invalid"),
            lease_seconds=lease,
            processing_timeout_seconds=processing,
            cancel_grace_seconds=cancel,
            control_timeout_seconds=control,
            stop_timeout_seconds=stop,
        )
        if len(_canonical(result.to_mapping())) > MAX_CONFIG_BYTES:
            raise WorkerFirstCompositionConfigError("config_capacity_exceeded")
        return result

    def to_mapping(self) -> dict[str, Any]:
        return {
            "consumer_id": CONSUMER_ID,
            "contract": CONFIG_CONTRACT,
            "dwd": {
                "process_limits": self.process_limits.to_mapping(),
                "process_limits_sha256": self.process_limits.fingerprint,
                "profile": self.profile.to_mapping(),
                "profile_sha256": self.profile.fingerprint,
            },
            "live_wiring": False,
            "meteorological_admission": METEOROLOGICAL_ADMISSION,
            "mode": MODE,
            "owner_id": self.owner_id,
            "paths": self.paths.to_mapping(),
            "runtime": {
                "cancel_grace_seconds": self.cancel_grace_seconds,
                "control_timeout_seconds": self.control_timeout_seconds,
                "lease_seconds": self.lease_seconds,
                "processing_timeout_seconds": self.processing_timeout_seconds,
                "stop_timeout_seconds": self.stop_timeout_seconds,
            },
            "stream_id": STREAM_ID,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.to_mapping())


@dataclass(frozen=True, slots=True, repr=False)
class WorkerFirstCompositionBootstrapReceipt:
    config_sha256: str
    authority_sha256: str
    authority_scope_sha256: str
    store_binding_sha256: str
    process_authority_sha256: str

    def to_mapping(self) -> dict[str, Any]:
        return {
            "authority_scope_sha256": self.authority_scope_sha256,
            "authority_sha256": self.authority_sha256,
            "config_sha256": self.config_sha256,
            "contract": BOOTSTRAP_RECEIPT_CONTRACT,
            "live_wiring": False,
            "process_authority_sha256": self.process_authority_sha256,
            "store_binding_sha256": self.store_binding_sha256,
            "store_count": len(REQUIRED_STORE_ROLES),
        }


@dataclass(frozen=True, slots=True, repr=False)
class _CompositionParts:
    desired: DesiredStateStore
    completion: AcquisitionEvidenceLifecycleStore
    facts: OperationalFactRevisionStore
    snapshots: OperationalSnapshotStore
    outbox: PublicationOutboxStore
    outputs: DwdRvCompletionOutputStore
    handoff: RawArtifactStore
    artifacts: DwdRvDerivedArtifactStore
    evidence: CanonicalEvidenceStore
    runner: DwdRvCompletionIsolatedRunner
    consumer: DwdRvCompletionConsumer
    outcome_authority: DwdRvPipelineOutcomeAuthority


def _validate_config_pin(
    config: WorkerFirstCompositionConfig,
    expected_config_sha256: str,
) -> str:
    if not isinstance(config, WorkerFirstCompositionConfig):
        raise WorkerFirstCompositionConfigError("config_type_invalid")
    expected = _sha(expected_config_sha256, "expected_config_sha_invalid")
    observed = config.fingerprint
    if observed != expected:
        raise WorkerFirstCompositionConfigError("config_checksum_mismatch")
    return observed


def _construct_store_parts(
    config: WorkerFirstCompositionConfig,
    *,
    clock: Callable[[], float],
) -> _CompositionParts:
    paths = config.paths
    desired = DesiredStateStore(paths.desired_state_db)
    desired.audit()
    completion = AcquisitionEvidenceLifecycleStore(
        paths.acquisition_completion_root,
        clock=clock,
    )
    facts = OperationalFactRevisionStore(paths.operational_fact_db, clock=clock)
    snapshots = OperationalSnapshotStore(
        paths.operational_snapshot_db,
        clock=clock,
    )
    snapshots.initialize()
    outbox = PublicationOutboxStore(paths.snapshot_outbox_db, clock=clock)
    outbox.initialize()
    handoff = RawArtifactStore(paths.dwd_handoff_raw_root)
    handoff.initialize()
    artifacts = DwdRvDerivedArtifactStore(paths.dwd_derived_artifact_root)
    evidence = CanonicalEvidenceStore(
        paths.dwd_canonical_evidence_db,
        raw_artifact_reader=handoff,
        clock=clock,
    )
    outputs = DwdRvCompletionOutputStore(paths.pipeline_output_db, clock=clock)
    runner = DwdRvCompletionIsolatedRunner(
        handoff_raw_store=handoff,
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        profile=config.profile,
        limits=config.process_limits,
    )
    consumer = DwdRvCompletionConsumer(
        lifecycle_store=completion,
        handoff_raw_store=handoff,
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        profile=config.profile,
        process_runner=runner,
        clock=clock,
    )
    outcome = DwdRvPipelineOutcomeAuthority(
        output_store=outputs,
        profile=config.profile,
        process_isolation_authority_sha256=runner.authority_sha256,
    )
    # Full, bounded validation before any ownership acquisition.
    if (
        desired.audit().get("status") != "ok"
        or completion.audit_archive().get("healthy") is not True
        or facts.audit().get("status") != "ok"
        or snapshots.audit().get("status") != "ok"
        or evidence.audit().get("healthy") is not True
    ):
        raise WorkerFirstCompositionIntegrityError("store_audit_failed")
    outputs.diagnostics()
    return _CompositionParts(
        desired=desired,
        completion=completion,
        facts=facts,
        snapshots=snapshots,
        outbox=outbox,
        outputs=outputs,
        handoff=handoff,
        artifacts=artifacts,
        evidence=evidence,
        runner=runner,
        consumer=consumer,
        outcome_authority=outcome,
    )


def _identity(
    *,
    role: str,
    config_sha256: str,
    store_contract: Mapping[str, Any],
) -> str:
    return _digest({
        "composition_config_sha256": config_sha256,
        "contract": STORE_IDENTITY_CONTRACT,
        "role": role,
        "store_contract": dict(store_contract),
    })


def _store_bindings(
    config: WorkerFirstCompositionConfig,
    parts: _CompositionParts,
    *,
    config_sha256: str,
) -> dict[str, tuple[Path, str]]:
    paths = config.paths
    contracts: dict[str, Mapping[str, Any]] = {
        "acquisition_completion_store": {
            "contract": ACQUISITION_CONTRACT,
            "limits": asdict(AcquisitionEvidenceLimits()),
            "schema_version": ACQUISITION_SCHEMA_VERSION,
        },
        "desired_state_store": {
            "contract": DESIRED_CONTRACT,
            "max_authority_claims": DESIRED_MAX_AUTHORITY_CLAIMS,
            "max_database_bytes": DESIRED_MAX_DATABASE_BYTES,
            "max_state_rows": DESIRED_MAX_STATE_ROWS,
            "method": DESIRED_METHOD,
            "schema_version": DESIRED_SCHEMA_VERSION,
        },
        "operational_fact_store": {
            "algorithm": FACT_ALGORITHM_VERSION,
            "contract": FACT_CONTRACT,
            "max_database_bytes": FACT_MAX_DATABASE_BYTES,
            "max_revision_bytes": FACT_MAX_REVISION_BYTES,
            "retention_rows": FACT_RETENTION_ROWS,
            "schema_version": FACT_SCHEMA_VERSION,
        },
        "operational_snapshot_store": {
            "algorithm": SNAPSHOT_ALGORITHM_VERSION,
            "max_commit_receipts": SNAPSHOT_MAX_COMMIT_RECEIPTS,
            "max_database_bytes": SNAPSHOT_MAX_DATABASE_BYTES,
            "max_snapshot_rows": SNAPSHOT_MAX_ROWS,
            "retention_rows": SNAPSHOT_RETENTION_ROWS,
            "schema_version": SNAPSHOT_SCHEMA_VERSION,
        },
        "pipeline_output_store": {
            "contract": RESULT_STORE_CONTRACT,
            "fact_binding_mode": "factless_dwd_completion_v2",
            "limits": asdict(DwdRvCompletionOutputLimits()),
            "outcome_authority_sha256": parts.outcome_authority.fingerprint,
            "process_authority_sha256": parts.runner.authority_sha256,
            "profile_sha256": config.profile.fingerprint,
            "schema_version": DWD_OUTPUT_SCHEMA_VERSION,
        },
        "snapshot_outbox_store": {
            "algorithm": OUTBOX_ALGORITHM_VERSION,
            "max_database_bytes": MAX_OUTBOX_DATABASE_BYTES,
            "schema_version": OUTBOX_SCHEMA_VERSION,
        },
    }
    role_paths = {
        "acquisition_completion_store": paths.acquisition_completion_root,
        "desired_state_store": paths.desired_state_db,
        "operational_fact_store": paths.operational_fact_db,
        "operational_snapshot_store": paths.operational_snapshot_db,
        "pipeline_output_store": paths.pipeline_output_db,
        "snapshot_outbox_store": paths.snapshot_outbox_db,
    }
    if set(contracts) != set(REQUIRED_STORE_ROLES) or set(role_paths) != set(
        REQUIRED_STORE_ROLES
    ):
        raise WorkerFirstCompositionIntegrityError("store_role_set_invalid")
    return {
        role: (
            role_paths[role],
            _identity(
                role=role,
                config_sha256=config_sha256,
                store_contract=contracts[role],
            ),
        )
        for role in sorted(REQUIRED_STORE_ROLES)
    }


def _store_authorities(
    bindings: Mapping[str, tuple[Path, str]],
) -> tuple[WorkerFirstStoreAuthority, ...]:
    return tuple(
        WorkerFirstStoreAuthority.create(
            role=role,
            path=path,
            identity_sha256=identity,
        )
        for role, (path, identity) in sorted(bindings.items())
    )


def _ensure_authority_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        metadata = path.parent.lstat()
        resolved = path.parent.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise WorkerFirstCompositionIntegrityError(
            "authority_parent_invalid"
        ) from error
    if (
        path.parent.is_symlink()
        or _is_reparse(metadata)
        or not stat.S_ISDIR(metadata.st_mode)
        or not _same_path(path.parent, resolved)
    ):
        raise WorkerFirstCompositionIntegrityError("authority_parent_invalid")


def _write_authority_once(path: Path, body: bytes) -> None:
    """Publish exact bytes once; an existing file must already be identical."""

    if path.exists():
        try:
            existing = path.read_bytes()
        except OSError as error:
            raise WorkerFirstCompositionIntegrityError(
                "authority_file_unavailable"
            ) from error
        if existing != body:
            raise WorkerFirstCompositionIntegrityError("authority_file_conflict")
        return
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
                raise WorkerFirstCompositionIntegrityError(
                    "authority_file_write_failed"
                )
            written += count
        os.fsync(descriptor)
    except FileExistsError:
        try:
            if path.read_bytes() != body:
                raise WorkerFirstCompositionIntegrityError(
                    "authority_file_conflict"
                )
        except WorkerFirstCompositionIntegrityError:
            raise
        except OSError as error:
            raise WorkerFirstCompositionIntegrityError(
                "authority_file_unavailable"
            ) from error
    except WorkerFirstCompositionIntegrityError:
        raise
    except OSError as error:
        raise WorkerFirstCompositionIntegrityError(
            "authority_file_write_failed"
        ) from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def initialize_worker_first_composition_authority(
    config: WorkerFirstCompositionConfig,
    *,
    expected_config_sha256: str,
    clock: Callable[[], float] = time.time,
) -> WorkerFirstCompositionBootstrapReceipt:
    """Create/verify the durable layout and publish one immutable authority."""

    config_sha = _validate_config_pin(config, expected_config_sha256)
    if not callable(clock):
        raise WorkerFirstCompositionConfigError("clock_invalid")
    try:
        parts = _construct_store_parts(config, clock=clock)
        bindings = _store_bindings(config, parts, config_sha256=config_sha)
        authorities = _store_authorities(bindings)
        _ensure_authority_parent(config.paths.authority_file)
        lock_path = initialize_worker_first_lock_file(
            config.paths.ownership_lock_file
        )
        body = build_worker_first_authority_bytes(
            consumer_id=CONSUMER_ID,
            stream_id=STREAM_ID,
            lock_path=lock_path,
            stores=authorities,
        )
        _write_authority_once(config.paths.authority_file, body)
        authority_sha = hashlib.sha256(body).hexdigest()
        binding = load_worker_first_authority(
            config.paths.authority_file,
            expected_authority_sha256=authority_sha,
        )
        binding.assert_coordinator_binding(
            consumer_id=CONSUMER_ID,
            stream_id=STREAM_ID,
            stores=bindings,
        )
    except WorkerFirstCompositionError:
        raise
    except Exception:
        raise WorkerFirstCompositionIntegrityError(
            "composition_bootstrap_failed"
        ) from None
    return WorkerFirstCompositionBootstrapReceipt(
        config_sha256=config_sha,
        authority_sha256=binding.authority_sha256,
        authority_scope_sha256=binding.authority_scope_sha256,
        store_binding_sha256=binding.store_binding_sha256,
        process_authority_sha256=parts.runner.authority_sha256,
    )


class _HashOnlyShadowPublisher:
    """Local receipt sink: validates bytes, retains only counters and SHA-256."""

    __slots__ = ("_count", "_last_sha256", "_lock")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._count = 0
        self._last_sha256: str | None = None

    def __call__(self, payload: bytes) -> None:
        if not isinstance(payload, bytes):
            raise TypeError("shadow_payload_invalid")
        decode_control_envelope(payload)
        digest = hashlib.sha256(payload).hexdigest()
        with self._lock:
            self._count = min(MAX_SHADOW_PUBLICATIONS, self._count + 1)
            self._last_sha256 = digest

    def diagnostics(self) -> dict[str, Any]:
        with self._lock:
            return {
                "contract": SHADOW_PUBLISHER_CONTRACT,
                "last_envelope_sha256": self._last_sha256,
                "publication_count": self._count,
                "transport": "local_hash_only",
            }


class WorkerFirstComposition:
    """Owned handle around exactly one inert worker-first coordinator."""

    __slots__ = (
        "_authority_sha256",
        "_config_sha256",
        "_consumer",
        "_coordinator",
        "_outcome_authority",
        "_process_authority_sha256",
        "_publisher",
        "_state",
        "_store_binding_sha256",
    )

    def __init__(
        self,
        *,
        coordinator: WorkerFirstRuntimeCoordinator,
        consumer: DwdRvCompletionConsumer,
        outcome_authority: DwdRvPipelineOutcomeAuthority,
        publisher: _HashOnlyShadowPublisher,
        config_sha256: str,
        authority_sha256: str,
        store_binding_sha256: str,
        process_authority_sha256: str,
    ) -> None:
        self._coordinator = coordinator
        self._consumer = consumer
        self._outcome_authority = outcome_authority
        self._publisher = publisher
        self._config_sha256 = config_sha256
        self._authority_sha256 = authority_sha256
        self._store_binding_sha256 = store_binding_sha256
        self._process_authority_sha256 = process_authority_sha256
        self._state = "constructed"

    @property
    def coordinator(self) -> WorkerFirstRuntimeCoordinator:
        return self._coordinator

    async def async_start(self) -> dict[str, Any]:
        result = await self._coordinator.async_start()
        self._state = "started"
        return result

    async def async_tick(self) -> dict[str, Any]:
        return await self._coordinator.async_tick()

    async def async_stop(self) -> dict[str, Any]:
        result = await self._coordinator.async_stop()
        self._state = (
            "stopped" if result.get("state") == "stopped" else "stop_pending"
        )
        return result

    def diagnostics(self) -> dict[str, Any]:
        result = {
            "authority_sha256": self._authority_sha256,
            "config_sha256": self._config_sha256,
            "contract": COMPOSITION_DIAGNOSTICS_CONTRACT,
            "fact_publication": METEOROLOGICAL_ADMISSION,
            "live_wiring": False,
            "mode": MODE,
            "pipeline_fact_binding": "absent_dwd_factless",
            "process_authority_sha256": self._process_authority_sha256,
            "publisher": self._publisher.diagnostics(),
            "runtime": self._coordinator.diagnostics(),
            "state": self._state,
            "store_binding_sha256": self._store_binding_sha256,
            "store_count": len(REQUIRED_STORE_ROLES),
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise WorkerFirstCompositionError("diagnostics_capacity_exceeded")
        return result


def build_worker_first_composition(
    config: WorkerFirstCompositionConfig,
    *,
    expected_config_sha256: str,
    expected_authority_sha256: str,
    clock: Callable[[], float] = time.time,
) -> WorkerFirstComposition:
    """Open exact stores, acquire one fence and construct one coordinator."""

    config_sha = _validate_config_pin(config, expected_config_sha256)
    authority_sha = _sha(
        expected_authority_sha256,
        "expected_authority_sha_invalid",
    )
    if not callable(clock):
        raise WorkerFirstCompositionConfigError("clock_invalid")
    fence: WorkerFirstOwnershipFence | None = None
    lease = None
    construction_phase = "authority"
    try:
        fence = WorkerFirstOwnershipFence(
            config.paths.authority_file,
            expected_authority_sha256=authority_sha,
        )
        configured_role_paths = {
            "acquisition_completion_store": (
                config.paths.acquisition_completion_root
            ),
            "desired_state_store": config.paths.desired_state_db,
            "operational_fact_store": config.paths.operational_fact_db,
            "operational_snapshot_store": config.paths.operational_snapshot_db,
            "pipeline_output_store": config.paths.pipeline_output_db,
            "snapshot_outbox_store": config.paths.snapshot_outbox_db,
        }
        if any(
            not _same_path(
                configured_role_paths[role],
                fence.binding.store(role).path,
            )
            for role in REQUIRED_STORE_ROLES
        ):
            raise WorkerFirstCompositionIntegrityError(
                "composition_authority_path_mismatch"
            )
        construction_phase = "stores"
        parts = _construct_store_parts(config, clock=clock)
        bindings = _store_bindings(config, parts, config_sha256=config_sha)
        fence.binding.assert_coordinator_binding(
            consumer_id=CONSUMER_ID,
            stream_id=STREAM_ID,
            stores=bindings,
        )
        publisher = _HashOnlyShadowPublisher()
        snapshots = OperationalSnapshotRuntime(
            publisher=publisher,
            status_supplier=lambda: {
                "failed_jobs": 0,
                "queue_depth": 0,
                "state": "unknown",
            },
            desired_state_store=parts.desired,
            snapshot_store=parts.snapshots,
            outbox_store=parts.outbox,
            fact_revision_store=parts.facts,
            clock=clock,
        )
        construction_phase = "runtime"
        lease = fence.acquire()
        coordinator = WorkerFirstRuntimeCoordinator(
            desired_state_store=parts.desired,
            completion_lifecycle=parts.completion,
            fact_revision_lifecycle=parts.facts,
            snapshot_lifecycle=snapshots,
            processor=parts.consumer.process_leased,
            outcome_authority=parts.outcome_authority,
            ownership_lease=lease,
            store_bindings=bindings,
            consumer_id=CONSUMER_ID,
            stream_id=STREAM_ID,
            owner_id=config.owner_id,
            lease_seconds=config.lease_seconds,
            processing_timeout_seconds=config.processing_timeout_seconds,
            cancel_grace_seconds=config.cancel_grace_seconds,
            control_timeout_seconds=config.control_timeout_seconds,
            stop_timeout_seconds=config.stop_timeout_seconds,
            clock=clock,
        )
    except WorkerFirstFenceBusy:
        if fence is not None:
            fence.close()
        raise WorkerFirstCompositionBusy("composition_owner_busy") from None
    except WorkerFirstCompositionIntegrityError:
        if lease is not None:
            try:
                lease.release()
            except WorkerFirstOwnershipError:
                pass
        if fence is not None:
            fence.close()
        raise
    except WorkerFirstOwnershipError:
        if lease is not None:
            lease.release()
        if fence is not None:
            fence.close()
        raise WorkerFirstCompositionIntegrityError(
            "composition_authority_invalid"
        ) from None
    except Exception:
        if lease is not None:
            try:
                lease.release()
            except WorkerFirstOwnershipError:
                pass
        if fence is not None:
            fence.close()
        if construction_phase == "stores":
            raise WorkerFirstCompositionIntegrityError(
                "composition_store_validation_failed"
            ) from None
        raise WorkerFirstCompositionError(
            "composition_runtime_construction_failed"
        ) from None

    return WorkerFirstComposition(
        coordinator=coordinator,
        consumer=parts.consumer,
        outcome_authority=parts.outcome_authority,
        publisher=publisher,
        config_sha256=config_sha,
        authority_sha256=authority_sha,
        store_binding_sha256=fence.binding.store_binding_sha256,
        process_authority_sha256=parts.runner.authority_sha256,
    )


__all__ = [
    "BOOTSTRAP_RECEIPT_CONTRACT",
    "COMPOSITION_DIAGNOSTICS_CONTRACT",
    "CONFIG_CONTRACT",
    "METEOROLOGICAL_ADMISSION",
    "MODE",
    "SHADOW_PUBLISHER_CONTRACT",
    "STORE_IDENTITY_CONTRACT",
    "WorkerFirstComposition",
    "WorkerFirstCompositionBootstrapReceipt",
    "WorkerFirstCompositionBusy",
    "WorkerFirstCompositionConfig",
    "WorkerFirstCompositionConfigError",
    "WorkerFirstCompositionError",
    "WorkerFirstCompositionIntegrityError",
    "WorkerFirstCompositionPaths",
    "build_worker_first_composition",
    "initialize_worker_first_composition_authority",
]
