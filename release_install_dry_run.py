"""Strictly local Storm Tracker release install and rollback simulation.

The simulator is deliberately incapable of contacting or modifying a running
installation.  Inputs are opened read-only and every mutation is confined to
one new run directory below either the repository ``.analysis`` directory or
the operating-system temporary directory.

The two package directories cannot be switched as one filesystem operation.
The contract therefore models component-atomic renames while both services are
stopped, and uses a durable decision journal to recover one coherent pair.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass, field
import errno
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tempfile
import threading
from typing import BinaryIO, Callable, Iterable, Mapping
import uuid

from atomic_noreplace_publication import (
    AtomicNoReplaceConflict,
    AtomicNoReplaceError,
)
from descriptor_relative_atomic_adapter import (
    DescriptorRelativeAtomicAdapter,
    OpenAuthorityAtomicCapability,
    probe_open_authority,
)
from descriptor_relative_authority import (
    AuthorityConflict,
    AuthorityError,
    AuthorityNotFound,
    AuthorityPostCreateError,
    BoundFile,
    DirectoryAuthority,
    NodeIdentity,
    RelativePath,
)
from release_install_bound_inputs import (
    BoundInputDeclaration,
    BoundInputError,
    BoundInputRegistry,
)
from release_install_bound_archives import (
    BoundArchiveError,
    BoundArchiveFile,
    BoundArchiveInspection,
    BoundArchiveTree,
    bound_archive_evidence,
    extract_tar_gz_to_sink,
    extract_zip_to_sink,
    inspect_legacy_tar_gz,
    inspect_legacy_zip,
    validate_candidate_tar_gz,
    validate_candidate_zip,
)
from release_install_owned_output_reads import (
    JournalReadLimits,
    JournalReadResult,
    OwnedOutputReadAuthority,
    OwnedOutputReadError,
    TreeExpectation,
    TreeReadLimits,
    owned_output_read_evidence,
)
from release_install_output_authority import (
    OutputAuthorityConflict,
    OutputAuthorityError,
    OutputAuthorityPostCreateError,
    OutputAuthorityPostPublicationError,
    ReleaseInstallOutputAuthority,
    output_authority_evidence,
)
from storm_tracker_worker.archive_member_stream import (
    ARCHIVE_MEMBER_CHUNK_BYTES,
    PRIVATE_KEY_MARKERS,
    streaming_archive_member_evidence,
)
from storm_tracker_worker.bounded_archive_headers import (
    ArchiveHeaderLimits,
)
from storm_tracker_worker.release_artifacts import (
    INTEGRATION_ROOT_NAME,
    WORKER_ROOT_NAME,
    ReleaseArtifactError,
    verify_release_candidate_evidence_bound_inputs,
)


DRY_RUN_CONTRACT = "stv3-local-release-install-rollback-dry-run-v1"
STATE_CONTRACT = "stv3-local-release-install-state-v2"
JOURNAL_CONTRACT = "stv3-local-release-install-journal-event-v1"
OPTIONS_CONTRACT = "stv3-sanitized-supervisor-options-v1"
PROCESS_LEASE_CONTRACT = "stv3-local-release-install-process-lease-v1"
PROCESS_LEASE_ANCHOR_NAMESPACE = (
    ".stv3-release-install-<run_root_sha256>.lease"
)

PROJECT_ROOT = Path(os.path.abspath(__file__)).parent
MAX_FILE_COUNT = 10_000
MAX_ARCHIVE_MEMBER_COUNT = 20_000
MAX_FILE_BYTES = 512 * 1024 * 1024
MAX_TOTAL_BYTES = 2 * 1024 * 1024 * 1024
MAX_INPUT_METADATA_BYTES = 4 * 1024 * 1024
MAX_ARCHIVE_PATH_BYTES = 1024
MAX_ARCHIVE_PATH_DEPTH = 32
MAX_ARCHIVE_PART_BYTES = 255
MAX_OPTIONS = 64
MAX_SECRET_OPTIONS = 64
MAX_JSON_DEPTH = 64
MAX_JSON_NODES = 50_000
MAX_JSON_STRING_BYTES = 1024 * 1024
MAX_JSON_KEY_BYTES = 256
MAX_JSON_INTEGER = MAX_TOTAL_BYTES

# These are acceptance blockers for the in-scope local output-authority gaps.
# The local state machine may complete while this list is non-empty.  Hostile
# same-user interference deliberately remains a separate external gate even
# when every blocker in this local universe is closed.
LOCAL_ACCEPTANCE_BLOCKERS = (
    "ancestor_and_final_output_path_race_exclusion_not_proven",
    "descriptor_relative_output_authority_not_proven",
)
EXTERNAL_INSTALLATION_GATES = (
    "actual_target_installation_not_exercised",
    "data_and_schema_rollback_not_validated",
    "hostile_same_user_output_races_not_excluded",
    "human_deploy_approval_not_granted",
    "runtime_service_stop_and_start_not_exercised",
    "supervisor_build_and_cache_not_exercised",
    "target_filesystem_power_loss_not_exercised",
)

_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
_WORKER_VERSION = re.compile(
    r'^version:\s*["\']?([0-9]+\.[0-9]+\.[0-9]+)["\']?\s*$',
    re.MULTILINE,
)
_PACKAGE_SLUG = re.compile(
    r'^slug:\s*["\']?([a-z0-9_]+)["\']?\s*$',
    re.MULTILINE,
)
_SAFE_OPTION_NAME = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SAFE_NONSECRET_OPTIONS = frozenset({
    "blitzortung_raw_retention_authorized",
    "compute_profile",
    "cpu_limit_percent",
    "cpu_threads",
    "heartbeat_seconds",
    "kmi_appv4_raw_retention_authorized",
    "log_level",
    "netatmo_station_id",
    "opera_raw_retention_authorized",
    "postgis_database",
    "postgis_host",
    "postgis_port",
    "postgis_projection_enabled",
    "postgis_sslmode",
    "postgis_user",
    "provider_cycle_seconds",
    "queue_size",
    "rainviewer_cache_attribution_accepted",
    "reserve_ha_core",
    "runtime_mode",
    "synoptic_raw_retention_terms_accepted",
    "worker_first_authority_sha256",
    "worker_first_config_path",
    "worker_first_config_sha256",
    "worker_nice",
})
_WORKER_FIRST_ACTIVATION_OPTIONS = (
    "runtime_mode",
    "worker_first_config_path",
    "worker_first_config_sha256",
    "worker_first_authority_sha256",
)
_WORKER_FIRST_OPTION_DECLARATIONS = {
    "runtime_mode": '"worker_only"',
    "worker_first_config_path": '""',
    "worker_first_config_sha256": '""',
    "worker_first_authority_sha256": '""',
}
_WORKER_FIRST_SCHEMA_DECLARATIONS = {
    "runtime_mode": (
        '"list(legacy|worker_first_canary|raw_capture_shadow|worker_only)"'
    ),
    "worker_first_config_path": '"str?"',
    "worker_first_config_sha256": '"str?"',
    "worker_first_authority_sha256": '"str?"',
}
_RAW_RETENTION_OPTIONS = (
    "synoptic_raw_retention_terms_accepted",
    "rainviewer_cache_attribution_accepted",
    "kmi_appv4_raw_retention_authorized",
    "opera_raw_retention_authorized",
    "blitzortung_raw_retention_authorized",
)
_RAW_RETENTION_OPTION_DECLARATIONS = {
    name: "false" for name in _RAW_RETENTION_OPTIONS
}
_RAW_RETENTION_SCHEMA_DECLARATIONS = {
    name: '"bool"' for name in _RAW_RETENTION_OPTIONS
}
_RAW_CAPTURE_EXTERNAL_INSTALLATION_GATES = (
    "raw_capture_target_activation_attestation_not_exercised",
    "raw_capture_target_materialization_not_exercised",
)
_MAX_WORKER_FIRST_CONFIG_PATH_CHARS = 4096
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_WINDOWS_RESERVED_BASENAMES = frozenset({
    "aux", "con", "nul", "prn",
    *(f"com{number}" for number in range(1, 10)),
    *(f"lpt{number}" for number in range(1, 10)),
})
_BANNED_DIRECTORY_NAMES = frozenset({
    ".analysis", ".git", ".mypy_cache", ".pytest_cache", "__pycache__",
    "node_modules", "tests",
})
_BANNED_FILE_NAMES = frozenset({".env", ".ds_store", "thumbs.db"})
_BANNED_FILE_SUFFIXES = (
    ".db", ".key", ".log", ".p12", ".pem", ".pfx", ".pyc", ".pyo",
    ".shm", ".sqlite", ".sqlite3", ".tmp", ".wal", ".zip",
)


class DryRunError(RuntimeError):
    """The local simulation cannot continue without weakening its contract."""


@dataclass(frozen=True, slots=True)
class LegacyRollbackPolicy:
    integration_version: str
    integration_sha256: str
    worker_version: str
    worker_sha256: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.integration_version, "rollback integration version"),
            (self.worker_version, "rollback worker version"),
        ):
            if _VERSION.fullmatch(value) is None:
                raise ValueError(f"{label} is not canonical")
        for value, label in (
            (self.integration_sha256, "rollback integration hash"),
            (self.worker_sha256, "rollback worker hash"),
        ):
            if _SHA256.fullmatch(value) is None:
                raise ValueError(f"{label} is not a lowercase SHA-256")


RECOVERY_BASELINE_POLICY = LegacyRollbackPolicy(
    integration_version="1.11.0",
    integration_sha256=(
        "50bc31f665eb8896fb5e2f75cfaa072d853942671f6d1d2728eb9ba813e1e260"
    ),
    worker_version="1.14.0",
    worker_sha256=(
        "b9f4736e056ad433e9086e47287515ce2ca5789d5f641b13d6d9abaa0b6a6b9f"
    ),
)


@dataclass(frozen=True, slots=True)
class FileRecord:
    path: str
    size: int
    sha256: str
    mode: int

    def to_mapping(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "path": self.path,
            "sha256": self.sha256,
            "size": self.size,
        }

    @classmethod
    def from_mapping(cls, raw: object) -> "FileRecord":
        if not isinstance(raw, dict) or set(raw) != {
            "mode", "path", "sha256", "size"
        }:
            raise DryRunError("tree file record has an unknown schema")
        path = raw["path"]
        size = raw["size"]
        sha256 = raw["sha256"]
        mode = raw["mode"]
        if (
            not isinstance(path, str)
            or not path
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
            or size > MAX_FILE_BYTES
            or not isinstance(sha256, str)
            or _SHA256.fullmatch(sha256) is None
            or mode not in (0o644, 0o755)
        ):
            raise DryRunError("tree file record is invalid")
        _safe_relative_path(path)
        return cls(path=path, size=size, sha256=sha256, mode=mode)


@dataclass(frozen=True, slots=True)
class TreeRecord:
    root_name: str
    files: tuple[FileRecord, ...]
    directories: tuple[str, ...]
    total_bytes: int
    content_sha256: str

    def to_mapping(self) -> dict[str, object]:
        return {
            "content_sha256": self.content_sha256,
            "directories": list(self.directories),
            "directory_count": len(self.directories),
            "file_count": len(self.files),
            "files": [item.to_mapping() for item in self.files],
            "root_name": self.root_name,
            "total_bytes": self.total_bytes,
        }

    @classmethod
    def create(
        cls,
        root_name: str,
        files: Iterable[FileRecord],
        directories: Iterable[str] = (),
    ) -> "TreeRecord":
        if (
            not isinstance(root_name, str)
            or not root_name
            or "/" in root_name
            or _safe_relative_path(root_name).as_posix() != root_name
        ):
            raise DryRunError("tree root name is invalid")
        materialized = tuple(files)
        if any(
            not isinstance(item, FileRecord)
            or FileRecord.from_mapping(item.to_mapping()) != item
            for item in materialized
        ):
            raise DryRunError("tree contains an invalid file record")
        records = tuple(sorted(materialized, key=lambda item: item.path))
        if not records or len(records) > MAX_FILE_COUNT:
            raise DryRunError("tree file count is outside the bounded contract")
        if len({item.path for item in records}) != len(records):
            raise DryRunError("tree contains duplicate file paths")
        if isinstance(directories, (str, bytes)):
            raise DryRunError("tree directories must be a sequence of paths")
        supplied_directories = tuple(directories)
        if (
            len(supplied_directories) > MAX_ARCHIVE_MEMBER_COUNT
            or any(not isinstance(item, str) or not item for item in supplied_directories)
        ):
            raise DryRunError("tree directory count or path is invalid")
        explicit_directories: set[str] = set()
        for item in supplied_directories:
            normalized = _safe_relative_path(item).as_posix()
            if normalized == "." or normalized != item:
                raise DryRunError("tree directory path is invalid")
            explicit_directories.add(normalized)
        if len(explicit_directories) != len(supplied_directories):
            raise DryRunError("tree contains duplicate directory paths")
        logical_directories = set(explicit_directories)
        for item in (*explicit_directories, *(record.path for record in records)):
            logical_directories.update(
                parent.as_posix()
                for parent in PurePosixPath(item).parents
                if parent != PurePosixPath(".")
            )
        file_paths = {item.path for item in records}
        if file_paths & logical_directories:
            raise DryRunError("tree path is both a file and a directory")
        canonical_directories = tuple(sorted(
            logical_directories, key=lambda value: (value.count("/"), value)
        ))
        if (
            len(canonical_directories) > MAX_ARCHIVE_MEMBER_COUNT
            or len(records) + len(canonical_directories) + 1
            > MAX_ARCHIVE_MEMBER_COUNT
        ):
            raise DryRunError("tree directory count is outside the bounded contract")
        total = sum(item.size for item in records)
        if total > MAX_TOTAL_BYTES:
            raise DryRunError("tree exceeds the total byte bound")
        content_sha256 = _sha_bytes(_canonical({
            "directories": list(canonical_directories),
            "files": [item.to_mapping() for item in records],
        }))
        return cls(root_name, records, canonical_directories, total, content_sha256)

    @classmethod
    def from_mapping(cls, raw: object) -> "TreeRecord":
        if not isinstance(raw, dict) or set(raw) != {
            "content_sha256", "directories", "directory_count", "file_count",
            "files", "root_name", "total_bytes",
        }:
            raise DryRunError("tree record has an unknown schema")
        if (
            not isinstance(raw["files"], list)
            or not isinstance(raw["directories"], list)
            or not isinstance(raw["root_name"], str)
        ):
            raise DryRunError("tree record files are invalid")
        tree = cls.create(
            str(raw["root_name"]),
            (FileRecord.from_mapping(item) for item in raw["files"]),
            raw["directories"],
        )
        if (
            type(raw["file_count"]) is not int
            or type(raw["directory_count"]) is not int
            or type(raw["total_bytes"]) is not int
            or not isinstance(raw["content_sha256"], str)
            or raw["file_count"] != len(tree.files)
            or raw["directory_count"] != len(tree.directories)
            or raw["directories"] != list(tree.directories)
            or raw["total_bytes"] != tree.total_bytes
            or raw["content_sha256"] != tree.content_sha256
        ):
            raise DryRunError("tree record hash, count or size differs")
        return tree


def _bound_archive_tree(value: TreeRecord) -> BoundArchiveTree:
    """Convert one simulator seal without weakening either representation."""

    if not isinstance(value, TreeRecord):
        raise TypeError("archive tree conversion requires a TreeRecord")
    try:
        resealed = TreeRecord.from_mapping(value.to_mapping())
        if resealed != value:
            raise DryRunError("archive tree input seal differs")
    except Exception as error:
        raise DryRunError("archive tree seal conversion failed") from error
    _assert_simulator_archive_tree_policy(resealed)
    try:
        converted = BoundArchiveTree.create(
            value.root_name,
            (
                BoundArchiveFile(item.path, item.size, item.sha256, item.mode)
                for item in value.files
            ),
            value.directories,
        )
        roundtrip = TreeRecord.create(
            converted.root_name,
            (
                FileRecord(item.path, item.size, item.sha256, item.mode)
                for item in converted.files
            ),
            converted.directories,
        )
    except Exception as error:
        raise DryRunError("archive tree seal conversion failed") from error
    if roundtrip != value:
        raise DryRunError("archive tree seal conversion changed the tree")
    return converted


def _assert_simulator_archive_tree_policy(value: TreeRecord) -> None:
    """Apply simulator package policy beyond the portable archive contract."""

    for directory in value.directories:
        path = _safe_relative_path(directory)
        if any(part.casefold() in _BANNED_DIRECTORY_NAMES for part in path.parts):
            raise DryRunError("archive contains a forbidden directory")
    for record in value.files:
        _safe_relative_path(record.path)


def _tree_record_from_bound_archive(value: BoundArchiveTree) -> TreeRecord:
    if not isinstance(value, BoundArchiveTree):
        raise TypeError("legacy archive inspection returned an invalid tree")
    try:
        converted = TreeRecord.create(
            value.root_name,
            (
                FileRecord(item.path, item.size, item.sha256, item.mode)
                for item in value.files
            ),
            value.directories,
        )
    except DryRunError:
        raise
    except Exception as error:
        raise DryRunError("legacy archive tree conversion failed") from error
    if _bound_archive_tree(converted) != value:
        raise DryRunError("legacy archive tree conversion changed the tree")
    return converted


def _tree_expectation(value: TreeRecord) -> TreeExpectation:
    """Convert only through the sealed state mapping used by recovery."""

    if not isinstance(value, TreeRecord):
        raise TypeError("owned tree conversion requires a TreeRecord")
    try:
        resealed = TreeRecord.from_mapping(value.to_mapping())
        if resealed != value:
            raise DryRunError("owned tree input seal differs")
        expectation = TreeExpectation.from_mapping(value.to_mapping())
    except Exception as error:
        raise DryRunError("owned tree seal conversion failed") from error
    if expectation.to_mapping() != value.to_mapping():
        raise DryRunError("owned tree seal conversion changed the tree")
    return expectation


_OWNED_TREE_LIMITS = TreeReadLimits(
    maximum_entries=MAX_ARCHIVE_MEMBER_COUNT,
    maximum_depth=MAX_ARCHIVE_PATH_DEPTH,
    maximum_file_bytes=MAX_FILE_BYTES,
    maximum_total_bytes=MAX_TOTAL_BYTES,
    maximum_directory_entries=MAX_ARCHIVE_MEMBER_COUNT,
    chunk_bytes=ARCHIVE_MEMBER_CHUNK_BYTES,
)


@dataclass(frozen=True, slots=True)
class FileWitness:
    path: Path
    device: int
    inode: int
    size: int
    modified_ns: int
    sha256: str


@dataclass(frozen=True, slots=True)
class DryRunResult:
    report_path: Path
    report_sha256: str
    report: Mapping[str, object]


@dataclass(slots=True)
class _AuthorityRunContext:
    """One lexical compatibility view backed by pinned filesystem authority."""

    lexical_root: Path
    allowed_root_path: Path
    run_relative: RelativePath
    allowed_root: DirectoryAuthority
    output: ReleaseInstallOutputAuthority
    root: DirectoryAuthority
    root_identity: NodeIdentity
    atomic_capability: OpenAuthorityAtomicCapability
    atomic: DescriptorRelativeAtomicAdapter
    reads: OwnedOutputReadAuthority
    created: bool
    expected_bound_archive_routes: int
    operation_kind: str
    process_lease: "_ExclusiveProcessLease"
    bound_sources: dict[str, BoundInputRegistry] = field(default_factory=dict)
    bound_outputs: dict[str, BoundInputRegistry] = field(default_factory=dict)
    bound_archive_routes: set[str] = field(default_factory=set)
    closed: bool = False

    def relative(self, path: Path) -> RelativePath:
        absolute = Path(os.path.abspath(path))
        try:
            parts = absolute.relative_to(self.lexical_root).parts
        except ValueError as error:
            raise DryRunError(
                "authority-relative path escapes the dry-run run root"
            ) from error
        if not parts:
            return RelativePath.root()
        try:
            return RelativePath(tuple(parts))
        except AuthorityError as error:
            raise DryRunError("authority-relative path is invalid") from error

    def reprove(self) -> NodeIdentity:
        if self.closed:
            raise DryRunError("dry-run output authority is closed")
        try:
            reopened = self.output.reprove_run_root()
            direct = self.root.identity()
        except (AuthorityError, OutputAuthorityError) as error:
            raise DryRunError("dry-run run-root authority reproof failed") from error
        if (
            not reopened.same_object(self.root_identity)
            or not direct.same_object(self.root_identity)
            or reopened.backend != self.root_identity.backend
            or direct.backend != self.root_identity.backend
        ):
            raise DryRunError("dry-run run-root authority identity changed")
        return direct

    def evidence(self) -> dict[str, object]:
        self.reprove()
        self.process_lease.assert_held()
        if self.operation_kind not in {"execute", "recover"}:
            raise DryRunError("dry-run authority operation kind is invalid")
        archive_routes = sorted(self.bound_archive_routes)
        expected_route_set = (
            _EXECUTE_BOUND_ARCHIVE_ROUTES
            if self.operation_kind == "execute"
            else frozenset()
        )
        expected_routes = len(expected_route_set)
        if expected_routes != self.expected_bound_archive_routes:
            raise DryRunError("bound archive operation route expectation differs")
        archive_processing_required = expected_routes != 0
        archive_routes_satisfied = set(archive_routes) == set(expected_route_set)
        bootstrap_proven = (
            self.allowed_root.absolute_ancestor_resolution_no_reparse_proven
            and self.allowed_root.authority_root_identity_pinned
            and self.allowed_root.identity().same_object(
                self.process_lease.allowed_identity
            )
            and self.allowed_root.identity().backend
            == self.process_lease.allowed_identity.backend
        )
        lease_evidence = self.process_lease.evidence()
        output_evidence = output_authority_evidence()
        atomic_evidence = self.atomic.evidence(include_publication_counts=False)
        owned_reads_base = owned_output_read_evidence()
        bound_archives_base = bound_archive_evidence()
        lease_proven = bool(
            lease_evidence["descriptor_relative_anchor"]
            and lease_evidence["allowed_root_bootstrap_proven"]
            and lease_evidence["anchor_identity_reproved"]
            and lease_evidence["parent_identity_reproved"]
            and lease_evidence["held_before_run_root_mutation"]
        )
        phase_a_integrated = bool(
            output_evidence["descriptor_relative_mutations"]
            and isinstance(self.atomic, DescriptorRelativeAtomicAdapter)
            and atomic_evidence["primitive_proven"]
        )
        phase_b_reads_integrated = bool(
            owned_reads_base["descriptor_relative_owned_reads"]
            and owned_reads_base["root_identity_pre_and_postproof"]
        )
        relative_resolution_descriptor_bound = bool(
            output_evidence["descriptor_relative_mutations"]
            and owned_reads_base["descriptor_relative_owned_reads"]
            and phase_b_reads_integrated
        )
        final_destination_atomic_noreplace = bool(
            output_evidence["final_files_staged_and_noreplace"]
            and atomic_evidence["primitive_proven"]
        )
        source_and_destination_reproved = bool(
            atomic_evidence[
                "source_consumption_and_destination_identity_reproved"
            ]
            and output_evidence[
                "source_consumption_and_destination_identity_reproved"
            ]
        )
        outcome_classification = bool(
            output_evidence["identity_pinned_stage_cleanup"]
            and output_evidence[
                "postcreate_and_postpublication_outcomes_classified"
            ]
            and output_evidence["postpublication_final_cleanup_proven"]
        )
        destination_cleanup = bool(
            output_evidence["postpublication_final_cleanup_proven"]
        )
        local_scope = {
            "ancestor_resolution_no_reparse_proven": bootstrap_proven,
            "authority_root_identity_pinned": (
                self.allowed_root.authority_root_identity_pinned
            ),
            "relative_resolution_descriptor_bound": (
                relative_resolution_descriptor_bound
            ),
            "final_destination_atomic_noreplace": (
                final_destination_atomic_noreplace
            ),
            "final_destination_parent_identity_pinned": bool(
                output_evidence.get("publication_parent_identity_pinned", False)
            ),
            "expected_source_identity_required": atomic_evidence[
                "expected_source_identity_required"
            ] and output_evidence["expected_source_identity_required"],
            "source_consumption_and_destination_identity_reproved": (
                source_and_destination_reproved
            ),
            "postcreate_and_postpublication_outcomes_classified": (
                outcome_classification
            ),
            "destination_io_failure_cleanup_proven": destination_cleanup,
        }
        common_integration_gate = all((
            bootstrap_proven,
            lease_proven,
            phase_a_integrated,
            phase_b_reads_integrated,
            archive_routes_satisfied,
            output_evidence["postpublication_final_cleanup_proven"],
        ))
        ancestor_and_final_proven = bool(
            common_integration_gate and all(local_scope.values())
        )
        descriptor_authority_proven = bool(
            common_integration_gate
            and atomic_evidence["primitive_proven"]
            and relative_resolution_descriptor_bound
        )
        local_blockers = []
        if not ancestor_and_final_proven:
            local_blockers.append(
                "ancestor_and_final_output_path_race_exclusion_not_proven"
            )
        if not descriptor_authority_proven:
            local_blockers.append(
                "descriptor_relative_output_authority_not_proven"
            )
        simulator_proven = not local_blockers
        owned_reads = {
            **owned_reads_base,
            "local_acceptance_blockers_closed": sorted(
                set(LOCAL_ACCEPTANCE_BLOCKERS) - set(local_blockers)
            ),
            "simulator_integration_proven": simulator_proven,
        }
        bound_archives = {
            **bound_archives_base,
            "integration": bool(
                archive_processing_required and archive_routes_satisfied
            ),
            "processing_required": archive_processing_required,
            "route_contract_satisfied": archive_routes_satisfied,
            "simulator_route_integration_proven": bool(
                archive_processing_required and archive_routes_satisfied
            ),
            "simulator_integration_proven": bool(
                simulator_proven
                and archive_processing_required
                and archive_routes_satisfied
            ),
            "local_acceptance_blockers_closed": (
                sorted(LOCAL_ACCEPTANCE_BLOCKERS)
                if simulator_proven and archive_processing_required
                else []
            ),
        }
        integrated_atomic_evidence = {
            **atomic_evidence,
            "absolute_ancestor_resolution_no_reparse_proven": (
                bootstrap_proven
            ),
            "allowed_root_bootstrap_proven": bootstrap_proven,
            "ancestor_and_final_output_path_race_exclusion_proven": (
                ancestor_and_final_proven
            ),
            "descriptor_relative_output_authority_proven": (
                descriptor_authority_proven
            ),
            "authority_root_identity_pinned": (
                self.allowed_root.authority_root_identity_pinned
            ),
            "local_acceptance_blockers_closed": sorted(
                set(LOCAL_ACCEPTANCE_BLOCKERS) - set(local_blockers)
            ),
            "simulator_integration_proven": simulator_proven,
        }
        return {
            **output_evidence,
            "process_lease_integration_proven": lease_proven,
            "allowed_root_bootstrap_mode": (
                self.allowed_root.absolute_bootstrap_mode
            ),
            "allowed_root_bootstrap_proven": bootstrap_proven,
            "atomic_publication_adapter_integrated": True,
            "atomic_capability": integrated_atomic_evidence,
            "bound_archive_processing": bound_archives,
            "bound_archive_processing_phase_b": (
                archive_processing_required and archive_routes_satisfied
            ),
            "bound_archive_processing_required": archive_processing_required,
            "bound_archive_route_count": len(archive_routes),
            "bound_archive_route_count_expected": expected_routes,
            "bound_archive_routes_expected": sorted(expected_route_set),
            "bound_archive_routes_satisfied": archive_routes_satisfied,
            "bound_archive_routes": archive_routes,
            "descriptor_relative_output_authority_proven": (
                descriptor_authority_proven
            ),
            "descriptor_relative_mutations_phase_a": phase_a_integrated,
            "descriptor_relative_owned_reads_phase_b": phase_b_reads_integrated,
            "local_acceptance_blockers_closed": sorted(
                set(LOCAL_ACCEPTANCE_BLOCKERS) - set(local_blockers)
            ),
            "owned_output_reads": owned_reads,
            "operation_kind": self.operation_kind,
            "path_race_scope": {
                "contract": "stv3-local-output-path-race-scope-v1",
                "local": local_scope,
                "local_ancestor_and_final_output_path_race_exclusion_proven": (
                    ancestor_and_final_proven
                ),
                "external": {
                    "hostile_same_user_output_races_excluded": False,
                },
                "hostile_same_user_in_local_scope": False,
            },
            "recovery_inventory_descriptor_relative": True,
            "recovery_reads_descriptor_relative": True,
            "root_identity_reproved": True,
            "simulator_integration_proven": simulator_proven,
            "local_acceptance_blockers": local_blockers,
        }

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        first_error: BaseException | None = None
        registries = (
            *tuple(self.bound_sources.values()),
            *tuple(self.bound_outputs.values()),
        )
        for registry in registries:
            try:
                registry.close()
            except BoundInputError as error:
                if first_error is None:
                    first_error = error
        self.bound_sources.clear()
        self.bound_outputs.clear()
        try:
            self.reads.close()
        except OwnedOutputReadError as error:
            if first_error is None:
                first_error = error
        for owned in (self.root, self.output, self.allowed_root):
            try:
                owned.close()
            except (AuthorityError, OutputAuthorityError) as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise DryRunError("dry-run output authority close failed") from first_error


FaultInjector = Callable[[str], None]


_JOURNAL_FAULT_PREFIXES = (
    "journal.initialized",
    "journal.upgrade_prepared",
    "journal.worker_baseline_backed_up",
    "journal.worker_candidate_activated",
    "journal.integration_baseline_backed_up",
    "journal.integration_candidate_activated",
    "journal.upgrade_commit_decided",
    "journal.upgrade_committed",
    "journal.rollback_decided",
    "journal.rollback_completed",
)
_RENAME_FAULT_PREFIXES = (
    "rename.upgrade_worker_backup",
    "rename.upgrade_worker_activate",
    "rename.upgrade_integration_backup",
    "rename.upgrade_integration_activate",
    "rename.rollback_integration_retain",
    "rename.rollback_integration_restore",
    "rename.rollback_worker_retain",
    "rename.rollback_worker_restore",
)
_RECOVERY_JOURNAL_FAULT_PREFIXES = (
    "journal.rollback_recovered",
    "journal.upgrade_recovered",
    "journal.upgrade_recovered_to_baseline",
)
_RECOVERY_RENAME_FAULT_PREFIXES = (
    "rename.recovery_integration_retain",
    "rename.recovery_integration_restore",
    "rename.recovery_worker_retain",
    "rename.recovery_worker_restore",
)
FAULT_POINTS = (
    *(f"{prefix}.after_pending_fsync_before_publish" for prefix in _JOURNAL_FAULT_PREFIXES),
    *(f"{prefix}.after_publish_before_directory_sync" for prefix in _JOURNAL_FAULT_PREFIXES),
    *(f"{prefix}.after_directory_sync" for prefix in _JOURNAL_FAULT_PREFIXES),
    *(f"{prefix}.before_rename" for prefix in _RENAME_FAULT_PREFIXES),
    *(f"{prefix}.after_rename_before_source_directory_sync" for prefix in _RENAME_FAULT_PREFIXES),
    *(f"{prefix}.after_source_directory_sync" for prefix in _RENAME_FAULT_PREFIXES),
    *(f"{prefix}.after_destination_directory_sync" for prefix in _RENAME_FAULT_PREFIXES),
    "upgrade.before_commit_decision",
)
RECOVERY_FAULT_POINTS = (
    *(
        f"{prefix}.after_pending_fsync_before_publish"
        for prefix in _RECOVERY_JOURNAL_FAULT_PREFIXES
    ),
    *(
        f"{prefix}.after_publish_before_directory_sync"
        for prefix in _RECOVERY_JOURNAL_FAULT_PREFIXES
    ),
    *(f"{prefix}.after_directory_sync" for prefix in _RECOVERY_JOURNAL_FAULT_PREFIXES),
    *(f"{prefix}.before_rename" for prefix in _RECOVERY_RENAME_FAULT_PREFIXES),
    *(
        f"{prefix}.after_rename_before_source_directory_sync"
        for prefix in _RECOVERY_RENAME_FAULT_PREFIXES
    ),
    *(
        f"{prefix}.after_source_directory_sync"
        for prefix in _RECOVERY_RENAME_FAULT_PREFIXES
    ),
    *(
        f"{prefix}.after_destination_directory_sync"
        for prefix in _RECOVERY_RENAME_FAULT_PREFIXES
    ),
)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _strict_json_bytes(value: bytes, label: str) -> object:
    if len(value) > MAX_INPUT_METADATA_BYTES:
        raise DryRunError(f"{label} exceeds the JSON byte bound")

    def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, item in pairs:
            if key in result:
                raise DryRunError(f"{label} contains a duplicate JSON key")
            result[key] = item
        return result

    def reject_constant(token: str) -> object:
        raise DryRunError(f"{label} contains a non-finite JSON value: {token}")

    def reject_float(token: str) -> object:
        raise DryRunError(f"{label} contains a forbidden floating JSON number: {token}")

    def bounded_integer(token: str) -> int:
        try:
            parsed = int(token, 10)
        except ValueError as error:
            raise DryRunError(f"{label} contains an invalid JSON integer") from error
        if abs(parsed) > MAX_JSON_INTEGER:
            raise DryRunError(f"{label} contains an out-of-range JSON integer")
        return parsed

    def validate_lexical_depth(source: str) -> None:
        depth = 0
        quoted = False
        escaped = False
        for character in source:
            if quoted:
                if escaped:
                    escaped = False
                elif character == "\\":
                    escaped = True
                elif character == '"':
                    quoted = False
                continue
            if character == '"':
                quoted = True
            elif character in "[{":
                depth += 1
                if depth > MAX_JSON_DEPTH:
                    raise DryRunError(f"{label} exceeds the JSON depth bound")
            elif character in "]}":
                depth -= 1

    def validate_materialized(document: object) -> None:
        pending = [document]
        nodes = 0
        while pending:
            item = pending.pop()
            nodes += 1
            if nodes > MAX_JSON_NODES:
                raise DryRunError(f"{label} exceeds the JSON node bound")
            if isinstance(item, str):
                if len(item.encode("utf-8")) > MAX_JSON_STRING_BYTES:
                    raise DryRunError(f"{label} exceeds the JSON string bound")
            elif isinstance(item, dict):
                for key, nested in item.items():
                    if len(key.encode("utf-8")) > MAX_JSON_KEY_BYTES:
                        raise DryRunError(f"{label} exceeds the JSON key bound")
                    pending.append(nested)
            elif isinstance(item, list):
                pending.extend(item)

    try:
        source = value.decode("utf-8")
        validate_lexical_depth(source)
        document = json.loads(
            source,
            object_pairs_hook=object_pairs,
            parse_constant=reject_constant,
            parse_float=reject_float,
            parse_int=bounded_integer,
        )
        validate_materialized(document)
        return document
    except (UnicodeError, json.JSONDecodeError, RecursionError, ValueError) as error:
        raise DryRunError(f"{label} is not canonical UTF-8 JSON") from error


def _strict_json_file(path: Path, label: str) -> object:
    try:
        value = path.read_bytes()
    except OSError as error:
        raise DryRunError(f"{label} cannot be read") from error
    return _strict_json_bytes(value, label)


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _is_reparse(details: os.stat_result) -> bool:
    flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(flag and int(getattr(details, "st_file_attributes", 0)) & flag)


def _same_path(left: Path, right: Path) -> bool:
    return os.path.normcase(str(left)) == os.path.normcase(str(right))


def _is_within(path: Path, root: Path) -> bool:
    try:
        return os.path.commonpath((
            os.path.normcase(str(path)), os.path.normcase(str(root)),
        )) == os.path.normcase(str(root))
    except ValueError:
        return False


def _lexical_absolute(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def _existing_chain(path: Path) -> tuple[Path, ...]:
    absolute = _lexical_absolute(path)
    chain = [absolute]
    while not _same_path(chain[-1], chain[-1].parent):
        chain.append(chain[-1].parent)
    return tuple(reversed(chain))


def _plain_existing_directory(path: Path, label: str) -> Path:
    absolute = _lexical_absolute(path)
    for component in _existing_chain(absolute):
        try:
            details = component.lstat()
        except OSError as error:
            raise DryRunError(f"{label} has an unavailable lexical ancestor") from error
        if (
            not stat.S_ISDIR(details.st_mode)
            or component.is_symlink()
            or _is_reparse(details)
        ):
            raise DryRunError(f"{label} has a symlink, junction or reparse ancestor")
    try:
        resolved = absolute.resolve(strict=True)
    except OSError as error:
        raise DryRunError(f"{label} cannot be resolved strictly") from error
    if not _same_path(absolute, resolved):
        raise DryRunError(f"{label} lexical and resolved paths differ")
    return absolute


def _plain_existing_file(path: Path, label: str) -> tuple[Path, os.stat_result]:
    absolute = _lexical_absolute(path)
    _plain_existing_directory(absolute.parent, f"{label} parent")
    try:
        details = absolute.lstat()
    except OSError as error:
        raise DryRunError(f"{label} is unavailable") from error
    if (
        not stat.S_ISREG(details.st_mode)
        or absolute.is_symlink()
        or _is_reparse(details)
        or int(details.st_nlink) != 1
    ):
        raise DryRunError(f"{label} is not a single-link plain regular file")
    try:
        resolved = absolute.resolve(strict=True)
    except OSError as error:
        raise DryRunError(f"{label} cannot be resolved strictly") from error
    if not _same_path(absolute, resolved):
        raise DryRunError(f"{label} lexical and resolved paths differ")
    return absolute, details


def _allowed_write_roots() -> tuple[Path, ...]:
    candidates = (PROJECT_ROOT / ".analysis", Path(tempfile.gettempdir()))
    roots: list[Path] = []
    for candidate in candidates:
        try:
            lexical = _plain_existing_directory(candidate, "dry-run capability anchor")
        except DryRunError:
            continue
        roots.append(lexical)
    return tuple(roots)


_PROCESS_LEASE_OWNERS_LOCK = threading.Lock()
_PROCESS_LEASE_OWNED_PATHS: set[str] = set()

_EXECUTE_BOUND_ARCHIVE_ROUTES = frozenset({
    "candidate_integration_validate",
    "candidate_integration_extract",
    "candidate_worker_validate",
    "candidate_worker_extract",
    "rollback_integration_inspect",
    "rollback_integration_extract",
    "rollback_worker_inspect",
    "rollback_worker_extract",
})


def _claim_process_lease_path(path: Path) -> bool:
    key = os.path.normcase(str(path))
    with _PROCESS_LEASE_OWNERS_LOCK:
        if key in _PROCESS_LEASE_OWNED_PATHS:
            return False
        _PROCESS_LEASE_OWNED_PATHS.add(key)
        return True


def _release_process_lease_path(path: Path) -> None:
    key = os.path.normcase(str(path))
    with _PROCESS_LEASE_OWNERS_LOCK:
        _PROCESS_LEASE_OWNED_PATHS.discard(key)


def _lock_process_lease_descriptor(descriptor: int) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        return
    import fcntl

    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_process_lease_descriptor(descriptor: int) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(descriptor, fcntl.LOCK_UN)


def _repair_process_lease_anchor_while_locked(
    *,
    anchor: BoundFile,
    anchor_identity: NodeIdentity,
    parent: DirectoryAuthority,
    anchor_relative: RelativePath,
    lock_stream: BinaryIO,
    expected: bytes,
) -> NodeIdentity:
    """Repair stale simulator-owned lease metadata under its exact inode lock.

    Only the reserved name derived by ``_process_lease_binding`` reaches this
    helper.  Divergent unlocked single-link content at that exact name is
    stale simulator lease state; no arbitrary caller-selected name is repaired.
    """

    try:
        opened_before = anchor.identity()
        named_before = parent.entry_identity(anchor_relative)
        if (
            not opened_before.same_object(anchor_identity)
            or opened_before.backend != anchor_identity.backend
            or not named_before.same_object(anchor_identity)
            or named_before.backend != anchor_identity.backend
            or opened_before.kind != "regular_file"
            or opened_before.link_count != 1
        ):
            raise DryRunError(
                "dry-run process lease stale anchor identity differs"
            )
        lock_stream.seek(0)
        observed = lock_stream.read(len(expected) + 1)
        if observed == expected:
            return opened_before
        lock_stream.seek(0)
        written = lock_stream.write(expected)
        if written != len(expected):
            raise OSError("lease anchor rewrite was short")
        lock_stream.truncate(len(expected))
        lock_stream.flush()
        os.fsync(lock_stream.fileno())
        parent.fsync()
        opened_after = anchor.identity()
        named_after = parent.entry_identity(anchor_relative)
        lock_stream.seek(0)
        repaired = lock_stream.read(len(expected) + 1)
        if (
            repaired != expected
            or not opened_after.same_object(anchor_identity)
            or opened_after.backend != anchor_identity.backend
            or not named_after.same_object(anchor_identity)
            or named_after.backend != anchor_identity.backend
            or opened_after.kind != "regular_file"
            or opened_after.link_count != 1
            or opened_after.size != len(expected)
        ):
            raise DryRunError(
                "dry-run process lease stale anchor repair was not proved"
            )
        return opened_after
    except DryRunError:
        raise
    except (AuthorityError, OSError, ValueError) as error:
        raise DryRunError(
            "dry-run process lease stale anchor repair failed"
        ) from error


def _same_file_identity(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        stat.S_IFMT(left.st_mode) == stat.S_IFMT(right.st_mode)
        and int(left.st_dev) == int(right.st_dev)
        and int(left.st_ino) == int(right.st_ino)
    )


@dataclass(slots=True, repr=False)
class _ExclusiveProcessLease:
    binding: "_LeaseBinding"
    allowed_root: DirectoryAuthority | None
    allowed_identity: NodeIdentity
    parent: DirectoryAuthority | None
    parent_identity: NodeIdentity
    anchor: BoundFile | None
    anchor_identity: NodeIdentity
    lock_stream: BinaryIO | None
    backend: str

    @property
    def anchor_path(self) -> Path:
        return self.binding.anchor_path

    @property
    def run_root_sha256(self) -> str:
        return self.binding.run_root_sha256

    @property
    def anchor_bytes(self) -> bytes:
        return self.binding.anchor_bytes

    @property
    def descriptor(self) -> int | None:
        stream = self.lock_stream
        if stream is None or stream.closed:
            return None
        try:
            return stream.fileno()
        except (OSError, ValueError):
            return None

    def assert_held(self) -> None:
        allowed = self.allowed_root
        parent = self.parent
        anchor = self.anchor
        descriptor = self.descriptor
        if allowed is None or parent is None or anchor is None or descriptor is None:
            raise DryRunError("dry-run process lease is not held")
        try:
            allowed_identity = allowed.identity()
            parent_identity = parent.identity()
            if self.binding.parent_relative.parts:
                parent_entry = allowed.entry_identity(
                    self.binding.parent_relative
                )
            else:
                parent_entry = allowed_identity
            opened = anchor.identity()
            named = parent.entry_identity(
                RelativePath((self.binding.anchor_leaf,))
            )
            if (
                not allowed_identity.same_object(self.allowed_identity)
                or allowed_identity.backend != self.allowed_identity.backend
                or not parent_identity.same_object(self.parent_identity)
                or not parent_entry.same_object(self.parent_identity)
                or not opened.same_object(self.anchor_identity)
                or not named.same_object(self.anchor_identity)
                or opened.backend != self.anchor_identity.backend
                or opened.kind != "regular_file"
                or opened.link_count != 1
                or opened.size != len(self.binding.anchor_bytes)
                or anchor.read_bounded(
                    len(self.binding.anchor_bytes)
                ) != self.binding.anchor_bytes
            ):
                raise DryRunError("dry-run process lease identity was lost")
        except DryRunError:
            raise
        except (AuthorityError, OSError, ValueError) as error:
            raise DryRunError("dry-run process lease identity was lost") from error

    def duplicate_allowed_root(self) -> DirectoryAuthority:
        self.assert_held()
        allowed = self.allowed_root
        if allowed is None:
            raise DryRunError("dry-run process lease is not held")
        duplicate: DirectoryAuthority | None = None
        try:
            duplicate = allowed.duplicate()
            identity = duplicate.identity()
            if (
                not identity.same_object(self.allowed_identity)
                or identity.backend != self.allowed_identity.backend
            ):
                raise DryRunError("dry-run allowed-root duplicate identity differs")
            return duplicate
        except BaseException:
            if duplicate is not None and not duplicate.closed:
                try:
                    duplicate.close()
                except AuthorityError:
                    pass
            raise

    def evidence(self) -> dict[str, object]:
        self.assert_held()
        return {
            "acquisition": "non_blocking",
            "anchor_content_sha256": _sha_bytes(self.anchor_bytes),
            "anchor_name": self.binding.anchor_leaf,
            "anchor_namespace": PROCESS_LEASE_ANCHOR_NAMESPACE,
            "divergent_unlocked_reserved_anchor_repaired_under_lock": True,
            "anchor_identity": self.anchor_identity.to_mapping(),
            "allowed_root_identity": self.allowed_identity.to_mapping(),
            "backend": self.backend,
            "contract": PROCESS_LEASE_CONTRACT,
            "descriptor_inheritable": False,
            "descriptor_relative_anchor": True,
            "allowed_root_bootstrap_mode": (
                self.allowed_root.absolute_bootstrap_mode
                if self.allowed_root is not None
                else None
            ),
            "allowed_root_bootstrap_proven": (
                self.allowed_root is not None
                and self.allowed_root.absolute_ancestor_resolution_no_reparse_proven
                and self.allowed_root.authority_root_identity_pinned
            ),
            "anchor_identity_reproved": True,
            "parent_identity_reproved": True,
            "held_before_run_root_mutation": True,
            "os_released_after_abrupt_exit": True,
            "parent_identity": self.parent_identity.to_mapping(),
            "run_root_sha256": self.run_root_sha256,
        }

    def release(self) -> None:
        stream = self.lock_stream
        if stream is None:
            return
        descriptor = self.descriptor
        self.lock_stream = None
        anchor = self.anchor
        parent = self.parent
        allowed = self.allowed_root
        self.anchor = None
        self.parent = None
        self.allowed_root = None
        release_errors: list[BaseException] = []
        unlock_succeeded = False
        stream_close_succeeded = False
        try:
            if descriptor is not None:
                _unlock_process_lease_descriptor(descriptor)
                unlock_succeeded = True
        except (OSError, ImportError) as error:
            release_errors.append(error)
        finally:
            try:
                stream.close()
                stream_close_succeeded = True
            except (OSError, ValueError) as error:
                release_errors.append(error)
            for owned in (anchor, parent, allowed):
                if owned is None or owned.closed:
                    continue
                try:
                    owned.close()
                except AuthorityError as error:
                    release_errors.append(error)
            if unlock_succeeded or stream_close_succeeded:
                _release_process_lease_path(self.anchor_path)
        if release_errors:
            raise DryRunError("dry-run process lease release failed") from (
                release_errors[0]
                if len(release_errors) == 1
                else BaseExceptionGroup(
                    "dry-run process lease release failures", release_errors
                )
            )


@dataclass(frozen=True, slots=True)
class _LeaseBinding:
    lexical_root: Path
    allowed_root_path: Path
    run_relative: RelativePath
    parent_relative: RelativePath
    anchor_leaf: str
    run_root_sha256: str
    anchor_bytes: bytes

    @property
    def anchor_path(self) -> Path:
        return self.lexical_root.parent / self.anchor_leaf


def _process_lease_binding(value: Path) -> _LeaseBinding:
    lexical_root = _lexical_absolute(Path(value))
    parent = _plain_existing_directory(
        lexical_root.parent, "dry-run run-root parent"
    )
    candidates = tuple(
        root for root in _allowed_write_roots() if _is_within(parent, root)
    )
    if not candidates:
        raise DryRunError("dry-run run root is outside temp and repository analysis")
    allowed_root_path = max(candidates, key=lambda item: len(item.parts))
    try:
        run_relative = RelativePath(
            tuple(lexical_root.relative_to(allowed_root_path).parts)
        )
    except (AuthorityError, ValueError) as error:
        raise DryRunError("dry-run run root cannot be made authority-relative") from error
    if not run_relative.parts:
        raise DryRunError("dry-run run root cannot equal its allowed authority root")
    parent_relative = run_relative.parent
    normalized = os.path.normcase(str(lexical_root))
    run_root_sha256 = _sha_bytes(normalized.encode("utf-8"))
    anchor_leaf = f".stv3-release-install-{run_root_sha256}.lease"
    anchor_bytes = _canonical({
        "contract": PROCESS_LEASE_CONTRACT,
        "run_root_sha256": run_root_sha256,
    })
    return _LeaseBinding(
        lexical_root=lexical_root,
        allowed_root_path=allowed_root_path,
        run_relative=run_relative,
        parent_relative=parent_relative,
        anchor_leaf=anchor_leaf,
        run_root_sha256=run_root_sha256,
        anchor_bytes=anchor_bytes,
    )


def _acquire_process_lease(value: Path) -> _ExclusiveProcessLease:
    """Acquire one run-root lease immediately, before any run-state mutation."""

    binding = _process_lease_binding(value)
    if not _claim_process_lease_path(binding.anchor_path):
        raise DryRunError("dry-run process lease is already held")
    allowed: DirectoryAuthority | None = None
    parent: DirectoryAuthority | None = None
    anchor: BoundFile | None = None
    lock_stream: BinaryIO | None = None
    anchor_identity: NodeIdentity | None = None
    created = False
    locked = False
    try:
        allowed = DirectoryAuthority.open_absolute(binding.allowed_root_path)
        allowed_identity = allowed.identity()
        parent = allowed.open_directory(binding.parent_relative)
        parent_identity = parent.identity()
        if binding.parent_relative.parts:
            named_parent = allowed.entry_identity(binding.parent_relative)
            if not named_parent.same_object(parent_identity):
                raise DryRunError("dry-run lease parent identity differs")
        elif not allowed_identity.same_object(parent_identity):
            raise DryRunError("dry-run lease parent identity differs")
        anchor_relative = RelativePath((binding.anchor_leaf,))
        try:
            anchor = parent.create_file(anchor_relative, mode=0o600)
            created = True
        except AuthorityConflict:
            anchor = parent.open_file(anchor_relative, writable=True)
        except AuthorityPostCreateError as error:
            created = True
            anchor_identity = error.created_identity
            raise
        anchor_identity = anchor.identity()
        if created:
            anchor.write_all(binding.anchor_bytes)
            anchor.flush()
            if os.name != "nt":
                os.fchmod(anchor.native_handle, 0o600)
                anchor.flush()
            parent.fsync()
        lock_stream = anchor.duplicate_binary("r+b")
        descriptor = lock_stream.fileno()
        os.set_inheritable(descriptor, False)
        _lock_process_lease_descriptor(descriptor)
        locked = True
        if not created:
            anchor_identity = _repair_process_lease_anchor_while_locked(
                anchor=anchor,
                anchor_identity=anchor_identity,
                parent=parent,
                anchor_relative=anchor_relative,
                lock_stream=lock_stream,
                expected=binding.anchor_bytes,
            )
        lease = _ExclusiveProcessLease(
            binding=binding,
            allowed_root=allowed,
            allowed_identity=allowed_identity,
            parent=parent,
            parent_identity=parent_identity,
            anchor=anchor,
            anchor_identity=anchor_identity,
            lock_stream=lock_stream,
            backend="windows_msvcrt" if os.name == "nt" else "posix_flock",
        )
        lease.assert_held()
        allowed = None
        parent = None
        anchor = None
        lock_stream = None
        return lease
    except BaseException as error:
        busy_codes = {
            errno.EACCES,
            errno.EAGAIN,
            errno.EDEADLK,
            getattr(errno, "EWOULDBLOCK", errno.EAGAIN),
        }
        if isinstance(error, (BlockingIOError, PermissionError)) or (
            isinstance(error, OSError) and error.errno in busy_codes
        ):
            primary: BaseException = DryRunError(
                "dry-run process lease is already held"
            )
        elif isinstance(error, DryRunError):
            primary = error
        elif isinstance(error, (AuthorityError, ImportError, OSError)):
            primary = DryRunError("dry-run process lease acquisition failed")
        else:
            primary = error

        cleanup_failures: list[BaseException] = []
        cleanup_lock_acquired = locked
        os_lock_released_or_never_acquired = not locked
        lock_stream_closed = lock_stream is None
        anchor_relative = RelativePath((binding.anchor_leaf,))
        if (
            anchor is None
            and created
            and anchor_identity is not None
            and parent is not None
        ):
            try:
                candidate = parent.open_file(anchor_relative, writable=True)
                candidate_identity = candidate.identity()
                if (
                    not candidate_identity.same_object(anchor_identity)
                    or candidate_identity.backend != anchor_identity.backend
                ):
                    candidate.close()
                    raise DryRunError(
                        "dry-run process lease anchor replacement was preserved"
                    )
                anchor = candidate
            except AuthorityNotFound:
                created = False
            except (AuthorityError, DryRunError) as cleanup_error:
                cleanup_failures.append(cleanup_error)
        if lock_stream is None and created and anchor is not None:
            try:
                lock_stream = anchor.duplicate_binary("r+b")
                lock_stream_closed = False
            except AuthorityError as cleanup_error:
                cleanup_failures.append(cleanup_error)
        if lock_stream is not None:
            try:
                descriptor = lock_stream.fileno()
            except (OSError, ValueError) as cleanup_error:
                descriptor = None
                cleanup_failures.append(cleanup_error)
            if locked and descriptor is None:
                cleanup_failures.append(
                    OSError("lease lock descriptor unavailable")
                )
                cleanup_lock_acquired = False
            elif not locked and created and descriptor is not None:
                try:
                    _lock_process_lease_descriptor(descriptor)
                    cleanup_lock_acquired = True
                except (ImportError, OSError) as cleanup_error:
                    if isinstance(cleanup_error, OSError) and (
                        isinstance(cleanup_error, (BlockingIOError, PermissionError))
                        or cleanup_error.errno in busy_codes
                    ):
                        cleanup_lock_acquired = False
                    else:
                        cleanup_failures.append(cleanup_error)
                        os_lock_released_or_never_acquired = False
        if anchor is not None and not anchor.closed and cleanup_lock_acquired:
            # On Windows the ordinary bound handle deliberately denies delete
            # sharing.  The duplicate stream retains the exact inode lock, so
            # close only the extra authority handle before exact removal.
            try:
                anchor.close()
            except AuthorityError as cleanup_error:
                cleanup_failures.append(cleanup_error)
        if (
            created
            and cleanup_lock_acquired
            and anchor_identity is not None
            and parent is not None
            and os.name != "nt"
        ):
            try:
                parent.remove_owned_tree(
                    RelativePath((binding.anchor_leaf,)),
                    expected_root=anchor_identity,
                    maximum_entries=1,
                )
                try:
                    parent.entry_identity(RelativePath((binding.anchor_leaf,)))
                except AuthorityNotFound:
                    pass
                else:
                    raise DryRunError(
                        "dry-run process lease anchor cleanup left a destination"
                    )
                parent.fsync()
            except (AuthorityError, DryRunError) as cleanup_error:
                cleanup_failures.append(cleanup_error)
        elif created and cleanup_lock_acquired and anchor_identity is None:
            cleanup_failures.append(
                DryRunError("dry-run process lease created identity is unavailable")
            )
        if lock_stream is not None:
            if cleanup_lock_acquired:
                try:
                    _unlock_process_lease_descriptor(lock_stream.fileno())
                    os_lock_released_or_never_acquired = True
                except (ImportError, OSError, ValueError) as cleanup_error:
                    cleanup_failures.append(cleanup_error)
            try:
                lock_stream.close()
                lock_stream_closed = True
            except (OSError, ValueError) as cleanup_error:
                cleanup_failures.append(cleanup_error)
        if anchor is not None and not anchor.closed:
            try:
                anchor.close()
            except AuthorityError as cleanup_error:
                cleanup_failures.append(cleanup_error)
        for owned in (parent, allowed):
            if owned is not None and not owned.closed:
                try:
                    owned.close()
                except AuthorityError as cleanup_error:
                    cleanup_failures.append(cleanup_error)
        if os_lock_released_or_never_acquired and lock_stream_closed:
            _release_process_lease_path(binding.anchor_path)
        if cleanup_failures:
            raise DryRunError(
                "dry-run process lease acquisition cleanup failed"
            ) from BaseExceptionGroup(
                "dry-run process lease acquisition and cleanup failures",
                [primary, *cleanup_failures],
            )
        if not (os_lock_released_or_never_acquired and lock_stream_closed):
            raise DryRunError(
                "dry-run process lease acquisition cleanup was not released"
            ) from primary
        if primary is error:
            raise
        raise primary from error


def _open_authority_run_context(
    value: Path,
    *,
    existing: bool,
    process_lease: _ExclusiveProcessLease,
) -> _AuthorityRunContext:
    """Bind the allowed root, run root and no-replace probe to open handles."""

    process_lease.assert_held()
    binding = process_lease.binding
    lexical_root = _lexical_absolute(Path(value))
    if not _same_path(lexical_root, binding.lexical_root):
        raise DryRunError("dry-run process lease and authority root differ")
    allowed_path = binding.allowed_root_path
    run_relative = binding.run_relative

    allowed: DirectoryAuthority | None = None
    output: ReleaseInstallOutputAuthority | None = None
    root: DirectoryAuthority | None = None
    reads: OwnedOutputReadAuthority | None = None
    created = False
    succeeded = False
    try:
        allowed = process_lease.duplicate_allowed_root()
        allowed_identity = allowed.identity()
        # This bounded probe is below the already-open allowed-root handle and
        # occurs while the process lease is held, before any run-root mutation.
        probe_open_authority(allowed, allowed_identity)
        process_lease.assert_held()
        if existing:
            output = ReleaseInstallOutputAuthority.open_run_root(
                allowed,
                run_relative,
            )
        else:
            output = ReleaseInstallOutputAuthority.create_run_root(
                allowed,
                run_relative,
                mode=0o700,
            )
            created = True
        root = allowed.open_directory(run_relative)
        root_identity = root.identity()
        output_identity = output.reprove_run_root()
        if (
            not output_identity.same_object(root_identity)
            or output_identity.backend != root_identity.backend
        ):
            raise DryRunError("dry-run output and root authorities differ")
        atomic_capability = probe_open_authority(root, root_identity)
        atomic = DescriptorRelativeAtomicAdapter(
            authority=root,
            root_identity=root_identity,
            capability=atomic_capability,
        )
        reads = OwnedOutputReadAuthority(root, output)
        context = _AuthorityRunContext(
            lexical_root=lexical_root,
            allowed_root_path=allowed_path,
            run_relative=run_relative,
            allowed_root=allowed,
            output=output,
            root=root,
            root_identity=root_identity,
            atomic_capability=atomic_capability,
            atomic=atomic,
            reads=reads,
            created=created,
            expected_bound_archive_routes=0 if existing else 8,
            operation_kind="recover" if existing else "execute",
            process_lease=process_lease,
        )
        process_lease.assert_held()
        context.reprove()
        succeeded = True
        return context
    except DryRunError:
        raise
    except OutputAuthorityConflict as error:
        raise DryRunError("dry-run run root must be new and absent") from error
    except (
        AuthorityError,
        AtomicNoReplaceError,
        OutputAuthorityError,
        OwnedOutputReadError,
    ) as error:
        raise DryRunError("dry-run authority bootstrap failed closed") from error
    finally:
        if not succeeded:
            cleanup_error: BaseException | None = None
            if reads is not None:
                try:
                    reads.close()
                except OwnedOutputReadError as error:
                    cleanup_error = error
            if root is not None:
                try:
                    root.close()
                except AuthorityError as error:
                    cleanup_error = error
            if output is not None:
                try:
                    if created:
                        output.remove_created_run_root(
                            maximum_entries=MAX_ARCHIVE_MEMBER_COUNT + 64
                        )
                    else:
                        output.close()
                except OutputAuthorityError as error:
                    if cleanup_error is None:
                        cleanup_error = error
            if allowed is not None and not allowed.closed:
                try:
                    allowed.close()
                except AuthorityError as error:
                    if cleanup_error is None:
                        cleanup_error = error
            if cleanup_error is not None:
                raise DryRunError(
                    "dry-run authority bootstrap cleanup failed"
                ) from cleanup_error


def _assert_mutation_path(path: Path, run_root: Path) -> None:
    absolute = Path(os.path.abspath(path))
    if not _is_within(absolute, run_root) or _same_path(absolute, run_root):
        raise DryRunError("mutation path escapes the dry-run capability root")


def _mkdir(
    path: Path,
    run_root: Path,
    authority_context: _AuthorityRunContext,
) -> None:
    _assert_mutation_path(path, run_root)
    relative = authority_context.relative(path)
    try:
        authority_context.output.mkdir(relative, mode=0o755)
        authority_context.reprove()
    except (AuthorityError, OutputAuthorityError) as error:
        # A post-create mode/fsync/open failure may leave the identity-owned
        # directory present.  Do not retry or infer absence from the exception.
        try:
            observed = authority_context.root.entry_identity(relative)
        except AuthorityNotFound:
            observed = None
        except AuthorityError as proof_error:
            raise DryRunError(
                "dry-run directory outcome could not be classified"
            ) from proof_error
        if observed is not None:
            raise DryRunError(
                "dry-run directory was created but its post-create proof failed"
            ) from error
        raise DryRunError("dry-run directory creation failed closed") from error


def _mkdir_parents(
    path: Path,
    tree_root: Path,
    run_root: Path,
    authority_context: _AuthorityRunContext,
) -> None:
    if _same_path(path, tree_root):
        return
    _assert_mutation_path(path, run_root)
    try:
        relative = path.relative_to(tree_root)
    except ValueError as error:
        raise DryRunError("extraction parent escapes its tree root") from error
    current = tree_root
    for part in relative.parts:
        current = current / part
        try:
            identity = authority_context.root.entry_identity(
                authority_context.relative(current)
            )
        except AuthorityNotFound:
            _mkdir(current, run_root, authority_context)
        except AuthorityError as error:
            raise DryRunError("extraction parent cannot be reproved") from error
        else:
            if identity.kind != "directory":
                raise DryRunError("extraction parent is not a directory")


def _sync_directory(path: Path) -> bool:
    if os.name == "nt":
        return False
    descriptor: int | None = None
    try:
        descriptor = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        os.fsync(descriptor)
    except OSError as error:
        raise DryRunError("dry-run directory fsync failed") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
    return True


def _raise_classified_output_postpublication_failure(
    *,
    label: str,
    relative: RelativePath,
    error: OutputAuthorityPostPublicationError,
    authority_context: _AuthorityRunContext,
) -> None:
    """Translate one typed publication outcome without deleting a replacement."""

    if error.destination != relative:
        raise DryRunError(
            f"{label} post-publication destination proof differs"
        ) from error
    try:
        observed = authority_context.root.entry_identity(relative)
    except AuthorityNotFound:
        observed = None
    except AuthorityError as proof_error:
        raise DryRunError(
            f"{label} post-publication cleanup outcome could not be classified"
        ) from proof_error
    authority_context.reprove()
    if error.cleanup_succeeded:
        if observed is not None:
            raise DryRunError(
                f"{label} post-publication cleanup proof was contradicted"
            ) from error
        raise DryRunError(
            f"{label} failed closed after exact post-publication cleanup"
        ) from error
    if observed is None:
        raise DryRunError(
            f"{label} is absent but post-publication cleanup was not durable"
        ) from error
    if observed.same_object(error.published_identity):
        raise DryRunError(
            f"{label} remains published after post-publication cleanup failed"
        ) from error
    raise DryRunError(
        f"{label} replacement was preserved after post-publication cleanup failed"
    ) from error


def _write_exclusive(
    path: Path,
    data: bytes,
    *,
    mode: int,
    run_root: Path,
    authority_context: _AuthorityRunContext,
) -> None:
    _assert_mutation_path(path, run_root)
    _mkdir_parents(path.parent, run_root, run_root, authority_context)
    relative = authority_context.relative(path)
    try:
        publication = authority_context.output.write_bytes_noreplace(
            relative,
            data,
            mode=mode,
            forbidden_markers=(),
        )
        if publication.backend != authority_context.atomic_capability.publication_backend:
            raise DryRunError("dry-run output publication backend changed")
        authority_context.reprove()
    except OutputAuthorityPostPublicationError as error:
        _raise_classified_output_postpublication_failure(
            label="dry-run output",
            relative=relative,
            error=error,
            authority_context=authority_context,
        )
    except OutputAuthorityError as error:
        try:
            authority_context.root.entry_identity(relative)
        except AuthorityNotFound:
            pass
        except AuthorityError as proof_error:
            raise DryRunError(
                "dry-run output publication outcome could not be classified"
            ) from proof_error
        else:
            raise DryRunError(
                "dry-run output may be persistent after a publication failure"
            ) from error
        raise DryRunError("dry-run output publication failed closed") from error


def _write_member_exclusive(
    path: Path,
    source: BinaryIO,
    *,
    record: FileRecord,
    run_root: Path,
    authority_context: _AuthorityRunContext,
    label: str,
) -> None:
    """Stream one proved archive body into a new, exclusive destination."""

    _assert_mutation_path(path, run_root)
    _mkdir_parents(path.parent, run_root, run_root, authority_context)
    relative = authority_context.relative(path)
    try:
        publication = authority_context.output.stream_noreplace(
            relative,
            source,
            expected_size=record.size,
            maximum_size=MAX_FILE_BYTES,
            expected_sha256=record.sha256,
            forbidden_markers=PRIVATE_KEY_MARKERS,
            mode=record.mode,
            chunk_bytes=ARCHIVE_MEMBER_CHUNK_BYTES,
        )
        if publication.backend != authority_context.atomic_capability.publication_backend:
            raise DryRunError(f"{label} publication backend changed")
        authority_context.reprove()
    except OutputAuthorityPostPublicationError as error:
        _raise_classified_output_postpublication_failure(
            label=label,
            relative=relative,
            error=error,
            authority_context=authority_context,
        )
    except OutputAuthorityError as error:
        try:
            authority_context.root.entry_identity(relative)
        except AuthorityNotFound:
            pass
        except AuthorityError as proof_error:
            raise DryRunError(
                f"{label} publication outcome could not be classified"
            ) from proof_error
        else:
            raise DryRunError(
                f"{label} may be persistent after a publication failure"
            ) from error
        raise DryRunError(f"{label} failed closed before publication") from error


def _write_json_exclusive(
    path: Path,
    value: Mapping[str, object],
    run_root: Path,
    authority_context: _AuthorityRunContext,
) -> None:
    _write_exclusive(
        path,
        _canonical(value) + b"\n",
        mode=0o644,
        run_root=run_root,
        authority_context=authority_context,
    )


def _assert_destination_absent(path: Path, label: str) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        return
    except OSError as error:
        raise DryRunError(f"{label} cannot be inspected lexically") from error
    raise DryRunError(f"{label} was observed to exist before rename")


def _open_witness(path: Path, label: str) -> FileWitness:
    resolved, before = _plain_existing_file(path, label)
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(str(resolved), flags)
    digest = hashlib.sha256()
    try:
        opened = os.fstat(descriptor)
        if (
            int(opened.st_dev) != int(before.st_dev)
            or int(opened.st_ino) != int(before.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or int(opened.st_nlink) != 1
        ):
            raise DryRunError(f"{label} changed before it was read")
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        int(after.st_dev) != int(opened.st_dev)
        or int(after.st_ino) != int(opened.st_ino)
        or int(after.st_size) != int(opened.st_size)
        or int(after.st_mtime_ns) != int(opened.st_mtime_ns)
        or int(after.st_nlink) != 1
    ):
        raise DryRunError(f"{label} changed while it was read")
    return FileWitness(
        path=resolved,
        device=int(after.st_dev),
        inode=int(after.st_ino),
        size=int(after.st_size),
        modified_ns=int(after.st_mtime_ns),
        sha256=digest.hexdigest(),
    )


def _spool_input(
    source_path: Path,
    destination: Path,
    *,
    label: str,
    maximum_bytes: int,
    run_root: Path,
    authority_context: _AuthorityRunContext,
    fault_injector: FaultInjector | None,
    fault_prefix: str,
) -> tuple[FileWitness, FileWitness]:
    source, lexical_before = _plain_existing_file(source_path, label)
    _assert_mutation_path(destination, run_root)
    _mkdir_parents(
        destination.parent,
        run_root,
        run_root,
        authority_context,
    )
    relative_destination = authority_context.relative(destination)
    source_authority: DirectoryAuthority | None = None
    registry: BoundInputRegistry | None = None
    registered = False
    digest = hashlib.sha256()
    total = 0
    try:
        source_authority = DirectoryAuthority.open_absolute(source.parent)
        source_relative = RelativePath((source.name,))
        with source_authority.open_file(source_relative) as bound:
            source_identity = bound.identity()
            if (
                source_identity.kind != "regular_file"
                or source_identity.link_count != 1
                or source_identity.size > maximum_bytes
            ):
                raise DryRunError(f"{label} is outside its bound input contract")
            with bound.duplicate_binary("rb") as source_stream:
                opened = os.fstat(source_stream.fileno())
                if (
                    not _same_file_identity(lexical_before, opened)
                    or not stat.S_ISREG(opened.st_mode)
                    or int(opened.st_nlink) != 1
                    or int(opened.st_size) != source_identity.size
                ):
                    raise DryRunError(f"{label} changed before it was captured")
                while True:
                    chunk = source_stream.read(1024 * 1024)
                    if not isinstance(chunk, bytes):
                        raise DryRunError(f"{label} returned non-binary input")
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > maximum_bytes:
                        raise DryRunError(f"{label} exceeds its spool byte bound")
                    digest.update(chunk)
                opened_after = os.fstat(source_stream.fileno())
                if (
                    not _same_file_identity(opened, opened_after)
                    or int(opened_after.st_size) != total
                    or int(opened_after.st_mtime_ns) != int(opened.st_mtime_ns)
                    or int(opened_after.st_nlink) != 1
                ):
                    raise DryRunError(f"{label} changed while it was captured")
            source_after_capture = bound.identity()
        if (
            not source_after_capture.same_object(source_identity)
            or source_after_capture.size != total
            or source_after_capture.link_count != 1
        ):
            raise DryRunError(f"{label} changed while it was captured")
        declaration = BoundInputDeclaration(
            relative=source_relative,
            identity=source_identity,
            size=total,
            sha256=digest.hexdigest(),
            maximum_bytes=maximum_bytes,
        )
        registry = BoundInputRegistry.seal(
            source_authority,
            {"input": declaration},
            expected_keys=("input",),
        )
        _inject(fault_injector, f"{fault_prefix}.after_source_open")
        with registry.open_stream("input") as source_stream:
            publication = authority_context.output.stream_noreplace(
                relative_destination,
                source_stream,
                expected_size=total,
                maximum_size=maximum_bytes,
                expected_sha256=digest.hexdigest(),
                forbidden_markers=(),
                mode=0o400,
            )
        if publication.backend != authority_context.atomic_capability.publication_backend:
            raise DryRunError("owned spool publication backend changed")
        _inject(
            fault_injector,
            f"{fault_prefix}.after_spool_fsync_before_source_reproof",
        )
        with registry.open_stream("input"):
            pass
        authority_context.reprove()
        original = FileWitness(
            path=source,
            device=source_identity.volume,
            inode=int(source_identity.file_id, 16),
            size=total,
            modified_ns=0,
            sha256=digest.hexdigest(),
        )
        spool = FileWitness(
            path=destination,
            device=publication.identity.volume,
            inode=int(publication.identity.file_id, 16),
            size=publication.identity.size,
            modified_ns=0,
            sha256=publication.stream.sha256,
        )
        if spool.sha256 != original.sha256 or spool.size != original.size:
            raise DryRunError(f"owned {label} spool bytes differ")
        registry_key = os.path.normcase(str(source))
        if registry_key in authority_context.bound_sources:
            raise DryRunError("bound input source was registered twice")
        authority_context.bound_sources[registry_key] = registry
        registered = True
        _inject(fault_injector, f"{fault_prefix}.after_source_reproof")
        return original, spool
    except OutputAuthorityPostPublicationError as error:
        _raise_classified_output_postpublication_failure(
            label=f"owned {label} spool",
            relative=relative_destination,
            error=error,
            authority_context=authority_context,
        )
    except (AuthorityError, BoundInputError, OutputAuthorityError) as error:
        try:
            authority_context.root.entry_identity(relative_destination)
        except AuthorityNotFound:
            pass
        except AuthorityError as proof_error:
            raise DryRunError(
                f"owned {label} spool outcome could not be classified"
            ) from proof_error
        else:
            raise DryRunError(
                f"owned {label} spool may persist after publication failure"
            ) from error
        raise DryRunError(f"{label} could not be bound and spooled") from error
    finally:
        if registry is not None and not registered:
            registry.close()
        if source_authority is not None:
            source_authority.close()


def _seal_output_spools(
    authority_context: _AuthorityRunContext,
    entries: Mapping[str, tuple[FileWitness, int]],
) -> BoundInputRegistry:
    declarations: dict[str, BoundInputDeclaration] = {}
    registry: BoundInputRegistry | None = None
    succeeded = False
    try:
        for key, (witness, maximum_bytes) in entries.items():
            relative = authority_context.relative(witness.path)
            identity = authority_context.root.entry_identity(relative)
            _assert_witness_identity(witness, identity, "owned spool")
            declarations[key] = BoundInputDeclaration(
                relative=relative,
                identity=identity,
                size=witness.size,
                sha256=witness.sha256,
                maximum_bytes=maximum_bytes,
            )
        registry = BoundInputRegistry.seal(
            authority_context.root,
            declarations,
            expected_keys=tuple(entries),
        )
        authority_context.reprove()
        succeeded = True
        return registry
    except (AuthorityError, BoundInputError) as error:
        raise DryRunError("owned spools could not be bound for consumption") from error
    finally:
        if registry is not None and not succeeded:
            try:
                registry.close()
            except BoundInputError as close_error:
                raise DryRunError(
                    "failed owned spool registry could not be closed"
                ) from close_error


def _assert_witness_identity(
    witness: FileWitness,
    identity: NodeIdentity,
    label: str,
) -> None:
    try:
        file_id = int(identity.file_id, 16)
    except (TypeError, ValueError) as error:
        raise DryRunError(f"{label} identity is invalid") from error
    if (
        identity.kind != "regular_file"
        or identity.link_count != 1
        or identity.volume != witness.device
        or file_id != witness.inode
        or identity.size != witness.size
    ):
        raise DryRunError(f"{label} identity differs from its witness")


def _bound_output_archive(
    authority_context: _AuthorityRunContext,
    cache_key: str,
    witness: FileWitness,
) -> BoundInputRegistry:
    """Return one reusable, exact-one-key registry for an owned archive."""

    if not isinstance(cache_key, str) or not cache_key:
        raise TypeError("bound archive cache key must be non-empty text")
    existing = authority_context.bound_outputs.get(cache_key)
    if existing is not None:
        try:
            existing.require_exact_keys(("archive",))
            metadata = existing.metadata("archive")
        except BoundInputError as error:
            raise DryRunError("bound archive registry changed") from error
        if (
            metadata.relative != authority_context.relative(witness.path)
            or metadata.size != witness.size
            or metadata.sha256 != witness.sha256
        ):
            raise DryRunError("bound archive registry seal differs")
        _assert_witness_identity(witness, metadata.identity, "bound archive")
        return existing
    registry = _seal_output_spools(
        authority_context,
        {"archive": (witness, MAX_TOTAL_BYTES)},
    )
    authority_context.bound_outputs[cache_key] = registry
    return registry


def _record_bound_archive_route(
    authority_context: _AuthorityRunContext,
    route: str,
) -> None:
    expected = _EXECUTE_BOUND_ARCHIVE_ROUTES
    if route not in expected or route in authority_context.bound_archive_routes:
        raise DryRunError("bound archive route accounting differs")
    authority_context.bound_archive_routes.add(route)


def _candidate_archive_names_from_bound_evidence(
    registry: BoundInputRegistry,
) -> tuple[str, str]:
    try:
        document = _strict_json_bytes(
            registry.read_bounded(
                "evidence",
                maximum_bytes=MAX_INPUT_METADATA_BYTES,
            ),
            "bound candidate evidence",
        )
    except BoundInputError as error:
        raise DryRunError("bound candidate evidence could not be read") from error
    if not isinstance(document, dict):
        raise DryRunError("bound candidate evidence is not an object")
    integration_version = document.get("integration_version")
    worker_version = document.get("worker_version")
    archives = document.get("archives")
    if (
        not isinstance(integration_version, str)
        or _VERSION.fullmatch(integration_version) is None
        or not isinstance(worker_version, str)
        or _VERSION.fullmatch(worker_version) is None
        or not isinstance(archives, dict)
    ):
        raise DryRunError("bound candidate evidence identity is invalid")
    try:
        integration_name = archives["integration"]["filename"]
        worker_name = archives["worker"]["filename"]
    except (KeyError, TypeError) as error:
        raise DryRunError("bound candidate evidence archive names are absent") from error
    expected_integration = f"storm_tracker_v3_v{integration_version}_deploy.zip"
    expected_worker = f"storm_tracker_worker_v{worker_version}_deploy.tar.gz"
    if integration_name != expected_integration or worker_name != expected_worker:
        raise DryRunError("bound candidate evidence archive names differ")
    return expected_integration, expected_worker


def _reprove_witness(
    witness: FileWitness,
    label: str,
    authority_context: _AuthorityRunContext,
) -> None:
    registry = authority_context.bound_sources.get(
        os.path.normcase(str(witness.path))
    )
    if registry is not None:
        try:
            metadata = registry.metadata("input")
            with registry.open_stream("input"):
                pass
        except BoundInputError as error:
            raise DryRunError(f"{label} changed during the dry run") from error
        if (
            metadata.identity.volume != witness.device
            or int(metadata.identity.file_id, 16) != witness.inode
            or metadata.size != witness.size
            or metadata.sha256 != witness.sha256
        ):
            raise DryRunError(f"{label} differs from its bound input seal")
        return

    relative = authority_context.relative(witness.path)
    digest = hashlib.sha256()
    total = 0
    try:
        with authority_context.root.open_file(relative) as bound:
            before = bound.identity()
            with bound.duplicate_binary("rb") as stream:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not isinstance(chunk, bytes):
                        raise DryRunError(f"{label} returned non-binary bytes")
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > witness.size:
                        raise DryRunError(f"{label} exceeded its sealed size")
                    digest.update(chunk)
            after = bound.identity()
    except AuthorityError as error:
        raise DryRunError(f"{label} could not be reproved by authority") from error
    if (
        not before.same_object(after)
        or after.size != witness.size
        or after.link_count != 1
        or total != witness.size
        or digest.hexdigest() != witness.sha256
    ):
        raise DryRunError(f"{label} changed during the dry run")


def _spool_report(
    run_root: Path,
    source: FileWitness,
    spool: FileWitness,
    authority_context: _AuthorityRunContext,
) -> dict[str, object]:
    _reprove_witness(spool, "owned input spool", authority_context)
    try:
        identity = authority_context.root.entry_identity(
            authority_context.relative(spool.path)
        )
    except AuthorityError as error:
        raise DryRunError("owned input spool identity is unavailable") from error
    if source.size != spool.size or source.sha256 != spool.sha256:
        raise DryRunError("owned input spool no longer matches its source witness")
    return {
        "exclusive_creation": True,
        "fsynced_before_use": True,
        "physical_read_only": os.name != "nt",
        "single_link": identity.link_count == 1,
        "source_sha256": source.sha256,
        "spool_relative_path": spool.path.relative_to(run_root).as_posix(),
        "spool_sha256": spool.sha256,
        "spool_size": spool.size,
    }


def _safe_relative_path(value: str) -> PurePosixPath:
    if (
        not isinstance(value, str)
        or not value
        or "\\" in value
        or "\x00" in value
        or len(value.encode("utf-8")) > MAX_ARCHIVE_PATH_BYTES
    ):
        raise DryRunError("archive contains an invalid path")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or ".." in path.parts
        or "." in path.parts
        or len(path.parts) > MAX_ARCHIVE_PATH_DEPTH
    ):
        raise DryRunError("archive path escapes its package root")
    for part in path.parts:
        if (
            not part
            or len(part.encode("utf-8")) > MAX_ARCHIVE_PART_BYTES
            or part.endswith((".", " "))
            or ":" in part
            or any(ord(character) < 32 for character in part)
            or part.split(".", 1)[0].casefold() in _WINDOWS_RESERVED_BASENAMES
        ):
            raise DryRunError("archive path is not cross-platform canonical")
    if any(part.lower() in _BANNED_DIRECTORY_NAMES for part in path.parts[:-1]):
        raise DryRunError("archive contains a forbidden directory")
    name = path.name.lower()
    if name in _BANNED_FILE_NAMES or name.endswith("~"):
        raise DryRunError("archive contains a forbidden file")
    if name.endswith(_BANNED_FILE_SUFFIXES) or name.endswith(".tar.gz"):
        raise DryRunError("archive contains a forbidden file")
    return path


def _records_from_evidence(raw: object, expected_root: str) -> TreeRecord:
    if not isinstance(raw, dict) or set(raw) != {
        "content_sha256", "file_count", "files", "root_name", "total_bytes"
    }:
        raise DryRunError("candidate evidence tree has an unknown schema")
    if not isinstance(raw["files"], list) or not isinstance(raw["root_name"], str):
        raise DryRunError("candidate evidence tree files are invalid")
    files = tuple(FileRecord.from_mapping(item) for item in raw["files"])
    tree = TreeRecord.create(str(raw["root_name"]), files)
    evidence_content_sha256 = _sha_bytes(_canonical([
        item.to_mapping() for item in tree.files
    ]))
    if (
        type(raw["file_count"]) is not int
        or type(raw["total_bytes"]) is not int
        or not isinstance(raw["content_sha256"], str)
        or raw["file_count"] != len(tree.files)
        or raw["total_bytes"] != tree.total_bytes
        or raw["content_sha256"] != evidence_content_sha256
    ):
        raise DryRunError("candidate evidence tree hash, count or size differs")
    if tree.root_name != expected_root:
        raise DryRunError("candidate evidence uses the wrong package root")
    return tree


def _expected_directories(tree: TreeRecord) -> tuple[str, ...]:
    return tree.directories


def _empty_directories(tree: TreeRecord) -> tuple[str, ...]:
    empty: list[str] = []
    for directory in tree.directories:
        prefix = f"{directory}/"
        if not any(
            item.path.startswith(prefix) for item in tree.files
        ) and not any(
            other != directory and other.startswith(prefix)
            for other in tree.directories
        ):
            empty.append(directory)
    return tuple(empty)


def _materialize_tree_directories(
    tree: TreeRecord,
    target: Path,
    run_root: Path,
    authority_context: _AuthorityRunContext,
) -> None:
    for relative in tree.directories:
        destination = target.joinpath(*PurePosixPath(relative).parts)
        _mkdir_parents(
            destination.parent,
            target,
            run_root,
            authority_context,
        )
        _mkdir(destination, run_root, authority_context)


def _archive_header_limits() -> ArchiveHeaderLimits:
    if type(MAX_TOTAL_BYTES) is not int or MAX_TOTAL_BYTES <= 0:
        raise DryRunError("archive total bound must be a positive integer")
    if type(MAX_FILE_BYTES) is not int or MAX_FILE_BYTES <= 0:
        raise DryRunError("archive member bound must be a positive integer")
    return ArchiveHeaderLimits(
        max_members=MAX_ARCHIVE_MEMBER_COUNT,
        max_path_bytes=MAX_ARCHIVE_PATH_BYTES,
        max_path_part_bytes=MAX_ARCHIVE_PART_BYTES,
        max_path_depth=MAX_ARCHIVE_PATH_DEPTH,
        max_member_uncompressed_bytes=MAX_FILE_BYTES,
        max_total_uncompressed_bytes=MAX_TOTAL_BYTES,
    )


def _extract_bound_archive(
    registry: BoundInputRegistry,
    tree: TreeRecord,
    target: Path,
    run_root: Path,
    authority_context: _AuthorityRunContext,
    *,
    archive_format: str,
    mode_policy: str,
    route: str,
    exact_legacy_sha256: str | None = None,
) -> None:
    """Extract one already-inspected owned archive without reopening its path."""

    bound_tree = _bound_archive_tree(tree)
    expected = {item.path: item for item in tree.files}
    prepared = False
    consumed: list[str] = []
    target_identity: NodeIdentity | None = None
    completed = False

    def sink(archive_record: BoundArchiveFile, source: BinaryIO) -> None:
        nonlocal prepared, target_identity
        record = expected.get(archive_record.path)
        if record is None or archive_record != BoundArchiveFile(
            record.path,
            record.size,
            record.sha256,
            record.mode,
        ):
            raise DryRunError("bound archive sink record differs from its tree seal")
        if archive_record.path in consumed:
            raise DryRunError("bound archive sink delivered a file twice")
        if not prepared:
            _assert_mutation_path(target, run_root)
            try:
                created_target = authority_context.output.mkdir(
                    authority_context.relative(target),
                    mode=0o755,
                )
                target_identity = created_target.identity
                authority_context.reprove()
            except OutputAuthorityPostCreateError as error:
                target_identity = error.created_identity
                raise DryRunError(
                    "bound archive target post-create proof failed"
                ) from error
            except (AuthorityError, OutputAuthorityError) as error:
                raise DryRunError(
                    "bound archive target could not be created and pinned"
                ) from error
            if target_identity.kind != "directory":
                raise DryRunError("bound archive target is not a directory")
            _materialize_tree_directories(
                tree,
                target,
                run_root,
                authority_context,
            )
            prepared = True
        destination = target.joinpath(*PurePosixPath(record.path).parts)
        _write_member_exclusive(
            destination,
            source,
            record=record,
            run_root=run_root,
            authority_context=authority_context,
            label=f"bound {archive_format} extraction",
        )
        consumed.append(record.path)

    try:
        try:
            if archive_format == "zip":
                extraction = extract_zip_to_sink(
                    registry,
                    "archive",
                    bound_tree,
                    sink,
                    mode_policy=mode_policy,
                    limits=_archive_header_limits(),
                )
            elif archive_format == "tar.gz":
                extraction = extract_tar_gz_to_sink(
                    registry,
                    "archive",
                    bound_tree,
                    sink,
                    mode_policy=mode_policy,
                    exact_legacy_sha256=exact_legacy_sha256,
                    limits=_archive_header_limits(),
                )
            else:
                raise DryRunError("bound archive format is unsupported")
        except DryRunError:
            raise
        except (BoundArchiveError, BoundInputError) as error:
            raise DryRunError("bound archive extraction failed closed") from error
        if (
            not prepared
            or tuple(consumed) != tuple(item.path for item in tree.files)
            or extraction.file_count != len(tree.files)
            or extraction.total_bytes != tree.total_bytes
        ):
            raise DryRunError("bound archive extraction summary differs")
        _sync_tree_authority(authority_context, target, tree)
        _record_bound_archive_route(authority_context, route)
        completed = True
    except BaseException as primary:
        if target_identity is not None and not completed:
            try:
                relative_target = authority_context.relative(target)
                try:
                    observed_target = authority_context.root.entry_identity(
                        relative_target
                    )
                except AuthorityNotFound:
                    observed_target = None
                else:
                    if (
                        observed_target.backend != target_identity.backend
                        or not observed_target.same_object(target_identity)
                    ):
                        raise DryRunError(
                            "failed bound archive target identity was replaced"
                        )
                    authority_context.root.remove_owned_tree(
                        relative_target,
                        expected_root=target_identity,
                        maximum_entries=MAX_ARCHIVE_MEMBER_COUNT + 1,
                    )
                if observed_target is not None:
                    try:
                        authority_context.root.entry_identity(relative_target)
                    except AuthorityNotFound:
                        pass
                    else:
                        raise DryRunError(
                            "failed bound archive target still exists after cleanup"
                        )
                authority_context.reprove()
            except (AuthorityError, DryRunError) as cleanup_error:
                combined = BaseExceptionGroup(
                    "bound archive operation and cleanup both failed",
                    (primary, cleanup_error),
                )
                raise DryRunError(
                    "failed bound archive target cleanup failed closed"
                ) from combined
        raise


def _sync_tree_authority(
    authority_context: _AuthorityRunContext,
    root: Path,
    tree: TreeRecord,
) -> bool:
    directories = [root]
    directories.extend(
        root.joinpath(*PurePosixPath(item).parts) for item in tree.directories
    )
    supported = True
    for directory in sorted(
        directories,
        key=lambda item: len(item.parts),
        reverse=True,
    ):
        try:
            supported = authority_context.output.fsync(
                authority_context.relative(directory)
            ) and supported
        except OutputAuthorityError as error:
            raise DryRunError("extracted tree directory fsync failed") from error
    authority_context.reprove()
    return supported


def _fingerprint_tree(root: Path, expected: TreeRecord) -> dict[str, object]:
    if root.is_symlink() or not root.is_dir() or _is_reparse(root.lstat()):
        raise DryRunError("installed tree root is not a plain directory")
    expected_by_path = {item.path: item for item in expected.files}
    seen_files: dict[str, FileRecord] = {}
    seen_directories: set[str] = set()
    owner_ids: set[int] = set()
    def walk_error(error: OSError) -> None:
        raise DryRunError("installed tree cannot be enumerated exactly") from error

    for current, directories, files in os.walk(
        root, followlinks=False, onerror=walk_error
    ):
        current_path = Path(current)
        for name in directories:
            path = current_path / name
            relative = path.relative_to(root).as_posix()
            details = path.lstat()
            if not stat.S_ISDIR(details.st_mode) or path.is_symlink() or _is_reparse(details):
                raise DryRunError("installed tree contains a reparse directory")
            if os.name != "nt" and stat.S_IMODE(details.st_mode) != 0o755:
                raise DryRunError("installed directory mode differs from normalized policy")
            seen_directories.add(relative)
            owner_ids.add(int(getattr(details, "st_uid", 0)))
        for name in files:
            path = current_path / name
            relative = path.relative_to(root).as_posix()
            _safe_relative_path(relative)
            details = path.lstat()
            if (
                not stat.S_ISREG(details.st_mode)
                or path.is_symlink()
                or _is_reparse(details)
                or int(details.st_nlink) != 1
            ):
                raise DryRunError("installed tree contains a non-owned regular file")
            record = expected_by_path.get(relative)
            if record is None:
                raise DryRunError("installed tree contains an unexpected file")
            data = path.read_bytes()
            if len(data) != record.size or _sha_bytes(data) != record.sha256:
                raise DryRunError("installed tree file bytes differ")
            if os.name != "nt" and stat.S_IMODE(details.st_mode) != record.mode:
                raise DryRunError("installed file mode differs from normalized policy")
            seen_files[relative] = record
            owner_ids.add(int(getattr(details, "st_uid", 0)))
    if set(seen_files) != set(expected_by_path):
        raise DryRunError("installed tree misses expected files")
    if seen_directories != set(_expected_directories(expected)):
        raise DryRunError("installed tree directory set differs")
    return {
        "content_sha256": expected.content_sha256,
        "directory_count": len(expected.directories),
        "empty_directories": list(_empty_directories(expected)),
        "explicit_empty_directories_proven": True,
        "file_count": len(expected.files),
        "logical_directory_mode": 0o755,
        "logical_file_modes": sorted({item.mode for item in expected.files}),
        "logical_target_group": "root",
        "logical_target_owner": "root",
        "physical_mode_enforced": os.name != "nt",
        "sandbox_owner_ids": sorted(owner_ids),
        "single_link_files": True,
        "total_bytes": expected.total_bytes,
    }


def _owned_fingerprint_tree(
    path: Path,
    expected: TreeRecord,
    authority_context: _AuthorityRunContext,
) -> dict[str, object]:
    try:
        fingerprint = authority_context.reads.fingerprint_tree(
            authority_context.relative(path),
            _tree_expectation(expected),
            limits=_OWNED_TREE_LIMITS,
        )
    except OwnedOutputReadError as error:
        raise DryRunError("installed tree could not be fingerprinted by authority") from error
    mapping = fingerprint.to_mapping()
    if mapping["content_sha256"] != expected.content_sha256:
        raise DryRunError("owned tree fingerprint seal differs")
    return mapping


def _sealed_file_record(tree: TreeRecord, path: str, label: str) -> FileRecord:
    matches = tuple(item for item in tree.files if item.path == path)
    if len(matches) != 1:
        raise DryRunError(f"{label} is absent from the sealed tree")
    return matches[0]


def _read_identity_bytes(
    root: Path,
    tree: TreeRecord,
    path: str,
    label: str,
    authority_context: _AuthorityRunContext,
    *,
    maximum_bytes: int = MAX_INPUT_METADATA_BYTES,
) -> bytes:
    record = _sealed_file_record(tree, path, label)
    if record.size > maximum_bytes:
        raise DryRunError(f"{label} exceeds its identity byte bound")
    try:
        return authority_context.reads.read_bounded(
            authority_context.relative(root / path),
            maximum_bytes=maximum_bytes,
            expected_size=record.size,
            expected_sha256=record.sha256,
        )
    except OwnedOutputReadError as error:
        raise DryRunError(f"{label} could not be read by authority") from error


def _read_identity_text(
    root: Path,
    tree: TreeRecord,
    path: str,
    label: str,
    authority_context: _AuthorityRunContext,
    *,
    maximum_bytes: int = MAX_INPUT_METADATA_BYTES,
) -> str:
    record = _sealed_file_record(tree, path, label)
    if record.size > maximum_bytes:
        raise DryRunError(f"{label} exceeds its identity byte bound")
    try:
        return authority_context.reads.read_utf8_text(
            authority_context.relative(root / path),
            maximum_bytes=maximum_bytes,
            expected_size=record.size,
            expected_sha256=record.sha256,
        )
    except OwnedOutputReadError as error:
        raise DryRunError(f"{label} could not be read by authority") from error


def _integration_identity(
    root: Path,
    *,
    tree: TreeRecord,
    authority_context: _AuthorityRunContext,
    integration_version: str,
    worker_version: str | None,
    legacy_without_build_id: bool,
) -> dict[str, object]:
    try:
        manifest = _strict_json_bytes(
            _read_identity_bytes(
                root,
                tree,
                "manifest.json",
                "integration manifest",
                authority_context,
            ),
            "integration manifest",
        )
        source = _read_identity_text(
            root,
            tree,
            "__init__.py",
            "integration __init__",
            authority_context,
        )
        parsed = ast.parse(source, filename="storm_tracker_v3/__init__.py")
    except (OSError, UnicodeError, json.JSONDecodeError, SyntaxError) as error:
        raise DryRunError("integration identity files cannot be parsed") from error
    if (
        not isinstance(manifest, dict)
        or manifest.get("domain") != INTEGRATION_ROOT_NAME
        or manifest.get("version") != integration_version
    ):
        raise DryRunError("integration manifest identity differs")
    values = [
        keyword.value
        for node in ast.walk(parsed)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if keyword.arg == "build_id"
    ]
    literals = [
        item.value for item in values
        if isinstance(item, ast.Constant) and isinstance(item.value, str)
    ]
    if legacy_without_build_id:
        if values:
            raise DryRunError("legacy integration unexpectedly declares a build ID")
        build_id: str | None = None
    else:
        expected = f"integration-{integration_version}-worker-{worker_version}"
        if len(values) != 1 or literals != [expected]:
            raise DryRunError("candidate integration pair build ID differs")
        build_id = expected
    return {
        "build_id": build_id,
        "build_id_legacy_absence": legacy_without_build_id,
        "domain": manifest["domain"],
        "manifest_sha256": _sha_bytes(_canonical(manifest)),
        "version": integration_version,
    }


def _yaml_section_entries(
    source: str,
    section: str,
) -> tuple[tuple[str, str], ...]:
    lines = source.splitlines()
    starts = [index for index, line in enumerate(lines) if line == f"{section}:"]
    if len(starts) != 1:
        raise DryRunError(f"worker config must have exactly one {section} section")
    result: list[tuple[str, str]] = []
    for line in lines[starts[0] + 1:]:
        if line and not line.startswith((" ", "#")):
            break
        if not line or line.lstrip().startswith("#"):
            continue
        match = re.fullmatch(
            r"  ([a-z][a-z0-9_]{0,63}):(?:\s(.*))?",
            line,
        )
        if match is None:
            raise DryRunError(f"worker {section} section is outside the restricted syntax")
        result.append((match.group(1), match.group(2) or ""))
    names = [name for name, _value in result]
    if not result or len(names) != len(set(names)):
        raise DryRunError(f"worker {section} section is empty or duplicated")
    return tuple(result)


def _worker_identity(
    root: Path,
    worker_version: str,
    *,
    tree: TreeRecord,
    authority_context: _AuthorityRunContext,
    require_worker_first_activation_contract: bool = False,
) -> dict[str, object]:
    try:
        config = _read_identity_text(
            root,
            tree,
            "config.yaml",
            "worker config",
            authority_context,
        )
        parsed = ast.parse(
            _read_identity_text(
                root,
                tree,
                "__init__.py",
                "worker __init__",
                authority_context,
            ),
            filename="storm_tracker_worker/__init__.py",
        )
    except (OSError, UnicodeError, SyntaxError) as error:
        raise DryRunError("worker identity files cannot be parsed") from error
    versions = _WORKER_VERSION.findall(config)
    slugs = _PACKAGE_SLUG.findall(config)
    if versions != [worker_version] or slugs != [WORKER_ROOT_NAME]:
        raise DryRunError("worker config version or package slug differs")
    declared: list[str] = []
    for statement in parsed.body:
        if not isinstance(statement, (ast.Assign, ast.AnnAssign)):
            continue
        targets = (
            statement.targets
            if isinstance(statement, ast.Assign)
            else [statement.target]
        )
        if not any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in targets
        ):
            continue
        value = statement.value
        if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
            raise DryRunError("worker __version__ is not a literal")
        declared.append(value.value)
    if declared != [worker_version]:
        raise DryRunError("worker config and __version__ differ")
    for required in ("compose.yaml", "run.sh", "service.py"):
        _read_identity_bytes(
            root,
            tree,
            required,
            f"worker {required}",
            authority_context,
        )
    option_entries = _yaml_section_entries(config, "options")
    schema_entries = _yaml_section_entries(config, "schema")
    option_names = tuple(name for name, _value in option_entries)
    schema_names = tuple(name for name, _value in schema_entries)
    if set(option_names) != set(schema_names):
        raise DryRunError("worker options and schema keysets differ")
    option_mapping = dict(option_entries)
    schema_mapping = dict(schema_entries)
    declared_activation_names = (
        set(option_mapping) | set(schema_mapping)
    ) & set(_WORKER_FIRST_ACTIVATION_OPTIONS)
    if require_worker_first_activation_contract or declared_activation_names:
        actual_options = {
            name: option_mapping.get(name)
            for name in _WORKER_FIRST_ACTIVATION_OPTIONS
        }
        actual_schema = {
            name: schema_mapping.get(name)
            for name in _WORKER_FIRST_ACTIVATION_OPTIONS
        }
        if (
            actual_options != _WORKER_FIRST_OPTION_DECLARATIONS
            or actual_schema != _WORKER_FIRST_SCHEMA_DECLARATIONS
        ):
            raise DryRunError(
                "worker-first activation options or schema declarations differ"
            )
    declared_raw_retention_names = (
        set(option_mapping) | set(schema_mapping)
    ) & set(_RAW_RETENTION_OPTIONS)
    if require_worker_first_activation_contract or declared_raw_retention_names:
        actual_options = {
            name: option_mapping.get(name)
            for name in _RAW_RETENTION_OPTIONS
        }
        actual_schema = {
            name: schema_mapping.get(name)
            for name in _RAW_RETENTION_OPTIONS
        }
        if (
            actual_options != _RAW_RETENTION_OPTION_DECLARATIONS
            or actual_schema != _RAW_RETENTION_SCHEMA_DECLARATIONS
        ):
            raise DryRunError(
                "worker RAW-retention options or schema declarations differ"
            )
    return {
        "activation_options_contract_exact": bool(declared_activation_names),
        "config_sha256": _sha_bytes(config.encode("utf-8")),
        "option_names": list(option_names),
        "package_slug": slugs[0],
        "raw_retention_options_contract_exact": bool(
            declared_raw_retention_names
        ),
        "schema_names": list(schema_names),
        "version": worker_version,
    }


def _valid_option_value(name: str, value: object) -> bool:
    if name == "compute_profile":
        return type(value) is str and value in {
            "shared_ha", "dedicated_nas"
        }
    if name == "cpu_threads":
        return type(value) is int and value == 2
    if name == "reserve_ha_core":
        return value is True
    if name == "queue_size":
        return type(value) is int and 1 <= value <= 16
    if name == "cpu_limit_percent":
        return type(value) is int and 25 <= value <= 100
    if name == "worker_nice":
        return type(value) is int and 0 <= value <= 19
    if name == "heartbeat_seconds":
        return type(value) is int and 2 <= value <= 60
    if name == "provider_cycle_seconds":
        return type(value) is int and 30 <= value <= 300
    if name == "log_level":
        return type(value) is str and value in {
            "DEBUG", "INFO", "WARNING", "ERROR"
        }
    if name == "runtime_mode":
        return type(value) is str and value in {
            "legacy", "worker_first_canary", "raw_capture_shadow", "worker_only"
        }
    if name == "postgis_projection_enabled":
        return type(value) is bool
    if name == "postgis_port":
        return type(value) is int and 1 <= value <= 65_535
    if name == "postgis_sslmode":
        return type(value) is str and value in {
            "disable", "require", "verify-full"
        }
    if name == "postgis_host":
        return (
            type(value) is str
            and len(value) <= 255
            and value == value.strip()
            and "\x00" not in value
        )
    if name in {"postgis_database", "postgis_user"}:
        return (
            type(value) is str
            and len(value) <= 128
            and value == value.strip()
            and "\x00" not in value
        )
    if name == "netatmo_station_id":
        return (
            type(value) is str
            and len(value) <= 128
            and value == value.strip()
            and "\x00" not in value
        )
    if name in _RAW_RETENTION_OPTIONS:
        return type(value) is bool
    if name == "worker_first_config_path":
        return (
            type(value) is str
            and len(value) <= _MAX_WORKER_FIRST_CONFIG_PATH_CHARS
            and "\x00" not in value
            and value == value.strip()
        )
    if name in {
        "worker_first_config_sha256",
        "worker_first_authority_sha256",
    }:
        return (
            type(value) is str
            and (value == "" or _SHA256.fullmatch(value) is not None)
        )
    return False


def _validate_worker_first_activation_snapshot(
    safe_values: Mapping[str, object],
) -> dict[str, object]:
    present_names = set(safe_values) & set(_WORKER_FIRST_ACTIVATION_OPTIONS)
    if not present_names:
        return {
            "activation_pins_bound": False,
            "raw_capture_shadow_selected": False,
        }
    if present_names != set(_WORKER_FIRST_ACTIVATION_OPTIONS):
        raise DryRunError(
            "sanitized worker-first activation option group is incomplete"
        )
    runtime_mode = safe_values["runtime_mode"]
    config_path = safe_values["worker_first_config_path"]
    config_sha256 = safe_values["worker_first_config_sha256"]
    authority_sha256 = safe_values["worker_first_authority_sha256"]
    if runtime_mode == "legacy":
        if any((config_path, config_sha256, authority_sha256)):
            raise DryRunError(
                "sanitized legacy activation options must leave all worker-first pins empty"
            )
        return {
            "activation_pins_bound": False,
            "raw_capture_shadow_selected": False,
        }
    pins = (config_path, config_sha256, authority_sha256)
    if runtime_mode == "raw_capture_shadow" and not any(pins):
        return {
            "activation_pins_bound": False,
            "raw_capture_shadow_selected": True,
        }
    if runtime_mode == "worker_only" and not any(pins):
        return {
            "activation_pins_bound": False,
            "raw_capture_shadow_selected": False,
        }
    if not all(pins):
        raise DryRunError(
            "sanitized worker-first activation pins are incomplete"
        )
    assert isinstance(config_path, str)
    canonical_path = PurePosixPath(config_path)
    if (
        not config_path.startswith("/")
        or config_path == "/"
        or canonical_path.as_posix() != config_path
        or any(part in {"", ".", ".."} for part in canonical_path.parts[1:])
    ):
        raise DryRunError("sanitized worker-first config path is invalid")
    if (
        not isinstance(config_sha256, str)
        or _SHA256.fullmatch(config_sha256) is None
        or not isinstance(authority_sha256, str)
        or _SHA256.fullmatch(authority_sha256) is None
    ):
        raise DryRunError("sanitized worker-first hash pin is invalid")
    return {
        "activation_pins_bound": True,
        "raw_capture_shadow_selected": runtime_mode == "raw_capture_shadow",
    }


def _validate_options_snapshot_document(
    document: object,
    *,
    options_sha256: str,
) -> dict[str, object]:
    if not isinstance(document, dict) or set(document) != {
        "contract", "options", "secret_presence"
    }:
        raise DryRunError("sanitized options snapshot has an unknown schema")
    if document["contract"] != OPTIONS_CONTRACT:
        raise DryRunError("sanitized options snapshot contract differs")
    options = document["options"]
    secret_presence = document["secret_presence"]
    if not isinstance(options, dict) or not isinstance(secret_presence, dict):
        raise DryRunError("sanitized options snapshot mappings are invalid")
    if len(options) > MAX_OPTIONS or len(secret_presence) > MAX_SECRET_OPTIONS:
        raise DryRunError("sanitized options snapshot exceeds its key-count bound")
    if set(options) & set(secret_presence):
        raise DryRunError("option and secret-presence keysets must be disjoint")
    safe_values: dict[str, object] = {}
    for name, value in options.items():
        if not isinstance(name, str) or _SAFE_OPTION_NAME.fullmatch(name) is None:
            raise DryRunError("sanitized option name is invalid")
        if name not in _SAFE_NONSECRET_OPTIONS:
            raise DryRunError("only allowlisted non-secret values may appear in options")
        if not _valid_option_value(name, value):
            raise DryRunError(f"sanitized option value is invalid for {name}")
        safe_values[name] = value
    activation = _validate_worker_first_activation_snapshot(safe_values)
    safe_presence: dict[str, bool] = {}
    for name, present in secret_presence.items():
        if (
            not isinstance(name, str)
            or _SAFE_OPTION_NAME.fullmatch(name) is None
            or not isinstance(present, bool)
        ):
            raise DryRunError("secret-presence entry is invalid")
        safe_presence[name] = present
    if (
        safe_values.get("cpu_threads") != 2
        or safe_values.get("reserve_ha_core") is not True
    ):
        raise DryRunError(
            "sanitized legacy options must preserve cpu_threads=2 and "
            "reserve_ha_core=true"
        )
    return {
        "option_names": sorted(safe_values),
        "option_values": safe_values,
        "options_sha256": options_sha256,
        "raw_capture_shadow_selected": activation[
            "raw_capture_shadow_selected"
        ],
        "raw_capture_target_activation_pins_bound": activation[
            "activation_pins_bound"
        ],
        "secret_option_names": sorted(safe_presence),
        "secret_presence": safe_presence,
        "secret_presence_sha256": _sha_bytes(_canonical(safe_presence)),
    }


def _load_options_snapshot(path: Path) -> tuple[FileWitness, dict[str, object]]:
    """Legacy standalone reader retained for direct parser-boundary tests."""

    witness = _open_witness(path, "sanitized options snapshot")
    document = _strict_json_file(witness.path, "sanitized options snapshot")
    return witness, _validate_options_snapshot_document(
        document,
        options_sha256=witness.sha256,
    )


def _load_options_snapshot_bound(
    registry: BoundInputRegistry,
) -> dict[str, object]:
    """Parse one owned options spool without reopening its lexical path."""

    try:
        registry.require_exact_keys(("options",))
        metadata = registry.metadata("options")
        document = _strict_json_bytes(
            registry.read_bounded(
                "options",
                maximum_bytes=MAX_INPUT_METADATA_BYTES,
            ),
            "bound sanitized options snapshot",
        )
    except BoundInputError as error:
        raise DryRunError(
            "bound sanitized options snapshot could not be read"
        ) from error
    return _validate_options_snapshot_document(
        document,
        options_sha256=metadata.sha256,
    )


def _options_compatibility(
    snapshot: Mapping[str, object],
    baseline: Mapping[str, object],
    candidate: Mapping[str, object],
) -> dict[str, object]:
    option_names = set(snapshot["option_names"])
    secret_names = set(snapshot["secret_option_names"])
    option_values = dict(snapshot["option_values"])
    secret_presence = dict(snapshot["secret_presence"])
    baseline_schema = set(baseline["schema_names"])
    candidate_schema = set(candidate["schema_names"])
    documented_names = option_names | secret_names
    schema_union = baseline_schema | candidate_schema
    blockers: list[str] = []
    blockers.extend(
        f"documented_option_missing_from_worker_schemas:{name}"
        for name in sorted(documented_names - schema_union)
    )
    for name in sorted(schema_union - documented_names):
        if name in candidate_schema - baseline_schema:
            blockers.append(
                f"candidate_schema_added_key_without_roundtrip_snapshot:{name}"
            )
        elif name in baseline_schema - candidate_schema:
            blockers.append(
                f"baseline_schema_removed_key_without_roundtrip_snapshot:{name}"
            )
        else:
            blockers.append(f"shared_schema_key_without_roundtrip_snapshot:{name}")
    removed_names = sorted(baseline_schema - candidate_schema)
    added_names = sorted(candidate_schema - baseline_schema)
    baseline_values = {
        name: option_values[name]
        for name in sorted(baseline_schema & option_names)
    }
    baseline_presence = {
        name: secret_presence[name]
        for name in sorted(baseline_schema & secret_names)
    }
    candidate_values = {
        name: option_values[name]
        for name in sorted(candidate_schema & option_names)
    }
    candidate_presence = {
        name: secret_presence[name]
        for name in sorted(candidate_schema & secret_names)
    }
    rollback_values = {
        name: (
            candidate_values[name]
            if name in candidate_values
            else option_values[name]
        )
        for name in sorted(baseline_schema & option_names)
    }
    rollback_presence = {
        name: (
            candidate_presence[name]
            if name in candidate_presence
            else secret_presence[name]
        )
        for name in sorted(baseline_schema & secret_names)
    }
    schema_roundtrip_blockers = list(blockers)
    roundtrip_proven = (
        not schema_roundtrip_blockers
        and rollback_values == baseline_values
        and rollback_presence == baseline_presence
        and set(candidate_values) | set(candidate_presence) == candidate_schema
        and set(baseline_values) | set(baseline_presence) == baseline_schema
    )
    if not roundtrip_proven:
        blockers.append("supervisor_options_roundtrip_not_proven")
    raw_capture_shadow_selected = bool(
        snapshot["raw_capture_shadow_selected"]
    )
    raw_capture_target_activation_pins_bound = bool(
        snapshot["raw_capture_target_activation_pins_bound"]
    )
    if (
        raw_capture_shadow_selected
        and not raw_capture_target_activation_pins_bound
    ):
        blockers.append("raw_capture_target_activation_pins_absent")
    blockers = sorted(set(blockers))
    return {
        "activation_option_names": list(_WORKER_FIRST_ACTIVATION_OPTIONS),
        "activation_option_values_reported": False,
        "added_candidate_schema_names": added_names,
        "blockers": blockers,
        "candidate_schema_names": sorted(candidate_schema),
        "documented_option_names": sorted(option_names),
        "documented_secret_option_names": sorted(secret_names),
        "migration_proven": roundtrip_proven,
        "raw_capture_retention_option_names": list(_RAW_RETENTION_OPTIONS),
        "raw_capture_retention_option_values_reported": False,
        "raw_capture_shadow_selected": raw_capture_shadow_selected,
        "raw_capture_target_activation_pins_bound": (
            raw_capture_target_activation_pins_bound
        ),
        "removed_baseline_schema_names": removed_names,
        "snapshot_sha256": snapshot["options_sha256"],
        "supervisor_options_roundtrip_proven": roundtrip_proven,
    }


def _state_document(
    *,
    candidate_integration: TreeRecord,
    candidate_worker: TreeRecord,
    baseline_integration: TreeRecord,
    baseline_worker: TreeRecord,
) -> dict[str, object]:
    unsigned = {
        "baseline": {
            "integration": baseline_integration.to_mapping(),
            "worker": baseline_worker.to_mapping(),
        },
        "candidate": {
            "integration": candidate_integration.to_mapping(),
            "worker": candidate_worker.to_mapping(),
        },
        "contract": STATE_CONTRACT,
    }
    return {**unsigned, "state_sha256": _sha_bytes(_canonical(unsigned))}


def _load_state(
    run_root: Path,
    authority_context: _AuthorityRunContext,
) -> dict[str, object]:
    try:
        document = authority_context.reads.strict_json(
            authority_context.relative(run_root / "dry_run_state.json"),
            label="dry_run_state",
            maximum_bytes=MAX_INPUT_METADATA_BYTES,
        )
    except OwnedOutputReadError as error:
        raise DryRunError("dry-run state could not be read by authority") from error
    if not isinstance(document, dict) or set(document) != {
        "baseline", "candidate", "contract", "state_sha256"
    } or document["contract"] != STATE_CONTRACT:
        raise DryRunError("dry-run state has an unknown schema")
    supplied = document["state_sha256"]
    unsigned = dict(document)
    del unsigned["state_sha256"]
    if not isinstance(supplied, str) or supplied != _sha_bytes(_canonical(unsigned)):
        raise DryRunError("dry-run state hash differs")
    return document


_NORMAL_JOURNAL_EVENTS = (
    "initialized",
    "upgrade_prepared",
    "worker_baseline_backed_up",
    "worker_candidate_activated",
    "integration_baseline_backed_up",
    "integration_candidate_activated",
    "upgrade_commit_decided",
    "upgrade_committed",
    "rollback_decided",
    "rollback_completed",
)


def _validate_journal_payload(event: Mapping[str, object]) -> None:
    name = event.get("event")
    payload = event.get("payload")
    if not isinstance(name, str) or not isinstance(payload, dict):
        raise DryRunError("dry-run journal event name or payload type differs")
    if name == "initialized":
        expected: Mapping[str, object] = {
            "component_atomic": True,
            "pair_atomic": False,
            "services_assumed_stopped": True,
        }
    elif name == "upgrade_prepared":
        expected = {"services_assumed_stopped": True}
    else:
        expected = {}
    if _canonical(payload) != _canonical(expected):
        raise DryRunError("dry-run journal event payload schema differs")


def _validate_journal_sequence(events: Iterable[Mapping[str, object]]) -> None:
    materialized = tuple(events)
    for event in materialized:
        _validate_journal_payload(event)
    names = tuple(str(event["event"]) for event in materialized)
    valid = {
        _NORMAL_JOURNAL_EVENTS[:length]
        for length in range(len(_NORMAL_JOURNAL_EVENTS) + 1)
    }
    valid.update(
        _NORMAL_JOURNAL_EVENTS[:length] + ("upgrade_recovered_to_baseline",)
        for length in range(0, 7)
    )
    valid.add(_NORMAL_JOURNAL_EVENTS[:7] + ("upgrade_recovered",))
    valid.add(_NORMAL_JOURNAL_EVENTS[:9] + ("rollback_recovered",))
    if names not in valid:
        raise DryRunError("dry-run journal transition grammar differs")


def _read_journal(
    run_root: Path,
    authority_context: _AuthorityRunContext,
) -> JournalReadResult:
    try:
        return authority_context.reads.read_journal(
            authority_context.relative(run_root / "journal"),
            expected_contract=JOURNAL_CONTRACT,
            limits=JournalReadLimits(
                maximum_events=len(_NORMAL_JOURNAL_EVENTS) + 1,
                maximum_event_bytes=MAX_INPUT_METADATA_BYTES,
                maximum_total_bytes=(len(_NORMAL_JOURNAL_EVENTS) + 1)
                * MAX_INPUT_METADATA_BYTES,
                maximum_directory_entries=MAX_ARCHIVE_MEMBER_COUNT,
            ),
            validate_sequence=_validate_journal_sequence,
        )
    except OwnedOutputReadError as error:
        raise DryRunError("dry-run journal could not be read by authority") from error


def _append_journal(
    run_root: Path,
    event_name: str,
    payload: Mapping[str, object] | None = None,
    *,
    authority_context: _AuthorityRunContext,
    fault_injector: FaultInjector | None = None,
    fault_prefix: str | None = None,
) -> str:
    journal_result = _read_journal(run_root, authority_context)
    events = list(journal_result.events)
    previous = journal_result.head_sha256
    sequence = len(events) + 1
    unsigned = {
        "contract": JOURNAL_CONTRACT,
        "event": event_name,
        "payload": dict(payload or {}),
        "previous_sha256": previous,
        "sequence": sequence,
    }
    event = {**unsigned, "event_sha256": _sha_bytes(_canonical(unsigned))}
    _validate_journal_sequence([*events, event])
    journal = run_root / "journal"
    pending = journal / f"{sequence:06d}.{uuid.uuid4().hex}.pending"
    final = journal / f"{sequence:06d}.json"
    _write_exclusive(
        pending,
        _canonical(event) + b"\n",
        mode=0o644,
        run_root=run_root,
        authority_context=authority_context,
    )
    pending_relative = authority_context.relative(pending)
    final_relative = authority_context.relative(final)
    try:
        pending_identity = authority_context.root.entry_identity(pending_relative)
    except AuthorityError as error:
        raise DryRunError("dry-run journal pending entry disappeared") from error
    try:
        authority_context.root.entry_identity(final_relative)
    except AuthorityNotFound:
        pass
    except AuthorityError as error:
        raise DryRunError("dry-run journal publication cannot be prepared") from error
    else:
        raise DryRunError("dry-run journal destination already exists")
    if fault_prefix is not None:
        _inject(fault_injector, f"{fault_prefix}.after_pending_fsync_before_publish")
    try:
        authority_context.atomic.publish(
            pending_relative,
            final_relative,
            expected_source=pending_identity,
            kind="journal_pending_to_final",
        )
    except AtomicNoReplaceConflict:
        raise DryRunError("dry-run journal destination already exists") from None
    except AtomicNoReplaceError as error:
        _raise_classified_atomic_failure(
            authority_context,
            pending_relative,
            final_relative,
            pending_identity,
            "dry-run journal publication",
            error,
        )
    if fault_prefix is not None:
        _inject(fault_injector, f"{fault_prefix}.after_publish_before_directory_sync")
    try:
        authority_context.output.fsync(authority_context.relative(journal))
    except OutputAuthorityError as error:
        raise DryRunError(
            "dry-run journal published but directory fsync failed"
        ) from error
    authority_context.reprove()
    if fault_prefix is not None:
        _inject(fault_injector, f"{fault_prefix}.after_directory_sync")
    return event["event_sha256"]


def _event_names(result: JournalReadResult) -> tuple[str, ...]:
    if not isinstance(result, JournalReadResult):
        raise TypeError("journal names require one JournalReadResult")
    return tuple(str(item["event"]) for item in result.events)


def _rename(
    source: Path,
    destination: Path,
    run_root: Path,
    *,
    authority_context: _AuthorityRunContext,
    fault_injector: FaultInjector | None = None,
    fault_prefix: str | None = None,
) -> None:
    _assert_mutation_path(source, run_root)
    _assert_mutation_path(destination, run_root)
    source_relative = authority_context.relative(source)
    destination_relative = authority_context.relative(destination)
    try:
        source_identity = authority_context.root.entry_identity(source_relative)
    except AuthorityError as error:
        raise DryRunError("atomic switch source is absent or indirect") from error
    if source_identity.kind != "directory":
        raise DryRunError("atomic switch source is not a directory")
    try:
        authority_context.root.entry_identity(destination_relative)
    except AuthorityNotFound:
        pass
    except AuthorityError as error:
        raise DryRunError("atomic switch destination cannot be reproved") from error
    else:
        raise DryRunError("atomic switch destination already exists")
    if fault_prefix is not None:
        _inject(fault_injector, f"{fault_prefix}.before_rename")
    try:
        authority_context.atomic.publish(
            source_relative,
            destination_relative,
            expected_source=source_identity,
            kind="generation_directory",
        )
    except AtomicNoReplaceConflict:
        raise DryRunError("atomic switch destination already exists") from None
    except AtomicNoReplaceError as error:
        _raise_classified_atomic_failure(
            authority_context,
            source_relative,
            destination_relative,
            source_identity,
            "atomic switch",
            error,
        )
    if fault_prefix is not None:
        _inject(
            fault_injector,
            f"{fault_prefix}.after_rename_before_source_directory_sync",
        )
    try:
        authority_context.output.fsync(
            authority_context.relative(source.parent)
        )
    except OutputAuthorityError as error:
        raise DryRunError(
            "atomic switch published but source directory fsync failed"
        ) from error
    if fault_prefix is not None:
        _inject(fault_injector, f"{fault_prefix}.after_source_directory_sync")
    if not _same_path(source.parent, destination.parent):
        try:
            authority_context.output.fsync(
                authority_context.relative(destination.parent)
            )
        except OutputAuthorityError as error:
            raise DryRunError(
                "atomic switch published but destination directory fsync failed"
            ) from error
    if fault_prefix is not None:
        _inject(fault_injector, f"{fault_prefix}.after_destination_directory_sync")
    authority_context.reprove()


def _raise_classified_atomic_failure(
    authority_context: _AuthorityRunContext,
    source: RelativePath,
    destination: RelativePath,
    expected_source: NodeIdentity,
    label: str,
    error: AtomicNoReplaceError,
) -> None:
    try:
        try:
            source_identity = authority_context.root.entry_identity(source)
        except AuthorityNotFound:
            source_identity = None
        try:
            destination_identity = authority_context.root.entry_identity(destination)
        except AuthorityNotFound:
            destination_identity = None
        authority_context.reprove()
    except (AuthorityError, DryRunError) as proof_error:
        raise DryRunError(f"{label} outcome could not be classified") from proof_error
    if (
        source_identity is None
        and destination_identity is not None
        and destination_identity.backend == expected_source.backend
        and destination_identity.same_object(expected_source)
    ):
        raise DryRunError(f"{label} published but postproof failed") from error
    if (
        source_identity is not None
        and source_identity.backend == expected_source.backend
        and source_identity.same_object(expected_source)
        and destination_identity is None
    ):
        raise DryRunError(f"{label} failed before publication") from error
    raise DryRunError(f"{label} outcome is inconsistent") from error


def _paths(run_root: Path) -> dict[str, Path]:
    return {
        "active_integration": run_root / "install" / "integration.active",
        "active_worker": run_root / "install" / "worker.active",
        "backup_integration": run_root / "backup" / "integration.baseline",
        "backup_worker": run_root / "backup" / "worker.baseline",
        "candidate_integration": run_root / "stage" / "integration.candidate",
        "candidate_worker": run_root / "stage" / "worker.candidate",
        "retained_integration": run_root / "retained" / "integration.candidate",
        "retained_worker": run_root / "retained" / "worker.candidate",
    }


_LAYOUT_INITIAL = ("baseline", "absent", "candidate", "absent")
_LAYOUT_BACKED_UP = ("absent", "baseline", "candidate", "absent")
_LAYOUT_CANDIDATE_ACTIVE = ("candidate", "baseline", "absent", "absent")
_LAYOUT_RECOVERY_RETAINED = ("absent", "baseline", "absent", "candidate")
_LAYOUT_BASELINE_RESTORED = ("baseline", "absent", "absent", "candidate")
_VALID_GENERATION_LAYOUTS = frozenset({
    _LAYOUT_INITIAL,
    _LAYOUT_BACKED_UP,
    _LAYOUT_CANDIDATE_ACTIVE,
    _LAYOUT_RECOVERY_RETAINED,
    _LAYOUT_BASELINE_RESTORED,
})


def _tree_kind(
    path: Path,
    baseline: TreeRecord,
    candidate: TreeRecord,
    authority_context: _AuthorityRunContext,
) -> str:
    try:
        return authority_context.reads.sealed_tree_kind(
            authority_context.relative(path),
            _tree_expectation(baseline),
            _tree_expectation(candidate),
            limits=_OWNED_TREE_LIMITS,
        )
    except OwnedOutputReadError as error:
        raise DryRunError(
            "transaction tree matches neither sealed generation"
        ) from error


def _assert_generation_inventory(
    run_root: Path,
    state: Mapping[str, object],
    authority_context: _AuthorityRunContext,
    *,
    allowed_layouts: Iterable[tuple[str, str, str, str]] | None = None,
) -> dict[str, tuple[str, str, str, str]]:
    baseline = state.get("baseline")
    candidate = state.get("candidate")
    if not isinstance(baseline, dict) or not isinstance(candidate, dict):
        raise DryRunError("dry-run state generations are invalid")
    try:
        layouts = authority_context.reads.generation_inventory(
            baseline={
                component: _tree_expectation(
                    TreeRecord.from_mapping(baseline[component])
                )
                for component in ("integration", "worker")
            },
            candidate={
                component: _tree_expectation(
                    TreeRecord.from_mapping(candidate[component])
                )
                for component in ("integration", "worker")
            },
            allowed_layouts=allowed_layouts,
            limits=_OWNED_TREE_LIMITS,
        )
    except OwnedOutputReadError as error:
        raise DryRunError(
            "transaction generation inventory failed authority proof"
        ) from error
    if allowed_layouts is None and any(
        layout not in _VALID_GENERATION_LAYOUTS for layout in layouts.values()
    ):
        raise DryRunError("transaction generation placement is outside phase-safe layouts")
    return layouts


def _precommit_normal_layouts(
    names: tuple[str, ...],
) -> tuple[set[tuple[str, ...]], set[tuple[str, ...]]]:
    if "upgrade_prepared" not in names:
        return {_LAYOUT_INITIAL}, {_LAYOUT_INITIAL}
    if "worker_baseline_backed_up" not in names:
        return {_LAYOUT_INITIAL}, {_LAYOUT_INITIAL, _LAYOUT_BACKED_UP}
    if "worker_candidate_activated" not in names:
        return {_LAYOUT_INITIAL}, {_LAYOUT_BACKED_UP, _LAYOUT_CANDIDATE_ACTIVE}
    if "integration_baseline_backed_up" not in names:
        return {_LAYOUT_INITIAL, _LAYOUT_BACKED_UP}, {_LAYOUT_CANDIDATE_ACTIVE}
    if "integration_candidate_activated" not in names:
        return {_LAYOUT_BACKED_UP, _LAYOUT_CANDIDATE_ACTIVE}, {_LAYOUT_CANDIDATE_ACTIVE}
    return {_LAYOUT_CANDIDATE_ACTIVE}, {_LAYOUT_CANDIDATE_ACTIVE}


def _restore_trace(layout: tuple[str, ...]) -> tuple[tuple[tuple[str, ...], ...], tuple[str, ...]]:
    if layout == _LAYOUT_INITIAL:
        return (_LAYOUT_INITIAL,), _LAYOUT_INITIAL
    if layout == _LAYOUT_BACKED_UP:
        return (_LAYOUT_BACKED_UP, _LAYOUT_INITIAL), _LAYOUT_INITIAL
    if layout == _LAYOUT_CANDIDATE_ACTIVE:
        return (
            _LAYOUT_CANDIDATE_ACTIVE,
            _LAYOUT_RECOVERY_RETAINED,
            _LAYOUT_BASELINE_RESTORED,
        ), _LAYOUT_BASELINE_RESTORED
    raise DryRunError("precommit recovery started from an invalid generation layout")


def _assert_recovery_phase_layout(
    run_root: Path,
    state: Mapping[str, object],
    names: tuple[str, ...],
    authority_context: _AuthorityRunContext,
    *,
    layouts: Mapping[str, tuple[str, str, str, str]] | None = None,
) -> dict[str, tuple[str, str, str, str]]:
    if "rollback_completed" in names or "rollback_recovered" in names:
        allowed = {(_LAYOUT_BASELINE_RESTORED, _LAYOUT_BASELINE_RESTORED)}
    elif "rollback_decided" in names:
        allowed = {
            (_LAYOUT_CANDIDATE_ACTIVE, _LAYOUT_CANDIDATE_ACTIVE),
            (_LAYOUT_RECOVERY_RETAINED, _LAYOUT_CANDIDATE_ACTIVE),
            (_LAYOUT_BASELINE_RESTORED, _LAYOUT_CANDIDATE_ACTIVE),
            (_LAYOUT_BASELINE_RESTORED, _LAYOUT_RECOVERY_RETAINED),
            (_LAYOUT_BASELINE_RESTORED, _LAYOUT_BASELINE_RESTORED),
        }
    elif "upgrade_recovered_to_baseline" in names:
        restored = {_LAYOUT_INITIAL, _LAYOUT_BASELINE_RESTORED}
        allowed = {(integration, worker) for integration in restored for worker in restored}
    elif "upgrade_commit_decided" in names:
        allowed = {(_LAYOUT_CANDIDATE_ACTIVE, _LAYOUT_CANDIDATE_ACTIVE)}
    else:
        integration_starts, worker_starts = _precommit_normal_layouts(names)
        allowed = set()
        for integration_start in integration_starts:
            integration_trace, integration_final = _restore_trace(integration_start)
            for worker_start in worker_starts:
                worker_trace, _worker_final = _restore_trace(worker_start)
                allowed.update((item, worker_start) for item in integration_trace)
                allowed.update((integration_final, item) for item in worker_trace)
    if layouts is None:
        observed_layouts = _assert_generation_inventory(
            run_root,
            state,
            authority_context,
            allowed_layouts={layout for pair in allowed for layout in pair},
        )
    else:
        observed_layouts = dict(layouts)
        if set(observed_layouts) != {"integration", "worker"}:
            raise DryRunError("transaction generation snapshot is invalid")
    observed = (
        observed_layouts["integration"],
        observed_layouts["worker"],
    )
    if observed not in allowed:
        raise DryRunError("transaction generation layout differs from the journal phase")
    return observed_layouts


def _restore_component_baseline(
    *,
    component: str,
    active: Path,
    backup: Path,
    stage: Path,
    retained: Path,
    baseline: TreeRecord,
    candidate: TreeRecord,
    run_root: Path,
    authority_context: _AuthorityRunContext,
    fault_injector: FaultInjector | None,
    initial_layout: tuple[str, str, str, str],
) -> bool:
    active_kind, backup_kind, stage_kind, retained_kind = initial_layout
    changed = False
    if backup_kind not in ("absent", "baseline") or retained_kind not in ("absent", "candidate"):
        raise DryRunError("recovery generation placement is invalid")
    if active_kind == "candidate":
        if retained_kind != "absent":
            raise DryRunError("recovery has two retained candidate generations")
        _rename(
            active,
            retained,
            run_root,
            authority_context=authority_context,
            fault_injector=fault_injector,
            fault_prefix=f"rename.recovery_{component}_retain",
        )
        changed = True
        active_kind = "absent"
    if active_kind == "absent":
        if backup_kind != "baseline":
            raise DryRunError("recovery cannot locate the sealed baseline")
        _rename(
            backup,
            active,
            run_root,
            authority_context=authority_context,
            fault_injector=fault_injector,
            fault_prefix=f"rename.recovery_{component}_restore",
        )
        changed = True
    elif active_kind != "baseline":
        raise DryRunError("recovery active generation is invalid")
    if stage_kind not in ("absent", "candidate"):
        raise DryRunError("recovery stage is not the sealed candidate")
    return changed


def _restore_baseline(
    run_root: Path,
    state: Mapping[str, object],
    authority_context: _AuthorityRunContext,
    fault_injector: FaultInjector | None = None,
    *,
    initial_layouts: Mapping[str, tuple[str, str, str, str]],
) -> dict[str, tuple[str, str, str, str]]:
    baseline = state["baseline"]
    candidate = state["candidate"]
    if not isinstance(baseline, dict) or not isinstance(candidate, dict):
        raise DryRunError("dry-run state generations are invalid")
    paths = _paths(run_root)
    changed = False
    for component in ("integration", "worker"):
        changed = _restore_component_baseline(
            component=component,
            active=paths[f"active_{component}"],
            backup=paths[f"backup_{component}"],
            stage=paths[f"candidate_{component}"],
            retained=paths[f"retained_{component}"],
            baseline=TreeRecord.from_mapping(baseline[component]),
            candidate=TreeRecord.from_mapping(candidate[component]),
            run_root=run_root,
            authority_context=authority_context,
            fault_injector=fault_injector,
            initial_layout=initial_layouts[component],
        ) or changed
    if not changed:
        return dict(initial_layouts)
    return _assert_generation_inventory(run_root, state, authority_context)


def _assert_candidate_active(
    run_root: Path,
    state: Mapping[str, object],
    authority_context: _AuthorityRunContext,
    *,
    initial_layouts: Mapping[str, tuple[str, str, str, str]],
) -> None:
    candidate = state["candidate"]
    if not isinstance(candidate, dict):
        raise DryRunError("dry-run candidate state is invalid")
    if any(
        initial_layouts.get(component) != _LAYOUT_CANDIDATE_ACTIVE
        for component in ("integration", "worker")
    ):
        raise DryRunError("candidate recovery generation layout differs")


def _recover_release_install_dry_run_under_lease(
    run_root: Path,
    *,
    process_lease: _ExclusiveProcessLease,
    fault_injector: FaultInjector | None = None,
) -> Mapping[str, object]:
    process_lease.assert_held()
    authority_context = _open_authority_run_context(
        run_root,
        existing=True,
        process_lease=process_lease,
    )
    root = authority_context.lexical_root
    try:
        process_lease.assert_held()
        state = _load_state(root, authority_context)
        journal = _read_journal(root, authority_context)
        names = _event_names(journal)
        initial_layouts = _assert_recovery_phase_layout(
            root, state, names, authority_context
        )
        process_lease.assert_held()
        if "rollback_decided" in names:
            current_layouts = _restore_baseline(
                root,
                state,
                authority_context,
                fault_injector,
                initial_layouts=initial_layouts,
            )
            if (
                "rollback_completed" not in names
                and "rollback_recovered" not in names
            ):
                _append_journal(
                    root,
                    "rollback_recovered",
                    authority_context=authority_context,
                    fault_injector=fault_injector,
                    fault_prefix="journal.rollback_recovered",
                )
            outcome = "baseline"
        elif "upgrade_commit_decided" in names:
            _assert_candidate_active(
                root,
                state,
                authority_context,
                initial_layouts=initial_layouts,
            )
            current_layouts = dict(initial_layouts)
            if "upgrade_committed" not in names and "upgrade_recovered" not in names:
                _append_journal(
                    root,
                    "upgrade_recovered",
                    authority_context=authority_context,
                    fault_injector=fault_injector,
                    fault_prefix="journal.upgrade_recovered",
                )
            outcome = "candidate"
        else:
            current_layouts = _restore_baseline(
                root,
                state,
                authority_context,
                fault_injector,
                initial_layouts=initial_layouts,
            )
            if "upgrade_recovered_to_baseline" not in names:
                _append_journal(
                    root,
                    "upgrade_recovered_to_baseline",
                    authority_context=authority_context,
                    fault_injector=fault_injector,
                    fault_prefix="journal.upgrade_recovered_to_baseline",
                )
            outcome = "baseline"
        final_journal = _read_journal(root, authority_context)
        final_names = _event_names(final_journal)
        _assert_recovery_phase_layout(
            root,
            state,
            final_names,
            authority_context,
            layouts=current_layouts,
        )
        process_lease.assert_held()
        authority_context.reprove()
        output_evidence = authority_context.evidence()
        atomic_evidence = dict(output_evidence["atomic_capability"])
        return {
            "atomic_noreplace_publication": atomic_evidence,
            "atomic_noreplace_publication_proven": True,
            "deployment_performed": False,
            "exclusive_process_lease": process_lease.evidence(),
            "journal_event_count": len(final_journal.events),
            "journal_head_sha256": final_journal.head_sha256,
            "live_paths_touched": False,
            "network_performed": False,
            "outcome": outcome,
            "output_authority": output_evidence,
        }
    finally:
        authority_context.close()


def recover_release_install_dry_run(
    run_root: Path,
    *,
    fault_injector: FaultInjector | None = None,
) -> Mapping[str, object]:
    """Recover locally while holding the run root's exclusive OS lease."""

    process_lease = _acquire_process_lease(run_root)
    try:
        result = _recover_release_install_dry_run_under_lease(
            run_root,
            process_lease=process_lease,
            fault_injector=fault_injector,
        )
    except BaseException as primary:
        try:
            process_lease.release()
        except DryRunError as release_error:
            raise DryRunError(
                "dry-run recovery failed and process lease release failed"
            ) from BaseExceptionGroup(
                "dry-run recovery and process lease release failures",
                [primary, release_error],
            )
        raise
    process_lease.release()
    return result


def _inject(fault_injector: FaultInjector | None, point: str) -> None:
    if fault_injector is not None:
        fault_injector(point)


def _upgrade_then_rollback(
    run_root: Path,
    state: Mapping[str, object],
    authority_context: _AuthorityRunContext,
    fault_injector: FaultInjector | None,
) -> tuple[dict[str, object], dict[str, object]]:
    paths = _paths(run_root)
    baseline = state["baseline"]
    candidate = state["candidate"]
    if not isinstance(baseline, dict) or not isinstance(candidate, dict):
        raise DryRunError("dry-run state generations are invalid")
    baseline_integration = TreeRecord.from_mapping(baseline["integration"])
    baseline_worker = TreeRecord.from_mapping(baseline["worker"])
    candidate_integration = TreeRecord.from_mapping(candidate["integration"])
    candidate_worker = TreeRecord.from_mapping(candidate["worker"])
    _assert_generation_inventory(run_root, state, authority_context)

    _append_journal(
        run_root,
        "upgrade_prepared",
        {"services_assumed_stopped": True},
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="journal.upgrade_prepared",
    )

    _rename(
        paths["active_worker"], paths["backup_worker"], run_root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="rename.upgrade_worker_backup",
    )
    _append_journal(
        run_root, "worker_baseline_backed_up",
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="journal.worker_baseline_backed_up",
    )
    _rename(
        paths["candidate_worker"], paths["active_worker"], run_root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="rename.upgrade_worker_activate",
    )
    _append_journal(
        run_root, "worker_candidate_activated",
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="journal.worker_candidate_activated",
    )

    _rename(
        paths["active_integration"], paths["backup_integration"], run_root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="rename.upgrade_integration_backup",
    )
    _append_journal(
        run_root, "integration_baseline_backed_up",
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="journal.integration_baseline_backed_up",
    )
    _rename(
        paths["candidate_integration"], paths["active_integration"], run_root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="rename.upgrade_integration_activate",
    )
    _append_journal(
        run_root, "integration_candidate_activated",
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="journal.integration_candidate_activated",
    )

    upgrade_trees = {
        "integration": _owned_fingerprint_tree(
            paths["active_integration"], candidate_integration, authority_context
        ),
        "worker": _owned_fingerprint_tree(
            paths["active_worker"], candidate_worker, authority_context
        ),
    }
    _assert_generation_inventory(run_root, state, authority_context)
    _inject(fault_injector, "upgrade.before_commit_decision")
    _append_journal(
        run_root, "upgrade_commit_decided",
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="journal.upgrade_commit_decided",
    )
    _append_journal(
        run_root, "upgrade_committed",
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="journal.upgrade_committed",
    )

    _append_journal(
        run_root, "rollback_decided",
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="journal.rollback_decided",
    )
    _rename(
        paths["active_integration"], paths["retained_integration"], run_root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="rename.rollback_integration_retain",
    )
    _rename(
        paths["backup_integration"], paths["active_integration"], run_root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="rename.rollback_integration_restore",
    )
    _rename(
        paths["active_worker"], paths["retained_worker"], run_root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="rename.rollback_worker_retain",
    )
    _rename(
        paths["backup_worker"], paths["active_worker"], run_root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="rename.rollback_worker_restore",
    )
    rollback_trees = {
        "integration": _owned_fingerprint_tree(
            paths["active_integration"], baseline_integration, authority_context
        ),
        "worker": _owned_fingerprint_tree(
            paths["active_worker"], baseline_worker, authority_context
        ),
    }
    _owned_fingerprint_tree(
        paths["retained_integration"], candidate_integration, authority_context
    )
    _owned_fingerprint_tree(
        paths["retained_worker"], candidate_worker, authority_context
    )
    _assert_generation_inventory(run_root, state, authority_context)
    _append_journal(
        run_root, "rollback_completed",
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="journal.rollback_completed",
    )
    return upgrade_trees, rollback_trees


def _execute_release_install_rollback_dry_run_under_lease(
    *,
    candidate_evidence: Path,
    rollback_integration_archive: Path,
    rollback_worker_archive: Path,
    options_snapshot: Path,
    run_root: Path,
    rollback_policy: LegacyRollbackPolicy = RECOVERY_BASELINE_POLICY,
    fault_injector: FaultInjector | None = None,
    process_lease: _ExclusiveProcessLease,
) -> DryRunResult:
    process_lease.assert_held()
    authority_context = _open_authority_run_context(
        run_root,
        existing=False,
        process_lease=process_lease,
    )
    root = authority_context.lexical_root
    try:
        return _execute_release_install_rollback_with_authority(
            candidate_evidence=candidate_evidence,
            rollback_integration_archive=rollback_integration_archive,
            rollback_worker_archive=rollback_worker_archive,
            options_snapshot=options_snapshot,
            root=root,
            rollback_policy=rollback_policy,
            fault_injector=fault_injector,
            process_lease=process_lease,
            authority_context=authority_context,
        )
    finally:
        authority_context.close()


def _execute_release_install_rollback_with_authority(
    *,
    candidate_evidence: Path,
    rollback_integration_archive: Path,
    rollback_worker_archive: Path,
    options_snapshot: Path,
    root: Path,
    rollback_policy: LegacyRollbackPolicy,
    fault_injector: FaultInjector | None,
    process_lease: _ExclusiveProcessLease,
    authority_context: _AuthorityRunContext,
) -> DryRunResult:
    process_lease.assert_held()
    authority_context.reprove()
    evidence_witness, evidence_spool = _spool_input(
        candidate_evidence,
        root / "spool" / "candidate" / "release_evidence.json",
        label="candidate evidence",
        maximum_bytes=MAX_INPUT_METADATA_BYTES,
        run_root=root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="input.candidate_evidence",
    )
    evidence_registry = _seal_output_spools(
        authority_context,
        {"evidence": (evidence_spool, MAX_INPUT_METADATA_BYTES)},
    )
    try:
        candidate_integration_name, candidate_worker_name = (
            _candidate_archive_names_from_bound_evidence(evidence_registry)
        )
    finally:
        evidence_registry.close()
    candidate_integration_witness, candidate_integration_spool = _spool_input(
        evidence_witness.path.parent / candidate_integration_name,
        evidence_spool.path.parent / candidate_integration_name,
        label="candidate integration archive",
        maximum_bytes=MAX_TOTAL_BYTES,
        run_root=root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="input.candidate_integration",
    )
    candidate_worker_witness, candidate_worker_spool = _spool_input(
        evidence_witness.path.parent / candidate_worker_name,
        evidence_spool.path.parent / candidate_worker_name,
        label="candidate worker archive",
        maximum_bytes=MAX_TOTAL_BYTES,
        run_root=root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="input.candidate_worker",
    )
    rollback_integration_witness, rollback_integration_spool = _spool_input(
        rollback_integration_archive,
        root / "spool" / "rollback" / "integration.zip",
        label="rollback integration archive",
        maximum_bytes=MAX_TOTAL_BYTES,
        run_root=root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="input.rollback_integration",
    )
    rollback_worker_witness, rollback_worker_spool = _spool_input(
        rollback_worker_archive,
        root / "spool" / "rollback" / "worker.tar.gz",
        label="rollback worker archive",
        maximum_bytes=MAX_TOTAL_BYTES,
        run_root=root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="input.rollback_worker",
    )
    options_witness, options_spool = _spool_input(
        options_snapshot,
        root / "spool" / "options" / "sanitized_options.json",
        label="sanitized options snapshot",
        maximum_bytes=MAX_INPUT_METADATA_BYTES,
        run_root=root,
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="input.options_snapshot",
    )
    options_registry = _seal_output_spools(
        authority_context,
        {"options": (options_spool, MAX_INPUT_METADATA_BYTES)},
    )
    try:
        snapshot = _load_options_snapshot_bound(options_registry)
    finally:
        options_registry.close()
    _inject(fault_injector, "inputs.after_spooled")
    if rollback_integration_witness.sha256 != rollback_policy.integration_sha256:
        raise DryRunError("rollback integration archive is outside the exact allowlist")
    if rollback_worker_witness.sha256 != rollback_policy.worker_sha256:
        raise DryRunError("rollback worker archive is outside the exact allowlist")

    candidate_registry = _seal_output_spools(
        authority_context,
        {
            "evidence": (evidence_spool, MAX_INPUT_METADATA_BYTES),
            "integration": (candidate_integration_spool, MAX_TOTAL_BYTES),
            "worker": (candidate_worker_spool, MAX_TOTAL_BYTES),
        },
    )
    try:
        evidence = verify_release_candidate_evidence_bound_inputs(
            candidate_registry
        )
    except (BoundInputError, ReleaseArtifactError, ValueError) as error:
        raise DryRunError("candidate evidence verification failed") from error
    finally:
        candidate_registry.close()
    if not isinstance(evidence, Mapping):
        raise DryRunError("candidate evidence result is invalid")
    source_trees = evidence.get("source_trees")
    archives = evidence.get("archives")
    if not isinstance(source_trees, dict) or not isinstance(archives, dict):
        raise DryRunError("candidate evidence misses archive trees")
    candidate_integration_tree = _records_from_evidence(
        source_trees["integration"], INTEGRATION_ROOT_NAME
    )
    candidate_worker_tree = _records_from_evidence(
        source_trees["worker"], WORKER_ROOT_NAME
    )
    if (
        archives["integration"]["filename"] != candidate_integration_name
        or archives["worker"]["filename"] != candidate_worker_name
    ):
        raise DryRunError("verified candidate evidence archive names changed")
    candidate_integration_archive = _bound_output_archive(
        authority_context,
        "candidate_integration",
        candidate_integration_spool,
    )
    candidate_worker_archive = _bound_output_archive(
        authority_context,
        "candidate_worker",
        candidate_worker_spool,
    )
    rollback_integration_archive_source = _bound_output_archive(
        authority_context,
        "rollback_integration",
        rollback_integration_spool,
    )
    rollback_worker_archive_source = _bound_output_archive(
        authority_context,
        "rollback_worker",
        rollback_worker_spool,
    )
    try:
        candidate_integration_bound_tree = _bound_archive_tree(
            candidate_integration_tree
        )
        candidate_integration_inspection = validate_candidate_zip(
            candidate_integration_archive,
            "archive",
            candidate_integration_bound_tree,
            limits=_archive_header_limits(),
        )
        if (
            not isinstance(candidate_integration_inspection, BoundArchiveInspection)
            or candidate_integration_inspection.format != "zip"
            or candidate_integration_inspection.tree != candidate_integration_bound_tree
        ):
            raise DryRunError("candidate integration archive inspection differs")
        _record_bound_archive_route(
            authority_context, "candidate_integration_validate"
        )
        candidate_worker_bound_tree = _bound_archive_tree(candidate_worker_tree)
        candidate_worker_inspection = validate_candidate_tar_gz(
            candidate_worker_archive,
            "archive",
            candidate_worker_bound_tree,
            limits=_archive_header_limits(),
        )
        if (
            not isinstance(candidate_worker_inspection, BoundArchiveInspection)
            or candidate_worker_inspection.format != "tar.gz"
            or candidate_worker_inspection.tree != candidate_worker_bound_tree
        ):
            raise DryRunError("candidate worker archive inspection differs")
        _record_bound_archive_route(
            authority_context, "candidate_worker_validate"
        )
        baseline_integration_inspection = inspect_legacy_zip(
            rollback_integration_archive_source,
            "archive",
            INTEGRATION_ROOT_NAME,
            limits=_archive_header_limits(),
        )
        if (
            not isinstance(baseline_integration_inspection, BoundArchiveInspection)
            or baseline_integration_inspection.format != "zip"
        ):
            raise DryRunError("rollback integration archive inspection differs")
        baseline_integration_tree = _tree_record_from_bound_archive(
            baseline_integration_inspection.tree
        )
        _record_bound_archive_route(
            authority_context, "rollback_integration_inspect"
        )
        baseline_worker_inspection = inspect_legacy_tar_gz(
            rollback_worker_archive_source,
            "archive",
            WORKER_ROOT_NAME,
            exact_archive_sha256=rollback_policy.worker_sha256,
            limits=_archive_header_limits(),
        )
        if (
            not isinstance(baseline_worker_inspection, BoundArchiveInspection)
            or baseline_worker_inspection.format != "tar.gz"
        ):
            raise DryRunError("rollback worker archive inspection differs")
        baseline_worker_tree = _tree_record_from_bound_archive(
            baseline_worker_inspection.tree
        )
        _record_bound_archive_route(authority_context, "rollback_worker_inspect")
    except (BoundArchiveError, BoundInputError) as error:
        raise DryRunError("bound archive validation failed closed") from error
    baseline_integration_modes = tuple(sorted({
        item.mode for item in baseline_integration_inspection.observed_modes
    }))
    baseline_worker_modes = tuple(sorted({
        item.mode for item in baseline_worker_inspection.observed_modes
    }))

    for name in ("journal", "stage", "install", "backup", "retained"):
        _mkdir(root / name, root, authority_context)
    paths = _paths(root)
    _extract_bound_archive(
        rollback_integration_archive_source,
        baseline_integration_tree,
        paths["active_integration"],
        root,
        authority_context,
        archive_format="zip",
        mode_policy="legacy",
        route="rollback_integration_extract",
    )
    _extract_bound_archive(
        rollback_worker_archive_source,
        baseline_worker_tree,
        paths["active_worker"],
        root,
        authority_context,
        archive_format="tar.gz",
        mode_policy="legacy",
        route="rollback_worker_extract",
        exact_legacy_sha256=rollback_policy.worker_sha256,
    )
    _extract_bound_archive(
        candidate_integration_archive,
        candidate_integration_tree,
        paths["candidate_integration"],
        root,
        authority_context,
        archive_format="zip",
        mode_policy="candidate",
        route="candidate_integration_extract",
    )
    _extract_bound_archive(
        candidate_worker_archive,
        candidate_worker_tree,
        paths["candidate_worker"],
        root,
        authority_context,
        archive_format="tar.gz",
        mode_policy="candidate",
        route="candidate_worker_extract",
    )

    baseline_integration_identity = _integration_identity(
        paths["active_integration"],
        tree=baseline_integration_tree,
        authority_context=authority_context,
        integration_version=rollback_policy.integration_version,
        worker_version=None,
        legacy_without_build_id=True,
    )
    baseline_worker_identity = _worker_identity(
        paths["active_worker"],
        rollback_policy.worker_version,
        tree=baseline_worker_tree,
        authority_context=authority_context,
    )
    candidate_integration_identity = _integration_identity(
        paths["candidate_integration"],
        tree=candidate_integration_tree,
        authority_context=authority_context,
        integration_version=str(evidence["integration_version"]),
        worker_version=str(evidence["worker_version"]),
        legacy_without_build_id=False,
    )
    candidate_worker_identity = _worker_identity(
        paths["candidate_worker"],
        str(evidence["worker_version"]),
        tree=candidate_worker_tree,
        authority_context=authority_context,
        require_worker_first_activation_contract=True,
    )
    compatibility = _options_compatibility(
        snapshot, baseline_worker_identity, candidate_worker_identity
    )

    baseline_fingerprints = {
        "integration": _owned_fingerprint_tree(
            paths["active_integration"], baseline_integration_tree,
            authority_context,
        ),
        "worker": _owned_fingerprint_tree(
            paths["active_worker"], baseline_worker_tree, authority_context
        ),
    }
    candidate_fingerprints = {
        "integration": _owned_fingerprint_tree(
            paths["candidate_integration"], candidate_integration_tree,
            authority_context,
        ),
        "worker": _owned_fingerprint_tree(
            paths["candidate_worker"], candidate_worker_tree, authority_context
        ),
    }
    state = _state_document(
        candidate_integration=candidate_integration_tree,
        candidate_worker=candidate_worker_tree,
        baseline_integration=baseline_integration_tree,
        baseline_worker=baseline_worker_tree,
    )
    _assert_generation_inventory(root, state, authority_context)
    _write_json_exclusive(
        root / "dry_run_state.json",
        state,
        root,
        authority_context,
    )
    _append_journal(
        root,
        "initialized",
        {
            "component_atomic": True,
            "pair_atomic": False,
            "services_assumed_stopped": True,
        },
        authority_context=authority_context,
        fault_injector=fault_injector,
        fault_prefix="journal.initialized",
    )
    upgrade_trees, rollback_trees = _upgrade_then_rollback(
        root,
        state,
        authority_context,
        fault_injector,
    )

    for witness, label in (
        (evidence_witness, "candidate evidence"),
        (candidate_integration_witness, "candidate integration archive"),
        (candidate_worker_witness, "candidate worker archive"),
        (rollback_integration_witness, "rollback integration archive"),
        (rollback_worker_witness, "rollback worker archive"),
        (options_witness, "sanitized options snapshot"),
        (evidence_spool, "owned candidate evidence spool"),
        (candidate_integration_spool, "owned candidate integration spool"),
        (candidate_worker_spool, "owned candidate worker spool"),
        (rollback_integration_spool, "owned rollback integration spool"),
        (rollback_worker_spool, "owned rollback worker spool"),
        (options_spool, "owned options snapshot spool"),
    ):
        _reprove_witness(witness, label, authority_context)
    journal_result = _read_journal(root, authority_context)
    journal_events = journal_result.events
    journal_head = journal_result.head_sha256
    supervisor_installation_blockers = sorted(set(compatibility["blockers"]))
    unresolved_external_gates = list(EXTERNAL_INSTALLATION_GATES)
    if compatibility["raw_capture_shadow_selected"]:
        unresolved_external_gates.extend(
            _RAW_CAPTURE_EXTERNAL_INSTALLATION_GATES
        )
        unresolved_external_gates.sort()
    input_spools = {
        "candidate_evidence": _spool_report(
            root, evidence_witness, evidence_spool, authority_context
        ),
        "candidate_integration": _spool_report(
            root, candidate_integration_witness, candidate_integration_spool,
            authority_context,
        ),
        "candidate_worker": _spool_report(
            root, candidate_worker_witness, candidate_worker_spool,
            authority_context,
        ),
        "rollback_integration": _spool_report(
            root, rollback_integration_witness, rollback_integration_spool,
            authority_context,
        ),
        "rollback_worker": _spool_report(
            root, rollback_worker_witness, rollback_worker_spool,
            authority_context,
        ),
        "sanitized_options": _spool_report(
            root, options_witness, options_spool, authority_context
        ),
    }
    output_authority_report = authority_context.evidence()
    if (
        output_authority_report["bound_archive_route_count"] != 8
        or output_authority_report["bound_archive_processing_phase_b"] is not True
    ):
        raise DryRunError("bound archive phase-B route proof is incomplete")
    archive_routes_proven = True
    local_acceptance_blockers = list(
        output_authority_report["local_acceptance_blockers"]
    )
    local_acceptance_blockers_closed = list(
        output_authority_report["local_acceptance_blockers_closed"]
    )
    if set(local_acceptance_blockers) | set(local_acceptance_blockers_closed) != set(
        LOCAL_ACCEPTANCE_BLOCKERS
    ) or set(local_acceptance_blockers) & set(local_acceptance_blockers_closed):
        raise DryRunError("local acceptance blocker evidence is inconsistent")
    blocker_namespaces = {
        "external_installation": unresolved_external_gates,
        "local_simulation": local_acceptance_blockers,
        "supervisor_installation": supervisor_installation_blockers,
    }
    blockers = sorted(
        f"{namespace}:{code}"
        for namespace, codes in blocker_namespaces.items()
        for code in codes
    )
    atomic_noreplace_report = {
        **output_authority_report["atomic_capability"],
        "generation_directory_publications": (
            authority_context.atomic.generation_directory_publications
        ),
        "journal_pending_to_final_publications": (
            authority_context.atomic.journal_publications
        ),
    }
    report: dict[str, object] = {
        "archive_inputs_unchanged": True,
        "atomic_noreplace_publication": atomic_noreplace_report,
        "atomic_noreplace_publication_proven": True,
        "baseline": {
            "integration": {
                "archive_sha256": rollback_integration_witness.sha256,
                "identity": baseline_integration_identity,
                "observed_legacy_modes": list(baseline_integration_modes),
                "tree": baseline_fingerprints["integration"],
            },
            "worker": {
                "archive_sha256": rollback_worker_witness.sha256,
                "identity": baseline_worker_identity,
                "observed_legacy_modes": list(baseline_worker_modes),
                "tree": baseline_fingerprints["worker"],
            },
        },
        "bounded_resource_policy": {
            "archive_member_chunk_bytes": ARCHIVE_MEMBER_CHUNK_BYTES,
            "archive_member_count": MAX_ARCHIVE_MEMBER_COUNT,
            "archive_path_depth": MAX_ARCHIVE_PATH_DEPTH,
            "archive_path_utf8_bytes": MAX_ARCHIVE_PATH_BYTES,
            "archive_path_part_utf8_bytes": MAX_ARCHIVE_PART_BYTES,
            "file_count": MAX_FILE_COUNT,
            "input_metadata_bytes": MAX_INPUT_METADATA_BYTES,
            "json_depth": MAX_JSON_DEPTH,
            "json_integer_absolute": MAX_JSON_INTEGER,
            "json_key_utf8_bytes": MAX_JSON_KEY_BYTES,
            "json_nodes": MAX_JSON_NODES,
            "json_string_utf8_bytes": MAX_JSON_STRING_BYTES,
            "member_bytes": MAX_FILE_BYTES,
            "option_count": MAX_OPTIONS,
            "secret_option_count": MAX_SECRET_OPTIONS,
            "total_archive_bytes": MAX_TOTAL_BYTES,
        },
        "candidate": {
            "evidence_file_sha256": evidence_witness.sha256,
            "evidence_internal_sha256": evidence["evidence_sha256"],
            "integration": {
                "archive_sha256": candidate_integration_witness.sha256,
                "identity": candidate_integration_identity,
                "tree": candidate_fingerprints["integration"],
            },
            "worker": {
                "archive_sha256": candidate_worker_witness.sha256,
                "identity": candidate_worker_identity,
                "tree": candidate_fingerprints["worker"],
            },
        },
        "candidate_worker_gzip_header_canonicality_proven": True,
        "component_atomic": True,
        "contract": DRY_RUN_CONTRACT,
        "deployment_performed": False,
        "destination_absence_prechecked": True,
        "directory_durability_proven": False,
        "exclusive_process_lease": process_lease.evidence(),
        "exclusive_process_lease_proven": True,
        "blocker_namespaces": blocker_namespaces,
        "installation_preflight_blockers": blockers,
        "installation_preflight_passed": not blockers,
        "input_sources_reproved_after_simulation": True,
        "input_spools": input_spools,
        "input_spools_exclusive_fsynced_read_only": all(
            item["physical_read_only"] for item in input_spools.values()
        ),
        "input_spools_exclusive_fsynced_sealed": True,
        "journal_event_count": len(journal_events),
        "journal_head_sha256": journal_head,
        "legacy_empty_directory_canonicality_proven": True,
        "legacy_modes_normalized": True,
        "legacy_worker_noncanonical_gzip_header_hash_bound": True,
        "live_paths_touched": False,
        "local_acceptance_blockers": local_acceptance_blockers,
        "local_acceptance_blockers_closed": local_acceptance_blockers_closed,
        "local_simulation_passed": bool(
            output_authority_report["simulator_integration_proven"]
            and not local_acceptance_blockers
        ),
        "network_performed": False,
        "options_compatibility": compatibility,
        "output_authority": output_authority_report,
        "package_cache_entries": 0,
        "pair_atomic": False,
        "real_installation_ready": False,
        "rollback_final_trees": rollback_trees,
        "services_assumed_stopped": True,
        "simulation_completed": True,
        "spooled_inputs_only_after_capture": True,
        "streaming_archive_member_processing": {
            **streaming_archive_member_evidence(),
            "archive_validation_precedes_destination_creation": archive_routes_proven,
            "destination_io_failure_cleanup_proven": bool(
                output_authority_report[
                    "postpublication_final_cleanup_proven"
                ]
                and output_authority_report["path_race_scope"]["local"][
                    "postcreate_and_postpublication_outcomes_classified"
                ]
            ),
        },
        "streaming_archive_member_processing_proven": True,
        "bounded_streaming_archive_header_parsing": {
            "archive_extensions_fail_closed": [
                "gnu_long_name_or_link",
                "pax",
                "zip64",
            ],
            "bounded_metadata_before_stdlib_parse": True,
            "declared_member_and_total_bytes_bounded": True,
            "exact_index_tree_binding": [
                "count",
                "kind",
                "path",
                "size",
            ],
            "same_descriptor_for_preflight_and_stdlib": archive_routes_proven,
            "stdlib_archive_reads_preflighted": output_authority_report[
                "bound_archive_route_count"
            ],
        },
        "bounded_streaming_archive_header_parsing_proven": True,
        "sandbox_directory_barriers_exercised": os.name != "nt",
        "supervisor_build_cache_validated": False,
        "supervisor_installation_blockers": supervisor_installation_blockers,
        "supervisor_options_roundtrip_proven": compatibility[
            "supervisor_options_roundtrip_proven"
        ],
        "upgrade_verified_trees": upgrade_trees,
        "unresolved_external_gates": unresolved_external_gates,
    }
    report_path = root / "dry_run_report.json"
    _write_json_exclusive(report_path, report, root, authority_context)
    expected_report_bytes = _canonical(report) + b"\n"
    try:
        report_bytes = authority_context.reads.read_bounded(
            authority_context.relative(report_path),
            maximum_bytes=MAX_INPUT_METADATA_BYTES,
            expected_size=len(expected_report_bytes),
            expected_sha256=_sha_bytes(expected_report_bytes),
        )
    except OwnedOutputReadError as error:
        raise DryRunError("dry-run report could not be reread by authority") from error
    report_sha256 = _sha_bytes(report_bytes)
    process_lease.assert_held()
    return DryRunResult(report_path, report_sha256, report)


def execute_release_install_rollback_dry_run(
    *,
    candidate_evidence: Path,
    rollback_integration_archive: Path,
    rollback_worker_archive: Path,
    options_snapshot: Path,
    run_root: Path,
    rollback_policy: LegacyRollbackPolicy = RECOVERY_BASELINE_POLICY,
    fault_injector: FaultInjector | None = None,
) -> DryRunResult:
    """Run the complete local simulation under one exclusive OS lease."""

    process_lease = _acquire_process_lease(run_root)
    try:
        result = _execute_release_install_rollback_dry_run_under_lease(
            candidate_evidence=candidate_evidence,
            rollback_integration_archive=rollback_integration_archive,
            rollback_worker_archive=rollback_worker_archive,
            options_snapshot=options_snapshot,
            run_root=run_root,
            rollback_policy=rollback_policy,
            fault_injector=fault_injector,
            process_lease=process_lease,
        )
    except BaseException as primary:
        try:
            process_lease.release()
        except DryRunError as release_error:
            raise DryRunError(
                "dry-run simulation failed and process lease release failed"
            ) from BaseExceptionGroup(
                "dry-run simulation and process lease release failures",
                [primary, release_error],
            )
        raise
    process_lease.release()
    return result


__all__ = [
    "DRY_RUN_CONTRACT",
    "DryRunError",
    "DryRunResult",
    "FAULT_POINTS",
    "LegacyRollbackPolicy",
    "OPTIONS_CONTRACT",
    "PROCESS_LEASE_CONTRACT",
    "RECOVERY_FAULT_POINTS",
    "RECOVERY_BASELINE_POLICY",
    "execute_release_install_rollback_dry_run",
    "recover_release_install_dry_run",
]
