"""Atomic no-replace adapter for one already-open directory authority.

No API in this module accepts or reopens a filesystem path.  The destructive
capability probe operates below an already-open
``descriptor_relative_authority.DirectoryAuthority`` and binds its result to
that exact root identity.  Publication then requires the exact source identity
and delegates to the descriptor/handle-relative primitive.

This is a standalone phase-2 seam.  It deliberately claims neither local
simulator blocker: the simulator does not yet use it and allowed-root bootstrap
policy remains outside this module.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Final
import uuid

from atomic_noreplace_publication import (
    AtomicNoReplaceConflict,
    AtomicNoReplaceError,
    AtomicNoReplaceFailure,
    AtomicNoReplaceUnavailable,
)
from descriptor_relative_authority import (
    AuthorityConflict,
    AuthorityError,
    AuthorityNotFound,
    AuthorityUnavailable,
    AuthorityViolation,
    DirectoryAuthority,
    NodeIdentity,
    RelativePath,
)


DESCRIPTOR_RELATIVE_ATOMIC_ADAPTER_CONTRACT: Final = (
    "stv3-descriptor-relative-atomic-adapter-v1"
)
OPEN_AUTHORITY_ATOMIC_PROBE_CONTRACT: Final = (
    "stv3-open-directory-authority-atomic-probe-v1"
)
_PUBLICATION_KINDS: Final = frozenset(
    {"generation_directory", "journal_pending_to_final"}
)
_PROBE_MAXIMUM_ENTRIES: Final = 64


def _root_identity(
    authority: DirectoryAuthority,
    expected: NodeIdentity,
    label: str,
) -> NodeIdentity:
    try:
        actual = authority.identity()
    except AuthorityError as error:
        raise AtomicNoReplaceFailure(f"{label}_query_failed") from error
    if (
        expected.backend != authority.backend
        or actual.backend != authority.backend
        or actual.kind != "directory"
        or not actual.same_object(expected)
    ):
        raise AtomicNoReplaceFailure(f"{label}_changed")
    return actual


def _translate_authority_error(error: AuthorityError) -> AtomicNoReplaceError:
    if isinstance(error, AuthorityConflict):
        return AtomicNoReplaceConflict("atomic_noreplace_destination_exists")
    if isinstance(error, AuthorityUnavailable):
        return AtomicNoReplaceUnavailable(
            "descriptor_relative_atomic_backend_unavailable"
        )
    if isinstance(error, AuthorityNotFound):
        return AtomicNoReplaceFailure(
            "descriptor_relative_atomic_source_not_found"
        )
    if isinstance(error, AuthorityViolation):
        return AtomicNoReplaceFailure(
            "descriptor_relative_atomic_authority_violation"
        )
    return AtomicNoReplaceFailure("descriptor_relative_atomic_io_failed")


def _backend_family(backend: str) -> str:
    if backend.startswith("linux_"):
        return "linux"
    if backend.startswith("darwin_"):
        return "darwin"
    if backend.startswith("windows_"):
        return "windows"
    if backend == "posix_openat":
        return "posix"
    raise AtomicNoReplaceUnavailable(
        "descriptor_relative_atomic_backend_family_unknown"
    )


def _publication_matches_authority(
    authority_backend: str,
    publication_backend: str,
) -> bool:
    authority_family = _backend_family(authority_backend)
    publication_family = _backend_family(publication_backend)
    if authority_family == "posix":
        return publication_family in {"linux", "darwin"}
    return authority_family == publication_family


def _entry_identity_or_failure(
    authority: DirectoryAuthority,
    relative: RelativePath,
    label: str,
) -> NodeIdentity:
    try:
        identity = authority.entry_identity(relative)
    except AuthorityError as error:
        raise AtomicNoReplaceFailure(f"{label}_identity_reproof_failed") from error
    if identity.backend != authority.backend:
        raise AtomicNoReplaceFailure(f"{label}_identity_backend_changed")
    return identity


def _optional_entry_identity(
    authority: DirectoryAuthority,
    relative: RelativePath,
    label: str,
) -> NodeIdentity | None:
    try:
        identity = authority.entry_identity(relative)
    except AuthorityNotFound:
        return None
    except AuthorityError as error:
        raise AtomicNoReplaceFailure(f"{label}_identity_reproof_failed") from error
    if identity.backend != authority.backend:
        raise AtomicNoReplaceFailure(f"{label}_identity_backend_changed")
    return identity


def _same_bound_object(actual: NodeIdentity, expected: NodeIdentity) -> bool:
    return actual.backend == expected.backend and actual.same_object(expected)


def _prove_consumed(
    authority: DirectoryAuthority,
    source: RelativePath,
    destination: RelativePath,
    expected_source: NodeIdentity,
    label: str,
) -> None:
    try:
        authority.entry_identity(source)
    except AuthorityNotFound:
        pass
    except AuthorityError as error:
        raise AtomicNoReplaceFailure(
            f"{label}_source_consumption_reproof_failed"
        ) from error
    else:
        raise AtomicNoReplaceFailure(f"{label}_source_not_consumed")
    published = _entry_identity_or_failure(authority, destination, label)
    if not _same_bound_object(published, expected_source):
        raise AtomicNoReplaceFailure(f"{label}_destination_identity_changed")


def _prove_conflict_preserved(
    authority: DirectoryAuthority,
    source: RelativePath,
    destination: RelativePath,
    expected_source: NodeIdentity,
    expected_destination: NodeIdentity,
    label: str,
) -> None:
    actual_source = _entry_identity_or_failure(authority, source, label)
    actual_destination = _entry_identity_or_failure(authority, destination, label)
    if not _same_bound_object(actual_source, expected_source):
        raise AtomicNoReplaceFailure(f"{label}_conflict_changed_source")
    if not _same_bound_object(actual_destination, expected_destination):
        raise AtomicNoReplaceFailure(f"{label}_conflict_changed_destination")


def _prove_conflict_outcome(
    authority: DirectoryAuthority,
    source: RelativePath,
    destination: RelativePath,
    expected_source: NodeIdentity,
    destination_before: NodeIdentity | None,
    label: str,
) -> None:
    actual_source = _entry_identity_or_failure(authority, source, label)
    if not _same_bound_object(actual_source, expected_source):
        raise AtomicNoReplaceFailure(f"{label}_conflict_changed_source")
    actual_destination = _entry_identity_or_failure(authority, destination, label)
    if destination_before is not None and not _same_bound_object(
        actual_destination,
        destination_before,
    ):
        raise AtomicNoReplaceFailure(f"{label}_conflict_changed_destination")


def _write_probe_file(
    authority: DirectoryAuthority,
    relative: RelativePath,
    value: bytes,
) -> NodeIdentity:
    try:
        with authority.create_file(relative) as bound:
            bound.write_all(value)
            bound.flush()
            return bound.identity()
    except AuthorityError as error:
        raise AtomicNoReplaceFailure("open_authority_probe_file_write_failed") from error


@dataclass(frozen=True, slots=True)
class OpenAuthorityAtomicCapability:
    """Proof bound to one already-open root identity and one backend pair."""

    authority_backend: str
    publication_backend: str
    root_identity: NodeIdentity
    file_publication_succeeded: bool
    file_conflict_preserved: bool
    directory_publication_succeeded: bool
    directory_conflict_preserved: bool
    ancestor_pin_mode: str
    listing_reproved: bool
    directory_fsync_supported: bool
    root_identity_reproved: bool
    cleanup_completed: bool

    @property
    def proven(self) -> bool:
        try:
            backend_matches = _publication_matches_authority(
                self.authority_backend,
                self.publication_backend,
            )
        except AtomicNoReplaceError:
            return False
        fsync_proven = self.directory_fsync_supported or (
            _backend_family(self.authority_backend) == "windows"
        )
        return (
            backend_matches
            and self.root_identity.backend == self.authority_backend
            and self.root_identity.kind == "directory"
            and self.file_publication_succeeded
            and self.file_conflict_preserved
            and self.directory_publication_succeeded
            and self.directory_conflict_preserved
            and self.ancestor_pin_mode
            in {"descriptor_survived_rename", "rename_blocked_while_pinned"}
            and self.listing_reproved
            and fsync_proven
            and self.root_identity_reproved
            and self.cleanup_completed
        )

    def to_mapping(self) -> dict[str, object]:
        return {
            "ancestor_pin_mode": self.ancestor_pin_mode,
            "ancestor_pin_proven": self.ancestor_pin_mode
            in {"descriptor_survived_rename", "rename_blocked_while_pinned"},
            "authority_backend": self.authority_backend,
            "backend": self.publication_backend,
            "cleanup_completed": self.cleanup_completed,
            "contract": OPEN_AUTHORITY_ATOMIC_PROBE_CONTRACT,
            "descriptor_relative_create_read_list": True,
            "descriptor_relative_probe": True,
            "directory_conflict_rejected": self.directory_conflict_preserved,
            "directory_fsync_supported": self.directory_fsync_supported,
            "directory_publication_succeeded": (
                self.directory_publication_succeeded
            ),
            "file_conflict_rejected": self.file_conflict_preserved,
            "file_publication_succeeded": self.file_publication_succeeded,
            "listing_reproved": self.listing_reproved,
            "local_acceptance_blockers_closed": [],
            "opened_root_identity_bound": True,
            "path_reopen_used": False,
            "path_authority_scope": "already_open_directory_authority_handle",
            "primitive_proven": self.proven,
            "root_identity_reproved": self.root_identity_reproved,
            "same_filesystem": True,
            "simulator_integration_proven": False,
            "unopened_ancestors_pinned": False,
        }


def probe_open_authority(
    authority: DirectoryAuthority,
    root_identity: NodeIdentity,
) -> OpenAuthorityAtomicCapability:
    """Probe atomic publication below ``authority`` without reopening a path."""

    if not isinstance(authority, DirectoryAuthority):
        raise TypeError("authority must be a DirectoryAuthority")
    if not isinstance(root_identity, NodeIdentity):
        raise TypeError("root_identity must be a NodeIdentity")
    _root_identity(authority, root_identity, "open_authority_probe_root_before")

    probe_relative = RelativePath.parse(
        f".stv3-open-authority-atomic-{uuid.uuid4().hex}"
    )
    probe: DirectoryAuthority | None = None
    probe_identity: NodeIdentity | None = None
    owned = False
    result: OpenAuthorityAtomicCapability | None = None
    try:
        try:
            probe = authority.create_directory(probe_relative)
        except AuthorityError as error:
            raise _translate_authority_error(error) from error
        owned = True
        probe_identity = probe.opened_identity

        file_source = RelativePath.parse("file-source")
        file_destination = RelativePath.parse("file-destination")
        file_identity = _write_probe_file(probe, file_source, b"file-source\n")
        try:
            publication_backend = probe.publish_noreplace(
                file_source,
                file_destination,
                expected_source=file_identity,
            )
        except AuthorityError as error:
            raise _translate_authority_error(error) from error
        _prove_consumed(
            probe,
            file_source,
            file_destination,
            file_identity,
            "open_authority_file_success",
        )

        file_conflict = RelativePath.parse("file-conflict")
        conflict_identity = _write_probe_file(
            probe, file_conflict, b"file-conflict\n"
        )
        destination_identity = _entry_identity_or_failure(
            probe, file_destination, "open_authority_file_conflict"
        )
        try:
            probe.publish_noreplace(
                file_conflict,
                file_destination,
                expected_source=conflict_identity,
            )
        except AuthorityConflict:
            pass
        except AuthorityError as error:
            raise _translate_authority_error(error) from error
        else:
            raise AtomicNoReplaceFailure(
                "open_authority_file_conflict_not_rejected"
            )
        _prove_conflict_preserved(
            probe,
            file_conflict,
            file_destination,
            conflict_identity,
            destination_identity,
            "open_authority_file_conflict",
        )

        directory_source = RelativePath.parse("directory-source")
        try:
            with probe.create_directory(directory_source) as source_directory:
                directory_identity = source_directory.opened_identity
                _write_probe_file(
                    source_directory,
                    RelativePath.parse("sentinel"),
                    b"directory-source\n",
                )
        except AuthorityError as error:
            raise _translate_authority_error(error) from error
        directory_destination = RelativePath.parse("directory-destination")
        try:
            directory_backend = probe.publish_noreplace(
                directory_source,
                directory_destination,
                expected_source=directory_identity,
            )
        except AuthorityError as error:
            raise _translate_authority_error(error) from error
        if directory_backend != publication_backend:
            raise AtomicNoReplaceFailure("open_authority_probe_backend_changed")
        _prove_consumed(
            probe,
            directory_source,
            directory_destination,
            directory_identity,
            "open_authority_directory_success",
        )
        try:
            with probe.open_directory(directory_destination) as published_directory:
                if published_directory.read_file(
                    "sentinel", maximum_bytes=64
                ) != b"directory-source\n":
                    raise AtomicNoReplaceFailure(
                        "open_authority_directory_payload_changed"
                    )
        except AuthorityError as error:
            raise AtomicNoReplaceFailure(
                "open_authority_directory_payload_reproof_failed"
            ) from error

        conflict_source_path = RelativePath.parse("directory-conflict-source")
        conflict_destination_path = RelativePath.parse(
            "directory-conflict-destination"
        )
        try:
            with probe.create_directory(
                conflict_source_path
            ) as conflict_source_directory:
                conflict_source_directory_identity = (
                    conflict_source_directory.opened_identity
                )
                _write_probe_file(
                    conflict_source_directory,
                    RelativePath.parse("source"),
                    b"source\n",
                )
            with probe.create_directory(
                conflict_destination_path
            ) as conflict_destination_directory:
                conflict_destination_identity = (
                    conflict_destination_directory.opened_identity
                )
                _write_probe_file(
                    conflict_destination_directory,
                    RelativePath.parse("destination"),
                    b"destination\n",
                )
        except AuthorityError as error:
            raise _translate_authority_error(error) from error
        try:
            probe.publish_noreplace(
                conflict_source_path,
                conflict_destination_path,
                expected_source=conflict_source_directory_identity,
            )
        except AuthorityConflict:
            pass
        except AuthorityError as error:
            raise _translate_authority_error(error) from error
        else:
            raise AtomicNoReplaceFailure(
                "open_authority_directory_conflict_not_rejected"
            )
        _prove_conflict_preserved(
            probe,
            conflict_source_path,
            conflict_destination_path,
            conflict_source_directory_identity,
            conflict_destination_identity,
            "open_authority_directory_conflict",
        )

        pin_source = RelativePath.parse("pin-source")
        pin_destination = RelativePath.parse("pin-destination")
        try:
            pinned = probe.create_directory(pin_source)
        except AuthorityError as error:
            raise _translate_authority_error(error) from error
        try:
            pinned_identity = pinned.opened_identity
            if authority.backend.startswith("windows_"):
                try:
                    probe.publish_noreplace(
                        pin_source,
                        pin_destination,
                        expected_source=pinned_identity,
                    )
                except AuthorityViolation as error:
                    if "pinned_against_delete" not in str(error):
                        raise _translate_authority_error(error) from error
                except AuthorityError as error:
                    raise _translate_authority_error(error) from error
                else:
                    raise AtomicNoReplaceFailure(
                        "open_authority_windows_pin_did_not_block_rename"
                    )
            else:
                try:
                    pin_backend = probe.publish_noreplace(
                        pin_source,
                        pin_destination,
                        expected_source=pinned_identity,
                    )
                    _write_probe_file(
                        pinned,
                        RelativePath.parse("inside"),
                        b"pinned\n",
                    )
                except AuthorityError as error:
                    raise _translate_authority_error(error) from error
        finally:
            if not pinned.closed:
                pinned.close()
        if authority.backend.startswith("windows_"):
            try:
                pin_backend = probe.publish_noreplace(
                    pin_source,
                    pin_destination,
                    expected_source=pinned_identity,
                )
            except AuthorityError as error:
                raise _translate_authority_error(error) from error
            ancestor_pin_mode = "rename_blocked_while_pinned"
        else:
            try:
                with probe.open_directory(pin_destination) as moved:
                    if moved.read_file("inside", maximum_bytes=64) != b"pinned\n":
                        raise AtomicNoReplaceFailure(
                            "open_authority_posix_pin_redirected"
                        )
            except AuthorityError as error:
                raise _translate_authority_error(error) from error
            ancestor_pin_mode = "descriptor_survived_rename"
        if pin_backend != publication_backend:
            raise AtomicNoReplaceFailure("open_authority_probe_backend_changed")
        _prove_consumed(
            probe,
            pin_source,
            pin_destination,
            pinned_identity,
            "open_authority_pin_success",
        )

        try:
            entries = probe.list_entries(maximum_entries=_PROBE_MAXIMUM_ENTRIES)
        except AuthorityError as error:
            raise AtomicNoReplaceFailure(
                "open_authority_probe_listing_failed"
            ) from error
        required = {
            "file-destination",
            "file-conflict",
            "directory-destination",
            "directory-conflict-source",
            "directory-conflict-destination",
            "pin-destination",
        }
        if not required.issubset({entry.name for entry in entries}):
            raise AtomicNoReplaceFailure("open_authority_probe_listing_changed")
        try:
            probe.fsync()
        except AuthorityUnavailable:
            directory_fsync_supported = False
            if not authority.backend.startswith("windows_"):
                raise AtomicNoReplaceFailure(
                    "open_authority_probe_directory_fsync_unavailable"
                )
        except AuthorityError as error:
            raise AtomicNoReplaceFailure(
                "open_authority_probe_directory_fsync_failed"
            ) from error
        else:
            directory_fsync_supported = True

        _root_identity(
            authority,
            root_identity,
            "open_authority_probe_root_after_operations",
        )
        result = OpenAuthorityAtomicCapability(
            authority_backend=authority.backend,
            publication_backend=publication_backend,
            root_identity=root_identity,
            file_publication_succeeded=True,
            file_conflict_preserved=True,
            directory_publication_succeeded=True,
            directory_conflict_preserved=True,
            ancestor_pin_mode=ancestor_pin_mode,
            listing_reproved=True,
            directory_fsync_supported=directory_fsync_supported,
            root_identity_reproved=True,
            cleanup_completed=True,
        )
    finally:
        cleanup_error: BaseException | None = None
        if probe is not None and not probe.closed:
            try:
                probe.close()
            except BaseException as error:
                cleanup_error = error
        if owned and probe_identity is not None:
            try:
                authority.remove_owned_tree(
                    probe_relative,
                    expected_root=probe_identity,
                    maximum_entries=_PROBE_MAXIMUM_ENTRIES,
                )
                try:
                    authority.fsync()
                except AuthorityUnavailable:
                    if not authority.backend.startswith("windows_"):
                        raise
            except BaseException as error:
                if cleanup_error is None:
                    cleanup_error = error
        if cleanup_error is not None:
            raise AtomicNoReplaceFailure(
                "open_authority_probe_cleanup_failed"
            ) from cleanup_error

    _root_identity(
        authority,
        root_identity,
        "open_authority_probe_root_after_cleanup",
    )
    if result is None or not result.proven:
        raise AtomicNoReplaceFailure("open_authority_probe_not_proven")
    return result


@dataclass(slots=True)
class DescriptorRelativeAtomicAdapter:
    """Counted, fail-closed atomic publication under one pinned root."""

    authority: DirectoryAuthority
    root_identity: NodeIdentity
    capability: OpenAuthorityAtomicCapability
    journal_publications: int = 0
    generation_directory_publications: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.authority, DirectoryAuthority):
            raise TypeError("authority must be a DirectoryAuthority")
        if not isinstance(self.root_identity, NodeIdentity):
            raise TypeError("root_identity must be a NodeIdentity")
        if not isinstance(self.capability, OpenAuthorityAtomicCapability):
            raise TypeError(
                "capability must be an OpenAuthorityAtomicCapability"
            )
        if not self.capability.proven:
            raise AtomicNoReplaceUnavailable(
                "open_authority_atomic_capability_not_proven"
            )
        _root_identity(self.authority, self.root_identity, "authority_root_before_bind")
        if not self.capability.root_identity.same_object(self.root_identity):
            raise AtomicNoReplaceUnavailable(
                "open_authority_atomic_capability_root_mismatch"
            )
        if self.capability.authority_backend != self.authority.backend:
            raise AtomicNoReplaceUnavailable(
                "open_authority_atomic_capability_backend_mismatch"
            )

    def publish(
        self,
        source: RelativePath,
        destination: RelativePath,
        *,
        expected_source: NodeIdentity,
        kind: str,
    ) -> str:
        if type(source) is not RelativePath or type(destination) is not RelativePath:
            raise TypeError("publication paths must be validated RelativePath values")
        if not source.parts or not destination.parts:
            raise AtomicNoReplaceFailure("publication_requires_source_and_destination")
        if not isinstance(expected_source, NodeIdentity):
            raise TypeError("expected_source must be a NodeIdentity")
        if kind not in _PUBLICATION_KINDS:
            raise AtomicNoReplaceFailure("atomic_noreplace_publication_kind_unknown")
        if kind == "journal_pending_to_final" and expected_source.kind != "regular_file":
            raise AtomicNoReplaceFailure("journal_publication_source_kind_invalid")
        if kind == "generation_directory" and expected_source.kind != "directory":
            raise AtomicNoReplaceFailure("generation_publication_source_kind_invalid")
        if expected_source.backend != self.authority.backend:
            raise AtomicNoReplaceFailure("publication_source_backend_mismatch")

        _root_identity(
            self.authority,
            self.root_identity,
            "authority_root_before_publication",
        )
        source_before = _entry_identity_or_failure(
            self.authority,
            source,
            "atomic_noreplace_source_before_publication",
        )
        if not _same_bound_object(source_before, expected_source):
            raise AtomicNoReplaceFailure(
                "atomic_noreplace_source_identity_changed_before_publication"
            )
        destination_before = _optional_entry_identity(
            self.authority,
            destination,
            "atomic_noreplace_destination_before_publication",
        )
        try:
            backend = self.authority.publish_noreplace(
                source,
                destination,
                expected_source=expected_source,
            )
        except AuthorityConflict as error:
            _root_identity(
                self.authority,
                self.root_identity,
                "authority_root_after_publication_conflict",
            )
            _prove_conflict_outcome(
                self.authority,
                source,
                destination,
                expected_source,
                destination_before,
                "atomic_noreplace_publication",
            )
            raise AtomicNoReplaceConflict(
                "atomic_noreplace_destination_exists"
            ) from error
        except AuthorityError as error:
            _root_identity(
                self.authority,
                self.root_identity,
                "authority_root_after_publication_failure",
            )
            raise _translate_authority_error(error) from error
        _root_identity(
            self.authority,
            self.root_identity,
            "authority_root_after_publication",
        )
        if backend != self.capability.publication_backend:
            raise AtomicNoReplaceFailure(
                "atomic_noreplace_backend_changed_after_probe"
            )
        _prove_consumed(
            self.authority,
            source,
            destination,
            expected_source,
            "atomic_noreplace_publication",
        )

        if kind == "journal_pending_to_final":
            self.journal_publications += 1
        else:
            self.generation_directory_publications += 1
        return backend

    def evidence(self, *, include_publication_counts: bool = True) -> dict[str, object]:
        evidence: dict[str, object] = {
            **self.capability.to_mapping(),
            "adapter_contract": DESCRIPTOR_RELATIVE_ATOMIC_ADAPTER_CONTRACT,
            "allowed_root_bootstrap_proven": False,
            "ancestor_and_final_output_path_race_exclusion_proven": False,
            "descriptor_relative_output_authority_proven": False,
            "expected_source_identity_required": True,
            "local_acceptance_blockers_closed": [],
            "path_reopen_used": False,
            "publication_scopes": sorted(_PUBLICATION_KINDS),
            "root_identity_reproved_before_and_after_publication": True,
            "simulator_integration_proven": False,
            "source_consumption_and_destination_identity_reproved": True,
        }
        if include_publication_counts:
            evidence.update(
                generation_directory_publications=(
                    self.generation_directory_publications
                ),
                journal_pending_to_final_publications=(
                    self.journal_publications
                ),
            )
        return evidence


__all__ = [
    "DESCRIPTOR_RELATIVE_ATOMIC_ADAPTER_CONTRACT",
    "OPEN_AUTHORITY_ATOMIC_PROBE_CONTRACT",
    "DescriptorRelativeAtomicAdapter",
    "OpenAuthorityAtomicCapability",
    "probe_open_authority",
]
