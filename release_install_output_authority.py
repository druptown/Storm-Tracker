"""Descriptor-relative output mutations for the local release simulator.

This module is an integration building block, not an acceptance claim.  It
never opens an absolute path: callers must supply an already-open, authorized
``DirectoryAuthority``.  Every path accepted after that bootstrap is a
``RelativePath`` and every final file is published from a fully validated,
identity-pinned sibling staging file with no-replace semantics.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import io
from itertools import islice
import os
import re
from typing import BinaryIO, Final, Iterable
import uuid

from descriptor_relative_authority import (
    AuthorityConflict,
    AuthorityError,
    AuthorityNotFound,
    AuthorityPostCreateError as DescriptorAuthorityPostCreateError,
    AuthorityPostPublicationError as DescriptorAuthorityPostPublicationError,
    AuthorityUnavailable,
    BoundFile,
    DirectoryAuthority,
    DirectoryEntry,
    NodeIdentity,
    RelativePath,
)
from storm_tracker_worker.archive_member_stream import PRIVATE_KEY_MARKERS


RELEASE_INSTALL_OUTPUT_AUTHORITY_CONTRACT: Final = (
    "stv3-release-install-output-authority-v1"
)
OUTPUT_STREAM_CHUNK_BYTES: Final = 1024 * 1024
MAX_OUTPUT_STREAM_BYTES: Final = 2 * 1024 * 1024 * 1024
MAX_FORBIDDEN_MARKERS: Final = 16
MAX_FORBIDDEN_MARKER_BYTES: Final = 256
MAX_STAGE_CREATE_ATTEMPTS: Final = 8
POSTPUBLICATION_CLEANUP_SUCCEEDED: Final = "succeeded"
POSTPUBLICATION_CLEANUP_FAILED: Final = "failed"

_SHA256: Final = re.compile(r"^[0-9a-f]{64}$")


class OutputAuthorityError(RuntimeError):
    """A descriptor-relative simulator output operation failed closed."""


class OutputAuthorityConflict(OutputAuthorityError):
    """An exclusive output destination or every bounded stage name existed."""


class OutputAuthorityValidationError(OutputAuthorityError):
    """A source body or caller-supplied output contract was invalid."""


class OutputAuthorityIOError(OutputAuthorityError):
    """An authority-backed output I/O operation could not finish."""


class OutputAuthorityPostCreateError(OutputAuthorityIOError):
    """A directory create completed, but exact rollback was not proved."""

    def __init__(
        self,
        *,
        created_identity: NodeIdentity | None,
        relative: RelativePath,
    ) -> None:
        super().__init__("output_directory_created_but_cleanup_failed")
        self.created_identity = created_identity
        self.relative = relative


class OutputAuthorityPostPublicationError(OutputAuthorityError):
    """Publication completed, but its final proof or durability check failed."""

    def __init__(
        self,
        *,
        cleanup_succeeded: bool,
        destination: RelativePath,
        published_identity: NodeIdentity,
        stream: "OutputStreamResult",
    ) -> None:
        super().__init__("output_published_but_postproof_failed")
        self.cleanup_succeeded = cleanup_succeeded
        self.cleanup_status = (
            POSTPUBLICATION_CLEANUP_SUCCEEDED
            if cleanup_succeeded
            else POSTPUBLICATION_CLEANUP_FAILED
        )
        self.destination = destination
        self.published_identity = published_identity
        self.stream = stream


@dataclass(frozen=True, slots=True)
class OutputStreamResult:
    size: int
    sha256: str
    read_calls: int
    write_calls: int


@dataclass(frozen=True, slots=True)
class OutputPublicationResult:
    backend: str
    directory_fsync_supported: bool
    identity: NodeIdentity
    stream: OutputStreamResult


@dataclass(frozen=True, slots=True)
class OutputDirectoryResult:
    directory_fsync_supported: bool
    identity: NodeIdentity
    parent_fsync_supported: bool


def _relative(value: RelativePath, *, allow_root: bool) -> RelativePath:
    if not isinstance(value, RelativePath):
        raise TypeError("output paths must be RelativePath instances")
    if not allow_root and not value.parts:
        raise OutputAuthorityValidationError("output_path_requires_relative_leaf")
    return value


def _mode(value: int, label: str) -> int:
    if type(value) is not int or value < 0 or value > 0o777:
        raise TypeError(f"{label} mode must be an integer from 0 through 0o777")
    return value


def _markers(value: Iterable[bytes]) -> tuple[bytes, ...]:
    if isinstance(value, (str, bytes, bytearray, memoryview)):
        raise OutputAuthorityValidationError("output_forbidden_markers_invalid")
    try:
        materialized = tuple(islice(iter(value), MAX_FORBIDDEN_MARKERS + 1))
    except Exception as error:
        raise OutputAuthorityValidationError(
            "output_forbidden_markers_invalid"
        ) from error
    if (
        len(materialized) > MAX_FORBIDDEN_MARKERS
        or len(set(materialized)) != len(materialized)
        or any(
            not isinstance(marker, bytes)
            or not marker
            or len(marker) > MAX_FORBIDDEN_MARKER_BYTES
            for marker in materialized
        )
    ):
        raise OutputAuthorityValidationError("output_forbidden_markers_invalid")
    return materialized


def _flush_directory(authority: DirectoryAuthority) -> bool:
    try:
        authority.fsync()
    except AuthorityUnavailable:
        return False
    except AuthorityError as error:
        raise OutputAuthorityIOError("output_directory_fsync_failed") from error
    return True


def _remove_published_final(
    parent: DirectoryAuthority,
    destination: RelativePath,
    expected_identity: NodeIdentity,
) -> None:
    """Remove one exact published leaf and prove its durable absence."""

    leaf = RelativePath((destination.leaf,))
    try:
        actual_identity = parent.entry_identity(leaf)
        if (
            not actual_identity.same_object(expected_identity)
            or actual_identity.size != expected_identity.size
            or actual_identity.link_count != expected_identity.link_count
        ):
            raise OutputAuthorityValidationError(
                "output_final_cleanup_identity_changed"
            )
        parent.remove_owned_tree(
            leaf,
            expected_root=expected_identity,
            maximum_entries=1,
        )
        _flush_directory(parent)
        try:
            parent.entry_identity(leaf)
        except AuthorityNotFound:
            return
        raise OutputAuthorityValidationError(
            "output_final_cleanup_absence_unproved"
        )
    except OutputAuthorityError:
        raise
    except AuthorityError as error:
        raise OutputAuthorityIOError("output_final_cleanup_failed") from error


def _set_bound_file_mode(target: BoundFile, mode: int) -> None:
    if os.name == "nt" or not hasattr(os, "fchmod"):
        return
    try:
        os.fchmod(target.native_handle, mode)
    except OSError as error:
        raise OutputAuthorityIOError("output_file_mode_failed") from error


def _set_bound_directory_mode(target: DirectoryAuthority, mode: int) -> None:
    if os.name == "nt" or not hasattr(os, "fchmod"):
        return
    try:
        os.fchmod(target.native_directory_handle, mode)
    except OSError as error:
        raise OutputAuthorityIOError("output_directory_mode_failed") from error


def stream_binary_to_bound_file(
    source: BinaryIO,
    target: BoundFile,
    *,
    expected_size: int,
    maximum_size: int,
    expected_sha256: str | None = None,
    forbidden_markers: Iterable[bytes] = PRIVATE_KEY_MARKERS,
    chunk_bytes: int = OUTPUT_STREAM_CHUNK_BYTES,
) -> OutputStreamResult:
    """Copy one exact bounded body into an already-open authority file."""

    if not isinstance(target, BoundFile) or target.closed:
        raise OutputAuthorityValidationError("output_target_invalid")
    if (
        type(expected_size) is not int
        or type(maximum_size) is not int
        or expected_size < 0
        or maximum_size < 0
        or expected_size > maximum_size
        or maximum_size > MAX_OUTPUT_STREAM_BYTES
    ):
        raise OutputAuthorityValidationError("output_size_bound_invalid")
    if (
        type(chunk_bytes) is not int
        or chunk_bytes < 1
        or chunk_bytes > OUTPUT_STREAM_CHUNK_BYTES
    ):
        raise OutputAuthorityValidationError("output_chunk_bound_invalid")
    if expected_sha256 is not None and (
        not isinstance(expected_sha256, str)
        or _SHA256.fullmatch(expected_sha256) is None
    ):
        raise OutputAuthorityValidationError("output_sha256_invalid")
    try:
        reader = getattr(source, "read", None)
    except Exception as error:
        raise OutputAuthorityValidationError("output_source_invalid") from error
    if not callable(reader):
        raise OutputAuthorityValidationError("output_source_invalid")

    try:
        target.rewind()
        initial_identity = target.identity()
    except AuthorityError as error:
        raise OutputAuthorityIOError("output_target_identity_failed") from error
    if initial_identity.size != 0 or initial_identity.link_count != 1:
        raise OutputAuthorityValidationError("output_target_not_private_empty_file")

    markers = _markers(forbidden_markers)
    overlap = max((len(marker) - 1 for marker in markers), default=0)
    tail = b""
    digest = hashlib.sha256()
    total = 0
    read_calls = 0
    write_calls = 0
    while total < expected_size:
        request = min(chunk_bytes, expected_size - total)
        try:
            chunk = reader(request)
        except Exception as error:
            raise OutputAuthorityValidationError("output_source_read_failed") from error
        read_calls += 1
        if not isinstance(chunk, bytes):
            raise OutputAuthorityValidationError("output_source_returned_nonbytes")
        if not chunk:
            raise OutputAuthorityValidationError("output_body_truncated")
        if len(chunk) > request:
            raise OutputAuthorityValidationError("output_read_bound_exceeded")
        total += len(chunk)
        if total > expected_size or total > maximum_size:
            raise OutputAuthorityValidationError("output_body_exceeds_bound")
        if markers:
            window = tail + chunk
            if any(marker in window for marker in markers):
                raise OutputAuthorityValidationError("output_forbidden_bytes")
            tail = window[-overlap:] if overlap else b""
        digest.update(chunk)
        try:
            target.write_all(chunk)
        except AuthorityError as error:
            raise OutputAuthorityIOError("output_target_write_failed") from error
        write_calls += 1

    try:
        overrun = reader(1)
    except Exception as error:
        raise OutputAuthorityValidationError("output_source_read_failed") from error
    read_calls += 1
    if not isinstance(overrun, bytes):
        raise OutputAuthorityValidationError("output_source_returned_nonbytes")
    if len(overrun) > 1:
        raise OutputAuthorityValidationError("output_read_bound_exceeded")
    if overrun:
        raise OutputAuthorityValidationError("output_body_exceeds_bound")
    observed_sha256 = digest.hexdigest()
    if expected_sha256 is not None and observed_sha256 != expected_sha256:
        raise OutputAuthorityValidationError("output_sha256_mismatch")
    try:
        final_identity = target.identity()
    except AuthorityError as error:
        raise OutputAuthorityIOError("output_target_identity_failed") from error
    if (
        not final_identity.same_object(initial_identity)
        or final_identity.size != total
        or final_identity.link_count != 1
    ):
        raise OutputAuthorityValidationError("output_target_identity_changed")
    return OutputStreamResult(total, observed_sha256, read_calls, write_calls)


class ReleaseInstallOutputAuthority:
    """One pinned simulator run-root below an already authorized directory."""

    __slots__ = (
        "_allowed_root",
        "_closed",
        "_created",
        "_root",
        "_root_identity",
        "_run_relative",
    )

    def __init__(
        self,
        *,
        allowed_root: DirectoryAuthority,
        root: DirectoryAuthority,
        run_relative: RelativePath,
        created: bool,
    ) -> None:
        self._allowed_root = allowed_root
        self._root = root
        self._run_relative = run_relative
        self._root_identity = root.identity()
        self._created = created
        self._closed = False

    @classmethod
    def create_run_root(
        cls,
        allowed_root: DirectoryAuthority,
        run_relative: RelativePath,
        *,
        mode: int = 0o700,
    ) -> "ReleaseInstallOutputAuthority":
        if not isinstance(allowed_root, DirectoryAuthority) or allowed_root.closed:
            raise TypeError("allowed_root must be an open DirectoryAuthority")
        relative = _relative(run_relative, allow_root=False)
        selected_mode = _mode(mode, "run-root")
        try:
            owner = allowed_root.duplicate()
        except AuthorityError as error:
            raise OutputAuthorityIOError("output_allowed_root_duplicate_failed") from error
        root: DirectoryAuthority | None = None
        identity: NodeIdentity | None = None
        transferred = False
        try:
            root = owner.create_directory(relative, mode=selected_mode)
            identity = root.opened_identity
            _set_bound_directory_mode(root, selected_mode)
            _flush_directory(root)
            with owner.open_directory(relative.parent) as parent:
                _flush_directory(parent)
            result = cls(
                allowed_root=owner,
                root=root,
                run_relative=relative,
                created=True,
            )
            transferred = True
            return result
        except AuthorityConflict as error:
            raise OutputAuthorityConflict("output_run_root_exists") from error
        except OutputAuthorityError:
            raise
        except AuthorityError as error:
            raise OutputAuthorityIOError("output_run_root_create_failed") from error
        finally:
            if not transferred:
                cleanup_error: BaseException | None = None
                if root is not None:
                    try:
                        root.close()
                    except AuthorityError as error:
                        cleanup_error = error
                if identity is not None:
                    try:
                        owner.remove_owned_tree(
                            relative,
                            expected_root=identity,
                            maximum_entries=1,
                        )
                        with owner.open_directory(relative.parent) as parent:
                            _flush_directory(parent)
                    except (AuthorityError, OutputAuthorityError) as error:
                        cleanup_error = error
                try:
                    owner.close()
                except AuthorityError as error:
                    cleanup_error = error
                if cleanup_error is not None:
                    raise OutputAuthorityIOError(
                        "output_run_root_create_cleanup_failed"
                    ) from cleanup_error

    @classmethod
    def open_run_root(
        cls,
        allowed_root: DirectoryAuthority,
        run_relative: RelativePath,
    ) -> "ReleaseInstallOutputAuthority":
        if not isinstance(allowed_root, DirectoryAuthority) or allowed_root.closed:
            raise TypeError("allowed_root must be an open DirectoryAuthority")
        relative = _relative(run_relative, allow_root=False)
        owner = allowed_root.duplicate()
        try:
            root = owner.open_directory(relative)
        except AuthorityNotFound as error:
            owner.close()
            raise OutputAuthorityValidationError("output_run_root_absent") from error
        except AuthorityError as error:
            owner.close()
            raise OutputAuthorityIOError("output_run_root_open_failed") from error
        return cls(
            allowed_root=owner,
            root=root,
            run_relative=relative,
            created=False,
        )

    @property
    def backend(self) -> str:
        self._require_open()
        return self._root.backend

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def root_identity(self) -> NodeIdentity:
        self._require_open()
        return self._root_identity

    def _require_open(self) -> None:
        if self._closed:
            raise OutputAuthorityValidationError("output_authority_closed")

    def reprove_run_root(self) -> NodeIdentity:
        self._require_open()
        try:
            with self._allowed_root.open_directory(self._run_relative) as reopened:
                identity = reopened.identity()
        except AuthorityError as error:
            raise OutputAuthorityIOError("output_run_root_reproof_failed") from error
        if not identity.same_object(self._root_identity):
            raise OutputAuthorityValidationError("output_run_root_identity_changed")
        return identity

    def mkdir(
        self,
        relative: RelativePath,
        *,
        mode: int = 0o755,
    ) -> OutputDirectoryResult:
        self._require_open()
        path = _relative(relative, allow_root=False)
        selected_mode = _mode(mode, "directory")
        directory: DirectoryAuthority | None = None
        parent: DirectoryAuthority | None = None
        identity: NodeIdentity | None = None
        result: OutputDirectoryResult | None = None
        primary: BaseException | None = None
        primary_traceback = None
        try:
            directory = self._root.create_directory(path, mode=selected_mode)
            identity = directory.opened_identity
            _set_bound_directory_mode(directory, selected_mode)
            directory_synced = _flush_directory(directory)
            parent = self._root.open_directory(path.parent)
            parent_synced = _flush_directory(parent)
            result = OutputDirectoryResult(
                directory_synced,
                identity,
                parent_synced,
            )
        except DescriptorAuthorityPostCreateError as error:
            identity = error.created_identity
            primary = error
            primary_traceback = error.__traceback__
        except BaseException as error:
            primary = error
            primary_traceback = error.__traceback__

        close_failures: list[BaseException] = []
        if parent is not None and not parent.closed:
            try:
                parent.close()
            except AuthorityError as error:
                close_failures.append(error)
        if directory is not None and not directory.closed:
            try:
                directory.close()
            except AuthorityError as error:
                close_failures.append(error)

        if primary is None and not close_failures:
            if result is None:
                raise OutputAuthorityIOError("output_directory_create_outcome_invalid")
            return result

        creation_possible = identity is not None or isinstance(
            primary, DescriptorAuthorityPostCreateError
        )
        if not creation_possible:
            if close_failures:
                failures = ([primary] if primary is not None else []) + close_failures
                raise OutputAuthorityIOError(
                    "output_directory_create_cleanup_failed"
                ) from BaseExceptionGroup(
                    "output directory pre-create cleanup failures",
                    failures,
                )
            if isinstance(primary, AuthorityConflict):
                raise OutputAuthorityConflict("output_directory_exists") from primary
            if isinstance(primary, OutputAuthorityError):
                raise primary.with_traceback(primary_traceback)
            if isinstance(primary, AuthorityError):
                raise OutputAuthorityIOError(
                    "output_directory_create_failed"
                ) from primary
            if primary is not None:
                raise primary.with_traceback(primary_traceback)
            raise OutputAuthorityIOError("output_directory_create_outcome_invalid")

        cleanup_failures = list(close_failures)
        if identity is None:
            cleanup_failures.append(
                OutputAuthorityIOError("output_directory_created_identity_unknown")
            )
        else:
            try:
                try:
                    actual = self._root.entry_identity(path)
                except AuthorityNotFound:
                    actual = None
                if actual is not None:
                    if not actual.same_object(identity):
                        raise OutputAuthorityValidationError(
                            "output_directory_cleanup_identity_changed"
                        )
                    self._root.remove_owned_tree(
                        path,
                        expected_root=identity,
                        maximum_entries=1,
                    )
                try:
                    self._root.entry_identity(path)
                except AuthorityNotFound:
                    pass
                else:
                    raise OutputAuthorityIOError(
                        "output_directory_cleanup_absence_unproved"
                    )
                with self._root.open_directory(path.parent) as cleanup_parent:
                    _flush_directory(cleanup_parent)
            except (AuthorityError, OutputAuthorityError) as error:
                cleanup_failures.append(error)

        if cleanup_failures:
            failures: list[BaseException] = []
            if primary is not None:
                failures.append(primary)
            failures.extend(cleanup_failures)
            raise OutputAuthorityPostCreateError(
                created_identity=identity,
                relative=path,
            ) from BaseExceptionGroup(
                "output directory create and cleanup failures",
                failures,
            )
        if primary is None:
            raise OutputAuthorityIOError("output_directory_create_outcome_invalid")
        if isinstance(primary, AuthorityConflict):
            raise OutputAuthorityConflict("output_directory_exists") from primary
        if isinstance(primary, OutputAuthorityError):
            raise primary.with_traceback(primary_traceback)
        if isinstance(primary, AuthorityError):
            raise OutputAuthorityIOError("output_directory_create_failed") from primary
        raise primary.with_traceback(primary_traceback)

    def list(
        self,
        relative: RelativePath = RelativePath.root(),
        *,
        maximum_entries: int,
    ) -> tuple[DirectoryEntry, ...]:
        self._require_open()
        path = _relative(relative, allow_root=True)
        try:
            return self._root.list_entries(path, maximum_entries=maximum_entries)
        except AuthorityError as error:
            raise OutputAuthorityIOError("output_directory_list_failed") from error

    def read(self, relative: RelativePath, *, maximum_bytes: int) -> bytes:
        self._require_open()
        path = _relative(relative, allow_root=False)
        try:
            return self._root.read_file(path, maximum_bytes=maximum_bytes)
        except AuthorityError as error:
            raise OutputAuthorityIOError("output_file_read_failed") from error

    def fsync(self, relative: RelativePath = RelativePath.root()) -> bool:
        self._require_open()
        path = _relative(relative, allow_root=True)
        try:
            with self._root.open_directory(path) as directory:
                return _flush_directory(directory)
        except OutputAuthorityError:
            raise
        except AuthorityError as error:
            raise OutputAuthorityIOError("output_directory_open_failed") from error

    def write_bytes_noreplace(
        self,
        relative: RelativePath,
        value: bytes,
        *,
        mode: int = 0o644,
        forbidden_markers: Iterable[bytes] = (),
    ) -> OutputPublicationResult:
        if not isinstance(value, bytes):
            raise TypeError("output bytes must be bytes")
        return self.stream_noreplace(
            relative,
            io.BytesIO(value),
            expected_size=len(value),
            maximum_size=len(value),
            expected_sha256=hashlib.sha256(value).hexdigest(),
            forbidden_markers=forbidden_markers,
            mode=mode,
        )

    def stream_noreplace(
        self,
        relative: RelativePath,
        source: BinaryIO,
        *,
        expected_size: int,
        maximum_size: int,
        expected_sha256: str | None = None,
        forbidden_markers: Iterable[bytes] = PRIVATE_KEY_MARKERS,
        mode: int = 0o644,
        chunk_bytes: int = OUTPUT_STREAM_CHUNK_BYTES,
    ) -> OutputPublicationResult:
        self._require_open()
        destination = _relative(relative, allow_root=False)
        selected_mode = _mode(mode, "file")
        try:
            parent = self._root.open_directory(destination.parent)
        except AuthorityError as error:
            raise OutputAuthorityIOError("output_parent_open_failed") from error
        cleanup_parent: DirectoryAuthority | None = None
        try:
            cleanup_parent = parent.duplicate()
        except AuthorityError as error:
            try:
                parent.close()
            except AuthorityError as close_error:
                raise OutputAuthorityIOError(
                    "output_cleanup_parent_duplicate_failed"
                ) from BaseExceptionGroup(
                    "output cleanup parent duplicate and close failures",
                    [error, close_error],
                )
            raise OutputAuthorityIOError(
                "output_cleanup_parent_duplicate_failed"
            ) from error
        stage = None
        stage_identity: NodeIdentity | None = None
        target: BoundFile | None = None
        published = False
        sealed_identity: NodeIdentity | None = None
        stream_result: OutputStreamResult | None = None
        publication_result: OutputPublicationResult | None = None
        descriptor_postpublication_error: (
            DescriptorAuthorityPostPublicationError | None
        ) = None
        primary: BaseException | None = None
        primary_traceback = None
        try:
            for _attempt in range(MAX_STAGE_CREATE_ATTEMPTS):
                candidate = f".stv3-output-{uuid.uuid4().hex}.pending"
                try:
                    target = parent.create_file(candidate, mode=selected_mode)
                except AuthorityConflict:
                    continue
                stage = RelativePath((candidate,))
                stage_identity = target.identity()
                break
            if target is None or stage is None or stage_identity is None:
                raise OutputAuthorityConflict("output_stage_names_exhausted")
            stream_result = stream_binary_to_bound_file(
                source,
                target,
                expected_size=expected_size,
                maximum_size=maximum_size,
                expected_sha256=expected_sha256,
                forbidden_markers=forbidden_markers,
                chunk_bytes=chunk_bytes,
            )
            _set_bound_file_mode(target, selected_mode)
            try:
                target.flush()
                sealed_identity = target.identity()
            except AuthorityError as error:
                raise OutputAuthorityIOError("output_stage_seal_failed") from error
            if (
                not sealed_identity.same_object(stage_identity)
                or sealed_identity.size != stream_result.size
                or sealed_identity.link_count != 1
            ):
                raise OutputAuthorityValidationError("output_stage_identity_changed")
            target.close()
            target = None
            try:
                backend = parent.publish_noreplace(
                    stage,
                    RelativePath((destination.leaf,)),
                    expected_source=stage_identity,
                )
            except DescriptorAuthorityPostPublicationError as error:
                published = True
                backend = error.backend
                descriptor_postpublication_error = error
                if (
                    error.source != stage
                    or error.destination != RelativePath((destination.leaf,))
                    or not error.published_identity.same_object(stage_identity)
                ):
                    raise OutputAuthorityValidationError(
                        "output_publication_evidence_invalid"
                    ) from error
            except AuthorityConflict as error:
                raise OutputAuthorityConflict("output_destination_exists") from error
            except AuthorityError as error:
                raise OutputAuthorityIOError("output_publication_failed") from error
            published = True
            try:
                final_identity = parent.entry_identity(destination.leaf)
                if (
                    not final_identity.same_object(stage_identity)
                    or final_identity.size != stream_result.size
                    or final_identity.link_count != 1
                ):
                    raise OutputAuthorityValidationError(
                        "output_final_identity_changed"
                    )
                directory_synced = _flush_directory(parent)
            except (AuthorityError, OutputAuthorityError) as error:
                cause: BaseException = error
                if descriptor_postpublication_error is not None:
                    cause = BaseExceptionGroup(
                        "descriptor publication and final proof failures",
                        [descriptor_postpublication_error, error],
                    )
                raise OutputAuthorityIOError(
                    "output_final_postproof_failed"
                ) from cause
            publication_result = OutputPublicationResult(
                backend,
                directory_synced,
                final_identity,
                stream_result,
            )
            if descriptor_postpublication_error is not None:
                raise OutputAuthorityIOError(
                    "output_descriptor_postpublication_failed"
                ) from descriptor_postpublication_error
        except BaseException as error:
            primary = error
            primary_traceback = error.__traceback__

        cleanup_failures: list[BaseException] = []
        if target is not None and not target.closed:
            try:
                target.close()
            except AuthorityError as error:
                cleanup_failures.append(error)
        if not published and stage is not None and stage_identity is not None:
            try:
                parent.remove_owned_tree(
                    stage,
                    expected_root=stage_identity,
                    maximum_entries=1,
                )
                _flush_directory(parent)
            except (AuthorityError, OutputAuthorityError) as error:
                cleanup_failures.append(error)
        if not parent.closed:
            try:
                parent.close()
            except BaseException as error:
                cleanup_failures.append(error)

        if published:
            if sealed_identity is None or stream_result is None:
                raise OutputAuthorityIOError("output_publication_outcome_invalid")
            failures: list[BaseException] = []
            if primary is not None:
                failures.append(primary)
            failures.extend(cleanup_failures)
            if not failures:
                if publication_result is None:
                    raise OutputAuthorityIOError("output_publication_outcome_invalid")
                try:
                    cleanup_parent.close()
                except BaseException as error:
                    failures.append(error)
                    cleanup_parent = None
                else:
                    return publication_result

            final_cleanup_succeeded = False
            final_cleanup_parent = cleanup_parent
            if final_cleanup_parent is None:
                try:
                    final_cleanup_parent = self._root.open_directory(
                        destination.parent
                    )
                except BaseException as error:
                    failures.append(error)
            if final_cleanup_parent is not None:
                try:
                    _remove_published_final(
                        final_cleanup_parent,
                        destination,
                        sealed_identity,
                    )
                    final_cleanup_succeeded = True
                except BaseException as error:
                    failures.append(error)
                finally:
                    if not final_cleanup_parent.closed:
                        try:
                            final_cleanup_parent.close()
                        except BaseException as error:
                            failures.append(error)
            if not parent.closed:
                try:
                    parent.close()
                except BaseException as error:
                    failures.append(error)
            raise OutputAuthorityPostPublicationError(
                cleanup_succeeded=final_cleanup_succeeded,
                destination=destination,
                published_identity=sealed_identity,
                stream=stream_result,
            ) from BaseExceptionGroup(
                "output post-publication and cleanup failures",
                failures,
            )

        if cleanup_parent is not None and not cleanup_parent.closed:
            try:
                cleanup_parent.close()
            except BaseException as error:
                cleanup_failures.append(error)
        if primary is None:
            raise OutputAuthorityIOError("output_publication_outcome_invalid")
        if cleanup_failures:
            raise OutputAuthorityIOError("output_stage_cleanup_failed") from (
                BaseExceptionGroup(
                    "output pre-publication and cleanup failures",
                    [primary, *cleanup_failures],
                )
            )
        raise primary.with_traceback(primary_traceback)

    def remove_created_run_root(self, *, maximum_entries: int) -> bool:
        self._require_open()
        if not self._created:
            raise OutputAuthorityValidationError("output_run_root_not_owned")
        try:
            self._root.close()
            self._allowed_root.remove_owned_tree(
                self._run_relative,
                expected_root=self._root_identity,
                maximum_entries=maximum_entries,
            )
            supported = _flush_directory(self._allowed_root)
        except AuthorityError as error:
            raise OutputAuthorityIOError("output_run_root_cleanup_failed") from error
        self._closed = True
        self._allowed_root.close()
        return supported

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        first_error: AuthorityError | None = None
        try:
            self._root.close()
        except AuthorityError as error:
            first_error = error
        try:
            self._allowed_root.close()
        except AuthorityError as error:
            if first_error is None:
                first_error = error
        if first_error is not None:
            raise OutputAuthorityIOError("output_authority_close_failed") from first_error

    def __enter__(self) -> "ReleaseInstallOutputAuthority":
        self._require_open()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


def output_authority_evidence() -> dict[str, object]:
    """Describe this isolated primitive without claiming simulator closure."""

    postcreate_failure_outcomes_classified = True
    postpublication_failure_outcomes_classified = True
    return {
        "contract": RELEASE_INSTALL_OUTPUT_AUTHORITY_CONTRACT,
        "absolute_path_reopen": False,
        "descriptor_relative_mutations": True,
        "expected_source_identity_required": True,
        "final_files_staged_and_noreplace": True,
        "identity_pinned_stage_cleanup": True,
        "postcreate_failure_outcomes_classified": (
            postcreate_failure_outcomes_classified
        ),
        "postpublication_failure_outcomes_classified": (
            postpublication_failure_outcomes_classified
        ),
        "postcreate_and_postpublication_outcomes_classified": (
            postcreate_failure_outcomes_classified
            and postpublication_failure_outcomes_classified
        ),
        "postpublication_final_cleanup_proven": True,
        "publication_parent_identity_pinned": True,
        "private_marker_stream_gate": True,
        "source_consumption_and_destination_identity_reproved": True,
        "process_lease_integration_proven": False,
        "atomic_publication_adapter_integrated": False,
        "simulator_integration_proven": False,
        "local_acceptance_blockers_closed": [],
    }


__all__ = [
    "MAX_FORBIDDEN_MARKER_BYTES",
    "MAX_FORBIDDEN_MARKERS",
    "MAX_OUTPUT_STREAM_BYTES",
    "MAX_STAGE_CREATE_ATTEMPTS",
    "OUTPUT_STREAM_CHUNK_BYTES",
    "POSTPUBLICATION_CLEANUP_FAILED",
    "POSTPUBLICATION_CLEANUP_SUCCEEDED",
    "RELEASE_INSTALL_OUTPUT_AUTHORITY_CONTRACT",
    "OutputAuthorityConflict",
    "OutputAuthorityError",
    "OutputAuthorityIOError",
    "OutputAuthorityPostCreateError",
    "OutputAuthorityPostPublicationError",
    "OutputAuthorityValidationError",
    "OutputDirectoryResult",
    "OutputPublicationResult",
    "OutputStreamResult",
    "ReleaseInstallOutputAuthority",
    "output_authority_evidence",
    "stream_binary_to_bound_file",
]
