"""Path-free archive inspection and extraction over sealed input streams.

Every public operation consumes exactly one ``BoundInputSource.open_stream``
context.  The bounded header parser, the standard-library container reader and
all member bodies therefore share one descriptor whose registry performs its
normal post-use identity, size and SHA-256 proof.  No function in this module
accepts or reopens a filesystem path.

This is an isolated integration primitive.  It intentionally makes no claim
that the release simulator or either remaining output-authority blocker has
been closed.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
from itertools import islice
import os
from pathlib import PurePosixPath
import re
import stat
import tarfile
from typing import BinaryIO, Final, Iterable, Iterator, Literal, Protocol, cast
import zipfile

from release_install_bound_inputs import (
    BoundInputMetadata,
    BoundInputSource,
)
from storm_tracker_worker.archive_member_stream import (
    ARCHIVE_MEMBER_CHUNK_BYTES,
    ArchiveMemberStreamError,
    PRIVATE_KEY_MARKERS,
    stream_archive_member,
)
from storm_tracker_worker.bounded_archive_headers import (
    ArchiveHeaderError,
    ArchiveHeaderIndex,
    ArchiveHeaderLimits,
    DEFAULT_ARCHIVE_HEADER_LIMITS,
    validate_tar_gz_header_stream,
    validate_zip_header_stream,
)


BOUND_ARCHIVE_READER_CONTRACT: Final = "stv3-release-install-bound-archives-v1"
CANONICAL_GZIP_HEADER: Final = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff"
MAX_TREE_RECORDS: Final = 20_000
MAX_MARKERS: Final = 16
MAX_MARKER_BYTES: Final = 256

ArchiveExtractionModePolicy = Literal["candidate", "legacy"]

_SHA256: Final = re.compile(r"^[0-9a-f]{64}$")
_WINDOWS_RESERVED: Final = frozenset(
    {
        "aux",
        "con",
        "nul",
        "prn",
        *(f"com{number}" for number in range(1, 10)),
        *(f"lpt{number}" for number in range(1, 10)),
    }
)


class BoundArchiveError(RuntimeError):
    """Base error for a path-free bound archive operation."""


class BoundArchiveContractError(BoundArchiveError):
    """The caller supplied an invalid archive contract."""


class BoundArchiveIntegrityError(BoundArchiveError):
    """Archive metadata or payload differed from its exact contract."""


class BoundArchiveMemberError(BoundArchiveIntegrityError):
    """A streamed archive member violated its bounded body contract."""


def _portable_relative(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or "\0" in value:
        raise BoundArchiveContractError(f"{label}_path_invalid")
    try:
        value.encode("utf-8")
    except UnicodeError as error:
        raise BoundArchiveContractError(f"{label}_path_invalid") from error
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or not path.parts
        or "." in path.parts
        or ".." in path.parts
    ):
        raise BoundArchiveContractError(f"{label}_path_invalid")
    for part in path.parts:
        if (
            not part
            or ":" in part
            or part.endswith((".", " "))
            or any(ord(character) < 32 for character in part)
            or part.split(".", 1)[0].casefold() in _WINDOWS_RESERVED
        ):
            raise BoundArchiveContractError(f"{label}_path_not_portable")
    return value


def _root_name(value: object) -> str:
    checked = _portable_relative(value, "archive_root")
    if "/" in checked:
        raise BoundArchiveContractError("archive_root_not_single_component")
    return checked


def _canonical_mode(path: str) -> int:
    return 0o755 if path.endswith(".sh") else 0o644


@dataclass(frozen=True, slots=True)
class BoundArchiveFile:
    """One exact root-relative regular-file body."""

    path: str
    size: int
    sha256: str
    mode: int

    def __post_init__(self) -> None:
        _portable_relative(self.path, "archive_file")
        if type(self.size) is not int or self.size < 0:
            raise BoundArchiveContractError("archive_file_size_invalid")
        if not isinstance(self.sha256, str) or _SHA256.fullmatch(self.sha256) is None:
            raise BoundArchiveContractError("archive_file_sha256_invalid")
        if type(self.mode) is not int or self.mode < 0 or self.mode > 0o777:
            raise BoundArchiveContractError("archive_file_mode_invalid")


def _directory_sort_key(value: str) -> tuple[int, str]:
    return value.count("/"), value


def _logical_directories(
    explicit: Iterable[str],
    file_paths: Iterable[str],
) -> tuple[str, ...]:
    names = set(explicit)
    for value in (*tuple(names), *tuple(file_paths)):
        names.update(
            parent.as_posix()
            for parent in PurePosixPath(value).parents
            if parent != PurePosixPath(".")
        )
    return tuple(sorted(names, key=_directory_sort_key))


@dataclass(frozen=True, slots=True)
class BoundArchiveTree:
    """Generic logical archive inventory independent of simulator records."""

    root_name: str
    files: tuple[BoundArchiveFile, ...]
    directories: tuple[str, ...]

    def __post_init__(self) -> None:
        _root_name(self.root_name)
        if type(self.files) is not tuple or type(self.directories) is not tuple:
            raise BoundArchiveContractError("archive_tree_sequences_must_be_tuples")
        if len(self.files) + len(self.directories) > MAX_TREE_RECORDS:
            raise BoundArchiveContractError("archive_tree_record_count_exceeded")
        if any(not isinstance(item, BoundArchiveFile) for item in self.files):
            raise BoundArchiveContractError("archive_tree_file_record_invalid")
        checked_directories = tuple(
            _portable_relative(item, "archive_directory")
            for item in self.directories
        )
        paths = tuple(item.path for item in self.files)
        if paths != tuple(sorted(paths)) or len(paths) != len(set(paths)):
            raise BoundArchiveContractError("archive_tree_files_not_unique_sorted")
        if (
            checked_directories
            != tuple(sorted(checked_directories, key=_directory_sort_key))
            or len(checked_directories) != len(set(checked_directories))
        ):
            raise BoundArchiveContractError(
                "archive_tree_directories_not_unique_sorted"
            )
        if set(paths) & set(checked_directories):
            raise BoundArchiveContractError("archive_tree_file_directory_collision")
        if _logical_directories(checked_directories, paths) != checked_directories:
            raise BoundArchiveContractError("archive_tree_parent_directory_missing")

    @classmethod
    def create(
        cls,
        root_name: str,
        files: Iterable[BoundArchiveFile],
        directories: Iterable[str] = (),
    ) -> "BoundArchiveTree":
        try:
            materialized_files = tuple(islice(iter(files), MAX_TREE_RECORDS + 1))
            materialized_directories = tuple(
                islice(iter(directories), MAX_TREE_RECORDS + 1)
            )
        except (TypeError, ValueError) as error:
            raise BoundArchiveContractError(
                "archive_tree_inventory_not_iterable"
            ) from error
        if len(materialized_files) > MAX_TREE_RECORDS or (
            len(materialized_files) + len(materialized_directories)
            > MAX_TREE_RECORDS
        ):
            raise BoundArchiveContractError("archive_tree_record_count_exceeded")
        if any(not isinstance(item, BoundArchiveFile) for item in materialized_files):
            raise BoundArchiveContractError("archive_tree_file_record_invalid")
        checked_directories = tuple(
            _portable_relative(item, "archive_directory")
            for item in materialized_directories
        )
        logical = _logical_directories(
            checked_directories,
            (item.path for item in materialized_files),
        )
        return cls(
            _root_name(root_name),
            tuple(sorted(materialized_files, key=lambda item: item.path)),
            logical,
        )

    @property
    def total_bytes(self) -> int:
        return sum(item.size for item in self.files)


@dataclass(frozen=True, slots=True)
class BoundArchiveObservedMode:
    """One source mode bound to its exact canonical archive path and kind."""

    archive_path: str
    kind: Literal["file", "directory"]
    mode: int

    def __post_init__(self) -> None:
        _portable_relative(self.archive_path, "archive_observed_mode")
        if type(self.kind) is not str or self.kind not in ("file", "directory"):
            raise BoundArchiveContractError("archive_observed_mode_kind_invalid")
        if type(self.mode) is not int or self.mode < 0 or self.mode > 0o7777:
            raise BoundArchiveContractError("archive_observed_mode_value_invalid")


@dataclass(frozen=True, slots=True)
class BoundArchiveInspection:
    """A same-stream header, metadata and body inspection result."""

    format: str
    tree: BoundArchiveTree
    observed_modes: tuple[BoundArchiveObservedMode, ...]
    header_index: ArchiveHeaderIndex
    gzip_header_policy: str | None


@dataclass(frozen=True, slots=True)
class BoundArchiveExtraction:
    """Summary of a complete callback-driven extraction."""

    format: str
    file_count: int
    total_bytes: int
    header_index: ArchiveHeaderIndex
    gzip_header_policy: str | None


class ArchiveMemberSource(Protocol):
    """The only body reader exposed to an extraction sink."""

    def read(self, size: int) -> bytes: ...


class ArchiveMemberSink(Protocol):
    """Consume exactly one file without receiving a filesystem path."""

    def __call__(
        self,
        record: BoundArchiveFile,
        source: ArchiveMemberSource,
    ) -> None: ...


class _BoundedContainerStream:
    """Keep every header and stdlib read inside the public chunk bound."""

    __slots__ = ("_size", "_source")

    def __init__(self, source: BinaryIO, size: int) -> None:
        self._source = source
        self._size = size

    def read(self, size: int = -1) -> bytes:
        if (
            type(size) is not int
            or size < 0
            or size > ARCHIVE_MEMBER_CHUNK_BYTES
        ):
            raise BoundArchiveIntegrityError(
                "archive_container_read_bound_exceeded"
            )
        if size == 0:
            return b""
        try:
            chunk = self._source.read(size)
        except Exception as error:
            raise BoundArchiveIntegrityError(
                "archive_container_read_failed"
            ) from error
        if type(chunk) is not bytes:
            raise BoundArchiveIntegrityError(
                "archive_container_returned_nonbytes"
            )
        if len(chunk) > size:
            raise BoundArchiveIntegrityError(
                "archive_container_read_bound_exceeded"
            )
        return chunk

    def read1(self, size: int = -1) -> bytes:
        return self.read(size)

    def readinto(self, buffer: object) -> int:
        try:
            view = memoryview(buffer)
        except TypeError as error:
            raise BoundArchiveIntegrityError(
                "archive_container_readinto_buffer_invalid"
            ) from error
        if view.readonly or view.nbytes > ARCHIVE_MEMBER_CHUNK_BYTES:
            raise BoundArchiveIntegrityError(
                "archive_container_read_bound_exceeded"
            )
        try:
            byte_view = view.cast("B")
        except (TypeError, ValueError) as error:
            raise BoundArchiveIntegrityError(
                "archive_container_readinto_buffer_invalid"
            ) from error
        chunk = self.read(byte_view.nbytes)
        byte_view[: len(chunk)] = chunk
        return len(chunk)

    def readinto1(self, buffer: object) -> int:
        return self.readinto(buffer)

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if type(offset) is not int or type(whence) is not int:
            raise BoundArchiveIntegrityError("archive_container_seek_invalid")
        try:
            position = self._source.seek(offset, whence)
        except Exception as error:
            raise BoundArchiveIntegrityError(
                "archive_container_seek_failed"
            ) from error
        if type(position) is not int or position < 0 or position > self._size:
            raise BoundArchiveIntegrityError("archive_container_seek_differs")
        return position

    def tell(self) -> int:
        try:
            position = self._source.tell()
        except Exception as error:
            raise BoundArchiveIntegrityError(
                "archive_container_tell_failed"
            ) from error
        if type(position) is not int or position < 0 or position > self._size:
            raise BoundArchiveIntegrityError("archive_container_tell_differs")
        return position

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False


def _markers(value: Iterable[bytes]) -> tuple[bytes, ...]:
    if isinstance(value, (str, bytes, bytearray, memoryview)):
        raise BoundArchiveContractError("archive_marker_contract_invalid")
    try:
        materialized = tuple(islice(iter(value), MAX_MARKERS + 1))
    except Exception as error:
        raise BoundArchiveContractError("archive_marker_contract_invalid") from error
    if (
        len(materialized) > MAX_MARKERS
        or len(set(materialized)) != len(materialized)
        or any(
            type(marker) is not bytes
            or not marker
            or len(marker) > MAX_MARKER_BYTES
            for marker in materialized
        )
    ):
        raise BoundArchiveContractError("archive_marker_contract_invalid")
    return materialized


def _validate_tree_limits(
    tree: BoundArchiveTree,
    limits: ArchiveHeaderLimits,
) -> None:
    if not isinstance(tree, BoundArchiveTree):
        raise TypeError("tree must be a BoundArchiveTree")
    if not isinstance(limits, ArchiveHeaderLimits):
        raise TypeError("limits must be ArchiveHeaderLimits")
    # ZIP may encode parent directories implicitly.  Validate the maximum
    # exact inventory shape here; route-specific header binding remains exact.
    if 1 + len(tree.files) + len(tree.directories) > limits.max_members:
        raise BoundArchiveContractError("archive_tree_member_limit_exceeded")
    if tree.total_bytes > limits.max_total_uncompressed_bytes:
        raise BoundArchiveContractError("archive_tree_total_limit_exceeded")
    for value in (tree.root_name, *tree.directories, *(item.path for item in tree.files)):
        parts = PurePosixPath(value).parts
        if (
            len(value.encode("utf-8")) > limits.max_path_bytes
            or len(parts) + 1 > limits.max_path_depth
            or any(
                len(part.encode("utf-8")) > limits.max_path_part_bytes
                for part in parts
            )
        ):
            raise BoundArchiveContractError("archive_tree_path_limit_exceeded")
    if any(item.size > limits.max_member_uncompressed_bytes for item in tree.files):
        raise BoundArchiveContractError("archive_tree_file_limit_exceeded")


def _metadata(source: BoundInputSource, key: str) -> BoundInputMetadata:
    if not isinstance(source, BoundInputSource):
        raise TypeError("source must implement BoundInputSource")
    if not isinstance(key, str) or not key:
        raise BoundArchiveContractError("archive_input_key_invalid")
    source.require_exact_keys((key,))
    if source.keys != (key,):
        raise BoundArchiveContractError("archive_input_keyset_differs")
    metadata = source.metadata(key)
    if not isinstance(metadata, BoundInputMetadata) or metadata.key != key:
        raise BoundArchiveContractError("archive_input_metadata_invalid")
    if (
        metadata.size != metadata.identity.size
        or metadata.size < 1
        or metadata.size > metadata.maximum_bytes
        or _SHA256.fullmatch(metadata.sha256) is None
    ):
        raise BoundArchiveContractError("archive_input_metadata_differs")
    return metadata


@contextmanager
def _open_bound_stream(
    source: BoundInputSource,
    key: str,
    limits: ArchiveHeaderLimits,
) -> Iterator[tuple[BinaryIO, BoundInputMetadata]]:
    metadata = _metadata(source, key)
    if metadata.size > limits.max_archive_bytes:
        raise BoundArchiveContractError("archive_input_size_exceeds_header_bound")
    with source.open_stream(key) as raw_stream:
        yield _BoundedContainerStream(raw_stream, metadata.size), metadata
    after = _metadata(source, key)
    if after != metadata:
        raise BoundArchiveIntegrityError("archive_input_metadata_changed")


def _seek_start(source: BinaryIO, label: str) -> None:
    try:
        position = source.seek(0, os.SEEK_SET)
    except (OSError, TypeError, ValueError) as error:
        raise BoundArchiveIntegrityError(f"{label}_stream_not_seekable") from error
    if position != 0:
        raise BoundArchiveIntegrityError(f"{label}_stream_seek_differs")


def _read_exact(source: BinaryIO, size: int, label: str) -> bytes:
    if type(size) is not int or size < 0 or size > ARCHIVE_MEMBER_CHUNK_BYTES:
        raise BoundArchiveContractError(f"{label}_read_bound_invalid")
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        try:
            chunk = source.read(remaining)
        except (OSError, TypeError, ValueError) as error:
            raise BoundArchiveIntegrityError(f"{label}_read_failed") from error
        if type(chunk) is not bytes:
            raise BoundArchiveIntegrityError(f"{label}_returned_nonbytes")
        if not chunk:
            raise BoundArchiveIntegrityError(f"{label}_truncated")
        if len(chunk) > remaining:
            raise BoundArchiveIntegrityError(f"{label}_read_bound_exceeded")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _gzip_policy(
    stream: BinaryIO,
    metadata: BoundInputMetadata,
    *,
    exact_legacy_sha256: str | None,
) -> str:
    _seek_start(stream, "gzip")
    header = _read_exact(stream, len(CANONICAL_GZIP_HEADER), "gzip_header")
    if header == CANONICAL_GZIP_HEADER:
        _seek_start(stream, "gzip")
        return "canonical"
    if (
        exact_legacy_sha256 is None
        or not isinstance(exact_legacy_sha256, str)
        or _SHA256.fullmatch(exact_legacy_sha256) is None
        or metadata.sha256 != exact_legacy_sha256
    ):
        raise BoundArchiveIntegrityError("legacy_gzip_header_not_exactly_allowlisted")
    _seek_start(stream, "gzip")
    return "legacy_exact_archive_sha256"


def _candidate_gzip_policy(
    stream: BinaryIO,
    metadata: BoundInputMetadata,
) -> str:
    policy = _gzip_policy(stream, metadata, exact_legacy_sha256=None)
    if policy != "canonical":
        raise BoundArchiveIntegrityError("candidate_gzip_header_not_canonical")
    return policy


def _member_relative(
    value: str,
    expected_root: str,
) -> tuple[str | None, bool, str]:
    if not isinstance(value, str) or not value or "\\" in value or "\0" in value:
        raise BoundArchiveIntegrityError("archive_member_path_invalid")
    directory = value.endswith("/")
    normalized = value[:-1] if directory else value
    try:
        _portable_relative(normalized, "archive_member")
    except BoundArchiveContractError as error:
        raise BoundArchiveIntegrityError("archive_member_path_not_portable") from error
    member = PurePosixPath(normalized)
    if member.parts[0] != expected_root:
        raise BoundArchiveIntegrityError("archive_member_outside_expected_root")
    if len(member.parts) == 1:
        return None, directory, normalized
    relative = PurePosixPath(*member.parts[1:]).as_posix()
    return relative, directory, normalized


def _header_entries(index: ArchiveHeaderIndex) -> tuple[tuple[str, str, int], ...]:
    return tuple(
        (entry.path, entry.kind, entry.uncompressed_size)
        for entry in index.entries
    )


def _stdlib_entries_zip(
    infos: Iterable[zipfile.ZipInfo],
    expected_root: str,
) -> tuple[tuple[str, str, int], ...]:
    entries: list[tuple[str, str, int]] = []
    for info in infos:
        _relative, suffix_directory, normalized = _member_relative(
            info.filename,
            expected_root,
        )
        directory = info.is_dir()
        if directory != suffix_directory:
            raise BoundArchiveIntegrityError("zip_member_name_type_differs")
        entries.append(
            (normalized, "directory" if directory else "file", info.file_size)
        )
    return tuple(entries)


def _stdlib_entries_tar(
    members: Iterable[tarfile.TarInfo],
    expected_root: str,
) -> tuple[tuple[str, str, int], ...]:
    entries: list[tuple[str, str, int]] = []
    for member in members:
        _relative, suffix_directory, normalized = _member_relative(
            member.name,
            expected_root,
        )
        directory = member.isdir()
        if suffix_directory and not directory:
            raise BoundArchiveIntegrityError("tar_member_name_type_differs")
        if not directory and not member.isfile():
            raise BoundArchiveIntegrityError("tar_special_member_forbidden")
        entries.append(
            (normalized, "directory" if directory else "file", member.size)
        )
    return tuple(entries)


def _assert_stdlib_matches_header(
    observed: tuple[tuple[str, str, int], ...],
    index: ArchiveHeaderIndex,
) -> None:
    if observed != _header_entries(index) or len(observed) != len(set(observed)):
        raise BoundArchiveIntegrityError("archive_stdlib_header_index_differs")


def _assert_index_tree(
    index: ArchiveHeaderIndex,
    tree: BoundArchiveTree,
    *,
    exact_directories: bool,
) -> None:
    root = (tree.root_name, "directory", 0)
    observed = tuple(sorted(_header_entries(index)))
    files = tuple(item for item in observed if item[1] == "file")
    directories = tuple(item for item in observed if item[1] == "directory")
    expected_files = tuple(
        sorted(
            (f"{tree.root_name}/{item.path}", "file", item.size)
            for item in tree.files
        )
    )
    relative_directories = tuple(
        path.removeprefix(f"{tree.root_name}/")
        for path, _kind, _size in directories
        if path != tree.root_name
    )
    directory_valid = (
        root in directories
        and all(value in tree.directories for value in relative_directories)
        and _logical_directories(
            relative_directories,
            (item.path for item in tree.files),
        )
        == tree.directories
    )
    if exact_directories:
        directory_valid = directory_valid and tuple(
            sorted(relative_directories, key=_directory_sort_key)
        ) == tree.directories
    if (
        index.root_name != tree.root_name
        or index.total_uncompressed_bytes != tree.total_bytes
        or files != expected_files
        or not directory_valid
        or len(observed) != len(files) + len(directories)
    ):
        raise BoundArchiveIntegrityError("archive_header_tree_binding_differs")


class _CumulativeBudget:
    __slots__ = ("_maximum", "total")

    def __init__(self, maximum: int) -> None:
        self._maximum = maximum
        self.total = 0

    def add(self, amount: int) -> None:
        self.total += amount
        if self.total > self._maximum:
            raise BoundArchiveMemberError("archive_actual_total_exceeds_bound")


class _VerifiedMemberReader:
    """Bound one stdlib member stream before exposing it to a sink."""

    __slots__ = (
        "_budget",
        "_digest",
        "_expected",
        "_finished",
        "_markers",
        "_overlap",
        "_remaining",
        "_source",
        "_tail",
    )

    def __init__(
        self,
        source: BinaryIO,
        expected: BoundArchiveFile,
        *,
        markers: tuple[bytes, ...],
        budget: _CumulativeBudget,
    ) -> None:
        self._source = source
        self._expected = expected
        self._markers = markers
        self._overlap = max((len(marker) - 1 for marker in markers), default=0)
        self._tail = b""
        self._digest = hashlib.sha256()
        self._remaining = expected.size
        self._finished = False
        self._budget = budget

    def _finish(self) -> None:
        if self._finished:
            return
        if self._remaining:
            raise BoundArchiveMemberError("archive_sink_did_not_consume_member")
        try:
            overrun = self._source.read(1)
        except Exception as error:
            raise BoundArchiveMemberError("archive_member_read_failed") from error
        if type(overrun) is not bytes:
            raise BoundArchiveMemberError("archive_member_returned_nonbytes")
        if len(overrun) > 1:
            raise BoundArchiveMemberError("archive_member_read_bound_exceeded")
        if overrun:
            raise BoundArchiveMemberError("archive_member_body_exceeds_bound")
        if self._digest.hexdigest() != self._expected.sha256:
            raise BoundArchiveMemberError("archive_member_sha256_mismatch")
        self._finished = True

    def read(self, size: int) -> bytes:
        if type(size) is not int or size < 1 or size > ARCHIVE_MEMBER_CHUNK_BYTES:
            raise BoundArchiveContractError("archive_sink_read_bound_invalid")
        if not self._remaining:
            self._finish()
            return b""
        requested = min(size, self._remaining)
        try:
            chunk = self._source.read(requested)
        except Exception as error:
            raise BoundArchiveMemberError("archive_member_read_failed") from error
        if type(chunk) is not bytes:
            raise BoundArchiveMemberError("archive_member_returned_nonbytes")
        if not chunk:
            raise BoundArchiveMemberError("archive_member_body_truncated")
        if len(chunk) > requested:
            raise BoundArchiveMemberError("archive_member_read_bound_exceeded")
        self._remaining -= len(chunk)
        self._budget.add(len(chunk))
        if self._markers:
            window = self._tail + chunk
            if any(marker in window for marker in self._markers):
                raise BoundArchiveMemberError("archive_member_forbidden_bytes")
            self._tail = window[-self._overlap:] if self._overlap else b""
        self._digest.update(chunk)
        return chunk

    def finalize(self) -> None:
        self._finish()


def _stream_record(
    source: BinaryIO,
    record: BoundArchiveFile,
    *,
    limits: ArchiveHeaderLimits,
    markers: tuple[bytes, ...],
) -> int:
    try:
        result = stream_archive_member(
            source,
            expected_size=record.size,
            maximum_size=limits.max_member_uncompressed_bytes,
            expected_sha256=record.sha256,
            forbidden_markers=markers,
            chunk_bytes=ARCHIVE_MEMBER_CHUNK_BYTES,
        )
    except ArchiveMemberStreamError as error:
        raise BoundArchiveMemberError("archive_member_stream_contract_failed") from error
    return result.size


def _stream_unknown_record(
    source: BinaryIO,
    *,
    path: str,
    size: int,
    limits: ArchiveHeaderLimits,
    markers: tuple[bytes, ...],
) -> BoundArchiveFile:
    try:
        result = stream_archive_member(
            source,
            expected_size=size,
            maximum_size=limits.max_member_uncompressed_bytes,
            forbidden_markers=markers,
            chunk_bytes=ARCHIVE_MEMBER_CHUNK_BYTES,
        )
    except ArchiveMemberStreamError as error:
        raise BoundArchiveMemberError("archive_member_stream_contract_failed") from error
    return BoundArchiveFile(path, result.size, result.sha256, _canonical_mode(path))


def _translate_header_error(error: ArchiveHeaderError) -> BoundArchiveIntegrityError:
    message = str(error)
    if "member count exceeds" in message:
        return BoundArchiveIntegrityError("archive_member_count_exceeds_bound")
    if "total member bytes exceed" in message:
        return BoundArchiveIntegrityError("archive_total_bytes_exceed_bound")
    if "unsupported" in message or "special member" in message:
        return BoundArchiveIntegrityError("archive_unsafe_extension_forbidden")
    return BoundArchiveIntegrityError("archive_header_preflight_failed")


def _zip_header(
    stream: BinaryIO,
    metadata: BoundInputMetadata,
    tree_root: str,
    limits: ArchiveHeaderLimits,
) -> ArchiveHeaderIndex:
    try:
        return validate_zip_header_stream(
            stream,
            archive_size=metadata.size,
            expected_root=tree_root,
            limits=limits,
        )
    except ArchiveHeaderError as error:
        raise _translate_header_error(error) from error


def _tar_header(
    stream: BinaryIO,
    metadata: BoundInputMetadata,
    tree_root: str,
    limits: ArchiveHeaderLimits,
) -> ArchiveHeaderIndex:
    try:
        return validate_tar_gz_header_stream(
            stream,
            archive_size=metadata.size,
            expected_root=tree_root,
            limits=limits,
        )
    except ArchiveHeaderError as error:
        raise _translate_header_error(error) from error


def _zip_infos(
    archive: zipfile.ZipFile,
    index: ArchiveHeaderIndex,
    root_name: str,
    limits: ArchiveHeaderLimits,
) -> list[zipfile.ZipInfo]:
    try:
        infos = archive.infolist()
    except (OSError, RuntimeError, zipfile.BadZipFile) as error:
        raise BoundArchiveIntegrityError("zip_index_parse_failed") from error
    if len(infos) > limits.max_members:
        raise BoundArchiveIntegrityError("archive_member_count_exceeds_bound")
    observed = _stdlib_entries_zip(infos, root_name)
    _assert_stdlib_matches_header(observed, index)
    return infos


def _tar_members(
    archive: tarfile.TarFile,
    index: ArchiveHeaderIndex,
    root_name: str,
    limits: ArchiveHeaderLimits,
) -> list[tarfile.TarInfo]:
    try:
        members = archive.getmembers()
    except (OSError, tarfile.TarError) as error:
        raise BoundArchiveIntegrityError("tar_index_parse_failed") from error
    if len(members) > limits.max_members:
        raise BoundArchiveIntegrityError("archive_member_count_exceeds_bound")
    observed = _stdlib_entries_tar(members, root_name)
    _assert_stdlib_matches_header(observed, index)
    return members


def _open_zip(stream: BinaryIO) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(stream, mode="r")
    except (OSError, RuntimeError, zipfile.BadZipFile) as error:
        raise BoundArchiveIntegrityError("zip_container_open_failed") from error


def _open_tar(stream: BinaryIO) -> tarfile.TarFile:
    try:
        return tarfile.open(fileobj=stream, mode="r:gz")
    except (OSError, tarfile.TarError) as error:
        raise BoundArchiveIntegrityError("tar_container_open_failed") from error


def validate_candidate_zip(
    source: BoundInputSource,
    key: str,
    tree: BoundArchiveTree,
    *,
    limits: ArchiveHeaderLimits = DEFAULT_ARCHIVE_HEADER_LIMITS,
    forbidden_markers: Iterable[bytes] = PRIVATE_KEY_MARKERS,
) -> BoundArchiveInspection:
    """Validate the deterministic candidate ZIP profile and all file bodies."""

    _validate_tree_limits(tree, limits)
    markers = _markers(forbidden_markers)
    expected_files = {
        f"{tree.root_name}/{record.path}": record for record in tree.files
    }
    expected_order = [f"{tree.root_name}/", *expected_files]
    modes: list[BoundArchiveObservedMode] = []
    budget = _CumulativeBudget(limits.max_total_uncompressed_bytes)
    with _open_bound_stream(source, key, limits) as (stream, metadata):
        index = _zip_header(stream, metadata, tree.root_name, limits)
        if index.archive_bytes != metadata.size:
            raise BoundArchiveIntegrityError("zip_header_archive_size_differs")
        _assert_index_tree(index, tree, exact_directories=False)
        _seek_start(stream, "zip")
        archive = _open_zip(stream)
        with archive:
            infos = _zip_infos(archive, index, tree.root_name, limits)
            names = [info.filename for info in infos]
            if names != expected_order or len(names) != len(set(names)):
                raise BoundArchiveIntegrityError("candidate_zip_order_differs")
            if archive.comment:
                raise BoundArchiveIntegrityError("candidate_zip_comment_forbidden")
            for info in infos:
                _relative, directory, normalized = _member_relative(
                    info.filename,
                    tree.root_name,
                )
                if (
                    info.flag_bits & ~0x0800
                    or info.comment
                    or info.extra
                    or info.create_system != 3
                    or info.date_time != (1980, 1, 1, 0, 0, 0)
                ):
                    raise BoundArchiveIntegrityError(
                        "candidate_zip_metadata_not_canonical"
                    )
                unix_mode = (int(info.external_attr) >> 16) & 0xFFFF
                modes.append(
                    BoundArchiveObservedMode(
                        normalized,
                        "directory" if directory else "file",
                        stat.S_IMODE(unix_mode),
                    )
                )
                if info.filename == f"{tree.root_name}/":
                    if (
                        not info.is_dir()
                        or unix_mode != (stat.S_IFDIR | 0o755)
                        or info.file_size != 0
                    ):
                        raise BoundArchiveIntegrityError(
                            "candidate_zip_root_metadata_differs"
                        )
                    continue
                expected = expected_files.get(info.filename)
                if expected is None or info.is_dir():
                    raise BoundArchiveIntegrityError(
                        "candidate_zip_inventory_differs"
                    )
                if (
                    unix_mode != (stat.S_IFREG | expected.mode)
                    or info.compress_type != zipfile.ZIP_DEFLATED
                    or info.file_size != expected.size
                ):
                    raise BoundArchiveIntegrityError(
                        "candidate_zip_file_metadata_differs"
                    )
                try:
                    member_source = archive.open(info, mode="r")
                except (OSError, RuntimeError, zipfile.BadZipFile) as error:
                    raise BoundArchiveIntegrityError(
                        "candidate_zip_member_open_failed"
                    ) from error
                with member_source:
                    budget.add(
                        _stream_record(
                            member_source,
                            expected,
                            limits=limits,
                            markers=markers,
                        )
                    )
    if budget.total != tree.total_bytes:
        raise BoundArchiveIntegrityError("candidate_zip_actual_total_differs")
    return BoundArchiveInspection("zip", tree, tuple(modes), index, None)


def validate_candidate_tar_gz(
    source: BoundInputSource,
    key: str,
    tree: BoundArchiveTree,
    *,
    limits: ArchiveHeaderLimits = DEFAULT_ARCHIVE_HEADER_LIMITS,
    forbidden_markers: Iterable[bytes] = PRIVATE_KEY_MARKERS,
) -> BoundArchiveInspection:
    """Validate the deterministic candidate USTAR/gzip profile and bodies."""

    _validate_tree_limits(tree, limits)
    markers = _markers(forbidden_markers)
    expected_files = {
        f"{tree.root_name}/{record.path}": record for record in tree.files
    }
    expected_directories = {
        tree.root_name,
        *(f"{tree.root_name}/{value}" for value in tree.directories),
    }
    expected_order = [
        tree.root_name,
        *(f"{tree.root_name}/{value}" for value in tree.directories),
        *expected_files,
    ]
    modes: list[BoundArchiveObservedMode] = []
    budget = _CumulativeBudget(limits.max_total_uncompressed_bytes)
    with _open_bound_stream(source, key, limits) as (stream, metadata):
        index = _tar_header(stream, metadata, tree.root_name, limits)
        if index.archive_bytes != metadata.size:
            raise BoundArchiveIntegrityError("tar_header_archive_size_differs")
        gzip_policy = _candidate_gzip_policy(stream, metadata)
        _assert_index_tree(index, tree, exact_directories=True)
        _seek_start(stream, "tar")
        archive = _open_tar(stream)
        with archive:
            members = _tar_members(archive, index, tree.root_name, limits)
            names = [
                _member_relative(member.name, tree.root_name)[2]
                for member in members
            ]
            if names != expected_order or len(names) != len(set(names)):
                raise BoundArchiveIntegrityError("candidate_tar_order_differs")
            for member in members:
                _relative, _directory_suffix, normalized = _member_relative(
                    member.name,
                    tree.root_name,
                )
                if (
                    member.uid != 0
                    or member.gid != 0
                    or member.uname != "root"
                    or member.gname != "root"
                    or member.mtime != 0
                    or member.linkname
                    or member.pax_headers
                ):
                    raise BoundArchiveIntegrityError(
                        "candidate_tar_metadata_not_canonical"
                    )
                modes.append(
                    BoundArchiveObservedMode(
                        normalized,
                        "directory" if member.isdir() else "file",
                        stat.S_IMODE(member.mode),
                    )
                )
                if normalized in expected_directories:
                    if (
                        not member.isdir()
                        or member.mode != 0o755
                        or member.size != 0
                    ):
                        raise BoundArchiveIntegrityError(
                            "candidate_tar_directory_metadata_differs"
                        )
                    continue
                expected = expected_files.get(normalized)
                if (
                    expected is None
                    or not member.isfile()
                    or member.mode != expected.mode
                    or member.size != expected.size
                ):
                    raise BoundArchiveIntegrityError(
                        "candidate_tar_file_metadata_differs"
                    )
                try:
                    member_source = archive.extractfile(member)
                except (OSError, tarfile.TarError) as error:
                    raise BoundArchiveIntegrityError(
                        "candidate_tar_member_open_failed"
                    ) from error
                if member_source is None:
                    raise BoundArchiveIntegrityError(
                        "candidate_tar_member_open_failed"
                    )
                with member_source:
                    budget.add(
                        _stream_record(
                            member_source,
                            expected,
                            limits=limits,
                            markers=markers,
                        )
                    )
    if budget.total != tree.total_bytes:
        raise BoundArchiveIntegrityError("candidate_tar_actual_total_differs")
    return BoundArchiveInspection(
        "tar.gz",
        tree,
        tuple(modes),
        index,
        gzip_policy,
    )


def inspect_legacy_zip(
    source: BoundInputSource,
    key: str,
    expected_root: str,
    *,
    limits: ArchiveHeaderLimits = DEFAULT_ARCHIVE_HEADER_LIMITS,
    forbidden_markers: Iterable[bytes] = PRIVATE_KEY_MARKERS,
) -> BoundArchiveInspection:
    """Stream a bounded legacy ZIP into generic records and observed modes."""

    root = _root_name(expected_root)
    if not isinstance(limits, ArchiveHeaderLimits):
        raise TypeError("limits must be ArchiveHeaderLimits")
    markers = _markers(forbidden_markers)
    records: list[BoundArchiveFile] = []
    directories: list[str] = []
    modes: list[BoundArchiveObservedMode] = []
    budget = _CumulativeBudget(limits.max_total_uncompressed_bytes)
    with _open_bound_stream(source, key, limits) as (stream, metadata):
        index = _zip_header(stream, metadata, root, limits)
        _seek_start(stream, "zip")
        archive = _open_zip(stream)
        with archive:
            infos = _zip_infos(archive, index, root, limits)
            root_seen = False
            for info in infos:
                relative, directory, normalized = _member_relative(
                    info.filename,
                    root,
                )
                unix_mode = (int(info.external_attr) >> 16) & 0xFFFF
                kind = stat.S_IFMT(unix_mode)
                modes.append(
                    BoundArchiveObservedMode(
                        normalized,
                        "directory" if directory else "file",
                        stat.S_IMODE(unix_mode),
                    )
                )
                if directory:
                    if (
                        not info.is_dir()
                        or kind not in (0, stat.S_IFDIR)
                        or info.file_size != 0
                        or info.flag_bits & 0x1
                    ):
                        raise BoundArchiveIntegrityError(
                            "legacy_zip_directory_metadata_unsafe"
                        )
                    root_seen = root_seen or relative is None
                    if relative is not None:
                        directories.append(relative)
                    continue
                if relative is None or kind not in (0, stat.S_IFREG):
                    raise BoundArchiveIntegrityError(
                        "legacy_zip_special_member_forbidden"
                    )
                if info.flag_bits & 0x1 or info.file_size > limits.max_member_uncompressed_bytes:
                    raise BoundArchiveIntegrityError(
                        "legacy_zip_file_metadata_unsafe"
                    )
                try:
                    member_source = archive.open(info, mode="r")
                except (OSError, RuntimeError, zipfile.BadZipFile) as error:
                    raise BoundArchiveIntegrityError(
                        "legacy_zip_member_open_failed"
                    ) from error
                with member_source:
                    record = _stream_unknown_record(
                        member_source,
                        path=relative,
                        size=info.file_size,
                        limits=limits,
                        markers=markers,
                    )
                budget.add(record.size)
                records.append(record)
            if not root_seen:
                raise BoundArchiveIntegrityError("legacy_zip_root_missing")
        tree = BoundArchiveTree.create(root, records, directories)
        _validate_tree_limits(tree, limits)
        _assert_index_tree(index, tree, exact_directories=False)
    if budget.total != tree.total_bytes:
        raise BoundArchiveIntegrityError("legacy_zip_actual_total_differs")
    return BoundArchiveInspection("zip", tree, tuple(modes), index, None)


def inspect_legacy_tar_gz(
    source: BoundInputSource,
    key: str,
    expected_root: str,
    *,
    exact_archive_sha256: str,
    limits: ArchiveHeaderLimits = DEFAULT_ARCHIVE_HEADER_LIMITS,
    forbidden_markers: Iterable[bytes] = PRIVATE_KEY_MARKERS,
) -> BoundArchiveInspection:
    """Stream a bounded legacy USTAR into generic records and modes."""

    root = _root_name(expected_root)
    if not isinstance(limits, ArchiveHeaderLimits):
        raise TypeError("limits must be ArchiveHeaderLimits")
    markers = _markers(forbidden_markers)
    records: list[BoundArchiveFile] = []
    directories: list[str] = []
    modes: list[BoundArchiveObservedMode] = []
    budget = _CumulativeBudget(limits.max_total_uncompressed_bytes)
    with _open_bound_stream(source, key, limits) as (stream, metadata):
        index = _tar_header(stream, metadata, root, limits)
        gzip_policy = _gzip_policy(
            stream,
            metadata,
            exact_legacy_sha256=exact_archive_sha256,
        )
        _seek_start(stream, "tar")
        archive = _open_tar(stream)
        with archive:
            members = _tar_members(archive, index, root, limits)
            root_seen = False
            for member in members:
                relative, _directory_suffix, normalized = _member_relative(
                    member.name,
                    root,
                )
                modes.append(
                    BoundArchiveObservedMode(
                        normalized,
                        "directory" if member.isdir() else "file",
                        stat.S_IMODE(member.mode),
                    )
                )
                if member.isdir():
                    if member.size != 0 or member.linkname:
                        raise BoundArchiveIntegrityError(
                            "legacy_tar_directory_metadata_unsafe"
                        )
                    root_seen = root_seen or relative is None
                    if relative is not None:
                        directories.append(relative)
                    continue
                if relative is None or not member.isfile() or member.linkname:
                    raise BoundArchiveIntegrityError(
                        "legacy_tar_special_member_forbidden"
                    )
                if member.size > limits.max_member_uncompressed_bytes:
                    raise BoundArchiveIntegrityError(
                        "legacy_tar_file_metadata_unsafe"
                    )
                try:
                    member_source = archive.extractfile(member)
                except (OSError, tarfile.TarError) as error:
                    raise BoundArchiveIntegrityError(
                        "legacy_tar_member_open_failed"
                    ) from error
                if member_source is None:
                    raise BoundArchiveIntegrityError("legacy_tar_member_open_failed")
                with member_source:
                    record = _stream_unknown_record(
                        member_source,
                        path=relative,
                        size=member.size,
                        limits=limits,
                        markers=markers,
                    )
                budget.add(record.size)
                records.append(record)
            if not root_seen:
                raise BoundArchiveIntegrityError("legacy_tar_root_missing")
        tree = BoundArchiveTree.create(root, records, directories)
        _validate_tree_limits(tree, limits)
        _assert_index_tree(index, tree, exact_directories=True)
    if budget.total != tree.total_bytes:
        raise BoundArchiveIntegrityError("legacy_tar_actual_total_differs")
    return BoundArchiveInspection(
        "tar.gz",
        tree,
        tuple(modes),
        index,
        gzip_policy,
    )


def _validate_sink(value: ArchiveMemberSink) -> ArchiveMemberSink:
    if not callable(value):
        raise TypeError("sink must be callable")
    return value


def _mode_policy(value: object) -> ArchiveExtractionModePolicy:
    if type(value) is not str or value not in ("candidate", "legacy"):
        raise BoundArchiveContractError("archive_extraction_mode_policy_invalid")
    return cast(ArchiveExtractionModePolicy, value)


def extract_zip_to_sink(
    source: BoundInputSource,
    key: str,
    tree: BoundArchiveTree,
    sink: ArchiveMemberSink,
    *,
    mode_policy: ArchiveExtractionModePolicy = "candidate",
    limits: ArchiveHeaderLimits = DEFAULT_ARCHIVE_HEADER_LIMITS,
    forbidden_markers: Iterable[bytes] = PRIVATE_KEY_MARKERS,
) -> BoundArchiveExtraction:
    """Extract exact ZIP bodies through a bounded path-free member reader.

    ``candidate`` requires each source file mode to equal its record.  The
    ``legacy`` policy accepts only a safe regular-file kind and deliberately
    treats the caller's record mode as the normalized output mode; callers
    must obtain that tree from a completed legacy inspection.
    """

    _validate_tree_limits(tree, limits)
    checked_mode_policy = _mode_policy(mode_policy)
    checked_sink = _validate_sink(sink)
    markers = _markers(forbidden_markers)
    budget = _CumulativeBudget(limits.max_total_uncompressed_bytes)
    with _open_bound_stream(source, key, limits) as (stream, metadata):
        index = _zip_header(stream, metadata, tree.root_name, limits)
        _assert_index_tree(index, tree, exact_directories=False)
        _seek_start(stream, "zip")
        archive = _open_zip(stream)
        with archive:
            infos = _zip_infos(archive, index, tree.root_name, limits)
            by_relative: dict[str, zipfile.ZipInfo] = {}
            for info in infos:
                relative, directory, _normalized = _member_relative(
                    info.filename,
                    tree.root_name,
                )
                unix_mode = (int(info.external_attr) >> 16) & 0xFFFF
                if directory:
                    if (
                        stat.S_IFMT(unix_mode) not in (0, stat.S_IFDIR)
                        or info.file_size != 0
                        or info.flag_bits & 0x1
                    ):
                        raise BoundArchiveIntegrityError(
                            "zip_directory_metadata_unsafe"
                        )
                    if (
                        checked_mode_policy == "candidate"
                        and unix_mode != (stat.S_IFDIR | 0o755)
                    ):
                        raise BoundArchiveIntegrityError(
                            "zip_directory_mode_differs"
                        )
                    continue
                if relative is None or relative in by_relative:
                    raise BoundArchiveIntegrityError("zip_file_inventory_differs")
                by_relative[relative] = info
            if set(by_relative) != {item.path for item in tree.files}:
                raise BoundArchiveIntegrityError("zip_file_inventory_differs")
            for record in tree.files:
                info = by_relative[record.path]
                unix_mode = (int(info.external_attr) >> 16) & 0xFFFF
                if (
                    info.is_dir()
                    or stat.S_IFMT(unix_mode) not in (0, stat.S_IFREG)
                    or info.flag_bits & 0x1
                    or info.file_size != record.size
                ):
                    raise BoundArchiveIntegrityError("zip_file_metadata_unsafe")
                if (
                    checked_mode_policy == "candidate"
                    and unix_mode != (stat.S_IFREG | record.mode)
                ):
                    raise BoundArchiveIntegrityError("zip_file_mode_differs")
                try:
                    member_source = archive.open(info, mode="r")
                except (OSError, RuntimeError, zipfile.BadZipFile) as error:
                    raise BoundArchiveIntegrityError(
                        "zip_member_open_failed"
                    ) from error
                with member_source:
                    verified = _VerifiedMemberReader(
                        member_source,
                        record,
                        markers=markers,
                        budget=budget,
                    )
                    checked_sink(record, verified)
                    verified.finalize()
    if budget.total != tree.total_bytes:
        raise BoundArchiveIntegrityError("zip_extraction_total_differs")
    return BoundArchiveExtraction(
        "zip",
        len(tree.files),
        budget.total,
        index,
        None,
    )


def extract_tar_gz_to_sink(
    source: BoundInputSource,
    key: str,
    tree: BoundArchiveTree,
    sink: ArchiveMemberSink,
    *,
    mode_policy: ArchiveExtractionModePolicy = "candidate",
    exact_legacy_sha256: str | None = None,
    limits: ArchiveHeaderLimits = DEFAULT_ARCHIVE_HEADER_LIMITS,
    forbidden_markers: Iterable[bytes] = PRIVATE_KEY_MARKERS,
) -> BoundArchiveExtraction:
    """Extract exact USTAR bodies through a bounded path-free reader.

    ``candidate`` requires each source file mode to equal its record.  The
    ``legacy`` policy accepts safe regular files and treats the record mode as
    the normalized output mode after a completed legacy inspection.

    A canonical gzip header is accepted directly.  A noncanonical legacy
    header is accepted only when ``exact_legacy_sha256`` equals the sealed
    bound-input metadata SHA-256.
    """

    _validate_tree_limits(tree, limits)
    checked_mode_policy = _mode_policy(mode_policy)
    checked_sink = _validate_sink(sink)
    markers = _markers(forbidden_markers)
    budget = _CumulativeBudget(limits.max_total_uncompressed_bytes)
    with _open_bound_stream(source, key, limits) as (stream, metadata):
        index = _tar_header(stream, metadata, tree.root_name, limits)
        _assert_index_tree(index, tree, exact_directories=True)
        gzip_policy = _gzip_policy(
            stream,
            metadata,
            exact_legacy_sha256=exact_legacy_sha256,
        )
        _seek_start(stream, "tar")
        archive = _open_tar(stream)
        with archive:
            members = _tar_members(archive, index, tree.root_name, limits)
            by_relative: dict[str, tarfile.TarInfo] = {}
            for member in members:
                relative, _directory_suffix, _normalized = _member_relative(
                    member.name,
                    tree.root_name,
                )
                if member.isdir():
                    if member.size != 0 or member.linkname:
                        raise BoundArchiveIntegrityError(
                            "tar_directory_metadata_unsafe"
                        )
                    if (
                        checked_mode_policy == "candidate"
                        and member.mode != 0o755
                    ):
                        raise BoundArchiveIntegrityError(
                            "tar_directory_mode_differs"
                        )
                    continue
                if (
                    relative is None
                    or relative in by_relative
                    or not member.isfile()
                    or member.linkname
                ):
                    raise BoundArchiveIntegrityError("tar_file_inventory_differs")
                by_relative[relative] = member
            if set(by_relative) != {item.path for item in tree.files}:
                raise BoundArchiveIntegrityError("tar_file_inventory_differs")
            for record in tree.files:
                member = by_relative[record.path]
                if member.size != record.size:
                    raise BoundArchiveIntegrityError("tar_file_metadata_unsafe")
                if (
                    checked_mode_policy == "candidate"
                    and member.mode != record.mode
                ):
                    raise BoundArchiveIntegrityError("tar_file_mode_differs")
                try:
                    member_source = archive.extractfile(member)
                except (OSError, tarfile.TarError) as error:
                    raise BoundArchiveIntegrityError(
                        "tar_member_open_failed"
                    ) from error
                if member_source is None:
                    raise BoundArchiveIntegrityError("tar_member_open_failed")
                with member_source:
                    verified = _VerifiedMemberReader(
                        member_source,
                        record,
                        markers=markers,
                        budget=budget,
                    )
                    checked_sink(record, verified)
                    verified.finalize()
    if budget.total != tree.total_bytes:
        raise BoundArchiveIntegrityError("tar_extraction_total_differs")
    return BoundArchiveExtraction(
        "tar.gz",
        len(tree.files),
        budget.total,
        index,
        gzip_policy,
    )


def bound_archive_evidence() -> dict[str, object]:
    """Return honest evidence for this standalone path-free adapter."""

    return {
        "contract": BOUND_ARCHIVE_READER_CONTRACT,
        "integration": False,
        "blockers": [],
        "archive_paths_accepted": False,
        "bound_input_exact_key_and_metadata": True,
        "same_stream_header_stdlib_and_bodies": True,
        "outer_stream_read_chunk_bytes": ARCHIVE_MEMBER_CHUNK_BYTES,
        "outer_stream_read_minus_one_allowed": False,
        "stdlib_negative_read_rejected_before_bound_input": True,
        "bounded_header_preflight_before_stdlib": True,
        "member_read_chunk_bytes": ARCHIVE_MEMBER_CHUNK_BYTES,
        "member_read_minus_one_allowed": False,
        "member_size_sha256_and_eof_reproved": True,
        "private_marker_overlap_reproved": True,
        "cumulative_body_budget_reproved": True,
        "extraction_mode_policy_explicit": True,
        "per_member_source_modes_recorded": True,
        "unsafe_archive_extensions_fail_closed": True,
        "consumer_error_registry_postproof": True,
        "simulator_integration_proven": False,
        "local_acceptance_blockers_closed": [],
    }


__all__ = [
    "BOUND_ARCHIVE_READER_CONTRACT",
    "CANONICAL_GZIP_HEADER",
    "ArchiveExtractionModePolicy",
    "ArchiveMemberSink",
    "ArchiveMemberSource",
    "BoundArchiveContractError",
    "BoundArchiveError",
    "BoundArchiveExtraction",
    "BoundArchiveFile",
    "BoundArchiveInspection",
    "BoundArchiveIntegrityError",
    "BoundArchiveMemberError",
    "BoundArchiveObservedMode",
    "BoundArchiveTree",
    "bound_archive_evidence",
    "extract_tar_gz_to_sink",
    "extract_zip_to_sink",
    "inspect_legacy_tar_gz",
    "inspect_legacy_zip",
    "validate_candidate_tar_gz",
    "validate_candidate_zip",
]
