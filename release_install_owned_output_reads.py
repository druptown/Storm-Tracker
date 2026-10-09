"""Bounded descriptor-relative reads of simulator-owned release output.

The simulator integration intentionally lives elsewhere.  This module owns a
duplicate of an already-open run-root authority and borrows the corresponding
``ReleaseInstallOutputAuthority`` only to re-prove the run-root name before and
after every public operation.  It never turns an authority-relative name back
into an absolute path.

The public filesystem API accepts ``RelativePath`` objects only.  Directory
components are matched with exact spelling after a bounded listing, every
opened node is identity-pinned, regular files must have one link, and node and
root identities are proved again after parsing, hashing, or consumer code.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import io
from itertools import islice
import json
import os
import re
import stat
from typing import BinaryIO, Callable, Final, Iterable, Iterator, Mapping, TypeVar
import unicodedata

from descriptor_relative_authority import (
    AuthorityError,
    AuthorityLimitExceeded,
    AuthorityNotFound,
    AuthorityViolation,
    BoundFile,
    DirectoryAuthority,
    DirectoryEntry,
    MAX_DIRECTORY_ENTRIES,
    MAX_READ_BYTES,
    NodeIdentity,
    RelativePath,
)
from release_install_output_authority import (
    OutputAuthorityError,
    ReleaseInstallOutputAuthority,
)


OWNED_OUTPUT_READ_CONTRACT: Final = "stv3-release-install-owned-output-read-v1"
MAX_JSON_BYTES: Final = 4 * 1024 * 1024
MAX_JSON_DEPTH: Final = 64
MAX_JSON_NODES: Final = 50_000
MAX_JSON_STRING_BYTES: Final = 1024 * 1024
MAX_JSON_KEY_BYTES: Final = 256
MAX_JSON_INTEGER: Final = 2 * 1024 * 1024 * 1024
MAX_JOURNAL_EVENTS: Final = 10_000
MAX_JOURNAL_TOTAL_BYTES: Final = 64 * 1024 * 1024
MAX_TREE_ENTRIES: Final = 20_000
MAX_TREE_DEPTH: Final = 64
MAX_TREE_FILE_BYTES: Final = 512 * 1024 * 1024
MAX_TREE_TOTAL_BYTES: Final = 2 * 1024 * 1024 * 1024
READ_CHUNK_BYTES: Final = 1024 * 1024
MAX_COMPONENTS: Final = 16
MAX_ALLOWED_LAYOUTS: Final = 81

_SHA256: Final = re.compile(r"[0-9a-f]{64}\Z")
_JOURNAL_PENDING: Final = re.compile(r"[0-9]{6}\.[0-9a-f]{32}\.pending\Z")
_ZERO_SHA256: Final = "0" * 64
_GENERATION_CONTAINERS: Final = ("install", "backup", "stage", "retained")
_GENERATION_SLOTS: Final = (
    ("active", "install", "active"),
    ("backup", "backup", "baseline"),
    ("candidate", "stage", "candidate"),
    ("retained", "retained", "candidate"),
)


class OwnedOutputReadError(RuntimeError):
    """Base error for a read whose bounded authority contract did not finish."""


class OwnedOutputReadValidationError(OwnedOutputReadError):
    """Owned output did not satisfy its declared closed-world structure."""


class OwnedOutputReadNotFound(OwnedOutputReadValidationError):
    """An exact authority-relative node was absent."""


class OwnedOutputReadChanged(OwnedOutputReadValidationError):
    """A pinned root or node did not survive its post-operation proof."""


class OwnedOutputReadLimitExceeded(OwnedOutputReadError):
    """A caller-controlled byte, depth, node, or entry bound was exceeded."""


class OwnedOutputReadIOError(OwnedOutputReadError):
    """The descriptor-relative backend failed without a safe classification."""


class _BoundedDuplicate(io.BufferedIOBase):
    """Seekable read-only view that cannot observe bytes beyond a sealed size."""

    def __init__(self, stream: BinaryIO, extent: int) -> None:
        super().__init__()
        if type(extent) is not int or extent < 0 or extent > MAX_TREE_TOTAL_BYTES:
            raise TypeError("archive stream extent is outside the supported bound")
        self._stream = stream
        self._extent = extent

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        self._checkClosed()
        try:
            position = self._stream.tell()
        except (OSError, ValueError) as error:
            raise OwnedOutputReadIOError(
                "owned_archive_stream_tell_failed"
            ) from error
        if type(position) is not int or position < 0 or position > self._extent:
            raise OwnedOutputReadChanged("owned_archive_stream_position_invalid")
        return position

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        self._checkClosed()
        if type(offset) is not int or type(whence) is not int:
            raise TypeError("archive stream seek arguments must be integers")
        if whence == os.SEEK_SET:
            target = offset
        elif whence == os.SEEK_CUR:
            target = self.tell() + offset
        elif whence == os.SEEK_END:
            target = self._extent + offset
        else:
            raise ValueError("invalid archive stream seek whence")
        if target < 0 or target > self._extent:
            raise OwnedOutputReadLimitExceeded(
                "owned_archive_stream_seek_outside_seal"
            )
        try:
            observed = self._stream.seek(target, os.SEEK_SET)
        except (OSError, ValueError) as error:
            raise OwnedOutputReadIOError(
                "owned_archive_stream_seek_failed"
            ) from error
        if observed != target:
            raise OwnedOutputReadChanged("owned_archive_stream_seek_differs")
        return observed

    def read(self, size: int = -1) -> bytes:
        self._checkClosed()
        if type(size) is not int:
            raise TypeError("archive stream read size must be an integer")
        if size < 0 or size > READ_CHUNK_BYTES:
            raise OwnedOutputReadLimitExceeded(
                "owned_archive_stream_read_request_limit_exceeded"
            )
        position = self.tell()
        remaining = self._extent - position
        request = min(size, remaining + 1)
        try:
            value = self._stream.read(request)
        except (OSError, ValueError) as error:
            raise OwnedOutputReadIOError(
                "owned_archive_stream_read_failed"
            ) from error
        if not isinstance(value, bytes):
            raise OwnedOutputReadValidationError(
                "owned_archive_stream_read_returned_nonbytes"
            )
        if len(value) > request:
            raise OwnedOutputReadLimitExceeded(
                "owned_archive_stream_read_overreturned"
            )
        if len(value) > remaining:
            raise OwnedOutputReadChanged("owned_archive_stream_grew_beyond_seal")
        return value

    def readinto(self, buffer: object) -> int:
        try:
            view = memoryview(buffer).cast("B")
        except (TypeError, ValueError) as error:
            raise TypeError("archive stream readinto buffer is invalid") from error
        if view.readonly:
            raise TypeError("archive stream readinto buffer must be writable")
        if len(view) > READ_CHUNK_BYTES:
            raise OwnedOutputReadLimitExceeded(
                "owned_archive_stream_readinto_limit_exceeded"
            )
        value = self.read(len(view))
        view[: len(value)] = value
        return len(value)

    def close(self) -> None:
        if self.closed:
            return
        error: BaseException | None = None
        try:
            self._stream.close()
        except (OSError, ValueError) as caught:
            error = OwnedOutputReadIOError(
                "owned_archive_stream_underlying_close_failed"
            )
            error.__cause__ = caught
        finally:
            super().close()
        if error is not None:
            raise error


def _bounded_integer(value: object, label: str, low: int, high: int) -> int:
    if type(value) is not int or value < low or value > high:
        raise TypeError(f"{label} must be an integer from {low} through {high}")
    return value


def _relative(value: object, *, allow_root: bool) -> RelativePath:
    if not isinstance(value, RelativePath):
        raise TypeError("owned output paths must be RelativePath objects")
    if not allow_root and not value.parts:
        raise OwnedOutputReadValidationError("owned_output_path_requires_leaf")
    for component in value.parts:
        if unicodedata.normalize("NFC", component) != component:
            raise OwnedOutputReadValidationError(
                "owned_output_path_component_not_nfc"
            )
    return value


def _identity_stable(before: NodeIdentity, after: NodeIdentity) -> bool:
    same_node = (
        before.same_object(after)
        and before.backend == after.backend
        and before.kind == after.kind
    )
    if not same_node:
        return False
    if before.kind == "directory":
        # Directory size and link count are mutable metadata: adding an owned
        # entry may change either without changing the pinned directory object.
        return True
    return (
        before.link_count == after.link_count
        and before.size == after.size
    )


def _translate_authority(error: AuthorityError, label: str) -> OwnedOutputReadError:
    if isinstance(error, AuthorityLimitExceeded):
        return OwnedOutputReadLimitExceeded(f"{label}_limit_exceeded")
    if isinstance(error, AuthorityNotFound):
        return OwnedOutputReadNotFound(f"{label}_not_found")
    if isinstance(error, AuthorityViolation):
        return OwnedOutputReadValidationError(f"{label}_authority_violation")
    return OwnedOutputReadIOError(f"{label}_io_failed")


def _proof_error(error: BaseException, label: str) -> BaseException:
    if isinstance(error, AuthorityError):
        return _translate_authority(error, label)
    return error


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError) as error:
        raise OwnedOutputReadValidationError(
            "owned_output_value_cannot_be_canonicalized"
        ) from error


def _strict_json_bytes(value: bytes, label: str, maximum_bytes: int) -> object:
    if not isinstance(value, bytes):
        raise TypeError("strict JSON input must be bytes")
    if len(value) > maximum_bytes:
        raise OwnedOutputReadLimitExceeded(f"{label}_json_byte_limit_exceeded")

    def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, item in pairs:
            if key in result:
                raise OwnedOutputReadValidationError(
                    f"{label}_json_duplicate_key"
                )
            result[key] = item
        return result

    def reject_constant(token: str) -> object:
        raise OwnedOutputReadValidationError(
            f"{label}_json_nonfinite_value_{token}"
        )

    def reject_float(token: str) -> object:
        raise OwnedOutputReadValidationError(f"{label}_json_float_forbidden")

    def parse_integer(token: str) -> int:
        try:
            parsed = int(token, 10)
        except ValueError as error:
            raise OwnedOutputReadValidationError(
                f"{label}_json_integer_invalid"
            ) from error
        if abs(parsed) > MAX_JSON_INTEGER:
            raise OwnedOutputReadLimitExceeded(
                f"{label}_json_integer_limit_exceeded"
            )
        return parsed

    def validate_depth(source: str) -> None:
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
                    raise OwnedOutputReadLimitExceeded(
                        f"{label}_json_depth_limit_exceeded"
                    )
            elif character in "]}":
                depth -= 1

    def validate_nodes(document: object) -> None:
        pending = [document]
        nodes = 0
        while pending:
            item = pending.pop()
            nodes += 1
            if nodes > MAX_JSON_NODES:
                raise OwnedOutputReadLimitExceeded(
                    f"{label}_json_node_limit_exceeded"
                )
            if isinstance(item, str):
                if len(item.encode("utf-8")) > MAX_JSON_STRING_BYTES:
                    raise OwnedOutputReadLimitExceeded(
                        f"{label}_json_string_limit_exceeded"
                    )
            elif isinstance(item, dict):
                for key, nested in item.items():
                    if len(key.encode("utf-8")) > MAX_JSON_KEY_BYTES:
                        raise OwnedOutputReadLimitExceeded(
                            f"{label}_json_key_limit_exceeded"
                        )
                    pending.append(nested)
            elif isinstance(item, list):
                pending.extend(item)

    try:
        source = value.decode("utf-8", "strict")
        validate_depth(source)
        document = json.loads(
            source,
            object_pairs_hook=object_pairs,
            parse_constant=reject_constant,
            parse_float=reject_float,
            parse_int=parse_integer,
        )
        validate_nodes(document)
        return document
    except OwnedOutputReadError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError, ValueError) as error:
        raise OwnedOutputReadValidationError(
            f"{label}_not_strict_utf8_json"
        ) from error


@dataclass(frozen=True, slots=True)
class JournalReadLimits:
    maximum_events: int = MAX_JOURNAL_EVENTS
    maximum_event_bytes: int = MAX_JSON_BYTES
    maximum_total_bytes: int = MAX_JOURNAL_TOTAL_BYTES
    maximum_directory_entries: int = MAX_DIRECTORY_ENTRIES

    def __post_init__(self) -> None:
        _bounded_integer(self.maximum_events, "maximum_events", 0, MAX_JOURNAL_EVENTS)
        _bounded_integer(
            self.maximum_event_bytes,
            "maximum_event_bytes",
            0,
            MAX_JSON_BYTES,
        )
        _bounded_integer(
            self.maximum_total_bytes,
            "maximum_total_bytes",
            0,
            MAX_JOURNAL_TOTAL_BYTES,
        )
        _bounded_integer(
            self.maximum_directory_entries,
            "maximum_directory_entries",
            0,
            MAX_DIRECTORY_ENTRIES,
        )


@dataclass(frozen=True, slots=True)
class TreeReadLimits:
    maximum_entries: int = MAX_TREE_ENTRIES
    maximum_depth: int = MAX_TREE_DEPTH
    maximum_file_bytes: int = MAX_TREE_FILE_BYTES
    maximum_total_bytes: int = MAX_TREE_TOTAL_BYTES
    maximum_directory_entries: int = MAX_DIRECTORY_ENTRIES
    chunk_bytes: int = READ_CHUNK_BYTES

    def __post_init__(self) -> None:
        _bounded_integer(
            self.maximum_entries, "maximum_entries", 0, MAX_TREE_ENTRIES
        )
        _bounded_integer(self.maximum_depth, "maximum_depth", 0, MAX_TREE_DEPTH)
        _bounded_integer(
            self.maximum_file_bytes,
            "maximum_file_bytes",
            0,
            MAX_TREE_FILE_BYTES,
        )
        _bounded_integer(
            self.maximum_total_bytes,
            "maximum_total_bytes",
            0,
            MAX_TREE_TOTAL_BYTES,
        )
        _bounded_integer(
            self.maximum_directory_entries,
            "maximum_directory_entries",
            0,
            MAX_DIRECTORY_ENTRIES,
        )
        _bounded_integer(self.chunk_bytes, "chunk_bytes", 1, READ_CHUNK_BYTES)


@dataclass(frozen=True, slots=True)
class FileExpectation:
    path: RelativePath
    size: int
    sha256: str
    mode: int

    def __post_init__(self) -> None:
        _relative(self.path, allow_root=False)
        _bounded_integer(self.size, "expected file size", 0, MAX_TREE_FILE_BYTES)
        if not isinstance(self.sha256, str) or _SHA256.fullmatch(self.sha256) is None:
            raise OwnedOutputReadValidationError("expected_file_sha256_invalid")
        if self.mode not in {0o644, 0o755}:
            raise OwnedOutputReadValidationError("expected_file_mode_invalid")

    def to_mapping(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "path": str(self.path),
            "sha256": self.sha256,
            "size": self.size,
        }

    @classmethod
    def from_mapping(cls, raw: object) -> "FileExpectation":
        if not isinstance(raw, dict) or set(raw) != {
            "mode",
            "path",
            "sha256",
            "size",
        }:
            raise OwnedOutputReadValidationError("expected_file_schema_invalid")
        try:
            path = RelativePath.parse(raw["path"])
        except (AuthorityError, TypeError, ValueError) as error:
            raise OwnedOutputReadValidationError("expected_file_path_invalid") from error
        return cls(path, raw["size"], raw["sha256"], raw["mode"])


@dataclass(frozen=True, slots=True)
class TreeExpectation:
    root_name: str
    files: tuple[FileExpectation, ...]
    directories: tuple[RelativePath, ...]
    total_bytes: int
    content_sha256: str

    @classmethod
    def create(
        cls,
        root_name: str,
        files: Iterable[FileExpectation],
        directories: Iterable[RelativePath] = (),
    ) -> "TreeExpectation":
        if type(root_name) is not str:
            raise TypeError("expected tree root_name must be text")
        try:
            root_component = RelativePath((root_name,))
        except (AuthorityError, TypeError, ValueError) as error:
            raise OwnedOutputReadValidationError("expected_tree_root_name_invalid") from error
        _relative(root_component, allow_root=False)
        if isinstance(files, (str, bytes)):
            raise TypeError("expected tree files must be an iterable of records")
        try:
            materialized_files = tuple(
                islice(iter(files), MAX_TREE_ENTRIES + 1)
            )
        except TypeError as error:
            raise TypeError("expected tree files must be iterable") from error
        if not materialized_files or len(materialized_files) > MAX_TREE_ENTRIES or any(
            not isinstance(item, FileExpectation) for item in materialized_files
        ):
            raise OwnedOutputReadLimitExceeded("expected_tree_file_count_invalid")
        ordered_files = tuple(sorted(materialized_files, key=lambda item: item.path.parts))
        if len({item.path.parts for item in ordered_files}) != len(ordered_files):
            raise OwnedOutputReadValidationError("expected_tree_duplicate_file")
        if isinstance(directories, (str, bytes)):
            raise TypeError("expected tree directories must be an iterable of paths")
        try:
            supplied_directories = tuple(
                islice(iter(directories), MAX_TREE_ENTRIES + 1)
            )
        except TypeError as error:
            raise TypeError("expected tree directories must be iterable") from error
        if len(supplied_directories) > MAX_TREE_ENTRIES:
            raise OwnedOutputReadLimitExceeded("expected_tree_directory_count_invalid")
        explicit: set[tuple[str, ...]] = set()
        for directory in supplied_directories:
            path = _relative(directory, allow_root=False)
            explicit.add(path.parts)
        if len(explicit) != len(supplied_directories):
            raise OwnedOutputReadValidationError("expected_tree_duplicate_directory")
        logical = set(explicit)
        for path in (*explicit, *(item.path.parts for item in ordered_files)):
            for length in range(1, len(path)):
                logical.add(path[:length])
        file_paths = {item.path.parts for item in ordered_files}
        if file_paths & logical:
            raise OwnedOutputReadValidationError("expected_tree_path_kind_conflict")
        aliases: dict[tuple[str, ...], str] = {}
        for parts in (*logical, *file_paths):
            folded = tuple(component.casefold() for component in parts)
            text = "/".join(parts)
            previous = aliases.get(folded)
            if previous is not None and previous != text:
                raise OwnedOutputReadValidationError(
                    "expected_tree_case_alias_collision"
                )
            aliases[folded] = text
        ordered_directories = tuple(
            RelativePath(parts)
            for parts in sorted(logical, key=lambda item: (len(item), item))
        )
        if len(ordered_directories) + len(ordered_files) + 1 > MAX_TREE_ENTRIES:
            raise OwnedOutputReadLimitExceeded("expected_tree_entry_count_invalid")
        total = sum(item.size for item in ordered_files)
        if total > MAX_TREE_TOTAL_BYTES:
            raise OwnedOutputReadLimitExceeded("expected_tree_total_bytes_invalid")
        content = hashlib.sha256(
            _canonical(
                {
                    "directories": [str(item) for item in ordered_directories],
                    "files": [item.to_mapping() for item in ordered_files],
                }
            )
        ).hexdigest()
        return cls(root_name, ordered_files, ordered_directories, total, content)

    @classmethod
    def from_mapping(cls, raw: object) -> "TreeExpectation":
        if not isinstance(raw, dict) or set(raw) != {
            "content_sha256",
            "directories",
            "directory_count",
            "file_count",
            "files",
            "root_name",
            "total_bytes",
        }:
            raise OwnedOutputReadValidationError("expected_tree_schema_invalid")
        if (
            not isinstance(raw["files"], list)
            or not isinstance(raw["directories"], list)
            or not isinstance(raw["root_name"], str)
        ):
            raise OwnedOutputReadValidationError("expected_tree_values_invalid")
        try:
            directories = tuple(RelativePath.parse(item) for item in raw["directories"])
        except (AuthorityError, TypeError, ValueError) as error:
            raise OwnedOutputReadValidationError(
                "expected_tree_directory_path_invalid"
            ) from error
        created = cls.create(
            raw["root_name"],
            (FileExpectation.from_mapping(item) for item in raw["files"]),
            directories,
        )
        if (
            type(raw["file_count"]) is not int
            or type(raw["directory_count"]) is not int
            or type(raw["total_bytes"]) is not int
            or not isinstance(raw["content_sha256"], str)
            or raw["file_count"] != len(created.files)
            or raw["directory_count"] != len(created.directories)
            or raw["directories"] != [str(item) for item in created.directories]
            or raw["total_bytes"] != created.total_bytes
            or raw["content_sha256"] != created.content_sha256
        ):
            raise OwnedOutputReadValidationError("expected_tree_seal_invalid")
        return created

    def to_mapping(self) -> dict[str, object]:
        return {
            "content_sha256": self.content_sha256,
            "directories": [str(item) for item in self.directories],
            "directory_count": len(self.directories),
            "file_count": len(self.files),
            "files": [item.to_mapping() for item in self.files],
            "root_name": self.root_name,
            "total_bytes": self.total_bytes,
        }


@dataclass(frozen=True, slots=True)
class FileObservation:
    path: RelativePath
    size: int
    sha256: str
    physical_mode: int | None


@dataclass(frozen=True, slots=True)
class TreeFingerprint:
    content_sha256: str
    directories: tuple[RelativePath, ...]
    empty_directories: tuple[RelativePath, ...]
    files: tuple[FileObservation, ...]
    physical_directory_modes: tuple[tuple[RelativePath, int | None], ...]
    physical_root_mode: int | None
    sandbox_owner_ids: tuple[int, ...]
    logical_file_modes: tuple[int, ...]
    total_bytes: int

    def to_mapping(self) -> dict[str, object]:
        return {
            "content_sha256": self.content_sha256,
            "directory_count": len(self.directories),
            "empty_directories": [str(item) for item in self.empty_directories],
            "explicit_empty_directories_proven": True,
            "file_count": len(self.files),
            "logical_directory_mode": 0o755,
            "logical_file_modes": list(self.logical_file_modes),
            "logical_target_group": "root",
            "logical_target_owner": "root",
            "physical_mode_enforced": os.name != "nt",
            "sandbox_owner_ids": list(self.sandbox_owner_ids),
            "single_link_files": True,
            "total_bytes": self.total_bytes,
        }


@dataclass(frozen=True, slots=True)
class JournalReadResult:
    events: tuple[dict[str, object], ...]
    head_sha256: str
    total_bytes: int


@dataclass(slots=True)
class _TreeBudget:
    limits: TreeReadLimits
    entries: int = 0
    total_bytes: int = 0

    def add_entry(self, depth: int) -> None:
        if depth > self.limits.maximum_depth:
            raise OwnedOutputReadLimitExceeded("owned_tree_depth_limit_exceeded")
        self.entries += 1
        if self.entries > self.limits.maximum_entries:
            raise OwnedOutputReadLimitExceeded("owned_tree_entry_limit_exceeded")

    def add_file(self, size: int) -> None:
        if size > self.limits.maximum_file_bytes:
            raise OwnedOutputReadLimitExceeded("owned_tree_file_limit_exceeded")
        if self.total_bytes + size > self.limits.maximum_total_bytes:
            raise OwnedOutputReadLimitExceeded("owned_tree_total_limit_exceeded")
        self.total_bytes += size


@dataclass(slots=True)
class _TreeScan:
    files: list[FileObservation]
    directories: list[RelativePath]
    directory_modes: list[tuple[RelativePath, int | None]]
    child_counts: dict[tuple[str, ...], int]
    owner_ids: set[int]
    root_mode: int | None
    total_bytes: int


T = TypeVar("T")


def _sealed_tree_expectation(
    value: object,
    label: str,
) -> TreeExpectation:
    if not isinstance(value, TreeExpectation):
        raise TypeError(f"{label} must be TreeExpectation")
    if (
        type(value.files) is not tuple
        or type(value.directories) is not tuple
        or len(value.files) > MAX_TREE_ENTRIES
        or len(value.directories) > MAX_TREE_ENTRIES
    ):
        raise OwnedOutputReadLimitExceeded(f"{label}_entry_limit_exceeded")
    try:
        sealed = TreeExpectation.from_mapping(value.to_mapping())
    except OwnedOutputReadError:
        raise
    except (AttributeError, TypeError, ValueError) as error:
        raise OwnedOutputReadValidationError(f"{label}_seal_invalid") from error
    if sealed != value:
        raise OwnedOutputReadValidationError(f"{label}_seal_invalid")
    return sealed


class OwnedOutputReadAuthority:
    """Read-only adapter bound to one simulator-owned run root."""

    __slots__ = ("_closed", "_output", "_root", "_root_identity")

    def __init__(
        self,
        root: DirectoryAuthority,
        output: ReleaseInstallOutputAuthority,
    ) -> None:
        if not isinstance(root, DirectoryAuthority) or root.closed:
            raise TypeError("root must be an open DirectoryAuthority")
        if not isinstance(output, ReleaseInstallOutputAuthority) or output.closed:
            raise TypeError("output must be an open ReleaseInstallOutputAuthority")
        try:
            identity = root.identity()
        except AuthorityError as error:
            raise _translate_authority(
                error, "owned_output_root_identity"
            ) from error
        output_identity = output.root_identity
        if not _identity_stable(identity, output_identity):
            raise OwnedOutputReadValidationError("owned_output_root_binding_differs")
        try:
            duplicate = root.duplicate()
        except AuthorityError as error:
            raise _translate_authority(error, "owned_output_root_duplicate") from error
        self._root = duplicate
        self._output = output
        self._root_identity = identity
        self._closed = False
        try:
            self._prove_root()
        except BaseException:
            try:
                duplicate.close()
            except AuthorityError:
                pass
            self._closed = True
            raise

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def root_identity(self) -> NodeIdentity:
        self._require_open()
        return self._root_identity

    def _require_open(self) -> None:
        if self._closed:
            raise OwnedOutputReadValidationError("owned_output_reader_closed")

    def _prove_root(self) -> None:
        self._require_open()
        try:
            pinned = self._root.identity()
            rebound = self._output.reprove_run_root()
        except (AuthorityError, OutputAuthorityError) as error:
            raise OwnedOutputReadChanged("owned_output_root_reproof_failed") from error
        if (
            not _identity_stable(self._root_identity, pinned)
            or not _identity_stable(self._root_identity, rebound)
            or not _identity_stable(pinned, rebound)
        ):
            raise OwnedOutputReadChanged("owned_output_root_identity_changed")

    def _list(
        self,
        directory: DirectoryAuthority,
        maximum_entries: int,
    ) -> tuple[DirectoryEntry, ...]:
        try:
            entries = directory.list_entries(
                RelativePath.root(),
                maximum_entries=maximum_entries,
            )
        except AuthorityError as error:
            raise _translate_authority(error, "owned_output_directory_list") from error
        aliases: dict[str, str] = {}
        for entry in entries:
            if unicodedata.normalize("NFC", entry.name) != entry.name:
                raise OwnedOutputReadValidationError(
                    "owned_output_entry_name_not_nfc"
                )
            folded = unicodedata.normalize("NFC", entry.name).casefold()
            previous = aliases.get(folded)
            if previous is not None and previous != entry.name:
                raise OwnedOutputReadValidationError(
                    "owned_output_case_alias_collision"
                )
            aliases[folded] = entry.name
        return entries

    def _exact_entry(
        self,
        directory: DirectoryAuthority,
        name: str,
        maximum_entries: int,
        *,
        missing_ok: bool = False,
    ) -> DirectoryEntry | None:
        entries = self._list(directory, maximum_entries)
        return self._exact_from_entries(
            entries,
            name,
            missing_ok=missing_ok,
        )

    @staticmethod
    def _exact_from_entries(
        entries: tuple[DirectoryEntry, ...],
        name: str,
        *,
        missing_ok: bool = False,
    ) -> DirectoryEntry | None:
        exact = next((entry for entry in entries if entry.name == name), None)
        if exact is not None:
            return exact
        folded = unicodedata.normalize("NFC", name).casefold()
        if any(
            unicodedata.normalize("NFC", entry.name).casefold() == folded
            for entry in entries
        ):
            raise OwnedOutputReadValidationError(
                "owned_output_component_case_alias"
            )
        if missing_ok:
            return None
        raise OwnedOutputReadNotFound("owned_output_exact_component_absent")

    @staticmethod
    def _same_entries(
        before: tuple[DirectoryEntry, ...],
        after: tuple[DirectoryEntry, ...],
    ) -> bool:
        return before == after

    @contextmanager
    def _directory(
        self,
        relative: RelativePath,
        maximum_entries: int,
    ) -> Iterator[DirectoryAuthority]:
        path = _relative(relative, allow_root=True)
        self._prove_root()
        chain: list[DirectoryAuthority] = []
        links: list[tuple[DirectoryAuthority, str, NodeIdentity, DirectoryAuthority]] = []
        opened: NodeIdentity | None = None
        current: DirectoryAuthority | None = None
        primary: BaseException | None = None
        try:
            try:
                current = self._root.duplicate()
            except AuthorityError as error:
                raise _translate_authority(error, "owned_output_root_duplicate") from error
            chain.append(current)
            for component in path.parts:
                entry = self._exact_entry(current, component, maximum_entries)
                assert entry is not None
                if entry.identity.kind != "directory":
                    raise OwnedOutputReadValidationError(
                        "owned_output_expected_directory"
                    )
                child: DirectoryAuthority | None = None
                try:
                    child = current.open_directory(RelativePath((component,)))
                    child_identity = child.identity()
                except AuthorityError as error:
                    if child is not None:
                        try:
                            child.close()
                        except AuthorityError as close_error:
                            raise _translate_authority(
                                close_error,
                                "owned_output_directory_identity_failure_close",
                            ) from error
                    raise _translate_authority(
                        error, "owned_output_directory_open"
                    ) from error
                if not _identity_stable(entry.identity, child_identity):
                    try:
                        child.close()
                    except AuthorityError as close_error:
                        raise _translate_authority(
                            close_error,
                            "owned_output_directory_mismatch_close",
                        )
                    raise OwnedOutputReadChanged(
                        "owned_output_directory_changed_during_open"
                    )
                links.append((current, component, entry.identity, child))
                chain.append(child)
                current = child
            try:
                opened = current.identity()
            except AuthorityError as error:
                raise _translate_authority(
                    error, "owned_output_directory_identity"
                ) from error
            yield current
        except BaseException as error:
            primary = error
            raise
        finally:
            proof_error: BaseException | None = None
            if opened is not None and current is not None:
                try:
                    final_identity = current.identity()
                    if not _identity_stable(opened, final_identity):
                        raise OwnedOutputReadChanged(
                            "owned_output_directory_changed_during_consumer"
                        )
                except BaseException as error:
                    proof_error = _proof_error(
                        error, "owned_output_directory_postidentity"
                    )
            for parent, component, expected, child in reversed(links):
                try:
                    if not _identity_stable(expected, child.identity()):
                        raise OwnedOutputReadChanged(
                            "owned_output_directory_handle_changed"
                        )
                    rebound = self._exact_entry(parent, component, maximum_entries)
                    if rebound is None or not _identity_stable(expected, rebound.identity):
                        raise OwnedOutputReadChanged(
                            "owned_output_directory_name_changed"
                        )
                except BaseException as error:
                    if proof_error is None:
                        proof_error = _proof_error(
                            error, "owned_output_directory_postproof"
                        )
            for authority in reversed(chain):
                try:
                    authority.close()
                except AuthorityError as error:
                    if proof_error is None:
                        proof_error = _translate_authority(
                            error, "owned_output_directory_close"
                        )
            try:
                self._prove_root()
            except BaseException as error:
                if proof_error is None:
                    proof_error = error
            if proof_error is not None:
                if primary is not None:
                    raise proof_error from primary
                raise proof_error

    @contextmanager
    def _child_file(
        self,
        parent: DirectoryAuthority,
        name: str,
        maximum_entries: int,
    ) -> Iterator[tuple[BoundFile, NodeIdentity]]:
        entry = self._exact_entry(parent, name, maximum_entries)
        assert entry is not None
        if entry.identity.kind != "regular_file" or entry.identity.link_count != 1:
            raise OwnedOutputReadValidationError(
                "owned_output_expected_single_link_regular_file"
            )
        with self._child_file_from_entry(parent, entry) as opened:
            primary: BaseException | None = None
            try:
                yield opened
            except BaseException as error:
                primary = error
                raise
            finally:
                proof_error: BaseException | None = None
                try:
                    rebound = self._exact_entry(parent, name, maximum_entries)
                    if rebound is None or not _identity_stable(
                        opened[1], rebound.identity
                    ):
                        raise OwnedOutputReadChanged(
                            "owned_output_file_name_changed"
                        )
                except BaseException as error:
                    proof_error = error
                if proof_error is not None:
                    if primary is not None:
                        raise proof_error from primary
                    raise proof_error

    @contextmanager
    def _child_directory(
        self,
        parent: DirectoryAuthority,
        name: str,
        maximum_entries: int,
    ) -> Iterator[DirectoryAuthority]:
        entry = self._exact_entry(parent, name, maximum_entries)
        assert entry is not None
        with self._child_directory_from_entry(parent, entry) as opened:
            primary: BaseException | None = None
            try:
                yield opened
            except BaseException as error:
                primary = error
                raise
            finally:
                proof_error: BaseException | None = None
                try:
                    rebound = self._exact_entry(parent, name, maximum_entries)
                    if rebound is None or not _identity_stable(
                        entry.identity, rebound.identity
                    ):
                        raise OwnedOutputReadChanged(
                            "owned_output_directory_name_changed"
                        )
                except BaseException as error:
                    proof_error = error
                if proof_error is not None:
                    if primary is not None:
                        raise proof_error from primary
                    raise proof_error

    @contextmanager
    def _child_file_from_entry(
        self,
        parent: DirectoryAuthority,
        entry: DirectoryEntry,
    ) -> Iterator[tuple[BoundFile, NodeIdentity]]:
        """Open one captured listing entry without re-listing its parent."""

        if not isinstance(entry, DirectoryEntry):
            raise TypeError("entry must be DirectoryEntry")
        if entry.identity.kind != "regular_file" or entry.identity.link_count != 1:
            raise OwnedOutputReadValidationError(
                "owned_output_expected_single_link_regular_file"
            )
        bound: BoundFile | None = None
        try:
            bound = parent.open_file(RelativePath((entry.name,)))
            opened = bound.identity()
        except AuthorityError as error:
            if bound is not None:
                try:
                    bound.close()
                except AuthorityError as close_error:
                    raise _translate_authority(
                        close_error,
                        "owned_output_file_identity_failure_close",
                    ) from error
            raise _translate_authority(error, "owned_output_file_open") from error
        if not _identity_stable(entry.identity, opened):
            try:
                bound.close()
            except AuthorityError as close_error:
                raise _translate_authority(
                    close_error,
                    "owned_output_file_mismatch_close",
                )
            raise OwnedOutputReadChanged("owned_output_file_changed_during_open")
        primary: BaseException | None = None
        try:
            yield bound, opened
        except BaseException as error:
            primary = error
            raise
        finally:
            proof_error: BaseException | None = None
            try:
                if not _identity_stable(opened, bound.identity()):
                    raise OwnedOutputReadChanged(
                        "owned_output_file_handle_changed"
                    )
            except BaseException as error:
                proof_error = _proof_error(
                    error, "owned_output_file_postproof"
                )
            try:
                bound.close()
            except AuthorityError as error:
                if proof_error is None:
                    proof_error = _translate_authority(
                        error, "owned_output_file_close"
                    )
            if proof_error is not None:
                if primary is not None:
                    raise proof_error from primary
                raise proof_error

    @contextmanager
    def _child_directory_from_entry(
        self,
        parent: DirectoryAuthority,
        entry: DirectoryEntry,
    ) -> Iterator[DirectoryAuthority]:
        """Open one captured directory entry without re-listing its parent."""

        if not isinstance(entry, DirectoryEntry):
            raise TypeError("entry must be DirectoryEntry")
        if entry.identity.kind != "directory":
            raise OwnedOutputReadValidationError(
                "owned_output_expected_directory"
            )
        child: DirectoryAuthority | None = None
        try:
            child = parent.open_directory(RelativePath((entry.name,)))
            opened = child.identity()
        except AuthorityError as error:
            if child is not None:
                try:
                    child.close()
                except AuthorityError as close_error:
                    raise _translate_authority(
                        close_error,
                        "owned_output_directory_identity_failure_close",
                    ) from error
            raise _translate_authority(
                error, "owned_output_directory_open"
            ) from error
        if not _identity_stable(entry.identity, opened):
            try:
                child.close()
            except AuthorityError as close_error:
                raise _translate_authority(
                    close_error,
                    "owned_output_directory_mismatch_close",
                )
            raise OwnedOutputReadChanged(
                "owned_output_directory_changed_during_open"
            )
        primary: BaseException | None = None
        try:
            yield child
        except BaseException as error:
            primary = error
            raise
        finally:
            proof_error: BaseException | None = None
            try:
                if not _identity_stable(opened, child.identity()):
                    raise OwnedOutputReadChanged(
                        "owned_output_directory_handle_changed"
                    )
            except BaseException as error:
                proof_error = _proof_error(
                    error, "owned_output_directory_postproof"
                )
            try:
                child.close()
            except AuthorityError as error:
                if proof_error is None:
                    proof_error = _translate_authority(
                        error, "owned_output_directory_close"
                    )
            if proof_error is not None:
                if primary is not None:
                    raise proof_error from primary
                raise proof_error

    @contextmanager
    def _file(
        self,
        relative: RelativePath,
        maximum_entries: int,
    ) -> Iterator[tuple[BoundFile, NodeIdentity]]:
        path = _relative(relative, allow_root=False)
        with self._directory(path.parent, maximum_entries) as parent:
            with self._child_file(parent, path.leaf, maximum_entries) as opened:
                yield opened

    @staticmethod
    def _physical_file_details(bound: BoundFile) -> tuple[int | None, int]:
        if os.name == "nt":
            return None, 0
        try:
            details = os.fstat(bound.native_handle)
        except (OSError, ValueError) as error:
            raise OwnedOutputReadIOError("owned_output_file_stat_failed") from error
        if not stat.S_ISREG(details.st_mode) or int(details.st_nlink) != 1:
            raise OwnedOutputReadValidationError(
                "owned_output_file_physical_kind_invalid"
            )
        return stat.S_IMODE(details.st_mode), int(details.st_uid)

    @staticmethod
    def _physical_directory_details(
        directory: DirectoryAuthority,
    ) -> tuple[int | None, int]:
        if os.name == "nt":
            return None, 0
        try:
            details = os.fstat(directory.native_directory_handle)
        except (OSError, ValueError) as error:
            raise OwnedOutputReadIOError(
                "owned_output_directory_stat_failed"
            ) from error
        if not stat.S_ISDIR(details.st_mode):
            raise OwnedOutputReadValidationError(
                "owned_output_directory_physical_kind_invalid"
            )
        return stat.S_IMODE(details.st_mode), int(details.st_uid)

    def strict_json(
        self,
        relative: RelativePath,
        *,
        label: str,
        maximum_bytes: int = MAX_JSON_BYTES,
    ) -> object:
        path = _relative(relative, allow_root=False)
        if type(label) is not str or not label:
            raise TypeError("strict JSON label must be non-empty text")
        limit = _bounded_integer(maximum_bytes, "maximum_bytes", 0, MAX_JSON_BYTES)
        with self._file(path, MAX_DIRECTORY_ENTRIES) as (bound, identity):
            if identity.size > limit:
                raise OwnedOutputReadLimitExceeded(
                    f"{label}_json_byte_limit_exceeded"
                )
            try:
                value = bound.read_bounded(limit)
            except AuthorityError as error:
                raise _translate_authority(error, "owned_output_json_read") from error
            if not isinstance(value, bytes):
                raise OwnedOutputReadValidationError(
                    f"{label}_json_read_returned_nonbytes"
                )
            if len(value) != identity.size:
                raise OwnedOutputReadChanged(
                    f"{label}_json_size_changed_during_read"
                )
            return _strict_json_bytes(value, label, limit)

    def read_bounded(
        self,
        relative: RelativePath,
        *,
        maximum_bytes: int,
        expected_size: int | None = None,
        expected_sha256: str | None = None,
    ) -> bytes:
        """Read one exact regular file while its name and root remain proved."""

        path = _relative(relative, allow_root=False)
        maximum = _bounded_integer(
            maximum_bytes,
            "maximum_bytes",
            0,
            min(MAX_READ_BYTES, MAX_TREE_TOTAL_BYTES),
        )
        if expected_size is not None:
            _bounded_integer(expected_size, "expected_size", 0, maximum)
        if expected_sha256 is not None and (
            not isinstance(expected_sha256, str)
            or _SHA256.fullmatch(expected_sha256) is None
        ):
            raise OwnedOutputReadValidationError(
                "owned_output_expected_sha256_invalid"
            )
        with self._file(path, MAX_DIRECTORY_ENTRIES) as (bound, identity):
            if identity.size > maximum:
                raise OwnedOutputReadLimitExceeded(
                    "owned_output_file_byte_limit_exceeded"
                )
            if expected_size is not None and identity.size != expected_size:
                raise OwnedOutputReadValidationError(
                    "owned_output_file_size_differs"
                )
            try:
                value = bound.read_bounded(maximum)
            except AuthorityError as error:
                raise _translate_authority(
                    error, "owned_output_file_read"
                ) from error
            if not isinstance(value, bytes):
                raise OwnedOutputReadValidationError(
                    "owned_output_file_read_returned_nonbytes"
                )
            if len(value) > maximum:
                raise OwnedOutputReadLimitExceeded(
                    "owned_output_file_read_overreturned"
                )
            if len(value) != identity.size:
                raise OwnedOutputReadChanged(
                    "owned_output_file_size_changed_during_read"
                )
            digest = hashlib.sha256(value).hexdigest()
            if expected_sha256 is not None and digest != expected_sha256:
                raise OwnedOutputReadValidationError(
                    "owned_output_file_sha256_differs"
                )
            return value

    def read_utf8_text(
        self,
        relative: RelativePath,
        *,
        maximum_bytes: int,
        expected_size: int | None = None,
        expected_sha256: str | None = None,
    ) -> str:
        value = self.read_bounded(
            relative,
            maximum_bytes=maximum_bytes,
            expected_size=expected_size,
            expected_sha256=expected_sha256,
        )
        if value.startswith(b"\xef\xbb\xbf"):
            raise OwnedOutputReadValidationError("owned_output_utf8_bom_forbidden")
        try:
            return value.decode("utf-8", "strict")
        except UnicodeError as error:
            raise OwnedOutputReadValidationError(
                "owned_output_text_not_utf8"
            ) from error

    def node_kind(
        self,
        relative: RelativePath,
        *,
        maximum_directory_entries: int = MAX_DIRECTORY_ENTRIES,
    ) -> str:
        path = _relative(relative, allow_root=False)
        maximum = _bounded_integer(
            maximum_directory_entries,
            "maximum_directory_entries",
            0,
            MAX_DIRECTORY_ENTRIES,
        )
        try:
            with self._directory(path.parent, maximum) as parent:
                before = self._list(parent, maximum)
                entry = self._exact_from_entries(
                    before, path.leaf, missing_ok=True
                )
                if entry is None:
                    after = self._list(parent, maximum)
                    if not self._same_entries(before, after):
                        raise OwnedOutputReadChanged(
                            "owned_output_absence_changed_during_proof"
                        )
                    return "absent"
                if entry.identity.kind == "directory":
                    with self._child_directory(parent, path.leaf, maximum):
                        return "directory"
                if entry.identity.kind == "regular_file":
                    with self._child_file(parent, path.leaf, maximum):
                        return "regular_file"
                raise OwnedOutputReadValidationError("owned_output_node_kind_forbidden")
        except OwnedOutputReadNotFound:
            return "absent"

    @contextmanager
    def archive_stream(
        self,
        relative: RelativePath,
        *,
        maximum_bytes: int,
        expected_size: int | None = None,
    ) -> Iterator[BinaryIO]:
        path = _relative(relative, allow_root=False)
        maximum = _bounded_integer(
            maximum_bytes, "maximum_bytes", 0, min(MAX_READ_BYTES, MAX_TREE_TOTAL_BYTES)
        )
        if expected_size is not None:
            _bounded_integer(expected_size, "expected_size", 0, maximum)
        with self._file(path, MAX_DIRECTORY_ENTRIES) as (bound, identity):
            if identity.size > maximum:
                raise OwnedOutputReadLimitExceeded(
                    "owned_archive_byte_limit_exceeded"
                )
            if expected_size is not None and identity.size != expected_size:
                raise OwnedOutputReadValidationError("owned_archive_size_differs")
            try:
                duplicate = bound.duplicate_binary("rb")
            except AuthorityError as error:
                raise _translate_authority(
                    error, "owned_archive_stream_duplicate"
                ) from error
            try:
                stream = _BoundedDuplicate(duplicate, identity.size)
            except BaseException:
                try:
                    duplicate.close()
                except OSError:
                    pass
                raise
            try:
                primary: BaseException | None = None
                try:
                    yield stream
                except BaseException as error:
                    primary = error
                    raise
            finally:
                try:
                    stream.close()
                except (OSError, ValueError, OwnedOutputReadError) as error:
                    if isinstance(error, OwnedOutputReadError):
                        if primary is not None:
                            raise error from primary
                        raise
                    close_error = OwnedOutputReadIOError(
                        "owned_archive_stream_close_failed"
                    )
                    if primary is not None:
                        raise close_error from primary
                    raise close_error from error

    def consume_archive(
        self,
        relative: RelativePath,
        consumer: Callable[[BinaryIO], T],
        *,
        maximum_bytes: int,
        expected_size: int | None = None,
    ) -> T:
        if not callable(consumer):
            raise TypeError("archive consumer must be callable")
        with self.archive_stream(
            relative,
            maximum_bytes=maximum_bytes,
            expected_size=expected_size,
        ) as stream:
            return consumer(stream)

    def _read_child_json(
        self,
        parent: DirectoryAuthority,
        entry: DirectoryEntry,
        *,
        label: str,
        maximum_bytes: int,
    ) -> tuple[object, int]:
        with self._child_file_from_entry(parent, entry) as (bound, identity):
            if identity.size > maximum_bytes:
                raise OwnedOutputReadLimitExceeded(
                    f"{label}_json_byte_limit_exceeded"
                )
            try:
                value = bound.read_bounded(maximum_bytes)
            except AuthorityError as error:
                raise _translate_authority(error, "owned_journal_event_read") from error
            if not isinstance(value, bytes):
                raise OwnedOutputReadValidationError(
                    f"{label}_json_read_returned_nonbytes"
                )
            if len(value) != identity.size:
                raise OwnedOutputReadChanged(
                    f"{label}_json_size_changed_during_read"
                )
            return _strict_json_bytes(value, label, maximum_bytes), len(value)

    def read_journal(
        self,
        relative: RelativePath,
        *,
        expected_contract: str,
        limits: JournalReadLimits = JournalReadLimits(),
        validate_sequence: Callable[[tuple[Mapping[str, object], ...]], None]
        | None = None,
    ) -> JournalReadResult:
        path = _relative(relative, allow_root=False)
        if type(expected_contract) is not str or not expected_contract:
            raise TypeError("expected_contract must be non-empty text")
        if not isinstance(limits, JournalReadLimits):
            raise TypeError("limits must be JournalReadLimits")
        if validate_sequence is not None and not callable(validate_sequence):
            raise TypeError("validate_sequence must be callable or None")
        with self._directory(path, limits.maximum_directory_entries) as journal:
            before = self._list(journal, limits.maximum_directory_entries)
            json_entries: list[DirectoryEntry] = []
            for entry in before:
                if entry.identity.kind != "regular_file":
                    raise OwnedOutputReadValidationError(
                        "owned_journal_entry_not_regular_file"
                    )
                folded = entry.name.casefold()
                if folded.endswith(".json"):
                    if not entry.name.endswith(".json"):
                        raise OwnedOutputReadValidationError(
                            "owned_journal_suffix_case_alias"
                        )
                    json_entries.append(entry)
                elif _JOURNAL_PENDING.fullmatch(entry.name) is None:
                    raise OwnedOutputReadValidationError(
                        "owned_journal_entry_name_not_canonical"
                    )
            json_entries.sort(key=lambda item: item.name)
            if len(json_entries) > limits.maximum_events:
                raise OwnedOutputReadLimitExceeded(
                    "owned_journal_event_limit_exceeded"
                )
            events: list[dict[str, object]] = []
            previous = _ZERO_SHA256
            total_bytes = 0
            for sequence, entry in enumerate(json_entries, start=1):
                canonical_name = f"{sequence:06d}.json"
                if entry.name != canonical_name:
                    raise OwnedOutputReadValidationError(
                        "owned_journal_sequence_name_not_canonical"
                    )
                document, size = self._read_child_json(
                    journal,
                    entry,
                    label="owned_journal_event",
                    maximum_bytes=limits.maximum_event_bytes,
                )
                total_bytes += size
                if total_bytes > limits.maximum_total_bytes:
                    raise OwnedOutputReadLimitExceeded(
                        "owned_journal_total_byte_limit_exceeded"
                    )
                if not isinstance(document, dict) or set(document) != {
                    "contract",
                    "event",
                    "event_sha256",
                    "payload",
                    "previous_sha256",
                    "sequence",
                }:
                    raise OwnedOutputReadValidationError(
                        "owned_journal_event_schema_invalid"
                    )
                supplied = document["event_sha256"]
                unsigned = dict(document)
                del unsigned["event_sha256"]
                if (
                    document["contract"] != expected_contract
                    or type(document["sequence"]) is not int
                    or document["sequence"] != sequence
                    or document["previous_sha256"] != previous
                    or not isinstance(supplied, str)
                    or _SHA256.fullmatch(supplied) is None
                    or supplied
                    != hashlib.sha256(_canonical(unsigned)).hexdigest()
                ):
                    raise OwnedOutputReadValidationError(
                        "owned_journal_hash_chain_invalid"
                    )
                events.append(document)
                previous = supplied
            materialized: tuple[Mapping[str, object], ...] = tuple(events)
            if validate_sequence is not None:
                event_seal = hashlib.sha256(_canonical(events)).hexdigest()
                validate_sequence(materialized)
                if hashlib.sha256(_canonical(events)).hexdigest() != event_seal:
                    raise OwnedOutputReadValidationError(
                        "owned_journal_sequence_validator_mutated_events"
                    )
            after = self._list(journal, limits.maximum_directory_entries)
            if not self._same_entries(before, after):
                raise OwnedOutputReadChanged(
                    "owned_journal_directory_changed_during_read"
                )
            return JournalReadResult(tuple(events), previous, total_bytes)

    def _hash_file(
        self,
        parent: DirectoryAuthority,
        entry: DirectoryEntry,
        relative: RelativePath,
        budget: _TreeBudget,
    ) -> tuple[FileObservation, int]:
        with self._child_file_from_entry(
            parent,
            entry,
        ) as (bound, identity):
            budget.add_file(identity.size)
            physical_mode, owner_id = self._physical_file_details(bound)
            digest = hashlib.sha256()
            total = 0
            try:
                stream = bound.duplicate_binary("rb")
            except AuthorityError as error:
                raise _translate_authority(error, "owned_tree_file_duplicate") from error
            try:
                while total < identity.size:
                    request = min(
                        budget.limits.chunk_bytes,
                        identity.size - total,
                    )
                    try:
                        chunk = stream.read(request)
                    except OSError as error:
                        raise OwnedOutputReadIOError(
                            "owned_tree_file_read_failed"
                        ) from error
                    if not isinstance(chunk, bytes):
                        raise OwnedOutputReadValidationError(
                            "owned_tree_file_read_returned_nonbytes"
                        )
                    if not chunk:
                        raise OwnedOutputReadChanged("owned_tree_file_truncated")
                    if len(chunk) > request:
                        raise OwnedOutputReadLimitExceeded(
                            "owned_tree_file_read_overreturned"
                        )
                    digest.update(chunk)
                    total += len(chunk)
                try:
                    overrun = stream.read(1)
                except OSError as error:
                    raise OwnedOutputReadIOError(
                        "owned_tree_file_read_failed"
                    ) from error
                if not isinstance(overrun, bytes):
                    raise OwnedOutputReadValidationError(
                        "owned_tree_file_read_returned_nonbytes"
                    )
                if overrun:
                    raise OwnedOutputReadChanged("owned_tree_file_grew_during_read")
            finally:
                try:
                    stream.close()
                except OSError as error:
                    raise OwnedOutputReadIOError(
                        "owned_tree_file_stream_close_failed"
                    ) from error
            final_mode, final_owner_id = self._physical_file_details(bound)
            if final_mode != physical_mode or final_owner_id != owner_id:
                raise OwnedOutputReadChanged("owned_tree_file_metadata_changed")
            observation = FileObservation(
                relative,
                total,
                digest.hexdigest(),
                physical_mode,
            )
            return observation, owner_id

    def _scan_directory(
        self,
        directory: DirectoryAuthority,
        prefix: RelativePath,
        depth: int,
        budget: _TreeBudget,
        scan: _TreeScan,
    ) -> None:
        try:
            opened = directory.identity()
        except AuthorityError as error:
            raise _translate_authority(
                error, "owned_tree_directory_identity"
            ) from error
        mode, owner_id = self._physical_directory_details(directory)
        scan.owner_ids.add(owner_id)
        before = self._list(
            directory,
            budget.limits.maximum_directory_entries,
        )
        scan.child_counts[prefix.parts] = len(before)
        for entry in before:
            try:
                child = (
                    prefix.child(entry.name)
                    if prefix.parts
                    else RelativePath((entry.name,))
                )
            except AuthorityLimitExceeded as error:
                raise OwnedOutputReadLimitExceeded(
                    "owned_tree_path_limit_exceeded"
                ) from error
            except AuthorityError as error:
                raise OwnedOutputReadValidationError(
                    "owned_tree_path_invalid"
                ) from error
            budget.add_entry(depth + 1)
            if entry.identity.kind == "directory":
                with self._child_directory_from_entry(
                    directory,
                    entry,
                ) as child_directory:
                    scan.directories.append(child)
                    child_mode, _child_owner = self._physical_directory_details(
                        child_directory
                    )
                    scan.directory_modes.append((child, child_mode))
                    self._scan_directory(
                        child_directory,
                        child,
                        depth + 1,
                        budget,
                        scan,
                    )
            elif entry.identity.kind == "regular_file":
                observation, file_owner = self._hash_file(
                    directory,
                    entry,
                    child,
                    budget,
                )
                scan.files.append(observation)
                scan.owner_ids.add(file_owner)
            else:
                raise OwnedOutputReadValidationError("owned_tree_node_kind_forbidden")
        after = self._list(
            directory,
            budget.limits.maximum_directory_entries,
        )
        if not self._same_entries(before, after):
            raise OwnedOutputReadChanged("owned_tree_directory_changed_during_scan")
        try:
            final_identity = directory.identity()
        except AuthorityError as error:
            raise _translate_authority(
                error, "owned_tree_directory_postidentity"
            ) from error
        if not _identity_stable(opened, final_identity):
            raise OwnedOutputReadChanged("owned_tree_directory_handle_changed")
        final_mode, _final_owner = self._physical_directory_details(directory)
        if final_mode != mode:
            raise OwnedOutputReadChanged("owned_tree_directory_mode_changed")

    def _scan_tree(
        self,
        relative: RelativePath,
        limits: TreeReadLimits,
    ) -> _TreeScan:
        path = _relative(relative, allow_root=False)
        if not isinstance(limits, TreeReadLimits):
            raise TypeError("limits must be TreeReadLimits")
        budget = _TreeBudget(limits)
        budget.add_entry(0)
        with self._directory(path, limits.maximum_directory_entries) as root:
            root_mode, root_owner = self._physical_directory_details(root)
            scan = _TreeScan([], [], [], {}, {root_owner}, root_mode, 0)
            self._scan_directory(
                root,
                RelativePath.root(),
                0,
                budget,
                scan,
            )
            scan.total_bytes = budget.total_bytes
            return scan

    @staticmethod
    def _scan_matches(scan: _TreeScan, expected: TreeExpectation) -> bool:
        actual_directories = {item.parts for item in scan.directories}
        if actual_directories != {item.parts for item in expected.directories}:
            return False
        actual_files = {item.path.parts: item for item in scan.files}
        if set(actual_files) != {item.path.parts for item in expected.files}:
            return False
        if scan.total_bytes != expected.total_bytes:
            return False
        for record in expected.files:
            actual = actual_files[record.path.parts]
            if actual.size != record.size or actual.sha256 != record.sha256:
                return False
            if actual.physical_mode is not None and actual.physical_mode != record.mode:
                return False
        if scan.root_mode is not None and scan.root_mode != 0o755:
            return False
        if any(mode is not None and mode != 0o755 for _path, mode in scan.directory_modes):
            return False
        return True

    @staticmethod
    def _fingerprint(scan: _TreeScan, expected: TreeExpectation) -> TreeFingerprint:
        if not OwnedOutputReadAuthority._scan_matches(scan, expected):
            raise OwnedOutputReadValidationError("owned_tree_fingerprint_differs")
        empty = tuple(
            path
            for path in expected.directories
            if scan.child_counts.get(path.parts, 0) == 0
        )
        return TreeFingerprint(
            expected.content_sha256,
            expected.directories,
            empty,
            tuple(sorted(scan.files, key=lambda item: item.path.parts)),
            tuple(sorted(scan.directory_modes, key=lambda item: item[0].parts)),
            scan.root_mode,
            tuple(sorted(scan.owner_ids)),
            tuple(sorted({item.mode for item in expected.files})),
            scan.total_bytes,
        )

    def fingerprint_tree(
        self,
        relative: RelativePath,
        expected: TreeExpectation,
        *,
        limits: TreeReadLimits = TreeReadLimits(),
    ) -> TreeFingerprint:
        path = _relative(relative, allow_root=False)
        expected = _sealed_tree_expectation(expected, "expected_tree")
        scan = self._scan_tree(path, limits)
        return self._fingerprint(scan, expected)

    def sealed_tree_kind(
        self,
        relative: RelativePath,
        baseline: TreeExpectation,
        candidate: TreeExpectation,
        *,
        limits: TreeReadLimits = TreeReadLimits(),
    ) -> str:
        path = _relative(relative, allow_root=False)
        if not isinstance(limits, TreeReadLimits):
            raise TypeError("limits must be TreeReadLimits")
        baseline = _sealed_tree_expectation(baseline, "baseline_tree")
        candidate = _sealed_tree_expectation(candidate, "candidate_tree")
        kind = self.node_kind(
            path,
            maximum_directory_entries=limits.maximum_directory_entries,
        )
        if kind == "absent":
            return "absent"
        if kind != "directory":
            raise OwnedOutputReadValidationError(
                "owned_generation_slot_not_directory"
            )
        scan = self._scan_tree(path, limits)
        matches = tuple(
            name
            for name, expected in (("baseline", baseline), ("candidate", candidate))
            if self._scan_matches(scan, expected)
        )
        if len(matches) != 1:
            if not matches:
                raise OwnedOutputReadValidationError(
                    "owned_generation_matches_neither_seal"
                )
            raise OwnedOutputReadValidationError(
                "owned_generation_seal_ambiguous"
            )
        return matches[0]

    def generation_inventory(
        self,
        *,
        baseline: Mapping[str, TreeExpectation],
        candidate: Mapping[str, TreeExpectation],
        allowed_layouts: Iterable[tuple[str, str, str, str]] | None = None,
        limits: TreeReadLimits = TreeReadLimits(),
    ) -> dict[str, tuple[str, str, str, str]]:
        if not isinstance(limits, TreeReadLimits):
            raise TypeError("limits must be TreeReadLimits")
        if not isinstance(baseline, Mapping) or not isinstance(candidate, Mapping):
            raise TypeError("baseline and candidate must be mappings")
        baseline_components = tuple(
            islice(iter(baseline), MAX_COMPONENTS + 1)
        )
        candidate_components = tuple(
            islice(iter(candidate), MAX_COMPONENTS + 1)
        )
        if (
            len(baseline_components) > MAX_COMPONENTS
            or len(candidate_components) > MAX_COMPONENTS
        ):
            raise OwnedOutputReadLimitExceeded(
                "owned_generation_component_limit_exceeded"
            )
        if (
            not baseline_components
            or set(baseline_components) != set(candidate_components)
        ):
            raise OwnedOutputReadValidationError(
                "owned_generation_component_sets_differ"
            )
        if any(type(component) is not str for component in baseline_components):
            raise TypeError("generation component names must be text")
        components = tuple(sorted(baseline_components))
        folded: set[str] = set()
        baseline_seals: dict[str, TreeExpectation] = {}
        candidate_seals: dict[str, TreeExpectation] = {}
        for component in components:
            path = RelativePath((component,))
            _relative(path, allow_root=False)
            alias = component.casefold()
            if alias in folded:
                raise OwnedOutputReadValidationError(
                    "owned_generation_component_case_alias"
                )
            folded.add(alias)
            baseline_seals[component] = _sealed_tree_expectation(
                baseline[component], f"baseline_{component}"
            )
            candidate_seals[component] = _sealed_tree_expectation(
                candidate[component], f"candidate_{component}"
            )
        allowed: set[tuple[str, str, str, str]] | None = None
        if allowed_layouts is not None:
            if isinstance(allowed_layouts, (str, bytes)):
                raise TypeError("allowed_layouts must be an iterable of tuples")
            try:
                materialized_layouts = tuple(
                    islice(iter(allowed_layouts), MAX_ALLOWED_LAYOUTS + 1)
                )
            except TypeError as error:
                raise TypeError("allowed_layouts must be iterable") from error
            if len(materialized_layouts) > MAX_ALLOWED_LAYOUTS:
                raise OwnedOutputReadLimitExceeded(
                    "owned_generation_allowed_layout_limit_exceeded"
                )
            if any(
                type(layout) is not tuple
                or len(layout) != 4
                or any(
                    item not in {"absent", "baseline", "candidate"}
                    for item in layout
                )
                for layout in materialized_layouts
            ):
                raise OwnedOutputReadValidationError(
                    "owned_generation_allowed_layout_invalid"
                )
            allowed = set(materialized_layouts)
        expected_by_container: dict[str, set[str]] = {
            name: set() for name in _GENERATION_CONTAINERS
        }
        for component in components:
            for _slot, container, suffix in _GENERATION_SLOTS:
                expected_by_container[container].add(f"{component}.{suffix}")
        container_snapshots: dict[str, tuple[DirectoryEntry, ...]] = {}
        for container in _GENERATION_CONTAINERS:
            relative = RelativePath((container,))
            with self._directory(relative, limits.maximum_directory_entries) as opened:
                entries = self._list(opened, limits.maximum_directory_entries)
                names = {entry.name for entry in entries}
                if not names <= expected_by_container[container]:
                    raise OwnedOutputReadValidationError(
                        "owned_generation_unexpected_slot"
                    )
                container_snapshots[container] = entries
        result: dict[str, tuple[str, str, str, str]] = {}
        for component in components:
            kinds = tuple(
                self.sealed_tree_kind(
                    RelativePath((container, f"{component}.{suffix}")),
                    baseline_seals[component],
                    candidate_seals[component],
                    limits=limits,
                )
                for _slot, container, suffix in _GENERATION_SLOTS
            )
            layout = (kinds[0], kinds[1], kinds[2], kinds[3])
            if allowed is not None and layout not in allowed:
                raise OwnedOutputReadValidationError(
                    "owned_generation_layout_not_allowed"
                )
            result[component] = layout
        for container in _GENERATION_CONTAINERS:
            relative = RelativePath((container,))
            with self._directory(relative, limits.maximum_directory_entries) as opened:
                after = self._list(opened, limits.maximum_directory_entries)
                if not self._same_entries(container_snapshots[container], after):
                    raise OwnedOutputReadChanged(
                        "owned_generation_inventory_changed_during_read"
                    )
        return result

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._root.close()
        except AuthorityError as error:
            raise _translate_authority(error, "owned_output_reader_close") from error

    def __enter__(self) -> "OwnedOutputReadAuthority":
        self._require_open()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            self.close()
        except BaseException as close_error:
            if exc is not None:
                raise close_error from exc
            raise


def owned_output_read_evidence() -> dict[str, object]:
    """Describe this isolated primitive without claiming simulator closure."""

    return {
        "contract": OWNED_OUTPUT_READ_CONTRACT,
        "absolute_path_reopen": False,
        "bounded_directory_iteration": True,
        "bounded_strict_json": True,
        "case_aliases_rejected": True,
        "consumer_exception_postproof": True,
        "descriptor_relative_owned_archive_stream": True,
        "descriptor_relative_owned_reads": True,
        "bounded_utf8_identity_reads": True,
        "hardlink_aliases_rejected": True,
        "journal_hash_chain_descriptor_relative": True,
        "node_identity_pre_and_postproof": True,
        "recursive_tree_fingerprint_descriptor_relative": True,
        "root_identity_pre_and_postproof": True,
        "simulator_integration_proven": False,
        "symlink_and_reparse_points_rejected": True,
        "local_acceptance_blockers_closed": [],
    }


__all__ = [
    "FileExpectation",
    "FileObservation",
    "JournalReadLimits",
    "JournalReadResult",
    "MAX_JSON_BYTES",
    "MAX_JOURNAL_EVENTS",
    "MAX_TREE_ENTRIES",
    "OWNED_OUTPUT_READ_CONTRACT",
    "OwnedOutputReadAuthority",
    "OwnedOutputReadChanged",
    "OwnedOutputReadError",
    "OwnedOutputReadIOError",
    "OwnedOutputReadLimitExceeded",
    "OwnedOutputReadNotFound",
    "OwnedOutputReadValidationError",
    "TreeExpectation",
    "TreeFingerprint",
    "TreeReadLimits",
    "owned_output_read_evidence",
]
