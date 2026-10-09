"""Descriptor-relative filesystem authority primitives.

This module is intentionally standalone.  It supplies the bounded primitive
needed by a later installer integration, but it does not claim that either
installer acceptance blocker is closed.  Every operation below is relative to
an already-open directory object:

* POSIX uses ``openat``-style ``dir_fd`` calls, ``O_NOFOLLOW`` and, on Linux,
  ``openat2(RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS)``
  when the running kernel accepts that contract;
* Windows opens one canonical full NT object path with ``NtCreateFile``, a
  null ``OBJECT_ATTRIBUTES.RootDirectory`` and ``OBJ_DONT_REPARSE``.  The
  resulting authority-root handle is identity pinned and every subsequent
  operation is relative to it.

The public capability probe mutates only one unpredictable, exclusively
created child directory and removes only that owned tree.  A successful probe
proves this primitive on that filesystem.  It deliberately reports simulator
integration as false and closes no local acceptance blocker.
"""
from __future__ import annotations

from dataclasses import dataclass
import ctypes
import errno
from itertools import islice
import os
from pathlib import Path
import platform
import stat
from typing import BinaryIO, Final, Iterable
import uuid


DESCRIPTOR_RELATIVE_AUTHORITY_CONTRACT: Final = (
    "stv3-descriptor-relative-authority-v1"
)
MAX_RELATIVE_DEPTH: Final = 64
MAX_RELATIVE_PATH_BYTES: Final = 4096
MAX_COMPONENT_BYTES: Final = 255
MAX_DIRECTORY_ENTRIES: Final = 20_000
MAX_READ_BYTES: Final = 2 * 1024 * 1024 * 1024
IO_CHUNK_BYTES: Final = 1024 * 1024
OPENAT2_RETRIES: Final = 8
_MAX_UNICODE_STRING_BYTES: Final = 0xFFFF - 2
_STATUS_REPARSE_POINT_ENCOUNTERED: Final = 0xC000050B

RENAME_NOREPLACE: Final = 1
RENAME_EXCL: Final = 0x00000004

_WINDOWS_RESERVED_NAMES: Final = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"COM{number}" for number in range(1, 10)}
    | {f"LPT{number}" for number in range(1, 10)}
)


class AuthorityError(RuntimeError):
    """Base error for a descriptor-relative operation that did not finish."""


class AuthorityConflict(AuthorityError):
    """An exclusive create/publication found an existing destination."""


class AuthorityNotFound(AuthorityError):
    """A requested authority-relative object does not exist."""


class AuthorityUnavailable(AuthorityError):
    """The platform/filesystem cannot supply the requested proof primitive."""


class AuthorityViolation(AuthorityError):
    """An object escaped policy, changed identity, or was a link/reparse point."""


class AuthorityLimitExceeded(AuthorityError):
    """A caller-controlled count, path, or byte limit was exceeded."""


class AuthorityIOError(AuthorityError):
    """The selected primitive failed for an unexpected I/O reason."""


class AuthorityPostCreateError(AuthorityIOError):
    """A create completed, but exact-identity rollback could not be proved."""

    def __init__(
        self,
        message: str,
        *,
        created_identity: "NodeIdentity | None",
    ) -> None:
        super().__init__(message)
        self.created_identity = created_identity


class AuthorityPostPublicationError(AuthorityIOError):
    """A no-replace publication completed, but later cleanup failed."""

    def __init__(
        self,
        message: str,
        *,
        backend: str,
        published_identity: "NodeIdentity",
        source: "RelativePath",
        destination: "RelativePath",
    ) -> None:
        super().__init__(message)
        self.backend = backend
        self.published_identity = published_identity
        self.source = source
        self.destination = destination


def _validate_component(value: str) -> str:
    if type(value) is not str:
        raise TypeError("relative path components must be strings")
    if value in {"", ".", ".."}:
        raise AuthorityViolation("relative_path_component_invalid")
    if "\x00" in value or "/" in value or "\\" in value or ":" in value:
        raise AuthorityViolation("relative_path_component_invalid")
    if value[-1] in {" ", "."}:
        raise AuthorityViolation("relative_path_component_windows_ambiguous")
    if any(ord(character) < 32 for character in value):
        raise AuthorityViolation("relative_path_component_control_character")
    try:
        encoded = value.encode("utf-8", "strict")
    except UnicodeError as error:
        raise AuthorityViolation("relative_path_component_not_utf8") from error
    if len(encoded) > MAX_COMPONENT_BYTES:
        raise AuthorityLimitExceeded("relative_path_component_too_long")
    if value.split(".", 1)[0].upper() in _WINDOWS_RESERVED_NAMES:
        raise AuthorityViolation("relative_path_component_windows_reserved")
    return value


def _win_absolute_nt_path(
    value: Path | str | os.PathLike[str],
) -> str:
    """Translate one canonical DOS/UNC path to a bounded NT object name."""

    raw = os.fspath(value)
    if type(raw) is not str:
        raise TypeError("Windows authority path must decode to text")
    if not raw or "\x00" in raw or "/" in raw:
        raise AuthorityViolation("windows_absolute_path_not_canonical")
    folded = raw.casefold()
    if folded.startswith(("\\\\?\\", "\\\\.\\", "\\??\\", "\\device\\")):
        raise AuthorityViolation("windows_namespace_path_forbidden")

    if raw.startswith("\\\\"):
        components = raw[2:].split("\\")
        if len(components) < 2 or any(not component for component in components):
            raise AuthorityViolation("windows_unc_path_malformed")
        prefix = "\\??\\UNC\\"
    else:
        if (
            len(raw) < 3
            or not raw[0].isascii()
            or not raw[0].isalpha()
            or raw[1:3] != ":\\"
        ):
            raise AuthorityViolation("windows_absolute_drive_path_required")
        tail = raw[3:]
        if tail:
            components = tail.split("\\")
            if any(not component for component in components):
                raise AuthorityViolation("windows_absolute_path_not_canonical")
        else:
            components = []
        prefix = f"\\??\\{raw[0].upper()}:\\"

    validated: list[str] = []
    for component in components:
        if any(character in '<>"|?*' for character in component):
            raise AuthorityViolation("windows_absolute_component_invalid")
        validated.append(_validate_component(component))
    if raw.startswith("\\\\"):
        translated = prefix + "\\".join(validated)
    else:
        translated = prefix + "\\".join(validated)
    try:
        encoded_length = len(translated.encode("utf-16-le", "strict"))
    except UnicodeError as error:
        raise AuthorityViolation("windows_absolute_path_not_utf16") from error
    if encoded_length > _MAX_UNICODE_STRING_BYTES:
        raise AuthorityLimitExceeded("windows_absolute_path_too_long")
    return translated


@dataclass(frozen=True, slots=True)
class RelativePath:
    """A bounded portable relative path represented by validated components."""

    parts: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.parts) is not tuple:
            raise TypeError("relative path parts must be a tuple")
        if len(self.parts) > MAX_RELATIVE_DEPTH:
            raise AuthorityLimitExceeded("relative_path_too_deep")
        validated = tuple(_validate_component(part) for part in self.parts)
        encoded_length = sum(len(part.encode("utf-8")) for part in validated)
        encoded_length += max(0, len(validated) - 1)
        if encoded_length > MAX_RELATIVE_PATH_BYTES:
            raise AuthorityLimitExceeded("relative_path_too_long")

    @classmethod
    def root(cls) -> "RelativePath":
        return cls(())

    @classmethod
    def parse(
        cls,
        value: "RelativePath | str | os.PathLike[str] | Iterable[str]",
    ) -> "RelativePath":
        if isinstance(value, cls):
            return value
        if isinstance(value, (str, os.PathLike)):
            raw = os.fspath(value)
            if type(raw) is not str:
                raise TypeError("relative path must decode to text")
            if raw == "":
                return cls.root()
            if raw.startswith(("/", "\\")) or "\\" in raw:
                raise AuthorityViolation("relative_path_must_be_portable_relative")
            parts = raw.split("/")
            return cls(tuple(parts))
        iterator = iter(value)
        parts = tuple(islice(iterator, MAX_RELATIVE_DEPTH + 1))
        if len(parts) > MAX_RELATIVE_DEPTH:
            raise AuthorityLimitExceeded("relative_path_too_deep")
        return cls(parts)

    @property
    def parent(self) -> "RelativePath":
        if not self.parts:
            raise AuthorityViolation("authority_root_has_no_parent")
        return RelativePath(self.parts[:-1])

    @property
    def leaf(self) -> str:
        if not self.parts:
            raise AuthorityViolation("authority_root_has_no_leaf")
        return self.parts[-1]

    def child(self, name: str) -> "RelativePath":
        return RelativePath(self.parts + (_validate_component(name),))

    def __str__(self) -> str:
        return "/".join(self.parts)


@dataclass(frozen=True, slots=True)
class NodeIdentity:
    """Stable object identity plus bounded diagnostic metadata."""

    backend: str
    volume: int
    file_id: str
    kind: str
    link_count: int
    size: int

    def same_object(self, other: "NodeIdentity") -> bool:
        return (
            isinstance(other, NodeIdentity)
            and self.volume == other.volume
            and self.file_id == other.file_id
            and self.kind == other.kind
        )

    def to_mapping(self) -> dict[str, object]:
        return {
            "backend": self.backend,
            "file_id": self.file_id,
            "kind": self.kind,
            "link_count": self.link_count,
            "size": self.size,
            "volume": self.volume,
        }


@dataclass(frozen=True, slots=True)
class DirectoryEntry:
    name: str
    identity: NodeIdentity

    def to_mapping(self) -> dict[str, object]:
        return {"identity": self.identity.to_mapping(), "name": self.name}


@dataclass(frozen=True, slots=True)
class DescriptorAuthorityCapability:
    backend: str
    absolute_bootstrap_mode: str
    absolute_ancestor_resolution_no_reparse_proven: bool
    ancestor_pin_mode: str
    authority_root_identity_pinned: bool
    descriptor_relative_create_read_list: bool
    directory_conflict_rejected: bool
    directory_fsync_supported: bool
    directory_publication_succeeded: bool
    file_conflict_rejected: bool
    file_publication_succeeded: bool
    reanchor_identity_proven: bool
    same_filesystem: bool

    @property
    def primitive_proven(self) -> bool:
        return (
            self.absolute_ancestor_resolution_no_reparse_proven
            and self.ancestor_pin_mode
            in {"descriptor_survived_rename", "rename_blocked_while_pinned"}
            and self.authority_root_identity_pinned
            and self.descriptor_relative_create_read_list
            and self.directory_conflict_rejected
            and self.directory_publication_succeeded
            and self.file_conflict_rejected
            and self.file_publication_succeeded
            and self.reanchor_identity_proven
            and self.same_filesystem
        )

    def to_mapping(self) -> dict[str, object]:
        return {
            "absolute_ancestor_resolution_no_reparse_proven": (
                self.absolute_ancestor_resolution_no_reparse_proven
            ),
            "absolute_bootstrap": self.absolute_bootstrap_mode,
            "absolute_bootstrap_mode": self.absolute_bootstrap_mode,
            "ancestor_pin_mode": self.ancestor_pin_mode,
            "allowed_root_bootstrap_proven": (
                self.absolute_ancestor_resolution_no_reparse_proven
                and self.authority_root_identity_pinned
            ),
            "authority_root_identity_pinned": (
                self.authority_root_identity_pinned
            ),
            "backend": self.backend,
            "contract": DESCRIPTOR_RELATIVE_AUTHORITY_CONTRACT,
            "descriptor_relative_create_read_list": (
                self.descriptor_relative_create_read_list
            ),
            "directory_conflict_rejected": self.directory_conflict_rejected,
            "directory_fsync_supported": self.directory_fsync_supported,
            "directory_publication_succeeded": (
                self.directory_publication_succeeded
            ),
            "directory_listing_identity_binding": (
                "reopen_each_name_beneath_pinned_parent"
            ),
            "hostile_same_user_name_races_excluded": False,
            "file_conflict_rejected": self.file_conflict_rejected,
            "file_publication_succeeded": self.file_publication_succeeded,
            "final_output_race_exclusion_simulator_proven": False,
            "local_acceptance_blockers_closed": [],
            "primitive_proven": self.primitive_proven,
            "reanchor_identity_proven": self.reanchor_identity_proven,
            "same_filesystem": self.same_filesystem,
            "simulator_integration_proven": False,
            "unopened_ancestors_pinned": False,
        }


def _posix_identity(descriptor: int, backend: str) -> NodeIdentity:
    try:
        details = os.fstat(descriptor)
    except OSError as error:
        raise AuthorityIOError("descriptor_identity_query_failed") from error
    if stat.S_ISDIR(details.st_mode):
        kind = "directory"
    elif stat.S_ISREG(details.st_mode):
        kind = "regular_file"
    else:
        raise AuthorityViolation("descriptor_object_kind_forbidden")
    return NodeIdentity(
        backend=backend,
        volume=int(details.st_dev),
        file_id=f"{int(details.st_ino):x}",
        kind=kind,
        link_count=int(details.st_nlink),
        size=int(details.st_size),
    )


def _ensure_expected_identity(
    actual: NodeIdentity,
    expected: NodeIdentity | None,
    label: str,
) -> None:
    if expected is not None and not actual.same_object(expected):
        raise AuthorityViolation(f"{label}_identity_changed")


def _ensure_unaliased_regular_file(identity: NodeIdentity) -> None:
    if identity.kind == "regular_file" and identity.link_count != 1:
        raise AuthorityViolation("regular_file_hardlink_alias_forbidden")


_OPEN_HOW_FLAGS: Final = 0
_RESOLVE_NO_XDEV: Final = 0x01
_RESOLVE_NO_MAGICLINKS: Final = 0x02
_RESOLVE_NO_SYMLINKS: Final = 0x04
_RESOLVE_BENEATH: Final = 0x08
_OPENAT2_UNSUPPORTED_ERRNOS: Final = frozenset(
    {
        errno.ENOSYS,
        errno.EINVAL,
        getattr(errno, "EOPNOTSUPP", errno.EINVAL),
    }
)


class _OpenHow(ctypes.Structure):
    _fields_ = [
        ("flags", ctypes.c_uint64),
        ("mode", ctypes.c_uint64),
        ("resolve", ctypes.c_uint64),
    ]


def _openat2_syscall_number() -> int | None:
    if platform.system() != "Linux":
        return None
    machine = platform.machine().lower()
    if machine in {
        "x86_64",
        "amd64",
        "aarch64",
        "arm64",
        "riscv64",
        "ppc64",
        "ppc64le",
        "s390x",
        "i386",
        "i686",
    }:
        return 437
    return None


def _linux_openat2(
    parent_descriptor: int,
    name: str,
    flags: int,
    *,
    no_xdev: bool,
) -> int:
    syscall_number = _openat2_syscall_number()
    if syscall_number is None:
        raise AuthorityUnavailable("openat2_architecture_unsupported")
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        syscall = libc.syscall
    except (AttributeError, OSError) as error:
        raise AuthorityUnavailable("openat2_syscall_unavailable") from error
    syscall.argtypes = (
        ctypes.c_long,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.POINTER(_OpenHow),
        ctypes.c_size_t,
    )
    syscall.restype = ctypes.c_long
    resolve = _RESOLVE_BENEATH | _RESOLVE_NO_SYMLINKS | _RESOLVE_NO_MAGICLINKS
    if no_xdev:
        resolve |= _RESOLVE_NO_XDEV
    how = _OpenHow(flags=flags, mode=_OPEN_HOW_FLAGS, resolve=resolve)
    for _attempt in range(OPENAT2_RETRIES):
        result = syscall(
            syscall_number,
            parent_descriptor,
            os.fsencode(name),
            ctypes.byref(how),
            ctypes.sizeof(how),
        )
        if result >= 0:
            return int(result)
        code = ctypes.get_errno()
        if code == errno.EAGAIN:
            continue
        if code in _OPENAT2_UNSUPPORTED_ERRNOS:
            raise AuthorityUnavailable("openat2_not_supported")
        if code in {errno.ENOENT, errno.ENOTDIR}:
            raise AuthorityNotFound("authority_relative_object_not_found")
        if code in {errno.ELOOP, errno.EXDEV}:
            raise AuthorityViolation("authority_resolution_forbidden")
        raise AuthorityIOError(f"openat2_failed_errno_{code}")
    raise AuthorityViolation("openat2_resolution_retry_limit")


def _select_posix_backend(descriptor: int) -> str:
    if platform.system() != "Linux":
        return "posix_openat"
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        probe = _linux_openat2(descriptor, ".", flags, no_xdev=False)
    except AuthorityUnavailable:
        return "posix_openat"
    except AuthorityError:
        # A supported kernel should accept the already-open directory itself.
        raise
    else:
        os.close(probe)
        return "linux_openat2"


def _posix_open_child(
    parent_descriptor: int,
    name: str,
    *,
    backend: str,
    directory: bool | None,
    writable: bool = False,
    create: bool = False,
    mode: int = 0o600,
    exclusive: bool = False,
    no_xdev: bool = True,
) -> int:
    if not hasattr(os, "O_NOFOLLOW"):
        raise AuthorityUnavailable("posix_o_nofollow_unavailable")
    if not hasattr(os, "O_CLOEXEC"):
        raise AuthorityUnavailable("posix_o_cloexec_unavailable")
    if writable:
        flags = os.O_RDWR
    else:
        flags = os.O_RDONLY
    if directory is True:
        flags |= getattr(os, "O_DIRECTORY", 0)
    else:
        # Opening an unexpected FIFO must never block the bounded authority.
        flags |= getattr(os, "O_NONBLOCK", 0)
    flags |= os.O_CLOEXEC | os.O_NOFOLLOW
    if create:
        flags |= os.O_CREAT
    if exclusive:
        flags |= os.O_EXCL
    try:
        if backend == "linux_openat2" and not create:
            return _linux_openat2(
                parent_descriptor,
                name,
                flags,
                no_xdev=no_xdev,
            )
        if os.open not in os.supports_dir_fd:
            raise AuthorityUnavailable("posix_dir_fd_open_unavailable")
        return os.open(name, flags, mode, dir_fd=parent_descriptor)
    except FileExistsError as error:
        raise AuthorityConflict("authority_destination_exists") from error
    except FileNotFoundError as error:
        raise AuthorityNotFound("authority_relative_object_not_found") from error
    except IsADirectoryError as error:
        raise AuthorityViolation("authority_expected_regular_file") from error
    except NotADirectoryError as error:
        raise AuthorityViolation("authority_expected_directory") from error
    except AuthorityError:
        raise
    except OSError as error:
        if error.errno in {errno.ELOOP, getattr(errno, "EMLINK", errno.ELOOP)}:
            raise AuthorityViolation("authority_link_forbidden") from error
        if error.errno == errno.EXDEV:
            raise AuthorityViolation("authority_cross_filesystem_forbidden") from error
        raise AuthorityIOError(
            f"authority_relative_open_failed_errno_{error.errno}"
        ) from error


def _posix_publish_noreplace(
    source_parent: int,
    source_name: str,
    destination_parent: int,
    destination_name: str,
) -> str:
    system = platform.system()
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        if system == "Linux":
            function = libc.renameat2
            flag = RENAME_NOREPLACE
            backend = "linux_renameat2_noreplace"
        elif system == "Darwin":
            function = libc.renameatx_np
            flag = RENAME_EXCL
            backend = "darwin_renameatx_np_excl"
        else:
            raise AuthorityUnavailable("atomic_noreplace_platform_unsupported")
    except AttributeError as error:
        raise AuthorityUnavailable("atomic_noreplace_function_unavailable") from error
    function.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    function.restype = ctypes.c_int
    if function(
        source_parent,
        os.fsencode(source_name),
        destination_parent,
        os.fsencode(destination_name),
        flag,
    ) == 0:
        return backend
    code = ctypes.get_errno()
    if code in {errno.EEXIST, errno.ENOTEMPTY}:
        raise AuthorityConflict("authority_destination_exists")
    if code in {
        errno.ENOSYS,
        errno.EINVAL,
        getattr(errno, "ENOTSUP", errno.EINVAL),
        getattr(errno, "EOPNOTSUPP", errno.EINVAL),
    }:
        raise AuthorityUnavailable("atomic_noreplace_filesystem_unsupported")
    if code == errno.EXDEV:
        raise AuthorityUnavailable("atomic_noreplace_cross_filesystem")
    raise AuthorityIOError(f"atomic_noreplace_failed_errno_{code}")


# Windows declarations are guarded so importing this module remains portable.
if os.name == "nt":  # pragma: no branch - definitions are platform-specific
    from ctypes import wintypes as _wt

    _HANDLE = _wt.HANDLE
    _ULONG_PTR = ctypes.c_size_t

    class _UNICODE_STRING(ctypes.Structure):
        _fields_ = [
            ("Length", _wt.USHORT),
            ("MaximumLength", _wt.USHORT),
            ("Buffer", _wt.LPWSTR),
        ]

    class _OBJECT_ATTRIBUTES(ctypes.Structure):
        _fields_ = [
            ("Length", _wt.ULONG),
            ("RootDirectory", _HANDLE),
            ("ObjectName", ctypes.POINTER(_UNICODE_STRING)),
            ("Attributes", _wt.ULONG),
            ("SecurityDescriptor", ctypes.c_void_p),
            ("SecurityQualityOfService", ctypes.c_void_p),
        ]

    class _IO_STATUS_UNION(ctypes.Union):
        _fields_ = [("Status", _wt.LONG), ("Pointer", ctypes.c_void_p)]

    class _IO_STATUS_BLOCK(ctypes.Structure):
        _anonymous_ = ("result",)
        _fields_ = [("result", _IO_STATUS_UNION), ("Information", _ULONG_PTR)]

    class _FILE_ID_128(ctypes.Structure):
        _fields_ = [("Identifier", ctypes.c_ubyte * 16)]

    class _FILE_ID_INFO(ctypes.Structure):
        _fields_ = [
            ("VolumeSerialNumber", ctypes.c_uint64),
            ("FileId", _FILE_ID_128),
        ]

    class _FILE_STANDARD_INFO(ctypes.Structure):
        _fields_ = [
            ("AllocationSize", ctypes.c_int64),
            ("EndOfFile", ctypes.c_int64),
            ("NumberOfLinks", _wt.DWORD),
            ("DeletePending", ctypes.c_ubyte),
            ("Directory", ctypes.c_ubyte),
        ]

    class _FILE_ATTRIBUTE_TAG_INFO(ctypes.Structure):
        _fields_ = [("FileAttributes", _wt.DWORD), ("ReparseTag", _wt.DWORD)]

    class _FILE_ID_BOTH_DIR_INFO(ctypes.Structure):
        _fields_ = [
            ("NextEntryOffset", _wt.DWORD),
            ("FileIndex", _wt.DWORD),
            ("CreationTime", ctypes.c_int64),
            ("LastAccessTime", ctypes.c_int64),
            ("LastWriteTime", ctypes.c_int64),
            ("ChangeTime", ctypes.c_int64),
            ("EndOfFile", ctypes.c_int64),
            ("AllocationSize", ctypes.c_int64),
            ("FileAttributes", _wt.DWORD),
            ("FileNameLength", _wt.DWORD),
            ("EaSize", _wt.DWORD),
            ("ShortNameLength", ctypes.c_byte),
            ("ShortName", _wt.WCHAR * 12),
            ("FileId", ctypes.c_int64),
            ("FileName", _wt.WCHAR * 1),
        ]

    class _FILE_RENAME_UNION(ctypes.Union):
        _fields_ = [("ReplaceIfExists", ctypes.c_ubyte), ("Flags", _wt.DWORD)]

    class _FILE_RENAME_INFO(ctypes.Structure):
        _anonymous_ = ("replace",)
        _fields_ = [
            ("replace", _FILE_RENAME_UNION),
            ("RootDirectory", _HANDLE),
            ("FileNameLength", _wt.DWORD),
            ("FileName", _wt.WCHAR * 1),
        ]

    class _FILE_DISPOSITION_INFO(ctypes.Structure):
        _fields_ = [("DeleteFile", ctypes.c_ubyte)]

    if (
        _FILE_STANDARD_INFO.DeletePending.offset != 20
        or _FILE_STANDARD_INFO.Directory.offset != 21
        or ctypes.sizeof(_FILE_STANDARD_INFO) != 24
        or ctypes.sizeof(_FILE_DISPOSITION_INFO) != 1
        or _FILE_RENAME_INFO.ReplaceIfExists.offset != 0
        or _FILE_RENAME_INFO.RootDirectory.offset != 8
        or _FILE_RENAME_INFO.FileNameLength.offset != 16
        or _FILE_RENAME_INFO.FileName.offset != 20
        or ctypes.sizeof(_FILE_RENAME_INFO) != 24
    ):
        raise RuntimeError("Windows FILE_*_INFO ABI layout mismatch")

    _KERNEL32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _NTDLL = ctypes.WinDLL("ntdll", use_last_error=True)

    _CloseHandle = _KERNEL32.CloseHandle
    _CloseHandle.argtypes = (_HANDLE,)
    _CloseHandle.restype = _wt.BOOL

    _DuplicateHandle = _KERNEL32.DuplicateHandle
    _DuplicateHandle.argtypes = (
        _HANDLE,
        _HANDLE,
        _HANDLE,
        ctypes.POINTER(_HANDLE),
        _wt.DWORD,
        _wt.BOOL,
        _wt.DWORD,
    )
    _DuplicateHandle.restype = _wt.BOOL

    _GetCurrentProcess = _KERNEL32.GetCurrentProcess
    _GetCurrentProcess.argtypes = ()
    _GetCurrentProcess.restype = _HANDLE

    _GetFileInformationByHandleEx = _KERNEL32.GetFileInformationByHandleEx
    _GetFileInformationByHandleEx.argtypes = (
        _HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        _wt.DWORD,
    )
    _GetFileInformationByHandleEx.restype = _wt.BOOL

    _SetFileInformationByHandle = _KERNEL32.SetFileInformationByHandle
    _SetFileInformationByHandle.argtypes = (
        _HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        _wt.DWORD,
    )
    _SetFileInformationByHandle.restype = _wt.BOOL

    _FlushFileBuffers = _KERNEL32.FlushFileBuffers
    _FlushFileBuffers.argtypes = (_HANDLE,)
    _FlushFileBuffers.restype = _wt.BOOL

    _ReadFile = _KERNEL32.ReadFile
    _ReadFile.argtypes = (
        _HANDLE,
        ctypes.c_void_p,
        _wt.DWORD,
        ctypes.POINTER(_wt.DWORD),
        ctypes.c_void_p,
    )
    _ReadFile.restype = _wt.BOOL

    _WriteFile = _KERNEL32.WriteFile
    _WriteFile.argtypes = (
        _HANDLE,
        ctypes.c_void_p,
        _wt.DWORD,
        ctypes.POINTER(_wt.DWORD),
        ctypes.c_void_p,
    )
    _WriteFile.restype = _wt.BOOL

    _SetFilePointerEx = _KERNEL32.SetFilePointerEx
    _SetFilePointerEx.argtypes = (
        _HANDLE,
        ctypes.c_int64,
        ctypes.POINTER(ctypes.c_int64),
        _wt.DWORD,
    )
    _SetFilePointerEx.restype = _wt.BOOL

    _NtCreateFile = _NTDLL.NtCreateFile
    _NtCreateFile.argtypes = (
        ctypes.POINTER(_HANDLE),
        _wt.DWORD,
        ctypes.POINTER(_OBJECT_ATTRIBUTES),
        ctypes.POINTER(_IO_STATUS_BLOCK),
        ctypes.c_void_p,
        _wt.ULONG,
        _wt.ULONG,
        _wt.ULONG,
        _wt.ULONG,
        ctypes.c_void_p,
        _wt.ULONG,
    )
    _NtCreateFile.restype = _wt.LONG

    _NtSetInformationFile = _NTDLL.NtSetInformationFile
    _NtSetInformationFile.argtypes = (
        _HANDLE,
        ctypes.POINTER(_IO_STATUS_BLOCK),
        ctypes.c_void_p,
        _wt.ULONG,
        ctypes.c_int,
    )
    _NtSetInformationFile.restype = _wt.LONG

    _RtlNtStatusToDosError = _NTDLL.RtlNtStatusToDosError
    _RtlNtStatusToDosError.argtypes = (_wt.LONG,)
    _RtlNtStatusToDosError.restype = _wt.ULONG

    _OBJ_CASE_INSENSITIVE = 0x00000040
    _OBJ_DONT_REPARSE = 0x00001000
    _FILE_LIST_DIRECTORY = 0x00000001
    _FILE_READ_DATA = 0x00000001
    _FILE_WRITE_DATA = 0x00000002
    _FILE_ADD_FILE = 0x00000002
    _FILE_ADD_SUBDIRECTORY = 0x00000004
    _FILE_TRAVERSE = 0x00000020
    _FILE_READ_ATTRIBUTES = 0x00000080
    _FILE_WRITE_ATTRIBUTES = 0x00000100
    _DELETE = 0x00010000
    _SYNCHRONIZE = 0x00100000
    _FILE_SHARE_READ = 0x00000001
    _FILE_SHARE_WRITE = 0x00000002
    _FILE_SHARE_DELETE = 0x00000004
    _FILE_SUPERSEDE = 0
    _FILE_OPEN = 1
    _FILE_CREATE = 2
    _FILE_DIRECTORY_FILE = 0x00000001
    _FILE_SYNCHRONOUS_IO_NONALERT = 0x00000020
    _FILE_NON_DIRECTORY_FILE = 0x00000040
    _FILE_OPEN_FOR_BACKUP_INTENT = 0x00004000
    _FILE_OPEN_REPARSE_POINT = 0x00200000
    _FILE_ATTRIBUTE_NORMAL = 0x00000080
    _FILE_ATTRIBUTE_DIRECTORY = 0x00000010
    _FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
    _FILE_ID_BOTH_DIRECTORY_INFO_CLASS = 10
    _FILE_ID_BOTH_DIRECTORY_RESTART_INFO_CLASS = 11
    _FILE_RENAME_INFO_CLASS = 3
    _NATIVE_FILE_RENAME_INFORMATION_CLASS = 10
    _FILE_DISPOSITION_INFO_CLASS = 4
    _FILE_ATTRIBUTE_TAG_INFO_CLASS = 9
    _FILE_ID_INFO_CLASS = 18
    _FILE_STANDARD_INFO_CLASS = 1
    _ERROR_NO_MORE_FILES = 18
    _ERROR_FILE_EXISTS = 80
    _ERROR_ALREADY_EXISTS = 183
    _ERROR_SHARING_VIOLATION = 32
    _ERROR_ACCESS_DENIED = 5
    _ERROR_NOT_SUPPORTED = 50
    _ERROR_INVALID_FUNCTION = 1
    _ERROR_INVALID_PARAMETER = 87
    _ERROR_FILE_NOT_FOUND = 2
    _ERROR_PATH_NOT_FOUND = 3
    _DUPLICATE_SAME_ACCESS = 0x00000002
    _FILE_BEGIN = 0


def _win_handle_value(handle: object) -> int:
    value = getattr(handle, "value", handle)
    if value is None:
        return 0
    return int(value)


def _win_close(handle: int) -> None:
    if os.name != "nt" or not handle:
        return
    if not _CloseHandle(_HANDLE(handle)):
        code = ctypes.get_last_error()
        raise AuthorityIOError(f"close_handle_failed_winerror_{code}")


def _win_duplicate(handle: int) -> int:
    target = _HANDLE()
    process = _GetCurrentProcess()
    if not _DuplicateHandle(
        process,
        _HANDLE(handle),
        process,
        ctypes.byref(target),
        0,
        False,
        _DUPLICATE_SAME_ACCESS,
    ):
        code = ctypes.get_last_error()
        raise AuthorityIOError(f"duplicate_handle_failed_winerror_{code}")
    return _win_handle_value(target)


def _win_error_from_ntstatus(status: int) -> int:
    signed_status = ctypes.c_int32(int(status) & 0xFFFFFFFF).value
    return int(_RtlNtStatusToDosError(signed_status))


def _classify_windows_nt_open_status(status: int, label: str) -> None:
    unsigned_status = int(status) & 0xFFFFFFFF
    if unsigned_status == _STATUS_REPARSE_POINT_ENCOUNTERED:
        raise AuthorityViolation(f"{label}_reparse_point_encountered")
    _classify_windows_open_error(
        _win_error_from_ntstatus(unsigned_status),
        label,
    )


def _classify_windows_open_error(code: int, label: str) -> None:
    if code in {_ERROR_FILE_EXISTS, _ERROR_ALREADY_EXISTS}:
        raise AuthorityConflict(f"{label}_destination_exists")
    if code in {_ERROR_FILE_NOT_FOUND, _ERROR_PATH_NOT_FOUND}:
        raise AuthorityNotFound(f"{label}_not_found")
    if code == _ERROR_SHARING_VIOLATION:
        raise AuthorityViolation(f"{label}_pinned_against_delete")
    if code in {
        _ERROR_NOT_SUPPORTED,
        _ERROR_INVALID_FUNCTION,
        _ERROR_INVALID_PARAMETER,
    }:
        raise AuthorityUnavailable(f"{label}_unsupported_winerror_{code}")
    raise AuthorityIOError(f"{label}_failed_winerror_{code}")


def _win_nt_open(
    parent_handle: int,
    name: str,
    *,
    desired_access: int,
    disposition: int,
    directory: bool | None,
    share_delete: bool = False,
) -> int:
    if os.name != "nt":
        raise AuthorityUnavailable("windows_backend_not_running")
    _validate_component(name)
    name_buffer = ctypes.create_unicode_buffer(name)
    encoded_length = len(name.encode("utf-16-le"))
    unicode_name = _UNICODE_STRING(
        Length=encoded_length,
        MaximumLength=encoded_length + 2,
        Buffer=ctypes.cast(name_buffer, _wt.LPWSTR),
    )
    attributes = _OBJECT_ATTRIBUTES(
        Length=ctypes.sizeof(_OBJECT_ATTRIBUTES),
        RootDirectory=_HANDLE(parent_handle),
        ObjectName=ctypes.pointer(unicode_name),
        Attributes=_OBJ_CASE_INSENSITIVE,
        SecurityDescriptor=None,
        SecurityQualityOfService=None,
    )
    io_status = _IO_STATUS_BLOCK()
    result_handle = _HANDLE()
    options = (
        _FILE_SYNCHRONOUS_IO_NONALERT
        | _FILE_OPEN_REPARSE_POINT
        | _FILE_OPEN_FOR_BACKUP_INTENT
    )
    attributes_bits = _FILE_ATTRIBUTE_NORMAL
    if directory is True:
        options |= _FILE_DIRECTORY_FILE
        attributes_bits = _FILE_ATTRIBUTE_DIRECTORY
    elif directory is False:
        options |= _FILE_NON_DIRECTORY_FILE
    share = _FILE_SHARE_READ | _FILE_SHARE_WRITE
    if share_delete:
        share |= _FILE_SHARE_DELETE
    status = int(
        _NtCreateFile(
            ctypes.byref(result_handle),
            desired_access,
            ctypes.byref(attributes),
            ctypes.byref(io_status),
            None,
            attributes_bits,
            share,
            disposition,
            options,
            None,
            0,
        )
    )
    if status & 0x80000000:
        failed_handle = _win_handle_value(result_handle)
        if failed_handle:
            _win_close(failed_handle)
        _classify_windows_nt_open_status(status, "ntcreatefile")
    handle = _win_handle_value(result_handle)
    if not handle:
        raise AuthorityIOError("ntcreatefile_returned_null_handle")
    try:
        _win_identity(handle, "windows_ntcreatefile")
    except BaseException:
        _win_close(handle)
        raise
    return handle


def _win_query(handle: int, info_class: int, value: ctypes.Structure) -> None:
    if not _GetFileInformationByHandleEx(
        _HANDLE(handle),
        info_class,
        ctypes.byref(value),
        ctypes.sizeof(value),
    ):
        code = ctypes.get_last_error()
        raise AuthorityIOError(f"handle_information_failed_winerror_{code}")


def _win_identity(handle: int, backend: str) -> NodeIdentity:
    if os.name != "nt":
        raise AuthorityUnavailable("windows_backend_not_running")
    attributes = _FILE_ATTRIBUTE_TAG_INFO()
    _win_query(handle, _FILE_ATTRIBUTE_TAG_INFO_CLASS, attributes)
    if int(attributes.FileAttributes) & _FILE_ATTRIBUTE_REPARSE_POINT:
        raise AuthorityViolation(
            f"windows_reparse_point_forbidden_tag_{int(attributes.ReparseTag):x}"
        )
    identifier = _FILE_ID_INFO()
    _win_query(handle, _FILE_ID_INFO_CLASS, identifier)
    standard = _FILE_STANDARD_INFO()
    _win_query(handle, _FILE_STANDARD_INFO_CLASS, standard)
    kind = "directory" if bool(standard.Directory) else "regular_file"
    file_id = bytes(identifier.FileId.Identifier).hex()
    return NodeIdentity(
        backend=backend,
        volume=int(identifier.VolumeSerialNumber),
        file_id=file_id,
        kind=kind,
        link_count=int(standard.NumberOfLinks),
        size=int(standard.EndOfFile),
    )


def _win_open_absolute_no_reparse(
    path: Path | str | os.PathLike[str],
) -> int:
    """Open a full NT path once, rejecting reparses anywhere in resolution."""

    if os.name != "nt":
        raise AuthorityUnavailable("windows_backend_not_running")
    nt_path = _win_absolute_nt_path(path)
    name_buffer = ctypes.create_unicode_buffer(nt_path)
    encoded_length = len(nt_path.encode("utf-16-le", "strict"))
    unicode_name = _UNICODE_STRING(
        Length=encoded_length,
        MaximumLength=encoded_length + 2,
        Buffer=ctypes.cast(name_buffer, _wt.LPWSTR),
    )
    attributes = _OBJECT_ATTRIBUTES(
        Length=ctypes.sizeof(_OBJECT_ATTRIBUTES),
        RootDirectory=_HANDLE(),
        ObjectName=ctypes.pointer(unicode_name),
        Attributes=_OBJ_CASE_INSENSITIVE | _OBJ_DONT_REPARSE,
        SecurityDescriptor=None,
        SecurityQualityOfService=None,
    )
    io_status = _IO_STATUS_BLOCK()
    result_handle = _HANDLE()
    options = (
        _FILE_DIRECTORY_FILE
        | _FILE_SYNCHRONOUS_IO_NONALERT
        | _FILE_OPEN_FOR_BACKUP_INTENT
        | _FILE_OPEN_REPARSE_POINT
    )
    status = int(
        _NtCreateFile(
            ctypes.byref(result_handle),
            _win_directory_access(mutable=True),
            ctypes.byref(attributes),
            ctypes.byref(io_status),
            None,
            _FILE_ATTRIBUTE_DIRECTORY,
            _FILE_SHARE_READ | _FILE_SHARE_WRITE,
            _FILE_OPEN,
            options,
            None,
            0,
        )
    )
    if status & 0x80000000:
        failed_handle = _win_handle_value(result_handle)
        if failed_handle:
            _win_close(failed_handle)
        _classify_windows_nt_open_status(status, "windows_absolute_open")
    handle = _win_handle_value(result_handle)
    if not handle:
        raise AuthorityIOError("windows_absolute_open_returned_null_handle")
    try:
        identity = _win_identity(handle, "windows_ntcreatefile")
        if identity.kind != "directory":
            raise AuthorityViolation("windows_absolute_root_not_directory")
    except BaseException:
        _win_close(handle)
        raise
    return handle


def _win_directory_access(*, mutable: bool) -> int:
    access = (
        _FILE_LIST_DIRECTORY
        | _FILE_TRAVERSE
        | _FILE_READ_ATTRIBUTES
        | _SYNCHRONIZE
    )
    if mutable:
        access |= _FILE_ADD_FILE | _FILE_ADD_SUBDIRECTORY | _FILE_WRITE_ATTRIBUTES
    return access


def _win_open_directory_relative(
    parent_handle: int,
    name: str,
    *,
    mutable: bool,
) -> int:
    handle = _win_nt_open(
        parent_handle,
        name,
        desired_access=_win_directory_access(mutable=mutable),
        disposition=_FILE_OPEN,
        directory=True,
    )
    identity = _win_identity(handle, "windows_ntcreatefile")
    if identity.kind != "directory":
        _win_close(handle)
        raise AuthorityViolation("authority_expected_directory")
    return handle


def _win_list_names(handle: int, maximum_entries: int) -> tuple[str, ...]:
    buffer_size = 64 * 1024
    names: list[str] = []
    restart = True
    while True:
        buffer = ctypes.create_string_buffer(buffer_size)
        info_class = (
            _FILE_ID_BOTH_DIRECTORY_RESTART_INFO_CLASS
            if restart
            else _FILE_ID_BOTH_DIRECTORY_INFO_CLASS
        )
        restart = False
        if not _GetFileInformationByHandleEx(
            _HANDLE(handle), info_class, buffer, buffer_size
        ):
            code = ctypes.get_last_error()
            if code == _ERROR_NO_MORE_FILES:
                break
            raise AuthorityIOError(
                f"directory_enumeration_failed_winerror_{code}"
            )
        offset = 0
        while True:
            if offset + _FILE_ID_BOTH_DIR_INFO.FileName.offset > buffer_size:
                raise AuthorityViolation("directory_enumeration_record_overrun")
            record = _FILE_ID_BOTH_DIR_INFO.from_buffer(buffer, offset)
            name_bytes = int(record.FileNameLength)
            if name_bytes % 2 or name_bytes > MAX_COMPONENT_BYTES * 4:
                raise AuthorityViolation("directory_entry_name_length_invalid")
            end = offset + _FILE_ID_BOTH_DIR_INFO.FileName.offset + name_bytes
            if end > buffer_size:
                raise AuthorityViolation("directory_entry_name_overrun")
            name = ctypes.wstring_at(
                ctypes.addressof(buffer)
                + offset
                + _FILE_ID_BOTH_DIR_INFO.FileName.offset,
                name_bytes // 2,
            )
            if name not in {".", ".."}:
                _validate_component(name)
                names.append(name)
                if len(names) > maximum_entries:
                    raise AuthorityLimitExceeded("directory_entry_limit_exceeded")
            next_offset = int(record.NextEntryOffset)
            if next_offset == 0:
                break
            if next_offset < _FILE_ID_BOTH_DIR_INFO.FileName.offset:
                raise AuthorityViolation("directory_enumeration_offset_invalid")
            offset += next_offset
            if offset >= buffer_size:
                raise AuthorityViolation("directory_enumeration_offset_overrun")
    return tuple(names)


def _win_publish(
    source_handle: int,
    destination_parent_handle: int,
    destination_name: str,
) -> str:
    encoded = destination_name.encode("utf-16-le")
    name_offset = _FILE_RENAME_INFO.FileName.offset
    # Keep the native record at least one complete SDK structure wide and add
    # the entire variable name.  FileNameLength excludes the zeroed tail.
    total_size = ctypes.sizeof(_FILE_RENAME_INFO) + len(encoded)
    buffer = ctypes.create_string_buffer(total_size)
    info = _FILE_RENAME_INFO.from_buffer(buffer)
    info.ReplaceIfExists = False
    info.RootDirectory = _HANDLE(destination_parent_handle)
    info.FileNameLength = len(encoded)
    ctypes.memmove(ctypes.addressof(buffer) + name_offset, encoded, len(encoded))
    io_status = _IO_STATUS_BLOCK()
    status = int(
        _NtSetInformationFile(
            _HANDLE(source_handle),
            ctypes.byref(io_status),
            buffer,
            total_size,
            _NATIVE_FILE_RENAME_INFORMATION_CLASS,
        )
    )
    if status < 0:
        code = _win_error_from_ntstatus(status)
        if code in {_ERROR_FILE_EXISTS, _ERROR_ALREADY_EXISTS}:
            raise AuthorityConflict("authority_destination_exists")
        if code == _ERROR_SHARING_VIOLATION:
            raise AuthorityViolation("publication_source_pinned_against_delete")
        if code in {
            _ERROR_NOT_SUPPORTED,
            _ERROR_INVALID_FUNCTION,
            _ERROR_INVALID_PARAMETER,
        }:
            raise AuthorityUnavailable(
                f"windows_ntsetinformationfile_rename_unsupported_winerror_{code}"
            )
        raise AuthorityIOError(
            f"windows_ntsetinformationfile_rename_failed_winerror_{code}"
        )
    return "windows_ntsetinformationfile_noreplace"


def _win_delete_handle(handle: int) -> None:
    info = _FILE_DISPOSITION_INFO(DeleteFile=True)
    if not _SetFileInformationByHandle(
        _HANDLE(handle),
        _FILE_DISPOSITION_INFO_CLASS,
        ctypes.byref(info),
        ctypes.sizeof(info),
    ):
        code = ctypes.get_last_error()
        raise AuthorityIOError(f"windows_handle_delete_failed_winerror_{code}")


class BoundFile:
    """An owned regular-file descriptor/handle with bounded stream helpers."""

    __slots__ = ("_backend", "_native", "_writable", "_closed")

    def __init__(self, backend: str, native: int, *, writable: bool) -> None:
        self._backend = backend
        self._native = native
        self._writable = writable
        self._closed = False
        identity = self.identity()
        if identity.kind != "regular_file":
            raise AuthorityViolation("authority_expected_regular_file")
        if identity.link_count != 1:
            raise AuthorityViolation("regular_file_hardlink_alias_forbidden")

    @property
    def backend(self) -> str:
        return self._backend

    @property
    def native_handle(self) -> int:
        self._require_open()
        return self._native

    @property
    def closed(self) -> bool:
        return self._closed

    def _require_open(self) -> None:
        if self._closed:
            raise AuthorityViolation("bound_file_closed")

    def identity(self) -> NodeIdentity:
        self._require_open()
        if os.name == "nt":
            return _win_identity(self._native, self._backend)
        return _posix_identity(self._native, self._backend)

    def rewind(self) -> None:
        self._require_open()
        try:
            if os.name == "nt":
                if not _SetFilePointerEx(
                    _HANDLE(self._native), 0, None, _FILE_BEGIN
                ):
                    code = ctypes.get_last_error()
                    raise AuthorityIOError(
                        f"bound_file_seek_failed_winerror_{code}"
                    )
            else:
                os.lseek(self._native, 0, os.SEEK_SET)
        except AuthorityError:
            raise
        except OSError as error:
            raise AuthorityIOError("bound_file_seek_failed") from error

    def write_all(self, value: bytes | bytearray | memoryview) -> None:
        self._require_open()
        if not self._writable:
            raise AuthorityViolation("bound_file_not_writable")
        view = memoryview(value).cast("B")
        try:
            while view:
                chunk = view[:IO_CHUNK_BYTES]
                if os.name == "nt":
                    buffer = ctypes.create_string_buffer(chunk.tobytes())
                    written = _wt.DWORD()
                    if not _WriteFile(
                        _HANDLE(self._native),
                        buffer,
                        len(chunk),
                        ctypes.byref(written),
                        None,
                    ):
                        code = ctypes.get_last_error()
                        raise AuthorityIOError(
                            f"bound_file_write_failed_winerror_{code}"
                        )
                    count = int(written.value)
                else:
                    count = os.write(self._native, chunk)
                if count <= 0:
                    raise AuthorityIOError("bound_file_write_stalled")
                view = view[count:]
        except AuthorityError:
            raise
        except (OSError, ValueError) as error:
            raise AuthorityIOError("bound_file_write_failed") from error

    def read_bounded(self, maximum_bytes: int) -> bytes:
        self._require_open()
        if type(maximum_bytes) is not int or maximum_bytes < 0:
            raise TypeError("maximum_bytes must be a non-negative integer")
        if maximum_bytes > MAX_READ_BYTES:
            raise AuthorityLimitExceeded("bound_file_read_limit_too_large")
        self.rewind()
        chunks: list[bytes] = []
        total = 0
        try:
            while True:
                request = min(IO_CHUNK_BYTES, maximum_bytes - total + 1)
                if request <= 0:
                    raise AuthorityLimitExceeded("bound_file_read_limit_exceeded")
                if os.name == "nt":
                    buffer = ctypes.create_string_buffer(request)
                    received = _wt.DWORD()
                    if not _ReadFile(
                        _HANDLE(self._native),
                        buffer,
                        request,
                        ctypes.byref(received),
                        None,
                    ):
                        code = ctypes.get_last_error()
                        raise AuthorityIOError(
                            f"bound_file_read_failed_winerror_{code}"
                        )
                    chunk = buffer.raw[: int(received.value)]
                else:
                    chunk = os.read(self._native, request)
                if not chunk:
                    return b"".join(chunks)
                total += len(chunk)
                if total > maximum_bytes:
                    raise AuthorityLimitExceeded("bound_file_read_limit_exceeded")
                chunks.append(chunk)
        except AuthorityError:
            raise
        except OSError as error:
            raise AuthorityIOError("bound_file_read_failed") from error

    def flush(self) -> None:
        self._require_open()
        try:
            if os.name == "nt":
                if not _FlushFileBuffers(_HANDLE(self._native)):
                    code = ctypes.get_last_error()
                    raise AuthorityIOError(
                        f"bound_file_flush_failed_winerror_{code}"
                    )
            else:
                os.fsync(self._native)
        except AuthorityError:
            raise
        except OSError as error:
            raise AuthorityIOError("bound_file_flush_failed") from error

    def duplicate_binary(self, mode: str = "rb") -> BinaryIO:
        self._require_open()
        if mode not in {"rb", "r+b"}:
            raise ValueError("duplicate_binary mode must be 'rb' or 'r+b'")
        if mode == "r+b" and not self._writable:
            raise AuthorityViolation("bound_file_not_writable")
        self.rewind()
        duplicate_native: int | None = None
        descriptor: int | None = None
        try:
            if os.name == "nt":
                import msvcrt

                duplicate_native = _win_duplicate(self._native)
                flags = os.O_BINARY | (os.O_RDWR if mode == "r+b" else os.O_RDONLY)
                descriptor = msvcrt.open_osfhandle(duplicate_native, flags)
                duplicate_native = None  # ownership transferred to the CRT fd
            else:
                descriptor = os.dup(self._native)
            os.set_inheritable(descriptor, False)
            stream = os.fdopen(descriptor, mode, closefd=True)
            descriptor = None  # ownership transferred to the stream
            return stream
        except AuthorityError:
            raise
        except (OSError, ValueError) as error:
            raise AuthorityIOError("bound_file_duplicate_failed") from error
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            if duplicate_native is not None:
                try:
                    _win_close(duplicate_native)
                except AuthorityError:
                    pass

    def close(self) -> None:
        if self._closed:
            return
        native = self._native
        self._closed = True
        self._native = -1
        if os.name == "nt":
            _win_close(native)
        else:
            try:
                os.close(native)
            except OSError as error:
                raise AuthorityIOError("bound_file_close_failed") from error

    def __enter__(self) -> "BoundFile":
        self._require_open()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


class DirectoryAuthority:
    """An owned, identity-pinned directory authority."""

    __slots__ = (
        "_absolute_bootstrap",
        "_absolute_bootstrap_mode",
        "_backend",
        "_closed",
        "_handles",
        "_anchor_path",
        "_opened_identity",
        "_same_filesystem",
        "_scope_volume",
    )

    def __init__(
        self,
        *,
        backend: str,
        handles: tuple[int, ...],
        anchor_path: Path | None,
        same_filesystem: bool,
        absolute_bootstrap: str | None = None,
        absolute_bootstrap_mode: str | None = None,
    ) -> None:
        if not handles:
            raise ValueError("directory authority needs an owned handle")
        self._backend = backend
        self._absolute_bootstrap = absolute_bootstrap
        if absolute_bootstrap_mode is None:
            absolute_bootstrap_mode = (
                "nt_absolute_obj_dont_reparse"
                if backend == "windows_ntcreatefile"
                else "filesystem_root_descriptor_walk_no_symlinks"
            )
        if absolute_bootstrap_mode not in {
            "filesystem_root_descriptor_walk_no_symlinks",
            "nt_absolute_obj_dont_reparse",
        }:
            raise ValueError("unknown absolute bootstrap mode")
        self._absolute_bootstrap_mode = absolute_bootstrap_mode
        self._handles = handles
        self._anchor_path = anchor_path
        self._same_filesystem = bool(same_filesystem)
        self._closed = False
        identity = self.identity()
        if identity.kind != "directory":
            raise AuthorityViolation("authority_root_not_directory")
        self._opened_identity = identity
        self._scope_volume = identity.volume

    @staticmethod
    def _construct_owned(
        *,
        backend: str,
        handles: tuple[int, ...],
        anchor_path: Path | None,
        same_filesystem: bool,
        absolute_bootstrap: str | None = None,
        absolute_bootstrap_mode: str | None = None,
    ) -> "DirectoryAuthority":
        """Construct and close every supplied owned handle on any failure."""

        try:
            return DirectoryAuthority(
                backend=backend,
                handles=handles,
                anchor_path=anchor_path,
                same_filesystem=same_filesystem,
                absolute_bootstrap=absolute_bootstrap,
                absolute_bootstrap_mode=absolute_bootstrap_mode,
            )
        except BaseException:
            for handle in reversed(handles):
                try:
                    if os.name == "nt":
                        _win_close(handle)
                    else:
                        os.close(handle)
                except (AuthorityError, OSError):
                    pass
            raise

    @classmethod
    def open_absolute(
        cls,
        path: Path | str | os.PathLike[str],
        *,
        same_filesystem: bool = True,
    ) -> "DirectoryAuthority":
        raw = os.fspath(path)
        if type(raw) is not str or "\x00" in raw:
            raise AuthorityViolation("authority_root_must_be_absolute")
        if os.name == "nt":
            _win_absolute_nt_path(raw)
            lexical = Path(raw)
            handle: int | None = None
            try:
                handle = _win_open_absolute_no_reparse(raw)
                owned_handle = handle
                handle = None
                authority = cls._construct_owned(
                    backend="windows_ntcreatefile",
                    handles=(owned_handle,),
                    anchor_path=lexical,
                    same_filesystem=same_filesystem,
                    absolute_bootstrap=str(lexical),
                    absolute_bootstrap_mode="nt_absolute_obj_dont_reparse",
                )
                return authority
            except BaseException:
                if handle is not None:
                    _win_close(handle)
                raise
        if not os.path.isabs(raw):
            raise AuthorityViolation("authority_root_must_be_absolute")
        lexical = Path(raw)
        components = lexical.parts[1:]
        RelativePath(tuple(components))
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor: int | None = None
        try:
            descriptor = os.open(lexical.anchor or "/", flags)
            os.set_inheritable(descriptor, False)
            backend = _select_posix_backend(descriptor)
            for component in components:
                child = _posix_open_child(
                    descriptor,
                    component,
                    backend=backend,
                    directory=True,
                    no_xdev=False,
                )
                os.close(descriptor)
                descriptor = child
            owned_descriptor = descriptor
            descriptor = None
            authority = cls._construct_owned(
                backend=backend,
                handles=(owned_descriptor,),
                anchor_path=lexical,
                same_filesystem=same_filesystem,
                absolute_bootstrap=str(Path(lexical.anchor or "/")),
                absolute_bootstrap_mode=(
                    "filesystem_root_descriptor_walk_no_symlinks"
                ),
            )
            return authority
        except AuthorityError:
            if descriptor is not None:
                os.close(descriptor)
            raise
        except OSError as error:
            if descriptor is not None:
                os.close(descriptor)
            raise AuthorityIOError("authority_absolute_open_failed") from error

    @property
    def backend(self) -> str:
        return self._backend

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def absolute_bootstrap(self) -> str | None:
        self._require_open()
        return self._absolute_bootstrap

    @property
    def absolute_bootstrap_mode(self) -> str:
        self._require_open()
        return self._absolute_bootstrap_mode

    @property
    def absolute_ancestor_resolution_no_reparse_proven(self) -> bool:
        self._require_open()
        return self._absolute_bootstrap_mode in {
            "filesystem_root_descriptor_walk_no_symlinks",
            "nt_absolute_obj_dont_reparse",
        }

    @property
    def authority_root_identity_pinned(self) -> bool:
        self._require_open()
        return self.identity().same_object(self._opened_identity)

    @property
    def native_directory_handle(self) -> int:
        self._require_open()
        return self._handles[-1]

    def _require_open(self) -> None:
        if self._closed:
            raise AuthorityViolation("directory_authority_closed")

    def _clone_handles(self) -> tuple[int, ...]:
        self._require_open()
        if os.name == "nt":
            duplicates: list[int] = []
            try:
                for handle in self._handles:
                    duplicates.append(_win_duplicate(handle))
            except BaseException:
                for handle in reversed(duplicates):
                    _win_close(handle)
                raise
            return tuple(duplicates)
        try:
            duplicate = os.dup(self._handles[-1])
            os.set_inheritable(duplicate, False)
            return (duplicate,)
        except OSError as error:
            raise AuthorityIOError("directory_authority_duplicate_failed") from error

    def identity(self) -> NodeIdentity:
        self._require_open()
        if os.name == "nt":
            return _win_identity(self._handles[-1], self._backend)
        return _posix_identity(self._handles[-1], self._backend)

    @property
    def opened_identity(self) -> NodeIdentity:
        self._require_open()
        return self._opened_identity

    def _check_volume(self, identity: NodeIdentity) -> None:
        if self._same_filesystem and identity.volume != self._scope_volume:
            raise AuthorityViolation("authority_cross_filesystem_forbidden")

    def duplicate(self) -> "DirectoryAuthority":
        return self._construct_owned(
            backend=self._backend,
            handles=self._clone_handles(),
            anchor_path=self._anchor_path,
            same_filesystem=self._same_filesystem,
            absolute_bootstrap=self._absolute_bootstrap,
            absolute_bootstrap_mode=self._absolute_bootstrap_mode,
        )

    def open_directory(
        self,
        relative: RelativePath | str | os.PathLike[str] | Iterable[str] = "",
    ) -> "DirectoryAuthority":
        path = RelativePath.parse(relative)
        if not path.parts:
            return self.duplicate()
        if os.name == "nt":
            handles = list(self._clone_handles())
            authority: DirectoryAuthority | None = None
            try:
                for index, component in enumerate(path.parts):
                    handles.append(
                        _win_open_directory_relative(
                            handles[-1],
                            component,
                            mutable=index == len(path.parts) - 1,
                        )
                    )
                owned_handles = tuple(handles)
                handles = []
                authority = self._construct_owned(
                    backend=self._backend,
                    handles=owned_handles,
                    anchor_path=(
                        self._anchor_path.joinpath(*path.parts)
                        if self._anchor_path is not None
                        else None
                    ),
                    same_filesystem=self._same_filesystem,
                    absolute_bootstrap=self._absolute_bootstrap,
                    absolute_bootstrap_mode=self._absolute_bootstrap_mode,
                )
                try:
                    self._check_volume(authority.identity())
                except BaseException:
                    try:
                        authority.close()
                    finally:
                        authority = None
                    raise
                return authority
            except BaseException:
                for handle in reversed(handles):
                    _win_close(handle)
                raise
        descriptor = self._clone_handles()[0]
        authority = None
        try:
            for component in path.parts:
                child = _posix_open_child(
                    descriptor,
                    component,
                    backend=self._backend,
                    directory=True,
                    no_xdev=self._same_filesystem,
                )
                os.close(descriptor)
                descriptor = child
            owned_descriptor = descriptor
            descriptor = -1
            authority = self._construct_owned(
                backend=self._backend,
                handles=(owned_descriptor,),
                anchor_path=(
                    self._anchor_path.joinpath(*path.parts)
                    if self._anchor_path is not None
                    else None
                ),
                same_filesystem=self._same_filesystem,
                absolute_bootstrap=self._absolute_bootstrap,
                absolute_bootstrap_mode=self._absolute_bootstrap_mode,
            )
            try:
                self._check_volume(authority.identity())
            except BaseException:
                try:
                    authority.close()
                finally:
                    authority = None
                raise
            return authority
        except BaseException:
            if descriptor >= 0:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            raise

    def _parent_and_leaf(
        self,
        relative: RelativePath | str | os.PathLike[str] | Iterable[str],
    ) -> tuple["DirectoryAuthority", str]:
        path = RelativePath.parse(relative)
        if not path.parts:
            raise AuthorityViolation("operation_requires_relative_leaf")
        return self.open_directory(path.parent), path.leaf

    def create_directory(
        self,
        relative: RelativePath | str | os.PathLike[str] | Iterable[str],
        *,
        mode: int = 0o700,
    ) -> "DirectoryAuthority":
        if type(mode) is not int or mode < 0 or mode > 0o777:
            raise TypeError("directory mode must be an integer from 0 through 0o777")
        path = RelativePath.parse(relative)
        parent, leaf = self._parent_and_leaf(path)
        created_handle: int | None = None
        created_descriptor: int | None = None
        created_identity: NodeIdentity | None = None
        created = False
        authority: DirectoryAuthority | None = None
        try:
            if os.name == "nt":
                created_handle = _win_nt_open(
                    parent.native_directory_handle,
                    leaf,
                    desired_access=_win_directory_access(mutable=True),
                    disposition=_FILE_CREATE,
                    directory=True,
                )
                created = True
                created_identity = _win_identity(created_handle, self._backend)
                handles: list[int] = []
                handles.extend(parent._clone_handles())
                handles.append(created_handle)
                created_handle = None
                owned_handles = tuple(handles)
                handles = []
                authority = self._construct_owned(
                    backend=self._backend,
                    handles=owned_handles,
                    anchor_path=None,
                    same_filesystem=self._same_filesystem,
                    absolute_bootstrap=self._absolute_bootstrap,
                    absolute_bootstrap_mode=self._absolute_bootstrap_mode,
                )
            else:
                if os.mkdir not in os.supports_dir_fd:
                    raise AuthorityUnavailable("posix_dir_fd_mkdir_unavailable")
                try:
                    os.mkdir(leaf, mode, dir_fd=parent.native_directory_handle)
                    created = True
                except FileExistsError as error:
                    raise AuthorityConflict("authority_destination_exists") from error
                except OSError as error:
                    raise AuthorityIOError("authority_directory_create_failed") from error
                created_descriptor = _posix_open_child(
                    parent.native_directory_handle,
                    leaf,
                    backend=self._backend,
                    directory=True,
                    no_xdev=self._same_filesystem,
                )
                created_identity = _posix_identity(
                    created_descriptor, self._backend
                )
                owned_descriptor = created_descriptor
                created_descriptor = None
                authority = self._construct_owned(
                    backend=self._backend,
                    handles=(owned_descriptor,),
                    anchor_path=None,
                    same_filesystem=self._same_filesystem,
                    absolute_bootstrap=self._absolute_bootstrap,
                    absolute_bootstrap_mode=self._absolute_bootstrap_mode,
                )
            try:
                self._check_volume(authority.identity())
            except BaseException:
                try:
                    authority.close()
                finally:
                    authority = None
                raise
            # Do not transfer the child handle while a parent-handle close can
            # still turn this successful kernel create into an exception.
            parent.close()
            created = False
            return authority
        except BaseException as error:
            cleanup_failures: list[BaseException] = []
            if authority is not None and not authority.closed:
                try:
                    authority.close()
                except AuthorityError as cleanup_error:
                    cleanup_failures.append(cleanup_error)
            if created_handle is not None:
                try:
                    _win_close(created_handle)
                except AuthorityError as cleanup_error:
                    cleanup_failures.append(cleanup_error)
                created_handle = None
            if created_descriptor is not None:
                try:
                    os.close(created_descriptor)
                except OSError as cleanup_error:
                    cleanup_failures.append(cleanup_error)
                created_descriptor = None
            if not parent.closed:
                try:
                    parent.close()
                except AuthorityError as cleanup_error:
                    cleanup_failures.append(cleanup_error)
            if created:
                if created_identity is None:
                    cleanup_failures.append(
                        AuthorityIOError("authority_created_identity_unavailable")
                    )
                else:
                    try:
                        self.remove_owned_tree(
                            path,
                            expected_root=created_identity,
                            maximum_entries=1,
                        )
                        try:
                            self._entry_identity_raw(path)
                        except AuthorityNotFound:
                            pass
                        else:
                            raise AuthorityIOError(
                                "authority_created_directory_still_exists"
                            )
                    except BaseException as cleanup_error:
                        cleanup_failures.append(cleanup_error)
            if cleanup_failures:
                raise AuthorityPostCreateError(
                    "authority_directory_create_cleanup_failed",
                    created_identity=created_identity,
                ) from BaseExceptionGroup(
                    "authority directory create and cleanup failures",
                    [error, *cleanup_failures],
                )
            raise error
        finally:
            if created_handle is not None:
                _win_close(created_handle)
            if created_descriptor is not None:
                os.close(created_descriptor)
            parent.close()

    def create_file(
        self,
        relative: RelativePath | str | os.PathLike[str] | Iterable[str],
        *,
        mode: int = 0o600,
    ) -> BoundFile:
        if type(mode) is not int or mode < 0 or mode > 0o777:
            raise TypeError("file mode must be an integer from 0 through 0o777")
        path = RelativePath.parse(relative)
        parent, leaf = self._parent_and_leaf(path)
        native: int | None = None
        created_identity: NodeIdentity | None = None
        created = False
        bound: BoundFile | None = None
        try:
            if os.name == "nt":
                native = _win_nt_open(
                    parent.native_directory_handle,
                    leaf,
                    desired_access=(
                        _FILE_READ_DATA
                        | _FILE_WRITE_DATA
                        | _FILE_READ_ATTRIBUTES
                        | _FILE_WRITE_ATTRIBUTES
                        | _SYNCHRONIZE
                    ),
                    disposition=_FILE_CREATE,
                    directory=False,
                )
            else:
                native = _posix_open_child(
                    parent.native_directory_handle,
                    leaf,
                    backend=self._backend,
                    directory=False,
                    writable=True,
                    create=True,
                    exclusive=True,
                    mode=mode,
                    no_xdev=self._same_filesystem,
                )
            created = True
            if os.name == "nt":
                created_identity = _win_identity(native, self._backend)
            else:
                created_identity = _posix_identity(native, self._backend)
            bound = BoundFile(self._backend, native, writable=True)
            native = None
            try:
                self._check_volume(bound.identity())
            except BaseException:
                bound.close()
                raise
            parent.close()
            created = False
            return bound
        except BaseException as error:
            cleanup_failures: list[BaseException] = []
            if bound is not None and not bound.closed:
                try:
                    bound.close()
                except AuthorityError as cleanup_error:
                    cleanup_failures.append(cleanup_error)
            if native is not None:
                try:
                    if os.name == "nt":
                        _win_close(native)
                    else:
                        os.close(native)
                except (AuthorityError, OSError) as cleanup_error:
                    cleanup_failures.append(cleanup_error)
                native = None
            if not parent.closed:
                try:
                    parent.close()
                except AuthorityError as cleanup_error:
                    cleanup_failures.append(cleanup_error)
            if created:
                if created_identity is None:
                    cleanup_failures.append(
                        AuthorityIOError("authority_created_identity_unavailable")
                    )
                else:
                    try:
                        self.remove_owned_tree(
                            path,
                            expected_root=created_identity,
                            maximum_entries=1,
                        )
                        try:
                            self._entry_identity_raw(path)
                        except AuthorityNotFound:
                            pass
                        else:
                            raise AuthorityIOError(
                                "authority_created_file_still_exists"
                            )
                    except BaseException as cleanup_error:
                        cleanup_failures.append(cleanup_error)
            if cleanup_failures:
                raise AuthorityPostCreateError(
                    "authority_file_create_cleanup_failed",
                    created_identity=created_identity,
                ) from BaseExceptionGroup(
                    "authority file create and cleanup failures",
                    [error, *cleanup_failures],
                )
            raise error
        finally:
            if native is not None:
                if os.name == "nt":
                    _win_close(native)
                else:
                    os.close(native)
            parent.close()

    def open_file(
        self,
        relative: RelativePath | str | os.PathLike[str] | Iterable[str],
        *,
        writable: bool = False,
    ) -> BoundFile:
        parent, leaf = self._parent_and_leaf(relative)
        native: int | None = None
        bound: BoundFile | None = None
        try:
            if os.name == "nt":
                desired = _FILE_READ_DATA | _FILE_READ_ATTRIBUTES | _SYNCHRONIZE
                if writable:
                    desired |= _FILE_WRITE_DATA | _FILE_WRITE_ATTRIBUTES
                native = _win_nt_open(
                    parent.native_directory_handle,
                    leaf,
                    desired_access=desired,
                    disposition=_FILE_OPEN,
                    directory=False,
                )
            else:
                native = _posix_open_child(
                    parent.native_directory_handle,
                    leaf,
                    backend=self._backend,
                    directory=False,
                    writable=writable,
                    no_xdev=self._same_filesystem,
                )
            bound = BoundFile(self._backend, native, writable=writable)
            native = None
            try:
                self._check_volume(bound.identity())
            except BaseException:
                bound.close()
                raise
            parent.close()
            return bound
        except BaseException as error:
            cleanup_failures: list[BaseException] = []
            if bound is not None and not bound.closed:
                try:
                    bound.close()
                except AuthorityError as cleanup_error:
                    cleanup_failures.append(cleanup_error)
            if native is not None:
                try:
                    if os.name == "nt":
                        _win_close(native)
                    else:
                        os.close(native)
                except (AuthorityError, OSError) as cleanup_error:
                    cleanup_failures.append(cleanup_error)
                native = None
            if not parent.closed:
                try:
                    parent.close()
                except AuthorityError as cleanup_error:
                    cleanup_failures.append(cleanup_error)
            if cleanup_failures:
                raise AuthorityIOError("authority_open_file_cleanup_failed") from (
                    BaseExceptionGroup(
                        "authority file open and cleanup failures",
                        [error, *cleanup_failures],
                    )
                )
            raise error

    def read_file(
        self,
        relative: RelativePath | str | os.PathLike[str] | Iterable[str],
        *,
        maximum_bytes: int,
    ) -> bytes:
        with self.open_file(relative) as bound:
            return bound.read_bounded(maximum_bytes)

    def _entry_identity_raw(
        self,
        relative: RelativePath | str | os.PathLike[str] | Iterable[str],
    ) -> NodeIdentity:
        parent, leaf = self._parent_and_leaf(relative)
        native: int | None = None
        try:
            if os.name == "nt":
                native = _win_nt_open(
                    parent.native_directory_handle,
                    leaf,
                    desired_access=_FILE_READ_ATTRIBUTES | _SYNCHRONIZE,
                    disposition=_FILE_OPEN,
                    directory=None,
                )
                identity = _win_identity(native, self._backend)
            else:
                native = _posix_open_child(
                    parent.native_directory_handle,
                    leaf,
                    backend=self._backend,
                    directory=None,
                    no_xdev=self._same_filesystem,
                )
                identity = _posix_identity(native, self._backend)
            self._check_volume(identity)
            return identity
        finally:
            if native is not None:
                if os.name == "nt":
                    _win_close(native)
                else:
                    os.close(native)
            parent.close()

    def entry_identity(
        self,
        relative: RelativePath | str | os.PathLike[str] | Iterable[str],
    ) -> NodeIdentity:
        identity = self._entry_identity_raw(relative)
        _ensure_unaliased_regular_file(identity)
        return identity

    def exists(
        self,
        relative: RelativePath | str | os.PathLike[str] | Iterable[str],
    ) -> bool:
        try:
            self.entry_identity(relative)
        except AuthorityNotFound:
            return False
        return True

    def list_entries(
        self,
        relative: RelativePath | str | os.PathLike[str] | Iterable[str] = "",
        *,
        maximum_entries: int = MAX_DIRECTORY_ENTRIES,
    ) -> tuple[DirectoryEntry, ...]:
        if (
            type(maximum_entries) is not int
            or maximum_entries < 0
            or maximum_entries > MAX_DIRECTORY_ENTRIES
        ):
            raise TypeError(
                f"maximum_entries must be from 0 through {MAX_DIRECTORY_ENTRIES}"
            )
        directory = self.open_directory(relative)
        try:
            if os.name == "nt":
                names = _win_list_names(
                    directory.native_directory_handle, maximum_entries
                )
            else:
                names_list: list[str] = []
                try:
                    with os.scandir(directory.native_directory_handle) as iterator:
                        for entry in iterator:
                            _validate_component(entry.name)
                            names_list.append(entry.name)
                            if len(names_list) > maximum_entries:
                                raise AuthorityLimitExceeded(
                                    "directory_entry_limit_exceeded"
                                )
                except AuthorityError:
                    raise
                except OSError as error:
                    raise AuthorityIOError("directory_enumeration_failed") from error
                names = tuple(names_list)
            entries = [
                DirectoryEntry(name, directory.entry_identity(name))
                for name in names
            ]
            entries.sort(key=lambda item: item.name)
            return tuple(entries)
        finally:
            directory.close()

    def fsync(self) -> None:
        self._require_open()
        try:
            if os.name == "nt":
                if not _FlushFileBuffers(_HANDLE(self.native_directory_handle)):
                    code = ctypes.get_last_error()
                    if code in {
                        _ERROR_ACCESS_DENIED,
                        _ERROR_INVALID_FUNCTION,
                        _ERROR_INVALID_PARAMETER,
                    }:
                        raise AuthorityUnavailable(
                            f"directory_flush_unsupported_winerror_{code}"
                        )
                    raise AuthorityIOError(
                        f"directory_flush_failed_winerror_{code}"
                    )
            else:
                os.fsync(self.native_directory_handle)
        except AuthorityError:
            raise
        except OSError as error:
            raise AuthorityIOError("directory_flush_failed") from error

    def reanchor(
        self,
        absolute_path: Path | str | os.PathLike[str] | None = None,
    ) -> NodeIdentity:
        self._require_open()
        path = Path(absolute_path) if absolute_path is not None else self._anchor_path
        if path is None:
            raise AuthorityViolation("authority_has_no_lexical_reanchor")
        candidate = DirectoryAuthority.open_absolute(
            path,
            same_filesystem=self._same_filesystem,
        )
        try:
            actual = self.identity()
            reopened = candidate.identity()
            if not actual.same_object(reopened):
                raise AuthorityViolation("authority_reanchor_identity_changed")
            return reopened
        finally:
            candidate.close()

    def publish_noreplace(
        self,
        source: RelativePath | str | os.PathLike[str] | Iterable[str],
        destination: RelativePath | str | os.PathLike[str] | Iterable[str],
        *,
        expected_source: NodeIdentity | None = None,
    ) -> str:
        source_path = RelativePath.parse(source)
        destination_path = RelativePath.parse(destination)
        source_parent, source_leaf = self._parent_and_leaf(source_path)
        destination_parent: DirectoryAuthority | None = None
        source_native: int | None = None
        identity: NodeIdentity | None = None
        backend: str | None = None
        published = False
        primary: BaseException | None = None
        primary_traceback = None
        try:
            destination_parent, destination_leaf = self._parent_and_leaf(
                destination_path
            )
            if (
                source_parent.identity().volume
                != destination_parent.identity().volume
            ):
                raise AuthorityUnavailable("publication_cross_filesystem")
            if os.name == "nt":
                source_native = _win_nt_open(
                    source_parent.native_directory_handle,
                    source_leaf,
                    desired_access=_DELETE | _FILE_READ_ATTRIBUTES | _SYNCHRONIZE,
                    disposition=_FILE_OPEN,
                    directory=None,
                    share_delete=True,
                )
                identity = _win_identity(source_native, self._backend)
                self._check_volume(identity)
                _ensure_unaliased_regular_file(identity)
                _ensure_expected_identity(identity, expected_source, "publication_source")

                def prove_windows_publish_preconditions() -> None:
                    current_handle_identity = _win_identity(
                        source_native, self._backend
                    )
                    self._check_volume(current_handle_identity)
                    _ensure_unaliased_regular_file(current_handle_identity)
                    _ensure_expected_identity(
                        current_handle_identity,
                        identity,
                        "publication_source_handle",
                    )
                    _ensure_expected_identity(
                        current_handle_identity,
                        expected_source,
                        "publication_source",
                    )
                    named_source_native: int | None = None
                    try:
                        try:
                            named_source_native = _win_nt_open(
                                source_parent.native_directory_handle,
                                source_leaf,
                                desired_access=_FILE_READ_ATTRIBUTES | _SYNCHRONIZE,
                                disposition=_FILE_OPEN,
                                directory=None,
                                share_delete=True,
                            )
                        except AuthorityNotFound as error:
                            raise AuthorityViolation(
                                "publication_source_name_identity_changed"
                            ) from error
                        named_source_identity = _win_identity(
                            named_source_native, self._backend
                        )
                    finally:
                        if named_source_native is not None:
                            _win_close(named_source_native)
                    _ensure_unaliased_regular_file(named_source_identity)
                    _ensure_expected_identity(
                        named_source_identity,
                        identity,
                        "publication_source_name",
                    )
                    _ensure_expected_identity(
                        named_source_identity,
                        expected_source,
                        "publication_source",
                    )
                    destination_native: int | None = None
                    try:
                        try:
                            destination_native = _win_nt_open(
                                destination_parent.native_directory_handle,
                                destination_leaf,
                                desired_access=_FILE_READ_ATTRIBUTES | _SYNCHRONIZE,
                                disposition=_FILE_OPEN,
                                directory=None,
                                share_delete=True,
                            )
                        except AuthorityNotFound:
                            pass
                        else:
                            raise AuthorityConflict(
                                "authority_destination_exists"
                            )
                    finally:
                        if destination_native is not None:
                            _win_close(destination_native)

                for attempt in range(2):
                    prove_windows_publish_preconditions()
                    try:
                        backend = _win_publish(
                            source_native,
                            destination_parent.native_directory_handle,
                            destination_leaf,
                        )
                        break
                    except AuthorityIOError as error:
                        _, marker, winerror = str(error).rpartition("_winerror_")
                        if not marker or winerror != str(_ERROR_ACCESS_DENIED):
                            raise
                        if attempt == 1:
                            raise
            else:
                source_native = _posix_open_child(
                    source_parent.native_directory_handle,
                    source_leaf,
                    backend=self._backend,
                    directory=None,
                    no_xdev=self._same_filesystem,
                )
                identity = _posix_identity(source_native, self._backend)
                self._check_volume(identity)
                _ensure_unaliased_regular_file(identity)
                _ensure_expected_identity(
                    identity, expected_source, "publication_source"
                )
                backend = _posix_publish_noreplace(
                    source_parent.native_directory_handle,
                    source_leaf,
                    destination_parent.native_directory_handle,
                    destination_leaf,
                )
            published = True
        except BaseException as error:
            primary = error
            primary_traceback = error.__traceback__

        cleanup_failures: list[BaseException] = []
        if source_native is not None:
            try:
                if os.name == "nt":
                    _win_close(source_native)
                else:
                    os.close(source_native)
            except (AuthorityError, OSError) as error:
                cleanup_failures.append(error)
        if destination_parent is not None and not destination_parent.closed:
            try:
                destination_parent.close()
            except AuthorityError as error:
                cleanup_failures.append(error)
        if not source_parent.closed:
            try:
                source_parent.close()
            except AuthorityError as error:
                cleanup_failures.append(error)

        if published:
            if backend is None or identity is None:
                raise AuthorityIOError("authority_publication_outcome_invalid")
            if cleanup_failures:
                raise AuthorityPostPublicationError(
                    "authority_published_but_cleanup_failed",
                    backend=backend,
                    published_identity=identity,
                    source=source_path,
                    destination=destination_path,
                ) from BaseExceptionGroup(
                    "authority publication cleanup failures",
                    cleanup_failures,
                )
            return backend
        if primary is None:
            raise AuthorityIOError("authority_publication_outcome_invalid")
        if cleanup_failures:
            causes: list[BaseException] = []
            if primary.__cause__ is not None:
                causes.append(primary.__cause__)
            causes.extend(cleanup_failures)
            raise primary.with_traceback(primary_traceback) from BaseExceptionGroup(
                "authority publication failure cleanup",
                causes,
            )
        raise primary.with_traceback(primary_traceback)

    def _remove_entry(
        self,
        relative: RelativePath,
        *,
        directory: bool,
        expected: NodeIdentity,
    ) -> None:
        parent, leaf = self._parent_and_leaf(relative)
        native: int | None = None
        try:
            if os.name == "nt":
                native = _win_nt_open(
                    parent.native_directory_handle,
                    leaf,
                    desired_access=_DELETE | _FILE_READ_ATTRIBUTES | _SYNCHRONIZE,
                    disposition=_FILE_OPEN,
                    directory=directory,
                    share_delete=True,
                )
                actual = _win_identity(native, self._backend)
                _ensure_expected_identity(actual, expected, "remove_entry")
                _win_delete_handle(native)
            else:
                native = _posix_open_child(
                    parent.native_directory_handle,
                    leaf,
                    backend=self._backend,
                    directory=directory,
                    no_xdev=self._same_filesystem,
                )
                actual = _posix_identity(native, self._backend)
                _ensure_expected_identity(actual, expected, "remove_entry")
                if directory:
                    os.rmdir(leaf, dir_fd=parent.native_directory_handle)
                else:
                    os.unlink(leaf, dir_fd=parent.native_directory_handle)
        except AuthorityError:
            raise
        except OSError as error:
            raise AuthorityIOError("authority_owned_remove_failed") from error
        finally:
            if native is not None:
                if os.name == "nt":
                    _win_close(native)
                else:
                    os.close(native)
            parent.close()

    def remove_owned_tree(
        self,
        relative: RelativePath | str | os.PathLike[str] | Iterable[str],
        *,
        expected_root: NodeIdentity,
        maximum_entries: int = 256,
    ) -> None:
        root = RelativePath.parse(relative)
        if not root.parts:
            raise AuthorityViolation("cannot_remove_authority_root")
        remaining = maximum_entries

        def visit(path: RelativePath, expected: NodeIdentity) -> None:
            nonlocal remaining
            if remaining <= 0:
                raise AuthorityLimitExceeded("owned_tree_cleanup_limit")
            remaining -= 1
            if expected.kind == "regular_file":
                self._remove_entry(path, directory=False, expected=expected)
                return
            directory = self.open_directory(path)
            try:
                actual = directory.identity()
                _ensure_expected_identity(actual, expected, "owned_tree")
                entries = directory.list_entries(
                    maximum_entries=min(remaining, MAX_DIRECTORY_ENTRIES)
                )
            finally:
                directory.close()
            for entry in entries:
                visit(path.child(entry.name), entry.identity)
            self._remove_entry(path, directory=True, expected=expected)

        visit(root, expected_root)

    def close(self) -> None:
        if self._closed:
            return
        handles = self._handles
        self._handles = ()
        self._closed = True
        first_error: AuthorityError | None = None
        for handle in reversed(handles):
            try:
                if os.name == "nt":
                    _win_close(handle)
                else:
                    os.close(handle)
            except (AuthorityError, OSError) as error:
                if first_error is None:
                    first_error = AuthorityIOError("directory_authority_close_failed")
                    first_error.__cause__ = error
        if first_error is not None:
            raise first_error

    def __enter__(self) -> "DirectoryAuthority":
        self._require_open()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


def _write_probe_file(
    authority: DirectoryAuthority,
    name: str,
    value: bytes,
) -> NodeIdentity:
    with authority.create_file(name) as bound:
        bound.write_all(value)
        bound.flush()
        return bound.identity()


def probe_descriptor_relative_authority(
    directory: Path | str | os.PathLike[str],
) -> DescriptorAuthorityCapability:
    """Destructively prove this primitive inside an authorized directory."""

    authority = DirectoryAuthority.open_absolute(directory)
    probe_name = f".stv3-authority-{uuid.uuid4().hex}"
    probe_path = RelativePath.parse(probe_name)
    probe: DirectoryAuthority | None = None
    probe_identity: NodeIdentity | None = None
    owned = False
    succeeded = False
    result: DescriptorAuthorityCapability | None = None
    primary: BaseException | None = None
    primary_traceback = None
    try:
        probe = authority.create_directory(probe_path)
        owned = True
        probe_identity = probe.opened_identity
        if probe.reanchor(Path(directory) / probe_name).kind != "directory":
            raise AuthorityViolation("probe_reanchor_failed")

        file_source_identity = _write_probe_file(
            probe, "file-source", b"descriptor-source\n"
        )
        file_backend = probe.publish_noreplace(
            "file-source",
            "file-destination",
            expected_source=file_source_identity,
        )
        if probe.exists("file-source") or probe.read_file(
            "file-destination", maximum_bytes=64
        ) != b"descriptor-source\n":
            raise AuthorityViolation("probe_file_publication_failed")
        _write_probe_file(probe, "file-conflict", b"conflict-source\n")
        try:
            probe.publish_noreplace("file-conflict", "file-destination")
        except AuthorityConflict:
            pass
        else:
            raise AuthorityViolation("probe_file_conflict_not_rejected")
        if probe.read_file("file-conflict", maximum_bytes=64) != b"conflict-source\n":
            raise AuthorityViolation("probe_file_conflict_changed_source")

        directory_source = probe.create_directory("directory-source")
        directory_source_identity = directory_source.identity()
        _write_probe_file(directory_source, "sentinel", b"directory-source\n")
        directory_source.close()
        directory_backend = probe.publish_noreplace(
            "directory-source",
            "directory-destination",
            expected_source=directory_source_identity,
        )
        with probe.open_directory("directory-destination") as destination:
            if destination.read_file("sentinel", maximum_bytes=64) != b"directory-source\n":
                raise AuthorityViolation("probe_directory_publication_failed")
        directory_conflict_source = probe.create_directory(
            "directory-conflict-source"
        )
        _write_probe_file(directory_conflict_source, "source", b"source\n")
        directory_conflict_source.close()
        directory_conflict_destination = probe.create_directory(
            "directory-conflict-destination"
        )
        _write_probe_file(
            directory_conflict_destination, "destination", b"destination\n"
        )
        directory_conflict_destination.close()
        try:
            probe.publish_noreplace(
                "directory-conflict-source", "directory-conflict-destination"
            )
        except AuthorityConflict:
            pass
        else:
            raise AuthorityViolation("probe_directory_conflict_not_rejected")

        pinned = probe.create_directory("ancestor-pin-source")
        pinned_identity = pinned.identity()
        if os.name == "nt":
            try:
                probe.publish_noreplace(
                    "ancestor-pin-source",
                    "ancestor-pin-moved",
                    expected_source=pinned_identity,
                )
            except AuthorityViolation as error:
                if "pinned_against_delete" not in str(error):
                    raise
                ancestor_pin_mode = "rename_blocked_while_pinned"
            else:
                raise AuthorityViolation("windows_ancestor_pin_did_not_block_rename")
            finally:
                pinned.close()
            probe.publish_noreplace(
                "ancestor-pin-source",
                "ancestor-pin-moved",
                expected_source=pinned_identity,
            )
            with probe.open_directory("ancestor-pin-moved") as moved:
                _write_probe_file(moved, "inside", b"pinned\n")
        else:
            probe.publish_noreplace(
                "ancestor-pin-source",
                "ancestor-pin-moved",
                expected_source=pinned_identity,
            )
            _write_probe_file(pinned, "inside", b"pinned\n")
            pinned.close()
            with probe.open_directory("ancestor-pin-moved") as moved:
                if moved.read_file("inside", maximum_bytes=64) != b"pinned\n":
                    raise AuthorityViolation("posix_ancestor_pin_redirected")
            ancestor_pin_mode = "descriptor_survived_rename"

        entries = probe.list_entries(maximum_entries=32)
        if not any(entry.name == "file-destination" for entry in entries):
            raise AuthorityViolation("probe_descriptor_listing_failed")
        try:
            probe.fsync()
        except AuthorityUnavailable:
            directory_fsync_supported = False
        else:
            directory_fsync_supported = True
        if os.name != "nt" and not directory_fsync_supported:
            raise AuthorityViolation("posix_directory_fsync_unavailable")
        if file_backend.split("_", 1)[0] != directory_backend.split("_", 1)[0]:
            raise AuthorityViolation("publication_backend_changed")

        result = DescriptorAuthorityCapability(
            backend=authority.backend,
            absolute_bootstrap_mode=authority.absolute_bootstrap_mode,
            absolute_ancestor_resolution_no_reparse_proven=(
                authority.absolute_ancestor_resolution_no_reparse_proven
            ),
            ancestor_pin_mode=ancestor_pin_mode,
            authority_root_identity_pinned=(
                authority.authority_root_identity_pinned
            ),
            descriptor_relative_create_read_list=True,
            directory_conflict_rejected=True,
            directory_fsync_supported=directory_fsync_supported,
            directory_publication_succeeded=True,
            file_conflict_rejected=True,
            file_publication_succeeded=True,
            reanchor_identity_proven=True,
            same_filesystem=True,
        )
        succeeded = True
    except OSError as error:
        primary = AuthorityIOError("descriptor_authority_probe_io_failed")
        primary.__cause__ = error
        primary_traceback = error.__traceback__
    except BaseException as error:
        primary = error
        primary_traceback = error.__traceback__

    cleanup_failures: list[BaseException] = []
    if probe is not None and not probe.closed:
        try:
            probe.close()
        except AuthorityError as error:
            cleanup_failures.append(error)
    if owned and probe_identity is not None:
        try:
            authority.remove_owned_tree(
                probe_path,
                expected_root=probe_identity,
                maximum_entries=64,
            )
            try:
                authority._entry_identity_raw(probe_path)
            except AuthorityNotFound:
                pass
            else:
                raise AuthorityIOError("descriptor_authority_probe_still_exists")
            try:
                authority.fsync()
            except AuthorityUnavailable:
                if os.name != "nt":
                    raise
        except BaseException as error:
            cleanup_failures.append(error)
    if not authority.closed:
        try:
            authority.close()
        except AuthorityError as error:
            cleanup_failures.append(error)
    if cleanup_failures:
        failures: list[BaseException] = []
        if primary is not None:
            failures.append(primary)
        failures.extend(cleanup_failures)
        raise AuthorityIOError(
            "descriptor_authority_probe_cleanup_failed"
        ) from BaseExceptionGroup(
            "descriptor authority probe and cleanup failures",
            failures,
        )
    if primary is not None:
        raise primary.with_traceback(primary_traceback)
    if not succeeded or result is None:
        raise AuthorityIOError("descriptor_authority_probe_outcome_invalid")
    return result


__all__ = [
    "DESCRIPTOR_RELATIVE_AUTHORITY_CONTRACT",
    "MAX_COMPONENT_BYTES",
    "MAX_DIRECTORY_ENTRIES",
    "MAX_READ_BYTES",
    "MAX_RELATIVE_DEPTH",
    "MAX_RELATIVE_PATH_BYTES",
    "AuthorityConflict",
    "AuthorityError",
    "AuthorityIOError",
    "AuthorityLimitExceeded",
    "AuthorityNotFound",
    "AuthorityPostCreateError",
    "AuthorityPostPublicationError",
    "AuthorityUnavailable",
    "AuthorityViolation",
    "BoundFile",
    "DescriptorAuthorityCapability",
    "DirectoryAuthority",
    "DirectoryEntry",
    "NodeIdentity",
    "RelativePath",
    "probe_descriptor_relative_authority",
]
