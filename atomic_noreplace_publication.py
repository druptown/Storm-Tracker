"""Portable, fail-closed atomic no-replace publication capability.

This module deliberately exposes one narrow filesystem primitive.  It does not
claim descriptor-relative authority for the complete path on Windows, and it
never falls back to a check-then-rename, replacement, copy/delete, or
link/unlink sequence.
"""
from __future__ import annotations

from dataclasses import dataclass
import ctypes
import errno
import os
from pathlib import Path
import platform
import stat
import uuid
from typing import Final


ATOMIC_NOREPLACE_CONTRACT: Final = (
    "stv3-atomic-noreplace-publication-capability-v1"
)
RENAME_NOREPLACE: Final = 1
RENAME_EXCL: Final = 0x00000004
ERROR_FILE_EXISTS: Final = 80
ERROR_ALREADY_EXISTS: Final = 183
ERROR_NOT_SUPPORTED: Final = 50
ERROR_CALL_NOT_IMPLEMENTED: Final = 120
ERROR_INVALID_FUNCTION: Final = 1
ERROR_INVALID_PARAMETER: Final = 87

_UNSUPPORTED_ERRNOS: Final = frozenset({
    errno.ENOSYS,
    errno.EINVAL,
    getattr(errno, "ENOTSUP", errno.EINVAL),
    getattr(errno, "EOPNOTSUPP", errno.EINVAL),
})
_WINDOWS_UNSUPPORTED_ERRORS: Final = frozenset({
    ERROR_NOT_SUPPORTED,
    ERROR_CALL_NOT_IMPLEMENTED,
    ERROR_INVALID_FUNCTION,
    ERROR_INVALID_PARAMETER,
})
_WINDOWS_CONFLICT_ERRORS: Final = frozenset({
    ERROR_FILE_EXISTS,
    ERROR_ALREADY_EXISTS,
})


class AtomicNoReplaceError(RuntimeError):
    """Base error for a no-replace publication that did not complete."""


class AtomicNoReplaceConflict(AtomicNoReplaceError):
    """The destination already exists and was left untouched."""


class AtomicNoReplaceUnavailable(AtomicNoReplaceError):
    """The platform/filesystem cannot prove the required primitive."""


class AtomicNoReplaceFailure(AtomicNoReplaceError):
    """The selected primitive failed for a reason other than a conflict."""


@dataclass(frozen=True, slots=True)
class AtomicNoReplaceCapability:
    backend: str
    destination_conflict_rejected: bool
    directory_destination_conflict_rejected: bool
    directory_source_consumed_on_success: bool
    destructive_probe_completed: bool
    same_filesystem: bool
    source_consumed_on_success: bool

    @property
    def proven(self) -> bool:
        return (
            self.destination_conflict_rejected
            and self.directory_destination_conflict_rejected
            and self.directory_source_consumed_on_success
            and self.destructive_probe_completed
            and self.same_filesystem
            and self.source_consumed_on_success
        )

    def to_mapping(self) -> dict[str, object]:
        return {
            "backend": self.backend,
            "contract": ATOMIC_NOREPLACE_CONTRACT,
            "destination_conflict_rejected": (
                self.destination_conflict_rejected
            ),
            "directory_destination_conflict_rejected": (
                self.directory_destination_conflict_rejected
            ),
            "directory_source_consumed_on_success": (
                self.directory_source_consumed_on_success
            ),
            "destructive_probe_completed": self.destructive_probe_completed,
            "fallback_used": False,
            "no_replace_atomicity_proven": self.proven,
            "path_authority_scope": (
                "path_based_windows_api"
                if self.backend.startswith("windows_")
                else "parent_directory_descriptors"
            ),
            "same_filesystem": self.same_filesystem,
            "source_consumed_on_success": self.source_consumed_on_success,
        }


def _plain_directory(path: Path, label: str) -> os.stat_result:
    try:
        details = path.lstat()
    except OSError as error:
        raise AtomicNoReplaceFailure(f"{label}_unavailable") from error
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    is_reparse = bool(
        reparse_flag
        and int(getattr(details, "st_file_attributes", 0)) & reparse_flag
    )
    if (
        not stat.S_ISDIR(details.st_mode)
        or path.is_symlink()
        or is_reparse
    ):
        raise AtomicNoReplaceFailure(f"{label}_not_plain_directory")
    return details


def _leaf_name(path: Path, label: str) -> str:
    if (
        path.name in {"", ".", ".."}
        or path.name != str(path)
        or "\x00" in path.name
        or "/" in path.name
        or "\\" in path.name
    ):
        raise AtomicNoReplaceFailure(f"{label}_not_single_component")
    return path.name


def _open_directory_descriptor(path: Path) -> int:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        os.set_inheritable(descriptor, False)
        opened = os.fstat(descriptor)
        lexical = _plain_directory(path, "publication_parent")
        if (
            not stat.S_ISDIR(opened.st_mode)
            or int(opened.st_dev) != int(lexical.st_dev)
            or int(opened.st_ino) != int(lexical.st_ino)
        ):
            raise AtomicNoReplaceFailure("publication_parent_identity_changed")
        return descriptor
    except AtomicNoReplaceError:
        if descriptor is not None:
            os.close(descriptor)
        raise
    except OSError as error:
        if descriptor is not None:
            os.close(descriptor)
        raise AtomicNoReplaceFailure("publication_parent_open_failed") from error


def _classify_posix_error(code: int, backend: str) -> None:
    if code in {errno.EEXIST, errno.ENOTEMPTY}:
        raise AtomicNoReplaceConflict("atomic_noreplace_destination_exists")
    if code in _UNSUPPORTED_ERRNOS:
        raise AtomicNoReplaceUnavailable(
            f"atomic_noreplace_{backend}_unsupported"
        )
    raise AtomicNoReplaceFailure(
        f"atomic_noreplace_{backend}_failed_errno_{code}"
    )


def _linux_publish(
    source_parent_fd: int,
    source_name: str,
    destination_parent_fd: int,
    destination_name: str,
) -> str:
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = libc.renameat2
    except (AttributeError, OSError) as error:
        raise AtomicNoReplaceUnavailable(
            "atomic_noreplace_renameat2_unavailable"
        ) from error
    renameat2.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameat2.restype = ctypes.c_int
    result = renameat2(
        source_parent_fd,
        os.fsencode(source_name),
        destination_parent_fd,
        os.fsencode(destination_name),
        RENAME_NOREPLACE,
    )
    if result != 0:
        _classify_posix_error(ctypes.get_errno(), "renameat2")
    return "linux_renameat2_noreplace"


def _darwin_publish(
    source_parent_fd: int,
    source_name: str,
    destination_parent_fd: int,
    destination_name: str,
) -> str:
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        renameatx = libc.renameatx_np
    except (AttributeError, OSError) as error:
        raise AtomicNoReplaceUnavailable(
            "atomic_noreplace_renameatx_np_unavailable"
        ) from error
    renameatx.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameatx.restype = ctypes.c_int
    result = renameatx(
        source_parent_fd,
        os.fsencode(source_name),
        destination_parent_fd,
        os.fsencode(destination_name),
        RENAME_EXCL,
    )
    if result != 0:
        _classify_posix_error(ctypes.get_errno(), "renameatx_np")
    return "darwin_renameatx_np_excl"


def _windows_publish(source: Path, destination: Path) -> str:
    source_parent = _plain_directory(source.parent, "source_parent")
    destination_parent = _plain_directory(
        destination.parent, "destination_parent"
    )
    if int(source_parent.st_dev) != int(destination_parent.st_dev):
        raise AtomicNoReplaceUnavailable(
            "atomic_noreplace_cross_volume_unsupported"
        )
    try:
        move_file = ctypes.WinDLL(
            "kernel32", use_last_error=True
        ).MoveFileExW
    except (AttributeError, OSError) as error:
        raise AtomicNoReplaceUnavailable(
            "atomic_noreplace_movefileex_unavailable"
        ) from error
    move_file.argtypes = (
        ctypes.c_wchar_p,
        ctypes.c_wchar_p,
        ctypes.c_uint32,
    )
    move_file.restype = ctypes.c_int
    # Zero explicitly excludes MOVEFILE_REPLACE_EXISTING and
    # MOVEFILE_COPY_ALLOWED.  The latter prevents a copy/delete fallback.
    if not move_file(str(source), str(destination), 0):
        code = ctypes.get_last_error()
        if code in _WINDOWS_CONFLICT_ERRORS:
            raise AtomicNoReplaceConflict(
                "atomic_noreplace_destination_exists"
            )
        if code in _WINDOWS_UNSUPPORTED_ERRORS:
            raise AtomicNoReplaceUnavailable(
                "atomic_noreplace_movefileex_unsupported"
            )
        raise AtomicNoReplaceFailure(
            f"atomic_noreplace_movefileex_failed_winerror_{code}"
        )
    return "windows_movefileex_no_replace"


def publish_atomic_noreplace(source: Path, destination: Path) -> str:
    """Move ``source`` to absent ``destination`` atomically or fail closed."""

    source = Path(source)
    destination = Path(destination)
    if (
        not source.is_absolute()
        or not destination.is_absolute()
        or source.name in {"", ".", ".."}
        or destination.name in {"", ".", ".."}
    ):
        raise AtomicNoReplaceFailure("publication_leaf_invalid")
    _plain_directory(source.parent, "source_parent")
    _plain_directory(destination.parent, "destination_parent")
    if os.name == "nt":
        return _windows_publish(source, destination)
    source_name = _leaf_name(Path(source.name), "source_leaf")
    destination_name = _leaf_name(Path(destination.name), "destination_leaf")
    source_parent_fd = _open_directory_descriptor(source.parent)
    destination_parent_fd: int | None = None
    try:
        destination_parent_fd = _open_directory_descriptor(destination.parent)
        system = platform.system()
        if system == "Linux":
            return _linux_publish(
                source_parent_fd,
                source_name,
                destination_parent_fd,
                destination_name,
            )
        if system == "Darwin":
            return _darwin_publish(
                source_parent_fd,
                source_name,
                destination_parent_fd,
                destination_name,
            )
        raise AtomicNoReplaceUnavailable(
            "atomic_noreplace_platform_unsupported"
        )
    finally:
        if destination_parent_fd is not None:
            os.close(destination_parent_fd)
        os.close(source_parent_fd)


def _write_probe_file_exclusive(path: Path, value: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags, 0o600)
        os.set_inheritable(descriptor, False)
        view = memoryview(value)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise AtomicNoReplaceFailure(
                    "atomic_noreplace_probe_write_stalled"
                )
            view = view[written:]
        os.fsync(descriptor)
    except FileExistsError as error:
        raise AtomicNoReplaceFailure(
            "atomic_noreplace_probe_name_collision"
        ) from error
    except AtomicNoReplaceError:
        raise
    except OSError as error:
        raise AtomicNoReplaceFailure(
            "atomic_noreplace_probe_create_failed"
        ) from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


def probe_atomic_noreplace(directory: Path) -> AtomicNoReplaceCapability:
    """Destructively prove success and conflict semantics inside ``directory``.

    Only uniquely named regular files and empty directories created by this
    call are touched.  The caller must be authorized to make and remove these
    bounded probe entries in the supplied directory.
    """

    directory = Path(directory)
    _plain_directory(directory, "probe_directory")
    probe_id = uuid.uuid4().hex
    source = directory / f".atomic-noreplace-{probe_id}.source"
    destination = directory / f".atomic-noreplace-{probe_id}.destination"
    conflict_source = directory / f".atomic-noreplace-{probe_id}.conflict"
    directory_source = directory / f".atomic-noreplace-{probe_id}.dir-source"
    directory_destination = (
        directory / f".atomic-noreplace-{probe_id}.dir-destination"
    )
    directory_conflict_source = (
        directory / f".atomic-noreplace-{probe_id}.dir-conflict-source"
    )
    directory_conflict_destination = (
        directory / f".atomic-noreplace-{probe_id}.dir-conflict-destination"
    )
    backend: str | None = None
    owned_files: set[Path] = set()
    owned_directories: set[Path] = set()
    probe_succeeded = False
    try:
        _write_probe_file_exclusive(
            source,
            b"stv3-atomic-noreplace-source\n",
        )
        owned_files.add(source)
        backend = publish_atomic_noreplace(source, destination)
        owned_files.remove(source)
        owned_files.add(destination)
        if source.exists() or destination.read_bytes() != b"stv3-atomic-noreplace-source\n":
            raise AtomicNoReplaceFailure("atomic_noreplace_success_probe_failed")
        _write_probe_file_exclusive(
            conflict_source,
            b"stv3-atomic-noreplace-conflict\n",
        )
        owned_files.add(conflict_source)
        try:
            publish_atomic_noreplace(conflict_source, destination)
        except AtomicNoReplaceConflict:
            pass
        else:
            raise AtomicNoReplaceFailure(
                "atomic_noreplace_conflict_probe_was_not_rejected"
            )
        if (
            not conflict_source.is_file()
            or conflict_source.read_bytes() != b"stv3-atomic-noreplace-conflict\n"
            or destination.read_bytes() != b"stv3-atomic-noreplace-source\n"
        ):
            raise AtomicNoReplaceFailure(
                "atomic_noreplace_conflict_probe_changed_a_file"
            )
        try:
            directory_source.mkdir(mode=0o700)
            owned_directories.add(directory_source)
            directory_conflict_source.mkdir(mode=0o700)
            owned_directories.add(directory_conflict_source)
            directory_conflict_destination.mkdir(mode=0o700)
            owned_directories.add(directory_conflict_destination)
        except FileExistsError as error:
            raise AtomicNoReplaceFailure(
                "atomic_noreplace_probe_name_collision"
            ) from error
        except OSError as error:
            raise AtomicNoReplaceFailure(
                "atomic_noreplace_probe_directory_create_failed"
            ) from error
        directory_backend = publish_atomic_noreplace(
            directory_source,
            directory_destination,
        )
        owned_directories.remove(directory_source)
        owned_directories.add(directory_destination)
        if directory_backend != backend:
            raise AtomicNoReplaceFailure(
                "atomic_noreplace_probe_backend_changed"
            )
        if directory_source.exists() or not directory_destination.is_dir():
            raise AtomicNoReplaceFailure(
                "atomic_noreplace_directory_success_probe_failed"
            )
        try:
            publish_atomic_noreplace(
                directory_conflict_source,
                directory_conflict_destination,
            )
        except AtomicNoReplaceConflict:
            pass
        else:
            raise AtomicNoReplaceFailure(
                "atomic_noreplace_directory_conflict_was_not_rejected"
            )
        if (
            not directory_conflict_source.is_dir()
            or not directory_conflict_destination.is_dir()
        ):
            raise AtomicNoReplaceFailure(
                "atomic_noreplace_directory_conflict_changed_a_directory"
            )
        assert backend is not None
        probe_succeeded = True
        return AtomicNoReplaceCapability(
            backend=backend,
            destination_conflict_rejected=True,
            directory_destination_conflict_rejected=True,
            directory_source_consumed_on_success=True,
            destructive_probe_completed=True,
            same_filesystem=True,
            source_consumed_on_success=True,
        )
    except AtomicNoReplaceError:
        raise
    except OSError as error:
        raise AtomicNoReplaceFailure("atomic_noreplace_probe_io_failed") from error
    finally:
        cleanup_failed = False
        for path in tuple(owned_files):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                cleanup_failed = True
        for path in tuple(owned_directories):
            try:
                path.rmdir()
            except FileNotFoundError:
                pass
            except OSError:
                cleanup_failed = True
        # Cleanup cannot turn a successful primitive probe into a positive
        # capability result.  On an earlier failure, preserve the causal
        # exception instead of masking it with a secondary cleanup failure.
        if cleanup_failed and probe_succeeded:
            raise AtomicNoReplaceFailure(
                "atomic_noreplace_probe_cleanup_failed"
            ) from None


__all__ = [
    "ATOMIC_NOREPLACE_CONTRACT",
    "AtomicNoReplaceCapability",
    "AtomicNoReplaceConflict",
    "AtomicNoReplaceError",
    "AtomicNoReplaceFailure",
    "AtomicNoReplaceUnavailable",
    "probe_atomic_noreplace",
    "publish_atomic_noreplace",
]
