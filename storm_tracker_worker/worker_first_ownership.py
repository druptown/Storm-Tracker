"""Closed-world, cross-process ownership authority for worker-first runtime.

This module is deliberately inert.  It neither imports nor instantiates the
worker-first coordinator, service, MQTT, Home Assistant, or any provider.  A
future composition root may inject a held :class:`WorkerFirstOwnershipLease`
into the coordinator and must keep that lease alive for the complete mutable
lifecycle.

The lock file is a permanent, immutable anchor.  It never contains a PID,
timestamp, owner name, or takeover marker.  POSIX ``flock`` and Windows CRT
byte-range locking are released by the operating system when the owning
process dies, so there is no stale-file takeover protocol.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import threading
from typing import Any, Final


AUTHORITY_CONTRACT: Final = "stv3-worker-first-ownership-authority-v1"
AUTHORITY_SCOPE_CONTRACT: Final = (
    "stv3-worker-first-ownership-authority-scope-v1"
)
LOCK_ANCHOR_CONTRACT: Final = "stv3-worker-first-ownership-lock-anchor-v1"
DIAGNOSTICS_CONTRACT: Final = "stv3-worker-first-ownership-diagnostics-v1"

LOCK_ANCHOR_BYTES: Final = (
    b'{"contract":"stv3-worker-first-ownership-lock-anchor-v1"}'
)
LOCK_ANCHOR_SHA256: Final = hashlib.sha256(LOCK_ANCHOR_BYTES).hexdigest()

REQUIRED_STORE_ROLES: Final[Mapping[str, str]] = {
    "acquisition_completion_store": "directory",
    "desired_state_store": "file",
    "operational_fact_store": "file",
    "operational_snapshot_store": "file",
    "pipeline_output_store": "file",
    "snapshot_outbox_store": "file",
}

MAX_AUTHORITY_BYTES: Final = 128 * 1024
MAX_DIAGNOSTIC_BYTES: Final = 4096
MAX_PATH_CHARS: Final = 4096
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")


class WorkerFirstOwnershipError(RuntimeError):
    """Base error with privacy-safe, bounded messages only."""


class WorkerFirstAuthorityError(WorkerFirstOwnershipError):
    """The trusted authority or an injected binding differs."""


class WorkerFirstFenceBusy(WorkerFirstOwnershipError):
    """Another coordinator currently owns the authority's OS lock."""


class WorkerFirstFenceLost(WorkerFirstOwnershipError):
    """The caller no longer owns the exact authority fence."""


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
        raise WorkerFirstAuthorityError("authority_not_canonical") from error


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha256(value: object, code: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise WorkerFirstAuthorityError(code)
    return value


def _identifier(value: object, code: str) -> str:
    if (
        not isinstance(value, str)
        or _IDENTIFIER_RE.fullmatch(value) is None
        or "://" in value
    ):
        raise WorkerFirstAuthorityError(code)
    return value


def _exact_mapping(
    value: object,
    fields: set[str],
    code: str,
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise WorkerFirstAuthorityError(code)
    return dict(value)


def _is_reparse(metadata: os.stat_result) -> bool:
    flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    attributes = int(getattr(metadata, "st_file_attributes", 0))
    return bool(flag and attributes & flag)


def _same_path(left: str | Path, right: str | Path) -> bool:
    return os.path.normcase(str(left)) == os.path.normcase(str(right))


@dataclass(frozen=True, slots=True, repr=False)
class _PathWitness:
    device: int
    inode: int
    mode_kind: int


def _witness(metadata: os.stat_result) -> _PathWitness:
    return _PathWitness(
        device=int(metadata.st_dev),
        inode=int(metadata.st_ino),
        mode_kind=stat.S_IFMT(metadata.st_mode),
    )


def _witness_matches(expected: _PathWitness, actual: os.stat_result) -> bool:
    observed = _witness(actual)
    if expected.mode_kind != observed.mode_kind:
        return False
    # CPython exposes stable file IDs on supported Windows and POSIX
    # filesystems.  Zero means the platform did not supply one; path/type and
    # immutable content checks remain mandatory in that case.
    if expected.inode and observed.inode:
        return (
            expected.device == observed.device
            and expected.inode == observed.inode
        )
    return True


def _existing_canonical_path(
    value: object,
    *,
    kind: str,
    code: str,
) -> tuple[Path, _PathWitness]:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > MAX_PATH_CHARS
        or "\x00" in value
    ):
        raise WorkerFirstAuthorityError(code)
    candidate = Path(value)
    if not candidate.is_absolute():
        raise WorkerFirstAuthorityError(code)
    try:
        metadata = candidate.lstat()
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise WorkerFirstAuthorityError(code) from error
    if (
        candidate.is_symlink()
        or _is_reparse(metadata)
        or not _same_path(value, resolved)
    ):
        raise WorkerFirstAuthorityError(code)
    if kind == "file" and not stat.S_ISREG(metadata.st_mode):
        raise WorkerFirstAuthorityError(code)
    if kind == "directory" and not stat.S_ISDIR(metadata.st_mode):
        raise WorkerFirstAuthorityError(code)
    if kind not in {"file", "directory"}:
        raise WorkerFirstAuthorityError(code)
    return resolved, _witness(metadata)


def _verify_witness(path: Path, witness: _PathWitness, *, kind: str) -> None:
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise WorkerFirstAuthorityError("authority_path_identity_changed") from error
    if (
        path.is_symlink()
        or _is_reparse(metadata)
        or not _same_path(path, resolved)
        or not _witness_matches(witness, metadata)
        or (kind == "file" and not stat.S_ISREG(metadata.st_mode))
        or (kind == "directory" and not stat.S_ISDIR(metadata.st_mode))
    ):
        raise WorkerFirstAuthorityError("authority_path_identity_changed")


@dataclass(frozen=True, slots=True, repr=False)
class WorkerFirstStoreAuthority:
    """One mutable store path bound to an immutable schema/profile identity."""

    role: str
    kind: str
    path: Path
    identity_sha256: str
    _path_witness: _PathWitness

    @classmethod
    def create(
        cls,
        *,
        role: str,
        path: str | Path,
        identity_sha256: str,
    ) -> "WorkerFirstStoreAuthority":
        if role not in REQUIRED_STORE_ROLES:
            raise WorkerFirstAuthorityError("store_role_invalid")
        kind = REQUIRED_STORE_ROLES[role]
        canonical_path, witness = _existing_canonical_path(
            str(path),
            kind=kind,
            code="store_path_invalid",
        )
        return cls(
            role=role,
            kind=kind,
            path=canonical_path,
            identity_sha256=_sha256(
                identity_sha256,
                "store_identity_invalid",
            ),
            _path_witness=witness,
        )

    def to_mapping(self) -> dict[str, str]:
        return {
            "identity_sha256": self.identity_sha256,
            "kind": self.kind,
            "path": str(self.path),
            "role": self.role,
        }

    def verify_path(self) -> None:
        _verify_witness(self.path, self._path_witness, kind=self.kind)


@dataclass(frozen=True, slots=True, repr=False)
class WorkerFirstAuthorityBinding:
    """Trusted, exact authority made available to a future composition root."""

    authority_path: Path
    authority_sha256: str
    authority_scope_sha256: str
    consumer_id: str
    stream_id: str
    lock_path: Path
    stores: tuple[WorkerFirstStoreAuthority, ...]
    _authority_witness: _PathWitness
    _lock_witness: _PathWitness

    @property
    def store_binding_sha256(self) -> str:
        return _digest([item.to_mapping() for item in self.stores])

    def store(self, role: str) -> WorkerFirstStoreAuthority:
        for item in self.stores:
            if item.role == role:
                return item
        raise WorkerFirstAuthorityError("store_role_missing")

    def verify_paths(self) -> None:
        _verify_witness(
            self.authority_path,
            self._authority_witness,
            kind="file",
        )
        _verify_witness(self.lock_path, self._lock_witness, kind="file")
        for store in self.stores:
            store.verify_path()

    def assert_coordinator_binding(
        self,
        *,
        consumer_id: str,
        stream_id: str,
        stores: Mapping[str, tuple[str | Path, str]],
    ) -> None:
        """Fail closed unless the coordinator injection is byte-semantically exact."""

        try:
            consumer = _identifier(consumer_id, "coordinator_binding_invalid")
            stream = _identifier(stream_id, "coordinator_binding_invalid")
        except WorkerFirstAuthorityError:
            raise WorkerFirstAuthorityError("coordinator_binding_invalid") from None
        if consumer != self.consumer_id or stream != self.stream_id:
            raise WorkerFirstAuthorityError("coordinator_binding_invalid")
        if not isinstance(stores, Mapping) or set(stores) != set(
            REQUIRED_STORE_ROLES
        ):
            raise WorkerFirstAuthorityError("coordinator_binding_invalid")
        for authority in self.stores:
            supplied = stores.get(authority.role)
            if not isinstance(supplied, tuple) or len(supplied) != 2:
                raise WorkerFirstAuthorityError("coordinator_binding_invalid")
            raw_path, raw_identity = supplied
            if (
                not _same_path(str(raw_path), authority.path)
                or raw_identity != authority.identity_sha256
            ):
                raise WorkerFirstAuthorityError("coordinator_binding_invalid")


def _scope_payload(
    *,
    consumer_id: str,
    stream_id: str,
    lock_path: Path,
    stores: tuple[WorkerFirstStoreAuthority, ...],
) -> dict[str, Any]:
    return {
        "consumer_id": consumer_id,
        "contract": AUTHORITY_SCOPE_CONTRACT,
        "lock": {
            "anchor_sha256": LOCK_ANCHOR_SHA256,
            "contract": LOCK_ANCHOR_CONTRACT,
            "path": str(lock_path),
        },
        "stores": [item.to_mapping() for item in stores],
        "stream_id": stream_id,
    }


def build_worker_first_authority_bytes(
    *,
    consumer_id: str,
    stream_id: str,
    lock_path: str | Path,
    stores: Iterable[WorkerFirstStoreAuthority],
) -> bytes:
    """Build canonical bytes; writing/publishing remains an external action."""

    consumer = _identifier(consumer_id, "consumer_id_invalid")
    stream = _identifier(stream_id, "stream_id_invalid")
    canonical_lock, _lock_witness = _existing_canonical_path(
        str(lock_path),
        kind="file",
        code="lock_path_invalid",
    )
    _verify_lock_anchor_path(canonical_lock)
    raw_stores = tuple(stores)
    if any(not isinstance(item, WorkerFirstStoreAuthority) for item in raw_stores):
        raise WorkerFirstAuthorityError("store_set_invalid")
    supplied = tuple(sorted(raw_stores, key=lambda item: item.role))
    if (
        tuple(item.role for item in supplied)
        != tuple(sorted(REQUIRED_STORE_ROLES))
        or len({str(item.path) for item in supplied}) != len(supplied)
        or any(item.path == canonical_lock for item in supplied)
    ):
        raise WorkerFirstAuthorityError("store_set_invalid")
    for item in supplied:
        item.verify_path()
    scope = _scope_payload(
        consumer_id=consumer,
        stream_id=stream,
        lock_path=canonical_lock,
        stores=supplied,
    )
    document = {
        "authority_scope_sha256": _digest(scope),
        "consumer_id": consumer,
        "contract": AUTHORITY_CONTRACT,
        "lock": scope["lock"],
        "stores": scope["stores"],
        "stream_id": stream,
    }
    body = _canonical(document)
    if len(body) > MAX_AUTHORITY_BYTES:
        raise WorkerFirstAuthorityError("authority_capacity_exceeded")
    return body


def _read_exact_file(path: Path, maximum_bytes: int) -> bytes:
    try:
        size = path.stat().st_size
        if not 0 < size <= maximum_bytes:
            raise WorkerFirstAuthorityError("authority_capacity_exceeded")
        with path.open("rb") as stream:
            body = stream.read(maximum_bytes + 1)
    except WorkerFirstAuthorityError:
        raise
    except OSError as error:
        raise WorkerFirstAuthorityError("authority_unavailable") from error
    if len(body) != size or len(body) > maximum_bytes:
        raise WorkerFirstAuthorityError("authority_capacity_exceeded")
    return body


def _verify_lock_anchor_path(path: Path) -> None:
    body = _read_exact_file(path, len(LOCK_ANCHOR_BYTES))
    if (
        len(body) != len(LOCK_ANCHOR_BYTES)
        or not hashlib.sha256(body).hexdigest() == LOCK_ANCHOR_SHA256
        or body != LOCK_ANCHOR_BYTES
    ):
        raise WorkerFirstAuthorityError("lock_anchor_invalid")


def load_worker_first_authority(
    authority_path: str | Path,
    *,
    expected_authority_sha256: str,
) -> WorkerFirstAuthorityBinding:
    """Load one trusted canonical authority without exposing paths in errors."""

    expected_sha = _sha256(
        expected_authority_sha256,
        "expected_authority_sha256_invalid",
    )
    canonical_authority, authority_witness = _existing_canonical_path(
        str(authority_path),
        kind="file",
        code="authority_path_invalid",
    )
    body = _read_exact_file(canonical_authority, MAX_AUTHORITY_BYTES)
    if hashlib.sha256(body).hexdigest() != expected_sha:
        raise WorkerFirstAuthorityError("authority_checksum_mismatch")
    try:
        document = json.loads(body.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise WorkerFirstAuthorityError("authority_not_canonical") from error
    if _canonical(document) != body:
        raise WorkerFirstAuthorityError("authority_not_canonical")
    exact = _exact_mapping(
        document,
        {
            "authority_scope_sha256",
            "consumer_id",
            "contract",
            "lock",
            "stores",
            "stream_id",
        },
        "authority_schema_invalid",
    )
    if exact["contract"] != AUTHORITY_CONTRACT:
        raise WorkerFirstAuthorityError("authority_contract_invalid")
    consumer = _identifier(exact["consumer_id"], "consumer_id_invalid")
    stream = _identifier(exact["stream_id"], "stream_id_invalid")
    lock = _exact_mapping(
        exact["lock"],
        {"anchor_sha256", "contract", "path"},
        "lock_authority_invalid",
    )
    if (
        lock["contract"] != LOCK_ANCHOR_CONTRACT
        or lock["anchor_sha256"] != LOCK_ANCHOR_SHA256
    ):
        raise WorkerFirstAuthorityError("lock_authority_invalid")
    canonical_lock, lock_witness = _existing_canonical_path(
        lock["path"],
        kind="file",
        code="lock_path_invalid",
    )
    # Anchor bytes are verified only after this process wins the nonblocking
    # OS lock.  On Windows an existing owner's byte-range lock intentionally
    # denies overlapping reads, so reading here would misclassify contention
    # as authority corruption.
    raw_stores = exact["stores"]
    if not isinstance(raw_stores, list) or len(raw_stores) != len(
        REQUIRED_STORE_ROLES
    ):
        raise WorkerFirstAuthorityError("store_set_invalid")
    stores: list[WorkerFirstStoreAuthority] = []
    for raw in raw_stores:
        item = _exact_mapping(
            raw,
            {"identity_sha256", "kind", "path", "role"},
            "store_schema_invalid",
        )
        role = item["role"]
        if role not in REQUIRED_STORE_ROLES or item["kind"] != REQUIRED_STORE_ROLES[role]:
            raise WorkerFirstAuthorityError("store_role_invalid")
        stores.append(WorkerFirstStoreAuthority.create(
            role=role,
            path=item["path"],
            identity_sha256=item["identity_sha256"],
        ))
    ordered = tuple(sorted(stores, key=lambda item: item.role))
    if (
        [item.to_mapping() for item in ordered] != raw_stores
        or tuple(item.role for item in ordered)
        != tuple(sorted(REQUIRED_STORE_ROLES))
        or len({str(item.path) for item in ordered}) != len(ordered)
        or any(item.path in {canonical_authority, canonical_lock} for item in ordered)
    ):
        raise WorkerFirstAuthorityError("store_set_invalid")
    scope = _scope_payload(
        consumer_id=consumer,
        stream_id=stream,
        lock_path=canonical_lock,
        stores=ordered,
    )
    if _sha256(
        exact["authority_scope_sha256"],
        "authority_scope_invalid",
    ) != _digest(scope):
        raise WorkerFirstAuthorityError("authority_scope_mismatch")
    return WorkerFirstAuthorityBinding(
        authority_path=canonical_authority,
        authority_sha256=expected_sha,
        authority_scope_sha256=exact["authority_scope_sha256"],
        consumer_id=consumer,
        stream_id=stream,
        lock_path=canonical_lock,
        stores=ordered,
        _authority_witness=authority_witness,
        _lock_witness=lock_witness,
    )


def initialize_worker_first_lock_file(path: str | Path) -> Path:
    """Create the immutable anchor once; never truncate or replace it."""

    candidate = Path(path)
    if not candidate.is_absolute() or len(str(candidate)) > MAX_PATH_CHARS:
        raise WorkerFirstAuthorityError("lock_path_invalid")
    try:
        parent = candidate.parent.resolve(strict=True)
        if not _same_path(candidate.parent, parent):
            raise WorkerFirstAuthorityError("lock_path_invalid")
    except WorkerFirstAuthorityError:
        raise
    except (OSError, RuntimeError) as error:
        raise WorkerFirstAuthorityError("lock_path_invalid") from error
    resolved_candidate = parent / candidate.name
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(resolved_candidate, flags, 0o600)
        written = 0
        while written < len(LOCK_ANCHOR_BYTES):
            count = os.write(descriptor, LOCK_ANCHOR_BYTES[written:])
            if count <= 0:
                raise WorkerFirstAuthorityError("lock_anchor_write_failed")
            written += count
        os.fsync(descriptor)
    except FileExistsError:
        pass
    except WorkerFirstAuthorityError:
        raise
    except OSError as error:
        raise WorkerFirstAuthorityError("lock_anchor_write_failed") from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
    canonical, _path_witness = _existing_canonical_path(
        str(resolved_candidate),
        kind="file",
        code="lock_path_invalid",
    )
    _verify_lock_anchor_path(canonical)
    return canonical


def _backend() -> str:
    return "windows_msvcrt" if os.name == "nt" else "posix_flock"


def _lock_descriptor(descriptor: int) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        return
    import fcntl

    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_descriptor(descriptor: int) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(descriptor, fcntl.LOCK_UN)


def _read_anchor_descriptor(descriptor: int) -> bytes:
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks: list[bytes] = []
    remaining = len(LOCK_ANCHOR_BYTES) + 1
    while remaining:
        chunk = os.read(descriptor, remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


_PROCESS_OWNERS_LOCK = threading.Lock()
_PROCESS_OWNED_PATHS: set[str] = set()


def _claim_process_path(path: Path) -> bool:
    key = os.path.normcase(str(path))
    with _PROCESS_OWNERS_LOCK:
        if key in _PROCESS_OWNED_PATHS:
            return False
        _PROCESS_OWNED_PATHS.add(key)
        return True


def _release_process_path(path: Path) -> None:
    key = os.path.normcase(str(path))
    with _PROCESS_OWNERS_LOCK:
        _PROCESS_OWNED_PATHS.discard(key)


@dataclass(slots=True, repr=False)
class WorkerFirstOwnershipLease:
    """Stable coordinator injection handle; ownership lasts while this is held."""

    _fence: "WorkerFirstOwnershipFence"
    _token: object

    @property
    def binding(self) -> WorkerFirstAuthorityBinding:
        self.assert_held()
        return self._fence.binding

    @property
    def consumer_id(self) -> str:
        return self.binding.consumer_id

    @property
    def stream_id(self) -> str:
        return self.binding.stream_id

    @property
    def authority_sha256(self) -> str:
        return self.binding.authority_sha256

    @property
    def authority_scope_sha256(self) -> str:
        return self.binding.authority_scope_sha256

    def assert_held(self) -> None:
        self._fence._assert_held(self._token)

    def assert_coordinator_binding(
        self,
        *,
        consumer_id: str,
        stream_id: str,
        stores: Mapping[str, tuple[str | Path, str]],
    ) -> None:
        self.assert_held()
        self._fence.binding.assert_coordinator_binding(
            consumer_id=consumer_id,
            stream_id=stream_id,
            stores=stores,
        )

    def diagnostics(self) -> dict[str, Any]:
        return self._fence.diagnostics(token=self._token)

    def release(self) -> None:
        self._fence._release(self._token)

    close = release

    def __enter__(self) -> "WorkerFirstOwnershipLease":
        self.assert_held()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()

    def __del__(self) -> None:
        try:
            self.release()
        except BaseException:
            pass


class WorkerFirstOwnershipFence:
    """Non-blocking OS ownership fence for one checksummed authority."""

    def __init__(
        self,
        authority_path: str | Path,
        *,
        expected_authority_sha256: str,
    ) -> None:
        self.binding = load_worker_first_authority(
            authority_path,
            expected_authority_sha256=expected_authority_sha256,
        )
        self._state_lock = threading.Lock()
        self._descriptor: int | None = None
        self._token: object | None = None
        self._state = "ready"

    def _verify_authority(self, *, read_lock_anchor: bool = True) -> None:
        self.binding.verify_paths()
        body = _read_exact_file(
            self.binding.authority_path,
            MAX_AUTHORITY_BYTES,
        )
        if hashlib.sha256(body).hexdigest() != self.binding.authority_sha256:
            raise WorkerFirstAuthorityError("authority_checksum_mismatch")
        # Windows byte-range locks deny overlapping reads even through another
        # descriptor in the same process.  Once held, the already-open locked
        # descriptor is the authoritative anchor-content witness.
        if read_lock_anchor:
            _verify_lock_anchor_path(self.binding.lock_path)

    def acquire(self) -> WorkerFirstOwnershipLease:
        """Acquire immediately or raise ``WorkerFirstFenceBusy``; never wait."""

        with self._state_lock:
            if self._descriptor is not None or self._token is not None:
                raise WorkerFirstFenceBusy("ownership_busy")
            # Do not read the anchor through a second descriptor here: on
            # Windows another process' byte lock correctly denies that read.
            # The constructor already verified the immutable bytes; after a
            # successful OS lock we verify them again through the locked fd.
            self._verify_authority(read_lock_anchor=False)
            if not _claim_process_path(self.binding.lock_path):
                self._state = "busy"
                raise WorkerFirstFenceBusy("ownership_busy")
            flags = os.O_RDWR
            flags |= getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor: int | None = None
            try:
                descriptor = os.open(self.binding.lock_path, flags)
                os.set_inheritable(descriptor, False)
                metadata = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or not _witness_matches(
                        self.binding._lock_witness,
                        metadata,
                    )
                ):
                    raise WorkerFirstAuthorityError("lock_anchor_invalid")
                _lock_descriptor(descriptor)
                # Reverify after the OS lock to close the open/lock race.
                if _read_anchor_descriptor(descriptor) != LOCK_ANCHOR_BYTES:
                    raise WorkerFirstAuthorityError("lock_anchor_invalid")
                self._verify_authority(read_lock_anchor=False)
            except WorkerFirstAuthorityError:
                if descriptor is not None:
                    try:
                        _unlock_descriptor(descriptor)
                    except (ImportError, OSError):
                        pass
                    os.close(descriptor)
                _release_process_path(self.binding.lock_path)
                self._state = "error"
                raise
            except (BlockingIOError, PermissionError):
                if descriptor is not None:
                    os.close(descriptor)
                _release_process_path(self.binding.lock_path)
                self._state = "busy"
                raise WorkerFirstFenceBusy("ownership_busy") from None
            except (ImportError, OSError) as error:
                if descriptor is not None:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                _release_process_path(self.binding.lock_path)
                self._state = "error"
                raise WorkerFirstOwnershipError("ownership_boundary_failed") from error
            token = object()
            self._descriptor = descriptor
            self._token = token
            self._state = "held"
            return WorkerFirstOwnershipLease(self, token)

    def _assert_held(self, token: object) -> None:
        with self._state_lock:
            if (
                self._descriptor is None
                or self._token is None
                or token is not self._token
                or self._state != "held"
            ):
                raise WorkerFirstFenceLost("ownership_not_held")
            try:
                metadata = os.fstat(self._descriptor)
                if (
                    not _witness_matches(self.binding._lock_witness, metadata)
                    or _read_anchor_descriptor(self._descriptor)
                    != LOCK_ANCHOR_BYTES
                ):
                    raise WorkerFirstFenceLost("ownership_identity_lost")
                self._verify_authority(read_lock_anchor=False)
            except WorkerFirstAuthorityError as error:
                raise WorkerFirstFenceLost("ownership_identity_lost") from error
            except OSError as error:
                raise WorkerFirstFenceLost("ownership_identity_lost") from error

    def _release(self, token: object) -> None:
        with self._state_lock:
            if self._token is None and self._descriptor is None:
                return
            if token is not self._token:
                raise WorkerFirstFenceLost("ownership_not_held")
            descriptor = self._descriptor
            self._descriptor = None
            self._token = None
            self._state = "released"
            try:
                if descriptor is not None:
                    _unlock_descriptor(descriptor)
            except (ImportError, OSError):
                # Closing the descriptor is the authoritative OS release.
                pass
            finally:
                if descriptor is not None:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                _release_process_path(self.binding.lock_path)

    def diagnostics(self, *, token: object | None = None) -> dict[str, Any]:
        """Return only fixed codes, counts and one-way binding hashes."""

        with self._state_lock:
            held = (
                self._descriptor is not None
                and self._token is not None
                and self._state == "held"
                and (token is None or token is self._token)
            )
            result = {
                "authority_scope_sha256": self.binding.authority_scope_sha256,
                "authority_sha256": self.binding.authority_sha256,
                "backend": _backend(),
                "consumer_binding_sha256": hashlib.sha256(
                    self.binding.consumer_id.encode("utf-8")
                ).hexdigest(),
                "contract": DIAGNOSTICS_CONTRACT,
                "held": held,
                "state": self._state,
                "store_binding_sha256": self.binding.store_binding_sha256,
                "store_count": len(self.binding.stores),
                "stream_binding_sha256": hashlib.sha256(
                    self.binding.stream_id.encode("utf-8")
                ).hexdigest(),
            }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise WorkerFirstOwnershipError("diagnostics_capacity_exceeded")
        return result

    def close(self) -> None:
        with self._state_lock:
            token = self._token
        if token is not None:
            self._release(token)

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


__all__ = [
    "AUTHORITY_CONTRACT",
    "AUTHORITY_SCOPE_CONTRACT",
    "DIAGNOSTICS_CONTRACT",
    "LOCK_ANCHOR_BYTES",
    "LOCK_ANCHOR_CONTRACT",
    "LOCK_ANCHOR_SHA256",
    "MAX_AUTHORITY_BYTES",
    "MAX_DIAGNOSTIC_BYTES",
    "REQUIRED_STORE_ROLES",
    "WorkerFirstAuthorityBinding",
    "WorkerFirstAuthorityError",
    "WorkerFirstFenceBusy",
    "WorkerFirstFenceLost",
    "WorkerFirstOwnershipError",
    "WorkerFirstOwnershipFence",
    "WorkerFirstOwnershipLease",
    "WorkerFirstStoreAuthority",
    "build_worker_first_authority_bytes",
    "initialize_worker_first_lock_file",
    "load_worker_first_authority",
]
