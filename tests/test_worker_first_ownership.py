"""Cross-process proofs for the inert worker-first ownership authority."""
from __future__ import annotations

import ast
from dataclasses import dataclass
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from storm_tracker_worker import worker_first_ownership as ownership_module
from storm_tracker_worker.worker_first_ownership import (
    AUTHORITY_CONTRACT,
    LOCK_ANCHOR_BYTES,
    REQUIRED_STORE_ROLES,
    WorkerFirstAuthorityError,
    WorkerFirstFenceBusy,
    WorkerFirstFenceLost,
    WorkerFirstOwnershipFence,
    WorkerFirstStoreAuthority,
    build_worker_first_authority_bytes,
    initialize_worker_first_lock_file,
)


CONSUMER = "private-consumer-token-123"
STREAM = "private.stream.secret"
ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "storm_tracker_worker" / "worker_first_ownership.py"


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


@dataclass(frozen=True)
class _Authority:
    root: Path
    path: Path
    body: bytes
    sha256: str
    lock_path: Path
    stores: tuple[WorkerFirstStoreAuthority, ...]

    @property
    def injection(self) -> dict[str, tuple[Path, str]]:
        return {
            item.role: (item.path, item.identity_sha256)
            for item in self.stores
        }


def _authority(root: Path) -> _Authority:
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    lock_path = initialize_worker_first_lock_file(
        (root / "worker-first.lock").resolve()
    )
    stores: list[WorkerFirstStoreAuthority] = []
    for role, kind in sorted(REQUIRED_STORE_ROLES.items()):
        path = (root / role).resolve()
        if kind == "directory":
            path.mkdir()
        else:
            path.write_bytes(f"{role}:mutable-store\n".encode("ascii"))
        stores.append(WorkerFirstStoreAuthority.create(
            role=role,
            path=path,
            identity_sha256=_sha(f"{role}:schema-profile-v1"),
        ))
    body = build_worker_first_authority_bytes(
        consumer_id=CONSUMER,
        stream_id=STREAM,
        lock_path=lock_path,
        stores=stores,
    )
    path = (root / "worker-first-authority.json").resolve()
    path.write_bytes(body)
    return _Authority(
        root=root,
        path=path,
        body=body,
        sha256=hashlib.sha256(body).hexdigest(),
        lock_path=lock_path,
        stores=tuple(stores),
    )


def _spawn_hold(
    authority_path: str,
    authority_sha256: str,
    ready,
    release,
    result,
) -> None:
    try:
        fence = WorkerFirstOwnershipFence(
            authority_path,
            expected_authority_sha256=authority_sha256,
        )
        lease = fence.acquire()
        result.put(("held", lease.diagnostics()))
        ready.set()
        if not release.wait(20.0):
            result.put(("timeout", None))
        lease.release()
    except BaseException as error:
        result.put(("error", type(error).__name__, str(error)))
        ready.set()


def _spawn_attempt(
    authority_path: str,
    authority_sha256: str,
    result,
) -> None:
    try:
        fence = WorkerFirstOwnershipFence(
            authority_path,
            expected_authority_sha256=authority_sha256,
        )
        with fence.acquire() as lease:
            result.put(("acquired", lease.diagnostics()))
    except WorkerFirstFenceBusy as error:
        result.put(("busy", str(error)))
    except BaseException as error:
        result.put(("error", type(error).__name__, str(error)))


def _spawn_crash(
    authority_path: str,
    authority_sha256: str,
    ready,
) -> None:
    fence = WorkerFirstOwnershipFence(
        authority_path,
        expected_authority_sha256=authority_sha256,
    )
    fence.acquire()
    ready.set()
    os._exit(23)


def _close_queue(queue) -> None:
    queue.close()
    queue.join_thread()


def test_authority_is_canonical_exact_and_injection_is_privacy_safe(tmp_path):
    authority = _authority(tmp_path / "private-coordinate-50.99-token")
    document = json.loads(authority.body)
    assert document["contract"] == AUTHORITY_CONTRACT
    assert authority.body == json.dumps(
        document,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    assert [item["role"] for item in document["stores"]] == sorted(
        REQUIRED_STORE_ROLES
    )

    fence = WorkerFirstOwnershipFence(
        authority.path,
        expected_authority_sha256=authority.sha256,
    )
    with fence.acquire() as lease:
        assert lease.consumer_id == CONSUMER
        assert lease.stream_id == STREAM
        lease.assert_coordinator_binding(
            consumer_id=CONSUMER,
            stream_id=STREAM,
            stores=authority.injection,
        )
        diagnostics = json.dumps(lease.diagnostics(), sort_keys=True)
        assert str(authority.root) not in diagnostics
        assert CONSUMER not in diagnostics
        assert STREAM not in diagnostics
        assert "50.99" not in diagnostics
        assert "token" not in diagnostics.casefold()
        assert "secret" not in diagnostics.casefold()
        assert str(authority.root) not in repr(lease)
        assert CONSUMER not in repr(lease)

        bad = dict(authority.injection)
        path, _identity = bad["pipeline_output_store"]
        bad["pipeline_output_store"] = (path, "f" * 64)
        with pytest.raises(
            WorkerFirstAuthorityError,
            match="coordinator_binding_invalid",
        ) as mismatch:
            lease.assert_coordinator_binding(
                consumer_id=CONSUMER,
                stream_id=STREAM,
                stores=bad,
            )
        assert str(authority.root) not in str(mismatch.value)
        with pytest.raises(
            WorkerFirstAuthorityError,
            match="coordinator_binding_invalid",
        ):
            lease.assert_coordinator_binding(
                consumer_id="different-consumer",
                stream_id=STREAM,
                stores=authority.injection,
            )

    with pytest.raises(WorkerFirstFenceLost, match="ownership_not_held"):
        lease.assert_held()


def test_base_and_subclass_share_the_same_process_fence(tmp_path):
    authority = _authority(tmp_path)

    class SpecializedFence(WorkerFirstOwnershipFence):
        pass

    first = WorkerFirstOwnershipFence(
        authority.path,
        expected_authority_sha256=authority.sha256,
    )
    second = SpecializedFence(
        authority.path,
        expected_authority_sha256=authority.sha256,
    )
    lease = first.acquire()
    with pytest.raises(WorkerFirstFenceBusy, match="ownership_busy"):
        second.acquire()
    lease.release()
    with second.acquire() as successor:
        assert successor.diagnostics()["held"] is True


def test_two_spawned_processes_are_exclusive_then_release_and_reacquire(
    tmp_path,
):
    authority = _authority(tmp_path)
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    holder_result = context.Queue()
    contender_result = context.Queue()
    holder = context.Process(
        target=_spawn_hold,
        args=(
            str(authority.path),
            authority.sha256,
            ready,
            release,
            holder_result,
        ),
    )
    contender = context.Process(
        target=_spawn_attempt,
        args=(
            str(authority.path),
            authority.sha256,
            contender_result,
        ),
    )
    holder.start()
    try:
        assert ready.wait(10.0)
        held = holder_result.get(timeout=5.0)
        assert held[0] == "held", held
        contender.start()
        contender.join(10.0)
        assert contender.exitcode == 0
        busy = contender_result.get(timeout=5.0)
    finally:
        release.set()
        holder.join(10.0)
        if holder.is_alive():
            holder.terminate()
            holder.join(5.0)
        if contender.is_alive():
            contender.terminate()
            contender.join(5.0)
    assert holder.exitcode == 0
    assert busy == ("busy", "ownership_busy")

    reacquire_result = context.Queue()
    successor = context.Process(
        target=_spawn_attempt,
        args=(
            str(authority.path),
            authority.sha256,
            reacquire_result,
        ),
    )
    successor.start()
    successor.join(10.0)
    assert successor.exitcode == 0
    reacquired = reacquire_result.get(timeout=5.0)
    assert reacquired[0] == "acquired", reacquired

    for item in (held[1], reacquired[1]):
        encoded = json.dumps(item, sort_keys=True)
        assert str(authority.root) not in encoded
        assert CONSUMER not in encoded
        assert STREAM not in encoded
    _close_queue(holder_result)
    _close_queue(contender_result)
    _close_queue(reacquire_result)


def test_process_crash_releases_os_lock_without_stale_takeover(tmp_path):
    authority = _authority(tmp_path)
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    process = context.Process(
        target=_spawn_crash,
        args=(str(authority.path), authority.sha256, ready),
    )
    process.start()
    assert ready.wait(10.0)
    process.join(10.0)
    assert process.exitcode == 23

    fence = WorkerFirstOwnershipFence(
        authority.path,
        expected_authority_sha256=authority.sha256,
    )
    with fence.acquire() as lease:
        assert lease.diagnostics()["held"] is True
    assert authority.lock_path.read_bytes() == LOCK_ANCHOR_BYTES


def test_authority_checksum_scope_anchor_and_runtime_paths_fail_closed(tmp_path):
    wrong_sha = _authority(tmp_path / "wrong-sha")
    with pytest.raises(
        WorkerFirstAuthorityError,
        match="authority_checksum_mismatch",
    ):
        WorkerFirstOwnershipFence(
            wrong_sha.path,
            expected_authority_sha256="f" * 64,
        )

    tampered = _authority(tmp_path / "tampered-authority")
    tampered.path.write_bytes(tampered.body + b"\n")
    with pytest.raises(
        WorkerFirstAuthorityError,
        match="authority_checksum_mismatch",
    ):
        WorkerFirstOwnershipFence(
            tampered.path,
            expected_authority_sha256=tampered.sha256,
        )

    scope = _authority(tmp_path / "scope")
    document = json.loads(scope.body)
    document["authority_scope_sha256"] = "0" * 64
    body = json.dumps(
        document,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    scope.path.write_bytes(body)
    with pytest.raises(WorkerFirstAuthorityError, match="authority_scope_mismatch"):
        WorkerFirstOwnershipFence(
            scope.path,
            expected_authority_sha256=hashlib.sha256(body).hexdigest(),
        )

    anchor = _authority(tmp_path / "anchor")
    anchor.lock_path.write_bytes(b"tampered")
    anchor_fence = WorkerFirstOwnershipFence(
        anchor.path,
        expected_authority_sha256=anchor.sha256,
    )
    with pytest.raises(WorkerFirstAuthorityError, match="lock_anchor_invalid"):
        anchor_fence.acquire()

    replaced = _authority(tmp_path / "replaced-store")
    fence = WorkerFirstOwnershipFence(
        replaced.path,
        expected_authority_sha256=replaced.sha256,
    )
    store = fence.binding.store("pipeline_output_store").path
    original = store.with_suffix(".original")
    store.replace(original)
    store.write_bytes(b"replacement")
    with pytest.raises(
        WorkerFirstAuthorityError,
        match="authority_path_identity_changed",
    ):
        fence.acquire()


def test_relative_and_symlink_authority_paths_are_rejected(
    tmp_path,
    monkeypatch,
):
    authority = _authority(tmp_path)
    with pytest.raises(WorkerFirstAuthorityError, match="authority_path_invalid"):
        WorkerFirstOwnershipFence(
            Path("relative-authority.json"),
            expected_authority_sha256=authority.sha256,
        )

    link = tmp_path / "authority-link.json"
    try:
        link.symlink_to(authority.path)
    except (NotImplementedError, OSError):
        original = Path.is_symlink
        monkeypatch.setattr(
            Path,
            "is_symlink",
            lambda self: True if self == authority.path else original(self),
        )
        link = authority.path
    with pytest.raises(WorkerFirstAuthorityError, match="authority_path_invalid"):
        WorkerFirstOwnershipFence(
            link.absolute(),
            expected_authority_sha256=authority.sha256,
        )


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text("utf-8"), filename=str(path))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    return imports


def test_boundary_is_inert_and_not_live_wired():
    imports = _imports(MODULE)
    assert not any(
        name.startswith("homeassistant")
        or name.startswith("custom_components")
        or name in {
            "requests",
            "socket",
            "subprocess",
            "storm_tracker_worker.service",
            "storm_tracker_worker.worker_first_runtime",
        }
        for name in imports
    )
    candidate = "worker_first_ownership"
    assert candidate not in (
        ROOT / "storm_tracker_worker" / "service.py"
    ).read_text("utf-8")
    runtime_source = (
        ROOT / "storm_tracker_worker" / "worker_first_runtime.py"
    ).read_text("utf-8")
    assert "from .worker_first_ownership import REQUIRED_STORE_ROLES" in (
        runtime_source
    )


def test_posix_backend_requests_nonblocking_exclusive_flock(
    tmp_path,
    monkeypatch,
):
    calls: list[tuple[int, int]] = []
    fake_fcntl = SimpleNamespace(
        LOCK_EX=1,
        LOCK_NB=2,
        LOCK_UN=4,
        flock=lambda descriptor, operation: calls.append(
            (descriptor, operation)
        ),
    )
    fake_os = SimpleNamespace(
        name="posix",
        SEEK_SET=os.SEEK_SET,
        lseek=os.lseek,
    )
    monkeypatch.setitem(sys.modules, "fcntl", fake_fcntl)
    monkeypatch.setattr(ownership_module, "os", fake_os)
    path = tmp_path / "flock-byte"
    path.write_bytes(b"x")
    descriptor = os.open(path, os.O_RDWR)
    try:
        ownership_module._lock_descriptor(descriptor)
        ownership_module._unlock_descriptor(descriptor)
    finally:
        os.close(descriptor)
    assert calls == [
        (descriptor, fake_fcntl.LOCK_EX | fake_fcntl.LOCK_NB),
        (descriptor, fake_fcntl.LOCK_UN),
    ]
