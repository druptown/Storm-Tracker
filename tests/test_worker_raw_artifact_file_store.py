from __future__ import annotations

import hashlib
import os
from pathlib import Path
import subprocess
import sys

import pytest

from storm_tracker_worker import raw_artifacts
from storm_tracker_worker.raw_artifacts import (
    RawArtifactCapacityError,
    RawArtifactConflictError,
    RawArtifactIntegrityError,
    RawArtifactLimits,
    RawArtifactStore,
)


NOW = 1_800_000_000.0


def _limits(*, max_object_bytes: int = 8 * 1024 * 1024) -> RawArtifactLimits:
    return RawArtifactLimits(
        max_object_bytes=max_object_bytes,
        max_total_bytes=32 * 1024 * 1024,
        max_object_entries=16,
        max_staging_entries=4,
        max_receipt_entries=32,
        max_capture_entries=32,
        max_capture_members=4,
        max_catalog_bytes=8 * 1024 * 1024,
    )


def _provenance() -> dict[str, str]:
    return {
        "representation": "upstream_native_bytes",
        "capture_stage": "before_decode",
        "collector": "file-test",
        "collector_version": "file-test-v1",
    }


def _store_file(
    store: RawArtifactStore,
    source: Path,
    *,
    payload: bytes,
    fetch_id: str = "fetch-1",
    upstream_id: str | None = None,
):
    digest = hashlib.sha256(payload).hexdigest()
    return store.store_file(
        provider="file-test",
        product="exact-file",
        fetch_id=fetch_id,
        content_type="application/octet-stream",
        received_at=NOW,
        upstream_id=upstream_id or f"fixture|sha256={digest}",
        provenance=_provenance(),
        source_path=source,
        expected_size=len(payload),
        expected_sha256=digest,
    )


def test_store_file_publishes_exact_bytes_without_consuming_source(tmp_path):
    payload = bytes(range(251)) * 9_000
    source = (tmp_path / "download.part").resolve()
    source.write_bytes(payload)
    store = RawArtifactStore((tmp_path / "raw").resolve(), limits=_limits())
    store.initialize()

    receipt = _store_file(store, source, payload=payload)

    assert source.read_bytes() == payload
    assert store.read_bytes(receipt.receipt_id) == payload
    assert receipt.content_sha256 == hashlib.sha256(payload).hexdigest()
    assert receipt.size_bytes == len(payload)
    assert Path(receipt.artifact_path).parent == store.objects_path
    stats = store.stats()
    assert stats["object_count"] == 1
    assert stats["receipt_count"] == 1
    assert stats["filesystem_staging_entries"] == 0
    assert stats["unmanaged_file_count"] == 0
    assert store.integrity_check()["healthy"] is True


def test_store_file_reads_source_in_bounded_chunks(monkeypatch, tmp_path):
    payload = b"bounded-file-source" * 150_000
    source = (tmp_path / "bounded.part").resolve()
    source.write_bytes(payload)
    source_identity = (source.stat().st_dev, source.stat().st_ino)
    requests: list[int] = []
    real_read = raw_artifacts.os.read

    def observed_read(descriptor: int, size: int) -> bytes:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) == source_identity:
            requests.append(size)
        return real_read(descriptor, size)

    monkeypatch.setattr(raw_artifacts.os, "read", observed_read)
    store = RawArtifactStore((tmp_path / "raw").resolve(), limits=_limits())
    store.initialize()

    _store_file(store, source, payload=payload)

    assert requests
    assert max(requests) <= raw_artifacts.FILE_STORE_CHUNK_BYTES
    assert sum(requests[:-1]) >= len(payload)


def test_store_file_is_idempotent_and_reproves_the_supplied_source(tmp_path):
    payload = b"idempotent-file"
    source = (tmp_path / "idempotent.part").resolve()
    source.write_bytes(payload)
    store = RawArtifactStore((tmp_path / "raw").resolve(), limits=_limits())
    store.initialize()

    first = _store_file(store, source, payload=payload)
    second = _store_file(store, source, payload=payload)

    assert second == first
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 1
    source.write_bytes(b"idempotent-fail")
    assert len(source.read_bytes()) == len(payload)
    with pytest.raises(RawArtifactIntegrityError, match="SHA-256|identity"):
        _store_file(store, source, payload=payload)


@pytest.mark.parametrize("actual", (b"short", b"too-long-for-contract"))
def test_store_file_rejects_short_or_long_source_before_catalog_write(
    tmp_path,
    actual,
):
    expected = b"expected-size"
    source = (tmp_path / "wrong-size.part").resolve()
    source.write_bytes(actual)
    store = RawArtifactStore((tmp_path / "raw").resolve(), limits=_limits())
    store.initialize()

    with pytest.raises(RawArtifactIntegrityError, match="single-link identity"):
        _store_file(store, source, payload=expected)

    assert store.stats()["object_count"] == 0
    assert store.stats()["receipt_count"] == 0
    assert store.stats()["filesystem_staging_entries"] == 0


def test_store_file_rejects_wrong_sha_and_removes_ordinary_stage(tmp_path):
    actual = b"same-size-source"
    claimed = b"same-size-claims"
    assert len(actual) == len(claimed)
    source = (tmp_path / "wrong-sha.part").resolve()
    source.write_bytes(actual)
    store = RawArtifactStore((tmp_path / "raw").resolve(), limits=_limits())
    store.initialize()

    with pytest.raises(RawArtifactIntegrityError, match="SHA-256"):
        _store_file(store, source, payload=claimed)

    stats = store.stats()
    assert stats["object_count"] == 0
    assert stats["receipt_count"] == 0
    assert stats["filesystem_staging_entries"] == 0
    assert stats["unmanaged_file_count"] == 0


def test_store_file_rejects_source_replacement_after_descriptor_open(tmp_path):
    payload = b"replace-me-with-same-bytes"
    source = (tmp_path / "replace.part").resolve()
    source.write_bytes(payload)
    replaced = False

    def replace_at_stage_open(checkpoint: str) -> None:
        nonlocal replaced
        if checkpoint == "store_file_after_stage_open" and not replaced:
            replaced = True
            source.unlink()
            source.write_bytes(payload)

    store = RawArtifactStore(
        (tmp_path / "raw").resolve(),
        limits=_limits(),
        fault_injector=replace_at_stage_open,
    )
    store.initialize()

    with pytest.raises(RawArtifactIntegrityError, match="identity"):
        _store_file(store, source, payload=payload)

    assert replaced is True
    assert store.stats()["receipt_count"] == 0
    assert store.stats()["filesystem_staging_entries"] == 0


def test_store_file_rejects_hardlinked_source(tmp_path):
    payload = b"single-link-required"
    source = (tmp_path / "source.part").resolve()
    alias = (tmp_path / "source-alias.part").resolve()
    source.write_bytes(payload)
    try:
        os.link(source, alias)
    except OSError as exc:
        pytest.skip(f"hardlinks niet beschikbaar: {exc}")
    store = RawArtifactStore((tmp_path / "raw").resolve(), limits=_limits())
    store.initialize()

    with pytest.raises(RawArtifactIntegrityError, match="single-link identity"):
        _store_file(store, source, payload=payload)


def test_store_file_rejects_symlink_source(tmp_path):
    payload = b"no-follow"
    target = (tmp_path / "target.part").resolve()
    source = (tmp_path / "source-link.part").resolve()
    target.write_bytes(payload)
    try:
        source.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"symlinks niet beschikbaar: {exc}")
    store = RawArtifactStore((tmp_path / "raw").resolve(), limits=_limits())
    store.initialize()

    with pytest.raises(RawArtifactIntegrityError, match="single-link identity"):
        _store_file(store, source, payload=payload)


def test_store_file_rejects_source_inside_raw_store(tmp_path):
    store = RawArtifactStore((tmp_path / "raw").resolve(), limits=_limits())
    store.initialize()
    source = (store.root / "caller-source.part").resolve()
    source.write_bytes(b"not-external")

    with pytest.raises(RawArtifactIntegrityError, match="niet in de raw-store"):
        _store_file(store, source, payload=b"not-external")


def test_store_file_preserves_capacity_and_identity_conflict_fences(tmp_path):
    payload = b"12345678"
    source = (tmp_path / "capacity.part").resolve()
    source.write_bytes(payload)
    limited = RawArtifactStore(
        (tmp_path / "limited").resolve(),
        limits=_limits(max_object_bytes=4),
    )
    limited.initialize()
    with pytest.raises(RawArtifactCapacityError):
        _store_file(limited, source, payload=payload)

    store = RawArtifactStore((tmp_path / "raw").resolve(), limits=_limits())
    store.initialize()
    _store_file(store, source, payload=payload)
    other = (tmp_path / "other.part").resolve()
    other.write_bytes(b"ABCDEFGH")
    with pytest.raises(RawArtifactConflictError, match="upstream-id"):
        _store_file(
            store,
            other,
            payload=b"ABCDEFGH",
            fetch_id="fetch-2",
            upstream_id=f"fixture|sha256={hashlib.sha256(payload).hexdigest()}",
        )


@pytest.mark.parametrize(
    ("checkpoint", "expected_staging", "expected_receipts"),
    (
        ("store_file_after_stage_open", 1, 0),
        ("store_file_after_stage_fsync", 1, 0),
        ("store_file_after_object_link", 1, 0),
        ("store_after_object_publish", 0, 0),
        ("store_before_commit", 0, 0),
        ("store_after_commit", 0, 1),
    ),
)
def test_store_file_restart_recovers_and_cleans_exact_crash_stage(
    tmp_path,
    checkpoint,
    expected_staging,
    expected_receipts,
):
    payload = b"restart-safe-file-stage" * 10_000
    source = (tmp_path / "crash-source.part").resolve()
    source.write_bytes(payload)
    store_root = (tmp_path / checkpoint).resolve()
    helper = Path(__file__).resolve().parent / "helpers/raw_artifact_file_store_crash_child.py"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])

    result = subprocess.run(
        [sys.executable, str(helper), str(store_root), str(source), checkpoint],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 73, result.stderr.decode(errors="replace")

    reopened = RawArtifactStore(store_root, limits=_limits())
    before = reopened.initialize()
    assert before["filesystem_staging_entries"] == expected_staging
    assert before["receipt_count"] == expected_receipts
    receipt = _store_file(
        reopened,
        source,
        payload=payload,
        fetch_id="crash-fetch",
    )
    assert reopened.read_bytes(receipt.receipt_id) == payload
    after = reopened.stats()
    assert after["object_count"] == 1
    assert after["receipt_count"] == 1
    assert after["filesystem_staging_entries"] == 0
    assert after["unmanaged_file_count"] == 0
    assert reopened.integrity_check()["healthy"] is True
