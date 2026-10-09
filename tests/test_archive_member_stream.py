"""Exact bounded-stream tests for ZIP/TAR member bodies."""
from __future__ import annotations

import hashlib
import io
import itertools
import os
from pathlib import Path

import pytest

import storm_tracker_worker.archive_member_stream as stream_module
from storm_tracker_worker.archive_member_stream import (
    ARCHIVE_MEMBER_CHUNK_BYTES,
    ARCHIVE_MEMBER_STREAM_CONTRACT,
    ArchiveMemberStreamError,
    PRIVATE_KEY_MARKERS,
    stream_archive_member,
    streaming_archive_member_evidence,
)


class _RecordingReader:
    def __init__(self, value: bytes) -> None:
        self._source = io.BytesIO(value)
        self.requests: list[int] = []

    def read(self, size: int) -> bytes:
        self.requests.append(size)
        return self._source.read(size)


def test_private_key_markers_are_exact_without_source_self_reference():
    expected = (
        b"-----BEGIN PRIVATE KEY-----",
        b"-----BEGIN RSA PRIVATE KEY-----",
        b"-----BEGIN EC PRIVATE KEY-----",
        b"-----BEGIN OPENSSH PRIVATE KEY-----",
    )

    assert PRIVATE_KEY_MARKERS == expected
    source = stream_module.__file__
    assert source is not None
    source_bytes = Path(source).read_bytes()
    assert all(marker not in source_bytes for marker in PRIVATE_KEY_MARKERS)


def test_member_body_is_hashed_with_only_bounded_reads():
    value = b"a" * (ARCHIVE_MEMBER_CHUNK_BYTES * 2 + 17)
    source = _RecordingReader(value)
    result = stream_archive_member(
        source,
        expected_size=len(value),
        maximum_size=len(value),
        expected_sha256=hashlib.sha256(value).hexdigest(),
    )
    assert result.size == len(value)
    assert result.sha256 == hashlib.sha256(value).hexdigest()
    assert result.read_calls == 4
    assert source.requests == [
        ARCHIVE_MEMBER_CHUNK_BYTES,
        ARCHIVE_MEMBER_CHUNK_BYTES,
        17,
        1,
    ]


def test_declared_size_bound_fails_before_the_first_read():
    source = _RecordingReader(b"abcd")
    with pytest.raises(ArchiveMemberStreamError, match="size_bound_invalid"):
        stream_archive_member(source, expected_size=4, maximum_size=3)
    assert source.requests == []


@pytest.mark.parametrize(
    ("value", "expected_size", "code"),
    ((b"abc", 4, "body_truncated"), (b"abcd", 3, "body_exceeds_bound")),
)
def test_truncated_and_overlong_bodies_fail_closed(
    value: bytes,
    expected_size: int,
    code: str,
):
    with pytest.raises(ArchiveMemberStreamError, match=code):
        stream_archive_member(
            io.BytesIO(value),
            expected_size=expected_size,
            maximum_size=expected_size,
            chunk_bytes=2,
        )


def test_forbidden_marker_is_detected_across_a_chunk_boundary():
    value = b"abc---secret---xyz"
    with pytest.raises(ArchiveMemberStreamError, match="forbidden_bytes"):
        stream_archive_member(
            io.BytesIO(value),
            expected_size=len(value),
            maximum_size=len(value),
            forbidden_markers=(b"secret",),
            chunk_bytes=8,
        )


def test_marker_iterable_is_consumed_only_to_its_exact_bound():
    consumed = 0

    def markers():
        nonlocal consumed
        for number in itertools.count():
            consumed += 1
            yield f"marker-{number}".encode("ascii")

    with pytest.raises(ArchiveMemberStreamError, match="markers_invalid"):
        stream_archive_member(
            io.BytesIO(b""),
            expected_size=0,
            maximum_size=0,
            forbidden_markers=markers(),
        )
    assert consumed == stream_module.MAX_FORBIDDEN_MARKERS + 1


def test_partial_descriptor_writes_are_completed(monkeypatch, tmp_path):
    value = b"partial-write-proof"
    output = tmp_path / "output.bin"
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    original_write = stream_module.os.write
    calls = 0

    def partial_write(target: int, data) -> int:
        nonlocal calls
        calls += 1
        return original_write(target, bytes(data[:2]))

    monkeypatch.setattr(stream_module.os, "write", partial_write)
    try:
        result = stream_archive_member(
            io.BytesIO(value),
            expected_size=len(value),
            maximum_size=len(value),
            target_descriptor=descriptor,
            chunk_bytes=len(value),
        )
    finally:
        os.close(descriptor)
    assert calls > 1
    assert result.write_calls == calls
    assert output.read_bytes() == value


def test_zero_progress_descriptor_write_fails_closed(monkeypatch, tmp_path):
    output = tmp_path / "stalled.bin"
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    monkeypatch.setattr(stream_module.os, "write", lambda _target, _data: 0)
    try:
        with pytest.raises(ArchiveMemberStreamError, match="target_write_stalled"):
            stream_archive_member(
                io.BytesIO(b"x"),
                expected_size=1,
                maximum_size=1,
                target_descriptor=descriptor,
            )
    finally:
        os.close(descriptor)


def test_read_exception_and_hash_mismatch_are_fixed_domain_errors():
    class FailedReader:
        def read(self, _size: int) -> bytes:
            raise OSError("secret source detail")

    with pytest.raises(ArchiveMemberStreamError, match="source_read_failed") as raised:
        stream_archive_member(FailedReader(), expected_size=1, maximum_size=1)
    assert "secret source detail" not in str(raised.value)

    with pytest.raises(ArchiveMemberStreamError, match="sha256_mismatch"):
        stream_archive_member(
            io.BytesIO(b"x"),
            expected_size=1,
            maximum_size=1,
            expected_sha256="0" * 64,
        )

    class FailedOverrunProbe:
        def __init__(self) -> None:
            self.calls = 0

        def read(self, _size: int) -> bytes:
            self.calls += 1
            if self.calls == 1:
                return b"x"
            raise RuntimeError("secret overrun detail")

    with pytest.raises(ArchiveMemberStreamError, match="source_read_failed") as raised:
        stream_archive_member(FailedOverrunProbe(), expected_size=1, maximum_size=1)
    assert "secret overrun detail" not in str(raised.value)


def test_streaming_evidence_is_exact_and_positive():
    assert streaming_archive_member_evidence() == {
        "actual_size_reproved": True,
        "chunk_bytes": ARCHIVE_MEMBER_CHUNK_BYTES,
        "contract": ARCHIVE_MEMBER_STREAM_CONTRACT,
        "forbidden_marker_overlap_reproved": True,
        "member_sha256_reproved": True,
        "overrun_probe_bytes": 1,
        "partial_writes_handled": True,
        "whole_member_materialization": False,
    }
