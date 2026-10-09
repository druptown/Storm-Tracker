from __future__ import annotations

import ast
import hashlib
import io
import json
import os
from pathlib import Path
from typing import Iterator

import pytest

import descriptor_relative_authority as descriptor
import release_install_owned_output_reads as reads
import release_install_output_authority as output


JOURNAL_CONTRACT = "test-journal-v1"


def _path(value: str = "") -> descriptor.RelativePath:
    return descriptor.RelativePath.parse(value)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


@pytest.fixture
def authority_bundle(tmp_path: Path) -> Iterator[
    tuple[
        descriptor.DirectoryAuthority,
        output.ReleaseInstallOutputAuthority,
        descriptor.DirectoryAuthority,
        reads.OwnedOutputReadAuthority,
    ]
]:
    allowed = descriptor.DirectoryAuthority.open_absolute(tmp_path)
    run = output.ReleaseInstallOutputAuthority.create_run_root(
        allowed,
        _path("run"),
    )
    root = allowed.open_directory(_path("run"))
    reader = reads.OwnedOutputReadAuthority(root, run)
    try:
        yield allowed, run, root, reader
    finally:
        reader.close()
        root.close()
        run.close()
        allowed.close()


def _write_json(
    run: output.ReleaseInstallOutputAuthority,
    relative: str,
    document: object,
) -> None:
    run.write_bytes_noreplace(
        _path(relative),
        _canonical(document) + b"\n",
        forbidden_markers=(),
    )


def _journal_event(
    sequence: int,
    previous: str,
    name: str,
    payload: dict[str, object] | None = None,
) -> dict[str, object]:
    unsigned = {
        "contract": JOURNAL_CONTRACT,
        "event": name,
        "payload": payload or {},
        "previous_sha256": previous,
        "sequence": sequence,
    }
    return {
        **unsigned,
        "event_sha256": hashlib.sha256(_canonical(unsigned)).hexdigest(),
    }


def _file(path: str, value: bytes, mode: int = 0o644) -> reads.FileExpectation:
    return reads.FileExpectation(
        _path(path),
        len(value),
        hashlib.sha256(value).hexdigest(),
        mode,
    )


def _make_tree(
    run: output.ReleaseInstallOutputAuthority,
    relative: str,
    *,
    value: bytes,
    empty_directory: bool = True,
) -> reads.TreeExpectation:
    run.mkdir(_path(relative), mode=0o755)
    run.mkdir(_path(f"{relative}/nested"), mode=0o755)
    directories = [_path("nested")]
    if empty_directory:
        run.mkdir(_path(f"{relative}/empty"), mode=0o755)
        directories.append(_path("empty"))
    run.write_bytes_noreplace(
        _path(f"{relative}/root.txt"),
        value,
        mode=0o644,
        forbidden_markers=(),
    )
    run.write_bytes_noreplace(
        _path(f"{relative}/nested/tool.sh"),
        b"#!/bin/sh\n",
        mode=0o755,
        forbidden_markers=(),
    )
    return reads.TreeExpectation.create(
        "package",
        (
            _file("root.txt", value),
            _file("nested/tool.sh", b"#!/bin/sh\n", 0o755),
        ),
        directories,
    )


def test_identity_stability_ignores_only_mutable_directory_metadata():
    directory = descriptor.NodeIdentity(
        backend="posix",
        volume=7,
        file_id="abc",
        kind="directory",
        link_count=2,
        size=4096,
    )
    changed_directory_metadata = descriptor.NodeIdentity(
        backend="posix",
        volume=7,
        file_id="abc",
        kind="directory",
        link_count=19,
        size=32768,
    )
    regular_file = descriptor.NodeIdentity(
        backend="posix",
        volume=7,
        file_id="def",
        kind="regular_file",
        link_count=1,
        size=4,
    )

    assert reads._identity_stable(directory, changed_directory_metadata) is True
    assert reads._identity_stable(
        regular_file,
        descriptor.NodeIdentity(
            backend="posix",
            volume=7,
            file_id="def",
            kind="regular_file",
            link_count=1,
            size=5,
        ),
    ) is False
    assert reads._identity_stable(
        regular_file,
        descriptor.NodeIdentity(
            backend="posix",
            volume=7,
            file_id="def",
            kind="regular_file",
            link_count=2,
            size=4,
        ),
    ) is False


def test_reader_created_before_owned_mutations_reads_json_and_tree(authority_bundle):
    _allowed, run, _root, reader = authority_bundle
    _write_json(run, "state.json", {"ready": True})
    expected = _make_tree(run, "tree", value=b"baseline")

    assert reader.strict_json(_path("state.json"), label="state") == {
        "ready": True
    }
    assert reader.fingerprint_tree(_path("tree"), expected).to_mapping()[
        "content_sha256"
    ] == expected.content_sha256


def test_reader_still_rejects_mocked_root_replacement_after_owned_mutations(
    authority_bundle,
    monkeypatch: pytest.MonkeyPatch,
):
    _allowed, run, _root, reader = authority_bundle
    _write_json(run, "state.json", {"ready": True})
    identity = run.reprove_run_root()
    replacement = descriptor.NodeIdentity(
        backend=identity.backend,
        volume=identity.volume,
        file_id=f"{identity.file_id}ff",
        kind="directory",
        link_count=identity.link_count,
        size=identity.size,
    )

    monkeypatch.setattr(
        output.ReleaseInstallOutputAuthority,
        "reprove_run_root",
        lambda self: replacement,
    )

    with pytest.raises(reads.OwnedOutputReadChanged, match="root_identity_changed"):
        reader.strict_json(_path("state.json"), label="state")


@pytest.mark.skipif(os.name == "nt", reason="open directory rename is blocked on Windows")
def test_reader_rejects_actual_run_root_replacement_after_owned_mutations(
    tmp_path: Path,
):
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            with allowed.open_directory(_path("run")) as root:
                with reads.OwnedOutputReadAuthority(root, run) as reader:
                    _write_json(run, "state.json", {"ready": True})
                    (tmp_path / "run").rename(tmp_path / "original-run")
                    (tmp_path / "run").mkdir()

                    with pytest.raises(
                        reads.OwnedOutputReadChanged,
                        match="root_identity_changed",
                    ):
                        reader.strict_json(_path("state.json"), label="state")


def test_strict_json_reads_beneath_pinned_root_and_rejects_string_paths(
    authority_bundle,
):
    _allowed, run, _root, reader = authority_bundle
    _write_json(run, "state.json", {"ready": True, "sequence": 3})

    assert reader.strict_json(
        _path("state.json"),
        label="state",
    ) == {"ready": True, "sequence": 3}
    with pytest.raises(TypeError, match="RelativePath"):
        reader.strict_json("state.json", label="state")
    with pytest.raises(TypeError, match="RelativePath"):
        reader.node_kind("state.json")
    with pytest.raises(TypeError, match="RelativePath"):
        with reader.archive_stream("state.json", maximum_bytes=100):
            pass


@pytest.mark.parametrize(
    ("payload", "error_type", "message"),
    (
        (b'{"x":1,"x":2}', reads.OwnedOutputReadValidationError, "duplicate"),
        (b'{"x":1.5}', reads.OwnedOutputReadValidationError, "float"),
        (b'{"x":NaN}', reads.OwnedOutputReadValidationError, "nonfinite"),
        (
            b'{"x":2147483649}',
            reads.OwnedOutputReadLimitExceeded,
            "integer_limit",
        ),
        (b"[" * 65 + b"0" + b"]" * 65, reads.OwnedOutputReadLimitExceeded, "depth"),
        (b"\xff", reads.OwnedOutputReadValidationError, "strict_utf8"),
    ),
)
def test_strict_json_rejects_ambiguous_or_unbounded_documents(
    authority_bundle,
    payload: bytes,
    error_type: type[BaseException],
    message: str,
):
    _allowed, run, _root, reader = authority_bundle
    run.write_bytes_noreplace(
        _path("bad.json"),
        payload,
        forbidden_markers=(),
    )

    with pytest.raises(error_type, match=message):
        reader.strict_json(_path("bad.json"), label="bad")


def test_json_byte_bound_is_checked_from_pinned_identity(authority_bundle):
    _allowed, run, _root, reader = authority_bundle
    run.write_bytes_noreplace(
        _path("large.json"),
        b'{"value":"abcdef"}',
        forbidden_markers=(),
    )

    with pytest.raises(reads.OwnedOutputReadLimitExceeded, match="byte_limit"):
        reader.strict_json(
            _path("large.json"),
            label="large",
            maximum_bytes=4,
        )


def test_generic_bounded_read_binds_size_hash_and_utf8(authority_bundle):
    _allowed, run, _root, reader = authority_bundle
    payload = "build-id: integration-1.2.3\n".encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    run.write_bytes_noreplace(
        _path("config.yaml"),
        payload,
        forbidden_markers=(),
    )

    assert reader.read_bounded(
        _path("config.yaml"),
        maximum_bytes=len(payload),
        expected_size=len(payload),
        expected_sha256=digest,
    ) == payload
    assert reader.read_utf8_text(
        _path("config.yaml"),
        maximum_bytes=len(payload),
        expected_size=len(payload),
        expected_sha256=digest,
    ) == payload.decode("utf-8")

    with pytest.raises(reads.OwnedOutputReadLimitExceeded, match="byte_limit"):
        reader.read_bounded(_path("config.yaml"), maximum_bytes=1)
    with pytest.raises(reads.OwnedOutputReadValidationError, match="size_differs"):
        reader.read_bounded(
            _path("config.yaml"),
            maximum_bytes=len(payload),
            expected_size=len(payload) - 1,
        )
    with pytest.raises(reads.OwnedOutputReadValidationError, match="sha256_differs"):
        reader.read_bounded(
            _path("config.yaml"),
            maximum_bytes=len(payload),
            expected_sha256="0" * 64,
        )


def test_generic_bounded_read_rejects_backend_overreturn(
    authority_bundle,
    monkeypatch: pytest.MonkeyPatch,
):
    _allowed, run, _root, reader = authority_bundle
    run.write_bytes_noreplace(
        _path("config.yaml"),
        b"data",
        forbidden_markers=(),
    )
    monkeypatch.setattr(
        descriptor.BoundFile,
        "read_bounded",
        lambda _self, maximum_bytes: b"x" * (maximum_bytes + 1),
    )

    with pytest.raises(reads.OwnedOutputReadLimitExceeded, match="overreturned"):
        reader.read_bounded(_path("config.yaml"), maximum_bytes=4)


def test_generic_bounded_read_postidentity_failure_closes_the_bound_file(
    authority_bundle,
    monkeypatch: pytest.MonkeyPatch,
):
    _allowed, run, _root, reader = authority_bundle
    run.write_bytes_noreplace(
        _path("config.yaml"),
        b"data",
        forbidden_markers=(),
    )
    original_open = descriptor.DirectoryAuthority.open_file
    original_identity = descriptor.BoundFile.identity
    tracked: list[descriptor.BoundFile] = []
    tracked_identity_calls = 0

    def recording_open(self, relative, *, writable=False):
        bound = original_open(self, relative, writable=writable)
        if str(relative) == "config.yaml":
            tracked.append(bound)
        return bound

    def failing_postidentity(self):
        nonlocal tracked_identity_calls
        if tracked and self is tracked[-1]:
            tracked_identity_calls += 1
            if tracked_identity_calls == 2:
                raise descriptor.AuthorityIOError("injected-file-postidentity")
        return original_identity(self)

    monkeypatch.setattr(descriptor.DirectoryAuthority, "open_file", recording_open)
    monkeypatch.setattr(descriptor.BoundFile, "identity", failing_postidentity)

    with pytest.raises(reads.OwnedOutputReadIOError, match="postproof"):
        reader.read_bounded(_path("config.yaml"), maximum_bytes=4)
    assert tracked_identity_calls == 2
    assert len(tracked) == 1
    assert tracked[0].closed is True


@pytest.mark.parametrize(
    ("payload", "message"),
    (
        (b"\xef\xbb\xbftext", "bom_forbidden"),
        (b"\xff", "not_utf8"),
    ),
)
def test_utf8_text_rejects_bom_and_invalid_encoding(
    authority_bundle,
    payload: bytes,
    message: str,
):
    _allowed, run, _root, reader = authority_bundle
    run.write_bytes_noreplace(
        _path("text.txt"),
        payload,
        forbidden_markers=(),
    )

    with pytest.raises(reads.OwnedOutputReadValidationError, match=message):
        reader.read_utf8_text(
            _path("text.txt"),
            maximum_bytes=len(payload),
        )


def test_exact_case_is_required_even_on_case_sensitive_filesystems(authority_bundle):
    _allowed, run, _root, reader = authority_bundle
    _write_json(run, "State.json", {"ok": True})

    with pytest.raises(
        reads.OwnedOutputReadValidationError,
        match="case_alias",
    ):
        reader.strict_json(_path("state.json"), label="state")


def test_hardlink_alias_is_rejected(authority_bundle, tmp_path: Path):
    _allowed, run, _root, reader = authority_bundle
    _write_json(run, "state.json", {"ok": True})
    try:
        os.link(tmp_path / "run" / "state.json", tmp_path / "run" / "alias.json")
    except OSError as error:
        pytest.skip(f"hardlinks unavailable: {error}")

    with pytest.raises(reads.OwnedOutputReadValidationError):
        reader.strict_json(_path("state.json"), label="state")


def test_symlink_or_reparse_entry_is_rejected(authority_bundle, tmp_path: Path):
    _allowed, run, _root, reader = authority_bundle
    _write_json(run, "target.json", {"ok": True})
    try:
        os.symlink(
            tmp_path / "run" / "target.json",
            tmp_path / "run" / "alias.json",
        )
    except OSError as error:
        pytest.skip(f"symlinks unavailable: {error}")

    with pytest.raises(reads.OwnedOutputReadValidationError):
        reader.strict_json(_path("alias.json"), label="alias")


def test_node_kind_proves_absence_file_and_directory(authority_bundle):
    _allowed, run, _root, reader = authority_bundle
    run.mkdir(_path("directory"))
    run.write_bytes_noreplace(
        _path("file.bin"),
        b"value",
        forbidden_markers=(),
    )

    assert reader.node_kind(_path("missing")) == "absent"
    assert reader.node_kind(_path("missing/child")) == "absent"
    assert reader.node_kind(_path("directory")) == "directory"
    assert reader.node_kind(_path("file.bin")) == "regular_file"


def test_node_kind_absence_uses_one_before_and_after_snapshot(
    authority_bundle,
    monkeypatch: pytest.MonkeyPatch,
):
    _allowed, run, _root, reader = authority_bundle
    original = reads.OwnedOutputReadAuthority._list
    calls = 0

    def racing_list(self, directory, maximum_entries):
        nonlocal calls
        calls += 1
        if calls == 2:
            run.write_bytes_noreplace(
                _path("appeared"),
                b"value",
                forbidden_markers=(),
            )
        return original(self, directory, maximum_entries)

    monkeypatch.setattr(reads.OwnedOutputReadAuthority, "_list", racing_list)

    with pytest.raises(reads.OwnedOutputReadChanged) as captured:
        reader.node_kind(_path("appeared"))
    assert calls == 2
    messages: list[str] = []
    error: BaseException | None = captured.value
    while error is not None:
        messages.append(str(error))
        error = error.__cause__
    assert any("absence_changed" in message for message in messages)


def test_directory_identity_failure_after_open_closes_the_new_authority(
    authority_bundle,
    monkeypatch: pytest.MonkeyPatch,
):
    _allowed, run, _root, reader = authority_bundle
    run.mkdir(_path("nested"))
    _write_json(run, "nested/state.json", {"ok": True})
    original_open = descriptor.DirectoryAuthority.open_directory
    original_identity = descriptor.DirectoryAuthority.identity
    tracked: list[descriptor.DirectoryAuthority] = []
    failed = False

    def recording_open(self, relative=""):
        child = original_open(self, relative)
        if str(relative) == "nested":
            tracked.append(child)
        return child

    def failing_identity(self):
        nonlocal failed
        if tracked and self is tracked[-1] and not failed:
            failed = True
            raise descriptor.AuthorityIOError("injected-directory-identity")
        return original_identity(self)

    monkeypatch.setattr(descriptor.DirectoryAuthority, "open_directory", recording_open)
    monkeypatch.setattr(descriptor.DirectoryAuthority, "identity", failing_identity)

    with pytest.raises(reads.OwnedOutputReadIOError):
        reader.strict_json(_path("nested/state.json"), label="state")
    assert failed is True
    assert len(tracked) == 1
    assert tracked[0].closed is True


def test_file_identity_failure_after_open_closes_the_bound_file(
    authority_bundle,
    monkeypatch: pytest.MonkeyPatch,
):
    _allowed, run, _root, reader = authority_bundle
    _write_json(run, "state.json", {"ok": True})
    original_open = descriptor.DirectoryAuthority.open_file
    original_identity = descriptor.BoundFile.identity
    tracked: list[descriptor.BoundFile] = []
    failed = False

    def recording_open(self, relative, *, writable=False):
        bound = original_open(self, relative, writable=writable)
        if str(relative) == "state.json":
            tracked.append(bound)
        return bound

    def failing_identity(self):
        nonlocal failed
        if tracked and self is tracked[-1] and not failed:
            failed = True
            raise descriptor.AuthorityIOError("injected-file-identity")
        return original_identity(self)

    monkeypatch.setattr(descriptor.DirectoryAuthority, "open_file", recording_open)
    monkeypatch.setattr(descriptor.BoundFile, "identity", failing_identity)

    with pytest.raises(reads.OwnedOutputReadIOError):
        reader.strict_json(_path("state.json"), label="state")
    assert failed is True
    assert len(tracked) == 1
    assert tracked[0].closed is True


def test_tree_child_identity_failure_after_open_closes_the_child_authority(
    authority_bundle,
    monkeypatch: pytest.MonkeyPatch,
):
    _allowed, run, _root, reader = authority_bundle
    expected = _make_tree(run, "tree", value=b"baseline")
    original_open = descriptor.DirectoryAuthority.open_directory
    original_identity = descriptor.DirectoryAuthority.identity
    tracked: list[descriptor.DirectoryAuthority] = []
    failed = False

    def recording_open(self, relative=""):
        child = original_open(self, relative)
        if str(relative) == "nested":
            tracked.append(child)
        return child

    def failing_identity(self):
        nonlocal failed
        if tracked and self is tracked[-1] and not failed:
            failed = True
            raise descriptor.AuthorityIOError("injected-tree-directory-identity")
        return original_identity(self)

    monkeypatch.setattr(descriptor.DirectoryAuthority, "open_directory", recording_open)
    monkeypatch.setattr(descriptor.DirectoryAuthority, "identity", failing_identity)

    with pytest.raises(reads.OwnedOutputReadIOError):
        reader.fingerprint_tree(_path("tree"), expected)
    assert failed is True
    assert len(tracked) == 1
    assert tracked[0].closed is True


def test_archive_stream_is_a_read_only_duplicate_and_closes_after_use(
    authority_bundle,
):
    _allowed, run, _root, reader = authority_bundle
    payload = b"archive-payload"
    run.write_bytes_noreplace(
        _path("candidate.zip"),
        payload,
        forbidden_markers=(),
    )

    with reader.archive_stream(
        _path("candidate.zip"),
        maximum_bytes=len(payload),
        expected_size=len(payload),
    ) as stream:
        assert stream.read(len(payload)) == payload
        assert stream.writable() is False
        assert stream.seek(-7, os.SEEK_END) == len(payload) - 7
        assert stream.read(7) == b"payload"
        with pytest.raises(reads.OwnedOutputReadLimitExceeded, match="seek"):
            stream.seek(1, os.SEEK_END)
    assert stream.closed is True

    with pytest.raises(reads.OwnedOutputReadLimitExceeded):
        with reader.archive_stream(_path("candidate.zip"), maximum_bytes=1):
            pass
    with pytest.raises(reads.OwnedOutputReadValidationError, match="size_differs"):
        with reader.archive_stream(
            _path("candidate.zip"),
            maximum_bytes=len(payload),
            expected_size=len(payload) - 1,
        ):
            pass


@pytest.mark.parametrize("read_size", (-1, reads.READ_CHUNK_BYTES + 1))
def test_archive_duplicate_rejects_unbounded_requests_before_underlying_read(
    read_size: int,
):
    class RecordingStream(io.BytesIO):
        def __init__(self) -> None:
            super().__init__(b"payload")
            self.requests: list[int] = []

        def read(self, size: int = -1) -> bytes:
            self.requests.append(size)
            return super().read(size)

    underlying = RecordingStream()
    stream = reads._BoundedDuplicate(underlying, 7)
    try:
        with pytest.raises(reads.OwnedOutputReadLimitExceeded, match="request_limit"):
            stream.read(read_size)
        assert underlying.requests == []
    finally:
        stream.close()


def test_consumer_exception_still_runs_file_and_root_postproof(
    authority_bundle,
    monkeypatch: pytest.MonkeyPatch,
):
    _allowed, run, _root, reader = authority_bundle
    run.write_bytes_noreplace(
        _path("candidate.zip"),
        b"payload",
        forbidden_markers=(),
    )
    calls = 0
    original = output.ReleaseInstallOutputAuthority.reprove_run_root

    def recording_reproof(self):
        nonlocal calls
        calls += 1
        return original(self)

    monkeypatch.setattr(
        output.ReleaseInstallOutputAuthority,
        "reprove_run_root",
        recording_reproof,
    )

    def failing_consumer(stream):
        assert stream.read(1) == b"p"
        raise LookupError("consumer-failed")

    with pytest.raises(LookupError, match="consumer-failed"):
        reader.consume_archive(
            _path("candidate.zip"),
            failing_consumer,
            maximum_bytes=7,
        )
    assert calls >= 2


def test_postproof_failure_overrides_and_chains_consumer_exception(
    authority_bundle,
    monkeypatch: pytest.MonkeyPatch,
):
    _allowed, run, _root, reader = authority_bundle
    run.write_bytes_noreplace(
        _path("candidate.zip"),
        b"payload",
        forbidden_markers=(),
    )
    original = reads.OwnedOutputReadAuthority._exact_entry
    file_lookups = 0

    def fail_second_file_lookup(self, directory, name, maximum_entries, **kwargs):
        nonlocal file_lookups
        if name == "candidate.zip":
            file_lookups += 1
            if file_lookups == 2:
                raise reads.OwnedOutputReadChanged("injected-postproof")
        return original(self, directory, name, maximum_entries, **kwargs)

    monkeypatch.setattr(
        reads.OwnedOutputReadAuthority,
        "_exact_entry",
        fail_second_file_lookup,
    )

    def failing_consumer(_stream):
        raise LookupError("consumer-failed")

    with pytest.raises(reads.OwnedOutputReadChanged, match="injected-postproof") as captured:
        reader.consume_archive(
            _path("candidate.zip"),
            failing_consumer,
            maximum_bytes=7,
        )
    assert isinstance(captured.value.__cause__, LookupError)


def test_archive_consumer_mutation_is_detected_by_postproof(
    authority_bundle,
    tmp_path: Path,
):
    _allowed, run, _root, reader = authority_bundle
    run.write_bytes_noreplace(
        _path("candidate.zip"),
        b"payload",
        forbidden_markers=(),
    )

    def mutating_consumer(stream):
        assert stream.read(1) == b"p"
        with (tmp_path / "run" / "candidate.zip").open("ab") as mutable:
            mutable.write(b"-changed")
        return "returned"

    with pytest.raises(reads.OwnedOutputReadChanged):
        reader.consume_archive(
            _path("candidate.zip"),
            mutating_consumer,
            maximum_bytes=7,
        )


def test_journal_listing_read_and_hash_chain_are_canonical(authority_bundle):
    _allowed, run, _root, reader = authority_bundle
    run.mkdir(_path("journal"))
    first = _journal_event(1, "0" * 64, "initialized")
    second = _journal_event(2, first["event_sha256"], "prepared")
    _write_json(run, "journal/000001.json", first)
    _write_json(run, "journal/000002.json", second)
    run.write_bytes_noreplace(
        _path("journal/000003.0123456789abcdef0123456789abcdef.pending"),
        b"pending",
        forbidden_markers=(),
    )
    validated: list[tuple[str, ...]] = []

    result = reader.read_journal(
        _path("journal"),
        expected_contract=JOURNAL_CONTRACT,
        validate_sequence=lambda events: validated.append(
            tuple(str(event["event"]) for event in events)
        ),
    )

    assert tuple(event["event"] for event in result.events) == (
        "initialized",
        "prepared",
    )
    assert result.head_sha256 == second["event_sha256"]
    assert result.total_bytes > 0
    assert validated == [("initialized", "prepared")]


@pytest.mark.parametrize(
    ("filename", "mutator", "message"),
    (
        ("000002.json", None, "sequence_name"),
        ("000001.JSON", None, "suffix_case"),
        (
            "000001.json",
            lambda event: {**event, "event_sha256": "0" * 64},
            "hash_chain",
        ),
    ),
)
def test_journal_rejects_noncanonical_name_suffix_or_hash(
    authority_bundle,
    filename: str,
    mutator,
    message: str,
):
    _allowed, run, _root, reader = authority_bundle
    run.mkdir(_path("journal"))
    event = _journal_event(1, "0" * 64, "initialized")
    if mutator is not None:
        event = mutator(event)
    _write_json(run, f"journal/{filename}", event)

    with pytest.raises(reads.OwnedOutputReadValidationError, match=message):
        reader.read_journal(
            _path("journal"),
            expected_contract=JOURNAL_CONTRACT,
        )


def test_journal_rejects_unknown_nonfinal_entries(authority_bundle):
    _allowed, run, _root, reader = authority_bundle
    run.mkdir(_path("journal"))
    run.write_bytes_noreplace(
        _path("journal/notes.txt"),
        b"unexpected",
        forbidden_markers=(),
    )

    with pytest.raises(reads.OwnedOutputReadValidationError, match="not_canonical"):
        reader.read_journal(
            _path("journal"),
            expected_contract=JOURNAL_CONTRACT,
        )


def test_journal_bounds_cover_events_each_file_total_and_directory(authority_bundle):
    _allowed, run, _root, reader = authority_bundle
    run.mkdir(_path("journal"))
    event = _journal_event(1, "0" * 64, "initialized")
    _write_json(run, "journal/000001.json", event)

    with pytest.raises(reads.OwnedOutputReadLimitExceeded):
        reader.read_journal(
            _path("journal"),
            expected_contract=JOURNAL_CONTRACT,
            limits=reads.JournalReadLimits(maximum_events=0),
        )
    with pytest.raises(reads.OwnedOutputReadLimitExceeded):
        reader.read_journal(
            _path("journal"),
            expected_contract=JOURNAL_CONTRACT,
            limits=reads.JournalReadLimits(maximum_event_bytes=8),
        )
    with pytest.raises(reads.OwnedOutputReadLimitExceeded):
        reader.read_journal(
            _path("journal"),
            expected_contract=JOURNAL_CONTRACT,
            limits=reads.JournalReadLimits(maximum_total_bytes=8),
        )
    with pytest.raises(reads.OwnedOutputReadLimitExceeded):
        reader.read_journal(
            _path("journal"),
            expected_contract=JOURNAL_CONTRACT,
            limits=reads.JournalReadLimits(maximum_directory_entries=0),
        )


def test_many_journal_events_use_only_two_journal_directory_listings(
    authority_bundle,
    monkeypatch: pytest.MonkeyPatch,
):
    _allowed, run, _root, reader = authority_bundle
    run.mkdir(_path("journal"))
    previous = "0" * 64
    for sequence in range(1, 101):
        event = _journal_event(sequence, previous, f"event-{sequence}")
        _write_json(run, f"journal/{sequence:06d}.json", event)
        previous = event["event_sha256"]
    calls = 0
    journal_calls = 0
    original = descriptor.DirectoryAuthority.list_entries

    def recording_list(self, relative="", *, maximum_entries=descriptor.MAX_DIRECTORY_ENTRIES):
        nonlocal calls, journal_calls
        calls += 1
        entries = original(self, relative, maximum_entries=maximum_entries)
        if len(entries) == 100:
            journal_calls += 1
        return entries

    monkeypatch.setattr(descriptor.DirectoryAuthority, "list_entries", recording_list)

    result = reader.read_journal(
        _path("journal"),
        expected_contract=JOURNAL_CONTRACT,
        limits=reads.JournalReadLimits(maximum_events=100),
    )

    assert len(result.events) == 100
    assert calls == 4
    assert journal_calls == 2


def test_sequence_validator_exception_runs_directory_and_root_postproof(
    authority_bundle,
    monkeypatch: pytest.MonkeyPatch,
):
    _allowed, run, _root, reader = authority_bundle
    run.mkdir(_path("journal"))
    _write_json(
        run,
        "journal/000001.json",
        _journal_event(1, "0" * 64, "initialized"),
    )
    calls = 0
    original = output.ReleaseInstallOutputAuthority.reprove_run_root

    def recording(self):
        nonlocal calls
        calls += 1
        return original(self)

    monkeypatch.setattr(
        output.ReleaseInstallOutputAuthority,
        "reprove_run_root",
        recording,
    )

    def reject(_events):
        raise LookupError("sequence-rejected")

    with pytest.raises(LookupError, match="sequence-rejected"):
        reader.read_journal(
            _path("journal"),
            expected_contract=JOURNAL_CONTRACT,
            validate_sequence=reject,
        )
    assert calls >= 2


def test_recursive_tree_fingerprint_proves_files_modes_and_empty_directories(
    authority_bundle,
):
    _allowed, run, _root, reader = authority_bundle
    expected = _make_tree(run, "tree", value=b"baseline")

    fingerprint = reader.fingerprint_tree(_path("tree"), expected)

    assert fingerprint.content_sha256 == expected.content_sha256
    assert tuple(str(item) for item in fingerprint.directories) == (
        "empty",
        "nested",
    )
    assert tuple(str(item) for item in fingerprint.empty_directories) == ("empty",)
    assert {str(item.path): item.sha256 for item in fingerprint.files} == {
        "nested/tool.sh": hashlib.sha256(b"#!/bin/sh\n").hexdigest(),
        "root.txt": hashlib.sha256(b"baseline").hexdigest(),
    }
    evidence = fingerprint.to_mapping()
    assert evidence["explicit_empty_directories_proven"] is True
    assert evidence["single_link_files"] is True
    assert evidence["physical_mode_enforced"] is (os.name != "nt")
    assert evidence["logical_file_modes"] == [0o644, 0o755]


def test_flat_tree_uses_a_constant_number_of_directory_listings(
    authority_bundle,
    monkeypatch: pytest.MonkeyPatch,
):
    _allowed, run, _root, reader = authority_bundle
    run.mkdir(_path("flat"), mode=0o755)
    records: list[reads.FileExpectation] = []
    for index in range(24):
        name = f"file-{index:02d}.bin"
        value = f"value-{index}".encode("ascii")
        run.write_bytes_noreplace(
            _path(f"flat/{name}"),
            value,
            forbidden_markers=(),
        )
        records.append(_file(name, value))
    expected = reads.TreeExpectation.create("package", records)
    calls = 0
    original = descriptor.DirectoryAuthority.list_entries

    def recording_list(self, relative="", *, maximum_entries=descriptor.MAX_DIRECTORY_ENTRIES):
        nonlocal calls
        calls += 1
        return original(self, relative, maximum_entries=maximum_entries)

    monkeypatch.setattr(descriptor.DirectoryAuthority, "list_entries", recording_list)

    fingerprint = reader.fingerprint_tree(_path("flat"), expected)

    assert len(fingerprint.files) == 24
    assert calls == 4


def test_tree_expectation_mapping_roundtrips_and_is_sealed(authority_bundle):
    _allowed, run, _root, reader = authority_bundle
    expected = _make_tree(run, "tree", value=b"baseline")
    mapping = expected.to_mapping()

    assert reads.TreeExpectation.from_mapping(mapping) == expected
    corrupted = dict(mapping)
    corrupted["content_sha256"] = "0" * 64
    with pytest.raises(reads.OwnedOutputReadValidationError, match="seal"):
        reads.TreeExpectation.from_mapping(corrupted)
    assert reader.fingerprint_tree(
        _path("tree"),
        reads.TreeExpectation.from_mapping(mapping),
    ).total_bytes == expected.total_bytes

    forged = reads.TreeExpectation(
        expected.root_name,
        expected.files,
        expected.directories,
        expected.total_bytes,
        "0" * 64,
    )
    with pytest.raises(reads.OwnedOutputReadValidationError, match="seal"):
        reader.fingerprint_tree(_path("tree"), forged)


def test_tree_expectation_rejects_cross_platform_case_aliases():
    with pytest.raises(reads.OwnedOutputReadValidationError, match="case_alias"):
        reads.TreeExpectation.create(
            "package",
            (
                _file("Readme.txt", b"one"),
                _file("README.TXT", b"two"),
            ),
        )


def test_tree_fingerprint_rejects_wrong_bytes_unexpected_entries_and_tight_caps(
    authority_bundle,
):
    _allowed, run, _root, reader = authority_bundle
    expected = _make_tree(run, "tree", value=b"baseline")
    wrong = reads.TreeExpectation.create(
        "package",
        (
            _file("root.txt", b"different"),
            _file("nested/tool.sh", b"#!/bin/sh\n", 0o755),
        ),
        (_path("nested"), _path("empty")),
    )
    with pytest.raises(reads.OwnedOutputReadValidationError, match="differs"):
        reader.fingerprint_tree(_path("tree"), wrong)

    run.write_bytes_noreplace(
        _path("tree/unexpected.txt"),
        b"unexpected",
        forbidden_markers=(),
    )
    with pytest.raises(reads.OwnedOutputReadValidationError, match="differs"):
        reader.fingerprint_tree(_path("tree"), expected)
    with pytest.raises(reads.OwnedOutputReadLimitExceeded, match="entry_limit"):
        reader.fingerprint_tree(
            _path("tree"),
            expected,
            limits=reads.TreeReadLimits(maximum_entries=1),
        )
    with pytest.raises(reads.OwnedOutputReadLimitExceeded, match="total_limit"):
        reader.fingerprint_tree(
            _path("tree"),
            expected,
            limits=reads.TreeReadLimits(maximum_total_bytes=1),
        )


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode proof")
def test_tree_fingerprint_rejects_mode_changes(authority_bundle, tmp_path: Path):
    _allowed, run, _root, reader = authority_bundle
    expected = _make_tree(run, "tree", value=b"baseline")
    os.chmod(tmp_path / "run" / "tree" / "root.txt", 0o600)

    with pytest.raises(reads.OwnedOutputReadValidationError, match="differs"):
        reader.fingerprint_tree(_path("tree"), expected)


def test_sealed_tree_kind_reads_once_and_requires_unambiguous_seal(authority_bundle):
    _allowed, run, _root, reader = authority_bundle
    baseline = _make_tree(run, "tree", value=b"baseline")
    candidate = reads.TreeExpectation.create(
        "package",
        (
            _file("root.txt", b"candidate"),
            _file("nested/tool.sh", b"#!/bin/sh\n", 0o755),
        ),
        (_path("nested"), _path("empty")),
    )

    assert reader.sealed_tree_kind(_path("missing"), baseline, candidate) == "absent"
    assert reader.sealed_tree_kind(_path("tree"), baseline, candidate) == "baseline"
    with pytest.raises(reads.OwnedOutputReadValidationError, match="ambiguous"):
        reader.sealed_tree_kind(_path("tree"), baseline, baseline)


def test_generation_inventory_is_closed_world_and_layout_bounded(authority_bundle):
    _allowed, run, _root, reader = authority_bundle
    for container in ("install", "backup", "stage", "retained"):
        run.mkdir(_path(container))
    baseline = _make_tree(
        run,
        "install/integration.active",
        value=b"baseline",
    )
    candidate = _make_tree(
        run,
        "stage/integration.candidate",
        value=b"candidate",
    )
    valid = {("baseline", "absent", "candidate", "absent")}

    assert reader.generation_inventory(
        baseline={"integration": baseline},
        candidate={"integration": candidate},
        allowed_layouts=valid,
    ) == {"integration": ("baseline", "absent", "candidate", "absent")}

    run.write_bytes_noreplace(
        _path("backup/unexpected"),
        b"x",
        forbidden_markers=(),
    )
    with pytest.raises(reads.OwnedOutputReadValidationError, match="unexpected_slot"):
        reader.generation_inventory(
            baseline={"integration": baseline},
            candidate={"integration": candidate},
        )


def test_generation_inventory_rejects_wrong_phase_layout(authority_bundle):
    _allowed, run, _root, reader = authority_bundle
    for container in ("install", "backup", "stage", "retained"):
        run.mkdir(_path(container))
    baseline = _make_tree(
        run,
        "install/integration.active",
        value=b"baseline",
    )
    candidate = reads.TreeExpectation.create(
        "package",
        (_file("root.txt", b"candidate"),),
    )

    with pytest.raises(reads.OwnedOutputReadValidationError, match="layout_not_allowed"):
        reader.generation_inventory(
            baseline={"integration": baseline},
            candidate={"integration": candidate},
            allowed_layouts={("candidate", "absent", "absent", "absent")},
        )

    with pytest.raises(reads.OwnedOutputReadValidationError, match="allowed_layout"):
        reader.generation_inventory(
            baseline={"integration": baseline},
            candidate={"integration": candidate},
            allowed_layouts={
                ("baseline", "absent", "absent", "forbidden")
            },
        )


def test_reader_owns_only_its_duplicate_root_handle(authority_bundle):
    _allowed, run, root, reader = authority_bundle
    reader.close()

    assert reader.closed is True
    assert root.closed is False
    assert run.closed is False
    assert run.reprove_run_root().same_object(root.identity())


def test_constructor_failure_closes_its_new_duplicate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            with allowed.open_directory(_path("run")) as root:
                original_duplicate = descriptor.DirectoryAuthority.duplicate
                original_reproof = output.ReleaseInstallOutputAuthority.reprove_run_root
                tracked: list[descriptor.DirectoryAuthority] = []

                def recording_duplicate(self):
                    duplicate = original_duplicate(self)
                    tracked.append(duplicate)
                    return duplicate

                def failing_reproof(self):
                    raise output.OutputAuthorityIOError("injected-root-reproof")

                monkeypatch.setattr(
                    descriptor.DirectoryAuthority,
                    "duplicate",
                    recording_duplicate,
                )
                monkeypatch.setattr(
                    output.ReleaseInstallOutputAuthority,
                    "reprove_run_root",
                    failing_reproof,
                )

                with pytest.raises(reads.OwnedOutputReadChanged):
                    reads.OwnedOutputReadAuthority(root, run)
                assert len(tracked) == 1
                assert tracked[0].closed is True

                monkeypatch.setattr(
                    output.ReleaseInstallOutputAuthority,
                    "reprove_run_root",
                    original_reproof,
                )


def test_constructor_rejects_root_bound_to_another_output(tmp_path: Path):
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run-a"),
        ) as run_a:
            with output.ReleaseInstallOutputAuthority.create_run_root(
                allowed,
                _path("run-b"),
            ) as run_b:
                with allowed.open_directory(_path("run-a")) as root_a:
                    with pytest.raises(
                        reads.OwnedOutputReadValidationError,
                        match="binding_differs",
                    ):
                        reads.OwnedOutputReadAuthority(root_a, run_b)
                assert run_a.reprove_run_root().kind == "directory"


def test_module_has_no_path_reopen_or_lexical_tree_read_and_claims_no_closure():
    source = Path(reads.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    bare_calls = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    called_attributes = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }

    assert "pathlib" not in imported_names
    assert "open" not in bare_calls
    assert not {
        "walk",
        "glob",
        "iterdir",
        "read_bytes",
        "lstat",
        "open_absolute",
    } & called_attributes
    evidence = reads.owned_output_read_evidence()
    assert evidence["absolute_path_reopen"] is False
    assert evidence["consumer_exception_postproof"] is True
    assert evidence["simulator_integration_proven"] is False
    assert evidence["local_acceptance_blockers_closed"] == []
