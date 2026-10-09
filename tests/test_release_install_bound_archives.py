from __future__ import annotations

import ast
from contextlib import contextmanager
from dataclasses import replace
import gzip
import hashlib
import io
from pathlib import Path
import stat
import tarfile
from typing import Iterator
import zipfile

import pytest

import descriptor_relative_authority as authority
import release_install_bound_archives as archives
from release_install_bound_inputs import (
    BoundInputContractError,
    BoundInputDeclaration,
    BoundInputIntegrityError,
    BoundInputMetadata,
    BoundInputRegistry,
)
from storm_tracker_worker.archive_member_stream import ARCHIVE_MEMBER_CHUNK_BYTES
from storm_tracker_worker.bounded_archive_headers import ArchiveHeaderLimits


ROOT_NAME = "package"
PAYLOADS = {
    "a.txt": b"alpha\n",
    "nested/run.sh": b"#!/bin/sh\necho bounded\n",
}


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _tree(payloads: dict[str, bytes] = PAYLOADS) -> archives.BoundArchiveTree:
    return archives.BoundArchiveTree.create(
        ROOT_NAME,
        (
            archives.BoundArchiveFile(
                path,
                len(value),
                _sha(value),
                0o755 if path.endswith(".sh") else 0o644,
            )
            for path, value in payloads.items()
        ),
    )


def _zip_bytes(
    payloads: dict[str, bytes] = PAYLOADS,
    *,
    order: tuple[str, ...] | None = None,
    file_mode: int | None = None,
    explicit_directories: tuple[str, ...] = (),
    archive_comment: bytes = b"",
    extra: bytes = b"",
) -> bytes:
    target = io.BytesIO()
    with zipfile.ZipFile(
        target,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        root = zipfile.ZipInfo(f"{ROOT_NAME}/", (1980, 1, 1, 0, 0, 0))
        root.create_system = 3
        root.external_attr = (stat.S_IFDIR | 0o755) << 16
        archive.writestr(root, b"")
        for directory in explicit_directories:
            info = zipfile.ZipInfo(
                f"{ROOT_NAME}/{directory}/",
                (1980, 1, 1, 0, 0, 0),
            )
            info.create_system = 3
            info.external_attr = (stat.S_IFDIR | 0o755) << 16
            archive.writestr(info, b"")
        selected = order if order is not None else tuple(sorted(payloads))
        for path in selected:
            info = zipfile.ZipInfo(
                f"{ROOT_NAME}/{path}",
                (1980, 1, 1, 0, 0, 0),
            )
            info.create_system = 3
            mode = file_mode if file_mode is not None else (
                0o755 if path.endswith(".sh") else 0o644
            )
            info.external_attr = (stat.S_IFREG | mode) << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            info.extra = extra
            archive.writestr(info, payloads[path])
        archive.comment = archive_comment
    return target.getvalue()


def _tar_bytes(
    payloads: dict[str, bytes] = PAYLOADS,
    *,
    order: tuple[str, ...] | None = None,
    file_mode: int | None = None,
    explicit_directories: tuple[str, ...] | None = None,
    symlink: bool = False,
    pax: bool = False,
) -> bytes:
    raw_tar = io.BytesIO()
    selected_format = tarfile.PAX_FORMAT if pax else tarfile.USTAR_FORMAT
    with tarfile.open(fileobj=raw_tar, mode="w", format=selected_format) as archive:
        root = tarfile.TarInfo(f"{ROOT_NAME}/")
        root.type = tarfile.DIRTYPE
        root.mode = 0o755
        root.uid = root.gid = 0
        root.uname = root.gname = "root"
        root.mtime = 0
        archive.addfile(root)
        directories = (
            _tree(payloads).directories
            if explicit_directories is None
            else explicit_directories
        )
        for directory in directories:
            info = tarfile.TarInfo(f"{ROOT_NAME}/{directory}/")
            info.type = tarfile.DIRTYPE
            info.mode = 0o755
            info.uid = info.gid = 0
            info.uname = info.gname = "root"
            info.mtime = 0
            archive.addfile(info)
        selected = order if order is not None else tuple(sorted(payloads))
        for path in selected:
            info = tarfile.TarInfo(f"{ROOT_NAME}/{path}")
            info.type = tarfile.SYMTYPE if symlink else tarfile.REGTYPE
            info.linkname = "elsewhere" if symlink else ""
            info.mode = file_mode if file_mode is not None else (
                0o755 if path.endswith(".sh") else 0o644
            )
            info.uid = info.gid = 0
            info.uname = info.gname = "root"
            info.mtime = 0
            info.size = 0 if symlink else len(payloads[path])
            if pax:
                info.pax_headers = {"comment": "unsupported"}
            archive.addfile(info, None if symlink else io.BytesIO(payloads[path]))
    compressed = io.BytesIO()
    with gzip.GzipFile(
        filename="",
        mode="wb",
        fileobj=compressed,
        compresslevel=9,
        mtime=0,
    ) as stream:
        stream.write(raw_tar.getvalue())
    value = compressed.getvalue()
    assert value[:10] == archives.CANONICAL_GZIP_HEADER
    return value


def _noncanonical_gzip(value: bytes) -> bytes:
    changed = bytearray(value)
    changed[4:8] = (1).to_bytes(4, "little")
    return bytes(changed)


@contextmanager
def _registry(
    tmp_path: Path,
    value: bytes,
    *,
    key: str = "archive",
    extra: tuple[tuple[str, bytes], ...] = (),
) -> Iterator[BoundInputRegistry]:
    archive_path = tmp_path / f"{key}.bin"
    archive_path.write_bytes(value)
    for extra_key, extra_value in extra:
        (tmp_path / f"{extra_key}.bin").write_bytes(extra_value)
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        declarations: dict[str, BoundInputDeclaration] = {}
        for selected_key, selected_value in ((key, value), *extra):
            relative = authority.RelativePath.parse(f"{selected_key}.bin")
            identity = root.entry_identity(relative)
            declarations[selected_key] = BoundInputDeclaration(
                relative=relative,
                identity=identity,
                size=len(selected_value),
                sha256=_sha(selected_value),
                maximum_bytes=max(1, len(selected_value)),
            )
        with BoundInputRegistry.seal(
            root,
            declarations,
            expected_keys=tuple(declarations),
        ) as registry:
            yield registry


class _ObservedSource:
    def __init__(self, registry: BoundInputRegistry) -> None:
        self.registry = registry
        self.open_calls = 0
        self.exit_calls = 0
        self.stream_ids: list[int] = []

    @property
    def keys(self):
        return self.registry.keys

    def require_exact_keys(self, values):
        return self.registry.require_exact_keys(values)

    def metadata(self, key):
        return self.registry.metadata(key)

    @contextmanager
    def open_stream(self, key):
        self.open_calls += 1
        try:
            with self.registry.open_stream(key) as stream:
                self.stream_ids.append(id(stream))
                yield stream
        finally:
            self.exit_calls += 1

    def read_bounded(self, key, *, maximum_bytes):
        return self.registry.read_bounded(key, maximum_bytes=maximum_bytes)

    def read_json_object(self, key, *, maximum_bytes, expected_keys):
        return self.registry.read_json_object(
            key,
            maximum_bytes=maximum_bytes,
            expected_keys=expected_keys,
        )


class _OuterReadGuard:
    def __init__(self, source, requests: list[int]) -> None:
        self.source = source
        self.requests = requests

    def read(self, size=-1):
        assert type(size) is int
        assert 0 <= size <= ARCHIVE_MEMBER_CHUNK_BYTES
        self.requests.append(size)
        chunk = self.source.read(size)
        assert type(chunk) is bytes
        assert len(chunk) <= size
        return chunk

    def seek(self, offset, whence=0):
        return self.source.seek(offset, whence)

    def tell(self):
        return self.source.tell()


class _GuardedObservedSource(_ObservedSource):
    def __init__(self, registry: BoundInputRegistry) -> None:
        super().__init__(registry)
        self.read_requests: list[int] = []

    @contextmanager
    def open_stream(self, key):
        self.open_calls += 1
        try:
            with self.registry.open_stream(key) as stream:
                guarded = _OuterReadGuard(stream, self.read_requests)
                self.stream_ids.append(id(guarded))
                yield guarded
        finally:
            self.exit_calls += 1


class _MalformedOuterStream:
    def __init__(self, source, behavior: str) -> None:
        self.source = source
        self.behavior = behavior

    def read(self, size=-1):
        if self.behavior == "over_return":
            return b"x" * (size + 1)
        if self.behavior == "nonbytes":
            return bytearray(size)
        raise OSError("injected outer read failure")

    def seek(self, offset, whence=0):
        return self.source.seek(offset, whence)

    def tell(self):
        return self.source.tell()


class _MalformedObservedSource(_ObservedSource):
    def __init__(self, registry: BoundInputRegistry, behavior: str) -> None:
        super().__init__(registry)
        self.behavior = behavior

    @contextmanager
    def open_stream(self, key):
        self.open_calls += 1
        try:
            with self.registry.open_stream(key) as stream:
                yield _MalformedOuterStream(stream, self.behavior)
        finally:
            self.exit_calls += 1


@pytest.mark.parametrize(
    "archive_size",
    (17, ARCHIVE_MEMBER_CHUNK_BYTES + 1),
)
def test_outer_wrapper_rejects_read_minus_one_for_every_archive_size(
    archive_size: int,
):
    raw = io.BytesIO(b"x" * min(archive_size, 32))
    stream = archives._BoundedContainerStream(raw, archive_size)
    with pytest.raises(
        archives.BoundArchiveIntegrityError,
        match="read_bound_exceeded",
    ):
        stream.read(-1)
    assert raw.tell() == 0


def _sink_values():
    observed: dict[str, bytes] = {}

    def sink(record, source):
        chunks: list[bytes] = []
        while True:
            chunk = source.read(3)
            if not chunk:
                break
            chunks.append(chunk)
        observed[record.path] = b"".join(chunks)

    return observed, sink


def _small_limits(*, total: int, member: int | None = None) -> ArchiveHeaderLimits:
    maximum_member = total if member is None else member
    member_count = 32
    end_padding = 1024 * 1024
    return ArchiveHeaderLimits(
        max_archive_bytes=8 * 1024 * 1024,
        max_members=member_count,
        max_member_uncompressed_bytes=maximum_member,
        max_total_uncompressed_bytes=total,
        max_tar_stream_bytes=(
            total + (2 * member_count + 2) * 512 + end_padding
        ),
        max_tar_end_padding_bytes=end_padding,
    )


@pytest.mark.parametrize("kind", ("zip", "tar"))
def test_candidate_profiles_validate_inventory_order_modes_and_bodies(
    tmp_path: Path,
    kind: str,
):
    value = _zip_bytes() if kind == "zip" else _tar_bytes()
    with _registry(tmp_path, value) as registry:
        if kind == "zip":
            result = archives.validate_candidate_zip(registry, "archive", _tree())
            assert result.gzip_header_policy is None
        else:
            result = archives.validate_candidate_tar_gz(
                registry,
                "archive",
                _tree(),
            )
            assert result.gzip_header_policy == "canonical"

    assert result.format == ("zip" if kind == "zip" else "tar.gz")
    assert result.tree == _tree()
    assert result.header_index.total_uncompressed_bytes == _tree().total_bytes


@pytest.mark.parametrize("kind", ("zip", "tar"))
def test_candidate_noncanonical_order_is_rejected(tmp_path: Path, kind: str):
    reverse = tuple(reversed(sorted(PAYLOADS)))
    value = (
        _zip_bytes(order=reverse)
        if kind == "zip"
        else _tar_bytes(order=reverse)
    )
    with _registry(tmp_path, value) as registry:
        call = (
            archives.validate_candidate_zip
            if kind == "zip"
            else archives.validate_candidate_tar_gz
        )
        with pytest.raises(archives.BoundArchiveIntegrityError, match="order_differs"):
            call(registry, "archive", _tree())


@pytest.mark.parametrize("kind", ("zip", "tar"))
def test_candidate_wrong_mode_is_rejected(tmp_path: Path, kind: str):
    value = (
        _zip_bytes(file_mode=0o600)
        if kind == "zip"
        else _tar_bytes(file_mode=0o600)
    )
    with _registry(tmp_path, value) as registry:
        call = (
            archives.validate_candidate_zip
            if kind == "zip"
            else archives.validate_candidate_tar_gz
        )
        with pytest.raises(
            archives.BoundArchiveIntegrityError,
            match="file_metadata_differs",
        ):
            call(registry, "archive", _tree())


def test_candidate_zip_comment_and_extra_are_rejected(tmp_path: Path):
    for index, value in enumerate(
        (
            _zip_bytes(archive_comment=b"comment"),
            _zip_bytes(extra=b"\x02\x00\x00\x00"),
        )
    ):
        directory = tmp_path / str(index)
        directory.mkdir()
        with _registry(directory, value) as registry:
            with pytest.raises(archives.BoundArchiveIntegrityError):
                archives.validate_candidate_zip(registry, "archive", _tree())


def test_candidate_tar_requires_exact_canonical_gzip_header(tmp_path: Path):
    value = _noncanonical_gzip(_tar_bytes())
    with _registry(tmp_path, value) as registry:
        with pytest.raises(
            archives.BoundArchiveIntegrityError,
            match="legacy_gzip_header_not_exactly_allowlisted",
        ):
            archives.validate_candidate_tar_gz(registry, "archive", _tree())


@pytest.mark.parametrize("kind", ("zip", "tar"))
def test_candidate_private_marker_split_across_chunks_is_rejected(
    tmp_path: Path,
    kind: str,
):
    marker = b"-----BEGIN PRIVATE KEY-----"
    payloads = {
        "secret.txt": b"x" * (ARCHIVE_MEMBER_CHUNK_BYTES - 10) + marker + b"suffix"
    }
    value = _zip_bytes(payloads) if kind == "zip" else _tar_bytes(payloads)
    with _registry(tmp_path, value) as registry:
        call = (
            archives.validate_candidate_zip
            if kind == "zip"
            else archives.validate_candidate_tar_gz
        )
        with pytest.raises(
            archives.BoundArchiveMemberError,
            match="stream_contract_failed",
        ):
            call(registry, "archive", _tree(payloads))


def test_legacy_zip_streams_records_modes_and_empty_directories(tmp_path: Path):
    value = _zip_bytes(explicit_directories=("empty", "nested"))
    with _registry(tmp_path, value) as registry:
        result = archives.inspect_legacy_zip(registry, "archive", ROOT_NAME)

    assert result.tree == archives.BoundArchiveTree.create(
        ROOT_NAME,
        _tree().files,
        ("empty", "nested"),
    )
    assert {record.sha256 for record in result.tree.files} == {
        _sha(value) for value in PAYLOADS.values()
    }
    assert {0o644, 0o755}.issubset(
        {item.mode for item in result.observed_modes}
    )
    assert {
        (item.archive_path, item.kind)
        for item in result.observed_modes
    } == {
        (ROOT_NAME, "directory"),
        (f"{ROOT_NAME}/empty", "directory"),
        (f"{ROOT_NAME}/nested", "directory"),
        (f"{ROOT_NAME}/a.txt", "file"),
        (f"{ROOT_NAME}/nested/run.sh", "file"),
    }


def test_legacy_tar_noncanonical_header_needs_exact_archive_sha(tmp_path: Path):
    value = _noncanonical_gzip(
        _tar_bytes(explicit_directories=("empty", "nested"))
    )
    with _registry(tmp_path, value) as registry:
        result = archives.inspect_legacy_tar_gz(
            registry,
            "archive",
            ROOT_NAME,
            exact_archive_sha256=_sha(value),
        )
        assert result.gzip_header_policy == "legacy_exact_archive_sha256"
        assert "empty" in result.tree.directories

        with pytest.raises(
            archives.BoundArchiveIntegrityError,
            match="not_exactly_allowlisted",
        ):
            archives.inspect_legacy_tar_gz(
                registry,
                "archive",
                ROOT_NAME,
                exact_archive_sha256="0" * 64,
            )


@pytest.mark.parametrize(
    ("filename", "kind", "root_name"),
    (
        ("storm_tracker_v3_v1.11.0_deploy.zip", "zip", "storm_tracker_v3"),
        (
            "storm_tracker_worker_v1.14.0_deploy.tar.gz",
            "tar",
            "storm_tracker_worker",
        ),
    ),
)
def test_real_legacy_release_artifacts_remain_supported(
    tmp_path: Path,
    filename: str,
    kind: str,
    root_name: str,
):
    value = (Path(__file__).resolve().parents[1] / filename).read_bytes()
    with _registry(tmp_path, value) as registry:
        if kind == "zip":
            result = archives.inspect_legacy_zip(
                registry,
                "archive",
                root_name,
            )
        else:
            result = archives.inspect_legacy_tar_gz(
                registry,
                "archive",
                root_name,
                exact_archive_sha256=_sha(value),
            )

    assert result.tree.root_name == root_name
    assert result.tree.files
    assert result.header_index.archive_bytes == len(value)


@pytest.mark.parametrize("kind", ("zip", "tar"))
def test_extraction_streams_only_exact_bounded_bodies_to_sink(
    tmp_path: Path,
    kind: str,
):
    value = _zip_bytes() if kind == "zip" else _tar_bytes()
    observed, sink = _sink_values()
    with _registry(tmp_path, value) as registry:
        if kind == "zip":
            result = archives.extract_zip_to_sink(
                registry,
                "archive",
                _tree(),
                sink,
            )
        else:
            result = archives.extract_tar_gz_to_sink(
                registry,
                "archive",
                _tree(),
                sink,
            )

    assert observed == PAYLOADS
    assert result.file_count == len(PAYLOADS)
    assert result.total_bytes == sum(map(len, PAYLOADS.values()))


@pytest.mark.parametrize("kind", ("zip", "tar"))
def test_extraction_mode_policy_is_explicit_and_candidate_is_exact(
    tmp_path: Path,
    kind: str,
):
    value = _zip_bytes(file_mode=0o600) if kind == "zip" else _tar_bytes(file_mode=0o600)
    call = (
        archives.extract_zip_to_sink
        if kind == "zip"
        else archives.extract_tar_gz_to_sink
    )
    with _registry(tmp_path, value) as registry:
        with pytest.raises(
            archives.BoundArchiveIntegrityError,
            match="file_mode_differs",
        ):
            call(registry, "archive", _tree(), lambda _record, _source: None)

        observed, sink = _sink_values()
        result = call(
            registry,
            "archive",
            _tree(),
            sink,
            mode_policy="legacy",
        )

    assert observed == PAYLOADS
    assert result.file_count == len(PAYLOADS)


def test_extraction_rejects_unknown_mode_policy_before_open(tmp_path: Path):
    with _registry(tmp_path, _zip_bytes()) as registry:
        observed = _ObservedSource(registry)
        with pytest.raises(
            archives.BoundArchiveContractError,
            match="mode_policy_invalid",
        ):
            archives.extract_zip_to_sink(
                observed,
                "archive",
                _tree(),
                lambda _record, _source: None,
                mode_policy="automatic",
            )
        assert observed.open_calls == 0


@pytest.mark.parametrize(
    "requested_size",
    (-1, 0, ARCHIVE_MEMBER_CHUNK_BYTES + 1),
)
def test_sink_cannot_request_unbounded_or_oversized_payload(
    tmp_path: Path,
    requested_size: int,
):
    def sink(_record, source):
        source.read(requested_size)

    with _registry(tmp_path, _zip_bytes()) as registry:
        with pytest.raises(
            archives.BoundArchiveContractError,
            match="sink_read_bound_invalid",
        ):
            archives.extract_zip_to_sink(
                registry,
                "archive",
                _tree(),
                sink,
            )


def test_sink_must_consume_every_member(tmp_path: Path):
    def sink(_record, source):
        source.read(1)

    with _registry(tmp_path, _zip_bytes()) as registry:
        with pytest.raises(
            archives.BoundArchiveMemberError,
            match="sink_did_not_consume_member",
        ):
            archives.extract_zip_to_sink(
                registry,
                "archive",
                _tree(),
                sink,
            )


def test_sink_consumer_error_still_exits_registry_postproof_context(tmp_path: Path):
    class ConsumerFailure(RuntimeError):
        pass

    def sink(_record, _source):
        raise ConsumerFailure("injected-consumer")

    with _registry(tmp_path, _zip_bytes()) as registry:
        observed = _ObservedSource(registry)
        with pytest.raises(ConsumerFailure, match="injected-consumer"):
            archives.extract_zip_to_sink(
                observed,
                "archive",
                _tree(),
                sink,
            )
        assert observed.open_calls == observed.exit_calls == 1


def test_mutation_and_consumer_error_still_fail_registry_postproof(tmp_path: Path):
    class ConsumerFailure(RuntimeError):
        pass

    value = _zip_bytes()

    def sink(_record, _source):
        (tmp_path / "archive.bin").write_bytes(b"x" * len(value))
        raise ConsumerFailure("injected-consumer-after-mutation")

    with _registry(tmp_path, value) as registry:
        with pytest.raises(
            BoundInputIntegrityError,
            match="size or SHA",
        ) as raised:
            archives.extract_zip_to_sink(
                registry,
                "archive",
                _tree(),
                sink,
            )
        assert isinstance(raised.value.__cause__, ConsumerFailure)


@pytest.mark.parametrize(
    "operation",
    (
        "validate_candidate_zip",
        "validate_candidate_tar_gz",
        "inspect_legacy_zip",
        "inspect_legacy_tar_gz",
        "extract_zip_to_sink",
        "extract_tar_gz_to_sink",
    ),
)
def test_all_public_operations_bound_outer_input_stream_reads(
    tmp_path: Path,
    operation: str,
):
    is_tar = "tar" in operation
    value = _tar_bytes() if is_tar else _zip_bytes()
    with _registry(tmp_path, value) as registry:
        observed = _GuardedObservedSource(registry)
        if operation == "validate_candidate_zip":
            archives.validate_candidate_zip(observed, "archive", _tree())
        elif operation == "validate_candidate_tar_gz":
            archives.validate_candidate_tar_gz(observed, "archive", _tree())
        elif operation == "inspect_legacy_zip":
            archives.inspect_legacy_zip(observed, "archive", ROOT_NAME)
        elif operation == "inspect_legacy_tar_gz":
            archives.inspect_legacy_tar_gz(
                observed,
                "archive",
                ROOT_NAME,
                exact_archive_sha256=_sha(value),
            )
        else:
            _values, sink = _sink_values()
            call = (
                archives.extract_tar_gz_to_sink
                if is_tar
                else archives.extract_zip_to_sink
            )
            call(observed, "archive", _tree(), sink)

    assert observed.open_calls == observed.exit_calls == 1
    assert observed.read_requests
    assert all(
        0 <= request <= ARCHIVE_MEMBER_CHUNK_BYTES
        for request in observed.read_requests
    )


@pytest.mark.parametrize(
    ("behavior", "message"),
    (
        ("over_return", "read_bound_exceeded"),
        ("nonbytes", "returned_nonbytes"),
        ("exception", "read_failed"),
    ),
)
def test_malformed_outer_reader_fails_closed(
    tmp_path: Path,
    behavior: str,
    message: str,
):
    with _registry(tmp_path, _zip_bytes()) as registry:
        observed = _MalformedObservedSource(registry, behavior)
        with pytest.raises(
            archives.BoundArchiveIntegrityError,
            match=message,
        ):
            archives.validate_candidate_zip(observed, "archive", _tree())
        assert observed.open_calls == observed.exit_calls == 1


@pytest.mark.parametrize(
    "operation",
    (
        "validate_candidate_zip",
        "validate_candidate_tar_gz",
        "inspect_legacy_zip",
        "inspect_legacy_tar_gz",
        "extract_zip_to_sink",
        "extract_tar_gz_to_sink",
    ),
)
def test_each_operation_uses_one_stream_for_header_and_stdlib(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
):
    is_tar = "tar" in operation
    value = _tar_bytes() if is_tar else _zip_bytes()
    header_ids: list[int] = []
    raw_ids: list[int] = []
    stdlib_ids: list[int] = []
    if not is_tar:
        original_header = archives.validate_zip_header_stream
        original_open = archives.zipfile.ZipFile

        def header(stream, **kwargs):
            header_ids.append(id(stream))
            raw_ids.append(id(stream._source))
            return original_header(stream, **kwargs)

        def open_container(stream, *args, **kwargs):
            stdlib_ids.append(id(stream))
            return original_open(stream, *args, **kwargs)

        monkeypatch.setattr(archives, "validate_zip_header_stream", header)
        monkeypatch.setattr(archives.zipfile, "ZipFile", open_container)
    else:
        original_header = archives.validate_tar_gz_header_stream
        original_open = archives.tarfile.open

        def header(stream, **kwargs):
            header_ids.append(id(stream))
            raw_ids.append(id(stream._source))
            return original_header(stream, **kwargs)

        def open_container(*, fileobj, mode):
            stdlib_ids.append(id(fileobj))
            return original_open(fileobj=fileobj, mode=mode)

        monkeypatch.setattr(archives, "validate_tar_gz_header_stream", header)
        monkeypatch.setattr(archives.tarfile, "open", open_container)

    with _registry(tmp_path, value) as registry:
        observed = _ObservedSource(registry)
        if operation == "validate_candidate_zip":
            archives.validate_candidate_zip(observed, "archive", _tree())
        elif operation == "validate_candidate_tar_gz":
            archives.validate_candidate_tar_gz(observed, "archive", _tree())
        elif operation == "inspect_legacy_zip":
            archives.inspect_legacy_zip(observed, "archive", ROOT_NAME)
        elif operation == "inspect_legacy_tar_gz":
            archives.inspect_legacy_tar_gz(
                observed,
                "archive",
                ROOT_NAME,
                exact_archive_sha256=_sha(value),
            )
        else:
            _values, sink = _sink_values()
            call = (
                archives.extract_tar_gz_to_sink
                if is_tar
                else archives.extract_zip_to_sink
            )
            call(observed, "archive", _tree(), sink)

    assert observed.open_calls == observed.exit_calls == 1
    assert len(header_ids) == 1
    assert header_ids == stdlib_ids
    assert raw_ids == observed.stream_ids


def test_exact_keyset_and_metadata_size_are_required(tmp_path: Path):
    value = _zip_bytes()
    with _registry(tmp_path, value, extra=(("other", b"other"),)) as registry:
        with pytest.raises(BoundInputContractError, match="keys differ"):
            archives.validate_candidate_zip(registry, "archive", _tree())

    with _registry(tmp_path, value) as registry:
        class MetadataLie(_ObservedSource):
            def metadata(self, key):
                actual = self.registry.metadata(key)
                lied_identity = replace(
                    actual.identity,
                    size=actual.identity.size + 1,
                )
                return BoundInputMetadata(
                    key=actual.key,
                    relative=actual.relative,
                    identity=lied_identity,
                    size=actual.size + 1,
                    sha256=actual.sha256,
                    maximum_bytes=actual.maximum_bytes + 1,
                )

        with pytest.raises(archives.BoundArchiveIntegrityError):
            archives.validate_candidate_zip(
                MetadataLie(registry),
                "archive",
                _tree(),
            )


@pytest.mark.parametrize("kind", ("zip", "tar"))
def test_header_budget_fails_before_any_sink_callback(tmp_path: Path, kind: str):
    value = _zip_bytes() if kind == "zip" else _tar_bytes()
    calls = 0

    def sink(_record, _source):
        nonlocal calls
        calls += 1

    limits = _small_limits(total=_tree().total_bytes - 1)
    with _registry(tmp_path, value) as registry:
        call = (
            archives.extract_zip_to_sink
            if kind == "zip"
            else archives.extract_tar_gz_to_sink
        )
        with pytest.raises(archives.BoundArchiveError):
            call(registry, "archive", _tree(), sink, limits=limits)
    assert calls == 0


def test_unsafe_tar_extensions_fail_closed_before_stdlib(tmp_path: Path):
    for index, value in enumerate((_tar_bytes(symlink=True), _tar_bytes(pax=True))):
        directory = tmp_path / str(index)
        directory.mkdir()
        with _registry(directory, value) as registry:
            with pytest.raises(
                archives.BoundArchiveIntegrityError,
                match="unsafe_extension|header_preflight",
            ):
                archives.inspect_legacy_tar_gz(
                    registry,
                    "archive",
                    ROOT_NAME,
                    exact_archive_sha256=_sha(value),
                )


def test_tree_contract_is_unique_sorted_parent_complete_and_portable():
    file = archives.BoundArchiveFile("nested/value.txt", 1, _sha(b"x"), 0o644)
    tree = archives.BoundArchiveTree.create(ROOT_NAME, (file,))
    assert tree.directories == ("nested",)

    with pytest.raises(archives.BoundArchiveContractError):
        archives.BoundArchiveTree(ROOT_NAME, (file, file), ("nested",))
    with pytest.raises(archives.BoundArchiveContractError):
        archives.BoundArchiveFile("../escape", 1, _sha(b"x"), 0o644)
    with pytest.raises(archives.BoundArchiveContractError):
        archives.BoundArchiveFile("NUL.txt", 1, _sha(b"x"), 0o644)


def test_empty_legacy_directory_inventory_is_preserved_during_extraction(
    tmp_path: Path,
):
    value = _zip_bytes(explicit_directories=("empty", "nested"))
    observed, sink = _sink_values()
    with _registry(tmp_path, value) as registry:
        inspected = archives.inspect_legacy_zip(registry, "archive", ROOT_NAME)
        extracted = archives.extract_zip_to_sink(
            registry,
            "archive",
            inspected.tree,
            sink,
            mode_policy="legacy",
        )

    assert inspected.tree.directories == ("empty", "nested")
    assert observed == PAYLOADS
    assert extracted.file_count == len(inspected.tree.files)


def test_module_is_pathless_bounded_and_makes_no_integration_claim():
    source = Path(archives.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    called = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "read"
        ):
            assert len(node.args) == 1
            assert not (
                isinstance(node.args[0], ast.Constant)
                and node.args[0].value == -1
            )

    imported_pathlib_names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "pathlib"
        for alias in node.names
    }
    assert imported_pathlib_names == {"PurePosixPath"}
    assert not any(
        isinstance(node, ast.Name) and node.id == "Path"
        for node in ast.walk(tree)
    )
    assert "open_absolute" not in called
    assert not {"tempfile", "shutil"} & imported
    assert "release_install_dry_run" not in imported
    assert "release_artifacts" not in imported
    assert "TreeRecord" not in source
    assert "FileRecord" not in source
    evidence = archives.bound_archive_evidence()
    assert evidence["integration"] is False
    assert evidence["blockers"] == []
    assert evidence["archive_paths_accepted"] is False
    assert evidence["member_read_minus_one_allowed"] is False
    assert evidence["outer_stream_read_minus_one_allowed"] is False
    assert evidence["stdlib_negative_read_rejected_before_bound_input"] is True
    assert evidence["extraction_mode_policy_explicit"] is True
    assert evidence["per_member_source_modes_recorded"] is True
    assert evidence["simulator_integration_proven"] is False
    assert evidence["local_acceptance_blockers_closed"] == []
