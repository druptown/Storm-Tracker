"""Adversarial coverage for the standalone bounded archive preflight."""
from __future__ import annotations

from dataclasses import replace
import gzip
import io
import os
from pathlib import Path
import stat
import struct
import tarfile
import warnings
import zipfile
import zlib

import pytest

from storm_tracker_worker.bounded_archive_headers import (
    ArchiveHeaderError,
    ArchiveHeaderLimits,
    DEFAULT_ARCHIVE_HEADER_LIMITS,
    validate_tar_gz_header_stream,
    validate_tar_gz_headers,
    validate_zip_header_stream,
    validate_zip_headers,
    validated_tar_gz_file,
    validated_zip_file,
)


_ZIP_CENTRAL = struct.Struct("<4s6H3L5H2L")
_ZIP_EOCD_SIGNATURE = b"PK\x05\x06"
_TAR_BLOCK_BYTES = 512


class _ReadBoundBytesIO(io.BytesIO):
    """Reject accidental whole-stream reads in either public stream parser."""

    def __init__(self, value: bytes) -> None:
        super().__init__(value)
        self.maximum_read = 0

    def read(self, size: int = -1) -> bytes:
        if size < 0 or size > 64 * 1024:
            raise AssertionError(f"unbounded archive read requested: {size}")
        self.maximum_read = max(self.maximum_read, size)
        return super().read(size)


class _FaultyReadBytesIO(io.BytesIO):
    def __init__(self, value: bytes, fault: str, *, deflate_only: bool = False) -> None:
        super().__init__(value)
        self._fault = fault
        self._deflate_only = deflate_only
        self._reads = 0

    def read(self, size: int = -1):
        self._reads += 1
        if not self._deflate_only or self._reads == 2:
            if self._fault == "overread":
                return b"x" * (size + 1)
            if self._fault == "nonbytes":
                return bytearray(b"x")
            if self._fault == "exception":
                raise OSError("synthetic read failure")
        return super().read(size)


class _ForwardOnlyBytesIO(io.BytesIO):
    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        raise OSError("forward-only archive stream")

    def tell(self) -> int:
        raise OSError("forward-only archive stream")


def _zip_info(name: str, *, directory: bool) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
    info.create_system = 3
    if directory:
        info.external_attr = ((stat.S_IFDIR | 0o755) << 16) | 0x10
    else:
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = (stat.S_IFREG | 0o644) << 16
    return info


def _zip_bytes(
    *,
    file_name: str = "pkg/data.txt",
    data: bytes = b"bounded\n",
    file_extra: bytes = b"",
    file_comment: bytes = b"",
    archive_comment: bytes = b"",
    duplicate_file: bool = False,
    second_file: bool = False,
) -> bytes:
    target = io.BytesIO()
    with zipfile.ZipFile(
        target,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
        strict_timestamps=True,
    ) as archive:
        archive.writestr(_zip_info("pkg/", directory=True), b"")
        info = _zip_info(file_name, directory=False)
        info.extra = file_extra
        info.comment = file_comment
        archive.writestr(info, data)
        if duplicate_file:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                archive.writestr(info, data)
        if second_file:
            archive.writestr(_zip_info("pkg/more.txt", directory=False), data)
        archive.comment = archive_comment
    return target.getvalue()


def _signed_descriptor_zip_bytes() -> bytes:
    """Convert the last regular entry to the exact legacy descriptor profile."""

    value = _zip_bytes()
    central_offset, local_offset = _zip_offsets(value)
    eocd_offset = value.rfind(_ZIP_EOCD_SIGNATURE)
    directory_offset = struct.unpack_from("<I", value, eocd_offset + 16)[0]
    crc32 = struct.unpack_from("<I", value, central_offset + 16)[0]
    compressed_size = struct.unpack_from("<I", value, central_offset + 20)[0]
    uncompressed_size = struct.unpack_from("<I", value, central_offset + 24)[0]
    name_size = struct.unpack_from("<H", value, local_offset + 26)[0]
    extra_size = struct.unpack_from("<H", value, local_offset + 28)[0]
    descriptor_offset = local_offset + 30 + name_size + extra_size + compressed_size
    assert descriptor_offset == directory_offset
    descriptor = struct.pack(
        "<4sIII",
        b"PK\x07\x08",
        crc32,
        compressed_size,
        uncompressed_size,
    )
    converted = bytearray(value[:directory_offset] + descriptor + value[directory_offset:])
    converted_central_offset = central_offset + len(descriptor)
    converted_eocd_offset = eocd_offset + len(descriptor)
    central_flags = struct.unpack_from("<H", converted, converted_central_offset + 8)[0]
    local_flags = struct.unpack_from("<H", converted, local_offset + 6)[0]
    struct.pack_into("<H", converted, converted_central_offset + 8, central_flags | 0x08)
    struct.pack_into("<H", converted, local_offset + 6, local_flags | 0x08)
    struct.pack_into("<III", converted, local_offset + 14, 0, 0, 0)
    struct.pack_into("<I", converted, converted_eocd_offset + 16, directory_offset + 16)
    return bytes(converted)


def _zip_offsets(value: bytes, index: int = 1) -> tuple[int, int]:
    eocd_offset = value.rfind(_ZIP_EOCD_SIGNATURE)
    assert eocd_offset >= 0
    central_offset = struct.unpack_from("<I", value, eocd_offset + 16)[0]
    offset = central_offset
    for current in range(index + 1):
        fields = _ZIP_CENTRAL.unpack_from(value, offset)
        assert fields[0] == b"PK\x01\x02"
        if current == index:
            return offset, fields[-1]
        offset += _ZIP_CENTRAL.size + fields[10] + fields[11] + fields[12]
    raise AssertionError("ZIP entry index is absent")


def _tar_info(name: str, *, directory: bool, size: int = 0) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.uid = 0
    info.gid = 0
    info.uname = "root"
    info.gname = "root"
    info.mtime = 0
    info.mode = 0o755 if directory else 0o644
    info.type = tarfile.DIRTYPE if directory else tarfile.REGTYPE
    info.size = 0 if directory else size
    return info


def _tar_gz_bytes(data: bytes = b"bounded\n") -> bytes:
    target = io.BytesIO()
    with gzip.GzipFile(
        filename="",
        mode="wb",
        fileobj=target,
        compresslevel=9,
        mtime=0,
    ) as compressed:
        with tarfile.open(
            fileobj=compressed,
            mode="w",
            format=tarfile.PAX_FORMAT,
        ) as archive:
            archive.addfile(_tar_info("pkg/", directory=True))
            archive.addfile(
                _tar_info("pkg/data.txt", directory=False, size=len(data)),
                io.BytesIO(data),
            )
    return target.getvalue()


def _octal(value: int, width: int) -> bytes:
    return f"{value:0{width - 1}o}\0".encode("ascii")


def _tar_header(
    name: str,
    *,
    type_flag: bytes,
    size: int,
    size_field: bytes | None = None,
) -> bytes:
    raw_name = name.encode("ascii")
    assert len(raw_name) <= 100
    assert len(type_flag) == 1
    header = bytearray(_TAR_BLOCK_BYTES)
    header[0:len(raw_name)] = raw_name
    header[100:108] = _octal(0o755 if type_flag == b"5" else 0o644, 8)
    header[108:116] = _octal(0, 8)
    header[116:124] = _octal(0, 8)
    header[124:136] = size_field if size_field is not None else _octal(size, 12)
    header[136:148] = _octal(0, 12)
    header[148:156] = b" " * 8
    header[156:157] = type_flag
    header[257:263] = b"ustar\0"
    header[263:265] = b"00"
    header[265:269] = b"root"
    header[297:301] = b"root"
    header[329:337] = _octal(0, 8)
    header[337:345] = _octal(0, 8)
    return _tar_rechecksum(header)


def _tar_rechecksum(value: bytes | bytearray) -> bytes:
    header = bytearray(value)
    header[148:156] = b" " * 8
    checksum = sum(header)
    header[148:156] = f"{checksum:06o}\0 ".encode("ascii")
    return bytes(header)


def _raw_tar(
    records: list[tuple[bytes, bytes]],
    *,
    padding_byte: bytes = b"\0",
    end_padding: bytes = b"",
) -> bytes:
    assert len(padding_byte) == 1
    chunks: list[bytes] = []
    for header, payload in records:
        chunks.extend((header, payload))
        chunks.append(padding_byte * ((-len(payload)) % _TAR_BLOCK_BYTES))
    chunks.extend((b"\0" * _TAR_BLOCK_BYTES, b"\0" * _TAR_BLOCK_BYTES))
    chunks.append(end_padding)
    return b"".join(chunks)


def _gzip_member(
    payload: bytes,
    *,
    extra: bytes = b"",
    name: bytes = b"",
    comment: bytes = b"",
    header_crc: bool = False,
) -> bytes:
    flags = 0
    if extra:
        flags |= 0x04
    if name:
        flags |= 0x08
    if comment:
        flags |= 0x10
    if header_crc:
        flags |= 0x02
    header = bytearray(b"\x1f\x8b\x08" + bytes([flags]) + b"\0\0\0\0\x02\xff")
    if extra:
        header.extend(struct.pack("<H", len(extra)))
        header.extend(extra)
    if name:
        header.extend(name + b"\0")
    if comment:
        header.extend(comment + b"\0")
    if header_crc:
        header.extend(struct.pack("<H", zlib.crc32(header) & 0xFFFF))
    compressor = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS)
    encoded = compressor.compress(payload) + compressor.flush()
    trailer = struct.pack(
        "<II",
        zlib.crc32(payload) & 0xFFFFFFFF,
        len(payload) & 0xFFFFFFFF,
    )
    return bytes(header) + encoded + trailer


def _manual_tar(*records: tuple[bytes, bytes], **gzip_options: object) -> bytes:
    return _gzip_member(_raw_tar(list(records)), **gzip_options)


def _validate_zip_bytes(
    value: bytes,
    *,
    expected_root: str = "pkg",
    limits=DEFAULT_ARCHIVE_HEADER_LIMITS,
):
    return validate_zip_header_stream(
        io.BytesIO(value),
        archive_size=len(value),
        expected_root=expected_root,
        limits=limits,
    )


def _validate_tar_bytes(
    value: bytes,
    *,
    expected_root: str = "pkg",
    limits=DEFAULT_ARCHIVE_HEADER_LIMITS,
):
    return validate_tar_gz_header_stream(
        io.BytesIO(value),
        archive_size=len(value),
        expected_root=expected_root,
        limits=limits,
    )


def test_current_builder_profiles_validate_through_streams_and_paths(
    tmp_path: Path,
) -> None:
    zip_value = _zip_bytes()
    tar_value = _tar_gz_bytes()
    zip_path = tmp_path / "candidate.zip"
    tar_path = tmp_path / "candidate.tar.gz"
    zip_path.write_bytes(zip_value)
    tar_path.write_bytes(tar_value)

    zip_index = validate_zip_headers(zip_path, expected_root="pkg")
    tar_index = validate_tar_gz_headers(tar_path, expected_root="pkg")

    assert zip_index.format == "zip"
    assert tar_index.format == "tar.gz"
    assert zip_index.root_name == tar_index.root_name == "pkg"
    assert [(item.path, item.kind) for item in zip_index.entries] == [
        ("pkg", "directory"),
        ("pkg/data.txt", "file"),
    ]
    assert [(item.path, item.kind) for item in tar_index.entries] == [
        ("pkg", "directory"),
        ("pkg/data.txt", "file"),
    ]
    assert zip_index.total_uncompressed_bytes == 8
    assert tar_index.total_uncompressed_bytes == 8
    assert zip_index.archive_bytes == len(zip_value)
    assert tar_index.archive_bytes == len(tar_value)


def test_public_stream_parsers_never_request_an_unbounded_read() -> None:
    zip_value = _zip_bytes(data=b"a" * 100_000)
    tar_value = _tar_gz_bytes(data=b"a" * 100_000)
    zip_stream = _ReadBoundBytesIO(zip_value)
    tar_stream = _ReadBoundBytesIO(tar_value)

    validate_zip_header_stream(
        zip_stream,
        archive_size=len(zip_value),
        expected_root="pkg",
    )
    validate_tar_gz_header_stream(
        tar_stream,
        archive_size=len(tar_value),
        expected_root="pkg",
    )

    assert 0 < zip_stream.maximum_read <= 64 * 1024
    assert 0 < tar_stream.maximum_read <= 64 * 1024


def test_tar_gz_parser_accepts_a_forward_only_stream() -> None:
    value = _tar_gz_bytes()

    index = validate_tar_gz_header_stream(
        _ForwardOnlyBytesIO(value),
        archive_size=len(value),
        expected_root="pkg",
    )

    assert index.root_name == "pkg"
    assert len(index.entries) == 2


def test_stream_archive_size_must_equal_the_source_extent() -> None:
    zip_value = _zip_bytes()
    tar_value = _tar_gz_bytes()

    with pytest.raises(ArchiveHeaderError, match="ZIP stream exceeds"):
        validate_zip_header_stream(
            io.BytesIO(zip_value + b"x"),
            archive_size=len(zip_value),
            expected_root="pkg",
        )
    with pytest.raises(ArchiveHeaderError, match="archive stream exceeds"):
        validate_tar_gz_header_stream(
            io.BytesIO(tar_value + b"x"),
            archive_size=len(tar_value),
            expected_root="pkg",
        )


def test_limit_contract_is_exact_and_rejects_inconsistent_values() -> None:
    assert tuple(ArchiveHeaderLimits.__dataclass_fields__) == (
        "max_archive_bytes",
        "max_members",
        "max_path_bytes",
        "max_path_part_bytes",
        "max_path_depth",
        "max_member_uncompressed_bytes",
        "max_total_uncompressed_bytes",
        "max_zip_central_directory_bytes",
        "max_zip_extra_bytes",
        "max_zip_entry_comment_bytes",
        "max_zip_archive_comment_bytes",
        "max_gzip_extra_bytes",
        "max_gzip_name_bytes",
        "max_gzip_comment_bytes",
        "max_tar_stream_bytes",
        "max_tar_end_padding_bytes",
    )
    with pytest.raises(TypeError, match="positive integers"):
        replace(DEFAULT_ARCHIVE_HEADER_LIMITS, max_members=True)
    with pytest.raises(TypeError, match="positive integers"):
        replace(DEFAULT_ARCHIVE_HEADER_LIMITS, max_members=0)
    with pytest.raises(ValueError, match="path-part"):
        replace(
            DEFAULT_ARCHIVE_HEADER_LIMITS,
            max_path_bytes=1,
            max_path_part_bytes=2,
        )
    with pytest.raises(ValueError, match="member limit"):
        replace(
            DEFAULT_ARCHIVE_HEADER_LIMITS,
            max_member_uncompressed_bytes=2,
            max_total_uncompressed_bytes=1,
        )
    with pytest.raises(ValueError, match="TAR stream limit"):
        replace(
            DEFAULT_ARCHIVE_HEADER_LIMITS,
            max_members=1,
            max_member_uncompressed_bytes=1,
            max_total_uncompressed_bytes=1,
            max_tar_stream_bytes=2049,
            max_tar_end_padding_bytes=1,
        )


@pytest.mark.parametrize(
    ("fault", "message"),
    [
        ("overread", "more bytes than requested"),
        ("nonbytes", "non-byte data"),
        ("exception", "could not be read"),
    ],
)
def test_zip_stream_reader_contract_fails_closed(fault: str, message: str) -> None:
    value = _zip_bytes()
    source = _FaultyReadBytesIO(value + b"sentinel", fault)

    with pytest.raises(ArchiveHeaderError, match=message):
        validate_zip_header_stream(
            source,
            archive_size=len(value),
            expected_root="pkg",
        )


@pytest.mark.parametrize(
    ("fault", "message"),
    [
        ("overread", "more bytes than requested"),
        ("nonbytes", "non-byte data"),
        ("exception", "could not be read"),
    ],
)
def test_deflate_reader_contract_fails_closed(fault: str, message: str) -> None:
    value = _tar_gz_bytes()
    source = _FaultyReadBytesIO(value, fault, deflate_only=True)

    with pytest.raises(ArchiveHeaderError, match=message):
        validate_tar_gz_header_stream(
            source,
            archive_size=len(value),
            expected_root="pkg",
        )


def test_zip_seek_and_tell_failures_are_translated() -> None:
    value = _zip_bytes()

    class _SeekFailure(io.BytesIO):
        def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
            raise OSError("synthetic seek failure")

    class _TellFailure(io.BytesIO):
        def tell(self) -> int:
            raise OSError("synthetic tell failure")

    with pytest.raises(ArchiveHeaderError, match="not safely seekable"):
        validate_zip_header_stream(
            _SeekFailure(value),
            archive_size=len(value),
            expected_root="pkg",
        )
    with pytest.raises(ArchiveHeaderError, match="position is unavailable"):
        validate_zip_header_stream(
            _TellFailure(value),
            archive_size=len(value),
            expected_root="pkg",
        )


@pytest.mark.parametrize(
    ("limits", "message"),
    [
        (replace(DEFAULT_ARCHIVE_HEADER_LIMITS, max_members=1), "member count"),
        (
            replace(
                DEFAULT_ARCHIVE_HEADER_LIMITS,
                max_path_bytes=4,
                max_path_part_bytes=4,
            ),
            "member name exceeds",
        ),
        (
            replace(
                DEFAULT_ARCHIVE_HEADER_LIMITS,
                max_member_uncompressed_bytes=4,
            ),
            "member size",
        ),
    ],
)
def test_zip_declared_counts_paths_and_sizes_fail_before_materialization(
    limits, message: str
) -> None:
    with pytest.raises(ArchiveHeaderError, match=message):
        _validate_zip_bytes(_zip_bytes(), limits=limits)


def test_zip_total_declared_size_is_bounded_across_members() -> None:
    limits = replace(
        DEFAULT_ARCHIVE_HEADER_LIMITS,
        max_member_uncompressed_bytes=10,
        max_total_uncompressed_bytes=10,
    )

    with pytest.raises(ArchiveHeaderError, match="total member bytes"):
        _validate_zip_bytes(_zip_bytes(second_file=True), limits=limits)


def test_zip_central_directory_cap_is_checked_from_eocd() -> None:
    value = _zip_bytes()
    eocd_offset = value.rfind(_ZIP_EOCD_SIGNATURE)
    central_size = struct.unpack_from("<I", value, eocd_offset + 12)[0]
    limits = replace(
        DEFAULT_ARCHIVE_HEADER_LIMITS,
        max_zip_central_directory_bytes=central_size - 1,
    )

    with pytest.raises(ArchiveHeaderError, match="central directory"):
        _validate_zip_bytes(value, limits=limits)


@pytest.mark.parametrize("metadata", ["extra", "entry_comment", "archive_comment"])
def test_zip_variable_metadata_caps_are_enforced(metadata: str) -> None:
    extra = b"\xfe\xca\x05\0abcde" if metadata == "extra" else b""
    entry_comment = b"abcde" if metadata == "entry_comment" else b""
    archive_comment = b"abcde" if metadata == "archive_comment" else b""
    value = _zip_bytes(
        file_extra=extra,
        file_comment=entry_comment,
        archive_comment=archive_comment,
    )
    limit_name = {
        "extra": "max_zip_extra_bytes",
        "entry_comment": "max_zip_entry_comment_bytes",
        "archive_comment": "max_zip_archive_comment_bytes",
    }[metadata]
    limits = replace(DEFAULT_ARCHIVE_HEADER_LIMITS, **{limit_name: 4})

    with pytest.raises(ArchiveHeaderError, match="(extra|comment|end record)"):
        _validate_zip_bytes(value, limits=limits)


def test_zip64_profile_fails_closed() -> None:
    value = bytearray(_zip_bytes())
    central_offset, _local_offset = _zip_offsets(value)
    zip64 = bytearray(value)
    struct.pack_into("<I", zip64, central_offset + 24, 0xFFFFFFFF)

    with pytest.raises(ArchiveHeaderError, match="ZIP64"):
        _validate_zip_bytes(bytes(zip64))


def test_legacy_signed_data_descriptor_profile_is_exactly_validated() -> None:
    value = _signed_descriptor_zip_bytes()
    central_offset, local_offset = _zip_offsets(value)
    flags = struct.unpack_from("<H", value, central_offset + 8)[0]
    compressed_size = struct.unpack_from("<I", value, central_offset + 20)[0]
    name_size = struct.unpack_from("<H", value, local_offset + 26)[0]
    extra_size = struct.unpack_from("<H", value, local_offset + 28)[0]
    descriptor_offset = local_offset + 30 + name_size + extra_size + compressed_size

    assert flags & 0x08
    assert value[descriptor_offset:descriptor_offset + 4] == b"PK\x07\x08"
    assert _validate_zip_bytes(value).root_name == "pkg"

    corrupt_signature = bytearray(value)
    corrupt_signature[descriptor_offset] ^= 0x01
    with pytest.raises(ArchiveHeaderError, match="signed data descriptor differs"):
        _validate_zip_bytes(bytes(corrupt_signature))

    corrupt_crc = bytearray(value)
    corrupt_crc[descriptor_offset + 4] ^= 0x01
    with pytest.raises(ArchiveHeaderError, match="signed data descriptor differs"):
        _validate_zip_bytes(bytes(corrupt_crc))

    nonzero_local_size = bytearray(value)
    struct.pack_into("<I", nonzero_local_size, local_offset + 18, 1)
    with pytest.raises(ArchiveHeaderError, match="local and central"):
        _validate_zip_bytes(bytes(nonzero_local_size))


def test_zip_local_central_differential_and_duplicate_names_fail_closed() -> None:
    value = bytearray(_zip_bytes())
    _central_offset, local_offset = _zip_offsets(value)
    value[local_offset + 14] ^= 0x01

    with pytest.raises(ArchiveHeaderError, match="local and central"):
        _validate_zip_bytes(bytes(value))
    with pytest.raises(ArchiveHeaderError, match="duplicate"):
        _validate_zip_bytes(_zip_bytes(duplicate_file=True))


def test_zip_eocd_member_bomb_is_rejected_before_index_walk() -> None:
    value = bytearray(_zip_bytes())
    eocd_offset = value.rfind(_ZIP_EOCD_SIGNATURE)
    struct.pack_into("<H", value, eocd_offset + 8, 0xFFFE)
    struct.pack_into("<H", value, eocd_offset + 10, 0xFFFE)

    with pytest.raises(ArchiveHeaderError, match="member count"):
        _validate_zip_bytes(bytes(value))


def test_tar_declared_count_path_member_and_total_caps_fail_before_payload() -> None:
    root = (_tar_header("pkg/", type_flag=b"5", size=0), b"")
    first = (_tar_header("pkg/a", type_flag=b"0", size=6), b"a" * 6)
    second = (_tar_header("pkg/b", type_flag=b"0", size=6), b"b" * 6)

    with pytest.raises(ArchiveHeaderError, match="member count"):
        _validate_tar_bytes(
            _manual_tar(root, first),
            limits=replace(DEFAULT_ARCHIVE_HEADER_LIMITS, max_members=1),
        )
    with pytest.raises(ArchiveHeaderError, match="path exceeds"):
        _validate_tar_bytes(
            _manual_tar(root, first),
            limits=replace(
                DEFAULT_ARCHIVE_HEADER_LIMITS,
                max_path_bytes=4,
                max_path_part_bytes=4,
            ),
        )
    with pytest.raises(ArchiveHeaderError, match="member size"):
        _validate_tar_bytes(
            _manual_tar(
                root,
                (_tar_header("pkg/a", type_flag=b"0", size=4096), b""),
            ),
            limits=replace(
                DEFAULT_ARCHIVE_HEADER_LIMITS,
                max_member_uncompressed_bytes=8,
            ),
        )
    with pytest.raises(ArchiveHeaderError, match="total member bytes"):
        _validate_tar_bytes(
            _manual_tar(root, first, second),
            limits=replace(
                DEFAULT_ARCHIVE_HEADER_LIMITS,
                max_member_uncompressed_bytes=10,
                max_total_uncompressed_bytes=10,
            ),
        )


@pytest.mark.parametrize("type_flag", [b"x", b"g", b"L", b"K"])
def test_pax_and_gnu_extension_records_fail_closed(type_flag: bytes) -> None:
    root = (_tar_header("pkg/", type_flag=b"5", size=0), b"")
    extension = (_tar_header("PaxHeader", type_flag=type_flag, size=0), b"")

    with pytest.raises(ArchiveHeaderError, match="PAX and GNU"):
        _validate_tar_bytes(_manual_tar(root, extension))


def test_tar_base256_numeric_encoding_and_checksum_fail_closed() -> None:
    root = (_tar_header("pkg/", type_flag=b"5", size=0), b"")
    base256 = _tar_header(
        "pkg/data",
        type_flag=b"0",
        size=0,
        size_field=b"\x80" + (b"\0" * 11),
    )
    corrupt = bytearray(_tar_header("pkg/data", type_flag=b"0", size=0))
    corrupt[100] ^= 0x01

    with pytest.raises(ArchiveHeaderError, match="numeric encoding"):
        _validate_tar_bytes(_manual_tar(root, (base256, b"")))
    with pytest.raises(ArchiveHeaderError, match="checksum"):
        _validate_tar_bytes(_manual_tar(root, (bytes(corrupt), b"")))


def test_tar_nonzero_member_padding_fails_closed() -> None:
    root = (_tar_header("pkg/", type_flag=b"5", size=0), b"")
    member = (_tar_header("pkg/data", type_flag=b"0", size=1), b"x")
    value = _gzip_member(_raw_tar([root, member], padding_byte=b"!"))

    with pytest.raises(ArchiveHeaderError, match="padding contains nonzero"):
        _validate_tar_bytes(value)


@pytest.mark.parametrize("field", ["extra", "name", "comment"])
def test_gzip_optional_header_fields_are_bounded(field: str) -> None:
    root = (_tar_header("pkg/", type_flag=b"5", size=0), b"")
    payload = _raw_tar([root])
    options = {field: b"abcde"}
    value = _gzip_member(payload, **options)
    limit_name = {
        "extra": "max_gzip_extra_bytes",
        "name": "max_gzip_name_bytes",
        "comment": "max_gzip_comment_bytes",
    }[field]
    limits = replace(DEFAULT_ARCHIVE_HEADER_LIMITS, **{limit_name: 4})

    with pytest.raises(ArchiveHeaderError, match=f"gzip (.*){field}.*bound"):
        _validate_tar_bytes(value, limits=limits)


def test_gzip_header_crc_is_verified_without_unbounded_reads() -> None:
    root = (_tar_header("pkg/", type_flag=b"5", size=0), b"")
    payload = _raw_tar([root])
    valid = _gzip_member(payload, name=b"worker.tar", header_crc=True)
    corrupt = bytearray(valid)
    header_crc_offset = 10 + len(b"worker.tar") + 1
    corrupt[header_crc_offset] ^= 0x01

    assert _validate_tar_bytes(valid).root_name == "pkg"
    with pytest.raises(ArchiveHeaderError, match="header CRC"):
        _validate_tar_bytes(bytes(corrupt))


def test_concatenated_gzip_members_fail_closed() -> None:
    root = (_tar_header("pkg/", type_flag=b"5", size=0), b"")
    value = _manual_tar(root)

    with pytest.raises(ArchiveHeaderError, match="concatenated or trailing"):
        _validate_tar_bytes(value + value)


def test_deflate_output_bomb_is_stopped_at_the_stream_cap() -> None:
    root = (_tar_header("pkg/", type_flag=b"5", size=0), b"")
    payload = _raw_tar([root], end_padding=b"\0" * 4096)
    value = _gzip_member(payload)
    limits = replace(
        DEFAULT_ARCHIVE_HEADER_LIMITS,
        max_members=1,
        max_member_uncompressed_bytes=1,
        max_total_uncompressed_bytes=1,
        max_tar_end_padding_bytes=1,
        max_tar_stream_bytes=2050,
    )

    with pytest.raises(ArchiveHeaderError, match="decompressed TAR stream"):
        _validate_tar_bytes(value, limits=limits)


def test_descriptor_bound_wrappers_reject_hardlinked_archives(tmp_path: Path) -> None:
    original = tmp_path / "candidate.zip"
    alias = tmp_path / "candidate-alias.zip"
    original.write_bytes(_zip_bytes())
    try:
        os.link(original, alias)
    except OSError:
        pytest.skip("hard links are unavailable on this platform")

    with pytest.raises(ArchiveHeaderError, match="single-link regular file"):
        validate_zip_headers(original, expected_root="pkg")


@pytest.mark.parametrize("archive_kind", ["zip", "tar"])
def test_validated_context_keeps_one_descriptor_across_a_path_swap(
    tmp_path: Path,
    archive_kind: str,
) -> None:
    original = _zip_bytes() if archive_kind == "zip" else _tar_gz_bytes()
    replacement = (
        _zip_bytes(file_name="evil/payload", data=b"evil")
        if archive_kind == "zip"
        else _manual_tar(
            (_tar_header("evil/", type_flag=b"5", size=0), b""),
            (_tar_header("evil/payload", type_flag=b"0", size=4), b"evil"),
        )
    )
    archive_path = tmp_path / ("candidate.zip" if archive_kind == "zip" else "x.tar.gz")
    replacement_path = tmp_path / "replacement.bin"
    archive_path.write_bytes(original)
    replacement_path.write_bytes(replacement)
    context = validated_zip_file if archive_kind == "zip" else validated_tar_gz_file

    swapped = False
    swap_error: OSError | None = None
    context_error: ArchiveHeaderError | None = None
    try:
        with context(archive_path, expected_root="pkg") as (source, index):
            try:
                os.replace(replacement_path, archive_path)
            except OSError as error:
                if os.name != "nt":
                    raise
                swap_error = error
            else:
                swapped = True
            assert index.root_name == "pkg"
            if archive_kind == "zip":
                with zipfile.ZipFile(source, "r") as archive:
                    assert archive.fp is source
                    assert archive.namelist() == ["pkg/", "pkg/data.txt"]
            else:
                with tarfile.open(fileobj=source, mode="r:gz") as archive:
                    assert archive.fileobj.fileobj is source
                    assert [member.name for member in archive.getmembers()] == [
                        "pkg",
                        "pkg/data.txt",
                    ]
            assert archive_path.read_bytes() == (replacement if swapped else original)
    except ArchiveHeaderError as error:
        context_error = error
    if swapped:
        assert context_error is not None
        assert "changed during" in str(context_error)
    else:
        assert os.name == "nt"
        assert context_error is None
        assert isinstance(swap_error, PermissionError)
        assert getattr(swap_error, "winerror", None) in {5, 32, 33}
        assert archive_path.is_file() and not archive_path.is_symlink()
        assert archive_path.read_bytes() == original
        assert replacement_path.read_bytes() == replacement
