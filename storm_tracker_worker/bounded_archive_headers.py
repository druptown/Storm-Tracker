"""Strictly bounded ZIP and gzip/TAR header validation.

This module is deliberately independent from the release builder and installer.
It validates archive structure and declared resource bounds before callers hand
an archive to :mod:`zipfile` or :mod:`tarfile`.  It never decompresses ZIP
members and streams a gzip/TAR with bounded buffers while skipping member data.

The accepted profile is intentionally small:

* single-disk, non-ZIP64 ZIP with stored or deflated members;
* no encryption or prepended bytes; descriptors must use one exact signed form;
* one ordinary gzip member with bounded optional header fields;
* POSIX USTAR regular files and directories;
* PAX, GNU long-name/link, sparse and special TAR records fail closed.

Payload hashes and CRCs still belong to a later bounded materializer.  This
module proves only that metadata parsing and every declared size are bounded.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import os
from pathlib import Path, PurePosixPath
import stat
import struct
from typing import BinaryIO, Final, Iterator
import zlib


_ZIP_EOCD: Final = struct.Struct("<4s4H2LH")
_ZIP_CENTRAL: Final = struct.Struct("<4s6H3L5H2L")
_ZIP_LOCAL: Final = struct.Struct("<4s5H3L2H")
_ZIP_DESCRIPTOR: Final = struct.Struct("<4s3L")
_ZIP_EOCD_SIGNATURE: Final = b"PK\x05\x06"
_ZIP_CENTRAL_SIGNATURE: Final = b"PK\x01\x02"
_ZIP_LOCAL_SIGNATURE: Final = b"PK\x03\x04"
_ZIP_DESCRIPTOR_SIGNATURE: Final = b"PK\x07\x08"
_ZIP64_EXTRA_ID: Final = 0x0001
_ZIP_AES_EXTRA_ID: Final = 0x9901
_ZIP_UNICODE_PATH_EXTRA_ID: Final = 0x7075
_ZIP_DATA_DESCRIPTOR_FLAG: Final = 0x0008
_ZIP_UTF8_FLAG: Final = 0x0800
_ZIP_ALLOWED_FLAGS: Final = _ZIP_UTF8_FLAG | _ZIP_DATA_DESCRIPTOR_FLAG
_ZIP_ALLOWED_METHODS: Final = frozenset({0, 8})

_GZIP_FIXED_HEADER_BYTES: Final = 10
_GZIP_TRAILER_BYTES: Final = 8
_GZIP_FLAG_FHCRC: Final = 0x02
_GZIP_FLAG_FEXTRA: Final = 0x04
_GZIP_FLAG_FNAME: Final = 0x08
_GZIP_FLAG_FCOMMENT: Final = 0x10
_GZIP_RESERVED_FLAGS: Final = 0xE0

_TAR_BLOCK_BYTES: Final = 512
_TAR_ZERO_BLOCK: Final = b"\0" * _TAR_BLOCK_BYTES
_TAR_UNSUPPORTED_EXTENSION_TYPES: Final = frozenset({b"g", b"x", b"L", b"K"})
_STREAM_CHUNK_BYTES: Final = 64 * 1024


class ArchiveHeaderError(RuntimeError):
    """Archive metadata cannot be validated inside the closed bounded profile."""


@dataclass(frozen=True, slots=True)
class ArchiveHeaderLimits:
    """Immutable byte, path and count limits for one archive preflight."""

    max_archive_bytes: int = 2 * 1024 * 1024 * 1024
    max_members: int = 20_000
    max_path_bytes: int = 1024
    max_path_part_bytes: int = 255
    max_path_depth: int = 32
    max_member_uncompressed_bytes: int = 512 * 1024 * 1024
    max_total_uncompressed_bytes: int = 2 * 1024 * 1024 * 1024
    max_zip_central_directory_bytes: int = 32 * 1024 * 1024
    max_zip_extra_bytes: int = 4096
    max_zip_entry_comment_bytes: int = 1024
    max_zip_archive_comment_bytes: int = 1024
    max_gzip_extra_bytes: int = 4096
    max_gzip_name_bytes: int = 1024
    max_gzip_comment_bytes: int = 1024
    max_tar_stream_bytes: int = 2 * 1024 * 1024 * 1024 + 32 * 1024 * 1024
    max_tar_end_padding_bytes: int = 1024 * 1024

    def __post_init__(self) -> None:
        values = tuple(getattr(self, field) for field in self.__dataclass_fields__)
        if any(type(value) is not int or value <= 0 for value in values):
            raise TypeError("archive header limits must be positive integers")
        if self.max_path_part_bytes > self.max_path_bytes:
            raise ValueError("archive path-part limit exceeds full path limit")
        if self.max_member_uncompressed_bytes > self.max_total_uncompressed_bytes:
            raise ValueError("archive member limit exceeds total member limit")
        minimum_tar_stream = (
            self.max_total_uncompressed_bytes
            + (2 * self.max_members + 2) * _TAR_BLOCK_BYTES
            + self.max_tar_end_padding_bytes
        )
        if self.max_tar_stream_bytes < minimum_tar_stream:
            raise ValueError("TAR stream limit cannot cover declared member bounds")


DEFAULT_ARCHIVE_HEADER_LIMITS: Final = ArchiveHeaderLimits()


@dataclass(frozen=True, slots=True)
class ArchiveHeaderEntry:
    """One normalized file or directory declared by an archive index."""

    path: str
    kind: str
    uncompressed_size: int
    compressed_size: int | None


@dataclass(frozen=True, slots=True)
class ArchiveHeaderIndex:
    """Bounded header-only index safe to compare with external evidence."""

    format: str
    root_name: str
    entries: tuple[ArchiveHeaderEntry, ...]
    total_uncompressed_bytes: int
    archive_bytes: int
    metadata_bytes: int


@dataclass(frozen=True, slots=True)
class _ZipCentralEntry:
    index: ArchiveHeaderEntry
    raw_name: bytes
    flags: int
    method: int
    crc32: int
    local_offset: int
    external_attributes: int


def _bounded_archive_size(value: int, limits: ArchiveHeaderLimits) -> int:
    if type(value) is not int or value < 0 or value > limits.max_archive_bytes:
        raise ArchiveHeaderError("archive byte size is outside the bounded profile")
    return value


def _read_exact(source: BinaryIO, size: int, label: str) -> bytes:
    if type(size) is not int or size < 0:
        raise ArchiveHeaderError("archive parser requested an invalid read")
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        requested = min(remaining, _STREAM_CHUNK_BYTES)
        chunk = _read_bounded(source, requested, label)
        if not chunk:
            raise ArchiveHeaderError(f"{label} is truncated")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_bounded(source: BinaryIO, size: int, label: str) -> bytes:
    if type(size) is not int or size <= 0 or size > _STREAM_CHUNK_BYTES:
        raise ArchiveHeaderError("archive parser requested an invalid bounded read")
    try:
        chunk = source.read(size)
    except (OSError, TypeError, ValueError) as error:
        raise ArchiveHeaderError(f"{label} could not be read") from error
    if type(chunk) is not bytes:
        raise ArchiveHeaderError(f"{label} returned non-byte data")
    if len(chunk) > size:
        raise ArchiveHeaderError(f"{label} returned more bytes than requested")
    return chunk


def _seek_absolute(source: BinaryIO, offset: int) -> None:
    try:
        position = source.seek(offset, os.SEEK_SET)
    except (OSError, TypeError, ValueError) as error:
        raise ArchiveHeaderError("ZIP input is not safely seekable") from error
    if type(position) is not int or position != offset:
        raise ArchiveHeaderError("ZIP input did not seek to the requested offset")


def _tell_position(source: BinaryIO) -> int:
    try:
        position = source.tell()
    except (OSError, TypeError, ValueError) as error:
        raise ArchiveHeaderError("archive input position is unavailable") from error
    if type(position) is not int or position < 0:
        raise ArchiveHeaderError("archive input position is invalid")
    return position


def _read_at(source: BinaryIO, offset: int, size: int, label: str) -> bytes:
    if type(offset) is not int or offset < 0:
        raise ArchiveHeaderError("archive parser requested an invalid offset")
    _seek_absolute(source, offset)
    return _read_exact(source, size, label)


def _validate_seekable_extent(source: BinaryIO, archive_size: int) -> None:
    _seek_absolute(source, archive_size)
    if _read_bounded(source, 1, "ZIP extent probe"):
        raise ArchiveHeaderError("ZIP stream exceeds its declared archive size")


class _ArchiveSizeReader:
    """Forward-only reader that cannot consume beyond a declared archive size."""

    def __init__(self, source: BinaryIO, archive_size: int) -> None:
        self._source = source
        self._remaining = archive_size

    def read(self, size: int = -1) -> bytes:
        if type(size) is not int or size <= 0:
            raise ArchiveHeaderError("archive parser requested an invalid bounded read")
        if self._remaining == 0:
            return b""
        requested = min(size, self._remaining)
        value = _read_bounded(self._source, requested, "bounded archive input")
        self._remaining -= len(value)
        return value

    def validate_extent(self) -> None:
        if self._remaining:
            raise ArchiveHeaderError("archive stream is shorter than its declared size")
        if _read_bounded(self._source, 1, "archive extent probe"):
            raise ArchiveHeaderError("archive stream exceeds its declared size")


def _decode_path_bytes(raw: bytes, *, utf8: bool) -> str:
    if not raw or b"\0" in raw or b"\\" in raw:
        raise ArchiveHeaderError("archive path bytes are not canonical")
    try:
        if utf8:
            return raw.decode("utf-8")
        if any(value > 0x7F for value in raw):
            raise UnicodeError("non-ASCII name without UTF-8 flag")
        return raw.decode("ascii")
    except UnicodeError as error:
        raise ArchiveHeaderError("archive path encoding is not canonical") from error


def _canonical_archive_path(
    value: str,
    *,
    expected_root: str | None,
    limits: ArchiveHeaderLimits,
) -> tuple[str, bool, str]:
    if not isinstance(value, str) or not value or "\\" in value or "\0" in value:
        raise ArchiveHeaderError("archive path is invalid")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as error:
        raise ArchiveHeaderError("archive path encoding is not canonical") from error
    if len(encoded) > limits.max_path_bytes:
        raise ArchiveHeaderError("archive path exceeds its byte bound")
    directory = value.endswith("/")
    normalized = value[:-1] if directory else value
    path = PurePosixPath(normalized)
    if (
        not normalized
        or not path.parts
        or path.is_absolute()
        or path.as_posix() != normalized
        or "." in path.parts
        or ".." in path.parts
        or len(path.parts) > limits.max_path_depth
    ):
        raise ArchiveHeaderError("archive path is not a canonical relative path")
    for part in path.parts:
        if not part or len(part.encode("utf-8")) > limits.max_path_part_bytes:
            raise ArchiveHeaderError("archive path component exceeds its bound")
        if any(ord(character) < 32 for character in part):
            raise ArchiveHeaderError("archive path contains a control character")
    root_name = path.parts[0]
    if expected_root is not None and root_name != expected_root:
        raise ArchiveHeaderError("archive member is outside the expected root")
    return normalized, directory, root_name


def _validate_expected_root(value: str | None, limits: ArchiveHeaderLimits) -> None:
    if value is None:
        return
    normalized, directory, root = _canonical_archive_path(
        value, expected_root=None, limits=limits
    )
    if directory or normalized != root or "/" in normalized:
        raise ArchiveHeaderError("expected archive root is not canonical")


def _finalize_index(
    *,
    format_name: str,
    entries: list[ArchiveHeaderEntry],
    expected_root: str | None,
    total_uncompressed: int,
    archive_size: int,
    metadata_bytes: int,
) -> ArchiveHeaderIndex:
    if not entries:
        raise ArchiveHeaderError("archive has no members")
    root_name = entries[0].path
    if entries[0].kind != "directory" or "/" in root_name:
        raise ArchiveHeaderError("archive must begin with one explicit root directory")
    if expected_root is not None and root_name != expected_root:
        raise ArchiveHeaderError("archive root differs from the expected root")
    if sum(item.path == root_name for item in entries) != 1:
        raise ArchiveHeaderError("archive root entry is not unique")
    if any(PurePosixPath(item.path).parts[0] != root_name for item in entries):
        raise ArchiveHeaderError("archive contains multiple package roots")
    return ArchiveHeaderIndex(
        format=format_name,
        root_name=root_name,
        entries=tuple(entries),
        total_uncompressed_bytes=total_uncompressed,
        archive_bytes=archive_size,
        metadata_bytes=metadata_bytes,
    )


def _validate_zip_extra(raw: bytes, limits: ArchiveHeaderLimits) -> None:
    if len(raw) > limits.max_zip_extra_bytes:
        raise ArchiveHeaderError("ZIP extra field exceeds its bound")
    position = 0
    seen: set[int] = set()
    while position < len(raw):
        if len(raw) - position < 4:
            raise ArchiveHeaderError("ZIP extra field is truncated")
        field_id, field_size = struct.unpack_from("<HH", raw, position)
        position += 4
        if field_size > len(raw) - position:
            raise ArchiveHeaderError("ZIP extra subfield exceeds its container")
        if field_id in seen:
            raise ArchiveHeaderError("ZIP extra field contains duplicate identifiers")
        seen.add(field_id)
        if field_id in {
            _ZIP64_EXTRA_ID,
            _ZIP_AES_EXTRA_ID,
            _ZIP_UNICODE_PATH_EXTRA_ID,
        }:
            raise ArchiveHeaderError("ZIP extra field requests an unsupported extension")
        position += field_size


def _find_zip_eocd(
    source: BinaryIO,
    archive_size: int,
    limits: ArchiveHeaderLimits,
) -> tuple[int, tuple[int, ...], bytes]:
    if archive_size < _ZIP_EOCD.size:
        raise ArchiveHeaderError("ZIP is shorter than its end record")
    tail_size = min(
        archive_size,
        _ZIP_EOCD.size + limits.max_zip_archive_comment_bytes,
    )
    tail_offset = archive_size - tail_size
    tail = _read_at(source, tail_offset, tail_size, "ZIP end-record window")
    for relative in range(tail_size - _ZIP_EOCD.size, -1, -1):
        if tail[relative:relative + 4] != _ZIP_EOCD_SIGNATURE:
            continue
        fixed = tail[relative:relative + _ZIP_EOCD.size]
        values = _ZIP_EOCD.unpack(fixed)
        comment_size = values[-1]
        if comment_size > limits.max_zip_archive_comment_bytes:
            continue
        if relative + _ZIP_EOCD.size + comment_size != len(tail):
            continue
        comment = tail[relative + _ZIP_EOCD.size:]
        return tail_offset + relative, values[1:-1], comment
    raise ArchiveHeaderError("ZIP end record is absent or its comment exceeds the bound")


def validate_zip_header_stream(
    source: BinaryIO,
    *,
    archive_size: int,
    expected_root: str | None = None,
    limits: ArchiveHeaderLimits = DEFAULT_ARCHIVE_HEADER_LIMITS,
) -> ArchiveHeaderIndex:
    """Validate one seekable ZIP index without invoking :class:`ZipFile`."""

    if not isinstance(limits, ArchiveHeaderLimits):
        raise TypeError("limits must be ArchiveHeaderLimits")
    size = _bounded_archive_size(archive_size, limits)
    _validate_seekable_extent(source, size)
    _validate_expected_root(expected_root, limits)
    eocd_offset, eocd, archive_comment = _find_zip_eocd(source, size, limits)
    (
        disk_number,
        central_disk,
        disk_entries,
        total_entries,
        central_size,
        central_offset,
    ) = eocd
    if disk_number != 0 or central_disk != 0 or disk_entries != total_entries:
        raise ArchiveHeaderError("multi-disk ZIP is unsupported")
    if (
        total_entries == 0xFFFF
        or central_size == 0xFFFFFFFF
        or central_offset == 0xFFFFFFFF
    ):
        raise ArchiveHeaderError("ZIP64 is unsupported")
    if total_entries > limits.max_members:
        raise ArchiveHeaderError("ZIP member count exceeds its bound")
    if central_size > limits.max_zip_central_directory_bytes:
        raise ArchiveHeaderError("ZIP central directory exceeds its byte bound")
    if central_offset + central_size != eocd_offset:
        raise ArchiveHeaderError("ZIP central directory placement is not canonical")
    if central_offset > size or central_size > size - central_offset:
        raise ArchiveHeaderError("ZIP central directory escapes the archive")
    if len(archive_comment) > limits.max_zip_archive_comment_bytes:
        raise ArchiveHeaderError("ZIP archive comment exceeds its bound")

    _seek_absolute(source, central_offset)
    central_end = central_offset + central_size
    central_entries: list[_ZipCentralEntry] = []
    names: set[str] = set()
    total_uncompressed = 0
    inferred_root: str | None = expected_root
    for _index in range(total_entries):
        if _tell_position(source) + _ZIP_CENTRAL.size > central_end:
            raise ArchiveHeaderError("ZIP central directory is truncated")
        fixed = _read_exact(source, _ZIP_CENTRAL.size, "ZIP central header")
        values = _ZIP_CENTRAL.unpack(fixed)
        if values[0] != _ZIP_CENTRAL_SIGNATURE:
            raise ArchiveHeaderError("ZIP central header signature differs")
        (
            _signature,
            _made_by,
            _needed,
            flags,
            method,
            _modified_time,
            _modified_date,
            crc32,
            compressed_size,
            uncompressed_size,
            name_size,
            extra_size,
            comment_size,
            member_disk,
            _internal_attributes,
            external_attributes,
            local_offset,
        ) = values
        if flags & ~_ZIP_ALLOWED_FLAGS:
            raise ArchiveHeaderError("ZIP member flags request an unsupported feature")
        if method not in _ZIP_ALLOWED_METHODS:
            raise ArchiveHeaderError("ZIP compression method is unsupported")
        if member_disk != 0:
            raise ArchiveHeaderError("ZIP member starts on another disk")
        if compressed_size == 0xFFFFFFFF or uncompressed_size == 0xFFFFFFFF:
            raise ArchiveHeaderError("ZIP64 member sizes are unsupported")
        if name_size == 0 or name_size > limits.max_path_bytes:
            raise ArchiveHeaderError("ZIP member name exceeds its bound")
        if extra_size > limits.max_zip_extra_bytes:
            raise ArchiveHeaderError("ZIP extra field exceeds its bound")
        if comment_size > limits.max_zip_entry_comment_bytes:
            raise ArchiveHeaderError("ZIP member comment exceeds its bound")
        variable_size = name_size + extra_size + comment_size
        if _tell_position(source) + variable_size > central_end:
            raise ArchiveHeaderError("ZIP central variable fields are truncated")
        raw_name = _read_exact(source, name_size, "ZIP central member name")
        extra = _read_exact(source, extra_size, "ZIP central extra field")
        _read_exact(source, comment_size, "ZIP central member comment")
        _validate_zip_extra(extra, limits)
        decoded = _decode_path_bytes(raw_name, utf8=bool(flags & _ZIP_UTF8_FLAG))
        path, directory, root = _canonical_archive_path(
            decoded, expected_root=inferred_root, limits=limits
        )
        if inferred_root is None:
            inferred_root = root
        if path in names:
            raise ArchiveHeaderError("ZIP contains duplicate member paths")
        names.add(path)
        unix_mode = (external_attributes >> 16) & 0xFFFF
        file_type = stat.S_IFMT(unix_mode)
        expected_type = stat.S_IFDIR if directory else stat.S_IFREG
        if file_type not in (0, expected_type):
            raise ArchiveHeaderError("ZIP member type differs from its path")
        if directory:
            if flags & _ZIP_DATA_DESCRIPTOR_FLAG:
                raise ArchiveHeaderError("ZIP directory cannot use a data descriptor")
            if uncompressed_size != 0 or compressed_size != 0:
                raise ArchiveHeaderError("ZIP directory declares payload bytes")
        else:
            if uncompressed_size > limits.max_member_uncompressed_bytes:
                raise ArchiveHeaderError("ZIP member size exceeds its bound")
            total_uncompressed += uncompressed_size
            if total_uncompressed > limits.max_total_uncompressed_bytes:
                raise ArchiveHeaderError("ZIP total member bytes exceed their bound")
        entry = ArchiveHeaderEntry(
            path=path,
            kind="directory" if directory else "file",
            uncompressed_size=uncompressed_size,
            compressed_size=compressed_size,
        )
        central_entries.append(_ZipCentralEntry(
            index=entry,
            raw_name=raw_name,
            flags=flags,
            method=method,
            crc32=crc32,
            local_offset=local_offset,
            external_attributes=external_attributes,
        ))
    if _tell_position(source) != central_end:
        raise ArchiveHeaderError("ZIP central directory size or count differs")

    spans: list[tuple[int, int]] = []
    seen_offsets: set[int] = set()
    for central in central_entries:
        if central.local_offset in seen_offsets or central.local_offset >= central_offset:
            raise ArchiveHeaderError("ZIP local-header offset is duplicated or invalid")
        seen_offsets.add(central.local_offset)
        fixed = _read_at(
            source,
            central.local_offset,
            _ZIP_LOCAL.size,
            "ZIP local header",
        )
        values = _ZIP_LOCAL.unpack(fixed)
        if values[0] != _ZIP_LOCAL_SIGNATURE:
            raise ArchiveHeaderError("ZIP local header signature differs")
        (
            _signature,
            _needed,
            flags,
            method,
            _modified_time,
            _modified_date,
            crc32,
            compressed_size,
            uncompressed_size,
            name_size,
            extra_size,
        ) = values
        if name_size == 0 or name_size > limits.max_path_bytes:
            raise ArchiveHeaderError("ZIP local member name exceeds its bound")
        if extra_size > limits.max_zip_extra_bytes:
            raise ArchiveHeaderError("ZIP local extra field exceeds its bound")
        variable_offset = central.local_offset + _ZIP_LOCAL.size
        raw_name = _read_at(
            source, variable_offset, name_size, "ZIP local member name"
        )
        extra = _read_at(
            source,
            variable_offset + name_size,
            extra_size,
            "ZIP local extra field",
        )
        _validate_zip_extra(extra, limits)
        descriptor = bool(central.flags & _ZIP_DATA_DESCRIPTOR_FLAG)
        if descriptor:
            local_sizes_match = (
                crc32 == 0
                and compressed_size == 0
                and uncompressed_size == 0
            )
        else:
            local_sizes_match = (
                crc32 == central.crc32
                and compressed_size == central.index.compressed_size
                and uncompressed_size == central.index.uncompressed_size
            )
        if (
            raw_name != central.raw_name
            or flags != central.flags
            or method != central.method
            or not local_sizes_match
        ):
            raise ArchiveHeaderError("ZIP local and central metadata differ")
        data_offset = variable_offset + name_size + extra_size
        central_compressed_size = central.index.compressed_size
        if central_compressed_size is None:
            raise ArchiveHeaderError("ZIP central compressed size is absent")
        data_end = data_offset + central_compressed_size
        record_end = data_end
        if descriptor:
            raw_descriptor = _read_at(
                source,
                data_end,
                _ZIP_DESCRIPTOR.size,
                "ZIP signed data descriptor",
            )
            (
                descriptor_signature,
                descriptor_crc32,
                descriptor_compressed_size,
                descriptor_uncompressed_size,
            ) = _ZIP_DESCRIPTOR.unpack(raw_descriptor)
            if (
                descriptor_signature != _ZIP_DESCRIPTOR_SIGNATURE
                or descriptor_crc32 != central.crc32
                or descriptor_compressed_size != central.index.compressed_size
                or descriptor_uncompressed_size != central.index.uncompressed_size
            ):
                raise ArchiveHeaderError("ZIP signed data descriptor differs")
            record_end += _ZIP_DESCRIPTOR.size
        if record_end > central_offset:
            raise ArchiveHeaderError("ZIP member payload overlaps its central directory")
        spans.append((central.local_offset, record_end))
    ordered_spans = sorted(spans)
    expected_offset = 0
    for start, end in ordered_spans:
        if start != expected_offset or end < start:
            raise ArchiveHeaderError("ZIP local records are not contiguous and canonical")
        expected_offset = end
    if expected_offset != central_offset:
        raise ArchiveHeaderError("ZIP local records do not end at the central directory")
    return _finalize_index(
        format_name="zip",
        entries=[item.index for item in central_entries],
        expected_root=expected_root,
        total_uncompressed=total_uncompressed,
        archive_size=size,
        metadata_bytes=central_size + _ZIP_EOCD.size + len(archive_comment),
    )


def _read_gzip_cstring(
    source: BinaryIO,
    *,
    maximum_bytes: int,
    label: str,
    header_crc: bytearray,
) -> int:
    consumed = 0
    for _index in range(maximum_bytes + 1):
        value = _read_exact(source, 1, label)
        consumed += 1
        header_crc.extend(value)
        if value == b"\0":
            return consumed
    raise ArchiveHeaderError(f"{label} exceeds its bound")


def _read_gzip_header(source: BinaryIO, limits: ArchiveHeaderLimits) -> int:
    fixed = _read_exact(source, _GZIP_FIXED_HEADER_BYTES, "gzip fixed header")
    if fixed[:2] != b"\x1f\x8b" or fixed[2] != 8:
        raise ArchiveHeaderError("gzip magic or compression method differs")
    flags = fixed[3]
    if flags & _GZIP_RESERVED_FLAGS:
        raise ArchiveHeaderError("gzip reserved flags are set")
    header_crc = bytearray(fixed)
    header_bytes = len(fixed)
    if flags & _GZIP_FLAG_FEXTRA:
        raw_size = _read_exact(source, 2, "gzip extra length")
        header_bytes += len(raw_size)
        header_crc.extend(raw_size)
        extra_size = struct.unpack("<H", raw_size)[0]
        if extra_size > limits.max_gzip_extra_bytes:
            raise ArchiveHeaderError("gzip extra field exceeds its bound")
        extra = _read_exact(source, extra_size, "gzip extra field")
        header_bytes += len(extra)
        header_crc.extend(extra)
    if flags & _GZIP_FLAG_FNAME:
        header_bytes += _read_gzip_cstring(
            source,
            maximum_bytes=limits.max_gzip_name_bytes,
            label="gzip original name",
            header_crc=header_crc,
        )
    if flags & _GZIP_FLAG_FCOMMENT:
        header_bytes += _read_gzip_cstring(
            source,
            maximum_bytes=limits.max_gzip_comment_bytes,
            label="gzip comment",
            header_crc=header_crc,
        )
    if flags & _GZIP_FLAG_FHCRC:
        expected = struct.unpack("<H", _read_exact(source, 2, "gzip header CRC"))[0]
        header_bytes += 2
        if expected != (zlib.crc32(header_crc) & 0xFFFF):
            raise ArchiveHeaderError("gzip header CRC differs")
    return header_bytes


class _BoundedDeflateReader:
    """Small-buffer raw-deflate reader with gzip trailer verification."""

    def __init__(self, source: BinaryIO, *, maximum_output_bytes: int) -> None:
        self._source = source
        self._maximum_output_bytes = maximum_output_bytes
        self._decoder = zlib.decompressobj(-zlib.MAX_WBITS)
        self._compressed_pending = b""
        self._output = bytearray()
        self._post_deflate = bytearray()
        self._eof = False
        self._crc32 = 0
        self.total_output_bytes = 0

    def _fill(self) -> None:
        while not self._output and not self._eof:
            if self._compressed_pending:
                incoming = self._compressed_pending
                self._compressed_pending = b""
            else:
                incoming = _read_bounded(
                    self._source,
                    _STREAM_CHUNK_BYTES,
                    "gzip deflate stream",
                )
                if not incoming:
                    raise ArchiveHeaderError("gzip deflate stream is truncated")
            remaining = self._maximum_output_bytes - self.total_output_bytes
            maximum = min(_STREAM_CHUNK_BYTES, max(1, remaining + 1))
            try:
                output = self._decoder.decompress(incoming, maximum)
            except zlib.error as error:
                raise ArchiveHeaderError("gzip deflate stream is invalid") from error
            self._compressed_pending = self._decoder.unconsumed_tail
            if output:
                self.total_output_bytes += len(output)
                if self.total_output_bytes > self._maximum_output_bytes:
                    raise ArchiveHeaderError("decompressed TAR stream exceeds its bound")
                self._crc32 = zlib.crc32(output, self._crc32)
                self._output.extend(output)
            if self._decoder.eof:
                self._eof = True
                self._post_deflate.extend(self._decoder.unused_data)
                self._compressed_pending = b""

    def read_exact(self, size: int, label: str) -> bytes:
        chunks: list[bytes] = []
        remaining = size
        while remaining:
            self._fill()
            if not self._output:
                raise ArchiveHeaderError(f"{label} is truncated")
            amount = min(remaining, len(self._output))
            chunks.append(bytes(self._output[:amount]))
            del self._output[:amount]
            remaining -= amount
        return b"".join(chunks)

    def discard(self, size: int, label: str) -> None:
        remaining = size
        while remaining:
            self._fill()
            if not self._output:
                raise ArchiveHeaderError(f"{label} is truncated")
            amount = min(remaining, len(self._output))
            del self._output[:amount]
            remaining -= amount

    def discard_zeroes(self, size: int, label: str) -> None:
        remaining = size
        while remaining:
            self._fill()
            if not self._output:
                raise ArchiveHeaderError(f"{label} is truncated")
            amount = min(remaining, len(self._output))
            if any(self._output[:amount]):
                raise ArchiveHeaderError(f"{label} contains nonzero bytes")
            del self._output[:amount]
            remaining -= amount

    def read_some(self) -> bytes:
        self._fill()
        if not self._output:
            return b""
        value = bytes(self._output[:_STREAM_CHUNK_BYTES])
        del self._output[:len(value)]
        return value

    def verify_trailer_and_eof(self) -> None:
        while not self._eof:
            self._fill()
            if self._output:
                raise ArchiveHeaderError("gzip payload was not fully consumed")
        while len(self._post_deflate) < _GZIP_TRAILER_BYTES:
            chunk = _read_bounded(
                self._source,
                _GZIP_TRAILER_BYTES - len(self._post_deflate),
                "gzip trailer",
            )
            if not chunk:
                raise ArchiveHeaderError("gzip trailer is truncated")
            self._post_deflate.extend(chunk)
        expected_crc32, expected_size = struct.unpack(
            "<II", self._post_deflate[:_GZIP_TRAILER_BYTES]
        )
        trailing = self._post_deflate[_GZIP_TRAILER_BYTES:]
        if trailing or _read_bounded(self._source, 1, "gzip trailing data"):
            raise ArchiveHeaderError("concatenated or trailing gzip data is unsupported")
        if expected_crc32 != self._crc32:
            raise ArchiveHeaderError("gzip payload CRC differs")
        if expected_size != (self.total_output_bytes & 0xFFFFFFFF):
            raise ArchiveHeaderError("gzip payload size trailer differs")


def _parse_tar_octal(raw: bytes, label: str) -> int:
    if not raw or raw[0] & 0x80:
        raise ArchiveHeaderError(f"TAR {label} uses unsupported numeric encoding")
    stripped = raw.strip(b" \0")
    if not stripped:
        return 0
    if any(value < ord("0") or value > ord("7") for value in stripped):
        raise ArchiveHeaderError(f"TAR {label} is not canonical octal")
    return int(stripped, 8)


def _tar_text(raw: bytes, label: str) -> bytes:
    before, separator, after = raw.partition(b"\0")
    if separator and any(after):
        raise ArchiveHeaderError(f"TAR {label} has nonzero bytes after NUL")
    return before if separator else raw.rstrip(b" ")


def _tar_member_path(header: bytes, limits: ArchiveHeaderLimits) -> str:
    name = _tar_text(header[0:100], "name")
    prefix = _tar_text(header[345:500], "prefix")
    if not name:
        raise ArchiveHeaderError("TAR member name is empty")
    raw = prefix + (b"/" if prefix else b"") + name
    return _decode_path_bytes(raw, utf8=False)


def _validate_tar_checksum(header: bytes) -> None:
    expected = _parse_tar_octal(header[148:156], "checksum")
    calculated = sum(header[:148]) + (8 * ord(" ")) + sum(header[156:])
    if expected != calculated:
        raise ArchiveHeaderError("TAR header checksum differs")


def validate_tar_gz_header_stream(
    source: BinaryIO,
    *,
    archive_size: int,
    expected_root: str | None = None,
    limits: ArchiveHeaderLimits = DEFAULT_ARCHIVE_HEADER_LIMITS,
) -> ArchiveHeaderIndex:
    """Stream one gzip/TAR index with bounded buffers and no ``getmembers``."""

    if not isinstance(limits, ArchiveHeaderLimits):
        raise TypeError("limits must be ArchiveHeaderLimits")
    size = _bounded_archive_size(archive_size, limits)
    _validate_expected_root(expected_root, limits)
    bounded_source = _ArchiveSizeReader(source, size)
    header_bytes = _read_gzip_header(bounded_source, limits)
    if header_bytes + _GZIP_TRAILER_BYTES >= size:
        raise ArchiveHeaderError("gzip archive has no bounded deflate payload")
    reader = _BoundedDeflateReader(
        bounded_source, maximum_output_bytes=limits.max_tar_stream_bytes
    )
    entries: list[ArchiveHeaderEntry] = []
    names: set[str] = set()
    total_uncompressed = 0
    inferred_root: str | None = expected_root
    zero_blocks = 0
    while zero_blocks < 2:
        header = reader.read_exact(_TAR_BLOCK_BYTES, "TAR header")
        if header == _TAR_ZERO_BLOCK:
            zero_blocks += 1
            continue
        if zero_blocks:
            raise ArchiveHeaderError("TAR has a lone zero block before another member")
        _validate_tar_checksum(header)
        _parse_tar_octal(header[100:108], "mode")
        _parse_tar_octal(header[108:116], "owner id")
        _parse_tar_octal(header[116:124], "group id")
        _parse_tar_octal(header[136:148], "modification time")
        _parse_tar_octal(header[329:337], "device major number")
        _parse_tar_octal(header[337:345], "device minor number")
        magic = header[257:263]
        version = header[263:265]
        if magic != b"ustar\0" or version != b"00":
            raise ArchiveHeaderError("TAR is outside the POSIX USTAR profile")
        type_flag = header[156:157]
        if type_flag in _TAR_UNSUPPORTED_EXTENSION_TYPES:
            raise ArchiveHeaderError("PAX and GNU TAR extension records are unsupported")
        if type_flag not in (b"\0", b"0", b"5"):
            raise ArchiveHeaderError("TAR special member type is unsupported")
        if any(header[157:257]):
            raise ArchiveHeaderError("TAR link target is not allowed")
        path_text = _tar_member_path(header, limits)
        path, directory_suffix, root = _canonical_archive_path(
            path_text, expected_root=inferred_root, limits=limits
        )
        if inferred_root is None:
            inferred_root = root
        directory = type_flag == b"5"
        if directory_suffix != directory:
            raise ArchiveHeaderError("TAR member type differs from its path suffix")
        if path in names:
            raise ArchiveHeaderError("TAR contains duplicate member paths")
        names.add(path)
        if len(entries) >= limits.max_members:
            raise ArchiveHeaderError("TAR member count exceeds its bound")
        member_size = _parse_tar_octal(header[124:136], "member size")
        if directory:
            if member_size != 0:
                raise ArchiveHeaderError("TAR directory declares payload bytes")
        else:
            if member_size > limits.max_member_uncompressed_bytes:
                raise ArchiveHeaderError("TAR member size exceeds its bound")
            total_uncompressed += member_size
            if total_uncompressed > limits.max_total_uncompressed_bytes:
                raise ArchiveHeaderError("TAR total member bytes exceed their bound")
        entries.append(ArchiveHeaderEntry(
            path=path,
            kind="directory" if directory else "file",
            uncompressed_size=member_size,
            compressed_size=None,
        ))
        reader.discard(member_size, "TAR member payload")
        padding_size = (-member_size) % _TAR_BLOCK_BYTES
        reader.discard_zeroes(padding_size, "TAR member padding")

    end_padding = 0
    while True:
        chunk = reader.read_some()
        if not chunk:
            break
        end_padding += len(chunk)
        if end_padding > limits.max_tar_end_padding_bytes:
            raise ArchiveHeaderError("TAR end padding exceeds its bound")
        if any(chunk):
            raise ArchiveHeaderError("TAR contains nonzero bytes after its end blocks")
    if reader.total_output_bytes % _TAR_BLOCK_BYTES:
        raise ArchiveHeaderError("TAR decompressed size is not block aligned")
    reader.verify_trailer_and_eof()
    bounded_source.validate_extent()
    return _finalize_index(
        format_name="tar.gz",
        entries=entries,
        expected_root=expected_root,
        total_uncompressed=total_uncompressed,
        archive_size=size,
        metadata_bytes=(
            header_bytes
            + len(entries) * _TAR_BLOCK_BYTES
            + 2 * _TAR_BLOCK_BYTES
            + end_padding
            + _GZIP_TRAILER_BYTES
        ),
    )


@contextmanager
def _open_bound_archive(
    path: Path,
    limits: ArchiveHeaderLimits,
) -> Iterator[tuple[BinaryIO, int]]:
    archive_path = Path(path)
    try:
        before = archive_path.lstat()
    except OSError as error:
        raise ArchiveHeaderError("archive file is unavailable") from error
    if (
        archive_path.is_symlink()
        or not stat.S_ISREG(before.st_mode)
        or int(before.st_nlink) != 1
    ):
        raise ArchiveHeaderError("archive is not a single-link regular file")
    size = _bounded_archive_size(int(before.st_size), limits)
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(str(archive_path), flags)
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or int(opened.st_nlink) != 1
            or int(opened.st_dev) != int(before.st_dev)
            or int(opened.st_ino) != int(before.st_ino)
            or int(opened.st_size) != size
        ):
            raise ArchiveHeaderError("archive changed before its descriptor was bound")
        with os.fdopen(descriptor, "rb", closefd=True) as source:
            descriptor = None
            yield source, size
            after = os.fstat(source.fileno())
            try:
                lexical_after = archive_path.lstat()
            except OSError as error:
                raise ArchiveHeaderError(
                    "archive path disappeared during header validation"
                ) from error
            if (
                int(after.st_dev) != int(opened.st_dev)
                or int(after.st_ino) != int(opened.st_ino)
                or int(after.st_size) != int(opened.st_size)
                or int(after.st_mtime_ns) != int(opened.st_mtime_ns)
                or int(after.st_nlink) != 1
                or archive_path.is_symlink()
                or not stat.S_ISREG(lexical_after.st_mode)
                or int(lexical_after.st_dev) != int(opened.st_dev)
                or int(lexical_after.st_ino) != int(opened.st_ino)
                or int(lexical_after.st_size) != int(opened.st_size)
                or int(lexical_after.st_mtime_ns) != int(opened.st_mtime_ns)
                or int(lexical_after.st_nlink) != 1
            ):
                raise ArchiveHeaderError("archive changed during header validation")
    except ArchiveHeaderError:
        raise
    except OSError as error:
        raise ArchiveHeaderError("archive could not be opened safely") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


def validate_zip_headers(
    path: Path,
    *,
    expected_root: str | None = None,
    limits: ArchiveHeaderLimits = DEFAULT_ARCHIVE_HEADER_LIMITS,
) -> ArchiveHeaderIndex:
    """Validate a local ZIP using a descriptor-bound bounded parser."""

    if not isinstance(limits, ArchiveHeaderLimits):
        raise TypeError("limits must be ArchiveHeaderLimits")
    with _open_bound_archive(path, limits) as (source, size):
        return validate_zip_header_stream(
            source,
            archive_size=size,
            expected_root=expected_root,
            limits=limits,
        )


def validate_tar_gz_headers(
    path: Path,
    *,
    expected_root: str | None = None,
    limits: ArchiveHeaderLimits = DEFAULT_ARCHIVE_HEADER_LIMITS,
) -> ArchiveHeaderIndex:
    """Validate a local gzip/TAR using a descriptor-bound streaming parser."""

    if not isinstance(limits, ArchiveHeaderLimits):
        raise TypeError("limits must be ArchiveHeaderLimits")
    with _open_bound_archive(path, limits) as (source, size):
        return validate_tar_gz_header_stream(
            source,
            archive_size=size,
            expected_root=expected_root,
            limits=limits,
        )


@contextmanager
def validated_zip_file(
    path: Path,
    *,
    expected_root: str | None = None,
    limits: ArchiveHeaderLimits = DEFAULT_ARCHIVE_HEADER_LIMITS,
) -> Iterator[tuple[BinaryIO, ArchiveHeaderIndex]]:
    """Yield the same descriptor whose bounded ZIP index was validated."""

    if not isinstance(limits, ArchiveHeaderLimits):
        raise TypeError("limits must be ArchiveHeaderLimits")
    with _open_bound_archive(path, limits) as (source, size):
        index = validate_zip_header_stream(
            source,
            archive_size=size,
            expected_root=expected_root,
            limits=limits,
        )
        _seek_absolute(source, 0)
        yield source, index


@contextmanager
def validated_tar_gz_file(
    path: Path,
    *,
    expected_root: str | None = None,
    limits: ArchiveHeaderLimits = DEFAULT_ARCHIVE_HEADER_LIMITS,
) -> Iterator[tuple[BinaryIO, ArchiveHeaderIndex]]:
    """Yield the same descriptor whose bounded gzip/TAR index was validated."""

    if not isinstance(limits, ArchiveHeaderLimits):
        raise TypeError("limits must be ArchiveHeaderLimits")
    with _open_bound_archive(path, limits) as (source, size):
        index = validate_tar_gz_header_stream(
            source,
            archive_size=size,
            expected_root=expected_root,
            limits=limits,
        )
        _seek_absolute(source, 0)
        yield source, index


__all__ = [
    "ArchiveHeaderEntry",
    "ArchiveHeaderError",
    "ArchiveHeaderIndex",
    "ArchiveHeaderLimits",
    "DEFAULT_ARCHIVE_HEADER_LIMITS",
    "validated_tar_gz_file",
    "validated_zip_file",
    "validate_tar_gz_header_stream",
    "validate_tar_gz_headers",
    "validate_zip_header_stream",
    "validate_zip_headers",
]
