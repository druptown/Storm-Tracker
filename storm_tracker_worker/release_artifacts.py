"""Deterministic, fail-closed builders for local Storm Tracker releases.

The evidence document is the commit marker for a candidate pair.  Archives are
created and fully re-read before that marker is published.  This module never
installs or deploys anything and refuses to replace an existing artefact.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tarfile
import tempfile
from typing import TYPE_CHECKING, BinaryIO, Iterable, Mapping
import zipfile

from .archive_member_stream import (
    ArchiveMemberStreamError,
    PRIVATE_KEY_MARKERS,
    stream_archive_member,
)
from .bounded_archive_headers import (
    ArchiveHeaderError,
    ArchiveHeaderIndex,
    ArchiveHeaderLimits,
    validate_tar_gz_header_stream,
    validate_zip_header_stream,
    validated_tar_gz_file,
    validated_zip_file,
)

if TYPE_CHECKING:
    from release_install_bound_inputs import BoundInputSource


RELEASE_EVIDENCE_CONTRACT = "stv3-local-release-artifact-evidence-v1"
INTEGRATION_ROOT_NAME = "storm_tracker_v3"
WORKER_ROOT_NAME = "storm_tracker_worker"
MAX_FILE_COUNT = 10_000
MAX_ARCHIVE_MEMBER_COUNT = 2 * MAX_FILE_COUNT + 1
MAX_SOURCE_FILE_BYTES = 512 * 1024 * 1024
MAX_TOTAL_SOURCE_BYTES = 2 * 1024 * 1024 * 1024
MAX_RELEASE_EVIDENCE_BYTES = 64 * 1024 * 1024
MAX_RELEASE_EVIDENCE_JSON_DEPTH = 64
MAX_RELEASE_EVIDENCE_JSON_NODES = 100_000
MAX_RELEASE_EVIDENCE_JSON_KEY_BYTES = 256
MAX_RELEASE_EVIDENCE_JSON_STRING_BYTES = 1024 * 1024
MAX_RELEASE_EVIDENCE_JSON_INTEGER_ABSOLUTE = MAX_TOTAL_SOURCE_BYTES
BOUND_RELEASE_INPUT_KEYS = ("evidence", "integration", "worker")

_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
_WORKER_VERSION = re.compile(
    r'^version:\s*["\']?([0-9]+\.[0-9]+\.[0-9]+)["\']?\s*$',
    re.MULTILINE,
)
_PRIVATE_KEY = re.compile(
    br"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"
)
_BANNED_DIRECTORY_NAMES = frozenset({
    ".analysis",
    ".calibration-data",
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    "__pycache__",
    "node_modules",
    "tests",
})
_BANNED_FILE_SUFFIXES = (
    ".db",
    ".key",
    ".log",
    ".p12",
    ".pem",
    ".pfx",
    ".pyc",
    ".pyo",
    ".shm",
    ".sqlite",
    ".sqlite3",
    ".tmp",
    ".wal",
    ".zip",
)
_BANNED_FILE_NAMES = frozenset({".env", "thumbs.db", ".ds_store"})


class ReleaseArtifactError(RuntimeError):
    """The candidate cannot be built or verified without weakening a gate."""


@dataclass(frozen=True, slots=True)
class SourceFile:
    relative_path: str
    size: int
    sha256: str
    mode: int

    def to_mapping(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "path": self.relative_path,
            "sha256": self.sha256,
            "size": self.size,
        }


@dataclass(frozen=True, slots=True)
class SourceTree:
    root_name: str
    files: tuple[SourceFile, ...]
    total_bytes: int
    content_sha256: str

    def to_mapping(self) -> dict[str, object]:
        return {
            "content_sha256": self.content_sha256,
            "file_count": len(self.files),
            "files": [item.to_mapping() for item in self.files],
            "root_name": self.root_name,
            "total_bytes": self.total_bytes,
        }


def _logical_directories(files: Iterable[SourceFile]) -> tuple[str, ...]:
    return tuple(sorted({
        parent.as_posix()
        for item in files
        for parent in PurePosixPath(item.relative_path).parents
        if parent != PurePosixPath(".")
    }, key=lambda value: (value.count("/"), value)))


def _assert_ustar_tree_representable(tree: SourceTree) -> None:
    paths = (
        f"{tree.root_name}/",
        *(f"{tree.root_name}/{name}/" for name in _logical_directories(tree.files)),
        *(f"{tree.root_name}/{item.relative_path}" for item in tree.files),
    )
    for archive_path in paths:
        try:
            archive_path.encode("ascii", "strict")
            tarfile.TarInfo(archive_path).tobuf(
                format=tarfile.USTAR_FORMAT,
                encoding="ascii",
                errors="strict",
            )
        except (UnicodeError, ValueError) as error:
            raise ReleaseArtifactError(
                "pakketpad past niet in het vaste ASCII USTAR-profiel"
            ) from error


@dataclass(frozen=True, slots=True)
class BuiltReleaseCandidate:
    integration_archive: Path
    worker_archive: Path
    evidence_path: Path
    integration_sha256: str
    worker_sha256: str
    evidence_sha256: str


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _rewind_bound_stream(source: BinaryIO) -> None:
    try:
        position = source.seek(0, os.SEEK_SET)
    except (OSError, TypeError, ValueError) as error:
        raise ReleaseArtifactError(
            "gebonden release-input is niet veilig seekable"
        ) from error
    if type(position) is not int or position != 0:
        raise ReleaseArtifactError(
            "gebonden release-input keerde niet terug naar zijn begin"
        )


def _hash_bound_stream(
    source: BinaryIO,
    *,
    expected_size: int,
    maximum_size: int,
    collect: bool = False,
) -> tuple[str, bytes | None]:
    """Hash exactly one bounded descriptor stream and rewind that descriptor."""

    if (
        type(expected_size) is not int
        or expected_size < 0
        or type(maximum_size) is not int
        or maximum_size < 0
        or expected_size > maximum_size
    ):
        raise ReleaseArtifactError("gebonden release-input heeft een ongeldige grootte")
    _rewind_bound_stream(source)
    digest = hashlib.sha256()
    chunks: list[bytes] | None = [] if collect else None
    total = 0
    try:
        while True:
            request = min(1024 * 1024, expected_size - total + 1)
            if request <= 0:
                raise ReleaseArtifactError(
                    "gebonden release-input overschrijdt zijn verklaarde grootte"
                )
            chunk = source.read(request)
            if type(chunk) is not bytes or len(chunk) > request:
                raise ReleaseArtifactError(
                    "gebonden release-input leverde ongeldige binaire data"
                )
            if not chunk:
                break
            total += len(chunk)
            if total > expected_size:
                raise ReleaseArtifactError(
                    "gebonden release-input overschrijdt zijn verklaarde grootte"
                )
            digest.update(chunk)
            if chunks is not None:
                chunks.append(chunk)
    except ReleaseArtifactError:
        raise
    except (OSError, TypeError, ValueError) as error:
        raise ReleaseArtifactError(
            "gebonden release-input kon niet begrensd worden gelezen"
        ) from error
    finally:
        _rewind_bound_stream(source)
    if total != expected_size:
        raise ReleaseArtifactError(
            "gebonden release-inputgrootte verschilt van zijn seal"
        )
    return digest.hexdigest(), (b"".join(chunks) if chunks is not None else None)


def _version(value: object, label: str) -> str:
    if not isinstance(value, str) or _VERSION.fullmatch(value) is None:
        raise ReleaseArtifactError(f"{label} is geen canonieke x.y.z-versie")
    return value


def _safe_member_name(value: str, expected_root: str) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ReleaseArtifactError("archief bevat een ongeldig pad")
    member = PurePosixPath(value)
    if member.is_absolute() or ".." in member.parts:
        raise ReleaseArtifactError("archief bevat path traversal")
    if not member.parts or member.parts[0] != expected_root:
        raise ReleaseArtifactError("archief bevat een bestand buiten de topmap")
    return member


def _canonical_mode(relative_path: str) -> int:
    return 0o755 if relative_path.endswith(".sh") else 0o644


def _validate_source_path(root: Path, path: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise ReleaseArtifactError("bronpad ontsnapt uit de pakketroot") from error
    parts = relative.parts
    if any(part.lower() in _BANNED_DIRECTORY_NAMES for part in parts[:-1]):
        raise ReleaseArtifactError(f"verboden pakketmap: {relative.as_posix()}")
    name = path.name.lower()
    if name in _BANNED_FILE_NAMES or name.endswith("~"):
        raise ReleaseArtifactError(f"verboden pakketbestand: {relative.as_posix()}")
    if name.endswith(_BANNED_FILE_SUFFIXES) or name.endswith(".tar.gz"):
        raise ReleaseArtifactError(f"verboden pakketbestand: {relative.as_posix()}")
    if path.is_symlink() or path.is_dir():
        raise ReleaseArtifactError(f"pakketentry is geen regulier bestand: {relative.as_posix()}")
    try:
        file_stat = path.stat(follow_symlinks=False)
    except OSError as error:
        raise ReleaseArtifactError("pakketbestand kon niet worden gelezen") from error
    if not stat.S_ISREG(file_stat.st_mode):
        raise ReleaseArtifactError(f"pakketentry is niet regulier: {relative.as_posix()}")
    if file_stat.st_size > MAX_SOURCE_FILE_BYTES:
        raise ReleaseArtifactError(
            f"pakketbestand overschrijdt de bytegrens: {relative.as_posix()}"
        )


def inspect_source_tree(root: Path, *, expected_root_name: str) -> SourceTree:
    """Return a bounded content manifest after rejecting unsafe entries."""

    source_argument = Path(root)
    if source_argument.is_symlink():
        raise ReleaseArtifactError("pakketroot mag geen symlink zijn")
    source_root = source_argument.resolve(strict=True)
    if source_root.name != expected_root_name or not source_root.is_dir():
        raise ReleaseArtifactError(
            f"pakketroot moet exact {expected_root_name!r} heten"
        )
    paths: list[Path] = []
    for current, directories, filenames in os.walk(source_root, followlinks=False):
        current_path = Path(current)
        for directory in tuple(directories):
            directory_path = current_path / directory
            relative = directory_path.relative_to(source_root)
            if directory_path.is_symlink():
                raise ReleaseArtifactError(
                    f"pakketmap mag geen symlink zijn: {relative.as_posix()}"
                )
            if directory.lower() in _BANNED_DIRECTORY_NAMES:
                raise ReleaseArtifactError(
                    f"verboden pakketmap: {relative.as_posix()}"
                )
        for filename in filenames:
            paths.append(current_path / filename)
            if len(paths) > MAX_FILE_COUNT:
                raise ReleaseArtifactError("pakket overschrijdt de bestandsgrens")
    records: list[SourceFile] = []
    total = 0
    for path in sorted(paths, key=lambda item: item.relative_to(source_root).as_posix()):
        _validate_source_path(source_root, path)
        relative = path.relative_to(source_root).as_posix()
        data = path.read_bytes()
        if _PRIVATE_KEY.search(data):
            raise ReleaseArtifactError(f"private key aangetroffen in {relative}")
        total += len(data)
        if total > MAX_TOTAL_SOURCE_BYTES:
            raise ReleaseArtifactError("pakket overschrijdt de totale bytegrens")
        records.append(SourceFile(
            relative_path=relative,
            size=len(data),
            sha256=_sha_bytes(data),
            mode=_canonical_mode(relative),
        ))
    if not records:
        raise ReleaseArtifactError("pakketroot bevat geen bestanden")
    logical_directories = _logical_directories(records)
    if 1 + len(logical_directories) + len(records) > MAX_ARCHIVE_MEMBER_COUNT:
        raise ReleaseArtifactError("pakket overschrijdt de archiefmembergrens")
    mapping = [item.to_mapping() for item in records]
    tree = SourceTree(
        root_name=expected_root_name,
        files=tuple(records),
        total_bytes=total,
        content_sha256=_sha_bytes(_canonical(mapping)),
    )
    _assert_ustar_tree_representable(tree)
    return tree


def _validate_versions(
    integration_root: Path,
    worker_root: Path,
    integration_version: str,
    worker_version: str,
) -> None:
    manifest_path = integration_root / "manifest.json"
    integration_init_path = integration_root / "__init__.py"
    config_path = worker_root / "config.yaml"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ReleaseArtifactError("integratiemanifest is niet geldige JSON") from error
    if not isinstance(manifest, dict):
        raise ReleaseArtifactError("integratiemanifest moet een object zijn")
    if manifest.get("domain") != "storm_tracker_v3":
        raise ReleaseArtifactError("integratiemanifest heeft het verkeerde domein")
    if manifest.get("version") != integration_version:
        raise ReleaseArtifactError("integratieversie verschilt van de aanvraag")
    try:
        config = config_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise ReleaseArtifactError("workerconfig kon niet worden gelezen") from error
    versions = _WORKER_VERSION.findall(config)
    if versions != [worker_version]:
        raise ReleaseArtifactError("workerconfig bevat niet exact de gevraagde versie")
    try:
        worker_init = ast.parse(
            (worker_root / "__init__.py").read_text(encoding="utf-8"),
            filename="storm_tracker_worker/__init__.py",
        )
    except (OSError, UnicodeError, SyntaxError) as error:
        raise ReleaseArtifactError("worker __init__ kon niet veilig worden geparseerd") from error
    declared_versions = []
    for statement in worker_init.body:
        if not isinstance(statement, (ast.Assign, ast.AnnAssign)):
            continue
        targets = (
            statement.targets
            if isinstance(statement, ast.Assign)
            else [statement.target]
        )
        if not any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in targets
        ):
            continue
        value = statement.value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            declared_versions.append(value.value)
        else:
            raise ReleaseArtifactError("worker __version__ moet een vaste tekstliteral zijn")
    if declared_versions != [worker_version]:
        raise ReleaseArtifactError("worker __version__ verschilt van config en aanvraag")
    try:
        integration_init = ast.parse(
            integration_init_path.read_text(encoding="utf-8"),
            filename="storm_tracker_v3/__init__.py",
        )
    except (OSError, UnicodeError, SyntaxError) as error:
        raise ReleaseArtifactError(
            "integratie __init__ kon niet veilig worden geparseerd"
        ) from error
    build_id_values = [
        keyword.value
        for node in ast.walk(integration_init)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if keyword.arg == "build_id"
    ]
    if len(build_id_values) != 1:
        raise ReleaseArtifactError(
            "integratie __init__ moet exact één vaste build_id bevatten"
        )
    build_id_value = build_id_values[0]
    if not (
        isinstance(build_id_value, ast.Constant)
        and isinstance(build_id_value.value, str)
    ):
        raise ReleaseArtifactError(
            "integratie build_id moet een vaste tekstliteral zijn"
        )
    expected_build_id = (
        f"integration-{integration_version}-worker-{worker_version}"
    )
    if build_id_value.value != expected_build_id:
        raise ReleaseArtifactError(
            "integratie build_id verschilt van manifest, worker en aanvraag"
        )
    if not (worker_root / "run.sh").is_file():
        raise ReleaseArtifactError("workerpakket mist run.sh")
    if not (worker_root / "service.py").is_file():
        raise ReleaseArtifactError("workerpakket mist service.py")


def _write_zip(path: Path, root: Path, tree: SourceTree) -> None:
    with path.open("xb") as raw:
        with zipfile.ZipFile(
            raw,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
            strict_timestamps=True,
        ) as archive:
            root_info = zipfile.ZipInfo(f"{tree.root_name}/", (1980, 1, 1, 0, 0, 0))
            root_info.create_system = 3
            root_info.external_attr = (0o40755 << 16) | 0x10
            archive.writestr(root_info, b"")
            for item in tree.files:
                info = zipfile.ZipInfo(
                    f"{tree.root_name}/{item.relative_path}",
                    (1980, 1, 1, 0, 0, 0),
                )
                info.compress_type = zipfile.ZIP_DEFLATED
                info.create_system = 3
                info.external_attr = ((0o100000 | item.mode) << 16)
                archive.writestr(info, (root / item.relative_path).read_bytes())
        raw.flush()
        os.fsync(raw.fileno())


def _tar_info(name: str, *, directory: bool, size: int = 0, mode: int = 0o644) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.uid = 0
    info.gid = 0
    info.uname = "root"
    info.gname = "root"
    info.mtime = 0
    info.mode = 0o755 if directory else mode
    info.type = tarfile.DIRTYPE if directory else tarfile.REGTYPE
    info.size = 0 if directory else size
    return info


def _write_tar_gz(path: Path, root: Path, tree: SourceTree) -> None:
    with path.open("xb") as raw:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=raw,
            compresslevel=9,
            mtime=0,
        ) as compressed:
            with tarfile.open(
                fileobj=compressed,
                mode="w",
                format=tarfile.USTAR_FORMAT,
            ) as archive:
                archive.addfile(_tar_info(f"{tree.root_name}/", directory=True))
                directory_names = _logical_directories(tree.files)
                for directory in directory_names:
                    archive.addfile(_tar_info(
                        f"{tree.root_name}/{directory}/",
                        directory=True,
                    ))
                for item in tree.files:
                    data = (root / item.relative_path).read_bytes()
                    archive.addfile(
                        _tar_info(
                            f"{tree.root_name}/{item.relative_path}",
                            directory=False,
                            size=len(data),
                            mode=item.mode,
                        ),
                        io.BytesIO(data),
                    )
        raw.flush()
        os.fsync(raw.fileno())


def _expected_files(tree: SourceTree) -> dict[str, SourceFile]:
    return {f"{tree.root_name}/{item.relative_path}": item for item in tree.files}


def _archive_header_limits() -> ArchiveHeaderLimits:
    return ArchiveHeaderLimits(
        max_members=MAX_ARCHIVE_MEMBER_COUNT,
        max_member_uncompressed_bytes=MAX_SOURCE_FILE_BYTES,
        max_total_uncompressed_bytes=MAX_TOTAL_SOURCE_BYTES,
    )


def _expected_header_entries(
    tree: SourceTree,
    format_name: str,
) -> tuple[tuple[str, str, int], ...]:
    directories: tuple[str, ...] = ()
    if format_name == "tar.gz":
        directories = _logical_directories(tree.files)
    return (
        (tree.root_name, "directory", 0),
        *tuple(
            (f"{tree.root_name}/{name}", "directory", 0)
            for name in directories
        ),
        *tuple(
            (f"{tree.root_name}/{item.relative_path}", "file", item.size)
            for item in tree.files
        ),
    )


def _assert_header_index(index: ArchiveHeaderIndex, tree: SourceTree) -> None:
    observed = tuple(sorted(
        (item.path, item.kind, item.uncompressed_size) for item in index.entries
    ))
    if (
        index.root_name != tree.root_name
        or index.total_uncompressed_bytes != tree.total_bytes
        or observed != tuple(sorted(_expected_header_entries(tree, index.format)))
    ):
        raise ReleaseArtifactError(
            "begrensde archiefheaderindex verschilt van de bronboom"
        )


def _verify_zip_payload(
    archive_source: BinaryIO,
    header_index: ArchiveHeaderIndex,
    tree: SourceTree,
) -> None:
    expected = _expected_files(tree)
    _assert_header_index(header_index, tree)
    with zipfile.ZipFile(archive_source, "r") as archive:
        infos = archive.infolist()
        names = [item.filename for item in infos]
        if len(names) != len(set(names)):
            raise ReleaseArtifactError("integratiearchief bevat dubbele entries")
        for name in names:
            _safe_member_name(name, tree.root_name)
        files = {item.filename: item for item in infos if not item.is_dir()}
        if set(files) != set(expected):
            raise ReleaseArtifactError("integratiearchiefbestandset verschilt van bron")
        for name, source in expected.items():
            info = files[name]
            if info.file_size != source.size or source.size > MAX_SOURCE_FILE_BYTES:
                raise ReleaseArtifactError("integratiearchief bevat gewijzigde grootte")
            with archive.open(info, "r") as extracted:
                stream_archive_member(
                    extracted,
                    expected_size=source.size,
                    maximum_size=MAX_SOURCE_FILE_BYTES,
                    expected_sha256=source.sha256,
                    forbidden_markers=PRIVATE_KEY_MARKERS,
                )


def _verify_zip(path: Path, tree: SourceTree) -> None:
    try:
        with validated_zip_file(
            path,
            expected_root=tree.root_name,
            limits=_archive_header_limits(),
        ) as (archive_source, header_index):
            _verify_zip_payload(archive_source, header_index, tree)
    except ReleaseArtifactError:
        raise
    except (
        ArchiveHeaderError,
        ArchiveMemberStreamError,
        EOFError,
        OSError,
        RuntimeError,
        zipfile.BadZipFile,
    ) as error:
        raise ReleaseArtifactError(
            "integratiearchief bevat gewijzigde of verboden bytes"
        ) from error


def _verify_zip_bound_stream(
    archive_source: BinaryIO,
    *,
    archive_size: int,
    tree: SourceTree,
) -> None:
    try:
        header_index = validate_zip_header_stream(
            archive_source,
            archive_size=archive_size,
            expected_root=tree.root_name,
            limits=_archive_header_limits(),
        )
        _rewind_bound_stream(archive_source)
        _verify_zip_payload(archive_source, header_index, tree)
    except ReleaseArtifactError:
        raise
    except (
        ArchiveHeaderError,
        ArchiveMemberStreamError,
        EOFError,
        OSError,
        RuntimeError,
        zipfile.BadZipFile,
    ) as error:
        raise ReleaseArtifactError(
            "integratiearchief bevat gewijzigde of verboden bytes"
        ) from error


def _verify_tar_payload(
    archive_source: BinaryIO,
    header_index: ArchiveHeaderIndex,
    tree: SourceTree,
) -> None:
    expected = _expected_files(tree)
    _assert_header_index(header_index, tree)
    with tarfile.open(fileobj=archive_source, mode="r:gz") as archive:
        members = archive.getmembers()
        names = [item.name for item in members]
        if len(names) != len(set(names)):
            raise ReleaseArtifactError("workerarchief bevat dubbele entries")
        for member in members:
            _safe_member_name(member.name, tree.root_name)
            if not (member.isfile() or member.isdir()):
                raise ReleaseArtifactError("workerarchief bevat een speciaal bestand")
        files = {item.name: item for item in members if item.isfile()}
        if set(files) != set(expected):
            raise ReleaseArtifactError("workerarchiefbestandset verschilt van bron")
        for name, source in expected.items():
            member = files[name]
            if member.size != source.size or source.size > MAX_SOURCE_FILE_BYTES:
                raise ReleaseArtifactError("workerarchief bevat gewijzigde grootte")
            extracted = archive.extractfile(member)
            if extracted is None:
                raise ReleaseArtifactError("workerarchiefbestand kon niet worden gelezen")
            with extracted:
                stream_archive_member(
                    extracted,
                    expected_size=source.size,
                    maximum_size=MAX_SOURCE_FILE_BYTES,
                    expected_sha256=source.sha256,
                    forbidden_markers=PRIVATE_KEY_MARKERS,
                )


def _verify_tar(path: Path, tree: SourceTree) -> None:
    try:
        with validated_tar_gz_file(
            path,
            expected_root=tree.root_name,
            limits=_archive_header_limits(),
        ) as (archive_source, header_index):
            _verify_tar_payload(archive_source, header_index, tree)
    except ReleaseArtifactError:
        raise
    except (
        ArchiveHeaderError,
        ArchiveMemberStreamError,
        EOFError,
        OSError,
        RuntimeError,
        tarfile.TarError,
    ) as error:
        raise ReleaseArtifactError(
            "workerarchief bevat gewijzigde of verboden bytes"
        ) from error


def _verify_tar_bound_stream(
    archive_source: BinaryIO,
    *,
    archive_size: int,
    tree: SourceTree,
) -> None:
    try:
        header_index = validate_tar_gz_header_stream(
            archive_source,
            archive_size=archive_size,
            expected_root=tree.root_name,
            limits=_archive_header_limits(),
        )
        _rewind_bound_stream(archive_source)
        _verify_tar_payload(archive_source, header_index, tree)
    except ReleaseArtifactError:
        raise
    except (
        ArchiveHeaderError,
        ArchiveMemberStreamError,
        EOFError,
        OSError,
        RuntimeError,
        tarfile.TarError,
    ) as error:
        raise ReleaseArtifactError(
            "workerarchief bevat gewijzigde of verboden bytes"
        ) from error


def _staging_path(directory: Path, suffix: str) -> Path:
    descriptor, raw_path = tempfile.mkstemp(
        prefix=".stv3-release-staging-",
        suffix=suffix,
        dir=directory,
    )
    os.close(descriptor)
    path = Path(raw_path)
    path.unlink()
    return path


def _publish_exclusive(staging: Path, destination: Path) -> None:
    try:
        os.link(staging, destination)
    except FileExistsError as error:
        raise ReleaseArtifactError(f"artefact bestaat al: {destination.name}") from error
    except OSError as error:
        raise ReleaseArtifactError("artefact kon niet atomair worden gepubliceerd") from error


def _cleanup(paths: Iterable[Path]) -> None:
    for path in paths:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def build_release_candidate(
    *,
    integration_root: Path,
    worker_root: Path,
    integration_version: str,
    worker_version: str,
    output_directory: Path,
) -> BuiltReleaseCandidate:
    """Build and verify one deterministic pair without overwriting files."""

    integration_version = _version(integration_version, "integratieversie")
    worker_version = _version(worker_version, "workerversie")
    integration_argument = Path(integration_root)
    worker_argument = Path(worker_root)
    output_argument = Path(output_directory)
    if (
        integration_argument.is_symlink()
        or worker_argument.is_symlink()
        or output_argument.is_symlink()
    ):
        raise ReleaseArtifactError("pakket- en uitvoerroots mogen geen symlink zijn")
    integration_source = integration_argument.resolve(strict=True)
    worker_source = worker_argument.resolve(strict=True)
    output = output_argument.resolve(strict=True)
    if not output.is_dir():
        raise ReleaseArtifactError("uitvoermap moet een bestaande echte map zijn")
    _validate_versions(
        integration_source,
        worker_source,
        integration_version,
        worker_version,
    )
    integration_tree = inspect_source_tree(
        integration_source,
        expected_root_name=INTEGRATION_ROOT_NAME,
    )
    worker_tree = inspect_source_tree(
        worker_source,
        expected_root_name=WORKER_ROOT_NAME,
    )
    # Bind the version decision to the exact tree that was just hashed.  A
    # later source mutation is caught by archive byte verification; a mutation
    # before inventory cannot smuggle a different internal version.
    _validate_versions(
        integration_source,
        worker_source,
        integration_version,
        worker_version,
    )
    integration_name = f"storm_tracker_v3_v{integration_version}_deploy.zip"
    worker_name = f"storm_tracker_worker_v{worker_version}_deploy.tar.gz"
    evidence_name = (
        f"storm_tracker_release_v{integration_version}_worker_v{worker_version}_evidence.json"
    )
    destinations = tuple(output / item for item in (
        integration_name,
        worker_name,
        evidence_name,
    ))
    if any(path.exists() for path in destinations):
        raise ReleaseArtifactError("minstens één releaseartefact bestaat al")
    zip_stage = _staging_path(output, ".zip")
    tar_stage = _staging_path(output, ".tar.gz")
    evidence_stage = _staging_path(output, ".json")
    stages = (zip_stage, tar_stage, evidence_stage)
    try:
        _write_zip(zip_stage, integration_source, integration_tree)
        try:
            _write_tar_gz(tar_stage, worker_source, worker_tree)
        except (OSError, tarfile.TarError, UnicodeError, ValueError) as error:
            raise ReleaseArtifactError(
                "workerarchief kon niet in het vaste USTAR-profiel worden geschreven"
            ) from error
        _verify_zip(zip_stage, integration_tree)
        _verify_tar(tar_stage, worker_tree)
        integration_sha = _sha_file(zip_stage)
        worker_sha = _sha_file(tar_stage)
        unsigned = {
            "archives": {
                "integration": {
                    "bytes": zip_stage.stat().st_size,
                    "filename": integration_name,
                    "root_name": INTEGRATION_ROOT_NAME,
                    "sha256": integration_sha,
                },
                "worker": {
                    "bytes": tar_stage.stat().st_size,
                    "filename": worker_name,
                    "root_name": WORKER_ROOT_NAME,
                    "sha256": worker_sha,
                },
            },
            "contract": RELEASE_EVIDENCE_CONTRACT,
            "deployment_performed": False,
            "integration_version": integration_version,
            "source_trees": {
                "integration": integration_tree.to_mapping(),
                "worker": worker_tree.to_mapping(),
            },
            "worker_version": worker_version,
        }
        evidence_sha = _sha_bytes(_canonical(unsigned))
        evidence = {**unsigned, "evidence_sha256": evidence_sha}
        with evidence_stage.open("xb") as target:
            target.write(_canonical(evidence) + b"\n")
            target.flush()
            os.fsync(target.fileno())
        _publish_exclusive(zip_stage, destinations[0])
        zip_stage.unlink()
        _publish_exclusive(tar_stage, destinations[1])
        tar_stage.unlink()
        _publish_exclusive(evidence_stage, destinations[2])
        evidence_stage.unlink()
        verify_release_candidate_evidence(destinations[2], archive_directory=output)
        return BuiltReleaseCandidate(
            integration_archive=destinations[0],
            worker_archive=destinations[1],
            evidence_path=destinations[2],
            integration_sha256=integration_sha,
            worker_sha256=worker_sha,
            evidence_sha256=evidence_sha,
        )
    finally:
        _cleanup(stages)


def _tree_from_mapping(value: object) -> SourceTree:
    if not isinstance(value, dict) or set(value) != {
        "content_sha256", "file_count", "files", "root_name", "total_bytes"
    }:
        raise ReleaseArtifactError("evidence bevat een ongeldig source-treecontract")
    raw_files = value["files"]
    if not isinstance(raw_files, list):
        raise ReleaseArtifactError("evidence source tree mist files")
    files: list[SourceFile] = []
    for raw in raw_files:
        if not isinstance(raw, dict) or set(raw) != {"mode", "path", "sha256", "size"}:
            raise ReleaseArtifactError("evidence bevat een ongeldig bronbestand")
        path = raw["path"]
        sha = raw["sha256"]
        size = raw["size"]
        mode = raw["mode"]
        if (
            not isinstance(path, str)
            or not path
            or PurePosixPath(path).is_absolute()
            or ".." in PurePosixPath(path).parts
            or "\\" in path
            or not isinstance(sha, str)
            or re.fullmatch(r"[0-9a-f]{64}", sha) is None
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or size > MAX_SOURCE_FILE_BYTES
            or mode not in (0o644, 0o755)
        ):
            raise ReleaseArtifactError("evidencebronbestand is ongeldig")
        files.append(SourceFile(path, size, sha, mode))
    if len(files) != value["file_count"] or len(files) > MAX_FILE_COUNT:
        raise ReleaseArtifactError("evidence file_count klopt niet")
    if tuple(item.relative_path for item in files) != tuple(sorted(
        item.relative_path for item in files
    )) or len({item.relative_path for item in files}) != len(files):
        raise ReleaseArtifactError("evidence bronbestanden zijn niet canoniek")
    total = sum(item.size for item in files)
    content_sha = _sha_bytes(_canonical([item.to_mapping() for item in files]))
    if (
        total > MAX_TOTAL_SOURCE_BYTES
        or total != value["total_bytes"]
        or content_sha != value["content_sha256"]
    ):
        raise ReleaseArtifactError("evidence source-treehash klopt niet")
    root_name = value["root_name"]
    if root_name not in (INTEGRATION_ROOT_NAME, WORKER_ROOT_NAME):
        raise ReleaseArtifactError("evidence heeft een ongeldige pakketroot")
    return SourceTree(root_name, tuple(files), total, content_sha)


def _decode_bound_evidence(value: bytes) -> object:
    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, nested in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = nested
        return result

    def reject_float(token: str) -> object:
        raise ValueError(f"JSON float is forbidden: {token}")

    def reject_constant(token: str) -> object:
        raise ValueError(f"JSON constant is forbidden: {token}")

    def bounded_integer(token: str) -> int:
        magnitude = token[1:] if token.startswith("-") else token
        bound = str(MAX_RELEASE_EVIDENCE_JSON_INTEGER_ABSOLUTE)
        if (
            not magnitude
            or len(magnitude) > len(bound)
            or (len(magnitude) == len(bound) and magnitude > bound)
        ):
            raise ValueError("JSON integer exceeds its bound")
        return int(token)

    try:
        document = json.loads(
            value.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_float=reject_float,
            parse_constant=reject_constant,
            parse_int=bounded_integer,
        )
    except (UnicodeError, json.JSONDecodeError, RecursionError, ValueError) as error:
        raise ReleaseArtifactError(
            "gebonden release-evidence is geen geldige begrensde JSON"
        ) from error
    pending: list[tuple[object, int]] = [(document, 1)]
    nodes = 0
    while pending:
        nested, depth = pending.pop()
        nodes += 1
        if (
            nodes > MAX_RELEASE_EVIDENCE_JSON_NODES
            or depth > MAX_RELEASE_EVIDENCE_JSON_DEPTH
        ):
            raise ReleaseArtifactError(
                "gebonden release-evidence overschrijdt zijn JSON-structuurbound"
            )
        if isinstance(nested, dict):
            for key, item in nested.items():
                try:
                    key_bytes = key.encode("utf-8")
                except (AttributeError, UnicodeError) as error:
                    raise ReleaseArtifactError(
                        "gebonden release-evidence bevat een ongeldige JSON-sleutel"
                    ) from error
                if len(key_bytes) > MAX_RELEASE_EVIDENCE_JSON_KEY_BYTES:
                    raise ReleaseArtifactError(
                        "gebonden release-evidence overschrijdt zijn JSON-keybound"
                    )
                pending.append((item, depth + 1))
        elif isinstance(nested, list):
            pending.extend((item, depth + 1) for item in nested)
        elif isinstance(nested, str):
            try:
                string_bytes = nested.encode("utf-8")
            except UnicodeError as error:
                raise ReleaseArtifactError(
                    "gebonden release-evidence bevat ongeldige UTF-8-tekst"
                ) from error
            if len(string_bytes) > MAX_RELEASE_EVIDENCE_JSON_STRING_BYTES:
                raise ReleaseArtifactError(
                    "gebonden release-evidence overschrijdt zijn JSON-stringbound"
                )
        elif nested is None or isinstance(nested, (bool, int)):
            continue
        else:
            raise ReleaseArtifactError(
                "gebonden release-evidence bevat een verboden JSON-type"
            )
    return document


def _validate_release_document(
    document: object,
) -> tuple[
    dict[str, object],
    dict[str, SourceTree],
    dict[str, dict[str, object]],
]:
    expected_keys = {
        "archives", "contract", "deployment_performed", "evidence_sha256",
        "integration_version", "source_trees", "worker_version",
    }
    if not isinstance(document, dict) or set(document) != expected_keys:
        raise ReleaseArtifactError("release-evidence heeft een onbekend schema")
    if document["contract"] != RELEASE_EVIDENCE_CONTRACT:
        raise ReleaseArtifactError("release-evidencecontract is onbekend")
    if document["deployment_performed"] is not False:
        raise ReleaseArtifactError("lokale builder mag geen deploy claimen")
    integration_version = _version(document["integration_version"], "integratieversie")
    worker_version = _version(document["worker_version"], "workerversie")
    supplied_sha = document["evidence_sha256"]
    unsigned = dict(document)
    del unsigned["evidence_sha256"]
    if not isinstance(supplied_sha, str) or supplied_sha != _sha_bytes(_canonical(unsigned)):
        raise ReleaseArtifactError("release-evidencehash klopt niet")
    raw_trees = document["source_trees"]
    if not isinstance(raw_trees, dict) or set(raw_trees) != {"integration", "worker"}:
        raise ReleaseArtifactError("release-evidence mist source trees")
    trees = {
        "integration": _tree_from_mapping(raw_trees["integration"]),
        "worker": _tree_from_mapping(raw_trees["worker"]),
    }
    if (
        trees["integration"].root_name != INTEGRATION_ROOT_NAME
        or trees["worker"].root_name != WORKER_ROOT_NAME
    ):
        raise ReleaseArtifactError("release-evidence verwisselt pakketroots")
    raw_archives = document["archives"]
    if not isinstance(raw_archives, dict) or set(raw_archives) != {"integration", "worker"}:
        raise ReleaseArtifactError("release-evidence mist archieven")
    expected_names = {
        "integration": f"storm_tracker_v3_v{integration_version}_deploy.zip",
        "worker": f"storm_tracker_worker_v{worker_version}_deploy.tar.gz",
    }
    archives: dict[str, dict[str, object]] = {}
    for kind in ("integration", "worker"):
        item = raw_archives[kind]
        if not isinstance(item, dict) or set(item) != {
            "bytes", "filename", "root_name", "sha256"
        }:
            raise ReleaseArtifactError("release-evidence bevat ongeldige archiefmetadata")
        size = item["bytes"]
        sha256 = item["sha256"]
        if (
            item["filename"] != expected_names[kind]
            or item["root_name"] != trees[kind].root_name
        ):
            raise ReleaseArtifactError("release-archiefnaam of root klopt niet")
        if (
            type(size) is not int
            or size < 0
            or size > _archive_header_limits().max_archive_bytes
            or not isinstance(sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
        ):
            raise ReleaseArtifactError("release-evidence bevat ongeldige archiefmetadata")
        archives[kind] = item
    return document, trees, archives


def verify_release_candidate_evidence(
    evidence_path: Path,
    *,
    archive_directory: Path | None = None,
) -> Mapping[str, object]:
    """Verify the commit marker, archive hashes, roots and every file byte."""

    path_argument = Path(evidence_path)
    if path_argument.is_symlink():
        raise ReleaseArtifactError("release-evidencebestand mag geen symlink zijn")
    path = path_argument.resolve(strict=True)
    if not path.is_file() or path.stat().st_size > 64 * 1024 * 1024:
        raise ReleaseArtifactError("release-evidencebestand is ongeldig")
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ReleaseArtifactError("release-evidence is geen geldige JSON") from error
    document, trees, archives = _validate_release_document(document)
    directory_argument = Path(archive_directory) if archive_directory else path.parent
    if directory_argument.is_symlink():
        raise ReleaseArtifactError("release-uitvoermap mag geen symlink zijn")
    directory = directory_argument.resolve(strict=True)
    for kind in ("integration", "worker"):
        tree = trees[kind]
        item = archives[kind]
        archive_argument = directory / item["filename"]
        if archive_argument.is_symlink():
            raise ReleaseArtifactError("release-archief mag geen symlink zijn")
        archive_path = archive_argument.resolve(strict=True)
        try:
            archive_path.relative_to(directory)
        except ValueError as error:
            raise ReleaseArtifactError("release-archief ontsnapt uit de uitvoermap") from error
        if not archive_path.is_file():
            raise ReleaseArtifactError("release-archief is geen regulier bestand")
        if (
            archive_path.stat().st_size != item["bytes"]
            or _sha_file(archive_path) != item["sha256"]
        ):
            raise ReleaseArtifactError("release-archiefhash of -grootte klopt niet")
        if kind == "integration":
            _verify_zip(archive_path, tree)
        else:
            _verify_tar(archive_path, tree)
    return document


def _bound_input_metadata(
    source: "BoundInputSource",
    key: str,
    *,
    maximum_size: int,
) -> tuple[int, str]:
    try:
        metadata = source.metadata(key)
        size = metadata.size
        sha256 = metadata.sha256
    except Exception as error:
        raise ReleaseArtifactError(
            "gebonden release-inputmetadata is niet beschikbaar"
        ) from error
    if (
        type(size) is not int
        or size < 0
        or size > maximum_size
        or not isinstance(sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
    ):
        raise ReleaseArtifactError(
            "gebonden release-inputmetadata is ongeldig"
        )
    return size, sha256


def verify_release_candidate_evidence_bound_inputs(
    source: "BoundInputSource",
) -> Mapping[str, object]:
    """Verify a candidate using only three authority-bound descriptor streams.

    The caller binds the logical ``evidence``, ``integration`` and ``worker``
    keys.  This verifier never receives or reconstructs a pathname.  Each
    archive's hash, bounded header preflight and stdlib/member verification all
    consume the same stream instance before its registry context is released.
    """

    try:
        source.require_exact_keys(BOUND_RELEASE_INPUT_KEYS)
    except Exception as error:
        raise ReleaseArtifactError(
            "gebonden release-inputs hebben niet exact de vereiste sleutelset"
        ) from error

    evidence_size, evidence_sha256 = _bound_input_metadata(
        source,
        "evidence",
        maximum_size=MAX_RELEASE_EVIDENCE_BYTES,
    )
    try:
        with source.open_stream("evidence") as evidence_stream:
            observed_sha256, evidence_bytes = _hash_bound_stream(
                evidence_stream,
                expected_size=evidence_size,
                maximum_size=MAX_RELEASE_EVIDENCE_BYTES,
                collect=True,
            )
            if observed_sha256 != evidence_sha256 or evidence_bytes is None:
                raise ReleaseArtifactError(
                    "gebonden release-evidencehash verschilt van zijn seal"
                )
            document, trees, archives = _validate_release_document(
                _decode_bound_evidence(evidence_bytes)
            )
    except ReleaseArtifactError:
        raise
    except Exception as error:
        raise ReleaseArtifactError(
            "gebonden release-evidence veranderde tijdens verificatie"
        ) from error
    for kind in ("integration", "worker"):
        item = archives[kind]
        expected_size = item["bytes"]
        expected_sha256 = item["sha256"]
        metadata_size, metadata_sha256 = _bound_input_metadata(
            source,
            kind,
            maximum_size=_archive_header_limits().max_archive_bytes,
        )
        if (
            metadata_size != expected_size
            or metadata_sha256 != expected_sha256
        ):
            raise ReleaseArtifactError(
                "gebonden release-archiefmetadata verschilt van evidence"
            )
        try:
            with source.open_stream(kind) as archive_stream:
                observed_sha256, _unused = _hash_bound_stream(
                    archive_stream,
                    expected_size=metadata_size,
                    maximum_size=_archive_header_limits().max_archive_bytes,
                )
                if observed_sha256 != metadata_sha256:
                    raise ReleaseArtifactError(
                        "gebonden release-archiefhash verschilt van evidence"
                    )
                if kind == "integration":
                    _verify_zip_bound_stream(
                        archive_stream,
                        archive_size=metadata_size,
                        tree=trees[kind],
                    )
                else:
                    _verify_tar_bound_stream(
                        archive_stream,
                        archive_size=metadata_size,
                        tree=trees[kind],
                    )
        except ReleaseArtifactError:
            raise
        except Exception as error:
            raise ReleaseArtifactError(
                "gebonden release-archief veranderde tijdens verificatie"
            ) from error
    return document


__all__ = [
    "BuiltReleaseCandidate",
    "INTEGRATION_ROOT_NAME",
    "RELEASE_EVIDENCE_CONTRACT",
    "ReleaseArtifactError",
    "SourceFile",
    "SourceTree",
    "WORKER_ROOT_NAME",
    "build_release_candidate",
    "inspect_source_tree",
    "verify_release_candidate_evidence",
    "verify_release_candidate_evidence_bound_inputs",
]
