"""Offline, fail-closed contract for the RADFLOOD21 validation corpus.

The contract is intentionally not a provider.  It performs no network I/O,
does not inspect ZIP members or HDF5 pixels and is not wired into any worker
registry, scheduler, decoder or admission path.  It can only:

* validate the pinned official Zenodo/KMI/HESS manifest;
* validate a caller-supplied Zenodo metadata snapshot against that manifest;
* stream-check a local artifact against its official byte size and MD5; and
* expose the officially documented accumulation support.

MD5 is used solely because it is the checksum published by Zenodo.  A local
SHA-256 is also calculated, but it is not presented as upstream authority.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
from typing import Any, Mapping


SCHEMA_VERSION = "radflood21-validation-corpus-manifest-v1"
SHADOW_ONLY = True
MANIFEST_FILENAME = "radflood21_official_manifest.json"
PINNED_MANIFEST_CANONICAL_SHA256 = (
    "f70b360db33e2d0aac5a052183d97a42a89ee3b9dc1a6916b9125ca53aefa741"
)

ZENODO_RECORD_ID = 7_740_059
ZENODO_CONCEPT_RECORD_ID = 7_740_058
ZENODO_DOI = "10.5281/zenodo.7740059"
ZENODO_CONCEPT_DOI = "10.5281/zenodo.7740058"
ZENODO_REVISION = 8
ZENODO_CREATED_AT = "2023-03-16T09:04:01.915565+00:00"
ZENODO_UPDATED_AT = "2023-10-17T12:06:57.625415+00:00"
ZENODO_PUBLICATION_DATE = "2023-03-16"
ZENODO_TITLE = (
    "Observational rainfall data of the 2021 mid-July flood event in "
    "Belgium – Part 2. Radar product RADFLOOD21"
)
ZENODO_LICENSE_ID = "cc-by-4.0"

CORPUS_ARCHIVE_NAME = "radflood21.zip"
CORPUS_ARCHIVE_SIZE_BYTES = 2_642_551_002
CORPUS_ARCHIVE_MD5 = "2cb3660a9a2b81fcc3fb98d103984e12"
VISUALISATION_NAME = "radflood21.mp4"
VISUALISATION_SIZE_BYTES = 45_375_862
VISUALISATION_MD5 = "ab87ef2df8bf0008a39dd6b791168ea2"

PRODUCT_START = "2021-07-11T00:00:00Z"
PRODUCT_END = "2021-07-18T00:00:00Z"
CORE_ANALYSIS_START = "2021-07-13T06:00:00Z"
CORE_ANALYSIS_END = "2021-07-16T06:00:00Z"
SUPPORTED_ACCUMULATION_SECONDS = frozenset({300, 3_600})

MAX_MANIFEST_BYTES = 64 * 1024
MAX_ZENODO_SNAPSHOT_BYTES = 2 * 1024 * 1024
DEFAULT_FILE_CHUNK_BYTES = 1024 * 1024
MAX_FILE_CHUNK_BYTES = 16 * 1024 * 1024

_MD5_RE = re.compile(r"^[0-9a-f]{32}$")
_SAFE_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{1,127}$")

OPEN_DECODER_GATES = (
    "zip_internal_member_inventory_not_pinned",
    "hdf5_dataset_paths_not_pinned",
    "odim_product_and_quantity_tokens_not_pinned",
    "hdf5_gain_offset_nodata_undetect_not_pinned",
    "pixel_qc_fields_and_codes_not_pinned",
    "exact_grid_dimensions_origin_and_projdef_not_pinned",
    "archive_member_filename_timestamp_contract_not_pinned",
    "published_window_endpoint_inclusivity_not_pinned",
    "official_small_golden_hdf5_not_available",
)

EXPECTED_CREATORS = (
    (
        "Goudenhoofdt, Edouard",
        "Royal Meteorological Institute of Belgium",
    ),
    (
        "Journée, Michel",
        "Royal Meteorological Institute of Belgium",
    ),
    (
        "Delobbe, Laurent",
        "Royal Meteorological Institute of Belgium",
    ),
)


class Radflood21ValidationError(ValueError):
    """The local or supplied evidence differs from the pinned authority."""


@dataclass(frozen=True, slots=True)
class Radflood21Artifact:
    """One exact Zenodo file identity."""

    name: str
    role: str
    size_bytes: int
    checksum_algorithm: str
    checksum_hex: str

    def __post_init__(self) -> None:
        if _SAFE_NAME_RE.fullmatch(self.name) is None:
            raise Radflood21ValidationError("onveilige RADFLOOD21-bestandsnaam")
        if _SAFE_NAME_RE.fullmatch(self.role) is None:
            raise Radflood21ValidationError("onveilige RADFLOOD21-artefactrol")
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes <= 0
        ):
            raise Radflood21ValidationError("ongeldige RADFLOOD21-bestandsgrootte")
        if self.checksum_algorithm != "md5":
            raise Radflood21ValidationError("RADFLOOD21 verwacht officiële MD5")
        if _MD5_RE.fullmatch(self.checksum_hex) is None:
            raise Radflood21ValidationError("ongeldige RADFLOOD21-MD5")

    @property
    def zenodo_checksum(self) -> str:
        return f"md5:{self.checksum_hex}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "role": self.role,
            "size_bytes": self.size_bytes,
            "checksum_algorithm": self.checksum_algorithm,
            "checksum_hex": self.checksum_hex,
        }


@dataclass(frozen=True, slots=True)
class Radflood21Manifest:
    """Validated immutable subset needed for offline corpus identity."""

    canonical_sha256: str
    record_id: int
    concept_record_id: int
    doi: str
    concept_doi: str
    revision: int
    title: str
    license_id: str
    creators: tuple[tuple[str, str], ...]
    artifacts: tuple[Radflood21Artifact, ...]
    open_decoder_gates: tuple[str, ...]

    @property
    def corpus_archive(self) -> Radflood21Artifact:
        matches = tuple(
            artifact
            for artifact in self.artifacts
            if artifact.role == "validation_corpus_archive"
        )
        if len(matches) != 1:
            raise Radflood21ValidationError(
                "manifest heeft niet exact één RADFLOOD21-corpusarchief"
            )
        return matches[0]

    @property
    def archive_identity_validation_eligible(self) -> bool:
        return True

    @property
    def decoder_eligible(self) -> bool:
        return False

    @property
    def live_provider_eligible(self) -> bool:
        return False

    @property
    def operational_admission_eligible(self) -> bool:
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "canonical_sha256": self.canonical_sha256,
            "record_id": self.record_id,
            "concept_record_id": self.concept_record_id,
            "doi": self.doi,
            "concept_doi": self.concept_doi,
            "revision": self.revision,
            "title": self.title,
            "license_id": self.license_id,
            "creators": [
                {"name": name, "affiliation": affiliation}
                for name, affiliation in self.creators
            ],
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "open_decoder_gates": list(self.open_decoder_gates),
            "archive_identity_validation_eligible": (
                self.archive_identity_validation_eligible
            ),
            "decoder_eligible": self.decoder_eligible,
            "live_provider_eligible": self.live_provider_eligible,
            "operational_admission_eligible": (
                self.operational_admission_eligible
            ),
        }


@dataclass(frozen=True, slots=True)
class ZenodoSnapshotReceipt:
    """Receipt for an exact metadata comparison; no remote URL is retained."""

    record_id: int
    revision: int
    metadata_sha256: str
    artifact_count: int
    matches_pinned_manifest: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "revision": self.revision,
            "metadata_sha256": self.metadata_sha256,
            "artifact_count": self.artifact_count,
            "matches_pinned_manifest": self.matches_pinned_manifest,
            "decoder_eligible": False,
            "operational_admission_eligible": False,
        }


@dataclass(frozen=True, slots=True)
class LocalArtifactReceipt:
    """Offline file-integrity receipt after a complete streaming read."""

    name: str
    size_bytes: int
    official_md5: str
    local_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "size_bytes": self.size_bytes,
            "official_md5": self.official_md5,
            "local_sha256": self.local_sha256,
            "upstream_checksum_matched": True,
            "decoder_eligible": False,
            "operational_admission_eligible": False,
        }


@dataclass(frozen=True, slots=True)
class AccumulationSupport:
    """Official end-stamped temporal support of one accumulation."""

    start: str
    end: str
    duration_seconds: int
    timestamp_role: str = "end_of_accumulation_period"

    def to_dict(self) -> dict[str, Any]:
        return {
            "start": self.start,
            "end": self.end,
            "duration_seconds": self.duration_seconds,
            "timestamp_role": self.timestamp_role,
        }


def _reject_duplicate_keys(pairs):
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise Radflood21ValidationError(
                f"dubbele RADFLOOD21 JSON-key {key!r}"
            )
        result[key] = value
    return result


def _parse_json_bytes(payload: bytes, *, maximum_bytes: int, role: str) -> dict:
    if not isinstance(payload, bytes):
        raise TypeError(f"{role} moet exacte bytes zijn")
    if not payload or len(payload) > maximum_bytes:
        raise Radflood21ValidationError(f"{role} overschrijdt bytecontract")
    try:
        text = payload.decode("utf-8", errors="strict")
        parsed = json.loads(text, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Radflood21ValidationError(f"{role} is geen geldige UTF-8 JSON") from exc
    if type(parsed) is not dict:
        raise Radflood21ValidationError(f"{role} root is geen object")
    return parsed


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise Radflood21ValidationError(
            "RADFLOOD21 metadata is niet canoniek JSON"
        ) from exc


def canonical_manifest_sha256(value: Mapping[str, Any]) -> str:
    """Hash semantic JSON content independently of whitespace/key order."""

    if type(value) is not dict:
        raise TypeError("RADFLOOD21-manifest moet een dict zijn")
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _mapping(value: Any, role: str) -> dict:
    if type(value) is not dict:
        raise Radflood21ValidationError(f"{role} moet een object zijn")
    return value


def _exact_keys(value: dict, keys: set[str], role: str) -> None:
    if set(value) != keys:
        raise Radflood21ValidationError(f"{role} heeft ontbrekende/extra metadata")


def _same_file_identity(left: os.stat_result, right: os.stat_result) -> bool:
    """Compare the opened object, not merely its replaceable path."""

    return (
        left.st_dev == right.st_dev
        and left.st_ino == right.st_ino
        and left.st_mode == right.st_mode
    )


def _open_readonly_no_follow(path: Path) -> int:
    flags = os.O_RDONLY
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        return os.open(path, flags)
    except OSError as exc:
        raise Radflood21ValidationError(
            "RADFLOOD21-bestand kon niet veilig worden geopend"
        ) from exc


def _artifact_from_mapping(value: Any) -> Radflood21Artifact:
    item = _mapping(value, "RADFLOOD21-artefact")
    _exact_keys(
        item,
        {
            "name",
            "role",
            "size_bytes",
            "checksum_algorithm",
            "checksum_hex",
        },
        "RADFLOOD21-artefact",
    )
    return Radflood21Artifact(
        name=item["name"],
        role=item["role"],
        size_bytes=item["size_bytes"],
        checksum_algorithm=item["checksum_algorithm"],
        checksum_hex=item["checksum_hex"],
    )


def validate_manifest_mapping(value: Mapping[str, Any]) -> Radflood21Manifest:
    """Validate every pinned manifest fact and reject any semantic drift."""

    if type(value) is not dict:
        raise TypeError("RADFLOOD21-manifest moet een dict zijn")
    digest = canonical_manifest_sha256(value)
    if digest != PINNED_MANIFEST_CANONICAL_SHA256:
        raise Radflood21ValidationError(
            "RADFLOOD21-manifest wijkt af van de gepinde officiële metadata"
        )

    _exact_keys(
        value,
        {
            "schema_version",
            "record",
            "artifacts",
            "product",
            "decoder_gate",
            "primary_sources",
            "audit_date",
        },
        "RADFLOOD21-manifest",
    )
    if value["schema_version"] != SCHEMA_VERSION:
        raise Radflood21ValidationError("onverwachte RADFLOOD21-schemaversie")

    record = _mapping(value["record"], "RADFLOOD21-record")
    _exact_keys(
        record,
        {
            "record_id",
            "concept_record_id",
            "doi",
            "concept_doi",
            "revision",
            "created_at",
            "updated_at",
            "publication_date",
            "title",
            "resource_type",
            "access_right",
            "license_id",
            "creators",
        },
        "RADFLOOD21-record",
    )
    expected_record = {
        "record_id": ZENODO_RECORD_ID,
        "concept_record_id": ZENODO_CONCEPT_RECORD_ID,
        "doi": ZENODO_DOI,
        "concept_doi": ZENODO_CONCEPT_DOI,
        "revision": ZENODO_REVISION,
        "created_at": ZENODO_CREATED_AT,
        "updated_at": ZENODO_UPDATED_AT,
        "publication_date": ZENODO_PUBLICATION_DATE,
        "title": ZENODO_TITLE,
        "resource_type": "dataset",
        "access_right": "open",
        "license_id": ZENODO_LICENSE_ID,
    }
    for key, expected in expected_record.items():
        if record[key] != expected or type(record[key]) is not type(expected):
            raise Radflood21ValidationError(
                f"RADFLOOD21 recordveld {key!r} wijkt af"
            )
    creators_value = record["creators"]
    if type(creators_value) is not list:
        raise Radflood21ValidationError("RADFLOOD21 creators is geen lijst")
    creators: list[tuple[str, str]] = []
    for creator in creators_value:
        item = _mapping(creator, "RADFLOOD21 creator")
        _exact_keys(item, {"name", "affiliation"}, "RADFLOOD21 creator")
        creators.append((item["name"], item["affiliation"]))
    if tuple(creators) != EXPECTED_CREATORS:
        raise Radflood21ValidationError("RADFLOOD21 provenance wijkt af")

    artifacts_value = value["artifacts"]
    if type(artifacts_value) is not list:
        raise Radflood21ValidationError("RADFLOOD21 artifacts is geen lijst")
    artifacts = tuple(_artifact_from_mapping(item) for item in artifacts_value)
    expected_artifacts = {
        CORPUS_ARCHIVE_NAME: (
            "validation_corpus_archive",
            CORPUS_ARCHIVE_SIZE_BYTES,
            CORPUS_ARCHIVE_MD5,
        ),
        VISUALISATION_NAME: (
            "visualisation_only",
            VISUALISATION_SIZE_BYTES,
            VISUALISATION_MD5,
        ),
    }
    if len(artifacts) != len(expected_artifacts):
        raise Radflood21ValidationError("RADFLOOD21 artefactset is onvolledig")
    if len({item.name for item in artifacts}) != len(artifacts):
        raise Radflood21ValidationError("dubbel RADFLOOD21-artefact")
    for artifact in artifacts:
        expected = expected_artifacts.get(artifact.name)
        if expected is None or (
            artifact.role,
            artifact.size_bytes,
            artifact.checksum_hex,
        ) != expected:
            raise Radflood21ValidationError(
                f"RADFLOOD21 artefact {artifact.name!r} wijkt af"
            )

    decoder_gate = _mapping(value["decoder_gate"], "RADFLOOD21 decoder_gate")
    if (
        decoder_gate.get("decoder_eligible") is not False
        or decoder_gate.get("live_provider_eligible") is not False
        or decoder_gate.get("operational_admission_eligible") is not False
        or tuple(decoder_gate.get("open_gates") or ()) != OPEN_DECODER_GATES
    ):
        raise Radflood21ValidationError("RADFLOOD21 decodergate is niet gesloten")

    return Radflood21Manifest(
        canonical_sha256=digest,
        record_id=record["record_id"],
        concept_record_id=record["concept_record_id"],
        doi=record["doi"],
        concept_doi=record["concept_doi"],
        revision=record["revision"],
        title=record["title"],
        license_id=record["license_id"],
        creators=tuple(creators),
        artifacts=artifacts,
        open_decoder_gates=OPEN_DECODER_GATES,
    )


def load_official_manifest(path: Path | None = None) -> Radflood21Manifest:
    """Load the small local manifest; never contact Zenodo or KMI."""

    manifest_path = (
        Path(__file__).with_name(MANIFEST_FILENAME)
        if path is None
        else Path(path)
    )
    try:
        info = manifest_path.stat(follow_symlinks=False)
    except OSError as exc:
        raise Radflood21ValidationError("RADFLOOD21-manifest ontbreekt") from exc
    if (
        manifest_path.is_symlink()
        or not stat.S_ISREG(info.st_mode)
        or info.st_size <= 0
        or info.st_size > MAX_MANIFEST_BYTES
    ):
        raise Radflood21ValidationError("onveilig RADFLOOD21-manifestbestand")
    descriptor = _open_readonly_no_follow(manifest_path)
    try:
        opened = os.fstat(descriptor)
        if (
            not _same_file_identity(info, opened)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_size != info.st_size
        ):
            raise Radflood21ValidationError(
                "RADFLOOD21-manifest wijzigde tijdens openen"
            )
        with os.fdopen(descriptor, "rb", buffering=0) as stream:
            descriptor = -1
            payload = stream.read(MAX_MANIFEST_BYTES + 1)
            opened_after = os.fstat(stream.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        path_after = manifest_path.stat(follow_symlinks=False)
    except OSError as exc:
        raise Radflood21ValidationError(
            "RADFLOOD21-manifest verdween tijdens lezen"
        ) from exc
    if (
        len(payload) != info.st_size
        or not _same_file_identity(opened, opened_after)
        or not _same_file_identity(opened, path_after)
        or opened_after.st_size != opened.st_size
        or opened_after.st_mtime_ns != opened.st_mtime_ns
    ):
        raise Radflood21ValidationError("RADFLOOD21-manifest wijzigde tijdens lezen")
    return validate_manifest_mapping(
        _parse_json_bytes(
            payload,
            maximum_bytes=MAX_MANIFEST_BYTES,
            role="RADFLOOD21-manifest",
        )
    )


def _snapshot_mapping(snapshot: bytes | Mapping[str, Any]) -> tuple[dict, str]:
    if isinstance(snapshot, bytes):
        value = _parse_json_bytes(
            snapshot,
            maximum_bytes=MAX_ZENODO_SNAPSHOT_BYTES,
            role="Zenodo-snapshot",
        )
    elif type(snapshot) is dict:
        value = snapshot
    else:
        raise TypeError("Zenodo-snapshot moet bytes of dict zijn")
    canonical = _canonical_bytes(value)
    if len(canonical) > MAX_ZENODO_SNAPSHOT_BYTES:
        raise Radflood21ValidationError("Zenodo-snapshot overschrijdt bytecontract")
    return value, hashlib.sha256(canonical).hexdigest()


def validate_zenodo_record_snapshot(
    snapshot: bytes | Mapping[str, Any],
    *,
    manifest: Radflood21Manifest | None = None,
) -> ZenodoSnapshotReceipt:
    """Compare an already obtained metadata snapshot; make no HTTP request."""

    contract = load_official_manifest() if manifest is None else manifest
    if not isinstance(contract, Radflood21Manifest):
        raise TypeError("manifest moet Radflood21Manifest zijn")
    value, digest = _snapshot_mapping(snapshot)

    required = {
        "id",
        "conceptrecid",
        "doi",
        "conceptdoi",
        "revision",
        "created",
        "updated",
        "metadata",
        "files",
    }
    if not required.issubset(value):
        raise Radflood21ValidationError("Zenodo-snapshot mist verplichte metadata")
    expected_root = {
        "id": contract.record_id,
        "conceptrecid": str(contract.concept_record_id),
        "doi": contract.doi,
        "conceptdoi": contract.concept_doi,
        "revision": contract.revision,
        "created": ZENODO_CREATED_AT,
        "updated": ZENODO_UPDATED_AT,
    }
    for key, expected in expected_root.items():
        if value[key] != expected or type(value[key]) is not type(expected):
            raise Radflood21ValidationError(
                f"Zenodo-snapshotveld {key!r} wijkt af"
            )

    metadata = _mapping(value["metadata"], "Zenodo metadata")
    metadata_required = {
        "title",
        "publication_date",
        "access_right",
        "license",
        "resource_type",
        "creators",
    }
    if not metadata_required.issubset(metadata):
        raise Radflood21ValidationError("Zenodo productmetadata is onvolledig")
    if (
        metadata["title"] != contract.title
        or metadata["publication_date"] != ZENODO_PUBLICATION_DATE
        or metadata["access_right"] != "open"
        or _mapping(metadata["license"], "Zenodo licentie").get("id")
        != contract.license_id
        or _mapping(metadata["resource_type"], "Zenodo resource_type").get("type")
        != "dataset"
    ):
        raise Radflood21ValidationError("Zenodo productmetadata wijkt af")
    creators = metadata["creators"]
    if type(creators) is not list or tuple(
        (
            _mapping(item, "Zenodo creator").get("name"),
            _mapping(item, "Zenodo creator").get("affiliation"),
        )
        for item in creators
    ) != contract.creators:
        raise Radflood21ValidationError("Zenodo creatorprovenance wijkt af")

    files = value["files"]
    if type(files) is not list or len(files) != len(contract.artifacts):
        raise Radflood21ValidationError("Zenodo bestandenset is onvolledig")
    expected_by_name = {item.name: item for item in contract.artifacts}
    seen: set[str] = set()
    for file_value in files:
        item = _mapping(file_value, "Zenodo bestand")
        if not {"key", "size", "checksum"}.issubset(item):
            raise Radflood21ValidationError("Zenodo bestandmetadata is onvolledig")
        name = item["key"]
        artifact = expected_by_name.get(name)
        if artifact is None or name in seen:
            raise Radflood21ValidationError("Zenodo bestandidentiteit wijkt af")
        seen.add(name)
        if (
            item["size"] != artifact.size_bytes
            or type(item["size"]) is not int
            or item["checksum"] != artifact.zenodo_checksum
        ):
            raise Radflood21ValidationError(
                f"Zenodo checksum/grootte voor {name!r} wijkt af"
            )
    if seen != set(expected_by_name):
        raise Radflood21ValidationError("Zenodo bestandenset mist een artefact")

    return ZenodoSnapshotReceipt(
        record_id=contract.record_id,
        revision=contract.revision,
        metadata_sha256=digest,
        artifact_count=len(seen),
    )


def validate_local_artifact(
    path: Path,
    artifact: Radflood21Artifact,
    *,
    chunk_bytes: int = DEFAULT_FILE_CHUNK_BYTES,
) -> LocalArtifactReceipt:
    """Stream an exact local file once; never extract or interpret content."""

    if not isinstance(artifact, Radflood21Artifact):
        raise TypeError("artifact moet Radflood21Artifact zijn")
    if (
        isinstance(chunk_bytes, bool)
        or not isinstance(chunk_bytes, int)
        or chunk_bytes <= 0
        or chunk_bytes > MAX_FILE_CHUNK_BYTES
    ):
        raise ValueError("ongeldige RADFLOOD21 chunkgrootte")
    candidate = Path(path)
    if candidate.name != artifact.name:
        raise Radflood21ValidationError("lokale RADFLOOD21-bestandsnaam wijkt af")
    try:
        before = candidate.stat(follow_symlinks=False)
    except OSError as exc:
        raise Radflood21ValidationError("lokaal RADFLOOD21-artefact ontbreekt") from exc
    if candidate.is_symlink() or not stat.S_ISREG(before.st_mode):
        raise Radflood21ValidationError("onveilig lokaal RADFLOOD21-artefact")
    if before.st_size != artifact.size_bytes:
        raise Radflood21ValidationError("lokale RADFLOOD21-grootte wijkt af")

    descriptor = _open_readonly_no_follow(candidate)
    try:
        opened = os.fstat(descriptor)
        if (
            not _same_file_identity(before, opened)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_size != artifact.size_bytes
        ):
            raise Radflood21ValidationError(
                "RADFLOOD21-artefact wijzigde tijdens openen"
            )
        upstream_md5 = hashlib.md5(usedforsecurity=False)
        local_sha256 = hashlib.sha256()
        read_bytes = 0
        with os.fdopen(descriptor, "rb", buffering=0) as stream:
            descriptor = -1
            while True:
                chunk = stream.read(chunk_bytes)
                if not chunk:
                    break
                read_bytes += len(chunk)
                if read_bytes > artifact.size_bytes:
                    raise Radflood21ValidationError(
                        "RADFLOOD21-artefact groeide tijdens lezen"
                    )
                upstream_md5.update(chunk)
                local_sha256.update(chunk)
            opened_after = os.fstat(stream.fileno())
    except Radflood21ValidationError:
        raise
    except OSError as exc:
        raise Radflood21ValidationError(
            "RADFLOOD21-artefact kon niet volledig worden gelezen"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        after = candidate.stat(follow_symlinks=False)
    except OSError as exc:
        raise Radflood21ValidationError(
            "RADFLOOD21-artefact verdween tijdens validatie"
        ) from exc
    if (
        read_bytes != artifact.size_bytes
        or not _same_file_identity(opened, opened_after)
        or not _same_file_identity(opened, after)
        or opened_after.st_size != opened.st_size
        or opened_after.st_mtime_ns != opened.st_mtime_ns
    ):
        raise Radflood21ValidationError(
            "RADFLOOD21-artefact wijzigde tijdens validatie"
        )
    if upstream_md5.hexdigest() != artifact.checksum_hex:
        raise Radflood21ValidationError("lokale RADFLOOD21-MD5 wijkt af")
    return LocalArtifactReceipt(
        name=artifact.name,
        size_bytes=artifact.size_bytes,
        official_md5=upstream_md5.hexdigest(),
        local_sha256=local_sha256.hexdigest(),
    )


def validate_official_corpus_archive(path: Path) -> LocalArtifactReceipt:
    """Validate the 2.6 GB archive only when a caller already has it locally."""

    return validate_local_artifact(path, load_official_manifest().corpus_archive)


def _parse_utc(value: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise Radflood21ValidationError("RADFLOOD21-tijd moet expliciet UTC zijn")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise Radflood21ValidationError("ongeldige RADFLOOD21 UTC-tijd") from exc
    if parsed.tzinfo != timezone.utc or not math.isfinite(parsed.timestamp()):
        raise Radflood21ValidationError("ongeldige RADFLOOD21 UTC-tijd")
    return parsed


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def accumulation_support(
    end_timestamp: str,
    duration_seconds: int,
) -> AccumulationSupport:
    """Return the official [end-duration, end] support without frame claims."""

    if (
        isinstance(duration_seconds, bool)
        or not isinstance(duration_seconds, int)
        or duration_seconds not in SUPPORTED_ACCUMULATION_SECONDS
    ):
        raise Radflood21ValidationError(
            "RADFLOOD21 ondersteunt alleen 5-minuten- en uuraccumulaties"
        )
    end = _parse_utc(end_timestamp)
    start = end - timedelta(seconds=duration_seconds)
    return AccumulationSupport(
        start=_format_utc(start),
        end=_format_utc(end),
        duration_seconds=duration_seconds,
    )


__all__ = [
    "CORPUS_ARCHIVE_MD5",
    "CORPUS_ARCHIVE_NAME",
    "CORPUS_ARCHIVE_SIZE_BYTES",
    "CORE_ANALYSIS_END",
    "CORE_ANALYSIS_START",
    "LocalArtifactReceipt",
    "MANIFEST_FILENAME",
    "OPEN_DECODER_GATES",
    "PINNED_MANIFEST_CANONICAL_SHA256",
    "PRODUCT_END",
    "PRODUCT_START",
    "Radflood21Artifact",
    "Radflood21Manifest",
    "Radflood21ValidationError",
    "SCHEMA_VERSION",
    "SHADOW_ONLY",
    "SUPPORTED_ACCUMULATION_SECONDS",
    "VISUALISATION_MD5",
    "VISUALISATION_NAME",
    "VISUALISATION_SIZE_BYTES",
    "ZENODO_CONCEPT_DOI",
    "ZENODO_CONCEPT_RECORD_ID",
    "ZENODO_DOI",
    "ZENODO_LICENSE_ID",
    "ZENODO_RECORD_ID",
    "ZenodoSnapshotReceipt",
    "accumulation_support",
    "canonical_manifest_sha256",
    "load_official_manifest",
    "validate_local_artifact",
    "validate_manifest_mapping",
    "validate_official_corpus_archive",
    "validate_zenodo_record_snapshot",
]
