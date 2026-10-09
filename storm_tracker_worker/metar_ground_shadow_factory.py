"""Closed-world factory for the METAR ground-fusion shadow process.

This module is deliberately not wired into the worker service.  It constructs
the existing :class:`DurableGroundFusionShadowRuntime` only from an exact
parent-authorised artifact set.  The artifact set remains candidate-only:
neither this factory nor its process result grants meteorological admission or
an operational confirmation claim.

The production factory installs a child-local audit hook before opening the
authorised artifacts.  The hook denies network/process creation and file
access outside the exact immutable files, the raw manifest tree, and the two
explicit mutable stores (receipt database and sealed archive).
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import sys
import threading
from typing import Any, Mapping, Sequence

from . import metar_ground_evidence, metar_native
from .canonical_ground_evidence_store import (
    CanonicalGroundEvidenceStore,
    GroundEvidenceLimits,
)
from .ground_fusion_shadow_runtime import (
    DurableGroundFusionShadowRuntime,
    GroundFusionShadowLimits,
    GroundFusionShadowRequest,
    GroundFusionShadowRunResult,
    footprint_geometry_sha256,
)
from .raw_artifacts import RawArtifactLimits, RawArtifactStore
from .tracking_core.engine.canonical_ground_fusion_bridge import (
    GroundSemanticsCertificate,
    GroundSemanticsProfile,
)
from .tracking_core.engine.ground_fusion import GroundFusionPolicy
from .tracking_core.engine.physical_site_catalog import (
    PHYSICAL_SITE_CATALOG_ALGORITHM,
    PhysicalSiteCatalog,
    PhysicalSiteRecord,
    SensorCapability,
    SitingAttribute,
    VerificationEvidence,
    VerifiedStationAlias,
    VerifiedStationIdentifier,
)


METAR_GROUND_FACTORY_CONFIG_CONTRACT = (
    "stv3-metar-ground-shadow-factory-config-v1"
)
METAR_RAW_TREE_MANIFEST_CONTRACT = "stv3-metar-sealed-raw-tree-v1"
METAR_SITE_AUTHORITY_CONTRACT = "stv3-metar-site-authority-v1"
METAR_SEMANTICS_AUTHORITY_CONTRACT = "stv3-metar-semantics-authority-v1"
METAR_FOOTPRINT_AUTHORITY_CONTRACT = "stv3-metar-footprint-authority-v1"
METAR_CLOSED_WORLD_FACTORY_ID = "stv3-metar-ground-closed-world-factory-v1"

_SHA_LENGTH = 64
_MAX_DOCUMENT_BYTES = 2 * 1024 * 1024
_MAX_RAW_FILES = 256
_MAX_RAW_TREE_BYTES = 2 * 1024 * 1024 * 1024
_MAX_SITE_RECORDS = 100_000
_MAX_FOOTPRINT_BINDINGS = 64
_EXPECTED_PATH_ROLES = {
    "canonical_ground_label": "canonical_ground_database",
    "receipt_store_label": "ground_receipt_database",
    "archive_directory_label": "ground_archive_directory",
    "raw_store_label": "read_directory",
    "raw_manifest_label": "read_file",
    "site_catalog_label": "read_file",
    "semantics_label": "read_file",
    "footprint_label": "read_file",
}
_NETWORK_AUDIT_PREFIXES = (
    "socket.",
    "subprocess.",
    "os.exec",
    "os.spawn",
    "os.posix_spawn",
    "os.system",
)


class MetarGroundFactoryError(RuntimeError):
    """An exact closed-world factory contract failed closed."""


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _sha(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise MetarGroundFactoryError(f"{label} checksum is invalid")
    return value


def _text(value: object, label: str, *, maximum: int = 512) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\0" in value
        or any(ord(character) < 32 for character in value)
    ):
        raise MetarGroundFactoryError(f"{label} identifier is invalid")
    return value


def _finite(value: object, label: str) -> float:
    if isinstance(value, bool):
        raise MetarGroundFactoryError(f"{label} number is invalid")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise MetarGroundFactoryError(f"{label} number is invalid") from exc
    if not math.isfinite(result):
        raise MetarGroundFactoryError(f"{label} number is invalid")
    return result


def _exact_keys(value: object, expected: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise MetarGroundFactoryError(f"{label} schema is not exact")
    return value


def _is_link_or_junction(path: Path) -> bool:
    try:
        junction = getattr(path, "is_junction", None)
        return path.is_symlink() or (junction is not None and junction())
    except OSError as exc:
        raise MetarGroundFactoryError("authorised path cannot be inspected") from exc


def _read_exact_file(path: Path, *, maximum_bytes: int = _MAX_DOCUMENT_BYTES) -> bytes:
    try:
        status = path.lstat()
        if _is_link_or_junction(path) or not stat.S_ISREG(status.st_mode):
            raise MetarGroundFactoryError("authorised input is not a regular file")
        if status.st_size > maximum_bytes:
            raise MetarGroundFactoryError("authorised input exceeds its byte bound")
        body = path.read_bytes()
    except MetarGroundFactoryError:
        raise
    except OSError as exc:
        raise MetarGroundFactoryError("authorised input cannot be read") from exc
    if len(body) != status.st_size:
        raise MetarGroundFactoryError("authorised input changed while read")
    return body


def _hash_exact_file(path: Path, *, maximum_bytes: int) -> tuple[str, int]:
    """Hash one regular file with bounded memory and stable before/after stat."""

    try:
        before = path.lstat()
        if _is_link_or_junction(path) or not stat.S_ISREG(before.st_mode):
            raise MetarGroundFactoryError("authorised input is not a regular file")
        if before.st_size > maximum_bytes:
            raise MetarGroundFactoryError("authorised input exceeds its byte bound")
        digest = hashlib.sha256()
        total = 0
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > maximum_bytes:
                    raise MetarGroundFactoryError(
                        "authorised input exceeds its byte bound"
                    )
                digest.update(chunk)
        after = path.lstat()
    except MetarGroundFactoryError:
        raise
    except OSError as exc:
        raise MetarGroundFactoryError("authorised input cannot be read") from exc
    if (
        total != before.st_size
        or after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
        or after.st_ino != before.st_ino
    ):
        raise MetarGroundFactoryError("authorised input changed while hashed")
    return digest.hexdigest(), total


def _load_canonical_document(path: Path, expected_sha256: str) -> Mapping[str, Any]:
    body = _read_exact_file(path)
    if hashlib.sha256(body).hexdigest() != _sha(expected_sha256, "document"):
        raise MetarGroundFactoryError("authorised document checksum differs")
    try:
        value = json.loads(body.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MetarGroundFactoryError("authorised document is not canonical JSON") from exc
    if not isinstance(value, dict) or _canonical_json(value) != body:
        raise MetarGroundFactoryError("authorised document is not canonical JSON")
    return value


@dataclass(frozen=True, slots=True)
class _PathRole:
    label: str
    access: str
    path: Path
    identity_sha256: str | None


def _authority_paths(authority: Mapping[str, Any]) -> dict[str, _PathRole]:
    paths = authority.get("authorized_paths")
    if not isinstance(paths, list) or len(paths) != len(_EXPECTED_PATH_ROLES):
        raise MetarGroundFactoryError("authorised path set is not closed-world")
    result: dict[str, _PathRole] = {}
    resolved: set[Path] = set()
    for raw in paths:
        item = _exact_keys(
            raw,
            {"access", "identity_sha256", "label", "path"},
            "authorised path",
        )
        label = _text(item["label"], "authorised path label", maximum=128)
        access = _text(item["access"], "authorised path access", maximum=64)
        raw_path = _text(item["path"], "authorised path", maximum=1024)
        path = Path(raw_path)
        try:
            actual = path.resolve(strict=True)
        except OSError as exc:
            raise MetarGroundFactoryError("authorised path cannot be resolved") from exc
        if not path.is_absolute() or actual != path or _is_link_or_junction(path):
            raise MetarGroundFactoryError("authorised path is not canonical")
        identity = item["identity_sha256"]
        if access == "read_file":
            identity = _sha(identity, "read file")
        elif identity is not None and access != "canonical_ground_database":
            raise MetarGroundFactoryError("mutable path claims immutable identity")
        if label in result or path in resolved:
            raise MetarGroundFactoryError("authorised path set contains duplicates")
        result[label] = _PathRole(label, access, path, identity)
        resolved.add(path)
    config = _exact_keys(
        authority.get("factory_config"),
        {
            "admission_blockers",
            "authority_scope_sha256",
            "canonical_content_sha256",
            "capture_id",
            "contract",
            "footprint_document_sha256",
            "footprint_label",
            "fusion_policy",
            "ground_evidence_limits",
            "operational_admission_granted",
            "raw_artifact_limits",
            "raw_manifest_label",
            "raw_store_label",
            "raw_tree_manifest_sha256",
            "semantics_document_sha256",
            "semantics_label",
            "site_catalog_document_sha256",
            "site_catalog_label",
        },
        "factory config",
    )
    if config["contract"] != METAR_GROUND_FACTORY_CONFIG_CONTRACT:
        raise MetarGroundFactoryError("factory config contract differs")
    for field, access in _EXPECTED_PATH_ROLES.items():
        label = authority.get(field) if field in authority else config.get(field)
        label = _text(label, field, maximum=128)
        role = result.get(label)
        if role is None or role.access != access:
            raise MetarGroundFactoryError("factory path role differs")
    return result


def _selected_path(
    authority: Mapping[str, Any],
    paths: Mapping[str, _PathRole],
    field: str,
) -> _PathRole:
    config = authority["factory_config"]
    label = authority[field] if field in authority else config[field]
    return paths[str(label)]


@dataclass(frozen=True, slots=True)
class _RawTreeEntry:
    relative_path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class _RawTreeManifest:
    directories: tuple[str, ...]
    entries: tuple[_RawTreeEntry, ...]


def _parse_raw_manifest(value: Mapping[str, Any]) -> _RawTreeManifest:
    document = _exact_keys(
        value,
        {
            "contract", "directories", "directory_count", "entries",
            "entry_count", "total_bytes", "tree_sha256",
        },
        "raw tree manifest",
    )
    if document["contract"] != METAR_RAW_TREE_MANIFEST_CONTRACT:
        raise MetarGroundFactoryError("raw tree manifest contract differs")
    raw_entries = document["entries"]
    if not isinstance(raw_entries, list) or not 1 <= len(raw_entries) <= _MAX_RAW_FILES:
        raise MetarGroundFactoryError("raw tree entry bound is invalid")
    entries: list[_RawTreeEntry] = []
    seen: set[str] = set()
    total = 0
    for raw in raw_entries:
        item = _exact_keys(raw, {"relative_path", "sha256", "size_bytes"}, "raw tree entry")
        relative = _text(item["relative_path"], "raw relative path", maximum=512)
        candidate = Path(relative)
        if candidate.is_absolute() or ".." in candidate.parts or str(candidate).replace("\\", "/") != relative:
            raise MetarGroundFactoryError("raw relative path is unsafe")
        size = item["size_bytes"]
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise MetarGroundFactoryError("raw tree size is invalid")
        if relative in seen:
            raise MetarGroundFactoryError("raw tree path is duplicated")
        total += size
        if total > _MAX_RAW_TREE_BYTES:
            raise MetarGroundFactoryError("raw tree byte bound exceeded")
        seen.add(relative)
        entries.append(_RawTreeEntry(relative, size, _sha(item["sha256"], "raw entry")))
    entries.sort(key=lambda item: item.relative_path)
    raw_directories = document["directories"]
    if not isinstance(raw_directories, list) or len(raw_directories) > _MAX_RAW_FILES:
        raise MetarGroundFactoryError("raw directory bound is invalid")
    directories: list[str] = []
    for raw_directory in raw_directories:
        relative = _text(raw_directory, "raw relative directory", maximum=512)
        candidate = Path(relative)
        if (
            candidate.is_absolute()
            or ".." in candidate.parts
            or str(candidate).replace("\\", "/") != relative
            or relative in directories
        ):
            raise MetarGroundFactoryError("raw relative directory is unsafe")
        directories.append(relative)
    directories.sort()
    canonical_entries = [
        {"relative_path": item.relative_path, "sha256": item.sha256, "size_bytes": item.size_bytes}
        for item in entries
    ]
    if (
        document["directory_count"] != len(directories)
        or raw_directories != directories
        or document["entry_count"] != len(entries)
        or document["total_bytes"] != total
        or document["tree_sha256"] != _digest({
            "directories": directories,
            "entries": canonical_entries,
        })
        or raw_entries != canonical_entries
    ):
        raise MetarGroundFactoryError("raw tree manifest accounting differs")
    return _RawTreeManifest(tuple(directories), tuple(entries))


def _verify_raw_tree(root: Path, manifest: _RawTreeManifest) -> tuple[Path, ...]:
    expected = {entry.relative_path: entry for entry in manifest.entries}
    expected_directories = set(manifest.directories)
    actual: dict[str, Path] = {}
    actual_directories: set[str] = set()
    try:
        pending = [root]
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    candidate = Path(entry.path)
                    if entry.is_symlink() or _is_link_or_junction(candidate):
                        raise MetarGroundFactoryError(
                            "raw tree contains a link or junction"
                        )
                    if entry.is_dir(follow_symlinks=False):
                        actual_directories.add(
                            candidate.relative_to(root).as_posix()
                        )
                        pending.append(candidate)
                    elif entry.is_file(follow_symlinks=False):
                        relative = candidate.relative_to(root).as_posix()
                        actual[relative] = candidate
                    else:
                        raise MetarGroundFactoryError(
                            "raw tree contains a special file"
                        )
                    if (
                        len(actual) > _MAX_RAW_FILES
                        or len(actual_directories) > _MAX_RAW_FILES
                    ):
                        raise MetarGroundFactoryError(
                            "raw tree entry bound exceeded"
                        )
    except MetarGroundFactoryError:
        raise
    except OSError as exc:
        raise MetarGroundFactoryError("raw tree cannot be enumerated") from exc
    if set(actual) != set(expected) or actual_directories != expected_directories:
        raise MetarGroundFactoryError("raw tree has missing or additional entries")
    verified: list[Path] = []
    for relative in sorted(expected):
        entry = expected[relative]
        path = actual[relative]
        digest, size = _hash_exact_file(
            path,
            maximum_bytes=max(entry.size_bytes, 1),
        )
        if size != entry.size_bytes or digest != entry.sha256:
            raise MetarGroundFactoryError("raw tree file identity differs")
        verified.append(path)
    return tuple(verified)


def _verification(value: object, label: str) -> VerificationEvidence:
    item = _exact_keys(value, {"authority", "content_sha256", "reference", "verified_at"}, label)
    return VerificationEvidence(
        authority=item["authority"],
        reference=item["reference"],
        verified_at=item["verified_at"],
        content_sha256=item["content_sha256"],
    )


def _site_catalog(value: Mapping[str, Any], expected_scope: str) -> PhysicalSiteCatalog:
    document = _exact_keys(
        value,
        {"admission", "authority_scope_sha256", "catalog", "contract", "site_assertions_sha256"},
        "site authority",
    )
    if document["contract"] != METAR_SITE_AUTHORITY_CONTRACT:
        raise MetarGroundFactoryError("site authority contract differs")
    if _sha(document["authority_scope_sha256"], "site scope") != expected_scope:
        raise MetarGroundFactoryError("site authority scope differs")
    admission = _exact_keys(document["admission"], {"granted", "missing", "status"}, "site admission")
    if admission["granted"] is not False or admission["status"] != "candidate_only_unadmitted":
        raise MetarGroundFactoryError("site authority attempted operational admission")
    if not isinstance(admission["missing"], list) or not admission["missing"]:
        raise MetarGroundFactoryError("site authority missing-evidence list is absent")
    catalog_value = _exact_keys(
        document["catalog"],
        {"algorithm_version", "catalog_fingerprint", "catalog_name", "catalog_version", "proximity_audit_radius_m", "records"},
        "physical site catalog",
    )
    if catalog_value["algorithm_version"] != PHYSICAL_SITE_CATALOG_ALGORITHM:
        raise MetarGroundFactoryError("site catalog algorithm differs")
    raw_records = catalog_value["records"]
    if not isinstance(raw_records, list) or len(raw_records) > _MAX_SITE_RECORDS:
        raise MetarGroundFactoryError("site catalog record bound exceeded")
    if document["site_assertions_sha256"] != _digest({
        "admission": admission,
        "catalog_name": catalog_value["catalog_name"],
        "proximity_audit_radius_m": catalog_value["proximity_audit_radius_m"],
        "records": raw_records,
    }):
        raise MetarGroundFactoryError("site assertion checksum differs")
    records: list[PhysicalSiteRecord] = []
    for raw_record in raw_records:
        record = _exact_keys(
            raw_record,
            {
                "aliases", "authoritative_identifiers", "capabilities", "coordinate_tolerance_m",
                "elevation_m", "independence_evidence", "independence_group", "independence_group_key",
                "latitude", "lifecycle_evidence", "longitude", "physical_site_id", "replaces_site_key",
                "site_evidence", "site_key", "siting", "valid_from", "valid_until",
            },
            "physical site record",
        )
        aliases = tuple(
            VerifiedStationAlias(
                provider_id=item["provider_id"], station_id=item["station_id"],
                mapping_evidence=_verification(item["mapping_evidence"], "alias evidence"),
                valid_from=item["valid_from"], valid_until=item["valid_until"],
            )
            for item in (
                _exact_keys(raw, {"mapping_evidence", "provider_id", "station_id", "valid_from", "valid_until"}, "station alias")
                for raw in record["aliases"]
            )
        )
        identifiers = tuple(
            VerifiedStationIdentifier(
                namespace=item["namespace"], value=item["value"],
                evidence=_verification(item["evidence"], "identifier evidence"),
            )
            for item in (
                _exact_keys(raw, {"evidence", "namespace", "value"}, "station identifier")
                for raw in record["authoritative_identifiers"]
            )
        )
        capabilities = tuple(
            SensorCapability(
                provider_id=item["provider_id"], sensor_id=item["sensor_id"],
                measurement_kind=item["measurement_kind"],
                can_observe_absence=item["can_observe_absence"],
                evidence=_verification(item["evidence"], "capability evidence"),
                valid_from=item["valid_from"], valid_until=item["valid_until"],
            )
            for item in (
                _exact_keys(raw, {"can_observe_absence", "evidence", "measurement_kind", "provider_id", "sensor_id", "valid_from", "valid_until"}, "sensor capability")
                for raw in record["capabilities"]
            )
        )
        siting = tuple(
            SitingAttribute(
                name=item["name"], value=item["value"],
                evidence=_verification(item["evidence"], "siting evidence"),
            )
            for item in (
                _exact_keys(raw, {"evidence", "name", "value"}, "siting attribute")
                for raw in record["siting"]
            )
        )
        records.append(PhysicalSiteRecord(
            site_key=record["site_key"], latitude=record["latitude"], longitude=record["longitude"],
            coordinate_tolerance_m=record["coordinate_tolerance_m"],
            independence_group_key=record["independence_group_key"],
            site_evidence=_verification(record["site_evidence"], "site evidence"),
            independence_evidence=_verification(record["independence_evidence"], "independence evidence"),
            aliases=aliases, authoritative_identifiers=identifiers, capabilities=capabilities,
            siting=siting, elevation_m=record["elevation_m"], valid_from=record["valid_from"],
            valid_until=record["valid_until"], replaces_site_key=record["replaces_site_key"],
            lifecycle_evidence=(None if record["lifecycle_evidence"] is None else _verification(record["lifecycle_evidence"], "lifecycle evidence")),
        ))
    catalog = PhysicalSiteCatalog(
        catalog_name=catalog_value["catalog_name"],
        catalog_version=catalog_value["catalog_version"],
        records=tuple(records),
        proximity_audit_radius_m=catalog_value["proximity_audit_radius_m"],
    )
    if json.loads(catalog.canonical_json) != catalog_value:
        raise MetarGroundFactoryError("site catalog canonical identity differs")
    return catalog


def _semantics(value: Mapping[str, Any]) -> tuple[GroundSemanticsCertificate, ...]:
    document = _exact_keys(value, {"admission", "certificates", "contract"}, "semantics authority")
    if document["contract"] != METAR_SEMANTICS_AUTHORITY_CONTRACT:
        raise MetarGroundFactoryError("semantics authority contract differs")
    admission = _exact_keys(document["admission"], {"granted", "missing", "status"}, "semantics admission")
    if admission["granted"] is not False or admission["status"] != "candidate_only_unadmitted":
        raise MetarGroundFactoryError("semantics authority attempted operational admission")
    if not isinstance(admission["missing"], list) or not admission["missing"]:
        raise MetarGroundFactoryError("semantics missing-evidence list is absent")
    raw_certificates = document["certificates"]
    if not isinstance(raw_certificates, list) or not 1 <= len(raw_certificates) <= 16:
        raise MetarGroundFactoryError("semantics certificate bound is invalid")
    certificates: list[GroundSemanticsCertificate] = []
    for raw in raw_certificates:
        item = _exact_keys(
            raw,
            {
                "certificate_id", "certificate_version", "config_sha256", "decoder_identity",
                "decoder_version", "fingerprint", "primary_sources", "product", "profile",
                "provider", "review_evidence", "review_evidence_sha256",
            },
            "semantics certificate",
        )
        review = item["review_evidence"]
        if not isinstance(review, dict) or _digest(review) != item["review_evidence_sha256"]:
            raise MetarGroundFactoryError("semantics review checksum differs")
        sources = item["primary_sources"]
        if not isinstance(sources, list):
            raise MetarGroundFactoryError("semantics primary sources are invalid")
        try:
            profile = GroundSemanticsProfile(item["profile"])
        except (TypeError, ValueError) as exc:
            raise MetarGroundFactoryError("semantics profile is invalid") from exc
        certificate = GroundSemanticsCertificate(
            certificate_id=item["certificate_id"], certificate_version=item["certificate_version"],
            provider=item["provider"], product=item["product"], decoder_identity=item["decoder_identity"],
            decoder_version=item["decoder_version"], config_sha256=item["config_sha256"],
            profile=profile, primary_sources=tuple(sources), review_evidence_sha256=item["review_evidence_sha256"],
        )
        if certificate.fingerprint != item["fingerprint"]:
            raise MetarGroundFactoryError("semantics certificate fingerprint differs")
        certificates.append(certificate)
    fingerprints = [item.fingerprint for item in certificates]
    if fingerprints != sorted(fingerprints) or len(fingerprints) != len(set(fingerprints)):
        raise MetarGroundFactoryError("semantics certificates are not canonical")
    return tuple(certificates)


@dataclass(frozen=True, slots=True)
class _FootprintBinding:
    cell_binding_sha256: str
    decision_key_sha256: str
    evaluated_at: float
    footprint_geometry_sha256: str
    footprint_proof_sha256: str
    region_key_sha256: str
    source_kind: str
    target_snapshot_sha256: str


def _footprint_bindings(value: Mapping[str, Any]) -> dict[str, _FootprintBinding]:
    document = _exact_keys(value, {"admission", "bindings", "contract"}, "footprint authority")
    if document["contract"] != METAR_FOOTPRINT_AUTHORITY_CONTRACT:
        raise MetarGroundFactoryError("footprint authority contract differs")
    admission = _exact_keys(document["admission"], {"granted", "missing", "status"}, "footprint admission")
    if admission["granted"] is not False or admission["status"] != "candidate_only_unadmitted":
        raise MetarGroundFactoryError("footprint authority attempted operational admission")
    if not isinstance(admission["missing"], list) or not admission["missing"]:
        raise MetarGroundFactoryError("footprint missing-evidence list is absent")
    raw_bindings = document["bindings"]
    if not isinstance(raw_bindings, list) or len(raw_bindings) > _MAX_FOOTPRINT_BINDINGS:
        raise MetarGroundFactoryError("footprint binding bound exceeded")
    result: dict[str, _FootprintBinding] = {}
    for raw in raw_bindings:
        item = _exact_keys(
            raw,
            {
                "cell_binding_sha256", "decision_key_sha256", "evaluated_at",
                "footprint_geometry_sha256", "footprint_proof_sha256", "region_key_sha256",
                "source_kind", "target_snapshot_sha256",
            },
            "footprint binding",
        )
        binding = _FootprintBinding(
            cell_binding_sha256=_sha(item["cell_binding_sha256"], "cell binding"),
            decision_key_sha256=_sha(item["decision_key_sha256"], "decision key"),
            evaluated_at=_finite(item["evaluated_at"], "evaluated at"),
            footprint_geometry_sha256=_sha(item["footprint_geometry_sha256"], "footprint geometry"),
            footprint_proof_sha256=_sha(item["footprint_proof_sha256"], "footprint proof"),
            region_key_sha256=_sha(item["region_key_sha256"], "region key"),
            source_kind=_text(item["source_kind"], "footprint source kind", maximum=128),
            target_snapshot_sha256=_sha(item["target_snapshot_sha256"], "target snapshot"),
        )
        if binding.source_kind not in {
            "preserved_science_receipt",
            "fixture_only_unadmitted",
        }:
            raise MetarGroundFactoryError("footprint source kind is not closed-world")
        if binding.cell_binding_sha256 in result:
            raise MetarGroundFactoryError("footprint binding is duplicated")
        result[binding.cell_binding_sha256] = binding
    if raw_bindings != [
        {
            "cell_binding_sha256": item.cell_binding_sha256,
            "decision_key_sha256": item.decision_key_sha256,
            "evaluated_at": item.evaluated_at,
            "footprint_geometry_sha256": item.footprint_geometry_sha256,
            "footprint_proof_sha256": item.footprint_proof_sha256,
            "region_key_sha256": item.region_key_sha256,
            "source_kind": item.source_kind,
            "target_snapshot_sha256": item.target_snapshot_sha256,
        }
        for item in sorted(result.values(), key=lambda candidate: candidate.cell_binding_sha256)
    ]:
        raise MetarGroundFactoryError("footprint bindings are not canonical")
    return result


class _ClosedWorldAuditGuard:
    """One-way child audit guard; it is never installed in the parent."""

    def __init__(
        self,
        *,
        exact_files: Sequence[Path],
        raw_root: Path,
        receipt_path: Path,
        archive_root: Path,
        directory_paths: Sequence[Path],
        mutable_sidecars: Sequence[Path],
    ) -> None:
        self._lock = threading.RLock()
        self._exact_files = {self._key(path) for path in exact_files}
        self._raw_root = raw_root
        self._raw_files: set[str] | None = None
        self._receipt_path = receipt_path
        self._archive_root = archive_root
        self._directories = {self._key(path) for path in directory_paths}
        self._mutable_sidecars = {self._key(path) for path in mutable_sidecars}
        self.denied_accesses = 0
        self.network_attempts = 0

    @staticmethod
    def _key(path: Path) -> str:
        return os.path.normcase(os.path.abspath(os.fspath(path)))

    def admit_raw_files(self, paths: Sequence[Path]) -> None:
        with self._lock:
            if self._raw_files is not None:
                raise MetarGroundFactoryError("raw audit file set was already sealed")
            self._raw_files = {self._key(path) for path in paths}

    def _under(self, candidate: str, root: Path) -> bool:
        try:
            return os.path.commonpath((candidate, self._key(root))) == self._key(root)
        except ValueError:
            return False

    def _file_allowed(self, raw: object) -> bool:
        if isinstance(raw, int):
            return True
        if not isinstance(raw, (str, bytes, os.PathLike)):
            return False
        path = self._key(Path(os.fsdecode(raw)))
        if path in self._exact_files:
            return True
        if self._raw_files is not None and path in self._raw_files:
            return True
        receipt = self._key(self._receipt_path)
        if path in {receipt, receipt + "-journal"}:
            return True
        if path in self._mutable_sidecars:
            return True
        if self._under(path, self._archive_root):
            return True
        return path in self._directories

    def __call__(self, event: str, arguments: tuple[Any, ...]) -> None:
        if event.startswith(_NETWORK_AUDIT_PREFIXES):
            self.network_attempts += 1
            raise PermissionError("closed-world network/process access denied")
        raw = None
        if event == "open" and arguments:
            raw = arguments[0]
        elif event in {"os.listdir", "os.scandir", "os.remove", "os.mkdir", "os.rmdir"} and arguments:
            raw = arguments[0]
        elif event in {"os.rename", "os.replace"} and arguments:
            if len(arguments) < 2 or not all(
                self._file_allowed(item) for item in arguments[:2]
            ):
                self.denied_accesses += 1
                raise PermissionError("closed-world path access denied")
            return
        if raw is not None and not self._file_allowed(raw):
            self.denied_accesses += 1
            raise PermissionError("closed-world path access denied")


class ClosedWorldMetarGroundShadowRuntime(DurableGroundFusionShadowRuntime):
    """The existing durable runtime plus exact target/cell authority checks."""

    def __init__(
        self,
        *args: Any,
        footprint_bindings: Mapping[str, _FootprintBinding],
        audit_guard: _ClosedWorldAuditGuard | None,
        immutable_canonical_path: Path,
        immutable_canonical_sha256: str,
        immutable_raw_root: Path,
        immutable_raw_manifest: _RawTreeManifest,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._closed_world_footprint_bindings = dict(footprint_bindings)
        self._closed_world_audit_guard = audit_guard
        self._closed_world_canonical_path = immutable_canonical_path
        self._closed_world_canonical_sha256 = immutable_canonical_sha256
        self._closed_world_raw_root = immutable_raw_root
        self._closed_world_raw_manifest = immutable_raw_manifest

    def _verify_immutable_inputs(self) -> None:
        if _hash_exact_file(
            self._closed_world_canonical_path,
            maximum_bytes=_MAX_RAW_TREE_BYTES,
        )[0] != self._closed_world_canonical_sha256:
            raise MetarGroundFactoryError("canonical ground content checksum differs")
        _verify_raw_tree(
            self._closed_world_raw_root,
            self._closed_world_raw_manifest,
        )

    def process(self, request: GroundFusionShadowRequest, *, deadline_at: float) -> GroundFusionShadowRunResult:
        if not isinstance(request, GroundFusionShadowRequest):
            raise TypeError("request must be GroundFusionShadowRequest")
        binding = self._closed_world_footprint_bindings.get(request.cell.fingerprint)
        if (
            binding is None
            or binding.decision_key_sha256 != request.decision_key_sha256
            or binding.region_key_sha256 != request.region_key_sha256
            or binding.target_snapshot_sha256 != request.target.fingerprint
            or binding.evaluated_at != request.evaluated_at
            or binding.footprint_geometry_sha256 != footprint_geometry_sha256(request.cell.footprint)
            or binding.footprint_proof_sha256 != request.cell.footprint_proof.fingerprint
        ):
            raise ValueError("target/cell/footprint authority differs")
        self._verify_immutable_inputs()
        result = super().process(request, deadline_at=deadline_at)
        self._verify_immutable_inputs()
        guard = self._closed_world_audit_guard
        if guard is not None and (guard.denied_accesses or guard.network_attempts):
            raise MetarGroundFactoryError("closed-world audit invariant failed")
        return result


def _policy(value: object) -> GroundFusionPolicy:
    item = _exact_keys(
        value,
        {
            "freshness_seconds_by_provider", "maximum_future_skew_seconds",
            "maximum_site_coordinate_disagreement_km", "minimum_independent_wet_groups",
            "minimum_interval_overlap_fraction", "spatial_tolerance_km", "temporal_tolerance_seconds",
        },
        "fusion policy",
    )
    pairs = item["freshness_seconds_by_provider"]
    if not isinstance(pairs, list) or any(not isinstance(pair, list) or len(pair) != 2 for pair in pairs):
        raise MetarGroundFactoryError("fusion freshness policy is invalid")
    policy = GroundFusionPolicy(
        freshness_seconds_by_provider=tuple((pair[0], pair[1]) for pair in pairs),
        minimum_independent_wet_groups=item["minimum_independent_wet_groups"],
        spatial_tolerance_km=item["spatial_tolerance_km"],
        temporal_tolerance_seconds=item["temporal_tolerance_seconds"],
        minimum_interval_overlap_fraction=item["minimum_interval_overlap_fraction"],
        maximum_site_coordinate_disagreement_km=item["maximum_site_coordinate_disagreement_km"],
        maximum_future_skew_seconds=item["maximum_future_skew_seconds"],
    )
    expected = {
        "freshness_seconds_by_provider": [[provider.casefold(), float(seconds)] for provider, seconds in sorted(policy.freshness_seconds_by_provider, key=lambda pair: pair[0].casefold())],
        "maximum_future_skew_seconds": policy.maximum_future_skew_seconds,
        "maximum_site_coordinate_disagreement_km": policy.maximum_site_coordinate_disagreement_km,
        "minimum_independent_wet_groups": policy.minimum_independent_wet_groups,
        "minimum_interval_overlap_fraction": policy.minimum_interval_overlap_fraction,
        "spatial_tolerance_km": policy.spatial_tolerance_km,
        "temporal_tolerance_seconds": policy.temporal_tolerance_seconds,
    }
    if item != expected:
        raise MetarGroundFactoryError("fusion policy is not canonical")
    return policy


def _dataclass_limits(value: object, cls: type[Any], label: str) -> Any:
    if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
        raise MetarGroundFactoryError(f"{label} schema is not exact")
    try:
        result = cls(**dict(value))
    except (TypeError, ValueError) as exc:
        raise MetarGroundFactoryError(f"{label} is invalid") from exc
    expected = {field: getattr(result, field) for field in cls.__dataclass_fields__}
    if value != expected:
        raise MetarGroundFactoryError(f"{label} mapping is not canonical")
    return result


def _build_runtime(authority: Mapping[str, Any], *, enforce_audit: bool) -> ClosedWorldMetarGroundShadowRuntime:
    paths = _authority_paths(authority)
    config = authority["factory_config"]
    if config["operational_admission_granted"] is not False:
        raise MetarGroundFactoryError("closed-world factory cannot grant operational admission")
    blockers = config["admission_blockers"]
    if not isinstance(blockers, list) or not blockers or blockers != sorted(set(blockers)):
        raise MetarGroundFactoryError("admission blockers are absent or non-canonical")

    raw_role = _selected_path(authority, paths, "raw_store_label")
    raw_manifest_role = _selected_path(authority, paths, "raw_manifest_label")
    site_role = _selected_path(authority, paths, "site_catalog_label")
    semantics_role = _selected_path(authority, paths, "semantics_label")
    footprint_role = _selected_path(authority, paths, "footprint_label")
    canonical_role = _selected_path(authority, paths, "canonical_ground_label")
    receipt_role = _selected_path(authority, paths, "receipt_store_label")
    archive_role = _selected_path(authority, paths, "archive_directory_label")

    for field, role in (
        ("raw_tree_manifest_sha256", raw_manifest_role),
        ("site_catalog_document_sha256", site_role),
        ("semantics_document_sha256", semantics_role),
        ("footprint_document_sha256", footprint_role),
    ):
        if config[field] != role.identity_sha256:
            raise MetarGroundFactoryError("factory document identity differs")

    raw_manifest_document = _load_canonical_document(raw_manifest_role.path, config["raw_tree_manifest_sha256"])
    raw_manifest = _parse_raw_manifest(raw_manifest_document)
    raw_paths = tuple(
        raw_role.path / entry.relative_path for entry in raw_manifest.entries
    )
    guard = None
    if enforce_audit:
        directories = {
            raw_role.path,
            archive_role.path,
            canonical_role.path.parent,
            receipt_role.path.parent,
        }
        for relative in raw_manifest.directories:
            directories.add(raw_role.path / relative)
        for path in raw_paths:
            parent = path.parent
            while parent != raw_role.path.parent:
                directories.add(parent)
                if parent == raw_role.path:
                    break
                parent = parent.parent
        mutable_sidecars = [Path(str(canonical_role.path) + "-journal")]
        mutable_sidecars.extend(
            Path(str(path) + "-journal")
            for path in raw_paths
            if path.suffix.casefold() in {".db", ".sqlite", ".sqlite3"}
        )
        guard = _ClosedWorldAuditGuard(
            exact_files=(raw_manifest_role.path, site_role.path, semantics_role.path, footprint_role.path, canonical_role.path, receipt_role.path, *raw_paths),
            raw_root=raw_role.path,
            receipt_path=receipt_role.path,
            archive_root=archive_role.path,
            directory_paths=tuple(directories),
            mutable_sidecars=tuple(mutable_sidecars),
        )
        guard.admit_raw_files(raw_paths)
        sys.addaudithook(guard)

    raw_files = _verify_raw_tree(raw_role.path, raw_manifest)
    if set(raw_files) != set(raw_paths):
        raise MetarGroundFactoryError("raw manifest path resolution differs")

    canonical_sha, _canonical_size = _hash_exact_file(
        canonical_role.path,
        maximum_bytes=_MAX_RAW_TREE_BYTES,
    )
    if canonical_sha != _sha(config["canonical_content_sha256"], "canonical content"):
        raise MetarGroundFactoryError("canonical ground content checksum differs")

    site_document = _load_canonical_document(site_role.path, config["site_catalog_document_sha256"])
    semantics_document = _load_canonical_document(semantics_role.path, config["semantics_document_sha256"])
    footprint_document = _load_canonical_document(footprint_role.path, config["footprint_document_sha256"])
    scope = _digest({
        "canonical_content_sha256": canonical_sha,
        "capture_id": config["capture_id"],
        "footprint_document_sha256": config["footprint_document_sha256"],
        "ground_evidence_limits_sha256": _digest(
            config["ground_evidence_limits"]
        ),
        "raw_artifact_limits_sha256": _digest(config["raw_artifact_limits"]),
        "raw_tree_manifest_sha256": config["raw_tree_manifest_sha256"],
        "semantics_document_sha256": config["semantics_document_sha256"],
        "site_assertions_sha256": site_document.get("site_assertions_sha256"),
    })
    if scope != _sha(config["authority_scope_sha256"], "authority scope"):
        raise MetarGroundFactoryError("closed-world authority scope differs")

    catalog = _site_catalog(site_document, scope)
    certificates = _semantics(semantics_document)
    bindings = _footprint_bindings(footprint_document)
    policy = _policy(config["fusion_policy"])
    raw_limits = _dataclass_limits(config["raw_artifact_limits"], RawArtifactLimits, "raw artifact limits")
    ground_evidence_limits = _dataclass_limits(config["ground_evidence_limits"], GroundEvidenceLimits, "ground evidence limits")
    ground_limits = GroundFusionShadowLimits(**dict(authority["ground_limits"]))

    raw_store = RawArtifactStore(raw_role.path, limits=raw_limits)
    raw_store.initialize()
    _verify_raw_tree(raw_role.path, raw_manifest)
    capture_id = _text(config["capture_id"], "METAR capture id", maximum=96)
    capture = raw_store.load_capture_receipt(capture_id)
    if (
        capture is None
        or capture.provider != "metar"
        or capture.product != metar_native.METAR_CAPTURE_PRODUCT
        or len(capture.members) != 1
        or capture.members[0].role != "csv_gzip"
    ):
        raise MetarGroundFactoryError("authorised METAR capture differs")
    evidence = CanonicalGroundEvidenceStore(
        canonical_role.path,
        raw_artifact_reader=raw_store,
        limits=ground_evidence_limits,
    )
    if _hash_exact_file(
        canonical_role.path,
        maximum_bytes=_MAX_RAW_TREE_BYTES,
    )[0] != canonical_sha:
        raise MetarGroundFactoryError("canonical ground store mutated during open")
    if ground_evidence_limits.max_query_rows < ground_limits.max_source_facts + 1:
        raise MetarGroundFactoryError("canonical query bound is below runtime scan bound")
    if any(
        certificate.provider != "metar"
        or certificate.product != metar_native.METAR_CAPTURE_PRODUCT
        or certificate.decoder_identity != metar_ground_evidence.DECODER_IDENTITY
        or certificate.decoder_version != metar_ground_evidence.DECODER_VERSION
        or certificate.config_sha256 != metar_ground_evidence.DEFAULT_CONFIG_SHA256
        for certificate in certificates
    ):
        raise MetarGroundFactoryError("METAR semantics certificate binding differs")

    runtime = ClosedWorldMetarGroundShadowRuntime(
        evidence,
        site_catalog=catalog,
        semantics_certificates=certificates,
        fusion_policy=policy,
        receipt_path=receipt_role.path,
        limits=ground_limits,
        footprint_bindings=bindings,
        audit_guard=guard,
        immutable_canonical_path=canonical_role.path,
        immutable_canonical_sha256=canonical_sha,
        immutable_raw_root=raw_role.path,
        immutable_raw_manifest=raw_manifest,
    )
    if guard is not None and (guard.denied_accesses or guard.network_attempts):
        raise MetarGroundFactoryError("closed-world audit invariant failed")
    return runtime


def runtime_profile_for_authority(authority: Mapping[str, Any]) -> str:
    """Derive the exact profile without installing a parent-global audit hook."""

    return _build_runtime(authority, enforce_audit=False).runtime_profile_sha256


def build_closed_world_metar_runtime(authority: Mapping[str, Any]) -> DurableGroundFusionShadowRuntime:
    """Spawn-importable production candidate factory; never service-wired."""

    return _build_runtime(authority, enforce_audit=True)


__all__ = [
    "ClosedWorldMetarGroundShadowRuntime",
    "METAR_CLOSED_WORLD_FACTORY_ID",
    "METAR_FOOTPRINT_AUTHORITY_CONTRACT",
    "METAR_GROUND_FACTORY_CONFIG_CONTRACT",
    "METAR_RAW_TREE_MANIFEST_CONTRACT",
    "METAR_SEMANTICS_AUTHORITY_CONTRACT",
    "METAR_SITE_AUTHORITY_CONTRACT",
    "MetarGroundFactoryError",
    "build_closed_world_metar_runtime",
    "runtime_profile_for_authority",
]
