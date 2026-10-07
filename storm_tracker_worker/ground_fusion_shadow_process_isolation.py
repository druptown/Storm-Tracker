"""Hard parent-enforced process boundary for ground-fusion shadow decisions.

This candidate is deliberately not imported by the worker service, Home
Assistant, MQTT, or an operational publication path.  A spawned child receives
one bounded canonical target/cell command and opens data only through an exact,
checksummed authority document.  Ground observations never cross IPC.

The parent treats the child response as advisory.  After every terminal child
outcome it independently reopens the durable ground receipt store and, when the
hot idempotency window has expired, the sealed archive.  A kill racing an
atomic receipt commit therefore observes either no receipt or one byte-verified
receipt; it never invents a weather result from IPC.
"""
from __future__ import annotations

from dataclasses import dataclass
import errno
import hashlib
import importlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time
from typing import Any, Callable, Iterable, Literal, Mapping

from .ground_fusion_shadow_runtime import (
    GROUND_FUSION_SHADOW_ALGORITHM,
    DurableGroundFusionShadowRuntime,
    GeographicFootprintProof,
    GroundCellTrackBinding,
    GroundFusionShadowArchiveError,
    GroundFusionShadowCapacityError,
    GroundFusionShadowConflict,
    GroundFusionShadowDeadlineError,
    GroundFusionShadowLimits,
    GroundFusionShadowReceipt,
    GroundFusionShadowReceiptStore,
    GroundFusionShadowRequest,
    GroundFusionShadowRetentionPolicy,
    GroundFusionShadowStoreError,
    GroundFusionShadowWindowExpired,
    GroundTargetSnapshot,
    footprint_geometry_sha256,
)
from .hard_decoder_isolation import (
    DecoderEffectContract,
    DecoderProcessResult,
    DecoderResourceLimits,
    HardDecoderExecutor,
    _canonical_json_bytes,
)
from .tracking_core.engine.ground_fusion import CellTrackFootprint


GROUND_SHADOW_PROCESS_AUTHORITY_CONTRACT = (
    "stv3-ground-shadow-process-authority-v1"
)
GROUND_SHADOW_PROCESS_REQUEST_CONTRACT = "stv3-ground-shadow-process-request-v1"
GROUND_SHADOW_PROCESS_CHILD_CONTRACT = (
    "stv3-ground-shadow-process-child-result-v1"
)
GROUND_SHADOW_PROCESS_RECEIPT_METADATA_CONTRACT = (
    "stv3-ground-shadow-process-receipt-metadata-v1"
)
GROUND_SHADOW_PROCESS_RESULT_CONTRACT = (
    "stv3-ground-shadow-process-parent-result-v1"
)
GROUND_SHADOW_PROCESS_EFFECT_CONTRACT = DecoderEffectContract(
    contract_id="ground-shadow.transactional-receipt.v1",
    hard_kill_safe=True,
    durable_effects="transactional",
    spawns_descendants=False,
)

PROCESS_TERMINATION_GRACE_SECONDS = 0.35
HARD_MAX_AUTHORITY_BYTES = 1024 * 1024
HARD_MAX_AUTHORIZED_PATHS = 64
HARD_MAX_PROCESS_REQUEST_BYTES = 512 * 1024
HARD_MAX_PROCESS_RESULT_BYTES = 1024 * 1024
MAX_PROCESS_DIAGNOSTIC_BYTES = 128 * 1024
MAX_CANONICAL_STORE_IDENTITY_BYTES = 512 * 1024
MAX_CANONICAL_STORE_SCHEMA_ROWS = 256
MAX_CANONICAL_STORE_MANIFEST_ROWS = 64

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
_SAFE_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_TARGET_RE = re.compile(
    r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*$",
    re.ASCII,
)
_SECRET_KEY_RE = re.compile(
    r"(?:authorization|api[_-]?key|access[_-]?token|secret|password|credential)",
    re.IGNORECASE,
)


class GroundShadowProcessError(RuntimeError):
    """Base failure at the parent-owned ground process boundary."""


class GroundShadowAuthorityError(GroundShadowProcessError):
    """The exact authority, path, or runtime identity was rejected."""


GroundShadowProcessStatus = Literal[
    "success",
    "deadline",
    "resource",
    "crash",
    "corrupt_contract",
    "cancelled",
    "busy",
    "boundary",
]
DurableDisposition = Literal["committed", "not_committed", "unknown"]


def _require_sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise GroundShadowAuthorityError(f"{label} is not a lowercase SHA-256")
    return value


def _require_safe_id(value: object, label: str) -> str:
    if not isinstance(value, str) or _SAFE_ID_RE.fullmatch(value) is None:
        raise GroundShadowAuthorityError(f"{label} is not a bounded identifier")
    return value


def _require_text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise GroundShadowAuthorityError(f"{label} is not text")
    return value


def _require_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise GroundShadowAuthorityError(f"{label} is not an integer")
    return value


def _require_float(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise GroundShadowAuthorityError(f"{label} is not numeric")
    result = float(value)
    if not math.isfinite(result):
        raise GroundShadowAuthorityError(f"{label} is not finite")
    return result


def _sha_mapping(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _read_regular_file(path: Path, *, maximum_bytes: int) -> bytes:
    if not path.is_absolute() or path.is_symlink():
        raise GroundShadowAuthorityError("authority path is not an absolute regular file")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise GroundShadowAuthorityError("authority file cannot be opened safely") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or not 1 <= before.st_size <= maximum_bytes:
            raise GroundShadowAuthorityError("authority file size/type is invalid")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                raise GroundShadowAuthorityError("authority file was truncated")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise GroundShadowAuthorityError("authority file grew while reading")
        after = os.fstat(descriptor)
        if (
            before.st_dev != after.st_dev
            or before.st_ino != after.st_ino
            or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
        ):
            raise GroundShadowAuthorityError("authority file changed while reading")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _reject_secret_keys(value: Any) -> None:
    stack = [value]
    nodes = 0
    while stack:
        current = stack.pop()
        nodes += 1
        if nodes > 262_144:
            raise GroundShadowAuthorityError("authority JSON node bound exceeded")
        if isinstance(current, dict):
            for key, nested in current.items():
                if not isinstance(key, str) or _SECRET_KEY_RE.search(key):
                    raise GroundShadowAuthorityError(
                        "authority contains a forbidden or non-string key"
                    )
                stack.append(nested)
        elif isinstance(current, list):
            stack.extend(current)


def _iter_absolute_strings(value: Any) -> Iterable[str]:
    stack = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, str) and os.path.isabs(current):
            yield current
        elif isinstance(current, dict):
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)


def _reject_relative_path_strings(value: Any) -> None:
    """Prevent an audited factory config from smuggling cwd-relative paths."""

    stack = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, str):
            if (
                not os.path.isabs(current)
                and not current.startswith(("https://", "http://"))
                and (
                    "/" in current
                    or "\\" in current
                    or current in {".", ".."}
                    or current.startswith(("./", "../", ".\\", "..\\"))
                )
            ):
                raise GroundShadowAuthorityError(
                    "factory config contains a relative path-like string"
                )
        elif isinstance(current, dict):
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)


def canonical_ground_store_identity_sha256(path: str | Path) -> str:
    """Hash only immutable canonical-store schema/manifest, never ground facts."""

    candidate = Path(path)
    try:
        safe_path = (
            candidate.is_absolute()
            and not candidate.is_symlink()
            and candidate.is_file()
            and candidate.resolve(strict=True) == candidate
        )
    except OSError as exc:
        raise GroundShadowAuthorityError(
            "canonical ground database path cannot be resolved"
        ) from exc
    if not safe_path:
        raise GroundShadowAuthorityError("canonical ground database path is unsafe")
    try:
        database = sqlite3.connect(
            candidate.as_uri() + "?mode=ro",
            uri=True,
            timeout=2.0,
        )
        try:
            database.execute("PRAGMA query_only=ON")
            schema = [
                [str(row[0]), str(row[1]), str(row[2] or "")]
                for row in database.execute(
                    "SELECT type,name,sql FROM sqlite_schema "
                    "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
                )
            ]
            manifest = [
                [str(row[0]), str(row[1])]
                for row in database.execute(
                    "SELECT key,value FROM manifest ORDER BY key"
                )
            ]
        finally:
            database.close()
    except sqlite3.DatabaseError as exc:
        raise GroundShadowAuthorityError(
            "canonical ground database identity cannot be verified"
        ) from exc
    if (
        not schema
        or not manifest
        or len(schema) > MAX_CANONICAL_STORE_SCHEMA_ROWS
        or len(manifest) > MAX_CANONICAL_STORE_MANIFEST_ROWS
    ):
        raise GroundShadowAuthorityError("canonical ground identity row bound failed")
    payload = {
        "contract": "stv3-canonical-ground-process-store-identity-v1",
        "manifest": manifest,
        "schema": schema,
    }
    encoded = _canonical_json_bytes(payload)
    if len(encoded) > MAX_CANONICAL_STORE_IDENTITY_BYTES:
        raise GroundShadowAuthorityError("canonical ground identity byte bound failed")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class GroundShadowIsolationLimits:
    """Fixed parent/kernel/IPC bounds for one fresh ground child."""

    wall_seconds: float = 30.0
    cpu_seconds: float = 20.0
    rss_bytes: int = 768 * 1024 * 1024
    max_authority_bytes: int = 256 * 1024
    max_request_bytes: int = 256 * 1024
    max_result_bytes: int = 64 * 1024
    max_authorized_paths: int = 16

    def __post_init__(self) -> None:
        for label, value in (
            ("wall_seconds", self.wall_seconds),
            ("cpu_seconds", self.cpu_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or not 0.05 <= float(value) <= 3_600.0
            ):
                raise ValueError(f"{label} is outside 0.05..3600")
        for label, value, minimum, maximum in (
            ("rss_bytes", self.rss_bytes, 32 * 1024 * 1024, 64 * 1024**3),
            (
                "max_authority_bytes",
                self.max_authority_bytes,
                1024,
                HARD_MAX_AUTHORITY_BYTES,
            ),
            (
                "max_request_bytes",
                self.max_request_bytes,
                4096,
                HARD_MAX_PROCESS_REQUEST_BYTES,
            ),
            (
                "max_result_bytes",
                self.max_result_bytes,
                1024,
                HARD_MAX_PROCESS_RESULT_BYTES,
            ),
            (
                "max_authorized_paths",
                self.max_authorized_paths,
                3,
                HARD_MAX_AUTHORIZED_PATHS,
            ),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise ValueError(f"{label} is outside its hard boundary")

    @property
    def termination_grace_seconds(self) -> float:
        return PROCESS_TERMINATION_GRACE_SECONDS

    def decoder_limits(self) -> DecoderResourceLimits:
        return DecoderResourceLimits(
            wall_seconds=float(self.wall_seconds),
            cpu_seconds=float(self.cpu_seconds),
            memory_bytes=self.rss_bytes,
            max_request_bytes=self.max_request_bytes,
            max_result_bytes=self.max_result_bytes,
        )

    def to_mapping(self) -> dict[str, int | float]:
        return {
            "cpu_seconds": float(self.cpu_seconds),
            "max_authority_bytes": self.max_authority_bytes,
            "max_authorized_paths": self.max_authorized_paths,
            "max_request_bytes": self.max_request_bytes,
            "max_result_bytes": self.max_result_bytes,
            "rss_bytes": self.rss_bytes,
            "termination_grace_seconds": self.termination_grace_seconds,
            "wall_seconds": float(self.wall_seconds),
        }


@dataclass(frozen=True, slots=True)
class GroundShadowFactoryContract:
    """Audited factory claim; no arbitrary factory is selected by command data."""

    factory_id: str
    target: str
    opens_only_authorized_paths: bool
    canonical_raw_reverification: bool
    hard_kill_safe: bool
    transactional_receipt_only: bool
    spawns_descendants: bool = False

    def __post_init__(self) -> None:
        _require_safe_id(self.factory_id, "factory_id")
        if not isinstance(self.target, str) or _TARGET_RE.fullmatch(self.target) is None:
            raise ValueError("factory target is not a bounded import reference")
        if self.target.startswith("__") or self.target.split(":", 1)[1].startswith("__"):
            raise ValueError("dunder factory targets are forbidden")
        for label in (
            "opens_only_authorized_paths",
            "canonical_raw_reverification",
            "hard_kill_safe",
            "transactional_receipt_only",
            "spawns_descendants",
        ):
            if type(getattr(self, label)) is not bool:
                raise TypeError(f"{label} must be boolean")

    @property
    def admitted(self) -> bool:
        return (
            self.opens_only_authorized_paths
            and self.canonical_raw_reverification
            and self.hard_kill_safe
            and self.transactional_receipt_only
            and not self.spawns_descendants
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "canonical_raw_reverification": self.canonical_raw_reverification,
            "factory_id": self.factory_id,
            "hard_kill_safe": self.hard_kill_safe,
            "opens_only_authorized_paths": self.opens_only_authorized_paths,
            "spawns_descendants": self.spawns_descendants,
            "target": self.target,
            "transactional_receipt_only": self.transactional_receipt_only,
        }

    @property
    def fingerprint(self) -> str:
        return _sha_mapping(self.to_mapping())

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "GroundShadowFactoryContract":
        if set(value) != {
            "canonical_raw_reverification",
            "factory_id",
            "hard_kill_safe",
            "opens_only_authorized_paths",
            "spawns_descendants",
            "target",
            "transactional_receipt_only",
        }:
            raise GroundShadowAuthorityError("factory contract fields are not exact")
        try:
            return cls(
                factory_id=value["factory_id"],
                target=value["target"],
                opens_only_authorized_paths=value["opens_only_authorized_paths"],
                canonical_raw_reverification=value["canonical_raw_reverification"],
                hard_kill_safe=value["hard_kill_safe"],
                transactional_receipt_only=value["transactional_receipt_only"],
                spawns_descendants=value["spawns_descendants"],
            )
        except (TypeError, ValueError) as exc:
            raise GroundShadowAuthorityError("factory contract is invalid") from exc


@dataclass(frozen=True, slots=True, repr=False)
class _LoadedAuthority:
    document: dict[str, Any]
    authority_sha256: str
    runtime_profile_sha256: str
    ground_limits: GroundFusionShadowLimits
    retention_policy: GroundFusionShadowRetentionPolicy
    canonical_ground_path: Path
    receipt_store_path: Path
    archive_directory_path: Path


def _validate_authorized_path(item: Mapping[str, Any]) -> tuple[str, Path, str]:
    if set(item) != {"access", "identity_sha256", "label", "path"}:
        raise GroundShadowAuthorityError("authorized path fields are not exact")
    label = _require_safe_id(item["label"], "authorized path label")
    access = item["access"]
    if access not in {
        "canonical_ground_database",
        "ground_receipt_database",
        "ground_archive_directory",
        "read_file",
        "read_directory",
    }:
        raise GroundShadowAuthorityError("authorized path access is invalid")
    raw_path = item["path"]
    if not isinstance(raw_path, str) or "\0" in raw_path or len(raw_path) > 1024:
        raise GroundShadowAuthorityError("authorized path text is invalid")
    path = Path(raw_path)
    try:
        present = path.exists()
        symlink = path.is_symlink()
        resolved = path.resolve(strict=True) if present and not symlink else None
    except OSError as exc:
        raise GroundShadowAuthorityError(
            "authorized path cannot be resolved safely"
        ) from exc
    if not path.is_absolute() or symlink or not present:
        raise GroundShadowAuthorityError("authorized path is absent, relative or a symlink")
    if resolved != path:
        raise GroundShadowAuthorityError("authorized path is not already normalized")
    identity = item["identity_sha256"]
    if access == "canonical_ground_database":
        expected = _require_sha(identity, "canonical ground store identity")
        if canonical_ground_store_identity_sha256(path) != expected:
            raise GroundShadowAuthorityError("canonical ground store identity differs")
    elif access == "read_file":
        expected = _require_sha(identity, "authorized read file identity")
        body = _read_regular_file(path, maximum_bytes=HARD_MAX_AUTHORITY_BYTES)
        if hashlib.sha256(body).hexdigest() != expected:
            raise GroundShadowAuthorityError("authorized read file checksum differs")
    elif identity is not None:
        raise GroundShadowAuthorityError("mutable/directory path cannot claim content SHA")
    if access.endswith("database") and not path.is_file():
        raise GroundShadowAuthorityError("authorized database is not a file")
    if access.endswith("directory") and not path.is_dir():
        raise GroundShadowAuthorityError("authorized directory is not a directory")
    return label, path, str(access)


def _load_authority(
    authority_path: Path,
    expected_authority_sha256: str,
    factory_contract: GroundShadowFactoryContract,
    isolation_limits: GroundShadowIsolationLimits,
) -> _LoadedAuthority:
    expected_sha = _require_sha(expected_authority_sha256, "authority SHA")
    if not factory_contract.admitted:
        raise GroundShadowAuthorityError("factory kill/path contract is not admitted")
    body = _read_regular_file(
        authority_path,
        maximum_bytes=isolation_limits.max_authority_bytes,
    )
    if hashlib.sha256(body).hexdigest() != expected_sha:
        raise GroundShadowAuthorityError("authority SHA is stale or belongs to another file")
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GroundShadowAuthorityError("authority is not JSON") from exc
    if not isinstance(value, dict) or _canonical_json_bytes(value) != body:
        raise GroundShadowAuthorityError("authority is not canonical JSON")
    if set(value) != {
        "archive_directory_label",
        "authorized_paths",
        "canonical_ground_label",
        "contract",
        "factory_config",
        "factory_contract_sha256",
        "factory_id",
        "ground_limits",
        "isolation_limits",
        "receipt_store_label",
        "retention_policy",
        "runtime_profile_sha256",
    } or value.get("contract") != GROUND_SHADOW_PROCESS_AUTHORITY_CONTRACT:
        raise GroundShadowAuthorityError("authority schema is not exact")
    _reject_secret_keys(value)
    if (
        value["factory_id"] != factory_contract.factory_id
        or value["factory_contract_sha256"] != factory_contract.fingerprint
        or value["isolation_limits"] != isolation_limits.to_mapping()
    ):
        raise GroundShadowAuthorityError("authority factory/isolation binding differs")
    paths_value = value["authorized_paths"]
    if (
        not isinstance(paths_value, list)
        or not 3 <= len(paths_value) <= isolation_limits.max_authorized_paths
    ):
        raise GroundShadowAuthorityError("authorized path count is invalid")
    authorized: dict[str, tuple[Path, str]] = {}
    resolved_paths: set[Path] = set()
    for item in paths_value:
        if not isinstance(item, dict):
            raise GroundShadowAuthorityError("authorized path record is invalid")
        label, path, access = _validate_authorized_path(item)
        if label in authorized or path in resolved_paths:
            raise GroundShadowAuthorityError("authorized paths are duplicated")
        authorized[label] = (path, access)
        resolved_paths.add(path)
    core = {
        "canonical_ground_label": "canonical_ground_database",
        "receipt_store_label": "ground_receipt_database",
        "archive_directory_label": "ground_archive_directory",
    }
    selected: dict[str, Path] = {}
    for field, required_access in core.items():
        label = _require_safe_id(value[field], field)
        if label not in authorized or authorized[label][1] != required_access:
            raise GroundShadowAuthorityError("core authority path/access differs")
        selected[field] = authorized[label][0]
    config = value["factory_config"]
    if not isinstance(config, dict):
        raise GroundShadowAuthorityError("factory config must be an object")
    _reject_relative_path_strings(config)
    for absolute in _iter_absolute_strings(config):
        if Path(absolute) not in resolved_paths:
            raise GroundShadowAuthorityError(
                "factory config references an unauthorized absolute path"
            )
    try:
        ground_limits = GroundFusionShadowLimits(**dict(value["ground_limits"]))
        retention = GroundFusionShadowRetentionPolicy(
            **dict(value["retention_policy"])
        )
    except (TypeError, ValueError) as exc:
        raise GroundShadowAuthorityError("ground limits or retention policy are invalid") from exc
    if (
        ground_limits.to_mapping() != value["ground_limits"]
        or retention.to_mapping() != value["retention_policy"]
        or retention.hot_receipts_to_keep >= ground_limits.max_receipts
    ):
        raise GroundShadowAuthorityError("ground limits/policy mapping is not exact")
    return _LoadedAuthority(
        document=value,
        authority_sha256=expected_sha,
        runtime_profile_sha256=_require_sha(
            value["runtime_profile_sha256"],
            "authority runtime profile",
        ),
        ground_limits=ground_limits,
        retention_policy=retention,
        canonical_ground_path=selected["canonical_ground_label"],
        receipt_store_path=selected["receipt_store_label"],
        archive_directory_path=selected["archive_directory_label"],
    )


def _resolve_factory(
    contract: GroundShadowFactoryContract,
) -> Callable[[Mapping[str, Any]], Any]:
    module_name, function_name = contract.target.split(":", 1)
    function = getattr(importlib.import_module(module_name), function_name)
    if not callable(function):
        raise GroundShadowAuthorityError("factory target is not callable")
    return function


def _footprint_mapping(value: CellTrackFootprint) -> dict[str, Any]:
    return {
        "cell_detection_id": value.cell_detection_id,
        "cell_track_id": value.cell_track_id,
        "footprint_id": value.footprint_id,
        "geometry": [
            [[list(point) for point in ring] for ring in polygon]
            for polygon in value.geometry
        ],
        "radar_source": value.radar_source,
        "raw_artifact_receipt_id": value.raw_artifact_receipt_id,
        "representative_at": float(value.representative_at),
        "valid_from": float(value.valid_from),
        "valid_until": float(value.valid_until),
    }


def ground_shadow_request_mapping(value: GroundFusionShadowRequest) -> dict[str, Any]:
    """Canonical bounded command mapping; contains no ground observations."""

    if not isinstance(value, GroundFusionShadowRequest):
        raise TypeError("request must be GroundFusionShadowRequest")
    cell = value.cell
    proof = cell.footprint_proof
    return {
        "attempt_id": value.attempt_id,
        "cell": {
            "analysis_grid_id": cell.analysis_grid_id,
            "applied_lineage_hypothesis_ids": list(
                cell.applied_lineage_hypothesis_ids
            ),
            "child_track_ids": list(cell.child_track_ids),
            "engine_id": cell.engine_id,
            "engine_state_revision": cell.engine_state_revision,
            "footprint": _footprint_mapping(cell.footprint),
            "footprint_proof": {
                field: getattr(proof, field)
                for field in proof.__dataclass_fields__
            },
            "lineage_event_ids": list(cell.lineage_event_ids),
            "parent_track_ids": list(cell.parent_track_ids),
            "radar_semantics_certificate_sha256": (
                cell.radar_semantics_certificate_sha256
            ),
            "science_receipt_sha256": cell.science_receipt_sha256,
            "science_run_id": cell.science_run_id,
            "science_transition_id": cell.science_transition_id,
            "source_frame_id": cell.source_frame_id,
            "target_grid_sha256": cell.target_grid_sha256,
            "track_revision": cell.track_revision,
            "transform_certificate_sha256": (
                cell.transform_certificate_sha256
            ),
        },
        "decision_key": value.decision_key,
        "evaluated_at": float(value.evaluated_at),
        "region_key": value.region_key,
        "target": {
            "latitude": float(value.target.latitude),
            "longitude": float(value.target.longitude),
            "observed_at": float(value.target.observed_at),
            "source_state_sha256": value.target.source_state_sha256,
            "target_id": value.target.target_id,
            "target_revision": value.target.target_revision,
        },
    }


def _string_tuple(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise GroundShadowAuthorityError(f"{label} is not a string list")
    return tuple(value)


def _request_from_mapping(value: Any) -> GroundFusionShadowRequest:
    if not isinstance(value, dict) or set(value) != {
        "attempt_id",
        "cell",
        "decision_key",
        "evaluated_at",
        "region_key",
        "target",
    }:
        raise GroundShadowAuthorityError("ground command fields are not exact")
    target = value["target"]
    if not isinstance(target, dict) or set(target) != {
        "latitude",
        "longitude",
        "observed_at",
        "source_state_sha256",
        "target_id",
        "target_revision",
    }:
        raise GroundShadowAuthorityError("target command fields are not exact")
    cell = value["cell"]
    if not isinstance(cell, dict) or set(cell) != {
        "analysis_grid_id",
        "applied_lineage_hypothesis_ids",
        "child_track_ids",
        "engine_id",
        "engine_state_revision",
        "footprint",
        "footprint_proof",
        "lineage_event_ids",
        "parent_track_ids",
        "radar_semantics_certificate_sha256",
        "science_receipt_sha256",
        "science_run_id",
        "science_transition_id",
        "source_frame_id",
        "target_grid_sha256",
        "track_revision",
        "transform_certificate_sha256",
    }:
        raise GroundShadowAuthorityError("cell command fields are not exact")
    footprint = cell["footprint"]
    if not isinstance(footprint, dict) or set(footprint) != {
        "cell_detection_id",
        "cell_track_id",
        "footprint_id",
        "geometry",
        "radar_source",
        "raw_artifact_receipt_id",
        "representative_at",
        "valid_from",
        "valid_until",
    }:
        raise GroundShadowAuthorityError("footprint command fields are not exact")
    geometry = footprint["geometry"]
    if not isinstance(geometry, list):
        raise GroundShadowAuthorityError("footprint geometry is not a list")
    polygons = []
    for polygon in geometry:
        if not isinstance(polygon, list):
            raise GroundShadowAuthorityError("footprint polygon is not a list")
        rings = []
        for ring in polygon:
            if not isinstance(ring, list):
                raise GroundShadowAuthorityError("footprint ring is not a list")
            points = []
            for point in ring:
                if not isinstance(point, list) or len(point) != 2:
                    raise GroundShadowAuthorityError("footprint point is invalid")
                points.append((
                    _require_float(point[0], "footprint latitude"),
                    _require_float(point[1], "footprint longitude"),
                ))
            rings.append(tuple(points))
        polygons.append(tuple(rings))
    raw_receipt = footprint["raw_artifact_receipt_id"]
    if raw_receipt is not None and not isinstance(raw_receipt, str):
        raise GroundShadowAuthorityError("raw artifact receipt ID is invalid")
    footprint_value = CellTrackFootprint(
        footprint_id=_require_text(footprint["footprint_id"], "footprint_id"),
        cell_track_id=_require_text(footprint["cell_track_id"], "cell_track_id"),
        cell_detection_id=_require_text(
            footprint["cell_detection_id"], "cell_detection_id"
        ),
        valid_from=_require_float(footprint["valid_from"], "valid_from"),
        valid_until=_require_float(footprint["valid_until"], "valid_until"),
        representative_at=_require_float(
            footprint["representative_at"], "representative_at"
        ),
        rings=(),
        radar_source=_require_text(footprint["radar_source"], "radar_source"),
        raw_artifact_receipt_id=raw_receipt,
        polygons=tuple(polygons),
    )
    proof = cell["footprint_proof"]
    proof_fields = set(GeographicFootprintProof.__dataclass_fields__)
    if not isinstance(proof, dict) or set(proof) != proof_fields:
        raise GroundShadowAuthorityError("footprint proof fields are not exact")
    proof_value = GeographicFootprintProof(
        **{field: _require_text(proof[field], field) for field in proof_fields}
    )
    request = GroundFusionShadowRequest(
        attempt_id=_require_text(value["attempt_id"], "attempt_id"),
        decision_key=_require_text(value["decision_key"], "decision_key"),
        region_key=_require_text(value["region_key"], "region_key"),
        target=GroundTargetSnapshot(
            target_id=_require_text(target["target_id"], "target_id"),
            target_revision=_require_text(
                target["target_revision"], "target_revision"
            ),
            latitude=_require_float(target["latitude"], "target latitude"),
            longitude=_require_float(target["longitude"], "target longitude"),
            observed_at=_require_float(target["observed_at"], "target observed_at"),
            source_state_sha256=_require_text(
                target["source_state_sha256"], "source_state_sha256"
            ),
        ),
        cell=GroundCellTrackBinding(
            footprint=footprint_value,
            engine_id=_require_text(cell["engine_id"], "engine_id"),
            engine_state_revision=_require_int(
                cell["engine_state_revision"], "engine_state_revision"
            ),
            track_revision=_require_int(cell["track_revision"], "track_revision"),
            analysis_grid_id=_require_text(
                cell["analysis_grid_id"], "analysis_grid_id"
            ),
            science_run_id=_require_text(cell["science_run_id"], "science_run_id"),
            science_transition_id=_require_text(
                cell["science_transition_id"], "science_transition_id"
            ),
            science_receipt_sha256=_require_text(
                cell["science_receipt_sha256"], "science_receipt_sha256"
            ),
            source_frame_id=_require_text(cell["source_frame_id"], "source_frame_id"),
            target_grid_sha256=_require_text(
                cell["target_grid_sha256"], "target_grid_sha256"
            ),
            radar_semantics_certificate_sha256=_require_text(
                cell["radar_semantics_certificate_sha256"],
                "radar_semantics_certificate_sha256",
            ),
            transform_certificate_sha256=_require_text(
                cell["transform_certificate_sha256"],
                "transform_certificate_sha256",
            ),
            parent_track_ids=_string_tuple(
                cell["parent_track_ids"], "parent_track_ids"
            ),
            child_track_ids=_string_tuple(
                cell["child_track_ids"], "child_track_ids"
            ),
            lineage_event_ids=_string_tuple(
                cell["lineage_event_ids"], "lineage_event_ids"
            ),
            applied_lineage_hypothesis_ids=_string_tuple(
                cell["applied_lineage_hypothesis_ids"],
                "applied_lineage_hypothesis_ids",
            ),
            footprint_proof=proof_value,
        ),
        evaluated_at=_require_float(value["evaluated_at"], "evaluated_at"),
    )
    if ground_shadow_request_mapping(request) != value:
        raise GroundShadowAuthorityError("ground command is not exactly canonical")
    return request


def ground_shadow_request_sha256(
    request: GroundFusionShadowRequest,
    *,
    runtime_profile_sha256: str,
) -> str:
    profile = _require_sha(runtime_profile_sha256, "runtime profile")
    return _sha_mapping({
        "algorithm": GROUND_FUSION_SHADOW_ALGORITHM,
        "cell_binding_sha256": request.cell.fingerprint,
        "decision_key_sha256": request.decision_key_sha256,
        "evaluated_at": request.evaluated_at,
        "region_key_sha256": request.region_key_sha256,
        "runtime_profile_sha256": profile,
        "target_snapshot_sha256": request.target.fingerprint,
    })


def _command_sha256(
    *,
    authority_sha256: str,
    request_mapping: Mapping[str, Any],
    request_sha256: str,
    runtime_profile_sha256: str,
) -> str:
    return _sha_mapping({
        "authority_sha256": authority_sha256,
        "request": dict(request_mapping),
        "request_sha256": request_sha256,
        "runtime_profile_sha256": runtime_profile_sha256,
    })


def _ground_receipt_metadata(
    receipt: GroundFusionShadowReceipt,
    *,
    authority_sha256: str,
) -> dict[str, Any]:
    payload = {
        "accounting_counts": receipt.accounting.counts,
        "accounting_full_scan_complete": receipt.accounting.full_scan_complete,
        "accounting_overflowed": receipt.accounting.overflowed,
        "accounting_source_fact_count": receipt.accounting.source_fact_count,
        "admission_status": receipt.admission_status,
        "authority_sha256": authority_sha256,
        "candidate_confirmed": receipt.candidate_confirmed,
        "candidate_state": receipt.candidate_state.value,
        "cell_binding_sha256": receipt.cell_binding_sha256,
        "contract": GROUND_SHADOW_PROCESS_RECEIPT_METADATA_CONTRACT,
        "decision_key_sha256": receipt.decision_key_sha256,
        "meteorological_admission_granted": (
            receipt.meteorological_admission_granted
        ),
        "operational_confirmation_claim": receipt.operational_confirmation_claim,
        "operational_snapshot_claim_generated": (
            receipt.operational_snapshot_claim_generated
        ),
        "receipt_sha256": receipt.receipt_sha256,
        "request_sha256": receipt.request_sha256,
        "runtime_profile_sha256": receipt.runtime_profile_sha256,
        "target_id_sha256": receipt.target_id_sha256,
    }
    result = dict(payload)
    result["metadata_sha256"] = _sha_mapping(payload)
    if len(_canonical_json_bytes(result)) > MAX_PROCESS_DIAGNOSTIC_BYTES:
        raise GroundShadowProcessError("receipt metadata exceeds diagnostic bound")
    return result


def _validate_receipt_metadata(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GroundShadowProcessError("child receipt metadata is not an object")
    expected = {
        "accounting_counts",
        "accounting_full_scan_complete",
        "accounting_overflowed",
        "accounting_source_fact_count",
        "admission_status",
        "authority_sha256",
        "candidate_confirmed",
        "candidate_state",
        "cell_binding_sha256",
        "contract",
        "decision_key_sha256",
        "metadata_sha256",
        "meteorological_admission_granted",
        "operational_confirmation_claim",
        "operational_snapshot_claim_generated",
        "receipt_sha256",
        "request_sha256",
        "runtime_profile_sha256",
        "target_id_sha256",
    }
    if (
        set(value) != expected
        or value.get("contract") != GROUND_SHADOW_PROCESS_RECEIPT_METADATA_CONTRACT
    ):
        raise GroundShadowProcessError("child receipt metadata schema differs")
    payload = dict(value)
    metadata_sha = payload.pop("metadata_sha256")
    if _require_sha(metadata_sha, "metadata SHA") != _sha_mapping(payload):
        raise GroundShadowProcessError("child receipt metadata checksum differs")
    if any(
        key in value
        for key in ("path", "station_id", "provider_station_id", "coordinates")
    ):
        raise GroundShadowProcessError("child metadata contains private diagnostics")
    if len(_canonical_json_bytes(value)) > MAX_PROCESS_DIAGNOSTIC_BYTES:
        raise GroundShadowProcessError("child receipt metadata is oversized")
    return value


def _child_envelope(
    *,
    status: Literal[
        "success",
        "deadline",
        "resource",
        "crash",
        "corrupt_contract",
        "window_expired",
    ],
    authority_sha256: str,
    code: str,
    receipt_metadata: Mapping[str, Any] | None = None,
    duplicate: bool | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "authority_sha256": authority_sha256,
        "code": code,
        "contract": GROUND_SHADOW_PROCESS_CHILD_CONTRACT,
        "status": status,
    }
    if status == "success":
        result["duplicate"] = duplicate
        result["receipt_metadata"] = dict(receipt_metadata or {})
    return result


def _ground_shadow_child_entry(request: Mapping[str, Any]) -> dict[str, Any]:
    """Spawn target; exception text, paths, station IDs, and facts never return."""

    authority_sha = "0" * 64
    try:
        if not isinstance(request, dict) or set(request) != {
            "authority_path",
            "authority_sha256",
            "command_sha256",
            "contract",
            "factory_contract",
            "factory_contract_sha256",
            "ground_request",
            "ground_request_sha256",
            "isolation_limits",
        } or request.get("contract") != GROUND_SHADOW_PROCESS_REQUEST_CONTRACT:
            raise GroundShadowAuthorityError("process request schema differs")
        authority_sha = _require_sha(request["authority_sha256"], "authority SHA")
        factory_value = request["factory_contract"]
        if not isinstance(factory_value, dict):
            raise GroundShadowAuthorityError("factory contract is absent")
        factory_contract = GroundShadowFactoryContract.from_mapping(factory_value)
        if factory_contract.fingerprint != _require_sha(
            request["factory_contract_sha256"], "factory contract SHA"
        ):
            raise GroundShadowAuthorityError("factory contract checksum differs")
        isolation_value = request["isolation_limits"]
        if not isinstance(isolation_value, dict) or set(isolation_value) != {
            "cpu_seconds",
            "max_authority_bytes",
            "max_authorized_paths",
            "max_request_bytes",
            "max_result_bytes",
            "rss_bytes",
            "termination_grace_seconds",
            "wall_seconds",
        } or isolation_value.get(
            "termination_grace_seconds"
        ) != PROCESS_TERMINATION_GRACE_SECONDS:
            raise GroundShadowAuthorityError("isolation limits schema differs")
        isolation = GroundShadowIsolationLimits(
            wall_seconds=isolation_value["wall_seconds"],
            cpu_seconds=isolation_value["cpu_seconds"],
            rss_bytes=isolation_value["rss_bytes"],
            max_authority_bytes=isolation_value["max_authority_bytes"],
            max_request_bytes=isolation_value["max_request_bytes"],
            max_result_bytes=isolation_value["max_result_bytes"],
            max_authorized_paths=isolation_value["max_authorized_paths"],
        )
        if isolation.to_mapping() != isolation_value:
            raise GroundShadowAuthorityError("isolation limits mapping differs")
        authority = _load_authority(
            Path(_require_text(request["authority_path"], "authority_path")),
            authority_sha,
            factory_contract,
            isolation,
        )
        command_value = request["ground_request"]
        ground_request = _request_from_mapping(command_value)
        request_sha = ground_shadow_request_sha256(
            ground_request,
            runtime_profile_sha256=authority.runtime_profile_sha256,
        )
        if request_sha != _require_sha(
            request["ground_request_sha256"], "ground request SHA"
        ) or _command_sha256(
            authority_sha256=authority_sha,
            request_mapping=command_value,
            request_sha256=request_sha,
            runtime_profile_sha256=authority.runtime_profile_sha256,
        ) != _require_sha(request["command_sha256"], "command SHA"):
            raise GroundShadowAuthorityError("ground command checksum differs")
        factory = _resolve_factory(factory_contract)
        runtime = factory(authority.document)
        if not isinstance(runtime, DurableGroundFusionShadowRuntime):
            raise GroundShadowAuthorityError("factory returned another runtime type")
        evidence_path = getattr(runtime._evidence_store, "path", None)
        if (
            runtime.runtime_profile_sha256 != authority.runtime_profile_sha256
            or runtime.receipt_store.path != authority.receipt_store_path
            or evidence_path != authority.canonical_ground_path
            or runtime._limits.to_mapping() != authority.ground_limits.to_mapping()
            or runtime._request_sha256(ground_request) != request_sha
        ):
            raise GroundShadowAuthorityError("factory runtime/store binding differs")
        deadline = time.monotonic() + min(
            float(isolation.wall_seconds),
            float(runtime._limits.max_runtime_seconds),
        )
        result = runtime.process(ground_request, deadline_at=deadline)
        metadata = _ground_receipt_metadata(
            result.receipt,
            authority_sha256=authority_sha,
        )
        return _child_envelope(
            status="success",
            authority_sha256=authority_sha,
            code="receipt_committed_or_exact_duplicate",
            receipt_metadata=metadata,
            duplicate=result.duplicate,
        )
    except GroundFusionShadowWindowExpired:
        return _child_envelope(
            status="window_expired",
            authority_sha256=authority_sha,
            code="durable_window_expired",
        )
    except GroundFusionShadowDeadlineError:
        return _child_envelope(
            status="deadline",
            authority_sha256=authority_sha,
            code="ground_runtime_deadline",
        )
    except GroundFusionShadowCapacityError:
        return _child_envelope(
            status="resource",
            authority_sha256=authority_sha,
            code="ground_runtime_capacity",
        )
    except (
        GroundShadowAuthorityError,
        GroundFusionShadowConflict,
        TypeError,
        ValueError,
    ):
        return _child_envelope(
            status="corrupt_contract",
            authority_sha256=authority_sha,
            code="authority_or_request_invalid",
        )
    except (GroundFusionShadowArchiveError, GroundFusionShadowStoreError):
        return _child_envelope(
            status="corrupt_contract",
            authority_sha256=authority_sha,
            code="durable_store_contract_failed",
        )
    except MemoryError:
        raise
    except OSError as exc:
        if exc.errno == errno.ENOMEM:
            raise
        return _child_envelope(
            status="crash",
            authority_sha256=authority_sha,
            code="ground_child_failed",
        )
    except Exception:
        return _child_envelope(
            status="crash",
            authority_sha256=authority_sha,
            code="ground_child_failed",
        )


@dataclass(frozen=True, slots=True)
class GroundShadowProcessResult:
    """Secret/path/station-free parent result; never an operational claim."""

    status: GroundShadowProcessStatus
    execution_outcome: str
    detail_code: str
    authority_sha256: str
    durable_disposition: DurableDisposition
    receipt_metadata: Mapping[str, Any] | None
    duplicate: bool | None
    isolation_enforced: bool
    child_exit_code: int | None
    elapsed_seconds: float
    meteorological_admission_granted: bool = False
    operational_confirmation_claim: bool = False
    operational_snapshot_claim_generated: bool = False
    contract: str = GROUND_SHADOW_PROCESS_RESULT_CONTRACT

    def __post_init__(self) -> None:
        if self.status not in {
            "success",
            "deadline",
            "resource",
            "crash",
            "corrupt_contract",
            "cancelled",
            "busy",
            "boundary",
        }:
            raise ValueError("unknown ground process terminal status")
        if not isinstance(self.execution_outcome, str) or not self.execution_outcome:
            raise ValueError("execution outcome is invalid")
        if not isinstance(self.detail_code, str) or _SAFE_CODE_RE.fullmatch(
            self.detail_code
        ) is None:
            raise ValueError("detail code is invalid")
        _require_sha(self.authority_sha256, "result authority SHA")
        if self.durable_disposition not in {
            "committed",
            "not_committed",
            "unknown",
        }:
            raise ValueError("durable disposition is invalid")
        if self.receipt_metadata is not None:
            _validate_receipt_metadata(dict(self.receipt_metadata))
        if self.durable_disposition == "committed" and self.receipt_metadata is None:
            raise ValueError("committed result requires receipt metadata")
        if self.duplicate is not None and type(self.duplicate) is not bool:
            raise TypeError("duplicate must be bool or None")
        if type(self.isolation_enforced) is not bool:
            raise TypeError("isolation_enforced must be boolean")
        if self.child_exit_code is not None and not isinstance(self.child_exit_code, int):
            raise TypeError("child exit code is invalid")
        if not math.isfinite(self.elapsed_seconds) or self.elapsed_seconds < 0.0:
            raise ValueError("elapsed_seconds is invalid")
        if (
            self.meteorological_admission_granted
            or self.operational_confirmation_claim
            or self.operational_snapshot_claim_generated
            or self.contract != GROUND_SHADOW_PROCESS_RESULT_CONTRACT
        ):
            raise ValueError("process result contains a forbidden operational claim")
        if len(_canonical_json_bytes(self.to_mapping())) > MAX_PROCESS_DIAGNOSTIC_BYTES:
            raise ValueError("process result diagnostics exceed bound")

    @property
    def committed(self) -> bool:
        return self.durable_disposition == "committed"

    @property
    def succeeded(self) -> bool:
        return self.status == "success" and self.committed

    def to_mapping(self) -> dict[str, Any]:
        return {
            "authority_sha256": self.authority_sha256,
            "child_exit_code": self.child_exit_code,
            "contract": self.contract,
            "detail_code": self.detail_code,
            "duplicate": self.duplicate,
            "durable_disposition": self.durable_disposition,
            "elapsed_seconds": self.elapsed_seconds,
            "execution_outcome": self.execution_outcome,
            "isolation_enforced": self.isolation_enforced,
            "meteorological_admission_granted": (
                self.meteorological_admission_granted
            ),
            "operational_confirmation_claim": self.operational_confirmation_claim,
            "operational_snapshot_claim_generated": (
                self.operational_snapshot_claim_generated
            ),
            "receipt_metadata": (
                None if self.receipt_metadata is None else dict(self.receipt_metadata)
            ),
            "status": self.status,
        }


def _status_for_decoder(result: DecoderProcessResult) -> GroundShadowProcessStatus:
    if result.outcome == "succeeded":
        return "corrupt_contract"
    if result.outcome == "deadline_exceeded":
        return "deadline"
    if result.outcome in {
        "cpu_limit_exceeded",
        "memory_limit_exceeded",
        "result_too_large",
    }:
        return "resource"
    if result.outcome == "cancelled":
        return "cancelled"
    if result.outcome == "busy":
        return "busy"
    if result.outcome in {
        "unsupported_platform",
        "boundary_setup_failed",
        "unsafe_effect_contract",
    }:
        return "boundary"
    if result.outcome in {
        "invalid_request",
        "result_not_json",
        "protocol_error",
        "target_load_failed",
    }:
        return "corrupt_contract"
    return "crash"


class GroundShadowIsolatedRunner:
    """Parent-owned one-shot ground runner with durable reconciliation."""

    def __init__(
        self,
        *,
        authority_path: str | Path,
        expected_authority_sha256: str,
        factory_contract: GroundShadowFactoryContract,
        limits: GroundShadowIsolationLimits = GroundShadowIsolationLimits(),
        context: multiprocessing.context.BaseContext | None = None,
    ) -> None:
        path = Path(authority_path)
        if not path.is_absolute():
            raise ValueError("authority_path must be absolute")
        self._authority_path = path
        self._expected_authority_sha256 = _require_sha(
            expected_authority_sha256,
            "expected authority SHA",
        )
        if not isinstance(factory_contract, GroundShadowFactoryContract):
            raise TypeError("factory_contract has wrong type")
        if not factory_contract.admitted:
            raise ValueError("factory contract is not hard-kill/path safe")
        if not isinstance(limits, GroundShadowIsolationLimits):
            raise TypeError("limits has wrong type")
        self._factory_contract = factory_contract
        self._limits = limits
        self._executor = HardDecoderExecutor(
            limits=limits.decoder_limits(),
            context=context,
        )

    def _terminal(
        self,
        *,
        status: GroundShadowProcessStatus,
        execution_outcome: str,
        detail_code: str,
        started: float,
        durable_disposition: DurableDisposition = "not_committed",
        receipt_metadata: Mapping[str, Any] | None = None,
        duplicate: bool | None = None,
        isolation_enforced: bool = False,
        child_exit_code: int | None = None,
        elapsed_seconds: float | None = None,
    ) -> GroundShadowProcessResult:
        return GroundShadowProcessResult(
            status=status,
            execution_outcome=execution_outcome,
            detail_code=detail_code,
            authority_sha256=self._expected_authority_sha256,
            durable_disposition=durable_disposition,
            receipt_metadata=receipt_metadata,
            duplicate=duplicate,
            isolation_enforced=isolation_enforced,
            child_exit_code=child_exit_code,
            elapsed_seconds=(
                max(0.0, time.monotonic() - started)
                if elapsed_seconds is None
                else elapsed_seconds
            ),
        )

    @staticmethod
    def _load_receipt(
        authority: _LoadedAuthority,
        *,
        request: GroundFusionShadowRequest,
        request_sha256: str,
    ) -> GroundFusionShadowReceipt | None:
        store = GroundFusionShadowReceiptStore(
            authority.receipt_store_path,
            runtime_profile_sha256=authority.runtime_profile_sha256,
            limits=authority.ground_limits,
        )
        store.initialize()
        try:
            return store.load_existing(
                attempt_id=request.attempt_id,
                request_sha256=request_sha256,
            )
        except GroundFusionShadowWindowExpired:
            receipt = store.load_archived(
                authority.archive_directory_path,
                policy=authority.retention_policy,
                request_sha256=request_sha256,
            )
            if receipt is None:
                raise GroundFusionShadowArchiveError(
                    "window-expired index has no sealed receipt"
                )
            return receipt

    def execute(
        self,
        request: GroundFusionShadowRequest,
        *,
        cancel_event: threading.Event | None = None,
    ) -> GroundShadowProcessResult:
        started = time.monotonic()
        try:
            if not isinstance(request, GroundFusionShadowRequest):
                raise GroundShadowAuthorityError("ground request type is invalid")
            authority = _load_authority(
                self._authority_path,
                self._expected_authority_sha256,
                self._factory_contract,
                self._limits,
            )
            command = ground_shadow_request_mapping(request)
            request_sha = ground_shadow_request_sha256(
                request,
                runtime_profile_sha256=authority.runtime_profile_sha256,
            )
        except (GroundShadowAuthorityError, TypeError, ValueError):
            return self._terminal(
                status="corrupt_contract",
                execution_outcome="authority_rejected",
                detail_code="authority_or_request_invalid",
                started=started,
            )

        try:
            existing = self._load_receipt(
                authority,
                request=request,
                request_sha256=request_sha,
            )
        except GroundFusionShadowCapacityError:
            return self._terminal(
                status="resource",
                execution_outcome="durable_store_capacity",
                detail_code="durable_store_resource_limit",
                started=started,
                durable_disposition="unknown",
            )
        except (
            GroundFusionShadowArchiveError,
            GroundFusionShadowConflict,
            GroundFusionShadowStoreError,
            ValueError,
        ):
            return self._terminal(
                status="corrupt_contract",
                execution_outcome="durable_store_rejected",
                detail_code="durable_store_contract_failed",
                started=started,
                durable_disposition="unknown",
            )
        if existing is not None:
            return self._terminal(
                status="success",
                execution_outcome="durable_duplicate",
                detail_code="exact_durable_duplicate",
                started=started,
                durable_disposition="committed",
                receipt_metadata=_ground_receipt_metadata(
                    existing,
                    authority_sha256=authority.authority_sha256,
                ),
                duplicate=True,
            )

        process_request = {
            "authority_path": str(self._authority_path),
            "authority_sha256": authority.authority_sha256,
            "command_sha256": _command_sha256(
                authority_sha256=authority.authority_sha256,
                request_mapping=command,
                request_sha256=request_sha,
                runtime_profile_sha256=authority.runtime_profile_sha256,
            ),
            "contract": GROUND_SHADOW_PROCESS_REQUEST_CONTRACT,
            "factory_contract": self._factory_contract.to_mapping(),
            "factory_contract_sha256": self._factory_contract.fingerprint,
            "ground_request": command,
            "ground_request_sha256": request_sha,
            "isolation_limits": self._limits.to_mapping(),
        }
        decoder_result = self._executor.execute(
            target=(
                "storm_tracker_worker.ground_fusion_shadow_process_isolation:"
                "_ground_shadow_child_entry"
            ),
            request=process_request,
            effect_contract=GROUND_SHADOW_PROCESS_EFFECT_CONTRACT,
            cancel_event=cancel_event,
        )

        receipt: GroundFusionShadowReceipt | None = None
        durable_unknown = False
        authority_valid = True
        try:
            _load_authority(
                self._authority_path,
                self._expected_authority_sha256,
                self._factory_contract,
                self._limits,
            )
        except GroundShadowAuthorityError:
            authority_valid = False
        try:
            receipt = self._load_receipt(
                authority,
                request=request,
                request_sha256=request_sha,
            )
        except GroundFusionShadowCapacityError:
            durable_unknown = True
        except (
            GroundFusionShadowArchiveError,
            GroundFusionShadowConflict,
            GroundFusionShadowStoreError,
            ValueError,
        ):
            durable_unknown = True
        metadata = (
            None
            if receipt is None
            else _ground_receipt_metadata(
                receipt,
                authority_sha256=authority.authority_sha256,
            )
        )
        disposition: DurableDisposition = (
            "unknown" if durable_unknown else ("committed" if receipt else "not_committed")
        )
        isolation_enforced = decoder_result.outcome not in {
            "busy",
            "unsupported_platform",
            "boundary_setup_failed",
            "unsafe_effect_contract",
            "invalid_request",
        } and not (
            decoder_result.outcome == "cancelled"
            and decoder_result.detail_code == "cancelled_before_spawn"
        )
        if not authority_valid:
            return self._terminal(
                status="corrupt_contract",
                execution_outcome="authority_changed",
                detail_code="authority_changed_during_execution",
                started=started,
                durable_disposition=disposition,
                receipt_metadata=metadata,
                duplicate=None,
                isolation_enforced=isolation_enforced,
                child_exit_code=decoder_result.child_exit_code,
                elapsed_seconds=decoder_result.elapsed_seconds,
            )
        if decoder_result.outcome != "succeeded":
            status = _status_for_decoder(decoder_result)
            detail = {
                "deadline": "hard_parent_deadline",
                "resource": "hard_resource_limit",
                "crash": "child_process_crashed",
                "corrupt_contract": "child_protocol_corrupt",
                "cancelled": "parent_cancelled",
                "busy": "isolation_slot_busy",
                "boundary": "kernel_boundary_unavailable",
                "success": "child_success",
            }[status]
            return self._terminal(
                status=status,
                execution_outcome=decoder_result.outcome,
                detail_code=detail,
                started=started,
                durable_disposition=disposition,
                receipt_metadata=metadata,
                duplicate=None,
                isolation_enforced=isolation_enforced,
                child_exit_code=decoder_result.child_exit_code,
                elapsed_seconds=decoder_result.elapsed_seconds,
            )

        try:
            envelope = decoder_result.result
            if (
                not isinstance(envelope, dict)
                or envelope.get("contract") != GROUND_SHADOW_PROCESS_CHILD_CONTRACT
            ):
                raise GroundShadowProcessError("child envelope contract differs")
            if (
                envelope.get("authority_sha256") != authority.authority_sha256
                or not isinstance(envelope.get("code"), str)
                or _SAFE_CODE_RE.fullmatch(envelope["code"]) is None
            ):
                raise GroundShadowProcessError(
                    "child authority or result code differs"
                )
            child_status = envelope.get("status")
            if child_status == "success":
                if set(envelope) != {
                    "authority_sha256",
                    "code",
                    "contract",
                    "duplicate",
                    "receipt_metadata",
                    "status",
                } or type(envelope["duplicate"]) is not bool:
                    raise GroundShadowProcessError("success envelope schema differs")
                child_metadata = _validate_receipt_metadata(
                    envelope["receipt_metadata"]
                )
                if (
                    receipt is None
                    or metadata != child_metadata
                ):
                    raise GroundShadowProcessError(
                        "child receipt differs from durable authority"
                    )
                return self._terminal(
                    status="success",
                    execution_outcome="succeeded",
                    detail_code="child_receipt_verified",
                    started=started,
                    durable_disposition="committed",
                    receipt_metadata=metadata,
                    duplicate=envelope["duplicate"],
                    isolation_enforced=True,
                    child_exit_code=decoder_result.child_exit_code,
                    elapsed_seconds=decoder_result.elapsed_seconds,
                )
            if child_status == "window_expired" and receipt is not None:
                return self._terminal(
                    status="success",
                    execution_outcome="durable_duplicate",
                    detail_code="archived_durable_duplicate",
                    started=started,
                    durable_disposition="committed",
                    receipt_metadata=metadata,
                    duplicate=True,
                    isolation_enforced=True,
                    child_exit_code=decoder_result.child_exit_code,
                    elapsed_seconds=decoder_result.elapsed_seconds,
                )
            if child_status not in {
                "deadline",
                "resource",
                "crash",
                "corrupt_contract",
                "window_expired",
            } or set(envelope) != {
                "authority_sha256",
                "code",
                "contract",
                "status",
            }:
                raise GroundShadowProcessError("failure envelope schema differs")
            status: GroundShadowProcessStatus = {
                "deadline": "deadline",
                "resource": "resource",
                "crash": "crash",
                "corrupt_contract": "corrupt_contract",
                "window_expired": "corrupt_contract",
            }[child_status]
            return self._terminal(
                status=status,
                execution_outcome=f"child_{child_status}",
                detail_code={
                    "deadline": "ground_runtime_deadline",
                    "resource": "ground_runtime_resource_limit",
                    "crash": "ground_child_failed",
                    "corrupt_contract": "child_contract_rejected",
                    "window_expired": "archived_receipt_unavailable",
                }[child_status],
                started=started,
                durable_disposition=disposition,
                receipt_metadata=metadata,
                duplicate=None,
                isolation_enforced=True,
                child_exit_code=decoder_result.child_exit_code,
                elapsed_seconds=decoder_result.elapsed_seconds,
            )
        except (
            GroundShadowAuthorityError,
            GroundShadowProcessError,
            TypeError,
            ValueError,
        ):
            return self._terminal(
                status="corrupt_contract",
                execution_outcome="protocol_error",
                detail_code="child_contract_corrupt",
                started=started,
                durable_disposition=disposition,
                receipt_metadata=metadata,
                duplicate=None,
                isolation_enforced=True,
                child_exit_code=decoder_result.child_exit_code,
                elapsed_seconds=decoder_result.elapsed_seconds,
            )


__all__ = [
    "GROUND_SHADOW_PROCESS_AUTHORITY_CONTRACT",
    "GROUND_SHADOW_PROCESS_CHILD_CONTRACT",
    "GROUND_SHADOW_PROCESS_RECEIPT_METADATA_CONTRACT",
    "GROUND_SHADOW_PROCESS_REQUEST_CONTRACT",
    "GROUND_SHADOW_PROCESS_RESULT_CONTRACT",
    "GroundShadowAuthorityError",
    "GroundShadowFactoryContract",
    "GroundShadowIsolatedRunner",
    "GroundShadowIsolationLimits",
    "GroundShadowProcessError",
    "GroundShadowProcessResult",
    "canonical_ground_store_identity_sha256",
    "ground_shadow_request_mapping",
    "ground_shadow_request_sha256",
]
