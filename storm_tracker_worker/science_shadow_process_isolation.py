"""Hard parent-enforced execution boundary for the science shadow candidate.

This module is deliberately not imported by the worker service, Home
Assistant, MQTT or an operational publication path.  It combines the existing
``HardDecoderExecutor`` kernel boundary with the transactional
``DurableScienceShadowOrchestrator``.  Explicit IPC is canonical JSON and
contains only content-addressed authority/config identities, one absolute
authority-file path, run/attempt IDs and bounded lineage hypotheses.  Raster
arrays never cross IPC.

The child opens only paths listed by the exact checksummed authority document
through an explicitly audited factory contract.  The parent never trusts IPC
as state authority: after every terminal child outcome it independently opens
the receipt store and reconciles the exact request digest.  Therefore a hard
kill observes either the old durable head or a complete replayable receipt.
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
import stat
import threading
import time
from typing import Any, Callable, Iterable, Literal, Mapping

from .hard_decoder_isolation import (
    DecoderEffectContract,
    DecoderProcessResult,
    DecoderResourceLimits,
    HardDecoderExecutor,
    _canonical_json_bytes,
)
from .science_shadow_orchestrator import (
    DurableScienceShadowOrchestrator,
    ScienceShadowLimits,
    ScienceShadowReceipt,
    ScienceShadowReceiptStore,
    ScienceShadowStoreConflict,
    ScienceShadowStoreError,
    ScienceShadowWindowExpired,
    _canonical_lineage_hypotheses,
    _lineage_from_mapping,
    _lineage_mapping,
    science_shadow_request_sha256,
)
from .tracking_core.engine.cell_track_engine import CellTrackLineageHypothesis


SCIENCE_SHADOW_PROCESS_AUTHORITY_CONTRACT = (
    "stv3-science-shadow-process-authority-v1"
)
SCIENCE_SHADOW_PROCESS_REQUEST_CONTRACT = (
    "stv3-science-shadow-process-request-v1"
)
SCIENCE_SHADOW_PROCESS_CHILD_CONTRACT = (
    "stv3-science-shadow-process-child-result-v1"
)
SCIENCE_SHADOW_PROCESS_RECEIPT_METADATA_CONTRACT = (
    "stv3-science-shadow-process-receipt-metadata-v1"
)
SCIENCE_SHADOW_PROCESS_RESULT_CONTRACT = (
    "stv3-science-shadow-process-parent-result-v1"
)

SCIENCE_SHADOW_PROCESS_EFFECT_CONTRACT = DecoderEffectContract(
    contract_id="science-shadow.transactional-receipt.v1",
    hard_kill_safe=True,
    durable_effects="transactional",
    spawns_descendants=False,
)

PROCESS_TERMINATION_GRACE_SECONDS = 0.35
HARD_MAX_AUTHORITY_BYTES = 1024 * 1024
HARD_MAX_AUTHORIZED_PATHS = 64
HARD_MAX_PROCESS_REQUEST_BYTES = 256 * 1024
HARD_MAX_PROCESS_RESULT_BYTES = 1024 * 1024
MAX_PROCESS_DIAGNOSTIC_BYTES = 128 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID_RE = re.compile(r"^run-[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.:/-]{0,191}$")
_SAFE_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_TARGET_RE = re.compile(
    r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*$",
    re.ASCII,
)
_SECRET_KEY_RE = re.compile(
    r"(?:authorization|api[_-]?key|access[_-]?token|secret|password|credential)",
    re.IGNORECASE,
)


class ScienceShadowProcessError(RuntimeError):
    """Base failure at the parent-owned process boundary."""


class ScienceShadowAuthorityError(ScienceShadowProcessError):
    """The checksummed authority/config contract is invalid or stale."""


ScienceShadowProcessStatus = Literal[
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
        raise ScienceShadowAuthorityError(f"{label} is not a lowercase SHA-256")
    return value


def _require_safe_id(value: object, label: str) -> str:
    if not isinstance(value, str) or _SAFE_ID_RE.fullmatch(value) is None:
        raise ScienceShadowAuthorityError(f"{label} is not a bounded identifier")
    return value


def _sha_mapping(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _read_regular_file(
    path: Path,
    *,
    maximum_bytes: int,
) -> bytes:
    if not path.is_absolute() or path.is_symlink():
        raise ScienceShadowAuthorityError("authority path is not an absolute regular file")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ScienceShadowAuthorityError("authority file cannot be opened safely") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or not 1 <= before.st_size <= maximum_bytes:
            raise ScienceShadowAuthorityError("authority file size/type is invalid")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                raise ScienceShadowAuthorityError("authority file was truncated")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ScienceShadowAuthorityError("authority file grew while reading")
        after = os.fstat(descriptor)
        if (
            before.st_dev != after.st_dev
            or before.st_ino != after.st_ino
            or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
        ):
            raise ScienceShadowAuthorityError("authority file changed while reading")
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
            raise ScienceShadowAuthorityError("authority JSON node bound exceeded")
        if isinstance(current, dict):
            for key, nested in current.items():
                if not isinstance(key, str) or _SECRET_KEY_RE.search(key):
                    raise ScienceShadowAuthorityError(
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


@dataclass(frozen=True, slots=True)
class ScienceShadowIsolationLimits:
    """Fixed parent/kernel/IPC bounds for one fresh science child."""

    wall_seconds: float = 90.0
    cpu_seconds: float = 60.0
    rss_bytes: int = 1024 * 1024 * 1024
    max_authority_bytes: int = 256 * 1024
    max_request_bytes: int = 32 * 1024
    max_result_bytes: int = 64 * 1024
    max_authorized_paths: int = 32

    def __post_init__(self) -> None:
        if (
            isinstance(self.wall_seconds, bool)
            or not isinstance(self.wall_seconds, (int, float))
            or not math.isfinite(float(self.wall_seconds))
            or not 0.05 <= float(self.wall_seconds) <= 3_600.0
        ):
            raise ValueError("wall_seconds is outside 0.05..3600")
        if (
            isinstance(self.cpu_seconds, bool)
            or not isinstance(self.cpu_seconds, (int, float))
            or not math.isfinite(float(self.cpu_seconds))
            or not 0.05 <= float(self.cpu_seconds) <= 3_600.0
        ):
            raise ValueError("cpu_seconds is outside 0.05..3600")
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
                1024,
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
                1,
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
class ScienceShadowFactoryContract:
    """Audited child factory claim; no arbitrary factory is data-selected."""

    factory_id: str
    target: str
    opens_only_authorized_paths: bool
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
            and self.hard_kill_safe
            and self.transactional_receipt_only
            and not self.spawns_descendants
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
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
    def from_mapping(cls, value: Mapping[str, Any]) -> "ScienceShadowFactoryContract":
        if set(value) != {
            "factory_id",
            "hard_kill_safe",
            "opens_only_authorized_paths",
            "spawns_descendants",
            "target",
            "transactional_receipt_only",
        }:
            raise ScienceShadowAuthorityError("factory contract fields are not exact")
        if type(value["factory_id"]) is not str or type(value["target"]) is not str:
            raise ScienceShadowAuthorityError("factory contract text fields are invalid")
        try:
            return cls(
                factory_id=value["factory_id"],
                target=value["target"],
                opens_only_authorized_paths=value["opens_only_authorized_paths"],
                hard_kill_safe=value["hard_kill_safe"],
                transactional_receipt_only=value["transactional_receipt_only"],
                spawns_descendants=value["spawns_descendants"],
            )
        except (TypeError, ValueError) as exc:
            raise ScienceShadowAuthorityError("factory contract is invalid") from exc


@dataclass(frozen=True, slots=True, repr=False)
class _LoadedAuthority:
    document: dict[str, Any]
    authority_sha256: str
    engine_id: str
    runtime_profile_sha256: str
    science_limits: ScienceShadowLimits
    receipt_store_path: Path


def _validate_authorized_path(
    item: Mapping[str, Any],
    *,
    maximum_file_bytes: int,
) -> tuple[str, Path]:
    if set(item) != {"access", "content_sha256", "label", "path"}:
        raise ScienceShadowAuthorityError("authorized path fields are not exact")
    label = _require_safe_id(item["label"], "authorized path label")
    access = item["access"]
    if access not in {
        "read_file",
        "read_directory",
        "transactional_file",
        "transactional_directory",
    }:
        raise ScienceShadowAuthorityError("authorized path access is invalid")
    raw_path = item["path"]
    if not isinstance(raw_path, str) or "\0" in raw_path or len(raw_path) > 1024:
        raise ScienceShadowAuthorityError("authorized path text is invalid")
    path = Path(raw_path)
    if not path.is_absolute() or path.is_symlink() or not path.exists():
        raise ScienceShadowAuthorityError("authorized path is absent, relative or a symlink")
    resolved = path.resolve(strict=True)
    if resolved != path:
        raise ScienceShadowAuthorityError("authorized path is not already normalized")
    digest = item["content_sha256"]
    if access == "read_file":
        expected = _require_sha(digest, "authorized file content SHA")
        body = _read_regular_file(path, maximum_bytes=maximum_file_bytes)
        if hashlib.sha256(body).hexdigest() != expected:
            raise ScienceShadowAuthorityError("authorized read file checksum differs")
    elif access.endswith("file"):
        if digest is not None or not path.is_file():
            raise ScienceShadowAuthorityError("transactional file contract is invalid")
    else:
        if digest is not None or not path.is_dir():
            raise ScienceShadowAuthorityError("authorized directory contract is invalid")
    return label, path


def _load_authority(
    authority_path: Path,
    expected_authority_sha256: str,
    factory_contract: ScienceShadowFactoryContract,
    isolation_limits: ScienceShadowIsolationLimits,
) -> _LoadedAuthority:
    expected_sha = _require_sha(expected_authority_sha256, "authority SHA")
    if not factory_contract.admitted:
        raise ScienceShadowAuthorityError("factory kill/path contract is not admitted")
    body = _read_regular_file(
        authority_path,
        maximum_bytes=isolation_limits.max_authority_bytes,
    )
    if hashlib.sha256(body).hexdigest() != expected_sha:
        raise ScienceShadowAuthorityError("authority SHA is stale or belongs to another file")
    try:
        value = json.loads(body.decode("utf-8"))
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        OverflowError,
        RecursionError,
    ) as exc:
        raise ScienceShadowAuthorityError("authority is not JSON") from exc
    if not isinstance(value, dict):
        raise ScienceShadowAuthorityError("authority is not a JSON object")
    try:
        canonical_body = _canonical_json_bytes(value)
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ScienceShadowAuthorityError(
            "authority JSON violates canonical structural bounds"
        ) from exc
    if canonical_body != body:
        raise ScienceShadowAuthorityError("authority is not canonical JSON")
    if set(value) != {
        "authorized_paths",
        "contract",
        "engine_id",
        "factory_config",
        "factory_contract_sha256",
        "factory_id",
        "isolation_limits",
        "receipt_store_label",
        "runtime_profile_sha256",
        "science_limits",
    } or value.get("contract") != SCIENCE_SHADOW_PROCESS_AUTHORITY_CONTRACT:
        raise ScienceShadowAuthorityError("authority schema is not exact")
    _reject_secret_keys(value)
    if (
        value["factory_id"] != factory_contract.factory_id
        or value["factory_contract_sha256"] != factory_contract.fingerprint
        or value["isolation_limits"] != isolation_limits.to_mapping()
    ):
        raise ScienceShadowAuthorityError("authority factory/isolation binding differs")
    paths_value = value["authorized_paths"]
    if (
        not isinstance(paths_value, list)
        or not 1 <= len(paths_value) <= isolation_limits.max_authorized_paths
    ):
        raise ScienceShadowAuthorityError("authorized path count is invalid")
    authorized: dict[str, Path] = {}
    resolved_paths: set[Path] = set()
    for item in paths_value:
        if not isinstance(item, dict):
            raise ScienceShadowAuthorityError("authorized path record is invalid")
        label, path = _validate_authorized_path(
            item,
            maximum_file_bytes=isolation_limits.max_authority_bytes,
        )
        if label in authorized or path in resolved_paths:
            raise ScienceShadowAuthorityError("authorized paths are duplicated")
        authorized[label] = path
        resolved_paths.add(path)
    receipt_label = _require_safe_id(value["receipt_store_label"], "receipt label")
    if receipt_label not in authorized:
        raise ScienceShadowAuthorityError("receipt store path is not authorized")
    receipt_record = next(
        item for item in paths_value if item["label"] == receipt_label
    )
    if receipt_record["access"] != "transactional_file":
        raise ScienceShadowAuthorityError("receipt store is not transactional")
    config = value["factory_config"]
    if not isinstance(config, dict):
        raise ScienceShadowAuthorityError("factory config must be an object")
    for absolute in _iter_absolute_strings(config):
        if Path(absolute) not in resolved_paths:
            raise ScienceShadowAuthorityError(
                "factory config references an unauthorized absolute path"
            )
    try:
        science_limits = ScienceShadowLimits(**dict(value["science_limits"]))
    except (TypeError, ValueError) as exc:
        raise ScienceShadowAuthorityError("science limits are invalid") from exc
    if science_limits.to_mapping() != value["science_limits"]:
        raise ScienceShadowAuthorityError("science limits mapping is not exact")
    return _LoadedAuthority(
        document=value,
        authority_sha256=expected_sha,
        engine_id=_require_safe_id(value["engine_id"], "authority engine_id"),
        runtime_profile_sha256=_require_sha(
            value["runtime_profile_sha256"],
            "authority runtime profile",
        ),
        science_limits=science_limits,
        receipt_store_path=authorized[receipt_label],
    )


def _resolve_factory(contract: ScienceShadowFactoryContract) -> Callable[[Mapping[str, Any]], Any]:
    module_name, function_name = contract.target.split(":", 1)
    try:
        function = getattr(importlib.import_module(module_name), function_name)
    except (ImportError, AttributeError, ValueError) as exc:
        raise ScienceShadowAuthorityError("factory target cannot be resolved") from exc
    if not callable(function):
        raise ScienceShadowAuthorityError("factory target is not callable")
    return function


def _receipt_metadata(
    receipt: ScienceShadowReceipt,
    *,
    authority_sha256: str,
) -> dict[str, Any]:
    provenance = None if receipt.provenance is None else receipt.provenance.to_mapping()
    payload = {
        "accounting": receipt.accounting.to_mapping(),
        "admission_status": receipt.admission_status,
        "applied_lineage_hypothesis_ids": list(
            receipt.applied_lineage_hypothesis_ids
        ),
        "authority_sha256": authority_sha256,
        "canonical_run_id": receipt.canonical_run_id,
        "contract": SCIENCE_SHADOW_PROCESS_RECEIPT_METADATA_CONTRACT,
        "engine_id": receipt.engine_id,
        "input_state_revision": receipt.input_state_revision,
        "lineage_hypothesis_ids": [
            item.hypothesis_id for item in receipt.lineage_hypotheses
        ],
        "meteorological_admission_granted": (
            receipt.meteorological_admission_granted
        ),
        "next_state_revision": receipt.next_state_revision,
        "operational_admission_blockers": list(
            receipt.operational_admission_blockers
        ),
        "operational_snapshot_claim_generated": (
            receipt.operational_snapshot_claim_generated
        ),
        "outcome": receipt.outcome.value,
        "previous_advanced_receipt_sha256": (
            receipt.previous_advanced_receipt_sha256
        ),
        "process_isolation_certified": receipt.process_isolation_certified,
        "provenance": provenance,
        "receipt_sha256": receipt.receipt_sha256,
        "request_sha256": receipt.request_sha256,
        "runtime_profile_sha256": receipt.runtime_profile_sha256,
        "stage": receipt.stage.value,
        "transition_id": receipt.transition_id,
    }
    result = dict(payload)
    result["metadata_sha256"] = _sha_mapping(payload)
    if len(_canonical_json_bytes(result)) > MAX_PROCESS_DIAGNOSTIC_BYTES:
        raise ScienceShadowProcessError("receipt metadata exceeds diagnostic bound")
    return result


def _validate_receipt_metadata(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ScienceShadowProcessError("child receipt metadata is not an object")
    expected = {
        "accounting",
        "admission_status",
        "applied_lineage_hypothesis_ids",
        "authority_sha256",
        "canonical_run_id",
        "contract",
        "engine_id",
        "input_state_revision",
        "lineage_hypothesis_ids",
        "metadata_sha256",
        "meteorological_admission_granted",
        "next_state_revision",
        "operational_admission_blockers",
        "operational_snapshot_claim_generated",
        "outcome",
        "previous_advanced_receipt_sha256",
        "process_isolation_certified",
        "provenance",
        "receipt_sha256",
        "request_sha256",
        "runtime_profile_sha256",
        "stage",
        "transition_id",
    }
    if (
        set(value) != expected
        or value.get("contract")
        != SCIENCE_SHADOW_PROCESS_RECEIPT_METADATA_CONTRACT
    ):
        raise ScienceShadowProcessError("child receipt metadata schema differs")
    payload = dict(value)
    metadata_sha = payload.pop("metadata_sha256")
    if _require_sha(metadata_sha, "metadata SHA") != _sha_mapping(payload):
        raise ScienceShadowProcessError("child receipt metadata checksum differs")
    if len(_canonical_json_bytes(value)) > MAX_PROCESS_DIAGNOSTIC_BYTES:
        raise ScienceShadowProcessError("child receipt metadata is oversized")
    return value


def _child_envelope(
    *,
    status: Literal["success", "corrupt_contract", "crash"],
    authority_sha256: str,
    code: str,
    receipt_metadata: Mapping[str, Any] | None = None,
    duplicate: bool | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "authority_sha256": authority_sha256,
        "code": code,
        "contract": SCIENCE_SHADOW_PROCESS_CHILD_CONTRACT,
        "status": status,
    }
    if status == "success":
        result["duplicate"] = duplicate
        result["receipt_metadata"] = dict(receipt_metadata or {})
    return result


def _science_shadow_child_entry(request: Mapping[str, Any]) -> dict[str, Any]:
    """Importable spawn target; exception text and paths never return."""

    authority_sha = "0" * 64
    try:
        if not isinstance(request, dict) or set(request) != {
            "attempt_id",
            "authority_path",
            "authority_sha256",
            "canonical_run_id",
            "contract",
            "factory_contract",
            "factory_contract_sha256",
            "isolation_limits",
            "lineage_hypotheses",
            "request_sha256",
        } or request.get("contract") != SCIENCE_SHADOW_PROCESS_REQUEST_CONTRACT:
            raise ScienceShadowAuthorityError("process request schema differs")
        authority_sha = _require_sha(request["authority_sha256"], "authority SHA")
        factory_value = request["factory_contract"]
        if not isinstance(factory_value, dict):
            raise ScienceShadowAuthorityError("factory contract is absent")
        factory_contract = ScienceShadowFactoryContract.from_mapping(factory_value)
        if (
            factory_contract.fingerprint
            != _require_sha(request["factory_contract_sha256"], "factory contract SHA")
        ):
            raise ScienceShadowAuthorityError("factory contract checksum differs")
        isolation_value = request["isolation_limits"]
        if not isinstance(isolation_value, dict):
            raise ScienceShadowAuthorityError("isolation limits are absent")
        if set(isolation_value) != {
            "cpu_seconds",
            "max_authority_bytes",
            "max_authorized_paths",
            "max_request_bytes",
            "max_result_bytes",
            "rss_bytes",
            "termination_grace_seconds",
            "wall_seconds",
        } or isolation_value.get("termination_grace_seconds") != PROCESS_TERMINATION_GRACE_SECONDS:
            raise ScienceShadowAuthorityError("isolation limits schema differs")
        isolation_limits = ScienceShadowIsolationLimits(
            wall_seconds=isolation_value["wall_seconds"],
            cpu_seconds=isolation_value["cpu_seconds"],
            rss_bytes=isolation_value["rss_bytes"],
            max_authority_bytes=isolation_value["max_authority_bytes"],
            max_request_bytes=isolation_value["max_request_bytes"],
            max_result_bytes=isolation_value["max_result_bytes"],
            max_authorized_paths=isolation_value["max_authorized_paths"],
        )
        if isolation_limits.to_mapping() != isolation_value:
            raise ScienceShadowAuthorityError("isolation limits mapping differs")
        authority_path = Path(str(request["authority_path"]))
        authority = _load_authority(
            authority_path,
            authority_sha,
            factory_contract,
            isolation_limits,
        )
        factory = _resolve_factory(factory_contract)
        orchestrator = factory(authority.document)
        if not isinstance(orchestrator, DurableScienceShadowOrchestrator):
            raise ScienceShadowAuthorityError("factory returned another runtime type")
        if (
            orchestrator.runtime_profile_sha256 != authority.runtime_profile_sha256
            or orchestrator.receipt_store.path != authority.receipt_store_path
        ):
            raise ScienceShadowAuthorityError("factory runtime/store binding differs")
        attempt_id = _require_safe_id(request["attempt_id"], "attempt_id")
        run_id = request["canonical_run_id"]
        if not isinstance(run_id, str) or _RUN_ID_RE.fullmatch(run_id) is None:
            raise ScienceShadowAuthorityError("canonical run ID is invalid")
        lineage_value = request["lineage_hypotheses"]
        if not isinstance(lineage_value, list):
            raise ScienceShadowAuthorityError("lineage hypotheses are invalid")
        hypotheses = _canonical_lineage_hypotheses(
            (_lineage_from_mapping(item) for item in lineage_value),
            authority.science_limits.max_lineage_hypotheses,
        )
        request_sha = science_shadow_request_sha256(
            attempt_id=attempt_id,
            canonical_run_id=run_id,
            engine_id=authority.engine_id,
            runtime_profile_sha256=authority.runtime_profile_sha256,
            lineage_hypotheses=hypotheses,
            maximum_lineage_hypotheses=(
                authority.science_limits.max_lineage_hypotheses
            ),
        )
        if request_sha != _require_sha(request["request_sha256"], "request SHA"):
            raise ScienceShadowAuthorityError("request checksum differs")
        deadline = time.monotonic() + min(
            float(isolation_limits.wall_seconds),
            orchestrator.maximum_runtime_seconds,
        )
        result = orchestrator.process(
            attempt_id=attempt_id,
            run_id=run_id,
            deadline_at=deadline,
            lineage_hypotheses=hypotheses,
        )
        metadata = _receipt_metadata(
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
    except ScienceShadowAuthorityError:
        return _child_envelope(
            status="corrupt_contract",
            authority_sha256=authority_sha,
            code="authority_or_request_invalid",
        )
    except (ScienceShadowStoreConflict, ScienceShadowStoreError):
        return _child_envelope(
            status="corrupt_contract",
            authority_sha256=authority_sha,
            code="durable_store_contract_failed",
        )
    except MemoryError:
        # Preserve the kernel/resource terminal class owned by
        # ``HardDecoderExecutor``; swallowing this here would mislabel an RSS
        # breach as an ordinary science failure.
        raise
    except OSError as exc:
        if exc.errno == errno.ENOMEM:
            raise
        return _child_envelope(
            status="crash",
            authority_sha256=authority_sha,
            code="science_child_failed",
        )
    except Exception:
        # ``_CpuLimitReached`` intentionally inherits BaseException and must
        # escape this target so the outer hard-isolation protocol can retain
        # the distinct CPU-limit outcome.
        return _child_envelope(
            status="crash",
            authority_sha256=authority_sha,
            code="science_child_failed",
        )


@dataclass(frozen=True, slots=True)
class ScienceShadowProcessResult:
    """Secret/path-free parent result; never an operational weather claim."""

    status: ScienceShadowProcessStatus
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
    operational_snapshot_claim_generated: bool = False
    contract: str = SCIENCE_SHADOW_PROCESS_RESULT_CONTRACT

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
            raise ValueError("unknown science process terminal status")
        if not isinstance(self.execution_outcome, str) or not self.execution_outcome:
            raise ValueError("execution outcome is invalid")
        if (
            not isinstance(self.detail_code, str)
            or _SAFE_CODE_RE.fullmatch(self.detail_code) is None
        ):
            raise ValueError("detail code is invalid")
        _require_sha(self.authority_sha256, "result authority SHA")
        if self.durable_disposition not in {"committed", "not_committed", "unknown"}:
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
            or self.operational_snapshot_claim_generated
            or self.contract != SCIENCE_SHADOW_PROCESS_RESULT_CONTRACT
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
            "operational_snapshot_claim_generated": (
                self.operational_snapshot_claim_generated
            ),
            "receipt_metadata": (
                None if self.receipt_metadata is None else dict(self.receipt_metadata)
            ),
            "status": self.status,
        }


def _status_for_decoder(result: DecoderProcessResult) -> ScienceShadowProcessStatus:
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
    }:
        return "corrupt_contract"
    return "crash"


class ScienceShadowIsolatedRunner:
    """Parent-owned one-shot process runner with durable reconciliation."""

    def __init__(
        self,
        *,
        authority_path: str | Path,
        expected_authority_sha256: str,
        factory_contract: ScienceShadowFactoryContract,
        limits: ScienceShadowIsolationLimits = ScienceShadowIsolationLimits(),
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
        if not isinstance(factory_contract, ScienceShadowFactoryContract):
            raise TypeError("factory_contract has wrong type")
        if not factory_contract.admitted:
            raise ValueError("factory contract is not hard-kill/path safe")
        if not isinstance(limits, ScienceShadowIsolationLimits):
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
        status: ScienceShadowProcessStatus,
        execution_outcome: str,
        detail_code: str,
        started: float,
        durable_disposition: DurableDisposition = "not_committed",
        receipt_metadata: Mapping[str, Any] | None = None,
        duplicate: bool | None = None,
        isolation_enforced: bool = False,
        child_exit_code: int | None = None,
        elapsed_seconds: float | None = None,
    ) -> ScienceShadowProcessResult:
        return ScienceShadowProcessResult(
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

    def _load_receipt(
        self,
        authority: _LoadedAuthority,
        *,
        attempt_id: str,
        canonical_run_id: str,
        request_sha256: str,
    ) -> ScienceShadowReceipt | None:
        store = ScienceShadowReceiptStore(
            authority.receipt_store_path,
            engine_id=authority.engine_id,
            runtime_profile_sha256=authority.runtime_profile_sha256,
            limits=authority.science_limits,
        )
        receipt = store.load_attempt(
            attempt_id,
            request_sha256=request_sha256,
        )
        if receipt is not None:
            return receipt
        # The durable orchestrator deliberately de-duplicates an already
        # advanced canonical run even when the caller supplies a fresh attempt
        # key.  Such a receipt remains bound to its original request digest;
        # looking up only the fresh attempt would incorrectly classify the
        # child's valid duplicate response as corrupt IPC.  The run index is
        # checksummed by ``ScienceShadowReceiptStore`` and is therefore the
        # second (and only other) authoritative reconciliation key.
        return store.load_advanced_run(canonical_run_id)

    def execute(
        self,
        *,
        attempt_id: str,
        canonical_run_id: str,
        lineage_hypotheses: Iterable[CellTrackLineageHypothesis] = (),
        cancel_event: threading.Event | None = None,
    ) -> ScienceShadowProcessResult:
        started = time.monotonic()
        try:
            attempt = _require_safe_id(attempt_id, "attempt_id")
            if not isinstance(canonical_run_id, str) or _RUN_ID_RE.fullmatch(
                canonical_run_id
            ) is None:
                raise ScienceShadowAuthorityError("canonical run ID is invalid")
            authority = _load_authority(
                self._authority_path,
                self._expected_authority_sha256,
                self._factory_contract,
                self._limits,
            )
            hypotheses = _canonical_lineage_hypotheses(
                lineage_hypotheses,
                authority.science_limits.max_lineage_hypotheses,
            )
            request_sha = science_shadow_request_sha256(
                attempt_id=attempt,
                canonical_run_id=canonical_run_id,
                engine_id=authority.engine_id,
                runtime_profile_sha256=authority.runtime_profile_sha256,
                lineage_hypotheses=hypotheses,
                maximum_lineage_hypotheses=(
                    authority.science_limits.max_lineage_hypotheses
                ),
            )
        except (ScienceShadowAuthorityError, TypeError, ValueError):
            return self._terminal(
                status="corrupt_contract",
                execution_outcome="authority_rejected",
                detail_code="authority_or_request_invalid",
                started=started,
            )

        try:
            existing = self._load_receipt(
                authority,
                attempt_id=attempt,
                canonical_run_id=canonical_run_id,
                request_sha256=request_sha,
            )
        except ScienceShadowWindowExpired:
            return self._terminal(
                status="corrupt_contract",
                execution_outcome="window_expired",
                detail_code="window_expired",
                started=started,
            )
        except (ScienceShadowStoreConflict, ScienceShadowStoreError):
            return self._terminal(
                status="corrupt_contract",
                execution_outcome="durable_store_rejected",
                detail_code="durable_store_contract_failed",
                started=started,
                durable_disposition="unknown",
            )
        if existing is not None:
            metadata = _receipt_metadata(
                existing,
                authority_sha256=authority.authority_sha256,
            )
            return self._terminal(
                status="success",
                execution_outcome="durable_duplicate",
                detail_code="exact_durable_duplicate",
                started=started,
                durable_disposition="committed",
                receipt_metadata=metadata,
                duplicate=True,
            )

        request = {
            "attempt_id": attempt,
            "authority_path": str(self._authority_path),
            "authority_sha256": authority.authority_sha256,
            "canonical_run_id": canonical_run_id,
            "contract": SCIENCE_SHADOW_PROCESS_REQUEST_CONTRACT,
            "factory_contract": self._factory_contract.to_mapping(),
            "factory_contract_sha256": self._factory_contract.fingerprint,
            "isolation_limits": self._limits.to_mapping(),
            "lineage_hypotheses": [_lineage_mapping(item) for item in hypotheses],
            "request_sha256": request_sha,
        }
        decoder_result = self._executor.execute(
            target=(
                "storm_tracker_worker.science_shadow_process_isolation:"
                "_science_shadow_child_entry"
            ),
            request=request,
            effect_contract=SCIENCE_SHADOW_PROCESS_EFFECT_CONTRACT,
            cancel_event=cancel_event,
        )

        receipt: ScienceShadowReceipt | None = None
        durable_unknown = False
        try:
            receipt = self._load_receipt(
                authority,
                attempt_id=attempt,
                canonical_run_id=canonical_run_id,
                request_sha256=request_sha,
            )
        except ScienceShadowWindowExpired:
            durable_unknown = True
        except (ScienceShadowStoreConflict, ScienceShadowStoreError):
            durable_unknown = True
        metadata = (
            None
            if receipt is None
            else _receipt_metadata(
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
                or envelope.get("contract")
                != SCIENCE_SHADOW_PROCESS_CHILD_CONTRACT
            ):
                raise ScienceShadowProcessError("child envelope contract differs")
            if envelope.get("authority_sha256") != authority.authority_sha256:
                raise ScienceShadowProcessError("child authority identity differs")
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
                    raise ScienceShadowProcessError("success envelope schema differs")
                child_metadata = _validate_receipt_metadata(
                    envelope["receipt_metadata"]
                )
                if (
                    receipt is None
                    or metadata != child_metadata
                ):
                    raise ScienceShadowProcessError(
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
            if child_status not in {"corrupt_contract", "crash"} or set(envelope) != {
                "authority_sha256",
                "code",
                "contract",
                "status",
            }:
                raise ScienceShadowProcessError("failure envelope schema differs")
            status = (
                "corrupt_contract" if child_status == "corrupt_contract" else "crash"
            )
            return self._terminal(
                status=status,
                execution_outcome=f"child_{child_status}",
                detail_code=(
                    "child_contract_rejected"
                    if status == "corrupt_contract"
                    else "science_child_failed"
                ),
                started=started,
                durable_disposition=disposition,
                receipt_metadata=metadata,
                isolation_enforced=True,
                child_exit_code=decoder_result.child_exit_code,
                elapsed_seconds=decoder_result.elapsed_seconds,
            )
        except (ScienceShadowAuthorityError, ScienceShadowProcessError, TypeError, ValueError):
            return self._terminal(
                status="corrupt_contract",
                execution_outcome="protocol_error",
                detail_code="child_contract_corrupt",
                started=started,
                durable_disposition=disposition,
                receipt_metadata=metadata,
                isolation_enforced=True,
                child_exit_code=decoder_result.child_exit_code,
                elapsed_seconds=decoder_result.elapsed_seconds,
            )


__all__ = [
    "HARD_MAX_AUTHORITY_BYTES",
    "HARD_MAX_AUTHORIZED_PATHS",
    "PROCESS_TERMINATION_GRACE_SECONDS",
    "SCIENCE_SHADOW_PROCESS_AUTHORITY_CONTRACT",
    "SCIENCE_SHADOW_PROCESS_CHILD_CONTRACT",
    "SCIENCE_SHADOW_PROCESS_RECEIPT_METADATA_CONTRACT",
    "SCIENCE_SHADOW_PROCESS_REQUEST_CONTRACT",
    "SCIENCE_SHADOW_PROCESS_RESULT_CONTRACT",
    "ScienceShadowAuthorityError",
    "ScienceShadowFactoryContract",
    "ScienceShadowIsolatedRunner",
    "ScienceShadowIsolationLimits",
    "ScienceShadowProcessError",
    "ScienceShadowProcessResult",
]
