"""Preview and execute an explicitly bounded Storm Tracker history reset.

The default mode only writes a content-addressed manifest.  Deletion is a
separate operation that requires the exact manifest bytes, their SHA-256, an
explicit execution flag, a fixed confirmation sentence and a new evidence
path.  This module never discovers live paths and never deletes a configured
scope root.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
from typing import Any, Iterable, Mapping, Sequence


CONFIG_CONTRACT = "storm_tracker_runtime_history_reset_config_v1"
MANIFEST_CONTRACT = "storm_tracker_runtime_history_reset_manifest_v1"
EXECUTION_EVIDENCE_CONTRACT = (
    "storm_tracker_runtime_history_reset_execution_evidence_v1"
)
PRESERVATION_POLICY = "storm_tracker_history_preservation_boundaries_v1"
ROOT_SCOPE_ASSERTION = "dedicated_storm_tracker_runtime_storage_v1"
TARGET_DATA_ASSERTION = (
    "contains_only_storm_tracker_meteorological_runtime_data_v1"
)
RESET_ACTION = "delete_explicit_storm_tracker_runtime_history_targets"
EXECUTION_CONFIRMATION = (
    "DELETE_ONLY_STORM_TRACKER_RUNTIME_HISTORY_WORKER_STOPPED"
)

MAX_CONFIG_BYTES = 1024 * 1024
MAX_MANIFEST_BYTES = 256 * 1024 * 1024
MAX_ROOTS = 32
MAX_TARGETS = 256
MAX_ENTRIES = 2_000_000
MAX_RELATIVE_PARTS = 32
MAX_IDENTIFIER_LENGTH = 96
MAX_PATH_TEXT_LENGTH = 4096

ALLOWED_TARGET_CATEGORIES = frozenset({
    "meteorological_raw_capture_payloads",
    "meteorological_raw_capture_metadata",
    "meteorological_runtime_history",
    "meteorological_replay_cache",
    "meteorological_derived_cache",
    "meteorological_calibration_history",
})
ALLOWED_TARGET_KINDS = frozenset({"directory", "file"})
PRESERVED_DOMAINS = (
    "home_assistant_recorder_and_history",
    "home_assistant_configuration_and_secrets",
    "home_assistant_config_entries_and_registries",
    "provider_credentials_and_token_state",
    "desired_state_authority_and_control_state",
    "rollback_packages",
    "release_and_test_evidence",
)

_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,95}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GLOB_OR_EXPANSION_CHARACTERS = frozenset("*?[]{}")
_REPARSE_ATTRIBUTE = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)

_PROTECTED_EXACT_NAMES = frozenset({
    ".storage",
    "configuration.yaml",
    "configuration.yml",
    "secrets.yaml",
    "secrets.yml",
    "automations.yaml",
    "automations.yml",
    "scripts.yaml",
    "scripts.yml",
    "scenes.yaml",
    "scenes.yml",
    "home-assistant_v2.db",
    "home-assistant_v2.db-shm",
    "home-assistant_v2.db-wal",
    "core.config_entries",
    "core.entity_registry",
    "core.device_registry",
    "core.area_registry",
    "core.floor_registry",
    "core.label_registry",
    "core.restore_state",
    "auth",
    "auth_provider.homeassistant",
    "manifest.json",
    "config.yaml",
    "config.yml",
    "requirements.txt",
    "dockerfile",
    "run.sh",
    "compose.yaml",
    "compose.yml",
    "__init__.py",
})
_PROTECTED_NAME_FRAGMENTS = (
    "auth_state",
    "auth-state",
    "authorization",
    "backup",
    "config_entries",
    "config-entries",
    "control",
    "credential",
    "device_registry",
    "device-registry",
    "entity_registry",
    "entity-registry",
    "evidence",
    "ha_history",
    "ha-history",
    "home_assistant_history",
    "home-assistant-history",
    "recorder",
    "registry",
    "release_proof",
    "release-proof",
    "secret",
    "token",
    "token_state",
    "token-state",
    "oauth",
    "api_key",
    "api-key",
    "password",
    "desired_state",
    "desired-state",
    "authority",
    "control_plane",
    "control-plane",
    "control_state",
    "control-state",
    "rollback",
    "pre_rollback",
    "pre-rollback",
    "release_evidence",
    "release-evidence",
    "test_evidence",
    "test-evidence",
    "test_result",
    "test-result",
    "_deploy",
)
_PROTECTED_EXACT_SEGMENTS = frozenset({
    "credentials",
    "secrets",
    "tokens",
    "oauth",
    "authority",
    "control",
    "desired_state",
    "desired-state",
    "rollback",
    "backups",
    "release",
    "evidence",
})
_PROTECTED_PACKAGE_MARKERS = (
    "_deploy.zip",
    "_deploy.tar.gz",
    "_deploy.tgz",
    "backup.zip",
    "backup.tar",
    "backup.tar.gz",
    "backup.tgz",
)
_PROTECTED_SOURCE_SUFFIXES = (
    ".py",
    ".pyc",
    ".pyo",
    ".ps1",
    ".sh",
    ".yaml",
    ".yml",
)


class ResetSafetyError(RuntimeError):
    """A fixed-domain failure that occurs before an unsafe action can continue."""


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _canonical_json(document: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            document,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _reject_float(_value: str) -> None:
    raise ResetSafetyError("zwevendekommagetallen zijn niet toegestaan")


def _reject_constant(_value: str) -> None:
    raise ResetSafetyError("niet-eindige JSON-waarden zijn niet toegestaan")


def _bounded_integer(value: str) -> int:
    parsed = int(value, 10)
    if abs(parsed) > (2**63 - 1):
        raise ResetSafetyError("JSON-geheel getal valt buiten het contract")
    return parsed


def _unique_object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ResetSafetyError("dubbele JSON-sleutel is niet toegestaan")
        result[key] = value
    return result


def _validate_json_shape(value: Any, *, depth: int = 0) -> None:
    if depth > 64:
        raise ResetSafetyError("JSON-document is te diep genest")
    if isinstance(value, str):
        if len(value) > MAX_PATH_TEXT_LENGTH:
            raise ResetSafetyError("JSON-tekst overschrijdt de vaste grens")
        return
    if value is None or isinstance(value, (bool, int)):
        return
    if isinstance(value, list):
        for item in value:
            _validate_json_shape(item, depth=depth + 1)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ResetSafetyError("JSON-object bevat een ongeldige sleutel")
            _validate_json_shape(key, depth=depth + 1)
            _validate_json_shape(item, depth=depth + 1)
        return
    raise ResetSafetyError("JSON-document bevat een niet-toegestaan waardetype")


def _decode_document(raw: bytes, *, label: str) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ResetSafetyError(f"{label} is geen geldige UTF-8") from error
    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_float,
            parse_int=_bounded_integer,
            parse_constant=_reject_constant,
        )
    except json.JSONDecodeError as error:
        raise ResetSafetyError(f"{label} is geen geldig JSON-document") from error
    _validate_json_shape(decoded)
    if not isinstance(decoded, dict):
        raise ResetSafetyError(f"{label} moet een JSON-object zijn")
    return decoded


def _require_exact_keys(
    value: Mapping[str, Any], expected: Iterable[str], *, label: str
) -> None:
    expected_set = frozenset(expected)
    if frozenset(value) != expected_set:
        raise ResetSafetyError(f"{label} heeft niet de exacte sleutelset")


def _require_text(
    value: Mapping[str, Any], key: str, *, label: str
) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ResetSafetyError(f"{label}.{key} moet niet-lege tekst zijn")
    return result


def _require_list(
    value: Mapping[str, Any], key: str, *, label: str
) -> list[Any]:
    result = value.get(key)
    if not isinstance(result, list):
        raise ResetSafetyError(f"{label}.{key} moet een lijst zijn")
    return result


def _require_integer(
    value: Mapping[str, Any],
    key: str,
    *,
    label: str,
    minimum: int = 0,
) -> int:
    result = value.get(key)
    if type(result) is not int or result < minimum:  # bool is deliberately rejected
        raise ResetSafetyError(f"{label}.{key} moet een begrensd geheel getal zijn")
    return result


def _validate_identifier(value: str, *, label: str) -> str:
    if len(value) > MAX_IDENTIFIER_LENGTH or _IDENTIFIER.fullmatch(value) is None:
        raise ResetSafetyError(f"{label} is geen vaste veilige identifier")
    return value


def _has_forbidden_path_syntax(text: str) -> bool:
    return (
        not text
        or "\x00" in text
        or any(character in text for character in _GLOB_OR_EXPANSION_CHARACTERS)
    )


def _explicit_absolute_path(value: os.PathLike[str] | str, *, label: str) -> Path:
    text = os.fspath(value)
    if not isinstance(text, str) or len(text) > MAX_PATH_TEXT_LENGTH:
        raise ResetSafetyError(f"{label} is geen begrensd pad")
    if _has_forbidden_path_syntax(text):
        raise ResetSafetyError(f"{label} bevat wildcard- of expansiesyntaxis")
    path = Path(text)
    if not path.is_absolute():
        raise ResetSafetyError(f"{label} moet expliciet absoluut zijn")
    if any(part == ".." for part in path.parts):
        raise ResetSafetyError(f"{label} mag geen oudersegment bevatten")
    return path


def _is_reparse_or_link(info: os.stat_result) -> bool:
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, "st_file_attributes", 0) & _REPARSE_ATTRIBUTE
    )


def _assert_existing_chain_without_reparse(path: Path, *, label: str) -> None:
    chain = tuple(reversed((path, *path.parents)))
    for candidate in chain:
        try:
            info = os.lstat(candidate)
        except OSError as error:
            raise ResetSafetyError(f"{label} bestaat niet volledig") from error
        if _is_reparse_or_link(info):
            raise ResetSafetyError(f"{label} loopt via een reparsepunt of link")


def _canonical_existing(
    value: os.PathLike[str] | str,
    *,
    label: str,
    expected_kind: str | None = None,
) -> Path:
    path = _explicit_absolute_path(value, label=label)
    _assert_existing_chain_without_reparse(path, label=label)
    try:
        canonical = path.resolve(strict=True)
    except OSError as error:
        raise ResetSafetyError(f"{label} kan niet canoniek worden geopend") from error
    _assert_existing_chain_without_reparse(canonical, label=label)
    info = os.lstat(canonical)
    if _is_reparse_or_link(info):
        raise ResetSafetyError(f"{label} is een reparsepunt of link")
    if expected_kind == "directory" and not stat.S_ISDIR(info.st_mode):
        raise ResetSafetyError(f"{label} is geen map")
    if expected_kind == "file" and not stat.S_ISREG(info.st_mode):
        raise ResetSafetyError(f"{label} is geen regulier bestand")
    return canonical


def _new_output_path(value: os.PathLike[str] | str, *, label: str) -> Path:
    path = _explicit_absolute_path(value, label=label)
    if path.exists() or os.path.lexists(path):
        raise ResetSafetyError(f"{label} bestaat al en wordt niet overschreven")
    parent = _canonical_existing(
        path.parent,
        label=f"{label}-bovenmap",
        expected_kind="directory",
    )
    return parent / path.name


def _is_within_or_equal(path: Path, root: Path) -> bool:
    try:
        common = os.path.commonpath((os.fspath(path), os.fspath(root)))
    except ValueError:
        return False
    return os.path.normcase(common) == os.path.normcase(os.fspath(root))


def _is_strict_descendant(path: Path, root: Path) -> bool:
    return path != root and _is_within_or_equal(path, root)


def _assert_outside_roots(path: Path, roots: Sequence[Path], *, label: str) -> None:
    if any(_is_within_or_equal(path, root) for root in roots):
        raise ResetSafetyError(
            f"{label} moet buiten iedere verwijderingsauthority worden bewaard"
        )


def _relative_parts(value: str, *, label: str) -> tuple[str, ...]:
    if (
        not isinstance(value, str)
        or len(value) > MAX_PATH_TEXT_LENGTH
        or _has_forbidden_path_syntax(value)
        or "\\" in value
        or value.startswith("/")
        or ":" in value
    ):
        raise ResetSafetyError(f"{label} is geen vast relatief POSIX-pad")
    pure = PurePosixPath(value)
    parts = pure.parts
    if (
        not parts
        or len(parts) > MAX_RELATIVE_PARTS
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise ResetSafetyError(f"{label} bevat een ongeldig padsegment")
    return tuple(parts)


def _portable_relative(path: Path, root: Path) -> str:
    return PurePosixPath(*path.relative_to(root).parts).as_posix()


def _protected_reason(relative_path: str) -> str | None:
    parts = tuple(part.casefold() for part in PurePosixPath(relative_path).parts)
    for part in parts:
        if part in _PROTECTED_EXACT_NAMES:
            return "Home Assistant-, configuratie- of packagebestand"
        if part in _PROTECTED_EXACT_SEGMENTS:
            return "credential-, control-, rollback- of evidencegrens"
        if any(fragment in part for fragment in _PROTECTED_NAME_FRAGMENTS):
            return "credential-, control-, rollback- of evidencegrens"
        if part.startswith("home-assistant_v2.db"):
            return "Home Assistant Recorder-database"
        if any(part.endswith(marker) for marker in _PROTECTED_PACKAGE_MARKERS):
            return "rollback- of releasepakket"
        if any(part.endswith(suffix) for suffix in _PROTECTED_SOURCE_SUFFIXES):
            return "installatie- of broncode"
    return None


def _protected_ancestor_reason(path: Path) -> str | None:
    """Reject a deletion authority rooted below a preserved path segment."""
    parts = path.parts[1:] if path.is_absolute() else path.parts
    if not parts:
        return None
    return _protected_reason(PurePosixPath(*parts).as_posix())


def _entry_kind(info: os.stat_result, *, label: str) -> str:
    if _is_reparse_or_link(info):
        raise ResetSafetyError(f"{label} is een reparsepunt of link")
    if stat.S_ISDIR(info.st_mode):
        return "directory"
    if stat.S_ISREG(info.st_mode):
        return "file"
    raise ResetSafetyError(f"{label} is geen regulier bestand of map")


def _snapshot_entry(path: Path, root: Path, *, target_id: str) -> dict[str, Any]:
    if not _is_strict_descendant(path, root):
        raise ResetSafetyError(f"target {target_id} verlaat zijn canonieke root")
    _assert_existing_chain_without_reparse(
        path,
        label=f"target {target_id}",
    )
    relative = _portable_relative(path, root)
    reason = _protected_reason(relative)
    if reason is not None:
        raise ResetSafetyError(
            f"target {target_id} raakt een beschermde grens: {reason}"
        )
    try:
        info = os.lstat(path)
    except OSError as error:
        raise ResetSafetyError(f"target {target_id} veranderde tijdens preview") from error
    kind = _entry_kind(info, label=f"target {target_id}")
    try:
        canonical = path.resolve(strict=True)
    except OSError as error:
        raise ResetSafetyError(f"target {target_id} is niet canoniek leesbaar") from error
    if canonical != path or not _is_strict_descendant(canonical, root):
        raise ResetSafetyError(f"target {target_id} wijzigt canonieke identiteit")
    return {
        "device": int(info.st_dev),
        "inode": int(info.st_ino),
        "kind": kind,
        "mode": int(stat.S_IMODE(info.st_mode)),
        "mtime_ns": int(info.st_mtime_ns),
        "relative_path": relative,
        "size": int(info.st_size),
    }


def _scan_target(path: Path, root: Path, *, target_id: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []

    def visit(candidate: Path) -> None:
        if len(entries) >= MAX_ENTRIES:
            raise ResetSafetyError("targetinventaris overschrijdt de vaste grens")
        snapshot = _snapshot_entry(candidate, root, target_id=target_id)
        entries.append(snapshot)
        if snapshot["kind"] != "directory":
            return
        try:
            with os.scandir(candidate) as children:
                names = sorted((child.name for child in children), key=str.casefold)
        except OSError as error:
            raise ResetSafetyError(f"target {target_id} kan niet worden gescand") from error
        for name in names:
            visit(candidate / name)

    visit(path)
    return entries


def _read_bounded_file(
    value: os.PathLike[str] | str, *, label: str, maximum_bytes: int
) -> tuple[Path, bytes]:
    path = _canonical_existing(value, label=label, expected_kind="file")
    info = os.lstat(path)
    if info.st_size > maximum_bytes:
        raise ResetSafetyError(f"{label} overschrijdt de vaste bytegrens")
    try:
        with path.open("rb") as source:
            raw = source.read(maximum_bytes + 1)
    except OSError as error:
        raise ResetSafetyError(f"{label} kan niet veilig worden gelezen") from error
    if len(raw) > maximum_bytes:
        raise ResetSafetyError(f"{label} overschrijdt de vaste bytegrens")
    after = os.lstat(path)
    before_identity = (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
        info.st_mode,
    )
    after_identity = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_mode,
    )
    if before_identity != after_identity or _is_reparse_or_link(after):
        raise ResetSafetyError(f"{label} veranderde tijdens het lezen")
    return path, raw


def _validate_nonoverlap(paths: Sequence[Path], *, label: str) -> None:
    for index, left in enumerate(paths):
        for right in paths[index + 1:]:
            if _is_within_or_equal(left, right) or _is_within_or_equal(right, left):
                raise ResetSafetyError(f"{label} mogen niet overlappen")


def _root_from_config(value: Any, *, index: int) -> tuple[dict[str, str], Path]:
    label = f"roots[{index}]"
    if not isinstance(value, dict):
        raise ResetSafetyError(f"{label} moet een object zijn")
    _require_exact_keys(
        value,
        {"absolute_path", "root_id", "scope_assertion"},
        label=label,
    )
    root_id = _validate_identifier(
        _require_text(value, "root_id", label=label),
        label=f"{label}.root_id",
    )
    if _require_text(value, "scope_assertion", label=label) != ROOT_SCOPE_ASSERTION:
        raise ResetSafetyError(f"{label} mist de vaste scope-assertie")
    path = _canonical_existing(
        _require_text(value, "absolute_path", label=label),
        label=f"{label}.absolute_path",
        expected_kind="directory",
    )
    if path.parent == path:
        raise ResetSafetyError("een bestandssysteemanker is geen geldige root")
    protected_reason = _protected_ancestor_reason(path)
    if protected_reason is not None:
        raise ResetSafetyError(
            f"{label} ligt onder een beschermde grens: {protected_reason}"
        )
    return ({
        "canonical_path": str(path),
        "root_id": root_id,
        "scope_assertion": ROOT_SCOPE_ASSERTION,
    }, path)


def _target_from_config(
    value: Any,
    *,
    index: int,
    roots_by_id: Mapping[str, Path],
) -> tuple[dict[str, Any], Path]:
    label = f"targets[{index}]"
    if not isinstance(value, dict):
        raise ResetSafetyError(f"{label} moet een object zijn")
    _require_exact_keys(
        value,
        {
            "category",
            "data_assertion",
            "expected_kind",
            "relative_path",
            "root_id",
            "target_id",
        },
        label=label,
    )
    target_id = _validate_identifier(
        _require_text(value, "target_id", label=label),
        label=f"{label}.target_id",
    )
    root_id = _validate_identifier(
        _require_text(value, "root_id", label=label),
        label=f"{label}.root_id",
    )
    root = roots_by_id.get(root_id)
    if root is None:
        raise ResetSafetyError(f"{label} verwijst naar een onbekende root")
    category = _require_text(value, "category", label=label)
    if category not in ALLOWED_TARGET_CATEGORIES:
        raise ResetSafetyError(f"{label} gebruikt geen toegestane datacategorie")
    expected_kind = _require_text(value, "expected_kind", label=label)
    if expected_kind not in ALLOWED_TARGET_KINDS:
        raise ResetSafetyError(f"{label} heeft een ongeldig doeltype")
    if _require_text(value, "data_assertion", label=label) != TARGET_DATA_ASSERTION:
        raise ResetSafetyError(f"{label} mist de vaste data-assertie")
    relative = _require_text(value, "relative_path", label=label)
    parts = _relative_parts(relative, label=f"{label}.relative_path")
    lexical = root.joinpath(*parts)
    path = _canonical_existing(
        lexical,
        label=f"{label}.relative_path",
        expected_kind=expected_kind,
    )
    if not _is_strict_descendant(path, root):
        raise ResetSafetyError(f"{label} mag de root zelf niet verwijderen")
    canonical_relative = _portable_relative(path, root)
    if canonical_relative != PurePosixPath(*parts).as_posix():
        raise ResetSafetyError(f"{label} wijzigt na canonicalisatie")
    entries = _scan_target(path, root, target_id=target_id)
    return ({
        "canonical_path": str(path),
        "category": category,
        "data_assertion": TARGET_DATA_ASSERTION,
        "entries": entries,
        "entry_count": len(entries),
        "expected_kind": expected_kind,
        "relative_path": canonical_relative,
        "root_id": root_id,
        "target_id": target_id,
    }, path)


def _build_manifest_document(
    config: Mapping[str, Any],
    *,
    config_sha256: str,
) -> tuple[dict[str, Any], tuple[Path, ...], tuple[Path, ...]]:
    _require_exact_keys(
        config,
        {"contract", "operation_id", "preservation_policy", "roots", "targets"},
        label="config",
    )
    if _require_text(config, "contract", label="config") != CONFIG_CONTRACT:
        raise ResetSafetyError("config gebruikt niet het vaste contract")
    operation_id = _validate_identifier(
        _require_text(config, "operation_id", label="config"),
        label="config.operation_id",
    )
    if (
        _require_text(config, "preservation_policy", label="config")
        != PRESERVATION_POLICY
    ):
        raise ResetSafetyError("config mist het vaste preserveerbeleid")
    root_values = _require_list(config, "roots", label="config")
    target_values = _require_list(config, "targets", label="config")
    if not 1 <= len(root_values) <= MAX_ROOTS:
        raise ResetSafetyError("config bevat geen begrensde rootlijst")
    if not 1 <= len(target_values) <= MAX_TARGETS:
        raise ResetSafetyError("config bevat geen begrensde targetlijst")

    root_records: list[dict[str, str]] = []
    root_paths: list[Path] = []
    roots_by_id: dict[str, Path] = {}
    for index, root_value in enumerate(root_values):
        record, path = _root_from_config(root_value, index=index)
        root_id = record["root_id"]
        if root_id in roots_by_id:
            raise ResetSafetyError("root-id moet uniek zijn")
        roots_by_id[root_id] = path
        root_records.append(record)
        root_paths.append(path)
    _validate_nonoverlap(root_paths, label="canonieke roots")

    target_records: list[dict[str, Any]] = []
    target_paths: list[Path] = []
    target_ids: set[str] = set()
    used_root_ids: set[str] = set()
    for index, target_value in enumerate(target_values):
        record, path = _target_from_config(
            target_value,
            index=index,
            roots_by_id=roots_by_id,
        )
        target_id = record["target_id"]
        if target_id in target_ids:
            raise ResetSafetyError("target-id moet uniek zijn")
        target_ids.add(target_id)
        used_root_ids.add(record["root_id"])
        target_records.append(record)
        target_paths.append(path)
    _validate_nonoverlap(target_paths, label="canonieke targets")
    if used_root_ids != set(roots_by_id):
        raise ResetSafetyError("iedere root moet ten minste een expliciet target hebben")
    for target_path in target_paths:
        if any(_is_within_or_equal(root, target_path) for root in root_paths):
            raise ResetSafetyError("een target mag geen andere root omvatten")

    document = {
        "action": RESET_ACTION,
        "config_sha256": config_sha256,
        "contract": MANIFEST_CONTRACT,
        "created_at_utc": _utc_now(),
        "destructive_action_performed": False,
        "execution_confirmation_required": EXECUTION_CONFIRMATION,
        "operation_id": operation_id,
        "preservation": {
            "domains": list(PRESERVED_DOMAINS),
            "policy": PRESERVATION_POLICY,
        },
        "roots": sorted(root_records, key=lambda item: item["root_id"]),
        "targets": sorted(target_records, key=lambda item: item["target_id"]),
        "worker_stop_required": True,
    }
    return document, tuple(root_paths), tuple(target_paths)


def _open_new_file(path: Path) -> int:
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOINHERIT", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        return os.open(path, flags, 0o600)
    except OSError as error:
        raise ResetSafetyError("nieuw evidencebestand kan niet exclusief worden gemaakt") from error


def _write_reserved_file(descriptor: int, raw: bytes) -> None:
    view = memoryview(raw)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise ResetSafetyError("evidencebestand kon niet volledig worden geschreven")
        view = view[written:]
    os.fsync(descriptor)


def _write_exclusive(path: Path, raw: bytes) -> None:
    descriptor = _open_new_file(path)
    try:
        _write_reserved_file(descriptor, raw)
    finally:
        os.close(descriptor)


def build_reset_manifest(
    *,
    config_path: os.PathLike[str] | str,
    manifest_output: os.PathLike[str] | str,
) -> dict[str, Any]:
    """Write a preview manifest without deleting or modifying any target."""
    config_file, config_raw = _read_bounded_file(
        config_path,
        label="resetconfig",
        maximum_bytes=MAX_CONFIG_BYTES,
    )
    config = _decode_document(config_raw, label="resetconfig")
    document, roots, _targets = _build_manifest_document(
        config,
        config_sha256=_sha256(config_raw),
    )
    _assert_outside_roots(config_file, roots, label="resetconfig")
    output = _new_output_path(manifest_output, label="manifest-output")
    _assert_outside_roots(output, roots, label="manifest-output")
    raw = _canonical_json(document)
    _write_exclusive(output, raw)
    return {
        "destructive_action_performed": False,
        "entry_count": sum(item["entry_count"] for item in document["targets"]),
        "manifest_path": str(output),
        "manifest_sha256": _sha256(raw),
        "target_count": len(document["targets"]),
    }


def _manifest_roots(
    document: Mapping[str, Any],
) -> tuple[dict[str, Path], tuple[Path, ...]]:
    roots_value = _require_list(document, "roots", label="manifest")
    if not 1 <= len(roots_value) <= MAX_ROOTS:
        raise ResetSafetyError("manifest bevat geen begrensde rootlijst")
    roots: dict[str, Path] = {}
    root_paths: list[Path] = []
    for index, value in enumerate(roots_value):
        label = f"manifest.roots[{index}]"
        if not isinstance(value, dict):
            raise ResetSafetyError(f"{label} moet een object zijn")
        _require_exact_keys(
            value,
            {"canonical_path", "root_id", "scope_assertion"},
            label=label,
        )
        root_id = _validate_identifier(
            _require_text(value, "root_id", label=label),
            label=f"{label}.root_id",
        )
        if root_id in roots:
            raise ResetSafetyError("manifest-root-id moet uniek zijn")
        if _require_text(value, "scope_assertion", label=label) != ROOT_SCOPE_ASSERTION:
            raise ResetSafetyError("manifest-root mist de vaste scope-assertie")
        claimed = _require_text(value, "canonical_path", label=label)
        root = _canonical_existing(
            claimed,
            label=f"{label}.canonical_path",
            expected_kind="directory",
        )
        if str(root) != claimed or root.parent == root:
            raise ResetSafetyError("manifest-root is niet exact canoniek")
        protected_reason = _protected_ancestor_reason(root)
        if protected_reason is not None:
            raise ResetSafetyError(
                f"manifest-root ligt onder een beschermde grens: {protected_reason}"
            )
        roots[root_id] = root
        root_paths.append(root)
    _validate_nonoverlap(root_paths, label="manifest-roots")
    if roots_value != sorted(roots_value, key=lambda item: item["root_id"]):
        raise ResetSafetyError("manifest-roots staan niet in canonieke volgorde")
    return roots, tuple(root_paths)


def _manifest_targets(
    document: Mapping[str, Any],
    *,
    roots: Mapping[str, Path],
) -> tuple[list[dict[str, Any]], tuple[Path, ...]]:
    values = _require_list(document, "targets", label="manifest")
    if not 1 <= len(values) <= MAX_TARGETS:
        raise ResetSafetyError("manifest bevat geen begrensde targetlijst")
    actual_records: list[dict[str, Any]] = []
    target_paths: list[Path] = []
    target_ids: set[str] = set()
    used_root_ids: set[str] = set()
    for index, value in enumerate(values):
        label = f"manifest.targets[{index}]"
        if not isinstance(value, dict):
            raise ResetSafetyError(f"{label} moet een object zijn")
        _require_exact_keys(
            value,
            {
                "canonical_path",
                "category",
                "data_assertion",
                "entries",
                "entry_count",
                "expected_kind",
                "relative_path",
                "root_id",
                "target_id",
            },
            label=label,
        )
        target_id = _validate_identifier(
            _require_text(value, "target_id", label=label),
            label=f"{label}.target_id",
        )
        if target_id in target_ids:
            raise ResetSafetyError("manifest-target-id moet uniek zijn")
        target_ids.add(target_id)
        root_id = _validate_identifier(
            _require_text(value, "root_id", label=label),
            label=f"{label}.root_id",
        )
        root = roots.get(root_id)
        if root is None:
            raise ResetSafetyError("manifest-target verwijst naar onbekende root")
        used_root_ids.add(root_id)
        category = _require_text(value, "category", label=label)
        if category not in ALLOWED_TARGET_CATEGORIES:
            raise ResetSafetyError("manifest-target gebruikt verboden datacategorie")
        expected_kind = _require_text(value, "expected_kind", label=label)
        if expected_kind not in ALLOWED_TARGET_KINDS:
            raise ResetSafetyError("manifest-target heeft ongeldig doeltype")
        if _require_text(value, "data_assertion", label=label) != TARGET_DATA_ASSERTION:
            raise ResetSafetyError("manifest-target mist de data-assertie")
        claimed_count = _require_integer(
            value,
            "entry_count",
            label=label,
            minimum=1,
        )
        claimed_entries = _require_list(value, "entries", label=label)
        if claimed_count != len(claimed_entries) or claimed_count > MAX_ENTRIES:
            raise ResetSafetyError("manifest-target heeft geen begrensde entrylijst")
        claimed_relative_paths: set[str] = set()
        for entry_index, entry in enumerate(claimed_entries):
            entry_label = f"{label}.entries[{entry_index}]"
            if not isinstance(entry, dict):
                raise ResetSafetyError(f"{entry_label} moet een object zijn")
            _require_exact_keys(
                entry,
                {
                    "device",
                    "inode",
                    "kind",
                    "mode",
                    "mtime_ns",
                    "relative_path",
                    "size",
                },
                label=entry_label,
            )
            for integer_key in ("device", "inode", "mode", "mtime_ns", "size"):
                _require_integer(entry, integer_key, label=entry_label)
            entry_kind = _require_text(entry, "kind", label=entry_label)
            if entry_kind not in ALLOWED_TARGET_KINDS:
                raise ResetSafetyError("manifest-entry heeft een ongeldig type")
            entry_relative = _require_text(entry, "relative_path", label=entry_label)
            _relative_parts(entry_relative, label=f"{entry_label}.relative_path")
            if entry_relative in claimed_relative_paths:
                raise ResetSafetyError("manifest-entrypad moet uniek zijn")
            claimed_relative_paths.add(entry_relative)
        relative = _require_text(value, "relative_path", label=label)
        parts = _relative_parts(relative, label=f"{label}.relative_path")
        path = _canonical_existing(
            root.joinpath(*parts),
            label=f"{label}.relative_path",
            expected_kind=expected_kind,
        )
        if not _is_strict_descendant(path, root):
            raise ResetSafetyError("manifest-target is geen strikte afstammeling")
        if str(path) != _require_text(value, "canonical_path", label=label):
            raise ResetSafetyError("manifest-target heeft gewijzigde canonicalisatie")
        entries = _scan_target(path, root, target_id=target_id)
        actual = {
            "canonical_path": str(path),
            "category": category,
            "data_assertion": TARGET_DATA_ASSERTION,
            "entries": entries,
            "entry_count": len(entries),
            "expected_kind": expected_kind,
            "relative_path": _portable_relative(path, root),
            "root_id": root_id,
            "target_id": target_id,
        }
        if value != actual:
            raise ResetSafetyError(
                "targetinventaris wijkt af van het verzegelde manifest"
            )
        actual_records.append(actual)
        target_paths.append(path)
    _validate_nonoverlap(target_paths, label="manifest-targets")
    if used_root_ids != set(roots):
        raise ResetSafetyError("iedere manifest-root moet worden gebruikt")
    if values != sorted(values, key=lambda item: item["target_id"]):
        raise ResetSafetyError("manifest-targets staan niet in canonieke volgorde")
    for target in target_paths:
        if any(_is_within_or_equal(root, target) for root in roots.values()):
            raise ResetSafetyError("manifest-target omvat een root")
    return actual_records, tuple(target_paths)


def _validate_manifest_document(
    document: Mapping[str, Any],
) -> tuple[dict[str, Path], tuple[Path, ...], list[dict[str, Any]], tuple[Path, ...]]:
    _require_exact_keys(
        document,
        {
            "action",
            "config_sha256",
            "contract",
            "created_at_utc",
            "destructive_action_performed",
            "execution_confirmation_required",
            "operation_id",
            "preservation",
            "roots",
            "targets",
            "worker_stop_required",
        },
        label="manifest",
    )
    if _require_text(document, "contract", label="manifest") != MANIFEST_CONTRACT:
        raise ResetSafetyError("manifest gebruikt niet het vaste contract")
    if _require_text(document, "action", label="manifest") != RESET_ACTION:
        raise ResetSafetyError("manifest vraagt geen toegestane actie")
    if document.get("destructive_action_performed") is not False:
        raise ResetSafetyError("manifest is geen zuivere preview")
    if document.get("worker_stop_required") is not True:
        raise ResetSafetyError("manifest bindt de worker-stopvoorwaarde niet")
    if (
        _require_text(document, "execution_confirmation_required", label="manifest")
        != EXECUTION_CONFIRMATION
    ):
        raise ResetSafetyError("manifest bindt niet de vaste bevestiging")
    _validate_identifier(
        _require_text(document, "operation_id", label="manifest"),
        label="manifest.operation_id",
    )
    if _SHA256.fullmatch(
        _require_text(document, "config_sha256", label="manifest")
    ) is None:
        raise ResetSafetyError("manifest bevat geen geldige config-SHA-256")
    _require_text(document, "created_at_utc", label="manifest")
    preservation = document.get("preservation")
    expected_preservation = {
        "domains": list(PRESERVED_DOMAINS),
        "policy": PRESERVATION_POLICY,
    }
    if preservation != expected_preservation:
        raise ResetSafetyError("manifest wijzigt de preserveergrenzen")
    roots, root_paths = _manifest_roots(document)
    targets, target_paths = _manifest_targets(document, roots=roots)
    return roots, root_paths, targets, target_paths


def _entry_path(
    entry: Mapping[str, Any],
    *,
    root: Path,
    target: Path,
    target_id: str,
) -> Path:
    relative = entry.get("relative_path")
    if not isinstance(relative, str):
        raise ResetSafetyError("manifest-entry mist een relatief pad")
    parts = _relative_parts(relative, label="manifest-entry.relative_path")
    path = root.joinpath(*parts)
    if not _is_within_or_equal(path, target) or not _is_strict_descendant(path, root):
        raise ResetSafetyError(f"entry van target {target_id} verlaat zijn target")
    return path


def _assert_current_file(
    path: Path,
    expected: Mapping[str, Any],
    *,
    root: Path,
    target_id: str,
) -> None:
    actual = _snapshot_entry(path, root, target_id=target_id)
    if actual != expected or actual["kind"] != "file":
        raise ResetSafetyError("bestand wijzigde na de volledige preflight")


def _assert_current_directory(
    path: Path,
    expected: Mapping[str, Any],
    *,
    root: Path,
    target_id: str,
) -> None:
    actual = _snapshot_entry(path, root, target_id=target_id)
    stable_fields = ("device", "inode", "kind", "mode", "relative_path")
    if actual["kind"] != "directory" or any(
        actual[field] != expected.get(field) for field in stable_fields
    ):
        raise ResetSafetyError("map wijzigde na de volledige preflight")


def _delete_targets(
    targets: Sequence[Mapping[str, Any]],
    *,
    roots: Mapping[str, Path],
    deleted: list[str],
) -> None:
    for target_record in targets:
        target_id = str(target_record["target_id"])
        root = roots[str(target_record["root_id"])]
        target = Path(str(target_record["canonical_path"]))
        entries = list(target_record["entries"])
        files = sorted(
            (item for item in entries if item["kind"] == "file"),
            key=lambda item: (-len(PurePosixPath(item["relative_path"]).parts), item["relative_path"]),
        )
        directories = sorted(
            (item for item in entries if item["kind"] == "directory"),
            key=lambda item: (-len(PurePosixPath(item["relative_path"]).parts), item["relative_path"]),
        )
        for entry in files:
            path = _entry_path(
                entry,
                root=root,
                target=target,
                target_id=target_id,
            )
            _assert_current_file(
                path,
                entry,
                root=root,
                target_id=target_id,
            )
            try:
                os.unlink(path)
            except OSError as error:
                raise ResetSafetyError("expliciet bestand kon niet worden verwijderd") from error
            deleted.append(f"{target_id}:{entry['relative_path']}")
        for entry in directories:
            path = _entry_path(
                entry,
                root=root,
                target=target,
                target_id=target_id,
            )
            _assert_current_directory(
                path,
                entry,
                root=root,
                target_id=target_id,
            )
            try:
                os.rmdir(path)
            except OSError as error:
                raise ResetSafetyError("expliciete map was niet veilig leeg") from error
            deleted.append(f"{target_id}:{entry['relative_path']}")


def execute_reset_manifest(
    *,
    manifest_path: os.PathLike[str] | str,
    manifest_sha256: str,
    confirmation: str,
    execution_evidence_output: os.PathLike[str] | str,
) -> dict[str, Any]:
    """Execute a fully revalidated manifest and preserve separate evidence."""
    if _SHA256.fullmatch(manifest_sha256) is None:
        raise ResetSafetyError("manifest-SHA-256 moet exact 64 kleine hextekens zijn")
    if confirmation != EXECUTION_CONFIRMATION:
        raise ResetSafetyError("expliciete uitvoeringsbevestiging ontbreekt")
    manifest_file, raw = _read_bounded_file(
        manifest_path,
        label="resetmanifest",
        maximum_bytes=MAX_MANIFEST_BYTES,
    )
    if _sha256(raw) != manifest_sha256:
        raise ResetSafetyError("manifest-SHA-256 komt niet exact overeen")
    document = _decode_document(raw, label="resetmanifest")
    if raw != _canonical_json(document):
        raise ResetSafetyError("resetmanifest heeft geen canonieke bytevorm")
    roots, root_paths, targets, _target_paths = _validate_manifest_document(document)
    _assert_outside_roots(manifest_file, root_paths, label="resetmanifest")
    evidence_path = _new_output_path(
        execution_evidence_output,
        label="uitvoeringsevidence-output",
    )
    _assert_outside_roots(
        evidence_path,
        root_paths,
        label="uitvoeringsevidence-output",
    )
    if evidence_path == manifest_file:
        raise ResetSafetyError("uitvoeringsevidence mag niet het manifest zijn")

    descriptor = _open_new_file(evidence_path)
    deleted: list[str] = []
    try:
        try:
            _delete_targets(targets, roots=roots, deleted=deleted)
        except Exception as error:
            failure = {
                "completed": False,
                "contract": EXECUTION_EVIDENCE_CONTRACT,
                "deleted_entry_count": len(deleted),
                "failure_class": type(error).__name__,
                "manifest_sha256": manifest_sha256,
                "operation_id": document["operation_id"],
                "recorded_at_utc": _utc_now(),
            }
            _write_reserved_file(descriptor, _canonical_json(failure))
            if isinstance(error, ResetSafetyError):
                raise
            raise ResetSafetyError("resetuitvoering stopte fail-closed") from error
        success = {
            "completed": True,
            "contract": EXECUTION_EVIDENCE_CONTRACT,
            "deleted_entry_count": len(deleted),
            "failure_class": None,
            "manifest_sha256": manifest_sha256,
            "operation_id": document["operation_id"],
            "recorded_at_utc": _utc_now(),
        }
        success_raw = _canonical_json(success)
        _write_reserved_file(descriptor, success_raw)
    finally:
        os.close(descriptor)
    return {
        "deleted_entry_count": len(deleted),
        "destructive_action_performed": True,
        "execution_evidence_path": str(evidence_path),
        "execution_evidence_sha256": _sha256(success_raw),
        "manifest_sha256": manifest_sha256,
        "target_count": len(targets),
    }


def _arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Maak standaard alleen een fail-closed resetmanifest. Uitvoering "
            "vereist daarnaast --execute, een exact manifest-hash, de vaste "
            "bevestiging en een nieuw evidencepad."
        )
    )
    parser.add_argument("--config", type=Path)
    parser.add_argument("--manifest-output", type=Path)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--execute-manifest", type=Path)
    parser.add_argument("--manifest-sha256")
    parser.add_argument("--confirm")
    parser.add_argument("--execution-evidence-output", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _arguments(argv)
    if not args.execute:
        if args.config is None or args.manifest_output is None:
            raise ResetSafetyError(
                "preview vereist --config en --manifest-output"
            )
        if any((
            args.execute_manifest is not None,
            args.manifest_sha256 is not None,
            args.confirm is not None,
            args.execution_evidence_output is not None,
        )):
            raise ResetSafetyError("preview accepteert geen uitvoeringsargumenten")
        result = build_reset_manifest(
            config_path=args.config,
            manifest_output=args.manifest_output,
        )
    else:
        if args.config is not None or args.manifest_output is not None:
            raise ResetSafetyError("uitvoering accepteert geen previewconfig")
        if any((
            args.execute_manifest is None,
            args.manifest_sha256 is None,
            args.confirm is None,
            args.execution_evidence_output is None,
        )):
            raise ResetSafetyError(
                "uitvoering vereist manifest, hash, bevestiging en evidence-output"
            )
        result = execute_reset_manifest(
            manifest_path=args.execute_manifest,
            manifest_sha256=args.manifest_sha256,
            confirmation=args.confirm,
            execution_evidence_output=args.execution_evidence_output,
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ResetSafetyError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
