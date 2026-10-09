"""Read-only service runtime selection for worker-owned service roots.

This module only selects a mode and, for an explicit worker-owned mode,
validates one content-addressed configuration document into an existing typed
configuration object.  It does not construct a service/runtime/store, open an
authority, acquire ownership, start a thread, connect to MQTT, or write state.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
from typing import TYPE_CHECKING, Any, Final, Literal

if TYPE_CHECKING:
    from .raw_capture_service_composition import RawCaptureServiceConfig
    from .worker_first_service_composition import WorkerFirstServiceCanaryConfig


ACTIVATION_CONTRACT: Final = "stv3-worker-first-service-activation-plan-v1"
RUNTIME_MODE_ENV: Final = "STV3_RUNTIME_MODE"
CONFIG_PATH_ENV: Final = "STV3_WORKER_FIRST_CONFIG_PATH"
CONFIG_SHA256_ENV: Final = "STV3_WORKER_FIRST_CONFIG_SHA256"
AUTHORITY_SHA256_ENV: Final = "STV3_WORKER_FIRST_AUTHORITY_SHA256"

LEGACY_MODE: Final = "legacy"
WORKER_FIRST_CANARY_MODE: Final = "worker_first_canary"
RAW_CAPTURE_SHADOW_MODE: Final = "raw_capture_shadow"
WORKER_ONLY_MODE: Final = "worker_only"
RUNTIME_MODES: Final = frozenset({
    LEGACY_MODE,
    RAW_CAPTURE_SHADOW_MODE,
    WORKER_ONLY_MODE,
    WORKER_FIRST_CANARY_MODE,
})
# Pinned to worker_first_service_composition.MAX_CONFIG_BYTES.  Keep this
# local so importing/selecting the default legacy mode does not import the
# worker-first composition graph merely to obtain a resource-limit constant.
MAX_ACTIVATION_CONFIG_BYTES: Final = 384 * 1024
MAX_CONFIG_PATH_CHARS: Final = 4096
MAX_JSON_DEPTH: Final = 32
MAX_JSON_NODES: Final = 20_000
MAX_JSON_KEY_CHARS: Final = 128
MAX_JSON_STRING_CHARS: Final = 8192
MAX_JSON_INTEGER_DIGITS: Final = 20
MAX_JSON_FLOAT_CHARS: Final = 64
MAX_JSON_FLOAT_ABS_EXPONENT: Final = 308

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CANARY_ENV_KEYS = (
    CONFIG_PATH_ENV,
    CONFIG_SHA256_ENV,
    AUTHORITY_SHA256_ENV,
)
_CANARY_CONFIG_ERROR_TRANSLATIONS: Final = {
    "canary_service_config_schema_invalid": "service_config_schema_invalid",
    "canary_acquisition_schema_invalid": "service_acquisition_schema_invalid",
    "canary_source_paths_schema_invalid": "service_source_paths_schema_invalid",
    "canary_retention_schema_invalid": "service_retention_schema_invalid",
    "canary_timeouts_schema_invalid": "service_timeouts_schema_invalid",
    "canary_service_config_authority_invalid": (
        "service_config_authority_invalid"
    ),
    "canary_capacity_profile_schema_invalid": (
        "capacity_profile_schema_invalid"
    ),
    "canary_capacity_providers_invalid": "capacity_providers_invalid",
    "canary_capacity_provider_schema_invalid": (
        "capacity_provider_schema_invalid"
    ),
    "canary_capacity_profile_invalid": "capacity_profile_invalid",
    "canary_capacity_profile_identity_mismatch": (
        "capacity_profile_identity_mismatch"
    ),
    "canary_raw_store_limits_schema_invalid": (
        "raw_store_limits_schema_invalid"
    ),
    "canary_raw_store_limits_invalid": "raw_store_limits_invalid",
    "canary_raw_store_limits_identity_mismatch": (
        "raw_store_limits_identity_mismatch"
    ),
    "canary_service_config_identity_mismatch": (
        "service_config_identity_mismatch"
    ),
}


class WorkerFirstServiceActivationError(RuntimeError):
    """The inert runtime-selection input differs from its closed contract."""


class _DuplicateJsonKey(ValueError):
    pass


class _InvalidJsonNumber(ValueError):
    pass


@dataclass(frozen=True, slots=True, repr=False)
class WorkerFirstServiceActivationPlan:
    """Validated data only; this object has no activation methods.

    ``config_path`` is provenance metadata for the completed read.  A later
    phase must consume ``composition_config`` and must never reopen that path.
    """

    runtime_mode: Literal[
        "legacy",
        "raw_capture_shadow",
        "worker_only",
        "worker_first_canary",
    ]
    config_path: Path | None
    config_sha256: str | None
    authority_sha256: str | None
    composition_config: (
        WorkerFirstServiceCanaryConfig | RawCaptureServiceConfig | None
    )

    @property
    def worker_first_selected(self) -> bool:
        return self.runtime_mode == WORKER_FIRST_CANARY_MODE

    @property
    def raw_capture_selected(self) -> bool:
        return self.runtime_mode == RAW_CAPTURE_SHADOW_MODE

    @property
    def worker_only_selected(self) -> bool:
        return self.runtime_mode == WORKER_ONLY_MODE

    @property
    def isolated_runtime_selected(self) -> bool:
        return (
            self.worker_first_selected
            or self.raw_capture_selected
            or self.worker_only_selected
        )

    def to_mapping(self) -> dict[str, object]:
        result = {
            "activation_performed": False,
            "authority_sha256": self.authority_sha256,
            "config_loaded": self.composition_config is not None,
            "config_path": (
                None if self.config_path is None else str(self.config_path)
            ),
            "config_sha256": self.config_sha256,
            "contract": ACTIVATION_CONTRACT,
            "runtime_mode": self.runtime_mode,
            "worker_first_selected": self.worker_first_selected,
        }
        # Preserve the exact v1 legacy/canary diagnostic shape.  The new mode
        # identifies its distinct root explicitly without relabelling the DWD
        # canary as a general raw-capture service.
        if self.raw_capture_selected:
            result["raw_capture_selected"] = True
        if self.worker_only_selected:
            result["worker_only_selected"] = True
        return result


def _sha256(value: object, error_code: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise WorkerFirstServiceActivationError(error_code)
    return value


def _is_reparse(metadata: os.stat_result) -> bool:
    flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(flag and int(getattr(metadata, "st_file_attributes", 0)) & flag)


def _config_path(value: object) -> Path:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > MAX_CONFIG_PATH_CHARS
        or "\x00" in value
    ):
        raise WorkerFirstServiceActivationError("config_path_invalid")
    supplied = Path(value)
    if not supplied.is_absolute() or supplied == Path(supplied.anchor):
        raise WorkerFirstServiceActivationError("config_path_invalid")
    try:
        resolved = supplied.resolve(strict=True)
    except (OSError, RuntimeError):
        raise WorkerFirstServiceActivationError("config_path_unavailable") from None
    if value != str(resolved):
        raise WorkerFirstServiceActivationError("config_path_not_canonical")
    try:
        for component in (supplied, *supplied.parents):
            metadata = component.lstat()
            if stat.S_ISLNK(metadata.st_mode) or _is_reparse(metadata):
                raise WorkerFirstServiceActivationError("config_path_unsafe")
    except OSError:
        raise WorkerFirstServiceActivationError("config_path_unavailable") from None
    try:
        metadata = supplied.lstat()
    except OSError:
        raise WorkerFirstServiceActivationError("config_path_unavailable") from None
    if not stat.S_ISREG(metadata.st_mode):
        raise WorkerFirstServiceActivationError("config_path_not_regular")
    return resolved


def _file_witness(
    metadata: os.stat_result,
) -> tuple[int, int, int, int, int]:
    return (
        int(metadata.st_dev),
        int(metadata.st_ino),
        int(metadata.st_mode),
        int(metadata.st_size),
        int(metadata.st_mtime_ns),
    )


def _parent_witness(path: Path) -> tuple[int, int, int]:
    try:
        metadata = path.lstat()
    except OSError:
        raise WorkerFirstServiceActivationError("config_file_changed") from None
    if (
        stat.S_ISLNK(metadata.st_mode)
        or _is_reparse(metadata)
        or not stat.S_ISDIR(metadata.st_mode)
    ):
        raise WorkerFirstServiceActivationError("config_path_unsafe")
    return (
        int(metadata.st_dev),
        int(metadata.st_ino),
        int(metadata.st_mode),
    )


def _read_config_bytes(path: Path) -> bytes:
    try:
        parent_before = _parent_witness(path.parent)
        before = path.lstat()
        if (
            stat.S_ISLNK(before.st_mode)
            or _is_reparse(before)
            or not stat.S_ISREG(before.st_mode)
        ):
            raise WorkerFirstServiceActivationError("config_path_unsafe")
        if not 0 < before.st_size <= MAX_ACTIVATION_CONFIG_BYTES:
            raise WorkerFirstServiceActivationError("config_size_invalid")
        with path.open("rb") as source:
            opened = os.fstat(source.fileno())
            if (
                not stat.S_ISREG(opened.st_mode)
                or _file_witness(opened) != _file_witness(before)
            ):
                raise WorkerFirstServiceActivationError("config_file_changed")
            payload = source.read(MAX_ACTIVATION_CONFIG_BYTES + 1)
            after = os.fstat(source.fileno())
        try:
            final = path.lstat()
        except OSError:
            raise WorkerFirstServiceActivationError(
                "config_file_changed"
            ) from None
        parent_after = _parent_witness(path.parent)
        if (
            _file_witness(after) != _file_witness(opened)
            or stat.S_ISLNK(final.st_mode)
            or _is_reparse(final)
            or not stat.S_ISREG(final.st_mode)
            or _file_witness(final) != _file_witness(opened)
            or parent_after != parent_before
            or len(payload) != opened.st_size
        ):
            raise WorkerFirstServiceActivationError("config_file_changed")
    except WorkerFirstServiceActivationError:
        raise
    except OSError:
        raise WorkerFirstServiceActivationError("config_read_failed") from None
    return payload


def _decode_canonical_json(payload: bytes) -> dict[str, Any]:
    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise _DuplicateJsonKey(key)
            result[key] = value
        return result

    def bounded_integer(token: str) -> int:
        digits = token[1:] if token.startswith("-") else token
        if not digits or len(digits) > MAX_JSON_INTEGER_DIGITS:
            raise _InvalidJsonNumber(token)
        return int(token)

    def bounded_float(token: str) -> float:
        if len(token) > MAX_JSON_FLOAT_CHARS:
            raise _InvalidJsonNumber(token)
        _mantissa, separator, raw_exponent = token.lower().partition("e")
        if separator:
            try:
                exponent = int(raw_exponent)
            except (ValueError, OverflowError):
                raise _InvalidJsonNumber(token) from None
            if abs(exponent) > MAX_JSON_FLOAT_ABS_EXPONENT:
                raise _InvalidJsonNumber(token)
        try:
            result = float(token)
        except (ValueError, OverflowError):
            raise _InvalidJsonNumber(token) from None
        if not math.isfinite(result):
            raise _InvalidJsonNumber(token)
        try:
            canonical_token = json.dumps(
                result,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
            )
        except (TypeError, ValueError, OverflowError):
            raise _InvalidJsonNumber(token) from None
        if canonical_token != token:
            raise _InvalidJsonNumber(token)
        return result

    def reject_constant(value: str) -> None:
        raise _InvalidJsonNumber(value)

    try:
        value = json.loads(
            payload.decode("ascii"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
            parse_float=bounded_float,
            parse_int=bounded_integer,
        )
    except _DuplicateJsonKey:
        raise WorkerFirstServiceActivationError("config_json_duplicate_key") from None
    except _InvalidJsonNumber:
        raise WorkerFirstServiceActivationError("config_json_number_invalid") from None
    except (UnicodeError, ValueError, TypeError, RecursionError, ArithmeticError):
        raise WorkerFirstServiceActivationError("config_json_invalid") from None
    if not isinstance(value, dict):
        raise WorkerFirstServiceActivationError("config_json_root_invalid")
    _validate_json_shape(value)
    try:
        canonical = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (
        TypeError,
        ValueError,
        UnicodeError,
        RecursionError,
        ArithmeticError,
    ):
        raise WorkerFirstServiceActivationError("config_json_invalid") from None
    if payload != canonical:
        raise WorkerFirstServiceActivationError("config_json_not_canonical")
    return value


def _validate_json_shape(value: dict[str, Any]) -> None:
    nodes = 0
    pending: list[tuple[object, int]] = [(value, 1)]
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if depth > MAX_JSON_DEPTH or nodes > MAX_JSON_NODES:
            raise WorkerFirstServiceActivationError(
                "config_json_resource_limit"
            )
        if isinstance(item, dict):
            for key, nested in item.items():
                if len(key) > MAX_JSON_KEY_CHARS:
                    raise WorkerFirstServiceActivationError(
                        "config_json_resource_limit"
                    )
                pending.append((nested, depth + 1))
        elif isinstance(item, list):
            pending.extend((nested, depth + 1) for nested in item)
        elif isinstance(item, str) and len(item) > MAX_JSON_STRING_CHARS:
            raise WorkerFirstServiceActivationError(
                "config_json_resource_limit"
            )


def _parse_service_config(
    value: object,
) -> WorkerFirstServiceCanaryConfig:
    # Import the typed canary authority only after the caller has explicitly
    # selected canary mode and its pinned bytes have passed path/hash/JSON
    # validation.  Default legacy selection therefore stays import-inert.
    from .worker_first_service_composition import (
        WorkerFirstServiceCanaryConfig,
        WorkerFirstServiceCompositionConfigError,
    )

    try:
        result = WorkerFirstServiceCanaryConfig.from_mapping(value)
    except WorkerFirstServiceCompositionConfigError as error:
        error_code = _CANARY_CONFIG_ERROR_TRANSLATIONS.get(
            str(error),
            "service_config_invalid",
        )
        raise WorkerFirstServiceActivationError(error_code) from None
    except (TypeError, ValueError, ArithmeticError):
        raise WorkerFirstServiceActivationError("service_config_invalid") from None
    if result.to_mapping() != value:
        raise WorkerFirstServiceActivationError("service_config_identity_mismatch")
    return result


def _parse_raw_capture_config(value: object) -> RawCaptureServiceConfig:
    # Like the canary parser above, keep the raw-capture graph import-inert
    # until pinned canonical bytes selected its explicit runtime mode.
    from .raw_capture_service_composition import (
        RawCaptureServiceConfig,
        RawCaptureServiceConfigError,
    )

    try:
        result = RawCaptureServiceConfig.from_mapping(value)
    except RawCaptureServiceConfigError as error:
        code = str(error)
        if not code or len(code) > 96 or not code.replace("_", "a").isalnum():
            code = "raw_capture_config_invalid"
        raise WorkerFirstServiceActivationError(code) from None
    except (TypeError, ValueError, ArithmeticError):
        raise WorkerFirstServiceActivationError(
            "raw_capture_config_invalid"
        ) from None
    if result.to_mapping() != value:
        raise WorkerFirstServiceActivationError(
            "raw_capture_config_identity_mismatch"
        )
    return result


def _environment_value(
    environment: Mapping[str, str],
    name: str,
    error_code: str,
) -> str:
    if name not in environment:
        raise WorkerFirstServiceActivationError(error_code)
    value = environment[name]
    if not isinstance(value, str) or not value or value != value.strip():
        raise WorkerFirstServiceActivationError(error_code)
    return value


def load_worker_first_service_activation(
    environment: Mapping[str, str] | None = None,
) -> WorkerFirstServiceActivationPlan:
    """Select legacy or load one pinned service config without activating it."""

    source = os.environ if environment is None else environment
    if not isinstance(source, Mapping):
        raise WorkerFirstServiceActivationError("environment_invalid")
    if RUNTIME_MODE_ENV not in source:
        mode = LEGACY_MODE
    else:
        raw_mode = source[RUNTIME_MODE_ENV]
        if (
            not isinstance(raw_mode, str)
            or not raw_mode
            or raw_mode != raw_mode.strip()
            or raw_mode not in RUNTIME_MODES
        ):
            raise WorkerFirstServiceActivationError("runtime_mode_invalid")
        mode = raw_mode

    if mode == LEGACY_MODE:
        if any(name in source for name in _CANARY_ENV_KEYS):
            raise WorkerFirstServiceActivationError(
                "legacy_canary_configuration_forbidden"
            )
        return WorkerFirstServiceActivationPlan(
            runtime_mode=LEGACY_MODE,
            config_path=None,
            config_sha256=None,
            authority_sha256=None,
            composition_config=None,
        )

    path = _config_path(_environment_value(
        source,
        CONFIG_PATH_ENV,
        "config_path_required",
    ))
    expected_config_sha256 = _sha256(
        _environment_value(
            source,
            CONFIG_SHA256_ENV,
            "config_sha256_required",
        ),
        "config_sha256_invalid",
    )
    authority_sha256 = _sha256(
        _environment_value(
            source,
            AUTHORITY_SHA256_ENV,
            "authority_sha256_required",
        ),
        "authority_sha256_invalid",
    )
    payload = _read_config_bytes(path)
    actual_config_sha256 = hashlib.sha256(payload).hexdigest()
    if actual_config_sha256 != expected_config_sha256:
        raise WorkerFirstServiceActivationError("config_sha256_mismatch")
    decoded = _decode_canonical_json(payload)
    config = (
        _parse_raw_capture_config(decoded)
        if mode in {RAW_CAPTURE_SHADOW_MODE, WORKER_ONLY_MODE}
        else _parse_service_config(decoded)
    )
    if config.fingerprint != actual_config_sha256:
        raise WorkerFirstServiceActivationError("config_identity_mismatch")
    return WorkerFirstServiceActivationPlan(
        runtime_mode=mode,
        config_path=path,
        config_sha256=actual_config_sha256,
        authority_sha256=authority_sha256,
        composition_config=config,
    )


__all__ = [
    "ACTIVATION_CONTRACT",
    "AUTHORITY_SHA256_ENV",
    "CONFIG_PATH_ENV",
    "CONFIG_SHA256_ENV",
    "LEGACY_MODE",
    "MAX_ACTIVATION_CONFIG_BYTES",
    "MAX_JSON_DEPTH",
    "MAX_JSON_FLOAT_ABS_EXPONENT",
    "MAX_JSON_FLOAT_CHARS",
    "MAX_JSON_INTEGER_DIGITS",
    "MAX_JSON_KEY_CHARS",
    "MAX_JSON_NODES",
    "MAX_JSON_STRING_CHARS",
    "RAW_CAPTURE_SHADOW_MODE",
    "RUNTIME_MODE_ENV",
    "RUNTIME_MODES",
    "WORKER_FIRST_CANARY_MODE",
    "WORKER_ONLY_MODE",
    "WorkerFirstServiceActivationError",
    "WorkerFirstServiceActivationPlan",
    "load_worker_first_service_activation",
]
