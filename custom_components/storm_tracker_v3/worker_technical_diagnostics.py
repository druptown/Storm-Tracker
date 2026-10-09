"""Bounded, privacy-safe worker control-plane diagnostics projection.

This module is deliberately pure and unwired.  It accepts one already bounded
technical source mapping and returns a fixed-shape read model.  It performs no
Home Assistant, MQTT, provider, filesystem, database or network work and never
derives meteorological truth, admission or alert decisions.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from itertools import islice
import json
import math
import re
from typing import Any


CONTRACT = "stv3-ha-worker-technical-diagnostics-v1"
SOURCE_CONTRACT = "stv3-worker-technical-diagnostics-source-v1"
MAX_DIAGNOSTIC_BYTES = 65_536
MAX_REASON_CODES = 16
MAX_COUNTER = 1_000_000_000
MAX_AGE_SECONDS = 31_536_000.0
MAX_HEADROOM_BYTES = 2**63 - 1
MAX_TIMESTAMP = 253_402_300_799.0
WORKER_STALE_AFTER_SECONDS = 35.0
MAX_FUTURE_SKEW_SECONDS = 5.0

SOURCE_FIELDS = frozenset({
    "build_sha256",
    "cache_age_seconds",
    "cache_status",
    "capacity_alarm",
    "config_sha256",
    "contract",
    "current_version",
    "desired_state_sha256",
    "failed_jobs",
    "frame_age_seconds",
    "frame_status",
    "headroom_bytes",
    "headroom_percent",
    "next_version",
    "provider_age_seconds",
    "provider_status",
    "queue_depth",
    "replay_algorithm_version",
    "replay_contract_version",
    "retention_alarm",
    "storage_reason_codes",
    "terminal_jobs",
    "transition_reason_codes",
    "transition_status",
    "worker_observed_at",
    "worker_state",
    "worker_version",
})

_LIVE_WORKER_STATES = frozenset({"online", "busy", "degraded"})
_WORKER_STATES = frozenset({
    "waiting", "online", "busy", "degraded", "offline", "unknown",
})
_FRESHNESS_STATES = frozenset({"current", "stale", "unknown"})
_ALARM_STATES = frozenset({"clear", "warning", "critical", "unknown"})
_TRANSITION_STATES = frozenset({
    "stable", "pending", "blocked", "applying", "unknown",
})
_SHA256_RE = re.compile(r"(?:sha256:)?([0-9a-fA-F]{64})")
_VERSION_RE = re.compile(
    r"(?:v?[0-9]+(?:\.[0-9]+){0,3}|[A-Za-z0-9][A-Za-z0-9._+-]{0,55}-v[0-9]{1,6})"
)
_REASON_RE = re.compile(r"[a-z][a-z0-9_.:-]{0,63}")
_INVALID = object()


def canonical_diagnostics_bytes(value: Mapping[str, Any]) -> bytes:
    """Return the exact bounded-wire representation used by this contract."""

    return json.dumps(
        dict(value),
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _get(source: Mapping[str, Any], key: str) -> Any:
    try:
        return source.get(key)
    except Exception:
        return None


def _timestamp(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if type(value) is int and value.bit_length() > 48:
        return None
    try:
        result = float(value)
    except (OverflowError, ValueError):
        return None
    if not math.isfinite(result) or result < 0.0:
        return None
    if result > MAX_TIMESTAMP:
        return None
    return result


def _bounded_age(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if type(value) is int and value.bit_length() > 26:
        return None
    try:
        result = float(value)
    except (OverflowError, ValueError):
        return None
    if not math.isfinite(result) or not 0.0 <= result <= MAX_AGE_SECONDS:
        return None
    return round(result, 3)


def _bounded_counter(value: Any) -> tuple[int | None, bool]:
    if type(value) is not int or value < 0:
        return None, False
    if value.bit_length() > MAX_COUNTER.bit_length():
        return MAX_COUNTER, True
    return min(value, MAX_COUNTER), value > MAX_COUNTER


def _bounded_headroom(value: Any) -> int | None:
    if (
        type(value) is not int
        or value < 0
        or value.bit_length() > MAX_HEADROOM_BYTES.bit_length()
        or value > MAX_HEADROOM_BYTES
    ):
        return None
    return value


def _bounded_percent(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if type(value) is int and value.bit_length() > 7:
        return None
    try:
        result = float(value)
    except (OverflowError, ValueError):
        return None
    if not math.isfinite(result) or not 0.0 <= result <= 100.0:
        return None
    return round(result, 3)


def _enum(value: Any, allowed: frozenset[str], default: str) -> str:
    return (
        value
        if isinstance(value, str) and len(value) <= 16 and value in allowed
        else default
    )


def _version(value: Any) -> str | None:
    if (
        not isinstance(value, str)
        or len(value) > 64
        or value == "unknown"
        or _VERSION_RE.fullmatch(value) is None
    ):
        return None
    return value


def _content_address(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    if len(value) not in {64, 71}:
        return None
    match = _SHA256_RE.fullmatch(value)
    if match is None:
        return None
    return f"sha256:{match.group(1).lower()}"


def _reason_codes(value: Any) -> tuple[list[str], bool]:
    """Read at most seventeen items, retaining at most sixteen safe codes."""

    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Iterable
    ):
        return [], False
    try:
        sampled = list(islice(iter(value), MAX_REASON_CODES + 1))
    except Exception:
        return [], False
    complete = len(sampled) <= MAX_REASON_CODES
    bounded = sampled[:MAX_REASON_CODES]
    safe: list[str] = []
    for item in bounded:
        if (
            not isinstance(item, str)
            or len(item) > 64
            or _REASON_RE.fullmatch(item) is None
        ):
            complete = False
            continue
        if item in safe:
            complete = False
            continue
        safe.append(item)
    return sorted(safe), complete


def _strict_optional(value: Any, normalizer) -> Any:
    if value is None:
        return None
    normalized = normalizer(value)
    return _INVALID if normalized is None else normalized


def _strict_optional_counter(value: Any) -> int | None | object:
    if value is None:
        return None
    if type(value) is not int or not 0 <= value <= MAX_COUNTER:
        return _INVALID
    return value


def _strict_optional_alarm(value: Any) -> str | None | object:
    if value is None:
        return None
    if isinstance(value, bool):
        return "critical" if value else "clear"
    normalized = _enum(value, _ALARM_STATES, "")
    return normalized if normalized else _INVALID


def _strict_reason_source(value: Any) -> list[str] | None | object:
    if value is None:
        return None
    if not isinstance(value, list) or len(value) > MAX_REASON_CODES:
        return _INVALID
    safe, complete = _reason_codes(value)
    if not complete or len(safe) != len(value):
        return _INVALID
    return safe


def normalize_worker_technical_source(
    source: Mapping[str, Any] | None,
    *,
    worker_state: str,
    observed_at: float,
) -> dict[str, Any] | None:
    """Validate one exact wire source and bind its liveness locally.

    The returned mapping has exactly :data:`SOURCE_FIELDS`, contains no free
    text or caller-owned containers and is safe to retain for a later stale
    re-projection.  Wire liveness is validated but never trusted: the bridge's
    compact state and receive timestamp replace it.
    """

    if not isinstance(source, Mapping):
        return None
    try:
        exact_shape = len(source) == len(SOURCE_FIELDS) and all(
            key in source for key in SOURCE_FIELDS
        )
    except Exception:
        return None
    if not exact_shape or _get(source, "contract") != SOURCE_CONTRACT:
        return None

    wire_state = _enum(_get(source, "worker_state"), _WORKER_STATES, "")
    wire_observed_at = _timestamp(_get(source, "worker_observed_at"))
    bound_state = _enum(worker_state, _WORKER_STATES, "")
    bound_observed_at = _timestamp(observed_at)
    if (
        not wire_state
        or wire_observed_at is None
        or not bound_state
        or bound_observed_at is None
    ):
        return None

    normalized: dict[str, Any] = {
        "contract": SOURCE_CONTRACT,
        "worker_state": bound_state,
        "worker_observed_at": bound_observed_at,
        "worker_version": _strict_optional(
            _get(source, "worker_version"), _version
        ),
        "build_sha256": _strict_optional(
            _get(source, "build_sha256"), _content_address
        ),
        "config_sha256": _strict_optional(
            _get(source, "config_sha256"), _content_address
        ),
        "desired_state_sha256": _strict_optional(
            _get(source, "desired_state_sha256"), _content_address
        ),
        "provider_age_seconds": _strict_optional(
            _get(source, "provider_age_seconds"), _bounded_age
        ),
        "provider_status": _enum(
            _get(source, "provider_status"), _FRESHNESS_STATES, ""
        ),
        "frame_age_seconds": _strict_optional(
            _get(source, "frame_age_seconds"), _bounded_age
        ),
        "frame_status": _enum(
            _get(source, "frame_status"), _FRESHNESS_STATES, ""
        ),
        "cache_age_seconds": _strict_optional(
            _get(source, "cache_age_seconds"), _bounded_age
        ),
        "cache_status": _enum(
            _get(source, "cache_status"), _FRESHNESS_STATES, ""
        ),
        "queue_depth": _strict_optional_counter(
            _get(source, "queue_depth")
        ),
        "terminal_jobs": _strict_optional_counter(
            _get(source, "terminal_jobs")
        ),
        "failed_jobs": _strict_optional_counter(
            _get(source, "failed_jobs")
        ),
        "retention_alarm": _strict_optional_alarm(
            _get(source, "retention_alarm")
        ),
        "capacity_alarm": _strict_optional_alarm(
            _get(source, "capacity_alarm")
        ),
        "headroom_bytes": _strict_optional(
            _get(source, "headroom_bytes"), _bounded_headroom
        ),
        "headroom_percent": _strict_optional(
            _get(source, "headroom_percent"), _bounded_percent
        ),
        "storage_reason_codes": _strict_reason_source(
            _get(source, "storage_reason_codes")
        ),
        "transition_status": _enum(
            _get(source, "transition_status"), _TRANSITION_STATES, ""
        ),
        "current_version": _strict_optional(
            _get(source, "current_version"), _version
        ),
        "next_version": _strict_optional(
            _get(source, "next_version"), _version
        ),
        "transition_reason_codes": _strict_reason_source(
            _get(source, "transition_reason_codes")
        ),
        "replay_contract_version": _strict_optional(
            _get(source, "replay_contract_version"), _version
        ),
        "replay_algorithm_version": _strict_optional(
            _get(source, "replay_algorithm_version"), _version
        ),
    }
    if any(value is _INVALID or value == "" for value in normalized.values()):
        return None
    return normalized


def _effective_group_status(overall: str, *, known: bool) -> str:
    if not known or overall == "unknown":
        return "unknown"
    return "stale" if overall == "stale" else "current"


def _freshness_projection(
    source: Mapping[str, Any],
    prefix: str,
    overall: str,
) -> dict[str, Any]:
    age = _bounded_age(_get(source, f"{prefix}_age_seconds"))
    declared = _enum(
        _get(source, f"{prefix}_status"),
        _FRESHNESS_STATES,
        "unknown",
    )
    if age is None or declared == "unknown" or overall == "unknown":
        status = "unknown"
    elif overall == "stale" or declared == "stale":
        status = "stale"
    else:
        status = "current"
    return {"status": status, "age_seconds": age}


def _unknown_projection(projected_at: float, reason: str) -> dict[str, Any]:
    unknown_freshness = {"status": "unknown", "age_seconds": None}
    return {
        "contract": CONTRACT,
        "source_contract": SOURCE_CONTRACT,
        "status": "unknown",
        "projected_at": projected_at,
        "projection_reason_codes": [reason],
        "worker": {
            "state": "unknown",
            "observed_at": None,
            "age_seconds": None,
            "version": None,
            "build_sha256": None,
            "config_sha256": None,
            "desired_state_sha256": None,
        },
        "freshness": {
            "provider": dict(unknown_freshness),
            "frame": dict(unknown_freshness),
            "cache": dict(unknown_freshness),
        },
        "jobs": {
            "status": "unknown",
            "queue_depth": None,
            "terminal_jobs": None,
            "failed_jobs": None,
            "counts_saturated": False,
        },
        "storage": {
            "status": "unknown",
            "retention_alarm": "unknown",
            "capacity_alarm": "unknown",
            "headroom_bytes": None,
            "headroom_percent": None,
            "reason_codes": [],
            "reason_codes_complete": False,
        },
        "transition": {
            "status": "unknown",
            "phase": "unknown",
            "current_version": None,
            "next_version": None,
            "reason_codes": [],
            "reason_codes_complete": False,
        },
        "replay": {
            "status": "unknown",
            "contract_version": None,
            "algorithm_version": None,
        },
        "claims": {
            "meteorological_truth": False,
            "operational_admission": False,
            "alert": False,
        },
    }


def build_worker_technical_diagnostics(
    source: Mapping[str, Any] | None,
    *,
    now: float,
) -> dict[str, Any]:
    """Project one fixed-size, capability-free worker diagnostic document.

    Missing or invalid source values become ``None``/``unknown``.  The function
    never copies arbitrary keys, free-form text, paths, topics or payloads and
    never hashes caller content.  Hash fields must already be exact SHA-256
    content addresses.
    """

    projected_at = _timestamp(now)
    if projected_at is None:
        raise ValueError("now must be a finite non-negative timestamp")
    if not isinstance(source, Mapping):
        result = _unknown_projection(projected_at, "diagnostic_source_missing")
        return result
    if _get(source, "contract") != SOURCE_CONTRACT:
        return _unknown_projection(
            projected_at,
            "diagnostic_source_contract_invalid",
        )

    state = _enum(_get(source, "worker_state"), _WORKER_STATES, "unknown")
    observed_at = _timestamp(_get(source, "worker_observed_at"))
    worker_age: float | None = None
    projection_reasons: list[str] = []
    if state == "unknown":
        projection_reasons.append("worker_state_unknown")
    if observed_at is None:
        projection_reasons.append("worker_observation_unknown")
        overall = "unknown"
    elif observed_at > projected_at + MAX_FUTURE_SKEW_SECONDS:
        projection_reasons.append("worker_observation_invalid")
        observed_at = None
        overall = "unknown"
    else:
        worker_age = round(max(0.0, projected_at - observed_at), 3)
        if state == "unknown":
            overall = "unknown"
        elif state not in _LIVE_WORKER_STATES:
            projection_reasons.append("worker_state_not_live")
            overall = "stale"
        elif worker_age > WORKER_STALE_AFTER_SECONDS:
            projection_reasons.append("worker_heartbeat_stale")
            overall = "stale"
        else:
            overall = "current"

    queue_depth, queue_saturated = _bounded_counter(
        _get(source, "queue_depth")
    )
    terminal_jobs, terminal_saturated = _bounded_counter(
        _get(source, "terminal_jobs")
    )
    failed_jobs, failed_saturated = _bounded_counter(
        _get(source, "failed_jobs")
    )
    jobs_known = any(
        value is not None for value in (queue_depth, terminal_jobs, failed_jobs)
    )

    retention_alarm = _get(source, "retention_alarm")
    if isinstance(retention_alarm, bool):
        retention_alarm = "critical" if retention_alarm else "clear"
    retention_alarm = _enum(retention_alarm, _ALARM_STATES, "unknown")
    capacity_alarm = _get(source, "capacity_alarm")
    if isinstance(capacity_alarm, bool):
        capacity_alarm = "critical" if capacity_alarm else "clear"
    capacity_alarm = _enum(capacity_alarm, _ALARM_STATES, "unknown")
    headroom_bytes = _bounded_headroom(_get(source, "headroom_bytes"))
    headroom_percent = _bounded_percent(_get(source, "headroom_percent"))
    storage_reasons, storage_reasons_complete = _reason_codes(
        _get(source, "storage_reason_codes")
    )
    storage_known = (
        retention_alarm != "unknown"
        or capacity_alarm != "unknown"
        or headroom_bytes is not None
        or headroom_percent is not None
        or bool(storage_reasons)
    )

    transition_status = _enum(
        _get(source, "transition_status"),
        _TRANSITION_STATES,
        "unknown",
    )
    current_version = _version(_get(source, "current_version"))
    next_version = _version(_get(source, "next_version"))
    transition_reasons, transition_reasons_complete = _reason_codes(
        _get(source, "transition_reason_codes")
    )
    transition_known = (
        transition_status != "unknown"
        or current_version is not None
        or next_version is not None
        or bool(transition_reasons)
    )

    replay_contract = _version(_get(source, "replay_contract_version"))
    replay_algorithm = _version(_get(source, "replay_algorithm_version"))
    replay_known = replay_contract is not None or replay_algorithm is not None

    result = {
        "contract": CONTRACT,
        "source_contract": SOURCE_CONTRACT,
        "status": overall,
        "projected_at": projected_at,
        "projection_reason_codes": projection_reasons,
        "worker": {
            "state": state,
            "observed_at": observed_at,
            "age_seconds": worker_age,
            "version": _version(_get(source, "worker_version")),
            "build_sha256": _content_address(
                _get(source, "build_sha256")
            ),
            "config_sha256": _content_address(
                _get(source, "config_sha256")
            ),
            "desired_state_sha256": _content_address(
                _get(source, "desired_state_sha256")
            ),
        },
        "freshness": {
            name: _freshness_projection(source, name, overall)
            for name in ("provider", "frame", "cache")
        },
        "jobs": {
            "status": _effective_group_status(overall, known=jobs_known),
            "queue_depth": queue_depth,
            "terminal_jobs": terminal_jobs,
            "failed_jobs": failed_jobs,
            "counts_saturated": (
                queue_saturated or terminal_saturated or failed_saturated
            ),
        },
        "storage": {
            "status": _effective_group_status(overall, known=storage_known),
            "retention_alarm": retention_alarm,
            "capacity_alarm": capacity_alarm,
            "headroom_bytes": headroom_bytes,
            "headroom_percent": headroom_percent,
            "reason_codes": storage_reasons,
            "reason_codes_complete": storage_reasons_complete,
        },
        "transition": {
            "status": _effective_group_status(overall, known=transition_known),
            "phase": transition_status,
            "current_version": current_version,
            "next_version": next_version,
            "reason_codes": transition_reasons,
            "reason_codes_complete": transition_reasons_complete,
        },
        "replay": {
            "status": _effective_group_status(overall, known=replay_known),
            "contract_version": replay_contract,
            "algorithm_version": replay_algorithm,
        },
        "claims": {
            "meteorological_truth": False,
            "operational_admission": False,
            "alert": False,
        },
    }
    if len(canonical_diagnostics_bytes(result)) > MAX_DIAGNOSTIC_BYTES:
        return _unknown_projection(
            projected_at,
            "diagnostic_projection_budget_exceeded",
        )
    return result


__all__ = [
    "CONTRACT",
    "MAX_DIAGNOSTIC_BYTES",
    "MAX_REASON_CODES",
    "SOURCE_FIELDS",
    "SOURCE_CONTRACT",
    "build_worker_technical_diagnostics",
    "canonical_diagnostics_bytes",
    "normalize_worker_technical_source",
]
