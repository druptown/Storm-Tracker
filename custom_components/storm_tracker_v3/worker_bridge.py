"""Lightweight MQTT bridge to the standalone Storm Tracker worker."""
from __future__ import annotations

import asyncio
import base64
import gzip
import json
import logging
import math
import re
import time
import uuid
from datetime import timedelta

from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_track_time_interval

from .const import (
    DATA_WORKER_TECHNICAL_DIAGNOSTICS,
    DATA_WORKER_TECHNICAL_SOURCE,
    DOMAIN,
    OPERATIONAL_SNAPSHOT_UPDATE_EVENT,
    WORKER_TECHNICAL_DIAGNOSTICS_UPDATE_EVENT,
)
from .operational_snapshot import (
    MAX_RECEIVER_FUTURE_SKEW_SECONDS,
    OperationalSnapshotCache,
    OperationalSnapshotError,
    decode_control_envelope,
)
from .worker_technical_diagnostics import (
    build_worker_technical_diagnostics,
    normalize_worker_technical_source,
)

_LOGGER = logging.getLogger(__name__)

CONTRACT_VERSION = 1
TOPIC_ROOT = "storm_tracker/v1"
STATUS_TOPIC = f"{TOPIC_ROOT}/worker/status"
REQUEST_TOPIC = f"{TOPIC_ROOT}/jobs/request"
JOB_STATUS_TOPIC = f"{TOPIC_ROOT}/jobs/+/status"
OPERATIONAL_SNAPSHOT_TOPIC = f"{TOPIC_ROOT}/operational/snapshot"
PING_INTERVAL = timedelta(seconds=30)
WORKER_STALE_SECONDS = 35.0
COMPRESS_PAYLOAD_FROM_BYTES = 128_000
MAX_MQTT_MESSAGE_BYTES = 900_000
MAX_DIAGNOSTIC_TEXT_CHARS = 160
MAX_DIAGNOSTIC_COUNT = 1_000_000_000
_LIVE_WORKER_STATES = frozenset({"online", "busy", "degraded"})
_POSTGIS_PROJECTION_STATES = frozenset({
    "disabled",
    "waiting",
    "current",
    "degraded",
})
_POSTGIS_PROJECTION_ERROR_CODES = frozenset({
    "postgis_projection_failed",
    "postgis_projection_runtime_failed",
    "postgis_provenance_conflict",
})
_SAFE_WORKER_ERROR_TOKEN = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,95}")
_SAFE_WORKER_REASON_CODE = re.compile(r"[a-z][a-z0-9_]{0,95}")
_SNAPSHOT_EXPIRY_HANDLE = "_operational_snapshot_expiry_handle"
_WORKER_STALE_HANDLE = "_worker_stale_handle"
_PENDING_OPERATIONAL_SNAPSHOT_PAYLOAD = (
    "_operational_snapshot_pending_authority_payload"
)


def _on_hass_event_loop(hass: HomeAssistant) -> bool:
    """Return true only while running on this HA instance's event loop."""

    try:
        return asyncio.get_running_loop() is hass.loop
    except RuntimeError:
        return False


def _dispatch_to_hass_event_loop(
    hass: HomeAssistant,
    callback,
    *args,
) -> bool:
    """Marshal an MQTT callback before it mutates HA-owned state."""

    if _on_hass_event_loop(hass):
        return False
    if hass.loop.is_closed():
        _LOGGER.error("Workerbrug-eventloopdispatch geweigerd (loop_closed)")
        return True
    try:
        hass.loop.call_soon_threadsafe(callback, *args)
    except RuntimeError:
        _LOGGER.error("Workerbrug-eventloopdispatch geweigerd (loop_closed)")
    return True


def _new_operational_snapshot_diagnostics(
    cache: OperationalSnapshotCache,
) -> dict:
    """Return the fixed-size diagnostics record for the control plane."""
    return {
        "state": cache.availability,
        "last_action": None,
        "revision": cache.revision,
        "snapshot_sha256": cache.snapshot_sha256,
        "desired_state_sha256": cache.get_status()["desired_state_sha256"],
        "availability": cache.availability,
        "valid_until": cache.valid_until,
        "envelope_bytes": 0,
        "applied_count": 0,
        "duplicate_count": 0,
        "rejected_count": 0,
        "last_error_type": None,
        "last_error": None,
    }


def _bounded_diagnostic_text(error: Exception) -> str:
    """Collapse an exception to one small line without retaining its payload."""
    text = " ".join(str(error).split())
    return text[:MAX_DIAGNOSTIC_TEXT_CHARS]


def _worker_job_error(status: dict) -> str:
    """Expose only fixed worker error tokens and secret-free reason codes."""

    error = status.get("error")
    if not isinstance(error, str) or _SAFE_WORKER_ERROR_TOKEN.fullmatch(error) is None:
        error = "geen_detail"
    detail = status.get("error_detail")
    if (
        isinstance(detail, str)
        and _SAFE_WORKER_REASON_CODE.fullmatch(detail) is not None
        and detail != error
    ):
        return f"{error}: {detail}"
    return error


def _increment_diagnostic(diagnostics: dict, key: str) -> None:
    """Increment a diagnostics counter without allowing unbounded integers."""
    current = diagnostics.get(key, 0)
    if not isinstance(current, int) or isinstance(current, bool) or current < 0:
        current = 0
    diagnostics[key] = min(MAX_DIAGNOSTIC_COUNT, current + 1)


def _cancel_timer(data: dict, key: str) -> None:
    """Cancel and forget one bridge-owned loop timer idempotently."""
    handle = data.pop(key, None)
    if handle is not None:
        handle.cancel()


def _clear_pending_operational_snapshot(data: dict) -> None:
    """Forget the single bounded snapshot awaiting confirmed authority."""

    data.pop(_PENDING_OPERATIONAL_SNAPSHOT_PAYLOAD, None)


def _bounded_worker_counter(value) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return min(value, MAX_DIAGNOSTIC_COUNT)


def _bounded_worker_setting(value, *, minimum: int, maximum: int) -> int | None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        return None
    return value


def _bounded_worker_text(value, *, maximum: int = 96) -> str | None:
    if not isinstance(value, str):
        return None
    return " ".join(value.split())[:maximum]


def _bounded_raw_calibration_source_statistics(value) -> list[dict] | None:
    """Validate one fixed-size, payload-free raw capture inventory."""

    if not isinstance(value, list) or len(value) > 64:
        return None
    result: list[dict] = []
    seen: set[str] = set()
    for row in value:
        if not isinstance(row, dict) or set(row) != {
            "source_id",
            "provider",
            "captures",
            "latest_received_at",
        }:
            return None
        source_id = _bounded_worker_text(row.get("source_id"), maximum=128)
        provider = _bounded_worker_text(row.get("provider"), maximum=128)
        captures = _bounded_worker_counter(row.get("captures"))
        latest = row.get("latest_received_at")
        if (
            source_id is None
            or provider is None
            or captures is None
            or captures < 1
            or source_id in seen
            or isinstance(latest, bool)
            or not isinstance(latest, (int, float))
            or not math.isfinite(float(latest))
            or latest < 0
        ):
            return None
        seen.add(source_id)
        result.append({
            "source_id": source_id,
            "provider": provider,
            "captures": captures,
            "latest_received_at": float(latest),
        })
    return result


def _bounded_postgis_projection(value) -> dict | None:
    """Keep only bounded, secret-free PostGIS projection diagnostics."""

    if not isinstance(value, dict):
        return None
    allowed = {
        "contract",
        "enabled",
        "status",
        "batch",
        "ready",
        "leased",
        "projected",
        "last_success_at",
        "last_error_code",
        "counts",
    }
    if not set(value) <= allowed:
        return None
    enabled = value.get("enabled")
    status = value.get("status")
    if (
        not isinstance(enabled, bool)
        or status not in _POSTGIS_PROJECTION_STATES
        or (not enabled and status != "disabled")
        or (enabled and status == "disabled")
    ):
        return None
    result = {"enabled": enabled, "status": status}
    for key in ("batch", "ready", "leased", "projected"):
        counter = _bounded_worker_counter(value.get(key))
        if counter is None:
            return None
        result[key] = counter
    last_success_at = value.get("last_success_at")
    if last_success_at is not None:
        if (
            isinstance(last_success_at, bool)
            or not isinstance(last_success_at, (int, float))
            or not math.isfinite(float(last_success_at))
            or last_success_at < 0
        ):
            return None
        result["last_success_at"] = float(last_success_at)
    error_code = value.get("last_error_code")
    if error_code is not None:
        if error_code not in _POSTGIS_PROJECTION_ERROR_CODES:
            return None
        result["last_error_code"] = error_code
    return result


def _compact_worker_status(status: dict, current: dict, *, now: float) -> dict:
    """Keep only fixed-size control fields from an untrusted worker status."""
    state = status.get("state")
    if state not in _LIVE_WORKER_STATES:
        state = "offline"
    compact = {
        "state": state,
        "contract_version": CONTRACT_VERSION,
        "job_admission_open": status.get("job_admission_open") is True,
        "last_seen": now,
        "last_ping_job_id": current.get("last_ping_job_id"),
        "last_ping_state": current.get("last_ping_state"),
        "last_ping_requested_at": current.get("last_ping_requested_at"),
        "last_ping_response_at": current.get("last_ping_response_at"),
    }
    for key in (
        "last_ping_job_id",
        "last_ping_state",
        "last_ping_requested_at",
        "last_ping_response_at",
    ):
        if compact[key] is None:
            compact.pop(key)
    version = _bounded_worker_text(
        status.get("worker_version", status.get("version"))
    )
    if version is not None:
        compact["version"] = version
    rejection_code = _bounded_worker_text(
        status.get("job_admission_rejection_code"), maximum=64
    )
    if rejection_code is not None:
        compact["job_admission_rejection_code"] = rejection_code
    profile = status.get("compute_profile")
    if profile in {"shared_ha", "dedicated_nas", "dedicated_nas_fast"}:
        compact["compute_profile"] = profile
    for source_key, minimum, maximum in (
        ("cpu_limit_percent", 1, 200),
        ("worker_nice", 0, 19),
        ("cpu_threads", 1, 64),
    ):
        value = _bounded_worker_setting(
            status.get(source_key), minimum=minimum, maximum=maximum
        )
        if value is not None:
            compact[source_key] = value
    provider_cycle_seconds = status.get("provider_cycle_seconds")
    if (
        isinstance(provider_cycle_seconds, (int, float))
        and not isinstance(provider_cycle_seconds, bool)
        and math.isfinite(float(provider_cycle_seconds))
        and 2 <= float(provider_cycle_seconds) <= 300
    ):
        compact["provider_cycle_seconds"] = float(provider_cycle_seconds)
    for source_key in ("queue_depth", "failed_jobs"):
        value = _bounded_worker_counter(status.get(source_key))
        if value is not None:
            compact[source_key] = value
    runtime = status.get("operational_snapshot_runtime")
    if isinstance(runtime, dict):
        for source_key, target_key in (
            ("provider_count", "runtime_provider_count"),
            ("captured_provider_count", "runtime_captured_provider_count"),
            ("snapshot_revision", "worker_snapshot_revision"),
            ("snapshot_rich_built", "snapshot_rich_built"),
            ("snapshot_rich_withheld", "snapshot_rich_withheld"),
            ("snapshot_rich_builder_failures", "snapshot_rich_builder_failures"),
            ("snapshot_unknown_built", "snapshot_unknown_built"),
        ):
            value = _bounded_worker_counter(runtime.get(source_key))
            if value is not None:
                compact[target_key] = value
        transport_state = _bounded_worker_text(
            runtime.get("snapshot_transport_state"), maximum=32
        )
        if transport_state is not None:
            compact["snapshot_transport_state"] = transport_state
        last_source = _bounded_worker_text(
            runtime.get("last_tick_workload_id"), maximum=48
        )
        last_reason = _bounded_worker_text(
            runtime.get("last_tick_reason_code"), maximum=47
        )
        if last_source is not None and last_reason is not None:
            compact["last_acquisition"] = f"{last_source}|{last_reason}"[:96]
        cycle_counts = runtime.get("cycle_status_counts")
        if isinstance(cycle_counts, dict):
            for status_name in (
                "current",
                "stale",
                "error",
                "blocked",
                "not_applicable",
            ):
                value = _bounded_worker_counter(cycle_counts.get(status_name))
                if value is not None:
                    compact[f"cycle_{status_name}"] = value
        raw_calibration = runtime.get("raw_calibration")
        if isinstance(raw_calibration, dict):
            for source_key, target_key in (
                ("captures", "raw_calibration_captures"),
                ("sources", "raw_calibration_sources"),
                ("providers", "raw_calibration_providers"),
                ("database_bytes", "raw_calibration_database_bytes"),
            ):
                value = _bounded_worker_counter(raw_calibration.get(source_key))
                if value is not None:
                    compact[target_key] = value
            calibration_status = _bounded_worker_text(
                raw_calibration.get("status"), maximum=32
            )
            if calibration_status is not None:
                compact["raw_calibration_status"] = calibration_status
            latest = raw_calibration.get("latest_received_at")
            if (
                isinstance(latest, (int, float))
                and not isinstance(latest, bool)
                and math.isfinite(float(latest))
                and latest >= 0
            ):
                compact["raw_calibration_latest_received_at"] = float(latest)
            source_statistics = _bounded_raw_calibration_source_statistics(
                raw_calibration.get("source_statistics")
            )
            if (
                source_statistics is not None
                and len(source_statistics)
                == compact.get("raw_calibration_sources")
            ):
                compact["raw_calibration_source_statistics"] = (
                    source_statistics
                )
        postgis_projection = _bounded_postgis_projection(
            runtime.get("postgis_projection")
        )
        if postgis_projection is not None:
            compact["postgis_projection_enabled"] = postgis_projection[
                "enabled"
            ]
            compact["postgis_projection_status"] = postgis_projection[
                "status"
            ]
            for key in ("ready", "leased", "projected"):
                compact[f"postgis_projection_{key}"] = postgis_projection[key]
            if "last_success_at" in postgis_projection:
                compact["postgis_projection_last_success_at"] = (
                    postgis_projection["last_success_at"]
                )
            if "last_error_code" in postgis_projection:
                compact["postgis_projection_last_error_code"] = (
                    postgis_projection["last_error_code"]
                )
    return compact


def _worker_technical_event_payload(document: dict) -> dict:
    """Expose only fixed cache-coordination metadata on the HA event bus."""

    return {
        "status": document["status"],
        "projected_at": document["projected_at"],
    }


def _store_worker_technical_projection(
    hass: HomeAssistant,
    data: dict,
    source: dict | None,
    *,
    now: float,
    fire_event: bool,
) -> dict:
    """Atomically replace the safe source and its deterministic projection."""

    document = build_worker_technical_diagnostics(source, now=now)
    data[DATA_WORKER_TECHNICAL_SOURCE] = source
    data[DATA_WORKER_TECHNICAL_DIAGNOSTICS] = document
    if fire_event:
        hass.bus.async_fire(
            WORKER_TECHNICAL_DIAGNOSTICS_UPDATE_EVENT,
            _worker_technical_event_payload(document),
        )
    return document


def _apply_worker_technical_status(
    hass: HomeAssistant,
    data: dict,
    status: dict,
    compact: dict,
    *,
    now: float,
) -> dict:
    """Validate an exact nested source and bind liveness to bridge truth."""

    source = normalize_worker_technical_source(
        status.get("technical_diagnostics"),
        worker_state=compact["state"],
        observed_at=now,
    )
    return _store_worker_technical_projection(
        hass,
        data,
        source,
        now=now,
        fire_event=True,
    )


def _mark_worker_technical_offline(
    hass: HomeAssistant,
    data: dict,
) -> None:
    """Re-project the last safe source across the worker offline boundary."""

    now = time.time()
    if not math.isfinite(now) or now < 0.0:
        _store_worker_technical_projection(
            hass,
            data,
            None,
            now=0.0,
            fire_event=True,
        )
        return
    retained = data.get(DATA_WORKER_TECHNICAL_SOURCE)
    observed_at = (
        retained.get("worker_observed_at")
        if isinstance(retained, dict)
        else None
    )
    source = normalize_worker_technical_source(
        retained,
        worker_state="offline",
        observed_at=observed_at,
    )
    _store_worker_technical_projection(
        hass,
        data,
        source,
        now=now,
        fire_event=True,
    )


def _snapshot_update_event_payload(
    cache: OperationalSnapshotCache,
    *,
    reason: str,
    envelope_bytes: int,
) -> dict:
    """Build the only bounded event emitted by the snapshot receiver."""
    return {
        "revision": cache.revision,
        "snapshot_sha256": cache.snapshot_sha256,
        "availability": cache.availability,
        "reason": reason,
        "envelope_bytes": envelope_bytes,
    }


def _apply_operational_snapshot_payload(
    hass: HomeAssistant,
    data: dict,
    payload: bytes,
):
    """Validate and atomically install one canonical bounded snapshot.

    This callback deliberately stays synchronous and in memory.  The contract
    rejects non-canonical, compressed, encoded and over-64-KiB input before the
    cache reference can change.
    """
    cache: OperationalSnapshotCache = data["operational_snapshot_cache"]
    diagnostics = data["operational_snapshot_diagnostics"]
    try:
        if not isinstance(payload, bytes):
            raise TypeError("operational snapshot moet canonieke bytes zijn")
        receiver_now = time.time()
        authority_required = (
            data.get("operational_snapshot_authority_required") is True
        )
        expected_authority = data.get(
            "operational_snapshot_expected_desired_state_sha256"
        )
        if authority_required and expected_authority is None:
            # MQTT may deliver the retained snapshot before the desired-state
            # controller finishes its startup handshake.  Validate the exact
            # canonical, 64-KiB-bounded envelope now, but do not install it
            # without authority.  At most one raw candidate is retained and a
            # later valid retained message replaces the prior candidate.
            decode_control_envelope(payload)
            data[_PENDING_OPERATIONAL_SNAPSHOT_PAYLOAD] = bytes(payload)
            raise OperationalSnapshotError(
                "desired-state-authoriteit is nog niet bevestigd"
            )
        result = cache.apply_bytes(
            payload,
            expected_desired_state_sha256=(
                expected_authority if authority_required else None
            ),
            receiver_now=receiver_now,
            max_future_skew_seconds=MAX_RECEIVER_FUTURE_SKEW_SECONDS,
        )
    except (OperationalSnapshotError, TypeError, UnicodeError) as err:
        diagnostics.update({
            "state": "rejected",
            "last_action": "rejected",
            "revision": cache.revision,
            "snapshot_sha256": cache.snapshot_sha256,
            "desired_state_sha256": cache.get_status()[
                "desired_state_sha256"
            ],
            "availability": cache.availability,
            "last_error_type": type(err).__name__[:64],
            "last_error": _bounded_diagnostic_text(err),
        })
        _increment_diagnostic(diagnostics, "rejected_count")
        return None

    diagnostics.update({
        "state": cache.availability,
        "last_action": result.action,
        "revision": result.revision,
        "snapshot_sha256": result.snapshot_sha256,
        "desired_state_sha256": cache.get_status()["desired_state_sha256"],
        "availability": cache.availability,
        "valid_until": cache.valid_until,
        "envelope_bytes": result.envelope_bytes,
        "last_error_type": None,
        "last_error": None,
    })
    expired = _mark_operational_snapshot_expired(
        hass,
        data,
        now=receiver_now,
    )
    if not expired:
        _schedule_operational_snapshot_expiry(
            hass,
            data,
            receiver_now=receiver_now,
        )
    if result.action == "duplicate":
        _increment_diagnostic(diagnostics, "duplicate_count")
        return result

    _increment_diagnostic(diagnostics, "applied_count")
    if expired:
        # The bytes remain the cryptographically validated cache head, but no
        # transient current projection is emitted for an already-expired fact.
        return result
    hass.bus.async_fire(
        OPERATIONAL_SNAPSHOT_UPDATE_EVENT,
        _snapshot_update_event_payload(
            cache,
            reason="applied",
            envelope_bytes=result.envelope_bytes,
        ),
    )
    return result


def _mark_operational_snapshot_expired(
    hass: HomeAssistant,
    data: dict,
    *,
    now: float,
) -> bool:
    """Fail one expired cache head closed without changing worker heartbeat."""
    cache: OperationalSnapshotCache = data["operational_snapshot_cache"]
    if not cache.mark_expired(now=now):
        return False
    diagnostics = data["operational_snapshot_diagnostics"]
    diagnostics.update({
        "state": "stale",
        "last_action": "snapshot_expired",
        "revision": cache.revision,
        "snapshot_sha256": cache.snapshot_sha256,
        "desired_state_sha256": cache.get_status()["desired_state_sha256"],
        "availability": "stale",
        "valid_until": cache.valid_until,
        "last_error_type": None,
        "last_error": None,
    })
    hass.bus.async_fire(
        OPERATIONAL_SNAPSHOT_UPDATE_EVENT,
        _snapshot_update_event_payload(
            cache,
            reason="snapshot_expired",
            envelope_bytes=diagnostics["envelope_bytes"],
        ),
    )
    return True


def _schedule_operational_snapshot_expiry(
    hass: HomeAssistant,
    data: dict,
    *,
    receiver_now: float,
) -> None:
    """Arm an exact one-shot expiry for the currently accepted cache head."""
    _cancel_timer(data, _SNAPSHOT_EXPIRY_HANDLE)
    cache: OperationalSnapshotCache = data["operational_snapshot_cache"]
    status = cache.get_status()
    digest = cache.snapshot_sha256
    deadline = cache.valid_until
    if (
        status.get("availability") != "current"
        or digest is None
        or deadline is None
    ):
        return
    delay = max(0.0, float(deadline) - float(receiver_now))
    handle = None

    @callback
    def _expire_exact_head() -> None:
        if data.get(_SNAPSHOT_EXPIRY_HANDLE) is not handle:
            return
        data.pop(_SNAPSHOT_EXPIRY_HANDLE, None)
        if not cache.mark_expired_head(
            snapshot_sha256=digest,
            valid_until=deadline,
        ):
            return
        diagnostics = data["operational_snapshot_diagnostics"]
        diagnostics.update({
            "state": "stale",
            "last_action": "snapshot_expired",
            "revision": cache.revision,
            "snapshot_sha256": cache.snapshot_sha256,
            "desired_state_sha256": cache.get_status()[
                "desired_state_sha256"
            ],
            "availability": "stale",
            "valid_until": cache.valid_until,
            "last_error_type": None,
            "last_error": None,
        })
        hass.bus.async_fire(
            OPERATIONAL_SNAPSHOT_UPDATE_EVENT,
            _snapshot_update_event_payload(
                cache,
                reason="snapshot_expired",
                envelope_bytes=diagnostics["envelope_bytes"],
            ),
        )

    handle = hass.loop.call_later(delay, _expire_exact_head)
    data[_SNAPSHOT_EXPIRY_HANDLE] = handle


def _mark_operational_snapshot_worker_offline(
    hass: HomeAssistant,
    data: dict,
) -> bool:
    """Atomically fail closed to unknown/stale, once per state change."""
    _cancel_timer(data, _SNAPSHOT_EXPIRY_HANDLE)
    cache: OperationalSnapshotCache = data["operational_snapshot_cache"]
    if not cache.mark_worker_offline():
        return False
    diagnostics = data["operational_snapshot_diagnostics"]
    diagnostics.update({
        "state": "stale",
        "last_action": "worker_offline",
        "revision": cache.revision,
        "snapshot_sha256": cache.snapshot_sha256,
        "desired_state_sha256": cache.get_status()["desired_state_sha256"],
        "availability": "stale",
        "valid_until": cache.valid_until,
        "last_error_type": None,
        "last_error": None,
    })
    hass.bus.async_fire(
        OPERATIONAL_SNAPSHOT_UPDATE_EVENT,
        _snapshot_update_event_payload(
            cache,
            reason="worker_offline",
            envelope_bytes=diagnostics["envelope_bytes"],
        ),
    )
    return True


def _set_worker_offline(hass: HomeAssistant, data: dict) -> None:
    """Publish one bounded offline transition and invalidate all live facts."""
    _cancel_timer(data, _WORKER_STALE_HANDLE)
    status = dict(data.get("worker_status", {}))
    changed = status.get("state") != "offline"
    status["state"] = "offline"
    status["job_admission_open"] = False
    status["job_admission_rejection_code"] = "worker_offline"
    data["worker_status"] = status
    if changed:
        _mark_worker_technical_offline(hass, data)
        hass.bus.async_fire(f"{DOMAIN}_worker_status_update")
    _mark_operational_snapshot_worker_offline(hass, data)


def _schedule_worker_stale_deadline(
    hass: HomeAssistant,
    data: dict,
    *,
    last_seen: float,
) -> None:
    """Arm a monotonic deadline for exactly the latest live heartbeat."""
    _cancel_timer(data, _WORKER_STALE_HANDLE)
    handle = None

    @callback
    def _expire_heartbeat() -> None:
        if data.get(_WORKER_STALE_HANDLE) is not handle:
            return
        data.pop(_WORKER_STALE_HANDLE, None)
        status = data.get("worker_status", {})
        if (
            status.get("state") not in _LIVE_WORKER_STATES
            or status.get("last_seen") != last_seen
        ):
            return
        _set_worker_offline(hass, data)

    handle = hass.loop.call_later(WORKER_STALE_SECONDS, _expire_heartbeat)
    data[_WORKER_STALE_HANDLE] = handle


def mark_operational_snapshot_authority_changed(
    hass: HomeAssistant,
    *,
    expected_desired_state_sha256: str | None,
) -> bool:
    """Install receiver authority and fail the prior projection closed."""

    data = hass.data.get(DOMAIN)
    if not isinstance(data, dict):
        return False
    _cancel_timer(data, _SNAPSHOT_EXPIRY_HANDLE)
    previous_authority = data.get(
        "operational_snapshot_expected_desired_state_sha256"
    )
    data["operational_snapshot_expected_desired_state_sha256"] = (
        expected_desired_state_sha256
    )
    if expected_desired_state_sha256 is None:
        # Preserve a startup candidate across the controller's initial
        # None -> None notification, but never across a real authority reset.
        if previous_authority is not None:
            _clear_pending_operational_snapshot(data)
        pending_payload = None
    else:
        # Pop before replay so every candidate is attempted at most once,
        # including when its desired-state hash is wrong.
        pending_payload = data.pop(
            _PENDING_OPERATIONAL_SNAPSHOT_PAYLOAD,
            None,
        )
    cache = data.get("operational_snapshot_cache")
    diagnostics = data.get("operational_snapshot_diagnostics")
    if not isinstance(cache, OperationalSnapshotCache) or not isinstance(
        diagnostics, dict
    ):
        return False
    transitioned = cache.mark_stale(
        reason="desired_state_transition", worker_state="unknown"
    )
    if transitioned:
        diagnostics.update({
            "state": "stale",
            "last_action": "desired_state_transition",
            "revision": cache.revision,
            "snapshot_sha256": cache.snapshot_sha256,
            "desired_state_sha256": cache.get_status()[
                "desired_state_sha256"
            ],
            "availability": "stale",
            "valid_until": cache.valid_until,
            "last_error_type": None,
            "last_error": None,
        })
        hass.bus.async_fire(
            OPERATIONAL_SNAPSHOT_UPDATE_EVENT,
            _snapshot_update_event_payload(
                cache,
                reason="desired_state_transition",
                envelope_bytes=diagnostics["envelope_bytes"],
            ),
        )
    replayed = (
        _apply_operational_snapshot_payload(hass, data, pending_payload)
        if pending_payload is not None
        else None
    )
    return transitioned or replayed is not None


def worker_is_available(hass: HomeAssistant, *, now: float | None = None) -> bool:
    """Return a cheap heartbeat-only admission decision.

    Callers use this before building, hashing or durably staging large worker
    payloads. The authoritative check is repeated immediately before publish.
    """
    status = hass.data.get(DOMAIN, {}).get("worker_status", {})
    last_seen = status.get("last_seen")
    if status.get("state") not in _LIVE_WORKER_STATES or last_seen is None:
        return False
    try:
        age = (time.time() if now is None else float(now)) - float(last_seen)
    except (TypeError, ValueError):
        return False
    return 0.0 <= age <= WORKER_STALE_SECONDS


def _decode(payload: bytes | str) -> dict:
    if isinstance(payload, bytes):
        raw_size = len(payload)
    elif isinstance(payload, str):
        raw_size = len(payload.encode("utf-8"))
    else:
        raise ValueError("workerbericht moet bytes of tekst zijn")
    if raw_size > MAX_MQTT_MESSAGE_BYTES:
        raise ValueError("workerbericht overschrijdt veilige MQTT-grens")
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    message = json.loads(payload)
    if not isinstance(message, dict):
        raise ValueError("workerbericht is geen object")
    if message.get("contract_version") != CONTRACT_VERSION:
        raise ValueError("workercontractversie is niet ondersteund")
    return message


def _encode(message: dict) -> str:
    return json.dumps(
        message, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )


def _job_message(job_id: str, job_type: str, payload: dict, timeout: float) -> str:
    """Encodeer een job compact zonder observaties of cellen weg te laten."""
    payload_json = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    message = {
        "contract_version": CONTRACT_VERSION,
        "message_type": "job_request",
        "timestamp": time.time(),
        "job_id": job_id,
        "job_type": job_type,
        "deadline": time.time() + timeout,
    }
    if len(payload_json) >= COMPRESS_PAYLOAD_FROM_BYTES:
        message["payload_codec"] = "gzip+base64+json"
        message["payload_data"] = base64.b64encode(
            gzip.compress(payload_json, compresslevel=6)
        ).decode("ascii")
        message["payload_uncompressed_bytes"] = len(payload_json)
    else:
        message["payload"] = payload
    encoded = _encode(message)
    if len(encoded.encode("utf-8")) > MAX_MQTT_MESSAGE_BYTES:
        raise ValueError("Gecomprimeerde workerjob overschrijdt veilige MQTT-grens")
    return encoded


async def async_setup_worker_bridge(hass: HomeAssistant) -> None:
    """Observeer workerstatus en voer een goedkope end-to-end ping uit."""
    data = hass.data[DOMAIN]
    _clear_pending_operational_snapshot(data)
    cache = data.get("operational_snapshot_cache")
    if not isinstance(cache, OperationalSnapshotCache):
        cache = OperationalSnapshotCache()
        data["operational_snapshot_cache"] = cache
    data["operational_snapshot_diagnostics"] = (
        _new_operational_snapshot_diagnostics(cache)
    )
    data["worker_status"] = {
        "state": "waiting",
        "contract_version": CONTRACT_VERSION,
        "job_admission_open": False,
        "last_seen": None,
        "last_ping_job_id": None,
        "last_ping_state": None,
    }
    data["worker_pending_jobs"] = {}
    initial_now = time.time()
    if not math.isfinite(initial_now) or initial_now < 0.0:
        initial_now = 0.0
    _store_worker_technical_projection(
        hass,
        data,
        None,
        now=initial_now,
        fire_event=False,
    )
    if not await mqtt.async_wait_for_mqtt_client(hass):
        data["worker_status"]["state"] = "offline"
        _mark_operational_snapshot_worker_offline(hass, data)
        _LOGGER.warning("MQTT-client niet beschikbaar; workerbrug blijft offline")
        return

    @callback
    def _apply_operational_snapshot(payload: bytes | str) -> None:
        _apply_operational_snapshot_payload(hass, data, payload)

    @callback
    def _operational_snapshot(message) -> None:
        payload = message.payload
        if _dispatch_to_hass_event_loop(
            hass, _apply_operational_snapshot, payload
        ):
            return
        _apply_operational_snapshot(payload)

    @callback
    def _apply_worker_status(payload: bytes | str) -> None:
        try:
            status = _decode(payload)
            if status.get("message_type") != "worker_status":
                return
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as err:
            _LOGGER.warning("Ongeldige Storm Tracker-workerstatus: %s", err)
            return
        now = time.time()
        if not math.isfinite(now) or now < 0.0:
            _set_worker_offline(hass, data)
            return
        current = _compact_worker_status(
            status,
            dict(data.get("worker_status", {})),
            now=now,
        )
        data["worker_status"] = current
        _apply_worker_technical_status(
            hass,
            data,
            status,
            current,
            now=now,
        )
        hass.bus.async_fire(f"{DOMAIN}_worker_status_update")
        if current.get("state") in _LIVE_WORKER_STATES:
            _schedule_worker_stale_deadline(
                hass,
                data,
                last_seen=now,
            )
        else:
            _set_worker_offline(hass, data)

    @callback
    def _worker_status(message) -> None:
        payload = message.payload
        if _dispatch_to_hass_event_loop(hass, _apply_worker_status, payload):
            return
        _apply_worker_status(payload)

    @callback
    def _apply_job_status(payload: bytes | str) -> None:
        try:
            status = _decode(payload)
            if status.get("message_type") != "job_status":
                return
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as err:
            _LOGGER.warning("Ongeldige Storm Tracker-jobstatus: %s", err)
            return
        current = dict(data.get("worker_status", {}))
        if status.get("job_id") == current.get("last_ping_job_id"):
            current["last_ping_state"] = status.get("state")
            current["last_ping_response_at"] = time.time()
            data["worker_status"] = current
            hass.bus.async_fire(f"{DOMAIN}_worker_status_update")
        future = data.get("worker_pending_jobs", {}).get(status.get("job_id"))
        if future is None or future.done():
            return
        state = status.get("state")
        if state == "completed":
            future.set_result(dict(status.get("result") or {}))
        elif state in {"failed", "expired", "cancelled"}:
            future.set_exception(
                RuntimeError(
                    f"Workerjob {state}: {_worker_job_error(status)}"
                )
            )

    @callback
    def _job_status(message) -> None:
        payload = message.payload
        if _dispatch_to_hass_event_loop(hass, _apply_job_status, payload):
            return
        _apply_job_status(payload)

    data["unsubscribers"].append(
        await mqtt.async_subscribe(hass, STATUS_TOPIC, _worker_status, qos=1)
    )
    data["unsubscribers"].append(
        await mqtt.async_subscribe(hass, JOB_STATUS_TOPIC, _job_status, qos=1)
    )
    data["unsubscribers"].append(
        await mqtt.async_subscribe(
            hass,
            OPERATIONAL_SNAPSHOT_TOPIC,
            _operational_snapshot,
            qos=1,
            # The snapshot contract verifies the exact canonical wire bytes.
            # Home Assistant decodes MQTT payloads to UTF-8 text by default;
            # explicitly disable that conversion for this one binary-safe
            # subscription so the receiver does not reject every live frame.
            encoding=None,
        )
    )
    data["unsubscribers"].extend((
        lambda: _cancel_timer(data, _SNAPSHOT_EXPIRY_HANDLE),
        lambda: _cancel_timer(data, _WORKER_STALE_HANDLE),
        lambda: _clear_pending_operational_snapshot(data),
    ))

    async def _ping_worker(_now=None) -> None:
        status = dict(data.get("worker_status", {}))
        last_seen = status.get("last_seen")
        now = time.time()
        if not math.isfinite(now) or now < 0.0:
            _set_worker_offline(hass, data)
            return
        if (
            last_seen is None
            or status.get("state") not in _LIVE_WORKER_STATES
        ):
            _set_worker_offline(hass, data)
            return
        job_id = str(uuid.uuid4())
        status["last_ping_job_id"] = job_id
        status["last_ping_state"] = "requested"
        status["last_ping_requested_at"] = now
        data["worker_status"] = status
        await mqtt.async_publish(
            hass,
            REQUEST_TOPIC,
            _encode(
                {
                    "contract_version": CONTRACT_VERSION,
                    "message_type": "job_request",
                    "timestamp": now,
                    "job_id": job_id,
                    "job_type": "ping",
                    "deadline": now + 20,
                    "payload": {},
                }
            ),
            qos=1,
            retain=False,
        )

    data["unsubscribers"].append(
        async_track_time_interval(hass, _ping_worker, PING_INTERVAL)
    )
    await _ping_worker()


async def async_request_worker_job(
    hass: HomeAssistant,
    job_type: str,
    payload: dict,
    *,
    timeout: float = 35.0,
    job_id: str | None = None,
) -> dict:
    """Voer één begrensde workerjob uit en wacht asynchroon op metadata."""
    data = hass.data[DOMAIN]
    lock = data.setdefault("worker_request_lock", asyncio.Lock())

    async with lock:
        if not worker_is_available(hass):
            raise RuntimeError("Storm Tracker-worker is niet beschikbaar")
        job_id = str(job_id or uuid.uuid4()).strip()
        if not job_id:
            raise ValueError("worker job_id mag niet leeg zijn")
        # Encode and enforce the MQTT bound before publishing a pending receipt.
        # A serialization/size failure must never leave this job_id stuck forever.
        encoded_request = _job_message(job_id, job_type, payload, timeout)
        future = hass.loop.create_future()
        pending = data.setdefault("worker_pending_jobs", {})
        if job_id in pending:
            raise RuntimeError(f"workerjob {job_id} wacht al op antwoord")
        pending[job_id] = future
        try:
            await mqtt.async_publish(
                hass,
                REQUEST_TOPIC,
                encoded_request,
                qos=1,
                retain=False,
            )
            # Eén identieke QoS1-herpublicatie sluit het venster waarin de broker
            # de eerste publish aannam maar de worker/bridge herverbond. De worker
            # antwoordt voor dezelfde job-ID vanuit zijn actuele of duurzame
            # receipt; de job wordt dus nooit als een nieuwe berekening behandeld.
            retry_after = min(5.0, max(0.25, timeout / 2.0))
            started = time.monotonic()
            try:
                return await asyncio.wait_for(
                    asyncio.shield(future),
                    timeout=retry_after,
                )
            except TimeoutError:
                await mqtt.async_publish(
                    hass,
                    REQUEST_TOPIC,
                    encoded_request,
                    qos=1,
                    retain=False,
                )
            remaining = max(0.01, timeout - (time.monotonic() - started))
            return await asyncio.wait_for(future, timeout=remaining)
        finally:
            pending.pop(job_id, None)
            if not future.done():
                future.cancel()
