"""Unwired, bounded Home Assistant desired-state controller candidate.

The controller is deliberately a control-plane object only.  It coalesces one
latest Home Assistant target/settings snapshot and serialises the three worker
operations required to publish it: claim authority, read the privacy-minimal
CAS head, and apply one canonical desired-state document.  This module does not
register listeners, entities or services and does not provide a local fallback.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import math
import re
import time
from typing import Any
import uuid

from homeassistant.core import HomeAssistant

from .desired_state_builder import (
    MAX_TARGETS,
    DesiredStateEnvelope,
    build_desired_state,
    canonical_json_bytes,
)
from .worker_bridge import async_request_worker_job


WORKER_METHOD = "worker_desired_state_store_v1"
MAX_RESPONSE_BYTES = 8 * 1024
MAX_DIAGNOSTIC_COUNT = 1_000_000_000
MAX_SNAPSHOT_SEQUENCE = 1_000_000_000
MIN_DYNAMIC_MOVEMENT_KM = 1.0
# A stationary Life360 tracker still carries fresh liveness evidence.  Refresh
# that lease before the worker's 15-minute dynamic-position limit, without
# publishing every small GPS/timestamp update as a new authority generation.
MAX_SEMANTIC_SILENCE_SECONDS = 10 * 60.0

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$")
_CREDENTIAL_ID_RE = re.compile(
    r"(?:^|[-_.:@/])(?:access[-_]?token|refresh[-_]?token|authorization|"
    r"cookie|client[-_]?(?:id|secret)|application[-_]?key|api[-_]?key|"
    r"private[-_]?key|password|secret)(?:$|[-_.:@/])",
    re.IGNORECASE,
)
_POSITION_KEYS = (
    "latitude",
    "longitude",
    "position_observed_at",
    "location_accuracy_m",
    "country_code",
)
_DYNAMIC_TARGET_KINDS = frozenset({"device_tracker", "person", "test"})
_TRANSIENT_SEMANTIC_TARGET_FIELDS = frozenset({
    "location_accuracy_m",
    "position_observed_at",
})
_CAS_CONFLICT_MARKERS = (
    "actieve sha",
    "active sha",
    "bouwt niet op",
    "compare-and-swap",
    "cas conflict",
    "generation",
    "fencing",
    "producer authority",
    "authority_epoch",
    "authority epoch",
    "eerste desired-state",
    "previous_state_sha256",
    "desired_state_conflict",
    "desired-state conflict",
)

WorkerRequester = Callable[..., Awaitable[dict[str, Any]]]


class DesiredStateControllerError(RuntimeError):
    """The candidate received an invalid worker control-plane response."""


@dataclass(frozen=True, slots=True)
class _Head:
    state_sha256: str | None
    producer_id: str | None
    authority_epoch: int | None
    generation: int | None


@dataclass(frozen=True, slots=True)
class _Attempt:
    envelope: DesiredStateEnvelope
    document: dict[str, Any]
    semantic_sha256: str
    job_id: str


def _safe_identifier(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} moet tekst zijn")
    result = value.strip()
    if (
        not result
        or len(result) > 160
        or _IDENTIFIER_RE.fullmatch(result) is None
        or _CREDENTIAL_ID_RE.search(result)
    ):
        raise ValueError(f"{name} is geen veilig begrensd ID")
    return result


def _bounded_number(value: Any, name: str, *, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} moet een getal zijn")
    result = float(value)
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} valt buiten de grens")
    return result


def _required_int(value: Any, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or not minimum <= value <= 2**63 - 1:
        raise DesiredStateControllerError(f"ongeldige {name} in workerantwoord")
    return value


def _required_sha(value: Any, name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise DesiredStateControllerError(f"ongeldige {name} in workerantwoord")
    return value


def _small_result(value: Any, operation: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise DesiredStateControllerError(f"{operation} gaf geen object terug")
    result = dict(value)
    try:
        encoded = json.dumps(
            result,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as err:
        raise DesiredStateControllerError(
            f"{operation} gaf geen begrensde metadata terug"
        ) from err
    if len(encoded) > MAX_RESPONSE_BYTES:
        raise DesiredStateControllerError(
            f"{operation} overschrijdt de metadataresponsgrens"
        )
    return result


def _exact_result_keys(
    result: Mapping[str, Any], expected: set[str], operation: str
) -> None:
    if set(result) != expected:
        raise DesiredStateControllerError(
            f"{operation} heeft onverwachte metadata-velden"
        )


def _is_cas_conflict(error: Exception) -> bool:
    if not isinstance(error, RuntimeError):
        return False
    text = " ".join(str(error).lower().split())[:512]
    return any(marker in text for marker in _CAS_CONFLICT_MARKERS)


def _failure_code(error: Exception) -> str:
    if isinstance(error, TimeoutError):
        return "worker_timeout"
    if isinstance(error, DesiredStateControllerError):
        return "worker_protocol_rejected"
    if isinstance(error, RuntimeError):
        text = " ".join(str(error).lower().split())[:512]
        if "niet beschikbaar" in text or "unavailable" in text or "offline" in text:
            return "worker_offline"
        if _is_cas_conflict(error):
            return "cas_conflict"
        return "worker_job_failed"
    return "controller_failure"


class DesiredStateController:
    """One-in-flight desired-state publisher candidate for Home Assistant.

    The caller owns listener registration and passes complete target/settings
    snapshots.  The object retains at most the latest 64 targets and one
    uncertain canonical apply attempt; there is no positional or job backlog.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        producer_id: str,
        boot_id: str | None = None,
        requester: WorkerRequester = async_request_worker_job,
        authority_callback: Callable[[str | None], None] | None = None,
        clock: Callable[[], float] = time.time,
        debounce_seconds: float = 0.35,
        request_timeout: float = 35.0,
    ) -> None:
        self._hass = hass
        self._producer_id = _safe_identifier(producer_id, "producer_id")
        if boot_id is None:
            boot_token = uuid.uuid4().hex
        elif isinstance(boot_id, str) and 0 < len(boot_id) <= 256:
            boot_token = boot_id
        else:
            raise ValueError("boot_id moet begrensde tekst zijn")
        claim_digest = hashlib.sha256(
            f"{self._producer_id}\0{boot_token}".encode("utf-8")
        ).hexdigest()
        self._claim_id = f"ha-boot:{claim_digest[:40]}"
        self._job_token = hashlib.sha256(
            f"{self._producer_id}\0{self._claim_id}".encode("utf-8")
        ).hexdigest()[:24]
        self._claim_job_id = f"stv3-ds-claim-{self._job_token}"
        self._requester = requester
        if authority_callback is not None and not callable(authority_callback):
            raise TypeError("authority_callback moet callable zijn")
        self._authority_callback = authority_callback
        self._clock = clock
        self._debounce_seconds = _bounded_number(
            debounce_seconds,
            "debounce_seconds",
            minimum=0.0,
            maximum=30.0,
        )
        self._request_timeout = _bounded_number(
            request_timeout,
            "request_timeout",
            minimum=0.05,
            maximum=120.0,
        )

        self._targets: tuple[dict[str, Any], ...] | None = None
        self._settings: dict[str, Any] | None = None
        self._semantic_sha256: str | None = None
        self._published_semantic_sha256: str | None = None
        self._authority_epoch: int | None = None
        self._head = _Head(None, None, None, None)
        self._pending_attempt: _Attempt | None = None
        self._snapshot_sequence = 0
        self._last_issued_at = 0.0
        self._last_applied_at: float | None = None
        self._lease_refresh_pending = False

        self._started = False
        self._stopped = False
        self._dirty = False
        self._needs_handshake = True
        self._retry_blocked = False
        self._state = "new"
        self._last_error_code: str | None = None
        self._last_error_type: str | None = None
        self._drive_task: asyncio.Task[None] | None = None
        self._debounce_handle: asyncio.TimerHandle | None = None
        self._lease_refresh_handle: asyncio.TimerHandle | None = None
        self._counters = {
            "accepted_updates": 0,
            "coalesced_updates": 0,
            "lease_refreshes": 0,
            "regressive_positions_filtered": 0,
            "worker_requests": 0,
            "claim_requests": 0,
            "snapshot_requests": 0,
            "apply_requests": 0,
            "applied_states": 0,
            "recovered_receipts": 0,
            "cas_refreshes": 0,
            "reconnects": 0,
            "failures": 0,
            "cancellations": 0,
        }

    def _increment(self, name: str) -> None:
        self._counters[name] = min(
            MAX_DIAGNOSTIC_COUNT, self._counters[name] + 1
        )

    def _notify_authority(self, value: str | None) -> None:
        callback = self._authority_callback
        if callback is not None:
            callback(value)

    def _normalize_inputs(
        self,
        targets: Sequence[Mapping[str, Any]],
        settings: Mapping[str, Any],
    ) -> tuple[tuple[dict[str, Any], ...], dict[str, Any]]:
        # The real builder is the sole schema authority.  Dummy CAS metadata is
        # discarded; only its detached normalized targets/settings are kept.
        envelope = build_desired_state(
            producer_id=self._producer_id,
            authority_epoch=1,
            generation=0,
            issued_at=0.0,
            previous_state_sha256=None,
            targets=targets,
            settings=settings,
        )
        document = envelope.as_dict()
        return (
            tuple(dict(target) for target in document["targets"]),
            dict(document["settings"]),
        )

    def _merge_non_regressive_positions(
        self, targets: tuple[dict[str, Any], ...]
    ) -> tuple[dict[str, Any], ...]:
        if self._targets is None:
            return targets
        previous_by_id = {target["target_id"]: target for target in self._targets}
        merged: list[dict[str, Any]] = []
        for target in targets:
            previous = previous_by_id.get(target["target_id"])
            if previous is None:
                merged.append(target)
                continue
            old_at = previous["position_observed_at"]
            new_at = target["position_observed_at"]
            regressive = old_at is not None and (
                new_at is None
                or new_at < old_at
                or (
                    new_at == old_at
                    and (
                        target["latitude"] != previous["latitude"]
                        or target["longitude"] != previous["longitude"]
                    )
                )
            )
            if not regressive:
                merged.append(target)
                continue
            safe_target = dict(target)
            for key in _POSITION_KEYS:
                safe_target[key] = previous[key]
            merged.append(safe_target)
            self._increment("regressive_positions_filtered")
        return tuple(merged)

    @staticmethod
    def _position_distance_km(
        first: Mapping[str, Any], second: Mapping[str, Any]
    ) -> float:
        first_latitude = math.radians(float(first["latitude"]))
        second_latitude = math.radians(float(second["latitude"]))
        latitude_delta = second_latitude - first_latitude
        longitude_delta = math.radians(
            float(second["longitude"]) - float(first["longitude"])
        )
        value = math.sin(latitude_delta / 2.0) ** 2 + (
            math.cos(first_latitude)
            * math.cos(second_latitude)
            * math.sin(longitude_delta / 2.0) ** 2
        )
        return 6371.0088 * 2.0 * math.asin(min(1.0, math.sqrt(value)))

    def _coalesce_dynamic_position_noise(
        self, targets: tuple[dict[str, Any], ...]
    ) -> tuple[dict[str, Any], ...]:
        """Keep a stable worker position until a tracker moves at least 1 km."""

        if self._targets is None:
            return targets
        previous_by_id = {target["target_id"]: target for target in self._targets}
        coalesced: list[dict[str, Any]] = []
        for target in targets:
            previous = previous_by_id.get(target["target_id"])
            if (
                previous is None
                or target["kind"] not in _DYNAMIC_TARGET_KINDS
                or target["kind"] != previous["kind"]
                or target["available"] != previous["available"]
                or self._position_distance_km(previous, target)
                >= MIN_DYNAMIC_MOVEMENT_KM
            ):
                coalesced.append(target)
                continue
            stable = dict(target)
            stable["latitude"] = previous["latitude"]
            stable["longitude"] = previous["longitude"]
            coalesced.append(stable)
        return tuple(coalesced)

    @staticmethod
    def _semantic_targets(
        targets: tuple[dict[str, Any], ...]
    ) -> tuple[dict[str, Any], ...]:
        # Observation timestamps and GPS accuracy are liveness evidence, not
        # acquisition-plan changes. Publishing them on every Life360 refresh
        # invalidates all desired-state-bound provider facts. Availability and
        # meaningful movement remain part of the semantic authority.
        return tuple({
            key: value
            for key, value in target.items()
            if key not in _TRANSIENT_SEMANTIC_TARGET_FIELDS
        } for target in targets)

    def _set_inputs(
        self,
        targets: Sequence[Mapping[str, Any]],
        settings: Mapping[str, Any],
    ) -> bool:
        normalized_targets, normalized_settings = self._normalize_inputs(
            targets, settings
        )
        normalized_targets = self._merge_non_regressive_positions(
            normalized_targets
        )
        normalized_targets = self._coalesce_dynamic_position_noise(
            normalized_targets
        )
        semantic_bytes = canonical_json_bytes({
            "settings": normalized_settings,
            "targets": self._semantic_targets(normalized_targets),
        })
        semantic_sha = hashlib.sha256(semantic_bytes).hexdigest()
        if semantic_sha == self._semantic_sha256:
            # Retain the latest observation time for regression filtering and
            # freshness scheduling.  Periodically publish that liveness-only
            # change before the worker's dynamic-position lease can expire.
            liveness_changed = normalized_targets != self._targets
            self._targets = normalized_targets
            self._settings = normalized_settings
            lease_elapsed = False
            has_available_dynamic_target = any(
                target.get("kind") in _DYNAMIC_TARGET_KINDS
                and target.get("available") is True
                and target.get("position_observed_at") is not None
                for target in normalized_targets
            )
            if self._last_applied_at is not None and not self._dirty:
                try:
                    now = float(self._clock())
                except (TypeError, ValueError, OverflowError):
                    now = math.nan
                lease_elapsed = (
                    has_available_dynamic_target
                    and math.isfinite(now)
                    and now >= self._last_applied_at
                    + MAX_SEMANTIC_SILENCE_SECONDS
                )
            if lease_elapsed:
                self._lease_refresh_pending = False
                self._cancel_lease_refresh()
                self._dirty = True
                self._notify_authority(None)
                self._increment("accepted_updates")
                self._increment("lease_refreshes")
                return True
            if liveness_changed and has_available_dynamic_target:
                self._lease_refresh_pending = True
                self._schedule_lease_refresh()
            self._increment("coalesced_updates")
            return False
        self._lease_refresh_pending = False
        self._cancel_lease_refresh()
        self._targets = normalized_targets
        self._settings = normalized_settings
        self._semantic_sha256 = semantic_sha
        self._dirty = semantic_sha != self._published_semantic_sha256
        if self._dirty:
            self._notify_authority(None)
        self._increment("accepted_updates")
        return True

    async def async_start(
        self,
        *,
        targets: Sequence[Mapping[str, Any]],
        settings: Mapping[str, Any],
        defer_initial_flush: bool = False,
    ) -> None:
        """Start explicitly and optionally await worker job admission."""

        if self._started or self._stopped:
            raise RuntimeError("desired-state controller kan niet opnieuw starten")
        if type(defer_initial_flush) is not bool:
            raise TypeError("defer_initial_flush moet bool zijn")
        self._set_inputs(targets, settings)
        self._started = True
        self._state = "waiting_for_worker" if defer_initial_flush else "dirty"
        if defer_initial_flush:
            return
        await self.async_flush()

    async def async_update(
        self,
        *,
        targets: Sequence[Mapping[str, Any]],
        settings: Mapping[str, Any],
    ) -> bool:
        """Accept one complete latest snapshot and debounce its publication."""

        if not self._started or self._stopped:
            raise RuntimeError("desired-state controller is niet actief")
        changed = self._set_inputs(targets, settings)
        if changed:
            self._state = "dirty"
            self._schedule_debounce()
        return changed

    def _schedule_debounce(self) -> None:
        if self._stopped or self._retry_blocked:
            return
        if self._debounce_handle is not None:
            self._debounce_handle.cancel()
        loop = asyncio.get_running_loop()
        self._state = "debouncing"
        self._debounce_handle = loop.call_later(
            self._debounce_seconds, self._debounce_ready
        )

    def _cancel_lease_refresh(self) -> None:
        if self._lease_refresh_handle is not None:
            self._lease_refresh_handle.cancel()
            self._lease_refresh_handle = None

    def _schedule_lease_refresh(self) -> None:
        self._cancel_lease_refresh()
        if (
            self._stopped
            or not self._started
            or not self._lease_refresh_pending
            or self._last_applied_at is None
            or self._targets is None
        ):
            return
        if not any(
            target.get("kind") in _DYNAMIC_TARGET_KINDS
            and target.get("available") is True
            and target.get("position_observed_at") is not None
            for target in self._targets
        ):
            self._lease_refresh_pending = False
            return
        try:
            now = float(self._clock())
        except (TypeError, ValueError, OverflowError):
            return
        if not math.isfinite(now):
            return
        delay = max(
            0.0,
            self._last_applied_at + MAX_SEMANTIC_SILENCE_SECONDS - now,
        )
        loop = asyncio.get_running_loop()
        self._lease_refresh_handle = loop.call_later(
            delay, self._lease_refresh_ready
        )

    def _lease_refresh_ready(self) -> None:
        self._lease_refresh_handle = None
        if (
            self._stopped
            or not self._started
            or not self._lease_refresh_pending
            or self._dirty
            or self._last_applied_at is None
        ):
            return
        try:
            now = float(self._clock())
        except (TypeError, ValueError, OverflowError):
            return
        deadline = self._last_applied_at + MAX_SEMANTIC_SILENCE_SECONDS
        if not math.isfinite(now):
            return
        if now < deadline:
            self._schedule_lease_refresh()
            return
        self._lease_refresh_pending = False
        self._dirty = True
        self._notify_authority(None)
        self._increment("accepted_updates")
        self._increment("lease_refreshes")
        self._state = "dirty"
        self._schedule_debounce()

    def _debounce_ready(self) -> None:
        self._debounce_handle = None
        self._ensure_drive()

    def _ensure_drive(self, *, force: bool = False) -> asyncio.Task[None] | None:
        if self._stopped or not self._started or self._targets is None:
            return None
        if self._retry_blocked and not force:
            return None
        if self._drive_task is not None and not self._drive_task.done():
            return self._drive_task
        self._retry_blocked = False
        task = asyncio.create_task(
            self._async_drive(), name="stv3-desired-state-controller"
        )
        self._drive_task = task
        task.add_done_callback(self._drive_done)
        return task

    def _drive_done(self, task: asyncio.Task[None]) -> None:
        if self._drive_task is task:
            self._drive_task = None
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as err:  # defensive: _async_drive normally contains all failures
            self._record_failure(err)

    async def async_flush(self) -> None:
        """Bypass debounce and wait until the current cycle reaches a boundary."""

        if self._stopped:
            return
        if self._debounce_handle is not None:
            self._debounce_handle.cancel()
            self._debounce_handle = None
        task = self._ensure_drive(force=True)
        if task is not None:
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                # Unload owns cancellation of the shared drive task.  An
                # unrelated cancellation of this waiter must still propagate.
                if not self._stopped:
                    raise

    async def async_worker_reconnected(self) -> None:
        """Reprove the worker head and republish only when it changed."""

        if not self._started or self._stopped:
            return
        self._increment("reconnects")
        self._needs_handshake = True
        self._retry_blocked = False
        await self.async_flush()

    async def async_stop(self) -> None:
        """Cancel debounce/in-flight control work; safe for integration unload."""

        if self._stopped:
            return
        self._stopped = True
        self._state = "stopped"
        if self._debounce_handle is not None:
            self._debounce_handle.cancel()
            self._debounce_handle = None
        self._cancel_lease_refresh()
        task = self._drive_task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._drive_task = None

    async_unload = async_stop

    async def _request(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        job_id: str,
    ) -> dict[str, Any]:
        self._increment("worker_requests")
        return await self._requester(
            self._hass,
            job_type,
            payload,
            timeout=self._request_timeout,
            job_id=job_id,
        )

    def _next_snapshot_job_id(self) -> str:
        if self._snapshot_sequence >= MAX_SNAPSHOT_SEQUENCE:
            raise DesiredStateControllerError("snapshotvolgnummer is uitgeput")
        self._snapshot_sequence += 1
        return (
            f"stv3-ds-snapshot-{self._job_token}-"
            f"{self._snapshot_sequence:08x}"
        )

    async def _claim(self) -> int:
        self._state = "claiming"
        self._increment("claim_requests")
        raw = await self._request(
            "desired_state_claim",
            {"producer_id": self._producer_id, "claim_id": self._claim_id},
            job_id=self._claim_job_id,
        )
        result = _small_result(raw, "desired_state_claim")
        _exact_result_keys(
            result,
            {
                "method",
                "status",
                "producer_id",
                "authority_epoch",
                "claim_id",
                "claimed_at",
            },
            "desired_state_claim",
        )
        if (
            result["method"] != WORKER_METHOD
            or result["status"] not in {"claimed", "claim_idempotent"}
            or result["producer_id"] != self._producer_id
            or result["claim_id"] != self._claim_id
        ):
            raise DesiredStateControllerError("authorityclaim antwoordt inconsistent")
        epoch = _required_int(result["authority_epoch"], "authority_epoch", minimum=1)
        _bounded_number(
            result["claimed_at"], "claimed_at", minimum=0.0, maximum=float(2**53)
        )
        return epoch

    async def _snapshot(self) -> _Head:
        self._state = "snapshotting"
        self._increment("snapshot_requests")
        raw = await self._request(
            "desired_state_snapshot",
            {},
            job_id=self._next_snapshot_job_id(),
        )
        result = _small_result(raw, "desired_state_snapshot")
        if result.get("method") != WORKER_METHOD:
            raise DesiredStateControllerError("desired-state snapshotmethode is ongeldig")
        status = result.get("status")
        if status == "unconfigured":
            allowed = {"method", "status"}
            if "producer_id" in result or "authority_epoch" in result:
                allowed.update({"producer_id", "authority_epoch"})
            _exact_result_keys(result, allowed, "desired_state_snapshot")
            producer = result.get("producer_id")
            epoch = result.get("authority_epoch")
            if producer is not None:
                producer = _safe_identifier(producer, "snapshot producer_id")
                epoch = _required_int(epoch, "snapshot authority_epoch", minimum=1)
            return _Head(None, producer, epoch, None)
        if status != "configured":
            raise DesiredStateControllerError("desired-state snapshotstatus is ongeldig")
        _exact_result_keys(
            result,
            {
                "method",
                "status",
                "state_sha256",
                "producer_id",
                "authority_epoch",
                "generation",
                "issued_at",
                "first_received_at",
                "last_received_at",
                "receipt_count",
                "activated_at",
                "target_count",
            },
            "desired_state_snapshot",
        )
        if _required_int(result["target_count"], "target_count") > MAX_TARGETS:
            raise DesiredStateControllerError("snapshot target_count overschrijdt 64")
        _required_int(result["receipt_count"], "receipt_count", minimum=1)
        return _Head(
            _required_sha(result["state_sha256"], "state_sha256"),
            _safe_identifier(result["producer_id"], "snapshot producer_id"),
            _required_int(result["authority_epoch"], "authority_epoch", minimum=1),
            _required_int(result["generation"], "generation"),
        )

    def _recover_pending_head(self) -> None:
        attempt = self._pending_attempt
        if attempt is None or self._head.state_sha256 != attempt.envelope.state_sha256:
            return
        self._accept_attempt(attempt, recovered=True)

    async def _handshake(self) -> None:
        self._authority_epoch = await self._claim()
        self._head = await self._snapshot()
        self._needs_handshake = False
        self._recover_pending_head()

    def _build_attempt(self) -> _Attempt:
        if (
            self._targets is None
            or self._settings is None
            or self._semantic_sha256 is None
            or self._authority_epoch is None
        ):
            raise DesiredStateControllerError("desired-state input is niet compleet")
        pending = self._pending_attempt
        if (
            pending is not None
            and pending.semantic_sha256 == self._semantic_sha256
            and pending.envelope.authority_epoch == self._authority_epoch
            and pending.envelope.previous_state_sha256 == self._head.state_sha256
        ):
            return pending
        if (
            self._head.producer_id == self._producer_id
            and self._head.authority_epoch == self._authority_epoch
            and self._head.generation is not None
        ):
            generation = self._head.generation + 1
        else:
            generation = 1
        if generation > 2**63 - 1:
            raise DesiredStateControllerError("desired-state generation is uitgeput")
        issued_at = max(float(self._clock()), self._last_issued_at)
        envelope = build_desired_state(
            producer_id=self._producer_id,
            authority_epoch=self._authority_epoch,
            generation=generation,
            issued_at=issued_at,
            previous_state_sha256=self._head.state_sha256,
            targets=self._targets,
            settings=self._settings,
        )
        self._last_issued_at = issued_at
        attempt = _Attempt(
            envelope=envelope,
            document=envelope.as_dict(),
            semantic_sha256=self._semantic_sha256,
            job_id=f"stv3-ds-apply-{envelope.state_sha256[:32]}",
        )
        self._pending_attempt = attempt
        return attempt

    async def _apply(self, attempt: _Attempt) -> None:
        self._state = "applying"
        self._increment("apply_requests")
        raw = await self._request(
            "desired_state_apply",
            {"desired_state": attempt.document},
            job_id=attempt.job_id,
        )
        result = _small_result(raw, "desired_state_apply")
        _exact_result_keys(
            result,
            {
                "method",
                "status",
                "state_sha256",
                "producer_id",
                "authority_epoch",
                "generation",
                "issued_at",
                "first_received_at",
                "last_received_at",
                "receipt_count",
                "target_count",
            },
            "desired_state_apply",
        )
        if (
            result["method"] != WORKER_METHOD
            or result["status"] not in {"activated", "idempotent"}
            or result["state_sha256"] != attempt.envelope.state_sha256
            or result["producer_id"] != self._producer_id
            or result["authority_epoch"] != attempt.envelope.authority_epoch
            or result["generation"] != attempt.envelope.generation
            or result["target_count"] != attempt.envelope.target_count
        ):
            raise DesiredStateControllerError("desired-state applyantwoord is inconsistent")
        _required_int(result["receipt_count"], "receipt_count", minimum=1)

    def _accept_attempt(self, attempt: _Attempt, *, recovered: bool) -> None:
        self._head = _Head(
            attempt.envelope.state_sha256,
            self._producer_id,
            attempt.envelope.authority_epoch,
            attempt.envelope.generation,
        )
        self._published_semantic_sha256 = attempt.semantic_sha256
        self._last_applied_at = float(attempt.document["issued_at"])
        self._pending_attempt = None
        self._dirty = self._semantic_sha256 != self._published_semantic_sha256
        if self._lease_refresh_pending:
            self._schedule_lease_refresh()
        else:
            self._cancel_lease_refresh()
        self._notify_authority(None if self._dirty else self._head.state_sha256)
        self._increment("applied_states")
        if recovered:
            self._increment("recovered_receipts")

    def _record_failure(self, error: Exception) -> None:
        self._increment("failures")
        self._last_error_code = _failure_code(error)
        self._last_error_type = type(error).__name__[:64]
        self._retry_blocked = True
        self._needs_handshake = True
        self._state = (
            "offline" if self._last_error_code == "worker_offline" else "error"
        )

    async def _async_drive(self) -> None:
        conflict_refreshes = 0
        try:
            if self._needs_handshake or self._authority_epoch is None:
                previous_head_sha256 = self._head.state_sha256
                await self._handshake()
                if (
                    not self._dirty
                    and self._semantic_sha256 is not None
                    and self._published_semantic_sha256
                    == self._semantic_sha256
                    and self._head.state_sha256 != previous_head_sha256
                ):
                    self._dirty = True
                    self._notify_authority(None)
            if not self._dirty:
                self._state = "idle"
                return
            while self._dirty and not self._stopped:
                attempt = self._build_attempt()
                try:
                    await self._apply(attempt)
                except asyncio.CancelledError:
                    raise
                except Exception as err:
                    if _is_cas_conflict(err) and conflict_refreshes < 1:
                        conflict_refreshes += 1
                        self._increment("cas_refreshes")
                        self._head = await self._snapshot()
                        self._recover_pending_head()
                        if self._pending_attempt is not None:
                            # A different head invalidates the uncertain CAS base.
                            if (
                                self._head.state_sha256
                                != self._pending_attempt.envelope.previous_state_sha256
                            ):
                                self._pending_attempt = None
                        continue
                    raise
                self._accept_attempt(attempt, recovered=False)
                conflict_refreshes = 0
            if not self._stopped:
                self._state = "idle" if not self._dirty else "dirty"
                self._last_error_code = None
                self._last_error_type = None
        except asyncio.CancelledError:
            self._increment("cancellations")
            if not self._stopped:
                self._state = "cancelled"
            raise
        except Exception as err:
            self._record_failure(err)

    @property
    def head_state_sha256(self) -> str | None:
        """Return only the worker-confirmed control-authority identity."""

        return self._head.state_sha256

    def diagnostics(self) -> dict[str, Any]:
        """Return fixed-shape metadata without IDs, names or coordinates."""

        target_count = 0 if self._targets is None else len(self._targets)
        pending_sha = (
            None
            if self._pending_attempt is None
            else self._pending_attempt.envelope.state_sha256
        )
        return {
            "state": self._state,
            "started": self._started,
            "stopped": self._stopped,
            "dirty": self._dirty,
            "in_flight": self._drive_task is not None
            and not self._drive_task.done(),
            "retry_blocked": self._retry_blocked,
            "target_count": target_count,
            "target_limit": MAX_TARGETS,
            "authority_claimed": self._authority_epoch is not None,
            "authority_epoch": self._authority_epoch,
            "head_state_sha256": self._head.state_sha256,
            "head_generation": self._head.generation,
            "pending_state_sha256": pending_sha,
            "debounce_seconds": self._debounce_seconds,
            "request_timeout": self._request_timeout,
            "last_error_code": self._last_error_code,
            "last_error_type": self._last_error_type,
            "counters": dict(self._counters),
        }


__all__ = [
    "DesiredStateController",
    "DesiredStateControllerError",
    "MAX_DIAGNOSTIC_COUNT",
    "MAX_RESPONSE_BYTES",
    "WORKER_METHOD",
]
