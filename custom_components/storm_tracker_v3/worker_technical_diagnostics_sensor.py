"""Event-driven HA sensor for bounded worker technical diagnostics."""
from __future__ import annotations

from copy import deepcopy
from collections.abc import Mapping
import math
import time
from typing import Any

from homeassistant.components.sensor import SensorEntity
from homeassistant.core import callback

from .const import (
    DATA_ENTRY_RUNTIMES,
    DATA_WORKER_TECHNICAL_DIAGNOSTICS,
    DATA_WORKER_TECHNICAL_SOURCE,
    DOMAIN,
    INTEGRATION_VERSION,
    WORKER_TECHNICAL_DIAGNOSTICS_UPDATE_EVENT,
)
from .worker_technical_diagnostics import (
    CONTRACT,
    MAX_DIAGNOSTIC_BYTES,
    build_worker_technical_diagnostics,
    canonical_diagnostics_bytes,
)


def _safe_now() -> float:
    now = time.time()
    return now if math.isfinite(now) and now >= 0.0 else 0.0


def _unknown_document() -> dict[str, Any]:
    return build_worker_technical_diagnostics(None, now=_safe_now())


def _read_safe_document(hass) -> dict[str, Any]:
    """Re-prove the cached projection from only its retained safe source."""

    domain_data = hass.data.get(DOMAIN, {})
    if not isinstance(domain_data, Mapping):
        return _unknown_document()
    document = domain_data.get(DATA_WORKER_TECHNICAL_DIAGNOSTICS)
    source = domain_data.get(DATA_WORKER_TECHNICAL_SOURCE)
    if type(document) is not dict or document.get("contract") != CONTRACT:
        return _unknown_document()
    projected_at = document.get("projected_at")
    try:
        reproved = build_worker_technical_diagnostics(
            source if isinstance(source, Mapping) else None,
            now=projected_at,
        )
        if reproved != document:
            return _unknown_document()
        if len(canonical_diagnostics_bytes(reproved)) > MAX_DIAGNOSTIC_BYTES:
            return _unknown_document()
    except (TypeError, ValueError):
        return _unknown_document()
    return reproved


def _unknown_control_diagnostics() -> dict[str, Any]:
    return {
        "state": "unknown",
        "started": False,
        "dirty": False,
        "in_flight": False,
        "retry_blocked": False,
        "target_count": 0,
        "authority_claimed": False,
        "head_generation": None,
        "last_error_code": None,
        "last_error_type": None,
        "reconnects": 0,
        "worker_requests": 0,
        "failures": 0,
    }


def _read_control_diagnostics(hass) -> dict[str, Any]:
    """Project fixed, non-sensitive desired-state controller metadata."""

    unknown = _unknown_control_diagnostics()
    domain_data = hass.data.get(DOMAIN, {})
    records = (
        domain_data.get(DATA_ENTRY_RUNTIMES)
        if isinstance(domain_data, Mapping)
        else None
    )
    if not isinstance(records, Mapping) or len(records) != 1:
        return unknown
    record = next(iter(records.values()))
    runtime = record.get("runtime") if isinstance(record, Mapping) else None
    diagnostics = getattr(runtime, "diagnostics", None)
    if not callable(diagnostics):
        return unknown
    try:
        runtime_document = diagnostics()
        controller = (
            runtime_document.get("controller")
            if isinstance(runtime_document, Mapping)
            else None
        )
        if not isinstance(controller, Mapping):
            return unknown
        counters = controller.get("counters")
        counters = counters if isinstance(counters, Mapping) else {}
        state = controller.get("state")
        error_code = controller.get("last_error_code")
        error_type = controller.get("last_error_type")
        generation = controller.get("head_generation")
        projected = {
            "state": state[:32] if isinstance(state, str) else "unknown",
            "started": controller.get("started") is True,
            "dirty": controller.get("dirty") is True,
            "in_flight": controller.get("in_flight") is True,
            "retry_blocked": controller.get("retry_blocked") is True,
            "target_count": controller.get("target_count", 0),
            "authority_claimed": controller.get("authority_claimed") is True,
            "head_generation": generation,
            "last_error_code": (
                error_code[:64] if isinstance(error_code, str) else None
            ),
            "last_error_type": (
                error_type[:64] if isinstance(error_type, str) else None
            ),
            "reconnects": counters.get("reconnects", 0),
            "worker_requests": counters.get("worker_requests", 0),
            "failures": counters.get("failures", 0),
        }
        for key in (
            "target_count",
            "reconnects",
            "worker_requests",
            "failures",
        ):
            value = projected[key]
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
                or value > 2**31 - 1
            ):
                return unknown
        if generation is not None and (
            not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation < 0
            or generation > 2**63 - 1
        ):
            return unknown
        return projected
    except Exception:
        return unknown


class WorkerTechnicalDiagnosticsSensor(SensorEntity):
    """Expose only the fixed technical projection; never poll or recompute work."""

    _attr_icon = "mdi:server-network"
    _attr_name = "STV3 Worker Technical Diagnostics"
    _attr_should_poll = False

    def __init__(self, unique_id_prefix: str) -> None:
        if not isinstance(unique_id_prefix, str) or not unique_id_prefix:
            raise ValueError("worker technical unique_id_prefix ontbreekt")
        self._attr_unique_id = (
            f"{unique_id_prefix}_worker_technical_diagnostics"
        )
        self._document = _unknown_document()
        self._control = _unknown_control_diagnostics()

    @callback
    def _refresh(self, _event=None) -> None:
        self._document = _read_safe_document(self.hass)
        self._control = _read_control_diagnostics(self.hass)
        self.async_write_ha_state()

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        unsubscribe = self.hass.bus.async_listen(
            WORKER_TECHNICAL_DIAGNOSTICS_UPDATE_EVENT,
            self._refresh,
        )
        if not callable(unsubscribe):
            raise RuntimeError("worker technical eventlistener mist cleanup")
        self.async_on_remove(unsubscribe)
        self._refresh()

    @property
    def available(self) -> bool:
        """Unknown is a valid fail-closed technical state, not unavailability."""

        return True

    @property
    def native_value(self) -> str:
        return self._document["status"]

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        attributes = deepcopy(self._document)
        attributes["integration_version"] = INTEGRATION_VERSION
        attributes["desired_state_control"] = deepcopy(self._control)
        return attributes


__all__ = ["WorkerTechnicalDiagnosticsSensor"]
