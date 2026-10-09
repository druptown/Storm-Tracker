"""Persistente MCS-historiek, gegroepeerd per dynamische RegionEngine."""
from __future__ import annotations

import logging

from homeassistant.helpers.storage import Store

from ..const import DOMAIN

_LOGGER = logging.getLogger(__name__)
STORAGE_VERSION = 1
STORAGE_KEY = f"{DOMAIN}.mcs_history"


class McsHistoryStore:
    """Dunne HA Store-adapter; nooit volledige radarrasters opslaan."""

    def __init__(self, hass) -> None:
        self._store = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        self._data: dict[str, list[dict]] = {}

    async def async_load(self) -> None:
        raw = await self._store.async_load() or {}
        engines = raw.get("engines", {}) if isinstance(raw, dict) else {}
        self._data = engines if isinstance(engines, dict) else {}

    def restore_engine(self, engine_key: str, storm_engine) -> int:
        return storm_engine.restore_mcs_history(self._data.get(engine_key, []))

    async def async_save_engine(self, engine_key: str, storm_engine) -> None:
        snapshots = storm_engine.export_mcs_history()
        if snapshots:
            self._data[engine_key] = snapshots
        else:
            self._data.pop(engine_key, None)
        await self._store.async_save({"engines": self._data})

    async def async_remove_engine(self, engine_key: str) -> None:
        self._data.pop(engine_key, None)
        await self._store.async_save({"engines": self._data})
