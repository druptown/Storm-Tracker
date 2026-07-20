"""Officiële Met Office 1 km radarcomposiet voor Groot-Brittannië."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import logging
from xml.etree import ElementTree

from .base import Capability, CoverageResult
from .odim_hdf5 import parse_odim_rainfall

_LOGGER = logging.getLogger(__name__)
BUCKET_URL = "https://met-office-radar-obs-data.s3.eu-west-2.amazonaws.com/"
MAX_FILE_BYTES = 20 * 1024 * 1024


def latest_key_from_listing(xml: str) -> str | None:
    root = ElementTree.fromstring(xml)
    keys = [node.text for node in root.iter() if node.tag.endswith("Key") and node.text and node.text.endswith(".h5")]
    return max(keys, default=None)


class MetOfficeRadarProvider:
    plugin_id = "met_office_radar"
    capabilities = frozenset({Capability.RADAR})
    priority = 100

    def __init__(self, session) -> None:
        self._session = session
        self._areas = ()
        self._last_key = None

    def supports(self, area):
        margin = area.horizon_km / 80.0
        ok = 48.0 - margin <= area.center_lat <= 62.0 + margin and -12.0 - margin <= area.center_lon <= 4.0 + margin
        return CoverageResult(ok, 1.0 if ok else 0.0, 0.99 if ok else 0.0, "Met Office 1 km UK" if ok else "buiten Met Office-dekking")

    async def async_start(self, context): self._areas = tuple(context.config.get("areas", (context.area,)))
    async def async_update_areas(self, areas): self._areas = tuple(areas)
    async def async_stop(self): self._areas = ()

    async def _latest_key(self):
        now = datetime.now(timezone.utc)
        keys = []
        for day in (now, now - timedelta(days=1)):
            prefix = day.strftime("radar/%Y/%m/%d/")
            async with self._session.get(BUCKET_URL, params={"list-type": "2", "prefix": prefix}) as response:
                response.raise_for_status()
                key = latest_key_from_listing(await response.text())
                if key: keys.append(key)
        return max(keys, default=None)

    async def async_fetch(self):
        key = await self._latest_key()
        if not key or key == self._last_key:
            return []
        async with self._session.get(BUCKET_URL + key) as response:
            response.raise_for_status()
            if response.content_length and response.content_length > MAX_FILE_BYTES:
                raise ValueError("Met Office-bestand overschrijdt veiligheidslimiet")
            payload = await response.read()
        if len(payload) > MAX_FILE_BYTES:
            raise ValueError("Met Office-bestand overschrijdt veiligheidslimiet")
        observations = await asyncio.to_thread(parse_odim_rainfall, payload, self._areas, source=self.plugin_id, quality=0.99, max_age_seconds=45 * 60)
        self._last_key = key
        _LOGGER.info("Met Office radar: %d observaties binnen actieve engines", len(observations))
        return observations
