"""Officiële Météo-France DPRadar-provider voor Europees Frankrijk."""
from __future__ import annotations

import asyncio
import logging

from .base import Capability, CoverageResult
from .odim_hdf5 import parse_odim_rainfall

_LOGGER = logging.getLogger(__name__)
PRODUCT_URL = "https://public-api.meteofrance.fr/public/DPRadar/v1/mosaiques/METROPOLE/observations/LAME_D_EAU/produit"
MAX_FILE_BYTES = 40 * 1024 * 1024


class MeteoFranceRadarProvider:
    plugin_id = "meteofrance_radar"
    capabilities = frozenset({Capability.RADAR})
    priority = 100

    def __init__(self, session, token: str) -> None:
        self._session, self._token, self._areas = session, token, ()

    def supports(self, area):
        margin = area.horizon_km / 80.0
        ok = 40.5 - margin <= area.center_lat <= 52.0 + margin and -6.0 - margin <= area.center_lon <= 10.0 + margin
        return CoverageResult(ok, 1.0 if ok else 0.0, 0.99 if ok else 0.0, "Météo-France 500 m" if ok else "buiten Météo-France-dekking")

    async def async_start(self, context): self._areas = tuple(context.config.get("areas", (context.area,)))
    async def async_update_areas(self, areas): self._areas = tuple(areas)
    async def async_stop(self): self._areas = ()

    async def async_fetch(self):
        headers = {"Authorization": f"Bearer {self._token}"}
        async with self._session.get(PRODUCT_URL, params={"maille": 500}, headers=headers) as response:
            response.raise_for_status()
            if response.content_length and response.content_length > MAX_FILE_BYTES:
                raise ValueError("Météo-France-bestand overschrijdt veiligheidslimiet")
            payload = await response.read()
        if len(payload) > MAX_FILE_BYTES:
            raise ValueError("Météo-France-bestand overschrijdt veiligheidslimiet")
        observations = await asyncio.to_thread(parse_odim_rainfall, payload, self._areas, source=self.plugin_id, quality=0.99, max_age_seconds=25 * 60, sample_stride=8, accumulation_minutes=5)
        _LOGGER.info("Météo-France radar: %d observaties binnen actieve engines", len(observations))
        return observations
