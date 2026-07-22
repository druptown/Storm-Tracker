"""Officiële Météo-France DPRadar-provider voor Europees Frankrijk."""
from __future__ import annotations

import asyncio
import logging
import time

from .base import Capability, CoverageResult
from .odim_hdf5 import parse_odim_rainfall

_LOGGER = logging.getLogger(__name__)
PRODUCT_URL = "https://public-api.meteofrance.fr/public/DPRadar/v1/mosaiques/METROPOLE/observations/LAME_D_EAU/produit"
TOKEN_URL = "https://portail-api.meteofrance.fr/token"
MAX_FILE_BYTES = 40 * 1024 * 1024


class MeteoFranceRadarProvider:
    plugin_id = "meteofrance_radar"
    capabilities = frozenset({Capability.RADAR})
    priority = 100

    def __init__(
        self, session, token: str | None = None,
        application_id: str | None = None,
    ) -> None:
        self._session, self._static_token, self._areas = session, token, ()
        self._application_id = (
            (application_id or "").removeprefix("Basic ").strip()
        )
        self._access_token_value = None
        self._access_token_expires_at = 0.0
        self.overlay = None

    async def _access_token(self, *, force: bool = False) -> str:
        if self._application_id:
            if (
                not force
                and self._access_token_value
                and time.monotonic() < self._access_token_expires_at - 60
            ):
                return self._access_token_value
            async with self._session.post(
                TOKEN_URL,
                data={"grant_type": "client_credentials"},
                headers={"Authorization": f"Basic {self._application_id}"},
            ) as response:
                response.raise_for_status()
                body = await response.json(content_type=None)
            token = body.get("access_token")
            if not token:
                raise ValueError(
                    "Météo-France authenticatie gaf geen access_token"
                )
            self._access_token_value = str(token)
            self._access_token_expires_at = (
                time.monotonic() + float(body.get("expires_in", 3600))
            )
            return self._access_token_value
        if self._static_token:
            return self._static_token
        raise ValueError("Météo-France credential ontbreekt")

    def supports(self, area):
        margin = area.horizon_km / 80.0
        ok = (
            40.5 - margin <= area.center_lat <= 52.0 + margin
            and -6.0 - margin <= area.center_lon <= 10.0 + margin
        )
        return CoverageResult(
            ok, 1.0 if ok else 0.0, 0.99 if ok else 0.0,
            "Météo-France 500 m" if ok else "buiten Météo-France-dekking",
        )

    async def async_start(self, context):
        self._areas = tuple(context.config.get("areas", (context.area,)))

    async def async_update_areas(self, areas):
        self._areas = tuple(areas)

    async def async_stop(self):
        self._areas = ()

    async def async_fetch(self):
        payload = None
        for attempt in range(2):
            token = await self._access_token(force=attempt > 0)
            headers = {"Authorization": f"Bearer {token}"}
            async with self._session.get(
                PRODUCT_URL, params={"maille": 500}, headers=headers
            ) as response:
                if (
                    response.status == 401
                    and self._application_id
                    and attempt == 0
                ):
                    continue
                response.raise_for_status()
                if (
                    response.content_length
                    and response.content_length > MAX_FILE_BYTES
                ):
                    raise ValueError(
                        "Météo-France-bestand overschrijdt veiligheidslimiet"
                    )
                payload = await response.read()
                break
        if payload is None:
            raise ValueError("Météo-France authenticatie bleef ongeldig")
        if len(payload) > MAX_FILE_BYTES:
            raise ValueError(
                "Météo-France-bestand overschrijdt veiligheidslimiet"
            )
        overlays = []
        observations = await asyncio.to_thread(
            parse_odim_rainfall,
            payload,
            self._areas,
            source=self.plugin_id,
            quality=0.99,
            max_age_seconds=25 * 60,
            sample_stride=8,
            accumulation_minutes=5,
            overlay_out=overlays,
        )
        self.overlay = overlays[0] if overlays else None
        _LOGGER.info(
            "Météo-France radar: %d observaties binnen actieve engines",
            len(observations),
        )
        return observations
