"""Officiële Météo-France DPRadar-provider voor Europees Frankrijk."""
from __future__ import annotations

import asyncio
import json
import logging
import time

from ..engine.bounded_payload import iter_limited_async
from .base import Capability, CoverageResult
from .odim_hdf5 import parse_odim_rainfall

_LOGGER = logging.getLogger(__name__)
PRODUCT_URL = "https://public-api.meteofrance.fr/public/DPRadar/v1/mosaiques/METROPOLE/observations/LAME_D_EAU/produit"
TOKEN_URL = "https://portail-api.meteofrance.fr/token"
MAX_FILE_BYTES = 40 * 1024 * 1024
MAX_TOKEN_RESPONSE_BYTES = 512 * 1024
_HTTP_CHUNK_BYTES = 64 * 1024


async def _read_bounded_response(response, *, maximum_bytes: int) -> bytes:
    """Lees één ongecomprimeerde HTTP-body onder een expliciet bytecontract."""
    encoding = response.headers.get("Content-Encoding")
    if encoding and encoding.lower() != "identity":
        raise ValueError(
            "Météo-France-response gebruikt onverwachte transportcompressie"
        )
    return await iter_limited_async(
        response.content.iter_chunked(_HTTP_CHUNK_BYTES),
        maximum_bytes=maximum_bytes,
        declared_bytes=response.headers.get("Content-Length"),
        require_declared_match=True,
    )


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
        self._hass = None
        self._last_observations = []

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
                headers={
                    "Accept-Encoding": "identity",
                    "Authorization": f"Basic {self._application_id}",
                },
            ) as response:
                response.raise_for_status()
                raw = await _read_bounded_response(
                    response,
                    maximum_bytes=MAX_TOKEN_RESPONSE_BYTES,
                )
                body = json.loads(raw.decode("utf-8"))
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
        self._hass = context.hass
        self._areas = tuple(context.config.get("areas", (context.area,)))

    async def async_update_areas(self, areas):
        self._areas = tuple(areas)

    async def async_stop(self):
        self._areas = ()

    async def async_fetch(self):
        if self._hass is not None:
            try:
                from ..worker_radar_client import async_fetch_worker_radar
                observations, overlay, diagnostics, artifact = await async_fetch_worker_radar(
                    self._hass, "meteofrance_radar_fetch",
                    {
                        "credential_ref": "worker_environment",
                        "areas": [{"center_lat": area.center_lat, "center_lon": area.center_lon, "horizon_km": area.horizon_km} for area in self._areas],
                    },
                    provider=self.plugin_id, timeout=70,
                    max_age_seconds=25 * 60,
                )
                self.overlay = overlay
                self.last_worker_artifact = artifact
                self.diagnostics = diagnostics
                self._last_observations = list(observations)
                return observations
            except Exception as err:
                _LOGGER.warning("Meteo-France-workerfout; lokale verwerking blijft uit: %s", err)
                return list(self._last_observations)
        payload = None
        for attempt in range(2):
            token = await self._access_token(force=attempt > 0)
            headers = {
                "Accept-Encoding": "identity",
                "Authorization": f"Bearer {token}",
            }
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
                payload = await _read_bounded_response(
                    response,
                    maximum_bytes=MAX_FILE_BYTES,
                )
                break
        if payload is None:
            raise ValueError("Météo-France authenticatie bleef ongeldig")
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
