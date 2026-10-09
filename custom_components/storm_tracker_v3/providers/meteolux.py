"""Publieke MeteoLux-nowcast als lokale validatiebron."""
from __future__ import annotations

import json

from ..engine.bounded_payload import iter_limited_async
from .base import Capability, CoverageResult

URL = "https://metapi.ana.lu/api/v1/metapp/weather"
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
_HTTP_CHUNK_BYTES = 64 * 1024


async def _read_bounded_response(response, *, maximum_bytes: int) -> bytes:
    headers = response.headers
    encoding = headers.get("Content-Encoding")
    if encoding and encoding.lower() != "identity":
        raise ValueError("MeteoLux-response gebruikt onverwachte transportcompressie")
    return await iter_limited_async(
        response.content.iter_chunked(_HTTP_CHUNK_BYTES),
        maximum_bytes=maximum_bytes,
        declared_bytes=headers.get("Content-Length"),
        require_declared_match=headers.get("Content-Length") is not None,
    )


class MeteoLuxProvider:
    plugin_id = "meteolux"
    capabilities = frozenset({Capability.NOWCAST})
    priority = 100

    def __init__(self, session):
        self._session, self._areas = session, ()
        self.diagnostics = {}

    def supports(self, area):
        margin = area.horizon_km / 100.0
        ok = 48.5 - margin <= area.center_lat <= 51.0 + margin and 4.5 - margin <= area.center_lon <= 7.5 + margin
        return CoverageResult(ok, 1.0 if ok else 0.0, 0.95 if ok else 0.0, "MeteoLux lokale nowcast" if ok else "buiten MeteoLux-dekking")

    async def async_start(self, context): self._areas = tuple(context.config.get("areas", (context.area,)))
    async def async_update_areas(self, areas): self._areas = tuple(areas)
    async def async_stop(self): self._areas = ()

    async def async_fetch(self):
        snapshots = []
        for area in self._areas:
            async with self._session.get(
                URL,
                params={
                    "langcode": "en",
                    "lat": area.center_lat,
                    "long": area.center_lon,
                },
                headers={"Accept-Encoding": "identity"},
            ) as response:
                response.raise_for_status()
                raw = await _read_bounded_response(
                    response,
                    maximum_bytes=MAX_RESPONSE_BYTES,
                )
                snapshots.append(json.loads(raw.decode("utf-8")))
        self.diagnostics = {"areas_queried": len(snapshots), "source_role": "validation_only", "official_api": URL}
        return []
