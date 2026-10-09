"""Officiële Met Office 1 km radarcomposiet voor Groot-Brittannië."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import logging
from xml.etree import ElementTree

from ..engine.bounded_payload import iter_limited_async
from .base import Capability, CoverageResult

_LOGGER = logging.getLogger(__name__)
BUCKET_URL = "https://met-office-radar-obs-data.s3.eu-west-2.amazonaws.com/"
MAX_FILE_BYTES = 20 * 1024 * 1024
MAX_LIST_RESPONSE_BYTES = 2 * 1024 * 1024
_HTTP_CHUNK_BYTES = 64 * 1024


async def _read_bounded_response(response, *, maximum_bytes: int) -> bytes:
    headers = response.headers
    encoding = headers.get("Content-Encoding")
    if encoding and encoding.lower() != "identity":
        raise ValueError(
            "Met Office-response gebruikt onverwachte transportcompressie"
        )
    return await iter_limited_async(
        response.content.iter_chunked(_HTTP_CHUNK_BYTES),
        maximum_bytes=maximum_bytes,
        declared_bytes=headers.get("Content-Length"),
        require_declared_match=headers.get("Content-Length") is not None,
    )


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
        self.overlay = None
        self._hass = None
        self._parse_executor = None
        self._last_observations = []

    def supports(self, area):
        margin = area.horizon_km / 80.0
        ok = 48.0 - margin <= area.center_lat <= 62.0 + margin and -12.0 - margin <= area.center_lon <= 4.0 + margin
        return CoverageResult(ok, 1.0 if ok else 0.0, 0.99 if ok else 0.0, "Met Office 1 km UK" if ok else "buiten Met Office-dekking")

    async def async_start(self, context):
        self._hass = context.hass
        self._areas = tuple(context.config.get("areas", (context.area,)))
        self._parse_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="storm_tracker_met_office",
        )
    async def async_update_areas(self, areas): self._areas = tuple(areas)
    async def async_stop(self):
        self._areas = ()
        if self._parse_executor is not None:
            self._parse_executor.shutdown(wait=False, cancel_futures=True)
            self._parse_executor = None

    async def _find_latest_key(self):
        now = datetime.now(timezone.utc)
        keys = []
        for day in (now, now - timedelta(days=1)):
            prefix = day.strftime("radar/%Y/%m/%d/")
            async with self._session.get(
                BUCKET_URL,
                params={"list-type": "2", "prefix": prefix},
                headers={"Accept-Encoding": "identity"},
            ) as response:
                response.raise_for_status()
                payload = await _read_bounded_response(
                    response,
                    maximum_bytes=MAX_LIST_RESPONSE_BYTES,
                )
                key = latest_key_from_listing(payload.decode("utf-8"))
                if key: keys.append(key)
        return max(keys, default=None)

    async def async_fetch(self):
        key = await self._find_latest_key()
        if not key or key == self._last_key:
            return []
        if self._hass is not None:
            try:
                from ..worker_bridge import async_request_worker_job
                from .dwd_radolan import read_radar_worker_artifact

                result = await async_request_worker_job(
                    self._hass,
                    "odim_hdf5_fetch",
                    {
                        "source": self.plugin_id,
                        "url": BUCKET_URL + key,
                        "max_file_bytes": MAX_FILE_BYTES,
                        "quality": 0.99,
                        "max_age_seconds": 45 * 60,
                        "sample_stride": 4,
                        "areas": [
                            {
                                "center_lat": area.center_lat,
                                "center_lon": area.center_lon,
                                "horizon_km": area.horizon_km,
                            }
                            for area in self._areas
                        ],
                    },
                    timeout=40,
                )
                observations, self.overlay = await asyncio.get_running_loop().run_in_executor(
                    self._parse_executor,
                    read_radar_worker_artifact,
                    result,
                    self.plugin_id,
                    45 * 60,
                )
                from ..worker_radar_client import remember_worker_radar_artifact
                self.last_worker_artifact = remember_worker_radar_artifact(
                    self._hass, self.plugin_id, result
                )
                self._last_key = key
                self._last_observations = list(observations)
                _LOGGER.info(
                    "Met Office worker: %d observaties binnen actieve engines",
                    len(observations),
                )
                return observations
            except (
                asyncio.TimeoutError,
                KeyError,
                OSError,
                RuntimeError,
                TypeError,
                ValueError,
            ) as err:
                _LOGGER.warning(
                    "Met Office-worker niet bruikbaar; laatst geldige workerdata blijft actief: %s",
                    err,
                )
                return list(self._last_observations)
        return []
