"""Worker-first NOAA Enterprise Rain Rate global satellite provider."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import time

from .base import Capability, CoverageResult

_LOGGER = logging.getLogger(__name__)
MAX_FRAME_AGE_SECONDS = 45 * 60
POLL_INTERVAL_SECONDS = 10 * 60


class NoaaEnterpriseRainRateProvider:
    """Thin HA adapter; download and NetCDF processing stay in the worker."""

    plugin_id = "noaa_enterprise_rainrate"
    capabilities = frozenset({Capability.RADAR})
    priority = 30

    def __init__(self, hass=None) -> None:
        self._hass = hass
        self._areas = ()
        self._artifact_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="storm_tracker_noaa_err_artifact",
        )
        self._last_attempt = 0.0
        self._last_success_ts = None
        self._last_observations = []
        self.overlay = None
        self.healthy = False
        self.diagnostics = {"status": "standby"}

    def supports(self, area):
        margin = float(area.horizon_km) / 110.574
        supported = -60.0 - margin <= area.center_lat <= 70.0 + margin
        return CoverageResult(
            supported,
            1.0 if supported else 0.0,
            0.72 if supported else 0.0,
            (
                "NOAA ERR wereldwijde satellietregen"
                if supported else "buiten NOAA ERR-dekking"
            ),
        )

    async def async_start(self, context):
        self._hass = context.hass
        self._areas = tuple(context.config.get("areas", (context.area,)))
        if self._artifact_executor is None:
            self._artifact_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="storm_tracker_noaa_err_artifact",
            )

    async def async_update_areas(self, areas):
        self._areas = tuple(areas)

    async def async_stop(self):
        self._areas = ()
        if self._artifact_executor is not None:
            self._artifact_executor.shutdown(wait=False, cancel_futures=True)
            self._artifact_executor = None

    async def async_fetch(self, areas=None):
        selected = tuple(areas or self._areas)
        if not selected:
            self.sleep("geen actieve regio's")
            return []
        now = time.time()
        if (
            self._last_success_ts is not None
            and now - self._last_attempt < POLL_INTERVAL_SECONDS
        ):
            return list(self._last_observations)
        self._last_attempt = now
        try:
            from ..worker_bridge import async_request_worker_job
            from .dwd_radolan import read_radar_worker_artifact

            result = await async_request_worker_job(
                self._hass,
                "enterprise_rainrate_fetch",
                {
                    "areas": [
                        {
                            "center_lat": area.center_lat,
                            "center_lon": area.center_lon,
                            "horizon_km": area.horizon_km,
                        }
                        for area in selected
                    ],
                },
                timeout=40,
            )
            observations, overlay = await asyncio.get_running_loop().run_in_executor(
                self._artifact_executor,
                read_radar_worker_artifact,
                result,
                self.plugin_id,
                MAX_FRAME_AGE_SECONDS,
            )
            from ..worker_radar_client import remember_worker_radar_artifact
            self.last_worker_artifact = remember_worker_radar_artifact(
                self._hass, self.plugin_id, result
            )
            self.overlay = overlay
            self._last_observations = list(observations)
            self._last_success_ts = time.time()
            self.healthy = True
            self.diagnostics = {
                "status": "active",
                "mode": "worker_first",
                "source": "worker",
                "frame_timestamp": result.get("frame_timestamp"),
                "observations": len(observations),
                "overlay_runs": result.get("overlay_run_count", 0),
                "download_bytes": result.get("download_bytes", 0),
                "source_key": result.get("source_key"),
            }
            return list(observations)
        except (
            asyncio.TimeoutError,
            KeyError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
        ) as err:
            self.healthy = self._last_success_ts is not None
            self.diagnostics = {
                "status": "fallback" if self.healthy else "error",
                "mode": "worker_first",
                "error": f"{type(err).__name__}: {err}",
            }
            _LOGGER.warning(
                "NOAA ERR-worker niet bruikbaar; laatst geldige data blijft actief: %s",
                err,
            )
            return list(self._last_observations)

    def sleep(self, reason="bruikbare radar beschikbaar"):
        self.diagnostics = {
            **self.diagnostics,
            "status": "standby",
            "reason": reason,
        }
