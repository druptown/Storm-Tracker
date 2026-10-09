"""DWD RADOLAN/RADVOR RV nationale radarprovider voor Duitsland."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import io
import hashlib
import json
import logging
import math
from pathlib import Path
import tarfile

import h5py
import numpy as np
from pyproj import CRS, Transformer

from ..engine.observation import Observation, ObservationType
from ..engine.bounded_payload import decompress_limited, read_limited_stream
from .base import Capability, CoverageArea, CoverageResult
from .raster_components import extract_components, extract_intensity_runs

_LOGGER = logging.getLogger(__name__)

RV_LATEST_URL = (
    "https://opendata.dwd.de/weather/radar/composite/rv/"
    "composite_rv_LATEST.tar"
)
MAX_ARCHIVE_BYTES = 8 * 1024 * 1024
MAX_FRAME_BYTES = 40 * 1024 * 1024
MAX_FRAME_AGE_SECONDS = 15 * 60
WORKER_ARTIFACT_ROOT = Path("/share/storm_tracker_worker")
MAX_WORKER_ARTIFACT_BYTES = 32 * 1024 * 1024
MAX_WORKER_OBSERVATIONS = 10_000
MAX_WORKER_GEOMETRY_POINTS = 250_000
MAX_WORKER_OVERLAY_RUNS = 25_000
MAX_WORKER_FUTURE_SECONDS = 2 * 60 * 60
MAX_WORKER_PAST_SECONDS = 6 * 60 * 60
MAX_WORKER_RING_POINTS = 64
MAX_WORKER_ID_LENGTH = 512


def _text(value) -> str:
    return value.decode("ascii") if isinstance(value, bytes) else str(value)


def _intensity(rain_rate: float) -> int:
    if rain_rate < 0.1:
        return 0
    for level, threshold in enumerate((0.1, 0.5, 1, 2, 5, 10, 25), start=1):
        if rain_rate < threshold:
            return max(1, level - 1)
    return 8


def _shadow_frame_timestamp(
    observations: list[Observation],
    overlay: dict | None,
) -> float | None:
    if observations:
        return observations[0].timestamp
    if isinstance(overlay, dict):
        return overlay.get("timestamp")
    return None


def _worker_point(value) -> tuple[float, float]:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ValueError("Workerartifact contains invalid geometry")
    lat, lon = float(value[0]), float(value[1])
    if (
        not math.isfinite(lat)
        or not math.isfinite(lon)
        or not -90.0 <= lat <= 90.0
        or not -180.0 <= lon <= 180.0
    ):
        raise ValueError("Workerartifact contains invalid geometry")
    return lat, lon


def _worker_points(value) -> tuple[tuple[float, float], ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError("Workerartifact contains invalid geometry")
    return tuple(_worker_point(point) for point in value)


def _worker_polygons(value) -> tuple:
    if not isinstance(value, (list, tuple)):
        raise ValueError("Workerartifact contains invalid geometry")
    polygons = []
    for polygon in value:
        if not isinstance(polygon, (list, tuple)):
            raise ValueError("Workerartifact contains invalid geometry")
        rings = []
        for ring in polygon:
            points = _worker_points(ring)
            if len(points) > MAX_WORKER_RING_POINTS:
                raise ValueError("Workerartifact exceeds ring budget")
            rings.append(points)
        polygons.append(tuple(rings))
    return tuple(polygons)


def _validate_overlay(
    overlay: dict,
    *,
    expected_provider: str,
    expected_timestamp: float | None = None,
) -> int:
    if not isinstance(overlay, dict) or overlay.get("source") != expected_provider:
        raise ValueError("Workerartifact has inconsistent overlay metadata")
    try:
        timestamp = float(overlay["timestamp"])
    except (KeyError, TypeError, ValueError) as err:
        raise ValueError("Workerartifact has invalid overlay time") from err
    runs = overlay.get("runs")
    if (
        not math.isfinite(timestamp)
        or (
            expected_timestamp is not None
            and abs(timestamp - float(expected_timestamp)) > 1.0
        )
        or not isinstance(runs, list)
        or len(runs) > MAX_WORKER_OVERLAY_RUNS
    ):
        raise ValueError("Workerartifact has inconsistent overlay metadata")
    points = 0
    for run in runs:
        if not isinstance(run, dict):
            raise ValueError("Workerartifact contains an invalid overlay run")
        try:
            intensity = int(run["intensity"])
        except (KeyError, TypeError, ValueError) as err:
            raise ValueError("Workerartifact contains an invalid overlay run") from err
        ring = _worker_points(run.get("ring"))
        if not 1 <= intensity <= 255 or not 4 <= len(ring) <= MAX_WORKER_RING_POINTS:
            raise ValueError("Workerartifact contains an invalid overlay run")
        points += len(ring)
        if points > MAX_WORKER_GEOMETRY_POINTS:
            raise ValueError("Workerartifact exceeds overlay geometry budget")
    return points


def _observation_from_worker(value: dict, expected_source: str) -> Observation:
    """Zet één gevalideerde workerobservatie om naar het interne model."""
    if value.get("obs_type") != ObservationType.RADAR.value:
        raise ValueError("Workerartifact bevat een ongeldig observatietype")
    lat = float(value["lat"])
    lon = float(value["lon"])
    timestamp = float(value["timestamp"])
    intensity = int(value["intensity"])
    area_km2 = float(value["area_km2"])
    parent_area_km2 = float(value["parent_area_km2"])
    quality = float(value["quality"])
    radar_cell_id = str(value["radar_cell_id"])
    parent_system_id = str(value["parent_system_id"])
    if (
        not all(
            math.isfinite(item)
            for item in (
                lat, lon, timestamp, area_km2, parent_area_km2, quality
            )
        )
        or not -90.0 <= lat <= 90.0
        or not -180.0 <= lon <= 180.0
        or not 0 <= intensity <= 255
        or area_km2 < 0.0
        or parent_area_km2 < 0.0
        or not 0.0 <= quality <= 1.0
        or not radar_cell_id
        or len(radar_cell_id) > MAX_WORKER_ID_LENGTH
        or not parent_system_id
        or len(parent_system_id) > MAX_WORKER_ID_LENGTH
    ):
        raise ValueError("Workerartifact contains invalid radar values")
    footprint_points = _worker_points(value.get("footprint_points", ()))
    footprint_polygons = _worker_polygons(
        value.get("footprint_polygons", ())
    )
    parent_footprint_points = _worker_points(
        value.get("parent_footprint_points", ())
    )
    parent_footprint_polygons = _worker_polygons(
        value.get("parent_footprint_polygons", ())
    )
    return Observation(
        obs_type=ObservationType.RADAR,
        lat=lat,
        lon=lon,
        timestamp=timestamp,
        intensity=intensity,
        area_km2=area_km2,
        quality=quality,
        footprint_points=footprint_points,
        footprint_polygons=footprint_polygons,
        radar_cell_id=radar_cell_id,
        parent_system_id=parent_system_id,
        parent_area_km2=parent_area_km2,
        parent_footprint_points=parent_footprint_points,
        parent_footprint_polygons=parent_footprint_polygons,
        source=expected_source,
    )


def read_radar_worker_artifact(
    metadata: dict,
    expected_provider: str = "dwd_radolan",
    max_age_seconds: float = MAX_FRAME_AGE_SECONDS,
    allowed_observation_sources: frozenset[str] | None = None,
) -> tuple[list[Observation], dict]:
    """Lees en verifieer een radarworkerartifact uit de gedeelde map."""
    raw_path = metadata.get("artifact_path")
    if not isinstance(raw_path, str):
        raise ValueError("Workerresultaat bevat geen artifactpad")
    root = WORKER_ARTIFACT_ROOT.resolve()
    path = Path(raw_path).resolve()
    if path.parent != root or path.suffixes[-2:] != [".json", ".gz"]:
        raise ValueError("Workerartifact staat buiten de toegestane map")
    try:
        expected_bytes = int(metadata.get("artifact_bytes", -1))
    except (TypeError, ValueError) as err:
        raise ValueError("Workerartifact heeft een ongeldige grootte") from err
    if (
        expected_bytes <= 0
        or expected_bytes > MAX_WORKER_ARTIFACT_BYTES
        or path.stat().st_size != expected_bytes
    ):
        raise ValueError("Workerartifact heeft een ongeldige grootte")
    with path.open("rb") as stream:
        compressed = read_limited_stream(
            stream,
            maximum_bytes=MAX_WORKER_ARTIFACT_BYTES,
            declared_bytes=expected_bytes,
            require_declared_match=True,
        )
    digest = hashlib.sha256(compressed).hexdigest()
    if digest != metadata.get("artifact_sha256"):
        raise ValueError("Workerartifact-controlesom klopt niet")
    decoded = decompress_limited(
        compressed,
        codec="gzip",
        maximum_compressed_bytes=MAX_WORKER_ARTIFACT_BYTES,
        maximum_uncompressed_bytes=MAX_WORKER_ARTIFACT_BYTES,
    )
    payload = json.loads(decoded)
    if (
        not isinstance(payload, dict)
        or payload.get("provider") != expected_provider
    ):
        raise ValueError("Workerartifact bevat het verkeerde radarresultaat")
    frame_timestamp = float(payload["frame_timestamp"])
    now = datetime.now(timezone.utc).timestamp()
    if not math.isfinite(frame_timestamp) or frame_timestamp > now + 5 * 60:
        raise ValueError(f"{expected_provider}-workerframe has invalid product time")
    if now - frame_timestamp > max_age_seconds:
        raise ValueError(f"{expected_provider}-workerframe is te oud")
    try:
        metadata_timestamp = float(metadata.get("frame_timestamp"))
    except (TypeError, ValueError) as err:
        raise ValueError("Workerartifact metadata has invalid product time") from err
    if (
        metadata.get("content_type") != "application/json+gzip"
        or not math.isfinite(metadata_timestamp)
        or abs(metadata_timestamp - frame_timestamp) > 1.0
    ):
        raise ValueError("Workerartifact metadata differs from payload")
    raw_observations = payload.get("observations")
    overlay = payload.get("overlay")
    if not isinstance(raw_observations, list) or not isinstance(overlay, dict):
        raise ValueError("Workerartifact is onvolledig")
    if len(raw_observations) > MAX_WORKER_OBSERVATIONS:
        raise ValueError("Workerartifact contains too many observations")
    overlay_geometry_points = _validate_overlay(
        overlay,
        expected_provider=expected_provider,
        expected_timestamp=frame_timestamp,
    )
    satellite_overlays = payload.get("overlays_by_satellite")
    if satellite_overlays is not None:
        if (
            expected_provider != "noaa_goes_rrqpe"
            or not isinstance(satellite_overlays, dict)
            or not satellite_overlays
            or not set(satellite_overlays).issubset({"18", "19"})
        ):
            raise ValueError("Workerartifact has invalid satellite overlays")
        satellite_times = []
        for satellite_overlay in satellite_overlays.values():
            _validate_overlay(
                satellite_overlay,
                expected_provider=expected_provider,
            )
            satellite_time = float(satellite_overlay["timestamp"])
            if (
                satellite_time > now + 5 * 60
                or now - satellite_time > max_age_seconds
            ):
                raise ValueError("Workerartifact has stale satellite overlay")
            satellite_times.append(satellite_time)
        if abs(min(satellite_times) - frame_timestamp) > 1.0:
            raise ValueError("Workerartifact has inconsistent satellite times")
    allowed_sources = allowed_observation_sources or frozenset({expected_provider})
    observations = [
        _observation_from_worker(
            value,
            str(value.get("source") or expected_provider),
        )
        for value in raw_observations
        if (
            isinstance(value, dict)
            and str(value.get("source") or expected_provider) in allowed_sources
        )
    ]
    if len(observations) != len(raw_observations):
        raise ValueError("Workerartifact bevat ongeldige observaties")
    if len(observations) != int(metadata.get("observation_count", -1)):
        raise ValueError("Aantal workerobservaties klopt niet")
    if any(
        observation.timestamp > now + MAX_WORKER_FUTURE_SECONDS
        or observation.timestamp < frame_timestamp - MAX_WORKER_PAST_SECONDS
        for observation in observations
    ):
        raise ValueError("Workerartifact contains inconsistent observation times")
    geometry_points = sum(
        len(observation.footprint_points)
        + len(observation.parent_footprint_points)
        + sum(
            len(ring)
            for polygon in observation.footprint_polygons
            for ring in polygon
        )
        + sum(
            len(ring)
            for polygon in observation.parent_footprint_polygons
            for ring in polygon
        )
        for observation in observations
    )
    if geometry_points + overlay_geometry_points > MAX_WORKER_GEOMETRY_POINTS:
        raise ValueError("Workerartifact exceeds geometry budget")
    return observations, overlay


def parse_rv_archive(
    payload: bytes,
    areas: tuple[CoverageArea, ...],
    *,
    now: float | None = None,
    overlay_out: list | None = None,
) -> list[Observation]:
    """Parseer uitsluitend het actuele (+000) RV HDF5-frame."""
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:") as archive:
        members = [
            item for item in archive.getmembers()
            if item.isfile() and item.name.endswith("_000-hd5")
        ]
        if not members:
            raise ValueError("DWD RV-archief bevat geen actueel HDF5-frame")
        member = sorted(members, key=lambda item: item.name)[-1]
        if member.size <= 0 or member.size > MAX_FRAME_BYTES:
            raise ValueError("DWD RV-frame overschrijdt productlimiet")
        stream = archive.extractfile(member)
        if stream is None:
            raise ValueError("DWD RV-frame kan niet worden gelezen")
        frame = stream.read(MAX_FRAME_BYTES + 1)
        if len(frame) != member.size or len(frame) > MAX_FRAME_BYTES:
            raise ValueError("DWD RV-frame heeft een ongeldige grootte")

    with h5py.File(io.BytesIO(frame), "r") as dataset:
        data = np.asarray(dataset["dataset1/data1/data"])
        data_what = dataset["dataset1/data1/what"].attrs
        frame_what = dataset["dataset1/what"].attrs
        where = dataset["where"].attrs
        timestamp = datetime.strptime(
            _text(frame_what["enddate"]) + _text(frame_what["endtime"]),
            "%Y%m%d%H%M%S",
        ).replace(tzinfo=timezone.utc).timestamp()
        reference_now = datetime.now(timezone.utc).timestamp() if now is None else now
        if reference_now - timestamp > MAX_FRAME_AGE_SECONDS:
            raise ValueError("DWD RV-frame is ouder dan 15 minuten")

        gain = float(data_what["gain"])
        offset = float(data_what["offset"])
        nodata = float(data_what["nodata"])
        undetect = float(data_what["undetect"])
        raw = data.astype(np.float64)
        decoded = raw * gain + offset
        rain_rate = decoded * 12.0  # ACRR is vijfminutenaccumulatie -> mm/u
        valid = (
            (raw != nodata)
            & (raw != undetect)
            & (rain_rate >= 0.1)
        )
        if not np.any(valid):
            return []
        xscale = float(where["xscale"])
        yscale = float(where["yscale"])
        ysize = int(where["ysize"])
        transformer = Transformer.from_crs(
            CRS.from_user_input(_text(where["projdef"])), "EPSG:4326",
            always_xy=True,
        )
        inverse = Transformer.from_crs(
            "EPSG:4326",
            CRS.from_user_input(_text(where["projdef"])),
            always_xy=True,
        )
        if "UL_lon" in where and "UL_lat" in where:
            ul_x, ul_y = inverse.transform(
                float(where["UL_lon"]), float(where["UL_lat"])
            )
        else:
            ul_x, ul_y = 0.0, ysize * yscale
        intensity_grid = np.zeros(data.shape, dtype=np.uint8)
        for row, column in np.argwhere(valid):
            intensity_grid[row, column] = _intensity(
                float(rain_rate[row, column])
            )

        def corner_to_latlon(row, column):
            lon, lat = transformer.transform(
                ul_x + column * xscale,
                ul_y - row * yscale,
            )
            return round(float(lat), 5), round(float(lon), 5)

        components = extract_components(intensity_grid, corner_to_latlon)
        if overlay_out is not None:
            overlay_out.append({
                "source": "dwd_radolan", "timestamp": timestamp,
                "runs": extract_intensity_runs(
                    intensity_grid, corner_to_latlon,
                    include_point=(lambda lat, lon: not areas or any(
                        area.contains(lat, lon) for area in areas
                    )),
                ),
            })

    observations = []
    frame_id = f"dwd_radolan:{timestamp:.0f}"
    pixel_area_km2 = xscale * yscale / 1_000_000.0
    for component in components:
        lon, lat = transformer.transform(
            ul_x + component.centroid_col * xscale,
            ul_y - component.centroid_row * yscale,
        )
        if areas and not any(area.contains(float(lat), float(lon)) for area in areas):
            continue
        area_km2 = len(component.pixels) * pixel_area_km2
        component_id = f"{frame_id}:c{component.index}"
        observations.append(Observation(
            obs_type=ObservationType.RADAR,
            lat=float(lat),
            lon=float(lon),
            timestamp=timestamp,
            intensity=component.max_intensity,
            area_km2=area_km2,
            quality=0.98,
            footprint_points=component.boundary,
            footprint_polygons=component.polygons,
            radar_cell_id=component_id,
            parent_system_id=component_id,
            parent_area_km2=area_km2,
            parent_footprint_points=component.boundary,
            parent_footprint_polygons=component.polygons,
            source="dwd_radolan",
        ))
    return observations


def _parse_rv_job(
    payload: bytes,
    areas: tuple[CoverageArea, ...],
) -> tuple[list[Observation], dict | None]:
    """Parseer één archief in de begrensde DWD-worker."""
    overlays = []
    observations = parse_rv_archive(
        payload, areas, overlay_out=overlays
    )
    return observations, overlays[0] if overlays else None


class DwdRadolanProvider:
    plugin_id = "dwd_radolan"
    capabilities = frozenset({Capability.RADAR})
    priority = 100

    def __init__(self, session) -> None:
        self._session = session
        self._areas: tuple[CoverageArea, ...] = ()
        self.overlay = None
        self._parse_executor: ThreadPoolExecutor | None = None
        self._parse_task: asyncio.Future | None = None
        self._hass = None
        self._last_observations: list[Observation] = []

    def supports(self, area: CoverageArea) -> CoverageResult:
        lat_margin = area.horizon_km / 111.0
        lon_margin = area.horizon_km / max(40.0, 111.0)
        supported = (
            45.5 - lat_margin <= area.center_lat <= 56.0 + lat_margin
            and 1.0 - lon_margin <= area.center_lon <= 19.0 + lon_margin
        )
        return CoverageResult(
            supported=supported,
            coverage_fraction=1.0 if supported else 0.0,
            quality=0.98 if supported else 0.0,
            reason="DWD RV 1 km Duitsland" if supported else "buiten DWD-dekking",
        )

    async def async_start(self, context) -> None:
        self._hass = context.hass
        self._areas = tuple(context.config.get("areas", (context.area,)))
        if self._parse_executor is None:
            self._parse_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="storm_tracker_dwd",
            )

    async def async_update_areas(self, areas) -> None:
        self._areas = tuple(areas)

    async def async_stop(self) -> None:
        self._areas = ()
        if self._parse_executor is not None:
            self._parse_executor.shutdown(wait=False, cancel_futures=True)
            self._parse_executor = None

    async def _await_parse_task(self) -> list[Observation]:
        """Herbruik een lopende parse, ook nadat de poll zelf time-outte."""
        task = self._parse_task
        if task is None:
            return []
        try:
            observations, overlay = await asyncio.shield(task)
        except asyncio.CancelledError:
            # wait_for annuleert alleen de wachtende poll. De dedicated
            # worker mag afwerken; een volgende cyclus neemt dit resultaat
            # over en start dus geen tweede zware parse.
            raise
        except Exception:
            if task.done() and self._parse_task is task:
                self._parse_task = None
            raise
        if self._parse_task is task:
            self._parse_task = None
        self.overlay = overlay
        self._last_observations = list(observations)
        _LOGGER.info(
            "DWD RADOLAN fallback: %d observaties binnen actieve engines",
            len(observations),
        )
        return observations

    async def _fetch_from_worker(self) -> list[Observation]:
        """Gebruik het gevalideerde workerresultaat als operationele invoer."""
        from ..worker_bridge import async_request_worker_job

        diagnostic = {
            "state": "running",
            "mode": "worker_first",
        }
        self._set_shadow_diagnostic(diagnostic)
        result = await async_request_worker_job(
            self._hass,
            "dwd_radolan_fetch",
            {
                "areas": [
                    {
                        "center_lat": area.center_lat,
                        "center_lon": area.center_lon,
                        "horizon_km": area.horizon_km,
                    }
                    for area in self._areas
                ]
            },
            timeout=35,
        )
        loop = asyncio.get_running_loop()
        observations, overlay = await loop.run_in_executor(
            self._parse_executor,
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
        diagnostic.update(
            {
                "state": "active",
                "source": "worker",
                "worker_observation_count": len(observations),
                "worker_frame_timestamp": result.get("frame_timestamp"),
                "artifact_sha256": result.get("artifact_sha256"),
            }
        )
        self._set_shadow_diagnostic(diagnostic)
        _LOGGER.info(
            "DWD RADOLAN worker: %d observaties binnen actieve engines",
            len(observations),
        )
        return observations

    def _set_shadow_diagnostic(self, diagnostic: dict) -> None:
        if self._hass is None:
            return
        from ..const import DOMAIN

        worker_status = dict(
            self._hass.data[DOMAIN].get("worker_status", {})
        )
        worker_status["dwd_radolan_shadow"] = diagnostic
        self._hass.data[DOMAIN]["worker_status"] = worker_status
        self._hass.bus.async_fire(f"{DOMAIN}_worker_status_update")

    async def async_fetch(self) -> list[Observation]:
        if self._parse_task is not None:
            return await self._await_parse_task()
        if self._parse_executor is None:
            self._parse_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="storm_tracker_dwd",
            )
        if self._hass is not None:
            try:
                return await self._fetch_from_worker()
            except (
                asyncio.TimeoutError,
                OSError,
                KeyError,
                RuntimeError,
                TypeError,
                ValueError,
                json.JSONDecodeError,
            ) as err:
                self._set_shadow_diagnostic(
                    {
                        "state": "worker_onbeschikbaar",
                        "mode": "worker_first",
                        "source": "bewaarde_workerdata",
                        "fallback_active": False,
                        "error": type(err).__name__,
                    }
                )
                _LOGGER.warning(
                    "DWD-worker niet bruikbaar; laatst geldige workerdata blijft actief: %s",
                    err,
                )
                return list(self._last_observations)
        return []
