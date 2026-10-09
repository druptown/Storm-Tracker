"""EUMETSAT MTG Lightning Imager fallback-provider.

Haalt uitsluitend de NetCDF BODY-entry van het nieuwste tienminutenproduct op.
De provider wordt door de runtime alleen gepolld wanneer Blitzortung offline is.
"""
from __future__ import annotations

from datetime import datetime, timezone
import io
import json
import logging
import time
from typing import Any

import aiohttp
import h5py

from ..engine.bounded_payload import iter_limited_async
from ..engine.observation import Observation, ObservationType

_LOGGER = logging.getLogger(__name__)

COLLECTION_ID = "EO:EUM:DAT:0691"
TOKEN_URL = "https://api.eumetsat.int/token"
SEARCH_URL = (
    "https://api.eumetsat.int/data/search-products/1.0.0/os"
    "?format=json&pi=EO%3AEUM%3ADAT%3A0691&si=0&c=4"
)
MAX_BODY_BYTES = 10 * 1024 * 1024
MAX_TOKEN_RESPONSE_BYTES = 512 * 1024
MAX_SEARCH_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_PRODUCT_AGE_S = 30 * 60
EPOCH_2000 = datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp()
_HTTP_CHUNK_BYTES = 64 * 1024


async def _read_bounded_response(response, *, maximum_bytes: int) -> bytes:
    headers = response.headers
    encoding = headers.get("Content-Encoding")
    if encoding and encoding.lower() != "identity":
        raise ValueError(
            "EUMETSAT-response gebruikt onverwachte transportcompressie"
        )
    return await iter_limited_async(
        response.content.iter_chunked(_HTTP_CHUNK_BYTES),
        maximum_bytes=maximum_bytes,
        declared_bytes=headers.get("Content-Length"),
        require_declared_match=headers.get("Content-Length") is not None,
    )


def _dataset_by_name(root: h5py.Group, name: str):
    """Zoek een dataset op basename, onafhankelijk van de MTG-groepslayout."""
    found = []

    def visitor(path: str, obj: Any) -> None:
        if isinstance(obj, h5py.Dataset) and path.rsplit("/", 1)[-1] == name:
            found.append(obj)

    root.visititems(visitor)
    return found[0] if found else None


def _decoded_values(dataset) -> list[float | None]:
    """Decodeer CF scale/offset en negeer fillwaarden."""
    raw_values = dataset[...].reshape(-1)
    scale = float(dataset.attrs.get("scale_factor", 1.0))
    offset = float(dataset.attrs.get("add_offset", 0.0))
    fill = dataset.attrs.get("_FillValue")
    if hasattr(fill, "item"):
        fill = fill.item()
    values: list[float | None] = []
    for raw in raw_values:
        scalar = raw.item() if hasattr(raw, "item") else raw
        if fill is not None and scalar == fill:
            values.append(None)
        else:
            values.append(float(scalar) * scale + offset)
    return values


def parse_lightning_flashes(payload: bytes) -> list[Observation]:
    """Zet één LI-2-LFL NetCDF BODY-bestand om naar LIGHTNING-observaties."""
    with h5py.File(io.BytesIO(payload), "r") as nc:
        lat_ds = _dataset_by_name(nc, "latitude")
        lon_ds = _dataset_by_name(nc, "longitude")
        time_ds = _dataset_by_name(nc, "flash_time")
        if lat_ds is None or lon_ds is None or time_ds is None:
            raise ValueError("EUMETSAT LI NetCDF mist latitude/longitude/flash_time")
        latitudes = _decoded_values(lat_ds)
        longitudes = _decoded_values(lon_ds)
        flash_times = _decoded_values(time_ds)

    if not (len(latitudes) == len(longitudes) == len(flash_times)):
        raise ValueError("EUMETSAT LI arrays hebben ongelijke lengtes")

    observations = []
    for lat, lon, flash_time in zip(latitudes, longitudes, flash_times):
        if lat is None or lon is None or flash_time is None:
            continue
        # Sommige productversies coderen westerlengtes als 0..360.
        if lon > 180:
            lon -= 360
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        observations.append(Observation(
            obs_type=ObservationType.LIGHTNING,
            lat=lat,
            lon=lon,
            timestamp=EPOCH_2000 + flash_time,
            source="eumetsat_li",
        ))
    return observations


class EumetsatLightningProvider:
    """Kleine async client voor de gratis MTG LI Lightning Flashes-collectie."""

    def __init__(
        self,
        session,
        consumer_key: str,
        consumer_secret: str,
        *,
        hass=None,
    ) -> None:
        self._session = session
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self._hass = hass
        self._token: str | None = None
        self._token_expires_at = 0.0
        self._last_product_id: str | None = None

    async def _access_token(self) -> str:
        if self._token and time.monotonic() < self._token_expires_at - 60:
            return self._token
        auth = aiohttp.BasicAuth(self._consumer_key, self._consumer_secret)
        async with self._session.post(
            TOKEN_URL,
            auth=auth,
            data={"grant_type": "client_credentials", "validity_period": "3600"},
            headers={"Accept-Encoding": "identity"},
        ) as response:
            response.raise_for_status()
            raw = await _read_bounded_response(
                response,
                maximum_bytes=MAX_TOKEN_RESPONSE_BYTES,
            )
            data = json.loads(raw.decode("utf-8"))
        token = data.get("access_token")
        if not token:
            raise ValueError("EUMETSAT authenticatie gaf geen access_token")
        self._token = str(token)
        self._token_expires_at = time.monotonic() + float(data.get("expires_in", 3300))
        return self._token

    async def fetch_observations(self) -> list[Observation]:
        if self._hass is not None:
            try:
                from ..worker_bridge import async_request_worker_job

                result = await async_request_worker_job(
                    self._hass,
                    "eumetsat_li_fetch",
                    {
                        "credential_ref": "worker_environment",
                        "last_product_id": self._last_product_id,
                    },
                    timeout=55,
                )
                raw = result.get("observations")
                if not isinstance(raw, list) or len(raw) > 4000:
                    raise ValueError("ongeldig EUMETSAT LI-workerresultaat")
                observations = [
                    Observation(
                        obs_type=ObservationType.LIGHTNING,
                        lat=float(item["lat"]),
                        lon=float(item["lon"]),
                        timestamp=float(item["timestamp"]),
                        source="eumetsat_li",
                    )
                    for item in raw
                    if isinstance(item, dict)
                    and item.get("obs_type") == "lightning"
                    and item.get("source") == "eumetsat_li"
                ]
                if len(observations) != len(raw):
                    raise ValueError("onvolledige EUMETSAT LI-workerobservaties")
                product_id = result.get("product_id")
                if product_id:
                    self._last_product_id = str(product_id)
                return observations
            except Exception as err:
                _LOGGER.warning(
                    "EUMETSAT LI-workerfout; lokale verwerking blijft uit: %s",
                    err,
                )
                return []
        async with self._session.get(
            SEARCH_URL,
            headers={"Accept-Encoding": "identity"},
        ) as response:
            response.raise_for_status()
            raw = await _read_bounded_response(
                response,
                maximum_bytes=MAX_SEARCH_RESPONSE_BYTES,
            )
            catalog = json.loads(raw.decode("utf-8"))
        features = catalog.get("features") or []
        if not features:
            return []
        token = await self._access_token()
        for feature in features:
            product_id = str(feature.get("id") or "")
            if not product_id or product_id == self._last_product_id:
                continue
            period = str(feature.get("properties", {}).get("date", "")).split("/")
            if len(period) == 2:
                sensing_end = datetime.fromisoformat(period[1].replace("Z", "+00:00"))
                if time.time() - sensing_end.timestamp() > MAX_PRODUCT_AGE_S:
                    continue

            links = feature.get("properties", {}).get("links", {})
            entries = links.get("sip-entries") or []
            body = next(
                (
                    entry for entry in entries
                    if "CHK-BODY" in str(entry.get("title", ""))
                    and str(entry.get("title", "")).endswith(".nc")
                ),
                None,
            )
            if not body or not body.get("href"):
                continue

            try:
                async with self._session.get(
                    body["href"],
                    headers={
                        "Accept-Encoding": "identity",
                        "Authorization": f"Bearer {token}",
                    },
                ) as response:
                    response.raise_for_status()
                    payload = await _read_bounded_response(
                        response,
                        maximum_bytes=MAX_BODY_BYTES,
                    )
            except aiohttp.ClientResponseError as err:
                if err.status != 404:
                    raise
                _LOGGER.info(
                    "EUMETSAT LI product %s nog niet downloadbaar; probeer ouder product",
                    product_id,
                )
                continue

            observations = parse_lightning_flashes(payload)
            self._last_product_id = product_id
            _LOGGER.info(
                "EUMETSAT LI fallback: %d flashes uit product %s",
                len(observations), product_id,
            )
            return observations

        _LOGGER.warning("EUMETSAT LI: geen recent downloadbaar product gevonden")
        return []
