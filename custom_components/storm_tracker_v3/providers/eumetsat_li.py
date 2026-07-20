"""EUMETSAT MTG Lightning Imager fallback-provider.

Haalt uitsluitend de NetCDF BODY-entry van het nieuwste tienminutenproduct op.
De provider wordt door de runtime alleen gepolld wanneer Blitzortung offline is.
"""
from __future__ import annotations

from datetime import datetime, timezone
import io
import logging
import time
from typing import Any

import aiohttp
import h5py

from ..engine.observation import Observation, ObservationType

_LOGGER = logging.getLogger(__name__)

COLLECTION_ID = "EO:EUM:DAT:0691"
TOKEN_URL = "https://api.eumetsat.int/token"
SEARCH_URL = (
    "https://api.eumetsat.int/data/search-products/1.0.0/os"
    "?format=json&pi=EO%3AEUM%3ADAT%3A0691&si=0&c=4"
)
MAX_BODY_BYTES = 10 * 1024 * 1024
MAX_PRODUCT_AGE_S = 30 * 60
EPOCH_2000 = datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp()


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

    def __init__(self, session, consumer_key: str, consumer_secret: str) -> None:
        self._session = session
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
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
        ) as response:
            response.raise_for_status()
            data = await response.json(content_type=None)
        token = data.get("access_token")
        if not token:
            raise ValueError("EUMETSAT authenticatie gaf geen access_token")
        self._token = str(token)
        self._token_expires_at = time.monotonic() + float(data.get("expires_in", 3300))
        return self._token

    async def fetch_observations(self) -> list[Observation]:
        async with self._session.get(SEARCH_URL) as response:
            response.raise_for_status()
            catalog = await response.json(content_type=None)
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

            chunks = []
            total = 0
            try:
                async with self._session.get(
                    body["href"], headers={"Authorization": f"Bearer {token}"}
                ) as response:
                    response.raise_for_status()
                    declared = response.content_length
                    if declared is not None and declared > MAX_BODY_BYTES:
                        raise ValueError(f"EUMETSAT BODY te groot: {declared} bytes")
                    async for chunk in response.content.iter_chunked(64 * 1024):
                        total += len(chunk)
                        if total > MAX_BODY_BYTES:
                            raise ValueError("EUMETSAT BODY overschrijdt veiligheidslimiet")
                        chunks.append(chunk)
            except aiohttp.ClientResponseError as err:
                if err.status != 404:
                    raise
                _LOGGER.info(
                    "EUMETSAT LI product %s nog niet downloadbaar; probeer ouder product",
                    product_id,
                )
                continue

            observations = parse_lightning_flashes(b"".join(chunks))
            self._last_product_id = product_id
            _LOGGER.info(
                "EUMETSAT LI fallback: %d flashes uit product %s",
                len(observations), product_id,
            )
            return observations

        _LOGGER.warning("EUMETSAT LI: geen recent downloadbaar product gevonden")
        return []
