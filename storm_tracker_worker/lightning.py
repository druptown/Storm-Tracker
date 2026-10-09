"""Worker-owned NOAA GOES GLM lightning retrieval and NetCDF decoding."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import base64
import io
import json
import os
import time
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

import h5py
import numpy as np

from .provider_radar import _download_with_headers

SATELLITES = (18, 19)
PREFIX = "GLM-L2-LCFA"
MAX_FILES = 12
MAX_FLASHES = 4000
EUMETSAT_TOKEN_URL = "https://api.eumetsat.int/token"
EUMETSAT_SEARCH_URL = (
    "https://api.eumetsat.int/data/search-products/1.0.0/os"
    "?format=json&pi=EO%3AEUM%3ADAT%3A0691&si=0&c=4"
)
EUMETSAT_MAX_BODY = 10 * 1024 * 1024
EUMETSAT_MAX_SEARCH = 4 * 1024 * 1024
EUMETSAT_MAX_TOKEN = 512 * 1024
EUMETSAT_MAX_AGE = 30 * 60
EUMETSAT_EPOCH_2000 = datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp()


def _scalar(value, default=0.0):
    values = np.asarray(value if value is not None else default).reshape(-1)
    return float(values[0]) if values.size else float(default)


def _text(value):
    return bytes(value).decode("ascii") if isinstance(value, (bytes, np.bytes_)) else str(value)


def _decode(dataset):
    raw = np.asarray(dataset[...]).reshape(-1)
    scale = _scalar(dataset.attrs.get("scale_factor"), 1.0)
    offset = _scalar(dataset.attrs.get("add_offset"), 0.0)
    unsigned = _text(dataset.attrs.get("_Unsigned", "false")).lower() == "true"
    bits = dataset.dtype.itemsize * 8
    result = []
    for value in raw:
        item = value.item() if hasattr(value, "item") else value
        if unsigned and dataset.dtype.kind == "i" and item < 0:
            item += 1 << bits
        result.append(float(item) * scale + offset)
    return result


def parse_goes_flashes(payload: bytes, satellite: int) -> list[dict]:
    with h5py.File(io.BytesIO(payload), "r") as product:
        required = ("flash_lat", "flash_lon", "flash_time_offset_of_first_event")
        if any(name not in product for name in required):
            raise ValueError("GOES GLM mist vereiste flashvelden")
        latitudes = _decode(product["flash_lat"])
        longitudes = _decode(product["flash_lon"])
        time_data = product["flash_time_offset_of_first_event"]
        units = _text(time_data.attrs.get("units", ""))
        marker = "seconds since "
        if not units.startswith(marker):
            raise ValueError("onbekende GOES-flashtijdeenheid")
        epoch = datetime.fromisoformat(
            units[len(marker):].strip().replace("Z", "+00:00")
        )
        if epoch.tzinfo is None:
            epoch = epoch.replace(tzinfo=timezone.utc)
        timestamps = [epoch.timestamp() + value for value in _decode(time_data)]
    if not (len(latitudes) == len(longitudes) == len(timestamps)):
        raise ValueError("GOES-flasharrays hebben ongelijke lengte")
    source = f"noaa_goes{satellite}_glm"
    return [
        {"obs_type": "lightning", "lat": lat, "lon": lon,
         "timestamp": timestamp, "source": source}
        for lat, lon, timestamp in zip(latitudes, longitudes, timestamps)
        if -90 <= lat <= 90 and -180 <= lon <= 180
    ]


def _recent_keys(satellite: int) -> list[str]:
    now = datetime.now(timezone.utc)
    keys = set()
    bucket = f"https://noaa-goes{satellite}.s3.amazonaws.com/"
    for moment in (now - timedelta(hours=1), now):
        prefix = f"{PREFIX}/{moment.year}/{moment.timetuple().tm_yday:03d}/{moment.hour:02d}/"
        query = urlencode({"list-type": "2", "prefix": prefix, "max-keys": "1000"})
        root = ET.fromstring(_download_with_headers(
            f"{bucket}?{query}", maximum_bytes=4 * 1024 * 1024
        ))
        keys.update(node.text for node in root.findall(".//{*}Key") if node.text)
    return sorted(keys)[-MAX_FILES:]


def goes_glm_fetch(job) -> dict:
    selected = {
        int(value) for value in (job.payload.get("satellites") or ())
        if int(value) in SATELLITES
    }
    processed = set(str(value) for value in (job.payload.get("processed_keys") or ()))
    observations = []
    new_keys = []
    status = {str(satellite): "standby" for satellite in SATELLITES}
    cutoff = datetime.now(timezone.utc).timestamp() - 5 * 60
    for satellite in sorted(selected):
        keys = _recent_keys(satellite)
        status[str(satellite)] = "active" if keys else "unavailable"
        for key in (key for key in keys if key not in processed):
            body = _download_with_headers(
                f"https://noaa-goes{satellite}.s3.amazonaws.com/{quote(key, safe='/')}",
                maximum_bytes=5 * 1024 * 1024,
            )
            observations.extend(
                item for item in parse_goes_flashes(body, satellite)
                if item["timestamp"] >= cutoff
            )
            new_keys.append(key)
            if len(observations) > MAX_FLASHES:
                observations = observations[-MAX_FLASHES:]
    return {
        "observations": observations,
        "processed_keys": (list(processed)[-188:] + new_keys)[-200:],
        "status": status,
    }


def _bounded_request(
    url: str,
    *,
    maximum_bytes: int,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
) -> bytes:
    if not url.startswith("https://"):
        raise ValueError("EUMETSAT accepteert uitsluitend HTTPS")
    request = Request(
        url,
        data=body,
        method="POST" if body is not None else "GET",
        headers={
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/1.16",
            **(headers or {}),
        },
    )
    with urlopen(request, timeout=45) as response:
        if str(response.headers.get("Content-Encoding") or "identity").lower() != "identity":
            raise ValueError("EUMETSAT-response gebruikt transportcompressie")
        declared = response.headers.get("Content-Length")
        if declared is not None and int(declared) > maximum_bytes:
            raise ValueError("EUMETSAT-response overschrijdt bytecontract")
        value = response.read(maximum_bytes + 1)
        if len(value) > maximum_bytes:
            raise ValueError("EUMETSAT-response overschrijdt bytecontract")
        if declared is not None and len(value) != int(declared):
            raise ValueError("EUMETSAT-response is onvolledig")
        from .thin_raw_archive import capture_received
        capture_received(str(response.geturl()), value, received_at=time.time(),
                         content_type=response.headers.get('Content-Type'), http_status=getattr(response,'status',None))
        return value


def _dataset_by_name(root: h5py.Group, name: str):
    found = []

    def visitor(path, value):
        if isinstance(value, h5py.Dataset) and path.rsplit("/", 1)[-1] == name:
            found.append(value)

    root.visititems(visitor)
    return found[0] if found else None


def _decoded_values(dataset) -> list[float | None]:
    raw = np.asarray(dataset[...]).reshape(-1)
    scale = _scalar(dataset.attrs.get("scale_factor"), 1.0)
    offset = _scalar(dataset.attrs.get("add_offset"), 0.0)
    fill = dataset.attrs.get("_FillValue")
    fill = fill.item() if hasattr(fill, "item") else fill
    return [
        None if fill is not None and (value.item() if hasattr(value, "item") else value) == fill
        else float(value.item() if hasattr(value, "item") else value) * scale + offset
        for value in raw
    ]


def parse_eumetsat_flashes(payload: bytes) -> list[dict]:
    if len(payload) > EUMETSAT_MAX_BODY:
        raise ValueError("EUMETSAT LI-product overschrijdt bytecontract")
    with h5py.File(io.BytesIO(payload), "r") as product:
        latitude = _dataset_by_name(product, "latitude")
        longitude = _dataset_by_name(product, "longitude")
        flash_time = _dataset_by_name(product, "flash_time")
        if latitude is None or longitude is None or flash_time is None:
            raise ValueError("EUMETSAT LI mist vereiste flashvelden")
        latitudes = _decoded_values(latitude)
        longitudes = _decoded_values(longitude)
        timestamps = _decoded_values(flash_time)
    if not (len(latitudes) == len(longitudes) == len(timestamps)):
        raise ValueError("EUMETSAT LI arrays hebben ongelijke lengte")
    result = []
    for lat, lon, offset in zip(latitudes, longitudes, timestamps):
        if lat is None or lon is None or offset is None:
            continue
        longitude_value = lon - 360.0 if lon > 180.0 else lon
        if -90 <= lat <= 90 and -180 <= longitude_value <= 180:
            result.append({
                "obs_type": "lightning",
                "lat": lat,
                "lon": longitude_value,
                "timestamp": EUMETSAT_EPOCH_2000 + offset,
                "source": "eumetsat_li",
            })
    return result[-MAX_FLASHES:]


def eumetsat_li_fetch(job) -> dict:
    if job.payload.get("consumer_key") or job.payload.get("consumer_secret"):
        raise ValueError("EUMETSAT-secret mag niet in workerjobpayload staan")
    consumer_key = os.getenv("STV3_EUMETSAT_CONSUMER_KEY", "").strip()
    consumer_secret = os.getenv("STV3_EUMETSAT_CONSUMER_SECRET", "").strip()
    previous_product_id = str(job.payload.get("last_product_id") or "")
    if not consumer_key or not consumer_secret:
        raise ValueError("EUMETSAT-referenties ontbreken")
    catalog = json.loads(_bounded_request(
        EUMETSAT_SEARCH_URL, maximum_bytes=EUMETSAT_MAX_SEARCH
    ).decode("utf-8"))
    credentials = base64.b64encode(
        f"{consumer_key}:{consumer_secret}".encode("utf-8")
    ).decode("ascii")
    token_payload = json.loads(_bounded_request(
        EUMETSAT_TOKEN_URL,
        maximum_bytes=EUMETSAT_MAX_TOKEN,
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        body=b"grant_type=client_credentials&validity_period=3600",
    ).decode("utf-8"))
    token = str(token_payload.get("access_token") or "")
    if not token:
        raise ValueError("EUMETSAT authenticatie gaf geen access_token")
    for feature in catalog.get("features") or ():
        product_id = str(feature.get("id") or "")
        if not product_id or product_id == previous_product_id:
            continue
        period = str(feature.get("properties", {}).get("date", "")).split("/")
        if len(period) == 2:
            sensing_end = datetime.fromisoformat(period[1].replace("Z", "+00:00"))
            if time.time() - sensing_end.timestamp() > EUMETSAT_MAX_AGE:
                continue
        entries = feature.get("properties", {}).get("links", {}).get("sip-entries") or ()
        body = next((
            entry for entry in entries
            if "CHK-BODY" in str(entry.get("title", ""))
            and str(entry.get("title", "")).endswith(".nc")
            and str(entry.get("href", "")).startswith("https://")
        ), None)
        if body is None:
            continue
        try:
            payload = _bounded_request(
                str(body["href"]),
                maximum_bytes=EUMETSAT_MAX_BODY,
                headers={"Authorization": f"Bearer {token}"},
            )
        except Exception as err:
            # A just-published catalogue entry can briefly return 404.  Keep
            # trying the older bounded entries; propagate all other failures.
            if "404" in str(err):
                continue
            raise
        observations = parse_eumetsat_flashes(payload)
        cutoff = time.time() - EUMETSAT_MAX_AGE
        return {
            "observations": [
                value for value in observations if value["timestamp"] >= cutoff
            ],
            "product_id": product_id,
        }
    return {"observations": [], "product_id": previous_product_id or None}
