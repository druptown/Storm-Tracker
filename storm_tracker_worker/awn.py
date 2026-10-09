"""Safe AWN credential and device-visibility probe.

The probe intentionally returns only capability metadata. Credentials are read
from the add-on environment and never included in results, URLs in exceptions,
MQTT payloads, or logs.
"""
from __future__ import annotations

import json
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .tracking_core.engine.bounded_payload import read_limited_stream

AWN_DEVICES_URL = "https://api.ambientweather.net/v1/devices"
AWN_TIMEOUT_SECONDS = 15
AWN_MAX_RESPONSE_BYTES = 8 * 1024 * 1024


def _latest_data(device: dict) -> dict:
    latest = device.get("lastData")
    return latest if isinstance(latest, dict) else {}


def _device_capabilities(device: dict) -> dict:
    latest = _latest_data(device)
    location = device.get("info")
    location = location if isinstance(location, dict) else {}
    coordinates = device.get("coords")
    coordinates = coordinates if isinstance(coordinates, dict) else {}
    return {
        "has_coordinates": all(
            coordinates.get(field) is not None for field in ("lat", "lon")
        ),
        "has_live_rain": any(
            latest.get(field) is not None
            for field in ("hourlyrainin", "eventrainin", "dailyrainin")
        ),
        "has_timestamp": latest.get("dateutc") is not None,
        "name_available": bool(location.get("name") or location.get("location")),
    }


def probe_devices(
    *,
    application_key: str | None = None,
    api_key: str | None = None,
    opener=urlopen,
) -> dict:
    """List only the devices authorized by the supplied AWN user key."""
    application_key = application_key or os.getenv("STV3_AWN_APPLICATION_KEY", "")
    api_key = api_key or os.getenv("STV3_AWN_API_KEY", "")
    if not application_key or not api_key:
        return {"status": "credentials_missing", "device_count": 0, "devices": []}
    query = urlencode({"applicationKey": application_key, "apiKey": api_key})
    request = Request(
        f"{AWN_DEVICES_URL}?{query}",
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker",
        },
    )
    try:
        with opener(request, timeout=AWN_TIMEOUT_SECONDS) as response:
            headers = getattr(response, "headers", None)
            declared = (
                headers.get("Content-Length") if headers is not None else None
            )
            raw = read_limited_stream(
                response,
                maximum_bytes=AWN_MAX_RESPONSE_BYTES,
                declared_bytes=declared,
                require_declared_match=declared is not None,
            )
            payload = json.loads(raw.decode("utf-8"))
    except HTTPError as err:
        return {
            "status": "http_error",
            "http_status": int(err.code),
            "device_count": 0,
            "devices": [],
        }
    except (
        URLError,
        TimeoutError,
        ValueError,
        json.JSONDecodeError,
        UnicodeDecodeError,
    ):
        return {"status": "transport_error", "device_count": 0, "devices": []}
    if not isinstance(payload, list):
        return {"status": "invalid_response", "device_count": 0, "devices": []}
    devices = [_device_capabilities(item) for item in payload if isinstance(item, dict)]
    return {
        "status": "ok",
        "device_count": len(devices),
        "devices": devices,
        "supports_public_radius_search": False,
    }


def main() -> None:
    print(json.dumps(probe_devices(), separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
