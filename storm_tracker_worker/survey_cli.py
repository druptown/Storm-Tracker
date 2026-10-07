"""Manual isolated coverage-survey entry point; not used by the service."""
from __future__ import annotations

import json
import os
import sys
import urllib.request

from .tracking_core.engine.bounded_payload import read_limited_stream


MAX_WORLD_BANK_RESPONSE_BYTES = 8 * 1024 * 1024

def _inherit_worker_environment() -> None:
    """Read the service process environment inside the same container."""
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            command = open(
                f"/proc/{name}/cmdline", "rb"
            ).read().replace(b"\0", b" ")
            if b"storm_tracker_worker.service" not in command:
                continue
            values = open(
                f"/proc/{name}/environ", "rb"
            ).read().split(b"\0")
        except OSError:
            continue
        for raw in values:
            key, separator, value = raw.partition(b"=")
            if separator and key.startswith(b"STV3_"):
                os.environ.setdefault(
                    key.decode("utf-8"), value.decode("utf-8")
                )
        return


def _all_capitals() -> dict:
    """Load every country/territory with a capital from the World Bank."""
    url = (
        "https://api.worldbank.org/v2/country"
        "?format=json&per_page=400"
    )
    with urllib.request.urlopen(url, timeout=30) as response:
        raw = read_limited_stream(
            response,
            maximum_bytes=MAX_WORLD_BANK_RESPONSE_BYTES,
            declared_bytes=response.headers.get("Content-Length"),
            require_declared_match=True,
        )
    payload = json.loads(raw.decode("utf-8"))
    countries = {}
    for item in payload[1]:
        code = str(item.get("iso2Code") or "").upper()
        capital = str(item.get("capitalCity") or "").strip()
        region = str((item.get("region") or {}).get("id") or "")
        try:
            latitude = float(item["latitude"])
            longitude = float(item["longitude"])
        except (KeyError, TypeError, ValueError):
            continue
        if len(code) != 2 or not capital or region == "NA":
            continue
        countries[code] = (capital, latitude, longitude)
    return countries


def main() -> None:
    _inherit_worker_environment()
    from . import ground_coverage

    prefixes = tuple(value.upper() for value in sys.argv[1:])
    if "ALL" in prefixes:
        ground_coverage.CAPITALS = _all_capitals()
    elif prefixes:
        countries = _all_capitals()
        ground_coverage.CAPITALS = {
            code: value
            for code, value in countries.items()
            if code.startswith(prefixes)
        }
    print(json.dumps(
        ground_coverage.survey({}),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
