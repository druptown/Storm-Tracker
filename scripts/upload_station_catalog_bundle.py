"""Build and upload a credential-complete importer bundle to Synology.

This helper is designed to run inside the NAS-Home Assistant SSH app. It reads
the already-authorized DSM config entry and the Storm Tracker app options
locally, keeps all secrets in memory (apart from the intentionally private
``.env`` inside the deliverable), and uploads the result to ``/transfer``.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import ssl
import tempfile
from typing import Any, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import uuid
import zipfile


NAS_HOST = "192.168.0.21"
ADDON_SLUG = "local_storm_tracker_worker"
ENV_ARCHIVE_PATH = (
    "storm-tracker-netatmo-importer/station_catalog_importer/.env"
)
REQUIRED_OPTIONS = (
    "postgis_host",
    "postgis_port",
    "postgis_database",
    "postgis_user",
    "postgis_password",
    "postgis_sslmode",
    "netatmo_client_id",
    "netatmo_client_secret",
    "netatmo_refresh_token",
)


def select_synology_entry(
    document: Mapping[str, Any], host: str = NAS_HOST
) -> Mapping[str, Any]:
    entries = document.get("data", {}).get("entries", [])
    matches = [
        entry.get("data", {})
        for entry in entries
        if entry.get("domain") == "synology_dsm"
        and str(entry.get("data", {}).get("host")) == host
    ]
    if len(matches) != 1:
        raise RuntimeError("synology_config_entry_not_unique")
    selected = matches[0]
    for field in ("username", "password", "device_token", "port"):
        if not selected.get(field):
            raise RuntimeError("synology_config_entry_incomplete")
    return selected


def build_importer_environment(options: Mapping[str, Any]) -> str:
    if any(options.get(key) in (None, "") for key in REQUIRED_OPTIONS):
        raise ValueError("importer_options_incomplete")
    values = {
        "STV3_POSTGIS_ENABLED": "true",
        "STV3_POSTGIS_HOST": options["postgis_host"],
        "STV3_POSTGIS_PORT": options["postgis_port"],
        "STV3_POSTGIS_DATABASE": options["postgis_database"],
        "STV3_POSTGIS_USER": options["postgis_user"],
        "STV3_POSTGIS_PASSWORD": options["postgis_password"],
        "STV3_POSTGIS_SSLMODE": options["postgis_sslmode"],
        "STV3_NETATMO_CLIENT_ID": options["netatmo_client_id"],
        "STV3_NETATMO_CLIENT_SECRET": options["netatmo_client_secret"],
        "STV3_NETATMO_REFRESH_TOKEN": options["netatmo_refresh_token"],
        "STV3_NETATMO_IMPORT_MINIMUM_CALL_INTERVAL_SECONDS": "10",
        "STV3_NETATMO_IMPORT_BATCH_CALLS": "6",
        "STV3_NETATMO_IMPORT_FAILURE_RETRY_SECONDS": "300",
    }
    return "".join(
        f"{key}={shlex.quote(str(value))}\n" for key, value in values.items()
    )


def inject_environment(source: Path, target: Path, environment: str) -> None:
    with zipfile.ZipFile(source, "r") as original, zipfile.ZipFile(
        target, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as complete:
        for item in original.infolist():
            if item.filename != ENV_ARCHIVE_PATH:
                complete.writestr(item, original.read(item.filename))
        private_env = zipfile.ZipInfo(ENV_ARCHIVE_PATH)
        private_env.compress_type = zipfile.ZIP_DEFLATED
        private_env.external_attr = 0o100600 << 16
        complete.writestr(private_env, environment.encode("utf-8"))
    os.chmod(target, 0o600)


def _json_request(
    url: str,
    *,
    data: bytes | None = None,
    headers: Mapping[str, str] | None = None,
    context: ssl.SSLContext | None = None,
) -> Mapping[str, Any]:
    request = Request(url, data=data, headers=dict(headers or {}))
    with urlopen(request, timeout=30, context=context) as response:
        result = json.loads(response.read())
    if not _response_succeeded(result):
        code = result.get("error", {}).get("code", "unknown")
        raise RuntimeError(f"remote_api_request_failed_{code}")
    return result


def _response_succeeded(result: Mapping[str, Any]) -> bool:
    """Accept both DSM's boolean and Supervisor's textual success envelope."""
    if "success" in result:
        return result.get("success") is True
    return result.get("result") == "ok"


def _addon_options() -> Mapping[str, Any]:
    token_path = Path("/var/run/s6/container_environment/HASSIO_TOKEN")
    token = token_path.read_text(encoding="utf-8").strip()
    result = _json_request(
        f"http://supervisor/addons/{ADDON_SLUG}/info",
        headers={"Authorization": f"Bearer {token}"},
    )
    options = result.get("data", {}).get("options")
    if not isinstance(options, dict):
        raise RuntimeError("addon_options_unavailable")
    return options


def _dsm_login(config: Mapping[str, Any]) -> tuple[str, str, str, ssl.SSLContext | None]:
    use_ssl = bool(config.get("ssl", True))
    scheme = "https" if use_ssl else "http"
    base_url = f"{scheme}://{config['host']}:{int(config['port'])}"
    context = ssl._create_unverified_context() if use_ssl and not config.get(
        "verify_ssl", False
    ) else None
    parameters = {
        "api": "SYNO.API.Auth",
        "version": "7",
        "method": "login",
        "account": config["username"],
        "passwd": config["password"],
        "session": "FileStation",
        "format": "sid",
        "enable_device_token": "yes",
        "device_name": "home-assistant",
        "device_id": config["device_token"],
    }
    result = _json_request(
        f"{base_url}/webapi/entry.cgi?{urlencode(parameters)}", context=context
    )
    data = result["data"]
    return base_url, str(data["sid"]), str(data.get("synotoken", "")), context


def _multipart_upload(
    *,
    base_url: str,
    sid: str,
    synotoken: str,
    context: ssl.SSLContext | None,
    source: Path,
    destination: str,
) -> None:
    boundary = f"----stormtracker{uuid.uuid4().hex}"
    chunks: list[bytes] = []
    for name, value in (
        ("path", "/transfers"),
        ("create_parents", "true"),
        ("overwrite", "true"),
    ):
        chunks.append(
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\""
            f"\r\n\r\n{value}\r\n".encode()
        )
    chunks.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
        f"filename=\"{destination}\"\r\nContent-Type: application/zip\r\n\r\n".encode()
    )
    chunks.append(source.read_bytes())
    chunks.append(f"\r\n--{boundary}--\r\n".encode())
    query = urlencode({
        "api": "SYNO.FileStation.Upload",
        "version": "2",
        "method": "upload",
        "_sid": sid,
        "SynoToken": synotoken,
    })
    _json_request(
        f"{base_url}/webapi/entry.cgi?{query}",
        data=b"".join(chunks),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        context=context,
    )


def _dsm_logout(
    base_url: str,
    sid: str,
    synotoken: str,
    context: ssl.SSLContext | None,
) -> None:
    query = urlencode({
        "api": "SYNO.API.Auth",
        "version": "7",
        "method": "logout",
        "session": "FileStation",
        "_sid": sid,
        "SynoToken": synotoken,
    })
    _json_request(f"{base_url}/webapi/entry.cgi?{query}", context=context)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument(
        "--core-config",
        type=Path,
        default=Path("/homeassistant/.storage/core.config_entries"),
    )
    parser.add_argument(
        "--destination", default="storm-tracker-netatmo-importer.zip"
    )
    args = parser.parse_args()
    config = select_synology_entry(
        json.loads(args.core_config.read_text(encoding="utf-8"))
    )
    environment = build_importer_environment(_addon_options())
    with tempfile.TemporaryDirectory(prefix="stv3-netatmo-") as temporary:
        complete = Path(temporary) / args.destination
        inject_environment(args.source, complete, environment)
        base_url, sid, synotoken, context = _dsm_login(config)
        try:
            _multipart_upload(
                base_url=base_url,
                sid=sid,
                synotoken=synotoken,
                context=context,
                source=complete,
                destination=args.destination,
            )
        finally:
            _dsm_logout(base_url, sid, synotoken, context)
    print(json.dumps({
        "destination": f"/transfers/{args.destination}",
        "secrets_embedded": True,
        "status": "uploaded",
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
