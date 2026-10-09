"""Exact, fail-closed capture of one account's authorized AWN devices.

Ambient Weather requires both an application key and a user/device API key.
Those credentials are read from the worker environment, used only to build the
HTTPS request and never written to the raw store, receipts, diagnostics or
return value.  The exact JSON response is made durable before its bounded
structural inspection.

This module deliberately does *not* turn fields such as ``hourlyrainin`` or
``dailyrainin`` into an instantaneous rain observation.  Their temporal and
sensor semantics require a separate certified decoder, physical-site catalogue
and ground-fusion admission.  A successful capture therefore still has weather
state ``unknown``.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Callable
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)


AWN_ORIGIN = "https://api.ambientweather.net"
AWN_DEVICES_PATH = "/v1/devices"
AWN_DEVICES_URL = f"{AWN_ORIGIN}{AWN_DEVICES_PATH}"
APPLICATION_KEY_ENV = "STV3_AWN_APPLICATION_KEY"
API_KEY_ENV = "STV3_AWN_API_KEY"

MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_DEVICES = 512
MAX_JSON_NODES = 131_072
MAX_JSON_DEPTH = 16
MAX_SECRET_CHARS = 512
HTTP_TIMEOUT_SECONDS = 15.0
CAPTURE_DEADLINE_SECONDS = 45.0

AWN_RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
AWN_RAW_ARTIFACT_ROOT = AWN_RAW_SHARE_ROOT / "raw" / "awn_authorized_devices"
AWN_RESPONSE_PRODUCT = "awn_authorized_devices_native_json"
AWN_CAPTURE_PRODUCT = "awn_authorized_devices_capture"
AWN_COLLECTOR = "ambientweather_authorized_devices_exact_raw"
AWN_COLLECTOR_VERSION = "worker-awn-authorized-devices-exact-raw-v1"
AWN_STRUCTURE_INSPECTOR_VERSION = "awn-authorized-devices-structure-v1"
AWN_SEMANTIC_CLASS = "authorized_personal_weather_station_snapshot"
AWN_ADMISSION_STATUS = "exact_raw_shadow_ground_semantics_not_certified"
AWN_PRIVACY_CLASS = "authorized_private_account_data"

_JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,479}$")
_RAW_STORE: RawArtifactStore | None = None
_RAW_STORE_LOCK = threading.Lock()
_CAPTURE_LOCK = threading.Lock()


class AwnNativeError(RuntimeError):
    """Base error; never a dry or safe-weather result."""


class AwnNativeContractError(AwnNativeError):
    """The remote or local contract is invalid."""


class AwnNativeCapacityError(AwnNativeError):
    """A bounded byte, device or JSON limit was exceeded."""


class AwnNativeSecurityError(AwnNativeError):
    """Credential-shaped data would cross a durable boundary."""


class AwnNativeCorruptionError(AwnNativeError):
    """A previously durable receipt or capture no longer verifies."""


@dataclass(frozen=True, slots=True)
class AwnHttpResponse:
    payload: bytes
    content_type: str
    received_at: float


@dataclass(frozen=True, slots=True)
class AwnDeviceInventory:
    device_count: int
    coordinate_device_count: int
    timestamp_device_count: int
    rain_field_device_count: int
    latest_data_device_count: int
    structure_sha256: str


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


def _open_without_redirects(request: Request, *, timeout: float):
    return build_opener(ProxyHandler({}), _RejectRedirects()).open(
        request, timeout=timeout
    )


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0:
        raise TimeoutError("AWN capturedeadline is verstreken")
    return remaining


def _credential(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise AwnNativeSecurityError(f"AWN {name} ontbreekt")
    result = value.strip()
    if (
        len(result) < 8
        or len(result) > MAX_SECRET_CHARS
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in result)
    ):
        raise AwnNativeSecurityError(f"AWN {name} is ongeldig")
    return result


def _credentials() -> tuple[str, str]:
    return (
        _credential(os.getenv(APPLICATION_KEY_ENV, ""), "application key"),
        _credential(os.getenv(API_KEY_ENV, ""), "API key"),
    )


def _validate_job(job: object) -> str:
    job_id = getattr(job, "job_id", None)
    if not isinstance(job_id, str) or _JOB_ID_RE.fullmatch(job_id) is None:
        raise AwnNativeContractError("AWN capturejob mist een geldige job_id")
    payload = getattr(job, "payload", None)
    if type(payload) is not dict or payload:
        raise AwnNativeSecurityError(
            "AWN capture vereist een exact lege payload; credentials horen in env"
        )
    return job_id


def _raw_store(injected: RawArtifactStore | None = None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    share = AWN_RAW_SHARE_ROOT.resolve(strict=False)
    root = AWN_RAW_ARTIFACT_ROOT.resolve(strict=False)
    if root == share or share not in root.parents:
        raise AwnNativeContractError("AWN raw-artifactpad ontsnapt uit worker-share")
    global _RAW_STORE
    with _RAW_STORE_LOCK:
        if _RAW_STORE is None:
            candidate = RawArtifactStore(root)
            candidate.initialize()
            _RAW_STORE = candidate
        return _RAW_STORE


def _declared_length(headers: object, *, maximum: int) -> int | None:
    getter = getattr(headers, "get", None)
    raw = getter("Content-Length") if callable(getter) else None
    if raw is None:
        return None
    value = str(raw).strip()
    if not value.isascii() or not value.isdecimal():
        raise AwnNativeContractError("AWN Content-Length is ongeldig")
    parsed = int(value)
    if not 0 < parsed <= maximum:
        raise AwnNativeCapacityError("AWN Content-Length overschrijdt de grens")
    return parsed


def _bounded_header(headers: object, name: str, maximum: int = 256) -> str:
    getter = getattr(headers, "get", None)
    raw = getter(name) if callable(getter) else None
    value = "" if raw is None else str(raw).strip()
    if len(value) > maximum or any(ord(character) < 0x20 for character in value):
        raise AwnNativeContractError(f"AWN {name}-header is ongeldig")
    return value


def _read_body(
    response: object,
    *,
    maximum: int,
    declared: int | None,
    deadline: float,
) -> bytes:
    reader = getattr(response, "read", None)
    if not callable(reader):
        raise AwnNativeContractError("AWN respons heeft geen bytestream")
    chunks: list[bytes] = []
    total = 0
    while total <= maximum:
        _remaining(deadline)
        requested = min(64 * 1024, maximum + 1 - total)
        chunk = reader(requested)
        _remaining(deadline)
        if not chunk:
            break
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise AwnNativeContractError("AWN responsstream leverde geen bytes")
        immutable = bytes(chunk)
        if len(immutable) > requested:
            raise AwnNativeCapacityError("AWN responschunk overschrijdt de grens")
        chunks.append(immutable)
        total += len(immutable)
        if total > maximum or (declared is not None and total > declared):
            raise AwnNativeCapacityError("AWN respons overschrijdt de bytegrens")
    payload = b"".join(chunks)
    if declared is not None and len(payload) != declared:
        raise AwnNativeContractError("AWN responslengte wijkt af van HTTP")
    return payload


def _reject_credential_echo(payload: bytes, secrets: tuple[str, str]) -> None:
    """Reject literal and JSON-decoded credential echoes before persistence.

    Parsing at this boundary is solely a privacy/security preflight.  The
    durable-before-decoder rule still applies to all weather/ground semantics.
    An invalid JSON response cannot be proven free of escaped credentials and
    is therefore discarded instead of archived.
    """
    if any(secret.encode("utf-8") in payload for secret in secrets):
        raise AwnNativeSecurityError(
            "AWN respons echoot een credential en wordt niet opgeslagen"
        )
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AwnNativeContractError(
            "AWN respons faalt de credential-veilige JSON preflight"
        ) from exc
    nodes = 0
    stack: list[object] = [parsed]
    while stack:
        current = stack.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES:
            raise AwnNativeCapacityError("AWN JSON overschrijdt de nodegrens")
        if isinstance(current, dict):
            stack.extend(current.keys())
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
        elif isinstance(current, str) and any(
            secret in current for secret in secrets
        ):
            raise AwnNativeSecurityError(
                "AWN respons echoot een credential en wordt niet opgeslagen"
            )


def _request_devices(
    *,
    application_key: str,
    api_key: str,
    deadline: float,
    opener: Callable[..., Any] | None = None,
) -> AwnHttpResponse:
    query = urlencode({"applicationKey": application_key, "apiKey": api_key})
    url = f"{AWN_DEVICES_URL}?{query}"
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/awn-native-v1",
        },
    )
    active = _open_without_redirects if opener is None else opener
    with active(
        request, timeout=min(HTTP_TIMEOUT_SECONDS, _remaining(deadline))
    ) as response:
        if getattr(response, "status", None) != 200:
            raise AwnNativeContractError("AWN endpoint gaf geen HTTP 200")
        geturl = getattr(response, "geturl", None)
        response_url = str(geturl()) if callable(geturl) else url
        parsed = urlsplit(response_url)
        query_values = parse_qs(parsed.query, keep_blank_values=True)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "api.ambientweather.net"
            or parsed.port not in (None, 443)
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path != AWN_DEVICES_PATH
            or parsed.fragment
            or query_values
            != {"applicationKey": [application_key], "apiKey": [api_key]}
        ):
            raise AwnNativeSecurityError("AWN response kwam van een ander endpoint")
        headers = getattr(response, "headers", {})
        encoding = (_bounded_header(headers, "Content-Encoding", 64) or "identity").lower()
        if encoding != "identity":
            raise AwnNativeContractError("AWN transportcompressie is verboden")
        content_type = _bounded_header(headers, "Content-Type").split(";", 1)[0].lower()
        if content_type not in {"application/json", "text/json"}:
            raise AwnNativeContractError("AWN Content-Type valt buiten contract")
        declared = _declared_length(headers, maximum=MAX_RESPONSE_BYTES)
        payload = _read_body(
            response,
            maximum=MAX_RESPONSE_BYTES,
            declared=declared,
            deadline=deadline,
        )
    _reject_credential_echo(payload, (application_key, api_key))
    return AwnHttpResponse(
        payload=payload,
        content_type=content_type,
        received_at=time.time(),
    )


def _bounded_json_shape(value: object) -> None:
    nodes = 0
    stack: list[tuple[object, int]] = [(value, 1)]
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES:
            raise AwnNativeCapacityError("AWN JSON overschrijdt de nodegrens")
        if depth > MAX_JSON_DEPTH:
            raise AwnNativeCapacityError("AWN JSON overschrijdt de dieptegrens")
        if isinstance(current, dict):
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in current)


def _inspect_devices(payload: bytes) -> AwnDeviceInventory:
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AwnNativeContractError("AWN respons is geen geldige UTF-8 JSON") from exc
    _bounded_json_shape(parsed)
    if not isinstance(parsed, list):
        raise AwnNativeContractError("AWN devices-respons is geen lijst")
    if len(parsed) > MAX_DEVICES:
        raise AwnNativeCapacityError("AWN respons bevat te veel devices")
    if any(not isinstance(item, dict) for item in parsed):
        raise AwnNativeContractError("AWN devices-respons bevat een niet-object")

    coordinate_count = 0
    timestamp_count = 0
    rain_count = 0
    latest_count = 0
    structural_rows: list[dict[str, object]] = []
    rain_fields = {
        "hourlyrainin",
        "eventrainin",
        "dailyrainin",
        "weeklyrainin",
        "monthlyrainin",
        "yearlyrainin",
        "rainratein",
    }
    for device in parsed:
        coords = device.get("coords")
        has_coordinates = isinstance(coords, dict) and all(
            isinstance(coords.get(name), (int, float))
            and not isinstance(coords.get(name), bool)
            and math.isfinite(float(coords[name]))
            for name in ("lat", "lon")
        )
        latest = device.get("lastData")
        latest = latest if isinstance(latest, dict) else None
        has_timestamp = False
        has_rain_fields = False
        if latest is not None:
            latest_count += 1
            timestamp = latest.get("dateutc")
            has_timestamp = (
                isinstance(timestamp, (int, float))
                and not isinstance(timestamp, bool)
                and math.isfinite(float(timestamp))
                and float(timestamp) >= 0
            )
            has_rain_fields = any(field in latest for field in rain_fields)
        coordinate_count += int(has_coordinates)
        timestamp_count += int(has_timestamp)
        rain_count += int(has_rain_fields)
        structural_rows.append({
            "has_coordinates": has_coordinates,
            "has_latest_data": latest is not None,
            "has_rain_fields": has_rain_fields,
            "has_timestamp": has_timestamp,
        })
    structure_bytes = json.dumps(
        structural_rows,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return AwnDeviceInventory(
        device_count=len(parsed),
        coordinate_device_count=coordinate_count,
        timestamp_device_count=timestamp_count,
        rain_field_device_count=rain_count,
        latest_data_device_count=latest_count,
        structure_sha256=hashlib.sha256(structure_bytes).hexdigest(),
    )


def _provenance() -> dict[str, object]:
    return {
        "authenticated_authorized_devices_only": True,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": AWN_COLLECTOR,
        "collector_version": AWN_COLLECTOR_VERSION,
        "credential_query_persisted": False,
        "endpoint_origin": AWN_ORIGIN,
        "endpoint_path": AWN_DEVICES_PATH,
        "instantaneous_rain_semantics_certified": False,
        "parser_version": AWN_STRUCTURE_INSPECTOR_VERSION,
        "privacy_class": AWN_PRIVACY_CLASS,
        "representation": NATIVE_REPRESENTATION,
        "semantic_class": AWN_SEMANTIC_CLASS,
    }


def _receipt_id(store: RawArtifactStore, job_id: str) -> str:
    return store._receipt_id(  # noqa: SLF001 - internal store identity contract
        provider="ambientweather",
        product=AWN_RESPONSE_PRODUCT,
        fetch_id=f"{job_id}:response_json",
    )


def _capture_id(store: RawArtifactStore, job_id: str) -> str:
    return store._capture_id(  # noqa: SLF001 - internal store identity contract
        provider="ambientweather",
        product=AWN_CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
    )


def _validate_receipt(receipt: RawArtifactReceipt, *, job_id: str) -> None:
    expected_upstream = (
        f"api.ambientweather.net{AWN_DEVICES_PATH}"
        f"|content_sha256={receipt.content_sha256}"
    )
    if (
        receipt.provider != "ambientweather"
        or receipt.product != AWN_RESPONSE_PRODUCT
        or receipt.fetch_id != f"{job_id}:response_json"
        or receipt.upstream_id != expected_upstream
        or receipt.content_type not in {"application/json", "text/json"}
        or not 0 <= receipt.size_bytes <= MAX_RESPONSE_BYTES
        or receipt.provenance != _provenance()
    ):
        raise AwnNativeCorruptionError("AWN raw receipt wijkt af van contract")


def _upstream_capture_id(receipt: RawArtifactReceipt) -> str:
    return f"{AWN_CAPTURE_PRODUCT}|content_sha256={receipt.content_sha256}"


def _validate_capture(
    capture: RawArtifactCaptureReceipt,
    *,
    job_id: str,
    receipt: RawArtifactReceipt,
) -> None:
    members = {member.role: member.receipt_id for member in capture.members}
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.provider != "ambientweather"
        or capture.product != AWN_CAPTURE_PRODUCT
        or capture.fetch_id != f"{job_id}:capture"
        or capture.upstream_capture_id != _upstream_capture_id(receipt)
        or capture.product_timestamp is not None
        or members != {"response_json": receipt.receipt_id}
    ):
        raise AwnNativeCorruptionError("AWN capture wijkt af van contract")


def _result(
    *,
    capture: RawArtifactCaptureReceipt,
    receipt: RawArtifactReceipt,
    inventory: AwnDeviceInventory,
) -> dict[str, object]:
    return {
        "admission_status": AWN_ADMISSION_STATUS,
        "capture_state": "captured_structure_inspected_not_decoded",
        "decode_status": "ground_semantics_not_decoded",
        "instantaneous_rain_claim": None,
        "method": "awn_authorized_devices_native_capture_v1",
        "operational_admission_eligible": False,
        "public_radius_search_supported": False,
        "source_scope": "authorized_account_devices_only",
        "weather_state": "unknown",
        "inventory": {
            "coordinate_device_count": inventory.coordinate_device_count,
            "device_count": inventory.device_count,
            "latest_data_device_count": inventory.latest_data_device_count,
            "rain_field_device_count": inventory.rain_field_device_count,
            "structure_sha256": inventory.structure_sha256,
            "timestamp_device_count": inventory.timestamp_device_count,
        },
        "raw_provenance": {
            "credential_query_persisted": False,
            "privacy_class": AWN_PRIVACY_CLASS,
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "raw_member_count": 1,
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "sources": {
                "response_json": {
                    "content_sha256": receipt.content_sha256,
                    "receipt_id": receipt.receipt_id,
                    "receipt_sha256": receipt.receipt_sha256,
                    "received_at": receipt.received_at,
                    "role": "response_json",
                    "size_bytes": receipt.size_bytes,
                }
            },
            "upstream_capture_id": capture.upstream_capture_id,
        },
    }


def _resume(
    store: RawArtifactStore,
    *,
    job_id: str,
    application_key: str,
    api_key: str,
) -> dict[str, object] | None:
    bundle = store.load_capture_bundle_bytes(_capture_id(store, job_id))
    if bundle is not None:
        capture, receipts, payloads = bundle
        if set(receipts) != {"response_json"} or set(payloads) != {"response_json"}:
            raise AwnNativeCorruptionError("AWN capture mist response_json")
        receipt = receipts["response_json"]
        payload = payloads["response_json"]
    else:
        capture = None
        loaded = store.load_receipt_bytes(_receipt_id(store, job_id))
        if loaded is None:
            return None
        receipt, payload = loaded
    _validate_receipt(receipt, job_id=job_id)
    _reject_credential_echo(payload, (application_key, api_key))
    inventory = _inspect_devices(payload)
    if capture is None:
        capture = store.commit_capture(
            provider="ambientweather",
            product=AWN_CAPTURE_PRODUCT,
            fetch_id=f"{job_id}:capture",
            upstream_capture_id=_upstream_capture_id(receipt),
            product_timestamp=None,
            members={"response_json": receipt.receipt_id},
        )
    _validate_capture(capture, job_id=job_id, receipt=receipt)
    return _result(capture=capture, receipt=receipt, inventory=inventory)


def capture_authorized_awn_devices(
    job: object,
    *,
    raw_store: RawArtifactStore | None = None,
    deadline_at: float | None = None,
    opener: Callable[..., Any] | None = None,
    _fault_injector: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Capture one exact authorized-device response without a rain claim."""
    job_id = _validate_job(job)
    application_key, api_key = _credentials()
    now = time.monotonic()
    if deadline_at is None:
        deadline = now + CAPTURE_DEADLINE_SECONDS
    elif (
        isinstance(deadline_at, bool)
        or not isinstance(deadline_at, (int, float))
        or not math.isfinite(float(deadline_at))
    ):
        raise AwnNativeContractError("AWN deadline_at is ongeldig")
    else:
        deadline = min(float(deadline_at), now + CAPTURE_DEADLINE_SECONDS)
    _remaining(deadline)
    store = _raw_store(raw_store)
    with _CAPTURE_LOCK:
        _remaining(deadline)
        resumed = _resume(
            store,
            job_id=job_id,
            application_key=application_key,
            api_key=api_key,
        )
        if resumed is not None:
            return resumed
        response = _request_devices(
            application_key=application_key,
            api_key=api_key,
            deadline=deadline,
            opener=opener,
        )
        digest = hashlib.sha256(response.payload).hexdigest()
        receipt = store.store(
            provider="ambientweather",
            product=AWN_RESPONSE_PRODUCT,
            fetch_id=f"{job_id}:response_json",
            content_type=response.content_type,
            received_at=response.received_at,
            upstream_id=(
                f"api.ambientweather.net{AWN_DEVICES_PATH}"
                f"|content_sha256={digest}"
            ),
            provenance=_provenance(),
            payload=response.payload,
        )
        if _fault_injector is not None:
            _fault_injector("awn.after_response_store")
        # Exact bytes are durable before JSON/device inspection.
        inventory = _inspect_devices(response.payload)
        _remaining(deadline)
        if _fault_injector is not None:
            _fault_injector("awn.before_capture_commit")
        capture = store.commit_capture(
            provider="ambientweather",
            product=AWN_CAPTURE_PRODUCT,
            fetch_id=f"{job_id}:capture",
            upstream_capture_id=_upstream_capture_id(receipt),
            product_timestamp=None,
            members={"response_json": receipt.receipt_id},
        )
        if _fault_injector is not None:
            _fault_injector("awn.after_capture_commit")
        _validate_capture(capture, job_id=job_id, receipt=receipt)
        return _result(capture=capture, receipt=receipt, inventory=inventory)


__all__ = [
    "API_KEY_ENV",
    "APPLICATION_KEY_ENV",
    "AWN_ADMISSION_STATUS",
    "AWN_CAPTURE_PRODUCT",
    "AWN_RESPONSE_PRODUCT",
    "AwnDeviceInventory",
    "AwnNativeCapacityError",
    "AwnNativeContractError",
    "AwnNativeCorruptionError",
    "AwnNativeError",
    "AwnNativeSecurityError",
    "capture_authorized_awn_devices",
]
