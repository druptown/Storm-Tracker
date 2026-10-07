"""Bounded, request-scoped capture of native weather input, never credentials.

No I/O, hashing or decoding is performed by this module. Receipts are volatile
until the transactional archive writer acknowledges them after commit.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import math
import re
from threading import RLock
from types import MappingProxyType
from typing import Any, Mapping
from urllib.parse import parse_qsl, urlsplit
from uuid import uuid4


@dataclass(frozen=True, slots=True)
class RawCaptureContext:
    request_id: str
    provider_id: str
    product_id: str
    requested_at: float
    attempt: int
    region_revision_ids: tuple[str, ...]
    target_positions: tuple[tuple[str, float, float, float | None], ...]
    code_revision: str


@dataclass(frozen=True, slots=True)
class RawCapture:
    capture_id: str
    context: RawCaptureContext
    transport: str
    safe_source: Mapping[str, Any]
    received_at: float
    product_valid_at: float | None
    http_status: int | None
    payload: bytes | None = field(repr=False)
    content_type: str | None
    disposition: str
    reason_code: str | None


# Routes, not arbitrary URLs. A provider must match its own host and path.
# Query parameters are not stored: credentials and signed download URLs may
# travel there. Product identity and the safe path remain independently useful.
_ROUTES = {
    "netatmo": (("api.netatmo.com", "/api/getpublicdata"),),
    "metar": (("aviationweather.gov", "/api/data/metar"),),
    "meteolux": (("metapi.ana.lu", "/api/v1/hvd/"),),
    "kmi_aws": (("opendata.meteo.be", "/geoserver/"),),
    "kmi_appv4": (("app.meteo.be", "/services/appv4/"),),
    "kmi_radqpe": (("opendata.meteo.be", "/"),),
    "opera_radar": (("s3.waw3-1.cloudferro.com", "/openradar-24h/"),),
    "knmi_radar": (("api.dataplatform.knmi.nl", "/open-data/v1/"),),
    "dwd_radolan": (("opendata.dwd.de", "/weather/radar/"),),
    "open_meteo": (("api.open-meteo.com", "/v1/forecast"),),
    "italiameteo_icon2i": (("api.open-meteo.com", "/v1/forecast"),),
    "italiameteo_dpc_catalog": (("meteohub.agenziaitaliameteo.it", "/api/datasets/"),),
    "geosphere": (("dataset.api.hub.geosphere.at", "/v1/timeseries/"),),
    "wunderground": (("api.weather.com", "/v2/pws/observations/current"),),
    "aemet_radar": (("www.aemet.es", "/es/api-eltiempo/radar/"),),
    "meteofrance_radar": (("public-api.meteofrance.fr", "/public/DPRadar/v1/"),),
    "dpc_radar": (("radar-api.protezionecivile.it", "/findLastProductByType"),
                  ("radar-api.protezionecivile.it", "/downloadProduct")),
    "metoffice_radar": (("met-office-radar-obs-data.s3.eu-west-2.amazonaws.com", "/"),),
    "rainviewer": (("api.rainviewer.com", "/public/"),
                   ("tilecache.rainviewer.com", "/")),
    "eumetsat_li": (("api.eumetsat.int", "/data/"),),
}
_ALIASES = {"ground.netatmo.region": "netatmo"}
_SECRET_KEYS = re.compile(r'(?:access[_-]?token|refresh[_-]?token|client[_-]?secret|password|authorization|cookie)', re.I)
_AUTH_BODY = re.compile(rb'["\'](?:access_token|refresh_token|id_token|client_secret|password|authorization|cookie)["\']\s*:', re.I)
_SCOPE: ContextVar[tuple[RawCaptureContext, "RawArchiveBuffer"] | None] = ContextVar("thin_raw_capture", default=None)


def safe_source_context(provider_id: str, source: str,
                        params: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return only a validated meteorological address, never a source dump."""
    try:
        parsed = urlsplit(source)
        if parsed.username or parsed.password:
            return {"allowed": False, "reason_code": "credentials_excluded"}
        if _SECRET_KEYS.search(parsed.path) or parsed.path.rstrip("/").endswith("/token"):
            return {"allowed": False, "reason_code": "auth_excluded"}
        if any(_SECRET_KEYS.search(key) for key, _ in parse_qsl(parsed.query)):
            return {"allowed": False, "reason_code": "credentials_excluded"}
        provider = _ALIASES.get(provider_id, provider_id)
        host = parsed.hostname or ""
        routes = _ROUTES.get(provider, ())
        allowed = parsed.scheme == "https" and any(host == h and parsed.path.startswith(p) for h, p in routes)
        if provider.startswith("goes") and provider.endswith(("_rrqpe", "_glm")):
            satellite = provider[4:6]
            allowed = parsed.scheme == "https" and host == f"noaa-goes{satellite}.s3.amazonaws.com"
        if not allowed:
            return {"allowed": False, "reason_code": "source_policy_missing"}
        return {"allowed": True, "scheme": parsed.scheme, "host": host, "path": parsed.path}
    except (TypeError, ValueError):
        return {"allowed": False, "reason_code": "source_invalid"}


def weather_payload_allowed(provider_id: str, safe_source: Mapping[str, Any],
                            payload: bytes, content_type: str | None) -> bool:
    if not safe_source.get("allowed") or not isinstance(payload, bytes):
        return False
    head = payload[:4096].lstrip().lower()
    if _AUTH_BODY.search(payload) or head.startswith((b"<!doctype html", b"<html")):
        return False
    if content_type and "html" in content_type.lower():
        return False
    # Weather JSON may be malformed after its recognisable envelope. Keep it
    # for decoder investigation; do not turn an auth/error envelope into weather.
    if _ALIASES.get(provider_id, provider_id) == "netatmo":
        return head.startswith(b"{") and bool(re.search(rb'"body"\s*:', payload[:4096]))
    if provider_id == "metar":
        return head.startswith(b"[")
    return bool(payload)


class RawArchiveBuffer:
    def __init__(self, maximum_bytes: int = 512 * 1024 * 1024, *, maximum_receipts: int = 16384):
        if maximum_bytes <= 0 or maximum_receipts <= 0:
            raise ValueError("raw_archive_budget_invalid")
        self.maximum_bytes = maximum_bytes
        self.maximum_receipts = maximum_receipts
        self._lock = RLock()
        self._pending: dict[str, RawCapture] = {}
        self._refs: dict[int, tuple[int, int]] = {}
        self._bytes = 0
        self._dropped = 0
        self._excluded = 0
        self._reasons: set[str] = set()

    def mark_gap(self, reason_code: str, context: RawCaptureContext | None = None) -> None:
        with self._lock:
            self._dropped += 1
            # Caller codes only, never an exception/URL/body. Bound metadata too.
            code = reason_code if re.fullmatch(r"[a-z0-9_]{1,80}", reason_code) else "capture_failed"
            if len(self._reasons) < 64:
                self._reasons.add(code)

    def exclude(self) -> None:
        with self._lock:
            self._excluded += 1

    def enqueue(self, receipt: RawCapture) -> bool:
        with self._lock:
            payload = receipt.payload
            key = id(payload)
            size, refs = self._refs.get(key, (len(payload or b""), 0))
            if len(self._pending) >= self.maximum_receipts or self._bytes + (0 if refs else size) > self.maximum_bytes:
                self.mark_gap("raw_buffer_overflow", receipt.context)
                return False
            self._pending[receipt.capture_id] = receipt
            self._refs[key] = (size, refs + 1)
            if not refs:
                self._bytes += size
            return True

    def pending(self) -> tuple[RawCapture, ...]:
        with self._lock:
            return tuple(self._pending.values())

    def ack(self, capture_ids: tuple[str, ...]) -> None:
        with self._lock:
            for identifier in capture_ids:
                receipt = self._pending.pop(identifier, None)
                if receipt is None:
                    continue
                key = id(receipt.payload)
                size, refs = self._refs[key]
                if refs == 1:
                    self._bytes -= size
                    del self._refs[key]
                else:
                    self._refs[key] = (size, refs - 1)

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {"status": "degraded" if self._reasons else "current",
                    "queued_bytes": self._bytes, "queued_receipts": len(self._pending),
                    "dropped_count": self._dropped, "excluded_count": self._excluded,
                    "reason_codes": tuple(sorted(self._reasons))}


@contextmanager
def capture_scope(context: RawCaptureContext, archive: RawArchiveBuffer):
    token = _SCOPE.set((context, archive))
    try:
        yield
    finally:
        _SCOPE.reset(token)


def capture_received(source: str, payload: bytes, *, received_at: float,
                     content_type: str | None = None, http_status: int | None = None,
                     transport: str = "http") -> str | None:
    scope = _SCOPE.get()
    if scope is None:
        return None
    context, archive = scope
    safe = safe_source_context(context.provider_id, source)
    if safe.get("reason_code") in {"auth_excluded", "credentials_excluded"} or _AUTH_BODY.search(payload):
        archive.exclude()
        return None
    if not safe.get("allowed"):
        archive.mark_gap(str(safe["reason_code"]), context)
        return None
    if not math.isfinite(received_at) or not math.isfinite(context.requested_at):
        archive.mark_gap("capture_time_invalid", context)
        return None
    if not weather_payload_allowed(context.provider_id, safe, payload, content_type):
        archive.mark_gap("weather_envelope_rejected", context)
        return None
    identifier = str(uuid4())
    receipt = RawCapture(identifier, context, transport, MappingProxyType(safe), received_at, None,
                         http_status, payload, content_type, "received", None)
    return identifier if archive.enqueue(receipt) else None
