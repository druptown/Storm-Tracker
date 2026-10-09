"""Exact raw-only DWD RADOLAN/RADVOR RV archive capture.

This module intentionally does not import :mod:`storm_tracker_worker.radar`.
That production module continues from the exact tar response into a legacy
HDF5/component decoder.  The acquisition shadow runtime needs a narrower
authority: retain the upstream tar bytes, inspect only the bounded tar member
layout needed to bind a product timestamp, and emit technical provenance.
No HDF5 member is opened and no precipitation value is interpreted here.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import io
from pathlib import Path
import re
import tarfile
import threading
import time
from typing import Any
from urllib.request import Request, urlopen

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)
from .tracking_core.engine.bounded_payload import read_limited_stream


RV_LATEST_URL = (
    "https://opendata.dwd.de/weather/radar/composite/rv/"
    "composite_rv_LATEST.tar"
)
MAX_ARCHIVE_BYTES = 8 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 64
MAX_DECLARED_MEMBER_BYTES = 40 * 1024 * 1024
HTTP_IO_TIMEOUT_SECONDS = 20.0
JOB_TOTAL_DEADLINE_SECONDS = 45.0

DWD_RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
DWD_RAW_ARTIFACT_ROOT = DWD_RAW_SHARE_ROOT / "raw" / "dwd_rv_exact"
DWD_ARCHIVE_PRODUCT = "dwd_open_data_composite_rv_tar"
DWD_CAPTURE_PRODUCT = "dwd_open_data_composite_rv_raw_capture"
DWD_COLLECTOR = "dwd_open_data_composite_rv"
DWD_COLLECTOR_VERSION = "worker-dwd-rv-tar-exact-raw-v2"
DWD_TAR_INSPECTOR_VERSION = "dwd-rv-tar-layout-only-v1"

_PRODUCTION_MEMBER_RE = re.compile(
    r"^composite_rv_(?P<date>\d{8})_(?P<time>\d{4})_000-hd5$"
)
_ALLOWED_CONTENT_TYPES = frozenset({
    "application/octet-stream",
    "application/x-tar",
    "binary/octet-stream",
})
_CAPTURE_LOCK = threading.Lock()
_DEFAULT_STORE: RawArtifactStore | None = None
_DEFAULT_STORE_LOCK = threading.Lock()


@dataclass(frozen=True, slots=True)
class DwdRvHttpResponse:
    payload: bytes
    content_type: str
    received_at: float


@dataclass(frozen=True, slots=True)
class DwdRvTarIdentity:
    member_name: str
    member_size: int
    product_timestamp: float


def _job_id(job: Any) -> str:
    value = getattr(job, "job_id", None)
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 480
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ValueError("DWD raw workerjob mist een geldige job_id")
    payload = getattr(job, "payload", None)
    if type(payload) is not dict or payload:
        raise ValueError("DWD raw workerjobpayload moet exact leeg zijn")
    return value


def _raw_store(injected: RawArtifactStore | None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    share_root = DWD_RAW_SHARE_ROOT.resolve(strict=False)
    raw_root = DWD_RAW_ARTIFACT_ROOT.resolve(strict=False)
    if raw_root == share_root or share_root not in raw_root.parents:
        raise RuntimeError("DWD raw-artifactpad ontsnapt uit worker-share")
    global _DEFAULT_STORE
    with _DEFAULT_STORE_LOCK:
        if _DEFAULT_STORE is None:
            candidate = RawArtifactStore(raw_root)
            candidate.initialize()
            _DEFAULT_STORE = candidate
        return _DEFAULT_STORE


def _download() -> DwdRvHttpResponse:
    request = Request(
        RV_LATEST_URL,
        headers={
            "Accept": "application/x-tar,application/octet-stream",
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/1.16",
        },
    )
    with urlopen(request, timeout=HTTP_IO_TIMEOUT_SECONDS) as response:
        headers = getattr(response, "headers", None)
        encoding = str(headers.get("Content-Encoding") or "identity") if headers else "identity"
        if encoding.lower() != "identity":
            raise ValueError("onverwachte DWD-transportcompressie")
        declared = headers.get("Content-Length") if headers else None
        content_type = str(headers.get("Content-Type") or "") if headers else ""
        content_type = content_type.split(";", 1)[0].strip().lower()
        if not content_type:
            content_type = "application/x-tar"
        if content_type not in _ALLOWED_CONTENT_TYPES:
            raise ValueError("DWD antwoord heeft een onverwacht Content-Type")
        payload = read_limited_stream(
            response,
            maximum_bytes=MAX_ARCHIVE_BYTES,
            declared_bytes=declared,
            require_declared_match=declared is not None,
        )
    if not payload:
        raise ValueError("DWD antwoord is leeg")
    return DwdRvHttpResponse(payload, content_type, time.time())


def _inspect_tar_layout(payload: bytes) -> DwdRvTarIdentity:
    """Inspect tar headers only; never open or decode the HDF5 member."""

    if not isinstance(payload, bytes) or not 1 <= len(payload) <= MAX_ARCHIVE_BYTES:
        raise ValueError("DWD tar ligt buiten de exacte transportgrens")
    candidates: list[tarfile.TarInfo] = []
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:") as archive:
        members = archive.getmembers()
        if not 1 <= len(members) <= MAX_ARCHIVE_MEMBERS:
            raise ValueError("DWD tar heeft een ongeldige ledenomvang")
        for member in members:
            name = str(member.name)
            if (
                not name
                or len(name) > 256
                or "/" in name
                or "\\" in name
                or not member.isfile()
                or member.size <= 0
                or member.size > MAX_DECLARED_MEMBER_BYTES
            ):
                raise ValueError("DWD tar bevat een onveilig of onbegrensd lid")
            if _PRODUCTION_MEMBER_RE.fullmatch(name) is not None:
                candidates.append(member)
    if len(candidates) != 1:
        raise ValueError("DWD tar bindt niet exact een productie +000-lid")
    member = candidates[0]
    match = _PRODUCTION_MEMBER_RE.fullmatch(member.name)
    if match is None:  # pragma: no cover - established above
        raise RuntimeError("DWD tar-inspector verloor zijn memberbinding")
    timestamp = datetime.strptime(
        match.group("date") + match.group("time"), "%Y%m%d%H%M"
    ).replace(tzinfo=timezone.utc).timestamp()
    return DwdRvTarIdentity(member.name, int(member.size), timestamp)


def _native_provenance() -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": DWD_COLLECTOR,
        "collector_version": DWD_COLLECTOR_VERSION,
        "structure_inspector_version": DWD_TAR_INSPECTOR_VERSION,
        "provenance_scope": "exact_upstream_tar_bytes_only",
    }


def _receipt_id(store: RawArtifactStore, job_id: str) -> str:
    return store._receipt_id(  # noqa: SLF001 - deterministic raw-store contract
        provider="dwd", product=DWD_ARCHIVE_PRODUCT, fetch_id=f"{job_id}:rv_tar"
    )


def _capture_id(store: RawArtifactStore, job_id: str) -> str:
    return store._capture_id(  # noqa: SLF001 - deterministic raw-store contract
        provider="dwd", product=DWD_CAPTURE_PRODUCT, fetch_id=f"{job_id}:capture"
    )


def _source(receipt: RawArtifactReceipt, identity: DwdRvTarIdentity) -> dict[str, object]:
    return {
        "role": "rv_tar",
        "raw_receipt_id": receipt.receipt_id,
        "raw_content_sha256": receipt.content_sha256,
        "raw_receipt_sha256": receipt.receipt_sha256,
        "received_at": receipt.received_at,
        "content_type": receipt.content_type,
        "archive_member_name": identity.member_name,
        "archive_member_size": identity.member_size,
        "declared_product_timestamp": identity.product_timestamp,
        "collector_version": DWD_COLLECTOR_VERSION,
        "structure_inspector_version": DWD_TAR_INSPECTOR_VERSION,
    }


def _result(
    capture: RawArtifactCaptureReceipt,
    receipt: RawArtifactReceipt,
    identity: DwdRvTarIdentity,
) -> dict[str, object]:
    return {
        "provider": "dwd",
        "product": DWD_ARCHIVE_PRODUCT,
        "capture_state": "captured_not_decoded",
        "decode_status": "not_decoded",
        "weather_state": "unknown",
        "quantitative_semantics_eligible": False,
        "operational_admission_eligible": False,
        "raw_provenance": {
            "raw_store_schema_version": capture.raw_store_schema_version,
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "raw_capture_fetch_id": capture.fetch_id,
            "product_timestamp": identity.product_timestamp,
            "collector_version": DWD_COLLECTOR_VERSION,
            "structure_inspector_versions": {
                "rv_tar": DWD_TAR_INSPECTOR_VERSION,
            },
            "provenance_grade": "exact_raw",
            "provenance_scope": "exact_upstream_tar_bytes_only",
            "raw_sources": {"rv_tar": _source(receipt, identity)},
        },
    }


def capture_latest_dwd_rv_tar(
    job: Any,
    *,
    raw_store: RawArtifactStore | None = None,
) -> dict[str, object]:
    """Capture a latest RV tar idempotently without importing a decoder."""

    job_id = _job_id(job)
    store = _raw_store(raw_store)
    with _CAPTURE_LOCK:
        complete = store.load_capture_bundle_bytes(_capture_id(store, job_id))
        if complete is not None:
            capture, receipts, payloads = complete
            if set(receipts) != {"rv_tar"} or set(payloads) != {"rv_tar"}:
                raise RuntimeError("DWD raw capture mist zijn exacte tarrol")
            receipt = receipts["rv_tar"]
            payload = payloads["rv_tar"]
            identity = _inspect_tar_layout(payload)
            return _result(capture, receipt, identity)

        loaded = store.load_receipt_bytes(_receipt_id(store, job_id))
        if loaded is None:
            response = _download()
            content_sha256 = hashlib.sha256(response.payload).hexdigest()
            receipt = store.store(
                provider="dwd",
                product=DWD_ARCHIVE_PRODUCT,
                fetch_id=f"{job_id}:rv_tar",
                content_type=response.content_type,
                received_at=response.received_at,
                upstream_id=(
                    f"{DWD_ARCHIVE_PRODUCT}|latest"
                    f"|content_sha256={content_sha256}"
                ),
                provenance=_native_provenance(),
                payload=response.payload,
            )
            payload = response.payload
        else:
            receipt, payload = loaded
        # Exact tar bytes are durable before the first tar-layout operation.
        identity = _inspect_tar_layout(payload)
        capture = store.commit_capture(
            provider="dwd",
            product=DWD_CAPTURE_PRODUCT,
            fetch_id=f"{job_id}:capture",
            upstream_capture_id=(
                f"{DWD_CAPTURE_PRODUCT}|member={identity.member_name}"
                f"|content_sha256={receipt.content_sha256}"
            ),
            product_timestamp=identity.product_timestamp,
            members={"rv_tar": receipt.receipt_id},
        )
        return _result(capture, receipt, identity)


DWD_RV_CANDIDATE_REQUIRED_ROLES = ("rv_tar",)

__all__ = [
    "DWD_RV_CANDIDATE_REQUIRED_ROLES",
    "JOB_TOTAL_DEADLINE_SECONDS",
    "MAX_ARCHIVE_BYTES",
    "capture_latest_dwd_rv_tar",
]
