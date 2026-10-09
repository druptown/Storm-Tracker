"""Closed DWD RV raw-capture-v2 to quantitative-decoder handoff.

The acquisition runtime deliberately captures DWD's complete ``LATEST`` tar
under the provider identity ``dwd`` without decoding any meteorological value.
The older quantitative decoder consumes a different, canonical
``dwd_radolan`` schema-3 capture.  This module is the only authority allowed to
bridge those identities.

The bridge is append-only and network-free.  It re-verifies the source capture
and every tar member, copies the exact tar bytes into a caller-owned handoff
store, and publishes a deterministic target receipt/capture.  The +000 member
is explicitly the sole observation candidate.  Forecast members +005..+120
remain individually hash-bound inside the exact tar and are never decoded by
this module or promoted into the observation path.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import io
import json
import math
import re
import tarfile
from typing import Any, Callable, Mapping

from . import dwd_rv_native as native
from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCaptureReceipt,
    RawArtifactIntegrityError,
    RawArtifactReceipt,
    RawArtifactStore,
)


HANDOFF_SCHEMA = "stv3-worker-dwd-rv-capture-handoff-v1"
HANDOFF_ADAPTER_ID = "dwd.rv.raw-capture-v2-to-observation-capture"
HANDOFF_ADAPTER_VERSION = "v1"
HANDOFF_TARGET_COLLECTOR_VERSION = "worker-dwd-rv-observation-handoff-v1"
HANDOFF_FORECAST_POLICY = "retained_exact_raw_separate_not_decoded"
HANDOFF_OBSERVATION_POLICY = "base_plus000_only"

SOURCE_PROVIDER = "dwd"
SOURCE_ARCHIVE_PRODUCT = native.DWD_ARCHIVE_PRODUCT
SOURCE_CAPTURE_PRODUCT = native.DWD_CAPTURE_PRODUCT
SOURCE_COLLECTOR = native.DWD_COLLECTOR
SOURCE_COLLECTOR_VERSION = native.DWD_COLLECTOR_VERSION
SOURCE_TAR_INSPECTOR_VERSION = native.DWD_TAR_INSPECTOR_VERSION

TARGET_PROVIDER = "dwd_radolan"
TARGET_ARCHIVE_PRODUCT = "dwd_open_data_composite_rv_tar"
TARGET_CAPTURE_PRODUCT = "dwd_open_data_composite_rv_capture"
TARGET_COLLECTOR = "dwd_open_data_composite_rv"

MAX_ARCHIVE_BYTES = native.MAX_ARCHIVE_BYTES
MAX_ARCHIVE_MEMBERS = native.MAX_ARCHIVE_MEMBERS
MAX_HDF_BYTES = native.MAX_DECLARED_MEMBER_BYTES
EXPECTED_FORECAST_LEADS = tuple(range(5, 121, 5))
EXPECTED_ALL_LEADS = (0, *EXPECTED_FORECAST_LEADS)

_ARCHIVE_MEMBER_RE = re.compile(
    r"^composite_rv_(?P<date>\d{8})_(?P<time>\d{4})_"
    r"(?P<lead>\d{3})-hd5$"
)
_SOURCE_CAPTURE_ID_RE = re.compile(r"^capture-[0-9a-f]{64}$")
_SOURCE_RECEIPT_ID_RE = re.compile(r"^raw-[0-9a-f]{64}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ALLOWED_SOURCE_CONTENT_TYPES = frozenset({
    "application/octet-stream",
    "application/x-tar",
    "binary/octet-stream",
})
_ALLOWED_TARGET_CONTENT_TYPES = frozenset({
    "application/octet-stream",
    "application/x-tar",
})


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


_HANDOFF_CONFIG = {
    "adapter_id": HANDOFF_ADAPTER_ID,
    "adapter_version": HANDOFF_ADAPTER_VERSION,
    "all_archive_leads_minutes": list(EXPECTED_ALL_LEADS),
    "forecast_policy": HANDOFF_FORECAST_POLICY,
    "max_archive_bytes": MAX_ARCHIVE_BYTES,
    "max_archive_members": MAX_ARCHIVE_MEMBERS,
    "max_hdf_member_bytes": MAX_HDF_BYTES,
    "observation_lead_minutes": 0,
    "observation_policy": HANDOFF_OBSERVATION_POLICY,
    "schema": HANDOFF_SCHEMA,
    "source": {
        "archive_product": SOURCE_ARCHIVE_PRODUCT,
        "capture_product": SOURCE_CAPTURE_PRODUCT,
        "collector": SOURCE_COLLECTOR,
        "collector_version": SOURCE_COLLECTOR_VERSION,
        "provider": SOURCE_PROVIDER,
        "tar_inspector_version": SOURCE_TAR_INSPECTOR_VERSION,
    },
    "target": {
        "archive_product": TARGET_ARCHIVE_PRODUCT,
        "capture_product": TARGET_CAPTURE_PRODUCT,
        "collector": TARGET_COLLECTOR,
        "collector_version": HANDOFF_TARGET_COLLECTOR_VERSION,
        "provider": TARGET_PROVIDER,
    },
}
HANDOFF_CONFIG_SHA256 = hashlib.sha256(_canonical_json(_HANDOFF_CONFIG)).hexdigest()


class DwdRvCaptureHandoffError(RuntimeError):
    """The source or adapted capture violates the closed handoff contract."""


@dataclass(frozen=True, slots=True)
class DwdRvArchiveMemberEvidence:
    lead_minutes: int
    member_name: str
    size_bytes: int
    content_sha256: str

    def to_mapping(self) -> dict[str, int | str]:
        return {
            "content_sha256": self.content_sha256,
            "lead_minutes": self.lead_minutes,
            "member_name": self.member_name,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class DwdRvArchiveInventory:
    archive_sha256: str
    base_time: float
    observation: DwdRvArchiveMemberEvidence
    forecasts: tuple[DwdRvArchiveMemberEvidence, ...]
    member_inventory_sha256: str
    forecast_inventory_sha256: str

    @property
    def members(self) -> tuple[DwdRvArchiveMemberEvidence, ...]:
        return (self.observation, *self.forecasts)


@dataclass(frozen=True, slots=True)
class DwdRvCaptureHandoffReceipt:
    source_capture_id: str
    source_capture_receipt_sha256: str
    source_manifest_sha256: str
    source_receipt_id: str
    archive_sha256: str
    target_capture_id: str
    target_capture_receipt_sha256: str
    target_manifest_sha256: str
    target_receipt_id: str
    observation_member_name: str
    observation_member_sha256: str
    forecast_inventory_sha256: str
    adapter_config_sha256: str = HANDOFF_CONFIG_SHA256


def _fail(message: str) -> None:
    raise DwdRvCaptureHandoffError(message)


def _exact_keys(value: object, expected: frozenset[str], field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        _fail(f"{field} differs from the closed handoff schema")
    return value


def _sha(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        _fail(f"{field} is not a SHA-256 identity")
    return value


def _timestamp_from_match(match: re.Match[str]) -> float:
    try:
        return datetime.strptime(
            match.group("date") + match.group("time"), "%Y%m%d%H%M"
        ).replace(tzinfo=timezone.utc).timestamp()
    except ValueError as exc:  # pragma: no cover - regex plus datetime boundary
        raise DwdRvCaptureHandoffError("archive base timestamp is invalid") from exc


def inspect_dwd_rv_archive(payload: bytes) -> DwdRvArchiveInventory:
    """Hash every exact member and classify +000 separately from forecasts."""

    if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_ARCHIVE_BYTES:
        _fail("archive lies outside the exact transport boundary")
    archive_sha = hashlib.sha256(payload).hexdigest()
    try:
        archive = tarfile.open(fileobj=io.BytesIO(payload), mode="r:")
    except tarfile.TarError as exc:
        raise DwdRvCaptureHandoffError("archive is not a plain bounded tar") from exc
    evidence: list[DwdRvArchiveMemberEvidence] = []
    base_stamp: tuple[str, str] | None = None
    seen_names: set[str] = set()
    seen_leads: set[int] = set()
    with archive:
        try:
            members = archive.getmembers()
        except tarfile.TarError as exc:
            raise DwdRvCaptureHandoffError("archive member catalog is invalid") from exc
        if len(members) != len(EXPECTED_ALL_LEADS) or len(members) > MAX_ARCHIVE_MEMBERS:
            _fail("archive must contain the exact +000..+120 five-minute member set")
        for member in members:
            name = str(member.name)
            match = _ARCHIVE_MEMBER_RE.fullmatch(name)
            if (
                match is None
                or name in seen_names
                or "/" in name
                or "\\" in name
                or not member.isfile()
                or member.size <= 0
                or member.size > MAX_HDF_BYTES
            ):
                _fail("archive contains an unsafe or unsupported member")
            stamp = (match.group("date"), match.group("time"))
            if base_stamp is None:
                base_stamp = stamp
            if stamp != base_stamp:
                _fail("archive members do not share one base time")
            lead = int(match.group("lead"))
            if lead not in EXPECTED_ALL_LEADS or lead in seen_leads:
                _fail("archive lead set differs from +000..+120 by five minutes")
            try:
                stream = archive.extractfile(member)
                if stream is None:
                    _fail("archive member has no readable stream")
                member_payload = stream.read(MAX_HDF_BYTES + 1)
                trailing = stream.read(1)
            except (OSError, tarfile.TarError) as exc:
                raise DwdRvCaptureHandoffError("archive member cannot be read") from exc
            if trailing or len(member_payload) != member.size:
                _fail("archive member length differs from its tar header")
            evidence.append(DwdRvArchiveMemberEvidence(
                lead_minutes=lead,
                member_name=name,
                size_bytes=len(member_payload),
                content_sha256=hashlib.sha256(member_payload).hexdigest(),
            ))
            seen_names.add(name)
            seen_leads.add(lead)
    if seen_leads != set(EXPECTED_ALL_LEADS) or base_stamp is None:
        _fail("archive lead set is incomplete")
    ordered = tuple(sorted(evidence, key=lambda item: item.lead_minutes))
    observation = ordered[0]
    forecasts = ordered[1:]
    if observation.lead_minutes != 0 or tuple(
        item.lead_minutes for item in forecasts
    ) != EXPECTED_FORECAST_LEADS:
        _fail("observation and forecast members are not strictly separated")
    all_mapping = [item.to_mapping() for item in ordered]
    forecast_mapping = [item.to_mapping() for item in forecasts]
    return DwdRvArchiveInventory(
        archive_sha256=archive_sha,
        base_time=_timestamp_from_match(
            _ARCHIVE_MEMBER_RE.fullmatch(observation.member_name)  # type: ignore[arg-type]
        ),
        observation=observation,
        forecasts=forecasts,
        member_inventory_sha256=hashlib.sha256(_canonical_json(all_mapping)).hexdigest(),
        forecast_inventory_sha256=hashlib.sha256(
            _canonical_json(forecast_mapping)
        ).hexdigest(),
    )


def _source_provenance(receipt: RawArtifactReceipt) -> None:
    expected = {
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": SOURCE_COLLECTOR,
        "collector_version": SOURCE_COLLECTOR_VERSION,
        "provenance_scope": "exact_upstream_tar_bytes_only",
        "representation": NATIVE_REPRESENTATION,
        "structure_inspector_version": SOURCE_TAR_INSPECTOR_VERSION,
    }
    if receipt.provenance != expected:
        _fail("source raw provenance differs from capture-v2")


def _validate_source_bundle(
    capture: RawArtifactCaptureReceipt,
    receipts: Mapping[str, RawArtifactReceipt],
    payloads: Mapping[str, bytes],
) -> tuple[RawArtifactReceipt, bytes, DwdRvArchiveInventory]:
    if (
        capture.provider != SOURCE_PROVIDER
        or capture.product != SOURCE_CAPTURE_PRODUCT
        or _SOURCE_CAPTURE_ID_RE.fullmatch(capture.capture_id) is None
        or not capture.fetch_id.endswith(":capture")
        or capture.product_timestamp is None
        or not math.isfinite(capture.product_timestamp)
        or len(capture.members) != 1
        or capture.members[0].role != "rv_tar"
        or set(receipts) != {"rv_tar"}
        or set(payloads) != {"rv_tar"}
    ):
        _fail("source capture identity or role binding differs from raw v2")
    source_job = capture.fetch_id.removesuffix(":capture")
    if not source_job:
        _fail("source capture has no stable acquisition identity")
    receipt = receipts["rv_tar"]
    payload = payloads["rv_tar"]
    member = capture.members[0]
    if (
        receipt.provider != SOURCE_PROVIDER
        or receipt.product != SOURCE_ARCHIVE_PRODUCT
        or _SOURCE_RECEIPT_ID_RE.fullmatch(receipt.receipt_id) is None
        or receipt.fetch_id != f"{source_job}:rv_tar"
        or receipt.receipt_id != member.receipt_id
        or receipt.receipt_sha256 != member.receipt_sha256
        or receipt.content_sha256 != member.content_sha256
        or receipt.received_at != member.received_at
        or receipt.size_bytes != len(payload)
        or receipt.content_type not in _ALLOWED_SOURCE_CONTENT_TYPES
    ):
        _fail("source raw receipt differs from capture-v2")
    _source_provenance(receipt)
    inventory = inspect_dwd_rv_archive(payload)
    if not hmac.compare_digest(inventory.archive_sha256, receipt.content_sha256):
        raise RawArtifactIntegrityError("source archive digest differs after inspection")
    expected_raw_upstream = (
        f"{SOURCE_ARCHIVE_PRODUCT}|latest"
        f"|content_sha256={inventory.archive_sha256}"
    )
    expected_capture_upstream = (
        f"{SOURCE_CAPTURE_PRODUCT}|member={inventory.observation.member_name}"
        f"|content_sha256={inventory.archive_sha256}"
    )
    if (
        receipt.upstream_id != expected_raw_upstream
        or capture.upstream_capture_id != expected_capture_upstream
        or capture.product_timestamp != inventory.base_time
    ):
        _fail("source capture upstream or time binding differs from raw v2")
    return receipt, payload, inventory


def _target_fetch_prefix(source_capture_id: str) -> str:
    if _SOURCE_CAPTURE_ID_RE.fullmatch(source_capture_id) is None:
        raise ValueError("source_capture_id is not a schema-3 capture identity")
    return f"dwd-rv-handoff-v1:{source_capture_id}"


def _target_provenance(
    source_capture: RawArtifactCaptureReceipt,
    source_receipt: RawArtifactReceipt,
    inventory: DwdRvArchiveInventory,
) -> dict[str, Any]:
    return {
        "capture_handoff": {
            "adapter_config_sha256": HANDOFF_CONFIG_SHA256,
            "adapter_id": HANDOFF_ADAPTER_ID,
            "adapter_version": HANDOFF_ADAPTER_VERSION,
            "archive": {
                "archive_sha256": inventory.archive_sha256,
                "forecast_inventory_sha256": inventory.forecast_inventory_sha256,
                "forecast_members": [
                    item.to_mapping() for item in inventory.forecasts
                ],
                "forecast_policy": HANDOFF_FORECAST_POLICY,
                "member_count": len(inventory.members),
                "member_inventory_sha256": inventory.member_inventory_sha256,
                "observation_member": inventory.observation.to_mapping(),
                "observation_policy": HANDOFF_OBSERVATION_POLICY,
            },
            "schema": HANDOFF_SCHEMA,
            "source": {
                "capture_id": source_capture.capture_id,
                "capture_manifest_sha256": source_capture.manifest_sha256,
                "capture_product": source_capture.product,
                "capture_receipt_sha256": source_capture.capture_receipt_sha256,
                "collector": SOURCE_COLLECTOR,
                "collector_version": SOURCE_COLLECTOR_VERSION,
                "provider": source_capture.provider,
                "raw_content_sha256": source_receipt.content_sha256,
                "raw_content_type": source_receipt.content_type,
                "raw_receipt_id": source_receipt.receipt_id,
                "raw_receipt_sha256": source_receipt.receipt_sha256,
                "raw_received_at": source_receipt.received_at,
            },
        },
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": TARGET_COLLECTOR,
        "collector_version": HANDOFF_TARGET_COLLECTOR_VERSION,
        "declared_accumulation_minutes": 5.0,
        "declared_quantity": "precipitation_accumulation",
        "declared_semantic_class": "quantitative_precipitation_accumulation",
        "declared_unit": "mm",
        "parser_version": HANDOFF_ADAPTER_ID + ":" + HANDOFF_ADAPTER_VERSION,
        "product": TARGET_ARCHIVE_PRODUCT,
        "quantitative_semantics_verified_after_decode": False,
        "representation": NATIVE_REPRESENTATION,
        "requested_forecast_lead_minutes": 0,
    }


_TARGET_PROVENANCE_KEYS = frozenset({
    "capture_handoff",
    "capture_stage",
    "collector",
    "collector_version",
    "declared_accumulation_minutes",
    "declared_quantity",
    "declared_semantic_class",
    "declared_unit",
    "parser_version",
    "product",
    "quantitative_semantics_verified_after_decode",
    "representation",
    "requested_forecast_lead_minutes",
})
_HANDOFF_KEYS = frozenset({
    "adapter_config_sha256",
    "adapter_id",
    "adapter_version",
    "archive",
    "schema",
    "source",
})
_SOURCE_KEYS = frozenset({
    "capture_id",
    "capture_manifest_sha256",
    "capture_product",
    "capture_receipt_sha256",
    "collector",
    "collector_version",
    "provider",
    "raw_content_sha256",
    "raw_content_type",
    "raw_receipt_id",
    "raw_receipt_sha256",
    "raw_received_at",
})
_ARCHIVE_KEYS = frozenset({
    "archive_sha256",
    "forecast_inventory_sha256",
    "forecast_members",
    "forecast_policy",
    "member_count",
    "member_inventory_sha256",
    "observation_member",
    "observation_policy",
})
_MEMBER_KEYS = frozenset({
    "content_sha256", "lead_minutes", "member_name", "size_bytes"
})


def validate_decoder_handoff_capture(
    capture: RawArtifactCaptureReceipt,
    receipt: RawArtifactReceipt,
    archive_payload: bytes,
) -> DwdRvArchiveInventory:
    """Validate a target capture without weakening the legacy decoder profile."""

    if (
        capture.provider != TARGET_PROVIDER
        or capture.product != TARGET_CAPTURE_PRODUCT
        or capture.product_timestamp is None
        or len(capture.members) != 1
        or capture.members[0].role != "rv_tar"
        or receipt.provider != TARGET_PROVIDER
        or receipt.product != TARGET_ARCHIVE_PRODUCT
        or receipt.receipt_id != capture.members[0].receipt_id
        or receipt.content_sha256 != capture.members[0].content_sha256
        or receipt.receipt_sha256 != capture.members[0].receipt_sha256
        or receipt.received_at != capture.members[0].received_at
        or receipt.content_type not in _ALLOWED_TARGET_CONTENT_TYPES
        or receipt.size_bytes != len(archive_payload)
    ):
        _fail("adapted capture identity or raw role differs")
    provenance = _exact_keys(
        receipt.provenance, _TARGET_PROVENANCE_KEYS, "target provenance"
    )
    exact_top = {
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": TARGET_COLLECTOR,
        "collector_version": HANDOFF_TARGET_COLLECTOR_VERSION,
        "declared_accumulation_minutes": 5.0,
        "declared_quantity": "precipitation_accumulation",
        "declared_semantic_class": "quantitative_precipitation_accumulation",
        "declared_unit": "mm",
        "parser_version": HANDOFF_ADAPTER_ID + ":" + HANDOFF_ADAPTER_VERSION,
        "product": TARGET_ARCHIVE_PRODUCT,
        "quantitative_semantics_verified_after_decode": False,
        "representation": NATIVE_REPRESENTATION,
        "requested_forecast_lead_minutes": 0,
    }
    if any(provenance.get(key) != value for key, value in exact_top.items()):
        _fail("adapted provenance semantic contract differs")
    handoff = _exact_keys(provenance["capture_handoff"], _HANDOFF_KEYS, "handoff")
    if (
        handoff.get("schema") != HANDOFF_SCHEMA
        or handoff.get("adapter_id") != HANDOFF_ADAPTER_ID
        or handoff.get("adapter_version") != HANDOFF_ADAPTER_VERSION
        or handoff.get("adapter_config_sha256") != HANDOFF_CONFIG_SHA256
    ):
        _fail("handoff adapter identity differs")
    source = _exact_keys(handoff["source"], _SOURCE_KEYS, "handoff source")
    if (
        _SOURCE_CAPTURE_ID_RE.fullmatch(str(source.get("capture_id"))) is None
        or source.get("capture_product") != SOURCE_CAPTURE_PRODUCT
        or source.get("provider") != SOURCE_PROVIDER
        or source.get("collector") != SOURCE_COLLECTOR
        or source.get("collector_version") != SOURCE_COLLECTOR_VERSION
        or _SOURCE_RECEIPT_ID_RE.fullmatch(str(source.get("raw_receipt_id"))) is None
        or source.get("raw_content_type") not in _ALLOWED_SOURCE_CONTENT_TYPES
        or isinstance(source.get("raw_received_at"), bool)
        or not isinstance(source.get("raw_received_at"), (int, float))
        or not math.isfinite(float(source["raw_received_at"]))
    ):
        _fail("handoff source identity differs")
    for field in (
        "capture_manifest_sha256",
        "capture_receipt_sha256",
        "raw_content_sha256",
        "raw_receipt_sha256",
    ):
        _sha(source.get(field), f"handoff source {field}")
    source_capture_id = str(source["capture_id"])
    target_prefix = _target_fetch_prefix(source_capture_id)
    if (
        capture.fetch_id != f"{target_prefix}:capture"
        or receipt.fetch_id != f"{target_prefix}:rv_tar"
        or float(source["raw_received_at"]) != receipt.received_at
    ):
        _fail("handoff target identity is not deterministically source-bound")
    inventory = inspect_dwd_rv_archive(archive_payload)
    archive = _exact_keys(handoff["archive"], _ARCHIVE_KEYS, "handoff archive")
    observation = _exact_keys(
        archive.get("observation_member"), _MEMBER_KEYS, "observation member"
    )
    forecast_rows = archive.get("forecast_members")
    if not isinstance(forecast_rows, list) or len(forecast_rows) != len(
        EXPECTED_FORECAST_LEADS
    ):
        _fail("forecast member evidence is incomplete")
    normalized_forecasts = [
        dict(_exact_keys(row, _MEMBER_KEYS, "forecast member"))
        for row in forecast_rows
    ]
    expected_observation = inventory.observation.to_mapping()
    expected_forecasts = [item.to_mapping() for item in inventory.forecasts]
    if (
        dict(observation) != expected_observation
        or normalized_forecasts != expected_forecasts
        or archive.get("archive_sha256") != inventory.archive_sha256
        or archive.get("forecast_inventory_sha256")
        != inventory.forecast_inventory_sha256
        or archive.get("member_inventory_sha256") != inventory.member_inventory_sha256
        or archive.get("member_count") != len(EXPECTED_ALL_LEADS)
        or archive.get("forecast_policy") != HANDOFF_FORECAST_POLICY
        or archive.get("observation_policy") != HANDOFF_OBSERVATION_POLICY
        or source.get("raw_content_sha256") != inventory.archive_sha256
        or receipt.content_sha256 != inventory.archive_sha256
        or capture.product_timestamp != inventory.base_time
    ):
        _fail("handoff archive evidence differs from exact bytes")
    expected_raw_upstream = (
        f"{TARGET_ARCHIVE_PRODUCT}|lead_minutes=0"
        f"|content_sha256={inventory.archive_sha256}"
    )
    expected_capture_upstream = (
        f"{TARGET_CAPTURE_PRODUCT}|member={inventory.observation.member_name}"
        f"|product_timestamp={inventory.base_time:.6f}"
        f"|content_sha256={inventory.archive_sha256}"
    )
    if (
        receipt.upstream_id != expected_raw_upstream
        or capture.upstream_capture_id != expected_capture_upstream
    ):
        _fail("adapted raw or capture upstream identity differs")
    return inventory


def _receipt(
    source_capture: RawArtifactCaptureReceipt,
    source_receipt: RawArtifactReceipt,
    target_capture: RawArtifactCaptureReceipt,
    target_receipt: RawArtifactReceipt,
    inventory: DwdRvArchiveInventory,
) -> DwdRvCaptureHandoffReceipt:
    return DwdRvCaptureHandoffReceipt(
        source_capture_id=source_capture.capture_id,
        source_capture_receipt_sha256=source_capture.capture_receipt_sha256,
        source_manifest_sha256=source_capture.manifest_sha256,
        source_receipt_id=source_receipt.receipt_id,
        archive_sha256=inventory.archive_sha256,
        target_capture_id=target_capture.capture_id,
        target_capture_receipt_sha256=target_capture.capture_receipt_sha256,
        target_manifest_sha256=target_capture.manifest_sha256,
        target_receipt_id=target_receipt.receipt_id,
        observation_member_name=inventory.observation.member_name,
        observation_member_sha256=inventory.observation.content_sha256,
        forecast_inventory_sha256=inventory.forecast_inventory_sha256,
    )


def adapt_dwd_rv_raw_capture(
    *,
    source_capture_id: str,
    source_raw_store: RawArtifactStore,
    handoff_raw_store: RawArtifactStore,
    fault_injector: Callable[[str], None] | None = None,
) -> DwdRvCaptureHandoffReceipt:
    """Publish or verify one deterministic decoder-compatible capture.

    ``source_raw_store`` and ``handoff_raw_store`` are explicit caller-owned
    authorities.  They may not share a root: keeping the compatibility alias in
    a separate bounded store prevents acquisition-ledger accounting from being
    silently changed by this still-unwired shadow path.
    """

    if not isinstance(source_raw_store, RawArtifactStore) or not isinstance(
        handoff_raw_store, RawArtifactStore
    ):
        raise TypeError("source and handoff stores must be RawArtifactStore")
    if source_raw_store.root.resolve(strict=False) == handoff_raw_store.root.resolve(
        strict=False
    ):
        raise ValueError("handoff store must be separate from acquisition raw store")
    prefix = _target_fetch_prefix(source_capture_id)
    source_raw_store.initialize()
    handoff_raw_store.initialize()
    source_bundle = source_raw_store.load_capture_bundle_bytes(source_capture_id)
    if source_bundle is None:
        raise DwdRvCaptureHandoffError("source raw-v2 capture is missing")
    source_capture, source_receipts, source_payloads = source_bundle
    source_receipt, source_payload, inventory = _validate_source_bundle(
        source_capture, source_receipts, source_payloads
    )
    if fault_injector is not None:
        fault_injector("handoff.after_source_verified")

    target_content_type = (
        source_receipt.content_type
        if source_receipt.content_type in _ALLOWED_TARGET_CONTENT_TYPES
        else "application/x-tar"
    )
    target_receipt = handoff_raw_store.store(
        provider=TARGET_PROVIDER,
        product=TARGET_ARCHIVE_PRODUCT,
        fetch_id=f"{prefix}:rv_tar",
        content_type=target_content_type,
        received_at=source_receipt.received_at,
        upstream_id=(
            f"{TARGET_ARCHIVE_PRODUCT}|lead_minutes=0"
            f"|content_sha256={inventory.archive_sha256}"
        ),
        provenance=_target_provenance(source_capture, source_receipt, inventory),
        payload=source_payload,
    )
    if fault_injector is not None:
        fault_injector("handoff.after_target_receipt")
        fault_injector("handoff.before_target_capture")
    target_capture = handoff_raw_store.commit_capture(
        provider=TARGET_PROVIDER,
        product=TARGET_CAPTURE_PRODUCT,
        fetch_id=f"{prefix}:capture",
        upstream_capture_id=(
            f"{TARGET_CAPTURE_PRODUCT}|member={inventory.observation.member_name}"
            f"|product_timestamp={inventory.base_time:.6f}"
            f"|content_sha256={inventory.archive_sha256}"
        ),
        product_timestamp=inventory.base_time,
        members={"rv_tar": target_receipt.receipt_id},
    )
    if fault_injector is not None:
        fault_injector("handoff.after_target_capture")
    target_bundle = handoff_raw_store.load_capture_bundle_bytes(target_capture.capture_id)
    if target_bundle is None:  # pragma: no cover - append-only commit contract
        raise RawArtifactIntegrityError("adapted capture disappeared after commit")
    verified_capture, verified_receipts, verified_payloads = target_bundle
    if set(verified_receipts) != {"rv_tar"} or set(verified_payloads) != {"rv_tar"}:
        _fail("adapted capture roles differ after commit")
    verified_receipt = verified_receipts["rv_tar"]
    verified_payload = verified_payloads["rv_tar"]
    verified_inventory = validate_decoder_handoff_capture(
        verified_capture, verified_receipt, verified_payload
    )
    if (
        verified_payload != source_payload
        or verified_inventory != inventory
        or verified_receipt.content_sha256 != source_receipt.content_sha256
    ):
        raise RawArtifactIntegrityError("adapted archive is not byte-exact source raw")
    return _receipt(
        source_capture,
        source_receipt,
        verified_capture,
        verified_receipt,
        inventory,
    )


__all__ = [
    "DwdRvArchiveInventory",
    "DwdRvArchiveMemberEvidence",
    "DwdRvCaptureHandoffError",
    "DwdRvCaptureHandoffReceipt",
    "EXPECTED_ALL_LEADS",
    "EXPECTED_FORECAST_LEADS",
    "HANDOFF_ADAPTER_ID",
    "HANDOFF_ADAPTER_VERSION",
    "HANDOFF_CONFIG_SHA256",
    "HANDOFF_FORECAST_POLICY",
    "HANDOFF_OBSERVATION_POLICY",
    "HANDOFF_SCHEMA",
    "HANDOFF_TARGET_COLLECTOR_VERSION",
    "SOURCE_ARCHIVE_PRODUCT",
    "SOURCE_CAPTURE_PRODUCT",
    "SOURCE_COLLECTOR_VERSION",
    "SOURCE_PROVIDER",
    "TARGET_ARCHIVE_PRODUCT",
    "TARGET_CAPTURE_PRODUCT",
    "TARGET_COLLECTOR",
    "TARGET_PROVIDER",
    "adapt_dwd_rv_raw_capture",
    "inspect_dwd_rv_archive",
    "validate_decoder_handoff_capture",
]
