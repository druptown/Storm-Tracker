"""Small HA-side adapter for worker-owned radar providers."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import re
import time
import uuid
from collections.abc import Mapping

from .providers.dwd_radolan import read_radar_worker_artifact
from .worker_bridge import async_request_worker_job

_ARTIFACT_READER: ThreadPoolExecutor | None = None

_MAX_ARTIFACT_PATH_CHARS = 2_048
_MAX_WORKER_ARTIFACT_BYTES = 32 * 1024 * 1024
_MAX_WORKER_OBSERVATIONS = 10_000
_MAX_CHAIN_METADATA_BYTES = 32 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RAW_RECEIPT_ID_RE = re.compile(r"^raw-[0-9a-f]{64}$")
_RAW_CAPTURE_ID_RE = re.compile(r"^capture-[0-9a-f]{64}$")
_AUTH_VALUE_RE = re.compile(
    r"\b(?:Bearer|Basic)\s+[A-Za-z0-9+/_.=-]{8,}", re.IGNORECASE
)
_JWT_VALUE_RE = re.compile(
    r"(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{12,}\."
    r"[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{8,}(?![A-Za-z0-9_-])"
)
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?:access[_-]?token|refresh[_-]?token|api[_-]?key|client[_-]?secret|"
    r"authorization|password|secret|credential|private[_-]?key|"
    r"x-amz-signature|signature|sig|(?:^|[?&|;,])k)\s*=",
    re.IGNORECASE,
)

_KMI_TOP_LEVEL_FIELDS = (
    "provider",
    "product",
    "semantic_class",
    "quantitative_semantics_eligible",
    "operational_use",
    "operational_admission_eligible",
    "admission_status",
    "legacy_decoder_quality_constant",
    "legacy_decoder_quality_operational",
    "provenance_grade",
    "provenance_scope",
)
_KMI_RAW_PROVENANCE_KEYS = frozenset({
    "raw_capture_id",
    "raw_capture_manifest_sha256",
    "raw_capture_fetch_id",
    "upstream_capture_id",
    "product_timestamp",
    "collector_version",
    "parser_versions",
    "provenance_grade",
    "provenance_scope",
    "raw_sources",
})
_KMI_RAW_SOURCE_KEYS = frozenset({
    "role",
    "raw_receipt_id",
    "raw_content_sha256",
    "fetch_id",
    "upstream_record_id",
    "received_at",
    "content_type",
    "parser_version",
    "collector_version",
    "provenance_grade",
    "provenance_scope",
    "semantic_class",
    "quantitative_semantics_eligible",
})
_KMI_BRIDGE_KEYS = frozenset({
    "status",
    "current_contract",
    "required_representation",
    "raw_capture_id",
    "raw_capture_manifest_sha256",
    "member_receipt_ids",
    "single_receipt_complete_set",
    "exact_raw_artifacts_complete_in_v1",
})


def _bounded_text(
    value: object,
    name: str,
    maximum: int,
    *,
    record_identity: bool = False,
) -> str:
    """Validate one compact secret-free text field without normalising it."""
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError(f"{name} is geen geldige begrensde tekst")
    if (
        _AUTH_VALUE_RE.search(value)
        or _JWT_VALUE_RE.search(value)
        or _SECRET_ASSIGNMENT_RE.search(value)
    ):
        raise ValueError(f"{name} bevat mogelijk geheim transportmateriaal")
    if record_identity and any(marker in value for marker in ("://", "?", "#", "&")):
        raise ValueError(f"{name} mag geen URL- of querymateriaal bevatten")
    return value


def _exact_mapping(
    value: object,
    name: str,
    keys: frozenset[str],
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ValueError(f"{name} heeft een onverwacht of onvolledig schema")
    return value


def _sha256(value: object, name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{name} is geen canonieke SHA-256")
    return value


def _timestamp(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} is geen geldige tijd")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"{name} is geen geldige tijd")
    return result


def _copy_base_artifact_metadata(result: Mapping[str, object]) -> dict:
    metadata: dict[str, object] = {}
    if "artifact_path" in result:
        metadata["artifact_path"] = _bounded_text(
            result["artifact_path"],
            "artifact_path",
            _MAX_ARTIFACT_PATH_CHARS,
        )
    if "artifact_sha256" in result:
        metadata["artifact_sha256"] = _sha256(
            result["artifact_sha256"], "artifact_sha256"
        )
    if "artifact_bytes" in result:
        value = result["artifact_bytes"]
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 < value <= _MAX_WORKER_ARTIFACT_BYTES
        ):
            raise ValueError("artifact_bytes valt buiten het workercontract")
        metadata["artifact_bytes"] = value
    if "content_type" in result:
        content_type = _bounded_text(result["content_type"], "content_type", 128)
        if content_type != "application/json+gzip":
            raise ValueError("content_type is geen JSON-workerartifact")
        metadata["content_type"] = content_type
    if "frame_timestamp" in result:
        metadata["frame_timestamp"] = _timestamp(
            result["frame_timestamp"], "frame_timestamp"
        )
    if "observation_count" in result:
        value = result["observation_count"]
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 <= value <= _MAX_WORKER_OBSERVATIONS
        ):
            raise ValueError("observation_count valt buiten het workercontract")
        metadata["observation_count"] = value
    return metadata


def _copy_kmi_raw_source(
    value: object,
    *,
    role: str,
    job_id: str,
    collector_version: str,
    parser_version: str,
) -> dict[str, object]:
    source = _exact_mapping(value, f"raw_sources.{role}", _KMI_RAW_SOURCE_KEYS)
    copied = {
        "role": _bounded_text(source["role"], f"{role}.role", 32),
        "raw_receipt_id": _bounded_text(
            source["raw_receipt_id"], f"{role}.raw_receipt_id", 96
        ),
        "raw_content_sha256": _sha256(
            source["raw_content_sha256"], f"{role}.raw_content_sha256"
        ),
        "fetch_id": _bounded_text(
            source["fetch_id"], f"{role}.fetch_id", 512,
            record_identity=True,
        ),
        "upstream_record_id": _bounded_text(
            source["upstream_record_id"], f"{role}.upstream_record_id", 1_024,
            record_identity=True,
        ),
        "received_at": _timestamp(source["received_at"], f"{role}.received_at"),
        "content_type": _bounded_text(
            source["content_type"], f"{role}.content_type", 128
        ),
        "parser_version": _bounded_text(
            source["parser_version"], f"{role}.parser_version", 256
        ),
        "collector_version": _bounded_text(
            source["collector_version"], f"{role}.collector_version", 256
        ),
        "provenance_grade": _bounded_text(
            source["provenance_grade"], f"{role}.provenance_grade", 64
        ),
        "provenance_scope": _bounded_text(
            source["provenance_scope"], f"{role}.provenance_scope", 64
        ),
        "semantic_class": _bounded_text(
            source["semantic_class"], f"{role}.semantic_class", 128
        ),
        "quantitative_semantics_eligible": source[
            "quantitative_semantics_eligible"
        ],
    }
    if copied["role"] != role:
        raise ValueError(f"raw_sources.{role} heeft een andere rol")
    if _RAW_RECEIPT_ID_RE.fullmatch(str(copied["raw_receipt_id"])) is None:
        raise ValueError(f"raw_sources.{role} heeft een ongeldig receipt-ID")
    if copied["fetch_id"] != f"{job_id}:{role}":
        raise ValueError(f"raw_sources.{role} hoort niet bij deze capture")
    if copied["collector_version"] != collector_version:
        raise ValueError(f"raw_sources.{role} heeft een andere collector")
    if copied["parser_version"] != parser_version:
        raise ValueError(f"raw_sources.{role} heeft een andere parser")
    if copied["provenance_grade"] != "exact_raw":
        raise ValueError(f"raw_sources.{role} mist exact-raw provenance")
    if copied["provenance_scope"] != "upstream_bytes_only":
        raise ValueError(f"raw_sources.{role} heeft een onjuiste provenance-scope")
    expected_semantic_class = (
        "forecast_manifest" if role == "forecast_json"
        else "categorical_rendered_image"
    )
    if copied["semantic_class"] != expected_semantic_class:
        raise ValueError(f"raw_sources.{role} heeft een onjuiste semantiek")
    if copied["quantitative_semantics_eligible"] is not False:
        raise ValueError(f"raw_sources.{role} maakt een kwantitatieve claim")
    return copied


def _copy_kmi_chain_metadata(result: Mapping[str, object]) -> dict[str, object]:
    """Validate and copy the complete linked KMI raw-capture proof."""
    provenance = _exact_mapping(
        result.get("raw_provenance"),
        "raw_provenance",
        _KMI_RAW_PROVENANCE_KEYS,
    )
    bridge = _exact_mapping(
        result.get("radar_cycle_bridge"),
        "radar_cycle_bridge",
        _KMI_BRIDGE_KEYS,
    )
    missing = [key for key in _KMI_TOP_LEVEL_FIELDS if key not in result]
    if missing:
        raise ValueError(f"KMI-chainmetadata mist {', '.join(missing)}")

    capture_id = _bounded_text(
        provenance["raw_capture_id"], "raw_capture_id", 96
    )
    if _RAW_CAPTURE_ID_RE.fullmatch(capture_id) is None:
        raise ValueError("raw_capture_id heeft een ongeldig formaat")
    capture_sha256 = _sha256(
        provenance["raw_capture_manifest_sha256"],
        "raw_capture_manifest_sha256",
    )
    capture_fetch_id = _bounded_text(
        provenance["raw_capture_fetch_id"],
        "raw_capture_fetch_id",
        512,
        record_identity=True,
    )
    if not capture_fetch_id.endswith(":capture"):
        raise ValueError("raw_capture_fetch_id mist de capturerol")
    job_id = capture_fetch_id.removesuffix(":capture")
    if not job_id:
        raise ValueError("raw_capture_fetch_id mist job-ID")
    collector_version = _bounded_text(
        provenance["collector_version"], "collector_version", 256
    )
    parser_versions = _exact_mapping(
        provenance["parser_versions"],
        "parser_versions",
        frozenset({"forecast_json", "radar_png"}),
    )
    copied_parser_versions = {
        role: _bounded_text(
            parser_versions[role], f"parser_versions.{role}", 256
        )
        for role in ("forecast_json", "radar_png")
    }
    sources = _exact_mapping(
        provenance["raw_sources"],
        "raw_sources",
        frozenset({"forecast_json", "radar_png"}),
    )
    copied_sources = {
        role: _copy_kmi_raw_source(
            sources[role],
            role=role,
            job_id=job_id,
            collector_version=collector_version,
            parser_version=copied_parser_versions[role],
        )
        for role in ("forecast_json", "radar_png")
    }
    copied_provenance: dict[str, object] = {
        "raw_capture_id": capture_id,
        "raw_capture_manifest_sha256": capture_sha256,
        "raw_capture_fetch_id": capture_fetch_id,
        "upstream_capture_id": _bounded_text(
            provenance["upstream_capture_id"],
            "upstream_capture_id",
            1_024,
            record_identity=True,
        ),
        "product_timestamp": _timestamp(
            provenance["product_timestamp"], "product_timestamp"
        ),
        "collector_version": collector_version,
        "parser_versions": copied_parser_versions,
        "provenance_grade": _bounded_text(
            provenance["provenance_grade"], "provenance_grade", 64
        ),
        "provenance_scope": _bounded_text(
            provenance["provenance_scope"], "provenance_scope", 64
        ),
        "raw_sources": copied_sources,
    }
    if copied_provenance["provenance_grade"] != "exact_raw":
        raise ValueError("KMI-capture mist exact-raw provenance")
    if copied_provenance["provenance_scope"] != "upstream_bytes_only":
        raise ValueError("KMI-capture heeft een onjuiste provenance-scope")

    member_receipt_ids = bridge["member_receipt_ids"]
    if (
        not isinstance(member_receipt_ids, list)
        or len(member_receipt_ids) != 2
    ):
        raise ValueError("radar_cycle_bridge heeft geen begrensde ledenlijst")
    copied_member_ids = [
        _bounded_text(value, "member_receipt_ids", 96)
        for value in member_receipt_ids
    ]
    expected_member_ids = {
        str(source["raw_receipt_id"]) for source in copied_sources.values()
    }
    if (
        len(set(copied_member_ids)) != 2
        or set(copied_member_ids) != expected_member_ids
    ):
        raise ValueError("radar_cycle_bridge verwijst naar andere receipts")
    copied_bridge: dict[str, object] = {
        "status": _bounded_text(bridge["status"], "bridge.status", 64),
        "current_contract": _bounded_text(
            bridge["current_contract"], "bridge.current_contract", 128
        ),
        "required_representation": _bounded_text(
            bridge["required_representation"],
            "bridge.required_representation",
            128,
        ),
        "raw_capture_id": _bounded_text(
            bridge["raw_capture_id"], "bridge.raw_capture_id", 96
        ),
        "raw_capture_manifest_sha256": _sha256(
            bridge["raw_capture_manifest_sha256"],
            "bridge.raw_capture_manifest_sha256",
        ),
        "member_receipt_ids": copied_member_ids,
        "single_receipt_complete_set": bridge["single_receipt_complete_set"],
        "exact_raw_artifacts_complete_in_v1": bridge[
            "exact_raw_artifacts_complete_in_v1"
        ],
    }
    if (
        copied_bridge["status"] != "not_bridged_linked_capture"
        or copied_bridge["current_contract"] != "stv3-radar-cycle-request-v1"
        or copied_bridge["required_representation"] != "raw_capture_set"
        or copied_bridge["raw_capture_id"] != capture_id
        or copied_bridge["raw_capture_manifest_sha256"] != capture_sha256
        or copied_bridge["single_receipt_complete_set"] is not False
        or copied_bridge["exact_raw_artifacts_complete_in_v1"] is not False
    ):
        raise ValueError("radar_cycle_bridge maakt een onjuiste v1-claim")

    expected_top_level: dict[str, object] = {
        "provider": "kmi",
        "product": "kmi_appv4_visualisation",
        "semantic_class": "categorical_rendered_image",
        "quantitative_semantics_eligible": False,
        "operational_use": "visualisation_only",
        "operational_admission_eligible": False,
        "admission_status": "uncertified_visualisation_only",
        "legacy_decoder_quality_constant": 0.95,
        "legacy_decoder_quality_operational": False,
        "provenance_grade": "exact_raw",
        "provenance_scope": "upstream_bytes_only",
    }
    copied: dict[str, object] = {}
    for key, expected in expected_top_level.items():
        actual = result[key]
        if actual != expected or type(actual) is not type(expected):
            raise ValueError(f"KMI-chainmetadata heeft een onjuiste {key}")
        copied[key] = actual
    if result.get("observation_count") != 0 or type(
        result.get("observation_count")
    ) is not int:
        raise ValueError("KMI-visualisatie mag geen operationele observaties bevatten")
    frame_timestamp = _timestamp(result.get("frame_timestamp"), "frame_timestamp")
    if copied_provenance["product_timestamp"] != frame_timestamp:
        raise ValueError("KMI product- en capturetijd verschillen")
    copied["raw_provenance"] = copied_provenance
    copied["radar_cycle_bridge"] = copied_bridge
    return copied


def _artifact_reader() -> ThreadPoolExecutor:
    """Return a lazy executor that can be recreated after entry reload."""
    global _ARTIFACT_READER
    if _ARTIFACT_READER is None:
        _ARTIFACT_READER = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="storm_tracker_worker_artifact_reader",
        )
    return _ARTIFACT_READER


def _radar_job_id(
    job_type: str,
    payload: dict,
    *,
    timestamp: float | None = None,
) -> str:
    """Build one stable ID per canonical request and five-minute product slot."""
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    slot = int((time.time() if timestamp is None else timestamp) // 300)
    request_key = f"{job_type}:{slot}:{hashlib.sha256(canonical).hexdigest()}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, request_key))


def worker_radar_artifact_metadata(result: dict) -> dict:
    """Return a bounded immutable handle, including linked KMI raw proof."""
    if not isinstance(result, Mapping):
        raise ValueError("workerresultaat is geen metadataobject")
    metadata = _copy_base_artifact_metadata(result)
    has_provenance = "raw_provenance" in result
    has_bridge = "radar_cycle_bridge" in result
    if has_provenance != has_bridge:
        raise ValueError("KMI-chainmetadata is slechts gedeeltelijk aanwezig")
    if has_provenance:
        metadata.update(_copy_kmi_chain_metadata(result))
    encoded = json.dumps(
        metadata,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(encoded) > _MAX_CHAIN_METADATA_BYTES:
        raise ValueError("worker-chainmetadata overschrijdt de veiligheidslimiet")
    return metadata


def remember_worker_radar_artifact(hass, provider: str, result: dict) -> dict:
    """Retain only immutable metadata needed for worker-to-worker chaining."""
    from .const import DOMAIN
    metadata = worker_radar_artifact_metadata(result)
    hass.data[DOMAIN].setdefault("provider_worker_artifacts", {})[
        provider
    ] = metadata
    return metadata


async def async_fetch_worker_radar(
    hass,
    job_type: str,
    payload: dict,
    *,
    provider: str,
    timeout: float,
    max_age_seconds: float,
    allowed_sources: frozenset[str] | None = None,
):
    """Request one provider job and validate its compact shared artifact."""
    job_id = _radar_job_id(job_type, payload)
    result = await async_request_worker_job(
        hass, job_type, payload, timeout=timeout, job_id=job_id
    )
    observations, overlay = await asyncio.get_running_loop().run_in_executor(
        _artifact_reader(),
        read_radar_worker_artifact,
        result,
        provider,
        max_age_seconds,
        allowed_sources,
    )
    artifact = remember_worker_radar_artifact(hass, provider, result)
    return (
        observations,
        overlay,
        dict(result.get("diagnostics") or {}),
        artifact,
    )


def close_worker_radar_client() -> None:
    """Stop the private artifact reader during integration unload."""
    global _ARTIFACT_READER
    executor, _ARTIFACT_READER = _ARTIFACT_READER, None
    if executor is not None:
        executor.shutdown(wait=False, cancel_futures=True)
