"""Pure, fail-closed target facts for the closed satellite source set.

The only quantitatively decoded satellite workload today is NOAA Enterprise
Rain Rate.  Its existing canonical radar-field bridge proves the source array,
units, native GLB-5 geometry and retained DQF planes before this module reuses
the already verified target sampler.  GOES-18 RRQPE, GOES-19 RRQPE and H SAF
H40B currently prove exact raw containers and metadata only: their native
contracts explicitly do not read pixel or coordinate values.  Those three
sources therefore remain visible for every target as content-bound withheld
evidence, never as invented rain, accumulation, dry weather or geolocation.

This module performs no network, filesystem, scheduling, service wiring,
meteorological admission, forecast or alert work.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import re
from typing import Any, Mapping, Sequence

from .canonical_radar_field_bridge import RADAR_FIELD_BRIDGE_ALGORITHM
from .desired_state import DesiredState, Target
from .multi_source_radar_facts import (
    RADAR_TARGET_CONTRIBUTION_CONTRACT,
    RadarClaimStatus,
    RadarCoverageState,
    RadarObservationState,
    RadarTargetContribution,
    VerifiedRadarFieldSource,
    build_radar_target_contribution_batch,
)
from .raw_manifest_audit_completion_consumer import (
    OUTPUT_DOCUMENT_SCHEMA,
    OUTPUT_MODE,
    OUTPUT_RECEIPT_SCHEMA,
    PROCESS_EFFECT_CONTRACT_SHA256,
    PROCESS_RECEIPT_SCHEMA,
    RawManifestAuditOutputReceipt,
)
from .raw_artifacts import RAW_ARTIFACT_RECEIPT_SCHEMA


SATELLITE_TARGET_CONTRIBUTION_CONTRACT = (
    "stv3-multi-source-satellite-target-contribution-v1"
)
SATELLITE_TARGET_BATCH_CONTRACT = "stv3-multi-source-satellite-target-batch-v1"
SATELLITE_TARGET_BATCH_PROCESS_RECEIPT_CONTRACT = (
    "stv3-multi-source-satellite-target-batch-process-receipt-v1"
)
SATELLITE_CYCLE_EVIDENCE_ROLE = "target_satellite_candidate_batch"
ACQUISITION_CATALOG_AUTHORITY_SHA256 = (
    "9cd9af5bf6324cea930e6eb044a7af5ed2f7783bedbceacb9056faf0a88199fb"
)
NOAA_ERR_SEMANTICS_CERTIFICATE_ID = "noaa.err.instantaneous-rate.v1"
MAX_SATELLITE_SOURCES = 4
MAX_SATELLITE_TARGETS = 64
MAX_RAW_AUDIT_DOCUMENT_BYTES = 64 * 1024
MAX_RAW_EVIDENCE_ROLES = 64

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_EVENT_RE = re.compile(r"^completion-[0-9a-f]{64}$")


class SatelliteTargetFactsError(ValueError):
    """A satellite authority, source receipt or target matrix is not exact."""


class SatelliteObservationState(str, Enum):
    POSITIVE_RAIN_CANDIDATE = "positive_rain_candidate"
    DRY_WITHHELD = "dry_withheld"
    NODATA_WITHHELD = "nodata_withheld"
    QUANTITATIVE_DECODER_WITHHELD = "quantitative_decoder_withheld"
    OUTSIDE_CATALOG_APPLICABILITY = "outside_catalog_applicability"
    OUTSIDE_NATIVE_DOMAIN = "outside_native_domain"
    TARGET_UNAVAILABLE = "target_unavailable"


class SatelliteCoverageState(str, Enum):
    IN_NATIVE_DOMAIN_VALID = "in_native_domain_valid"
    IN_NATIVE_DOMAIN_UNDETECT = "in_native_domain_undetect"
    IN_NATIVE_DOMAIN_NODATA = "in_native_domain_nodata"
    CATALOG_APPLICABLE_DECODER_WITHHELD = (
        "catalog_applicable_decoder_withheld"
    )
    OUTSIDE_CATALOG_APPLICABILITY = "outside_catalog_applicability"
    OUTSIDE_NATIVE_DOMAIN = "outside_native_domain"
    TARGET_UNAVAILABLE = "target_unavailable"


class SatelliteClaimStatus(str, Enum):
    POSITIVE_CANDIDATE_ONLY = "positive_current_rain_candidate_only"
    NEGATIVE_WITHHELD = (
        "negative_claim_withheld_requires_full_scan_and_external_truth"
    )
    QUANTITATIVE_WITHHELD = (
        "quantitative_claim_withheld_requires_pixel_decoder_geometry_units_qc"
    )
    NO_CLAIM = "no_operational_claim_missing_outside_or_unavailable"


def _canonical_json(value: Any, *, ascii_only: bool = False) -> bytes:
    try:
        text = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=ascii_only,
            separators=(",", ":"),
            sort_keys=True,
        )
        return text.encode("ascii" if ascii_only else "utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise SatelliteTargetFactsError("value is not canonical JSON") from exc


def _digest(value: Any, *, ascii_only: bool = False) -> str:
    return hashlib.sha256(_canonical_json(value, ascii_only=ascii_only)).hexdigest()


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise SatelliteTargetFactsError(f"{label} is not a lowercase SHA-256")
    return value


def _safe_id(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or _SAFE_ID_RE.fullmatch(value) is None
        or "://" in value
    ):
        raise SatelliteTargetFactsError(f"{label} is not a bounded identifier")
    return value


def _finite(value: object, label: str, *, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SatelliteTargetFactsError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < minimum:
        raise SatelliteTargetFactsError(f"{label} is outside its finite bound")
    return result


def _exact(value: object, keys: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise SatelliteTargetFactsError(f"{label} schema is not exact")
    return value


@dataclass(frozen=True, slots=True)
class SatelliteProviderProfile:
    """Closed workload identity plus catalog-only target applicability."""

    workload_id: str
    provider_id: str
    product_id: str
    platform_id: str | None
    coverage_bbox: tuple[float, float, float, float] | None
    evidence_mode: str
    raw_consumer_id: str | None
    semantics_certificate_id: str | None

    def __post_init__(self) -> None:
        for value, label in (
            (self.workload_id, "workload_id"),
            (self.provider_id, "provider_id"),
            (self.product_id, "product_id"),
            (self.evidence_mode, "evidence_mode"),
        ):
            _safe_id(value, f"profile.{label}")
        for value, label in (
            (self.platform_id, "platform_id"),
            (self.raw_consumer_id, "raw_consumer_id"),
            (self.semantics_certificate_id, "semantics_certificate_id"),
        ):
            if value is not None:
                _safe_id(value, f"profile.{label}")
        if self.evidence_mode not in {
            "verified_raw_container_no_pixel_decoder",
            "verified_canonical_rain_rate_field",
        }:
            raise SatelliteTargetFactsError("profile evidence mode is unsupported")
        if (self.raw_consumer_id is None) == (
            self.semantics_certificate_id is None
        ):
            raise SatelliteTargetFactsError(
                "profile must bind exactly one raw or quantitative authority"
            )
        if self.coverage_bbox is not None:
            if not isinstance(self.coverage_bbox, tuple) or len(self.coverage_bbox) != 4:
                raise SatelliteTargetFactsError("profile bbox is invalid")
            south, west, north, east = self.coverage_bbox
            if not (
                -90.0 <= south <= north <= 90.0
                and -180.0 <= west <= east <= 180.0
            ):
                raise SatelliteTargetFactsError("profile bbox is inverted")

    @property
    def authority_sha256(self) -> str:
        return _digest(
            {
                "acquisition_catalog_sha256": ACQUISITION_CATALOG_AUTHORITY_SHA256,
                "coverage_bbox": (
                    None if self.coverage_bbox is None else list(self.coverage_bbox)
                ),
                "evidence_mode": self.evidence_mode,
                "platform_id": self.platform_id,
                "product_id": self.product_id,
                "provider_id": self.provider_id,
                "raw_consumer_id": self.raw_consumer_id,
                "semantics_certificate_id": self.semantics_certificate_id,
                "workload_id": self.workload_id,
            }
        )


SATELLITE_PROVIDER_PROFILES = (
    SatelliteProviderProfile(
        workload_id="satellite.goes18.rrqpe",
        provider_id="noaa_goes",
        product_id="abi_l2_rrqpef_native_netcdf4",
        platform_id="G18",
        coverage_bbox=(-65.0, -180.0, 65.0, -55.0),
        evidence_mode="verified_raw_container_no_pixel_decoder",
        raw_consumer_id="goes18-rrqpe-raw-manifest-audit-v1",
        semantics_certificate_id=None,
    ),
    SatelliteProviderProfile(
        workload_id="satellite.goes19.rrqpe",
        provider_id="noaa_goes",
        product_id="abi_l2_rrqpef_native_netcdf4",
        platform_id="G19",
        coverage_bbox=(-65.0, -140.0, 65.0, 20.0),
        evidence_mode="verified_raw_container_no_pixel_decoder",
        raw_consumer_id="goes19-rrqpe-raw-manifest-audit-v1",
        semantics_certificate_id=None,
    ),
    SatelliteProviderProfile(
        workload_id="satellite.hsaf.h40b",
        provider_id="hsaf",
        product_id="hsaf_h40b_native_netcdf_gzip",
        platform_id=None,
        coverage_bbox=(-70.0, -70.0, 70.0, 70.0),
        evidence_mode="verified_raw_container_no_pixel_decoder",
        raw_consumer_id="hsaf-h40b-raw-manifest-audit-v1",
        semantics_certificate_id=None,
    ),
    SatelliteProviderProfile(
        workload_id="satellite.noaa.err",
        provider_id="noaa",
        product_id="enterprise_rainrate_glb5_native_netcdf4",
        platform_id=None,
        coverage_bbox=None,
        evidence_mode="verified_canonical_rain_rate_field",
        raw_consumer_id=None,
        semantics_certificate_id=NOAA_ERR_SEMANTICS_CERTIFICATE_ID,
    ),
)
_PROFILE_BY_WORKLOAD = {
    profile.workload_id: profile for profile in SATELLITE_PROVIDER_PROFILES
}
_RAW_WORKLOADS = frozenset(
    profile.workload_id
    for profile in SATELLITE_PROVIDER_PROFILES
    if profile.raw_consumer_id is not None
)
_EXPECTED_WORKLOADS = frozenset(_PROFILE_BY_WORKLOAD)


@dataclass(frozen=True, slots=True)
class SatelliteSourceEvidence:
    """Opaque verified source identity retained without provider-byte copying."""

    role: str
    content_sha256: str
    receipt_sha256: str
    schema_id: str

    def __post_init__(self) -> None:
        _safe_id(self.role, "evidence.role")
        _sha256(self.content_sha256, "evidence.content_sha256")
        _sha256(self.receipt_sha256, "evidence.receipt_sha256")
        _safe_id(self.schema_id, "evidence.schema_id")

    def as_dict(self) -> dict[str, Any]:
        return {
            "content_sha256": self.content_sha256,
            "receipt_sha256": self.receipt_sha256,
            "role": self.role,
            "schema_id": self.schema_id,
        }


@dataclass(frozen=True, slots=True)
class SatelliteQualitySample:
    plane: str
    semantics: str
    value: bool | int | float | None

    def __post_init__(self) -> None:
        _safe_id(self.plane, "quality.plane")
        if (
            not isinstance(self.semantics, str)
            or not self.semantics
            or len(self.semantics) > 256
        ):
            raise SatelliteTargetFactsError("quality semantics are invalid")
        if isinstance(self.value, float) and not math.isfinite(self.value):
            raise SatelliteTargetFactsError("quality value is not finite")
        if self.value is not None and not isinstance(
            self.value, (bool, int, float)
        ):
            raise SatelliteTargetFactsError("quality value type is invalid")

    def as_dict(self) -> dict[str, Any]:
        return {
            "plane": self.plane,
            "semantics": self.semantics,
            "value": self.value,
        }


@dataclass(frozen=True, slots=True)
class _VerifiedRawSnapshot:
    canonical_bytes: bytes
    evidence: tuple[SatelliteSourceEvidence, ...]


def _verify_raw_audit_receipt(
    receipt: RawManifestAuditOutputReceipt,
    profile: SatelliteProviderProfile,
) -> _VerifiedRawSnapshot:
    """Reprove a typed durable audit receipt without reopening any store."""

    if not isinstance(receipt, RawManifestAuditOutputReceipt):
        raise SatelliteTargetFactsError("raw source requires an audit output receipt")
    if profile.raw_consumer_id is None or profile.workload_id not in _RAW_WORKLOADS:
        raise SatelliteTargetFactsError("raw receipt was bound to a quantitative profile")
    doc = _exact(
        receipt.output,
        {
            "audit",
            "authority",
            "claims",
            "consumer_id",
            "disposition",
            "event",
            "fact_revision",
            "isolation",
            "mode",
            "schema",
            "stream_id",
            "workload_id",
        },
        "raw audit output",
    )
    body = _canonical_json(doc, ascii_only=True)
    if not 0 < len(body) <= MAX_RAW_AUDIT_DOCUMENT_BYTES:
        raise SatelliteTargetFactsError("raw audit output exceeds its byte bound")
    output_sha = hashlib.sha256(body).hexdigest()
    event = _exact(
        doc["event"],
        {"event_id", "event_sha256", "producer_receipt_sha256", "stream_sequence"},
        "raw audit event",
    )
    authority = _exact(
        doc["authority"],
        {"authority_sha256", "desired_state_sha256"},
        "raw audit authority",
    )
    audit = _exact(
        doc["audit"],
        {
            "acquisition_job_id",
            "capture_id",
            "capture_manifest_sha256",
            "capture_receipt_sha256",
            "product_id",
            "provider_id",
            "raw_bytes_verified",
            "role_set_content_sha256",
            "role_set_receipt_sha256",
            "roles",
            "total_bytes",
        },
        "raw audit evidence",
    )
    isolation = _exact(
        doc["isolation"],
        {
            "authority_sha256",
            "effect_contract_sha256",
            "limits_sha256",
            "process_boundary",
            "resource_reason",
            "runtime_wiring",
        },
        "raw audit isolation",
    )
    claims = _exact(
        doc["claims"],
        {
            "decoded",
            "fact_staged",
            "meteorological_admission",
            "operational_snapshot_published",
            "raw_bytes_copied",
            "raw_refetch_performed",
        },
        "raw audit claims",
    )
    if (
        doc["schema"] != OUTPUT_DOCUMENT_SCHEMA
        or doc["mode"] != OUTPUT_MODE
        or doc["workload_id"] != profile.workload_id
        or doc["consumer_id"] != profile.raw_consumer_id
        or doc["stream_id"] != profile.workload_id
        or doc["disposition"] != "raw_manifest_verified_shadow"
        or doc["fact_revision"] is not None
        or any(value is not False for value in claims.values())
        or audit["raw_bytes_verified"] is not True
        or audit["provider_id"] != profile.provider_id
        or audit["product_id"] != profile.product_id
        or isolation["effect_contract_sha256"] != PROCESS_EFFECT_CONTRACT_SHA256
        or isolation["process_boundary"]
        != "fresh_spawn_hard_cpu_rss_wall_output_caps"
        or isolation["resource_reason"]
        != "full_sha_pass_over_bounded_multi_role_raw_bytes"
        or isolation["runtime_wiring"] != "inert"
    ):
        raise SatelliteTargetFactsError("raw audit route or claims differ")
    if (
        not isinstance(event["event_id"], str)
        or _EVENT_RE.fullmatch(event["event_id"]) is None
        or isinstance(event["stream_sequence"], bool)
        or not isinstance(event["stream_sequence"], int)
        or event["stream_sequence"] < 1
    ):
        raise SatelliteTargetFactsError("raw audit event identity is invalid")
    for value, label in (
        (event["event_sha256"], "event_sha256"),
        (event["producer_receipt_sha256"], "producer_receipt_sha256"),
        (authority["authority_sha256"], "authority_sha256"),
        (authority["desired_state_sha256"], "desired_state_sha256"),
        (audit["capture_manifest_sha256"], "capture_manifest_sha256"),
        (audit["capture_receipt_sha256"], "capture_receipt_sha256"),
        (audit["role_set_content_sha256"], "role_set_content_sha256"),
        (audit["role_set_receipt_sha256"], "role_set_receipt_sha256"),
        (isolation["authority_sha256"], "isolation_authority_sha256"),
        (isolation["limits_sha256"], "isolation_limits_sha256"),
    ):
        _sha256(value, f"raw audit {label}")
    for value, label in (
        (audit["acquisition_job_id"], "acquisition_job_id"),
        (audit["capture_id"], "capture_id"),
    ):
        _safe_id(value, f"raw audit {label}")
    roles = audit["roles"]
    if not isinstance(roles, list) or not 1 <= len(roles) <= MAX_RAW_EVIDENCE_ROLES:
        raise SatelliteTargetFactsError("raw audit role count is invalid")
    evidence: list[SatelliteSourceEvidence] = []
    role_names: set[str] = set()
    total_bytes = 0
    for value in roles:
        role = _exact(
            value,
            {
                "content_sha256",
                "content_type",
                "product",
                "provider",
                "receipt_id",
                "receipt_sha256",
                "role",
                "size_bytes",
            },
            "raw audit role",
        )
        name = _safe_id(role["role"], "raw role")
        _safe_id(role["receipt_id"], "raw receipt ID")
        content_sha = _sha256(role["content_sha256"], "raw role content")
        role_receipt_sha = _sha256(role["receipt_sha256"], "raw role receipt")
        if (
            name in role_names
            or not isinstance(role["content_type"], str)
            or not role["content_type"]
            or len(role["content_type"]) > 256
            or isinstance(role["size_bytes"], bool)
            or not isinstance(role["size_bytes"], int)
            or not 1 <= role["size_bytes"] <= 512 * 1024 * 1024
        ):
            raise SatelliteTargetFactsError("raw audit role authority differs")
        _safe_id(role["provider"], "raw role provider")
        _safe_id(role["product"], "raw role product")
        role_names.add(name)
        total_bytes += role["size_bytes"]
        evidence.append(
            SatelliteSourceEvidence(
                role=f"raw_{name}",
                content_sha256=content_sha,
                receipt_sha256=role_receipt_sha,
                schema_id=RAW_ARTIFACT_RECEIPT_SCHEMA,
            )
        )
    if (
        isinstance(audit["total_bytes"], bool)
        or not isinstance(audit["total_bytes"], int)
        or audit["total_bytes"] != total_bytes
    ):
        raise SatelliteTargetFactsError("raw audit byte total differs")
    created_at = _finite(receipt.created_at, "raw receipt created_at")
    receipt_sha = _digest(
        {
            "created_at": created_at,
            "output_sha256": output_sha,
            "schema": OUTPUT_RECEIPT_SCHEMA,
        },
        ascii_only=True,
    )
    process_sha = _digest(
        {
            "created_at": created_at,
            "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
            "event_id": event["event_id"],
            "isolation_authority_sha256": isolation["authority_sha256"],
            "limits_sha256": isolation["limits_sha256"],
            "output_receipt_sha256": receipt_sha,
            "output_sha256": output_sha,
            "schema": PROCESS_RECEIPT_SCHEMA,
        },
        ascii_only=True,
    )
    checks = (
        (receipt.output_sha256, output_sha),
        (receipt.receipt_sha256, receipt_sha),
        (receipt.process_receipt_sha256, process_sha),
        (receipt.event_id, event["event_id"]),
        (receipt.event_sha256, event["event_sha256"]),
        (receipt.producer_receipt_sha256, event["producer_receipt_sha256"]),
        (receipt.workload_id, profile.workload_id),
        (receipt.consumer_id, profile.raw_consumer_id),
        (receipt.stream_id, profile.workload_id),
        (receipt.desired_state_sha256, authority["desired_state_sha256"]),
        (receipt.authority_sha256, authority["authority_sha256"]),
        (receipt.capture_id, audit["capture_id"]),
        (receipt.capture_manifest_sha256, audit["capture_manifest_sha256"]),
        (receipt.isolation_authority_sha256, isolation["authority_sha256"]),
        (receipt.isolation_limits_sha256, isolation["limits_sha256"]),
    )
    if any(left != right for left, right in checks):
        raise SatelliteTargetFactsError("raw audit typed receipt differs from output")
    evidence.append(
        SatelliteSourceEvidence(
            role="verified_raw_manifest_audit",
            content_sha256=output_sha,
            receipt_sha256=receipt_sha,
            schema_id=OUTPUT_DOCUMENT_SCHEMA,
        )
    )
    ordered = tuple(sorted(evidence, key=lambda item: item.role))
    if len({item.role for item in ordered}) != len(ordered):
        raise SatelliteTargetFactsError("raw evidence roles are ambiguous")
    return _VerifiedRawSnapshot(body, ordered)


@dataclass(frozen=True, slots=True)
class VerifiedRawSatelliteSource:
    """Typed, already durable raw audit proof for one raw-only workload."""

    receipt: RawManifestAuditOutputReceipt

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, RawManifestAuditOutputReceipt):
            raise SatelliteTargetFactsError("receipt has the wrong type")
        profile = _PROFILE_BY_WORKLOAD.get(self.receipt.workload_id)
        if profile is None or profile.raw_consumer_id is None:
            raise SatelliteTargetFactsError("receipt workload is not a raw satellite")
        _verify_raw_audit_receipt(self.receipt, profile)

    @property
    def workload_id(self) -> str:
        return self.receipt.workload_id


@dataclass(frozen=True, slots=True)
class SatelliteTargetContribution:
    contribution_id: str
    content_sha256: str
    process_evidence_sha256: str
    desired_state_sha256: str
    target_id: str
    target_latitude: float
    target_longitude: float
    workload_id: str
    provider_id: str
    product_id: str
    platform_id: str | None
    product_valid_at: float | None
    received_at: float
    quantity_id: str | None
    unit_id: str | None
    rain_rate_mm_h: float | None
    accumulation_mm: float | None
    observation_state: SatelliteObservationState
    coverage_state: SatelliteCoverageState
    claim_status: SatelliteClaimStatus
    current_rain_candidate: bool
    full_scan_bound: bool
    external_truth_bound: bool
    catalog_applicable: bool
    catalog_applicability_reason: str
    catalog_authority_sha256: str
    quality_state: str
    quality_samples: tuple[SatelliteQualitySample, ...]
    sampled_row: int | None
    sampled_column: int | None
    source_content_sha256: str
    source_receipt_sha256: str
    source_process_receipt_sha256: str
    source_evidence: tuple[SatelliteSourceEvidence, ...]

    def __post_init__(self) -> None:
        for value, label in (
            (self.content_sha256, "content_sha256"),
            (self.process_evidence_sha256, "process_evidence_sha256"),
            (self.desired_state_sha256, "desired_state_sha256"),
            (self.catalog_authority_sha256, "catalog_authority_sha256"),
            (self.source_content_sha256, "source_content_sha256"),
            (self.source_receipt_sha256, "source_receipt_sha256"),
            (self.source_process_receipt_sha256, "source_process_receipt_sha256"),
        ):
            _sha256(value, label)
        for value, label in (
            (self.target_id, "target_id"),
            (self.workload_id, "workload_id"),
            (self.provider_id, "provider_id"),
            (self.product_id, "product_id"),
            (self.catalog_applicability_reason, "catalog_applicability_reason"),
        ):
            _safe_id(value, label)
        if self.platform_id is not None:
            _safe_id(self.platform_id, "platform_id")
        if self.quantity_id is not None:
            _safe_id(self.quantity_id, "quantity_id")
        if self.unit_id is not None:
            _safe_id(self.unit_id, "unit_id")
        if self.contribution_id != f"satellite-target-{self.content_sha256}":
            raise SatelliteTargetFactsError("contribution ID is not content-addressed")
        if not -90.0 <= self.target_latitude <= 90.0 or not -180.0 <= self.target_longitude <= 180.0:
            raise SatelliteTargetFactsError("target coordinates are outside WGS84")
        received = _finite(self.received_at, "received_at")
        if self.product_valid_at is not None:
            valid_at = _finite(self.product_valid_at, "product_valid_at")
            if received < valid_at:
                raise SatelliteTargetFactsError("source timestamps are inverted")
        if not isinstance(self.observation_state, SatelliteObservationState):
            raise SatelliteTargetFactsError("observation state is invalid")
        if not isinstance(self.coverage_state, SatelliteCoverageState):
            raise SatelliteTargetFactsError("coverage state is invalid")
        if not isinstance(self.claim_status, SatelliteClaimStatus):
            raise SatelliteTargetFactsError("claim status is invalid")
        for value, label in (
            (self.current_rain_candidate, "current_rain_candidate"),
            (self.full_scan_bound, "full_scan_bound"),
            (self.external_truth_bound, "external_truth_bound"),
            (self.catalog_applicable, "catalog_applicable"),
        ):
            if type(value) is not bool:
                raise SatelliteTargetFactsError(f"{label} must be boolean")
        if self.full_scan_bound or self.external_truth_bound:
            raise SatelliteTargetFactsError("target facts cannot bind scan/truth authority")
        if self.accumulation_mm is not None:
            raise SatelliteTargetFactsError(
                "no closed satellite workload has a target accumulation decoder"
            )
        if self.rain_rate_mm_h is not None and (
            not math.isfinite(self.rain_rate_mm_h) or self.rain_rate_mm_h < 0.0
        ):
            raise SatelliteTargetFactsError("rain rate is invalid")
        profile = _PROFILE_BY_WORKLOAD.get(self.workload_id)
        if (
            profile is None
            or self.provider_id != profile.provider_id
            or self.product_id != profile.product_id
            or self.platform_id != profile.platform_id
            or self.catalog_authority_sha256 != profile.authority_sha256
        ):
            raise SatelliteTargetFactsError("source profile authority differs")
        if (
            not isinstance(self.quality_state, str)
            or not self.quality_state
            or len(self.quality_state) > 256
        ):
            raise SatelliteTargetFactsError("quality state is invalid")
        if (
            not isinstance(self.quality_samples, tuple)
            or any(not isinstance(item, SatelliteQualitySample) for item in self.quality_samples)
            or len(self.quality_samples) > 16
        ):
            raise SatelliteTargetFactsError("quality samples are invalid")
        planes = tuple(item.plane for item in self.quality_samples)
        if planes != tuple(sorted(planes)) or len(set(planes)) != len(planes):
            raise SatelliteTargetFactsError("quality samples are not canonical")
        if (
            not isinstance(self.source_evidence, tuple)
            or not self.source_evidence
            or len(self.source_evidence) > MAX_RAW_EVIDENCE_ROLES + 2
            or any(not isinstance(item, SatelliteSourceEvidence) for item in self.source_evidence)
        ):
            raise SatelliteTargetFactsError("source evidence is invalid")
        evidence_order = tuple(item.role for item in self.source_evidence)
        if evidence_order != tuple(sorted(evidence_order)) or len(set(evidence_order)) != len(evidence_order):
            raise SatelliteTargetFactsError("source evidence is not sorted/unique")
        has_pixel = self.sampled_row is not None or self.sampled_column is not None
        if has_pixel and (
            isinstance(self.sampled_row, bool)
            or not isinstance(self.sampled_row, int)
            or self.sampled_row < 0
            or isinstance(self.sampled_column, bool)
            or not isinstance(self.sampled_column, int)
            or self.sampled_column < 0
        ):
            raise SatelliteTargetFactsError("sampled pixel is invalid")
        self._validate_state(has_pixel)
        if _digest(self._body()) != self.content_sha256:
            raise SatelliteTargetFactsError("contribution content address differs")

    def _validate_state(self, has_pixel: bool) -> None:
        if self.observation_state is SatelliteObservationState.TARGET_UNAVAILABLE:
            if self.catalog_applicable or self.catalog_applicability_reason != "target_unavailable":
                raise SatelliteTargetFactsError("unavailable applicability is inconsistent")
        elif self.observation_state is SatelliteObservationState.OUTSIDE_CATALOG_APPLICABILITY:
            if self.catalog_applicable or self.catalog_applicability_reason != "catalog_bbox_mismatch":
                raise SatelliteTargetFactsError("outside applicability is inconsistent")
        elif not self.catalog_applicable:
            raise SatelliteTargetFactsError("in-scope state lacks catalog applicability")
        positive = self.observation_state is SatelliteObservationState.POSITIVE_RAIN_CANDIDATE
        if positive != self.current_rain_candidate:
            raise SatelliteTargetFactsError("positive flag differs from state")
        if positive:
            if (
                self.workload_id != "satellite.noaa.err"
                or self.claim_status is not SatelliteClaimStatus.POSITIVE_CANDIDATE_ONLY
                or self.coverage_state is not SatelliteCoverageState.IN_NATIVE_DOMAIN_VALID
                or self.rain_rate_mm_h is None
                or self.rain_rate_mm_h <= 0.0
                or not has_pixel
                or self.quantity_id != "instantaneous_precipitation_rate"
                or self.unit_id != "mm/h"
                or not self.catalog_applicable
            ):
                raise SatelliteTargetFactsError("positive satellite fact is inconsistent")
            return
        if self.observation_state is SatelliteObservationState.DRY_WITHHELD:
            if (
                self.workload_id != "satellite.noaa.err"
                or self.claim_status is not SatelliteClaimStatus.NEGATIVE_WITHHELD
                or self.coverage_state not in {
                    SatelliteCoverageState.IN_NATIVE_DOMAIN_VALID,
                    SatelliteCoverageState.IN_NATIVE_DOMAIN_UNDETECT,
                }
                or not has_pixel
                or not self.catalog_applicable
            ):
                raise SatelliteTargetFactsError("dry-withheld fact is inconsistent")
            return
        if self.rain_rate_mm_h is not None:
            raise SatelliteTargetFactsError("non-quantitative state invented rain rate")
        expected = {
            SatelliteObservationState.NODATA_WITHHELD: (
                SatelliteCoverageState.IN_NATIVE_DOMAIN_NODATA,
                SatelliteClaimStatus.NO_CLAIM,
                True,
            ),
            SatelliteObservationState.QUANTITATIVE_DECODER_WITHHELD: (
                SatelliteCoverageState.CATALOG_APPLICABLE_DECODER_WITHHELD,
                SatelliteClaimStatus.QUANTITATIVE_WITHHELD,
                False,
            ),
            SatelliteObservationState.OUTSIDE_CATALOG_APPLICABILITY: (
                SatelliteCoverageState.OUTSIDE_CATALOG_APPLICABILITY,
                SatelliteClaimStatus.NO_CLAIM,
                False,
            ),
            SatelliteObservationState.OUTSIDE_NATIVE_DOMAIN: (
                SatelliteCoverageState.OUTSIDE_NATIVE_DOMAIN,
                SatelliteClaimStatus.NO_CLAIM,
                False,
            ),
            SatelliteObservationState.TARGET_UNAVAILABLE: (
                SatelliteCoverageState.TARGET_UNAVAILABLE,
                SatelliteClaimStatus.NO_CLAIM,
                False,
            ),
        }.get(self.observation_state)
        if expected != (self.coverage_state, self.claim_status, has_pixel):
            raise SatelliteTargetFactsError("non-quantitative state is inconsistent")

    def _body(self) -> dict[str, Any]:
        return {
            "applicability": {
                "applicable": self.catalog_applicable,
                "catalog_authority_sha256": self.catalog_authority_sha256,
                "reason": self.catalog_applicability_reason,
            },
            "contract": SATELLITE_TARGET_CONTRIBUTION_CONTRACT,
            "desired_state_sha256": self.desired_state_sha256,
            "observation": {
                "accumulation_mm": self.accumulation_mm,
                "claim_status": self.claim_status.value,
                "coverage_state": self.coverage_state.value,
                "current_rain_candidate": self.current_rain_candidate,
                "external_truth_bound": self.external_truth_bound,
                "full_scan_bound": self.full_scan_bound,
                "quality_samples": [item.as_dict() for item in self.quality_samples],
                "quality_state": self.quality_state,
                "quantity_id": self.quantity_id,
                "rain_rate_mm_h": self.rain_rate_mm_h,
                "state": self.observation_state.value,
                "unit_id": self.unit_id,
            },
            "provenance": {
                "process_evidence_sha256": self.process_evidence_sha256,
                "source_content_sha256": self.source_content_sha256,
                "source_evidence": [item.as_dict() for item in self.source_evidence],
                "source_process_receipt_sha256": self.source_process_receipt_sha256,
                "source_receipt_sha256": self.source_receipt_sha256,
            },
            "sample": {"column": self.sampled_column, "row": self.sampled_row},
            "source": {
                "platform_id": self.platform_id,
                "product_id": self.product_id,
                "product_valid_at": self.product_valid_at,
                "provider_id": self.provider_id,
                "received_at": self.received_at,
                "workload_id": self.workload_id,
            },
            "target": {
                "latitude": self.target_latitude,
                "longitude": self.target_longitude,
                "target_id": self.target_id,
            },
        }

    def as_dict(self) -> dict[str, Any]:
        result = self._body()
        result.update(
            {
                "content_sha256": self.content_sha256,
                "contribution_id": self.contribution_id,
            }
        )
        return result


@dataclass(frozen=True, slots=True)
class SatelliteCycleEvidenceReference:
    """Field-for-field compatible projection for cycle ``EvidenceReference``."""

    role: str
    content_sha256: str
    receipt_sha256: str
    size_bytes: int
    schema_id: str

    def __post_init__(self) -> None:
        _safe_id(self.role, "cycle evidence role")
        _sha256(self.content_sha256, "cycle evidence content")
        _sha256(self.receipt_sha256, "cycle evidence receipt")
        _safe_id(self.schema_id, "cycle evidence schema")
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or not 1 <= self.size_bytes <= 512 * 1024 * 1024
        ):
            raise SatelliteTargetFactsError("cycle evidence byte size is invalid")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "content_sha256": self.content_sha256,
            "receipt_sha256": self.receipt_sha256,
            "role": self.role,
            "schema_id": self.schema_id,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class SatelliteTargetContributionBatch:
    batch_id: str
    content_sha256: str
    process_receipt_sha256: str
    desired_state_sha256: str
    source_count: int
    target_count: int
    contributions: tuple[SatelliteTargetContribution, ...]

    def __post_init__(self) -> None:
        for value, label in (
            (self.content_sha256, "batch content"),
            (self.process_receipt_sha256, "batch process receipt"),
            (self.desired_state_sha256, "batch desired state"),
        ):
            _sha256(value, label)
        if self.batch_id != f"satellite-target-batch-{self.content_sha256}":
            raise SatelliteTargetFactsError("batch ID is not content-addressed")
        if self.source_count != MAX_SATELLITE_SOURCES:
            raise SatelliteTargetFactsError("batch does not contain all four sources")
        if (
            isinstance(self.target_count, bool)
            or not isinstance(self.target_count, int)
            or not 1 <= self.target_count <= MAX_SATELLITE_TARGETS
        ):
            raise SatelliteTargetFactsError("batch target count is invalid")
        if (
            not isinstance(self.contributions, tuple)
            or len(self.contributions) != self.source_count * self.target_count
            or any(
                not isinstance(item, SatelliteTargetContribution)
                or item.desired_state_sha256 != self.desired_state_sha256
                for item in self.contributions
            )
        ):
            raise SatelliteTargetFactsError("batch contribution matrix is invalid")
        order = tuple((item.workload_id, item.target_id) for item in self.contributions)
        if order != tuple(sorted(order)) or len(set(order)) != len(order):
            raise SatelliteTargetFactsError("batch matrix is not sorted and unique")
        if frozenset(item.workload_id for item in self.contributions) != _EXPECTED_WORKLOADS:
            raise SatelliteTargetFactsError("batch workload set is not exact")
        if len({item.target_id for item in self.contributions}) != self.target_count:
            raise SatelliteTargetFactsError("batch target set is incomplete")
        if _digest(self._body()) != self.content_sha256:
            raise SatelliteTargetFactsError("batch content address differs")
        if _digest(self._process_receipt_body()) != self.process_receipt_sha256:
            raise SatelliteTargetFactsError("batch process receipt differs")

    @property
    def canonical_bytes(self) -> bytes:
        """Exact body bytes identified by ``content_sha256`` (not the envelope)."""

        return _canonical_json(self._body())

    @property
    def size_bytes(self) -> int:
        return len(self.canonical_bytes)

    @property
    def sample_count(self) -> int:
        return len(self.contributions)

    @property
    def valid_sample_count(self) -> int:
        return sum(item.rain_rate_mm_h is not None for item in self.contributions)

    @property
    def positive_candidate_count(self) -> int:
        return sum(item.current_rain_candidate for item in self.contributions)

    @property
    def withheld_count(self) -> int:
        return sum(
            item.observation_state
            is SatelliteObservationState.QUANTITATIVE_DECODER_WITHHELD
            for item in self.contributions
        )

    def _body(self) -> dict[str, Any]:
        return {
            "contract": SATELLITE_TARGET_BATCH_CONTRACT,
            "contributions": [item.as_dict() for item in self.contributions],
            "desired_state_sha256": self.desired_state_sha256,
            "source_count": self.source_count,
            "target_count": self.target_count,
        }

    def _process_receipt_body(self) -> dict[str, Any]:
        source_authorities: dict[str, dict[str, str]] = {}
        processes = []
        for item in self.contributions:
            authority = {
                "catalog_authority_sha256": item.catalog_authority_sha256,
                "source_content_sha256": item.source_content_sha256,
                "source_process_receipt_sha256": item.source_process_receipt_sha256,
                "source_receipt_sha256": item.source_receipt_sha256,
            }
            previous = source_authorities.setdefault(item.workload_id, authority)
            if previous != authority:
                raise SatelliteTargetFactsError(
                    "one workload has inconsistent source authority"
                )
            processes.append(
                {
                    "process_evidence_sha256": item.process_evidence_sha256,
                    "target_id": item.target_id,
                    "workload_id": item.workload_id,
                }
            )
        return {
            "batch_content_sha256": self.content_sha256,
            "batch_contract": SATELLITE_TARGET_BATCH_CONTRACT,
            "contract": SATELLITE_TARGET_BATCH_PROCESS_RECEIPT_CONTRACT,
            "contribution_process_evidence": processes,
            "desired_state_sha256": self.desired_state_sha256,
            "source_authorities": [
                {"workload_id": workload_id, **authority}
                for workload_id, authority in sorted(source_authorities.items())
            ],
        }

    def to_cycle_evidence_reference(self) -> SatelliteCycleEvidenceReference:
        return SatelliteCycleEvidenceReference(
            role=SATELLITE_CYCLE_EVIDENCE_ROLE,
            content_sha256=self.content_sha256,
            receipt_sha256=self.process_receipt_sha256,
            size_bytes=self.size_bytes,
            schema_id=SATELLITE_TARGET_BATCH_CONTRACT,
        )

    def as_dict(self) -> dict[str, Any]:
        result = self._body()
        result.update(
            {
                "batch_id": self.batch_id,
                "content_sha256": self.content_sha256,
                "positive_candidate_count": self.positive_candidate_count,
                "process_receipt_sha256": self.process_receipt_sha256,
                "sample_count": self.sample_count,
                "valid_sample_count": self.valid_sample_count,
                "withheld_count": self.withheld_count,
            }
        )
        return result


def _catalog_applicability(
    profile: SatelliteProviderProfile,
    target: Target,
) -> tuple[bool, str]:
    if not target.available:
        return False, "target_unavailable"
    if profile.coverage_bbox is None:
        return True, "catalog_global"
    south, west, north, east = profile.coverage_bbox
    if south <= target.latitude <= north and west <= target.longitude <= east:
        return True, "catalog_bbox_match"
    return False, "catalog_bbox_mismatch"


def _process_evidence_sha256(
    *,
    desired_state_sha256: str,
    target: Target,
    profile: SatelliteProviderProfile,
    source_content_sha256: str,
    source_receipt_sha256: str,
    source_process_receipt_sha256: str,
    target_sample_process_sha256: str | None,
    state: SatelliteObservationState,
) -> str:
    return _digest(
        {
            "catalog_authority_sha256": profile.authority_sha256,
            "contract": "stv3-multi-source-satellite-target-process-evidence-v1",
            "desired_state_sha256": desired_state_sha256,
            "observation_state": state.value,
            "source_content_sha256": source_content_sha256,
            "source_process_receipt_sha256": source_process_receipt_sha256,
            "source_receipt_sha256": source_receipt_sha256,
            "target_sample_process_sha256": target_sample_process_sha256,
            "target": {
                "available": target.available,
                "latitude": target.latitude,
                "longitude": target.longitude,
                "target_id": target.target_id,
            },
            "workload_id": profile.workload_id,
        }
    )


def _finish_contribution(values: dict[str, Any]) -> SatelliteTargetContribution:
    body_probe = SatelliteTargetContribution.__new__(SatelliteTargetContribution)
    for field_name, value in values.items():
        object.__setattr__(body_probe, field_name, value)
    content_sha = _digest(body_probe._body())
    values["content_sha256"] = content_sha
    values["contribution_id"] = f"satellite-target-{content_sha}"
    return SatelliteTargetContribution(**values)


def _raw_contribution(
    desired_state: DesiredState,
    target: Target,
    source: VerifiedRawSatelliteSource,
    profile: SatelliteProviderProfile,
) -> SatelliteTargetContribution:
    snapshot = _verify_raw_audit_receipt(source.receipt, profile)
    applicable, applicability_reason = _catalog_applicability(profile, target)
    if not target.available:
        state = SatelliteObservationState.TARGET_UNAVAILABLE
        coverage = SatelliteCoverageState.TARGET_UNAVAILABLE
        claim = SatelliteClaimStatus.NO_CLAIM
    elif not applicable:
        state = SatelliteObservationState.OUTSIDE_CATALOG_APPLICABILITY
        coverage = SatelliteCoverageState.OUTSIDE_CATALOG_APPLICABILITY
        claim = SatelliteClaimStatus.NO_CLAIM
    else:
        state = SatelliteObservationState.QUANTITATIVE_DECODER_WITHHELD
        coverage = SatelliteCoverageState.CATALOG_APPLICABLE_DECODER_WITHHELD
        claim = SatelliteClaimStatus.QUANTITATIVE_WITHHELD
    process_sha = _process_evidence_sha256(
        desired_state_sha256=desired_state.state_sha256,
        target=target,
        profile=profile,
        source_content_sha256=source.receipt.output_sha256,
        source_receipt_sha256=source.receipt.receipt_sha256,
        source_process_receipt_sha256=source.receipt.process_receipt_sha256,
        target_sample_process_sha256=None,
        state=state,
    )
    return _finish_contribution(
        {
            "process_evidence_sha256": process_sha,
            "desired_state_sha256": desired_state.state_sha256,
            "target_id": target.target_id,
            "target_latitude": target.latitude,
            "target_longitude": target.longitude,
            "workload_id": profile.workload_id,
            "provider_id": profile.provider_id,
            "product_id": profile.product_id,
            "platform_id": profile.platform_id,
            "product_valid_at": None,
            "received_at": float(source.receipt.created_at),
            "quantity_id": None,
            "unit_id": None,
            "rain_rate_mm_h": None,
            "accumulation_mm": None,
            "observation_state": state,
            "coverage_state": coverage,
            "claim_status": claim,
            "current_rain_candidate": False,
            "full_scan_bound": False,
            "external_truth_bound": False,
            "catalog_applicable": applicable,
            "catalog_applicability_reason": applicability_reason,
            "catalog_authority_sha256": profile.authority_sha256,
            "quality_state": "withheld_no_pixel_or_coordinate_values_read",
            "quality_samples": (),
            "sampled_row": None,
            "sampled_column": None,
            "source_content_sha256": source.receipt.output_sha256,
            "source_receipt_sha256": source.receipt.receipt_sha256,
            "source_process_receipt_sha256": source.receipt.process_receipt_sha256,
            "source_evidence": snapshot.evidence,
        }
    )


def _noaa_state(
    radar: RadarTargetContribution,
) -> tuple[SatelliteObservationState, SatelliteCoverageState, SatelliteClaimStatus]:
    states = {
        RadarObservationState.POSITIVE_RAIN_CANDIDATE: (
            SatelliteObservationState.POSITIVE_RAIN_CANDIDATE,
            SatelliteCoverageState.IN_NATIVE_DOMAIN_VALID,
            SatelliteClaimStatus.POSITIVE_CANDIDATE_ONLY,
        ),
        RadarObservationState.DRY_WITHHELD: (
            SatelliteObservationState.DRY_WITHHELD,
            (
                SatelliteCoverageState.IN_NATIVE_DOMAIN_UNDETECT
                if radar.coverage_state is RadarCoverageState.IN_DOMAIN_UNDETECT
                else SatelliteCoverageState.IN_NATIVE_DOMAIN_VALID
            ),
            SatelliteClaimStatus.NEGATIVE_WITHHELD,
        ),
        RadarObservationState.NODATA_WITHHELD: (
            SatelliteObservationState.NODATA_WITHHELD,
            SatelliteCoverageState.IN_NATIVE_DOMAIN_NODATA,
            SatelliteClaimStatus.NO_CLAIM,
        ),
        RadarObservationState.OUTSIDE_DOMAIN: (
            SatelliteObservationState.OUTSIDE_NATIVE_DOMAIN,
            SatelliteCoverageState.OUTSIDE_NATIVE_DOMAIN,
            SatelliteClaimStatus.NO_CLAIM,
        ),
        RadarObservationState.TARGET_UNAVAILABLE: (
            SatelliteObservationState.TARGET_UNAVAILABLE,
            SatelliteCoverageState.TARGET_UNAVAILABLE,
            SatelliteClaimStatus.NO_CLAIM,
        ),
    }
    result = states.get(radar.observation_state)
    expected_claim = {
        RadarObservationState.POSITIVE_RAIN_CANDIDATE: RadarClaimStatus.POSITIVE_CANDIDATE_ONLY,
        RadarObservationState.DRY_WITHHELD: RadarClaimStatus.NEGATIVE_WITHHELD,
        RadarObservationState.NODATA_WITHHELD: RadarClaimStatus.NO_CLAIM,
        RadarObservationState.OUTSIDE_DOMAIN: RadarClaimStatus.NO_CLAIM,
        RadarObservationState.TARGET_UNAVAILABLE: RadarClaimStatus.NO_CLAIM,
    }.get(radar.observation_state)
    if result is None or radar.claim_status is not expected_claim:
        raise SatelliteTargetFactsError("NOAA radar target state cannot be projected")
    return result


def _noaa_contribution(
    desired_state: DesiredState,
    target: Target,
    source: VerifiedRadarFieldSource,
    radar: RadarTargetContribution,
    source_process_receipt_sha256: str,
    profile: SatelliteProviderProfile,
) -> SatelliteTargetContribution:
    if radar.workload_id != profile.workload_id or radar.target_id != target.target_id:
        raise SatelliteTargetFactsError("NOAA target sampler returned another identity")
    applicable, applicability_reason = _catalog_applicability(profile, target)
    state, coverage, claim = _noaa_state(radar)
    evidence = tuple(
        sorted(
            (
                SatelliteSourceEvidence(
                    role="canonical_noaa_err_field",
                    content_sha256=radar.candidate_content_sha256,
                    receipt_sha256=radar.canonical_receipt_sha256,
                    schema_id=RADAR_FIELD_BRIDGE_ALGORITHM,
                ),
                SatelliteSourceEvidence(
                    role="canonical_noaa_err_target_sample",
                    content_sha256=radar.content_sha256,
                    receipt_sha256=radar.process_evidence_sha256,
                    schema_id=RADAR_TARGET_CONTRIBUTION_CONTRACT,
                ),
            ),
            key=lambda item: item.role,
        )
    )
    process_sha = _process_evidence_sha256(
        desired_state_sha256=desired_state.state_sha256,
        target=target,
        profile=profile,
        source_content_sha256=radar.candidate_content_sha256,
        source_receipt_sha256=radar.canonical_receipt_sha256,
        source_process_receipt_sha256=source_process_receipt_sha256,
        target_sample_process_sha256=radar.process_evidence_sha256,
        state=state,
    )
    return _finish_contribution(
        {
            "process_evidence_sha256": process_sha,
            "desired_state_sha256": desired_state.state_sha256,
            "target_id": target.target_id,
            "target_latitude": target.latitude,
            "target_longitude": target.longitude,
            "workload_id": profile.workload_id,
            "provider_id": profile.provider_id,
            "product_id": profile.product_id,
            "platform_id": profile.platform_id,
            "product_valid_at": float(radar.product_valid_at),
            "received_at": float(radar.received_at),
            "quantity_id": source.field.output_quantity,
            "unit_id": source.field.output_unit,
            "rain_rate_mm_h": radar.rain_rate_mm_h,
            "accumulation_mm": None,
            "observation_state": state,
            "coverage_state": coverage,
            "claim_status": claim,
            "current_rain_candidate": radar.current_rain_candidate,
            "full_scan_bound": False,
            "external_truth_bound": False,
            "catalog_applicable": applicable,
            "catalog_applicability_reason": applicability_reason,
            "catalog_authority_sha256": profile.authority_sha256,
            "quality_state": radar.quality_state,
            "quality_samples": tuple(
                SatelliteQualitySample(item.plane, item.semantics, item.value)
                for item in radar.qc_samples
            ),
            "sampled_row": radar.sampled_row,
            "sampled_column": radar.sampled_column,
            "source_content_sha256": radar.candidate_content_sha256,
            "source_receipt_sha256": radar.canonical_receipt_sha256,
            "source_process_receipt_sha256": source_process_receipt_sha256,
            "source_evidence": evidence,
        }
    )


def build_satellite_target_contribution_batch(
    desired_state: DesiredState,
    sources: Sequence[VerifiedRawSatelliteSource | VerifiedRadarFieldSource],
) -> SatelliteTargetContributionBatch:
    """Build the exact four-source Cartesian satellite target matrix."""

    if not isinstance(desired_state, DesiredState):
        raise SatelliteTargetFactsError("desired_state must be parsed and canonical")
    if (
        hashlib.sha256(desired_state.canonical_bytes).hexdigest()
        != desired_state.state_sha256
    ):
        raise SatelliteTargetFactsError("desired-state content address differs")
    if not 1 <= len(desired_state.targets) <= MAX_SATELLITE_TARGETS:
        raise SatelliteTargetFactsError("desired-state target count is outside the bound")
    target_ids = tuple(target.target_id for target in desired_state.targets)
    if target_ids != tuple(sorted(set(target_ids))):
        raise SatelliteTargetFactsError("desired-state targets are not canonical")
    if not isinstance(sources, Sequence) or isinstance(sources, (str, bytes, bytearray)):
        raise SatelliteTargetFactsError("sources must be a bounded sequence")
    selected = tuple(sources)
    if len(selected) != MAX_SATELLITE_SOURCES:
        raise SatelliteTargetFactsError("all four satellite sources are required")
    raw_sources: dict[str, VerifiedRawSatelliteSource] = {}
    noaa_source: VerifiedRadarFieldSource | None = None
    for source in selected:
        if isinstance(source, VerifiedRawSatelliteSource):
            if source.workload_id in raw_sources:
                raise SatelliteTargetFactsError("duplicate raw satellite workload")
            raw_sources[source.workload_id] = source
        elif isinstance(source, VerifiedRadarFieldSource):
            if noaa_source is not None:
                raise SatelliteTargetFactsError("duplicate quantitative satellite workload")
            if source.field.semantics_certificate_id != NOAA_ERR_SEMANTICS_CERTIFICATE_ID:
                raise SatelliteTargetFactsError("quantitative source is not NOAA ERR")
            noaa_source = source
        else:
            raise SatelliteTargetFactsError("satellite source has an unsupported type")
    if frozenset(raw_sources) != _RAW_WORKLOADS or noaa_source is None:
        raise SatelliteTargetFactsError("satellite source allowlist is incomplete")
    for workload_id, source in raw_sources.items():
        snapshot = _verify_raw_audit_receipt(source.receipt, _PROFILE_BY_WORKLOAD[workload_id])
        if source.receipt.desired_state_sha256 != desired_state.state_sha256 or not snapshot.canonical_bytes:
            raise SatelliteTargetFactsError("raw audit desired-state authority differs")
    radar_batch = build_radar_target_contribution_batch(desired_state, (noaa_source,))
    if radar_batch.source_count != 1 or radar_batch.target_count != len(desired_state.targets):
        raise SatelliteTargetFactsError("NOAA target sampler matrix differs")
    radar_by_target = {item.target_id: item for item in radar_batch.contributions}
    if frozenset(radar_by_target) != frozenset(target_ids):
        raise SatelliteTargetFactsError("NOAA target sampler omitted a target")
    contributions: list[SatelliteTargetContribution] = []
    for profile in SATELLITE_PROVIDER_PROFILES:
        for target in desired_state.targets:
            if profile.workload_id == "satellite.noaa.err":
                contributions.append(
                    _noaa_contribution(
                        desired_state,
                        target,
                        noaa_source,
                        radar_by_target[target.target_id],
                        radar_batch.process_receipt_sha256,
                        profile,
                    )
                )
            else:
                contributions.append(
                    _raw_contribution(
                        desired_state,
                        target,
                        raw_sources[profile.workload_id],
                        profile,
                    )
                )
    ordered = tuple(sorted(contributions, key=lambda item: (item.workload_id, item.target_id)))
    body = {
        "contract": SATELLITE_TARGET_BATCH_CONTRACT,
        "contributions": [item.as_dict() for item in ordered],
        "desired_state_sha256": desired_state.state_sha256,
        "source_count": MAX_SATELLITE_SOURCES,
        "target_count": len(desired_state.targets),
    }
    content_sha = _digest(body)
    provisional = SatelliteTargetContributionBatch.__new__(SatelliteTargetContributionBatch)
    for field_name, value in (
        ("batch_id", f"satellite-target-batch-{content_sha}"),
        ("content_sha256", content_sha),
        ("process_receipt_sha256", "0" * 64),
        ("desired_state_sha256", desired_state.state_sha256),
        ("source_count", MAX_SATELLITE_SOURCES),
        ("target_count", len(desired_state.targets)),
        ("contributions", ordered),
    ):
        object.__setattr__(provisional, field_name, value)
    process_sha = _digest(provisional._process_receipt_body())
    result = SatelliteTargetContributionBatch(
        batch_id=f"satellite-target-batch-{content_sha}",
        content_sha256=content_sha,
        process_receipt_sha256=process_sha,
        desired_state_sha256=desired_state.state_sha256,
        source_count=MAX_SATELLITE_SOURCES,
        target_count=len(desired_state.targets),
        contributions=ordered,
    )
    for workload_id, source in raw_sources.items():
        _verify_raw_audit_receipt(source.receipt, _PROFILE_BY_WORKLOAD[workload_id])
    return result


__all__ = [
    "ACQUISITION_CATALOG_AUTHORITY_SHA256",
    "MAX_SATELLITE_SOURCES",
    "MAX_SATELLITE_TARGETS",
    "MAX_RAW_EVIDENCE_ROLES",
    "NOAA_ERR_SEMANTICS_CERTIFICATE_ID",
    "SATELLITE_CYCLE_EVIDENCE_ROLE",
    "SATELLITE_PROVIDER_PROFILES",
    "SATELLITE_TARGET_BATCH_CONTRACT",
    "SATELLITE_TARGET_BATCH_PROCESS_RECEIPT_CONTRACT",
    "SATELLITE_TARGET_CONTRIBUTION_CONTRACT",
    "SatelliteClaimStatus",
    "SatelliteCoverageState",
    "SatelliteCycleEvidenceReference",
    "SatelliteObservationState",
    "SatelliteProviderProfile",
    "SatelliteQualitySample",
    "SatelliteSourceEvidence",
    "SatelliteTargetContribution",
    "SatelliteTargetContributionBatch",
    "SatelliteTargetFactsError",
    "VerifiedRawSatelliteSource",
    "build_satellite_target_contribution_batch",
]
