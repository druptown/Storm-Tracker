"""Pure target facts for four certified quantitative radar-like fields.

The canonical radar bridge already reopens raw evidence and provider artifacts,
checks their content addresses, and binds them to a closed semantics
certificate.  This module is the next deliberately small boundary: it samples
explicit :class:`~storm_tracker_worker.desired_state.Target` positions without
network, storage, scheduling, fusion, forecast or operational admission.  DWD,
NOAA and Met Office have an exact pixel-edge authority and can be sampled.  KNMI
RTCOR remains present but target sampling is withheld by its existing grid gate.

A strictly positive valid pixel may become a ``current_rain`` *candidate*.
Zero/undetect is never allowed to mean operationally dry here: a negative claim
remains withheld until a later authority binds a complete spatial scan and
independent external truth.  Nodata, outside-domain and unavailable targets are
retained as first-class contributions instead of silently disappearing.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import re
from typing import Any, Sequence

import numpy as np
from pyproj import Transformer
from pyproj.exceptions import CRSError, ProjError

from .analysis_grid_transform import (
    AffineGridTransformCertificate,
    TransformEvidenceClass,
    native_candidate_content_sha256,
)
from .canonical_radar_field_bridge import (
    AnalysisGridMapping,
    NativeRainRateFieldCandidate,
    RADAR_FIELD_ADMISSION_STATUS,
    RADAR_FIELD_BRIDGE_ALGORITHM,
    RadarFieldSemanticsCertificate,
    default_radar_semantics_certificates,
)
from .desired_state import DesiredState, Target


RADAR_TARGET_CONTRIBUTION_CONTRACT = (
    "stv3-multi-source-radar-target-contribution-v1"
)
RADAR_TARGET_BATCH_CONTRACT = "stv3-multi-source-radar-target-batch-v1"
RADAR_TARGET_BATCH_PROCESS_RECEIPT_CONTRACT = (
    "stv3-multi-source-radar-target-batch-process-receipt-v1"
)
RADAR_TARGET_SAMPLING_METHOD = (
    "wgs84_to_certified_native_crs_containing_pixel_half_open_v1"
)
RADAR_TARGET_NO_SAMPLE_METHOD = "explicit_no_target_pixel_fail_closed_v1"
MAX_RADAR_SOURCES = 4
MAX_QC_SAMPLES = 16

_DWD_GRID_CERTIFICATE_PREFIX = "dwd.rv.de1200.pixel-edges.v1:"
_DWD_GRID_ADAPTER_ID = "dwd.rv.de1200.native-affine"
_DWD_GRID_ADAPTER_VERSION = "v1"
_KNMI_GRID_GATE_SCHEMA = "stv3-knmi-rtcor-analysis-grid-authority-gate-v1"
_KNMI_GRID_BLOCKERS = (
    "array_display_origin_not_decoder_bound",
    "hdf36_payload_nl25_profile_version_gap",
    "kilometre_to_metre_crs_equivalence_not_certified",
    "official_crs_authorities_conflict",
    "official_live_hdf_grid_evidence_not_retained",
    "pixel_lu_edge_semantics_not_formally_defined",
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_CANDIDATE_ID_RE = re.compile(r"^radar-field-[0-9a-f]{64}$")


class RadarTargetFactsError(ValueError):
    """The source field, target set or evidence binding is not trustworthy."""


class RadarObservationState(str, Enum):
    """Meteorological meaning retained at one desired-state target."""

    POSITIVE_RAIN_CANDIDATE = "positive_rain_candidate"
    DRY_WITHHELD = "dry_withheld"
    NODATA_WITHHELD = "nodata_withheld"
    GRID_SEMANTICS_WITHHELD = "grid_semantics_withheld"
    OUTSIDE_DOMAIN = "outside_domain"
    TARGET_UNAVAILABLE = "target_unavailable"


class RadarCoverageState(str, Enum):
    """Why a target does or does not have a quantitative source value."""

    IN_DOMAIN_VALID = "in_domain_valid"
    IN_DOMAIN_NODATA = "in_domain_nodata"
    IN_DOMAIN_UNDETECT = "in_domain_undetect"
    GRID_SEMANTICS_WITHHELD = "grid_semantics_withheld"
    OUTSIDE_DOMAIN = "outside_domain"
    TARGET_UNAVAILABLE = "target_unavailable"


class RadarClaimStatus(str, Enum):
    """Admission boundary carried into every downstream representation."""

    POSITIVE_CANDIDATE_ONLY = "positive_current_rain_candidate_only"
    NEGATIVE_WITHHELD = (
        "negative_claim_withheld_requires_full_scan_and_external_truth"
    )
    NO_CLAIM = "no_operational_claim_missing_or_outside"


@dataclass(frozen=True, slots=True)
class RadarProviderProfile:
    """Exact certificate-to-workload bridge for the first four sources."""

    certificate_id: str
    workload_id: str
    expected_quality_state: str
    required_qc_planes: tuple[str, ...]
    geometry_policy: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.certificate_id, "certificate_id"),
            (self.workload_id, "workload_id"),
            (self.geometry_policy, "geometry_policy"),
        ):
            if not isinstance(value, str) or _SAFE_ID_RE.fullmatch(value) is None:
                raise RadarTargetFactsError(f"profile {label} is invalid")
        if (
            not isinstance(self.expected_quality_state, str)
            or not self.expected_quality_state
            or len(self.expected_quality_state) > 256
        ):
            raise RadarTargetFactsError("profile quality state is invalid")
        if (
            not isinstance(self.required_qc_planes, tuple)
            or self.required_qc_planes
            != tuple(sorted(self.required_qc_planes))
            or len(set(self.required_qc_planes))
            != len(self.required_qc_planes)
        ):
            raise RadarTargetFactsError("profile QC planes must be sorted/unique")
        for plane in self.required_qc_planes:
            if not isinstance(plane, str) or _SAFE_ID_RE.fullmatch(plane) is None:
                raise RadarTargetFactsError("profile QC plane is invalid")


RADAR_PROVIDER_PROFILES = (
    RadarProviderProfile(
        certificate_id="dwd.rv.accumulation-to-interval-mean-rate.v1",
        workload_id="radar.dwd.radolan_rv",
        expected_quality_state="unavailable:no_native_quality_plane",
        required_qc_planes=(),
        geometry_policy="dwd_official_de1200_edges",
    ),
    RadarProviderProfile(
        certificate_id="knmi.rtcor.accumulation-to-interval-mean-rate.v1",
        workload_id="radar.knmi.rtcor",
        expected_quality_state=(
            "native_quality_index_retained_not_interpreted"
        ),
        required_qc_planes=(
            "quality_index",
            "quality_missing_mask",
            "quality_out_of_image_mask",
            "quality_valid_mask",
        ),
        geometry_policy="knmi_grid_authority_withheld",
    ),
    RadarProviderProfile(
        certificate_id="met-office.odim-rate.v1",
        workload_id="radar.metoffice.uk_rate",
        expected_quality_state="unavailable:odim_quality_dataset_absent",
        required_qc_planes=(),
        geometry_policy="artifact_exact_analysis_grid",
    ),
    RadarProviderProfile(
        certificate_id="noaa.err.instantaneous-rate.v1",
        workload_id="satellite.noaa.err",
        expected_quality_state="native_dqf_flags_retained_not_applied",
        required_qc_planes=(
            "dqf_bad_quality_mask",
            "dqf_degraded_mask",
            "dqf_missing_mask",
            "dqf_raw",
            "quality_optimal_mask",
        ),
        geometry_policy="noaa_exact_regular_wgs84_window",
    ),
)

_PROFILE_BY_CERTIFICATE = {
    profile.certificate_id: profile for profile in RADAR_PROVIDER_PROFILES
}


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _sha256(value: str, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise RadarTargetFactsError(f"{label} is not a lowercase SHA-256")
    return value


def _safe_id(value: str, label: str) -> str:
    if not isinstance(value, str) or _SAFE_ID_RE.fullmatch(value) is None:
        raise RadarTargetFactsError(f"{label} is not a bounded identifier")
    return value


def _finite(value: float, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RadarTargetFactsError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise RadarTargetFactsError(f"{label} must be finite")
    return result


@dataclass(frozen=True, slots=True)
class RadarQcSample:
    """One provider-native QC value, retained without reinterpretation."""

    plane: str
    semantics: str
    value: bool | int | float | None

    def __post_init__(self) -> None:
        _safe_id(self.plane, "QC plane")
        if (
            not isinstance(self.semantics, str)
            or not self.semantics
            or len(self.semantics) > 256
        ):
            raise RadarTargetFactsError("QC semantics are invalid")
        if isinstance(self.value, float) and not math.isfinite(self.value):
            raise RadarTargetFactsError("QC float must be finite or null")
        if self.value is not None and not isinstance(
            self.value, (bool, int, float)
        ):
            raise RadarTargetFactsError("QC value has an unsupported type")

    def as_dict(self) -> dict[str, Any]:
        return {
            "plane": self.plane,
            "semantics": self.semantics,
            "value": self.value,
        }


@dataclass(frozen=True, slots=True)
class VerifiedRadarFieldSource:
    """One canonical field plus its exact worker receipt time."""

    field: NativeRainRateFieldCandidate
    received_at: float
    native_grid_certificate: AffineGridTransformCertificate | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.field, NativeRainRateFieldCandidate):
            raise RadarTargetFactsError(
                "field must be a NativeRainRateFieldCandidate"
            )
        if self.native_grid_certificate is not None and not isinstance(
            self.native_grid_certificate,
            AffineGridTransformCertificate,
        ):
            raise RadarTargetFactsError("native_grid_certificate has a wrong type")
        received = _finite(self.received_at, "received_at")
        if received < 0.0 or received < self.field.valid_at:
            raise RadarTargetFactsError(
                "received_at cannot precede the product valid time"
            )


@dataclass(frozen=True, slots=True)
class _PreparedRadarSource:
    profile: RadarProviderProfile
    certificate: RadarFieldSemanticsCertificate
    source: VerifiedRadarFieldSource
    candidate_content_sha256: str
    geometry_state: str
    geometry_evidence_sha256: str
    affine_grid_certificate: AffineGridTransformCertificate | None = None


@dataclass(frozen=True, slots=True)
class RadarTargetContribution:
    """Content-addressed, provider-neutral evidence for one target."""

    contribution_id: str
    content_sha256: str
    desired_state_sha256: str
    target_id: str
    target_latitude: float
    target_longitude: float
    provider: str
    workload_id: str
    product: str
    product_valid_at: float
    received_at: float
    rain_rate_mm_h: float | None
    observation_state: RadarObservationState
    coverage_state: RadarCoverageState
    claim_status: RadarClaimStatus
    current_rain_candidate: bool
    full_scan_bound: bool
    external_truth_bound: bool
    quality_state: str
    qc_samples: tuple[RadarQcSample, ...]
    geometry_state: str
    geometry_evidence_sha256: str
    sampled_row: int | None
    sampled_column: int | None
    sampling_method: str
    source_evidence_sha256: str
    process_evidence_sha256: str
    candidate_id: str
    candidate_content_sha256: str
    canonical_run_id: str
    canonical_commit_id: str
    canonical_receipt_sha256: str
    canonical_result_sha256: str
    capture_id: str
    capture_receipt_sha256: str
    capture_manifest_sha256: str
    artifact_sha256: str
    artifact_metadata_sha256: str
    decoder_config_sha256: str
    semantics_certificate_id: str
    semantics_certificate_sha256: str
    native_grid_definition_sha256: str
    native_coverage_definition_sha256: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.content_sha256, "content_sha256"),
            (self.desired_state_sha256, "desired_state_sha256"),
            (self.source_evidence_sha256, "source_evidence_sha256"),
            (self.process_evidence_sha256, "process_evidence_sha256"),
            (self.candidate_content_sha256, "candidate_content_sha256"),
            (
                self.geometry_evidence_sha256,
                "geometry_evidence_sha256",
            ),
            (self.canonical_receipt_sha256, "canonical_receipt_sha256"),
            (self.canonical_result_sha256, "canonical_result_sha256"),
            (self.capture_receipt_sha256, "capture_receipt_sha256"),
            (self.capture_manifest_sha256, "capture_manifest_sha256"),
            (self.artifact_sha256, "artifact_sha256"),
            (self.artifact_metadata_sha256, "artifact_metadata_sha256"),
            (self.decoder_config_sha256, "decoder_config_sha256"),
            (
                self.semantics_certificate_sha256,
                "semantics_certificate_sha256",
            ),
            (
                self.native_grid_definition_sha256,
                "native_grid_definition_sha256",
            ),
            (
                self.native_coverage_definition_sha256,
                "native_coverage_definition_sha256",
            ),
        ):
            _sha256(value, label)
        for value, label in (
            (self.target_id, "target_id"),
            (self.provider, "provider"),
            (self.workload_id, "workload_id"),
            (self.product, "product"),
            (self.semantics_certificate_id, "semantics_certificate_id"),
        ):
            _safe_id(value, label)
        if _CANDIDATE_ID_RE.fullmatch(self.candidate_id) is None:
            raise RadarTargetFactsError("candidate_id is invalid")
        if not self.contribution_id == f"radar-target-{self.content_sha256}":
            raise RadarTargetFactsError("contribution_id is not content-addressed")
        latitude = _finite(self.target_latitude, "target_latitude")
        longitude = _finite(self.target_longitude, "target_longitude")
        if not -90.0 <= latitude <= 90.0 or not -180.0 <= longitude <= 180.0:
            raise RadarTargetFactsError("target coordinates are out of range")
        valid_at = _finite(self.product_valid_at, "product_valid_at")
        received = _finite(self.received_at, "received_at")
        if valid_at < 0.0 or received < valid_at:
            raise RadarTargetFactsError("source timestamps are inverted")
        if not isinstance(self.observation_state, RadarObservationState):
            raise RadarTargetFactsError("observation_state is invalid")
        if not isinstance(self.coverage_state, RadarCoverageState):
            raise RadarTargetFactsError("coverage_state is invalid")
        if not isinstance(self.claim_status, RadarClaimStatus):
            raise RadarTargetFactsError("claim_status is invalid")
        if type(self.current_rain_candidate) is not bool:
            raise RadarTargetFactsError("current_rain_candidate must be boolean")
        if self.full_scan_bound is not False or self.external_truth_bound is not False:
            raise RadarTargetFactsError(
                "target sampling cannot bind full-scan or external truth"
            )
        if (
            not isinstance(self.quality_state, str)
            or not self.quality_state
            or len(self.quality_state) > 256
        ):
            raise RadarTargetFactsError("quality_state is invalid")
        if (
            not isinstance(self.geometry_state, str)
            or _SAFE_ID_RE.fullmatch(self.geometry_state) is None
        ):
            raise RadarTargetFactsError("geometry_state is invalid")
        if (
            not isinstance(self.qc_samples, tuple)
            or len(self.qc_samples) > MAX_QC_SAMPLES
            or any(not isinstance(item, RadarQcSample) for item in self.qc_samples)
        ):
            raise RadarTargetFactsError("qc_samples are invalid")
        planes = tuple(item.plane for item in self.qc_samples)
        if planes != tuple(sorted(planes)) or len(set(planes)) != len(planes):
            raise RadarTargetFactsError("qc_samples must be sorted and unique")
        has_pixel = self.sampled_row is not None or self.sampled_column is not None
        if has_pixel and (
            isinstance(self.sampled_row, bool)
            or not isinstance(self.sampled_row, int)
            or self.sampled_row < 0
            or isinstance(self.sampled_column, bool)
            or not isinstance(self.sampled_column, int)
            or self.sampled_column < 0
        ):
            raise RadarTargetFactsError("sample pixel is invalid")
        if self.sampling_method not in {
            RADAR_TARGET_NO_SAMPLE_METHOD,
            RADAR_TARGET_SAMPLING_METHOD,
        }:
            raise RadarTargetFactsError("sampling_method is unsupported")
        no_sample = self.observation_state in {
            RadarObservationState.GRID_SEMANTICS_WITHHELD,
            RadarObservationState.TARGET_UNAVAILABLE,
        }
        expected_method = (
            RADAR_TARGET_NO_SAMPLE_METHOD
            if no_sample
            else RADAR_TARGET_SAMPLING_METHOD
        )
        if self.sampling_method != expected_method:
            raise RadarTargetFactsError("sampling_method differs from target state")
        self._validate_state(has_pixel)
        expected = _digest(self._body())
        if expected != self.content_sha256:
            raise RadarTargetFactsError("contribution content address differs")

    def _validate_state(self, has_pixel: bool) -> None:
        positive = self.observation_state is RadarObservationState.POSITIVE_RAIN_CANDIDATE
        if positive != self.current_rain_candidate:
            raise RadarTargetFactsError("positive candidate flag is inconsistent")
        if positive:
            if (
                self.claim_status is not RadarClaimStatus.POSITIVE_CANDIDATE_ONLY
                or self.coverage_state is not RadarCoverageState.IN_DOMAIN_VALID
                or self.rain_rate_mm_h is None
                or not math.isfinite(self.rain_rate_mm_h)
                or self.rain_rate_mm_h <= 0.0
                or not has_pixel
            ):
                raise RadarTargetFactsError("positive contribution is inconsistent")
            return
        if self.rain_rate_mm_h is not None and (
            not math.isfinite(self.rain_rate_mm_h) or self.rain_rate_mm_h != 0.0
        ):
            raise RadarTargetFactsError("non-positive contribution invented a rate")
        if self.observation_state is RadarObservationState.DRY_WITHHELD:
            if self.claim_status is not RadarClaimStatus.NEGATIVE_WITHHELD or not has_pixel:
                raise RadarTargetFactsError("dry contribution is not fail-closed")
            if self.coverage_state not in {
                RadarCoverageState.IN_DOMAIN_VALID,
                RadarCoverageState.IN_DOMAIN_UNDETECT,
            }:
                raise RadarTargetFactsError("dry coverage state is inconsistent")
            return
        if self.claim_status is not RadarClaimStatus.NO_CLAIM:
            raise RadarTargetFactsError("missing/outside contribution grants a claim")
        if self.rain_rate_mm_h is not None:
            raise RadarTargetFactsError("missing/outside contribution has a rate")
        expected = {
            RadarObservationState.NODATA_WITHHELD: (
                RadarCoverageState.IN_DOMAIN_NODATA,
                True,
            ),
            RadarObservationState.GRID_SEMANTICS_WITHHELD: (
                RadarCoverageState.GRID_SEMANTICS_WITHHELD,
                False,
            ),
            RadarObservationState.OUTSIDE_DOMAIN: (
                RadarCoverageState.OUTSIDE_DOMAIN,
                False,
            ),
            RadarObservationState.TARGET_UNAVAILABLE: (
                RadarCoverageState.TARGET_UNAVAILABLE,
                False,
            ),
        }.get(self.observation_state)
        if expected is None or (self.coverage_state, has_pixel) != expected:
            raise RadarTargetFactsError("missing/outside state is inconsistent")

    def _body(self) -> dict[str, Any]:
        return {
            "contract": RADAR_TARGET_CONTRIBUTION_CONTRACT,
            "desired_state_sha256": self.desired_state_sha256,
            "observation": {
                "claim_status": self.claim_status.value,
                "coverage_state": self.coverage_state.value,
                "current_rain_candidate": self.current_rain_candidate,
                "external_truth_bound": self.external_truth_bound,
                "full_scan_bound": self.full_scan_bound,
                "quality_state": self.quality_state,
                "qc_samples": [item.as_dict() for item in self.qc_samples],
                "rain_rate_mm_h": self.rain_rate_mm_h,
                "state": self.observation_state.value,
            },
            "provenance": {
                "artifact_metadata_sha256": self.artifact_metadata_sha256,
                "artifact_sha256": self.artifact_sha256,
                "candidate_id": self.candidate_id,
                "candidate_content_sha256": self.candidate_content_sha256,
                "canonical_commit_id": self.canonical_commit_id,
                "canonical_receipt_sha256": self.canonical_receipt_sha256,
                "canonical_result_sha256": self.canonical_result_sha256,
                "canonical_run_id": self.canonical_run_id,
                "capture_id": self.capture_id,
                "capture_manifest_sha256": self.capture_manifest_sha256,
                "capture_receipt_sha256": self.capture_receipt_sha256,
                "decoder_config_sha256": self.decoder_config_sha256,
                "geometry_evidence_sha256": self.geometry_evidence_sha256,
                "geometry_state": self.geometry_state,
                "native_coverage_definition_sha256": (
                    self.native_coverage_definition_sha256
                ),
                "native_grid_definition_sha256": (
                    self.native_grid_definition_sha256
                ),
                "process_evidence_sha256": self.process_evidence_sha256,
                "semantics_certificate_id": self.semantics_certificate_id,
                "semantics_certificate_sha256": (
                    self.semantics_certificate_sha256
                ),
                "source_evidence_sha256": self.source_evidence_sha256,
            },
            "sample": {
                "column": self.sampled_column,
                "method": self.sampling_method,
                "row": self.sampled_row,
            },
            "source": {
                "product": self.product,
                "product_valid_at": self.product_valid_at,
                "provider": self.provider,
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
        result["content_sha256"] = self.content_sha256
        result["contribution_id"] = self.contribution_id
        return result


@dataclass(frozen=True, slots=True)
class RadarTargetContributionBatch:
    """Deterministic Cartesian batch of source fields and desired targets."""

    batch_id: str
    content_sha256: str
    process_receipt_sha256: str
    desired_state_sha256: str
    source_count: int
    target_count: int
    contributions: tuple[RadarTargetContribution, ...]

    def __post_init__(self) -> None:
        _sha256(self.content_sha256, "batch content_sha256")
        _sha256(self.process_receipt_sha256, "batch process_receipt_sha256")
        _sha256(self.desired_state_sha256, "batch desired_state_sha256")
        if self.batch_id != f"radar-target-batch-{self.content_sha256}":
            raise RadarTargetFactsError("batch_id is not content-addressed")
        if (
            isinstance(self.source_count, bool)
            or not isinstance(self.source_count, int)
            or not 1 <= self.source_count <= MAX_RADAR_SOURCES
            or isinstance(self.target_count, bool)
            or not isinstance(self.target_count, int)
            or self.target_count < 1
        ):
            raise RadarTargetFactsError("batch dimensions are invalid")
        if (
            not isinstance(self.contributions, tuple)
            or len(self.contributions) != self.source_count * self.target_count
            or any(
                not isinstance(item, RadarTargetContribution)
                or item.desired_state_sha256 != self.desired_state_sha256
                for item in self.contributions
            )
        ):
            raise RadarTargetFactsError("batch contribution matrix is invalid")
        order = tuple(
            (item.workload_id, item.target_id) for item in self.contributions
        )
        if order != tuple(sorted(order)) or len(set(order)) != len(order):
            raise RadarTargetFactsError("batch contributions are not unique/sorted")
        if len({item.workload_id for item in self.contributions}) != self.source_count:
            raise RadarTargetFactsError("batch source count differs")
        if len({item.target_id for item in self.contributions}) != self.target_count:
            raise RadarTargetFactsError("batch target count differs")
        if _digest(self._body()) != self.content_sha256:
            raise RadarTargetFactsError("batch content address differs")
        if _digest(self._process_receipt_body()) != self.process_receipt_sha256:
            raise RadarTargetFactsError("batch process receipt differs")

    @property
    def valid_sample_count(self) -> int:
        return sum(
            item.coverage_state is RadarCoverageState.IN_DOMAIN_VALID
            for item in self.contributions
        )

    @property
    def sample_count(self) -> int:
        return len(self.contributions)

    @property
    def canonical_bytes(self) -> bytes:
        """Exact immutable content bytes identified by ``content_sha256``."""

        return _canonical_json(self._body())

    @property
    def size_bytes(self) -> int:
        return len(self.canonical_bytes)

    @property
    def positive_candidate_count(self) -> int:
        return sum(item.current_rain_candidate for item in self.contributions)

    def _body(self) -> dict[str, Any]:
        return {
            "contract": RADAR_TARGET_BATCH_CONTRACT,
            "contributions": [item.as_dict() for item in self.contributions],
            "desired_state_sha256": self.desired_state_sha256,
            "source_count": self.source_count,
            "target_count": self.target_count,
        }

    def _process_receipt_body(self) -> dict[str, Any]:
        return _batch_process_receipt_body(
            content_sha256=self.content_sha256,
            desired_state_sha256=self.desired_state_sha256,
            contributions=self.contributions,
        )

    def as_dict(self) -> dict[str, Any]:
        result = self._body()
        result.update({
            "batch_id": self.batch_id,
            "content_sha256": self.content_sha256,
            "positive_candidate_count": self.positive_candidate_count,
            "process_receipt_sha256": self.process_receipt_sha256,
            "sample_count": self.sample_count,
            "valid_sample_count": self.valid_sample_count,
        })
        return result


def _batch_process_receipt_body(
    *,
    content_sha256: str,
    desired_state_sha256: str,
    contributions: Sequence[RadarTargetContribution],
) -> dict[str, Any]:
    """Bind one cycle receipt to every source and target process authority."""

    source_authorities: dict[str, dict[str, str]] = {}
    process_evidence = []
    for item in sorted(
        contributions,
        key=lambda contribution: (contribution.workload_id, contribution.target_id),
    ):
        authority = {
            "candidate_content_sha256": item.candidate_content_sha256,
            "candidate_id": item.candidate_id,
            "canonical_receipt_sha256": item.canonical_receipt_sha256,
            "geometry_evidence_sha256": item.geometry_evidence_sha256,
            "semantics_certificate_sha256": item.semantics_certificate_sha256,
            "source_evidence_sha256": item.source_evidence_sha256,
        }
        previous = source_authorities.setdefault(item.workload_id, authority)
        if previous != authority:
            raise RadarTargetFactsError(
                "one workload has inconsistent source candidate authority"
            )
        process_evidence.append({
            "process_evidence_sha256": item.process_evidence_sha256,
            "target_id": item.target_id,
            "workload_id": item.workload_id,
        })
    return {
        "batch_content_sha256": content_sha256,
        "batch_contract": RADAR_TARGET_BATCH_CONTRACT,
        "contract": RADAR_TARGET_BATCH_PROCESS_RECEIPT_CONTRACT,
        "contribution_process_evidence": process_evidence,
        "desired_state_sha256": desired_state_sha256,
        "source_candidate_authorities": [
            {
                "workload_id": workload_id,
                **source_authorities[workload_id],
            }
            for workload_id in sorted(source_authorities)
        ],
    }


def _official_certificates() -> dict[str, RadarFieldSemanticsCertificate]:
    certificates = default_radar_semantics_certificates()
    expected = frozenset(_PROFILE_BY_CERTIFICATE)
    actual = [item.certificate_id for item in certificates]
    if len(actual) != len(set(actual)) or frozenset(actual) != expected:
        raise RadarTargetFactsError(
            "official radar semantics allowlist differs from the exact four-source set"
        )
    return {item.certificate_id: item for item in certificates}


def _candidate_address(field: NativeRainRateFieldCandidate) -> str:
    return "radar-field-" + _digest({
        "algorithm": RADAR_FIELD_BRIDGE_ALGORITHM,
        "artifact_metadata_sha256": field.artifact_metadata_sha256,
        "artifact_sha256": field.artifact_sha256,
        "canonical_receipt_sha256": field.canonical_receipt_sha256,
        "certificate_sha256": field.semantics_certificate_sha256,
    })


def _validate_geometry(
    field: NativeRainRateFieldCandidate,
    certificate: RadarFieldSemanticsCertificate,
) -> None:
    grid = field.native_grid
    coverage = field.native_coverage
    if (
        grid.kind != "regular"
        or grid.axis_order != "yx"
        or isinstance(grid.width, bool)
        or not isinstance(grid.width, int)
        or grid.width < 1
        or isinstance(grid.height, bool)
        or not isinstance(grid.height, int)
        or grid.height < 1
    ):
        raise RadarTargetFactsError("native grid is not a regular y/x field")
    _sha256(grid.definition_sha256, "native grid definition")
    _sha256(coverage.definition_sha256, "native coverage definition")
    bounds = (
        _finite(coverage.minimum_x, "coverage minimum_x"),
        _finite(coverage.minimum_y, "coverage minimum_y"),
        _finite(coverage.maximum_x, "coverage maximum_x"),
        _finite(coverage.maximum_y, "coverage maximum_y"),
    )
    if coverage.kind != "native_bbox" or not (
        bounds[0] < bounds[2] and bounds[1] < bounds[3]
    ):
        raise RadarTargetFactsError("native coverage bbox is invalid")
    if not isinstance(field.native_crs, str) or not field.native_crs.strip():
        raise RadarTargetFactsError("native CRS is absent")
    analysis = field.analysis_grid
    if certificate.analysis_grid_mapping is AnalysisGridMapping.METRIC_BBOX_EDGES:
        if analysis is None:
            raise RadarTargetFactsError("certificate requires an exact analysis grid")
        if (
            analysis.crs != field.native_crs
            or analysis.width != grid.width
            or analysis.height != grid.height
            or analysis.extent_edges_m != bounds
        ):
            raise RadarTargetFactsError(
                "analysis grid differs from the certified native geometry"
            )
    elif analysis is not None:
        raise RadarTargetFactsError(
            "native-only certificate unexpectedly exposes an analysis grid"
        )


def _validate_candidate(
    field: NativeRainRateFieldCandidate,
    certificates: dict[str, RadarFieldSemanticsCertificate],
) -> tuple[RadarProviderProfile, RadarFieldSemanticsCertificate, str]:
    if field.admission_status != RADAR_FIELD_ADMISSION_STATUS:
        raise RadarTargetFactsError("source field is not an unadmitted candidate")
    if field.algorithm_version != RADAR_FIELD_BRIDGE_ALGORITHM:
        raise RadarTargetFactsError("source field uses an unknown bridge algorithm")
    profile = _PROFILE_BY_CERTIFICATE.get(field.semantics_certificate_id)
    certificate = certificates.get(field.semantics_certificate_id)
    if profile is None or certificate is None:
        raise RadarTargetFactsError("source is outside the exact four-provider allowlist")
    for value, label in (
        (field.canonical_receipt_sha256, "canonical receipt"),
        (field.canonical_result_sha256, "canonical result"),
        (field.capture_receipt_sha256, "capture receipt"),
        (field.capture_manifest_sha256, "capture manifest"),
        (field.artifact_sha256, "artifact"),
        (field.artifact_metadata_sha256, "artifact metadata"),
        (field.decoder_config_sha256, "decoder config"),
        (field.semantics_certificate_sha256, "semantics certificate"),
    ):
        _sha256(value, label)
    expected_binding = (
        certificate.fingerprint,
        certificate.provider,
        certificate.product,
        certificate.decoder_identity,
        certificate.decoder_version,
        certificate.decoder_config_sha256,
        certificate.receipt_quantity,
        certificate.receipt_unit,
        certificate.output_quantity,
        certificate.output_unit,
        certificate.temporal_semantics,
        certificate.primary_sources,
        profile.expected_quality_state,
    )
    actual_binding = (
        field.semantics_certificate_sha256,
        field.provider,
        field.product,
        field.decoder_identity,
        field.decoder_version,
        field.decoder_config_sha256,
        field.source_quantity,
        field.source_unit,
        field.output_quantity,
        field.output_unit,
        field.temporal_semantics,
        field.primary_sources,
        field.quality_state,
    )
    if actual_binding != expected_binding:
        raise RadarTargetFactsError(
            "source field differs from its official semantics certificate"
        )
    if field.artifact_handle != certificate.artifact_handle_prefix + field.artifact_sha256:
        raise RadarTargetFactsError("artifact handle is not content-addressed")
    if field.candidate_id != _candidate_address(field):
        raise RadarTargetFactsError("canonical candidate address differs")
    _validate_geometry(field, certificate)
    planes = {item.name: item for item in field.auxiliary_planes}
    if len(planes) != len(field.auxiliary_planes):
        raise RadarTargetFactsError("source field has duplicate auxiliary planes")
    missing = set(profile.required_qc_planes) - set(planes)
    if missing:
        raise RadarTargetFactsError(
            f"source field lacks certified QC planes: {sorted(missing)}"
        )
    try:
        candidate_content_sha = native_candidate_content_sha256(field)
    except (TypeError, ValueError) as exc:
        raise RadarTargetFactsError(
            "complete canonical candidate content cannot be checksum-bound"
        ) from exc
    return profile, certificate, candidate_content_sha


def _prepare_dwd_source(
    profile: RadarProviderProfile,
    certificate: RadarFieldSemanticsCertificate,
    source: VerifiedRadarFieldSource,
    candidate_content_sha256: str,
) -> _PreparedRadarSource:
    field = source.field
    grid_certificate = source.native_grid_certificate
    if grid_certificate is None:
        raise RadarTargetFactsError(
            "DWD target sampling requires the official DE1200 edge certificate"
        )
    certificate_identity = _digest({
        "candidate_content_sha256": candidate_content_sha256,
        "coverage_definition_sha256": (
            field.native_coverage.definition_sha256
        ),
        "evidence_sha256": grid_certificate.evidence_sha256,
        "grid_definition_sha256": field.native_grid.definition_sha256,
        "semantics_certificate_sha256": field.semantics_certificate_sha256,
        "target_crs": grid_certificate.target_crs,
        "transform_config_sha256": grid_certificate.transform_config_sha256,
    })
    expected_certificate_id = _DWD_GRID_CERTIFICATE_PREFIX + certificate_identity
    expected_binding = (
        expected_certificate_id,
        _DWD_GRID_ADAPTER_ID,
        _DWD_GRID_ADAPTER_VERSION,
        TransformEvidenceClass.OFFICIAL_EXACT_GRID_ANCHORS,
        field.provider,
        field.product,
        field.semantics_certificate_id,
        field.semantics_certificate_sha256,
        field.algorithm_version,
        field.native_crs,
        field.native_grid.definition_sha256,
        field.native_coverage.definition_sha256,
        1100,
        1200,
        "yx",
        field.native_crs,
        -500.0,
        500.0,
        1000.0,
        1000.0,
        -500.0,
        -1_199_500.0,
        1_099_500.0,
        500.0,
        1100,
        1200,
    )
    actual_binding = (
        grid_certificate.certificate_id,
        grid_certificate.adapter_id,
        grid_certificate.adapter_version,
        grid_certificate.evidence_class,
        grid_certificate.source_provider,
        grid_certificate.source_product,
        grid_certificate.source_semantics_certificate_id,
        grid_certificate.source_semantics_certificate_sha256,
        grid_certificate.source_candidate_algorithm,
        grid_certificate.source_crs,
        grid_certificate.source_grid_definition_sha256,
        grid_certificate.source_coverage_definition_sha256,
        grid_certificate.source_width,
        grid_certificate.source_height,
        grid_certificate.source_array_axis_order,
        grid_certificate.target_crs,
        grid_certificate.source_west_edge_target_m,
        grid_certificate.source_north_edge_target_m,
        grid_certificate.source_x_resolution_target_m,
        grid_certificate.source_y_resolution_target_m,
        field.native_coverage.minimum_x,
        field.native_coverage.minimum_y,
        field.native_coverage.maximum_x,
        field.native_coverage.maximum_y,
        field.native_grid.width,
        field.native_grid.height,
    )
    if actual_binding != expected_binding:
        raise RadarTargetFactsError(
            "DWD target sampling official DE1200 certificate or geometry drifted"
        )
    return _PreparedRadarSource(
        profile=profile,
        certificate=certificate,
        source=source,
        candidate_content_sha256=candidate_content_sha256,
        geometry_state="certified_dwd_de1200_pixel_edges",
        geometry_evidence_sha256=grid_certificate.fingerprint,
        affine_grid_certificate=grid_certificate,
    )


def _prepare_noaa_source(
    profile: RadarProviderProfile,
    certificate: RadarFieldSemanticsCertificate,
    source: VerifiedRadarFieldSource,
    candidate_content_sha256: str,
) -> _PreparedRadarSource:
    from .noaa_err_quantitative import (
        GLOBAL_COLUMNS,
        GLOBAL_ROWS,
        GRID_NORTH,
        GRID_RESOLUTION_DEGREES,
        GRID_WEST,
    )

    field = source.field
    coverage = field.native_coverage
    resolution = GRID_RESOLUTION_DEGREES
    half = resolution / 2.0
    column_start = (coverage.minimum_x - (GRID_WEST - half)) / resolution
    row_start = ((GRID_NORTH + half) - coverage.maximum_y) / resolution
    aligned_column = round(column_start)
    aligned_row = round(row_start)
    if (
        field.native_crs != "EPSG:4326"
        or not math.isclose(
            coverage.maximum_x - coverage.minimum_x,
            field.native_grid.width * resolution,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        or not math.isclose(
            coverage.maximum_y - coverage.minimum_y,
            field.native_grid.height * resolution,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        or not math.isclose(
            column_start, aligned_column, rel_tol=0.0, abs_tol=1e-8
        )
        or not math.isclose(row_start, aligned_row, rel_tol=0.0, abs_tol=1e-8)
        or not 0 <= aligned_column < GLOBAL_COLUMNS
        or not 0 <= aligned_row < GLOBAL_ROWS
        or aligned_column + field.native_grid.width > GLOBAL_COLUMNS
        or aligned_row + field.native_grid.height > GLOBAL_ROWS
    ):
        raise RadarTargetFactsError(
            "NOAA target sampling grid differs from the exact GLB-5 window"
        )
    evidence_sha = _digest({
        "coverage_definition_sha256": coverage.definition_sha256,
        "global_columns": GLOBAL_COLUMNS,
        "global_rows": GLOBAL_ROWS,
        "grid_definition_sha256": field.native_grid.definition_sha256,
        "grid_north_center": GRID_NORTH,
        "grid_west_center": GRID_WEST,
        "resolution_degrees": resolution,
        "window_column_start": aligned_column,
        "window_row_start": aligned_row,
    })
    return _PreparedRadarSource(
        profile=profile,
        certificate=certificate,
        source=source,
        candidate_content_sha256=candidate_content_sha256,
        geometry_state="certified_noaa_regular_wgs84_window",
        geometry_evidence_sha256=evidence_sha,
    )


def _prepare_met_office_source(
    profile: RadarProviderProfile,
    certificate: RadarFieldSemanticsCertificate,
    source: VerifiedRadarFieldSource,
    candidate_content_sha256: str,
) -> _PreparedRadarSource:
    field = source.field
    analysis = field.analysis_grid
    if analysis is None:
        raise RadarTargetFactsError("Met Office exact analysis grid disappeared")
    evidence_sha = _digest({
        "artifact_metadata_sha256": field.artifact_metadata_sha256,
        "coverage_definition_sha256": field.native_coverage.definition_sha256,
        "grid": {
            "crs": analysis.crs,
            "height": analysis.height,
            "origin_x_m": analysis.origin_x_m,
            "origin_y_m": analysis.origin_y_m,
            "width": analysis.width,
            "x_resolution_m": analysis.x_resolution_m,
            "y_resolution_m": analysis.y_resolution_m,
        },
        "grid_definition_sha256": field.native_grid.definition_sha256,
        "semantics_certificate_sha256": field.semantics_certificate_sha256,
    })
    return _PreparedRadarSource(
        profile=profile,
        certificate=certificate,
        source=source,
        candidate_content_sha256=candidate_content_sha256,
        geometry_state="certified_artifact_exact_analysis_grid",
        geometry_evidence_sha256=evidence_sha,
    )


def _prepare_knmi_source(
    profile: RadarProviderProfile,
    certificate: RadarFieldSemanticsCertificate,
    source: VerifiedRadarFieldSource,
    candidate_content_sha256: str,
) -> _PreparedRadarSource:
    evidence_sha = _digest({
        "authority_gate": _KNMI_GRID_GATE_SCHEMA,
        "blockers": list(_KNMI_GRID_BLOCKERS),
        "certificate_issuer_state": "unconditionally_withheld",
        "decision": "no_target_pixel_or_rate_without_exact_edge_authority",
    })
    return _PreparedRadarSource(
        profile=profile,
        certificate=certificate,
        source=source,
        candidate_content_sha256=candidate_content_sha256,
        geometry_state="withheld_knmi_pixel_edge_authority",
        geometry_evidence_sha256=evidence_sha,
    )


def _prepare_source(
    profile: RadarProviderProfile,
    certificate: RadarFieldSemanticsCertificate,
    source: VerifiedRadarFieldSource,
    candidate_content_sha256: str,
) -> _PreparedRadarSource:
    if (
        profile.geometry_policy != "dwd_official_de1200_edges"
        and source.native_grid_certificate is not None
    ):
        raise RadarTargetFactsError(
            "native grid certificate is not valid for this provider"
        )
    preparers = {
        "dwd_official_de1200_edges": _prepare_dwd_source,
        "knmi_grid_authority_withheld": _prepare_knmi_source,
        "artifact_exact_analysis_grid": _prepare_met_office_source,
        "noaa_exact_regular_wgs84_window": _prepare_noaa_source,
    }
    preparer = preparers.get(profile.geometry_policy)
    if preparer is None:
        raise RadarTargetFactsError("provider geometry policy is unsupported")
    return preparer(
        profile,
        certificate,
        source,
        candidate_content_sha256,
    )


def _source_evidence_sha256(
    field: NativeRainRateFieldCandidate,
    candidate_content_sha256: str,
) -> str:
    return _digest({
        "artifact_metadata_sha256": field.artifact_metadata_sha256,
        "artifact_sha256": field.artifact_sha256,
        "candidate_content_sha256": candidate_content_sha256,
        "capture_id": field.capture_id,
        "capture_manifest_sha256": field.capture_manifest_sha256,
        "capture_receipt_sha256": field.capture_receipt_sha256,
    })


def _process_evidence_sha256(
    prepared: _PreparedRadarSource,
    *,
    desired_state_sha256: str,
    target: Target,
    sampled_row: int | None,
    sampled_column: int | None,
    rain_rate_mm_h: float | None,
    observation_state: RadarObservationState,
    coverage_state: RadarCoverageState,
    sampling_method: str,
) -> str:
    field = prepared.source.field
    return _digest({
        "algorithm": sampling_method,
        "canonical_algorithm": field.algorithm_version,
        "candidate_id": field.candidate_id,
        "candidate_content_sha256": prepared.candidate_content_sha256,
        "canonical_commit_id": field.canonical_commit_id,
        "canonical_receipt_sha256": field.canonical_receipt_sha256,
        "canonical_result_sha256": field.canonical_result_sha256,
        "canonical_run_id": field.canonical_run_id,
        "decoder_config_sha256": field.decoder_config_sha256,
        "desired_state_sha256": desired_state_sha256,
        "geometry_evidence_sha256": prepared.geometry_evidence_sha256,
        "geometry_state": prepared.geometry_state,
        "native_coverage_definition_sha256": (
            field.native_coverage.definition_sha256
        ),
        "native_grid_definition_sha256": field.native_grid.definition_sha256,
        "observation": {
            "coverage_state": coverage_state.value,
            "rain_rate_mm_h": rain_rate_mm_h,
            "state": observation_state.value,
        },
        "sample": {
            "column": sampled_column,
            "row": sampled_row,
        },
        "semantics_certificate_sha256": field.semantics_certificate_sha256,
        "target": {
            "available": target.available,
            "latitude": target.latitude,
            "longitude": target.longitude,
            "target_id": target.target_id,
        },
    })


def _transform_target(
    field: NativeRainRateFieldCandidate,
    target: Target,
) -> tuple[float, float]:
    try:
        transformer = Transformer.from_crs(
            "EPSG:4326",
            field.native_crs,
            always_xy=True,
        )
        x_value, y_value = transformer.transform(
            target.longitude,
            target.latitude,
            errcheck=True,
        )
    except (CRSError, ProjError, TypeError, ValueError) as exc:
        raise RadarTargetFactsError(
            "target cannot be transformed into the certified native CRS"
        ) from exc
    x = _finite(float(x_value), "projected target x")
    y = _finite(float(y_value), "projected target y")
    return x, y


def _pixel(
    prepared: _PreparedRadarSource,
    x: float,
    y: float,
) -> tuple[int, int] | None:
    field = prepared.source.field
    coverage = field.native_coverage
    if not (
        coverage.minimum_x <= x < coverage.maximum_x
        and coverage.minimum_y < y <= coverage.maximum_y
    ):
        return None
    grid_certificate = prepared.affine_grid_certificate
    if grid_certificate is not None:
        column = math.floor(
            (x - grid_certificate.source_west_edge_target_m)
            / grid_certificate.source_x_resolution_target_m
        )
        row = math.floor(
            (grid_certificate.source_north_edge_target_m - y)
            / grid_certificate.source_y_resolution_target_m
        )
    elif field.analysis_grid is not None:
        try:
            return field.analysis_grid.point_to_pixel(x, y)
        except ValueError as exc:  # Geometry equality was checked before sampling.
            raise RadarTargetFactsError(
                "analysis grid rejected a point inside certified coverage"
            ) from exc
    else:
        column = math.floor(
            (x - coverage.minimum_x)
            * field.native_grid.width
            / (coverage.maximum_x - coverage.minimum_x)
        )
        row = math.floor(
            (coverage.maximum_y - y)
            * field.native_grid.height
            / (coverage.maximum_y - coverage.minimum_y)
        )
    if not (
        0 <= row < field.native_grid.height
        and 0 <= column < field.native_grid.width
    ):
        raise RadarTargetFactsError(
            "certified coverage produced an out-of-range pixel"
        )
    return row, column


def _qc_value(value: np.generic[Any] | bool | int | float) -> bool | int | float | None:
    scalar = value.item() if isinstance(value, np.generic) else value
    if isinstance(scalar, bool):
        return scalar
    if isinstance(scalar, int):
        return scalar
    if isinstance(scalar, float):
        return scalar if math.isfinite(scalar) else None
    raise RadarTargetFactsError("provider QC plane contains an unsupported scalar")


def _qc_samples(
    field: NativeRainRateFieldCandidate,
    profile: RadarProviderProfile,
    pixel: tuple[int, int] | None,
) -> tuple[RadarQcSample, ...]:
    if pixel is None:
        return ()
    row, column = pixel
    planes = {item.name: item for item in field.auxiliary_planes}
    return tuple(
        RadarQcSample(
            plane=name,
            semantics=planes[name].semantics,
            value=_qc_value(planes[name].values[row, column]),
        )
        for name in sorted(profile.required_qc_planes)
    )


def _state(
    field: NativeRainRateFieldCandidate,
    pixel: tuple[int, int] | None,
) -> tuple[
    float | None,
    RadarObservationState,
    RadarCoverageState,
    RadarClaimStatus,
]:
    if pixel is None:
        return (
            None,
            RadarObservationState.OUTSIDE_DOMAIN,
            RadarCoverageState.OUTSIDE_DOMAIN,
            RadarClaimStatus.NO_CLAIM,
        )
    row, column = pixel
    if bool(field.nodata_mask[row, column]):
        return (
            None,
            RadarObservationState.NODATA_WITHHELD,
            RadarCoverageState.IN_DOMAIN_NODATA,
            RadarClaimStatus.NO_CLAIM,
        )
    if bool(field.undetect_mask[row, column]):
        return (
            None,
            RadarObservationState.DRY_WITHHELD,
            RadarCoverageState.IN_DOMAIN_UNDETECT,
            RadarClaimStatus.NEGATIVE_WITHHELD,
        )
    rate = field.value_or_none(row, column)
    if rate is None:
        raise RadarTargetFactsError("valid source pixel did not expose a rate")
    if rate > 0.0:
        return (
            rate,
            RadarObservationState.POSITIVE_RAIN_CANDIDATE,
            RadarCoverageState.IN_DOMAIN_VALID,
            RadarClaimStatus.POSITIVE_CANDIDATE_ONLY,
        )
    return (
        0.0,
        RadarObservationState.DRY_WITHHELD,
        RadarCoverageState.IN_DOMAIN_VALID,
        RadarClaimStatus.NEGATIVE_WITHHELD,
    )


def _build_contribution(
    desired_state: DesiredState,
    target: Target,
    prepared: _PreparedRadarSource,
) -> RadarTargetContribution:
    source = prepared.source
    profile = prepared.profile
    field = source.field
    pixel: tuple[int, int] | None = None
    if not target.available:
        rate = None
        observation = RadarObservationState.TARGET_UNAVAILABLE
        coverage = RadarCoverageState.TARGET_UNAVAILABLE
        claim = RadarClaimStatus.NO_CLAIM
    elif profile.geometry_policy == "knmi_grid_authority_withheld":
        rate = None
        observation = RadarObservationState.GRID_SEMANTICS_WITHHELD
        coverage = RadarCoverageState.GRID_SEMANTICS_WITHHELD
        claim = RadarClaimStatus.NO_CLAIM
    else:
        pixel = _pixel(prepared, *_transform_target(field, target))
        rate, observation, coverage, claim = _state(field, pixel)
    row = None if pixel is None else pixel[0]
    column = None if pixel is None else pixel[1]
    sampling_method = (
        RADAR_TARGET_NO_SAMPLE_METHOD
        if observation in {
            RadarObservationState.GRID_SEMANTICS_WITHHELD,
            RadarObservationState.TARGET_UNAVAILABLE,
        }
        else RADAR_TARGET_SAMPLING_METHOD
    )
    source_sha = _source_evidence_sha256(
        field,
        prepared.candidate_content_sha256,
    )
    process_sha = _process_evidence_sha256(
        prepared,
        desired_state_sha256=desired_state.state_sha256,
        target=target,
        sampled_row=row,
        sampled_column=column,
        rain_rate_mm_h=rate,
        observation_state=observation,
        coverage_state=coverage,
        sampling_method=sampling_method,
    )
    values = {
        "contribution_id": "",
        "content_sha256": "0" * 64,
        "desired_state_sha256": desired_state.state_sha256,
        "target_id": target.target_id,
        "target_latitude": target.latitude,
        "target_longitude": target.longitude,
        "provider": field.provider,
        "workload_id": profile.workload_id,
        "product": field.product,
        "product_valid_at": field.valid_at,
        "received_at": float(source.received_at),
        "rain_rate_mm_h": rate,
        "observation_state": observation,
        "coverage_state": coverage,
        "claim_status": claim,
        "current_rain_candidate": (
            observation is RadarObservationState.POSITIVE_RAIN_CANDIDATE
        ),
        "full_scan_bound": False,
        "external_truth_bound": False,
        "quality_state": field.quality_state,
        "qc_samples": _qc_samples(field, profile, pixel),
        "geometry_state": prepared.geometry_state,
        "geometry_evidence_sha256": prepared.geometry_evidence_sha256,
        "sampled_row": row,
        "sampled_column": column,
        "sampling_method": sampling_method,
        "source_evidence_sha256": source_sha,
        "process_evidence_sha256": process_sha,
        "candidate_id": field.candidate_id,
        "candidate_content_sha256": prepared.candidate_content_sha256,
        "canonical_run_id": field.canonical_run_id,
        "canonical_commit_id": field.canonical_commit_id,
        "canonical_receipt_sha256": field.canonical_receipt_sha256,
        "canonical_result_sha256": field.canonical_result_sha256,
        "capture_id": field.capture_id,
        "capture_receipt_sha256": field.capture_receipt_sha256,
        "capture_manifest_sha256": field.capture_manifest_sha256,
        "artifact_sha256": field.artifact_sha256,
        "artifact_metadata_sha256": field.artifact_metadata_sha256,
        "decoder_config_sha256": field.decoder_config_sha256,
        "semantics_certificate_id": field.semantics_certificate_id,
        "semantics_certificate_sha256": field.semantics_certificate_sha256,
        "native_grid_definition_sha256": field.native_grid.definition_sha256,
        "native_coverage_definition_sha256": (
            field.native_coverage.definition_sha256
        ),
    }
    provisional = RadarTargetContribution.__new__(RadarTargetContribution)
    for name, value in values.items():
        object.__setattr__(provisional, name, value)
    content_sha = _digest(provisional._body())
    values["content_sha256"] = content_sha
    values["contribution_id"] = f"radar-target-{content_sha}"
    return RadarTargetContribution(**values)


def build_radar_target_contribution_batch(
    desired_state: DesiredState,
    sources: Sequence[VerifiedRadarFieldSource],
) -> RadarTargetContributionBatch:
    """Sample one to four certified fields at every explicit desired target.

    The result is deterministic for the same desired-state bytes, canonical
    candidates and receipt times.  Duplicate providers are rejected so a later
    cycle cannot accidentally select an arbitrary frame.
    """

    if not isinstance(desired_state, DesiredState):
        raise RadarTargetFactsError("desired_state must be parsed and canonical")
    _sha256(desired_state.state_sha256, "desired state")
    if not desired_state.targets:
        raise RadarTargetFactsError("at least one explicit desired target is required")
    if (
        not isinstance(sources, Sequence)
        or isinstance(sources, (str, bytes, bytearray))
    ):
        raise RadarTargetFactsError("sources must be a bounded sequence")
    selected = tuple(sources)
    if (
        not 1 <= len(selected) <= MAX_RADAR_SOURCES
        or any(not isinstance(item, VerifiedRadarFieldSource) for item in selected)
    ):
        raise RadarTargetFactsError("one to four verified radar sources are required")
    certificates = _official_certificates()
    prepared_sources = []
    for source in selected:
        profile, certificate, candidate_sha = _validate_candidate(
            source.field,
            certificates,
        )
        prepared_sources.append(
            _prepare_source(profile, certificate, source, candidate_sha)
        )
    workloads = [item.profile.workload_id for item in prepared_sources]
    providers = [item.source.field.provider for item in prepared_sources]
    if len(set(workloads)) != len(workloads) or len(set(providers)) != len(providers):
        raise RadarTargetFactsError("duplicate radar provider/workload is ambiguous")
    contributions = tuple(
        _build_contribution(desired_state, target, prepared)
        for prepared in sorted(
            prepared_sources,
            key=lambda item: item.profile.workload_id,
        )
        for target in desired_state.targets
    )
    for prepared in prepared_sources:
        if (
            native_candidate_content_sha256(prepared.source.field)
            != prepared.candidate_content_sha256
        ):
            raise RadarTargetFactsError(
                "canonical candidate content changed during target sampling"
            )
    body = {
        "contract": RADAR_TARGET_BATCH_CONTRACT,
        "contributions": [item.as_dict() for item in contributions],
        "desired_state_sha256": desired_state.state_sha256,
        "source_count": len(prepared_sources),
        "target_count": len(desired_state.targets),
    }
    content_sha = _digest(body)
    process_receipt_sha = _digest(
        _batch_process_receipt_body(
            content_sha256=content_sha,
            desired_state_sha256=desired_state.state_sha256,
            contributions=contributions,
        )
    )
    return RadarTargetContributionBatch(
        batch_id=f"radar-target-batch-{content_sha}",
        content_sha256=content_sha,
        process_receipt_sha256=process_receipt_sha,
        desired_state_sha256=desired_state.state_sha256,
        source_count=len(prepared_sources),
        target_count=len(desired_state.targets),
        contributions=contributions,
    )


__all__ = [
    "MAX_RADAR_SOURCES",
    "RADAR_PROVIDER_PROFILES",
    "RADAR_TARGET_BATCH_CONTRACT",
    "RADAR_TARGET_BATCH_PROCESS_RECEIPT_CONTRACT",
    "RADAR_TARGET_CONTRIBUTION_CONTRACT",
    "RADAR_TARGET_NO_SAMPLE_METHOD",
    "RADAR_TARGET_SAMPLING_METHOD",
    "RadarClaimStatus",
    "RadarCoverageState",
    "RadarObservationState",
    "RadarProviderProfile",
    "RadarQcSample",
    "RadarTargetContribution",
    "RadarTargetContributionBatch",
    "RadarTargetFactsError",
    "VerifiedRadarFieldSource",
    "build_radar_target_contribution_batch",
]
