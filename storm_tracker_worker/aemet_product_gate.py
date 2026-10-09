"""Fail-closed evidence gate for the AEMET national radar composite.

This is a vNext shadow primitive.  It performs no network or filesystem I/O,
does not decode pixels and is not imported by the worker registry or
scheduler.  It records only claims that can be traced to AEMET's own radar
help, OpenData specification and legal notice.

The operational ``aemet_radar_fetch`` path must not be treated as a
quantitative decoder.  AEMET identifies the downloadable national product as
lowest-elevation PPI reflectivity in dBZ and says that the per-file ``ESCALA``
metadata maps RGBA colours to meteorological value intervals.  The current
parser instead assigns meaning directly to byte values 1..8 and discards all
other values.  No official source audited here establishes that mapping, the
timestamp timezone/support, the complete grid, the missing/no-echo/coverage
codes, or pixel QC semantics.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import re
from typing import Any
from urllib.parse import urlsplit


SCHEMA_VERSION = "aemet-national-radar-shadow-gate-v1"
PROVIDER = "aemet_radar"
OPERATIONAL_JOB_TYPE = "aemet_radar_fetch"
SHADOW_ONLY = True

LEGACY_WEB_ARCHIVE_URL = (
    "https://www.aemet.es/es/api-eltiempo/radar/download/compo"
)
OPENDATA_SERVER = "https://opendata.aemet.es/opendata"
OPENDATA_NATIONAL_RADAR_PATH = "/api/red/radar/nacional"

AEMET_RADAR_HELP = (
    "https://www.aemet.es/en/eltiempo/observacion/radar/ayuda"
)
AEMET_OPENDATA_SPECIFICATION = (
    "https://opendata.aemet.es/AEMET_OpenData_specification.json"
)
AEMET_LEGAL_NOTICE = "https://www.aemet.es/es/nota_legal"

OFFICIAL_PRIMARY_SOURCES = (
    AEMET_RADAR_HELP,
    AEMET_OPENDATA_SPECIFICATION,
    AEMET_LEGAL_NOTICE,
)

# The OpenData success schema requires both URL-bearing fields.  A future
# exact capture must retain the response that selected them and both selected
# responses before inspecting any of the three bodies.  The api_key itself is
# transport-only and must never be placed in a receipt or record identifier.
REQUIRED_EXACT_CAPTURE_ROLES = (
    "opendata_envelope_json",
    "opendata_data_response",
    "opendata_metadata_response",
)

_LEGACY_MEMBER_RE = re.compile(
    r"^down_radw(?P<timestamp>\d{12})_4326\.tif$"
)


class EvidenceState(str, Enum):
    """Whether one product fact is proven by the pinned primary sources."""

    VERIFIED = "VERIFIED"
    UNVERIFIED = "UNVERIFIED"
    BLOCKED = "BLOCKED"


class AemetProductGateError(ValueError):
    """Input is not safe enough to retain even as shadow product evidence."""


@dataclass(frozen=True, slots=True)
class EvidenceFinding:
    """One source-traceable claim; non-VERIFIED claims remain blockers."""

    key: str
    state: EvidenceState
    finding: str
    source_urls: tuple[str, ...]

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{2,95}", self.key):
            raise AemetProductGateError("ongeldige AEMET evidence-key")
        if not isinstance(self.state, EvidenceState):
            raise TypeError("state moet EvidenceState zijn")
        if not isinstance(self.finding, str) or not self.finding.strip():
            raise AemetProductGateError("AEMET finding is leeg")
        if not self.source_urls:
            raise AemetProductGateError("AEMET finding mist primaire bron")
        for source in self.source_urls:
            parsed = urlsplit(source)
            if (
                parsed.scheme != "https"
                or parsed.hostname
                not in {"www.aemet.es", "opendata.aemet.es"}
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
            ):
                raise AemetProductGateError(
                    "AEMET finding bevat geen veilige primaire HTTPS-bron"
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "state": self.state.value,
            "finding": self.finding,
            "source_urls": list(self.source_urls),
        }


FINDINGS = (
    EvidenceFinding(
        key="product_identity",
        state=EvidenceState.VERIFIED,
        finding=(
            "National Peninsular-and-Balearic composite of the lowest PPI "
            "reflectivity scans; the Canary Islands radar is excluded."
        ),
        source_urls=(AEMET_RADAR_HELP,),
    ),
    EvidenceFinding(
        key="meteorological_quantity",
        state=EvidenceState.VERIFIED,
        finding=(
            "The downloadable national composite represents radar "
            "reflectivity, not precipitation rate."
        ),
        source_urls=(AEMET_RADAR_HELP,),
    ),
    EvidenceFinding(
        key="physical_unit",
        state=EvidenceState.VERIFIED,
        finding="Reflectivity intervals are expressed in dBZ.",
        source_urls=(AEMET_RADAR_HELP,),
    ),
    EvidenceFinding(
        key="rain_rate_interpretation",
        state=EvidenceState.BLOCKED,
        finding=(
            "AEMET explicitly says reflectivity and precipitation intensity "
            "are not one-to-one; no unconditional dBZ-to-rain-rate truth is "
            "available."
        ),
        source_urls=(AEMET_RADAR_HELP,),
    ),
    EvidenceFinding(
        key="raster_crs",
        state=EvidenceState.VERIFIED,
        finding="The downloadable radar GeoTIFF is reprojected to EPSG:4326.",
        source_urls=(AEMET_RADAR_HELP,),
    ),
    EvidenceFinding(
        key="exact_grid_geometry",
        state=EvidenceState.UNVERIFIED,
        finding=(
            "The official help pins the CRS but not dimensions, resolution, "
            "pixel-is-area/point support, axis order, origin or affine "
            "transform for this exact national product revision."
        ),
        source_urls=(AEMET_RADAR_HELP,),
    ),
    EvidenceFinding(
        key="pixel_scale_mapping",
        state=EvidenceState.BLOCKED,
        finding=(
            "AEMET requires the per-file ESCALA metadata to map RGBA colours "
            "to value intervals; the existing byte-value 1..8 mapping is not "
            "an official quantitative legend."
        ),
        source_urls=(AEMET_RADAR_HELP,),
    ),
    EvidenceFinding(
        key="timestamp_timezone",
        state=EvidenceState.UNVERIFIED,
        finding=(
            "The sources say each raster has a date and time, but do not "
            "define the timezone or prove that the legacy filename token is "
            "UTC."
        ),
        source_urls=(AEMET_RADAR_HELP,),
    ),
    EvidenceFinding(
        key="observation_support",
        state=EvidenceState.UNVERIFIED,
        finding=(
            "No source pins whether the nominal time is scan start, scan end, "
            "mosaic completion, midpoint or another support convention."
        ),
        source_urls=(AEMET_RADAR_HELP,),
    ),
    EvidenceFinding(
        key="nodata_noecho_coverage",
        state=EvidenceState.UNVERIFIED,
        finding=(
            "AEMET documents dark grey as no operational radar coverage, but "
            "does not pin exact sample/RGBA codes that distinguish no-data, "
            "no-echo and no-coverage in the audited transport."
        ),
        source_urls=(AEMET_RADAR_HELP,),
    ),
    EvidenceFinding(
        key="pixel_qc_semantics",
        state=EvidenceState.UNVERIFIED,
        finding=(
            "The mosaic selection considers orographic blockage and radar "
            "distance, but no complete per-pixel QC field/code contract is "
            "published in the audited sources."
        ),
        source_urls=(AEMET_RADAR_HELP,),
    ),
    EvidenceFinding(
        key="documented_api_access",
        state=EvidenceState.VERIFIED,
        finding=(
            "The official OpenData API exposes GET /api/red/radar/nacional, "
            "requires api_key authentication and declares a 30-minute "
            "periodicity."
        ),
        source_urls=(AEMET_OPENDATA_SPECIFICATION,),
    ),
    EvidenceFinding(
        key="legacy_transport_contract",
        state=EvidenceState.UNVERIFIED,
        finding=(
            "The existing fetch uses the web download route rather than the "
            "documented OpenData endpoint; its tar/member/cadence contract is "
            "not pinned by the OpenData specification."
        ),
        source_urls=(AEMET_RADAR_HELP, AEMET_OPENDATA_SPECIFICATION),
    ),
    EvidenceFinding(
        key="complete_artifact_set",
        state=EvidenceState.BLOCKED,
        finding=(
            "The documented success envelope requires datos and metadatos; "
            "the existing fetch captures neither envelope nor separate "
            "metadata response as immutable schema-3 evidence before decode."
        ),
        source_urls=(AEMET_OPENDATA_SPECIFICATION,),
    ),
    EvidenceFinding(
        key="retention_and_reuse",
        state=EvidenceState.VERIFIED,
        finding=(
            "AEMET permits commercial and non-commercial reuse, reproduction, "
            "distribution and transformation under its general legal notice."
        ),
        source_urls=(AEMET_LEGAL_NOTICE,),
    ),
    EvidenceFinding(
        key="attribution_and_metadata_duties",
        state=EvidenceState.VERIFIED,
        finding=(
            "Reuse must attribute AEMET, preserve included update/terms "
            "metadata, avoid implying endorsement and not distort meaning."
        ),
        source_urls=(AEMET_LEGAL_NOTICE,),
    ),
    EvidenceFinding(
        key="official_golden_artifact",
        state=EvidenceState.BLOCKED,
        finding=(
            "No exact current official envelope/data/metadata fixture with a "
            "verified checksum was available to this audit."
        ),
        source_urls=(AEMET_OPENDATA_SPECIFICATION, AEMET_RADAR_HELP),
    ),
)


@dataclass(frozen=True, slots=True)
class AemetNationalRadarGate:
    """Pinned provider gate; convenience properties cannot admit data."""

    schema_version: str = SCHEMA_VERSION
    provider: str = PROVIDER
    operational_job_type: str = OPERATIONAL_JOB_TYPE
    shadow_only: bool = SHADOW_ONLY
    findings: tuple[EvidenceFinding, ...] = FINDINGS

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise AemetProductGateError("onverwachte AEMET gate-versie")
        if (
            self.provider != PROVIDER
            or self.operational_job_type != OPERATIONAL_JOB_TYPE
        ):
            raise AemetProductGateError("AEMET provider/job-identiteit wijkt af")
        keys = tuple(item.key for item in self.findings)
        if len(keys) != len(set(keys)):
            raise AemetProductGateError("dubbele AEMET evidence-key")

    @property
    def blocking_gaps(self) -> tuple[str, ...]:
        return tuple(
            item.key
            for item in self.findings
            if item.state is not EvidenceState.VERIFIED
        )

    @property
    def exact_native_capture_candidate_eligible(self) -> bool:
        return False

    @property
    def quantitative_decode_eligible(self) -> bool:
        return False

    @property
    def rain_rate_eligible(self) -> bool:
        return False

    @property
    def detection_eligible(self) -> bool:
        return False

    @property
    def forecast_eligible(self) -> bool:
        return False

    @property
    def operational_admission_eligible(self) -> bool:
        return False

    @property
    def retention_rights_verified(self) -> bool:
        return self._state("retention_and_reuse") is EvidenceState.VERIFIED

    def _state(self, key: str) -> EvidenceState:
        for finding in self.findings:
            if finding.key == key:
                return finding.state
        raise AemetProductGateError(f"onbekende AEMET evidence-key {key!r}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "provider": self.provider,
            "operational_job_type": self.operational_job_type,
            "shadow_only": self.shadow_only,
            "weather_state": "unknown",
            "required_exact_capture_roles": list(REQUIRED_EXACT_CAPTURE_ROLES),
            "blocking_gaps": list(self.blocking_gaps),
            "exact_native_capture_candidate_eligible": (
                self.exact_native_capture_candidate_eligible
            ),
            "quantitative_decode_eligible": self.quantitative_decode_eligible,
            "rain_rate_eligible": self.rain_rate_eligible,
            "detection_eligible": self.detection_eligible,
            "forecast_eligible": self.forecast_eligible,
            "operational_admission_eligible": (
                self.operational_admission_eligible
            ),
            "retention_rights_verified": self.retention_rights_verified,
            "findings": [item.to_dict() for item in self.findings],
        }


AEMET_NATIONAL_RADAR_GATE = AemetNationalRadarGate()


@dataclass(frozen=True, slots=True)
class LegacyMemberFacts:
    """Structural filename facts without inventing a UTC observation time."""

    member_name: str
    timestamp_token: str
    crs_token: str = "4326"
    timezone: None = None
    support_start: None = None
    support_end: None = None

    @property
    def nominal_timestamp(self) -> None:
        return None

    @property
    def quantitative_decode_eligible(self) -> bool:
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "member_name": self.member_name,
            "timestamp_token": self.timestamp_token,
            "crs_token": self.crs_token,
            "timezone": self.timezone,
            "support_start": self.support_start,
            "support_end": self.support_end,
            "nominal_timestamp": self.nominal_timestamp,
            "identity_basis": (
                "legacy_parser_pattern_not_official_transport_specification"
            ),
            "quantitative_decode_eligible": self.quantitative_decode_eligible,
        }


def parse_legacy_member_facts(member_name: str) -> LegacyMemberFacts:
    """Validate only the observed legacy name; never assume a timezone."""

    if not isinstance(member_name, str):
        raise TypeError("AEMET member_name moet tekst zijn")
    match = _LEGACY_MEMBER_RE.fullmatch(member_name)
    if match is None:
        raise AemetProductGateError("ongeldige legacy AEMET-membernaam")
    token = match.group("timestamp")
    try:
        datetime.strptime(token, "%Y%m%d%H%M")
    except ValueError as exc:
        raise AemetProductGateError(
            "legacy AEMET-member bevat een ongeldige kalenderdatum"
        ) from exc
    return LegacyMemberFacts(
        member_name=member_name,
        timestamp_token=token,
    )


__all__ = [
    "AEMET_LEGAL_NOTICE",
    "AEMET_NATIONAL_RADAR_GATE",
    "AEMET_OPENDATA_SPECIFICATION",
    "AEMET_RADAR_HELP",
    "AemetNationalRadarGate",
    "AemetProductGateError",
    "EvidenceFinding",
    "EvidenceState",
    "LEGACY_WEB_ARCHIVE_URL",
    "LegacyMemberFacts",
    "OFFICIAL_PRIMARY_SOURCES",
    "OPENDATA_NATIONAL_RADAR_PATH",
    "OPENDATA_SERVER",
    "OPERATIONAL_JOB_TYPE",
    "PROVIDER",
    "REQUIRED_EXACT_CAPTURE_ROLES",
    "SCHEMA_VERSION",
    "SHADOW_ONLY",
    "parse_legacy_member_facts",
]
