"""Crash-durable raw-capture to capacity-ledger reconciliation.

This module is deliberately network-free and decoder-free.  It recognizes
only explicitly registered exact raw identities, verifies an entire immutable
role set from ``RawArtifactStore``, and CAS-commits an existing capacity
reservation.  It never creates a reservation after bytes exist, never deletes
or rewrites raw data, and never handles credentials.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import re
import threading
from types import MappingProxyType
from typing import Any, Callable, Mapping

from .acquisition_activation import (
    ACTIVATION_RULES,
    LOCATION_SHADOW_CANDIDATE_RULES,
    ActivationRule,
)
from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCaptureReceipt,
    RawArtifactError,
    RawArtifactReceipt,
    RawArtifactStore,
)
from .raw_capacity_ledger import (
    CapacityReservationReceipt,
    RawCapacityLedger,
    RawCapacityLedgerBusyError,
    RawCapacityLedgerCapacityError,
    RawCapacityLedgerConflict,
    RawCapacityLedgerIntegrityError,
)
from .worker_acquisition_plan import AcquisitionJob, validate_worker_acquisition_job


CONTRACT = "stv3-worker-acquisition-capture-recovery-v2"
ROLE_SET_DIGEST_CONTRACT = "stv3-worker-raw-role-set-digest-v1"
MAX_DIAGNOSTIC_BYTES = 4096
MAX_STABLE_INVENTORY_ATTEMPTS = 3
MAX_RECOVERY_RESERVATIONS = 65_536
_JOB_ID = re.compile(r"^acquisition:[0-9a-f]{64}$")
_SAFE_NATIVE_JOB_ID = re.compile(
    r"^acq:[a-z2-7]{8}(?::z:[a-z2-7]{8}){5}:z:[a-z2-7]{4}$"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DWD_MEMBER = re.compile(
    r"^composite_rv_(?P<date>\d{8})_(?P<time>\d{4})_000-hd5$"
)
_KNMI_FILENAME = re.compile(
    r"^RAD_NL25_RAC_RT_(?P<timestamp>\d{12})\.h5$"
)
_DPC_KEY = re.compile(
    r"^SRI/(?P<day>\d{2})-(?P<month>\d{2})-(?P<year>\d{4})-"
    r"(?P<hour>\d{2})-(?P<minute>\d{2})\.tif$"
)
_NOAA_ERR_PREFIX = re.compile(
    r"^BLEND/RainRate-Blend-INST/\d{4}/\d{2}/\d{2}/$"
)
_NOAA_ERR_KEY = re.compile(
    r"^BLEND/RainRate-Blend-INST/"
    r"(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{2})/(?P<hour>\d{2})/"
    r"RRQPE-INST-GLB-(?P<inputs>[1-9])_v(?P<major>\d+)r(?P<revision>\d+)_blend_"
    r"s(?P<start>\d{15})_e(?P<end>\d{15})_c(?P<created>\d{15})\.nc$"
)
_GOES_PREFIX = re.compile(r"^ABI-L2-RRQPEF/\d{4}/\d{3}/\d{2}/$")
_GOES_KEY = re.compile(
    r"^ABI-L2-RRQPEF/(?P<year>\d{4})/(?P<day>\d{3})/(?P<hour>\d{2})/"
    r"(?P<filename>OR_ABI-L2-RRQPEF-M(?P<mode>[346])_G(?P<satellite>18|19)_"
    r"s(?P<start>\d{14})_e(?P<end>\d{14})_c(?P<created>\d{14})\.nc)$"
)
_METOFFICE_PREFIX = re.compile(r"^radar/\d{4}/\d{2}/\d{2}/$")
_METOFFICE_KEY = re.compile(
    r"^radar/(?P<day>\d{4}/\d{2}/\d{2})/"
    r"(?P<stamp>\d{12})_ODIM_ng_radar_rainrate_composite_1km_UK\.h5$"
)
_FORBIDDEN_PROVENANCE_KEYS = frozenset({
    "api_key",
    "authorization",
    "temporary_download_url",
    "url",
})
_FORBIDDEN_PROVENANCE_VALUES = (
    "authorization:",
    "bearer ",
    "x-amz-credential",
    "x-amz-security-token",
    "x-amz-signature",
)
_ALLOWED_PUBLIC_PROVENANCE_LOCATORS = frozenset({
    "https://api.netatmo.com",
    "https://api.synopticdata.com",
    "https://radar.protezionecivile.it",
    "https://s3-prod-dpc-radar.s3.eu-south-1.amazonaws.com",
    "https://www.ospo.noaa.gov/operations/goes/product-quality-overview/index.html",
})
_RECEIPT_PROVENANCE_KINDS = frozenset({
    "aemet_data_v1",
    "aemet_envelope_v1",
    "aemet_metadata_v1",
    "closed_role_set_v1",
    "dpc_catalog_v1",
    "dpc_geotiff_v1",
    "dwd_rv_v2",
    "eumetsat_mtg_li_netcdf_v1",
    "eumetsat_mtg_li_search_v1",
    "goes_rrqpe_listing_v1",
    "goes_rrqpe_netcdf4_v1",
    "hsaf_h40b_listing_v1",
    "hsaf_h40b_netcdf_gzip_v1",
    "knmi_rtcor_v1",
    "kmi_appv4_forecast_response_v2",
    "kmi_appv4_image_response_v2",
    "kmi_opendata_response_v1",
    "metar_v2",
    "metoffice_listing_v1",
    "metoffice_odim_v1",
    "meteofrance_catalog_v1",
    "meteofrance_hdf5_v1",
    "noaa_err_listing_v1",
    "noaa_err_netcdf4_v1",
    "noaa_goes_glm_listing_v1",
    "noaa_goes_glm_netcdf_v1",
    "netatmo_v2",
    "opera_stream_raw_v1",
    "openmeteo_unverified_response_v1",
    "openmeteo_icon2i_unverified_response_v1",
    "geosphere_inca_unverified_response_v1",
    "italiameteo_catalog_unverified_response_v1",
    "meteolux_weather_unverified_response_v1",
    "rainviewer_coverage_tile_v1",
    "rainviewer_manifest_v1",
    "rainviewer_radar_tile_v1",
    "synoptic_v5",
})
_CAPTURE_PROVENANCE_KINDS = frozenset({
    "aemet_opendata_v1",
    "closed_role_set_v1",
    "dpc_sri_v1",
    "dwd_rv_v2",
    "eumetsat_mtg_li_exact_raw_v1",
    "goes18_rrqpe_v1",
    "goes19_rrqpe_v1",
    "hsaf_h40b_v1",
    "knmi_rtcor_v1",
    "kmi_appv4_visual_capture_v2",
    "kmi_opendata_v1",
    "metar_v2",
    "metoffice_uk_rate_v1",
    "meteofrance_dpradar_v1",
    "noaa_err_v1",
    "noaa_goes_glm_exact_raw_v1",
    "netatmo_v2",
    "opera_stream_raw_v1",
    "openmeteo_unverified_response_v1",
    "openmeteo_icon2i_unverified_response_v1",
    "geosphere_inca_unverified_response_v1",
    "italiameteo_catalog_unverified_response_v1",
    "meteolux_weather_unverified_response_v1",
    "rainviewer_visual_v1",
    "synoptic_v5",
})
_PUBLIC_SOURCE_PROVENANCE_KINDS = frozenset({
    "openmeteo_unverified_response_v1",
    "openmeteo_icon2i_unverified_response_v1",
    "geosphere_inca_unverified_response_v1",
    "italiameteo_catalog_unverified_response_v1",
    "meteolux_weather_unverified_response_v1",
})
_LIGHTNING_RECEIPT_PROVENANCE_KINDS = frozenset({
    "eumetsat_mtg_li_search_v1",
    "eumetsat_mtg_li_netcdf_v1",
    "noaa_goes_glm_listing_v1",
    "noaa_goes_glm_netcdf_v1",
})
_LIGHTNING_CAPTURE_PROVENANCE_KINDS = frozenset({
    "eumetsat_mtg_li_exact_raw_v1",
    "noaa_goes_glm_exact_raw_v1",
})
_KMI_APPV4_RECEIPT_PROVENANCE_KINDS = frozenset({
    "kmi_appv4_forecast_response_v2",
    "kmi_appv4_image_response_v2",
})
_EMPTY_EXACT_RESPONSE_PROVENANCE_KINDS = frozenset({
    *_PUBLIC_SOURCE_PROVENANCE_KINDS,
    *_LIGHTNING_RECEIPT_PROVENANCE_KINDS,
    *_KMI_APPV4_RECEIPT_PROVENANCE_KINDS,
})


class CaptureRecoveryError(RuntimeError):
    """Base class for visible, fail-closed recovery failures."""


class CaptureRecoveryBusyError(CaptureRecoveryError):
    """A bounded catalog or ledger concurrency window did not settle."""


class CaptureRecoveryCapacityError(CaptureRecoveryError):
    """The pre-existing reservation cannot admit its own durable evidence."""


class CaptureRecoveryIntegrityError(CaptureRecoveryError):
    """Raw evidence, its reservation, or their exact binding is invalid."""


@dataclass(frozen=True, slots=True)
class RawRecoveryMemberBinding:
    """Closed transport/provenance contract for one capture role."""

    role: str
    raw_product: str
    receipt_suffix: str
    maximum_transport_bytes: int
    allowed_content_types: frozenset[str]
    provenance_kind: str
    receipt_attempt_limit: int | None = None


@dataclass(frozen=True, slots=True)
class RawRecoveryDynamicMemberBinding:
    """Bounded zero-based role family materialized from durable role names."""

    role_prefix: str
    role_index_width: int
    minimum_count: int
    maximum_count: int
    raw_product: str
    receipt_suffix_template: str
    maximum_transport_bytes: int
    allowed_content_types: frozenset[str]
    provenance_kind: str

    @property
    def role_pattern(self) -> str:
        if (
            self.role_prefix == "netcdf_"
            and self.role_index_width == 2
            and self.maximum_count == 12
        ):
            return r"^netcdf_(?P<index>0[0-9]|1[01])$"
        raise CaptureRecoveryIntegrityError(
            "dynamic raw role family has no closed pattern"
        )

    def role_for_index(self, index: int) -> str:
        if (
            isinstance(index, bool)
            or not isinstance(index, int)
            or not 0 <= index < self.maximum_count
        ):
            raise CaptureRecoveryIntegrityError(
                "dynamic raw role index is outside its closed bound"
            )
        return f"{self.role_prefix}{index:0{self.role_index_width}d}"

    def materialize(self, role: str) -> RawRecoveryMemberBinding:
        possible = tuple(
            self.role_for_index(index) for index in range(self.maximum_count)
        )
        if role not in possible:
            raise CaptureRecoveryIntegrityError(
                "dynamic raw role is outside its closed family"
            )
        return RawRecoveryMemberBinding(
            role=role,
            raw_product=self.raw_product,
            receipt_suffix=self.receipt_suffix_template.format(role=role),
            maximum_transport_bytes=self.maximum_transport_bytes,
            allowed_content_types=self.allowed_content_types,
            provenance_kind=self.provenance_kind,
        )


@dataclass(frozen=True, slots=True)
class RawRecoveryBinding:
    """Closed aggregate contract for one executable acquisition handler.

    ``members`` order is the exact activation ``required_roles`` order.  Role
    set digests are nevertheless sorted by role, so callers cannot influence a
    durable digest by changing mapping iteration order.
    """

    handler_id: str
    workload_id: str
    ledger_provider: str
    raw_provider: str
    capture_product: str
    capture_suffix: str
    members: tuple[RawRecoveryMemberBinding, ...]
    maximum_transport_bytes: int
    capture_provenance_kind: str
    job_identity_kind: str = "origin_v1"
    dynamic_member: RawRecoveryDynamicMemberBinding | None = None
    satellite: int | None = None

    @property
    def required_roles(self) -> tuple[str, ...]:
        return tuple(member.role for member in self.members)

    @property
    def maximum_artifacts(self) -> int:
        return len(self.members) + (
            0 if self.dynamic_member is None else self.dynamic_member.maximum_count
        )

    @property
    def minimum_artifacts(self) -> int:
        return len(self.members) + (
            0 if self.dynamic_member is None else self.dynamic_member.minimum_count
        )

    @property
    def dynamic_role_pattern(self) -> str | None:
        return (
            None
            if self.dynamic_member is None
            else self.dynamic_member.role_pattern
        )

    @property
    def dynamic_role_minimum(self) -> int:
        return 0 if self.dynamic_member is None else self.dynamic_member.minimum_count

    @property
    def dynamic_role_maximum(self) -> int:
        return 0 if self.dynamic_member is None else self.dynamic_member.maximum_count

    def members_for_roles(
        self, roles: object
    ) -> tuple[RawRecoveryMemberBinding, ...]:
        if isinstance(roles, Mapping):
            values = tuple(roles)
        elif isinstance(roles, (set, frozenset, tuple, list)):
            values = tuple(roles)
        else:
            raise CaptureRecoveryIntegrityError(
                "raw recovery role inventory is invalid"
            )
        if any(not isinstance(role, str) for role in values) or len(values) != len(
            set(values)
        ):
            raise CaptureRecoveryIntegrityError(
                "raw recovery role inventory is not unique"
            )
        actual = set(values)
        static = set(self.required_roles)
        if not static.issubset(actual):
            raise CaptureRecoveryIntegrityError(
                "raw recovery static role set drifted"
            )
        dynamic = actual - static
        if self.dynamic_member is None:
            if dynamic:
                raise CaptureRecoveryIntegrityError(
                    "raw recovery role is outside its closed binding"
                )
            return self.members
        template = self.dynamic_member
        count = len(dynamic)
        if not template.minimum_count <= count <= template.maximum_count:
            raise CaptureRecoveryIntegrityError(
                "raw recovery dynamic role count drifted"
            )
        expected = tuple(template.role_for_index(index) for index in range(count))
        if dynamic != set(expected):
            raise CaptureRecoveryIntegrityError(
                "raw recovery dynamic roles are not contiguous"
            )
        return self.members + tuple(template.materialize(role) for role in expected)

    def member(self, role: str) -> RawRecoveryMemberBinding:
        matches = tuple(member for member in self.members if member.role == role)
        if len(matches) == 1:
            return matches[0]
        if self.dynamic_member is not None:
            return self.dynamic_member.materialize(role)
        raise CaptureRecoveryIntegrityError(
            "raw recovery role is outside its closed binding"
        )

    def _single_member(self) -> RawRecoveryMemberBinding:
        if len(self.members) != 1:
            raise CaptureRecoveryIntegrityError(
                "single-member compatibility property used for a role set"
            )
        return self.members[0]

    # Compatibility views for the three existing single-role bindings. New
    # code must use ``members``/``required_roles``.
    @property
    def raw_product(self) -> str:
        return self._single_member().raw_product

    @property
    def role(self) -> str:
        return self._single_member().role

    @property
    def receipt_suffix(self) -> str:
        return self._single_member().receipt_suffix

    @property
    def allowed_content_types(self) -> frozenset[str]:
        return self._single_member().allowed_content_types

    @property
    def provenance_kind(self) -> str:
        return self._single_member().provenance_kind


@dataclass(frozen=True, slots=True)
class RecoveryEvidenceDigest:
    """Exact capacity-ledger evidence for one complete role set."""

    actual_bytes: int
    actual_entries: int
    content_sha256: str
    receipt_sha256: str
    capture_sha256: str


@dataclass(frozen=True, slots=True)
class RecoveredCaptureEvidence:
    capture_id: str
    actual_bytes: int
    actual_entries: int
    content_sha256: str
    receipt_sha256: str
    capture_sha256: str
    capacity_commit_sha256: str
    already_committed: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "actual_bytes": self.actual_bytes,
            "actual_entries": self.actual_entries,
            "already_committed": self.already_committed,
            "capacity_commit_sha256": self.capacity_commit_sha256,
            "capture_id": self.capture_id,
            "capture_sha256": self.capture_sha256,
            "content_sha256": self.content_sha256,
            "contract": CONTRACT,
            "receipt_sha256": self.receipt_sha256,
        }


@dataclass(frozen=True, slots=True)
class RecoveryAuditResult:
    receipt_count: int
    capture_count: int
    incomplete_receipt_count: int
    recovered_commit_count: int
    already_committed_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "already_committed_count": self.already_committed_count,
            "capture_count": self.capture_count,
            "contract": CONTRACT,
            "incomplete_receipt_count": self.incomplete_receipt_count,
            "receipt_count": self.receipt_count,
            "recovered_commit_count": self.recovered_commit_count,
        }


_RAINVIEWER_RECOVERY_MEMBERS = tuple(sorted(
    (
        RawRecoveryMemberBinding(
            role="manifest",
            raw_product="rainviewer_weather_maps_manifest_json",
            receipt_suffix=":manifest",
            maximum_transport_bytes=2 * 1024 * 1024,
            allowed_content_types=frozenset({"application/json", "text/json"}),
            provenance_kind="rainviewer_manifest_v1",
        ),
    )
    + tuple(
        RawRecoveryMemberBinding(
            role=f"{kind}_dx_{dx}_dy_{dy}",
            raw_product=(
                "rainviewer_radar_coverage_png"
                if kind == "coverage"
                else "rainviewer_composite_reflectivity_png"
            ),
            receipt_suffix=f":{kind}_dx_{dx}_dy_{dy}",
            maximum_transport_bytes=2 * 1024 * 1024,
            allowed_content_types=frozenset({"image/png"}),
            provenance_kind=(
                "rainviewer_coverage_tile_v1"
                if kind == "coverage"
                else "rainviewer_radar_tile_v1"
            ),
        )
        for kind in ("coverage", "radar")
        for dx in ("0", "m1", "p1")
        for dy in ("0", "m1", "p1")
    ),
    key=lambda item: item.role,
))


def _public_source_recovery_binding(
    *,
    handler_id: str,
    workload_id: str,
    provider: str,
    raw_product: str,
    capture_product: str,
    provenance_kind: str,
    maximum_transport_bytes: int,
) -> RawRecoveryBinding:
    return RawRecoveryBinding(
        handler_id=handler_id,
        workload_id=workload_id,
        ledger_provider=provider,
        raw_provider=provider,
        capture_product=capture_product,
        capture_suffix=":capture",
        members=(RawRecoveryMemberBinding(
            role="response_bytes",
            raw_product=raw_product,
            receipt_suffix=":response_bytes",
            maximum_transport_bytes=maximum_transport_bytes,
            allowed_content_types=frozenset({"application/octet-stream"}),
            provenance_kind=provenance_kind,
        ),),
        maximum_transport_bytes=maximum_transport_bytes,
        capture_provenance_kind=provenance_kind,
        job_identity_kind="safe_native_alias_v1",
    )


RECOVERY_BINDING_CATALOG = MappingProxyType({
    "aviationweather_metar_cache_exact_raw_v1": RawRecoveryBinding(
        handler_id="aviationweather_metar_cache_exact_raw_v1",
        workload_id="ground.metar.global",
        ledger_provider="aviationweather",
        raw_provider="metar",
        capture_product="aviationweather_current_metars_capture",
        capture_suffix=":capture",
        members=(RawRecoveryMemberBinding(
            role="csv_gzip",
            raw_product="aviationweather_current_metars_csv_gzip",
            receipt_suffix=":csv_gzip",
            maximum_transport_bytes=16 * 1024 * 1024,
            allowed_content_types=frozenset({
                "application/gzip",
                "application/octet-stream",
                "application/x-gzip",
            }),
            provenance_kind="metar_v2",
        ),),
        maximum_transport_bytes=16 * 1024 * 1024,
        capture_provenance_kind="metar_v2",
    ),
    "aemet_opendata_three_role_exact_raw_v1": RawRecoveryBinding(
        handler_id="aemet_opendata_three_role_exact_raw_v1",
        workload_id="radar.aemet.composite",
        ledger_provider="aemet",
        raw_provider="aemet_radar",
        capture_product="aemet_national_radar_opendata_exact_capture",
        capture_suffix=":capture",
        members=(
            RawRecoveryMemberBinding(
                role="opendata_data_response",
                raw_product="aemet_national_radar_opendata_data_response",
                receipt_suffix=":opendata_data_response",
                maximum_transport_bytes=96 * 1024 * 1024,
                allowed_content_types=frozenset({
                    "application/gzip",
                    "application/octet-stream",
                    "application/x-gzip",
                    "application/x-tar",
                    "application/zip",
                    "binary/octet-stream",
                    "image/tiff",
                }),
                provenance_kind="aemet_data_v1",
            ),
            RawRecoveryMemberBinding(
                role="opendata_envelope_json",
                raw_product="aemet_national_radar_opendata_envelope_json",
                receipt_suffix=":opendata_envelope_json",
                maximum_transport_bytes=64 * 1024,
                allowed_content_types=frozenset({"application/json"}),
                provenance_kind="aemet_envelope_v1",
            ),
            RawRecoveryMemberBinding(
                role="opendata_metadata_response",
                raw_product="aemet_national_radar_opendata_metadata_response",
                receipt_suffix=":opendata_metadata_response",
                maximum_transport_bytes=4 * 1024 * 1024,
                allowed_content_types=frozenset({
                    "application/json",
                    "application/octet-stream",
                    "text/plain",
                    "text/xml",
                }),
                provenance_kind="aemet_metadata_v1",
            ),
        ),
        maximum_transport_bytes=101 * 1024 * 1024,
        capture_provenance_kind="aemet_opendata_v1",
    ),
    "dwd_rv_tar_exact_raw_only_v1": RawRecoveryBinding(
        handler_id="dwd_rv_tar_exact_raw_only_v1",
        workload_id="radar.dwd.radolan_rv",
        ledger_provider="dwd",
        raw_provider="dwd",
        capture_product="dwd_open_data_composite_rv_raw_capture",
        capture_suffix=":capture",
        members=(RawRecoveryMemberBinding(
            role="rv_tar",
            raw_product="dwd_open_data_composite_rv_tar",
            receipt_suffix=":rv_tar",
            maximum_transport_bytes=8 * 1024 * 1024,
            allowed_content_types=frozenset({
                "application/octet-stream",
                "application/x-tar",
                "binary/octet-stream",
            }),
            provenance_kind="dwd_rv_v2",
        ),),
        maximum_transport_bytes=8 * 1024 * 1024,
        capture_provenance_kind="dwd_rv_v2",
    ),
    "knmi_rtcor_hdf5_exact_raw_v1": RawRecoveryBinding(
        handler_id="knmi_rtcor_hdf5_exact_raw_v1",
        workload_id="radar.knmi.rtcor",
        ledger_provider="knmi",
        raw_provider="knmi",
        capture_product="knmi_nl_rdr_data_rtcor_5m_capture",
        capture_suffix=":capture",
        members=(RawRecoveryMemberBinding(
            role="hdf5",
            raw_product="knmi_nl_rdr_data_rtcor_5m_hdf5",
            receipt_suffix=":hdf5",
            maximum_transport_bytes=40 * 1024 * 1024,
            allowed_content_types=frozenset({
                "application/octet-stream",
                "application/x-hdf5",
            }),
            provenance_kind="knmi_rtcor_v1",
        ),),
        maximum_transport_bytes=40 * 1024 * 1024,
        capture_provenance_kind="knmi_rtcor_v1",
    ),
    "kmi_aws_10min_wfs_exact_raw_v1": RawRecoveryBinding(
        handler_id="kmi_aws_10min_wfs_exact_raw_v1",
        workload_id="ground.kmi.aws_10min",
        ledger_provider="kmi",
        raw_provider="kmi",
        capture_product="kmi_aws_10min_wfs_exact_capture",
        capture_suffix=":capture",
        members=(RawRecoveryMemberBinding(
            role="geoserver_response",
            raw_product="kmi_aws_10min_wfs_exact_response",
            receipt_suffix=":response",
            maximum_transport_bytes=32 * 1024 * 1024,
            allowed_content_types=frozenset({
                "application/geo+json",
                "application/json",
                "application/octet-stream",
                "text/plain",
                "text/xml",
            }),
            provenance_kind="kmi_opendata_response_v1",
        ),),
        maximum_transport_bytes=32 * 1024 * 1024,
        capture_provenance_kind="kmi_opendata_v1",
    ),
    "kmi_public_radar_wcs_exact_raw_v1": RawRecoveryBinding(
        handler_id="kmi_public_radar_wcs_exact_raw_v1",
        workload_id="radar.kmi.public_wcs_unknown_quantity",
        ledger_provider="kmi",
        raw_provider="kmi",
        capture_product="kmi_public_radar_wcs_exact_capture",
        capture_suffix=":capture",
        members=(RawRecoveryMemberBinding(
            role="geoserver_response",
            raw_product="kmi_public_radar_wcs_exact_response",
            receipt_suffix=":response",
            maximum_transport_bytes=64 * 1024 * 1024,
            allowed_content_types=frozenset({
                "application/octet-stream",
                "binary/octet-stream",
                "image/geotiff",
                "image/tiff",
            }),
            provenance_kind="kmi_opendata_response_v1",
        ),),
        maximum_transport_bytes=64 * 1024 * 1024,
        capture_provenance_kind="kmi_opendata_v1",
    ),
    "kmi_appv4_visual_exact_raw_only_v1": RawRecoveryBinding(
        handler_id="kmi_appv4_visual_exact_raw_only_v1",
        workload_id="radar.kmi.appv4_visual",
        ledger_provider="kmi",
        raw_provider="kmi",
        capture_product="kmi_appv4_visualisation_capture",
        capture_suffix=":capture",
        members=(
            RawRecoveryMemberBinding(
                role="forecast_json",
                raw_product="kmi_appv4_forecast_manifest",
                receipt_suffix=":forecast_json",
                maximum_transport_bytes=4 * 1024 * 1024,
                allowed_content_types=frozenset({"application/octet-stream"}),
                provenance_kind="kmi_appv4_forecast_response_v2",
            ),
            RawRecoveryMemberBinding(
                role="radar_png",
                raw_product="kmi_appv4_visualisation_png",
                receipt_suffix=":radar_png",
                maximum_transport_bytes=8 * 1024 * 1024,
                allowed_content_types=frozenset({"application/octet-stream"}),
                provenance_kind="kmi_appv4_image_response_v2",
            ),
        ),
        maximum_transport_bytes=12 * 1024 * 1024,
        capture_provenance_kind="kmi_appv4_visual_capture_v2",
        job_identity_kind="safe_native_alias_v1",
    ),
    "opera_dbzh_hdf5_exact_raw_only_v1": RawRecoveryBinding(
        handler_id="opera_dbzh_hdf5_exact_raw_only_v1",
        workload_id="radar.opera.dbzh",
        ledger_provider="opera",
        raw_provider="opera",
        capture_product="eumetnet_opera_cirrus_dbzh_capture",
        capture_suffix=":capture",
        members=(RawRecoveryMemberBinding(
            role="hdf5",
            raw_product="eumetnet_opera_cirrus_dbzh_odim_hdf5",
            receipt_suffix=":hdf5",
            maximum_transport_bytes=384 * 1024 * 1024,
            allowed_content_types=frozenset({
                "application/octet-stream",
                "application/x-hdf5",
                "binary/octet-stream",
            }),
            provenance_kind="opera_stream_raw_v1",
        ),),
        maximum_transport_bytes=384 * 1024 * 1024,
        capture_provenance_kind="opera_stream_raw_v1",
    ),
    "dpc_sri_catalog_geotiff_exact_raw_v1": RawRecoveryBinding(
        handler_id="dpc_sri_catalog_geotiff_exact_raw_v1",
        workload_id="radar.dpc.sri",
        ledger_provider="dpc",
        raw_provider="dpc_radar",
        capture_product="dpc_sri_catalog_geotiff_capture",
        capture_suffix=":capture",
        members=(
            RawRecoveryMemberBinding(
                role="catalog_json",
                raw_product="dpc_sri_last_product_catalog_json",
                receipt_suffix=":catalog_json",
                maximum_transport_bytes=2 * 1024 * 1024,
                allowed_content_types=frozenset({"application/json", "text/json"}),
                provenance_kind="dpc_catalog_v1",
            ),
            RawRecoveryMemberBinding(
                role="geotiff",
                raw_product="dpc_sri_original_geotiff",
                receipt_suffix=":geotiff",
                maximum_transport_bytes=25 * 1024 * 1024,
                allowed_content_types=frozenset({
                    "application/octet-stream",
                    "binary/octet-stream",
                    "image/geotiff",
                    "image/tiff",
                }),
                provenance_kind="dpc_geotiff_v1",
            ),
        ),
        maximum_transport_bytes=32 * 1024 * 1024,
        capture_provenance_kind="dpc_sri_v1",
    ),
    "noaa_err_listing_netcdf4_exact_raw_v1": RawRecoveryBinding(
        handler_id="noaa_err_listing_netcdf4_exact_raw_v1",
        workload_id="satellite.noaa.err",
        ledger_provider="noaa",
        raw_provider="noaa_enterprise_rainrate",
        capture_product="noaa_err_listing_and_netcdf4_capture",
        capture_suffix=":capture",
        members=(
            RawRecoveryMemberBinding(
                role="listing_xml",
                raw_product="noaa_err_s3_listobjects_v2_xml",
                receipt_suffix=":listing_xml",
                maximum_transport_bytes=1 * 1024 * 1024,
                allowed_content_types=frozenset({"application/xml", "text/xml"}),
                provenance_kind="noaa_err_listing_v1",
            ),
            RawRecoveryMemberBinding(
                role="netcdf4",
                raw_product="noaa_err_instantaneous_glb5_netcdf4",
                receipt_suffix=":netcdf4",
                maximum_transport_bytes=32 * 1024 * 1024,
                allowed_content_types=frozenset({
                    "application/netcdf",
                    "application/octet-stream",
                    "application/x-netcdf",
                    "binary/octet-stream",
                }),
                provenance_kind="noaa_err_netcdf4_v1",
            ),
        ),
        maximum_transport_bytes=36 * 1024 * 1024,
        capture_provenance_kind="noaa_err_v1",
    ),
    "noaa_goes18_rrqpe_listings_netcdf4_exact_raw_v1": RawRecoveryBinding(
        handler_id="noaa_goes18_rrqpe_listings_netcdf4_exact_raw_v1",
        workload_id="satellite.goes18.rrqpe",
        ledger_provider="noaa_goes",
        raw_provider="noaa_goes_rrqpe",
        capture_product="noaa_goes_rrqpe_exact_native_capture",
        capture_suffix=":G18:capture",
        members=(
            RawRecoveryMemberBinding(
                role="listing_current",
                raw_product="noaa_goes_rrqpe_s3_listobjects_v2_xml",
                receipt_suffix=":G18:listing_current",
                maximum_transport_bytes=2 * 1024 * 1024,
                allowed_content_types=frozenset({"application/xml", "text/xml"}),
                provenance_kind="goes_rrqpe_listing_v1",
            ),
            RawRecoveryMemberBinding(
                role="listing_previous",
                raw_product="noaa_goes_rrqpe_s3_listobjects_v2_xml",
                receipt_suffix=":G18:listing_previous",
                maximum_transport_bytes=2 * 1024 * 1024,
                allowed_content_types=frozenset({"application/xml", "text/xml"}),
                provenance_kind="goes_rrqpe_listing_v1",
            ),
            RawRecoveryMemberBinding(
                role="netcdf4",
                raw_product="noaa_goes_rrqpe_abi_l2_original_netcdf4",
                receipt_suffix=":G18:netcdf4",
                maximum_transport_bytes=16 * 1024 * 1024,
                allowed_content_types=frozenset({
                    "application/netcdf",
                    "application/octet-stream",
                    "application/x-netcdf",
                    "binary/octet-stream",
                }),
                provenance_kind="goes_rrqpe_netcdf4_v1",
            ),
        ),
        maximum_transport_bytes=20 * 1024 * 1024,
        capture_provenance_kind="goes18_rrqpe_v1",
    ),
    "noaa_goes19_rrqpe_listings_netcdf4_exact_raw_v1": RawRecoveryBinding(
        handler_id="noaa_goes19_rrqpe_listings_netcdf4_exact_raw_v1",
        workload_id="satellite.goes19.rrqpe",
        ledger_provider="noaa_goes",
        raw_provider="noaa_goes_rrqpe",
        capture_product="noaa_goes_rrqpe_exact_native_capture",
        capture_suffix=":G19:capture",
        members=(
            RawRecoveryMemberBinding(
                role="listing_current",
                raw_product="noaa_goes_rrqpe_s3_listobjects_v2_xml",
                receipt_suffix=":G19:listing_current",
                maximum_transport_bytes=2 * 1024 * 1024,
                allowed_content_types=frozenset({"application/xml", "text/xml"}),
                provenance_kind="goes_rrqpe_listing_v1",
            ),
            RawRecoveryMemberBinding(
                role="listing_previous",
                raw_product="noaa_goes_rrqpe_s3_listobjects_v2_xml",
                receipt_suffix=":G19:listing_previous",
                maximum_transport_bytes=2 * 1024 * 1024,
                allowed_content_types=frozenset({"application/xml", "text/xml"}),
                provenance_kind="goes_rrqpe_listing_v1",
            ),
            RawRecoveryMemberBinding(
                role="netcdf4",
                raw_product="noaa_goes_rrqpe_abi_l2_original_netcdf4",
                receipt_suffix=":G19:netcdf4",
                maximum_transport_bytes=16 * 1024 * 1024,
                allowed_content_types=frozenset({
                    "application/netcdf",
                    "application/octet-stream",
                    "application/x-netcdf",
                    "binary/octet-stream",
                }),
                provenance_kind="goes_rrqpe_netcdf4_v1",
            ),
        ),
        maximum_transport_bytes=20 * 1024 * 1024,
        capture_provenance_kind="goes19_rrqpe_v1",
    ),
    "eumetsat_mtg_li_search_netcdf_exact_raw_v1": RawRecoveryBinding(
        handler_id="eumetsat_mtg_li_search_netcdf_exact_raw_v1",
        workload_id="lightning.eumetsat.li",
        ledger_provider="eumetsat",
        raw_provider="eumetsat",
        capture_product="eumetsat_mtg_li_exact_raw_capture",
        capture_suffix=":capture",
        members=(
            RawRecoveryMemberBinding(
                role="search_response",
                raw_product="eumetsat_mtg_li_search_response_bytes",
                receipt_suffix=":search_response",
                maximum_transport_bytes=4 * 1024 * 1024,
                allowed_content_types=frozenset({"application/octet-stream"}),
                provenance_kind="eumetsat_mtg_li_search_v1",
            ),
            RawRecoveryMemberBinding(
                role="netcdf4",
                raw_product="eumetsat_mtg_li_original_netcdf_response_bytes",
                receipt_suffix=":netcdf4",
                maximum_transport_bytes=10 * 1024 * 1024,
                allowed_content_types=frozenset({"application/octet-stream"}),
                provenance_kind="eumetsat_mtg_li_netcdf_v1",
            ),
        ),
        maximum_transport_bytes=14 * 1024 * 1024,
        capture_provenance_kind="eumetsat_mtg_li_exact_raw_v1",
        job_identity_kind="safe_native_alias_v1",
    ),
    "noaa_goes18_glm_listings_netcdf_exact_raw_v1": RawRecoveryBinding(
        handler_id="noaa_goes18_glm_listings_netcdf_exact_raw_v1",
        workload_id="lightning.goes18.glm",
        ledger_provider="noaa_goes",
        raw_provider="noaa_goes",
        capture_product="noaa_goes_glm_exact_raw_capture",
        capture_suffix=":capture",
        members=(
            RawRecoveryMemberBinding(
                role="listing_previous",
                raw_product="noaa_goes_glm_s3_listobjects_v2_response_bytes",
                receipt_suffix=":listing_previous",
                maximum_transport_bytes=4 * 1024 * 1024,
                allowed_content_types=frozenset({"application/octet-stream"}),
                provenance_kind="noaa_goes_glm_listing_v1",
            ),
            RawRecoveryMemberBinding(
                role="listing_current",
                raw_product="noaa_goes_glm_s3_listobjects_v2_response_bytes",
                receipt_suffix=":listing_current",
                maximum_transport_bytes=4 * 1024 * 1024,
                allowed_content_types=frozenset({"application/octet-stream"}),
                provenance_kind="noaa_goes_glm_listing_v1",
            ),
        ),
        maximum_transport_bytes=68 * 1024 * 1024,
        capture_provenance_kind="noaa_goes_glm_exact_raw_v1",
        job_identity_kind="safe_native_alias_v1",
        dynamic_member=RawRecoveryDynamicMemberBinding(
            role_prefix="netcdf_",
            role_index_width=2,
            minimum_count=1,
            maximum_count=12,
            raw_product="noaa_goes_glm_original_netcdf_response_bytes",
            receipt_suffix_template=":{role}",
            maximum_transport_bytes=5 * 1024 * 1024,
            allowed_content_types=frozenset({"application/octet-stream"}),
            provenance_kind="noaa_goes_glm_netcdf_v1",
        ),
        satellite=18,
    ),
    "noaa_goes19_glm_listings_netcdf_exact_raw_v1": RawRecoveryBinding(
        handler_id="noaa_goes19_glm_listings_netcdf_exact_raw_v1",
        workload_id="lightning.goes19.glm",
        ledger_provider="noaa_goes",
        raw_provider="noaa_goes",
        capture_product="noaa_goes_glm_exact_raw_capture",
        capture_suffix=":capture",
        members=(
            RawRecoveryMemberBinding(
                role="listing_previous",
                raw_product="noaa_goes_glm_s3_listobjects_v2_response_bytes",
                receipt_suffix=":listing_previous",
                maximum_transport_bytes=4 * 1024 * 1024,
                allowed_content_types=frozenset({"application/octet-stream"}),
                provenance_kind="noaa_goes_glm_listing_v1",
            ),
            RawRecoveryMemberBinding(
                role="listing_current",
                raw_product="noaa_goes_glm_s3_listobjects_v2_response_bytes",
                receipt_suffix=":listing_current",
                maximum_transport_bytes=4 * 1024 * 1024,
                allowed_content_types=frozenset({"application/octet-stream"}),
                provenance_kind="noaa_goes_glm_listing_v1",
            ),
        ),
        maximum_transport_bytes=68 * 1024 * 1024,
        capture_provenance_kind="noaa_goes_glm_exact_raw_v1",
        job_identity_kind="safe_native_alias_v1",
        dynamic_member=RawRecoveryDynamicMemberBinding(
            role_prefix="netcdf_",
            role_index_width=2,
            minimum_count=1,
            maximum_count=12,
            raw_product="noaa_goes_glm_original_netcdf_response_bytes",
            receipt_suffix_template=":{role}",
            maximum_transport_bytes=5 * 1024 * 1024,
            allowed_content_types=frozenset({"application/octet-stream"}),
            provenance_kind="noaa_goes_glm_netcdf_v1",
        ),
        satellite=19,
    ),
    "metoffice_uk_rate_listings_odim_exact_raw_v1": RawRecoveryBinding(
        handler_id="metoffice_uk_rate_listings_odim_exact_raw_v1",
        workload_id="radar.metoffice.uk_rate",
        ledger_provider="metoffice",
        raw_provider="met_office",
        capture_product="met_office_uk_radar_selection_capture",
        capture_suffix=":capture",
        members=(
            RawRecoveryMemberBinding(
                role="listing_current",
                raw_product="met_office_uk_radar_s3_listobjects_v2_xml",
                receipt_suffix=":listing_current",
                maximum_transport_bytes=2 * 1024 * 1024,
                allowed_content_types=frozenset({"application/xml", "text/xml"}),
                provenance_kind="metoffice_listing_v1",
            ),
            RawRecoveryMemberBinding(
                role="listing_previous",
                raw_product="met_office_uk_radar_s3_listobjects_v2_xml",
                receipt_suffix=":listing_previous",
                maximum_transport_bytes=2 * 1024 * 1024,
                allowed_content_types=frozenset({"application/xml", "text/xml"}),
                provenance_kind="metoffice_listing_v1",
            ),
            RawRecoveryMemberBinding(
                role="odim_hdf5",
                raw_product="met_office_uk_radar_rainrate_odim_hdf5",
                receipt_suffix=":odim_hdf5",
                maximum_transport_bytes=20 * 1024 * 1024,
                allowed_content_types=frozenset({
                    "application/octet-stream",
                    "application/x-hdf5",
                    "binary/octet-stream",
                }),
                provenance_kind="metoffice_odim_v1",
            ),
        ),
        maximum_transport_bytes=32 * 1024 * 1024,
        capture_provenance_kind="metoffice_uk_rate_v1",
    ),
    "meteofrance_dpradar_catalog_hdf5_exact_raw_v1": RawRecoveryBinding(
        handler_id="meteofrance_dpradar_catalog_hdf5_exact_raw_v1",
        workload_id="radar.meteofrance.dpradar",
        ledger_provider="meteofrance",
        raw_provider="meteofrance_radar",
        capture_product="meteofrance_dpradar_catalog_hdf5_capture",
        capture_suffix=":capture",
        members=(
            RawRecoveryMemberBinding(
                role="catalog_json",
                raw_product="meteofrance_dpradar_metro_lame_eau_catalog_json",
                receipt_suffix=":catalog_json",
                maximum_transport_bytes=512 * 1024,
                allowed_content_types=frozenset({
                    "application/hal+json",
                    "application/json",
                }),
                provenance_kind="meteofrance_catalog_v1",
            ),
            RawRecoveryMemberBinding(
                role="hdf5",
                raw_product="meteofrance_iprn20_metro_lame_eau_500m_hdf5",
                receipt_suffix=":hdf5",
                maximum_transport_bytes=40 * 1024 * 1024,
                allowed_content_types=frozenset({
                    "application/octet-stream",
                    "application/x-hdf",
                    "application/x-hdf5",
                    "binary/octet-stream",
                }),
                provenance_kind="meteofrance_hdf5_v1",
            ),
        ),
        maximum_transport_bytes=48 * 1024 * 1024,
        capture_provenance_kind="meteofrance_dpradar_v1",
    ),
    "hsaf_h40b_nlst_netcdf_gzip_exact_raw_v1": RawRecoveryBinding(
        handler_id="hsaf_h40b_nlst_netcdf_gzip_exact_raw_v1",
        workload_id="satellite.hsaf.h40b",
        ledger_provider="hsaf",
        raw_provider="hsaf_h40b",
        capture_product="hsaf_h40b_ftp_selection_netcdf_gzip_capture",
        capture_suffix=":capture",
        members=(
            RawRecoveryMemberBinding(
                role="listing_nlst",
                raw_product="hsaf_h40b_official_ftp_nlst",
                receipt_suffix=":listing_nlst",
                maximum_transport_bytes=2 * 1024 * 1024,
                allowed_content_types=frozenset({"text/plain"}),
                provenance_kind="hsaf_h40b_listing_v1",
            ),
            RawRecoveryMemberBinding(
                role="netcdf_gzip",
                raw_product="hsaf_h40b_pin_fci_original_netcdf_gzip",
                receipt_suffix=":netcdf_gzip",
                maximum_transport_bytes=64 * 1024 * 1024,
                allowed_content_types=frozenset({"application/gzip"}),
                provenance_kind="hsaf_h40b_netcdf_gzip_v1",
            ),
        ),
        maximum_transport_bytes=66 * 1024 * 1024,
        capture_provenance_kind="hsaf_h40b_v1",
    ),
    "netatmo_getpublicdata_exact_raw_v1": RawRecoveryBinding(
        handler_id="netatmo_getpublicdata_exact_raw_v1",
        workload_id="ground.netatmo.region",
        ledger_provider="netatmo",
        raw_provider="netatmo",
        capture_product="netatmo_getpublicdata_query_capture",
        capture_suffix=":capture",
        members=(RawRecoveryMemberBinding(
            role="response_json",
            raw_product="netatmo_getpublicdata_json_response",
            receipt_suffix=":response_json",
            maximum_transport_bytes=8 * 1024 * 1024,
            allowed_content_types=frozenset({"application/json"}),
            provenance_kind="netatmo_v2",
        ),),
        maximum_transport_bytes=8 * 1024 * 1024,
        capture_provenance_kind="netatmo_v2",
        job_identity_kind="safe_native_alias_v1",
    ),
    "synoptic_timeseries_exact_raw_v1": RawRecoveryBinding(
        handler_id="synoptic_timeseries_exact_raw_v1",
        workload_id="ground.synoptic.region",
        ledger_provider="synoptic",
        raw_provider="synoptic",
        capture_product="synoptic_timeseries_query_capture",
        capture_suffix=":capture",
        members=(RawRecoveryMemberBinding(
            role="response_json",
            raw_product="synoptic_timeseries_json_response",
            receipt_suffix=":response_json:attempt:",
            maximum_transport_bytes=16 * 1024 * 1024,
            allowed_content_types=frozenset({"application/json"}),
            provenance_kind="synoptic_v5",
            receipt_attempt_limit=8,
        ),),
        maximum_transport_bytes=16 * 1024 * 1024,
        capture_provenance_kind="synoptic_v5",
        job_identity_kind="safe_native_alias_v1",
    ),
    "rainviewer_visual_region_exact_raw_v1": RawRecoveryBinding(
        handler_id="rainviewer_visual_region_exact_raw_v1",
        workload_id="radar.rainviewer.visual",
        ledger_provider="rainviewer",
        raw_provider="rainviewer",
        capture_product="rainviewer_region_visual_capture",
        capture_suffix=":capture",
        members=_RAINVIEWER_RECOVERY_MEMBERS,
        maximum_transport_bytes=38 * 1024 * 1024,
        capture_provenance_kind="rainviewer_visual_v1",
        job_identity_kind="safe_native_alias_v1",
    ),
    "openmeteo_forecast_unverified_exact_raw_v1": (
        _public_source_recovery_binding(
            handler_id="openmeteo_forecast_unverified_exact_raw_v1",
            workload_id="model.openmeteo.region",
            provider="openmeteo",
            raw_product=(
                "openmeteo_forecast_native_unverified_response_bytes"
            ),
            capture_product=(
                "openmeteo_forecast_native_unverified_exact_capture"
            ),
            provenance_kind="openmeteo_unverified_response_v1",
            maximum_transport_bytes=8 * 1024 * 1024,
        )
    ),
    "openmeteo_italiameteo_icon2i_unverified_exact_raw_v1": (
        _public_source_recovery_binding(
            handler_id=(
                "openmeteo_italiameteo_icon2i_unverified_exact_raw_v1"
            ),
            workload_id="model.openmeteo.italiameteo_icon2i.region",
            provider="openmeteo",
            raw_product=(
                "openmeteo_italia_meteo_arpae_icon_2i_forecast_"
                "unverified_response_bytes"
            ),
            capture_product=(
                "openmeteo_italia_meteo_arpae_icon_2i_forecast_"
                "unverified_exact_capture"
            ),
            provenance_kind="openmeteo_icon2i_unverified_response_v1",
            maximum_transport_bytes=4 * 1024 * 1024,
        )
    ),
    "geosphere_inca_location_unverified_exact_raw_v1": (
        _public_source_recovery_binding(
            handler_id="geosphere_inca_location_unverified_exact_raw_v1",
            workload_id="validation.geosphere.location",
            provider="geosphere",
            raw_product=(
                "geosphere_inca_point_nowcast_validation_"
                "unverified_response_bytes"
            ),
            capture_product=(
                "geosphere_inca_point_nowcast_validation_"
                "unverified_exact_capture"
            ),
            provenance_kind="geosphere_inca_unverified_response_v1",
            maximum_transport_bytes=4 * 1024 * 1024,
        )
    ),
    "italiameteo_dpc_sri_catalog_unverified_exact_raw_v1": (
        _public_source_recovery_binding(
            handler_id=(
                "italiameteo_dpc_sri_catalog_unverified_exact_raw_v1"
            ),
            workload_id=(
                "validation.italiameteo.dpc_sri_historical_catalog"
            ),
            provider="italiameteo",
            raw_product=(
                "italiameteo_dpc_sri_historical_daily_catalog_"
                "unverified_response_bytes"
            ),
            capture_product=(
                "italiameteo_dpc_sri_historical_daily_catalog_"
                "unverified_exact_capture"
            ),
            provenance_kind="italiameteo_catalog_unverified_response_v1",
            maximum_transport_bytes=4 * 1024 * 1024,
        )
    ),
    "meteolux_location_weather_unverified_exact_raw_v1": (
        _public_source_recovery_binding(
            handler_id="meteolux_location_weather_unverified_exact_raw_v1",
            workload_id="validation.meteolux.location",
            provider="meteolux",
            raw_product=(
                "meteolux_location_weather_validation_"
                "unverified_response_bytes"
            ),
            capture_product=(
                "meteolux_location_weather_validation_"
                "unverified_exact_capture"
            ),
            provenance_kind="meteolux_weather_unverified_response_v1",
            maximum_transport_bytes=4 * 1024 * 1024,
        )
    ),
})


RECOVERY_BINDINGS = MappingProxyType({
    handler_id: RECOVERY_BINDING_CATALOG[handler_id]
    for handler_id in sorted({
        rule.handler_id
        for rule in ACTIVATION_RULES.values()
        if rule.executable and rule.handler_id in RECOVERY_BINDING_CATALOG
    })
})

LOCATION_SHADOW_RECOVERY_BINDINGS = MappingProxyType({
    str(rule.handler_id): RECOVERY_BINDING_CATALOG[str(rule.handler_id)]
    for rule in LOCATION_SHADOW_CANDIDATE_RULES.values()
})


def validate_recovery_binding_catalog(
    bindings: Mapping[str, RawRecoveryBinding],
) -> None:
    """Validate every active or dormant exact binding without activating it."""

    if not isinstance(bindings, Mapping):
        raise RuntimeError("capture recovery catalog must be a mapping")
    receipt_identities: set[tuple[str, str, str, int | None]] = set()
    capture_identities: set[tuple[str, str, str, int | None]] = set()
    identifier = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,255}$")
    content_type = re.compile(
        r"^[a-z0-9][a-z0-9.+-]{0,63}/[a-z0-9][a-z0-9.+-]{0,127}$"
    )
    for handler_id, binding in bindings.items():
        binding_texts = (
            handler_id,
            binding.handler_id,
            binding.workload_id,
            binding.ledger_provider,
            binding.raw_provider,
            binding.capture_product,
        )
        if (
            handler_id != binding.handler_id
            or not isinstance(binding.members, tuple)
            or not binding.members
            or any(
                not isinstance(value, str) or identifier.fullmatch(value) is None
                for value in binding_texts
            )
            or binding.job_identity_kind not in {
                "origin_v1", "safe_native_alias_v1"
            }
            or binding.capture_provenance_kind not in _CAPTURE_PROVENANCE_KINDS
            or (
                binding.satellite is not None
                and binding.satellite not in {18, 19}
            )
        ):
            raise RuntimeError("capture recovery binding identity is invalid")
        roles = binding.required_roles
        if len(set(roles)) != len(roles):
            raise RuntimeError("capture recovery binding has duplicate roles")
        dynamic_members: tuple[RawRecoveryMemberBinding, ...] = ()
        dynamic = binding.dynamic_member
        if dynamic is not None:
            if (
                dynamic.role_prefix != "netcdf_"
                or dynamic.role_index_width != 2
                or dynamic.minimum_count != 1
                or dynamic.maximum_count != 12
                or dynamic.receipt_suffix_template != ":{role}"
                or dynamic.role_pattern != r"^netcdf_(?P<index>0[0-9]|1[01])$"
                or dynamic.provenance_kind != "noaa_goes_glm_netcdf_v1"
                or binding.capture_provenance_kind
                != "noaa_goes_glm_exact_raw_v1"
                or binding.satellite not in {18, 19}
            ):
                raise RuntimeError("dynamic recovery member binding is invalid")
            dynamic_members = tuple(
                dynamic.materialize(dynamic.role_for_index(index))
                for index in range(dynamic.maximum_count)
            )
            if set(roles) & {member.role for member in dynamic_members}:
                raise RuntimeError("dynamic recovery roles overlap static roles")
        elif binding.satellite is not None:
            raise RuntimeError("satellite discriminator requires dynamic recovery")
        all_members = binding.members + dynamic_members
        if (
            isinstance(binding.maximum_transport_bytes, bool)
            or not isinstance(binding.maximum_transport_bytes, int)
            or binding.maximum_transport_bytes <= 0
            or sum(member.maximum_transport_bytes for member in all_members)
            > binding.maximum_transport_bytes
        ):
            raise RuntimeError("capture recovery aggregate transport bound is invalid")
        capture_identity = (
            binding.raw_provider,
            binding.capture_product,
            binding.capture_suffix,
            binding.satellite,
        )
        if capture_identity in capture_identities:
            raise RuntimeError("capture recovery capture identity is ambiguous")
        if (
            not binding.capture_suffix.startswith(":")
            or not 2 <= len(binding.capture_suffix) <= 128
            or any(ord(character) < 33 or ord(character) == 127
                   for character in binding.capture_suffix)
            or binding.capture_suffix in {
                member.receipt_suffix for member in all_members
            }
        ):
            raise RuntimeError("capture recovery capture suffix is invalid")
        capture_identities.add(capture_identity)
        for member in all_members:
            identity = (
                binding.raw_provider,
                member.raw_product,
                member.receipt_suffix,
                binding.satellite,
            )
            if (
                identity in receipt_identities
                or not member.receipt_suffix.startswith(":")
                or not 2 <= len(member.receipt_suffix) <= 128
                or any(ord(character) < 33 or ord(character) == 127
                       for character in member.receipt_suffix)
                or member.receipt_suffix == ":capture"
                or isinstance(member.maximum_transport_bytes, bool)
                or not isinstance(member.maximum_transport_bytes, int)
                or member.maximum_transport_bytes <= 0
                or not isinstance(member.allowed_content_types, frozenset)
                or not member.allowed_content_types
                or any(
                    not isinstance(value, str)
                    or content_type.fullmatch(value) is None
                    for value in member.allowed_content_types
                )
                or any(
                    not isinstance(value, str) or identifier.fullmatch(value) is None
                    for value in (member.role, member.raw_product)
                )
                or member.provenance_kind not in _RECEIPT_PROVENANCE_KINDS
                or (
                    member.receipt_attempt_limit is not None
                    and (
                        isinstance(member.receipt_attempt_limit, bool)
                        or not isinstance(member.receipt_attempt_limit, int)
                        or member.receipt_attempt_limit <= 0
                        or not member.receipt_suffix.endswith(":")
                    )
                )
            ):
                raise RuntimeError("capture recovery member binding is invalid")
            receipt_identities.add(identity)
        suffix_identities = (
            binding.capture_suffix,
            *(member.receipt_suffix for member in all_members),
        )
        if any(
            left != right and left.endswith(right)
            for left in suffix_identities
            for right in suffix_identities
        ):
            raise RuntimeError("capture recovery suffix identities overlap")
    for identities, label in (
        (receipt_identities, "receipt"),
        (capture_identities, "capture"),
    ):
        if any(
            left_provider == right_provider
            and left_product == right_product
            and left_suffix != right_suffix
            and left_suffix.endswith(right_suffix)
            and left_satellite == right_satellite
            for left_provider, left_product, left_suffix, left_satellite in identities
            for right_provider, right_product, right_suffix, right_satellite in identities
        ):
            raise RuntimeError(f"capture recovery {label} suffixes are ambiguous")


def validate_closed_recovery_bindings(
    bindings: Mapping[str, RawRecoveryBinding],
    activation_rules: Mapping[str, ActivationRule],
) -> None:
    """Prove a one-to-one closed binding for every executable handler.

    This is the stable extension boundary: a provider may be activated only in
    the same atomic change that adds its exact role/member binding and passes
    this validator. Unknown, missing and extra bindings all fail closed.
    """

    if not isinstance(activation_rules, Mapping):
        raise RuntimeError("capture recovery activation matrix must be a mapping")
    validate_recovery_binding_catalog(bindings)
    active = {
        rule.handler_id: rule
        for rule in activation_rules.values()
        if rule.executable
    }
    if (
        None in active
        or len(active) != sum(rule.executable for rule in activation_rules.values())
        or set(active) != set(bindings)
    ):
        raise RuntimeError("capture recovery bindings differ from the active matrix")
    for handler_id, binding in bindings.items():
        rule = active[handler_id]
        if (
            rule.workload_id != binding.workload_id
            or rule.provider_id != binding.ledger_provider
            or rule.required_roles != binding.required_roles
            or rule.dynamic_role_pattern != binding.dynamic_role_pattern
            or rule.dynamic_role_minimum != binding.dynamic_role_minimum
            or rule.dynamic_role_maximum != binding.dynamic_role_maximum
            or rule.maximum_transport_bytes != binding.maximum_transport_bytes
            or rule.maximum_artifacts != binding.maximum_artifacts
            or (
                binding.dynamic_member is not None
                and rule.expected_satellite_id != f"G{binding.satellite}"
            )
        ):
            raise RuntimeError("capture recovery binding drifted from activation")


def validate_dormant_recovery_bindings(
    bindings: Mapping[str, RawRecoveryBinding],
    candidate_rules: Mapping[str, ActivationRule],
) -> None:
    """Close every dormant rule to one catalog binding without activating it."""

    validate_recovery_binding_catalog(bindings)
    if not isinstance(candidate_rules, Mapping):
        raise RuntimeError("dormant recovery candidate matrix must be a mapping")
    by_handler = {
        rule.handler_id: rule
        for rule in candidate_rules.values()
        if rule.handler_id is not None
    }
    if (
        len(by_handler) != len(candidate_rules)
        or set(bindings) != set(by_handler)
    ):
        raise RuntimeError("dormant recovery bindings differ from candidates")
    for handler_id, binding in bindings.items():
        rule = by_handler[handler_id]
        if (
            rule.workload_id != binding.workload_id
            or rule.provider_id != binding.ledger_provider
            or rule.required_roles != binding.required_roles
            or rule.dynamic_role_pattern != binding.dynamic_role_pattern
            or rule.dynamic_role_minimum != binding.dynamic_role_minimum
            or rule.dynamic_role_maximum != binding.dynamic_role_maximum
            or rule.maximum_transport_bytes != binding.maximum_transport_bytes
            or rule.maximum_artifacts != binding.maximum_artifacts
            or binding.job_identity_kind != "safe_native_alias_v1"
        ):
            raise RuntimeError("dormant recovery binding drifted from candidate")


validate_recovery_binding_catalog(RECOVERY_BINDING_CATALOG)
validate_closed_recovery_bindings(RECOVERY_BINDINGS, ACTIVATION_RULES)
validate_dormant_recovery_bindings(
    LOCATION_SHADOW_RECOVERY_BINDINGS,
    LOCATION_SHADOW_CANDIDATE_RULES,
)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as err:
        raise CaptureRecoveryIntegrityError(
            "raw recovery metadata is not canonical JSON"
        ) from err


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise CaptureRecoveryIntegrityError(f"{label} is not canonical")
    return value


def _raw_identity_for_origin(binding: RawRecoveryBinding, job_id: str) -> str:
    if _JOB_ID.fullmatch(job_id) is None:
        raise CaptureRecoveryIntegrityError(
            "capacity reservation has no canonical acquisition job identity"
        )
    if binding.job_identity_kind == "origin_v1":
        return job_id
    if binding.job_identity_kind != "safe_native_alias_v1":
        raise CaptureRecoveryIntegrityError("raw recovery identity kind is unknown")
    from .acquisition_query_authority import safe_native_job_id

    try:
        return safe_native_job_id(job_id)
    except ValueError as err:
        raise CaptureRecoveryIntegrityError(
            "safe native identity cannot be derived from reservation"
        ) from err


def _raw_job_from_fetch_id(
    fetch_id: str,
    *,
    binding: RawRecoveryBinding,
    suffix: str,
    attempt_limit: int | None = None,
) -> str:
    if not isinstance(fetch_id, str):
        raise CaptureRecoveryIntegrityError("raw fetch identity is invalid")
    terminal = suffix
    if attempt_limit is not None:
        marker = fetch_id.rfind(suffix)
        attempt = fetch_id[marker + len(suffix):] if marker >= 0 else ""
        if (
            marker < 0
            or not attempt.isascii()
            or not attempt.isdigit()
            or attempt.startswith("0")
            or not 1 <= int(attempt) <= attempt_limit
        ):
            raise CaptureRecoveryIntegrityError(
                "raw fetch identity has an invalid bounded attempt suffix"
            )
        terminal = f"{suffix}{attempt}"
    if not fetch_id.endswith(terminal):
        raise CaptureRecoveryIntegrityError("raw fetch identity has a wrong role suffix")
    raw_job_id = fetch_id[:-len(terminal)]
    pattern = (
        _JOB_ID
        if binding.job_identity_kind == "origin_v1"
        else _SAFE_NATIVE_JOB_ID
    )
    if pattern.fullmatch(raw_job_id) is None:
        raise CaptureRecoveryIntegrityError(
            "raw fetch identity has no valid bound job identity"
        )
    return raw_job_id


def _receipt_matches_binding(
    receipt: RawArtifactReceipt,
    binding: RawRecoveryBinding,
    member: RawRecoveryMemberBinding,
) -> bool:
    if (
        receipt.provider != binding.raw_provider
        or receipt.product != member.raw_product
    ):
        return False
    try:
        _raw_job_from_fetch_id(
            receipt.fetch_id,
            binding=binding,
            suffix=member.receipt_suffix,
            attempt_limit=member.receipt_attempt_limit,
        )
    except CaptureRecoveryIntegrityError:
        return False
    return True


def _binding_member_for_receipt(
    receipt: RawArtifactReceipt,
    bindings: Mapping[str, RawRecoveryBinding],
) -> tuple[RawRecoveryBinding, RawRecoveryMemberBinding]:
    matches = tuple(
        (binding, member)
        for binding in bindings.values()
        for member in (
            binding.members
            + (
                ()
                if binding.dynamic_member is None
                else tuple(
                    binding.dynamic_member.materialize(
                        binding.dynamic_member.role_for_index(index)
                    )
                    for index in range(binding.dynamic_member.maximum_count)
                )
            )
        )
        if _receipt_matches_binding(receipt, binding, member)
        and (
            binding.satellite is None
            or receipt.provenance.get("selection_satellite")
            == binding.satellite
        )
    )
    if len(matches) != 1:
        raise CaptureRecoveryIntegrityError(
            "raw receipt is outside the closed recovery bindings"
        )
    return matches[0]


def _binding_for_capture(
    capture: RawArtifactCaptureReceipt,
    bindings: Mapping[str, RawRecoveryBinding],
) -> RawRecoveryBinding:
    matches = tuple(
        binding
        for binding in bindings.values()
        if capture.provider == binding.raw_provider
        and capture.product == binding.capture_product
        and capture.fetch_id.endswith(binding.capture_suffix)
    )
    if len(matches) != 1:
        raise CaptureRecoveryIntegrityError(
            "raw capture is outside the closed recovery bindings"
        )
    return matches[0]


def _capture_suffix_for_recovery(
    capture: RawArtifactCaptureReceipt,
    binding: RawRecoveryBinding,
) -> str:
    """Return the exact current or narrowly supported historical suffix.

    GOES-18 and GOES-19 originally shared the generic capture suffix while
    their member receipts already carried the satellite identity.  Retained
    captures remain unambiguous once the member-derived binding is known.
    """

    if capture.fetch_id.endswith(binding.capture_suffix):
        return binding.capture_suffix
    if (
        binding.capture_suffix in {":G18:capture", ":G19:capture"}
        and capture.fetch_id.endswith(":capture")
    ):
        return ":capture"
    if ":capture:" in capture.fetch_id:
        shape = "capture_revision"
    elif capture.fetch_id.endswith(":captured"):
        shape = "captured"
    elif capture.fetch_id.endswith(":capture"):
        shape = "plain_capture"
    else:
        shape = "other"
    raise CaptureRecoveryIntegrityError(
        f"active raw capture fetch suffix drifted:{binding.workload_id}:{shape}"
    )


_OPERA_CERTIFIED_CAPTURE_FETCH = re.compile(
    r"^(?P<raw_fetch>.+:capture):certified:(?P<binding>[0-9a-f]{64})$"
)


def _raw_captures_after_certified_derivative_audit(
    captures: tuple[RawArtifactCaptureReceipt, ...],
) -> tuple[RawArtifactCaptureReceipt, ...]:
    """Validate and exclude metadata-only OPERA certification manifests.

    Certification reuses the exact raw member receipts and adds no raw object
    bytes.  It is therefore catalog evidence, not a second provider capture.
    """

    raw = tuple(
        capture for capture in captures
        if ":certified:" not in capture.fetch_id
    )
    raw_by_identity = {
        (capture.provider, capture.product, capture.fetch_id): capture
        for capture in raw
    }
    if len(raw_by_identity) != len(raw):
        raise CaptureRecoveryIntegrityError(
            "raw capture catalog contains duplicate identities"
        )
    for capture in captures:
        if ":certified:" not in capture.fetch_id:
            continue
        match = _OPERA_CERTIFIED_CAPTURE_FETCH.fullmatch(capture.fetch_id)
        if (
            match is None
            or capture.provider != "opera"
            or capture.product != "eumetnet_opera_cirrus_dbzh_capture"
            or capture.product_timestamp is None
            or isinstance(capture.product_timestamp, bool)
            or not isinstance(capture.product_timestamp, (int, float))
            or not math.isfinite(float(capture.product_timestamp))
        ):
            raise CaptureRecoveryIntegrityError(
                "OPERA certified capture identity is invalid"
            )
        original = raw_by_identity.get((
            capture.provider,
            capture.product,
            match.group("raw_fetch"),
        ))
        binding_sha256 = match.group("binding")
        if (
            original is None
            or original.product_timestamp is not None
            or capture.members != original.members
            or capture.upstream_capture_id
            != (
                f"{original.upstream_capture_id}"
                f"|decoder_certification_sha256={binding_sha256}"
            )
        ):
            raise CaptureRecoveryIntegrityError(
                "OPERA certified capture does not bind its exact raw capture"
            )
    return raw


def _provenance_text(
    provenance: Mapping[str, Any],
    *,
    allow_kmi_credential_transport_none: bool = False,
    allowed_kmi_descriptor_endpoint: str | None = None,
    allowed_kmi_canonical_request_url: str | None = None,
) -> str:
    text = _canonical(provenance).decode("utf-8").lower()
    def inspect(value: Any, path: tuple[str, ...]) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                lowered = str(key).lower()
                allowed_url_field = (
                    path == ("canonical_request",)
                    and lowered == "url"
                    and isinstance(item, str)
                    and item == allowed_kmi_canonical_request_url
                )
                allowed_credential_transport = (
                    path == ()
                    and lowered == "credential_transport"
                    and item == "none"
                    and allow_kmi_credential_transport_none
                )
                if (
                    (
                        lowered in _FORBIDDEN_PROVENANCE_KEYS
                        and not allowed_url_field
                    )
                    or (
                        "credential" in lowered
                        and lowered not in {
                            "credentials_used",
                            "credential_query_persisted",
                            "credentials_persisted",
                        }
                        and not allowed_credential_transport
                    )
                    or "signature" in lowered
                ):
                    raise CaptureRecoveryIntegrityError(
                        "raw provenance contains a forbidden secret field"
                    )
                if lowered in {
                    "credentials_used",
                    "credential_query_persisted",
                    "credentials_persisted",
                } and item is not False:
                    raise CaptureRecoveryIntegrityError(
                        "raw provenance claims credential use or persistence"
                    )
                inspect(item, (*path, lowered))
            return
        if isinstance(value, (list, tuple)):
            for item in value:
                inspect(item, path)
            return
        if isinstance(value, str):
            lowered = value.lower()
            if any(fragment in lowered for fragment in _FORBIDDEN_PROVENANCE_VALUES):
                raise CaptureRecoveryIntegrityError(
                    "raw provenance contains a forbidden secret value"
                )
            allowed_url_value = (
                path == ("canonical_request", "url")
                and value == allowed_kmi_canonical_request_url
            )
            allowed_endpoint_value = (
                path == ("descriptor", "endpoint")
                and value == allowed_kmi_descriptor_endpoint
            )
            if (
                "://" in value
                and value not in _ALLOWED_PUBLIC_PROVENANCE_LOCATORS
                and not allowed_url_value
                and not allowed_endpoint_value
            ):
                raise CaptureRecoveryIntegrityError(
                    "raw provenance contains an unbound transport locator"
                )

    inspect(provenance, ())
    return text


def _bounded_optional_text(value: Any, name: str, maximum: int = 2048) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise CaptureRecoveryIntegrityError(f"raw provenance {name} is invalid")
    return value


def _finite_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CaptureRecoveryIntegrityError(f"raw provenance {name} is not numeric")
    result = float(value)
    if not math.isfinite(result):
        raise CaptureRecoveryIntegrityError(f"raw provenance {name} is not finite")
    return result


def _validate_http_metadata(
    value: Any,
    *,
    receipt: RawArtifactReceipt,
    carries_lengths: bool,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CaptureRecoveryIntegrityError("raw provenance HTTP metadata is absent")
    common = {
        "etag", "last_modified", "request_id", "request_id_2",
        "server_side_encryption", "version_id",
    }
    expected = common | (
        {"content_type", "content_length", "declared_content_length"}
        if carries_lengths else set()
    )
    if set(value) != expected:
        raise CaptureRecoveryIntegrityError("raw provenance HTTP metadata drifted")
    for key in common:
        if key == "last_modified" and not carries_lengths:
            item = value.get(key)
            if item is not None:
                _finite_number(item, "HTTP last_modified")
        else:
            _bounded_optional_text(value.get(key), f"HTTP {key}")
    if carries_lengths and (
        value.get("content_type") != receipt.content_type
        or value.get("content_length") != receipt.size_bytes
        or value.get("declared_content_length") not in {None, receipt.size_bytes}
    ):
        raise CaptureRecoveryIntegrityError("raw provenance HTTP size drifted")
    return value


def _dpc_product_timestamp_ms(key: str) -> int:
    match = _DPC_KEY.fullmatch(key)
    if match is None:
        raise CaptureRecoveryIntegrityError("DPC GeoTIFF key is not canonical")
    try:
        moment = datetime(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
            int(match.group("hour")),
            int(match.group("minute")),
            tzinfo=timezone.utc,
        )
    except ValueError as err:
        raise CaptureRecoveryIntegrityError("DPC GeoTIFF key time is invalid") from err
    return int(moment.timestamp() * 1000)


def _satellite_for_binding(binding: RawRecoveryBinding) -> int:
    if binding.capture_provenance_kind == "goes18_rrqpe_v1":
        return 18
    if binding.capture_provenance_kind == "goes19_rrqpe_v1":
        return 19
    raise CaptureRecoveryIntegrityError("GOES recovery binding has no satellite")


def _public_source_spec_for_binding(
    binding: RawRecoveryBinding,
    member: RawRecoveryMemberBinding,
):
    from . import public_source_raw_capture

    spec = public_source_raw_capture.HANDLER_SPECS_BY_WORKLOAD.get(
        binding.workload_id
    )
    if (
        spec is None
        or spec.handler_id != binding.handler_id
        or spec.provider != binding.raw_provider
        or spec.provider != binding.ledger_provider
        or spec.capture_product != binding.capture_product
        or spec.capture_fetch_suffix != binding.capture_suffix
        or spec.role != member.role
        or spec.raw_product != member.raw_product
        or spec.fetch_suffix != member.receipt_suffix
        or spec.maximum_transport_bytes != member.maximum_transport_bytes
        or spec.maximum_transport_bytes != binding.maximum_transport_bytes
        or spec.provenance_kind != member.provenance_kind
        or spec.provenance_kind != binding.capture_provenance_kind
    ):
        raise CaptureRecoveryIntegrityError(
            "public-source recoverybinding drifted from native contract"
        )
    return public_source_raw_capture, spec


def _validate_provenance(
    receipt: RawArtifactReceipt,
    binding: RawRecoveryBinding,
    member: RawRecoveryMemberBinding,
) -> str | None:
    provenance = receipt.provenance
    kmi_opendata_descriptor = None
    kmi_opendata_job_id = None
    allowed_kmi_descriptor_endpoint = None
    allowed_kmi_canonical_request_url = None
    if member.provenance_kind == "kmi_opendata_response_v1":
        from . import kmi_opendata_raw

        if member.role != "geoserver_response":
            raise CaptureRecoveryIntegrityError(
                "KMI OpenData recovery role drifted"
            )
        kmi_opendata_job_id = _raw_job_from_fetch_id(
            receipt.fetch_id,
            binding=binding,
            suffix=member.receipt_suffix,
            attempt_limit=member.receipt_attempt_limit,
        )
        try:
            kmi_opendata_descriptor = (
                kmi_opendata_raw.validate_kmi_opendata_receipt_for_recovery(
                    receipt,
                    job_id=kmi_opendata_job_id,
                )
            )
            allowed_kmi_canonical_request_url = (
                kmi_opendata_raw.canonical_request(
                    kmi_opendata_descriptor
                ).url
            )
            allowed_kmi_descriptor_endpoint = (
                kmi_opendata_descriptor.endpoint
            )
        except kmi_opendata_raw.KmiOpenDataError as err:
            raise CaptureRecoveryIntegrityError(
                "KMI OpenData receipt provenance drifted"
            ) from err
    _provenance_text(
        provenance,
        allow_kmi_credential_transport_none=(
            kmi_opendata_descriptor is not None
        ),
        allowed_kmi_descriptor_endpoint=allowed_kmi_descriptor_endpoint,
        allowed_kmi_canonical_request_url=(
            allowed_kmi_canonical_request_url
        ),
    )
    common = {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
    }
    if any(provenance.get(key) != value for key, value in common.items()):
        raise CaptureRecoveryIntegrityError("raw provenance is not pre-decode native")
    if member.provenance_kind == "opera_stream_raw_v1":
        from . import opera_native

        if member.role != "hdf5":
            raise CaptureRecoveryIntegrityError(
                "OPERA recovery role/provenance binding drifted"
            )
        job_id = _raw_job_from_fetch_id(
            receipt.fetch_id,
            binding=binding,
            suffix=member.receipt_suffix,
            attempt_limit=member.receipt_attempt_limit,
        )
        try:
            context = opera_native.validate_opera_raw_only_receipt_for_recovery(
                receipt,
                job_id=job_id,
            )
        except (TypeError, ValueError) as err:
            raise CaptureRecoveryIntegrityError(
                "OPERA exact receipt provenance drifted"
            ) from err
        _sha256(
            context.get("revision_token"),
            "OPERA upstream revision token",
        )
        upstream_id = context.get("upstream_id")
        if not isinstance(upstream_id, str) or upstream_id != receipt.upstream_id:
            raise CaptureRecoveryIntegrityError(
                "OPERA upstream receipt identity drifted"
            )
        return upstream_id
    if member.provenance_kind in _KMI_APPV4_RECEIPT_PROVENANCE_KINDS:
        from . import kmi_appv4_raw_capture

        job_id = _raw_job_from_fetch_id(
            receipt.fetch_id,
            binding=binding,
            suffix=member.receipt_suffix,
            attempt_limit=member.receipt_attempt_limit,
        )
        expected_kind = {
            "forecast_json": "kmi_appv4_forecast_response_v2",
            "radar_png": "kmi_appv4_image_response_v2",
        }
        if expected_kind.get(member.role) != member.provenance_kind:
            raise CaptureRecoveryIntegrityError(
                "KMI app-v4 recovery role/provenance binding drifted"
            )
        try:
            return kmi_appv4_raw_capture.validate_kmi_appv4_receipt_for_raw_recovery(
                receipt,
                job_id=job_id,
                role=member.role,
            )
        except kmi_appv4_raw_capture.KmiAppv4RawError as err:
            raise CaptureRecoveryIntegrityError(
                "KMI app-v4 exact receipt provenance drifted"
            ) from err
    if member.provenance_kind in _LIGHTNING_RECEIPT_PROVENANCE_KINDS:
        from . import lightning_exact_raw

        job_id = _raw_job_from_fetch_id(
            receipt.fetch_id,
            binding=binding,
            suffix=member.receipt_suffix,
            attempt_limit=member.receipt_attempt_limit,
        )
        base_keys = {
            "access_class", "authentication_material_persisted",
            "capture_stage", "collector", "collector_version",
            "content_encoding", "declared_length", "endpoint_host",
            "endpoint_path_class", "final_url_sha256", "legal_advice",
            "locator_sha256", "meteorological_parse_attempted_before_store",
            "operational_admission", "provider", "provider_rights_claim",
            "provenance_kind", "reported_media_type", "representation",
            "request_sha256", "response_headers_sha256", "response_status",
            "retention_right_claim", "role", "semantic_status",
            "transport_sha256", "weather_state",
        }
        extras: set[str] = set()
        if member.provenance_kind == "noaa_goes_glm_listing_v1":
            extras = {"selection_prefix", "selection_satellite"}
        elif member.provenance_kind == "noaa_goes_glm_netcdf_v1":
            extras = {"object_key_sha256", "selection_satellite"}
        if set(provenance) != base_keys | extras:
            raise CaptureRecoveryIntegrityError(
                "lightning exact transport provenance drifted"
            )
        request_sha = _sha256(
            provenance.get("request_sha256"), "lightning request SHA-256"
        )
        locator_sha = _sha256(
            provenance.get("locator_sha256"), "lightning locator SHA-256"
        )
        if member.provenance_kind == "eumetsat_mtg_li_search_v1":
            expected = (
                "search_response", lightning_exact_raw.EUMETSAT_SEARCH_HOST,
                lightning_exact_raw.EUMETSAT_SEARCH_PATH,
                "public_anonymous_no_secret",
                lightning_exact_raw.EUMETSAT_SEARCH_MAX_BYTES,
            )
        elif member.provenance_kind == "eumetsat_mtg_li_netcdf_v1":
            expected = (
                "netcdf4", lightning_exact_raw.EUMETSAT_SEARCH_HOST,
                lightning_exact_raw.EUMETSAT_DOWNLOAD_PATH_PREFIX
                + "authorized_netcdf_entry",
                "oauth_bearer_ephemeral_child_only",
                lightning_exact_raw.EUMETSAT_NETCDF_MAX_BYTES,
            )
        else:
            satellite = binding.satellite
            if satellite not in {18, 19} or provenance.get(
                "selection_satellite"
            ) != satellite:
                raise CaptureRecoveryIntegrityError(
                    "lightning satellite provenance drifted"
                )
            if member.provenance_kind == "noaa_goes_glm_listing_v1":
                prefix = provenance.get("selection_prefix")
                if (
                    not isinstance(prefix, str)
                    or lightning_exact_raw._GOES_PREFIX_RE.fullmatch(prefix)  # noqa: SLF001
                    is None
                ):
                    raise CaptureRecoveryIntegrityError(
                        "lightning listing prefix drifted"
                    )
                expected = (
                    member.role, lightning_exact_raw._goes_host(satellite),  # noqa: SLF001
                    "s3_listobjects_v2", "public_anonymous_no_secret",
                    lightning_exact_raw.GOES_MAX_LISTING_BYTES,
                )
            else:
                _sha256(
                    provenance.get("object_key_sha256"),
                    "lightning object-key SHA-256",
                )
                expected = (
                    member.role, lightning_exact_raw._goes_host(satellite),  # noqa: SLF001
                    f"/{lightning_exact_raw.GOES_PREFIX}/native_netcdf",
                    "public_anonymous_no_secret",
                    lightning_exact_raw.GOES_MAX_NETCDF_BYTES,
                )
        role, host, path_class, access_class, maximum_bytes = expected
        if (
            member.role != role
            or provenance.get("endpoint_host") != host
            or provenance.get("endpoint_path_class") != path_class
            or provenance.get("access_class") != access_class
        ):
            raise CaptureRecoveryIntegrityError(
                "lightning role/request authority drifted"
            )
        request_binding = lightning_exact_raw._RequestBinding(  # noqa: SLF001
            "", request_sha, host, path_class, access_class
        )
        try:
            lightning_exact_raw._validate_receipt(  # noqa: SLF001
                receipt,
                job_id=job_id,
                provider=binding.raw_provider,
                product=member.raw_product,
                role=member.role,
                provenance_kind=member.provenance_kind,
                binding=request_binding,
                locator_sha256=locator_sha,
                maximum_bytes=maximum_bytes,
            )
        except lightning_exact_raw.LightningRawError as err:
            raise CaptureRecoveryIntegrityError(
                "lightning exact receipt provenance drifted"
            ) from err
        return _sha256(
            provenance.get("transport_sha256"),
            "lightning transport SHA-256",
        )
    if member.provenance_kind in _PUBLIC_SOURCE_PROVENANCE_KINDS:
        public_source, spec = _public_source_spec_for_binding(binding, member)
        job_id = _raw_job_from_fetch_id(
            receipt.fetch_id,
            binding=binding,
            suffix=member.receipt_suffix,
            attempt_limit=member.receipt_attempt_limit,
        )
        request_sha256 = provenance.get("request_sha256")
        query_count = provenance.get("query_count")
        if (
            not isinstance(request_sha256, str)
            or _SHA256.fullmatch(request_sha256) is None
            or isinstance(query_count, bool)
            or not isinstance(query_count, int)
            or not 0 <= query_count <= public_source.MAX_LOCATION_QUERIES
        ):
            raise CaptureRecoveryIntegrityError(
                "public-source requestbinding is invalid"
            )
        request_binding = public_source._RequestBinding(  # noqa: SLF001
            "", request_sha256, query_count
        )
        try:
            return public_source._validate_receipt(  # noqa: SLF001
                spec,
                request_binding,
                job_id,
                receipt,
            )
        except public_source.PublicSourceRawError as err:
            raise CaptureRecoveryIntegrityError(
                "public-source receipt provenance drifted"
            ) from err
    if member.provenance_kind == "kmi_opendata_response_v1":
        if (
            kmi_opendata_descriptor is None
            or kmi_opendata_job_id is None
        ):  # pragma: no cover - established before generic secret scanning
            raise CaptureRecoveryIntegrityError(
                "KMI OpenData receipt provenance drifted"
            )
        return kmi_opendata_descriptor.descriptor_id
    if member.provenance_kind in {
        "aemet_data_v1",
        "aemet_envelope_v1",
        "aemet_metadata_v1",
    }:
        from . import aemet_native

        expected_kind = {
            "opendata_data_response": "aemet_data_v1",
            "opendata_envelope_json": "aemet_envelope_v1",
            "opendata_metadata_response": "aemet_metadata_v1",
        }
        if expected_kind.get(member.role) != member.provenance_kind:
            raise CaptureRecoveryIntegrityError(
                "AEMET recovery role/provenance binding drifted"
            )
        if member.role == "opendata_envelope_json":
            expected = aemet_native._provenance(role=member.role)  # noqa: SLF001
            if provenance != expected:
                raise CaptureRecoveryIntegrityError(
                    "AEMET envelope provenance drifted"
                )
            return None
        selection = _sha256(
            provenance.get("selection_url_sha256"),
            "AEMET selection SHA-256",
        )
        _bounded_optional_text(provenance.get("etag"), "AEMET ETag", 256)
        _bounded_optional_text(
            provenance.get("last_modified"), "AEMET Last-Modified", 128
        )
        response = aemet_native.AemetHttpResponse(
            payload=b"",
            content_type=receipt.content_type,
            received_at=receipt.received_at,
            content_length=receipt.size_bytes,
            etag=provenance.get("etag"),
            last_modified=provenance.get("last_modified"),
        )
        expected = aemet_native._provenance(  # noqa: SLF001
            role=member.role,
            response=response,
            selection_sha256=selection,
        )
        if provenance != expected:
            raise CaptureRecoveryIntegrityError(
                "AEMET selected-response provenance drifted"
            )
        return selection
    if member.provenance_kind in {
        "meteofrance_catalog_v1",
        "meteofrance_hdf5_v1",
    }:
        from . import meteofrance_native

        job_id = _raw_job_from_fetch_id(
            receipt.fetch_id,
            binding=binding,
            suffix=member.receipt_suffix,
            attempt_limit=member.receipt_attempt_limit,
        )
        try:
            if member.provenance_kind == "meteofrance_catalog_v1":
                if member.role != "catalog_json":
                    raise CaptureRecoveryIntegrityError(
                        "Meteo-France catalog role drifted"
                    )
                meteofrance_native._validate_catalog_receipt(  # noqa: SLF001
                    receipt, job_id=job_id
                )
                return None
            if member.role != "hdf5":
                raise CaptureRecoveryIntegrityError(
                    "Meteo-France HDF5 role drifted"
                )
            identity = meteofrance_native.MeteoFranceProductIdentity(
                filename=provenance.get("filename"),
                product_reference=provenance.get("product_reference"),
                product_timestamp=provenance.get("product_timestamp"),
            )
            meteofrance_native._validate_hdf5_receipt(  # noqa: SLF001
                receipt, job_id=job_id, identity=identity
            )
        except meteofrance_native.MeteoFranceNativeError as err:
            raise CaptureRecoveryIntegrityError(
                "Meteo-France raw provenance drifted"
            ) from err
        return identity.filename
    if member.provenance_kind in {
        "hsaf_h40b_listing_v1",
        "hsaf_h40b_netcdf_gzip_v1",
    }:
        from . import hsaf_h40b_native

        job_id = _raw_job_from_fetch_id(
            receipt.fetch_id,
            binding=binding,
            suffix=member.receipt_suffix,
            attempt_limit=member.receipt_attempt_limit,
        )
        response = hsaf_h40b_native.HsafFtpResponse(
            payload=b"",
            received_at=receipt.received_at,
            completion=provenance.get("ftp_completion"),
            declared_size=provenance.get("ftp_declared_size"),
        )
        try:
            if member.provenance_kind == "hsaf_h40b_listing_v1":
                if member.role != "listing_nlst":
                    raise CaptureRecoveryIntegrityError(
                        "H SAF listing role drifted"
                    )
                expected = hsaf_h40b_native._listing_provenance(  # noqa: SLF001
                    response
                )
                if provenance != expected:
                    raise CaptureRecoveryIntegrityError(
                        "H SAF listing provenance drifted"
                    )
                hsaf_h40b_native._validate_listing_receipt(  # noqa: SLF001
                    receipt, job_id=job_id
                )
                return None

            if member.role != "netcdf_gzip":
                raise CaptureRecoveryIntegrityError(
                    "H SAF NetCDF-gzip role drifted"
                )
            basename = provenance.get("basename")
            remote_path = provenance.get("remote_path")
            nominal_slot = provenance.get("nominal_slot_time")
            if not isinstance(basename, str) or not isinstance(remote_path, str):
                raise CaptureRecoveryIntegrityError(
                    "H SAF product identity is incomplete"
                )
            canonical_slot = hsaf_h40b_native._slot_from_basename(  # noqa: SLF001
                basename
            )
            if (
                remote_path
                != f"{hsaf_h40b_native.FTP_DIRECTORY}/{basename}"
                or nominal_slot != canonical_slot
            ):
                raise CaptureRecoveryIntegrityError(
                    "H SAF product path or slot drifted"
                )
            identity = hsaf_h40b_native.HsafListingIdentity(
                selected_basename=basename,
                selected_remote_path=remote_path,
                nominal_slot_time=canonical_slot,
                candidate_count=1,
                first_slot_time=canonical_slot,
                last_slot_time=canonical_slot,
            )
            expected = hsaf_h40b_native._product_provenance(  # noqa: SLF001
                response, identity
            )
            if provenance != expected:
                raise CaptureRecoveryIntegrityError(
                    "H SAF NetCDF-gzip provenance drifted"
                )
            hsaf_h40b_native._validate_product_receipt(  # noqa: SLF001
                receipt, job_id=job_id, identity=identity
            )
        except hsaf_h40b_native.HsafH40bNativeError as err:
            raise CaptureRecoveryIntegrityError(
                "H SAF raw provenance drifted"
            ) from err
        return basename
    if member.provenance_kind == "netatmo_v2":
        from . import netatmo_native

        context = provenance.get("query_context")
        attempt = provenance.get("transport_attempt")
        if type(context) is not dict or (
            isinstance(attempt, bool)
            or not isinstance(attempt, int)
            or not 1 <= attempt <= netatmo_native.MAX_TRANSPORT_ATTEMPTS
            or provenance != netatmo_native._native_provenance(  # noqa: SLF001
                context, transport_attempt=attempt
            )
        ):
            raise CaptureRecoveryIntegrityError("Netatmo raw provenance drifted")
        return netatmo_native._query_context_sha256(context)  # noqa: SLF001
    if member.provenance_kind == "synoptic_v5":
        from . import synoptic_native

        context = provenance.get("query_context")
        attempt = provenance.get("request_attempt")
        if type(context) is not dict or (
            isinstance(attempt, bool)
            or not isinstance(attempt, int)
            or not 1 <= attempt <= synoptic_native.MAX_REQUEST_ATTEMPTS_PER_JOB
            or provenance != synoptic_native._native_provenance(  # noqa: SLF001
                context, request_attempt=attempt
            )
        ):
            raise CaptureRecoveryIntegrityError("Synoptic raw provenance drifted")
        return synoptic_native._query_context_sha256(context)  # noqa: SLF001
    if member.provenance_kind == "rainviewer_manifest_v1":
        from . import rainviewer_native

        if provenance != rainviewer_native._manifest_provenance():  # noqa: SLF001
            raise CaptureRecoveryIntegrityError(
                "RainViewer manifest provenance drifted"
            )
        return None
    if member.provenance_kind in {
        "rainviewer_coverage_tile_v1", "rainviewer_radar_tile_v1"
    }:
        from . import rainviewer_native

        kind = (
            "coverage"
            if member.provenance_kind == "rainviewer_coverage_tile_v1"
            else "radar"
        )
        expected_role = f"{kind}_tile"
        frame_path = provenance.get("frame_path")
        frame_timestamp = provenance.get("frame_generation_timestamp")
        x = provenance.get("x")
        y = provenance.get("y")
        latitude = provenance.get("request_center_lat")
        longitude = provenance.get("request_center_lon")
        horizon = provenance.get("request_horizon_km")
        if (
            provenance.get("response_role") != expected_role
            or isinstance(x, bool)
            or not isinstance(x, int)
            or isinstance(y, bool)
            or not isinstance(y, int)
            or not 0 <= x < 2**rainviewer_native.RAINVIEWER_ZOOM
            or not 0 <= y < 2**rainviewer_native.RAINVIEWER_ZOOM
            or not isinstance(frame_timestamp, (int, float))
            or isinstance(frame_timestamp, bool)
            or not math.isfinite(float(frame_timestamp))
            or (
                kind == "radar"
                and (not isinstance(frame_path, str) or not frame_path)
            )
            or (kind == "coverage" and frame_path is not None)
            or not isinstance(latitude, (int, float))
            or isinstance(latitude, bool)
            or not math.isfinite(float(latitude))
            or not -90.0 <= float(latitude) <= 90.0
            or not isinstance(longitude, (int, float))
            or isinstance(longitude, bool)
            or not math.isfinite(float(longitude))
            or not -180.0 <= float(longitude) <= 180.0
            or not isinstance(horizon, (int, float))
            or isinstance(horizon, bool)
            or not math.isfinite(float(horizon))
            or not 0.0 < float(horizon)
        ):
            raise CaptureRecoveryIntegrityError(
                "RainViewer tile provenance identity is invalid"
            )
        identity = type("_RainviewerIdentity", (), {
            "frame_path": frame_path,
            "frame_generation_timestamp": frame_timestamp,
        })()
        expected = rainviewer_native._tile_provenance(  # noqa: SLF001
            identity,
            kind=kind,
            x=x,
            y=y,
            center_lat=float(latitude),
            center_lon=float(longitude),
            horizon_km=float(horizon),
        )
        if provenance != expected:
            raise CaptureRecoveryIntegrityError("RainViewer tile provenance drifted")
        if kind == "coverage":
            return (
                f"rainviewer-coverage-frame-{int(frame_timestamp)}"
                f"-id-coverage-v2-z{rainviewer_native.RAINVIEWER_ZOOM}"
                f"-x{x}-y{y}"
            )
        return rainviewer_native._tile_upstream_id(  # noqa: SLF001
            identity, kind=kind, x=x, y=y
        )
    if member.provenance_kind == "metar_v2":
        expected = {
            **common,
            "collector": "aviationweather_metars_cache",
            "collector_version": "worker-metar-cache-exact-raw-v2",
            "structure_inspector_version": "aviationweather-metars-csv-structure-v2",
            "upstream_filename": "metars.cache.csv.gz",
            "declared_semantic_class": "raw_terminal_observation_reports",
            "coverage": "worldwide_current_reports",
            "provider_update_cadence_seconds": 60,
            "meteorological_interpretation_verified_after_decode": False,
        }
        if provenance != expected:
            raise CaptureRecoveryIntegrityError("METAR raw provenance drifted")
        return None
    if member.provenance_kind == "dwd_rv_v2":
        expected = {
            **common,
            "collector": "dwd_open_data_composite_rv",
            "collector_version": "worker-dwd-rv-tar-exact-raw-v2",
            "structure_inspector_version": "dwd-rv-tar-layout-only-v1",
            "provenance_scope": "exact_upstream_tar_bytes_only",
        }
        if provenance != expected:
            raise CaptureRecoveryIntegrityError("DWD RV raw provenance drifted")
        return None
    if member.provenance_kind == "dpc_catalog_v1":
        expected = {
            **common,
            "collector": "dpc_radar_v2_exact_native",
            "collector_version": "worker-dpc-radar-v2-exact-native-v1",
            "structure_inspector_version": "dpc-radar-v2-catalog-v1",
            "operation": "findLastProductByType",
            "request_method": "GET",
            "request_product_type": "SRI",
            "origin_required": "https://radar.protezionecivile.it",
        }
        if provenance != expected:
            raise CaptureRecoveryIntegrityError("DPC catalog provenance drifted")
        return None
    if member.provenance_kind == "dpc_geotiff_v1":
        expected_keys = {
            "representation", "capture_stage", "collector", "collector_version",
            "structure_inspector_version", "operation", "bucket", "upstream_key",
            "object_origin", "object_path", "signed_url_expires_seconds",
            "presigned_url_persisted", "presigned_query_persisted", "product_type",
            "product_time_ms", "content_type", "content_length", "etag",
            "last_modified", "semantic_class", "declared_quantity", "declared_unit",
            "pixel_values_read", "quantitative_semantics_admitted",
        }
        key = provenance.get("upstream_key")
        product_time_ms = provenance.get("product_time_ms")
        if (
            set(provenance) != expected_keys
            or provenance.get("collector") != "dpc_radar_v2_exact_native"
            or provenance.get("collector_version")
            != "worker-dpc-radar-v2-exact-native-v1"
            or provenance.get("structure_inspector_version")
            != "dpc-sri-geotiff-container-v1"
            or provenance.get("operation") != "GetObject"
            or provenance.get("bucket") != "s3-prod-dpc-radar"
            or provenance.get("object_origin")
            != "https://s3-prod-dpc-radar.s3.eu-south-1.amazonaws.com"
            or not isinstance(key, str)
            or provenance.get("object_path") != f"/{key}"
            or isinstance(provenance.get("signed_url_expires_seconds"), bool)
            or not isinstance(provenance.get("signed_url_expires_seconds"), int)
            or not 1 <= provenance["signed_url_expires_seconds"] <= 900
            or provenance.get("presigned_url_persisted") is not False
            or provenance.get("presigned_query_persisted") is not False
            or provenance.get("product_type") != "SRI"
            or product_time_ms != _dpc_product_timestamp_ms(key)
            or provenance.get("content_type") != receipt.content_type
            or provenance.get("content_length") != receipt.size_bytes
            or provenance.get("semantic_class")
            != "surface_rainfall_intensity_estimate"
            or provenance.get("declared_quantity") != "surface_rainfall_intensity"
            or provenance.get("declared_unit") != "mm/h"
            or provenance.get("pixel_values_read") is not False
            or provenance.get("quantitative_semantics_admitted") is not False
        ):
            raise CaptureRecoveryIntegrityError("DPC GeoTIFF provenance drifted")
        _bounded_optional_text(provenance.get("etag"), "DPC ETag", 256)
        _bounded_optional_text(
            provenance.get("last_modified"), "DPC Last-Modified", 128
        )
        return key
    if member.provenance_kind == "noaa_err_listing_v1":
        expected_keys = {
            "representation", "capture_stage", "collector", "collector_version",
            "structure_inspector_version", "bucket", "operation", "listing_prefix",
            "list_type", "max_keys", "encoding_type", "http",
        }
        prefix = provenance.get("listing_prefix")
        if (
            set(provenance) != expected_keys
            or provenance.get("collector") != "noaa_err_public_s3_native"
            or provenance.get("collector_version") != "worker-noaa-err-exact-raw-v1"
            or provenance.get("structure_inspector_version")
            != "s3-listobjectsv2-noaa-err-v1"
            or provenance.get("bucket") != "noaa-enterprise-rainrate-pds"
            or provenance.get("operation") != "ListObjectsV2"
            or not isinstance(prefix, str)
            or _NOAA_ERR_PREFIX.fullmatch(prefix) is None
            or provenance.get("list_type") != 2
            or provenance.get("max_keys") != 1000
            or provenance.get("encoding_type") != "url"
        ):
            raise CaptureRecoveryIntegrityError("NOAA ERR listing provenance drifted")
        _validate_http_metadata(provenance.get("http"), receipt=receipt, carries_lengths=True)
        return prefix
    if member.provenance_kind == "noaa_err_netcdf4_v1":
        expected_keys = {
            "representation", "capture_stage", "collector", "collector_version",
            "structure_inspector_version", "bucket", "upstream_key", "listing_etag",
            "listing_last_modified", "listing_size_bytes", "listing_storage_class",
            "listing_checksum_algorithms", "listing_checksum_type",
            "filename_product_start", "filename_product_end", "filename_created",
            "filename_version", "declared_semantic_class", "declared_quantity",
            "declared_unit", "quantitative_semantics_verified_after_decode", "http",
        }
        key = provenance.get("upstream_key")
        algorithms = provenance.get("listing_checksum_algorithms")
        if (
            set(provenance) != expected_keys
            or provenance.get("collector") != "noaa_err_public_s3_native"
            or provenance.get("collector_version") != "worker-noaa-err-exact-raw-v1"
            or provenance.get("structure_inspector_version")
            != "noaa-err-netcdf4-golden-v2"
            or provenance.get("bucket") != "noaa-enterprise-rainrate-pds"
            or not isinstance(key, str)
            or _NOAA_ERR_KEY.fullmatch(key) is None
            or provenance.get("listing_size_bytes") != receipt.size_bytes
            or not isinstance(algorithms, list)
            or len(algorithms) > 8
            or any(not isinstance(value, str) or not 1 <= len(value) <= 64 for value in algorithms)
            or provenance.get("declared_semantic_class")
            != "satellite_precipitation_rate_product"
            or provenance.get("declared_quantity")
            != "instantaneous_precipitation_rate"
            or provenance.get("declared_unit") != "mm/h"
            or provenance.get("quantitative_semantics_verified_after_decode") is not False
        ):
            raise CaptureRecoveryIntegrityError("NOAA ERR NetCDF provenance drifted")
        for key_name in (
            "listing_etag", "listing_last_modified", "listing_storage_class",
            "listing_checksum_type", "filename_version",
        ):
            _bounded_optional_text(provenance.get(key_name), f"NOAA ERR {key_name}")
        for time_name in (
            "filename_product_start", "filename_product_end", "filename_created",
        ):
            _finite_number(provenance.get(time_name), f"NOAA ERR {time_name}")
        http = _validate_http_metadata(
            provenance.get("http"), receipt=receipt, carries_lengths=True
        )
        if http.get("etag") != provenance.get("listing_etag"):
            raise CaptureRecoveryIntegrityError("NOAA ERR object ETag drifted")
        return str(key)
    if member.provenance_kind == "goes_rrqpe_listing_v1":
        expected_keys = {
            "representation", "capture_stage", "collector", "collector_version",
            "satellite", "bucket", "operation", "role", "prefix", "list_type",
            "max_keys", "encoding_type", "anonymous_public_access",
            "credentials_used", "http", "decoded_or_admitted",
        }
        satellite = _satellite_for_binding(binding)
        prefix = provenance.get("prefix")
        if (
            set(provenance) != expected_keys
            or provenance.get("collector") != "noaa_goes_public_s3_native"
            or provenance.get("collector_version")
            != "worker-goes-rrqpe-exact-raw-v1"
            or provenance.get("satellite") != satellite
            or provenance.get("bucket") != f"noaa-goes{satellite}"
            or provenance.get("operation") != "ListObjectsV2"
            or provenance.get("role") != member.role
            or not isinstance(prefix, str)
            or _GOES_PREFIX.fullmatch(prefix) is None
            or provenance.get("list_type") != 2
            or provenance.get("max_keys") != 1000
            or provenance.get("encoding_type") != "url"
            or provenance.get("anonymous_public_access") is not True
            or provenance.get("credentials_used") is not False
            or provenance.get("decoded_or_admitted") is not False
        ):
            raise CaptureRecoveryIntegrityError("GOES listing provenance drifted")
        _validate_http_metadata(provenance.get("http"), receipt=receipt, carries_lengths=False)
        return prefix
    if member.provenance_kind == "goes_rrqpe_netcdf4_v1":
        expected_keys = {
            "representation", "capture_stage", "collector", "collector_version",
            "satellite", "bucket", "operation", "anonymous_public_access",
            "credentials_used", "upstream_key", "filename", "product", "abi_mode",
            "filename_observation_start", "filename_observation_end",
            "filename_creation_time", "listing_etag", "listing_last_modified",
            "listing_size_bytes", "listing_storage_class", "listing_checksum_algorithms",
            "listing_checksum_type", "http", "schema3_full_object_buffer_required",
            "operational_memory_certified", "decoded_or_admitted",
            "official_product_maturity",
        }
        satellite = _satellite_for_binding(binding)
        key = provenance.get("upstream_key")
        match = _GOES_KEY.fullmatch(key) if isinstance(key, str) else None
        maturity = provenance.get("official_product_maturity")
        expected_maturity = {
            "level": "full" if satellite == 18 else "provisional",
            "effective_date": "2025-01-04" if satellite == 18 else "2025-02-05",
            "source": (
                "https://www.ospo.noaa.gov/operations/goes/"
                "product-quality-overview/index.html"
            ),
            "provenance_only_not_a_quality_score": True,
        }
        if (
            set(provenance) != expected_keys
            or provenance.get("collector") != "noaa_goes_public_s3_native"
            or provenance.get("collector_version")
            != "worker-goes-rrqpe-exact-raw-v1"
            or provenance.get("satellite") != satellite
            or provenance.get("bucket") != f"noaa-goes{satellite}"
            or provenance.get("operation") != "GetObject"
            or provenance.get("anonymous_public_access") is not True
            or provenance.get("credentials_used") is not False
            or match is None
            or int(match.group("satellite")) != satellite
            or provenance.get("filename") != match.group("filename")
            or provenance.get("product") != "ABI-L2-RRQPEF"
            or provenance.get("abi_mode") != int(match.group("mode"))
            or provenance.get("listing_size_bytes") != receipt.size_bytes
            or provenance.get("schema3_full_object_buffer_required") is not True
            or provenance.get("operational_memory_certified") is not False
            or provenance.get("decoded_or_admitted") is not False
            or maturity != expected_maturity
        ):
            raise CaptureRecoveryIntegrityError("GOES NetCDF provenance drifted")
        for time_name in (
            "filename_observation_start", "filename_observation_end",
            "filename_creation_time",
        ):
            _finite_number(provenance.get(time_name), f"GOES {time_name}")
        for key_name in (
            "listing_etag", "listing_storage_class", "listing_checksum_type",
        ):
            _bounded_optional_text(provenance.get(key_name), f"GOES {key_name}")
        _finite_number(provenance.get("listing_last_modified"), "GOES modified")
        algorithms = provenance.get("listing_checksum_algorithms")
        if (
            not isinstance(algorithms, list)
            or len(algorithms) > 8
            or any(not isinstance(value, str) or not 1 <= len(value) <= 64 for value in algorithms)
        ):
            raise CaptureRecoveryIntegrityError("GOES checksum provenance drifted")
        _validate_http_metadata(provenance.get("http"), receipt=receipt, carries_lengths=False)
        return str(key)
    if member.provenance_kind == "metoffice_listing_v1":
        expected_keys = {
            "representation", "capture_stage", "collector", "collector_version",
            "bucket", "operation", "role", "prefix", "list_type", "max_keys",
            "encoding_type", "http", "decoded_or_admitted",
        }
        prefix = provenance.get("prefix")
        if (
            set(provenance) != expected_keys
            or provenance.get("collector") != "met_office_public_s3_native"
            or provenance.get("collector_version")
            != "worker-met-office-exact-raw-v1"
            or provenance.get("bucket") != "met-office-radar-obs-data"
            or provenance.get("operation") != "ListObjectsV2"
            or provenance.get("role") != member.role
            or not isinstance(prefix, str)
            or _METOFFICE_PREFIX.fullmatch(prefix) is None
            or provenance.get("list_type") != 2
            or provenance.get("max_keys") != 1000
            or provenance.get("encoding_type") != "url"
            or provenance.get("decoded_or_admitted") is not False
        ):
            raise CaptureRecoveryIntegrityError("Met Office listing provenance drifted")
        _validate_http_metadata(provenance.get("http"), receipt=receipt, carries_lengths=False)
        return prefix
    if member.provenance_kind == "metoffice_odim_v1":
        expected_keys = {
            "representation", "capture_stage", "collector", "collector_version",
            "bucket", "operation", "upstream_key", "listing_etag",
            "listing_last_modified", "listing_size_bytes", "listing_storage_class",
            "listing_checksum_algorithm", "listing_checksum_type",
            "filename_product_timestamp", "http", "schema3_full_object_buffer_required",
            "operational_memory_certified", "decoded_or_admitted",
        }
        key = provenance.get("upstream_key")
        if (
            set(provenance) != expected_keys
            or provenance.get("collector") != "met_office_public_s3_native"
            or provenance.get("collector_version")
            != "worker-met-office-exact-raw-v1"
            or provenance.get("bucket") != "met-office-radar-obs-data"
            or provenance.get("operation") != "GetObject"
            or not isinstance(key, str)
            or _METOFFICE_KEY.fullmatch(key) is None
            or provenance.get("listing_size_bytes") != receipt.size_bytes
            or provenance.get("schema3_full_object_buffer_required") is not True
            or provenance.get("operational_memory_certified") is not False
            or provenance.get("decoded_or_admitted") is not False
        ):
            raise CaptureRecoveryIntegrityError("Met Office ODIM provenance drifted")
        _finite_number(
            provenance.get("filename_product_timestamp"),
            "Met Office product timestamp",
        )
        for key_name in (
            "listing_etag", "listing_storage_class", "listing_checksum_algorithm",
            "listing_checksum_type",
        ):
            _bounded_optional_text(provenance.get(key_name), f"Met Office {key_name}")
        _finite_number(provenance.get("listing_last_modified"), "Met Office modified")
        _validate_http_metadata(provenance.get("http"), receipt=receipt, carries_lengths=False)
        return str(key)
    if member.provenance_kind == "closed_role_set_v1":
        expected = {
            **common,
            "collector": "closed_role_set_capture",
            "collector_version": ROLE_SET_DIGEST_CONTRACT,
            "meteorological_interpretation_verified_after_decode": False,
            "raw_product": member.raw_product,
            "role": member.role,
        }
        if provenance != expected:
            raise CaptureRecoveryIntegrityError(
                "closed role-set raw provenance drifted"
            )
        return None
    if member.provenance_kind != "knmi_rtcor_v1":
        raise CaptureRecoveryIntegrityError("unknown raw provenance binding")
    expected_keys = {
        "representation", "capture_stage", "collector", "collector_version",
        "structure_inspector_version", "dataset_name", "dataset_version",
        "dataset_license", "selection_order", "selection_limit",
        "upstream_filename", "listing_created", "listing_last_modified",
        "listing_size_bytes", "download_last_modified", "download_size_bytes",
        "download_content_type", "declared_semantic_class", "declared_quantity",
        "declared_unit", "declared_accumulation_minutes",
        "quantitative_semantics_verified_after_decode",
    }
    filename = provenance.get("upstream_filename")
    if (
        set(provenance) != expected_keys
        or provenance.get("collector") != "knmi_open_data_native_file"
        or provenance.get("collector_version")
        != "worker-knmi-rtcor-5m-exact-raw-v1"
        or provenance.get("structure_inspector_version")
        != "knmi-rtcor-live-golden-identity-v2"
        or provenance.get("dataset_name") != "nl_rdr_data_rtcor_5m"
        or provenance.get("dataset_version") != "1.0"
        or provenance.get("dataset_license") != "CC-BY-4.0"
        or provenance.get("selection_order") != "created_descending"
        or provenance.get("selection_limit") != 1
        or provenance.get("listing_size_bytes") != receipt.size_bytes
        or provenance.get("download_size_bytes") != receipt.size_bytes
        or provenance.get("download_content_type") != receipt.content_type
        or provenance.get("declared_semantic_class")
        != "quantitative_precipitation_accumulation"
        or provenance.get("declared_quantity") != "precipitation_accumulation"
        or provenance.get("declared_unit") != "mm"
        or provenance.get("declared_accumulation_minutes") != 5.0
        or provenance.get("quantitative_semantics_verified_after_decode") is not False
        or not isinstance(filename, str)
        or _KNMI_FILENAME.fullmatch(filename) is None
    ):
        raise CaptureRecoveryIntegrityError("KNMI RTCOR raw provenance drifted")
    _validate_rfc3339(provenance.get("listing_created"))
    listing_modified = _validate_rfc3339(provenance.get("listing_last_modified"))
    download_modified = _validate_rfc3339(provenance.get("download_last_modified"))
    if listing_modified != download_modified:
        raise CaptureRecoveryIntegrityError("KNMI transport identity changed")
    return filename


def _timestamp_from_token(token: str, pattern: str) -> float:
    try:
        return datetime.strptime(token, pattern).replace(tzinfo=timezone.utc).timestamp()
    except (OverflowError, ValueError) as err:
        raise CaptureRecoveryIntegrityError("raw product timestamp token is invalid") from err


def _validate_rfc3339(value: object) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise CaptureRecoveryIntegrityError("KNMI transport timestamp is invalid")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        moment = datetime.fromisoformat(candidate)
    except ValueError as err:
        raise CaptureRecoveryIntegrityError(
            "KNMI transport timestamp is invalid"
        ) from err
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise CaptureRecoveryIntegrityError("KNMI transport timestamp lacks timezone")
    timestamp = moment.timestamp()
    if not math.isfinite(timestamp):
        raise CaptureRecoveryIntegrityError("KNMI transport timestamp is not finite")
    return value


def _validate_receipt(
    receipt: RawArtifactReceipt,
    binding: RawRecoveryBinding,
    member: RawRecoveryMemberBinding,
) -> tuple[str, str | None]:
    job_id = _raw_job_from_fetch_id(
        receipt.fetch_id,
        binding=binding,
        suffix=member.receipt_suffix,
        attempt_limit=member.receipt_attempt_limit,
    )
    filename = _validate_provenance(receipt, binding, member)
    if (
        receipt.provider != binding.raw_provider
        or receipt.product != member.raw_product
        or receipt.content_type not in member.allowed_content_types
        or not (
            0
            if member.provenance_kind in _EMPTY_EXACT_RESPONSE_PROVENANCE_KINDS
            else 1
        ) <= receipt.size_bytes <= member.maximum_transport_bytes
    ):
        raise CaptureRecoveryIntegrityError("raw receipt violates its transport binding")
    if member.provenance_kind == "aemet_envelope_v1":
        from . import aemet_native

        expected = (
            f"{aemet_native.API_HOST}{aemet_native.API_PATH}"
            f"|content_sha256={receipt.content_sha256}"
        )
    elif member.provenance_kind in {"aemet_data_v1", "aemet_metadata_v1"}:
        from . import aemet_native

        if filename is None or _SHA256.fullmatch(filename) is None:
            raise CaptureRecoveryIntegrityError(
                "AEMET selected response lost its selection binding"
            )
        expected = (
            f"{aemet_native.API_HOST}|selection_sha256={filename}"
            f"|content_sha256={receipt.content_sha256}"
        )
    elif member.provenance_kind == "meteofrance_catalog_v1":
        from . import meteofrance_native

        expected = (
            f"{meteofrance_native.API_HOST}{meteofrance_native.CATALOG_PATH}"
            f"|content_sha256={receipt.content_sha256}"
        )
    elif member.provenance_kind == "meteofrance_hdf5_v1":
        from . import meteofrance_native

        if filename is None:
            raise CaptureRecoveryIntegrityError(
                "Meteo-France HDF5 lost its filename binding"
            )
        expected = (
            f"{meteofrance_native.PRODUCT_REFERENCE}:{filename}"
            f"|content_sha256={receipt.content_sha256}"
        )
    elif member.provenance_kind == "hsaf_h40b_listing_v1":
        from . import hsaf_h40b_native

        expected = (
            f"ftp:{hsaf_h40b_native.FTP_HOST}:"
            f"{hsaf_h40b_native.FTP_PORT}"
            f"{hsaf_h40b_native.FTP_DIRECTORY}:NLST"
            f"|content_sha256={receipt.content_sha256}"
        )
    elif member.provenance_kind == "hsaf_h40b_netcdf_gzip_v1":
        from . import hsaf_h40b_native

        if filename is None:
            raise CaptureRecoveryIntegrityError(
                "H SAF NetCDF-gzip lost its filename binding"
            )
        expected = (
            f"{hsaf_h40b_native.PRODUCT_ID}:{filename}"
            f"|content_sha256={receipt.content_sha256}"
        )
    elif member.provenance_kind == "netatmo_v2":
        from . import netatmo_native

        context = receipt.provenance["query_context"]
        expected = netatmo_native._expected_upstream_id(  # noqa: SLF001
            context, receipt.content_sha256
        )
    elif member.provenance_kind == "synoptic_v5":
        from . import synoptic_native

        context = receipt.provenance["query_context"]
        attempt = receipt.provenance["request_attempt"]
        if not receipt.fetch_id.endswith(
            f"{member.receipt_suffix}{attempt}"
        ):
            raise CaptureRecoveryIntegrityError(
                "Synoptic receipt attempt identity drifted"
            )
        expected = synoptic_native._expected_upstream_id(  # noqa: SLF001
            context, receipt.content_sha256, request_attempt=attempt
        )
    elif member.provenance_kind == "rainviewer_manifest_v1":
        expected = (
            "rainviewer-public-weather-maps-v2-manifest-sha256-"
            f"{receipt.content_sha256}"
        )
    elif member.provenance_kind in {
        "rainviewer_coverage_tile_v1", "rainviewer_radar_tile_v1"
    }:
        expected = filename
    elif member.provenance_kind == "metar_v2":
        expected = (
            "aviationweather/metars.cache.csv.gz"
            f"|content_sha256={receipt.content_sha256}"
        )
    elif member.provenance_kind == "dwd_rv_v2":
        expected = (
            f"{member.raw_product}|latest"
            f"|content_sha256={receipt.content_sha256}"
        )
    elif member.provenance_kind == "dpc_catalog_v1":
        expected = (
            "radar-api.protezionecivile.it/findLastProductByType?type=SRI"
            f"|content_sha256={receipt.content_sha256}"
        )
    elif member.provenance_kind == "dpc_geotiff_v1":
        expected = (
            f"s3:s3-prod-dpc-radar:{filename}"
            f"|content_sha256={receipt.content_sha256}"
        )
    elif member.provenance_kind == "noaa_err_listing_v1":
        expected = (
            f"s3-list-v2://noaa-enterprise-rainrate-pds/{filename}"
            f"|max_keys=1000|content_sha256={receipt.content_sha256}"
        )
    elif member.provenance_kind == "noaa_err_netcdf4_v1":
        expected = f"s3://noaa-enterprise-rainrate-pds/{filename}"
    elif member.provenance_kind == "goes_rrqpe_listing_v1":
        satellite = _satellite_for_binding(binding)
        expected = (
            f"s3-list-v2:noaa-goes{satellite}:{filename}:max-1000"
            f":sha256:{receipt.content_sha256}"
        )
    elif member.provenance_kind == "goes_rrqpe_netcdf4_v1":
        satellite = _satellite_for_binding(binding)
        expected = (
            f"s3:noaa-goes{satellite}:{filename}"
            f":etag:{receipt.provenance.get('listing_etag')}"
        )
    elif member.provenance_kind == "metoffice_listing_v1":
        expected = (
            f"s3-list-v2:met-office-radar-obs-data:{filename}:max-1000"
        )
    elif member.provenance_kind == "metoffice_odim_v1":
        expected = (
            f"s3:met-office-radar-obs-data:{filename}"
            f":etag:{receipt.provenance.get('listing_etag')}"
        )
    elif member.provenance_kind == "closed_role_set_v1":
        expected = (
            f"{member.raw_product}|role={member.role}"
            f"|content_sha256={receipt.content_sha256}"
        )
    elif member.provenance_kind == "knmi_rtcor_v1":
        expected = f"nl_rdr_data_rtcor_5m/1.0|filename={filename}"
    elif member.provenance_kind == "kmi_opendata_response_v1":
        request_sha256 = _sha256(
            receipt.provenance.get("canonical_request_sha256"),
            "KMI canonical request SHA-256",
        )
        expected = (
            f"kmi-opendata:request={request_sha256}"
            f":response={receipt.content_sha256}"
        )
    elif member.provenance_kind == "opera_stream_raw_v1":
        if filename is None:
            raise CaptureRecoveryIntegrityError(
                "OPERA receipt lost its upstream revision binding"
            )
        expected = filename
    elif member.provenance_kind in _KMI_APPV4_RECEIPT_PROVENANCE_KINDS:
        from . import kmi_appv4_raw_capture

        request_sha256 = _sha256(
            receipt.provenance.get("request_sha256"),
            "KMI app-v4 request SHA-256",
        )
        if member.role == "forecast_json":
            expected = (
                f"{kmi_appv4_raw_capture.FORECAST_HOST}"
                f"{kmi_appv4_raw_capture.FORECAST_PATH}"
                f"|request={request_sha256}|content={receipt.content_sha256}"
            )
        elif member.role == "radar_png":
            path_sha256 = _sha256(
                receipt.provenance.get("selected_uri_path_sha256"),
                "KMI app-v4 selected path SHA-256",
            )
            expected = (
                f"{receipt.provenance.get('endpoint_host')}"
                f"|target={request_sha256}|path={path_sha256}"
                f"|content={receipt.content_sha256}"
            )
        else:  # pragma: no cover - closed binding validates the role
            raise CaptureRecoveryIntegrityError(
                "KMI app-v4 receipt role drifted"
            )
    elif member.provenance_kind in _LIGHTNING_RECEIPT_PROVENANCE_KINDS:
        provenance = receipt.provenance
        expected = (
            f"{member.raw_product}|role={member.role}"
            f"|locator={provenance['locator_sha256']}"
            f"|request={provenance['request_sha256']}"
            f"|transport={provenance['transport_sha256']}"
            f"|content={receipt.content_sha256}"
        )
    elif member.provenance_kind in _PUBLIC_SOURCE_PROVENANCE_KINDS:
        _public_source, spec = _public_source_spec_for_binding(
            binding, member
        )
        request_sha256 = _sha256(
            receipt.provenance.get("request_sha256"),
            "public-source request SHA-256",
        )
        if filename is None or _SHA256.fullmatch(filename) is None:
            raise CaptureRecoveryIntegrityError(
                "public-source receipt lost its transport binding"
            )
        expected = (
            f"{spec.host}{spec.path}|request={request_sha256}"
            f"|transport={filename}|content={receipt.content_sha256}"
        )
    else:  # pragma: no cover - provenance validation rejects unknown kinds
        raise CaptureRecoveryIntegrityError("unknown raw receipt provenance binding")
    if receipt.upstream_id != expected:
        raise CaptureRecoveryIntegrityError("raw upstream receipt identity drifted")
    expected_id = RawArtifactStore._receipt_id(  # noqa: SLF001
        provider=binding.raw_provider,
        product=member.raw_product,
        fetch_id=receipt.fetch_id,
    )
    if receipt.receipt_id != expected_id:
        raise CaptureRecoveryIntegrityError("raw receipt ID is not deterministic")
    return job_id, filename


def recovery_binding_sha256(binding: RawRecoveryBinding) -> str:
    """Return the canonical fingerprint of one complete closed binding."""

    body = {
        "capture_product": binding.capture_product,
        "capture_provenance_kind": binding.capture_provenance_kind,
        "capture_suffix": binding.capture_suffix,
        "contract": ROLE_SET_DIGEST_CONTRACT,
        "handler_id": binding.handler_id,
        "ledger_provider": binding.ledger_provider,
        "maximum_transport_bytes": binding.maximum_transport_bytes,
        "dynamic_member": (
            None
            if binding.dynamic_member is None
            else {
                "allowed_content_types": sorted(
                    binding.dynamic_member.allowed_content_types
                ),
                "maximum_count": binding.dynamic_member.maximum_count,
                "maximum_transport_bytes": (
                    binding.dynamic_member.maximum_transport_bytes
                ),
                "minimum_count": binding.dynamic_member.minimum_count,
                "provenance_kind": binding.dynamic_member.provenance_kind,
                "raw_product": binding.dynamic_member.raw_product,
                "receipt_suffix_template": (
                    binding.dynamic_member.receipt_suffix_template
                ),
                "role_index_width": binding.dynamic_member.role_index_width,
                "role_pattern": binding.dynamic_member.role_pattern,
                "role_prefix": binding.dynamic_member.role_prefix,
            }
        ),
        "members": [
            {
                "allowed_content_types": sorted(member.allowed_content_types),
                "maximum_transport_bytes": member.maximum_transport_bytes,
                "provenance_kind": member.provenance_kind,
                "raw_product": member.raw_product,
                "receipt_attempt_limit": member.receipt_attempt_limit,
                "receipt_suffix": member.receipt_suffix,
                "role": member.role,
            }
            for member in sorted(binding.members, key=lambda value: value.role)
        ],
        "raw_provider": binding.raw_provider,
        "job_identity_kind": binding.job_identity_kind,
        "satellite": binding.satellite,
        "workload_id": binding.workload_id,
    }
    return hashlib.sha256(_canonical(body)).hexdigest()


def recovery_evidence_digest(
    *,
    binding: RawRecoveryBinding,
    receipts_by_role: Mapping[str, RawArtifactReceipt],
    capture: RawArtifactCaptureReceipt,
) -> RecoveryEvidenceDigest:
    """Bind a complete role set to the legacy scalar capacity-ledger fields.

    Single-role handlers retain their original scalar hashes so already
    committed durable rows remain replay-compatible. Multi-role handlers use
    canonical aggregate hashes over role-sorted members and the full binding
    fingerprint. The capture receipt hash remains a separate immutable proof.
    """

    binding.members_for_roles(receipts_by_role)
    ordered = tuple(
        (role, receipts_by_role[role]) for role in sorted(receipts_by_role)
    )
    receipt_ids: set[str] = set()
    job_ids: set[str] = set()
    for role, receipt in ordered:
        member = binding.member(role)
        if not isinstance(receipt, RawArtifactReceipt):
            raise CaptureRecoveryIntegrityError(
                "raw recovery role is not a durable receipt"
            )
        if (
            receipt.provider != binding.raw_provider
            or receipt.product != member.raw_product
            or receipt.content_type not in member.allowed_content_types
            or isinstance(receipt.size_bytes, bool)
            or not isinstance(receipt.size_bytes, int)
            or not (
                0
                if member.provenance_kind in _EMPTY_EXACT_RESPONSE_PROVENANCE_KINDS
                else 1
            ) <= receipt.size_bytes <= member.maximum_transport_bytes
            or receipt.receipt_id in receipt_ids
        ):
            raise CaptureRecoveryIntegrityError(
                "raw recovery role differs from its closed member binding"
            )
        job_ids.add(_raw_job_from_fetch_id(
            receipt.fetch_id,
            binding=binding,
            suffix=member.receipt_suffix,
            attempt_limit=member.receipt_attempt_limit,
        ))
        receipt_ids.add(receipt.receipt_id)
        _sha256(receipt.content_sha256, "content SHA-256")
        _sha256(receipt.receipt_sha256, "receipt SHA-256")
    if len(job_ids) != 1:
        raise CaptureRecoveryIntegrityError(
            "raw recovery role set changed acquisition job identity"
        )
    capture_sha256 = _sha256(
        getattr(capture, "capture_receipt_sha256", None),
        "capture receipt SHA-256",
    )
    actual_bytes = sum(receipt.size_bytes for _, receipt in ordered)
    actual_entries = len(ordered)
    if (
        not binding.minimum_artifacts
        <= actual_entries
        <= binding.maximum_artifacts
        or actual_bytes > binding.maximum_transport_bytes
    ):
        raise CaptureRecoveryIntegrityError(
            "raw role set exceeds its aggregate transport binding"
        )
    if actual_entries == 1:
        receipt = ordered[0][1]
        return RecoveryEvidenceDigest(
            actual_bytes=actual_bytes,
            actual_entries=1,
            content_sha256=receipt.content_sha256,
            receipt_sha256=receipt.receipt_sha256,
            capture_sha256=capture_sha256,
        )
    binding_sha256 = recovery_binding_sha256(binding)
    content_body = {
        "binding_sha256": binding_sha256,
        "contract": ROLE_SET_DIGEST_CONTRACT,
        "digest_kind": "content_role_set",
        "members": [
            {
                "content_sha256": receipt.content_sha256,
                "raw_product": binding.member(role).raw_product,
                "role": role,
                "size_bytes": receipt.size_bytes,
            }
            for role, receipt in ordered
        ],
    }
    receipt_body = {
        "binding_sha256": binding_sha256,
        "contract": ROLE_SET_DIGEST_CONTRACT,
        "digest_kind": "receipt_role_set",
        "members": [
            {
                "content_sha256": receipt.content_sha256,
                "receipt_id": receipt.receipt_id,
                "receipt_sha256": receipt.receipt_sha256,
                "role": role,
                "size_bytes": receipt.size_bytes,
            }
            for role, receipt in ordered
        ],
    }
    return RecoveryEvidenceDigest(
        actual_bytes=actual_bytes,
        actual_entries=actual_entries,
        content_sha256=hashlib.sha256(_canonical(content_body)).hexdigest(),
        receipt_sha256=hashlib.sha256(_canonical(receipt_body)).hexdigest(),
        capture_sha256=capture_sha256,
    )


def _validate_capture(
    capture: RawArtifactCaptureReceipt,
    *,
    binding: RawRecoveryBinding,
    receipts_by_role: Mapping[str, RawArtifactReceipt],
    metadata_by_role: Mapping[str, str | None],
    payloads_by_role: Mapping[str, bytes] | None = None,
) -> str:
    capture_suffix = _capture_suffix_for_recovery(capture, binding)
    job_id = _raw_job_from_fetch_id(
        capture.fetch_id,
        binding=binding,
        suffix=capture_suffix,
    )
    expected_capture_id = RawArtifactStore._capture_id(  # noqa: SLF001
        provider=binding.raw_provider,
        product=binding.capture_product,
        fetch_id=capture.fetch_id,
    )
    members = {member.role: member.receipt_id for member in capture.members}
    expected_members = {
        role: receipt.receipt_id for role, receipt in receipts_by_role.items()
    }
    actual_member_bindings = binding.members_for_roles(receipts_by_role)
    if (
        capture.capture_id != expected_capture_id
        or capture.provider != binding.raw_provider
        or capture.product != binding.capture_product
        or set(metadata_by_role) != set(receipts_by_role)
        or members != expected_members
    ):
        raise CaptureRecoveryIntegrityError("raw capture/member binding drifted")
    receipt_job_ids = {
        _raw_job_from_fetch_id(
            receipts_by_role[member.role].fetch_id,
            binding=binding,
            suffix=member.receipt_suffix,
            attempt_limit=member.receipt_attempt_limit,
        )
        for member in actual_member_bindings
    }
    if receipt_job_ids != {job_id}:
        raise CaptureRecoveryIntegrityError("raw capture changed job identity")
    if binding.capture_provenance_kind == "opera_stream_raw_v1":
        from . import opera_native

        try:
            opera_native.validate_opera_raw_only_capture_for_recovery(
                capture,
                receipts_by_role,
                job_id=job_id,
            )
        except (TypeError, ValueError) as err:
            raise CaptureRecoveryIntegrityError(
                "OPERA exact capture provenance drifted"
            ) from err
        expected_upstream = capture.upstream_capture_id
        expected_timestamp = None
    elif binding.capture_provenance_kind in _LIGHTNING_CAPTURE_PROVENANCE_KINDS:
        from . import lightning_exact_raw

        if (
            payloads_by_role is None
            or set(payloads_by_role) != set(receipts_by_role)
            or any(not isinstance(value, bytes) for value in payloads_by_role.values())
        ):
            raise CaptureRecoveryIntegrityError(
                "lightning exact recovery requires every durable payload byte"
            )
        try:
            if (
                binding.capture_provenance_kind
                == "eumetsat_mtg_li_exact_raw_v1"
            ):
                lightning_exact_raw._validate_eumetsat_bundle(  # noqa: SLF001
                    job_id,
                    capture,
                    receipts_by_role,
                    payloads_by_role,
                )
            else:
                if binding.satellite not in {18, 19}:
                    raise CaptureRecoveryIntegrityError(
                        "lightning capture lost its satellite discriminator"
                    )
                lightning_exact_raw._validate_goes_complete_bundle(  # noqa: SLF001
                    job_id=job_id,
                    satellite=binding.satellite,
                    capture=capture,
                    receipts=receipts_by_role,
                    payloads=payloads_by_role,
                )
        except lightning_exact_raw.LightningRawError as err:
            raise CaptureRecoveryIntegrityError(
                "lightning exact capture provenance drifted"
            ) from err
        expected_upstream = lightning_exact_raw._capture_upstream_id(  # noqa: SLF001
            binding.capture_product, receipts_by_role
        )
        expected_timestamp = None
    elif binding.capture_provenance_kind == "kmi_appv4_visual_capture_v2":
        from . import kmi_appv4_raw_capture

        if (
            payloads_by_role is None
            or set(payloads_by_role) != set(receipts_by_role)
            or any(not isinstance(value, bytes) for value in payloads_by_role.values())
        ):
            raise CaptureRecoveryIntegrityError(
                "KMI app-v4 exact recovery requires every durable payload byte"
            )
        try:
            result = (
                kmi_appv4_raw_capture.validate_kmi_appv4_capture_for_raw_recovery(
                    capture,
                    receipts_by_role,
                    payloads_by_role,
                    job_id=job_id,
                )
            )
        except kmi_appv4_raw_capture.KmiAppv4RawError as err:
            raise CaptureRecoveryIntegrityError(
                "KMI app-v4 exact capture provenance drifted"
            ) from err
        expected_timestamp = result["product_timestamp"]
        expected_upstream = capture.upstream_capture_id
    elif binding.capture_provenance_kind == "aemet_opendata_v1":
        envelope = receipts_by_role["opendata_envelope_json"]
        data = receipts_by_role["opendata_data_response"]
        metadata = receipts_by_role["opendata_metadata_response"]
        data_selection = metadata_by_role["opendata_data_response"]
        metadata_selection = metadata_by_role["opendata_metadata_response"]
        if (
            data_selection is None
            or metadata_selection is None
            or _SHA256.fullmatch(data_selection) is None
            or _SHA256.fullmatch(metadata_selection) is None
        ):
            raise CaptureRecoveryIntegrityError(
                "AEMET capture lost its envelope-selection binding"
            )
        expected_upstream = (
            "aemet-national-radar-opendata"
            f"|envelope={envelope.content_sha256}"
            f"|data_selection={data_selection}"
            f"|data={data.content_sha256}"
            f"|metadata_selection={metadata_selection}"
            f"|metadata={metadata.content_sha256}"
        )
        expected_timestamp = None
    elif binding.capture_provenance_kind == "meteofrance_dpradar_v1":
        from . import meteofrance_native

        catalog = receipts_by_role["catalog_json"]
        hdf5 = receipts_by_role["hdf5"]
        filename = metadata_by_role["hdf5"]
        if filename is None:
            raise CaptureRecoveryIntegrityError(
                "Meteo-France capture lost its product filename"
            )
        expected_timestamp = _finite_number(
            hdf5.provenance.get("product_timestamp"),
            "Meteo-France product timestamp",
        )
        expected_upstream = (
            f"{meteofrance_native.PRODUCT_REFERENCE}|filename={filename}"
            f"|catalog={catalog.content_sha256}"
            f"|hdf5={hdf5.content_sha256}"
        )
    elif binding.capture_provenance_kind == "hsaf_h40b_v1":
        from . import hsaf_h40b_native

        listing = receipts_by_role["listing_nlst"]
        product = receipts_by_role["netcdf_gzip"]
        filename = metadata_by_role["netcdf_gzip"]
        if (
            metadata_by_role["listing_nlst"] is not None
            or filename is None
        ):
            raise CaptureRecoveryIntegrityError(
                "H SAF capture lost its listing/product binding"
            )
        try:
            expected_timestamp = hsaf_h40b_native._slot_from_basename(  # noqa: SLF001
                filename
            )
            identity = hsaf_h40b_native.HsafListingIdentity(
                selected_basename=filename,
                selected_remote_path=(
                    f"{hsaf_h40b_native.FTP_DIRECTORY}/{filename}"
                ),
                nominal_slot_time=expected_timestamp,
                candidate_count=1,
                first_slot_time=expected_timestamp,
                last_slot_time=expected_timestamp,
            )
            hsaf_h40b_native._validate_capture(  # noqa: SLF001
                capture,
                job_id=job_id,
                receipts=receipts_by_role,
                identity=identity,
            )
        except hsaf_h40b_native.HsafH40bNativeError as err:
            raise CaptureRecoveryIntegrityError(
                "H SAF capture provenance drifted"
            ) from err
        expected_upstream = (
            f"{hsaf_h40b_native.PRODUCT_ID}|filename={filename}"
            f"|listing={listing.content_sha256}"
            f"|netcdf_gzip={product.content_sha256}"
        )
    elif binding.capture_provenance_kind in {"netatmo_v2", "synoptic_v5"}:
        receipt = receipts_by_role["response_json"]
        query_sha = metadata_by_role["response_json"]
        if query_sha is None or _SHA256.fullmatch(query_sha) is None:
            raise CaptureRecoveryIntegrityError(
                "ground capture lost its query-context binding"
            )
        expected_upstream = (
            f"{binding.capture_product}|query_sha256={query_sha}"
            f"|content_sha256={receipt.content_sha256}"
        )
        expected_timestamp = None
    elif binding.capture_provenance_kind == "rainviewer_visual_v1":
        center = receipts_by_role["radar_dx_0_dy_0"].provenance
        frame_path = center.get("frame_path")
        frame_timestamp = center.get("frame_generation_timestamp")
        center_x = center.get("x")
        center_y = center.get("y")
        latitude = center.get("request_center_lat")
        longitude = center.get("request_center_lon")
        horizon = center.get("request_horizon_km")
        if (
            not isinstance(frame_path, str)
            or not isinstance(center_x, int)
            or isinstance(center_x, bool)
            or not isinstance(center_y, int)
            or isinstance(center_y, bool)
            or not isinstance(frame_timestamp, (int, float))
            or isinstance(frame_timestamp, bool)
            or not math.isfinite(float(frame_timestamp))
            or not isinstance(latitude, (int, float))
            or isinstance(latitude, bool)
            or not isinstance(longitude, (int, float))
            or isinstance(longitude, bool)
            or not isinstance(horizon, (int, float))
            or isinstance(horizon, bool)
        ):
            raise CaptureRecoveryIntegrityError(
                "RainViewer capture lost its frame/region identity"
            )
        offset = {"m1": -1, "0": 0, "p1": 1}
        for member in binding.members:
            if member.role == "manifest":
                continue
            item = receipts_by_role[member.role].provenance
            kind, _, dx_token, _, dy_token = member.role.split("_")
            dx = offset[dx_token]
            dy = offset[dy_token]
            expected_x = (center_x + dx) % (2**5)
            expected_y = center_y + dy
            if (
                item.get("frame_generation_timestamp") != frame_timestamp
                or item.get("request_center_lat") != latitude
                or item.get("request_center_lon") != longitude
                or item.get("request_horizon_km") != horizon
                or item.get("x") != expected_x
                or item.get("y") != expected_y
                or (kind == "radar" and item.get("frame_path") != frame_path)
                or (kind == "coverage" and item.get("frame_path") is not None)
            ):
                raise CaptureRecoveryIntegrityError(
                    "RainViewer role grid or region binding drifted"
                )
        expected_timestamp = float(frame_timestamp)
        expected_upstream = (
            f"rainviewer-region-frame-{int(frame_timestamp)}"
            f"-id-{frame_path.rsplit('/', 1)[-1]}"
            f"-z5-center-x{center_x}-y{center_y}"
            f"-lat{float(latitude):.8f}-lon{float(longitude):.8f}"
            f"-horizon{float(horizon):.3f}km"
        )
    elif binding.capture_provenance_kind == "metar_v2":
        receipt = receipts_by_role[binding.role]
        expected_upstream = (
            f"{binding.capture_product}|content_sha256={receipt.content_sha256}"
        )
        expected_timestamp = None
    elif binding.capture_provenance_kind == "dwd_rv_v2":
        receipt = receipts_by_role[binding.role]
        prefix = f"{binding.capture_product}|member="
        suffix = f"|content_sha256={receipt.content_sha256}"
        if (
            not capture.upstream_capture_id.startswith(prefix)
            or not capture.upstream_capture_id.endswith(suffix)
        ):
            raise CaptureRecoveryIntegrityError("DWD capture identity drifted")
        member_name = capture.upstream_capture_id[len(prefix):-len(suffix)]
        match = _DWD_MEMBER.fullmatch(member_name)
        if match is None:
            raise CaptureRecoveryIntegrityError("DWD capture member is not canonical")
        expected_timestamp = _timestamp_from_token(
            match.group("date") + match.group("time"), "%Y%m%d%H%M"
        )
        expected_upstream = capture.upstream_capture_id
    elif binding.capture_provenance_kind == "knmi_rtcor_v1":
        receipt = receipts_by_role[binding.role]
        filename = metadata_by_role[binding.role]
        if filename is None:
            raise CaptureRecoveryIntegrityError("KNMI capture lost its filename binding")
        match = _KNMI_FILENAME.fullmatch(filename)
        if match is None:  # pragma: no cover - receipt validation established this
            raise CaptureRecoveryIntegrityError("KNMI filename is not canonical")
        expected_timestamp = _timestamp_from_token(match.group("timestamp"), "%Y%m%d%H%M")
        expected_upstream = (
            f"{binding.capture_product}|filename={filename}"
            f"|product_timestamp={expected_timestamp:.6f}"
            f"|content_sha256={receipt.content_sha256}"
        )
    elif binding.capture_provenance_kind == "kmi_opendata_v1":
        from . import kmi_opendata_raw

        receipt = receipts_by_role["geoserver_response"]
        try:
            kmi_opendata_raw.validate_kmi_opendata_capture_for_recovery(
                capture,
                receipt,
                job_id=job_id,
            )
        except kmi_opendata_raw.KmiOpenDataError as err:
            raise CaptureRecoveryIntegrityError(
                "KMI OpenData capture provenance drifted"
            ) from err
        expected_timestamp = None
        expected_upstream = capture.upstream_capture_id
    elif binding.capture_provenance_kind in _PUBLIC_SOURCE_PROVENANCE_KINDS:
        member = binding.members[0]
        public_source, spec = _public_source_spec_for_binding(
            binding, member
        )
        receipt = receipts_by_role[member.role]
        transport_sha256 = metadata_by_role[member.role]
        request_sha256 = receipt.provenance.get("request_sha256")
        query_count = receipt.provenance.get("query_count")
        if (
            transport_sha256 is None
            or _SHA256.fullmatch(transport_sha256) is None
            or not isinstance(request_sha256, str)
            or _SHA256.fullmatch(request_sha256) is None
            or isinstance(query_count, bool)
            or not isinstance(query_count, int)
            or not 0 <= query_count <= public_source.MAX_LOCATION_QUERIES
        ):
            raise CaptureRecoveryIntegrityError(
                "public-source capture lost its request/transport binding"
            )
        request_binding = public_source._RequestBinding(  # noqa: SLF001
            "", request_sha256, query_count
        )
        try:
            public_source._validate_capture(  # noqa: SLF001
                spec,
                request_binding,
                job_id,
                receipt,
                capture,
                transport_sha256,
            )
        except public_source.PublicSourceRawError as err:
            raise CaptureRecoveryIntegrityError(
                "public-source capture provenance drifted"
            ) from err
        expected_timestamp = None
        expected_upstream = capture.upstream_capture_id
    elif binding.capture_provenance_kind == "dpc_sri_v1":
        catalog = receipts_by_role["catalog_json"]
        geotiff = receipts_by_role["geotiff"]
        key = metadata_by_role["geotiff"]
        product_time_ms = geotiff.provenance.get("product_time_ms")
        if key is None or product_time_ms != _dpc_product_timestamp_ms(key):
            raise CaptureRecoveryIntegrityError("DPC capture lost its product identity")
        expected_timestamp = product_time_ms / 1000.0
        expected_upstream = (
            f"{binding.capture_product}|product_time_ms={product_time_ms}"
            f"|key={key}|catalog={catalog.content_sha256}"
            f"|geotiff={geotiff.content_sha256}"
        )
    elif binding.capture_provenance_kind == "noaa_err_v1":
        listing = receipts_by_role["listing_xml"]
        netcdf = receipts_by_role["netcdf4"]
        provenance = netcdf.provenance
        key = metadata_by_role["netcdf4"]
        if key is None:
            raise CaptureRecoveryIntegrityError("NOAA ERR capture lost its object key")
        expected_timestamp = _finite_number(
            provenance.get("filename_product_start"), "NOAA ERR product start"
        )
        expected_upstream = (
            f"{binding.capture_product}|key={key}"
            f"|etag={provenance.get('listing_etag')}"
            f"|listing={listing.content_sha256}"
            f"|netcdf4={netcdf.content_sha256}"
        )
    elif binding.capture_provenance_kind in {
        "goes18_rrqpe_v1", "goes19_rrqpe_v1",
    }:
        previous = receipts_by_role["listing_previous"]
        current = receipts_by_role["listing_current"]
        netcdf = receipts_by_role["netcdf4"]
        provenance = netcdf.provenance
        key = metadata_by_role["netcdf4"]
        satellite = _satellite_for_binding(binding)
        if key is None:
            raise CaptureRecoveryIntegrityError("GOES capture lost its object key")
        expected_timestamp = _finite_number(
            provenance.get("filename_observation_start"), "GOES observation start"
        )
        expected_upstream = (
            f"{binding.capture_product}|G{satellite}|key={key}"
            f"|etag={provenance.get('listing_etag')}"
            f"|previous={previous.content_sha256}"
            f"|current={current.content_sha256}"
            f"|netcdf4={netcdf.content_sha256}"
        )
    elif binding.capture_provenance_kind == "metoffice_uk_rate_v1":
        previous = receipts_by_role["listing_previous"]
        current = receipts_by_role["listing_current"]
        odim = receipts_by_role["odim_hdf5"]
        provenance = odim.provenance
        key = metadata_by_role["odim_hdf5"]
        if key is None:
            raise CaptureRecoveryIntegrityError(
                "Met Office capture lost its object key"
            )
        expected_timestamp = _finite_number(
            provenance.get("filename_product_timestamp"),
            "Met Office product timestamp",
        )
        expected_upstream = (
            f"{binding.capture_product}|key={key}"
            f"|etag={provenance.get('listing_etag')}"
            f"|previous={previous.content_sha256}"
            f"|current={current.content_sha256}"
            f"|hdf5={odim.content_sha256}"
        )
    elif binding.capture_provenance_kind == "closed_role_set_v1":
        evidence = recovery_evidence_digest(
            binding=binding,
            receipts_by_role=receipts_by_role,
            capture=capture,
        )
        expected_timestamp = None
        expected_upstream = (
            f"{binding.capture_product}"
            f"|role_set_content_sha256={evidence.content_sha256}"
        )
    else:
        raise CaptureRecoveryIntegrityError("unknown raw capture provenance binding")
    if (
        capture.upstream_capture_id != expected_upstream
        or capture.product_timestamp != expected_timestamp
    ):
        raise CaptureRecoveryIntegrityError("raw capture timestamp identity drifted")
    return job_id


def _validate_reservation(
    reservation: CapacityReservationReceipt | None,
    *,
    binding: RawRecoveryBinding,
    job_id: str,
) -> CapacityReservationReceipt:
    if reservation is None:
        raise CaptureRecoveryIntegrityError(
            "raw bytes exist without a durable prior capacity reservation"
        )
    if (
        reservation.provider != binding.ledger_provider
        or reservation.job_identity != job_id
        or reservation.requested_bytes != binding.maximum_transport_bytes
        or reservation.requested_entries != binding.maximum_artifacts
    ):
        raise CaptureRecoveryIntegrityError("raw reservation identity or budget drifted")
    return reservation


def _matches_commit(
    reservation: CapacityReservationReceipt,
    *,
    evidence: RecoveryEvidenceDigest,
) -> bool:
    return (
        reservation.status == "COMMITTED"
        and reservation.actual_bytes == evidence.actual_bytes
        and reservation.actual_entries == evidence.actual_entries
        and reservation.content_sha256 == evidence.content_sha256
        and reservation.receipt_sha256 == evidence.receipt_sha256
        and reservation.capture_sha256 == evidence.capture_sha256
        and reservation.commit_sha256 is not None
    )


class CaptureCapacityReconciler:
    """Verify exact raw evidence and idempotently close capacity reservations."""

    def __init__(
        self,
        *,
        raw_store: RawArtifactStore,
        capacity_ledger: RawCapacityLedger,
        fault_injector: Callable[[str], None] | None = None,
        recovery_bindings: Mapping[str, RawRecoveryBinding] | None = None,
        activation_rules: Mapping[str, ActivationRule] | None = None,
        selected_binding_ids: frozenset[str] | None = None,
        selected_workload_ids: frozenset[str] | None = None,
    ) -> None:
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store must be RawArtifactStore")
        if not isinstance(capacity_ledger, RawCapacityLedger):
            raise TypeError("capacity_ledger must be RawCapacityLedger")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("capture recovery fault injector must be callable")
        selected_bindings = (
            RECOVERY_BINDINGS if recovery_bindings is None else recovery_bindings
        )
        selected_rules = ACTIVATION_RULES if activation_rules is None else activation_rules
        validate_closed_recovery_bindings(selected_bindings, selected_rules)
        if (selected_binding_ids is None) != (selected_workload_ids is None):
            raise TypeError(
                "selected binding and workload scopes must be supplied together"
            )
        if selected_binding_ids is not None:
            if (
                type(selected_binding_ids) is not frozenset
                or type(selected_workload_ids) is not frozenset
                or any(type(value) is not str for value in selected_binding_ids)
                or any(type(value) is not str for value in selected_workload_ids)
            ):
                raise TypeError("selected recovery scopes must be immutable frozensets")
            if selected_binding_ids != frozenset(selected_bindings):
                raise ValueError("selected binding scope differs from recovery bindings")
            if selected_workload_ids != frozenset(
                rule.workload_id
                for rule in selected_rules.values()
                if rule.executable
            ):
                raise ValueError("selected workload scope differs from activation rules")
        self._bindings = MappingProxyType(dict(selected_bindings))
        self._activation_rules = MappingProxyType(dict(selected_rules))
        self._selected_binding_ids = selected_binding_ids
        self._selected_workload_ids = selected_workload_ids
        self._raw_store = raw_store
        self._capacity_ledger = capacity_ledger
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self._counters = {
            "already_committed": 0,
            "audits": 0,
            "failures": 0,
            "incomplete_receipts": 0,
            "inspected_captures": 0,
            "inspected_receipts": 0,
            "recovered_commits": 0,
        }

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _commit(
        self,
        *,
        reservation: CapacityReservationReceipt,
        capture: RawArtifactCaptureReceipt,
        evidence: RecoveryEvidenceDigest,
    ) -> RecoveredCaptureEvidence:
        if reservation.status == "COMMITTED":
            if not _matches_commit(reservation, evidence=evidence):
                raise CaptureRecoveryIntegrityError(
                    "capacity commit differs from durable raw evidence"
                )
            return RecoveredCaptureEvidence(
                capture_id=capture.capture_id,
                actual_bytes=evidence.actual_bytes,
                actual_entries=evidence.actual_entries,
                content_sha256=evidence.content_sha256,
                receipt_sha256=evidence.receipt_sha256,
                capture_sha256=evidence.capture_sha256,
                capacity_commit_sha256=str(reservation.commit_sha256),
                already_committed=True,
            )
        self._fault("reconcile.after_capture_verified")
        self._fault("reconcile.before_capacity_commit")
        try:
            committed = self._capacity_ledger.commit(
                reservation_id=reservation.reservation_id,
                actual_bytes=evidence.actual_bytes,
                actual_entries=evidence.actual_entries,
                content_sha256=evidence.content_sha256,
                receipt_sha256=evidence.receipt_sha256,
                capture_sha256=evidence.capture_sha256,
            )
        except RawCapacityLedgerBusyError as err:
            raise CaptureRecoveryBusyError("capacity ledger remained busy") from err
        except RawCapacityLedgerCapacityError as err:
            raise CaptureRecoveryCapacityError(
                "existing capacity reservation cannot accept raw evidence"
            ) from err
        except (RawCapacityLedgerConflict, RawCapacityLedgerIntegrityError) as err:
            raise CaptureRecoveryIntegrityError(
                "capacity ledger rejected exact raw evidence"
            ) from err
        self._fault("reconcile.after_capacity_commit")
        if not _matches_commit(committed, evidence=evidence):
            raise CaptureRecoveryIntegrityError("capacity commit was not durable and exact")
        return RecoveredCaptureEvidence(
            capture_id=capture.capture_id,
            actual_bytes=evidence.actual_bytes,
            actual_entries=evidence.actual_entries,
            content_sha256=evidence.content_sha256,
            receipt_sha256=evidence.receipt_sha256,
            capture_sha256=evidence.capture_sha256,
            capacity_commit_sha256=str(committed.commit_sha256),
            already_committed=False,
        )

    @staticmethod
    def _inventory_fingerprint(stats: Mapping[str, Any]) -> tuple[int, ...]:
        fields = (
            "object_count", "receipt_count", "capture_count", "catalog_bytes",
            "filesystem_bytes", "filesystem_object_entries",
            "filesystem_staging_entries", "unmanaged_file_count",
        )
        values: list[int] = []
        for field in fields:
            value = stats.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise CaptureRecoveryIntegrityError("raw store stats are invalid")
            values.append(value)
        return tuple(values)

    def _stable_inventory(
        self,
    ) -> tuple[
        tuple[RawArtifactReceipt, ...],
        tuple[RawArtifactCaptureReceipt, ...],
        Mapping[str, Any],
    ]:
        for _attempt in range(MAX_STABLE_INVENTORY_ATTEMPTS):
            before = self._raw_store.stats()
            receipts = self._raw_store.accounting_receipts()
            captures = self._raw_store.accounting_captures()
            after = self._raw_store.stats()
            if self._inventory_fingerprint(before) == self._inventory_fingerprint(after):
                return receipts, captures, after
        raise CaptureRecoveryBusyError("raw inventory changed during bounded audit")

    def _selected_inventory(
        self,
        *,
        receipts: tuple[RawArtifactReceipt, ...],
        captures: tuple[RawArtifactCaptureReceipt, ...],
        reservations: tuple[CapacityReservationReceipt, ...],
    ) -> tuple[
        tuple[RawArtifactReceipt, ...],
        tuple[RawArtifactCaptureReceipt, ...],
        tuple[CapacityReservationReceipt, ...],
        Mapping[str, int],
    ]:
        """Project a valid global inventory onto one immutable recovery scope."""

        selected_receipts = tuple(
            receipt
            for receipt in receipts
            if any(
                receipt.provider == binding.raw_provider
                and any(
                    receipt.product == member.raw_product
                    for member in (
                        binding.members
                        + (
                            ()
                            if binding.dynamic_member is None
                            else (
                                binding.dynamic_member.materialize(
                                    binding.dynamic_member.role_for_index(0)
                                ),
                            )
                        )
                    )
                )
                for binding in self._bindings.values()
            )
        )
        selected_captures = tuple(
            capture
            for capture in captures
            if any(
                capture.provider == binding.raw_provider
                and capture.product == binding.capture_product
                for binding in self._bindings.values()
            )
        )
        selected_origin_jobs: set[str] = set()
        for receipt in selected_receipts:
            binding, member = _binding_member_for_receipt(
                receipt, self._bindings
            )
            raw_job_id, _filename = _validate_receipt(receipt, binding, member)
            for reservation in reservations:
                if (
                    reservation.provider == binding.ledger_provider
                    and reservation.requested_bytes
                    == binding.maximum_transport_bytes
                    and reservation.requested_entries
                    == binding.maximum_artifacts
                    and _raw_identity_for_origin(
                        binding, reservation.job_identity
                    ) == raw_job_id
                ):
                    selected_origin_jobs.add(reservation.job_identity)
        selected_reservations: list[CapacityReservationReceipt] = []
        selected_ids = set(self._bindings)
        for reservation in reservations:
            catalog_candidates = tuple(
                binding
                for binding in RECOVERY_BINDINGS.values()
                if binding.ledger_provider == reservation.provider
                and binding.maximum_transport_bytes == reservation.requested_bytes
                and binding.maximum_artifacts == reservation.requested_entries
            )
            selected_candidates = tuple(
                binding
                for binding in catalog_candidates
                if binding.handler_id in selected_ids
            )
            if selected_candidates and (
                all(
                    binding.handler_id in selected_ids
                    for binding in catalog_candidates
                )
                or reservation.job_identity in selected_origin_jobs
            ):
                selected_reservations.append(reservation)
        objects: dict[str, int] = {}
        for receipt in selected_receipts:
            prior = objects.setdefault(receipt.content_sha256, receipt.size_bytes)
            if prior != receipt.size_bytes:
                raise CaptureRecoveryIntegrityError(
                    "selected raw object has conflicting sizes"
                )
        selected_stats = {
            "object_count": len(objects),
            "receipt_count": len(selected_receipts),
            "capture_count": len(selected_captures),
            "catalog_bytes": sum(objects.values()),
            "filesystem_bytes": sum(objects.values()),
            "filesystem_object_entries": len(objects),
            "filesystem_staging_entries": 0,
            "unmanaged_file_count": 0,
        }
        return (
            selected_receipts,
            selected_captures,
            tuple(selected_reservations),
            selected_stats,
        )

    def audit_existing(self) -> RecoveryAuditResult:
        """Fail closed on unaccounted bytes and reconcile every complete capture."""

        with self._lock:
            try:
                receipts, captures, stats = self._stable_inventory()
                try:
                    ledger_inventory = self._capacity_ledger.accounting_reservations(
                        max_rows=MAX_RECOVERY_RESERVATIONS
                    )
                except RawCapacityLedgerBusyError as err:
                    raise CaptureRecoveryBusyError(
                        "capacity reservation inventory remained busy"
                    ) from err
                except RawCapacityLedgerCapacityError as err:
                    raise CaptureRecoveryCapacityError(
                        "capacity reservation inventory exceeds recovery bound"
                    ) from err
                except (RawCapacityLedgerConflict,
                        RawCapacityLedgerIntegrityError) as err:
                    raise CaptureRecoveryIntegrityError(
                        "capacity reservation inventory failed closed"
                    ) from err
                if self._selected_binding_ids is None:
                    if (
                        stats["filesystem_staging_entries"] != 0
                        or stats["unmanaged_file_count"] != 0
                        or stats["filesystem_object_entries"] != stats["object_count"]
                        or stats["filesystem_bytes"] != stats["catalog_bytes"]
                        or stats["receipt_count"] != len(receipts)
                        or stats["capture_count"] != len(captures)
                    ):
                        raise CaptureRecoveryIntegrityError(
                            "raw bytes exist outside the exact catalog inventory"
                        )
                else:
                    receipts, captures, ledger_inventory, stats = (
                        self._selected_inventory(
                            receipts=receipts,
                            captures=captures,
                            reservations=ledger_inventory,
                        )
                    )
                reservations: dict[str, CapacityReservationReceipt] = {}
                reservation_candidates: dict[
                    str, tuple[RawRecoveryBinding, ...]
                ] = {}
                origin_by_raw_identity: dict[tuple[str, str], str] = {}
                for reservation in ledger_inventory:
                    origin_job_id = reservation.job_identity
                    if _JOB_ID.fullmatch(origin_job_id) is None:
                        raise CaptureRecoveryIntegrityError(
                            "capacity reservation has no acquisition job identity"
                        )
                    candidates = tuple(
                        binding
                        for binding in self._bindings.values()
                        if binding.ledger_provider == reservation.provider
                        and binding.maximum_transport_bytes
                        == reservation.requested_bytes
                        and binding.maximum_artifacts
                        == reservation.requested_entries
                    )
                    if not candidates:
                        raise CaptureRecoveryIntegrityError(
                            "capacity reservation is outside recovery"
                        )
                    _validate_reservation(
                        reservation,
                        binding=candidates[0],
                        job_id=origin_job_id,
                    )
                    if origin_job_id in reservations:
                        raise CaptureRecoveryIntegrityError(
                            "capacity ledger contains duplicate job identities"
                        )
                    reservations[origin_job_id] = reservation
                    reservation_candidates[origin_job_id] = candidates
                    for candidate in candidates:
                        raw_identity = _raw_identity_for_origin(
                            candidate, origin_job_id
                        )
                        key = (candidate.handler_id, raw_identity)
                        prior = origin_by_raw_identity.setdefault(
                            key, origin_job_id
                        )
                        if prior != origin_job_id:
                            raise CaptureRecoveryIntegrityError(
                                "safe native job alias collision detected"
                            )
                unique_objects: dict[str, int] = {}
                receipt_bindings: dict[
                    str,
                    tuple[
                        RawRecoveryBinding,
                        RawRecoveryMemberBinding,
                        str,
                        str | None,
                    ],
                ] = {}
                receipts_by_id: dict[str, RawArtifactReceipt] = {}
                receipts_by_job_role: dict[
                    str, dict[str, RawArtifactReceipt]
                ] = {}
                metadata_by_job_role: dict[str, dict[str, str | None]] = {}
                job_bindings: dict[str, RawRecoveryBinding] = {}
                for receipt in receipts:
                    binding, member_binding = _binding_member_for_receipt(
                        receipt, self._bindings
                    )
                    raw_job_id, filename = _validate_receipt(
                        receipt, binding, member_binding
                    )
                    try:
                        job_id = origin_by_raw_identity[
                            (binding.handler_id, raw_job_id)
                        ]
                        reservation = reservations[job_id]
                    except KeyError as err:
                        raise CaptureRecoveryIntegrityError(
                            "raw bytes exist without a durable prior origin reservation"
                        ) from err
                    if binding not in reservation_candidates[job_id]:
                        raise CaptureRecoveryIntegrityError(
                            "raw binding differs from its reservation envelope"
                        )
                    existing_size = unique_objects.setdefault(
                        receipt.content_sha256, receipt.size_bytes
                    )
                    if existing_size != receipt.size_bytes:
                        raise CaptureRecoveryIntegrityError(
                            "content-addressed raw object has conflicting sizes"
                        )
                    prior_binding = job_bindings.setdefault(job_id, binding)
                    if prior_binding != binding:
                        raise CaptureRecoveryIntegrityError(
                            "one acquisition job appears in multiple raw bindings"
                        )
                    role_receipts = receipts_by_job_role.setdefault(job_id, {})
                    role_metadata = metadata_by_job_role.setdefault(job_id, {})
                    if member_binding.role in role_receipts:
                        raise CaptureRecoveryIntegrityError(
                            "one acquisition job has duplicate raw roles"
                        )
                    role_receipts[member_binding.role] = receipt
                    role_metadata[member_binding.role] = filename
                    if (
                        sum(item.size_bytes for item in role_receipts.values())
                        > binding.maximum_transport_bytes
                    ):
                        raise CaptureRecoveryIntegrityError(
                            "raw receipts exceed their aggregate transport binding"
                        )
                    receipt_bindings[receipt.receipt_id] = (
                        binding, member_binding, job_id, filename
                    )
                    receipts_by_id[receipt.receipt_id] = receipt
                if (
                    len(unique_objects) != stats["object_count"]
                    or sum(unique_objects.values()) != stats["catalog_bytes"]
                ):
                    raise CaptureRecoveryIntegrityError(
                        "raw catalog contains an object without an accounted receipt"
                    )
                raw_captures = _raw_captures_after_certified_derivative_audit(
                    captures
                )
                captured_receipts: set[str] = set()
                captured_jobs: set[str] = set()
                recovered = 0
                already = 0
                for capture in raw_captures:
                    if not capture.members:
                        raise CaptureRecoveryIntegrityError(
                            "active raw capture has no members"
                        )
                    try:
                        member_records = tuple(
                            receipt_bindings[item.receipt_id]
                            for item in capture.members
                        )
                    except KeyError as err:
                        raise CaptureRecoveryIntegrityError(
                            "raw capture member has no catalog receipt"
                        ) from err
                    capture_bindings = {item[0] for item in member_records}
                    if len(capture_bindings) != 1:
                        raise CaptureRecoveryIntegrityError(
                            "raw capture spans multiple recovery bindings"
                        )
                    binding = next(iter(capture_bindings))
                    if binding.handler_id not in self._bindings:
                        raise CaptureRecoveryIntegrityError(
                            "active raw capture handler is not selected"
                        )
                    if capture.provider != binding.raw_provider:
                        raise CaptureRecoveryIntegrityError(
                            "active raw capture provider drifted"
                        )
                    if capture.product != binding.capture_product:
                        raise CaptureRecoveryIntegrityError(
                            "active raw capture product drifted"
                        )
                    _capture_suffix_for_recovery(capture, binding)
                    binding.members_for_roles(
                        tuple(item.role for item in capture.members)
                    )
                    if binding.capture_provenance_kind == "opera_stream_raw_v1":
                        reference_bundle = self._raw_store.load_capture_bundle(
                            capture.capture_id
                        )
                        loaded_bundle = (
                            None
                            if reference_bundle is None
                            else (reference_bundle[0], reference_bundle[1], None)
                        )
                    else:
                        loaded_bundle = self._raw_store.load_capture_bundle_bytes(
                            capture.capture_id
                        )
                    if loaded_bundle is None:
                        raise CaptureRecoveryIntegrityError(
                            "active raw capture bytes are unavailable"
                        )
                    loaded_capture, loaded_receipts, payloads_by_role = loaded_bundle
                    if (
                        loaded_capture != capture
                        or set(loaded_receipts)
                        != {item.role for item in capture.members}
                        or (
                            binding.capture_provenance_kind
                            != "opera_stream_raw_v1"
                            and set(payloads_by_role or {})
                            != set(loaded_receipts)
                        )
                        or (
                            binding.capture_provenance_kind
                            == "opera_stream_raw_v1"
                            and payloads_by_role is not None
                        )
                    ):
                        raise CaptureRecoveryIntegrityError(
                            "active raw capture byte inventory drifted"
                        )
                    capture_receipts: dict[str, RawArtifactReceipt] = {}
                    capture_metadata: dict[str, str | None] = {}
                    receipt_job_ids: set[str] = set()
                    for capture_member in capture.members:
                        try:
                            (
                                receipt_binding,
                                member_binding,
                                receipt_job_id,
                                filename,
                            ) = receipt_bindings[capture_member.receipt_id]
                        except KeyError as err:
                            raise CaptureRecoveryIntegrityError(
                                "raw capture member has no catalog receipt"
                            ) from err
                        if (
                            receipt_binding != binding
                            or capture_member.role != member_binding.role
                            or capture_member.receipt_id in captured_receipts
                        ):
                            raise CaptureRecoveryIntegrityError(
                                "raw capture and receipt use different closed bindings"
                            )
                        capture_receipts[member_binding.role] = receipts_by_id[
                            capture_member.receipt_id
                        ]
                        capture_metadata[member_binding.role] = filename
                        receipt_job_ids.add(receipt_job_id)
                    raw_job_id = _validate_capture(
                        capture,
                        binding=binding,
                        receipts_by_role=capture_receipts,
                        metadata_by_role=capture_metadata,
                        payloads_by_role=payloads_by_role,
                    )
                    try:
                        job_id = origin_by_raw_identity[
                            (binding.handler_id, raw_job_id)
                        ]
                    except KeyError as err:
                        raise CaptureRecoveryIntegrityError(
                            "raw capture has no derivable origin reservation"
                        ) from err
                    if receipt_job_ids != {job_id}:
                        raise CaptureRecoveryIntegrityError("raw capture changed job identity")
                    if job_id in captured_jobs:
                        raise CaptureRecoveryIntegrityError(
                            "one acquisition job has multiple complete raw captures"
                        )
                    evidence_digest = recovery_evidence_digest(
                        binding=binding,
                        receipts_by_role=capture_receipts,
                        capture=capture,
                    )
                    evidence = self._commit(
                        reservation=reservations[job_id],
                        capture=capture,
                        evidence=evidence_digest,
                    )
                    recovered += int(not evidence.already_committed)
                    already += int(evidence.already_committed)
                    captured_receipts.update(
                        receipt.receipt_id for receipt in capture_receipts.values()
                    )
                    captured_jobs.add(job_id)
                incomplete = len(receipts) - len(captured_receipts)
                for receipt in receipts:
                    if receipt.receipt_id in captured_receipts:
                        continue
                    _, _, job_id, _ = receipt_bindings[receipt.receipt_id]
                    if reservations[job_id].status == "COMMITTED":
                        raise CaptureRecoveryIntegrityError(
                            "committed capacity has no complete raw capture"
                        )
                for reservation in ledger_inventory:
                    if (
                        reservation.status == "COMMITTED"
                        and reservation.job_identity not in captured_jobs
                    ):
                        raise CaptureRecoveryIntegrityError(
                            "committed capacity has no durable raw capture"
                        )
                result = RecoveryAuditResult(
                    len(receipts), len(captures), incomplete, recovered, already
                )
                self._counters["audits"] += 1
                self._counters["inspected_receipts"] += len(receipts)
                self._counters["inspected_captures"] += len(captures)
                self._counters["incomplete_receipts"] += incomplete
                self._counters["recovered_commits"] += recovered
                self._counters["already_committed"] += already
                return result
            except CaptureRecoveryError:
                self._counters["failures"] += 1
                raise
            except (RawArtifactError, OSError, ValueError) as err:
                self._counters["failures"] += 1
                raise CaptureRecoveryIntegrityError(
                    "raw recovery inventory failed closed"
                ) from err

    def reconcile_job(
        self,
        *,
        job: AcquisitionJob,
        rule: ActivationRule,
        reservation_id: str,
    ) -> RecoveredCaptureEvidence | None:
        """Recover one exact active job, or return ``None`` before capture commit."""

        try:
            validated = validate_worker_acquisition_job(job)
            binding = self._bindings.get(str(rule.handler_id))
            if (
                binding is None
                or not rule.executable
                or self._activation_rules.get(rule.workload_id) != rule
                or validated.workload_id != binding.workload_id
                or validated.provider_id != binding.ledger_provider
                or rule.required_roles != binding.required_roles
                or rule.dynamic_role_pattern != binding.dynamic_role_pattern
                or rule.dynamic_role_minimum != binding.dynamic_role_minimum
                or rule.dynamic_role_maximum != binding.dynamic_role_maximum
                or rule.maximum_transport_bytes != binding.maximum_transport_bytes
                or rule.maximum_artifacts != binding.maximum_artifacts
            ):
                raise CaptureRecoveryIntegrityError(
                    "job is outside the closed raw recovery subset"
                )
            reservation = _validate_reservation(
                self._capacity_ledger.load(reservation_id),
                binding=binding,
                job_id=validated.job_id,
            )
            raw_job_id = _raw_identity_for_origin(binding, validated.job_id)
            expected_capture_id = RawArtifactStore._capture_id(  # noqa: SLF001
                provider=binding.raw_provider,
                product=binding.capture_product,
                fetch_id=f"{raw_job_id}{binding.capture_suffix}",
            )
            if binding.capture_provenance_kind == "opera_stream_raw_v1":
                reference_bundle = self._raw_store.load_capture_bundle(
                    expected_capture_id
                )
                bundle = (
                    None
                    if reference_bundle is None
                    else (reference_bundle[0], reference_bundle[1], None)
                )
            else:
                bundle = self._raw_store.load_capture_bundle_bytes(
                    expected_capture_id
                )
            if bundle is None:
                if reservation.status == "COMMITTED":
                    raise CaptureRecoveryIntegrityError(
                        "capacity was committed without its exact raw capture"
                    )
                return None
            capture, receipts_by_role, payloads_by_role = bundle
            member_bindings = binding.members_for_roles(receipts_by_role)
            receipt_job_ids: set[str] = set()
            metadata_by_role: dict[str, str | None] = {}
            for member in member_bindings:
                receipt_job_id, metadata = _validate_receipt(
                    receipts_by_role[member.role], binding, member
                )
                receipt_job_ids.add(receipt_job_id)
                metadata_by_role[member.role] = metadata
            capture_job_id = _validate_capture(
                capture,
                binding=binding,
                receipts_by_role=receipts_by_role,
                metadata_by_role=metadata_by_role,
                payloads_by_role=payloads_by_role,
            )
            if receipt_job_ids != {raw_job_id} or capture_job_id != raw_job_id:
                raise CaptureRecoveryIntegrityError("raw recovery job identity drifted")
            evidence_digest = recovery_evidence_digest(
                binding=binding,
                receipts_by_role=receipts_by_role,
                capture=capture,
            )
            with self._lock:
                evidence = self._commit(
                    reservation=reservation,
                    capture=capture,
                    evidence=evidence_digest,
                )
                self._counters["inspected_receipts"] += len(receipts_by_role)
                self._counters["inspected_captures"] += 1
                key = "already_committed" if evidence.already_committed else "recovered_commits"
                self._counters[key] += 1
                return evidence
        except CaptureRecoveryError:
            with self._lock:
                self._counters["failures"] += 1
            raise
        except (RawArtifactError, RawCapacityLedgerConflict,
                RawCapacityLedgerIntegrityError, OSError, ValueError) as err:
            with self._lock:
                self._counters["failures"] += 1
            raise CaptureRecoveryIntegrityError(
                "raw capture recovery failed closed"
            ) from err

    def diagnostics(self) -> dict[str, Any]:
        """Return fixed aggregate counters only; no jobs, paths, or credentials."""

        with self._lock:
            result = {
                "contract": CONTRACT,
                **{key: int(value) for key, value in sorted(self._counters.items())},
            }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise CaptureRecoveryIntegrityError("recovery diagnostics exceed bound")
        return result


__all__ = [
    "CONTRACT",
    "RECOVERY_BINDING_CATALOG",
    "RECOVERY_BINDINGS",
    "LOCATION_SHADOW_RECOVERY_BINDINGS",
    "ROLE_SET_DIGEST_CONTRACT",
    "CaptureCapacityReconciler",
    "CaptureRecoveryBusyError",
    "CaptureRecoveryCapacityError",
    "CaptureRecoveryError",
    "CaptureRecoveryIntegrityError",
    "RawRecoveryBinding",
    "RawRecoveryDynamicMemberBinding",
    "RawRecoveryMemberBinding",
    "RecoveryEvidenceDigest",
    "RecoveredCaptureEvidence",
    "RecoveryAuditResult",
    "recovery_binding_sha256",
    "recovery_evidence_digest",
    "validate_closed_recovery_bindings",
    "validate_dormant_recovery_bindings",
    "validate_recovery_binding_catalog",
]
