"""Fresh-process entrypoint for the closed raw-only acquisition subset.

The parent sends bounded technical control data only. Provider modules are
imported lazily after the closed handler/workload binding has been validated.
The result contains durable raw hashes and counters, never payload bytes,
coordinates, weather facts, credentials, exception text or lease material.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import time
from types import MappingProxyType, SimpleNamespace
from typing import Any, Mapping

from .raw_artifacts import RawArtifactLimits, RawArtifactStore


CONTRACT = "stv3-worker-acquisition-child-request-v2"
RESULT_CONTRACT = "stv3-worker-acquisition-child-result-v2"
MAX_REQUEST_FIELDS = 8
LOCATION_GUARD_CONTRACT = "stv3-location-shadow-execution-guard-v1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_LIMIT_FIELDS = frozenset({
    "max_capture_entries",
    "max_capture_members",
    "max_catalog_bytes",
    "max_object_bytes",
    "max_object_entries",
    "max_receipt_entries",
    "max_staging_entries",
    "max_total_bytes",
})


@dataclass(frozen=True, slots=True)
class _RoleSpec:
    role: str
    raw_product: str
    fetch_suffix: str
    maximum_bytes: int
    allowed_content_types: frozenset[str]
    provenance_kind: str | None = None
    receipt_attempt_limit: int | None = None


@dataclass(frozen=True, slots=True)
class _DynamicRoleSpec:
    """One bounded, contiguous zero-based dynamic role family."""

    role_prefix: str
    role_index_width: int
    minimum_count: int
    maximum_count: int
    raw_product: str
    fetch_suffix_template: str
    maximum_bytes: int
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
        raise ValueError("dynamic role family has no closed pattern")

    def role_for_index(self, index: int) -> str:
        if (
            isinstance(index, bool)
            or not isinstance(index, int)
            or not 0 <= index < self.maximum_count
        ):
            raise ValueError("dynamic role index is outside its closed bound")
        return f"{self.role_prefix}{index:0{self.role_index_width}d}"

    def materialize(self, role: str) -> _RoleSpec:
        expected = tuple(self.role_for_index(index) for index in range(self.maximum_count))
        if role not in expected:
            raise ValueError("dynamic role is outside its closed family")
        return _RoleSpec(
            role=role,
            raw_product=self.raw_product,
            fetch_suffix=self.fetch_suffix_template.format(role=role),
            maximum_bytes=self.maximum_bytes,
            allowed_content_types=self.allowed_content_types,
            provenance_kind=self.provenance_kind,
        )


@dataclass(frozen=True, slots=True)
class _HandlerSpec:
    handler_id: str
    workload_id: str
    native_provider: str
    capture_product: str
    capture_fetch_suffix: str
    result_provider: str | None
    source_field: str
    source_hash_prefix: str
    roles: tuple[_RoleSpec, ...]
    decode_status: str
    maximum_transport_bytes: int
    satellite: int | None = None
    ledger_provider: str | None = None
    capture_provenance_kind: str | None = None
    capture_state: str = "captured_not_decoded"
    decode_field: str = "decode_status"
    payload_contract: str = "exact_empty_object_v1"
    safe_native_identity: bool = False
    source_size_optional: bool = False
    dynamic_role: _DynamicRoleSpec | None = None
    requires_execution_guard: bool | None = None

    @property
    def maximum_artifacts(self) -> int:
        return len(self.roles) + (
            0 if self.dynamic_role is None else self.dynamic_role.maximum_count
        )

    @property
    def minimum_artifacts(self) -> int:
        return len(self.roles) + (
            0 if self.dynamic_role is None else self.dynamic_role.minimum_count
        )

    @property
    def execution_guard_required(self) -> bool:
        if self.requires_execution_guard is None:
            return self.safe_native_identity
        return self.requires_execution_guard

    def role_specs_for_roles(self, roles: object) -> tuple[_RoleSpec, ...]:
        if not isinstance(roles, (set, frozenset, tuple, list, dict)):
            raise ValueError("native handler role inventory is invalid")
        actual = set(roles)
        static = {item.role for item in self.roles}
        if not static.issubset(actual):
            raise ValueError("native handler returned an incomplete static role set")
        dynamic = actual - static
        if self.dynamic_role is None:
            if dynamic:
                raise ValueError("native handler returned an unknown role")
            return self.roles
        count = len(dynamic)
        family = self.dynamic_role
        if not family.minimum_count <= count <= family.maximum_count:
            raise ValueError("native handler dynamic role count is outside its bound")
        expected = tuple(family.role_for_index(index) for index in range(count))
        if dynamic != set(expected):
            raise ValueError("native handler dynamic roles are not contiguous")
        return self.roles + tuple(family.materialize(role) for role in expected)


METAR_HANDLER_ID = "aviationweather_metar_cache_exact_raw_v1"
METAR_WORKLOAD_ID = "ground.metar.global"
AEMET_HANDLER_ID = "aemet_opendata_three_role_exact_raw_v1"
AEMET_WORKLOAD_ID = "radar.aemet.composite"
DWD_HANDLER_ID = "dwd_rv_tar_exact_raw_only_v1"
DWD_WORKLOAD_ID = "radar.dwd.radolan_rv"
KNMI_HANDLER_ID = "knmi_rtcor_hdf5_exact_raw_v1"
KNMI_WORKLOAD_ID = "radar.knmi.rtcor"
KMI_AWS_HANDLER_ID = "kmi_aws_10min_wfs_exact_raw_v1"
KMI_AWS_WORKLOAD_ID = "ground.kmi.aws_10min"
KMI_PUBLIC_RADAR_HANDLER_ID = "kmi_public_radar_wcs_exact_raw_v1"
KMI_PUBLIC_RADAR_WORKLOAD_ID = "radar.kmi.public_wcs_unknown_quantity"
KMI_APPV4_HANDLER_ID = "kmi_appv4_visual_exact_raw_only_v1"
KMI_APPV4_WORKLOAD_ID = "radar.kmi.appv4_visual"
OPERA_HANDLER_ID = "opera_dbzh_hdf5_exact_raw_only_v1"
OPERA_WORKLOAD_ID = "radar.opera.dbzh"
DPC_HANDLER_ID = "dpc_sri_catalog_geotiff_exact_raw_v1"
DPC_WORKLOAD_ID = "radar.dpc.sri"
NOAA_ERR_HANDLER_ID = "noaa_err_listing_netcdf4_exact_raw_v1"
NOAA_ERR_WORKLOAD_ID = "satellite.noaa.err"
GOES18_HANDLER_ID = "noaa_goes18_rrqpe_listings_netcdf4_exact_raw_v1"
GOES18_WORKLOAD_ID = "satellite.goes18.rrqpe"
GOES19_HANDLER_ID = "noaa_goes19_rrqpe_listings_netcdf4_exact_raw_v1"
GOES19_WORKLOAD_ID = "satellite.goes19.rrqpe"
EUMETSAT_LI_HANDLER_ID = "eumetsat_mtg_li_search_netcdf_exact_raw_v1"
EUMETSAT_LI_WORKLOAD_ID = "lightning.eumetsat.li"
GOES18_GLM_HANDLER_ID = "noaa_goes18_glm_listings_netcdf_exact_raw_v1"
GOES18_GLM_WORKLOAD_ID = "lightning.goes18.glm"
GOES19_GLM_HANDLER_ID = "noaa_goes19_glm_listings_netcdf_exact_raw_v1"
GOES19_GLM_WORKLOAD_ID = "lightning.goes19.glm"
METOFFICE_HANDLER_ID = "metoffice_uk_rate_listings_odim_exact_raw_v1"
METOFFICE_WORKLOAD_ID = "radar.metoffice.uk_rate"
METEOFRANCE_HANDLER_ID = "meteofrance_dpradar_catalog_hdf5_exact_raw_v1"
METEOFRANCE_WORKLOAD_ID = "radar.meteofrance.dpradar"
HSAF_H40B_HANDLER_ID = "hsaf_h40b_nlst_netcdf_gzip_exact_raw_v1"
HSAF_H40B_WORKLOAD_ID = "satellite.hsaf.h40b"
NETATMO_HANDLER_ID = "netatmo_getpublicdata_exact_raw_v1"
NETATMO_WORKLOAD_ID = "ground.netatmo.region"
SYNOPTIC_HANDLER_ID = "synoptic_timeseries_exact_raw_v1"
SYNOPTIC_WORKLOAD_ID = "ground.synoptic.region"
RAINVIEWER_HANDLER_ID = "rainviewer_visual_region_exact_raw_v1"
RAINVIEWER_WORKLOAD_ID = "radar.rainviewer.visual"
OPENMETEO_PUBLIC_HANDLER_ID = "openmeteo_forecast_unverified_exact_raw_v1"
OPENMETEO_PUBLIC_WORKLOAD_ID = "model.openmeteo.region"
OPENMETEO_ICON2I_PUBLIC_HANDLER_ID = (
    "openmeteo_italiameteo_icon2i_unverified_exact_raw_v1"
)
OPENMETEO_ICON2I_PUBLIC_WORKLOAD_ID = (
    "model.openmeteo.italiameteo_icon2i.region"
)
GEOSPHERE_PUBLIC_HANDLER_ID = (
    "geosphere_inca_location_unverified_exact_raw_v1"
)
GEOSPHERE_PUBLIC_WORKLOAD_ID = "validation.geosphere.location"
ITALIAMETEO_CATALOG_PUBLIC_HANDLER_ID = (
    "italiameteo_dpc_sri_catalog_unverified_exact_raw_v1"
)
ITALIAMETEO_CATALOG_PUBLIC_WORKLOAD_ID = (
    "validation.italiameteo.dpc_sri_historical_catalog"
)
METEOLUX_PUBLIC_HANDLER_ID = (
    "meteolux_location_weather_unverified_exact_raw_v1"
)
METEOLUX_PUBLIC_WORKLOAD_ID = "validation.meteolux.location"
PUBLIC_SOURCE_HANDLER_IDS = frozenset({
    OPENMETEO_PUBLIC_HANDLER_ID,
    OPENMETEO_ICON2I_PUBLIC_HANDLER_ID,
    GEOSPHERE_PUBLIC_HANDLER_ID,
    ITALIAMETEO_CATALOG_PUBLIC_HANDLER_ID,
    METEOLUX_PUBLIC_HANDLER_ID,
})
_PUBLIC_SOURCE_PROVENANCE_KINDS = frozenset({
    "openmeteo_unverified_response_v1",
    "openmeteo_icon2i_unverified_response_v1",
    "geosphere_inca_unverified_response_v1",
    "italiameteo_catalog_unverified_response_v1",
    "meteolux_weather_unverified_response_v1",
})
_EMPTY_EXACT_RESPONSE_PROVENANCE_KINDS = frozenset({
    *_PUBLIC_SOURCE_PROVENANCE_KINDS,
    "eumetsat_mtg_li_search_v1",
    "eumetsat_mtg_li_netcdf_v1",
    "kmi_appv4_forecast_response_v2",
    "kmi_appv4_image_response_v2",
    "noaa_goes_glm_listing_v1",
    "noaa_goes_glm_netcdf_v1",
})

# Compatibility aliases for the original METAR-only candidate.
HANDLER_ID = METAR_HANDLER_ID
WORKLOAD_ID = METAR_WORKLOAD_ID

_RAINVIEWER_ROLE_SPECS = tuple(sorted(
    (
        _RoleSpec(
            "manifest",
            "rainviewer_weather_maps_manifest_json",
            ":manifest",
            2 * 1024 * 1024,
            frozenset({"application/json", "text/json"}),
            "rainviewer_manifest_v1",
        ),
    )
    + tuple(
        _RoleSpec(
            f"{kind}_dx_{dx}_dy_{dy}",
            (
                "rainviewer_radar_coverage_png"
                if kind == "coverage"
                else "rainviewer_composite_reflectivity_png"
            ),
            f":{kind}_dx_{dx}_dy_{dy}",
            2 * 1024 * 1024,
            frozenset({"image/png"}),
            (
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


def _public_source_handler_spec(
    *,
    handler_id: str,
    workload_id: str,
    provider: str,
    raw_product: str,
    capture_product: str,
    provenance_kind: str,
    maximum_transport_bytes: int,
    payload_contract: str,
) -> _HandlerSpec:
    return _HandlerSpec(
        handler_id=handler_id,
        workload_id=workload_id,
        native_provider=provider,
        capture_product=capture_product,
        capture_fetch_suffix=":capture",
        result_provider=provider,
        source_field="sources",
        source_hash_prefix="",
        roles=(_RoleSpec(
            role="response_bytes",
            raw_product=raw_product,
            fetch_suffix=":response_bytes",
            maximum_bytes=maximum_transport_bytes,
            allowed_content_types=frozenset({"application/octet-stream"}),
            provenance_kind=provenance_kind,
        ),),
        decode_status="not_decoded",
        maximum_transport_bytes=maximum_transport_bytes,
        ledger_provider=provider,
        capture_provenance_kind=provenance_kind,
        payload_contract=payload_contract,
        safe_native_identity=True,
    )

_HANDLERS = MappingProxyType({
    METAR_HANDLER_ID: _HandlerSpec(
        METAR_HANDLER_ID,
        METAR_WORKLOAD_ID,
        "metar",
        "aviationweather_current_metars_capture",
        ":capture",
        "metar",
        "raw_sources",
        "raw_",
        (_RoleSpec(
            "csv_gzip",
            "aviationweather_current_metars_csv_gzip",
            ":csv_gzip",
            16 * 1024 * 1024,
            frozenset({
                "application/gzip",
                "application/octet-stream",
                "application/x-gzip",
            }),
            "metar_v2",
        ),),
        "not_meteorologically_decoded",
        16 * 1024 * 1024,
        ledger_provider="aviationweather",
        capture_provenance_kind="metar_v2",
    ),
    AEMET_HANDLER_ID: _HandlerSpec(
        AEMET_HANDLER_ID,
        AEMET_WORKLOAD_ID,
        "aemet_radar",
        "aemet_national_radar_opendata_exact_capture",
        ":capture",
        None,
        "sources",
        "",
        (
            _RoleSpec(
                "opendata_data_response",
                "aemet_national_radar_opendata_data_response",
                ":opendata_data_response",
                96 * 1024 * 1024,
                frozenset({
                    "application/gzip",
                    "application/octet-stream",
                    "application/x-gzip",
                    "application/x-tar",
                    "application/zip",
                    "binary/octet-stream",
                    "image/tiff",
                }),
                "aemet_data_v1",
            ),
            _RoleSpec(
                "opendata_envelope_json",
                "aemet_national_radar_opendata_envelope_json",
                ":opendata_envelope_json",
                64 * 1024,
                frozenset({"application/json"}),
                "aemet_envelope_v1",
            ),
            _RoleSpec(
                "opendata_metadata_response",
                "aemet_national_radar_opendata_metadata_response",
                ":opendata_metadata_response",
                4 * 1024 * 1024,
                frozenset({
                    "application/json",
                    "application/octet-stream",
                    "text/plain",
                    "text/xml",
                }),
                "aemet_metadata_v1",
            ),
        ),
        "blocked_no_certified_geotiff_decoder",
        101 * 1024 * 1024,
        ledger_provider="aemet",
        capture_provenance_kind="aemet_opendata_v1",
    ),
    DWD_HANDLER_ID: _HandlerSpec(
        DWD_HANDLER_ID,
        DWD_WORKLOAD_ID,
        "dwd",
        "dwd_open_data_composite_rv_raw_capture",
        ":capture",
        "dwd",
        "raw_sources",
        "raw_",
        (_RoleSpec(
            "rv_tar",
            "dwd_open_data_composite_rv_tar",
            ":rv_tar",
            8 * 1024 * 1024,
            frozenset({
                "application/octet-stream",
                "application/x-tar",
                "binary/octet-stream",
            }),
            "dwd_rv_v2",
        ),),
        "not_decoded",
        8 * 1024 * 1024,
        ledger_provider="dwd",
        capture_provenance_kind="dwd_rv_v2",
    ),
    KNMI_HANDLER_ID: _HandlerSpec(
        KNMI_HANDLER_ID,
        KNMI_WORKLOAD_ID,
        "knmi",
        "knmi_nl_rdr_data_rtcor_5m_capture",
        ":capture",
        "knmi",
        "raw_sources",
        "raw_",
        (_RoleSpec(
            "hdf5",
            "knmi_nl_rdr_data_rtcor_5m_hdf5",
            ":hdf5",
            40 * 1024 * 1024,
            frozenset({"application/octet-stream", "application/x-hdf5"}),
            "knmi_rtcor_v1",
        ),),
        "not_decoded",
        40 * 1024 * 1024,
        ledger_provider="knmi",
        capture_provenance_kind="knmi_rtcor_v1",
    ),
    KMI_AWS_HANDLER_ID: _HandlerSpec(
        KMI_AWS_HANDLER_ID,
        KMI_AWS_WORKLOAD_ID,
        "kmi",
        "kmi_aws_10min_wfs_exact_capture",
        ":capture",
        "kmi",
        "sources",
        "",
        (_RoleSpec(
            "geoserver_response",
            "kmi_aws_10min_wfs_exact_response",
            ":response",
            32 * 1024 * 1024,
            frozenset({
                "application/geo+json",
                "application/json",
                "application/octet-stream",
                "text/plain",
                "text/xml",
            }),
            "kmi_opendata_response_v1",
        ),),
        "not_attempted",
        32 * 1024 * 1024,
        ledger_provider="kmi",
        capture_provenance_kind="kmi_opendata_v1",
        capture_state="captured_exact_raw",
        payload_contract="canonical_previous_ten_minute_window_v1",
    ),
    KMI_PUBLIC_RADAR_HANDLER_ID: _HandlerSpec(
        KMI_PUBLIC_RADAR_HANDLER_ID,
        KMI_PUBLIC_RADAR_WORKLOAD_ID,
        "kmi",
        "kmi_public_radar_wcs_exact_capture",
        ":capture",
        "kmi",
        "sources",
        "",
        (_RoleSpec(
            "geoserver_response",
            "kmi_public_radar_wcs_exact_response",
            ":response",
            64 * 1024 * 1024,
            frozenset({
                "application/octet-stream",
                "binary/octet-stream",
                "image/geotiff",
                "image/tiff",
            }),
            "kmi_opendata_response_v1",
        ),),
        "not_attempted",
        64 * 1024 * 1024,
        ledger_provider="kmi",
        capture_provenance_kind="kmi_opendata_v1",
        capture_state="captured_exact_raw",
        # GeoServer must select its declared default/latest mosaic.  Binding a
        # wall-clock five-minute timestamp recreates the invalid/404 request
        # that this route deliberately replaced.  Keep this identical to the
        # activation rule and to KMI_PUBLIC_RADAR_LATEST.
        payload_contract="exact_empty_object_v1",
    ),
    KMI_APPV4_HANDLER_ID: _HandlerSpec(
        KMI_APPV4_HANDLER_ID,
        KMI_APPV4_WORKLOAD_ID,
        "kmi",
        "kmi_appv4_visualisation_capture",
        ":capture",
        "kmi",
        "sources",
        "",
        (
            _RoleSpec(
                "forecast_json",
                "kmi_appv4_forecast_manifest",
                ":forecast_json",
                4 * 1024 * 1024,
                frozenset({"application/octet-stream"}),
                "kmi_appv4_forecast_response_v2",
            ),
            _RoleSpec(
                "radar_png",
                "kmi_appv4_visualisation_png",
                ":radar_png",
                8 * 1024 * 1024,
                frozenset({"application/octet-stream"}),
                "kmi_appv4_image_response_v2",
            ),
        ),
        "not_attempted",
        12 * 1024 * 1024,
        ledger_provider="kmi",
        capture_provenance_kind="kmi_appv4_visual_capture_v2",
        payload_contract="public_region_query_v1",
        safe_native_identity=True,
    ),
    OPERA_HANDLER_ID: _HandlerSpec(
        OPERA_HANDLER_ID,
        OPERA_WORKLOAD_ID,
        "opera",
        "eumetnet_opera_cirrus_dbzh_capture",
        ":capture",
        "opera",
        "raw_sources",
        "raw_",
        (_RoleSpec(
            "hdf5",
            "eumetnet_opera_cirrus_dbzh_odim_hdf5",
            ":hdf5",
            384 * 1024 * 1024,
            frozenset({
                "application/octet-stream",
                "application/x-hdf5",
                "binary/octet-stream",
            }),
            "opera_stream_raw_v1",
        ),),
        "not_attempted",
        384 * 1024 * 1024,
        ledger_provider="opera",
        capture_provenance_kind="opera_stream_raw_v1",
        capture_state="exact_raw_committed",
        payload_contract="canonical_previous_complete_slot_timestamp_v1",
    ),
    DPC_HANDLER_ID: _HandlerSpec(
        DPC_HANDLER_ID,
        DPC_WORKLOAD_ID,
        "dpc_radar",
        "dpc_sri_catalog_geotiff_capture",
        ":capture",
        None,
        "sources",
        "",
        (
            _RoleSpec(
                "catalog_json",
                "dpc_sri_last_product_catalog_json",
                ":catalog_json",
                2 * 1024 * 1024,
                frozenset({"application/json", "text/json"}),
                "dpc_catalog_v1",
            ),
            _RoleSpec(
                "geotiff",
                "dpc_sri_original_geotiff",
                ":geotiff",
                25 * 1024 * 1024,
                frozenset({
                    "application/octet-stream",
                    "binary/octet-stream",
                    "image/geotiff",
                    "image/tiff",
                }),
                "dpc_geotiff_v1",
            ),
        ),
        "not_decoded",
        32 * 1024 * 1024,
        ledger_provider="dpc",
        capture_provenance_kind="dpc_sri_v1",
    ),
    NOAA_ERR_HANDLER_ID: _HandlerSpec(
        NOAA_ERR_HANDLER_ID,
        NOAA_ERR_WORKLOAD_ID,
        "noaa_enterprise_rainrate",
        "noaa_err_listing_and_netcdf4_capture",
        ":capture",
        "noaa_enterprise_rainrate",
        "raw_sources",
        "raw_",
        (
            _RoleSpec(
                "listing_xml",
                "noaa_err_s3_listobjects_v2_xml",
                ":listing_xml",
                1 * 1024 * 1024,
                frozenset({"application/xml", "text/xml"}),
                "noaa_err_listing_v1",
            ),
            _RoleSpec(
                "netcdf4",
                "noaa_err_instantaneous_glb5_netcdf4",
                ":netcdf4",
                32 * 1024 * 1024,
                frozenset({
                    "application/netcdf",
                    "application/octet-stream",
                    "application/x-netcdf",
                    "binary/octet-stream",
                }),
                "noaa_err_netcdf4_v1",
            ),
        ),
        "not_decoded",
        36 * 1024 * 1024,
        ledger_provider="noaa",
        capture_provenance_kind="noaa_err_v1",
    ),
    GOES18_HANDLER_ID: _HandlerSpec(
        GOES18_HANDLER_ID,
        GOES18_WORKLOAD_ID,
        "noaa_goes_rrqpe",
        "noaa_goes_rrqpe_exact_native_capture",
        ":G18:capture",
        None,
        "sources",
        "",
        (
            _RoleSpec(
                "listing_current",
                "noaa_goes_rrqpe_s3_listobjects_v2_xml",
                ":G18:listing_current",
                2 * 1024 * 1024,
                frozenset({"application/xml", "text/xml"}),
                "goes_rrqpe_listing_v1",
            ),
            _RoleSpec(
                "listing_previous",
                "noaa_goes_rrqpe_s3_listobjects_v2_xml",
                ":G18:listing_previous",
                2 * 1024 * 1024,
                frozenset({"application/xml", "text/xml"}),
                "goes_rrqpe_listing_v1",
            ),
            _RoleSpec(
                "netcdf4",
                "noaa_goes_rrqpe_abi_l2_original_netcdf4",
                ":G18:netcdf4",
                16 * 1024 * 1024,
                frozenset({
                    "application/netcdf",
                    "application/octet-stream",
                    "application/x-netcdf",
                    "binary/octet-stream",
                }),
                "goes_rrqpe_netcdf4_v1",
            ),
        ),
        "not_decoded",
        20 * 1024 * 1024,
        18,
        ledger_provider="noaa_goes",
        capture_provenance_kind="goes18_rrqpe_v1",
    ),
    GOES19_HANDLER_ID: _HandlerSpec(
        GOES19_HANDLER_ID,
        GOES19_WORKLOAD_ID,
        "noaa_goes_rrqpe",
        "noaa_goes_rrqpe_exact_native_capture",
        ":G19:capture",
        None,
        "sources",
        "",
        (
            _RoleSpec(
                "listing_current",
                "noaa_goes_rrqpe_s3_listobjects_v2_xml",
                ":G19:listing_current",
                2 * 1024 * 1024,
                frozenset({"application/xml", "text/xml"}),
                "goes_rrqpe_listing_v1",
            ),
            _RoleSpec(
                "listing_previous",
                "noaa_goes_rrqpe_s3_listobjects_v2_xml",
                ":G19:listing_previous",
                2 * 1024 * 1024,
                frozenset({"application/xml", "text/xml"}),
                "goes_rrqpe_listing_v1",
            ),
            _RoleSpec(
                "netcdf4",
                "noaa_goes_rrqpe_abi_l2_original_netcdf4",
                ":G19:netcdf4",
                16 * 1024 * 1024,
                frozenset({
                    "application/netcdf",
                    "application/octet-stream",
                    "application/x-netcdf",
                    "binary/octet-stream",
                }),
                "goes_rrqpe_netcdf4_v1",
            ),
        ),
        "not_decoded",
        20 * 1024 * 1024,
        19,
        ledger_provider="noaa_goes",
        capture_provenance_kind="goes19_rrqpe_v1",
    ),
    EUMETSAT_LI_HANDLER_ID: _HandlerSpec(
        handler_id=EUMETSAT_LI_HANDLER_ID,
        workload_id=EUMETSAT_LI_WORKLOAD_ID,
        native_provider="eumetsat",
        capture_product="eumetsat_mtg_li_exact_raw_capture",
        capture_fetch_suffix=":capture",
        result_provider="eumetsat",
        source_field="sources",
        source_hash_prefix="",
        roles=(
            _RoleSpec(
                "search_response",
                "eumetsat_mtg_li_search_response_bytes",
                ":search_response",
                4 * 1024 * 1024,
                frozenset({"application/octet-stream"}),
                "eumetsat_mtg_li_search_v1",
            ),
            _RoleSpec(
                "netcdf4",
                "eumetsat_mtg_li_original_netcdf_response_bytes",
                ":netcdf4",
                10 * 1024 * 1024,
                frozenset({"application/octet-stream"}),
                "eumetsat_mtg_li_netcdf_v1",
            ),
        ),
        decode_status="not_decoded",
        maximum_transport_bytes=14 * 1024 * 1024,
        ledger_provider="eumetsat",
        capture_provenance_kind="eumetsat_mtg_li_exact_raw_v1",
        safe_native_identity=True,
        requires_execution_guard=False,
    ),
    GOES18_GLM_HANDLER_ID: _HandlerSpec(
        handler_id=GOES18_GLM_HANDLER_ID,
        workload_id=GOES18_GLM_WORKLOAD_ID,
        native_provider="noaa_goes",
        capture_product="noaa_goes_glm_exact_raw_capture",
        capture_fetch_suffix=":capture",
        result_provider="noaa_goes",
        source_field="sources",
        source_hash_prefix="",
        roles=(
            _RoleSpec(
                "listing_previous",
                "noaa_goes_glm_s3_listobjects_v2_response_bytes",
                ":listing_previous",
                4 * 1024 * 1024,
                frozenset({"application/octet-stream"}),
                "noaa_goes_glm_listing_v1",
            ),
            _RoleSpec(
                "listing_current",
                "noaa_goes_glm_s3_listobjects_v2_response_bytes",
                ":listing_current",
                4 * 1024 * 1024,
                frozenset({"application/octet-stream"}),
                "noaa_goes_glm_listing_v1",
            ),
        ),
        decode_status="not_decoded",
        maximum_transport_bytes=68 * 1024 * 1024,
        satellite=18,
        ledger_provider="noaa_goes",
        capture_provenance_kind="noaa_goes_glm_exact_raw_v1",
        safe_native_identity=True,
        dynamic_role=_DynamicRoleSpec(
            role_prefix="netcdf_",
            role_index_width=2,
            minimum_count=1,
            maximum_count=12,
            raw_product="noaa_goes_glm_original_netcdf_response_bytes",
            fetch_suffix_template=":{role}",
            maximum_bytes=5 * 1024 * 1024,
            allowed_content_types=frozenset({"application/octet-stream"}),
            provenance_kind="noaa_goes_glm_netcdf_v1",
        ),
        requires_execution_guard=False,
    ),
    GOES19_GLM_HANDLER_ID: _HandlerSpec(
        handler_id=GOES19_GLM_HANDLER_ID,
        workload_id=GOES19_GLM_WORKLOAD_ID,
        native_provider="noaa_goes",
        capture_product="noaa_goes_glm_exact_raw_capture",
        capture_fetch_suffix=":capture",
        result_provider="noaa_goes",
        source_field="sources",
        source_hash_prefix="",
        roles=(
            _RoleSpec(
                "listing_previous",
                "noaa_goes_glm_s3_listobjects_v2_response_bytes",
                ":listing_previous",
                4 * 1024 * 1024,
                frozenset({"application/octet-stream"}),
                "noaa_goes_glm_listing_v1",
            ),
            _RoleSpec(
                "listing_current",
                "noaa_goes_glm_s3_listobjects_v2_response_bytes",
                ":listing_current",
                4 * 1024 * 1024,
                frozenset({"application/octet-stream"}),
                "noaa_goes_glm_listing_v1",
            ),
        ),
        decode_status="not_decoded",
        maximum_transport_bytes=68 * 1024 * 1024,
        satellite=19,
        ledger_provider="noaa_goes",
        capture_provenance_kind="noaa_goes_glm_exact_raw_v1",
        safe_native_identity=True,
        dynamic_role=_DynamicRoleSpec(
            role_prefix="netcdf_",
            role_index_width=2,
            minimum_count=1,
            maximum_count=12,
            raw_product="noaa_goes_glm_original_netcdf_response_bytes",
            fetch_suffix_template=":{role}",
            maximum_bytes=5 * 1024 * 1024,
            allowed_content_types=frozenset({"application/octet-stream"}),
            provenance_kind="noaa_goes_glm_netcdf_v1",
        ),
        requires_execution_guard=False,
    ),
    METOFFICE_HANDLER_ID: _HandlerSpec(
        METOFFICE_HANDLER_ID,
        METOFFICE_WORKLOAD_ID,
        "met_office",
        "met_office_uk_radar_selection_capture",
        ":capture",
        None,
        "sources",
        "",
        (
            _RoleSpec(
                "listing_current",
                "met_office_uk_radar_s3_listobjects_v2_xml",
                ":listing_current",
                2 * 1024 * 1024,
                frozenset({"application/xml", "text/xml"}),
                "metoffice_listing_v1",
            ),
            _RoleSpec(
                "listing_previous",
                "met_office_uk_radar_s3_listobjects_v2_xml",
                ":listing_previous",
                2 * 1024 * 1024,
                frozenset({"application/xml", "text/xml"}),
                "metoffice_listing_v1",
            ),
            _RoleSpec(
                "odim_hdf5",
                "met_office_uk_radar_rainrate_odim_hdf5",
                ":odim_hdf5",
                20 * 1024 * 1024,
                frozenset({
                    "application/octet-stream",
                    "application/x-hdf5",
                    "binary/octet-stream",
                }),
                "metoffice_odim_v1",
            ),
        ),
        "not_decoded",
        32 * 1024 * 1024,
        ledger_provider="metoffice",
        capture_provenance_kind="metoffice_uk_rate_v1",
    ),
    METEOFRANCE_HANDLER_ID: _HandlerSpec(
        METEOFRANCE_HANDLER_ID,
        METEOFRANCE_WORKLOAD_ID,
        "meteofrance_radar",
        "meteofrance_dpradar_catalog_hdf5_capture",
        ":capture",
        None,
        "sources",
        "",
        (
            _RoleSpec(
                "catalog_json",
                "meteofrance_dpradar_metro_lame_eau_catalog_json",
                ":catalog_json",
                512 * 1024,
                frozenset({"application/hal+json", "application/json"}),
                "meteofrance_catalog_v1",
            ),
            _RoleSpec(
                "hdf5",
                "meteofrance_iprn20_metro_lame_eau_500m_hdf5",
                ":hdf5",
                40 * 1024 * 1024,
                frozenset({
                    "application/octet-stream",
                    "application/x-hdf",
                    "application/x-hdf5",
                    "binary/octet-stream",
                }),
                "meteofrance_hdf5_v1",
            ),
        ),
        "blocked_incomplete_hdf5_semantics",
        48 * 1024 * 1024,
        ledger_provider="meteofrance",
        capture_provenance_kind="meteofrance_dpradar_v1",
    ),
    HSAF_H40B_HANDLER_ID: _HandlerSpec(
        HSAF_H40B_HANDLER_ID,
        HSAF_H40B_WORKLOAD_ID,
        "hsaf_h40b",
        "hsaf_h40b_ftp_selection_netcdf_gzip_capture",
        ":capture",
        None,
        "sources",
        "",
        (
            _RoleSpec(
                "listing_nlst",
                "hsaf_h40b_official_ftp_nlst",
                ":listing_nlst",
                2 * 1024 * 1024,
                frozenset({"text/plain"}),
                "hsaf_h40b_listing_v1",
            ),
            _RoleSpec(
                "netcdf_gzip",
                "hsaf_h40b_pin_fci_original_netcdf_gzip",
                ":netcdf_gzip",
                64 * 1024 * 1024,
                frozenset({"application/gzip"}),
                "hsaf_h40b_netcdf_gzip_v1",
            ),
        ),
        "not_decoded",
        66 * 1024 * 1024,
        ledger_provider="hsaf",
        capture_provenance_kind="hsaf_h40b_v1",
    ),
    NETATMO_HANDLER_ID: _HandlerSpec(
        NETATMO_HANDLER_ID,
        NETATMO_WORKLOAD_ID,
        "netatmo",
        "netatmo_getpublicdata_query_capture",
        ":capture",
        "netatmo",
        "raw_sources",
        "raw_",
        (_RoleSpec(
            "response_json",
            "netatmo_getpublicdata_json_response",
            ":response_json",
            8 * 1024 * 1024,
            frozenset({"application/json"}),
            "netatmo_v2",
        ),),
        "not_decoded",
        8 * 1024 * 1024,
        ledger_provider="netatmo",
        capture_provenance_kind="netatmo_v2",
        capture_state="captured_structural_only",
        decode_field="measurement_semantics",
        payload_contract="acquisition_query_authority_projection_v1",
        safe_native_identity=True,
        source_size_optional=True,
    ),
    SYNOPTIC_HANDLER_ID: _HandlerSpec(
        SYNOPTIC_HANDLER_ID,
        SYNOPTIC_WORKLOAD_ID,
        "synoptic",
        "synoptic_timeseries_query_capture",
        ":capture",
        "synoptic",
        "raw_sources",
        "raw_",
        (_RoleSpec(
            "response_json",
            "synoptic_timeseries_json_response",
            ":response_json:attempt:",
            16 * 1024 * 1024,
            frozenset({"application/json"}),
            "synoptic_v5",
            8,
        ),),
        "not_meteorologically_decoded",
        16 * 1024 * 1024,
        ledger_provider="synoptic",
        capture_provenance_kind="synoptic_v5",
        payload_contract="acquisition_query_authority_projection_v1",
        safe_native_identity=True,
        source_size_optional=True,
    ),
    RAINVIEWER_HANDLER_ID: _HandlerSpec(
        RAINVIEWER_HANDLER_ID,
        RAINVIEWER_WORKLOAD_ID,
        "rainviewer",
        "rainviewer_region_visual_capture",
        ":capture",
        "rainviewer",
        "raw_sources",
        "raw_",
        _RAINVIEWER_ROLE_SPECS,
        "not_decoded",
        38 * 1024 * 1024,
        ledger_provider="rainviewer",
        capture_provenance_kind="rainviewer_visual_v1",
        payload_contract="acquisition_query_authority_projection_v1",
        safe_native_identity=True,
        source_size_optional=True,
    ),
    OPENMETEO_PUBLIC_HANDLER_ID: _public_source_handler_spec(
        handler_id=OPENMETEO_PUBLIC_HANDLER_ID,
        workload_id=OPENMETEO_PUBLIC_WORKLOAD_ID,
        provider="openmeteo",
        raw_product=(
            "openmeteo_forecast_native_unverified_response_bytes"
        ),
        capture_product=(
            "openmeteo_forecast_native_unverified_exact_capture"
        ),
        provenance_kind="openmeteo_unverified_response_v1",
        maximum_transport_bytes=8 * 1024 * 1024,
        payload_contract="openmeteo_target_query_batch_v1",
    ),
    OPENMETEO_ICON2I_PUBLIC_HANDLER_ID: _public_source_handler_spec(
        handler_id=OPENMETEO_ICON2I_PUBLIC_HANDLER_ID,
        workload_id=OPENMETEO_ICON2I_PUBLIC_WORKLOAD_ID,
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
        payload_contract="public_region_query_v1",
    ),
    GEOSPHERE_PUBLIC_HANDLER_ID: _public_source_handler_spec(
        handler_id=GEOSPHERE_PUBLIC_HANDLER_ID,
        workload_id=GEOSPHERE_PUBLIC_WORKLOAD_ID,
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
        payload_contract="public_region_query_v1",
    ),
    ITALIAMETEO_CATALOG_PUBLIC_HANDLER_ID: _public_source_handler_spec(
        handler_id=ITALIAMETEO_CATALOG_PUBLIC_HANDLER_ID,
        workload_id=ITALIAMETEO_CATALOG_PUBLIC_WORKLOAD_ID,
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
        payload_contract="exact_empty_object_v1",
    ),
    METEOLUX_PUBLIC_HANDLER_ID: _public_source_handler_spec(
        handler_id=METEOLUX_PUBLIC_HANDLER_ID,
        workload_id=METEOLUX_PUBLIC_WORKLOAD_ID,
        provider="meteolux",
        raw_product=(
            "meteolux_location_weather_validation_unverified_response_bytes"
        ),
        capture_product=(
            "meteolux_location_weather_validation_unverified_exact_capture"
        ),
        provenance_kind="meteolux_weather_unverified_response_v1",
        maximum_transport_bytes=4 * 1024 * 1024,
        payload_contract="public_region_query_v1",
    ),
})


def _validate_handlers() -> None:
    if set(_HANDLERS) != {spec.handler_id for spec in _HANDLERS.values()}:
        raise RuntimeError("closed child handler identities are inconsistent")
    workload_ids: set[str] = set()
    for spec in _HANDLERS.values():
        roles = tuple(role.role for role in spec.roles)
        if (
            spec.workload_id in workload_ids
            or not roles
            or len(roles) != len(set(roles))
            or spec.source_field not in {"raw_sources", "sources"}
            or spec.source_hash_prefix not in {"", "raw_"}
            or spec.decode_field not in {"decode_status", "measurement_semantics"}
            or spec.payload_contract not in {
                "exact_empty_object_v1",
                "acquisition_query_authority_projection_v1",
                "canonical_previous_ten_minute_window_v1",
                "canonical_previous_complete_slot_timestamp_v1",
                "openmeteo_target_query_batch_v1",
                "public_region_query_v1",
            }
            or (
                sum(role.maximum_bytes for role in spec.roles)
                + (
                    0
                    if spec.dynamic_role is None
                    else spec.dynamic_role.maximum_count
                    * spec.dynamic_role.maximum_bytes
                )
            )
            > spec.maximum_transport_bytes
            or any(
                not role.allowed_content_types
                or not role.fetch_suffix.startswith(":")
                or role.fetch_suffix == spec.capture_fetch_suffix
                or (
                    role.receipt_attempt_limit is not None
                    and (
                        isinstance(role.receipt_attempt_limit, bool)
                        or not isinstance(role.receipt_attempt_limit, int)
                        or role.receipt_attempt_limit <= 0
                        or not role.fetch_suffix.endswith(":")
                    )
                )
                for role in spec.roles
            )
            or (
                spec.dynamic_role is not None
                and (
                    spec.dynamic_role.role_prefix != "netcdf_"
                    or spec.dynamic_role.role_index_width != 2
                    or spec.dynamic_role.minimum_count != 1
                    or spec.dynamic_role.maximum_count != 12
                    or spec.dynamic_role.fetch_suffix_template != ":{role}"
                    or not spec.dynamic_role.allowed_content_types
                    or spec.dynamic_role.provenance_kind
                    != "noaa_goes_glm_netcdf_v1"
                    or spec.dynamic_role.role_pattern
                    != r"^netcdf_(?P<index>0[0-9]|1[01])$"
                )
            )
            or (
                spec.requires_execution_guard is False
                and spec.handler_id not in {
                    EUMETSAT_LI_HANDLER_ID,
                    GOES18_GLM_HANDLER_ID,
                    GOES19_GLM_HANDLER_ID,
                }
            )
        ):
            raise RuntimeError("closed child handler contract is invalid")
        workload_ids.add(spec.workload_id)


_validate_handlers()


def _exact_string(value: object, name: str, maximum: int = 512) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"invalid {name}")
    return value


def _limits(value: object) -> RawArtifactLimits:
    if not isinstance(value, Mapping) or set(value) != _LIMIT_FIELDS:
        raise ValueError("raw store limits are incomplete")
    normalized: dict[str, int] = {}
    for name in sorted(_LIMIT_FIELDS):
        item = value[name]
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            raise ValueError("raw store limit is invalid")
        normalized[name] = item
    return RawArtifactLimits(**normalized)


def _hash(value: object, name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"invalid {name}")
    return value


def _canonical_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(
        dict(value), allow_nan=False, ensure_ascii=False,
        separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")).hexdigest()


def _finite_time(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"invalid {name}")
    result = float(value)
    if not (0.0 <= result <= 2**53) or result != result or result in {
        float("inf"), float("-inf")
    }:
        raise ValueError(f"invalid {name}")
    return result


def _validate_execution_guard(
    guard: object,
    *,
    job_id: str,
    handler_payload: dict[str, Any],
    clock: Any,
    authority_required: bool,
) -> None:
    expected = {
        "authority_sha256",
        "contract",
        "deadline_at",
        "dynamic_position_valid_from",
        "dynamic_position_valid_until",
        "evaluated_at",
        "guard_sha256",
        "job_sha256",
        "native_job_id_sha256",
        "payload_sha256",
        "scheduled_for",
    }
    if type(guard) is not dict or set(guard) != expected:
        raise ValueError("location execution guard is incomplete")
    if guard["contract"] != LOCATION_GUARD_CONTRACT:
        raise ValueError("location execution guard contract mismatch")
    guard_sha = _hash(guard["guard_sha256"], "execution guard SHA-256")
    body = {name: guard[name] for name in sorted(expected - {"guard_sha256"})}
    if _canonical_hash(body) != guard_sha:
        raise ValueError("location execution guard checksum mismatch")
    authority_sha = guard["authority_sha256"]
    if authority_sha is not None:
        _hash(authority_sha, "query authority SHA-256")
    if authority_required != (authority_sha is not None):
        raise ValueError("location query authority binding is inconsistent")
    _hash(guard["job_sha256"], "acquisition job SHA-256")
    if guard["payload_sha256"] != _canonical_hash(handler_payload):
        raise ValueError("location handler payload drifted")
    from .acquisition_query_authority import safe_native_job_id

    native_id = safe_native_job_id(job_id)
    if guard["native_job_id_sha256"] != hashlib.sha256(
        native_id.encode("ascii")
    ).hexdigest():
        raise ValueError("location native identity drifted")
    scheduled = _finite_time(guard["scheduled_for"], "scheduled_for")
    deadline = _finite_time(guard["deadline_at"], "deadline_at")
    evaluated = _finite_time(guard["evaluated_at"], "evaluated_at")
    if not scheduled <= evaluated < deadline:
        raise ValueError("location execution guard had no valid job window")
    valid_from = guard["dynamic_position_valid_from"]
    valid_until = guard["dynamic_position_valid_until"]
    if (valid_from is None) != (valid_until is None):
        raise ValueError("dynamic-position guard is incomplete")
    if valid_from is not None:
        valid_from = _finite_time(valid_from, "dynamic position valid from")
        valid_until = _finite_time(valid_until, "dynamic position valid until")
        if valid_from > valid_until:
            raise ValueError("dynamic-position guard is inverted")
    try:
        now = _finite_time(clock(), "trusted worker clock")
    except Exception as err:
        raise ValueError("trusted worker clock failed") from err
    if not scheduled <= now < deadline:
        raise ValueError("location job is outside its trusted execution window")
    if valid_from is not None and not valid_from <= now <= valid_until:
        raise ValueError("dynamic target position is not fresh at execution")


def _run_native(
    spec: _HandlerSpec,
    *,
    job_id: str,
    store: RawArtifactStore,
    handler_payload: dict[str, Any] | None = None,
    execution_deadline_at: float | None = None,
) -> dict[str, Any]:
    """Closed dispatch; no caller-controlled import reference is accepted."""

    native_job_id = job_id
    if spec.safe_native_identity:
        from .acquisition_query_authority import safe_native_job_id

        native_job_id = safe_native_job_id(job_id)
    job = SimpleNamespace(
        job_id=native_job_id,
        payload={} if handler_payload is None else dict(handler_payload),
    )
    if spec.handler_id == METAR_HANDLER_ID:
        from .metar_native import capture_latest_metar_cache

        return capture_latest_metar_cache(job, raw_store=store)
    if spec.handler_id == AEMET_HANDLER_ID:
        from .aemet_native import capture_latest_aemet_national_radar

        return capture_latest_aemet_national_radar(job, raw_store=store)
    if spec.handler_id == DWD_HANDLER_ID:
        from .dwd_rv_native import capture_latest_dwd_rv_tar

        return capture_latest_dwd_rv_tar(job, raw_store=store)
    if spec.handler_id == KNMI_HANDLER_ID:
        from .knmi_native import capture_latest_knmi_hdf5

        return capture_latest_knmi_hdf5(job, raw_store=store)
    if spec.handler_id == KMI_AWS_HANDLER_ID:
        from .kmi_opendata_raw import capture_kmi_aws_10min_window

        if handler_payload is None:
            raise ValueError("KMI AWS handler mist zijn vensterbinding")
        return capture_kmi_aws_10min_window(
            SimpleNamespace(job_id=job_id, payload={}),
            start_at=datetime.fromtimestamp(
                handler_payload["window_start"], timezone.utc
            ),
            end_at=datetime.fromtimestamp(
                handler_payload["window_end"], timezone.utc
            ),
            raw_store=store,
        )
    if spec.handler_id == KMI_PUBLIC_RADAR_HANDLER_ID:
        from .kmi_opendata_raw import (
            KMI_PUBLIC_RADAR_LATEST,
            capture_kmi_opendata,
        )

        return capture_kmi_opendata(
            SimpleNamespace(job_id=job_id, payload={}),
            descriptor=KMI_PUBLIC_RADAR_LATEST,
            raw_store=store,
        )
    if spec.handler_id == KMI_APPV4_HANDLER_ID:
        from .kmi_appv4_raw_capture import capture_kmi_appv4_visual_raw

        return capture_kmi_appv4_visual_raw(job, raw_store=store)
    if spec.handler_id == OPERA_HANDLER_ID:
        from .opera_native import (
            _canonical_job_id,
            capture_opera_dbzh_hdf5_raw_only,
        )

        if handler_payload is None:
            raise ValueError("OPERA handler mist zijn slotbinding")
        timestamp = handler_payload.get("product_timestamp")
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            raise ValueError("OPERA handler heeft een ongeldig slot")
        opera_job = SimpleNamespace(
            job_id=_canonical_job_id(float(timestamp)),
            payload={"product_timestamp": timestamp},
        )
        return capture_opera_dbzh_hdf5_raw_only(
            opera_job,
            raw_store=store,
            raw_identity=job_id,
        )
    if spec.handler_id == DPC_HANDLER_ID:
        from .dpc_native import capture_latest_dpc_sri

        return capture_latest_dpc_sri(job, raw_store=store)
    if spec.handler_id == NOAA_ERR_HANDLER_ID:
        from .noaa_err_native import capture_latest_noaa_err_netcdf4

        return capture_latest_noaa_err_netcdf4(job, raw_store=store)
    if spec.handler_id in {GOES18_HANDLER_ID, GOES19_HANDLER_ID}:
        from .goes_rrqpe_native import capture_latest_goes_rrqpe

        if spec.satellite is None:  # pragma: no cover - closed table invariant
            raise ValueError("GOES handler has no fixed satellite binding")
        return capture_latest_goes_rrqpe(
            job,
            satellite=spec.satellite,
            raw_store=store,
        )
    if spec.handler_id in {GOES18_GLM_HANDLER_ID, GOES19_GLM_HANDLER_ID}:
        from .lightning_exact_raw import capture_noaa_goes_glm_raw

        if spec.satellite is None:  # pragma: no cover - closed table invariant
            raise ValueError("GLM handler has no fixed satellite binding")
        return capture_noaa_goes_glm_raw(
            job,
            satellite=spec.satellite,
            raw_store=store,
        )
    if spec.handler_id == EUMETSAT_LI_HANDLER_ID:
        from .lightning_exact_raw import (
            EUMETSAT_EPHEMERAL_TOKEN_ENV,
            EphemeralBearerToken,
            capture_eumetsat_mtg_li_raw,
        )

        token_value = os.environ.pop(EUMETSAT_EPHEMERAL_TOKEN_ENV, None)
        if not isinstance(token_value, str) or not token_value:
            raise ValueError("EUMETSAT ephemeral child token is unavailable")
        token = EphemeralBearerToken(token_value)
        token_value = ""
        if execution_deadline_at is None:
            token.close()
            raise ValueError("EUMETSAT child deadline is unavailable")
        try:
            return capture_eumetsat_mtg_li_raw(
                job,
                raw_store=store,
                access_token_supplier=lambda: token,
                deadline_at=execution_deadline_at,
            )
        finally:
            token.close()
    if spec.handler_id == METOFFICE_HANDLER_ID:
        from .met_office_native import capture_latest_met_office_odim

        return capture_latest_met_office_odim(job, raw_store=store)
    if spec.handler_id == METEOFRANCE_HANDLER_ID:
        from .meteofrance_native import capture_latest_meteofrance_hdf5

        return capture_latest_meteofrance_hdf5(job, raw_store=store)
    if spec.handler_id == HSAF_H40B_HANDLER_ID:
        from .hsaf_h40b_native import capture_latest_hsaf_h40b

        return capture_latest_hsaf_h40b(job, raw_store=store)
    if spec.handler_id == NETATMO_HANDLER_ID:
        from .netatmo_native import capture_netatmo_public_data

        credential = os.environ.get("STV3_NETATMO_ACCESS_TOKEN")
        if not isinstance(credential, str) or not credential:
            raise ValueError("Netatmo worker credential is unavailable")
        return capture_netatmo_public_data(
            job, raw_store=store, access_token=credential
        )
    if spec.handler_id == SYNOPTIC_HANDLER_ID:
        from .synoptic_native import capture_synoptic_timeseries

        return capture_synoptic_timeseries(job, raw_store=store)
    if spec.handler_id == RAINVIEWER_HANDLER_ID:
        from .rainviewer_native import capture_latest_rainviewer_region

        return capture_latest_rainviewer_region(job, raw_store=store)
    if spec.handler_id in PUBLIC_SOURCE_HANDLER_IDS:
        from .public_source_raw_capture import capture_public_source_handler

        return capture_public_source_handler(
            spec.handler_id,
            job,
            raw_store=store,
        )
    raise ValueError("handler is absent from the closed child dispatcher")


def _recovery_binding(spec: _HandlerSpec):
    """Build the exact shared digest binding without importing providers."""

    if (
        spec.ledger_provider is None
        or spec.capture_provenance_kind is None
        or any(role.provenance_kind is None for role in spec.roles)
    ):
        raise ValueError("handler has no complete recovery digest binding")
    from .acquisition_capture_recovery import (
        RECOVERY_BINDING_CATALOG,
        RawRecoveryBinding,
        RawRecoveryDynamicMemberBinding,
        RawRecoveryMemberBinding,
    )

    binding = RawRecoveryBinding(
        handler_id=spec.handler_id,
        workload_id=spec.workload_id,
        ledger_provider=spec.ledger_provider,
        raw_provider=spec.native_provider,
        capture_product=spec.capture_product,
        capture_suffix=spec.capture_fetch_suffix,
        members=tuple(
            RawRecoveryMemberBinding(
                role=role.role,
                raw_product=role.raw_product,
                receipt_suffix=role.fetch_suffix,
                maximum_transport_bytes=role.maximum_bytes,
                allowed_content_types=role.allowed_content_types,
                provenance_kind=str(role.provenance_kind),
                receipt_attempt_limit=role.receipt_attempt_limit,
            )
            for role in spec.roles
        ),
        maximum_transport_bytes=spec.maximum_transport_bytes,
        capture_provenance_kind=spec.capture_provenance_kind,
        job_identity_kind=(
            "safe_native_alias_v1" if spec.safe_native_identity else "origin_v1"
        ),
        dynamic_member=(
            None
            if spec.dynamic_role is None
            else RawRecoveryDynamicMemberBinding(
                role_prefix=spec.dynamic_role.role_prefix,
                role_index_width=spec.dynamic_role.role_index_width,
                minimum_count=spec.dynamic_role.minimum_count,
                maximum_count=spec.dynamic_role.maximum_count,
                raw_product=spec.dynamic_role.raw_product,
                receipt_suffix_template=(
                    spec.dynamic_role.fetch_suffix_template
                ),
                maximum_transport_bytes=spec.dynamic_role.maximum_bytes,
                allowed_content_types=spec.dynamic_role.allowed_content_types,
                provenance_kind=spec.dynamic_role.provenance_kind,
            )
        ),
        satellite=spec.satellite if spec.handler_id in {
            GOES18_GLM_HANDLER_ID, GOES19_GLM_HANDLER_ID
        } else None,
    )
    registered = RECOVERY_BINDING_CATALOG.get(spec.handler_id)
    if registered != binding:
        raise ValueError("child and recovery bindings differ")
    return binding


def _role_fetch_matches(
    role: _RoleSpec,
    fetch_id: str,
    native_job_id: str,
) -> bool:
    prefix = f"{native_job_id}{role.fetch_suffix}"
    if role.receipt_attempt_limit is None:
        return fetch_id == prefix
    attempt = fetch_id[len(prefix):] if fetch_id.startswith(prefix) else ""
    return (
        attempt.isascii()
        and attempt.isdigit()
        and not attempt.startswith("0")
        and 1 <= int(attempt) <= role.receipt_attempt_limit
    )


def _technical_evidence(
    result: Any,
    *,
    spec: _HandlerSpec,
    store: RawArtifactStore,
    job_id: str,
) -> dict[str, Any]:
    native_job_id = job_id
    if spec.safe_native_identity:
        from .acquisition_query_authority import safe_native_job_id

        native_job_id = safe_native_job_id(job_id)
    if (
        not isinstance(result, dict)
        or result.get(spec.decode_field) != spec.decode_status
        or result.get("capture_state") != spec.capture_state
        or result.get("weather_state") != "unknown"
        or result.get("operational_admission_eligible") is not False
    ):
        raise ValueError("native handler returned an operational or unknown contract")
    if spec.result_provider is None:
        if "provider" in result:
            raise ValueError("native handler unexpectedly exposed a provider result")
    elif result.get("provider") != spec.result_provider:
        raise ValueError("native handler provider binding is invalid")
    provenance = result.get("raw_provenance")
    if not isinstance(provenance, dict):
        raise ValueError("native handler omitted raw provenance")
    sources = provenance.get(spec.source_field)
    if not isinstance(sources, dict):
        raise ValueError("native handler returned an incomplete role set")
    role_specs = spec.role_specs_for_roles(sources)
    expected_roles = tuple(role.role for role in role_specs)
    capture_sha = _hash(
        provenance.get("raw_capture_receipt_sha256"),
        "capture receipt SHA-256",
    )
    manifest_value = provenance.get("raw_capture_manifest_sha256")
    manifest_sha = _hash(manifest_value, "capture manifest SHA-256")
    capture_id = _exact_string(provenance.get("raw_capture_id"), "raw capture ID")
    capture = store.load_capture(capture_id)
    if (
        capture is None
        or capture.provider != spec.native_provider
        or capture.product != spec.capture_product
        or capture.fetch_id != f"{native_job_id}{spec.capture_fetch_suffix}"
        or capture.manifest_sha256 != manifest_sha
        or capture.capture_receipt_sha256 != capture_sha
    ):
        raise ValueError("native handler evidence differs from durable raw state")
    capture_members = {member.role: member for member in capture.members}
    if set(capture_members) != set(expected_roles):
        raise ValueError("durable capture role set differs from handler binding")
    if "raw_member_count" in provenance and (
        provenance["raw_member_count"] != len(expected_roles)
    ):
        raise ValueError("native handler raw member count drifted")

    receipts_by_role = {}
    for role_spec in role_specs:
        source = sources[role_spec.role]
        if not isinstance(source, dict) or source.get("role") != role_spec.role:
            raise ValueError("native handler role binding is invalid")
        receipt_key = f"{spec.source_hash_prefix}receipt_id"
        content_key = f"{spec.source_hash_prefix}content_sha256"
        receipt_sha_key = f"{spec.source_hash_prefix}receipt_sha256"
        receipt_id = _exact_string(source.get(receipt_key), "raw receipt ID")
        content_sha = _hash(source.get(content_key), "content SHA-256")
        receipt_sha = _hash(source.get(receipt_sha_key), "receipt SHA-256")
        size_value = source.get("size_bytes")
        if size_value is None and (
            spec.source_size_optional or len(role_specs) == 1
        ):
            size_bytes = None
        elif (
            isinstance(size_value, bool)
            or not isinstance(size_value, int)
            or not (
                0
                if role_spec.provenance_kind
                in _EMPTY_EXACT_RESPONSE_PROVENANCE_KINDS
                else 1
            ) <= size_value <= role_spec.maximum_bytes
        ):
            raise ValueError("native handler source size violates its role bound")
        else:
            size_bytes = size_value
        receipt = store.load_receipt(receipt_id)
        member = capture_members[role_spec.role]
        if (
            receipt is None
            or receipt.provider != spec.native_provider
            or receipt.product != role_spec.raw_product
            or not _role_fetch_matches(
                role_spec, receipt.fetch_id, native_job_id
            )
            or receipt.content_type not in role_spec.allowed_content_types
            or receipt.content_sha256 != content_sha
            or receipt.receipt_sha256 != receipt_sha
            or (
                size_bytes is not None
                and receipt.size_bytes != size_bytes
            )
            or not (
                0
                if role_spec.provenance_kind
                in _EMPTY_EXACT_RESPONSE_PROVENANCE_KINDS
                else 1
            ) <= receipt.size_bytes <= role_spec.maximum_bytes
            or member.receipt_id != receipt.receipt_id
            or member.content_sha256 != receipt.content_sha256
            or member.receipt_sha256 != receipt.receipt_sha256
        ):
            raise ValueError("native handler role evidence differs from durable raw state")
        receipts_by_role[role_spec.role] = receipt

    # Import lazily after all child/native evidence has been checked.  This is
    # the one shared canonical aggregate implementation also used by crash
    # recovery and the capacity ledger; duplicating it here would permit a
    # child/recovery hash split after future role-set changes.
    from .acquisition_capture_recovery import (
        recovery_evidence_digest,
    )
    binding = _recovery_binding(spec)
    digest = recovery_evidence_digest(
        binding=binding,
        receipts_by_role=receipts_by_role,
        capture=capture,
    )
    evidence = {
        "actual_bytes": digest.actual_bytes,
        "actual_entries": digest.actual_entries,
        "capture_sha256": digest.capture_sha256,
        "content_sha256": digest.content_sha256,
        "handler_id": spec.handler_id,
        "operational_admission_eligible": False,
        "receipt_sha256": digest.receipt_sha256,
        "roles": list(expected_roles),
        "status": "captured_shadow_raw",
        "workload_id": spec.workload_id,
    }
    evidence["evidence_sha256"] = hashlib.sha256(
        json.dumps(
            evidence,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return {"contract": RESULT_CONTRACT, **evidence}


def execute_shadow_capture(
    request: Mapping[str, Any],
    *,
    _clock: Any = None,
) -> dict[str, Any]:
    """Run one matrix-admitted raw capture and return technical evidence."""

    base_expected = {
        "acquisition_job_id",
        "contract",
        "handler_id",
        "handler_payload",
        "raw_store_limits",
        "raw_store_root",
        "workload_id",
    }
    if not isinstance(request, Mapping) or len(request) > MAX_REQUEST_FIELDS:
        raise ValueError("invalid child request")
    if request.get("contract") != CONTRACT or "handler_id" not in request:
        raise ValueError("child request contract mismatch")
    handler_id = _exact_string(request["handler_id"], "handler ID")
    try:
        spec = _HANDLERS[handler_id]
    except KeyError as err:
        raise ValueError("handler is absent from the closed child dispatcher") from err
    expected = base_expected | (
        {"execution_guard"} if spec.execution_guard_required else set()
    )
    if spec.handler_id == EUMETSAT_LI_HANDLER_ID:
        expected |= {"execution_deadline_at"}
    if set(request) != expected:
        raise ValueError("child request contract mismatch")
    if request["workload_id"] != spec.workload_id:
        raise ValueError("handler/workload binding mismatch")
    handler_payload = request["handler_payload"]
    if type(handler_payload) is not dict:
        raise ValueError("raw handler payload must be exactly an object")
    if spec.payload_contract == "exact_empty_object_v1":
        if handler_payload:
            raise ValueError("raw handler payload must be exactly empty")
    elif not handler_payload:
        raise ValueError("location handler payload is unavailable")
    job_id = _exact_string(request["acquisition_job_id"], "acquisition job ID")
    clock = time.time if _clock is None else _clock
    execution_deadline_at: float | None = None
    if spec.handler_id == EUMETSAT_LI_HANDLER_ID:
        deadline_epoch = _finite_time(
            request["execution_deadline_at"], "EUMETSAT execution deadline"
        )
        now_epoch = _finite_time(clock(), "trusted worker clock")
        remainder = deadline_epoch - now_epoch
        if not 0.0 < remainder <= 50.0:
            raise ValueError("EUMETSAT execution deadline is outside its bound")
        execution_deadline_at = time.monotonic() + remainder
    if spec.execution_guard_required:
        _validate_execution_guard(
            request["execution_guard"],
            job_id=job_id,
            handler_payload=handler_payload,
            clock=clock,
            authority_required=spec.payload_contract in {
                "acquisition_query_authority_projection_v1",
                "openmeteo_target_query_batch_v1",
                "public_region_query_v1",
            },
        )
    raw_root = _exact_string(request["raw_store_root"], "raw store root", 2048)
    limits = _limits(request["raw_store_limits"])
    maximum_role_bytes = max(
        [role.maximum_bytes for role in spec.roles]
        + (
            []
            if spec.dynamic_role is None
            else [spec.dynamic_role.maximum_bytes]
        )
    )
    if limits.max_object_bytes < maximum_role_bytes:
        raise ValueError("raw store cannot hold the handler transport envelope")
    if limits.max_capture_members < spec.maximum_artifacts:
        raise ValueError("raw store cannot hold the handler role set")
    if limits.max_total_bytes < spec.maximum_transport_bytes:
        raise ValueError("raw store total bound is below the handler reservation")
    store = RawArtifactStore(raw_root, limits=limits)
    # The parent runtime has already created and fully audited this exact
    # catalog before admission opened.  A fresh provider child must only
    # attach to that authority; repeating the full startup integrity scan in
    # every parallel child serializes otherwise independent provider I/O.
    store.attach_existing()
    if spec.handler_id == EUMETSAT_LI_HANDLER_ID:
        now_epoch = _finite_time(clock(), "trusted worker clock")
        remainder = float(request["execution_deadline_at"]) - now_epoch
        if not 0.0 < remainder <= 50.0:
            raise ValueError("EUMETSAT execution deadline expired before I/O")
        execution_deadline_at = time.monotonic() + remainder
    if spec.execution_guard_required:
        # Re-read the trusted worker wall clock after all local setup and at
        # the last boundary before the native handler can perform network I/O.
        _validate_execution_guard(
            request["execution_guard"],
            job_id=job_id,
            handler_payload=handler_payload,
            clock=clock,
            authority_required=spec.payload_contract in {
                "acquisition_query_authority_projection_v1",
                "openmeteo_target_query_batch_v1",
                "public_region_query_v1",
            },
        )
        if spec.payload_contract == "exact_empty_object_v1":
            result = _run_native(
                spec,
                job_id=job_id,
                store=store,
                execution_deadline_at=execution_deadline_at,
            )
        else:
            result = _run_native(
                spec,
                job_id=job_id,
                store=store,
                handler_payload=handler_payload,
            )
    else:
        if spec.payload_contract == "exact_empty_object_v1":
            result = _run_native(
                spec,
                job_id=job_id,
                store=store,
                execution_deadline_at=execution_deadline_at,
            )
        else:
            result = _run_native(
                spec,
                job_id=job_id,
                store=store,
                handler_payload=handler_payload,
            )
    return _technical_evidence(result, spec=spec, store=store, job_id=job_id)


__all__ = [
    "AEMET_HANDLER_ID",
    "AEMET_WORKLOAD_ID",
    "CONTRACT",
    "DPC_HANDLER_ID",
    "DPC_WORKLOAD_ID",
    "DWD_HANDLER_ID",
    "DWD_WORKLOAD_ID",
    "EUMETSAT_LI_HANDLER_ID",
    "EUMETSAT_LI_WORKLOAD_ID",
    "GOES18_GLM_HANDLER_ID",
    "GOES18_GLM_WORKLOAD_ID",
    "GOES18_HANDLER_ID",
    "GOES18_WORKLOAD_ID",
    "GOES19_HANDLER_ID",
    "GOES19_WORKLOAD_ID",
    "GOES19_GLM_HANDLER_ID",
    "GOES19_GLM_WORKLOAD_ID",
    "HANDLER_ID",
    "HSAF_H40B_HANDLER_ID",
    "HSAF_H40B_WORKLOAD_ID",
    "KNMI_HANDLER_ID",
    "KNMI_WORKLOAD_ID",
    "KMI_AWS_HANDLER_ID",
    "KMI_AWS_WORKLOAD_ID",
    "KMI_APPV4_HANDLER_ID",
    "KMI_APPV4_WORKLOAD_ID",
    "KMI_PUBLIC_RADAR_HANDLER_ID",
    "KMI_PUBLIC_RADAR_WORKLOAD_ID",
    "METOFFICE_HANDLER_ID",
    "METOFFICE_WORKLOAD_ID",
    "METEOFRANCE_HANDLER_ID",
    "METEOFRANCE_WORKLOAD_ID",
    "METAR_HANDLER_ID",
    "METAR_WORKLOAD_ID",
    "NETATMO_HANDLER_ID",
    "NETATMO_WORKLOAD_ID",
    "NOAA_ERR_HANDLER_ID",
    "NOAA_ERR_WORKLOAD_ID",
    "OPERA_HANDLER_ID",
    "OPERA_WORKLOAD_ID",
    "RESULT_CONTRACT",
    "RAINVIEWER_HANDLER_ID",
    "RAINVIEWER_WORKLOAD_ID",
    "SYNOPTIC_HANDLER_ID",
    "SYNOPTIC_WORKLOAD_ID",
    "WORKLOAD_ID",
    "execute_shadow_capture",
]
